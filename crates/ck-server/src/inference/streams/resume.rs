//! `GET /v1/streams?after=<cursor>`: attaches a new reader to a committed response
//! (`docs/INTERNAL-STREAM.md` §6.6–6.7). The identity travels only in `X-Ck-Stream-Id`; a
//! resume sends no upstream request, starts no attempt and accounts no usage.

use std::collections::VecDeque;
use std::sync::Arc;
use std::task::Poll;

use axum::body::Body;
use axum::extract::{Extension, RawQuery, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::Response;
use bytes::Bytes;
use ck_core::exec::Caller;
use ck_stream::{Lease, Read, Reader, Terminal, Writer};
use ck_translate::codec::event::Kind;
use ck_translate::codec::{self, RequestView, ResponseEgress};

use super::protocol::response_identity;
use super::{Attached, CURSOR_HEADER, Cursor, Owner, ProtocolEgress, Registry, STREAM_ID_HEADER, StreamId, Units};
use crate::Runtime;
use crate::http::egress::{Framing, Protocol};
use crate::http::respond::error_detail;
use crate::inference::responses_state::{Publication, passed};

pub async fn resume(
    State(rt): State<Arc<Runtime>>,
    Extension(caller): Extension<Caller>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Response {
    match start(&rt, caller, query.as_deref().unwrap_or_default(), &headers).await {
        Ok(response) => response,
        Err(failure) => failure.response(),
    }
}

enum Failure {
    /// A client error after authentication: identity header or cursor.
    Request(&'static str),
    /// Unknown, foreign, expired or evicted, answered identically.
    Gone,
    Storage,
    Defect,
}

impl Failure {
    fn response(self) -> Response {
        match self {
            Self::Request(message) => error_detail(400, message, "invalid_request_error"),
            Self::Gone => error_detail(410, "The stream expired or does not exist.", "stream.gone"),
            Self::Storage => error_detail(503, "Stream storage is exhausted.", "stream.storage_exhausted"),
            Self::Defect => error_detail(500, "The stream could not be read.", "server_error"),
        }
    }
}

impl From<ck_stream::Error> for Failure {
    fn from(error: ck_stream::Error) -> Self {
        match error {
            ck_stream::Error::Gone | ck_stream::Error::Closed => Self::Gone,
            ck_stream::Error::StorageExhausted | ck_stream::Error::Io(_) => Self::Storage,
            ck_stream::Error::InvalidPosition
            | ck_stream::Error::InvalidRecord
            | ck_stream::Error::IncompleteRecord => Self::Defect,
        }
    }
}

impl From<codec::Error> for Failure {
    fn from(error: codec::Error) -> Self {
        match error {
            codec::Error::Storage(error) => error.into(),
            _ => Self::Defect,
        }
    }
}

async fn start(rt: &Runtime, caller: Caller, query: &str, headers: &HeaderMap) -> Result<Response, Failure> {
    let id = single(headers, STREAM_ID_HEADER)?.ok_or(Failure::Request("X-Ck-Stream-Id is required."))?;
    // Authorization and liveness come before any cursor check: every unknown, foreign,
    // expired or evicted id is the same 410, whatever cursor arrives with it.
    let id = StreamId::parse(id.as_bytes()).ok_or(Failure::Gone)?;
    let owner = Owner { caller, scope: None };
    let attached = rt.streams().attach(&id, &owner).ok_or(Failure::Gone)?;
    let last_event = single(headers, "last-event-id")?;
    let after = query_cursor(query)?;
    let framing = attached.output.framing;
    // `Last-Event-ID` replaces `after` for SSE only.
    let last_event = match last_event.filter(|_| framing == Framing::Sse) {
        Some(value) => Some(Cursor::parse(value.as_bytes()).ok_or(Failure::Request("Last-Event-ID is not a cursor."))?),
        None => None,
    };
    let cursor = match (after, last_event) {
        (Some(after), Some(last)) if after != last => {
            return Err(Failure::Request("after and Last-Event-ID name different cursors."));
        }
        (Some(cursor), _) | (None, Some(cursor)) => cursor,
        (None, None) => return Err(Failure::Request("A cursor is required.")),
    };
    if !cursor.fits(framing) {
        return Err(Failure::Request("The cursor kind does not match the stream."));
    }
    let mut replay = Replay::open(rt.streams(), attached, cursor).await?;
    let first = replay.catch_up()?;
    Ok(delivery(replay, first, framing, &id, cursor))
}

/// A client response delivering `replay` after `cursor`, which `first` begins. Live delivery
/// and resume answer with the same headers.
pub(crate) fn delivery(replay: Replay, first: Vec<Bytes>, framing: Framing, id: &StreamId, cursor: Cursor) -> Response {
    let content_type = replay.egress.content_type(framing);
    let status = StatusCode::from_u16(replay.egress.status()).expect("an HTTP status");
    let mut response = Response::new(body(replay, first));
    *response.status_mut() = status;
    let set = response.headers_mut();
    set.insert(header::CONTENT_TYPE, content_type);
    set.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    set.insert(STREAM_ID_HEADER, id.header_value());
    set.insert(
        CURSOR_HEADER,
        HeaderValue::from_str(&cursor.to_string()).expect("cursor text"),
    );
    response
}

/// The one value of `name`; a repeated header is a client error.
fn single<'a>(headers: &'a HeaderMap, name: &str) -> Result<Option<&'a HeaderValue>, Failure> {
    let mut values = headers.get_all(name).iter();
    let first = values.next();
    if values.next().is_some() {
        return Err(Failure::Request("A resume header is repeated."));
    }
    Ok(first)
}

fn query_cursor(query: &str) -> Result<Option<Cursor>, Failure> {
    let mut cursor = None;
    for (name, value) in url::form_urlencoded::parse(query.as_bytes()) {
        if name != "after" {
            continue;
        }
        if cursor.is_some() {
            return Err(Failure::Request("after is repeated."));
        }
        cursor = Some(Cursor::parse(value.as_bytes()).ok_or(Failure::Request("after is not a cursor."))?);
    }
    Ok(cursor)
}

/// One delivery: a reader from the stream's first record encoded through the stream's
/// output, with the body before the cursor dropped. The live client gets it from the zero
/// cursor, so a resume re-encodes exactly the bytes the live client got.
pub(crate) struct Replay {
    response: Writer,
    reader: Reader,
    egress: ProtocolEgress,
    units: Units,
    /// Set once the stream's terminal was read: whether the egress delivered a terminal
    /// record. A terminal without one (cancel, shutdown, expiry, a lost producer) closes the
    /// body abnormally, so the client never sees a clean end of an incomplete response.
    ended: Option<bool>,
    /// The index publication's gate: the response's `done` is encoded only once it opens, so
    /// no client sees a completed response whose id does not resolve yet.
    gate: Option<tokio::sync::watch::Receiver<bool>>,
    /// The live attempt's publication, reserved when the attempt reaches `done` uncommitted.
    publication: Option<Arc<Publication>>,
}

impl Replay {
    async fn open(streams: &Registry, attached: Attached, cursor: Cursor) -> Result<Self, Failure> {
        let Attached {
            output,
            mut response,
            mut request,
            shown,
        } = attached;
        let (reader, mut first, mut records) = streams.with_store(|store| {
            Ok::<_, ck_stream::Error>((
                store.reader(response.id(), 0)?,
                store.reader(response.id(), 0)?,
                store.reader(request.id(), 0)?,
            ))
        })??;
        let identity = response_identity(&mut response, &mut first).await?;
        let egress = if matches!(
            output.protocol,
            Protocol::Speech | Protocol::Images(_) | Protocol::Search | Protocol::Video(_)
        ) {
            ProtocolEgress::media(output, &mut response, identity)?
        } else {
            // The request view is rebuilt from the request stream's records (contract §2).
            let (document, media) = codec::chat::request::read_request(&mut request, &mut records)?;
            let view = RequestView {
                document: &document,
                media: &media,
                operation: output.operation,
            };
            ProtocolEgress::new(
                output,
                &mut response,
                identity,
                view,
                (shown.id, shown.model.as_deref()),
            )?
        };
        Ok(Self {
            response,
            reader,
            egress,
            units: Units::new(output.framing, cursor),
            ended: None,
            gate: shown.gate,
            publication: None,
        })
    }

    /// Live delivery of the response stream `response`, encoded by `egress` from the zero
    /// cursor. `publication` is the attempt's index publication, if its turn is indexed.
    pub(crate) fn live(
        response: Writer,
        reader: Reader,
        egress: ProtocolEgress,
        framing: Framing,
        publication: Option<Arc<Publication>>,
    ) -> Self {
        Self {
            response,
            reader,
            egress,
            units: Units::new(framing, Cursor::start(framing)),
            ended: None,
            gate: publication.as_ref().map(|publication| publication.gate()),
            publication,
        }
    }

    /// The commitment point (`docs/EXECUTION-CONTRACT.md` §9): encodes records until the
    /// egress emits deliverable bytes and returns them. Records the egress turns into no bytes,
    /// such as metadata, deliver nothing. An `error` record ends the attempt before it reaches
    /// the egress, so a failed attempt never commits.
    pub(crate) async fn first(&mut self) -> Result<Vec<Bytes>, Uncommitted> {
        loop {
            let lease = match self.reader.next().await {
                Some(Read::Record(lease)) if lease.record().kind == Kind::Error as u32 => {
                    return Err(Uncommitted::Error);
                }
                // A buffered body commits at its `done`: its slot is reserved before it.
                Some(Read::Record(lease)) if lease.record().kind == Kind::Done as u32 => {
                    if let Some(publication) = &self.publication {
                        publication.reserve();
                    }
                    passed(&mut self.gate).await;
                    lease
                }
                Some(Read::Record(lease)) => lease,
                Some(Read::Terminal(terminal)) => return Err(Uncommitted::Terminal(terminal)),
                None => return Err(Uncommitted::Terminal(Terminal::ProducerLost)),
            };
            let mut out = Vec::new();
            self.advance(lease, &mut out).map_err(Uncommitted::Codec)?;
            if !out.is_empty() {
                return Ok(out);
            }
        }
    }

    /// Produces the body up to the cursor from the records already published and returns
    /// the bytes past it. A cursor past the published output is a client error. A raw body
    /// also reads its media type, which a committed stream published before its first byte.
    fn catch_up(&mut self) -> Result<Vec<Bytes>, Failure> {
        let mut out = Vec::new();
        let mut gone = false;
        while !(self.units.reached() && self.egress.typed()) && self.ended.is_none() {
            match self.reader.try_next() {
                Poll::Ready(Some(Read::Record(lease))) => self.advance(lease, &mut out)?,
                Poll::Ready(Some(Read::Terminal(terminal))) => {
                    gone = terminal == ck_stream::Terminal::Gone;
                    self.ended = Some(self.egress.finish().is_ok());
                }
                Poll::Ready(None) | Poll::Pending => break,
            }
        }
        if self.units.reached() {
            Ok(out)
        } else if gone {
            Err(Failure::Gone)
        } else {
            Err(Failure::Request("The cursor is past the published output."))
        }
    }

    fn advance(&mut self, lease: Lease, out: &mut Vec<Bytes>) -> codec::Result<()> {
        let Self {
            response,
            egress,
            units,
            ..
        } = self;
        egress.advance(response, lease, |output| {
            units.push(&output, out).map_err(codec::Error::Storage)
        })
    }

    /// The next body bytes; `None` at the end, `Some(Err)` to close the body abnormally.
    async fn next(&mut self, pending: &mut VecDeque<Bytes>) -> Option<Result<Bytes, std::io::Error>> {
        loop {
            if let Some(bytes) = pending.pop_front() {
                return Some(Ok(bytes));
            }
            match self.ended {
                Some(true) => return None,
                Some(false) => {
                    self.ended = Some(true);
                    return Some(Err(std::io::Error::other("the stream ended without its terminal")));
                }
                None => {}
            }
            match self.reader.next().await {
                Some(Read::Record(lease)) => {
                    if lease.record().kind == Kind::Done as u32 {
                        passed(&mut self.gate).await;
                    }
                    let mut out = Vec::new();
                    let advanced = self.advance(lease, &mut out);
                    pending.extend(out);
                    if advanced.is_err() {
                        self.ended = Some(false);
                    }
                }
                Some(Read::Terminal(_)) => self.ended = Some(self.egress.finish().is_ok()),
                None => self.ended = Some(false),
            }
        }
    }
}

/// What ended an attempt before its first deliverable byte. Nothing reached the client.
#[derive(Debug)]
pub enum Uncommitted {
    /// The stream published an `error` record: an upstream or producer failure.
    Error,
    /// The stream ended with this terminal and no deliverable byte.
    Terminal(Terminal),
    /// The egress failed: storage, or a gateway defect.
    Codec(codec::Error),
}

/// The response body. Dropping it, as a client disconnect does, releases only the reader.
fn body(replay: Replay, first: Vec<Bytes>) -> Body {
    let state = (replay, VecDeque::from(first));
    Body::from_stream(futures_util::stream::unfold(
        state,
        |(mut replay, mut pending)| async move {
            let item = replay.next(&mut pending).await?;
            Some((item, (replay, pending)))
        },
    ))
}
