//! `GET /v1/streams?after=<cursor>`: attaches a new reader to a committed response
//! (`docs/INTERNAL-STREAM.md` §6.6–6.7). The identity travels only in `X-Ck-Stream-Id`; a
//! resume sends no upstream request, starts no attempt and accounts no usage.

use std::collections::VecDeque;
use std::sync::Arc;
use std::task::Poll;

use axum::body::Body;
use axum::extract::{Extension, RawQuery, State};
use axum::http::{HeaderMap, HeaderValue, header};
use axum::response::Response;
use bytes::Bytes;
use ck_core::exec::Caller;
use ck_stream::{Lease, Read, Reader, Writer};
use ck_translate::codec::{self, RequestView, ResponseEgress};

use super::protocol::response_identity;
use super::{Attached, CURSOR_HEADER, Cursor, Owner, ProtocolEgress, Registry, STREAM_ID_HEADER, StreamId, Units};
use crate::Runtime;
use crate::http::egress::Framing;
use crate::http::respond::error_detail;

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
    let last_event = single(headers, "last-event-id")?;
    let after = query_cursor(query)?;
    // Authorization comes before any cursor lookup: every unusable id is the same 410.
    let id = StreamId::parse(id.as_bytes()).ok_or(Failure::Gone)?;
    let owner = Owner { caller, scope: None };
    let attached = rt.streams().attach(&id, &owner).ok_or(Failure::Gone)?;
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
    let mut response = Response::new(body(replay, first));
    let set = response.headers_mut();
    set.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(if framing == Framing::Sse {
            "text/event-stream"
        } else {
            "application/json"
        }),
    );
    set.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    set.insert(STREAM_ID_HEADER, id.header_value());
    set.insert(
        CURSOR_HEADER,
        HeaderValue::from_str(&cursor.to_string()).expect("cursor text"),
    );
    Ok(response)
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

/// One resumed delivery: a reader from the stream's first record re-encoded through the
/// stream's output, with the body before the cursor dropped.
struct Replay {
    response: Writer,
    reader: Reader,
    egress: ProtocolEgress,
    units: Units,
    /// Set once the stream's terminal was read: whether the egress delivered a terminal
    /// record. A terminal without one (cancel, shutdown, expiry, a lost producer) closes the
    /// body abnormally, so the client never sees a clean end of an incomplete response.
    ended: Option<bool>,
}

impl Replay {
    async fn open(streams: &Registry, attached: Attached, cursor: Cursor) -> Result<Self, Failure> {
        let Attached {
            output,
            mut response,
            mut request,
        } = attached;
        let (reader, mut first, mut records) = streams.with_store(|store| {
            Ok::<_, ck_stream::Error>((
                store.reader(response.id(), 0)?,
                store.reader(response.id(), 0)?,
                store.reader(request.id(), 0)?,
            ))
        })??;
        let identity = response_identity(&mut response, &mut first).await?;
        // The request view is rebuilt from the request stream's records (contract §2).
        let (document, media) = codec::chat::request::read_request(&mut request, &mut records)?;
        let view = RequestView {
            document: &document,
            media: &media,
            operation: output.operation,
        };
        let egress = ProtocolEgress::new(output, &mut response, identity, view)?;
        Ok(Self {
            response,
            reader,
            egress,
            units: Units::new(output.framing, cursor),
            ended: None,
        })
    }

    /// Produces the body up to the cursor from the records already published and returns
    /// the bytes past it. A cursor past the published output is a client error.
    fn catch_up(&mut self) -> Result<Vec<Bytes>, Failure> {
        let mut out = Vec::new();
        let mut gone = false;
        while !self.units.reached() && self.ended.is_none() {
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

    fn advance(&mut self, lease: Lease, out: &mut Vec<Bytes>) -> Result<(), Failure> {
        let Self {
            response,
            egress,
            units,
            ..
        } = self;
        egress.advance(response, lease, |output| {
            units.push(&output, out).map_err(codec::Error::Storage)
        })?;
        Ok(())
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
