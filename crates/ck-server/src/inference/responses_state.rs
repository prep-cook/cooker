//! Local Responses state (D2-PLAN §2.1–2.5). cooker is the endpoint of Responses state:
//! every Responses exchange shows its client one response id cooker issued, fixed across
//! failover and resume (Q-E), and a later request continues it with `previous_response_id`
//! whatever upstream serves either turn (Settled 54).
//!
//! The index maps `(Owner, response id)` to the committed turn's request and response streams
//! and the account that produced the response. The payload stays in those streams under the
//! stream cache rules, so an entry lives only while both streams do: one hour without a read,
//! least recently read evicted first within the pool (owner ruling Q-A). Every miss, whatever
//! its cause, is the same `previous_response_not_found`.
//!
//! Publication is a barrier: the committed attempt reserves a slot at commitment (a buffered
//! one at its `done`), and its finalizer fills the slot when the response stream ends with
//! `done`, independently of the usage reader. Client egress, live or resumed, holds the
//! terminal output until the slot is decided, so a client never sees a completed response
//! whose id does not resolve yet. A turn the index cannot hold still completes; a later
//! continuation of it gets `previous_response_not_found` (owner, 2026-11-15).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use axum::response::Response;
use ck_stream::Writer;
use ck_translate::codec::document::{Document, Value};
use ck_translate::codec::event::{Direction, Identity, LocalId};
use ck_translate::codec::media::Registry as Media;
use ck_translate::codec::{self, RequestView, provenance};
use ck_translate::responses::history::{self, Stored};
use tokio::sync::watch;

use super::streams::{Exchange, Owner, Registry};

/// A response id cooker issued: `resp_` and 128 random bits in lowercase hexadecimal.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ResponseId([u8; 16]);

impl ResponseId {
    fn random() -> Self {
        let mut bytes = [0; 16];
        getrandom::fill(&mut bytes).expect("system randomness");
        Self(bytes)
    }

    /// The id the client sees.
    pub fn text(&self) -> LocalId {
        let digits = super::streams::StreamId(self.0).header_value();
        let mut text = *b"resp_00000000000000000000000000000000";
        text[5..].copy_from_slice(digits.as_bytes());
        LocalId::new(std::str::from_utf8(&text).expect("ASCII")).expect("37 ASCII bytes")
    }

    /// The inverse of [`ResponseId::text`]; `None` for any other text.
    fn parse(text: &str) -> Option<Self> {
        let digits = text.strip_prefix("resp_")?;
        super::streams::StreamId::parse(digits.as_bytes()).map(|id| Self(id.0))
    }
}

/// What a Responses exchange records before its first attempt.
#[derive(Clone, Copy)]
pub struct Turn {
    pub id: ResponseId,
    /// The client did not send `store: false`: the committed turn is indexed.
    pub stored: bool,
    /// The request continues a stored turn (`previous_response_id`), so the exchange's
    /// request stream holds the expanded history rather than the client's body.
    pub continued: bool,
}

/// The index of committed turns. Its capacity is the pool's header slots, fixed when the store
/// first opens: every live entry holds two streams, so live entries never fill it.
#[derive(Default)]
pub struct Index {
    slots: Mutex<Option<Slots>>,
}

struct Slots {
    entries: HashMap<ResponseId, Slot>,
    capacity: usize,
}

enum Slot {
    /// A committed turn still running.
    Reserved,
    Published(Entry),
}

/// A committed turn that ended with `done`.
struct Entry {
    owner: Owner,
    /// Allocation handles on the turn's request stream, which holds its expanded request, and
    /// its response stream.
    request: Writer,
    response: Writer,
    provider: Box<str>,
    credential: Box<str>,
}

impl Entry {
    /// Expiry or eviction invalidated either stream; checked as `StreamHandle::gone` does,
    /// without refreshing either stream's last read.
    fn gone(&self) -> bool {
        self.request.allocator().reserve_metadata(0).is_err() || self.response.allocator().reserve_metadata(0).is_err()
    }
}

/// A stored turn found for a continuation, with fresh allocation handles.
struct Found {
    request: Writer,
    response: Writer,
    provider: Box<str>,
    credential: Box<str>,
}

impl Index {
    fn slots<R>(&self, capacity: usize, f: impl FnOnce(&mut Slots) -> R) -> R {
        let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        f(slots.get_or_insert_with(|| Slots {
            entries: HashMap::with_capacity(capacity),
            capacity,
        }))
    }

    /// Reserves a slot for `id`. A full table first drops every entry whose streams are gone;
    /// `false` when it is still full.
    fn reserve(&self, capacity: usize, id: ResponseId) -> bool {
        self.slots(capacity, |slots| {
            if slots.entries.len() >= slots.capacity {
                slots
                    .entries
                    .retain(|_, slot| !matches!(slot, Slot::Published(entry) if entry.gone()));
            }
            if slots.entries.len() >= slots.capacity {
                return false;
            }
            slots.entries.insert(id, Slot::Reserved);
            true
        })
    }

    fn fill(&self, id: ResponseId, entry: Entry) {
        let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(slot) = slots.as_mut().and_then(|slots| slots.entries.get_mut(&id))
            && matches!(slot, Slot::Reserved)
        {
            *slot = Slot::Published(entry);
        }
    }

    fn release(&self, id: ResponseId) {
        let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(slots) = slots.as_mut()
            && matches!(slots.entries.get(&id), Some(Slot::Reserved))
        {
            slots.entries.remove(&id);
        }
    }

    /// The published turn `id` that `owner` created. `None` alike for an unknown id, another
    /// owner, a turn still running, and an expired or evicted one, which leaves the table.
    fn lookup(&self, owner: &Owner, id: ResponseId) -> Option<Found> {
        let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        let slots = slots.as_mut()?;
        let Slot::Published(entry) = slots.entries.get(&id)? else {
            return None;
        };
        if entry.gone() {
            slots.entries.remove(&id);
            return None;
        }
        (entry.owner == *owner).then(|| Found {
            request: entry.request.allocator(),
            response: entry.response.allocator(),
            provider: entry.provider.clone(),
            credential: entry.credential.clone(),
        })
    }
}

/// One attempt's publication. Delivery reserves the slot when the attempt commits; the
/// attempt's finalizer reports how its response stream ended. The slot is filled once both
/// happened and the stream ended with `done`, and the gate opens once the slot is decided:
/// filled, refused or released.
pub(crate) struct Publication {
    index: Arc<Index>,
    capacity: usize,
    id: ResponseId,
    entry: Mutex<Progress>,
    decided: watch::Sender<bool>,
}

struct Progress {
    reserved: bool,
    /// How the response stream ended: `Some(true)` for `done`.
    ended: Option<bool>,
    entry: Option<Entry>,
}

impl Publication {
    /// The publication of an attempt of `exchange` whose response stream is `response` and
    /// whose account is `producer`; `None` for a turn that is not indexed.
    pub(crate) fn new(
        streams: &Registry,
        exchange: &Exchange,
        response: &Writer,
        producer: provenance::Producer<'_>,
    ) -> Option<Arc<Self>> {
        let turn = exchange.turn.filter(|turn| turn.stored)?;
        let capacity = streams.store().ok()?.limits().header_slots as usize;
        Some(Arc::new(Self {
            index: streams.responses().clone(),
            capacity,
            id: turn.id,
            entry: Mutex::new(Progress {
                reserved: false,
                ended: None,
                entry: Some(Entry {
                    owner: exchange.owner.clone(),
                    request: exchange.request.allocator(),
                    response: response.allocator(),
                    provider: producer.provider.into(),
                    credential: producer.credential.into(),
                }),
            }),
            decided: watch::Sender::new(false),
        }))
    }

    /// The gate egress waits on before it encodes the response's `done`.
    pub(crate) fn gate(&self) -> watch::Receiver<bool> {
        self.decided.subscribe()
    }

    /// The attempt committed, or reached its `done` before it could commit.
    pub(crate) fn reserve(&self) {
        let mut progress = self.entry.lock().unwrap_or_else(PoisonError::into_inner);
        if progress.reserved || *self.decided.borrow() {
            return;
        }
        if !self.index.reserve(self.capacity, self.id) {
            self.decided.send_replace(true);
            return;
        }
        progress.reserved = true;
        if let Some(done) = progress.ended {
            self.settle(&mut progress, done);
        }
    }

    /// The finalizer saw the response stream end, with `done` when `done` is true.
    pub(crate) fn ended(&self, done: bool) {
        let mut progress = self.entry.lock().unwrap_or_else(PoisonError::into_inner);
        progress.ended = Some(done);
        if progress.reserved || !done {
            self.settle(&mut progress, done);
        }
    }

    fn settle(&self, progress: &mut Progress, done: bool) {
        match progress.entry.take() {
            Some(entry) if done && progress.reserved => self.index.fill(self.id, entry),
            _ if progress.reserved => self.index.release(self.id),
            _ => {}
        }
        progress.reserved = false;
        self.decided.send_replace(true);
    }
}

impl Drop for Publication {
    fn drop(&mut self) {
        let progress = self.entry.get_mut().unwrap_or_else(PoisonError::into_inner);
        if progress.reserved {
            self.index.release(self.id);
        }
    }
}

/// Waits until `gate` is decided; a gate whose publication went away is decided too.
pub(crate) async fn passed(gate: &mut Option<watch::Receiver<bool>>) {
    if let Some(gate) = gate {
        let _ = gate.wait_for(|decided| *decided).await;
    }
}

/// Why a Responses request is answered before dispatch. Nothing was sent upstream.
#[derive(Debug)]
pub enum Refused {
    /// `conversation` is not served in S1 (owner ruling Q-G).
    Conversation,
    /// `previous_response_id` names nothing this caller may continue.
    NotFound,
    /// The stored turn holds what a continuation cannot carry back, such as media.
    Unsupported,
    /// The expanded request exceeds stream storage.
    Storage,
    Defect,
}

impl Refused {
    /// The client's answer, in the Responses error shape.
    pub fn response(&self) -> Response {
        let (status, body) = self.parts();
        crate::http::respond::gin_json(status, body)
    }

    /// The same answer as a request-scoped error, for the route's error writer.
    pub fn error(&self) -> ck_core::exec::ExecError {
        let (status, body) = self.parts();
        ck_core::exec::ExecError::local(status, ck_core::exec::FailureScope::Request, body)
    }

    fn parts(&self) -> (u16, String) {
        let (status, message, kind, param, code) = match self {
            Self::Conversation => (
                400,
                "The conversation parameter is not supported; continue with previous_response_id or send the full input.",
                "invalid_request_error",
                "conversation",
                "unsupported_parameter",
            ),
            Self::NotFound => (
                400,
                "The previous response was not found; send the full conversation input without previous_response_id.",
                "invalid_request_error",
                "previous_response_id",
                "previous_response_not_found",
            ),
            Self::Unsupported => (
                400,
                "The previous response cannot be continued; send the full conversation input without previous_response_id.",
                "invalid_request_error",
                "previous_response_id",
                "unsupported_value",
            ),
            Self::Storage => (
                503,
                "Stream storage is exhausted.",
                "server_error",
                "previous_response_id",
                "stream.storage_exhausted",
            ),
            Self::Defect => (
                500,
                "The request could not be prepared.",
                "server_error",
                "",
                "server_error",
            ),
        };
        let mut detail = crate::http::gojson::Obj::new()
            .str("message", message)
            .str("type", kind);
        detail = if param.is_empty() {
            detail.raw("param", "null")
        } else {
            detail.str("param", param)
        };
        let detail = detail.str("code", code).finish();
        (status, crate::http::gojson::Obj::new().raw("error", &detail).finish())
    }
}

/// Prepares a Responses exchange before its first attempt. It issues the exchange's response
/// id, refuses `conversation`, and expands a `previous_response_id` continuation into a new
/// request stream that replaces the exchange's. `view` is the client's request view, read from
/// the exchange's request stream; `request_id` is the exchange's codec request id.
///
/// Returns the expanded request view for a continuation, `None` for any other request.
pub fn begin(
    streams: &Registry,
    exchange: &mut Exchange,
    view: RequestView<'_>,
    request_id: LocalId,
) -> Result<Option<(Document, Media)>, Refused> {
    let doc = view.document;
    let x = doc.required(doc.root(), "x_ck").map_err(|_| Refused::Defect)?;
    if conversation(doc, x).map_err(|_| Refused::Defect)? {
        return Err(Refused::Conversation);
    }
    let stored = match doc.member(doc.root(), "store") {
        Ok(Some(store)) => !matches!(doc.node(store).map(|n| n.value), Ok(Value::Bool(false))),
        Ok(None) => true,
        Err(_) => return Err(Refused::Defect),
    };
    let previous = previous(doc, x).map_err(|_| Refused::Defect)?;
    exchange.turn = Some(Turn {
        id: ResponseId::random(),
        stored,
        continued: previous.is_some(),
    });
    let Some(previous) = previous else {
        return Ok(None);
    };
    let found = previous
        .and_then(|id| streams.responses().lookup(&exchange.owner, id))
        .ok_or(Refused::NotFound)?;
    let store = streams.store().map_err(stored_error)?;
    let mut target = store.create().map_err(stored_error)?;
    let identity = Identity {
        request_id,
        stream_id: request_id,
        attempt: 0,
        direction: Direction::Request,
    };
    let Found {
        mut request,
        mut response,
        provider,
        credential,
    } = found;
    let expanded = (|| {
        let mut request_records = store.reader(request.id(), 0)?;
        let mut response_records = store.reader(response.id(), 0)?;
        let stored = Stored {
            request: (&mut request, &mut request_records),
            response: (&mut response, &mut response_records),
            producer: provenance::Producer {
                provider: &provider,
                credential: &credential,
            },
        };
        history::expand(&mut target, identity, view, stored)?;
        let mut records = store.reader(target.id(), 0)?;
        codec::chat::request::read_request(&mut target, &mut records)
    })();
    match expanded {
        Ok(expanded) => {
            let mut client = std::mem::replace(&mut exchange.request, target);
            client.finish(ck_stream::Terminal::Gone);
            Ok(Some(expanded))
        }
        Err(error) => {
            target.finish(ck_stream::Terminal::Gone);
            Err(match error {
                codec::Error::Storage(error) => stored_error(error),
                codec::Error::Unsupported | codec::Error::Schema => Refused::Unsupported,
                _ => Refused::Defect,
            })
        }
    }
}

/// A stored turn that expired while it was read is not found; any other storage failure is
/// exhaustion.
fn stored_error(error: ck_stream::Error) -> Refused {
    match error {
        ck_stream::Error::Gone => Refused::NotFound,
        ck_stream::Error::StorageExhausted | ck_stream::Error::Io(_) => Refused::Storage,
        _ => Refused::Defect,
    }
}

/// Whether the request names a `conversation`.
fn conversation(doc: &Document, x: usize) -> codec::Result<bool> {
    let Some(responses) = doc
        .member(x, "provider_parameters")?
        .map(|providers| doc.member(providers, "responses"))
        .transpose()?
        .flatten()
    else {
        return Ok(false);
    };
    Ok(match doc.member(responses, "conversation")? {
        Some(value) => !matches!(doc.node(value)?.value, Value::Null),
        None => false,
    })
}

/// The `previous_response_id` the request continues: `None` without one, `Some(None)` for an id
/// cooker never issues.
fn previous(doc: &Document, x: usize) -> codec::Result<Option<Option<ResponseId>>> {
    let Some(id) = doc
        .member(x, "session")?
        .map(|session| doc.member(session, "previous_response_id"))
        .transpose()?
        .flatten()
    else {
        return Ok(None);
    };
    if matches!(doc.node(id)?.value, Value::Null) {
        return Ok(None);
    }
    let mut text = String::new();
    let mut chars = doc.chars(id)?;
    while let Some(c) = chars.next_scalar()? {
        if text.len() > 64 {
            return Ok(Some(None));
        }
        text.push(c);
    }
    Ok(Some(ResponseId::parse(&text)))
}
