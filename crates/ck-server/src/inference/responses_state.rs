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
//!
//! The same entries serve `GET`, `POST .../cancel` and `DELETE /v1/responses/{id}` for their
//! owner, and a buffered `background` request (F30): its turn runs on a stream task under a
//! slot reserved from its start, and the client gets the queued response at once. Every id
//! another caller created, or none created, gets the same not-found answer; nothing is asked
//! upstream. A deleted or cancelled turn is never published, so it cannot be continued.
//!
//! A Responses WebSocket turn sent with `store: false` is pinned to its socket (E-PLAN §2.4.1):
//! only the socket's last such turn is indexed, under the socket's key, and only that socket
//! continues it. `responses.ignore-client-store-false` stores every turn instead, the reconnect
//! path of clients that always send `store: false`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use std::future::Future;

use axum::extract::{Extension, Path, State};
use axum::response::Response;
use ck_core::exec::Caller;
use ck_stream::Writer;
use ck_translate::codec::document::{Document, Value};
use ck_translate::codec::event::{Direction, Identity, LocalId};
use ck_translate::codec::media::Registry as Media;
use ck_translate::codec::{self, RequestView, provenance};
use ck_translate::responses::history::{self, Stored};
use tokio::sync::watch;

use super::dispatch::{Failure, Served};
use super::streams::{Attached, Exchange, Owner, Registry, Shown};
use crate::Runtime;
use crate::http::egress::{Framing, Output, Protocol};

/// A response id cooker issued: `resp_` and 128 random bits in lowercase hexadecimal.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ResponseId([u8; 16]);

impl ResponseId {
    pub(crate) fn random() -> Self {
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

/// The key of one Responses WebSocket connection, scoping the turn its socket pins.
pub type SocketKey = u128;

/// What a Responses exchange records before its first attempt.
#[derive(Clone, Copy)]
pub struct Turn {
    pub id: ResponseId,
    /// The committed turn is indexed for every connection of its owner, and its response shows
    /// `store: true`: the client did not send `store: false`, or the deploy ignores it.
    pub stored: bool,
    /// The socket that pins this turn when it is not `stored`.
    pub socket: Option<SocketKey>,
    /// The client asked for `background`: its response shows `background: true`.
    pub background: bool,
}

impl Turn {
    fn indexed(&self) -> bool {
        self.stored || self.socket.is_some()
    }
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
    /// A buffered background turn, from its start until it is published.
    Background(Box<Background>),
    Published(Box<Entry>),
}

/// A background turn without its response: running, or ended without one.
struct Background {
    owner: Owner,
    /// The model the client asked for, and the creation time, as the pending response shows them.
    model: Box<str>,
    created_at: u64,
    state: Phase,
    /// Aborts the turn's task while it runs.
    task: Option<tokio::task::AbortHandle>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Running,
    Failed,
    Cancelled,
}

impl Background {
    /// A slot no response will fill; a full table drops it.
    fn ended(&self) -> bool {
        self.state != Phase::Running
    }

    fn stop(&mut self, state: Phase) {
        if self.state == Phase::Running {
            self.state = state;
            if let Some(task) = self.task.take() {
                task.abort();
            }
        }
    }
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
    /// The socket that pinned the turn; `None` for a stored turn.
    socket: Option<SocketKey>,
    /// The model alias the response shows, and whether it ran in the background.
    model: Option<Arc<str>>,
    background: bool,
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

    /// Reserves a slot for `id`. A full table first drops every entry whose streams are gone
    /// and every background turn that ended without a response; `false` when it is still full.
    /// A `pending` turn, a buffered background one, keeps the slot reserved at its start, and
    /// finds none once it was deleted.
    pub(crate) fn reserve(&self, capacity: usize, id: ResponseId, pending: bool) -> bool {
        self.slots(capacity, |slots| {
            if pending {
                return matches!(slots.entries.get(&id), Some(Slot::Background(_)));
            }
            slots.insert(id, Slot::Reserved)
        })
    }

    /// Reserves `id` for a background turn of `owner` that asked for `model`.
    fn start(&self, capacity: usize, id: ResponseId, owner: &Owner, model: &str, created_at: u64) -> bool {
        self.slots(capacity, |slots| {
            let background = Background {
                owner: owner.clone(),
                model: model.into(),
                created_at,
                state: Phase::Running,
                task: None,
            };
            slots.insert(id, Slot::Background(Box::new(background)))
        })
    }

    /// Runs `f` on the background turn `id`, if it is still in the table.
    fn background(&self, id: ResponseId, f: impl FnOnce(&mut Background)) {
        let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(Slot::Background(background)) = slots.as_mut().and_then(|slots| slots.entries.get_mut(&id)) {
            f(background);
        }
    }

    /// Publishes `id`. A socket pins only its last turn, so its earlier pin leaves the table.
    fn fill(&self, id: ResponseId, entry: Entry) {
        let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(slots) = slots.as_mut() else {
            return;
        };
        match slots.entries.get(&id) {
            Some(Slot::Reserved) => {}
            Some(Slot::Background(background)) if !background.ended() => {}
            _ => return,
        }
        if let Some(socket) = entry.socket {
            slots.entries.retain(|_, slot| !pinned_by(slot, socket));
        }
        slots.entries.insert(id, Slot::Published(Box::new(entry)));
    }

    /// Drops the turn `socket` pins: its connection ended.
    pub(crate) fn unpin(&self, socket: SocketKey) {
        let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(slots) = slots.as_mut() {
            slots.entries.retain(|_, slot| !pinned_by(slot, socket));
        }
    }

    /// The turn `id` ended without publication: a background turn shows it failed.
    fn release(&self, id: ResponseId) {
        let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(slots) = slots.as_mut() else {
            return;
        };
        match slots.entries.get_mut(&id) {
            Some(Slot::Reserved) => {
                slots.entries.remove(&id);
            }
            Some(Slot::Background(background)) => background.stop(Phase::Failed),
            _ => {}
        }
    }

    /// What `GET`, `cancel` or `DELETE /v1/responses/{id}` finds for `owner`, after `action`.
    /// `None` alike for an unknown id, another owner, a socket-pinned or still-running
    /// foreground turn, and an expired or evicted one, which leaves the table.
    fn resource(&self, owner: &Owner, id: ResponseId, action: Action) -> Option<Resource> {
        let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        let slots = slots.as_mut()?;
        let resource = match slots.entries.get_mut(&id)? {
            Slot::Reserved => return None,
            Slot::Background(background) => {
                if background.owner != *owner {
                    return None;
                }
                if action != Action::Get {
                    background.stop(Phase::Cancelled);
                }
                Resource::Pending {
                    model: background.model.clone(),
                    created_at: background.created_at,
                    state: background.state,
                }
            }
            Slot::Published(entry) => {
                if entry.gone() {
                    slots.entries.remove(&id);
                    return None;
                }
                if entry.owner != *owner || entry.socket.is_some() {
                    return None;
                }
                match action {
                    Action::Cancel if !entry.background => return Some(Resource::NotBackground),
                    Action::Delete => Resource::Deleted,
                    _ => Resource::Stored(Attached {
                        output: Output {
                            protocol: Protocol::Responses,
                            operation: codec::Operation::Generate,
                            framing: Framing::Json,
                        },
                        response: entry.response.allocator(),
                        request: entry.request.allocator(),
                        shown: Shown {
                            id: Some(id.text()),
                            model: entry.model.clone(),
                            stored: true,
                            background: entry.background,
                            gate: None,
                        },
                    }),
                }
            }
        };
        if action == Action::Delete {
            slots.entries.remove(&id);
            return Some(Resource::Deleted);
        }
        Some(resource)
    }

    /// The published turn `id` that `owner` created, on `socket` when a socket pinned it. `None`
    /// alike for an unknown id, another owner or socket, a turn still running, and an expired
    /// or evicted one, which leaves the table.
    fn lookup(&self, owner: &Owner, id: ResponseId, socket: Option<SocketKey>) -> Option<Found> {
        let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        let slots = slots.as_mut()?;
        let Slot::Published(entry) = slots.entries.get(&id)? else {
            return None;
        };
        if entry.gone() {
            slots.entries.remove(&id);
            return None;
        }
        (entry.owner == *owner && (entry.socket.is_none() || entry.socket == socket)).then(|| Found {
            request: entry.request.allocator(),
            response: entry.response.allocator(),
            provider: entry.provider.clone(),
            credential: entry.credential.clone(),
        })
    }
}

impl Slots {
    /// Inserts `slot` under `id`. A full table first drops every entry whose streams are gone
    /// and every background turn that ended without a response; `false` when it is still full.
    fn insert(&mut self, id: ResponseId, slot: Slot) -> bool {
        if self.entries.len() >= self.capacity {
            self.entries.retain(|_, slot| match slot {
                Slot::Published(entry) => !entry.gone(),
                Slot::Background(background) => !background.ended(),
                Slot::Reserved => true,
            });
        }
        if self.entries.len() >= self.capacity {
            return false;
        }
        self.entries.insert(id, slot);
        true
    }
}

fn pinned_by(slot: &Slot, socket: SocketKey) -> bool {
    matches!(slot, Slot::Published(entry) if entry.socket == Some(socket))
}

/// One attempt's publication. Delivery reserves the slot when the attempt commits; the
/// attempt's finalizer reports how its response stream ended. The slot is filled once both
/// happened and the stream ended with `done`, and the gate opens once the slot is decided:
/// filled, refused or released.
pub(crate) struct Publication {
    index: Arc<Index>,
    capacity: usize,
    id: ResponseId,
    /// The turn's slot was reserved at its start ([`background`]).
    pending: bool,
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
        let turn = exchange.turn.filter(Turn::indexed)?;
        let capacity = streams.store().ok()?.limits().header_slots as usize;
        Some(Arc::new(Self {
            index: streams.responses().clone(),
            capacity,
            id: turn.id,
            pending: pending_turn(exchange).is_some(),
            entry: Mutex::new(Progress {
                reserved: false,
                ended: None,
                entry: Some(Entry {
                    owner: exchange.owner.clone(),
                    request: exchange.request.allocator(),
                    response: response.allocator(),
                    provider: producer.provider.into(),
                    credential: producer.credential.into(),
                    socket: turn.socket.filter(|_| !turn.stored),
                    model: exchange.model.clone(),
                    background: turn.background,
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
        if !self.index.reserve(self.capacity, self.id, self.pending) {
            if !self.pending {
                tracing::warn!(target: "ck_dispatch", "history not retained: the response index is full");
            }
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
    /// `background` with `store: false`: nothing could retrieve the response.
    Background,
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
            Self::Background => (
                400,
                "A background response must be stored; send store: true or leave background unset.",
                "invalid_request_error",
                "background",
                "unsupported_parameter",
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

/// How a Responses exchange's turn is kept for continuation.
#[derive(Clone, Copy, Default)]
pub struct Keep {
    /// `responses.ignore-client-store-false`: a `store: false` turn is stored anyway.
    pub ignore_store_false: bool,
    /// The WebSocket connection the turn runs on: it pins a `store: false` turn and continues
    /// the turn it pinned.
    pub socket: Option<SocketKey>,
}

impl Keep {
    /// The deploy's rule and `socket`, the connection a WebSocket turn runs on.
    pub fn new(cfg: &ck_core::config::Config, socket: Option<SocketKey>) -> Self {
        let ignore_store_false = ["responses", "ignore-client-store-false"]
            .iter()
            .try_fold(&cfg.document, |v, k| v.get(*k))
            .and_then(serde_yaml_ng::Value::as_bool)
            .unwrap_or(false);
        Self {
            ignore_store_false,
            socket,
        }
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
    keep: Keep,
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
    // A token count is no turn: it shows no response id, stores nothing and never runs in the
    // background.
    if view.operation != codec::Operation::CountTokens {
        let stored = stored || keep.ignore_store_false;
        let background = asks_background(doc, x).map_err(|_| Refused::Defect)?;
        if background && !stored {
            return Err(Refused::Background);
        }
        exchange.turn = Some(Turn {
            id: ResponseId::random(),
            stored,
            socket: keep.socket,
            background,
        });
    }
    let Some(previous) = previous(doc, x).map_err(|_| Refused::Defect)? else {
        return Ok(None);
    };
    let found = previous
        .and_then(|id| streams.responses().lookup(&exchange.owner, id, keep.socket))
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

/// The Responses provider parameter `name`, if the request names it.
fn responses_parameter(doc: &Document, x: usize, name: &str) -> codec::Result<Option<usize>> {
    let Some(responses) = doc
        .member(x, "provider_parameters")?
        .map(|providers| doc.member(providers, "responses"))
        .transpose()?
        .flatten()
    else {
        return Ok(None);
    };
    doc.member(responses, name)
}

/// Whether the request names a `conversation`.
fn conversation(doc: &Document, x: usize) -> codec::Result<bool> {
    Ok(match responses_parameter(doc, x, "conversation")? {
        Some(value) => !matches!(doc.node(value)?.value, Value::Null),
        None => false,
    })
}

/// Removes `background` from the request every candidate gets, the first decode and every
/// document an attempt rebuilds from the request stream: cooker runs a background turn itself,
/// and the upstream serves an ordinary one, streamed or not as its transport needs.
pub fn withdraw_background(doc: &mut Document) -> codec::Result<()> {
    let Some(x) = doc.member(doc.root(), "x_ck")? else {
        return Ok(());
    };
    let Some(providers) = doc.member(x, "provider_parameters")? else {
        return Ok(());
    };
    let Some(responses) = doc.member(providers, "responses")? else {
        return Ok(());
    };
    doc.remove(responses, "background")?;
    if doc.node(responses)?.first.is_none() {
        doc.remove(providers, "responses")?;
    }
    if doc.node(providers)?.first.is_none() {
        doc.remove(x, "provider_parameters")?;
    }
    Ok(())
}

/// Whether the request asks for `background: true`.
fn asks_background(doc: &Document, x: usize) -> codec::Result<bool> {
    Ok(match responses_parameter(doc, x, "background")? {
        Some(value) => matches!(doc.node(value)?.value, Value::Bool(true)),
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

/// What a request on `/v1/responses/{id}` does.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Action {
    Get,
    Cancel,
    Delete,
}

/// What a request on `/v1/responses/{id}` found.
enum Resource {
    /// A background turn without its response.
    Pending {
        model: Box<str>,
        created_at: u64,
        state: Phase,
    },
    /// A published turn, rendered from its streams.
    Stored(Attached),
    Deleted,
    /// A cancel of a turn that did not run in the background.
    NotBackground,
}

/// A buffered background turn reserved in the index ([`background`]); [`Pending::spawn`]
/// runs it.
pub struct Pending {
    index: Arc<Index>,
    id: ResponseId,
    model: Box<str>,
    created_at: u64,
}

/// Reserves the slot of `exchange`'s turn when it asked for `background` and a buffered body;
/// `None` for any other exchange, which runs as usual. A background turn that asked to
/// stream is delivered live and is retrieved like any stored turn. `model` is the model the
/// client asked for.
pub fn background(streams: &Registry, exchange: &Exchange, model: &str) -> Result<Option<Pending>, Refused> {
    let Some(turn) = pending_turn(exchange) else {
        return Ok(None);
    };
    let capacity = streams.store().map_err(stored_error)?.limits().header_slots as usize;
    let created_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_secs());
    if !streams
        .responses()
        .start(capacity, turn.id, &exchange.owner, model, created_at)
    {
        return Err(Refused::Storage);
    }
    Ok(Some(Pending {
        index: streams.responses().clone(),
        id: turn.id,
        model: model.into(),
        created_at,
    }))
}

/// The turn of `exchange` that runs under a slot reserved from its start: a buffered
/// `background` generation. A streamed one is published as any stored turn, and a compact
/// answers in its request.
fn pending_turn(exchange: &Exchange) -> Option<Turn> {
    let output = exchange.output;
    exchange.turn.filter(|turn| {
        turn.background && output.framing == Framing::Json && output.operation == codec::Operation::Generate
    })
}

impl Pending {
    /// Runs `turn`, the selection loop of the reserved exchange, on a stream task and returns
    /// the queued response for the client. The committed body is dropped unread: its producer
    /// and its index publication go on without a reader. A turn that does not commit shows
    /// failed.
    // ponytail: an old-arm answer has no response stream to retrieve, so it shows failed too;
    // it goes when the old arm does (D9).
    pub fn spawn<F>(self, streams: &Registry, turn: F) -> Response
    where
        F: Future<Output = Result<Served, Failure>> + Send + 'static,
    {
        let index = self.index.clone();
        let id = self.id;
        let task = streams.spawn(async move {
            match turn.await {
                Ok(Served::Committed(_)) => {}
                Ok(Served::Old(_)) => index.release(id),
                Err(failure) => {
                    tracing::info!(target: "ck_dispatch", status = failure.status(), "background response failed");
                    index.release(id);
                }
            }
        });
        let abort = task.abort_handle();
        self.index.background(id, |background| {
            if background.state == Phase::Running {
                background.task = Some(abort);
            }
        });
        pending(id, &self.model, self.created_at, "queued")
    }
}

/// `GET /v1/responses/{id}`: the caller's stored or background response.
pub async fn retrieve(
    State(rt): State<Arc<Runtime>>,
    Extension(caller): Extension<Caller>,
    Path(id): Path<String>,
) -> Response {
    answer(&rt, caller, &id, Action::Get).await
}

/// `POST /v1/responses/{id}/cancel`: stops the caller's background response.
pub async fn cancel(
    State(rt): State<Arc<Runtime>>,
    Extension(caller): Extension<Caller>,
    Path(id): Path<String>,
) -> Response {
    answer(&rt, caller, &id, Action::Cancel).await
}

/// `DELETE /v1/responses/{id}`: forgets the caller's response; it can no longer be continued.
pub async fn delete(
    State(rt): State<Arc<Runtime>>,
    Extension(caller): Extension<Caller>,
    Path(id): Path<String>,
) -> Response {
    answer(&rt, caller, &id, Action::Delete).await
}

async fn answer(rt: &Runtime, caller: Caller, text: &str, action: Action) -> Response {
    // The same owner an exchange records (`dispatch::exchange`).
    let owner = Owner { caller, scope: None };
    let streams = rt.streams();
    let found = ResponseId::parse(text).and_then(|id| Some((id, streams.responses().resource(&owner, id, action)?)));
    let Some((id, resource)) = found else {
        return not_found();
    };
    match resource {
        Resource::Pending {
            model,
            created_at,
            state,
        } => {
            let status = match state {
                Phase::Running => "in_progress",
                Phase::Failed => "failed",
                Phase::Cancelled => "cancelled",
            };
            pending(id, &model, created_at, status)
        }
        Resource::Stored(attached) => super::streams::render(streams, attached)
            .await
            .unwrap_or_else(not_found),
        Resource::Deleted => {
            let body = crate::http::gojson::Obj::new()
                .str("id", id.text().as_str())
                .str("object", "response")
                .raw("deleted", "true")
                .finish();
            crate::http::respond::gin_json(200, body)
        }
        Resource::NotBackground => error(400, "Only a background response can be cancelled."),
    }
}

/// A background response without its output, in the `Response` shape.
fn pending(id: ResponseId, model: &str, created_at: u64, status: &str) -> Response {
    let error = if status == "failed" {
        r#"{"code":"server_error","message":"Response failed"}"#
    } else {
        "null"
    };
    let body = crate::http::gojson::Obj::new()
        .str("id", id.text().as_str())
        .str("object", "response")
        .raw("created_at", &created_at.to_string())
        .str("status", status)
        .raw("background", "true")
        .raw("error", error)
        .raw("incomplete_details", "null")
        .str("model", model)
        .raw("output", "[]")
        .raw("store", "true")
        .finish();
    crate::http::respond::gin_json(200, body)
}

/// Every id the caller cannot see gets this answer, which repeats nothing of the request.
fn not_found() -> Response {
    error(404, "The response was not found.")
}

fn error(status: u16, message: &str) -> Response {
    let detail = crate::http::gojson::Obj::new()
        .str("message", message)
        .str("type", "invalid_request_error")
        .raw("param", "null")
        .raw("code", "null")
        .finish();
    crate::http::respond::gin_json(status, crate::http::gojson::Obj::new().raw("error", &detail).finish())
}
