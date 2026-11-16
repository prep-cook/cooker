//! The Codex upstream event stream on its way into the Responses response ingress
//! (`codex_exec`): Go's per-event Codex rules (codex_executor_stream.go), applied to the
//! upstream body as it arrives, one event at a time.
//!
//! - Data spread over several `data:` lines is joined (`codex_sse`), and each payload is
//!   trimmed and has the client's `collaboration` names restored when the request renamed
//!   them (multi-agent v2). Data that is not UTF-8 ends the body with an upstream failure.
//! - A terminal failure event ends the body with its classified error. A `response.failed`
//!   passes to the ingress first, which publishes its identity and usage and leaves the
//!   `error` record to that error (`Ingress::classified_failures`). An invalid-signature
//!   failure clears the reasoning replay.
//! - Only `response.completed` and `response.incomplete` end the response. An empty terminal
//!   output gets the announced items from scratch leases. Only completed turns enter replay.
//! - The completion counts at the blank line ending its event; nothing after that is read.
//!   A body that ends before it, with or without a completion, is Go's incomplete stream.
//!
//! Everything else passes line by line; the ingress decodes the result.
//!
//! Meta Muse speaks the same wire with its own rules ([`Backend::Meta`]): a failure event is
//! classified as Meta's executor does, only `response.completed` and `response.incomplete`
//! complete the response, and there is no empty-incomplete rule, rename or replay.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use bytes::Bytes;
use ck_core::exec::{ExecError, FailureScope};
use ck_stream::{ScratchLease, Writer};
use futures_util::StreamExt;
use gjson::Kind;

use crate::codex_replay::{Cache, Scope};
use crate::codex_response::{self as response, SCANNER_MAX_LINE};
use crate::types::Body;

/// The per-response Codex rules over one upstream body.
pub(crate) struct Events {
    /// The physical line still waiting for its end.
    line: Vec<u8>,
    sse: crate::codex_sse::DataLines,
    /// Scratch storage on the response stream for the announced items.
    scratch: Writer,
    items: Items,
    /// The request renamed the `collaboration` namespace.
    restore: bool,
    backend: Backend,
    /// Claude clients' reasoning replay for this request.
    replay: Option<(Arc<Cache>, Scope)>,
    /// Processed output not yet passed on.
    out: VecDeque<Piece>,
    phase: Phase,
}

/// Whose rules the upstream events follow.
#[derive(Clone, Copy)]
pub(crate) enum Backend {
    /// The Codex backend (codex_executor_stream.go).
    Codex { model_level_cooling: bool },
    /// Meta Muse (meta_executor.go).
    Meta,
}

impl Backend {
    /// Go's scanner refuses a line past its limit.
    fn too_long(self) -> ExecError {
        match self {
            Self::Codex { .. } => too_long(),
            Self::Meta => ExecError::local(500, FailureScope::Request, "bufio.Scanner: token too long"),
        }
    }

    /// A body that ended before its completion's event did.
    fn incomplete(self) -> ExecError {
        match self {
            Self::Codex { .. } => response::request_scoped(408, response::INCOMPLETE_MESSAGE),
            Self::Meta => ExecError::local(408, FailureScope::Credential, crate::meta::DISCONNECTED),
        }
    }

    fn not_utf8(self) -> ExecError {
        not_utf8(match self {
            Self::Codex { .. } => "codex",
            Self::Meta => "meta",
        })
    }
}

/// Where the body is relative to its completion.
enum Phase {
    Reading,
    /// The completion was read; its event has not ended. Holds the turn to cache then.
    Ending(Option<Vec<Vec<u8>>>),
    Completed,
}

/// Output waiting to be passed on: processed text, or an announced item read from its
/// lease one frame at a time.
enum Piece {
    Text(Vec<u8>),
    Item(ScratchLease, usize),
}

/// One `response.output_item.done` item: its JSON in a lease, and its id.
struct Item {
    json: ScratchLease,
    id: Option<String>,
}

/// The announced items by output index, then those without one.
#[derive(Default)]
struct Items {
    by_index: BTreeMap<i64, Item>,
    fallback: Vec<Item>,
}

impl Items {
    fn len(&self) -> usize {
        self.by_index.len() + self.fallback.len()
    }

    /// The items in completion order, leaving none behind.
    fn drain(&mut self) -> Vec<Item> {
        let by_index = std::mem::take(&mut self.by_index).into_values();
        by_index.chain(std::mem::take(&mut self.fallback)).collect()
    }
}

impl Events {
    pub(crate) fn new(scratch: Writer, restore: bool, backend: Backend, replay: Option<(Arc<Cache>, Scope)>) -> Self {
        Self {
            line: Vec::new(),
            sse: crate::codex_sse::DataLines::new(SCANNER_MAX_LINE),
            scratch,
            items: Items::default(),
            restore,
            backend,
            replay,
            out: VecDeque::new(),
            phase: Phase::Reading,
        }
    }

    fn completed(&self) -> bool {
        matches!(self.phase, Phase::Completed)
    }

    /// Consumes one upstream chunk. Stops at the completion's event end or the first failure.
    fn push(&mut self, mut chunk: &[u8]) -> Result<(), ExecError> {
        while !self.completed() {
            let Some(end) = chunk.iter().position(|b| *b == b'\n') else {
                if self.line.len() + chunk.len() > SCANNER_MAX_LINE {
                    return Err(self.backend.too_long());
                }
                self.line.extend_from_slice(chunk);
                return Ok(());
            };
            let physical = if self.line.is_empty() {
                chunk[..end].to_vec()
            } else {
                let mut line = std::mem::take(&mut self.line);
                line.extend_from_slice(&chunk[..end]);
                line
            };
            chunk = &chunk[end + 1..];
            self.physical(&physical)?;
        }
        Ok(())
    }

    /// The upstream body ended: a last unterminated line and a collected event go out, and
    /// a body that ended before its completion's event did is incomplete.
    fn finish(&mut self) -> Result<(), ExecError> {
        if !self.completed() && !self.line.is_empty() {
            let line = std::mem::take(&mut self.line);
            self.physical(&line)?;
        }
        let mut scanned = Vec::new();
        if !self.completed() {
            self.sse.finish(&mut scanned);
        }
        for item in scanned {
            self.delivered(&item.line)?;
        }
        if self.completed() {
            Ok(())
        } else {
            Err(self.backend.incomplete())
        }
    }

    fn physical(&mut self, raw: &[u8]) -> Result<(), ExecError> {
        let raw = raw.strip_suffix(b"\r").unwrap_or(raw);
        let mut scanned = Vec::new();
        self.sse.push(raw, &mut scanned);
        for item in scanned {
            if self.completed() {
                break;
            }
            self.delivered(&item.line)?;
        }
        Ok(())
    }

    /// One line as the Codex scanner delivers it.
    fn delivered(&mut self, line: &[u8]) -> Result<(), ExecError> {
        // Lines of the completion's event pass as read, so the ingress judges further data.
        if let Phase::Ending(turn) = &mut self.phase {
            if line.is_empty() {
                if let (Some(turn), Some((cache, scope))) = (turn.take(), &self.replay) {
                    crate::codex_replay::keep(cache, scope, &turn);
                }
                self.phase = Phase::Completed;
            }
            self.text(line);
            self.text(b"\n");
            return Ok(());
        }
        let Some(data) = line.strip_prefix(b"data:") else {
            self.text(line);
            self.text(b"\n");
            return Ok(());
        };
        let text = std::str::from_utf8(data).map_err(|_| self.backend.not_utf8())?;
        let payload = response::restore(text.trim(), self.restore);
        self.payload(payload.as_ref())
    }

    /// Go's rules on one data payload, in Go's order; queues the line it becomes.
    fn payload(&mut self, payload: &str) -> Result<(), ExecError> {
        let failure = match self.backend {
            Backend::Codex { model_level_cooling } => self.codex_failure(payload, model_level_cooling),
            Backend::Meta => crate::meta::stream_event_error(payload.as_bytes()),
        };
        let kind = gjson::get(payload, "type");
        if let Some(error) = failure {
            // Only JSON the ingress can parse, so its decoding never replaces this error.
            if kind.str() == "response.failed" && gjson::valid(payload) {
                self.text(b"data: ");
                self.text(payload.as_bytes());
                self.text(b"\n\n");
            }
            return Err(error);
        }
        match kind.str() {
            "response.output_item.done" => self.collect(payload)?,
            kind @ ("response.completed" | "response.incomplete") => {
                let incomplete = kind == "response.incomplete";
                return self.completion(payload.to_owned(), incomplete);
            }
            _ => {}
        }
        self.text(b"data: ");
        self.text(payload.as_bytes());
        self.text(b"\n");
        Ok(())
    }

    /// Classify Codex failure events without changing incomplete terminals.
    fn codex_failure(&mut self, payload: &str, model_level_cooling: bool) -> Option<ExecError> {
        let (error, body) = response::terminal_failure(payload, model_level_cooling)?;
        if let Some((cache, scope)) = &self.replay {
            crate::codex_replay::clear_on_invalid_signature(cache, scope, error.status, body.as_bytes());
        }
        Some(error)
    }

    /// Keeps an announced item in a scratch lease, replacing an earlier one at its index.
    fn collect(&mut self, payload: &str) -> Result<(), ExecError> {
        let item = gjson::get(payload, "item");
        if !matches!(item.kind(), Kind::Object | Kind::Array) {
            return Ok(());
        }
        let json = item.json().as_bytes();
        let mut reservation = self.scratch.begin_scratch(json.len()).map_err(stored)?;
        reservation.write(json).map_err(stored)?;
        let id = item.get("id");
        let id = (id.kind() == Kind::String && !id.str().trim().is_empty()).then(|| id.json().to_owned());
        let item = Item {
            json: reservation.complete().map_err(stored)?,
            id,
        };
        let index = gjson::get(payload, "output_index");
        if index.exists() {
            self.items.by_index.insert(index.i64(), item);
        } else {
            self.items.fallback.push(item);
        }
        Ok(())
    }

    /// Queues the completion: a listed output gets the announced items' ids where it lacks
    /// them (`hydrateCodexCompletedOutputItemIDs`); an empty one gets the items.
    fn completion(&mut self, payload: String, incomplete: bool) -> Result<(), ExecError> {
        let output = gjson::get(&payload, "response.output");
        let listed = output.kind() == Kind::Array && !output.array().is_empty();
        if listed || self.items.len() == 0 {
            let payload = if listed { self.hydrate(payload) } else { payload };
            self.items = Items::default();
            let turn = (!incomplete)
                .then(|| listed_turn(self.replay.as_ref(), &payload))
                .flatten();
            self.phase = Phase::Ending(turn);
            self.text(b"data: ");
            self.text(payload.as_bytes());
            self.text(b"\n");
            return Ok(());
        }
        let payload = crate::codex_json::set_raw(&payload, "response.output", "[]");
        let output = ck_wire::json::get(payload.as_bytes(), "response.output");
        let open = output.index;
        if payload.as_bytes().get(open..open + 2) != Some(b"[]".as_slice()) {
            return Err(defect("completion output not found"));
        }
        let items = self.items.drain();
        let turn = match (&self.replay, incomplete) {
            (Some((_, scope)), false) => {
                // One item on the heap at a time; the turn keeps only what it replays.
                let mut failure = None;
                let copies = items
                    .iter()
                    .map_while(|item| copy(&item.json).map_err(|e| failure = Some(e)).ok());
                let turn = crate::codex_shape::replay_turn(scope, copies);
                if let Some(error) = failure {
                    return Err(error);
                }
                turn
            }
            _ => None,
        };
        self.phase = Phase::Ending(turn);
        self.text(b"data: ");
        self.text(&payload.as_bytes()[..=open]);
        for (i, item) in items.into_iter().enumerate() {
            if i > 0 {
                self.text(b",");
            }
            self.out.push_back(Piece::Item(item.json, 0));
        }
        self.text(&payload.as_bytes()[open + 1..]);
        self.text(b"\n");
        Ok(())
    }

    /// Fills missing ids of a listed output from the items announced at the same index.
    fn hydrate(&self, payload: String) -> String {
        let output = gjson::get(&payload, "response.output");
        let outputs = output.array();
        let mut patched = payload.clone();
        for (i, item) in outputs.iter().enumerate() {
            let id = item.get("id");
            let has_id =
                id.exists() && id.kind() != Kind::Null && (id.kind() != Kind::String || !id.str().trim().is_empty());
            if has_id {
                continue;
            }
            if let Some(done) = self.items.by_index.get(&(i as i64))
                && let Some(done_id) = &done.id
            {
                patched = crate::codex_json::set_raw(&patched, &format!("response.output.{i}.id"), done_id);
            }
        }
        patched
    }

    /// Appends processed text after what is queued.
    fn text(&mut self, bytes: &[u8]) {
        match self.out.back_mut() {
            Some(Piece::Text(text)) => text.extend_from_slice(bytes),
            _ => self.out.push_back(Piece::Text(bytes.to_vec())),
        }
    }

    /// The next queued output: the text read so far, or one frame of an item.
    fn take(&mut self) -> Option<Result<Bytes, ExecError>> {
        match self.out.front_mut()? {
            Piece::Text(text) => {
                let text = std::mem::take(text);
                self.out.pop_front();
                (!text.is_empty())
                    .then(|| Ok(Bytes::from(text)))
                    .or_else(|| self.take())
            }
            Piece::Item(lease, offset) => {
                let chunk = match lease.chunk(*offset) {
                    Ok(chunk) => Bytes::copy_from_slice(chunk),
                    Err(error) => return Some(Err(stored(error))),
                };
                *offset += chunk.len();
                if *offset >= lease.len() {
                    self.out.pop_front();
                }
                Some(Ok(chunk))
            }
        }
    }
}

/// The replay turn of a completion that lists its output.
fn listed_turn(replay: Option<&(Arc<Cache>, Scope)>, payload: &str) -> Option<Vec<Vec<u8>>> {
    let (_, scope) = replay?;
    let items = ck_wire::json::get(payload.as_bytes(), "response.output").array();
    crate::codex_shape::replay_turn(scope, items.iter().map(|item| item.raw().to_vec()))
}

/// A lease's bytes, for the replay turn.
fn copy(lease: &ScratchLease) -> Result<Vec<u8>, ExecError> {
    let mut bytes = Vec::with_capacity(lease.len());
    while bytes.len() < lease.len() {
        bytes.extend_from_slice(lease.chunk(bytes.len()).map_err(stored)?);
    }
    Ok(bytes)
}

/// Go's scanner refuses a line past its limit.
fn too_long() -> ExecError {
    ExecError::local(
        502,
        FailureScope::Transport,
        "codex executor: upstream SSE line too long",
    )
}

/// Upstream bytes that are not UTF-8 text are a failed upstream response, never repaired.
fn not_utf8(executor: &str) -> ExecError {
    ExecError::local(
        502,
        crate::upstream::scope_for(502),
        format!("{executor} executor: upstream event is not valid UTF-8"),
    )
}

/// The attempt failure of a scratch storage error.
fn stored(error: ck_stream::Error) -> ExecError {
    match error {
        ck_stream::Error::StorageExhausted | ck_stream::Error::Io(_) => {
            ExecError::local(503, FailureScope::Request, "stream.storage_exhausted")
        }
        ck_stream::Error::Gone => ExecError::local(410, FailureScope::Request, "stream.gone"),
        error => defect(&format!("scratch storage failed: {error}")),
    }
}

fn defect(what: &str) -> ExecError {
    ExecError::local(500, FailureScope::Request, format!("codex executor: {what}"))
}

/// `upstream` with the Codex rules applied. Processed output is passed on as it is ready;
/// a failure follows the output that preceded it.
pub(crate) fn body(upstream: Body, events: Events) -> Body {
    struct State {
        upstream: Body,
        events: Events,
        failed: Option<ExecError>,
        ended: bool,
    }
    let state = State {
        upstream,
        events,
        failed: None,
        ended: false,
    };
    futures_util::stream::unfold(state, |mut st| async move {
        loop {
            match st.events.take() {
                Some(Ok(out)) => return Some((Ok(out), st)),
                Some(Err(error)) => {
                    st.events.out.clear();
                    st.failed.get_or_insert(error);
                }
                None => {}
            }
            if let Some(error) = st.failed.take() {
                st.ended = true;
                return Some((Err(error), st));
            }
            if st.ended {
                return None;
            }
            let step = match st.upstream.next().await {
                Some(Ok(chunk)) => st.events.push(&chunk),
                Some(Err(error)) => Err(error),
                None => {
                    st.ended = true;
                    st.events.finish()
                }
            };
            st.ended |= st.events.completed();
            if let Err(error) = step {
                st.failed = Some(error);
            }
        }
    })
    .boxed()
}

/// A compaction body with the client's `collaboration` names restored. The body is one
/// JSON value, so restoring reads all of it; without a rename it passes as read.
// ponytail: restoring collects the compaction (bounded by Go's scanner limit); a streaming
// rename would avoid it if compactions grow large.
pub(crate) fn compaction(upstream: Body, restore: bool) -> Body {
    if !restore {
        return upstream;
    }
    futures_util::stream::once(async move {
        let mut upstream = upstream;
        let mut data = Vec::new();
        while let Some(chunk) = upstream.next().await {
            data.extend_from_slice(&chunk?);
            if data.len() > SCANNER_MAX_LINE {
                return Err(too_long());
            }
        }
        let text = std::str::from_utf8(&data).map_err(|_| not_utf8("codex"))?;
        Ok(Bytes::from(response::restore(text, true).into_owned()))
    })
    .boxed()
}

#[cfg(test)]
#[path = "codex_events_tests.rs"]
mod tests;
