//! The Codex upstream event stream on its way into the Responses response ingress
//! (`codex_exec`): Go's per-event Codex rules (codex_executor_stream.go), applied to the
//! upstream body as it arrives, one event at a time.
//!
//! - Data spread over several `data:` lines is joined (`codex_sse`), and each payload is
//!   trimmed and has the client's `collaboration` names restored when the request renamed
//!   them (multi-agent v2).
//! - A terminal failure event, or a `response.incomplete` with no output at all, ends the
//!   body with Go's classified error instead of the event, so the attempt fails with the
//!   status, scope and retry hint the scheduler and cooldown need. An invalid-signature
//!   failure clears the reasoning replay.
//! - `response.done` is the completion; a completion whose `output` is empty gets the items
//!   `response.output_item.done` announced, which the ingress reconciles against. A
//!   completed turn is cached for reasoning replay.
//! - Nothing after the completion is read.
//!
//! Everything else passes line by line; the ingress decodes the result.

use std::sync::Arc;

use bytes::Bytes;
use ck_core::exec::{ExecError, FailureScope};
use futures_util::StreamExt;

use crate::codex_replay::{Cache, Scope};
use crate::codex_response::{self as response, OutputItems, SCANNER_MAX_LINE};
use crate::types::Body;

/// The per-response Codex rules over one upstream body.
pub(crate) struct Events {
    /// The physical line still waiting for its end.
    line: Vec<u8>,
    sse: crate::codex_sse::DataLines,
    items: OutputItems,
    saw_delta: bool,
    /// The request renamed the `collaboration` namespace.
    restore: bool,
    model_level_cooling: bool,
    /// Claude clients' reasoning replay for this request.
    replay: Option<(Arc<Cache>, Scope)>,
    /// Processed output not yet passed on.
    out: Vec<u8>,
    completed: bool,
}

impl Events {
    pub(crate) fn new(restore: bool, model_level_cooling: bool, replay: Option<(Arc<Cache>, Scope)>) -> Self {
        Self {
            line: Vec::new(),
            sse: crate::codex_sse::DataLines::new(SCANNER_MAX_LINE),
            items: OutputItems::default(),
            saw_delta: false,
            restore,
            model_level_cooling,
            replay,
            out: Vec::new(),
            completed: false,
        }
    }

    /// Consumes one upstream chunk. Stops at the completion or the first failure.
    fn push(&mut self, mut chunk: &[u8]) -> Result<(), ExecError> {
        while !self.completed {
            let Some(end) = chunk.iter().position(|b| *b == b'\n') else {
                if self.line.len() + chunk.len() > SCANNER_MAX_LINE {
                    return Err(too_long());
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

    /// The upstream body ended: a last unterminated line and a collected event go out.
    fn finish(&mut self) -> Result<(), ExecError> {
        if !self.completed && !self.line.is_empty() {
            let line = std::mem::take(&mut self.line);
            self.physical(&line)?;
        }
        let mut scanned = Vec::new();
        if !self.completed {
            self.sse.finish(&mut scanned);
        }
        for item in scanned {
            self.delivered(&item.line)?;
        }
        Ok(())
    }

    fn physical(&mut self, raw: &[u8]) -> Result<(), ExecError> {
        let raw = raw.strip_suffix(b"\r").unwrap_or(raw);
        let mut scanned = Vec::new();
        self.sse.push(raw, &mut scanned);
        for item in scanned {
            if self.completed {
                break;
            }
            self.delivered(&item.line)?;
        }
        Ok(())
    }

    /// One line as the Codex scanner delivers it.
    fn delivered(&mut self, line: &[u8]) -> Result<(), ExecError> {
        let Some(data) = line.strip_prefix(b"data:") else {
            self.out.extend_from_slice(line);
            self.out.push(b'\n');
            return Ok(());
        };
        let text = String::from_utf8_lossy(data);
        let payload = response::restore(text.trim(), self.restore);
        let payload = self.payload(payload.as_ref())?;
        self.out.extend_from_slice(b"data: ");
        self.out.extend_from_slice(payload.as_bytes());
        // The completion's event ends here: nothing after it is read.
        self.out.extend_from_slice(if self.completed { b"\n\n" } else { b"\n" });
        Ok(())
    }

    /// Go's rules on one data payload, in Go's order.
    fn payload(&mut self, payload: &str) -> Result<String, ExecError> {
        if let Some((error, body)) = response::terminal_failure(payload, self.model_level_cooling) {
            if let Some((cache, scope)) = &self.replay {
                crate::codex_replay::clear_on_invalid_signature(cache, scope, error.status, body.as_bytes());
            }
            return Err(error);
        }
        if response::meaningful_delta(payload) {
            self.saw_delta = true;
        }
        if response::empty_incomplete(payload, self.items.len(), self.saw_delta) {
            return Err(response::request_scoped(502, response::EMPTY_INCOMPLETE_MESSAGE));
        }
        let kind = gjson::get(payload, "type");
        match kind.str() {
            "response.output_item.done" => self.items.collect(payload),
            kind @ ("response.completed" | "response.incomplete" | "response.done") => {
                let incomplete = kind == "response.incomplete";
                let completed = self.items.patch(response::normalize_completion(payload.to_owned()));
                if !incomplete && let Some((cache, scope)) = &self.replay {
                    crate::codex_replay::cache_completed(cache, scope, completed.as_bytes());
                }
                self.completed = true;
                return Ok(completed);
            }
            _ => {}
        }
        Ok(payload.to_owned())
    }

    fn take(&mut self) -> Option<Bytes> {
        (!self.out.is_empty()).then(|| Bytes::from(std::mem::take(&mut self.out)))
    }
}

/// Go's scanner refuses a line past its limit.
fn too_long() -> ExecError {
    ExecError::local(
        502,
        FailureScope::Transport,
        "codex executor: upstream SSE line too long",
    )
}

/// `upstream` with the Codex rules applied. Processed lines are passed on per upstream
/// chunk; a failure follows the output that preceded it.
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
            st.ended |= st.events.completed;
            if let Err(error) = step {
                st.failed = Some(error);
            }
            if let Some(out) = st.events.take() {
                return Some((Ok(out), st));
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
        let restored = response::restore(&String::from_utf8_lossy(&data), true).into_owned();
        Ok(Bytes::from(restored))
    })
    .boxed()
}
