//! Responses WebSocket turns on the execution contract (`docs/EXECUTION-CONTRACT.md` §12,
//! [`Turns`]): one turn of a downstream connection on the connection's pooled upstream socket.
//!
//! - Request. The frame is the upload HTTP generation encodes, shaped with the WebSocket rules
//!   (`codex_exec::Shape`, [`Call::Websocket`]) and sent as one `response.create` with the old
//!   path's handshake headers. The request carries the expanded history, so the frame has no
//!   `previous_response_id` and nothing about earlier turns is kept.
//! - Responses. Each upstream response is its own attempt stream and usage scope. The turn's
//!   body carries its first response's frames, each as one SSE `data:` event through the Codex
//!   event rules (`codex_events`) into the Responses ingress, and ends at that response's
//!   terminal. A response the upstream starts afterwards, such as the automatic successor of
//!   accepted steering, comes from [`TurnControl::next`] and opens on its own attempt.
//! - Failures. An `error` frame fails the attempt as `ws_error` classifies it
//!   (`tests/fixtures/codex_ws_errors.json`); a lost socket fails it with transport scope. A
//!   turn never dials or sends its request again; the next turn dials when the socket is gone.
//!   An upgrade the upstream refuses with 426 sent nothing, so that turn's request goes over
//!   HTTP (`CodexExecutor::generate_http`) with no socket control, as the official client
//!   falls back (codex-rs `core/src/client.rs`).
//! - Control. Steering and interrupt frames go to the turn's socket; frames outside any
//!   response come back as received. Cancel interrupts the response in flight and closes the
//!   socket, which never serves another turn while a response is in flight.
//! - Turns come from the Responses WebSocket, so the client is always a Responses client:
//!   no Claude reasoning replay applies.
//!
//! The turn holds its connection's socket until its control and bodies are dropped; the next
//! turn on the connection waits for that.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use ck_core::exec::{ExecError, FailureScope, UsageSink};
use ck_core::format::Format;
use futures_util::StreamExt;
use http::HeaderMap;
use tokio::sync::{OwnedMutexGuard, mpsc};

use super::{
    Read, Session, Target, UNEXPECTED_BINARY, Upstream, request_frame, transport, turn_error, ws_error, ws_url,
};
use crate::codex::CodexExecutor;
use crate::codex_capture::Wire;
use crate::codex_events::{self as events, Backend, Events};
use crate::codex_exec::{CodexIngress, Shape, encode, generation, report};
use crate::codex_json::set_str;
use crate::codex_quota::{self, QuotaSignals};
use crate::codex_request::{self as request, Call, Inbound, Settings};
use crate::session::{CancelTurn, Turn, Turns};
use crate::types::{self, Attempt, Body, Request, Streams};

impl Turns for CodexExecutor {
    type Cancel = TurnControl;

    async fn turn(
        &self,
        attempt: &Attempt<'_>,
        request: &mut Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Turn<CodexIngress, TurnControl>, ExecError> {
        crate::codex_siwc::require_token(attempt.credential)?;
        let connection = connection(request)?;
        let upload = encode(&request.validated(), streams.request, true)?;
        let shape = Shape::new(attempt, request, &upload)?;
        let (view, settings) = shape.view();
        let shaped = shape.generate(&view, &settings, Call::Websocket, &self.replay)?;
        let inbound = shape.inbound();
        let headers = request::ws_headers(
            &view,
            &settings,
            request.client.headers,
            &shaped.text,
            &shaped.base_model,
            shaped.cache.as_deref(),
            inbound.native(),
        );
        let steer = SteerRules::new(attempt, request, &inbound, view.siwc, &settings);
        let url = ws_url(&format!("{}/responses", view.base_url))?;
        let target = Target::new(attempt.credential, url, attempt.cfg);
        let usage = request.client.usage;
        report(usage, true, &shaped.text);
        let frame = request_frame(shaped.text);
        let wire = Wire::new(usage.capture(), attempt.credential);
        let scratch = streams.response.allocator();
        let ingress = generation(streams.response, attempt)?;

        let session = self.ws.session(connection);
        let guard = session.turn.clone().lock_owned().await;
        wire.ws_request(&target.url, &headers, frame.as_bytes());
        let cooling = settings.model_level_cooling;
        let (conn, handshake) = match self
            .ensure(&session, &target, &headers, &shaped.base_model, cooling, Some(&wire))
            .await
        {
            Ok(dialed) => dialed,
            // The official client's transport fallback: a refused upgrade sent nothing, so the
            // turn's request, which carries the expanded history, goes over HTTP instead.
            // ponytail: the next turn dials again, where the official client keeps HTTP for the
            // rest of its session; record the refusal on `Session` if the extra handshake matters.
            Err(error) if error.status == 426 => {
                let upstream = self.generate_http(&shape, Call::Stream, scratch, ingress).await?;
                return Ok(Turn { upstream, cancel: None });
            }
            Err(error) => return Err(error),
        };
        if let Some(handshake) = &handshake {
            wire.ws_handshake(101, handshake);
            self.quota()
                .observe(&attempt.credential.id, &shaped.base_model, handshake);
        }
        usage.round_trip_started();
        let line = Arc::new(Line {
            reads: Arc::new(tokio::sync::Mutex::new(conn.activate())),
            observed: Mutex::new(handshake.clone().unwrap_or_default()),
            steered: AtomicBool::new(false),
            session,
            conn,
            credential: attempt.credential.id.clone(),
            model: shaped.base_model,
            quota: self.quota_handle(),
            wire,
            restore: shaped.optimized,
            cooling,
            steer,
            _turn: guard,
        });
        line.conn.link.lock().expect("link").awaiting = true;
        if let Err(error) = line.conn.send(frame).await {
            line.wire.ws_exec_error("send", &error);
            line.close();
            return Err(error);
        }
        let frames = Frames::turn(&line, usage.clone());
        Ok(Turn {
            upstream: types::Upstream {
                status: 200,
                headers: handshake.unwrap_or_default(),
                body: frames.body(scratch),
                ingress,
            },
            cancel: Some(TurnControl { line }),
        })
    }
}

/// The connection a turn belongs to: its execution session.
fn connection<'a>(request: &Request<'a>) -> Result<&'a str, ExecError> {
    request
        .client
        .execution_session
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| {
            ExecError::local(
                500,
                FailureScope::Request,
                "codex websockets executor: a turn needs its connection's execution session",
            )
        })
}

/// What the payload rules read for the turn's steering frames, as the old path's
/// `steer_rules` reads it from the turn's request.
struct SteerRules {
    model: String,
    requested: String,
    source: Format,
    headers: HeaderMap,
    route: String,
    /// A Sign in with ChatGPT credential: steering frames lose their `configuration_update` items.
    siwc: bool,
    settings: Settings,
}

impl SteerRules {
    fn new(
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        inbound: &Inbound<'_>,
        siwc: bool,
        settings: &Settings,
    ) -> Self {
        Self {
            model: attempt.model.to_owned(),
            requested: inbound.requested_model.to_owned(),
            source: inbound.source,
            headers: request.client.headers.clone(),
            route: request.client.route.to_owned(),
            siwc,
            settings: settings.clone(),
        }
    }

    fn apply(&self, frame: &[u8]) -> String {
        let mut payload = String::from_utf8_lossy(frame).into_owned();
        if self.siwc {
            let stripped = ck_wire::thinking::strip_configuration_update_items(payload.as_bytes());
            if stripped != payload.as_bytes() {
                payload = String::from_utf8_lossy(&stripped).into_owned();
            }
        }
        let inbound = Inbound {
            model: &self.model,
            requested_model: &self.requested,
            source: self.source,
            response: self.source,
            payload: &[],
            headers: &self.headers,
            request_path: &self.route,
            resolved: None,
            execution_session: None,
            derived_session: None,
            principal: "",
        };
        request::steer_rules_for(&inbound, &self.settings, payload)
    }
}

/// One turn's hold on its connection's socket, shared by its control and response bodies.
struct Line {
    session: Arc<Session>,
    conn: Arc<Upstream>,
    /// The socket's frames for this turn. A response body holds the lock while it reads.
    reads: Arc<tokio::sync::Mutex<mpsc::Receiver<Read>>>,
    /// Handshake headers plus the quota headers of the socket's events.
    observed: Mutex<HeaderMap>,
    /// The turn wrote a steer: a successor may start on the socket at any time.
    steered: AtomicBool,
    credential: String,
    /// The upstream base model, the key of per-model quota snapshots.
    model: String,
    quota: Arc<QuotaSignals>,
    wire: Wire,
    /// The request renamed the `collaboration` namespace upstream.
    restore: bool,
    cooling: bool,
    steer: SteerRules,
    /// The connection's turn lock (`reqMu`), released when the last holder is dropped.
    _turn: OwnedMutexGuard<()>,
}

impl Line {
    /// Captures one upstream event and its quota headers, before names are restored.
    fn observe(&self, payload: &str) {
        self.wire.ws_response(payload);
        if let Some(headers) = codex_quota::event_headers(payload) {
            let mut observed = self.observed.lock().expect("observed headers");
            codex_quota::merge(&mut observed, &headers);
            self.quota.observe(&self.credential, &self.model, &observed);
        }
    }

    /// Ends the socket: what it still carries cannot be told apart from a later turn's.
    fn close(&self) {
        self.session.invalidate(&self.conn);
    }

    /// The socket's loss, as a turn reads it.
    fn lost(&self) -> ExecError {
        let lost = self.conn.link.lock().expect("link").lost.clone();
        lost.map_or_else(
            || transport("codex websockets executor: session read channel closed"),
            |lost| turn_error(&lost),
        )
    }
}

/// One upstream response's frames, read from the socket until its terminal.
struct Frames {
    line: Arc<Line>,
    reads: OwnedMutexGuard<mpsc::Receiver<Read>>,
    /// A successor's `response.created`, read before its body opened.
    first: Option<String>,
    /// The turn's request-side facts until the first frame; a successor has none.
    usage: Option<UsageSink>,
    done: bool,
}

impl Drop for Frames {
    fn drop(&mut self) {
        if !self.done {
            self.line.close();
        }
    }
}

impl Frames {
    /// The turn's own response, read from the turn's first frame.
    fn turn(line: &Arc<Line>, usage: UsageSink) -> Self {
        let reads = line
            .reads
            .clone()
            .try_lock_owned()
            .expect("a new turn's frames are free");
        Self::new(line.clone(), reads, None, Some(usage))
    }

    fn new(
        line: Arc<Line>,
        reads: OwnedMutexGuard<mpsc::Receiver<Read>>,
        first: Option<String>,
        usage: Option<UsageSink>,
    ) -> Self {
        Self {
            line,
            reads,
            first,
            usage,
            done: false,
        }
    }

    /// The response through the Codex event rules, as the attempt's upstream body.
    fn body(self, scratch: ck_stream::Writer) -> Body {
        let line = &self.line;
        let backend = Backend::Codex {
            model_level_cooling: line.cooling,
        };
        let rules = Events::new(scratch, line.restore, backend, None);
        let frames = futures_util::stream::unfold(self, |mut frames| async move {
            let item = frames.next().await?;
            Some((item, frames))
        });
        events::body(frames.boxed(), rules)
    }

    async fn next(&mut self) -> Option<Result<Bytes, ExecError>> {
        while !self.done {
            let text = match self.first.take() {
                Some(text) => text,
                None => match self.reads.recv().await {
                    Some(Read::Text(text)) => text,
                    Some(Read::Failed(error)) => return Some(Err(self.lost(error))),
                    None => {
                        let error = self.line.lost();
                        return Some(Err(self.lost(error)));
                    }
                },
            };
            let payload = text.trim();
            if payload.is_empty() {
                continue;
            }
            if let Some(usage) = self.usage.take() {
                usage.first_byte();
            }
            self.line.observe(payload);
            if let Some(error) = ws_error(payload, self.line.cooling) {
                self.line.wire.ws_exec_error("upstream_error", &error);
                self.done = true;
                self.line.close();
                return Some(Err(error));
            }
            match gjson::get(payload, "type").str() {
                "response.completed" | "response.incomplete" => self.done = true,
                // The event rules fail the attempt; the old path drops the socket with it.
                "response.failed" | "error" => {
                    self.done = true;
                    self.line.close();
                }
                _ => {}
            }
            return Some(Ok(Bytes::from(format!("data: {payload}\n\n"))));
        }
        None
    }

    /// The socket failed while the response was in flight; the reader already dropped it.
    fn lost(&mut self, error: ExecError) -> ExecError {
        let binary = error.body.as_ref() == UNEXPECTED_BINARY.as_bytes();
        self.line
            .wire
            .ws_exec_error(if binary { "unexpected_binary" } else { "read" }, &error);
        self.done = true;
        error
    }
}

/// The control of one Codex turn ([`Turns::Cancel`]).
#[derive(Clone)]
pub struct TurnControl {
    line: Arc<Line>,
}

/// What the turn's socket carries after a response ended.
pub enum Next {
    /// A response the upstream started on its own, such as the automatic successor of
    /// accepted steering. It is its own attempt stream and usage scope ([`Successor::open`]).
    Response(Successor),
    /// A frame outside any response, as received: a steering or interrupt acknowledgement,
    /// rate limits or an error.
    Event(Bytes),
}

/// A response the upstream started after the turn's own; dropping it unopened closes the
/// socket, since its frames are still in flight.
pub struct Successor {
    frames: Frames,
}

impl Successor {
    /// The successor's frames, from its `response.created`, on `attempt`'s response stream.
    pub fn open(self, attempt: &Attempt<'_>, streams: Streams<'_>) -> Result<types::Upstream<CodexIngress>, ExecError> {
        let scratch = streams.response.allocator();
        let ingress = generation(streams.response, attempt)?;
        Ok(types::Upstream {
            status: 200,
            headers: HeaderMap::new(),
            body: self.frames.body(scratch),
            ingress,
        })
    }
}

impl TurnControl {
    /// Writes a client's `response.steer` to the turn's socket as the old path does: a Sign
    /// in with ChatGPT credential drops its `configuration_update` items, then the payload
    /// rules apply. The server checks the frame and forwards steering only under
    /// `response-steering`.
    pub async fn steer(&self, frame: &[u8]) -> Result<(), ExecError> {
        let payload = self.line.steer.apply(frame);
        self.line.steered.store(true, Ordering::Release);
        self.line.wire.ws_frame(&self.line.conn.target.url, payload.as_bytes());
        self.written(self.line.conn.send(payload).await)
    }

    /// Writes a client's `response.interrupt` to the turn's socket unchanged. One for a
    /// response this socket already finished is done without a write, as on the old path.
    pub async fn interrupt(&self, frame: &[u8]) -> Result<(), ExecError> {
        let id = ck_wire::json::get(frame, "response_id").bytes();
        let finished =
            std::str::from_utf8(&id).is_ok_and(|id| self.line.conn.link.lock().expect("link").terminal.contains(id));
        if finished {
            return Ok(());
        }
        self.written(self.line.conn.send_raw(Bytes::copy_from_slice(frame)).await)
    }

    /// A failed or timed-out write may leave a partial frame, so the socket goes.
    fn written(&self, sent: Result<(), ExecError>) -> Result<(), ExecError> {
        sent.inspect_err(|_| self.line.close())
    }

    /// The next frame on the turn's socket once its response body ended: it waits while a
    /// body reads. `None` once the socket is gone, or when the next turn on the connection
    /// took it.
    pub async fn next(&self) -> Option<Next> {
        let mut reads = self.line.reads.clone().lock_owned().await;
        loop {
            let text = match reads.recv().await {
                Some(Read::Text(text)) => text,
                Some(Read::Failed(_)) | None => return None,
            };
            let payload = text.trim();
            if payload.is_empty() {
                continue;
            }
            if gjson::get(payload, "type").str() == "response.created" {
                let frames = Frames::new(self.line.clone(), reads, Some(text), None);
                return Some(Next::Response(Successor { frames }));
            }
            self.line.observe(payload);
            return Some(Next::Event(Bytes::copy_from_slice(payload.as_bytes())));
        }
    }
}

impl CancelTurn for TurnControl {
    /// The client went away. The response live on the socket, as its reader last read it
    /// (the turn's own or a successor, consumed or not), gets the transport's cancel
    /// command, `response.interrupt` discarding partial items, and the socket closes. With
    /// none live the socket still closes while the turn's create awaits its
    /// `response.created`, or after the turn steered, since a successor may yet start; an
    /// idle socket stays for the next turn.
    async fn cancel(self) {
        let (live, awaiting) = {
            let link = self.line.conn.link.lock().expect("link");
            (link.live.clone(), link.awaiting)
        };
        if live.is_none() && !awaiting && !self.line.steered.load(Ordering::Acquire) {
            return;
        }
        if let Some(id) = live {
            let frame = set_str(r#"{"type":"response.interrupt"}"#, "response_id", &id);
            let _ = self
                .line
                .conn
                .send(set_str(&frame, "mode", "discard_partial_items"))
                .await;
        }
        self.line.close();
    }
}
