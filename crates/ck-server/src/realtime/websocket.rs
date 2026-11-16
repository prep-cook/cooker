//! Responses WebSocket: `GET /v1/responses` and `GET /backend-api/codex/responses`
//! (E-PLAN §2.4.1, the keeper model; S1 live standard §2.3).
//!
//! - Every `response.create` (or `response.append`) is one Responses exchange on the codec path
//!   `POST /v1/responses` takes: cooker issues the response id, a `previous_response_id` expands
//!   the stored history, and every attempt receives that history, so a continuation fails over
//!   to any candidate. A Codex or xAI credential with `websockets` runs the turn on its upstream
//!   socket (`ck_exec::Executors::start_turn`). The turn's SSE events go out as text frames.
//! - Turns run in order. While one runs, one more create waits; a further one gets an overload
//!   `error`. A `response.interrupt` goes to the in-flight turn's upstream socket.
//! - A turn that fails before it started gets an `error` frame with its status; one that fails
//!   after gets the egress's `response.failed`. The socket serves the next turn either way.
//! - A `store: false` turn is pinned to this socket (`responses_state`): only its last such turn
//!   continues, and only here. The pin goes when the connection ends.
//! - A client close cancels the turn in flight upstream (Settled 56); accepted history stays.
//! - The connection's end is logged once as `responses_ws_end`, with the close code and never
//!   the close reason (R-13).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use axum::Extension;
use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::extract::ws::rejection::WebSocketUpgradeRejection;
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use ck_core::exec::{Caller, Operation};
use ck_core::format::Format;
use ck_translate::codec;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;

use crate::http::egress::{Framing, Output, Protocol};
use crate::inference::dispatch::{self, Call, Served, Socket};
use crate::realtime::websocket_requests::{APPEND, CREATE, delete, error_payload, field, set_raw};
use crate::runtime::state::Runtime;

pub fn routes() -> Router<Arc<Runtime>> {
    Router::new()
        .route("/v1/responses", get(upgrade))
        .route("/backend-api/codex/responses", get(upgrade))
}

const TURN_STATE: &str = "x-codex-turn-state";
// ponytail: 64 MiB (the HTTP body cap) bounds one client message.
const MAX_MESSAGE: usize = crate::MAX_REQUEST_BYTES;
/// Creates waiting behind the turn in flight (`K:README.md:55`).
const WAITING: usize = 1;
/// Close code 1011: the turn's delivery broke inside cooker.
const CLOSE_INTERNAL: u16 = 1011;

/// The connection's write half, shared with the reader, which writes the errors it answers.
type Writer = Arc<tokio::sync::Mutex<SplitSink<WebSocket, Message>>>;
/// The upgrade request's request-log timeline; `None` when request logging is off.
type Timeline = Option<Arc<crate::observability::request_logging::WebsocketLog>>;

/// gorilla `returnError` for a request that is not a websocket handshake: plain status
/// text and the supported version.
pub(crate) fn upgrade_rejected() -> Response {
    let mut response = (StatusCode::BAD_REQUEST, "Bad Request\n").into_response();
    let h = response.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    h.insert("x-content-type-options", HeaderValue::from_static("nosniff"));
    h.insert(header::SEC_WEBSOCKET_VERSION, HeaderValue::from_static("13"));
    response
}

#[allow(clippy::too_many_arguments)]
async fn upgrade(
    State(rt): State<Arc<Runtime>>,
    Extension(caller): Extension<Caller>,
    peer: dispatch::Peer,
    matched: axum::extract::MatchedPath,
    headers: HeaderMap,
    owner: Option<Extension<crate::net::listener::ConnectionWork>>,
    ws: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Response {
    let Ok(ws) = ws else {
        return upgrade_rejected();
    };
    // Sticky turn state survives reconnects.
    let turn_state = headers
        .get(TURN_STATE)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .and_then(|v| HeaderValue::from_str(v).ok());
    let mut connection = Connection {
        rt,
        caller,
        headers,
        request_path: matched.as_str().to_owned(),
        peer: dispatch::peer(peer),
        socket: Arc::new(Socket::new()),
        capture: Default::default(),
        request_id: crate::observability::api::current_request_id(),
        timeline: None,
    };
    // Every turn's upstream capture and the WebSocket timeline go to the upgrade's log.
    if let Some(log) = crate::observability::request_logging::current() {
        connection.capture = log.capture_sink();
        connection.timeline = log.detach_websocket().map(Arc::new);
    }
    let mut response = crate::net::listener::websocket(
        ws.max_message_size(MAX_MESSAGE).max_frame_size(MAX_MESSAGE),
        owner,
        move |socket| connection.run(socket),
    );
    if let Some(turn_state) = turn_state {
        response.headers_mut().insert(TURN_STATE, turn_state);
    }
    response
}

struct Connection {
    rt: Arc<Runtime>,
    caller: Caller,
    headers: HeaderMap,
    request_path: String,
    peer: Option<std::net::SocketAddr>,
    socket: Arc<Socket>,
    capture: ck_core::exec::CaptureSink,
    request_id: Option<String>,
    timeline: Timeline,
}

/// The connection's write half, its timeline and its counters.
#[derive(Clone)]
struct Sink {
    writer: Writer,
    timeline: Timeline,
    counts: Arc<Counts>,
}

#[derive(Default)]
struct Counts {
    messages_in: AtomicU64,
    bytes_in: AtomicU64,
    messages_out: AtomicU64,
    bytes_out: AtomicU64,
}

impl Sink {
    /// One request-log timeline part (`websocket.<kind>`).
    fn event(&self, kind: &str, payload: &[u8]) {
        let Some(timeline) = self.timeline.as_deref() else {
            return;
        };
        let payload = payload.trim_ascii();
        if payload.is_empty() {
            return;
        }
        let header = format!(
            "Timestamp: {}\nEvent: websocket.{kind}\n",
            crate::observability::request_logging::timestamp(chrono::Local::now())
        );
        let _ = timeline.append_part(header.as_bytes(), payload);
    }

    /// Writes one text frame; `false` once the client cannot take it.
    async fn text(&self, payload: String) -> bool {
        self.event("response", payload.as_bytes());
        self.counts.messages_out.fetch_add(1, Ordering::Relaxed);
        self.counts.bytes_out.fetch_add(payload.len() as u64, Ordering::Relaxed);
        self.writer
            .lock()
            .await
            .send(Message::Text(payload.into()))
            .await
            .is_ok()
    }

    async fn close(&self, code: u16) {
        let close = Message::Close(Some(CloseFrame {
            code,
            reason: "".into(),
        }));
        let _ = tokio::time::timeout(crate::inference::sessions::CLOSE_WITHIN, async {
            self.writer.lock().await.send(close).await
        })
        .await;
    }
}

/// Why a connection ended (S1 live standard §2.2, `cause`).
#[derive(Clone, Copy)]
enum End {
    /// The client sent a close frame, with its code when it had one.
    ClientClose(Option<u16>),
    /// The client went away without one, or a write found it gone.
    ClientGone,
    /// A turn's delivery broke inside cooker; the connection closed with 1011.
    RelayError,
}

impl End {
    fn cause(self) -> &'static str {
        match self {
            Self::ClientClose(_) => "client_close",
            Self::ClientGone => "client_gone",
            Self::RelayError => "relay_error",
        }
    }

    fn initiator(self) -> &'static str {
        match self {
            Self::ClientClose(_) | Self::ClientGone => "client",
            Self::RelayError => "gateway",
        }
    }

    fn close_code(self) -> Option<u16> {
        match self {
            Self::ClientClose(code) => code,
            Self::ClientGone => None,
            Self::RelayError => Some(CLOSE_INTERNAL),
        }
    }
}

impl Connection {
    async fn run(self, websocket: WebSocket) {
        let started = Instant::now();
        let (writer, stream) = websocket.split();
        let sink = Sink {
            writer: Arc::new(tokio::sync::Mutex::new(writer)),
            timeline: self.timeline.clone(),
            counts: Arc::default(),
        };
        let (turns, mut waiting) = mpsc::channel(WAITING);
        let mut reader = tokio::spawn(read_input(stream, turns, sink.clone(), self.socket.clone()));
        let mut cancelled = false;
        let end = loop {
            let frame = tokio::select! {
                biased;
                end = &mut reader => break end.unwrap_or(End::ClientGone),
                frame = waiting.recv() => frame,
            };
            let Some(frame) = frame else {
                break (&mut reader).await.unwrap_or(End::ClientGone);
            };
            let turn = self.turn(&sink, frame);
            tokio::pin!(turn);
            let ended = tokio::select! {
                biased;
                end = &mut reader => Some(end.unwrap_or(End::ClientGone)),
                ended = &mut turn => ended,
            };
            if let Some(end) = ended {
                // The turn in flight is cancelled upstream before its future is dropped.
                cancelled = self.socket.cancel().await;
                break match end {
                    // A write found the client gone; its close, if it sent one, is the cause.
                    End::ClientGone if !reader.is_finished() => {
                        tokio::time::timeout(crate::inference::sessions::CLOSE_WITHIN, &mut reader)
                            .await
                            .map_or(End::ClientGone, |end| end.unwrap_or(End::ClientGone))
                    }
                    end => end,
                };
            }
            self.socket.ended();
            // A finished turn wakes idle work such as the heap trim (one atomic load).
            ck_wire::idle::activity();
        };
        if let End::RelayError = end {
            sink.close(CLOSE_INTERNAL).await;
        }
        reader.abort();
        self.rt.executors.close_session(&self.socket.session());
        self.rt.streams().responses().unpin(self.socket.key);
        log_end(end, started, &sink.counts, cancelled, self.request_id.as_deref());
        // The reader has ended or was aborted, so this is the timeline's last reference.
        drop(sink);
        if let Some(timeline) = self.timeline.and_then(Arc::into_inner) {
            timeline.close().await;
        }
    }

    /// Runs one turn; `Some` when the connection ends with it.
    async fn turn(&self, sink: &Sink, frame: Bytes) -> Option<End> {
        sink.event("request", &frame);
        let payload = String::from_utf8_lossy(&frame);
        let body = set_raw(&delete(&payload, "type"), "stream", "true");
        let settings = ck_wire::codex_client::Settings::from_config(&self.rt.config());
        // The spawn_agent model list comes from the Codex client catalog.
        if ck_wire::codex_client::multi_agent_client(&self.headers, settings.optimize_multi_agent_v2) {
            crate::inference::model_updater::codex_client_catalog_wanted();
        }
        let body = ck_wire::codex_client::prepare_responses_request(&self.headers, body.as_bytes(), &settings);
        let call = Call {
            entry: Format::OpenAIResponse,
            response: Format::OpenAIResponse,
            operation: Operation::Generate,
            model: field(&String::from_utf8_lossy(&body), "model"),
            body: Bytes::from(body),
            stream: true,
            alt: None,
            headers: self.headers.clone(),
            caller: self.caller.clone(),
            forced_provider: None,
            selection_model: None,
            execution_session: Some(self.socket.session()),
            request_path: self.request_path.clone(),
            peer: self.peer,
            socket: Some(self.socket.clone()),
            media: None,
        };
        let output = Output {
            protocol: Protocol::Responses,
            operation: codec::Operation::Generate,
            framing: Framing::Sse,
        };
        let trace = dispatch::Trace::with_request_id(self.request_id.clone()).with_capture(self.capture.clone());
        let response = match dispatch::exchange::run(&self.rt, call, output, &trace).await {
            Ok(Served::Committed(response)) => response,
            Ok(Served::Old(_)) => {
                // The old arm serves no Responses candidate (`attempt::run`).
                return (!sink.text(error_payload(500, "", None)).await).then_some(End::ClientGone);
            }
            Err(failure) => {
                return (!sink.text(failure_payload(&failure)).await).then_some(End::ClientGone);
            }
        };
        let mut body = response.into_body().into_data_stream();
        let mut events = Events::default();
        while let Some(chunk) = body.next().await {
            // The body ends abnormally only when the response stream ended without its
            // terminal (shutdown, expiry, a lost producer).
            let Ok(chunk) = chunk else {
                return Some(End::RelayError);
            };
            for payload in events.push(&chunk) {
                if !sink.text(payload).await {
                    return Some(End::ClientGone);
                }
            }
        }
        None
    }
}

/// The `error` frame of a turn that failed before it started, sanitized as the HTTP route's
/// streaming errors are.
fn failure_payload(failure: &dispatch::Failure) -> String {
    let status = failure.status();
    let text = match failure.direct() {
        Some(_) => failure.text(),
        None => crate::inference::openai::sanitize_error_text(status, &failure.text()),
    };
    error_payload(status, &text, None)
}

/// The SSE events of a committed turn's body, as the `data` payloads a text frame carries.
#[derive(Default)]
struct Events {
    pending: Vec<u8>,
}

impl Events {
    fn push(&mut self, chunk: &[u8]) -> Vec<String> {
        self.pending.extend_from_slice(chunk);
        let mut out = Vec::new();
        while let Some(end) = self.pending.windows(2).position(|w| w == b"\n\n") {
            let event: Vec<u8> = self.pending.drain(..end + 2).collect();
            let data: Vec<&[u8]> = event
                .split(|b| *b == b'\n')
                .filter_map(|line| line.strip_prefix(b"data:"))
                .map(|data| data.strip_prefix(b" ").unwrap_or(data))
                .collect();
            if data.is_empty() || data == [b"[DONE]"] {
                continue;
            }
            out.push(String::from_utf8_lossy(&data.join(&b'\n')).into_owned());
        }
        out
    }
}

/// The connection's only reader. Turn requests go to the bounded queue, so one waits behind the
/// turn in flight and a further one is refused; `response.interrupt` goes to the turn's
/// upstream socket at once. Returns why the client went away.
async fn read_input(
    mut stream: SplitStream<WebSocket>,
    turns: mpsc::Sender<Bytes>,
    sink: Sink,
    socket: Arc<Socket>,
) -> End {
    loop {
        let frame = match stream.next().await {
            Some(Ok(Message::Text(text))) => Bytes::from(text),
            Some(Ok(Message::Binary(bytes))) => bytes,
            Some(Ok(Message::Close(frame))) => return End::ClientClose(frame.map(|frame| frame.code)),
            Some(Ok(_)) => continue,
            Some(Err(_)) | None => return End::ClientGone,
        };
        sink.counts.messages_in.fetch_add(1, Ordering::Relaxed);
        sink.counts.bytes_in.fetch_add(frame.len() as u64, Ordering::Relaxed);
        let kind = ck_wire::json::get(&frame, "type");
        let refusal = match &*kind.bytes() {
            _ if kind.kind != ck_wire::json::Kind::String => Some(error_payload(
                400,
                "websocket request requires a string field: type",
                None,
            )),
            b"response.interrupt" => match socket.interrupt(&frame).await {
                Some(Ok(())) => None,
                Some(Err(error)) => Some(error_payload(error.status, &String::from_utf8_lossy(&error.body), None)),
                None => Some(error_payload(
                    400,
                    "no response in flight on an upstream websocket for response.interrupt",
                    None,
                )),
            },
            kind if kind == CREATE.as_bytes() || kind == APPEND.as_bytes() => match turns.try_send(frame) {
                Ok(()) => None,
                Err(mpsc::error::TrySendError::Full(_)) => Some(overloaded()),
                Err(mpsc::error::TrySendError::Closed(_)) => return End::ClientGone,
            },
            kind => Some(error_payload(
                400,
                &format!("unsupported websocket request type: {}", String::from_utf8_lossy(kind)),
                None,
            )),
        };
        if let Some(refusal) = refusal
            && !sink.text(refusal).await
        {
            return End::ClientGone;
        }
    }
}

/// The keeper's answer to a create past the queue (`K:working.go:83`).
fn overloaded() -> String {
    error_payload(
        503,
        r#"{"error":{"type":"server_error","code":"working_set_unavailable","message":"Working capacity unavailable; retry later."}}"#,
        None,
    )
}

/// The one `responses_ws_end` record of a connection (S1 live standard §2.2): its cause, close
/// code and counters, never message content or the close reason (R-13).
fn log_end(end: End, started: Instant, counts: &Counts, cancelled: bool, request_id: Option<&str>) {
    let count = |c: &AtomicU64| c.load(Ordering::Relaxed);
    macro_rules! record {
        ($level:ident) => {
            tracing::$level!(
                target: "ck_server::responses_ws",
                event = "responses_ws_end",
                cause = end.cause(),
                initiator = end.initiator(),
                close_code = end.close_code(),
                request_id,
                duration_ms = started.elapsed().as_millis() as u64,
                messages_in = count(&counts.messages_in),
                bytes_in = count(&counts.bytes_in),
                messages_out = count(&counts.messages_out),
                bytes_out = count(&counts.bytes_out),
                upstream_cancelled = cancelled,
                "responses_ws_end"
            )
        };
    }
    match end {
        End::ClientClose(_) => record!(info),
        End::ClientGone => record!(warn),
        End::RelayError => record!(error),
    }
}

#[cfg(test)]
#[path = "websocket_tests.rs"]
mod tests;
