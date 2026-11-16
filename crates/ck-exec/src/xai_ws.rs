//! xAI upstream Responses WebSocket (`docs/EXECUTION-CONTRACT.md` §12, [`Turns`]): one turn
//! of a downstream Responses WebSocket on the client connection's pooled upstream socket,
//! when the credential sets `websockets`.
//!
//! - The turn is an ordinary attempt. Its request carries the expanded history, so the frame
//!   is the upload HTTP generation would send (shaped without `previous_response_id`, with
//!   `store: false`) as one `response.create`. The connection keeps no transcript and no
//!   response-id map; the reasoning replay cache applies exactly as for HTTP generation. A
//!   `compaction_trigger` input item reaches xAI as received; xAI compacts.
//! - Every upstream text frame goes to the attempt's ingress unchanged as one SSE `data:`
//!   event, through the same xAI line rules (`xai_events`) as HTTP generation. The one
//!   exception is the WebSocket mode's own terminal, `response.done`, which goes in as the
//!   `response.completed` the ingress ends a response on.
//! - A lost socket is the attempt's failure. xAI ends every connection after 59 minutes; a
//!   close or drop during a turn ends its body with a transport-scope error, which fails over
//!   before the client commit and is an error after it. The transport never dials or sends
//!   again within a turn; the next turn dials a fresh socket.
//! - A warmup turn (`generate: false`) gets the `response.completed` xAI never sends for it.
//!
//! Sockets live in the Codex executor's pool type ([`codex_ws::Pool`]); xAI keeps its own
//! pool instance, keyed by the client connection's execution session.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use ck_core::credential::Credential;
use ck_core::exec::{CaptureEvent, CaptureSink, ExecError, FailureScope, UpstreamRequest, UsageSink};
use ck_core::format::Format;
use ck_translate::codec::{BodyMode, Operation};
use ck_translate::responses::response::Ingress;
use ck_wire::json as gj;
use futures_util::StreamExt;
use http::{HeaderMap, HeaderName, HeaderValue};
use tokio::sync::{OwnedMutexGuard, mpsc};

use crate::codex_ws::{self, Pool, Read, Session, Target, Upstream};
use crate::openai_compat_payload::ensure_responses_usage_details;
use crate::session::{CancelTurn, Turn, Turns};
use crate::types::{self, Attempt, Client, Request, Streams};
use crate::xai::{MAX_LINE, XaiExecutor, creds};
use crate::xai_auth::DEFAULT_API_BASE_URL;
use crate::xai_events::{Events, XaiIngress};
use crate::xai_exec::{Call, encode, encoding};
use crate::xai_response::{self as response, text};

/// `codexResponsesWebsocketHandshakeTO`.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);
/// Bound on a handshake rejection body.
const MAX_HANDSHAKE_BODY: usize = 64 * 1024;
/// `buildXAIWebsocketWarmupCompletedPayload`'s empty usage.
const EMPTY_USAGE: &str = r#"{"input_tokens":0,"input_tokens_details":{"cached_tokens":0},"output_tokens":0,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":0}"#;

/// Go reads xAI sockets without a deadline (only the Codex reader sets one); the shared
/// reader's deadline is set past any session.
const NO_READ_DEADLINE: Duration = Duration::from_secs(10 * 365 * 24 * 3600);

/// The executor's sockets (`globalXAIWebsocketSessionStore`).
pub(crate) struct Ws {
    pool: Pool,
}

impl Default for Ws {
    fn default() -> Self {
        let mut pool = Pool::default();
        pool.idle = NO_READ_DEADLINE;
        Self { pool }
    }
}

impl Ws {
    /// `CloseExecutionSession`: the downstream connection ended.
    pub(crate) fn close(&self, id: &str) {
        self.pool.close(id.trim());
    }

    /// `closeXAIWebsocketSessionsInStoreForAuthID`: ends a removed credential's sessions.
    pub(crate) fn close_credential(&self, credential: &str) {
        self.pool.close_credential(credential);
    }
}

// --- request and frame helpers ----------------------------------------------------------

/// `xaiWebsocketsEnabled` (and Go's identical `authWebsocketsEnabled`): the
/// `websockets` attribute when it parses, else the metadata value (a bool, or a string
/// that parses).
pub fn websockets_enabled(credential: &Credential) -> bool {
    if let Some(parsed) = credential
        .attributes
        .get("websockets")
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .and_then(crate::xai::parse_bool)
    {
        return parsed;
    }
    match credential.metadata.get("websockets") {
        Some(serde_json::Value::Bool(b)) => *b,
        Some(serde_json::Value::String(s)) => crate::xai::parse_bool(s.trim()).unwrap_or(false),
        _ => false,
    }
}

/// A plain Go error (`fmt.Errorf`), as the manager sees it.
fn plain(message: impl Into<String>) -> ExecError {
    crate::openai_compat::plain_err(message.into())
}

/// `buildXAIResponsesWebsocketURL`.
fn ws_url(http_url: &str) -> Result<String, ExecError> {
    let mut parsed = crate::xai_url::parse(http_url.trim()).map_err(crate::xai_url::statusless)?;
    parsed.scheme = match parsed.scheme.as_str() {
        "http" => "ws".to_owned(),
        "https" => "wss".to_owned(),
        "ws" | "wss" => parsed.scheme.clone(),
        other => {
            return Err(crate::xai_url::statusless(format!(
                "xai websockets executor: unsupported responses websocket URL scheme {}",
                ck_wire::gostr::quote(other)
            )));
        }
    };
    if parsed.host.trim().is_empty() {
        return Err(crate::xai_url::statusless(
            "xai websockets executor: responses websocket URL host is empty",
        ));
    }
    Ok(crate::xai_url::string(&parsed))
}

/// `buildXAIWebsocketRequestBody`: every turn is a `response.create`; the payload rules
/// (`rules`) see the WebSocket fields, then the type is fixed again. The shaped body's
/// `store: false` stays (owner ruling E9c-1, 2026-11-16): no turn continues from a response
/// stored at xAI, so Go's `store: true` would only keep the turn's data there.
fn request_frame(body: Vec<u8>, rules: impl FnOnce(Vec<u8>) -> Vec<u8>) -> Vec<u8> {
    let mut out = body;
    gj::set_str(&mut out, "type", "response.create");
    for key in ["stream", "stream_options", "background"] {
        gj::delete(&mut out, key);
    }
    let mut out = rules(out);
    gj::set_str(&mut out, "type", "response.create");
    out
}

/// `xaiWebsocketGenerateFalse`: a warmup turn.
fn generate_false(payload: &[u8]) -> bool {
    let generate = gj::get(payload, "generate");
    generate.exists() && !generate.bool()
}

/// `buildXAIWebsocketWarmupCompletedPayload`: the `response.completed` a warmup turn
/// never receives, built from its `response.created`.
fn warmup_completed(created: &[u8]) -> Vec<u8> {
    let mut completed =
        format!(r#"{{"type":"response.completed","response":{{"output":[],"usage":{EMPTY_USAGE}}}}}"#).into_bytes();
    let sequence = gj::get(created, "sequence_number");
    if sequence.exists() {
        gj::set_int(&mut completed, "sequence_number", sequence.int() + 1);
    }
    let response = gj::get(created, "response");
    if response.exists() && response.is_object() {
        let mut inner = response.raw().to_vec();
        gj::set_str(&mut inner, "status", "completed");
        if !gj::get(&inner, "output").exists() {
            gj::set_raw(&mut inner, "output", "[]");
        }
        if !gj::get(&inner, "usage").exists() {
            gj::set_raw(&mut inner, "usage", EMPTY_USAGE);
        }
        gj::set_raw(&mut completed, "response", inner);
    }
    ensure_responses_usage_details(&completed)
}

/// `parseXAIWebsocketError`: an `error` frame classified like Codex's, then remapped by
/// `xaiStatusErr` (403 bad credentials to 401, the free-usage retry hint); a bare frame
/// with an `error` field takes its status from the frame.
fn ws_error(payload: &[u8]) -> Option<ExecError> {
    let text_payload = String::from_utf8_lossy(payload);
    if let Some(mut error) = codex_ws::ws_error(&text_payload, false) {
        let xai = response::status_error(error.status, payload);
        error.status = xai.status;
        if xai.retry_after.is_some() {
            error.retry_after = xai.retry_after;
        }
        return Some(error);
    }
    if payload.is_empty() || !gj::get(payload, "error").exists() {
        return None;
    }
    let mut status = gj::get(payload, "status").int();
    if status <= 0 {
        status = gj::get(payload, "status_code").int();
    }
    if status <= 0 {
        status = bare_error_status(payload);
    }
    let mut out = b"{}".to_vec();
    gj::set_str(&mut out, "type", "error");
    gj::set_int(&mut out, "status", status);
    let node = gj::get(payload, "error");
    if node.exists() {
        gj::set_raw(&mut out, "error", node.raw());
    }
    // Go keeps any int status; ExecError carries u16 (a larger code reads as 500).
    Some(response::status_error(u16::try_from(status).unwrap_or(500), &out))
}

/// `xaiBareWebsocketErrorStatus`.
fn bare_error_status(payload: &[u8]) -> i64 {
    for path in ["error.code", "error.status", "code"] {
        let raw = text(&gj::get(payload, path)).trim().to_owned();
        if raw.is_empty() {
            continue;
        }
        if let Some(status) = go_atoi(&raw).filter(|s| *s > 0) {
            return status;
        }
    }
    let message = text(&gj::get(payload, "error.message"));
    if message.contains(r#""code":"400""#) || message.contains("Request validation error") {
        return 400;
    }
    500
}

/// `strconv.Atoi`.
fn go_atoi(s: &str) -> Option<i64> {
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

/// A socket failure from the shared pool, as Go's xAI executor reports it: a close 1009
/// is the request-scoped 413 with `mapCodexWebsocketReadError`'s fixed body, and every
/// other read or write failure is a plain Go error (500) under the xAI executor's name,
/// whose transport scope lets the attempt fail over.
// ponytail: Go's message for other closes is gorilla's ("websocket: close 1011 (internal
// server error): internal"); the shared reader keeps no close details, so the text is
// the pool's.
fn pool_error(mut error: ExecError) -> ExecError {
    const CODEX: &[u8] = b"codex websockets executor: ";
    if error.status == 413 && error.scope == FailureScope::Request {
        error.body = Bytes::from_static(
            br#"{"error":{"message":"upstream websocket message too big","type":"invalid_request_error","code":"message_too_big"}}"#,
        );
        return error;
    }
    if error.body.starts_with(CODEX) {
        error.body = Bytes::from([&b"xai websockets executor: "[..], &error.body[CODEX.len()..]].concat());
    }
    if error.scope == FailureScope::Transport {
        error.status = 500;
    }
    error
}

/// `applyXAIWebsocketHeaders`.
fn handshake_headers(credential: &Credential, token: &str, session_id: &str, client: &Client<'_>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    let mut set = |name: &str, value: &str| {
        if let (Ok(name), Ok(value)) = (HeaderName::from_bytes(name.as_bytes()), HeaderValue::from_str(value)) {
            headers.insert(name, value);
        }
    };
    set("content-type", "application/json");
    if !token.trim().is_empty() {
        set("authorization", &format!("Bearer {token}"));
    }
    if !session_id.is_empty() {
        set("x-grok-conv-id", session_id);
    }
    let session = ck_wire::session::cpa_session_id(client.session);
    for (name, value) in ck_wire::headers::custom_headers(&credential.attributes, client.headers, session.as_deref()) {
        set(&name, &value);
    }
    if ck_core::registry::dynamic::is_api_key(credential) {
        set("user-agent", "cooker");
    }
    headers
}

// --- capture (RecordAPIWebsocket*) ------------------------------------------------------

/// The upstream WebSocket's capture: the request log Go builds once per turn (`wsReqLog`)
/// and the events of its socket. A no-op without an observer.
struct WsCapture {
    sink: CaptureSink,
    url: String,
    headers: Vec<(String, String)>,
    frame: Vec<u8>,
    auth_id: String,
    auth_label: String,
    auth_type: &'static str,
    auth_value: String,
}

impl WsCapture {
    fn new(usage: &UsageSink, credential: &Credential, url: &str, headers: &HeaderMap, frame: &[u8]) -> Self {
        let sink = usage.capture().clone();
        if !sink.enabled() {
            // Nothing reads the fields without an observer: keep no copy of the frame.
            return Self {
                sink,
                url: String::new(),
                headers: Vec::new(),
                frame: Vec::new(),
                auth_id: String::new(),
                auth_label: String::new(),
                auth_type: "",
                auth_value: String::new(),
            };
        }
        let (auth_type, auth_value) = ck_core::registry::dynamic::account_info(credential);
        Self {
            headers: crate::openai_compat_http::header_pairs(headers),
            sink,
            url: url.to_owned(),
            frame: frame.to_vec(),
            auth_id: credential.id.clone(),
            auth_label: credential.label.clone(),
            auth_type,
            auth_value,
        }
    }

    fn log<'a>(
        &'a self,
        url: &'a str,
        method: &'a str,
        headers: &'a [(String, String)],
        body: &'a [u8],
    ) -> UpstreamRequest<'a> {
        UpstreamRequest {
            url,
            method,
            headers,
            body,
            provider: crate::xai::PROVIDER,
            auth_id: &self.auth_id,
            auth_label: &self.auth_label,
            auth_type: self.auth_type,
            auth_value: &self.auth_value,
        }
    }

    /// `RecordAPIWebsocketRequest`.
    fn request(&self) {
        if self.sink.enabled() {
            self.sink.record(CaptureEvent::WebsocketRequest(self.log(
                &self.url,
                "WEBSOCKET",
                &self.headers,
                &self.frame,
            )));
        }
    }

    /// `RecordAPIWebsocketUpgradeRejection`: an HTTP attempt (`websocketUpgradeRequestLog`:
    /// GET on the http(s) URL, no body, Connection and Upgrade defaulted).
    fn rejection(&self, status: u16, headers: &HeaderMap, body: &[u8]) {
        if !self.sink.enabled() {
            return;
        }
        let url = upgrade_url(&self.url);
        let mut request_headers = self.headers.clone();
        for (name, value) in [("Connection", "Upgrade"), ("Upgrade", "websocket")] {
            if !request_headers.iter().any(|(n, v)| n == name && !v.trim().is_empty()) {
                request_headers.retain(|(n, _)| n != name);
                request_headers.push((name.to_owned(), value.to_owned()));
            }
        }
        self.sink
            .record(CaptureEvent::Request(self.log(&url, "GET", &request_headers, &[])));
        let response_headers = crate::openai_compat_http::header_pairs(headers);
        self.sink
            .record(CaptureEvent::ResponseMetadata(status, &response_headers));
        self.sink.record(CaptureEvent::ResponseChunk(body));
    }

    /// `recordAPIWebsocketHandshake` for a newly dialed socket.
    fn handshake(&self, headers: &HeaderMap) {
        if self.sink.enabled() {
            let pairs = crate::openai_compat_http::header_pairs(headers);
            self.sink.record(CaptureEvent::WebsocketHandshake(101, &pairs));
        }
    }

    /// `RecordAPIWebsocketError`.
    fn error(&self, stage: &str, error: &ExecError) {
        if self.sink.enabled() {
            self.sink.record(CaptureEvent::WebsocketError {
                stage,
                error: &String::from_utf8_lossy(&error.body),
            });
        }
    }

    /// `AppendAPIWebsocketResponse`.
    fn response(&self, payload: &[u8]) {
        self.sink.record(CaptureEvent::WebsocketResponse(payload));
    }
}

/// `helps.WebsocketUpgradeRequestURL`: ws to http, wss to https.
fn upgrade_url(raw: &str) -> String {
    let raw = raw.trim();
    match raw.find(':') {
        Some(i) if raw[..i].eq_ignore_ascii_case("ws") => format!("http{}", &raw[i..]),
        Some(i) if raw[..i].eq_ignore_ascii_case("wss") => format!("https{}", &raw[i..]),
        _ => raw.to_owned(),
    }
}

// --- the turn --------------------------------------------------------------------------

/// One turn's upstream frames as the attempt's body. Dropping it ends the turn: the socket
/// stays pooled for the next turn and the session's turn lock is released.
struct Frames {
    rx: mpsc::Receiver<Read>,
    conn: Arc<Upstream>,
    session: Arc<Session>,
    _turn: OwnedMutexGuard<()>,
    /// Shared with the turn's [`Cancel`]; cleared once the body is dropped.
    in_flight: Arc<AtomicBool>,
    warmup: bool,
    /// The synthesized warmup completion, after its `response.created`.
    pending: Option<Bytes>,
    usage: UsageSink,
    capture: WsCapture,
    done: bool,
}

impl Drop for Frames {
    fn drop(&mut self) {
        self.in_flight.store(false, Ordering::Release);
        self.conn.deactivate();
    }
}

/// One SSE event whose data is `payload` as received.
fn event(payload: &[u8]) -> Bytes {
    Bytes::from([&b"data: "[..], payload, b"\n\n"].concat())
}

impl Frames {
    async fn next(&mut self) -> Option<Result<Bytes, ExecError>> {
        if let Some(completed) = self.pending.take() {
            self.done = true;
            return Some(Ok(completed));
        }
        while !self.done {
            let read = match self.rx.recv().await {
                Some(read) => read,
                // The reader's terminal error did not fit a full queue (Go's
                // `sendTerminalWebsocketRead` waits for room); it is kept on the socket.
                None => {
                    let lost = self.conn.link.lock().expect("link").lost.clone();
                    Read::Failed(lost.unwrap_or_else(|| plain("xai websockets executor: session read channel closed")))
                }
            };
            let message = match read {
                Read::Text(message) => message,
                Read::Failed(error) => return Some(Err(self.lost(error))),
            };
            let payload = message.as_bytes();
            if ck_wire::gostr::trim_space(payload).is_empty() {
                continue;
            }
            self.usage.first_byte();
            self.capture.response(payload);
            if let Some(error) = ws_error(payload) {
                self.capture.error("upstream_error", &error);
                self.session.invalidate(&self.conn, &error, false);
                self.done = true;
                return Some(Err(error));
            }
            let kind = text(&gj::get(payload, "type"));
            if self.warmup && kind == "response.created" {
                self.pending = Some(event(&warmup_completed(payload)));
            }
            if kind == "response.done" {
                // xAI's WebSocket mode may end a response with `response.done` alone, which
                // HTTP streams never send and the Responses ingress does not know. As the
                // completion, its `response.status` still decides how the response ends.
                let mut completed = payload.to_vec();
                gj::set_str(&mut completed, "type", "response.completed");
                return Some(Ok(event(&completed)));
            }
            return Some(Ok(event(payload)));
        }
        None
    }

    /// The socket failed during the turn: the attempt's upstream loss.
    fn lost(&mut self, error: ExecError) -> ExecError {
        let error = pool_error(error);
        // Every socket here has a session, whose reader hands a binary message over as a
        // read error: Go logs it at the `read` stage.
        self.capture.error("read", &error);
        self.done = true;
        error
    }
}

/// Cancels an xAI turn: xAI's WebSocket mode has no cancel command, so the turn's socket
/// closes unless the turn already ended.
pub struct Cancel {
    session: Arc<Session>,
    conn: Arc<Upstream>,
    in_flight: Arc<AtomicBool>,
}

impl CancelTurn for Cancel {
    async fn cancel(self) {
        if self.in_flight.load(Ordering::Acquire) {
            let error = plain("xai websockets executor: turn cancelled");
            self.session.invalidate(&self.conn, &error, false);
        }
    }
}

impl Turns for XaiExecutor {
    type Cancel = Cancel;

    async fn turn(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Turn<XaiIngress, Cancel>, ExecError> {
        let session_id = request
            .client
            .execution_session
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| {
                ExecError::local(
                    500,
                    FailureScope::Request,
                    "xai websockets executor: a turn needs its connection's execution session",
                )
            })?;
        let credential = attempt.credential;
        let upload = encode(&request.validated, true, streams.request)?;
        let call = Call::new(attempt, request)?;
        let mut prepared = call.shape(&upload, &self.replay, true, Format::Codex)?;
        call.report(&prepared.body);
        let frame = request_frame(std::mem::take(&mut prepared.body), |body| {
            call.rules(Format::Codex, &upload, body)
        });
        // The official API (or an explicit base_url): cli-chat-proxy refuses upgrades.
        let (token, mut base) = creds(credential);
        if base.is_empty() {
            base = DEFAULT_API_BASE_URL.to_owned();
        }
        let http_url = format!("{}/responses", base.strip_suffix('/').unwrap_or(&base));
        let target = Target::new(credential, self.url(ws_url(&http_url)?), &call.cfg);
        let headers = handshake_headers(credential, &token, &prepared.session_id, &request.client);
        let usage = request.client.usage.clone();
        let capture = WsCapture::new(&usage, credential, &target.url, &headers, &frame);
        let warmup = generate_false(&frame);
        let ingress = Ingress::new(
            streams.response,
            attempt.identity,
            Operation::Generate,
            BodyMode::Streaming,
            MAX_LINE as u64,
        )
        .map_err(encoding)?;
        let events = Events::new(ingress, prepared, self.replay.clone());

        let session = self.ws.pool.session(session_id);
        let guard = session.turn.clone().lock_owned().await;
        capture.request();
        let (conn, handshake) = self
            .socket(&session, (&target, &headers), (credential, &call.cfg), &capture)
            .await?;
        if let Some(handshake) = &handshake {
            capture.handshake(handshake);
        }
        usage.round_trip_started();
        let rx = conn.activate();
        if let Err(error) = conn.send(String::from_utf8_lossy(&frame).into_owned()).await {
            let error = pool_error(error);
            capture.error("send", &error);
            session.invalidate(&conn, &error, false);
            return Err(error);
        }
        let in_flight = Arc::new(AtomicBool::new(true));
        let cancel = Cancel {
            session: session.clone(),
            conn: conn.clone(),
            in_flight: in_flight.clone(),
        };
        let frames = Frames {
            rx,
            conn,
            session,
            _turn: guard,
            in_flight,
            warmup,
            pending: None,
            usage,
            capture,
            done: false,
        };
        let body = futures_util::stream::unfold(frames, |mut frames| async move {
            let item = frames.next().await?;
            Some((item, frames))
        })
        .boxed();
        Ok(Turn {
            upstream: types::Upstream {
                status: 200,
                headers: handshake.unwrap_or_default(),
                body,
                ingress: XaiIngress::Generate(Box::new(events)),
            },
            cancel,
        })
    }
}

impl XaiExecutor {
    /// `ensureUpstreamConn`: the session's socket when it still serves `target`, else a new
    /// one (closing a socket for another target) with its handshake headers. A socket xAI
    /// closed is already gone from the session, so this dials.
    async fn socket(
        &self,
        session: &Arc<Session>,
        (target, headers): (&Target, &HeaderMap),
        (credential, cfg): (&Credential, &ck_core::config::Config),
        capture: &WsCapture,
    ) -> Result<(Arc<Upstream>, Option<HeaderMap>), ExecError> {
        if let Some(current) = session.current() {
            if current.target.serves(target) {
                return Ok((current, None));
            }
            // Another credential, URL or proxy now serves this session.
            let mut slot = session.conn.lock().expect("session conn");
            if slot.as_ref().is_some_and(|c| Arc::ptr_eq(c, &current)) {
                *slot = None;
            }
            drop(slot);
            current.shutdown();
        }
        let client = self.clients.for_credential(credential, cfg);
        let (socket, handshake) = dial(&client, target, headers, capture).await?;
        let (sink, stream) = socket.split();
        let conn = Upstream::new(target.clone(), sink, self.ws.pool.write);
        // Publish before the reader runs, as the Codex executor does.
        let mut reader = conn.reader.lock().expect("reader handle");
        session.publish(conn.clone());
        let task = tokio::spawn(codex_ws::read_loop(
            stream,
            Arc::downgrade(session),
            conn.clone(),
            self.ws.pool.idle,
        ));
        *reader = Some(task.abort_handle());
        drop(reader);
        Ok((conn, Some(handshake)))
    }
}

/// `dialXAIWebsocket`: a rejected upgrade is `xaiStatusErr(status, body)`. The capture
/// records a rejected upgrade (any handshake response, including a 101 gorilla refuses) as
/// an HTTP attempt and any other failure as a `dial` error.
// ponytail: Go returns `xaiStatusErr(101, body)` for a 101 gorilla refuses; this keeps the
// plain handshake error, since a 101 status error has no sensible client response.
async fn dial(
    client: &wreq::Client,
    target: &Target,
    headers: &HeaderMap,
    capture: &WsCapture,
) -> Result<(codex_ws::WebSocket, HeaderMap), ExecError> {
    match dial_once(client, target, headers).await {
        Ok(socket) => Ok(socket),
        Err(Dial::Rejected(rejected)) => {
            let Rejection {
                error,
                status,
                headers,
                body,
            } = *rejected;
            capture.rejection(status, &headers, &body);
            Err(error)
        }
        Err(Dial::Failed(error)) => {
            capture.error("dial", &error);
            Err(error)
        }
    }
}

/// A failed dial: a handshake response gorilla rejects, or any other failure.
enum Dial {
    Rejected(Box<Rejection>),
    Failed(ExecError),
}

/// A handshake response gorilla rejects: the returned error and the response.
struct Rejection {
    error: ExecError,
    status: u16,
    headers: HeaderMap,
    body: Vec<u8>,
}

async fn dial_once(
    client: &wreq::Client,
    target: &Target,
    headers: &HeaderMap,
) -> Result<(codex_ws::WebSocket, HeaderMap), Dial> {
    // Go's xAI dialer is gorilla's with `EnableCompression`, like the Codex one, and the
    // socket joins the same pool type, so both dial through codex_ws's upgrade.
    let mut headers = headers.clone();
    let key = codex_ws::offer_compression(&mut headers);
    let builder = client.websocket(&target.url).headers(headers).accept_key(key.clone());
    let attempt = async {
        let mut res = builder.send().await.map_err(|e| {
            Dial::Failed(plain(format!(
                "xai websockets executor: dial failed: {}",
                crate::upstream::transport_text(&e)
            )))
        })?;
        let status = res.status().as_u16();
        let handshake = res.headers().clone();
        if status != 101 {
            let inner = std::mem::replace(&mut *res, wreq::Response::from(http::Response::new(Vec::<u8>::new())));
            let mut body = Vec::new();
            let mut stream = inner.bytes_stream();
            while let Some(Ok(chunk)) = stream.next().await {
                body.extend_from_slice(&chunk[..chunk.len().min(MAX_HANDSHAKE_BODY - body.len())]);
                if body.len() >= MAX_HANDSHAKE_BODY {
                    break;
                }
            }
            // Go marks the upstream attempt on a dial error; here the upstream headers do
            // (the server's `upstream_attempted`).
            let mut error = response::status_error(status, &body);
            error.headers = Box::new(handshake.clone());
            return Err(Dial::Rejected(Box::new(Rejection {
                error,
                status,
                headers: handshake,
                body,
            })));
        }
        let socket = codex_ws::upgrade(&mut res, &key, &handshake).await.map_err(|e| {
            let error = match e {
                codex_ws::UpgradeError::Handshake => plain("xai websockets executor: websocket handshake failed"),
                codex_ws::UpgradeError::Compression => {
                    plain("xai websockets executor: websocket: invalid compression negotiation")
                }
            };
            Dial::Rejected(Box::new(Rejection {
                error,
                status,
                headers: handshake.clone(),
                body: Vec::new(),
            }))
        })?;
        Ok((socket, handshake))
    };
    tokio::time::timeout(HANDSHAKE_TIMEOUT, attempt)
        .await
        .unwrap_or_else(|_| Err(Dial::Failed(plain("xai websockets executor: handshake timed out"))))
}

#[cfg(test)]
#[path = "xai_ws_tests.rs"]
mod tests;
