//! Codex upstream Responses WebSocket (codex_websockets_executor.go, _connection.go,
//! _session.go, _stream.go, _errors.go).
//!
//! One upstream socket per downstream session, keyed by credential, URL and proxy. A
//! single reader task per socket enforces the 5-minute idle deadline (pongs are answered
//! while reading) and hands frames to the turn in progress through a bounded channel;
//! frames that arrive between turns are dropped, as in Go. Turns on one session are
//! serialised. The socket stays pooled after a completed response so the next turn can
//! continue with `previous_response_id` and incremental input.
//!
//! Lifecycle: the reader task ends with the socket; [`Pool::close`] (downstream gone)
//! aborts it and sends a close frame. No task outlives its session.
//!
//! [`turns`] runs a turn on the execution contract (`session::Turns`) over the same pool:
//! expanded history every turn, one attempt per upstream response, no redial. The server's
//! Responses WebSocket handler still drives the old turn path here until it moves to it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use bytes::Bytes;
use ck_core::config::Config;
use ck_core::credential::Credential;
use ck_core::exec::{ExecError, ExecRequest, ExecResponse, ExecSession, ExecStream, FailureScope, ResponseBody};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use gjson::Kind;
use http::HeaderMap;
use tokio::sync::{OwnedMutexGuard, mpsc, watch};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::{Role, WebSocketConfig};

use deflate::Inflate;

/// The upstream socket: tungstenite over the upgraded connection, with permessage-deflate
/// reads when negotiated.
pub(crate) type WebSocket = WebSocketStream<Inflate<wreq::Upgraded>>;

use crate::codex::CodexExecutor;
use crate::codex_json::{set_raw, set_str};
use crate::codex_quota::{self, QuotaSignals};
use crate::codex_request::{self as request, Call, Settings, View};
use crate::codex_response::{self as response, OutputItems};
use crate::proxy::Proxy;

mod execute;
mod io;
mod session;
mod turn;
mod turns;
pub(crate) use io::{UpgradeError, offer_compression, read_loop, upgrade};
use turn::{bootstrap, rest};

/// `codexResponsesWebsocketIdleTimeout`: read deadline renewed before every read.
pub(crate) const IDLE_TIMEOUT: Duration = Duration::from_secs(300);
/// `codexResponsesWebsocketHandshakeTO`.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);
/// Bound on one upstream frame write (docs/DIFFERENCES-FROM-GO.md). Go sets no write
/// deadline, so a write into a half-open socket held the turn until the 300 s read
/// deadline. A large frame gets as long as it takes at `rate` bytes per second.
#[derive(Debug, Clone, Copy)]
pub(crate) struct WriteLimit {
    pub(crate) floor: Duration,
    pub(crate) rate: usize,
}

// ponytail: a fixed 128 KiB/s floor rate (about 1 Mbit/s); a slower uplink sending a
// multi-megabyte frame times out. Make it configurable if such a link shows up.
pub(crate) const WRITE_LIMIT: WriteLimit = WriteLimit {
    floor: Duration::from_secs(10),
    rate: 128 << 10,
};

impl WriteLimit {
    fn deadline(self, len: usize) -> Duration {
        self.floor
            .max(Duration::from_secs(len.div_ceil(self.rate.max(1)) as u64))
    }
}
/// Go's per-turn read channel capacity.
const TURN_BUFFER: usize = 4096;
/// Bound on a handshake rejection body.
const MAX_HANDSHAKE_BODY: usize = 64 * 1024;
/// Largest upstream frame or message read.
// ponytail: Go sets no read limit; 64 MiB (the HTTP body cap) keeps one socket's memory
// bounded while fitting image-bearing output items.
const MAX_UPSTREAM_MESSAGE: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone)]
pub(crate) struct Target {
    pub(crate) credential: String,
    pub(crate) url: String,
    /// The proxy to dial with, parsed from `setting`.
    pub(crate) proxy: Proxy,
    /// The trimmed effective setting: the credential's own, else `requests.proxy-url`.
    /// Go compares these strings (`websocketSessionTargetMatches`), not parsed proxies:
    /// `direct` and `none` are different targets.
    setting: String,
    /// The trimmed `requests.proxy-url` when the target was resolved.
    global: String,
    /// Whether `setting` came from `requests.proxy-url` (the credential sets none).
    inherited: bool,
    // xAI shares the socket reader but retains its own terminal-event contract.
    accepts_done: bool,
}

impl Target {
    pub(crate) fn new(credential: &Credential, url: String, cfg: &Config) -> Self {
        let own = Proxy::own_url(credential);
        let global = Proxy::global_url(cfg);
        let setting = own.map_or_else(|| global.clone(), str::to_owned);
        Self {
            credential: credential.id.clone(),
            url,
            proxy: Proxy::parse(&setting),
            setting,
            global,
            inherited: own.is_none(),
            accepts_done: credential.provider == "xai",
        }
    }

    /// Whether the live socket dialed for `self` still serves `want`. Go 1e37b52b
    /// (`effectiveConfig`): for the same credential, the global proxy is the one the
    /// socket dialed with, so a reload that changes only `requests.proxy-url` keeps the
    /// socket. New sockets dial with the new setting.
    pub(crate) fn serves(&self, want: &Target) -> bool {
        let setting = if want.inherited { &self.global } else { &want.setting };
        self.credential == want.credential && self.url == want.url && self.setting == *setting
    }
}

pub(crate) enum Read {
    Text(String),
    /// The socket failed; the reader has already invalidated it.
    Failed(ExecError),
}

/// The reader's hand-off to the turn in progress. One lock covers both fields, so a turn
/// that activates after the reader exited sees why instead of waiting forever.
#[derive(Default)]
pub(crate) struct Link {
    /// The turn currently reading, if any.
    active: Option<mpsc::Sender<Read>>,
    /// Why the reader stopped (`upstreamDisconnectError`); set once when it exits.
    pub(crate) lost: Option<ExecError>,
    /// Response IDs this socket already finished (`terminalResponseIDs`): a late
    /// `response.interrupt` for one is done, not an error and not forwarded.
    // ponytail: grows by one ID per finished response for the socket's life, as Go's
    // does; bound it if sockets ever outlive thousands of turns.
    terminal: std::collections::HashSet<String>,
    /// The response live on the socket: the last `response.created` the reader read whose
    /// terminal it has not read. The reader records it before delivering the frame, so it
    /// holds whether or not a turn has consumed that frame yet.
    pub(crate) live: Option<String>,
    /// A `response.create` was written and the reader has read no `response.created` since.
    pub(crate) awaiting: bool,
}

/// A provider-recognized terminal with a non-empty response ID.
fn terminal_response_id(payload: &str, accepts_done: bool) -> Option<String> {
    let kind = gjson::get(payload, "type");
    if !matches!(
        kind.str(),
        "response.completed" | "response.incomplete" | "response.failed"
    ) && !(accepts_done && kind.str() == "response.done")
    {
        return None;
    }
    let id = gjson::get(payload, "response.id");
    (id.kind() == Kind::String && !id.str().is_empty()).then(|| id.str().to_owned())
}

/// Go `ErrNoActiveUpstreamWebsocket`.
pub const NO_ACTIVE_UPSTREAM: &str = "no active upstream websocket for response.interrupt";

/// Why [`CodexExecutor::interrupt`] did not deliver a `response.interrupt`.
#[derive(Debug, PartialEq, Eq)]
pub enum InterruptError {
    /// No turn is reading on the session's current socket ([`NO_ACTIVE_UPSTREAM`]): an
    /// HTTP turn may still cancel locally.
    NoActive,
    /// The socket was lost, the credential is disabled, or the write failed.
    Rejected(String),
}

impl std::fmt::Display for InterruptError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoActive => f.write_str(NO_ACTIVE_UPSTREAM),
            Self::Rejected(text) => f.write_str(text),
        }
    }
}

pub(crate) struct Upstream {
    pub(crate) target: Target,
    pub(crate) sink: tokio::sync::Mutex<SplitSink<WebSocket, Message>>,
    pub(crate) link: Mutex<Link>,
    pub(crate) reader: Mutex<Option<tokio::task::AbortHandle>>,
    /// The last request that touched the `collaboration` namespace on this socket renamed
    /// it (`multiAgentV2OptimizedConn`): later continuations restore upstream names too.
    multi_agent: std::sync::atomic::AtomicBool,
    write: WriteLimit,
}

pub(crate) struct Session {
    /// Serialises turns (`reqMu`).
    pub(crate) turn: Arc<tokio::sync::Mutex<()>>,
    pub(crate) conn: Mutex<Option<Arc<Upstream>>>,
    /// The current attempt's socket loss; cleared when another credential is selected.
    closed: watch::Sender<Option<ExecError>>,
    /// Pauses a test's disconnect immediately before it is published.
    #[cfg(test)]
    before_notify: Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
    /// The downstream connection's client frames when response steering is configured.
    steering: Mutex<Option<Arc<SteeringInput>>>,
    /// The credential of the last socket dialed (Go keeps `sess.authID` after the socket
    /// is gone, and removing that credential still ends the session).
    credential: Mutex<String>,
}

/// Upstream sockets by downstream session id.
pub(crate) struct Pool {
    sessions: Mutex<HashMap<String, Arc<Session>>>,
    /// Read deadline for each upstream application message.
    pub(crate) idle: Duration,
    pub(crate) write: WriteLimit,
}

fn transport(message: &str) -> ExecError {
    ExecError::local(502, FailureScope::Transport, message)
}

/// gorilla/websocket `CloseError.Error()`: what Go returns for an upstream close.
fn close_error_text(code: u16, text: &str) -> String {
    let name = match code {
        1000 => " (normal)",
        1001 => " (going away)",
        1002 => " (protocol error)",
        1003 => " (unsupported data)",
        1005 => " (no status)",
        1006 => " (abnormal closure)",
        1007 => " (invalid payload data)",
        1008 => " (policy violation)",
        1009 => " (message too big)",
        1010 => " (mandatory extension missing)",
        1011 => " (internal server error)",
        1015 => " (TLS handshake error)",
        _ => "",
    };
    let mut out = format!("websocket: close {code}{name}");
    if !text.is_empty() {
        out.push_str(": ");
        out.push_str(text);
    }
    out
}

/// The session's loss on an upstream close 1009: Go's reader notifies the downstream
/// handler with gorilla's raw close error, so the client sees close 1009 with the
/// upstream's reason ("message too big" when empty).
/// `close_reason` keeps gorilla's `CloseError.Text` verbatim for the handler, which only
/// trims the message of a mapped 413.
fn closed_too_big(reason: &str) -> ExecError {
    let message = if reason.is_empty() { "message too big" } else { reason };
    ExecError::local(
        413,
        FailureScope::Request,
        serde_json::json!({
            "error": {"message": message, "type": "invalid_request_error", "code": "message_too_big"},
            "close_reason": reason,
        })
        .to_string(),
    )
}

/// `mapCodexWebsocketReadError` / `mapCodexWebsocketWriteError`: what a turn reports
/// after the upstream closed with 1009, a fixed request-scoped 413.
fn message_too_big() -> ExecError {
    ExecError::local(
        413,
        FailureScope::Request,
        r#"{"error":{"message":"upstream websocket message too big","type":"invalid_request_error","code":"message_too_big"}}"#,
    )
}

/// The error a turn reads from the session's loss: the mapped 413 for a 1009 close (only
/// [`closed_too_big`] makes a 413 loss), the loss itself otherwise.
/// The reader's error for a binary message (Go's `unexpected_binary` stage).
const UNEXPECTED_BINARY: &str = "codex websockets executor: unexpected binary message";

/// A failed or timed-out write: the request-scoped 413 when the reader already saw the
/// upstream close with 1009 (the frame was too big), else a transport error.
pub(crate) fn write_error(lost: Option<&ExecError>, message: &str) -> ExecError {
    lost.filter(|e| e.status == 413)
        .map_or_else(|| transport(message), turn_error)
}

fn turn_error(lost: &ExecError) -> ExecError {
    if lost.status == 413 {
        message_too_big()
    } else {
        lost.clone()
    }
}

/// `buildCodexResponsesWebsocketURL`. Go returns its parse, scheme and empty-host errors
/// as plain errors before dialing.
fn ws_url(http_url: &str) -> Result<String, ExecError> {
    let mut parsed = crate::xai_url::parse(http_url.trim()).map_err(crate::xai_url::statusless)?;
    parsed.scheme = match parsed.scheme.as_str() {
        "http" => "ws".to_owned(),
        "https" => "wss".to_owned(),
        other => {
            return Err(crate::xai_url::statusless(format!(
                "codex websockets executor: unsupported responses websocket URL scheme {}",
                ck_wire::gostr::quote(other)
            )));
        }
    };
    if parsed.host.trim().is_empty() {
        return Err(crate::xai_url::statusless(
            "codex websockets executor: responses websocket URL host is empty",
        ));
    }
    Ok(crate::xai_url::string(&parsed))
}

/// A WebSocket turn's body after the prompt cache key, in Go's order:
/// `SanitizeCodexInputItemIDs`, `applyCodexSIWCWebsocketBodyRules` (the id pass sees
/// the `configuration_update` items the SIWC rules then drop), then the payload rules.
pub(crate) fn turn_body(
    view: &View<'_>,
    req: &ExecRequest,
    settings: &Settings,
    finalize: &request::Finalizer,
    body: String,
) -> String {
    let mut body = request::sanitize_input_ids(body);
    if view.siwc {
        body = request::siwc_request_body(body, req, Call::Websocket);
    }
    finalize.apply(req, settings, body)
}

/// `frameCodexWebsocketRequestBody`: every turn is a `response.create`.
fn request_frame(body: String) -> String {
    set_str(&body, "type", "response.create")
}

/// `buildCodexWebsocketErrorPayload` for an `error` frame with a positive status: the
/// status and the body Go classifies (and clears the reasoning replay with).
fn ws_error_body(payload: &str) -> Option<(u16, String)> {
    if gjson::get(payload, "type").str().trim() != "error" {
        return None;
    }
    let mut status = gjson::get(payload, "status").i64();
    if status == 0 {
        status = gjson::get(payload, "status_code").i64();
    }
    // Go accepts any positive status; larger than u16 cannot be represented here.
    let Ok(status) = u16::try_from(status) else {
        return None;
    };
    if status == 0 {
        return None;
    }
    let mut out = set_raw("{}", "status", &status.to_string());
    let body = gjson::get(payload, "body");
    let error = gjson::get(payload, "error");
    if body.exists() {
        out = set_raw(&out, "body", body.json());
    }
    if body.exists() && body.get("error").exists() {
        out = set_raw(&out, "error", body.get("error").json());
    } else if error.exists() {
        out = set_raw(&out, "error", error.json());
    } else {
        out = set_str(&out, "error.type", "server_error");
        out = set_str(&out, "error.message", response::go_status_text(status));
    }
    Some((status, out))
}

/// A request's reasoning replay (Claude clients only): the executor's cache and the
/// request's scope.
#[derive(Clone, Default)]
pub(super) struct Replay {
    cache: Arc<crate::codex_replay::Cache>,
    scope: crate::codex_replay::Scope,
}

impl Replay {
    /// `applyCodexReasoningReplayCacheRequired`: cached turns inserted into `body`.
    fn apply(
        cache: &Arc<crate::codex_replay::Cache>,
        req: &ExecRequest,
        credential: &str,
        body: String,
    ) -> (String, Self) {
        let (body, scope) = crate::codex_replay::apply(cache, req, credential, body);
        let replay = Self {
            cache: cache.clone(),
            scope,
        };
        (body, replay)
    }

    /// `clearCodexReasoningReplayOnWebsocketError` or `...OnInvalidSignature` for a
    /// rejection event: an invalid thinking signature drops the cached reasoning.
    fn clear(&self, payload: &str, cooling: bool) {
        if let Some((status, body)) = ws_error_body(payload) {
            crate::codex_replay::clear_on_invalid_signature(&self.cache, &self.scope, status, body.as_bytes());
        } else if let Some((error, body)) = response::terminal_failure(payload, cooling) {
            crate::codex_replay::clear_on_invalid_signature(&self.cache, &self.scope, error.status, body.as_bytes());
        }
    }

    /// `cacheCodexReasoningReplayFromCompleted`.
    fn completed(&self, payload: &str) {
        crate::codex_replay::cache_completed(&self.cache, &self.scope, payload.as_bytes());
    }
}

/// `parseCodexWebsocketErrorWithCooling`: an `error` frame that carries an HTTP status.
pub(crate) fn ws_error(payload: &str, model_level_cooling: bool) -> Option<ExecError> {
    let (status, out) = ws_error_body(payload)?;
    let mut headers = HeaderMap::new();
    let raw_headers = gjson::get(payload, "headers");
    if raw_headers.kind() == Kind::Object {
        raw_headers.each(|k, v| {
            let value = match v.kind() {
                Kind::String => v.str().trim().to_owned(),
                Kind::Number | Kind::True | Kind::False => v.json().trim().to_owned(),
                _ => String::new(),
            };
            if let (Ok(name), Ok(value)) = (
                http::HeaderName::try_from(k.str().trim()),
                http::HeaderValue::from_str(&value),
            ) && !value.is_empty()
            {
                headers.insert(name, value);
            }
            true
        });
    }
    let connection_limit = [
        "error.code",
        "error.type",
        "body.error.code",
        "body.error.type",
        "code",
        "error",
    ]
    .iter()
    .any(|p| gjson::get(payload, p).str().trim() == "websocket_connection_limit_reached");
    // Go `statusErrWithHeaders`: the frame's headers reach the client under passthrough.
    let addon = (!headers.is_empty()).then(|| Box::new(headers.clone()));
    let mut error = response::status_error_raw(status, &out, headers, model_level_cooling);
    error.addon = addon;
    if error.retry_after.is_none() && connection_limit {
        error.retry_after = Some(Duration::ZERO);
    }
    Some(error)
}

/// One upstream message, classified.
enum Frame {
    /// Empty text frame (heartbeat).
    Skip,
    Event {
        out: Bytes,
        raw_len: usize,
        bufferable: bool,
        terminal: bool,
    },
    /// The response failed. `read` means the reader already dropped the socket;
    /// `overload` is a transient capacity rejection another credential may serve.
    Failed {
        error: ExecError,
        overload: bool,
        read: bool,
    },
}

/// One turn on a pooled socket. Dropping it releases the turn lock and the reader.
struct Turn {
    rx: mpsc::Receiver<Read>,
    conn: Arc<Upstream>,
    session: Arc<Session>,
    _turn: Option<OwnedMutexGuard<()>>,
    items: OutputItems,
    native: bool,
    cooling: bool,
    quota: Arc<QuotaSignals>,
    credential: String,
    /// Handshake headers plus quota headers from this turn's events.
    observed: HeaderMap,
    /// The request renamed the `collaboration` namespace (multi-agent v2).
    restore: bool,
    /// The attempt's usage record.
    usage: ck_core::exec::UsageSink,
    /// The upstream model, for per-model quota snapshots.
    model: String,
    replay: Replay,
    /// The attempt's wire capture (Go's `api.websocket.*` timeline events).
    wire: crate::codex_capture::Wire,
}

impl Drop for Turn {
    fn drop(&mut self) {
        self.conn.deactivate();
        drop(self._turn.take());
    }
}

#[path = "codex_ws_deflate.rs"]
pub(crate) mod deflate;
#[path = "codex_duplex.rs"]
mod duplex;
pub use duplex::{ClientFrames, SteeringInput};
pub use turns::{Next, Successor, TurnControl};

#[cfg(test)]
#[path = "codex_ws_tests.rs"]
mod tests;

#[cfg(test)]
mod close_text_tests {
    use super::close_error_text;

    /// gorilla/websocket `CloseError.Error()` texts, which Go returns unchanged.
    #[test]
    fn close_errors_read_like_gorilla() {
        assert_eq!(close_error_text(1000, ""), "websocket: close 1000 (normal)");
        assert_eq!(close_error_text(1001, "bye"), "websocket: close 1001 (going away): bye");
        assert_eq!(
            close_error_text(1006, "unexpected EOF"),
            "websocket: close 1006 (abnormal closure): unexpected EOF"
        );
        assert_eq!(close_error_text(4000, "x"), "websocket: close 4000: x");
    }
}
