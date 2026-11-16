//! `/v1/ws`: the AI Studio relay route (sdk/cliproxy service_auth.go
//! `ensureWebsocketGateway`, internal/api `AttachWebsocketRoute`, internal/wsrelay).
//!
//! A browser running the AI Studio bridge connects here. Its session is registered with
//! the executors' [`Relay`] under a random `aistudio-…` name, and a runtime-only
//! `aistudio` credential of that name serves requests until the socket closes. Only a key
//! listed in `oauth.providers.aistudio.relay-keys` registers a browser; client keys do
//! not, and an empty list refuses every browser.
//!
//! The transport follows gorilla and session.go: a ping every 30 seconds, a read
//! deadline of 60 seconds that only a pong extends, 10-second writes, frames up to
//! 64 MiB; text and binary frames are both read as JSON messages.

use std::sync::{Arc, Weak};
use std::time::Duration;

use axum::Router;
use axum::extract::ws::rejection::WebSocketUpgradeRejection;
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Request, State};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::get;
use ck_core::config::Config;
use ck_core::credential::Credential;
use ck_exec::wsrelay::{self, Relay};
use ck_wire::gostr::GoStr;
use futures_util::{SinkExt, StreamExt};
use tokio::time::{Instant, sleep_until, timeout};

use crate::Runtime;

const READ_TIMEOUT: Duration = Duration::from_secs(60);
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
const HEARTBEAT: Duration = Duration::from_secs(30);
/// `maxInboundMessageLen`.
const MAX_MESSAGE: usize = 64 << 20;

/// `oauth.providers.aistudio.relay-keys`: the keys that may register a browser.
pub fn relay_keys(cfg: &Config) -> Vec<String> {
    cfg.document
        .get("oauth")
        .and_then(|o| o.get("providers"))
        .and_then(|p| p.get("aistudio"))
        .and_then(|a| a.get("relay-keys"))
        .and_then(serde_yaml_ng::Value::as_sequence)
        .map(|keys| keys.iter().filter_map(|k| k.as_str().map(str::to_owned)).collect())
        .unwrap_or_default()
}

/// The route, with the relay's session events wired to the runtime's credentials.
pub(crate) fn routes(rt: &Arc<Runtime>) -> Router<Arc<Runtime>> {
    let weak = Arc::downgrade(rt);
    rt.executors
        .google
        .aistudio
        .relay
        .set_observer(Arc::new(move |provider, cause| observe(&weak, provider, cause)));
    Router::new()
        .route(wsrelay::PATH, get(upgrade))
        .route_layer(middleware::from_fn_with_state(rt.clone(), require_relay_key))
}

/// Go `wsOnConnected` / `wsOnDisconnected`.
fn observe(rt: &Weak<Runtime>, provider: &str, cause: Option<&str>) {
    let Some(rt) = rt.upgrade() else {
        return;
    };
    match cause {
        None => {
            if !provider.go_lower().starts_with("aistudio-") {
                return;
            }
            if rt.store().add_runtime(Credential::relay_session(provider)) {
                tracing::info!("websocket provider connected: {provider}");
            }
        }
        Some(cause) if cause.contains("replaced by new connection") => {
            tracing::info!("websocket provider replaced: {provider}");
        }
        Some(cause) => {
            tracing::warn!("websocket provider disconnected: {provider} ({cause})");
            rt.store().remove_runtime(provider);
        }
    }
}

/// Registration needs a listed relay key, sent like a client key. An empty list refuses
/// every browser.
async fn require_relay_key(State(rt): State<Arc<Runtime>>, req: Request, next: Next) -> Response {
    let keys = relay_keys(&rt.config());
    if keys.is_empty() {
        return crate::http::errors::local(&req, 403, "AI Studio relay registration is disabled");
    }
    match crate::net::access::authenticate(&keys, req.headers(), req.uri().query().unwrap_or_default()) {
        Ok(_) => next.run(req).await,
        Err(message) => crate::http::errors::local(&req, 401, message),
    }
}

async fn upgrade(
    State(rt): State<Arc<Runtime>>,
    owner: Option<axum::Extension<crate::net::listener::ConnectionWork>>,
    ws: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Response {
    let Ok(ws) = ws else {
        return crate::realtime::websocket::upgrade_rejected();
    };
    let relay = rt.executors.google.aistudio.relay.clone();
    crate::net::listener::websocket(
        ws.max_message_size(MAX_MESSAGE).max_frame_size(MAX_MESSAGE),
        owner,
        move |socket| serve(relay, socket),
    )
}

/// gorilla's `CloseError` text for a close frame.
fn close_error(frame: Option<&CloseFrame>) -> String {
    let (code, reason) = frame.map_or((1005, ""), |f| (f.code, f.reason.as_str()));
    let name = match code {
        1000 => "normal",
        1001 => "going away",
        1002 => "protocol error",
        1003 => "unsupported data",
        1005 => "no status",
        1006 => "abnormal closure",
        1007 => "invalid payload data",
        1008 => "policy violation",
        1009 => "message too big",
        1010 => "mandatory extension missing",
        1011 => "internal server error",
        1012 => "service restart",
        1013 => "try again later",
        1015 => "TLS handshake error",
        _ => "",
    };
    let mut text = format!("websocket: close {code}");
    if !name.is_empty() {
        text.push_str(&format!(" ({name})"));
    }
    if !reason.is_empty() {
        text.push_str(&format!(": {reason}"));
    }
    text
}

/// One connection: frames out from the session, frames in to it, the heartbeat and the
/// read deadline, until either side ends.
// ponytail: transport failures carry this module's wording, not Go's net errors.
async fn serve(relay: Arc<Relay>, socket: WebSocket) {
    let (session, mut frames) = relay.connect();
    let (mut sink, mut stream) = socket.split();
    let mut closed = session.closed();
    let mut heartbeat = tokio::time::interval_at(Instant::now() + HEARTBEAT, HEARTBEAT);
    let mut deadline = Instant::now() + READ_TIMEOUT;
    loop {
        tokio::select! {
            frame = frames.recv() => {
                let Some(frame) = frame else { break };
                match timeout(WRITE_TIMEOUT, sink.send(Message::Text(frame.into()))).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        session.close(&format!("write json: {e}"));
                        break;
                    }
                    Err(_) => {
                        session.close("write json: i/o timeout");
                        break;
                    }
                }
            }
            message = stream.next() => match message {
                Some(Ok(Message::Text(text))) => {
                    if session.receive(text.as_bytes()).await.is_err() {
                        break;
                    }
                }
                Some(Ok(Message::Binary(data))) => {
                    if session.receive(&data).await.is_err() {
                        break;
                    }
                }
                Some(Ok(Message::Pong(_))) => deadline = Instant::now() + READ_TIMEOUT,
                Some(Ok(Message::Ping(_))) => {}
                Some(Ok(Message::Close(frame))) => {
                    session.close(&close_error(frame.as_ref()));
                    break;
                }
                Some(Err(e)) => {
                    session.close(&e.to_string());
                    break;
                }
                None => {
                    session.close("websocket: close 1006 (abnormal closure): unexpected EOF");
                    break;
                }
            },
            _ = heartbeat.tick() => {
                let ping = sink.send(Message::Ping(axum::body::Bytes::from_static(b"ping")));
                if !matches!(timeout(WRITE_TIMEOUT, ping).await, Ok(Ok(()))) {
                    session.close("websocket: ping failed");
                    break;
                }
            }
            _ = sleep_until(deadline) => {
                session.close("websocket: read deadline exceeded");
                break;
            }
            _ = wsrelay::wait_closed(&mut closed) => break,
        }
    }
    session.close(wsrelay::CLOSED);
    let _ = sink.close().await;
}
