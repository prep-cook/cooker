//! The call sideband (sideband.go `HandleSideband`) and the standard Realtime WebSocket
//! (websocket.go `HandleDirectWebsocket`): both dial an upstream socket with a Codex
//! OAuth credential, then run a live session over both sockets ([`super::session`]) until
//! either side closes. The credential is chosen before the upstream session opens and stays
//! pinned to it; a sideband reuses the call's own.
//!
//! A sideband claims its call while it runs: a second join gets 409, the call does not
//! expire meanwhile, a hangup ends the session, and the call is forgotten when it ends.
//! With `realtime.max-sockets-per-key` set, a key's socket past it is refused before dialling.

use std::sync::Arc;

use axum::Extension;
use axum::extract::rejection::PathRejection;
use axum::extract::ws::WebSocketUpgrade;
use axum::extract::ws::rejection::WebSocketUpgradeRejection;
use axum::extract::{OriginalUri, Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use ck_exec::codex_live::{self as live, DialError, LiveSocket, LiveTarget, Sideband};
use ck_translate::realtime::Events;
use tokio_util::sync::CancellationToken;
use wreq::ws::message as up;

use super::calls::{Claim, ClaimGuard};
use super::http::{HANDSHAKE_HEADERS, copy_headers, header_text};
use super::session::{self, Opened};
use super::{Live, Principal, header_session, live_error, realtime_error, select_oauth, trace, with_trace};
use crate::runtime::state::Runtime;

/// gorilla `tokenListContainsValue`: `token` is one of the comma-separated values.
fn has_token(headers: &HeaderMap, name: header::HeaderName, token: &str) -> bool {
    headers.get_all(name).iter().any(|v| {
        String::from_utf8_lossy(v.as_bytes())
            .split(',')
            .any(|t| t.trim_matches([' ', '\t']).eq_ignore_ascii_case(token))
    })
}

/// gorilla `IsWebSocketUpgrade`: `Connection` lists `upgrade` and `Upgrade` lists
/// `websocket`.
fn is_upgrade(headers: &HeaderMap) -> bool {
    has_token(headers, header::CONNECTION, "upgrade") && has_token(headers, header::UPGRADE, "websocket")
}

/// The rest of gorilla's `Upgrader.Upgrade` checks, which run after the upstream dial:
/// version 13 among the offered versions and a challenge key of 16 base64 bytes.
fn acceptable_handshake(headers: &HeaderMap) -> bool {
    use base64::Engine;
    let key = header_text(headers, header::SEC_WEBSOCKET_KEY);
    has_token(headers, header::SEC_WEBSOCKET_VERSION, "13")
        && base64::engine::general_purpose::STANDARD
            .decode(key.as_bytes())
            .is_ok_and(|k| k.len() == 16)
}

/// Lets axum's upgrade extractor accept what gorilla accepts: it wants exactly
/// `Upgrade: websocket` and `Sec-WebSocket-Version: 13`, gorilla token lists. Neither
/// header is forwarded upstream.
pub(super) async fn normalize_upgrade(mut req: axum::extract::Request, next: axum::middleware::Next) -> Response {
    let headers = req.headers_mut();
    if is_upgrade(headers) && has_token(headers, header::SEC_WEBSOCKET_VERSION, "13") {
        headers.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
        headers.insert(header::SEC_WEBSOCKET_VERSION, HeaderValue::from_static("13"));
    }
    next.run(req).await
}

/// gorilla `Subprotocols`: the first `Sec-WebSocket-Protocol` header, comma separated.
fn subprotocols(headers: &HeaderMap) -> Vec<String> {
    let value = header_text(headers, header::SEC_WEBSOCKET_PROTOCOL);
    let value = value.trim();
    if value.is_empty() {
        return Vec::new();
    }
    value.split(',').map(|p| p.trim().to_owned()).collect()
}

fn upgrade_required(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(header::UPGRADE, HeaderValue::from_static("websocket"));
    response
}

/// gorilla `Upgrader.returnError` for a handshake it cannot complete.
fn bad_handshake() -> Response {
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

/// Everything a WebSocket route receives.
pub(super) struct Inbound {
    rt: Arc<Runtime>,
    owner: Option<Extension<crate::net::listener::ConnectionWork>>,
    live: Arc<Live>,
    principal: Principal,
    headers: HeaderMap,
    ws: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
}

/// `GET /v1/live/{call_id}`.
pub(super) async fn live_sideband(
    State(rt): State<Arc<Runtime>>,
    owner: Option<Extension<crate::net::listener::ConnectionWork>>,
    Extension(live): Extension<Arc<Live>>,
    Extension(principal): Extension<Principal>,
    path: Result<Path<String>, PathRejection>,
    headers: HeaderMap,
    ws: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Response {
    let inbound = Inbound {
        rt,
        live,
        principal,
        headers,
        ws,
        owner,
    };
    sideband(inbound, false, Sideband::Frameless, path_param(path)).await
}

/// `GET /v1/realtime/calls/{call_id}`.
pub(super) async fn calls_sideband(
    State(rt): State<Arc<Runtime>>,
    owner: Option<Extension<crate::net::listener::ConnectionWork>>,
    Extension(live): Extension<Arc<Live>>,
    Extension(principal): Extension<Principal>,
    path: Result<Path<String>, PathRejection>,
    headers: HeaderMap,
    ws: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Response {
    let inbound = Inbound {
        rt,
        live,
        principal,
        headers,
        ws,
        owner,
    };
    sideband(inbound, true, Sideband::Calls, path_param(path)).await
}

/// `GET /v1/realtime` (`HandleRealtimeWebsocket`): a sideband when `call_id` is given,
/// else a standard Realtime session.
pub(super) async fn realtime(
    State(rt): State<Arc<Runtime>>,
    owner: Option<Extension<crate::net::listener::ConnectionWork>>,
    Extension(live): Extension<Arc<Live>>,
    Extension(principal): Extension<Principal>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    ws: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Response {
    let query = |name| {
        crate::net::access::query_get(uri.query().unwrap_or_default(), name)
            .map(|v| String::from_utf8_lossy(&v).trim().to_owned())
            .unwrap_or_default()
    };
    let inbound = Inbound {
        rt,
        live,
        principal,
        headers,
        ws,
        owner,
    };
    let call_id = query("call_id");
    if !call_id.is_empty() {
        return sideband(inbound, true, Sideband::Query, call_id).await;
    }
    direct(inbound, query("model")).await
}

fn path_param(path: Result<Path<String>, PathRejection>) -> String {
    path.map(|Path(id)| id.trim().to_owned()).unwrap_or_default()
}

/// `HandleSideband`.
async fn sideband(inbound: Inbound, realtime: bool, style: Sideband, call_id: String) -> Response {
    let Inbound {
        rt,
        live,
        principal,
        headers,
        ws,
        owner,
    } = inbound;
    let fail = |status, message: &str| live_error(realtime, status, message);
    if !is_upgrade(&headers) {
        return upgrade_required(fail(426, "WebSocket upgrade required"));
    }
    if !live::valid_call_id(&call_id) {
        return fail(400, "Invalid Codex live call ID");
    }
    let guard = match live.calls.claim(&call_id) {
        Claim::Acquired(call) => ClaimGuard::new(live.calls.clone(), call),
        Claim::Busy => return fail(409, "Codex live session already joining"),
        Claim::Missing => return fail(404, "Codex live session not found"),
    };
    let call = guard.call.clone();
    if let Some(secret) = &principal.secret {
        if call.secret_principal.is_empty() || secret.principal != call.secret_principal {
            return realtime_error(
                403,
                "Realtime client secret is not valid for this call",
                "invalid_request_error",
                "realtime_client_secret_scope_mismatch",
            );
        }
    } else if !call.owner_key.is_empty()
        && (principal.key != call.owner_key || principal.provider != call.owner_provider)
    {
        return realtime_error(
            403,
            "Realtime call belongs to another API principal",
            "invalid_request_error",
            "realtime_call_scope_mismatch",
        );
    }
    let Some(slot) = live.sockets.take(&principal.key) else {
        return socket_limit(&live, Some(&call_id));
    };
    let cfg = rt.config();
    let selection_headers = principal.selection_headers(&headers);
    // Go: a pick pinned to the call's credential that lasts as long as this relay.
    let lease = match select_oauth(&rt, &cfg, Some(&call.auth_id), &selection_headers, &[], Some(&call_id)).await {
        Ok(lease) => lease,
        Err(rejection) => return rejection.render(realtime),
    };
    let credential = lease.credential.clone();
    let trace = trace(&credential);
    let target = LiveTarget {
        credential: &credential,
        cfg: &cfg,
        client: &headers,
        session: header_session(&selection_headers, &[], Some(&call_id), &call.session_id),
        capture: Default::default(),
    };
    let url = live::sideband_url(&rt.executors.codex.live_endpoints().api_base, style, &call_id);
    let dial =
        rt.executors
            .codex
            .live_websocket(&target, &url, live::protocol_headers(&headers), subprotocols(&headers));
    let upstream = match dial.await {
        Ok(upstream) => upstream,
        Err(error) => {
            return with_trace(
                dial_failed(error, |status| fail(status, "Codex live sideband upstream unavailable")),
                &trace,
            );
        }
    };
    let Some(ws) = ws.ok().filter(|_| acceptable_handshake(&headers)) else {
        return with_trace(bad_handshake(), &trace);
    };
    let opened = Opened {
        rt: rt.clone(),
        settings: live.settings,
        owner: principal.owner(),
        events: Events::Sideband,
        model: None,
        upstream_model: String::new(),
        credential: credential.clone(),
        call_id: Some(call_id.clone()),
        facts: principal.facts(&cfg, &headers, "GET /v1/realtime", ""),
        hangup: call.ended.clone(),
    };
    upgrade(ws, upstream, owner, opened, move || {
        let mut guard = guard;
        guard.consume = true;
        drop(slot);
        // Ending the call closes its media; the pick ends once that close finished.
        let media = guard.call.media.clone();
        drop(guard);
        match media {
            Some(media) => media.after_close(Box::new(move || drop(lease))),
            None => drop(lease),
        }
    })
}

/// Refuses a socket past `realtime.max-sockets-per-key` and logs its end.
fn socket_limit(live: &Live, call_id: Option<&str>) -> Response {
    tracing::warn!(
        request_id = crate::observability::api::current_request_id().as_deref(),
        call_id,
        cause = "socket_limit",
        initiator = "gateway",
        max_sockets_per_key = live.settings.max_sockets_per_key,
        "live_session_end"
    );
    realtime_error(
        429,
        "Too many Realtime sockets are open for this key. Close one and connect again.",
        "rate_limit_error",
        "realtime_socket_limit",
    )
}

/// `HandleDirectWebsocket`.
async fn direct(inbound: Inbound, model: String) -> Response {
    let Inbound {
        rt,
        live,
        principal,
        headers,
        ws,
        owner,
    } = inbound;
    if !is_upgrade(&headers) {
        return upgrade_required(realtime_error(
            426,
            "WebSocket upgrade required",
            "invalid_request_error",
            "websocket_upgrade_required",
        ));
    }
    let requested = if model.is_empty() {
        live::DEFAULT_STANDARD_MODEL.to_owned()
    } else {
        model
    };
    let selection_model = live::codex_model(&requested);
    let secret_session = principal.secret.as_ref().map(|g| g.session.clone()).unwrap_or_default();
    if !secret_session.is_empty() && live::codex_model(&live::model_from_json(&secret_session)) != selection_model {
        return realtime_error(
            403,
            "Realtime client secret is not valid for the requested model",
            "invalid_request_error",
            "realtime_client_secret_scope_mismatch",
        );
    }
    let Some(slot) = live.sockets.take(&principal.key) else {
        return socket_limit(&live, None);
    };
    let cfg = rt.config();
    let selection_headers = principal.selection_headers(&headers);
    let lease = match select_oauth(&rt, &cfg, None, &selection_headers, &[], None).await {
        Ok(lease) => lease,
        Err(rejection) => return rejection.render(true),
    };
    let credential = lease.credential.clone();
    let trace = trace(&credential);
    let traced = |response| with_trace(response, &trace);
    let target = LiveTarget {
        credential: &credential,
        cfg: &cfg,
        client: &headers,
        session: header_session(&selection_headers, &[], None, ""),
        capture: Default::default(),
    };
    let url = live::direct_url(&rt.executors.codex.live_endpoints().api_base, &requested);
    let dial = rt
        .executors
        .codex
        .live_websocket(&target, &url, live::direct_headers(&headers), subprotocols(&headers));
    let mut upstream = match dial.await {
        Ok(upstream) => upstream,
        Err(error) => {
            return traced(dial_failed(error, |status| {
                let (status, message, kind, code) = match status {
                    404 | 501 => (
                        501,
                        "Direct Realtime WebSocket is not supported by the Codex OAuth upstream",
                        "not_supported_error",
                        "realtime_capability_not_supported",
                    ),
                    401 => (
                        401,
                        "Codex Realtime WebSocket upstream unavailable",
                        "authentication_error",
                        "realtime_upstream_unauthorized",
                    ),
                    _ => (
                        status,
                        "Codex Realtime WebSocket upstream unavailable",
                        "api_error",
                        "realtime_websocket_upstream_unavailable",
                    ),
                };
                realtime_error(status, message, kind, code)
            }));
        }
    };
    if !secret_session.is_empty() {
        let Ok(session) = live::session_update(&secret_session) else {
            return traced(realtime_error(
                500,
                "Failed to apply Realtime client secret session",
                "server_error",
                "realtime_session_failed",
            ));
        };
        let mut update = br#"{"type":"session.update","session":"#.to_vec();
        update.extend_from_slice(&ck_wire::json::compact(&session, true));
        update.push(b'}');
        let update = String::from_utf8_lossy(&update).into_owned();
        if upstream.socket.send(up::Message::text(update)).await.is_err() {
            return traced(realtime_error(
                502,
                "Failed to apply Realtime client secret session",
                "api_error",
                "realtime_upstream_unavailable",
            ));
        }
    }
    let Some(ws) = ws.ok().filter(|_| acceptable_handshake(&headers)) else {
        return traced(bad_handshake());
    };
    let opened = Opened {
        rt: rt.clone(),
        settings: live.settings,
        owner: principal.owner(),
        events: Events::Realtime,
        model: Some(requested.clone()),
        upstream_model: selection_model.clone(),
        credential: credential.clone(),
        call_id: None,
        facts: principal.facts(&cfg, &headers, "GET /v1/realtime", &requested),
        hangup: CancellationToken::new(),
    };
    // Go `selection.Retain()` until the session closes.
    upgrade(ws, upstream, owner, opened, move || {
        drop(slot);
        drop(lease);
    })
}

/// A rejected or failed upstream dial (`handleSidebandDialError` and its direct twin): the
/// upstream's own 401 passes through with its body; anything else becomes `other(status)`.
fn dial_failed(error: DialError, other: impl FnOnce(u16) -> Response) -> Response {
    let Some(handshake) = error.response else {
        tracing::debug!(error = %error.message, "codex live upstream dial failed");
        // Go `HTTPStatusFromErrorOr(errDial, 502)`: a local refusal keeps its status.
        return other(error.status.unwrap_or(502));
    };
    let mut response = if handshake.status == 401 {
        let mut headers = HeaderMap::new();
        if let Some(content_type) = handshake.headers.get(header::CONTENT_TYPE).filter(|v| !v.is_empty()) {
            headers.insert(header::CONTENT_TYPE, content_type.clone());
        }
        super::http::written(401, headers, handshake.body)
    } else {
        other(handshake.status)
    };
    copy_headers(response.headers_mut(), &handshake.headers, &HANDSHAKE_HEADERS);
    response
}

/// Completes the downstream upgrade with the upstream's subprotocol, then runs the live
/// session and `ended` once it is over.
fn upgrade(
    ws: WebSocketUpgrade,
    upstream: LiveSocket,
    owner: Option<Extension<crate::net::listener::ConnectionWork>>,
    opened: Opened,
    ended: impl FnOnce() + Send + 'static,
) -> Response {
    let mut ws = ws
        .max_message_size(live::MAX_WS_MESSAGE)
        .max_frame_size(live::MAX_WS_MESSAGE);
    if let Some(protocol) = upstream.protocol.as_deref().and_then(|p| HeaderValue::from_str(p).ok()) {
        ws.set_selected_protocol(protocol);
    }
    crate::net::listener::websocket(ws, owner, move |downstream| async move {
        session::run(opened, downstream, upstream).await;
        ended();
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upgrade_detection_and_subprotocols_follow_gorilla() {
        let mut headers = HeaderMap::new();
        assert!(!is_upgrade(&headers));
        headers.insert(header::CONNECTION, "keep-alive, Upgrade".parse().unwrap());
        assert!(!is_upgrade(&headers));
        headers.insert(header::UPGRADE, "WebSocket".parse().unwrap());
        assert!(is_upgrade(&headers));
        headers.insert(
            header::SEC_WEBSOCKET_PROTOCOL,
            " realtime , openai-insecure-api-key.x ".parse().unwrap(),
        );
        headers.append(header::SEC_WEBSOCKET_PROTOCOL, "ignored".parse().unwrap());
        assert_eq!(subprotocols(&headers), ["realtime", "openai-insecure-api-key.x"]);
    }
}
