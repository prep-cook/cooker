//! Realtime and Live: OpenAI Realtime over the ChatGPT/Codex OAuth upstream
//! (internal/client/codex/live, realtime middleware in internal/api/server_middleware.go).
//!
//! - `POST /v1/live`, `/v1/realtime`, `/v1/realtime/calls`: WebRTC SDP negotiation with the
//!   Codex realtime calls endpoint ([`http::call`]). A successful call is remembered
//!   for an hour with its credential, so the sideband and hangup reuse it ([`calls`]).
//! - `GET /v1/live/{id}`, `/v1/realtime/calls/{id}`, `/v1/realtime?call_id=`: the call's
//!   sideband WebSocket; `GET /v1/realtime` without a call: a standard Realtime
//!   WebSocket ([`socket`]).
//! - `POST /v1/realtime/client_secrets` and the legacy `/v1/realtime/sessions`: local
//!   ephemeral keys (`ek_...`) scoped to one session config ([`secrets`]).
//! - Hangup is forwarded; transcription, translation and SIP control are 501 stubs.
//!
//! Three access modes, as Go registers them: `/v1/live*` sits in the ordinary `/v1`
//! group (plain `{"error": msg}`), realtime call and sideband routes also accept
//! ephemeral keys, and the rest take configured keys only with OpenAI-style errors.
//!
//! Request logs capture the live call and hangup upstream exchanges at Go's
//! `helps.Record*` call sites. The sockets are GET requests, which Go never logs.
//!
//! Each Realtime socket and sideband records its responses' usage ([`session`]); the call
//! and hangup exchanges record none. ponytail: Go also records a failed downstream write of
//! the call's answer; the text is the socket's own error, which `TrackedBody` cannot see.

mod calls;
#[cfg(test)]
mod capture_tests;
#[cfg(feature = "media-relay")]
mod dialer;
mod http;
#[cfg(feature = "media-relay")]
mod media;
mod relay;
mod secrets;
mod session;
mod socket;
#[cfg(feature = "media-relay")]
mod tunnel;

use std::sync::Arc;

use axum::Router;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use ck_core::config::Config;
use ck_core::credential::Credential;

use crate::runtime::state::{AcquireError, Lease, Runtime, Selection};

/// Realtime state shared by every route: remembered calls, ephemeral keys, the client socket
/// settings read at startup and the open sockets per key.
pub(crate) struct Live {
    calls: Arc<calls::Calls>,
    secrets: secrets::Secrets,
    relays: relay::Relays,
    settings: ck_core::config::Realtime,
    sockets: Arc<Sockets>,
}

impl Default for Live {
    fn default() -> Self {
        Self::new(ck_core::config::Realtime::default())
    }
}

impl Live {
    fn new(settings: ck_core::config::Realtime) -> Self {
        Self {
            calls: calls::Calls::new(),
            secrets: secrets::Secrets::default(),
            relays: relay::Relays::default(),
            settings,
            sockets: Arc::new(Sockets {
                limit: settings.max_sockets_per_key,
                open: Default::default(),
            }),
        }
    }
}

/// Open realtime sockets per client key, against `realtime.max-sockets-per-key`.
struct Sockets {
    /// 0 is no limit.
    limit: u32,
    open: std::sync::Mutex<std::collections::HashMap<String, u32>>,
}

/// One open socket of a key; dropping it frees its place.
struct Slot(Arc<Sockets>, String);

impl Sockets {
    /// A place for one more socket of `key`; `None` past the limit.
    fn take(self: &Arc<Self>, key: &str) -> Option<Slot> {
        if self.limit == 0 {
            return Some(Slot(self.clone(), String::new()));
        }
        let mut open = self.open.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let count = open.entry(key.to_owned()).or_default();
        if *count >= self.limit {
            return None;
        }
        *count += 1;
        Some(Slot(self.clone(), key.to_owned()))
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        if self.0.limit == 0 {
            return;
        }
        let mut open = self.0.open.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(count) = open.get_mut(&self.1) {
            *count -= 1;
            if *count == 0 {
                open.remove(&self.1);
            }
        }
    }
}

impl Drop for Live {
    /// Go `Handler.Close` on server shutdown.
    fn drop(&mut self) {
        self.calls.close_all("server_stopped");
    }
}

pub fn routes(rt: &Arc<Runtime>) -> Router<Arc<Runtime>> {
    // Deploy-only: read once when the routes are built.
    #[cfg_attr(not(any(test, feature = "testing")), allow(unused_mut))]
    let mut live = Live::new(rt.config().realtime());
    #[cfg(any(test, feature = "testing"))]
    if rt.passthrough_media_relay.load(std::sync::atomic::Ordering::Relaxed) {
        live.relays.fixed = Some(Arc::new(relay::PassThrough));
    }
    routes_with(rt, Arc::new(live))
}

fn routes_with(rt: &Arc<Runtime>, live: Arc<Live>) -> Router<Arc<Runtime>> {
    let auth = |mode| middleware::from_fn_with_state((rt.clone(), live.clone(), mode), authenticate);
    let ordinary = Router::new()
        .route("/v1/live", post(http::call))
        .route("/v1/live/{call_id}", get(socket::live_sideband))
        .route_layer(middleware::from_fn(socket::normalize_upgrade))
        .route_layer(auth(Mode::Ordinary));
    let realtime = Router::new()
        .route("/v1/realtime", get(socket::realtime).post(http::call))
        .route("/v1/realtime/calls", post(http::call))
        .route("/v1/realtime/calls/{call_id}", get(socket::calls_sideband))
        .route(
            "/v1/realtime/translations",
            get(http::translation).post(http::translation),
        )
        .route_layer(middleware::from_fn(socket::normalize_upgrade))
        .route_layer(auth(Mode::Realtime));
    let standard = Router::new()
        .route("/v1/realtime/client_secrets", post(secrets::create))
        .route("/v1/realtime/sessions", post(secrets::legacy))
        .route("/v1/realtime/transcription_sessions", post(http::transcription))
        .route("/v1/realtime/translations/client_secrets", post(http::translation))
        .route("/v1/realtime/calls/{call_id}/hangup", post(http::hangup))
        .route("/v1/realtime/calls/{call_id}/accept", post(http::sip))
        .route("/v1/realtime/calls/{call_id}/reject", post(http::sip))
        .route("/v1/realtime/calls/{call_id}/refer", post(http::sip))
        .route_layer(auth(Mode::Standard));
    ordinary.merge(realtime).merge(standard).layer(axum::Extension(live))
}

/// Which Go middleware guards a route.
#[derive(Clone, Copy)]
enum Mode {
    /// `AuthMiddleware`: configured keys, `{"error": msg}`.
    Ordinary,
    /// `realtimeStandardAuthMiddleware`: configured keys, OpenAI error envelope.
    Standard,
    /// `realtimeAuthMiddleware`: ephemeral `ek_` keys, else as `Standard`.
    Realtime,
}

/// Who called (Go `userApiKey` / `accessProvider`, plus the ephemeral key's grant).
#[derive(Clone, Default)]
pub(crate) struct Principal {
    pub key: String,
    pub provider: String,
    pub secret: Option<Arc<secrets::Grant>>,
    /// The downstream peer, as the listener records it.
    pub peer: Option<std::net::SocketAddr>,
}

impl Principal {
    /// Who owns the session stream: the key, and the ephemeral key's call scope.
    fn owner(&self) -> crate::inference::streams::Owner {
        crate::inference::streams::Owner {
            caller: ck_core::exec::Caller {
                principal: self.key.clone(),
                source: "authorization",
                provider: self.provider.clone(),
            },
            scope: self.secret.as_ref().map(|grant| grant.principal.clone()),
        }
    }

    /// The usage record's request facts for a socket on `endpoint` asking for `model`.
    fn facts(
        &self,
        cfg: &Config,
        headers: &HeaderMap,
        endpoint: &str,
        model: &str,
    ) -> Arc<crate::observability::usage_record::Facts> {
        let trace = crate::inference::dispatch::Trace::with_request_id(crate::observability::api::current_request_id());
        let client = crate::observability::usage_record::Client {
            request_id: trace.request_id(),
            endpoint: endpoint.to_owned(),
            api_key: self.key.clone(),
            access_provider: self.provider.clone(),
            ..crate::inference::dispatch::request_client(cfg, headers, self.peer)
        };
        Arc::new(crate::observability::usage_record::Facts::new(
            client,
            ck_core::format::Format::OpenAI,
            ck_core::format::Format::OpenAI,
            model,
            b"{}",
            true,
            &trace,
        ))
    }

    /// `liveSelectionHeaders`: a local ephemeral key never reaches session affinity.
    fn selection_headers(&self, headers: &HeaderMap) -> HeaderMap {
        let mut headers = headers.clone();
        if self.secret.is_some() {
            headers.remove(header::AUTHORIZATION);
            headers.remove(header::PROXY_AUTHORIZATION);
        }
        headers
    }
}

async fn authenticate(
    State((rt, live, mode)): State<(Arc<Runtime>, Arc<Live>, Mode)>,
    mut req: Request,
    next: Next,
) -> Response {
    let authorization = req
        .headers()
        .get(header::AUTHORIZATION)
        .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
        .unwrap_or_default();
    let token = ck_exec::codex_live::bearer_token(&authorization);
    let principal = if matches!(mode, Mode::Realtime) && token.starts_with(secrets::PREFIX) {
        let Some(grant) = live.secrets.authenticate(token) else {
            return realtime_error(
                401,
                "Realtime client secret is invalid or expired",
                "invalid_request_error",
                "invalid_realtime_client_secret",
            );
        };
        Principal {
            key: Some(&grant.issuer_key)
                .filter(|k| !k.is_empty())
                .unwrap_or(&grant.principal)
                .clone(),
            provider: Some(&grant.issuer_provider)
                .filter(|p| !p.is_empty())
                .cloned()
                .unwrap_or_else(|| "realtime-client-secret".into()),
            secret: Some(grant),
            peer: None,
        }
    } else {
        match crate::net::access::client_caller(&rt, req.headers(), req.uri().query().unwrap_or_default()) {
            // No client keys: Go sets nothing.
            Ok(caller) => Principal {
                key: caller.principal,
                provider: caller.provider,
                secret: None,
                peer: None,
            },
            Err(message) if matches!(mode, Mode::Ordinary) => {
                let body = crate::http::gojson::Obj::new().str("error", message).finish();
                return crate::http::respond::gin_json(401, body);
            }
            Err(message) => {
                return realtime_error(401, message, "authentication_error", "invalid_api_key");
            }
        }
    };
    let peer = req
        .extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
        .map(|info| info.0);
    req.extensions_mut().insert(Principal { peer, ..principal });
    next.run(req).await
}

/// `writeRealtimeError`: gin.H marshals with sorted keys.
fn realtime_error(status: u16, message: &str, kind: &str, code: &str) -> Response {
    let detail = crate::http::gojson::Obj::new()
        .str("code", code)
        .str("message", message)
        .raw("param", "null")
        .str("type", kind)
        .finish();
    crate::http::respond::gin_json(status, crate::http::gojson::Obj::new().raw("error", &detail).finish())
}

/// `writeLiveError`: the OpenAI envelope under `/v1/realtime`, else `{"error": msg}`.
fn live_error(realtime: bool, status: u16, message: &str) -> Response {
    if !realtime {
        return crate::http::respond::gin_json(status, crate::http::gojson::Obj::new().str("error", message).finish());
    }
    let kind = match status {
        401 => "authentication_error",
        400..=499 => "invalid_request_error",
        _ => "api_error",
    };
    realtime_error(status, message, kind, "realtime_request_failed")
}

/// gin's recovery after a Go panic: 500 with no body.
fn panic_response() -> Response {
    StatusCode::INTERNAL_SERVER_ERROR.into_response()
}

/// A failed credential selection (`writeSelectionError`): status, Go's error text and
/// the scheduler's `Retry-After`.
struct Rejection {
    status: u16,
    text: String,
    /// Seconds, from the scheduler error (`auth.SafeResponseHeaders`).
    retry_after: Option<u64>,
}

impl Rejection {
    fn render(self, realtime: bool) -> Response {
        let mut response = live_error(realtime, self.status, &self.text);
        if let Some(seconds) = self.retry_after {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from(seconds));
        }
        response
    }
}

/// `selectOAuth`: `SelectAuthByKind(codex, "", oauth)`, optionally pinned to one
/// credential (`PinnedAuthMetadataKey`). Session affinity follows the request's session
/// identity. The lease records no outcome (Go marks none for live traffic).
async fn select_oauth(
    rt: &Arc<Runtime>,
    cfg: &Config,
    pinned: Option<&str>,
    headers: &HeaderMap,
    body: &[u8],
    execution_session: Option<&str>,
) -> Result<Lease, Rejection> {
    let policy = rt.policy();
    let exclude = rt
        .store()
        .snapshot()
        .iter()
        .filter(|c| c.provider == "codex")
        .filter(|c| ck_core::registry::dynamic::auth_kind(c) != Some("oauth") || pinned.is_some_and(|id| id != c.id))
        .map(|c| c.id.clone())
        .collect();
    let (primary, parent, fork) = session_hierarchy(headers, body, execution_session);
    let bound = |s: String| (!s.is_empty()).then(|| ck_wire::session::bound_session_identity(&s));
    let selection = Selection {
        provider: "codex".into(),
        session: bound(primary),
        session_parent: bound(parent),
        session_fork: fork,
        exclude,
        // Go selects with an empty model; an existing call's selection names its
        // credential and call (sideband.go:381-388, capabilities.go:80-87).
        ..Selection::default()
    };
    rt.acquire(selection, cfg, policy, &rt.registry())
        .await
        .map_err(|error| match error {
            AcquireError::Unavailable { retry_after: None, .. } => Rejection {
                status: 503,
                text: "auth_not_found: no auth available".into(),
                retry_after: None,
            },
            AcquireError::Unavailable {
                retry_after: Some(wait),
                cause,
            } => {
                // `authUnavailableError`: the earliest retry and the last upstream error.
                let mut text = "auth_unavailable: no auth available".to_owned();
                if let Some(summary) = cause
                    .as_deref()
                    .map(crate::inference::dispatch::upstream_summary)
                    .filter(|s| !s.is_empty() && !text.contains(s.as_str()))
                {
                    text.push_str(&format!(" (last upstream error: {summary})"));
                }
                let failure = crate::inference::dispatch::Failure::Unavailable {
                    code: "auth_unavailable",
                    providers: vec!["codex".into()],
                    model: String::new(),
                    cause: None,
                    retry_after: Some(wait),
                };
                Rejection {
                    status: 503,
                    text,
                    retry_after: failure.retry_after(),
                }
            }
            AcquireError::Cooldown { wait, cause } => {
                let failure = crate::inference::dispatch::Failure::Cooldown {
                    model: String::new(),
                    provider: "codex".into(),
                    wait,
                    cause,
                };
                Rejection {
                    status: 429,
                    text: failure.text(),
                    retry_after: failure.retry_after(),
                }
            }
            AcquireError::Prepare { error, .. } => Rejection {
                status: error.status,
                text: String::from_utf8_lossy(&error.body).into_owned(),
                retry_after: None,
            },
        })
}

/// The request's session and its parent (`EnrichContextWithSessionHierarchy`): the
/// explicit identity when there is one, else the derived one; plus the fork flag.
fn session_hierarchy(headers: &HeaderMap, body: &[u8], execution_session: Option<&str>) -> (String, String, bool) {
    let meta = ck_wire::session::Meta {
        execution_session,
        derived: None,
    };
    let (primary, parent, fork) = ck_wire::session::explicit_session_ids(headers, body, &meta);
    if !primary.is_empty() {
        return (primary, parent, fork);
    }
    let (primary, parent) = ck_wire::session::session_ids(headers, body, &meta);
    (primary, parent, fork)
}

/// The session a call remembers.
fn session_id(headers: &HeaderMap, body: &[u8]) -> String {
    session_hierarchy(headers, body, None).0
}

/// `$CPA-SESSION-ID` for custom headers: the call's stored session, else the request's.
fn header_session(headers: &HeaderMap, body: &[u8], execution_session: Option<&str>, stored: &str) -> Option<String> {
    if !stored.is_empty() {
        return ck_wire::session::cpa_session_id(Some(stored));
    }
    let meta = ck_wire::session::Meta {
        execution_session,
        derived: None,
    };
    ck_wire::session::cpa_session_id(Some(&ck_wire::session::extract_session_id(headers, body, &meta)))
}

/// Go's `X-Ck-Trace-Id`, stamped when the credential is selected
/// (`logging.SetGinCPATraceID`): the time, the auth index and the request ID.
fn trace(credential: &Credential) -> Option<HeaderValue> {
    let trace = crate::inference::dispatch::Trace::with_request_id(crate::observability::api::current_request_id());
    trace.selected(credential);
    trace.id().and_then(|id| HeaderValue::from_str(&id).ok())
}

/// Adds the trace header to a response sent after the selection.
fn with_trace(mut response: Response, trace: &Option<HeaderValue>) -> Response {
    if let Some(value) = trace {
        response.headers_mut().insert("x-ck-trace-id", value.clone());
    }
    response
}
