//! Management-started logins (Go auth_files_provider_oauth.go, oauth_sessions.go,
//! oauth_callback.go, auth_files_oauth_callback.go, auth_files_v8.go):
//! `GET /oauth/auth-url`, `GET /oauth/status`, `DELETE /oauth/session`,
//! `GET|POST /oauth/callback` and `POST /oauth/import` (vertex_import.go).
//!
//! Go hands callbacks to the waiting login through `.oauth-<provider>-<state>.oauth`
//! files in auth-dir; cooker keeps the callback with the pending session in
//! memory, so nothing extra appears in the watched directory.

use ck_core::gostr::GoStr;
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime};

use axum::extract::{RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use ck_core::exec::ExecError;
use ck_exec::login_store::{LoginSave, save_login};
use ck_exec::proxy::Proxy;
use futures_util::FutureExt;
use serde_json::{Value, json};

use super::auth_files::{Query, fail, reply};
use super::{Management, json as respond};
use crate::{runtime::state::OAuthDelivery as Delivery, store::config_store};

const SESSION_TTL: Duration = Duration::from_secs(30 * 60);
/// Go `oauthCompletedSessionTTL`, for a binding failed by a file change. A completed
/// sign-in keeps [`SESSION_TTL`] instead (DIFFERENCES-FROM-GO.md).
const COMPLETED_TTL: Duration = Duration::from_secs(60);
const CALLBACK_WAIT: Duration = Duration::from_secs(5 * 60);
const ANTHROPIC_CALLBACK_PORT: u16 = 54545;
const CODEX_CALLBACK_PORT: u16 = 1455;

#[derive(Clone)]
struct Session {
    identity: Arc<()>,
    provider: String,
    status: String,
    completed: bool,
    expires: Instant,
    callback: Option<Callback>,
    /// A v8 Sign in with ChatGPT binding (Go `oauthSession.SIWC`).
    siwc: Option<Arc<super::siwc::Flow>>,
    /// The binding's callback was accepted and its exchange started.
    siwc_exchanging: bool,
    /// Go `SIWCCommitting`: a started disk commit keeps the session past its expiry.
    siwc_committing: bool,
}

#[derive(Clone)]
struct Callback {
    code: String,
    state: String,
    error: String,
}

/// Go `oauthSessionStore`.
#[derive(Default)]
pub(crate) struct Sessions(Mutex<HashMap<String, Session>>);

impl Sessions {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Session>> {
        let mut map = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let now = Instant::now();
        map.retain(|_, s| {
            let keep = s.expires > now || s.siwc_committing;
            if let Some(flow) = s.siwc.as_ref().filter(|_| !keep) {
                flow.cancel();
            }
            keep
        });
        map
    }

    fn register(&self, state: &str, provider: &str) {
        let (state, provider) = (state.trim(), provider.trim().go_lower());
        if state.is_empty() || provider.is_empty() {
            return;
        }
        self.lock().insert(
            state.to_owned(),
            Session {
                identity: Arc::new(()),
                provider,
                status: String::new(),
                completed: false,
                expires: Instant::now() + SESSION_TTL,
                callback: None,
                siwc: None,
                siwc_exchanging: false,
                siwc_committing: false,
            },
        );
    }

    /// Go `GetOAuthSessionDetails`: `(provider, status, completed)`.
    pub(super) fn details(&self, state: &str) -> Option<(String, String, bool)> {
        self.lock()
            .get(state.trim())
            .map(|s| (s.provider.clone(), s.status.clone(), s.completed))
    }

    pub(super) fn set_error(&self, state: &str, message: &str) {
        let message = match message.trim() {
            "" => "Authentication failed",
            m => m,
        };
        if let Some(s) = self.lock().get_mut(state.trim()).filter(|s| !s.completed) {
            s.status = message.to_owned();
            s.siwc_committing = false;
            s.expires = Instant::now() + SESSION_TTL;
        }
    }

    pub(super) fn complete(&self, state: &str) {
        if let Some(s) = self.lock().get_mut(state.trim()).filter(|s| !s.completed) {
            s.status.clear();
            s.callback = None;
            s.siwc_committing = false;
            s.completed = true;
            // Go keeps it a minute; a user who approves on the provider's tab and comes
            // back later should still read the success.
            s.expires = Instant::now() + SESSION_TTL;
        }
    }

    /// `(provider, status, completed)`.
    fn get(&self, state: &str) -> Option<(String, String, bool)> {
        self.lock()
            .get(state.trim())
            .map(|s| (s.provider.clone(), s.status.clone(), s.completed))
    }

    pub(super) fn is_pending(&self, state: &str, provider: &str) -> bool {
        let provider = provider.trim().go_lower();
        self.lock().get(state.trim()).is_some_and(|s| {
            !s.completed && s.status.is_empty() && (provider.is_empty() || s.provider.go_eq_fold(&provider))
        })
    }

    /// Go `Cancel`. A binding whose disk commit has started is past cancelling: Go
    /// waits on the session gate for that commit, which then leaves the session
    /// completed or failed, so the answer is `false` either way.
    fn cancel(&self, state: &str) -> bool {
        let mut map = self.lock();
        let state = state.trim();
        if map
            .get(state)
            .is_some_and(|s| !s.completed && s.status.is_empty() && !s.siwc_committing)
        {
            if let Some(flow) = map.remove(state).and_then(|s| s.siwc) {
                flow.cancel();
            }
            return true;
        }
        false
    }

    /// Go `WriteOAuthCallbackFileForPendingSession`: keeps the callback for the waiter.
    /// A binding's state is refused: its code may only reach the v8 callback.
    fn deliver(&self, provider: &str, state: &str, code: &str, error: &str) -> Delivery {
        let Some(provider) = normalize_callback_provider(provider) else {
            return Delivery::NotPending;
        };
        if !valid_state(state) {
            return Delivery::NotPending;
        }
        let mut map = self.lock();
        let Some(s) = map.get_mut(state.trim()) else {
            return Delivery::NotPending;
        };
        if s.siwc.is_some() {
            return Delivery::RequiresV8;
        }
        if s.completed || !s.status.is_empty() || !s.provider.go_eq_fold(&provider) {
            return Delivery::NotPending;
        }
        s.callback = Some(Callback {
            code: code.trim().to_owned(),
            state: state.trim().to_owned(),
            error: error.trim().to_owned(),
        });
        Delivery::Delivered
    }

    /// Go `registerSIWC`: `false` when the state exists or the credential already has
    /// a pending binding.
    pub(super) fn register_siwc(&self, state: &str, flow: Arc<super::siwc::Flow>, ttl: Duration) -> bool {
        let mut map = self.lock();
        let busy = map.values().any(|s| {
            !s.completed && s.status.is_empty() && s.siwc.as_ref().is_some_and(|f| f.target_id == flow.target_id)
        });
        if busy || map.contains_key(state) {
            return false;
        }
        map.insert(
            state.to_owned(),
            Session {
                identity: Arc::new(()),
                provider: "codex".into(),
                status: String::new(),
                completed: false,
                expires: Instant::now() + ttl,
                callback: None,
                siwc: Some(flow),
                siwc_exchanging: false,
                siwc_committing: false,
            },
        );
        true
    }

    /// The binding a state belongs to, whatever its status.
    pub(super) fn siwc(&self, state: &str) -> Option<Arc<super::siwc::Flow>> {
        self.lock().get(state.trim()).and_then(|s| s.siwc.clone())
    }

    /// Accepts the binding's one callback while it is pending.
    pub(super) fn begin_siwc(&self, state: &str, flow: &Arc<super::siwc::Flow>) -> bool {
        let mut map = self.lock();
        match map.get_mut(state) {
            Some(s) if Self::siwc_pending(s, flow) && !s.siwc_exchanging => {
                s.siwc_exchanging = true;
                true
            }
            _ => false,
        }
    }

    /// The binding is still pending (not cancelled, expired, failed or completed).
    pub(super) fn siwc_is_pending(&self, state: &str, flow: &Arc<super::siwc::Flow>) -> bool {
        self.lock().get(state).is_some_and(|s| Self::siwc_pending(s, flow))
    }

    /// Go `claimSIWCCommit`; callers hold `disk`, so file mutations are fenced.
    pub(super) fn claim_siwc_commit(&self, state: &str, flow: &Arc<super::siwc::Flow>) -> bool {
        let mut map = self.lock();
        match map.get_mut(state) {
            Some(s) if Self::siwc_pending(s, flow) && !s.siwc_committing => {
                s.siwc_committing = true;
                true
            }
            _ => false,
        }
    }

    pub(super) fn release_siwc_commit(&self, state: &str, flow: &Arc<super::siwc::Flow>) {
        if let Some(s) = self.lock().get_mut(state).filter(|s| Self::siwc_pending(s, flow)) {
            s.siwc_committing = false;
        }
    }

    fn siwc_pending(s: &Session, flow: &Arc<super::siwc::Flow>) -> bool {
        !s.completed && s.status.is_empty() && s.siwc.as_ref().is_some_and(|f| Arc::ptr_eq(f, flow))
    }

    /// Go `beginSIWCFileMutation` (callers hold `disk`): a v8 upload or delete of a
    /// binding's file, even an identical one, fails that binding.
    pub(super) fn invalidate_siwc(&self, path: &std::path::Path) {
        let mut map = self.lock();
        for s in map.values_mut() {
            if let Some(flow) = s.siwc.as_ref()
                && !s.completed
                && s.status.is_empty()
                && super::siwc::same_path(path, &flow.path)
            {
                flow.cancel();
                s.status = "SIWC binding target changed; start a new binding".into();
                s.expires = Instant::now() + COMPLETED_TTL;
            }
        }
    }

    fn take_callback(&self, state: &str) -> Option<Callback> {
        self.lock().get_mut(state).and_then(|s| s.callback.take())
    }
}

/// Go `ValidateOAuthState`.
pub(super) fn valid_state(state: &str) -> bool {
    let s = state.trim();
    !s.is_empty()
        && s.len() <= 128
        && !s.contains("..")
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

/// Go `NormalizeOAuthCallbackProvider`: the built-in aliases, else the lowercased
/// name when it holds only letters, digits and hyphens.
pub(crate) fn normalize_callback_provider(provider: &str) -> Option<String> {
    let p = provider.trim().go_lower();
    let builtin = match p.as_str() {
        "anthropic" | "claude" => Some("anthropic"),
        "codex" | "openai" => Some("codex"),
        "antigravity" | "anti-gravity" => Some("antigravity"),
        "xai" | "x-ai" | "x.ai" | "grok" => Some("xai"),
        "devin" | "cognition" => Some("devin"),
        "meta" | "muse" => Some("meta"),
        _ => None,
    };
    if let Some(b) = builtin {
        return Some(b.to_owned());
    }
    (!p.is_empty()
        && p.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'))
    .then_some(p)
}

/// Go `oauthSessionErrorWithCause`.
fn with_cause(message: &str, cause: &str) -> String {
    match redact_upstream(cause).trim() {
        "" => message.to_owned(),
        detail => format!("{message}: {detail}"),
    }
}

/// Cuts upstream-supplied text from a login failure cause: a response body after its
/// status, an OAuth error description after its error code, and a decoder diagnostic
/// (which quotes response values) after its `parse ...` context. Go keeps them, but
/// they can carry tokens and session errors are readable by anyone holding the state
/// (the key-less callback). The earliest boundary wins: upstream text follows it.
fn redact_upstream(cause: &str) -> String {
    let mut cut: Option<(usize, String)> = None;
    let mut consider = |at: usize, kept: String| {
        if cut.as_ref().is_none_or(|(i, _)| at < *i) {
            cut = Some((at, kept));
        }
    };
    for marker in ["with status ", "(HTTP "] {
        if let Some(i) = cause.find(marker) {
            let start = i + marker.len();
            let mut end = start + cause[start..].bytes().take_while(u8::is_ascii_digit).count();
            if marker == "(HTTP " && cause[end..].starts_with(')') {
                end += 1;
            }
            consider(i, cause[..end].to_owned());
        }
    }
    for marker in [
        "error from authorization server: ",
        "device token error: ",
        "OAuth error: ",
    ] {
        if let Some(i) = cause.find(marker) {
            let start = i + marker.len();
            let code: String = cause[start..]
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .take(64)
                .collect();
            consider(i, format!("{}{code}", &cause[..start]));
        }
    }
    if let Some(i) = cause.find("parse ")
        && let Some(j) = cause[i..].find(": ")
    {
        consider(i, cause[..i + j].to_owned());
    }
    cut.map_or_else(|| cause.to_owned(), |(_, kept)| kept)
}

fn exec_text(e: &ExecError) -> String {
    String::from_utf8_lossy(&e.body).into_owned()
}

/// Installs the main listener's provider-callback hook (Go's `/anthropic/callback`
/// etc. call `WriteOAuthCallbackFileForPendingSession`).
pub(super) fn install_callback_sink(state: &Arc<Management>) {
    let weak = Arc::downgrade(state);
    state
        .rt
        .set_oauth_callback_sink(Some(Arc::new(move |cb: &crate::runtime::state::OAuthCallback| {
            weak.upgrade().map_or(Delivery::NotPending, |m| {
                m.oauth.deliver(cb.provider, &cb.state, &cb.code, &cb.error)
            })
        })));
}

fn status_error(status: StatusCode, message: &str) -> Response {
    respond(status, &json!({"status": "error", "error": message}))
}

fn unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos())
}

/// Go `misc.GenerateRandomState`: 16 random bytes, hex.
fn random_state() -> Option<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).ok()?;
    Some(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

/// `requests.proxy-url`, which Go's auth services use.
fn global_proxy(state: &Management) -> Proxy {
    let cfg = state.rt.config();
    let global = cfg
        .document
        .get("requests")
        .and_then(|r| r.get("proxy-url"))
        .and_then(serde_yaml_ng::Value::as_str)
        .unwrap_or_default();
    Proxy::parse(global)
}

/// The client for login traffic: the global proxy (else the environment's proxies);
/// local endpoints in tests.
pub(super) fn login_client(state: &Management) -> wreq::Client {
    if state.login_base.is_some() {
        return state.clients.get(&Proxy::Direct);
    }
    state.clients.get(&global_proxy(state))
}

/// New grant work recovers publication before network I/O, without carrying the
/// owner gate over a provider call.
pub(super) async fn admit_grant_work(state: &Arc<Management>) -> bool {
    if state.stopping.is_cancelled() {
        return false;
    }
    let backend = state.rt.store().credential_backend();
    state
        .work
        .clone()
        .spawn_blocking(move || backend.admit_work())
        .await
        .is_ok_and(|result| result.is_ok())
        && !state.stopping.is_cancelled()
}

/// Go `StartOAuthV8`.
pub(crate) async fn auth_url(State(state): State<Arc<Management>>, RawQuery(raw): RawQuery) -> Response {
    if !admit_grant_work(&state).await {
        return fail(StatusCode::SERVICE_UNAVAILABLE, "credential recovery required");
    }
    let q = Query::parse(raw);
    let webui = matches!(
        q.first("is_webui").trim().go_lower().as_str(),
        "1" | "true" | "yes" | "on"
    );
    match q.first("provider").trim().go_lower().as_str() {
        "" => fail(StatusCode::BAD_REQUEST, "provider is required"),
        "claude" => start_claude(state, webui).await,
        "codex" => start_codex(state, webui).await,
        "antigravity" => start_antigravity(state, webui).await,
        "kimi" => {
            // Go `RequestKimiToken`: `domain`, else `channel`, else kimi.com.
            let domain = [q.first("domain"), q.first("channel")]
                .into_iter()
                .map(str::trim)
                .find(|v| !v.is_empty())
                .unwrap_or(ck_exec::kimi_auth::DOMAIN_COM)
                .to_owned();
            start_kimi(state, domain).await
        }
        "kimi-ai" => start_kimi(state, ck_exec::kimi_auth::DOMAIN_AI.to_owned()).await,
        "meta" => start_meta(state).await,
        "xai" => start_xai(state).await,
        "devin" => start_devin(state).await,
        _ => fail(StatusCode::NOT_FOUND, "provider_not_found"),
    }
}

/// Go `ImportOAuthV8`.
pub(crate) async fn import(
    State(state): State<Arc<Management>>,
    RawQuery(raw): RawQuery,
    headers: HeaderMap,
    read: crate::http::body_limit::Read,
) -> Response {
    let query = Query::parse(raw);
    match query.first("provider").trim().go_lower().as_str() {
        "" => fail(StatusCode::BAD_REQUEST, "provider is required"),
        "vertex" => import_vertex(state, &query, &headers, read).await,
        _ => fail(StatusCode::NOT_FOUND, "provider_not_found"),
    }
}

/// Go `ImportVertexCredential`: a service-account key uploaded as the `file` form field;
/// `location` from the form, else the query.
async fn import_vertex(
    state: Arc<Management>,
    query: &Query,
    headers: &HeaderMap,
    read: crate::http::body_limit::Read,
) -> Response {
    use super::multipart::Failed;
    use ck_exec::vertex_auth::{UploadError, import_upload};
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    // Go `FormFile("file")`: any form error reads as a missing file, with the limit's
    // status for the limit error.
    let body = match super::multipart::body(read, content_type) {
        Ok(body) => body,
        Err(Failed::Read(rejection)) => return rejection.into_response(),
        Err(Failed::Form(status, _)) => {
            return fail(
                StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_REQUEST),
                "file required",
            );
        }
    };
    let Ok((mut files, values)) = super::multipart::form(content_type, &body) else {
        return fail(StatusCode::BAD_REQUEST, "file required");
    };
    let Some(file) = files.remove("file").and_then(|parts| parts.into_iter().next()) else {
        return fail(StatusCode::BAD_REQUEST, "file required");
    };
    let mut location = values
        .get("location")
        .and_then(|v| v.first())
        .map(|v| v.trim().to_owned())
        .unwrap_or_default();
    if location.is_empty() {
        location = query.first("location").trim().to_owned();
    }
    let auth_dir = state.rt.config().auth_dir.clone();
    let saved = state
        .work
        .clone()
        .spawn_blocking(move || {
            let _guard = state.disk.lock().unwrap_or_else(PoisonError::into_inner);
            let mut upload = import_upload(&auth_dir, &file.data, &location)?;
            state
                .rt
                .store()
                .credential_backend()
                .write_files(std::slice::from_ref(&upload.path.clone()), || {
                    upload
                        .write()
                        .map_err(|_| anyhow::anyhow!("failed to write Vertex credential"))
                })
                .map_err(|e| UploadError::Save(format!("{e:#}")))?;
            state.credential_changes.notify_one();
            state.publish((*state.rt.config()).clone(), None);
            Ok(upload)
        })
        .await;
    let message = |error: &str, message: String| {
        reply(
            StatusCode::BAD_REQUEST,
            [("error", Value::from(error)), ("message", Value::from(message))],
        )
    };
    match saved {
        Ok(Ok(upload)) => reply(
            StatusCode::OK,
            [
                ("status", "ok".into()),
                ("auth-file", upload.path.to_string_lossy().into_owned().into()),
                ("project_id", upload.project.into()),
                ("email", upload.email.into()),
                ("location", upload.location.into()),
            ],
        ),
        Ok(Err(UploadError::InvalidJson(e))) => message("invalid json", e),
        Ok(Err(UploadError::InvalidServiceAccount(e))) => message("invalid service account", e),
        Ok(Err(UploadError::ProjectMissing)) => fail(StatusCode::BAD_REQUEST, "project_id missing"),
        Ok(Err(UploadError::Save(e))) => reply(
            StatusCode::INTERNAL_SERVER_ERROR,
            [("error", Value::from("save_failed")), ("message", Value::from(e))],
        ),
        Err(_) => fail(StatusCode::INTERNAL_SERVER_ERROR, "save_failed"),
    }
}

/// How a login ended, for the session (Go's `SetOAuthSessionError` texts).
enum Outcome {
    Saved,
    /// The session stopped being pending: nothing is saved or reported.
    Cancelled,
    Failed(String),
}

/// Saves through `write` only while the session is still pending (Go
/// `guardOAuthSessionPendingForSave`), then resyncs credentials.
struct PrivateLoginRender(PathBuf);
impl PrivateLoginRender {
    fn new() -> anyhow::Result<Self> {
        let path = std::env::temp_dir().join(format!("cooker-login-render-{}", uuid::Uuid::new_v4()));
        let create = std::fs::DirBuilder::new();
        #[cfg(unix)]
        let mut create = create;
        #[cfg(unix)]
        std::os::unix::fs::DirBuilderExt::mode(&mut create, 0o700);
        create.create(&path)?;
        Ok(Self(path))
    }
}
impl Drop for PrivateLoginRender {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A deterministic refusal cannot be repaired by retrying local I/O.
#[derive(Debug)]
pub(crate) struct LoginRejected;
impl std::fmt::Display for LoginRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("login result is no longer admissible")
    }
}
impl std::error::Error for LoginRejected {}

// ponytail: legacy writers expose strings, not typed effects. Keep opaque
// failures retryable; migrate this boundary when adding another validation code.
pub(crate) fn login_render_error(error: String) -> anyhow::Error {
    if error.starts_with("auth filestore: invalid metadata weight:")
        || matches!(
            error.as_str(),
            "invalid credential email filename"
                | "claude token storage missing account information"
                | "codex token storage missing account information"
                | "meta token storage missing access token"
        )
    {
        anyhow::Error::new(LoginRejected)
    } else {
        anyhow::Error::msg(error)
    }
}

type LoginRenderer =
    dyn Fn(&crate::store::config_store::State) -> anyhow::Result<Vec<(PathBuf, Option<Vec<u8>>)>> + Send + Sync;

/// The returned provider value stays captured here until complete bytes reach the
/// pending candidate. Cache repair and legacy merge failures can then be retried
/// without exchanging another grant.
pub(crate) struct HeldLogin {
    backend: std::sync::Weak<crate::store::credential_store::CredentialDb>,
    // Issued while pending; UI expiry/cancellation cannot un-consume the result.
    _session: Arc<()>,
    pub(crate) provider: String,
    render: Box<LoginRenderer>,
    destinations: Arc<Mutex<std::collections::BTreeMap<String, [u8; 32]>>>,
    rejected: Box<dyn Fn() + Send + Sync>,
}
impl HeldLogin {
    pub(crate) fn finish(
        &self,
        current: &crate::store::config_store::State,
        pending: &mut Option<crate::store::config_store::Pending>,
    ) -> anyhow::Result<()> {
        let backend = self
            .backend
            .upgrade()
            .ok_or_else(|| anyhow::Error::new(LoginRejected))?;
        let documents = (self.render)(current)?;
        backend.retain_login(documents, pending)
    }

    pub(crate) fn reject(&self, current: &config_store::State, store: &config_store::ConfigStore) {
        store.reject_login_sources(
            current,
            &self.destinations.lock().unwrap_or_else(PoisonError::into_inner),
        );
        (self.rejected)();
    }
}

async fn save_if_pending<T: Clone + Send + Sync + 'static>(
    state: &Arc<Management>,
    sid: &str,
    provider: &str,
    value: T,
    write: impl Fn(PathBuf, T, LoginSave) -> Result<PathBuf, String> + Send + Sync + 'static,
    save_error: &str,
) -> Outcome {
    let session = {
        let sessions = state.oauth.lock();
        let Some(session) = sessions
            .get(sid)
            .filter(|s| !s.completed && s.status.is_empty() && s.provider.go_eq_fold(provider))
        else {
            return Outcome::Cancelled;
        };
        session.identity.clone()
    };
    let backend = state.rt.store().credential_backend();
    let expected = Arc::new(Mutex::new(None));
    let db = Arc::downgrade(&backend);
    let owner = Arc::downgrade(state);
    let destinations = Arc::new(Mutex::new(std::collections::BTreeMap::new()));
    let resolved = destinations.clone();
    let rejected = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let rejection = rejected.clone();
    let sid = sid.to_owned();
    let save_error = save_error.to_owned();
    let failure = save_error.clone();
    let reporter = owner.clone();
    let held = HeldLogin {
        backend: db.clone(),
        _session: session,
        provider: provider.to_owned(),
        destinations,
        rejected: Box::new(move || {
            rejection.store(true, std::sync::atomic::Ordering::SeqCst);
            if let Some(owner) = reporter.upgrade() {
                owner.oauth.set_error(&sid, &failure);
            }
        }),
        render: Box::new(move |current| {
            let output = Arc::new(Mutex::new(None));
            let result = output.clone();
            let render_error = Arc::new(Mutex::new(None));
            let captured_error = render_error.clone();
            let resolved = resolved.clone();
            let db = db.clone();
            let expected = expected.clone();
            let current = current.clone();
            {
                let mut captured = expected.lock().unwrap_or_else(PoisonError::into_inner);
                if captured.is_none() {
                    *captured = Some(
                        db.upgrade()
                            .ok_or_else(|| anyhow::Error::new(LoginRejected))?
                            .login_sources(&current)?,
                    );
                }
            }
            let render_dir = PrivateLoginRender::new()?;
            db.upgrade()
                .ok_or_else(|| anyhow::Error::new(LoginRejected))?
                .hydrate_login_render(&current, &render_dir.0)?;
            let save: LoginSave = Arc::new(move |paths, write| {
                let db = db.upgrade().ok_or("login owner stopped")?;
                let documents = db
                    .render_login(
                        &current,
                        paths,
                        &mut expected.lock().unwrap_or_else(PoisonError::into_inner),
                        &mut resolved.lock().unwrap_or_else(PoisonError::into_inner),
                        write,
                    )
                    .map_err(|error| {
                        let message = format!("{error:#}");
                        *captured_error.lock().unwrap_or_else(PoisonError::into_inner) = Some(error);
                        message
                    })?;
                *result.lock().unwrap_or_else(PoisonError::into_inner) = Some(documents);
                Ok(())
            });
            assert!(!owner.upgrade().is_some_and(|s| s.login_panic), "injected login panic");
            if let Err(error) = write(render_dir.0.clone(), value.clone(), save) {
                return Err(render_error
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take()
                    .unwrap_or_else(|| login_render_error(error)));
            }
            let documents = output
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
                .ok_or_else(|| anyhow::Error::new(LoginRejected))?;
            Ok(documents)
        }),
    };
    // Transfer ownership before locks, hydration or provider merge/write effects.
    state.config_store().hold_login(held);
    let saved = async {
        let (worker, admitted) = (state.clone(), backend.clone());
        let admission = save_blocking(state, move || {
            let admission = {
                let _disk = worker.disk.lock().unwrap_or_else(PoisonError::into_inner);
                admitted.admit_work()
            };
            anyhow::ensure!(!rejected.load(std::sync::atomic::Ordering::SeqCst), LoginRejected);
            Ok(admission)
        })
        .await?;
        // The result committed at admission, so the cache repair never gives up on a busy
        // document: it waits for that lock alone, off `disk` and the store, so the refresh
        // holding it can commit, then repairs again. Shutdown stops the wait; the stored
        // credential stays, and `CredentialDb::initialize` restores every cache file at
        // the next start.
        loop {
            let (worker, repairing) = (state.clone(), backend.clone());
            let busy = save_blocking(state, move || {
                let _disk = worker.disk.lock().unwrap_or_else(PoisonError::into_inner);
                repairing.repair_cache()
            })
            .await?;
            let Some(busy) = busy else { break };
            #[cfg(test)]
            crate::testing::installation_effects::run(
                &busy,
                crate::testing::installation_effects::Stage::LoginRepairBusy,
            )?;
            tokio::select! {
                biased;
                _ = state.stopping.cancelled() => break,
                lock = state.rt.lock_codex_file(&busy) => {
                    let lock = lock.map_err(anyhow::Error::msg)?;
                    ck_exec::codex_siwc_bind::unlock(&lock).map_err(anyhow::Error::msg)?;
                }
            }
        }
        admission?;
        state.credential_changes.notify_one();
        Ok::<_, anyhow::Error>(())
    };
    match saved.await {
        Ok(()) => Outcome::Saved,
        Err(_) => Outcome::Failed(save_error.to_owned()),
    }
}

/// Runs one step of a login save on the blocking pool, where `disk` may be taken.
async fn save_blocking<T: Send + 'static>(
    state: &Management,
    step: impl FnOnce() -> anyhow::Result<T> + Send + 'static,
) -> anyhow::Result<T> {
    match state.work.clone().spawn_blocking(step).await {
        Ok(result) => result,
        Err(e) => {
            resume_panic(e);
            Err(anyhow::anyhow!("login save task cancelled"))
        }
    }
}

/// A blocking save task that did not finish: its panic goes on to the worker's panic
/// catch ([`panicked`]); a cancelled task returns, for the caller's save failure.
fn resume_panic(e: tokio::task::JoinError) {
    if let Ok(panic) = e.try_into_panic() {
        std::panic::resume_unwind(panic);
    }
}

async fn settle(state: &Arc<Management>, sid: &str, outcome: Outcome) {
    match outcome {
        Outcome::Saved => {
            // `disk` can be held by a save that waits on the store's async I/O, so it is
            // never taken on a runtime worker.
            let worker = state.clone();
            let published = state
                .work
                .clone()
                .spawn_blocking(move || {
                    let _guard = worker.disk.lock().unwrap_or_else(PoisonError::into_inner);
                    worker.publish((*worker.rt.config()).clone(), None);
                })
                .await;
            if let Err(e) = published {
                resume_panic(e);
                return fail_session(state, sid, "Failed to save authentication tokens");
            }
            state.oauth.complete(sid);
        }
        Outcome::Cancelled => {}
        Outcome::Failed(message) => return fail_session(state, sid, &message),
    }
    if let Some(settled) = &state.login_settled {
        let _ = settled.send(sid.to_owned());
    }
}

/// [`settle`] for a failed login, which publishes nothing.
fn fail_session(state: &Management, sid: &str, message: &str) {
    state.oauth.set_error(sid, message);
    if let Some(settled) = &state.login_settled {
        let _ = settled.send(sid.to_owned());
    }
}

/// The session error after a login worker panicked.
pub(super) const PANIC_MESSAGE: &str = "Sign-in failed because of an internal error. Start the sign-in again.";

/// Awaits a login worker, catching a panic in it, including one a blocking save task
/// passed on ([`resume_panic`]). Its error line names only the provider and the state (a
/// panic payload can quote tokens). Returns whether it panicked. Go recovers nothing in
/// these goroutines, so there a panic ends the process.
pub(super) async fn panicked(sid: &str, provider: &str, work: impl Future<Output = ()>) -> bool {
    let caught = AssertUnwindSafe(work).catch_unwind().await.is_err();
    if caught {
        tracing::error!("{provider} sign-in worker panicked; sign-in {sid} failed");
    }
    caught
}

/// Spawns a login's worker: a panic fails the session ([`PANIC_MESSAGE`]) instead of
/// leaving it waiting until [`SESSION_TTL`], and the login's callback forwarder
/// (`(port, id)`) stops however the worker ends.
fn spawn_login(
    state: &Arc<Management>,
    sid: &str,
    provider: &'static str,
    forwarder: Option<(u16, u64)>,
    work: impl Future<Output = ()> + Send + 'static,
) {
    let (worker, sid) = (state.clone(), sid.to_owned());
    state.work.spawn(async move {
        if panicked(&sid, provider, work).await {
            fail_session(&worker, &sid, PANIC_MESSAGE);
        }
        if let Some((port, id)) = forwarder {
            stop_forwarder(&worker, port, id);
        }
    });
}

fn started(url: String, sid: String) -> Response {
    reply(
        StatusCode::OK,
        [("status", "ok".into()), ("url", url.into()), ("state", sid.into())],
    )
}

/// Go `RequestAnthropicToken`.
async fn start_claude(state: Arc<Management>, webui: bool) -> Response {
    use ck_exec::claude_login::ManagedLogin;
    let login = match ManagedLogin::start() {
        Ok(l) => l,
        Err(_) => return fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to generate PKCE codes"),
    };
    state.oauth.register(&login.state, "anthropic");
    let forwarder = if webui {
        match start_forwarder(&state, ANTHROPIC_CALLBACK_PORT, "/anthropic/callback").await {
            Ok(id) => Some(id),
            Err(message) => return fail(StatusCode::INTERNAL_SERVER_ERROR, message),
        }
    } else {
        None
    };
    let (url, sid) = (login.url.clone(), login.state.clone());
    let worker = state.clone();
    let forwarder = forwarder.map(|id| (ANTHROPIC_CALLBACK_PORT, id));
    spawn_login(&state, &sid, "anthropic", forwarder, async move {
        if let Some(code) = wait_for_callback(&worker, &login.state, "anthropic", "Bad request").await {
            if !admit_grant_work(&worker).await {
                return fail_session(&worker, &login.state, "credential recovery required");
            }
            let exchanged = match &worker.login_base {
                Some(base) => {
                    let oauth = ck_exec::oauth::OAuth::with_endpoints(
                        login_client(&worker),
                        &format!("{base}/v1/oauth/token"),
                        &format!("{base}/api/oauth/profile"),
                        &format!("{base}/api/oauth/claude_cli/roles"),
                    );
                    login.exchange_with(&oauth, &code).await
                }
                None => login.exchange(&code, &global_proxy(&worker)).await,
            };
            let outcome = match exchanged {
                Ok(patch) => {
                    let write = |dir: PathBuf, patch, save: LoginSave| {
                        ManagedLogin::save(&dir, patch, Some(&save)).map_err(|e| exec_text(&e))
                    };
                    save_if_pending(
                        &worker,
                        &login.state,
                        "anthropic",
                        patch,
                        write,
                        "Failed to save authentication tokens",
                    )
                    .await
                }
                Err(_) => Outcome::Failed("Failed to exchange authorization code for tokens".into()),
            };
            settle(&worker, &login.state, outcome).await;
        }
    });
    started(url, sid)
}

/// Go `RequestCodexToken`.
async fn start_codex(state: Arc<Management>, webui: bool) -> Response {
    use ck_exec::codex_oauth::{CodexOAuth, authorize_url, redirect_uri, write_login};
    let Ok((verifier, challenge)) = ck_exec::oauth::pkce() else {
        return fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to generate PKCE codes");
    };
    let Some(sid) = random_state() else {
        return fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to generate state parameter");
    };
    let url = authorize_url(&sid, &challenge, CODEX_CALLBACK_PORT);
    state.oauth.register(&sid, "codex");
    let forwarder = if webui {
        match start_forwarder(&state, CODEX_CALLBACK_PORT, "/codex/callback").await {
            Ok(id) => Some(id),
            Err(message) => return fail(StatusCode::INTERNAL_SERVER_ERROR, message),
        }
    } else {
        None
    };
    let worker = state.clone();
    let flow = sid.clone();
    let forwarder = forwarder.map(|id| (CODEX_CALLBACK_PORT, id));
    spawn_login(&state, &sid, "codex", forwarder, async move {
        if let Some(code) = wait_for_callback(&worker, &flow, "codex", "Bad Request").await {
            if !admit_grant_work(&worker).await {
                return fail_session(&worker, &flow, "credential recovery required");
            }
            let client = login_client(&worker);
            let oauth = match &worker.login_base {
                Some(base) => CodexOAuth::with_endpoints(
                    client,
                    &format!("{base}/oauth/token"),
                    &format!("{base}/api/accounts/deviceauth/usercode"),
                    &format!("{base}/api/accounts/deviceauth/token"),
                ),
                None => CodexOAuth::new(client),
            };
            let outcome = match oauth
                .exchange(&code, &redirect_uri(CODEX_CALLBACK_PORT), &verifier)
                .await
            {
                Ok(tokens) => {
                    let write = |dir: PathBuf, tokens, save: LoginSave| {
                        write_login(&dir, &tokens, Some(&save)).map_err(|e| exec_text(&e))
                    };
                    save_if_pending(
                        &worker,
                        &flow,
                        "codex",
                        tokens,
                        write,
                        "Failed to save authentication tokens",
                    )
                    .await
                }
                Err(e) => Outcome::Failed(with_cause(
                    "Failed to exchange authorization code for tokens",
                    &exec_text(&e),
                )),
            };
            settle(&worker, &flow, outcome).await;
        }
    });
    started(url, sid)
}

/// Go `RequestAntigravityToken`: reuse the CLI exchange and storage, but unlike
/// `CompleteAntigravityOAuth`, a project discovery failure does not fail the login.
async fn start_antigravity(state: Arc<Management>, webui: bool) -> Response {
    use ck_exec::antigravity_auth::{DEFAULT_CALLBACK_PORT, OAuth, auth_url, build_auth, redirect_uri, write_login};
    let Some(sid) = random_state() else {
        return fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to generate state parameter");
    };
    let redirect = redirect_uri(DEFAULT_CALLBACK_PORT);
    let url = auth_url(&sid, &redirect);
    state.oauth.register(&sid, "antigravity");
    let forwarder = if webui {
        match start_forwarder(&state, DEFAULT_CALLBACK_PORT, "/antigravity/callback").await {
            Ok(id) => Some(id),
            Err(message) => return fail(StatusCode::INTERNAL_SERVER_ERROR, message),
        }
    } else {
        None
    };
    let worker = state.clone();
    let flow = sid.clone();
    let forwarder = forwarder.map(|id| (DEFAULT_CALLBACK_PORT, id));
    spawn_login(&state, &sid, "antigravity", forwarder, async move {
        if let Some(cb) = next_callback(&worker, &flow, "antigravity").await {
            let outcome = async {
                if !cb.error.is_empty() {
                    return Outcome::Failed("Authentication failed".into());
                }
                if !cb.state.is_empty() && cb.state != flow {
                    return Outcome::Failed("Authentication failed: state mismatch".into());
                }
                if cb.code.is_empty() {
                    return Outcome::Failed("Authentication failed: code not found".into());
                }
                if !admit_grant_work(&worker).await {
                    return Outcome::Failed("credential recovery required".into());
                }
                let client = login_client(&worker);
                let oauth = match &worker.login_base {
                    Some(base) => OAuth::with_endpoints(
                        client,
                        &format!("{base}/token"),
                        &format!("{base}/oauth2/v2/userinfo?alt=json"),
                        base,
                        base,
                    ),
                    None => OAuth::new(client),
                };
                let tokens = match oauth.exchange_code(&cb.code, &redirect).await {
                    Ok(tokens) if !tokens.access_token.trim().is_empty() => tokens,
                    _ => return Outcome::Failed("Failed to exchange token".into()),
                };
                let access = tokens.access_token.trim();
                let email = match oauth.user_info(access).await {
                    Ok(email) if !email.trim().is_empty() => email,
                    _ => return Outcome::Failed("Failed to fetch user info".into()),
                };
                let project = oauth.project_id(access).await.unwrap_or_else(|_| {
                    // Upstream bodies can contain credentials; do not log the cause.
                    tracing::warn!("antigravity: failed to fetch project ID during management login");
                    String::new()
                });
                let record = build_auth(&tokens, &email, &project, chrono::Utc::now());
                let write =
                    |dir: PathBuf, (name, metadata): (String, serde_json::Map<String, Value>), save: LoginSave| {
                        let path = dir.join(&name);
                        save_login(Some(&save), &[path], || write_login(&dir, &name, &metadata))
                    };
                save_if_pending(
                    &worker,
                    &flow,
                    "antigravity",
                    record,
                    write,
                    "Failed to save token to file",
                )
                .await
            }
            .await;
            settle(&worker, &flow, outcome).await;
        }
    });
    started(url, sid)
}

/// Go's callback wait: every 500 ms for up to five minutes, until the callback
/// arrives, the session stops being pending, or the deadline passes (recorded as the
/// session's error).
async fn next_callback(state: &Management, sid: &str, provider: &str) -> Option<Callback> {
    let deadline = Instant::now() + CALLBACK_WAIT;
    loop {
        if !state.oauth.is_pending(sid, provider) {
            return None;
        }
        if Instant::now() > deadline {
            let message = if provider == "antigravity" {
                "OAuth flow timed out"
            } else {
                "Timeout waiting for OAuth callback"
            };
            state.oauth.set_error(sid, message);
            return None;
        }
        if let Some(cb) = state.oauth.take_callback(sid) {
            return Some(cb);
        }
        tokio::select! {
            biased;
            () = state.stopping.cancelled() => {
                state.oauth.set_error(sid, "Sign-in cancelled because the service is shutting down");
                return None;
            },
            () = tokio::time::sleep(Duration::from_millis(500)) => {},
        }
    }
}

/// [`next_callback`] with the Claude and Codex checks: an error first, then the state.
/// Returns the code.
async fn wait_for_callback(state: &Management, sid: &str, provider: &str, bad_request: &str) -> Option<String> {
    let cb = next_callback(state, sid, provider).await?;
    if !cb.error.is_empty() {
        state.oauth.set_error(sid, bad_request);
        return None;
    }
    if cb.state != sid {
        state.oauth.set_error(sid, "State code error");
        return None;
    }
    Some(cb.code)
}

/// Go `RequestDevinToken`: PKCE login whose callback reaches the main listener's
/// `/callback` (Devin only accepts `http://127.0.0.1:<port>/callback`).
async fn start_devin(state: Arc<Management>) -> Response {
    use ck_exec::devin_auth::{DevinAuth, generate_pkce, save_record};
    let port = state.rt.config().port;
    if port == 0 {
        return fail(StatusCode::INTERNAL_SERVER_ERROR, "callback server unavailable");
    }
    let redirect = format!("http://127.0.0.1:{port}/callback");
    let Ok(pkce) = generate_pkce() else {
        return fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to generate PKCE codes");
    };
    let Some(sid) = random_state() else {
        return fail(StatusCode::INTERNAL_SERVER_ERROR, "failed to generate state parameter");
    };
    let mut auth = DevinAuth::new(login_client(&state));
    if let Some(base) = &state.login_base {
        auth = auth.with_app_base(base).with_api_base(base).with_server_base(base);
    }
    let url = auth.build_authorization_url(&redirect, &pkce.challenge, &sid);
    state.oauth.register(&sid, "devin");
    let worker = state.clone();
    let flow = sid.clone();
    spawn_login(&state, &sid, "devin", None, async move {
        // Go runs the whole flow under one five-minute context.
        let deadline = tokio::time::Instant::now() + CALLBACK_WAIT;
        let Some(cb) = next_callback(&worker, &flow, "devin").await else {
            return;
        };
        let fail = |message: &str| {
            if worker.oauth.is_pending(&flow, "devin") {
                fail_session(&worker, &flow, message);
            }
        };
        if cb.state != flow {
            return fail("State code error");
        }
        if !cb.error.is_empty() {
            return fail("Devin authorization denied");
        }
        if cb.code.trim().is_empty() {
            return fail("Missing authorization code");
        }
        if !admit_grant_work(&worker).await {
            return fail("credential recovery required");
        }
        // Upstream errors can carry tokens or codes: only Go's fixed messages surface.
        let exchanged = tokio::select! {
            r = auth.exchange_code(&cb.code, &pkce.verifier) => r.ok(),
            () = tokio::time::sleep_until(deadline) => None,
            () = cancelled(&worker, &flow, "devin") => return,
        };
        let token = match exchanged {
            Some(token) if !token.trim().is_empty() => token,
            _ => return fail("Failed to exchange authorization code for tokens"),
        };
        if !worker.oauth.is_pending(&flow, "devin") {
            return;
        }
        let created = tokio::select! {
            r = auth.create_auth_record(&token) => r.ok(),
            () = tokio::time::sleep_until(deadline) => None,
            () = cancelled(&worker, &flow, "devin") => return,
        };
        let Some(record) = created else {
            return fail("Failed to create Devin authentication record");
        };
        let write = |dir: PathBuf, record: ck_exec::devin_auth::AuthRecord, save: LoginSave| {
            let path = dir.join(&record.file_name);
            save_login(Some(&save), &[path], || save_record(&dir, &record))
        };
        let outcome = save_if_pending(
            &worker,
            &flow,
            "devin",
            record,
            write,
            "Failed to save authentication tokens",
        )
        .await;
        settle(&worker, &flow, outcome).await;
    });
    started(url, sid)
}

/// Go `watchOAuthSessionCancel`: resolves once the session stops being pending.
async fn cancelled(state: &Management, sid: &str, provider: &str) {
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        if !state.oauth.is_pending(sid, provider) {
            return;
        }
    }
}

/// The JSON answer for a started device login.
fn device_started(url: String, sid: String, user_code: &str, expires_in: Option<i64>) -> Response {
    let mut out = vec![
        ("status", Value::from("ok")),
        ("url", url.into()),
        ("state", sid.into()),
        ("flow", "device".into()),
    ];
    if !user_code.trim().is_empty() {
        out.push(("user_code", user_code.trim().into()));
    }
    if let Some(e) = expires_in {
        out.push(("expires_in", e.into()));
    }
    reply(StatusCode::OK, out)
}

/// Go `requestKimiTokenWithDomain`.
async fn start_kimi(state: Arc<Management>, domain: String) -> Response {
    use ck_exec::kimi_auth::{DeviceFlow, is_ai_domain, login_record, write_login};
    let (provider, prefix) = if is_ai_domain(&domain) {
        ("kimi-ai", "kmi-ai")
    } else {
        ("kimi", "kmi")
    };
    let sid = format!("{prefix}-{}", unix_nanos());
    let mut flow = DeviceFlow::new(login_client(&state), &domain, "");
    if let Some(base) = &state.login_base {
        flow = flow.with_oauth_host(base);
    }
    let Ok(code) = flow.request_device_code().await else {
        return fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to generate authorization url",
        );
    };
    let url = if code.verification_uri_complete.is_empty() {
        code.verification_uri.clone()
    } else {
        code.verification_uri_complete.clone()
    };
    state.oauth.register(&sid, provider);
    let response = device_started(
        url,
        sid.clone(),
        &code.user_code,
        (code.expires_in > 0).then_some(code.expires_in),
    );
    let worker = state.clone();
    let flow_sid = sid.clone();
    spawn_login(&state, &flow_sid, provider, None, async move {
        let polled = tokio::select! {
            t = flow.poll(&code) => t,
            () = cancelled(&worker, &sid, provider) => return,
        };
        let outcome = match polled {
            Ok(tokens) => {
                let mut record = login_record(
                    provider,
                    &tokens,
                    flow.device_id(),
                    chrono::Utc::now().timestamp_millis(),
                );
                // Go records the requested domain as given.
                record.metadata.insert("domain".into(), domain.clone().into());
                let write = |dir: PathBuf, record: ck_exec::kimi_auth::LoginRecord, save: LoginSave| {
                    let path = dir.join(&record.file_name);
                    save_login(Some(&save), &[path], || {
                        write_login(&dir, &record).map_err(|e| exec_text(&e))
                    })
                };
                save_if_pending(
                    &worker,
                    &sid,
                    provider,
                    record,
                    write,
                    "Failed to save authentication tokens",
                )
                .await
            }
            Err(_) if !worker.oauth.is_pending(&sid, provider) => Outcome::Cancelled,
            Err(e) => Outcome::Failed(with_cause("Authentication failed", &exec_text(&e))),
        };
        settle(&worker, &sid, outcome).await;
    });
    response
}

/// Go `RequestMetaToken`.
async fn start_meta(state: Arc<Management>) -> Response {
    use ck_exec::meta_auth::{MetaAuth, create_token_storage, save_login};
    const MAX_POLL_SECONDS: i64 = 15 * 60;
    let sid = format!("meta-{}", unix_nanos());
    let mut auth = MetaAuth::new(login_client(&state));
    if let Some(base) = &state.login_base {
        auth = auth
            .with_auth_host(base)
            .with_mint_url(&format!("{base}/muse-code/key"));
    }
    let Ok(code) = auth.start_device_flow().await else {
        return fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to start device authorization flow",
        );
    };
    let url = match code.verification_uri_complete.trim() {
        "" => code.verification_uri.trim().to_owned(),
        u => u.to_owned(),
    };
    state.oauth.register(&sid, "meta");
    let expires = if code.expires_in > 0 {
        code.expires_in
    } else {
        MAX_POLL_SECONDS
    };
    let response = device_started(url, sid.clone(), &code.user_code, Some(expires));
    let worker = state.clone();
    let flow_sid = sid.clone();
    spawn_login(&state, &flow_sid, "meta", None, async move {
        let waited = tokio::select! {
            b = auth.wait_for_authorization(&code) => b,
            () = cancelled(&worker, &sid, "meta") => return,
        };
        let now = chrono::Utc::now();
        let outcome = match waited {
            Err(_) if !worker.oauth.is_pending(&sid, "meta") => Outcome::Cancelled,
            Err(e) => Outcome::Failed(with_cause("Authentication failed", &e)),
            Ok(_) if !worker.oauth.is_pending(&sid, "meta") => Outcome::Cancelled,
            Ok(bundle) if create_token_storage(&bundle, now).access_token.trim().is_empty() => {
                Outcome::Failed("Failed to exchange token".into())
            }
            Ok(bundle) => {
                let write = move |dir: PathBuf, bundle, save: LoginSave| {
                    save_login(&dir, &bundle, now, Some(&save)).map(|o| o.path)
                };
                save_if_pending(&worker, &sid, "meta", bundle, write, "Failed to save token to file").await
            }
        };
        settle(&worker, &sid, outcome).await;
    });
    response
}

fn save_xai_login(dir: &std::path::Path, record: ck_exec::xai_auth::LoginRecord) -> Result<PathBuf, String> {
    let path = dir.join(&record.file_name);
    let existing = match std::fs::read(&path) {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.to_string()),
    };
    let document = ck_exec::xai_auth::saved_document(record.metadata, existing.as_deref())?;
    let text = ck_wire::json::GoValue::Object(document).encode_indented();
    std::fs::create_dir_all(dir)
        .and_then(|()| super::auth_files::write_file(&path, &text))
        .map_err(|e| e.to_string())?;
    Ok(path)
}

/// Go `RequestXAIToken`: device flow; the poller saves the credential file.
async fn start_xai(state: Arc<Management>) -> Response {
    use ck_exec::xai_auth::{LoginRecord, MAX_POLL_DURATION, XaiAuth, login_record};
    let sid = format!("xai-{}", unix_nanos());
    let mut auth = XaiAuth::new(login_client(&state));
    if let Some(base) = &state.login_base {
        auth = auth
            .with_issuer_origin(base)
            .with_min_poll_interval(std::time::Duration::from_millis(10));
    }
    let Ok(code) = auth.start_device_flow().await else {
        return fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to start device authorization flow",
        );
    };
    let url = match code.verification_uri_complete.trim() {
        "" => code.verification_uri.trim().to_owned(),
        u => u.to_owned(),
    };
    state.oauth.register(&sid, "xai");
    let expires = if code.expires_in > 0 {
        code.expires_in
    } else {
        MAX_POLL_DURATION.as_secs() as i64
    };
    let response = device_started(url, sid.clone(), &code.user_code, Some(expires));
    // Go `saveTokenRecord` through the file token store: merged with an existing file.
    let worker = state.clone();
    let flow_sid = sid.clone();
    spawn_login(&state, &flow_sid, "xai", None, async move {
        let waited = tokio::select! {
            t = auth.poll(&code) => t,
            () = cancelled(&worker, &sid, "xai") => return,
        };
        let now = chrono::Utc::now();
        let outcome = match waited {
            _ if !worker.oauth.is_pending(&sid, "xai") => Outcome::Cancelled,
            Err(e) => Outcome::Failed(with_cause("Authentication failed", &exec_text(&e))),
            Ok(tokens) if tokens.access_token.trim().is_empty() => Outcome::Failed("Failed to exchange token".into()),
            Ok(tokens) => {
                // Go `WaitForAuthorization`: RFC 3339 seconds in UTC.
                let last_refresh = now.format("%Y-%m-%dT%H:%M:%SZ").to_string();
                let record = login_record(&tokens, &last_refresh, &code.token_endpoint, now.timestamp_millis());
                let write = |dir: PathBuf, record: LoginRecord, save: LoginSave| {
                    let path = dir.join(&record.file_name);
                    save_login(Some(&save), &[path], || save_xai_login(&dir, record))
                };
                save_if_pending(&worker, &sid, "xai", record, write, "Failed to save token to file").await
            }
        };
        settle(&worker, &sid, outcome).await;
    });
    response
}

/// A running callback forwarder (Go `callbackForwarder`).
pub(crate) struct Forwarder {
    id: u64,
    task: tokio::task::JoinHandle<()>,
}

/// Go `startCallbackForwarder`: `0.0.0.0:<port>` redirects every request to the
/// main listener's provider callback. A forwarder already on that port is stopped
/// (and its listener released) first. Errors carry Go's message for the 500 answer.
async fn start_forwarder(state: &Management, port: u16, path: &str) -> Result<u64, &'static str> {
    let cfg = state.rt.config();
    if cfg.port == 0 {
        return Err("callback server unavailable");
    }
    let tls = cfg
        .document
        .get("server")
        .and_then(|s| s.get("tls"))
        .and_then(|t| t.get("enable"))
        .and_then(serde_yaml_ng::Value::as_bool)
        .unwrap_or(false);
    let target = format!("{}://127.0.0.1:{}{path}", if tls { "https" } else { "http" }, cfg.port);
    let previous = state
        .forwarders
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(&port);
    if let Some(previous) = previous {
        previous.task.abort();
        let _ = previous.task.await;
    }
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .map_err(|_| "failed to start callback server")?;
    let work = state.work.clone();
    let stopping = state.stopping.clone();
    let task = state.work.spawn(async move {
        loop {
            let accepted = tokio::select! {
                biased;
                () = stopping.cancelled() => return,
                accepted = listener.accept() => accepted,
            };
            let Ok((stream, _)) = accepted else { return };
            let (stopping, target) = (stopping.clone(), target.clone());
            work.spawn(async move {
                tokio::select! {
                    biased;
                    () = stopping.cancelled() => {},
                    () = forward(stream, target) => {},
                }
            });
        }
    });
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    state
        .forwarders
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(port, Forwarder { id, task });
    Ok(id)
}

const FORWARD_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_REQUEST_HEAD: usize = 16 << 10;

/// One forwarder connection with Go's server deadlines (`ReadHeaderTimeout` and
/// `WriteTimeout`, 5 s each): read the request head, answer with the redirect, close.
async fn forward(mut stream: tokio::net::TcpStream, target: String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut head = Vec::new();
    let read = tokio::time::timeout(FORWARD_TIMEOUT, async {
        let mut buf = [0u8; 2048];
        while !head.windows(4).any(|w| w == b"\r\n\r\n") && head.len() < MAX_REQUEST_HEAD {
            let n = stream.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            head.extend_from_slice(&buf[..n]);
        }
        Ok::<_, std::io::Error>(())
    })
    .await;
    if !matches!(read, Ok(Ok(()))) || !head.windows(4).any(|w| w == b"\r\n\r\n") {
        return;
    }
    let line = head.split(|b| *b == b'\n').next().unwrap_or_default();
    let line = String::from_utf8_lossy(line);
    let mut parts = line.trim_end_matches('\r').split(' ');
    let response = match (parts.next(), parts.next(), parts.next()) {
        (Some(method), Some(uri), Some(version)) if version.starts_with("HTTP/") => {
            redirect_bytes(method, &target, uri.split_once('?').map(|(_, q)| q))
        }
        _ => b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
    };
    let _ = tokio::time::timeout(FORWARD_TIMEOUT, async {
        stream.write_all(&response).await?;
        stream.shutdown().await
    })
    .await;
}

/// The forwarder's answer: Go `http.Redirect` (302; an HTML link body for GET and
/// HEAD) plus `Cache-Control: no-store`.
fn redirect_bytes(method: &str, target: &str, query: Option<&str>) -> Vec<u8> {
    let location = match query.filter(|q| !q.is_empty()) {
        Some(q) if target.contains('?') => format!("{target}&{q}"),
        Some(q) => format!("{target}?{q}"),
        None => target.to_owned(),
    };
    let mut out = format!("HTTP/1.1 302 Found\r\nCache-Control: no-store\r\nLocation: {location}\r\n");
    let body = if matches!(method, "GET" | "HEAD") {
        let escaped = location
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&#34;")
            .replace('\'', "&#39;");
        format!("<a href=\"{escaped}\">Found</a>.\n\n")
    } else {
        String::new()
    };
    if !body.is_empty() {
        out.push_str("Content-Type: text/html; charset=utf-8\r\n");
    }
    out.push_str(&format!("Content-Length: {}\r\nConnection: close\r\n\r\n", body.len()));
    if method != "HEAD" {
        out.push_str(&body);
    }
    out.into_bytes()
}

/// Go `stopCallbackForwarderInstance`: only the forwarder this login started.
fn stop_forwarder(state: &Management, port: u16, id: u64) {
    let mut forwarders = state.forwarders.lock().unwrap_or_else(PoisonError::into_inner);
    if forwarders.get(&port).is_some_and(|f| f.id == id)
        && let Some(f) = forwarders.remove(&port)
    {
        f.task.abort();
    }
}

/// Go `GetAuthStatus`.
pub(crate) async fn status(State(state): State<Arc<Management>>, RawQuery(raw): RawQuery) -> Response {
    let q = Query::parse(raw);
    let sid = q.first("state").trim().to_owned();
    if sid.is_empty() {
        return reply(StatusCode::OK, [("status", "ok".into())]);
    }
    if !valid_state(&sid) {
        return status_error(StatusCode::BAD_REQUEST, "invalid state");
    }
    match state.oauth.details(&sid) {
        None => status_error(StatusCode::OK, "unknown or expired state"),
        Some((_, _, true)) => reply(StatusCode::OK, [("status", "ok".into())]),
        Some((_, status, _)) if !status.is_empty() => status_error(StatusCode::OK, &status),
        Some(_) => reply(StatusCode::OK, [("status", "wait".into())]),
    }
}

/// Go `CancelAuthSession`.
pub(crate) async fn cancel(State(state): State<Arc<Management>>, RawQuery(raw): RawQuery) -> Response {
    let q = Query::parse(raw);
    let sid = q.first("state").trim().to_owned();
    if sid.is_empty() {
        return status_error(StatusCode::BAD_REQUEST, "missing state");
    }
    if !valid_state(&sid) {
        return status_error(StatusCode::BAD_REQUEST, "invalid state");
    }
    let cancelled = state.oauth.cancel(&sid);
    reply(
        StatusCode::OK,
        [("status", "ok".into()), ("cancelled", cancelled.into())],
    )
}

#[derive(Default)]
pub(super) struct CallbackRequest {
    pub(super) provider: String,
    pub(super) redirect_url: String,
    pub(super) code: String,
    pub(super) state: String,
    pub(super) error: String,
    /// v8 only: the client ID a Sign in with ChatGPT authorization issued.
    pub(super) client_id: String,
}

/// Go `GetOAuthCallback`, and under v8 `GetOAuthCallbackV8`.
pub(crate) async fn callback_get(
    State(state): State<Arc<Management>>,
    uri: axum::http::Uri,
    RawQuery(raw): RawQuery,
) -> Response {
    let q = Query::parse(raw);
    let error = [q.first("error"), q.first("error_description")]
        .into_iter()
        .map(str::trim)
        .find(|v| !v.is_empty())
        .unwrap_or_default()
        .to_owned();
    let req = CallbackRequest {
        provider: q.first("provider").trim().to_owned(),
        code: q.first("code").trim().to_owned(),
        state: q.first("state").trim().to_owned(),
        error,
        client_id: q.first("client_id").trim().to_owned(),
        ..CallbackRequest::default()
    };
    if uri.path().starts_with("/v8/")
        && let Some(response) = super::siwc::callback(&state, &req, Some(&q))
    {
        return response;
    }
    handle_callback(&state, req)
}

/// Go `PostOAuthCallback`: gin's `ShouldBindJSON` into `oauthCallbackRequest`
/// (first JSON value, exact-then-folded names, type mismatches rejected).
/// Under v8, `PostOAuthCallbackV8`, whose request also has `client_id`.
pub(crate) async fn callback_post(
    State(state): State<Arc<Management>>,
    uri: axum::http::Uri,
    body: Result<crate::http::body_limit::FirstJson, axum::extract::rejection::BytesRejection>,
) -> Response {
    let body = match body {
        Ok(crate::http::body_limit::FirstJson(body)) => body,
        Err(rejection) => return status_error(super::read_status(&rejection), "invalid body"),
    };
    let v8 = uri.path().starts_with("/v8/");
    const FIELDS: [&str; 6] = ["provider", "redirect_url", "code", "state", "error", "client_id"];
    let fields = if v8 { &FIELDS[..] } else { &FIELDS[..5] };
    let invalid = || status_error(StatusCode::BAD_REQUEST, "invalid body");
    let Some(members) = super::api_call::bind_struct(&body, fields) else {
        return invalid();
    };
    let mut req = CallbackRequest::default();
    for (i, v) in members {
        let slot = match i {
            0 => &mut req.provider,
            1 => &mut req.redirect_url,
            2 => &mut req.code,
            3 => &mut req.state,
            4 => &mut req.error,
            _ => &mut req.client_id,
        };
        match v {
            Value::String(s) => *slot = s,
            Value::Null => {}
            _ => return invalid(),
        }
    }
    if v8 && let Some(response) = super::siwc::callback(&state, &req, None) {
        return response;
    }
    handle_callback(&state, req)
}

/// Go `handleOAuthCallback`.
fn handle_callback(state: &Management, req: CallbackRequest) -> Response {
    let (mut sid, mut code, mut error) = (
        req.state.trim().to_owned(),
        req.code.trim().to_owned(),
        req.error.trim().to_owned(),
    );
    let redirect = req.redirect_url.trim();
    if !redirect.is_empty() {
        let Some(query) = redirect_query(redirect) else {
            return status_error(StatusCode::BAD_REQUEST, "invalid redirect_url");
        };
        let q = Query::parse(Some(query));
        let get = |k: &str| q.first(k).trim().to_owned();
        if sid.is_empty() {
            sid = get("state");
        }
        if code.is_empty() {
            code = get("code");
        }
        if error.is_empty() {
            error = Some(get("error"))
                .filter(|e| !e.is_empty())
                .unwrap_or_else(|| get("error_description"));
        }
    }
    if sid.is_empty() {
        return status_error(StatusCode::BAD_REQUEST, "state is required");
    }
    if !valid_state(&sid) {
        return status_error(StatusCode::BAD_REQUEST, "invalid state");
    }
    if code.is_empty() && error.is_empty() {
        return status_error(StatusCode::BAD_REQUEST, "code or error is required");
    }
    let Some((session_provider, status, completed)) = state.oauth.details(&sid) else {
        return status_error(StatusCode::NOT_FOUND, "unknown or expired state");
    };
    if completed {
        return status_error(StatusCode::CONFLICT, "oauth flow is already completed");
    }
    let provider = match req.provider.trim() {
        "" => session_provider.clone(),
        p => p.to_owned(),
    };
    let Some(canonical) = normalize_callback_provider(&provider) else {
        return status_error(StatusCode::BAD_REQUEST, "unsupported provider");
    };
    if !status.is_empty() {
        return status_error(StatusCode::CONFLICT, &status);
    }
    if !session_provider.go_eq_fold(&canonical) {
        return status_error(StatusCode::BAD_REQUEST, "provider does not match state");
    }
    if matches!(
        state.oauth.deliver(&canonical, &sid, &code, &error),
        Delivery::Delivered
    ) {
        return reply(StatusCode::OK, [("status", "ok".into())]);
    }
    match state.oauth.get(&sid) {
        Some((_, status, false)) if !status.is_empty() => status_error(StatusCode::CONFLICT, &status),
        _ => status_error(StatusCode::CONFLICT, "oauth flow is not pending"),
    }
}

/// `url.Parse(raw).RawQuery`, or `None` where Go's parser fails.
fn redirect_query(raw: &str) -> Option<String> {
    ck_core::config::go_url::parse(raw)?;
    let without_fragment = raw.split('#').next().unwrap_or_default();
    Some(
        without_fragment
            .split_once('?')
            .map(|(_, q)| q.to_owned())
            .unwrap_or_default(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sqlite_state(
        label: &str,
    ) -> (
        PathBuf,
        Arc<Management>,
        Arc<crate::store::credential_store::CredentialDb>,
    ) {
        let root = std::env::temp_dir().join(format!("oauth-{label}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let config = Arc::new(crate::testing::config_store(&root.join("config.db")).unwrap());
        let db = crate::store::credential_store::CredentialDb::initialize(config.clone()).unwrap();
        let mut cfg = config.load().unwrap().config;
        cfg.auth_dir = db.auth_dir();
        let rt = Arc::new(crate::testing::runtime_with_db(
            cfg,
            Vec::new(),
            ck_exec::Executors {
                claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
                codex: Default::default(),
                devices: Default::default(),
                openai: Default::default(),
                google: Default::default(),
            },
            db.clone(),
        ));
        (root, Management::new(rt, config), db)
    }

    #[tokio::test]
    async fn shutdown_stops_online_admission_and_joins_an_already_completed_login() {
        use std::future::Future as _;
        use std::task::Poll;
        use tower::ServiceExt as _;
        let (root, state, db) = sqlite_state("shutdown-owned-login");
        let workspace = state.config.working_directory().unwrap().to_owned();
        let raw = br#"{"type":"vertex","opaque":"owned-completed-grant"}"#.to_vec();
        let expected = raw.clone();
        let (entered, started) = tokio::sync::oneshot::channel();
        let release = Arc::new(tokio::sync::Notify::new());
        state.oauth.register("shutdown-owned", "vertex");
        let (worker, held) = (state.clone(), release.clone());
        spawn_login(&state, "shutdown-owned", "vertex", None, async move {
            entered.send(()).unwrap();
            held.notified().await;
            let outcome = save_if_pending(
                &worker,
                "shutdown-owned",
                "vertex",
                raw,
                |dir, raw, save| {
                    let path = dir.join("shutdown-owned.json");
                    save_login(Some(&save), std::slice::from_ref(&path), || {
                        std::fs::write(&path, raw).map_err(|error| error.to_string())
                    })?;
                    Ok(path)
                },
                "save failed",
            )
            .await;
            settle(&worker, "shutdown-owned", outcome).await;
        });
        started.await.unwrap();
        state.begin_shutdown();
        let revision = state.config.load().unwrap().rev;
        let response = super::super::router(state.clone())
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/admin/v1/apply")
                    .header("content-type", "application/json")
                    .header("authorization", "Bearer test-only")
                    .body(axum::body::Body::from(r#"{"ops":[]}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(state.config.load().unwrap().rev, revision);
        let mut shutdown = Box::pin(state.shutdown());
        let early = std::future::poll_fn(|cx| Poll::Ready(shutdown.as_mut().poll(cx).is_ready())).await;
        release.notify_one();
        if !early {
            shutdown.as_mut().await;
        }
        drop(shutdown);
        assert!(!early, "shutdown returned before the owned login result was saved");
        assert_eq!(
            db.read(&db.auth_dir().join("shutdown-owned.json")).unwrap().unwrap(),
            expected
        );
        assert!(state.oauth.get("shutdown-owned").unwrap().2);
        state.rt.shutdown_codex_refreshes().await;
        drop(state);
        drop(db);
        assert!(!workspace.exists());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn shutdown_reports_and_retains_an_unpublished_checkpoint_for_resume() {
        let (root, state, db) = sqlite_state("shutdown-unresolved");
        let workspace = state.config.working_directory().unwrap().to_owned();
        let published = root.join("config.db");
        std::fs::rename(&published, root.join("old.db")).unwrap();
        std::fs::create_dir(&published).unwrap();
        let raw = br#"{"type":"vertex","opaque":"completed-before-publication-failure"}"#;
        let path = db.auth_dir().join("pending.json");
        assert!(db.save(&path, raw).is_err());
        state.shutdown().await;
        assert!(
            state
                .rt
                .finish_shutdown()
                .await
                .unwrap_err()
                .to_string()
                .contains("incomplete")
        );
        drop(state);
        drop(db);
        assert!(workspace.is_dir(), "failed shutdown erased the only newer checkpoint");
        std::fs::remove_dir(&published).unwrap();
        std::fs::rename(root.join("old.db"), &published).unwrap();
        let resumed = Arc::new(crate::testing::config_store(&published).unwrap());
        let db = crate::store::credential_store::CredentialDb::initialize(resumed.clone()).unwrap();
        assert_eq!(db.read(&path).unwrap().unwrap(), raw);
        drop(db);
        drop(resumed);
        assert!(!workspace.exists());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn shutdown_cancels_callback_wait_without_starting_an_exchange() {
        let (root, state, db) = sqlite_state("shutdown-callback-wait");
        let response = start_claude(state.clone(), false).await;
        assert_eq!(response.status(), StatusCode::OK);
        state.shutdown().await;
        let sessions = state.oauth.lock();
        assert_eq!(sessions.len(), 1);
        assert!(
            sessions
                .values()
                .all(|session| session.status.contains("shutting down") && !session.completed)
        );
        drop(sessions);
        drop(state);
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn pending_provider_writer_restores_bytes_on_error_and_respects_cancellation() {
        let (root, state, db) = sqlite_state("provider-rollback");
        let fixture: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../ck-exec/tests/fixtures/vertex_go.json"
        )))
        .unwrap();
        let key = fixture["import"][0]["input"].as_str().unwrap();
        let path = db.auth_dir().join("vertex-proj-1.json");
        let original = br#"{ "type":"vertex", "prefix":"team", "disabled":true }"#;
        db.save(&path, original).unwrap();
        std::fs::write(&path, br#"{"type":"vertex","prefix":"cache-edit","disabled":false}"#).unwrap();
        let upload = ck_exec::vertex_auth::import_upload(&db.auth_dir(), key.as_bytes(), "").unwrap();
        state.oauth.register("active", "vertex");
        let outcome = save_if_pending(
            &state,
            "active",
            "vertex",
            upload,
            |dir, mut upload, save| {
                upload.path = dir.join(upload.path.file_name().unwrap());
                let path = upload.path.clone();
                save_login(Some(&save), std::slice::from_ref(&path), || {
                    upload.write().map_err(|_| "Vertex writer failed".to_owned())?;
                    let written: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                    assert_eq!(written["prefix"], "team");
                    assert_eq!(written["disabled"], true);
                    assert!(written.get("service_account").is_some(), "real provider writer ran");
                    Err("injected failure after provider write".to_owned())
                })
            },
            "safe save failure",
        )
        .await;
        assert!(matches!(outcome, Outcome::Failed(message) if message == "safe save failure"));
        assert_eq!(db.read(&path).unwrap().unwrap(), original);
        assert_eq!(std::fs::read(&path).unwrap(), original);

        state.oauth.register("cancelled", "vertex");
        assert!(state.oauth.cancel("cancelled"));
        let upload = ck_exec::vertex_auth::import_upload(&db.auth_dir(), key.as_bytes(), "").unwrap();
        let outcome = save_if_pending(
            &state,
            "cancelled",
            "vertex",
            upload,
            |_, upload, save| {
                let path = upload.path;
                save_login(Some(&save), std::slice::from_ref(&path), || {
                    panic!("cancelled writer ran")
                })
            },
            "safe save failure",
        )
        .await;
        assert!(matches!(outcome, Outcome::Cancelled));
        assert_eq!(db.read(&path).unwrap().unwrap(), original);
        assert_eq!(std::fs::read(&path).unwrap(), original);
        drop(state);
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn terminal_builtin_login_missing_claude_account_does_not_block_writers() {
        let (root, state, db) = sqlite_state("terminal-claude-account");
        let path = db.auth_dir().join("claude-existing.json");
        let original = br#"{"type":"claude","email":"existing@example.test","access_token":"R0","opaque":"keep"}"#;
        db.save(&path, original).unwrap();
        state.publish((*state.rt.config()).clone(), None);
        state.oauth.register("missing-account", "anthropic");
        let mut patch = ck_core::credential::MetadataPatch::default();
        patch.set.insert("access_token".into(), "discarded-login".into());
        patch.set.insert("email".into(), "".into());
        let outcome = save_if_pending(
            &state,
            "missing-account",
            "anthropic",
            patch,
            |dir, patch, save| {
                ck_exec::claude_login::ManagedLogin::save(&dir, patch, Some(&save)).map_err(|error| exec_text(&error))
            },
            "Failed to save authentication tokens",
        )
        .await;
        assert!(
            matches!(outcome, Outcome::Failed(_)),
            "missing identity must not report success"
        );
        settle(&state, "missing-account", outcome).await;
        let session = state.oauth.get("missing-account").unwrap();
        assert!(!session.2);
        assert!(!session.1.is_empty(), "the failed login remains visible");
        assert_eq!(db.read(&path).unwrap().unwrap(), original);
        state
            .config_store()
            .import_yaml("server: {port: 8319}\n", "test", "unrelated")
            .expect("missing Claude identity must not wedge unrelated config writers");
        db.delete(&path).expect("credential deletion remains admitted");
        let repaired = br#"{"type":"claude","email":"existing@example.test","access_token":"manual-repair"}"#;
        db.save(&path, repaired).expect("credential repair remains admitted");
        db.admit_work().unwrap();
        assert_eq!(
            db.read(&path).unwrap().unwrap(),
            repaired,
            "disposed login must not replay"
        );
        assert_eq!(
            db.documents().unwrap().len(),
            1,
            "missing identity must not create another file"
        );
        drop((state, db));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn terminal_builtin_login_invalid_xai_weight_does_not_block_writers() {
        let (root, state, db) = sqlite_state("terminal-xai");
        let path = db.auth_dir().join("xai-w.json");
        // The legacy import and ordinary raw-document writer both preserve this
        // source even though synthesis rejects its metadata weight.
        let original = br#"{"type":"xai","weight":1000001,"access_token":"R0"}"#;
        db.save(&path, original).unwrap();
        state.publish((*state.rt.config()).clone(), None);
        assert!(state.rt.store().get("xai-w.json").is_none());
        state.oauth.register("invalid-weight", "xai");
        let record = ck_exec::xai_auth::LoginRecord {
            file_name: "xai-w.json".into(),
            metadata: [
                ("type".into(), ck_wire::json::GoValue::String("xai".into())),
                ("access_token".into(), ck_wire::json::GoValue::String("R2".into())),
            ]
            .into(),
            label: "private test".into(),
        };
        let outcome = save_if_pending(
            &state,
            "invalid-weight",
            "xai",
            record,
            |dir, record, save| {
                let path = dir.join(&record.file_name);
                save_login(Some(&save), &[path], || save_xai_login(&dir, record))
            },
            "Failed to save token to file",
        )
        .await;
        assert!(
            matches!(outcome, Outcome::Failed(_)),
            "validation must not report success"
        );
        settle(&state, "invalid-weight", outcome).await;
        let session = state.oauth.get("invalid-weight").unwrap();
        assert!(!session.2);
        assert!(!session.1.is_empty(), "terminal login failure is visible");
        assert_eq!(db.read(&path).unwrap().unwrap(), original);
        state
            .config_store()
            .import_yaml("server: {port: 8319}\n", "test", "unrelated")
            .expect("terminal render must not wedge unrelated config writers");
        db.delete(&path).expect("the offending source can be deleted");
        db.save(&path, br#"{"type":"xai","weight":1,"access_token":"manual-repair"}"#)
            .expect("source repair is admitted");
        db.admit_work().unwrap();
        let raw: Value = serde_json::from_slice(&db.read(&path).unwrap().unwrap()).unwrap();
        assert_eq!(raw["access_token"], "manual-repair", "disposed result never retries");
        drop((state, db));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn terminal_builtin_login_withholds_only_its_unchanged_valid_destination() {
        let (root, state, db) = sqlite_state("terminal-exact-fence");
        let path = db.auth_dir().join("xai-w.json");
        db.save(&path, br#"{"type":"xai","access_token":"old-w","weight":1}"#)
            .unwrap();
        let other = db.auth_dir().join("xai-other.json");
        db.save(&other, br#"{"type":"xai","access_token":"other"}"#).unwrap();
        state.publish((*state.rt.config()).clone(), None);
        let old = state.rt.store().get("xai-w.json").unwrap();
        let unaffected = state.rt.store().get("xai-other.json").unwrap();
        assert!(!state.rt.store().tokens_rejected(&old));
        state.oauth.register("invalid-new-weight", "xai");
        let record = ck_exec::xai_auth::LoginRecord {
            file_name: "xai-w.json".into(),
            metadata: [
                ("type".into(), ck_wire::json::GoValue::String("xai".into())),
                ("access_token".into(), ck_wire::json::GoValue::String("R2".into())),
                ("weight".into(), ck_wire::json::GoValue::Number("1000001".into())),
            ]
            .into(),
            label: "private test".into(),
        };
        let outcome = save_if_pending(
            &state,
            "invalid-new-weight",
            "xai",
            record,
            |dir, record, save| {
                let path = dir.join(&record.file_name);
                save_login(Some(&save), &[path], || save_xai_login(&dir, record))
            },
            "save failed",
        )
        .await;
        assert!(matches!(outcome, Outcome::Failed(_)));
        settle(&state, "invalid-new-weight", outcome).await;
        assert!(
            state.rt.store().tokens_rejected(&old),
            "do not re-enable superseded source"
        );
        assert!(
            !state.rt.store().tokens_rejected(&unaffected),
            "dispose provider-wide withholding"
        );
        state
            .config_store()
            .import_yaml("server: {port: 8319}\n", "test", "unrelated")
            .unwrap();
        assert!(
            state.rt.store().tokens_rejected(&old),
            "unrelated commit cannot settle exact-source fence"
        );
        db.save(&path, br#"{"type":"xai","access_token":"replacement","weight":1}"#)
            .unwrap();
        state.publish((*state.rt.config()).clone(), None);
        let adopted = state.rt.store().get("xai-w.json").unwrap();
        assert_eq!(adopted.str("access_token"), Some("replacement"));
        assert!(!state.rt.store().tokens_rejected(&adopted));
        drop((state, db));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn builtin_login_render_errors_distinguish_rejection_from_retryable_effects() {
        for error in [
            "auth filestore: invalid metadata weight: weight must not exceed 1000000",
            "invalid credential email filename",
            "claude token storage missing account information",
            "codex token storage missing account information",
            "meta token storage missing access token",
        ] {
            assert!(login_render_error(error.into()).is::<LoginRejected>(), "{error}");
        }
        for error in [
            "Permission denied",
            "No space left on device",
            "injected pre-render effect failure",
        ] {
            assert!(!login_render_error(error.into()).is::<LoginRejected>(), "{error}");
        }
    }

    #[tokio::test]
    async fn terminal_builtin_login_captured_source_replaced_by_grant_does_not_block_writers() {
        let (root, state, db) = sqlite_state("terminal-source");
        let path = db.auth_dir().join("codex.json");
        db.save(&path, br#"{"type":"codex","access_token":"D0"}"#).unwrap();
        state.publish((*state.rt.config()).clone(), None);
        state.oauth.register("captured", "codex");
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = attempts.clone();
        let outcome = save_if_pending(
            &state,
            "captured",
            "codex",
            (),
            move |dir, (), save| {
                let path = dir.join("codex.json");
                let first = observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0;
                if first {
                    std::fs::remove_file(&path).unwrap();
                    std::fs::create_dir(&path).unwrap(); // Retryable hydration I/O after D0 capture.
                }
                save_login(Some(&save), std::slice::from_ref(&path), || {
                    std::fs::write(&path, br#"{"type":"codex","access_token":"stale-held"}"#)
                        .map_err(|e| e.to_string())?;
                    Ok(path.clone())
                })
            },
            "save failed",
        )
        .await;
        assert!(matches!(outcome, Outcome::Failed(_)));
        settle(&state, "captured", outcome).await;
        let old = state.rt.store().get("codex.json").unwrap();
        assert!(state.rt.store().tokens_rejected(&old));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let sent = Arc::new(Mutex::new(Vec::new()));
        let captured = sent.clone();
        let app = axum::Router::new().fallback(move |headers: axum::http::HeaderMap| {
            let captured = captured.clone();
            async move {
                captured
                    .lock()
                    .unwrap()
                    .push(headers.get("authorization").unwrap().to_str().unwrap().to_owned());
                (StatusCode::IM_A_TEAPOT, "local mock received request")
            }
        });
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let d1 = serde_json::to_vec(&serde_json::json!({
            "type":"codex","access_token":"D1","opaque":"competing-SIWC","base_url":base
        }))
        .unwrap();
        // The same already-in-flight SIWC preservation admission bypasses queue
        // draining, but not canonical source/version checks in its real adapter.
        db.granted_command().unwrap().save_checked(&path, &d1, None).unwrap();
        db.admit_work()
            .expect("captured D0 conflict must be disposed, not requeued forever");
        assert_eq!(db.read(&path).unwrap().unwrap(), d1);
        state
            .config_store()
            .import_yaml("server: {port: 8319}\n", "test", "unrelated")
            .unwrap();
        state.publish((*state.rt.config()).clone(), None);
        let adopted = state.rt.store().get("codex.json").unwrap();
        assert_eq!(adopted.str("access_token"), Some("D1"));
        assert!(
            !state.rt.store().tokens_rejected(&adopted),
            "D1 dispatch admission is not withheld by disposed D0"
        );
        let cfg = state.rt.config();
        let lease = state
            .rt
            .lease_credential(
                "codex.json",
                crate::runtime::state::Selection::new("codex", "gpt-5"),
                &cfg,
                |credential| {
                    let mut local = credential.clone();
                    // Executor endpoints are attributes, not source metadata.
                    local.attributes.insert("base_url".into(), base.clone());
                    local.attributes.insert("proxy_url".into(), "direct".into());
                    local
                },
            )
            .await
            .expect("D1 receives a real dispatch lease");
        assert_eq!(lease.credential.attributes.get("base_url"), Some(&base));
        assert!(base.starts_with("http://127.0.0.1:"));
        let body = bytes::Bytes::from_static(br#"{"model":"gpt-5","input":"private local mock"}"#);
        let request = ck_core::exec::ExecRequest {
            operation: ck_core::exec::Operation::Generate,
            source_format: ck_core::format::Format::OpenAIResponse,
            response_format: ck_core::format::Format::OpenAIResponse,
            requested_model: "gpt-5".into(),
            model: "gpt-5".into(),
            original_body: body.clone(),
            body,
            stream: false,
            alt: None,
            session: None,
            execution_session: None,
            derived_session: None,
            headers: axum::http::HeaderMap::new(),
            resolved_model: None,
            usage: Default::default(),
            request_path: String::new(),
            caller: ck_core::exec::Caller {
                principal: String::new(),
                source: "authorization",
                provider: String::new(),
            },
        };
        let dispatch = state
            .rt
            .executors
            .codex
            .execute(&crate::testing::guarded(&lease.credential, &cfg), request, &cfg)
            .await;
        match dispatch {
            Err(error) => assert_eq!(
                error.status,
                418,
                "local dispatch error: {}",
                String::from_utf8_lossy(&error.body)
            ),
            Ok(_) => panic!("local mock must reject with 418"),
        }
        assert_eq!(
            *sent.lock().unwrap(),
            ["Bearer D1"],
            "only D1 reaches the local upstream"
        );
        drop(lease);
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
        db.admit_work().unwrap();
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 2);
        // Dispatch preparation may canonically normalize disabled:false. It must
        // retain D1 and the competing grant's fields, never the disposed result.
        let after_dispatch: Value = serde_json::from_slice(&db.read(&path).unwrap().unwrap()).unwrap();
        assert_eq!(after_dispatch["access_token"], "D1");
        assert_eq!(after_dispatch["opaque"], "competing-SIWC");
        let session = state.oauth.get("captured").unwrap();
        assert!(!session.2);
        assert!(!session.1.is_empty());
        // Dropping a dispatch lease can schedule owned cooldown persistence.
        state.rt.shutdown_codex_refreshes().await;
        drop((state, db));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn completed_builtin_login_survives_failures_before_complete_bytes_exist() {
        for phase in ["before-writer", "hydrate"] {
            let (root, state, db) = sqlite_state(phase);
            let path = db.auth_dir().join("codex.json");
            db.save(&path, br#"{"type":"codex","access_token":"R0","opaque":"keep"}"#)
                .unwrap();
            state.publish((*state.rt.config()).clone(), None);
            state.oauth.register("consumed", "codex");
            let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let observed = attempts.clone();
            let runtime = state.rt.clone();
            // This value represents one completed exchange. Only local rendering
            // is repeated: no upstream effect is inside the held result.
            let outcome = save_if_pending(
                &state,
                "consumed",
                "codex",
                "R2".to_owned(),
                move |dir, token, save| {
                    let first = observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0;
                    if !first {
                        let old = runtime.store().get("codex.json").unwrap();
                        assert!(
                            runtime.store().tokens_rejected(&old),
                            "withhold through rendering, not only until owner admission"
                        );
                    }
                    if first && phase == "before-writer" {
                        return Err("injected pre-render effect failure".into());
                    }
                    let path = dir.join("codex.json");
                    if first && phase == "hydrate" {
                        std::fs::remove_file(&path).unwrap();
                        std::fs::create_dir(&path).unwrap(); // Real hydration I/O fails before writer output.
                    }
                    save_login(Some(&save), std::slice::from_ref(&path), || {
                        let mut raw: Value =
                            serde_json::from_slice(&std::fs::read(&path).map_err(|e| e.to_string())?).unwrap();
                        raw["access_token"] = token.into();
                        std::fs::write(&path, serde_json::to_vec(&raw).unwrap()).map_err(|e| e.to_string())?;
                        Ok(path.clone())
                    })
                },
                "save failed",
            )
            .await;
            assert!(
                matches!(outcome, Outcome::Failed(_)),
                "{phase}: do not acknowledge unresolved result"
            );
            assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
            let old = state.rt.store().get("codex.json").unwrap();
            assert!(
                state.rt.store().tokens_rejected(&old),
                "{phase}: withhold potentially invalidated old credentials"
            );
            assert_eq!(
                serde_json::from_slice::<Value>(&db.read(&path).unwrap().unwrap()).unwrap()["access_token"],
                "R0"
            );
            state.oauth.set_error("consumed", "save failed"); // Reported failure is not cancellation.
            // UI expiry must not expire an already-consumed provider result.
            state.oauth.0.lock().unwrap().get_mut("consumed").unwrap().expires = Instant::now();
            assert!(state.oauth.get("consumed").is_none());
            assert!(
                state.rt.store().tokens_rejected(&old),
                "expiry cannot release the source fence"
            );
            db.admit_work().unwrap();
            let raw: Value = serde_json::from_slice(&db.read(&path).unwrap().unwrap()).unwrap();
            assert_eq!(
                raw["access_token"], "R2",
                "{phase}: original completed result recovered"
            );
            assert_eq!(raw["opaque"], "keep");
            state.publish((*state.rt.config()).clone(), None);
            let adopted = state.rt.store().get("codex.json").unwrap();
            assert_eq!(adopted.str("access_token"), Some("R2"));
            assert!(!state.rt.store().tokens_rejected(&adopted));
            assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 2);
            db.admit_work().unwrap();
            assert_eq!(
                attempts.load(std::sync::atomic::Ordering::SeqCst),
                2,
                "result finished once"
            );
            drop((state, db));
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn completed_login_results_merge_behind_pending_publication_without_success() {
        let (root, state, db) = sqlite_state("pending-logins");
        let database = state.config_store().clone();
        let blocked = crate::testing::block_publication(database.path().unwrap());
        assert!(
            database
                .import_yaml("server: {port: 8318}\n", "test", "pending")
                .is_err()
        );
        for name in ["a.json", "b.json"] {
            state.oauth.register(name, "codex");
            let outcome = save_if_pending(
                &state,
                name,
                "codex",
                name.to_owned(),
                |dir, name, save| {
                    let path = dir.join(&name);
                    save_login(Some(&save), std::slice::from_ref(&path), || {
                        crate::store::credential_store::atomic_write(&path,&serde_json::to_vec(&serde_json::json!({
                        "type":"codex","access_token":format!("grant-{name}"),"refresh_token":format!("refresh-{name}")
                    })).unwrap())?;
                        Ok(path.clone())
                    })
                },
                "save pending",
            )
            .await;
            assert!(
                matches!(outcome, Outcome::Failed(_)),
                "preservation is not save success"
            );
        }
        let ordinary = db.auth_dir().join("ordinary.json");
        assert!(
            db.save(&ordinary, br#"{"type":"codex","access_token":"manual"}"#)
                .is_err()
        );
        assert!(db.documents().unwrap().is_empty(), "accepted memory is still old");
        drop(blocked);
        db.admit_work().unwrap();
        let docs = db.documents().unwrap();
        assert_eq!(
            docs.len(),
            2,
            "both completed initial results survive, not the refused ordinary save"
        );
        for (path, bytes) in docs {
            let document: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(
                document["refresh_token"],
                format!("refresh-{}", path.file_name().unwrap().to_str().unwrap())
            );
        }
        drop((state, db, database));
        std::fs::remove_dir_all(root).unwrap();
    }

    /// Saves a Vertex login result for `sid` through the login save worker.
    async fn save_vertex(state: &Arc<Management>, sid: &str) -> Outcome {
        save_if_pending(
            state,
            sid,
            "vertex",
            (),
            |dir, (), save| {
                let path = dir.join("vertex.json");
                save_login(Some(&save), std::slice::from_ref(&path), || {
                    std::fs::write(&path, VERTEX_SAVED).map_err(|error| error.to_string())?;
                    Ok(path.clone())
                })
            },
            "save failed",
        )
        .await
    }

    const VERTEX_SAVED: &[u8] = br#"{"type":"vertex","prefix":"saved"}"#;

    /// Fires once when the login save finds `path`'s lock held and is about to wait for it.
    fn busy_signal(
        path: &std::path::Path,
    ) -> (
        crate::testing::installation_effects::Guard,
        tokio::sync::oneshot::Receiver<()>,
    ) {
        use crate::testing::installation_effects::{self, Stage};
        let (busy_tx, busy_rx) = tokio::sync::oneshot::channel();
        let guard = installation_effects::once(path, Stage::LoginRepairBusy, move || {
            let _ = busy_tx.send(());
            Ok(())
        });
        (guard, busy_rx)
    }

    /// Bounds only a regression's failure; a correct wait ends well before it.
    const BOUNDED: Duration = Duration::from_secs(10);

    /// Cache repair rewrites every document, so a login save waits while a refresh holds
    /// any document's lock, not only its destination's.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn login_save_waits_for_a_lock_on_an_unrelated_document() {
        let (root, state, db) = sqlite_state("unrelated-lock");
        let other = db.auth_dir().join("other.json");
        let before = br#"{"type":"claude","access_token":"unrelated-fixture"}"#;
        db.save(&other, before).unwrap();
        let document = ck_exec::codex_siwc_bind::lock_file(&other).unwrap();
        let (first, busy) = busy_signal(&other);
        state.oauth.register("unrelated-lock", "vertex");
        let saving = tokio::spawn({
            let state = state.clone();
            async move { save_vertex(&state, "unrelated-lock").await }
        });
        tokio::time::timeout(BOUNDED, busy)
            .await
            .expect("the login save found the unrelated lock held")
            .unwrap();
        assert!(
            !saving.is_finished(),
            "the login save repaired the cache under a held lock"
        );
        drop(first);
        let (_again, mut again) = busy_signal(&other);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            again.try_recv().is_err(),
            "the login save probed again without waiting for the held lock"
        );
        drop(document);
        let outcome = tokio::time::timeout(BOUNDED, saving)
            .await
            .expect("the login save waits for the released lock")
            .unwrap();
        assert!(matches!(outcome, Outcome::Saved));
        let path = db.auth_dir().join("vertex.json");
        assert_eq!(db.read(&path).unwrap().unwrap(), VERTEX_SAVED);
        assert_eq!(std::fs::read(&path).unwrap(), VERTEX_SAVED, "the cache was repaired");
        assert_eq!(std::fs::read(&other).unwrap(), before);
        drop(state);
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }

    /// The login result commits at admission, before the cache repair. Locks that keep
    /// changing hands therefore delay the repair but never fail a login whose credential
    /// is already stored.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn login_save_repairs_after_any_number_of_busy_documents() {
        let (root, state, db) = sqlite_state("busy-handoff");
        let documents: Vec<_> = (0..17)
            .map(|i| {
                let data = format!(r#"{{"type":"claude","access_token":"busy-fixture-{i:02}"}}"#);
                (db.auth_dir().join(format!("busy-{i:02}.json")), data.into_bytes())
            })
            .collect();
        db.command().unwrap().save_documents(&documents).unwrap();
        // The repair probes documents in name order, so each wait meets the next lock.
        let held: Vec<_> = documents
            .iter()
            .map(|(path, _)| (ck_exec::codex_siwc_bind::lock_file(path).unwrap(), busy_signal(path)))
            .collect();
        state.oauth.register("busy-handoff", "vertex");
        let saving = tokio::spawn({
            let state = state.clone();
            async move { save_vertex(&state, "busy-handoff").await }
        });
        for (document, (guard, busy)) in held {
            tokio::time::timeout(BOUNDED, busy)
                .await
                .expect("the login save waits for each held lock in turn")
                .unwrap();
            drop(document);
            drop(guard);
        }
        let outcome = tokio::time::timeout(BOUNDED, saving).await.unwrap().unwrap();
        assert!(matches!(outcome, Outcome::Saved));
        assert_eq!(
            db.read(&db.auth_dir().join("vertex.json")).unwrap().unwrap(),
            VERTEX_SAVED
        );
        drop(state);
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }

    /// Shutdown joins login workers, so a save waiting for a refresh's lock must stop
    /// waiting. The credential committed at admission stays; the next start's
    /// `CredentialDb::initialize` restores every cache file.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_stops_a_login_save_waiting_for_a_document_lock() {
        let (root, state, db) = sqlite_state("shutdown-busy");
        let other = db.auth_dir().join("other.json");
        db.save(&other, br#"{"type":"claude","access_token":"shutdown-fixture"}"#)
            .unwrap();
        let document = ck_exec::codex_siwc_bind::lock_file(&other).unwrap();
        let (_busy, busy) = busy_signal(&other);
        state.oauth.register("shutdown-busy", "vertex");
        let worker = state.clone();
        spawn_login(&state, "shutdown-busy", "vertex", None, async move {
            let outcome = save_vertex(&worker, "shutdown-busy").await;
            settle(&worker, "shutdown-busy", outcome).await;
        });
        tokio::time::timeout(BOUNDED, busy)
            .await
            .expect("the login save found the lock held")
            .unwrap();
        tokio::time::timeout(BOUNDED, state.shutdown())
            .await
            .expect("shutdown waited for a login save blocked on a document lock");
        assert_eq!(
            db.read(&db.auth_dir().join("vertex.json")).unwrap().unwrap(),
            VERTEX_SAVED
        );
        assert!(
            state.oauth.get("shutdown-busy").unwrap().2,
            "the stored login reported success"
        );
        drop(document);
        state.rt.shutdown_codex_refreshes().await;
        drop(state);
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }

    /// Marks the re-executed test process that runs under a lowered descriptor limit.
    #[cfg(unix)]
    const LOW_NOFILE_CHILD: &str = "CK_OAUTH_LOW_NOFILE_CHILD";

    /// A login save must not hold a descriptor per stored document: the child lowers only
    /// its own soft `RLIMIT_NOFILE` before any runtime exists, so the parent and other
    /// tests keep theirs.
    #[cfg(unix)]
    #[test]
    fn login_save_beside_2000_documents_fits_a_1024_descriptor_limit() {
        const NAME: &str =
            "management::api::oauth::tests::login_save_beside_2000_documents_fits_a_1024_descriptor_limit";
        if std::env::var_os(LOW_NOFILE_CHILD).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", NAME, "--nocapture", "--test-threads=1"])
                .env(LOW_NOFILE_CHILD, "1")
                .output()
                .unwrap();
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                output.status.success() && stdout.contains("1 passed"),
                "low-descriptor child failed ({}):\n--- stdout\n{stdout}\n--- stderr\n{stderr}",
                output.status
            );
            return;
        }
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: getrlimit and setrlimit only read and write the given struct.
        assert_eq!(unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) }, 0);
        assert!(limit.rlim_max >= 1024, "hard descriptor limit below 1024");
        limit.rlim_cur = 1024;
        assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) }, 0);
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let (root, state, db) = sqlite_state("low-nofile");
                let documents: Vec<_> = (0..2000)
                    .map(|i| {
                        let data = format!(r#"{{"type":"claude","access_token":"low-nofile-fixture-{i:04}"}}"#);
                        (db.auth_dir().join(format!("low-nofile-{i:04}.json")), data.into_bytes())
                    })
                    .collect();
                db.command().unwrap().save_documents(&documents).unwrap();
                state.oauth.register("low-nofile", "vertex");
                let outcome = save_if_pending(
                    &state,
                    "low-nofile",
                    "vertex",
                    (),
                    |dir, (), save| {
                        let path = dir.join("vertex.json");
                        save_login(Some(&save), std::slice::from_ref(&path), || {
                            std::fs::write(&path, br#"{"type":"vertex","prefix":"saved"}"#)
                                .map_err(|error| error.to_string())?;
                            Ok(path.clone())
                        })
                    },
                    "save failed",
                )
                .await;
                assert!(matches!(outcome, Outcome::Saved));
                let path = db.auth_dir().join("vertex.json");
                let saved = br#"{"type":"vertex","prefix":"saved"}"#;
                assert_eq!(db.read(&path).unwrap().unwrap(), saved);
                assert_eq!(std::fs::read(&path).unwrap(), saved);
                assert_eq!(db.documents().unwrap().len(), 2001);
                drop(state);
                drop(db);
                std::fs::remove_dir_all(root).unwrap();
            });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pending_provider_save_waits_for_document_before_taking_disk() {
        let (root, state, db) = sqlite_state("provider-lock-order");
        let path = db.auth_dir().join("vertex.json");
        db.save(&path, br#"{"type":"vertex","prefix":"before"}"#).unwrap();
        let document = ck_exec::codex_siwc_bind::lock_file(&path).unwrap();
        state.oauth.register("lock-order", "vertex");
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let started_tx = Arc::new(Mutex::new(Some(started_tx)));
        let saving = tokio::spawn({
            let state = state.clone();
            async move {
                save_if_pending(
                    &state,
                    "lock-order",
                    "vertex",
                    (),
                    move |dir, (), save| {
                        let path = dir.join("vertex.json");
                        if let Some(started) = started_tx.lock().unwrap().take() {
                            started.send(()).unwrap();
                        }
                        save_login(Some(&save), std::slice::from_ref(&path), || {
                            std::fs::write(&path, br#"{"type":"vertex","prefix":"saved"}"#)
                                .map_err(|error| error.to_string())?;
                            Ok(path.clone())
                        })
                    },
                    "save failed",
                )
                .await
            }
        });
        started_rx.await.unwrap();
        let (released_tx, released_rx) = tokio::sync::oneshot::channel();
        let disk = tokio::task::spawn_blocking({
            let state = state.clone();
            move || {
                drop(state.disk.lock().unwrap_or_else(PoisonError::into_inner));
                released_tx.send(()).unwrap();
            }
        });
        // The channel proves disk was acquired and released while the document is
        // still locked. The timeout only bounds a lock-order regression's failure.
        let released = tokio::time::timeout(Duration::from_secs(5), released_rx).await;
        drop(document);
        released.expect("document lock wait held management disk").unwrap();
        disk.await.unwrap();
        assert!(matches!(saving.await.unwrap(), Outcome::Saved));
        let saved = br#"{"type":"vertex","prefix":"saved"}"#;
        assert_eq!(db.read(&path).unwrap().unwrap(), saved);
        assert_eq!(std::fs::read(&path).unwrap(), saved);
        drop(state);
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn session_causes_drop_upstream_bodies_and_descriptions() {
        for (cause, want) in [
            (
                r#"xai device token request failed with status 500: {"access_token":"sentinel"}"#,
                "xai device token request failed with status 500",
            ),
            (
                "xai device token error: invalid_grant: sentinel-description",
                "xai device token error: invalid_grant",
            ),
            ("xai device token error: weird", "xai device token error: weird"),
            (
                "meta device flow failed (HTTP 401): sentinel-body",
                "meta device flow failed (HTTP 401)",
            ),
            (
                "meta auth: error from authorization server: server_error: sentinel",
                "meta auth: error from authorization server: server_error",
            ),
            ("kimi: OAuth error: bad_code - sentinel", "kimi: OAuth error: bad_code"),
            ("kimi: OAuth error: sentinel$token!", "kimi: OAuth error: sentinel"),
            ("xai device code expired", "xai device code expired"),
            // A later marker inside the description must not decide the cut.
            (
                "kimi: OAuth error: invalid_grant - SENTINEL with status 500",
                "kimi: OAuth error: invalid_grant",
            ),
            (
                "xai device token error: x: SENTINEL (HTTP 500)",
                "xai device token error: x",
            ),
            // Decoder diagnostics quote response values.
            (
                r#"failed to parse token response: invalid type: string "SENTINEL", expected i64"#,
                "failed to parse token response",
            ),
            (
                "xai device token: parse response: json: cannot unmarshal number 123456789012345678901234567890",
                "xai device token: parse response",
            ),
            (
                "meta auth: parse mint response: number 99999999999999999999",
                "meta auth: parse mint response",
            ),
        ] {
            assert_eq!(redact_upstream(cause), want);
        }
        assert_eq!(
            with_cause(
                "Authentication failed",
                "kimi: device code request failed with status 400: body"
            ),
            "Authentication failed: kimi: device code request failed with status 400"
        );
    }

    #[test]
    fn states_and_providers_follow_go() {
        assert!(valid_state("kmi-ai-1759.x_y"));
        for bad in ["", "a/b", "a\\b", "a..b", "sp ace", &"x".repeat(129)] {
            assert!(!valid_state(bad), "{bad}");
        }
        assert_eq!(normalize_callback_provider(" Claude ").as_deref(), Some("anthropic"));
        assert_eq!(normalize_callback_provider("grok").as_deref(), Some("xai"));
        assert_eq!(normalize_callback_provider("kimi").as_deref(), Some("kimi"));
        assert_eq!(normalize_callback_provider("kimi_ai"), None);
    }

    #[test]
    fn sessions_follow_go_lifecycle() {
        let s = Sessions::default();
        s.register("st", "codex");
        assert!(s.is_pending("st", "CODEX"));
        assert_eq!(s.deliver("openai", "st", "c", ""), Delivery::Delivered);
        assert_eq!(s.take_callback("st").map(|c| c.code).as_deref(), Some("c"));
        s.set_error("st", " ");
        assert_eq!(
            s.get("st"),
            Some(("codex".into(), "Authentication failed".into(), false))
        );
        assert!(!s.cancel("st"), "an errored session is not cancellable");
        assert_eq!(s.deliver("codex", "st", "c", ""), Delivery::NotPending);
        s.register("done", "meta");
        s.complete("done");
        assert_eq!(s.get("done"), Some(("meta".into(), String::new(), true)));
        assert!(!s.cancel("done"));
        // 61 s later the success still reads, and a second callback is still refused.
        s.lock().get_mut("done").unwrap().expires -= Duration::from_secs(61);
        assert_eq!(s.get("done"), Some(("meta".into(), String::new(), true)));
        assert_eq!(s.deliver("meta", "done", "c", ""), Delivery::NotPending);
        s.register("live", "kimi");
        assert!(s.cancel("live"));
        assert!(s.get("live").is_none());
    }

    /// Go `TestSIWCBindCommitSurvivesSessionExpiry`: a binding whose disk commit has
    /// started outlives its session expiry so it can report the outcome, and completing
    /// it releases that hold.
    #[test]
    fn a_claimed_siwc_commit_outlives_the_session_expiry() {
        let s = Sessions::default();
        let flow = super::super::siwc::Flow::idle("codex.json");
        assert!(s.register_siwc("st", flow.clone(), Duration::from_secs(300)));
        assert!(s.claim_siwc_commit("st", &flow));
        let expire = || s.lock().get_mut("st").unwrap().expires = Instant::now();
        expire();
        assert_eq!(s.get("st"), Some(("codex".into(), String::new(), false)));
        assert!(!s.cancel("st"), "a started commit is past cancelling");
        s.complete("st");
        assert_eq!(s.get("st"), Some(("codex".into(), String::new(), true)));
        expire();
        assert!(s.get("st").is_none(), "the completed session no longer holds on");
    }

    #[test]
    fn forwarder_redirect_keeps_the_query() {
        let r = String::from_utf8(redirect_bytes(
            "GET",
            "http://127.0.0.1:8317/codex/callback",
            Some("code=a&state=b"),
        ))
        .unwrap();
        assert!(r.starts_with("HTTP/1.1 302 Found\r\n"), "{r}");
        assert!(
            r.contains("\r\nLocation: http://127.0.0.1:8317/codex/callback?code=a&state=b\r\n"),
            "{r}"
        );
        assert!(r.contains("\r\nCache-Control: no-store\r\n"), "{r}");
        assert!(r.ends_with("\">Found</a>.\n\n"), "{r}");
        let post = String::from_utf8(redirect_bytes("POST", "http://h/cb", None)).unwrap();
        assert!(
            post.ends_with("Content-Length: 0\r\nConnection: close\r\n\r\n"),
            "{post}"
        );
    }

    /// A client that never finishes its request head is dropped after Go's 5 s
    /// header deadline; a complete one gets the redirect.
    #[tokio::test]
    async fn forwarder_connections_have_go_deadlines() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((s, _)) = listener.accept().await {
                tokio::spawn(forward(s, "http://127.0.0.1:1/x".into()));
            }
        });
        let mut slow = tokio::net::TcpStream::connect(addr).await.unwrap();
        slow.write_all(b"GET /?a=1 HTTP/1.1\r\nHost: x\r\n").await.unwrap();
        let mut buf = Vec::new();
        let closed = tokio::time::timeout(Duration::from_secs(8), slow.read_to_end(&mut buf)).await;
        assert!(matches!(closed, Ok(Ok(0))), "dropped without an answer");
        let mut ok = tokio::net::TcpStream::connect(addr).await.unwrap();
        ok.write_all(b"GET /cb?code=c HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        let mut out = String::new();
        ok.read_to_string(&mut out).await.unwrap();
        assert!(out.contains("Location: http://127.0.0.1:1/x?code=c\r\n"), "{out}");
    }
}
