//! Management API: the `/admin/v1` config resources ([`admin`]) and the v8 routes for
//! credentials, sign-ins and logs (plus the few v0 spellings the dashboard
//! predates).
//!
//! Routing follows gin: an unknown path or an unregistered method on a known path is a
//! bare 404 that never reaches authentication. Access rules live in [`access`].
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};

use axum::body::Bytes;
use axum::extract::rejection::BytesRejection;
use axum::extract::{DefaultBodyLimit, OriginalUri, State};
use axum::handler::Handler;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{MethodRouter, any, get};
use axum::{Router, middleware};
use ck_core::config::{Config, ConfigDocument, credentials};
use ck_core::credential::{Credential, Source};
use ck_wire::gostr::GoStr;
use serde_json::{Value, json};

use crate::Runtime;
use crate::scheduling::scheduler::{ErrorRule, Policy};
use crate::store::config_store::{CommitError, Committed, ConfigStore};

mod access;
mod admin;
mod api_call;
mod auth_files;
mod auth_index;
mod iso_currency;
pub(crate) mod legacy;
mod logs;
pub(crate) mod multipart;
mod oauth;
pub(crate) use oauth::{
    HeldLogin, LoginRejected, login_render_error, normalize_callback_provider as oauth_provider_key,
};
pub mod observability;
pub(crate) mod quota;
mod quota_probe;
mod session;
mod siwc;
mod streams_stats;
pub use access::cors;
pub(crate) use access::{Cors, cors_headers};

pub struct Management {
    pub(crate) rt: Arc<Runtime>,
    /// Where the config lives; every write goes through [`Management::commit_with`].
    pub(crate) config: Arc<ConfigStore>,
    /// The revision and digest of the stored config the runtime last published.
    published: Mutex<(u64, String)>,
    /// Another database connection committed; the watcher must rescan canonical credentials.
    pub(crate) credential_changes: Arc<tokio::sync::Notify>,
    /// Settings actually used to construct the listener and runtime.
    startup: Arc<Config>,
    // ponytail: one lock for config and auth disk operations; split only if management
    // throughput matters. The watcher shares it, preventing stale disk publication.
    pub(crate) disk: Mutex<()>,
    /// Uploaded files no synthesizer claims, kept listed until restart like Go's
    /// fallback auths (see `credentials::upload_fallback`).
    pub(crate) fallbacks: Mutex<std::collections::BTreeSet<PathBuf>>,
    /// Credentials disabled through the status or fields endpoints: Go's in-memory
    /// auth then reports `disabled via management API` until re-enabled.
    pub(crate) disabled_via_api: Mutex<std::collections::BTreeSet<String>>,
    /// When runtime-only credentials were first seen and last changed (Go's
    /// `CreatedAt`/`UpdatedAt` for them; the store keeps no timestamps).
    pub(crate) runtime_seen: Mutex<std::collections::HashMap<String, auth_files::RuntimeSeen>>,
    /// Go net/http clients by proxy for `api-call`.
    pub(crate) clients: ck_exec::proxy::GoClients,
    pub(crate) latest_release_url: std::borrow::Cow<'static, str>,
    pub(crate) update_check_disabled: bool,
    // No task or expiry timer: allocated only after a successful, explicit check.
    // Wall-clock time, so a cached result also expires across sleep or hibernation.
    pub(crate) latest_release: tokio::sync::Mutex<Option<(std::time::SystemTime, String)>>,
    /// Pending and recent management logins (Go `oauthSessionStore`).
    pub(crate) oauth: oauth::Sessions,
    /// Callback forwarders by port (Go `callbackForwarders`).
    pub(crate) forwarders: Mutex<std::collections::HashMap<u16, oauth::Forwarder>>,
    pub(crate) login_base: Option<String>,
    /// Told the state of every management login that has finished (tests only).
    pub(crate) login_settled: Option<tokio::sync::mpsc::UnboundedSender<String>>,
    /// Every login worker panics on its blocking save task (tests only).
    pub(crate) login_panic: bool,
    /// Go `Handler.logDir`: resolved once at startup.
    pub(crate) log_dir: PathBuf,
    /// The zone log line timestamps are parsed in (Go `time.Local` when `None`).
    pub(crate) log_zone: logs::Zone,
    access: access::Access,
    configuration_subscription: Mutex<Vec<crate::store::config_store::ConfigurationSubscription>>,
    admission_closed: Mutex<bool>,
    pub(crate) work: tokio_util::task::TaskTracker,
    pub(crate) stopping: tokio_util::sync::CancellationToken,
    pub(crate) transport_stop: tokio_util::sync::CancellationToken,
}

#[derive(Default)]
pub struct Options {
    /// `--password`: accepted from loopback clients only, like Go's local password.
    pub local_password: String,
    /// `-tui -standalone`: the local password alone keeps management on, across
    /// reloads, for loopback clients. Go refuses every request without a configured
    /// secret, so its standalone TUI cannot sign in with a keyless config.
    pub standalone: bool,
    /// Overrides the `CK_MANAGEMENT_PASSWORD` environment variable when set.
    pub management_password: Option<String>,
    /// Overrides [`observability::LATEST_RELEASE_URL`] (tests point it at a local server).
    pub latest_release_url: Option<String>,
    /// Overrides CK_NO_UPDATE_CHECK (tests only).
    pub update_check_disabled: Option<bool>,
    /// One local base URL for every provider's login endpoints (tests only).
    pub login_base: Option<String>,
    /// Receives the state of every management login once its worker has finished,
    /// saved, failed or cancelled (tests only).
    pub login_settled: Option<tokio::sync::mpsc::UnboundedSender<String>>,
    /// Every management login worker panics on its blocking save task (tests only).
    pub login_panic: bool,
    /// Overrides the log directory Go resolves at startup (tests only).
    pub log_dir: Option<PathBuf>,
    /// Parses log line timestamps in this zone instead of the local one (tests only).
    pub log_zone: Option<chrono::FixedOffset>,
}

impl Management {
    pub(crate) fn enter(&self) -> Option<tokio_util::task::task_tracker::TaskTrackerToken> {
        let closed = self.admission_closed.lock().unwrap_or_else(PoisonError::into_inner);
        if *closed { None } else { Some(self.work.token()) }
    }

    pub fn begin_shutdown(&self) {
        let mut closed = self.admission_closed.lock().unwrap_or_else(PoisonError::into_inner);
        *closed = true;
        self.stopping.cancel();
    }

    /// Join accepted requests, login workers, callback connections, blocking saves and
    /// the hourly attempt purge.
    pub async fn shutdown(&self) {
        self.begin_shutdown();
        self.transport_stop.cancel();
        self.work.close();
        self.work.wait().await;
    }

    /// Go `managementRoutesEnabled`, for the RESP protocol on the main listener.
    pub(crate) fn routes_enabled(&self) -> bool {
        self.access.available()
    }

    /// Go `AuthenticateManagementKey` for callers outside HTTP (the RESP protocol).
    pub(crate) async fn authenticate_key(
        &self,
        ip: &str,
        local: bool,
        provided: &[u8],
    ) -> Result<(), (StatusCode, String)> {
        let _operation = self.enter().ok_or((
            StatusCode::SERVICE_UNAVAILABLE,
            "configuration service is shutting down".into(),
        ))?;
        self.access
            .authenticate(&self.rt.config(), ip, local, provided, &self.work)
            .await
    }

    pub fn new(rt: Arc<Runtime>, config: Arc<ConfigStore>) -> Arc<Self> {
        Self::with_options(rt, config, Options::default())
    }

    /// Captures startup-only settings (trusted proxies, environment secret) and
    /// publishes the scheduler policy derived from the current config.
    pub fn with_options(rt: Arc<Runtime>, config: Arc<ConfigStore>, options: Options) -> Arc<Self> {
        let cfg = rt.config();
        rt.publish_policy(policy(&cfg));
        let latest_release_url = options
            .latest_release_url
            .clone()
            .map(std::borrow::Cow::Owned)
            .unwrap_or(std::borrow::Cow::Borrowed(observability::LATEST_RELEASE_URL));
        let update_check_disabled = options
            .update_check_disabled
            .unwrap_or_else(|| std::env::var("CK_NO_UPDATE_CHECK").is_ok_and(|v| v == "1"));
        let login_base = options.login_base.clone();
        let log_dir = options
            .log_dir
            .clone()
            .unwrap_or_else(|| crate::observability::logging::resolve_log_dir(&cfg));
        let mut options = options;
        let login_settled = options.login_settled.take();
        let login_panic = options.login_panic;
        let log_zone = options.log_zone;
        let access = access::Access::new(&cfg, options);
        rt.usage_queue().configure(access.available(), &cfg);
        let published = config
            .load()
            .ok()
            .filter(|l| l.config.document == cfg.document)
            .map(|l| (l.rev, l.digest))
            .unwrap_or_default();
        let state = Arc::new(Self {
            access,
            configuration_subscription: Mutex::new(Vec::new()),
            admission_closed: Mutex::new(false),
            work: tokio_util::task::TaskTracker::new(),
            stopping: tokio_util::sync::CancellationToken::new(),
            transport_stop: tokio_util::sync::CancellationToken::new(),
            rt,
            config,
            published: Mutex::new(published),
            credential_changes: Arc::default(),
            startup: cfg,
            disk: Mutex::new(()),
            fallbacks: Mutex::default(),
            disabled_via_api: Mutex::default(),
            runtime_seen: Mutex::default(),
            clients: ck_exec::proxy::GoClients::new(ck_exec::proxy::Hooks::default()),
            latest_release_url,
            update_check_disabled,
            latest_release: tokio::sync::Mutex::new(None),
            oauth: oauth::Sessions::default(),
            forwarders: Mutex::default(),
            login_base,
            login_settled,
            login_panic,
            log_dir,
            log_zone,
        });
        let weak = Arc::downgrade(&state);
        let notify_owner = weak.clone();
        let notify = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let installed = notify.clone();
        // Retain the original transition input until this whole adapter completes.
        // Runtime.config can advance before a fallible derived-install tail.
        let progress = Mutex::new(None::<(u64, String, Arc<Config>, bool)>);
        let (installation, notification) = state
            .config
            .install_configuration_and_notify(
                move |view| {
                    if let Some(state) = weak.upgrade() {
                        state.config.pin_configuration_owner(state.clone());
                        let previous = {
                            let mut progress = progress.lock().unwrap_or_else(PoisonError::into_inner);
                            if !progress
                                .as_ref()
                                .is_some_and(|(rev, digest, _, _)| *rev == view.revision && *digest == view.digest)
                            {
                                *progress = Some((view.revision, view.digest.clone(), state.rt.config(), false));
                            }
                            let (_, _, previous, completed) = progress.as_ref().unwrap();
                            if *completed {
                                return Ok(());
                            }
                            previous.clone()
                        };
                        state
                            .rt
                            .bind_configuration(|| state.accept_configuration(&view, previous));
                        progress
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .as_mut()
                            .unwrap()
                            .3 = true;
                        if view.change == crate::store::config_store::ConfigurationChange::Committed {
                            installed.store(true, std::sync::atomic::Ordering::Release);
                        }
                    }
                    Ok(())
                },
                move |view| {
                    // The initial view is announced once its installation succeeded
                    // (registration fails otherwise), so subscribers leave the
                    // pre-binding configuration.
                    let announce = match view.change {
                        crate::store::config_store::ConfigurationChange::Initial => true,
                        crate::store::config_store::ConfigurationChange::Committed => {
                            notify.swap(false, std::sync::atomic::Ordering::AcqRel)
                        }
                    };
                    if announce && let Some(state) = notify_owner.upgrade() {
                        // Runtime observers are ordinary success notifications too:
                        // never announce success while another required adapter failed.
                        state.config.pin_configuration_owner(state.clone());
                        state.rt.service_configuration_installed();
                    }
                    Ok(())
                },
            )
            .expect("validated configuration initial view");
        state
            .configuration_subscription
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend([installation, notification]);
        oauth::install_callback_sink(&state);
        access::start_purge(&state);
        state
    }

    /// Credentials always load from SQLite's materialized working folder.
    pub(crate) fn lock_auth_dir(&self, cfg: &mut Config) {
        cfg.auth_dir = self.rt.store().credential_backend().auth_dir();
    }

    /// Serializes Management's adapter work under `disk`. The service command
    /// itself owns persistence, runtime-view installation and ordered dispatch.
    /// Also reports whether a startup-only listener/TLS/proxy setting changed.
    pub(crate) fn commit_with(
        &self,
        write: impl FnOnce(&ConfigStore) -> Result<Committed, CommitError>,
    ) -> Result<(Committed, bool), CommitError> {
        self.config
            .check_reentry()
            .map_err(|error| CommitError::invalid(error.to_string()))?;
        let disk = self.disk.lock().unwrap_or_else(PoisonError::into_inner);
        self.commit_locked(&disk, write)
    }

    /// [`Self::commit_with`] for a caller already holding `disk`.
    pub(crate) fn commit_locked(
        &self,
        _disk: &std::sync::MutexGuard<'_, ()>,
        write: impl FnOnce(&ConfigStore) -> Result<Committed, CommitError>,
    ) -> Result<(Committed, bool), CommitError> {
        let done = write(&self.config)?;
        let restart = startup_changed(&self.startup, &done.config);
        Ok((done, restart))
    }

    /// Edits the config as one document, the way the credential endpoints change it; the caller holds `disk`. `edit`'s own error comes back as `Err(Ok(e))`.
    pub(crate) fn edit_document<E>(
        &self,
        disk: &std::sync::MutexGuard<'_, ()>,
        actor: &str,
        message: &str,
        edit: impl FnOnce(&mut ConfigDocument) -> Result<(), E>,
    ) -> Result<Committed, Result<E, CommitError>> {
        let mut failed = None;
        let done = self.commit_locked(disk, |store| {
            store.edit(actor, message, |json| {
                let invalid = |e: &dyn std::fmt::Display| {
                    crate::store::config_store::CommitError::invalid(format!("config is not YAML: {e}"))
                };
                let yaml = serde_yaml_ng::to_value(&*json).map_err(|e| invalid(&e))?;
                let mut doc = ConfigDocument::from_value(yaml);
                if let Err(e) = edit(&mut doc) {
                    failed = Some(e);
                    return Err(crate::store::config_store::CommitError::invalid("edit refused"));
                }
                *json = serde_json::to_value(ck_core::config::AsJson(doc.value())).map_err(|e| invalid(&e))?;
                Ok(())
            })
        });
        match (done, failed) {
            (_, Some(e)) => Err(Ok(e)),
            (Ok((done, _)), None) => Ok(done),
            (Err(e), None) => Err(Err(e)),
        }
    }

    /// Required projection adapter invoked by the service before change dispatch.
    /// Never takes Management.disk or submits a command to the active owner gate.
    fn accept_configuration(&self, view: &crate::store::config_store::ConfigurationView, previous: Arc<Config>) {
        let mut cfg = (*view.config).clone();
        self.lock_auth_dir(&mut cfg);
        if cfg != *previous {
            crate::store::config_diff::log(&previous, &cfg);
            self.access.config_published(&cfg);
            crate::observability::logging::configure(&cfg);
            self.rt.usage_queue().configure(self.access.available(), &cfg);
        }
        // Repeat incomplete derived effects even if Runtime.config already advanced.
        // Comparing the original transition input above also preserves unchanged
        // initial binding's existing access/logging/usage behavior.
        let policy = policy(&cfg);
        assert!(
            self.rt.install_service_configuration(cfg, policy, previous),
            "validated service configuration must install before acknowledgement"
        );
        if view.change == crate::store::config_store::ConfigurationChange::Committed {
            self.publish((*self.rt.config()).clone(), None);
        }
        *self.published.lock().unwrap_or_else(PoisonError::into_inner) = (view.revision, view.digest.clone());
    }

    /// The config database this server reads and writes.
    pub fn config_store(&self) -> &Arc<ConfigStore> {
        &self.config
    }

    /// The revision the runtime runs on.
    pub(crate) fn published_rev(&self) -> u64 {
        self.published.lock().unwrap_or_else(PoisonError::into_inner).0
    }

    /// Checks canonical configuration validity for the legacy reload adapter and
    /// supplies the already accepted runtime view. The caller holds `disk`.
    pub(crate) fn stored_config(&self) -> Result<Config, crate::store::config_store::Fault> {
        let loaded = self.config.load()?;
        let published = self.published.lock().unwrap_or_else(PoisonError::into_inner);
        if loaded.rev < published.0 {
            return Err(crate::store::config_store::Fault::new(
                None,
                "stored revision is older than the running revision",
            ));
        }
        // Credential reconciliation is not a configuration publication owner.
        Ok((*self.rt.config()).clone())
    }

    /// Reconciles credential documents and config-derived credentials using the
    /// accepted runtime config. This compatibility adapter cannot publish config.
    /// `files` replaces a scan; its hashes prevent stale credential replacement.
    /// Management callers hold `disk`; the service's required projection adapter
    /// invokes it under its own admission gate, without acquiring `disk`.
    pub(crate) fn publish(&self, cfg: Config, files: Option<credentials::AuthScan>) {
        // Nothing below is applied for a config the runtime would refuse to publish.
        if !Runtime::accepts_config(&cfg) {
            return;
        }
        // Only the service's configuration adapter may change runtime config.
        // Legacy callers pass a scan-time config; use the accepted runtime view.
        let mut cfg = (*self.rt.config()).clone();
        self.lock_auth_dir(&mut cfg);
        use sha2::{Digest, Sha256};
        let scan = files.unwrap_or_else(|| self.scan_credentials(&cfg));
        let mut all = scan.credentials;
        let mut read = scan.read;
        // Uploads record absolute paths; a relative auth-dir scans relative ones.
        let absolute = |p: &PathBuf| std::path::absolute(p).unwrap_or_else(|_| p.clone());
        // Fallbacks follow their file: gone with it, replaced once a synthesizer
        // claims it, otherwise rebuilt from its current content.
        let mut claimed = Vec::new();
        let mut claimed_paths: std::collections::HashSet<_> = all
            .iter()
            .filter_map(|c| match &c.source {
                Source::File(path) => Some(absolute(path)),
                _ => None,
            })
            .collect();
        let mut fallbacks = self.fallbacks.lock().unwrap_or_else(PoisonError::into_inner);
        fallbacks.retain(|path| {
            let Ok(data) = auth_files::read_file(self, path) else {
                return false;
            };
            if claimed_paths.contains(&absolute(path)) {
                claimed.push(path.clone());
                return false;
            }
            match credentials::upload_fallback(&cfg.auth_dir, path, &data) {
                Some(c) => {
                    // Bound to the bytes it was built from, under its own path, so the
                    // freshness check compares this read, not the scan's.
                    if let Source::File(p) = &c.source {
                        read.insert(p.clone(), Sha256::digest(&data).into());
                        claimed_paths.insert(absolute(p));
                    }
                    all.push(c);
                    true
                }
                None => false,
            }
        });
        drop(fallbacks);
        #[cfg(test)]
        {
            let mut hooks = BEFORE_RECONCILE.lock().unwrap();
            let hook = hooks
                .iter()
                .position(|(dir, _)| *dir == cfg.auth_dir)
                .map(|i| hooks.remove(i).1);
            drop(hooks);
            if let Some(hook) = hook {
                hook();
            }
        }
        all.extend(credentials::from_config(&cfg));
        let before: std::collections::HashMap<String, u64> = (self.rt.store().snapshot().iter())
            .map(|c| (c.id.clone(), c.revision))
            .collect();
        let rejected = self.rt.store().reconcile_scanned(all, &read);
        // A document of a type no executor serves stays stored and is listed as an
        // error; warn once when it appears or changes.
        for c in self.rt.store().snapshot().iter() {
            if matches!(c.source, Source::File(_))
                && !self.rt.executors.supports(&c.provider)
                && before.get(&c.id) != Some(&c.revision)
            {
                tracing::warn!(
                    credential = %c.id,
                    provider = %c.provider,
                    "{}: no executor serves it, so it is never selected",
                    auth_files::UNSUPPORTED_TYPE
                );
            }
        }
        // A synthesizer claimed the file but its credential was rejected as stale, so the
        // fallback stays live: keep tracking it, or the next reload would drop it.
        let rejected: std::collections::HashSet<_> = rejected.iter().map(absolute).collect();
        let kept: Vec<PathBuf> = claimed
            .into_iter()
            .filter(|path| rejected.contains(&absolute(path)))
            .collect();
        if !kept.is_empty() {
            self.fallbacks
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .extend(kept);
        }
        // Go's watcher calls redisqueue.NotifyUsageRefresh after every client reload.
        self.rt.usage_queue().notify_usage_refresh();
    }

    /// Reads canonical documents; a failed scan keeps loaded file credentials while
    /// allowing the config itself to take effect.
    pub(crate) fn scan_credentials(&self, cfg: &Config) -> credentials::AuthScan {
        self.rt.store().credential_backend().scan(cfg).unwrap_or_else(|error| {
            tracing::error!("credential database: reading credentials failed, keeping the loaded ones: {error:#}");
            let credentials = self
                .rt
                .store()
                .snapshot()
                .iter()
                .filter(|c| matches!(c.source, Source::File(_)))
                .map(|c| Credential::clone(c))
                .collect();
            credentials::AuthScan {
                credentials,
                ..Default::default()
            }
        })
    }
}

/// `Policy::from(routing)` plus the provider-level rules Go reads from config:
/// `oauth.request-scoped-errors`, sanitized like `SanitizeOAuthRequestScopedErrors`, and
/// cooker `routing.providers`.
pub fn policy(cfg: &Config) -> Policy {
    let mut policy = Policy::from(&cfg.routing);
    policy.providers = std::sync::Arc::new(crate::scheduling::provider_policy::Providers::global(cfg));
    let rules = cfg
        .document
        .get("oauth")
        .and_then(|o| o.get("request-scoped-errors"))
        .and_then(serde_yaml_ng::Value::as_mapping);
    for (channel, list) in rules.into_iter().flatten() {
        let channel = channel.as_str().unwrap_or_default().trim().go_lower();
        let clean: Vec<ErrorRule> = list
            .as_sequence()
            .into_iter()
            .flatten()
            .filter_map(|r| {
                let words = |k: &str| -> Vec<String> {
                    r.get(k)
                        .and_then(serde_yaml_ng::Value::as_sequence)
                        .into_iter()
                        .flatten()
                        .filter_map(|v| v.as_str().map(|s| s.trim().to_owned()))
                        .filter(|s| !s.is_empty())
                        .collect()
                };
                let rule = ErrorRule {
                    status: r.get("status").and_then(serde_yaml_ng::Value::as_i64).unwrap_or(0),
                    r#match: words("match"),
                    match_regex: words("match-regexr"),
                    action: r
                        .get("action")
                        .and_then(serde_yaml_ng::Value::as_str)
                        .unwrap_or_default()
                        .trim()
                        .go_lower(),
                };
                (rule.status > 0
                    && !(rule.r#match.is_empty() && rule.match_regex.is_empty())
                    && !rule.action.is_empty())
                .then_some(rule)
            })
            .collect();
        if !channel.is_empty() && !clean.is_empty() {
            policy.oauth_request_scoped_errors.insert(channel, clean);
        }
    }
    policy
}

/// gin `c.JSON` of a `gin.H` map: Go's `json.Marshal` (sorted keys; `<`, `>`, `&`,
/// U+2028 and U+2029 escaped) with the charset parameter.
pub(crate) fn json(status: StatusCode, value: &Value) -> Response {
    json_body(status, crate::http::gojson::sorted(value))
}

/// gin `c.JSON` of a value that holds Go structs: objects keep their insertion order
/// (struct fields as Go declares them; callers insert map levels sorted, see
/// [`sorted_keys`]), strings escaped like `json.Marshal`.
pub(crate) fn json_ordered(status: StatusCode, value: &Value) -> Response {
    let mut out = Vec::new();
    write_ordered(value, &mut out);
    json_body(status, String::from_utf8(out).expect("JSON text is UTF-8"))
}

fn write_ordered(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Null => out.extend_from_slice(b"null"),
        Value::Bool(b) => out.extend_from_slice(if *b { b"true" } else { b"false" }),
        Value::Number(n) => out.extend_from_slice(n.to_string().as_bytes()),
        Value::String(s) => out.extend(ck_wire::json::quote(s)),
        Value::Array(items) => {
            out.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_ordered(item, out);
            }
            out.push(b']');
        }
        Value::Object(map) => {
            out.push(b'{');
            for (i, (key, item)) in map.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                out.extend(ck_wire::json::quote(key));
                out.push(b':');
                write_ordered(item, out);
            }
            out.push(b'}');
        }
    }
}

/// A copy whose objects all have sorted keys, as Go encodes a `map[string]any`.
pub(crate) fn sorted_keys(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let sorted: std::collections::BTreeMap<&String, Value> =
                map.iter().map(|(k, v)| (k, sorted_keys(v))).collect();
            Value::Object(sorted.into_iter().map(|(k, v)| (k.clone(), v)).collect())
        }
        Value::Array(items) => Value::Array(items.iter().map(sorted_keys).collect()),
        other => other.clone(),
    }
}

fn json_body(status: StatusCode, body: String) -> Response {
    (
        status,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json; charset=utf-8"),
        )],
        body,
    )
        .into_response()
}

pub(crate) fn json_error(status: StatusCode, message: &str) -> Response {
    json(status, &json!({"error": message}))
}

/// Go `HTTPStatusFromErrorOr(err, 400)` for a failed body read in the handlers
/// dd7b4334 changed: 413 at `server.max-request-body-bytes` (and at a route's own
/// `DefaultBodyLimit`), else 400.
pub(crate) fn read_status(rejection: &BytesRejection) -> StatusCode {
    if crate::http::body_limit::is_too_large(rejection) || rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
        StatusCode::PAYLOAD_TOO_LARGE
    } else {
        StatusCode::BAD_REQUEST
    }
}

/// The text of a failed body read: Go's `err.Error()` at the limit.
pub(crate) fn read_reason(rejection: &BytesRejection) -> String {
    crate::inference::claude::read_error(rejection).1
}

fn error(code: u16, message: &str) -> Response {
    json_error(StatusCode::from_u16(code).unwrap(), message)
}

/// Wraps one method handler in the management guard; methods without a handler fall
/// through to [`no_route`] like gin's NoRoute.
macro_rules! guarded {
    ($state:expr, $handler:expr) => {
        $handler.layer(middleware::from_fn_with_state($state.clone(), access::guard))
    };
}

/// Availability only, no key: Go's OAuth callback routes.
macro_rules! open {
    ($state:expr, $handler:expr) => {
        $handler.layer(middleware::from_fn_with_state($state.clone(), access::available))
    };
}

/// gin answers HEAD and unregistered methods on a known path with NoRoute (no `Allow`
/// header; starting from `any` keeps axum from adding one).
fn methods() -> MethodRouter<Arc<Management>> {
    // An explicit HEAD keeps axum from answering HEAD with the GET handler.
    any(no_route).head(no_route)
}

/// gin's NoRoute: a 404, behind the management guard for `/v0/management` paths so a
/// caller without a key cannot tell existing routes from missing ones.
async fn no_route(State(state): State<Arc<Management>>, req: axum::extract::Request) -> Response {
    use tower::ServiceExt as _;
    // gin matches NoRoute prefixes on the decoded path.
    let path = percent_decode(req.uri().path());
    if path != "/v0/management" && !path.starts_with("/v0/management/") {
        return access::not_found();
    }
    let guarded: MethodRouter<(), std::convert::Infallible> = guarded!(state, any(|| async { access::not_found() }));
    match guarded.oneshot(req).await {
        Ok(response) => response,
        Err(never) => match never {},
    }
}

/// The OAuth callback POST needs no key, so unlike Go it keeps a bound: legitimate
/// bodies are a state, a code and a redirect URL.
const CALLBACK_BODY_LIMIT: usize = 64 * 1024;

pub fn router(state: Arc<Management>) -> Router {
    let s = &state;
    let v8 = "/v8/management";
    let v0 = "/v0/management";
    let admin = "/admin/v1";
    let mut router = Router::new()
        .route(&format!("{admin}/r/{{kind}}"), methods().get(guarded!(s, admin::list)))
        .route(
            &format!("{admin}/r/{{kind}}/{{id}}"),
            methods()
                .get(guarded!(s, admin::get))
                .put(guarded!(s, admin::put))
                .patch(guarded!(s, admin::patch))
                .delete(guarded!(s, admin::delete)),
        )
        .route(&format!("{admin}/apply"), methods().post(guarded!(s, admin::apply)))
        .route(
            &format!("{admin}/effective/config"),
            methods().get(guarded!(s, admin::effective_config)),
        )
        .route(
            &format!("{admin}/revisions"),
            methods().get(guarded!(s, admin::revisions)),
        )
        .route(
            &format!("{admin}/revisions/{{rev}}"),
            methods().get(guarded!(s, admin::revision)),
        )
        .route(
            &format!("{admin}/revisions/{{rev}}/restore"),
            methods().post(guarded!(s, admin::restore)),
        )
        .route(&format!("{admin}/export"), methods().get(guarded!(s, admin::export)))
        .route(&format!("{admin}/import"), methods().post(guarded!(s, admin::import)))
        .route(
            &format!("{admin}/credentials"),
            methods().get(guarded!(s, admin::credential_sources)),
        )
        .route(
            &format!("{admin}/credentials/batch"),
            methods().post(guarded!(s, admin::credential_batch)),
        );
    // v8 and its deprecated v0 spellings share Go's handlers.
    for (base, files, definitions) in [
        (v8, "credentials", "routing/model-definitions"),
        (v0, "auth-files", "model-definitions"),
    ] {
        router = router
            .route(
                &format!("{base}/{files}"),
                methods()
                    .get(guarded!(s, auth_files::list))
                    .post(guarded!(s, auth_files::upload))
                    .delete(guarded!(s, auth_files::delete)),
            )
            .route(
                &format!("{base}/{files}/models"),
                methods().get(guarded!(s, auth_files::models)),
            )
            .route(
                &format!("{base}/{files}/download"),
                methods().get(guarded!(s, auth_files::download)),
            )
            .route(
                &format!("{base}/{files}/status"),
                methods().patch(guarded!(s, auth_files::status)),
            )
            .route(
                &format!("{base}/{files}/fields"),
                methods().patch(guarded!(s, auth_files::fields)),
            )
            .route(
                &format!("{base}/{files}/refresh"),
                methods().post(guarded!(s, auth_files::refresh)),
            )
            .route(
                &format!("{base}/{definitions}/{{channel}}"),
                methods().get(guarded!(s, auth_files::model_definitions)),
            );
    }
    for (path, route) in [
        // The dashboard's sign-in: each handler checks its own credential (session.rs).
        (
            "session",
            methods()
                .get(open!(s, session::check))
                .post(open!(s, session::create))
                .delete(open!(s, session::revoke)),
        ),
        (
            "server/latest-version",
            methods().get(guarded!(s, observability::latest_version)),
        ),
        ("requests/api-call", methods().post(guarded!(s, api_call::api_call))),
        (
            "routing/cooldown/reset",
            methods().post(guarded!(s, auth_files::cooldown_reset)),
        ),
        (
            "observability/usage/api-keys",
            methods().get(guarded!(s, observability::api_key_usage)),
        ),
        (
            "observability/usage/queue",
            methods().get(guarded!(s, observability::usage_queue)),
        ),
        ("oauth/auth-url", methods().get(guarded!(s, oauth::auth_url))),
        ("oauth/status", methods().get(guarded!(s, oauth::status))),
        ("oauth/session", methods().delete(guarded!(s, oauth::cancel))),
        ("oauth/import", methods().post(guarded!(s, oauth::import))),
        ("oauth/siwc/bind", methods().post(guarded!(s, siwc::bind))),
        (
            "observability/logs",
            methods()
                .get(guarded!(s, logs::get_logs))
                .delete(guarded!(s, logs::delete_logs)),
        ),
        (
            "observability/logs/errors",
            methods().get(guarded!(s, logs::error_logs)),
        ),
        (
            "observability/logs/errors/{name}",
            methods().get(guarded!(s, logs::download_error_log)),
        ),
        (
            "observability/logs/requests/{id}",
            methods().get(guarded!(s, logs::request_log)),
        ),
        (
            "oauth/callback",
            methods()
                .get(open!(s, oauth::callback_get))
                .post(open!(s, oauth::callback_post).layer(DefaultBodyLimit::max(CALLBACK_BODY_LIMIT))),
        ),
    ] {
        router = router.route(&format!("{v8}/{path}"), route);
    }
    for (path, route) in [
        (
            "latest-version",
            methods().get(guarded!(s, observability::latest_version)),
        ),
        ("api-call", methods().post(guarded!(s, api_call::api_call))),
        ("reset-quota", methods().post(guarded!(s, auth_files::cooldown_reset))),
        (
            "api-key-usage",
            methods().get(guarded!(s, observability::api_key_usage)),
        ),
        ("usage-queue", methods().get(guarded!(s, observability::usage_queue))),
        ("get-auth-status", methods().get(guarded!(s, oauth::status))),
        ("oauth-session", methods().delete(guarded!(s, oauth::cancel))),
        (
            "oauth-callback",
            methods()
                .get(open!(s, oauth::callback_get))
                .post(open!(s, oauth::callback_post).layer(DefaultBodyLimit::max(CALLBACK_BODY_LIMIT))),
        ),
    ] {
        router = router.route(&format!("{v0}/{path}"), route);
    }
    router = router.route(&format!("{v0}/quota/fetch"), methods().post(guarded!(s, quota::fetch)));
    for (path, _) in legacy::AUTH_URL_ROUTES {
        router = router.route(&format!("{v0}/{path}"), methods().get(guarded!(s, legacy::auth_url)));
    }
    router = router
        .route(
            &format!("{v0}/vertex/import"),
            methods().post(guarded!(s, legacy::vertex_import)),
        )
        .route(
            &format!("{v0}/logs"),
            methods()
                .get(guarded!(s, logs::get_logs))
                .delete(guarded!(s, logs::delete_logs)),
        )
        .route(
            &format!("{v0}/request-error-logs"),
            methods().get(guarded!(s, logs::error_logs)),
        )
        .route(
            &format!("{v0}/request-error-logs/{{name}}"),
            methods().get(guarded!(s, logs::download_error_log)),
        )
        .route(
            &format!("{v0}/request-log-by-id/{{id}}"),
            methods().get(guarded!(s, logs::request_log)),
        );
    router
        .fallback(no_route)
        .route("/management.html", get(panel))
        .route("/assets/{*path}", get(panel))
        .route("/fonts/{*path}", get(panel))
        .route("/favicon.svg", get(panel))
        // gin reads management bodies without a size limit (uploads, config writes);
        // they are only read after authentication.
        .layer(DefaultBodyLimit::disable())
        .layer(middleware::from_fn_with_state(state.clone(), access::owned))
        .layer(middleware::from_fn(cors))
        .with_state(state)
}

pub(crate) fn percent_decode(path: &str) -> String {
    let bytes = path.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = bytes
            .get(i + 1..i + 3)
            .and_then(|h| u8::from_str_radix(std::str::from_utf8(h).ok()?, 16).ok());
        match (bytes[i], hex) {
            (b'%', Some(b)) => {
                out.push(b);
                i += 3;
            }
            (b, _) => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Test hook run once by the next [`Management::publish`] for the auth folder it names,
/// after it has built its fallback credentials and before it reconciles them.
#[cfg(test)]
pub(crate) static BEFORE_RECONCILE: Mutex<Vec<(PathBuf, LayoutHook)>> = Mutex::new(Vec::new());

#[cfg(test)]
pub(crate) type LayoutHook = Box<dyn FnOnce() + Send>;

/// Go `liveAuthIndexFromManager`: the index each config-backed credential carries.
pub(super) fn live_auth_indexes(state: &Management) -> std::collections::HashMap<String, String> {
    state
        .rt
        .store()
        .snapshot()
        .iter()
        .filter(|c| matches!(c.source, Source::Config { .. }))
        .map(|c| (c.id.clone(), credentials::auth_index(c)))
        .collect()
}

/// Whether a setting the server reads only at startup differs: the listener address,
/// TLS, trusted proxies, or the configured Tokio worker count when the environment
/// does not override it.
fn startup_changed(before: &Config, after: &Config) -> bool {
    let tls = |c: &Config| c.document.get("server").and_then(|s| s.get("tls")).cloned();
    before.host != after.host
        || before.port != after.port
        || before.trusted_proxies != after.trusted_proxies
        || tls(before) != tls(after)
        || (std::env::var_os("TOKIO_WORKER_THREADS").is_none()
            && before.document.get("worker-threads") != after.document.get("worker-threads"))
}

include!(concat!(env!("OUT_DIR"), "/dashboard.rs"));

fn accepts_gzip(headers: &HeaderMap) -> bool {
    let mut gzip = None;
    let mut wildcard = false;
    for value in headers.get_all(header::ACCEPT_ENCODING) {
        let Ok(value) = value.to_str() else { continue };
        for coding in value.split(',') {
            let mut parts = coding.split(';');
            let name = parts.next().unwrap_or_default().trim();
            if !name.eq_ignore_ascii_case("gzip") && name != "*" {
                continue;
            }
            let quality = parts
                .filter_map(|part| part.split_once('='))
                .find(|(name, _)| name.trim().eq_ignore_ascii_case("q"))
                .map_or(1.0, |(_, value)| value.trim().parse::<f32>().unwrap_or(0.0));
            let accepted = quality > 0.0 && quality <= 1.0;
            if name == "*" {
                wildcard = accepted;
            } else {
                gzip = Some(accepted);
            }
        }
    }
    gzip.unwrap_or(wildcard)
}

async fn panel(State(state): State<Arc<Management>>, OriginalUri(uri): OriginalUri, headers: HeaderMap) -> Response {
    if state.rt.config().management.disable_control_panel {
        return error(404, "not_found");
    }
    let Some((bytes, mime, gzip)) = dashboard_asset(uri.path()) else {
        return error(404, "not_found");
    };
    let encoded = gzip && accepts_gzip(&headers);
    let body = if gzip && !encoded {
        use tokio::io::AsyncReadExt;
        let mut decoded = Vec::new();
        if async_compression::tokio::bufread::GzipDecoder::new(bytes)
            .read_to_end(&mut decoded)
            .await
            .is_err()
        {
            return error(500, "dashboard_decode_failed");
        }
        Bytes::from(decoded)
    } else {
        Bytes::from_static(bytes)
    };
    let mut response = (
        [(header::CONTENT_TYPE, mime), (header::CACHE_CONTROL, "no-cache")],
        body,
    )
        .into_response();
    if gzip {
        response
            .headers_mut()
            .insert(header::VARY, HeaderValue::from_static("Accept-Encoding"));
    }
    if encoded {
        response
            .headers_mut()
            .insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    }
    response
}
