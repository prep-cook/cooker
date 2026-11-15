//! Axum-independent runtime: config snapshot, credential store, selection, credential
//! preparation and attempt outcomes.
//!
//! Every attempt is a [`Lease`]: an owned guard that reports exactly one [`Outcome`],
//! either explicitly or as `Cancelled` when dropped (client gone mid-execute, stream
//! dropped). Streams wrap their lease in [`Completing`].

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::task::{Context, Poll, ready};
use std::time::{Duration, Instant};

use bytes::Bytes;
use ck_core::config::Config;
use ck_core::credential::{Credential, MetadataPatch, Source};
use ck_core::exec::{ExecError, ExecStream, FailureScope};
use ck_exec::{Executors, Readiness};
use ck_wire::gostr::GoStr;
use futures_util::{FutureExt, Stream, StreamExt};
use serde_json::{Map, Value};

use crate::scheduling::refresh::RefreshState;
use crate::scheduling::scheduler::{Policy, Scheduler};
use crate::scheduling::{classify, registry::Registry};

/// The config snapshot and credential epoch a registry was built from.
type RegistryCache = (Arc<Config>, (u64, u64, u64), Arc<Registry>);

/// One runtime configuration publication: the installed view and its place in
/// publication order.
pub struct ConfigChange {
    pub sequence: u64,
    pub config: Arc<Config>,
}

/// A runtime consumer's subscription: the view current at registration, then every
/// later publication in order, each with its own view. Consumers act on these views,
/// never on [`Runtime::config`].
pub struct ConfigChanges {
    current: Arc<ConfigChange>,
    receiver: tokio::sync::mpsc::UnboundedReceiver<Arc<ConfigChange>>,
}

impl ConfigChanges {
    /// The last view delivered to this subscriber (initially the registration view).
    pub fn current(&self) -> &Arc<ConfigChange> {
        &self.current
    }

    /// The next publication, in order; `None` once the runtime is gone.
    pub async fn next(&mut self) -> Option<Arc<ConfigChange>> {
        let change = self.receiver.recv().await?;
        self.current = change.clone();
        Some(change)
    }

    /// The next publication if one is already delivered, in order.
    pub fn try_next(&mut self) -> Option<Arc<ConfigChange>> {
        let change = self.receiver.try_recv().ok()?;
        self.current = change.clone();
        Some(change)
    }
}

/// Lossless, ordered fan-out: one unbounded queue per subscriber, pruned when its
/// receiver is dropped. Registration and delivery share this lock.
// ponytail: no hard bound. A queue holds the publications made while its consumer is
// busy; consumers keep receiving during slow work, and each commit takes a full image
// save. Upgrade path: per-subscriber capacity with commit admission waiting, outside
// the owner's locks, for a full subscriber.
struct ConfigFanout {
    latest: Arc<ConfigChange>,
    /// The view a service install placed, announced by the owner's notification.
    installed: Option<Arc<Config>>,
    subscribers: Vec<tokio::sync::mpsc::UnboundedSender<Arc<ConfigChange>>>,
}

impl ConfigFanout {
    fn new(config: Arc<Config>) -> Self {
        Self {
            latest: Arc::new(ConfigChange { sequence: 0, config }),
            installed: None,
            subscribers: Vec::new(),
        }
    }
}

pub struct Runtime {
    config: RwLock<Arc<Config>>,
    store: Arc<CredentialStore>,
    pub executors: Executors,
    refresh_task: Mutex<Option<tokio::task::AbortHandle>>,
    refresh_state: Mutex<RefreshState>,
    /// The registry derived from the current config and credential generation.
    registry: Mutex<Option<RegistryCache>>,
    oauth_sink: RwLock<Option<OAuthCallbackSink>>,
    /// Go `modelPoolOffsets`: rotation cursors for OpenAI-compatible alias pools.
    pool_offsets: Mutex<HashMap<String, usize>>,
    /// Usage records for `GET /observability/usage/queue` (management configures it).
    usage: Arc<crate::observability::usage::UsageQueue>,
    /// `--local-model`: embedded model catalogs only, no remote catalog refresh.
    local_model: std::sync::atomic::AtomicBool,
    /// Set by test runtime fixtures: executor calls get credentials that
    /// cannot leave the machine.
    pub(crate) deny_external: std::sync::atomic::AtomicBool,
    /// Every config publication, in order, for runtime consumers ([`Runtime::subscribe_config`]).
    config_changes: Mutex<ConfigFanout>,
    /// Marked whenever a background refresh job ends, so the refresh loop looks again
    /// (a job may end after the loop stopped waiting for it).
    refresh_done: tokio::sync::watch::Sender<()>,
    /// Codex refresh transactions in flight ([`Runtime::shutdown_codex_refreshes`]).
    codex_refreshes: CodexRefreshes,
    /// The model catalog updaters' policy, once started (`model_updater::start`).
    catalogs: Mutex<Option<crate::inference::model_updater::Session>>,
    /// Serializes config commits through their catalog reconfigure.
    config_commit: Mutex<()>,
    /// Set under config_commit before registering the persistent owner's view.
    managed_config: std::sync::atomic::AtomicBool,
    /// The model routers' benchmarks and conversation bindings ([`crate::inference::model_router`]).
    pub(crate) model_router: crate::inference::model_router::State,
    /// The stream store, committed stream handles and stream tasks.
    streams: crate::inference::streams::Registry,
}

/// The Codex refresh transactions under a credential file's lock: shutdown stops the
/// ones still waiting for the lock and waits for the ones that hold it, whose grant may
/// have spent a refresh token.
#[derive(Clone, Default)]
struct CodexRefreshes {
    admission_closed: tokio_util::sync::CancellationToken,
    shutdown: tokio_util::sync::CancellationToken,
    tasks: tokio_util::task::TaskTracker,
}

/// An OAuth provider redirect received on the main listener.
#[derive(Debug, Clone)]
pub struct OAuthCallback {
    /// `anthropic`, `codex`, `antigravity` or `devin`.
    pub provider: &'static str,
    pub state: String,
    pub code: String,
    pub error: String,
}

/// What became of a provider callback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OAuthDelivery {
    Delivered,
    /// No login with that state is pending, or the callback could not be recorded.
    NotPending,
    /// The state belongs to a Sign in with ChatGPT binding, which takes its callback
    /// only through the v8 Management API (Go `ErrSIWCCallbackRequiresV8`).
    RequiresV8,
}

/// Hands a callback to a pending management login (Go
/// `WriteOAuthCallbackFileForPendingSession`).
pub type OAuthCallbackSink = Arc<dyn Fn(&OAuthCallback) -> OAuthDelivery + Send + Sync>;

impl Runtime {
    /// The scheduler policy is derived from the config (`routing` plus the OAuth
    /// provider rules), as on every publish.
    pub fn new(
        config: Config,
        credentials: Vec<Credential>,
        executors: Executors,
        backend: Arc<crate::store::credential_store::CredentialDb>,
    ) -> Self {
        Self::with_cooldown_backend(config, credentials, executors, backend.clone(), backend)
    }

    /// Installs a cooldown backend before restoring state, including test backends.
    pub fn with_cooldown_backend(
        config: Config,
        credentials: Vec<Credential>,
        executors: Executors,
        db: Arc<crate::store::credential_store::CredentialDb>,
        backend: Arc<dyn crate::store::cooldown_store::Backend>,
    ) -> Self {
        Self::build(
            config,
            credentials,
            executors,
            db,
            backend,
            #[cfg(any(test, feature = "testing"))]
            None,
        )
    }

    pub(crate) fn build(
        mut config: Config,
        credentials: Vec<Credential>,
        executors: Executors,
        db: Arc<crate::store::credential_store::CredentialDb>,
        backend: Arc<dyn crate::store::cooldown_store::Backend>,
        #[cfg(any(test, feature = "testing"))] clock: Option<Arc<crate::testing::SchedulerClock>>,
    ) -> Self {
        config.auth_dir = db.auth_dir();
        let mut policy = crate::management::api::policy(&config);
        policy.compat_disable_cooling = crate::scheduling::scheduler::compat_cooling(&config);
        for problem in crate::scheduling::provider_policy::Providers::problems(&config, &credentials) {
            tracing::warn!("{problem}");
        }
        let cooldown_dir = cooldown_dir(&config, &policy);
        let (enabled, strict) = signature_cache_config(&config);
        ck_translate::set_antigravity_signature_cache_config(enabled, strict);
        let config = Arc::new(config);
        let rt = Self {
            config_changes: Mutex::new(ConfigFanout::new(config.clone())),
            config: RwLock::new(config),
            store: CredentialStore::new(credentials, db),
            executors,
            refresh_task: Mutex::default(),
            refresh_state: Mutex::default(),
            registry: Mutex::default(),
            oauth_sink: RwLock::default(),
            pool_offsets: Mutex::default(),
            usage: Arc::default(),
            local_model: Default::default(),
            deny_external: Default::default(),
            refresh_done: tokio::sync::watch::Sender::new(()),
            codex_refreshes: CodexRefreshes::default(),
            catalogs: Mutex::default(),
            config_commit: Mutex::default(),
            managed_config: Default::default(),
            model_router: Default::default(),
            streams: Default::default(),
        };
        #[cfg(any(test, feature = "testing"))]
        if let Some(clock) = clock {
            assert!(rt.store.clock.set(clock).is_ok());
        }
        rt.publish_policy(policy);
        rt.store.set_cooldown_backend(backend);
        rt.store.configure_cooldown_store(cooldown_dir);
        // Failed attempts publish Go's error events on the RESP `errors` channel.
        let _ = rt.store.error_events.set(rt.usage.clone());
        let _ = rt
            .store
            .session_closer
            .set(Box::new(rt.executors.credential_session_closer()));
        rt
    }

    /// The stream store and the committed streams a resume attaches to.
    pub fn streams(&self) -> &crate::inference::streams::Registry {
        &self.streams
    }

    /// The usage queue: the request path calls `enqueue` with one serialized Go
    /// `queuedUsageDetail` per upstream request when `accepts()` (additive API).
    pub fn usage_queue(&self) -> &crate::observability::usage::UsageQueue {
        &self.usage
    }

    /// Records `--local-model` (Go `modelCatalogUpdaterPlan`'s `localModel`): remote
    /// model catalog updaters must not start when set (additive API).
    pub fn set_local_model(&self, on: bool) {
        self.local_model.store(on, std::sync::atomic::Ordering::Relaxed);
    }

    /// Replaces the cooldown backend, including for test backends.
    pub fn set_cooldown_backend(&self, backend: Arc<dyn crate::store::cooldown_store::Backend>) {
        self.store.set_cooldown_backend(backend);
    }

    /// Whether `--local-model` was given.
    pub fn local_model(&self) -> bool {
        self.local_model.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The config snapshot to use for one whole request.
    pub fn config(&self) -> Arc<Config> {
        self.config.read().unwrap_or_else(PoisonError::into_inner).clone()
    }

    /// Starts the model catalog updaters for this runtime's lifetime under the commit
    /// mutex, so no commit is configured before or skipped by the startup configure.
    pub(crate) fn start_catalogs(&self, session: crate::inference::model_updater::Session) {
        let commit = self.config_commit.lock().unwrap_or_else(PoisonError::into_inner);
        session.configure(&self.config(), true);
        let previous = self
            .catalogs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .replace(session);
        drop(commit);
        drop(previous);
    }

    /// Whether [`Runtime::publish_config_and_policy`] would commit `config`: Go
    /// `commitConfigUpdate` rejects the whole update over invalid catalog sources.
    /// Callers that apply other state before publishing check this first.
    pub(crate) fn accepts_config(config: &Config) -> bool {
        match ck_core::config::validate_catalog_sources(config.document.get("models")) {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!("rejected invalid model catalog sources: {error}");
                false
            }
        }
    }

    /// The current configuration view and, in order, every later publication with
    /// its own view. Registration and publication share one lock, so no change falls
    /// between the initial view and the first delivery.
    pub fn subscribe_config(&self) -> ConfigChanges {
        let mut fanout = self.config_changes.lock().unwrap_or_else(PoisonError::into_inner);
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        fanout.subscribers.push(sender);
        ConfigChanges {
            current: fanout.latest.clone(),
            receiver,
        }
    }

    /// Delivers one publication to every live subscriber, in publication order.
    fn notify_config(&self, config: Arc<Config>) {
        let mut fanout = self.config_changes.lock().unwrap_or_else(PoisonError::into_inner);
        let change = Arc::new(ConfigChange {
            sequence: fanout.latest.sequence + 1,
            config,
        });
        fanout.latest = change.clone();
        fanout.subscribers.retain(|sender| sender.send(change.clone()).is_ok());
    }

    /// Replaces standalone configuration and its derived scheduler policy.
    /// Once Management binds a persistent owner this volatile port returns false,
    /// without effects: submit a configuration command to that owner instead.
    /// Requests already running retain their snapshot.
    pub fn publish_config(&self, config: Config) -> bool {
        let policy = crate::management::api::policy(&config);
        self.publish_config_and_policy(config, policy)
    }

    /// Standalone counterpart accepting parsed routing settings. Returns false
    /// without effects for invalid config or a runtime bound to a persistent owner.
    pub fn publish_config_and_policy(&self, config: Config, policy: Policy) -> bool {
        self.install_config_and_policy(config, policy, None)
    }

    pub(crate) fn bind_configuration(&self, install: impl FnOnce()) {
        struct Binding<'a> {
            runtime: &'a Runtime,
            previous: bool,
            installed: bool,
        }
        impl Drop for Binding<'_> {
            fn drop(&mut self) {
                if !self.installed {
                    let _commit = self
                        .runtime
                        .config_commit
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner);
                    self.runtime
                        .managed_config
                        .store(self.previous, std::sync::atomic::Ordering::Release);
                }
            }
        }
        let previous = {
            // Fence volatile ports before initial installation, but undo binding
            // if the required adapter unwinds during registration.
            let _commit = self.config_commit.lock().unwrap_or_else(PoisonError::into_inner);
            self.managed_config.swap(true, std::sync::atomic::Ordering::AcqRel)
        };
        let mut binding = Binding {
            runtime: self,
            previous,
            installed: false,
        };
        install();
        binding.installed = true;
    }

    pub(crate) fn install_service_configuration(&self, config: Config, policy: Policy, previous: Arc<Config>) -> bool {
        self.install_config_and_policy(config, policy, Some(previous))
    }

    fn install_config_and_policy(
        &self,
        config: Config,
        mut policy: Policy,
        transition_from: Option<Arc<Config>>,
    ) -> bool {
        let service = transition_from.is_some();
        let commit = self.config_commit.lock().unwrap_or_else(PoisonError::into_inner);
        if !service && self.managed_config.load(std::sync::atomic::Ordering::Acquire) {
            return false;
        }
        if !Self::accepts_config(&config) {
            return false;
        }
        policy.compat_disable_cooling = crate::scheduling::scheduler::compat_cooling(&config);
        for problem in crate::scheduling::provider_policy::Providers::problems(&config, &self.store.snapshot()) {
            tracing::warn!("{problem}");
        }
        let (enabled, strict) = signature_cache_config(&config);
        ck_translate::set_antigravity_signature_cache_config(enabled, strict);
        let dir = cooldown_dir(&config, &policy);
        let ws_auth = crate::realtime::aistudio_relay::ws_auth(&config);
        let config = Arc::new(config);
        // Commits are serialized, so the catalogs are configured in commit order; the
        // config lock itself is released first, as Go releases cfgMu before
        // `UpdateModelCatalogSources`, so readers never wait on a catalog publish.
        let previous = {
            let mut current = self.config.write().unwrap_or_else(PoisonError::into_inner);
            let previous = std::mem::replace(&mut *current, config.clone());
            self.publish_policy(policy);
            previous
        };
        let previous = transition_from.unwrap_or(previous);
        #[cfg(test)]
        crate::testing::installation_effects::run(
            &config.auth_dir,
            crate::testing::installation_effects::Stage::RuntimeDerivedTail,
        )
        .expect("private derived-install fault");
        // Go `UpdateModelCatalogSources`; the reads stay async.
        if let Some(catalogs) = self.catalogs.lock().unwrap_or_else(PoisonError::into_inner).as_ref() {
            catalogs.configure(&config, false);
        }
        // Go's websocket auth change handler: turning ws-auth on ends the relay sessions
        // that connected without a key.
        if ws_auth && !crate::realtime::aistudio_relay::ws_auth(&previous) {
            self.executors.google.aistudio.relay.stop();
        }
        self.store.configure_cooldown_store(dir);
        if service {
            // Announced only by the owner's ordinary notification, after every
            // required adapter installed this commit.
            self.config_changes
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .installed = Some(config);
        } else {
            self.notify_config(config);
        }
        drop(commit);
        true
    }

    /// The owner's ordinary notification for a committed view: delivers the view
    /// that commit's service install placed.
    pub(crate) fn service_configuration_installed(&self) {
        let installed = self
            .config_changes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .installed
            .take();
        if let Some(config) = installed {
            self.notify_config(config);
        }
    }

    /// A coherent config/policy pair for the entire route attempt loop.
    pub fn request_snapshot(&self) -> (Arc<Config>, Arc<Policy>) {
        let config = self.config.read().unwrap_or_else(PoisonError::into_inner);
        (config.clone(), self.policy())
    }

    pub fn store(&self) -> &Arc<CredentialStore> {
        &self.store
    }

    /// Go `nextModelPoolOffset`: the start index for the next pass over a pool.
    pub fn next_pool_offset(&self, key: &str, size: usize) -> usize {
        if size <= 1 || key.trim().is_empty() {
            return 0;
        }
        let key = key.trim();
        let mut offsets = self.pool_offsets.lock().unwrap_or_else(PoisonError::into_inner);
        // ponytail: Go never prunes, so keys of removed credentials would accumulate
        // forever. Past 4096 keys the map is cleared, which restarts rotation for every
        // pool from offset 0. Upgrade path: drop a pool's key when its credential set
        // changes in reconcile.
        if offsets.len() >= 4096 && !offsets.contains_key(key) {
            offsets.clear();
        }
        let slot = offsets.entry(key.to_owned()).or_default();
        let offset = if *slot >= 2_147_483_640 { 0 } else { *slot };
        *slot = offset + 1;
        offset % size
    }

    /// One credential's projected state for one registered model (Go
    /// `clientModelProjectionForAuth`), for listings and `auto`.
    pub fn suspension(&self, credential_id: &str, model: &str) -> crate::scheduling::registry::Suspension {
        use crate::scheduling::registry::Suspension;
        let Some(credential) = self.store.get(credential_id) else {
            return Suspension::None;
        };
        let aliases = crate::scheduling::registry::global_aliases(&self.config());
        let key = crate::scheduling::registry::selection_model(&aliases, &credential, model);
        self.store.suspension(&credential, &key)
    }

    /// Wires OAuth callback delivery (management owns pending login sessions).
    pub fn set_oauth_callback_sink(&self, sink: Option<OAuthCallbackSink>) {
        *self.oauth_sink.write().unwrap_or_else(PoisonError::into_inner) = sink;
    }

    pub fn deliver_oauth_callback(&self, callback: &OAuthCallback) -> OAuthDelivery {
        let sink = self.oauth_sink.read().unwrap_or_else(PoisonError::into_inner).clone();
        sink.map_or(OAuthDelivery::NotPending, |sink| sink(callback))
    }

    /// The credential an executor call receives: the stored one, or in a test runtime
    /// ([`crate::testing`]) a copy that cannot leave the machine.
    pub(crate) fn for_executor(&self, credential: &Arc<Credential>) -> Arc<Credential> {
        #[cfg(any(test, feature = "testing"))]
        if self.deny_external.load(std::sync::atomic::Ordering::Relaxed) {
            return crate::testing::guarded(credential, &self.config());
        }
        credential.clone()
    }

    /// The model registry for the current config and credential set. Rebuilt only when
    /// either changed, so it is always derived, never separately maintained.
    pub fn registry(&self) -> Arc<Registry> {
        let config = self.config();
        // A refreshed static catalog re-registers every credential (Go's model refresh
        // callback), so the catalog generation is part of the key.
        // Capability probes (Antigravity web search) refine registered models later.
        let generation = (
            self.store.epoch(),
            ck_core::registry::catalog_generation(),
            ck_core::registry::dynamic::capabilities_generation(),
        );
        let mut cache = self.registry.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((cfg, g, registry)) = cache.as_ref()
            && Arc::ptr_eq(cfg, &config)
            && *g == generation
        {
            return registry.clone();
        }
        let snapshot = self.store.snapshot();
        let registry = Arc::new(Registry::build(&config, &snapshot));
        let credentials: Vec<_> = snapshot.iter().map(|c| self.for_executor(c)).collect();
        self.executors.models_registered(&credentials, &config);
        *cache = Some((config, generation, registry.clone()));
        registry
    }

    pub fn policy(&self) -> Arc<Policy> {
        self.store.policy.read().unwrap_or_else(PoisonError::into_inner).clone()
    }

    pub fn publish_policy(&self, policy: Policy) {
        {
            let mut current = self.store.policy.write().unwrap_or_else(PoisonError::into_inner);
            self.store
                .scheduler
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .configure(&current, &policy);
            *current = Arc::new(policy);
        }
        self.store.clear_disabled_cooldowns();
    }

    /// Selects a credential that registered the route model. Waits for preparation only
    /// when the executor reports [`Readiness::PrepareNow`] (single-flighted per
    /// credential); token refreshes stay with the background loop. The returned lease
    /// holds the committed snapshot.
    pub async fn acquire(
        &self,
        selection: Selection,
        cfg: &Config,
        policy: Arc<Policy>,
        registry: &Registry,
    ) -> Result<Lease, AcquireError> {
        let aliases = crate::scheduling::registry::global_aliases(cfg);
        let scope = selection.clone();
        let admit = admission(registry, &aliases, &scope, &self.executors);
        // `soonest-reset` ranks by the quota windows the executors last observed.
        let now = std::time::SystemTime::now();
        let ranks = |c: &Credential| {
            let provider = c.provider.trim().to_ascii_lowercase();
            let snapshot = match provider.as_str() {
                "claude" => self.executors.claude.quota().snapshot(&c.id),
                "codex" => self.executors.codex.quota().snapshot(&c.id),
                _ => None,
            };
            snapshot.map_or_else(crate::scheduling::scheduler::Windows::default, |s| {
                crate::scheduling::scheduler::Windows::observed(&provider, &s.signals, s.observed_at, now)
            })
        };
        let mut lease = self.store.select_ranked(selection, policy, &admit, &ranks)?;
        if self.executors.readiness(&lease.credential, cfg) != Readiness::PrepareNow {
            return Ok(lease);
        }
        let id = lease.credential.id.clone();
        match self.prepare_credential(&id, cfg, None).await {
            Ok(current) => {
                lease.credential = current;
                // Go stamps the dispatch after preparing the credential.
                lease.redispatch();
            }
            Err(error) => {
                lease.complete(Outcome::Failure(error.clone()));
                return Err(AcquireError::Prepare { id, error });
            }
        }
        Ok(lease)
    }

    /// A lease on credential `id` outside selection, prepared like [`Self::acquire`]
    /// (Go's credits fallback, `findAllAntigravityCreditsCandidateAuths` then
    /// `prepareRequestAuth`). `None` when the credential is gone or preparation fails;
    /// Go records no result for either.
    pub(crate) async fn lease_credential(
        &self,
        id: &str,
        selection: Selection,
        cfg: &Config,
        prepare: impl FnOnce(&Credential) -> Credential,
    ) -> Option<Lease> {
        let mut credential = self.store.get(id).filter(|c| !self.store.tokens_rejected(c))?;
        if self.executors.readiness(&credential, cfg) == Readiness::PrepareNow {
            credential = self.prepare_credential(id, cfg, None).await.ok()?;
        }
        // Outside selection: the lease answers no bounded window or probe reservation.
        let (picked, seq) = {
            let mut scheduler = self.store.scheduler.lock().unwrap_or_else(PoisonError::into_inner);
            (scheduler.outside_selection_generation(), scheduler.dispatch())
        };
        Some(Lease {
            store: self.store.clone(),
            credential: Arc::new(prepare(&credential)),
            execution_model: selection.model.clone(),
            selection,
            attempt: self.store.attempts.fetch_add(1, Ordering::Relaxed),
            policy: self.policy(),
            reported: false,
            picked,
            seq,
            lcp: None,
        })
    }

    /// At shutdown: Codex refreshes still waiting for a credential file's lock (another
    /// process holds it) give up, and those that hold it finish, so new tokens a grant
    /// returned are written before the process exits. Later refreshes fail.
    pub fn begin_shutdown(&self) {
        self.codex_refreshes.admission_closed.cancel();
        self.stop_auto_refresh();
        self.streams.begin_shutdown();
    }

    pub async fn shutdown_codex_refreshes(&self) {
        if let Err(error) = self.finish_shutdown().await {
            tracing::error!("{error:#}");
        }
    }

    pub async fn finish_shutdown(&self) -> anyhow::Result<()> {
        self.begin_shutdown();
        let streams = self.streams.finish_shutdown().await;
        self.codex_refreshes.shutdown.cancel();
        self.codex_refreshes.tasks.close();
        self.codex_refreshes.tasks.wait().await;
        self.store.persistence_tasks.close();
        self.store.persistence_tasks.wait().await;
        let store = self.store.clone();
        let settled = self
            .store
            .persistence_tasks
            .spawn_blocking(move || {
                let admission = store.credential_backend().admit_work();
                // Stop and join the coalescing thread outside its registry lock.
                let cooldown = store
                    .cooldown_write
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take();
                let cooldown = cooldown.map_or(Ok(()), crate::store::cooldown_store::Writer::stop);
                admission?;
                store.credential_backend().admit_work()?;
                cooldown.map_err(|error| anyhow::anyhow!("final cooldown checkpoint failed: {error}"))?;
                Ok::<_, anyhow::Error>(())
            })
            .await;
        // A recovered required projection can queue normalization during settlement.
        self.store.persistence_tasks.wait().await;
        match settled {
            Ok(Ok(())) => streams.map_err(|error| anyhow::anyhow!("stream storage shutdown failed: {error}")),
            // An unresolved publication keeps its recovery workspace; a lost cooldown
            // checkpoint alone has nothing left to recover.
            Ok(Err(error)) => Err(error.context("configuration shutdown incomplete")),
            Err(_) => anyhow::bail!("configuration shutdown incomplete"),
        }
    }

    /// The credential lock of the Codex file at `path`, for a write that is not a refresh
    /// (a management save), waited for as a refresh waits: shutdown stops the wait.
    pub(crate) async fn lock_codex_file(&self, path: &Path) -> Result<std::fs::File, String> {
        wait_codex_lock(path, &self.codex_refreshes.shutdown).await
    }

    /// Prepares and commits one credential. `failed` is the revision whose token an
    /// upstream rejected: preparation then runs even when not due, unless another task
    /// already replaced that revision (Go `refreshAuthForRequest`).
    async fn prepare_credential(
        &self,
        id: &str,
        cfg: &Config,
        failed: Option<u64>,
    ) -> Result<Arc<Credential>, ExecError> {
        self.prepare_credential_with(id, cfg, failed, false, None).await
    }

    /// [`Self::prepare_credential`]; `management` marks a management refresh, which
    /// refreshes every Codex token partition (Go `IsForceRefreshContext`). `refused` is
    /// the credential as an upstream refused it with a 401: a refresh that then fails with
    /// invalid_grant records it ([`CredentialStore::reject_refused`]) before the
    /// preparation lock is released.
    async fn prepare_credential_with(
        &self,
        id: &str,
        cfg: &Config,
        failed: Option<u64>,
        management: bool,
        refused: Option<&Credential>,
    ) -> Result<Arc<Credential>, ExecError> {
        if self.codex_refreshes.admission_closed.is_cancelled() {
            return Err(ExecError::local(
                503,
                FailureScope::Request,
                "credential preparation is shutting down",
            ));
        }
        test_hook("prepare", id);
        // Owned: the commit below moves it into its blocking job, which a cancelled caller
        // cannot stop, so the lock covers the read through the commit either way.
        let guard = self.store.prepare_lock(id).lock_owned().await;
        if self.codex_refreshes.admission_closed.is_cancelled() {
            return Err(ExecError::local(
                503,
                FailureScope::Request,
                "credential preparation is shutting down",
            ));
        }
        let backend = self.store.credential_backend();
        self.store
            .persistence_tasks
            .spawn_blocking(move || backend.admit_work())
            .await
            .map_err(|e| ExecError::local(500, FailureScope::Request, e.to_string()))?
            .map_err(|e| ExecError::local(500, FailureScope::Request, format!("pending credential recovery: {e}")))?;
        if self.codex_refreshes.admission_closed.is_cancelled() {
            return Err(ExecError::local(
                503,
                FailureScope::Request,
                "credential preparation is shutting down",
            ));
        }
        // Another request may have prepared it while we waited. The registration it is
        // current under is what the refresh commits under.
        let (current, registered) = self
            .store
            .get_registered(id)
            .filter(|(c, _)| !c.disabled)
            .ok_or_else(|| {
                ExecError::local(
                    409,
                    FailureScope::Request,
                    "credential removed or disabled during preparation",
                )
            })?;
        // Go `refreshAuthForRequestAtEpoch` under its refresh lock: tokens refused for good
        // are not refreshed again, except by a management refresh.
        if !management && self.store.tokens_refused(&current) {
            return Err(ExecError::local(
                401,
                FailureScope::Request,
                "credential tokens were refused; sign in again",
            ));
        }
        match failed {
            Some(revision) if current.revision != revision && !self.store.grant_blocks(&current) => return Ok(current),
            Some(_) => {}
            None if self.executors.readiness(&current, cfg) == Readiness::Ready
                && !self.store.grant_blocks(&current) =>
            {
                return Ok(current);
            }
            None => {}
        }
        let refused = refused.map(|c| Arc::new(c.clone()));
        let view = self.for_executor(&current);
        // Go `CodexExecutor.Refresh` on a Codex credential file: one transaction, under
        // the file's credential lock, owned apart from the caller.
        if let Source::File(path) = &current.source
            && current.provider == "codex"
        {
            let mode = match failed {
                Some(_) if management => ck_exec::codex_refresh::RefreshMode::Management,
                Some(_) => ck_exec::codex_refresh::RefreshMode::Rejected,
                None => ck_exec::codex_refresh::RefreshMode::Due,
            };
            let refresher = self.executors.codex.refresher(&view, cfg);
            let file = path.clone();
            return refresh_codex_file(
                &self.codex_refreshes,
                self.store.clone(),
                refresher,
                guard,
                current,
                registered,
                view,
                file,
                mode,
                refused,
            )
            .await;
        }
        // Every locally owned SQLite document shares the same cross-process lock.
        // Codex above owns its lock in its cancellation-safe transaction instead.
        let document_lock = match &current.source {
            Source::File(path) => {
                let backend = self.store.credential_backend();
                let lock = self.lock_codex_file(path).await.map_err(|e| {
                    ExecError::local(500, FailureScope::Request, format!("locking credential document: {e}"))
                })?;
                let store = self.store.clone();
                let cfg = cfg.clone();
                let original = current.clone();
                let locked = self
                    .store
                    .persistence_tasks
                    .spawn_blocking(move || {
                        store
                            .restore_refresh_document(&backend, &original, registered, &cfg)
                            .map(|fresh| (lock, fresh))
                    })
                    .await
                    .map_err(|e| PatchError::Io(e.to_string()))
                    .and_then(|r| r)
                    .map_err(|e| {
                        ExecError::local(
                            500,
                            FailureScope::Request,
                            format!("reading canonical credential: {e:?}"),
                        )
                    })?;
                Some(locked)
            }
            _ => None,
        };
        let original_access = ck_core::config::credentials::access_token_sha256(&current);
        let (document_lock, (current, registered)) = match document_lock {
            Some((lock, fresh)) => (Some(lock), fresh),
            None => (None, (current, registered)),
        };
        if current.disabled {
            return Err(ExecError::local(
                409,
                FailureScope::Request,
                "credential disabled during preparation",
            ));
        }
        if document_lock.is_some()
            && ((!management
                && failed.is_some()
                && original_access != ck_core::config::credentials::access_token_sha256(&current))
                || (failed.is_none() && self.executors.readiness(&current, cfg) == Readiness::Ready))
        {
            return Ok(current);
        }
        let view = self.for_executor(&current);
        let prepared = match failed {
            Some(_) if management => self.executors.prepare_management(&view, cfg).await,
            Some(_) => self.executors.prepare_forced(&view, cfg).await,
            None => self.executors.prepare(&view, cfg).await,
        };
        // Still under the preparation lock, so a refresh queued behind this one sees it.
        let patch = prepared.inspect_err(|e| {
            if let Some(refused) = &refused {
                self.store.reject_refused(refused, registered.registration, e);
            }
        })?;
        let store = self.store.clone();
        let fence = RefreshFence { registered };
        let committed = self
            .store
            .persistence_tasks
            .spawn_blocking({
                let id = id.to_owned();
                move || {
                    let _guard = guard;
                    let _document_lock = document_lock;
                    commit_refresh(&store, &id, &current, fence, &patch)
                }
            })
            .await
            .map_err(|e| PatchError::Io(e.to_string()))
            .and_then(|r| r)
            .map_err(|e| {
                ExecError::local(
                    500,
                    FailureScope::Request,
                    format!("committing prepared credential: {e:?}"),
                )
            })?;
        Ok(committed)
    }

    /// Go `tryRefreshAfterUnauthorized`: after an upstream 401 on a credential whose
    /// executor reports a refresh credential, prepare it once (due or not) and return the
    /// committed replacement to retry with.
    // ponytail: refresh goes through `Executors::prepare_forced`: Codex refreshes when not
    // due, and Claude's prepare already refreshed on demand before 50d5fe18. A provider
    // whose prepare refreshes only inside its lead cannot recover a revoked token here.
    pub async fn refresh_after_unauthorized(&self, credential: &Credential, cfg: &Config) -> Option<Arc<Credential>> {
        if !self.executors.has_refresh_credential(credential) {
            return None;
        }
        let refreshed = match self
            .prepare_credential_with(&credential.id, cfg, Some(credential.revision), false, Some(credential))
            .await
        {
            Ok(refreshed) => refreshed,
            Err(error) => {
                // Go `refreshAuthForRequestAtEpoch`: neither token can recover without a
                // new sign-in when the refresh token is refused while the access token
                // the upstream refused is still current. The credential is neither
                // selected nor refreshed again until its tokens change. Preparation
                // recorded that, or refused because another 401 had.
                if self.store.tokens_rejected(credential) {
                    if !classify::is_invalid_grant(error.status, &String::from_utf8_lossy(&error.body)) {
                        return None;
                    }
                    tracing::warn!(
                        "credential refresh failed for {} ({}) after an upstream 401: status {}: {}; it is not used again until it is signed in again or its file changes",
                        credential.provider,
                        credential.id,
                        error.status,
                        refresh_error_text(&error)
                    );
                    return None;
                }
                // The background loop's schedule is the next attempt, unless another 401
                // comes first.
                let next = self
                    .refresh_state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .retry_at(credential)
                    .map(|at| at.saturating_duration_since(Instant::now()));
                warn_refresh_failed(credential, " after an upstream 401", &error, next);
                return None;
            }
        };
        (refreshed.revision != credential.revision).then(|| {
            tracing::info!(
                "credential refreshed for {} ({}) after an upstream 401",
                credential.provider,
                credential.id
            );
            refreshed
        })
    }

    /// One replaceable refresh loop. Uses the same preparation lock and atomic
    /// revision-checked commit as request acquisition; executors never persist files.
    /// ponytail: needs_prepare controls eligibility. Unauthorized lifecycle gating
    /// and end-to-end token rotation await executor/lifecycle integration (M4-0027).
    pub fn start_auto_refresh(self: &Arc<Self>) {
        let mut task = self.refresh_task.lock().unwrap_or_else(PoisonError::into_inner);
        if self.codex_refreshes.admission_closed.is_cancelled() {
            return;
        }
        if let Some(previous) = task.take() {
            previous.abort();
        }
        let weak = Arc::downgrade(self);
        // Only shutdown (the runtime dropped) or an abort ends the loop: a panic anywhere
        // in it is logged and the loop starts again, with fresh subscriptions.
        let handle = self.codex_refreshes.tasks.spawn(async move {
            loop {
                match AssertUnwindSafe(refresh_loop(weak.clone())).catch_unwind().await {
                    Ok(()) => break,
                    Err(panic) => {
                        tracing::error!(
                            "credential auto-refresh loop panicked ({}); restarting it in {}s",
                            panic_text(&*panic),
                            REFRESH_LOOP_RESTART.as_secs()
                        );
                        tokio::time::sleep(REFRESH_LOOP_RESTART).await;
                    }
                }
            }
        });
        *task = Some(handle.abort_handle());
    }

    pub fn stop_auto_refresh(&self) {
        if let Some(task) = self.refresh_task.lock().unwrap_or_else(PoisonError::into_inner).take() {
            task.abort();
        }
    }
}

/// The body of [`Runtime::start_auto_refresh`]'s task. Returns when the runtime is gone.
async fn refresh_loop(weak: std::sync::Weak<Runtime>) {
    // Wakes at the next deadline, or at the latest after `HORIZON` of awake time while
    // a credential can ever come due, to catch wall-clock jumps such as a resumed
    // laptop (monotonic time stops during suspend). Credentials that never need
    // preparation (API keys, Gemini CLI tokens) set no timer.
    const HORIZON: Duration = Duration::from_secs(600);
    // Store changes (each committed refresh is one) are coalesced over this delay.
    const COALESCE: Duration = Duration::from_secs(1);
    let Some(rt) = weak.upgrade() else {
        return;
    };
    let mut store_changes = rt.store.subscribe();
    let mut config_changes = rt.subscribe_config();
    let mut unauthorized = rt.store.subscribe_unauthorized();
    let mut jobs_ended = rt.refresh_done.subscribe();
    // The worker bound (`oauth.auth-auto-refresh-workers`), taken inside each job.
    let mut workers = (0, Arc::new(tokio::sync::Semaphore::new(0)));
    drop(rt);
    loop {
        let Some(rt) = weak.upgrade() else {
            break;
        };
        // Everything published so far is in this scan, including the commits of
        // the previous batch. Configuration views already delivered are taken in
        // order; the scan uses the newest of them.
        store_changes.borrow_and_update();
        while config_changes.try_next().is_some() {}
        unauthorized.borrow_and_update();
        jobs_ended.borrow_and_update();
        let cfg = config_changes.current().config.clone();
        // A soft bound while the setting changes: jobs holding the old semaphore's
        // permits run beside the new one's.
        if workers.0 != refresh_workers(&cfg) {
            workers = (
                refresh_workers(&cfg),
                Arc::new(tokio::sync::Semaphore::new(refresh_workers(&cfg))),
            );
        }
        // Each reservation is handed to its spawned job at once, so whatever ends this
        // loop (an abort, a panic later in the scan), the job ends the reservation.
        let spawn = |c: &Arc<Credential>, rejected: bool| {
            let job = run_refresh_job(rt.clone(), cfg.clone(), workers.1.clone(), c.clone(), rejected);
            rt.codex_refreshes.tasks.spawn(job)
        };
        // Before the snapshot, so a 401 on a revision newer than it stays.
        let rejected = rt.store.unauthorized();
        let snapshot = rt.store.snapshot();
        let (now, wall) = (Instant::now(), chrono::Utc::now());
        let horizon = chrono::Duration::from_std(HORIZON).unwrap_or_default();
        let mut wake: Option<Instant> = None;
        let mut soonest = |at: Instant| wake = Some(wake.map_or(at, |w| w.min(at)));
        let jobs: Vec<_> = {
            let mut state = rt.refresh_state.lock().unwrap_or_else(PoisonError::into_inner);
            state.reconcile(&snapshot);
            let mut jobs = Vec::new();
            rt.store.forget_stale_unauthorized(&rejected, &snapshot);
            // Far enough ahead that any expiry has passed, near enough for chrono.
            let eventually = wall + chrono::Duration::days(36_500);
            // Go `shouldRefresh`: tokens refused for good wait for new ones.
            let refused = rt.store.rejected_ids(&snapshot);
            for c in snapshot.iter().filter(|c| !refused.contains(&c.id)) {
                if !refresh_candidate(c) {
                    // Go refreshes a disabled credential; here its due refresh waits until
                    // it is enabled, logged once per revision, when it comes due.
                    if c.disabled && refreshable(c) && !state.disabled_noted(c) {
                        if rt.executors.needs_prepare_at(c, &cfg, eventually) {
                            soonest(now + HORIZON);
                        }
                        let due = rt.executors.prepare_due(c, &cfg, wall, horizon);
                        match due {
                            Some(at) if at > wall => soonest(now + (at - wall).to_std().unwrap_or_default()),
                            Some(_) => {
                                state.note_disabled(c);
                                tracing::info!(
                                    "credential refresh for {} ({}) is due but the credential is disabled; it is refreshed once it is enabled",
                                    c.provider,
                                    c.id
                                );
                            }
                            None => {}
                        }
                    }
                    continue;
                }
                test_hook("scan", &c.id);
                // Go `scheduleUnauthorizedRefresh`: a rejected token refreshes now,
                // or after `refreshFailureBackoff` when it was refreshed just before.
                // Its deadline replaces the ordinary one (Go `NextRefreshAfter`).
                if let Some((revision, noted)) = rejected.pending.get(&c.id)
                    && *revision == c.revision
                    && rt.executors.has_refresh_credential(c)
                {
                    let at = rejected
                        .refreshed
                        .get(&c.id)
                        .filter(|(r, t)| {
                            *r <= *revision && noted.saturating_duration_since(*t) < REFRESH_FAILURE_BACKOFF
                        })
                        .map_or(*noted, |_| *noted + REFRESH_FAILURE_BACKOFF);
                    if at > now {
                        soonest(at);
                    } else if state.reserve(c, now) {
                        jobs.push(spawn(c, true));
                    } else {
                        warn_refresh_held(&mut state, c);
                        if let Some(at) = state.retry_at(c).filter(|at| *at > now) {
                            soonest(at);
                        }
                    }
                    continue;
                }
                if rt.executors.needs_prepare_at(c, &cfg, eventually) {
                    soonest(now + HORIZON);
                }
                let due = rt.executors.prepare_due(c, &cfg, wall, horizon);
                match due {
                    Some(at) if at > wall => soonest(now + (at - wall).to_std().unwrap_or_default()),
                    Some(_) if state.reserve(c, now) => jobs.push(spawn(c, false)),
                    // A job still running, or backing off: look again when that ends.
                    Some(_) => {
                        warn_refresh_held(&mut state, c);
                        if let Some(at) = state.retry_at(c).filter(|at| *at > now) {
                            soonest(at);
                        }
                    }
                    None => {}
                }
            }
            jobs
        };
        let busy = !jobs.is_empty();
        // The loop waits for the batch, at most `REFRESH_JOB_BOUND` per job, then looks
        // again. A job it stops waiting for runs on, records its own outcome and wakes the
        // loop when it ends.
        futures_util::future::join_all(jobs.into_iter().map(|job| async move {
            let _ = tokio::time::timeout(REFRESH_JOB_BOUND, job).await;
        }))
        .await;
        drop(rt);
        // After a batch, look again at once: finished jobs set their backoff,
        // and the next scan turns it into a deadline.
        if busy {
            continue;
        }
        let deadline = async {
            match wake {
                Some(at) => tokio::time::sleep_until(at.into()).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            () = deadline => {}
            changed = store_changes.changed() => {
                if changed.is_err() {
                    break;
                }
                tokio::time::sleep(COALESCE).await;
            }
            changed = config_changes.next() => {
                if changed.is_none() {
                    break;
                }
                tokio::time::sleep(COALESCE).await;
            }
            changed = unauthorized.changed() => {
                if changed.is_err() {
                    break;
                }
            }
            changed = jobs_ended.changed() => {
                if changed.is_err() {
                    break;
                }
                tokio::time::sleep(COALESCE).await;
            }
        }
    }
}

/// One background refresh job of [`refresh_loop`], which reserved `credential`: prepares
/// it, then records the outcome in the refresh state, which ends the reservation, and
/// logs it. The reservation ends exactly once on every path: a panic while preparing is
/// a failure, and [`Reservation`]'s drop covers the rest. The exception is a job dropped
/// before its first poll at runtime teardown, whose reservation goes with the runtime.
// A job past the loop's wait bound keeps its reservation and owned tracker token.
// Stopping the loop does not abort a possibly consumed grant; explicit shutdown
// waits for the same job, including its persistence, before workspace cleanup.
async fn run_refresh_job(
    rt: Arc<Runtime>,
    cfg: Arc<Config>,
    workers: Arc<tokio::sync::Semaphore>,
    credential: Arc<Credential>,
    rejected: bool,
) {
    let mut reservation = Reservation {
        rt: rt.clone(),
        credential: credential.clone(),
        ended: false,
    };
    let worker = tokio::select! {
        biased;
        () = rt.codex_refreshes.shutdown.cancelled() => return,
        permit = workers.acquire_owned() => permit,
    };
    let failed = rejected.then_some(credential.revision);
    // A 401 refresh records a refused token as the inline one does.
    let refused = rejected.then_some(&*credential);
    let prepare =
        AssertUnwindSafe(rt.prepare_credential_with(&credential.id, &cfg, failed, false, refused)).catch_unwind();
    tokio::pin!(prepare);
    // Past the bound, counted from when the job got its worker, the worker goes to the
    // next job, and this one awaits the same preparation, never cancelled, still
    // holding its reservation.
    let prepared = match tokio::time::timeout(REFRESH_JOB_BOUND, &mut prepare).await {
        Ok(prepared) => prepared,
        Err(_) => {
            tracing::warn!(
                "credential refresh for {} ({}) still running after {}s; no other refresh of it starts until it ends",
                credential.provider,
                credential.id,
                REFRESH_JOB_BOUND.as_secs()
            );
            drop(worker);
            prepare.await
        }
    };
    let result = prepared.unwrap_or_else(|panic| {
        let text = format!("refresh panicked: {}", panic_text(&*panic));
        Err(ExecError::local(500, FailureScope::Request, text))
    });
    if rejected && result.is_ok() {
        rt.store.forget_unauthorized(&credential.id, credential.revision);
    }
    let trigger = if rejected { " after an upstream 401" } else { "" };
    let current = rt.store.get(&credential.id);
    // A concurrent removal/re-import or management edit must not transfer the old
    // refresh failure/backoff to the replacement, which is tried when it is due.
    let Some(current) = current.filter(|c| match &result {
        Ok(prepared) => prepared.revision == c.revision,
        Err(_) => c.revision == credential.revision,
    }) else {
        reservation.end(|state| state.release(&credential.id));
        if let Err(error) = &result {
            warn_refresh_failed(&credential, trigger, error, None);
        }
        return;
    };
    let ineffective = result.is_ok() && rt.executors.readiness(&current, &cfg) != Readiness::Ready;
    let now = Instant::now();
    let retry = reservation.end(|state| {
        state.finish(&current, result.as_ref().err(), ineffective, now);
        state.retry_at(&current)
    });
    match &result {
        Err(error) => {
            let next = retry.map(|at| at.saturating_duration_since(now));
            warn_refresh_failed(&current, trigger, error, next);
        }
        // Go retries an ineffective refresh after 30 s and logs nothing.
        Ok(_) if ineffective => {}
        Ok(_) if current.revision != credential.revision => {
            tracing::info!(
                "credential refreshed for {} ({}){trigger}",
                current.provider,
                current.id
            );
        }
        Ok(_) => {}
    }
}

/// A refresh job's reservation of its credential. [`Self::end`] records the outcome and
/// wakes the refresh loop; a job that ends any other way (a panic outside preparation,
/// cancellation at shutdown) records a failure when this drops.
struct Reservation {
    rt: Arc<Runtime>,
    credential: Arc<Credential>,
    ended: bool,
}

impl Reservation {
    fn end<T>(&mut self, record: impl FnOnce(&mut RefreshState) -> T) -> T {
        let recorded = record(&mut self.rt.refresh_state.lock().unwrap_or_else(PoisonError::into_inner));
        self.ended = true;
        self.rt.refresh_done.send_replace(());
        recorded
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.ended {
            let error = ExecError::local(500, FailureScope::Request, "refresh job ended without a result");
            let credential = self.credential.clone();
            self.end(|state| state.finish(&credential, Some(&error), false, Instant::now()));
            warn_refresh_failed(&credential, "", &error, None);
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        if let Some(task) = self
            .refresh_task
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            task.abort();
        }
    }
}

/// The cooldown persistence setting; all state goes through the configured backend.
fn cooldown_dir(cfg: &Config, policy: &Policy) -> Option<std::path::PathBuf> {
    policy.save_cooldown_status.then(|| cfg.auth_dir.clone())
}

/// How long the refresh loop waits for one refresh job, the wait for the credential's
/// preparation lock included, before it moves on. The job runs on and records its own
/// outcome; until then no other job for the credential starts.
const REFRESH_JOB_BOUND: Duration = Duration::from_secs(if cfg!(test) { 2 } else { 300 });

/// The pause before a panicked refresh loop starts again.
const REFRESH_LOOP_RESTART: Duration = Duration::from_secs(if cfg!(test) { 0 } else { 5 });

/// One warning per failed refresh (Go logs most of these at Debug; see
/// DIFFERENCES-FROM-GO.md): provider, credential, status, the error and when the next
/// attempt is. `trigger` names what started it. A built-in executor's error text is
/// sanitized as client-facing upstream errors are (Go `SanitizeUpstreamErrorSummary`),
/// and token endpoint bodies never reach the error text.
fn warn_refresh_failed(c: &Credential, trigger: &str, error: &ExecError, next: Option<Duration>) {
    let text = refresh_error_text(error);
    let next = match next {
        Some(after) if !after.is_zero() => format!("next attempt in {}s", ceil_secs(after)),
        _ => "next attempt when it is next due".to_owned(),
    };
    tracing::warn!(
        "credential refresh failed for {} ({}){trigger}: status {}: {text}; {next}",
        c.provider,
        c.id,
        error.status
    );
}

/// What [`warn_refresh_failed`] logs of a refresh error.
fn refresh_error_text(error: &ExecError) -> String {
    crate::inference::sanitize::summary(&String::from_utf8_lossy(&error.body))
}

/// A due refresh held back by its previous job, which is still running: logged once
/// per job. A finished job's backoff is not logged here: a failure announced it, and Go
/// is silent about an ineffective refresh.
fn warn_refresh_held(state: &mut RefreshState, c: &Credential) {
    if state.note_hold(c) {
        tracing::warn!(
            "credential refresh for {} ({}) is due but its previous refresh is still running; it is tried again once that ends",
            c.provider,
            c.id
        );
    }
}

fn ceil_secs(d: Duration) -> u64 {
    d.as_secs() + u64::from(d.subsec_nanos() > 0)
}

/// A panic payload's message.
fn panic_text(panic: &(dyn std::any::Any + Send)) -> &str {
    panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string payload")
}

/// Go `refreshFailureBackoff`: an unauthorized refresh waits this long after a refresh
/// that just rotated the tokens.
const REFRESH_FAILURE_BACKOFF: Duration = Duration::from_secs(300);

/// `oauth.auth-auto-refresh-workers`: non-positive means Go's default of 16.
fn refresh_workers(cfg: &Config) -> usize {
    cfg.document
        .get("oauth")
        .and_then(|o| o.get("auth-auto-refresh-workers"))
        .and_then(serde_yaml_ng::Value::as_i64)
        .filter(|n| *n > 0)
        .map_or(16, |n| n as usize)
}

/// A selection's eligible credentials with their execution models, and those of them
/// that are ready now with their provider keys; when none is ready, the error Go's
/// availability check reports. The configured selector's own filter applies (weighted
/// round-robin skips non-positive weights).
#[allow(clippy::type_complexity)]
fn ready<'a>(
    creds: &'a [Arc<Credential>],
    scheduler: &crate::scheduling::scheduler::Scheduler,
    selection: &Selection,
    policy: &Policy,
    admit: &Admit<'_>,
    now: Instant,
) -> Result<(Vec<(&'a Arc<Credential>, String)>, Vec<(&'a Arc<Credential>, String)>), AcquireError> {
    let eligible: Vec<_> = creds
        .iter()
        .filter(|c| !c.disabled && !selection.exclude.contains(&c.id))
        .filter(|c| {
            !crate::scheduling::provider_policy::Providers::out_of_scope(
                selection.scope.as_deref(),
                &policy.providers,
                c,
                &selection.model,
            )
        })
        .filter(|c| policy.retry_limit(c) >= selection.retry_round && scheduler.admits(c, policy))
        .filter_map(|c| admit(c).map(|m| (c, m)))
        .collect();
    let candidates: Vec<_> = eligible
        .iter()
        .filter(|(c, m)| scheduler.wait(c, m, now).is_none() && !scheduler.tokens_rejected(c))
        .map(|(c, _)| (*c, crate::scheduling::registry::provider_key(c)))
        .collect();
    if candidates.is_empty() {
        let cause = eligible
            .iter()
            .filter_map(|(c, m)| scheduler.last_error(c, m))
            .max_by_key(|s| s.deadline)
            .map(|s| s.error.clone());
        let wait = eligible.iter().filter_map(|(c, m)| scheduler.wait(c, m, now)).min();
        if !eligible.is_empty() && eligible.iter().all(|(c, m)| scheduler.quota_cooling(c, m, now)) {
            return Err(AcquireError::Cooldown {
                wait: wait.unwrap_or_default(),
                cause,
            });
        }
        return Err(AcquireError::Unavailable {
            retry_after: if eligible.is_empty() { None } else { wait },
            cause,
        });
    }
    Ok((eligible, candidates))
}

#[derive(Debug)]
pub enum AcquireError {
    /// No candidate is ready. `retry_after` is set when candidates exist but are all
    /// blocked (Go `auth_unavailable`); unset means none is eligible (`auth_not_found`).
    Unavailable {
        retry_after: Option<Duration>,
        cause: Option<String>,
    },
    /// Every eligible candidate is quota-cooling for this model.
    Cooldown {
        wait: Duration,
        cause: Option<String>,
    },
    Prepare {
        id: String,
        error: ExecError,
    },
}

/// Route-model state is separate from the lease's resolved execution model.
#[derive(Debug, Clone, Default)]
pub struct Selection {
    /// Provider keys that may serve the model, in preference order.
    pub providers: Vec<String>,
    /// Single-provider shorthand, used when `providers` is empty.
    pub provider: String,
    /// The route model (requested model after `auto` resolution, suffix kept).
    pub model: String,
    /// The session this request binds to (Go `extractSessionIDs` primary).
    pub session: Option<String>,
    /// The session's parent, or the conversation alias of a prompt-cache-key session
    /// (Go fallback ID).
    pub session_parent: Option<String>,
    /// The session forked from `session_parent` (inherits its binding).
    pub session_fork: bool,
    /// A request without an explicit session, for the LCP matcher (Go `pickLCP`). Set
    /// only with session affinity on; it then decides the binding instead of `session`.
    pub lcp: Option<Arc<crate::scheduling::lcp::Request>>,
    /// Credential IDs already tried in this request.
    pub exclude: Vec<String>,
    pub retry_round: usize,
    /// A model router's provider scope and order (cooker only); `None` follows
    /// `routing.providers`.
    pub scope: Option<Arc<crate::scheduling::provider_policy::Providers>>,
}

impl Selection {
    pub fn new(provider: &str, model: &str) -> Self {
        Self {
            providers: vec![provider.to_owned()],
            model: model.to_owned(),
            ..Self::default()
        }
    }

    /// The provider keys this selection may use.
    pub fn provider_keys(&self) -> Vec<String> {
        if self.providers.is_empty() && !self.provider.is_empty() {
            vec![self.provider.clone()]
        } else {
            self.providers.clone()
        }
    }
}

/// Whether `next` would change nothing about the stored `current` (reconcile keeps
/// such a credential and its revision).
fn unchanged(current: &Credential, next: &Credential) -> bool {
    current.id == next.id
        && current.source == next.source
        && current.metadata == next.metadata
        && current.attributes == next.attributes
        && current.provider == next.provider
        && current.disabled == next.disabled
        && current.label == next.label
}

/// Admission without a registry: provider match, prefix, config aliases and the
/// credential's exclusions (attributes first, then file metadata).
pub fn standalone_admission<'a>(
    selection: &'a Selection,
    policy: &'a Policy,
) -> impl Fn(&Credential) -> Option<String> + 'a {
    let providers = selection.provider_keys();
    move |c| {
        if !providers.contains(&crate::scheduling::registry::provider_key(c)) {
            return None;
        }
        let route = selection.model.trim();
        let prefix = crate::scheduling::registry::credential_prefix(c);
        let base = crate::scheduling::scheduler::canonical_model(route);
        if !prefix.is_empty()
            && !base.starts_with(&format!("{prefix}/"))
            && (policy.force_model_prefix || base.contains('/'))
        {
            return None;
        }
        let (models, _) = crate::scheduling::registry::execution_models(&HashMap::new(), c, route);
        let model = models.into_iter().next().unwrap_or_default();
        // Go lowercases model IDs and patterns with `strings.ToLower`.
        let key = crate::scheduling::scheduler::canonical_model(&model).go_lower();
        let excluded: Vec<String> = match c.attributes.get("excluded_models").filter(|v| !v.trim().is_empty()) {
            Some(list) => list.split(',').map(|p| p.trim().go_lower()).collect(),
            None => c
                .metadata
                .get("excluded_models")
                .and_then(serde_json::Value::as_array)
                .map(|l| {
                    l.iter()
                        .filter_map(|v| v.as_str())
                        .map(|p| p.trim().go_lower())
                        .collect()
                })
                .unwrap_or_default(),
        };
        if excluded.iter().any(|p| crate::scheduling::registry::wildcard(p, &key)) {
            return None;
        }
        Some(model)
    }
}

#[derive(Debug, Clone)]
pub enum Outcome {
    Success,
    Failure(ExecError),
    /// A failure that must not change availability (compact request faults, a
    /// missing count_tokens endpoint).
    Neutral(ExecError),
    /// The client went away before the response finished.
    Cancelled,
}

/// Decides which credentials may serve a selection and under which model key their
/// cooldowns live. `None` rejects the credential.
pub type Admit<'a> = dyn Fn(&Credential) -> Option<String> + 'a;

/// Registry admission (Go `authSupportsRouteModel`) plus a registered executor.
pub fn admission<'a>(
    registry: &'a Registry,
    aliases: &'a HashMap<String, Vec<crate::scheduling::registry::OAuthAlias>>,
    selection: &'a Selection,
    executors: &'a Executors,
) -> impl Fn(&Credential) -> Option<String> + 'a {
    move |c| {
        let provider = crate::scheduling::registry::provider_key(c);
        if !selection.provider_keys().contains(&provider) || !executors.supports(&c.provider) {
            return None;
        }
        let key = crate::scheduling::registry::selection_model(aliases, c, &selection.model);
        let route = crate::scheduling::scheduler::canonical_model(&selection.model);
        if route.is_empty() {
            return Some(key);
        }
        let selection_key = crate::scheduling::scheduler::canonical_model(&key);
        (registry.client_supports(&c.id, route)
            || (selection_key != route && registry.client_supports(&c.id, selection_key)))
        .then_some(key)
    }
}

/// One attempt with one credential. Not cloneable: it reports exactly one outcome.
pub struct Lease {
    store: Arc<CredentialStore>,
    pub credential: Arc<Credential>,
    pub selection: Selection,
    /// The model key the outcome is recorded under (Go `stateModelForExecution`).
    pub execution_model: String,
    pub attempt: u64,
    policy: Arc<Policy>,
    reported: bool,
    /// The scheduler generation at the pick (`Scheduler::reserve_probe`): bounded
    /// windows opened or probes reserved after it are not answered by this attempt.
    picked: u32,
    /// The upstream dispatch stamp (`Scheduler::dispatch`).
    seq: u64,
    /// The LCP binding of the pick: the attempt's canonical session and lineage.
    pub lcp: Option<crate::scheduling::lcp::Match>,
}

impl Lease {
    /// A new upstream dispatch on this lease (a refresh-and-retry): a cooldown recorded
    /// before it no longer predates this attempt (Go `nextExecutionSeq` per dispatch).
    pub(crate) fn redispatch(&mut self) {
        self.seq = self
            .store
            .scheduler
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .dispatch();
    }

    pub fn complete(mut self, outcome: Outcome) {
        self.report(outcome);
    }

    /// The upstream accepted this attempt (a stream's first chunk arrived): an ended
    /// bounded window it probes has its answer, so other requests may use the account
    /// while the stream runs (`Scheduler::accept_probe`).
    pub fn accepted(&self) {
        self.store.accept_probe(self);
    }

    /// Records an intermediate outcome for one model of a pooled alias without ending
    /// the lease.
    pub fn note(&self, model: &str, outcome: &Outcome) {
        self.store.record_model(self, model, outcome);
    }

    fn report(&mut self, outcome: Outcome) {
        if std::mem::replace(&mut self.reported, true) {
            return;
        }
        self.store.record(self, &outcome);
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.report(Outcome::Cancelled);
    }
}

impl std::fmt::Debug for Lease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lease")
            .field("credential", &self.credential.id)
            .field("attempt", &self.attempt)
            .finish()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum PatchError {
    NotFound,
    /// The credential changed since the caller read it; re-read and retry.
    Stale {
        current: u64,
    },
    /// `type` decides the executor and cannot change in place.
    TypeIsImmutable,
    /// Config-sourced credentials persist through the config document, not here.
    ConfigBacked,
    Io(String),
    /// Another holder has the credential lock of this Codex file. The writer releases
    /// what it holds, waits for the lock ([`Runtime::lock_codex_file`]) and retries.
    LockBusy(std::path::PathBuf),
}

/// What a token refresh commits under ([`CredentialStore::apply_refresh`]).
#[derive(Debug, Clone)]
pub(crate) struct RefreshFence {
    /// The registration and credential version the refresh started under
    /// ([`CredentialStore::get_registered`]).
    pub registered: Registered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AttemptStats {
    pub success: u64,
    pub failure: u64,
    pub cancelled: u64,
}

struct Inner {
    creds: Vec<Arc<Credential>>,
    /// Last revision handed out. Revisions are never reused.
    generation: u64,
    /// Bumped on every reconcile and patch, including pure removals.
    epoch: u64,
}

/// Pending background refreshes after a 401, and the last token rotations.
#[derive(Debug, Clone, Default)]
pub(crate) struct Rejections {
    /// The rejected revision of each credential and when the 401 was recorded.
    pub pending: HashMap<String, (u64, Instant)>,
    /// The revision each credential's last token rotation committed, and when.
    pub refreshed: HashMap<String, (u64, Instant)>,
}

pub struct CredentialStore {
    inner: RwLock<Inner>,
    policy: RwLock<Arc<Policy>>,
    scheduler: Mutex<Scheduler>,
    #[cfg(any(test, feature = "testing"))]
    clock: std::sync::OnceLock<Arc<crate::testing::SchedulerClock>>,
    attempts: AtomicU64,
    stats: [AtomicU64; 3],
    prepare_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// Go 8a01bc02: a credential file is written with `inner` unlocked. Each
    /// credential's lock serializes its own writes, and the reload gate (shared by
    /// writes, exclusive for a set replacement) keeps a reload from landing between a
    /// write and its publication.
    write_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    reload: RwLock<()>,
    /// `auth-dir` while `save-cooldown-status` is on (Go `cooldownStore`).
    cooldown_dir: RwLock<Option<std::path::PathBuf>>,
    /// Cooldowns persist through SQLite or an explicit test backend.
    cooldown_backend: RwLock<Option<Arc<dyn crate::store::cooldown_store::Backend>>>,
    /// The SQLite authority for file credentials.
    backend: Arc<crate::store::credential_store::CredentialDb>,
    /// Serializes snapshot submission; SQLite writes happen on one coalescing worker.
    cooldown_write: Mutex<Option<crate::store::cooldown_store::Writer>>,
    activity: Mutex<HashMap<String, CredentialActivity>>,
    /// Credential revisions whose file already holds Go's persisted form.
    persisted: Mutex<HashMap<String, u64>>,
    /// At most one normalization job and one latest revision per credential.
    persisting: Mutex<HashMap<String, Arc<Credential>>>,
    persistence_tasks: tokio_util::task::TaskTracker,
    /// What each file credential's metadata was built from, by ID ([`Provenance`]).
    provenance: Mutex<HashMap<String, Provenance>>,
    /// Where failed attempts publish Go's error events (set by the runtime).
    error_events: std::sync::OnceLock<Arc<crate::observability::usage::UsageQueue>>,
    /// Marked on every epoch bump, so background loops sleep until the set changes.
    changed: tokio::sync::watch::Sender<()>,
    /// Codex credentials an upstream answered 401, and the last token rotations, for the
    /// background refresh (Go `RejectedAccessToken`, `scheduleUnauthorizedRefresh` and
    /// `LastRefreshedAt`).
    unauthorized: Mutex<Rejections>,
    unauthorized_noted: tokio::sync::watch::Sender<()>,
    /// Ends the upstream WebSocket sessions a removed credential serves (set by the
    /// runtime to [`ck_exec::Executors::credential_session_closer`]).
    session_closer: std::sync::OnceLock<SessionCloser>,
    /// What a refresh of each credential starts under, registered with every change to
    /// the set ([`Self::register`]).
    registrations: Mutex<HashMap<String, Registered>>,
}

/// Ends the upstream WebSocket sessions of one removed credential.
type SessionCloser = Box<dyn Fn(&str) + Send + Sync>;

/// What a refresh of a credential started under: Go `RegistrationEpoch`, set when the
/// credential's ID enters the set and kept while it is registered again, and
/// `CredentialVersion`, bumped by every registration that changes its tokens or keys. A
/// refresh that meets another registration or version drops its result, as Go's does,
/// unless a Codex token partition it refreshed still applies.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub(crate) struct Registered {
    pub registration: u64,
    pub credential_version: u64,
}

/// The source of [`Registered::registration`] numbers.
static REGISTRATIONS: AtomicU64 = AtomicU64::new(1);

/// [`CredentialStore::register`] on the map itself.
fn register_change(
    registrations: &mut HashMap<String, Registered>,
    before: &[Arc<Credential>],
    after: &[Arc<Credential>],
) {
    let previous: HashMap<&str, &Credential> = before.iter().map(|c| (c.id.as_str(), &**c)).collect();
    let mut registered = Vec::new();
    for c in after {
        let previous = previous.get(c.id.as_str());
        if previous.is_some_and(|p| p.revision == c.revision) {
            continue;
        }
        let kept = previous.and_then(|_| registrations.get(&c.id).copied());
        let next = match (kept, previous) {
            (Some(kept), Some(previous)) => Registered {
                registration: kept.registration,
                credential_version: kept.credential_version + u64::from(credentials_changed(previous, c)),
            },
            _ => Registered {
                registration: REGISTRATIONS.fetch_add(1, Ordering::Relaxed),
                credential_version: 1,
            },
        };
        registered.push((c.id.clone(), next));
    }
    let live: std::collections::HashSet<&str> = after.iter().map(|c| c.id.as_str()).collect();
    for id in previous.keys().filter(|id| !live.contains(*id)) {
        registrations.remove(*id);
    }
    registrations.extend(registered);
}

/// The ids in `before` that `after` no longer has.
fn removed_ids(before: &[Arc<Credential>], after: &[Arc<Credential>]) -> Vec<String> {
    let kept: std::collections::HashSet<&str> = after.iter().map(|c| c.id.as_str()).collect();
    before
        .iter()
        .filter(|c| !kept.contains(c.id.as_str()))
        .map(|c| c.id.clone())
        .collect()
}

/// Whether `key` is set to `true` on `credential` (Go `isRuntimeOnlyAuth`).
fn marked(credential: &Credential, key: &str) -> bool {
    credential
        .attributes
        .get(key)
        .is_some_and(|v| v.trim().eq_ignore_ascii_case("true"))
}

/// What a file credential's metadata was built from at one revision. Go holds a loaded
/// file's numbers as float64 read from its text, and a saved record's numbers as that
/// save held them; a management save reproduces them only from this, never from the
/// file as it is now, which may have changed since without a reload.
#[derive(Clone)]
pub struct Provenance {
    pub revision: u64,
    /// SHA-256 of the file bytes the revision was loaded from or committed as.
    pub sha256: [u8; 32],
    /// The document a management save committed (Go's metadata after that save); `None`
    /// for a load, where Go's metadata is the float64 reading of those bytes.
    pub doc: Option<Arc<ck_wire::json::GoValue>>,
    /// `doc` is the text of the bytes the record was loaded from (numbers as written,
    /// read as float64 by a save) with a refresh's values in place, not Go's metadata.
    pub loaded: bool,
}

/// How [`CredentialStore::commit_file_backed`] commits a management save.
pub struct Commit {
    /// The normalized bytes used to synthesize `next`; persisted when `write` is true.
    pub bytes: Vec<u8>,
    /// `false` when the canonical source already holds an equal document: retain its
    /// original bytes while durably acknowledging the save before publishing `next`.
    pub write: bool,
    /// What SQLite must still hold under the write lock, else `Stale`: `Some(None)`
    /// for no document, `Some(Some(hash))` for those bytes; `None` skips the check.
    pub disk: Option<Option<[u8; 32]>>,
    /// Go's metadata after the save, recorded as the new revision's [`Provenance`].
    pub doc: Option<Arc<ck_wire::json::GoValue>>,
}

/// A value a refresh set, as Go holds it (integers exact, other numbers float64), or
/// with `literal` its number text as written.
fn go_typed(v: &Value, literal: bool) -> ck_wire::json::GoValue {
    use ck_wire::json::GoValue;
    match v {
        Value::Null => GoValue::Null,
        Value::Bool(b) => GoValue::Bool(*b),
        Value::Number(n) if literal || n.is_i64() || n.is_u64() => GoValue::Number(n.to_string()),
        Value::Number(n) => {
            let text = n.to_string();
            GoValue::Number(ck_wire::json::json_float(ck_wire::json::parse_float(text.as_bytes())).unwrap_or(text))
        }
        Value::String(s) => GoValue::String(s.clone()),
        Value::Array(items) => GoValue::Array(items.iter().map(|v| go_typed(v, literal)).collect()),
        Value::Object(map) => GoValue::Object(map.iter().map(|(k, v)| (k.clone(), go_typed(v, literal))).collect()),
    }
}

pub(crate) fn sha256(bytes: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes).into()
}

impl CredentialStore {
    pub fn new(credentials: Vec<Credential>, backend: Arc<crate::store::credential_store::CredentialDb>) -> Arc<Self> {
        let mut inner = Inner {
            creds: Vec::new(),
            generation: 0,
            epoch: 0,
        };
        for mut cred in credentials {
            inner.generation += 1;
            cred.revision = inner.generation;
            inner.creds.push(Arc::new(cred));
        }
        let mut registrations = HashMap::new();
        register_change(&mut registrations, &[], &inner.creds);
        Arc::new(Self {
            inner: RwLock::new(inner),
            policy: RwLock::new(Arc::new(Policy::default())),
            scheduler: Mutex::default(),
            #[cfg(any(test, feature = "testing"))]
            clock: Default::default(),
            attempts: AtomicU64::new(0),
            stats: Default::default(),
            prepare_locks: Mutex::default(),
            write_locks: Mutex::default(),
            reload: RwLock::default(),
            cooldown_dir: RwLock::default(),
            cooldown_backend: RwLock::default(),
            backend,
            cooldown_write: Mutex::default(),
            activity: Mutex::default(),
            persisted: Mutex::default(),
            persisting: Mutex::default(),
            persistence_tasks: tokio_util::task::TaskTracker::new(),
            provenance: Mutex::default(),
            error_events: std::sync::OnceLock::new(),
            changed: tokio::sync::watch::Sender::new(()),
            unauthorized: Mutex::default(),
            unauthorized_noted: tokio::sync::watch::Sender::new(()),
            session_closer: std::sync::OnceLock::new(),
            registrations: Mutex::new(registrations),
        })
    }

    fn now(&self) -> Instant {
        #[cfg(any(test, feature = "testing"))]
        if let Some(clock) = self.clock.get() {
            return clock.now();
        }
        Instant::now()
    }

    fn wall(&self) -> std::time::SystemTime {
        #[cfg(any(test, feature = "testing"))]
        if let Some(clock) = self.clock.get() {
            return clock.wall();
        }
        std::time::SystemTime::now()
    }

    pub fn credential_backend(&self) -> Arc<crate::store::credential_store::CredentialDb> {
        self.backend.clone()
    }

    /// A successful save precedes publication, including when the working file already
    /// holds these bytes. SQLite commits synchronously with its materialized view.
    fn credential_command(&self) -> Result<crate::store::credential_store::CredentialCommand<'_>, PatchError> {
        self.backend.command().map_err(|e| PatchError::Io(e.to_string()))
    }

    fn granted_command(&self) -> Result<crate::store::credential_store::CredentialCommand<'_>, PatchError> {
        self.backend
            .granted_command()
            .map_err(|e| PatchError::Io(e.to_string()))
    }

    fn grant_blocks(&self, credential: &Credential) -> bool {
        let Source::File(path) = &credential.source else {
            return false;
        };
        self.backend.required_source(path).is_some_and(|required| {
            self.provenance(&credential.id)
                .is_none_or(|source| source.revision != credential.revision || Some(source.sha256) != required)
        })
    }

    fn save_document(
        &self,
        command: &mut Option<crate::store::credential_store::CredentialCommand<'_>>,
        path: &Path,
        bytes: &[u8],
    ) -> Result<(), PatchError> {
        #[cfg(test)]
        tests::stall_write(path);
        command
            .as_mut()
            .ok_or(PatchError::ConfigBacked)?
            .save(path, bytes)
            .map_err(|e| PatchError::Io(e.to_string()))
    }

    /// Called under the stable document lock, before a local refresh can spend a token.
    fn restore_refresh_document(
        &self,
        backend: &crate::store::credential_store::CredentialDb,
        current: &Arc<Credential>,
        registered: Registered,
        cfg: &Config,
    ) -> Result<(Arc<Credential>, Registered), PatchError> {
        use ck_core::config::credentials;
        let Source::File(path) = &current.source else {
            return Err(PatchError::ConfigBacked);
        };
        backend.restore(path).map_err(|e| PatchError::Io(e.to_string()))?;
        let bytes = backend
            .read(path)
            .map_err(|e| PatchError::Io(e.to_string()))?
            .ok_or(PatchError::NotFound)?;
        let hash = sha256(&bytes);
        if self
            .provenance(&current.id)
            .is_some_and(|p| p.revision == current.revision && p.sha256 == hash)
        {
            return Ok((current.clone(), registered));
        }
        let parsed = credentials::from_file(cfg, &cfg.auth_dir, path, &bytes)
            .map_err(PatchError::Io)?
            .filter(|c| c.provider == current.provider)
            .ok_or(PatchError::NotFound)?;
        let mut next = Credential::clone(current);
        next.metadata = parsed.metadata;
        next.disabled = parsed.disabled;
        next.refresh_derived();
        publish_metadata_proxy(&mut next);
        #[cfg(test)]
        tests::before_restore(path);
        let adopted = self
            .write_unlocked(
                &current.id,
                current.revision,
                Some(registered),
                false,
                |_, command| {
                    let held = command
                        .as_ref()
                        .ok_or(PatchError::ConfigBacked)?
                        .read(path)
                        .map_err(|e| PatchError::Io(e.to_string()))?;
                    if held.as_deref() != Some(bytes.as_slice()) {
                        return Err(PatchError::Stale {
                            current: current.revision,
                        });
                    }
                    Ok(Some((None, bytes, next)))
                },
                |inner, index, _, mut next| {
                    if !unchanged(&inner.creds[index], &next) {
                        inner.generation += 1;
                        next.revision = inner.generation;
                        let next = Arc::new(next);
                        self.register(std::slice::from_ref(&inner.creds[index]), std::slice::from_ref(&next));
                        inner.creds[index] = next;
                        inner.epoch += 1;
                        self.changed.send_replace(());
                    }
                    let next = inner.creds[index].clone();
                    self.note_provenance(&next.id, next.revision, hash, None);
                    let registered = self.registered(&next.id);
                    (next, registered)
                },
            )?
            .expect("document adoption always publishes");
        if credentials_changed(current, &adopted.0) {
            self.recover_unauthorized(&current.id);
        }
        Ok(adopted)
    }

    /// Credential `id` and what a refresh of it starts under: its registration and
    /// credential version ([`Registered`]), read together.
    pub(crate) fn get_registered(&self, id: &str) -> Option<(Arc<Credential>, Registered)> {
        let inner = self.read();
        let credential = inner.creds.iter().find(|c| c.id == id)?.clone();
        Some((credential, self.registered(id)))
    }

    /// What a refresh of `id` starts under; zeros for none.
    fn registered(&self, id: &str) -> Registered {
        self.registrations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(id)
            .copied()
            .unwrap_or_default()
    }

    /// Checks a refresh outcome of credential `id` (Go reads `m.auths[id]` under
    /// `m.mu`): the current credential while it is under `registration` (`NotFound`
    /// otherwise) and, given `at`, still at that revision and credential version
    /// (`Stale` otherwise).
    pub(crate) fn record_refresh(
        &self,
        id: &str,
        registration: u64,
        at: Option<(u64, u64)>,
    ) -> Result<Arc<Credential>, PatchError> {
        let inner = self.read();
        let current = inner.creds.iter().find(|c| c.id == id).ok_or(PatchError::NotFound)?;
        let registered = self.registered(id);
        if registered.registration != registration {
            return Err(PatchError::NotFound);
        }
        if at.is_some_and(|at| at != (current.revision, registered.credential_version)) {
            return Err(PatchError::Stale {
                current: current.revision,
            });
        }
        Ok(current.clone())
    }

    /// Registers a change to the set from `before` to `after` (the credentials it
    /// replaced, and the set or the credentials replacing them); called with the change,
    /// under the write lock. Go `Register` gives an auth new to the manager the next
    /// epoch for its ID; `Update` keeps it, and bumps its credential version when its
    /// credentials changed (`updateInternal`, `CredentialsChanged`). A credential that
    /// leaves the set loses its registration (Go `Manager.Remove`).
    fn register(&self, before: &[Arc<Credential>], after: &[Arc<Credential>]) {
        let mut registrations = self.registrations.lock().unwrap_or_else(PoisonError::into_inner);
        register_change(&mut registrations, before, after);
    }

    /// Go `ApplyConfigWithCooldownStateStore` plus the restore that follows a config
    /// update: the current state goes to the old store before the swap, and a newly
    /// enabled store is restored from. `None` disables persistence.
    pub fn configure_cooldown_store(&self, dir: Option<std::path::PathBuf>) {
        let old = self.cooldown_dir.read().unwrap_or_else(PoisonError::into_inner).clone();
        if old == dir {
            return;
        }
        if old.is_some() {
            self.persist_cooldowns();
        }
        *self.cooldown_dir.write().unwrap_or_else(PoisonError::into_inner) = dir.clone();
        if let Some(dir) = dir {
            self.restore_cooldowns(&dir);
        }
    }

    /// Restores cooldowns from the backend when persistence is enabled.
    pub fn set_cooldown_backend(&self, backend: Arc<dyn crate::store::cooldown_store::Backend>) {
        *self.cooldown_write.lock().unwrap_or_else(PoisonError::into_inner) =
            Some(crate::store::cooldown_store::Writer::new(backend.clone()));
        *self.cooldown_backend.write().unwrap_or_else(PoisonError::into_inner) = Some(backend);
        let dir = self.cooldown_dir.read().unwrap_or_else(PoisonError::into_inner).clone();
        if let Some(dir) = dir {
            self.restore_cooldowns(&dir);
        }
    }

    fn cooldown_backend(&self) -> Option<Arc<dyn crate::store::cooldown_store::Backend>> {
        self.cooldown_backend
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Go `RestoreCooldownStates`: live records of live credentials whose cooling is
    /// enabled, then a rewrite that drops everything else.
    fn restore_cooldowns(&self, _dir: &std::path::Path) {
        let Some(backend) = self.cooldown_backend() else {
            return;
        };
        let records = match backend.load() {
            Ok(records) => records,
            Err(error) => {
                tracing::warn!(%error, "failed to restore cooldown state");
                return;
            }
        };
        if !records.is_empty() {
            let policy = self.policy.read().unwrap_or_else(PoisonError::into_inner).clone();
            let inner = self.read();
            let mut scheduler = self.scheduler.lock().unwrap_or_else(PoisonError::into_inner);
            let (now, wall) = (self.now(), self.wall());
            for record in &records {
                let Some(c) = inner.creds.iter().find(|c| c.id == record.auth_id.trim()) else {
                    continue;
                };
                if c.disabled || policy.cooling_disabled(c) {
                    continue;
                }
                let has_models = records
                    .iter()
                    .any(|r| r.auth_id.trim() == c.id && !r.model.trim().is_empty());
                scheduler.restore(c, record, has_models, policy.max_trusted_cooldown, now, wall);
            }
        }
        self.persist_cooldowns();
    }

    /// Persists the live cooldowns through the configured backend.
    pub fn persist_cooldowns(&self) {
        if self
            .cooldown_dir
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .is_none()
        {
            return;
        }
        let writer = self.cooldown_write.lock().unwrap_or_else(PoisonError::into_inner);
        let wall = self.wall();
        let mut records = {
            let policy = self.policy.read().unwrap_or_else(PoisonError::into_inner).clone();
            let inner = self.read();
            let scheduler = self.scheduler.lock().unwrap_or_else(PoisonError::into_inner);
            let now = self.now();
            inner
                .creds
                .iter()
                .filter(|c| !c.disabled && !policy.cooling_disabled(c))
                .flat_map(|c| scheduler.records(c, now, wall))
                .collect::<Vec<_>>()
        };
        records.sort_by(|a, b| (&a.provider, &a.auth_id, &a.model).cmp(&(&b.provider, &b.auth_id, &b.model)));
        if let Some(writer) = &*writer {
            writer.save(records, wall);
        }
    }

    #[cfg(test)]
    fn flush_cooldowns(&self) {
        if let Some(writer) = &*self.cooldown_write.lock().unwrap_or_else(PoisonError::into_inner) {
            writer.flush();
        }
    }

    fn queue_persist_credential(self: &Arc<Self>, credential: &Arc<Credential>) {
        if !matches!(&credential.source, Source::File(_))
            || credential.disabled
            || marked(credential, "runtime_only")
            || self
                .persisted
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get(&credential.id)
                == Some(&credential.revision)
        {
            return;
        }
        let handle = match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle,
            Err(_) => {
                if let Err(error) = self.persist_credential(credential) {
                    tracing::warn!(id = %credential.id, ?error, "failed to persist credential");
                }
                return;
            }
        };
        {
            let mut pending = self.persisting.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(latest) = pending.get_mut(&credential.id) {
                if latest.revision < credential.revision {
                    *latest = credential.clone();
                }
                return;
            }
            pending.insert(credential.id.clone(), credential.clone());
        }
        let store = self.clone();
        let mut credential = credential.clone();
        self.persistence_tasks.spawn_blocking_on(
            move || loop {
                if let Err(error) = store.persist_credential(&credential) {
                    tracing::warn!(id = %credential.id, ?error, "failed to persist credential");
                }
                let mut pending = store.persisting.lock().unwrap_or_else(PoisonError::into_inner);
                let latest = pending.get(&credential.id).unwrap();
                if latest.revision == credential.revision {
                    pending.remove(&credential.id);
                    break;
                }
                credential = latest.clone();
            },
            &handle,
        );
    }

    /// Normalizes a live local credential once per revision, after its result.
    fn persist_credential(&self, credential: &Arc<Credential>) -> Result<(), PatchError> {
        let Source::File(path) = &credential.source else {
            return Ok(());
        };
        // Go also skips runtime-only credentials.
        if credential.disabled || marked(credential, "runtime_only") {
            return Ok(());
        }
        {
            let persisted = self.persisted.lock().unwrap_or_else(PoisonError::into_inner);
            if persisted.get(&credential.id) == Some(&credential.revision) {
                return Ok(());
            }
        }
        let backend = self.credential_backend();
        // Result recording must not wait behind an in-flight token grant. Leave the
        // revision unmarked and retry its normalization on a later result.
        let _document_lock = ck_exec::codex_siwc_bind::open_lock_file(path).map_err(PatchError::Io)?;
        if !ck_exec::codex_siwc_bind::try_lock(&_document_lock).map_err(PatchError::Io)? {
            return Ok(());
        }
        backend.restore(path).map_err(|e| PatchError::Io(e.to_string()))?;
        // Written only while the credential is still at this revision: a refresh committed
        // after this request started must not be overwritten with the older token.
        let written = self.write_unlocked(
            &credential.id,
            credential.revision,
            None,
            false,
            |current, _command| {
                let mut metadata = current.metadata.clone();
                metadata.insert("disabled".into(), Value::Bool(current.disabled));
                let go = |bytes: &[u8]| ck_wire::json::GoValue::parse_f64(bytes);
                // An unreadable file or metadata is skipped (`NotFound`), not reported.
                let Some(target) = serde_json::to_vec(&metadata).ok().and_then(|b| go(&b)) else {
                    return Err(PatchError::NotFound);
                };
                let existing = backend
                    .read(path)
                    .map_err(|e| PatchError::Io(e.to_string()))?
                    .ok_or(PatchError::NotFound)?;
                if self
                    .provenance(&current.id)
                    .is_some_and(|p| p.revision == current.revision && p.sha256 != sha256(&existing))
                {
                    return Err(PatchError::NotFound);
                }
                if go(&existing).as_ref() == Some(&target) {
                    return Ok(Some((Some(path.clone()), existing, ())));
                }
                Ok(Some((Some(path.clone()), target.marshal(), ())))
            },
            |inner, index, bytes, ()| {
                // Same revision: the file now says what memory already meant (absent
                // `disabled` is false, numbers are float64), so memory takes the written
                // form and the watcher's reload finds nothing changed.
                if let Ok(written) = serde_json::from_slice::<Map<String, Value>>(bytes) {
                    let mut next = Credential::clone(&inner.creds[index]);
                    next.metadata = written;
                    inner.creds[index] = Arc::new(next);
                }
                // The source digest still proves adoption after normalization, even
                // when no exact Go document is available for later metadata edits.
                let id = &inner.creds[index].id;
                if let Some(provenance) = self
                    .provenance
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .get_mut(id)
                {
                    provenance.sha256 = sha256(bytes);
                }
            },
        );
        match written {
            Ok(_) => {}
            Err(error @ PatchError::Io(_)) => return Err(error),
            Err(_) => return Ok(()),
        }
        self.persisted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(credential.id.clone(), credential.revision);
        Ok(())
    }

    /// Go `updateInternalLocked` for a credential a reload, save or import
    /// replaced with other tokens or another API key (`CredentialsChanged`), enabled
    /// before and after: its unauthorized cooldowns go, so the new token is selectable
    /// at once, and every other cooldown (a model's 429, a live credential quota) stays.
    /// Called with the change, under the write lock; returns whether anything was
    /// cleared, for the caller to persist.
    fn clear_replaced_unauthorized(&self, before: &[Arc<Credential>], after: &[Arc<Credential>]) -> bool {
        let replaced: Vec<&str> = after
            .iter()
            .filter(|next| {
                before
                    .iter()
                    .find(|c| c.id == next.id)
                    .is_some_and(|previous| !previous.disabled && !next.disabled && credentials_changed(previous, next))
            })
            .map(|c| c.id.as_str())
            .collect();
        if replaced.is_empty() {
            return false;
        }
        let mut scheduler = self.scheduler.lock().unwrap_or_else(PoisonError::into_inner);
        let now = self.now();
        let mut cleared = false;
        for id in replaced {
            cleared |= scheduler.clear_unauthorized(id, now);
        }
        cleared
    }

    /// Go `clearDisabledCooldownStates` after a policy change.
    fn clear_disabled_cooldowns(&self) {
        let policy = self.policy.read().unwrap_or_else(PoisonError::into_inner).clone();
        let cleared = {
            let inner = self.read();
            self.scheduler
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clear_disabled(&inner.creds, &policy)
        };
        if cleared {
            self.persist_cooldowns();
        }
    }

    pub fn snapshot(&self) -> Vec<Arc<Credential>> {
        self.read().creds.clone()
    }

    pub fn get(&self, id: &str) -> Option<Arc<Credential>> {
        self.read().creds.iter().find(|c| c.id == id).cloned()
    }

    /// Selects among the selection's providers without registry admission (tests and
    /// callers that already filtered credentials).
    pub fn select(self: &Arc<Self>, selection: Selection) -> Option<Lease> {
        let policy = self.policy.read().unwrap_or_else(PoisonError::into_inner).clone();
        let scope = selection.clone();
        let admit = standalone_admission(&scope, &policy);
        self.select_with(selection, policy.clone(), &admit).ok()
    }

    pub fn select_with(
        self: &Arc<Self>,
        selection: Selection,
        policy: Arc<Policy>,
        admit: &Admit<'_>,
    ) -> Result<Lease, AcquireError> {
        self.select_ranked(selection, policy, admit, &|_| {
            crate::scheduling::scheduler::Windows::default()
        })
    }

    /// [`Self::select_with`] with each credential's usage windows (`soonest-reset`).
    pub fn select_ranked(
        self: &Arc<Self>,
        selection: Selection,
        policy: Arc<Policy>,
        admit: &Admit<'_>,
        ranks: &crate::scheduling::scheduler::Ranks<'_>,
    ) -> Result<Lease, AcquireError> {
        let allowed = |c: &Credential| (!self.grant_blocks(c)).then(|| admit(c)).flatten();
        let admit: &Admit<'_> = &allowed;
        let now = self.now();
        let credential = {
            let inner = self.read();
            let mut scheduler = self.scheduler.lock().unwrap_or_else(PoisonError::into_inner);
            let (eligible, candidates) = ready(&inner.creds, &scheduler, &selection, &policy, admit, now)?;
            let refs: Vec<(&Credential, &str)> = candidates.iter().map(|(c, p)| (c.as_ref(), p.as_str())).collect();
            let (picked, lcp) = scheduler.pick_session(&refs, &selection, &policy, ranks, now).unwrap();
            let (c, m) = eligible.iter().find(|(c, _)| c.id == picked.id).unwrap();
            let generation = scheduler.reserve_probe(c, m, now);
            (Arc::clone(c), lcp, generation, scheduler.dispatch())
        };
        let (credential, lcp, picked, seq) = credential;
        let execution_model = admit(&credential).unwrap_or_else(|| selection.model.clone());
        Ok(Lease {
            store: self.clone(),
            credential,
            selection,
            execution_model,
            attempt: self.attempts.fetch_add(1, Ordering::Relaxed),
            policy,
            reported: false,
            picked,
            seq,
            lcp,
        })
    }

    /// The registry projection of this credential's cooldown state for `model`.
    pub fn suspension(&self, credential: &Credential, model: &str) -> crate::scheduling::registry::Suspension {
        use crate::scheduling::registry::Suspension;
        let scheduler = self.scheduler.lock().unwrap_or_else(PoisonError::into_inner);
        let now = self.now();
        let live = |m: &str| {
            scheduler
                .cooldowns
                .get(&(
                    credential.id.clone(),
                    crate::scheduling::scheduler::canonical_model(m).to_owned(),
                ))
                .filter(|s| s.deadline > now)
        };
        if let Some(state) = live(model) {
            return match (state.quota, state.status) {
                (true, 429) => Suspension::Quota,
                (quota_exceeded, _) => Suspension::Other { quota_exceeded },
            };
        }
        if live("").is_some() {
            // Credential-wide quota: Go reports these models as `credential_quota`.
            return Suspension::Other { quota_exceeded: false };
        }
        Suspension::None
    }

    /// Whether `model` is cooling for this credential right now, or its tokens were
    /// refused for good. A probe reservation does not count: the lease that holds it is
    /// the one asking.
    pub fn blocked(&self, credential: &Credential, model: &str) -> bool {
        let scheduler = self.scheduler.lock().unwrap_or_else(PoisonError::into_inner);
        self.grant_blocks(credential)
            || scheduler.cooling(credential, model, self.now())
            || scheduler.tokens_rejected(credential)
    }

    /// Records that `refused`, the credential as an upstream refused it with a 401, can no
    /// longer recover, when `error` (its refresh's) is invalid_grant and the store still
    /// holds it with the same tokens and a non-empty access token (Go
    /// `accessTokenRejected`), under the registration the refresh started under
    /// (`registration`, Go `RegistrationEpoch`), so a credential removed and imported
    /// again while the exchange ran is not rejected. Checked and recorded under the
    /// store's read lock, so a replacement or removal lands entirely before or after.
    pub(crate) fn reject_refused(&self, refused: &Credential, registration: u64, error: &ExecError) -> bool {
        if !crate::scheduling::classify::is_invalid_grant(error.status, &String::from_utf8_lossy(&error.body)) {
            return false;
        }
        let inner = self.read();
        let Some(current) = inner.creds.iter().find(|c| c.id == refused.id) else {
            return false;
        };
        let token = effective_access_token(refused);
        if current.disabled
            || self.registered(&current.id).registration != registration
            || token.is_empty()
            || effective_access_token(current) != token
            || credentials_changed(refused, current)
        {
            return false;
        }
        self.scheduler
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .reject_tokens(current.clone());
        true
    }

    /// Whether `c`'s tokens were refused for good ([`crate::scheduling::scheduler::Scheduler::tokens_rejected`]).
    pub fn tokens_rejected(&self, c: &Credential) -> bool {
        self.grant_blocks(c) || self.tokens_refused(c)
    }

    fn tokens_refused(&self, c: &Credential) -> bool {
        self.scheduler
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .tokens_rejected(c)
    }

    /// The IDs in `snapshot` whose tokens were refused for good.
    fn rejected_ids(&self, snapshot: &[Arc<Credential>]) -> std::collections::HashSet<String> {
        let scheduler = self.scheduler.lock().unwrap_or_else(PoisonError::into_inner);
        snapshot
            .iter()
            .filter(|c| scheduler.tokens_rejected(c) || self.grant_blocks(c))
            .map(|c| c.id.clone())
            .collect()
    }

    /// Clears every cooldown and affinity binding of one credential (Go `ResetQuota`).
    /// Returns the model keys that were cooling.
    pub fn reset_cooldown(&self, id: &str) -> Vec<String> {
        let models = self.scheduler.lock().unwrap_or_else(PoisonError::into_inner).reset(id);
        if !models.is_empty() {
            self.persist_cooldowns();
        }
        models
    }

    /// Returns the next round's wait, if any credential still permits that round
    /// (Go `closestCooldownWaitWithAttempted`). A wait exceeding the cap is rejected,
    /// not shortened. `status` is the round error's status; `attempted` the credentials
    /// that executed this round (they get the 10s quota floor after a 429).
    pub fn retry_wait(
        &self,
        selection: &Selection,
        policy: &Policy,
        status: u16,
        attempted: &[String],
        admit: &Admit<'_>,
    ) -> Option<Duration> {
        self.retry_wait_at(selection, policy, status, attempted, admit, self.now())
    }

    fn retry_wait_at(
        &self,
        selection: &Selection,
        policy: &Policy,
        status: u16,
        attempted: &[String],
        admit: &Admit<'_>,
        now: Instant,
    ) -> Option<Duration> {
        let inner = self.read();
        let scheduler = self.scheduler.lock().unwrap_or_else(PoisonError::into_inner);
        let wait = inner
            .creds
            .iter()
            .filter(|c| !c.disabled)
            .filter(|c| policy.retry_limit(c) > selection.retry_round && scheduler.admits(c, policy))
            .filter_map(|c| admit(c).map(|m| (c, m)))
            .filter(|(c, m)| {
                scheduler.retry_eligible(c, m, now) && !scheduler.tokens_rejected(c) && !self.grant_blocks(c)
            })
            .map(|(c, m)| {
                let wait = scheduler.wait(c, &m, now).unwrap_or_default();
                if status == 429 && attempted.contains(&c.id) && !policy.cooling_disabled(c) {
                    wait.max(Duration::from_secs(10))
                } else {
                    wait
                }
            })
            .min()?;
        if wait > policy.max_retry_interval {
            None
        } else {
            Some(wait)
        }
    }

    fn accept_probe(&self, lease: &Lease) {
        let inner = self.read();
        // Like results: a credential edited or re-created since the pick is left alone.
        if !inner
            .creds
            .iter()
            .any(|c| c.id == lease.credential.id && c.revision == lease.credential.revision)
        {
            return;
        }
        self.scheduler
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .accept_probe_picked(&lease.credential, &lease.execution_model, lease.picked, self.now());
    }

    fn record(&self, lease: &Lease, outcome: &Outcome) {
        self.record_model(lease, &lease.execution_model, outcome);
    }

    fn record_model(&self, lease: &Lease, model: &str, outcome: &Outcome) {
        let slot = match outcome {
            Outcome::Success => 0,
            Outcome::Failure(_) | Outcome::Neutral(_) => 1,
            Outcome::Cancelled => 2,
        };
        self.stats[slot].fetch_add(1, Ordering::Relaxed);
        if !matches!(outcome, Outcome::Cancelled) {
            self.note_activity(&lease.credential.id, matches!(outcome, Outcome::Success));
        }
        let inner = self.read();
        // Outcomes from credentials deleted/re-created during an attempt must not
        // poison the replacement. Metadata edits likewise invalidate stale results.
        if inner
            .creds
            .iter()
            .any(|c| c.id == lease.credential.id && c.revision == lease.credential.revision)
        {
            let (now, wall) = (self.now(), self.wall());
            let persist = self
                .cooldown_dir
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .is_some();
            let mut scheduler = self.scheduler.lock().unwrap_or_else(PoisonError::into_inner);
            let before = persist.then(|| scheduler.records(&lease.credential, now, wall));
            scheduler.record_dispatched(
                &lease.credential,
                model,
                outcome,
                &lease.policy,
                lease.picked,
                lease.seq,
                now,
            );
            scheduler.session_result(
                &lease.credential,
                &lease.selection,
                outcome,
                &lease.policy,
                lease.lcp.as_ref(),
                now,
            );
            // Go MarkResult: a 401 that cools the credential schedules a background
            // refresh of the rejected token (feb63984; the runtime checks for a refresh
            // credential). A forced cooldown counts even on a request-scoped 401.
            // ponytail: Codex only for now; other providers (Claude's included) still
            // refresh after a 401 inline only, until the post-replay port.
            let rejected = matches!(outcome, Outcome::Failure(error)
                if lease.credential.provider == "codex"
                    && crate::scheduling::classify::names_unauthorized(error)
                    && lease.policy.error_action(&lease.credential, error).cooldown);
            // Go `publishErrorEvent` after MarkResult (and the availability-neutral record).
            if let (Outcome::Failure(error) | Outcome::Neutral(error), Some(queue)) = (outcome, self.error_events.get())
                && queue.wants_errors()
            {
                let records = scheduler.records(&lease.credential, now, wall);
                queue.enqueue_error(&crate::observability::error_events::payload(
                    &lease.credential,
                    model,
                    error,
                    &records,
                ));
            }
            // Go MarkResult persists only when this credential's cooldown records changed.
            let changed = before.is_some_and(|before| before != scheduler.records(&lease.credential, now, wall));
            drop(scheduler);
            drop(inner);
            // After the store lock: a refresh commit holds this lock while it writes.
            if rejected {
                let mut state = self.unauthorized.lock().unwrap_or_else(PoisonError::into_inner);
                let revision = lease.credential.revision;
                // Go sets `NextRefreshAfter` only when none is pending.
                if state
                    .pending
                    .get(&lease.credential.id)
                    .is_none_or(|(r, _)| *r != revision)
                {
                    state.pending.insert(lease.credential.id.clone(), (revision, now));
                    self.unauthorized_noted.send_replace(());
                }
            }
            if changed {
                self.persist_cooldowns();
            }
            lease.store.queue_persist_credential(&lease.credential);
        }
    }

    /// Resolves after the next change to the credential set or any credential.
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<()> {
        self.changed.subscribe()
    }

    /// Pending 401 refreshes and the last rotations. Read it before the credential
    /// snapshot it is compared with.
    pub(crate) fn unauthorized(&self) -> Rejections {
        self.unauthorized.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    /// Forgets a pending unauthorized refresh unless a 401 on a newer revision replaced it.
    pub(crate) fn forget_unauthorized(&self, id: &str, revision: u64) {
        let mut state = self.unauthorized.lock().unwrap_or_else(PoisonError::into_inner);
        if state.pending.get(id).is_some_and(|(r, _)| *r <= revision) {
            state.pending.remove(id);
        }
    }

    /// Drops the rejections `seen` (read before `snapshot`) whose credential the snapshot
    /// replaced or removed. A 401 recorded after `seen` is never touched.
    pub(crate) fn forget_stale_unauthorized(&self, seen: &Rejections, snapshot: &[Arc<Credential>]) {
        for (id, (revision, _)) in &seen.pending {
            if snapshot
                .iter()
                .find(|c| c.id == *id)
                .is_none_or(|c| c.revision > *revision)
            {
                self.forget_unauthorized(id, *revision);
            }
        }
        let mut state = self.unauthorized.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .refreshed
            .retain(|id, _| !seen.refreshed.contains_key(id) || snapshot.iter().any(|c| c.id == *id));
    }

    /// [`Self::apply_patch`] for a token refresh that started under `fence`'s
    /// registration: refused (`NotFound`) once the credential's ID was registered again
    /// (Go's `RegistrationEpoch` check). When `rotated` says the
    /// candidate replaces the tokens, its time is recorded with memory publication,
    /// before any scan can see the new revision without it (Go `LastRefreshedAt`).
    /// `rotated` is a pure comparison of the prepared candidate, not an after-commit
    /// command hook; it runs without memory locks, before persistence.
    pub(crate) fn apply_refresh(
        &self,
        id: &str,
        expected_revision: u64,
        fence: RefreshFence,
        patch: &MetadataPatch,
        edit: impl FnOnce(&mut Credential),
        rotated: impl FnOnce(&Credential) -> bool,
    ) -> Result<(Arc<Credential>, bool), PatchError> {
        let before = self.get(id).ok_or(PatchError::NotFound)?;
        let mut after = before.as_ref().clone();
        patch.apply(&mut after.metadata);
        after.refresh_derived();
        let changes_tokens = credentials_changed(&before, &after);
        // Only post-prepare token results may bypass failed publication admission.
        let mut command = Some(if changes_tokens {
            self.granted_command()?
        } else {
            self.credential_command()?
        });
        self.apply_patch_admitted(&mut command, (id, expected_revision), Some(fence), patch, edit, rotated)
    }

    /// Go `updateInternal` for a Codex refresh of `original`, run while the caller holds
    /// the file's credential lock. Refused (`NotFound`) once the ID was registered again since
    /// `registered`. When the credential version moved or the credentials changed since
    /// `original` and no refreshed partition still holds a version `patch` may be applied
    /// over (Go `CanApplyTo`), the refresh is discarded with no side effect. Otherwise
    /// `patch` is applied to the file as it is now, written with legacy keys renamed (Go
    /// `PersistCodexAuthFile`), and that document published as the credential's
    /// metadata, with its proxy, its time is recorded as [`Self::apply_refresh`] records it,
    /// and new tokens clear unauthorized cooldowns. Blocking.
    fn admit_codex_refresh(
        &self,
        original: &Credential,
        registered: Registered,
        patch: &ck_exec::codex_refresh::RefreshPatch,
    ) -> Result<CodexAdmission, PatchError> {
        let id = original.id.as_str();
        let mut command = Some(self.credential_command()?);
        let committed = loop {
            let revision = self.get(id).ok_or(PatchError::NotFound)?.revision;
            let mut discarded = None;
            // The registration and version are checked here, not by the writer: a
            // moved version is a discard decision, not an edit to retry on. The
            // writer's gate holds off registrations, its lock this credential's writes.
            let admitted = self.write_admitted(
                &mut command,
                (id, revision),
                None,
                false,
                |current, command| {
                    let now = self.registered(id);
                    if now.registration != registered.registration {
                        return Err(PatchError::NotFound);
                    }
                    let moved = now.credential_version != registered.credential_version;
                    if (moved || credentials_changed(original, current)) && !patch.can_apply_to(&current.metadata) {
                        discarded = Some(current.clone());
                        return Ok(None);
                    }
                    let Source::File(path) = &current.source else {
                        return Err(PatchError::ConfigBacked);
                    };
                    let out = save_codex_refresh(command.as_mut().ok_or(PatchError::ConfigBacked)?, path, patch)?;
                    let Ok(Value::Object(metadata)) = serde_json::from_slice::<Value>(&out) else {
                        unreachable!("Go's encoding of a JSON object");
                    };
                    let mut next = Credential::clone(current);
                    next.metadata = metadata;
                    next.refresh_derived();
                    publish_metadata_proxy(&mut next);
                    Ok(Some((None, out, next)))
                },
                |inner, index, bytes, mut next| {
                    inner.generation += 1;
                    next.revision = inner.generation;
                    self.note_provenance(&next.id, next.revision, sha256(bytes), None);
                    let next = Arc::new(next);
                    self.register(std::slice::from_ref(&inner.creds[index]), std::slice::from_ref(&next));
                    if credentials_changed(original, &next) {
                        self.unauthorized
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .refreshed
                            .insert(id.to_owned(), (next.revision, Instant::now()));
                    }
                    inner.creds[index] = next;
                    inner.epoch += 1;
                    self.changed.send_replace(());
                    inner.creds[index].clone()
                },
            );
            match admitted {
                // Edited in memory meanwhile: decide again on the edit.
                Err(PatchError::Stale { .. }) => continue,
                Ok(None) => {
                    let current = discarded.expect("only a discard writes nothing");
                    return Ok(CodexAdmission::Discarded(current));
                }
                other => break other?.expect("checked above"),
            }
        };
        if credentials_changed(original, &committed) {
            self.recover_unauthorized(id);
        }
        Ok(CodexAdmission::Committed(committed))
    }

    /// Changes whenever a 401 schedules a background refresh.
    pub(crate) fn subscribe_unauthorized(&self) -> tokio::sync::watch::Receiver<()> {
        self.unauthorized_noted.subscribe()
    }

    /// Changes whenever the credential set or any credential changes.
    pub fn epoch(&self) -> u64 {
        self.read().epoch
    }

    /// Attempts finished so far, by outcome.
    pub fn stats(&self) -> AttemptStats {
        let get = |i: usize| self.stats[i].load(Ordering::Relaxed);
        AttemptStats {
            success: get(0),
            failure: get(1),
            cancelled: get(2),
        }
    }

    /// Serializes preparation of one credential across concurrent requests.
    pub fn prepare_lock(&self, id: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.prepare_locks.lock().unwrap_or_else(PoisonError::into_inner);
        locks.entry(id.to_owned()).or_default().clone()
    }

    /// Applies a metadata change if the credential is still at `expected_revision`.
    /// File-backed credentials are written atomically (0600) before memory changes.
    /// Blocking: call from `spawn_blocking` in async code.
    pub fn apply_patch(
        &self,
        id: &str,
        expected_revision: u64,
        patch: &MetadataPatch,
    ) -> Result<Arc<Credential>, PatchError> {
        self.apply_patch_with(id, expected_revision, None, patch, |_| {})
    }

    /// [`Self::apply_patch`], with `edit` applied to the credential before it is
    /// published (fields outside the document),
    /// and a token refresh's `fence` ([`Self::apply_refresh`]).
    fn apply_patch_with(
        &self,
        id: &str,
        expected_revision: u64,
        fence: Option<RefreshFence>,
        patch: &MetadataPatch,
        edit: impl FnOnce(&mut Credential),
    ) -> Result<Arc<Credential>, PatchError> {
        let mut command = Some(self.credential_command()?);
        self.apply_patch_admitted(&mut command, (id, expected_revision), fence, patch, edit, |_| false)
            .map(|(credential, _)| credential)
    }

    fn apply_patch_admitted(
        &self,
        command: &mut Option<crate::store::credential_store::CredentialCommand<'_>>,
        (id, expected_revision): (&str, u64),
        fence: Option<RefreshFence>,
        patch: &MetadataPatch,
        edit: impl FnOnce(&mut Credential),
        rotated: impl FnOnce(&Credential) -> bool,
    ) -> Result<(Arc<Credential>, bool), PatchError> {
        let committed = self.write_admitted(
            command,
            (id, expected_revision),
            fence.as_ref().map(|f| f.registered),
            false,
            |current, _command| {
                if patch.remove.iter().any(|k| k == "type")
                    || patch
                        .set
                        .get("type")
                        .is_some_and(|t| t.as_str() != Some(current.provider.as_str()))
                {
                    return Err(PatchError::TypeIsImmutable);
                }
                let Source::File(path) = &current.source else {
                    return Err(PatchError::ConfigBacked);
                };
                let mut next = Credential::clone(current);
                if let Some(command) = _command.as_ref().filter(|command| command.preserves_source(path)) {
                    let bytes = command
                        .read(path)
                        .map_err(|e| PatchError::Io(e.to_string()))?
                        .ok_or(PatchError::NotFound)?;
                    next.metadata = serde_json::from_slice(&bytes).map_err(|e| PatchError::Io(e.to_string()))?;
                }
                patch.apply(&mut next.metadata);
                next.refresh_derived();
                edit(&mut next);
                // Go `FileTokenStore.Save` always records the disabled state.
                next.metadata.insert("disabled".into(), Value::Bool(next.disabled));
                // Go's `json.NewEncoder(f).Encode`: compact JSON and a newline.
                let mut bytes = serde_json::to_vec(&next.metadata).map_err(|e| PatchError::Io(e.to_string()))?;
                bytes.push(b'\n');
                let basis = if _command.as_ref().is_some_and(|command| command.preserves_source(path)) {
                    ck_wire::json::GoValue::parse(&bytes).map(|doc| (doc, true))
                } else {
                    self.patched_doc(current, path, patch, next.disabled)
                };
                let rotated = rotated(&next);
                Ok(Some((Some(path.clone()), bytes, (next, basis, rotated))))
            },
            |inner, index, bytes, (mut next, basis, rotated)| {
                inner.generation += 1;
                next.revision = inner.generation;
                // The written bytes are serde's, not Go's: a later management save takes
                // its numbers from `doc`, Go's metadata after the refresh, or refuses.
                match basis {
                    Some((doc, loaded)) => {
                        let provenance = Provenance {
                            revision: next.revision,
                            sha256: sha256(bytes),
                            doc: Some(Arc::new(doc)),
                            loaded,
                        };
                        self.provenance
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .insert(next.id.clone(), provenance);
                    }
                    None => {
                        self.provenance
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .remove(&next.id);
                    }
                }
                let next = Arc::new(next);
                self.register(std::slice::from_ref(&inner.creds[index]), std::slice::from_ref(&next));
                if rotated {
                    self.unauthorized
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .refreshed
                        .insert(id.to_owned(), (next.revision, Instant::now()));
                }
                inner.creds[index] = next;
                inner.epoch += 1;
                self.changed.send_replace(());
                (inner.creds[index].clone(), rotated)
            },
        )?;
        Ok(committed.expect("apply_patch always writes"))
    }

    /// Go 8a01bc02 (`persistLocked`): writes a file-backed credential with `inner`
    /// unlocked, so a slow disk never stalls selection or reads of other credentials.
    /// `prepare` sees the credential at `expected_revision` (`Stale` otherwise) and
    /// returns the file to write (`None` to write nothing) and the bytes, or `None` for
    /// nothing to do; `publish` then
    /// puts the result in place under the write lock. The credential's write lock and
    /// the reload gate (shared) are held throughout, so neither another write of it nor
    /// a set replacement can land between the write and its publication. A publication
    /// that touches another credential (an identity change) passes `exclusive` and holds
    /// the gate exclusively instead, so no write of any credential is in flight. A token
    /// refresh passes what it started under: it is refused (`NotFound`) once the
    /// credential's ID was registered again, before anything is merged, and meets a
    /// credential version moved since as an edit (`Stale`).
    fn write_unlocked<T, R>(
        &self,
        id: &str,
        expected_revision: u64,
        registered: Option<Registered>,
        exclusive: bool,
        prepare: impl FnOnce(
            &Arc<Credential>,
            &mut Option<crate::store::credential_store::CredentialCommand<'_>>,
        ) -> Result<Option<(Option<std::path::PathBuf>, Vec<u8>, T)>, PatchError>,
        publish: impl FnOnce(&mut Inner, usize, &[u8], T) -> R,
    ) -> Result<Option<R>, PatchError> {
        let mut command = Some(self.credential_command()?);
        self.write_admitted(
            &mut command,
            (id, expected_revision),
            registered,
            exclusive,
            prepare,
            publish,
        )
    }

    fn write_admitted<T, R>(
        &self,
        command: &mut Option<crate::store::credential_store::CredentialCommand<'_>>,
        (id, expected_revision): (&str, u64),
        registered: Option<Registered>,
        exclusive: bool,
        prepare: impl FnOnce(
            &Arc<Credential>,
            &mut Option<crate::store::credential_store::CredentialCommand<'_>>,
        ) -> Result<Option<(Option<std::path::PathBuf>, Vec<u8>, T)>, PatchError>,
        publish: impl FnOnce(&mut Inner, usize, &[u8], T) -> R,
    ) -> Result<Option<R>, PatchError> {
        // Acquired once, before the check and the write: re-taking the gate while holding
        // it can deadlock behind a waiting reload.
        let lock = (!exclusive).then(|| {
            let mut locks = self.write_locks.lock().unwrap_or_else(PoisonError::into_inner);
            locks.entry(id.to_owned()).or_default().clone()
        });
        let _gate = if exclusive {
            (None, Some(self.reload.write().unwrap_or_else(PoisonError::into_inner)))
        } else {
            (Some(self.reload.read().unwrap_or_else(PoisonError::into_inner)), None)
        };
        let _own = lock.as_ref().map(|l| l.lock().unwrap_or_else(PoisonError::into_inner));
        let current = self
            .read()
            .creds
            .iter()
            .find(|c| c.id == id)
            .cloned()
            .ok_or(PatchError::NotFound)?;
        // Go `updateInternal`: "stale registration epoch" for a refresh, ahead of the
        // revision check. The gate holds off every set replacement.
        if let Some(registered) = registered {
            let now = self.registered(id);
            if now.registration != registered.registration {
                return Err(PatchError::NotFound);
            }
            if now.credential_version != registered.credential_version {
                return Err(PatchError::Stale {
                    current: current.revision,
                });
            }
        }
        if current.revision != expected_revision {
            return Err(PatchError::Stale {
                current: current.revision,
            });
        }
        let Some((path, bytes, value)) = prepare(&current, command)? else {
            return Ok(None);
        };
        if let Some(path) = path {
            if self.grant_blocks(&current) && !command.as_ref().is_some_and(|command| command.preserves_source(&path)) {
                return Err(PatchError::Io(
                    "credential source requires recovery adoption before editing".into(),
                ));
            }
            if let Some(command) = command.as_mut() {
                command.defer_grant_publication();
            }
            self.save_document(command, &path, &bytes)?;
            // Required projections can take reload.write and re-admit held results.
            // Retention precedes releasing these guards; publication never runs under them.
            if command.as_ref().is_some_and(|command| command.is_grant()) {
                drop(_own);
                drop(_gate);
                let command = command.as_mut().unwrap();
                command
                    .resolve_grant_publication()
                    .map_err(|e| PatchError::Io(e.to_string()))?;
                command.restore(&path).map_err(|e| PatchError::Io(e.to_string()))?;
                let _gate = if exclusive {
                    (None, Some(self.reload.write().unwrap_or_else(PoisonError::into_inner)))
                } else {
                    (Some(self.reload.read().unwrap_or_else(PoisonError::into_inner)), None)
                };
                let _own = lock.as_ref().map(|l| l.lock().unwrap_or_else(PoisonError::into_inner));
                let mut inner = self.inner.write().unwrap_or_else(PoisonError::into_inner);
                let index = inner
                    .creds
                    .iter()
                    .position(|c| c.id == id && c.revision == expected_revision)
                    .ok_or(PatchError::NotFound)?;
                if registered.is_some_and(|registered| self.registered(id) != registered) {
                    return Err(PatchError::NotFound);
                }
                return Ok(Some(publish(&mut inner, index, &bytes, value)));
            }
        }
        let mut inner = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        // Every other change to this credential waits on the locks held here.
        let index = inner
            .creds
            .iter()
            .position(|c| c.id == id && c.revision == expected_revision)
            .ok_or(PatchError::NotFound)?;
        Ok(Some(publish(&mut inner, index, &bytes, value)))
    }

    /// Replaces a config-backed or runtime-only credential in memory if it is still at
    /// `expected_revision`: Go's `Manager.Update` never persists config API keys (the
    /// next config publish re-synthesizes them) or `runtime_only` auths. `NotFound`
    /// unless `next.id` names such a credential.
    pub fn replace_config_backed(
        &self,
        next: Credential,
        expected_revision: u64,
    ) -> Result<Arc<Credential>, PatchError> {
        let mut inner = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        let generation = inner.generation + 1;
        let slot = inner
            .creds
            .iter_mut()
            .find(|c| c.id == next.id && matches!(c.source, Source::Config { .. } | Source::Runtime))
            .ok_or(PatchError::NotFound)?;
        if slot.revision != expected_revision {
            return Err(PatchError::Stale { current: slot.revision });
        }
        let mut next = next;
        next.source = slot.source.clone();
        next.revision = generation;
        let next = Arc::new(next);
        self.register(std::slice::from_ref(slot), std::slice::from_ref(&next));
        let cleared = self.clear_replaced_unauthorized(std::slice::from_ref(slot), std::slice::from_ref(&next));
        *slot = next;
        let committed = slot.clone();
        inner.generation = generation;
        // Registrations depend on the credential (prefix, models): invalidate the
        // registry cache like `apply_patch` does.
        inner.epoch += 1;
        self.changed.send_replace(());
        drop(inner);
        if cleared {
            self.persist_cooldowns();
        }
        // Go `Manager.Update` clears the cooldowns of an auth it stores disabled (or
        // with cooling off), so a later re-enable routes to it at once.
        self.clear_disabled_cooldowns();
        Ok(committed)
    }

    /// Go `Manager.Modify` for an auth file: while `id` is still at `expected_revision`,
    /// writes `bytes` to its file and puts `next` (the credential re-synthesized from
    /// those bytes, possibly under a new provider or ID; `None` drops it) in its place.
    /// One write lock covers the check, the file and the swap, so a concurrent token
    /// refresh ([`Self::apply_patch`]) lands either before (`Stale`) or after, never
    /// under it, and nothing else in the set is re-read or replaced.
    pub fn replace_file_backed(
        &self,
        id: &str,
        expected_revision: u64,
        bytes: &[u8],
        next: Option<Credential>,
    ) -> Result<(), PatchError> {
        let commit = Commit {
            bytes: bytes.to_vec(),
            write: true,
            disk: None,
            doc: None,
        };
        self.commit_file_backed(id, expected_revision, commit, next)
    }

    /// [`Self::replace_file_backed`] for a management save ([`Commit`]): the file check
    /// and the optional write run under the same lock and revision check, and the new
    /// revision records the save's [`Provenance`].
    pub fn commit_file_backed(
        &self,
        id: &str,
        expected_revision: u64,
        commit: Commit,
        next: Option<Credential>,
    ) -> Result<(), PatchError> {
        let mut command = Some(self.credential_command()?);
        self.commit_file_backed_admitted(&mut command, id, expected_revision, commit, next)
    }

    pub(crate) fn commit_file_backed_admitted(
        &self,
        command: &mut Option<crate::store::credential_store::CredentialCommand<'_>>,
        id: &str,
        expected_revision: u64,
        commit: Commit,
        next: Option<Credential>,
    ) -> Result<(), PatchError> {
        let Commit {
            bytes,
            write,
            disk,
            doc,
        } = commit;
        // A new ID can replace another credential, whose own write lock is not held here.
        let exclusive = next.as_ref().is_some_and(|n| n.id != id);
        let removed = self.write_admitted(
            command,
            (id, expected_revision),
            None,
            exclusive,
            |current, command| {
                let Source::File(path) = &current.source else {
                    return Err(PatchError::ConfigBacked);
                };
                let command = command.as_mut().ok_or(PatchError::ConfigBacked)?;
                if !command.is_grant() && self.grant_blocks(current) {
                    return Err(PatchError::Io(
                        "credential source requires recovery adoption before editing".into(),
                    ));
                }
                let (bytes, expected) = if write {
                    (bytes, disk)
                } else {
                    let original = command
                        .read(path)
                        .map_err(|e| PatchError::Io(e.to_string()))?
                        .ok_or(PatchError::NotFound)?;
                    let expected = disk.or_else(|| Some(Some(sha256(&original))));
                    (original, expected)
                };
                let saved = match expected {
                    Some(expected) => command.save_checked(path, &bytes, Some(expected)),
                    None => command.save(path, &bytes).map(|()| true),
                }
                .map_err(|e| PatchError::Io(e.to_string()))?;
                if !saved {
                    return Err(PatchError::Stale {
                        current: current.revision,
                    });
                }
                let held = Some(sha256(&bytes));
                // The SQL acknowledgement already happened, even for write=false.
                Ok(Some((None, bytes, (next, held))))
            },
            |inner, index, _, (next, held)| {
                let before = inner.creds.clone();
                let previous = inner.creds.remove(index);
                self.provenance
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove(&previous.id);
                if let Some(mut next) = next {
                    inner.generation += 1;
                    next.revision = inner.generation;
                    if let Some(held) = held {
                        self.note_provenance(&next.id, next.revision, held, doc);
                    }
                    inner.creds.retain(|c| c.id != next.id);
                    let at = index.min(inner.creds.len());
                    inner.creds.insert(at, Arc::new(next));
                }
                self.register(&before, &inner.creds);
                self.clear_replaced_unauthorized(&before, &inner.creds);
                inner.epoch += 1;
                self.changed.send_replace(());
                self.scheduler
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .reconcile(&inner.creds);
                removed_ids(&[previous], &inner.creds)
            },
        )?;
        self.close_sessions(removed.unwrap_or_default());
        self.clear_disabled_cooldowns();
        self.persist_cooldowns();
        Ok(())
    }

    /// Go's metadata after `patch` (a token refresh) lands on `current`: Go's refresh
    /// changes only the patched keys of the auth it holds. From a save, that is the saved
    /// document with the patched values as Go holds them; from a load, the loaded text
    /// (while `path` still holds it) with the patched values as written, `true`. `None`
    /// when neither is known.
    fn patched_doc(
        &self,
        current: &Credential,
        path: &std::path::Path,
        patch: &MetadataPatch,
        disabled: bool,
    ) -> Option<(ck_wire::json::GoValue, bool)> {
        use ck_wire::json::GoValue;
        let prior = self
            .provenance(&current.id)
            .filter(|p| p.revision == current.revision)?;
        let (base, loaded) = match prior.doc {
            Some(doc) => (GoValue::clone(&doc), prior.loaded),
            None => {
                let bytes = self
                    .credential_backend()
                    .read(path)
                    .ok()
                    .flatten()
                    .filter(|b| sha256(b) == prior.sha256)?;
                (GoValue::parse(&bytes)?, true)
            }
        };
        let mut doc = base.into_object().ok()?;
        for key in &patch.remove {
            doc.remove(key);
        }
        for (key, value) in &patch.set {
            doc.insert(key.clone(), go_typed(value, loaded));
        }
        doc.insert("disabled".into(), GoValue::Bool(disabled));
        Some((GoValue::Object(doc), loaded))
    }

    /// The [`Provenance`] of credential `id`, if one is known.
    pub fn provenance(&self, id: &str) -> Option<Provenance> {
        self.provenance
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(id)
            .cloned()
    }

    fn note_provenance(&self, id: &str, revision: u64, sha256: [u8; 32], doc: Option<Arc<ck_wire::json::GoValue>>) {
        self.provenance.lock().unwrap_or_else(PoisonError::into_inner).insert(
            id.to_owned(),
            Provenance {
                revision,
                sha256,
                doc,
                loaded: false,
            },
        );
    }

    /// Records that the current file credentials were loaded from the bytes `read`
    /// hashed (an auth-folder scan's [`AuthScan::read`]). A credential whose recorded
    /// save already holds those bytes at its revision keeps that save's document.
    ///
    /// [`AuthScan::read`]: ck_core::config::credentials::AuthScan
    pub fn note_loaded(&self, read: &std::collections::BTreeMap<std::path::PathBuf, [u8; 32]>) {
        let creds = self.snapshot();
        let mut provenance = self.provenance.lock().unwrap_or_else(PoisonError::into_inner);
        provenance.retain(|id, _| creds.iter().any(|c| c.id == *id));
        for c in creds.iter() {
            let Source::File(path) = &c.source else { continue };
            let Some(&hash) = read.get(path) else { continue };
            let same = provenance
                .get(&c.id)
                .is_some_and(|p| p.revision == c.revision && p.sha256 == hash);
            if !same {
                provenance.insert(
                    c.id.clone(),
                    Provenance {
                        revision: c.revision,
                        sha256: hash,
                        doc: None,
                        loaded: false,
                    },
                );
            }
        }
    }

    /// Go `Service.removeAuth` (1e37b52b): a credential that left the set ends only the
    /// upstream WebSocket sessions it serves. Runs with the set's locks released.
    fn close_sessions(&self, removed: Vec<String>) {
        if let Some(close) = self.session_closer.get() {
            for id in removed {
                close(&id);
            }
        }
    }

    /// Replaces the credential set (watcher reload, management import/delete). Unchanged
    /// credentials keep their revision; new and changed ones get fresh revisions.
    pub fn reconcile(&self, credentials: Vec<Credential>) {
        self.reconcile_scanned(credentials, &std::collections::BTreeMap::new());
    }

    /// Reconciles a scan of canonical documents. A scanned credential replaces the
    /// current one only while SQLite still holds the scanned bytes, so a concurrent
    /// refresh or management save cannot be undone. Returns rejected source paths.
    pub fn reconcile_scanned(
        &self,
        mut credentials: Vec<Credential>,
        read: &std::collections::BTreeMap<std::path::PathBuf, [u8; 32]>,
    ) -> Vec<std::path::PathBuf> {
        use sha2::{Digest, Sha256};
        let reload = self.reload.write().unwrap_or_else(PoisonError::into_inner);
        // A credential this scan synthesized has no revision yet; one kept for an
        // incomplete file is the store's own record, not built from `read`.
        let built: std::collections::HashSet<std::path::PathBuf> = credentials
            .iter()
            .filter(|c| c.revision == 0)
            .filter_map(|c| match &c.source {
                Source::File(path) => Some(path.clone()),
                _ => None,
            })
            .collect();
        let mut rejected = Vec::new();
        if !read.is_empty() {
            let current = self.snapshot();
            let by_id: HashMap<&str, &Arc<Credential>> = current.iter().map(|c| (c.id.as_str(), c)).collect();
            for cred in &mut credentials {
                let Source::File(path) = &cred.source else { continue };
                let Some(seen) = read.get(path) else { continue };
                let Some(&existing) = by_id.get(cred.id.as_str()) else {
                    continue;
                };
                if unchanged(existing, cred) {
                    continue;
                }
                let on_disk = self
                    .credential_backend()
                    .read(path)
                    .ok()
                    .flatten()
                    .filter(|data| !data.is_empty())
                    .map(|data| Sha256::digest(&data).into());
                if on_disk.as_ref() != Some(seen) {
                    rejected.push(path.clone());
                    *cred = Credential::clone(existing);
                }
            }
        }
        let mut guard = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        let inner = &mut *guard;
        let mut next = Vec::with_capacity(credentials.len());
        let mut by_id = HashMap::with_capacity(inner.creds.len());
        for c in &inner.creds {
            by_id.entry(c.id.as_str()).or_insert(c);
        }
        for mut cred in credentials {
            match by_id.get(cred.id.as_str()).filter(|c| unchanged(c, &cred)) {
                Some(&existing) => next.push(existing.clone()),
                None => {
                    inner.generation += 1;
                    cred.revision = inner.generation;
                    next.push(Arc::new(cred));
                }
            }
        }
        // Runtime-only credentials (relay sessions) have no file or config entry; they
        // stay until their session ends.
        let runtime: Vec<_> = inner
            .creds
            .iter()
            .filter(|c| c.source == Source::Runtime && !next.iter().any(|n| n.id == c.id))
            .cloned()
            .collect();
        next.extend(runtime);
        let removed = removed_ids(&inner.creds, &next);
        self.register(&inner.creds, &next);
        self.clear_replaced_unauthorized(&inner.creds, &next);
        inner.creds = next;
        inner.epoch += 1;
        self.changed.send_replace(());
        self.scheduler
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .reconcile(&inner.creds);
        drop(guard);
        // Under the gate still: no save lands between the scan's publication and this.
        let mut loaded = read.clone();
        loaded.retain(|path, _| built.contains(path) && !rejected.contains(path));
        self.note_loaded(&loaded);
        drop(reload);
        self.close_sessions(removed);
        self.clear_disabled_cooldowns();
        self.persist_cooldowns();
        rejected
    }

    /// Adds a runtime-only credential (Go's auth add for a `/v1/ws` relay session)
    /// unless one with its ID is already active. Additive API for the relay route.
    pub fn add_runtime(&self, mut credential: Credential) -> bool {
        let mut inner = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        if inner.creds.iter().any(|c| c.id == credential.id && !c.disabled) {
            return false;
        }
        let before = inner.creds.clone();
        inner.creds.retain(|c| c.id != credential.id);
        inner.generation += 1;
        credential.revision = inner.generation;
        inner.creds.push(Arc::new(credential));
        self.register(&before, &inner.creds);
        inner.epoch += 1;
        self.changed.send_replace(());
        self.scheduler
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .reconcile(&inner.creds);
        true
    }

    /// Removes a credential when its relay session ends (Go's auth delete). Additive
    /// API for the relay route.
    pub fn remove_runtime(&self, id: &str) {
        let mut inner = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        let before = inner.creds.clone();
        inner.creds.retain(|c| !(c.id == id && c.source == Source::Runtime));
        if inner.creds.len() == before.len() {
            return;
        }
        self.register(&before, &inner.creds);
        inner.epoch += 1;
        self.changed.send_replace(());
        self.scheduler
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .reconcile(&inner.creds);
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Inner> {
        self.inner.read().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Per-credential outcome counters for management views (Go `Auth.Success`/`Failed`
/// and its 20 x 10-minute recent-request ring). Additive read API.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CredentialActivity {
    pub success: u64,
    pub failed: u64,
    /// `(bucket, success, failed)` for the most recent buckets, where `bucket` is
    /// Unix seconds / 600; at most 20 entries, oldest first.
    pub recent: Vec<(i64, u64, u64)>,
}

pub const RECENT_BUCKET_SECONDS: i64 = 600;
const RECENT_BUCKETS: usize = 20;

impl CredentialStore {
    fn note_activity(&self, id: &str, success: bool) {
        let bucket = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs() as i64 / RECENT_BUCKET_SECONDS);
        let mut all = self.activity.lock().unwrap_or_else(PoisonError::into_inner);
        let entry = all.entry(id.to_owned()).or_default();
        if success {
            entry.success += 1;
        } else {
            entry.failed += 1;
        }
        match entry.recent.last_mut() {
            Some(last) if last.0 == bucket => {
                if success {
                    last.1 += 1;
                } else {
                    last.2 += 1;
                }
            }
            _ => entry.recent.push((bucket, u64::from(success), u64::from(!success))),
        }
        entry.recent.retain(|(b, _, _)| bucket - b < RECENT_BUCKETS as i64);
    }

    /// Counters for one credential; zero when it has served nothing yet.
    pub fn activity(&self, id: &str) -> CredentialActivity {
        self.activity
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(id)
            .cloned()
            .unwrap_or_default()
    }

    /// Active cooldowns of one credential.
    pub fn cooldowns(&self, id: &str) -> Vec<crate::scheduling::scheduler::CooldownState> {
        self.scheduler
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .cooldowns_of(id, self.now())
    }

    /// Clears the cooldowns of one credential (Go `Manager.ResetQuota`); returns the
    /// model keys that were cooling.
    pub fn reset_cooldowns(&self, id: &str) -> Vec<String> {
        let had = !self.cooldowns(id).is_empty();
        let models = self
            .scheduler
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .reset_cooldowns(id);
        if had {
            // Go `ResetQuota` persists the cleared state.
            self.persist_cooldowns();
        }
        models
    }

    /// The recovery Go runs after a token refresh and after `BindCodexSIWC` saves a
    /// binding: an enabled credential's unauthorized cooldowns are cleared and persisted,
    /// live quota blocks stay. Go also forgets the rejected access token; here a pending
    /// unauthorized refresh keys on the revision, which the new tokens replaced.
    pub fn recover_unauthorized(&self, id: &str) {
        if self.get(id).is_none_or(|c| c.disabled) {
            return;
        }
        let cleared = self
            .scheduler
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear_unauthorized(id, Instant::now());
        if cleared {
            self.persist_cooldowns();
        }
    }
}

impl Runtime {
    /// Runs the executor's single-flighted preparation now, due or not (management
    /// refresh, Go `RefreshAuthFile`). Returns the committed credential.
    pub async fn refresh_credential(&self, id: &str) -> Result<Arc<Credential>, ExecError> {
        let cfg = self.config();
        let revision = self.store.get(id).map(|c| c.revision);
        self.prepare_credential_with(id, &cfg, revision, true, None).await
    }
}

/// Commits a token refresh of credential `id` that started from `current` under
/// `fence`'s registration: `patch` is what the refresh changed, merged with any edit
/// committed since (Go `UpdateRefreshedAuth`). Blocking.
pub(crate) fn commit_refresh(
    store: &CredentialStore,
    id: &str,
    current: &Credential,
    mut fence: RefreshFence,
    patch: &MetadataPatch,
) -> Result<Arc<Credential>, PatchError> {
    let mut revision = current.revision;
    let mut attempt = patch.clone();
    loop {
        match store.apply_refresh(
            id,
            revision,
            fence.clone(),
            &attempt,
            |_| {},
            |c| credentials_changed(current, c),
        ) {
            // Edited since the snapshot (the prepare lock does not hold off other writers),
            // within the refresh's registration: the writer refuses a re-registered ID
            // first (Go "stale registration epoch"), on every retry. Go 935aa6e3: an edit
            // that changed the credentials wins and this refresh is dropped; any other edit
            // is merged with it (Go `MergeRefreshedAuth`), so a rotated refresh token is
            // not lost. Credentials changed and put back since (A→B→A) count as changed:
            // the credential version moved (Go `CredentialVersion`).
            Err(PatchError::Stale { .. }) => {
                #[cfg(test)]
                if let Some(hook) = tests::AFTER_STALE.with(std::cell::Cell::take) {
                    hook();
                }
                // The writer's check, before anything here returns: an ID registered
                // again since then fails as Go's "stale registration epoch" does,
                // whatever its credentials.
                let (latest, now) = store.get_registered(id).ok_or(PatchError::NotFound)?;
                if now.registration != fence.registered.registration {
                    return Err(PatchError::NotFound);
                }
                let (rebased, partition_applies) =
                    rebase_patch(&current.provider, &current.metadata, &latest.metadata, patch);
                // Go 7b0319fb: a Codex token partition the refresh still matches
                // applies even when the other partition changed meanwhile
                // (`CredentialRefreshPatch.CanApplyTo`).
                let moved = now.credential_version != fence.registered.credential_version;
                if (moved || credentials_changed(current, &latest)) && !partition_applies {
                    return Ok(latest);
                }
                // Merged into `latest` from here, at its credential version.
                fence.registered = now;
                attempt = rebased;
                revision = latest.revision;
                if attempt.set.is_empty() && attempt.remove.is_empty() {
                    // Nothing left to write, but the refresh happened (Go records
                    // `LastRefreshedAt` whatever the merge changes), recorded while the
                    // credential is still `latest` under the refresh's registration. An
                    // edit since goes round again: its newer revision fails the writer's
                    // check, as revisions are never reused, so the empty patch is never
                    // written.
                    let at = (revision, fence.registered.credential_version);
                    match store.record_refresh(id, fence.registered.registration, Some(at)) {
                        Err(PatchError::Stale { .. }) => continue,
                        recorded => return recorded,
                    }
                }
            }
            other => {
                let (committed, rotated) = other?;
                // Go `updateInternal` after a refresh: new tokens clear the credential's
                // unauthorized cooldowns, keeping live quota blocks (50d5fe18). Here, so a
                // cancelled caller still does.
                if rotated {
                    store.recover_unauthorized(id);
                }
                return Ok(committed);
            }
        }
    }
}

/// Go `mergeAuthContent` for a prepare patch computed from `base` (the snapshot) that
/// lost the race to another edit, `user` being the stored metadata now: the part of
/// `patch` to apply on top of `user`. A value the patch did not change keeps the edit;
/// a value both changed keeps the edit unless it is a token payload. A missing key is
/// not JSON null. Codex outer tokens move as one partition (Go `CredentialRefreshPatch`).
/// The Sign in with ChatGPT partition (`siwc`) moves the same way, inside its map. The
/// flag reports whether a refreshed Codex partition still matches the stored version.
// ponytail: no prepare patch sets `proxy_url` or `use_siwc`, so Go's dedicated merges for
// those keys are not ported; add them with the first producer.
fn rebase_patch(
    provider: &str,
    base: &Map<String, Value>,
    user: &Map<String, Value>,
    patch: &MetadataPatch,
) -> (MetadataPatch, bool) {
    // Go `codexOuterCredentialKeys` (internal/auth/codex/token.go).
    const CODEX_OUTER: [&str; 8] = [
        "id_token",
        "access_token",
        "refresh_token",
        "account_id",
        "last_refresh",
        "email",
        "expired",
        "plan_type",
    ];
    // Go `IsAuthTokenPayloadKey`.
    let token_payload = |k: &str| {
        matches!(
            k.trim().to_ascii_lowercase().as_str(),
            "access_token"
                | "refresh_token"
                | "id_token"
                | "session_id"
                | "expired"
                | "last_refresh"
                | "expires_in"
                | "timestamp"
                | "token_type"
                | "user_code"
                | "verification_uri"
                | "verification_uri_complete"
        )
    };
    let codex = provider == "codex";
    let kept_by_user = |k: &str| base.get(k) == user.get(k);
    let mut out = MetadataPatch::default();
    let mut partition_applies = false;
    for (k, v) in &patch.set {
        let outer = codex && (CODEX_OUTER.contains(&k.as_str()) || k == "siwc");
        if !outer && base.get(k) != Some(v) && (kept_by_user(k) || token_payload(k)) {
            out.set.insert(k.clone(), v.clone());
        }
    }
    out.remove = patch
        .remove
        .iter()
        .filter(|k| base.contains_key(*k) && kept_by_user(k))
        .cloned()
        .collect();
    if codex {
        // Go `CredentialRefreshPatch.Apply`: the refreshed tokens apply only while the
        // stored identity is still the one refreshed (or its result), and then only the
        // fields the refresh changed.
        let fields = |m: &Map<String, Value>, keys: &[&str]| -> Map<String, Value> {
            keys.iter()
                .filter_map(|k| m.get(*k).map(|v| ((*k).to_owned(), v.clone())))
                .collect()
        };
        let mut updated = base.clone();
        patch.apply(&mut updated);
        // Go `sameCredentialVersion` (`reflect.DeepEqual` on map reads: null is missing).
        let same = |current: &Map<String, Value>, b: &Map<String, Value>| {
            [
                "access_token",
                "refresh_token",
                "id_token",
                "account_id",
                "client_id",
                "device_id",
            ]
            .iter()
            .all(|k| current.get(*k).filter(|v| !v.is_null()) == b.get(*k).filter(|v| !v.is_null()))
        };
        // The changed fields of one partition, when `current` still holds the version
        // refreshed or its result.
        let changes = |current: &Map<String, Value>, before: &Map<String, Value>, after: &Map<String, Value>| {
            [before, after].into_iter().find(|b| same(current, b)).map(|baseline| {
                after
                    .iter()
                    .filter(|(k, v)| baseline.get(*k) != Some(*v))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect::<Map<String, Value>>()
            })
        };
        let (before, after) = (fields(base, &CODEX_OUTER), fields(&updated, &CODEX_OUTER));
        if before != after
            && let Some(changed) = changes(user, &before, &after)
        {
            partition_applies = true;
            out.set.extend(changed);
        }
        // Go `codexSIWCCredentialKeys`.
        const CODEX_SIWC: [&str; 9] = [
            "access_token",
            "refresh_token",
            "id_token",
            "client_id",
            "device_id",
            "expired",
            "last_refresh",
            "scope",
            "scopes",
        ];
        let siwc = |m: &Map<String, Value>| m.get("siwc").and_then(Value::as_object).cloned().unwrap_or_default();
        let (before, after) = (fields(&siwc(base), &CODEX_SIWC), fields(&siwc(&updated), &CODEX_SIWC));
        let current = siwc(user);
        if before != after
            && let Some(changed) = changes(&current, &before, &after)
        {
            partition_applies = true;
            if !changed.is_empty() {
                let mut merged = current;
                merged.extend(changed);
                out.set.insert("siwc".into(), Value::Object(merged));
            }
        }
    }
    (out, partition_applies)
}

/// Go `CodexExecutor.Refresh` for credential `current`, whose file is `path`, as one
/// transaction: the file's credential lock (`<file>.lock`, shared with Go and
/// `--siwc-bind`) is taken, the file read under it, the grants sent from the tokens it
/// holds ([`ck_exec::codex_refresh::Refresher::refresh_locked`]), their result written
/// onto the file as read again under the same lock when a grant was attempted
/// ([`codex_patched`]), and then admitted to the store, which writes the file again,
/// or discarded ([`CredentialStore::admit_codex_refresh`]), before the lock is released. A spawned
/// task owns it all, the preparation lock (`guard`) included: a cancelled caller cannot
/// release the lock while a grant is in flight and leave its new tokens unwritten.
#[allow(clippy::too_many_arguments)]
async fn refresh_codex_file(
    refreshes: &CodexRefreshes,
    store: Arc<CredentialStore>,
    refresher: ck_exec::codex_refresh::Refresher,
    guard: tokio::sync::OwnedMutexGuard<()>,
    current: Arc<Credential>,
    registered: Registered,
    view: Arc<Credential>,
    path: std::path::PathBuf,
    mode: ck_exec::codex_refresh::RefreshMode,
    refused: Option<Arc<Credential>>,
) -> Result<Arc<Credential>, ExecError> {
    let local = |e: String| ExecError::local(500, FailureScope::Request, format!("codex executor: {e}"));
    let shutdown = refreshes.shutdown.clone();
    let work = refreshes.tasks.clone();
    let transaction = refreshes.tasks.spawn(async move {
        let _guard = guard;
        let (lock, disk) = read_locked_codex_file(&path, &shutdown, store.credential_backend())
            .await
            .map_err(local)?;
        // Recorded while the preparation lock is held ([`Runtime::prepare_credential_with`]).
        let patch = refresher.refresh_locked(&view, &disk, mode).await.inspect_err(|e| {
            if let Some(refused) = &refused {
                store.reject_refused(refused, registered.registration, e);
            }
        })?;
        if patch.is_empty() {
            return Ok(current);
        }
        let admitted = work
            .spawn_blocking(move || {
                let _lock = lock;
                // Go `refreshLocked` writes after a grant, whatever its manager then admits;
                // an adopt-only refresh reaches the file only when admitted.
                if patch.granted() {
                    let command = store.granted_command()?;
                    let bytes = command
                        .read(&path)
                        .map_err(|e| PatchError::Io(e.to_string()))?
                        .ok_or(PatchError::NotFound)?;
                    let out = codex_patched(&bytes, &patch, true).ok_or_else(|| {
                        PatchError::Io(format!("invalid credential JSON in {} before write", path.display()))
                    })?;
                    store.save_document(&mut Some(command), &path, &out)?;
                }
                #[cfg(test)]
                tests::before_admit(&path);
                store.admit_codex_refresh(&current, registered, &patch)
            })
            .await
            .map_err(|e| PatchError::Io(e.to_string()))
            .and_then(|r| r)
            .map_err(|e| {
                ExecError::local(
                    500,
                    FailureScope::Request,
                    format!("committing prepared credential: {e:?}"),
                )
            })?;
        Ok(match admitted {
            CodexAdmission::Committed(c) | CodexAdmission::Discarded(c) => c,
        })
    });
    transaction.await.map_err(|e| local(e.to_string()))?
}

/// The Codex credential document `bytes` with `patch` applied (each partition over a
/// version it may still be applied over, every other field as it is), as Go's
/// `json.Marshal` of the decoded map; `None` when it is not a JSON object. For
/// `refresh_write`, Go `refreshLocked`'s write after a grant: the document must be a
/// non-empty object and keys stay as written. Otherwise Go `PersistCodexAuthFile`'s: any
/// object, legacy keys renamed (Go `NormalizeCredentialMetadata`).
fn codex_patched(bytes: &[u8], patch: &ck_exec::codex_refresh::RefreshPatch, refresh_write: bool) -> Option<Vec<u8>> {
    use ck_wire::json::GoValue;
    // ponytail: the file is read through serde for the partition baselines, so one
    // holding invalid UTF-8 or a lone surrogate escape fails here (DIFFERENCES-FROM-GO.md).
    let target = match serde_json::from_slice::<Value>(bytes) {
        Ok(Value::Object(target)) if !(refresh_write && target.is_empty()) => target,
        _ => return None,
    };
    let mut doc = GoValue::parse_f64(bytes)?.into_object().ok()?;
    patch.apply(&target, &mut doc);
    if !refresh_write {
        let legacy: Vec<String> = doc
            .keys()
            .filter(|k| ck_core::credential::canonical_metadata_key(k) != k.as_str())
            .cloned()
            .collect();
        for key in legacy {
            let value = doc.remove(&key).expect("listed above");
            doc.entry(ck_core::credential::canonical_metadata_key(&key).to_owned())
                .or_insert(value);
        }
    }
    Some(GoValue::Object(doc).marshal())
}

/// Persists an admitted refresh under the document lock. SQLite failures abort admission.
fn save_codex_refresh(
    command: &mut crate::store::credential_store::CredentialCommand<'_>,
    path: &Path,
    patch: &ck_exec::codex_refresh::RefreshPatch,
) -> Result<Vec<u8>, PatchError> {
    let bytes = command
        .read(path)
        .map_err(|e| PatchError::Io(e.to_string()))?
        .ok_or(PatchError::NotFound)?;
    let out = codex_patched(&bytes, patch, false)
        .ok_or_else(|| PatchError::Io(format!("invalid credential JSON in {}", path.display())))?;
    command.save(path, &out).map_err(|e| PatchError::Io(e.to_string()))?;
    Ok(out)
}

/// What [`CredentialStore::admit_codex_refresh`] did with a refresh.
enum CodexAdmission {
    /// Published as the credential.
    Committed(Arc<Credential>),
    /// Dropped, nothing changed: the credential as it is.
    Discarded(Arc<Credential>),
}

/// A credential's own proxy, published from its metadata (`proxy_url`, trimmed), or
/// none when the metadata has none (Go's canonical proxy merge).
fn publish_metadata_proxy(c: &mut Credential) {
    match c.str("proxy_url").map(str::trim).filter(|p| !p.is_empty()) {
        Some(proxy) => {
            let proxy = proxy.to_owned();
            c.attributes.insert("proxy_url".into(), proxy)
        }
        None => c.attributes.remove("proxy_url"),
    };
}

/// Why a wait for a Codex credential lock ended without it.
const LOCK_WAIT_STOPPED: &str = "shutting down while waiting for the credential lock";

/// The longest pause between two attempts at a Codex credential lock.
const LOCK_POLL_CEILING: Duration = Duration::from_millis(100);

/// The credential lock of the Codex file at `path` (Go's blocking `flock`), waited for
/// without holding a thread; the wait gives up at `shutdown`.
async fn wait_codex_lock(path: &Path, shutdown: &tokio_util::sync::CancellationToken) -> Result<std::fs::File, String> {
    #[cfg(test)]
    tests::LOCK_ATTEMPTS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push(path.to_owned());
    let stopped = || LOCK_WAIT_STOPPED.to_owned();
    if shutdown.is_cancelled() {
        return Err(stopped());
    }
    let lock = ck_exec::codex_siwc_bind::open_lock_file(path)?;
    // ponytail: polled, not a blocking wait, so shutdown can stop it; up to 100 ms later
    // than Go's blocking flock once the holder releases. Upgrade: a lock wait on a
    // blocking thread that a signal interrupts.
    let mut delay = Duration::from_millis(5);
    loop {
        // Before every attempt: a shutdown during the sleep wins over a lock freed then.
        if shutdown.is_cancelled() {
            return Err(stopped());
        }
        if ck_exec::codex_siwc_bind::try_lock(&lock)? {
            return Ok(lock);
        }
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Err(stopped()),
            _ = tokio::time::sleep(delay) => {}
        }
        delay = (delay * 2).min(LOCK_POLL_CEILING);
    }
}

/// A held credential lock and the file read under it.
type LockedFile = (std::fs::File, Map<String, Value>);

/// Go `refreshLocked`'s read: the Codex credential file under its lock, which the
/// returned file holds. A file that is gone, or that is not a non-empty JSON object,
/// fails before anything refreshes. The wait for the lock gives up at `shutdown`.
async fn read_locked_codex_file(
    path: &Path,
    shutdown: &tokio_util::sync::CancellationToken,
    backend: Arc<crate::store::credential_store::CredentialDb>,
) -> Result<LockedFile, String> {
    let lock = wait_codex_lock(path, shutdown).await?;
    backend
        .restore(path)
        .map_err(|e| format!("restore credential file {}: {e}", path.display()))?;
    let data = backend
        .read(path)
        .map_err(|e| format!("read credential document {}: {e}", path.display()))?
        .ok_or_else(|| format!("credential document {} not found", path.display()))?;
    match serde_json::from_slice(&data) {
        Ok(Value::Object(disk)) if !disk.is_empty() => Ok((lock, disk)),
        _ => Err(format!("invalid credential JSON in {}", path.display())),
    }
}

/// Go `CredentialsChanged`: the tokens or API key differ.
pub(crate) fn credentials_changed(a: &Credential, b: &Credential) -> bool {
    let text = |c: &Credential, keys: &[&str]| {
        keys.iter()
            .find_map(|k| {
                c.metadata
                    .get(*k)
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
            })
            .unwrap_or_default()
            .to_owned()
    };
    let siwc = |c: &Credential, key: &str| {
        c.metadata
            .get("siwc")
            .and_then(|s| s.get(key))
            .and_then(Value::as_str)
            .map_or("", str::trim)
            .to_owned()
    };
    let api_key = |c: &Credential| {
        c.attributes
            .get("api_key")
            .filter(|k| !k.is_empty())
            .cloned()
            .unwrap_or_else(|| text(c, &["api_key"]))
    };
    // Go `parseBoolAny`.
    let parse_bool = |s: &str| match s.trim() {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Some(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Some(false),
        _ => None,
    };
    let use_siwc = |c: &Credential| {
        let value = c.metadata.get("use_siwc")?;
        value
            .as_bool()
            .or_else(|| parse_bool(value.as_str()?))
            .or_else(|| value.as_f64().map(|n| n != 0.0))
    };
    // Go `authAccessToken`: the token requests use, which follows `IsCodexUseSIWC` (a
    // true attribute, else the file's flag).
    let effective_access_token = |c: &Credential| {
        let attribute = c.attributes.get("use_siwc").and_then(|v| parse_bool(v)) == Some(true);
        if attribute || use_siwc(c) == Some(true) {
            siwc(c, "access_token")
        } else {
            text(c, &["access_token", "accessToken"])
        }
    };
    effective_access_token(a) != effective_access_token(b)
        || [
            &["access_token", "accessToken"][..],
            &["refresh_token", "refreshToken"],
            &["id_token", "idToken"],
        ]
        .iter()
        .any(|keys| text(a, keys) != text(b, keys))
        || api_key(a) != api_key(b)
        || ["access_token", "refresh_token", "id_token", "client_id"]
            .iter()
            .any(|k| siwc(a, k) != siwc(b, k))
        || use_siwc(a) != use_siwc(b)
}

/// A response stream that reports its lease's outcome on end, on the first error (after
/// which it yields nothing more), or as `Cancelled` when dropped early. The upstream body
/// closes before the lease ends: `inner` is declared, and so dropped, before `lease`.
pub struct Completing {
    inner: ExecStream,
    lease: Option<Lease>,
}

impl Completing {
    pub fn new(inner: ExecStream, lease: Lease) -> Self {
        Self {
            inner,
            lease: Some(lease),
        }
    }
}

impl Stream for Completing {
    type Item = Result<Bytes, ExecError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.lease.is_none() {
            return Poll::Ready(None);
        }
        let item = ready!(this.inner.poll_next_unpin(cx));
        match &item {
            Some(Ok(_)) => {}
            None => this.lease.take().unwrap().complete(Outcome::Success),
            Some(Err(e)) => {
                this.inner = Box::pin(futures_util::stream::empty());
                this.lease.take().unwrap().complete(Outcome::Failure(e.clone()));
            }
        }
        Poll::Ready(item)
    }
}

/// Go `configuredSignatureCacheEnabled` / `configuredSignatureBypassStrict`
/// (internal/api/server_reload.go): `oauth.providers.antigravity.signature-cache-enabled`
/// (default true) and `.signature-bypass-strict` (default false). Applied on every
/// publish, as Go applies them at startup and on each reload.
fn signature_cache_config(cfg: &Config) -> (bool, bool) {
    let flag = |key: &str| {
        cfg.document
            .get("oauth")
            .and_then(|o| o.get("providers"))
            .and_then(|p| p.get("antigravity"))
            .and_then(|a| a.get(key))
            .and_then(serde_yaml_ng::Value::as_bool)
    };
    (
        flag("signature-cache-enabled").unwrap_or(true),
        flag("signature-bypass-strict").unwrap_or(false),
    )
}

/// Credentials the background refresh considers: enabled auth files, never API-key
/// kinds (Go `nextRefreshCheckAt` skips `AuthKind() == apikey` for every provider).
/// The access token requests send (Go `authAccessToken`): a Sign in with ChatGPT
/// credential's own, else `access_token`, trimmed.
fn effective_access_token(c: &Credential) -> &str {
    if ck_core::registry::dynamic::uses_siwc(c) {
        return ck_exec::codex_siwc::access_token(c);
    }
    c.str("access_token").map_or("", str::trim)
}

fn refresh_candidate(c: &Credential) -> bool {
    !c.disabled && refreshable(c)
}

/// [`refresh_candidate`] whether or not `c` is disabled.
fn refreshable(c: &Credential) -> bool {
    matches!(c.source, Source::File(_)) && ck_core::registry::dynamic::auth_kind(c) != Some("apikey")
}

/// Test hook at `site` for credential `id` ([`tests::hook`]); nothing otherwise.
fn test_hook(site: &'static str, id: &str) {
    #[cfg(test)]
    tests::hook(site, id);
    let _ = (site, id);
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    thread_local! {
        /// Runs once where [`commit_refresh`] meets an edit made since its snapshot,
        /// before it reads the credential again.
        pub(super) static AFTER_STALE: std::cell::Cell<Option<Box<dyn FnOnce()>>> = const { std::cell::Cell::new(None) };
    }

    /// Hook sites ([`test_hook`]) armed to panic once, by site and credential ID, and how
    /// often each was reached. Process-wide: refresh jobs run on any worker thread.
    static PANICS: Mutex<Vec<(&'static str, String)>> = Mutex::new(Vec::new());
    static REACHED: Mutex<Vec<(&'static str, String)>> = Mutex::new(Vec::new());

    pub(super) fn hook(site: &'static str, id: &str) {
        REACHED.lock().unwrap().push((site, id.to_owned()));
        let mut panics = PANICS.lock().unwrap();
        if let Some(i) = panics.iter().position(|(s, i)| *s == site && i == id) {
            panics.remove(i);
            drop(panics);
            panic!("injected {site} panic");
        }
    }

    /// The next time `site` is reached for credential `id`, it panics.
    fn arm_panic(site: &'static str, id: &str) {
        PANICS.lock().unwrap().push((site, id.to_owned()));
    }

    /// How often `site` was reached for credential `id`.
    fn reached(site: &'static str, id: &str) -> usize {
        REACHED
            .lock()
            .unwrap()
            .iter()
            .filter(|(s, i)| *s == site && i == id)
            .count()
    }

    /// Every Codex credential file a refresh or management write waited to lock, in
    /// order (all tests).
    pub(crate) static LOCK_ATTEMPTS: Mutex<Vec<std::path::PathBuf>> = Mutex::new(Vec::new());

    /// Waits until something waited to lock the Codex file at `path`.
    pub(crate) async fn lock_waited(path: &Path) {
        let waited = async {
            while !LOCK_ATTEMPTS.lock().unwrap().iter().any(|p| p == path) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(5), waited)
            .await
            .expect("something waits for the credential lock");
    }

    type AdmitHook = (std::path::PathBuf, Box<dyn FnOnce() + Send>);

    /// Runs once, for its file, where a Codex refresh is about to be admitted.
    static BEFORE_ADMIT: Mutex<Vec<AdmitHook>> = Mutex::new(Vec::new());

    /// Runs once, for its file, after a refresh read the stored document it will adopt
    /// and before it takes the credential writer to adopt it.
    static BEFORE_RESTORE: Mutex<Vec<AdmitHook>> = Mutex::new(Vec::new());

    pub(super) fn before_admit(path: &Path) {
        run_hook(&BEFORE_ADMIT, path);
    }

    pub(super) fn before_restore(path: &Path) {
        run_hook(&BEFORE_RESTORE, path);
    }

    fn run_hook(hooks: &Mutex<Vec<AdmitHook>>, path: &Path) {
        let hook = {
            let mut hooks = hooks.lock().unwrap();
            hooks.iter().position(|(p, _)| p == path).map(|i| hooks.remove(i).1)
        };
        if let Some(hook) = hook {
            hook();
        }
    }

    /// Go checks a refresh's `RegistrationEpoch` before anything else in
    /// `updateInternal`: a refresh that meets an edit, after which its ID is registered
    /// again with other tokens, fails as a stale registration, not as a refresh dropped
    /// for changed credentials (which hands the caller the new credential).
    #[test]
    fn a_refresh_meeting_an_edit_then_a_new_registration_fails_as_stale() {
        let dir = std::env::temp_dir().join(format!("cooker-refresh-stale-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let credential = {
            let dir = dir.clone();
            move |token: &str| {
                let metadata = serde_json::json!({"type":"pp","access_token":token});
                let path = dir.join("a.json");
                std::fs::write(&path, metadata.to_string()).unwrap();
                Credential::from_file(&dir, &path, metadata.as_object().cloned().unwrap()).unwrap()
            }
        };
        let store = Arc::new(test_store(vec![credential("a")]));
        let (current, registration) = store.get_registered("a.json").unwrap();
        // An unrelated edit under the same registration: the refresh meets it as stale.
        let mut note = MetadataPatch::default();
        note.set.insert("note".into(), "edited".into());
        store.apply_patch("a.json", current.revision, &note).unwrap();
        AFTER_STALE.with(|hook| {
            hook.set(Some(Box::new({
                let (store, credential) = (store.clone(), credential.clone());
                move || {
                    store.reconcile(vec![]);
                    store.reconcile(vec![credential("b")]);
                }
            })))
        });
        let mut patch = MetadataPatch::default();
        patch.set.insert("access_token".into(), "p2".into());
        let fence = RefreshFence {
            registered: registration,
        };
        let committed = commit_refresh(&store, "a.json", &current, fence, &patch);
        assert!(AFTER_STALE.with(|hook| hook.take()).is_none(), "the hook ran");
        assert!(matches!(committed, Err(PatchError::NotFound)), "{committed:?}");
        assert_eq!(store.get("a.json").unwrap().metadata["access_token"], "b");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Go `nextRefreshCheckAt` skips `AuthKind() == apikey` for every provider. The
    /// kinds are Go's `AuthKind` goldens (tests/fixtures/server_go.json `auth_kind`).
    #[test]
    fn refresh_skips_api_key_kinds_like_go() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../tests/fixtures/server_go.json")).unwrap();
        let mut kinds = std::collections::BTreeSet::new();
        for case in fixture["auth_kind"].as_array().unwrap() {
            let mut c = cred("x.json", "kimi", false);
            c.metadata = case["metadata"].as_object().cloned().unwrap_or_default();
            c.attributes = case["attributes"]
                .as_object()
                .map(|a| {
                    a.iter()
                        .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_owned()))
                        .collect()
                })
                .unwrap_or_default();
            let kind = case["kind"].as_str().unwrap();
            kinds.insert(kind);
            assert_eq!(refresh_candidate(&c), kind != "apikey", "case {case}");
            c.disabled = true;
            assert!(!refresh_candidate(&c));
        }
        assert!(kinds.contains("apikey") && kinds.contains("oauth"), "{kinds:?}");
    }

    #[test]
    fn signature_cache_config_reads_antigravity_settings() {
        let at = |yaml: &str| signature_cache_config(&Config::parse(yaml).unwrap());
        assert_eq!(at("{}\n"), (true, false));
        assert_eq!(
            at("oauth: {providers: {antigravity: {signature-cache-enabled: false, signature-bypass-strict: true}}}\n"),
            (false, true)
        );
        assert_eq!(
            at("oauth: {providers: {antigravity: {signature-cache-enabled: null}}}\n"),
            (true, false)
        );
    }

    fn cred(id: &str, provider: &str, disabled: bool) -> Credential {
        let mut metadata = Map::new();
        metadata.insert("type".into(), provider.into());
        metadata.insert("disabled".into(), disabled.into());
        thread_local! {
            static DIR: std::path::PathBuf = std::env::temp_dir().join(format!("cooker-runtime-credentials-{}", uuid::Uuid::new_v4()));
        }
        DIR.with(|dir| {
            std::fs::create_dir_all(dir).unwrap();
            Credential::from_file(dir, &dir.join(id), metadata).unwrap()
        })
    }

    /// The registration map follows the set: an edit keeps the registration, a removal
    /// drops the entry, and the same ID added again starts a new registration.
    #[test]
    fn registrations_follow_the_credential_set() {
        let a = Arc::new(cred("registered.json", "claude", false));
        let mut map = HashMap::new();
        register_change(&mut map, &[], std::slice::from_ref(&a));
        let first = map[&a.id];
        let mut edited = Credential::clone(&a);
        edited.revision += 1;
        edited.disabled = true;
        let edited = Arc::new(edited);
        register_change(&mut map, std::slice::from_ref(&a), std::slice::from_ref(&edited));
        assert_eq!(map[&a.id].registration, first.registration);
        register_change(&mut map, std::slice::from_ref(&edited), &[]);
        assert!(!map.contains_key(&a.id));
        register_change(&mut map, &[], std::slice::from_ref(&a));
        assert_ne!(map[&a.id].registration, first.registration);
    }

    fn sel(provider: &str) -> Selection {
        Selection {
            provider: provider.into(),
            ..Selection::default()
        }
    }

    #[test]
    fn select_round_robins_enabled_credentials_and_honours_exclusions() {
        let store = test_store(vec![
            cred("c1.json", "claude", false),
            cred("off.json", "claude", true),
            cred("x.json", "codex", false),
            cred("c2.json", "claude", false),
        ]);
        let picks: Vec<String> = (0..4)
            .map(|_| store.select(sel("claude")).unwrap().credential.id.clone())
            .collect();
        assert_eq!(picks, ["c1.json", "c2.json", "c1.json", "c2.json"]);
        assert!(store.select(sel("gemini")).is_none());
        let only = Selection {
            exclude: vec!["c1.json".into()],
            ..sel("claude")
        };
        for _ in 0..3 {
            assert_eq!(store.select(only.clone()).unwrap().credential.id, "c2.json");
        }
    }

    /// Sequential cache invalidation of the derived registry: after a prefix change, a
    /// disable, a removal or a re-registration the next use rebuilds it, and a request
    /// result alone does not. This is not Go's publication fence (7c2133e2, 08e5762a,
    /// f75ea073): a build already in flight still returns its older snapshot.
    #[test]
    fn the_registry_follows_the_latest_credentials() {
        let rt = test_runtime(
            Config::parse("").unwrap(),
            Vec::new(),
            Executors {
                claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
                codex: Default::default(),
                devices: Default::default(),
                openai: Default::default(),
                google: Default::default(),
            },
        );
        let with = |prefix: &str, disabled: bool| {
            let mut c = cred("a.json", "claude", disabled);
            c.metadata.insert("prefix".into(), prefix.into());
            c
        };
        rt.store.reconcile(vec![with("old", false)]);
        let model = ck_core::registry::dynamic::models_for(&rt.config(), &HashMap::new(), &with("", false), 0)
            .first()
            .map(|spec| spec.id.clone())
            .expect("a Claude catalog model");
        let supports = |prefix: &str| rt.registry().client_supports("a.json", &format!("{prefix}/{model}"));
        assert!(supports("old"));
        let built = rt.registry();
        rt.store
            .select(Selection::new("claude", &model))
            .unwrap()
            .complete(Outcome::Success);
        assert!(Arc::ptr_eq(&built, &rt.registry()), "a result does not rebuild it");
        rt.store.reconcile(vec![with("new", false)]);
        assert!(supports("new") && !supports("old"), "a superseded prefix");
        rt.store.reconcile(vec![with("new", true)]);
        assert!(!rt.registry().has_client("a.json"), "disabled");
        rt.store.reconcile(vec![with("new", false)]);
        assert!(supports("new"), "enabled again");
        rt.store.reconcile(Vec::new());
        assert!(!rt.registry().has_client("a.json"), "removed");
        rt.store.reconcile(vec![with("fresh", false)]);
        assert!(supports("fresh") && !supports("new"), "re-registered");
    }

    #[test]
    fn standalone_admission_reads_attributes_first() {
        let mut c = cred("a.json", "claude", false);
        c.metadata.insert("prefix".into(), "meta".into());
        c.metadata
            .insert("excluded_models".into(), serde_json::json!(["claude-sonnet*"]));
        c.attributes.insert("prefix".into(), "team".into());
        c.attributes.insert("excluded_models".into(), "claude-opus*".into());
        let policy = Policy::default();
        let admit = |model: &str| {
            let s = Selection::new("claude", model);
            standalone_admission(&s, &policy)(&c)
        };
        assert_eq!(
            admit("team/claude-sonnet-5(high)").as_deref(),
            Some("claude-sonnet-5(high)")
        );
        assert_eq!(admit("team/claude-opus-5"), None, "attribute exclusions win");
        assert_eq!(
            admit("meta/claude-sonnet-5"),
            None,
            "metadata prefix is not authoritative"
        );
        assert_eq!(admit("claude-sonnet-5").as_deref(), Some("claude-sonnet-5"));
        let forced = Policy {
            force_model_prefix: true,
            ..Policy::default()
        };
        let s = Selection::new("claude", "claude-sonnet-5");
        assert_eq!(standalone_admission(&s, &forced)(&c), None);
        assert_eq!(standalone_admission(&Selection::new("codex", "x"), &policy)(&c), None);
    }

    fn sqlite_backend(
        dir: &Path,
    ) -> (
        Arc<crate::store::config_store::ConfigStore>,
        Arc<crate::store::credential_store::CredentialDb>,
    ) {
        let path = dir.to_string_lossy();
        let database = path
            .strip_suffix(".credentials")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| dir.join("config.db"));
        let store = Arc::new(crate::testing::config_store(&database).unwrap());
        let backend = crate::store::credential_store::CredentialDb::initialize(store.clone()).unwrap();
        crate::testing::seed_credentials(&backend, dir);
        (store, backend)
    }

    /// Seeds only explicit private fixtures.
    fn private_backend(credentials: &[Credential]) -> Arc<crate::store::credential_store::CredentialDb> {
        let dir = std::env::temp_dir().join(format!("cooker-runtime-db-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let (_, backend) = sqlite_backend(&dir);
        for credential in credentials {
            if let Source::File(path) = &credential.source {
                assert!(
                    path.starts_with(std::env::temp_dir()),
                    "fixtures must use private source paths: {}",
                    path.display()
                );
                let bytes = std::fs::read(path).unwrap_or_else(|_| serde_json::to_vec(&credential.metadata).unwrap());
                backend.save(path, &bytes).unwrap();
            }
        }
        backend
    }

    fn test_store(credentials: Vec<Credential>) -> Arc<CredentialStore> {
        let backend = private_backend(&credentials);
        CredentialStore::new(credentials, backend)
    }

    fn test_runtime(config: Config, credentials: Vec<Credential>, executors: Executors) -> Runtime {
        let backend = private_backend(&credentials);
        let rt = Runtime::new(config, credentials, executors, backend);
        rt.deny_external.store(true, Ordering::Relaxed);
        rt.set_local_model(true);
        rt
    }

    fn scan_private(
        backend: &crate::store::credential_store::CredentialDb,
        cfg: &Config,
    ) -> ck_core::config::credentials::AuthScan {
        backend.scan(cfg).unwrap()
    }
    fn reload_private(rt: &Runtime, dir: &Path) {
        let mut cfg = (*rt.config()).clone();
        cfg.auth_dir = dir.to_owned();
        let scan = scan_private(&rt.store.credential_backend(), &cfg);
        rt.store
            .reconcile(scan.credentials.into_iter().map(crate::testing::local).collect());
    }
    #[test]
    fn failed_sqlite_saves_never_publish_patches_equal_file_commits_or_normalization() {
        let dir = std::env::temp_dir().join(format!("cooker-runtime-sqlite-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let before = br#"{"type":"codex","access_token":"before","refresh_token":"rt"}"#;
        std::fs::write(dir.join("codex.json"), before).unwrap();
        let (database, backend) = sqlite_backend(&dir);
        let mut cfg = Config::parse("").unwrap();
        cfg.auth_dir = backend.auth_dir();
        let scan = backend.scan(&cfg).unwrap();
        let store = CredentialStore::new(scan.credentials, backend.clone());
        store.note_loaded(&scan.read);
        let original = store.get("codex.json").unwrap();
        let path = cfg.auth_dir.join("codex.json");
        let epoch = store.epoch();
        let blocked = crate::testing::block_checkpoint_install(&database);

        let mut patch = MetadataPatch::default();
        patch.set.insert("access_token".into(), "after".into());
        assert!(matches!(
            store.apply_patch(&original.id, original.revision, &patch),
            Err(PatchError::Io(_))
        ));
        assert!(Arc::ptr_eq(&original, &store.get(&original.id).unwrap()));

        let mut next = Credential::clone(&original);
        patch.apply(&mut next.metadata);
        let bytes = serde_json::to_vec(&next.metadata).unwrap();
        // Equal working bytes still need a durable acknowledgement before publication.
        std::fs::write(&path, &bytes).unwrap();
        let commit = Commit {
            disk: Some(Some(sha256(before))),
            bytes,
            write: false,
            doc: None,
        };
        assert!(matches!(
            store.commit_file_backed(&original.id, original.revision, commit, Some(next)),
            Err(PatchError::Io(_))
        ));
        assert!(matches!(store.persist_credential(&original), Err(PatchError::Io(_))));
        assert!(Arc::ptr_eq(&original, &store.get(&original.id).unwrap()));
        assert_eq!(store.epoch(), epoch);
        assert!(!store.persisted.lock().unwrap().contains_key(&original.id));
        assert_eq!(backend.read(&path).unwrap().as_deref(), Some(before.as_slice()));
        assert_eq!(std::fs::read(&path).unwrap(), before);
        drop(blocked);
        drop(store);
        drop(backend);
        drop(database);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn sqlite_file_commit_checks_sql_cas_after_working_file_removal() {
        let dir = std::env::temp_dir().join(format!("cooker-runtime-cas-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let before = br#"{"type":"codex","access_token":"before"}"#;
        std::fs::write(dir.join("codex.json"), before).unwrap();
        let (database, backend) = sqlite_backend(&dir);
        let mut cfg = Config::parse("").unwrap();
        cfg.auth_dir = backend.auth_dir();
        let store = CredentialStore::new(ck_core::config::credentials::from_auth_dir(&cfg), backend.clone());
        let original = store.get("codex.json").unwrap();
        let path = cfg.auth_dir.join("codex.json");
        std::fs::remove_file(&path).unwrap();
        let mut next = Credential::clone(&original);
        next.metadata.insert("note".into(), "saved".into());
        let bytes = serde_json::to_vec(&next.metadata).unwrap();
        let commit = Commit {
            disk: Some(Some(sha256(before))),
            bytes: bytes.clone(),
            write: true,
            doc: None,
        };
        store
            .commit_file_backed(&original.id, original.revision, commit, Some(next))
            .unwrap();
        assert_eq!(backend.read(&path).unwrap().as_deref(), Some(bytes.as_slice()));
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        let published = store.get("codex.json").unwrap();
        assert!(published.revision > original.revision);
        assert_eq!(published.metadata["note"], "saved");
        let external = br#"{"type":"codex","access_token":"external"}"#;
        backend.save(&path, external).unwrap();
        let commit = Commit {
            disk: Some(Some(sha256(&bytes))),
            bytes,
            write: true,
            doc: None,
        };
        assert!(matches!(
            store.commit_file_backed(
                &published.id,
                published.revision,
                commit,
                Some(Credential::clone(&published))
            ),
            Err(PatchError::Stale { .. })
        ));
        assert!(Arc::ptr_eq(&published, &store.get("codex.json").unwrap()));
        assert_eq!(backend.read(&path).unwrap().as_deref(), Some(external.as_slice()));
        drop(store);
        drop(backend);
        drop(database);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn sqlite_equal_file_commit_preserves_raw_sql_bytes_after_working_file_removal() {
        let dir = std::env::temp_dir().join(format!("cooker-runtime-raw-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let before = b"{\n  \"type\": \"codex\",\n  \"access_token\": \"before\"\n}\n";
        let document: Value = serde_json::from_slice(before).unwrap();
        let compact = serde_json::to_vec(&document).unwrap();
        assert_ne!(compact, before);
        assert_eq!(serde_json::from_slice::<Value>(&compact).unwrap(), document);
        std::fs::write(dir.join("codex.json"), before).unwrap();
        let (database, backend) = sqlite_backend(&dir);
        let mut cfg = Config::parse("").unwrap();
        cfg.auth_dir = backend.auth_dir();
        let store = CredentialStore::new(ck_core::config::credentials::from_auth_dir(&cfg), backend.clone());
        let original = store.get("codex.json").unwrap();
        let epoch = store.epoch();
        let path = cfg.auth_dir.join("codex.json");
        std::fs::remove_file(&path).unwrap();
        let commit = Commit {
            disk: Some(Some(sha256(before))),
            bytes: compact,
            write: false,
            doc: None,
        };
        store
            .commit_file_backed(
                &original.id,
                original.revision,
                commit,
                Some(Credential::clone(&original)),
            )
            .unwrap();

        assert_eq!(backend.read(&path).unwrap().as_deref(), Some(before.as_slice()));
        assert_eq!(std::fs::read(&path).unwrap(), before);
        let published = store.get("codex.json").unwrap();
        assert!(published.revision > original.revision);
        assert_eq!(published.metadata, original.metadata);
        assert_eq!(store.epoch(), epoch + 1);
        let provenance = store.provenance(&published.id).unwrap();
        assert_eq!(provenance.revision, published.revision);
        assert_eq!(provenance.sha256, sha256(before));
        drop(store);
        drop(backend);
        drop(database);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn patch_persists_atomically_preserves_unknown_fields_and_rejects_stale() {
        let dir = std::env::temp_dir().join(format!("cooker-store-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("claude-a.json");
        std::fs::write(
            &path,
            r#"{"type":"claude","access_token":"old","zz_unknown":{"k":[1,2]}}"#,
        )
        .unwrap();
        // A stale temp-file-looking path and a symlink must not be reused or followed.
        let victim = dir.join("victim");
        std::fs::write(&victim, "keep").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(
            &victim,
            dir.join(format!(".claude-a.json.{}.0.tmp", std::process::id())),
        )
        .unwrap();
        let mut cfg = ck_core::config::Config::parse("").unwrap();
        cfg.auth_dir = dir.clone();
        let store = test_store(ck_core::config::credentials::from_auth_dir(&cfg));
        let rev = store.get("claude-a.json").unwrap().revision;

        let mut patch = MetadataPatch::default();
        patch.set.insert("access_token".into(), "new".into());
        patch.set.insert("email".into(), "a@x.test".into());
        patch.set.insert("disabled".into(), true.into());
        let updated = store.apply_patch("claude-a.json", rev, &patch).unwrap();
        assert!(updated.revision > rev);
        assert!(updated.disabled);
        assert_eq!(updated.label, "a@x.test", "derived label follows the new email");
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "{\"type\":\"claude\",\"access_token\":\"new\",\"zz_unknown\":{\"k\":[1,2]},\"email\":\"a@x.test\",\"disabled\":true}\n"
        );
        #[cfg(unix)]
        assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
        assert_eq!(
            std::fs::read_to_string(&victim).unwrap(),
            "keep",
            "symlink target untouched"
        );
        assert!(
            store.select(sel("claude")).is_none(),
            "patched-disabled credential leaves rotation"
        );

        assert_eq!(
            store.apply_patch("claude-a.json", rev, &patch).unwrap_err(),
            PatchError::Stale {
                current: updated.revision
            }
        );
        assert_eq!(
            store.apply_patch("nope.json", 0, &patch).unwrap_err(),
            PatchError::NotFound
        );
        let mut retype = MetadataPatch::default();
        retype.set.insert("type".into(), "codex".into());
        assert_eq!(
            store
                .apply_patch("claude-a.json", updated.revision, &retype)
                .unwrap_err(),
            PatchError::TypeIsImmutable
        );
        let leftovers: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .filter(|n| n.ends_with(".tmp") && !n.ends_with(".0.tmp"))
            .collect();
        assert!(leftovers.is_empty(), "no temp files left behind: {leftovers:?}");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A whole-document save of one credential running alongside token refreshes of
    /// another never reinstalls a stale copy of it: every refresh finds the revision it
    /// left.
    #[test]
    fn a_document_save_never_undoes_a_concurrent_patch() {
        let dir = std::env::temp_dir().join(format!("cooker-save-patch-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("refreshed.json"), r#"{"type":"claude","refresh_token":"r0"}"#).unwrap();
        std::fs::write(dir.join("saved.json"), r#"{"type":"claude"}"#).unwrap();
        let mut cfg = ck_core::config::Config::parse("").unwrap();
        cfg.auth_dir = dir.clone();
        let store = test_store(ck_core::config::credentials::from_auth_dir(&cfg));
        let saver = {
            let store = store.clone();
            std::thread::spawn(move || {
                for n in 0..500 {
                    let current = store.get("saved.json").unwrap();
                    let mut next = Credential::clone(&current);
                    next.metadata.insert("n".into(), n.into());
                    let bytes = serde_json::to_vec(&next.metadata).unwrap();
                    store
                        .replace_file_backed("saved.json", current.revision, &bytes, Some(next))
                        .unwrap_or_else(|e| panic!("save {n} was refused: {e:?}"));
                }
            })
        };
        let mut revision = store.get("refreshed.json").unwrap().revision;
        for n in 1..=500 {
            let mut patch = MetadataPatch::default();
            patch.set.insert("refresh_token".into(), format!("r{n}").into());
            revision = store
                .apply_patch("refreshed.json", revision, &patch)
                .unwrap_or_else(|e| panic!("refresh {n} was undone: {e:?}"))
                .revision;
        }
        saver.join().unwrap();
        let refreshed = store.get("refreshed.json").unwrap();
        assert_eq!(refreshed.revision, revision);
        assert_eq!(refreshed.metadata["refresh_token"], "r500");
        assert_eq!(store.get("saved.json").unwrap().metadata["n"], 499);
        assert_eq!(store.snapshot().len(), 2);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// The next database save of `path` stops before committing its bytes,
    /// before the file changes, until the returned release is sent or dropped.
    fn stall_next_write(path: &Path) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (entered_tx, entered) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        STALLS
            .lock()
            .unwrap()
            .insert(path.to_path_buf(), (entered_tx, release_rx));
        (entered, release)
    }

    type Stall = (std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>);
    static STALLS: std::sync::LazyLock<Mutex<HashMap<std::path::PathBuf, Stall>>> =
        std::sync::LazyLock::new(Mutex::default);

    pub(super) fn stall_write(path: &Path) {
        let Some((entered, release)) = STALLS.lock().unwrap().remove(path) else {
            return;
        };
        let _ = entered.send(());
        let _ = release.recv();
    }

    /// Runs `f` on its own thread; its result, or `None` while it is still running after
    /// `wait`.
    fn spawn_bounded<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> std::sync::mpsc::Receiver<T> {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(f());
        });
        rx
    }

    const BOUND: Duration = Duration::from_secs(5);
    const BLOCKED: Duration = Duration::from_millis(300);

    fn stalled_store(
        name: &str,
        files: &[&str],
    ) -> (std::path::PathBuf, ck_core::config::Config, Arc<CredentialStore>) {
        let dir = std::env::temp_dir().join(format!("cooker-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        for file in files {
            std::fs::write(dir.join(file), r#"{"type":"claude","access_token":"old"}"#).unwrap();
        }
        let mut cfg = ck_core::config::Config::parse("").unwrap();
        cfg.auth_dir = dir.clone();
        let store = test_store(ck_core::config::credentials::from_auth_dir(&cfg));
        (dir, cfg, store)
    }

    fn token_patch(token: &str) -> MetadataPatch {
        let mut patch = MetadataPatch::default();
        patch.set.insert("access_token".into(), token.into());
        patch
    }

    /// Go 8a01bc02 (`TestManagerBlockedSaveAllowsUnrelatedCredential`,
    /// `TestManagerBlockedSaveSerializesLoad`): while a refresh commit's file write is
    /// stalled on disk, snapshots and selection proceed, before and while a reload is
    /// pending; another credential's write proceeds; and neither the reload nor another
    /// write of the same credential overtakes the stalled one. The reload checks each
    /// credential against the bytes its scan read, as Go c4c1d43e's stale-snapshot check does.
    #[test]
    fn a_stalled_credential_write_blocks_commands_but_not_reads() {
        let (dir, cfg, store) = stalled_store("stalled-write", &["a.json", "b.json"]);
        let before = store.get("a.json").unwrap().revision;
        let (entered, release) = stall_next_write(&dir.join("a.json"));
        let writer = spawn_bounded({
            let store = store.clone();
            move || {
                store
                    .apply_patch("a.json", before, &token_patch("new"))
                    .map(|c| c.revision)
            }
        });
        entered.recv_timeout(BOUND).expect("the write reached the disk");
        // Leases stay alive until the end: dropping one records a result and persists.
        let mut leases = Vec::new();
        let read_and_select = |leases: &mut Vec<Lease>, when: &str| {
            let reader = spawn_bounded({
                let store = store.clone();
                move || store.snapshot().len()
            });
            assert_eq!(reader.recv_timeout(BOUND).ok(), Some(2), "a snapshot {when}");
            let selector = spawn_bounded({
                let store = store.clone();
                move || {
                    store.select(Selection {
                        exclude: vec!["a.json".into()],
                        ..sel("claude")
                    })
                }
            });
            let lease = selector
                .recv_timeout(BOUND)
                .unwrap_or_else(|_| panic!("a selection {when}"));
            leases.push(lease.expect("b.json"));
        };
        read_and_select(&mut leases, "during the write");
        let other = spawn_bounded({
            let store = store.clone();
            move || {
                let revision = store.get("b.json").unwrap().revision;
                store.apply_patch("b.json", revision, &token_patch("b2")).map(|_| ())
            }
        });
        assert!(
            other.recv_timeout(BLOCKED).is_err(),
            "durable commands serialize at the same service owner"
        );
        // A scan made now still reads A's old file, which is c4c1d43e's stale snapshot;
        // this reload carries what the write is about to publish, so the end state shows
        // which one landed last. It also read B before B's queued write. Nothing orders
        // that write against the reload once both are released, so the reload checks B
        // against the bytes it read, as every production reload does, and B's write
        // survives either order.
        let backend = store.credential_backend();
        let mut rescan = backend.scan(&cfg).unwrap();
        rescan.read.remove(&dir.join("a.json")).expect("the scan read A");
        assert!(rescan.read.contains_key(&dir.join("b.json")), "the scan read B");
        let in_scan = rescan.credentials.iter_mut().find(|c| c.id == "a.json").unwrap();
        in_scan.metadata.insert("access_token".into(), "new".into());
        in_scan.metadata.insert("reloaded".into(), true.into());
        let reload = spawn_bounded({
            let store = store.clone();
            move || store.reconcile_scanned(rescan.credentials, &rescan.read)
        });
        assert!(
            reload.recv_timeout(BLOCKED).is_err(),
            "the reload overtook the stalled write"
        );
        read_and_select(&mut leases, "while a reload is pending");
        let same = spawn_bounded({
            let store = store.clone();
            move || store.apply_patch("a.json", before, &token_patch("other")).map(|_| ())
        });
        assert!(
            same.recv_timeout(BLOCKED).is_err(),
            "a write of the same credential overtook it"
        );
        release.send(()).unwrap();
        let published = writer
            .recv_timeout(BOUND)
            .expect("the write finished")
            .expect("and published");
        assert!(published > before);
        assert!(matches!(other.recv_timeout(BOUND), Ok(Ok(()))));
        reload.recv_timeout(BOUND).expect("the reload finished");
        assert!(
            matches!(same.recv_timeout(BOUND), Ok(Err(PatchError::Stale { .. }))),
            "the same-credential write saw the published revision"
        );
        drop(leases);
        let file: Map<String, Value> = serde_json::from_slice(&std::fs::read(dir.join("a.json")).unwrap()).unwrap();
        assert_eq!(file["access_token"], "new");
        let a = store.get("a.json").unwrap();
        assert_eq!(
            a.metadata.get("reloaded"),
            Some(&Value::Bool(true)),
            "the reload landed last"
        );
        assert!(a.revision > published);
        assert_eq!(store.get("b.json").unwrap().metadata["access_token"], "b2");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// An edit that renames credential A to B's ID replaces B, so it must not run while
    /// a write of B is between its revision check and its publication.
    #[test]
    fn an_identity_change_waits_for_a_write_of_the_credential_it_replaces() {
        let (dir, _cfg, store) = stalled_store("identity-change", &["a.json", "b.json"]);
        let b = store.get("b.json").unwrap().revision;
        let a = store.get("a.json").unwrap();
        let (entered, release) = stall_next_write(&dir.join("b.json"));
        let writer = spawn_bounded({
            let store = store.clone();
            move || store.apply_patch("b.json", b, &token_patch("b2")).map(|c| c.revision)
        });
        entered.recv_timeout(BOUND).expect("B's write reached the disk");
        let mut renamed = Credential::clone(&a);
        renamed.id = "b.json".into();
        renamed.metadata.insert("access_token".into(), "renamed".into());
        let replace = spawn_bounded({
            let store = store.clone();
            move || {
                let bytes = br#"{"type":"claude","access_token":"renamed"}"#;
                store.replace_file_backed("a.json", a.revision, bytes, Some(renamed))
            }
        });
        assert!(replace.recv_timeout(BLOCKED).is_err(), "the rename overtook B's write");
        release.send(()).unwrap();
        let written = writer.recv_timeout(BOUND).expect("B's write finished");
        assert!(written.is_ok(), "B's write was published, not dropped: {written:?}");
        assert!(matches!(replace.recv_timeout(BOUND), Ok(Ok(()))));
        let ids: Vec<String> = store.snapshot().iter().map(|c| c.id.clone()).collect();
        assert_eq!(ids, ["b.json"]);
        assert_eq!(store.get("b.json").unwrap().metadata["access_token"], "renamed");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A scanned SQLite document cannot overwrite a later durable save or deletion.
    #[test]
    fn a_stale_scan_cannot_undo_a_later_write() {
        let dir = std::env::temp_dir().join(format!("cooker-stale-scan-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("synthetic.json");
        std::fs::write(&path, br#"{"type":"codex","prefix":"pfx-8"}"#).unwrap();
        let mut cfg = Config::parse("").unwrap();
        cfg.auth_dir = dir.clone();
        let store = test_store(ck_core::config::credentials::from_auth_dir(&cfg));
        let backend = store.credential_backend();
        let stale = backend.scan(&cfg).unwrap();
        let revision = store.get("synthetic.json").unwrap().revision;
        let mut patch = MetadataPatch::default();
        patch.set.insert("prefix".into(), "pfx-9".into());
        store.apply_patch("synthetic.json", revision, &patch).unwrap();
        let current = || store.get("synthetic.json").unwrap();
        let before = current();
        store.reconcile_scanned(stale.credentials, &stale.read);
        assert_eq!(current().metadata["prefix"], "pfx-9");
        assert_eq!(current().revision, before.revision);
        backend
            .save(&path, br#"{"type":"codex","prefix":"saved-edit"}"#)
            .unwrap();
        let saved = backend.scan(&cfg).unwrap();
        std::fs::write(&path, b"corrupt cache").unwrap();
        store.reconcile_scanned(saved.credentials, &saved.read);
        assert_eq!(
            current().metadata["prefix"],
            "saved-edit",
            "cache cannot veto a SQLite save"
        );
        let before = current();
        backend
            .save(&path, br#"{"type":"codex","prefix":"deleted-edit"}"#)
            .unwrap();
        let deleted = backend.scan(&cfg).unwrap();
        backend.delete(&path).unwrap();
        store.reconcile_scanned(deleted.credentials, &deleted.read);
        assert_eq!(
            current().revision,
            before.revision,
            "a deleted document's snapshot was applied"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn reconcile_keeps_unchanged_revisions_and_never_reuses_old_ones() {
        let store = test_store(vec![cred("a.json", "claude", false), cred("b.json", "claude", false)]);
        let a = store.get("a.json").unwrap();
        let b = store.get("b.json").unwrap();
        store.reconcile(vec![cred("a.json", "claude", false)]);
        assert!(
            Arc::ptr_eq(&store.get("a.json").unwrap(), &a),
            "unchanged credential kept as is"
        );
        assert!(store.get("b.json").is_none());
        store.reconcile(vec![cred("a.json", "claude", false), cred("b.json", "claude", false)]);
        assert!(
            store.get("b.json").unwrap().revision > b.revision,
            "re-created credential gets a new revision"
        );
    }

    #[test]
    fn attribute_only_reload_changes_priority_and_rejects_stale_outcome() {
        let a = cred("a.json", "claude", false);
        let mut b = cred("b.json", "claude", false);
        b.attributes.insert("priority".into(), "1".into());
        let store = test_store(vec![a.clone(), b.clone()]);
        let old = store.select(sel("claude")).unwrap();
        assert_eq!(old.credential.id, "b.json");
        let revision = old.credential.revision;
        b.attributes.insert("priority".into(), "2".into());
        store.reconcile(vec![a, b]);
        assert!(store.get("b.json").unwrap().revision > revision);
        old.complete(Outcome::Failure(ExecError::local(
            401,
            FailureScope::Credential,
            "expired",
        )));
        let current = store.select(sel("claude")).unwrap();
        assert_eq!(
            current.credential.id, "b.json",
            "stale outcome must not cool new revision"
        );
        assert_eq!(current.credential.attributes["priority"], "2");
        current.complete(Outcome::Success);
        assert_eq!(store.stats().failure, 1);
        assert_eq!(store.stats().success, 1);
    }

    #[test]
    fn stale_lease_cannot_poison_deleted_and_recreated_credential() {
        let credential = cred("a.json", "claude", false);
        let store = test_store(vec![credential.clone()]);
        let old = store.select(sel("claude")).unwrap();
        store.reconcile(Vec::new());
        store.reconcile(vec![credential]);
        old.complete(Outcome::Failure(ExecError::local(
            429,
            FailureScope::Credential,
            "quota",
        )));
        store.select(sel("claude")).unwrap().complete(Outcome::Success);
        assert_eq!(store.stats().failure, 1);
        assert_eq!(store.stats().success, 1);
    }

    /// Go `TestRetryIntervalFiltersCooldownCredentials` and
    /// `TestRetryRoundAvailabilityRejectsStaleQuotaForNonRetryableStatus`: the next round
    /// waits for the earliest cooldown within max-retry-interval, skipping longer ones,
    /// and a credential cooling after a 402 or 404 never permits a round, whatever its
    /// deadline, while one cooling after a 429 does.
    #[test]
    fn retry_wait_skips_cooldowns_beyond_the_cap_and_non_retryable_statuses() {
        let now = Instant::now();
        let selection = sel("claude");
        let admit_all = |_: &Credential| Some(String::new());
        // One credential per (status, retry-after seconds), each cooling from `now`.
        let wait = |cap: u64, cooldowns: &[(u16, u64)]| {
            let policy = Policy {
                request_retry: 1,
                max_retry_interval: Duration::from_secs(cap),
                ..Policy::default()
            };
            let ids: Vec<String> = (0..cooldowns.len()).map(|i| format!("c{i}.json")).collect();
            let store = test_store(ids.iter().map(|id| cred(id, "claude", false)).collect());
            for (id, (status, seconds)) in ids.iter().zip(cooldowns) {
                let mut error = ExecError::local(*status, FailureScope::Model, "cooling");
                error.retry_after = Some(Duration::from_secs(*seconds));
                let credential = store.get(id).unwrap();
                store.scheduler.lock().unwrap().record(
                    &credential,
                    &selection.model,
                    &Outcome::Failure(error),
                    &policy,
                    now,
                );
            }
            store.retry_wait_at(&selection, &policy, 429, &[], &admit_all, now)
        };
        let short_and_long = wait(30, &[(429, 10), (429, 60)]);
        assert!(
            short_and_long.is_some_and(|w| !w.is_zero() && w <= Duration::from_secs(10)),
            "{short_and_long:?}"
        );
        assert_eq!(wait(30, &[(429, 60)]), None, "only a cooldown beyond the cap");
        assert!(wait(3600, &[(429, 60)]).is_some(), "rate limit");
        assert_eq!(wait(3600, &[(402, 60)]), None, "payment required");
        assert_eq!(wait(3600, &[(404, 60)]), None, "not found");
    }

    #[test]
    fn retry_wait_obeys_exact_cap_and_attempted_quota_floor() {
        let store = test_store(vec![cred("a.json", "claude", false)]);
        let now = Instant::now();
        let mut selection = sel("claude");
        let admit_all = |_: &Credential| Some(String::new());
        let tried = ["a.json".to_owned()];
        let mut policy = Policy {
            request_retry: 1,
            ..Policy::default()
        };
        // A transport fault (status 0) retries immediately.
        assert_eq!(
            store.retry_wait_at(&selection, &policy, 0, &tried, &admit_all, now),
            Some(Duration::ZERO)
        );
        // An attempted credential after a 429 waits at least 10s, which the default
        // max-retry-interval of 0 forbids; an untried one may go immediately.
        assert_eq!(
            store.retry_wait_at(&selection, &policy, 429, &tried, &admit_all, now),
            None
        );
        assert_eq!(
            store.retry_wait_at(&selection, &policy, 429, &[], &admit_all, now),
            Some(Duration::ZERO)
        );
        policy.max_retry_interval = Duration::from_secs(10);
        assert_eq!(
            store.retry_wait_at(&selection, &policy, 429, &tried, &admit_all, now),
            Some(Duration::from_secs(10))
        );
        let quota = ExecError::local(429, FailureScope::Model, "quota");
        let credential = store.get("a.json").unwrap();
        store.scheduler.lock().unwrap().record(
            &credential,
            &selection.model,
            &Outcome::Failure(quota.clone()),
            &policy,
            now,
        );
        let later = now + Duration::from_millis(1);
        assert_eq!(
            store.retry_wait_at(&selection, &policy, 429, &tried, &admit_all, later),
            Some(Duration::from_secs(10))
        );
        policy.max_retry_interval = Duration::from_secs(9);
        assert_eq!(
            store.retry_wait_at(&selection, &policy, 429, &tried, &admit_all, later),
            None
        );
        policy.disable_cooling = true;
        store.scheduler.lock().unwrap().record(
            &credential,
            &selection.model,
            &Outcome::Failure(quota.clone()),
            &policy,
            later,
        );
        policy.max_retry_interval = Duration::ZERO;
        assert_eq!(
            store.retry_wait_at(&selection, &policy, 429, &tried, &admit_all, later),
            Some(Duration::ZERO),
            "disabled cooling has no floor"
        );
        selection.retry_round = 1;
        assert_eq!(
            store.retry_wait_at(&selection, &policy, 0, &tried, &admit_all, later),
            None
        );
    }

    /// A 429 with a reset six days away (`max-trusted-cooldown` bounds it).
    fn weekly_limit(scope: FailureScope) -> Outcome {
        let mut error = ExecError::local(429, scope, "usage_limit_reached");
        error.retry_after = Some(Duration::from_secs(6 * 24 * 3600));
        Outcome::Failure(error)
    }

    /// A probe whose hold lapsed was taken over by a later pick: the first probe's
    /// cancellation leaves the account reserved for the second, whose own does not.
    #[test]
    fn a_stale_probe_cancellation_keeps_the_newer_reservation() {
        let store = test_store(vec![cred("a.json", "claude", false)]);
        let selection = Selection::new("claude", "m");
        let credential = store.get("a.json").unwrap();
        // A bounded window of the 10 s minimum that ended a second ago.
        let policy = Policy {
            max_trusted_cooldown: Duration::from_secs(10),
            ..Policy::default()
        };
        let opened = Instant::now().checked_sub(Duration::from_secs(11)).unwrap();
        store
            .scheduler
            .lock()
            .unwrap()
            .record(&credential, "m", &weekly_limit(FailureScope::Model), &policy, opened);
        let first = store.select(selection.clone()).expect("the probe");
        assert_eq!(first.execution_model, "m");
        assert!(store.select(selection.clone()).is_none(), "reserved");
        // The first probe's hold lapses, and the next pick probes again.
        let key = ("a.json".to_owned(), "m".to_owned());
        store
            .scheduler
            .lock()
            .unwrap()
            .cooldowns
            .get_mut(&key)
            .unwrap()
            .trust
            .probe = Some(Instant::now());
        let second = store.select(selection.clone()).expect("the second probe");
        first.complete(Outcome::Cancelled);
        assert!(store.select(selection.clone()).is_none(), "still reserved");
        second.complete(Outcome::Cancelled);
        assert!(store.select(selection).is_some(), "its own cancellation frees it");
    }

    /// Go 35af74f9 (TestManager_Issue6643_InFlightSuccessesKeepFreshQuotaCooldown):
    /// successes of requests dispatched before a 429 was recorded, whether picked by
    /// selection or by the credits fallback outside it, leave the cooldown; a lease
    /// dispatched again after it (a refresh-and-retry) clears it.
    #[tokio::test]
    async fn successes_dispatched_before_a_cooldown_keep_it() {
        let rt = Arc::new(test_runtime(
            Config::parse("").unwrap(),
            vec![cred("a.json", "claude", false)],
            Executors {
                claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
                codex: Default::default(),
                devices: Default::default(),
                openai: Default::default(),
                google: Default::default(),
            },
        ));
        let selection = Selection::new("claude", "m");
        let cfg = rt.config();
        let in_flight = rt.store.select(selection.clone()).unwrap();
        let credits = rt
            .lease_credential("a.json", selection.clone(), &cfg, Credential::clone)
            .await
            .unwrap();
        let mut retried = rt.store.select(selection.clone()).unwrap();
        let mut quota = ExecError::local(429, FailureScope::Model, "rate limited");
        quota.retry_after = Some(Duration::from_secs(3600));
        rt.store
            .select(selection.clone())
            .unwrap()
            .complete(Outcome::Failure(quota));
        assert!(rt.store.select(selection.clone()).is_none(), "cooling");
        in_flight.complete(Outcome::Success);
        assert!(rt.store.select(selection.clone()).is_none(), "a stale success keeps it");
        credits.complete(Outcome::Success);
        assert!(
            rt.store.select(selection.clone()).is_none(),
            "a stale credits success keeps it"
        );
        retried.redispatch();
        retried.complete(Outcome::Success);
        assert!(rt.store.select(selection).is_some(), "a later dispatch clears it");
    }

    /// A request picked before a bounded window opened succeeds after it (a stream that
    /// outlived the 429): its answer is older than the window, which stays, live or
    /// ended, until its own probe.
    #[test]
    fn a_late_success_keeps_a_newer_window() {
        let selection = Selection::new("claude", "m");
        // A model window, opened by a request picked after the stream.
        let store = test_store(vec![cred("a.json", "claude", false)]);
        let stream = store.select(selection.clone()).unwrap();
        let other = store.select(selection.clone()).unwrap();
        other.complete(weekly_limit(FailureScope::Model));
        assert!(store.select(selection.clone()).is_none(), "cooling");
        stream.accepted();
        stream.complete(Outcome::Success);
        assert!(store.select(selection.clone()).is_none(), "the window stands");
        // A credential-wide window that already ended keeps its probe.
        let store = test_store(vec![cred("a.json", "claude", false)]);
        let stream = store.select(selection.clone()).unwrap();
        let policy = Policy {
            max_trusted_cooldown: Duration::from_secs(10),
            ..Policy::default()
        };
        let opened = Instant::now().checked_sub(Duration::from_secs(11)).unwrap();
        store.scheduler.lock().unwrap().record(
            &store.get("a.json").unwrap(),
            "m",
            &weekly_limit(FailureScope::Credential),
            &policy,
            opened,
        );
        stream.accepted();
        stream.complete(Outcome::Success);
        let _probe = store.select(selection.clone()).expect("the window's probe");
        assert!(store.select(selection).is_none(), "one probe");
    }

    #[test]
    fn config_routing_drives_policy_at_startup_and_on_publish() {
        let executors = || Executors {
            claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
            codex: Default::default(),
            devices: Default::default(),
            openai: Default::default(),
            google: Default::default(),
        };
        // Legacy top-level keys and the canonical routing block both reach the scheduler.
        let rt = test_runtime(
            Config::parse("routing:\n  strategy: ff\n  session-affinity-ttl: 250ms\n  retry:\n    request-retry: 2\n")
                .unwrap(),
            Vec::new(),
            executors(),
        );
        let policy = rt.policy();
        assert_eq!(policy.strategy, crate::scheduling::scheduler::Strategy::FillFirst);
        assert_eq!(policy.request_retry, 2);
        assert_eq!(policy.session_affinity_ttl, Duration::from_secs(1));

        rt.publish_config(Config::parse("routing:\n  strategy: wrr\n").unwrap());
        let policy = rt.policy();
        assert_eq!(
            policy.strategy,
            crate::scheduling::scheduler::Strategy::WeightedRoundRobin
        );
        assert_eq!(policy.request_retry, 0);
        assert_eq!(policy.session_affinity_ttl, Duration::from_secs(3600));
    }

    /// Standalone publications reach runtime consumers one by one, in
    /// order, each with its own view; a dropped subscriber is pruned.
    #[test]
    fn runtime_consumers_receive_every_publication_in_order() {
        let executors = Executors {
            claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
            codex: Default::default(),
            devices: Default::default(),
            openai: Default::default(),
            google: Default::default(),
        };
        let rt = test_runtime(Config::parse("server:\n  port: 8501\n").unwrap(), Vec::new(), executors);
        let mut kept = rt.subscribe_config();
        let dropped = rt.subscribe_config();
        assert_eq!(kept.current().config.port, 8501);
        let start = kept.current().sequence;
        drop(dropped);
        assert!(rt.publish_config(Config::parse("server:\n  port: 8502\n").unwrap()));
        assert!(rt.publish_config(Config::parse("server:\n  port: 8503\n").unwrap()));
        let first = kept.try_next().expect("the first publication");
        let second = kept.try_next().expect("the second publication");
        assert_eq!((first.sequence, first.config.port), (start + 1, 8502));
        assert_eq!((second.sequence, second.config.port), (start + 2, 8503));
        assert!(kept.try_next().is_none());
        // The view a consumer holds is its input, whatever was published later.
        assert_eq!(first.config.port, 8502);
        assert_eq!(rt.config().port, 8503);
        assert_eq!(rt.config_changes.lock().unwrap().subscribers.len(), 1);
    }

    /// Go `nextModelPoolOffset` (conductor_models.go): per-key cursor, no advance for
    /// single-entry pools or blank keys, reset to zero at the int32 guard.
    #[test]
    fn pool_offsets_rotate_per_key_like_go() {
        let rt = test_runtime(
            Config::parse("").unwrap(),
            Vec::new(),
            Executors {
                claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
                codex: Default::default(),
                devices: Default::default(),
                openai: Default::default(),
                google: Default::default(),
            },
        );
        let offsets: Vec<usize> = (0..4).map(|_| rt.next_pool_offset("a|openai|m", 3)).collect();
        assert_eq!(offsets, [0, 1, 2, 0]);
        assert_eq!(rt.next_pool_offset(" a|openai|m ", 3), 1, "keys are trimmed");
        assert_eq!(rt.next_pool_offset("b|openai|m", 3), 0, "keys rotate independently");
        assert_eq!(rt.next_pool_offset("c", 1), 0);
        assert_eq!(rt.next_pool_offset("c", 2), 0, "a single-entry pool does not advance");
        assert_eq!(rt.next_pool_offset("  ", 2), 0);
        rt.pool_offsets.lock().unwrap().insert("d".into(), 2_147_483_641);
        assert_eq!(rt.next_pool_offset("d", 3), 0, "the guard resets before use");
        assert_eq!(rt.next_pool_offset("d", 3), 1);
    }

    /// Go `Manager.persist` after `MarkResult`: the auth file gains `"disabled": false`
    /// in `json.Marshal` form (sorted keys, float64 numbers, HTML escaping, no newline)
    /// once, and is not rewritten while it already holds that JSON.
    #[test]
    fn first_use_persists_disabled_in_go_marshal_form() {
        let dir = std::env::temp_dir().join(format!(
            "persist-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("a.json");
        let original = "{\"type\":\"claude\",\"email\":\"a<b>@x.invalid\",\"n\":1.50,\"big\":1e3,\"nested\":{\"z\":1,\"a\":[true,null]}}\n";
        std::fs::write(&path, original).unwrap();
        let metadata: Map<String, Value> = serde_json::from_str(original).unwrap();
        let store = test_store(vec![Credential::from_file(&dir, &path, metadata).unwrap()]);
        let revision = store.get("a.json").unwrap().revision;
        store
            .select(Selection::new("claude", "m"))
            .unwrap()
            .complete(Outcome::Success);
        let written = std::fs::read_to_string(&path).unwrap();
        assert_eq!(
            written,
            r#"{"big":1000,"disabled":false,"email":"a\u003cb\u003e@x.invalid","n":1.5,"nested":{"a":[true,null],"z":1},"type":"claude"}"#
        );
        let current = store.get("a.json").unwrap();
        assert_eq!(current.revision, revision, "a neutral write keeps the revision");
        assert_eq!(current.metadata.get("disabled"), Some(&Value::Bool(false)));
        assert_eq!(current.metadata.get("big"), Some(&Value::from(1000)));
        // The watcher's reload of the written file changes nothing.
        let reloaded: Map<String, Value> = serde_json::from_str(&written).unwrap();
        store.reconcile(vec![Credential::from_file(&dir, &path, reloaded).unwrap()]);
        assert_eq!(store.get("a.json").unwrap().revision, revision);
        // Same JSON in another layout is left alone.
        store
            .credential_backend()
            .save(&path, b"{\"type\":\"claude\", \"disabled\":false}")
            .unwrap();
        let mut metadata = Map::new();
        metadata.insert("type".into(), "claude".into());
        metadata.insert("disabled".into(), false.into());
        store.reconcile(vec![Credential::from_file(&dir, &path, metadata).unwrap()]);
        store
            .select(Selection::new("claude", "m"))
            .unwrap()
            .complete(Outcome::Success);
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "{\"type\":\"claude\", \"disabled\":false}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A result recorded after a refresh committed a newer revision must not write the
    /// older token back (Go persists under the manager lock with generation checks).
    #[test]
    fn stale_result_does_not_overwrite_a_refreshed_credential() {
        let dir = std::env::temp_dir().join(format!(
            "stale-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("a.json");
        std::fs::write(&path, r#"{"type":"claude","access_token":"old"}"#).unwrap();
        let metadata: Map<String, Value> = serde_json::from_str(r#"{"type":"claude","access_token":"old"}"#).unwrap();
        let store = test_store(vec![Credential::from_file(&dir, &path, metadata).unwrap()]);
        let stale = store.get("a.json").unwrap();
        let mut patch = MetadataPatch::default();
        patch.set.insert("access_token".into(), "new".into());
        store.apply_patch("a.json", stale.revision, &patch).unwrap();
        let refreshed = std::fs::read_to_string(&path).unwrap();
        store.persist_credential(&stale).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), refreshed);
        assert!(refreshed.contains("\"new\"") && refreshed.contains("\"disabled\":false"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// SQLite cooldown snapshots survive restart and reset, and restore Go-format records.
    #[test]
    fn cooldowns_persist_restore_and_reset_through_sqlite() {
        let dir = std::env::temp_dir().join(format!(
            "cds-rt-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let config = || {
            let mut config = Config::parse("routing:\n  cooldown:\n    save-cooldown-status: true\n").unwrap();
            config.auth_dir = dir.clone();
            config
        };
        let creds = || {
            let metadata = |p: &str| {
                let mut m = Map::new();
                m.insert("type".into(), p.into());
                m
            };
            vec![
                Credential::from_file(&dir, &dir.join("a.json"), metadata("claude")).unwrap(),
                Credential::from_file(&dir, &dir.join("b.json"), metadata("claude")).unwrap(),
            ]
        };
        let executors = || Executors {
            claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
            codex: Default::default(),
            openai: Default::default(),
            google: Default::default(),
            devices: Default::default(),
        };
        let backend = private_backend(&creds());
        let rt = Runtime::new(config(), creds(), executors(), backend.clone());
        let mut selection = Selection::new("claude", "m1");
        selection.exclude.push("b.json".into());
        let lease = rt.store.select(selection.clone()).unwrap();
        let mut quota = ExecError::local(429, FailureScope::Model, "rate limited");
        quota.retry_after = Some(Duration::from_secs(120));
        lease.complete(Outcome::Failure(quota));
        rt.store.flush_cooldowns();
        let saved = crate::store::cooldown_store::Backend::load(&*backend).unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!((&*saved[0].model, &*saved[0].reason), ("m1", "quota"));
        assert!(!dir.join("a.cds").exists(), "runtime writes only SQLite cooldowns");
        // A successful attempt on another credential changes no cooldown.
        let mut other = Selection::new("claude", "m1");
        other.exclude.push("a.json".into());
        rt.store.select(other).unwrap().complete(Outcome::Success);
        rt.store.flush_cooldowns();
        assert_eq!(crate::store::cooldown_store::Backend::load(&*backend).unwrap().len(), 1);

        // A new process restores the cooldown.
        drop(rt);
        let rt = Runtime::new(config(), creds(), executors(), backend.clone());
        let a = rt.store.get("a.json").unwrap();
        let wait = rt
            .store
            .scheduler
            .lock()
            .unwrap()
            .wait(&a, "m1", Instant::now())
            .unwrap();
        assert!(
            wait > Duration::from_secs(100) && wait <= Duration::from_secs(120),
            "{wait:?}"
        );
        assert_eq!(rt.store.reset_cooldown("a.json"), ["m1"]);
        rt.store.flush_cooldowns();
        assert!(
            crate::store::cooldown_store::Backend::load(&*backend)
                .unwrap()
                .is_empty()
        );

        // A file CLIProxyAPI wrote (local time with an offset) restores model and
        // credential-wide quota records; an expired record is dropped on rewrite.
        let at = |secs: i64| {
            (chrono::Utc::now() + chrono::Duration::seconds(secs))
                .with_timezone(&chrono::FixedOffset::east_opt(7200).unwrap())
                .to_rfc3339_opts(chrono::SecondsFormat::Nanos, false)
        };
        let go = format!(
            r#"{{"version":1,"auth_id":"b.json","provider":"claude","updated_at":"{now}","records":[
{{"provider":"claude","auth_id":"b.json","status":"cooling","next_retry_after":"{late}","reason":"credential_quota","quota":{{"exceeded":true,"reason":"credential_quota","next_recover_at":"{late}","observed_at":"0001-01-01T00:00:00Z"}},"last_error":{{"message":"credential quota","retryable":false,"http_status":429}},"updated_at":"{now}"}},
{{"provider":"claude","auth_id":"b.json","model":"m2","status":"cooling","next_retry_after":"{soon}","reason":"unauthorized","quota":{{"exceeded":false,"next_recover_at":"0001-01-01T00:00:00Z","observed_at":"0001-01-01T00:00:00Z"}},"last_error":{{"message":"unauthorized","retryable":false,"http_status":401}},"updated_at":"{now}"}},
{{"provider":"claude","auth_id":"b.json","model":"m4","status":"cooling","next_retry_after":"{soon}","reason":"quota","quota":{{"exceeded":true,"reason":"quota","next_recover_at":"{late}","observed_at":"0001-01-01T00:00:00Z"}},"updated_at":"{now}"}},
{{"provider":"claude","auth_id":"b.json","model":"m5","status":"cooling","next_retry_after":"{past}","reason":"quota","quota":{{"exceeded":true,"reason":"quota","next_recover_at":"{late}","observed_at":"0001-01-01T00:00:00Z"}},"updated_at":"{now}"}},
{{"provider":"claude","auth_id":"b.json","model":"m3","status":"cooling","next_retry_after":"{past}","reason":"boom","quota":{{"exceeded":false,"next_recover_at":"0001-01-01T00:00:00Z","observed_at":"0001-01-01T00:00:00Z"}},"updated_at":"{now}"}},
{{"provider":"claude","auth_id":"gone.json","model":"m1","status":"cooling","next_retry_after":"{late}","quota":{{"exceeded":false,"next_recover_at":"0001-01-01T00:00:00Z","observed_at":"0001-01-01T00:00:00Z"}},"updated_at":"{now}"}}]}}"#,
            now = at(0),
            late = at(600),
            soon = at(60),
            past = at(-5),
        );
        let mut go: serde_json::Value = serde_json::from_str(&go).unwrap();
        let records: Vec<crate::store::cooldown_store::Record> = serde_json::from_value(go["records"].take()).unwrap();
        drop(rt);
        crate::store::cooldown_store::Backend::save(&*backend, records, std::time::SystemTime::now()).unwrap();
        let rt = Runtime::new(config(), creds(), executors(), backend.clone());
        let b = rt.store.get("b.json").unwrap();
        let now = Instant::now();
        let scheduler = rt.store.scheduler.lock().unwrap();
        let wait = |m: &str| scheduler.wait(&b, m, now).unwrap_or_default().as_secs();
        assert!((590..=600).contains(&wait("m9")), "credential quota blocks every model");
        assert!(scheduler.quota_cooling(&b, "m9", now));
        let records = scheduler.records(&b, now, std::time::SystemTime::now());
        drop(scheduler);
        let models: Vec<&str> = records.iter().map(|r| r.model.as_str()).collect();
        assert_eq!(
            models,
            ["", "m2", "m4"],
            "expired (by retry deadline) and unknown-credential records are dropped"
        );
        let m4 = records.iter().find(|r| r.model == "m4").unwrap();
        let left = m4
            .next_retry_after
            .unwrap()
            .duration_since(std::time::SystemTime::now())
            .unwrap();
        assert!(
            left > Duration::from_secs(590),
            "the later quota recovery wins: {left:?}"
        );
        rt.store.flush_cooldowns();
        let rewritten = crate::store::cooldown_store::Backend::load(&*backend).unwrap();
        assert!(rewritten.iter().all(|r| r.auth_id != "gone.json" && r.model != "m3"));

        // Turning the option off persists the final state and stops writing.
        let mut disabled = Config::parse("").unwrap();
        disabled.auth_dir = dir.clone();
        rt.publish_config(disabled);
        rt.store.reset_cooldown("b.json");
        rt.store.flush_cooldowns();
        assert!(
            !crate::store::cooldown_store::Backend::load(&*backend)
                .unwrap()
                .is_empty(),
            "no writes once disabled"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn cooldown_sqlite_constructor_flushes_on_shutdown() {
        let dir = std::env::temp_dir().join(format!("cooker-cooldown-sqlite-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let (database, backend) = sqlite_backend(&dir);
        let auth = backend.auth_dir().join("a.json");
        backend
            .save(&auth, br#"{"type":"claude","access_token":"private-test"}"#)
            .unwrap();
        let mut config = Config::parse("routing: {cooldown: {save-cooldown-status: true}}\n").unwrap();
        config.auth_dir = backend.auth_dir();
        let scan = scan_private(&backend, &config);
        let rt = Runtime::with_cooldown_backend(
            config,
            scan.credentials,
            Executors {
                claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
                codex: Default::default(),
                openai: Default::default(),
                google: Default::default(),
                devices: Default::default(),
            },
            backend.clone(),
            backend.clone(),
        );
        rt.deny_external.store(true, Ordering::Relaxed);
        rt.set_local_model(true);
        rt.store.note_loaded(&scan.read);
        assert!(rt.store.cooldowns("a.json").is_empty());
        rt.store.flush_cooldowns();
        assert!(
            crate::store::cooldown_store::Backend::load(&*backend)
                .unwrap()
                .is_empty()
        );
        let mut quota = ExecError::local(429, FailureScope::Model, "rate limited");
        quota.retry_after = Some(Duration::from_secs(120));
        rt.store
            .select(Selection::new("claude", "m2"))
            .unwrap()
            .complete(Outcome::Failure(quota));
        rt.shutdown_codex_refreshes().await;
        let saved = crate::store::cooldown_store::Backend::load(&*backend).unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].model, "m2");
        drop(rt);
        drop(backend);
        drop(database);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_failed_final_cooldown_checkpoint_makes_shutdown_incomplete() {
        let dir = std::env::temp_dir().join(format!("cooker-cooldown-failure-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let (database, backend) = sqlite_backend(&dir);
        backend
            .save(
                &backend.auth_dir().join("a.json"),
                br#"{"type":"claude","access_token":"private-test"}"#,
            )
            .unwrap();
        let mut config = Config::parse("routing: {cooldown: {save-cooldown-status: true}}\n").unwrap();
        config.auth_dir = backend.auth_dir();
        let scan = scan_private(&backend, &config);
        let rt = Runtime::with_cooldown_backend(
            config,
            scan.credentials,
            Executors {
                claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
                codex: Default::default(),
                openai: Default::default(),
                google: Default::default(),
                devices: Default::default(),
            },
            backend.clone(),
            backend.clone(),
        );
        rt.deny_external.store(true, Ordering::Relaxed);
        rt.set_local_model(true);
        rt.store.note_loaded(&scan.read);
        rt.store.flush_cooldowns();
        let blocked = crate::testing::block_checkpoint_install(&database);
        let mut quota = ExecError::local(429, FailureScope::Model, "rate limited");
        quota.retry_after = Some(Duration::from_secs(120));
        rt.store
            .select(Selection::new("claude", "m2"))
            .unwrap()
            .complete(Outcome::Failure(quota));
        // Volatile scheduling feedback is immediate even though its checkpoint will fail.
        assert!(!rt.store.cooldowns("a.json").is_empty());
        assert!(
            rt.finish_shutdown().await.is_err(),
            "a lost final cooldown checkpoint must not report a complete shutdown"
        );
        drop(blocked);
        assert!(
            crate::store::cooldown_store::Backend::load(&*backend)
                .unwrap()
                .is_empty()
        );
        drop(rt);
        drop(backend);
        drop(database);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn cooldown_credential_normalization_leaves_async_worker_and_runs_once_per_revision() {
        let dir = std::env::temp_dir().join(format!("cooker-result-sqlite-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("a.json"),
            br#"{"type":"claude","access_token":"private-test"}"#,
        )
        .unwrap();
        let (database, backend) = sqlite_backend(&dir);
        let mut config = Config::parse("").unwrap();
        config.auth_dir = backend.auth_dir();
        let scan = scan_private(&backend, &config);
        let rt = Runtime::new(
            config,
            scan.credentials,
            Executors {
                claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
                codex: Default::default(),
                openai: Default::default(),
                google: Default::default(),
                devices: Default::default(),
            },
            backend.clone(),
        );
        rt.store.note_loaded(&scan.read);
        let effects = crate::testing::publication_effects::Counter::new(&database);
        let (entered, ready) = tokio::sync::oneshot::channel();
        let (release, gate) = std::sync::mpsc::channel();
        let held = {
            let database = database.clone();
            std::thread::spawn(move || {
                let _conn = database.conn().unwrap();
                entered.send(()).unwrap();
                let _ = gate.recv_timeout(Duration::from_secs(5));
            })
        };
        ready.await.unwrap();
        let started = Instant::now();
        // Both outcome recording and another async job must finish while the real
        // storage coordination lock is held, not only after normalization drains.
        let completed = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(
                async {
                    for _ in 0..2 {
                        rt.store
                            .select(Selection::new("claude", "m1"))
                            .unwrap()
                            .complete(Outcome::Success);
                    }
                },
                tokio::task::yield_now(),
            );
        })
        .await;
        let elapsed = started.elapsed();
        let _ = release.send(());
        held.join().unwrap();
        assert!(
            completed.is_ok() && elapsed < Duration::from_secs(1),
            "result recording stalled the async worker on storage: {elapsed:?}"
        );

        async fn drain(rt: &Runtime) {
            rt.store.persistence_tasks.close();
            rt.store.persistence_tasks.wait().await;
            rt.store.persistence_tasks.reopen();
        }
        async fn outcomes(rt: &Runtime) {
            for _ in 0..2 {
                rt.store
                    .select(Selection::new("claude", "m1"))
                    .unwrap()
                    .complete(Outcome::Success);
            }
            drain(rt).await;
        }
        drain(&rt).await;
        assert_eq!(
            effects.get(),
            1,
            "one durable normalization, including the initial burst"
        );
        let normalized = backend.read(&backend.auth_dir().join("a.json")).unwrap().unwrap();
        assert_eq!(serde_json::from_slice::<Value>(&normalized).unwrap()["disabled"], false);
        assert_eq!(std::fs::read(backend.auth_dir().join("a.json")).unwrap(), normalized);
        outcomes(&rt).await;
        assert_eq!(
            effects.get(),
            1,
            "same relevant revision must not repeat equal-byte I/O"
        );
        let revision = rt.store.get("a.json").unwrap().revision;
        drop(effects);

        // An unrelated configuration revision is a real save, outside the counted
        // window. It must not cause normalization of this credential again.
        database
            .commit_if(
                &[crate::store::config_store::Op::Put {
                    kind: "setting".into(),
                    id: "routing".into(),
                    spec: serde_json::json!({"strategy":"fill-first"}),
                    ord: None,
                    description: None,
                    if_rev: None,
                    if_absent: false,
                }],
                "test",
                "unrelated",
                false,
                None,
            )
            .unwrap();
        let effects = crate::testing::publication_effects::Counter::new(&database);
        outcomes(&rt).await;
        assert_eq!(rt.store.get("a.json").unwrap().revision, revision);
        assert_eq!(effects.get(), 0, "unrelated revisions must not repeat normalization");
        assert_eq!(
            backend.read(&backend.auth_dir().join("a.json")).unwrap().unwrap(),
            normalized
        );
        drop(effects);

        let mut patch = MetadataPatch::default();
        patch.set.insert("note".into(), "new relevant source revision".into());
        rt.store.apply_patch("a.json", revision, &patch).unwrap();
        assert!(rt.store.get("a.json").unwrap().revision > revision);
        let effects = crate::testing::publication_effects::Counter::new(&database);
        outcomes(&rt).await;
        assert_eq!(
            effects.get(),
            1,
            "new relevant revision needs one normalization even for equal bytes"
        );
        outcomes(&rt).await;
        assert_eq!(
            effects.get(),
            1,
            "no redundant normalization after the new revision is saved"
        );
        rt.shutdown_codex_refreshes().await;
        drop(effects);
        drop(rt);
        drop(backend);
        drop(database);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn preparation_waiter_observes_deletion_and_refresh_loop_is_replaceable() {
        let rt = Arc::new(test_runtime(
            Config::parse("").unwrap(),
            vec![cred("a.json", "claude", false)],
            Executors {
                claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
                codex: Default::default(),
                devices: Default::default(),
                openai: Default::default(),
                google: Default::default(),
            },
        ));
        let lock = rt.store.prepare_lock("a.json");
        let guard = lock.lock().await;
        let worker = {
            let rt = rt.clone();
            tokio::spawn(async move { rt.prepare_credential("a.json", &rt.config(), None).await })
        };
        tokio::task::yield_now().await;
        assert!(!worker.is_finished());
        rt.store.reconcile(Vec::new());
        drop(guard);
        let error = worker.await.unwrap().unwrap_err();
        assert_eq!(error.scope, FailureScope::Request);
        assert!(rt.store.snapshot().is_empty());
        rt.start_auto_refresh();
        let first = rt.refresh_task.lock().unwrap().clone().unwrap();
        rt.start_auto_refresh();
        tokio::task::yield_now().await;
        assert!(first.is_finished());
        let second = rt.refresh_task.lock().unwrap().clone().unwrap();
        rt.stop_auto_refresh();
        tokio::task::yield_now().await;
        assert!(second.is_finished());
        assert!(rt.refresh_task.lock().unwrap().is_none());
    }

    /// A refresh whose caller is cancelled while its commit waits keeps the preparation
    /// lock until the commit returns: a writer that serializes on the lock (an SIWC
    /// binding) lands after the rotated tokens instead of making them stale.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cancelled_refresh_holds_the_preparation_lock_through_its_commit() {
        let (hit_tx, mut hit) = tokio::sync::mpsc::unbounded_channel();
        let answer = Arc::new(tokio::sync::Semaphore::new(0));
        let app = axum::Router::new().route(
            "/oauth/token",
            axum::routing::post({
                let answer = answer.clone();
                move || async move {
                    hit_tx.send(()).unwrap();
                    answer.acquire().await.unwrap().forget();
                    r#"{"access_token":"rotated","refresh_token":"rotated-rt","expires_in":3600}"#
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let token_url = format!("http://{}/oauth/token", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let dir = std::env::temp_dir().join(format!("cooker-prepare-cancel-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("codex.json");
        std::fs::write(
            &path,
            r#"{"type":"codex","access_token":"outer","refresh_token":"outer-rt"}"#,
        )
        .unwrap();
        let mut cfg = Config::parse("").unwrap();
        cfg.auth_dir = dir.clone();
        let client = wreq::Client::new();
        let oauth =
            ck_exec::codex_oauth::CodexOAuth::with_endpoints(client.clone(), &token_url, &token_url, &token_url);
        let rt = Arc::new(test_runtime(
            cfg.clone(),
            ck_core::config::credentials::from_auth_dir(&cfg)
                .into_iter()
                .map(crate::testing::local)
                .collect(),
            Executors {
                claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
                codex: ck_exec::codex::CodexExecutor::with_client(client, oauth),
                devices: Default::default(),
                openai: Default::default(),
                google: Default::default(),
            },
        ));
        let refresh = tokio::spawn({
            let rt = rt.clone();
            async move { rt.refresh_credential("codex.json").await }
        });
        if tokio::time::timeout(Duration::from_secs(5), hit.recv()).await.is_err() {
            panic!("no refresh request: {:?}", refresh.await);
        }
        // Hold the store so the refresh's commit job blocks inside `apply_patch`, then
        // let the token response through and cancel the caller while the job waits.
        let (held_tx, held) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel::<()>();
        let holder = std::thread::spawn({
            let store = rt.store.clone();
            move || {
                let _inner = store.inner.write().unwrap();
                held_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            }
        });
        held.recv().unwrap();
        answer.add_permits(1);
        tokio::time::sleep(Duration::from_millis(500)).await;
        refresh.abort();
        assert!(refresh.await.unwrap_err().is_cancelled());
        let lock = rt.store.prepare_lock("codex.json");
        assert!(
            tokio::time::timeout(Duration::from_millis(300), lock.clone().lock_owned())
                .await
                .is_err(),
            "the detached commit still holds the lock"
        );
        release.send(()).unwrap();
        holder.join().unwrap();
        let _binding = tokio::time::timeout(Duration::from_secs(5), lock.lock_owned())
            .await
            .unwrap();
        let current = rt.store.get("codex.json").unwrap();
        assert_eq!(current.str("access_token"), Some("rotated"));
        let mut patch = MetadataPatch::default();
        patch.set.insert("use_siwc".into(), true.into());
        let bound = rt.store.apply_patch("codex.json", current.revision, &patch).unwrap();
        let disk: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        for meta in [&disk, &Value::Object(bound.metadata.clone())] {
            assert_eq!(
                (&meta["access_token"], &meta["refresh_token"], &meta["use_siwc"]),
                (
                    &serde_json::json!("rotated"),
                    &serde_json::json!("rotated-rt"),
                    &serde_json::json!(true)
                ),
                "{meta}"
            );
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Go 935aa6e3 (TestManager_RefreshAuth_DropsStaleConcurrentCredential): tokens saved
    /// while a refresh is in flight win over its result, in memory and on disk, and the
    /// refresh answers with them. An edit that leaves the credentials alone is merged with
    /// the refresh (Go `MergeRefreshedAuth`), so a rotated refresh token is not lost and a
    /// field the refresh did not change keeps the edit.
    #[tokio::test]
    async fn an_in_flight_refresh_does_not_overwrite_newer_credentials() {
        for (edit, want) in [
            (
                serde_json::json!({"access_token": "saved", "refresh_token": "saved-rt"}),
                ("saved", "saved-rt", None, "free"),
            ),
            (
                serde_json::json!({"note": "kept"}),
                ("rotated", "rotated-rt", Some("kept"), "free"),
            ),
            // The refresh leaves the plan as it was, so the edit stands.
            (
                serde_json::json!({"plan_type": "team"}),
                ("rotated", "rotated-rt", None, "team"),
            ),
        ] {
            let (hit_tx, mut hit) = tokio::sync::mpsc::unbounded_channel();
            let answer = Arc::new(tokio::sync::Semaphore::new(0));
            let app = axum::Router::new().route(
                "/oauth/token",
                axum::routing::post({
                    let answer = answer.clone();
                    move || async move {
                        hit_tx.send(()).unwrap();
                        answer.acquire().await.unwrap().forget();
                        r#"{"access_token":"rotated","refresh_token":"rotated-rt","expires_in":3600}"#
                    }
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let token_url = format!("http://{}/oauth/token", listener.local_addr().unwrap());
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let dir = std::env::temp_dir().join(format!("cooker-refresh-stale-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join("codex.json");
            std::fs::write(
                &path,
                r#"{"type":"codex","access_token":"outer","refresh_token":"outer-rt","plan_type":"free"}"#,
            )
            .unwrap();
            let mut cfg = Config::parse("").unwrap();
            cfg.auth_dir = dir.clone();
            let client = wreq::Client::new();
            let oauth =
                ck_exec::codex_oauth::CodexOAuth::with_endpoints(client.clone(), &token_url, &token_url, &token_url);
            let rt = Arc::new(test_runtime(
                cfg.clone(),
                ck_core::config::credentials::from_auth_dir(&cfg)
                    .into_iter()
                    .map(crate::testing::local)
                    .collect(),
                Executors {
                    claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
                    codex: ck_exec::codex::CodexExecutor::with_client(client, oauth),
                    devices: Default::default(),
                    openai: Default::default(),
                    google: Default::default(),
                },
            ));
            let refresh = tokio::spawn({
                let rt = rt.clone();
                async move { rt.refresh_credential("codex.json").await }
            });
            tokio::time::timeout(Duration::from_secs(5), hit.recv())
                .await
                .expect("a refresh request");
            let mut patch = MetadataPatch::default();
            for (k, v) in edit.as_object().unwrap() {
                patch.set.insert(k.clone(), v.clone());
            }
            let before = rt.store.get("codex.json").unwrap().revision;
            rt.store.apply_patch("codex.json", before, &patch).unwrap();
            answer.add_permits(1);
            let answered = refresh.await.unwrap().expect("the refresh answers");
            let current = rt.store.get("codex.json").unwrap();
            let disk: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
            for meta in [
                &disk,
                &Value::Object(current.metadata.clone()),
                &Value::Object(answered.metadata.clone()),
            ] {
                assert_eq!(
                    (
                        meta["access_token"].as_str().unwrap(),
                        meta["refresh_token"].as_str().unwrap(),
                        meta["note"].as_str(),
                        meta["plan_type"].as_str().unwrap()
                    ),
                    want,
                    "{edit}: {meta}"
                );
            }
            std::fs::remove_dir_all(dir).unwrap();
        }
    }

    /// A runtime over Codex credential files in a private credential directory, refreshing against a
    /// loopback mock that records each grant as (partition, refresh token, client ID).
    struct CodexDisk {
        database: Arc<crate::store::config_store::ConfigStore>,
        rt: Arc<Runtime>,
        dir: std::path::PathBuf,
        grants: Arc<Mutex<Grants>>,
        /// Each grant takes a permit before the mock answers.
        gate: Arc<tokio::sync::Semaphore>,
        arrived: Arc<tokio::sync::Notify>,
        base: String,
    }

    type Grants = Vec<(&'static str, String, String)>;

    impl CodexDisk {
        async fn new(files: &[(&str, &Value)]) -> Self {
            Self::gated(files, 1000).await
        }

        async fn gated(files: &[(&str, &Value)], permits: usize) -> Self {
            let grants = Arc::new(Mutex::new(Vec::new()));
            let gate = Arc::new(tokio::sync::Semaphore::new(permits));
            let arrived = Arc::new(tokio::sync::Notify::new());
            let route = |partition: &'static str, answer: &'static str| {
                let (grants, gate, arrived) = (grants.clone(), gate.clone(), arrived.clone());
                axum::routing::post(move |body: String| async move {
                    let field = |key: &str| {
                        url::form_urlencoded::parse(body.as_bytes())
                            .find(|(k, _)| k == key)
                            .map(|(_, v)| v.into_owned())
                            .unwrap_or_default()
                    };
                    grants
                        .lock()
                        .unwrap()
                        .push((partition, field("refresh_token"), field("client_id")));
                    arrived.notify_one();
                    gate.acquire().await.unwrap().forget();
                    answer
                })
            };
            let app = axum::Router::new()
                .route(
                    "/oauth/token",
                    route(
                        "outer",
                        r#"{"access_token":"outer-access-refreshed","refresh_token":"outer-ref-refreshed","expires_in":864000}"#,
                    ),
                )
                .route(
                    "/siwc/token",
                    route(
                        "siwc",
                        r#"{"access_token":"siwc-access-refreshed","refresh_token":"siwc-ref-refreshed","scope":"openid chatgpt.tokens.use.direct","expires_in":3600}"#,
                    ),
                );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let dir = std::env::temp_dir().join(format!("cooker-codex-disk-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&dir).unwrap();
            for (name, body) in files {
                std::fs::write(dir.join(name), body.to_string()).unwrap();
            }
            let (database, backend) = sqlite_backend(&dir);
            let rt = Self::runtime_backend(backend, &base);
            let dir = rt.config().auth_dir.clone();
            Self {
                database,
                rt,
                dir,
                grants,
                gate,
                arrived,
                base,
            }
        }

        /// A runtime over the auth-dir as it is now, with its own refresh coordination:
        /// another process sharing the files and the mock.
        fn other_process(&self) -> Arc<Runtime> {
            Self::runtime_backend(self.rt.store.credential_backend(), &self.base)
        }

        fn runtime_backend(backend: Arc<crate::store::credential_store::CredentialDb>, base: &str) -> Arc<Runtime> {
            let mut cfg = Config::parse("").unwrap();
            cfg.auth_dir = backend.auth_dir();
            let client = wreq::Client::new();
            let oauth = ck_exec::codex_oauth::CodexOAuth::with_endpoints(
                client.clone(),
                &format!("{base}/oauth/token"),
                "",
                "",
            )
            .with_siwc_token_url(&format!("{base}/siwc/token"))
            .with_retry_delay(Duration::from_millis(1));
            let scan = scan_private(&backend, &cfg);
            let rt = Runtime::new(
                cfg,
                scan.credentials.into_iter().map(crate::testing::local).collect(),
                Executors {
                    claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
                    codex: ck_exec::codex::CodexExecutor::with_client(client, oauth),
                    devices: Default::default(),
                    openai: Default::default(),
                    google: Default::default(),
                },
                backend,
            );
            rt.store.note_loaded(&scan.read);
            rt.deny_external.store(true, Ordering::Relaxed);
            rt.set_local_model(true);
            Arc::new(rt)
        }

        /// Waits until `n` refreshes in all went to lock this auth-dir's `name`.
        async fn lock_attempts(&self, name: &str, n: usize) {
            let path = self.dir.join(name);
            let attempted = async {
                while LOCK_ATTEMPTS.lock().unwrap().iter().filter(|p| **p == path).count() < n {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            };
            tokio::time::timeout(Duration::from_secs(5), attempted)
                .await
                .expect("the refresh went to lock the file");
        }

        /// Waits until a grant reached the mock.
        async fn granted(&self) {
            let granted = async {
                while self.grants.lock().unwrap().is_empty() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            };
            tokio::time::timeout(Duration::from_secs(5), granted)
                .await
                .expect("a grant");
        }

        fn grants(&self) -> Grants {
            std::mem::take(&mut *self.grants.lock().unwrap())
        }

        async fn wait_for_grants(&self, count: usize) {
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let arrived = self.arrived.notified();
                    if self.grants.lock().unwrap().len() >= count {
                        return;
                    }
                    arrived.await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("mock grants arrived: {} of {count}", self.grants.lock().unwrap().len()));
        }
        /// The file and the stored credential, which must agree on `fields`.
        fn tokens(&self, name: &str, fields: &[&str]) -> Vec<Value> {
            let disk: Value = serde_json::from_slice(&std::fs::read(self.dir.join(name)).unwrap()).unwrap();
            let stored = Value::Object(self.rt.store.get(name).unwrap().metadata.clone());
            let pick = |m: &Value| {
                fields
                    .iter()
                    .map(|f| m.pointer(f).cloned().unwrap_or(Value::Null))
                    .collect()
            };
            let (disk, stored): (Vec<Value>, Vec<Value>) = (pick(&disk), pick(&stored));
            assert_eq!(disk, stored, "file and store differ");
            disk
        }
    }

    fn codex_file(outer: (&str, chrono::Duration), siwc: (&str, &str, chrono::Duration)) -> Value {
        let now = chrono::Utc::now();
        serde_json::json!({
            "type": "codex",
            "access_token": format!("outer-{}", outer.0),
            "refresh_token": format!("outer-ref-{}", outer.0),
            "expired": (now + outer.1).to_rfc3339(),
            "use_siwc": true,
            "siwc": {
                "access_token": format!("siwc-{}", siwc.0),
                "refresh_token": format!("siwc-ref-{}", siwc.0),
                "client_id": siwc.1,
                "device_id": "e61bbe28-07ef-466d-8e5d-a344f94ab305",
                "expired": (now + siwc.2).to_rfc3339(),
            },
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pending_publication_preserves_two_inflight_codex_grants() {
        let mock = CodexDisk::gated(&[], 0).await;
        let root = std::env::temp_dir().join(format!("cooker-pending-grants-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let (database, backend) = sqlite_backend(&root);
        database
            .import_yaml("server: {port: 8317}\n", "fixture", "initial")
            .unwrap();
        let mut old = codex_file(
            ("R0", chrono::Duration::minutes(-1)),
            ("keep", "oaiapp_x", chrono::Duration::days(2)),
        );
        old["use_siwc"] = false.into();
        old["opaque"] = serde_json::json!({"retained": [1, 2, 3]});
        for name in ["a.json", "b.json", "c.json"] {
            let mut document = old.clone();
            document["access_token"] = format!("outer-{name}").into();
            document["refresh_token"] = format!("outer-ref-{name}").into();
            backend
                .save(&backend.auth_dir().join(name), &serde_json::to_vec(&document).unwrap())
                .unwrap();
        }
        let rt = CodexDisk::runtime_backend(backend.clone(), &mock.base);
        for name in ["a.json", "b.json", "c.json"] {
            let mut selection = Selection::new("codex", "gpt-5");
            selection.exclude = ["a.json", "b.json", "c.json"]
                .into_iter()
                .filter(|id| *id != name)
                .map(str::to_owned)
                .collect();
            assert!(
                rt.store.select(selection).is_some(),
                "positive dispatch control: {name}"
            );
        }
        let mut jobs = tokio::task::JoinSet::new();
        for name in ["a.json", "b.json"] {
            let rt = rt.clone();
            jobs.spawn(async move { rt.prepare_credential(name, &rt.config(), None).await });
        }
        tokio::select! {
            () = mock.wait_for_grants(2) => {}
            early = jobs.join_next() => panic!("grant preparation ended before both mocks arrived: {early:?}"),
        }
        let blocked = crate::testing::block_publication(database.path().unwrap());
        mock.gate.add_permits(1);
        assert!(jobs.join_next().await.unwrap().unwrap().is_err());
        assert!(
            database
                .import_yaml("server: {port: 8318}\n", "fixture", "unrelated")
                .is_err()
        );
        mock.gate.add_permits(1);
        assert!(jobs.join_next().await.unwrap().unwrap().is_err());
        assert!(jobs.is_empty());
        let checkpoint = crate::testing::database_connection(&database);
        for name in ["local:a.json", "local:b.json"] {
            let bytes: Vec<u8> = checkpoint
                .query_row("SELECT bytes FROM credential_document WHERE name=?1", [name], |row| {
                    row.get(0)
                })
                .unwrap();
            let retained: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(retained["refresh_token"], "outer-ref-refreshed", "{name}");
            assert_eq!(retained["opaque"], old["opaque"]);
        }
        drop(checkpoint);
        assert!(rt.prepare_credential("c.json", &rt.config(), None).await.is_err());
        assert_eq!(
            mock.grants.lock().unwrap().len(),
            2,
            "no new grant starts while recovery is pending"
        );
        for name in ["a.json", "b.json"] {
            let current = rt.store.get(name).unwrap();
            assert_eq!(current.str("refresh_token"), Some(format!("outer-ref-{name}").as_str()));
            assert!(
                rt.store.tokens_rejected(&current),
                "known-invalidated R0 cannot dispatch"
            );
        }
        for name in ["a.json", "b.json", "c.json"] {
            let mut selection = Selection::new("codex", "gpt-5");
            selection.exclude = ["a.json", "b.json", "c.json"]
                .into_iter()
                .filter(|id| *id != name)
                .map(str::to_owned)
                .collect();
            assert_eq!(
                rt.store.select(selection).is_some(),
                name == "c.json",
                "only affected dispatch is paused: {name}"
            );
        }
        drop(blocked);
        database
            .import_yaml("server: {port: 8318}\n", "fixture", "resolved")
            .unwrap();
        let old_a = rt.store.get("a.json").unwrap();
        let mut stale_edit = MetadataPatch::default();
        stale_edit.set.insert("label".into(), "must not erase R1".into());
        assert!(rt.store.apply_patch("a.json", old_a.revision, &stale_edit).is_err());
        assert!(
            rt.store
                .replace_file_backed(
                    "a.json",
                    old_a.revision,
                    &serde_json::to_vec(&old_a.metadata).unwrap(),
                    Some(old_a.as_ref().clone()),
                )
                .is_err()
        );
        for name in ["a.json", "b.json"] {
            let admitted = rt.prepare_credential(name, &rt.config(), None).await.unwrap();
            assert_eq!(admitted.str("refresh_token"), Some("outer-ref-refreshed"));
            assert!(!rt.store.tokens_rejected(&admitted));
        }
        assert_eq!(
            mock.grants.lock().unwrap().len(),
            2,
            "recovery adopts rather than spending R0 again"
        );
        rt.shutdown_codex_refreshes().await;
        let copy = root.join("copy.db");
        std::fs::copy(database.path().unwrap(), &copy).unwrap();
        let cold = Arc::new(crate::testing::config_store(&copy).unwrap());
        let recovered = crate::store::credential_store::CredentialDb::initialize(cold.clone()).unwrap();
        assert_eq!(cold.load().unwrap().config.port, 8318);
        for name in ["a.json", "b.json"] {
            let bytes = recovered.read(&recovered.auth_dir().join(name)).unwrap().unwrap();
            let doc: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(doc["refresh_token"], "outer-ref-refreshed");
            assert_eq!(doc["opaque"], old["opaque"]);
        }
        rt.shutdown_codex_refreshes().await;
        // Deletion after recovery must not re-enable a retained stale runtime view.
        backend.delete(&backend.auth_dir().join("a.json")).unwrap();
        assert!(rt.store.tokens_rejected(&rt.store.get("a.json").unwrap()));
        let mut deleted = Selection::new("codex", "gpt-5");
        deleted.exclude = vec!["b.json".into(), "c.json".into()];
        assert!(rt.store.select(deleted).is_none());
        drop((recovered, cold, rt, backend, database));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn returning_grant_recovers_management_projection_after_releasing_credential_guards() {
        let root = std::env::temp_dir().join(format!("returning-grant-projection-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let (database, backend) = sqlite_backend(&root);
        let path = backend.auth_dir().join("grant.json");
        backend
            .save(&path, br#"{"type":"claude","access_token":"old","refresh_token":"R0"}"#)
            .unwrap();
        let mut cfg = database.load().unwrap().config;
        cfg.auth_dir = backend.auth_dir();
        let scan = scan_private(&backend, &cfg);
        let rt = Arc::new(crate::testing::runtime_with_db(
            cfg,
            scan.credentials,
            Executors {
                claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
                codex: Default::default(),
                devices: Default::default(),
                openai: Default::default(),
                google: Default::default(),
            },
            backend.clone(),
        ));
        rt.store.note_loaded(&scan.read);
        let management = crate::management::api::Management::new(rt.clone(), database.clone());
        let events = Arc::new(Mutex::new(Vec::new()));
        let observed = events.clone();
        let subscription = database
            .subscribe_configuration(move |view| {
                if view.change == crate::store::config_store::ConfigurationChange::Committed {
                    observed.lock().unwrap().push(view.revision);
                }
                Ok(())
            })
            .unwrap();
        let (original, registered) = rt.store.get_registered("grant.json").unwrap();
        let (admitted_tx, admitted_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn({
            let store = rt.store.clone();
            move || {
                admitted_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                let result = store.apply_refresh(
                    &original.id,
                    original.revision,
                    RefreshFence { registered },
                    &MetadataPatch {
                        set: serde_json::from_value(
                            serde_json::json!({"access_token":"returned","refresh_token":"R2"}),
                        )
                        .unwrap(),
                        remove: Vec::new(),
                    },
                    |_| {},
                    |_| true,
                );
                done_tx.send(result.is_ok()).unwrap();
            }
        });
        admitted_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let blocked = crate::testing::block_publication(database.path().unwrap());
        assert!(
            database
                .import_yaml("server: {port: 8318}\n", "test", "original-event")
                .is_err()
        );
        assert!(events.lock().unwrap().is_empty());
        drop(blocked); // Storage recovers before the consumed result is committed.
        release_tx.send(()).unwrap();
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("returning grant must not deadlock in Management projection");
        worker.join().unwrap();
        let raw: Value = serde_json::from_slice(&backend.read(&path).unwrap().unwrap()).unwrap();
        assert_eq!(raw["refresh_token"], "R2");
        assert_eq!(rt.store.get("grant.json").unwrap().str("refresh_token"), Some("R2"));
        assert_eq!(*events.lock().unwrap(), vec![database.load().unwrap().rev]);
        backend.admit_work().unwrap();
        assert_eq!(events.lock().unwrap().len(), 1, "original event delivered once");
        drop((subscription, management, rt, backend, database));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pending_publication_preserves_generic_oauth_grants_without_admitting_old_tokens() {
        let mock = CodexDisk::gated(&[], 0).await;
        let root = std::env::temp_dir().join(format!("cooker-pending-oauth-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let (database, backend) = sqlite_backend(&root);
        for name in ["a.json", "b.json", "c.json"] {
            let document = serde_json::json!({
                "type": "claude", "access_token": format!("sk-ant-oat-{name}"),
                "refresh_token": format!("refresh-{name}"),
                "expired": (chrono::Utc::now() - chrono::Duration::minutes(1)).to_rfc3339(),
                "account_uuid": "private-fixture", "claude_device_ids": ["b".repeat(64)],
                "opaque": {"original": name},
            });
            backend
                .save(&backend.auth_dir().join(name), &serde_json::to_vec(&document).unwrap())
                .unwrap();
        }
        let mut cfg = Config::parse("").unwrap();
        cfg.auth_dir = backend.auth_dir();
        let scan = scan_private(&backend, &cfg);
        let endpoint = format!("{}/oauth/token", mock.base);
        let rt = Arc::new(Runtime::new(
            cfg,
            scan.credentials,
            Executors {
                claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1")
                    .unwrap()
                    .with_oauth(ck_exec::oauth::OAuth::with_endpoints(
                        wreq::Client::new(),
                        &endpoint,
                        &endpoint,
                        &endpoint,
                    )),
                codex: Default::default(),
                devices: Default::default(),
                openai: Default::default(),
                google: Default::default(),
            },
            backend.clone(),
        ));
        rt.store.note_loaded(&scan.read);
        rt.deny_external.store(true, Ordering::Relaxed);
        let mut jobs = tokio::task::JoinSet::new();
        for name in ["a.json", "b.json"] {
            let rt = rt.clone();
            jobs.spawn(async move { rt.prepare_credential(name, &rt.config(), None).await });
        }
        tokio::select! {
            () = mock.wait_for_grants(2) => {}
            early = jobs.join_next() => panic!("grant preparation ended early: {early:?}"),
        }
        let blocked = crate::testing::block_publication(database.path().unwrap());
        mock.gate.add_permits(1);
        assert!(jobs.join_next().await.unwrap().unwrap().is_err());
        assert!(
            database
                .import_yaml("server: {port: 8318}\n", "test", "unrelated")
                .is_err()
        );
        mock.gate.add_permits(1);
        assert!(jobs.join_next().await.unwrap().unwrap().is_err());
        assert!(jobs.is_empty());
        assert!(rt.prepare_credential("c.json", &rt.config(), None).await.is_err());
        assert_eq!(mock.grants.lock().unwrap().len(), 2);
        for name in ["a.json", "b.json"] {
            let current = rt.store.get(name).unwrap();
            assert_eq!(current.str("refresh_token"), Some(format!("refresh-{name}").as_str()));
            assert!(rt.store.tokens_rejected(&current));
        }
        drop(blocked);
        backend.admit_work().unwrap();
        for name in ["a.json", "b.json"] {
            let adopted = rt.prepare_credential(name, &rt.config(), None).await.unwrap();
            assert_eq!(adopted.str("refresh_token"), Some("outer-ref-refreshed"));
            assert!(!rt.store.tokens_rejected(&adopted));
            let bytes = backend.read(&backend.auth_dir().join(name)).unwrap().unwrap();
            let raw: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(raw["refresh_token"], "outer-ref-refreshed");
            assert_eq!(raw["opaque"]["original"], name);
        }
        assert_eq!(
            mock.grants.lock().unwrap().len(),
            2,
            "adoption must not repeat the consumed grant"
        );
        rt.shutdown_codex_refreshes().await;
        drop((rt, backend, database));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sqlite_codex_refresh_restores_newer_accepted_tokens_under_the_document_lock() {
        let file = codex_file(
            ("keep", chrono::Duration::days(5)),
            ("A", "oaiapp_x", chrono::Duration::minutes(1)),
        );
        let f = CodexDisk::new(&[("codex.json", &file)]).await;
        let database = f.database.clone();
        let backend = f.rt.store.credential_backend();
        let path = backend.auth_dir().join("codex.json");
        let rt = CodexDisk::runtime_backend(backend.clone(), &f.base);
        let held = ck_exec::codex_siwc_bind::lock_file(&path).unwrap();
        let refresh = tokio::spawn({
            let rt = rt.clone();
            async move { rt.prepare_credential("codex.json", &rt.config(), None).await }
        });
        lock_waited(&path).await;
        assert!(!refresh.is_finished(), "the refresh waits for the document lock");
        assert!(f.grants().is_empty(), "no grant starts while the document is locked");
        let newer = codex_file(
            ("keep", chrono::Duration::days(5)),
            ("other", "oaiapp_x", chrono::Duration::hours(1)),
        );
        // Internal mutations and refreshes use the same canonical owner, not SQL hot adoption.
        let newer_bytes = serde_json::to_vec(&newer).unwrap();
        backend.save(&path, &newer_bytes).unwrap();
        std::fs::remove_file(&path).unwrap();
        drop(held);
        let adopted = tokio::time::timeout(Duration::from_secs(5), refresh)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(adopted.metadata["siwc"]["refresh_token"], "siwc-ref-other");
        assert_eq!(Value::Object(adopted.metadata.clone()), newer);
        assert!(f.grants().is_empty(), "the owner's accepted replacement needs no grant");
        let saved_bytes = backend.read(&path).unwrap().unwrap();
        assert_eq!(serde_json::from_slice::<Value>(&saved_bytes).unwrap(), newer);
        assert_eq!(std::fs::read(&path).unwrap(), saved_bytes);
        rt.shutdown_codex_refreshes().await;
        drop(rt);
        drop(backend);
        drop(database);
        std::fs::remove_dir_all(&f.dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cancelled_codex_grant_survives_sqlite_reopen_without_working_files() {
        let file = codex_file(
            ("keep", chrono::Duration::days(5)),
            ("A", "oaiapp_x", chrono::Duration::minutes(1)),
        );
        let f = CodexDisk::gated(&[("codex.json", &file)], 0).await;
        let database = f.database.clone();
        let backend = f.rt.store.credential_backend();
        let workspace = backend.auth_dir();
        let rt = CodexDisk::runtime_backend(backend.clone(), &f.base);
        let refresh = tokio::spawn({
            let rt = rt.clone();
            async move { rt.prepare_credential("codex.json", &rt.config(), None).await }
        });
        f.granted().await;
        refresh.abort();
        assert!(refresh.await.unwrap_err().is_cancelled());
        f.gate.add_permits(1);
        rt.shutdown_codex_refreshes().await;
        let path = workspace.join("codex.json");
        let durable_bytes = backend.read(&path).unwrap().unwrap();
        let durable: Value = serde_json::from_slice(&durable_bytes).unwrap();
        assert_eq!(durable["siwc"]["refresh_token"], "siwc-ref-refreshed");
        assert_eq!(std::fs::read(&path).unwrap(), durable_bytes);
        assert_eq!(f.grants(), [("siwc", "siwc-ref-A".to_owned(), "oaiapp_x".to_owned())]);
        let backup_dir = f.dir.parent().unwrap().join("standalone");
        std::fs::create_dir(&backup_dir).unwrap();
        std::fs::copy(database.path().unwrap(), backup_dir.join("config.db")).unwrap();
        let dir = f.dir.clone();
        drop((rt, backend, database, f));
        std::fs::remove_dir_all(&workspace).unwrap();
        // The working copy is gone; the reopened store restores the refreshed token.
        let (_, reopened) = sqlite_backend(&dir);
        let restored: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(restored, durable);
        assert_eq!(reopened.auth_dir(), workspace);
        assert_eq!(std::fs::read(&path).unwrap(), durable_bytes);
        assert_eq!(reopened.read(&path).unwrap().unwrap(), durable_bytes);
        assert_eq!(
            serde_json::from_slice::<Value>(&reopened.read(&path).unwrap().unwrap()).unwrap(),
            durable
        );
        drop(reopened);
        // A cold copy has neither the original checkpoint nor its materialized cache.
        let (backup_database, backup) = sqlite_backend(&backup_dir);
        let backup_path = backup.auth_dir().join("codex.json");
        assert_eq!(backup.read(&backup_path).unwrap().unwrap(), durable_bytes);
        assert_eq!(std::fs::read(&backup_path).unwrap(), durable_bytes);
        drop(backup);
        drop(backup_database);
        std::fs::remove_dir_all(backup_dir).unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_private_codex_backend_prepares_its_canonical_documents_before_registration() {
        let dir = std::env::temp_dir().join(format!("cooker-canonical-prepare-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let file = codex_file(
            ("keep", chrono::Duration::days(5)),
            ("A", "oaiapp_x", chrono::Duration::minutes(1)),
        );
        std::fs::write(dir.join("codex.json"), file.to_string()).unwrap();
        let (database, backend) = sqlite_backend(&dir);
        let rt = CodexDisk::runtime_backend(backend.clone(), "http://127.0.0.1:1");
        assert!(
            rt.store.get("codex.json").is_some(),
            "canonical fixture documents register"
        );
        assert_eq!(backend.documents().unwrap().len(), 1);
        drop(rt);
        drop((backend, database));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sqlite_codex_save_failures_never_publish_an_in_memory_fallback() {
        for before_admission in [false, true] {
            let file = codex_file(
                ("keep", chrono::Duration::days(5)),
                ("A", "oaiapp_x", chrono::Duration::minutes(1)),
            );
            let f = CodexDisk::new(&[("codex.json", &file)]).await;
            let database = f.database.clone();
            let backend = f.rt.store.credential_backend();
            let rt = CodexDisk::runtime_backend(backend.clone(), &f.base);
            let original = rt.store.get("codex.json").unwrap();
            let path = backend.auth_dir().join("codex.json");
            let blocked = Arc::new(Mutex::new(None));
            if before_admission {
                BEFORE_ADMIT.lock().unwrap().push((
                    path.clone(),
                    Box::new({
                        let database = database.clone();
                        let blocked = blocked.clone();
                        move || *blocked.lock().unwrap() = Some(crate::testing::block_checkpoint_install(&database))
                    }),
                ));
            } else {
                *blocked.lock().unwrap() = Some(crate::testing::block_checkpoint_install(&database));
            }
            assert!(rt.prepare_credential("codex.json", &rt.config(), None).await.is_err());
            assert!(Arc::ptr_eq(&original, &rt.store.get("codex.json").unwrap()));
            let saved: Value = serde_json::from_slice(&backend.read(&path).unwrap().unwrap()).unwrap();
            assert_eq!(
                saved["siwc"]["refresh_token"],
                if before_admission {
                    "siwc-ref-refreshed"
                } else {
                    "siwc-ref-A"
                }
            );
            assert_eq!(f.grants().len(), 1);
            rt.shutdown_codex_refreshes().await;
            drop(blocked.lock().unwrap().take());
            drop(rt);
            drop(backend);
            drop(database);
            std::fs::remove_dir_all(&f.dir).unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sqlite_refresh_waits_for_non_codex_document_lock_and_adopts_newer_accepted_tokens() {
        let (rt, dir) = claude_refresh_fixture(&["claude-sqlite.json"], "", |_| async {
            panic!("the newer accepted credential must be adopted without spending another refresh token")
        })
        .await;
        let backend = rt.store.credential_backend();
        let path = dir.join("claude-sqlite.json");
        let old = std::fs::read(&path).unwrap();
        let held = ck_exec::codex_siwc_bind::lock_file(&path).unwrap();
        let refresh = tokio::spawn({
            let rt = rt.clone();
            async move { rt.prepare_credential("claude-sqlite.json", &rt.config(), None).await }
        });
        lock_waited(&path).await;
        assert!(!refresh.is_finished());
        let mut newer: Value = serde_json::from_slice(&old).unwrap();
        newer["access_token"] = "sk-ant-oat-other-fake".into();
        newer["refresh_token"] = "other-refresh".into();
        newer["expired"] = (chrono::Utc::now() + chrono::Duration::days(30)).to_rfc3339().into();
        let newer_bytes = serde_json::to_vec(&newer).unwrap();
        backend.save(&path, &newer_bytes).unwrap();
        std::fs::write(&path, &old).unwrap();
        drop(held);
        let adopted = tokio::time::timeout(Duration::from_secs(5), refresh)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(adopted.str("refresh_token"), Some("other-refresh"));
        assert_eq!(Value::Object(adopted.metadata.clone()), newer);
        assert_eq!(backend.read(&path).unwrap().unwrap(), newer_bytes);
        assert_eq!(std::fs::read(&path).unwrap(), newer_bytes);
        drop(rt);
        drop(backend);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A write that lands between reading the stored document and adopting it wins: the
    /// adoption fails as stale instead of writing the older bytes back over it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_write_after_the_refresh_read_the_document_is_not_overwritten() {
        let (rt, dir) = claude_refresh_fixture(&["claude-raced.json"], "", |_| async {
            panic!("a stale adoption must not spend a refresh token")
        })
        .await;
        let backend = rt.store.credential_backend();
        let path = dir.join("claude-raced.json");
        let mut document: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        document["expired"] = (chrono::Utc::now() + chrono::Duration::days(30)).to_rfc3339().into();
        document["refresh_token"] = "read-refresh".into();
        backend.save(&path, &serde_json::to_vec(&document).unwrap()).unwrap();
        document["refresh_token"] = "raced-refresh".into();
        let raced = serde_json::to_vec(&document).unwrap();
        BEFORE_RESTORE.lock().unwrap().push((
            path.clone(),
            Box::new({
                let (backend, path, raced) = (backend.clone(), path.clone(), raced.clone());
                move || backend.save(&path, &raced).unwrap()
            }),
        ));
        let error = rt
            .prepare_credential("claude-raced.json", &rt.config(), None)
            .await
            .unwrap_err();
        assert!(format!("{error:?}").contains("Stale"), "{error:?}");
        assert_eq!(backend.read(&path).unwrap().unwrap(), raced);
        drop(rt);
        drop(backend);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A shutdown that lands while a credential lock wait sleeps stops it, even when
    /// the lock comes free and the sleep ends at the same moment: no lock is taken (and
    /// no grant started) after shutdown. Polled by hand, so both are ready at once.
    #[tokio::test(start_paused = true)]
    async fn a_lock_wait_stops_at_shutdown_even_when_the_lock_frees() {
        let dir = std::env::temp_dir().join(format!("cooker-lock-wait-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("codex.json");
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        for _ in 0..32 {
            let held = ck_exec::codex_siwc_bind::lock_file(&path).unwrap();
            let shutdown = tokio_util::sync::CancellationToken::new();
            let mut wait = std::pin::pin!(wait_codex_lock(&path, &shutdown));
            assert!(wait.as_mut().poll(&mut cx).is_pending(), "the lock is held");
            drop(held);
            tokio::time::advance(LOCK_POLL_CEILING).await;
            shutdown.cancel();
            let std::task::Poll::Ready(waited) = wait.as_mut().poll(&mut cx) else {
                panic!("the wait ends");
            };
            assert_eq!(waited.map(drop), Err(LOCK_WAIT_STOPPED.to_owned()));
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Go `TestCodexExecutorRefreshSerializesUnderFileLockAndSkipsAlreadyRotatedToken`
    /// (7b0319fb): a due refresh waits while another process holds the file's credential
    /// lock, then reads the file: the Sign in with ChatGPT partition that process rotated
    /// is adopted, in memory and on disk, and its refresh token is not spent again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_codex_refresh_waits_for_the_file_lock_and_adopts_what_another_process_rotated() {
        let (days, minutes) = (chrono::Duration::days(5), chrono::Duration::minutes(2));
        let f = CodexDisk::new(&[(
            "codex.json",
            &codex_file(("keep", days), ("old", "oaiapp_shared", minutes)),
        )])
        .await;
        let path = f.dir.join("codex.json");
        let (locked_tx, locked) = std::sync::mpsc::channel();
        let (release_tx, release) = std::sync::mpsc::channel::<()>();
        let other = std::thread::spawn({
            let path = path.clone();
            let backend = f.rt.store.credential_backend();
            move || {
                let lock = ck_exec::codex_siwc_bind::lock_file(&path).unwrap();
                locked_tx.send(()).unwrap();
                release.recv().unwrap();
                let rotated = codex_file(("keep", days), ("other", "oaiapp_shared", chrono::Duration::hours(1)));
                backend.save(&path, rotated.to_string().as_bytes()).unwrap();
                drop(lock);
            }
        });
        locked.recv().unwrap();
        let refresh = tokio::spawn({
            let rt = f.rt.clone();
            async move { rt.prepare_credential("codex.json", &rt.config(), None).await }
        });
        f.lock_attempts("codex.json", 1).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!refresh.is_finished(), "the refresh waits for the lock");
        assert_eq!(f.grants(), [], "nothing refreshes under another process's lock");
        release_tx.send(()).unwrap();
        other.join().unwrap();
        tokio::time::timeout(Duration::from_secs(5), refresh)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(f.grants(), [], "the rotated token is not refreshed again");
        assert_eq!(
            f.tokens(
                "codex.json",
                &["/access_token", "/siwc/access_token", "/siwc/refresh_token"]
            ),
            ["outer-keep", "siwc-other", "siwc-ref-other"]
        );
        std::fs::remove_dir_all(&f.dir).unwrap();
    }

    /// The A→B→C case of the same Go test: memory holds version A, the file version B,
    /// both due. The refresh spends B's refresh tokens (and B's client ID), and C reaches
    /// memory and disk.
    #[tokio::test]
    async fn a_codex_refresh_spends_the_tokens_on_disk_not_the_stale_snapshot() {
        let hour = chrono::Duration::hours(1);
        let minute = chrono::Duration::minutes(1);
        let f = CodexDisk::new(&[("codex.json", &codex_file(("A", hour), ("A", "oaiapp_A", minute)))]).await;
        let disk_b = codex_file(("B", hour), ("B", "oaiapp_B", minute));
        f.rt.store
            .credential_backend()
            .save(&f.dir.join("codex.json"), disk_b.to_string().as_bytes())
            .unwrap();
        f.rt.prepare_credential("codex.json", &f.rt.config(), None)
            .await
            .unwrap();
        assert_eq!(
            f.grants(),
            [
                (
                    "outer",
                    "outer-ref-B".to_owned(),
                    ck_exec::codex_oauth::CLIENT_ID.to_owned()
                ),
                ("siwc", "siwc-ref-B".to_owned(), "oaiapp_B".to_owned()),
            ]
        );
        assert_eq!(
            f.tokens(
                "codex.json",
                &[
                    "/access_token",
                    "/refresh_token",
                    "/siwc/access_token",
                    "/siwc/client_id"
                ]
            ),
            [
                "outer-access-refreshed",
                "outer-ref-refreshed",
                "siwc-access-refreshed",
                "oaiapp_B"
            ]
        );
        std::fs::remove_dir_all(&f.dir).unwrap();
    }

    /// The A→B→C case when the watcher loads B while B refreshes: the commit merges
    /// against B, the tokens the refresh started from, so C is kept, not dropped as a
    /// stale refresh of A.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_codex_refresh_keeps_its_result_when_the_watcher_loads_the_adopted_tokens() {
        let (days, minute) = (chrono::Duration::days(5), chrono::Duration::minutes(1));
        let f = CodexDisk::gated(
            &[("codex.json", &codex_file(("keep", days), ("A", "oaiapp_A", minute)))],
            0,
        )
        .await;
        let disk_b = codex_file(("keep", days), ("B", "oaiapp_B", minute));
        f.rt.store
            .credential_backend()
            .save(&f.dir.join("codex.json"), disk_b.to_string().as_bytes())
            .unwrap();
        let refresh = tokio::spawn({
            let rt = f.rt.clone();
            async move { rt.prepare_credential("codex.json", &rt.config(), None).await }
        });
        f.granted().await;
        // The watcher's reload of B, while the grant for B is in flight.
        let mut reload = MetadataPatch::default();
        reload.set.insert("siwc".into(), disk_b["siwc"].clone());
        let revision = f.rt.store.get("codex.json").unwrap().revision;
        f.rt.store.apply_patch("codex.json", revision, &reload).unwrap();
        f.gate.add_permits(1);
        tokio::time::timeout(Duration::from_secs(5), refresh)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(f.grants(), [("siwc", "siwc-ref-B".to_owned(), "oaiapp_B".to_owned())]);
        assert_eq!(
            f.tokens("codex.json", &["/siwc/access_token", "/siwc/refresh_token"]),
            ["siwc-access-refreshed", "siwc-ref-refreshed"]
        );
        std::fs::remove_dir_all(&f.dir).unwrap();
    }

    /// A file whose `type` is spelled differently from the provider it loaded as
    /// (`Codex`) still takes an adopted outer partition: the stored spelling stays.
    #[tokio::test]
    async fn a_codex_refresh_adopts_a_rotated_file_whatever_its_type_spelling() {
        let (days, hour) = (chrono::Duration::days(5), chrono::Duration::hours(1));
        let mut file = codex_file(("A", days), ("keep", "oaiapp_x", hour));
        file["type"] = "Codex".into();
        file["use_siwc"] = false.into();
        let f = CodexDisk::new(&[("codex.json", &file)]).await;
        let mut rotated = file.clone();
        rotated["access_token"] = "outer-B".into();
        rotated["refresh_token"] = "outer-ref-B".into();
        f.rt.store
            .credential_backend()
            .save(&f.dir.join("codex.json"), rotated.to_string().as_bytes())
            .unwrap();
        let current = f.rt.store.get("codex.json").unwrap();
        let cfg = f.rt.config();
        assert!(f.rt.refresh_after_unauthorized(&current, &cfg).await.is_some());
        assert_eq!(f.grants(), []);
        assert_eq!(
            f.tokens("codex.json", &["/type", "/access_token", "/refresh_token"]),
            ["Codex", "outer-B", "outer-ref-B"]
        );
        std::fs::remove_dir_all(&f.dir).unwrap();
    }

    /// After a 401 on the Sign in with ChatGPT token, a file whose partition another
    /// process already rotated is adopted without forcing a refresh of the new token
    /// (Go `RejectedAccessToken` names the old one). A file that is not valid JSON fails
    /// the refresh before any grant (Go: "invalid credential JSON").
    #[tokio::test]
    async fn a_rejected_codex_token_adopts_a_rotated_file_and_a_corrupt_file_refreshes_nothing() {
        let (days, hour) = (chrono::Duration::days(5), chrono::Duration::hours(1));
        let f = CodexDisk::new(&[(
            "codex.json",
            &codex_file(("keep", days), ("rejected", "oaiapp_x", hour)),
        )])
        .await;
        let path = f.dir.join("codex.json");
        f.rt.store
            .credential_backend()
            .save(
                &path,
                codex_file(("keep", days), ("other", "oaiapp_x", hour))
                    .to_string()
                    .as_bytes(),
            )
            .unwrap();
        let cfg = f.rt.config();
        let current = f.rt.store.get("codex.json").unwrap();
        let refreshed = f.rt.refresh_after_unauthorized(&current, &cfg).await.expect("adopted");
        assert_eq!(f.grants(), []);
        assert_eq!(refreshed.metadata["siwc"]["access_token"], "siwc-other");
        assert_eq!(
            f.tokens("codex.json", &["/siwc/access_token", "/siwc/refresh_token"]),
            ["siwc-other", "siwc-ref-other"]
        );
        f.rt.store.credential_backend().save(&path, b"{corrupt").unwrap();
        assert!(f.rt.refresh_after_unauthorized(&refreshed, &cfg).await.is_none());
        assert_eq!(f.grants(), []);
        std::fs::remove_dir_all(&f.dir).unwrap();
    }

    /// The commit takes the partition as the file held it under the lock as well as the
    /// snapshot's (Go `matchesBaselineOrAfter`): memory holds A and is edited while the
    /// refresh waits for another process, which rotates the file to B. C, refreshed from
    /// B, lands over B with the edit kept, instead of being dropped as a refresh of an A
    /// that changed meanwhile, which would leave B's spent refresh token on disk.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_codex_refresh_of_an_adopted_partition_lands_after_an_edit_to_the_snapshot() {
        let (days, minute) = (chrono::Duration::days(5), chrono::Duration::minutes(1));
        let f = CodexDisk::new(&[("codex.json", &codex_file(("keep", days), ("A", "oaiapp_x", minute)))]).await;
        let path = f.dir.join("codex.json");
        let (locked_tx, locked) = std::sync::mpsc::channel();
        let (release_tx, release) = std::sync::mpsc::channel::<()>();
        let other = std::thread::spawn({
            let path = path.clone();
            let backend = f.rt.store.credential_backend();
            move || {
                let lock = ck_exec::codex_siwc_bind::lock_file(&path).unwrap();
                locked_tx.send(()).unwrap();
                release.recv().unwrap();
                let mut rotated = codex_file(("keep", days), ("B", "oaiapp_x", minute));
                rotated["priority"] = 7.into();
                backend.save(&path, rotated.to_string().as_bytes()).unwrap();
                drop(lock);
            }
        });
        locked.recv().unwrap();
        let refresh = tokio::spawn({
            let rt = f.rt.clone();
            async move { rt.prepare_credential("codex.json", &rt.config(), None).await }
        });
        f.lock_attempts("codex.json", 1).await;
        // An edit of A that keeps its tokens (the watcher, a management save).
        let mut edit = MetadataPatch::default();
        edit.set.insert("priority".into(), 7.into());
        let revision = f.rt.store.get("codex.json").unwrap().revision;
        f.rt.store.apply_patch("codex.json", revision, &edit).unwrap();
        release_tx.send(()).unwrap();
        other.join().unwrap();
        tokio::time::timeout(Duration::from_secs(5), refresh)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(f.grants(), [("siwc", "siwc-ref-B".to_owned(), "oaiapp_x".to_owned())]);
        assert_eq!(
            f.tokens(
                "codex.json",
                &["/siwc/access_token", "/siwc/refresh_token", "/priority"]
            ),
            [
                Value::from("siwc-access-refreshed"),
                Value::from("siwc-ref-refreshed"),
                Value::from(7)
            ]
        );
        std::fs::remove_dir_all(&f.dir).unwrap();
    }

    /// A refresh whose caller is cancelled once its grant is sent keeps the file's
    /// credential lock until the new tokens are written (Go `refreshLocked` writes before
    /// `WithFileLock` returns): another process waiting on the lock then reads them and
    /// does not spend the old refresh token again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cancelled_codex_refresh_writes_its_tokens_before_another_process_reads_the_file() {
        let (days, minute) = (chrono::Duration::days(5), chrono::Duration::minutes(1));
        let f = CodexDisk::gated(
            &[("codex.json", &codex_file(("keep", days), ("A", "oaiapp_x", minute)))],
            0,
        )
        .await;
        let other = f.other_process();
        let refresh = tokio::spawn({
            let rt = f.rt.clone();
            async move { rt.prepare_credential("codex.json", &rt.config(), None).await }
        });
        f.granted().await;
        refresh.abort();
        assert!(refresh.await.unwrap_err().is_cancelled());
        let waiting = tokio::spawn({
            let other = other.clone();
            async move { other.prepare_credential("codex.json", &other.config(), None).await }
        });
        f.lock_attempts("codex.json", 2).await;
        f.gate.add_permits(10);
        let adopted = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(f.grants(), [("siwc", "siwc-ref-A".to_owned(), "oaiapp_x".to_owned())]);
        assert_eq!(adopted.metadata["siwc"]["refresh_token"], "siwc-ref-refreshed");
        assert_eq!(
            f.tokens("codex.json", &["/siwc/access_token", "/siwc/refresh_token"]),
            ["siwc-access-refreshed", "siwc-ref-refreshed"]
        );
        std::fs::remove_dir_all(&f.dir).unwrap();
    }

    /// Fields other than the refreshed tokens are written as the file holds them under
    /// the lock (Go applies the refresh patch to the file it reads again): a credential
    /// disabled in the file, not yet reloaded, stays disabled after a refresh of the
    /// tokens it adopted from that file.
    #[tokio::test]
    async fn a_codex_refresh_keeps_a_disabled_flag_it_finds_in_the_file() {
        let (days, minute) = (chrono::Duration::days(5), chrono::Duration::minutes(1));
        let f = CodexDisk::new(&[("codex.json", &codex_file(("keep", days), ("A", "oaiapp_x", minute)))]).await;
        let mut disk_b = codex_file(("keep", days), ("B", "oaiapp_x", minute));
        disk_b["disabled"] = true.into();
        f.rt.store
            .credential_backend()
            .save(&f.dir.join("codex.json"), disk_b.to_string().as_bytes())
            .unwrap();
        let refreshed =
            f.rt.prepare_credential("codex.json", &f.rt.config(), None)
                .await
                .unwrap();
        assert_eq!(f.grants(), [("siwc", "siwc-ref-B".to_owned(), "oaiapp_x".to_owned())]);
        assert!(refreshed.disabled);
        assert_eq!(
            f.tokens("codex.json", &["/disabled", "/siwc/access_token"]),
            [Value::Bool(true), Value::from("siwc-access-refreshed")]
        );
        std::fs::remove_dir_all(&f.dir).unwrap();
    }

    /// After a 401, a file that rotated only the refresh token of the partition requests
    /// use still holds the rejected access token, so the refresh is forced, with the
    /// file's refresh token (Go compares the adopted access token with
    /// `RejectedAccessToken`). Both partitions.
    #[tokio::test]
    async fn a_rejected_codex_token_refreshes_when_the_file_rotated_only_its_refresh_token() {
        let (days, hour) = (chrono::Duration::days(5), chrono::Duration::hours(1));
        for use_siwc in [true, false] {
            let mut file = codex_file(("A", days), ("A", "oaiapp_x", hour));
            file["use_siwc"] = use_siwc.into();
            let f = CodexDisk::new(&[("codex.json", &file)]).await;
            let (prefix, client) = match use_siwc {
                true => ("siwc", "oaiapp_x"),
                false => ("outer", ck_exec::codex_oauth::CLIENT_ID),
            };
            let pointer = if use_siwc {
                "/siwc/refresh_token"
            } else {
                "/refresh_token"
            };
            *file.pointer_mut(pointer).unwrap() = format!("{prefix}-ref-B").into();
            f.rt.store
                .credential_backend()
                .save(&f.dir.join("codex.json"), file.to_string().as_bytes())
                .unwrap();
            let current = f.rt.store.get("codex.json").unwrap();
            let cfg = f.rt.config();
            let refreshed = f.rt.refresh_after_unauthorized(&current, &cfg).await;
            assert_eq!(
                f.grants(),
                [(
                    if use_siwc { "siwc" } else { "outer" },
                    format!("{prefix}-ref-B"),
                    client.to_owned()
                )],
                "use_siwc {use_siwc}"
            );
            let access = if use_siwc {
                "/siwc/access_token"
            } else {
                "/access_token"
            };
            let refreshed = Value::Object(refreshed.expect("refreshed").metadata.clone());
            assert_eq!(
                refreshed.pointer(access).unwrap(),
                &format!("{prefix}-access-refreshed")
            );
            assert_eq!(
                f.tokens("codex.json", &[access]),
                [format!("{prefix}-access-refreshed")]
            );
            std::fs::remove_dir_all(&f.dir).unwrap();
        }
    }

    /// A document deleted from SQLite while its refresh waits fails before any grant.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_codex_refresh_fails_when_its_document_is_deleted_while_it_waits_for_the_lock() {
        let (days, minute) = (chrono::Duration::days(5), chrono::Duration::minutes(1));
        let f = CodexDisk::new(&[("codex.json", &codex_file(("keep", days), ("A", "oaiapp_x", minute)))]).await;
        let path = f.dir.join("codex.json");
        let (locked_tx, locked) = std::sync::mpsc::channel();
        let (release_tx, release) = std::sync::mpsc::channel::<()>();
        let other = std::thread::spawn({
            let path = path.clone();
            let backend = f.rt.store.credential_backend();
            move || {
                let lock = ck_exec::codex_siwc_bind::lock_file(&path).unwrap();
                locked_tx.send(()).unwrap();
                release.recv().unwrap();
                backend.delete(&path).unwrap();
                drop(lock);
            }
        });
        locked.recv().unwrap();
        let refresh = tokio::spawn({
            let rt = f.rt.clone();
            async move { rt.prepare_credential("codex.json", &rt.config(), None).await }
        });
        f.lock_attempts("codex.json", 1).await;
        release_tx.send(()).unwrap();
        other.join().unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), refresh)
            .await
            .unwrap()
            .unwrap();
        assert!(result.is_err(), "{result:?}");
        assert_eq!(f.grants(), []);
        std::fs::remove_dir_all(&f.dir).unwrap();
    }

    /// The commit applies a partition over the snapshot's version too (Go
    /// `matchesBaselineOrAfter`): memory holds A, the refresh reads B under the lock and
    /// spends B's outer refresh token, and a writer that does not take the lock puts A
    /// back meanwhile. B's adopted Sign in with ChatGPT partition then lands over A, its
    /// numbers written as Go's float64 reading of them, and legacy keys renamed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_codex_refresh_lands_over_the_snapshot_tokens_restored_while_it_ran() {
        let (days, minute) = (chrono::Duration::days(5), chrono::Duration::minutes(1));
        let file_a = codex_file(("A", minute), ("A", "oaiapp_x", days));
        let f = CodexDisk::gated(&[("codex.json", &file_a)], 0).await;
        let path = f.dir.join("codex.json");
        let mut file_b = codex_file(("A", minute), ("B", "oaiapp_x", days));
        file_b["siwc"]["expired"] = serde_json::from_str("9007199254740993").unwrap();
        file_b["siwc"]["last_refresh"] = serde_json::from_str("2000000000.0").unwrap();
        f.rt.store
            .credential_backend()
            .save(&path, serde_json::to_string(&file_b).unwrap().as_bytes())
            .unwrap();
        let refresh = tokio::spawn({
            let rt = f.rt.clone();
            async move { rt.prepare_credential("codex.json", &rt.config(), None).await }
        });
        f.granted().await;
        let mut restored = file_a.clone();
        restored["openapi-location"] = "x".into();
        f.rt.store
            .credential_backend()
            .save(&path, restored.to_string().as_bytes())
            .unwrap();
        f.gate.add_permits(1);
        tokio::time::timeout(Duration::from_secs(5), refresh)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            f.grants(),
            [(
                "outer",
                "outer-ref-A".to_owned(),
                ck_exec::codex_oauth::CLIENT_ID.to_owned()
            )]
        );
        assert_eq!(
            f.tokens(
                "codex.json",
                &["/access_token", "/siwc/access_token", "/siwc/refresh_token"]
            ),
            ["outer-access-refreshed", "siwc-B", "siwc-ref-B"]
        );
        let written = std::fs::read_to_string(&path).unwrap();
        for want in [
            r#""expired":9007199254740992,"#,
            r#""last_refresh":2000000000,"#,
            r#""openapi_location":"x""#,
        ] {
            assert!(written.contains(want), "{want} in {written}");
        }
        std::fs::remove_dir_all(&f.dir).unwrap();
    }

    /// A credential replaced in place while its refresh ran (same registration, other
    /// tokens) keeps the replacement: no refreshed partition applies over it, so the
    /// store discards the refresh without publishing anything (Go `updateInternal`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_codex_refresh_is_discarded_when_the_credential_was_replaced_meanwhile() {
        let (days, minute) = (chrono::Duration::days(5), chrono::Duration::minutes(1));
        let f = CodexDisk::gated(
            &[("codex.json", &codex_file(("keep", days), ("A", "oaiapp_x", minute)))],
            0,
        )
        .await;
        let refresh = tokio::spawn({
            let rt = f.rt.clone();
            async move { rt.prepare_credential("codex.json", &rt.config(), None).await }
        });
        f.granted().await;
        let mut replace = MetadataPatch::default();
        replace.set.insert(
            "siwc".into(),
            codex_file(("keep", days), ("D", "oaiapp_x", days))["siwc"].clone(),
        );
        let before = f.rt.store.get("codex.json").unwrap().revision;
        let replaced = f.rt.store.apply_patch("codex.json", before, &replace).unwrap();
        f.gate.add_permits(1);
        let answered = tokio::time::timeout(Duration::from_secs(5), refresh)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(f.grants(), [("siwc", "siwc-ref-A".to_owned(), "oaiapp_x".to_owned())]);
        assert_eq!(answered.revision, replaced.revision);
        assert_eq!(f.rt.store.get("codex.json").unwrap().revision, replaced.revision);
        assert_eq!(
            f.tokens("codex.json", &["/siwc/access_token", "/siwc/refresh_token"]),
            ["siwc-D", "siwc-ref-D"]
        );
        std::fs::remove_dir_all(&f.dir).unwrap();
    }

    /// A refresh is discarded when the credential version moved while it ran and no
    /// refreshed partition still matches (Go `updateInternal`), even though the tokens
    /// were put back: here the outer partition's account changed with its token, and
    /// only the token came back.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_codex_refresh_is_discarded_when_the_credential_version_moved_meanwhile() {
        let (days, minute) = (chrono::Duration::days(5), chrono::Duration::minutes(1));
        let mut file = codex_file(("A", minute), ("keep", "oaiapp_x", days));
        file["use_siwc"] = false.into();
        file["account_id"] = "acct-A".into();
        let f = CodexDisk::gated(&[("codex.json", &file)], 0).await;
        let refresh = tokio::spawn({
            let rt = f.rt.clone();
            async move { rt.prepare_credential("codex.json", &rt.config(), None).await }
        });
        f.granted().await;
        let mut last = None;
        for edit in [
            serde_json::json!({"access_token": "outer-B", "account_id": "acct-X"}),
            serde_json::json!({"access_token": "outer-A"}),
        ] {
            let mut patch = MetadataPatch::default();
            for (k, v) in edit.as_object().unwrap() {
                patch.set.insert(k.clone(), v.clone());
            }
            let revision = f.rt.store.get("codex.json").unwrap().revision;
            last = Some(f.rt.store.apply_patch("codex.json", revision, &patch).unwrap());
        }
        let edited = last.unwrap();
        f.gate.add_permits(1);
        let answered = tokio::time::timeout(Duration::from_secs(5), refresh)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(f.grants().len(), 1);
        assert_eq!(answered.revision, edited.revision);
        assert_eq!(f.rt.store.get("codex.json").unwrap().revision, edited.revision);
        assert_eq!(
            f.tokens("codex.json", &["/access_token", "/account_id"]),
            ["outer-A", "acct-X"]
        );
        std::fs::remove_dir_all(&f.dir).unwrap();
    }

    /// A refresh that only adopted tokens another process rotated on disk (no grant) and
    /// is then discarded leaves the file as it was, and the credential's cooldowns and
    /// refresh record too: Go writes such a refresh only through the persist of an
    /// admitted update.
    #[tokio::test]
    async fn a_discarded_adopt_only_codex_refresh_changes_nothing() {
        let (days, hour) = (chrono::Duration::days(5), chrono::Duration::hours(1));
        let f = CodexDisk::new(&[("codex.json", &codex_file(("keep", days), ("A", "oaiapp_x", hour)))]).await;
        let path = f.dir.join("codex.json");
        let select = |model: &str| f.rt.store.select(Selection::new("codex", model)).unwrap();
        select("m").complete(Outcome::Failure(ExecError::local(
            401,
            FailureScope::Credential,
            "unauthorized",
        )));
        // Written after the 401, whose cooldown state is persisted too.
        let mut file_b = codex_file(("keep", days), ("B", "oaiapp_x", hour));
        file_b["request-retry"] = 2.into();
        let bytes_b = file_b.to_string();
        f.rt.store.credential_backend().save(&path, bytes_b.as_bytes()).unwrap();
        // The store takes other tokens without writing the file (a reload of another
        // copy), once the refresh adopted B.
        let cooldowns = Arc::new(Mutex::new(None));
        BEFORE_ADMIT.lock().unwrap().push((
            path.clone(),
            Box::new({
                let (store, dir, path, cooldowns) =
                    (f.rt.store.clone(), f.dir.clone(), path.clone(), cooldowns.clone());
                move || {
                    let d = codex_file(("keep", days), ("D", "oaiapp_x", days));
                    let d = Credential::from_file(&dir, &path, d.as_object().cloned().unwrap()).unwrap();
                    store.reconcile(vec![crate::testing::local(d)]);
                    // The reload's new tokens cleared the 401 (Go `updateInternalLocked`);
                    // upstream rejects D too (recorded without the file write a lease's
                    // result makes).
                    let rejected = Outcome::Failure(ExecError::local(401, FailureScope::Credential, "unauthorized"));
                    let (d, policy) = (store.get("codex.json").unwrap(), store.policy.read().unwrap().clone());
                    let mut scheduler = store.scheduler.lock().unwrap();
                    scheduler.record(&d, "m", &rejected, &policy, Instant::now());
                    drop(scheduler);
                    *cooldowns.lock().unwrap() = Some(store.cooldowns("codex.json").len());
                }
            }),
        ));
        let current = f.rt.store.get("codex.json").unwrap();
        let cfg = f.rt.config();
        assert!(f.rt.refresh_after_unauthorized(&current, &cfg).await.is_some());
        assert_eq!(f.grants(), []);
        let stored = f.rt.store.get("codex.json").unwrap();
        assert_eq!(stored.metadata["siwc"]["access_token"], "siwc-D");
        assert_ne!(stored.revision, current.revision);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), bytes_b);
        let before = cooldowns.lock().unwrap().expect("the hook ran");
        assert_eq!(before, 1, "the 401 cooldown is still there");
        assert_eq!(f.rt.store.cooldowns("codex.json").len(), before);
        assert!(
            !f.rt
                .store
                .unauthorized
                .lock()
                .unwrap()
                .refreshed
                .contains_key("codex.json")
        );
        std::fs::remove_dir_all(&f.dir).unwrap();
    }

    /// Tokens replaced and put back while the grant ran (A→D→A) move the credential
    /// version, but the refreshed partition still holds the version it started from, so
    /// the refresh is admitted (Go `CanApplyTo`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_codex_refresh_is_admitted_over_tokens_put_back_meanwhile() {
        let (days, minute) = (chrono::Duration::days(5), chrono::Duration::minutes(1));
        let file_a = codex_file(("keep", days), ("A", "oaiapp_x", minute));
        let f = CodexDisk::gated(&[("codex.json", &file_a)], 0).await;
        let refresh = tokio::spawn({
            let rt = f.rt.clone();
            async move { rt.prepare_credential("codex.json", &rt.config(), None).await }
        });
        f.granted().await;
        let version = || f.rt.store.get_registered("codex.json").unwrap().1.credential_version;
        let started = version();
        for siwc in [
            codex_file(("keep", days), ("D", "oaiapp_x", days))["siwc"].clone(),
            file_a["siwc"].clone(),
        ] {
            let mut patch = MetadataPatch::default();
            patch.set.insert("siwc".into(), siwc);
            let revision = f.rt.store.get("codex.json").unwrap().revision;
            f.rt.store.apply_patch("codex.json", revision, &patch).unwrap();
        }
        assert_ne!(version(), started, "the put-back moved the credential version");
        f.gate.add_permits(1);
        let answered = tokio::time::timeout(Duration::from_secs(5), refresh)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(answered.metadata["siwc"]["access_token"], "siwc-access-refreshed");
        assert_eq!(
            f.tokens("codex.json", &["/siwc/access_token", "/siwc/refresh_token"]),
            ["siwc-access-refreshed", "siwc-ref-refreshed"]
        );
        std::fs::remove_dir_all(&f.dir).unwrap();
    }

    /// Damaged working copies are restored from SQLite when an admitted refresh saves.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_codex_refresh_restores_a_damaged_working_copy_after_its_grant() {
        let (days, minute) = (chrono::Duration::days(5), chrono::Duration::minutes(1));
        for truncate in [false, true] {
            let f = CodexDisk::gated(
                &[("codex.json", &codex_file(("keep", days), ("A", "oaiapp_x", minute)))],
                0,
            )
            .await;
            let path = f.dir.join("codex.json");
            BEFORE_ADMIT.lock().unwrap().push((
                path.clone(),
                Box::new({
                    let path = path.clone();
                    move || match truncate {
                        true => std::fs::write(&path, r#"{"type":"#).unwrap(),
                        false => std::fs::remove_file(&path).unwrap(),
                    }
                }),
            ));
            let refresh = tokio::spawn({
                let rt = f.rt.clone();
                async move { rt.prepare_credential("codex.json", &rt.config(), None).await }
            });
            f.granted().await;
            f.gate.add_permits(1);
            let answered = tokio::time::timeout(Duration::from_secs(5), refresh)
                .await
                .unwrap()
                .unwrap()
                .expect("the refresh succeeds");
            assert_eq!(answered.metadata["siwc"]["refresh_token"], "siwc-ref-refreshed");
            let stored = f.rt.store.get("codex.json").unwrap();
            assert_eq!(stored.metadata["siwc"]["refresh_token"], "siwc-ref-refreshed");
            let disk = std::fs::read_to_string(&path).unwrap();
            let disk: Value = serde_json::from_str(&disk).unwrap();
            assert_eq!(disk, Value::Object(stored.metadata.clone()));
            assert!(
                disk.get("disabled").is_none(),
                "restoring a cache preserves the canonical document's absent field"
            );
            assert!(!stored.disabled);
            std::fs::remove_dir_all(&f.dir).unwrap();
        }
    }

    /// At shutdown, a Codex refresh still waiting for the file's lock (another process
    /// holds it) gives up without a grant, and one whose grant is in flight is waited for
    /// until its tokens are written: an exit cannot strand a spent refresh token.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_stops_codex_lock_waits_and_drains_started_grants() {
        let (days, minute) = (chrono::Duration::days(5), chrono::Duration::minutes(1));
        let f = CodexDisk::gated(
            &[
                ("locked.json", &codex_file(("keep", days), ("L", "oaiapp_x", minute))),
                ("granting.json", &codex_file(("keep", days), ("G", "oaiapp_x", minute))),
            ],
            0,
        )
        .await;
        let held = ck_exec::codex_siwc_bind::lock_file(&f.dir.join("locked.json")).unwrap();
        let refresh = |id: &'static str| {
            let rt = f.rt.clone();
            tokio::spawn(async move { rt.prepare_credential(id, &rt.config(), None).await })
        };
        let (waiting, granting) = (refresh("locked.json"), refresh("granting.json"));
        f.lock_attempts("locked.json", 1).await;
        f.granted().await;
        let shutdown = tokio::spawn({
            let rt = f.rt.clone();
            async move { rt.shutdown_codex_refreshes().await }
        });
        let waited = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .unwrap()
            .unwrap();
        assert!(waited.is_err(), "{waited:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!shutdown.is_finished(), "shutdown waits for the started grant");
        f.gate.add_permits(1);
        tokio::time::timeout(Duration::from_secs(5), shutdown)
            .await
            .unwrap()
            .unwrap();
        assert!(granting.await.unwrap().is_ok());
        assert_eq!(f.grants(), [("siwc", "siwc-ref-G".to_owned(), "oaiapp_x".to_owned())]);
        assert_eq!(
            f.tokens("granting.json", &["/siwc/refresh_token"]),
            ["siwc-ref-refreshed"]
        );
        drop(held);
        std::fs::remove_dir_all(&f.dir).unwrap();
    }

    /// The new tokens reach the file even when the store refuses the refresh: here the
    /// file was removed and created again with the spent tokens, and the ID registered
    /// again, while the grant ran. Go writes under the lock before its manager refuses
    /// the stale registration, so the next load reads the new tokens, not spent ones.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_codex_refresh_writes_its_tokens_when_the_id_was_registered_again_meanwhile() {
        let (days, minute) = (chrono::Duration::days(5), chrono::Duration::minutes(1));
        let mut file_a = codex_file(("keep", days), ("A", "oaiapp_x", minute));
        file_a["request-retry"] = 2.into();
        let f = CodexDisk::gated(&[("codex.json", &file_a)], 0).await;
        let path = f.dir.join("codex.json");
        let refresh = tokio::spawn({
            let rt = f.rt.clone();
            async move { rt.prepare_credential("codex.json", &rt.config(), None).await }
        });
        f.granted().await;
        std::fs::remove_file(&path).unwrap();
        f.rt.store
            .credential_backend()
            .save(&path, file_a.to_string().as_bytes())
            .unwrap();
        let cfg = f.rt.config();
        let load = || {
            scan_private(&f.rt.store.credential_backend(), &cfg)
                .credentials
                .into_iter()
                .map(crate::testing::local)
                .collect::<Vec<_>>()
        };
        f.rt.store.reconcile(vec![]);
        f.rt.store.reconcile(load());
        f.gate.add_permits(1);
        let answered = tokio::time::timeout(Duration::from_secs(5), refresh)
            .await
            .unwrap()
            .unwrap();
        assert!(answered.is_err(), "{answered:?}");
        let disk: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(disk["siwc"]["refresh_token"], "siwc-ref-refreshed");
        // Go's write after a grant keeps the file's keys as written.
        assert_eq!(
            (&disk["request-retry"], &disk["request_retry"]),
            (&2.into(), &Value::Null)
        );
        assert_eq!(
            f.rt.store.get("codex.json").unwrap().metadata["siwc"]["refresh_token"],
            "siwc-ref-A"
        );
        // The watcher's next reload takes the new tokens.
        f.rt.store.reconcile(load());
        assert_eq!(
            f.rt.store.get("codex.json").unwrap().metadata["siwc"]["refresh_token"],
            "siwc-ref-refreshed"
        );
        std::fs::remove_dir_all(&f.dir).unwrap();
    }

    /// A refresh publishes the proxy of the file it adopted, as requests route through
    /// it: a changed proxy replaces the loaded one, and a removed one is gone.
    #[tokio::test]
    async fn a_codex_refresh_routes_through_the_proxy_of_the_file_it_adopted() {
        let (days, hour) = (chrono::Duration::days(5), chrono::Duration::hours(1));
        let mut file = codex_file(("keep", days), ("A", "oaiapp_x", hour));
        file["proxy_url"] = "http://127.0.0.1:9/".into();
        let f = CodexDisk::new(&[("codex.json", &file)]).await;
        let cfg = f.rt.config();
        let route = || ck_exec::proxy::Proxy::effective_url(&f.rt.store.get("codex.json").unwrap(), &cfg);
        assert_eq!(route(), "http://127.0.0.1:9/");
        for (version, proxy) in [("B", Some("http://127.0.0.1:8/")), ("C", None)] {
            let mut rotated = codex_file(("keep", days), (version, "oaiapp_x", hour));
            if let Some(proxy) = proxy {
                rotated["proxy_url"] = proxy.into();
            }
            f.rt.store
                .credential_backend()
                .save(&f.dir.join("codex.json"), rotated.to_string().as_bytes())
                .unwrap();
            let current = f.rt.store.get("codex.json").unwrap();
            assert!(f.rt.refresh_after_unauthorized(&current, &cfg).await.is_some());
            assert_eq!(route(), proxy.unwrap_or(""), "version {version}");
        }
        assert_eq!(f.grants(), []);
        std::fs::remove_dir_all(&f.dir).unwrap();
    }

    /// An in-flight refresh computes from the configuration it was dispatched with: a
    /// global proxy published while it waits does not reroute it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_in_flight_refresh_keeps_the_configuration_it_was_given() {
        let (days, minute) = (chrono::Duration::days(5), chrono::Duration::minutes(1));
        let f = CodexDisk::new(&[("codex.json", &codex_file(("keep", days), ("A", "oaiapp_x", minute)))]).await;
        let given = f.rt.config();
        // Holding the credential's preparation lock keeps the dispatched job in flight.
        let held = f.rt.store.prepare_lock("codex.json").lock_owned().await;
        let before = reached("prepare", "codex.json");
        let refresh = tokio::spawn({
            let (rt, given) = (f.rt.clone(), given.clone());
            async move { rt.prepare_credential("codex.json", &given, None).await }
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while reached("prepare", "codex.json") == before {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the refresh is dispatched");
        // Meanwhile every new request would go through a closed proxy.
        let mut ambient = Config::parse("requests:\n  proxy-url: http://127.0.0.1:9/\n").unwrap();
        ambient.auth_dir = given.auth_dir.clone();
        assert!(f.rt.publish_config(ambient));
        drop(held);
        let refreshed = tokio::time::timeout(Duration::from_secs(10), refresh)
            .await
            .unwrap()
            .unwrap();
        assert!(refreshed.is_ok(), "{refreshed:?}");
        assert!(!f.grants().is_empty(), "the token endpoint was reached directly");
        std::fs::remove_dir_all(&f.dir).unwrap();
    }

    /// A credential file emptied to `{}` while the grant ran fails the refresh at the
    /// write (Go: "invalid credential JSON ... before write"): nothing is written or
    /// published.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_codex_refresh_fails_when_its_file_is_emptied_while_the_grant_runs() {
        let (days, minute) = (chrono::Duration::days(5), chrono::Duration::minutes(1));
        let f = CodexDisk::gated(
            &[("codex.json", &codex_file(("keep", days), ("A", "oaiapp_x", minute)))],
            0,
        )
        .await;
        let path = f.dir.join("codex.json");
        let refresh = tokio::spawn({
            let rt = f.rt.clone();
            async move { rt.prepare_credential("codex.json", &rt.config(), None).await }
        });
        f.granted().await;
        f.rt.store.credential_backend().save(&path, b"{}").unwrap();
        f.gate.add_permits(1);
        let answered = tokio::time::timeout(Duration::from_secs(5), refresh)
            .await
            .unwrap()
            .unwrap();
        assert!(answered.is_err(), "{answered:?}");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "{}");
        assert_eq!(
            f.rt.store.get("codex.json").unwrap().metadata["siwc"]["refresh_token"],
            "siwc-ref-A"
        );
        std::fs::remove_dir_all(&f.dir).unwrap();
    }

    /// Go 50d5fe18 and feb63984 (conductor_issue6415_test, conductor_issue6742_test): a
    /// 401 on a Codex token that is not due schedules a background refresh at once, which
    /// rotates it and clears the unauthorized cooldown, not another model's quota. A 401
    /// right after that refresh waits `refreshFailureBackoff`.
    #[tokio::test]
    async fn a_rejected_token_is_refreshed_in_the_background() {
        let (hit_tx, mut hit) = tokio::sync::mpsc::unbounded_channel();
        let app = axum::Router::new().route(
            "/oauth/token",
            axum::routing::post(move || async move {
                hit_tx.send(()).unwrap();
                r#"{"access_token":"rotated","refresh_token":"rotated-rt","expires_in":864000}"#
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let token_url = format!("http://{}/oauth/token", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let dir = std::env::temp_dir().join(format!("cooker-refresh-401-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let expires = (chrono::Utc::now() + chrono::Duration::days(9)).to_rfc3339();
        std::fs::write(
            dir.join("codex.json"),
            serde_json::json!({"type": "codex", "access_token": "outer", "refresh_token": "outer-rt", "expired": expires})
                .to_string(),
        )
        .unwrap();
        let mut cfg = Config::parse("").unwrap();
        cfg.auth_dir = dir.clone();
        let client = wreq::Client::new();
        let oauth =
            ck_exec::codex_oauth::CodexOAuth::with_endpoints(client.clone(), &token_url, &token_url, &token_url);
        let rt = Arc::new(test_runtime(
            cfg.clone(),
            ck_core::config::credentials::from_auth_dir(&cfg)
                .into_iter()
                .map(crate::testing::local)
                .collect(),
            Executors {
                claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
                codex: ck_exec::codex::CodexExecutor::with_client(client, oauth),
                devices: Default::default(),
                openai: Default::default(),
                google: Default::default(),
            },
        ));
        rt.start_auto_refresh();
        async fn quiet(hit: &mut tokio::sync::mpsc::UnboundedReceiver<()>) {
            let pending = tokio::time::timeout(Duration::from_millis(1500), hit.recv());
            assert!(pending.await.is_err(), "no refresh");
        }
        quiet(&mut hit).await; // Nine days left: not due under the 96h lead.
        let mut quota = ExecError::local(429, FailureScope::Model, "rate limited");
        quota.retry_after = Some(Duration::from_secs(3600));
        let select = |model: &str| rt.store.select(Selection::new("codex", model)).unwrap();
        select("q").complete(Outcome::Failure(quota));
        let unauthorized = || Outcome::Failure(ExecError::local(401, FailureScope::Credential, "unauthorized"));
        select("m").complete(unauthorized());
        assert_eq!(rt.store.cooldowns("codex.json").len(), 2);
        tokio::time::timeout(Duration::from_secs(5), hit.recv())
            .await
            .expect("a refresh of the rejected token");
        let rotated = async {
            loop {
                let c = rt.store.get("codex.json").unwrap();
                if c.str("access_token") == Some("rotated") {
                    break c;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        let rotated = tokio::time::timeout(Duration::from_secs(5), rotated).await.unwrap();
        let left = rt.store.cooldowns("codex.json");
        assert_eq!(
            left.iter().map(|c| (c.model.as_str(), c.status)).collect::<Vec<_>>(),
            [("q", 429)]
        );
        // Rejected again right after: the refresh waits out the backoff.
        rt.store
            .select(Selection::new("codex", "m"))
            .unwrap()
            .complete(unauthorized());
        assert_eq!(
            rt.store.unauthorized().pending.get("codex.json").map(|(r, _)| *r),
            Some(rotated.revision)
        );
        quiet(&mut hit).await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Which 401s schedule a background refresh: Codex ones that cool the credential,
    /// including a request-scoped 401 a `*-and-cooldown` rule cools; not Claude's (that
    /// waits for the post-replay port). A scan whose snapshot predates a newer revision
    /// keeps the 401 recorded on it.
    #[test]
    fn which_401s_schedule_a_background_refresh() {
        let codex = |note: &str| {
            let mut c = cred("codex.json", "codex", false);
            c.metadata.insert("refresh_token".into(), "rt".into());
            c.metadata.insert("note".into(), note.into());
            c.metadata.insert(
                "request_scoped_errors".into(),
                serde_json::json!([{"status": 401, "match": ["revoked"], "action": "stop-and-cooldown"}]),
            );
            c
        };
        let mut claude = cred("claude.json", "claude", false);
        claude.metadata.insert("refresh_token".into(), "rt".into());
        let store = test_store(vec![codex("a"), claude]);
        let fail = |provider: &str, model: &str, scope, text: &str| {
            store
                .select(Selection::new(provider, model))
                .unwrap()
                .complete(Outcome::Failure(ExecError::local(401, scope, text)));
        };
        let pending = || {
            let mut ids: Vec<_> = store
                .unauthorized()
                .pending
                .into_iter()
                .map(|(id, (r, _))| (id, r))
                .collect();
            ids.sort();
            ids
        };
        fail("claude", "m", FailureScope::Credential, "unauthorized");
        fail("codex", "m", FailureScope::Request, "bad request");
        assert_eq!(pending(), [], "Claude, and an unforced request-scoped 401");
        fail("codex", "m", FailureScope::Request, "token revoked");
        let first = store.get("codex.json").unwrap().revision;
        assert_eq!(pending(), [("codex.json".to_owned(), first)], "a forced cooldown");
        // The scan's snapshot holds the first revision; a refresh commits a newer one,
        // which an upstream rejects too, before the scan reads the rejections.
        let old = store.snapshot();
        store.reconcile(vec![
            codex("b"),
            store.get("claude.json").map(|c| (*c).clone()).unwrap(),
        ]);
        let second = store.get("codex.json").unwrap().revision;
        assert!(second > first);
        fail("codex", "n", FailureScope::Request, "token revoked");
        store.forget_stale_unauthorized(&store.unauthorized(), &old);
        assert_eq!(
            pending(),
            [("codex.json".to_owned(), second)],
            "a stale snapshot keeps it"
        );
        store.forget_stale_unauthorized(&store.unauthorized(), &store.snapshot());
        assert_eq!(pending(), [("codex.json".to_owned(), second)], "still current");
        let seen = store.unauthorized();
        store.reconcile(vec![
            codex("c"),
            store.get("claude.json").map(|c| (*c).clone()).unwrap(),
        ]);
        store.forget_stale_unauthorized(&seen, &store.snapshot());
        assert_eq!(pending(), [], "replaced");
    }

    /// Go `mergeAuthContent` and `CredentialRefreshPatch.Apply` for a refresh that lost
    /// the race to another edit (metadata_merge_test.go, refresh_patch_test.go).
    #[test]
    fn a_stale_refresh_merges_like_go() {
        use serde_json::json;
        let map = |v: Value| v.as_object().unwrap().clone();
        let codex = json!({"type": "codex", "access_token": "a", "refresh_token": "r", "account_id": "acct", "email": "e@x", "plan_type": "plus"});
        let refresh = |plan: &str, email: &str| json!({"type": "codex", "access_token": "a2", "refresh_token": "r2", "account_id": "acct", "email": email, "plan_type": plan});
        let edit = |base: &Value, k: &str, v: Value| {
            let mut m = map(base.clone());
            m.insert(k.into(), v);
            Value::Object(m)
        };
        let kimi = json!({"type": "kimi", "note": "a", "expired": "t0", "gone": "a"});
        let siwc = json!({"type": "codex", "access_token": "a", "refresh_token": "r", "account_id": "acct",
            "siwc": {"access_token": "s", "refresh_token": "sr", "client_id": "c", "expired": "t0", "note": "n"}});
        let siwc_refresh = json!({"siwc": {"access_token": "s2", "refresh_token": "sr2", "client_id": "c", "expired": "t1", "note": "n"}});
        let siwc_edit = |k: &str, v: Value| {
            let mut m = map(siwc.clone());
            m["siwc"][k] = v;
            Value::Object(m)
        };
        for (name, provider, base, user, set, remove, want_set, want_remove) in [
            (
                "an unchanged value keeps the edit, a changed token is applied",
                "kimi",
                &kimi,
                edit(&kimi, "note", json!("b")),
                json!({"type": "kimi", "note": "a", "access_token": "new"}),
                vec![],
                json!({"access_token": "new"}),
                vec![],
            ),
            (
                "both changed a normal field: the edit wins",
                "kimi",
                &kimi,
                edit(&kimi, "note", json!("b")),
                json!({"note": "x"}),
                vec![],
                json!({}),
                vec![],
            ),
            (
                "both changed a token payload: the refresh wins",
                "kimi",
                &kimi,
                edit(&kimi, "expired", json!("t2")),
                json!({"expired": "t1"}),
                vec![],
                json!({"expired": "t1"}),
                vec![],
            ),
            (
                "a removal of an edited key is dropped, of an untouched key kept",
                "kimi",
                &kimi,
                edit(&kimi, "expired", json!("t2")),
                json!({}),
                vec!["expired", "gone"],
                json!({}),
                vec!["gone"],
            ),
            (
                "JSON null is an edit of a missing key",
                "kimi",
                &kimi,
                edit(&kimi, "fresh", Value::Null),
                json!({"fresh": "x"}),
                vec![],
                json!({}),
                vec![],
            ),
            (
                "Codex: an unchanged plan and email keep the edit",
                "codex",
                &codex,
                edit(&codex, "plan_type", json!("team")),
                refresh("plus", "e@x"),
                vec![],
                json!({"access_token": "a2", "refresh_token": "r2"}),
                vec![],
            ),
            (
                "Codex: a changed plan and email are applied over the edit",
                "codex",
                &codex,
                edit(&edit(&codex, "plan_type", json!("team")), "email", json!("u@x")),
                refresh("pro", "n@x"),
                vec![],
                json!({"access_token": "a2", "refresh_token": "r2", "plan_type": "pro", "email": "n@x"}),
                vec![],
            ),
            (
                "Codex SIWC: the refreshed fields land over an edit inside the partition",
                "codex",
                &siwc,
                siwc_edit("note", json!("edited")),
                siwc_refresh.clone(),
                vec![],
                json!({"siwc": {"access_token": "s2", "refresh_token": "sr2", "client_id": "c", "expired": "t1", "note": "edited"}}),
                vec![],
            ),
            (
                "Codex SIWC: another client drops the partition",
                "codex",
                &siwc,
                siwc_edit("client_id", json!("other")),
                siwc_refresh.clone(),
                vec![],
                json!({}),
                vec![],
            ),
            (
                "Codex SIWC: an edit already holding the result keeps it",
                "codex",
                &siwc,
                siwc_edit("access_token", json!("s2")),
                json!({"siwc": {"access_token": "s2", "refresh_token": "sr", "client_id": "c", "expired": "t0", "note": "n"}}),
                vec![],
                json!({}),
                vec![],
            ),
            (
                "Codex: another account drops the whole partition",
                "codex",
                &codex,
                edit(&codex, "account_id", json!("other")),
                refresh("pro", "n@x"),
                vec![],
                json!({}),
                vec![],
            ),
        ] {
            let patch = MetadataPatch {
                set: map(set),
                remove: remove.into_iter().map(str::to_owned).collect(),
            };
            let (out, _) = rebase_patch(provider, &map(base.clone()), &map(user), &patch);
            assert_eq!(Value::Object(out.set), want_set, "{name}");
            assert_eq!(out.remove, want_remove, "{name}");
        }
    }

    /// Go `CredentialRefreshPatch.CanApplyTo`: a refreshed partition that still matches
    /// the stored version applies even after the other partition's tokens changed.
    #[test]
    fn a_matching_codex_partition_applies_after_the_other_changed() {
        use serde_json::json;
        let map = |v: Value| v.as_object().unwrap().clone();
        let base = json!({"type": "codex", "access_token": "a", "refresh_token": "r",
            "siwc": {"access_token": "s", "refresh_token": "sr", "client_id": "c"}});
        let patch = MetadataPatch {
            set: map(json!({"siwc": {"access_token": "s2", "refresh_token": "sr2", "client_id": "c"}})),
            remove: vec![],
        };
        let mut outer_changed = map(base.clone());
        outer_changed.insert("access_token".into(), "other".into());
        let (out, applies) = rebase_patch("codex", &map(base.clone()), &outer_changed, &patch);
        assert!(applies);
        assert_eq!(out.set["siwc"]["access_token"], "s2");
        let mut siwc_changed = map(base.clone());
        siwc_changed["siwc"]["refresh_token"] = "rotated-elsewhere".into();
        let (out, applies) = rebase_patch("codex", &map(base.clone()), &siwc_changed, &patch);
        assert!(!applies && out.set.is_empty());
        // Only Codex partitions qualify.
        assert!(!rebase_patch("claude", &map(base.clone()), &outer_changed, &patch).1);
    }

    /// Go `CredentialsChanged` compares the token requests use, which a `use_siwc`
    /// attribute alone can switch.
    #[test]
    fn switching_to_the_siwc_token_changes_the_credentials() {
        let mut a = cred("codex.json", "codex", false);
        a.metadata.insert("access_token".into(), "outer".into());
        a.metadata
            .insert("siwc".into(), serde_json::json!({"access_token": "nested"}));
        let mut b = a.clone();
        assert!(!credentials_changed(&a, &b));
        b.attributes.insert("use_siwc".into(), "true".into());
        assert!(credentials_changed(&a, &b));
    }

    /// Log lines written on this thread while the guard lives.
    #[derive(Clone, Default)]
    struct Logs(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for Logs {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl Logs {
        fn install(&self) -> tracing::subscriber::DefaultGuard {
            let writer = self.clone();
            let subscriber = tracing_subscriber::fmt()
                .with_ansi(false)
                .without_time()
                .with_writer(move || writer.clone())
                .finish();
            tracing::subscriber::set_default(subscriber)
        }
        fn text(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
        async fn wait_for(&self, needle: &str, count: usize) {
            let found = tokio::time::timeout(Duration::from_secs(10), async {
                while self.text().matches(needle).count() < count {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            });
            found
                .await
                .unwrap_or_else(|_| panic!("{needle:?} not logged {count} times:\n{}", self.text()));
        }
    }

    /// A due Claude refresh that fails logs one warning with the status, the OAuth error
    /// and the next attempt, and is tried again when its backoff ends: invalid_grant
    /// after Go's 1 min, a 5xx (retried in place twice) after 5 min. A job that panics
    /// fails alone, a scan that panics restarts the loop, and the next success commits.
    /// Backoffs are ended by hand ([`RefreshState::expire`]) instead of waited for.
    #[tokio::test]
    async fn a_failed_background_refresh_is_logged_and_retried_after_its_backoff() {
        let answers = Arc::new(Mutex::new(std::collections::VecDeque::from([
            (
                400,
                r#"{"error":"invalid_grant","error_description":"Refresh token sk-ant-ort01-leaked is invalid"}"#,
            ),
            (500, r#"{"error":"server_error"}"#),
            (500, r#"{"error":"server_error"}"#),
            (500, r#"{"error":"server_error"}"#),
            (
                200,
                r#"{"access_token":"sk-ant-oat-new-fake","refresh_token":"fake-rt-2","expires_in":28800}"#,
            ),
        ])));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let app = axum::Router::new().route(
            "/token",
            axum::routing::post({
                let (answers, calls) = (answers.clone(), calls.clone());
                move || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let (status, body) = answers.lock().unwrap().pop_front().expect("an unexpected exchange");
                    (axum::http::StatusCode::from_u16(status).unwrap(), body)
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let dir = std::env::temp_dir().join(format!("cooker-refresh-fail-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("claude-backoff.json");
        std::fs::write(
            &path,
            serde_json::json!({"type": "claude", "access_token": "sk-ant-oat-old-fake", "refresh_token": "fake-rt-1",
                "expired": (chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339(),
                "account_uuid": "acct-fake", "claude_device_ids": ["b".repeat(64)]})
            .to_string(),
        )
        .unwrap();
        let mut cfg = Config::parse("").unwrap();
        cfg.auth_dir = dir.clone();
        let (token, profile) = (format!("{base}/token"), format!("{base}/profile"));
        let oauth = ck_exec::oauth::OAuth::with_endpoints(wreq::Client::new(), &token, &profile, &profile);
        let rt = Arc::new(test_runtime(
            cfg.clone(),
            ck_core::config::credentials::from_auth_dir(&cfg)
                .into_iter()
                .map(crate::testing::local)
                .collect(),
            Executors {
                claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1")
                    .unwrap()
                    .with_oauth(oauth),
                codex: Default::default(),
                devices: Default::default(),
                openai: Default::default(),
                google: Default::default(),
            },
        ));
        let logs = Logs::default();
        let _guard = logs.install();
        let failed = "credential refresh failed for claude (claude-backoff.json): status";
        let retry_in = |rt: &Runtime| {
            let c = rt.store.get("claude-backoff.json").unwrap();
            let at = rt.refresh_state.lock().unwrap().retry_at(&c).unwrap();
            at.saturating_duration_since(Instant::now())
        };
        // Ends the current backoff and wakes the loop.
        let expire = |rt: &Runtime| {
            let c = rt.store.get("claude-backoff.json").unwrap();
            rt.refresh_state.lock().unwrap().expire(&c);
            rt.notify_config(rt.config());
        };
        rt.start_auto_refresh();
        logs.wait_for(failed, 1).await;
        assert!(
            logs.text().contains(&format!(
                "{failed} 400: Claude OAuth acquisition failed: invalid_grant; next attempt in 60s"
            )),
            "{}",
            logs.text()
        );
        assert!(
            (55..=60).contains(&retry_in(&rt).as_secs()),
            "Go invalidGrantBackoffBase"
        );
        // A job that panics is that job's failure; the loop goes on.
        tests::arm_panic("prepare", "claude-backoff.json");
        expire(&rt);
        logs.wait_for(failed, 2).await;
        assert!(
            logs.text()
                .contains(&format!("{failed} 500: refresh panicked: injected prepare panic"))
        );
        expire(&rt);
        logs.wait_for(failed, 3).await;
        assert!(
            logs.text().contains(&format!(
                "{failed} 500: Claude OAuth acquisition failed: server_error; next attempt in 300s"
            )),
            "{}",
            logs.text()
        );
        assert!(
            (295..=300).contains(&retry_in(&rt).as_secs()),
            "Go refreshFailureBackoff"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            4,
            "one invalid_grant, then three 5xx tries in place"
        );
        // A scan that panics restarts the loop, which then refreshes.
        tests::arm_panic("scan", "claude-backoff.json");
        expire(&rt);
        logs.wait_for("credential auto-refresh loop panicked (injected scan panic)", 1)
            .await;
        let committed = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let c = rt.store.get("claude-backoff.json").unwrap();
                if c.str("access_token") == Some("sk-ant-oat-new-fake") {
                    break c;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the retry after the backoff commits");
        assert_eq!(committed.str("refresh_token"), Some("fake-rt-2"));
        let saved: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(saved["access_token"], "sk-ant-oat-new-fake");
        assert_eq!(logs.text().matches(failed).count(), 3, "{}", logs.text());
        let text = logs.text();
        assert!(
            !text.contains("fake-rt") && !text.contains("sk-ant-o"),
            "no token in the logs: {text}"
        );
        rt.stop_auto_refresh();
        rt.shutdown_codex_refreshes().await;
        drop(rt);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A Claude runtime over one credential file in a private credential directory whose token
    /// endpoint answers every refresh with `answer`; counts the exchanges.
    async fn claude_refusing(
        name: &str,
        file: Value,
        answer: (u16, &'static str),
    ) -> (Arc<Runtime>, std::path::PathBuf, Arc<std::sync::atomic::AtomicUsize>) {
        claude_gated(name, file, answer, None).await
    }

    /// Holds [`claude_gated`]'s token exchanges: each notifies `entered`, then answers
    /// once it takes a `release` permit.
    #[derive(Clone)]
    struct Gate {
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Semaphore>,
    }

    impl Gate {
        fn new() -> Self {
            Self {
                entered: Arc::new(tokio::sync::Notify::new()),
                release: Arc::new(tokio::sync::Semaphore::new(0)),
            }
        }
    }

    /// [`claude_refusing`] whose exchanges wait on `gate`.
    async fn claude_gated(
        name: &str,
        file: Value,
        answer: (u16, &'static str),
        gate: Option<Gate>,
    ) -> (Arc<Runtime>, std::path::PathBuf, Arc<std::sync::atomic::AtomicUsize>) {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let app = axum::Router::new().route(
            "/token",
            axum::routing::post({
                let calls = calls.clone();
                move || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    if let Some(gate) = gate {
                        gate.entered.notify_one();
                        gate.release.acquire().await.unwrap().forget();
                    }
                    (axum::http::StatusCode::from_u16(answer.0).unwrap(), answer.1)
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let dir = std::env::temp_dir().join(format!("cooker-{name}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(format!("{name}.json")), file.to_string()).unwrap();
        let mut cfg = Config::parse("").unwrap();
        cfg.auth_dir = dir.clone();
        let (token, profile) = (format!("{base}/token"), format!("{base}/profile"));
        let oauth = ck_exec::oauth::OAuth::with_endpoints(wreq::Client::new(), &token, &profile, &profile);
        let rt = Arc::new(test_runtime(
            cfg.clone(),
            ck_core::config::credentials::from_auth_dir(&cfg)
                .into_iter()
                .map(crate::testing::local)
                .collect(),
            Executors {
                claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1")
                    .unwrap()
                    .with_oauth(oauth),
                codex: Default::default(),
                devices: Default::default(),
                openai: Default::default(),
                google: Default::default(),
            },
        ));
        (rt, dir, calls)
    }

    fn claude_file(access: &str, expires_in: chrono::Duration, disabled: bool) -> Value {
        serde_json::json!({"type": "claude", "access_token": access, "refresh_token": "fake-rt-1",
            "expired": (chrono::Utc::now() + expires_in).to_rfc3339(), "disabled": disabled,
            "account_uuid": "acct-fake", "claude_device_ids": ["b".repeat(64)]})
    }

    /// Go `refreshAuthForRequestAtEpoch`: when the refresh after an upstream 401 is
    /// refused with invalid_grant while the refused access token is still current, the
    /// credential is neither selected nor refreshed again until its tokens change; a
    /// file with new tokens lifts that at once.
    #[tokio::test]
    async fn an_invalid_grant_after_a_401_blocks_the_credential_until_its_tokens_change() {
        let name = "claude-revoked";
        let id = format!("{name}.json");
        let refused = (400, r#"{"error":"invalid_grant","error_description":"revoked"}"#);
        let file = claude_file("sk-ant-oat-old-fake", chrono::Duration::hours(1), false);
        let (rt, dir, calls) = claude_refusing(name, file, refused).await;
        let cfg = rt.config();
        let claude = || Selection {
            provider: "claude".into(),
            ..Selection::default()
        };
        let rejected = rt.store.get(&id).unwrap();
        assert!(rt.store.select(claude()).is_some(), "selectable before the 401");
        // Two requests that both got the 401: the second waits on the preparation lock,
        // then finds the rejection and sends nothing.
        let both = [0, 1].map(|_| {
            let (rt, rejected, cfg) = (rt.clone(), rejected.clone(), cfg.clone());
            tokio::spawn(async move { rt.refresh_after_unauthorized(&rejected, &cfg).await })
        });
        for job in both {
            assert!(job.await.unwrap().is_none());
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1, "one token request for both 401s");
        let current = rt.store.get(&id).unwrap();
        assert!(rt.store.tokens_rejected(&current));
        assert!(rt.store.blocked(&current, ""));
        assert!(rt.store.select(claude()).is_none(), "no longer selected");
        // A refresh that starts after the rejection refuses too.
        assert_eq!(rt.prepare_credential(&id, &cfg, None).await.unwrap_err().status, 401);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // The background loop leaves it alone too, and so does the credits fallback.
        assert!(rt.store.rejected_ids(&rt.store.snapshot()).contains(&id));
        let lease = rt.lease_credential(&id, claude(), &cfg, |c| c.clone()).await;
        assert!(lease.is_none(), "the credits fallback leased a refused credential");
        // Deleted, then imported again with the same tokens: a new credential.
        let reload = |rt: &Runtime| reload_private(rt, &dir);
        let bytes = std::fs::read(dir.join(&id)).unwrap();
        rt.store.credential_backend().delete(&dir.join(&id)).unwrap();
        reload(&rt);
        assert!(rt.store.get(&id).is_none());
        rt.store.credential_backend().save(&dir.join(&id), &bytes).unwrap();
        reload(&rt);
        let imported = rt.store.get(&id).unwrap();
        assert!(
            !rt.store.tokens_rejected(&imported),
            "a re-import inherited the rejection"
        );
        assert!(rt.store.select(claude()).is_some());
        assert!(rt.refresh_after_unauthorized(&imported, &cfg).await.is_none());
        assert!(rt.store.tokens_rejected(&rt.store.get(&id).unwrap()));
        // A new sign-in writes other tokens: selectable again.
        let file = claude_file("sk-ant-oat-new-fake", chrono::Duration::hours(1), false);
        rt.store
            .credential_backend()
            .save(&dir.join(&id), file.to_string().as_bytes())
            .unwrap();
        reload_private(&rt, &dir);
        let replaced = rt.store.get(&id).unwrap();
        assert!(!rt.store.tokens_rejected(&replaced));
        assert_eq!(rt.store.select(claude()).unwrap().credential.id, id);
        rt.shutdown_codex_refreshes().await;
        drop(rt); // Joins the cooldown writer before its fixture directory is removed.
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Refreshes queued on the preparation lock behind a 401 refresh that is then refused
    /// with invalid_grant (a request's due preparation and a background 401 job) find
    /// the rejection under the lock and send nothing.
    #[tokio::test]
    async fn refreshes_queued_behind_a_refused_one_send_nothing() {
        let name = "claude-queued";
        let id = format!("{name}.json");
        let refused = (400, r#"{"error":"invalid_grant"}"#);
        let file = claude_file("sk-ant-oat-old-fake", chrono::Duration::hours(1), false);
        let gate = Gate::new();
        let (rt, dir, calls) = claude_gated(name, file, refused, Some(gate.clone())).await;
        let cfg = rt.config();
        let credential = rt.store.get(&id).unwrap();
        let first = {
            let (rt, c, cfg) = (rt.clone(), credential.clone(), cfg.clone());
            tokio::spawn(async move { rt.refresh_after_unauthorized(&c, &cfg).await })
        };
        gate.entered.notified().await;
        let queued = {
            let (rt, id, cfg) = (rt.clone(), id.clone(), cfg.clone());
            tokio::spawn(async move { rt.prepare_credential(&id, &cfg, None).await })
        };
        let job = tokio::spawn(run_refresh_job(
            rt.clone(),
            cfg.clone(),
            Arc::new(tokio::sync::Semaphore::new(1)),
            credential.clone(),
            true,
        ));
        // Both reach the preparation lock while the exchange is held.
        tokio::time::sleep(Duration::from_millis(300)).await;
        gate.release.add_permits(3);
        assert!(first.await.unwrap().is_none());
        assert_eq!(queued.await.unwrap().unwrap_err().status, 401);
        job.await.unwrap();
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "a queued refresh sent another exchange"
        );
        assert!(rt.store.tokens_rejected(&rt.store.get(&id).unwrap()));
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Go `refreshAuthForRequestAtEpoch` from the scheduled 401 refresh: when the refresh
    /// right after the 401 failed for another reason, the background job that retries it
    /// records the rejection on invalid_grant.
    #[tokio::test]
    async fn a_background_401_refresh_records_the_rejection() {
        let name = "claude-revoked-later";
        let id = format!("{name}.json");
        let refused = (400, r#"{"error":"invalid_grant"}"#);
        let file = claude_file("sk-ant-oat-old-fake", chrono::Duration::hours(1), false);
        let (rt, dir, calls) = claude_refusing(name, file, refused).await;
        let credential = rt.store.get(&id).unwrap();
        let workers = Arc::new(tokio::sync::Semaphore::new(1));
        run_refresh_job(rt.clone(), rt.config(), workers, credential, true).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            rt.store.tokens_rejected(&rt.store.get(&id).unwrap()),
            "the scheduled 401 refresh recorded nothing"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A refusal that lands after its credential was removed and imported again with the
    /// same tokens rejects nothing: the import is a new registration (Go
    /// `RegistrationEpoch`).
    #[tokio::test]
    async fn a_late_refusal_never_blocks_a_reimported_credential() {
        let name = "claude-reimported";
        let id = format!("{name}.json");
        let refused = (400, r#"{"error":"invalid_grant"}"#);
        let file = claude_file("sk-ant-oat-old-fake", chrono::Duration::hours(1), false);
        let gate = Gate::new();
        let (rt, dir, calls) = claude_gated(name, file, refused, Some(gate.clone())).await;
        let cfg = rt.config();
        let claude = || Selection {
            provider: "claude".into(),
            ..Selection::default()
        };
        let credential = rt.store.get(&id).unwrap();
        let job = {
            let (rt, c, cfg) = (rt.clone(), credential.clone(), cfg.clone());
            tokio::spawn(async move { rt.refresh_after_unauthorized(&c, &cfg).await })
        };
        gate.entered.notified().await;
        let reload = |rt: &Runtime| reload_private(rt, &dir);
        let bytes = std::fs::read(dir.join(&id)).unwrap();
        rt.store.credential_backend().delete(&dir.join(&id)).unwrap();
        reload(&rt);
        assert!(rt.store.get(&id).is_none());
        rt.store.credential_backend().save(&dir.join(&id), &bytes).unwrap();
        reload(&rt);
        gate.release.add_permits(1);
        assert!(job.await.unwrap().is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let imported = rt.store.get(&id).unwrap();
        assert!(
            !rt.store.tokens_rejected(&imported),
            "the old registration's refusal blocked the import"
        );
        assert_eq!(rt.store.select(claude()).unwrap().credential.id, id);
        // Selection feedback queues tracked normalization as well as cooldowns.
        rt.shutdown_codex_refreshes().await;
        drop(rt);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A rejection is recorded only for the tokens the store still holds: a failure for
    /// tokens A that lands after a rotation to B neither blocks B nor lifts B's own
    /// rejection; an empty access token is never rejected (Go `accessTokenRejected`).
    #[tokio::test]
    async fn a_stale_rejection_never_blocks_or_unblocks_rotated_tokens() {
        let name = "claude-rotated";
        let id = format!("{name}.json");
        let file = claude_file("sk-ant-oat-a-fake", chrono::Duration::hours(1), false);
        let (rt, dir, _) = claude_refusing(name, file, (500, "{}")).await;
        let invalid = ExecError::local(400, FailureScope::Request, r#"{"error":"invalid_grant"}"#);
        let a = rt.store.get(&id).unwrap();
        let registration = rt.store.get_registered(&id).unwrap().1.registration;
        let other = ExecError::local(400, FailureScope::Request, r#"{"error":"server_error"}"#);
        assert!(!rt.store.reject_refused(&a, registration, &other), "not invalid_grant");
        rt.store
            .credential_backend()
            .save(
                &dir.join(&id),
                claude_file("sk-ant-oat-b-fake", chrono::Duration::hours(1), false)
                    .to_string()
                    .as_bytes(),
            )
            .unwrap();
        reload_private(&rt, &dir);
        let b = rt.store.get(&id).unwrap();
        assert!(!rt.store.reject_refused(&a, registration, &invalid));
        assert!(!rt.store.tokens_rejected(&b), "a stale failure blocked the new tokens");
        assert!(rt.store.reject_refused(&b, registration, &invalid));
        assert!(!rt.store.reject_refused(&a, registration, &invalid));
        assert!(
            rt.store.tokens_rejected(&b),
            "a stale failure lifted the new tokens' rejection"
        );
        // A file whose access token is blank: nothing to compare, so never rejected.
        rt.store
            .credential_backend()
            .save(
                &dir.join(&id),
                claude_file(" ", chrono::Duration::hours(1), false)
                    .to_string()
                    .as_bytes(),
            )
            .unwrap();
        reload_private(&rt, &dir);
        let empty = rt.store.get(&id).unwrap();
        assert_eq!(empty.str("access_token"), Some(" "));
        assert!(
            !rt.store.reject_refused(&empty, registration, &invalid),
            "empty access token"
        );
        assert!(!rt.store.tokens_rejected(&empty));
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A due refresh of a disabled credential is skipped with one log line per
    /// revision, and nothing is sent to the token endpoint.
    #[tokio::test]
    async fn a_skipped_refresh_of_a_disabled_credential_is_logged_once() {
        let name = "claude-off";
        // Claude refreshes 4 h before expiry: due 3 s after the loop's first scan, which
        // must wake for it on its own.
        let expires = chrono::Duration::hours(4) + chrono::Duration::seconds(3);
        let file = claude_file("sk-ant-oat-old-fake", expires, true);
        let (rt, dir, calls) = claude_refusing(name, file, (500, "{}")).await;
        let logs = Logs::default();
        let _guard = logs.install();
        let skipped = "credential refresh for claude (claude-off.json) is due but the credential is disabled";
        rt.start_auto_refresh();
        logs.wait_for(skipped, 1).await;
        for _ in 0..2 {
            rt.notify_config(rt.config());
            tokio::time::sleep(Duration::from_millis(1500)).await;
        }
        assert_eq!(logs.text().matches(skipped).count(), 1, "{}", logs.text());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        rt.stop_auto_refresh();
        rt.shutdown_codex_refreshes().await;
        drop(rt);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Claude credential files `ids` due for a refresh within the hour (each with its own
    /// refresh token, `fake-rt-<id>`), with a token endpoint answering each request body
    /// by `answer` on loopback, and their runtime under config `yaml`. Test hooks are keyed by credential ID, so each test uses its own IDs.
    async fn claude_refresh_fixture<F, Fut>(ids: &[&str], yaml: &str, answer: F) -> (Arc<Runtime>, std::path::PathBuf)
    where
        F: Fn(String) -> Fut + Clone + Send + Sync + 'static,
        Fut: std::future::Future<Output = (axum::http::StatusCode, &'static str)> + Send + 'static,
    {
        let app = axum::Router::new().route("/token", axum::routing::post(move |body: String| answer(body)));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let dir = std::env::temp_dir().join(format!("cooker-refresh-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        for id in ids {
            std::fs::write(
                dir.join(id),
                serde_json::json!({"type": "claude", "access_token": "sk-ant-oat-old-fake",
                    "refresh_token": format!("fake-rt-{id}"),
                    "expired": (chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339(),
                    "account_uuid": "acct-fake", "claude_device_ids": ["b".repeat(64)]})
                .to_string(),
            )
            .unwrap();
        }
        let mut cfg = Config::parse(yaml).unwrap();
        cfg.auth_dir = dir.clone();
        let (token, profile) = (format!("{base}/token"), format!("{base}/profile"));
        let oauth = ck_exec::oauth::OAuth::with_endpoints(wreq::Client::new(), &token, &profile, &profile);
        let rt = Arc::new(test_runtime(
            cfg.clone(),
            ck_core::config::credentials::from_auth_dir(&cfg)
                .into_iter()
                .map(crate::testing::local)
                .collect(),
            Executors {
                claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1")
                    .unwrap()
                    .with_oauth(oauth),
                codex: Default::default(),
                devices: Default::default(),
                openai: Default::default(),
                google: Default::default(),
            },
        ));
        (rt, dir)
    }

    /// Waits until credential `id` holds the refreshed access token.
    async fn refreshed(rt: &Runtime, id: &str) -> Arc<Credential> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let c = rt.store.get(id).unwrap();
                if c.str("access_token") == Some("sk-ant-oat-new-fake") {
                    break c;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{id} was not refreshed"))
    }

    /// Go `markRefreshPending`: a job past [`REFRESH_JOB_BOUND`] (2 s under test, counted
    /// from when it got a worker) keeps its credential reserved, so no second job starts
    /// while it runs, even after the backoff ends, but gives up its worker: with one
    /// worker, another credential that comes due meanwhile refreshes while the first's
    /// token call is still held. The held job's own result, a failure, is recorded when
    /// it ends, and that wakes the loop: with no event after it, the rejected credential
    /// (whose scan sets no other timer) is retried once its backoff ends, and commits.
    #[tokio::test]
    async fn shutdown_waits_for_owned_background_refresh_before_cleanup() {
        let id = "claude-stop-owned.json";
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let answered = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let answer = {
            let (entered, release, answered) = (entered.clone(), release.clone(), answered.clone());
            move |_: String| {
                let (entered, release, answered) = (entered.clone(), release.clone(), answered.clone());
                async move {
                    entered.notify_one();
                    release.notified().await;
                    answered.store(true, Ordering::SeqCst);
                    (
                        axum::http::StatusCode::OK,
                        r#"{"access_token":"sk-ant-oat-stop-fake","refresh_token":"stop-fake-r","expires_in":28800}"#,
                    )
                }
            }
        };
        let (rt, dir) = claude_refresh_fixture(&[id], "", answer).await;
        rt.start_auto_refresh();
        entered.notified().await;
        rt.stop_auto_refresh();
        use std::future::Future as _;
        use std::task::Poll;
        let mut shutdown = Box::pin(rt.shutdown_codex_refreshes());
        let early = std::future::poll_fn(|cx| {
            Poll::Ready(match shutdown.as_mut().poll(cx) {
                Poll::Ready(()) => true,
                Poll::Pending => false,
            })
        })
        .await;
        release.notify_one();
        if !early {
            shutdown.as_mut().await;
        }
        drop(shutdown);
        assert!(
            !early,
            "shutdown returned while an owned refresh still awaited its upstream result"
        );
        assert!(answered.load(Ordering::SeqCst));
        assert_eq!(
            rt.store.get(id).unwrap().metadata["access_token"],
            "sk-ant-oat-stop-fake",
            "shutdown joins refresh persistence, not only the upstream response"
        );
        drop(rt);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_job_past_its_bound_stays_the_only_one_and_wakes_the_loop_when_it_ends() {
        let (slow, quick) = ("claude-slow.json", "claude-quick.json");
        let release = Arc::new(tokio::sync::Notify::new());
        let slow_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let answer = {
            let (release, slow_calls) = (release.clone(), slow_calls.clone());
            move |body: String| {
                let (release, slow_calls) = (release.clone(), slow_calls.clone());
                async move {
                    if body.contains(&format!("fake-rt-{slow}")) && slow_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        release.notified().await;
                        return (axum::http::StatusCode::BAD_REQUEST, r#"{"error":"invalid_request"}"#);
                    }
                    let ok = r#"{"access_token":"sk-ant-oat-new-fake","refresh_token":"fake-rt-2","expires_in":28800}"#;
                    (axum::http::StatusCode::OK, ok)
                }
            }
        };
        let workers = "oauth:\n  auth-auto-refresh-workers: 1\n";
        let (rt, dir) = claude_refresh_fixture(&[slow, quick], workers, answer).await;
        // Neither is due on its own: an upstream 401 is what schedules the slow one.
        let set_expiry = |id: &str, ahead: chrono::Duration| {
            let mut patch = MetadataPatch::default();
            patch
                .set
                .insert("expired".into(), (chrono::Utc::now() + ahead).to_rfc3339().into());
            let c = rt.store.get(id).unwrap();
            rt.store.apply_patch(id, c.revision, &patch).unwrap()
        };
        set_expiry(quick, chrono::Duration::days(9));
        let c = set_expiry(slow, chrono::Duration::days(9));
        rt.store
            .unauthorized
            .lock()
            .unwrap()
            .pending
            .insert(slow.into(), (c.revision, Instant::now()));
        crate::scheduling::refresh::override_backoff(slow, Duration::from_secs(1));
        let logs = Logs::default();
        let _guard = logs.install();
        rt.start_auto_refresh();
        logs.wait_for(
            &format!("credential refresh for claude ({slow}) still running after 2s"),
            1,
        )
        .await;
        // The only worker is free again: a credential that comes due now refreshes.
        set_expiry(quick, chrono::Duration::hours(1));
        refreshed(&rt, quick).await;
        // The backoff ends twice while the job runs; neither forced scan starts another.
        for _ in 0..2 {
            let scans = tests::reached("scan", slow);
            rt.refresh_state.lock().unwrap().expire(&c);
            rt.notify_config(rt.config());
            tokio::time::timeout(Duration::from_secs(10), async {
                while tests::reached("scan", slow) == scans {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("a forced scan");
        }
        assert_eq!(slow_calls.load(Ordering::SeqCst), 1);
        assert_eq!(tests::reached("prepare", slow), 1, "one job while it runs");
        release.notify_one();
        logs.wait_for(
            &format!("credential refresh failed for claude ({slow}) after an upstream 401: status 400"),
            1,
        )
        .await;
        // No event from here on: only the ended job can wake the loop.
        let committed = refreshed(&rt, slow).await;
        assert_eq!(slow_calls.load(Ordering::SeqCst), 2);
        // The job commits before it ends its reservation.
        tokio::time::timeout(Duration::from_secs(5), async {
            while rt.refresh_state.lock().unwrap().running(&committed) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the job ends its reservation");
        let text = logs.text();
        assert_eq!(text.matches("still running after").count(), 1, "{text}");
        assert_eq!(
            text.matches("its previous refresh is still running").count(),
            1,
            "{text}"
        );
        assert_eq!(text.matches("credential refresh failed").count(), 1, "{text}");
        rt.stop_auto_refresh();
        rt.shutdown_codex_refreshes().await;
        drop(rt);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Every reservation belongs to a spawned job from the moment it is made, and the job
    /// ends it on every path: a scan that panics after reserving a credential restarts
    /// the loop without stranding that credential, whose job then panics in preparation
    /// (after the loop that started it is gone), backs off and is retried.
    #[tokio::test]
    async fn no_reservation_is_stranded_by_a_panicking_scan_or_job() {
        let ids = ["claude-strand-a.json", "claude-strand-b.json"];
        let answer = |_| async {
            let ok = r#"{"access_token":"sk-ant-oat-new-fake","refresh_token":"fake-rt-2","expires_in":28800}"#;
            (axum::http::StatusCode::OK, ok)
        };
        let (rt, dir) = claude_refresh_fixture(&ids, "", answer).await;
        let order: Vec<String> = rt.store.snapshot().iter().map(|c| c.id.clone()).collect();
        let (first, second) = (order[0].as_str(), order[1].as_str());
        crate::scheduling::refresh::override_backoff(first, Duration::from_secs(1));
        tests::arm_panic("scan", second);
        tests::arm_panic("prepare", first);
        let logs = Logs::default();
        let _guard = logs.install();
        rt.start_auto_refresh();
        refreshed(&rt, first).await;
        refreshed(&rt, second).await;
        assert_eq!(tests::reached("prepare", first), 2, "the panicked job, then its retry");
        assert!(
            logs.text()
                .contains("credential auto-refresh loop panicked (injected scan panic)")
        );
        assert!(logs.text().contains(&format!(
            "credential refresh failed for claude ({first}): status 500: refresh panicked"
        )));
        rt.stop_auto_refresh();
        rt.shutdown_codex_refreshes().await;
        drop(rt);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// The loop sets no timer while nothing can come due, and a credential added later
    /// is tried after the store change, not on a schedule.
    #[tokio::test]
    async fn refresh_loop_sleeps_until_the_store_changes() {
        let unreachable = "http://127.0.0.1:1/token";
        let oauth = ck_exec::oauth::OAuth::with_endpoints(wreq::Client::new(), unreachable, unreachable, unreachable);
        let rt = Arc::new(test_runtime(
            Config::parse("").unwrap(),
            Vec::new(),
            Executors {
                claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1")
                    .unwrap()
                    .with_oauth(oauth),
                codex: Default::default(),
                devices: Default::default(),
                openai: Default::default(),
                google: Default::default(),
            },
        ));
        rt.start_auto_refresh();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut due = cred("a.json", "claude", false);
        due.metadata.insert("refresh_token".into(), "fake-refresh".into());
        due.metadata.insert(
            "expired".into(),
            (chrono::Utc::now() - chrono::Duration::hours(1)).to_rfc3339().into(),
        );
        rt.store.reconcile(vec![due]);
        let added = rt.store.get("a.json").unwrap();
        // The refresh fails (no token endpoint), which backs off for five minutes.
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let retry = rt.refresh_state.lock().unwrap().retry_at(&added);
                if retry.is_some_and(|at| at > Instant::now() + Duration::from_secs(200)) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the added credential was tried and backed off");
        rt.stop_auto_refresh();
    }

    #[tokio::test]
    async fn every_lease_reports_exactly_once() {
        let store = test_store(vec![cred("a.json", "claude", false)]);
        let stats = |s, f, c| AttemptStats {
            success: s,
            failure: f,
            cancelled: c,
        };

        // Dropped without completing (client gone mid-execute): cancelled.
        drop(store.select(sel("claude")).unwrap());
        assert_eq!(store.stats(), stats(0, 0, 1));
        // Completed explicitly: the drop that follows does not report again.
        store.select(sel("claude")).unwrap().complete(Outcome::Success);
        assert_eq!(store.stats(), stats(1, 0, 1));

        // A stream that errors and would then hang forever: the wrapper stops after Err.
        let err = ExecError::local(502, FailureScope::Transport, "boom");
        let inner = futures_util::stream::iter(vec![Ok(Bytes::from_static(b"a")), Err(err)])
            .chain(futures_util::stream::pending())
            .boxed();
        let mut s = Completing::new(inner, store.select(sel("claude")).unwrap());
        assert!(s.next().await.unwrap().is_ok());
        assert!(s.next().await.unwrap().is_err());
        assert!(s.next().await.is_none(), "fused after the first error");
        drop(s);
        assert_eq!(store.stats(), stats(1, 1, 1));

        let mut done = Completing::new(
            futures_util::stream::empty().boxed(),
            store.select(sel("claude")).unwrap(),
        );
        assert!(done.next().await.is_none());
        drop(done);
        assert_eq!(store.stats(), stats(2, 1, 1));

        // Dropped mid-stream: cancelled.
        let mut mid = Completing::new(
            futures_util::stream::iter(vec![Ok(Bytes::new())])
                .chain(futures_util::stream::pending())
                .boxed(),
            store.select(sel("claude")).unwrap(),
        );
        assert!(mid.next().await.is_some());
        drop(mid);
        assert_eq!(store.stats(), stats(2, 1, 2));
    }

    /// A lease ends once, and only after the upstream body it was streaming has been
    /// dropped, both on an upstream error and when the client stops reading.
    #[tokio::test]
    async fn leases_end_after_the_upstream_body_closes() {
        struct Probe(Arc<CredentialStore>, Arc<Mutex<Vec<AttemptStats>>>);
        impl Drop for Probe {
            fn drop(&mut self) {
                self.1.lock().unwrap().push(self.0.stats());
            }
        }
        let stats = |s, f, c| AttemptStats {
            success: s,
            failure: f,
            cancelled: c,
        };
        let store = test_store(vec![cred("a.json", "claude", false)]);
        let observed = Arc::new(Mutex::new(Vec::new()));
        let body = |items: Vec<Result<Bytes, ExecError>>| {
            let guard = Probe(store.clone(), observed.clone());
            futures_util::stream::iter(items)
                .chain(futures_util::stream::pending())
                .map(move |item| {
                    let _ = &guard;
                    item
                })
                .boxed()
        };

        let err = ExecError::local(502, FailureScope::Transport, "boom");
        let mut stream = Completing::new(
            body(vec![Ok(Bytes::from_static(b"a")), Err(err)]),
            store.select(sel("claude")).unwrap(),
        );
        assert!(stream.next().await.unwrap().is_ok());
        assert!(observed.lock().unwrap().is_empty(), "still streaming");
        assert!(stream.next().await.unwrap().is_err());
        assert_eq!(
            *observed.lock().unwrap(),
            vec![stats(0, 0, 0)],
            "body dropped before the lease ended"
        );
        assert_eq!(store.stats(), stats(0, 1, 0));
        drop(stream);
        assert_eq!(store.stats(), stats(0, 1, 0), "one end");

        let mut stream = Completing::new(body(vec![Ok(Bytes::new())]), store.select(sel("claude")).unwrap());
        assert!(stream.next().await.unwrap().is_ok());
        drop(stream);
        assert_eq!(
            observed.lock().unwrap()[1],
            stats(0, 1, 0),
            "body dropped before the cancelled lease ended"
        );
        assert_eq!(store.stats(), stats(0, 1, 1));
    }
}
