//! Test support: keeps a test runtime off the network beyond loopback.
//!
//! Tests must never reach a provider, even when a test credential has no local base URL
//! and its provider gains an executor later. A runtime built by [`runtime`] hands each
//! executor call (execute, session turns, prepare and refresh) a [`guarded`] copy of the
//! credential: one that is not local is routed to a dead loopback proxy through the
//! production rule every executor follows (`Proxy::effective`, where the credential's
//! own `proxy_url` wins), so it fails with a connection error instead of leaving the
//! machine. A credential is local when its base URL or its own proxy is loopback, or
//! the test marked it [`local`] because the test built its executor against a mock.
//! A non-loopback proxy (including `direct`) does not count: it could reach a
//! provider. Stored credentials never change, so management listings, reconciliation and
//! features that pick their own proxy (management `api-call`, `latest-version`) behave
//! as configured. Such a runtime also starts no remote model catalog refresh
//! (models.json, the Codex client list, Devin, the Antigravity version); the embedded
//! catalogs stay in use.
// ponytail: the realtime routes dial through the Codex executor's live endpoints
// directly, outside these executor calls; their tests build those endpoints on mocks.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use ck_core::config::Config;
use ck_core::credential::Credential;
use ck_exec::Executors;

use crate::Runtime;

/// Per-runtime scheduler time, including the wall-clock conversion used by SQLite.
/// HTTP and storage tasks keep running while cooldown time changes only explicitly.
pub struct SchedulerClock {
    elapsed: ck_stream::ManualClock,
    monotonic: std::time::Instant,
    wall: std::time::SystemTime,
}

impl Default for SchedulerClock {
    fn default() -> Self {
        Self {
            elapsed: Default::default(),
            monotonic: std::time::Instant::now(),
            wall: std::time::SystemTime::now(),
        }
    }
}

impl SchedulerClock {
    pub fn advance(&self, duration: std::time::Duration) {
        self.elapsed.advance(duration);
    }

    pub(crate) fn now(&self) -> std::time::Instant {
        self.monotonic + ck_stream::Clock::now(&self.elapsed)
    }

    pub fn wall(&self) -> std::time::SystemTime {
        self.wall + ck_stream::Clock::now(&self.elapsed)
    }
}

#[cfg(test)]
pub(crate) mod installation_effects;

/// The deny-all proxy: the discard port on loopback, where nothing listens.
pub const DENY_PROXY: &str = "http://127.0.0.1:9";

/// Attribute set by [`local`].
const LOCAL: &str = "cpa_test_local";

/// Opens a fixture database with a private working/coordinator namespace beside
/// its snapshot. This must be used even by integration tests, where cfg(test)
/// does not apply to this library. Never changes process-global TMPDIR.
pub fn config_store(path: &std::path::Path) -> anyhow::Result<crate::store::config_store::ConfigStore> {
    config_store_mode(path, true)
}

/// Observes the owner workspace without depending on its directory layout.
pub fn working_directory(store: &crate::store::config_store::ConfigStore) -> Option<std::path::PathBuf> {
    store.working_directory().map(std::path::Path::to_owned)
}

/// The same fixture namespace, opening only an existing database (offline reads).
pub fn existing_config_store(path: &std::path::Path) -> anyhow::Result<crate::store::config_store::ConfigStore> {
    config_store_mode(path, false)
}

fn config_store_mode(path: &std::path::Path, create: bool) -> anyhow::Result<crate::store::config_store::ConfigStore> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(std::path::Path::new("."));
    if create {
        std::fs::create_dir_all(parent)?;
    }
    let root = std::fs::canonicalize(parent)?.join(".cooker-test-storage");
    crate::store::config_store::ConfigStore::open_in(path, create, root)
}

/// Executes fixture SQL on a fresh copy of canonical state, then installs the
/// resulting fixture state. Do not retain raw SQLite handles across runtime calls.
pub fn database_execute(store: &crate::store::config_store::ConfigStore, sql: &str) -> anyhow::Result<()> {
    store.fixture_execute(sql)
}

/// Opens a test-only SQL connection to the mutable database for failure injection.
/// Keep `store` alive until this connection is closed; `store.path()` names the
/// immutable published backup, not the database the runtime writes.
pub fn database_connection(store: &crate::store::config_store::ConfigStore) -> rusqlite::Connection {
    let path = store.conn().unwrap().path().expect("on-disk test database").to_owned();
    rusqlite::Connection::open(path).expect("test database connection")
}

#[cfg(test)]
pub(crate) struct BlockedPublication {
    path: std::path::PathBuf,
    permissions: std::fs::Permissions,
}

#[cfg(test)]
impl Drop for BlockedPublication {
    fn drop(&mut self) {
        std::fs::set_permissions(&self.path, self.permissions.clone()).expect("restore publication permissions");
    }
}

/// Keep the snapshot readable but make its atomic replacement fail, after a writer
/// has installed its recovery checkpoint.
#[cfg(test)]
pub(crate) fn block_publication(path: &std::path::Path) -> BlockedPublication {
    #[cfg(unix)]
    let path = path.parent().expect("snapshot parent");
    let permissions = std::fs::metadata(path).unwrap().permissions();
    let mut blocked = permissions.clone();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        blocked.set_mode(0o500);
    }
    #[cfg(not(unix))]
    blocked.set_readonly(true);
    std::fs::set_permissions(path, blocked).unwrap();
    BlockedPublication {
        path: path.to_owned(),
        permissions,
    }
}

#[cfg(test)]
pub(crate) mod publication_effects {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, OnceLock};

    fn counters() -> &'static Mutex<HashMap<PathBuf, Arc<AtomicUsize>>> {
        static COUNTERS: OnceLock<Mutex<HashMap<PathBuf, Arc<AtomicUsize>>>> = OnceLock::new();
        COUNTERS.get_or_init(Mutex::default)
    }

    /// Counts completed published-file installs for one private fixture. No
    /// callbacks run at the storage edge, and setup effects are excluded by scope.
    pub(crate) struct Counter {
        path: PathBuf,
        count: Arc<AtomicUsize>,
    }

    impl Counter {
        pub(crate) fn new(store: &crate::store::config_store::ConfigStore) -> Self {
            let path = store.path().expect("published fixture").to_owned();
            let count = Arc::new(AtomicUsize::new(0));
            assert!(counters().lock().unwrap().insert(path.clone(), count.clone()).is_none());
            Self { path, count }
        }

        pub(crate) fn get(&self) -> usize {
            self.count.load(Ordering::SeqCst)
        }
    }

    impl Drop for Counter {
        fn drop(&mut self) {
            counters().lock().unwrap().remove(&self.path);
        }
    }

    pub(crate) fn installed(path: &Path) {
        if let Some(count) = counters().lock().unwrap().get(path) {
            count.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn counter_is_target_scoped_and_counts_equal_byte_publications() {
        let root = std::env::temp_dir().join(format!("cooker-publication-counter-{}", uuid::Uuid::new_v4()));
        let stores = ["one.db", "two.db"].map(|name| Arc::new(super::config_store(&root.join(name)).unwrap()));
        let backends = stores
            .each_ref()
            .map(|store| crate::store::credential_store::CredentialDb::initialize(store.clone()).unwrap());
        let path = backends[0].auth_dir().join("a.json");
        let bytes = br#"{"type":"claude","access_token":"private-test"}"#;
        backends[0].save(&path, bytes).unwrap();
        let counter = Counter::new(&stores[0]);
        backends[1].save(&backends[1].auth_dir().join("b.json"), bytes).unwrap();
        assert_eq!(counter.get(), 0, "other private targets cannot affect this fixture");
        for _ in 0..2 {
            backends[0].save(&path, bytes).unwrap();
        }
        assert_eq!(counter.get(), 2, "identical bytes still incur two durable installs");
        drop(counter);
        let counter = Counter::new(&stores[0]);
        assert_eq!(counter.get(), 0, "new scope must not inherit an old count");
        backends[0].save(&path, bytes).unwrap();
        assert_eq!(counter.get(), 1);
        drop(counter);
        drop(backends);
        drop(stores);
        std::fs::remove_dir_all(root).unwrap();
    }
}

pub struct BlockedCheckpoint {
    path: std::path::PathBuf,
    parked: std::path::PathBuf,
}

impl Drop for BlockedCheckpoint {
    fn drop(&mut self) {
        std::fs::remove_dir(&self.path).expect("remove checkpoint obstruction");
        std::fs::rename(&self.parked, &self.path).expect("restore old checkpoint");
    }
}

/// An explicit storage-effect fault: construction can complete, but installing
/// the checkpoint fails. The previous recovery artifact is retained unchanged.
pub fn block_checkpoint_install(store: &crate::store::config_store::ConfigStore) -> BlockedCheckpoint {
    let path = std::path::PathBuf::from(database_connection(store).path().unwrap());
    let parked = path.with_extension("blocked-checkpoint");
    std::fs::rename(&path, &parked).unwrap();
    std::fs::create_dir(&path).unwrap();
    BlockedCheckpoint { path, parked }
}

/// Saves every `.json` file under `dir` to `db` at the same relative name: the
/// credentials a fixture staged on disk. The database is the only credential source, so
/// a fixture saves its credentials after `initialize`, never before it.
pub fn seed_credentials(db: &crate::store::credential_store::CredentialDb, dir: &std::path::Path) {
    // The staged files are listed before the first save, and the database's own working
    // directory is skipped: a fixture may stage them beside `config.db`, and a save
    // materializes its working copy there, which a live walk would seed a second time.
    fn walk(skip: &std::path::Path, dir: &std::path::Path, found: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries {
            let path = entry.expect("test auth-dir entry").path();
            if std::fs::canonicalize(&path).is_ok_and(|real| real == skip) {
                continue;
            }
            if path.is_dir() {
                walk(skip, &path, found);
            } else if path.extension().is_some_and(|ext| ext.eq_ignore_ascii_case("json")) {
                found.push(path);
            }
        }
    }
    if dir.as_os_str().is_empty() {
        return;
    }
    let skip = std::fs::canonicalize(db.auth_dir()).unwrap_or_else(|_| db.auth_dir());
    let mut found = Vec::new();
    walk(&skip, dir, &mut found);
    for path in found {
        let bytes = std::fs::read(&path).expect("test credential file");
        db.save(&db.auth_dir().join(path.strip_prefix(dir).unwrap()), &bytes)
            .expect("test credential seed");
    }
}

/// Creates the database at `database` and saves every `.json` file under `dir` to it,
/// for a test that starts the binary on that database afterwards.
pub fn seed_database(database: &std::path::Path, dir: &std::path::Path) {
    let store = Arc::new(config_store(database).expect("test database"));
    let db = crate::store::credential_store::CredentialDb::initialize(store).expect("test credentials");
    seed_credentials(&db, dir);
}

/// `yaml` without the `oauth.auth-dir` line of a Go recording: the setting is gone, so a
/// recorded config can no longer be imported as it stands. An `oauth:` block left empty
/// goes with it. The harness hands the directory to the fixture as `Config::auth_dir`
/// and seeds it with [`seed_credentials`].
pub fn without_auth_dir(yaml: &str) -> String {
    let lines: Vec<&str> = yaml.split_inclusive('\n').collect();
    let mut out: Vec<&str> = Vec::with_capacity(lines.len());
    for (i, line) in lines.iter().enumerate() {
        if line.trim_start().starts_with("auth-dir:") {
            continue;
        }
        if line.trim_end() == "oauth:" {
            let next = lines.iter().skip(i + 1).find(|l| !l.trim().is_empty());
            let nested = next.is_some_and(|n| n.starts_with(' ') && !n.trim_start().starts_with("auth-dir:"));
            if !nested {
                continue;
            }
        }
        out.push(line);
    }
    out.concat()
}

/// A local runtime with a private SQLite database whose executors cannot leave the
/// machine. The `.json` files a test staged in `config.auth_dir` and the files of
/// `credentials` are saved to the database first.
pub fn runtime(mut config: Config, mut credentials: Vec<Credential>, executors: Executors) -> Runtime {
    let dir = std::env::temp_dir().join(format!("cooker-runtime-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&dir).expect("private runtime directory");
    let store = Arc::new(config_store(&dir.join("test.db")).expect("test database"));
    let original = config.auth_dir.clone();
    let db = crate::store::credential_store::CredentialDb::initialize(store).expect("test credentials");
    seed_credentials(&db, &original);
    for credential in &mut credentials {
        if let ck_core::credential::Source::File(path) = &mut credential.source {
            let source = path.clone();
            if let Ok(relative) = source.strip_prefix(&original) {
                *path = db.auth_dir().join(relative);
                for key in ["path", "source"] {
                    if credential
                        .attributes
                        .get(key)
                        .is_some_and(|value| value == source.to_string_lossy().as_ref())
                    {
                        credential.attributes.insert(key.into(), path.display().to_string());
                    }
                }
            }
            if db.read(path).expect("test credential read").is_none()
                && let Ok(bytes) = std::fs::read(&source)
            {
                db.save(path, &bytes).expect("test credential seed");
            }
        }
    }
    config.auth_dir = db.auth_dir();
    runtime_with_db(config, credentials, executors, db)
}

/// A guarded local runtime using a test's existing private credential database.
pub fn runtime_with_db(
    config: Config,
    credentials: Vec<Credential>,
    executors: Executors,
    db: Arc<crate::store::credential_store::CredentialDb>,
) -> Runtime {
    runtime_with_db_clock(config, credentials, executors, db, None)
}

/// Installs a manual scheduler clock before startup restores persisted cooldowns.
pub fn runtime_with_db_clock(
    config: Config,
    credentials: Vec<Credential>,
    executors: Executors,
    db: Arc<crate::store::credential_store::CredentialDb>,
    clock: Option<Arc<SchedulerClock>>,
) -> Runtime {
    use sha2::Digest;
    let read = db
        .documents()
        .expect("test source documents")
        .into_iter()
        .map(|(path, bytes)| (path, sha2::Sha256::digest(bytes).into()))
        .collect();
    let rt = Runtime::build(config, credentials, executors, db.clone(), db, clock);
    // Like the database, the stream pool is private to the runtime: it never depends on, or
    // shares, the host's `$XDG_RUNTIME_DIR`. The store opens there at first use.
    let pool = std::env::temp_dir().join(format!("cooker-pool-{}", uuid::Uuid::new_v4()));
    rt.streams()
        .directory
        .set(pool)
        .expect("a new runtime has no pool directory");
    rt.store().note_loaded(&read);
    rt.deny_external.store(true, Ordering::Relaxed);
    rt
}

/// Marks a credential whose executor the test pointed at a local mock (for example
/// `ClaudeExecutor::new(mock_url)`), so executor calls use it as stored.
pub fn local(mut credential: Credential) -> Credential {
    credential.attributes.insert(LOCAL.into(), "true".into());
    credential
}

/// `credential` itself when it is local, otherwise a copy whose `proxy_url` is
/// [`DENY_PROXY`]. A loopback base URL behind a proxy that is not loopback (its own or
/// `requests.proxy-url`) gets a `direct` copy, so the mock is reached without the
/// request ever going to that proxy. Only the credential's own loopback proxy exempts
/// it: a loopback `requests.proxy-url` may be a real local proxy that forwards out.
pub fn guarded(credential: &Arc<Credential>, cfg: &Config) -> Arc<Credential> {
    let text = |key: &str| {
        credential
            .attributes
            .get(key)
            .map(String::as_str)
            .or_else(|| credential.str(key))
            .unwrap_or_default()
            .trim()
    };
    if credential.attributes.contains_key(LOCAL) || is_loopback(text("proxy_url")) {
        return credential.clone();
    }
    let replacement = if is_loopback(text("base_url")) {
        let proxy = ck_exec::proxy::Proxy::effective_url(credential, cfg);
        if is_loopback(&proxy) || ck_exec::proxy::Proxy::parse(&proxy) == ck_exec::proxy::Proxy::Direct {
            return credential.clone();
        }
        "direct"
    } else {
        DENY_PROXY
    };
    let mut copy = Credential::clone(credential);
    copy.attributes.insert("proxy_url".into(), replacement.into());
    Arc::new(copy)
}

fn is_loopback(raw: &str) -> bool {
    let Ok(uri) = raw.parse::<axum::http::Uri>() else {
        return false;
    };
    let Some(host) = uri.host() else {
        return false;
    };
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host.eq_ignore_ascii_case("localhost") || host.parse::<std::net::IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ck_exec::proxy::Proxy;

    fn credential(attrs: &[(&str, &str)], meta: serde_json::Value) -> Arc<Credential> {
        let mut metadata = meta.as_object().unwrap().clone();
        metadata.insert("type".into(), "claude".into());
        let mut c = Credential::from_file(
            std::path::Path::new("/fake"),
            std::path::Path::new("/fake/a.json"),
            metadata,
        )
        .unwrap();
        for (k, v) in attrs {
            c.attributes.insert((*k).into(), (*v).into());
        }
        Arc::new(c)
    }

    #[test]
    fn only_local_credentials_dial_out() {
        let cfg = Config::parse("requests:\n  proxy-url: http://127.0.0.1:7\n").unwrap();
        let creds = [
            credential(&[], serde_json::json!({})),
            credential(&[("base_url", "http://127.0.0.1:4100")], serde_json::json!({})),
            credential(&[], serde_json::json!({"base_url": "http://localhost:1/v1"})),
            credential(&[("base_url", "http://[::1]:2")], serde_json::json!({})),
            credential(&[("base_url", "https://api.x.ai")], serde_json::json!({})),
            credential(&[("base_url", "http://127.0.0.1.example.com")], serde_json::json!({})),
            credential(&[("proxy_url", "direct")], serde_json::json!({})),
            credential(&[("proxy_url", "http://127.0.0.1:3128")], serde_json::json!({})),
            Arc::new(local(Credential::clone(&credential(&[], serde_json::json!({}))))),
        ];
        let deny = Proxy::Url(DENY_PROXY.into());
        let global = Proxy::Url("http://127.0.0.1:7".into());
        let got: Vec<Proxy> = creds
            .iter()
            .map(|c| Proxy::effective(&guarded(c, &cfg), &cfg))
            .collect();
        assert_eq!(
            got,
            [
                deny.clone(),
                global.clone(),
                global.clone(),
                global.clone(),
                deny.clone(),
                deny.clone(),
                deny,
                Proxy::Url("http://127.0.0.1:3128".into()),
                global,
            ]
        );
        // The stored credential is untouched.
        assert!(!creds[0].attributes.contains_key("proxy_url"));

        // A loopback mock behind a proxy that is not loopback is reached directly.
        let outside = Config::parse("requests:\n  proxy-url: http://proxy.example:3128\n").unwrap();
        let mock = credential(&[("base_url", "http://127.0.0.1:4100")], serde_json::json!({}));
        assert_eq!(Proxy::effective(&guarded(&mock, &outside), &outside), Proxy::Direct);
        let own = credential(
            &[
                ("base_url", "http://127.0.0.1:4100"),
                ("proxy_url", "http://proxy.example:1"),
            ],
            serde_json::json!({}),
        );
        assert_eq!(Proxy::effective(&guarded(&own, &cfg), &cfg), Proxy::Direct);
        let elsewhere = credential(&[("base_url", "https://api.x.ai")], serde_json::json!({}));
        assert_eq!(Proxy::effective(&guarded(&elsewhere, &outside), &outside), deny_proxy());
    }

    fn deny_proxy() -> Proxy {
        Proxy::Url(DENY_PROXY.into())
    }

    /// A guarded credential fails with a connection error at the dead proxy; it never
    /// reaches its provider.
    #[tokio::test]
    async fn guarded_credential_never_leaves_the_machine() {
        let cfg = Config::parse("{}\n").unwrap();
        let guarded = guarded(&credential(&[], serde_json::json!({})), &cfg);
        let client = ck_exec::proxy::GoClients::new(Default::default()).get(&Proxy::effective(&guarded, &cfg));
        let err = client
            .get("https://provider.invalid/v1/models")
            .send()
            .await
            .unwrap_err();
        // Refused at the proxy, not a name lookup of the target.
        assert!(format!("{err:?}").contains("Connection refused"), "{err:?}");
    }

    /// Only a runtime built by [`runtime`] guards executor calls.
    #[test]
    fn only_test_runtimes_guard() {
        let executors = || Executors {
            claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
            codex: Default::default(),
            devices: Default::default(),
            openai: Default::default(),
            google: Default::default(),
        };
        let c = credential(&[], serde_json::json!({}));
        let dir = std::env::temp_dir().join(format!("cooker-plain-runtime-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let store = Arc::new(config_store(&dir.join("test.db")).unwrap());
        let db = crate::store::credential_store::CredentialDb::initialize(store).unwrap();
        let plain = Runtime::new(Config::default(), Vec::new(), executors(), db);
        assert!(Arc::ptr_eq(&plain.for_executor(&c), &c));
        let test = runtime(Config::default(), Vec::new(), executors());
        assert_eq!(test.for_executor(&c).attributes["proxy_url"], DENY_PROXY);
        drop(plain);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

/// A point an attempt's finalizer passes only while it is open, so a test can hold one step of
/// it: the index publication or the usage reader ([`crate::inference::streams::Registry`]).
pub struct Hold(tokio::sync::watch::Sender<bool>);

impl Default for Hold {
    fn default() -> Self {
        Self(tokio::sync::watch::Sender::new(true))
    }
}

impl Hold {
    /// Finalizers stop here until [`Hold::open`].
    pub fn close(&self) {
        self.0.send_replace(false);
    }

    pub fn open(&self) {
        self.0.send_replace(true);
    }

    pub(crate) async fn pass(&self) {
        let _ = self.0.subscribe().wait_for(|open| *open).await;
    }
}

/// The finalizer steps a test can hold.
#[derive(Default)]
pub struct Holds {
    /// Before the finalizer reports the response stream's end to the index publication.
    pub publication: Hold,
    /// Before the usage reader drains the response stream.
    pub usage: Hold,
}
