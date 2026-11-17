//! Admin contract tests use a disposable SQLite database and real loopback HTTP.
//! No provider requests, real credentials, or default auth directory are involved.
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::extract::Request;
use axum::middleware::Next;
use ck_server::management::api::{self as management, Management, Options};
use ck_server::store::config_store::Kind;
use ck_server::{Runtime, observability::request_logging};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

const KEY: &str = "admin-http-test-key";
const BODY_LIMIT: usize = 16 * 1024 * 1024;
static NEXT: AtomicUsize = AtomicUsize::new(0);

struct Fixture {
    dir: PathBuf,
    state: Arc<Management>,
    rt: Arc<Runtime>,
    client: wreq::Client,
    base: String,
    server: tokio::task::JoinHandle<()>,
}

impl Fixture {
    async fn new(name: &str, trusted_proxy: bool) -> Self {
        Self::with_management_password(name, trusted_proxy, String::new()).await
    }

    async fn with_management_password(name: &str, trusted_proxy: bool, management_password: String) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "admin-contract-{name}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(dir.join("auth")).unwrap();
        let hash = bcrypt::hash(KEY, 4).unwrap();
        let trusted = if trusted_proxy {
            "trusted-proxies: ['127.0.0.1']"
        } else {
            "trusted-proxies: []"
        };
        let store = Arc::new(ck_server::testing::config_store(&dir.join("config.db")).unwrap());
        store.import_yaml(&format!(
            "config-version: 8\nserver: {{host: '127.0.0.1', port: 0, {trusted}}}\nmanagement: {{secret-key: '{hash}', allow-remote: false}}\nrouting: {{retry: {{request-retry: 3}}}}\naccess: {{api-keys: [client-http-test-secret]}}\nobservability: {{logs: {{request-log: true}}}}\n"), "test", "fixture").unwrap();
        let db = ck_server::store::credential_store::CredentialDb::initialize(store.clone()).unwrap();
        let mut cfg = store.load().unwrap().config;
        cfg.auth_dir = db.auth_dir();
        let credentials = ck_core::config::credentials::from_config(&cfg);
        let rt = Arc::new(ck_server::testing::runtime_with_db(
            cfg,
            credentials,
            ck_exec::Executors {
                claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:9").unwrap(),
                codex: Default::default(),
                devices: Default::default(),
                openai: Default::default(),
                google: Default::default(),
            },
            db,
        ));
        let state = Management::with_options(
            rt.clone(),
            store,
            Options {
                management_password: Some(management_password),
                log_dir: Some(dir.join("logs")),
                ..Default::default()
            },
        );
        let app = management::router(state.clone())
            .route(
                "/capture-probe",
                axum::routing::post(
                    |axum::Extension(_): axum::Extension<request_logging::RequestLog>| async { "capture-active" },
                ),
            )
            .layer(axum::middleware::from_fn(|request: Request, next: Next| async move {
                if request.uri().path() != "/capture-probe" {
                    assert!(
                        request.extensions().get::<request_logging::RequestLog>().is_none(),
                        "admin capture must be bypassed before reading secrets"
                    );
                }
                next.run(request).await
            }));
        let app = request_logging::router(&state, ck_server::app(rt.clone(), app));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .unwrap();
        });
        let client = wreq::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap();
        Self {
            dir,
            state,
            rt,
            client,
            base,
            server,
        }
    }

    fn request(&self, method: wreq::Method, path: &str) -> wreq::RequestBuilder {
        self.client
            .request(method, format!("{}{path}", self.base))
            .bearer_auth(KEY)
    }

    async fn export(&self) -> (String, Value) {
        let response = self
            .request(wreq::Method::GET, "/admin/v1/export")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let tag = response.headers()["etag"].to_str().unwrap().to_owned();
        let revision = response.headers()["x-ck-revision"]
            .to_str()
            .unwrap()
            .parse::<u64>()
            .unwrap();
        let body = response.json::<Value>().await.unwrap();
        assert_eq!(body["revision"], revision);
        assert_eq!(tag, format!("\"{revision}\""));
        (tag, body)
    }

    fn head(&self) -> u64 {
        self.state.config_store().snapshot().unwrap().0
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[tokio::test]
async fn legacy_multipart_credential_upload_keeps_partial_success() {
    let f = Fixture::new("legacy-partial-upload", false).await;
    let data = r#"{"type":"claude","disabled":false,"access_token":"legacy-fixture"}"#;
    let body = format!(
        "--batch\r\nContent-Disposition: form-data; name=\"files\"; filename=\"legacy-good.json\"\r\nContent-Type: application/json\r\n\r\n{data}\r\n--batch\r\nContent-Disposition: form-data; name=\"files\"; filename=\"legacy-bad.json\"\r\nContent-Type: application/json\r\n\r\n{{\r\n--batch--\r\n"
    );
    let response = f
        .request(wreq::Method::POST, "/v8/management/credentials")
        .header("content-type", "multipart/form-data; boundary=batch")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 207);
    let reply: Value = response.json().await.unwrap();
    assert_eq!(reply["status"], "partial");
    assert_eq!(reply["uploaded"], 1);
    assert_eq!(reply["failed"][0]["name"], "legacy-bad.json");
    assert_eq!(download_credential(&f, "legacy-good.json").await, data.as_bytes());
}

fn batch_document(name: &str, data: &[u8]) -> Value {
    use base64::Engine as _;
    json!({"name": name, "data": base64::engine::general_purpose::STANDARD.encode(data)})
}

async fn download_credential(f: &Fixture, name: &str) -> Vec<u8> {
    let response = f
        .request(
            wreq::Method::GET,
            &format!("/v8/management/credentials/download?name={name}"),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    response.bytes().await.unwrap().to_vec()
}

#[tokio::test]
async fn credential_batch_preserves_bytes_and_is_in_the_portable_snapshot() {
    let f = Fixture::new("credential-batch", false).await;
    let first = b" \n{ \"type\":\"claude\", \"access_token\":\"fixture-only\", \"opaque\": [1,true,null] }\n";
    let second = b"{\"type\":\"custom-extension\",\"private_extension\":{\"z\":2,\"a\":1}}\n";
    let head = f.head();
    let response = f
        .request(wreq::Method::POST, "/admin/v1/credentials/batch")
        .json(&json!({"documents": [batch_document("a.json", first), batch_document("b.json", second)]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(download_credential(&f, "a.json").await, first);
    assert_eq!(download_credential(&f, "b.json").await, second);
    assert_eq!(f.head(), head, "credentials do not create configuration revisions");

    let copy = f.dir.join("portable.db");
    std::fs::copy(f.state.config_store().path().unwrap(), &copy).unwrap();
    let store = Arc::new(ck_server::testing::config_store(&copy).unwrap());
    let db = ck_server::store::credential_store::CredentialDb::initialize(store.clone()).unwrap();
    assert_eq!(db.read(&db.auth_dir().join("a.json")).unwrap().unwrap(), first);
    assert_eq!(db.read(&db.auth_dir().join("b.json")).unwrap().unwrap(), second);
    drop((db, store));

    use sha2::Digest as _;
    let mut update = batch_document("a.json", second);
    update["if_match"] = format!("{:x}", sha2::Sha256::digest(first)).into();
    let mut create = batch_document("c.json", first);
    create["if_none_match"] = true.into();
    let response = f
        .request(wreq::Method::POST, "/admin/v1/credentials/batch")
        .json(&json!({"documents": [update, create]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(download_credential(&f, "a.json").await, second);
    assert_eq!(download_credential(&f, "c.json").await, first);
    assert_eq!(f.head(), head);
}

/// Marks the re-executed test process that runs under a lowered descriptor limit.
#[cfg(unix)]
const LOW_NOFILE_CHILD: &str = "CK_ADMIN_CONTRACT_LOW_NOFILE_CHILD";

/// One batch must not hold a descriptor per document: the child lowers only its own soft
/// `RLIMIT_NOFILE` before any runtime exists, so the parent and other tests keep theirs.
#[cfg(unix)]
#[test]
fn credential_batch_of_2000_documents_fits_a_1024_descriptor_limit() {
    const NAME: &str = "credential_batch_of_2000_documents_fits_a_1024_descriptor_limit";
    if std::env::var_os(LOW_NOFILE_CHILD).is_some() {
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
            .block_on(large_credential_batch(2000));
        return;
    }
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
}

#[cfg(unix)]
async fn large_credential_batch(count: usize) {
    use base64::Engine as _;
    let f = Fixture::new("credential-batch-low-nofile", false).await;
    let expected: std::collections::BTreeMap<String, Vec<u8>> = (0..count)
        .map(|i| {
            let name = format!("low-nofile-{i:04}.json");
            let data = format!(r#"{{"type":"claude","access_token":"low-nofile-fixture-{i:04}"}}"#);
            (name, data.into_bytes())
        })
        .collect();
    let documents: Vec<_> = expected.iter().map(|(name, data)| batch_document(name, data)).collect();
    let response = f
        .request(wreq::Method::POST, "/admin/v1/credentials/batch")
        .json(&json!({ "documents": documents }))
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body: Value = response.json().await.unwrap();
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["saved"], count);

    let response = f
        .request(wreq::Method::GET, "/admin/v1/credentials")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.unwrap();
    let canonical: std::collections::BTreeMap<String, Vec<u8>> = body["documents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|document| {
            let data = document["data"].as_str().unwrap();
            (
                document["name"].as_str().unwrap().to_owned(),
                base64::engine::general_purpose::STANDARD.decode(data).unwrap(),
            )
        })
        .collect();
    assert!(canonical == expected, "canonical documents differ from the batch");

    let copy = f.dir.join("portable.db");
    std::fs::copy(f.state.config_store().path().unwrap(), &copy).unwrap();
    let store = Arc::new(ck_server::testing::config_store(&copy).unwrap());
    let db = ck_server::store::credential_store::CredentialDb::initialize(store.clone()).unwrap();
    let portable: std::collections::BTreeMap<String, Vec<u8>> = db
        .documents()
        .unwrap()
        .into_iter()
        .map(|(path, bytes)| (path.file_name().unwrap().to_str().unwrap().to_owned(), bytes))
        .collect();
    assert!(portable == expected, "portable snapshot differs from the batch");
}

/// A batch touching a document whose lock a refresh holds waits for it without holding
/// the store, then checks its source conditions again against what the refresh wrote.
#[tokio::test]
async fn credential_batch_waits_for_a_held_document_lock_and_rechecks_its_source() {
    use sha2::Digest as _;
    let f = Fixture::new("credential-batch-busy", false).await;
    let seed = br#"{"type":"claude","access_token":"busy-seed-fixture"}"#;
    let refreshed = br#"{"type":"claude","access_token":"busy-refreshed-fixture"}"#;
    let replacement = br#"{"type":"claude","access_token":"busy-replacement-fixture"}"#;
    let response = f
        .request(wreq::Method::POST, "/admin/v1/credentials/batch")
        .json(&json!({"documents": [batch_document("busy.json", seed)]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let db = f.rt.store().credential_backend();
    let busy = db.auth_dir().join("busy.json");
    let post = |documents: Value| {
        f.request(wreq::Method::POST, "/admin/v1/credentials/batch")
            .json(&json!({ "documents": documents }))
            .send()
    };
    let mut stale = batch_document("busy.json", replacement);
    stale["if_match"] = format!("{:x}", sha2::Sha256::digest(seed)).into();
    let mut current = batch_document("busy.json", replacement);
    current["if_match"] = format!("{:x}", sha2::Sha256::digest(refreshed)).into();

    for (document, status, kept) in [(stale, 412, &refreshed[..]), (current, 200, &replacement[..])] {
        let refresh = ck_exec::codex_siwc_bind::lock_file(&busy).unwrap();
        let waiting = post(json!([document, batch_document("waiting-peer.json", seed)]));
        tokio::pin!(waiting);
        assert!(
            tokio::time::timeout(Duration::from_millis(300), &mut waiting)
                .await
                .is_err(),
            "the batch waits for the refresh"
        );
        // The waiting batch holds neither the management nor the store gate.
        let other = post(json!([batch_document("unrelated.json", seed)])).await.unwrap();
        assert_eq!(other.status(), 200);
        db.save(&busy, refreshed).unwrap();
        drop(refresh);
        let response = waiting.await.unwrap();
        assert_eq!(response.status(), status);
        assert_eq!(db.read(&busy).unwrap().unwrap(), kept);
        let peer = db.auth_dir().join("waiting-peer.json");
        assert_eq!(db.read(&peer).unwrap().is_some(), status == 200, "all or nothing");
    }
}

#[tokio::test]
async fn credential_batch_saves_and_deletes_atomically_with_raw_source_conditions() {
    use sha2::Digest as _;
    let f = Fixture::new("credential-batch-delete", false).await;
    let seed = br#"{"type":"claude","access_token":"original-fixture","opaque":{"keep":true}}"#;
    let replacement = br#" {"type":"claude","access_token":"replacement-fixture","opaque":[2,true]}
"#;
    let response = f
        .request(wreq::Method::POST, "/admin/v1/credentials/batch")
        .json(&json!({"documents":[batch_document("a.json",seed),batch_document("b.json",seed)]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let before = std::fs::read(f.state.config_store().path().unwrap()).unwrap();
    let head = f.head();
    let removed_path = f.rt.config().auth_dir.join("b.json");
    assert!(f.rt.store().snapshot().iter().any(
        |credential| matches!(&credential.source, ck_core::credential::Source::File(path) if path == &removed_path)
    ));
    let hash = format!("{:x}", sha2::Sha256::digest(seed));
    let mut update = batch_document("a.json", replacement);
    update["if_match"] = hash.clone().into();
    let deletion = json!({"name":"b.json","if_match":hash});
    for (bad, status) in [
        (json!({"name":"b.json","if_match":"00".repeat(32)}), 412),
        (json!({"name":"../outside.json","if_match":hash}), 400),
        (json!({"name":"a.json","if_match":hash}), 400),
        (json!({"name":"b.json"}), 400),
    ] {
        let response = f
            .request(wreq::Method::POST, "/admin/v1/credentials/batch")
            .json(&json!({"documents":[update],"deletions":[bad]}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), status);
        assert_eq!(download_credential(&f, "a.json").await, seed);
        assert_eq!(download_credential(&f, "b.json").await, seed);
        assert_eq!(std::fs::read(f.state.config_store().path().unwrap()).unwrap(), before);
    }
    let blocked = ck_server::testing::block_checkpoint_install(f.state.config_store());
    let response = f
        .request(wreq::Method::POST, "/admin/v1/credentials/batch")
        .json(&json!({"documents":[update],"deletions":[deletion]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 500);
    assert_eq!(download_credential(&f, "a.json").await, seed);
    assert_eq!(download_credential(&f, "b.json").await, seed);
    assert_eq!(std::fs::read(f.rt.config().auth_dir.join("b.json")).unwrap(), seed);
    drop(blocked);
    let response = f
        .request(wreq::Method::POST, "/admin/v1/credentials/batch")
        .json(&json!({"documents":[update],"deletions":[deletion]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let receipt: Value = response.json().await.unwrap();
    assert_eq!(receipt["saved"], 1);
    assert_eq!(receipt["deleted"], 1);
    assert_eq!(download_credential(&f, "a.json").await, replacement);
    assert_eq!(
        f.request(wreq::Method::GET, "/v8/management/credentials/download?name=b.json")
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    assert!(!f.rt.config().auth_dir.join("b.json").exists());
    assert!(
        !f.rt.store().snapshot().iter().any(
            |credential| matches!(&credential.source, ck_core::credential::Source::File(path) if path == &removed_path)
        ),
        "removed credential must be revoked before acknowledgement"
    );
    assert_eq!(
        f.head(),
        head,
        "credential deletion must not restore/change configuration history"
    );
    let copy = f.dir.join("mixed-portable.db");
    std::fs::copy(f.state.config_store().path().unwrap(), &copy).unwrap();
    let store = Arc::new(ck_server::testing::config_store(&copy).unwrap());
    let db = ck_server::store::credential_store::CredentialDb::initialize(store.clone()).unwrap();
    assert_eq!(db.read(&db.auth_dir().join("a.json")).unwrap().unwrap(), replacement);
    assert!(db.read(&db.auth_dir().join("b.json")).unwrap().is_none());
    let remove_last =
        json!({"deletions":[{"name":"a.json","if_match":format!("{:x}",sha2::Sha256::digest(replacement))}]});
    let response = f
        .request(wreq::Method::POST, "/admin/v1/credentials/batch")
        .json(&remove_last)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let receipt: Value = response.json().await.unwrap();
    assert_eq!(receipt["saved"], 0);
    assert_eq!(receipt["deleted"], 1);
    assert_eq!(
        f.request(wreq::Method::POST, "/admin/v1/credentials/batch")
            .json(&remove_last)
            .send()
            .await
            .unwrap()
            .status(),
        412,
        "a captured deletion cannot succeed after the source is gone"
    );
}

#[tokio::test]
async fn credential_batch_rejects_a_later_invalid_or_conflicting_document_without_partial_writes() {
    let f = Fixture::new("credential-batch-rejected", false).await;
    let seed = br#"{"type":"claude","disabled":false,"access_token":"original-fixture"}"#;
    for name in ["a.json", "b.json"] {
        let response = f
            .request(wreq::Method::POST, &format!("/v8/management/credentials?name={name}"))
            .body(seed.to_vec())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
    }
    let before = std::fs::read(f.state.config_store().path().unwrap()).unwrap();
    let revision = f.state.config_store().load().unwrap().rev;
    let mut config_watch = f.rt.subscribe_config();
    let runtime_tokens = || {
        let mut tokens: Vec<_> =
            f.rt.store()
                .snapshot()
                .iter()
                .map(|credential| (credential.id.clone(), credential.str("access_token").map(str::to_owned)))
                .collect();
        tokens.sort();
        tokens
    };
    let runtime_before = runtime_tokens();
    assert_eq!(runtime_before.len(), 2);
    assert!(
        runtime_before
            .iter()
            .all(|(_, token)| token.as_deref() == Some("original-fixture"))
    );
    let replacement = br#"{"type":"claude","access_token":"replacement-fixture"}"#;
    let mut conflict = batch_document("b.json", replacement);
    conflict["if_match"] = "00".repeat(32).into();
    let mut exists = batch_document("b.json", replacement);
    exists["if_none_match"] = true.into();
    for (later, status) in [(batch_document("b.json", b"{"), 400), (conflict, 412), (exists, 412)] {
        let response = f
            .request(wreq::Method::POST, "/admin/v1/credentials/batch")
            .json(&json!({"documents": [batch_document("a.json", replacement), later]}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), status);
        assert_eq!(download_credential(&f, "a.json").await, seed);
        assert_eq!(download_credential(&f, "b.json").await, seed);
        assert_eq!(std::fs::read(f.state.config_store().path().unwrap()).unwrap(), before);
        // Rejection publishes no runtime credential and no configuration revision.
        assert_eq!(runtime_tokens(), runtime_before);
        assert_eq!(f.state.config_store().load().unwrap().rev, revision);
        assert!(config_watch.try_next().is_none());
    }
    // The same observation sees an admitted batch, so the checks above are not vacuous.
    let response = f
        .request(wreq::Method::POST, "/admin/v1/credentials/batch")
        .json(&json!({"documents": [batch_document("a.json", replacement)]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_ne!(std::fs::read(f.state.config_store().path().unwrap()).unwrap(), before);
    assert!(
        runtime_tokens()
            .iter()
            .any(|(_, token)| token.as_deref() == Some("replacement-fixture"))
    );
}

#[tokio::test]
async fn credential_batch_requires_authentication_and_refuses_host_paths() {
    let f = Fixture::new("credential-batch-paths", false).await;
    let bytes = br#"{"type":"claude","access_token":"fixture-only"}"#;
    let response = f
        .client
        .post(format!("{}/admin/v1/credentials/batch", f.base))
        .body("not valid JSON")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 401, "authenticate before parsing the batch");
    let outside = f.dir.join("outside.json");
    std::fs::write(&outside, b"untouched").unwrap();
    let before = std::fs::read(f.state.config_store().path().unwrap()).unwrap();
    let response = f
        .request(wreq::Method::POST, "/admin/v1/credentials/batch?dry_run=true")
        .json(&json!({"documents": [batch_document("good.json", bytes)]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400, "unsupported dry run must not write credentials");
    assert_eq!(std::fs::read(f.state.config_store().path().unwrap()).unwrap(), before);
    for name in [
        outside.to_str().unwrap(),
        "../outside.json",
        "nested/a.json",
        "C:\\outside.json",
    ] {
        let response = f
            .request(wreq::Method::POST, "/admin/v1/credentials/batch")
            .json(&json!({"documents": [batch_document("good.json", bytes), batch_document(name, bytes)]}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 400);
        assert_eq!(std::fs::read(&outside).unwrap(), b"untouched");
        assert_eq!(std::fs::read(f.state.config_store().path().unwrap()).unwrap(), before);
    }
}

async fn rejected(response: wreq::Response, status: u16, code: &str) -> Value {
    assert_eq!(response.status().as_u16(), status);
    assert!(
        response.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("application/json")
    );
    assert_eq!(response.headers()["cache-control"], "no-store");
    let body = response.json::<Value>().await.unwrap();
    assert_eq!(body["error"], code);
    assert!(body["message"].as_str().is_some());
    body
}

#[tokio::test]
async fn credential_sources_are_coherent_managed_bytes_not_cache_or_sql_reads() {
    use base64::Engine as _;
    use sha2::Digest as _;
    let f = Fixture::new("credential-sources-memory", false).await;
    let db = f.rt.store().credential_backend();
    let bytes = b" \n{\"opaque\":{\"future\":[1,true]}}\n";
    let path = db.auth_dir().join("opaque.json");
    db.save(&path, bytes).unwrap();
    db.save(&f.dir.join("external.json"), br#"{"external":"not a managed target"}"#)
        .unwrap();
    std::fs::write(&path, b"untrusted cache edit").unwrap();
    let storage = ck_server::testing::database_connection(f.state.config_store());
    storage.execute_batch("BEGIN EXCLUSIVE").unwrap();
    let response = f.request(wreq::Method::GET, "/admin/v1/credentials").send().await;
    storage.execute_batch("ROLLBACK").unwrap();
    drop(storage);
    let response = response.unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["auth_dir"], db.auth_dir().to_str().unwrap());
    let documents = body["documents"].as_array().unwrap();
    assert_eq!(documents.len(), 1, "never authorize arbitrary external host paths");
    assert_eq!(documents[0]["name"], "opaque.json");
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(documents[0]["data"].as_str().unwrap())
            .unwrap(),
        bytes
    );
    assert_eq!(documents[0]["sha256"], format!("{:x}", sha2::Sha256::digest(bytes)));
}

#[tokio::test]
async fn committed_config_remains_readable_when_persistence_is_unavailable() {
    let f = Fixture::new("read-without-persistence", false).await;
    let (tag, committed) = f.export().await;

    // Fault at the fixture storage boundary: exclude all SQLite readers, not just
    // access to the published backup. The HTTP query must use accepted memory.
    let storage = ck_server::testing::database_connection(f.state.config_store());
    storage.execute_batch("BEGIN EXCLUSIVE").unwrap();
    let response = f.request(wreq::Method::GET, "/admin/v1/export").send().await;
    storage.execute_batch("ROLLBACK").unwrap();
    drop(storage);

    let response = response.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let body = response.json::<Value>().await.unwrap();
    assert_eq!(status, 200, "committed reads must not depend on storage: {body}");
    assert_eq!(headers["etag"], tag);
    assert_eq!(body, committed);
}

#[tokio::test]
async fn resource_and_history_queries_keep_committed_identity_without_storage() {
    let f = Fixture::new("resource-history-memory", false).await;
    // Ordinals oppose the ids' alphabetical order, so the listing proves `ord`.
    for (id, ord) in [("a-later", 20), ("b-first", 10)] {
        let response = f
            .request(wreq::Method::PUT, &format!("/admin/v1/r/provider/{id}"))
            .json(
                &json!({"ord": ord, "description": "retained description", "spec": {"type": "claude",
                "base-url": "http://127.0.0.1:1", "models": [{"name": "claude-sonnet-4-6", "alias": id}],
                "keys": [{"api-key": format!("sk-fake-{id}")}]}}),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
    }
    let revision = f.head();
    let original = f.state.config_store().state_at(revision).unwrap();
    let committed = f.state.config_store().snapshot().unwrap().1;
    let paths = [
        "/admin/v1/r/provider".to_owned(),
        "/admin/v1/r/provider/b-first".to_owned(),
        "/admin/v1/revisions".to_owned(),
        format!("/admin/v1/revisions/{revision}"),
        "/admin/v1/effective/config".to_owned(),
    ];
    let mut expected = Vec::new();
    for path in &paths {
        let response = f.request(wreq::Method::GET, path).send().await.unwrap();
        assert_eq!(response.status(), 200, "{path}");
        expected.push(response.json::<Value>().await.unwrap());
    }
    let storage = ck_server::testing::database_connection(f.state.config_store());
    storage.execute_batch("BEGIN EXCLUSIVE").unwrap();
    let mut actual = Vec::new();
    for path in &paths {
        let response = f.request(wreq::Method::GET, path).send().await.unwrap();
        actual.push((path, response.status(), response.json::<Value>().await.unwrap()));
    }
    let history = f.state.config_store().state_at(revision);
    storage.execute_batch("ROLLBACK").unwrap();
    for ((path, status, body), expected) in actual.into_iter().zip(expected) {
        assert_eq!(status, 200, "{path}: {body}");
        assert_eq!(body, expected, "{path}");
    }
    assert_eq!(history.unwrap(), original);
    let providers: Vec<_> = original.iter().filter(|r| r.kind == Kind::Provider).collect();
    assert_eq!(
        providers.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
        ["b-first", "a-later"]
    );
    assert_eq!(providers[0].description, "retained description");
    let first = committed
        .iter()
        .find(|r| r.kind == Kind::Provider && r.id == "b-first")
        .unwrap();
    assert_eq!(
        first.spec["models"],
        json!([{"name": "claude-sonnet-4-6", "alias": "b-first"}])
    );
}

#[tokio::test]
async fn readers_keep_the_previous_complete_view_during_a_slow_mutation() {
    let f = Fixture::new("overlapping-save", false).await;
    let (tag, previous) = f.export().await;
    let retained = f.state.config_store().load().unwrap();
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let owner = f.state.clone();
    // The existing public edit callback is an explicit validation/effect port.
    // Hold the candidate there, not with a sleep or a private-state inspection.
    let writer = std::thread::spawn(move || {
        owner.config_store().edit("test", "slow candidate", |document| {
            document["routing"]["retry"]["request-retry"] = json!(9);
            let _ = entered.send(());
            wait.recv_timeout(Duration::from_secs(10))
                .map_err(|_| ck_server::store::config_store::CommitError::invalid("test release timed out"))?;
            Ok(())
        })
    });
    started.await.unwrap();
    let response = f.request(wreq::Method::GET, "/admin/v1/export").send().await;
    release.send(()).unwrap();
    let committed = writer.join().unwrap().unwrap();
    let response = response.unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["etag"], tag);
    assert_eq!(response.json::<Value>().await.unwrap(), previous);
    let (new_tag, current) = f.export().await;
    assert_eq!(new_tag, format!("\"{}\"", committed.rev));
    assert_eq!(current["revision"], committed.rev);
    assert_eq!(
        f.state
            .config_store()
            .load()
            .unwrap()
            .config
            .routing
            .retry
            .request_retry,
        9
    );
    assert_eq!(retained.config.routing.retry.request_retry, 3);
    assert_eq!(retained.rev + 1, committed.rev);
    assert_ne!(new_tag, tag);
}

#[tokio::test]
async fn admin_auth_failures_use_the_admin_error_contract_and_expose_revisions() {
    let f = Fixture::new("auth-contract", false).await;
    for (key, status) in [(None, 401), (Some("wrong"), 401)] {
        let mut request = f.client.get(format!("{}/admin/v1/effective/config", f.base));
        if let Some(key) = key {
            request = request.bearer_auth(key);
        }
        let response = request.send().await.unwrap();
        assert!(response.headers().contains_key("x-ck-published-revision"));
        for name in ["x-ck-revision", "x-ck-published-revision"] {
            assert!(
                response
                    .headers()
                    .get_all("access-control-expose-headers")
                    .iter()
                    .any(|field| {
                        field
                            .to_str()
                            .unwrap()
                            .split(',')
                            .any(|value| value.trim().eq_ignore_ascii_case(name))
                    })
            );
        }
        rejected(response, status, "unauthorized").await;
    }
    let response = f
        .request(wreq::Method::GET, "/admin/v1/effective/config")
        .header("x-forwarded-for", "203.0.113.5")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let remote = Fixture::new("remote-auth-contract", true).await;
    rejected(
        remote
            .request(wreq::Method::GET, "/admin/v1/effective/config")
            .header("x-forwarded-for", "203.0.113.5")
            .send()
            .await
            .unwrap(),
        403,
        "forbidden",
    )
    .await;
}

#[tokio::test]
async fn validation_faults_locate_fields() {
    let f = Fixture::new("validation-pointers", false).await;
    let fault = rejected(
        f.request(wreq::Method::PUT, "/admin/v1/r/setting/observability")
            .json(&json!({"spec": {"logs": {"debug": []}}}))
            .send()
            .await
            .unwrap(),
        422,
        "invalid_config",
    )
    .await;
    assert_eq!(fault["resource"], "setting/observability");
    assert_eq!(fault["pointer"], "/logs/debug");
    // The plugin kind is gone: its resource paths answer the ordinary unknown kind.
    for method in [
        wreq::Method::GET,
        wreq::Method::PUT,
        wreq::Method::PATCH,
        wreq::Method::DELETE,
    ] {
        rejected(
            f.request(method, "/admin/v1/r/plugin/opaque")
                .json(&json!({"spec": {}}))
                .send()
                .await
                .unwrap(),
            404,
            "unknown_kind",
        )
        .await;
    }
}

#[tokio::test]
async fn strong_resource_conditions_include_wildcards_lists_and_absence() {
    let f = Fixture::new("conditions", false).await;
    let path = "/admin/v1/r/setting/routing";
    let response = f.request(wreq::Method::GET, path).send().await.unwrap();
    let tag = response.headers()["etag"].to_str().unwrap().to_owned();
    let original = response.json::<Value>().await.unwrap();
    assert_eq!(tag, format!("\"{}\"", original["rev"]));
    for header in ["1", "\"01\"", "\"1", "*, \"1\"", "\"1\",", ""] {
        rejected(
            f.request(wreq::Method::PATCH, path)
                .header("if-match", header)
                .json(&json!({"retry": {"request-retry": 7}}))
                .send()
                .await
                .unwrap(),
            400,
            "invalid_if_match",
        )
        .await;
    }
    rejected(
        f.request(wreq::Method::PATCH, path)
            .header("if-match", format!("W/{tag}"))
            .json(&json!({"retry": {"request-retry": 7}}))
            .send()
            .await
            .unwrap(),
        412,
        "precondition_failed",
    )
    .await;
    for method in [wreq::Method::PUT, wreq::Method::PATCH, wreq::Method::DELETE] {
        for condition in ["*", "\"99999\""] {
            let body = rejected(
                f.request(method.clone(), "/admin/v1/r/client-key/missing")
                    .header("if-match", condition)
                    .json(&json!({"spec": {"key": "must-not-be-created"}}))
                    .send()
                    .await
                    .unwrap(),
                412,
                "precondition_failed",
            )
            .await;
            assert_eq!(body["resource"], "client-key/missing");
        }
    }
    let response = f
        .request(wreq::Method::PATCH, path)
        .header("if-match", format!("W/{tag}, \"0\", {tag}"))
        .json(&json!({"retry": {"request-retry": 7}}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let next_tag = response.headers()["etag"].to_str().unwrap().to_owned();
    let done = response.json::<Value>().await.unwrap();
    assert_eq!(done["resource"]["spec"]["retry"]["request-retry"], 7);
    assert_eq!(next_tag, format!("\"{}\"", done["resource"]["rev"]));
    rejected(
        f.request(wreq::Method::PATCH, path)
            .header("if-match", &tag)
            .json(&json!({"retry": {"request-retry": 2}}))
            .send()
            .await
            .unwrap(),
        412,
        "precondition_failed",
    )
    .await;
    // Wildcard is an existence precondition, not an omitted precondition.
    let response = f
        .request(wreq::Method::PATCH, path)
        .header("if-match", "*")
        .json(&json!({"retry": {"request-retry": 7}}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["etag"], next_tag);
    let no_op = response.json::<Value>().await.unwrap();
    assert_eq!(no_op["written"], false);
    assert_eq!(no_op["resource"], done["resource"]);

    let key_path = "/admin/v1/r/client-key/create-only";
    let create = || {
        f.request(wreq::Method::PUT, key_path)
            .header("if-none-match", "*")
            .json(&json!({"spec": {"key": "create-only-test-secret"}}))
    };
    assert_eq!(create().send().await.unwrap().status(), 200);
    rejected(create().send().await.unwrap(), 412, "precondition_failed").await;
    rejected(
        f.request(wreq::Method::PUT, key_path)
            .header("if-match", "*")
            .header("if-none-match", "*")
            .json(&json!({"spec": {"key": "ignored"}}))
            .send()
            .await
            .unwrap(),
        400,
        "invalid_if_none_match",
    )
    .await;
    rejected(
        f.request(wreq::Method::PATCH, path)
            .header("if-none-match", "*")
            .json(&json!({}))
            .send()
            .await
            .unwrap(),
        400,
        "invalid_if_none_match",
    )
    .await;
}

#[tokio::test]
async fn collection_replacements_require_atomic_global_preconditions() {
    let f = Fixture::new("collection", false).await;
    let (old_tag, backup) = f.export().await;
    let initial = f.head();
    for path in [
        "/admin/v1/import".to_owned(),
        format!("/admin/v1/revisions/{initial}/restore"),
    ] {
        rejected(
            f.request(wreq::Method::POST, &path).json(&backup).send().await.unwrap(),
            428,
            "precondition_required",
        )
        .await;
        rejected(
            f.request(wreq::Method::POST, &path)
                .header("if-match", "*")
                .json(&backup)
                .send()
                .await
                .unwrap(),
            428,
            "precondition_required",
        )
        .await;
        rejected(
            f.request(wreq::Method::POST, &format!("{path}?dry_run=true"))
                .header("if-match", "*")
                .json(&backup)
                .send()
                .await
                .unwrap(),
            428,
            "precondition_required",
        )
        .await;
    }
    // A different client changes the owner's state after the export.
    let response = f
        .request(wreq::Method::PATCH, "/admin/v1/r/setting/routing")
        .json(&json!({"retry": {"request-retry": 8}}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let current = f.head();
    assert!(current > initial);
    for path in [
        "/admin/v1/import".to_owned(),
        "/admin/v1/import?dry_run=true".to_owned(),
        format!("/admin/v1/revisions/{initial}/restore"),
    ] {
        rejected(
            f.request(wreq::Method::POST, &path)
                .header("if-match", &old_tag)
                .json(&backup)
                .send()
                .await
                .unwrap(),
            412,
            "precondition_failed",
        )
        .await;
        assert_eq!(f.head(), current);
        assert_eq!(
            f.state
                .config_store()
                .load()
                .unwrap()
                .config
                .routing
                .retry
                .request_retry,
            8
        );
    }
    rejected(
        f.request(wreq::Method::POST, "/admin/v1/apply")
            .header("if-match", &old_tag)
            .json(&json!({"ops": []}))
            .send()
            .await
            .unwrap(),
        412,
        "precondition_failed",
    )
    .await;
    let current_tag = format!("\"{current}\"");
    let response = f
        .request(wreq::Method::POST, "/admin/v1/import?dry_run=true")
        .header("if-match", format!("\"0\", {current_tag}"))
        .json(&backup)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert!(response.headers().get("etag").is_none());
    let body = response.json::<Value>().await.unwrap();
    assert_eq!(body["dry_run"], true);
    assert_eq!(body["written"], false);
    assert_eq!(body["revision"], current);
    assert_eq!(f.head(), current);
    let response = f
        .request(
            wreq::Method::POST,
            &format!("/admin/v1/revisions/{initial}/restore?dry_run=true"),
        )
        .header("if-match", &current_tag)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.json::<Value>().await.unwrap()["written"], false);
    assert_eq!(f.head(), current);
    let response = f
        .request(wreq::Method::POST, "/admin/v1/import")
        .header("if-match", &current_tag)
        .json(&backup)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let head = response.headers()["x-ck-revision"]
        .to_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    let body = response.json::<Value>().await.unwrap();
    assert_eq!(body["revision"], head);
    assert_eq!(head, current + 1);
    assert_eq!(f.rt.config().routing.retry.request_retry, 3);
    // Even an uninitialized destination requires its concrete revision 0 rather
    // than a wildcard that says nothing about the snapshot being replaced.
    // Use the existing explicit environment-secret port: clearing canonical
    // resources now correctly removes the config secret from the running view.
    let empty = Fixture::with_management_password("uninitialized", false, KEY.into()).await;
    ck_server::testing::database_execute(
        empty.state.config_store(),
        "DELETE FROM change; DELETE FROM resource; DELETE FROM revision;",
    )
    .unwrap();
    assert_eq!(empty.head(), 0);
    rejected(
        empty
            .request(wreq::Method::POST, "/admin/v1/import")
            .header("if-match", "*")
            .json(&backup)
            .send()
            .await
            .unwrap(),
        428,
        "precondition_required",
    )
    .await;
    rejected(
        empty
            .request(wreq::Method::POST, "/admin/v1/apply")
            .header("if-match", "*")
            .json(&json!({"ops": []}))
            .send()
            .await
            .unwrap(),
        412,
        "precondition_failed",
    )
    .await;
}

#[tokio::test]
async fn successful_config_restore_reports_restart_without_restoring_revoked_credentials() {
    use sha2::Digest as _;
    let f = Fixture::new("restore-domain-and-restart", false).await;
    let name = "revoked-before-config-restore.json";
    let raw = br#"{"type":"claude","disabled":true,"access_token":"fake-revoked-credential","opaque":{"keep":true}}"#;
    let saved = f
        .request(wreq::Method::POST, "/admin/v1/credentials/batch")
        .json(&json!({"documents":[batch_document(name, raw)]}))
        .send()
        .await
        .unwrap();
    assert_eq!(saved.status(), 200);
    assert_eq!(download_credential(&f, name).await, raw);
    let credential_path = f.rt.config().auth_dir.join(name);
    assert!(f.rt.store().snapshot().iter().any(|credential| matches!(
        &credential.source, ck_core::credential::Source::File(path) if path == &credential_path
    )));

    let response = f
        .request(wreq::Method::PATCH, "/admin/v1/r/setting/routing")
        .json(&json!({"retry":{"request-retry":8}}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body = response.json::<Value>().await.unwrap();
    assert_eq!(body["restart_required"], false);
    assert_eq!(body["written"], true);
    let baseline = f.head();
    assert_eq!(body["revision"], baseline);
    let startup_port = f.rt.config().port;
    let changed_port = if startup_port == 8318 { 8319 } else { 8318 };

    let response = f
        .request(wreq::Method::PATCH, "/admin/v1/r/setting/server?dry_run=true")
        .json(&json!({"port":changed_port}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert!(response.headers().get("etag").is_none());
    let body = response.json::<Value>().await.unwrap();
    assert_eq!(body["restart_required"], true);
    assert_eq!(body["written"], false);
    assert_eq!(body["revision"], baseline);
    assert_eq!(f.head(), baseline);
    assert_eq!(f.rt.config().port, startup_port);

    let response = f
        .request(wreq::Method::PATCH, "/admin/v1/r/setting/server")
        .json(&json!({"port":changed_port}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body = response.json::<Value>().await.unwrap();
    assert_eq!(body["restart_required"], true);
    assert_eq!(body["written"], true);
    assert_eq!(body["revision"], f.head());
    assert_eq!(f.rt.config().port, changed_port);

    let deleted = f
        .request(wreq::Method::POST, "/admin/v1/credentials/batch")
        .json(&json!({"deletions":[{"name":name,"if_match":format!("{:x}",sha2::Sha256::digest(raw))}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), 200);
    assert_eq!(
        f.request(
            wreq::Method::GET,
            &format!("/v8/management/credentials/download?name={name}")
        )
        .send()
        .await
        .unwrap()
        .status(),
        404
    );
    let (current_tag, _) = f.export().await;
    let response = f
        .request(wreq::Method::POST, &format!("/admin/v1/revisions/{baseline}/restore"))
        .header("if-match", current_tag)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body = response.json::<Value>().await.unwrap();
    assert_eq!(body["written"], true);
    assert_eq!(body["dry_run"], false);
    assert_eq!(body["restart_required"], false);
    assert_eq!(body["revision"], f.head());
    assert_eq!(f.rt.config().port, startup_port);
    assert_eq!(f.rt.config().routing.retry.request_retry, 8);
    assert_eq!(
        f.request(
            wreq::Method::GET,
            &format!("/v8/management/credentials/download?name={name}")
        )
        .send()
        .await
        .unwrap()
        .status(),
        404,
        "configuration history resurrected a revoked document"
    );
    assert!(
        !f.rt.store().snapshot().iter().any(|credential| matches!(
            &credential.source, ck_core::credential::Source::File(path) if path == &credential_path
        )),
        "configuration restore re-admitted the revoked credential"
    );
    let copy = f.dir.join("restored-config-only.db");
    std::fs::copy(f.state.config_store().path().unwrap(), &copy).unwrap();
    let restored = Arc::new(ck_server::testing::config_store(&copy).unwrap());
    let db = ck_server::store::credential_store::CredentialDb::initialize(restored.clone()).unwrap();
    assert!(db.read(&db.auth_dir().join(name)).unwrap().is_none());
    assert_eq!(restored.load().unwrap().config.port, startup_port);
    assert_eq!(restored.load().unwrap().config.routing.retry.request_retry, 8);
    drop(db);
    drop(restored);
    f.rt.finish_shutdown().await.unwrap();
}

#[tokio::test]
async fn import_validates_the_complete_versioned_envelope_and_media_type() {
    let f = Fixture::new("export-format", false).await;
    let (tag, backup) = f.export().await;
    for (field, replacement, status, code) in [
        ("format", json!("other-config"), 422, "invalid_export"),
        ("version", json!(2), 422, "invalid_export"),
        ("version", json!("1"), 400, "invalid_body"),
        ("revision", json!(-1), 400, "invalid_body"),
        ("unexpected", json!("do-not-echo-this-secret"), 400, "invalid_body"),
    ] {
        let mut invalid = backup.clone();
        invalid[field] = replacement;
        let response = rejected(
            f.request(wreq::Method::POST, "/admin/v1/import")
                .header("if-match", &tag)
                .json(&invalid)
                .send()
                .await
                .unwrap(),
            status,
            code,
        )
        .await;
        assert!(!response.to_string().contains("do-not-echo-this-secret"));
    }
    for field in ["format", "version", "revision", "resources"] {
        let mut invalid = backup.clone();
        invalid.as_object_mut().unwrap().remove(field);
        rejected(
            f.request(wreq::Method::POST, "/admin/v1/import")
                .header("if-match", &tag)
                .json(&invalid)
                .send()
                .await
                .unwrap(),
            400,
            "invalid_body",
        )
        .await;
    }
    rejected(
        f.request(wreq::Method::POST, "/admin/v1/import")
            .header("if-match", &tag)
            .header("content-type", "application/json-not-really")
            .body(backup.to_string())
            .send()
            .await
            .unwrap(),
        415,
        "unsupported_media_type",
    )
    .await;
    let response = f
        .request(wreq::Method::POST, "/admin/v1/import?dry_run=true")
        .header("if-match", &tag)
        .header("content-type", "Application/JSON; charset=utf-8")
        .body(backup.to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(f.head(), backup["revision"].as_u64().unwrap());
}

#[tokio::test]
async fn errors_never_echo_parser_or_database_messages_and_keep_safe_locations() {
    let f = Fixture::new("safe-errors", false).await;
    let parser_secret = "parser-secret-must-not-be-serialized";
    let response = rejected(
        f.request(wreq::Method::PUT, "/admin/v1/r/setting/server")
            .json(&json!({"spec": {"port": parser_secret}}))
            .send()
            .await
            .unwrap(),
        422,
        "invalid_config",
    )
    .await;
    assert!(!response.to_string().contains(parser_secret));
    assert_eq!(response["resource"], "setting/server");
    let response = rejected(
        f.request(wreq::Method::PUT, "/admin/v1/r/provider/missing-secret")
            .json(&json!({"spec": {"type": "claude", "keys": [{"api-key": "$secret"}]}}))
            .send()
            .await
            .unwrap(),
        422,
        "secret_destination_changed",
    )
    .await;
    assert_eq!(response["resource"], "provider/missing-secret");
    // A new provider cannot bind an absent secret to a newly specified destination.
    assert_eq!(response["pointer"], "/keys/0/api-key");
    assert_eq!(response["pointers"], json!(["/keys/0/api-key"]));
    let response = rejected(
        f.request(wreq::Method::PUT, "/admin/v1/r/client-key/missing-secret")
            .json(&json!({"spec": {"key": "$secret"}}))
            .send()
            .await
            .unwrap(),
        422,
        "invalid_config",
    )
    .await;
    assert_eq!(response["resource"], "client-key/missing-secret");
    assert_eq!(response["pointer"], "/key");
    let response = f.request(wreq::Method::PUT, "/admin/v1/r/provider/destination-bound")
        .json(&json!({"spec": {"type": "claude", "base-url": "http://127.0.0.1:1",
            "headers": {"Authorization": "destination-header-secret"}, "keys": [{"api-key": "destination-key-secret"}]}}))
        .send().await.unwrap();
    assert_eq!(response.status(), 200);
    let mut candidate = response.json::<Value>().await.unwrap()["resource"]["spec"].clone();
    candidate["base-url"] = "http://127.0.0.1:2".into();
    let response = rejected(
        f.request(wreq::Method::PUT, "/admin/v1/r/provider/destination-bound")
            .json(&json!({"spec": candidate}))
            .send()
            .await
            .unwrap(),
        422,
        "secret_destination_changed",
    )
    .await;
    assert_eq!(response["resource"], "provider/destination-bound");
    let pointers = response["pointers"].as_array().unwrap();
    assert!(pointers.contains(&json!("/keys/0/api-key")));
    assert!(pointers.contains(&json!("/headers/Authorization")));
    assert!(pointers.contains(&response["pointer"]));
    assert!(!response.to_string().contains("destination-header-secret"));
    assert!(!response.to_string().contains("destination-key-secret"));
    let (tag, _) = f.export().await;
    let response = rejected(
        f.request(wreq::Method::POST, "/admin/v1/import")
            .header("if-match", &tag)
            .header("content-type", "application/yaml")
            .body(format!("server:\n  port: '{parser_secret}'\n"))
            .send()
            .await
            .unwrap(),
        422,
        "invalid_config",
    )
    .await;
    assert!(!response.to_string().contains(parser_secret));
    let response = rejected(
        f.request(wreq::Method::POST, "/admin/v1/apply")
            .json(&json!({"ops": [], "do-not-echo-this-secret": parser_secret}))
            .send()
            .await
            .unwrap(),
        400,
        "invalid_body",
    )
    .await;
    assert!(!response.to_string().contains(parser_secret));
    rejected(
        f.request(wreq::Method::POST, "/admin/v1/apply?dry_run=not-a-bool")
            .json(&json!({"ops": []}))
            .send()
            .await
            .unwrap(),
        400,
        "invalid_query",
    )
    .await;
    rejected(
        f.request(wreq::Method::GET, "/admin/v1/revisions/not-a-number")
            .send()
            .await
            .unwrap(),
        400,
        "invalid_path",
    )
    .await;
    let head = f.head();
    let _blocked = ck_server::testing::block_checkpoint_install(f.state.config_store());
    let response = rejected(
        f.request(wreq::Method::PATCH, "/admin/v1/r/setting/routing")
            .json(&json!({"retry": {"request-retry": 9}}))
            .send()
            .await
            .unwrap(),
        500,
        "store_failed",
    )
    .await;
    assert_eq!(response["message"], "the config store could not complete the request");
    assert!(!response.to_string().contains(f.dir.to_str().unwrap()));
    assert_eq!(f.head(), head);
}

#[tokio::test]
async fn ordinary_http_reads_and_write_responses_mask_every_secret() {
    let f = Fixture::new("masked", false).await;
    let secrets = [
        "provider-http-secret",
        "header-http-secret",
        "proxy-http-secret",
        "github-http-secret",
        "relay-http-secret",
    ];
    for (path, body) in [(
        "/admin/v1/r/provider/main",
        json!({"spec": {"type": "claude", "base-url": "http://127.0.0.1:1", "headers": {"Authorization": secrets[1]}, "proxy-url": format!("http://user:{}@127.0.0.1:1", secrets[2]), "keys": [{"api-key": secrets[0]}]}}),
    )] {
        let response = f.request(wreq::Method::PUT, path).json(&body).send().await.unwrap();
        assert_eq!(response.status(), 200);
        let text = response.text().await.unwrap();
        for secret in secrets {
            assert!(!text.contains(secret));
        }
    }
    for (method, path, body, secret) in [
        (
            wreq::Method::PATCH,
            "/admin/v1/r/setting/server",
            json!({"github-token": secrets[3]}),
            secrets[3],
        ),
        (
            wreq::Method::PUT,
            "/admin/v1/r/setting/oauth",
            json!({"spec": {"providers": {"aistudio": {"relay-keys": [secrets[4]]}}}}),
            secrets[4],
        ),
    ] {
        let response = f.request(method, path).json(&body).send().await.unwrap();
        assert_eq!(response.status(), 200, "{path}");
        assert!(!response.text().await.unwrap().contains(secret), "{path}");
    }
    let head = f.head();
    for path in [
        "/admin/v1/r/provider".to_owned(),
        "/admin/v1/r/provider/main".to_owned(),
        "/admin/v1/r/client-key".to_owned(),
        "/admin/v1/r/setting".to_owned(),
        "/admin/v1/effective/config".to_owned(),
        "/admin/v1/export".to_owned(),
        format!("/admin/v1/revisions/{head}"),
    ] {
        let response = f.request(wreq::Method::GET, &path).send().await.unwrap();
        assert_eq!(response.status(), 200, "{path}");
        let text = response.text().await.unwrap();
        for secret in secrets.into_iter().chain(["client-http-test-secret", KEY]) {
            assert!(!text.contains(secret), "{path} leaked a secret");
        }
        assert!(!text.contains("$2b$"), "{path} leaked the management hash");
        assert!(!text.contains("$2a$"), "{path} leaked the management hash");
    }
    let response = f
        .request(wreq::Method::GET, "/admin/v1/export?include_secrets=true")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let text = response.text().await.unwrap();
    for secret in secrets.into_iter().chain(["client-http-test-secret"]) {
        assert!(text.contains(secret));
    }
}

#[tokio::test]
async fn committed_configuration_reaches_access_and_runtime_and_credential_commits_do_not() {
    let f = Fixture::new("consumers", false).await;
    let models = |key: &'static str| f.client.get(format!("{}/v1/models", f.base)).bearer_auth(key).send();
    assert_eq!(models("client-http-test-secret").await.unwrap().status(), 200);
    assert_eq!(models("consumer-new-client-key").await.unwrap().status(), 401);
    let response = f
        .request(wreq::Method::POST, "/admin/v1/apply")
        .json(&json!({"ops": [
            {"op": "put", "kind": "client-key", "id": "consumer-new", "spec": {"key": "consumer-new-client-key"}, "if_absent": true},
            {"op": "patch", "kind": "setting", "id": "routing", "patch": {"retry": {"request-retry": 7}}},
        ]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let published: u64 = response.headers()["x-ck-published-revision"]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    let revision = response.json::<Value>().await.unwrap()["revision"].as_u64().unwrap();
    assert_eq!(published, revision, "success follows runtime publication");
    // Access and the runtime view observe the committed version, not a later reload.
    assert_eq!(models("consumer-new-client-key").await.unwrap().status(), 200);
    assert_eq!(f.rt.config().routing.retry.request_retry, 7);
    let before = f.rt.config();
    let mut watch = f.rt.subscribe_config();
    // A credential commit is a different domain: no configuration view is reinstalled.
    let response = f
        .request(wreq::Method::POST, "/admin/v1/credentials/batch")
        .json(&json!({"documents": [batch_document("consumer.json", br#"{"type":"claude","access_token":"consumer-at"}"#)]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert!(Arc::ptr_eq(&before, &f.rt.config()));
    assert!(watch.try_next().is_none());
    assert_eq!(f.head(), revision);
}

#[tokio::test]
async fn one_resource_can_be_read_with_its_secrets_without_the_full_export() {
    let f = Fixture::new("resource-secret", false).await;
    let proxy = "http://user:resource-proxy-secret@127.0.0.1:1";
    let response = f
        .request(wreq::Method::PUT, "/admin/v1/r/setting/requests")
        .json(&json!({"spec": {"proxy-url": proxy}}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let response = f
        .request(wreq::Method::PATCH, "/admin/v1/r/setting/server")
        .json(&json!({"github-token": "unrelated-resource-secret"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let read = |query: &'static str| {
        f.request(wreq::Method::GET, &format!("/admin/v1/r/setting/requests{query}"))
            .send()
    };
    let masked = read("").await.unwrap();
    assert_eq!(masked.status(), 200);
    let masked: Value = masked.json().await.unwrap();
    assert_eq!(masked["spec"]["proxy-url"], "$secret");
    let revealed = read("?include_secrets=true").await.unwrap();
    assert_eq!(revealed.status(), 200);
    assert_eq!(revealed.headers().get("cache-control").unwrap(), "no-store");
    let etag = revealed.headers().get("etag").cloned().unwrap();
    let text = revealed.text().await.unwrap();
    assert!(
        !text.contains("unrelated-resource-secret"),
        "only the named resource is revealed"
    );
    let revealed: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(revealed["spec"]["proxy-url"], proxy);
    assert_eq!(revealed["kind"], "setting");
    assert_eq!(revealed["id"], "requests");
    // The revealed read keeps the resource's own revision tag for conditional writes.
    let masked_tag = read("").await.unwrap().headers().get("etag").cloned().unwrap();
    assert_eq!(etag, masked_tag);
    rejected(read("?include_secrets=maybe").await.unwrap(), 400, "invalid_query").await;
    let anonymous = wreq::Client::new()
        .get(format!("{}/admin/v1/r/setting/requests?include_secrets=true", f.base))
        .send()
        .await
        .unwrap();
    assert_eq!(anonymous.status(), 401);
    assert!(!anonymous.text().await.unwrap().contains("resource-proxy-secret"));
}

#[tokio::test]
async fn effective_config_indexes_match_live_credentials_before_secrets_are_hidden() {
    let f = Fixture::new("effective-indexes", false).await;
    let response = f.request(wreq::Method::POST, "/admin/v1/apply").json(&json!({"ops": [
        {"op": "put", "kind": "provider", "id": "gemini-index", "spec": {
            "type": "gemini", "base-url": "http://127.0.0.1:1", "headers": {"Authorization": "gemini-header-secret"},
            "keys": [{"api-key": "gemini-index-secret"}]
        }},
        {"op": "put", "kind": "provider", "id": "meta-index", "spec": {
            "type": "meta", "base-url": "http://127.0.0.1:1",
            "keys": [{"api-key": "dca:skipped-index-secret"}, {"api-key": "meta-index-secret"}]
        }}
    ]})).send().await.unwrap();
    assert_eq!(response.status(), 200);
    let credentials = f.rt.store().snapshot();
    let index = |provider: &str| {
        let matches: Vec<_> = credentials.iter().filter(|c| c.provider == provider).collect();
        assert_eq!(
            matches.len(),
            1,
            "the fixture must synthesize one {provider} credential"
        );
        let index = ck_core::config::credentials::auth_index(matches[0]);
        assert!(!index.is_empty());
        index
    };
    let response = f
        .request(wreq::Method::GET, "/admin/v1/effective/config")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let effective = response.json::<Value>().await.unwrap();
    assert_eq!(
        effective["api-keys"]["gemini"][0]["keys"][0]["auth_index"],
        index("gemini")
    );
    let meta = effective["api-keys"]["meta"][0]["keys"].as_array().unwrap();
    assert!(
        meta[0].get("auth_index").is_none(),
        "the skipped dca: key must not steal the normal key's index"
    );
    assert_eq!(meta[1]["auth_index"], index("meta"));
    for key in effective["api-keys"]["gemini"][0]["keys"]
        .as_array()
        .unwrap()
        .iter()
        .chain(meta)
    {
        assert_eq!(key["api-key"], "$secret");
    }
    for secret in [
        "gemini-index-secret",
        "gemini-header-secret",
        "dca:skipped-index-secret",
        "meta-index-secret",
    ] {
        assert!(!effective.to_string().contains(secret));
    }
}

#[tokio::test]
async fn query_dry_runs_do_not_write_publish_or_contact_an_upstream() {
    let f = Fixture::new("dry-run", false).await;
    let upstream = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_url = format!("http://{}", upstream.local_addr().unwrap());
    let head = f.head();
    let response = f.request(wreq::Method::POST, "/admin/v1/apply?dry_run=true&message=query-message")
        .json(&json!({"ops": [
            {"op": "set", "path": "routing/retry/request-retry", "value": 10},
            {"op": "put", "kind": "provider", "id": "dry-only", "spec": {"type": "claude", "base-url": upstream_url, "keys": [{"api-key": "dry-run-secret"}]}}
        ]})).send().await.unwrap();
    assert_eq!(response.status(), 200);
    assert!(response.headers().get("etag").is_none());
    let body = response.json::<Value>().await.unwrap();
    assert_eq!(body["dry_run"], true);
    assert_eq!(body["written"], false);
    assert_eq!(body["revision"], head);
    assert!(!body["changes"].as_array().unwrap().is_empty());
    assert!(!body.to_string().contains("dry-run-secret"));
    for (method, path, body) in [
        (
            wreq::Method::PUT,
            "/admin/v1/r/client-key/dry-only?dry_run=true",
            json!({"spec": {"key": "dry-client-secret"}}),
        ),
        (
            wreq::Method::PATCH,
            "/admin/v1/r/setting/routing?dry_run=true",
            json!({"retry": {"request-retry": 11}}),
        ),
        (
            wreq::Method::DELETE,
            "/admin/v1/r/setting/routing?dry_run=true",
            json!(null),
        ),
    ] {
        let response = f.request(method, path).json(&body).send().await.unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(response.json::<Value>().await.unwrap()["written"], false);
    }
    assert_eq!(f.head(), head);
    assert_eq!(f.rt.config().routing.retry.request_retry, 3);
    assert_eq!(
        f.state
            .config_store()
            .load()
            .unwrap()
            .config
            .routing
            .retry
            .request_retry,
        3
    );
    let (_, stored) = f.state.config_store().snapshot().unwrap();
    assert!(!stored.iter().any(|r| r.kind == Kind::Provider && r.id == "dry-only"));
    assert!(!stored.iter().any(|r| r.kind == Kind::ClientKey && r.id == "dry-only"));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), upstream.accept())
            .await
            .is_err(),
        "a dry run contacted an upstream"
    );
}

async fn raw_http(base: &str, headers: &str) -> String {
    let mut stream = tokio::net::TcpStream::connect(base.strip_prefix("http://").unwrap())
        .await
        .unwrap();
    stream.write_all(headers.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    String::from_utf8(response).unwrap()
}

#[tokio::test]
async fn body_limit_is_authenticated_bounded_and_applies_to_chunked_http() {
    let f = Fixture::new("body-bound", false).await;
    for (authorization, status, code) in [
        ("".to_owned(), 401, "missing management key"),
        (format!("Authorization: Bearer {KEY}\r\n"), 413, "body_too_large"),
    ] {
        // No body is sent: authentication and declared-length rejection must not wait
        // for even the first byte of an oversized, attacker-controlled body.
        let response = raw_http(&f.base, &format!(
            "POST /admin/v1/apply HTTP/1.1\r\nHost: localhost\r\n{authorization}Content-Length: {}\r\nConnection: close\r\n\r\n", BODY_LIMIT + 1
        )).await;
        assert!(response.starts_with(&format!("HTTP/1.1 {status}")), "{response}");
        assert!(response.contains(code), "{response}");
    }
    let chunk = bytes::Bytes::from(vec![b' '; 64 * 1024]);
    let chunks = futures_util::stream::iter((0..257).map(move |_| Ok::<_, std::io::Error>(chunk.clone())));
    rejected(
        f.request(wreq::Method::POST, "/admin/v1/apply")
            .body(wreq::Body::wrap_stream(chunks))
            .send()
            .await
            .unwrap(),
        413,
        "body_too_large",
    )
    .await;
    assert_eq!(f.head(), 1);
}

fn log_files(dir: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|path| path.extension().is_some_and(|e| e == "log"))
        .collect()
}

#[tokio::test]
async fn admin_bodies_bypass_capture_and_trusted_proxy_policy_uses_the_real_http_peer() {
    let f = Fixture::new("trust-and-logging", true).await;
    let path = "/admin/v1/effective/config";
    assert_eq!(f.request(wreq::Method::GET, path).send().await.unwrap().status(), 200);
    let forbidden = f
        .request(wreq::Method::GET, path)
        .header("x-forwarded-for", "203.0.113.44")
        .header("x-real-ip", "127.0.0.1")
        .send()
        .await
        .unwrap();
    assert_eq!(
        forbidden.status(),
        403,
        "a trusted proxy's remote client must not inherit loopback privileges"
    );
    assert_eq!(f.request(wreq::Method::POST, "/admin/v1/apply")
        .json(&json!({"ops": [{"op": "put", "kind": "client-key", "id": "logging", "spec": {"key": "capture-must-not-see-this-secret"}}]}))
        .send().await.unwrap().status(), 200);
    let response = f
        .request(wreq::Method::POST, "/%61dmin/v1/apply")
        .body("encoded-path-must-not-be-captured")
        .send()
        .await
        .unwrap();
    let _ = response.bytes().await.unwrap();
    let probe = f
        .client
        .post(format!("{}/capture-probe", f.base))
        .body("ordinary-probe-body")
        .send()
        .await
        .unwrap();
    assert_eq!(probe.status(), 200, "the capture middleware must actually be enabled");
    assert_eq!(probe.text().await.unwrap(), "capture-active");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let files = log_files(&f.dir.join("logs"));
        let contents: Vec<_> = files.iter().map(|p| std::fs::read_to_string(p).unwrap()).collect();
        if contents
            .iter()
            .any(|s| s.contains("ordinary-probe-body") && s.contains("capture-active"))
        {
            assert_eq!(contents.len(), 1, "admin requests must not create capture files");
            assert!(!contents[0].contains("capture-must-not-see-this-secret"));
            assert!(!contents[0].contains("encoded-path-must-not-be-captured"));
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "ordinary probe capture did not finish"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn concurrent_write_responses_keep_the_committed_resource_and_revision_snapshot() {
    let f = Fixture::new("response-snapshot", false).await;
    let responses = futures_util::future::join_all((0..24).map(|i| {
        let request = f
            .request(wreq::Method::PUT, "/admin/v1/r/client-key/racing")
            .json(&json!({"spec": {"key": "racing-test-secret"}, "description": format!("writer-{i}")}));
        async move {
            let response = request.send().await.unwrap();
            assert_eq!(response.status(), 200);
            let revision = response.headers()["x-ck-revision"]
                .to_str()
                .unwrap()
                .parse::<u64>()
                .unwrap();
            let tag = response.headers()["etag"].to_str().unwrap().to_owned();
            let body = response.json::<Value>().await.unwrap();
            assert_eq!(body["revision"], revision);
            assert_eq!(body["resource"]["rev"], revision);
            assert_eq!(body["resource"]["description"], format!("writer-{i}"));
            assert_eq!(body["resource"]["spec"]["key"], "$secret");
            assert_eq!(tag, format!("\"{revision}\""));
            revision
        }
    }))
    .await;
    let mut revisions = responses;
    revisions.sort_unstable();
    revisions.dedup();
    assert_eq!(revisions.len(), 24);
}
