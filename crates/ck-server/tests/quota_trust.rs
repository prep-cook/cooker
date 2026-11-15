//! Bounded trust for quota resets (`routing.cooldown.max-trusted-cooldown`, a
//! cooker addition): one Claude credential answers 429 with a weekly reset six
//! days away, then recovers early. The stated reset is trusted only up to the bound,
//! so the next ordinary request after the bound reaches the upstream and succeeds.
//! Run with and without `save-cooldown-status`; with it, a restart restores the bounded
//! cooldown, not the six days. Requests that arrive together after the bound send one
//! probe upstream. Loopback upstream and fake keys only.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use ck_core::config::Config;
use ck_core::credential::Credential;
use ck_exec::Executors;
use ck_exec::claude::ClaudeExecutor;
use ck_server::store::cooldown_store::Backend;
use ck_server::testing::SchedulerClock;
use serde_json::json;

const FILE: &str = "claude-q.json";
/// The smallest allowed bound (Go's 10 s floor), advanced by the fixture clock.
const BOUND: Duration = Duration::from_secs(10);
const SIX_DAYS: Duration = Duration::from_secs(6 * 24 * 3600);

/// The scripted upstream. The first request is refused with a shared weekly window
/// resetting in six days; the second (the probe) succeeds once the test releases it, as
/// one JSON reply or, for a streaming request, a stream whose first event comes at once;
/// every later request succeeds at once (the provider reset early).
struct Up {
    calls: AtomicUsize,
    release: tokio::sync::Semaphore,
}

impl Default for Up {
    fn default() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            release: tokio::sync::Semaphore::new(0),
        }
    }
}

const REPLY: &str = r#"{"id":"msg_1","type":"message","role":"assistant","content":[],"model":"m","stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}"#;
const STREAM_START: &str = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"m\",\"stop_reason\":null,\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n";
const STREAM_END: &str = "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";

async fn upstream(State(up): State<Arc<Up>>, req: axum::extract::Request) -> Response {
    let body = axum::body::to_bytes(req.into_body(), usize::MAX).await.unwrap();
    let stream = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["stream"] == true;
    match up.calls.fetch_add(1, Ordering::SeqCst) {
        0 => {
            let reset = SystemTime::now() + SIX_DAYS;
            let reset = reset.duration_since(UNIX_EPOCH).unwrap().as_secs().to_string();
            (
                StatusCode::TOO_MANY_REQUESTS,
                [
                    ("content-type", "application/json"),
                    ("anthropic-ratelimit-unified-7d-status", "rejected"),
                    ("anthropic-ratelimit-unified-7d-reset", reset.as_str()),
                ],
                r#"{"type":"error","error":{"type":"rate_limit_error","message":"weekly limit"}}"#,
            )
                .into_response()
        }
        1 if stream => {
            use futures_util::StreamExt;
            let up = up.clone();
            let rest = futures_util::stream::once(async move {
                up.release.acquire().await.unwrap().forget();
                Ok::<_, std::convert::Infallible>(axum::body::Bytes::from(STREAM_END))
            });
            let chunks = futures_util::stream::once(async { Ok(axum::body::Bytes::from(STREAM_START)) }).chain(rest);
            (
                [("content-type", "text/event-stream")],
                axum::body::Body::from_stream(chunks),
            )
                .into_response()
        }
        1 => {
            up.release.acquire().await.unwrap().forget();
            ([("content-type", "application/json")], REPLY).into_response()
        }
        _ => ([("content-type", "application/json")], REPLY).into_response(),
    }
}

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    url
}

/// A proxy over the private `auth_dir` with one API-key credential at `upstream_url`.
async fn proxy(
    auth_dir: &Path,
    upstream_url: &str,
    save: bool,
    clock: Arc<SchedulerClock>,
) -> (String, Arc<ck_server::Runtime>) {
    proxy_with(
        auth_dir,
        upstream_url,
        save,
        &format!("{}s", BOUND.as_secs()),
        Vec::new(),
        clock,
    )
    .await
}

/// `cooldowns` are stored in the private database before the runtime starts.
async fn proxy_with(
    dir: &Path,
    upstream_url: &str,
    save: bool,
    bound: &str,
    cooldowns: Vec<ck_server::store::cooldown_store::Record>,
    clock: Arc<SchedulerClock>,
) -> (String, Arc<ck_server::Runtime>) {
    let yaml = format!(
        "access:\n\
         \x20 api-keys: [client-key]\n\
         routing:\n\
         \x20 cooldown:\n\
         \x20   save-cooldown-status: {save}\n\
         \x20   max-trusted-cooldown: \"{bound}\"\n",
    );
    let store = Arc::new(ck_server::testing::config_store(&dir.join("cooker.db")).unwrap());
    let db = ck_server::store::credential_store::CredentialDb::initialize(store).unwrap();
    let meta = json!({"type": "claude", "api_key": "sk-fake-claude", "base_url": upstream_url});
    let path = db.auth_dir().join(FILE);
    db.save(&path, meta.to_string().as_bytes()).unwrap();
    Backend::save(&*db, cooldowns, clock.wall()).unwrap();
    let mut credential = Credential::from_file(&db.auth_dir(), &path, meta.as_object().unwrap().clone()).unwrap();
    credential.attributes.insert("api_key".into(), "sk-fake-claude".into());
    credential.attributes.insert("base_url".into(), upstream_url.into());
    let mut config = Config::parse(&yaml).unwrap();
    config.auth_dir = db.auth_dir();
    let rt = Arc::new(ck_server::testing::runtime_with_db_clock(
        config,
        vec![credential],
        Executors {
            claude: ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
            codex: Default::default(),
            devices: Default::default(),
            openai: Default::default(),
            google: Default::default(),
        },
        db,
        Some(clock),
    ));
    let app = ck_server::app(rt.clone(), axum::Router::new());
    (serve(app).await, rt)
}

async fn message(url: &str) -> u16 {
    let body = json!({"model": "claude-opus-4-6", "max_tokens": 8, "messages": [{"role": "user", "content": "hi"}]});
    wreq::Client::new()
        .post(format!("{url}/v1/messages"))
        .header("x-api-key", "client-key")
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap()
        .status()
        .as_u16()
}

fn auth_dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("quota-trust-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn early_reset_is_noticed_after_the_bound(save: bool) {
    let dir = auth_dir(if save { "saved" } else { "memory" });
    let clock = Arc::new(SchedulerClock::default());
    let up = Arc::new(Up::default());
    let upstream_url = serve(axum::Router::new().fallback(upstream).with_state(up.clone())).await;
    let (url, rt) = proxy(&dir, &upstream_url, save, clock.clone()).await;

    let started = SystemTime::now();
    assert_eq!(message(&url).await, 429);
    let elapsed = started.elapsed().unwrap();
    let cooling = rt.store().cooldowns(FILE);
    let window = cooling
        .iter()
        .map(|c| c.remaining)
        .max()
        .expect("the 429 cools the credential");
    // The six-day header was read: the window is the bound (not Go's 1 s backoff for a
    // 429 without a reset), and the stated reset is kept.
    assert_eq!(window, BOUND);
    let stated = cooling
        .iter()
        .filter_map(|c| c.recover_in)
        .max()
        .expect("stated reset kept");
    // The absolute reset is rounded to seconds and parsed during the request;
    // executor jitter adds at most 30 s. Host scheduling is not a quota tolerance.
    assert!(
        stated >= SIX_DAYS.saturating_sub(elapsed + Duration::from_secs(1)),
        "{stated:?}"
    );
    assert!(stated <= SIX_DAYS + Duration::from_secs(30), "{stated:?}");
    assert_eq!(message(&url).await, 429, "still cooling inside the bound");
    assert_eq!(
        up.calls.load(Ordering::SeqCst),
        1,
        "a cooling credential is not sent upstream"
    );

    let (url, rt) = if save {
        rt.finish_shutdown().await.unwrap();
        let backend = rt.store().credential_backend();
        let saved = Backend::load(backend.as_ref()).unwrap();
        assert!(!saved.is_empty(), "save-cooldown-status writes SQLite");
        let next = saved[0].next_retry_after.unwrap();
        let left = next.duration_since(clock.wall()).unwrap();
        assert_eq!(left, BOUND, "SQLite holds the bound, not the six days");
        let recover = saved[0].quota.next_recover_at.unwrap();
        assert_eq!(recover, clock.wall() + stated, "the stated reset survives persistence");
        assert_eq!(saved[0].quota.trust_windows, Some(1));
        // A restart restores the bounded cooldown, neither escalated nor six days.
        let config = (*rt.config()).clone();
        let credentials = rt.store().snapshot().iter().map(|c| (**c).clone()).collect();
        drop(rt);
        let restarted = Arc::new(ck_server::testing::runtime_with_db_clock(
            config,
            credentials,
            Executors {
                claude: ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
                codex: Default::default(),
                devices: Default::default(),
                openai: Default::default(),
                google: Default::default(),
            },
            backend,
            Some(clock.clone()),
        ));
        let url = serve(ck_server::app(restarted.clone(), axum::Router::new())).await;
        let restored = restarted.store().cooldowns(FILE);
        let left = restored.iter().map(|c| c.remaining).max().expect("restored");
        assert_eq!(left, BOUND, "restored from SQLite");
        (url, restarted)
    } else {
        assert!(
            Backend::load(rt.store().credential_backend().as_ref())
                .unwrap()
                .is_empty()
        );
        (url, rt)
    };

    clock.advance(BOUND);
    // Four requests arrive together: the first pick is the probe, the others see the
    // credential cooling until it answers. The probe's reply is held until the other
    // three have their answers, so the check does not depend on timing.
    use futures_util::StreamExt;
    let mut pending: futures_util::stream::FuturesUnordered<_> = (0..4).map(|_| message(&url)).collect();
    let mut statuses = Vec::new();
    for _ in 0..3 {
        let status = tokio::time::timeout(Duration::from_secs(20), pending.next())
            .await
            .expect("three requests answer while the probe is held (a second probe would wait too)")
            .unwrap();
        statuses.push(status);
    }
    assert!(statuses.iter().all(|s| *s != 200), "{statuses:?}");
    up.release.add_permits(1);
    assert_eq!(pending.next().await, Some(200), "the probe succeeds");
    assert_eq!(
        up.calls.load(Ordering::SeqCst),
        2,
        "exactly one probe reached the upstream: {statuses:?}"
    );
    assert_eq!(message(&url).await, 200, "the account is back after the probe");
    rt.finish_shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn early_reset_is_noticed_after_the_bound_in_memory() {
    early_reset_is_noticed_after_the_bound(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn early_reset_is_noticed_after_the_bound_with_save_cooldown_status() {
    early_reset_is_noticed_after_the_bound(true).await;
}

/// A stored bounded cooldown in its third window (count 2, 4 h) restores as it is:
/// 4 h left, the six-day stated reset kept.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_saved_trust_count_restores_its_window() {
    use ck_server::store::cooldown_store::{Quota, Record};
    let dir = auth_dir("count");
    let clock = Arc::new(SchedulerClock::default());
    let at = clock.wall() + SIX_DAYS;
    let record = Record {
        provider: "claude".into(),
        auth_id: FILE.into(),
        status: "cooling".into(),
        // The third bounded window: probe in 4 h, stated reset in six days.
        next_retry_after: Some(clock.wall() + Duration::from_secs(4 * 3600)),
        reason: "credential_quota".into(),
        quota: Quota {
            exceeded: true,
            reason: "credential_quota".into(),
            next_recover_at: Some(at),
            trust_windows: Some(3),
            ..Default::default()
        },
        ..Default::default()
    };
    let (_url, rt) = proxy_with(&dir, "http://127.0.0.1:9", true, "", vec![record], clock).await;
    let restored = rt.store().cooldowns(FILE);
    let left = restored.iter().map(|c| c.remaining).max().expect("restored");
    let four_hours = Duration::from_secs(4 * 3600);
    assert_eq!(left, four_hours);
    let stated = restored
        .iter()
        .filter_map(|c| c.recover_in)
        .max()
        .expect("stated reset kept");
    assert_eq!(stated, SIX_DAYS);
    // The restored state is rewritten into SQLite with its trust count intact.
    rt.finish_shutdown().await.unwrap();
    let rewritten = Backend::load(rt.store().credential_backend().as_ref()).unwrap();
    assert_eq!(rewritten.len(), 1);
    assert_eq!(rewritten[0].quota.trust_windows, Some(3));
    let _ = std::fs::remove_dir_all(&dir);
}

/// A streaming probe answers the window when its first event arrives: while the stream
/// is still open, the next request goes upstream and succeeds instead of seeing the
/// account cooling until the stream ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_open_probe_stream_frees_the_account() {
    use futures_util::StreamExt;
    let dir = auth_dir("stream");
    let clock = Arc::new(SchedulerClock::default());
    let up = Arc::new(Up::default());
    let upstream_url = serve(axum::Router::new().fallback(upstream).with_state(up.clone())).await;
    let (url, rt) = proxy(&dir, &upstream_url, false, clock.clone()).await;
    assert_eq!(message(&url).await, 429);
    clock.advance(BOUND);
    let body = json!({"model": "claude-opus-4-6", "max_tokens": 8, "stream": true, "messages": [{"role": "user", "content": "hi"}]});
    let probe = wreq::Client::new()
        .post(format!("{url}/v1/messages"))
        .header("x-api-key", "client-key")
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(probe.status().as_u16(), 200);
    let mut chunks = probe.bytes_stream();
    let first = tokio::time::timeout(Duration::from_secs(10), chunks.next())
        .await
        .expect("the probe's first event")
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&first).contains("message_start"));
    assert_eq!(message(&url).await, 200, "served while the probe stream is open");
    assert_eq!(up.calls.load(Ordering::SeqCst), 3);
    up.release.add_permits(1);
    while let Some(chunk) = chunks.next().await {
        chunk.unwrap();
    }
    rt.finish_shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
