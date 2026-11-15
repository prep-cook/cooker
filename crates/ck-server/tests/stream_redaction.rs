//! The stream identity and resume cursors never reach a URL, a request log or a spool file
//! (`docs/INTERNAL-STREAM.md` §6.7).

mod stream_fixture;

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use ck_server::http::egress::Framing;
use ck_server::inference::streams::StreamId;
use stream_fixture::*;
use tower::ServiceExt;

/// Every file under `directory`, recursively, with its contents.
fn files(directory: &std::path::Path) -> Vec<(std::path::PathBuf, String)> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(directory).unwrap().flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(files(&path));
        } else if let Ok(bytes) = std::fs::read(&path) {
            out.push((path, String::from_utf8_lossy(&bytes).into_owned()));
        }
    }
    out
}

/// Sends a captured request carrying the resume headers and returns the log written for it.
/// With request logging off, the failed request still gets a forced error log, and its body
/// over 1 MiB goes through the deferred request-body spool first.
async fn captured(request_log: bool, id: &str) -> (Fixture, String) {
    let fixture = Fixture::with_config(
        &format!("observability: {{logs: {{request-log: {request_log}}}}}\n"),
        |_| {},
    );
    let logs = fixture.directory().join("logs");
    std::fs::create_dir(&logs).unwrap();
    let management = ck_server::management::api::Management::with_options(
        fixture.rt.clone(),
        Arc::new(ck_server::store::config_store::ConfigStore::memory_with(
            &ck_core::config::yaml_string(&fixture.rt.config().document).unwrap(),
        )),
        ck_server::management::api::Options {
            log_dir: Some(logs.clone()),
            management_password: Some(String::new()),
            ..Default::default()
        },
    );
    let app = ck_server::observability::request_logging::router(&management, fixture.app());
    let large = format!(
        r#"{{"model":"m","messages":[{{"role":"user","content":"{}"}}]}}"#,
        "x".repeat(2 << 20)
    );
    let request = Request::post("/v1/chat/completions")
        .header("authorization", format!("Bearer {KEY}"))
        .header("x-ck-stream-id", id)
        .header("Last-Event-ID", "v1.e.7")
        .body(Body::from(large))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert!(!response.status().is_success(), "no credential serves the request");
    let _ = body(response).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let logged: Vec<_> = files(&logs)
            .into_iter()
            .filter(|(path, _)| path.extension().is_some_and(|e| e == "log"))
            .collect();
        if let [(_, content)] = logged.as_slice()
            && content.contains("=== RESPONSE ===")
        {
            let content = content.clone();
            return (fixture, content);
        }
        assert!(tokio::time::Instant::now() < deadline, "no log in {logs:?}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn resume_headers_are_redacted_in_request_and_error_logs() {
    for request_log in [true, false] {
        let id = StreamId::random().header_value().to_str().unwrap().to_owned();
        let (fixture, log) = captured(request_log, &id).await;
        for name in ["x-ck-stream-id:", "last-event-id:"] {
            let line = log
                .lines()
                .find(|line| line.to_ascii_lowercase().starts_with(name))
                .unwrap_or_else(|| panic!("no {name} line in:\n{log}"));
            assert!(line.ends_with(": <redacted>"), "{line}");
        }
        // Nothing left on disk, spooled or logged, holds the identity or the cursor.
        for (path, content) in files(fixture.directory()) {
            for secret in [id.as_str(), &id.to_ascii_uppercase(), "v1.e.7"] {
                assert!(!content.contains(secret), "{secret:?} reached {path:?}");
            }
        }
    }
}

#[tokio::test]
async fn the_identity_travels_only_in_headers() {
    let fixture = Fixture::new(|_| {});
    let app = fixture.app();
    let mut live = commit(&fixture, owner(KEY), Framing::Sse, STREAMED);
    answer(&mut live);
    live.stopped().await;
    let id = live.header();

    let response = resume(&app, &live, "v1.e.0").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-ck-stream-id"], id.as_str());
    let (text, clean) = body(response).await;
    assert!(clean);
    assert!(!text.contains(&id), "the body carries cursors only");
    // A query parameter cannot supply the identity.
    let (status, _) = status_body(get(&app, KEY, &format!("?after=v1.e.0&id={id}"), &[]).await).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = status_body(get(&app, KEY, &format!("?after=v1.e.0&x-ck-stream-id={id}"), &[]).await).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[derive(Clone, Default)]
struct Capture(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn access_logs_carry_no_cursor_or_identity() {
    use tracing::instrument::WithSubscriber;
    let fixture = Fixture::new(|_| {});
    let mut live = commit(&fixture, owner(KEY), Framing::Sse, STREAMED);
    answer(&mut live);
    live.stopped().await;
    let app = ck_server::observability::api::router(&fixture.rt, fixture.app());
    let capture = Capture::default();
    let writer = capture.clone();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_max_level(tracing::Level::TRACE)
        .with_writer(move || writer.clone())
        .finish();
    let unknown = StreamId::random().header_value().to_str().unwrap().to_owned();
    async {
        // A delivered resume and a 410, each with its cursor in the query and the header.
        for id in [live.header(), unknown.clone()] {
            let response = get(
                &app,
                KEY,
                "?after=v1.e.123456789",
                &[("x-ck-stream-id", &id), ("last-event-id", "v1.e.987654321")],
            )
            .await;
            let _ = body(response).await;
        }
        let (status, _) = status_body(resume(&app, &live, "v1.e.2").await).await;
        assert_eq!(status, StatusCode::OK);
    }
    .with_subscriber(subscriber)
    .await;
    let log = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
    assert!(log.contains("/v1/streams?after=***"), "{log}");
    for secret in ["123456789", "987654321", "v1.e.2", &live.header(), &unknown] {
        assert!(!log.contains(secret), "{secret:?} reached the access log:\n{log}");
    }
}
