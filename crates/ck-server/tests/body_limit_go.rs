//! Request body limits through the binary's middleware stack in-process.
//! Success and management answers retain their recordings; inference errors follow F13/F14.
//! Plugin routes are gone with the plugin host; their first-JSON-value vectors replay on
//! `/v0/management/reset-quota` (see `reference/body_limit/README.md`). admin_contract
//! covers admin bodies.
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use axum::body::{Body, Bytes, HttpBody};
use base64::Engine as _;
use ck_exec::Executors;
use ck_exec::claude::ClaudeExecutor;
use ck_server::management::api::{self as management, Management};
use ck_server::store::config_store::ConfigStore;
use http_body::{Frame, SizeHint};
use serde_json::Value;
use tower::ServiceExt as _;

/// The client sends this many bytes per frame.
const FRAME: usize = 16;

/// A request body that counts what the server pulled from it.
struct Counted {
    rest: Bytes,
    read: Arc<AtomicU64>,
    declared: bool,
}

impl HttpBody for Counted {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        if self.rest.is_empty() {
            return Poll::Ready(None);
        }
        let n = self.rest.len().min(FRAME);
        let chunk = self.rest.split_to(n);
        self.read.fetch_add(n as u64, Ordering::Relaxed);
        Poll::Ready(Some(Ok(Frame::data(chunk))))
    }

    fn is_end_stream(&self) -> bool {
        self.rest.is_empty()
    }

    fn size_hint(&self) -> SizeHint {
        if self.declared {
            SizeHint::with_exact(self.rest.len() as u64)
        } else {
            SizeHint::default()
        }
    }
}

fn app(dir: &std::path::Path, yaml: &str) -> (axum::Router, Arc<ck_server::Runtime>, Arc<ConfigStore>) {
    app_to(dir, yaml, "http://127.0.0.1:1")
}

/// The Claude executor and any `api-keys.claude` entries in `yaml` talk to `upstream`.
fn app_to(
    dir: &std::path::Path,
    yaml: &str,
    upstream: &str,
) -> (axum::Router, Arc<ck_server::Runtime>, Arc<ConfigStore>) {
    std::fs::create_dir_all(dir.join("auth")).unwrap();
    let store = Arc::new(ConfigStore::memory_with(yaml));
    let config = store.load().unwrap().config;
    let credentials = ck_core::config::credentials::load(&config);
    let rt = Arc::new(ck_server::testing::runtime(
        config,
        credentials,
        Executors {
            claude: ClaudeExecutor::new(upstream).unwrap(),
            codex: Default::default(),
            devices: Default::default(),
            openai: Default::default(),
            google: Default::default(),
        },
    ));
    let state = Management::with_options(rt.clone(), store.clone(), management::Options::default());
    // crates/cooker/src/main.rs, without Home, observability and the heap trimmer.
    let app = ck_server::app(rt.clone(), management::router(state.clone()));
    let app = ck_server::net::safe_mode::router(&rt, app);
    let app = app.layer(axum::middleware::from_fn(management::cors));
    let app = ck_server::observability::request_logging::router(&state, app);
    (ck_server::http::body_limit::layer(&rt, app), rt, store)
}

/// Go answers a failed `/openai/v1/videos` request with a random video ID.
fn normalize(body: &str) -> String {
    match serde_json::from_str::<Value>(body) {
        Ok(mut v) if v["object"] == "video" => {
            v["id"] = "video_<id>".into();
            v.to_string()
        }
        Ok(v) => v.to_string(),
        Err(_) => body.to_owned(),
    }
}

fn assert_inference_error(path: &str, status: u16, body: &str, request_id: Option<&axum::http::HeaderValue>) {
    let value: Value = serde_json::from_str(body).expect("inference error is JSON");
    let error = value["error"].as_object().expect("error is an object");
    assert!(error["message"].as_str().is_some_and(|s| !s.is_empty()), "{body}");
    if path.starts_with("/v1/messages") {
        assert_eq!(value["type"], "error");
        let kind = match status {
            401 => "authentication_error",
            413 => "request_too_large",
            _ => "invalid_request_error",
        };
        assert_eq!(error["type"], kind);
        let id = value["request_id"].as_str().expect("local request_id");
        assert!(!id.is_empty());
        assert_eq!(Some(id), request_id.map(|v| v.to_str().unwrap()));
    } else if path.starts_with("/v1beta/") {
        assert_eq!(error["code"], status);
        let kind = match status {
            401 => "UNAUTHENTICATED",
            413 => "RESOURCE_EXHAUSTED",
            _ => "INVALID_ARGUMENT",
        };
        assert_eq!(error["status"], kind);
    } else {
        let kind = if status == 401 {
            "authentication_error"
        } else {
            "invalid_request_error"
        };
        assert_eq!(error["type"], kind, "{body}");
        assert_eq!(error.get("param"), Some(&Value::Null));
        let code = error.get("code").expect("nullable code is required");
        assert!(code.is_null() || code.is_string(), "{body}");
    }
}

fn uses_inference_error(case: &Value) -> bool {
    let path = case["path"].as_str().unwrap();
    case["status"].as_u64().unwrap() >= 400
        && (path.starts_with("/v1/")
            || path.starts_with("/v1beta/")
            || path.starts_with("/backend-api/codex/")
            || (path.starts_with("/openai/v1/videos")
                && serde_json::from_str::<Value>(case["response"].as_str().unwrap()).unwrap()["object"] != "video"))
        && !path.starts_with("/v1/realtime/")
        && path != "/v1/live"
}

#[tokio::test]
async fn body_limit_answers_match_go() {
    let fixture: Vec<Value> = serde_json::from_str(include_str!("fixtures/body_limit_go.json")).unwrap();
    let mut failures = Vec::new();
    for (i, case) in fixture.iter().enumerate() {
        let name = case["name"].as_str().unwrap();
        let dir = std::env::temp_dir().join(format!("cooker-body-limit-{}-{i}", std::process::id()));
        let (app, _, _) = app(&dir, case["yaml"].as_str().unwrap());
        let mut raw = base64::engine::general_purpose::STANDARD
            .decode(case["body_b64"].as_str().unwrap())
            .unwrap();
        if let Some(pad) = case["pad_to"].as_u64() {
            raw.resize(pad as usize, b'p');
        }
        let read = Arc::new(AtomicU64::new(0));
        let chunked = case["chunked"].as_bool().unwrap();
        let mut request = axum::http::Request::builder()
            .method(case["method"].as_str().unwrap())
            .uri(case["path"].as_str().unwrap());
        if !chunked {
            request = request.header("content-length", raw.len());
        }
        for (k, v) in case["headers"].as_object().into_iter().flatten() {
            request = request.header(k, v.as_str().unwrap());
        }
        let len = raw.len() as u64;
        let mut request = request
            .body(Body::new(Counted {
                rest: Bytes::from(raw),
                read: read.clone(),
                declared: !chunked,
            }))
            .unwrap();
        let peer: SocketAddr = "192.0.2.1:1234".parse().unwrap();
        request.extensions_mut().insert(axum::extract::ConnectInfo(peer));
        let response = app.oneshot(request).await.unwrap();
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get("content-type")
            .map(|v| v.to_str().unwrap().to_owned())
            .unwrap_or_default();
        let request_id = response.headers().get("request-id").cloned();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8_lossy(&body).into_owned();
        let got = (status, content_type, normalize(&body));
        let want = (
            case["status"].as_u64().unwrap() as u16,
            case["content_type"].as_str().unwrap().to_owned(),
            normalize(case["response"].as_str().unwrap()),
        );
        if uses_inference_error(case) {
            assert_eq!(status, want.0, "{name}: {body}");
            assert!(got.1.starts_with("application/json"), "{name}: {}", got.1);
            assert_inference_error(case["path"].as_str().unwrap(), status, &body, request_id.as_ref());
        } else if got != want {
            failures.push(format!("{name}: got {got:?}, want {want:?}"));
        }
        // Go reads at most the limit plus one byte; frames arrive whole here.
        let (go_read, rs_read) = (case["read"].as_u64().unwrap(), read.load(Ordering::Relaxed));
        let bounded = if go_read == 0 {
            rs_read == 0
        } else if go_read < len {
            rs_read < go_read + FRAME as u64
        } else {
            true
        };
        if !bounded {
            failures.push(format!("{name}: read {rs_read} of {len} bytes, Go read {go_read}"));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
    assert!(
        failures.is_empty(),
        "{} of {} cases differ:\n{}",
        failures.len(),
        fixture.len(),
        failures.join("\n")
    );
}

/// Go reads the limit from the current config on every request.
#[tokio::test]
async fn a_published_limit_applies_to_the_next_request() {
    let dir = std::env::temp_dir().join(format!("cooker-body-limit-reload-{}", std::process::id()));
    let yaml = "config-version: 8\naccess:\n  api-keys:\n    - test-key\n";
    let (app, _, store) = app(&dir, yaml);
    let send = |app: axum::Router| async move {
        let request = axum::http::Request::post("/v1/responses")
            .header("authorization", "Bearer test-key")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"model":"gpt-5","input":"ok"}"#))
            .unwrap();
        app.oneshot(request).await.unwrap().status().as_u16()
    };
    assert_eq!(send(app.clone()).await, 400, "no limit: the model lookup answers");
    let text = format!("{yaml}server:\n  max-request-body-bytes: 29\n");
    store.import_yaml(&text, "test", "reload body limit").unwrap();
    assert_eq!(send(app).await, 413);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Go stores the middleware's limit on the request: a reload while the body is still
/// arriving does not change the limit its decoding applies.
#[tokio::test]
async fn a_reload_during_a_request_keeps_its_limit() {
    use futures_util::StreamExt as _;
    let dir = std::env::temp_dir().join(format!("cooker-body-limit-mid-{}", std::process::id()));
    let yaml = "config-version: 8\nserver:\n  max-request-body-bytes: 128\naccess:\n  api-keys:\n    - test-key\n";
    let (app, rt, store) = app(&dir, yaml);
    let decoded = format!(r#"{{"model":"gpt-5","input":"{}"}}"#, "z".repeat(512));
    let compressed = zstd::encode_all(decoded.as_bytes(), 3).unwrap();
    assert!(compressed.len() < 128);
    let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(1);
    let stream = futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|chunk| (Ok::<_, std::io::Error>(chunk), rx))
    });
    let request = axum::http::Request::post("/v1/responses")
        .header("authorization", "Bearer test-key")
        .header("content-type", "application/json")
        .header("content-encoding", "zstd")
        .body(Body::from_stream(stream.boxed()))
        .unwrap();
    let response = tokio::spawn(app.oneshot(request));
    let (head, tail) = compressed.split_at(compressed.len() / 2);
    tx.send(Bytes::copy_from_slice(head)).await.unwrap();
    // The handler has taken the first half, so the middleware has run.
    while tx.capacity() < tx.max_capacity() {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    let text = yaml.replace("max-request-body-bytes: 128", "max-request-body-bytes: 0");
    store.import_yaml(&text, "test", "reload body limit").unwrap();
    assert_eq!(rt.config().max_request_body_bytes(), None);
    tx.send(Bytes::copy_from_slice(tail)).await.unwrap();
    drop(tx);
    let response = response.await.unwrap().unwrap();
    let status = response.status().as_u16();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(status, 413);
    assert_inference_error("/v1/responses", status, &String::from_utf8_lossy(&body), None);
    let _ = std::fs::remove_dir_all(&dir);
}

const ANTHROPIC_PATHS: [&str; 2] = ["/v1/messages", "/v1/messages/count_tokens"];
const KEYED: &str = "config-version: 8\naccess:\n  api-keys:\n    - test-key\n";

async fn answer(
    app: &axum::Router,
    path: &str,
    key: bool,
    body: Body,
) -> (u16, Option<axum::http::HeaderValue>, String) {
    let mut request = axum::http::Request::post(path).header("content-type", "application/json");
    if key {
        request = request.header("authorization", "Bearer test-key");
    }
    let response = app.clone().oneshot(request.body(body).unwrap()).await.unwrap();
    let status = response.status().as_u16();
    let request_id = response.headers().get("request-id").cloned();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, request_id, String::from_utf8_lossy(&body).into_owned())
}

/// Authentication, body-read and routing failures answer in the client's protocol.
#[tokio::test]
async fn local_failures_use_the_clients_error_shape() {
    let dir = std::env::temp_dir().join(format!("cooker-local-errors-{}", std::process::id()));
    let (app, _, _) = app(&dir, KEYED);
    let broken = || {
        Body::from_stream(futures_util::stream::once(async {
            Err::<Bytes, _>(std::io::Error::other("broken body"))
        }))
    };
    for path in [
        "/v1/messages",
        "/v1/messages/count_tokens",
        "/v1/chat/completions",
        "/v1beta/models/gemini-2.5-flash:generateContent",
        "/v1beta/interactions",
    ] {
        // (case, key, body, status)
        let rows: [(&str, bool, Body, u16); 4] = [
            ("no key", false, Body::empty(), 401),
            ("unreadable body", true, broken(), 400),
            ("invalid JSON", true, Body::from("{"), 400),
            (
                "unknown model",
                true,
                Body::from(r#"{"model":"no-such-model-xyz","messages":[],"input":"hi","contents":[]}"#),
                400,
            ),
        ];
        for (case, key, body, want) in rows {
            let (status, request_id, text) = answer(&app, path, key, body).await;
            assert_eq!(status, want, "{path} {case}: {text}");
            assert_inference_error(path, status, &text, request_id.as_ref());
            if case == "unknown model" && !path.starts_with("/v1beta/models/") {
                assert!(
                    text.contains("unknown provider for model no-such-model-xyz"),
                    "{path}: {text}"
                );
            }
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// Anthropic Messages and token counting admit up to 128 MB unless a smaller limit is
/// set; one buffer serves every row.
#[tokio::test]
async fn anthropic_body_ceiling_is_128_mb_unless_configured_lower() {
    const MAX: usize = 128_000_000;
    let request = r#"{"model":"claude-sonnet-4-6","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}"#;
    let mut buffer = request.as_bytes().to_vec();
    buffer.resize(MAX + 1, b' ');
    let buffer = Bytes::from(buffer);
    let dir = std::env::temp_dir().join(format!("cooker-anthropic-ceiling-{}", std::process::id()));
    // (server settings, size, rejected)
    let rows = [
        ("", MAX + 1, true),
        ("", MAX, false),
        ("server:\n  max-request-body-bytes: 256000000\n", MAX + 1, true),
        ("server:\n  max-request-body-bytes: 200\n", 201, true),
        ("server:\n  max-request-body-bytes: 200\n", 200, false),
    ];
    for (settings, size, rejected) in rows {
        let (app, _, _) = app(&dir, &format!("{KEYED}{settings}"));
        for path in ANTHROPIC_PATHS {
            for chunked in [false, true] {
                let bytes = buffer.slice(..size);
                let body = if chunked {
                    Body::from_stream(futures_util::stream::once(
                        async move { Ok::<_, std::io::Error>(bytes) },
                    ))
                } else {
                    Body::from(bytes)
                };
                let (status, request_id, text) = answer(&app, path, true, body).await;
                let case = format!("{settings:?} {path} {size} chunked={chunked}");
                assert_eq!(status == 413, rejected, "{case}: {status} {text}");
                if rejected {
                    assert_inference_error(path, status, &text, request_id.as_ref());
                    assert!(text.contains("request_too_large"), "{case}: {text}");
                }
            }
        }
    }
    // An admitted request must also get through the executor: a read or limit failure after
    // admission would still pass the status check above.
    let seen = Arc::new(AtomicU64::new(0));
    let counter = seen.clone();
    let upstream = axum::Router::new().fallback(move |req: axum::extract::Request| {
        let counter = counter.clone();
        async move {
            let body = axum::body::to_bytes(req.into_body(), usize::MAX).await.unwrap();
            assert!(serde_json::from_slice::<Value>(&body).is_ok_and(|v| v["model"] == "claude-sonnet-4-6"));
            counter.fetch_add(1, Ordering::Relaxed);
            (
                [("content-type", "application/json")],
                r#"{"id":"msg_test","type":"message","role":"assistant","model":"claude-sonnet-4-6","content":[],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":1}}"#,
            )
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });
    let yaml = format!(
        "{KEYED}api-keys:\n  claude:\n    - name: local\n      base-url: {url}\n      keys: [{{api-key: sk-fake}}]\n"
    );
    let (app, _, _) = app_to(&dir, &yaml, &url);
    let (status, _, text) = answer(&app, "/v1/messages", true, Body::from(buffer.slice(..MAX))).await;
    assert_eq!(status, 200, "{text}");
    assert_eq!(
        seen.load(Ordering::Relaxed),
        1,
        "the admitted request reached the upstream once"
    );
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
