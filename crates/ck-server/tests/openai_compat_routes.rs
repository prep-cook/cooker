//! OpenAI-compatible providers through the real router, runtime, registry overlay and
//! executor, against a local mock upstream. The executor's byte-level behaviour is
//! checked against Go in ck-exec (openai_compat_tests.rs); this proves the wiring:
//! config credentials, alias routing, client-key isolation and both response modes.

use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::{Request, State};
use axum::response::{IntoResponse, Response};
use ck_core::config::Config;
use ck_exec::Executors;
use ck_exec::claude::ClaudeExecutor;
use ck_server::router;

fn assert_local_error(text: &str, kind: &str) {
    let value: serde_json::Value = serde_json::from_str(text).unwrap();
    assert_eq!(value["error"]["type"], kind);
    assert!(value["error"]["message"].as_str().is_some_and(|s| !s.is_empty()));
    assert_eq!(value["error"].get("param"), Some(&serde_json::Value::Null));
    assert_eq!(value["error"].get("code"), Some(&serde_json::Value::Null));
}

/// Requests seen, the compact answers by arrival order (`(status, body)`; the first key to
/// send a compact is entry 0, the next entry 1, and a key without an entry answers
/// `{"ok":true}`; status 0 resets the connection), the SSE body to stream (`None` streams
/// `CHUNK` and `[DONE]`), and the keys in the order their first compact arrived.
#[derive(Default)]
struct Seen(
    Mutex<Vec<(String, Option<String>, String)>>,
    Mutex<Vec<(u16, &'static str)>>,
    Mutex<Option<String>>,
    Mutex<Vec<String>>,
);

const CHAT: &str = r#"{"id":"c1","object":"chat.completion","created":1,"model":"up-model","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}"#;
const CHUNK: &str = r#"{"id":"c1","object":"chat.completion.chunk","created":1,"model":"up-model","choices":[{"index":0,"delta":{"content":"ok"},"finish_reason":"stop"}]}"#;

async fn upstream(State(seen): State<Arc<Seen>>, req: Request) -> Response {
    let path = req.uri().path().to_owned();
    let auth = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let body = axum::body::to_bytes(req.into_body(), usize::MAX).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    let stream = body.contains(r#""include_usage":true"#);
    seen.0.lock().unwrap().push((path.clone(), auth.clone(), body));
    if path.ends_with("/responses/compact") {
        let answer = {
            let key = auth.clone().unwrap_or_default();
            let mut roles = seen.3.lock().unwrap();
            let role = roles.iter().position(|k| *k == key).unwrap_or_else(|| {
                roles.push(key);
                roles.len() - 1
            });
            seen.1.lock().unwrap().get(role).copied()
        };
        return match answer {
            Some((0, _)) => {
                let reset =
                    futures_util::stream::iter([Err::<Bytes, _>(std::io::Error::other("mock connection reset"))]);
                (
                    [("content-type", "application/json")],
                    axum::body::Body::from_stream(reset),
                )
                    .into_response()
            }
            Some((status, body)) => (
                axum::http::StatusCode::from_u16(status).unwrap(),
                [("content-type", "application/json")],
                body,
            )
                .into_response(),
            None => ([("content-type", "application/json")], r#"{"ok":true}"#).into_response(),
        };
    }
    if stream {
        let sse = seen.2.lock().unwrap().clone();
        let sse = sse.unwrap_or_else(|| format!("data: {CHUNK}\n\ndata: [DONE]\n\n"));
        ([("content-type", "text/event-stream")], Bytes::from(sse)).into_response()
    } else {
        ([("content-type", "application/json")], CHAT).into_response()
    }
}

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    url
}

async fn proxy() -> (String, Arc<Seen>) {
    proxy_keys(&["sk-fake-upstream"]).await
}

async fn proxy_keys(keys: &[&str]) -> (String, Arc<Seen>) {
    proxy_model(keys, "up-model").await
}

async fn proxy_model(keys: &[&str], model: &str) -> (String, Arc<Seen>) {
    let keys: String = keys.iter().map(|k| format!("        - api-key: {k}\n")).collect();
    let seen = Arc::new(Seen::default());
    let upstream_url = serve(axum::Router::new().fallback(upstream).with_state(seen.clone())).await;
    // A private credential directory: `testing::runtime` stores its `.json` files in the test database.
    let auth_dir = std::env::temp_dir().join(format!("cooker-compat-auths-{}", std::process::id()));
    std::fs::create_dir_all(&auth_dir).unwrap();
    let config = Config::parse(&format!(
        "access:\n  api-keys: [client-key]\napi-keys:\n  openai-compatibility:\n    - name: Acme\n      base-url: {upstream_url}/v1\n      models:\n        - name: {model}\n          alias: fast\n      keys:\n{keys}"))
    .unwrap();
    let credentials = ck_core::config::credentials::load(&config);
    let executors = Executors {
        claude: ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
        codex: Default::default(),
        devices: Default::default(),
        openai: Default::default(),
        google: Default::default(),
    };
    let rt = Arc::new(ck_server::testing::runtime(config, credentials, executors));
    (serve(router(rt)).await, seen)
}

async fn post(url: &str, body: &str) -> (u16, String, String) {
    let res = wreq::Client::new()
        .post(format!("{url}/v1/chat/completions"))
        .header("authorization", "Bearer client-key")
        .header("content-type", "application/json")
        .body(body.to_owned())
        .send()
        .await
        .unwrap();
    let status = res.status().as_u16();
    let content_type = res
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    (status, content_type, res.text().await.unwrap())
}

#[tokio::test]
async fn alias_routes_to_configured_upstream_with_its_key() {
    for (model, effort, expected) in [
        ("up-model", None, None),
        ("glm-5.3", Some("max"), Some("max")),
        ("glm-5.2", Some("max"), Some("max")),
        ("glm-5.2", Some("xhigh"), Some("xhigh")),
        ("glm-5.2", Some("minimal"), Some("minimal")),
        ("glm-5.3(max)", Some("max"), Some("max")),
    ] {
        let (url, seen) = proxy_model(&["sk-fake-upstream"], model).await;
        let mut request = serde_json::json!({"model":"fast","messages":[{"role":"user","content":"hi"}]});
        if let Some(effort) = effort {
            request["reasoning_effort"] = effort.into();
        }
        let (status, content_type, text) = post(&url, &request.to_string()).await;
        assert_eq!(status, 200, "{model}: {text}");
        assert!(content_type.starts_with("application/json"), "{content_type}");
        let reply: serde_json::Value = serde_json::from_str(&text).unwrap();
        let want: serde_json::Value = serde_json::from_str(CHAT).unwrap();
        assert_eq!(reply, want, "OpenAI-to-OpenAI non-stream responses keep every member");
        let seen = seen.0.lock().unwrap().clone();
        assert_eq!(seen.len(), 1);
        let (path, auth, body) = &seen[0];
        assert_eq!(path, "/v1/chat/completions");
        assert_eq!(
            auth.as_deref(),
            Some("Bearer sk-fake-upstream"),
            "the client key never reaches the upstream"
        );
        let sent: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(sent["model"], ck_wire::thinking::split_suffix(model).0);
        assert_eq!(sent["messages"], request["messages"]);
        assert_eq!(sent["reasoning_effort"].as_str(), expected, "{model}");
    }
}

#[tokio::test]
async fn streaming_requests_usage_and_ends_with_done() {
    let (url, seen) = proxy().await;
    let (status, content_type, text) = post(
        &url,
        r#"{"model":"fast","stream":true,"messages":[{"role":"user","content":"hi"}]}"#,
    )
    .await;
    assert_eq!(status, 200, "{text}");
    assert!(content_type.starts_with("text/event-stream"), "{content_type}");
    assert!(text.contains(r#""content":"ok""#), "{text}");
    assert!(
        text.trim_end()
            .rsplit("\n\n")
            .next()
            .unwrap()
            .starts_with("data: [DONE]\n"),
        "{text}"
    );
    let body: serde_json::Value = serde_json::from_str(&seen.0.lock().unwrap()[0].2).unwrap();
    assert_eq!(
        body,
        serde_json::json!({"model":"up-model","stream":true,"messages":[{"role":"user","content":"hi"}],"stream_options":{"include_usage":true}})
    );
}

/// A Chat stream that closes cleanly before any chunk carried a `finish_reason` never ends
/// with `[DONE]` (Go 97dd9eec): it fails before commitment with 502, or ends with an error
/// frame once committed. The upstream streams are Go's (tests/reference/stream_truncation).
#[tokio::test]
async fn a_stream_without_a_finish_reason_ends_with_an_error() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!("fixtures/stream_truncation_go.json")).unwrap();
    let mut ran = 0;
    for case in fixture.as_array().unwrap() {
        // A failed attempt cools its credential, so every case gets its own proxy.
        let (url, seen) = proxy().await;
        let name = case["name"].as_str().unwrap();
        if case["path"] != "/v1/chat/completions" {
            continue;
        }
        let sse: String = case["chunks"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| format!("data: {}\n\n", c.as_str().unwrap()))
            .collect();
        // Go's handler wrote `[DONE]` unless the stream was truncated.
        let truncated = !case["out"].as_str().unwrap().contains("[DONE]");
        if !truncated {
            continue;
        }
        *seen.2.lock().unwrap() = Some(sse);
        let body = r#"{"model":"fast","messages":[{"role":"user","content":"hi"}],"stream":true}"#;
        let (status, text) = send(&url, "/v1/chat/completions", &[], body.into()).await;
        let last = text.trim_end().rsplit("\n\n").next().unwrap_or_default();
        assert!(!text.contains("[DONE]"), "{name}: {text}");
        assert!(
            status == 502 || last.starts_with(r#"data: {"error":"#),
            "{name}: {status} {text}"
        );
        ran += 1;
    }
    assert_eq!(ran, 3);
}

async fn send(url: &str, path: &str, headers: &[(&str, &str)], body: Vec<u8>) -> (u16, String) {
    let mut req = wreq::Client::new()
        .post(format!("{url}{path}"))
        .header("authorization", "Bearer client-key")
        .header("content-type", "application/json");
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    let res = req.body(body).send().await.unwrap();
    (res.status().as_u16(), res.text().await.unwrap())
}

fn compact_calls(seen: &Seen) -> usize {
    seen.0
        .lock()
        .unwrap()
        .iter()
        .filter(|r| r.0.ends_with("/responses/compact"))
        .count()
}

/// Go `TestOpenAIResponsesCompactDecodesZstdRequestBody` (openai_responses_compact_test.go)
/// and `handlers.ReadRequestBody`.
#[tokio::test]
async fn compact_decodes_zstd_request_body() {
    let (url, seen) = proxy().await;
    let compressed = zstd::stream::encode_all(&br#"{"model":"fast","input":"hello"}"#[..], 0).unwrap();
    let (status, text) = send(
        &url,
        "/v1/responses/compact",
        &[("content-encoding", "zstd")],
        compressed,
    )
    .await;
    assert_eq!(status, 200, "{text}");
    assert_eq!(text, r#"{"ok":true}"#);
    assert_eq!(compact_calls(&seen), 1);
    let body = seen.0.lock().unwrap()[0].2.clone();
    assert!(
        body.contains(r#""input":"hello""#) && body.contains(r#""model":"up-model""#),
        "{body}"
    );

    // ReadRequestBody: an undecodable body that is valid JSON is used as sent; one
    // that is not answers 400 with the decoder's reason.
    let (status, text) = send(
        &url,
        "/v1/chat/completions",
        &[("content-encoding", "gzip")],
        br#"{"model":"fast","messages":[{"role":"user","content":"hi"}]}"#.to_vec(),
    )
    .await;
    assert_eq!(status, 200, "{text}");
    let (status, text) = send(
        &url,
        "/v1/chat/completions",
        &[("content-encoding", "br")],
        b"\x1f\x8b".to_vec(),
    )
    .await;
    assert_eq!(status, 400);
    assert_local_error(&text, "invalid_request_error");
}

/// Go `TestOpenAIResponsesCompactRejectsStream`: 400 before any upstream call.
#[tokio::test]
async fn compact_rejects_stream() {
    let (url, seen) = proxy().await;
    let body = br#"{"model":"fast","stream":true}"#.to_vec();
    let (status, text) = send(&url, "/v1/responses/compact", &[], body).await;
    assert_eq!(status, 400, "{text}");
    assert_local_error(&text, "invalid_request_error");
    assert!(seen.0.lock().unwrap().is_empty(), "the executor is never called");
}

/// Go `TestOpenAIResponsesCompactExecute`: the `responses/compact` alt reaches the
/// executor with the Responses source format, and its body is answered as is.
#[tokio::test]
async fn compact_execute() {
    let (url, seen) = proxy().await;
    let body = br#"{"model":"fast","input":"hello"}"#.to_vec();
    let (status, text) = send(&url, "/v1/responses/compact", &[], body).await;
    assert_eq!(status, 200, "{text}");
    assert_eq!(text, r#"{"ok":true}"#);
    let seen = seen.0.lock().unwrap().clone();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].0, "/v1/responses/compact", "the compact alt");
    // A chat-format source would have been translated to `messages`.
    assert_eq!(seen[0].2, r#"{"model":"up-model","input":"hello"}"#);
}

/// Compact calls each key received, in the order the keys first sent one.
fn compact_sends(seen: &Seen) -> Vec<usize> {
    let roles = seen.3.lock().unwrap().clone();
    let requests = seen.0.lock().unwrap();
    let sent = |key: &String| {
        requests
            .iter()
            .filter(|r| r.0.ends_with("/responses/compact") && r.1.as_ref() == Some(key))
            .count()
    };
    roles.iter().map(sent).collect()
}

/// A compact failure is routed by its class (D2-PLAN §3.3). The call is the probe: an
/// unsupported answer is skipped and never remembered, a credential fault or a transient
/// one fails over and cools the credential, and a client fault stops with nothing sent to
/// the next credential.
#[tokio::test]
async fn compact_failures_are_routed_by_class() {
    const OK: &str = r#"{"ok":true}"#;
    let quota = r#"{"error":{"message":"quota","type":"insufficient_quota"}}"#;
    struct Row {
        case: &'static str,
        /// What the credential tried first (`a`) answers (status 0 resets the connection) and
        /// what the next one (`b`) answers.
        a: (u16, &'static str),
        b: (u16, &'static str),
        /// The first compact's status and its text when it differs from `OK`.
        status: u16,
        /// Compact sends to `(a, b)` after the first and after a second compact.
        first: (usize, usize),
        second: (usize, usize),
    }
    let ok = (200, OK);
    let mut rows = vec![];
    for status in [404, 405, 501] {
        rows.push(Row {
            case: "an unsupported answer is skipped and tried again by the next compact",
            a: (status, "no such endpoint"),
            b: ok,
            status: 200,
            first: (1, 1),
            second: (2, 2),
        });
    }
    rows.push(Row {
        case: "a transport failure fails over; the scheduler never cools a connection fault",
        a: (0, ""),
        b: ok,
        status: 200,
        first: (1, 1),
        second: (2, 2),
    });
    for status in [408, 500, 502, 503, 504] {
        rows.push(Row {
            case: "a transient answer fails over and cools the credential",
            a: (status, "boom"),
            b: ok,
            status: 200,
            first: (1, 1),
            second: (1, 2),
        });
    }
    for status in [402, 403, 429] {
        rows.push(Row {
            case: "a credential answer fails over and cools the credential",
            a: (status, quota),
            b: ok,
            status: 200,
            first: (1, 1),
            second: (1, 2),
        });
    }
    for status in [400, 409, 413, 422] {
        rows.push(Row {
            case: "a client fault stops with nothing sent to the next credential",
            a: (status, "bad request"),
            b: ok,
            status,
            first: (1, 0),
            // Round robin moves on; the client's fault cooled nothing.
            second: (1, 1),
        });
    }
    rows.push(Row {
        case: "every credential unsupported answers the last one's status",
        a: (404, "no such endpoint"),
        b: (404, "no such endpoint"),
        status: 404,
        first: (1, 1),
        second: (2, 2),
    });
    let body = br#"{"model":"fast","input":"hello"}"#.to_vec();
    for row in rows {
        let (url, seen) = proxy_keys(&["sk-fake-a", "sk-fake-b"]).await;
        *seen.1.lock().unwrap() = vec![row.a, row.b];
        let sends = |seen: &Seen| {
            let mut sends = compact_sends(seen);
            sends.resize(2, 0);
            (sends[0], sends[1])
        };
        let case = format!("{} ({})", row.case, row.a.0);
        let (status, text) = send(&url, "/v1/responses/compact", &[], body.clone()).await;
        assert_eq!(status, row.status, "{case}: {text}");
        if row.status == 200 {
            assert_eq!(text, OK, "{case}");
        }
        assert_eq!(sends(&seen), row.first, "{case}: first compact");
        send(&url, "/v1/responses/compact", &[], body.clone()).await;
        assert_eq!(sends(&seen), row.second, "{case}: second compact");
    }
}

/// Go `pickLCP` through dispatch: with session affinity, an authenticated request
/// without an explicit session gets its LCP session as the attempt's canonical session,
/// in `$CPA-SESSION-ID` custom headers and in the usage record with its lineage.
#[tokio::test]
async fn lcp_session_reaches_custom_headers_and_usage_records() {
    type Sessions = Arc<Mutex<Vec<String>>>;
    let sessions: Sessions = Arc::default();
    let upstream = axum::Router::new()
        .fallback(|State(seen): State<Sessions>, req: Request| async move {
            let header = req.headers().get("x-session").map(|v| v.to_str().unwrap().to_owned());
            seen.lock().unwrap().push(header.unwrap_or_default());
            ([("content-type", "application/json")], CHAT).into_response()
        })
        .with_state(sessions.clone());
    let upstream_url = serve(upstream).await;
    // A private credential directory: `testing::runtime` stores its `.json` files in the test database.
    let auth_dir = std::env::temp_dir().join(format!("cooker-compat-lcp-auths-{}", std::process::id()));
    std::fs::create_dir_all(&auth_dir).unwrap();
    let config = Config::parse(&format!(
        "access:\n  api-keys: [client-key]\nrouting:\n  session-affinity: true\nobservability:\n  usage:\n    usage-statistics-enabled: true\napi-keys:\n  openai-compatibility:\n    - name: Acme\n      base-url: {upstream_url}/v1\n      headers:\n        X-Session: $CPA-SESSION-ID\n      models:\n        - name: up-model\n          alias: fast\n      keys:\n        - api-key: sk-fake-a\n"))
    .unwrap();
    let credentials = ck_core::config::credentials::load(&config);
    let executors = Executors {
        claude: ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
        codex: Default::default(),
        devices: Default::default(),
        openai: Default::default(),
        google: Default::default(),
    };
    let rt = Arc::new(ck_server::testing::runtime(config, credentials, executors));
    rt.usage_queue().configure(true, &rt.config());
    let url = serve(router(rt.clone())).await;
    let turns = |texts: &[&str]| {
        let messages: Vec<serde_json::Value> = texts
            .iter()
            .enumerate()
            .map(|(i, t)| serde_json::json!({"role": if i % 2 == 0 { "user" } else { "assistant" }, "content": t}))
            .collect();
        serde_json::json!({"model": "fast", "messages": messages}).to_string()
    };
    for body in [
        turns(&["hello"]),
        turns(&["hello", "hi", "more"]),
        turns(&["hello", "hi", "other"]),
    ] {
        let (status, _, text) = post(&url, &body).await;
        assert_eq!(status, 200, "{text}");
    }
    let (status, text) = send(
        &url,
        "/v1/chat/completions",
        &[("x-session-id", "explicit-1")],
        turns(&["hello"]).into_bytes(),
    )
    .await;
    assert_eq!(status, 200, "{text}");

    let seen = sessions.lock().unwrap().clone();
    assert_eq!(seen.len(), 4);
    assert!(seen[0].starts_with("lcp:v1:"), "{seen:?}");
    assert_eq!(seen[1], seen[0], "a growing conversation keeps its session");
    assert!(
        seen[2].starts_with("lcp:v1:") && seen[2] != seen[0],
        "a fork gets its own: {seen:?}"
    );
    assert!(!seen[3].starts_with("lcp:"), "an explicit session wins: {seen:?}");

    let records: Vec<serde_json::Value> = rt
        .usage_queue()
        .pop_oldest(10)
        .iter()
        .map(|r| serde_json::from_slice(r).unwrap())
        .collect();
    assert_eq!(records.len(), 4);
    let field = |i: usize, key: &str| records[i].get(key).cloned();
    assert!(field(0, "session_id").is_some());
    assert_eq!(field(1, "session_id"), field(0, "session_id"));
    assert_eq!(field(0, "node_kind"), None, "a plain hit carries no node kind");
    assert_ne!(field(2, "session_id"), field(0, "session_id"));
    assert_eq!(field(2, "node_kind"), Some("fork".into()));
    assert_eq!(field(2, "is_fork"), Some(true.into()));
    assert!(field(2, "parent_session_id").is_some(), "{}", records[2]);
    assert_eq!(field(2, "is_compaction"), None);
}
