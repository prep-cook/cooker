//! Gemini API keys through the real router, runtime, registry and executor, against a local
//! mock upstream. Byte-level parity with Go is checked in ck-exec (gemini_tests.rs); this
//! proves the wiring: config credentials, alias routing and the `/v1beta` routes encoding
//! the answer with the Gemini codec.

use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::{Request, State};
use axum::response::{IntoResponse, Response};
use ck_core::config::Config;
use ck_exec::Executors;
use ck_exec::claude::ClaudeExecutor;
use ck_server::router;

/// Path with query, the upstream API key header, the Api-Revision header and the body.
type Seen = Mutex<Vec<(String, Option<String>, Option<String>, String, Option<String>)>>;

const ANSWER: &str = r#"{"candidates":[{"content":{"role":"model","parts":[{"text":"ok"}]},"finishReason":"STOP"}],"usageMetadata":{"totalTokenCount":4}}"#;
const CHUNK: &str =
    r#"{"candidates":[{"content":{"role":"model","parts":[{"text":"he"}]}}],"usageMetadata":{"totalTokenCount":2}}"#;

async fn upstream(State(seen): State<Arc<Seen>>, req: Request) -> Response {
    let path = req.uri().path_and_query().map(|p| p.to_string()).unwrap_or_default();
    let (key, revision, session) = {
        let header = |name: &str| req.headers().get(name).and_then(|v| v.to_str().ok()).map(str::to_owned);
        (header("x-goog-api-key"), header("api-revision"), header("x-session"))
    };
    let body = axum::body::to_bytes(req.into_body(), usize::MAX).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    seen.lock()
        .unwrap()
        .push((path.clone(), key, revision, body.clone(), session));
    if path.contains(":streamGenerateContent") {
        let sse = format!("data: {CHUNK}\n\ndata: {ANSWER}\n\n");
        return ([("content-type", "text/event-stream")], Bytes::from(sse)).into_response();
    }
    if path.contains(":countTokens") {
        return (
            [("content-type", "application/json")],
            r#"{"totalTokens":7,"promptTokensDetails":[{"modality":"TEXT","tokenCount":7}]}"#,
        )
            .into_response();
    }
    ([("content-type", "application/json")], ANSWER).into_response()
}

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    url
}

async fn proxy() -> (String, Arc<Seen>) {
    let seen = Arc::new(Seen::default());
    let up = serve(axum::Router::new().fallback(upstream).with_state(seen.clone())).await;
    (proxy_to(&up).await, seen)
}

/// The router with a Gemini key whose base URL is `up`.
async fn proxy_to(up: &str) -> String {
    serve(router(runtime_to(up))).await
}

fn runtime_to(up: &str) -> Arc<ck_server::Runtime> {
    let config = Config::parse(&format!(
        "access:\n  api-keys: [client-key]\napi-keys:\n  gemini:\n    - base-url: {up}/\n      headers:\n        X-Session: \"s-$CPA-SESSION-ID\"\n      models:\n        - name: gemini-2.5-flash\n          alias: flash\n      keys:\n        - api-key: AIza-fake-upstream"
    ))
    .unwrap();
    let credentials = ck_core::config::credentials::from_config(&config);
    let executors = Executors {
        claude: ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
        codex: Default::default(),
        devices: Default::default(),
        openai: Default::default(),
        google: Default::default(),
    };
    Arc::new(ck_server::testing::runtime(config, credentials, executors))
}

async fn post(url: &str, path: &str, body: &str) -> (u16, String, String) {
    let res = wreq::Client::new()
        .post(format!("{url}{path}"))
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

const HI: &str = r#"{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}"#;

#[tokio::test]
async fn generate_content_routes_alias_to_gemini_key() {
    let (url, seen) = proxy().await;
    let (status, content_type, text) = post(&url, "/v1beta/models/flash:generateContent", HI).await;
    assert_eq!(status, 200, "{text}");
    assert!(content_type.starts_with("application/json"), "{content_type}");
    assert_reply(&text);
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 1);
    let (path, key, _, body, session) = &seen[0];
    // A Gemini request carries no `stream` member, so the executor streams upstream.
    assert_eq!(path, "/v1beta/models/gemini-2.5-flash:streamGenerateContent?alt=sse");
    // Without an explicit client session, $CPA-SESSION-ID is the derived canonical
    // session the conductor binds (Go syncMetadataSessionToContext).
    assert!(
        session.as_deref().is_some_and(|s| s.starts_with("s-derived:ctx:v1:")),
        "{session:?}"
    );
    assert_eq!(
        key.as_deref(),
        Some("AIza-fake-upstream"),
        "the client key never reaches the upstream"
    );
    // The URL names the model; the upload carries the client's contents.
    assert!(body.contains(r#""text":"hi""#), "{body}");
}

#[tokio::test]
async fn stream_generate_content_writes_executor_bytes() {
    let (url, seen) = proxy().await;
    let (status, content_type, text) = post(&url, "/v1beta/models/flash:streamGenerateContent?alt=sse", HI).await;
    assert_eq!(status, 200, "{text}");
    assert!(content_type.starts_with("text/event-stream"), "{content_type}");
    let frames: Vec<serde_json::Value> = text
        .split("\n\n")
        .filter_map(|frame| frame.lines().find_map(|line| line.strip_prefix("data: ")))
        .map(|data| serde_json::from_str(data).unwrap())
        .collect();
    assert_chunks(&frames);
    assert_eq!(
        seen.lock().unwrap()[0].0,
        "/v1beta/models/gemini-2.5-flash:streamGenerateContent?alt=sse"
    );

    // With another `alt`, the answer is one JSON array of the same chunks.
    let (status, _, text) = post(&url, "/v1beta/models/flash:streamGenerateContent?alt=json", HI).await;
    assert_eq!(status, 200, "{text}");
    let frames: Vec<serde_json::Value> = serde_json::from_str(&text).unwrap();
    assert_chunks(&frames);
}

/// The streamed answer: the first chunk's text, then the last with its finish and usage.
fn assert_chunks(frames: &[serde_json::Value]) {
    assert_eq!(frames.len(), 2, "{frames:?}");
    assert_eq!(frames[0]["candidates"][0]["content"]["parts"][0]["text"], "he");
    assert_eq!(frames[1]["candidates"][0]["content"]["parts"][0]["text"], "ok");
    assert_eq!(frames[1]["candidates"][0]["finishReason"], "STOP");
    assert_eq!(frames[1]["usageMetadata"]["totalTokenCount"], 4);
}

/// The buffered answer to the mock's [`CHUNK`] and [`ANSWER`].
fn assert_reply(text: &str) {
    let reply: serde_json::Value = serde_json::from_str(text).unwrap();
    let candidate = &reply["candidates"][0];
    assert_eq!(
        candidate["content"]["parts"],
        serde_json::json!([{"text": "he"}, {"text": "ok"}]),
        "{text}"
    );
    assert_eq!(candidate["finishReason"], "STOP", "{text}");
    assert_eq!(reply["usageMetadata"]["totalTokenCount"], 4, "{text}");
}

#[tokio::test]
async fn count_tokens_reaches_gemini_key() {
    let (url, seen) = proxy().await;
    let (status, _, text) = post(&url, "/v1beta/models/flash:countTokens", HI).await;
    assert_eq!(status, 200, "{text}");
    assert!(text.contains(r#""totalTokens":7"#), "{text}");
    assert_eq!(seen.lock().unwrap()[0].0, "/v1beta/models/gemini-2.5-flash:countTokens");
}

#[tokio::test]
async fn vertex_api_key_routes_alias() {
    let seen = Arc::new(Seen::default());
    let up = serve(axum::Router::new().fallback(upstream).with_state(seen.clone())).await;
    let config = Config::parse(&format!(
        "access:\n  api-keys: [client-key]\napi-keys:\n  vertex:\n    - base-url: {up}/api\n      models:\n        - name: gemini-2.5-flash\n          alias: vflash\n      keys:\n        - api-key: vk-fake\n"
    ))
    .unwrap();
    let credentials = ck_core::config::credentials::from_config(&config);
    let executors = Executors {
        claude: ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
        codex: Default::default(),
        devices: Default::default(),
        openai: Default::default(),
        google: Default::default(),
    };
    let rt = Arc::new(ck_server::testing::runtime(config, credentials, executors));
    let url = serve(router(rt)).await;
    let (status, _, text) = post(&url, "/v1beta/models/vflash:generateContent", HI).await;
    assert_eq!(status, 200, "{text}");
    assert_reply(&text);
    let (path, key, _, body, _) = seen.lock().unwrap()[0].clone();
    assert_eq!(
        path,
        "/api/v1/publishers/google/models/gemini-2.5-flash:streamGenerateContent?alt=sse"
    );
    assert_eq!(key.as_deref(), Some("vk-fake"));
    // The URL names the model; the upload carries the client's contents.
    assert!(body.contains(r#""text":"hi""#), "{body}");
}

/// Replays one scripted upstream answer (status, headers, body) to every request.
async fn scripted(reply: &serde_json::Value) -> String {
    let status = reply["status"].as_u64().unwrap() as u16;
    let headers: Vec<(String, String)> = reply["headers"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|h| (h[0].as_str().unwrap().to_owned(), h[1].as_str().unwrap().to_owned()))
        .collect();
    let body = reply["body"].as_str().unwrap().to_owned();
    let app = axum::Router::new().fallback(move || {
        let (headers, body) = (headers.clone(), body.clone());
        async move {
            let mut response = Response::new(axum::body::Body::from(body));
            *response.status_mut() = axum::http::StatusCode::from_u16(status).unwrap();
            for (name, value) in headers {
                response.headers_mut().append(
                    axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                    value.parse().unwrap(),
                );
            }
            response
        }
    });
    serve(app).await
}

/// A Gemini MALFORMED_FUNCTION_CALL has no Anthropic stop reason: a Claude answer, streamed or
/// not, ends as a normal turn, and the native reason stays in the internal stream
/// (`gemini/response/candidate.rs`).
#[tokio::test]
async fn a_malformed_function_call_ends_a_claude_answer_as_a_normal_turn() {
    let chunk = r#"{"candidates":[{"content":{"role":"model","parts":[]},"finishReason":"MALFORMED_FUNCTION_CALL","index":0}],"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":2,"totalTokenCount":5}}"#;
    // `generateContent` answers the candidate as JSON, `streamGenerateContent` as one event.
    let app = axum::Router::new().fallback(move |uri: axum::http::Uri| async move {
        if uri.path().ends_with(":streamGenerateContent") {
            ([("content-type", "text/event-stream")], format!("data: {chunk}\n\n"))
        } else {
            ([("content-type", "application/json")], chunk.to_owned())
        }
    });
    let url = proxy_to(&serve(app).await).await;
    for stream in [true, false] {
        let body = format!(
            r#"{{"model":"flash","max_tokens":64,"stream":{stream},"messages":[{{"role":"user","content":"hi"}}]}}"#
        );
        let (status, _, text) = post(&url, "/v1/messages", &body).await;
        assert_eq!(status, 200, "stream {stream}: {text}");
        assert!(!text.contains(r#""type":"error""#), "stream {stream}: {text}");
        assert!(text.contains(r#""stop_reason":"end_turn""#), "stream {stream}: {text}");
    }
}

/// The `usage_*` scenarios of the Go executor fixtures through the real router: the
/// record the usage queue stores must carry the tokens, response model and translated
/// reasoning effort Go's `UsageReporter` published for the same upstream answer. The
/// executors report upstream payloads (`ExecRequest::usage`); without them the server
/// would parse the translated client response instead.
#[tokio::test]
async fn usage_records_match_go() {
    // Each base recording with its v8.0.35 recordings replacing or adding scenarios.
    let fixtures = [
        (
            include_str!("../../ck-exec/tests/fixtures/gemini_go.json"),
            include_str!("../../ck-exec/tests/fixtures/gemini_go_v8.0.35.json"),
        ),
        (
            include_str!("../../ck-exec/tests/fixtures/vertex_go.json"),
            include_str!("../../ck-exec/tests/fixtures/vertex_go_v8.0.35.json"),
        ),
    ];
    let mut ran = 0;
    for (base, newer) in fixtures {
        let mut fixture: serde_json::Value = serde_json::from_str(base).unwrap();
        let newer: serde_json::Value = serde_json::from_str(newer).unwrap();
        let cases = fixture["scenarios"].as_array_mut().unwrap();
        for case in newer["scenarios"].as_array().unwrap() {
            match cases.iter_mut().find(|c| c["name"] == case["name"]) {
                Some(slot) => *slot = case.clone(),
                None => cases.push(case.clone()),
            }
        }
        for s in fixture["scenarios"].as_array().unwrap() {
            let name = s["name"].as_str().unwrap();
            if !name.starts_with("usage_") {
                continue;
            }
            // A scenario Go answers before any upstream request has no reply.
            let up = match s.get("upstream").or_else(|| s["replies"].get(0)) {
                Some(reply) => scripted(reply).await,
                None => "http://127.0.0.1:1".to_owned(),
            };
            let config = format!(
                "access:\n  api-keys: [client-key]\n{}",
                s["config"]
                    .as_str()
                    .unwrap()
                    .replace("UPSTREAM", up.trim_start_matches("http://"))
            );
            let config = Config::parse(&config).unwrap();
            let credentials = ck_core::config::credentials::from_config(&config);
            let executors = Executors {
                claude: ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
                codex: Default::default(),
                devices: Default::default(),
                openai: Default::default(),
                google: Default::default(),
            };
            let rt = Arc::new(ck_server::testing::runtime(config, credentials, executors));
            let queue = rt.usage_queue();
            queue.configure(
                true,
                &Config::parse("observability: {usage: {usage-statistics-enabled: true}}\n").unwrap(),
            );
            let url = serve(router(rt.clone())).await;
            let path = match s["source"].as_str().unwrap() {
                "openai" => "/v1/chat/completions",
                "claude" => "/v1/messages",
                "openai-response" => "/v1/responses",
                other => panic!("{name}: no route for {other}"),
            };
            let (status, _, text) = post(&url, path, s["payload"].as_str().unwrap()).await;
            // Only the old arm serves `gemini-interactions` (Q7), and it cannot answer a
            // Responses client, so the candidate is skipped before anything is sent.
            if path == "/v1/responses" {
                assert_eq!(status, 400, "{name}: {text}");
                assert!(text.contains("does not serve the Responses API"), "{name}: {text}");
                ran += 1;
                continue;
            }
            // `gemini-interactions` credentials still take the old arm (Q7).
            let codec = matches!(path, "/v1/chat/completions" | "/v1/messages")
                && s["provider"].as_str() != Some("gemini-interactions");
            // A stream's error comes after its 200 status line.
            let want_status = match s["error"]["status"].as_u64() {
                Some(status) if s["op"] == "execute" => status as u16,
                _ => 200,
            };
            // On a codec route a stream cut before its finish fails the attempt; when the cut
            // lands before the first byte is delivered nothing commits (`http::egress::deliver`).
            let truncated = codec && name.ends_with("eof_without_finish");
            if !(truncated && status == 502) {
                assert_eq!(status, want_status, "{name}: {text}");
            }
            let mut records = Vec::new();
            for _ in 0..50 {
                records = queue.pop_oldest(10);
                if !records.is_empty() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            assert_eq!(records.len(), 1, "{name}: one attempt");
            let got: serde_json::Value = serde_json::from_slice(&records[0]).unwrap();
            let want = &s["usage"];
            for field in [
                "input_tokens",
                "output_tokens",
                "reasoning_tokens",
                "cached_tokens",
                "cache_read_tokens",
                "cache_creation_tokens",
                "total_tokens",
            ] {
                // The contract arm's output count includes reasoning (`docs/INTERNAL-STREAM.md`,
                // usage); Go's Gemini record left thoughts out of it, while a Chat upstream's
                // `completion_tokens` already counts them.
                // An unreported count is absent there, where Go wrote 0.
                let gemini_upstream = want["executor_type"] != "OpenAICompatExecutor";
                let want_value = match field {
                    // A call refused before anything was sent reports no usage at all.
                    _ if codec && want["failed"] == true && want[field] == 0 => serde_json::Value::Null,
                    "output_tokens" if codec && gemini_upstream && want[field].is_u64() => {
                        serde_json::json!(
                            want[field].as_u64().unwrap() + want["reasoning_tokens"].as_u64().unwrap_or(0)
                        )
                    }
                    "reasoning_tokens" | "cached_tokens" | "cache_read_tokens" | "cache_creation_tokens"
                        if codec && want[field] == 0 =>
                    {
                        serde_json::Value::Null
                    }
                    _ => want[field].clone(),
                };
                assert_eq!(got["tokens"][field], want_value, "{name}: {field}");
            }
            let text = |v: &serde_json::Value| v.as_str().unwrap_or_default().to_owned();
            // Go failed a malformed function call; the codec ends it as a normal turn with the
            // upstream's model (`a_malformed_function_call_ends_a_claude_answer_as_a_normal_turn`).
            if !(codec && name.ends_with("malformed_function_call")) {
                assert_eq!(
                    text(&got["response_model"]),
                    text(&want["response_model"]),
                    "{name}: response_model"
                );
                assert_eq!(got["failed"], want["failed"], "{name}: failed");
            }
            // Recorded since d9451cb6: the executor identity, which picks the token rule.
            if let Some(executor_type) = want.get("executor_type") {
                assert_eq!(&got["executor_type"], executor_type, "{name}: executor_type");
                assert_eq!(got["model"], want["model"], "{name}: model");
                // The contract arm cannot classify usage that reports input without output
                // (`Detail::canonical`); Go counted the output as 0.
                let tokens = &got["tokens"];
                let no_output = tokens["input_tokens"].is_u64() && tokens["output_tokens"].is_null();
                let want_quality = match want["quality"].as_str() {
                    Some("complete") if codec && no_output => "unclassified".into(),
                    _ => want["quality"].clone(),
                };
                assert_eq!(got["token_breakdown"]["quality"], want_quality, "{name}: quality");
            }
            // Go's Interactions paths never set a translated effort: the record keeps the
            // conductor's context value (the client's effort), which the executor-level
            // generator does not bind. The Gemini and Vertex paths replace it.
            if s["provider"].as_str() != Some("gemini-interactions") {
                assert_eq!(
                    text(&got["reasoning_effort"]),
                    text(&want["reasoning_effort"]),
                    "{name}: reasoning_effort"
                );
            }
            ran += 1;
        }
    }
    assert_eq!(ran, 13, "usage scenarios in the Go fixtures");
}

/// With session affinity, a Gemini body nested far deeper than any conversation is refused by
/// the codec before anything is sent (the server does not overflow its stack), while a plain
/// one gets an LCP session.
#[tokio::test]
async fn deeply_nested_body_is_refused_before_selection() {
    let seen = Arc::new(Seen::default());
    let up = serve(axum::Router::new().fallback(upstream).with_state(seen.clone())).await;
    let config = Config::parse(&format!(
        "access:\n  api-keys: [client-key]\nrouting:\n  session-affinity: true\napi-keys:\n  gemini:\n    - base-url: {up}/\n      headers:\n        X-Session: \"s-$CPA-SESSION-ID\"\n      models:\n        - name: gemini-2.5-flash\n          alias: flash\n      keys:\n        - api-key: AIza-fake-upstream\n"))
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
    let url = serve(router(rt)).await;
    // Deep enough to overflow unbounded extraction, shallow enough for the derived ID.
    let levels = 1000;
    let deep = format!(
        r#"{{"contents":[{{"role":"user","parts":{}{{"text":"hi"}}{}}}]}}"#,
        "[".repeat(levels),
        "]".repeat(levels)
    );
    let (status, _, text) = post(&url, "/v1beta/models/flash:generateContent", &deep).await;
    assert_eq!(status, 413, "{text}");
    let (status, _, text) = post(&url, "/v1beta/models/flash:generateContent", HI).await;
    assert_eq!(status, 200, "{text}");
    let sessions: Vec<String> = seen
        .lock()
        .unwrap()
        .iter()
        .map(|s| s.4.clone().unwrap_or_default())
        .collect();
    assert_eq!(sessions.len(), 1);
    assert!(sessions[0].starts_with("s-lcp:v1:"), "{sessions:?}");
}

/// A service account whose `base_url` Go's URL parser refuses (`oa_sa_escaped_base` in
/// vertex_go_v8.0.35.json) fails with the parse error before any token exchange; the
/// statusless error starts no retry round and cools the model, so the next request is refused.
#[tokio::test]
async fn vertex_openapi_refused_url_cools_the_model_without_retry_rounds() {
    let tokens = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = tokens.clone();
    let up = serve(axum::Router::new().fallback(move || {
        counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        async { r#"{"access_token":"ya29.fake-access","expires_in":3599,"token_type":"Bearer"}"# }
    }))
    .await;
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../ck-exec/tests/fixtures/vertex_go.json")).unwrap();
    let port = up.rsplit(':').next().unwrap().to_owned();
    let dir = std::env::temp_dir().join(format!("cooker-gemini-vertex-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let file = serde_json::json!({
        "type": "vertex",
        "project_id": "proj-1",
        "email": "svc@proj-1.iam.gserviceaccount.com",
        "openapi": true,
        // An escaped loopback host (the token mock's): even a regression that sent the
        // request could only reach the mock, where it counts as another call.
        "base_url": format!("http://%31%32%37.0.0.1:{port}/v1"),
        "service_account": {
            "type": "service_account",
            "project_id": "proj-1",
            "client_email": "svc@proj-1.iam.gserviceaccount.com",
            "private_key_id": "kid-1",
            "private_key": fixture["keys"]["pkcs1"],
            "token_uri": format!("{up}/token"),
        },
    });
    std::fs::write(dir.join("vertex-sa.json"), file.to_string()).unwrap();
    let mut config =
        Config::parse("access:\n  api-keys: [client-key]\nrouting:\n  retry:\n    request-retry: 2\n").unwrap();
    config.auth_dir = dir;
    // The token endpoint is the local mock; the refused base URL never connects.
    let credentials: Vec<_> = ck_core::config::credentials::load(&config)
        .into_iter()
        .map(ck_server::testing::local)
        .collect();
    assert_eq!(credentials.len(), 1);
    let executors = Executors {
        claude: ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
        codex: Default::default(),
        devices: Default::default(),
        openai: Default::default(),
        google: Default::default(),
    };
    let url = serve(router(Arc::new(ck_server::testing::runtime(
        config,
        credentials,
        executors,
    ))))
    .await;
    let body = r#"{"model":"gemini-2.5-flash","messages":[{"role":"user","content":"hi"}]}"#;
    let parse = format!(
        r#"parse \"http://%31%32%37.0.0.1:{port}/v1/projects/proj-1/locations/us-central1/endpoints/openapi/chat/completions\": invalid URL escape \"%31\""#
    );
    let (status, _, text) = post(&url, "/v1/chat/completions", body).await;
    assert_eq!(status, 500, "{text}");
    assert!(text.contains(&parse), "{text}");
    assert_eq!(
        tokens.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "no token exchange for a refused URL"
    );
    let (status, _, text) = post(&url, "/v1/chat/completions", body).await;
    assert_eq!(status, 503, "{text}");
    assert!(
        text.contains("auth_unavailable") && text.contains("last upstream error: parse"),
        "{text}"
    );
    assert_eq!(
        tokens.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "no exchange for a cooled model"
    );
}
