//! Meta response and transport fixtures; request controls follow the Meta policy tests.

use std::time::Duration;

use serde_json::{Value, json};

use super::response::stream_events;
use super::*;
use crate::kimi_fixture::{Captured, Mock, assert_same_request, fixture, request};
use crate::meta_auth::{
    self, GoMap, MetaAuth, TokenStorage, credential_file_name, encode_token_file, go_map_from_json, login_with,
};
use ck_wire::json as gj;
#[path = "meta_auth_decode_tests.rs"]
mod auth_decode;
#[path = "meta_auth_fixture_tests.rs"]
mod auth_fixtures;
use auth_fixtures::login_auth;

/// A private directory under the system temp dir, removed on drop.
struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("cooker-meta-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn cfg() -> Config {
    Config::parse("").unwrap()
}

/// The Go capture server's origin, `http://127.0.0.1:<port>`, found anywhere in a fixture.
fn go_origin(fx: &Value) -> Option<String> {
    let text = fx.to_string();
    let start = text.find("http://127.0.0.1:")?;
    let rest = &text[start + "http://127.0.0.1:".len()..];
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    Some(format!("http://127.0.0.1:{digits}"))
}

fn replace_origin(value: &Value, origin: &str, url: &str) -> Value {
    match value {
        Value::String(s) => Value::String(s.replace(origin, url)),
        Value::Array(items) => Value::Array(items.iter().map(|v| replace_origin(v, origin, url)).collect()),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), replace_origin(v, origin, url)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// The credential Go executed with: metadata and attributes exactly as recorded, the
/// capture server's URL swapped for the Rust mock's.
fn meta_credential(fx: &Value, origin: &str, url: &str) -> Credential {
    let metadata = replace_origin(&fx["credential"], origin, url)
        .as_object()
        .cloned()
        .unwrap_or_default();
    let mut c = Credential {
        id: "meta-fixture.json".into(),
        provider: PROVIDER.into(),
        source: Source::File("/fixture/meta-fixture.json".into()),
        disabled: false,
        label: String::new(),
        attributes: Default::default(),
        metadata,
        revision: 0,
    };
    c.refresh_derived();
    c.attributes = replace_origin(&fx["attributes"], origin, url)
        .as_object()
        .map(|attrs| {
            attrs
                .iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_owned()))
                .collect()
        })
        .unwrap_or_default();
    // ck-core's config loader labels config API keys `<provider>-apikey`.
    if c.attributes.get("source").is_some_and(|s| s.starts_with("config:")) {
        c.label = format!("{PROVIDER}-apikey");
    }
    c
}

struct Run {
    usage: std::sync::Arc<crate::kimi_fixture::UsageLog>,
    fx: Value,
    origin: String,
    url: String,
    credential: Credential,
    captured: Vec<Captured>,
    body: Option<String>,
    chunks: Vec<String>,
    error: Option<ExecError>,
}

async fn run(name: &str) -> Run {
    let fx = fixture("meta", name);
    let origin = go_origin(&fx).expect("fixture names its capture server");
    let mock = Mock::start_replacing(&fx["responses"], Some(&origin)).await;
    let credential = meta_credential(&fx, &origin, &mock.url);
    let exec = MetaExecutor::with_client(default_client()).with_mint_url(&format!("{}/muse-code/key", mock.url));
    let (mut body, mut chunks, mut error) = (None, Vec::new(), None);
    let cfg = Config::parse(fx["request"]["config"].as_str().unwrap_or_default()).unwrap();
    let usage = std::sync::Arc::new(crate::kimi_fixture::UsageLog::default());
    let mut req = request(&fx, "");
    req.usage = usage.sink();
    match exec.execute(&credential, req, &cfg).await {
        Err(e) => error = Some(e),
        Ok(response) => match response.body {
            ResponseBody::Buffered(b) => body = Some(String::from_utf8(b.to_vec()).unwrap()),
            ResponseBody::Stream(mut s) => {
                while let Some(item) = s.next().await {
                    match item {
                        Ok(chunk) => chunks.push(String::from_utf8(chunk.to_vec()).unwrap()),
                        Err(e) => error = Some(e),
                    }
                }
            }
        },
    }
    Run {
        usage,
        captured: mock.captured(),
        url: mock.url,
        fx,
        origin,
        credential,
        body,
        chunks,
        error,
    }
}

fn assert_upstream(r: &Run, name: &str) {
    let go: Vec<Captured> = r.fx["upstream"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .map(|c| Captured::from_fixture(&replace_origin(c, &r.origin, &r.url)))
        .collect();
    assert_eq!(r.captured.len(), go.len(), "{name}: upstream request count");
    for (go, rust) in go.iter().zip(&r.captured) {
        if rust.target.ends_with("/responses") {
            assert_eq!(rust.method, go.method, "{name}: upstream method");
            assert_eq!(rust.target, go.target, "{name}: upstream target");
        } else {
            assert_same_request(name, go, rust, &["Host"]);
        }
    }
}

fn assert_downstream(r: &Run, name: &str) {
    let down = &r.fx["downstream"];
    if let Some(body) = down["body"].as_str().filter(|b| !b.is_empty()) {
        assert_eq!(r.body.as_deref(), Some(body), "{name}: response body");
    }
    // Stream output is client framing: Go's chunks joined by its Responses route
    // (responsesSSEFramer, before route repair), recorded as `frames`; other clients get
    // the chunks as they are.
    let key = if r.fx["request"]["source"] == "openai-response" {
        "frames"
    } else {
        "chunks"
    };
    let go_frames: Vec<&str> = down[key]
        .as_array()
        .map(|c| c.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    assert_eq!(r.chunks, go_frames, "{name}: stream frames");
    let go_error = down["stream_err"].as_str().or(down["err_body"].as_str());
    match (&r.error, down["err_status"].as_i64()) {
        (None, None) => {}
        (Some(e), Some(-1)) => {
            // A plain Go error (no status): the handler answers 500 with its text. The
            // upstream body Go appends is withheld (see MintError::redacted).
            let rust = String::from_utf8_lossy(&e.body);
            assert_eq!(e.status, 500, "{name}: plain errors are 500");
            assert!(
                go_error.unwrap().starts_with(&format!("{rust}: ")),
                "{name}: {rust:?} must be Go's message without the upstream body"
            );
        }
        (Some(e), Some(status)) => {
            assert_eq!(i64::from(e.status), status, "{name}: error status");
            assert_eq!(
                String::from_utf8_lossy(&e.body),
                go_error.unwrap(),
                "{name}: error body"
            );
        }
        (rust, go) => panic!("{name}: rust error {rust:?}, go status {go:?}"),
    }
}

#[tokio::test]
async fn response_and_transport_contracts_match_recordings() {
    for name in [
        "responses-nonstream-collected",
        "responses-stream",
        "responses-nonstream-json-body",
        "error-429-subscription-quota",
        "error-429-plain",
        "error-404-model",
        "error-404-resets",
        "stream-error-event",
        "stream-error-after-data-only",
        "nonstream-response-failed",
        "nonstream-incomplete-fallback-items",
        "nonstream-disconnect-before-completed",
        "stream-plain-json-lines",
        "compact-rejected",
        "count-tokens",
        "config-apikey-headers",
        "payload-rules-and-headers",
        "config-apikey-dca-only",
        "remint-from-dca",
        "remint-access-token-dca-minted-base",
        "remint-failure",
        "apply-patch-stream",
        "apply-patch-nonstream",
        "apply-patch-invalid-stream",
        "apply-patch-invalid-nonstream",
        "apply-patch-eof-stream",
        "stream-completed-then-error",
        "stream-done-only",
        "apply-patch-stream-event-lines",
        "stream-tier-merge",
        "stream-nbsp-data",
        "claude-stream-input-estimate",
    ] {
        let r = run(name).await;
        assert_upstream(&r, name);
        assert_downstream(&r, name);
    }
}

#[tokio::test]
async fn error_signals_follow_go_scopes_and_retry_hints() {
    for name in [
        "error-429-subscription-quota",
        "error-429-plain",
        "error-404-model",
        "error-404-resets",
        "stream-error-event",
        "nonstream-response-failed",
    ] {
        let r = run(name).await;
        let e = r.error.as_ref().unwrap_or_else(|| panic!("{name}: error expected"));
        let extra = &r.fx["extra"];
        let credential_scoped = extra["credential_scoped"].as_bool().unwrap();
        let expected_scope = match (e.status, credential_scoped) {
            (_, true) => FailureScope::Credential,
            (404 | 429, false) => FailureScope::Model,
            _ => FailureScope::Credential,
        };
        assert_eq!(e.scope, expected_scope, "{name}: scope");
        match extra["retry_after_secs"].as_f64() {
            None => assert_eq!(e.retry_after, None, "{name}: no retry hint"),
            Some(300.0) => assert_eq!(e.retry_after, Some(Duration::from_secs(300)), "{name}"),
            Some(go_secs) => {
                let elapsed = chrono::Utc::now().timestamp() - extra["now_unix"].as_i64().unwrap();
                let expected = go_secs - elapsed as f64;
                let got = e.retry_after.unwrap().as_secs_f64();
                assert!((got - expected).abs() < 5.0, "{name}: retry {got} vs {expected}");
            }
        }
    }
}

#[test]
fn retry_hint_follows_resets_at() {
    // TestMetaExecutor_ParseRetryAfter: a future resets_at about 45 minutes out.
    let future = chrono::Utc::now().timestamp() + 45 * 60;
    let body = format!(
        r#"{{"error":{{"code":"rate_limit_exceeded","message":"Subscription quota exhausted.","resets_at":{future},"type":"rate_limit_error"}}}}"#
    );
    let e = upstream_error(429, body.as_bytes());
    let secs = e.retry_after.unwrap().as_secs();
    assert!((40 * 60..=50 * 60).contains(&secs));
    assert_eq!(e.scope, FailureScope::Credential);
    let past = chrono::Utc::now().timestamp() - 600;
    let e = upstream_error(429, format!(r#"{{"error":{{"resets_at":{past}}}}}"#).as_bytes());
    assert_eq!(e.retry_after, None);
    assert_eq!(e.scope, FailureScope::Model);
    assert_eq!(upstream_error(400, body.as_bytes()).retry_after, None);
}

#[tokio::test]
async fn device_flow_errors_and_slow_down_follow_go() {
    let device = json!({"status":200,"body":r#"{"device_code":"d","user_code":"u","interval":1,"expires_in":600}"#});
    let poll = |status: u16, body: &str| json!({"status":status,"body":body});
    let cases = [
        (
            poll(400, r#"{"error":"access_denied"}"#),
            "meta auth: access was denied by user",
        ),
        (
            poll(400, r#"{"error":"expired_token"}"#),
            "meta auth: device code has expired",
        ),
        (
            poll(400, r#"{"error":"invalid_client","error_description":"bad client"}"#),
            "meta auth: error from authorization server: invalid_client: bad client",
        ),
        (
            poll(200, r#"{"token_type":"bearer"}"#),
            "meta auth: response missing access_token",
        ),
    ];
    for (answer, message) in cases {
        let mock = Mock::start(&json!([device, answer])).await;
        let auth = login_auth(&mock.url);
        let code = auth.start_device_flow().await.unwrap();
        assert_eq!(auth.wait_for_authorization(&code).await.unwrap_err(), message);
    }
}

#[tokio::test]
async fn device_flow_polling_and_deadline_follow_go() {
    let device = json!({"status":200,"body":r#"{"device_code":"d","user_code":"u","interval":1,"expires_in":600}"#});
    let poll = |status: u16, body: &str| json!({"status":status,"body":body});
    // slow_down adds five server seconds; unknown non-JSON answers keep polling.
    let mock = Mock::start(&json!([
        device,
        poll(400, r#"{"error":"slow_down"}"#),
        poll(502, "bad gateway"),
        poll(200, r#"{"access_token":"dca:t","expires_in":0}"#),
        poll(500, "mint down"),
    ]))
    .await;
    let auth = login_auth(&mock.url);
    let code = auth.start_device_flow().await.unwrap();
    let started = tokio::time::Instant::now();
    let bundle = auth.wait_for_authorization(&code).await.unwrap();
    assert!(
        started.elapsed() >= Duration::from_millis(10 + 60 + 60),
        "slow_down widened the interval"
    );
    assert_eq!(bundle.token.access_token, "dca:t");
    assert_eq!(bundle.minted, None, "a failed mint keeps the DCA-only bundle");
    // The deadline is the device code's expires_in.
    let mock = Mock::start(&json!([
        {"status":200,"body":r#"{"device_code":"d","user_code":"u","interval":1,"expires_in":3}"#},
        poll(400, r#"{"error":"authorization_pending"}"#),
        poll(400, r#"{"error":"authorization_pending"}"#),
        poll(400, r#"{"error":"authorization_pending"}"#),
    ]))
    .await;
    let auth = login_auth(&mock.url);
    let code = auth.start_device_flow().await.unwrap();
    assert_eq!(
        auth.wait_for_authorization(&code).await.unwrap_err(),
        "meta auth: authorization timed out or canceled: context deadline exceeded"
    );
    // Device authorization failures.
    let mock = Mock::start(&json!([
        poll(400, " nope "),
        poll(200, r#"{"device_code":"d","user_code":" "}"#),
        poll(200, r#"{"device_code":"d","user_code":"u","interval":1.5}"#),
    ]))
    .await;
    let auth = login_auth(&mock.url);
    crate::kimi_fixture::assert_go_message_without_body(
        "device flow 400",
        &auth.start_device_flow().await.unwrap_err(),
        "meta device flow failed (HTTP 400): nope",
    );
    assert_eq!(
        auth.start_device_flow().await.unwrap_err(),
        "meta device flow: response missing required device_code or user_code"
    );
    assert!(
        auth.start_device_flow()
            .await
            .unwrap_err()
            .starts_with("meta device flow: parse response: ")
    );
}

#[tokio::test]
async fn failed_error_body_read_is_not_classified_by_status() {
    // Go returns io.ReadAll's error before wrapMetaUpstreamError: a truncated 429 body
    // must not become a credential-wide quota cooldown.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 65536];
        let _ = socket.read(&mut buf).await;
        let body = r#"{"error":{"message":"subscription quota exhausted","resets_at":4102444800}}"#;
        let head = format!(
            "HTTP/1.1 429 Too Many Requests\r\nContent-Length: {}\r\n\r\n",
            body.len() + 100
        );
        let _ = socket.write_all(head.as_bytes()).await;
        let _ = socket.write_all(body.as_bytes()).await;
    });
    let fx = fixture("meta", "error-429-subscription-quota");
    let origin = go_origin(&fx).unwrap();
    let credential = meta_credential(&fx, &origin, &url);
    let error = MetaExecutor::with_client(default_client())
        .execute(&credential, request(&fx, ""), &cfg())
        .await
        .err()
        .unwrap();
    assert_ne!(error.status, 429);
    assert_eq!(error.scope, FailureScope::Transport);
    assert_eq!(error.retry_after, None);
}

#[tokio::test]
async fn empty_original_request_falls_back_to_the_payload() {
    // prepareResponsesRequest: originalPayload is req.Payload when OriginalRequest is empty,
    // so response.created carries the client model with its suffix.
    let fx = fixture("meta", "responses-stream");
    let origin = go_origin(&fx).unwrap();
    let mock = Mock::start(&json!([{"status":200,"headers":[["Content-Type","text/event-stream"]],"body":"data: {\"type\":\"response.created\",\"response\":{}}\n\n"}])).await;
    let credential = meta_credential(&fx, &origin, &mock.url);
    let mut req = request(&fx, "");
    req.original_body = Bytes::new();
    let response = MetaExecutor::with_client(default_client())
        .execute(&credential, req, &cfg())
        .await
        .unwrap();
    let ResponseBody::Stream(mut stream) = response.body else {
        panic!("stream expected");
    };
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(
        String::from_utf8(first.to_vec()).unwrap(),
        "data: {\"type\":\"response.created\",\"response\":{\"model\":\"muse-spark-1.3(high)\"}}\n\n"
    );
}

#[test]
fn strict_error_body_read_surfaces_errors_past_the_kept_prefix() {
    let chunks: Vec<Result<Bytes, ExecError>> = vec![
        Ok(Bytes::from_static(b"0123456789")),
        Ok(Bytes::from_static(b"abcdef")),
        Err(ExecError::local(502, FailureScope::Transport, "unexpected EOF")),
    ];
    let body = futures_util::stream::iter(chunks).boxed();
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let error = rt.block_on(crate::kimi_http::read_all_strict(body, 4)).unwrap_err();
    assert_eq!(error.scope, FailureScope::Transport);
    let ok = futures_util::stream::iter(vec![Ok(Bytes::from_static(b"0123456789"))]).boxed();
    assert_eq!(rt.block_on(crate::kimi_http::read_all_strict(ok, 4)).unwrap(), "0123");
}

#[tokio::test]
async fn apply_patch_failures_end_meta_streams_like_go() {
    // Go's Meta loop: StopApplyPatchStream after each translated line; at EOF only the
    // translator's flush (no FinalizeApplyPatchStream, no [DONE]).
    async fn run(lines: Vec<&'static str>, fail_on_finalize: bool) -> (Vec<String>, Vec<String>) {
        let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let translator = Box::new(crate::kimi_fixture::PatchProbe {
            failed: false,
            fail_on_finalize,
            log: log.clone(),
        });
        let upstream =
            futures_util::stream::iter(lines.into_iter().map(|l| Ok(Bytes::from_static(l.as_bytes())))).boxed();
        let inactive = ck_translate::apply_patch_responses::State::new(Format::OpenAIResponse, b"{}", b"{}");
        let out: Vec<String> = stream_events(
            upstream,
            translator,
            crate::gemini_stream::ClaudeInputTokens::new(
                Format::OpenAIResponse,
                Format::Codex,
                Format::OpenAIResponse,
                Bytes::new(),
            ),
            false,
            inactive,
            Default::default(),
            Default::default(),
        )
        .map(|item| match item {
            Ok(b) => String::from_utf8(b.to_vec()).unwrap(),
            Err(e) => format!("ERR {} {}", e.status, String::from_utf8_lossy(&e.body)),
        })
        .collect()
        .await;
        let log = log.lock().unwrap().clone();
        (out, log)
    }
    let patch = format!("ERR 502 {}", ck_translate::APPLY_PATCH_UPSTREAM_ERROR);
    let (out, log) = run(
        vec!["data: {\"a\":1}", "data: {\"BAD_PATCH\":1}", "data: {\"c\":1}"],
        false,
    )
    .await;
    assert_eq!(
        out,
        [
            "frame data: {\"a\":1}",
            "frame data: {\"BAD_PATCH\":1}",
            "flushed",
            patch.as_str()
        ]
    );
    assert_eq!(log.len(), 2);
    let (out, log) = run(vec!["data: {\"a\":1}"], true).await;
    assert_eq!(out, ["frame data: {\"a\":1}", "finished"]);
    assert_eq!(log, ["event data: {\"a\":1}", "finish"]);
}

#[tokio::test]
async fn usage_reports_match_go_records() {
    for name in [
        "responses-nonstream-collected",
        "responses-stream",
        "responses-nonstream-json-body",
        "stream-error-event",
        "nonstream-response-failed",
        "nonstream-incomplete-fallback-items",
        "payload-rules-and-headers",
        "remint-from-dca",
        "error-429-plain",
        "apply-patch-stream",
        "apply-patch-nonstream",
        "apply-patch-invalid-stream",
        "apply-patch-invalid-nonstream",
        "apply-patch-eof-stream",
        "stream-completed-then-error",
        "stream-done-only",
        "apply-patch-stream-event-lines",
        "stream-tier-merge",
        "stream-nbsp-data",
        "claude-stream-input-estimate",
    ] {
        let mut r = run(name).await;
        if name == "claude-stream-input-estimate" {
            // An omitted effort stays unknown instead of inheriting Codex's medium default.
            r.fx["extra"]["usage"][0]["reasoning_effort"] = json!("");
        }
        let failure = r.error.as_ref().map(crate::kimi_fixture::go_failure);
        crate::kimi_fixture::assert_usage_like_go(name, &r.fx, &r.usage, failure.as_ref());
    }
}

/// Go ignores a refusal only the original (baseline) body hits
/// (TranslateRequestWithAPIKeyModelCompatibility returns no error): the working body is
/// the one sent, and only its own refusal refuses the request.
#[tokio::test]
async fn a_refused_baseline_alone_does_not_refuse_the_request() {
    let fx = fixture("meta", "responses-nonstream-collected");
    let origin = go_origin(&fx).unwrap();
    let hi = br#"{"model":"muse-spark-1.3","max_tokens":16,"messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}"#;
    let refused = br#"{"model":"muse-spark-1.3","max_tokens":16,"messages":[{"role":"user","content":[{"type":"container_upload","source":{"type":"file","file_id":"f1"}}]}]}"#;
    for baseline_only in [true, false] {
        let mock = Mock::start(&fx["responses"]).await;
        let credential = meta_credential(&fx, &origin, &mock.url);
        let mut req = request(&fx, "");
        req.source_format = Format::Claude;
        req.response_format = Format::Claude;
        let (body, original): (&'static [u8], &'static [u8]) =
            if baseline_only { (hi, refused) } else { (refused, hi) };
        req.body = Bytes::from_static(body);
        req.original_body = Bytes::from_static(original);
        let result = MetaExecutor::with_client(default_client())
            .execute(&credential, req, &cfg())
            .await;
        assert_eq!(result.is_ok(), baseline_only, "baseline_only={baseline_only}");
        let captured = mock.captured();
        assert_eq!(captured.len(), usize::from(baseline_only));
        if let Some(sent) = captured.first() {
            assert!(String::from_utf8_lossy(&sent.body).contains(r#""text":"hi""#));
        }
    }
}
