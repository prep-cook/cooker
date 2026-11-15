//! Requests the OAuth cloak may shape stay on the old route, byte for byte, until owner
//! question 13; a key request on the contract path sends the client's thinking and effort as
//! written, with only the operator's model-name thinking suffix applied.

use std::collections::BTreeMap;
use std::path::Path;

use ck_core::credential::Credential;

use super::support::upstream::Captured;
use super::*;

const OPUS_4_7: &str = "claude-opus-4-7";
const HAIKU: &str = "claude-haiku-4-5-20251001";
const SESSION: &str = "11111111-2222-4333-8444-555555555555";

/// A fake, local-only OAuth credential (the harness fixture), which the cloak serves.
fn oauth() -> Credential {
    let metadata = json!({
        "type": "claude", "access_token": "sk-ant-oat01-FAKE-LOCAL-ONLY",
        "refresh_token": "sk-ant-ort01-FAKE-LOCAL-ONLY", "id_token": "",
        "email": "local-fixture@example.invalid", "account_uuid": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
        "organization_uuid": "12345678-1234-4234-8234-123456789abc", "organization_name": "Local fixture",
        "claude_device_ids": ["0123456789abcdef".repeat(4)],
        "expired": "2099-01-01T00:00:00Z", "last_refresh": "2026-10-01T00:00:00Z",
    });
    let metadata = metadata.as_object().unwrap().clone();
    Credential::from_file(Path::new("/auth"), Path::new("/auth/fixture.json"), metadata).unwrap()
}

/// The measured Claude Code headers of a Haiku Title280 session-title helper.
fn title_helper() -> Vec<(&'static str, &'static str)> {
    vec![
        ("user-agent", "claude-cli/2.1.280 (external, cli)"),
        ("x-app", "cli"),
        ("accept", "application/json"),
        ("content-type", "application/json"),
        ("x-stainless-lang", "js"),
        ("x-stainless-runtime", "node"),
        ("x-stainless-retry-count", "0"),
        ("x-stainless-timeout", "600"),
        ("x-stainless-os", "MacOS"),
        ("x-stainless-arch", "arm64"),
        ("x-stainless-package-version", "0.112.1"),
        ("x-stainless-runtime-version", "v26.3.0"),
        ("anthropic-version", "2023-06-01"),
        ("anthropic-dangerous-direct-browser-access", "true"),
        ("accept-encoding", "gzip, deflate, br, zstd"),
        (
            "anthropic-beta",
            "oauth-2025-04-20,interleaved-thinking-2025-05-14,redact-thinking-2026-02-12,\
             thinking-token-count-2026-05-13,context-management-2025-06-27,prompt-caching-scope-2026-01-05,\
             structured-outputs-2025-12-15,server-side-fallback-2026-06-01,fallback-credit-2026-06-01,\
             cache-diagnosis-2026-04-07",
        ),
        ("x-claude-code-session-id", SESSION),
    ]
}

/// The upstream request of `body` on `credential` through the old route: the selector's old
/// arm when `selector`, else `ClaudeExecutor::execute`. The contract path refuses the attempt.
async fn old_wire(
    credential: &Credential,
    model: &str,
    headers: &[(&str, &str)],
    body: &Value,
    selector: bool,
) -> Captured {
    let reply = if body["stream"] == true {
        Reply::sse(SSE)
    } else {
        Reply::json(MESSAGE)
    };
    let upstream = StreamUpstream::start([reply]).await;
    let mut credential = credential.clone();
    if let Some(base) = credential.attributes.get_mut("base_url") {
        *base = upstream.url();
    }
    let mut fixture = Fixture::with(credential, &upstream.url(), model);
    for (name, value) in headers {
        let name: http::HeaderName = name.parse().unwrap();
        fixture.exchange.headers.append(name, value.parse().unwrap());
    }
    let attempt = fixture.exchange.attempt(&fixture.streams.store);
    assert_eq!(fixture.executors.destination(&attempt), None);
    let mut request = fixture.streams.request();
    let store = &fixture.streams.store;
    let mut ingress = anthropic::request::Ingress::new(
        &mut request,
        store,
        identity(Direction::Request),
        Operation::Generate,
        LIMIT,
    )
    .unwrap();
    let bytes = serde_json::to_vec(body).unwrap();
    let fenced = fixture.fence(&mut ingress, &mut request, &bytes);
    assert!(matches!(fenced, Err(codec::Error::Destination)), "{fenced:?}");
    fixture.old_route(&bytes, selector).await;
    upstream.captured().pop().expect("one upstream request")
}

/// Headers by lowercase name, without `host`: each run has its own local upstream.
fn headers(captured: &Captured) -> BTreeMap<String, Vec<String>> {
    let mut map = BTreeMap::<String, Vec<String>>::new();
    for (k, v) in captured.headers.iter().filter(|(k, _)| !k.eq_ignore_ascii_case("host")) {
        map.entry(k.to_ascii_lowercase()).or_default().push(v.clone());
    }
    map
}

#[tokio::test]
async fn requests_the_cloak_may_shape_stay_on_the_old_route_byte_for_byte() {
    let tools = json!([{"name": "read", "description": "Read a file", "input_schema": {"type": "object"}}]);
    let messages = json!([{"role": "user", "content": "Summarise the plan in one line"}]);
    let user_id = format!(
        r#"{{"device_id":"{}","account_uuid":"","session_id":"{SESSION}"}}"#,
        "a".repeat(64)
    );
    let mut always = credential("claude", "http://127.0.0.1:1", "sk-ant-api03-test");
    always.attributes.insert("cloak_mode".into(), "always".into());
    // Without a cached user id the cloak draws a new device id per request.
    always.attributes.insert("cloak_cache_user_id".into(), "true".into());
    let cases = [
        (
            "plain",
            SONNET,
            vec![],
            json!({"model": SONNET, "max_tokens": 1024, "system": "Be brief.", "messages": messages}),
        ),
        (
            "stream",
            SONNET,
            vec![],
            json!({"model": SONNET, "max_tokens": 1024, "stream": true, "messages": messages}),
        ),
        (
            "sampling",
            SONNET,
            vec![],
            json!({"model": SONNET, "max_tokens": 1024, "temperature": 0.5, "top_p": 0.9, "messages": messages}),
        ),
        (
            "adaptive-forced-tool",
            OPUS_4_7,
            vec![],
            json!({"model": OPUS_4_7, "max_tokens": 4096,
            "thinking": {"type": "adaptive"}, "tools": tools, "tool_choice": {"type": "tool", "name": "read"}, "messages": messages}),
        ),
        (
            "disabled-effort",
            OPUS_4_7,
            vec![],
            json!({"model": OPUS_4_7, "max_tokens": 4096,
            "thinking": {"type": "disabled"}, "output_config": {"effort": "low"}, "messages": messages}),
        ),
        (
            "title-helper",
            HAIKU,
            title_helper(),
            json!({"model": HAIKU, "max_tokens": 80,
            "messages": [{"role": "user", "content": "title"}], "metadata": {"user_id": user_id},
            "output_config": {"format": {"type": "json_schema", "schema": {"type": "object"}}}}),
        ),
    ];
    for (name, model, extra, body) in cases {
        for credential in [oauth(), always.clone()] {
            let selected = old_wire(&credential, model, &extra, &body, true).await;
            let direct = old_wire(&credential, model, &extra, &body, false).await;
            assert_eq!(selected.path, direct.path, "{name}: path");
            assert_eq!(headers(&selected), headers(&direct), "{name}: headers");
            assert_eq!(
                String::from_utf8_lossy(&selected.body),
                String::from_utf8_lossy(&direct.body),
                "{name}: body"
            );
            if !credential.attributes.contains_key("cloak_mode") && name == "title-helper" {
                // The detection confirms the helper from the client's own body.
                assert_eq!(direct.header("x-claude-code-session-id"), Some(SESSION));
            }
        }
    }
}

/// `body` on a key with `model` through the contract path; returns the upstream body.
async fn contract(model: &str, body: &Value) -> Value {
    let upstream = StreamUpstream::start([Reply::json(MESSAGE)]).await;
    let fixture = Fixture::new(&upstream, model);
    let attempt = fixture.exchange.attempt(&fixture.streams.store);
    assert_eq!(fixture.executors.destination(&attempt), Some("anthropic"));
    let (started, _, _request) = fixture
        .anthropic(&serde_json::to_vec(body).unwrap(), Operation::Generate)
        .await;
    started.unwrap().producer.await.unwrap().unwrap();
    sent(&upstream).0
}

#[tokio::test]
async fn a_key_request_keeps_disabled_thinking_with_effort() {
    let body = json!({
        "model": OPUS_4_7, "max_tokens": 1024, "thinking": {"type": "disabled"},
        "output_config": {"effort": "low"}, "messages": [{"role": "user", "content": "hi"}],
    });
    let sent = contract(OPUS_4_7, &body).await;
    assert_eq!(sent["thinking"], json!({"type": "disabled"}));
    assert_eq!(sent["output_config"], json!({"effort": "low"}));
}

#[tokio::test]
async fn a_model_suffix_sets_thinking_as_on_the_old_route() {
    let model = format!("{SONNET}(8192)");
    let body = json!({"model": "m", "max_tokens": 16000, "messages": [{"role": "user", "content": "hi"}]});
    let new = contract(&model, &body).await;
    let upstream = StreamUpstream::start([Reply::json(MESSAGE)]).await;
    Fixture::new(&upstream, &model)
        .old_route(&serde_json::to_vec(&body).unwrap(), false)
        .await;
    assert_eq!(new, sent(&upstream).0);
    assert_eq!(new["thinking"]["budget_tokens"], 8192, "{new}");
    assert_eq!(new["model"], SONNET);
}

#[tokio::test]
async fn a_foreign_clients_upstream_streams_as_on_the_old_route() {
    let chat = json!({"model": "m", "max_completion_tokens": 300, "messages": [{"role": "user", "content": "hi"}]});
    assert_eq!(foreign("chat", chat).await["stream"], true);
}
