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

/// A Title280 helper body: Claude Code's member order and shape.
fn title_body() -> Value {
    let user_id = format!(
        r#"{{"device_id":"{}","account_uuid":"","session_id":"{SESSION}"}}"#,
        "a".repeat(64)
    );
    json!({"model": HAIKU, "max_tokens": 80, "messages": [{"role": "user", "content": "title"}],
        "metadata": {"user_id": user_id},
        "output_config": {"format": {"type": "json_schema", "schema": {"type": "object"}}}})
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
        ("title-helper", HAIKU, title_helper(), title_body()),
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

/// `body` on a key with `model` through the old route; returns the upstream body.
async fn old(model: &str, body: &Value) -> Value {
    let upstream = StreamUpstream::start([Reply::json(MESSAGE)]).await;
    Fixture::new(&upstream, model)
        .old_route(&serde_json::to_vec(body).unwrap(), false)
        .await;
    sent(&upstream).0
}

#[tokio::test]
async fn a_model_suffix_sets_thinking_as_on_the_old_route() {
    let model = format!("{SONNET}(8192)");
    let body = json!({"model": "m", "max_tokens": 16000, "messages": [{"role": "user", "content": "hi"}]});
    let new = contract(&model, &body).await;
    assert_eq!(new, old(&model, &body).await);
    assert_eq!(new["thinking"]["budget_tokens"], 8192, "{new}");
    assert_eq!(new["model"], SONNET);
}

#[tokio::test]
async fn a_foreign_clients_upstream_streams_as_on_the_old_route() {
    let chat = json!({"model": "m", "max_completion_tokens": 300, "messages": [{"role": "user", "content": "hi"}]});
    assert_eq!(foreign("chat", chat).await["stream"], true);
}

fn marked_block(text: &str, ttl: &str) -> Value {
    json!({"type": "text", "text": text, "cache_control": {"type": "ephemeral", "ttl": ttl}})
}

/// The cache rewrites change only the caller's cost, so a key request gets the old route's
/// (owner question 14): markers where the client set none, at most four, TTLs in order.
#[tokio::test]
async fn a_key_request_gets_the_old_routes_cache_rewrites() {
    let plain = json!({"model": SONNET, "max_tokens": 1024, "system": "Be brief.",
        "messages": [{"role": "user", "content": "hi"}]});
    let new = contract(SONNET, &plain).await;
    assert!(new.to_string().contains("cache_control"), "{new}");
    assert_eq!(new, old(SONNET, &plain).await);
    let marked = marked_block;
    let system = json!([
        marked("a", "5m"),
        marked("b", "1h"),
        marked("c", "1h"),
        marked("d", "1h"),
        marked("e", "1h")
    ]);
    let marked = json!({"model": SONNET, "max_tokens": 1024, "system": system,
        "messages": [{"role": "user", "content": "hi"}]});
    let new = contract(SONNET, &marked).await;
    assert!(new.to_string().matches("cache_control").count() <= 4, "{new}");
    assert_eq!(new, old(SONNET, &marked).await);
    let system = json!([marked_block("a", "5m"), marked_block("b", "1h")]);
    let ordered = json!({"model": SONNET, "max_tokens": 1024, "system": system,
        "messages": [{"role": "user", "content": "hi"}]});
    let new = contract(SONNET, &ordered).await;
    assert_ne!(
        new["system"], ordered["system"],
        "a 1h marker after a 5m one is reordered"
    );
    assert_eq!(new, old(SONNET, &ordered).await);
}

/// Thinking replay may store only a response known complete, which the contract path cannot
/// see, so a credential replay serves stays on the old route.
#[tokio::test]
async fn thinking_replay_credentials_stay_on_the_old_route() {
    use ck_core::exec::{ResolvedModel, ResolvedSource};
    let fixture = Fixture::new(&StreamUpstream::start([]).await, SONNET);
    let mut key = fixture.exchange.credential.clone();
    key.attributes.insert("auth_kind".into(), "apikey".into());
    let info = json!({"id": SONNET, "is_compat": true}).as_object().unwrap().clone();
    let compat = ResolvedModel {
        info: ck_core::registry::ModelInfo::from_raw(info).unwrap(),
        source: ResolvedSource::ApiKey,
    };
    let attempt = |resolved_model| ck_exec::types::Attempt {
        credential: &key,
        cfg: &fixture.exchange.cfg,
        model: SONNET,
        headers: &fixture.exchange.headers,
        resolved_model,
        store: &fixture.streams.store,
        identity: identity(Direction::Response),
    };
    assert_eq!(fixture.executors.destination(&attempt(Some(&compat))), None);
    assert_eq!(fixture.executors.destination(&attempt(None)), Some("anthropic"));
}

/// No signature sanitizing or proxy-owned prefix recovery: the official API judges the
/// client's history and signed thinking.
#[tokio::test]
async fn a_key_request_sends_history_and_signed_thinking_unchanged() {
    let assistant = json!({"role": "assistant", "content": [
        {"type": "thinking", "thinking": "t", "signature": "c2lnbmVk"}, {"type": "text", "text": "ok"}]});
    let messages = json!([{"role": "user", "content": "hi"}, assistant, {"role": "user", "content": "more"}]);
    let body = json!({"model": OPUS_4_7, "max_tokens": 4096, "thinking": {"type": "adaptive"}, "messages": messages});
    let sent = contract(OPUS_4_7, &body).await;
    assert_eq!(sent["messages"][0], body["messages"][0]);
    assert_eq!(sent["messages"][1], body["messages"][1]);
    assert_eq!(sent["thinking"], json!({"type": "adaptive"}));
}

/// A request carrying any Claude Code client signal stays on the old route even on a plain
/// key: detection reads the client's own body, which the contract path does not have (G6).
#[tokio::test]
async fn a_claude_code_request_on_a_plain_key_stays_on_the_old_route() {
    let key = credential("claude", "http://127.0.0.1:1", "sk-ant-api03-test");
    let selected = old_wire(&key, HAIKU, &title_helper(), &title_body(), true).await;
    let direct = old_wire(&key, HAIKU, &title_helper(), &title_body(), false).await;
    assert_eq!(selected.path, direct.path);
    assert_eq!(headers(&selected), headers(&direct));
    assert_eq!(selected.body, direct.body);
    // The old route confirms the helper: its body goes up without cache markers or `stream`.
    let sent: Value = serde_json::from_slice(&direct.body).unwrap();
    assert!(
        sent.get("stream").is_none() && !sent.to_string().contains("cache_control"),
        "{sent}"
    );
    let signals = [
        ("user-agent", "claude-cli/2.1.280 (external, cli)"),
        ("x-app", "cli"),
        ("x-claude-code-session-id", SESSION),
        ("x-stainless-lang", "js"),
        ("anthropic-beta", "claude-code-20250219"),
        ("anthropic-beta", "oauth-2025-04-20"),
    ];
    let mut fixture = Fixture::new(&StreamUpstream::start([]).await, HAIKU);
    for (name, value) in signals {
        fixture.exchange.headers = http::HeaderMap::from_iter([(name.parse().unwrap(), value.parse().unwrap())]);
        let attempt = fixture.exchange.attempt(&fixture.streams.store);
        assert_eq!(fixture.executors.destination(&attempt), None, "{name}");
    }
    fixture.exchange.headers = http::HeaderMap::from_iter([(http::header::USER_AGENT, "my-app/1.0".parse().unwrap())]);
    let attempt = fixture.exchange.attempt(&fixture.streams.store);
    assert_eq!(fixture.executors.destination(&attempt), Some("anthropic"));
}

/// A plain key on a Kimi origin keeps the caller's system text, even one that starts like
/// Claude Code's billing block; only the old route strips that attribution.
#[tokio::test]
async fn a_kimi_origin_keeps_the_callers_system_text() {
    let text = "x-anthropic-billing-header: preserve this caller text";
    for system in [json!(text), json!([{"type": "text", "text": text}])] {
        // Requests to the Kimi origin reach the local upstream through it as an HTTP proxy.
        let upstream = StreamUpstream::start([Reply::json(MESSAGE)]).await;
        let mut key = credential("claude", "http://api.kimi.com/anthropic", "sk-kimi-test");
        key.attributes.insert("proxy_url".into(), upstream.url());
        let fixture = Fixture::with(key, "http://127.0.0.1:1", SONNET);
        let attempt = fixture.exchange.attempt(&fixture.streams.store);
        assert_eq!(fixture.executors.destination(&attempt), Some("anthropic"));
        let body = json!({"model": SONNET, "max_tokens": 1024, "system": system,
            "messages": [{"role": "user", "content": "hi"}]});
        let (started, _, _request) = fixture
            .anthropic(&serde_json::to_vec(&body).unwrap(), Operation::Generate)
            .await;
        started.unwrap().producer.await.unwrap().unwrap();
        let (sent, captured) = sent(&upstream);
        assert!(captured.path.contains("api.kimi.com"), "{}", captured.path);
        assert!(sent["system"].to_string().contains(text), "{sent}");
    }
}
