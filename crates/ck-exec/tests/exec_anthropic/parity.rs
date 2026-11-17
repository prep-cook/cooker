//! A key request on the contract path sends the client's thinking and effort as written, with
//! only the operator's model-name thinking suffix applied, and keeps the old route's cache
//! rewrites. Requests the cloak shapes are in `cloak.rs`.

use super::*;

const OPUS_4_7: &str = "claude-opus-4-7";

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

/// The upstream requests a sink captured, as (url, body).
#[derive(Default)]
struct Sent(std::sync::Mutex<Vec<(String, Vec<u8>)>>);

impl ck_core::exec::CaptureObserver for Sent {
    fn record(&self, event: ck_core::exec::CaptureEvent<'_>) {
        if let ck_core::exec::CaptureEvent::Request(request) = event {
            let entry = (request.url.to_owned(), request.body.to_vec());
            self.0.lock().unwrap().push(entry);
        }
    }
}

/// count_tokens counts the context after the caller's edits, so a first-party count keeps
/// `context_management`; it takes no `metadata`.
#[tokio::test]
async fn a_first_party_count_keeps_the_callers_context_management() {
    let mut key = credential("claude", "https://api.anthropic.com", "sk-ant-api03-test");
    // The proxy refuses every connection: the count is captured, and nothing leaves the host.
    key.attributes.insert("proxy_url".into(), "http://127.0.0.1:1".into());
    let mut fixture = Fixture::with(key, "http://127.0.0.1:1", SONNET);
    let beta = "context-management-2025-06-27".parse().unwrap();
    fixture.exchange.headers.insert("anthropic-beta", beta);
    let sent = std::sync::Arc::new(Sent::default());
    let sink = ck_core::exec::CaptureSink::new(sent.clone());
    fixture.exchange.usage = UsageSink::default().with_capture(sink);
    let edits = json!({"edits": [{"type": "clear_tool_uses_20250919",
        "trigger": {"type": "input_tokens", "value": 30000}, "keep": {"type": "tool_uses", "value": 5}}]});
    let body = json!({"model": SONNET, "messages": [{"role": "user", "content": "hi"}],
        "context_management": edits, "metadata": {"user_id": "u-1"}});
    let (started, _, _request) = fixture
        .anthropic(&serde_json::to_vec(&body).unwrap(), Operation::CountTokens)
        .await;
    assert!(started.is_err(), "the loopback proxy refuses the connection");
    let (url, request) = sent.0.lock().unwrap().pop().expect("the prepared count request");
    assert!(
        url.starts_with("https://api.anthropic.com/v1/messages/count_tokens"),
        "{url}"
    );
    let request: Value = serde_json::from_slice(&request).unwrap();
    assert_eq!(request["context_management"], edits);
    assert!(request.get("metadata").is_none(), "{request}");
}
