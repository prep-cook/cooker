//! Where the contract path must send what the old route sends: every request the OAuth cloak
//! shapes, compared old against new on the wire; and where it must not, a key request's
//! thinking and effort as the client wrote them.

use std::collections::BTreeMap;
use std::path::Path;

use ck_core::credential::Credential;

use super::support::upstream::Captured;
use super::*;

const OPUS_4_7: &str = "claude-opus-4-7";

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

/// The upstream request `body` produces with `headers` on an OAuth credential: through the
/// old route when `old`, else through the contract path.
async fn cloaked(model: &str, headers: &[(&str, &str)], body: &Value, old: bool) -> Captured {
    let reply = if body["stream"] == true {
        Reply::sse(SSE)
    } else {
        Reply::json(MESSAGE)
    };
    let upstream = StreamUpstream::start([reply]).await;
    let mut fixture = Fixture::with(oauth(), &upstream.url(), model);
    for (name, value) in headers {
        let name: http::HeaderName = name.parse().unwrap();
        fixture.exchange.headers.append(name, value.parse().unwrap());
    }
    let body = serde_json::to_vec(body).unwrap();
    if old {
        fixture.old_route(&body).await;
    } else {
        let (started, _, _request) = fixture.anthropic(&body, Operation::Generate).await;
        started.unwrap().producer.await.unwrap().unwrap();
    }
    upstream.captured().pop().expect("one upstream request")
}

/// Asserts the old and the new upstream request agree: path, every header, and the body
/// as JSON (the egress writes members in its own order).
async fn same_wire(name: &str, model: &str, headers: &[(&str, &str)], body: Value) -> Value {
    let old = cloaked(model, headers, &body, true).await;
    let new = cloaked(model, headers, &body, false).await;
    assert_eq!(new.path, old.path, "{name}: path");
    let map = |c: &Captured| -> BTreeMap<String, Vec<String>> {
        let mut map = BTreeMap::<String, Vec<String>>::new();
        // Each route has its own local upstream, so only `host` differs by design.
        for (k, v) in c.headers.iter().filter(|(k, _)| !k.eq_ignore_ascii_case("host")) {
            map.entry(k.to_ascii_lowercase()).or_default().push(v.clone());
        }
        map
    };
    assert_eq!(map(&new), map(&old), "{name}: headers");
    // `cch` signs the body bytes, whose member order the egress sets.
    let unsigned = |c: &Captured| -> Value {
        let text = String::from_utf8(c.body.clone()).unwrap();
        let at = text.find("cch=").map_or(text.len(), |at| at + 4);
        let text = format!("{}{}", &text[..at], text.get(at + 5..).unwrap_or_default());
        serde_json::from_str(&text).unwrap()
    };
    let (new, old) = (unsigned(&new), unsigned(&old));
    assert_eq!(new, old, "{name}: body");
    new
}

#[tokio::test]
async fn cloaked_requests_reach_the_upstream_as_on_the_old_route() {
    let tools = json!([{"name": "read", "description": "Read a file", "input_schema": {"type": "object"}}]);
    let messages = json!([{"role": "user", "content": "Summarise the plan in one line"}]);
    let cases = [
        (
            "plain",
            SONNET,
            json!({"model": SONNET, "max_tokens": 1024, "system": "Be brief.", "messages": messages}),
        ),
        (
            "plain-stream",
            SONNET,
            json!({"model": SONNET, "max_tokens": 1024, "stream": true, "messages": messages}),
        ),
        (
            "sampling",
            SONNET,
            json!({"model": SONNET, "max_tokens": 1024, "temperature": 0.5, "top_p": 0.9, "messages": messages}),
        ),
        (
            "manual-thinking-tools",
            SONNET,
            json!({"model": SONNET, "max_tokens": 4096, "thinking": {"type": "enabled", "budget_tokens": 2048},
                "tools": tools, "tool_choice": {"type": "auto"}, "messages": messages}),
        ),
        (
            "adaptive-forced-tool",
            OPUS_4_7,
            json!({"model": OPUS_4_7, "max_tokens": 4096, "thinking": {"type": "adaptive"},
                "tools": tools, "tool_choice": {"type": "tool", "name": "read"}, "messages": messages}),
        ),
        (
            "disabled-effort",
            OPUS_4_7,
            json!({"model": OPUS_4_7, "max_tokens": 4096, "thinking": {"type": "disabled"},
                "output_config": {"effort": "low"}, "messages": messages}),
        ),
    ];
    for (name, model, body) in cases {
        let sent = same_wire(name, model, &[], body).await;
        if name == "adaptive-forced-tool" {
            // The cloak drops thinking for a forced tool, as the old route does.
            assert!(sent.get("thinking").is_none(), "{sent}");
            assert_eq!(sent["tool_choice"]["type"], "tool");
        }
    }
}

#[tokio::test]
async fn a_key_request_keeps_disabled_thinking_with_effort() {
    let upstream = StreamUpstream::start([Reply::json(MESSAGE)]).await;
    let fixture = Fixture::new(&upstream, OPUS_4_7);
    let body = json!({
        "model": OPUS_4_7, "max_tokens": 1024, "thinking": {"type": "disabled"},
        "output_config": {"effort": "low"}, "messages": [{"role": "user", "content": "hi"}],
    });
    let (started, _, _request) = fixture
        .anthropic(&serde_json::to_vec(&body).unwrap(), Operation::Generate)
        .await;
    started.unwrap().producer.await.unwrap().unwrap();
    let (sent, _) = sent(&upstream);
    assert_eq!(sent["thinking"], json!({"type": "disabled"}));
    assert_eq!(sent["output_config"], json!({"effort": "low"}));
}

#[tokio::test]
async fn a_cloaked_buffered_answer_restores_the_tool_names() {
    let tools = json!([{"name": "read", "description": "Read a file", "input_schema": {"type": "object"}}]);
    let body = json!({"model": SONNET, "max_tokens": 1024, "tools": tools,
        "messages": [{"role": "user", "content": "Read the plan"}]});
    let alias = cloaked(SONNET, &[], &body, false).await;
    let alias: Value = serde_json::from_slice(&alias.body).unwrap();
    let alias = alias["tools"][0]["name"].as_str().unwrap().to_owned();
    assert_ne!(alias, "read");
    let answer = json!({"id": "msg_3", "type": "message", "role": "assistant", "model": SONNET,
        "content": [{"type": "tool_use", "id": "toolu_1", "name": alias, "input": {}}],
        "stop_reason": "tool_use", "stop_sequence": null, "usage": {"input_tokens": 3, "output_tokens": 2}});
    let upstream = StreamUpstream::start([Reply::json(answer.to_string())]).await;
    let fixture = Fixture::with(oauth(), &upstream.url(), SONNET);
    let (started, id, _request) = fixture
        .anthropic(&serde_json::to_vec(&body).unwrap(), Operation::Generate)
        .await;
    let mut started = started.unwrap();
    started.producer.await.unwrap().unwrap();
    let native = fixture.egress(id, &mut started.stream, BodyMode::Buffered).await;
    let native: Value = serde_json::from_slice(&native).unwrap();
    assert_eq!(native["content"][0]["name"], "read", "{native}");
}

#[tokio::test]
async fn a_foreign_clients_upstream_streams_as_on_the_old_route() {
    let chat = json!({"model": "m", "max_completion_tokens": 300, "messages": [{"role": "user", "content": "hi"}]});
    assert_eq!(foreign("chat", chat).await["stream"], true);
}
