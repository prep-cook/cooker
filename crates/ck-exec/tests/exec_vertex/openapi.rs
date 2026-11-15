//! The `endpoints/openapi` route of partner and open models: the relay's Chat request at the
//! credential's base, a service account's fresh token or an API key, and the local count.

use super::*;

const PARTNER: &str = "deepseek-ai/deepseek-v3.1-maas";
const TOKEN: &str = r#"{"access_token":"ya29.vertex-test","token_type":"Bearer","expires_in":3600}"#;

fn chunk(delta: &str, finish: &str) -> String {
    format!(
        "data: {{\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"{PARTNER}\",\"choices\":[{{\"index\":0,\"delta\":{delta},\"finish_reason\":{finish}}}]}}\n\n"
    )
}

/// A complete Chat stream: "hel", "lo", a stop and usage, then `[DONE]`.
fn chat_sse() -> String {
    [
        chunk(r#"{"role":"assistant","content":"hel"}"#, "null"),
        chunk(r#"{"content":"lo"}"#, "null"),
        chunk("{}", r#""stop""#),
        format!("data: {{\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"{PARTNER}\",\"choices\":[],\"usage\":{{\"prompt_tokens\":3,\"completion_tokens\":2,\"total_tokens\":5}}}}\n\n"),
        "data: [DONE]\n\n".to_owned(),
    ]
    .concat()
}

/// A service-account credential whose token endpoint and `base_url` are the local upstream.
fn service_account(upstream: &StreamUpstream) -> Exchange {
    let fixture: Value = serde_json::from_str(include_str!("../fixtures/vertex_go.json")).unwrap();
    let mut credential = credential("vertex", &upstream.url(), "");
    credential.attributes.remove("api_key");
    credential.metadata.insert("project_id".into(), json!("proj"));
    credential.metadata.insert(
        "service_account".into(),
        json!({
            "type": "service_account",
            "project_id": "proj",
            "client_email": "runner@proj.iam.gserviceaccount.com",
            "private_key": fixture["keys"]["pkcs8"],
            "token_uri": format!("{}/token", upstream.url()),
        }),
    );
    Exchange::new(credential, PARTNER, "/client")
}

#[tokio::test]
async fn a_service_account_sends_chat_to_its_project_route() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::json(TOKEN), Reply::sse(chat_sse())]).await;
    let exchange = service_account(&upstream);
    let body = r#"{"model":"client-model","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    let run = run_on(&streams, &exchange, Client::Chat, Operation::Generate, body, 1).await;
    let (outcome, published, mut owner) = produce(&streams, run.started.unwrap()).await;
    outcome.unwrap();
    assert_eq!(published.terminal, Some(Terminal::Done));
    let captured = upstream.captured();
    assert_eq!(captured[0].path, "/token");
    let chat = &captured[1];
    assert_eq!(
        chat.path,
        "/v1/projects/proj/locations/us-central1/endpoints/openapi/chat/completions"
    );
    assert_eq!(chat.header("authorization"), Some("Bearer ya29.vertex-test"));
    assert_eq!(chat.header("accept"), Some("text/event-stream"));
    assert_eq!(chat.header("x-goog-api-key"), None);
    assert_eq!(
        json_body(chat),
        json!({
            "model": PARTNER,
            "stream": true,
            "messages": [{"role": "user", "content": "hi"}],
            "stream_options": {"include_usage": true}
        })
    );
    assert_every_egress(
        &streams,
        &mut owner,
        BodyMode::Streaming,
        "hel",
        ["stop", "end_turn", "completed", "STOP"],
    );
}

/// An API key reaches a third-party relay with the normalized `<publisher>/<model>`; a Gemini
/// client streams upstream.
#[tokio::test]
async fn an_api_key_reaches_its_relay_with_the_normalized_model() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::sse(chat_sse())]).await;
    let base = format!("{}/api", upstream.url());
    let mut exchange = Exchange::new(
        credential("vertex", &base, KEY),
        "publishers/Meta/models/llama-4",
        "/client",
    );
    exchange.headers.insert("x-client", "kept".parse().unwrap());
    exchange
        .credential
        .attributes
        .insert("header:X-Team".into(), "core".into());
    let run = run_on(&streams, &exchange, Client::Gemini, Operation::Generate, GEMINI, 3).await;
    produce(&streams, run.started.unwrap()).await.0.unwrap();
    let captured = &upstream.captured()[0];
    assert_eq!(captured.path, "/api/v1/endpoints/openapi/chat/completions");
    assert_eq!(captured.header("authorization"), Some(&*format!("Bearer {KEY}")));
    assert_eq!(captured.header("x-team"), Some("core"));
    let body = json_body(captured);
    assert_eq!(body["model"], "meta/llama-4");
    assert_eq!(body["stream"], true);
}

#[tokio::test]
async fn an_api_key_on_google_hosts_is_refused_before_sending() {
    let streams = Streams::new();
    let exchange = Exchange::new(
        credential("vertex", "https://aiplatform.googleapis.com", KEY),
        PARTNER,
        "/client",
    );
    let body = r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#;
    let error = run_on(&streams, &exchange, Client::Chat, Operation::Generate, body, 64)
        .await
        .started
        .err()
        .unwrap();
    assert_eq!(error.status, 400);
}

#[tokio::test]
async fn a_failed_token_exchange_sends_nothing_else() {
    let streams = Streams::new();
    let upstream =
        StreamUpstream::start([Reply::status(401, "application/json", r#"{"error":"invalid_grant"}"#)]).await;
    let exchange = service_account(&upstream);
    let body = r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#;
    let error = run_on(&streams, &exchange, Client::Chat, Operation::Generate, body, 64)
        .await
        .started
        .err()
        .unwrap();
    assert_eq!(error.status, 500);
    assert!(!String::from_utf8_lossy(&error.body).contains("invalid_grant"));
    assert_eq!(upstream.requests(), 1);
}

#[tokio::test]
async fn an_upstream_refusal_fails_the_attempt() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::status(429, "application/json", r#"{"error":"busy"}"#)]).await;
    let exchange = Exchange::new(credential("vertex", &upstream.url(), KEY), PARTNER, "/client");
    let body = r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#;
    let error = run_on(&streams, &exchange, Client::Chat, Operation::Generate, body, 64)
        .await
        .started
        .err()
        .unwrap();
    assert_eq!(error.status, 429);
}

/// A count is local: nothing goes upstream, not even a token exchange.
#[tokio::test]
async fn a_count_is_local() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([]).await;
    let exchange = service_account(&upstream);
    let body = r#"{"model":"claude-x","messages":[{"role":"user","content":"hello there"}]}"#;
    let run = run_on(&streams, &exchange, Client::Anthropic, Operation::CountTokens, body, 4).await;
    let (outcome, published, mut owner) = produce(&streams, run.started.unwrap()).await;
    outcome.unwrap();
    assert_eq!(published.terminal, Some(Terminal::Done));
    assert_eq!(upstream.requests(), 0);
    let id = identity(Direction::Response);
    let mut egress =
        anthropic::egress::Egress::new(&mut owner, id, Operation::CountTokens, BodyMode::Buffered, |_| {}).unwrap();
    let counted: Value = serde_json::from_str(&deliver(&streams, &mut owner, &mut egress)).unwrap();
    assert!(counted["input_tokens"].as_u64().unwrap() > 0, "{counted}");
}

/// The count measures the body generation would send: payload rules see the normalized model.
#[tokio::test]
async fn a_count_sees_the_normalized_model() {
    let streams = Streams::new();
    let body = r#"{"model":"claude-x","messages":[{"role":"user","content":"hi"}]}"#;
    let mut tokens = Vec::new();
    for rules in [
        "",
        "requests:\n  payload:\n    override:\n      - models: [{name: meta/llama-4}]\n        params: {messages.0.content: one two three four five six seven eight nine ten}\n",
    ] {
        let mut exchange = Exchange::new(
            credential("vertex", "http://relay.example", KEY),
            "publishers/meta/models/llama-4",
            "/client",
        );
        exchange.cfg = ck_core::config::Config::parse(rules).unwrap();
        let run = run_on(&streams, &exchange, Client::Anthropic, Operation::CountTokens, body, 64).await;
        let (outcome, published, _) = produce(&streams, run.started.unwrap()).await;
        outcome.unwrap();
        let record = kinds(&published);
        assert!(record.iter().any(|k| k == "x_ck.count_tokens"), "{record:?}");
        tokens.push(String::from_utf8(published.bytes()).unwrap());
    }
    assert_ne!(tokens[0], tokens[1]);
}

/// The model suffix's thinking applies; a relay compatibility entry that happens to share the
/// provider's name does not, as on the old route.
#[tokio::test]
async fn the_route_applies_thinking_without_relay_compatibility() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::sse(chat_sse())]).await;
    let base = format!("{}/api", upstream.url());
    let mut exchange = Exchange::new(credential("vertex", &base, KEY), &format!("{PARTNER}(high)"), "/client");
    exchange.cfg = ck_core::config::Config::parse(concat!(
        "api-keys:\n  openai-compatibility:\n    - name: vertex\n      base-url: http://relay\n      keys: []\n",
        "      models:\n        - name: deepseek-ai/deepseek-v3.1-maas\n          use-max-completion-tokens: true\n",
    ))
    .unwrap();
    let body = r#"{"model":"m","max_tokens":9,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    let run = run_on(&streams, &exchange, Client::Chat, Operation::Generate, body, 64).await;
    produce(&streams, run.started.unwrap()).await.0.unwrap();
    let sent = json_body(&upstream.captured()[0]);
    assert_eq!(sent["model"], PARTNER, "{sent}");
    assert_eq!(sent["reasoning_effort"], "high", "{sent}");
    assert_eq!(sent["max_tokens"], 9, "{sent}");
    assert!(sent.get("max_completion_tokens").is_none(), "{sent}");
}
