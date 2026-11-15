//! Token counts: the `countTokens` body each client shape sends, the payload-rule order it
//! shares with generation, and what each client egress makes of a Gemini count.

use super::*;

#[tokio::test]
async fn a_gemini_count_with_contents_only_is_sent_as_is() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::json(r#"{"totalTokens":7}"#)]).await;
    let body = r#"{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}"#;
    let run = run(&streams, &upstream, Client::Gemini, Operation::CountTokens, body, 1).await;
    let (outcome, published, _) = produce(&streams, run.started.unwrap()).await;
    outcome.unwrap();
    assert_eq!(published.terminal, Some(Terminal::Done));
    let captured = &upstream.captured()[0];
    assert_api_key_request(captured, "/v1beta/models/gemini-2.5-pro:countTokens");
    assert_eq!(json_body(captured), serde_json::from_str::<Value>(body).unwrap());
}

/// A native `countTokens` answer with every member the count response has.
const COUNT: &str =
    r#"{"totalTokens":9,"cachedContentTokenCount":2,"promptTokensDetails":[{"modality":"TEXT","tokenCount":9}]}"#;

/// Runs a count from `client` against an upstream answering [`COUNT`] and returns the
/// response stream's allocator for client egress.
async fn counted(streams: &Streams, client: Client, body: &str) -> Writer {
    let upstream = StreamUpstream::start([Reply::json(COUNT)]).await;
    let run = run(streams, &upstream, client, Operation::CountTokens, body, 5).await;
    let (outcome, published, owner) = produce(streams, run.started.unwrap()).await;
    outcome.unwrap();
    assert_eq!(published.terminal, Some(Terminal::Done));
    owner
}

#[tokio::test]
async fn a_gemini_client_receives_the_count_json_equal() {
    let streams = Streams::new();
    let body = r#"{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}"#;
    let mut owner = counted(&streams, Client::Gemini, body).await;
    let id = identity(Direction::Response);
    let mut egress =
        gemini::egress::Egress::new(&mut owner, id, Operation::CountTokens, BodyMode::Buffered, |_| {}).unwrap();
    let delivered = deliver(&streams, &mut owner, &mut egress);
    assert_eq!(
        serde_json::from_str::<Value>(&delivered).unwrap(),
        serde_json::from_str::<Value>(COUNT).unwrap()
    );
}

#[tokio::test]
async fn an_anthropic_client_receives_the_count_as_input_tokens() {
    let streams = Streams::new();
    let body = r#"{"model":"claude-x","messages":[{"role":"user","content":"hi"}]}"#;
    let mut owner = counted(&streams, Client::Anthropic, body).await;
    let id = identity(Direction::Response);
    let mut egress =
        anthropic::egress::Egress::new(&mut owner, id, Operation::CountTokens, BodyMode::Buffered, |_| {}).unwrap();
    assert_eq!(deliver(&streams, &mut owner, &mut egress), r#"{"input_tokens":9}"#);
}

/// Chat has no count operation: its request ingress refuses one, so no Chat client can
/// ask, and its response egress refuses to encode one.
#[tokio::test]
async fn chat_has_no_count_to_receive() {
    let streams = Streams::new();
    let mut writer = streams.request();
    let id = identity(Direction::Request);
    let ingress = chat::request::Ingress::new(&mut writer, &streams.store, id, Operation::CountTokens, LIMIT);
    assert!(matches!(ingress.err(), Some(codec::Error::Unsupported)));
    let id = identity(Direction::Response);
    let egress = chat::egress::Egress::new(
        &mut writer,
        id,
        Operation::CountTokens,
        BodyMode::Buffered,
        true,
        |_| {},
    );
    assert!(matches!(egress.err(), Some(codec::Error::Unsupported)));
}

/// Responses has no count operation in the codec: its request ingress refuses one, and its
/// response egress refuses to encode one.
#[tokio::test]
async fn responses_has_no_count_to_receive() {
    let streams = Streams::new();
    let mut writer = streams.request();
    let id = identity(Direction::Request);
    let ingress = responses::request::Ingress::new(&mut writer, &streams.store, id, Operation::CountTokens, LIMIT);
    assert!(matches!(ingress.err(), Some(codec::Error::Unsupported)));
    let egress = responses::egress::Egress::new(&mut writer, Operation::CountTokens, BodyMode::Buffered, |_, _| {});
    assert!(matches!(egress.err(), Some(codec::Error::Unsupported)));
}

#[tokio::test]
async fn a_count_with_a_system_prompt_carries_the_generation_request() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::json(r#"{"totalTokens":9}"#)]).await;
    let body = r#"{"model":"claude-x","system":"be brief","messages":[{"role":"user","content":"hi"}]}"#;
    let run = run(&streams, &upstream, Client::Anthropic, Operation::CountTokens, body, 2).await;
    let (outcome, published, _) = produce(&streams, run.started.unwrap()).await;
    outcome.unwrap();
    assert_eq!(published.terminal, Some(Terminal::Done));
    let captured = &upstream.captured()[0];
    assert_api_key_request(captured, "/v1beta/models/gemini-2.5-pro:countTokens");
    assert_eq!(
        json_body(captured),
        json!({"generateContentRequest": {
            "contents": [{"role": "user", "parts": [{"text": "hi"}]}],
            "systemInstruction": {"parts": [{"text": "be brief"}]},
            "model": "models/gemini-2.5-pro"
        }})
    );
}

#[tokio::test]
async fn a_wrapped_gemini_count_names_the_upstream_model() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::json(r#"{"totalTokens":4}"#)]).await;
    let body =
        r#"{"generateContentRequest":{"model":"models/alias","contents":[{"role":"user","parts":[{"text":"hi"}]}]}}"#;
    let run = run(&streams, &upstream, Client::Gemini, Operation::CountTokens, body, 3).await;
    produce(&streams, run.started.unwrap()).await.0.unwrap();
    let request = json_body(&upstream.captured()[0]);
    assert_eq!(request["generateContentRequest"]["model"], "models/gemini-2.5-pro");
    assert_eq!(
        request["generateContentRequest"]["contents"][0]["parts"][0]["text"],
        "hi"
    );
}

/// A payload rule matching the upstream model in the body applies to generation and to
/// every count shape alike, and its own `model` override survives the count envelope.
#[tokio::test]
async fn count_payload_rules_see_the_upstream_model_as_generation_does() {
    let streams = Streams::new();
    let replies = [
        Reply::sse(SSE),
        Reply::json(COUNT),
        Reply::json(COUNT),
        Reply::sse(SSE),
        Reply::json(COUNT),
        Reply::json(COUNT),
    ];
    let upstream = StreamUpstream::start(replies).await;
    let mut exchange = exchange(&upstream);
    let rule = |params: &str| {
        ck_core::config::Config::parse(&format!(
            "requests:\n  payload:\n    override:\n      - models: [{{name: gemini-2.5-pro, protocol: gemini, match: [{{model: models/gemini-2.5-pro}}]}}]\n        params: {{{params}}}\n"
        ))
        .unwrap()
    };
    let contents = r#""contents":[{"role":"user","parts":[{"text":"hi"}]}]"#;
    let generate = format!(r#"{{"model":"models/alias",{contents}}}"#);
    let wrapped = format!(r#"{{"generateContentRequest":{{"model":"models/alias",{contents}}}}}"#);
    let named = format!(r#"{{"model":"models/alias",{contents}}}"#);
    for params in ["systemInstruction.parts.0.text: be brief", "model: models/override"] {
        exchange.cfg = rule(params);
        for (operation, body) in [
            (Operation::Generate, &generate),
            (Operation::CountTokens, &wrapped),
            (Operation::CountTokens, &named),
        ] {
            let run = run_on(&streams, &exchange, Client::Gemini, operation, body, 64).await;
            produce(&streams, run.started.unwrap()).await.0.unwrap();
        }
    }
    let captured = upstream.captured();
    let request = |i: usize| {
        let body = json_body(&captured[i]);
        match body.get("generateContentRequest") {
            Some(request) => request.clone(),
            None => body,
        }
    };
    for i in 0..3 {
        assert_eq!(
            request(i)["systemInstruction"]["parts"][0]["text"],
            "be brief",
            "request {i}"
        );
        assert_eq!(request(i)["model"], "models/gemini-2.5-pro", "request {i}");
    }
    for i in 3..6 {
        assert_eq!(request(i)["model"], "models/override", "request {i}");
    }
}
