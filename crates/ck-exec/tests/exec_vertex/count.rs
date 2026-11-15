//! Token counts on the Gemini route: Vertex's flat `countTokens` body each client shape sends,
//! the payload-rule order it shares with generation, and what each client egress makes of it.

use super::*;

/// A Vertex `countTokens` answer.
const COUNT: &str =
    r#"{"totalTokens":9,"totalBillableCharacters":20,"promptTokensDetails":[{"modality":"TEXT","tokenCount":9}]}"#;

async fn counted(streams: &Streams, exchange: &Exchange, client: Client, body: &str) -> Writer {
    let run = run_on(streams, exchange, client, Operation::CountTokens, body, 5).await;
    let (outcome, published, owner) = produce(streams, run.started.unwrap()).await;
    outcome.unwrap();
    assert_eq!(published.terminal, Some(Terminal::Done));
    owner
}

#[tokio::test]
async fn a_gemini_count_with_contents_only_is_sent_as_is() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::json(COUNT)]).await;
    let mut owner = counted(&streams, &exchange(&upstream), Client::Gemini, GEMINI).await;
    let captured = &upstream.captured()[0];
    assert_api_key_request(captured, &format!("{MODELS}:countTokens"));
    assert_eq!(json_body(captured), serde_json::from_str::<Value>(GEMINI).unwrap());
    let id = identity(Direction::Response);
    let mut egress =
        gemini::egress::Egress::new(&mut owner, id, Operation::CountTokens, BodyMode::Buffered, |_| {}).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&deliver(&streams, &mut owner, &mut egress)).unwrap(),
        serde_json::from_str::<Value>(COUNT).unwrap()
    );
}

/// The Gemini API's `generateContentRequest` unwraps into Vertex's flat request, which has
/// no member for safety settings or tool configuration and takes no call ids.
#[tokio::test]
async fn a_wrapped_gemini_count_goes_out_flat() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::json(COUNT)]).await;
    let body = concat!(
        r#"{"generateContentRequest":{"model":"models/alias","contents":[{"role":"user","parts":[{"text":"hi"}]},"#,
        r#"{"role":"model","parts":[{"functionCall":{"id":"call_1","name":"f","args":{}}}]},"#,
        r#"{"role":"user","parts":[{"functionResponse":{"id":"call_1","name":"f","response":{}}}]}],"#,
        r#""systemInstruction":{"parts":[{"text":"be brief"}]},"tools":[{"functionDeclarations":[{"name":"f"}]}],"#,
        r#""toolConfig":{"functionCallingConfig":{"mode":"AUTO"}},"#,
        r#""safetySettings":[{"category":"HARM_CATEGORY_HARASSMENT","threshold":"BLOCK_NONE"}]}}"#,
    );
    counted(&streams, &exchange(&upstream), Client::Gemini, body).await;
    assert_eq!(
        json_body(&upstream.captured()[0]),
        json!({
            "model": "publishers/google/models/gemini-2.5-pro",
            "contents": [
                {"role": "user", "parts": [{"text": "hi"}]},
                {"role": "model", "parts": [{"functionCall": {"name": "f", "args": {}}}]},
                {"role": "user", "parts": [{"functionResponse": {"name": "f", "response": {}}}]}
            ],
            "systemInstruction": {"parts": [{"text": "be brief"}]},
            "tools": [{"functionDeclarations": [{"name": "f"}]}]
        })
    );
}

#[tokio::test]
async fn an_anthropic_client_receives_the_count_as_input_tokens() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::json(COUNT)]).await;
    let body = r#"{"model":"claude-x","system":"be brief","messages":[{"role":"user","content":"hi"}]}"#;
    let mut owner = counted(&streams, &exchange(&upstream), Client::Anthropic, body).await;
    assert_eq!(
        json_body(&upstream.captured()[0]),
        json!({
            "contents": [{"role": "user", "parts": [{"text": "hi"}]}],
            "systemInstruction": {"parts": [{"text": "be brief"}]}
        })
    );
    let id = identity(Direction::Response);
    let mut egress =
        anthropic::egress::Egress::new(&mut owner, id, Operation::CountTokens, BodyMode::Buffered, |_| {}).unwrap();
    assert_eq!(deliver(&streams, &mut owner, &mut egress), r#"{"input_tokens":9}"#);
}

/// Payload rules are the last change, after the projection, as for generation.
#[tokio::test]
async fn count_payload_rules_run_last() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::json(COUNT)]).await;
    let mut exchange = exchange(&upstream);
    exchange.cfg = ck_core::config::Config::parse(
        "requests:\n  payload:\n    override:\n      - models: [{name: gemini-2.5-pro, protocol: gemini}]\n        params: {generationConfig.temperature: 0.25}\n",
    )
    .unwrap();
    counted(&streams, &exchange, Client::Gemini, GEMINI).await;
    assert_eq!(
        json_body(&upstream.captured()[0])["generationConfig"],
        json!({"temperature": 0.25})
    );
}

/// Fences a Gemini client's `operation` request on the Gemini route.
fn gemini_fence(operation: Operation, body: &str) -> Result<(), codec::Error> {
    let streams = Streams::new();
    let exchange = Exchange::new(credential("vertex", "http://127.0.0.1:1", KEY), MODEL, "/client");
    let attempt = exchange.attempt(&streams.store);
    let mut writer = streams.request();
    let id = identity(Direction::Request);
    let mut ingress = gemini::request::Ingress::new(&mut writer, &streams.store, id, operation, MODEL, LIMIT).unwrap();
    ingress.advance(&mut writer, body.as_bytes()).unwrap();
    executors().fence(&attempt, &mut ingress, &mut writer).map(drop)
}

/// Cached content changes the count, and Vertex's count request cannot name it: the fence
/// refuses such a count. A null one names nothing, and generation keeps it.
#[test]
fn a_count_naming_cached_content_is_refused() {
    let wrapped = |cached: &str| {
        format!(
            r#"{{"generateContentRequest":{{"cachedContent":{cached},"contents":[{{"role":"user","parts":[{{"text":"hi"}}]}}]}}}}"#
        )
    };
    let named = wrapped(r#""cachedContents/abc""#);
    assert!(matches!(
        gemini_fence(Operation::CountTokens, &named),
        Err(codec::Error::Unsupported)
    ));
    gemini_fence(Operation::CountTokens, &wrapped("null")).unwrap();
    let generate = r#"{"cachedContent":"cachedContents/abc","contents":[{"role":"user","parts":[{"text":"hi"}]}]}"#;
    gemini_fence(Operation::Generate, generate).unwrap();
}
