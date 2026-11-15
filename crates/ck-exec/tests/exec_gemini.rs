//! The Gemini API-key executor through the pipeline against a local upstream: the request
//! each client protocol sends, the canonical response every client egress accepts, the
//! upstream token count, and failures before and after the upstream answered.

mod support;

use ck_core::exec::{ExecError, FailureScope};
use ck_exec::Executors;
use ck_exec::pipeline::Started;
use ck_stream::{Read, ScratchLease, Terminal, Writer};
use ck_translate::anthropic;
use ck_translate::codec::chat;
use ck_translate::codec::event::Direction;
use ck_translate::codec::{self, BodyMode, Operation, RequestIngress, ResponseEgress};
use ck_translate::gemini;
use ck_translate::responses;
use serde_json::{Value, json};
use support::upstream::{Captured, Reply, StreamUpstream};
use support::{Exchange, Published, Streams, credential, identity};

const MODEL: &str = "gemini-2.5-pro";
const KEY: &str = "AIza-test";
const LIMIT: u64 = 1 << 20;

const SSE: &str = concat!(
    "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"Hel\"}]},\"index\":0}],",
    "\"modelVersion\":\"gemini-2.5-pro\",\"responseId\":\"r1\"}\r\n\r\n",
    "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"lo\"}]},\"finishReason\":\"STOP\",",
    "\"index\":0}],\"usageMetadata\":{\"promptTokenCount\":3,\"candidatesTokenCount\":2,\"totalTokenCount\":5},",
    "\"modelVersion\":\"gemini-2.5-pro\",\"responseId\":\"r1\"}\r\n\r\n",
);
const JSON: &str = concat!(
    "{\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"Hello\"}]},\"finishReason\":\"STOP\",",
    "\"index\":0}],\"usageMetadata\":{\"promptTokenCount\":3,\"candidatesTokenCount\":2,\"totalTokenCount\":5},",
    "\"modelVersion\":\"gemini-2.5-pro\",\"responseId\":\"r1\"}",
);

/// One client request in its own protocol.
#[derive(Clone, Copy)]
enum Client {
    Chat,
    Anthropic,
    Responses,
    Gemini,
}

/// One attempt's outcome, with the request stream the response stream depends on.
struct Run {
    started: Result<Started, ExecError>,
    _request: Writer,
}

/// The production selector; only its Gemini arm runs here.
fn executors() -> Executors {
    Executors {
        claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
        codex: Default::default(),
        devices: Default::default(),
        openai: Default::default(),
        google: Default::default(),
    }
}

/// Fences `body` for the Gemini destination through the selector, fed `step` bytes at a time, and starts the
/// attempt through the pipeline.
async fn run_with<I: RequestIngress>(
    streams: &Streams,
    exchange: &Exchange,
    mut writer: Writer,
    mut ingress: I,
    body: &[u8],
    step: usize,
) -> Run {
    let response = streams.response(&writer);
    let attempt = exchange.attempt(&streams.store);
    for chunk in body.chunks(step) {
        ingress.advance(&mut writer, chunk).unwrap();
    }
    let executors = executors();
    assert_eq!(executors.destination(&attempt), Some("gemini"));
    let validated = executors
        .fence(&attempt, &mut ingress, &mut writer)
        .expect("the fence accepts");
    let validated = executors
        .refence(&attempt, validated.view())
        .expect("the re-fence accepts");
    let request = exchange.request(validated);
    let started = executors.start(&attempt, &request, &mut writer, response).await;
    Run {
        started,
        _request: writer,
    }
}

/// The default attempt: a `gemini` key on `upstream` for [`MODEL`].
fn exchange(upstream: &StreamUpstream) -> Exchange {
    Exchange::new(credential("gemini", &upstream.url(), KEY), MODEL, "/client")
}

async fn run(
    streams: &Streams,
    upstream: &StreamUpstream,
    client: Client,
    operation: Operation,
    body: &str,
    step: usize,
) -> Run {
    run_on(streams, &exchange(upstream), client, operation, body, step).await
}

async fn run_on(
    streams: &Streams,
    exchange: &Exchange,
    client: Client,
    operation: Operation,
    body: &str,
    step: usize,
) -> Run {
    let (store, id) = (&streams.store, identity(Direction::Request));
    let mut writer = streams.request();
    let body = body.as_bytes();
    match client {
        Client::Chat => {
            let ingress = chat::request::Ingress::new(&mut writer, store, id, operation, LIMIT).unwrap();
            run_with(streams, exchange, writer, ingress, body, step).await
        }
        Client::Anthropic => {
            let ingress = anthropic::request::Ingress::new(&mut writer, store, id, operation, LIMIT).unwrap();
            run_with(streams, exchange, writer, ingress, body, step).await
        }
        Client::Responses => {
            let ingress = responses::request::Ingress::new(&mut writer, store, id, operation, LIMIT).unwrap();
            run_with(streams, exchange, writer, ingress, body, step).await
        }
        Client::Gemini => {
            let ingress = gemini::request::Ingress::new(&mut writer, store, id, operation, MODEL, LIMIT).unwrap();
            run_with(streams, exchange, writer, ingress, body, step).await
        }
    }
}

/// Runs the producer to its end: its outcome and every record it published.
async fn produce(streams: &Streams, started: Started) -> (Result<(), ExecError>, Published, Writer) {
    let published = streams.read_to_end(started.stream.id()).await;
    let outcome = started.producer.await.expect("the producer does not panic");
    (outcome, published, started.stream)
}

fn json_body(captured: &Captured) -> Value {
    serde_json::from_slice(&captured.body).expect("the upload is JSON")
}

/// Checks the transport facts every Gemini API-key request carries.
fn assert_api_key_request(captured: &Captured, path: &str) {
    assert_eq!(captured.method, "POST");
    assert_eq!(captured.path, path);
    assert_eq!(captured.header("x-goog-api-key"), Some(KEY));
    // The transport's default, as the old path sends it.
    assert_eq!(captured.header("user-agent"), Some("Go-http-client/1.1"));
    assert_eq!(captured.header("content-type"), Some("application/json"));
    assert_eq!(captured.header("authorization"), None);
}

/// Records with `event: <kind>`.
fn kinds(published: &Published) -> Vec<String> {
    published
        .records
        .iter()
        .filter_map(|r| {
            let text = String::from_utf8_lossy(r);
            text.lines().find_map(|l| l.strip_prefix("event: ").map(str::to_owned))
        })
        .collect()
}

fn lease_bytes(lease: &ScratchLease) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(lease.len());
    while bytes.len() < lease.len() {
        bytes.extend_from_slice(lease.chunk(bytes.len()).unwrap());
    }
    bytes
}

/// Encodes the response stream `id` with `egress` and returns the client bytes.
fn deliver(streams: &Streams, owner: &mut Writer, egress: &mut impl ResponseEgress) -> String {
    let mut reader = streams.reader(owner.id());
    let mut out = Vec::new();
    while let std::task::Poll::Ready(Some(read)) = reader.try_next() {
        match read {
            Read::Record(lease) => egress
                .advance(owner, lease, |bytes| {
                    out.extend_from_slice(&lease_bytes(&bytes));
                    Ok(())
                })
                .expect("the client egress accepts the record"),
            Read::Terminal(terminal) => assert_eq!(terminal, Terminal::Done),
        }
    }
    egress.finish().expect("the client egress finishes");
    String::from_utf8(out).unwrap()
}

/// The text a client egress wrote, from every protocol's output.
fn assert_every_egress(streams: &Streams, owner: &mut Writer, mode: BodyMode) {
    let id = identity(Direction::Response);
    let op = Operation::Generate;
    let mut chat = chat::egress::Egress::new(owner, id, op, mode, true, |_| {}).unwrap();
    let out = deliver(streams, owner, &mut chat);
    assert!(out.contains("Hel") && out.contains("stop"), "chat: {out}");
    let mut messages = anthropic::egress::Egress::new(owner, id, op, mode, |_| {}).unwrap();
    let out = deliver(streams, owner, &mut messages);
    assert!(out.contains("Hel") && out.contains("end_turn"), "anthropic: {out}");
    let mut responses = crate::support::responses_egress(owner, op, mode).unwrap();
    let out = deliver(streams, owner, &mut responses);
    assert!(out.contains("Hel") && out.contains("completed"), "responses: {out}");
    let mut native = gemini::egress::Egress::new(owner, id, op, mode, |_| {}).unwrap();
    let out = deliver(streams, owner, &mut native);
    assert!(out.contains("Hel") && out.contains("STOP"), "gemini: {out}");
}

#[tokio::test]
async fn chat_streams_through_stream_generate_content() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::sse(SSE)]).await;
    let body = r#"{"model":"client-model","stream":true,"max_tokens":64,"messages":[{"role":"system","content":"be brief"},{"role":"user","content":"hi"}]}"#;
    let run = run(&streams, &upstream, Client::Chat, Operation::Generate, body, 1).await;
    let (outcome, published, mut owner) = produce(&streams, run.started.unwrap()).await;
    outcome.unwrap();
    assert_eq!(published.terminal, Some(Terminal::Done));
    let captured = &upstream.captured()[0];
    assert_api_key_request(captured, "/v1beta/models/gemini-2.5-pro:streamGenerateContent?alt=sse");
    assert_eq!(
        json_body(captured),
        json!({
            "contents": [{"role": "user", "parts": [{"text": "hi"}]}],
            "systemInstruction": {"parts": [{"text": "be brief"}]},
            "generationConfig": {"maxOutputTokens": 64}
        })
    );
    assert_every_egress(&streams, &mut owner, BodyMode::Streaming);
}

#[tokio::test]
async fn chat_without_stream_calls_generate_content() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::json(JSON)]).await;
    let body = r#"{"model":"client-model","stream":false,"messages":[{"role":"user","content":"hi"}]}"#;
    let run = run(&streams, &upstream, Client::Chat, Operation::Generate, body, 7).await;
    let (outcome, published, mut owner) = produce(&streams, run.started.unwrap()).await;
    outcome.unwrap();
    assert_eq!(published.terminal, Some(Terminal::Done));
    assert_api_key_request(&upstream.captured()[0], "/v1beta/models/gemini-2.5-pro:generateContent");
    assert_every_egress(&streams, &mut owner, BodyMode::Buffered);
}

#[tokio::test]
async fn anthropic_and_responses_clients_send_the_official_gemini_body() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::sse(SSE), Reply::sse(SSE)]).await;
    let messages = r#"{"model":"claude-x","max_tokens":32,"stream":true,"system":"be brief","messages":[{"role":"user","content":"hi"}]}"#;
    let run1 = run(&streams, &upstream, Client::Anthropic, Operation::Generate, messages, 3).await;
    produce(&streams, run1.started.unwrap()).await.0.unwrap();
    let responses = r#"{"model":"gpt-x","stream":true,"instructions":"be brief","input":"hi"}"#;
    let run2 = run(
        &streams,
        &upstream,
        Client::Responses,
        Operation::Generate,
        responses,
        5,
    )
    .await;
    produce(&streams, run2.started.unwrap()).await.0.unwrap();
    let captured = upstream.captured();
    for request in &captured {
        assert_api_key_request(request, "/v1beta/models/gemini-2.5-pro:streamGenerateContent?alt=sse");
        let body = json_body(request);
        assert_eq!(body["contents"], json!([{"role": "user", "parts": [{"text": "hi"}]}]));
        assert_eq!(body["systemInstruction"], json!({"parts": [{"text": "be brief"}]}));
    }
    assert_eq!(
        json_body(&captured[0])["generationConfig"],
        json!({"maxOutputTokens": 32})
    );
}

#[tokio::test]
async fn a_gemini_client_body_reaches_the_upstream_json_equal() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::sse(SSE).byte_at_a_time()]).await;
    let body = r#"{"contents":[{"role":"user","parts":[{"text":"hi"}]}],"generationConfig":{"temperature":0.5,"responseLogprobs":true,"logprobs":3}}"#;
    let run = run(&streams, &upstream, Client::Gemini, Operation::Generate, body, 1).await;
    let (outcome, published, _) = produce(&streams, run.started.unwrap()).await;
    outcome.unwrap();
    assert_eq!(published.terminal, Some(Terminal::Done));
    let captured = &upstream.captured()[0];
    assert_api_key_request(captured, "/v1beta/models/gemini-2.5-pro:streamGenerateContent?alt=sse");
    assert_eq!(json_body(captured), serde_json::from_str::<Value>(body).unwrap());
}

/// Conversion at ingress and egress may change whitespace and member order, never a value,
/// an array order or a member's presence.
#[tokio::test]
async fn a_reordered_gemini_body_reaches_the_upstream_json_equal() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::sse(SSE)]).await;
    let body = "{ \"generationConfig\" : { \"temperature\": 0.5 },\n  \"contents\": [ { \"parts\": [ {\"text\": \"hi\"} ], \"role\": \"user\" },\n {\"parts\":[{\"text\":\"ok\"}],\"role\":\"model\"}, {\"role\":\"user\",\"parts\":[{\"text\":\"more\"}]} ] }";
    let run = run(&streams, &upstream, Client::Gemini, Operation::Generate, body, 3).await;
    produce(&streams, run.started.unwrap()).await.0.unwrap();
    let captured = &upstream.captured()[0];
    assert_ne!(captured.body, body.as_bytes());
    assert_eq!(json_body(captured), serde_json::from_str::<Value>(body).unwrap());
}

#[tokio::test]
async fn upstream_split_does_not_change_the_canonical_response() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::sse(SSE), Reply::sse(SSE).byte_at_a_time()]).await;
    let body = r#"{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}"#;
    let whole = run(&streams, &upstream, Client::Gemini, Operation::Generate, body, 64).await;
    let (_, whole, _) = produce(&streams, whole.started.unwrap()).await;
    let split = run(&streams, &upstream, Client::Gemini, Operation::Generate, body, 64).await;
    let (_, split, _) = produce(&streams, split.started.unwrap()).await;
    assert_eq!(whole.bytes(), split.bytes());
    assert_eq!(kinds(&whole).last().map(String::as_str), Some("x_ck.done"));
}

#[tokio::test]
async fn refusals_before_the_answer_fail_the_attempt() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([
        Reply::status(
            429,
            "application/json",
            r#"{"error":{"code":429,"status":"RESOURCE_EXHAUSTED"}}"#,
        ),
        Reply::reset(),
    ])
    .await;
    let body = r#"{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}"#;
    let refused = run(&streams, &upstream, Client::Gemini, Operation::Generate, body, 64).await;
    let error = refused.started.err().unwrap();
    assert_eq!(error.status, 429);
    assert!(String::from_utf8_lossy(&error.body).contains("RESOURCE_EXHAUSTED"));
    let reset = run(&streams, &upstream, Client::Gemini, Operation::Generate, body, 64).await;
    assert_eq!(reset.started.err().unwrap().scope, FailureScope::Transport);
    assert_eq!(upstream.requests(), 2);
}

#[tokio::test]
async fn a_body_cut_mid_stream_publishes_one_error() {
    let streams = Streams::new();
    let cut = SSE.len() - 20;
    let upstream = StreamUpstream::start([Reply::sse(SSE).cut_after(cut)]).await;
    let body = r#"{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}"#;
    let run = run(&streams, &upstream, Client::Gemini, Operation::Generate, body, 64).await;
    let (outcome, published, _) = produce(&streams, run.started.unwrap()).await;
    assert!(outcome.is_err());
    assert_eq!(published.terminal, Some(Terminal::UpstreamError));
    let kinds = kinds(&published);
    assert_eq!(
        kinds.iter().filter(|k| k.as_str() == "x_ck.error").count(),
        1,
        "{kinds:?}"
    );
    assert_eq!(kinds.last().map(String::as_str), Some("x_ck.error"));
}

#[tokio::test]
async fn the_attempt_shapes_its_own_upload() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::sse(SSE), Reply::json(r#"{"totalTokens":2}"#)]).await;
    let mut exchange = exchange(&upstream);
    exchange.model = format!("{MODEL}(1024)");
    exchange.cfg = ck_core::config::Config::parse(
        "requests:\n  payload:\n    override:\n      - models: [{name: gemini-2.5-pro, protocol: gemini}]\n        params: {labels.team: core}\n",
    )
    .unwrap();
    exchange
        .credential
        .attributes
        .insert("header:X-Team".into(), "core".into());
    let body = r#"{"model":"models/alias","contents":[{"role":"user","parts":[{"text":"hi"}]}]}"#;
    let generate = run_on(&streams, &exchange, Client::Gemini, Operation::Generate, body, 64).await;
    produce(&streams, generate.started.unwrap()).await.0.unwrap();
    let count = r#"{"generateContentRequest":{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}}"#;
    let counted = run_on(&streams, &exchange, Client::Gemini, Operation::CountTokens, count, 64).await;
    produce(&streams, counted.started.unwrap()).await.0.unwrap();
    let captured = upstream.captured();
    // The suffix names thinking, not the upstream model.
    assert_api_key_request(
        &captured[0],
        "/v1beta/models/gemini-2.5-pro:streamGenerateContent?alt=sse",
    );
    assert_eq!(captured[0].header("x-team"), Some("core"));
    let generated = json_body(&captured[0]);
    assert_eq!(generated["model"], "models/gemini-2.5-pro");
    assert_eq!(generated["generationConfig"]["thinkingConfig"]["thinkingBudget"], 1024);
    assert_eq!(generated["labels"]["team"], "core");
    let counted = json_body(&captured[1]);
    assert_eq!(counted["generateContentRequest"]["labels"]["team"], "core");
    assert_eq!(counted["generateContentRequest"]["model"], "models/gemini-2.5-pro");
}

#[tokio::test]
async fn interactions_keys_keep_the_old_path() {
    let streams = Streams::new();
    let exchange = Exchange::new(
        credential("gemini-interactions", "http://127.0.0.1:1", KEY),
        MODEL,
        "/client",
    );
    assert_eq!(executors().destination(&exchange.attempt(&streams.store)), None);
}

#[tokio::test]
async fn claude_signed_thinking_is_not_sent_to_gemini() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::sse(SSE)]).await;
    let body = concat!(
        r#"{"model":"claude-x","max_tokens":32,"stream":true,"messages":[{"role":"user","content":"hi"},"#,
        r#"{"role":"assistant","content":[{"type":"thinking","thinking":"plan","signature":"sig-claude"},"#,
        r#"{"type":"text","text":"ok"}]},{"role":"user","content":"more"}]}"#,
    );
    let run = run(&streams, &upstream, Client::Anthropic, Operation::Generate, body, 9).await;
    produce(&streams, run.started.unwrap()).await.0.unwrap();
    let upload = String::from_utf8(upstream.captured()[0].body.clone()).unwrap();
    assert!(
        !upload.contains("sig-claude") && !upload.contains("thoughtSignature"),
        "{upload}"
    );
    assert!(upload.contains(r#"{"text":"ok"}"#), "{upload}");
}

#[tokio::test]
async fn an_empty_error_body_is_kept() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::status(400, "application/json", "")]).await;
    let body = r#"{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}"#;
    let error = run(&streams, &upstream, Client::Gemini, Operation::Generate, body, 64)
        .await
        .started
        .err()
        .unwrap();
    assert_eq!((error.status, error.body.len()), (400, 0));
}

#[path = "exec_gemini/count.rs"]
mod count;
#[path = "exec_gemini/fence.rs"]
mod fence;
