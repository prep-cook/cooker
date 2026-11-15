//! The Vertex executor through the pipeline against a local upstream: the request each client
//! protocol sends on the Gemini route, the canonical response every client egress accepts, the
//! upstream token count, the `endpoints/openapi` route, and failures before and after the
//! upstream answered.

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
const KEY: &str = "vertex-key";
const LIMIT: u64 = 1 << 20;
/// The Gemini route's model collection under an API-key base without a version.
const MODELS: &str = "/v1/publishers/google/models/gemini-2.5-pro";

const SSE: &str = concat!(
    "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"Hel\"}]},\"index\":0}],",
    "\"modelVersion\":\"gemini-2.5-pro\",\"createTime\":\"2026-11-15T00:00:00Z\",\"responseId\":\"r1\"}\r\n\r\n",
    "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"lo\"}]},\"finishReason\":\"STOP\",",
    "\"index\":0}],\"usageMetadata\":{\"promptTokenCount\":3,\"candidatesTokenCount\":2,\"totalTokenCount\":5},",
    "\"modelVersion\":\"gemini-2.5-pro\",\"createTime\":\"2026-11-15T00:00:00Z\",\"responseId\":\"r1\"}\r\n\r\n",
);
const JSON: &str = concat!(
    "{\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"Hello\"}]},\"finishReason\":\"STOP\",",
    "\"index\":0}],\"usageMetadata\":{\"promptTokenCount\":3,\"candidatesTokenCount\":2,\"totalTokenCount\":5},",
    "\"modelVersion\":\"gemini-2.5-pro\",\"responseId\":\"r1\"}",
);
const GEMINI: &str = r#"{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}"#;

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

/// The production selector; only its Vertex arm runs here.
fn executors() -> Executors {
    Executors {
        claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
        codex: Default::default(),
        devices: Default::default(),
        openai: Default::default(),
        google: Default::default(),
    }
}

/// Fences `body` through the selector, fed `step` bytes at a time, and starts the attempt.
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
    assert!(executors.destination(&attempt).is_some());
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

/// The default attempt: a `vertex` API key on `upstream` for [`MODEL`].
fn exchange(upstream: &StreamUpstream) -> Exchange {
    Exchange::new(credential("vertex", &upstream.url(), KEY), MODEL, "/client")
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

/// Checks the transport facts every Vertex API-key request carries.
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

/// Encodes the response stream with `egress` and returns the client bytes.
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

/// Every client egress accepts the canonical response and writes its text and stop.
fn assert_every_egress(streams: &Streams, owner: &mut Writer, mode: BodyMode, text: &str, stops: [&str; 4]) {
    let id = identity(Direction::Response);
    let op = Operation::Generate;
    let mut chat = chat::egress::Egress::new(owner, id, op, mode, true, |_| {}).unwrap();
    let chat = deliver(streams, owner, &mut chat);
    let mut messages = anthropic::egress::Egress::new(owner, id, op, mode, |_| {}).unwrap();
    let messages = deliver(streams, owner, &mut messages);
    let mut responses = responses::egress::Egress::new(owner, op, mode, |_, _| {}).unwrap();
    let responses = deliver(streams, owner, &mut responses);
    let mut native = gemini::egress::Egress::new(owner, id, op, mode, |_| {}).unwrap();
    let native = deliver(streams, owner, &mut native);
    for (out, stop) in [chat, messages, responses, native].iter().zip(stops) {
        assert!(out.contains(text) && out.contains(stop), "{stop}: {out}");
    }
}

const GEMINI_STOPS: [&str; 4] = ["stop", "end_turn", "completed", "STOP"];

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
    assert_api_key_request(captured, &format!("{MODELS}:streamGenerateContent?alt=sse"));
    assert_eq!(
        json_body(captured),
        json!({
            "contents": [{"role": "user", "parts": [{"text": "hi"}]}],
            "systemInstruction": {"parts": [{"text": "be brief"}]},
            "generationConfig": {"maxOutputTokens": 64}
        })
    );
    assert_every_egress(&streams, &mut owner, BodyMode::Streaming, "Hel", GEMINI_STOPS);
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
    assert_api_key_request(&upstream.captured()[0], &format!("{MODELS}:generateContent"));
    assert_every_egress(&streams, &mut owner, BodyMode::Buffered, "Hel", GEMINI_STOPS);
}

#[tokio::test]
async fn anthropic_and_responses_clients_send_the_official_vertex_body() {
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
        assert_api_key_request(request, &format!("{MODELS}:streamGenerateContent?alt=sse"));
        let body = json_body(request);
        assert_eq!(body["contents"], json!([{"role": "user", "parts": [{"text": "hi"}]}]));
        assert_eq!(body["systemInstruction"], json!({"parts": [{"text": "be brief"}]}));
    }
    assert_eq!(
        json_body(&captured[0])["generationConfig"],
        json!({"maxOutputTokens": 32})
    );
}

/// A Gemini client always streams upstream; its body arrives JSON-equal, also fed one byte
/// at a time and answered one byte at a time.
#[tokio::test]
async fn a_gemini_client_body_reaches_the_upstream_json_equal() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::sse(SSE).byte_at_a_time()]).await;
    let body = r#"{"contents":[{"role":"user","parts":[{"text":"hi"}]}],"safetySettings":[{"category":"HARM_CATEGORY_HARASSMENT","threshold":"BLOCK_NONE"}],"generationConfig":{"temperature":0.5}}"#;
    let run = run(&streams, &upstream, Client::Gemini, Operation::Generate, body, 1).await;
    let (outcome, published, _) = produce(&streams, run.started.unwrap()).await;
    outcome.unwrap();
    assert_eq!(published.terminal, Some(Terminal::Done));
    let captured = &upstream.captured()[0];
    assert_api_key_request(captured, &format!("{MODELS}:streamGenerateContent?alt=sse"));
    assert_eq!(json_body(captured), serde_json::from_str::<Value>(body).unwrap());
}

#[tokio::test]
async fn upstream_split_does_not_change_the_canonical_response() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::sse(SSE), Reply::sse(SSE).byte_at_a_time()]).await;
    let whole = run(&streams, &upstream, Client::Gemini, Operation::Generate, GEMINI, 64).await;
    let (_, whole, _) = produce(&streams, whole.started.unwrap()).await;
    let split = run(&streams, &upstream, Client::Gemini, Operation::Generate, GEMINI, 64).await;
    let (_, split, _) = produce(&streams, split.started.unwrap()).await;
    assert_eq!(whole.bytes(), split.bytes());
    assert_eq!(kinds(&whole).last().map(String::as_str), Some("x_ck.done"));
}

/// The model suffix's thinking is operator configuration; a body `model` names the upstream
/// model's resource; payload rules run last and custom headers go out.
#[tokio::test]
async fn the_attempt_shapes_its_own_upload() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::sse(SSE)]).await;
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
    let captured = &upstream.captured()[0];
    // The suffix names thinking, not the upstream model.
    assert_api_key_request(captured, &format!("{MODELS}:streamGenerateContent?alt=sse"));
    assert_eq!(captured.header("x-team"), Some("core"));
    let generated = json_body(captured);
    assert_eq!(generated["model"], "publishers/google/models/gemini-2.5-pro");
    assert_eq!(generated["generationConfig"]["thinkingConfig"]["thinkingBudget"], 1024);
    assert_eq!(generated["labels"]["team"], "core");
}

/// A base that already names the publisher's models is the collection URL as configured.
#[tokio::test]
async fn a_publishers_base_is_the_model_collection() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::sse(SSE)]).await;
    let base = format!("{}/v1beta1/publishers/google/models", upstream.url());
    let exchange = Exchange::new(credential("vertex", &base, KEY), MODEL, "/client");
    let run = run_on(&streams, &exchange, Client::Gemini, Operation::Generate, GEMINI, 64).await;
    produce(&streams, run.started.unwrap()).await.0.unwrap();
    assert_api_key_request(
        &upstream.captured()[0],
        "/v1beta1/publishers/google/models/gemini-2.5-pro:streamGenerateContent?alt=sse",
    );
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
    let refused = run(&streams, &upstream, Client::Gemini, Operation::Generate, GEMINI, 64).await;
    let error = refused.started.err().unwrap();
    assert_eq!(error.status, 429);
    assert_eq!(
        &error.body[..],
        br#"{"error":{"code":429,"status":"RESOURCE_EXHAUSTED"}}"#
    );
    let reset = run(&streams, &upstream, Client::Gemini, Operation::Generate, GEMINI, 64).await;
    assert_eq!(reset.started.err().unwrap().scope, FailureScope::Transport);
    assert_eq!(upstream.requests(), 2);
}

#[tokio::test]
async fn an_empty_error_body_is_kept() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::status(400, "application/json", "")]).await;
    let error = run(&streams, &upstream, Client::Gemini, Operation::Generate, GEMINI, 64)
        .await
        .started
        .err()
        .unwrap();
    assert_eq!((error.status, error.body.len()), (400, 0));
}

/// Exactly one `error` record ends the stream, whether the body breaks mid-stream or ends
/// before a finish reason.
#[tokio::test]
async fn a_body_ended_early_publishes_one_error() {
    let streams = Streams::new();
    let first = SSE.find("\r\n\r\n").unwrap() + 4;
    let upstream = StreamUpstream::start([Reply::sse(SSE).cut_after(SSE.len() - 20), Reply::sse(&SSE[..first])]).await;
    for _ in 0..2 {
        let run = run(&streams, &upstream, Client::Gemini, Operation::Generate, GEMINI, 64).await;
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
}

#[tokio::test]
async fn compact_is_refused_on_both_routes() {
    let streams = Streams::new();
    let body = br#"{"model":"m","input":"hi"}"#;
    for model in [MODEL, "deepseek-ai/deepseek-v3.1-maas"] {
        let exchange = Exchange::new(credential("vertex", "http://127.0.0.1:1", KEY), model, "/client");
        let attempt = exchange.attempt(&streams.store);
        let mut writer = streams.request();
        let id = identity(Direction::Request);
        let mut ingress =
            responses::request::Ingress::new(&mut writer, &streams.store, id, Operation::Compact, LIMIT).unwrap();
        ingress.advance(&mut writer, body).unwrap();
        let fenced = executors().fence(&attempt, &mut ingress, &mut writer);
        assert!(matches!(fenced, Err(codec::Error::Unsupported)), "{model}");
    }
}

#[tokio::test]
async fn imagen_models_keep_the_old_path() {
    let streams = Streams::new();
    for model in ["imagen-4.0-generate-001", "Imagen-3.0(high)"] {
        let exchange = Exchange::new(credential("vertex", "http://127.0.0.1:1", KEY), model, "/client");
        assert_eq!(executors().destination(&exchange.attempt(&streams.store)), None);
    }
}

/// Grep gate: the converted file calls neither the old pair registry nor the usage sink.
#[test]
fn the_executor_uses_no_old_translation() {
    let source = include_str!("../src/vertex_exec.rs");
    for old in [
        "ck_translate::pair",
        "translate_",
        "StreamTranslator",
        "ck_translate::stream::",
        ".response_",
        "chat_with",
    ] {
        assert!(!source.contains(old), "{old}");
    }
}

#[path = "exec_vertex/count.rs"]
mod count;
#[path = "exec_vertex/openapi.rs"]
mod openapi;
