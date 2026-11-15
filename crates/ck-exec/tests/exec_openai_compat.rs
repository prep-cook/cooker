//! The OpenAI-compatible executor under the execution contract, against the DT local
//! upstream: the request every client protocol puts on the wire, the canonical response every
//! client egress accepts, the local count, the native compact, and failures before and after
//! the upstream answered.

mod support;

use ck_core::config::Config;
use ck_core::exec::{ExecError, FailureScope};
use ck_exec::openai_compat::OpenAICompatExecutor;
use ck_exec::pipeline::{self, Started};
use ck_exec::types::Execute;
use ck_stream::{Read, Reader, ScratchLease, Terminal, Writer};
use ck_translate::codec::event::Direction;
use ck_translate::codec::{BodyMode, Operation, RequestIngress, ResponseEgress};
use ck_translate::{anthropic, codec::chat, gemini, responses};
use serde_json::{Value, json};
use support::upstream::{Captured, Reply, StreamUpstream};
use support::{Exchange, Published, Streams, credential, fence, identity};

const LIMIT: u64 = 1 << 20;
const CHAT: &[u8] = br#"{"model":"client-model","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
const RESPONSES: &[u8] = br#"{"model":"client-model","stream":true,"input":"hi"}"#;
const ANTHROPIC: &[u8] =
    br#"{"model":"client-model","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
const GEMINI: &[u8] = br#"{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}"#;

fn chunk(delta: &str, finish: &str) -> String {
    format!(
        "data: {{\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"up-model\",\"choices\":[{{\"index\":0,\"delta\":{delta},\"finish_reason\":{finish}}}]}}\n\n"
    )
}

/// A complete relay stream: "hel", "lo", a stop and usage, then `[DONE]`.
fn sse() -> String {
    [
        chunk(r#"{"role":"assistant","content":"hel"}"#, "null"),
        chunk(r#"{"content":"lo"}"#, "null"),
        chunk("{}", r#""stop""#),
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"up-model\",\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2,\"total_tokens\":5}}\n\n".to_owned(),
        "data: [DONE]\n\n".to_owned(),
    ]
    .concat()
}

fn executor() -> OpenAICompatExecutor {
    OpenAICompatExecutor::with_client(wreq::Client::new())
}

fn exchange(upstream: &StreamUpstream) -> Exchange {
    let base = format!("{}/v1", upstream.url());
    Exchange::new(
        credential("openai-compatibility", &base, "sk-up"),
        "up-model",
        "/v1/chat/completions",
    )
}

/// One attempt: the client request fenced `step` bytes at a time, then the pipeline.
async fn attempt<I: RequestIngress>(
    streams: &Streams,
    exchange: &Exchange,
    ingress: &mut I,
    request: &mut Writer,
    body: &[u8],
    step: usize,
) -> (Result<Started, ExecError>, u64) {
    let response = streams.response(request);
    let id = response.id();
    let executor = executor();
    let attempt = exchange.attempt(&streams.store);
    let validated = fence(ingress, request, body, step, &executor.capability(&attempt)).expect("the relay accepts it");
    let handle = exchange.request(validated);
    (
        pipeline::start(&executor, &attempt, &handle, request, response).await,
        id,
    )
}

/// Runs `body` from a `protocol` client. Returns the outcome, the response stream id and the
/// request stream, which must outlive the response.
async fn run(
    streams: &Streams,
    exchange: &Exchange,
    protocol: &str,
    operation: Operation,
    body: &[u8],
    step: usize,
) -> (Result<Started, ExecError>, u64, Writer) {
    let mut request = streams.request();
    let (store, id) = (&streams.store, identity(Direction::Request));
    let outcome = match protocol {
        "chat" => {
            let mut ingress = chat::request::Ingress::new(&mut request, store, id, operation, LIMIT).unwrap();
            attempt(streams, exchange, &mut ingress, &mut request, body, step).await
        }
        "responses" => {
            let mut ingress = responses::request::Ingress::new(&mut request, store, id, operation, LIMIT).unwrap();
            attempt(streams, exchange, &mut ingress, &mut request, body, step).await
        }
        "anthropic" => {
            let mut ingress = anthropic::request::Ingress::new(&mut request, store, id, operation, LIMIT).unwrap();
            attempt(streams, exchange, &mut ingress, &mut request, body, step).await
        }
        "gemini" => {
            let mut ingress =
                gemini::request::Ingress::new(&mut request, store, id, operation, "client-model", LIMIT).unwrap();
            attempt(streams, exchange, &mut ingress, &mut request, body, step).await
        }
        other => panic!("no client protocol {other}"),
    };
    (outcome.0, outcome.1, request)
}

/// The producer's outcome and everything the response stream holds.
async fn finish(streams: &Streams, started: Started, id: u64) -> (Result<(), ExecError>, Published, Writer) {
    let outcome = started.producer.await.expect("the producer does not panic");
    (outcome, streams.read_to_end(id).await, started.stream)
}

fn records(published: &Published) -> Vec<String> {
    published
        .records
        .iter()
        .map(|r| String::from_utf8(r.clone()).unwrap())
        .collect()
}

fn lease_bytes(lease: &ScratchLease) -> Vec<u8> {
    let mut out = Vec::with_capacity(lease.len());
    while out.len() < lease.len() {
        out.extend_from_slice(lease.chunk(out.len()).unwrap());
    }
    out
}

/// What a client egress writes for the whole response stream.
async fn deliver<E: ResponseEgress>(mut egress: E, stream: &mut Writer, mut reader: Reader) -> String {
    let mut out = Vec::new();
    while let Some(read) = reader.next().await {
        match read {
            Read::Record(lease) => egress
                .advance(stream, lease, |bytes| {
                    out.extend_from_slice(&lease_bytes(&bytes));
                    Ok(())
                })
                .expect("the egress accepts every canonical record"),
            Read::Terminal(terminal) => {
                assert_eq!(terminal, Terminal::Done);
                egress.finish().unwrap();
            }
        }
    }
    String::from_utf8(out).unwrap()
}

/// The client protocol's egress over stream `id`.
async fn egress(streams: &Streams, protocol: &str, stream: &mut Writer, id: u64, mode: BodyMode) -> String {
    let (reader, op, ident) = (streams.reader(id), Operation::Generate, identity(Direction::Response));
    match protocol {
        "chat" => {
            deliver(
                chat::egress::Egress::new(stream, ident, op, mode, true, |_| {}).unwrap(),
                stream,
                reader,
            )
            .await
        }
        "responses" => {
            deliver(
                crate::support::responses_egress(stream, op, mode).unwrap(),
                stream,
                reader,
            )
            .await
        }
        "anthropic" => {
            deliver(
                anthropic::egress::Egress::new(stream, ident, op, mode, |_| {}).unwrap(),
                stream,
                reader,
            )
            .await
        }
        "gemini" => {
            deliver(
                gemini::egress::Egress::new(stream, ident, op, mode, |_| {}).unwrap(),
                stream,
                reader,
            )
            .await
        }
        other => panic!("no client protocol {other}"),
    }
}

fn json_body(captured: &Captured) -> Value {
    serde_json::from_slice(&captured.body).unwrap()
}

/// A Chat client's streamed request reaches the relay as Chat with the upstream model, its key,
/// the stream headers and usage requested; nothing else of the body, `max_completion_tokens` included.
#[tokio::test]
async fn a_chat_client_streams_through_the_relay() {
    let upstream = StreamUpstream::start([Reply::sse(sse())]).await;
    let (streams, exchange) = (Streams::new(), exchange(&upstream));
    let body = br#"{"model":"client-model","stream":true,"max_completion_tokens":7,"messages":[{"role":"user","content":"hi"}]}"#;
    let (started, id, _request) = run(&streams, &exchange, "chat", Operation::Generate, body, 1).await;
    let (outcome, published, mut stream) = finish(&streams, started.unwrap(), id).await;
    outcome.unwrap();
    assert_eq!(published.terminal, Some(Terminal::Done));
    let captured = &upstream.captured()[0];
    assert_eq!(
        (captured.method.as_str(), captured.path.as_str()),
        ("POST", "/v1/chat/completions")
    );
    assert_eq!(captured.header("authorization"), Some("Bearer sk-up"));
    assert_eq!(captured.header("user-agent"), Some(ck_exec::openai_compat::USER_AGENT));
    assert_eq!(captured.header("accept"), Some("text/event-stream"));
    assert_eq!(
        json_body(captured),
        json!({"model":"up-model","messages":[{"role":"user","content":"hi"}],"max_completion_tokens":7,"stream":true,"stream_options":{"include_usage":true}})
    );
    let out = egress(&streams, "chat", &mut stream, id, BodyMode::Streaming).await;
    assert!(
        out.contains(r#""content":"hel""#) && out.ends_with("data: [DONE]\n\n"),
        "{out}"
    );
}

/// Every client protocol's request goes upstream as valid Chat, and its own egress accepts the
/// canonical response, both streamed and buffered.
#[tokio::test]
async fn every_client_protocol_goes_out_as_chat_and_comes_back() {
    for (protocol, body, streamed, buffered) in [
        ("chat", CHAT, "[DONE]", r#""finish_reason":"stop""#),
        ("responses", RESPONSES, "response.completed", r#""status":"completed""#),
        ("anthropic", ANTHROPIC, "message_stop", r#""stop_reason":"end_turn""#),
        ("gemini", GEMINI, "STOP", "STOP"),
    ] {
        let upstream = StreamUpstream::start([Reply::sse(sse())]).await;
        let (streams, exchange) = (Streams::new(), exchange(&upstream));
        let (started, id, _request) = run(&streams, &exchange, protocol, Operation::Generate, body, 7).await;
        let (outcome, published, mut stream) = finish(&streams, started.unwrap(), id).await;
        outcome.unwrap();
        assert_eq!(published.terminal, Some(Terminal::Done), "{protocol}");
        let sent = json_body(&upstream.captured()[0]);
        assert_eq!(sent["model"], "up-model", "{protocol}: {sent}");
        assert_eq!(sent["stream"], true, "{protocol}: {sent}");
        assert_eq!(
            sent["stream_options"],
            json!({"include_usage": true}),
            "{protocol}: {sent}"
        );
        let user = sent["messages"].as_array().unwrap().last().unwrap().clone();
        assert_eq!(user["role"], "user", "{protocol}: {sent}");
        assert!(user.to_string().contains("hi"), "{protocol}: {sent}");
        for (mode, done) in [(BodyMode::Streaming, streamed), (BodyMode::Buffered, buffered)] {
            let out = egress(&streams, protocol, &mut stream, id, mode).await;
            assert!(
                out.contains("hel") && out.contains("lo") && out.contains(done),
                "{protocol}: {out}"
            );
        }
    }
}

/// A client that does not stream gets a buffered upstream: `stream:false`, no usage option and
/// no stream headers, answered by one Chat completion.
#[tokio::test]
async fn a_buffered_client_gets_a_buffered_upstream() {
    let completion = r#"{"id":"c1","object":"chat.completion","created":1,"model":"up-model","choices":[{"index":0,"message":{"role":"assistant","content":"hello","tool_calls":null},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4}}"#;
    let upstream = StreamUpstream::start([Reply::json(completion)]).await;
    let (streams, exchange) = (Streams::new(), exchange(&upstream));
    let body = br#"{"model":"client-model","messages":[{"role":"user","content":"hi"}],"stream":false}"#;
    let (started, id, _request) = run(&streams, &exchange, "chat", Operation::Generate, body, 3).await;
    let (outcome, published, mut stream) = finish(&streams, started.unwrap(), id).await;
    outcome.unwrap();
    assert_eq!(published.terminal, Some(Terminal::Done));
    let captured = &upstream.captured()[0];
    assert_eq!(captured.header("accept"), None);
    assert_eq!(
        String::from_utf8_lossy(&captured.body),
        r#"{"model":"up-model","messages":[{"role":"user","content":"hi"}],"stream":false}"#
    );
    let out = egress(&streams, "chat", &mut stream, id, BodyMode::Buffered).await;
    let out: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(out["choices"][0]["message"]["content"], "hello", "{out}");
    assert!(out["choices"][0]["message"].get("tool_calls").is_none(), "{out}");
}

/// A relay's `"tool_calls":null` in a delta means no calls, and the canonical stream does not
/// depend on how the body was split.
#[tokio::test]
async fn null_tool_calls_and_any_split_give_the_same_response() {
    let body = [
        chunk(r#"{"role":"assistant","content":"hel","tool_calls":null}"#, "null"),
        sse(),
    ]
    .concat();
    let mut seen = Vec::new();
    for reply in [Reply::sse(body.clone()), Reply::sse(body.clone()).byte_at_a_time()] {
        let upstream = StreamUpstream::start([reply]).await;
        let (streams, exchange) = (Streams::new(), exchange(&upstream));
        let (started, id, _request) = run(&streams, &exchange, "chat", Operation::Generate, CHAT, 64).await;
        let (outcome, published, mut stream) = finish(&streams, started.unwrap(), id).await;
        outcome.unwrap();
        let out = egress(&streams, "chat", &mut stream, id, BodyMode::Buffered).await;
        assert!(!out.contains("tool_calls"), "{out}");
        seen.push(records(&published));
    }
    assert_eq!(seen[0], seen[1]);
}

/// A body cut after the upstream answered ends the stream with exactly one `error`, after the
/// parts already published; the attempt fails as a transport fault.
#[tokio::test]
async fn a_cut_body_ends_with_one_error() {
    let body = sse();
    let cut = body.find("lo").unwrap() + 4;
    let upstream = StreamUpstream::start([Reply::sse(body).cut_after(cut)]).await;
    let (streams, exchange) = (Streams::new(), exchange(&upstream));
    let (started, id, _request) = run(&streams, &exchange, "chat", Operation::Generate, CHAT, 64).await;
    let (outcome, published, _stream) = finish(&streams, started.unwrap(), id).await;
    assert!(outcome.is_err());
    assert_eq!(published.terminal, Some(Terminal::UpstreamError));
    let records = records(&published);
    let errors = records.iter().filter(|r| r.contains("event: x_ck.error")).count();
    assert_eq!(errors, 1, "{records:?}");
    assert!(records.last().unwrap().contains("event: x_ck.error"), "{records:?}");
    assert!(records.iter().any(|r| r.contains("hel")), "{records:?}");
}

/// Refusals before any response byte are attempt failures with Go's scopes, and the attempt's
/// response stream is discarded.
#[tokio::test]
async fn upstream_refusals_fail_the_attempt_before_commitment() {
    for (status, scope) in [
        (429, FailureScope::Model),
        (401, FailureScope::Credential),
        (400, FailureScope::Request),
    ] {
        let reply = Reply::status(status, "application/json", r#"{"error":{"message":"no"}}"#);
        let upstream = StreamUpstream::start([reply]).await;
        let (streams, exchange) = (Streams::new(), exchange(&upstream));
        let (started, id, _request) = run(&streams, &exchange, "chat", Operation::Generate, CHAT, 64).await;
        let error = started.err().expect("a refusal fails the attempt");
        assert_eq!((error.status, error.scope), (status, scope));
        assert!(String::from_utf8_lossy(&error.body).contains("no"));
        assert!(matches!(streams.store.reader(id, 0), Err(ck_stream::Error::Gone)));
    }
    let upstream = StreamUpstream::start([]).await;
    let (streams, mut exchange) = (Streams::new(), exchange(&upstream));
    exchange.credential.attributes.remove("base_url");
    let (started, _, _request) = run(&streams, &exchange, "chat", Operation::Generate, CHAT, 64).await;
    let error = started.err().unwrap();
    assert_eq!((error.status, error.scope), (401, FailureScope::Credential));
    assert_eq!(upstream.requests(), 0);
}

/// The relay's error body reaches the attempt failure as received: empty, and not UTF-8.
#[tokio::test]
async fn a_refusal_keeps_the_error_body_as_received() {
    for body in [&b""[..], &[101, 255, 114][..]] {
        let upstream = StreamUpstream::start([Reply::status(429, "application/json", body)]).await;
        let (streams, exchange) = (Streams::new(), exchange(&upstream));
        let (started, _, _request) = run(&streams, &exchange, "chat", Operation::Generate, CHAT, 64).await;
        let error = started.err().unwrap();
        assert_eq!((error.status, &error.body[..]), (429, body));
    }
}

/// Every capture event's text.
#[derive(Default)]
struct Capture(std::sync::Mutex<Vec<String>>);

impl ck_core::exec::CaptureObserver for Capture {
    fn record(&self, event: ck_core::exec::CaptureEvent<'_>) {
        let text = match event {
            ck_core::exec::CaptureEvent::Request(request) => format!("request {}", request.url),
            ck_core::exec::CaptureEvent::ResponseError(text) => format!("error {text}"),
            _ => "other".into(),
        };
        self.0.lock().unwrap().push(text);
    }
}

/// A redirect `Location` may carry credentials; neither the failure nor its capture names it.
#[tokio::test]
async fn a_redirect_location_never_reaches_the_failure_or_its_capture() {
    let location = "http://user:sk-review-secret@127.0.0.1:bad/?api_key=sk-review-secret";
    let upstream = StreamUpstream::start([Reply::status(302, "text/plain", "").header("location", location)]).await;
    let (streams, mut exchange) = (Streams::new(), exchange(&upstream));
    let captured = std::sync::Arc::new(Capture::default());
    exchange.usage =
        ck_core::exec::UsageSink::default().with_capture(ck_core::exec::CaptureSink::new(captured.clone()));
    let (started, _, _request) = run(&streams, &exchange, "chat", Operation::Generate, CHAT, 64).await;
    let error = started.err().expect("the redirect fails the attempt");
    assert!(
        !String::from_utf8_lossy(&error.body).contains("sk-review-secret"),
        "{error:?}"
    );
    let events = captured.0.lock().unwrap().clone();
    assert!(events.iter().any(|e| e.starts_with("error ")), "{events:?}");
    assert!(!events.iter().any(|e| e.contains("sk-review-secret")), "{events:?}");
}

/// A count is local: nothing goes upstream, and the stream carries the canonical result.
#[tokio::test]
async fn a_count_is_local() {
    let upstream = StreamUpstream::start([]).await;
    let (streams, exchange) = (Streams::new(), exchange(&upstream));
    let body = br#"{"model":"client-model","messages":[{"role":"user","content":"hello there, how are you"}]}"#;
    let (started, id, _request) = run(&streams, &exchange, "anthropic", Operation::CountTokens, body, 64).await;
    let (outcome, published, _stream) = finish(&streams, started.unwrap(), id).await;
    outcome.unwrap();
    assert_eq!(upstream.requests(), 0);
    let count = records(&published)
        .into_iter()
        .find(|r| r.contains("event: x_ck.count_tokens"))
        .expect("a count record");
    let tokens: u64 = count
        .split("\"input_tokens\":")
        .nth(1)
        .and_then(|rest| rest.split(|c: char| !c.is_ascii_digit()).next())
        .and_then(|n| n.parse().ok())
        .unwrap();
    assert!(tokens > 3, "{count}");
}

/// Compact is the relay's native `/responses/compact`, buffered, with the upstream model.
#[tokio::test]
async fn compact_goes_to_the_native_compact() {
    let compaction = r#"{"id":"c","object":"response.compaction","created_at":4,"model":"up-model","output":[{"id":"i","type":"compaction","encrypted_content":"secret"}],"usage":{"input_tokens":3,"output_tokens":1,"total_tokens":4}}"#;
    let upstream = StreamUpstream::start([Reply::json(compaction)]).await;
    let (streams, exchange) = (Streams::new(), exchange(&upstream));
    let body = br#"{"model":"client-model","input":[{"role":"user","content":"hi"}]}"#;
    let (started, id, _request) = run(&streams, &exchange, "responses", Operation::Compact, body, 64).await;
    let (outcome, published, _stream) = finish(&streams, started.unwrap(), id).await;
    outcome.unwrap();
    assert_eq!(published.terminal, Some(Terminal::Done));
    let captured = &upstream.captured()[0];
    assert_eq!(captured.path, "/v1/responses/compact");
    let sent = json_body(captured);
    assert_eq!(sent["model"], "up-model", "{sent}");
    assert!(sent.get("stream").is_none(), "{sent}");
    assert!(records(&published).iter().any(|r| r.contains("secret")));
}

/// The relay's configured rewrites run on the upload: `max_completion_tokens`, the prompt cache
/// key and the payload rules, which run last.
#[tokio::test]
async fn configured_relay_rewrites_apply_to_the_upload() {
    let upstream = StreamUpstream::start([Reply::sse(sse())]).await;
    let mut exchange = exchange(&upstream);
    exchange.model = "up-model(high)".into();
    exchange.cfg = Config::parse(concat!(
        "api-keys:\n  openai-compatibility:\n    - name: openai-compatibility\n      base-url: http://relay\n      keys: []\n",
        "      support-prompt-cache-key: true\n      models:\n        - name: up-model\n",
        "          alias: client-model\n          use-max-completion-tokens: true\n",
        "requests:\n  payload:\n    filter:\n      - models: [{name: up-model}]\n        params: [stream_options]\n",
    ))
    .unwrap();
    let streams = Streams::new();
    let body = br#"{"model":"client-model","max_tokens":9,"prompt_cache_key":" k1 ","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    let (started, id, _request) = run(&streams, &exchange, "chat", Operation::Generate, body, 64).await;
    finish(&streams, started.unwrap(), id).await.0.unwrap();
    let sent = json_body(&upstream.captured()[0]);
    assert_eq!(sent["max_completion_tokens"], 9, "{sent}");
    assert!(sent.get("max_tokens").is_none(), "{sent}");
    assert!(sent.get("stream_options").is_none(), "{sent}");
    assert_eq!(sent["prompt_cache_key"], "k1", "{sent}");
    assert_eq!(sent["model"], "up-model", "{sent}");
    assert_eq!(sent["reasoning_effort"], "high", "{sent}");
}

/// A Responses client's response repeats its tools, tool choice and parallel flag in the
/// Responses shapes, although the Chat upstream reports none of them.
#[tokio::test]
async fn a_responses_client_sees_its_tools_echoed() {
    let upstream = StreamUpstream::start([Reply::sse(sse())]).await;
    let (streams, exchange) = (Streams::new(), exchange(&upstream));
    let body = br#"{"model":"client-model","stream":true,"input":"hi","tools":[{"type":"function","name":"get","parameters":{"type":"object","properties":{}}}],"tool_choice":{"type":"function","name":"get"},"parallel_tool_calls":false}"#;
    let mut request = streams.request();
    let mut ingress = responses::request::Ingress::new(
        &mut request,
        &streams.store,
        identity(Direction::Request),
        Operation::Generate,
        LIMIT,
    )
    .unwrap();
    let response = streams.response(&request);
    let id = response.id();
    let executor = executor();
    let attempt = exchange.attempt(&streams.store);
    let validated = fence(&mut ingress, &mut request, body, 64, &executor.capability(&attempt)).unwrap();
    let handle = exchange.request(validated);
    let started = pipeline::start(&executor, &attempt, &handle, &mut request, response)
        .await
        .unwrap();
    let sent = json_body(&upstream.captured()[0]);
    assert_eq!(sent["tools"][0]["function"]["name"], "get", "{sent}");
    let (outcome, _, mut stream) = finish(&streams, started, id).await;
    outcome.unwrap();
    let mut egress = crate::support::responses_egress(&mut stream, Operation::Generate, BodyMode::Buffered).unwrap();
    egress.echo(&mut stream, handle.validated.view()).unwrap();
    let out: Value = serde_json::from_str(&deliver(egress, &mut stream, streams.reader(id)).await).unwrap();
    assert_eq!(out["tools"][0]["name"], "get", "{out}");
    assert!(out["tools"][0].get("function").is_none(), "{out}");
    assert_eq!(out["tool_choice"], json!({"type": "function", "name": "get"}), "{out}");
    assert_eq!(out["parallel_tool_calls"], false, "{out}");
}

/// A Responses client's flat allowed-tools choice reaches the Chat upstream in Chat's official
/// nested form, for both modes.
#[tokio::test]
async fn a_flat_allowed_tools_choice_goes_out_nested() {
    for mode in ["auto", "required"] {
        let upstream = StreamUpstream::start([Reply::sse(sse())]).await;
        let (streams, exchange) = (Streams::new(), exchange(&upstream));
        let body = format!(
            r#"{{"model":"m","stream":true,"input":"hi","tools":[{{"type":"function","name":"f","parameters":{{"type":"object"}}}},{{"type":"function","name":"g"}}],"tool_choice":{{"type":"allowed_tools","mode":"{mode}","tools":[{{"type":"function","name":"f"}}]}}}}"#
        );
        let (started, id, _request) = run(
            &streams,
            &exchange,
            "responses",
            Operation::Generate,
            body.as_bytes(),
            64,
        )
        .await;
        finish(&streams, started.unwrap(), id).await.0.unwrap();
        let sent = json_body(&upstream.captured()[0]);
        let want = json!({"type":"allowed_tools","allowed_tools":{"mode":mode,"tools":[{"type":"function","function":{"name":"f"}}]}});
        assert_eq!(sent["tool_choice"], want, "{sent}");
    }
}

/// The selector sends relay credentials through this executor and leaves other providers on
/// their old path.
#[tokio::test]
async fn the_selector_runs_relays_through_the_executor() {
    let executors = ck_exec::Executors {
        claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
        codex: Default::default(),
        devices: Default::default(),
        openai: Default::default(),
        google: Default::default(),
    };
    let upstream = StreamUpstream::start([Reply::sse(sse())]).await;
    let (streams, mut exchange) = (Streams::new(), exchange(&upstream));
    let attempt = exchange.attempt(&streams.store);
    assert_eq!(
        executors.destination(&attempt),
        Some(ck_exec::openai_compat_exec::DESTINATION)
    );
    let mut request = streams.request();
    let mut ingress = chat::request::Ingress::new(
        &mut request,
        &streams.store,
        identity(Direction::Request),
        Operation::Generate,
        LIMIT,
    )
    .unwrap();
    for chunk in CHAT.chunks(16) {
        ingress.advance(&mut request, chunk).unwrap();
    }
    let validated = executors.fence(&attempt, &mut ingress, &mut request).unwrap();
    let response = streams.response(&request);
    let id = response.id();
    let handle = exchange.request(validated);
    let started = executors
        .start(&attempt, &handle, &mut request, response)
        .await
        .unwrap();
    finish(&streams, started, id).await.0.unwrap();
    assert_eq!(upstream.requests(), 1);
    // An unconverted provider has no destination; Claude and Codex have one since P1 and P3.
    exchange.credential.provider = "devin".into();
    assert_eq!(executors.destination(&exchange.attempt(&streams.store)), None);
}

/// Grep gate: the converted files call neither the old pair registry nor the usage sink.
#[test]
fn the_executor_uses_no_old_translation() {
    let source = include_str!("../src/openai_compat_exec.rs");
    for old in [
        "ck_translate::pair",
        "translate_",
        "StreamTranslator",
        "ck_translate::stream::",
        ".response_",
    ] {
        assert!(!source.contains(old), "{old}");
    }
    // The legacy module is over the size cap: it may shrink, never grow (size ratchet).
    let legacy = include_str!("../src/openai_compat.rs").lines().count();
    assert!(legacy <= 1013, "openai_compat.rs has {legacy} lines");
}
