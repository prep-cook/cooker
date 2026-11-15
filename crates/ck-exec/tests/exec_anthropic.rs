//! The Claude executor under the execution contract, against a local upstream: the bytes each
//! client protocol puts on the wire, the canonical events every client egress reads, local
//! counts, the thinking-budget fence, errors around commitment, and the request-side facts
//! of contract G4.

mod support;

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use ck_core::exec::{
    CaptureEvent, CaptureObserver, CaptureSink, ExecError, ExecRequest, Operation as OldOperation, UsageObserver,
    UsageSink,
};
use ck_core::format::Format;
use ck_exec::Executors;
use ck_exec::claude::ClaudeExecutor;
use ck_exec::pipeline::Started;
use ck_stream::{Read, Terminal, Writer};
use ck_translate::codec::event::Direction;
use ck_translate::codec::{self, BodyMode, Operation, RequestIngress, ResponseEgress};
use ck_translate::{anthropic, gemini, responses};
use futures_util::StreamExt;
use serde_json::{Value, json};
use support::upstream::{Reply, StreamUpstream};
use support::{Exchange, Streams, credential, identity};

const SONNET: &str = "claude-sonnet-4-5-20250929";
const MESSAGES: &str = "/v1/messages";
const LIMIT: u64 = 1 << 20;

/// An Anthropic stream whose `message_start` carries the `Message.usage`-only members.
const SSE: &str = concat!(
    "event: message_start\n",
    r#"data: {"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"claude-sonnet-4-5-20250929","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":12,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0},"output_tokens":1,"service_tier":"standard"}}}"#,
    "\n\nevent: content_block_start\n",
    r#"data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#,
    "\n\nevent: content_block_delta\n",
    r#"data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}"#,
    "\n\nevent: content_block_stop\n",
    r#"data: {"type":"content_block_stop","index":0}"#,
    "\n\nevent: message_delta\n",
    r#"data: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"input_tokens":12,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"output_tokens":5}}"#,
    "\n\nevent: message_stop\n",
    r#"data: {"type":"message_stop"}"#,
    "\n\n",
);
const MESSAGE: &str = r#"{"id":"msg_2","type":"message","role":"assistant","model":"claude-sonnet-4-5-20250929","content":[{"type":"text","text":"Hello"}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":12,"output_tokens":5}}"#;

/// One exchange against the local upstream, through the `Executors` selector.
struct Fixture {
    streams: Streams,
    exchange: Exchange,
    executors: Executors,
}

impl Fixture {
    fn new(upstream: &StreamUpstream, model: &str) -> Self {
        Self {
            streams: Streams::new(),
            exchange: Exchange::new(
                credential("claude", &upstream.url(), "sk-ant-api03-test"),
                model,
                MESSAGES,
            ),
            executors: Executors {
                claude: ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
                codex: Default::default(),
                devices: Default::default(),
                openai: Default::default(),
                google: Default::default(),
            },
        }
    }

    /// Feeds `body` one byte at a time and closes the client fence for the attempt.
    fn fence<I: RequestIngress>(&self, ingress: &mut I, request: &mut Writer, body: &[u8]) -> codec::Result<()> {
        for byte in body.chunks(1) {
            ingress.advance(request, byte)?;
        }
        let attempt = self.exchange.attempt(&self.streams.store);
        self.executors.fence(&attempt, ingress, request).map(|_| ())
    }

    /// Fences `body` and starts the attempt; returns the outcome and the response stream id.
    async fn start<I: RequestIngress>(
        &self,
        ingress: &mut I,
        request: &mut Writer,
        body: &[u8],
    ) -> (Result<Started, ExecError>, u64) {
        for byte in body.chunks(1) {
            ingress.advance(request, byte).unwrap();
        }
        let attempt = self.exchange.attempt(&self.streams.store);
        let validated = self.executors.fence(&attempt, ingress, request).unwrap();
        let handle = self.exchange.request(validated);
        let response = self.streams.response(request);
        let id = response.id();
        (self.executors.start(&attempt, &handle, request, response).await, id)
    }

    /// An Anthropic client request run to its upstream answer.
    async fn anthropic(&self, body: &[u8], operation: Operation) -> (Result<Started, ExecError>, u64, Writer) {
        let mut request = self.streams.request();
        let mut ingress = anthropic::request::Ingress::new(
            &mut request,
            &self.streams.store,
            identity(Direction::Request),
            operation,
            LIMIT,
        )
        .unwrap();
        let (started, id) = self.start(&mut ingress, &mut request, body).await;
        (started, id, request)
    }

    /// Records of stream `id` through each client egress; returns the Anthropic output.
    async fn egress(&self, id: u64, stream: &mut Writer, mode: BodyMode) -> Vec<u8> {
        let response = identity(Direction::Response);
        let mut anthropic_out = Vec::new();
        let mut native = anthropic::egress::Egress::new(stream, response, Operation::Generate, mode, |_| {}).unwrap();
        self.deliver(id, stream, &mut native, &mut anthropic_out).await;
        let chat = codec::chat::egress::Egress::new(stream, response, Operation::Generate, mode, true, |_| {});
        self.deliver(id, stream, &mut chat.unwrap(), &mut Vec::new()).await;
        let responses = responses::egress::Egress::new(stream, Operation::Generate, mode, |_, _| {});
        self.deliver(id, stream, &mut responses.unwrap(), &mut Vec::new()).await;
        let gemini = gemini::egress::Egress::new(stream, response, Operation::Generate, mode, |_| {});
        self.deliver(id, stream, &mut gemini.unwrap(), &mut Vec::new()).await;
        anthropic_out
    }

    async fn deliver(&self, id: u64, stream: &mut Writer, egress: &mut impl ResponseEgress, out: &mut Vec<u8>) {
        let mut reader = self.streams.reader(id);
        while let Some(read) = reader.next().await {
            match read {
                Read::Record(lease) => egress
                    .advance(stream, lease, |bytes| {
                        out.extend_from_slice(&lease_bytes(&bytes));
                        Ok(())
                    })
                    .expect("every client egress accepts the canonical events"),
                Read::Terminal(terminal) => assert_eq!(terminal, Terminal::Done),
            }
        }
        egress.finish().unwrap();
    }
}

fn lease_bytes(lease: &ck_stream::ScratchLease) -> Vec<u8> {
    let mut out = Vec::new();
    while out.len() < lease.len() {
        out.extend_from_slice(lease.chunk(out.len()).unwrap());
    }
    out
}

/// An SSE body as (event, data) pairs; the egress writes members in its own order.
fn sse(body: &str) -> Vec<(String, Value)> {
    body.split("\n\n")
        .filter(|event| !event.is_empty())
        .map(|event| {
            let field = |prefix: &str| event.lines().find_map(|line| line.strip_prefix(prefix)).unwrap();
            (
                field("event: ").to_owned(),
                serde_json::from_str(field("data: ")).unwrap(),
            )
        })
        .collect()
}

fn sent(upstream: &StreamUpstream) -> (Value, support::upstream::Captured) {
    let captured = upstream.captured().pop().expect("one upstream request");
    (serde_json::from_slice(&captured.body).unwrap(), captured)
}

/// The record events of stream `id` as (name, data) pairs, up to its terminal.
async fn events(streams: &Streams, id: u64) -> (Vec<(String, Value)>, Option<Terminal>) {
    let published = streams.read_to_end(id).await;
    let events = published
        .records
        .iter()
        .map(|record| {
            let record = String::from_utf8_lossy(record);
            let field = |prefix: &str| {
                record
                    .lines()
                    .find_map(|line| line.strip_prefix(prefix))
                    .map(str::to_owned)
            };
            let data = field("data: ").map_or(Value::Null, |data| serde_json::from_str(&data).unwrap());
            (field("event: ").unwrap_or_default(), data)
        })
        .collect();
    (events, published.terminal)
}

#[tokio::test]
async fn anthropic_requests_reach_the_upstream_as_the_old_route_sent_them() {
    for stream in [true, false] {
        let body = json!({
            "model": "client-model", "max_tokens": 2048, "stream": stream,
            "system": [{"type": "text", "text": "Be brief."}],
            "messages": [{"role": "user", "content": "hi"}],
            "metadata": {"user_id": "u-1"},
        });
        let body = serde_json::to_vec(&body).unwrap();
        let reply = || if stream { Reply::sse(SSE) } else { Reply::json(MESSAGE) };
        let (new, old) = (
            StreamUpstream::start([reply()]).await,
            StreamUpstream::start([reply()]).await,
        );
        let fixture = Fixture::new(&new, SONNET);
        let (started, _, _request) = fixture.anthropic(&body, Operation::Generate).await;
        started.unwrap().producer.await.unwrap().unwrap();

        let old_fixture = Fixture::new(&old, SONNET);
        let exchange = &old_fixture.exchange;
        let req = ExecRequest {
            operation: OldOperation::Generate,
            source_format: Format::Claude,
            response_format: Format::Claude,
            requested_model: "client-model".into(),
            model: SONNET.into(),
            original_body: Bytes::from(body.clone()),
            body: Bytes::from(body),
            stream,
            alt: None,
            session: None,
            execution_session: None,
            derived_session: None,
            request_path: MESSAGES.into(),
            headers: exchange.headers.clone(),
            caller: exchange.caller.clone(),
            resolved_model: None,
            usage: UsageSink::default(),
        };
        let cfg = exchange.cfg.clone();
        let mut answer = old_fixture
            .executors
            .claude
            .execute(&exchange.credential, req, &cfg)
            .await
            .unwrap();
        if let ck_core::exec::ResponseBody::Stream(events) = &mut answer.body {
            while events.next().await.is_some() {}
        }
        let ((new_body, new), (old_body, _)) = (sent(&new), sent(&old));
        // The Anthropic egress writes members in its own order; every member and value is the
        // old route's.
        assert_eq!(new_body, old_body);
        assert_eq!(new.path, "/v1/messages?beta=true");
        // A key on an origin other than api.anthropic.com goes as a bearer token.
        assert_eq!(new.header("authorization"), Some("Bearer sk-ant-api03-test"));
        assert_eq!(new.header("anthropic-version"), Some("2023-06-01"));
        assert_eq!(new.header("user-agent"), Some("cooker"));
    }
}

/// Feeds a request of a foreign client protocol and returns the Messages body sent upstream.
async fn foreign(route: &str, body: Value) -> Value {
    let upstream = StreamUpstream::start([Reply::sse(SSE)]).await;
    let fixture = Fixture::new(&upstream, SONNET);
    let (store, mut request) = (&fixture.streams.store, fixture.streams.request());
    let body = serde_json::to_vec(&body).unwrap();
    let (op, request_id) = (Operation::Generate, identity(Direction::Request));
    let (started, _) = match route {
        "chat" => {
            let mut ingress = codec::chat::request::Ingress::new(&mut request, store, request_id, op, LIMIT).unwrap();
            fixture.start(&mut ingress, &mut request, &body).await
        }
        "responses" => {
            let mut ingress = responses::request::Ingress::new(&mut request, store, request_id, op, LIMIT).unwrap();
            fixture.start(&mut ingress, &mut request, &body).await
        }
        _ => {
            let mut ingress =
                gemini::request::Ingress::new(&mut request, store, request_id, op, SONNET, LIMIT).unwrap();
            fixture.start(&mut ingress, &mut request, &body).await
        }
    };
    started.unwrap().producer.await.unwrap().unwrap();
    sent(&upstream).0
}

#[tokio::test]
async fn every_client_protocol_reaches_messages() {
    let chat = foreign(
        "chat",
        json!({"model": "m", "stream": true, "max_completion_tokens": 300, "messages": [{"role": "user", "content": "hi"}]}),
    )
    .await;
    let responses = foreign(
        "responses",
        json!({"model": "m", "stream": true, "max_output_tokens": 300, "input": "hi"}),
    )
    .await;
    let gemini = foreign(
        "gemini",
        json!({"contents": [{"role": "user", "parts": [{"text": "hi"}]}], "generationConfig": {"maxOutputTokens": 300}}),
    )
    .await;
    for sent in [chat, responses, gemini] {
        assert_eq!(sent["model"], SONNET);
        assert_eq!(sent["max_tokens"], 300);
        assert_eq!(sent["stream"], true);
        assert_eq!(sent["messages"][0]["role"], "user");
        for absent in [
            "x_ck",
            "max_completion_tokens",
            "max_output_tokens",
            "input",
            "contents",
        ] {
            assert!(sent.get(absent).is_none(), "{absent} in {sent}");
        }
    }
}

#[tokio::test]
async fn a_one_byte_stream_is_canonical_and_every_egress_reads_it() {
    let upstream = StreamUpstream::start([Reply::sse(SSE).byte_at_a_time()]).await;
    let fixture = Fixture::new(&upstream, SONNET);
    let body = br#"{"model":"m","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    let (started, id, _request) = fixture.anthropic(body, Operation::Generate).await;
    let mut started = started.unwrap();
    started.producer.await.unwrap().unwrap();
    let (records, terminal) = events(&fixture.streams, id).await;
    assert_eq!(terminal, Some(Terminal::Done));
    let usage = records
        .iter()
        .rev()
        .find(|(name, _)| name == "x_ck.usage")
        .expect("canonical usage");
    assert_eq!(usage.1["usage"]["final"], true);
    assert_eq!(
        (
            usage.1["usage"]["input_tokens"].as_u64(),
            usage.1["usage"]["output_tokens"].as_u64()
        ),
        (Some(12), Some(5))
    );
    // Anthropic to Anthropic is the upstream stream itself; `message_delta.usage` keeps only
    // MessageDeltaUsage members.
    let native = fixture.egress(id, &mut started.stream, BodyMode::Streaming).await;
    assert_eq!(sse(&String::from_utf8(native).unwrap()), sse(SSE));
}

#[tokio::test]
async fn a_buffered_message_is_canonical_and_every_egress_reads_it() {
    let upstream = StreamUpstream::start([Reply::json(MESSAGE)]).await;
    let fixture = Fixture::new(&upstream, SONNET);
    let body = br#"{"model":"m","max_tokens":64,"messages":[{"role":"user","content":"hi"}]}"#;
    let (started, id, _request) = fixture.anthropic(body, Operation::Generate).await;
    let mut started = started.unwrap();
    started.producer.await.unwrap().unwrap();
    assert_eq!(sent(&upstream).0["stream"], false);
    let native = fixture.egress(id, &mut started.stream, BodyMode::Buffered).await;
    let native: Value = serde_json::from_slice(&native).unwrap();
    assert_eq!(native, serde_json::from_str::<Value>(MESSAGE).unwrap());
}

#[tokio::test]
async fn an_upstream_refusal_fails_the_attempt_before_commitment() {
    let error = r#"{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}"#;
    let upstream = StreamUpstream::start([Reply::status(429, "application/json", error)]).await;
    let fixture = Fixture::new(&upstream, SONNET);
    let body = br#"{"model":"m","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    let (started, id, _request) = fixture.anthropic(body, Operation::Generate).await;
    let error = started.err().expect("a 429 is an attempt failure");
    assert_eq!(error.status, 429);
    assert_eq!(upstream.requests(), 1);
    // The pipeline discards the response stream of a failed attempt.
    assert!(matches!(
        fixture.streams.store.reader(id, 0),
        Err(ck_stream::Error::Gone)
    ));
}

#[tokio::test]
async fn a_body_cut_mid_stream_publishes_exactly_one_error() {
    let cut = SSE.find("event: message_delta").unwrap();
    let upstream = StreamUpstream::start([Reply::sse(SSE).cut_after(cut)]).await;
    let fixture = Fixture::new(&upstream, SONNET);
    let body = br#"{"model":"m","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    let (started, id, _request) = fixture.anthropic(body, Operation::Generate).await;
    let outcome = started.unwrap().producer.await.unwrap();
    assert!(outcome.is_err(), "a cut body fails the attempt");
    let (records, terminal) = events(&fixture.streams, id).await;
    assert_eq!(terminal, Some(Terminal::UpstreamError));
    assert_eq!(
        records.iter().filter(|(name, _)| name == "x_ck.error").count(),
        1,
        "{records:?}"
    );
}

#[tokio::test]
async fn a_count_away_from_anthropic_is_local_and_sends_nothing() {
    let upstream = StreamUpstream::start([]).await;
    let fixture = Fixture::new(&upstream, SONNET);
    let body =
        br#"{"model":"m","system":"You are a scientist","messages":[{"role":"user","content":"Hello, Claude"}]}"#;
    let (started, id, _request) = fixture.anthropic(body, Operation::CountTokens).await;
    started.unwrap().producer.await.unwrap().unwrap();
    assert_eq!(upstream.requests(), 0);
    let (records, _) = events(&fixture.streams, id).await;
    let count = records
        .iter()
        .find(|(name, _)| name == "x_ck.count_tokens")
        .map(|(_, data)| &data["input_tokens"]);
    assert!(count.and_then(Value::as_u64).is_some_and(|n| n > 0), "{records:?}");
}

/// The client fence's verdict for an Anthropic request to `model`; nothing is ever sent.
async fn verdict(model: &str, body: Value) -> codec::Result<()> {
    let upstream = StreamUpstream::start([]).await;
    let fixture = Fixture::new(&upstream, model);
    let mut request = fixture.streams.request();
    let store = &fixture.streams.store;
    let operation = Operation::Generate;
    let mut ingress =
        anthropic::request::Ingress::new(&mut request, store, identity(Direction::Request), operation, LIMIT).unwrap();
    let verdict = fixture.fence(&mut ingress, &mut request, &serde_json::to_vec(&body).unwrap());
    assert_eq!(upstream.requests(), 0);
    verdict
}

#[tokio::test]
async fn thinking_budgets_and_forced_tools_are_refused_never_adjusted() {
    let request = |thinking: Value, max_tokens: u64, tool_choice: Value| {
        json!({
            "model": "m", "max_tokens": max_tokens, "thinking": thinking, "tool_choice": tool_choice,
            "tools": [{"name": "read", "input_schema": {"type": "object"}}],
            "messages": [{"role": "user", "content": "hi"}],
        })
    };
    let manual = |budget: u64| json!({"type": "enabled", "budget_tokens": budget});
    let auto = json!({"type": "auto"});
    let forced = json!({"type": "tool", "name": "read"});
    assert!(verdict(SONNET, request(manual(1024), 2048, auto.clone())).await.is_ok());
    let schema = |verdict: codec::Result<()>| matches!(verdict, Err(codec::Error::Schema));
    let unsupported = |verdict: codec::Result<()>| matches!(verdict, Err(codec::Error::Unsupported));
    assert!(schema(verdict(SONNET, request(manual(1023), 2048, auto.clone())).await));
    assert!(schema(verdict(SONNET, request(manual(2048), 2048, auto.clone())).await));
    assert!(unsupported(
        verdict("claude-opus-4-7", request(manual(2048), 4096, auto)).await
    ));
    let any = json!({"type": "any"});
    assert!(unsupported(verdict(SONNET, request(manual(1024), 4096, any)).await));
    assert!(unsupported(verdict(SONNET, request(manual(1024), 4096, forced)).await));
}

#[tokio::test]
async fn api_key_requests_keep_the_clients_values_and_betas() {
    let upstream = StreamUpstream::start([Reply::sse(SSE)]).await;
    let mut fixture = Fixture::new(&upstream, "claude-opus-4-7");
    let betas = "effort-2025-11-24,some-future-beta-2027-01-01";
    fixture
        .exchange
        .headers
        .insert("anthropic-beta", betas.parse().unwrap());
    let body = json!({
        "model": "m", "max_tokens": 64, "stream": true, "temperature": 0.5, "top_p": 0.9,
        "thinking": {"type": "adaptive"}, "tool_choice": {"type": "tool", "name": "read"},
        "tools": [{"name": "read", "input_schema": {"type": "object"}}],
        "system": [{"type": "text", "text": "s", "cache_control": {"type": "ephemeral", "ttl": "1h"}}],
        "messages": [{"role": "user", "content": "hi"}],
    });
    let (started, _, _request) = fixture
        .anthropic(&serde_json::to_vec(&body).unwrap(), Operation::Generate)
        .await;
    started.unwrap().producer.await.unwrap().unwrap();
    let (sent, captured) = sent(&upstream);
    assert_eq!(
        (sent["temperature"].clone(), sent["top_p"].clone()),
        (json!(0.5), json!(0.9))
    );
    assert_eq!(sent["thinking"], json!({"type": "adaptive"}));
    assert_eq!(sent["tool_choice"], json!({"type": "tool", "name": "read"}));
    assert_eq!(captured.header("anthropic-beta"), Some(betas));
}

#[tokio::test]
async fn compact_is_refused_at_the_fence() {
    let upstream = StreamUpstream::start([]).await;
    let fixture = Fixture::new(&upstream, SONNET);
    let mut request = fixture.streams.request();
    let store = &fixture.streams.store;
    let (id, op) = (identity(Direction::Request), Operation::Compact);
    let mut ingress = responses::request::Ingress::new(&mut request, store, id, op, LIMIT).unwrap();
    let body = br#"{"model":"m","input":"hi"}"#;
    assert!(fixture.fence(&mut ingress, &mut request, body).is_err());
    assert_eq!(upstream.requests(), 0);
}

/// Every usage and capture call, by name.
#[derive(Default)]
struct Observed(Mutex<Vec<String>>);

impl Observed {
    fn push(&self, call: String) {
        self.0.lock().unwrap().push(call);
    }
}

impl UsageObserver for Observed {
    fn response_body(&self, _format: Format, _body: &[u8]) {
        self.push("response_body".into());
    }
    fn response_line(&self, _format: Format, _line: &[u8]) {
        self.push("response_line".into());
    }
    fn request(&self, _format: Format, payload: &[u8]) {
        self.push(format!("request {}", String::from_utf8_lossy(payload)));
    }
    fn round_trip_started(&self) {
        self.push("round_trip_started".into());
    }
    fn first_byte(&self) {
        self.push("first_byte".into());
    }
}

impl CaptureObserver for Observed {
    fn record(&self, event: CaptureEvent<'_>) {
        let name = match event {
            CaptureEvent::Request(_) => "capture request",
            CaptureEvent::ResponseChunk(_) => "capture chunk",
            _ => "capture other",
        };
        self.push(name.into());
    }
}

#[tokio::test]
async fn request_side_facts_reach_the_usage_sink_and_response_usage_does_not() {
    let upstream = StreamUpstream::start([Reply::sse(SSE)]).await;
    let mut fixture = Fixture::new(&upstream, SONNET);
    let observed = Arc::new(Observed::default());
    fixture.exchange.usage = UsageSink::new(observed.clone()).with_capture(CaptureSink::new(observed.clone()));
    let body = br#"{"model":"m","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    let (started, _, _request) = fixture.anthropic(body, Operation::Generate).await;
    started.unwrap().producer.await.unwrap().unwrap();
    let calls = observed.0.lock().unwrap().clone();
    let wire = String::from_utf8(sent(&upstream).1.body).unwrap();
    assert!(calls.contains(&format!("request {wire}")), "{calls:?}");
    for call in ["round_trip_started", "first_byte", "capture request", "capture chunk"] {
        assert!(calls.iter().any(|c| c == call), "{call} missing from {calls:?}");
    }
    assert!(!calls.iter().any(|c| c.starts_with("response_")), "{calls:?}");
}
