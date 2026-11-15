//! Shared fixtures for the Meta executor tests: client bodies, a Meta event stream, and the
//! run, finish and egress steps of one attempt.

use ck_core::exec::ExecError;
use ck_exec::meta::MetaExecutor;
use ck_exec::pipeline::{self, Started};
use ck_exec::types::Execute;
use ck_stream::{Read, Reader, ScratchLease, Terminal, Writer};
use ck_translate::codec::event::Direction;
use ck_translate::codec::{BodyMode, Operation, RequestIngress, ResponseEgress};
use ck_translate::{anthropic, codec::chat, gemini, responses};
use serde_json::Value;

use crate::support::upstream::{Captured, Reply, StreamUpstream};
use crate::support::{Exchange, Published, Streams, credential, fence, identity};

pub const LIMIT: u64 = 1 << 20;
pub const MODEL: &str = "muse-spark";
pub const CHAT: &[u8] = br#"{"model":"client-model","stream":true,"messages":[{"role":"system","content":"be brief"},{"role":"user","content":"hi"}],"max_tokens":64,"temperature":0.5}"#;
pub const RESPONSES: &[u8] = br#"{"model":"client-model","stream":true,"input":"hi"}"#;
pub const ANTHROPIC: &[u8] = br#"{"model":"client-model","max_tokens":64,"stream":true,"system":"be brief","messages":[{"role":"user","content":"hi"}]}"#;
pub const GEMINI: &[u8] = br#"{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}"#;

pub fn event(kind: &str, data: &str) -> String {
    format!("event: {kind}\ndata: {data}\n\n")
}

/// One assistant message saying "hello" in two deltas, without its completion.
pub fn message_events() -> String {
    [
        event(
            "response.created",
            r#"{"type":"response.created","response":{"id":"resp_1","object":"response","created_at":1,"status":"in_progress","model":"muse-spark","output":[]}}"#,
        ),
        event(
            "response.output_item.added",
            r#"{"type":"response.output_item.added","output_index":0,"item":{"id":"msg_1","type":"message","status":"in_progress","role":"assistant","content":[]}}"#,
        ),
        event(
            "response.content_part.added",
            r#"{"type":"response.content_part.added","item_id":"msg_1","output_index":0,"content_index":0,"part":{"type":"output_text","text":"","annotations":[]}}"#,
        ),
        event(
            "response.output_text.delta",
            r#"{"type":"response.output_text.delta","item_id":"msg_1","output_index":0,"content_index":0,"delta":"hel"}"#,
        ),
        event(
            "response.output_text.delta",
            r#"{"type":"response.output_text.delta","item_id":"msg_1","output_index":0,"content_index":0,"delta":"lo"}"#,
        ),
        event(
            "response.output_item.done",
            r#"{"type":"response.output_item.done","output_index":0,"item":{"id":"msg_1","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"hello","annotations":[]}]}}"#,
        ),
    ]
    .concat()
}

/// A complete Meta stream whose completion lists no output.
pub fn sse() -> String {
    message_events()
        + &event(
            "response.completed",
            r#"{"type":"response.completed","response":{"id":"resp_1","object":"response","created_at":1,"status":"completed","model":"muse-spark","output":[],"usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}}"#,
        )
}

pub fn exchange(upstream: &StreamUpstream) -> Exchange {
    Exchange::new(credential("meta", &upstream.url(), "sk-up"), MODEL, "/v1/responses")
}

pub async fn attempt<I: RequestIngress>(
    streams: &Streams,
    exchange: &Exchange,
    ingress: &mut I,
    request: &mut Writer,
    (body, step): (&[u8], usize),
) -> (Result<Started, ExecError>, u64) {
    let executor = MetaExecutor::with_client(wreq::Client::new());
    let response = streams.response(request);
    let id = response.id();
    let attempt = exchange.attempt(&streams.store);
    let validated = fence(ingress, request, body, step, &executor.capability(&attempt)).expect("Meta accepts it");
    let handle = exchange.request(validated);
    (
        pipeline::start(&executor, &attempt, &handle, request, response).await,
        id,
    )
}

/// Runs `body` from a `protocol` client fenced `step` bytes at a time. Returns the outcome, the
/// response stream id and the request stream, which must outlive the response.
pub async fn run(
    streams: &Streams,
    exchange: &Exchange,
    protocol: &str,
    operation: Operation,
    body: &[u8],
    step: usize,
) -> (Result<Started, ExecError>, u64, Writer) {
    let mut request = streams.request();
    let (store, id, input) = (&streams.store, identity(Direction::Request), (body, step));
    let outcome = match protocol {
        "chat" => {
            let mut ingress = chat::request::Ingress::new(&mut request, store, id, operation, LIMIT).unwrap();
            attempt(streams, exchange, &mut ingress, &mut request, input).await
        }
        "responses" => {
            let mut ingress = responses::request::Ingress::new(&mut request, store, id, operation, LIMIT).unwrap();
            attempt(streams, exchange, &mut ingress, &mut request, input).await
        }
        "anthropic" => {
            let mut ingress = anthropic::request::Ingress::new(&mut request, store, id, operation, LIMIT).unwrap();
            attempt(streams, exchange, &mut ingress, &mut request, input).await
        }
        "gemini" => {
            let mut ingress =
                gemini::request::Ingress::new(&mut request, store, id, operation, "client-model", LIMIT).unwrap();
            attempt(streams, exchange, &mut ingress, &mut request, input).await
        }
        other => panic!("no client protocol {other}"),
    };
    (outcome.0, outcome.1, request)
}

/// The producer's outcome, the stream's terminal and its records.
pub async fn finish(streams: &Streams, started: Started, id: u64) -> (Result<(), ExecError>, Published, Writer) {
    let outcome = started.producer.await.expect("the producer does not panic");
    (outcome, streams.read_to_end(id).await, started.stream)
}

pub fn records(published: &Published) -> Vec<String> {
    let text = |r: &Vec<u8>| String::from_utf8(r.clone()).unwrap();
    published.records.iter().map(text).collect()
}

pub fn errors(records: &[String]) -> usize {
    records.iter().filter(|r| r.contains("event: x_ck.error")).count()
}

/// One Chat attempt on `body`: its outcome, terminal and records.
pub async fn chat_on(body: Vec<u8>) -> (Result<(), ExecError>, Option<Terminal>, Vec<String>) {
    let upstream = StreamUpstream::start([Reply::sse(body)]).await;
    let (streams, exchange) = (Streams::new(), exchange(&upstream));
    let (started, id, _request) = run(&streams, &exchange, "chat", Operation::Generate, CHAT, 64).await;
    let (outcome, published, _stream) = finish(&streams, started.unwrap(), id).await;
    (outcome, published.terminal, records(&published))
}

pub fn lease_bytes(lease: &ScratchLease) -> Vec<u8> {
    let mut out = Vec::with_capacity(lease.len());
    while out.len() < lease.len() {
        out.extend_from_slice(lease.chunk(out.len()).unwrap());
    }
    out
}

pub async fn deliver<E: ResponseEgress>(mut egress: E, stream: &mut Writer, mut reader: Reader) -> String {
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

/// What the client protocol's egress writes for stream `id`.
pub async fn egress(streams: &Streams, protocol: &str, stream: &mut Writer, id: u64, mode: BodyMode) -> String {
    let (reader, ident, op) = (streams.reader(id), identity(Direction::Response), Operation::Generate);
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

pub fn json_body(captured: &Captured) -> Value {
    serde_json::from_slice(&captured.body).unwrap()
}
