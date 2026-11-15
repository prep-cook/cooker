//! Shared fixtures for the Codex executor tests: client bodies, a Codex event stream, the
//! executor on plain transports, and the run, finish and egress steps of one attempt.

use ck_core::exec::ExecError;
use ck_exec::codex::CodexExecutor;
use ck_exec::codex_oauth::CodexOAuth;
use ck_exec::pipeline::{self, Started};
use ck_exec::types::Execute;
use ck_stream::{Read, Reader, ScratchLease, Terminal, Writer};
use ck_translate::codec::event::Direction;
use ck_translate::codec::{BodyMode, Operation, RequestIngress, ResponseEgress};
use ck_translate::{anthropic, codec::chat, gemini, responses};
use serde_json::Value;

use crate::support::upstream::{Captured, StreamUpstream};
use crate::support::{Exchange, Published, Streams, credential, fence, identity};

pub const LIMIT: u64 = 1 << 20;
pub const MODEL: &str = "gpt-5";
pub const CHAT: &[u8] = br#"{"model":"client-model","stream":true,"messages":[{"role":"system","content":"be brief"},{"role":"user","content":"hi"}],"max_tokens":64,"temperature":0.5}"#;
pub const RESPONSES: &[u8] = br#"{"model":"client-model","stream":true,"input":"hi"}"#;
pub const ANTHROPIC: &[u8] = br#"{"model":"client-model","max_tokens":64,"stream":true,"system":"be brief","messages":[{"role":"user","content":"hi"}]}"#;
pub const GEMINI: &[u8] = br#"{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}"#;

/// One Codex SSE event.
pub fn event(kind: &str, data: &str) -> String {
    format!("event: {kind}\ndata: {data}\n\n")
}

/// The output events of one assistant message saying "hello" in two deltas.
pub fn message_events() -> String {
    [
        event(
            "response.created",
            r#"{"type":"response.created","sequence_number":0,"response":{"id":"resp_1","object":"response","created_at":1,"status":"in_progress","model":"gpt-5","output":[]}}"#,
        ),
        event(
            "response.output_item.added",
            r#"{"type":"response.output_item.added","sequence_number":1,"output_index":0,"item":{"id":"msg_1","type":"message","status":"in_progress","role":"assistant","content":[]}}"#,
        ),
        event(
            "response.content_part.added",
            r#"{"type":"response.content_part.added","sequence_number":2,"item_id":"msg_1","output_index":0,"content_index":0,"part":{"type":"output_text","text":"","annotations":[]}}"#,
        ),
        event(
            "response.output_text.delta",
            r#"{"type":"response.output_text.delta","sequence_number":3,"item_id":"msg_1","output_index":0,"content_index":0,"delta":"hel"}"#,
        ),
        event(
            "response.output_text.delta",
            r#"{"type":"response.output_text.delta","sequence_number":4,"item_id":"msg_1","output_index":0,"content_index":0,"delta":"lo"}"#,
        ),
        event(
            "response.output_text.done",
            r#"{"type":"response.output_text.done","sequence_number":5,"item_id":"msg_1","output_index":0,"content_index":0,"text":"hello"}"#,
        ),
        event(
            "response.content_part.done",
            r#"{"type":"response.content_part.done","sequence_number":6,"item_id":"msg_1","output_index":0,"content_index":0,"part":{"type":"output_text","text":"hello","annotations":[]}}"#,
        ),
        event(
            "response.output_item.done",
            r#"{"type":"response.output_item.done","sequence_number":7,"output_index":0,"item":{"id":"msg_1","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"hello","annotations":[]}]}}"#,
        ),
    ]
    .concat()
}

/// A complete Codex stream. Like the ChatGPT backend, its completion lists no output.
pub fn sse() -> String {
    message_events()
        + &event(
            "response.completed",
            r#"{"type":"response.completed","sequence_number":8,"response":{"id":"resp_1","object":"response","created_at":1,"status":"completed","model":"gpt-5","output":[],"usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}}"#,
        )
}

pub fn executor() -> CodexExecutor {
    CodexExecutor::with_client(wreq::Client::new(), CodexOAuth::new(wreq::Client::new()))
}

/// A Codex API key whose base URL is the local upstream.
pub fn exchange(upstream: &StreamUpstream) -> Exchange {
    Exchange::new(credential("codex", &upstream.url(), "sk-up"), MODEL, "/v1/responses")
}

/// One attempt on `executor`: the client request fenced `step` bytes at a time, then the
/// pipeline, with `session` as the client's execution session.
async fn attempt<I: RequestIngress>(
    on: (&CodexExecutor, Option<&str>),
    streams: &Streams,
    exchange: &Exchange,
    ingress: &mut I,
    request: &mut Writer,
    (body, step): (&[u8], usize),
) -> (Result<Started, ExecError>, u64) {
    let (executor, session) = on;
    let response = streams.response(request);
    let id = response.id();
    let attempt = exchange.attempt(&streams.store);
    let validated = fence(ingress, request, body, step, &executor.capability(&attempt)).expect("Codex accepts it");
    let mut handle = exchange.request(validated);
    handle.client.execution_session = session;
    (
        pipeline::start(executor, &attempt, &handle, request, response).await,
        id,
    )
}

/// Runs `body` from a `protocol` client on a fresh executor. Returns the outcome, the response
/// stream id and the request stream, which must outlive the response.
pub async fn run(
    streams: &Streams,
    exchange: &Exchange,
    protocol: &str,
    operation: Operation,
    body: &[u8],
    step: usize,
) -> (Result<Started, ExecError>, u64, Writer) {
    run_on((&executor(), None), streams, exchange, protocol, operation, body, step).await
}

/// [`run`] on `executor` with an execution session.
pub async fn run_on(
    on: (&CodexExecutor, Option<&str>),
    streams: &Streams,
    exchange: &Exchange,
    protocol: &str,
    operation: Operation,
    body: &[u8],
    step: usize,
) -> (Result<Started, ExecError>, u64, Writer) {
    let mut request = streams.request();
    let (store, id) = (&streams.store, identity(Direction::Request));
    let input = (body, step);
    let outcome = match protocol {
        "chat" => {
            let mut ingress = chat::request::Ingress::new(&mut request, store, id, operation, LIMIT).unwrap();
            attempt(on, streams, exchange, &mut ingress, &mut request, input).await
        }
        "responses" => {
            let mut ingress = responses::request::Ingress::new(&mut request, store, id, operation, LIMIT).unwrap();
            attempt(on, streams, exchange, &mut ingress, &mut request, input).await
        }
        "anthropic" => {
            let mut ingress = anthropic::request::Ingress::new(&mut request, store, id, operation, LIMIT).unwrap();
            attempt(on, streams, exchange, &mut ingress, &mut request, input).await
        }
        "gemini" => {
            let mut ingress =
                gemini::request::Ingress::new(&mut request, store, id, operation, "client-model", LIMIT).unwrap();
            attempt(on, streams, exchange, &mut ingress, &mut request, input).await
        }
        other => panic!("no client protocol {other}"),
    };
    (outcome.0, outcome.1, request)
}

/// The producer's outcome and everything the response stream holds.
pub async fn finish(streams: &Streams, started: Started, id: u64) -> (Result<(), ExecError>, Published, Writer) {
    let outcome = started.producer.await.expect("the producer does not panic");
    (outcome, streams.read_to_end(id).await, started.stream)
}

pub fn records(published: &Published) -> Vec<String> {
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
                assert!(matches!(terminal, Terminal::Done | Terminal::UpstreamError));
                egress.finish().unwrap();
            }
        }
    }
    String::from_utf8(out).unwrap()
}

/// The client protocol's egress over stream `id`.
pub async fn egress(streams: &Streams, protocol: &str, stream: &mut Writer, id: u64, mode: BodyMode) -> String {
    egress_for(streams, protocol, stream, id, mode, Operation::Generate).await
}

/// [`egress`] for `operation`.
pub async fn egress_for(
    streams: &Streams,
    protocol: &str,
    stream: &mut Writer,
    id: u64,
    mode: BodyMode,
    op: Operation,
) -> String {
    let (reader, ident) = (streams.reader(id), identity(Direction::Response));
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
