//! The xAI executor through the pipeline against a local upstream: the request each client
//! protocol sends, the canonical response every client egress accepts, xAI's line rules, the
//! local count, the native compact, reasoning replay, failures before and after the upstream
//! answered, and Responses WebSocket turns on xAI's upstream socket.

mod support;

use std::collections::BTreeMap;

use ck_core::config::Config;
use ck_core::credential::{Credential, Source};
use ck_core::exec::{ExecError, FailureScope};
use ck_exec::Executors;
use ck_exec::pipeline::Started;
use ck_stream::{Read, ScratchLease, Terminal, Writer};
use ck_translate::anthropic;
use ck_translate::codec::chat;
use ck_translate::codec::event::Direction;
use ck_translate::codec::{BodyMode, Operation, RequestIngress, ResponseEgress};
use ck_translate::gemini;
use ck_translate::responses;
use serde_json::{Value, json};
use support::upstream::{Captured, Reply, StreamUpstream};
use support::{Exchange, Published, Streams, identity};

const MODEL: &str = "grok-4.3";
const TOKEN: &str = "xai-test-key";
const LIMIT: u64 = 1 << 20;

/// One client request in its own protocol.
#[derive(Clone, Copy, Debug)]
enum Client {
    Chat,
    Anthropic,
    Responses,
    Gemini,
}

/// The client sessions a request handle carries, and the dispatch loop's model binding.
#[derive(Clone, Copy, Default)]
struct Sessions<'s> {
    execution: Option<&'s str>,
    derived: Option<&'s str>,
    resolved: Option<&'s ck_core::exec::ResolvedModel>,
}

/// One attempt's outcome, with the request stream the response stream depends on.
struct Run {
    started: Result<Started, ExecError>,
    _request: Writer,
}

/// The production selector with a plain client for xAI.
fn executors() -> Executors {
    Executors {
        claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
        codex: Default::default(),
        devices: Default::default(),
        openai: ck_exec::OpenAIExecutors {
            compat: Default::default(),
            xai: ck_exec::xai::XaiExecutor::with_client(wreq::Client::new()),
        },
        google: Default::default(),
    }
}

/// An API-key xAI credential whose base URL is the local upstream.
fn credential(base_url: &str) -> Credential {
    Credential {
        id: "xai-test".into(),
        provider: "xai".into(),
        source: Source::Config {
            section: "xai-api-key".into(),
            index: 0,
        },
        disabled: false,
        label: "xai".into(),
        attributes: BTreeMap::from([
            ("base_url".to_owned(), base_url.to_owned()),
            ("api_key".to_owned(), TOKEN.to_owned()),
        ]),
        metadata: serde_json::Map::new(),
        revision: 1,
    }
}

/// The default attempt: an API key on `upstream`'s `/v1` for [`MODEL`].
fn exchange(upstream: &StreamUpstream) -> Exchange {
    Exchange::new(credential(&format!("{}/v1", upstream.url())), MODEL, "/v1/responses")
}

/// Fences `body` through the selector, fed `step` bytes at a time, and starts the attempt.
async fn run_with<I: RequestIngress>(
    streams: &Streams,
    executors: &Executors,
    exchange: &Exchange,
    (mut writer, mut ingress): (Writer, I),
    (body, step, sessions): (&[u8], usize, Sessions<'_>),
) -> Run {
    let response = streams.response(&writer);
    let mut attempt = exchange.attempt(&streams.store);
    attempt.resolved_model = sessions.resolved;
    for chunk in body.chunks(step) {
        ingress.advance(&mut writer, chunk).unwrap();
    }
    assert_eq!(executors.destination(&attempt), Some(ck_exec::xai_exec::DESTINATION));
    let validated = match executors.fence(&attempt, &mut ingress, &mut writer) {
        Ok(validated) => validated,
        Err(error) => {
            let started = Err(ExecError::local(400, FailureScope::Request, format!("fence: {error}")));
            return Run {
                started,
                _request: writer,
            };
        }
    };
    let validated = executors
        .refence(&attempt, validated.view())
        .expect("the re-fence accepts");
    let mut request = exchange.request(validated);
    request.client.execution_session = sessions.execution;
    request.client.derived_session = sessions.derived;
    let started = executors.start(&attempt, &mut request, &mut writer, response).await;
    Run {
        started,
        _request: writer,
    }
}

async fn run_on(
    streams: &Streams,
    executors: &Executors,
    exchange: &Exchange,
    client: Client,
    operation: Operation,
    (body, step, sessions): (&str, usize, Sessions<'_>),
) -> Run {
    let (store, id) = (&streams.store, identity(Direction::Request));
    let mut writer = streams.request();
    let body = body.as_bytes();
    match client {
        Client::Chat => {
            let ingress = chat::request::Ingress::new(&mut writer, store, id, operation, LIMIT).unwrap();
            run_with(streams, executors, exchange, (writer, ingress), (body, step, sessions)).await
        }
        Client::Anthropic => {
            let ingress = anthropic::request::Ingress::new(&mut writer, store, id, operation, LIMIT).unwrap();
            run_with(streams, executors, exchange, (writer, ingress), (body, step, sessions)).await
        }
        Client::Responses => {
            let ingress = responses::request::Ingress::new(&mut writer, store, id, operation, LIMIT).unwrap();
            run_with(streams, executors, exchange, (writer, ingress), (body, step, sessions)).await
        }
        Client::Gemini => {
            let ingress = gemini::request::Ingress::new(&mut writer, store, id, operation, MODEL, LIMIT).unwrap();
            run_with(streams, executors, exchange, (writer, ingress), (body, step, sessions)).await
        }
    }
}

async fn run(streams: &Streams, upstream: &StreamUpstream, client: Client, body: &str, step: usize) -> Run {
    let input = (body, step, Sessions::default());
    run_on(
        streams,
        &executors(),
        &exchange(upstream),
        client,
        Operation::Generate,
        input,
    )
    .await
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

fn records(published: &Published) -> Vec<String> {
    published
        .records
        .iter()
        .map(|r| String::from_utf8_lossy(r).into_owned())
        .collect()
}

/// The `error` records a stream holds.
fn errors(published: &Published) -> Vec<String> {
    records(published)
        .into_iter()
        .filter(|r| r.contains("event: x_ck.error"))
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

/// What a Responses client receives, buffered.
fn responses_json(streams: &Streams, owner: &mut Writer) -> Value {
    let mut egress = support::responses_egress(owner, Operation::Generate, BodyMode::Buffered).unwrap();
    serde_json::from_str(&deliver(streams, owner, &mut egress)).unwrap()
}

/// One upstream SSE event: `event:` named by the data's `type`, then `data:`.
fn event(data: Value) -> String {
    format!("event: {}\ndata: {data}\n\n", data["type"].as_str().unwrap())
}

fn message(text: &str) -> Value {
    json!({"type":"message","id":"msg_1","status":"completed","role":"assistant","content":[{"type":"output_text","text":text,"annotations":[]}]})
}

fn response(status: &str, output: Value) -> Value {
    let mut response = json!({"id":"resp_1","object":"response","created_at":1700000000,"status":status,"model":MODEL,"output":output});
    if status != "in_progress" {
        response["usage"] = json!({"input_tokens":12,"input_tokens_details":{"cached_tokens":3},"output_tokens":5,"output_tokens_details":{"reasoning_tokens":2},"total_tokens":17});
    }
    response
}

/// Grok's message events for "hello", without the terminal event.
fn message_events() -> String {
    let at = |kind: &str, extra: Value| {
        let mut data = json!({"type": kind, "item_id": "msg_1", "output_index": 0, "content_index": 0});
        data.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
        event(data)
    };
    [
        event(json!({"type":"response.created","sequence_number":0,"response":response("in_progress", json!([]))})),
        event(
            json!({"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"msg_1","status":"in_progress","role":"assistant","content":[]}}),
        ),
        at("response.content_part.added", json!({"part":{"type":"output_text","text":""}})),
        at("response.output_text.delta", json!({"delta":"hel"})),
        at("response.output_text.delta", json!({"delta":"lo"})),
        at("response.output_text.done", json!({"text":"hello"})),
        event(json!({"type":"response.output_item.done","output_index":0,"item":message("hello")})),
    ]
    .concat()
}

/// A complete Grok stream whose completion carries `output`.
fn sse_with(output: Value) -> String {
    message_events() + &event(json!({"type":"response.completed","response":response("completed", output)}))
}

/// A complete Grok stream; the completion repeats the message, as the official API does.
fn sse() -> String {
    sse_with(json!([message("hello")]))
}

#[path = "exec_xai/failures.rs"]
mod failures;
#[path = "exec_xai/flow.rs"]
mod flow;
#[path = "exec_xai/heap.rs"]
mod heap;
#[path = "exec_xai/parity.rs"]
mod parity;
#[path = "exec_xai/replay.rs"]
mod replay;
#[path = "exec_xai/socket.rs"]
mod socket;
