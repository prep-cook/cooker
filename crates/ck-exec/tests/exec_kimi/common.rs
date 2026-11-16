//! Helpers for the Kimi executor tests: the Go fixtures of the old path, the client fence for
//! each protocol, the pipeline run against the DT local upstream, and client egress.

use ck_core::config::Config;
use ck_core::credential::Credential;
use ck_core::exec::ExecError;
use ck_exec::kimi::KimiExecutor;
use ck_exec::pipeline::Started;
use ck_exec::{DeviceExecutors, Executors};
use ck_stream::{Read, Reader, ScratchLease, Terminal, Writer};
use ck_translate::codec::event::Direction;
use ck_translate::codec::{self, BodyMode, Operation, RequestIngress, ResponseEgress};
use ck_translate::{anthropic, codec::chat, gemini, responses};
use serde_json::Value;

use crate::support::upstream::{Captured, Reply, StreamUpstream};
use crate::support::{Exchange, Published, Streams, identity};

const LIMIT: u64 = 1 << 20;

/// The credential every non-fixture test uses, with a fixed device id so no home directory
/// is read.
pub const TOKEN: &str = "kimi-access-fixture";

pub fn fixture(name: &str) -> Value {
    let path = format!("{}/tests/device_fixtures/kimi/{name}.json", env!("CARGO_MANIFEST_DIR"));
    serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap()
}

/// A Kimi OAuth credential whose base URL is `base`: a fixture's `metadata`, or the test
/// defaults when it is null.
pub fn credential(base: &str, metadata: &Value) -> Credential {
    let mut fields = match metadata.as_object() {
        Some(given) => given.clone(),
        None => serde_json::Map::from_iter([
            ("type".to_owned(), Value::from("kimi")),
            ("access_token".to_owned(), Value::from(TOKEN)),
            ("device_id".to_owned(), Value::from("dev-fixture-1")),
        ]),
    };
    fields.insert("base_url".into(), Value::from(base));
    let dir = std::path::Path::new("/fixture");
    let mut credential = Credential::from_file(dir, &dir.join("kimi-fixture.json"), fields).unwrap();
    credential.provider = "kimi".into();
    credential
}

/// An exchange for upstream `model` on a Kimi credential at `upstream`.
pub fn exchange(upstream: &StreamUpstream, model: &str) -> Exchange {
    let base = format!("{}/coding", upstream.url());
    Exchange::new(credential(&base, &Value::Null), model, "")
}

/// The exchange a Go fixture ran: its credential, config, model and client headers.
pub fn fixture_exchange(upstream: &StreamUpstream, fx: &Value) -> Exchange {
    let request = &fx["request"];
    let base = format!("{}/coding", upstream.url());
    let mut exchange = exchange(upstream, request["model"].as_str().unwrap());
    exchange.credential = credential(&base, &fx["credential"]);
    for (name, value) in fx["attributes"].as_object().into_iter().flatten() {
        let value = value.as_str().unwrap_or_default().to_owned();
        exchange.credential.attributes.insert(name.clone(), value);
    }
    exchange.cfg = Config::parse(request["config"].as_str().unwrap_or_default()).unwrap();
    for header in request["headers"].as_array().into_iter().flatten() {
        let name = http::HeaderName::from_bytes(header[0].as_str().unwrap().as_bytes()).unwrap();
        exchange
            .headers
            .append(name, header[1].as_str().unwrap().parse().unwrap());
    }
    exchange
}

/// The fixture's scripted upstream answers.
pub fn replies(fx: &Value) -> Vec<Reply> {
    let mut replies = Vec::new();
    for reply in fx["responses"].as_array().into_iter().flatten() {
        let headers: Vec<(&str, &str)> = reply["headers"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|h| (h[0].as_str().unwrap(), h[1].as_str().unwrap()))
            .collect();
        let content_type = headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case("content-type"))
            .map_or("application/json", |(_, v)| *v);
        let status = reply["status"].as_u64().unwrap() as u16;
        replies.push(Reply::status(
            status,
            content_type,
            reply["body"].as_str().unwrap().to_owned(),
        ));
    }
    replies
}

/// The client protocol a fixture's `source` format names.
pub fn protocol(source: &str) -> &'static str {
    match source {
        "openai" => "chat",
        "openai-response" => "responses",
        "claude" => "anthropic",
        "gemini" => "gemini",
        other => panic!("no client protocol for {other}"),
    }
}

/// The selector with a Kimi executor whose client trusts the local upstream.
pub fn executors() -> Executors {
    let devices = DeviceExecutors {
        kimi: KimiExecutor::with_client(ck_exec::proxy::default_client()),
        ..Default::default()
    };
    Executors {
        claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
        codex: Default::default(),
        devices,
        openai: Default::default(),
        google: Default::default(),
    }
}

/// One attempt through the selector: `body` fed `step` bytes at a time, the client fence and
/// the attempt's re-fence, then the pipeline. A refusal comes back as `Err`.
async fn attempt<I: RequestIngress>(
    streams: &Streams,
    exchange: &Exchange,
    ingress: &mut I,
    request: &mut Writer,
    body: &[u8],
    step: usize,
) -> codec::Result<(Result<Started, ExecError>, u64)> {
    let response = streams.response(request);
    let id = response.id();
    let executors = executors();
    let attempt = exchange.attempt(&streams.store);
    assert_eq!(executors.destination(&attempt), Some("kimi"));
    for chunk in body.chunks(step) {
        ingress.advance(request, chunk)?;
    }
    let fenced = executors.fence(&attempt, ingress, request)?;
    let validated = executors.refence(&attempt, fenced.view())?;
    let handle = exchange.request(validated);
    Ok((executors.start(&attempt, &handle, request, response).await, id))
}

/// Runs `operation` from a `protocol` client: the fence's outcome, or the pipeline's start
/// and the response stream id. The request stream comes back too; it must outlive the response.
pub async fn try_run(
    streams: &Streams,
    exchange: &Exchange,
    protocol: &str,
    operation: Operation,
    body: &[u8],
    step: usize,
) -> (codec::Result<(Result<Started, ExecError>, u64)>, Writer) {
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
            let model = exchange.model.as_str();
            let mut ingress = gemini::request::Ingress::new(&mut request, store, id, operation, model, LIMIT).unwrap();
            attempt(streams, exchange, &mut ingress, &mut request, body, step).await
        }
        other => panic!("no client protocol {other}"),
    };
    (outcome, request)
}

/// A generation the fence accepts: the pipeline's start, the response stream id and the
/// request stream.
pub async fn run(
    streams: &Streams,
    exchange: &Exchange,
    protocol: &str,
    body: &[u8],
    step: usize,
) -> (Result<Started, ExecError>, u64, Writer) {
    let (outcome, request) = try_run(streams, exchange, protocol, Operation::Generate, body, step).await;
    let (started, id) = outcome.expect("Kimi accepts the request");
    (started, id, request)
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

/// The number of canonical `error` records.
pub fn errors(published: &Published) -> usize {
    records(published)
        .iter()
        .filter(|r| r.starts_with("event: x_ck.error\n"))
        .count()
}

fn lease_bytes(lease: &ScratchLease) -> Vec<u8> {
    let mut out = Vec::with_capacity(lease.len());
    while out.len() < lease.len() {
        out.extend_from_slice(lease.chunk(out.len()).unwrap());
    }
    out
}

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

/// What the `protocol` client egress writes for the whole response stream `id`.
pub async fn egress(streams: &Streams, protocol: &str, stream: &mut Writer, id: u64, mode: BodyMode) -> String {
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

/// Headers whose values depend on the machine, the port or the build.
const MASKED: [&str; 5] = [
    "host",
    "content-length",
    "x-msh-device-name",
    "x-msh-device-model",
    "user-agent",
];

/// Asserts the request Go's old path sent: target, header names and values in order (the
/// masked ones by name only), and a JSON-equal body.
pub fn assert_same_request(name: &str, go: &Value, ours: &Captured) {
    assert_eq!(ours.method, go["method"].as_str().unwrap(), "{name}: method");
    assert_eq!(ours.path, go["target"].as_str().unwrap(), "{name}: target");
    let go_headers: Vec<(String, String)> = go["headers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| (h[0].as_str().unwrap().to_owned(), h[1].as_str().unwrap().to_owned()))
        .collect();
    let names =
        |headers: &[(String, String)]| -> Vec<String> { headers.iter().map(|(n, _)| n.to_ascii_lowercase()).collect() };
    assert_eq!(names(&ours.headers), names(&go_headers), "{name}: header order");
    // A Go run whose credential named no device id read it from the generator's home.
    let home_device = go_headers.iter().any(|(_, v)| v == "cli-proxy-api-device");
    for ((n, ours), (_, go)) in ours.headers.iter().zip(&go_headers) {
        let n = n.to_ascii_lowercase();
        if n == "x-msh-version" || (n == "x-msh-device-id" && home_device) {
            assert!(!ours.is_empty(), "{name}: {n}");
        } else if !MASKED.contains(&n.as_str()) {
            assert_eq!(ours, go, "{name}: header {n}");
        }
    }
    let user_agent = ours.header("user-agent").unwrap();
    assert!(user_agent.starts_with("CLIProxyAPI/"), "{name}: {user_agent}");
    let go_body: Value = serde_json::from_str(go["body"].as_str().unwrap()).unwrap();
    let body: Value = serde_json::from_slice(&ours.body).unwrap();
    assert_eq!(body, go_body, "{name}: body");
}

/// The request the old path sends for `body` from a `source` client on `exchange`'s
/// credential, config and model, at `upstream`.
pub async fn old_upload(upstream: &StreamUpstream, exchange: &Exchange, source: &str, body: &[u8]) -> Captured {
    old_upload_resolved(upstream, exchange, source, body, None).await
}

/// [`old_upload`] with the registry facts the server attaches to an attempt.
pub async fn old_upload_resolved(
    upstream: &StreamUpstream,
    exchange: &Exchange,
    source: &str,
    body: &[u8],
    resolved_model: Option<ck_core::exec::ResolvedModel>,
) -> Captured {
    let format = ck_core::format::Format::parse(source).unwrap();
    let body = bytes::Bytes::copy_from_slice(body);
    let stream = serde_json::from_slice::<Value>(&body).unwrap()["stream"] == Value::Bool(true);
    let request = ck_core::exec::ExecRequest {
        operation: ck_core::exec::Operation::Generate,
        source_format: format,
        response_format: format,
        requested_model: exchange.model.clone(),
        model: exchange.model.clone(),
        original_body: body.clone(),
        body,
        stream,
        alt: None,
        session: None,
        execution_session: None,
        derived_session: None,
        request_path: String::new(),
        headers: exchange.headers.clone(),
        caller: exchange.caller.clone(),
        resolved_model,
        usage: Default::default(),
    };
    let claude = ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap();
    let executor = KimiExecutor::with_client(ck_exec::proxy::default_client());
    let before = upstream.captured().len();
    let response = executor
        .execute(&claude, &exchange.credential, request, &exchange.cfg)
        .await
        .unwrap();
    if let ck_core::exec::ResponseBody::Stream(mut stream) = response.body {
        while futures_util::StreamExt::next(&mut stream).await.is_some() {}
    }
    upstream.captured().remove(before)
}
