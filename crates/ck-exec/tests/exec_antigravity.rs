//! The Antigravity executor through the pipeline against a local upstream, checked against
//! CLIProxyAPI's recorded behaviour (`tests/fixtures/antigravity_go.json`, owner rulings
//! 65-67): the upstream request each client protocol makes, the refusal each failure
//! returns, the usage the canonical stream accounts, and the signatures and text each
//! client receives.

mod support;

use ck_core::config::Config;
use ck_core::credential::Credential;
use ck_core::exec::{ExecError, UsageObserver, UsageSink};
use ck_exec::Executors;
use ck_exec::antigravity_credits as credits;
use ck_exec::pipeline::Started;
use ck_stream::{Read, ScratchLease, Terminal, Writer};
use ck_translate::anthropic;
use ck_translate::codec::chat;
use ck_translate::codec::event::Direction;
use ck_translate::codec::{BodyMode, Operation, RequestIngress, ResponseEgress};
use ck_translate::gemini;
use ck_translate::responses;
use serde_json::Value;
use support::upstream::{Captured, Reply, StreamUpstream};
use support::{Exchange, Published, Streams, identity};

const LIMIT: u64 = 1 << 20;

/// The client protocol a recorded scenario's `source` names.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Client {
    Chat,
    Anthropic,
    Responses,
    Gemini,
}

impl Client {
    fn of(source: &str) -> Self {
        match source {
            "openai" => Self::Chat,
            "claude" => Self::Anthropic,
            "openai-response" => Self::Responses,
            "gemini" => Self::Gemini,
            other => panic!("no client codec for {other}"),
        }
    }
}

fn scenario(name: &str) -> Value {
    let fixture: Value = serde_json::from_str(include_str!("fixtures/antigravity_go.json")).unwrap();
    fixture["executor"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == name)
        .unwrap_or_else(|| panic!("no recorded scenario {name}"))
        .clone()
}

/// The scenario's credential, its `UPSTREAM` placeholders pointing at `addr`.
fn credential(s: &Value, addr: &str) -> Credential {
    let place = |v: &Value| match v {
        Value::String(text) => Value::String(text.replace("UPSTREAM", addr)),
        other => other.clone(),
    };
    let metadata = s["metadata"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(k, v)| (k.clone(), place(v)))
        .collect();
    let dir = std::path::Path::new("/fixture");
    let mut credential = Credential::from_file(dir, &dir.join("antigravity.json"), metadata).unwrap();
    credential.id = s["auth_id"].as_str().unwrap().to_owned();
    for (k, v) in s["attributes"].as_object().into_iter().flatten() {
        credential
            .attributes
            .insert(k.clone(), v.as_str().unwrap().replace("UPSTREAM", addr));
    }
    if s["known_credits"] == true {
        let known = credits::Hint {
            known: true,
            available: true,
            ..credits::Hint::default()
        };
        credits::set_hint(&credential.id, known);
    }
    if s["credits"] == true {
        credential = credits::request_on(&credential);
    }
    credential
}

fn reply(s: &Value) -> Reply {
    let upstream = &s["upstream"];
    let content_type = upstream["headers"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|h| h[0].as_str().unwrap().eq_ignore_ascii_case("content-type"))
        .map_or("application/json", |h| h[1].as_str().unwrap());
    let status = upstream["status"].as_u64().unwrap() as u16;
    Reply::status(status, content_type, upstream["body"].as_str().unwrap().to_owned())
}

/// What one recorded exchange produced on the contract path.
struct Run {
    started: Result<Started, ExecError>,
    client: Client,
    operation: Operation,
    streaming: bool,
    captured: Vec<Captured>,
    _request: Writer,
    /// Kept until the producer has read the answer.
    _upstream: Option<StreamUpstream>,
}

/// Sends the scenario's client request through the selector's Antigravity arm.
async fn run(streams: &Streams, s: &Value) -> Run {
    run_with(streams, s, UsageSink::default()).await
}

/// `run`, with `usage` as the attempt's usage sink.
async fn run_with(streams: &Streams, s: &Value, usage: UsageSink) -> Run {
    let upstream = StreamUpstream::start([reply(s)]).await;
    let run = run_on(streams, s, &upstream, usage).await;
    Run {
        captured: upstream.captured(),
        _upstream: Some(upstream),
        ..run
    }
}

async fn run_on(streams: &Streams, s: &Value, upstream: &StreamUpstream, usage: UsageSink) -> Run {
    let client = Client::of(s["source"].as_str().unwrap());
    let compact = s["alt"] == "responses/compact";
    let operation = match s["op"].as_str().unwrap() {
        "count" => Operation::CountTokens,
        _ if compact => Operation::Compact,
        _ => Operation::Generate,
    };
    let model = s["model"].as_str().unwrap();
    let addr = upstream.url();
    let addr = addr.trim_start_matches("http://");
    let mut exchange = Exchange::new(credential(s, addr), model, "/client");
    exchange.cfg = Config::parse(s["config"].as_str().unwrap_or_default()).unwrap();
    exchange.usage = usage;
    for (k, v) in s["headers"].as_object().into_iter().flatten() {
        exchange.headers.insert(
            http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
            v.as_str().unwrap().parse().unwrap(),
        );
    }
    let mut body: Value = serde_json::from_str(s["payload"].as_str().unwrap()).unwrap();
    // The compact route takes no `stream`; CLIProxyAPI answers it unstreamed.
    if compact {
        body.as_object_mut().unwrap().remove("stream");
    }
    let body = serde_json::to_vec(&body).unwrap();
    // A Gemini client states streaming in its route only, so it always streams upstream.
    let streaming = s["op"] == "stream" || (client == Client::Gemini && operation == Operation::Generate);
    let (store, id) = (&streams.store, identity(Direction::Request));
    let mut writer = streams.request();
    let response = streams.response(&writer);
    let executors = Executors {
        claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
        codex: Default::default(),
        devices: Default::default(),
        openai: Default::default(),
        google: Default::default(),
    };
    let attempt = exchange.attempt(store);
    assert_eq!(executors.destination(&attempt), Some("antigravity"));
    macro_rules! start {
        ($ingress:expr) => {{
            let mut ingress = $ingress.unwrap();
            ingress.advance(&mut writer, &body).unwrap();
            let validated = executors
                .fence(&attempt, &mut ingress, &mut writer)
                .expect("the fence accepts");
            let validated = executors
                .refence(&attempt, validated.view())
                .expect("the re-fence accepts");
            let request = exchange.request(validated);
            executors.start(&attempt, &request, &mut writer, response).await
        }};
    }
    let started = match client {
        Client::Chat => start!(chat::request::Ingress::new(&mut writer, store, id, operation, LIMIT)),
        Client::Anthropic => start!(anthropic::request::Ingress::new(
            &mut writer,
            store,
            id,
            operation,
            LIMIT
        )),
        Client::Responses => start!(responses::request::Ingress::new(
            &mut writer,
            store,
            id,
            operation,
            LIMIT
        )),
        Client::Gemini => start!(gemini::request::Ingress::new(
            &mut writer,
            store,
            id,
            operation,
            model,
            LIMIT
        )),
    };
    Run {
        started,
        client,
        operation,
        streaming,
        captured: Vec::new(),
        _request: writer,
        _upstream: None,
    }
}

/// CLIProxyAPI's random and versioned values, as its own replay tests normalize them.
fn normalize(text: &str) -> String {
    let rules = [
        (r#""requestId":"agent-[0-9a-f-]{36}""#, r#""requestId":"agent-<uuid>""#),
        (
            r"antigravity/hub/[0-9.]+ darwin/arm64",
            "antigravity/hub/<v> darwin/arm64",
        ),
        (r"(resp|cmp)_ag_compact_[0-9]+", "${1}_ag_compact_<nano>"),
        (r"cpa-ag-compact-v1:[A-Za-z0-9_-]+", "cpa-ag-compact-v1:<capsule>"),
        (r#""(created_at|completed_at|created)":1[0-9]{9}"#, r#""$1":<unix>"#),
    ];
    rules.iter().fold(text.to_owned(), |text, (pattern, with)| {
        regex::Regex::new(pattern)
            .unwrap()
            .replace_all(&text, *with)
            .into_owned()
    })
}

/// The recorded upstream request: request line target, headers and JSON body.
fn recorded_request(s: &Value) -> (String, Vec<(String, String)>, Value) {
    let wire = s["request"].as_str().unwrap();
    let (head, body) = wire.split_once("\r\n\r\n").unwrap();
    let mut lines = head.lines();
    let target = lines.next().unwrap().split(' ').nth(1).unwrap().to_owned();
    let headers = lines
        .filter_map(|l| l.split_once(": "))
        .filter(|(n, _)| !["Host", "Content-Length", "Accept-Encoding"].contains(n))
        .map(|(n, v)| (n.to_owned(), normalize(v)))
        .collect();
    (target, headers, serde_json::from_str(&normalize(body)).unwrap())
}

fn assert_request(name: &str, captured: &Captured, s: &Value) {
    let (target, headers, body) = recorded_request(s);
    assert_eq!(captured.method, "POST", "{name}");
    assert_eq!(captured.path, target, "{name}: request target");
    for (header, value) in headers {
        assert_eq!(captured.header(&header).map(normalize), Some(value), "{name}: {header}");
    }
    let sent: Value = serde_json::from_str(&normalize(&String::from_utf8_lossy(&captured.body))).unwrap();
    assert_eq!(sent, body, "{name}: request body");
}

async fn produce(streams: &Streams, started: Started) -> (Result<(), ExecError>, Published, Writer) {
    let published = streams.read_to_end(started.stream.id()).await;
    let outcome = started.producer.await.expect("the producer does not panic");
    (outcome, published, started.stream)
}

/// The last canonical usage snapshot the response stream holds, if any.
fn usage(published: &Published) -> Option<Value> {
    let record = published
        .records
        .iter()
        .rev()
        .map(|r| String::from_utf8_lossy(r).into_owned())
        .find(|r| r.lines().any(|l| l == "event: x_ck.usage"))?;
    let data = record.lines().find_map(|l| l.strip_prefix("data: "))?;
    let data: Value = serde_json::from_str(data).unwrap();
    Some(data["usage"].clone())
}

/// Checks the canonical snapshot against the usage CLIProxyAPI recorded. CLIProxyAPI counts
/// output without reasoning; the canonical output includes it (`docs/INTERNAL-STREAM.md`
/// §3.5). A reasoning count is compared where the client protocol reports one.
fn assert_usage(name: &str, published: &Published, s: &Value) {
    let want = |key: &str| s["usage"][key].as_u64().unwrap();
    let Some(got) = usage(published) else {
        assert_eq!(want("total_tokens"), 0, "{name}: the upstream reported usage");
        return;
    };
    let count = |key: &str| got[key].as_u64().unwrap_or(0);
    assert_eq!(count("input_tokens"), want("input_tokens"), "{name}: input in {got}");
    assert_eq!(
        count("output_tokens"),
        want("output_tokens") + want("reasoning_tokens"),
        "{name}: output in {got}"
    );
    assert_eq!(count("total_tokens"), want("total_tokens"), "{name}: total in {got}");
    assert_eq!(
        count("cache_read_tokens"),
        want("cached_tokens"),
        "{name}: cache read in {got}"
    );
    if got.get("reasoning_tokens").is_some() {
        assert_eq!(
            count("reasoning_tokens"),
            want("reasoning_tokens"),
            "{name}: reasoning in {got}"
        );
    }
}

fn lease_bytes(lease: &ScratchLease) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(lease.len());
    while bytes.len() < lease.len() {
        bytes.extend_from_slice(lease.chunk(bytes.len()).unwrap());
    }
    bytes
}

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

/// What the client's own egress writes from the response stream.
fn delivered(streams: &Streams, owner: &mut Writer, client: Client, op: Operation, streaming: bool) -> String {
    let id = identity(Direction::Response);
    let mode = if streaming {
        BodyMode::Streaming
    } else {
        BodyMode::Buffered
    };
    match client {
        Client::Chat => {
            let mut egress = chat::egress::Egress::new(owner, id, op, mode, true, |_| {}).unwrap();
            deliver(streams, owner, &mut egress)
        }
        Client::Anthropic => {
            let mut egress = anthropic::egress::Egress::new(owner, id, op, mode, |_| {}).unwrap();
            deliver(streams, owner, &mut egress)
        }
        Client::Responses => {
            let mut egress = support::responses_egress(owner, op, mode).unwrap();
            deliver(streams, owner, &mut egress)
        }
        Client::Gemini => {
            let mut egress = gemini::egress::Egress::new(owner, id, op, mode, |_| {}).unwrap();
            deliver(streams, owner, &mut egress)
        }
    }
}

/// Every JSON string under one of `keys` in `text`, which holds JSON documents and SSE `data:`
/// lines.
fn strings(text: &str, keys: &[&str]) -> Vec<String> {
    fn walk(value: &Value, keys: &[&str], out: &mut Vec<String>) {
        match value {
            Value::Object(map) => {
                for (k, v) in map {
                    match v {
                        Value::String(s) if keys.contains(&k.as_str()) && !s.is_empty() => out.push(s.clone()),
                        _ => walk(v, keys, out),
                    }
                }
            }
            Value::Array(items) => items.iter().for_each(|v| walk(v, keys, out)),
            _ => {}
        }
    }
    let mut out = Vec::new();
    let documents = std::iter::once(text).chain(text.lines().filter_map(|l| l.strip_prefix("data:")));
    for document in documents {
        if let Ok(value) = serde_json::from_str::<Value>(document.trim()) {
            walk(&value, keys, &mut out);
        }
    }
    out
}

/// The client receives every signature CLIProxyAPI gave it and every text the upstream sent.
fn assert_client_output(name: &str, delivered: &str, s: &Value) {
    let mut recorded = s["output"].as_str().unwrap_or_default().to_owned();
    for chunk in s["chunks"].as_array().into_iter().flatten() {
        recorded.push('\n');
        recorded.push_str(chunk.as_str().unwrap());
    }
    let delivered = &normalize(delivered);
    // Ruling 65: a Claude model's signature reaches the client as the upstream sent it, where
    // CLIProxyAPI decodes it; Gemini models keep CLIProxyAPI's carriers.
    let signatures = if s["model"].as_str().unwrap().contains("claude") {
        strings(s["upstream"]["body"].as_str().unwrap(), &["thoughtSignature"])
    } else {
        strings(&recorded, &["signature", "encrypted_content", "thoughtSignature"])
    };
    for signature in signatures {
        let signature = normalize(&signature);
        assert!(
            delivered.contains(&signature),
            "{name}: signature {signature} missing from {delivered}"
        );
    }
    for text in strings(s["upstream"]["body"].as_str().unwrap(), &["text"]) {
        if !recorded.contains(&text) {
            continue;
        }
        assert!(
            delivered.contains(&text) || delivered.contains(&serde_json::to_string(&text).unwrap()[1..]),
            "{name}: text {text:?} missing from {delivered}"
        );
    }
}

/// Runs one recorded scenario and compares every observable with the recording.
async fn replay(name: &str) {
    let s = scenario(name);
    let streams = Streams::new();
    let run = run(&streams, &s).await;
    assert_request(name, run.captured.first().expect("one upstream request"), &s);
    if let Some(want) = s["error"].as_object() {
        let error = run.started.err().expect("the upstream refusal fails the attempt");
        assert_eq!(error.status as u64, want["status"].as_u64().unwrap(), "{name}: status");
        assert_eq!(
            normalize(&String::from_utf8_lossy(&error.body)),
            normalize(want["message"].as_str().unwrap()),
            "{name}: body"
        );
        let retry = want.get("retry_after_ms").and_then(Value::as_u64);
        assert_eq!(
            error.retry_after.map(|d| d.as_millis() as u64),
            retry,
            "{name}: retry after"
        );
        return;
    }
    let Run {
        started,
        client,
        operation,
        streaming,
        ..
    } = run;
    let started = started.unwrap_or_else(|e| panic!("{name}: {} {}", e.status, String::from_utf8_lossy(&e.body)));
    let (outcome, published, mut owner) = produce(&streams, started).await;
    outcome.unwrap_or_else(|e| panic!("{name}: {}", String::from_utf8_lossy(&e.body)));
    assert_eq!(published.terminal, Some(Terminal::Done), "{name}");
    if operation == Operation::Generate {
        assert_usage(name, &published, &s);
    }
    let delivered = delivered(&streams, &mut owner, client, operation, streaming);
    assert_client_output(name, &delivered, &s);
}

/// Each row is a recorded CLIProxyAPI exchange: a client protocol, an operation and a model
/// family. Gemini clients appear only streamed, because their route alone says whether
/// they stream and the contract path always streams them.
#[tokio::test]
async fn every_client_protocol_matches_the_recorded_exchange() {
    for name in [
        // Claude models, signed thinking (ruling 65) and the stream merge.
        "claude_stream_claude_model",
        "claude_model_execute",
        "claude_client_claude_model_empty_signature",
        "claude_stream_gemini_model",
        // Credits: the injected flag and the exhausted balance.
        "credits_claude_execute",
        "credits_balance_exhausted",
        // Chat and Responses clients, streamed and unstreamed.
        "openai_stream",
        "openai_execute_tools_schema",
        "responses_stream",
        "responses_execute_reasoning",
        "gemini_stream",
        // Refusals before the answer.
        "stream_429",
        "execute_400",
        // Token counts.
        "gemini_count",
    ] {
        replay(name).await;
    }
}

/// Ruling 67: `/responses/compact`, a compaction trigger and an earlier compaction item are
/// served from a local summary turn, as CLIProxyAPI serves them.
#[tokio::test]
async fn compaction_is_served_locally_as_recorded() {
    for name in [
        "compaction_alt_compact",
        "compaction_trigger_stream",
        "compaction_item_expands",
    ] {
        replay(name).await;
    }
}

/// Ruling 66: the reasoning replay ledger a Claude client's first turn fills, keyed by model
/// and client session, restores the native call on its second turn.
#[tokio::test]
async fn the_replay_ledger_carries_a_session_to_its_next_turn() {
    for name in ["replay_claude_turn1", "replay_claude_turn2_provenance"] {
        replay(name).await;
    }
}

/// The Claude signature `claude_model_execute` answers with (`tests/fixtures/antigravity_go.json`).
const UPSTREAM_SIGNATURE: &str = "RXFzQ2t3SUJHQUlpUUhmYWtlLWNsYXVkZS1zaWduYXR1cmUtZm9yLWFudGlncmF2aXR5LXRlc3Rz";
/// A valid Claude signature in the double-layer form Antigravity mints and the single-layer
/// form CLIProxyAPI hands Claude clients (the `claude-antigravity/seed-cache` golden).
const DOUBLE_LAYER: &str = "RWhrS0Z3Z01FQUl5RVdOc1lYVmtaUzF6YjI1dVpYUXROQzAyR0FFPQ==";
const SINGLE_LAYER: &str = "EhkKFwgMEAIyEWNsYXVkZS1zb25uZXQtNC02GAE=";

/// An Anthropic history whose assistant turn thought "Let me think" under `signature`.
fn anthropic_history(signature: Option<&str>) -> Value {
    let mut thinking = serde_json::json!({"type": "thinking", "thinking": "Let me think"});
    if let Some(signature) = signature {
        thinking["signature"] = signature.into();
    }
    serde_json::json!({
        "model": "claude-sonnet-4-6", "max_tokens": 1024, "stream": true,
        "messages": [
            {"role": "user", "content": "q"},
            {"role": "assistant", "content": [thinking, {"type": "text", "text": "ok"}]},
            {"role": "user", "content": "more"}
        ]
    })
}

/// Sends `payload` from `source` to a Claude model and returns the thought signatures of the
/// upload, in order.
async fn sent_signatures(source: &str, payload: &Value) -> Vec<String> {
    let mut s = scenario("claude_stream_claude_model");
    s["source"] = source.into();
    s["payload"] = payload.to_string().into();
    let streams = Streams::new();
    let run = run(&streams, &s).await;
    let started = run.started.expect("the upstream answers");
    let (outcome, ..) = produce(&streams, started).await;
    outcome.expect("the answer completes");
    let body = String::from_utf8(run.captured[0].body.clone()).unwrap();
    strings(&body, &["thoughtSignature"])
}

/// Ruling 65: signed thinking goes back upstream exactly as the client holds it; CLIProxyAPI
/// would wrap the single-layer form. Thinking without a Claude signature is dropped, as
/// CLIProxyAPI and OmniRoute both drop it, and no cached signature fills the gap.
#[tokio::test]
async fn signed_thinking_goes_upstream_as_the_client_holds_it() {
    let rows: [(&str, Value, &[&str]); 4] = [
        ("claude", anthropic_history(Some(DOUBLE_LAYER)), &[DOUBLE_LAYER]),
        ("claude", anthropic_history(Some(SINGLE_LAYER)), &[SINGLE_LAYER]),
        ("claude", anthropic_history(Some("opaque-signature")), &[]),
        ("claude", anthropic_history(None), &[]),
    ];
    for (source, payload, want) in rows {
        assert_eq!(sent_signatures(source, &payload).await, want, "{source}: {payload}");
    }
}

/// Ruling 65: the client receives a Claude model's thinking and tool-call signatures as the
/// upstream sent them, buffered or streamed, and a later turn that drops the signature does
/// not get it back from a cache.
#[tokio::test]
async fn upstream_signatures_reach_the_client_unchanged_and_no_cache_restores_them() {
    for name in ["claude_model_execute", "claude_stream_claude_model"] {
        let mut s = scenario(name);
        let body = s["upstream"]["body"].as_str().unwrap().replace(
            r#"{"functionCall":{"id":"toolu_01""#,
            &format!(r#"{{"thoughtSignature":"{DOUBLE_LAYER}","functionCall":{{"id":"toolu_01""#),
        );
        s["upstream"]["body"] = body.into();
        let streams = Streams::new();
        let run = run(&streams, &s).await;
        let Run {
            started,
            client,
            operation,
            streaming,
            ..
        } = run;
        let (outcome, _, mut owner) = produce(&streams, started.expect("the upstream answers")).await;
        outcome.expect("the answer completes");
        let delivered = delivered(&streams, &mut owner, client, operation, streaming);
        assert_eq!(
            strings(&delivered, &["signature"]),
            [UPSTREAM_SIGNATURE, DOUBLE_LAYER],
            "{name}: {delivered}"
        );
    }
    assert_eq!(
        sent_signatures("claude", &anthropic_history(None)).await,
        Vec::<String>::new()
    );
}

/// The usage facts an attempt reports, by kind.
#[derive(Default)]
struct Facts(std::sync::Mutex<Vec<&'static str>>);

impl UsageObserver for Facts {
    fn response_body(&self, _: ck_core::format::Format, _: &[u8]) {
        self.0.lock().unwrap().push("response_body");
    }
    fn response_line(&self, _: ck_core::format::Format, _: &[u8]) {
        self.0.lock().unwrap().push("response_line");
    }
    fn request(&self, _: ck_core::format::Format, _: &[u8]) {
        self.0.lock().unwrap().push("request");
    }
}

/// Contract G4: the attempt's usage sink gets request-side facts only; the response's usage
/// reaches accounting through the canonical stream alone, buffered or streamed.
#[tokio::test]
async fn response_usage_reaches_accounting_only_through_the_canonical_stream() {
    for name in ["claude_model_execute", "claude_stream_claude_model"] {
        let s = scenario(name);
        let facts = std::sync::Arc::new(Facts::default());
        let streams = Streams::new();
        let run = run_with(&streams, &s, UsageSink::new(facts.clone())).await;
        let (outcome, published, _) = produce(&streams, run.started.expect("the upstream answers")).await;
        outcome.expect("the answer completes");
        assert!(
            usage(&published).is_some(),
            "{name}: the canonical stream carries the usage"
        );
        assert_eq!(*facts.0.lock().unwrap(), ["request"], "{name}: request-side facts only");
    }
}
