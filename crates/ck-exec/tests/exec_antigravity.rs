//! The Antigravity executor through the pipeline against a local upstream, checked against
//! CLIProxyAPI's recorded behaviour (`tests/fixtures/antigravity_go.json`, owner rulings
//! 68-70): the upstream request each client protocol makes, the refusal each failure
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

/// Every recorded CLIProxyAPI exchange, in recording order: the base set, minus the
/// scenarios v8.0.35 re-recorded under the same name, then the v8.0.27 and v8.0.35 sets
/// (the composition `antigravity::tests` replays on the old path).
fn recorded() -> Vec<Value> {
    let set = |raw: &str| -> Vec<Value> {
        let fixture: Value = serde_json::from_str(raw).unwrap();
        fixture["executor"].as_array().unwrap().clone()
    };
    let newer = set(include_str!("fixtures/antigravity_v8_0_35_go.json"));
    let mut all: Vec<Value> = set(include_str!("fixtures/antigravity_go.json"))
        .into_iter()
        .filter(|s| !newer.iter().any(|n| n["name"] == s["name"]))
        .collect();
    all.extend(set(include_str!("fixtures/antigravity_v8_0_27_go.json")));
    all.extend(newer);
    all
}

fn scenario(name: &str) -> Value {
    recorded()
        .into_iter()
        .find(|s| s["name"] == name)
        .unwrap_or_else(|| panic!("no recorded scenario {name}"))
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
    let body = upstream["body"].as_str().unwrap();
    let reply = Reply::status(status, content_type, body.to_owned());
    // `missing`: the upstream promised that many more bytes and closed the connection.
    match upstream["missing"].as_u64() {
        Some(missing) => reply.short_by(missing as usize),
        None => reply,
    }
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
    // The operation, not the body, picks CLIProxyAPI's unstreamed compact; the compact route
    // takes no `stream`.
    if compact {
        body.as_object_mut().unwrap().remove("stream");
    }
    let body = serde_json::to_vec(&body).unwrap();
    // A Gemini client states streaming in its route's action.
    let streaming = s["op"] == "stream";
    if client == Client::Gemini {
        exchange.streaming = Some(streaming);
    }
    if !compact {
        exchange.alt = s["alt"].as_str().filter(|a| !a.is_empty()).map(str::to_owned);
    }
    exchange.derived_session = s["derived_session"].as_str().map(str::to_owned);
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
            r#""requestId":"image_gen/[0-9]{13}/[0-9a-f-]{36}/12""#,
            r#""requestId":"image_gen/<ms>/<uuid>/12""#,
        ),
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
/// §3.5). A reasoning count and a total are compared where the client protocol reports one.
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
    // A protocol without a total (Anthropic) leaves the canonical total derived.
    let derived = |key: &str| got["derived"].as_array().is_some_and(|d| d.iter().any(|k| k == key));
    if !derived("total_tokens") {
        assert_eq!(count("total_tokens"), want("total_tokens"), "{name}: total in {got}");
    }
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
    // Ruling 68: a Claude model's signature reaches the client as the upstream sent it, where
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
async fn replay(s: &Value) {
    let name = s["name"].as_str().unwrap();
    let streams = Streams::new();
    let run = run(&streams, s).await;
    match s["request"].is_null() {
        true => assert!(run.captured.is_empty(), "{name}: CLIProxyAPI sent nothing upstream"),
        false => assert_request(name, run.captured.first().expect("one upstream request"), s),
    }
    if let Some(want) = s["error"].as_object() {
        // A refusal before the answer fails the attempt; one after it ends the producer.
        let error = match run.started {
            Err(error) => error,
            Ok(started) => produce(&streams, started)
                .await
                .0
                .expect_err("the upstream failure ends the answer"),
        };
        // Go's status 0 is an error without a status code (a scanner error); a Rust error
        // always carries one, so only the message is compared.
        let status = want["status"].as_u64().unwrap();
        if status != 0 {
            assert_eq!(error.status as u64, status, "{name}: status");
        }
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
        assert_usage(name, &published, s);
    }
    let delivered = delivered(&streams, &mut owner, client, operation, streaming);
    assert_client_output(name, &delivered, s);
}

/// Recorded exchanges the contract path answers differently, with the reason. Each must
/// still differ from its recording, so an entry cannot outlive its difference.
const DEVIATIONS: &[(&str, &str)] = &[
    (
        "gemini_function_response_roles",
        "the Gemini request codec refuses the request at the fence (Schema); D2 keeps it on the old arm",
    ),
    (
        "gemini_function_response_name_match",
        "the Gemini request codec refuses the request at the fence (Schema); D2 keeps it on the old arm",
    ),
    (
        "trailing_thought_plain_summary",
        "the Gemini request codec refuses the request at the fence (Schema); D2 keeps it on the old arm",
    ),
    (
        "trailing_thought_with_call",
        "the Gemini request codec drops `thought: true` from a function-call part",
    ),
    (
        "image_model_execute",
        "the canonical stream decodes inline media, and the recorded image data `iVBOR` is not base64",
    ),
    (
        "gemini_stream_alt",
        "with `alt` on a stream the Antigravity steps answer in JSON framing, which the Gemini \
         response ingress (SSE) does not read; D2 keeps such requests on the old arm",
    ),
    (
        "go_stream_empty_gemini",
        "an empty answer ends the attempt at the response ingress; the old server called it \
         `empty_stream`, and D2 maps it to an attempt failure",
    ),
    (
        "go_stream_empty_openai",
        "an empty answer ends the attempt at the response ingress; the old server called it \
         `empty_stream`, and D2 maps it to an attempt failure",
    ),
    (
        "stream_split_payload_gemini",
        "the Antigravity steps forward the joined payload with its line breaks, which is not one \
         SSE event for the Gemini response ingress",
    ),
    (
        "replay_after_completed_gemini",
        "the Gemini response ingress refuses a chunk after the finish reason, which CLIProxyAPI forwards",
    ),
    (
        "replay_apply_patch_failure_at_terminal",
        "the attempt fails with the same 502, but the Responses ingress reports its own message",
    ),
    (
        "stream_split_payload_claude",
        "the canonical stream accounts the usage the upstream reported (contract §8); CLIProxyAPI \
         recorded none",
    ),
    (
        "go_stream_no_finish_reason_gemini",
        "the canonical stream accounts the usage the upstream reported (contract §8); CLIProxyAPI \
         recorded none",
    ),
    (
        "stream_client_drop_mid_read",
        "a client disconnect does not stop the producer (Settled), so the short upstream body \
         fails the attempt where CLIProxyAPI stopped at the drop",
    ),
    (
        "stream_client_drop_after_terminal",
        "a client disconnect does not stop the producer (Settled), so the short upstream body \
         fails the attempt where CLIProxyAPI stopped at the drop",
    ),
    (
        "stream_client_drop_after_terminal_claude",
        "a client disconnect does not stop the producer (Settled), so the short upstream body \
         fails the attempt where CLIProxyAPI stopped at the drop",
    ),
    (
        "replay_drop_before_terminal",
        "a client disconnect does not stop the producer (Settled), so the short upstream body \
         fails the attempt where CLIProxyAPI stopped at the drop",
    ),
    (
        "replay_drop_after_terminal",
        "a client disconnect does not stop the producer (Settled), so the short upstream body \
         fails the attempt where CLIProxyAPI stopped at the drop",
    ),
];

/// Recorded exchanges this harness cannot drive, with the old-path test that replays them.
const NOT_REPLAYED: &[(&str, &str)] = &[
    (
        "execute_in_short_cooldown",
        "needs the cooldown an earlier scenario opened on one executor under a pinned clock \
         (test-only `cooldown_now`); antigravity::tests::go_reference_scenarios",
    ),
    (
        "credits_flag_without_config",
        "needs the cooldown an earlier scenario opened on one executor under a pinned clock \
         (test-only `cooldown_now`); antigravity::tests::go_reference_scenarios",
    ),
    (
        "claude_web_search_grounding",
        "a scripted multi-request upstream with grounding redirects; \
         antigravity::tests::web_search_grounding_matches_go",
    ),
    (
        "compaction_alt_stream_rejected",
        "no contract request expresses a streamed compact: the Responses codec refuses `stream` \
         on compact at the fence; antigravity::tests::go_reference_scenarios",
    ),
    (
        "count_429",
        "no contract request expresses a Chat token count: the Chat codec refuses it \
         (Unsupported); antigravity::tests::go_reference_scenarios",
    ),
    (
        "go_stream_interactions",
        "the contract has no Interactions ingress, so D2 keeps Interactions on the old arm; \
         antigravity::tests::go_reference_scenarios",
    ),
];

/// The text a panic carried.
fn panic_text(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_owned()))
        .unwrap_or_default()
}

/// Every recorded CLIProxyAPI exchange, run through the selector's Antigravity arm in
/// recording order: the upstream request, the refusal or the canonical usage, and what the
/// client receives. This covers every client protocol, credits, refusals, token counts,
/// compaction (ruling 70) and the replay ledger's turns (ruling 69).
#[tokio::test]
async fn every_recorded_exchange_matches_cliproxyapi() {
    let (mut failures, mut deviated) = (Vec::new(), 0);
    for s in recorded() {
        let name = s["name"].as_str().unwrap().to_owned();
        if NOT_REPLAYED.iter().any(|(n, _)| *n == name) {
            continue;
        }
        let outcome = futures_util::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(replay(&s)))
            .await
            .map_err(panic_text);
        match (DEVIATIONS.iter().any(|(n, _)| *n == name), outcome) {
            (false, Ok(())) => {}
            (false, Err(e)) => failures.push(format!("{name}: {e}")),
            (true, Ok(())) => failures.push(format!("{name}: matches its recording; remove it from DEVIATIONS")),
            (true, Err(_)) => deviated += 1,
        }
    }
    assert!(
        failures.is_empty(),
        "{} exchanges differ:\n{}",
        failures.len(),
        failures.join("\n")
    );
    assert_eq!(
        deviated,
        DEVIATIONS.len(),
        "every DEVIATIONS entry names a recorded exchange"
    );
    let names: Vec<Value> = recorded().into_iter().map(|s| s["name"].clone()).collect();
    for (name, _) in NOT_REPLAYED {
        assert!(
            names.contains(&Value::from(*name)),
            "NOT_REPLAYED names no recorded exchange: {name}"
        );
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

/// A Responses history whose assistant turn thought "Let me think" under `signature`, carried
/// as the reasoning item's `encrypted_content`.
fn responses_history(signature: Option<&str>) -> Value {
    let mut reasoning = serde_json::json!({
        "type": "reasoning", "id": "rs_1",
        "summary": [{"type": "summary_text", "text": "Let me think"}]
    });
    if let Some(signature) = signature {
        reasoning["encrypted_content"] = signature.into();
    }
    serde_json::json!({
        "model": "claude-sonnet-4-6", "stream": true,
        "input": [
            {"role": "user", "content": "q"},
            reasoning,
            {"role": "assistant", "content": [{"type": "output_text", "text": "ok"}]},
            {"role": "user", "content": "more"}
        ]
    })
}

/// A Responses history whose assistant turn called a tool under `signature`, carried by the
/// reasoning item a Responses client received just before the call.
fn responses_tool_history(signature: &str) -> Value {
    serde_json::json!({
        "model": "claude-sonnet-4-6", "stream": true,
        "tools": [{"type": "function", "name": "Read", "parameters": {"type": "object"}}],
        "input": [
            {"role": "user", "content": "q"},
            {"type": "reasoning", "id": "rs_r_detached_before_0", "summary": [], "encrypted_content": signature},
            {"type": "function_call", "call_id": "t1", "name": "Read", "arguments": "{\"a\":1}"},
            {"type": "function_call_output", "call_id": "t1", "output": "done"}
        ]
    })
}

/// A Gemini history whose model turn holds `part`.
fn gemini_history(part: Value) -> Value {
    serde_json::json!({
        "tools": [{"functionDeclarations": [{"name": "Read", "parameters": {"type": "object"}}]}],
        "contents": [
            {"role": "user", "parts": [{"text": "q"}]},
            {"role": "model", "parts": [part]},
            {"role": "user", "parts": [{"text": "more"}]}
        ]
    })
}

/// A Gemini thinking part signed with `signature` in Gemini's OpenAI-compatible nested field.
fn gemini_nested_thinking(signature: &str) -> Value {
    gemini_history(serde_json::json!({
        "thought": true, "text": "Let me think",
        "extra_content": {"google": {"thought_signature": signature}}
    }))
}

/// A Gemini function call signed with `signature`.
fn gemini_tool_call(signature: &str) -> Value {
    gemini_history(serde_json::json!({
        "functionCall": {"name": "Read", "args": {"a": 1}}, "thoughtSignature": signature
    }))
}

/// An Anthropic history whose assistant turn called a tool under `signature`.
fn tool_history(signature: &str) -> Value {
    serde_json::json!({
        "model": "claude-sonnet-4-6", "max_tokens": 1024, "stream": true,
        "tools": [{"name": "Read", "input_schema": {"type": "object"}}],
        "messages": [
            {"role": "user", "content": "q"},
            {"role": "assistant", "content": [
                {"type": "tool_use", "id": "t1", "name": "Read", "input": {"a": 1}, "signature": signature}
            ]},
            {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "t1", "content": "done"}]}
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

/// Ruling 68: signed thinking goes back upstream exactly as the client holds it, byte for byte,
/// on every client protocol that carries it (Chat carries none); CLIProxyAPI would wrap the
/// single-layer form. Only CLIProxyAPI's legacy `<group>#` prefix comes off. Thinking without a
/// Claude signature is dropped, as CLIProxyAPI and OmniRoute both drop it, and no cached
/// signature fills the gap. A tool-call signature a Gemini or Responses client replays itself is
/// withheld, since no producer proves who minted it (stored history: `ck-translate`
/// `history_tests`); the Anthropic path still sends it, the open conflict in PR #170.
#[tokio::test]
async fn signed_thinking_goes_upstream_as_the_client_holds_it() {
    let prefixed = format!("claude#{SINGLE_LAYER}");
    let padded = format!(" \n{SINGLE_LAYER}\r\n");
    let padded_sent = [padded.as_str()];
    let rows: Vec<(&str, Value, &[&str])> = vec![
        ("claude", anthropic_history(Some(DOUBLE_LAYER)), &[DOUBLE_LAYER]),
        ("claude", anthropic_history(Some(SINGLE_LAYER)), &[SINGLE_LAYER]),
        ("claude", anthropic_history(Some(&prefixed)), &[SINGLE_LAYER]),
        ("claude", anthropic_history(Some("opaque-signature")), &[]),
        ("claude", anthropic_history(None), &[]),
        ("claude", tool_history(DOUBLE_LAYER), &[DOUBLE_LAYER]),
        ("claude", tool_history(SINGLE_LAYER), &[SINGLE_LAYER]),
        ("claude", tool_history(&prefixed), &[SINGLE_LAYER]),
        (
            "openai-response",
            responses_history(Some(DOUBLE_LAYER)),
            &[DOUBLE_LAYER],
        ),
        (
            "openai-response",
            responses_history(Some(SINGLE_LAYER)),
            &[SINGLE_LAYER],
        ),
        ("openai-response", responses_history(Some(&prefixed)), &[SINGLE_LAYER]),
        ("openai-response", responses_history(Some("opaque-signature")), &[]),
        ("openai-response", responses_history(None), &[]),
        ("openai-response", responses_history(Some(&padded)), &padded_sent),
        ("openai-response", responses_tool_history(DOUBLE_LAYER), &[]),
        ("openai-response", responses_tool_history(SINGLE_LAYER), &[]),
        ("gemini", gemini_nested_thinking(DOUBLE_LAYER), &[DOUBLE_LAYER]),
        ("gemini", gemini_nested_thinking(SINGLE_LAYER), &[SINGLE_LAYER]),
        ("gemini", gemini_nested_thinking(&padded), &padded_sent),
        ("gemini", gemini_tool_call(DOUBLE_LAYER), &[]),
        ("claude", anthropic_history(Some(&padded)), &padded_sent),
    ];
    for (source, payload, want) in rows {
        assert_eq!(sent_signatures(source, &payload).await, want, "{source}: {payload}");
    }
}

/// Ruling 68: the client receives a Claude model's thinking and tool-call signatures as the
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

/// Ruling 68 on a Responses client: reasoning items carry the upstream's thinking and tool-call
/// signatures in `encrypted_content` as sent, buffered or streamed, so the client can return
/// them unchanged on the next turn.
#[tokio::test]
async fn a_responses_client_receives_the_upstream_signatures_as_sent() {
    // The second signature keeps the whitespace the upstream sent around it.
    let padded = format!(" \n{DOUBLE_LAYER}\r\n");
    for (stream, call_signature) in [
        (false, DOUBLE_LAYER),
        (true, DOUBLE_LAYER),
        (false, padded.as_str()),
        (true, padded.as_str()),
    ] {
        let mut s = scenario("claude_stream_claude_model");
        s["source"] = "openai-response".into();
        let escaped = serde_json::to_string(call_signature).unwrap();
        let body = s["upstream"]["body"].as_str().unwrap().replace(
            r#"{"functionCall":{"id":"toolu_01""#,
            &format!(r#"{{"thoughtSignature":{escaped},"functionCall":{{"id":"toolu_01""#),
        );
        s["upstream"]["body"] = body.into();
        let mut payload = serde_json::json!({
            "model": "claude-sonnet-4-6", "input": "q", "stream": stream,
            "tools": [{"type": "function", "name": "calc", "parameters": {"type": "object"}}]
        });
        if !stream {
            payload.as_object_mut().unwrap().remove("stream");
            s["op"] = "execute".into();
        }
        s["payload"] = payload.to_string().into();
        let streams = Streams::new();
        let Run {
            started,
            client,
            operation,
            streaming,
            _upstream,
            ..
        } = run(&streams, &s).await;
        let (outcome, _, mut owner) = produce(&streams, started.expect("the upstream answers")).await;
        outcome.expect("the answer completes");
        let delivered = delivered(&streams, &mut owner, client, operation, streaming);
        let mut got = strings(&delivered, &["encrypted_content"]);
        // A streamed item repeats its value in the added, done and completed events.
        let mut seen = std::collections::HashSet::new();
        got.retain(|v| seen.insert(v.clone()));
        assert_eq!(
            got,
            [UPSTREAM_SIGNATURE, call_signature],
            "stream {stream}: {delivered}"
        );
    }
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
