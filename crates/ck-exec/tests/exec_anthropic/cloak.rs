//! Contract G5: a request the OAuth cloak or the Claude Code identity shapes is prepared from
//! the client's own bytes and reaches the upstream exactly as the old route sent it: URL,
//! headers in wire order, and body, `cch` signature and cache markers included. Each route
//! sends to Anthropic's origin through a proxy that refuses every connection, so the capture
//! sink records the request and nothing leaves the host.

use std::path::Path;

use ck_core::credential::Credential;

use super::*;

const OPUS_4_7: &str = "claude-opus-4-7";
const HAIKU: &str = "claude-haiku-4-5-20251001";
const SESSION: &str = "11111111-2222-4333-8444-555555555555";
const ORIGIN: &str = "https://api.anthropic.com";

/// A fake, local-only OAuth credential (the harness fixture), which the cloak serves.
fn oauth() -> Credential {
    let metadata = json!({
        "type": "claude", "access_token": "sk-ant-oat01-FAKE-LOCAL-ONLY",
        "refresh_token": "sk-ant-ort01-FAKE-LOCAL-ONLY", "id_token": "",
        "email": "local-fixture@example.invalid", "account_uuid": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
        "organization_uuid": "12345678-1234-4234-8234-123456789abc", "organization_name": "Local fixture",
        "claude_device_ids": ["0123456789abcdef".repeat(4)],
        "expired": "2099-01-01T00:00:00Z", "last_refresh": "2026-10-01T00:00:00Z",
    });
    let metadata = metadata.as_object().unwrap().clone();
    Credential::from_file(Path::new("/auth"), Path::new("/auth/fixture.json"), metadata).unwrap()
}

/// The measured Claude Code headers of a Haiku Title280 session-title helper.
fn title_helper() -> Vec<(&'static str, &'static str)> {
    vec![
        ("user-agent", "claude-cli/2.1.280 (external, cli)"),
        ("x-app", "cli"),
        ("accept", "application/json"),
        ("content-type", "application/json"),
        ("x-stainless-lang", "js"),
        ("x-stainless-runtime", "node"),
        ("x-stainless-retry-count", "0"),
        ("x-stainless-timeout", "600"),
        ("x-stainless-os", "MacOS"),
        ("x-stainless-arch", "arm64"),
        ("x-stainless-package-version", "0.112.1"),
        ("x-stainless-runtime-version", "v26.3.0"),
        ("anthropic-version", "2023-06-01"),
        ("anthropic-dangerous-direct-browser-access", "true"),
        ("accept-encoding", "gzip, deflate, br, zstd"),
        (
            "anthropic-beta",
            "oauth-2025-04-20,interleaved-thinking-2025-05-14,redact-thinking-2026-02-12,\
             thinking-token-count-2026-05-13,context-management-2025-06-27,prompt-caching-scope-2026-01-05,\
             structured-outputs-2025-12-15,server-side-fallback-2026-06-01,fallback-credit-2026-06-01,\
             cache-diagnosis-2026-04-07",
        ),
        ("x-claude-code-session-id", SESSION),
    ]
}

/// A Title280 helper body: Claude Code's member order and shape.
fn title_body() -> Value {
    let user_id = format!(
        r#"{{"device_id":"{}","account_uuid":"","session_id":"{SESSION}"}}"#,
        "a".repeat(64)
    );
    json!({"model": HAIKU, "max_tokens": 80, "messages": [{"role": "user", "content": "title"}],
        "metadata": {"user_id": user_id},
        "output_config": {"format": {"type": "json_schema", "schema": {"type": "object"}}}})
}

/// The measured headers of a Claude Code main turn: the helper's, with the Claude Code beta.
fn main_turn() -> Vec<(&'static str, &'static str)> {
    let mut headers = title_helper();
    for (name, value) in &mut headers {
        if *name == "anthropic-beta" {
            *value = "claude-code-20250219,interleaved-thinking-2025-05-14";
        }
    }
    headers
}

/// A key the cloak shapes on every request; a cached user id keeps its device id stable.
fn always() -> Credential {
    let mut always = credential("claude", ORIGIN, "sk-ant-api03-test");
    always.attributes.insert("cloak_mode".into(), "always".into());
    always.attributes.insert("cloak_cache_user_id".into(), "true".into());
    always
}

/// One upstream request as the executor sent it.
#[derive(Debug, PartialEq)]
struct Wire {
    url: String,
    headers: Vec<(String, String)>,
    body: String,
}

#[derive(Default)]
struct Recorder(Mutex<Vec<Wire>>);

impl CaptureObserver for Recorder {
    fn record(&self, event: CaptureEvent<'_>) {
        if let CaptureEvent::Request(request) = event {
            // Both routes draw a fresh request id for every request.
            let id = |(k, v): &(String, String)| {
                let fresh = k == "x-client-request-id" && uuid::Uuid::parse_str(v).is_ok();
                (k.clone(), if fresh { "<fresh uuid>".into() } else { v.clone() })
            };
            self.0.lock().unwrap().push(Wire {
                url: request.url.to_owned(),
                headers: request.headers.iter().map(id).collect(),
                body: String::from_utf8_lossy(request.body).into_owned(),
            });
        }
    }
}

/// `credential` at `origin` behind the refusing proxy, with the client's `headers`.
fn at(origin: &str, credential: &Credential, model: &str, headers: &[(&str, &str)]) -> (Fixture, Arc<Recorder>) {
    let mut credential = credential.clone();
    credential
        .attributes
        .insert("proxy_url".into(), "http://127.0.0.1:1".into());
    let mut fixture = Fixture::with(credential, origin, model);
    for (name, value) in headers {
        let name: http::HeaderName = name.parse().unwrap();
        fixture.exchange.headers.append(name, value.parse().unwrap());
    }
    let recorder = Arc::new(Recorder::default());
    fixture.exchange.usage = UsageSink::default().with_capture(CaptureSink::new(recorder.clone()));
    (fixture, recorder)
}

/// The request `body` sends upstream on the old route; a count when `count`.
async fn old_wire(credential: &Credential, model: &str, headers: &[(&str, &str)], body: &Value, count: bool) -> Wire {
    let (fixture, recorder) = at(ORIGIN, credential, model, headers);
    let operation = if count {
        OldOperation::CountTokens
    } else {
        OldOperation::Generate
    };
    let usage = fixture.exchange.usage.clone();
    let req = fixture.old_request(&serde_json::to_vec(body).unwrap(), operation, usage);
    let (credential, cfg) = (&fixture.exchange.credential, &fixture.exchange.cfg);
    let answer = fixture.executors.claude.execute(credential, req, cfg).await;
    assert!(answer.is_err(), "the proxy refuses every connection");
    recorder.0.lock().unwrap().pop().expect("the old route's request")
}

/// The request `body` sends upstream on the contract path; a count when `count`.
async fn new_wire(credential: &Credential, model: &str, headers: &[(&str, &str)], body: &Value, count: bool) -> Wire {
    let (fixture, recorder) = at(ORIGIN, credential, model, headers);
    let attempt = fixture.exchange.attempt(&fixture.streams.store);
    assert_eq!(fixture.executors.destination(&attempt), Some("anthropic"));
    let operation = if count {
        Operation::CountTokens
    } else {
        Operation::Generate
    };
    let (started, _, _request) = fixture.anthropic(&serde_json::to_vec(body).unwrap(), operation).await;
    assert!(started.is_err(), "the proxy refuses every connection");
    recorder.0.lock().unwrap().pop().expect("the contract path's request")
}

#[tokio::test]
async fn shaped_requests_reach_the_upstream_byte_for_byte_as_on_the_old_route() {
    let tools = json!([{"name": "read", "description": "Read a file", "input_schema": {"type": "object"}}]);
    let messages = json!([{"role": "user", "content": "Summarise the plan in one line"}]);
    let cases = [
        (
            "plain",
            SONNET,
            json!({"model": SONNET, "max_tokens": 1024, "system": "Be brief.", "messages": messages}),
        ),
        (
            "client-model",
            SONNET,
            json!({"model": "client-model", "max_tokens": 1024, "messages": messages}),
        ),
        (
            "sampling",
            SONNET,
            json!({"model": SONNET, "max_tokens": 1024, "temperature": 0.5, "top_p": 0.9, "messages": messages}),
        ),
        (
            "adaptive-forced-tool",
            OPUS_4_7,
            json!({"model": OPUS_4_7, "max_tokens": 4096, "thinking": {"type": "adaptive"}, "tools": tools,
                "tool_choice": {"type": "tool", "name": "read"}, "messages": messages}),
        ),
        (
            "disabled-effort",
            OPUS_4_7,
            json!({"model": OPUS_4_7, "max_tokens": 4096, "thinking": {"type": "disabled"},
                "output_config": {"effort": "low"}, "messages": messages}),
        ),
    ];
    for (name, model, body) in cases {
        let mut streamed = body.clone();
        streamed["stream"] = json!(true);
        for credential in [oauth(), always()] {
            let new = new_wire(&credential, model, &[], &streamed, false).await;
            assert_eq!(new, old_wire(&credential, model, &[], &streamed, false).await, "{name}");
            if name == "plain" {
                // The cloak ran: the old route's automatic cache marker (`cpa_owns_cache`).
                assert!(new.body.contains("cache_control"), "{}", new.body);
            }
            let new = new_wire(&credential, model, &[], &body, false).await;
            assert_eq!(
                new,
                old_wire(&credential, model, &[], &body, false).await,
                "{name}: buffered"
            );
            let new = new_wire(&credential, model, &[], &body, true).await;
            assert_eq!(
                new,
                old_wire(&credential, model, &[], &body, true).await,
                "{name}: count"
            );
        }
    }
    // The OAuth token signs `cch` over the body the cloak built from the client's bytes.
    let new = new_wire(
        &oauth(),
        SONNET,
        &[],
        &json!({"model": SONNET, "max_tokens": 1024, "stream": true,
        "messages": messages}),
        false,
    )
    .await;
    assert!(
        new.body.contains("cch=") && !new.body.contains("cch=00000"),
        "{}",
        new.body
    );
    for credential in [oauth(), always()] {
        let new = new_wire(&credential, HAIKU, &title_helper(), &title_body(), true).await;
        assert_eq!(
            new,
            old_wire(&credential, HAIKU, &title_helper(), &title_body(), true).await
        );
    }
}

/// Claude Code on a plain key: detection reads the client's own member order, so the helper
/// goes up without cache markers or `stream`, and both keep the client's session.
#[tokio::test]
async fn claude_code_requests_on_a_key_reach_the_upstream_as_on_the_old_route() {
    let key = credential("claude", ORIGIN, "sk-ant-api03-test");
    let new = new_wire(&key, HAIKU, &title_helper(), &title_body(), false).await;
    assert_eq!(new, old_wire(&key, HAIKU, &title_helper(), &title_body(), false).await);
    let sent: Value = serde_json::from_str(&new.body).unwrap();
    assert!(
        sent.get("stream").is_none() && !new.body.contains("cache_control"),
        "{sent}"
    );
    let mut main = title_body();
    main["stream"] = json!(true);
    main["system"] = json!("You are Claude Code.");
    let new = new_wire(&key, HAIKU, &main_turn(), &main, false).await;
    assert_eq!(new, old_wire(&key, HAIKU, &main_turn(), &main, false).await);
    let session = new
        .headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("x-claude-code-session-id"));
    assert_eq!(session.map(|(_, v)| v.as_str()), Some(SESSION), "{:?}", new.headers);
}

/// A count away from Anthropic is local; a shaped one counts what the old route sanitized.
#[tokio::test]
async fn a_shaped_local_count_matches_the_old_route() {
    let thinking = "Weighing every option in turn before answering. ".repeat(40);
    let assistant = json!({"role": "assistant", "content": [
        {"type": "thinking", "thinking": thinking, "signature": "c2lnbmVk"}, {"type": "text", "text": "ok"}]});
    let body = json!({"model": OPUS_4_7, "max_tokens": 4096, "messages": [{"role": "user", "content": "hi"},
        assistant, {"role": "user", "content": "more"}]});
    let bytes = serde_json::to_vec(&body).unwrap();
    let local = "http://127.0.0.1:1";
    let (fixture, _) = at(local, &oauth(), OPUS_4_7, &[]);
    let req = fixture.old_request(&bytes, OldOperation::CountTokens, UsageSink::default());
    let (credential, cfg) = (&fixture.exchange.credential, &fixture.exchange.cfg);
    let answer = fixture.executors.claude.execute(credential, req, cfg).await.unwrap();
    let ck_core::exec::ResponseBody::Buffered(old) = answer.body else {
        panic!("a local count is buffered")
    };
    let old: Value = serde_json::from_slice(&old).unwrap();
    let (fixture, _) = at(local, &oauth(), OPUS_4_7, &[]);
    let (started, id, _request) = fixture.anthropic(&bytes, Operation::CountTokens).await;
    started.unwrap().producer.await.unwrap().unwrap();
    let (records, _) = events(&fixture.streams, id).await;
    let new = records.iter().find(|(name, _)| name == "x_ck.count_tokens");
    assert_eq!(
        new.map(|(_, data)| &data["input_tokens"]),
        Some(&old["input_tokens"]),
        "{records:?}"
    );
}

/// A buffered generation the cloak may shape runs on the contract path (owner ruling Q20):
/// the fence, the re-fence and the start accept it, and start sends it upstream.
#[tokio::test]
async fn a_buffered_generation_the_cloak_may_shape_takes_the_contract_path() {
    let body = json!({"model": SONNET, "max_tokens": 1024, "messages": [{"role": "user", "content": "hi"}]});
    let bytes = serde_json::to_vec(&body).unwrap();
    let op = Operation::Generate;
    for credential in [oauth(), always()] {
        let (cloak, recorder) = at(ORIGIN, &credential, SONNET, &[]);
        let store = &cloak.streams.store;
        let attempt = cloak.exchange.attempt(store);
        let mut request = cloak.streams.request();
        let id = identity(Direction::Request);
        let mut ingress = anthropic::request::Ingress::new(&mut request, store, id, op, LIMIT).unwrap();
        for byte in bytes.chunks(1) {
            ingress.advance(&mut request, byte).unwrap();
        }
        let view = cloak
            .executors
            .fence(&attempt, &mut ingress, &mut request)
            .unwrap()
            .view();
        let validated = cloak.executors.refence(&attempt, view).unwrap();
        let handle = cloak.exchange.request_with(validated, &bytes);
        let response = cloak.streams.response(&request);
        let started = cloak.executors.start(&attempt, &handle, &mut request, response).await;
        assert!(started.is_err(), "the proxy refuses every connection");
        assert_eq!(recorder.0.lock().unwrap().len(), 1, "start sends the request");
    }
}

/// Claude Code header signals no longer pick the old arm (contract G6): the executor reads the
/// client's own body instead (G5).
#[tokio::test]
async fn claude_code_header_signals_take_the_contract_path() {
    let signals = [
        ("user-agent", "claude-cli/2.1.280 (external, cli)"),
        ("x-app", "cli"),
        ("x-claude-code-session-id", SESSION),
        ("x-stainless-lang", "js"),
        ("anthropic-beta", "claude-code-20250219"),
        ("anthropic-beta", "oauth-2025-04-20"),
    ];
    let mut fixture = Fixture::new(&StreamUpstream::start([]).await, HAIKU);
    for (name, value) in signals {
        fixture.exchange.headers = http::HeaderMap::from_iter([(name.parse().unwrap(), value.parse().unwrap())]);
        let attempt = fixture.exchange.attempt(&fixture.streams.store);
        assert_eq!(fixture.executors.destination(&attempt), Some("anthropic"), "{name}");
    }
}

/// A Codex client's integer tool parameters are rewritten before the cloak renames the tools,
/// as on the old route (`normalize_codex_tool_integer_types`).
#[tokio::test]
async fn a_codex_clients_tool_integers_are_rewritten_before_the_cloak() {
    let tools = json!([{"name": "exec_command", "description": "Run a command", "input_schema":
        {"type": "object", "properties": {"yield_time_ms": {"type": "number"}}}}]);
    let mut body = json!({"model": SONNET, "max_tokens": 1024, "tools": tools,
        "messages": [{"role": "user", "content": "run ls"}]});
    let codex = [("user-agent", "codex_cli_rs/0.99")];
    let new = new_wire(&oauth(), SONNET, &codex, &body, true).await;
    assert_eq!(new, old_wire(&oauth(), SONNET, &codex, &body, true).await, "count");
    body["stream"] = json!(true);
    let new = new_wire(&oauth(), SONNET, &codex, &body, false).await;
    assert_eq!(new, old_wire(&oauth(), SONNET, &codex, &body, false).await);
    assert!(new.body.contains("\"integer\""), "{}", new.body);
}

/// Thread aliases and session continuity are recorded only for a response the canonical
/// ingress completed: a continuation of a rejected turn finds no thread state, one of a
/// completed turn reaches the upstream.
#[tokio::test]
async fn only_a_completed_turn_records_thread_state() {
    // `message_stop` without the finishing `message_delta`: the ingress rejects it.
    const CUT: &str = concat!(
        "event: message_start\n",
        r#"data: {"type":"message_start","message":{"id":"msg_cut","type":"message","role":"assistant","model":"claude-sonnet-4-5-20250929","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":1}}}"#,
        "\n\nevent: message_stop\n",
        r#"data: {"type":"message_stop"}"#,
        "\n\n",
    );
    let tools = json!([{"name": "Read", "description": "Read a file", "input_schema": {"type": "object"}}]);
    let messages = json!([{"role": "user", "content": "read it"}]);
    let create = serde_json::to_vec(&json!({"model": SONNET, "max_tokens": 1024, "stream": true,
        "thread": {"type": "create"}, "tools": tools, "messages": messages}))
    .unwrap();
    let after = |id: &str| {
        let thread = json!({"type": "continue", "previous_message_id": id});
        let body = json!({"model": SONNET, "max_tokens": 1024, "stream": true, "thread": thread, "messages": messages});
        serde_json::to_vec(&body).unwrap()
    };
    let upstream = StreamUpstream::start([Reply::sse(CUT), Reply::sse(SSE), Reply::sse(SSE)]).await;
    let fixture = Fixture::with(oauth(), &upstream.url(), SONNET);
    let (started, _, _request) = fixture.anthropic(&create, Operation::Generate).await;
    assert!(
        started.unwrap().producer.await.unwrap().is_err(),
        "the ingress rejects the cut turn"
    );
    let (started, _, _request) = fixture.anthropic(&after("msg_cut"), Operation::Generate).await;
    assert!(started.is_err(), "no thread state for a rejected turn");
    assert_eq!(upstream.requests(), 1);
    let (started, _, _request) = fixture.anthropic(&create, Operation::Generate).await;
    started.unwrap().producer.await.unwrap().unwrap();
    let (started, _, _request) = fixture.anthropic(&after("msg_1"), Operation::Generate).await;
    started.unwrap().producer.await.unwrap().unwrap();
    assert_eq!(upstream.requests(), 3);
}

/// A buffered cloaked turn (owner ruling Q20): the client reads its own tool names, and a
/// continuation of the turn finds the thread the completed response recorded. A name that
/// cannot be restored fails the attempt.
#[tokio::test]
async fn a_buffered_cloaked_turn_restores_tool_names_and_records_its_thread() {
    let schema = json!({"type": "object"});
    let tools = json!([{"name": "lookup_file", "input_schema": schema}, {"name": "Bash", "input_schema": schema},
        {"name": "mcp__a__query", "input_schema": schema}, {"name": "mcp__b__query", "input_schema": schema}]);
    let messages = json!([{"role": "user", "content": "read it"}]);
    let create = serde_json::to_vec(&json!({"model": SONNET, "max_tokens": 1024,
        "thread": {"type": "create"}, "tools": tools, "messages": messages}))
    .unwrap();
    // The alias the cloak sends for `lookup_file`, learnt from a first upstream.
    let learn = StreamUpstream::start([Reply::json(MESSAGE)]).await;
    let fixture = Fixture::with(oauth(), &learn.url(), SONNET);
    let (started, _, _request) = fixture.anthropic(&create, Operation::Generate).await;
    started.unwrap().producer.await.unwrap().unwrap();
    let sent_tools = sent(&learn).0["tools"].clone();
    let alias = sent_tools[0]["name"].as_str().unwrap().to_owned();
    assert_ne!(alias, "lookup_file");
    // The server part of an alias the cloak minted: `mcp__<server>__query` matches both
    // declared `query` tools, which the old route refuses to restore.
    let server = sent_tools[1]["name"].as_str().unwrap()["mcp__".len()..]
        .split("__")
        .next()
        .unwrap()
        .to_owned();
    let ambiguous = json!({"id": "msg_x", "type": "message", "role": "assistant", "model": SONNET,
        "content": [{"type": "tool_use", "id": "toolu_2", "name": format!("mcp__{server}__query"), "input": {}}],
        "stop_reason": "tool_use", "stop_sequence": null, "usage": {"input_tokens": 3, "output_tokens": 2}});
    let reply = json!({"id": "msg_t", "type": "message", "role": "assistant", "model": SONNET,
        "content": [{"type": "tool_use", "id": "toolu_1", "name": alias, "input": {}}],
        "stop_reason": "tool_use", "stop_sequence": null, "usage": {"input_tokens": 3, "output_tokens": 2}});
    let replies = [
        Reply::json(reply.to_string()),
        Reply::sse(SSE),
        Reply::json(ambiguous.to_string()),
    ];
    let upstream = StreamUpstream::start(replies).await;
    let fixture = Fixture::with(oauth(), &upstream.url(), SONNET);
    let (started, id, _request) = fixture.anthropic(&create, Operation::Generate).await;
    let mut started = started.unwrap();
    started.producer.await.unwrap().unwrap();
    let native = fixture.egress(id, &mut started.stream, BodyMode::Buffered).await;
    let native: Value = serde_json::from_slice(&native).unwrap();
    assert_eq!(native["content"][0]["name"], "lookup_file", "{native}");
    let thread = json!({"type": "continue", "previous_message_id": "msg_t"});
    let after = json!({"model": SONNET, "max_tokens": 1024, "stream": true, "thread": thread, "messages": messages});
    let (started, _, _request) = fixture
        .anthropic(&serde_json::to_vec(&after).unwrap(), Operation::Generate)
        .await;
    started.unwrap().producer.await.unwrap().unwrap();
    let (started, _, _request) = fixture.anthropic(&create, Operation::Generate).await;
    let error = started.unwrap().producer.await.unwrap().unwrap_err();
    let message = String::from_utf8_lossy(&error.body);
    assert!(
        message.starts_with("restore Claude OAuth tool name from response: "),
        "{message}"
    );
    assert_eq!(upstream.requests(), 3);
}

/// The collected body charges the exchange's stream quota; past it the response stream ends
/// with `StorageExhausted` and the attempt fails, before any of the body is published.
#[tokio::test]
async fn a_buffered_cloaked_body_over_the_stream_quota_fails_visibly() {
    let text = "a".repeat(300_000);
    let reply = json!({"id": "msg_big", "type": "message", "role": "assistant", "model": SONNET,
        "content": [{"type": "text", "text": text}],
        "stop_reason": "end_turn", "stop_sequence": null, "usage": {"input_tokens": 3, "output_tokens": 2}});
    let upstream = StreamUpstream::start([Reply::json(reply.to_string())]).await;
    let mut fixture = Fixture::with(oauth(), &upstream.url(), SONNET);
    fixture.streams = Streams::with_metadata(256 * 1024);
    let body = json!({"model": SONNET, "max_tokens": 1024, "messages": [{"role": "user", "content": "hi"}]});
    let (started, id, _request) = fixture
        .anthropic(&serde_json::to_vec(&body).unwrap(), Operation::Generate)
        .await;
    let error = started.unwrap().producer.await.unwrap().unwrap_err();
    assert_eq!((error.status, &error.body[..]), (503, &b"stream.storage_exhausted"[..]));
    let published = fixture.streams.published(id);
    assert_eq!(published.terminal, Some(Terminal::StorageExhausted));
    assert!(published.records.is_empty(), "nothing of the body is published");
}

/// A compaction on an OAuth credential is prepared like any cloaked request and still asks
/// for native compaction; it, and a cloaked request replaying a compaction block where the
/// client placed it, carry the compaction beta (D2 plan §2.7). Anthropic refuses `compaction`
/// with `context_management`, so the cloak adds none to it, even with thinking on through the
/// model's thinking suffix. One a payload rule adds is refused in words that name neither the
/// upstream nor the operator's setting.
#[tokio::test]
async fn cloaked_compactions_carry_the_beta_without_context_management() {
    let compact =
        br#"{"model":"m","max_output_tokens":16000,"input":[{"role":"user","content":"Explain lifetimes."}]}"#;
    let carried = json!({"role": "assistant", "content": [
        {"type": "compaction", "content": "Café — done.", "signature": "EqQBCkgIBxABGAIq"}]});
    let replay = serde_json::to_vec(&json!({"model": SONNET, "max_tokens": 64, "messages": [
        {"role": "user", "content": "Explain lifetimes."},
        {"role": "assistant", "content": "They bound references."},
        carried,
        {"role": "user", "content": "And now?"},
    ]}))
    .unwrap();
    let thinking = format!("{SONNET}(8192)");
    let rule = format!(
        "requests:\n  payload:\n    default:\n      - models: [{{name: {SONNET}}}]\n        params: {{context_management: {{edits: []}}}}\n"
    );
    for (body, operation, model, rules, refused) in [
        (&compact[..], Operation::Compact, SONNET, None, false),
        (&compact[..], Operation::Compact, thinking.as_str(), None, false),
        (&compact[..], Operation::Compact, SONNET, Some(&rule), true),
        (&replay[..], Operation::Generate, SONNET, None, false),
    ] {
        let (mut fixture, recorder) = at(ORIGIN, &oauth(), model, &[]);
        if let Some(rules) = rules {
            fixture.exchange.cfg = ck_core::config::Config::parse(rules).unwrap();
        }
        let started = if operation == Operation::Compact {
            let mut request = fixture.streams.request();
            let (store, id) = (&fixture.streams.store, identity(Direction::Request));
            let mut ingress = responses::request::Ingress::new(&mut request, store, id, operation, LIMIT).unwrap();
            fixture.start(&mut ingress, &mut request, body).await.0
        } else {
            fixture.anthropic(body, operation).await.0
        };
        let Err(error) = started else {
            panic!("the proxy refuses every connection")
        };
        let wire = recorder.0.lock().unwrap().pop();
        if refused {
            assert!(wire.is_none(), "{model}");
            let text = String::from_utf8_lossy(&error.body);
            assert!(text.contains("context_management"), "{text}");
            assert!(!text.contains("Anthropic") && !text.contains("payload"), "{text}");
            continue;
        }
        let wire = wire.expect("the upstream request");
        let sent: Value = serde_json::from_str(&wire.body).unwrap();
        if operation == Operation::Compact {
            assert_eq!(sent["compaction"], json!({"type": "summarize"}));
            assert_eq!(sent["stream"], false);
        } else {
            let messages = sent["messages"].as_array().unwrap();
            assert!(messages.contains(&carried), "{sent}");
        }
        assert!(sent.get("context_management").is_none(), "{sent}");
        assert_eq!(sent.get("thinking").is_some(), model != SONNET, "{sent}");
        let betas = wire
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("anthropic-beta"))
            .unwrap();
        for beta in ["oauth-2025-04-20", "compact-2026-09-04"] {
            assert!(betas.1.split(',').any(|b| b == beta), "{beta} missing from {}", betas.1);
        }
    }
}
