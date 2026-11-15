//! The Meta executor under the execution contract, against the DT local upstream: the request
//! every client protocol puts on the wire, the canonical response every client egress accepts,
//! the local count, the requests that keep the old route, and failures before and after the
//! upstream answered.

mod support;

#[path = "exec_meta/common.rs"]
mod common;
#[path = "exec_meta/failures.rs"]
mod failures;
#[path = "exec_meta/policy.rs"]
mod policy;

use bytes::Bytes;
use ck_core::exec::{ExecRequest, Operation as ExecOperation};
use ck_core::format::Format;
use ck_exec::meta::MetaExecutor;
use ck_exec::meta_exec::{DESTINATION, MetaCapability};
use ck_stream::Terminal;
use ck_translate::codec::event::Direction;
use ck_translate::codec::{BodyMode, Operation, RequestIngress};
use ck_translate::{codec::chat, responses};
use common::*;
use futures_util::StreamExt;
use serde_json::Value;
use support::upstream::{Captured, Reply, StreamUpstream};
use support::{Exchange, Streams, fence, identity};

/// Every client protocol goes upstream as the Responses request Meta takes, with the old path's
/// headers; its egress accepts the canonical response, streamed and buffered.
#[tokio::test]
async fn every_client_protocol_goes_out_as_meta_and_comes_back() {
    for (protocol, body, streamed, buffered) in [
        ("responses", RESPONSES, "response.completed", r#""status":"completed""#),
        ("chat", CHAT, "[DONE]", r#""finish_reason":"stop""#),
        ("anthropic", ANTHROPIC, "message_stop", r#""stop_reason":"end_turn""#),
        ("gemini", GEMINI, "STOP", "STOP"),
    ] {
        let upstream = StreamUpstream::start([Reply::sse(sse())]).await;
        let (streams, exchange) = (Streams::new(), exchange(&upstream));
        let (started, id, _request) = run(&streams, &exchange, protocol, Operation::Generate, body, 7).await;
        let (outcome, published, mut stream) = finish(&streams, started.unwrap(), id).await;
        outcome.unwrap();
        assert_eq!(published.terminal, Some(Terminal::Done), "{protocol}");
        let captured = &upstream.captured()[0];
        assert_eq!(
            (captured.method.as_str(), captured.path.as_str()),
            ("POST", "/responses")
        );
        for (name, value) in [
            ("authorization", "Bearer sk-up"),
            ("user-agent", ck_exec::meta::USER_AGENT),
            ("x-client-id", "tbh:tui"),
            ("accept", "text/event-stream"),
            ("cache-control", "no-cache"),
            ("content-type", "application/json"),
        ] {
            assert_eq!(captured.header(name), Some(value), "{protocol} {name}");
        }
        let sent = json_body(captured);
        assert_eq!(sent["model"], MODEL, "{protocol}: {sent}");
        assert_eq!(sent["stream"], true, "{protocol}: {sent}");
        assert_eq!(sent["store"], false, "{protocol}: {sent}");
        assert!(sent["instructions"].is_string(), "{protocol}: {sent}");
        for absent in ["stream_options", "client_metadata"] {
            assert!(sent.get(absent).is_none(), "{protocol} {absent}: {sent}");
        }
        for (mode, done) in [(BodyMode::Streaming, streamed), (BodyMode::Buffered, buffered)] {
            let out = egress(&streams, protocol, &mut stream, id, mode).await;
            assert!(
                out.contains("hel") && out.contains("lo") && out.contains(done),
                "{protocol}: {out}"
            );
        }
    }
}

/// The old path's upload for a Responses client: request, then drain its response.
async fn old_path(exchange: &Exchange, body: &[u8], stream: bool) {
    old_path_from(exchange, body, stream, Format::OpenAIResponse).await;
}

/// [`old_path`] for a client of `source`.
async fn old_path_from(exchange: &Exchange, body: &[u8], stream: bool, source: Format) {
    let request = ExecRequest {
        operation: ExecOperation::Generate,
        source_format: source,
        response_format: source,
        requested_model: "client-model".into(),
        model: exchange.model.clone(),
        original_body: Bytes::copy_from_slice(body),
        body: Bytes::copy_from_slice(body),
        stream,
        alt: None,
        session: None,
        execution_session: None,
        derived_session: None,
        request_path: exchange.route.clone(),
        headers: exchange.headers.clone(),
        caller: exchange.caller.clone(),
        resolved_model: None,
        usage: Default::default(),
    };
    let executor = MetaExecutor::with_client(wreq::Client::new());
    let response = executor
        .execute(&exchange.credential, request, &exchange.cfg)
        .await
        .unwrap();
    if let ck_core::exec::ResponseBody::Stream(mut events) = response.body {
        while events.next().await.is_some() {}
    }
}

fn wire_headers(captured: &Captured) -> Vec<(String, String)> {
    let connection = |n: &str| n.eq_ignore_ascii_case("host") || n.eq_ignore_ascii_case("content-length");
    let mut headers: Vec<_> = (captured.headers.iter())
        .filter(|(n, _)| !connection(n))
        .map(|(n, v)| (n.to_ascii_lowercase(), v.clone()))
        .collect();
    headers.sort();
    headers
}

const RICH: &str = r#"{"model":"client-model","stream":STREAM,"instructions":"be brief","input":[{"type":"message","role":"system","content":[{"type":"input_text","text":"rules"}]},{"type":"message","role":"user","content":[{"type":"input_text","text":"hi","prompt_cache_breakpoint":{"ttl":"5m"}}]},{"type":"function_call","call_id":"call_1","name":"lookup","arguments":"{}"},{"type":"function_call_output","call_id":"call_1","output":"found"},{"type":"reasoning","id":"rs_1","summary":[],"content":[{"type":"reasoning_text","text":"thought"}]}],"tools":[{"type":"function","name":"lookup","description":"Look up","parameters":{"type":"object","properties":{"q":{"type":"string"},"n":{"type":"integer"}}}},{"type":"web_search","search_content_types":["text"]},{"type":"custom","name":"grep","description":"Search"},{"type":"function","name":"exec_command","description":"Run","parameters":{"type":"object","properties":{"timeout_ms":{"type":"number"}}}}],"tool_choice":"auto","max_output_tokens":100,"temperature":0.5,"reasoning":{"effort":"high","summary":"auto"},"store":true,"service_tier":"fast","prompt_cache_key":"pk-1","text":{"verbosity":"low"},"user":"u1","safety_identifier":"s1","client_metadata":{"x-codex-window-id":"w1"}}"#;

/// A Responses client's request reaches Meta as the old path sent it: a JSON-equal body and the
/// same headers, streamed and buffered, with Meta's own edits applied.
#[tokio::test]
async fn a_responses_client_matches_the_old_path() {
    for stream in [true, false] {
        let body = RICH.replace("STREAM", if stream { "true" } else { "false" });
        let upstream = StreamUpstream::start([Reply::sse(sse()), Reply::sse(sse())]).await;
        let mut exchange = exchange(&upstream);
        exchange.cfg = ck_core::config::Config::parse(
            "requests:\n  payload:\n    override:\n      - models: [{name: \"muse-*\", protocol: meta}]\n        params: {text.verbosity: high}\n",
        )
        .unwrap();
        exchange
            .credential
            .attributes
            .insert("header:X-Extra".into(), "1".into());
        exchange.headers.insert("x-client", http::HeaderValue::from_static("c"));
        exchange
            .headers
            .insert("user-agent", http::HeaderValue::from_static("codex_cli_rs/0.50.0"));
        let streams = Streams::new();
        let (started, id, _request) = run(
            &streams,
            &exchange,
            "responses",
            Operation::Generate,
            body.as_bytes(),
            9,
        )
        .await;
        finish(&streams, started.unwrap(), id).await.0.unwrap();
        old_path(&exchange, body.as_bytes(), stream).await;
        let captured = upstream.captured();
        let (new, old) = (&captured[0], &captured[1]);
        let sent = json_body(new);
        assert_eq!(sent, json_body(old), "stream {stream}");
        assert!(sent["tools"][1].get("search_content_types").is_none(), "{sent}");
        assert_eq!(sent["tools"][2]["type"], "custom", "{sent}");
        assert_eq!(sent["text"]["verbosity"], "high", "payload rules apply: {sent}");
        assert_eq!(sent["safety_identifier"], "s1", "{sent}");
        let timeout = &sent["tools"][3]["parameters"]["properties"]["timeout_ms"]["type"];
        assert_eq!(timeout, "integer", "Codex clients' integer parameters: {sent}");
        let reasoning = &sent["input"][4];
        assert_eq!(reasoning["content"], serde_json::json!([]), "{sent}");
        assert_eq!(reasoning["summary"][0]["text"], "thought", "{sent}");
        assert_eq!(wire_headers(new), wire_headers(old), "stream {stream}");
        assert_eq!(new.header("x-extra"), Some("1"));
    }
}

/// String input and string message content reach Meta as the old path sent them: a Responses
/// client's string `input`, however long, as one `input_text` part and its own messages as
/// sent, a one-message untyped array included; every other client's string content as one
/// text part (`output_text` for the assistant).
#[tokio::test]
async fn string_input_and_content_match_the_old_path() {
    let long = format!(r#"{{"model":"client-model","input":"{}"}}"#, "a".repeat(30_000));
    let bodies = [
        ("responses", Format::OpenAIResponse, long.as_str()),
        (
            "responses",
            Format::OpenAIResponse,
            r#"{"model":"client-model","input":[{"role":"user","content":"hi"}]}"#,
        ),
        (
            "responses",
            Format::OpenAIResponse,
            r#"{"model":"client-model","input":[{"role":"user","content":"hi","input":"x"}]}"#,
        ),
        (
            "responses",
            Format::OpenAIResponse,
            r#"{"model":"client-model","input":"hi"}"#,
        ),
        (
            "responses",
            Format::OpenAIResponse,
            r#"{"model":"client-model","instructions":"be brief","input":"hi"}"#,
        ),
        (
            "responses",
            Format::OpenAIResponse,
            r#"{"model":"client-model","input":[{"type":"message","role":"user","content":"hi"},{"type":"message","role":"assistant","content":"hello"},{"type":"message","role":"user","content":"more"}]}"#,
        ),
        (
            "responses",
            Format::OpenAIResponse,
            r#"{"model":"client-model","input":[{"type":"message","role":"user","content":"hi"}]}"#,
        ),
        (
            "chat",
            Format::OpenAI,
            r#"{"model":"client-model","messages":[{"role":"system","content":"be brief"},{"role":"user","content":"hi"},{"role":"assistant","content":"hello"},{"role":"user","content":"more"}]}"#,
        ),
        (
            "anthropic",
            Format::Claude,
            r#"{"model":"client-model","max_tokens":64,"messages":[{"role":"user","content":"hi"},{"role":"assistant","content":"hello"},{"role":"user","content":"more"}]}"#,
        ),
    ];
    for (protocol, source, body) in bodies {
        let upstream = StreamUpstream::start([Reply::sse(sse()), Reply::sse(sse())]).await;
        let exchange = exchange(&upstream);
        let streams = Streams::new();
        let (started, id, _request) =
            run(&streams, &exchange, protocol, Operation::Generate, body.as_bytes(), 64).await;
        finish(&streams, started.unwrap(), id).await.0.unwrap();
        old_path_from(&exchange, body.as_bytes(), false, source).await;
        let captured = upstream.captured();
        let (new, old) = (json_body(&captured[0]), json_body(&captured[1]));
        if source == Format::OpenAIResponse {
            assert_eq!(new, old, "{body}");
        } else {
            assert_eq!(new["input"], old["input"], "{body}");
        }
    }
}

/// The upstream model's thinking suffix becomes the reasoning effort; the suffix never reaches
/// the wire.
#[tokio::test]
async fn suffix_thinking_reaches_the_upload() {
    let upstream = StreamUpstream::start([Reply::sse(sse())]).await;
    let mut exchange = exchange(&upstream);
    exchange.model = format!("{MODEL}(high)");
    let streams = Streams::new();
    let (started, id, _request) = run(&streams, &exchange, "responses", Operation::Generate, RESPONSES, 64).await;
    finish(&streams, started.unwrap(), id).await.0.unwrap();
    let sent = json_body(&upstream.captured()[0]);
    assert_eq!(sent["model"], MODEL, "{sent}");
    assert_eq!(sent["reasoning"]["effort"], "high", "{sent}");
}

/// A count is local over the Meta upload: nothing is sent, the stream holds one canonical
/// `count_tokens`, and it equals the old path's count for the same Anthropic request.
#[tokio::test]
async fn a_count_is_local_and_matches_the_old_path() {
    let body = br#"{"model":"client-model","system":"be brief","messages":[{"role":"user","content":"hi"}]}"#;
    let upstream = StreamUpstream::start([]).await;
    let (streams, exchange) = (Streams::new(), exchange(&upstream));
    let (started, id, _request) = run(&streams, &exchange, "anthropic", Operation::CountTokens, body, 5).await;
    let (outcome, published, _stream) = finish(&streams, started.unwrap(), id).await;
    outcome.unwrap();
    assert_eq!(upstream.requests(), 0);
    let records = records(&published);
    let count = records
        .iter()
        .find(|r| r.contains("event: x_ck.count_tokens"))
        .expect("a count");
    let at = count.find(r#""input_tokens":"#).unwrap() + 15;
    let tokens: String = count[at..].chars().take_while(char::is_ascii_digit).collect();
    let request = ExecRequest {
        operation: ExecOperation::CountTokens,
        source_format: Format::Claude,
        response_format: Format::Claude,
        requested_model: "client-model".into(),
        model: MODEL.into(),
        original_body: Bytes::from_static(body),
        body: Bytes::from_static(body),
        stream: false,
        alt: None,
        session: None,
        execution_session: None,
        derived_session: None,
        request_path: "/v1/messages/count_tokens".into(),
        headers: http::HeaderMap::new(),
        caller: exchange.caller.clone(),
        resolved_model: None,
        usage: Default::default(),
    };
    let executor = MetaExecutor::with_client(wreq::Client::new());
    let old = executor
        .execute(&exchange.credential, request, &exchange.cfg)
        .await
        .unwrap();
    let ck_core::exec::ResponseBody::Buffered(old) = old.body else {
        panic!("a buffered count")
    };
    let old: Value = serde_json::from_slice(&old).unwrap();
    assert_eq!(old["input_tokens"].to_string(), tokens, "{records:?}");
}

/// The outcome of fencing a `protocol` request for Meta, the error by name.
fn fenced(protocol: &str, operation: Operation, body: &[u8]) -> Result<(), String> {
    let streams = Streams::new();
    let mut request = streams.request();
    let (store, id) = (&streams.store, identity(Direction::Request));
    match protocol {
        "chat" => {
            let mut ingress = chat::request::Ingress::new(&mut request, store, id, operation, LIMIT).unwrap();
            fence(&mut ingress, &mut request, body, 64, &MetaCapability).map(drop)
        }
        _ => {
            let mut ingress = responses::request::Ingress::new(&mut request, store, id, operation, LIMIT).unwrap();
            fence(&mut ingress, &mut request, body, 64, &MetaCapability).map(drop)
        }
    }
    .map_err(|e| format!("{e:?}"))
}

/// Codex's freeform `apply_patch` tool, declared, nested in a namespace, or replayed, keeps the
/// old route, whose bridge alone sends it as the JSON function Meta takes. Another custom tool,
/// and Meta's lack of a native compact, are answered at the fence.
#[test]
fn apply_patch_keeps_the_old_route() {
    let custom = r#"{"type":"custom","name":"NAME","description":"Patch","format":{"type":"grammar","syntax":"lark","definition":"start: x"}}"#;
    let declared = |name: &str| {
        format!(
            r#"{{"model":"m","input":"hi","tools":[{}]}}"#,
            custom.replace("NAME", name)
        )
    };
    let nested = format!(
        r#"{{"model":"m","input":"hi","tools":[{{"type":"namespace","name":"fs","description":"files","tools":[{}]}}]}}"#,
        custom.replace("NAME", "apply_patch")
    );
    let replayed = r#"{"model":"m","input":[{"type":"custom_tool_call","call_id":"c1","name":"apply_patch","input":"*** Begin Patch"},{"type":"custom_tool_call_output","call_id":"c1","output":"ok"},{"role":"user","content":"next"}]}"#;
    let chat = r#"{"model":"m","messages":[{"role":"user","content":"hi"}],"tools":[{"type":"custom","custom":{"name":"apply_patch"}}]}"#;
    let destination = Err("Destination".to_owned());
    assert_eq!(
        fenced("responses", Operation::Generate, declared("apply_patch").as_bytes()),
        destination
    );
    assert_eq!(
        fenced("responses", Operation::Generate, declared(" apply_patch\\n").as_bytes()),
        destination
    );
    assert_eq!(fenced("responses", Operation::Generate, nested.as_bytes()), destination);
    assert_eq!(
        fenced("responses", Operation::Generate, replayed.as_bytes()),
        destination
    );
    assert_eq!(fenced("chat", Operation::Generate, chat.as_bytes()), destination);
    assert_eq!(
        fenced("responses", Operation::Generate, declared("apply_patches").as_bytes()),
        Ok(())
    );
    let compact = br#"{"model":"m","input":"hi"}"#;
    assert_eq!(
        fenced("responses", Operation::Compact, compact),
        Err("Unsupported".to_owned())
    );
}

/// The `apply_patch` fence reads only the carriers the old bridge rewrites: the object a
/// custom patch tool is made of, held as data anywhere else, routes nowhere.
#[test]
fn apply_patch_is_found_only_where_it_is_declared_chosen_or_called() {
    let patch = r#"{"type":"custom","name":"apply_patch"}"#;
    let lookup = r#"{"type":"function","name":"lookup","parameters":{"type":"object","properties":{"q":{"type":"object","default":PATCH}}}}"#
        .replace("PATCH", patch);
    let not_patch = [
        // A function call whose arguments hold the object.
        format!(
            r#"{{"model":"m","input":[{{"type":"function_call","call_id":"c1","name":"lookup","arguments":{}}},{{"type":"function_call_output","call_id":"c1","output":"ok"}}],"tools":[{lookup}]}}"#,
            serde_json::to_string(patch).unwrap()
        ),
        // A schema default.
        format!(r#"{{"model":"m","input":"hi","tools":[{lookup}]}}"#),
        // An unknown request field.
        format!(r#"{{"model":"m","input":"hi","x_extra":{patch}}}"#),
        // A custom tool's own extension member.
        format!(
            r#"{{"model":"m","input":"hi","tools":[{{"type":"custom","name":"lookup","format":{{"type":"text"}},"tools":[{patch}]}}]}}"#
        ),
        // A function tool's, and a named choice's.
        format!(
            r#"{{"model":"m","input":"hi","tools":[{{"type":"function","name":"lookup","parameters":{{}},"children":[{patch}]}}]}}"#
        ),
        format!(
            r#"{{"model":"m","input":"hi","tools":[{{"type":"custom","name":"lookup"}}],"tool_choice":{{"type":"custom","name":"lookup","tools":[{patch}]}}}}"#
        ),
    ];
    for body in &not_patch {
        assert_eq!(
            fenced("responses", Operation::Generate, body.as_bytes()),
            Ok(()),
            "{body}"
        );
    }
}

#[test]
fn apply_patch_carriers_require_the_bridge() {
    let patch = r#"{"type":"custom","name":"apply_patch"}"#;
    let custom = r#"{"type":"custom","name":"apply_patch","description":"Patch","format":{"type":"grammar","syntax":"lark","definition":"start: x"}}"#;
    let real = [
        ("responses", format!(r#"{{"model":"m","input":"hi","tools":[{custom}]}}"#)),
        (
            "responses",
            format!(r#"{{"model":"m","input":"hi","tools":[{{"type":"namespace","name":"fs","description":"files","tools":[{custom}]}}]}}"#),
        ),
        (
            "responses",
            format!(r#"{{"model":"m","input":"hi","tools":[{custom}],"tool_choice":{patch}}}"#),
        ),
        (
            "responses",
            r#"{"model":"m","input":[{"type":"custom_tool_call","call_id":"c1","name":"apply_patch","input":"*** Begin Patch"},{"type":"custom_tool_call_output","call_id":"c1","output":"ok"},{"role":"user","content":"next"}]}"#.into(),
        ),
        (
            "responses",
            r#"{"model":"m","input":[{"type":"custom_tool_call","call_id":"c1","namespace":"fs","name":"apply_patch","input":"*** Begin Patch"},{"type":"custom_tool_call_output","call_id":"c1","output":"ok"},{"role":"user","content":"next"}]}"#.into(),
        ),
        (
            "chat",
            r#"{"model":"m","messages":[{"role":"user","content":"hi"}],"tools":[{"type":"custom","custom":{"name":"apply_patch"}}]}"#.into(),
        ),
        (
            "responses",
            format!(r#"{{"model":"m","input":[{{"type":"additional_tools","tools":[{custom}]}},{{"role":"user","content":"hi"}}]}}"#),
        ),
        ("responses", format!(r#"{{"model":"m","input":"hi","tool_choice":{patch}}}"#)),
        (
            "responses",
            format!(r#"{{"model":"m","input":"hi","tools":[{{"type":"namespace","name":"fs","children":[{custom}]}}]}}"#),
        ),
        (
            "responses",
            format!(
                r#"{{"model":"m","input":"hi","tools":[{{"type":"function","name":"lookup","parameters":{{}}}}],"tool_choice":{{"type":"allowed_tools","mode":"auto","tools":[{patch}]}}}}"#
            ),
        ),
        (
            "chat",
            r#"{"model":"m","messages":[{"role":"user","content":"hi"},{"role":"assistant","tool_calls":[{"id":"c1","type":"custom","custom":{"name":"apply_patch","input":"*** Begin Patch"}}]},{"role":"tool","tool_call_id":"c1","content":"ok"}]}"#.into(),
        ),
    ];
    for (protocol, body) in &real {
        assert_eq!(
            fenced(protocol, Operation::Generate, body.as_bytes()),
            Err("Destination".to_owned()),
            "{protocol}: {body}"
        );
    }
}

/// The selector runs Meta credentials through this executor.
#[tokio::test]
async fn the_selector_runs_meta_through_the_executor() {
    let executors = ck_exec::Executors {
        claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
        codex: Default::default(),
        devices: Default::default(),
        openai: Default::default(),
        google: Default::default(),
    };
    let upstream = StreamUpstream::start([Reply::sse(sse())]).await;
    let (streams, exchange) = (Streams::new(), exchange(&upstream));
    let attempt = exchange.attempt(&streams.store);
    assert_eq!(executors.destination(&attempt), Some(DESTINATION));
    let mut request = streams.request();
    let id = identity(Direction::Request);
    let mut ingress =
        chat::request::Ingress::new(&mut request, &streams.store, id, Operation::Generate, LIMIT).unwrap();
    ingress.advance(&mut request, CHAT).unwrap();
    let validated = executors.fence(&attempt, &mut ingress, &mut request).unwrap();
    assert_eq!(
        executors.refence(&attempt, validated.view()).unwrap().destination(),
        DESTINATION
    );
    let response = streams.response(&request);
    let id = response.id();
    let handle = exchange.request(validated);
    let started = executors
        .start(&attempt, &handle, &mut request, response)
        .await
        .unwrap();
    let (outcome, published, _stream) = finish(&streams, started, id).await;
    outcome.unwrap();
    assert_eq!(published.terminal, Some(Terminal::Done));
}
