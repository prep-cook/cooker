//! The Kimi executor under the execution contract, against the DT local upstream: the upload
//! each client protocol puts on the wire next to the old path's Go fixtures, the canonical
//! response every client egress accepts, the requests that keep the old route, and failures
//! before and after the upstream answered.

mod support;

#[path = "exec_kimi/common.rs"]
mod common;

use std::sync::{Arc, Mutex};

use ck_core::exec::{CaptureEvent, CaptureObserver, CaptureSink, FailureScope, UsageSink};
use ck_exec::Executors;
use ck_stream::Terminal;
use ck_translate::codec::{self, BodyMode, Operation};
use common::{TOKEN, assert_same_request, egress, errors, exchange, finish, fixture, fixture_exchange, records};
use common::{old_upload, protocol, replies, run, try_run};
use support::Streams;
use support::upstream::{Reply, StreamUpstream};

const CHAT: &[u8] = br#"{"model":"kimi-k2","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
const RESPONSES: &[u8] = br#"{"model":"kimi-k3","stream":true,"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}"#;

fn chunk(delta: &str, finish: &str) -> String {
    format!(
        "data: {{\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"k2\",\"choices\":[{{\"index\":0,\"delta\":{delta},\"finish_reason\":{finish}}}]}}\n\n"
    )
}

/// "hel", "lo", a stop with usage; `[DONE]` when `done`.
fn chat_sse(done: bool) -> String {
    let mut sse = [
        chunk(r#"{"role":"assistant","content":"hel"}"#, "null"),
        chunk(r#"{"content":"lo"}"#, "null"),
        chunk("{}", r#""stop""#),
        "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"k2\",\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2,\"total_tokens\":5}}\n\n".to_owned(),
    ]
    .concat();
    if done {
        sse.push_str("data: [DONE]\n\n");
    }
    sse
}

const RESPONSES_SSE: &str = concat!(
    "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"object\":\"response\",\"created_at\":1,\"status\":\"in_progress\",\"model\":\"k3\",\"output\":[]}}\n\n",
    "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"m1\",\"type\":\"message\",\"status\":\"in_progress\",\"role\":\"assistant\",\"content\":[]}}\n\n",
    "event: response.content_part.added\ndata: {\"type\":\"response.content_part.added\",\"item_id\":\"m1\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"\",\"annotations\":[]}}\n\n",
    "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"m1\",\"output_index\":0,\"content_index\":0,\"delta\":\"hello\"}\n\n",
    "event: response.output_text.done\ndata: {\"type\":\"response.output_text.done\",\"item_id\":\"m1\",\"output_index\":0,\"content_index\":0,\"text\":\"hello\"}\n\n",
    "event: response.content_part.done\ndata: {\"type\":\"response.content_part.done\",\"item_id\":\"m1\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"hello\",\"annotations\":[]}}\n\n",
    "event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"m1\",\"type\":\"message\",\"status\":\"completed\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"hello\",\"annotations\":[]}]}}\n\n",
    "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"object\":\"response\",\"created_at\":1,\"status\":\"completed\",\"model\":\"k3\",\"output\":[{\"id\":\"m1\",\"type\":\"message\",\"status\":\"completed\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"hello\",\"annotations\":[]}]}],\"usage\":{\"input_tokens\":3,\"output_tokens\":1,\"total_tokens\":4}}}\n\n",
);

/// Runs a Go fixture's request through the contract path, `step` bytes at a time, and
/// asserts the upload matches the old path's. The fixture's answer only completes the exchange.
async fn parity(name: &str, step: usize) {
    let fx = fixture(name);
    let upstream = StreamUpstream::start(replies(&fx)).await;
    let exchange = fixture_exchange(&upstream, &fx);
    let streams = Streams::new();
    let source = protocol(fx["request"]["source"].as_str().unwrap());
    let body = fx["request"]["body"].as_str().unwrap().as_bytes();
    let (outcome, _request) = try_run(&streams, &exchange, source, Operation::Generate, body, step).await;
    let (started, id) = outcome.unwrap_or_else(|e| panic!("{name}: fence {e:?}"));
    let (outcome, _, _) = finish(&streams, started.expect("the upstream answered"), id).await;
    // Some scripted answers are not valid Chat streams (no `role`); only the upload is compared.
    drop(outcome);
    let captured = upstream.captured();
    assert_eq!(captured.len(), 1, "{name}: one upstream request");
    assert_same_request(name, &fx["upstream"][0], &captured[0]);
}

#[tokio::test]
async fn uploads_are_json_equal_to_the_old_path() {
    for name in [
        "chat-stream-suffix",
        "chat-kimi-ai-metadata-base",
        "chat-payload-override-after-normalize",
        "chat-stream-usage-then-model",
        "transport-custom-headers",
        "responses-nonstream-reorder-suffix",
        "responses-payload-rules",
        "responses-payload-override-after-normalize",
        "responses-stream-clamp",
    ] {
        parity(name, usize::MAX).await;
    }
}

const CHAT_JSON: &str = r#"{"id":"c1","object":"chat.completion","created":1,"model":"k2.5","choices":[{"index":0,"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7}}"#;

/// Sends `body` from a `client` (old path `source`) through the old path and the contract
/// path, and asserts both uploads are JSON-equal with the same headers.
async fn old_and_new(client: &str, source: &str, model: &str, body: &str, reply: Reply) {
    old_and_new_with(client, source, model, body, reply, "").await;
}

/// [`old_and_new`] under config `yaml`.
async fn old_and_new_with(client: &str, source: &str, model: &str, body: &str, reply: Reply, yaml: &str) {
    let upstream = StreamUpstream::start([reply.clone(), reply]).await;
    let mut exchange = exchange(&upstream, model);
    exchange.cfg = ck_core::config::Config::parse(yaml).unwrap();
    let old = old_upload(&upstream, &exchange, source, body.as_bytes()).await;
    let streams = Streams::new();
    let (started, id, _request) = run(&streams, &exchange, client, body.as_bytes(), usize::MAX).await;
    finish(&streams, started.unwrap(), id).await.0.unwrap();
    let new = upstream.captured().pop().unwrap();
    assert_eq!(new.path, old.path, "{body}");
    let skip = |(n, _): &&(String, String)| !n.eq_ignore_ascii_case("content-length");
    let headers = |c: &support::upstream::Captured| c.headers.iter().filter(skip).cloned().collect::<Vec<_>>();
    assert_eq!(headers(&new), headers(&old), "{body}");
    let json = |b: &[u8]| serde_json::from_slice::<serde_json::Value>(b).unwrap();
    assert_eq!(json(&new.body), json(&old.body), "{body}");
}

#[tokio::test]
async fn chat_normalizations_match_the_old_path() {
    let turns = r#"[{"role":"system","content":"sys"},{"role":"user","content":"hi"},{"role":"assistant","content":"","tool_calls":[{"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{\"q\":\"x\"}"}}]},{"role":"tool","tool_call_id":"call_1","content":"result"},{"role":"assistant","content":""},{"role":"user","content":"next"}]"#;
    let tools = r##"[{"type":"function","function":{"name":"lookup","parameters":{"properties":{"q":{"$ref":"#/$defs/Q"}},"$defs":{"Q":{"type":"string","description":"query"}}}}}]"##;
    for (stream, reply) in [(false, Reply::json(CHAT_JSON)), (true, Reply::sse(chat_sse(true)))] {
        let body = format!(
            r#"{{"model":"kimi-k2.5","stream":{stream},"temperature":0.7,"reasoning_effort":"high","messages":{turns},"tools":{tools}}}"#
        );
        old_and_new("chat", "openai", "kimi-k2.5", &body, reply).await;
    }
    let body = r#"{"model":"kimi-k2.8(high)","messages":[{"role":"user","content":"hi"}],"temperature":1}"#;
    old_and_new("chat", "openai", "kimi-k2.8(high)", body, Reply::json(CHAT_JSON)).await;
}

#[tokio::test]
async fn responses_normalizations_match_the_old_path() {
    let reply = Reply::json(
        r#"{"id":"r1","object":"response","created_at":1,"status":"completed","model":"k3","output":[],"usage":{"input_tokens":1,"output_tokens":0,"total_tokens":1}}"#,
    );
    let body = r##"{"model":"kimi-k3(high)","temperature":0.5,"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]},{"type":"function_call","call_id":"a","name":"f","arguments":"{}"},{"type":"function_call","call_id":"b","name":"f","arguments":"{}"},{"type":"message","role":"developer","content":[{"type":"input_text","text":"between"}]},{"type":"function_call_output","call_id":"a","output":"A"},{"type":"function_call_output","call_id":"b","output":"B"}],"tools":[{"type":"function","name":"f","parameters":{"properties":{"x":{"$ref":"#/definitions/X"}},"definitions":{"X":{"type":"integer"}}}}]}"##;
    old_and_new("responses", "openai-response", "kimi-k3(high)", body, reply).await;
    old_and_new(
        "responses",
        "openai-response",
        "kimi-k3",
        r#"{"model":"kimi-k3","stream":true,"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}"#,
        Reply::sse(RESPONSES_SSE),
    )
    .await;
}

/// The payload rules of the `chat-payload-rules` fixture, whose recorded request streamed by
/// its route while its body had no `stream`.
/// A Responses client's `input` goes as it sent it: a string stays a string, and message items
/// without `type` stay without one, also around typed items and the `instructions`.
#[tokio::test]
async fn responses_input_forms_match_the_old_path() {
    let reply = Reply::json(
        r#"{"id":"r1","object":"response","created_at":1,"status":"completed","model":"k3","output":[],"usage":{"input_tokens":1,"output_tokens":0,"total_tokens":1}}"#,
    );
    for body in [
        r#"{"model":"kimi-k3","input":"hi \"there\"\n"}"#,
        r#"{"model":"kimi-k3","instructions":"be brief","input":"hi"}"#,
        r#"{"model":"kimi-k3","input":[{"role":"user","content":[{"type":"input_text","text":"hi"}]}]}"#,
        r#"{"model":"kimi-k3","instructions":"i","input":[{"role":"user","content":"hi"},{"type":"message","role":"assistant","content":"yo"},{"role":"user","content":"more"}]}"#,
        r#"{"model":"kimi-k3","input":[{"role":"user","content":"hi"},{"type":"function_call","call_id":"a","name":"f","arguments":"{}"},{"type":"function_call_output","call_id":"a","output":"A"},{"role":"developer","content":"between"}]}"#,
        r#"{"model":"kimi-k3","input":[{"type":"message","role":"user","content":"hi"}]}"#,
    ] {
        old_and_new("responses", "openai-response", "kimi-k3", body, reply.clone()).await;
    }
}

#[tokio::test]
async fn payload_rules_come_last_like_the_old_path() {
    let yaml = fixture("chat-payload-rules")["request"]["config"]
        .as_str()
        .unwrap()
        .to_owned();
    for stream in [false, true] {
        let body = format!(
            r#"{{"model":"kimi-k3(high)","stream":{stream},"messages":[{{"role":"user","content":"hi"}}],"temperature":0.3,"max_tokens":50}}"#
        );
        let reply = if stream {
            Reply::sse(chat_sse(true))
        } else {
            Reply::json(CHAT_JSON)
        };
        old_and_new_with("chat", "openai", "kimi-k3(high)", &body, reply, &yaml).await;
    }
    let body = r#"{"model":"kimi-k3","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}],"temperature":0.3}"#;
    let reply = Reply::json(
        r#"{"id":"r1","object":"response","created_at":1,"status":"completed","model":"k3","output":[],"usage":{"input_tokens":1,"output_tokens":0,"total_tokens":1}}"#,
    );
    old_and_new_with("responses", "openai-response", "kimi-k3", body, reply, &yaml).await;
}

/// A default rule reads the client's request as encoded, before the suffix's thinking replaced
/// its `reasoning_effort`: the field the client set is not defaulted.
#[tokio::test]
async fn payload_defaults_read_the_request_before_shaping() {
    let yaml = "requests:\n  payload:\n    default:\n      - models: [{name: \"kimi-*\", protocol: openai}]\n        params: {reasoning_effort: medium}\n";
    let body = r#"{"model":"kimi-k3(high)","reasoning_effort":"low","messages":[{"role":"user","content":"hi"}]}"#;
    old_and_new_with("chat", "openai", "kimi-k3(high)", body, Reply::json(CHAT_JSON), yaml).await;
}

#[tokio::test]
async fn a_one_byte_fence_sends_the_same_upload() {
    parity("chat-stream-suffix", 1).await;
    parity("responses-nonstream-reorder-suffix", 1).await;
}

/// A Gemini client crosses protocols, so its upload is the shared Chat egress's: the same
/// request as the old path's except the text arrives as a content part.
#[tokio::test]
async fn a_gemini_client_streams_through_chat_completions() {
    let fx = fixture("chat-stream-gemini-client");
    let upstream = StreamUpstream::start([Reply::sse(chat_sse(true))]).await;
    let exchange = fixture_exchange(&upstream, &fx);
    let streams = Streams::new();
    let body = fx["request"]["body"].as_str().unwrap().as_bytes();
    let (started, id, _request) = run(&streams, &exchange, "gemini", body, 1).await;
    let (outcome, _, mut stream) = finish(&streams, started.unwrap(), id).await;
    outcome.unwrap();
    assert!(
        egress(&streams, "gemini", &mut stream, id, BodyMode::Streaming)
            .await
            .contains("hel")
    );
    let sent = &upstream.captured()[0];
    let mut go: serde_json::Value = serde_json::from_str(fx["upstream"][0]["body"].as_str().unwrap()).unwrap();
    go["messages"][0]["content"] = serde_json::json!([{"type": "text", "text": "hi"}]);
    assert_eq!(serde_json::from_slice::<serde_json::Value>(&sent.body).unwrap(), go);
    assert_eq!(sent.path, fx["upstream"][0]["target"].as_str().unwrap());
}

#[tokio::test]
async fn the_selector_routes_kimi_providers_here() {
    let upstream = StreamUpstream::start([]).await;
    let streams = Streams::new();
    let executors = Executors {
        claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
        codex: Default::default(),
        devices: Default::default(),
        openai: Default::default(),
        google: Default::default(),
    };
    for provider in ck_exec::kimi::PROVIDERS {
        let mut exchange = exchange(&upstream, "kimi-k2");
        exchange.credential.provider = provider.into();
        assert_eq!(
            executors.destination(&exchange.attempt(&streams.store)),
            Some("kimi"),
            "{provider}"
        );
    }
}

#[tokio::test]
async fn chat_stream_reaches_every_client_egress() {
    for client in ["chat", "responses", "anthropic", "gemini"] {
        let upstream = StreamUpstream::start([Reply::sse(chat_sse(true)).byte_at_a_time()]).await;
        let exchange = exchange(&upstream, "kimi-k2");
        let streams = Streams::new();
        let (started, id, _request) = run(&streams, &exchange, "chat", CHAT, usize::MAX).await;
        let (outcome, published, mut stream) = finish(&streams, started.unwrap(), id).await;
        outcome.unwrap();
        assert_eq!(published.terminal, Some(Terminal::Done));
        let out = egress(&streams, client, &mut stream, id, BodyMode::Streaming).await;
        assert!(out.contains("hel") && out.contains("lo"), "{client}: {out}");
    }
}

#[tokio::test]
async fn responses_stream_reaches_every_client_egress() {
    for client in ["chat", "responses", "anthropic", "gemini"] {
        let upstream = StreamUpstream::start([Reply::sse(RESPONSES_SSE).byte_at_a_time()]).await;
        let exchange = exchange(&upstream, "kimi-k3");
        let streams = Streams::new();
        let (started, id, _request) = run(&streams, &exchange, "responses", RESPONSES, 1).await;
        let (outcome, published, mut stream) = finish(&streams, started.unwrap(), id).await;
        outcome.unwrap();
        assert_eq!(published.terminal, Some(Terminal::Done));
        let out = egress(&streams, client, &mut stream, id, BodyMode::Streaming).await;
        assert!(out.contains("hello"), "{client}: {out}");
        let sent = &upstream.captured()[0];
        assert_eq!(sent.path, "/coding/v1/responses");
        assert_eq!(sent.header("accept"), Some("text/event-stream"));
    }
}

#[tokio::test]
async fn a_clean_chat_end_without_done_completes_like_the_old_path() {
    let upstream = StreamUpstream::start([Reply::sse(chat_sse(false))]).await;
    let exchange = exchange(&upstream, "kimi-k2");
    let streams = Streams::new();
    let (started, id, _request) = run(&streams, &exchange, "chat", CHAT, usize::MAX).await;
    let (outcome, published, _) = finish(&streams, started.unwrap(), id).await;
    outcome.unwrap();
    assert_eq!(published.terminal, Some(Terminal::Done));
    assert_eq!(errors(&published), 0);
}

#[tokio::test]
async fn a_chat_end_before_the_finish_is_one_error() {
    let unfinished = chunk(r#"{"role":"assistant","content":"hel"}"#, "null");
    let upstream = StreamUpstream::start([Reply::sse(unfinished)]).await;
    let exchange = exchange(&upstream, "kimi-k2");
    let streams = Streams::new();
    let (started, id, _request) = run(&streams, &exchange, "chat", CHAT, usize::MAX).await;
    let (outcome, published, _) = finish(&streams, started.unwrap(), id).await;
    assert_eq!(outcome.unwrap_err().status, 502);
    assert_eq!(errors(&published), 1, "{:?}", records(&published));
}

#[tokio::test]
async fn a_body_cut_mid_stream_publishes_one_error() {
    for (client, body, sse) in [
        ("chat", CHAT, chat_sse(true)),
        ("responses", RESPONSES, RESPONSES_SSE.to_owned()),
    ] {
        let cut = sse.len() - 30;
        let upstream = StreamUpstream::start([Reply::sse(sse).cut_after(cut)]).await;
        let mut exchange = exchange(&upstream, "kimi-k2");
        let capture = Arc::new(Recorder::default());
        exchange.usage = UsageSink::default().with_capture(CaptureSink::new(capture.clone()));
        let streams = Streams::new();
        let (started, id, _request) = run(&streams, &exchange, client, body, usize::MAX).await;
        let (outcome, published, _) = finish(&streams, started.unwrap(), id).await;
        let error = outcome.unwrap_err();
        assert_eq!((error.status, error.scope), (502, FailureScope::Transport), "{client}");
        assert_eq!(errors(&published), 1, "{client}: {:?}", records(&published));
        assert_eq!(published.terminal, Some(Terminal::UpstreamError), "{client}");
        let last = capture.0.lock().unwrap().last().cloned().unwrap();
        assert!(last.starts_with("error "), "{client}: the cut is captured: {last}");
    }
}

/// The old path's scanner limits: a Chat line over 1 MiB fails the stream, a Responses line
/// may hold up to 50 MiB. The long lines are JSON padded with white space.
#[tokio::test]
async fn line_limits_are_the_old_paths() {
    let pad = " ".repeat(1_100_000);
    let long = chat_sse(true).replacen("\"content\":\"hel\"", &format!("\"content\":\"hel\"{pad}"), 1);
    let upstream = StreamUpstream::start([Reply::sse(long)]).await;
    let chat = exchange(&upstream, "kimi-k2");
    let streams = Streams::new();
    let (started, id, _request) = run(&streams, &chat, "chat", CHAT, usize::MAX).await;
    let (outcome, published, _) = finish(&streams, started.unwrap(), id).await;
    assert!(outcome.is_err(), "a Chat line over 1 MiB");
    assert_eq!(errors(&published), 1);

    let long = RESPONSES_SSE.replacen("\"delta\":\"hello\"", &format!("\"delta\":\"hello\"{pad}"), 1);
    let upstream = StreamUpstream::start([Reply::sse(long)]).await;
    let exchange = exchange(&upstream, "kimi-k3");
    let streams = Streams::new();
    let (started, id, _request) = run(&streams, &exchange, "responses", RESPONSES, usize::MAX).await;
    let (outcome, published, _) = finish(&streams, started.unwrap(), id).await;
    outcome.expect("a Responses line over 1 MiB");
    assert_eq!(published.terminal, Some(Terminal::Done));
}

#[tokio::test]
async fn upstream_refusals_keep_the_old_classification_and_body() {
    let body = r#"{"error":{"message":"rate limited","type":"rate_limit_error"}}"#;
    for (status, scope) in [
        (429, FailureScope::Model),
        (401, FailureScope::Credential),
        (400, FailureScope::Request),
    ] {
        let upstream = StreamUpstream::start([Reply::status(status, "application/json", body)]).await;
        let exchange = exchange(&upstream, "kimi-k2");
        let streams = Streams::new();
        let (started, _, _request) = run(&streams, &exchange, "chat", CHAT, usize::MAX).await;
        let error = started.err().expect("a refusal fails the attempt");
        assert_eq!((error.status, error.scope), (status, scope));
        assert_eq!(&error.body[..], body.as_bytes(), "the body as received");
        assert_eq!(error.retry_after, None);
    }
}

#[tokio::test]
async fn a_transport_failure_names_no_credential() {
    let upstream = StreamUpstream::start([Reply::reset()]).await;
    let exchange = exchange(&upstream, "kimi-k2");
    let streams = Streams::new();
    let (started, _, _request) = run(&streams, &exchange, "chat", CHAT, usize::MAX).await;
    let error = started.err().expect("a reset fails the attempt");
    assert!(!String::from_utf8_lossy(&error.body).contains(TOKEN));
}

async fn refused(client: &str, operation: Operation, body: &[u8]) -> codec::Error {
    let upstream = StreamUpstream::start([]).await;
    let exchange = exchange(&upstream, "kimi-k3");
    let streams = Streams::new();
    let (outcome, _request) = try_run(&streams, &exchange, client, operation, body, usize::MAX).await;
    let error = outcome.err().expect("the fence refuses it");
    assert_eq!(upstream.requests(), 0);
    error
}

#[tokio::test]
async fn claude_clients_and_counts_keep_the_old_route() {
    let anthropic = br#"{"model":"kimi-k3","max_tokens":64,"messages":[{"role":"user","content":"hi"}]}"#;
    assert!(matches!(
        refused("anthropic", Operation::Generate, anthropic).await,
        codec::Error::Destination
    ));
    let gemini = br#"{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}"#;
    for (client, body) in [("anthropic", &anthropic[..]), ("gemini", &gemini[..])] {
        let error = refused(client, Operation::CountTokens, body).await;
        assert!(matches!(error, codec::Error::Destination), "{client}");
    }
    let compact = br#"{"model":"kimi-k3","input":"hi"}"#;
    assert!(matches!(
        refused("responses", Operation::Compact, compact).await,
        codec::Error::Unsupported
    ));
}

#[tokio::test]
async fn custom_apply_patch_keeps_the_old_route() {
    let patch = r#"{"type":"custom","name":" apply_patch ","description":"d","format":{"type":"grammar","syntax":"lark","definition":"x"}}"#;
    let call = r#"{"type":"custom_tool_call","call_id":"c1","name":"apply_patch","input":"*** Begin Patch"}"#;
    for body in [
        format!(r#"{{"model":"kimi-k3","input":"hi","tools":[{patch}]}}"#),
        format!(r#"{{"model":"kimi-k3","input":"hi","tools":[{{"type":"namespace","name":"ns","tools":[{patch}]}}]}}"#),
        format!(
            r#"{{"model":"kimi-k3","input":[{call},{{"type":"custom_tool_call_output","call_id":"c1","output":"ok"}}]}}"#
        ),
        format!(
            r#"{{"model":"kimi-k3","input":[{{"type":"additional_tools","tools":[{patch}]}},{{"role":"user","content":"hi"}}]}}"#
        ),
        r#"{"model":"kimi-k3","input":"hi","tool_choice":{"type":"custom","name":"apply_patch"}}"#.to_owned(),
        r#"{"model":"kimi-k3","input":"hi","tool_choice":{"type":"allowed_tools","mode":"auto","tools":[{"type":"custom","name":"apply_patch"}]}}"#.to_owned(),
    ] {
        let error = refused("responses", Operation::Generate, body.as_bytes()).await;
        assert!(matches!(error, codec::Error::Destination), "{body}");
    }
}

#[tokio::test]
async fn apply_patch_lookalikes_in_data_stay_on_the_contract() {
    let lookalike = r#"{"type":"custom","name":"apply_patch"}"#;
    let arguments = serde_json::to_string(lookalike).unwrap();
    for body in [
        format!(
            r#"{{"model":"kimi-k3","input":[{{"type":"function_call","call_id":"c1","name":"lookup","arguments":{arguments}}},{{"type":"function_call_output","call_id":"c1","output":"ok"}}]}}"#
        ),
        format!(
            r#"{{"model":"kimi-k3","input":"hi","tools":[{{"type":"function","name":"lookup","parameters":{{"type":"object","properties":{{"q":{{"type":"object","default":{lookalike}}}}}}}}}]}}"#
        ),
        format!(r#"{{"model":"kimi-k3","input":"hi","metadata_extra":{lookalike}}}"#),
    ] {
        let upstream = StreamUpstream::start([Reply::json(r#"{"id":"r1","object":"response","created_at":1,"status":"completed","model":"k3","output":[],"usage":{"input_tokens":1,"output_tokens":0,"total_tokens":1}}"#)]).await;
        let exchange = exchange(&upstream, "kimi-k3");
        let streams = Streams::new();
        let (started, id, _request) = run(&streams, &exchange, "responses", body.as_bytes(), usize::MAX).await;
        let (outcome, _, _) = finish(&streams, started.unwrap(), id).await;
        outcome.unwrap_or_else(|e| panic!("{body}: {e:?}"));
        assert_eq!(upstream.requests(), 1, "{body}");
    }
}

#[derive(Default)]
struct Recorder(Mutex<Vec<String>>);

impl CaptureObserver for Recorder {
    fn record(&self, event: CaptureEvent<'_>) {
        let line = match event {
            CaptureEvent::Request(request) => format!("request {}", request.url),
            CaptureEvent::ResponseMetadata(status, _) => format!("metadata {status}"),
            CaptureEvent::ResponseChunk(chunk) => format!("chunk {}", chunk.len()),
            CaptureEvent::ResponseError(error) => format!("error {error}"),
            _ => "other".to_owned(),
        };
        self.0.lock().unwrap().push(line);
    }
}

struct Facts(Mutex<Vec<String>>);

impl ck_core::exec::UsageObserver for Facts {
    fn response_body(&self, _: ck_core::format::Format, _: &[u8]) {
        self.0.lock().unwrap().push("response_body".into());
    }
    fn response_line(&self, _: ck_core::format::Format, _: &[u8]) {
        self.0.lock().unwrap().push("response_line".into());
    }
    fn request(&self, _: ck_core::format::Format, _: &[u8]) {
        self.0.lock().unwrap().push("request".into());
    }
    fn request_for(&self, identifier: &str, payload: &[u8]) {
        let model = serde_json::from_slice::<serde_json::Value>(payload).unwrap()["model"].clone();
        self.0.lock().unwrap().push(format!("request_for {identifier} {model}"));
    }
    fn upstream_model(&self, model: &str) {
        self.0.lock().unwrap().push(format!("upstream_model {model}"));
    }
}

#[tokio::test]
async fn request_facts_and_capture_go_through_the_client_usage() {
    let upstream = StreamUpstream::start([Reply::sse(chat_sse(true))]).await;
    let mut exchange = exchange(&upstream, "kimi-k2.8");
    let (capture, facts) = (Arc::new(Recorder::default()), Arc::new(Facts(Mutex::default())));
    exchange.usage = UsageSink::new(facts.clone()).with_capture(CaptureSink::new(capture.clone()));
    let streams = Streams::new();
    let (started, id, _request) = run(&streams, &exchange, "chat", CHAT, usize::MAX).await;
    finish(&streams, started.unwrap(), id).await.0.unwrap();
    let facts = facts.0.lock().unwrap().clone();
    assert_eq!(
        facts,
        ["upstream_model kimi-for-coding", "request_for kimi \"kimi-for-coding\""],
        "request-side facts only (G4)"
    );
    let capture = capture.0.lock().unwrap().clone();
    assert_eq!(
        capture[0],
        format!("request {}/coding/v1/chat/completions", upstream.url())
    );
    assert_eq!(capture[1], "metadata 200");
    let chunks: usize = capture[2..]
        .iter()
        .map(|l| l.strip_prefix("chunk ").unwrap().parse::<usize>().unwrap())
        .sum();
    assert_eq!(chunks, chat_sse(true).len(), "every response byte is captured");
}
