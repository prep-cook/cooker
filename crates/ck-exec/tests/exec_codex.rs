//! The Codex executor under the execution contract, against the DT local upstream: the request
//! every client protocol puts on the wire, the canonical response every client egress accepts,
//! the local count, the native compact, and failures before and after the upstream answered.

mod support;

#[path = "exec_codex/common.rs"]
mod common;
#[path = "exec_codex/count.rs"]
mod count;
#[path = "exec_codex/failures.rs"]
mod failures;
#[path = "exec_codex/parity.rs"]
mod parity;
#[path = "exec_codex/usage.rs"]
mod usage;

use ck_stream::Terminal;
use ck_translate::codec::{BodyMode, Operation};
use common::*;
use serde_json::{Value, json};
use support::Streams;
use support::upstream::{Reply, StreamUpstream};

/// Every client protocol goes upstream as the Responses request the Codex backend takes: the
/// upstream model, always streamed, `store` off, encrypted reasoning included, no sampling or
/// limit members, an `instructions` string, and the API key with the Codex headers. Its own
/// egress accepts the canonical response, streamed and buffered.
#[tokio::test]
async fn every_client_protocol_goes_out_as_codex_and_comes_back() {
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
        assert_eq!(captured.header("authorization"), Some("Bearer sk-up"), "{protocol}");
        assert_eq!(captured.header("accept"), Some("text/event-stream"), "{protocol}");
        assert_eq!(captured.header("user-agent"), Some("cooker"), "{protocol}");
        let sent = json_body(captured);
        assert_eq!(sent["model"], MODEL, "{protocol}: {sent}");
        assert_eq!(sent["stream"], true, "{protocol}: {sent}");
        assert_eq!(sent["store"], false, "{protocol}: {sent}");
        assert_eq!(
            sent["include"],
            json!(["reasoning.encrypted_content"]),
            "{protocol}: {sent}"
        );
        assert!(sent["instructions"].is_string(), "{protocol}: {sent}");
        for absent in ["max_output_tokens", "temperature", "stream_options"] {
            assert!(sent.get(absent).is_none(), "{protocol} {absent}: {sent}");
        }
        let input = sent["input"].as_array().unwrap();
        assert!(input.last().unwrap().to_string().contains("hi"), "{protocol}: {sent}");
        assert!(input.iter().all(|i| i["role"] != "system"), "{protocol}: {sent}");
        for (mode, done) in [(BodyMode::Streaming, streamed), (BodyMode::Buffered, buffered)] {
            let out = egress(&streams, protocol, &mut stream, id, mode).await;
            assert!(
                out.contains("hel") && out.contains("lo") && out.contains(done),
                "{protocol}: {out}"
            );
        }
    }
}

/// The backend sends `response.completed` with an empty `output`; the items announced by
/// `response.output_item.done` complete it, so a buffered Responses client gets the message.
#[tokio::test]
async fn an_empty_completion_gets_the_announced_items() {
    let upstream = StreamUpstream::start([Reply::sse(sse())]).await;
    let (streams, exchange) = (Streams::new(), exchange(&upstream));
    let body = br#"{"model":"client-model","input":"hi"}"#;
    let (started, id, _request) = run(&streams, &exchange, "responses", Operation::Generate, body, 64).await;
    let (outcome, _, mut stream) = finish(&streams, started.unwrap(), id).await;
    outcome.unwrap();
    let sent = json_body(&upstream.captured()[0]);
    assert_eq!(sent["stream"], true, "a buffered client still streams upstream: {sent}");
    let out: Value =
        serde_json::from_str(&egress(&streams, "responses", &mut stream, id, BodyMode::Buffered).await).unwrap();
    assert_eq!(out["status"], "completed", "{out}");
    assert_eq!(out["output"][0]["id"], "msg_1", "{out}");
    assert_eq!(out["output"][0]["content"][0]["text"], "hello", "{out}");
    assert_eq!(out["usage"]["output_tokens"], 2, "{out}");
}

/// The canonical stream does not depend on how the upstream body was split, nor on whether an
/// event's JSON spans several `data:` lines.
#[tokio::test]
async fn any_split_gives_the_same_response() {
    let spread = sse().replacen(
        r#"data: {"type":"response.output_text.delta","sequence_number":3,"#,
        "data: {\"type\":\"response.output_text.delta\",\ndata: \"sequence_number\":3,",
        1,
    );
    assert_ne!(spread, sse());
    let mut seen = Vec::new();
    for reply in [
        Reply::sse(sse()),
        Reply::sse(sse()).byte_at_a_time(),
        Reply::sse(spread.clone()),
        Reply::sse(spread.replace('\n', "\r\n")).byte_at_a_time(),
    ] {
        let upstream = StreamUpstream::start([reply]).await;
        let (streams, exchange) = (Streams::new(), exchange(&upstream));
        let (started, id, _request) = run(&streams, &exchange, "chat", Operation::Generate, CHAT, 64).await;
        let (outcome, published, _stream) = finish(&streams, started.unwrap(), id).await;
        outcome.unwrap();
        seen.push(records(&published));
    }
    assert!(seen.iter().all(|records| *records == seen[0]), "{seen:#?}");
}

/// An unrecognized done event cannot supply the completion boundary.
#[tokio::test]
async fn c11_response_done_does_not_complete_the_response() {
    let done = event(
        "response.done",
        r#"{"type":"response.done","response":{"id":"resp_1","object":"response","created_at":1,"status":"completed","model":"gpt-5","output":[]}}"#,
    );
    for body in [done.clone(), message_events() + &done] {
        let upstream = StreamUpstream::start([Reply::sse(body)]).await;
        let (streams, exchange) = (Streams::new(), exchange(&upstream));
        let (started, id, _request) = run(&streams, &exchange, "anthropic", Operation::Generate, ANTHROPIC, 64).await;
        let (outcome, published, mut stream) = finish(&streams, started.unwrap(), id).await;
        assert_eq!(
            outcome.unwrap_err().status,
            408,
            "response.done is not a completion boundary"
        );
        assert_eq!(published.terminal, Some(Terminal::UpstreamError));
        let out = egress(&streams, "anthropic", &mut stream, id, BodyMode::Streaming).await;
        assert!(out.contains("error"), "{out}");
        assert_eq!(upstream.captured().len(), 1);
    }
}

/// The upstream model's thinking suffix becomes the reasoning effort, and the client's own
/// prompt cache key names the cache and the session header.
#[tokio::test]
async fn suffix_thinking_and_the_prompt_cache_key_reach_the_upload() {
    let upstream = StreamUpstream::start([Reply::sse(sse())]).await;
    let mut exchange = exchange(&upstream);
    exchange.model = format!("{MODEL}(high)");
    let streams = Streams::new();
    let body = br#"{"model":"client-model","stream":true,"input":"hi","prompt_cache_key":"pk-1"}"#;
    let (started, id, _request) = run(&streams, &exchange, "responses", Operation::Generate, body, 64).await;
    finish(&streams, started.unwrap(), id).await.0.unwrap();
    let captured = &upstream.captured()[0];
    let sent = json_body(captured);
    assert_eq!(sent["model"], MODEL, "{sent}");
    assert_eq!(sent["reasoning"]["effort"], "high", "{sent}");
    assert_eq!(sent["prompt_cache_key"], "pk-1", "{sent}");
    assert_eq!(captured.header("session-id"), Some("pk-1"));
}

/// With the multi-agent v2 optimization, the `collaboration` namespace goes upstream renamed,
/// and the upstream's calls reach the client under the client's name again.
#[tokio::test]
async fn the_collaboration_namespace_is_restored_in_responses() {
    let call = r#"{"id":"fc_1","type":"function_call","status":"completed","namespace":"collaboration-optimize","name":"spawn_agent","call_id":"call_1","arguments":"{}"}"#;
    let body = [
        event(
            "response.created",
            r#"{"type":"response.created","response":{"id":"resp_1","object":"response","created_at":1,"status":"in_progress","model":"gpt-5","output":[]}}"#,
        ),
        event(
            "response.output_item.added",
            &format!(r#"{{"type":"response.output_item.added","output_index":0,"item":{call}}}"#),
        ),
        event(
            "response.output_item.done",
            &format!(r#"{{"type":"response.output_item.done","output_index":0,"item":{call}}}"#),
        ),
        event(
            "response.completed",
            r#"{"type":"response.completed","response":{"id":"resp_1","object":"response","created_at":1,"status":"completed","model":"gpt-5","output":[]}}"#,
        ),
    ]
    .concat();
    let upstream = StreamUpstream::start([Reply::sse(body)]).await;
    let mut exchange = exchange(&upstream);
    exchange.cfg = ck_core::config::Config::parse("client:\n  codex:\n    optimize-multi-agent-v2: true\n").unwrap();
    exchange
        .headers
        .insert("user-agent", http::HeaderValue::from_static("codex_cli_rs/0.50.0"));
    let streams = Streams::new();
    let request = br#"{"model":"client-model","stream":true,"input":"hi","tools":[{"type":"namespace","name":"collaboration","description":"agents","tools":[{"type":"function","name":"spawn_agent","description":"spawn","parameters":{"type":"object","properties":{}}}]}]}"#;
    let (started, id, _request) = run(&streams, &exchange, "responses", Operation::Generate, request, 64).await;
    let (outcome, _, mut stream) = finish(&streams, started.unwrap(), id).await;
    outcome.unwrap();
    let sent = json_body(&upstream.captured()[0]);
    assert_eq!(sent["tools"][0]["name"], "collaboration-optimize", "{sent}");
    let out = egress(&streams, "responses", &mut stream, id, BodyMode::Buffered).await;
    assert!(out.contains(r#""namespace":"collaboration""#), "{out}");
    assert!(!out.contains("collaboration-optimize"), "{out}");
}
