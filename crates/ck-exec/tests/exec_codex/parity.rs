//! The same-protocol path against the old execution path, and the reasoning replay of Claude
//! clients keyed by the execution session.

use bytes::Bytes;
use ck_core::exec::{ExecRequest, Operation as ExecOperation};
use ck_core::format::Format;
use ck_translate::codec::Operation;
use futures_util::StreamExt;
use http::{HeaderMap, HeaderValue};

use crate::common::*;
use crate::support::upstream::{Captured, Reply, StreamUpstream};
use crate::support::{Exchange, Streams};

const RICH: &str = r#"{"model":"client-model","stream":STREAM,"instructions":"be brief","input":[{"type":"message","role":"system","content":[{"type":"input_text","text":"rules"}]},{"type":"message","role":"user","content":[{"type":"input_text","text":"hi","prompt_cache_breakpoint":{"ttl":"5m"}}]},{"type":"function_call","call_id":"call_1","name":"lookup","arguments":"{}"},{"type":"function_call_output","call_id":"call_1","output":"found"}],"tools":[{"type":"function","name":"lookup","description":"Look up","parameters":{"type":"object","properties":{"q":{"type":"string"}}}},{"type":"web_search_preview"}],"tool_choice":"auto","max_output_tokens":100,"temperature":0.5,"reasoning":{"effort":"high","summary":"auto"},"store":true,"service_tier":"fast","prompt_cache_key":"pk-1","text":{"verbosity":"low"},"user":"u1","truncation":"auto","include":["web_search_call.action.sources"],"client_metadata":{"x-codex-window-id":"w1"}}"#;

fn client_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("user-agent", "codex_cli_rs/0.50.0 (Linux; x86_64)"),
        ("originator", "codex_cli_rs"),
        ("x-codex-window-id", "w1"),
        ("version", "0.50.0"),
    ] {
        headers.insert(name, HeaderValue::from_static(value));
    }
    headers
}

/// The old path's upload for `body` from a `source` client.
async fn old_path(exchange: &Exchange, body: &[u8], stream: bool, source: Format) {
    old_path_on(&executor(), exchange, body, stream, source).await;
}

async fn old_path_on(
    executor: &ck_exec::codex::CodexExecutor,
    exchange: &Exchange,
    body: &[u8],
    stream: bool,
    source: Format,
) {
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
    let response = executor
        .execute(&exchange.credential, request, &exchange.cfg)
        .await
        .unwrap();
    if let ck_core::exec::ResponseBody::Stream(mut events) = response.body {
        while events.next().await.is_some() {}
    }
}

/// Headers both paths send, without those of the connection.
fn headers(captured: &Captured) -> Vec<(String, String)> {
    let mut headers: Vec<_> = captured
        .headers
        .iter()
        .filter(|(n, _)| !n.eq_ignore_ascii_case("host") && !n.eq_ignore_ascii_case("content-length"))
        .map(|(n, v)| (n.to_ascii_lowercase(), v.clone()))
        .collect();
    headers.sort();
    headers
}

/// A Responses client's request reaches the backend as the old path sent it: a JSON-equal body
/// and the same headers, streamed and buffered.
#[tokio::test]
async fn a_responses_client_matches_the_old_path() {
    for stream in [true, false] {
        let body = RICH.replace("STREAM", if stream { "true" } else { "false" });
        let upstream = StreamUpstream::start([Reply::sse(sse()), Reply::sse(sse())]).await;
        let mut exchange = exchange(&upstream);
        exchange.headers = client_headers();
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
        old_path(&exchange, body.as_bytes(), stream, Format::OpenAIResponse).await;
        let captured = upstream.captured();
        let (new, old) = (&captured[0], &captured[1]);
        let sent = json_body(new);
        assert_eq!(sent, json_body(old), "stream {stream}");
        // The Codex requirements on this body: none of them is left to the old path alone.
        assert_eq!(sent["input"][0]["role"], "developer", "{sent}");
        assert!(
            sent["input"][1]["content"][0].get("prompt_cache_breakpoint").is_none(),
            "{sent}"
        );
        assert_eq!(sent["tools"][1]["type"], "web_search", "{sent}");
        assert_eq!(sent["service_tier"], "priority", "{sent}");
        assert_eq!(headers(new), headers(old), "stream {stream}");
        assert_eq!(new.path, old.path);
    }
}

/// A well-formed encrypted reasoning token (version, timestamp, IV, one AES block, HMAC).
const SIGNATURE: &str =
    "gAAAAAAAAAAABwcHBwcHBwcHBwcHBwcHBwkJCQkJCQkJCQkJCQkJCQkDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAw==";

/// A completed turn with reasoning, as the backend reports it.
fn reasoning_turn() -> String {
    event(
        "response.completed",
        &r#"{"type":"response.completed","response":{"id":"resp_1","object":"response","created_at":1,"status":"completed","model":"gpt-5","output":[{"id":"rs_1","type":"reasoning","encrypted_content":"SIG","summary":[]},{"id":"msg_1","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"hello","annotations":[]}]}]}}"#
            .replace("SIG", SIGNATURE),
    )
}

/// A Claude client drops Codex's encrypted reasoning from its history. The executor caches a
/// completed turn per execution session and puts its reasoning back into the next request of
/// that session only.
#[tokio::test]
async fn claude_reasoning_is_replayed_within_its_execution_session() {
    let first =
        br#"{"model":"client-model","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    let next = br#"{"model":"client-model","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hi"},{"role":"assistant","content":"hello"},{"role":"user","content":"more"}]}"#;
    let upstream = StreamUpstream::start([Reply::sse(reasoning_turn()), Reply::sse(sse()), Reply::sse(sse())]).await;
    let (streams, exchange) = (Streams::new(), exchange(&upstream));
    let executor = executor();
    for (session, body) in [("ws-1", &first[..]), ("ws-1", &next[..]), ("ws-2", &next[..])] {
        let on = (&executor, Some(session));
        let (started, id, _request) = run_on(on, &streams, &exchange, "anthropic", Operation::Generate, body, 64).await;
        finish(&streams, started.unwrap(), id).await.0.unwrap();
    }
    let captured = upstream.captured();
    let replayed = json_body(&captured[1]);
    let items = replayed["input"].as_array().unwrap();
    let reasoning = items
        .iter()
        .position(|i| i["type"] == "reasoning")
        .expect("the turn's reasoning is replayed");
    assert_eq!(items[reasoning]["encrypted_content"], SIGNATURE, "{replayed}");
    let assistant = items.iter().position(|i| i["role"] == "assistant").unwrap();
    assert!(reasoning < assistant, "{replayed}");
    let other = json_body(&captured[2]);
    assert!(
        other["input"]
            .as_array()
            .unwrap()
            .iter()
            .all(|i| i["type"] != "reasoning"),
        "{other}"
    );
}

/// An explicit thinking switch-off reaches Codex as the old path sent it: Anthropic's
/// `disabled` and Gemini's zero budget are `effort:"none"`, not the backend default.
#[tokio::test]
async fn explicit_thinking_off_reaches_codex_as_the_old_path_sent_it() {
    let anthropic = br#"{"model":"client-model","max_tokens":64,"stream":true,"thinking":{"type":"disabled"},"messages":[{"role":"user","content":"hi"}]}"#;
    let gemini = br#"{"contents":[{"role":"user","parts":[{"text":"hi"}]}],"generationConfig":{"thinkingConfig":{"thinkingBudget":0}}}"#;
    for (protocol, source, body) in [
        ("anthropic", Format::Claude, &anthropic[..]),
        ("gemini", Format::Gemini, &gemini[..]),
    ] {
        let upstream = StreamUpstream::start([Reply::sse(sse()), Reply::sse(sse())]).await;
        let (streams, exchange) = (Streams::new(), exchange(&upstream));
        let (started, id, _request) = run(&streams, &exchange, protocol, Operation::Generate, body, 9).await;
        finish(&streams, started.unwrap(), id).await.0.unwrap();
        old_path(&exchange, body, true, source).await;
        let captured = upstream.captured();
        let (new, old) = (json_body(&captured[0]), json_body(&captured[1]));
        assert_eq!(new["reasoning"]["effort"], "none", "{protocol}: {new}");
        assert_eq!(new["reasoning"], old["reasoning"], "{protocol}: {old}");
    }
}

/// A replayed turn stays with the caller and the credential that produced it: another caller,
/// or the same caller on another credential, with the same Claude Code session gets none of
/// its reasoning.
#[tokio::test]
async fn replay_stays_with_its_caller_and_credential() {
    replay_stays_with_its_owner(false).await;
    replay_stays_with_its_owner(true).await;
}

/// Replay isolation on the executor, or with `old` on the old execution path.
async fn replay_stays_with_its_owner(old: bool) {
    let first =
        br#"{"model":"client-model","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
    let next = br#"{"model":"client-model","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hi"},{"role":"assistant","content":"hello"},{"role":"user","content":"more"}]}"#;
    let replies = [
        Reply::sse(reasoning_turn()),
        Reply::sse(sse()),
        Reply::sse(sse()),
        Reply::sse(sse()),
    ];
    let upstream = StreamUpstream::start(replies).await;
    let streams = Streams::new();
    let executor = executor();
    let mut owner = exchange(&upstream);
    owner
        .headers
        .insert("x-claude-code-session-id", HeaderValue::from_static("cc-1"));
    let mut other_caller = exchange(&upstream);
    other_caller.headers = owner.headers.clone();
    other_caller.caller.principal = "sk-client-other".into();
    let mut other_credential = exchange(&upstream);
    other_credential.headers = owner.headers.clone();
    other_credential.credential = crate::support::credential("codex", &upstream.url(), "sk-other");
    other_credential.credential.id = "codex-other".into();
    for (exchange, body) in [
        (&owner, &first[..]),
        (&other_caller, &next[..]),
        (&other_credential, &next[..]),
        (&owner, &next[..]),
    ] {
        if old {
            old_path_on(&executor, exchange, body, true, Format::Claude).await;
            continue;
        }
        let (started, id, _request) = run_on(
            (&executor, None),
            &streams,
            exchange,
            "anthropic",
            Operation::Generate,
            body,
            64,
        )
        .await;
        finish(&streams, started.unwrap(), id).await.0.unwrap();
    }
    let reasoning = |index: usize| {
        let sent = json_body(&upstream.captured()[index]);
        sent["input"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i["type"] == "reasoning")
    };
    assert!(!reasoning(1), "old {old}: another caller");
    assert!(!reasoning(2), "old {old}: another credential");
    assert!(reasoning(3), "old {old}: the owner still gets its turn back");
}
