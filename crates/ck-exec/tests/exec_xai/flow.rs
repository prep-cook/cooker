//! Generation from every client protocol, xAI's line rules, any split of the body, the
//! local count, the native compact, reasoning replay and the selector.

use super::*;

const CHAT: &str = r#"{"model":"grok-4.3","stream":true,"max_tokens":64,"messages":[{"role":"user","content":"hi"}]}"#;
const RESPONSES: &str = r#"{"model":"grok-4.3","stream":true,"input":"hi"}"#;
const ANTHROPIC: &str =
    r#"{"model":"grok-4.3","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
const GEMINI: &str = r#"{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}"#;

/// Every client protocol goes upstream as a streamed Responses request with the API key, and
/// every client egress accepts the canonical response, streamed and buffered. A client that
/// does not stream still gets a streamed upstream.
#[tokio::test]
async fn every_client_protocol_streams_upstream_and_comes_back() {
    for (client, body) in [
        (Client::Chat, CHAT),
        (Client::Responses, RESPONSES),
        (Client::Anthropic, ANTHROPIC),
        (Client::Gemini, GEMINI),
        (
            Client::Chat,
            r#"{"model":"m","stream":false,"messages":[{"role":"user","content":"hi"}]}"#,
        ),
    ] {
        let streams = Streams::new();
        let upstream = StreamUpstream::start([Reply::sse(sse())]).await;
        let run = run(&streams, &upstream, client, body, 5).await;
        let (outcome, published, mut owner) = produce(&streams, run.started.unwrap()).await;
        outcome.unwrap();
        assert_eq!(published.terminal, Some(Terminal::Done), "{client:?}");
        let captured = &upstream.captured()[0];
        assert_eq!(
            (captured.method.as_str(), captured.path.as_str()),
            ("POST", "/v1/responses")
        );
        assert_eq!(captured.header("authorization"), Some("Bearer xai-test-key"));
        assert_eq!(captured.header("accept"), Some("text/event-stream"));
        assert_eq!(captured.header("connection"), Some("Keep-Alive"));
        let sent = json_body(captured);
        assert_eq!(sent["model"], MODEL, "{client:?}: {sent}");
        assert_eq!(sent["stream"], true, "{client:?}: {sent}");
        assert_eq!(sent["store"], false, "{client:?}: {sent}");
        assert_eq!(
            sent["include"],
            json!(["reasoning.encrypted_content"]),
            "{client:?}: {sent}"
        );
        assert!(sent["input"].to_string().contains("hi"), "{client:?}: {sent}");
        for mode in [BodyMode::Streaming, BodyMode::Buffered] {
            assert_every_egress(&streams, &mut owner, mode);
        }
    }
}

fn assert_every_egress(streams: &Streams, owner: &mut Writer, mode: BodyMode) {
    let id = identity(Direction::Response);
    let op = Operation::Generate;
    let mut out = chat::egress::Egress::new(owner, id, op, mode, true, |_| {}).unwrap();
    let text = deliver(streams, owner, &mut out);
    assert!(text.contains("hel") && text.contains("stop"), "chat: {text}");
    let mut out = anthropic::egress::Egress::new(owner, id, op, mode, |_| {}).unwrap();
    let text = deliver(streams, owner, &mut out);
    assert!(text.contains("hel") && text.contains("end_turn"), "anthropic: {text}");
    let mut out = responses::egress::Egress::new(owner, op, mode, |_, _| {}).unwrap();
    let text = deliver(streams, owner, &mut out);
    assert!(text.contains("hel") && text.contains("completed"), "responses: {text}");
    let mut out = gemini::egress::Egress::new(owner, id, op, mode, |_| {}).unwrap();
    let text = deliver(streams, owner, &mut out);
    assert!(text.contains("hel") && text.contains("STOP"), "gemini: {text}");
}

/// The canonical stream does not depend on how the body was split, a completion whose
/// `output` is empty included.
#[tokio::test]
async fn any_split_gives_the_same_records() {
    for body in [sse(), sse_with(json!([]))] {
        let mut seen = Vec::new();
        for reply in [Reply::sse(body.clone()), Reply::sse(body.clone()).byte_at_a_time()] {
            let streams = Streams::new();
            let upstream = StreamUpstream::start([reply]).await;
            let run = run(&streams, &upstream, Client::Responses, RESPONSES, 64).await;
            let (outcome, published, _) = produce(&streams, run.started.unwrap()).await;
            outcome.unwrap();
            seen.push(records(&published));
        }
        assert_eq!(seen[0], seen[1]);
    }
}

/// A completion with an empty `output` (as Grok sends it) gets the announced items, from the
/// response stream's scratch, so a buffered client sees its message.
#[tokio::test]
async fn an_empty_completion_gets_the_announced_items() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::sse(sse_with(json!([])))]).await;
    let run = run(&streams, &upstream, Client::Responses, RESPONSES, 64).await;
    let (outcome, _, mut owner) = produce(&streams, run.started.unwrap()).await;
    outcome.unwrap();
    let out = responses_json(&streams, &mut owner);
    assert_eq!(out["output"], json!([message("hello")]), "{out}");
}

/// Grok's reasoning text reaches a Responses client as a reasoning summary.
#[tokio::test]
async fn reasoning_text_becomes_a_summary() {
    let reasoning = json!({"type":"reasoning","id":"rs_1","summary":[],"content":[{"type":"reasoning_text","text":"think"}],"encrypted_content":"enc"});
    let body = [
        event(json!({"type":"response.created","response":response("in_progress", json!([]))})),
        event(
            json!({"type":"response.output_item.added","output_index":0,"item":{"type":"reasoning","id":"rs_1","summary":[]}}),
        ),
        event(
            json!({"type":"response.reasoning_text.delta","item_id":"rs_1","output_index":0,"content_index":0,"delta":"think"}),
        ),
        event(
            json!({"type":"response.reasoning_text.done","item_id":"rs_1","output_index":0,"content_index":0,"text":"think"}),
        ),
        event(json!({"type":"response.output_item.done","output_index":0,"item":reasoning})),
        event(json!({"type":"response.completed","response":response("completed", json!([reasoning]))})),
    ]
    .concat();
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::sse(body)]).await;
    let run = run(&streams, &upstream, Client::Responses, RESPONSES, 64).await;
    let (outcome, _, mut owner) = produce(&streams, run.started.unwrap()).await;
    outcome.unwrap();
    let out = responses_json(&streams, &mut owner);
    assert_eq!(
        out["output"][0]["summary"],
        json!([{"type":"summary_text","text":"think"}]),
        "{out}"
    );
}

/// A count is local over the body generation would send: nothing goes upstream.
#[tokio::test]
async fn a_count_is_local() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([]).await;
    let body = r#"{"model":"grok-4.3","messages":[{"role":"user","content":"hello there, how are you"}]}"#;
    let input = (body, 64, Sessions::default());
    let run = run_on(
        &streams,
        &executors(),
        &exchange(&upstream),
        Client::Anthropic,
        Operation::CountTokens,
        input,
    )
    .await;
    let (outcome, published, _) = produce(&streams, run.started.unwrap()).await;
    outcome.unwrap();
    assert_eq!(upstream.requests(), 0);
    let count = records(&published)
        .into_iter()
        .find(|r| r.contains("event: x_ck.count_tokens"))
        .expect("a count record");
    assert!(!count.contains("\"input_tokens\":0"), "{count}");
}

/// Compact is xAI's native `/responses/compact`, buffered, without stream flag or tools.
#[tokio::test]
async fn compact_goes_to_the_native_compact() {
    let compaction = r#"{"id":"cmp_1","object":"response.compaction","created_at":4,"model":"grok-4.3","output":[{"id":"c","type":"compaction","encrypted_content":"secret"}],"usage":{"input_tokens":3,"output_tokens":1,"total_tokens":4}}"#;
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::json(compaction)]).await;
    let body = r#"{"model":"grok-4.3","input":[{"role":"user","content":"hi"}],"previous_response_id":"resp_0"}"#;
    let input = (body, 64, Sessions::default());
    let run = run_on(
        &streams,
        &executors(),
        &exchange(&upstream),
        Client::Responses,
        Operation::Compact,
        input,
    )
    .await;
    let (outcome, published, _) = produce(&streams, run.started.unwrap()).await;
    outcome.unwrap();
    assert_eq!(published.terminal, Some(Terminal::Done));
    let captured = &upstream.captured()[0];
    assert_eq!(captured.path, "/v1/responses/compact");
    assert_eq!(captured.header("accept"), Some("application/json"));
    let sent = json_body(captured);
    assert!(sent.get("stream").is_none() && sent.get("tools").is_none(), "{sent}");
    assert_eq!(sent["previous_response_id"], "resp_0", "{sent}");
    assert!(records(&published).iter().any(|r| r.contains("secret")));
}

/// Grok's encrypted reasoning, as the Go recording has it (other blobs are sanitized away).
const ENC: &str = "TKwE++a7qP4CXIXITJIj2DYyQCSyR3JuUSrOe758M0CwwzrC2cbGVuOaEQaBqIrN6dvHWTX7FJ0iqroON2AsUyUnrtPc/TZpMRjPoWUeMN4m8PpoZ7WEOcq9ouGU9GXp1dV2okgnwdVvFfliii45xpY2Vr+czC4aqEgQFOmfOkP/1/2QzRooWeLd0i2AVLOOckBWoRQ1jCCa1racBhwpsfYf7Ax5lb393La7xuA7EJm6/n9nslYW3GJXW4GKnsKysoL7Ml0tLwoN1m+lBumus/5uivtkjccC5s5Jrf6Q41NADO7Q4yeiDfkUs7Ro4NVGD/ehf4+8qBtI28X3Gx54Fw";

/// One completed Responses turn with encrypted reasoning and a message.
fn reasoning_turn() -> String {
    let reasoning = json!({"type":"reasoning","id":"rs_1","summary":[],"encrypted_content":ENC,"status":"completed"});
    [
        event(json!({"type":"response.created","response":response("in_progress", json!([]))})),
        event(json!({"type":"response.output_item.done","output_index":0,"item":reasoning})),
        event(json!({"type":"response.output_item.done","output_index":1,"item":message("hello")})),
        event(
            json!({"type":"response.completed","response":response("completed", json!([reasoning, message("hello")]))}),
        ),
    ]
    .concat()
}

const TURN: &str =
    r#"{"model":"grok-4.3","stream":true,"prompt_cache_key":"k1","input":[{"role":"user","content":"hi"}]}"#;
const NEXT: &str = r#"{"model":"grok-4.3","stream":true,"prompt_cache_key":"k1","input":[{"role":"user","content":"hi"},{"role":"user","content":"more"}]}"#;

/// One turn of `body` on `executors` from caller `principal` over credential `id`: what went
/// upstream and the producer's outcome.
async fn turn(executors: &Executors, (principal, id): (&str, &str), body: &str, reply: Reply) -> (Value, bool) {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([reply]).await;
    let mut exchange = exchange(&upstream);
    exchange.caller.principal = principal.into();
    exchange.credential.id = id.into();
    let input = (body, 64, Sessions::default());
    let run = run_on(
        &streams,
        executors,
        &exchange,
        Client::Responses,
        Operation::Generate,
        input,
    )
    .await;
    let (outcome, _, _) = produce(&streams, run.started.unwrap()).await;
    (json_body(&upstream.captured()[0]), outcome.is_ok())
}

/// A completed turn's encrypted reasoning returns in the next turn of the same session,
/// caller and credential only (another caller's or credential's turn, which completes without
/// reasoning and so clears its own entry, leaves it in place); a completion cut before its event ended caches nothing.
#[tokio::test]
async fn replay_is_namespaced_and_published_after_completion() {
    let executors = executors();
    let a = ("sk-a", "xai-1");
    let (_, ok) = turn(&executors, a, TURN, Reply::sse(reasoning_turn())).await;
    assert!(ok);
    for other in [("sk-b", "xai-1"), ("sk-a", "xai-2")] {
        let (sent, _) = turn(&executors, other, NEXT, Reply::sse(sse())).await;
        assert!(!sent["input"].to_string().contains(ENC), "{other:?}: {sent}");
    }
    let (sent, _) = turn(&executors, a, NEXT, Reply::sse(sse())).await;
    assert!(sent["input"].to_string().contains(ENC), "{sent}");
    let body = reasoning_turn();
    let (_, ok) = turn(
        &executors,
        a,
        &TURN.replace("k1", "k2"),
        Reply::sse(body.trim_end().to_owned()),
    )
    .await;
    assert!(!ok, "a completion without its blank line does not complete");
    let (sent, _) = turn(&executors, a, &NEXT.replace("k1", "k2"), Reply::sse(sse())).await;
    assert!(!sent["input"].to_string().contains(ENC), "{sent}");
}

/// The selector sends xAI credentials through this executor.
#[test]
fn the_selector_has_an_xai_arm() {
    let streams = Streams::new();
    let exchange = Exchange::new(credential("http://127.0.0.1:1/v1"), MODEL, "/v1/responses");
    assert_eq!(executors().destination(&exchange.attempt(&streams.store)), Some("xai"));
}

/// Grep gate: the executor files call neither the old pair registry nor the usage sink, and
/// the legacy modules over the size cap do not grow (size ratchet).
#[test]
fn the_executor_uses_no_old_translation() {
    for source in [
        include_str!("../../src/xai_exec.rs"),
        include_str!("../../src/xai_events.rs"),
        include_str!("../../src/xai_shape.rs"),
    ] {
        for old in [
            "ck_translate::pair",
            "translate_",
            "StreamTranslator",
            "ck_translate::stream::",
            ".response_",
        ] {
            assert!(!source.contains(old), "{old}");
        }
    }
    for (lines, cap) in [
        (include_str!("../../src/xai.rs").lines().count(), 1196),
        (include_str!("../../src/xai_request.rs").lines().count(), 1446),
        (include_str!("../../src/xai_response.rs").lines().count(), 910),
        (include_str!("../../src/xai_url.rs").lines().count(), 627),
    ] {
        assert!(lines <= cap, "{lines} > {cap}");
    }
}
