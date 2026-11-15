//! Reasoning replay: keyed by caller, credential and the exact session, and published only
//! once the ingress accepted a successful completion.

use super::*;

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

/// Sessions that differ only in surrounding whitespace are different sessions.
#[tokio::test]
async fn a_padded_session_is_another_session() {
    let executors = executors();
    let a = ("sk-a", "xai-1");
    let (_, ok) = turn(&executors, a, TURN, Reply::sse(reasoning_turn())).await;
    assert!(ok);
    let (sent, _) = turn(&executors, a, &NEXT.replace("\"k1\"", "\" k1 \""), Reply::sse(sse())).await;
    assert!(!sent["input"].to_string().contains(ENC), "{sent}");
    let (sent, _) = turn(&executors, a, NEXT, Reply::sse(sse())).await;
    assert!(sent["input"].to_string().contains(ENC), "{sent}");
}

/// An upstream error ends the response; a completion after it in the same chunk, which the
/// ingress ignores, caches nothing. The error is read as the ingress reads it, from the whole
/// SSE event: named by its type, by an `event` line after its data, or split over data lines.
#[tokio::test]
async fn a_completion_after_an_error_caches_nothing() {
    let typed = event(json!({"type":"error","code":"server_error","message":"boom"}));
    let named_after = "data: {\"message\":\"boom\"}\nevent: error\n\n".to_owned();
    let split = "data: {\"type\":\ndata: \"error\",\"message\":\"boom\"}\n\n".to_owned();
    for error in [typed, named_after, split] {
        let executors = executors();
        let a = ("sk-a", "xai-1");
        let at = "event: response.completed";
        let body = reasoning_turn().replacen(at, &format!("{error}{at}"), 1);
        let (_, ok) = turn(&executors, a, TURN, Reply::sse(body)).await;
        assert!(!ok, "the error fails the attempt: {error}");
        let (sent, _) = turn(&executors, a, NEXT, Reply::sse(sse())).await;
        assert!(!sent["input"].to_string().contains(ENC), "{error}: {sent}");
    }
}
