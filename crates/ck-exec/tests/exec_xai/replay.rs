//! Reasoning replay: keyed by caller, credential and the exact session, and published only
//! once the ingress accepted a successful completion.

use super::*;

/// Grok's encrypted reasoning, as the Go recording has it (other blobs are sanitized away).
pub(super) const ENC: &str = "TKwE++a7qP4CXIXITJIj2DYyQCSyR3JuUSrOe758M0CwwzrC2cbGVuOaEQaBqIrN6dvHWTX7FJ0iqroON2AsUyUnrtPc/TZpMRjPoWUeMN4m8PpoZ7WEOcq9ouGU9GXp1dV2okgnwdVvFfliii45xpY2Vr+czC4aqEgQFOmfOkP/1/2QzRooWeLd0i2AVLOOckBWoRQ1jCCa1racBhwpsfYf7Ax5lb393La7xuA7EJm6/n9nslYW3GJXW4GKnsKysoL7Ml0tLwoN1m+lBumus/5uivtkjccC5s5Jrf6Q41NADO7Q4yeiDfkUs7Ro4NVGD/ehf4+8qBtI28X3Gx54Fw";

/// One completed Responses turn with encrypted reasoning and a message.
fn reasoning_turn() -> String {
    turn_ending("response.completed", "completed")
}

/// A turn with encrypted reasoning and a message, ended by event `kind` whose response has
/// `status`.
fn turn_ending(kind: &str, status: &str) -> String {
    let reasoning = json!({"type":"reasoning","id":"rs_1","summary":[],"encrypted_content":ENC,"status":"completed"});
    [
        event(json!({"type":"response.created","response":response("in_progress", json!([]))})),
        event(json!({"type":"response.output_item.done","output_index":0,"item":reasoning})),
        event(json!({"type":"response.output_item.done","output_index":1,"item":message("hello")})),
        event(json!({"type":kind,"response":response(status, json!([reasoning, message("hello")]))})),
    ]
    .concat()
}

pub(super) const TURN: &str =
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

/// Whether a turn whose upstream body is `body` fails, and whether the next turn of its
/// session gets its encrypted reasoning back.
async fn replayed(body: String) -> (bool, bool) {
    let executors = executors();
    let a = ("sk-a", "xai-1");
    let (_, ok) = turn(&executors, a, TURN, Reply::sse(body)).await;
    let (sent, _) = turn(&executors, a, NEXT, Reply::sse(sse())).await;
    (ok, sent["input"].to_string().contains(ENC))
}

/// `body` with `error` as an event before the completion.
fn before_completion(error: &str) -> String {
    let at = "event: response.completed";
    reasoning_turn().replacen(at, &format!("{error}{at}"), 1)
}

/// An upstream error ends the response; a completion after it, which the ingress ignores,
/// caches nothing. The error is read as the ingress reads the whole SSE event: named by its
/// type, by an `event` line after its data, split over data lines, or behind the BOM that
/// opens the body.
#[tokio::test]
async fn a_completion_after_an_error_caches_nothing() {
    let typed = event(json!({"type":"error","code":"server_error","message":"boom"}));
    let named_after = "data: {\"message\":\"boom\"}\nevent: error\n\n";
    let split = "data: {\"type\":\ndata: \"error\",\"message\":\"boom\"}\n\n";
    let bom_data = "\u{feff}data:{\"type\":\"error\",\"message\":\"boom\"}\n\n";
    let bom_named = "\u{feff}event:error\ndata:{\"message\":\"boom\"}\n\n";
    let bodies = [
        before_completion(&typed),
        before_completion(named_after),
        before_completion(split),
        format!("{bom_data}{}", reasoning_turn()),
        format!("{bom_named}{}", reasoning_turn()),
    ];
    for body in bodies {
        assert_eq!(replayed(body.clone()).await, (false, false), "{body}");
    }
}

/// A completion object on one data line of a larger error event is no completion: the
/// ingress reads the whole event, an error, and nothing is cached.
#[tokio::test]
async fn a_completion_inside_an_error_event_caches_nothing() {
    let turn = reasoning_turn();
    let at = turn.find("event: response.completed").unwrap();
    let completed = turn[at..].lines().nth(1).unwrap().strip_prefix("data: ").unwrap();
    let nested = format!("data: {{\"type\":\"error\",\"nested\":\ndata: {completed}\ndata: }}\n\n");
    let body = format!("{}{nested}", &turn[..at]);
    assert_eq!(replayed(body).await, (false, false));
}

/// Malformed JSON is opaque text to the ingress, not an error: the completion after it is
/// accepted and cached.
#[tokio::test]
async fn malformed_json_is_no_error() {
    for malformed in ["data:{\"type\":\"error\"}junk\n\n", "data:{\"type\":\"error\"\n\n"] {
        assert_eq!(
            replayed(before_completion(malformed)).await,
            (true, true),
            "{malformed}"
        );
    }
}

/// Replay takes no storage the delivery needs: at every pool size, an exchange ends the same
/// way with replay off (no caller) and on. A 240,000-character event before the completion
/// once failed with replay on only, at 17 to 21 frames.
#[tokio::test]
async fn replay_takes_no_storage_from_delivery() {
    let blob = event(json!({"type":"provider.extra","blob":"x".repeat(240_000)}));
    let completed = event(json!({"type":"response.completed","response":response("completed", json!([]))}));
    let body = format!("{blob}{completed}");
    let executors = executors();
    let mut completions = 0;
    for frames in 16..=32 {
        let mut ends = Vec::new();
        for principal in ["", "sk-a"] {
            let streams = Streams::with_frames(frames);
            let upstream = StreamUpstream::start([Reply::sse(body.clone())]).await;
            let mut exchange = exchange(&upstream);
            exchange.caller.principal = principal.into();
            let input = (TURN, 64, Sessions::default());
            let run = run_on(
                &streams,
                &executors,
                &exchange,
                Client::Responses,
                Operation::Generate,
                input,
            )
            .await;
            let (outcome, published, _) = produce(&streams, run.started.unwrap()).await;
            ends.push((outcome.is_ok(), published.terminal));
        }
        assert_eq!(ends[0], ends[1], "{frames} frames: replay off, then on");
        completions += usize::from(ends[1].0);
    }
    assert!(completions > 0, "no pool size completed");
}

/// Only a successful `response.completed` caches: an incomplete response, under either event,
/// and a completion whose response failed, which the ingress ends as a failure, cache nothing.
#[tokio::test]
async fn only_a_successful_completion_caches() {
    let incomplete = turn_ending("response.incomplete", "incomplete");
    assert_eq!(replayed(incomplete).await, (true, false));
    let unfinished = turn_ending("response.completed", "incomplete");
    assert_eq!(replayed(unfinished).await, (true, false));
    let failed = turn_ending("response.completed", "failed");
    assert_eq!(replayed(failed).await, (false, false));
}
