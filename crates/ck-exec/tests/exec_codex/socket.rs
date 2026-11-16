//! A Responses WebSocket turn on Codex's upstream socket (`session::Turns`) against a loopback
//! Codex backend: one `response.create` per turn with the expanded history, the Codex frame
//! and handshake, steering and interrupt frames, each upstream response on its own
//! attempt stream, `error` frames and socket loss as attempt failures, and cancel.

use std::sync::Arc;

use ck_core::credential::Credential;
use ck_core::exec::{CaptureSink, FailureScope, UsageSink};
use ck_exec::codex::{Next, Successor, TurnControl};
use ck_exec::session::CancelTurn;
use ck_exec::types::Body;
use ck_stream::{Terminal, Writer};
use futures_util::StreamExt;
use serde_json::{Value, json};

use crate::codex_socket::*;
use crate::common::*;
use crate::support::{Exchange, Streams};

/// Every turn sends one `response.create` holding the request's expanded history, never a
/// `previous_response_id`, on the connection's one socket; each turn's response reaches its
/// own stream.
#[tokio::test]
async fn every_turn_sends_its_expanded_history_as_one_create() {
    let upstream = CodexSocket::start([act(answer("resp_1", 3)), act(answer("resp_2", 4))]).await;
    let (streams, executor) = (Streams::new(), executor());
    let exchange = Exchange::new(upstream.api_key(), MODEL, "/v1/responses");
    let assistant =
        json!({"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "hello"}]});
    let turns = [
        (request(json!([user("first")])), "resp_1", vec!["first"]),
        (
            json!({"model": "client-model", "previous_response_id": "resp_1", "input": [user("first"), assistant, user("second")]}).to_string(),
            "resp_2",
            vec!["first", "hello", "second"],
        ),
    ];
    for (body, id, _) in &turns {
        let (turn, response, _request) = start(&streams, &exchange, &executor, body).await;
        let (upstream, _control) = turn.expect("the turn starts");
        let (outcome, published) = produce(&streams, upstream, response).await;
        outcome.unwrap();
        assert_eq!(published.terminal, Some(Terminal::Done));
        assert!(text(&published).contains(id), "{}", text(&published));
    }
    let sent = upstream.sent();
    assert_eq!(sent.len(), 2, "one frame per turn");
    for (frame, (_, _, history)) in sent.iter().zip(&turns) {
        assert_eq!(frame["type"], "response.create", "{frame}");
        assert!(frame.get("previous_response_id").is_none(), "{frame}");
        let texts: Vec<_> = frame["input"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["content"][0]["text"].as_str().unwrap_or_default())
            .collect();
        assert_eq!(&texts, history, "{frame}");
    }
    assert_eq!(upstream.upgrades().len(), 1, "both turns share the connection's socket");
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Kind {
    ApiKey,
    OAuth,
    Siwc,
}

impl Kind {
    fn of(self, upstream: &CodexSocket) -> Credential {
        match self {
            Self::ApiKey => upstream.api_key(),
            Self::OAuth => upstream.oauth(),
            Self::Siwc => upstream.siwc(),
        }
    }
}

/// A turn's handshake carries Codex's OAuth identity for an OAuth credential and
/// `User-Agent: cooker` for an API key (Q13); its frame follows the Sign in with ChatGPT
/// WebSocket rules for such a credential.
#[tokio::test]
async fn a_turn_sends_the_codex_frame_and_handshake() {
    let body = request(json!([user("hi")]));
    for kind in [Kind::ApiKey, Kind::OAuth, Kind::Siwc] {
        let oauth = kind == Kind::OAuth;
        let upstream = CodexSocket::start([act(answer("resp_1", 3))]).await;
        let credential = kind.of(&upstream);
        let mut exchange = Exchange::new(credential, MODEL, "/v1/responses");
        for (name, value) in [("version", "0.50.0"), ("x-client-request-id", "req-1")] {
            exchange.headers.insert(name, http::HeaderValue::from_static(value));
        }
        let streams = Streams::new();
        let (turn, response, _request) = start(&streams, &exchange, &executor(), &body).await;
        produce(&streams, turn.unwrap().0, response).await.0.unwrap();

        let frames = upstream.sent();
        assert_eq!(frames.len(), 1, "{kind:?}");
        // The Sign in with ChatGPT WebSocket rules drop `stream`.
        assert_eq!(frames[0].get("stream").is_none(), kind == Kind::Siwc, "{kind:?}");
        let upgrades: Vec<_> = upstream.upgrades().iter().map(handshake).collect();
        let [new] = <[_; 1]>::try_from(upgrades).unwrap();
        let header = |name: &str| new.iter().find(|(n, _)| n == name).map(|(_, v)| v.as_str());
        match kind {
            Kind::OAuth => {
                assert_eq!(header("authorization"), Some("Bearer oauth-token"));
                assert_eq!(header("chatgpt-account-id"), Some("acct-1"));
                assert_eq!(header("originator"), Some("codex-tui"));
                assert!(
                    header("user-agent").is_some_and(|ua| ua.starts_with("codex-tui/")),
                    "{new:?}"
                );
                assert_eq!(header("openai-beta"), Some("responses_websockets=2026-02-06"));
            }
            Kind::ApiKey => {
                assert_eq!(header("authorization"), Some("Bearer sk-up"));
                assert_eq!(header("user-agent"), Some("cooker"));
            }
            Kind::Siwc => assert_eq!(header("authorization"), Some("Bearer siwc-at")),
        }
        assert!(oauth || header("chatgpt-account-id").is_none(), "{kind:?}");
    }
}

/// The upgrade's headers without those each dial picks anew.
fn handshake(headers: &http::HeaderMap) -> Vec<(String, String)> {
    let mut pairs: Vec<_> = headers
        .iter()
        .filter(|(name, _)| *name != "sec-websocket-key")
        .map(|(name, value)| (name.as_str().to_owned(), value.to_str().unwrap().to_owned()))
        .collect();
    pairs.sort();
    pairs
}

const STEER: &str = r#"{"type":"response.steer","previous_response_id":"r1","input":[{"role":"user","content":[{"type":"input_text","text":"STEER_OK"}]}]}"#;
const ACCEPTED: &str = r#"{"type":"response.steer.accepted","steer":{"id":"s1","previous_response_id":"r1"}}"#;
const IDLE_STEER: &str = r#"{"type":"response.steer","previous_response_id":"missing","input":"recover me"}"#;
const FAILED: &str = r#"{"type":"response.steer.failed","steer":{"id":"s2","previous_response_id":"missing","input":"recover me"},"error":{"code":"response_not_found","message":"missing response"}}"#;
const INTERRUPT_R1: &str =
    r#"{"type":"response.interrupt","response_id":"r1","mode":"discard_partial_items","extension":"keep"}"#;

/// Steering reaches the turn's socket as the client sent it. The response the steer ends and
/// its automatic successor are separate attempt streams, each with its own usage; a frame
/// outside both comes back as received, and an interrupt for a finished response is not sent.
#[tokio::test]
async fn steering_reaches_the_socket_and_each_response_is_its_own_stream() {
    let mut first = answer("r1", 301);
    let last = first
        .pop()
        .unwrap()
        .replace(r#""type":"response.completed""#, r#""type":"response.incomplete""#);
    let mut steered = vec![ACCEPTED.to_owned()];
    steered.extend(first.into_iter().skip(1));
    steered.push(last);
    steered.extend(answer("r2", 707));
    let upstream = CodexSocket::start([act(vec![created("r1")]), act(steered), act(vec![FAILED.into()])]).await;
    let (streams, executor) = (Streams::new(), executor());
    let exchange = Exchange::new(upstream.api_key(), MODEL, "/v1/responses");
    let (turn, response, request) = start(&streams, &exchange, &executor, &request(json!([user("hi")]))).await;
    let (body, control) = turn.unwrap();
    control.steer(STEER.as_bytes()).await.unwrap();
    let (outcome, published) = produce(&streams, body, response).await;
    outcome.unwrap();
    let r1 = text(&published);
    assert!(r1.contains("response.steer.accepted") && r1.contains("301"), "{r1}");
    assert!(!r1.contains("707"), "{r1}");

    let Some(Next::Response(successor)) = control.next().await else {
        panic!("the successor follows");
    };
    let (opened, response) = open(successor, &streams, &exchange, &request);
    let (outcome, published) = produce(&streams, opened, response).await;
    outcome.unwrap();
    let r2 = text(&published);
    assert!(r2.contains(r#""r2""#) && r2.contains("707"), "{r2}");
    assert!(!r2.contains("301"), "{r2}");

    control.interrupt(INTERRUPT_R1.as_bytes()).await.unwrap();
    control.steer(IDLE_STEER.as_bytes()).await.unwrap();
    let Some(Next::Event(event)) = control.next().await else {
        panic!("the steering failure follows");
    };
    assert_eq!(event, FAILED.as_bytes());
    let frames = upstream.frames();
    assert_eq!(frames.len(), 3, "{frames:?}");
    assert_eq!((frames[1].as_str(), frames[2].as_str()), (STEER, IDLE_STEER));
    assert_eq!(upstream.upgrades().len(), 1);
}

/// Which control call a row makes.
#[derive(Debug, Clone, Copy)]
enum Control {
    Steer,
    Interrupt,
}

/// Steering and interrupt frames reach the turn's socket as the old path sends them: an
/// interrupt unchanged, a steer through the payload rules and, for a Sign in with ChatGPT
/// credential, without its `configuration_update` items.
#[tokio::test]
async fn control_frames_reach_the_socket_as_the_old_path_sends_them() {
    const OVERRIDE: &str = "requests:\n  payload:\n    override:\n      - models: [{name: \"*\", match: [{type: response.steer}]}]\n        params: {input: configured, type: response.create}\n";
    const SIWC_STEER: &str = r#"{"type":"response.steer","previous_response_id":"r1","temperature":1,"input":[{"type":"configuration_update","reasoning":{"effort":"high"}},{"type":"message","role":"user","content":"steer"}]}"#;
    // (credential, config, call, client frame, frame upstream receives)
    let rows = [
        (Kind::ApiKey, "", Control::Steer, STEER, STEER),
        (
            Kind::ApiKey,
            OVERRIDE,
            Control::Steer,
            r#"{"type":"response.steer","previous_response_id":"r1","input":"original"}"#,
            r#"{"type":"response.steer","previous_response_id":"r1","input":"configured"}"#,
        ),
        (
            Kind::Siwc,
            "",
            Control::Steer,
            SIWC_STEER,
            r#"{"type":"response.steer","previous_response_id":"r1","temperature":1,"input":[{"type":"message","role":"user","content":"steer"}]}"#,
        ),
        (Kind::OAuth, "", Control::Interrupt, INTERRUPT_R1, INTERRUPT_R1),
    ];
    for (kind, config, call, frame, sent) in rows {
        let upstream = CodexSocket::start([act(vec![created("r1")])]).await;
        let (streams, executor) = (Streams::new(), executor());
        let mut exchange = Exchange::new(kind.of(&upstream), MODEL, "/v1/responses");
        exchange.cfg = ck_core::config::Config::parse(config).unwrap();
        let (turn, _response, _request) = start(&streams, &exchange, &executor, &request(json!([user("hi")]))).await;
        let (mut body, control) = turn.unwrap();
        tokio::time::timeout(WAIT, body.body.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        match call {
            Control::Steer => control.steer(frame.as_bytes()).await.unwrap(),
            Control::Interrupt => control.interrupt(frame.as_bytes()).await.unwrap(),
        }
        upstream.received(2).await;
        assert_eq!(upstream.frames()[1], sent, "{kind:?} {call:?}");
    }
}

/// A turn reports the request-side facts and leaves response usage to the canonical stream
/// (contract G4). The quota its handshake or events carry reaches the snapshots, and the
/// capture records every frame each way, a steer and its answer included.
#[tokio::test]
async fn a_turn_reports_request_facts_quota_and_capture() {
    let limits = r#"{"type":"codex.rate_limits","rate_limits":{"allowed":true,"primary":{"used_percent":12.5,"window_minutes":300}}}"#;
    let mut events = answer("resp_1", 3);
    events.insert(1, limits.to_owned());
    // (headers of the 101 answer, the answer's frames)
    let rows = [
        (vec![("x-codex-primary-used-percent", "97")], answer("resp_1", 3)),
        (vec![], events),
    ];
    for (handshake, frames) in rows {
        let received = frames.len();
        let upstream = CodexSocket::start([act(frames), act(vec![FAILED.into()])]).await;
        upstream.answer_upgrades(handshake);
        let (streams, executor) = (Streams::new(), executor());
        let mut exchange = Exchange::new(upstream.api_key(), MODEL, "/v1/responses");
        let observed = Arc::new(crate::usage::Observed::default());
        exchange.usage = UsageSink::new(observed.clone()).with_capture(CaptureSink::new(observed.clone()));
        let (turn, response, _request) = start(&streams, &exchange, &executor, &request(json!([user("hi")]))).await;
        let (body, control) = turn.unwrap();
        produce(&streams, body, response).await.0.unwrap();
        control.steer(IDLE_STEER.as_bytes()).await.unwrap();
        assert!(matches!(control.next().await, Some(Next::Event(_))));
        let calls = observed.calls();
        assert!(calls.iter().any(|c| c.starts_with("request Codex {")), "{calls:?}");
        for call in [&format!("upstream_model {MODEL}"), "round_trip_started", "first_byte"] {
            assert!(calls.iter().any(|c| c == call), "{call} missing from {calls:?}");
        }
        assert!(!calls.iter().any(|c| c.starts_with("response_")), "{calls:?}");
        // The create, the handshake, each frame received, the steer and its answer.
        let captured = calls.iter().filter(|c| *c == "capture other").count();
        assert_eq!(captured, received + 4, "{calls:?}");
        let quota = executor.quota().snapshot(&exchange.credential.id);
        assert!(quota.is_some(), "the quota reaches the snapshots");
    }
}

/// A turn needs its connection: without an execution session it fails before dialing.
#[tokio::test]
async fn a_turn_without_its_connection_fails_before_dialing() {
    let upstream = CodexSocket::start([]).await;
    let (streams, executor) = (Streams::new(), executor());
    let exchange = Exchange::new(upstream.api_key(), MODEL, "/v1/responses");
    let (turn, _response, _request) =
        start_on(None, &streams, &exchange, &executor, &request(json!([user("hi")]))).await;
    let error = turn.err().expect("the turn fails");
    assert_eq!((error.status, error.scope), (500, FailureScope::Request), "{error}");
    assert!(upstream.upgrades().is_empty());
}

/// Every `error` frame of `codex_ws_errors.json` fails the turn's attempt, with Go's status,
/// body and retry hint where Go classifies it, and so do a failed response and the socket's
/// loss. Each turn dials and sends once, and the next turn dials a new socket.
#[tokio::test]
async fn error_frames_and_socket_loss_fail_the_attempt() {
    let fixture: Vec<Value> = serde_json::from_str(include_str!("../fixtures/codex_ws_errors.json")).unwrap();
    // (upstream act, status, scope, body and retry hint). A frame Go classifies keeps Go's
    // body, and its scope where Go makes it credential-wide; one Go leaves to the event rules
    // is an upstream failure without details.
    let mut rows: Vec<_> = fixture
        .iter()
        .filter(|case| case["model_level_cooling"] != true)
        .map(|case| {
            let frame = act(vec![case["payload"].as_str().unwrap().to_owned()]);
            if case["matched"] != true {
                return (frame, 502, None, None);
            }
            let status = u16::try_from(case["status"].as_u64().unwrap()).unwrap();
            let scope = (case["credential_scoped"] == true).then_some(FailureScope::Credential);
            let body = case["message"].as_str().unwrap().to_owned();
            (frame, status, scope, Some((body, case["retry_after_ms"].as_i64())))
        })
        .collect();
    assert_eq!(rows.len(), 14, "every fixture frame without model-level cooling");
    let failed = r#"{"type":"response.failed","response":{"id":"r1","status":"failed","error":{"code":"not_found","message":"gone"}}}"#;
    let loss = |end| act(vec![created("r1")]).then(end);
    rows.extend([
        (act(vec![created("r1"), failed.to_owned()]), 404, None, None),
        (loss(End::Close(1011)), 502, Some(FailureScope::Transport), None),
        (loss(End::Drop), 502, Some(FailureScope::Transport), None),
        (loss(End::Binary), 502, Some(FailureScope::Transport), None),
        (loss(End::Close(1009)), 413, Some(FailureScope::Request), None),
    ]);
    let upstream = CodexSocket::start(rows.iter().map(|row| row.0.clone())).await;
    let (streams, executor) = (Streams::new(), executor());
    let exchange = Exchange::new(upstream.api_key(), MODEL, "/v1/responses");
    let body = request(json!([user("hi")]));
    for (i, (_, status, scope, go)) in rows.iter().enumerate() {
        let (turn, response, _request) = start(&streams, &exchange, &executor, &body).await;
        let (outcome, _) = produce(&streams, turn.expect("the turn starts").0, response).await;
        let error = outcome.expect_err("the attempt fails");
        assert_eq!(error.status, *status, "row {i}: {error}");
        if let Some(scope) = scope {
            assert_eq!(error.scope, *scope, "row {i}: {error}");
        }
        if let Some((message, retry)) = go {
            assert_eq!(String::from_utf8_lossy(&error.body), *message, "row {i}");
            assert_eq!(error.retry_after.map(|d| d.as_millis() as i64), *retry, "row {i}");
        }
        assert_eq!(
            (upstream.frames().len(), upstream.upgrades().len()),
            (i + 1, i + 1),
            "row {i}"
        );
    }
}

/// How a test ends a turn while its response may be in flight.
#[derive(Debug, Clone, Copy)]
enum Ending {
    Cancel,
    DropBody,
}

/// Cancel sends `response.interrupt` for the response in flight and closes the socket; before
/// `response.created` it only closes. A body dropped in flight closes it too. A turn whose
/// response finished leaves the socket to the next turn.
#[tokio::test]
async fn ending_a_turn_in_flight_closes_its_socket() {
    const INTERRUPT: &str = r#"{"type":"response.interrupt","response_id":"r1","mode":"discard_partial_items"}"#;
    // (frames before the ending, read until, ending, frame sent after, socket closed)
    let rows = [
        (vec![], 0, Ending::Cancel, None, true),
        (vec![created("r1")], 1, Ending::Cancel, Some(INTERRUPT), true),
        (vec![created("r1")], 1, Ending::DropBody, None, true),
        (answer("r1", 3), usize::MAX, Ending::Cancel, None, false),
    ];
    for (send, read, ending, after, closed) in rows {
        let mut acts = vec![act(send)];
        acts.extend(after.map(|_| Act::default()));
        acts.push(act(answer("r2", 3)));
        let upstream = CodexSocket::start(acts).await;
        let (streams, executor) = (Streams::new(), executor());
        let exchange = Exchange::new(upstream.api_key(), MODEL, "/v1/responses");
        let body = request(json!([user("hi")]));
        let (turn, _response, _request) = start(&streams, &exchange, &executor, &body).await;
        let (upstream_body, control) = turn.unwrap();
        upstream.received(1).await;
        let mut frames = upstream_body.body;
        for _ in 0..read {
            match tokio::time::timeout(WAIT, frames.next()).await.unwrap() {
                Some(chunk) => drop(chunk.unwrap()),
                None => break,
            }
        }
        // A cancelled turn closes its socket while the body is still held.
        let held = match ending {
            Ending::Cancel => {
                control.cancel().await;
                Some(frames)
            }
            Ending::DropBody => {
                drop((control, frames));
                None
            }
        };
        if let Some(after) = after {
            upstream.received(2).await;
            assert_eq!(upstream.frames()[1], after, "{ending:?}");
        }
        if closed {
            upstream.ended(1).await;
        }
        // The turn holds the connection until its body goes too.
        drop(held);
        let (turn, response, _request) = start(&streams, &exchange, &executor, &body).await;
        produce(&streams, turn.unwrap().0, response).await.0.unwrap();
        let dials = if closed { 2 } else { 1 };
        assert_eq!(upstream.upgrades().len(), dials, "{ending:?} after {read:?}");
        assert_eq!(upstream.frames().len(), 2 + usize::from(after.is_some()), "{ending:?}");
    }
}

/// How far a test reads before it cancels a steered turn.
#[derive(Debug, Clone, Copy)]
enum Progress {
    Nothing,
    /// The turn's body to its end.
    Body,
    /// Then one [`Next::Event`].
    Event,
    /// Then the successor from [`TurnControl::next`], held unopened.
    Successor,
    /// Then the successor's body, opened and read this many frames.
    Output(usize),
}

/// Cancel interrupts the response live on the socket, whichever it is when cancel runs: the
/// turn's own, or the successor accepted steering started, whether or not the turn has read its
/// `response.created`. The upstream sees one interrupt for a live response and none when none
/// is live, and the socket closes, since a successor may still be on its way.
#[tokio::test]
async fn cancel_interrupts_the_response_live_on_the_socket() {
    let interrupt =
        |id: &str| format!(r#"{{"type":"response.interrupt","response_id":"{id}","mode":"discard_partial_items"}}"#);
    let mut r1 = answer("r1", 3);
    let end = r1
        .pop()
        .unwrap()
        .replace(r#""type":"response.completed""#, r#""type":"response.incomplete""#);
    r1.push(end);
    let (r1c, r1_rest) = (r1[0].clone(), r1[1..].to_vec());
    let r2 = answer("r2", 7);
    // r1's rest, then the steering acknowledgement, then the first `upto` frames of r2.
    let steered = |upto: usize| {
        let mut frames = r1_rest.clone();
        frames.push(ACCEPTED.to_owned());
        frames.extend(r2.iter().take(upto).cloned());
        frames
    };
    // (frames answering the create, frames answering the steer (none: no steer), progress,
    // the response cancel interrupts)
    let rows = [
        (vec![], None, Progress::Nothing, None),
        (
            vec![r1c.clone()],
            Some(vec![ACCEPTED.into()]),
            Progress::Nothing,
            Some("r1"),
        ),
        (vec![r1c.clone()], Some(steered(0)), Progress::Body, None),
        (vec![r1c.clone()], Some(steered(1)), Progress::Body, Some("r2")),
        (vec![r1c.clone()], Some(steered(1)), Progress::Event, Some("r2")),
        (vec![r1c.clone()], Some(steered(1)), Progress::Successor, Some("r2")),
        (vec![r1c.clone()], Some(steered(3)), Progress::Output(2), Some("r2")),
        (vec![r1c], Some(steered(r2.len())), Progress::Output(usize::MAX), None),
    ];
    for (i, (create, steer, progress, live)) in rows.into_iter().enumerate() {
        let steering = steer.is_some();
        let mut acts = vec![act(create).then(End::Sync)];
        acts.extend(steer.map(|frames| act(frames).then(End::Sync)));
        let upstream = CodexSocket::start(acts).await;
        let (streams, executor) = (Streams::new(), executor());
        let exchange = Exchange::new(upstream.api_key(), MODEL, "/v1/responses");
        let (turn, _response, request) = start(&streams, &exchange, &executor, &request(json!([user("hi")]))).await;
        let (upstream_body, control) = turn.unwrap();
        upstream.synced(1).await;
        if steering {
            control.steer(STEER.as_bytes()).await.unwrap();
            upstream.synced(2).await;
        }
        let mut body = upstream_body.body;
        let held = read_to(progress, &mut body, &control, (&streams, &exchange, &request)).await;
        control.cancel().await;
        upstream.ended(1).await;
        let mut interrupts = upstream.frames();
        interrupts.retain(|frame| frame.contains(r#""type":"response.interrupt""#));
        assert_eq!(interrupts, Vec::from_iter(live.map(interrupt)), "row {i}: {progress:?}");
        drop((body, held));
    }
}

/// Reads a steered turn as far as `progress` goes. What it returns is held across cancel:
/// dropping a successor or its body would close the socket on its own.
async fn read_to(
    progress: Progress,
    body: &mut Body,
    control: &TurnControl,
    (streams, exchange, request): (&Streams, &Exchange, &Writer),
) -> (Option<Successor>, Option<(Body, Writer)>) {
    if !matches!(progress, Progress::Nothing) {
        while tokio::time::timeout(WAIT, body.next()).await.unwrap().is_some() {}
    }
    if matches!(progress, Progress::Nothing | Progress::Body) {
        return (None, None);
    }
    let Some(Next::Event(event)) = control.next().await else {
        panic!("{progress:?}: the acknowledgement follows");
    };
    assert_eq!(event, ACCEPTED.as_bytes(), "{progress:?}");
    if let Progress::Event = progress {
        return (None, None);
    }
    let Some(Next::Response(successor)) = control.next().await else {
        panic!("{progress:?}: the successor follows");
    };
    let Progress::Output(frames) = progress else {
        return (Some(successor), None);
    };
    let (opened, response) = open(successor, streams, exchange, request);
    let mut opened = opened.body;
    for _ in 0..frames {
        if tokio::time::timeout(WAIT, opened.next()).await.unwrap().is_none() {
            break;
        }
    }
    (None, Some((opened, response)))
}
