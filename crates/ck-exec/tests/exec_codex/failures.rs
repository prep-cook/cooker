//! Failures before and after the Codex upstream answered: refusals, in-stream failure events,
//! an empty incomplete response and a cut body.

use ck_core::exec::FailureScope;
use ck_stream::Terminal;
use ck_translate::codec::Operation;

use crate::common::*;
use crate::support::Streams;
use crate::support::upstream::{Reply, StreamUpstream};

fn errors(records: &[String]) -> Vec<&String> {
    records.iter().filter(|r| r.contains("event: x_ck.error")).collect()
}

/// A refusal fails the attempt before commitment with Go's classification: a usage limit is
/// the credential's 429 with its reset as the retry hint, and nothing reaches the stream.
#[tokio::test]
async fn upstream_refusals_fail_the_attempt_before_commitment() {
    let limit = r#"{"error":{"type":"usage_limit_reached","message":"limit","resets_in_seconds":60}}"#;
    for (reply, status, scope) in [
        (
            Reply::status(429, "application/json", limit),
            429,
            FailureScope::Credential,
        ),
        (
            Reply::status(400, "application/json", r#"{"error":{"message":"bad"}}"#),
            400,
            FailureScope::Request,
        ),
    ] {
        let upstream = StreamUpstream::start([reply]).await;
        let (streams, exchange) = (Streams::new(), exchange(&upstream));
        let (started, id, _request) = run(&streams, &exchange, "chat", Operation::Generate, CHAT, 64).await;
        let error = started.err().expect("the attempt fails");
        assert_eq!((error.status, error.scope), (status, scope));
        if status == 429 {
            assert!(error.retry_after.is_some());
        }
        assert!(
            streams.store.reader(id, 0).is_err(),
            "the attempt's stream is discarded"
        );
        assert_eq!(upstream.requests(), 1);
    }
}

/// A failure event after the upstream answered ends the stream with one `error` carrying Go's
/// status, and the attempt fails with Go's scope: an in-stream usage limit is the credential's
/// 429, not a gateway 502.
#[tokio::test]
async fn a_failure_event_fails_with_the_codex_classification() {
    let failed = event(
        "response.failed",
        r#"{"type":"response.failed","sequence_number":4,"response":{"id":"resp_1","status":"failed","error":{"type":"usage_limit_reached","message":"limit","resets_in_seconds":60}}}"#,
    );
    let body = message_events()
        .split("event: response.output_text.done")
        .next()
        .unwrap()
        .to_owned()
        + &failed;
    let upstream = StreamUpstream::start([Reply::sse(body)]).await;
    let (streams, exchange) = (Streams::new(), exchange(&upstream));
    let (started, id, _request) = run(&streams, &exchange, "responses", Operation::Generate, RESPONSES, 64).await;
    let (outcome, published, _stream) = finish(&streams, started.unwrap(), id).await;
    let error = outcome.unwrap_err();
    assert_eq!((error.status, error.scope), (429, FailureScope::Credential));
    assert_eq!(published.terminal, Some(Terminal::UpstreamError));
    let records = records(&published);
    let errors = errors(&records);
    assert_eq!(errors.len(), 1, "{records:?}");
    assert!(errors[0].contains(r#""status":429"#), "{records:?}");
    assert!(records.iter().any(|r| r.contains("hel")), "{records:?}");
}

/// A `response.incomplete` with no output at all is Go's request-scoped 502.
#[tokio::test]
async fn an_empty_incomplete_response_fails() {
    let body = event(
        "response.created",
        r#"{"type":"response.created","response":{"id":"resp_1","object":"response","created_at":1,"status":"in_progress","model":"gpt-5","output":[]}}"#,
    ) + &event(
        "response.incomplete",
        r#"{"type":"response.incomplete","response":{"id":"resp_1","object":"response","created_at":1,"status":"incomplete","model":"gpt-5","output":[],"usage":{"input_tokens":3,"output_tokens":0,"total_tokens":3}}}"#,
    );
    let upstream = StreamUpstream::start([Reply::sse(body)]).await;
    let (streams, exchange) = (Streams::new(), exchange(&upstream));
    let (started, id, _request) = run(&streams, &exchange, "chat", Operation::Generate, CHAT, 64).await;
    let (outcome, published, _stream) = finish(&streams, started.unwrap(), id).await;
    let error = outcome.unwrap_err();
    assert_eq!((error.status, error.scope), (502, FailureScope::Request));
    assert_eq!(errors(&records(&published)).len(), 1);
}

/// A body cut after the upstream answered ends the stream with exactly one `error`, after the
/// parts already published.
#[tokio::test]
async fn a_cut_body_ends_with_one_error() {
    let body = sse();
    let cut = body.find(r#""delta":"lo""#).unwrap() + 16;
    for reply in [
        Reply::sse(body.clone()).cut_after(cut),
        Reply::sse(body).cut_after(cut).byte_at_a_time(),
    ] {
        let upstream = StreamUpstream::start([reply]).await;
        let (streams, exchange) = (Streams::new(), exchange(&upstream));
        let (started, id, _request) = run(&streams, &exchange, "anthropic", Operation::Generate, ANTHROPIC, 64).await;
        let (outcome, published, _stream) = finish(&streams, started.unwrap(), id).await;
        assert!(outcome.is_err());
        assert_eq!(published.terminal, Some(Terminal::UpstreamError));
        let records = records(&published);
        assert_eq!(errors(&records).len(), 1, "{records:?}");
        assert!(records.last().unwrap().contains("event: x_ck.error"), "{records:?}");
        assert!(records.iter().any(|r| r.contains("lo")), "{records:?}");
    }
}
