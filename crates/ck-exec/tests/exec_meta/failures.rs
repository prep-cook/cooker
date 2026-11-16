//! Failures before and after the Meta upstream answered, and the request-side usage facts.

use std::sync::{Arc, Mutex};

use ck_core::exec::{CaptureEvent, CaptureObserver, CaptureSink, FailureScope, UsageObserver, UsageSink};
use ck_core::format::Format;
use ck_stream::Terminal;
use ck_translate::codec::Operation;

use crate::common::*;
use crate::support::Streams;
use crate::support::upstream::{Reply, StreamUpstream};

/// A refusal fails the attempt before commitment with Meta's classification, the upstream body
/// and headers kept as received: a subscription quota is the credential's 429, a 404 cools the
/// model for five minutes, and nothing reaches the stream.
#[tokio::test]
async fn upstream_refusals_fail_the_attempt_before_commitment() {
    let quota = r#"{"error":{"code":"rate_limit_exceeded","message":"Subscription quota exhausted"}}"#;
    let missing = r#"{"error":{"message":"no such model"}}"#;
    for (body, status, scope, retry) in [
        (quota, 429, FailureScope::Credential, None),
        (missing, 404, FailureScope::Model, Some(300)),
        (r#"{"error":{"message":"bad"}}"#, 400, FailureScope::Request, None),
    ] {
        let reply = Reply::status(status, "application/json", body).header("x-request-id", "r1");
        let upstream = StreamUpstream::start([reply]).await;
        let (streams, exchange) = (Streams::new(), exchange(&upstream));
        let (started, id, _request) = run(&streams, &exchange, "chat", Operation::Generate, CHAT, 64).await;
        let error = started.err().expect("the attempt fails");
        assert_eq!((error.status, error.scope), (status, scope));
        assert_eq!(error.retry_after.map(|d| d.as_secs()), retry);
        assert_eq!(&error.body[..], body.as_bytes());
        assert_eq!(error.headers.get("x-request-id").unwrap(), "r1");
        // The attempt's stream ends for the server to discard after its usage reader.
        let mut reader = streams.store.reader(id, 0).unwrap();
        assert!(matches!(
            reader.next().await,
            Some(ck_stream::Read::Terminal(Terminal::UpstreamError))
        ));
    }
}

/// A failure event after the upstream answered ends the stream with one `error` after the text
/// already published, with Meta's status (`error.code` when it is an HTTP status, read from
/// `response.error` in `response.failed`, else 502) and scope. A `response.failed` publishes its
/// usage before the `error`, whatever its member order, and none of its upstream detail.
#[tokio::test]
async fn a_failure_event_fails_with_the_meta_classification() {
    let partial = message_events()
        .split("event: response.output_item.done")
        .next()
        .unwrap()
        .to_owned();
    let reordered = r#"{"response":{"output":[{"id":"msg_1","type":"message","status":"incomplete","role":"assistant","content":[{"type":"output_text","text":"hel","annotations":[]}]}],"usage":{"input_tokens":7,"output_tokens":2,"total_tokens":9},"error":{"code":"server_error","message":"internal https://vendor.invalid/x"},"status":"failed","model":"muse-spark","id":"resp_1","object":"response","created_at":1},"sequence_number":9,"type":"response.failed"}"#;
    let nested = r#"{"type":"response.failed","response":{"id":"resp_1","status":"failed","error":{"code":429,"message":"slow down"},"usage":{"input_tokens":7,"output_tokens":0,"total_tokens":7}}}"#;
    for (failure, status, scope, usage) in [
        (
            r#"{"type":"error","error":{"code":429,"message":"slow down"}}"#,
            429,
            FailureScope::Model,
            None,
        ),
        (
            r#"{"type":"response.failed","response":{"id":"resp_1","status":"failed"},"error":{"code":"server_error"}}"#,
            502,
            FailureScope::Credential,
            None,
        ),
        (reordered, 502, FailureScope::Credential, Some(r#""input_tokens":7"#)),
        (nested, 429, FailureScope::Model, Some(r#""input_tokens":7"#)),
    ] {
        let (outcome, terminal, records) = chat_on((partial.clone() + &event("x", failure)).into_bytes()).await;
        let error = outcome.unwrap_err();
        assert_eq!((error.status, error.scope), (status, scope), "{failure}");
        assert_eq!(&error.body[..], failure.as_bytes());
        assert_eq!(terminal, Some(Terminal::UpstreamError));
        assert_eq!(errors(&records), 1, "{records:?}");
        let error_record = records.iter().position(|r| r.contains("event: x_ck.error")).unwrap();
        assert!(
            records[error_record].contains(&format!(r#""status":{status}"#)),
            "{records:?}"
        );
        assert!(records.iter().any(|r| r.contains("hel")), "{records:?}");
        assert!(!records.iter().any(|r| r.contains("vendor.invalid")), "{records:?}");
        let usage_record = records.iter().position(|r| r.contains("event: x_ck.usage"));
        match usage {
            Some(count) => {
                let at = usage_record.unwrap_or_else(|| panic!("no usage: {records:?}"));
                assert!(records[at].contains(count), "{records:?}");
                assert!(at < error_record, "{records:?}");
            }
            None => assert_eq!(usage_record, None, "{records:?}"),
        }
    }
}

/// A body that ends before `response.completed` or `response.incomplete` is Meta's disconnect,
/// a credential-scoped 408; `response.done` is not Meta's completion.
#[tokio::test]
async fn an_end_before_completion_is_the_meta_disconnect() {
    let done = event(
        "response.done",
        r#"{"type":"response.done","response":{"id":"resp_1","object":"response","created_at":1,"status":"completed","output":[]}}"#,
    );
    for body in [
        message_events(),
        message_events() + &done,
        sse().strip_suffix('\n').unwrap().to_owned(),
    ] {
        let (outcome, terminal, records) = chat_on(body.into_bytes()).await;
        let error = outcome.unwrap_err();
        assert_eq!(
            (error.status, error.scope),
            (408, FailureScope::Credential),
            "{records:?}"
        );
        assert_eq!(&error.body[..], ck_exec::meta::DISCONNECTED.as_bytes());
        assert_eq!(terminal, Some(Terminal::UpstreamError));
        assert_eq!(errors(&records), 1, "{records:?}");
    }
}

/// A body cut mid-stream ends with exactly one `error`, after the parts already published.
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
        assert_eq!(errors(&records), 1, "{records:?}");
        assert!(records.last().unwrap().contains("event: x_ck.error"), "{records:?}");
        assert!(records.iter().any(|r| r.contains("lo")), "{records:?}");
    }
}

/// Invalid UTF-8 in an upstream event is an upstream failure, never repaired text.
#[tokio::test]
async fn invalid_utf8_fails_instead_of_being_repaired() {
    let text = sse();
    let at = text.find(r#""delta":"lo""#).unwrap() + 10;
    let mut body = text.into_bytes();
    body[at] = 0xff;
    let (outcome, terminal, records) = chat_on(body).await;
    assert_eq!(outcome.unwrap_err().status, 502);
    assert_eq!(terminal, Some(Terminal::UpstreamError));
    assert_eq!(errors(&records), 1, "{records:?}");
    assert!(records.iter().all(|r| !r.contains('\u{fffd}')), "{records:?}");
}

/// A line past Go's 50 MiB scanner limit fails as Meta's scanner error, after the parts already
/// published.
#[tokio::test]
async fn a_line_past_the_scanner_limit_fails() {
    let mut body = message_events().into_bytes();
    body.extend(std::iter::repeat_n(b'x', 52_428_801));
    let (outcome, terminal, records) = chat_on(body).await;
    let error = outcome.unwrap_err();
    assert_eq!((error.status, error.scope), (500, FailureScope::Request));
    assert_eq!(&error.body[..], b"bufio.Scanner: token too long");
    assert_eq!(terminal, Some(Terminal::UpstreamError));
    assert_eq!(errors(&records), 1);
}

/// The canonical stream does not depend on how the upstream body was split.
#[tokio::test]
async fn any_split_gives_the_same_response() {
    let mut seen = Vec::new();
    for reply in [
        Reply::sse(sse()),
        Reply::sse(sse()).byte_at_a_time(),
        Reply::sse(sse().replace('\n', "\r\n")).byte_at_a_time(),
    ] {
        let upstream = StreamUpstream::start([reply]).await;
        let (streams, exchange) = (Streams::new(), exchange(&upstream));
        let (started, id, _request) = run(&streams, &exchange, "chat", Operation::Generate, CHAT, 1).await;
        let (outcome, published, _stream) = finish(&streams, started.unwrap(), id).await;
        outcome.unwrap();
        seen.push(records(&published));
    }
    assert!(seen.iter().all(|records| *records == seen[0]), "{seen:#?}");
}

/// Every usage and capture call, by name.
#[derive(Default)]
struct Observed(Mutex<Vec<String>>);

impl Observed {
    fn push(&self, call: String) {
        self.0.lock().unwrap().push(call);
    }
}

impl UsageObserver for Observed {
    fn response_body(&self, _: Format, _: &[u8]) {
        self.push("response_body".into());
    }
    fn response_line(&self, _: Format, _: &[u8]) {
        self.push("response_line".into());
    }
    fn response_usage_line(&self, _: Format, _: &[u8]) {
        self.push("response_usage_line".into());
    }
    fn response_model(&self, _: &str) {
        self.push("response_model".into());
    }
    fn request(&self, format: Format, payload: &[u8]) {
        self.push(format!("request {format:?} {}", String::from_utf8_lossy(payload)));
    }
    fn round_trip_started(&self) {
        self.push("round_trip_started".into());
    }
    fn first_byte(&self) {
        self.push("first_byte".into());
    }
    fn token_event(&self, is_token: bool) {
        self.push(format!("token_event {is_token}"));
    }
}

impl CaptureObserver for Observed {
    fn record(&self, event: CaptureEvent<'_>) {
        self.push(match event {
            CaptureEvent::Request(info) => format!("capture request {}", info.url),
            CaptureEvent::ResponseChunk(_) => "capture chunk".into(),
            CaptureEvent::ResponseError(text) => format!("capture error {text}"),
            _ => "capture other".into(),
        });
    }
}

/// Request-side facts and the captured exchange reach the usage sink (contract G4), never a
/// `response_*` call. As on the old path, a streaming client's first byte is only the round
/// trip's packet, and a buffered client's is its first byte.
#[tokio::test]
async fn request_side_facts_reach_the_usage_sink_and_response_usage_does_not() {
    let buffered = br#"{"model":"client-model","input":"hi"}"#;
    for (body, first) in [(RESPONSES, "token_event false"), (&buffered[..], "first_byte")] {
        let upstream = StreamUpstream::start([Reply::sse(sse())]).await;
        let streams = Streams::new();
        let mut exchange = exchange(&upstream);
        let observed = Arc::new(Observed::default());
        exchange.usage = UsageSink::new(observed.clone()).with_capture(CaptureSink::new(observed.clone()));
        let (started, id, _request) = run(&streams, &exchange, "responses", Operation::Generate, body, 64).await;
        finish(&streams, started.unwrap(), id).await.0.unwrap();
        let calls = observed.0.lock().unwrap().clone();
        let wire = String::from_utf8(upstream.captured()[0].body.clone()).unwrap();
        for call in [
            format!("request Codex {wire}"),
            "round_trip_started".into(),
            first.into(),
            format!("capture request {}/responses", upstream.url()),
            "capture chunk".into(),
        ] {
            assert!(calls.contains(&call), "{call} missing from {calls:?}");
        }
        assert!(!calls.iter().any(|c| c.starts_with("response_")), "{calls:?}");
    }
}
