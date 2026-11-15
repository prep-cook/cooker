//! Failures before the upstream answered (attempt failures with Go's classification and the
//! upstream body as received) and after it (exactly one `error` record).

use super::*;

const RESPONSES: &str = r#"{"model":"grok-4.3","stream":true,"input":"hi"}"#;

/// The producer's outcome and records for `reply` to a Responses client.
async fn after(reply: Reply) -> (Result<(), ExecError>, Published) {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([reply]).await;
    let run = run(&streams, &upstream, Client::Responses, RESPONSES, 64).await;
    let (outcome, published, _) = produce(&streams, run.started.unwrap()).await;
    (outcome, published)
}

fn assert_one_error(published: &Published) {
    assert_eq!(published.terminal, Some(Terminal::UpstreamError));
    let records = records(published);
    assert_eq!(errors(published).len(), 1, "{records:?}");
    assert!(records.last().unwrap().contains("event: x_ck.error"), "{records:?}");
}

/// A body cut mid-stream ends with exactly one `error`, after the text already published.
#[tokio::test]
async fn a_cut_body_ends_with_one_error() {
    let body = sse();
    let cut = body.find("\"lo\"").unwrap() + 4;
    let (outcome, published) = after(Reply::sse(body).cut_after(cut)).await;
    assert!(outcome.is_err());
    assert_one_error(&published);
    assert!(records(&published).iter().any(|r| r.contains("hel")));
}

/// A body that ends before the completion fails as Go's disconnect: 408, credential scope.
#[tokio::test]
async fn eof_before_completion_fails_like_go() {
    let (outcome, published) = after(Reply::sse(message_events())).await;
    let error = outcome.unwrap_err();
    assert_eq!((error.status, error.scope), (408, FailureScope::Credential));
    assert_one_error(&published);
}

/// The completion boundary is the end of the completion's SSE event: a completion without its
/// blank line is a disconnect, whatever its JSON says.
#[tokio::test]
async fn a_completion_without_its_event_end_does_not_complete() {
    let body = sse();
    let (outcome, published) = after(Reply::sse(body.trim_end().to_owned())).await;
    assert_eq!(outcome.unwrap_err().status, 408);
    assert_one_error(&published);
}

/// Invalid UTF-8 from the upstream is refused, never repaired.
#[tokio::test]
async fn invalid_utf8_is_refused() {
    let mut body = sse().into_bytes();
    let at = body.windows(5).position(|w| w == b"\"lo\"}").unwrap() + 1;
    body[at] = 0xff;
    let (outcome, published) = after(Reply::sse(body)).await;
    assert!(outcome.is_err());
    assert_one_error(&published);
    assert!(!records(&published).iter().any(|r| r.contains('\u{fffd}')));
}

/// An event line that is not UTF-8 is not repaired before the ingress sees it.
#[tokio::test]
async fn an_invalid_event_name_is_not_repaired() {
    // An event the ingress names only by its SSE name, since its data has no `type`.
    let body = message_events() + "event: x_note.\u{1}\ndata: {\"note\":1}\n\n";
    let mut body = body.into_bytes();
    let at = body.windows(1).position(|w| w == [1]).unwrap();
    body[at] = 0xff;
    let (_, published) = after(Reply::sse(body)).await;
    assert!(
        !records(&published).iter().any(|r| r.contains('\u{fffd}')),
        "{:?}",
        records(&published)
    );
}

/// Refusals are attempt failures with `xaiStatusErr`'s status and retry hint, the upstream's
/// body as received, and the response stream discarded.
#[tokio::test]
async fn upstream_refusals_fail_the_attempt_before_commitment() {
    let bad = r#"{"code":"bad-credentials","error":"The access token could not be validated"}"#;
    let free = r#"{"code":"free-usage-exhausted","error":"You have used your included free usage"}"#;
    let invalid: &[u8] = b"{\"error\":\"\xff\"}";
    for (status, body, want, retry) in [
        (403, bad.as_bytes(), 401, None),
        (429, free.as_bytes(), 429, Some(86_400)),
        (403, b"{\"error\":\"forbidden model\"}".as_slice(), 403, None),
        (500, b"".as_slice(), 500, None),
        (400, invalid, 400, None),
    ] {
        let streams = Streams::new();
        let upstream = StreamUpstream::start([Reply::status(status, "application/json", body.to_vec())]).await;
        let run = run(&streams, &upstream, Client::Responses, RESPONSES, 64).await;
        let error = run.started.err().expect("a refusal fails the attempt");
        assert_eq!(error.status, want);
        assert_eq!(&error.body[..], body, "the body as received");
        assert_eq!(error.retry_after.map(|d| d.as_secs()), retry);
    }
}
