//! URLs in request logs: every query value and fragment masked and the userinfo dropped
//! (owner ruling Q18), in the downstream URL, upstream HTTP and WebSocket URLs, captured
//! error text and URL-bearing headers.

use super::tests::{persisted, persisted_request, upstream_request};
use axum::http::StatusCode;
use ck_core::exec::CaptureEvent::*;

const SECRET: &str = "sk-review-secret";

/// Asserts that `content` holds `want` as a whole line.
fn line(content: &str, want: &str) {
    assert!(content.lines().any(|l| l == want), "no line {want:?} in {content}");
}

#[tokio::test]
async fn upstream_url_credentials_are_redacted_in_logs() {
    let content = persisted(true, StatusCode::OK, |log| {
        let sink = log.capture_sink();
        sink.record(Request(upstream_request(
            "https://alice:pw-h1dden@proxy.example/v1/models?alt=sse&key=AIza-h1dden-query-123&access_token=tok-x-h1dden-x-456#frag",
            &[],
            b"",
        )));
        sink.record(WebsocketRequest(upstream_request(
            "wss://bob:pw2-h1dden@ws.example/realtime?client_secret=cs-xx-h1dden-xx-789&model=gpt",
            &[],
            b"",
        )));
    })
    .await;
    let content = String::from_utf8(content).unwrap();
    assert!(!content.contains("h1dden"), "{content}");
    assert!(!content.contains("alice") && !content.contains("bob"), "{content}");
    line(
        &content,
        "Upstream URL: https://proxy.example/v1/models?alt=***&key=***&access_token=***#***",
    );
    line(
        &content,
        "Upstream URL: wss://ws.example/realtime?client_secret=***&model=***",
    );
}

/// Review of #127, finding 2: names that no list calls sensitive, bare entries and
/// fragments, downstream and upstream.
#[tokio::test]
async fn every_query_value_and_fragment_is_masked_in_logs() {
    let (content, _) = persisted_request(
        true,
        StatusCode::OK,
        "/fixture?credential=sk-review-secret&sk-review-secret",
        &[],
        |log| {
            let sink = log.capture_sink();
            for url in [
                "http://127.0.0.1/v1?credential=sk-review-secret",
                "http://127.0.0.1/v1?sk-review-secret=",
                "http://127.0.0.1/v1#sk-review-secret",
                "ftp://127.0.0.1?key=prefix@sk-review-secret",
            ] {
                sink.record(Request(upstream_request(url, &[], b"")));
            }
            sink.record(WebsocketRequest(upstream_request(
                "ws://127.0.0.1/r?credential=sk-review-secret#sk-review-secret",
                &[],
                b"",
            )));
        },
    )
    .await;
    let content = String::from_utf8(content).unwrap();
    assert!(!content.contains(SECRET), "{content}");
    line(&content, "URL: /fixture?credential=***&***");
    for want in [
        "http://127.0.0.1/v1?credential=***\n",
        "http://127.0.0.1/v1?***\n",
        "http://127.0.0.1/v1#***\n",
        "<redacted url>\n",
        "ws://127.0.0.1/r?credential=***#***\n",
    ] {
        assert!(content.contains(&format!("Upstream URL: {want}")), "{want}: {content}");
    }
}

/// Review of #127, finding 3: the logged copies of URL-bearing headers are redacted, in
/// the request log and in the forced error log; the client receives them unchanged.
#[tokio::test]
async fn url_headers_are_redacted_in_logs_only() {
    const HEADERS: &[(&str, &str)] = &[
        ("location", "http://127.0.0.1/v1?key=sk-review-secret"),
        ("content-location", "/v1/r?token=sk-review-secret"),
        ("referer", "https://u:sk-review-secret@127.0.0.1/cb#sk-review-secret"),
        ("link", "<http://127.0.0.1/p?key=sk-review-secret>; rel=\"next\""),
    ];
    for enabled in [true, false] {
        let (content, received) = persisted_request(enabled, StatusCode::BAD_GATEWAY, "/fixture", HEADERS, |log| {
            let sink = log.capture_sink();
            let headers: Vec<_> = HEADERS
                .iter()
                .map(|(n, v)| (super::header_name(n), (*v).to_owned()))
                .collect();
            sink.record(Request(upstream_request("http://127.0.0.1/v1", &headers, b"")));
            sink.record(ResponseMetadata(302, &headers));
            sink.record(ResponseChunk(b"moved"));
        })
        .await;
        let content = String::from_utf8(content).unwrap();
        assert!(!content.contains(SECRET), "{content}");
        // Downstream request and response, upstream request and its captured response.
        let copies = if enabled { 4 } else { 3 };
        for want in [
            "Location: http://127.0.0.1/v1?key=***\n",
            "Content-Location: /v1/r?token=***\n",
            "Referer: https://127.0.0.1/cb#***\n",
            "Link: <http://127.0.0.1/p?key=***>; rel=\"next\"\n",
        ] {
            assert_eq!(content.matches(want).count(), copies, "{want}: {content}");
        }
        for (name, value) in HEADERS {
            assert_eq!(received[*name], *value, "{name}");
        }
    }
}

/// Error text an executor hands the log names no URL credential, even when the
/// executor did not redact it.
#[tokio::test]
async fn urls_in_captured_errors_are_redacted() {
    let error = format!(r#"Post "ftp://user:{SECRET}@127.0.0.1/?api_key={SECRET}": unsupported protocol scheme "ftp""#);
    let dial =
        format!("dial failed: error sending request for uri (http://127.0.0.1:1/?api_key={SECRET}/responses): EOF");
    let hostless = format!(r#"Post "ftp://127.0.0.1?key=prefix@{SECRET}": unsupported protocol scheme "ftp""#);
    let content = persisted(true, StatusCode::OK, move |log| {
        let sink = log.capture_sink();
        sink.record(Request(upstream_request("https://fixture.invalid/v1", &[], b"")));
        sink.record(ResponseError(&error));
        sink.record(ResponseError(&hostless));
        sink.record(WebsocketError {
            stage: "dial",
            error: &dial,
        });
    })
    .await;
    let content = String::from_utf8(content).unwrap();
    assert!(!content.contains(SECRET), "{content}");
    for want in [
        r#"Error: Post "ftp://127.0.0.1/?api_key=***": unsupported protocol scheme "ftp""#,
        r#"Error: Post "<redacted url>": unsupported protocol scheme "ftp""#,
        "Error: dial failed: error sending request for uri (http://127.0.0.1:1/?api_key=*** EOF",
    ] {
        assert!(content.contains(want), "{want}: {content}");
    }
}
