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

/// Names that no list calls sensitive, bare entries and
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

/// The logged copies of URL-bearing headers are redacted by value, whatever the name or
/// grammar, in the request log and in the forced error log;
/// the client receives them unchanged.
#[tokio::test]
async fn url_headers_are_redacted_in_logs_only() {
    const HEADERS: &[(&str, &str)] = &[
        ("location", "http://127.0.0.1/v1?key=sk-review-secret"),
        ("content-location", "/v1/r?token=sk-review-secret"),
        ("referer", "https://u:sk-review-secret@127.0.0.1/cb#sk-review-secret"),
        ("link", "http://127.0.0.1/?code=sk-review-secret"),
        ("refresh", "http://127.0.0.1/?code=sk-review-secret; url=/next"),
        (
            "reporting-endpoints",
            "default=\"http://127.0.0.1/reports?key=sk-review-secret\"",
        ),
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
            "Link: http://127.0.0.1/?code=***\n",
            "Refresh: http://127.0.0.1/?code=*** url=/next\n",
            "Reporting-Endpoints: default=\"http://127.0.0.1/reports?key=***\"\n",
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

/// An upstream error body is captured with its URLs redacted.
#[tokio::test]
async fn upstream_error_bodies_are_redacted_in_captures() {
    let content = persisted(true, StatusCode::BAD_GATEWAY, |log| {
        let sink = log.capture_sink();
        sink.record(Request(upstream_request("http://127.0.0.1/v1", &[], b"")));
        sink.record(ResponseMetadata(502, &[]));
        sink.record(ResponseChunk(
            br#"{"error":{"message":"request failed for http://u:sk-review-secret@127.0.0.1/?code=sk-review-secret#sk-review-secret"}}"#,
        ));
    })
    .await;
    let content = String::from_utf8(content).unwrap();
    assert!(!content.contains(SECRET), "{content}");
    line(
        &content,
        r#"{"error":{"message":"request failed for http://127.0.0.1/?code=***#***"}}"#,
    );
}

/// Quoted references whatever the quote, references after `=`, and
/// path-absolute references in captured errors.
#[tokio::test]
async fn every_reference_shape_in_captured_errors_is_redacted() {
    let content = persisted(true, StatusCode::OK, |log| {
        let sink = log.capture_sink();
        sink.record(Request(upstream_request("http://127.0.0.1/v1", &[], b"")));
        for error in [
            "Post 'http://127.0.0.1/?code=prefix sk-review-secret': EOF",
            "Post \" http://127.0.0.1/?code=prefix sk-review-secret\": EOF",
            "request failed: url=//user:sk-review-secret@127.0.0.1/?code=sk-review-secret",
            "request failed: /callback?code=sk-review-secret#sk-review-secret",
            "Post \"http://127.0.0.1/?code=prefix\r\nsk-review-secret\": EOF",
        ] {
            sink.record(ResponseError(error));
        }
        sink.record(WebsocketError {
            stage: "read",
            error: "Post \"http://127.0.0.1/?code=prefix\r\nsk-review-secret\": EOF",
        });
    })
    .await;
    let content = String::from_utf8(content).unwrap();
    assert!(!content.contains(SECRET), "{content}");
    for want in [
        "Error: Post 'http://127.0.0.1/?code=***': EOF",
        "Error: Post \" http://127.0.0.1/?code=***\": EOF",
        "Error: request failed: url=//127.0.0.1/?code=***",
        "Error: request failed: /callback?code=***#***",
    ] {
        line(&content, want);
    }
    // The captured error and the websocket error.
    assert_eq!(
        content.matches("\nError: Post \"<redacted url>\": EOF\n").count(),
        2,
        "{content}"
    );
}

/// A section past the memory limit goes to a spool file, and the
/// spool never holds a URL credential, even before the log is written.
#[tokio::test]
async fn spools_never_hold_url_credentials() {
    let spooled = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = spooled.clone();
    let content = persisted(true, StatusCode::OK, move |log| {
        let sink = log.capture_sink();
        let body = vec![b'x'; 65536];
        sink.record(Request(upstream_request(
            "http://u:sk-review-secret@127.0.0.1/v1?code=sk-review-secret#sk-review-secret",
            &[],
            &body,
        )));
        sink.record(ResponseMetadata(200, &[]));
        sink.record(ResponseChunk(&body));
        // A URL split across chunks of a spooled section.
        sink.record(ResponseChunk(b" http://127.0.0.1/?code=sk-review"));
        sink.record(ResponseChunk(b"-secret end"));
        let guard = log.0.lock().unwrap();
        let dir = &guard.as_ref().unwrap().state.dir;
        for entry in std::fs::read_dir(dir).unwrap().flatten() {
            if entry.path().extension().is_some_and(|e| e == "tmp") {
                seen.lock().unwrap().push(std::fs::read(entry.path()).unwrap());
            }
        }
    })
    .await;
    let spooled = spooled.lock().unwrap();
    assert_eq!(spooled.len(), 2, "both sections spool");
    for bytes in spooled.iter() {
        assert!(!String::from_utf8_lossy(bytes).contains(SECRET));
    }
    let content = String::from_utf8(content).unwrap();
    assert!(!content.contains(SECRET), "{content}");
    line(&content, "Upstream URL: http://127.0.0.1/v1?code=***#***");
    // The chunks the URL was split across are joined, not separated, so no tail of the
    // credential prints.
    assert!(!content.contains("-secret"), "{content}");
    line(&content, "http://127.0.0.1/?code=*** end");
}

/// A captured body keeps a quoted reference whole across a line
/// break, and a reference longer than the hold limit prints as the placeholder.
#[tokio::test]
async fn captured_bodies_keep_references_whole() {
    let long = format!(
        "http://127.0.0.1/?code={}sk-review-secret tail",
        "x".repeat(2 * ck_wire::redact::HOLD_LIMIT)
    );
    for (body, want) in [
        (
            b"Post \"http://127.0.0.1/?code=prefix\nsk-review-secret\": EOF".to_vec(),
            "Post \"<redacted url>\": EOF",
        ),
        (long.into_bytes(), "<redacted url> tail"),
    ] {
        let content = persisted(true, StatusCode::BAD_GATEWAY, move |log| {
            let sink = log.capture_sink();
            sink.record(Request(upstream_request("http://127.0.0.1/v1", &[], b"")));
            sink.record(ResponseMetadata(502, &[]));
            sink.record(ResponseChunk(&body));
        })
        .await;
        let content = String::from_utf8(content).unwrap();
        assert!(!content.contains(SECRET), "{}", &content[..content.len().min(4096)]);
        line(&content, want);
    }
}

/// Relative references carrying a fragment are redacted in every
/// section of the log; the client receives them unchanged.
#[tokio::test]
async fn relative_references_with_fragments_are_redacted() {
    const HEADERS: &[(&str, &str)] = &[
        ("location", "#sk-review-secret"),
        ("content-location", "next#sk-review-secret"),
        ("link", "<next#sk-review-secret>; rel=\"next\""),
    ];
    let (content, received) = persisted_request(true, StatusCode::BAD_GATEWAY, "/fixture", HEADERS, |log| {
        let sink = log.capture_sink();
        let headers: Vec<_> = HEADERS
            .iter()
            .map(|(n, v)| (super::header_name(n), (*v).to_owned()))
            .collect();
        sink.record(Request(upstream_request("http://127.0.0.1/", &headers, b"")));
        sink.record(ResponseMetadata(302, &headers));
    })
    .await;
    let content = String::from_utf8(content).unwrap();
    assert!(!content.contains(SECRET), "{content}");
    for want in [
        "Location: <redacted url>",
        "Content-Location: <redacted url>",
        "Link: <<redacted url> rel=\"next\"",
    ] {
        assert_eq!(content.lines().filter(|l| *l == want).count(), 4, "{want}: {content}");
    }
    for (name, value) in HEADERS {
        assert_eq!(received[*name], *value, "{name}");
    }
}

/// Every spool file is written through the redactor: a URL split across writes never
/// reaches the file with its credential, and ending the spool writes what was held.
#[test]
fn spool_files_are_written_redacted() {
    use std::io::Write as _;
    let dir = std::env::temp_dir().join(format!("cooker-url-spool-{}", uuid::Uuid::new_v4()));
    let mut spool = super::Spool::new(&dir, "request-body").unwrap();
    spool.file.write_all(b"{\"url\":\"http://u:sk-review").unwrap();
    spool
        .file
        .write_all(b"-secret@127.0.0.1/?code=sk-review-secret")
        .unwrap();
    assert!(!String::from_utf8_lossy(&std::fs::read(&spool.temp.path).unwrap()).contains("sk-review"));
    spool.file.write_all(b"\"}").unwrap();
    spool.file.end().unwrap();
    assert_eq!(
        std::fs::read_to_string(&spool.temp.path).unwrap(),
        "{\"url\":\"http://127.0.0.1/?code=***\"}"
    );
    drop(spool);
    std::fs::remove_dir_all(dir).unwrap();
}

/// A content-encoded request body is kept in memory, not spooled, until a forced error
/// log decodes it: the spool's redactor would corrupt the encoding. The decoded body is
/// redacted when the log is written.
#[tokio::test]
async fn encoded_request_bodies_reach_forced_logs_decoded() {
    use tokio::io::AsyncReadExt as _;
    use tower::Service as _;
    let json = br#"{"url":"http://u:sk-review-secret@127.0.0.1/?code=sk-review-secret"}"#;
    let mut encoded = Vec::new();
    async_compression::tokio::bufread::ZstdEncoder::new(&json[..])
        .read_to_end(&mut encoded)
        .await
        .unwrap();
    let dir = super::tests::scratch();
    let state = super::tests::management(&dir, false, false);
    let app =
        axum::Router::new().fallback(|_: axum::body::Bytes| async { (StatusCode::INTERNAL_SERVER_ERROR, "done") });
    let mut app = super::router(&state, app);
    // A streamed body of unknown length is captured as it is read.
    let body = axum::body::Body::from_stream(futures_util::stream::once(async move {
        Ok::<_, std::io::Error>(axum::body::Bytes::from(encoded))
    }));
    let request = axum::http::Request::builder()
        .method("POST")
        .uri("/fixture")
        .header("content-encoding", "zstd")
        .body(body)
        .unwrap();
    let response = app.call(request).await.unwrap();
    axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let mut content = String::new();
    for _ in 0..500 {
        if let [log] = &super::tests::logs(&dir)[..] {
            content = std::fs::read_to_string(log).unwrap();
            if content.trim_end().ends_with("done") {
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(!content.contains(SECRET), "{content}");
    line(&content, r#"{"url":"http://127.0.0.1/?code=***"}"#);
    drop(app);
    drop(state);
    std::fs::remove_dir_all(dir).unwrap();
}

/// A spooled request body ends its redactor before it is read, so a URL at its very end
/// is written, redacted.
#[test]
fn spooled_request_bodies_end_before_they_are_read() {
    use std::io::Write as _;
    let dir = super::tests::scratch();
    let mut deferred = super::Deferred {
        spool: Some(super::Spool::new(&dir, "request-body").unwrap()),
        memory: Vec::new(),
        length: None,
        read: 0,
        captured: 0,
        eof: true,
        truncated: false,
        failed: false,
    };
    let spool = deferred.spool.as_mut().unwrap();
    spool
        .file
        .write_all(b"see http://u:sk-review-secret@127.0.0.1/?code=sk-review-secret")
        .unwrap();
    let (body, _) = deferred.bytes().unwrap();
    assert_eq!(String::from_utf8(body).unwrap(), "see http://127.0.0.1/?code=***");
    drop(deferred);
    std::fs::remove_dir_all(dir).unwrap();
}
