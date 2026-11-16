//! URLs in request logs print as `<redacted url>` (owner ruling Settled 60), in the
//! downstream URL, upstream HTTP and WebSocket URLs, captured error text, bodies and
//! URL-bearing headers.

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
    assert_eq!(
        content.matches("\nUpstream URL: <redacted url>\n").count(),
        2,
        "{content}"
    );
}

/// Names that no list calls sensitive, bare entries and fragments, downstream and
/// upstream.
#[tokio::test]
async fn urls_with_a_query_or_fragment_are_redacted_in_logs() {
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
    line(&content, "URL: <redacted url>");
    assert_eq!(
        content.matches("\nUpstream URL: <redacted url>\n").count(),
        5,
        "{content}"
    );
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
            "Location: <redacted url>",
            "Content-Location: <redacted url>",
            "Referer: <redacted url>",
            "Link: <redacted url>",
            "Refresh: <redacted url> url=/next",
            "Reporting-Endpoints: default=\"<redacted url>\"",
        ] {
            assert_eq!(
                content.lines().filter(|l| *l == want).count(),
                copies,
                "{want}: {content}"
            );
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
    let quoted = r#"Error: Post "<redacted url>": unsupported protocol scheme "ftp""#;
    assert_eq!(content.matches(quoted).count(), 2, "{content}");
    line(
        &content,
        "Error: dial failed: error sending request for uri <redacted url> EOF",
    );
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
    line(&content, r#"{"error":{"message":"request failed for <redacted url>"}}"#);
}

/// Quoted values with spaces in them whatever the quote, references after `=`, and
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
            "dial https://user:sk-review-secret://host: refused",
            "dial redis:sk-review-secret@host: refused",
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
        "Error: Post <redacted url>",
        "Error: Post \" <redacted url>\": EOF",
        "Error: request failed: <redacted url>",
        "Error: dial <redacted url> refused",
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

/// A section past the memory limit goes to a spool file, which holds the bytes before
/// they are redacted and so has no name in the log directory.
#[tokio::test]
async fn spools_leave_no_file_in_the_log_directory() {
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
        let guard = log.0.lock().unwrap();
        let record = guard.as_ref().unwrap();
        assert!(record.api_request.spool.is_some() && record.api_response.spool.is_some());
        let entries: Vec<_> = std::fs::read_dir(&record.state.dir).unwrap().flatten().collect();
        assert!(entries.is_empty(), "{entries:?}");
    })
    .await;
    let content = String::from_utf8(content).unwrap();
    assert!(!content.contains(SECRET), "{content}");
    line(&content, "Upstream URL: <redacted url>");
}

/// A spooled section is redacted as it is copied into the log: a word longer than the
/// copy's hold limit prints as the placeholder, a URL at its end included, wherever the
/// read that passes the limit ends.
#[test]
fn spooled_sections_redact_urls_after_long_tokens() {
    let dir = super::tests::scratch();
    // 17 reads of 64 KiB: the read that passes the 1 MiB hold ends with `ending`.
    for ending in ["=", "(", "é", ":"] {
        let mut section = super::Section::new("api-response");
        let mut content = vec![b'a'; 17 * 65536 - ending.len()];
        content.extend_from_slice(ending.as_bytes());
        content.extend_from_slice(b"//user:sk-review-secret@host.example/ end");
        section.append(&dir, &content);
        assert!(section.spool.is_some());
        let mut out = Vec::new();
        section.write_to(&mut out).unwrap();
        let out = String::from_utf8(out).unwrap();
        assert_eq!(out, "<redacted url> end", "{ending}");
    }
    std::fs::remove_dir_all(dir).unwrap();
}

/// A URL split across two messages of a WebSocket timeline, at any byte, prints no part
/// of its credential, upstream and downstream, in memory and spooled. The secrets are
/// the only upper-case letters.
#[test]
fn urls_split_across_timeline_messages_are_redacted() {
    let dir = super::tests::scratch();
    let shapes = [
        "wss://user:QXKEY@host.example/",
        "see https://h.example/?k=QXKEY end",
        "dsn=redis:QXKEY@host.example",
        "x <http://h.example/?code=a QXKEY> y",
        "{\"u\":\"x//user:QXKEY@h.example\"}",
        "callback?code=QXKEY#QXKEY",
    ];
    type Event = fn(&mut super::Section, &std::path::Path, &[u8], &[u8]);
    let surfaces: [Event; 2] = [super::upstream_timeline, super::downstream_timeline];
    for filler in [0, super::SECTION_MEMORY] {
        for shape in shapes {
            for cut in 0..=shape.len() {
                for event in surfaces {
                    let mut section = super::Section::framed("timeline");
                    let first = [&vec![b'a'; filler][..], b" ", &shape.as_bytes()[..cut]].concat();
                    event(&mut section, &dir, b"t: 1\n", &first);
                    event(&mut section, &dir, b"t: 2\n", &shape.as_bytes()[cut..]);
                    assert_eq!(section.spool.is_some(), filler > 0);
                    let mut out = Vec::new();
                    section.write_to(&mut out).unwrap();
                    let out = String::from_utf8(out).unwrap();
                    assert!(
                        !out.bytes().any(|b| b.is_ascii_uppercase()),
                        "{shape} cut at {cut}: {out}"
                    );
                }
            }
        }
    }
    // The last message has no next one to go on into.
    for (messages, want) in [
        (["wss://user:", "APIKEY@host/"], "t: 1\n<redacted url>\n\nt: 2\n"),
        (["{}", "done"], "t: 1\n{}\n\nt: 2\ndone"),
    ] {
        let mut section = super::Section::framed("timeline");
        super::upstream_timeline(&mut section, &dir, b"t: 1\n", messages[0].as_bytes());
        super::upstream_timeline(&mut section, &dir, b"t: 2\n", messages[1].as_bytes());
        let mut out = Vec::new();
        section.write_to(&mut out).unwrap();
        assert_eq!(String::from_utf8(out).unwrap(), want, "{messages:?}");
    }
    std::fs::remove_dir_all(dir).unwrap();
}

/// Makes every later write to the upstream response section's spool fail.
fn fail_spool(log: &super::RequestLog) {
    let mut locked = log.0.lock().unwrap();
    let record = locked.as_mut().unwrap();
    let file = super::tests::unwritable(&record.state.dir);
    let spool = record.api_response.spool.as_mut().expect("the section spilled");
    spool.file = file;
}

/// A captured response section is redacted as a whole: a URL split across chunks at any
/// byte, in memory or across a spill, prints redacted, and a section whose spool write
/// failed prints a marker instead of its content.
#[tokio::test]
async fn captured_response_sections_are_redacted_whole() {
    let filler = |tail: &[u8]| [&[b'x'; 64 << 10][..], tail].concat();
    let long = format!("http://127.0.0.1/?code={}sk-review-secret tail", "x".repeat(2 << 20));
    // (chunks, the spool fails after this many chunks, text the log holds)
    let rows = vec![
        (
            vec![
                b"see http:/".to_vec(),
                b"/u:sk-review-secret@127.0.0.1/path end".to_vec(),
            ],
            None,
            "see <redacted url> end",
        ),
        (
            vec![filler(b" http:/"), b"/u:sk-review-secret@127.0.0.1/path end".to_vec()],
            None,
            "x <redacted url> end",
        ),
        (
            vec![
                filler(b" http://u:sk-review"),
                [&b"-secret@127.0.0.1/x end "[..], &[b'y'; 64 << 10]].concat(),
            ],
            Some(1),
            "\n[capture dropped: write failed]\n",
        ),
        (
            vec![b"Post \"http://127.0.0.1/?code=prefix\nsk-review-secret\": EOF".to_vec()],
            None,
            "Post \"<redacted url>\": EOF",
        ),
        // A spooled section is redacted as it is copied into the log; a reference
        // longer than the copy's hold limit prints as the placeholder.
        (vec![long.into_bytes()], None, "\n<redacted url> tail"),
    ];
    for (chunks, fail, want) in rows {
        let content = persisted(true, StatusCode::BAD_GATEWAY, move |log| {
            let sink = log.capture_sink();
            sink.record(Request(upstream_request("http://127.0.0.1/v1", &[], b"")));
            sink.record(ResponseMetadata(502, &[]));
            for (i, chunk) in chunks.iter().enumerate() {
                if fail == Some(i) {
                    fail_spool(log);
                }
                sink.record(ResponseChunk(chunk));
            }
        })
        .await;
        let content = String::from_utf8(content).unwrap();
        let shown = &content[..content.len().min(4096)];
        assert!(
            !content.contains(SECRET) && !content.contains("-secret"),
            "{want}: {shown}"
        );
        assert!(content.contains(want), "{want}: {shown}");
    }
}

/// Relative references carrying a query or fragment value are redacted in every section
/// of the log; the client receives them unchanged.
#[tokio::test]
async fn relative_references_with_fragments_are_redacted() {
    const HEADERS: &[(&str, &str)] = &[
        ("location", "#access_token=sk-review-secret"),
        ("content-location", "next?code=sk-review-secret"),
        ("link", "<next#t=sk-review-secret>; rel=\"next\""),
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
        // `<` opens a quoted value, dropped up to the next `"`.
        "Link: <redacted url>\"next\"",
    ] {
        assert_eq!(content.lines().filter(|l| *l == want).count(), 4, "{want}: {content}");
    }
    for (name, value) in HEADERS {
        assert_eq!(received[*name], *value, "{name}");
    }
}

/// A content-encoded request body is spooled as read and decoded when a forced error log
/// is written; the decoded body is redacted then.
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
    line(&content, r#"{"url":"<redacted url>"}"#);
    drop(app);
    drop(state);
    std::fs::remove_dir_all(dir).unwrap();
}
