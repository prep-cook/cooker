//! Self-test of the local stream upstream (`support/stream_upstream.rs`): the wire behaviour
//! Phase D tests rely on, checked with a real client and with raw sockets where the chunk
//! framing itself is the claim.

#[path = "support/stream_upstream.rs"]
mod stream_upstream;

use std::time::{Duration, Instant};

use stream_upstream::{Reply, StreamUpstream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const FIXTURE: &[u8] =
    b"data: {\"id\":\"chatcmpl-1\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n";

/// A response read to EOF over a raw socket.
struct Raw {
    status: u16,
    head: String,
    /// Chunk payloads in wire order.
    chunks: Vec<Vec<u8>>,
    /// The terminating zero-size chunk arrived.
    terminated: bool,
}

async fn raw(upstream: &StreamUpstream, request: &[u8]) -> Raw {
    let mut socket = TcpStream::connect(upstream.addr()).await.unwrap();
    socket.write_all(request).await.unwrap();
    let mut bytes = Vec::new();
    socket.read_to_end(&mut bytes).await.unwrap();
    parse(&bytes)
}

fn parse(bytes: &[u8]) -> Raw {
    if bytes.is_empty() {
        return Raw {
            status: 0,
            head: String::new(),
            chunks: Vec::new(),
            terminated: false,
        };
    }
    let split = bytes.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let head = String::from_utf8(bytes[..split].to_vec()).unwrap();
    let status = head[9..12].parse().unwrap();
    let mut rest = &bytes[split + 4..];
    let mut chunks = Vec::new();
    while let Some(line) = rest.windows(2).position(|w| w == b"\r\n") {
        let size = usize::from_str_radix(std::str::from_utf8(&rest[..line]).unwrap(), 16).unwrap();
        if size == 0 {
            return Raw {
                status,
                head,
                chunks,
                terminated: true,
            };
        }
        chunks.push(rest[line + 2..line + 2 + size].to_vec());
        rest = &rest[line + 4 + size..];
    }
    assert!(rest.is_empty(), "a chunk was cut inside its frame");
    Raw {
        status,
        head,
        chunks,
        terminated: false,
    }
}

const POST: &[u8] = b"POST /v1/chat/completions HTTP/1.1\r\nhost: up\r\ncontent-length: 2\r\n\r\n{}";

#[tokio::test]
async fn replays_fixture_and_captures_requests() {
    let upstream = StreamUpstream::start([Reply::sse(FIXTURE).header("x-request-id", "up-1")]).await;
    let client = wreq::Client::new();
    let res = client
        .post(format!("{}/v1/chat/completions?alt=sse", upstream.url()))
        .header("authorization", "Bearer sk-up")
        .body(r#"{"model":"m"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    assert_eq!(res.headers()["content-type"], "text/event-stream");
    assert_eq!(res.headers()["x-request-id"], "up-1");
    assert_eq!(res.bytes().await.unwrap(), FIXTURE);

    let delivery = upstream.delivered(0).await;
    assert!(delivery.complete && delivery.error.is_none());
    assert_eq!(delivery.written, FIXTURE.len());
    assert_eq!(upstream.requests(), 1);
    let captured = upstream.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].method, "POST");
    assert_eq!(captured[0].path, "/v1/chat/completions?alt=sse");
    assert_eq!(captured[0].header("Authorization"), Some("Bearer sk-up"));
    assert_eq!(captured[0].body, br#"{"model":"m"}"#);
}

#[tokio::test]
async fn scripted_status_precedes_success_and_unscripted_requests_are_visible() {
    let upstream = StreamUpstream::start([
        Reply::status(429, "application/json", r#"{"error":{"type":"rate_limit_error"}}"#).header("retry-after", "3"),
        Reply::json(r#"{"ok":true}"#),
    ])
    .await;
    let client = wreq::Client::new();
    let first = client.post(upstream.url()).body("{}").send().await.unwrap();
    assert_eq!(first.status(), 429);
    assert_eq!(first.headers()["retry-after"], "3");
    assert_eq!(first.bytes().await.unwrap(), r#"{"error":{"type":"rate_limit_error"}}"#);
    let second = client.post(upstream.url()).body("{}").send().await.unwrap();
    assert_eq!(second.status(), 200);
    assert_eq!(second.bytes().await.unwrap(), r#"{"ok":true}"#);
    let third = client.post(upstream.url()).body("{}").send().await.unwrap();
    assert_eq!(third.status(), 501);
    assert_eq!(upstream.requests(), 3);
}

#[tokio::test]
async fn reset_answers_nothing() {
    let upstream = StreamUpstream::start([Reply::reset()]).await;
    let response = raw(&upstream, POST).await;
    assert_eq!(response.status, 0, "no status line before the reset");
    assert!(
        wreq::Client::new()
            .post(upstream.url())
            .send()
            .await
            .is_ok_and(|r| r.status() == 501)
    );
    let delivery = upstream.delivered(0).await;
    assert_eq!((delivery.written, delivery.complete), (0, false));
    assert_eq!(upstream.requests(), 2);
}

#[tokio::test]
async fn cut_ends_the_body_without_a_terminator() {
    let upstream = StreamUpstream::start([Reply::sse(FIXTURE).cut_after(10)]).await;
    let response = raw(&upstream, POST).await;
    assert_eq!(response.status, 200);
    assert!(response.head.contains("transfer-encoding: chunked"));
    assert_eq!(response.chunks.concat(), &FIXTURE[..10]);
    assert!(!response.terminated);
    let delivery = upstream.delivered(0).await;
    assert_eq!((delivery.written, delivery.complete), (10, false));

    let upstream = StreamUpstream::start([Reply::sse(FIXTURE).cut_after(10)]).await;
    let res = wreq::Client::new().post(upstream.url()).send().await.unwrap();
    assert!(res.bytes().await.is_err(), "a client sees the cut as a body error");
}

#[tokio::test]
async fn byte_cadence_frames_every_byte_and_throttle_paces_writes() {
    let upstream = StreamUpstream::start([Reply::sse(FIXTURE).byte_at_a_time()]).await;
    let response = raw(&upstream, POST).await;
    assert!(response.terminated);
    assert_eq!(response.chunks.len(), FIXTURE.len());
    assert!(response.chunks.iter().all(|c| c.len() == 1));
    assert_eq!(response.chunks.concat(), FIXTURE);

    let upstream = StreamUpstream::start([Reply::sse(&b"0123456789"[..])
        .first_byte_after(Duration::from_millis(50))
        .throttle(2, Duration::from_millis(10))])
    .await;
    let start = Instant::now();
    let response = raw(&upstream, POST).await;
    assert!(start.elapsed() >= Duration::from_millis(90), "{:?}", start.elapsed());
    assert_eq!(response.chunks.len(), 5);
    assert_eq!(response.chunks.concat(), b"0123456789");
}

#[tokio::test]
async fn slow_read_takes_chunked_request_bodies() {
    let upstream = StreamUpstream::start([Reply::json("{}").slow_read(3, Duration::from_millis(10))]).await;
    let start = Instant::now();
    let response = raw(
        &upstream,
        b"POST /up HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n6;x=1\r\n{\"a\":1\r\n4\r\n,\"b\"\r\n3\r\n:2}\r\n0\r\nx-trailer: t\r\n\r\n",
    )
    .await;
    assert!(start.elapsed() >= Duration::from_millis(40), "{:?}", start.elapsed());
    assert_eq!(response.status, 200);
    assert_eq!(upstream.captured()[0].body, br#"{"a":1,"b":2}"#);
}

/// The receipt signal D2 needs: while the client reads nothing, a body larger than the
/// socket buffers stays undelivered; once the client drains, the upstream reports completion.
#[tokio::test]
async fn delivery_waits_for_a_stalled_client() {
    let body = vec![b'x'; 32 << 20];
    let upstream = StreamUpstream::start([Reply::ok("application/octet-stream", body.clone())]).await;
    let mut socket = TcpStream::connect(upstream.addr()).await.unwrap();
    socket.write_all(POST).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(300), upstream.delivered(0))
            .await
            .is_err()
    );
    assert!(upstream.delivery(0).is_none());
    let mut bytes = Vec::new();
    socket.read_to_end(&mut bytes).await.unwrap();
    let delivery = upstream.delivered(0).await;
    assert!(delivery.complete);
    assert_eq!(delivery.written, body.len());
    assert_eq!(parse(&bytes).chunks.concat(), body);
}
