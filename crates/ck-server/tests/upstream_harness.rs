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
async fn byte_cadence_frames_every_byte() {
    let upstream = StreamUpstream::start([Reply::sse(FIXTURE).byte_at_a_time()]).await;
    let response = raw(&upstream, POST).await;
    assert!(response.terminated);
    assert_eq!(response.chunks.len(), FIXTURE.len());
    assert!(response.chunks.iter().all(|c| c.len() == 1));
    assert_eq!(response.chunks.concat(), FIXTURE);
}

/// A response read chunk by chunk as the bytes arrive, so a test can assert that the next
/// chunk has not arrived yet.
struct Live {
    socket: TcpStream,
    buffer: Vec<u8>,
}

impl Live {
    /// Sends `POST` and returns once the response head arrived.
    async fn open(upstream: &StreamUpstream) -> Self {
        let mut socket = TcpStream::connect(upstream.addr()).await.unwrap();
        socket.write_all(POST).await.unwrap();
        let mut live = Self {
            socket,
            buffer: Vec::new(),
        };
        while !live.buffer.windows(4).any(|w| w == b"\r\n\r\n") {
            assert!(live.fill().await, "the connection ended before the response head");
        }
        let split = live.buffer.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
        live.buffer.drain(..split + 4);
        live
    }

    /// Reads more bytes; false at EOF or a reset. Cancel-safe: bytes join the buffer only
    /// after a read completes.
    async fn fill(&mut self) -> bool {
        let mut bytes = [0; 4096];
        match self.socket.read(&mut bytes).await {
            Ok(0) | Err(_) => false,
            Ok(n) => {
                self.buffer.extend_from_slice(&bytes[..n]);
                true
            }
        }
    }

    /// The next chunk payload; `None` at the terminating chunk or when the connection ends.
    async fn chunk(&mut self) -> Option<Vec<u8>> {
        loop {
            if let Some(line) = self.buffer.windows(2).position(|w| w == b"\r\n") {
                let size = usize::from_str_radix(std::str::from_utf8(&self.buffer[..line]).unwrap(), 16).unwrap();
                if size == 0 {
                    return None;
                }
                if self.buffer.len() >= line + 4 + size {
                    let chunk = self.buffer[line + 2..line + 2 + size].to_vec();
                    self.buffer.drain(..line + 4 + size);
                    return Some(chunk);
                }
            }
            if !self.fill().await {
                return None;
            }
        }
    }

    /// Asserts that no chunk arrives within `quiet`, then returns the next one.
    async fn chunk_after(&mut self, quiet: Duration) -> Option<Vec<u8>> {
        assert!(
            tokio::time::timeout(quiet, self.chunk()).await.is_err(),
            "a chunk arrived within {quiet:?}"
        );
        self.chunk().await
    }
}

/// The margins are half the configured pauses: a busy host only lengthens a pause.
#[tokio::test]
async fn first_byte_delay_holds_the_body_after_the_head() {
    let upstream =
        StreamUpstream::start([Reply::sse(&b"0123456789"[..]).first_byte_after(Duration::from_millis(400))]).await;
    let mut live = Live::open(&upstream).await;
    assert_eq!(
        live.chunk_after(Duration::from_millis(200)).await.unwrap(),
        b"0123456789"
    );
    assert_eq!(live.chunk().await, None);
    assert!(upstream.delivered(0).await.complete);
}

#[tokio::test]
async fn throttle_paces_every_chunk() {
    let upstream =
        StreamUpstream::start([Reply::sse(&b"0123456789"[..]).throttle(2, Duration::from_millis(200))]).await;
    let mut live = Live::open(&upstream).await;
    assert_eq!(live.chunk().await.unwrap(), b"01");
    for expected in [b"23", b"45", b"67", b"89"] {
        assert_eq!(live.chunk_after(Duration::from_millis(100)).await.unwrap(), expected);
    }
    assert_eq!(live.chunk().await, None);
}

/// Dropping the upstream ends its open exchanges: a client mid-body sees the connection
/// end without the rest of the body.
#[tokio::test]
async fn drop_ends_open_connections() {
    let upstream =
        StreamUpstream::start([Reply::sse(&b"still alive"[..]).throttle(1, Duration::from_millis(50))]).await;
    let mut live = Live::open(&upstream).await;
    assert_eq!(live.chunk().await.unwrap(), b"s");
    drop(upstream);
    let mut rest = Vec::new();
    let ended = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(chunk) = live.chunk().await {
            rest.extend(chunk);
        }
    })
    .await;
    assert!(ended.is_ok(), "the connection stayed open after the drop");
    assert!(
        rest.len() < b"till alive".len(),
        "the body continued after the drop: {rest:?}"
    );
    assert!(
        !live.buffer.windows(5).any(|w| w == b"0\r\n\r\n"),
        "the terminating chunk arrived after the drop"
    );
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
