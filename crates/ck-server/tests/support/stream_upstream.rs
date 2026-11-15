//! A local HTTP/1.1 upstream for execution and flow tests (Phase D).
//!
//! It answers each request with the next scripted [`Reply`] and records what arrived. A
//! reply replays fixture bytes as a chunked body and can delay its first byte, write at a
//! fixed cadence (down to one byte per chunk), cut the body short, answer with an error
//! status before any body byte, or close the connection without answering. It counts request
//! heads as they arrive, so a test can prove that a request was never sent, and reports when
//! each body was fully written, so a test can prove upstream receipt finished while the
//! client stalled.
//!
//! Every connection serves one exchange and closes (`connection: close`). Dropping the
//! upstream closes its listener and every open connection. `ck-exec` tests
//! include this file through `#[path]` from `ck-exec/tests/support/mod.rs`.
#![allow(dead_code, reason = "each test target uses a subset of the harness")]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};

/// The scripted answer to one request.
#[derive(Clone, Debug)]
pub struct Reply {
    status: u16,
    headers: Vec<(String, String)>,
    body: Bytes,
    /// Pause after the response head, before the first body byte.
    first_byte_delay: Duration,
    /// Body bytes per chunk and the pause after each chunk; `None` writes one chunk.
    cadence: Option<(usize, Duration)>,
    /// Close after this many body bytes, without the terminating chunk.
    cut_after: Option<usize>,
    /// Close after reading the request, before any response byte.
    reset: bool,
    /// Request body bytes per read and the pause after each read.
    slow_read: Option<(usize, Duration)>,
}

impl Reply {
    /// A 200 answer whose body is `body`, for example a captured fixture.
    pub fn ok(content_type: &str, body: impl Into<Bytes>) -> Self {
        Self::status(200, content_type, body)
    }

    /// A 200 `text/event-stream` answer.
    pub fn sse(body: impl Into<Bytes>) -> Self {
        Self::ok("text/event-stream", body)
    }

    /// A 200 `application/json` answer.
    pub fn json(body: impl Into<Bytes>) -> Self {
        Self::ok("application/json", body)
    }

    /// An answer with `status`, sent whole before the first body byte matters, for example
    /// a 429 or 500 error document.
    pub fn status(status: u16, content_type: &str, body: impl Into<Bytes>) -> Self {
        Self {
            status,
            headers: vec![("content-type".into(), content_type.into())],
            body: body.into(),
            first_byte_delay: Duration::ZERO,
            cadence: None,
            cut_after: None,
            reset: false,
            slow_read: None,
        }
    }

    /// Closes the connection after reading the request, with no status line: a transport
    /// error before the first byte.
    pub fn reset() -> Self {
        Self {
            reset: true,
            ..Self::status(200, "text/plain", Bytes::new())
        }
    }

    /// Adds a response header.
    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// Waits `delay` after the response head before the first body byte.
    pub fn first_byte_after(mut self, delay: Duration) -> Self {
        self.first_byte_delay = delay;
        self
    }

    /// Writes `bytes` body bytes per chunk, flushed, and pauses `interval` after each chunk.
    pub fn throttle(mut self, bytes: usize, interval: Duration) -> Self {
        assert!(bytes > 0, "a chunk carries at least one byte");
        self.cadence = Some((bytes, interval));
        self
    }

    /// One body byte per chunk, each flushed on its own.
    pub fn byte_at_a_time(self) -> Self {
        self.throttle(1, Duration::ZERO)
    }

    /// Closes the connection after `bytes` body bytes, without the terminating chunk.
    pub fn cut_after(mut self, bytes: usize) -> Self {
        assert!(bytes < self.body.len(), "a cut ends before the body does");
        self.cut_after = Some(bytes);
        self
    }

    /// Reads the request body `bytes` at a time and pauses `interval` after each read.
    pub fn slow_read(mut self, bytes: usize, interval: Duration) -> Self {
        assert!(bytes > 0, "a read takes at least one byte");
        self.slow_read = Some((bytes, interval));
        self
    }
}

/// One request as it arrived.
#[derive(Clone, Debug)]
pub struct Captured {
    /// Arrival order of the request head, which also selected the reply.
    pub index: usize,
    pub method: String,
    /// The request target as sent, query included.
    pub path: String,
    /// Header names and values in wire order and case.
    pub headers: Vec<(String, String)>,
    /// The body after chunked decoding.
    pub body: Vec<u8>,
}

impl Captured {
    /// The first value of header `name`, compared case-insensitively.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// How one reply ended.
#[derive(Clone, Debug)]
pub struct Delivery {
    /// Body bytes written to the socket.
    pub written: usize,
    /// Every body byte and the terminating chunk were written.
    pub complete: bool,
    /// The write error, if the client went away first.
    pub error: Option<String>,
    /// When the response head was written; `None` for a reset or a failed head write.
    pub head: Option<Instant>,
    /// When each body chunk's write began, in order. Pacing checks compare these server-side
    /// times, which a slow client cannot shift.
    pub writes: Vec<Instant>,
    pub finished: Instant,
}

/// What `respond` wrote before it returned.
#[derive(Default)]
struct Log {
    written: usize,
    head: Option<Instant>,
    writes: Vec<Instant>,
}

#[derive(Default)]
struct State {
    heads: usize,
    captured: Vec<Captured>,
    delivered: Vec<Option<Delivery>>,
}

pub struct StreamUpstream {
    addr: SocketAddr,
    state: watch::Sender<State>,
    accept: JoinHandle<()>,
}

impl StreamUpstream {
    /// Listens on a loopback port and answers request `i` with `replies[i]`. A request past
    /// the script gets a 501 naming the harness, so a test sees an unplanned send.
    pub async fn start(replies: impl IntoIterator<Item = Reply>) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind upstream");
        let addr = listener.local_addr().expect("upstream address");
        let replies: Arc<[Reply]> = replies.into_iter().collect();
        let state = watch::Sender::new(State::default());
        let shared = state.clone();
        // The accept task owns every connection task: aborting it drops the set, which
        // aborts them, so no exchange outlives the upstream.
        let accept = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            while let Ok((socket, _)) = listener.accept().await {
                while connections.try_join_next().is_some() {}
                connections.spawn(serve(socket, replies.clone(), shared.clone()));
            }
        });
        Self { addr, state, accept }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Base URL without a trailing slash, for a credential's `base_url`.
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Request heads received so far, including requests whose body is still arriving.
    pub fn requests(&self) -> usize {
        self.state.borrow().heads
    }

    /// Requests whose body was read completely, in head order.
    pub fn captured(&self) -> Vec<Captured> {
        let mut captured = self.state.borrow().captured.clone();
        captured.sort_by_key(|c| c.index);
        captured
    }

    /// Waits until reply `index` ended: written completely, cut, reset or failed.
    pub async fn delivered(&self, index: usize) -> Delivery {
        let mut state = self.state.subscribe();
        let state = state
            .wait_for(|s| s.delivered.get(index).is_some_and(Option::is_some))
            .await
            .expect("the upstream keeps its state sender");
        state.delivered[index].clone().expect("checked by wait_for")
    }

    /// Reply `index`'s outcome if it already ended.
    pub fn delivery(&self, index: usize) -> Option<Delivery> {
        self.state.borrow().delivered.get(index).cloned().flatten()
    }
}

impl Drop for StreamUpstream {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

async fn serve(socket: TcpStream, replies: Arc<[Reply]>, state: watch::Sender<State>) {
    socket.set_nodelay(true).expect("set TCP_NODELAY");
    let mut socket = BufReader::new(socket);
    let Some((method, path, headers)) = read_head(&mut socket).await else {
        return;
    };
    let mut index = 0;
    state.send_modify(|s| {
        index = s.heads;
        s.heads += 1;
        s.delivered.push(None);
    });
    let reply = replies.get(index).cloned().unwrap_or_else(|| {
        Reply::status(
            501,
            "text/plain",
            format!("stream_upstream: no reply scripted for request {index}"),
        )
    });
    let Some(body) = read_body(&mut socket, &headers, reply.slow_read).await else {
        finish(
            &state,
            index,
            Log::default(),
            false,
            Some("request body ended early".into()),
        );
        return;
    };
    state.send_modify(|s| {
        s.captured.push(Captured {
            index,
            method,
            path,
            headers,
            body,
        })
    });
    let mut socket = socket.into_inner();
    let mut log = Log::default();
    if reply.reset {
        let _ = socket.shutdown().await;
        finish(&state, index, log, false, None);
        return;
    }
    let (complete, error) = match respond(&mut socket, &reply, &mut log).await {
        Ok(complete) => (complete, None),
        Err(error) => (false, Some(error.to_string())),
    };
    let _ = socket.shutdown().await;
    finish(&state, index, log, complete, error);
}

fn finish(state: &watch::Sender<State>, index: usize, log: Log, complete: bool, error: Option<String>) {
    let delivery = Delivery {
        written: log.written,
        complete,
        error,
        head: log.head,
        writes: log.writes,
        finished: Instant::now(),
    };
    state.send_modify(|s| s.delivered[index] = Some(delivery));
}

/// Writes the head and the body into `log`; returns whether the body completed.
async fn respond(socket: &mut TcpStream, reply: &Reply, log: &mut Log) -> std::io::Result<bool> {
    let mut head = format!("HTTP/1.1 {} \r\n", reply.status);
    for (name, value) in &reply.headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("transfer-encoding: chunked\r\nconnection: close\r\n\r\n");
    write(socket, head.as_bytes()).await?;
    log.head = Some(Instant::now());
    tokio::time::sleep(reply.first_byte_delay).await;
    let end = reply.cut_after.unwrap_or(reply.body.len());
    let (step, interval) = reply.cadence.unwrap_or((end.max(1), Duration::ZERO));
    while log.written < end {
        let piece = &reply.body[log.written..end.min(log.written + step)];
        let mut chunk = format!("{:x}\r\n", piece.len()).into_bytes();
        chunk.extend_from_slice(piece);
        chunk.extend_from_slice(b"\r\n");
        log.writes.push(Instant::now());
        write(socket, &chunk).await?;
        log.written += piece.len();
        tokio::time::sleep(interval).await;
    }
    if reply.cut_after.is_some() {
        return Ok(false);
    }
    write(socket, b"0\r\n\r\n").await?;
    Ok(true)
}

async fn write(socket: &mut TcpStream, bytes: &[u8]) -> std::io::Result<()> {
    socket.write_all(bytes).await?;
    socket.flush().await
}

type Head = (String, String, Vec<(String, String)>);

async fn read_head(socket: &mut BufReader<TcpStream>) -> Option<Head> {
    let line = read_line(socket).await?;
    let mut parts = line.split(' ');
    let (method, path) = (parts.next()?.to_owned(), parts.next()?.to_owned());
    let mut headers = Vec::new();
    loop {
        let line = read_line(socket).await?;
        if line.is_empty() {
            return Some((method, path, headers));
        }
        let (name, value) = line.split_once(':')?;
        headers.push((name.to_owned(), value.trim().to_owned()));
    }
}

async fn read_line(socket: &mut BufReader<TcpStream>) -> Option<String> {
    let mut line = String::new();
    if socket.read_line(&mut line).await.ok()? == 0 {
        return None;
    }
    Some(line.trim_end_matches(['\r', '\n']).to_owned())
}

async fn read_body(
    socket: &mut BufReader<TcpStream>,
    headers: &[(String, String)],
    slow: Option<(usize, Duration)>,
) -> Option<Vec<u8>> {
    let header = |name: &str| {
        headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    };
    let mut body = Vec::new();
    if header("transfer-encoding").is_some_and(|v| v.eq_ignore_ascii_case("chunked")) {
        loop {
            let size = read_line(socket).await?;
            let size = usize::from_str_radix(size.split(';').next()?.trim(), 16).ok()?;
            if size == 0 {
                // Trailers, then the blank line.
                while !read_line(socket).await?.is_empty() {}
                return Some(body);
            }
            read_exact(socket, &mut body, size, slow).await?;
            if !read_line(socket).await?.is_empty() {
                return None;
            }
        }
    }
    let length = header("content-length").map_or(Some(0), |v| v.parse().ok())?;
    read_exact(socket, &mut body, length, slow).await?;
    Some(body)
}

async fn read_exact(
    socket: &mut BufReader<TcpStream>,
    body: &mut Vec<u8>,
    len: usize,
    slow: Option<(usize, Duration)>,
) -> Option<()> {
    let (step, interval) = slow.unwrap_or((len.max(1), Duration::ZERO));
    let end = body.len() + len;
    while body.len() < end {
        let start = body.len();
        body.resize(end.min(start + step), 0);
        socket.read_exact(&mut body[start..]).await.ok()?;
        tokio::time::sleep(interval).await;
    }
    Some(())
}
