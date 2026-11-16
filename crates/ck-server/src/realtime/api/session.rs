//! One Realtime WebSocket or call sideband as a live session (S1 live standard §2.2): the
//! session runtime (`crate::inference::sessions`) forwards every message as received after
//! its admission and metadata projection (`ck_translate::realtime`), and this module supplies
//! the client socket, its liveness, the end log and the usage.
//!
//! - Liveness (Settled 58): a ping every `realtime.ping-interval-seconds`; the session ends
//!   when no pong arrived for `realtime.read-timeout-seconds` (only a pong extends it) or one
//!   write blocks past `realtime.write-timeout-seconds`. Pings from the client are answered by
//!   the transport.
//! - Every session ends with exactly one `live_session_end` log naming its cause.
//! - Usage: once the session has ended, its stream is read once; each response's and each
//!   transcription's usage is its own usage record beside the session's (E-Q13). A response
//!   without `response.done` usage records none.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use axum::extract::ws::{self, WebSocket};
use bytes::Bytes;
use ck_core::config::Realtime;
use ck_core::credential::Credential;
use ck_exec::codex_live::LiveSocket;
use ck_exec::session::{Close, Frame};
use ck_stream::{Read, Terminal};
use ck_translate::codec::document::{Document, Value};
use ck_translate::codec::event::{Direction, Identity, Kind, LocalId};
use ck_translate::codec::internal;
use ck_translate::codec::live::Refusal;
use ck_translate::realtime::{self, Admission, Events, Start};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;

use crate::inference::sessions::{self, ClientRead, ClientWrite, End, Ended};
use crate::inference::streams::Owner;
use crate::observability::usage_record::{Facts, Tracker};
use crate::observability::usage_snapshot::{self, ResponseFacts, Usage};
use crate::runtime::state::Runtime;

/// Everything a session needs besides its sockets.
pub(super) struct Opened {
    pub rt: Arc<Runtime>,
    pub settings: Realtime,
    pub owner: Owner,
    pub events: Events,
    /// The model the client asked for: `session.update` may not name another. `None` for a
    /// sideband, whose call fixed its model.
    pub model: Option<String>,
    /// The model the upstream session runs, for the usage record.
    pub upstream_model: String,
    pub credential: Arc<Credential>,
    pub call_id: Option<String>,
    pub facts: Arc<Facts>,
    /// The call's end (hangup, expiry, replacement); a direct session never sees it.
    pub hangup: CancellationToken,
}

/// Why the client side stopped, when the gateway or a hangup stopped it.
#[derive(Clone, Copy, Debug)]
enum Stop {
    Hangup,
    ReadTimeout { idle: Duration },
    WriteTimeout { blocked: Duration },
}

/// Liveness of the client socket, shared by its two halves and read by the end log.
#[derive(Default)]
struct Liveness {
    pings_sent: u64,
    pongs_received: u64,
    last_ping: Option<Instant>,
    last_pong: Option<Instant>,
    last_rtt: Option<Duration>,
    max_rtt: Option<Duration>,
    max_write: Duration,
    stop: Option<Stop>,
}

type Shared = Arc<Mutex<Liveness>>;
type Sink = Arc<AsyncMutex<SplitSink<WebSocket, ws::Message>>>;

fn lock(live: &Shared) -> std::sync::MutexGuard<'_, Liveness> {
    live.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Writes `message` to the client within the write deadline, recording how long it took.
async fn write(sink: &Sink, live: &Shared, limit: Duration, message: ws::Message) -> Result<(), ()> {
    let started = Instant::now();
    let result = tokio::time::timeout(limit, async { sink.lock().await.send(message).await }).await;
    let took = started.elapsed();
    let mut live = lock(live);
    live.max_write = live.max_write.max(took);
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(()),
        Err(_) => {
            live.stop.get_or_insert(Stop::WriteTimeout { blocked: took });
            Err(())
        }
    }
}

/// The reading half of the client socket; it also sends the pings.
struct ClientRx {
    stream: SplitStream<WebSocket>,
    sink: Sink,
    live: Shared,
    settings: Realtime,
    ping: tokio::time::Interval,
    /// Extended only by a pong.
    deadline: tokio::time::Instant,
    started: Instant,
    hangup: CancellationToken,
}

impl ClientRx {
    fn stop(&self, stop: Stop) -> Option<Frame> {
        lock(&self.live).stop.get_or_insert(stop);
        None
    }
}

impl ClientRead for ClientRx {
    async fn recv(&mut self) -> Option<Frame> {
        loop {
            tokio::select! {
                biased;
                () = self.hangup.cancelled() => return self.stop(Stop::Hangup),
                () = tokio::time::sleep_until(self.deadline) => {
                    let since = lock(&self.live).last_pong.unwrap_or(self.started);
                    return self.stop(Stop::ReadTimeout { idle: since.elapsed() });
                }
                _ = self.ping.tick() => {
                    let sent = Instant::now();
                    let ping = ws::Message::Ping(Bytes::new());
                    write(&self.sink, &self.live, self.settings.write_timeout, ping).await.ok()?;
                    let mut live = lock(&self.live);
                    live.pings_sent += 1;
                    live.last_ping = Some(sent);
                }
                message = self.stream.next() => match message? {
                    Ok(ws::Message::Text(text)) => return Some(Frame::Text(Bytes::from(text))),
                    Ok(ws::Message::Binary(data)) => return Some(Frame::Binary(data)),
                    Ok(ws::Message::Close(close)) => {
                        return Some(Frame::Close(close.map(|close| Close {
                            code: close.code,
                            reason: Bytes::from(close.reason),
                        })));
                    }
                    Ok(ws::Message::Pong(_)) => {
                        let now = Instant::now();
                        self.deadline = tokio::time::Instant::from_std(now + self.settings.read_timeout);
                        let mut live = lock(&self.live);
                        live.pongs_received += 1;
                        live.last_pong = Some(now);
                        if let Some(ping) = live.last_ping {
                            let rtt = now - ping;
                            live.last_rtt = Some(rtt);
                            live.max_rtt = Some(live.max_rtt.map_or(rtt, |max| max.max(rtt)));
                        }
                    }
                    Ok(ws::Message::Ping(_)) => {}
                    Err(_) => return None,
                },
            }
        }
    }
}

/// The writing half of the client socket.
struct ClientTx {
    sink: Sink,
    live: Shared,
    limit: Duration,
}

impl ClientWrite for ClientTx {
    async fn send(&mut self, frame: Frame) -> Result<(), ()> {
        write(&self.sink, &self.live, self.limit, client_message(frame)).await
    }

    async fn refuse(&mut self, refusal: Refusal) -> Result<(), ()> {
        let message = ws::Message::Text(error_event(&refusal).into());
        write(&self.sink, &self.live, self.limit, message).await
    }
}

fn client_message(frame: Frame) -> ws::Message {
    match frame {
        Frame::Text(text) => match ws::Utf8Bytes::try_from(text) {
            Ok(text) => ws::Message::Text(text),
            // Upstream text messages are UTF-8 the transport validated.
            Err(_) => unreachable!("upstream text frames are UTF-8"),
        },
        Frame::Binary(data) => ws::Message::Binary(data),
        Frame::Close(close) => ws::Message::Close(close.map(|close| ws::CloseFrame {
            code: close.code,
            reason: ws::Utf8Bytes::try_from(close.reason).unwrap_or_default(),
        })),
    }
}

/// The Realtime `error` server event for a refused client message.
fn error_event(refusal: &Refusal) -> String {
    let detail = crate::http::gojson::Obj::new()
        .str("type", "invalid_request_error")
        .str("code", refusal.code)
        .str("message", refusal.message);
    let detail = match refusal.param {
        Some(param) => detail.str("param", param),
        None => detail.raw("param", "null"),
    }
    .finish();
    crate::http::gojson::Obj::new()
        .str("type", "error")
        .raw("error", &detail)
        .finish()
}

/// Runs one live session over the upgraded client socket and the open upstream socket, then
/// logs its end and records its usage.
pub(super) async fn run(opened: Opened, downstream: WebSocket, upstream: LiveSocket) {
    let started = Instant::now();
    let Opened {
        rt,
        settings,
        owner,
        events,
        model,
        upstream_model,
        credential,
        call_id,
        facts,
        hangup,
    } = opened;
    let request_id = facts.client.request_id.clone();
    let session_id = format!("rt_{}", uuid::Uuid::new_v4().simple());
    let (sink, stream) = downstream.split();
    let sink: Sink = Arc::new(AsyncMutex::new(sink));
    let live = Shared::default();
    let session = open(&rt, owner, &session_id, &request_id, events, model.as_deref());
    let (ended, held) = match session {
        Ok(session) => {
            let held = session.hold();
            let now = tokio::time::Instant::now();
            let read = ClientRx {
                stream,
                sink: sink.clone(),
                live: live.clone(),
                settings,
                ping: tokio::time::interval_at(now + settings.ping_interval, settings.ping_interval),
                deadline: now + settings.read_timeout,
                started,
                hangup,
            };
            let write = ClientTx {
                sink: sink.clone(),
                live: live.clone(),
                limit: settings.write_timeout,
            };
            let task = sessions::spawn(&rt, session, upstream.into_session(), read, write);
            match task.await {
                Ok(ended) => (ended, Some(held)),
                Err(_) => (failed(Terminal::ProducerLost), Some(held)),
            }
        }
        Err(error) => {
            tracing::error!(%error, "live session could not start");
            (failed(Terminal::StorageExhausted), None)
        }
    };
    close_client(&sink, &ended.end).await;
    let stop = lock(&live).stop;
    let (cause, initiator) = cause(&ended.end, stop);
    let counts = match held {
        Some(held) => settle(&rt, &facts, &credential, &upstream_model, held).await,
        None => Counts::default(),
    };
    let live = lock(&live);
    let close = match &ended.end {
        End::ClientClose(close) | End::UpstreamClose(close) => close.clone(),
        _ => None,
    };
    let ms = |d: Duration| d.as_millis() as u64;
    macro_rules! end_log {
        ($level:ident) => {
            tracing::$level!(
                request_id = %request_id,
                session_id = %session_id,
                call_id = call_id.as_deref(),
                auth_id = %credential.id,
                cause,
                initiator,
                close_code = close.as_ref().map(|c| u64::from(c.code)),
                close_reason = close.as_ref().map(|c| String::from_utf8_lossy(&c.reason).into_owned()),
                duration_ms = ms(started.elapsed()),
                idle_ms = match stop { Some(Stop::ReadTimeout { idle }) => Some(ms(idle)), _ => None },
                write_blocked_ms = match stop { Some(Stop::WriteTimeout { blocked }) => Some(ms(blocked)), _ => None },
                pings_sent = live.pings_sent,
                pongs_received = live.pongs_received,
                last_pong_rtt_ms = live.last_rtt.map(ms),
                max_pong_rtt_ms = live.max_rtt.map(ms),
                max_write_ms = ms(live.max_write),
                ping_interval_s = settings.ping_interval.as_secs(),
                read_timeout_s = settings.read_timeout.as_secs(),
                write_timeout_s = settings.write_timeout.as_secs(),
                max_sockets_per_key = settings.max_sockets_per_key,
                client_messages = counts.client[0],
                client_bytes = counts.client[1],
                client_decoded_bytes = counts.client[2],
                upstream_messages = counts.upstream[0],
                upstream_bytes = counts.upstream[1],
                upstream_decoded_bytes = counts.upstream[2],
                upstream_closed = !matches!(ended.end, End::UpstreamClose(_) | End::UpstreamGone),
                "live_session_end"
            )
        };
    }
    match level(cause) {
        tracing::Level::INFO => end_log!(info),
        tracing::Level::WARN => end_log!(warn),
        _ => end_log!(error),
    }
}

fn failed(terminal: Terminal) -> Ended {
    Ended {
        end: End::Failed(terminal),
        counters: Ok(()),
    }
}

/// The session runtime over a fresh session stream, pinned to the credential already dialled.
fn open(
    rt: &Runtime,
    owner: Owner,
    session_id: &str,
    request_id: &str,
    events: Events,
    model: Option<&str>,
) -> Result<sessions::Session<realtime::Session>, ck_translate::codec::Error> {
    let store = rt.streams().store()?;
    let mut stream = store.create()?;
    let identity = Identity {
        request_id: LocalId::new(request_id).or_else(|_| LocalId::new(session_id))?,
        stream_id: LocalId::new(session_id)?,
        attempt: 0,
        direction: Direction::Response,
    };
    let created = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let start = Start {
        id: session_id,
        model: model.unwrap_or_default(),
        created,
    };
    let admission = Admission {
        events,
        model: model.and_then(|model| LocalId::new(model).ok()),
    };
    let projector = realtime::Session::new(&mut stream, identity, &start, admission)?;
    Ok(sessions::Session::new(owner, stream, projector, ())?)
}

/// Closes the client socket once the session ended: a close the peer did not already
/// exchange (Go `websocketCloseDetails`), then the closing flush.
async fn close_client(sink: &Sink, end: &End) {
    let close = |code, reason: &'static str| {
        Some(ws::Message::Close(Some(ws::CloseFrame {
            code,
            reason: reason.into(),
        })))
    };
    let message = match end {
        // The transport answers a client close; the runtime forwarded the upstream's.
        End::ClientClose(_) | End::UpstreamClose(_) => None,
        End::ClientGone | End::UpstreamGone => close(1000, ""),
        // 1001 going away (RFC 6455 §7.4.1).
        End::Shutdown => close(1001, ""),
        End::Failed(_) => close(1011, "relay closed"),
    };
    let closing = async {
        let mut sink = sink.lock().await;
        if let Some(message) = message {
            let _ = sink.send(message).await;
        }
        let _ = sink.close().await;
    };
    let _ = tokio::time::timeout(sessions::CLOSE_WITHIN, closing).await;
}

/// The end log's `cause` and `initiator` (S1 live standard §2.2, "断开日志").
fn cause(end: &End, stop: Option<Stop>) -> (&'static str, &'static str) {
    match (end, stop) {
        (End::ClientClose(_), _) => ("client_close", "client"),
        (End::ClientGone, Some(Stop::Hangup)) => ("hangup", "client"),
        (End::ClientGone, Some(Stop::ReadTimeout { .. })) => ("client_read_timeout", "gateway"),
        (End::ClientGone, Some(Stop::WriteTimeout { .. })) => ("client_write_timeout", "gateway"),
        (End::ClientGone, None) => ("client_gone", "client"),
        (End::UpstreamClose(_), _) => ("upstream_close", "upstream"),
        (End::UpstreamGone, _) => ("upstream_gone", "upstream"),
        (End::Shutdown, _) => ("shutdown", "gateway"),
        (End::Failed(_), _) => ("relay_error", "gateway"),
    }
}

fn level(cause: &str) -> tracing::Level {
    match cause {
        "client_close" | "upstream_close" | "hangup" | "shutdown" => tracing::Level::INFO,
        "relay_error" => tracing::Level::ERROR,
        _ => tracing::Level::WARN,
    }
}

/// Messages, wire bytes and decoded media bytes per sender, from the final counters.
#[derive(Default)]
struct Counts {
    client: [u64; 3],
    upstream: [u64; 3],
}

/// Reads the ended session's stream once: every usage snapshot by scope, recorded as the
/// session's usage records, and the final counters for the end log. Drops `held` after.
async fn settle(
    rt: &Arc<Runtime>,
    facts: &Arc<Facts>,
    credential: &Credential,
    upstream_model: &str,
    held: sessions::Held,
) -> Counts {
    let mut stream = held.stream();
    let mut usage = Usage::default();
    let mut counts = Counts::default();
    let mut response = ResponseFacts::default();
    match rt.streams().store().and_then(|store| store.reader(stream.id(), 0)) {
        Ok(mut reader) => {
            while let Some(Read::Record(lease)) = reader.next().await {
                let read = internal::decode(&mut stream, lease).and_then(|(kind, doc)| match kind {
                    Kind::Usage => usage_snapshot::observe(&doc, &mut usage),
                    Kind::Operation => read_counters(&doc, &mut counts),
                    _ => Ok(()),
                });
                if let Err(error) = read {
                    tracing::warn!(%error, "live session usage reader stopped before the stream's terminal");
                    break;
                }
            }
        }
        Err(error) => tracing::warn!(%error, "live session stream unreadable for usage"),
    }
    drop(held);
    if !upstream_model.is_empty() {
        response.model = Some(upstream_model.to_owned());
    }
    let mut tracker = Tracker::start(rt, facts, credential, upstream_model);
    tracker.usage(&usage, &response);
    tracker.succeed();
    counts
}

/// The `realtime.counters` operation's per-sender counts; other operations are skipped.
fn read_counters(doc: &Document, counts: &mut Counts) -> ck_translate::codec::Result<()> {
    let root = doc.root();
    let Some(kind) = doc.member(root, "kind")? else {
        return Ok(());
    };
    if !matches!(doc.node(kind)?.value, Value::String(_) | Value::Text(_))
        || !doc.string_is(kind, "realtime.counters")?
    {
        return Ok(());
    }
    let value = doc.required(root, "value")?;
    for (name, slot) in [("client", &mut counts.client), ("upstream", &mut counts.upstream)] {
        let sender = doc.required(value, name)?;
        for (index, field) in ["messages", "bytes", "decoded_bytes"].into_iter().enumerate() {
            slot[index] = doc.unsigned(doc.required(sender, field)?)?;
        }
    }
    Ok(())
}
