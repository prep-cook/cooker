//! Live-session runtime (`docs/EXECUTION-CONTRACT.md` §12, Settled 55): the producer that
//! carries one Realtime, Live or WebRTC sideband session, and the admission of each client
//! message before it reaches the upstream (E-Q14).
//!
//! - One runtime-owned task per session ([`spawn`]) runs [`run`]: client and upstream
//!   messages are forwarded as received, each after its metadata projection, in arrival
//!   order per direction. No message is re-encoded and none is stored for reconnection.
//! - Every client message passes [`Projector::admit`] before anything else happens to it; a
//!   refused message is never sent upstream and the session continues.
//! - The session stream holds every projection record. Each response is its own exchange,
//!   from its upstream start to its end, reported to [`Observe`] with its [`Origin`]: the
//!   client's only when a protocol field of the start names a pending client request,
//!   otherwise the upstream's own (server VAD). Nothing is inferred from order or counts,
//!   and no pending request is ever dropped: past [`PENDING_REQUESTS`] a new one is refused.
//! - While a write is in flight both sides are still read, one message ahead, so a client or
//!   upstream close or loss ends the session and cancels the write. Runtime shutdown cancels
//!   it too.
//! - At the end the projector publishes its final counters, the session stream and every
//!   open exchange end with the session's terminal, and only then are the peers closed,
//!   each within [`CLOSE_WITHIN`]. Runtime shutdown waits for this before the store closes.
//!   Room for the counters is held from the session's start (`Writer::reserve_final`), so a
//!   session at its stream quota still publishes them; a failure is reported in [`Ended`].
//! - Nothing is registered for resume. The session stream stays in the stream cache, never
//!   evicted or expired (`Writer::keep_cached`), until the session has ended and its last
//!   accounting reader ([`Held`]) is gone; then it is released.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use ck_exec::session::{Close, Frame, FrameSink, Frames, LiveUpstream};
use ck_stream::{Terminal, Writer};
use ck_translate::codec::{
    self,
    live::{Opcode, Projector, Refusal, Route, Sender},
};
use futures_util::StreamExt;
use tokio_util::sync::CancellationToken;

use super::streams::Owner;
use crate::Runtime;

/// How long each closing write may take once a session ended; a stalled peer must not hold
/// the session task, which runtime shutdown waits for.
pub const CLOSE_WITHIN: Duration = Duration::from_secs(1);

/// Client requests a session waits on at once for a start that names them. A further request
/// is refused with [`TOO_MANY_PENDING`]; none is dropped, since a later start may name it.
pub const PENDING_REQUESTS: usize = 16;

/// The refusal of a client request past [`PENDING_REQUESTS`].
pub const TOO_MANY_PENDING: Refusal = Refusal {
    code: "too_many_pending_responses",
    message: "Too many requested responses have not started yet. Wait for one to start, then send the request again.",
    param: None,
};

/// Who asked for a response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Origin {
    /// Its start names a client request the upstream had not refused.
    Client,
    /// Anything else: the upstream started it on its own (server VAD).
    Server,
}

/// Learns of every response exchange of a session, e.g. to settle its usage.
pub trait Observe: Send + 'static {
    /// Response `id` started.
    fn opened(&mut self, owner: &Owner, origin: Origin, id: &str);
    /// Response `id` ended: `Terminal::Done` at its own end, otherwise the session's terminal.
    fn closed(&mut self, id: &str, terminal: Terminal);
}

impl Observe for () {
    fn opened(&mut self, _: &Owner, _: Origin, _: &str) {}
    fn closed(&mut self, _: &str, _: Terminal) {}
}

/// The reading half of a live session's client socket.
pub trait ClientRead: Send {
    /// The next client message; `None` when the connection ended without a close frame.
    /// Cancel-safe: [`run`] polls it while it writes and against shutdown.
    fn recv(&mut self) -> impl Future<Output = Option<Frame>> + Send;
}

/// The writing half of a live session's client socket; the relay owns its liveness and log.
pub trait ClientWrite: Send {
    /// Writes one upstream message to the client unchanged. `Err` means the client is gone.
    /// A session end may cancel it.
    fn send(&mut self, frame: Frame) -> impl Future<Output = Result<(), ()>> + Send;

    /// Answers a refused client message with the protocol's own `error` event. `Err` means
    /// the client is gone. A session end may cancel it.
    fn refuse(&mut self, refusal: Refusal) -> impl Future<Output = Result<(), ()>> + Send;
}

/// Why a session ended.
#[derive(Clone, Debug, PartialEq)]
pub enum End {
    /// The client sent a close frame; it is forwarded upstream.
    ClientClose(Option<Close>),
    /// The client connection ended without a close frame, or a write to it failed.
    ClientGone,
    /// The upstream sent a close frame; it is forwarded to the client.
    UpstreamClose(Option<Close>),
    /// The upstream connection ended without a close frame.
    UpstreamGone,
    /// The runtime shut down.
    Shutdown,
    /// A projection could not be stored or an upstream write failed; the terminal says which.
    Failed(Terminal),
}

impl End {
    /// The terminal the session stream and every open exchange end with.
    pub fn terminal(&self) -> Terminal {
        match self {
            // ponytail: Settled 55 names `Terminal::ClientDisconnected` (`client_disconnect`)
            // for these; W2 adds that variant and changes this arm.
            Self::ClientClose(_) | Self::ClientGone => Terminal::Cancelled,
            Self::UpstreamClose(_) => Terminal::Done,
            Self::UpstreamGone => Terminal::UpstreamError,
            Self::Shutdown => Terminal::Shutdown,
            Self::Failed(terminal) => *terminal,
        }
    }

    /// The close frame the upstream gets; `None` when the upstream itself ended.
    fn upstream_close(&self) -> Option<Option<Close>> {
        let close = |code| {
            Some(Some(Close {
                code,
                reason: Default::default(),
            }))
        };
        match self {
            Self::UpstreamClose(_) | Self::UpstreamGone => None,
            Self::ClientClose(close) => Some(close.clone()),
            // 1001 going away and 1011 internal error (RFC 6455 §7.4.1).
            Self::ClientGone | Self::Shutdown => close(1001),
            Self::Failed(_) => close(1011),
        }
    }
}

/// How a session ended, and whether its final counters were published.
#[derive(Clone, Debug, PartialEq)]
#[must_use]
pub struct Ended {
    pub end: End,
    /// `Err` names the terminal the session stream took instead of `end.terminal()`, because
    /// the projector's final counters could not be stored.
    pub counters: Result<(), Terminal>,
}

/// What admission made of one client message.
#[derive(Debug)]
pub enum Admitted {
    /// Projected; the caller sends it upstream unchanged.
    Forward,
    /// Refused: nothing was projected, and it must not be sent.
    Refused(Refusal),
}

/// Keeps a session's stream for an accounting reader. The stream is released once the
/// session has ended and every `Held` is dropped.
#[derive(Clone)]
pub struct Held(Arc<Release>);

/// Keeps the session stream cached from the session's first fallible step, and discards it
/// when dropped, so every exit releases what it kept.
struct Release(Writer);

impl Release {
    fn keep(session: &Writer) -> Self {
        session.keep_cached();
        Self(session.allocator())
    }
}

impl Drop for Release {
    fn drop(&mut self) {
        self.0.finish(Terminal::Gone);
    }
}

impl Held {
    /// An allocation handle on the session stream, e.g. to attach a reader.
    pub fn stream(&self) -> Writer {
        self.0.0.allocator()
    }
}

/// One live session's projector and streams. Pinned to the credential its upstream was
/// opened on; nothing here chooses or rotates one.
pub struct Session<P, O = ()> {
    projector: P,
    lifecycle: Lifecycle<O>,
    held: Held,
}

struct Lifecycle<O> {
    owner: Owner,
    session: Writer,
    /// Responses started and not ended, in start order; a session has few at a time.
    open: Vec<Box<str>>,
    /// Client requests no start has named and the upstream has not refused.
    pending: VecDeque<Box<str>>,
    observe: O,
}

impl<P: Projector, O: Observe> Session<P, O> {
    /// A session over `session`, a fresh stream from the runtime's store. Holds room for the
    /// final counters; fails when the stream has none left. The stream stays cached until the
    /// session has ended and the last [`Held`] drops, so no other exchange evicts its records.
    pub fn new(owner: Owner, mut session: Writer, projector: P, observe: O) -> Result<Self, ck_stream::Error> {
        let held = Held(Arc::new(Release::keep(&session)));
        session.reserve_final(P::FINAL_RECORD_BYTES)?;
        Ok(Self {
            projector,
            lifecycle: Lifecycle {
                owner,
                session,
                open: Vec::new(),
                pending: VecDeque::new(),
                observe,
            },
            held,
        })
    }

    /// Keeps the session stream readable after the session ends, until the `Held` drops.
    pub fn hold(&self) -> Held {
        self.held.clone()
    }

    /// Admits and projects one client text or binary message. Only [`Admitted::Forward`]
    /// may be sent upstream; a refused message reaches neither the stream nor the upstream.
    pub fn client(&mut self, frame: &Frame) -> Result<Admitted, End> {
        let Some((opcode, payload)) = frame.message() else {
            return Err(End::ClientClose(None));
        };
        if let Err(refusal) = self.projector.admit(&mut self.lifecycle, opcode, payload) {
            return Ok(Admitted::Refused(refusal));
        }
        self.project(Sender::Client, opcode, payload)?;
        Ok(Admitted::Forward)
    }

    /// Projects one upstream text or binary message before the caller forwards it.
    pub fn upstream(&mut self, frame: &Frame) -> Result<(), End> {
        match frame.message() {
            Some((opcode, payload)) => self.project(Sender::Upstream, opcode, payload),
            None => Ok(()),
        }
    }

    fn project(&mut self, sender: Sender, opcode: Opcode, payload: &[u8]) -> Result<(), End> {
        self.projector
            .project(&mut self.lifecycle, sender, opcode, payload)
            .map_err(failed)
    }

    /// Publishes the final counters into the room held since the start, closes every open
    /// exchange and ends the session stream with `terminal`. When the counters cannot be
    /// stored, the stream and the exchanges end with the failure's terminal instead, returned
    /// as `Err`. The stream is released once no [`Held`] remains.
    pub fn finish(self, terminal: Terminal) -> Result<(), Terminal> {
        let Self {
            mut projector,
            mut lifecycle,
            held,
        } = self;
        lifecycle.session.release_final();
        let counters = projector.finish(&mut lifecycle.session).map_err(lost);
        let terminal = counters.err().unwrap_or(terminal);
        for id in lifecycle.open.drain(..) {
            lifecycle.observe.closed(&id, terminal);
        }
        lifecycle.session.finish(terminal);
        drop(held);
        counters
    }
}

fn failed(error: codec::Error) -> End {
    End::Failed(lost(error))
}

/// The terminal a projection failure ends the session with.
fn lost(error: codec::Error) -> Terminal {
    match error {
        codec::Error::Storage(ck_stream::Error::StorageExhausted) => Terminal::StorageExhausted,
        _ => Terminal::UpstreamError,
    }
}

impl<O: Observe> Route for Lifecycle<O> {
    fn session(&mut self) -> &mut Writer {
        &mut self.session
    }

    fn requested(&mut self, request: &str) -> Result<(), Refusal> {
        if self.pending.len() == PENDING_REQUESTS {
            return Err(TOO_MANY_PENDING);
        }
        self.pending.push_back(request.into());
        Ok(())
    }

    fn rejected(&mut self, request: &str) {
        if let Some(index) = self.pending.iter().position(|pending| **pending == *request) {
            self.pending.remove(index);
        }
    }

    fn started(&mut self, id: &str, request: Option<&str>) {
        if self.open.iter().any(|open| **open == *id) {
            return;
        }
        let named = request.and_then(|request| self.pending.iter().position(|pending| **pending == *request));
        let origin = match named {
            Some(index) => {
                self.pending.remove(index);
                Origin::Client
            }
            None => Origin::Server,
        };
        self.open.push(id.into());
        self.observe.opened(&self.owner, origin, id);
    }

    fn done(&mut self, id: &str) {
        if let Some(index) = self.open.iter().position(|open| **open == *id) {
            self.open.remove(index);
            self.observe.closed(id, Terminal::Done);
        }
    }
}

/// Drives one session until it ends: client messages pass admission and go upstream,
/// upstream messages go to the client, each after its projection. Shutdown, or a close or
/// loss on either side, cancels whatever write is in flight. The session finishes (final
/// counters, then [`End::terminal`]) before either peer is closed.
pub async fn run<P, O, S, R, W>(
    mut session: Session<P, O>,
    upstream: LiveUpstream<S>,
    read: R,
    mut write: W,
    shutdown: CancellationToken,
) -> Ended
where
    P: Projector,
    O: Observe,
    S: FrameSink,
    R: ClientRead,
    W: ClientWrite,
{
    let LiveUpstream { mut sink, frames } = upstream;
    let mut sides = Sides {
        read,
        frames,
        client: None,
        upstream: None,
    };
    let end = tokio::select! {
        biased;
        () = shutdown.cancelled() => End::Shutdown,
        end = forward(&mut session, &mut sink, &mut write, &mut sides) => end,
    };
    let counters = session.finish(end.terminal());
    if let Some(close) = end.upstream_close() {
        let _ = tokio::time::timeout(CLOSE_WITHIN, sink.send(Frame::Close(close))).await;
    }
    if let End::UpstreamClose(close) = &end {
        let _ = tokio::time::timeout(CLOSE_WITHIN, write.send(Frame::Close(close.clone()))).await;
    }
    Ended { end, counters }
}

/// Both incoming sides, each with at most one message read ahead while a write is in flight.
struct Sides<R> {
    read: R,
    frames: Frames,
    client: Option<Frame>,
    upstream: Option<Frame>,
}

enum Next {
    Client(Frame),
    Upstream(Frame),
}

fn from_client(frame: Option<Frame>) -> Result<Frame, End> {
    match frame {
        None => Err(End::ClientGone),
        Some(Frame::Close(close)) => Err(End::ClientClose(close)),
        Some(frame) => Ok(frame),
    }
}

fn from_upstream(frame: Option<Frame>) -> Result<Frame, End> {
    match frame {
        None => Err(End::UpstreamGone),
        Some(Frame::Close(close)) => Err(End::UpstreamClose(close)),
        Some(frame) => Ok(frame),
    }
}

impl<R: ClientRead> Sides<R> {
    /// The next message to forward, read-ahead messages first.
    async fn next(&mut self) -> Result<Next, End> {
        if let Some(frame) = self.client.take() {
            return Ok(Next::Client(frame));
        }
        if let Some(frame) = self.upstream.take() {
            return Ok(Next::Upstream(frame));
        }
        tokio::select! {
            biased;
            frame = self.read.recv() => from_client(frame).map(Next::Client),
            frame = self.frames.next() => from_upstream(frame).map(Next::Upstream),
        }
    }

    /// Awaits `write` while reading each side one message ahead, so a close or loss on
    /// either side ends the session and cancels the write.
    // ponytail: one message of read-ahead per side; behind it the socket is not read until
    // the write completes, and W2's pong-only read deadline ends a session stuck there.
    async fn during<T>(&mut self, write: impl Future<Output = T>) -> Result<T, End> {
        tokio::pin!(write);
        loop {
            tokio::select! {
                biased;
                done = &mut write => return Ok(done),
                frame = self.read.recv(), if self.client.is_none() => self.client = Some(from_client(frame)?),
                frame = self.frames.next(), if self.upstream.is_none() => self.upstream = Some(from_upstream(frame)?),
            }
        }
    }
}

/// The forwarding loop; it returns only when the session ends.
async fn forward<P, O, S, R, W>(session: &mut Session<P, O>, sink: &mut S, write: &mut W, sides: &mut Sides<R>) -> End
where
    P: Projector,
    O: Observe,
    S: FrameSink,
    R: ClientRead,
    W: ClientWrite,
{
    loop {
        let next = match sides.next().await {
            Ok(next) => next,
            Err(end) => return end,
        };
        let ended = match next {
            Next::Client(frame) => match session.client(&frame) {
                Err(end) => Some(end),
                Ok(Admitted::Refused(refusal)) => match sides.during(write.refuse(refusal)).await {
                    Ok(Ok(())) => None,
                    Ok(Err(())) => Some(End::ClientGone),
                    Err(end) => Some(end),
                },
                Ok(Admitted::Forward) => match sides.during(sink.send(frame)).await {
                    Ok(Ok(())) => None,
                    Ok(Err(_)) => Some(End::Failed(Terminal::UpstreamError)),
                    Err(end) => Some(end),
                },
            },
            Next::Upstream(frame) => match session.upstream(&frame) {
                Err(end) => Some(end),
                Ok(()) => match sides.during(write.send(frame)).await {
                    Ok(Ok(())) => None,
                    Ok(Err(())) => Some(End::ClientGone),
                    Err(end) => Some(end),
                },
            },
        };
        if let Some(end) = ended {
            return end;
        }
    }
}

/// Starts `run` as a runtime-owned session task. Runtime shutdown ends the session and waits
/// for it to finish before the store closes.
pub fn spawn<P, O, S, R, W>(
    rt: &Runtime,
    session: Session<P, O>,
    upstream: LiveUpstream<S>,
    read: R,
    write: W,
) -> tokio::task::JoinHandle<Ended>
where
    P: Projector + Send + 'static,
    O: Observe,
    S: FrameSink,
    R: ClientRead + 'static,
    W: ClientWrite + 'static,
{
    rt.streams()
        .spawn_session(|shutdown| run(session, upstream, read, write, shutdown))
}
