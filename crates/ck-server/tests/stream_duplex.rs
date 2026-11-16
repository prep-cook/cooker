//! The live-session runtime (`ck_server::inference::sessions`): per-message admission, the
//! origin of each response exchange, how every way a session ends (shutdown during a
//! stalled write included) finishes its stream before the upstream closes, and the release
//! of a finished session's storage. Loopback halves and private stream stores only.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use ck_core::exec::{Caller, ExecError};
use ck_exec::session::{Close, Frame, FrameSink, LiveUpstream};
use ck_server::inference::sessions::{self, ClientRead, ClientWrite, End, Ended, Held, Observe, Origin, Session};
use ck_server::inference::streams::{Owner, Registry};
use ck_stream::{Read, Store, Terminal, Writer};
use ck_translate::codec::{
    self,
    live::{Opcode, Projector, Refusal, Route, Sender},
};
use futures_util::{FutureExt, StreamExt};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// A projector over `{"type", "id", "event_id", "request"}` messages. It refuses `type:
/// "forbidden"`, cannot store `type: "unstorable"`, projects `type: "fill"` as a [`FILL`]-byte
/// record and appends `<sender>:<type>` to the session stream otherwise; after `type:
/// "unstorable_counters"` its final counters cannot be stored. Lifecycle: admitting a client
/// `response.create` requests its `event_id`, upstream `error` refuses its `event_id`,
/// `response.created` starts `id` naming `request`, and `response.done` ends `id`.
struct Types {
    counters: bool,
}

fn field(payload: &[u8], name: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(payload).ok()?;
    Some(value.get(name)?.as_str()?.to_owned())
}

/// The record a `fill` message projects to, in bytes.
const FILL: usize = 10_000;

const FORBIDDEN: Refusal = Refusal {
    code: "unknown_event_type",
    message: "refused",
    param: Some("type"),
};

impl Projector for Types {
    const FINAL_RECORD_BYTES: usize = b"counters".len();

    fn admit(&mut self, route: &mut impl Route, _: Opcode, payload: &[u8]) -> Result<(), Refusal> {
        match (field(payload, "type").as_deref(), field(payload, "event_id")) {
            (Some("forbidden"), _) => Err(FORBIDDEN),
            (Some("response.create"), Some(event)) => route.requested(&event),
            _ => Ok(()),
        }
    }

    fn project(&mut self, route: &mut impl Route, sender: Sender, _: Opcode, payload: &[u8]) -> codec::Result<()> {
        let kind = field(payload, "type").unwrap_or_else(|| "binary".into());
        if kind == "unstorable" {
            return Err(ck_stream::Error::StorageExhausted.into());
        }
        if kind == "unstorable_counters" {
            self.counters = false;
        }
        if kind == "fill" {
            route.session().append(&[b'x'; FILL])?;
            return Ok(());
        }
        let side = if sender == Sender::Client { "client" } else { "upstream" };
        route.session().append(format!("{side}:{kind}").as_bytes())?;
        let (id, event, request) = (
            field(payload, "id"),
            field(payload, "event_id"),
            field(payload, "request"),
        );
        match (sender, kind.as_str(), id, event) {
            (Sender::Upstream, "error", _, Some(event)) => route.rejected(&event),
            (Sender::Upstream, "response.created", Some(id), _) => route.started(&id, request.as_deref()),
            (Sender::Upstream, "response.done", Some(id), _) => route.done(&id),
            _ => {}
        }
        Ok(())
    }

    fn finish(&mut self, session: &mut Writer) -> codec::Result<()> {
        if !self.counters {
            return Err(ck_stream::Error::StorageExhausted.into());
        }
        session.append(b"counters")?;
        Ok(())
    }
}

/// One origin row: client messages, upstream messages, exchanges reported, client requests
/// refused.
type Case = (Vec<String>, Vec<String>, Vec<Exchange>, usize);

/// One exchange event as [`Observe`] reports it.
#[derive(Clone, Debug, PartialEq)]
enum Exchange {
    Opened(Origin, String),
    Closed(String, Terminal),
}

#[derive(Clone, Default)]
struct Exchanges(Arc<Mutex<Vec<Exchange>>>);

impl Observe for Exchanges {
    fn opened(&mut self, _: &Owner, origin: Origin, id: &str) {
        self.0.lock().unwrap().push(Exchange::Opened(origin, id.into()));
    }
    fn closed(&mut self, id: &str, terminal: Terminal) {
        self.0.lock().unwrap().push(Exchange::Closed(id.into(), terminal));
    }
}

/// Which write never completes.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Stall {
    Never,
    /// Every write completes only after yielding once, so the session reads ahead meanwhile.
    Slow,
    /// An upstream write of a message whose type is `stall`.
    UpstreamMessage,
    /// The upstream close.
    UpstreamClose,
    /// A client write of a message whose type is `stall`.
    ClientMessage,
}

fn stalls(frame: &Frame) -> bool {
    frame
        .message()
        .is_some_and(|(_, payload)| field(payload, "type").as_deref() == Some("stall"))
}

/// The upstream half: records each write as it starts and, at a close, the session stream's
/// terminal by then.
struct Upstream {
    stall: Stall,
    sent: Arc<Mutex<Vec<Frame>>>,
    session: Writer,
    at_close: Arc<Mutex<Option<Option<Terminal>>>>,
}

impl Clone for Upstream {
    fn clone(&self) -> Self {
        Self {
            stall: self.stall,
            sent: self.sent.clone(),
            session: self.session.allocator(),
            at_close: self.at_close.clone(),
        }
    }
}

impl FrameSink for Upstream {
    async fn send(&mut self, frame: Frame) -> Result<(), ExecError> {
        let close = matches!(frame, Frame::Close(_));
        if close {
            *self.at_close.lock().unwrap() = Some(self.session.cancelled().now_or_never());
        }
        let stalled =
            (close && self.stall == Stall::UpstreamClose) || (self.stall == Stall::UpstreamMessage && stalls(&frame));
        self.sent.lock().unwrap().push(frame);
        if stalled {
            std::future::pending::<()>().await;
        }
        if self.stall == Stall::Slow {
            tokio::task::yield_now().await;
        }
        Ok(())
    }
}

/// The client half: scripted messages in, forwarded messages and refusals out.
struct Peer {
    stall: Stall,
    received: Arc<Mutex<Vec<Frame>>>,
    refused: Arc<Mutex<Vec<Refusal>>>,
}

struct Inbox(mpsc::UnboundedReceiver<Frame>);

impl ClientRead for Inbox {
    async fn recv(&mut self) -> Option<Frame> {
        self.0.recv().await
    }
}

impl ClientWrite for Peer {
    async fn send(&mut self, frame: Frame) -> Result<(), ()> {
        let stalled = self.stall == Stall::ClientMessage && stalls(&frame);
        self.received.lock().unwrap().push(frame);
        if stalled {
            std::future::pending::<()>().await;
        }
        if self.stall == Stall::Slow {
            tokio::task::yield_now().await;
        }
        Ok(())
    }
    async fn refuse(&mut self, refusal: Refusal) -> Result<(), ()> {
        self.refused.lock().unwrap().push(refusal);
        if self.stall == Stall::Slow {
            tokio::task::yield_now().await;
        }
        Ok(())
    }
}

fn store() -> (Store, std::path::PathBuf) {
    quota_store(None)
}

/// A private store whose streams hold at most `max_frames` frames, when given.
fn quota_store(max_frames: Option<u32>) -> (Store, std::path::PathBuf) {
    pool_store(64, max_frames)
}

/// A private store of `frames` pool frames whose streams hold at most `max_frames`.
fn pool_store(frames: usize, max_frames: Option<u32>) -> (Store, std::path::PathBuf) {
    let directory = std::env::temp_dir().join(format!("ck-sessions-{}", uuid::Uuid::new_v4()));
    let mut config = ck_stream::Config::new(directory.clone());
    config.pool_bytes = Some(frames * ck_stream::FRAME_BYTES);
    config.max_frames = max_frames;
    config.max_event_bytes = 2 * ck_stream::FRAME_BYTES;
    config.workers = 1;
    (Store::open(config).unwrap(), directory)
}

/// The cause of a session that published its final counters.
fn ended(ended: Ended) -> End {
    assert_eq!(
        ended.counters,
        Ok(()),
        "{:?}: the final counters are published",
        ended.end
    );
    ended.end
}

/// Every record of the stream `id` and its terminal.
async fn records(store: &Store, id: u64) -> (Vec<String>, Terminal) {
    drain(store.reader(id, 0).unwrap()).await
}

/// Every record `reader` has left and the stream's terminal.
async fn drain(mut reader: ck_stream::Reader) -> (Vec<String>, Terminal) {
    let mut records = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(5), reader.next()).await {
            Ok(Some(Read::Record(lease))) => {
                let mut bytes = Vec::new();
                lease.visit(|chunk| bytes.extend_from_slice(chunk)).unwrap();
                lease.acknowledge();
                records.push(String::from_utf8(bytes).unwrap());
            }
            Ok(Some(Read::Terminal(terminal))) => return (records, terminal),
            other => panic!("stream ended without a terminal: {:?}", other.is_ok()),
        }
    }
}

fn owner() -> Owner {
    Owner {
        caller: Caller {
            principal: "sk-test".into(),
            source: "authorization",
            provider: "config-inline".into(),
        },
        scope: None,
    }
}

fn text(payload: &str) -> Frame {
    Frame::Text(Bytes::copy_from_slice(payload.as_bytes()))
}

/// One running session: its client inbox, upstream script and both halves' records.
struct Running {
    client: mpsc::UnboundedSender<Frame>,
    upstream: mpsc::UnboundedSender<Frame>,
    received: Arc<Mutex<Vec<Frame>>>,
    refused: Arc<Mutex<Vec<Refusal>>>,
    sink: Upstream,
    exchanges: Exchanges,
    session: Writer,
    held: Held,
    task: tokio::task::JoinHandle<Ended>,
}

/// Where a session task runs: on its own with a test-owned shutdown, or on a registry.
enum Launch<'a> {
    Alone(CancellationToken),
    Registry(&'a Registry),
}

fn start(store: &Store, stall: Stall, launch: Launch<'_>) -> Running {
    let exchanges = Exchanges::default();
    let session = Session::new(
        owner(),
        store.create().unwrap(),
        Types { counters: true },
        exchanges.clone(),
    )
    .unwrap();
    let held = session.hold();
    let stream = held.stream();
    let sink = Upstream {
        stall,
        sent: Arc::default(),
        session: stream.allocator(),
        at_close: Arc::default(),
    };
    let (client, inbox) = mpsc::unbounded_channel();
    let (upstream, frames) = mpsc::unbounded_channel();
    let peer = Peer {
        stall,
        received: Arc::default(),
        refused: Arc::default(),
    };
    let (received, refused) = (peer.received.clone(), peer.refused.clone());
    let live = LiveUpstream {
        sink: sink.clone(),
        frames: futures_util::stream::unfold(
            frames,
            |mut frames| async move { frames.recv().await.map(|f| (f, frames)) },
        )
        .boxed(),
    };
    let run = move |shutdown| sessions::run(session, live, Inbox(inbox), peer, shutdown);
    let task = match launch {
        Launch::Alone(shutdown) => tokio::spawn(run(shutdown)),
        Launch::Registry(registry) => registry.spawn_session(run),
    };
    Running {
        client,
        upstream,
        received,
        refused,
        sink,
        exchanges,
        session: stream,
        held,
        task,
    }
}

async fn until(what: &str, done: impl Fn() -> bool) {
    for _ in 0..1000 {
        if done() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("never happened: {what}");
}

/// Waits until the session forwarded everything the client sent so far.
async fn settle(running: &Running) {
    let marker = text(r#"{"type":"marker"}"#);
    running.client.send(marker.clone()).unwrap();
    until("marker forwarded", || {
        running.sink.sent.lock().unwrap().contains(&marker)
    })
    .await;
    running.sink.sent.lock().unwrap().retain(|frame| *frame != marker);
}

#[tokio::test]
async fn admission_refuses_a_unit_before_anything_is_sent_or_projected() {
    // Slow writes make the session read further client messages while each write is in
    // flight; those must still be admitted, projected and sent in order.
    for mode in [Stall::Never, Stall::Slow] {
        let (store, directory) = store();
        let running = start(&store, mode, Launch::Alone(CancellationToken::new()));
        // (client message, forwarded upstream)
        let cases = [
            (text(r#"{"type":"input_audio_buffer.append","audio":"AAAA"}"#), true),
            (text(r#"{"type":"forbidden"}"#), false),
            (Frame::Binary(Bytes::from_static(&[0, 1, 2])), true),
            (text(r#"{"type":"forbidden","audio":"AAAA"}"#), false),
            (text(r#"{ "type" :"session.update" }"#), true),
        ];
        for (frame, _) in &cases {
            running.client.send(frame.clone()).unwrap();
        }
        settle(&running).await;
        let forwarded: Vec<Frame> = cases.iter().filter(|(_, sent)| *sent).map(|(f, _)| f.clone()).collect();
        assert_eq!(
            *running.sink.sent.lock().unwrap(),
            forwarded,
            "{mode:?}: admitted units arrive unchanged and in order; refused ones never"
        );
        assert_eq!(
            *running.refused.lock().unwrap(),
            [FORBIDDEN, FORBIDDEN],
            "{mode:?}: each refusal is answered once"
        );
        let Running { client, task, held, .. } = running;
        drop(client);
        assert_eq!(ended(task.await.unwrap()), End::ClientGone);
        let (records, _) = records(&store, held.stream().id()).await;
        let expected = [
            "client:input_audio_buffer.append",
            "client:binary",
            "client:session.update",
            "client:marker",
            "counters",
        ];
        assert_eq!(records, expected, "{mode:?}: a refused unit leaves no projection");
        std::fs::remove_dir_all(&directory).unwrap();
    }
}

#[tokio::test]
async fn a_response_is_the_clients_only_when_its_start_names_a_pending_request() {
    use Exchange::{Closed, Opened};
    use Origin::{Client, Server};
    // (client messages, upstream messages, exchanges reported; the session ends by upstream loss)
    let rows: [(&[&str], &[&str], Vec<Exchange>); 5] = [
        (
            // A server VAD start arriving before the client's response is the upstream's own.
            &[r#"{"type":"response.create","event_id":"c1"}"#],
            &[
                r#"{"type":"response.created","id":"vad"}"#,
                r#"{"type":"response.created","id":"answer","request":"c1"}"#,
                r#"{"type":"response.done","id":"answer"}"#,
            ],
            vec![
                Opened(Server, "vad".into()),
                Opened(Client, "answer".into()),
                Closed("answer".into(), Terminal::Done),
                Closed("vad".into(), Terminal::UpstreamError),
            ],
        ),
        (
            // The upstream refused c1, so nothing answers it.
            &[r#"{"type":"response.create","event_id":"c1"}"#],
            &[
                r#"{"type":"error","event_id":"c1"}"#,
                r#"{"type":"response.created","id":"vad"}"#,
                r#"{"type":"response.created","id":"late","request":"c1"}"#,
            ],
            vec![
                Opened(Server, "vad".into()),
                Opened(Server, "late".into()),
                Closed("vad".into(), Terminal::UpstreamError),
                Closed("late".into(), Terminal::UpstreamError),
            ],
        ),
        (
            // A start naming a request the client never sent.
            &[],
            &[r#"{"type":"response.created","id":"r1","request":"c9"}"#],
            vec![
                Opened(Server, "r1".into()),
                Closed("r1".into(), Terminal::UpstreamError),
            ],
        ),
        (
            // A request answers one start only; a repeated start of an open id is one exchange.
            &[r#"{"type":"response.create","event_id":"c1"}"#],
            &[
                r#"{"type":"response.created","id":"r1","request":"c1"}"#,
                r#"{"type":"response.created","id":"r1","request":"c1"}"#,
                r#"{"type":"response.created","id":"r2","request":"c1"}"#,
                r#"{"type":"response.done","id":"r1"}"#,
                r#"{"type":"response.done","id":"r2"}"#,
            ],
            vec![
                Opened(Client, "r1".into()),
                Opened(Server, "r2".into()),
                Closed("r1".into(), Terminal::Done),
                Closed("r2".into(), Terminal::Done),
            ],
        ),
        (
            // A request without a correlation can never be named.
            &[r#"{"type":"response.create"}"#],
            &[
                r#"{"type":"response.created","id":"r1"}"#,
                r#"{"type":"response.done","id":"r1"}"#,
            ],
            vec![Opened(Server, "r1".into()), Closed("r1".into(), Terminal::Done)],
        ),
    ];
    let own = |messages: &[&str]| messages.iter().map(|m| m.to_string()).collect::<Vec<_>>();
    let mut cases: Vec<Case> = rows
        .into_iter()
        .map(|(client, upstream, expected)| (own(client), own(upstream), expected, 0))
        .collect();
    // One request past the pending limit is refused, never an earlier one dropped: starts
    // naming c0 and c1 stay the client's, and c16 was never sent.
    let requests =
        (0..=sessions::PENDING_REQUESTS).map(|n| format!(r#"{{"type":"response.create","event_id":"c{n}"}}"#));
    let answers = ["vad", "answer0", "answer1", "answer16"];
    cases.push((
        requests.collect(),
        vec![
            r#"{"type":"response.created","id":"vad"}"#.into(),
            r#"{"type":"response.created","id":"answer0","request":"c0"}"#.into(),
            r#"{"type":"response.created","id":"answer1","request":"c1"}"#.into(),
            r#"{"type":"response.created","id":"answer16","request":"c16"}"#.into(),
        ],
        [Server, Client, Client, Server]
            .into_iter()
            .zip(answers)
            .map(|(origin, id)| Opened(origin, id.into()))
            .chain(answers.map(|id| Closed(id.into(), Terminal::UpstreamError)))
            .collect(),
        1,
    ));
    // Slow writes make the session read each side ahead while the other side's write is in
    // flight; nothing read ahead may be lost or reordered.
    let cases = cases
        .into_iter()
        .flat_map(|case| [(Stall::Never, case.clone()), (Stall::Slow, case)]);
    for (index, (mode, (from_client, from_upstream, expected, refusals))) in cases.enumerate() {
        let index = format!("{index} {mode:?}");
        let (store, directory) = store();
        let running = start(&store, mode, Launch::Alone(CancellationToken::new()));
        for message in &from_client {
            running.client.send(text(message)).unwrap();
        }
        settle(&running).await;
        for message in &from_upstream {
            running.upstream.send(text(message)).unwrap();
        }
        let Running {
            client: _client,
            upstream,
            received,
            refused,
            exchanges,
            task,
            ..
        } = running;
        drop(upstream);
        assert_eq!(ended(task.await.unwrap()), End::UpstreamGone, "case {index}");
        assert_eq!(
            received.lock().unwrap().len(),
            from_upstream.len(),
            "case {index}: all forwarded"
        );
        assert_eq!(*exchanges.0.lock().unwrap(), expected, "case {index}");
        let refused = refused.lock().unwrap().clone();
        assert_eq!(refused, vec![sessions::TOO_MANY_PENDING; refusals], "case {index}");
        std::fs::remove_dir_all(&directory).unwrap();
    }
}

#[derive(Clone, Copy, Debug)]
enum Cause {
    ClientClose,
    ClientGone,
    UpstreamClose,
    UpstreamGone,
    Shutdown,
    Unstorable,
    /// The client sends more than the stream quota holds.
    Exhausted,
    /// Shutdown after the projector lost the means to store its counters.
    CountersUnstorable,
}

#[tokio::test]
async fn every_end_finishes_the_session_before_the_upstream_closes() {
    let close = |code, reason: &'static [u8]| {
        Some(Close {
            code,
            reason: Bytes::from_static(reason),
        })
    };
    let (going_away, failed, bye) = (close(1001, b""), close(1011, b""), close(1000, b"bye"));
    // (how the session ends, which write stalls, its end, the terminal, the close the upstream gets)
    let cases = [
        (
            Cause::ClientClose,
            Stall::Never,
            End::ClientClose(bye.clone()),
            Terminal::ClientDisconnected,
            Some(bye.clone()),
        ),
        (
            Cause::ClientGone,
            Stall::Never,
            End::ClientGone,
            Terminal::ClientDisconnected,
            Some(going_away.clone()),
        ),
        (
            Cause::UpstreamClose,
            Stall::Never,
            End::UpstreamClose(None),
            Terminal::Done,
            Some(None),
        ),
        (
            Cause::UpstreamGone,
            Stall::Never,
            End::UpstreamGone,
            Terminal::UpstreamError,
            None,
        ),
        (
            Cause::Shutdown,
            Stall::Never,
            End::Shutdown,
            Terminal::Shutdown,
            Some(going_away.clone()),
        ),
        (
            Cause::Unstorable,
            Stall::Never,
            End::Failed(Terminal::StorageExhausted),
            Terminal::StorageExhausted,
            Some(failed.clone()),
        ),
        // A write in flight never hides the end: it is cancelled.
        (
            Cause::Shutdown,
            Stall::UpstreamMessage,
            End::Shutdown,
            Terminal::Shutdown,
            Some(going_away.clone()),
        ),
        (
            Cause::Shutdown,
            Stall::ClientMessage,
            End::Shutdown,
            Terminal::Shutdown,
            Some(going_away.clone()),
        ),
        (
            Cause::ClientGone,
            Stall::UpstreamMessage,
            End::ClientGone,
            Terminal::ClientDisconnected,
            Some(going_away.clone()),
        ),
        (
            Cause::ClientClose,
            Stall::UpstreamMessage,
            End::ClientClose(bye.clone()),
            Terminal::ClientDisconnected,
            Some(bye.clone()),
        ),
        (
            Cause::UpstreamGone,
            Stall::ClientMessage,
            End::UpstreamGone,
            Terminal::UpstreamError,
            None,
        ),
        // A projection past the stream quota ends the session, and the counters still fit.
        (
            Cause::Exhausted,
            Stall::Never,
            End::Failed(Terminal::StorageExhausted),
            Terminal::StorageExhausted,
            Some(failed.clone()),
        ),
        // Counters that cannot be stored are reported, and the stream says why it ended.
        (
            Cause::CountersUnstorable,
            Stall::Never,
            End::Shutdown,
            Terminal::StorageExhausted,
            Some(going_away.clone()),
        ),
        // An idle session whose upstream never takes the close still ends.
        (
            Cause::Shutdown,
            Stall::UpstreamClose,
            End::Shutdown,
            Terminal::Shutdown,
            Some(going_away.clone()),
        ),
    ];
    for (cause, stall, end, terminal, close) in cases {
        let (store, directory) = quota_store(matches!(cause, Cause::Exhausted).then_some(4));
        let shutdown = CancellationToken::new();
        let running = start(&store, stall, Launch::Alone(shutdown.clone()));
        running
            .client
            .send(text(r#"{"type":"response.create","event_id":"c1"}"#))
            .unwrap();
        settle(&running).await;
        running
            .upstream
            .send(text(r#"{"type":"response.created","id":"r1","request":"c1"}"#))
            .unwrap();
        until("r1 opened", || !running.exchanges.0.lock().unwrap().is_empty()).await;
        let Running {
            client,
            upstream,
            received,
            sink,
            exchanges,
            session,
            task,
            ..
        } = running;
        let stall_message = text(r#"{"type":"stall"}"#);
        match stall {
            Stall::UpstreamMessage => {
                client.send(stall_message.clone()).unwrap();
                until("upstream write started", || {
                    sink.sent.lock().unwrap().contains(&stall_message)
                })
                .await;
            }
            Stall::ClientMessage => {
                upstream.send(stall_message.clone()).unwrap();
                until("client write started", || {
                    received.lock().unwrap().contains(&stall_message)
                })
                .await;
            }
            Stall::Never | Stall::Slow | Stall::UpstreamClose => {}
        }
        match cause {
            Cause::ClientClose => client.send(Frame::Close(bye.clone())).unwrap(),
            Cause::ClientGone => drop(client),
            Cause::UpstreamClose => upstream.send(Frame::Close(None)).unwrap(),
            Cause::UpstreamGone => drop(upstream),
            Cause::Shutdown => shutdown.cancel(),
            Cause::Unstorable => upstream.send(text(r#"{"type":"unstorable"}"#)).unwrap(),
            Cause::CountersUnstorable => {
                client.send(text(r#"{"type":"unstorable_counters"}"#)).unwrap();
                until("projected", || {
                    sink.sent
                        .lock()
                        .unwrap()
                        .contains(&text(r#"{"type":"unstorable_counters"}"#))
                })
                .await;
                shutdown.cancel();
            }
            Cause::Exhausted => {
                for _ in 0..4 * ck_stream::FRAME_BYTES / FILL {
                    client.send(text(r#"{"type":"fill"}"#)).unwrap();
                }
            }
        }
        let finished = tokio::time::timeout(sessions::CLOSE_WITHIN * 3, task).await;
        let label = format!("{cause:?} {stall:?}");
        let finished = finished.expect("the session task ends").unwrap();
        let counters = match cause {
            Cause::CountersUnstorable => Err(Terminal::StorageExhausted),
            _ => Ok(()),
        };
        assert_eq!(
            finished,
            Ended {
                end: end.clone(),
                counters
            },
            "{label}"
        );
        assert_eq!(session.cancelled().now_or_never(), Some(terminal), "{label}");
        let (records, _) = records(&store, session.id()).await;
        let stored = !matches!(cause, Cause::CountersUnstorable);
        assert_eq!(
            records.last().map(String::as_str) == Some("counters"),
            stored,
            "{label}: final counters"
        );
        let exchanges = exchanges.0.lock().unwrap().clone();
        assert_eq!(
            exchanges.last(),
            Some(&Exchange::Closed("r1".into(), terminal)),
            "{label}"
        );
        let closes: Vec<Frame> = sink
            .sent
            .lock()
            .unwrap()
            .iter()
            .filter(|f| matches!(f, Frame::Close(_)))
            .cloned()
            .collect();
        assert_eq!(
            closes,
            close.into_iter().map(Frame::Close).collect::<Vec<_>>(),
            "{label}"
        );
        let forwarded_close: Vec<Frame> = received
            .lock()
            .unwrap()
            .iter()
            .filter(|f| matches!(f, Frame::Close(_)))
            .cloned()
            .collect();
        let expected_close = match &end {
            End::UpstreamClose(close) => vec![Frame::Close(close.clone())],
            _ => Vec::new(),
        };
        assert_eq!(
            forwarded_close, expected_close,
            "{label}: only an upstream close reaches the client"
        );
        if let Some(at_close) = *sink.at_close.lock().unwrap() {
            assert_eq!(
                at_close,
                Some(terminal),
                "{label}: the session ends before the upstream closes"
            );
        }
        std::fs::remove_dir_all(&directory).unwrap();
    }
}

#[tokio::test]
async fn runtime_shutdown_ends_every_session_even_with_a_stalled_write() {
    let (store, directory) = store();
    let registry = Registry::default();
    assert!(registry.install(store).is_ok());
    let store = registry.store().unwrap();
    // (which write stalls, what the client sends before shutdown)
    let cases = [
        (Stall::Never, None),
        (Stall::UpstreamMessage, Some(text(r#"{"type":"stall"}"#))),
        (Stall::UpstreamClose, None),
    ];
    let mut sessions = Vec::new();
    for (stall, message) in cases {
        let running = start(&store, stall, Launch::Registry(&registry));
        if let Some(message) = message {
            running.client.send(message.clone()).unwrap();
            until("write started", || running.sink.sent.lock().unwrap().contains(&message)).await;
        }
        // Attached before shutdown, as an accounting reader would be.
        let reader = store.reader(running.session.id(), 0).unwrap();
        sessions.push((stall, running, reader));
    }
    drop(store);
    let stopped = tokio::time::timeout(sessions::CLOSE_WITHIN * 3, registry.finish_shutdown());
    stopped
        .await
        .expect("shutdown does not wait on a stalled write")
        .unwrap();
    for (stall, running, reader) in sessions {
        assert!(running.task.is_finished(), "{stall:?}");
        let (records, terminal) = drain(reader).await;
        assert_eq!(
            records.last().map(String::as_str),
            Some("counters"),
            "{stall:?}: final counters"
        );
        assert_eq!(terminal, Terminal::Shutdown, "{stall:?}");
        assert_eq!(ended(running.task.await.unwrap()), End::Shutdown, "{stall:?}");
        assert_eq!(
            running.session.cancelled().now_or_never(),
            Some(Terminal::Shutdown),
            "{stall:?}"
        );
    }
    std::fs::remove_dir_all(&directory).unwrap();
}

#[tokio::test]
async fn a_session_releases_its_storage_once_it_ends_or_fails_to_start() {
    // (whether an accounting reader holds the stream after the session ended)
    for held in [false, true] {
        let (store, directory) = store();
        let before = store.used_frames();
        let running = start(&store, Stall::Never, Launch::Alone(CancellationToken::new()));
        running.client.send(text(r#"{"type":"session.update"}"#)).unwrap();
        settle(&running).await;
        for message in [
            r#"{"type":"response.created","id":"r1"}"#,
            r#"{"type":"response.done","id":"r1"}"#,
        ] {
            running.upstream.send(text(message)).unwrap();
        }
        until("r1 closed", || running.exchanges.0.lock().unwrap().len() == 2).await;
        assert!(
            store.used_frames() > before,
            "the session stream holds frames while it runs"
        );
        let Running {
            client,
            task,
            held: reader,
            session,
            sink,
            ..
        } = running;
        let id = session.id();
        drop((session, sink));
        let reader = held.then_some(reader);
        drop(client);
        assert_eq!(ended(task.await.unwrap()), End::ClientGone);
        if let Some(reader) = reader {
            let (records, terminal) = records(&store, id).await;
            assert_eq!(
                records.last().map(String::as_str),
                Some("counters"),
                "held: readable after the end"
            );
            assert_eq!(terminal, Terminal::ClientDisconnected);
            drop(reader);
        }
        assert!(store.reader(id, 0).is_err(), "held={held}: the stream left the cache");
        assert_eq!(store.used_frames(), before, "held={held}: its frames are free");
        drop(store);
        std::fs::remove_dir_all(&directory).unwrap();
    }
    // A session that cannot hold room for its counters: another exchange pins all but the
    // frame the projector's start takes. Each failed start leaves nothing behind.
    let (store, directory) = pool_store(4, None);
    for attempt in 0..3 {
        let mut other = store.create().unwrap();
        let pressure = other.begin_scratch(3 * ck_stream::FRAME_BYTES).unwrap();
        let (stream, projector) = realtime_projector(&store);
        let id = stream.id();
        let failed = Session::new(owner(), stream, projector, ()).err();
        assert!(
            matches!(failed, Some(ck_stream::Error::StorageExhausted)),
            "attempt {attempt}: {failed:?}"
        );
        drop((pressure, other));
        assert!(
            store.reader(id, 0).is_err(),
            "attempt {attempt}: the stream left the cache"
        );
        assert_eq!(store.used_frames(), 0, "attempt {attempt}: its frames are free");
    }
    drop(store);
    std::fs::remove_dir_all(&directory).unwrap();
}

/// What [`realtime_at_quota`] leaves: the session, its hold, how many clears were projected,
/// the end a refused one caused, and the pool frames another exchange pins.
type AtQuota = (
    Session<ck_translate::realtime::Session>,
    Held,
    usize,
    Option<End>,
    Option<ck_stream::ScratchReservation>,
);

/// What another exchange does once the session's response is open.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Other {
    Idle,
    /// Pins every pool frame left as scratch.
    PinsRest,
    /// Appends this many one-frame records, each allowed to evict cached streams.
    Appends(usize),
}

/// A fresh stream with the Realtime projector's `response.start` in it.
fn realtime_projector(store: &Store) -> (ck_stream::Writer, ck_translate::realtime::Session) {
    use ck_translate::codec::event::{Direction, Identity, LocalId};
    use ck_translate::realtime::{self, Admission, Events, Start};
    let mut stream = store.create().unwrap();
    let identity = Identity {
        request_id: LocalId::new("live-request").unwrap(),
        stream_id: LocalId::new("live-session").unwrap(),
        attempt: 0,
        direction: Direction::Response,
    };
    let start = Start {
        id: "rt_1",
        model: "gpt-realtime",
        created: 7,
    };
    let admission = Admission {
        events: Events::Realtime,
        model: None,
    };
    let projector = realtime::Session::new(&mut stream, identity, &start, admission).unwrap();
    (stream, projector)
}

/// A session on the Realtime projector: an open response with output audio, what `other`
/// does, then `clears` input messages, stopping at the first the stream quota refuses.
fn realtime_at_quota(store: &Store, clears: usize, other: Other) -> AtQuota {
    let (stream, projector) = realtime_projector(store);
    let mut session = Session::new(owner(), stream, projector, ()).unwrap();
    let held = session.hold();
    for message in [
        r#"{"type":"response.created","response":{"id":"r1"}}"#,
        r#"{"type":"response.output_audio.delta","response_id":"r1","delta":"QUJD"}"#,
    ] {
        session.upstream(&text(message)).unwrap();
    }
    let append = text(r#"{"type":"input_audio_buffer.append","audio":"QUJD"}"#);
    assert!(matches!(session.client(&append), Ok(sessions::Admitted::Forward)));
    let free = (store.capacity_frames() - store.used_frames()) as usize;
    let mut pinned = None;
    match other {
        Other::Idle => {}
        Other::PinsRest => {
            let mut writer = store.create().unwrap();
            pinned = Some(writer.begin_scratch(free * ck_stream::FRAME_BYTES).unwrap());
        }
        Other::Appends(records) => {
            let mut writer = store.create().unwrap();
            for _ in 0..records {
                let _ = writer.append(&[1; ck_stream::FRAME_BYTES]);
            }
        }
    }
    let clear = text(r#"{"type":"input_audio_buffer.clear"}"#);
    for projected in 0..clears {
        match session.client(&clear) {
            Ok(sessions::Admitted::Forward) => {}
            Ok(sessions::Admitted::Refused(refusal)) => panic!("refused {refusal:?}"),
            Err(end) => return (session, held, projected, Some(end), pinned),
        }
    }
    (session, held, clears, None, pinned)
}

#[tokio::test]
async fn the_realtime_counters_fit_a_session_at_its_stream_quota() {
    // How many clears the quota takes, measured on a session of its own.
    let (store, directory) = quota_store(Some(4));
    let (session, _held, fit, refused, _) = realtime_at_quota(&store, 10_000, Other::Idle);
    assert_eq!(
        refused,
        Some(End::Failed(Terminal::StorageExhausted)),
        "the input reaches the quota"
    );
    let _ = session.finish(Terminal::StorageExhausted);
    drop(store);
    std::fs::remove_dir_all(&directory).unwrap();
    // (pool frames, what another exchange does, clears sent, the terminal the session
    // finishes with)
    let cases = [
        // Every record fit, and the stream is full: shutdown still publishes the counters.
        (64, Other::Idle, fit, Terminal::Shutdown),
        // One more is refused and ends the session; the counters still fit.
        (64, Other::Idle, fit + 1, Terminal::StorageExhausted),
        // Another exchange holds every other pool frame: the counters take the frame the
        // session held for them.
        (4, Other::PinsRest, 10_000, Terminal::StorageExhausted),
        // Another exchange needs more frames than the pool has free: it cannot evict the
        // session stream, whose records and counters stay.
        (4, Other::Appends(3), 0, Terminal::Shutdown),
    ];
    for (pool, other, clears, terminal) in cases {
        let (store, directory) = pool_store(pool, Some(4));
        let (session, held, _, refused, pinned) = realtime_at_quota(&store, clears, other);
        assert_eq!(
            pinned.is_some(),
            other == Other::PinsRest,
            "{other:?}: the rest of the pool is pinned"
        );
        let expected = (terminal == Terminal::StorageExhausted).then_some(End::Failed(terminal));
        assert_eq!(refused, expected, "{other:?} {clears} clears");
        assert_eq!(
            session.finish(terminal),
            Ok(()),
            "{other:?} {clears} clears: the counters are stored"
        );
        let (records, ended) = records(&store, held.stream().id()).await;
        let last = records.last().unwrap();
        assert!(
            last.contains("realtime.counters"),
            "{other:?} {clears} clears: the counters are the last record"
        );
        assert!(
            last.contains(r#""response_id":"r1""#),
            "{other:?} {clears} clears: with the open response's audio"
        );
        assert_eq!(ended, terminal, "{other:?} {clears} clears");
        drop((held, pinned, store));
        std::fs::remove_dir_all(&directory).unwrap();
    }
}
