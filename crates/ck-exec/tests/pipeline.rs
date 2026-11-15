//! The executor pipeline (`docs/EXECUTION-CONTRACT.md` §4–5, §7, §9): fences, attempt start
//! and the runtime-owned producer, driven through a stub executor whose upstream body the
//! test feeds over a channel.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use ck_core::config::Config;
use ck_core::credential::{Credential, Source};
use ck_core::exec::{Caller, ExecError, FailureScope};
use ck_exec::pipeline::{self, Started};
use ck_exec::types::{Attempt, Body, Client, Counted, Execute, Request, Streams, Upstream};
use ck_stream::{ManualClock, Read, Store, Terminal, Writer};
use ck_translate::codec::chat::{request, response};
use ck_translate::codec::event::{Direction, Identity, LocalId};
use ck_translate::codec::{self, BodyMode, Capability, Operation, RequestIngress, RequestView, ResponseIngress};
use futures_util::{StreamExt, stream};
use http::HeaderMap;
use tokio::sync::{Notify, mpsc};

const REQUEST: &[u8] = br#"{"model":"m","messages":[{"role":"user","content":"hi"}],"stream":true}"#;
const WAIT: Duration = Duration::from_secs(10);

fn chunk(delta: &str, finish: &str) -> String {
    format!(
        "data: {{\"id\":\"r\",\"model\":\"m\",\"created\":1,\"choices\":[{{\"index\":0,\"delta\":{delta},\"finish_reason\":{finish}}}]}}\n\n"
    )
}

/// A complete Chat stream up to and including its completion boundary.
fn complete() -> String {
    [
        chunk(r#"{"role":"assistant","content":"hel"}"#, "null"),
        chunk(r#"{"content":"lo"}"#, "null"),
        chunk("{}", r#""stop""#),
        "data: [DONE]\n\n".to_owned(),
    ]
    .concat()
}

fn identity(direction: Direction) -> Identity {
    Identity {
        request_id: LocalId::new("request").unwrap(),
        stream_id: LocalId::new("stream").unwrap(),
        attempt: 1,
        direction,
    }
}

/// A destination that accepts every request, so a count can be fenced too.
struct Open;

impl Capability for Open {
    fn destination(&self) -> &'static str {
        "stub"
    }
    fn validate(&self, _request: RequestView<'_>) -> codec::Result<()> {
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Answer {
    Stream,
    Panic,
    Refuse,
    Local,
    /// Binds the response ingress to the request stream: a gateway defect.
    Misbound,
}

/// The Chat response ingress, with a test-controlled panic and flush deadline.
struct Probe {
    inner: response::Ingress,
    panic: bool,
    deadline: Option<Instant>,
    flushed: Arc<Notify>,
}

impl ResponseIngress for Probe {
    fn flush_deadline(&self) -> Option<Instant> {
        self.deadline.or(self.inner.flush_deadline())
    }
    fn flush(&mut self, owner: &mut Writer) -> codec::Result<()> {
        self.deadline = None;
        self.flushed.notify_one();
        self.inner.flush(owner)
    }
    fn advance(&mut self, owner: &mut Writer, bytes: &[u8]) -> codec::Result<()> {
        assert!(!self.panic, "probe panic");
        self.inner.advance(owner, bytes)
    }
    fn finish(&mut self, owner: &mut Writer) -> codec::Result<()> {
        self.inner.finish(owner)
    }
}

/// An executor over a local upstream whose body the test feeds.
struct Stub {
    answer: Answer,
    body: Mutex<Option<mpsc::UnboundedReceiver<Result<Bytes, ExecError>>>>,
    deadline: AtomicBool,
    flushed: Arc<Notify>,
}

impl Stub {
    fn new(answer: Answer) -> (Self, mpsc::UnboundedSender<Result<Bytes, ExecError>>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let stub = Self {
            answer,
            body: Mutex::new(Some(receiver)),
            deadline: AtomicBool::new(false),
            flushed: Arc::new(Notify::new()),
        };
        (stub, sender)
    }
}

impl Execute for Stub {
    type Capability = Open;
    type Ingress = Probe;

    fn capability(&self, _attempt: &Attempt<'_>) -> Open {
        Open
    }

    async fn execute(
        &self,
        attempt: &Attempt<'_>,
        _request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<Probe>, ExecError> {
        if self.answer == Answer::Refuse {
            return Err(ExecError::local(503, FailureScope::Transport, "refused"));
        }
        let owner = if self.answer == Answer::Misbound {
            streams.request
        } else {
            &mut *streams.response
        };
        let inner = response::Ingress::new(
            owner,
            attempt.store,
            attempt.identity,
            Operation::Generate,
            BodyMode::Streaming,
            1024 * 1024,
        )
        .unwrap();
        let receiver = self.body.lock().unwrap().take().unwrap();
        let body: Body = stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|item| (item, receiver))
        })
        .boxed();
        Ok(Upstream {
            status: 200,
            headers: HeaderMap::new(),
            body,
            ingress: Probe {
                inner,
                panic: self.answer == Answer::Panic,
                deadline: self.deadline.load(Ordering::Acquire).then(|| streams.response.now()),
                flushed: self.flushed.clone(),
            },
        })
    }

    async fn count_tokens(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Counted<Probe>, ExecError> {
        if self.answer == Answer::Local {
            return Ok(Counted::Local { input_tokens: 12 });
        }
        self.execute(attempt, request, streams).await.map(Counted::Upstream)
    }

    async fn compact(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<Probe>, ExecError> {
        self.execute(attempt, request, streams).await
    }
}

struct Fixture {
    store: Store,
    directory: std::path::PathBuf,
    credential: Credential,
    cfg: Config,
    caller: Caller,
    headers: HeaderMap,
}

impl Fixture {
    fn new(max_metadata_bytes: usize) -> Self {
        let directory = std::env::temp_dir().join(format!("ck-pipeline-{}", uuid::Uuid::new_v4()));
        let mut config = ck_stream::Config::new(directory.clone());
        config.pool_bytes = Some(64 * ck_stream::FRAME_BYTES);
        config.max_metadata_bytes = max_metadata_bytes;
        config.header_slots = 8;
        config.workers = 1;
        Self {
            store: Store::with_clock(config, Arc::new(ManualClock::default())).unwrap(),
            directory,
            credential: Credential {
                id: "stub".into(),
                provider: "stub".into(),
                source: Source::Config {
                    section: "stub".into(),
                    index: 0,
                },
                disabled: false,
                label: "stub".into(),
                attributes: BTreeMap::new(),
                metadata: serde_json::Map::new(),
                revision: 1,
            },
            cfg: Config::default(),
            caller: Caller {
                principal: "sk-client".into(),
                source: "authorization",
                provider: "config-inline".into(),
            },
            headers: HeaderMap::new(),
        }
    }

    fn attempt(&self) -> Attempt<'_> {
        Attempt {
            credential: &self.credential,
            cfg: &self.cfg,
            model: "m",
            resolved_model: None,
            store: &self.store,
            identity: identity(Direction::Response),
        }
    }

    fn client(&self) -> Client<'_> {
        Client {
            caller: &self.caller,
            headers: &self.headers,
            route: "/v1/chat/completions",
            session: None,
            execution_session: None,
            derived_session: None,
        }
    }

    /// Fences the client request, re-fences it as `operation` and starts one attempt. The
    /// request stream is returned too: it keeps the exchange quota the response charges.
    async fn launch(&self, stub: &Stub, operation: Operation) -> (Result<Started, ExecError>, Writer, u64) {
        let attempt = self.attempt();
        let mut upload = self.store.create().unwrap();
        let mut ingress = request::Ingress::new(
            &mut upload,
            &self.store,
            identity(Direction::Request),
            Operation::Generate,
            1024 * 1024,
        )
        .unwrap();
        ingress.advance(&mut upload, REQUEST).unwrap();
        let fenced = pipeline::fence(stub, &attempt, &mut ingress, &mut upload).unwrap();
        let view = RequestView {
            operation,
            ..fenced.view()
        };
        let request = Request {
            validated: pipeline::refence(stub, &attempt, view).unwrap(),
            client: self.client(),
        };
        assert_eq!(request.validated.destination(), pipeline::destination(stub, &attempt));
        let response = self.store.create_dependent(&upload).unwrap();
        let id = response.id();
        let started = pipeline::start(stub, &attempt, &request, &mut upload, response).await;
        (started, upload, id)
    }

    /// Every record of stream `id` and its terminal; waits for the terminal.
    async fn records(&self, id: u64) -> (Vec<String>, Terminal) {
        let mut reader = self.store.reader(id, 0).unwrap();
        let mut records = Vec::new();
        loop {
            match tokio::time::timeout(WAIT, reader.next()).await.unwrap().unwrap() {
                Read::Record(lease) => {
                    let mut bytes = Vec::new();
                    lease.visit(|chunk| bytes.extend_from_slice(chunk)).unwrap();
                    records.push(String::from_utf8(bytes).unwrap());
                }
                Read::Terminal(terminal) => return (records, terminal),
            }
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

async fn finished(started: Started) -> Result<(), ExecError> {
    tokio::time::timeout(WAIT, started.producer).await.unwrap().unwrap()
}

/// Feeds `chunks` and closes the body; the producer's outcome, records and terminal.
async fn produce(chunks: &[&[u8]]) -> (Result<(), ExecError>, Vec<String>, Terminal) {
    let fixture = Fixture::new(4 * 1024 * 1024);
    let (stub, upstream) = Stub::new(Answer::Stream);
    let (started, _upload, id) = fixture.launch(&stub, Operation::Generate).await;
    for chunk in chunks {
        upstream.send(Ok(Bytes::copy_from_slice(chunk))).unwrap();
    }
    drop(upstream);
    let outcome = finished(started.unwrap()).await;
    let (records, terminal) = fixture.records(id).await;
    (outcome, records, terminal)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_one_byte_feed_publishes_what_one_chunk_does() {
    let body = complete();
    let (whole, expected, terminal) = produce(&[body.as_bytes()]).await;
    whole.unwrap();
    assert_eq!(terminal, Terminal::Done);
    assert!(expected.last().unwrap().contains("x_ck.done"));
    let bytes: Vec<&[u8]> = body.as_bytes().chunks(1).collect();
    let (split, records, terminal) = produce(&bytes).await;
    split.unwrap();
    assert_eq!(terminal, Terminal::Done);
    assert_eq!(records, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_upstream_finishes_while_the_reader_stalls() {
    let fixture = Fixture::new(4 * 1024 * 1024);
    let (stub, upstream) = Stub::new(Answer::Stream);
    let (started, _upload, id) = fixture.launch(&stub, Operation::Generate).await;
    // Attached before any upstream byte and never read until the producer ended.
    let stalled = fixture.store.reader(id, 0).unwrap();
    let mut body = chunk(r#"{"role":"assistant","content":""}"#, "null");
    for _ in 0..200 {
        body.push_str(&chunk(r#"{"content":"0123456789"}"#, "null"));
    }
    body.push_str(&chunk("{}", r#""stop""#));
    body.push_str("data: [DONE]\n\n");
    for piece in body.as_bytes().chunks(97) {
        upstream.send(Ok(Bytes::copy_from_slice(piece))).unwrap();
    }
    drop(upstream);
    finished(started.unwrap()).await.unwrap();
    drop(stalled);
    let (records, terminal) = fixture.records(id).await;
    assert_eq!(terminal, Terminal::Done);
    assert!(records.len() > 200);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eof_before_the_completion_boundary_publishes_error() {
    let open = chunk(r#"{"role":"assistant","content":"hel"}"#, "null");
    let (outcome, records, terminal) = produce(&[open.as_bytes()]).await;
    let error = outcome.unwrap_err();
    assert_eq!(error.status, 502);
    assert_eq!(terminal, Terminal::UpstreamError);
    assert!(records.last().unwrap().contains("event: x_ck.error"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_body_error_publishes_error_and_keeps_its_scope() {
    let fixture = Fixture::new(4 * 1024 * 1024);
    let (stub, upstream) = Stub::new(Answer::Stream);
    let (started, _upload, id) = fixture.launch(&stub, Operation::Generate).await;
    let open = chunk(r#"{"role":"assistant","content":"hel"}"#, "null");
    upstream.send(Ok(Bytes::from(open))).unwrap();
    upstream
        .send(Err(ExecError::local(429, FailureScope::Credential, "cut")))
        .unwrap();
    let error = finished(started.unwrap()).await.unwrap_err();
    assert_eq!((error.status, error.scope), (429, FailureScope::Credential));
    let (records, terminal) = fixture.records(id).await;
    assert_eq!(terminal, Terminal::UpstreamError);
    assert!(records.last().unwrap().contains("event: x_ck.error"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_body_error_after_the_completion_boundary_keeps_done() {
    let fixture = Fixture::new(4 * 1024 * 1024);
    let (stub, upstream) = Stub::new(Answer::Stream);
    let (started, _upload, id) = fixture.launch(&stub, Operation::Generate).await;
    upstream.send(Ok(Bytes::from(complete()))).unwrap();
    upstream
        .send(Err(ExecError::local(502, FailureScope::Transport, "reset")))
        .unwrap();
    finished(started.unwrap()).await.unwrap();
    let (records, terminal) = fixture.records(id).await;
    assert_eq!(terminal, Terminal::Done);
    assert!(records.last().unwrap().contains("x_ck.done"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_after_the_completion_boundary_keeps_done() {
    let body = format!("{}{}", complete(), chunk(r#"{"content":"late"}"#, "null"));
    let (outcome, records, terminal) = produce(&[body.as_bytes()]).await;
    outcome.unwrap();
    assert_eq!(terminal, Terminal::Done);
    assert!(records.last().unwrap().contains("x_ck.done"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_producer_panic_is_producer_lost() {
    let fixture = Fixture::new(4 * 1024 * 1024);
    let (stub, upstream) = Stub::new(Answer::Panic);
    let (started, _upload, id) = fixture.launch(&stub, Operation::Generate).await;
    upstream.send(Ok(Bytes::from(complete()))).unwrap();
    let joined = tokio::time::timeout(WAIT, started.unwrap().producer).await.unwrap();
    assert!(joined.unwrap_err().is_panic());
    let (_, terminal) = fixture.records(id).await;
    assert_eq!(terminal, Terminal::ProducerLost);
    assert_eq!(terminal.error_code(), Some("stream.producer_lost"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancel_stops_the_producer() {
    let fixture = Fixture::new(4 * 1024 * 1024);
    let (stub, upstream) = Stub::new(Answer::Stream);
    let (started, _upload, id) = fixture.launch(&stub, Operation::Generate).await;
    let mut started = started.unwrap();
    started.stream.finish(Terminal::Cancelled);
    finished(started).await.unwrap();
    assert!(upstream.is_closed(), "the producer dropped the upstream body");
    assert_eq!(fixture.records(id).await.1, Terminal::Cancelled);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_discarded_stream_fails_the_attempt_as_gone() {
    let fixture = Fixture::new(4 * 1024 * 1024);
    let (stub, _upstream) = Stub::new(Answer::Stream);
    let (started, _upload, _) = fixture.launch(&stub, Operation::Generate).await;
    let mut started = started.unwrap();
    started.stream.finish(Terminal::Gone);
    assert_eq!(finished(started).await.unwrap_err().status, 410);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_gateway_defect_is_producer_lost() {
    let fixture = Fixture::new(4 * 1024 * 1024);
    let (stub, upstream) = Stub::new(Answer::Misbound);
    let (started, _upload, id) = fixture.launch(&stub, Operation::Generate).await;
    upstream.send(Ok(Bytes::from(complete()))).unwrap();
    let error = finished(started.unwrap()).await.unwrap_err();
    assert_eq!((error.status, error.scope), (502, FailureScope::Transport));
    let (records, terminal) = fixture.records(id).await;
    assert!(records.is_empty());
    assert_eq!(terminal, Terminal::ProducerLost);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_flush_deadline_is_served_while_the_upstream_is_idle() {
    let fixture = Fixture::new(4 * 1024 * 1024);
    let (stub, upstream) = Stub::new(Answer::Stream);
    stub.deadline.store(true, Ordering::Release);
    let (started, _upload, _) = fixture.launch(&stub, Operation::Generate).await;
    let started = started.unwrap();
    tokio::time::timeout(WAIT, stub.flushed.notified()).await.unwrap();
    assert!(!started.producer.is_finished());
    drop(upstream);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_attempt_discards_its_response_stream() {
    let fixture = Fixture::new(4 * 1024 * 1024);
    let (stub, _upstream) = Stub::new(Answer::Refuse);
    let (started, _upload, id) = fixture.launch(&stub, Operation::Generate).await;
    let error = started.err().unwrap();
    assert_eq!((error.status, error.scope), (503, FailureScope::Transport));
    assert!(matches!(fixture.store.reader(id, 0), Err(ck_stream::Error::Gone)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_local_count_publishes_the_canonical_result() {
    let fixture = Fixture::new(4 * 1024 * 1024);
    let (stub, _upstream) = Stub::new(Answer::Local);
    let (started, _upload, id) = fixture.launch(&stub, Operation::CountTokens).await;
    let started = started.unwrap();
    assert_eq!(started.status, 200);
    finished(started).await.unwrap();
    let (records, terminal) = fixture.records(id).await;
    assert_eq!(terminal, Terminal::Done);
    assert_eq!(records.len(), 3);
    assert!(records[0].contains("x_ck.response.start") && records[0].contains(r#""model":"m""#));
    assert!(records[1].contains("x_ck.count_tokens") && records[1].contains(r#""input_tokens":12"#));
    assert!(records[2].contains("x_ck.done"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_producer_charges_the_exchange_quota() {
    let fixture = Fixture::new(4 * 1024 * 1024);
    let (stub, upstream) = Stub::new(Answer::Stream);
    let (started, mut upload, id) = fixture.launch(&stub, Operation::Generate).await;
    // The request stream takes what is left of the exchange's metadata quota.
    let mut permits = Vec::new();
    while let Ok(permit) = upload.reserve_metadata(256) {
        permits.push(permit);
    }
    let mut body = String::new();
    for _ in 0..64 {
        body.push_str(&chunk(r#"{"role":"assistant","content":"0123456789"}"#, "null"));
    }
    upstream.send(Ok(Bytes::from(body))).unwrap();
    let error = finished(started.unwrap()).await.unwrap_err();
    assert_eq!((error.status, error.scope), (503, FailureScope::Request));
    assert_eq!(fixture.records(id).await.1, Terminal::StorageExhausted);
}

/// Grep gate: the pipeline passes upstream chunks on and never collects a body on the heap.
#[test]
fn the_pipeline_collects_no_body() {
    for (file, source) in [
        ("lib.rs", include_str!("../src/lib.rs")),
        ("types.rs", include_str!("../src/types.rs")),
        ("pipeline.rs", include_str!("../src/pipeline.rs")),
        ("wire.rs", include_str!("../src/wire.rs")),
        ("payload_barrier.rs", include_str!("../src/payload_barrier.rs")),
        ("rawjson.rs", include_str!("../src/rawjson.rs")),
    ] {
        assert!(!source.contains("BytesMut"), "{file} collects bytes in BytesMut");
    }
}
