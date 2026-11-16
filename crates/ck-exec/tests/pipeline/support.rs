//! The stub executor, its probe ingress and the store fixture the pipeline tests drive.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use ck_core::config::Config;
use ck_core::credential::{Credential, Source};
use ck_core::exec::{Caller, ExecError, FailureScope, UsageSink};
use ck_exec::pipeline::{self, Started};
use ck_exec::types::{Attempt, Body, Client, Counted, Execute, Request, Streams, Upstream};
use ck_stream::{ManualClock, Read, Store, Terminal, Writer};
use ck_translate::codec::chat::{request, response};
use ck_translate::codec::event::{Direction, Identity, LocalId};
use ck_translate::codec::{self, BodyMode, Capability, Operation, RequestIngress, RequestView, ResponseIngress};
use ck_translate::{anthropic, gemini, responses};
use futures_util::{StreamExt, stream};
use http::HeaderMap;
use tokio::sync::{Notify, mpsc};

pub const REQUEST: &[u8] = br#"{"model":"m","messages":[{"role":"user","content":"hi"}],"stream":true}"#;
pub const WAIT: Duration = Duration::from_secs(10);

pub fn chunk(delta: &str, finish: &str) -> String {
    format!(
        "data: {{\"id\":\"r\",\"model\":\"m\",\"created\":1,\"choices\":[{{\"index\":0,\"delta\":{delta},\"finish_reason\":{finish}}}]}}\n\n"
    )
}

/// A complete Chat stream up to and including its completion boundary.
pub fn complete() -> String {
    [
        chunk(r#"{"role":"assistant","content":"hel"}"#, "null"),
        chunk(r#"{"content":"lo"}"#, "null"),
        chunk("{}", r#""stop""#),
        "data: [DONE]\n\n".to_owned(),
    ]
    .concat()
}

pub fn identity(direction: Direction) -> Identity {
    Identity {
        request_id: LocalId::new("request").unwrap(),
        stream_id: LocalId::new("stream").unwrap(),
        attempt: 1,
        direction,
    }
}

/// A destination that accepts every request, so a count can be fenced too.
pub struct Open;

impl Capability for Open {
    fn destination(&self) -> &'static str {
        "stub"
    }
    fn validate(&self, _request: RequestView<'_>) -> codec::Result<()> {
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum Answer {
    Stream,
    Panic,
    Refuse,
    Local,
    /// Binds the response ingress to the request stream: a gateway defect.
    Misbound,
    /// A buffered Chat JSON body.
    Buffered,
    /// A streamed Responses body.
    Responses,
    /// A streamed Anthropic Messages body.
    Anthropic,
    /// A streamed Gemini body.
    Gemini,
}

/// A response ingress with a test-controlled panic and flush deadline. `early` counts the
/// input it took while its deadline was set. `postpone` moves the inner ingress's own
/// deadline later, so that only demand flushes what it holds.
pub struct Probe {
    inner: Box<dyn ResponseIngress + Send>,
    panic: bool,
    deadline: Option<Instant>,
    postpone: Duration,
    flushed: Arc<Notify>,
    early: Arc<AtomicUsize>,
}

impl ResponseIngress for Probe {
    fn flush_deadline(&self) -> Option<Instant> {
        self.deadline
            .or(self.inner.flush_deadline().map(|deadline| deadline + self.postpone))
    }
    fn flush(&mut self, owner: &mut Writer) -> codec::Result<()> {
        self.deadline = None;
        self.flushed.notify_one();
        self.inner.flush(owner)
    }
    fn advance(&mut self, owner: &mut Writer, bytes: &[u8]) -> codec::Result<()> {
        assert!(!self.panic, "probe panic");
        if self.deadline.is_some() {
            self.early.fetch_add(1, Ordering::AcqRel);
        }
        self.inner.advance(owner, bytes)
    }
    fn finish(&mut self, owner: &mut Writer) -> codec::Result<()> {
        self.inner.finish(owner)
    }
    fn fail(&mut self, owner: &mut Writer, status: u16, scope: &'static str) -> codec::Result<()> {
        self.inner.fail(owner, status, scope)
    }
}

/// An executor over a local upstream whose body the test feeds.
pub struct Stub {
    answer: Answer,
    body: Mutex<Option<mpsc::UnboundedReceiver<Result<Bytes, ExecError>>>>,
    /// Store-clock time from the upstream answer to the probe's flush deadline.
    pub deadline: Mutex<Option<Duration>>,
    /// Added to the inner ingress's flush deadline.
    pub postpone: Mutex<Duration>,
    pub flushed: Arc<Notify>,
    pub early: Arc<AtomicUsize>,
}

impl Stub {
    pub fn new(answer: Answer) -> (Self, mpsc::UnboundedSender<Result<Bytes, ExecError>>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let stub = Self {
            answer,
            body: Mutex::new(Some(receiver)),
            deadline: Mutex::new(None),
            postpone: Mutex::new(Duration::ZERO),
            flushed: Arc::new(Notify::new()),
            early: Arc::new(AtomicUsize::new(0)),
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
        _request: &mut Request<'_>,
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
        let (identity, limit) = (attempt.identity, 1024 * 1024);
        let inner: Box<dyn ResponseIngress + Send> = match self.answer {
            Answer::Responses => Box::new(
                responses::response::Ingress::new(owner, identity, Operation::Generate, BodyMode::Streaming, limit)
                    .unwrap(),
            ),
            Answer::Anthropic => Box::new(
                anthropic::response::Ingress::new(owner, identity, Operation::Generate, BodyMode::Streaming, limit)
                    .unwrap(),
            ),
            Answer::Gemini => Box::new(
                gemini::response::Ingress::new(owner, identity, Operation::Generate, BodyMode::Streaming, limit)
                    .unwrap(),
            ),
            Answer::Buffered => Box::new(
                response::Ingress::new(
                    owner,
                    attempt.store,
                    identity,
                    Operation::Generate,
                    BodyMode::Buffered,
                    limit,
                )
                .unwrap(),
            ),
            _ => Box::new(
                response::Ingress::new(
                    owner,
                    attempt.store,
                    identity,
                    Operation::Generate,
                    BodyMode::Streaming,
                    limit,
                )
                .unwrap(),
            ),
        };
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
                deadline: self
                    .deadline
                    .lock()
                    .unwrap()
                    .map(|delay| streams.response.now() + delay),
                postpone: *self.postpone.lock().unwrap(),
                flushed: self.flushed.clone(),
                early: self.early.clone(),
            },
        })
    }

    async fn count_tokens(
        &self,
        attempt: &Attempt<'_>,
        request: &mut Request<'_>,
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
        request: &mut Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<Probe>, ExecError> {
        self.execute(attempt, request, streams).await
    }
}

pub struct Fixture {
    pub store: Store,
    directory: std::path::PathBuf,
    credential: Credential,
    cfg: Config,
    caller: Caller,
    headers: HeaderMap,
    usage: UsageSink,
}

impl Fixture {
    pub fn new(max_metadata_bytes: usize) -> Self {
        let directory = std::env::temp_dir().join(format!("ck-pipeline-{}", uuid::Uuid::new_v4()));
        let mut config = ck_stream::Config::new(directory.clone());
        config.pool_bytes = Some(768 * ck_stream::FRAME_BYTES);
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
            usage: UsageSink::default(),
        }
    }

    pub fn attempt(&self) -> Attempt<'_> {
        Attempt {
            credential: &self.credential,
            cfg: &self.cfg,
            model: "m",
            headers: &self.headers,
            resolved_model: None,
            store: &self.store,
            identity: identity(Direction::Response),
        }
    }

    pub fn client(&self) -> Client<'_> {
        Client {
            caller: &self.caller,
            headers: &self.headers,
            route: "/v1/chat/completions",
            alt: None,
            streaming: None,
            session: None,
            execution_session: None,
            derived_session: None,
            usage: &self.usage,
            original: &[],
        }
    }

    /// Fences the client request, re-fences it as `operation` and starts one attempt. The
    /// request stream is returned too: it keeps the exchange quota the response charges.
    pub async fn launch(&self, stub: &Stub, operation: Operation) -> (Result<Started, ExecError>, Writer, u64) {
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
        let fenced = ingress
            .finish(&mut upload, &ck_translate::codec::ClientCapability)
            .and_then(|fenced| pipeline::refence(stub, &attempt, fenced.view()))
            .unwrap();
        let view = RequestView {
            operation,
            ..fenced.view()
        };
        let mut request = Request::new(pipeline::refence(stub, &attempt, view).unwrap(), self.client());
        assert_eq!(request.validated().destination(), pipeline::destination(stub, &attempt));
        let response = self.store.create_dependent(&upload).unwrap();
        let id = response.id();
        let started = pipeline::start(stub, &attempt, &mut request, &mut upload, response).await;
        (started, upload, id)
    }

    /// Every record of stream `id` and its terminal; waits for the terminal.
    pub async fn records(&self, id: u64) -> (Vec<String>, Terminal) {
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

pub async fn finished(started: Started) -> Result<(), ExecError> {
    tokio::time::timeout(WAIT, started.producer).await.unwrap().unwrap()
}

/// Feeds `items` to an `answer` upstream and closes the body; the producer's outcome, the
/// records and the terminal. The items are queued before the producer starts: a producer that
/// ends at an earlier item drops the body, and a later send would race that drop.
pub async fn feed(
    answer: Answer,
    items: Vec<Result<Bytes, ExecError>>,
) -> (Result<(), ExecError>, Vec<String>, Terminal) {
    let fixture = Fixture::new(4 * 1024 * 1024);
    let (stub, upstream) = Stub::new(answer);
    for item in items {
        upstream.send(item).unwrap();
    }
    drop(upstream);
    let (started, _upload, id) = fixture.launch(&stub, Operation::Generate).await;
    let outcome = finished(started.unwrap()).await;
    let (records, terminal) = fixture.records(id).await;
    (outcome, records, terminal)
}

pub fn data(bytes: impl Into<Bytes>) -> Result<Bytes, ExecError> {
    Ok(bytes.into())
}

pub fn cut(status: u16, scope: FailureScope) -> Result<Bytes, ExecError> {
    Err(ExecError::local(status, scope, "cut"))
}
