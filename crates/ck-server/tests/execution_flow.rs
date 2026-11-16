//! The execution contract (`docs/EXECUTION-CONTRACT.md`) carries one exchange end to end with
//! the real Chat codecs: a failed first attempt and a second attempt that re-fence and encode
//! from the same request stream, response egress encoding while the producer still runs, and
//! a resume after both streams finished that rebuilds the request view.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::http::HeaderMap;
use bytes::Bytes;
use ck_core::config::Config;
use ck_core::credential::{Credential, Source};
use ck_core::exec::{Caller, ExecError, FailureScope, UsageSink};
use ck_exec::pipeline::Started;
use ck_exec::types::{Attempt, Client, Counted, Execute, Request, Streams, Upstream};
use ck_server::http::egress::{Framing, Output, Protocol};
use ck_server::inference::streams::{Exchange, Owner, StreamHandle, StreamId};
use ck_stream::{ManualClock, Read, Reader, ScratchLease, Store, Terminal, Writer};
use ck_translate::codec::chat::{egress, request, response};
use ck_translate::codec::event::{Direction, Identity, LocalId};
use ck_translate::codec::{
    BodyMode, Operation, RequestEgress, RequestFence, RequestIngress, RequestView, ResponseEgress, ResponseIngress,
};
use futures_util::{StreamExt, stream};
use tokio::sync::mpsc;

const REQUEST: &[u8] = br#"{"model":"m","messages":[{"role":"user","content":"hi"}],"stream":true}"#;

fn chunk(delta: &str, finish: &str) -> Bytes {
    Bytes::from(format!(
        "data: {{\"id\":\"r\",\"model\":\"m\",\"created\":1,\"choices\":[{{\"index\":0,\"delta\":{delta},\"finish_reason\":{finish}}}]}}\n\n"
    ))
}

fn identity(direction: Direction, attempt: u64) -> Identity {
    Identity {
        request_id: LocalId::new("request").unwrap(),
        stream_id: LocalId::new("stream").unwrap(),
        attempt,
        direction,
    }
}

fn lease_bytes(lease: &ScratchLease) -> Vec<u8> {
    let mut bytes = Vec::new();
    while bytes.len() < lease.len() {
        bytes.extend_from_slice(lease.chunk(bytes.len()).unwrap());
    }
    bytes
}

/// A Chat upstream: refuses its first call, then answers with the body the test feeds.
struct Probe {
    refuse: AtomicBool,
    body: Mutex<Option<mpsc::UnboundedReceiver<Result<Bytes, ExecError>>>>,
    uploads: Mutex<Vec<Vec<u8>>>,
}

impl Execute for Probe {
    type Capability = request::ChatCapability;
    type Ingress = response::Ingress;

    fn capability(&self, _attempt: &Attempt<'_>) -> request::ChatCapability {
        request::ChatCapability
    }

    async fn execute(
        &self,
        attempt: &Attempt<'_>,
        request: &mut Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<response::Ingress>, ExecError> {
        let local = |_| ExecError::local(500, FailureScope::Request, "codec");
        let upload = request::Egress {
            destination: "chat",
            upstream_stream: true,
        }
        .encode(&request.validated(), streams.request)
        .map_err(local)?;
        self.uploads.lock().unwrap().push(lease_bytes(&upload));
        let attempt_number = self.uploads.lock().unwrap().len() as u64;
        let ingress = response::Ingress::new(
            streams.response,
            attempt.store,
            identity(Direction::Response, attempt_number),
            Operation::Generate,
            BodyMode::Streaming,
            1024 * 1024,
        )
        .map_err(local)?;
        tokio::task::yield_now().await;
        if self.refuse.swap(false, Ordering::AcqRel) {
            return Err(ExecError::local(503, FailureScope::Transport, "refused"));
        }
        let receiver = self.body.lock().unwrap().take().unwrap();
        let body = stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|item| (item, receiver))
        })
        .boxed();
        Ok(Upstream {
            status: 200,
            headers: HeaderMap::new(),
            body,
            ingress,
        })
    }

    async fn count_tokens(
        &self,
        _attempt: &Attempt<'_>,
        _request: &mut Request<'_>,
        _streams: Streams<'_>,
    ) -> Result<Counted<response::Ingress>, ExecError> {
        unreachable!("generation only")
    }

    async fn compact(
        &self,
        _attempt: &Attempt<'_>,
        _request: &mut Request<'_>,
        _streams: Streams<'_>,
    ) -> Result<Upstream<response::Ingress>, ExecError> {
        unreachable!("generation only")
    }
}

/// D1's producer, reduced to what the contract fixes.
async fn produce(mut upstream: Upstream<response::Ingress>, mut writer: Writer) -> Result<(), ExecError> {
    let failed = |_| ExecError::local(502, FailureScope::Transport, "upstream");
    while let Some(chunk) = upstream.body.next().await {
        upstream.ingress.advance(&mut writer, &chunk?).map_err(failed)?;
    }
    upstream.ingress.finish(&mut writer).map_err(failed)?;
    writer.finish(Terminal::Done);
    Ok(())
}

fn chat_egress(stream: &mut Writer) -> egress::Egress {
    egress::Egress::new(
        stream,
        identity(Direction::Response, 2),
        Operation::Generate,
        BodyMode::Streaming,
        false,
        |_| {},
    )
    .unwrap()
}

/// Encodes the next record for the client; `None` at the stream's terminal.
async fn deliver(
    egress: &mut egress::Egress,
    stream: &mut Writer,
    reader: &mut Reader,
    output: &mut Vec<u8>,
) -> Option<()> {
    match reader.next().await? {
        Read::Record(lease) => {
            egress
                .advance(stream, lease, |bytes| {
                    output.extend_from_slice(&lease_bytes(&bytes));
                    Ok(())
                })
                .unwrap();
            Some(())
        }
        Read::Terminal(terminal) => {
            assert_eq!(terminal, Terminal::Done);
            None
        }
    }
}

struct Fixture {
    store: Store,
    clock: Arc<ManualClock>,
    directory: std::path::PathBuf,
    credential: Credential,
    cfg: Config,
    caller: Caller,
    headers: HeaderMap,
    usage: UsageSink,
}

impl Fixture {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!("ck-flow-{}", uuid::Uuid::new_v4()));
        let mut config = ck_stream::Config::new(directory.clone());
        config.pool_bytes = Some(768 * ck_stream::FRAME_BYTES);
        config.max_metadata_bytes = 4 * 1024 * 1024;
        config.header_slots = 8;
        config.workers = 1;
        let clock = Arc::new(ManualClock::default());
        Self {
            store: Store::with_clock(config, clock.clone()).unwrap(),
            clock,
            directory,
            credential: Credential {
                id: "probe".into(),
                provider: "openai-compatibility".into(),
                source: Source::Config {
                    section: "openai-compatibility".into(),
                    index: 0,
                },
                disabled: false,
                label: "probe".into(),
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

    fn attempt(&self) -> Attempt<'_> {
        Attempt {
            credential: &self.credential,
            cfg: &self.cfg,
            model: "m",
            headers: &self.headers,
            resolved_model: None,
            store: &self.store,
            identity: identity(Direction::Response, 1),
        }
    }

    fn client(&self) -> Client<'_> {
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
}

/// The live exchange: two attempts, egress during production, commitment.
/// Runs attempts until one starts: each re-fences the client view for its own destination,
/// encodes into the request stream and gets a fresh response stream that depends on it.
async fn start(fixture: &Fixture, probe: &Probe, exchange: &mut Exchange, fenced: RequestView<'_>) -> Started {
    let store = &fixture.store;
    let attempt = fixture.attempt();
    loop {
        let mut request = Request::new(
            RequestFence::default()
                .end(fenced, &probe.capability(&attempt))
                .unwrap(),
            fixture.client(),
        );
        let mut response = store.create_dependent(&exchange.request).unwrap();
        let streams = Streams {
            request: &mut exchange.request,
            response: &mut response,
        };
        match probe.execute(&attempt, &mut request, streams).await {
            Ok(upstream) => {
                return Started {
                    status: upstream.status,
                    headers: upstream.headers.clone(),
                    stream: response.allocator(),
                    producer: tokio::spawn(produce(upstream, response)),
                };
            }
            Err(error) => {
                assert_eq!(error.scope, FailureScope::Transport);
                let id = response.id();
                response.finish(Terminal::Gone);
                assert!(store.reader(id, 0).is_err(), "a failed attempt is discarded");
            }
        }
    }
}

/// Delivers the response while the producer is still waiting for upstream bytes.
async fn egress_while_producing(
    store: &Store,
    started: &mut Started,
    upstream: mpsc::UnboundedSender<Result<Bytes, ExecError>>,
) -> Vec<u8> {
    let stream = &mut started.stream;
    let mut egress = chat_egress(stream);
    let mut reader = store.reader(stream.id(), 0).unwrap();
    let mut output = Vec::new();
    upstream
        .send(Ok(chunk(r#"{"role":"assistant","content":"hel"}"#, "null")))
        .unwrap();
    while !String::from_utf8_lossy(&output).contains("hel") {
        deliver(&mut egress, stream, &mut reader, &mut output).await.unwrap();
    }
    assert!(
        !started.producer.is_finished(),
        "egress encodes while the producer runs"
    );
    upstream.send(Ok(chunk(r#"{"content":"lo"}"#, "null"))).unwrap();
    upstream.send(Ok(chunk("{}", r#""stop""#))).unwrap();
    upstream.send(Ok(Bytes::from_static(b"data: [DONE]\n\n"))).unwrap();
    drop(upstream);
    while deliver(&mut egress, stream, &mut reader, &mut output).await.is_some() {}
    egress.finish().unwrap();
    output
}

/// The live exchange: two attempts, egress during production, commitment.
async fn live(
    fixture: &Fixture,
    probe: &Probe,
    upstream: mpsc::UnboundedSender<Result<Bytes, ExecError>>,
) -> (StreamHandle, Vec<u8>) {
    let store = &fixture.store;
    let owner = Owner {
        caller: fixture.caller.clone(),
        scope: None,
    };
    let output = Output {
        protocol: Protocol::Chat,
        operation: Operation::Generate,
        framing: Framing::Sse,
    };
    let mut exchange = Exchange::new(owner, output, store).unwrap();
    let mut ingress = request::Ingress::new(
        &mut exchange.request,
        store,
        identity(Direction::Request, 0),
        Operation::Generate,
        1024 * 1024,
    )
    .unwrap();
    ingress.advance(&mut exchange.request, REQUEST).unwrap();
    // The client fence publishes `request.end` against the first destination.
    let fenced = ingress
        .finish(&mut exchange.request, &probe.capability(&fixture.attempt()))
        .unwrap();
    let mut started = start(fixture, probe, &mut exchange, fenced.view()).await;
    let uploads = probe.uploads.lock().unwrap().clone();
    assert_eq!(uploads.len(), 2);
    assert_eq!(uploads[0], uploads[1], "both attempts encode the same request");
    assert_eq!(started.status, 200);
    let output = egress_while_producing(store, &mut started, upstream).await;
    started.producer.await.unwrap().unwrap();

    // Commitment: the exchange becomes a handle; the request writer finishes.
    let handle = exchange.into_handle(StreamId([7; 16]), started.stream);
    (handle, output)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_attempts_egress_while_producing_and_resume_after_finish() {
    let fixture = Fixture::new();
    let (sender, receiver) = mpsc::unbounded_channel();
    let probe = Probe {
        refuse: AtomicBool::new(true),
        body: Mutex::new(Some(receiver)),
        uploads: Mutex::new(Vec::new()),
    };
    let (mut handle, delivered) = live(&fixture, &probe, sender).await;
    assert!(String::from_utf8_lossy(&delivered).contains("lo"));

    // A client stays attached to the response for two hours; the response stream depends on
    // the request stream, so neither expires.
    let attached = fixture.store.reader(handle.response.id(), 0).unwrap();
    fixture.clock.advance(Duration::from_secs(7200));
    fixture.store.tick().unwrap();

    // Resume after both streams finished: rebuild the request view and re-encode.
    let mut records = fixture.store.reader(handle.request.id(), 0).unwrap();
    let (document, media) = request::read_request(&mut handle.request, &mut records).unwrap();
    let view = RequestView {
        document: &document,
        media: &media,
        operation: handle.output.operation,
    };
    let validated = RequestFence::default().end(view, &request::ChatCapability).unwrap();
    let upload = request::Egress {
        destination: "chat",
        upstream_stream: true,
    }
    .encode(&validated, &mut handle.request)
    .unwrap();
    assert_eq!(
        lease_bytes(&upload),
        probe.uploads.lock().unwrap()[1],
        "the rebuilt view is the request"
    );

    let mut egress = chat_egress(&mut handle.response);
    let mut reader = fixture.store.reader(handle.response.id(), 0).unwrap();
    let mut resumed = Vec::new();
    while deliver(&mut egress, &mut handle.response, &mut reader, &mut resumed)
        .await
        .is_some()
    {}
    egress.finish().unwrap();
    assert_eq!(resumed, delivered, "resume re-encodes the same bytes");

    // Explicit cancel through the handle is a terminal, not a lost producer; the finished
    // stream keeps its first terminal.
    handle.response.finish(Terminal::Cancelled);
    drop((attached, reader, records, upload, validated, egress));
    drop((document, media));
    drop(handle);
    let Fixture {
        mut store, directory, ..
    } = fixture;
    store.shutdown().unwrap();
    std::fs::remove_dir(&directory).unwrap();
}
