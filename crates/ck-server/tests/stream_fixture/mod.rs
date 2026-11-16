//! Shared by the stream tests: a runtime with a private stream store under a manual clock,
//! committed Chat exchanges whose upstream the test feeds, and `GET /v1/streams`.
#![allow(dead_code, reason = "each stream test target uses part of the fixture")]

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use bytes::Bytes;
use ck_core::config::Config;
use ck_core::exec::Caller;
use ck_server::Runtime;
use ck_server::http::egress::{Framing, Output, Protocol};
use ck_server::inference::streams::{Exchange, Owner, StreamId};
use ck_stream::{ManualClock, Store, Terminal, Writer};
use ck_translate::codec::chat::{request, response};
use ck_translate::codec::event::{Direction, Identity, LocalId};
use ck_translate::codec::{BodyMode, Operation, RequestIngress, ResponseIngress};
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tower::ServiceExt;

pub const KEY: &str = "sk-owner";
pub const OTHER: &str = "sk-other";
pub const STREAMED: &[u8] =
    br#"{"model":"m","messages":[{"role":"user","content":"hi"}],"stream":true,"stream_options":{"include_usage":true}}"#;
pub const BUFFERED: &[u8] = br#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#;

pub struct Fixture {
    pub rt: Arc<Runtime>,
    pub clock: Arc<ManualClock>,
    directory: std::path::PathBuf,
}

impl Fixture {
    /// `configure` adjusts the store's settings: a 768-frame pool and 16 header slots.
    pub fn new(configure: impl FnOnce(&mut ck_stream::Config)) -> Self {
        Self::with_config("", configure)
    }

    /// [`Fixture::new`] with the runtime configured from `yaml`.
    pub fn with_config(yaml: &str, configure: impl FnOnce(&mut ck_stream::Config)) -> Self {
        Self::build(yaml, Vec::new(), "http://127.0.0.1:9", configure)
    }

    /// [`Fixture::new`] with `credentials` and the Claude executor on `claude`, a loopback URL.
    pub fn with_credentials(credentials: Vec<ck_core::credential::Credential>, claude: &str) -> Self {
        Self::build("", credentials, claude, |_| {})
    }

    /// [`Fixture::with_credentials`] with the runtime configured from `yaml`.
    pub fn with_credentials_config(
        yaml: &str,
        credentials: Vec<ck_core::credential::Credential>,
        claude: &str,
    ) -> Self {
        Self::build(yaml, credentials, claude, |_| {})
    }

    /// [`Fixture::with_credentials`] with the store's settings adjusted by `configure`.
    pub fn with_credentials_and(
        credentials: Vec<ck_core::credential::Credential>,
        claude: &str,
        configure: impl FnOnce(&mut ck_stream::Config),
    ) -> Self {
        Self::build("", credentials, claude, configure)
    }

    fn build(
        yaml: &str,
        credentials: Vec<ck_core::credential::Credential>,
        claude: &str,
        configure: impl FnOnce(&mut ck_stream::Config),
    ) -> Self {
        let directory = std::env::temp_dir().join(format!("ck-streams-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let mut config = Config::parse(yaml).unwrap();
        config.api_keys = vec![KEY.into(), OTHER.into()];
        let executors = ck_exec::Executors {
            claude: ck_exec::claude::ClaudeExecutor::new(claude).unwrap(),
            codex: Default::default(),
            devices: Default::default(),
            openai: Default::default(),
            google: Default::default(),
        };
        let rt = Arc::new(ck_server::testing::runtime(config, credentials, executors));
        let mut stream = ck_stream::Config::new(directory.join("pool"));
        stream.pool_bytes = Some(768 * ck_stream::FRAME_BYTES);
        stream.max_metadata_bytes = 4 * 1024 * 1024;
        stream.header_slots = 16;
        stream.workers = 1;
        configure(&mut stream);
        let clock = Arc::new(ManualClock::default());
        assert!(
            rt.streams()
                .install(Store::with_clock(stream, clock.clone()).unwrap())
                .is_ok()
        );
        Self { rt, clock, directory }
    }

    pub fn app(&self) -> Router {
        ck_server::router(self.rt.clone())
    }

    /// Advances the store clock and runs one maintenance pass (expiry).
    pub fn advance(&self, by: std::time::Duration) {
        self.clock.advance(by);
        self.rt.streams().with_store(|store| store.tick().unwrap()).unwrap();
    }
}

impl Fixture {
    /// A private directory removed with the fixture.
    pub fn directory(&self) -> &std::path::Path {
        &self.directory
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

/// The owner the client-key middleware admits for `key` with a bearer token.
pub fn owner(key: &str) -> Owner {
    Owner {
        caller: Caller {
            principal: key.into(),
            source: "authorization",
            provider: "config-inline".into(),
        },
        scope: None,
    }
}

pub fn identity(direction: Direction) -> Identity {
    Identity {
        request_id: LocalId::new("request").unwrap(),
        stream_id: LocalId::new("stream").unwrap(),
        attempt: 1,
        direction,
    }
}

/// An exchange's attempt: its producer publishes what the test sends as upstream bytes.
pub struct Live {
    /// The handle's id once committed.
    pub id: StreamId,
    /// Allocation handle on the response stream, as the runtime's usage reader holds it.
    pub response: Writer,
    upstream: Option<mpsc::UnboundedSender<Option<Bytes>>>,
    /// The terminal the producer saw: `Done` when it finished the stream itself.
    pub producer: tokio::task::JoinHandle<Terminal>,
}

impl Live {
    pub fn send(&self, bytes: impl Into<Bytes>) {
        self.upstream.as_ref().unwrap().send(Some(bytes.into())).unwrap();
    }

    /// The upstream body ends: the producer finishes the stream with `Done`.
    pub fn end(&mut self) {
        self.upstream = None;
    }

    /// The upstream body fails: the producer publishes the upstream failure.
    pub fn fail(&mut self) {
        self.upstream.take().unwrap().send(None).unwrap();
    }

    /// The producer's terminal; a producer still running after five seconds fails the test.
    pub async fn stopped(&mut self) -> Terminal {
        tokio::time::timeout(std::time::Duration::from_secs(5), &mut self.producer)
            .await
            .expect("the producer stops")
            .unwrap()
    }

    pub fn header(&self) -> String {
        self.id.header_value().to_str().unwrap().to_owned()
    }
}

/// Runs request ingress for `body` and starts one attempt with a Chat upstream: the exchange
/// before commitment. The attempt's id is set at commitment.
pub fn start(fixture: &Fixture, owner: Owner, output: Output, body: &[u8]) -> (Exchange, Live) {
    let streams = fixture.rt.streams();
    let exchange = exchange(fixture, owner, output, body);
    let (writer, ingress) = streams
        .with_store(|store| {
            let mut writer = store.create_dependent(&exchange.request).unwrap();
            let ingress = response::Ingress::new(
                &mut writer,
                store,
                identity(Direction::Response),
                Operation::Generate,
                BodyMode::Streaming,
                1 << 20,
            )
            .unwrap();
            (writer, ingress)
        })
        .unwrap();
    let response = writer.allocator();
    let (sender, receiver) = mpsc::unbounded_channel();
    let producer = streams.spawn(produce(ingress, writer, receiver));
    let live = Live {
        id: StreamId([0; 16]),
        response,
        upstream: Some(sender),
        producer,
    };
    (exchange, live)
}

/// Runs Chat request ingress for `body`: the exchange before its first attempt.
pub fn exchange(fixture: &Fixture, owner: Owner, output: Output, body: &[u8]) -> Exchange {
    fixture
        .rt
        .streams()
        .with_store(|store| {
            let mut exchange = Exchange::new(owner, output, store).unwrap();
            let request = &mut exchange.request;
            let mut client = request::Ingress::new(
                request,
                store,
                identity(Direction::Request),
                Operation::Generate,
                1 << 20,
            )
            .unwrap();
            client.advance(request, body).unwrap();
            client.finish(request, &request::ChatCapability).unwrap();
            drop(client);
            exchange
        })
        .unwrap()
}

/// [`start`] for a Chat client, committed at once under a fresh id.
pub fn commit(fixture: &Fixture, owner: Owner, framing: Framing, body: &[u8]) -> Live {
    let output = Output {
        protocol: Protocol::Chat,
        operation: Operation::Generate,
        framing,
    };
    let (exchange, mut live) = start(fixture, owner, output, body);
    live.id = StreamId::random();
    let handle = exchange.into_handle(live.id, live.response.allocator());
    fixture.rt.streams().register(handle);
    live
}

/// D1's producer reduced to the contract: publishes upstream bytes until they end, fail or
/// the stream stops.
async fn produce(
    mut ingress: response::Ingress,
    mut writer: Writer,
    mut upstream: mpsc::UnboundedReceiver<Option<Bytes>>,
) -> Terminal {
    let stopped = writer.cancelled();
    tokio::pin!(stopped);
    loop {
        tokio::select! {
            terminal = &mut stopped => return terminal,
            bytes = upstream.recv() => match bytes {
                // Held output is flushed at once, as the producer does once its deadline passed.
                Some(Some(bytes)) => {
                    ingress.advance(&mut writer, &bytes).unwrap();
                    if ingress.flush_deadline().is_some() {
                        ingress.flush(&mut writer).unwrap();
                    }
                }
                Some(None) => {
                    ingress.fail(&mut writer, 502, "upstream").unwrap();
                    writer.finish(Terminal::UpstreamError);
                    return Terminal::UpstreamError;
                }
                None => {
                    ingress.finish(&mut writer).unwrap();
                    writer.finish(Terminal::Done);
                    return Terminal::Done;
                }
            },
        }
    }
}

pub fn chunk(delta: &str, finish: &str) -> Bytes {
    Bytes::from(format!(
        "data: {{\"id\":\"r\",\"model\":\"m\",\"created\":1,\"choices\":[{{\"index\":0,\"delta\":{delta},\"finish_reason\":{finish}}}]}}\n\n"
    ))
}

pub fn done() -> Bytes {
    Bytes::from_static(b"data: [DONE]\n\n")
}

/// Sends a whole three-chunk answer and ends the upstream body.
pub fn answer(live: &mut Live) {
    live.send(chunk(r#"{"role":"assistant","content":"hel"}"#, "null"));
    live.send(chunk(r#"{"content":"lo"}"#, "null"));
    live.send(chunk("{}", r#""stop""#));
    live.send(done());
    live.end();
}

/// `GET /v1/streams` with a bearer `key` (none when empty), `query` and extra headers.
pub async fn get(app: &Router, key: &str, query: &str, headers: &[(&str, &str)]) -> axum::response::Response {
    let mut request = Request::get(format!("/v1/streams{query}"));
    if !key.is_empty() {
        request = request.header("authorization", format!("Bearer {key}"));
    }
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    app.clone().oneshot(request.body(Body::empty()).unwrap()).await.unwrap()
}

/// A resume of `live` from `cursor` by its owner.
pub async fn resume(app: &Router, live: &Live, cursor: &str) -> axum::response::Response {
    get(
        app,
        KEY,
        &format!("?after={cursor}"),
        &[("x-ck-stream-id", &live.header())],
    )
    .await
}

/// The whole body and whether it ended cleanly; an abnormal close keeps what came first.
pub async fn body(response: axum::response::Response) -> (String, bool) {
    let mut stream = response.into_body().into_data_stream();
    let mut out = Vec::new();
    while let Some(frame) = stream.next().await {
        match frame {
            Ok(bytes) => out.extend_from_slice(&bytes),
            Err(_) => return (String::from_utf8(out).unwrap(), false),
        }
    }
    (String::from_utf8(out).unwrap(), true)
}

pub async fn status_body(response: axum::response::Response) -> (StatusCode, String) {
    let status = response.status();
    (status, body(response).await.0)
}

/// The complete SSE events of `text`, each with its blank line.
pub fn events(text: &str) -> Vec<String> {
    text.split_inclusive("\n\n").map(str::to_owned).collect()
}
