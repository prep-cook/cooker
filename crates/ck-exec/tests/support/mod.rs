//! Shared helpers for executor tests (Phase D): a private ck-stream store on a manual clock,
//! the request and dependent response streams, the borrowed pieces an `Execute` call takes, the request fence, and the local upstream from
//! `ck-server/tests/support/stream_upstream.rs`.
//!
//! The producer that feeds an `Upstream` body into its response ingress is D1's
//! `pipeline` code; these helpers stop at the `Upstream` an executor returns and at the
//! records a stream holds, so a test drives the real producer between them.
#![allow(dead_code, reason = "each test target uses a subset of the harness")]

#[path = "../../../ck-server/tests/support/stream_upstream.rs"]
pub mod upstream;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use ck_core::config::Config;
use ck_core::credential::{Credential, Source};
use ck_core::exec::{Caller, ExecError, UsageSink};
use ck_exec::types::{Attempt, Body, Client, Request};
use ck_stream::{ManualClock, Read, Reader, Store, Terminal, Writer};
use ck_translate::codec::event::{Direction, Identity, LocalId};
use ck_translate::codec::{self, Capability, RequestIngress, ValidatedRequest};
use futures_util::StreamExt;
use http::HeaderMap;

/// A private store under a fresh temporary directory whose clock moves only through
/// [`Streams::advance`], so codec flush deadlines (`Writer::now`) are deterministic.
pub struct Streams {
    pub store: Store,
    pub clock: Arc<ManualClock>,
    directory: PathBuf,
}

impl Streams {
    pub fn new() -> Self {
        Self::with_frames(64)
    }

    /// A pool of `frames` frames, for storage-exhaustion cases.
    pub fn with_frames(frames: usize) -> Self {
        Self::with_limits(frames, 4 * 1024 * 1024)
    }

    /// An exchange metadata quota of `bytes`, for metadata-exhaustion cases.
    pub fn with_metadata(bytes: usize) -> Self {
        Self::with_limits(64, bytes)
    }

    fn with_limits(frames: usize, metadata: usize) -> Self {
        let directory = std::env::temp_dir().join(format!("ck-exec-{}", uuid::Uuid::new_v4()));
        let mut config = ck_stream::Config::new(directory.clone());
        config.pool_bytes = Some(frames * ck_stream::FRAME_BYTES);
        config.max_event_bytes = 16 * ck_stream::FRAME_BYTES;
        config.max_metadata_bytes = metadata;
        config.header_slots = 8;
        config.workers = 1;
        let clock = Arc::new(ManualClock::default());
        let store = Store::with_clock(config, clock.clone()).expect("open the test store");
        Self {
            store,
            clock,
            directory,
        }
    }

    /// A new exchange's request stream.
    pub fn request(&self) -> Writer {
        self.store.create().expect("create a test request stream")
    }

    /// An attempt's fresh response stream, which depends on the exchange's `request` stream.
    pub fn response(&self, request: &Writer) -> Writer {
        self.store
            .create_dependent(request)
            .expect("create a test response stream")
    }

    pub fn advance(&self, by: Duration) {
        self.clock.advance(by);
    }

    /// A reader from the start of stream `id`. Holding it unread is a stalled client.
    pub fn reader(&self, id: u64) -> Reader {
        self.store.reader(id, 0).expect("open a test reader")
    }

    /// The records stream `id` has published so far, without waiting, and its terminal if
    /// one was published.
    pub fn published(&self, id: u64) -> Published {
        let mut reader = self.reader(id);
        let mut published = Published::default();
        while let Poll::Ready(Some(read)) = reader.try_next() {
            published.push(read);
        }
        published
    }

    /// Every record of stream `id` up to its terminal, waiting for the producer.
    pub async fn read_to_end(&self, id: u64) -> Published {
        let mut reader = self.reader(id);
        let mut published = Published::default();
        while let Some(read) = reader.next().await {
            published.push(read);
        }
        published
    }
}

impl Default for Streams {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Streams {
    fn drop(&mut self) {
        self.store.shutdown().expect("shut the test store down");
        std::fs::remove_dir(&self.directory).expect("the test store leaves no files");
    }
}

/// Records copied out of a stream for assertions, and its terminal.
#[derive(Debug, Default)]
pub struct Published {
    pub records: Vec<Vec<u8>>,
    pub terminal: Option<Terminal>,
}

impl Published {
    fn push(&mut self, read: Read) {
        match read {
            Read::Record(lease) => {
                let mut record = Vec::new();
                lease
                    .visit(|chunk| record.extend_from_slice(chunk))
                    .expect("read a published record");
                self.records.push(record);
            }
            Read::Terminal(terminal) => self.terminal = Some(terminal),
        }
    }

    /// All record bytes in order.
    pub fn bytes(&self) -> Vec<u8> {
        self.records.concat()
    }
}

/// A codec identity for attempt 1 of a test request.
pub fn identity(direction: Direction) -> Identity {
    Identity {
        request_id: LocalId::new("request").expect("a valid local id"),
        stream_id: LocalId::new("stream").expect("a valid local id"),
        attempt: 1,
        direction,
    }
}

pub type ResponsesEgress = ck_translate::responses::egress::Egress<fn(&'static str, u64)>;

/// The Responses client egress of the test exchange, whose response id is `resp_test`.
pub fn responses_egress(
    stream: &mut Writer,
    operation: codec::Operation,
    mode: codec::BodyMode,
) -> codec::Result<ResponsesEgress> {
    let id = LocalId::new("resp_test").expect("a valid local id");
    let provider = LocalId::new("test").expect("a valid local id");
    let producer = codec::identity::Producer {
        provider,
        credential: [0; 32],
    };
    ck_translate::responses::egress::Egress::new(stream, id, producer, operation, mode, |_, _| {})
}

/// Feeds `body` into `ingress` `step` bytes at a time and closes the fence against
/// `destination`. `owner` is the writer the ingress was created on.
pub fn fence<'a, I: RequestIngress>(
    ingress: &'a mut I,
    owner: &mut Writer,
    body: &[u8],
    step: usize,
    destination: &impl Capability,
) -> codec::Result<ValidatedRequest<'a>> {
    for chunk in body.chunks(step) {
        ingress.advance(owner, chunk)?;
    }
    ingress.finish(owner, destination)
}

/// A config-backed API-key credential for `provider` whose `base_url` is the local upstream.
pub fn credential(provider: &str, base_url: &str, api_key: &str) -> Credential {
    Credential {
        id: format!("{provider}-test"),
        provider: provider.into(),
        source: Source::Config {
            section: format!("{provider}-api-key"),
            index: 0,
        },
        disabled: false,
        label: provider.into(),
        attributes: BTreeMap::from([
            ("base_url".to_owned(), base_url.to_owned()),
            ("api_key".to_owned(), api_key.to_owned()),
        ]),
        metadata: serde_json::Map::new(),
        revision: 1,
    }
}

/// The values an attempt and a request handle borrow. Fields are public so a test can
/// change the credential, config, model or client headers before borrowing.
pub struct Exchange {
    pub credential: Credential,
    pub cfg: Config,
    pub model: String,
    pub caller: Caller,
    pub headers: HeaderMap,
    pub usage: UsageSink,
    pub route: String,
    pub alt: Option<String>,
    pub streaming: Option<bool>,
    pub session: Option<String>,
    pub derived_session: Option<String>,
}

impl Exchange {
    /// An attempt on `credential` for upstream `model` with the default config, from a
    /// configured client key over `route`.
    pub fn new(credential: Credential, model: &str, route: &str) -> Self {
        Self {
            credential,
            cfg: Config::default(),
            model: model.into(),
            caller: Caller {
                principal: "sk-client-test".into(),
                source: "authorization",
                provider: "config-inline".into(),
            },
            headers: HeaderMap::new(),
            usage: UsageSink::default(),
            route: route.into(),
            alt: None,
            streaming: None,
            session: None,
            derived_session: None,
        }
    }

    /// The attempt over `store`, which response ingress constructors read their stream from.
    pub fn attempt<'a>(&'a self, store: &'a Store) -> Attempt<'a> {
        Attempt {
            credential: &self.credential,
            cfg: &self.cfg,
            model: &self.model,
            headers: &self.headers,
            resolved_model: None,
            store,
            identity: identity(Direction::Response),
        }
    }

    pub fn request<'a>(&'a self, validated: ValidatedRequest<'a>) -> Request<'a> {
        self.request_with(validated, &[])
    }

    /// [`Self::request`] whose client sent `original` (contract G5).
    pub fn request_with<'a>(&'a self, validated: ValidatedRequest<'a>, original: &'a [u8]) -> Request<'a> {
        Request {
            validated,
            client: Client {
                caller: &self.caller,
                headers: &self.headers,
                route: &self.route,
                alt: self.alt.as_deref(),
                streaming: self.streaming,
                session: self.session.as_deref(),
                execution_session: None,
                derived_session: self.derived_session.as_deref(),
                usage: &self.usage,
                original,
            },
        }
    }
}

/// Reads an upstream body to its end: the chunks in arrival order, and the error that
/// ended it, if any.
pub async fn drain(mut body: Body) -> (Vec<Bytes>, Option<ExecError>) {
    let mut chunks = Vec::new();
    while let Some(item) = body.next().await {
        match item {
            Ok(chunk) => chunks.push(chunk),
            Err(error) => return (chunks, Some(error)),
        }
    }
    (chunks, None)
}
