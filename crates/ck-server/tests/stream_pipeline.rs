//! Client egress and commitment (`docs/EXECUTION-CONTRACT.md` §9): the client gets nothing
//! before the first deliverable byte, an attempt that ends first leaves its exchange for
//! another attempt, and a committed response never fails over or holds the producer back.
//! The attempt loop around it (D2-PLAN §3.2, §5.2) settles and discards each failed attempt
//! before the next one starts.

mod stream_fixture;

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::http::{HeaderMap, HeaderValue, StatusCode};
use bytes::Bytes;
use ck_core::config::Config;
use ck_core::credential::Credential;
use ck_core::exec::{ExecError, FailureScope};
use ck_core::format::Format;
use ck_exec::pipeline::Started;
use ck_exec::types::{Attempt, Body, Counted, Execute, Request, Streams, Upstream};
use ck_server::Runtime;
use ck_server::http::egress::{Failed, Framing, Output, Protocol, deliver};
use ck_server::inference::dispatch::{self, Arms, Call, Codec, Served, Trace};
use ck_server::inference::streams::{Exchange, StreamId, Uncommitted};
use ck_stream::{Terminal, Writer};
use ck_translate::codec::chat::request::{ChatCapability, read_request};
use ck_translate::codec::chat::response::Ingress as ChatIngress;
use ck_translate::codec::event::{Direction, LocalId};
use ck_translate::codec::{self, BodyMode, Operation, RequestFence, RequestView, ResponseIngress, ValidatedRequest};
use futures_util::{StreamExt, stream};
use serde_json::Value;
use stream_fixture::*;
use tokio::sync::oneshot;

/// Long enough for a published record to reach a waiting delivery.
const SETTLE: Duration = Duration::from_millis(200);

fn output(protocol: Protocol, framing: Framing) -> Output {
    Output {
        protocol,
        operation: Operation::Generate,
        framing,
    }
}

fn zero(framing: Framing) -> &'static str {
    if framing == Framing::Sse { "v1.e.0" } else { "v1.b.0" }
}

/// Starts `output`'s exchange and delivers it: `steps` runs against the attempt while the
/// delivery waits, and the delivery's result comes back with the attempt and the id of the
/// exchange's request stream.
async fn delivered(
    fixture: &Fixture,
    output: Output,
    steps: impl AsyncFnOnce(
        &mut Live,
        &mut std::pin::Pin<&mut dyn Future<Output = Result<axum::response::Response, Box<Failed>>>>,
    ),
) -> (Result<axum::response::Response, Box<Failed>>, Live, u64) {
    let (mut exchange, mut live) = start(fixture, owner(KEY), output, STREAMED);
    let (document, media) = request_view(fixture, &mut exchange);
    let view = RequestView {
        document: &document,
        media: &media,
        operation: Operation::Generate,
    };
    let mut headers = HeaderMap::new();
    headers.insert("x-ck-trace-id", HeaderValue::from_static("trace"));
    let request = exchange.request.id();
    let response = live.response.allocator();
    let delivery = deliver(
        fixture.rt.streams(),
        exchange,
        &response,
        identity(Direction::Response),
        view,
        headers,
    );
    let mut delivery: std::pin::Pin<&mut dyn Future<Output = _>> = std::pin::pin!(delivery);
    steps(&mut live, &mut delivery).await;
    let result = tokio::time::timeout(Duration::from_secs(5), delivery)
        .await
        .expect("the delivery decides");
    (result, live, request)
}

/// The terminal of the stream `id`, read once every record was published.
async fn terminal(fixture: &Fixture, id: u64) -> Terminal {
    let mut reader = fixture
        .rt
        .streams()
        .with_store(|store| store.reader(id, 0))
        .unwrap()
        .unwrap();
    loop {
        match reader.next().await {
            Some(ck_stream::Read::Record(_)) => {}
            Some(ck_stream::Read::Terminal(terminal)) => return terminal,
            None => panic!("a terminal precedes the end"),
        }
    }
}

fn request_view(
    fixture: &Fixture,
    exchange: &mut Exchange,
) -> (
    ck_translate::codec::document::Document,
    ck_translate::codec::media::Registry,
) {
    let mut records = fixture
        .rt
        .streams()
        .with_store(|store| store.reader(exchange.request.id(), 0))
        .unwrap()
        .unwrap();
    read_request(&mut exchange.request, &mut records).unwrap()
}

/// Whether `delivery` is still undecided after the published records reached it.
async fn waiting(
    delivery: &mut std::pin::Pin<&mut dyn Future<Output = Result<axum::response::Response, Box<Failed>>>>,
) -> bool {
    tokio::time::timeout(SETTLE, delivery.as_mut()).await.is_err()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_first_deliverable_byte_commits_and_resume_repeats_the_body() {
    /// When the client egress first emits bytes for a three-chunk answer.
    #[derive(Clone, Copy, PartialEq)]
    enum Commits {
        /// At the first content chunk.
        FirstChunk,
        /// Only once the upstream completed: a buffered body, or a Gemini part, which the
        /// egress sends whole.
        Completion,
    }
    let fixture = Fixture::new(|_| {});
    let app = fixture.app();
    for (protocol, framing, commits) in [
        (Protocol::Chat, Framing::Sse, Commits::FirstChunk),
        (Protocol::Chat, Framing::Json, Commits::Completion),
        (Protocol::Anthropic, Framing::Sse, Commits::FirstChunk),
        (Protocol::Anthropic, Framing::Json, Commits::Completion),
        (Protocol::Responses, Framing::Sse, Commits::FirstChunk),
        (Protocol::Responses, Framing::Json, Commits::Completion),
        (Protocol::Gemini, Framing::Sse, Commits::Completion),
        (Protocol::Gemini, Framing::JsonArray, Commits::Completion),
        (Protocol::Gemini, Framing::Json, Commits::Completion),
    ] {
        let case = format!("{protocol:?} {framing:?}");
        let (result, mut live, request) = delivered(&fixture, output(protocol, framing), async |live, delivery| {
            assert!(waiting(delivery).await, "{case}: nothing commits before upstream bytes");
            live.send(chunk(r#"{"role":"assistant","content":"hel"}"#, "null"));
            if commits == Commits::Completion {
                assert!(
                    waiting(delivery).await,
                    "{case}: output that is not yet deliverable does not commit"
                );
                answer_rest(live);
            }
        })
        .await;
        let Ok(response) = result else {
            panic!("{case}: the attempt did not commit");
        };
        let headers = response.headers().clone();
        live.id = StreamId::parse(headers["x-ck-stream-id"].as_bytes()).expect("the stream id");
        assert_eq!(headers["x-ck-stream-cursor"], zero(framing), "{case}");
        assert_eq!(headers["x-ck-trace-id"], "trace", "{case}");
        assert_eq!(
            terminal(&fixture, request).await,
            Terminal::Done,
            "{case}: commitment ends the request"
        );
        if commits == Commits::FirstChunk {
            answer_rest(&mut live);
        }
        let (text, clean) = body(response).await;
        assert!(clean && text.contains("hel"), "{case}: {text}");
        assert_eq!(live.stopped().await, Terminal::Done, "{case}");
        let resumed = body(resume(&app, &live, zero(framing)).await).await;
        assert_eq!(resumed, (text, true), "{case}: resume repeats the live body");
    }
}

fn answer_rest(live: &mut Live) {
    live.send(chunk(r#"{"content":"lo"}"#, "null"));
    live.send(chunk("{}", r#""stop""#));
    live.send(done());
    live.end();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_attempt_that_ends_before_its_first_byte_leaves_the_exchange() {
    #[derive(Clone, Copy, Debug)]
    enum End {
        /// The upstream fails before any chunk.
        FailAtOnce,
        /// One content chunk arrives, then the upstream fails.
        FailAfterContent,
        /// The stream is cancelled before any chunk.
        Cancel,
    }
    let fixture = Fixture::new(|_| {});
    for (protocol, framing, end) in [
        (Protocol::Chat, Framing::Sse, End::FailAtOnce),
        (Protocol::Responses, Framing::Sse, End::FailAtOnce),
        (Protocol::Anthropic, Framing::Sse, End::FailAtOnce),
        (Protocol::Chat, Framing::Json, End::FailAfterContent),
        (Protocol::Responses, Framing::Json, End::FailAfterContent),
        (Protocol::Gemini, Framing::Json, End::FailAfterContent),
        (Protocol::Chat, Framing::Sse, End::Cancel),
    ] {
        let case = format!("{protocol:?} {framing:?} {end:?}");
        let (result, mut live, _) = delivered(&fixture, output(protocol, framing), async |live, _| match end {
            End::FailAtOnce => live.fail(),
            End::FailAfterContent => {
                live.send(chunk(r#"{"role":"assistant","content":"hel"}"#, "null"));
                live.fail();
            }
            End::Cancel => live.response.finish(Terminal::Cancelled),
        })
        .await;
        let Err(failed) = result else {
            panic!("{case}: the attempt committed before its first byte");
        };
        let Failed { exchange, reason } = *failed;
        let expected = match end {
            End::Cancel => matches!(reason, Uncommitted::Terminal(Terminal::Cancelled)),
            End::FailAtOnce | End::FailAfterContent => matches!(reason, Uncommitted::Error),
        };
        assert!(expected, "{case}: {reason:?}");
        assert_eq!(
            exchange.output,
            output(protocol, framing),
            "{case}: the exchange is left"
        );
        // The request stream stays open for the next attempt's response stream.
        fixture
            .rt
            .streams()
            .with_store(|store| store.create_dependent(&exchange.request))
            .unwrap()
            .expect("the next attempt depends on the request stream")
            .finish(Terminal::Gone);
        live.stopped().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_committed_response_never_holds_back_the_producer() {
    #[derive(Clone, Copy, Debug)]
    enum Client {
        /// Reads nothing more after commitment.
        Throttled,
        /// The first write fails: the connection is gone.
        Disconnected,
    }
    let fixture = Fixture::new(|_| {});
    let app = fixture.app();
    for client in [Client::Throttled, Client::Disconnected] {
        let (result, mut live, _) = delivered(&fixture, output(Protocol::Chat, Framing::Sse), async |live, _| {
            live.send(chunk(r#"{"role":"assistant","content":"hel"}"#, "null"));
        })
        .await;
        let Ok(response) = result else {
            panic!("{client:?}: the attempt did not commit");
        };
        live.id = StreamId::parse(response.headers()["x-ck-stream-id"].as_bytes()).unwrap();
        let held = match client {
            Client::Throttled => Some(response),
            Client::Disconnected => None,
        };
        answer_rest(&mut live);
        assert_eq!(
            live.stopped().await,
            Terminal::Done,
            "{client:?}: the upstream finishes"
        );
        let (text, clean) = body(resume(&app, &live, "v1.e.0").await).await;
        assert!(
            clean && text.contains("lo") && text.contains("[DONE]"),
            "{client:?}: {text}"
        );
        if let Some(response) = held {
            assert_eq!(body(response).await, (text, true), "{client:?}");
        }
    }
}

/// What the fake contract arm does for one credential.
#[derive(Clone, Copy, Debug)]
enum Script {
    /// `destination()` is `None`: the old arm runs against the refusing loopback upstream.
    Old,
    /// `refence` answers this error.
    Refence(fn() -> codec::Error),
    /// The executor fails before any response record.
    Refused,
    /// The executor publishes a usage snapshot, then fails.
    UsageThenRefused,
    /// The executor fails with a stream resource error.
    StorageExhausted,
    /// The upstream body fails before any content.
    BodyFails,
    /// The upstream answers in full.
    Answers,
    /// One content chunk, then the body fails once the test releases it.
    FailsAfterContent,
    /// One content chunk and a body error, both ready at once.
    FailsAtOnce,
    /// The whole answer with 7 input tokens of usage, once the test releases it.
    Held,
}

/// A usage-only Chat chunk: 7 input tokens.
const USAGE: &str = "data: {\"id\":\"r\",\"model\":\"m\",\"created\":1,\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3,\"total_tokens\":10}}\n\n";

/// One `start` call, recorded on entry.
struct Begun {
    credential: String,
    stream: u64,
    identity: String,
    attempt: u64,
    /// Whether the previous attempt's response stream was already discarded.
    previous_gone: Option<bool>,
    /// Usage records enqueued before this attempt started.
    records: usize,
}

/// The contract arm scripted per credential id; `start` runs the real `pipeline::start`.
struct Fake {
    rt: Arc<Runtime>,
    scripts: [(&'static str, Script); 2],
    begun: Mutex<Vec<Begun>>,
    records: Mutex<Vec<Value>>,
    release: Mutex<Option<oneshot::Receiver<()>>>,
}

impl Fake {
    fn script(&self, attempt: &Attempt<'_>) -> Script {
        let id = attempt.credential.id.rsplit('/').next().unwrap();
        let script = self.scripts.iter().find(|(credential, _)| *credential == id);
        script.expect("a scripted credential").1
    }

    /// Moves the enqueued usage records into `records`; how many there are.
    fn drain(&self) -> usize {
        let mut records = self.records.lock().unwrap();
        let popped = self.rt.usage_queue().pop_oldest(usize::MAX);
        records.extend(
            popped
                .iter()
                .map(|record| serde_json::from_slice::<Value>(record).unwrap()),
        );
        records.len()
    }

    /// The usage records once `count` arrived or five seconds passed, and any that follow
    /// within the settle window.
    async fn records(&self, count: usize) -> Vec<Value> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while self.drain() < count && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        self.drain();
        self.records.lock().unwrap().clone()
    }
}

impl Arms for Fake {
    fn destination(&self, attempt: &Attempt<'_>) -> Option<&'static str> {
        match self.script(attempt) {
            Script::Old => None,
            _ => Some("chat"),
        }
    }

    fn refence<'a>(&self, attempt: &Attempt<'_>, view: RequestView<'a>) -> codec::Result<ValidatedRequest<'a>> {
        match self.script(attempt) {
            Script::Refence(error) => Err(error()),
            _ => RequestFence::default().end(view, &ChatCapability),
        }
    }

    async fn start(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        upload: &mut Writer,
        response: Writer,
    ) -> Result<Started, ExecError> {
        let records = self.drain();
        {
            let mut begun = self.begun.lock().unwrap();
            let previous = begun.last().map(|previous| previous.stream);
            begun.push(Begun {
                credential: attempt.credential.id.rsplit('/').next().unwrap().to_owned(),
                stream: response.id(),
                identity: attempt.identity.stream_id.as_str().to_owned(),
                attempt: attempt.identity.attempt,
                previous_gone: previous.map(|id| matches!(attempt.store.reader(id, 0), Err(ck_stream::Error::Gone))),
                records,
            });
        }
        let executor = Executor {
            script: self.script(attempt),
            release: &self.release,
        };
        ck_exec::pipeline::start(&executor, attempt, request, upload, response).await
    }
}

/// A Chat upstream following one [`Script`].
struct Executor<'f> {
    script: Script,
    release: &'f Mutex<Option<oneshot::Receiver<()>>>,
}

impl Execute for Executor<'_> {
    type Capability = ChatCapability;
    type Ingress = ChatIngress;

    fn capability(&self, _attempt: &Attempt<'_>) -> ChatCapability {
        ChatCapability
    }

    async fn execute(
        &self,
        attempt: &Attempt<'_>,
        _request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<ChatIngress>, ExecError> {
        let mut ingress = ChatIngress::new(
            streams.response,
            attempt.store,
            attempt.identity,
            Operation::Generate,
            BodyMode::Streaming,
            1 << 20,
        )
        .unwrap();
        let refused = || ExecError::local(503, FailureScope::Transport, "refused");
        let broken = || ExecError::local(502, FailureScope::Transport, "upstream");
        let content = || chunk(r#"{"role":"assistant","content":"hel"}"#, "null");
        let body: Body = match self.script {
            Script::Refused => return Err(refused()),
            Script::StorageExhausted => {
                return Err(ExecError::local(503, FailureScope::Request, "stream.storage_exhausted"));
            }
            Script::UsageThenRefused => {
                ingress.advance(streams.response, USAGE.as_bytes()).unwrap();
                if ingress.flush_deadline().is_some() {
                    ingress.flush(streams.response).unwrap();
                }
                return Err(refused());
            }
            Script::BodyFails => stream::iter([Err(broken())]).boxed(),
            Script::FailsAtOnce => stream::iter([Ok(content()), Err(broken())]).boxed(),
            Script::Held => {
                let release = self.release.lock().unwrap().take().unwrap();
                let answer = [
                    content(),
                    Bytes::from_static(USAGE.as_bytes()),
                    chunk("{}", r#""stop""#),
                    done(),
                ];
                stream::once(release)
                    .flat_map(move |_| stream::iter(answer.clone().map(Ok)))
                    .boxed()
            }
            Script::Answers => stream::iter(
                [
                    content(),
                    chunk(r#"{"content":"lo"}"#, "null"),
                    chunk("{}", r#""stop""#),
                    done(),
                ]
                .map(Ok),
            )
            .boxed(),
            Script::FailsAfterContent => {
                let release = self.release.lock().unwrap().take().unwrap();
                stream::iter([Ok(content())])
                    .chain(stream::once(async move {
                        let _ = release.await;
                        Err(broken())
                    }))
                    .boxed()
            }
            Script::Old | Script::Refence(_) => unreachable!("never started"),
        };
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
        _request: &Request<'_>,
        _streams: Streams<'_>,
    ) -> Result<Counted<ChatIngress>, ExecError> {
        unreachable!("generation only")
    }

    async fn compact(
        &self,
        _attempt: &Attempt<'_>,
        _request: &Request<'_>,
        _streams: Streams<'_>,
    ) -> Result<Upstream<ChatIngress>, ExecError> {
        unreachable!("generation only")
    }
}

/// A loopback upstream that answers every request 503: the old arm's Claude executor; its
/// request count.
async fn refusing_upstream() -> (String, Arc<AtomicUsize>) {
    let sends = Arc::new(AtomicUsize::new(0));
    let counted = sends.clone();
    let app = axum::Router::new().fallback(move || {
        counted.fetch_add(1, Ordering::SeqCst);
        async { (StatusCode::SERVICE_UNAVAILABLE, "unavailable") }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (url, sends)
}

fn credential(id: &str) -> Credential {
    let mut metadata = serde_json::Map::new();
    metadata.insert("type".into(), "claude".into());
    metadata.insert("access_token".into(), format!("fake-{id}").into());
    // An operator rule that fails a 503 over: a stream resource failure must stop anyway.
    metadata.insert(
        "request_scoped_errors".into(),
        serde_json::json!([{"status": 503, "match": ["stream.storage_exhausted"], "action": "continue-and-cooldown"}]),
    );
    let path = Path::new("/mock");
    ck_server::testing::local(Credential::from_file(path, &path.join(id), metadata).unwrap())
}

const CLIENT: &[u8] = br#"{"model":"claude-sonnet-5","messages":[{"role":"user","content":"hi"}],"stream":true}"#;

fn call() -> Call {
    Call {
        entry: Format::OpenAI,
        response: Format::OpenAI,
        operation: ck_core::exec::Operation::Generate,
        model: "claude-sonnet-5".into(),
        body: Bytes::from_static(CLIENT),
        stream: true,
        alt: None,
        headers: HeaderMap::new(),
        caller: owner(KEY).caller,
        forced_provider: None,
        selection_model: None,
        execution_session: None,
        request_path: "/v1/chat/completions".into(),
        peer: None,
        turn: None,
        media: None,
    }
}

#[tokio::test]
async fn each_attempt_settles_and_is_discarded_before_the_next() {
    /// How the call ends.
    #[derive(Clone, Copy, Debug)]
    enum Ends {
        /// The answer commits. `clean`: the whole answer and nothing from a failed attempt.
        Commits { clean: bool },
        /// No candidate committed: the call's status.
        Status(u16),
        /// The client goes away while the egress waits for the first byte; the upstream
        /// then completes.
        Gone,
    }
    struct Row {
        case: &'static str,
        /// Credentials `a` and `b`. A fresh runtime's round robin tries `a` first, as
        /// `scheduler_attempts.rs` relies on.
        scripts: [Script; 2],
        /// The exchange's request stream ends with this terminal before the call.
        request: Option<Terminal>,
        ends: Ends,
        /// Credentials whose contract-arm attempt started, in order.
        started: &'static [&'static str],
        /// Usage records enqueued before each started attempt.
        settled: &'static [usize],
        /// `failed` of each usage record, oldest first.
        failed: &'static [bool],
        /// Input tokens of the first usage record.
        input_tokens: Option<i64>,
        /// Requests the old arm sent upstream.
        old: usize,
        /// Credentials cooling down for the model afterwards; a skip or a resource failure
        /// is neutral and cools nothing.
        cooled: &'static [&'static str],
    }
    let unsupported = || codec::Error::Unsupported;
    let rows = [
        Row {
            case: "an executor error before any record fails over",
            scripts: [Script::Refused, Script::Answers],
            request: None,
            ends: Ends::Commits { clean: true },
            started: &["a", "b"],
            settled: &[0, 1],
            failed: &[true, false],
            input_tokens: None,
            old: 0,
            cooled: &[],
        },
        Row {
            case: "a usage snapshot before an executor error is kept",
            scripts: [Script::UsageThenRefused, Script::Answers],
            request: None,
            ends: Ends::Commits { clean: true },
            started: &["a", "b"],
            settled: &[0, 1],
            failed: &[true, false],
            input_tokens: Some(7),
            old: 0,
            cooled: &[],
        },
        Row {
            case: "a producer error before commitment fails over",
            scripts: [Script::BodyFails, Script::Answers],
            request: None,
            ends: Ends::Commits { clean: true },
            started: &["a", "b"],
            settled: &[0, 1],
            failed: &[true, false],
            input_tokens: None,
            old: 0,
            cooled: &[],
        },
        Row {
            case: "every candidate refuses",
            scripts: [Script::Refence(unsupported), Script::Refence(unsupported)],
            request: None,
            ends: Ends::Status(400),
            started: &[],
            settled: &[],
            failed: &[],
            input_tokens: None,
            old: 0,
            cooled: &[],
        },
        Row {
            case: "an unsupported request skips to the next candidate",
            scripts: [Script::Refence(unsupported), Script::Answers],
            request: None,
            ends: Ends::Commits { clean: true },
            started: &["b"],
            settled: &[0],
            failed: &[false],
            input_tokens: None,
            old: 0,
            cooled: &[],
        },
        Row {
            case: "a schema refusal skips to the next candidate",
            scripts: [Script::Refence(|| codec::Error::Schema), Script::Answers],
            request: None,
            ends: Ends::Commits { clean: true },
            started: &["b"],
            settled: &[0],
            failed: &[false],
            input_tokens: None,
            old: 0,
            cooled: &[],
        },
        Row {
            case: "exhausted storage at the fence stops",
            scripts: [
                Script::Refence(|| codec::Error::Storage(ck_stream::Error::StorageExhausted)),
                Script::Answers,
            ],
            request: None,
            ends: Ends::Status(503),
            started: &[],
            settled: &[],
            failed: &[],
            input_tokens: None,
            old: 0,
            cooled: &[],
        },
        Row {
            case: "a gone stream at the fence stops",
            scripts: [
                Script::Refence(|| codec::Error::Storage(ck_stream::Error::Gone)),
                Script::Answers,
            ],
            request: None,
            ends: Ends::Status(410),
            started: &[],
            settled: &[],
            failed: &[],
            input_tokens: None,
            old: 0,
            cooled: &[],
        },
        Row {
            case: "a codec defect at the fence stops",
            scripts: [Script::Refence(|| codec::Error::Position), Script::Answers],
            request: None,
            ends: Ends::Status(500),
            started: &[],
            settled: &[],
            failed: &[],
            input_tokens: None,
            old: 0,
            cooled: &[],
        },
        Row {
            case: "a cancelled request stops",
            scripts: [
                Script::Refence(|| codec::Error::Storage(ck_stream::Error::Closed)),
                Script::Answers,
            ],
            request: Some(Terminal::Cancelled),
            ends: Ends::Status(499),
            started: &[],
            settled: &[],
            failed: &[],
            input_tokens: None,
            old: 0,
            cooled: &[],
        },
        Row {
            case: "a request ended by shutdown stops",
            scripts: [
                Script::Refence(|| codec::Error::Storage(ck_stream::Error::Closed)),
                Script::Answers,
            ],
            request: Some(Terminal::Shutdown),
            ends: Ends::Status(503),
            started: &[],
            settled: &[],
            failed: &[],
            input_tokens: None,
            old: 0,
            cooled: &[],
        },
        Row {
            case: "a request ended by exhausted storage stops",
            scripts: [
                Script::Refence(|| codec::Error::Storage(ck_stream::Error::Closed)),
                Script::Answers,
            ],
            request: Some(Terminal::StorageExhausted),
            ends: Ends::Status(503),
            started: &[],
            settled: &[],
            failed: &[],
            input_tokens: None,
            old: 0,
            cooled: &[],
        },
        Row {
            case: "an executor resource error stops",
            scripts: [Script::StorageExhausted, Script::Answers],
            request: None,
            ends: Ends::Status(503),
            started: &["a"],
            settled: &[0],
            failed: &[true],
            input_tokens: None,
            old: 0,
            cooled: &[],
        },
        Row {
            case: "a failure after commitment does not retry",
            scripts: [Script::FailsAfterContent, Script::Answers],
            request: None,
            ends: Ends::Commits { clean: false },
            started: &["a"],
            settled: &[0],
            failed: &[true],
            input_tokens: None,
            old: 0,
            cooled: &[],
        },
        Row {
            case: "an old-arm failure fails over to the contract arm",
            scripts: [Script::Old, Script::Answers],
            request: None,
            ends: Ends::Commits { clean: true },
            started: &["b"],
            settled: &[1],
            failed: &[true, false],
            input_tokens: None,
            old: 1,
            cooled: &["a"],
        },
        Row {
            case: "a producer failure ready with the first output does not commit",
            scripts: [Script::FailsAtOnce, Script::Answers],
            request: None,
            ends: Ends::Commits { clean: true },
            started: &["a", "b"],
            settled: &[0, 1],
            failed: &[true, false],
            input_tokens: None,
            old: 0,
            cooled: &[],
        },
        Row {
            case: "a client gone before the first byte still accounts the final usage",
            scripts: [Script::Held, Script::Answers],
            request: None,
            ends: Ends::Gone,
            started: &["a"],
            settled: &[0],
            failed: &[false],
            input_tokens: Some(7),
            old: 0,
            cooled: &[],
        },
    ];
    let (upstream, sends) = refusing_upstream().await;
    let usage = Config::parse("observability: {usage: {usage-statistics-enabled: true}}\n").unwrap();
    for row in rows {
        let case = row.case;
        let fixture = Fixture::with_credentials(vec![credential("a"), credential("b")], &upstream);
        fixture.rt.usage_queue().configure(true, &usage);
        let (release, released) = oneshot::channel();
        let fake = Fake {
            rt: fixture.rt.clone(),
            scripts: [("a", row.scripts[0]), ("b", row.scripts[1])],
            begun: Mutex::default(),
            records: Mutex::default(),
            release: Mutex::new(Some(released)),
        };
        let mut exchange = exchange(&fixture, owner(KEY), output(Protocol::Chat, Framing::Sse), CLIENT);
        let (document, media) = request_view(&fixture, &mut exchange);
        let request = exchange.request.id();
        if let Some(terminal) = row.request {
            exchange.request.finish(terminal);
        }
        let codec = Codec {
            exchange,
            view: RequestView {
                document: &document,
                media: &media,
                operation: Operation::Generate,
            },
            request_id: LocalId::new("request").unwrap(),
        };
        let sent = sends.load(Ordering::SeqCst);
        let trace = Trace::with_request_id(Some("trace".into()));
        let mut run = Box::pin(dispatch::run_exchange(&fixture.rt, &fake, call(), &trace, codec));
        if let Ends::Gone = row.ends {
            let started = async {
                while fake.begun.lock().unwrap().is_empty() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                tokio::time::sleep(SETTLE).await;
            };
            tokio::select! {
                _ = &mut run => panic!("{case}: answered before the upstream did"),
                () = started => {}
            }
            drop(run);
            let _ = release.send(());
        } else {
            let served = tokio::time::timeout(Duration::from_secs(5), run)
                .await
                .unwrap_or_else(|_| panic!("{case}: the call finishes"));
            match (served, row.ends) {
                (Ok(Served::Committed(response)), Ends::Commits { clean }) => {
                    let _ = release.send(());
                    let (text, ended) = tokio::time::timeout(Duration::from_secs(5), body(response))
                        .await
                        .unwrap_or_else(|_| panic!("{case}: the body ends"));
                    assert!(text.contains("hel"), "{case}: {text}");
                    if clean {
                        assert!(
                            ended && text.contains("lo") && !text.contains("error"),
                            "{case}: only the committed attempt reaches the client: {text}"
                        );
                    }
                }
                (Err(error), Ends::Status(status)) => assert_eq!(error.failure.status(), status, "{case}"),
                (Ok(Served::Old(_)), _) => panic!("{case}: the old arm answered"),
                (Ok(Served::Committed(_)), ends) => panic!("{case}: committed, expected {ends:?}"),
                (Err(error), ends) => panic!("{case}: status {}, expected {ends:?}", error.failure.status()),
            }
        }
        let records = fake.records(row.failed.len()).await;
        let failed: Vec<bool> = records
            .iter()
            .map(|record| record["failed"].as_bool().unwrap())
            .collect();
        assert_eq!(failed, row.failed, "{case}: one usage record per attempt: {records:?}");
        if let Some(tokens) = row.input_tokens {
            assert_eq!(
                records[0]["tokens"]["input_tokens"], tokens,
                "{case}: the record carries the attempt's last usage snapshot"
            );
        }
        // An exchange that ends without commitment discards its request stream; a committed
        // one keeps it for resume.
        let kept = fixture
            .rt
            .streams()
            .with_store(|store| store.reader(request, 0).map(drop))
            .unwrap();
        match row.ends {
            Ends::Commits { .. } => assert!(kept.is_ok(), "{case}: the committed request stays"),
            Ends::Status(_) | Ends::Gone => {
                assert!(
                    matches!(kept, Err(ck_stream::Error::Gone)),
                    "{case}: the request stream is discarded"
                )
            }
        }
        assert_eq!(sends.load(Ordering::SeqCst) - sent, row.old, "{case}: old-arm sends");
        let cooled: Vec<&str> = ["a", "b"]
            .into_iter()
            .filter(|id| {
                let credential = fixture.rt.store().snapshot().into_iter().find(|c| c.id.ends_with(id));
                fixture.rt.store().blocked(&credential.unwrap(), "claude-sonnet-5")
            })
            .collect();
        assert_eq!(cooled, row.cooled, "{case}: cooling credentials");
        let begun = fake.begun.lock().unwrap();
        let started: Vec<&str> = begun.iter().map(|begun| begun.credential.as_str()).collect();
        assert_eq!(started, row.started, "{case}: started attempts");
        for (i, attempt) in begun.iter().enumerate() {
            assert_eq!(attempt.attempt, i as u64 + 1, "{case}: attempt number");
            assert_eq!(
                attempt.records,
                row.settled[i],
                "{case}: attempt {} starts after the previous one accounted",
                i + 1
            );
            if let Some(previous) = i.checked_sub(1).map(|i| &begun[i]) {
                assert!(
                    attempt.stream != previous.stream && attempt.identity != previous.identity,
                    "{case}: each attempt gets a new stream"
                );
                assert_eq!(
                    attempt.previous_gone,
                    Some(true),
                    "{case}: attempt {} starts after the previous one was discarded",
                    i + 1
                );
            }
        }
    }
}
