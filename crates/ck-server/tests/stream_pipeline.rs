//! Client egress and commitment (`docs/EXECUTION-CONTRACT.md` §9): the client gets nothing
//! before the first deliverable byte, an attempt that ends first leaves its exchange for
//! another attempt, and a committed response never fails over or holds the producer back.

mod stream_fixture;

use std::time::Duration;

use axum::http::{HeaderMap, HeaderValue};
use ck_server::http::egress::{Failed, Framing, Output, Protocol, deliver};
use ck_server::inference::streams::{Exchange, StreamId, Uncommitted};
use ck_stream::Terminal;
use ck_translate::codec::chat::request::read_request;
use ck_translate::codec::event::Direction;
use ck_translate::codec::{Operation, RequestView};
use stream_fixture::*;

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
