//! Codex Alpha Search under the execution contract: the request the backend receives, the
//! result as one canonical `operation` record, refusals before any send, and failure.

use ck_core::credential::Credential;
use ck_core::exec::FailureScope;
use ck_exec::pipeline;
use ck_exec::types::Execute;
use ck_stream::Terminal;
use ck_translate::codec::document::Document;
use ck_translate::codec::media::Registry;
use ck_translate::codec::{self, Capability, ClientCapability, Operation, RequestFence, RequestView};
use ck_translate::search::{Decoder, Phase};
use serde_json::{Value, json};

use crate::common::*;
use crate::support::upstream::{Reply, StreamUpstream};
use crate::support::{Exchange, Streams, credential};

const REQUEST: &str =
    r#"{"id":"s1","model":"client-model","query":"rust","limit":3,"prompt_cache_key":"k","future":{"a":1}}"#;

fn result_body() -> String {
    json!({"results":[{"title":"T","uri":"https://e.x","text":"body","rank":1}],"next":null}).to_string()
}

/// A Codex API key on `upstream`, allowed Alpha Search unless `flag` says otherwise.
fn keyed(upstream: &StreamUpstream, flag: Option<&str>) -> Exchange {
    let mut key: Credential = credential("codex", &upstream.url(), "sk-up");
    if let Some(flag) = flag {
        key.attributes.insert("codex_alpha_search".into(), flag.into());
    }
    Exchange::new(key, MODEL, "/v1/alpha/search")
}

/// The client's search request, decoded and held until each attempt re-fences it.
fn decoded(streams: &Streams, step: usize) -> (ck_stream::Writer, Document, Registry) {
    decoded_as(streams, REQUEST, Phase::Request, step)
}

fn decoded_as(streams: &Streams, body: &str, phase: Phase, step: usize) -> (ck_stream::Writer, Document, Registry) {
    let mut writer = streams.request();
    let mut decoder = Decoder::new(&mut writer, phase, LIMIT).unwrap();
    for chunk in body.as_bytes().chunks(step) {
        decoder.advance(&mut writer, chunk).unwrap();
    }
    let document = decoder.finish(&mut writer).unwrap();
    let registry = Registry::new(&mut writer).unwrap();
    (writer, document, registry)
}

fn client_view<'a>(document: &'a Document, registry: &'a Registry) -> RequestView<'a> {
    RequestView {
        document,
        media: registry,
        operation: Operation::AlphaSearch,
    }
}

type Held = (ck_stream::Writer, Document, Registry);
type Outcome = Result<(Result<(), ck_core::exec::ExecError>, Vec<String>, Option<Terminal>), codec::Error>;

/// One attempt on `exchange`, with the pipeline's outcome, the stream id and its records.
async fn attempt(streams: &Streams, exchange: &Exchange, held: &mut Held) -> Outcome {
    run_attempt(streams, exchange, held, false).await
}

/// [`attempt`], optionally handing the pipeline the client's own fence instead of this
/// destination's re-fence, as a caller that skipped the re-fence would.
async fn run_attempt(streams: &Streams, exchange: &Exchange, held: &mut Held, client_only: bool) -> Outcome {
    let executor = executor();
    let attempt = exchange.attempt(&streams.store);
    let (upload, document, registry) = held;
    let client = RequestFence::default().end(client_view(document, registry), &ClientCapability)?;
    let validated = if client_only {
        client
    } else {
        pipeline::refence(&executor, &attempt, client_view(document, registry))?
    };
    let response = streams.response(upload);
    let id = response.id();
    let mut handle = exchange.request(validated);
    handle.client.session = Some("s1");
    let started = pipeline::start(&executor, &attempt, &handle, upload, response).await;
    let started = match started {
        Ok(started) => started,
        Err(error) => {
            let published = streams.read_to_end(id).await;
            return Ok((Err(error), records(&published), published.terminal));
        }
    };
    let (outcome, published, _stream) = finish(streams, started, id).await;
    Ok((outcome, records(&published), published.terminal))
}

/// The result reaches the stream as `response.start`, one `operation` and `done`, fed through
/// the codec whatever the chunking; the backend gets the search with its model, no cache
/// members and its other native members.
#[tokio::test]
async fn a_search_result_is_one_operation_record() {
    for reply in [Reply::json(result_body()), Reply::json(result_body()).byte_at_a_time()] {
        let upstream = StreamUpstream::start([reply]).await;
        let (streams, mut exchange) = (Streams::new(), keyed(&upstream, Some("true")));
        exchange
            .credential
            .attributes
            .insert("header:X-Sess".into(), "$CPA-SESSION-ID".into());
        // An API key identifies as cooker over a configured User-Agent.
        exchange
            .credential
            .attributes
            .insert("header:User-Agent".into(), "configured/1".into());
        let mut held = decoded(&streams, 5);
        let (outcome, records, terminal) = attempt(&streams, &exchange, &mut held).await.unwrap();
        outcome.unwrap();
        assert_eq!(terminal, Some(Terminal::Done));
        let kinds: Vec<_> = records.iter().map(|r| r.lines().next().unwrap().to_owned()).collect();
        assert_eq!(
            kinds,
            [
                "event: x_ck.response.start",
                "event: x_ck.operation",
                "event: x_ck.done"
            ]
        );
        let data = records[1].lines().find_map(|l| l.strip_prefix("data: ")).unwrap();
        let operation: Value = serde_json::from_str(data).unwrap();
        assert_eq!(operation["kind"], "alpha_search");
        assert_eq!(
            operation["value"],
            json!({"phase":"result","results":[{"title":"T","uri":"https://e.x","text":"body"}]})
        );
        let original: Vec<_> = operation["x_ck"]["original"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| (e["pointer"].clone(), e["value"].clone()))
            .collect();
        assert_eq!(
            original,
            [(json!("/next"), json!(null)), (json!("/results/0/rank"), json!(1))]
        );

        let sent = upstream.captured();
        assert_eq!(sent.len(), 1);
        assert_eq!(
            (sent[0].method.as_str(), sent[0].path.as_str()),
            ("POST", "/alpha/search")
        );
        assert_eq!(sent[0].header("authorization"), Some("Bearer sk-up"));
        assert_eq!(sent[0].header("user-agent"), Some("cooker"));
        assert_eq!(
            sent[0].header("x-sess"),
            ck_wire::session::cpa_session_id(Some("s1")).as_deref()
        );
        let body = json_body(&sent[0]);
        assert_eq!(
            body,
            json!({"id":"s1","model":MODEL,"query":"rust","limit":3,"future":{"a":1}})
        );
    }
}

/// A key without the permission, or a text-only destination, refuses before any send, so the
/// next candidate is tried with nothing spent.
#[tokio::test]
async fn a_credential_without_alpha_search_is_refused_before_any_send() {
    for flag in [None, Some("false"), Some("")] {
        let upstream = StreamUpstream::start([Reply::json(result_body())]).await;
        let (streams, exchange) = (Streams::new(), keyed(&upstream, flag));
        let mut held = decoded(&streams, 64);
        let refused = attempt(&streams, &exchange, &mut held).await;
        assert!(matches!(refused, Err(codec::Error::Unsupported)), "{flag:?}");
        assert_eq!(upstream.requests(), 0, "{flag:?}");
    }
    // Only a search request is one: a decoded result is not.
    let upstream = StreamUpstream::start([]).await;
    let (streams, exchange) = (Streams::new(), keyed(&upstream, Some("true")));
    let mut held = decoded_as(&streams, &result_body(), Phase::Result, 64);
    let refused = attempt(&streams, &exchange, &mut held).await;
    assert!(matches!(refused, Err(codec::Error::Schema)));
    assert_eq!(upstream.requests(), 0);
    // A key allowed to search still needs somewhere to send it.
    let upstream = StreamUpstream::start([]).await;
    let (streams, mut exchange) = (Streams::new(), keyed(&upstream, Some("true")));
    exchange.credential.attributes.remove("base_url");
    let mut held = decoded(&streams, 64);
    let refused = attempt(&streams, &exchange, &mut held).await;
    assert!(matches!(refused, Err(codec::Error::Unsupported)));
    // The capability is the destination's own, so the same attempt admits a generation.
    let upstream = StreamUpstream::start([]).await;
    let exchange = keyed(&upstream, None);
    let streams = Streams::new();
    let attempt = exchange.attempt(&streams.store);
    assert!(executor().capability(&attempt).admits(Operation::Generate));
    assert!(!executor().capability(&attempt).admits(Operation::AlphaSearch));
}

/// The client's own fence authorizes no send, whether or not the key may search: only the
/// destination's re-fence does.
#[tokio::test]
async fn a_client_only_fence_never_authorizes_a_send() {
    for flag in [None, Some("true")] {
        let upstream = StreamUpstream::start([Reply::json(result_body())]).await;
        let (streams, exchange) = (Streams::new(), keyed(&upstream, flag));
        let mut held = decoded(&streams, 64);
        let (outcome, records, terminal) = run_attempt(&streams, &exchange, &mut held, true).await.unwrap();
        assert!(outcome.is_err(), "{flag:?}");
        assert_eq!(
            (records.len(), terminal),
            (0, Some(Terminal::UpstreamError)),
            "{flag:?}"
        );
        assert_eq!(upstream.requests(), 0, "{flag:?}");
    }
}

/// An upstream refusal fails the first attempt before commitment, and the second candidate
/// answers the same decoded request.
#[tokio::test]
async fn a_failed_candidate_hands_the_same_request_to_the_next() {
    let limit = r#"{"error":{"type":"usage_limit_reached","message":"limit","resets_in_seconds":60}}"#;
    let (first, second) = (
        StreamUpstream::start([Reply::status(429, "application/json", limit)]).await,
        StreamUpstream::start([Reply::json(result_body())]).await,
    );
    let streams = Streams::new();
    let mut held = decoded(&streams, 64);
    let (outcome, records, terminal) = attempt(&streams, &keyed(&first, Some("true")), &mut held)
        .await
        .unwrap();
    let error = outcome.unwrap_err();
    assert_eq!((error.status, error.scope), (429, FailureScope::Credential));
    assert_eq!((records.len(), terminal), (0, Some(Terminal::UpstreamError)));
    let (outcome, records, terminal) = attempt(&streams, &keyed(&second, Some("true")), &mut held)
        .await
        .unwrap();
    outcome.unwrap();
    assert_eq!(terminal, Some(Terminal::Done));
    assert_eq!((first.requests(), second.requests(), records.len()), (1, 1, 3));
}

/// A body that is not a whole valid result never publishes a partial one: the stream ends in
/// one `error` after `response.start`.
#[tokio::test]
async fn a_cut_or_malformed_result_publishes_no_operation() {
    let body = result_body();
    let cut = body.len() / 2;
    for reply in [
        Reply::json(body.clone()).cut_after(cut),
        Reply::json(body.clone()).cut_after(cut).byte_at_a_time(),
        Reply::json(r#"{"results":[{"title":1}]}"#),
        Reply::json(format!("{body} trailing")),
    ] {
        let upstream = StreamUpstream::start([reply]).await;
        let (streams, exchange) = (Streams::new(), keyed(&upstream, Some("true")));
        let mut held = decoded(&streams, 64);
        let (outcome, records, terminal) = attempt(&streams, &exchange, &mut held).await.unwrap();
        assert!(outcome.is_err());
        assert_eq!(terminal, Some(Terminal::UpstreamError));
        assert!(records.iter().all(|r| !r.contains("x_ck.operation")), "{records:?}");
        assert_eq!(records.iter().filter(|r| r.contains("event: x_ck.error")).count(), 1);
    }
}
