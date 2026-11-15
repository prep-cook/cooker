//! The local token count, the native compact and the selector arm.

use ck_core::exec::FailureScope;
use ck_stream::Terminal;
use ck_translate::codec::event::Direction;
use ck_translate::codec::{BodyMode, Operation, RequestIngress};
use ck_translate::{codec::chat, responses};

use crate::common::*;
use crate::support::upstream::{Reply, StreamUpstream};
use crate::support::{Streams, identity};

/// A count is local: nothing is sent, and the stream holds one canonical `count_tokens` over
/// what the generation would carry, for every protocol with a count route (Chat and Responses
/// have none).
#[tokio::test]
async fn a_count_is_local() {
    let mut counts = Vec::new();
    let anthropic = br#"{"model":"client-model","system":"be brief","messages":[{"role":"user","content":"hi"}]}"#;
    for (protocol, body) in [("anthropic", &anthropic[..]), ("gemini", GEMINI)] {
        let upstream = StreamUpstream::start([]).await;
        let (streams, exchange) = (Streams::new(), exchange(&upstream));
        let (started, id, _request) = run(&streams, &exchange, protocol, Operation::CountTokens, body, 5).await;
        let (outcome, published, _stream) = finish(&streams, started.unwrap(), id).await;
        outcome.unwrap();
        assert_eq!(upstream.requests(), 0, "{protocol}");
        assert_eq!(published.terminal, Some(Terminal::Done), "{protocol}");
        let records = records(&published);
        let count = records
            .iter()
            .find(|r| r.contains("event: x_ck.count_tokens"))
            .unwrap_or_else(|| panic!("{protocol}: {records:?}"));
        let at = count.find(r#""input_tokens":"#).unwrap() + 15;
        let tokens: u64 = count[at..]
            .chars()
            .take_while(char::is_ascii_digit)
            .collect::<String>()
            .parse()
            .unwrap();
        counts.push((protocol, tokens));
    }
    // "be brief" and "hi" for Anthropic, "hi" alone for Gemini; the image tool counts nothing,
    // having no name or parameters.
    assert!(counts[0].1 > counts[1].1 && counts[1].1 > 0, "{counts:?}");
}

/// Compact is the backend's native `/responses/compact`: buffered, no `stream` member, the
/// upstream model, and the compaction items in the canonical stream.
#[tokio::test]
async fn compact_goes_to_the_native_compact() {
    let compaction = r#"{"id":"cmp_1","object":"response.compaction","created_at":4,"model":"gpt-5","output":[{"id":"i","type":"compaction","encrypted_content":"secret"}],"usage":{"input_tokens":3,"output_tokens":1,"total_tokens":4}}"#;
    let upstream = StreamUpstream::start([Reply::json(compaction)]).await;
    let (streams, exchange) = (Streams::new(), exchange(&upstream));
    let body = br#"{"model":"client-model","input":[{"role":"user","content":"hi"}]}"#;
    let (started, id, _request) = run(&streams, &exchange, "responses", Operation::Compact, body, 64).await;
    let (outcome, published, mut stream) = finish(&streams, started.unwrap(), id).await;
    outcome.unwrap();
    assert_eq!(published.terminal, Some(Terminal::Done));
    let captured = &upstream.captured()[0];
    assert_eq!(captured.path, "/responses/compact");
    assert_eq!(captured.header("accept"), Some("application/json"));
    let sent = json_body(captured);
    assert_eq!(sent["model"], MODEL, "{sent}");
    assert!(sent.get("stream").is_none(), "{sent}");
    let out = egress_for(
        &streams,
        "responses",
        &mut stream,
        id,
        BodyMode::Buffered,
        Operation::Compact,
    )
    .await;
    assert!(out.contains("secret"), "{out}");
}

/// A compact refusal follows Go's compact policy: a missing endpoint stops the request
/// rather than cooling the model.
#[tokio::test]
async fn a_compact_refusal_is_the_request_s_fault() {
    let upstream =
        StreamUpstream::start([Reply::status(404, "application/json", r#"{"error":{"message":"no"}}"#)]).await;
    let (streams, exchange) = (Streams::new(), exchange(&upstream));
    let body = br#"{"model":"client-model","input":[{"role":"user","content":"hi"}]}"#;
    let (started, _, _request) = run(&streams, &exchange, "responses", Operation::Compact, body, 64).await;
    let error = started.err().expect("the attempt fails");
    assert_eq!((error.status, error.scope), (404, FailureScope::Request));
}

/// A streamed compact is refused at the fence: the backend's compact is buffered only.
#[tokio::test]
async fn a_streamed_compact_is_refused() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([]).await;
    let exchange = exchange(&upstream);
    let mut request = streams.request();
    let id = identity(Direction::Request);
    let mut ingress =
        responses::request::Ingress::new(&mut request, &streams.store, id, Operation::Compact, LIMIT).unwrap();
    let attempt = exchange.attempt(&streams.store);
    let body = br#"{"model":"client-model","stream":true,"input":"hi"}"#;
    let executors = selector();
    ingress.advance(&mut request, body).unwrap();
    assert!(executors.fence(&attempt, &mut ingress, &mut request).is_err());
}

fn selector() -> ck_exec::Executors {
    ck_exec::Executors {
        claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
        codex: executor(),
        devices: Default::default(),
        openai: Default::default(),
        google: Default::default(),
    }
}

/// The selector sends Codex credentials through this executor.
#[tokio::test]
async fn the_selector_runs_codex_through_the_executor() {
    let executors = selector();
    let upstream = StreamUpstream::start([Reply::sse(sse())]).await;
    let (streams, exchange) = (Streams::new(), exchange(&upstream));
    let attempt = exchange.attempt(&streams.store);
    assert_eq!(executors.destination(&attempt), Some(ck_exec::codex_exec::DESTINATION));
    let mut request = streams.request();
    let id = identity(Direction::Request);
    let mut ingress =
        chat::request::Ingress::new(&mut request, &streams.store, id, Operation::Generate, LIMIT).unwrap();
    for chunk in CHAT.chunks(16) {
        ingress.advance(&mut request, chunk).unwrap();
    }
    let validated = executors.fence(&attempt, &mut ingress, &mut request).unwrap();
    let refenced = executors.refence(&attempt, validated.view()).unwrap();
    assert_eq!(refenced.destination(), ck_exec::codex_exec::DESTINATION);
    let response = streams.response(&request);
    let id = response.id();
    let handle = exchange.request(validated);
    let started = executors
        .start(&attempt, &handle, &mut request, response)
        .await
        .unwrap();
    let (outcome, published, _stream) = finish(&streams, started, id).await;
    outcome.unwrap();
    assert_eq!(published.terminal, Some(Terminal::Done));
    assert_eq!(upstream.requests(), 1);
}
