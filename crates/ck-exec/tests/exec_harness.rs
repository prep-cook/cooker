//! Self-test of the executor test harness (`support/mod.rs`): a probe Chat executor runs
//! through the fence, attempt and request helpers against the local upstream, and the
//! stream helpers read what a writer published under the manual clock.

mod support;

use std::time::Duration;

use ck_core::exec::{ExecError, FailureScope};
use ck_exec::types::{Attempt, Counted, Execute, Request, Streams as AttemptStreams, Upstream};
use ck_stream::Terminal;
use ck_translate::codec::chat::request::{self, ChatCapability, Egress};
use ck_translate::codec::chat::response;
use ck_translate::codec::event::Direction;
use ck_translate::codec::{BodyMode, Operation, RequestEgress};
use futures_util::{StreamExt, TryStreamExt};
use support::upstream::{Reply, StreamUpstream};
use support::{Exchange, Streams, credential, drain, fence, identity};

const REQUEST: &[u8] = br#"{"model":"client-model","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
const SSE: &[u8] = b"data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n";

/// Encodes through Chat request egress and posts to the credential's `base_url`.
struct Probe;

fn transport(error: impl std::fmt::Display) -> ExecError {
    ExecError::local(502, FailureScope::Transport, error.to_string())
}

impl Execute for Probe {
    type Capability = ChatCapability;
    type Ingress = response::Ingress;

    fn capability(&self, _attempt: &Attempt<'_>) -> ChatCapability {
        ChatCapability
    }

    async fn execute(
        &self,
        attempt: &Attempt<'_>,
        request: &mut Request<'_>,
        streams: AttemptStreams<'_>,
    ) -> Result<Upstream<response::Ingress>, ExecError> {
        let egress = Egress {
            destination: "chat",
            upstream_stream: true,
        };
        let lease = egress
            .encode(&request.validated(), streams.request)
            .map_err(transport)?;
        let mut body = Vec::with_capacity(lease.len());
        while body.len() < lease.len() {
            body.extend_from_slice(lease.chunk(body.len()).map_err(transport)?);
        }
        let attributes = &attempt.credential.attributes;
        let res = wreq::Client::new()
            .post(format!("{}/v1/chat/completions", attributes["base_url"]))
            .bearer_auth(&attributes["api_key"])
            .body(body)
            .send()
            .await
            .map_err(transport)?;
        let status = res.status().as_u16();
        if status != 200 {
            let body = res.bytes().await.map_err(transport)?;
            return Err(ExecError::local(
                status,
                FailureScope::Credential,
                String::from_utf8_lossy(&body),
            ));
        }
        let ingress = response::Ingress::new(
            streams.response,
            attempt.store,
            identity(Direction::Response),
            Operation::Generate,
            BodyMode::Streaming,
            1 << 20,
        )
        .map_err(transport)?;
        Ok(Upstream {
            status,
            headers: res.headers().clone(),
            body: res.bytes_stream().map_err(transport).boxed(),
            ingress,
        })
    }

    async fn count_tokens(
        &self,
        _attempt: &Attempt<'_>,
        _request: &mut Request<'_>,
        _streams: AttemptStreams<'_>,
    ) -> Result<Counted<response::Ingress>, ExecError> {
        Err(ExecError::local(400, FailureScope::Request, "probe counts nothing"))
    }

    async fn compact(
        &self,
        _attempt: &Attempt<'_>,
        _request: &mut Request<'_>,
        _streams: AttemptStreams<'_>,
    ) -> Result<Upstream<response::Ingress>, ExecError> {
        Err(ExecError::local(400, FailureScope::Request, "probe compacts nothing"))
    }
}

/// Runs the probe for one request against `upstream`, the request fed one byte at a time.
async fn run(streams: &Streams, upstream: &StreamUpstream) -> Result<Upstream<response::Ingress>, ExecError> {
    let exchange = Exchange::new(
        credential("openai-compatibility", &upstream.url(), "sk-up"),
        "up-model",
        "/v1/chat/completions",
    );
    let mut request_stream = streams.request();
    let mut response_stream = streams.response(&request_stream);
    let mut ingress = request::Ingress::new(
        &mut request_stream,
        &streams.store,
        identity(Direction::Request),
        Operation::Generate,
        1 << 20,
    )
    .unwrap();
    let attempt = exchange.attempt(&streams.store);
    let validated = fence(
        &mut ingress,
        &mut request_stream,
        REQUEST,
        1,
        &Probe.capability(&attempt),
    )
    .unwrap();
    let mut request = exchange.request(validated);
    let attempt_streams = AttemptStreams {
        request: &mut request_stream,
        response: &mut response_stream,
    };
    Probe.execute(&attempt, &mut request, attempt_streams).await
}

#[tokio::test]
async fn executor_reaches_the_upstream_and_its_body_streams() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::sse(SSE).byte_at_a_time()]).await;
    let upstream_response = run(&streams, &upstream).await.unwrap();
    assert_eq!(upstream_response.status, 200);
    assert_eq!(upstream_response.headers["content-type"], "text/event-stream");
    let (chunks, error) = drain(upstream_response.body).await;
    assert!(error.is_none());
    assert_eq!(chunks.concat(), SSE);

    assert_eq!(upstream.requests(), 1);
    let captured = &upstream.captured()[0];
    assert_eq!(captured.path, "/v1/chat/completions");
    assert_eq!(captured.header("authorization"), Some("Bearer sk-up"));
    let body: serde_json::Value = serde_json::from_slice(&captured.body).unwrap();
    assert_eq!(body["messages"][0]["content"], "hi");
    assert_eq!(body["stream"], true);
}

#[tokio::test]
async fn upstream_failures_reach_the_executor() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([
        Reply::status(429, "application/json", r#"{"error":"slow down"}"#),
        Reply::reset(),
        Reply::sse(SSE).cut_after(20),
    ])
    .await;
    let refused = run(&streams, &upstream).await.err().unwrap();
    assert_eq!((refused.status, refused.scope), (429, FailureScope::Credential));
    let reset = run(&streams, &upstream).await.err().unwrap();
    assert_eq!(reset.scope, FailureScope::Transport);
    let cut = run(&streams, &upstream).await.unwrap();
    let (chunks, error) = drain(cut.body).await;
    assert_eq!(chunks.concat(), &SSE[..20]);
    assert_eq!(error.unwrap().scope, FailureScope::Transport);
    assert_eq!(upstream.requests(), 3);
}

#[tokio::test]
async fn fence_refuses_a_truncated_request() {
    let streams = Streams::new();
    let mut writer = streams.request();
    let mut ingress = request::Ingress::new(
        &mut writer,
        &streams.store,
        identity(Direction::Request),
        Operation::Generate,
        1 << 20,
    )
    .unwrap();
    assert!(
        fence(
            &mut ingress,
            &mut writer,
            &REQUEST[..REQUEST.len() - 1],
            7,
            &ChatCapability
        )
        .is_err()
    );
}

#[tokio::test]
async fn streams_read_published_records_under_the_manual_clock() {
    let streams = Streams::new();
    let mut writer = streams.request();
    let start = writer.now();
    streams.advance(Duration::from_millis(250));
    assert_eq!(writer.now() - start, Duration::from_millis(250));

    let mut stalled = streams.reader(writer.id());
    writer.append(b"event: one\n\n").unwrap();
    writer.append(b"event: two\n\n").unwrap();
    let published = streams.published(writer.id());
    assert_eq!(published.records.len(), 2);
    assert_eq!(published.terminal, None);

    let id = writer.id();
    let reading = tokio::spawn(async move {
        let streams = streams;
        let published = streams.read_to_end(id).await;
        (streams, published)
    });
    tokio::task::yield_now().await;
    writer.finish(Terminal::Done);
    let (streams, published) = reading.await.unwrap();
    assert_eq!(published.bytes(), b"event: one\n\nevent: two\n\n");
    assert_eq!(published.terminal, Some(Terminal::Done));
    assert!(
        stalled.try_next().is_ready(),
        "an unread reader still sees every record"
    );
    drop(stalled);
    drop(writer);
    drop(streams);
}
