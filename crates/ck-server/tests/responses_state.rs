//! The local Responses state store (D2-PLAN §2.1–2.5): a turn continues by
//! `previous_response_id` across upstreams, under the exchange's own fixed response id, and is
//! found as soon as its client saw it complete.

#[path = "stream_fixture/mod.rs"]
mod stream_fixture;

use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::http::{HeaderMap, StatusCode};
use bytes::Bytes;
use ck_core::credential::Credential;
use ck_core::exec::ExecError;
use ck_core::format::Format;
use ck_exec::pipeline::Started;
use ck_exec::types::{Attempt, Body, Counted, Execute, Request, Streams, Upstream};
use ck_server::http::egress::{Framing, Output, Protocol};
use ck_server::inference::dispatch::{self, Arms, Call, Codec, Served, Trace};
use ck_server::inference::responses_state::{self, Refused};
use ck_server::inference::streams::{Exchange, Owner};
use ck_stream::Writer;
use ck_translate::anthropic::request::AnthropicCapability;
use ck_translate::codec::chat::request::{ChatCapability, Egress as ChatEgress, read_request};
use ck_translate::codec::event::{Direction, LocalId};
use ck_translate::codec::{
    self, BodyMode, ClientCapability, Operation, OwnedRequest, RequestEgress, RequestFence, RequestIngress,
    RequestView, ValidatedRequest,
};
use futures_util::{StreamExt, stream};
use serde_json::{Value, json};
use stream_fixture::*;
use tracing::instrument::WithSubscriber;

/// Long enough for a published record to reach a waiting delivery.
const SETTLE: Duration = Duration::from_millis(200);
const MODEL: &str = "claude-sonnet-5";

/// What a credential's upstream does with its next attempt.
#[derive(Clone, Copy)]
enum Script {
    /// A Chat answer: `text` and a stop.
    Answers(&'static str),
    /// An Anthropic answer carrying signed thinking, then `text`.
    Signed(&'static str),
    /// The destination refuses the request; the candidate is skipped.
    Skips,
    /// Content, then a failure once the test releases it.
    FailsAfterContent,
    /// Content, then the rest of the answer once the test releases it.
    Held,
}

/// The contract arm scripted per credential; it records each attempt's upstream request.
struct Fake {
    scripts: Mutex<Vec<(&'static str, Script)>>,
    /// The next attempt of any credential is refused, so a turn fails over.
    refuse_next: AtomicBool,
    /// `(credential, upstream Chat body)` for every started attempt.
    sent: Mutex<Vec<(String, Value)>>,
    release: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

impl Fake {
    fn new() -> Self {
        Self {
            scripts: Mutex::new(vec![("a", Script::Answers("hello")), ("b", Script::Answers("hello"))]),
            refuse_next: AtomicBool::new(false),
            sent: Mutex::default(),
            release: Mutex::default(),
        }
    }

    fn set(&self, credential: &'static str, script: Script) {
        let mut scripts = self.scripts.lock().unwrap();
        scripts.iter_mut().find(|(id, _)| *id == credential).unwrap().1 = script;
    }

    /// A release for the next `Held` or `FailsAfterContent` attempt.
    fn hold(&self) -> tokio::sync::oneshot::Sender<()> {
        let (send, receive) = tokio::sync::oneshot::channel();
        *self.release.lock().unwrap() = Some(receive);
        send
    }

    fn script(&self, attempt: &Attempt<'_>) -> Script {
        let id = attempt.credential.id.rsplit('/').next().unwrap();
        let scripts = self.scripts.lock().unwrap();
        scripts.iter().find(|(credential, _)| *credential == id).unwrap().1
    }

    fn sent(&self) -> Vec<(String, Value)> {
        std::mem::take(&mut *self.sent.lock().unwrap())
    }
}

impl Arms for Fake {
    fn destination(&self, _attempt: &Attempt<'_>) -> Option<&'static str> {
        Some("chat")
    }

    fn refence<'a>(&self, attempt: &Attempt<'_>, view: RequestView<'a>) -> codec::Result<ValidatedRequest<'a>> {
        match self.script(attempt) {
            Script::Skips => Err(codec::Error::Unsupported),
            Script::Signed(_) => RequestFence::default().end(view, &anthropic()),
            _ => RequestFence::default().end(view, &ChatCapability),
        }
    }

    async fn start(
        &self,
        attempt: &Attempt<'_>,
        request: &mut Request<'_>,
        upload: &mut Writer,
        response: Writer,
    ) -> Result<Started, ExecError> {
        let script = self.script(attempt);
        let credential = attempt.credential.id.rsplit('/').next().unwrap().to_owned();
        let body = match script {
            // Anthropic signed thinking has no Chat form: the Chat body shows the history's text.
            Script::Signed(_) => Value::Null,
            _ => {
                let lease = ChatEgress {
                    destination: "chat",
                    upstream_stream: true,
                }
                .encode(&request.validated(), upload)
                .unwrap();
                let mut out = Vec::new();
                while out.len() < lease.len() {
                    out.extend_from_slice(lease.chunk(out.len()).unwrap());
                }
                serde_json::from_slice(&out).unwrap()
            }
        };
        self.sent.lock().unwrap().push((credential, body));
        let refused = self.refuse_next.swap(false, Ordering::SeqCst);
        let release = &self.release;
        match script {
            Script::Signed(text) => {
                ck_exec::pipeline::start(&AnthropicUpstream { text, refused }, attempt, request, upload, response).await
            }
            _ => {
                let upstream = ChatUpstream {
                    script,
                    refused,
                    release,
                };
                ck_exec::pipeline::start(&upstream, attempt, request, upload, response).await
            }
        }
    }
}

fn anthropic() -> AnthropicCapability {
    AnthropicCapability {
        default_max_tokens: Some(1024),
        ..Default::default()
    }
}

fn refusal() -> ExecError {
    ExecError::local(503, ck_core::exec::FailureScope::Transport, "refused")
}

struct ChatUpstream<'f> {
    script: Script,
    refused: bool,
    release: &'f Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

impl Execute for ChatUpstream<'_> {
    type Capability = ChatCapability;
    type Ingress = ck_translate::codec::chat::response::Ingress;

    fn capability(&self, _attempt: &Attempt<'_>) -> ChatCapability {
        ChatCapability
    }

    async fn execute(
        &self,
        attempt: &Attempt<'_>,
        _request: &mut Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<Self::Ingress>, ExecError> {
        if self.refused {
            return Err(refusal());
        }
        let ingress = Self::Ingress::new(
            streams.response,
            attempt.store,
            attempt.identity,
            Operation::Generate,
            BodyMode::Streaming,
            1 << 20,
        )
        .unwrap();
        let answer = |text: &str| {
            [
                chunk(&json!({"role": "assistant", "content": text}).to_string(), "null"),
                chunk("{}", r#""stop""#),
                done(),
            ]
        };
        let body: Body = match self.script {
            Script::Answers(text) => stream::iter(answer(text).map(Ok)).boxed(),
            Script::Held => {
                let release = self.release.lock().unwrap().take().unwrap();
                let rest = [chunk("{}", r#""stop""#), done()];
                stream::iter([Ok(chunk(r#"{"role":"assistant","content":"held"}"#, "null"))])
                    .chain(stream::once(release).flat_map(move |_| stream::iter(rest.clone().map(Ok))))
                    .boxed()
            }
            Script::FailsAfterContent => {
                let release = self.release.lock().unwrap().take().unwrap();
                stream::iter([Ok(chunk(r#"{"role":"assistant","content":"hel"}"#, "null"))])
                    .chain(stream::once(async move {
                        let _ = release.await;
                        Err(ExecError::local(
                            502,
                            ck_core::exec::FailureScope::Transport,
                            "upstream",
                        ))
                    }))
                    .boxed()
            }
            Script::Skips | Script::Signed(_) => unreachable!("never answers in Chat"),
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
        _request: &mut Request<'_>,
        _streams: Streams<'_>,
    ) -> Result<Counted<Self::Ingress>, ExecError> {
        unreachable!("generation only")
    }

    async fn compact(
        &self,
        _attempt: &Attempt<'_>,
        _request: &mut Request<'_>,
        _streams: Streams<'_>,
    ) -> Result<Upstream<Self::Ingress>, ExecError> {
        unreachable!("generation only")
    }
}

/// An Anthropic upstream answering with signed thinking and `text`.
struct AnthropicUpstream {
    text: &'static str,
    refused: bool,
}

impl Execute for AnthropicUpstream {
    type Capability = AnthropicCapability;
    type Ingress = ck_translate::anthropic::response::Ingress;

    fn capability(&self, _attempt: &Attempt<'_>) -> AnthropicCapability {
        anthropic()
    }

    async fn execute(
        &self,
        attempt: &Attempt<'_>,
        _request: &mut Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<Self::Ingress>, ExecError> {
        if self.refused {
            return Err(refusal());
        }
        let ingress = Self::Ingress::new(
            streams.response,
            attempt.identity,
            Operation::Generate,
            BodyMode::Buffered,
            1 << 20,
        )
        .unwrap();
        let body = json!({
            "id": "msg_1", "type": "message", "role": "assistant", "model": "claude",
            "content": [
                {"type": "thinking", "thinking": "hmm", "signature": "SIG=="},
                {"type": "text", "text": self.text}
            ],
            "stop_reason": "end_turn", "usage": {"input_tokens": 1, "output_tokens": 2}
        });
        Ok(Upstream {
            status: 200,
            headers: HeaderMap::new(),
            body: stream::iter([Ok(Bytes::from(body.to_string()))]).boxed(),
            ingress,
        })
    }

    async fn count_tokens(
        &self,
        _attempt: &Attempt<'_>,
        _request: &mut Request<'_>,
        _streams: Streams<'_>,
    ) -> Result<Counted<Self::Ingress>, ExecError> {
        unreachable!("generation only")
    }

    async fn compact(
        &self,
        _attempt: &Attempt<'_>,
        _request: &mut Request<'_>,
        _streams: Streams<'_>,
    ) -> Result<Upstream<Self::Ingress>, ExecError> {
        unreachable!("generation only")
    }
}

fn credential(id: &str) -> Credential {
    let mut metadata = serde_json::Map::new();
    metadata.insert("type".into(), "claude".into());
    metadata.insert("access_token".into(), format!("fake-{id}").into());
    let path = Path::new("/mock");
    ck_server::testing::local(Credential::from_file(path, &path.join(id), metadata).unwrap())
}

/// A fixture with credentials `a` and `b`, whose store `configure` adjusts.
fn fixture(configure: impl FnOnce(&mut ck_stream::Config)) -> Fixture {
    Fixture::with_credentials_and(vec![credential("a"), credential("b")], "http://127.0.0.1:9", configure)
}

fn scoped(key: &str, scope: Option<&str>) -> Owner {
    Owner {
        scope: scope.map(str::to_owned),
        ..owner(key)
    }
}

/// One client request: who sends it, its Responses body and its framing.
struct Ask {
    owner: Owner,
    body: Value,
    framing: Framing,
}

impl Ask {
    fn new(input: &str) -> Self {
        Self {
            owner: owner(KEY),
            body: json!({"model": MODEL, "input": input, "stream": true}),
            framing: Framing::Sse,
        }
    }

    fn after(mut self, id: &str) -> Self {
        self.body["previous_response_id"] = json!(id);
        self
    }

    fn buffered(mut self) -> Self {
        self.framing = Framing::Json;
        self.body["stream"] = json!(false);
        self
    }
}

/// What a request came to.
enum Answer {
    /// Committed: the response, whose body the caller reads.
    Committed(axum::response::Response),
    /// Answered before dispatch.
    Refused(Refused),
    /// The dispatch failed with this status.
    Failed(u16),
}

/// Runs `ask` through request ingress, `responses_state::begin` and the contract arm.
async fn send(fixture: &Fixture, fake: &Fake, ask: Ask) -> Answer {
    let output = Output {
        protocol: Protocol::Responses,
        operation: Operation::Generate,
        framing: ask.framing,
    };
    let body = serde_json::to_vec(&ask.body).unwrap();
    let streams = fixture.rt.streams();
    let mut exchange = streams
        .with_store(|store| {
            let mut exchange = Exchange::new(ask.owner.clone(), output, store).unwrap();
            let request = &mut exchange.request;
            let mut client = ck_translate::responses::request::Ingress::new(
                request,
                store,
                identity(Direction::Request),
                Operation::Generate,
                1 << 20,
            )
            .unwrap();
            client.advance(request, &body).unwrap();
            client.finish(request, &ClientCapability).unwrap();
            exchange
        })
        .unwrap();
    let mut records = streams
        .with_store(|store| store.reader(exchange.request.id(), 0))
        .unwrap()
        .unwrap();
    let (document, media) = read_request(&mut exchange.request, &mut records).unwrap();
    let request_id = LocalId::new("request").unwrap();
    let view = RequestView {
        document: &document,
        media: &media,
        operation: Operation::Generate,
    };
    let expanded = match responses_state::begin(streams, &mut exchange, view, request_id) {
        Ok(expanded) => expanded,
        Err(refused) => return Answer::Refused(refused),
    };
    let (document, media) = expanded.unwrap_or((document, media));
    let codec = Codec {
        exchange,
        request: OwnedRequest {
            document,
            media,
            operation: Operation::Generate,
        },
        request_id,
    };
    let mut call = Call {
        entry: Format::OpenAIResponse,
        response: Format::OpenAIResponse,
        operation: ck_core::exec::Operation::Generate,
        model: MODEL.into(),
        body: Bytes::from(body),
        stream: ask.framing == Framing::Sse,
        alt: None,
        headers: HeaderMap::new(),
        caller: ask.owner.caller.clone(),
        forced_provider: None,
        selection_model: None,
        execution_session: None,
        request_path: "/v1/responses".into(),
        peer: None,
        turn: None,
        media: None,
    };
    call.headers.clear();
    let trace = Trace::with_request_id(Some("trace".into()));
    let served = tokio::time::timeout(
        Duration::from_secs(5),
        dispatch::run_exchange(&fixture.rt, fake, call, &trace, codec, None),
    )
    .await
    .expect("the request is decided");
    match served {
        Ok(Served::Committed(response)) => Answer::Committed(response),
        Ok(Served::Old(_)) => panic!("the old arm answered"),
        Err(error) => Answer::Failed(error.failure.status()),
    }
}

/// The committed response of `answer`.
fn committed(answer: Answer) -> axum::response::Response {
    match answer {
        Answer::Committed(response) => response,
        Answer::Refused(refused) => panic!("refused before dispatch: {refused:?}"),
        Answer::Failed(status) => panic!("failed with {status}"),
    }
}

/// The whole body of a committed response, within five seconds.
async fn read(response: axum::response::Response) -> String {
    let (text, ended) = tokio::time::timeout(Duration::from_secs(5), body(response))
        .await
        .expect("the body ends");
    assert!(ended, "the body ends cleanly: {text}");
    text
}

/// The response id a Responses body shows, and whether every id it shows is that one.
fn response_id(text: &str) -> String {
    let mut ids = Vec::new();
    let documents: Vec<Value> = if text.starts_with('{') {
        vec![serde_json::from_str(text).unwrap()]
    } else {
        text.lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .map(|data| serde_json::from_str(data).unwrap())
            .collect()
    };
    for document in documents {
        let response = document.get("response").unwrap_or(&document);
        if let Some(id) = response.get("id").and_then(Value::as_str)
            && response.get("object").and_then(Value::as_str) == Some("response")
        {
            ids.push(id.to_owned());
        }
    }
    assert!(!ids.is_empty(), "the body shows a response id: {text}");
    assert!(ids.iter().all(|id| *id == ids[0]), "one id throughout: {ids:?}");
    ids.remove(0)
}

/// A turn that completed: its response id.
async fn turn(fixture: &Fixture, fake: &Fake, ask: Ask) -> String {
    response_id(&read(committed(send(fixture, fake, ask).await)).await)
}

/// The Chat messages an upstream received, as role and text.
fn messages(body: &Value) -> Vec<(String, String)> {
    body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| {
            let text = match &m["content"] {
                Value::String(text) => text.clone(),
                Value::Array(parts) => parts.iter().filter_map(|p| p["text"].as_str()).collect(),
                other => other.to_string(),
            };
            (m["role"].as_str().unwrap().to_owned(), text)
        })
        .collect()
}

fn said(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs.iter().map(|(r, t)| ((*r).to_owned(), (*t).to_owned())).collect()
}

#[tokio::test]
async fn a_continuation_fails_over_with_the_stored_turn_under_one_id() {
    let fixture = fixture(|_| {});
    let fake = Fake::new();
    let first = turn(&fixture, &fake, Ask::new("hi")).await;
    assert!(first.starts_with("resp_") && first.len() == 37, "{first}");
    fake.sent();
    fake.refuse_next.store(true, Ordering::SeqCst);
    let response = committed(send(&fixture, &fake, Ask::new("more").after(&first)).await);
    let stream_id = response.headers()["x-ck-stream-id"].to_str().unwrap().to_owned();
    let text = read(response).await;
    let second = response_id(&text);
    assert_ne!(second, first);
    let sent = fake.sent();
    assert_eq!(sent.len(), 2, "the turn failed over once: {sent:?}");
    assert_ne!(sent[0].0, sent[1].0, "to another credential");
    let history = said(&[("user", "hi"), ("assistant", "hello"), ("user", "more")]);
    for (credential, body) in &sent {
        assert_eq!(messages(body), history, "{credential} got the stored turn");
        assert!(body.get("previous_response_id").is_none(), "{body}");
    }
    // A resume shows the same id.
    let resumed = get(&fixture.app(), KEY, "?after=v1.e.0", &[("x-ck-stream-id", &stream_id)]).await;
    assert_eq!(resumed.status(), StatusCode::OK);
    assert_eq!(response_id(&read(resumed).await), second);
    // A third turn continues the second, which holds the first.
    turn(&fixture, &fake, Ask::new("again").after(&second)).await;
    let sent = fake.sent();
    assert_eq!(messages(&sent[0].1).len(), 5, "{:?}", sent[0].1);
}

/// A completed turn is found at once, whether the usage reader is still busy or the client read
/// the completion through a resume while the index publication was held.
#[tokio::test]
async fn completed_turns_keep_request_fields_and_are_available_for_continuation() {
    for (fields, expected) in [
        (
            json!({}),
            json!({"instructions": null, "metadata": null, "temperature": 1.0, "top_p": 1.0}),
        ),
        (
            json!({"instructions": "Be brief", "metadata": {"tag": "one"}, "temperature": 0.25, "top_p": 0.8}),
            json!({"instructions": "Be brief", "metadata": {"tag": "one"}, "temperature": 0.25, "top_p": 0.8}),
        ),
    ] {
        for mut ask in [Ask::new("hi"), Ask::new("hi").buffered()] {
            ask.body
                .as_object_mut()
                .unwrap()
                .extend(fields.as_object().unwrap().clone());
            let fixture = fixture(|_| {});
            let fake = Fake::new();
            fixture.rt.streams().holds.usage.close();
            let framing = ask.framing;
            let text = read(committed(send(&fixture, &fake, ask).await)).await;
            let responses: Vec<Value> = if framing == Framing::Sse {
                text.lines()
                    .filter_map(|line| line.strip_prefix("data: "))
                    .map(|data| serde_json::from_str::<Value>(data).unwrap())
                    .filter_map(|mut event| event.as_object_mut().unwrap().remove("response"))
                    .collect()
            } else {
                vec![serde_json::from_str(&text).unwrap()]
            };
            assert!(!responses.is_empty(), "{text}");
            for response in responses {
                for (name, value) in expected.as_object().unwrap() {
                    assert_eq!(response.get(name), Some(value), "{framing:?} {name}: {text}");
                }
            }
            let first = response_id(&text);
            turn(&fixture, &fake, Ask::new("more").after(&first)).await;
            fixture.rt.streams().holds.usage.open();
        }
    }

    let fixture = fixture(|_| {});
    let fake = Fake::new();
    fixture.rt.streams().holds.publication.close();
    let response = committed(send(&fixture, &fake, Ask::new("hi")).await);
    let stream_id = response.headers()["x-ck-stream-id"].to_str().unwrap().to_owned();
    let live = tokio::spawn(read(response));
    let resumed = get(&fixture.app(), KEY, "?after=v1.e.0", &[("x-ck-stream-id", &stream_id)]).await;
    let resumed = tokio::spawn(read(resumed));
    tokio::time::sleep(SETTLE).await;
    assert!(
        !live.is_finished() && !resumed.is_finished(),
        "completion waits for the index"
    );
    fixture.rt.streams().holds.publication.open();
    let first = response_id(&resumed.await.unwrap());
    turn(&fixture, &fake, Ask::new("more").after(&first)).await;
    assert_eq!(response_id(&live.await.unwrap()), first);
}

/// A buffered body is sent only once its turn is published.
#[tokio::test]
async fn a_buffered_body_waits_for_its_publication() {
    let fixture = fixture(|_| {});
    let fake = Fake::new();
    fixture.rt.streams().holds.publication.close();
    let asked = send(&fixture, &fake, Ask::new("hi").buffered());
    tokio::pin!(asked);
    assert!(
        tokio::time::timeout(SETTLE, &mut asked).await.is_err(),
        "nothing is sent before publication"
    );
    fixture.rt.streams().holds.publication.open();
    let first = response_id(&read(committed(asked.await)).await);
    turn(&fixture, &fake, Ask::new("more").after(&first)).await;
}

/// Reads `response` until it showed its response id; the rest of the body stays unread.
async fn shown_id(response: axum::response::Response) -> (String, axum::body::BodyDataStream) {
    let mut stream = response.into_body().into_data_stream();
    let mut text = String::new();
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("the id arrives")
            .expect("the body goes on")
            .unwrap();
        text.push_str(std::str::from_utf8(&frame).unwrap());
        if let Some(end) = text.find("\n\n") {
            return (response_id(&text[..end + 2]), stream);
        }
    }
}

/// Every continuation of something this caller cannot continue gets the same answer.
#[tokio::test]
async fn every_miss_is_the_same_not_found() {
    #[derive(Clone, Copy, Debug)]
    enum Miss {
        Unknown,
        AnotherCaller,
        AnotherScope,
        NotStored,
        Running,
        Failed,
        Cancelled,
    }
    let mut answers = Vec::new();
    for miss in [
        Miss::Unknown,
        Miss::AnotherCaller,
        Miss::AnotherScope,
        Miss::NotStored,
        Miss::Running,
        Miss::Failed,
        Miss::Cancelled,
    ] {
        let fixture = fixture(|_| {});
        let fake = Fake::new();
        let mut ask = Ask::new("hi");
        let script = match miss {
            Miss::Running | Miss::Cancelled => Script::Held,
            Miss::Failed => Script::FailsAfterContent,
            _ => Script::Answers("hello"),
        };
        fake.set("a", script);
        fake.set("b", script);
        let release = fake.hold();
        if let Miss::NotStored = miss {
            ask.body["store"] = json!(false);
        }
        let response = committed(send(&fixture, &fake, ask).await);
        let stream_id = response.headers()["x-ck-stream-id"].clone();
        let (mut first, mut rest) = shown_id(response).await;
        match miss {
            Miss::Unknown => first = "resp_0123456789abcdef0123456789abcdef".into(),
            Miss::Failed => {
                let _ = release.send(());
                while rest.next().await.is_some_and(|frame| frame.is_ok()) {}
            }
            Miss::Cancelled => {
                let id = ck_server::inference::streams::StreamId::parse(stream_id.as_bytes()).unwrap();
                assert!(fixture.rt.streams().cancel(&id, &owner(KEY)));
                while rest.next().await.is_some_and(|frame| frame.is_ok()) {}
            }
            _ => {}
        }
        let mut ask = Ask::new("more").after(&first);
        ask.owner = match miss {
            Miss::AnotherCaller => owner(OTHER),
            Miss::AnotherScope => scoped(KEY, Some("sess_1")),
            _ => owner(KEY),
        };
        match send(&fixture, &fake, ask).await {
            Answer::Refused(refused @ Refused::NotFound) => {
                answers.push((miss, status_body(refused.response()).await));
            }
            Answer::Refused(refused) => panic!("{miss:?}: {refused:?}"),
            Answer::Committed(_) => panic!("{miss:?}: continued"),
            Answer::Failed(status) => panic!("{miss:?}: failed with {status}"),
        }
        drop(rest);
    }
    let (_, (status, text)) = &answers[0];
    assert_eq!(*status, StatusCode::BAD_REQUEST);
    let error: Value = serde_json::from_str(text).unwrap();
    assert_eq!(error["error"]["code"], "previous_response_not_found");
    assert_eq!(error["error"]["type"], "invalid_request_error");
    assert_eq!(error["error"]["param"], "previous_response_id");
    for (miss, answer) in &answers {
        assert_eq!(answer, &answers[0].1, "{miss:?}");
    }
}

/// Another credential continues the turn; reasoning the first account signed is withheld from
/// it and logged, and the conversation's text goes on (Settled 73).
#[tokio::test]
async fn another_credential_continues_without_foreign_reasoning() {
    let fixture = fixture(|_| {});
    let fake = Fake::new();
    fake.set("a", Script::Signed("signed"));
    fake.set("b", Script::Skips);
    let first = turn(&fixture, &fake, Ask::new("hi")).await;
    assert_eq!(fake.sent().len(), 1);
    fake.set("a", Script::Skips);
    fake.set("b", Script::Answers("again"));
    let log = Capture::default();
    let writer = log.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .with_writer(move || writer.clone())
        .finish();
    let text = read(committed(
        send(&fixture, &fake, Ask::new("more").after(&first))
            .with_subscriber(subscriber)
            .await,
    ))
    .await;
    assert!(text.contains("again"), "{text}");
    let sent = fake.sent();
    let (_, body) = sent.iter().find(|(credential, _)| credential == "b").unwrap();
    assert_eq!(
        messages(body),
        said(&[("user", "hi"), ("assistant", "signed"), ("user", "more")])
    );
    let log = String::from_utf8(log.0.lock().unwrap().clone()).unwrap();
    assert_eq!(
        log.matches("reasoning from another account withheld").count(),
        1,
        "{log}"
    );
}

#[derive(Clone, Default)]
struct Capture(std::sync::Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A turn left unread for an hour expires with its streams and is no longer found.
#[tokio::test]
async fn an_idle_turn_expires_after_an_hour() {
    let fixture = fixture(|_| {});
    let fake = Fake::new();
    let first = turn(&fixture, &fake, Ask::new("hi")).await;
    fixture.advance(Duration::from_secs(59 * 60));
    let second = turn(&fixture, &fake, Ask::new("more").after(&first)).await;
    fixture.advance(Duration::from_secs(61 * 60));
    let answer = send(&fixture, &fake, Ask::new("again").after(&second)).await;
    assert!(matches!(answer, Answer::Refused(Refused::NotFound)));
}

/// With four stream headers, a chain of ten continuations evicts the oldest turns and their
/// index entries, and every turn is still indexed.
#[tokio::test]
async fn eviction_keeps_room_in_the_index() {
    let fixture = fixture(|config| config.header_slots = 4);
    let fake = Fake::new();
    let mut previous = turn(&fixture, &fake, Ask::new("0")).await;
    for i in 1..10 {
        let input = i.to_string();
        previous = turn(&fixture, &fake, Ask::new(&input).after(&previous)).await;
        let sent = fake.sent();
        let (_, body) = sent.last().unwrap();
        assert_eq!(messages(body).len(), 2 * i + 1, "turn {i} holds every earlier turn");
    }
}

/// A history larger than the exchange's metadata quota is refused with 503 before dispatch;
/// nothing falls back to the heap.
#[tokio::test]
async fn a_history_over_the_metadata_quota_is_refused() {
    let fixture = fixture(|config| {
        config.max_metadata_bytes = 640 * 1024;
        config.pool_bytes = Some(1024 * ck_stream::FRAME_BYTES);
    });
    let fake = Fake::new();
    let input: Vec<Value> = (0..150)
        .map(|i| json!({"role": "user", "content": format!("m{i}")}))
        .collect();
    let mut ask = Ask::new("");
    let mut previous = String::new();
    for round in 0..8 {
        ask.body["input"] = json!(input);
        if !previous.is_empty() {
            ask = ask.after(&previous);
        }
        match send(&fixture, &fake, ask).await {
            Answer::Committed(response) => previous = response_id(&read(response).await),
            Answer::Refused(Refused::Storage) => {
                assert!(round > 0, "the first turn fits");
                let (status, _) = status_body(Refused::Storage.response()).await;
                assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
                return;
            }
            Answer::Refused(refused) => panic!("round {round}: {refused:?}"),
            Answer::Failed(status) => panic!("round {round}: failed with {status}"),
        }
        ask = Ask::new("");
    }
    panic!("the history never outgrew the quota");
}

/// `conversation` is refused before dispatch (Q-G).
#[tokio::test]
async fn a_conversation_is_refused() {
    let fixture = fixture(|_| {});
    let fake = Fake::new();
    for conversation in [json!("conv_1"), json!({"id": "conv_1"})] {
        let mut ask = Ask::new("hi");
        ask.body["conversation"] = conversation;
        let Answer::Refused(refused @ Refused::Conversation) = send(&fixture, &fake, ask).await else {
            panic!("dispatched")
        };
        let (status, text) = status_body(refused.response()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(text.contains("\"param\":\"conversation\""), "{text}");
    }
    assert!(fake.sent().is_empty());
}
