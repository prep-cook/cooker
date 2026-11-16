//! A loopback Codex backend for Responses WebSocket turns: it answers every upgrade, records
//! the handshakes and frames it receives, and answers each frame with a scripted act.

use std::collections::VecDeque;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{FromRequestParts, State};
use axum::response::Response;
use ck_core::credential::Credential;
use ck_core::exec::ExecError;
use ck_exec::codex::{CodexExecutor, TurnControl};
use ck_exec::codex_exec::CodexIngress;
use ck_exec::session::Turns;
use ck_exec::types::{Execute, Upstream};
use ck_stream::{Terminal, Writer};
use ck_translate::codec::event::Direction;
use ck_translate::codec::{Operation, ResponseIngress};
use ck_translate::responses;
use futures_util::StreamExt;
use serde_json::{Value, json};
use tokio::sync::watch;

use crate::common::*;
use crate::support::{Exchange, Published, Streams, fence, identity};

pub(crate) const SESSION: &str = "conn-1";
pub(crate) const WAIT: Duration = Duration::from_secs(5);

/// What the loopback sends after one received text frame, then how the connection goes on.
#[derive(Clone, Default)]
pub(crate) struct Act {
    send: Vec<String>,
    end: End,
}

#[derive(Clone, Copy, Default)]
pub(crate) enum End {
    #[default]
    Open,
    /// A close frame with this code.
    Close(u16),
    /// The connection ends without a close frame.
    Drop,
    Binary,
}

pub(crate) fn act(send: Vec<String>) -> Act {
    Act { send, end: End::Open }
}

impl Act {
    pub(crate) fn then(mut self, end: End) -> Self {
        self.end = end;
        self
    }
}

#[derive(Default)]
struct Script {
    /// One act per received text frame, across connections.
    acts: VecDeque<Act>,
    frames: Vec<String>,
    upgrades: Vec<http::HeaderMap>,
    /// Headers the loopback adds to its 101 answers.
    answer: Vec<(&'static str, &'static str)>,
}

#[derive(Clone)]
struct Shared {
    script: Arc<Mutex<Script>>,
    /// Frames received, and connections ended, so far.
    received: watch::Sender<usize>,
    ended: watch::Sender<usize>,
}

/// A loopback Codex backend answering upgrades on any path.
pub(crate) struct CodexSocket {
    shared: Shared,
    url: String,
    task: tokio::task::JoinHandle<()>,
}

impl CodexSocket {
    pub(crate) async fn start(acts: impl IntoIterator<Item = Act>) -> Self {
        let shared = Shared {
            script: Arc::new(Mutex::new(Script {
                acts: acts.into_iter().collect(),
                ..Script::default()
            })),
            received: watch::channel(0).0,
            ended: watch::channel(0).0,
        };
        let app = axum::Router::new().fallback(upgrade).with_state(shared.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self { shared, url, task }
    }

    pub(crate) fn api_key(&self) -> Credential {
        let mut credential = crate::support::credential("codex", &self.url, "sk-up");
        credential.attributes.insert("websockets".into(), "true".into());
        credential
    }

    /// A Sign in with ChatGPT credential (7b0319fb).
    pub(crate) fn siwc(&self) -> Credential {
        let mut credential = self.api_key();
        credential.metadata.insert("use_siwc".into(), true.into());
        credential
            .metadata
            .insert("siwc".into(), json!({"access_token": "siwc-at"}));
        credential
    }

    pub(crate) fn oauth(&self) -> Credential {
        let metadata = json!({"type": "codex", "access_token": "oauth-token", "account_id": "acct-1"});
        let path = Path::new("/fake/codex-oauth.json");
        let mut credential =
            Credential::from_file(Path::new("/fake"), path, metadata.as_object().unwrap().clone()).unwrap();
        for (name, value) in [("base_url", self.url.as_str()), ("websockets", "true")] {
            credential.attributes.insert(name.into(), value.into());
        }
        credential
    }

    /// Adds `headers` to the loopback's 101 answers.
    pub(crate) fn answer_upgrades(&self, headers: Vec<(&'static str, &'static str)>) {
        self.shared.script.lock().unwrap().answer = headers;
    }

    pub(crate) fn frames(&self) -> Vec<String> {
        self.shared.script.lock().unwrap().frames.clone()
    }

    pub(crate) fn sent(&self) -> Vec<Value> {
        self.frames().iter().map(|f| serde_json::from_str(f).unwrap()).collect()
    }

    pub(crate) fn upgrades(&self) -> Vec<http::HeaderMap> {
        self.shared.script.lock().unwrap().upgrades.clone()
    }

    pub(crate) async fn received(&self, count: usize) {
        let mut received = self.shared.received.subscribe();
        tokio::time::timeout(WAIT, received.wait_for(|n| *n >= count))
            .await
            .expect("the upstream receives the frame")
            .unwrap();
    }

    pub(crate) async fn ended(&self, count: usize) {
        let mut ended = self.shared.ended.subscribe();
        tokio::time::timeout(WAIT, ended.wait_for(|n| *n >= count))
            .await
            .expect("the upstream connection ends")
            .unwrap();
    }
}

impl Drop for CodexSocket {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn upgrade(State(shared): State<Shared>, request: axum::extract::Request) -> Response {
    let (mut parts, _) = request.into_parts();
    let answer = {
        let mut script = shared.script.lock().unwrap();
        script.upgrades.push(parts.headers.clone());
        script.answer.clone()
    };
    let upgrade = WebSocketUpgrade::from_request_parts(&mut parts, &()).await.unwrap();
    let mut response = upgrade.on_upgrade(move |socket| async move {
        serve(socket, &shared).await;
        shared.ended.send_modify(|n| *n += 1);
    });
    for (name, value) in answer {
        response
            .headers_mut()
            .insert(name, http::HeaderValue::from_static(value));
    }
    response
}

async fn serve(mut socket: WebSocket, shared: &Shared) {
    while let Some(Ok(message)) = socket.recv().await {
        let Message::Text(text) = message else { continue };
        let act = {
            let mut script = shared.script.lock().unwrap();
            script.frames.push(text.as_str().to_owned());
            script.acts.pop_front().unwrap_or_default()
        };
        shared.received.send_modify(|n| *n += 1);
        for frame in act.send {
            if socket.send(Message::text(frame)).await.is_err() {
                return;
            }
        }
        match act.end {
            End::Open => {}
            End::Drop => return,
            End::Binary => {
                let _ = socket.send(Message::Binary(vec![1u8, 2, 3].into())).await;
            }
            End::Close(code) => {
                let reason = "bye".into();
                let _ = socket.send(Message::Close(Some(CloseFrame { code, reason }))).await;
                let _ = socket.recv().await;
                return;
            }
        }
    }
}

/// Codex's frames of one answer "hello" for response `id`, whose usage reports `input`
/// input tokens, completion included.
pub(crate) fn answer(id: &str, input: u64) -> Vec<String> {
    let usage = format!(
        r#""usage":{{"input_tokens":{input},"output_tokens":2,"total_tokens":{}}}"#,
        input + 2
    );
    sse()
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|data| {
            data.replace(r#""id":"resp_1""#, &format!(r#""id":"{id}""#)).replace(
                r#""usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}"#,
                &usage,
            )
        })
        .collect()
}

/// Codex's `response.created` for response `id`.
pub(crate) fn created(id: &str) -> String {
    answer(id, 0).swap_remove(0)
}

pub(crate) fn user(text: &str) -> Value {
    json!({"type": "message", "role": "user", "content": [{"type": "input_text", "text": text}]})
}

pub(crate) fn request(input: Value) -> String {
    json!({"model": "client-model", "instructions": "be brief", "input": input}).to_string()
}

/// One turn of `body` fenced for `exchange` on the connection's session: the turn, or its
/// failure, with its response stream and the request stream it depends on.
pub(crate) async fn start(
    streams: &Streams,
    exchange: &Exchange,
    executor: &CodexExecutor,
    body: &str,
) -> (Result<(Upstream<CodexIngress>, TurnControl), ExecError>, Writer, Writer) {
    start_on(Some(SESSION), streams, exchange, executor, body).await
}

/// [`start`] on the connection `session`; `None` is a request without one.
pub(crate) async fn start_on(
    session: Option<&str>,
    streams: &Streams,
    exchange: &Exchange,
    executor: &CodexExecutor,
    body: &str,
) -> (Result<(Upstream<CodexIngress>, TurnControl), ExecError>, Writer, Writer) {
    let mut request = streams.request();
    let mut response = streams.response(&request);
    let (store, id) = (&streams.store, identity(Direction::Request));
    let mut ingress = responses::request::Ingress::new(&mut request, store, id, Operation::Generate, LIMIT).unwrap();
    let attempt = exchange.attempt(&streams.store);
    let capability = executor.capability(&attempt);
    let validated = fence(&mut ingress, &mut request, body.as_bytes(), 64, &capability).expect("Codex accepts it");
    let mut handle = exchange.request(validated);
    handle.client.execution_session = session;
    let streams = ck_exec::types::Streams {
        request: &mut request,
        response: &mut response,
    };
    let turn = executor.turn(&attempt, &handle, streams).await;
    (turn.map(|t| (t.upstream, t.cancel)), response, request)
}

/// The attempt's producer in short: the body into the ingress to its end. The outcome is the
/// body's failure, if any; the records are the response stream's.
pub(crate) async fn produce(
    streams: &Streams,
    upstream: Upstream<CodexIngress>,
    mut writer: Writer,
) -> (Result<(), ExecError>, Published) {
    let id = writer.id();
    let Upstream {
        mut body, mut ingress, ..
    } = upstream;
    let outcome = loop {
        match tokio::time::timeout(WAIT, body.next()).await.expect("the body moves") {
            Some(Ok(chunk)) => ingress
                .advance(&mut writer, &chunk)
                .expect("the ingress accepts the frame"),
            Some(Err(error)) => {
                ingress.fail(&mut writer, error.status, "upstream").unwrap();
                writer.finish(Terminal::UpstreamError);
                break Err(error);
            }
            None => {
                ingress.finish(&mut writer).expect("the response completes");
                writer.finish(Terminal::Done);
                break Ok(());
            }
        }
    };
    (outcome, streams.read_to_end(id).await)
}

pub(crate) fn text(published: &Published) -> String {
    records(published).concat()
}
