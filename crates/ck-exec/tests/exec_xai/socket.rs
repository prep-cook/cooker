//! A Responses WebSocket turn on xAI's upstream socket (`session::Turns`) against a loopback
//! xAI: the one `response.create` each turn sends, the upstream frames reaching the ingress
//! as received, socket loss as the attempt's failure with no second send, cancel, warmup and
//! `compaction_trigger`.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{FromRequestParts, State};
use axum::response::{IntoResponse, Response};
use ck_core::exec::{CaptureEvent, CaptureObserver, CaptureSink, UsageSink};
use ck_exec::pipeline;
use ck_exec::session::{CancelTurn, Turns};
use ck_exec::xai::XaiExecutor;
use futures_util::StreamExt;
use tokio::sync::watch;

use super::*;

const SESSION: &str = "conn-1";

/// What the loopback does after one received text frame.
#[derive(Clone, Default)]
struct Act {
    send: Vec<String>,
    end: End,
}

#[derive(Clone, Copy, Default, Debug)]
enum End {
    /// Keeps the socket for the next frame.
    #[default]
    Open,
    /// Sends a close frame with this code.
    Close(u16),
    /// Ends the connection without a close frame.
    Drop,
    /// Sends a binary message.
    Binary,
}

impl Act {
    fn send(frames: Vec<String>) -> Self {
        Self {
            send: frames,
            end: End::Open,
        }
    }

    fn then(mut self, end: End) -> Self {
        self.end = end;
        self
    }
}

#[derive(Default)]
struct Script {
    /// One act per received text frame, across connections.
    acts: VecDeque<Act>,
    frames: Vec<Value>,
    upgrades: Vec<http::HeaderMap>,
    plain_requests: usize,
}

#[derive(Clone)]
struct Shared {
    script: Arc<Mutex<Script>>,
    /// Connections that ended, counted.
    ended: watch::Sender<usize>,
}

/// A loopback xAI answering upgrades on any path.
struct XaiSocket {
    shared: Shared,
    addr: String,
    task: tokio::task::JoinHandle<()>,
}

impl XaiSocket {
    async fn start(acts: impl IntoIterator<Item = Act>) -> Self {
        let shared = Shared {
            script: Arc::new(Mutex::new(Script {
                acts: acts.into_iter().collect(),
                ..Script::default()
            })),
            ended: watch::channel(0).0,
        };
        let app = axum::Router::new().fallback(upgrade).with_state(shared.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self { shared, addr, task }
    }

    fn credential(&self) -> Credential {
        let mut credential = crate::support::credential("xai", &format!("http://{}/v1", self.addr), TOKEN);
        credential.attributes.insert("websockets".into(), "true".into());
        credential
    }

    fn frames(&self) -> Vec<Value> {
        self.shared.script.lock().unwrap().frames.clone()
    }

    fn dials(&self) -> usize {
        self.shared.script.lock().unwrap().upgrades.len()
    }

    /// Waits until `count` connections ended on the upstream side.
    async fn ended(&self, count: usize) {
        let mut ended = self.shared.ended.subscribe();
        tokio::time::timeout(Duration::from_secs(5), ended.wait_for(|n| *n >= count))
            .await
            .expect("the upstream connection ends")
            .unwrap();
    }
}

impl Drop for XaiSocket {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn upgrade(State(shared): State<Shared>, request: axum::extract::Request) -> Response {
    let (mut parts, _) = request.into_parts();
    if !parts.headers.contains_key("upgrade") {
        shared.script.lock().unwrap().plain_requests += 1;
        return http::StatusCode::NOT_FOUND.into_response();
    }
    shared.script.lock().unwrap().upgrades.push(parts.headers.clone());
    let upgrade = WebSocketUpgrade::from_request_parts(&mut parts, &()).await.unwrap();
    upgrade.on_upgrade(move |socket| async move {
        serve(socket, &shared).await;
        shared.ended.send_modify(|n| *n += 1);
    })
}

async fn serve(mut socket: WebSocket, shared: &Shared) {
    while let Some(Ok(message)) = socket.recv().await {
        let Message::Text(text) = message else { continue };
        let act = {
            let mut script = shared.script.lock().unwrap();
            script.frames.push(serde_json::from_str(text.as_str()).unwrap());
            script.acts.pop_front().unwrap_or_default()
        };
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
                let close = CloseFrame {
                    code,
                    reason: "connection lifetime reached".into(),
                };
                let _ = socket.send(Message::Close(Some(close))).await;
                // The client's answer, or its end.
                let _ = socket.recv().await;
                return;
            }
        }
    }
}

/// Grok's frames for one answer "hello" with response id `id`, completion included.
fn answer(id: &str) -> Vec<String> {
    sse()
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|data| data.replace(r#""id":"resp_1""#, &format!(r#""id":"{id}""#)))
        .collect()
}

/// Grok's frames up to its `response.created`.
fn created() -> Vec<String> {
    answer("resp_1").into_iter().take(1).collect()
}

/// One turn's request stream and the attempt's response stream, until a turn takes it.
struct Fenced {
    writer: Writer,
    response: Option<Writer>,
}

/// Fences `body`, then runs `with` on the attempt, the request handle and the streams.
async fn fenced<T>(
    streams: &Streams,
    exchange: &Exchange,
    executor: &XaiExecutor,
    (body, session): (&str, Option<&str>),
    with: impl AsyncFnOnce(&ck_exec::types::Attempt<'_>, &mut ck_exec::types::Request<'_>, &mut Fenced) -> T,
) -> (T, Writer) {
    let mut writer = streams.request();
    let response = streams.response(&writer);
    let mut ingress = responses::request::Ingress::new(
        &mut writer,
        &streams.store,
        identity(Direction::Request),
        Operation::Generate,
        LIMIT,
    )
    .unwrap();
    for chunk in body.as_bytes().chunks(64) {
        ingress.advance(&mut writer, chunk).unwrap();
    }
    let attempt = exchange.attempt(&streams.store);
    let validated = pipeline::fence(executor, &attempt, &mut ingress, &mut writer).expect("the fence accepts");
    let validated = pipeline::refence(executor, &attempt, validated.view()).expect("the re-fence accepts");
    let mut request = exchange.request(validated);
    request.client.execution_session = session;
    let mut fenced = Fenced {
        writer,
        response: Some(response),
    };
    let out = with(&attempt, &mut request, &mut fenced).await;
    (out, fenced.writer)
}

type Turned = Result<(Started, <XaiExecutor as Turns>::Cancel), ExecError>;

/// Starts one turn of `body` through the pipeline on the client connection's session.
async fn start(streams: &Streams, exchange: &Exchange, executor: &XaiExecutor, body: &str) -> (Turned, Writer) {
    start_in(streams, exchange, executor, body, Some(SESSION)).await
}

/// [`start`] with the execution session `session`.
async fn start_in(
    streams: &Streams,
    exchange: &Exchange,
    executor: &XaiExecutor,
    body: &str,
    session: Option<&str>,
) -> (Turned, Writer) {
    fenced(
        streams,
        exchange,
        executor,
        (body, session),
        async |attempt, request, f| {
            let response = f.response.take().unwrap();
            pipeline::start_turn(executor, attempt, request, &mut f.writer, response).await
        },
    )
    .await
}

/// One turn of `body` to its end: the producer's outcome and the response stream.
async fn turn(
    streams: &Streams,
    exchange: &Exchange,
    executor: &XaiExecutor,
    body: &str,
) -> (Result<(), ExecError>, Published, Writer) {
    let (started, _request) = start(streams, exchange, executor, body).await;
    let (started, _cancel) = started.expect("the turn starts");
    tokio::time::timeout(Duration::from_secs(5), produce(streams, started))
        .await
        .expect("the turn ends")
}

fn request(input: Value) -> String {
    json!({"model": MODEL, "input": input}).to_string()
}

fn user(text: &str) -> Value {
    json!({"type":"message","role":"user","content":[{"type":"input_text","text":text}]})
}

/// One turn sends one `response.create` carrying the request as fenced, and the body hands
/// every upstream frame to the ingress unchanged, one SSE event each. The capture records
/// the frame sent, the handshake and every frame received.
#[tokio::test]
async fn a_turn_sends_one_create_and_passes_frames_unchanged() {
    let upstream = XaiSocket::start([Act::send(answer("resp_1"))]).await;
    let streams = Streams::new();
    let (captured, usage) = Captured::sink();
    let mut exchange = Exchange::new(upstream.credential(), MODEL, "/v1/responses");
    exchange.usage = usage;
    let executor = XaiExecutor::with_client(wreq::Client::new());
    let body = request(json!([user("hi")]));
    let want: Vec<String> = answer("resp_1").iter().map(|f| format!("data: {f}\n\n")).collect();
    // The producer stops reading at the completion; the body itself waits for more.
    let session = (body.as_str(), Some(SESSION));
    let (got, _request) = fenced(&streams, &exchange, &executor, session, async |attempt, request, f| {
        let streams = ck_exec::types::Streams {
            request: &mut f.writer,
            response: f.response.as_mut().unwrap(),
        };
        let turn = executor.turn(attempt, request, streams).await.expect("the turn starts");
        let chunks = turn.upstream.body.take(want.len()).collect::<Vec<_>>();
        tokio::time::timeout(Duration::from_secs(5), chunks)
            .await
            .expect("every frame arrives")
            .into_iter()
            .map(|chunk| String::from_utf8_lossy(&chunk.expect("a frame")).into_owned())
            .collect::<Vec<_>>()
    })
    .await;
    assert_eq!(got, want);

    let frames = upstream.frames();
    assert_eq!(frames.len(), 1);
    let sent = &frames[0];
    assert_eq!(sent["type"], "response.create", "{sent}");
    assert_eq!(sent["model"], MODEL, "{sent}");
    assert_eq!(sent["input"], json!([user("hi")]), "{sent}");
    for absent in ["stream", "stream_options", "background", "previous_response_id"] {
        assert!(sent.get(absent).is_none(), "{absent}: {sent}");
    }
    assert_eq!(sent["store"], false, "nothing is stored at xAI, as over HTTP: {sent}");
    let upgrade = &upstream.shared.script.lock().unwrap().upgrades[0];
    assert_eq!(upgrade["authorization"], format!("Bearer {TOKEN}"));
    assert_eq!(upgrade["user-agent"], "cooker", "Q13: API keys identify as cooker");
    let mut want = vec!["request WEBSOCKET".to_owned(), "handshake 101".to_owned()];
    want.extend(answer("resp_1").iter().map(|f| format!("response {f}")));
    assert_eq!(captured.lines(), want);
}

/// Turns on one socket keep no history between them: each sends its own request's expanded
/// input, a client `previous_response_id` never reaches xAI, and xAI's response ids reach
/// the canonical stream as xAI sent them, a repeated id included.
#[tokio::test]
async fn turns_send_their_own_history_and_keep_xai_ids() {
    let upstream = XaiSocket::start([Act::send(answer("resp_1")), Act::send(answer("resp_1"))]).await;
    let streams = Streams::new();
    let exchange = Exchange::new(upstream.credential(), MODEL, "/v1/responses");
    let executor = XaiExecutor::with_client(wreq::Client::new());
    let expanded = json!([user("first"), message("hello"), user("second")]);
    let bodies = [
        request(json!([user("first")])),
        json!({"model": MODEL, "previous_response_id": "resp_1", "input": expanded}).to_string(),
    ];
    for body in &bodies {
        let (outcome, published, _) = turn(&streams, &exchange, &executor, body).await;
        outcome.unwrap();
        assert_eq!(published.terminal, Some(Terminal::Done));
        let records = records(&published).concat();
        assert!(
            records.contains("resp_1") && !records.contains("resp_1-xai"),
            "{records}"
        );
    }
    let frames = upstream.frames();
    assert_eq!(upstream.dials(), 1, "both turns share the socket");
    assert_eq!(frames[0]["input"], json!([user("first")]));
    assert_eq!(frames[1]["input"].as_array().unwrap().len(), 3, "{}", frames[1]);
    assert_eq!(frames[1]["input"][2], user("second"));
    assert!(frames[1].get("previous_response_id").is_none(), "{}", frames[1]);
}

/// How a turn ends.
#[derive(Debug)]
enum Want {
    /// The response completes, with a usage record when it carries usage.
    Done { usage: bool },
    /// The attempt fails with this status and scope, and the stream's error record carries
    /// `record` as its status.
    Failed { outcome: (u16, FailureScope), record: u16 },
}

/// The status of the stream's `error` record.
fn error_status(published: &Published) -> Option<u64> {
    let record = errors(published).pop()?;
    let data = record.split_once("data: ")?.1.trim_end();
    serde_json::from_str::<Value>(data).ok()?["status"].as_u64()
}

/// How a turn ends for each upstream ending. xAI's WebSocket `response.done` ends a turn
/// like a completion. A lost socket (xAI's 59-minute connection end among them) is a
/// transport failure that may fail over before the client commit, and the request is never
/// sent a second time; an apply_patch call it cuts off is the bridge's 502 for the client.
#[tokio::test]
async fn upstream_endings_are_attempt_failures_without_a_second_send() {
    let after_created = |frame: &str| Act::send(created().into_iter().chain([frame.to_owned()]).collect());
    let usage = r#""usage":{"input_tokens":3,"output_tokens":5,"total_tokens":8}"#;
    let done = format!(
        r#"{{"type":"response.done","response":{{"id":"resp_d","status":"completed","model":"grok-4.3","output":[],{usage}}}}}"#
    );
    let incomplete = format!(
        r#"{{"type":"response.incomplete","response":{{"id":"resp_1","status":"incomplete","model":"grok-4.3","incomplete_details":{{"reason":"max_output_tokens"}},"output":[],{usage}}}}}"#
    );
    let patch_call = r#"{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"a","call_id":"call_a","name":"apply_patch","arguments":""}}"#;
    let plain = request(json!([user("hi")]));
    let patching =
        json!({"model": MODEL, "input": [user("patch")], "tools": [{"type": "custom", "name": "apply_patch"}]})
            .to_string();
    let failed = |status, scope| Want::Failed {
        outcome: (status, scope),
        record: status,
    };
    let cases = [
        (
            "completed",
            &plain,
            Act::send(answer("resp_1")),
            Want::Done { usage: true },
        ),
        (
            "done as the only terminal",
            &plain,
            Act::send(vec![done]),
            Want::Done { usage: true },
        ),
        (
            "incomplete",
            &plain,
            after_created(&incomplete),
            Want::Done { usage: true },
        ),
        (
            "close mid-turn",
            &plain,
            Act::send(created()).then(End::Close(1001)),
            failed(500, FailureScope::Transport),
        ),
        (
            "drop mid-turn",
            &plain,
            Act::send(created()).then(End::Drop),
            failed(500, FailureScope::Transport),
        ),
        (
            "drop during an apply_patch call",
            &patching,
            after_created(patch_call).then(End::Drop),
            Want::Failed {
                outcome: (500, FailureScope::Transport),
                record: 502,
            },
        ),
        (
            "binary message",
            &plain,
            Act::send(created()).then(End::Binary),
            failed(500, FailureScope::Transport),
        ),
        (
            "close 1009",
            &plain,
            Act::send(created()).then(End::Close(1009)),
            failed(413, FailureScope::Request),
        ),
        (
            "error frame",
            &plain,
            after_created(
                r#"{"type":"error","status":400,"error":{"code":"previous_response_not_found","message":"Previous response not found","param":"previous_response_id"}}"#,
            ),
            failed(400, FailureScope::Request),
        ),
        (
            "bare error",
            &plain,
            after_created(r#"{"error":{"code":"429","message":"slow down"}}"#),
            failed(429, FailureScope::Model),
        ),
    ];
    for (name, body, act, want) in cases {
        let upstream = XaiSocket::start([act]).await;
        let streams = Streams::new();
        let exchange = Exchange::new(upstream.credential(), MODEL, "/v1/responses");
        let executor = XaiExecutor::with_client(wreq::Client::new());
        let (outcome, published, _) = turn(&streams, &exchange, &executor, body).await;
        let usage_records = records(&published)
            .iter()
            .filter(|r| r.contains("event: x_ck.usage"))
            .count();
        match want {
            Want::Done { usage } => {
                outcome.unwrap_or_else(|e| panic!("{name}: {e:?}"));
                assert_eq!(published.terminal, Some(Terminal::Done), "{name}");
                assert_eq!(usage_records > 0, usage, "{name}: {:?}", records(&published));
            }
            Want::Failed { outcome: want, record } => {
                let error = outcome.expect_err(name);
                assert_eq!((error.status, error.scope), want, "{name}: {error:?}");
                assert_eq!(published.terminal, Some(Terminal::UpstreamError), "{name}");
                assert_eq!(error_status(&published), Some(u64::from(record)), "{name}");
            }
        }
        assert_eq!((upstream.dials(), upstream.frames().len()), (1, 1), "{name}");
    }
}

/// After xAI ends an idle socket, the next turn dials a new one and sends only its own
/// request: no transcript of the earlier turn is replayed.
#[tokio::test]
async fn the_turn_after_a_closed_socket_dials_and_sends_only_its_request() {
    let upstream = XaiSocket::start([
        Act::send(answer("resp_1")).then(End::Close(1001)),
        Act::send(answer("resp_2")),
    ])
    .await;
    let streams = Streams::new();
    let exchange = Exchange::new(upstream.credential(), MODEL, "/v1/responses");
    let executor = XaiExecutor::with_client(wreq::Client::new());
    let (outcome, ..) = turn(&streams, &exchange, &executor, &request(json!([user("first")]))).await;
    outcome.unwrap();
    upstream.ended(1).await;
    let (outcome, published, _) = turn(&streams, &exchange, &executor, &request(json!([user("second")]))).await;
    outcome.unwrap();
    assert_eq!(published.terminal, Some(Terminal::Done));
    assert_eq!(upstream.dials(), 2);
    assert_eq!(upstream.frames()[1]["input"], json!([user("second")]));
}

/// What closes, or leaves, a turn's pooled socket.
#[derive(Clone, Copy, Debug)]
enum Closer {
    /// The client disconnects with the turn in flight.
    CancelInFlight,
    /// The cancel arrives after the turn ended.
    CancelAfterEnd,
    /// The credential store removes this credential, or another one.
    Remove(&'static str),
}

/// A cancel closes the socket of a turn in flight and a credential's removal closes the
/// sockets it serves, so the next turn dials; a late cancel and another credential's
/// removal leave the pooled socket alone.
#[tokio::test]
async fn cancel_and_credential_removal_close_only_their_sockets() {
    let cases = [
        (Closer::CancelInFlight, 2),
        (Closer::CancelAfterEnd, 1),
        (Closer::Remove("xai-test"), 2),
        (Closer::Remove("xai-other"), 1),
    ];
    for (closer, dials) in cases {
        let first = match closer {
            Closer::CancelInFlight => Act::send(created()),
            _ => Act::send(answer("resp_1")),
        };
        let upstream = XaiSocket::start([first, Act::send(answer("resp_2"))]).await;
        let streams = Streams::new();
        let exchange = Exchange::new(upstream.credential(), MODEL, "/v1/responses");
        let executors = executors();
        let executor = &executors.openai.xai;
        let (started, _request) = start(&streams, &exchange, executor, &request(json!([user("hi")]))).await;
        let (started, cancel) = started.expect("the turn starts");
        match closer {
            Closer::CancelInFlight => {
                cancel.cancel().await;
                let (outcome, ..) = produce(&streams, started).await;
                let error = outcome.expect_err("the cancelled turn fails");
                assert_eq!(error.scope, FailureScope::Transport);
            }
            Closer::CancelAfterEnd => {
                produce(&streams, started).await.0.unwrap();
                cancel.cancel().await;
            }
            Closer::Remove(id) => {
                produce(&streams, started).await.0.unwrap();
                executors.credential_session_closer()(id);
            }
        }
        if dials == 2 {
            upstream.ended(1).await;
        }
        let (outcome, ..) = turn(&streams, &exchange, executor, &request(json!([user("next")]))).await;
        outcome.unwrap_or_else(|e| panic!("{closer:?}: {e:?}"));
        assert_eq!(upstream.dials(), dials, "{closer:?}");
    }
}

/// Q17: a `compaction_trigger` item reaches xAI over the socket as received, and xAI's
/// compaction comes back; nothing calls an HTTP compact.
#[tokio::test]
async fn compaction_trigger_reaches_xai_unchanged() {
    let compaction = json!({"type":"compaction","id":"cmp_1","encrypted_content":"remote-state"});
    let completed = json!({"type":"response.completed","response":response("completed", json!([compaction]))});
    let upstream = XaiSocket::start([Act::send(vec![created().remove(0), completed.to_string()])]).await;
    let streams = Streams::new();
    let exchange = Exchange::new(upstream.credential(), MODEL, "/v1/responses");
    let executor = XaiExecutor::with_client(wreq::Client::new());
    let trigger = json!({"type":"compaction_trigger","threshold":123});
    let (outcome, published, _) = turn(
        &streams,
        &exchange,
        &executor,
        &request(json!([user("hi"), trigger.clone()])),
    )
    .await;
    outcome.unwrap();
    assert_eq!(published.terminal, Some(Terminal::Done));
    assert!(records(&published).concat().contains("remote-state"));
    assert_eq!(upstream.frames()[0]["input"][1], trigger);
    assert_eq!(upstream.shared.script.lock().unwrap().plain_requests, 0);
}

/// A warmup turn (`generate: false`) completes after its `response.created`, which is all
/// xAI sends for it.
#[tokio::test]
async fn a_warmup_turn_completes_after_created() {
    let upstream = XaiSocket::start([Act::send(created())]).await;
    let streams = Streams::new();
    let exchange = Exchange::new(upstream.credential(), MODEL, "/v1/responses");
    let executor = XaiExecutor::with_client(wreq::Client::new());
    let body = json!({"model": MODEL, "generate": false, "input": [user("warm")]}).to_string();
    let (outcome, published, _) = turn(&streams, &exchange, &executor, &body).await;
    outcome.unwrap();
    assert_eq!(published.terminal, Some(Terminal::Done));
    assert_eq!(upstream.frames()[0]["generate"], false);
}

/// Turns that fail before any frame is sent. A rejected upgrade is the attempt's failure
/// under xAI's status rules (the free-usage retry hint), marked as an upstream attempt by
/// its headers. A turn without its connection's execution session is a gateway defect: an
/// internal error before any dial.
#[tokio::test]
async fn turns_that_fail_before_sending() {
    let upgrades = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let app = {
        let upgrades = upgrades.clone();
        axum::Router::new().fallback(move || {
            upgrades.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async {
                (
                    http::StatusCode::TOO_MANY_REQUESTS,
                    [("content-type", "application/json")],
                    r#"{"code":"subscription:free-usage-exhausted","error":"You've used all the included free usage for now."}"#,
                )
            }
        })
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut credential = crate::support::credential("xai", &format!("http://{addr}/v1"), TOKEN);
    credential.attributes.insert("websockets".into(), "true".into());
    let day = Some(Duration::from_secs(24 * 3600));
    let cases = [
        ("rejected upgrade", Some(SESSION), (429, day, true), 1),
        ("no execution session", None, (500, None, false), 0),
    ];
    for (name, session, (status, retry_after, answered), dialed) in cases {
        upgrades.store(0, std::sync::atomic::Ordering::SeqCst);
        let streams = Streams::new();
        let exchange = Exchange::new(credential.clone(), MODEL, "/v1/responses");
        let executor = XaiExecutor::with_client(wreq::Client::new());
        let body = request(json!([user("hi")]));
        let (started, _request) = start_in(&streams, &exchange, &executor, &body, session).await;
        let error = started.err().unwrap_or_else(|| panic!("{name}: the turn started"));
        assert_eq!((error.status, error.retry_after), (status, retry_after), "{name}");
        assert_eq!(!error.headers.is_empty(), answered, "{name}: upstream answered");
        if !answered {
            assert_eq!(error.scope, FailureScope::Request, "{name}");
        }
        assert_eq!(upgrades.load(std::sync::atomic::Ordering::SeqCst), dialed, "{name}");
    }
    server.abort();
}

/// The socket's capture events, in order.
#[derive(Default)]
struct Captured(Mutex<Vec<String>>);

impl CaptureObserver for Captured {
    fn record(&self, event: CaptureEvent<'_>) {
        let line = match event {
            CaptureEvent::WebsocketRequest(request) => format!("request {}", request.method),
            CaptureEvent::WebsocketHandshake(status, _) => format!("handshake {status}"),
            CaptureEvent::WebsocketResponse(payload) => {
                format!("response {}", String::from_utf8_lossy(payload))
            }
            CaptureEvent::WebsocketError { stage, error } => format!("error {stage}: {error}"),
            _ => return,
        };
        self.0.lock().unwrap().push(line);
    }
}

impl Captured {
    fn sink() -> (Arc<Self>, UsageSink) {
        let captured = Arc::new(Self::default());
        (
            captured.clone(),
            UsageSink::default().with_capture(CaptureSink::new(captured)),
        )
    }

    fn lines(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
}

/// A failed dial names the socket URL without the base URL's query key, in the returned
/// error and in the capture.
#[tokio::test]
async fn a_failed_dial_never_names_the_query_key() {
    const SECRET: &str = "sk-review-secret";
    let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = closed.local_addr().unwrap().to_string();
    drop(closed);
    let mut credential = crate::support::credential("xai", &format!("http://{addr}/?api_key={SECRET}"), TOKEN);
    credential.attributes.insert("websockets".into(), "true".into());
    let (captured, usage) = Captured::sink();
    let streams = Streams::new();
    let mut exchange = Exchange::new(credential, MODEL, "/v1/responses");
    exchange.usage = usage;
    let executor = XaiExecutor::with_client(wreq::Client::new());
    let (started, _request) = start(&streams, &exchange, &executor, &request(json!([user("hi")]))).await;
    let error = started.err().expect("a dial to a closed port fails");
    let body = String::from_utf8_lossy(&error.body).into_owned();
    assert!(
        body.contains(&format!(
            "dial failed: error sending request for uri (http://{addr}/?api_key=***"
        )),
        "{body}"
    );
    let captured: Vec<_> = captured
        .lines()
        .into_iter()
        .filter(|l| l.starts_with("error "))
        .collect();
    assert_eq!(captured.len(), 1, "{captured:?}");
    assert!(captured[0].starts_with("error dial: "), "{captured:?}");
    assert!(!format!("{body} {captured:?}").contains(SECRET));
}
