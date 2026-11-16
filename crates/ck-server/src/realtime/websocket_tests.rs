//! Responses WebSocket turns under the keeper model (E-PLAN §2.4.1): one row per acceptance
//! item, against a loopback Codex upstream speaking HTTP SSE or WebSocket with fake keys.
//! Every row also checks that no internal frame or `X-Ck-Stream-Id` reaches the client.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::extract::ws::rejection::WebSocketUpgradeRejection;
use axum::extract::ws::{Message as AxMessage, WebSocket as AxSocket, WebSocketUpgrade};
use axum::response::{IntoResponse, Response};
use ck_core::config::Config;
use ck_exec::Executors;
use futures_util::StreamExt;
use serde_json::{Value, json};
use wreq::ws::message::{CloseFrame as WsClose, Message as WsMessage};

use crate::router;

const MODEL: &str = "gpt-fixture";
const CREATED: &str = r#"{"type":"response.created","sequence_number":0,"response":{"id":"up_1","object":"response","created_at":1,"model":"gpt-fixture","status":"in_progress","output":[]}}"#;
const OPEN: [&str; 3] = [
    r#"{"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"msg_1","status":"in_progress","role":"assistant","content":[]}}"#,
    r#"{"type":"response.content_part.added","item_id":"msg_1","output_index":0,"content_index":0,"part":{"type":"output_text","text":""}}"#,
    r#"{"type":"response.output_text.delta","item_id":"msg_1","output_index":0,"content_index":0,"delta":"hi"}"#,
];
const CLOSE: [&str; 4] = [
    r#"{"type":"response.output_text.done","item_id":"msg_1","output_index":0,"content_index":0,"text":"hi"}"#,
    r#"{"type":"response.content_part.done","item_id":"msg_1","output_index":0,"content_index":0,"part":{"type":"output_text","text":"hi","annotations":[]}}"#,
    r#"{"type":"response.output_item.done","output_index":0,"item":{"type":"message","id":"msg_1","status":"completed","role":"assistant","content":[{"type":"output_text","text":"hi","annotations":[]}]}}"#,
    r#"{"type":"response.completed","response":{"id":"up_1","object":"response","created_at":1,"model":"gpt-fixture","status":"completed","store":false,"output":[],"usage":{"input_tokens":7,"output_tokens":1,"total_tokens":8}}}"#,
];
const FAILED: &str = r#"{"type":"response.failed","response":{"id":"up_1","status":"failed","error":{"code":"server_error","message":"upstream unavailable"}}}"#;

/// What a credential's upstream does with its next request.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Script {
    Answer,
    /// The response starts, then fails.
    FailsMidway,
    /// Answers with this HTTP status before any event.
    Refuses(u16),
    /// The response starts and never ends; the test sees whether the turn was cancelled.
    Holds,
}

#[derive(Default)]
struct Upstream {
    scripts: Mutex<HashMap<String, Script>>,
    /// `(key, request)` for every request or `response.create` frame.
    sent: Mutex<Vec<(String, Value)>>,
    /// A held response's HTTP body was dropped, or its socket got `response.interrupt`, the
    /// transport's cancel command.
    cancelled: AtomicBool,
}

impl Upstream {
    fn script(&self, key: &str) -> Script {
        self.scripts.lock().unwrap().get(key).copied().unwrap_or(Script::Answer)
    }

    fn set(&self, key: &str, script: Script) {
        self.scripts.lock().unwrap().insert(key.to_owned(), script);
    }

    fn sent(&self) -> Vec<(String, Value)> {
        std::mem::take(&mut *self.sent.lock().unwrap())
    }
}

/// Sets the flag when a held body is dropped.
struct Dropped(Arc<Upstream>);

impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.cancelled.store(true, Ordering::SeqCst);
    }
}

fn events(script: Script) -> Vec<&'static str> {
    let mut out = vec![CREATED];
    out.extend(OPEN);
    match script {
        Script::Answer => out.extend(CLOSE),
        Script::FailsMidway => out.push(FAILED),
        Script::Holds | Script::Refuses(_) => {}
    }
    out
}

async fn upstream(
    State(up): State<Arc<Upstream>>,
    headers: axum::http::HeaderMap,
    ws: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
    body: axum::body::Bytes,
) -> Response {
    let key = headers["authorization"]
        .to_str()
        .unwrap()
        .trim_start_matches("Bearer ")
        .to_owned();
    if let Ok(ws) = ws {
        return ws
            .on_upgrade(move |socket| serve_socket(up, key, socket))
            .into_response();
    }
    up.sent
        .lock()
        .unwrap()
        .push((key.clone(), serde_json::from_slice(&body).unwrap()));
    let script = up.script(&key);
    if let Script::Refuses(status) = script {
        let kind = if status < 500 {
            "invalid_request_error"
        } else {
            "server_error"
        };
        let body = format!(r#"{{"error":{{"message":"refused","type":"{kind}"}}}}"#);
        return (axum::http::StatusCode::from_u16(status).unwrap(), body).into_response();
    }
    let sse: String = events(script)
        .iter()
        .map(|event| format!("data: {event}\n\n"))
        .collect();
    let first = futures_util::stream::once(async move { Ok::<_, std::io::Error>(axum::body::Bytes::from(sse)) });
    // A midway failure comes after the client saw the response start.
    let body = if script == Script::FailsMidway {
        let open: String = events(Script::Holds)
            .iter()
            .map(|event| format!("data: {event}\n\n"))
            .collect();
        let failed = async {
            tokio::time::sleep(Duration::from_millis(300)).await;
            Ok::<_, std::io::Error>(axum::body::Bytes::from(format!("data: {FAILED}\n\n")))
        };
        futures_util::stream::iter([Ok(axum::body::Bytes::from(open))])
            .chain(futures_util::stream::once(failed))
            .boxed()
    } else if script == Script::Holds {
        let guard = Dropped(up.clone());
        first
            .chain(futures_util::stream::pending())
            .map(move |chunk| {
                let _ = &guard;
                chunk
            })
            .boxed()
    } else {
        first.boxed()
    };
    (
        [("content-type", "text/event-stream")],
        axum::body::Body::from_stream(body),
    )
        .into_response()
}

async fn serve_socket(up: Arc<Upstream>, key: String, mut socket: AxSocket) {
    let mut holding = false;
    while let Some(Ok(message)) = socket.recv().await {
        let AxMessage::Text(text) = message else {
            break;
        };
        let frame: Value = serde_json::from_str(text.as_str()).unwrap();
        if holding {
            up.cancelled
                .fetch_or(frame["type"] == "response.interrupt", Ordering::SeqCst);
            continue;
        }
        up.sent.lock().unwrap().push((key.clone(), frame));
        let script = up.script(&key);
        for event in events(script) {
            if socket.send(AxMessage::Text(event.into())).await.is_err() {
                return;
            }
        }
        holding = script == Script::Holds;
    }
}

/// A proxy whose two Codex keys, `x` preferred over `y`, reach the same mock upstream.
struct Proxy {
    addr: String,
    up: Arc<Upstream>,
    rt: Arc<crate::Runtime>,
}

async fn proxy(websockets: bool, extra: &str) -> Proxy {
    let up = Arc::new(Upstream::default());
    let upstream = serve(axum::Router::new().fallback(upstream).with_state(up.clone())).await;
    let entry = |key: &str, priority: u8| {
        format!(
            "    - base-url: http://{upstream}\n      priority: {priority}\n      models:\n        - name: {MODEL}\n      keys:\n        - api-key: {key}\n          websockets: {websockets}\n"
        )
    };
    let cfg = Config::parse(&format!(
        "{extra}api-keys:\n  codex:\n{}{}",
        entry("x", 2),
        entry("y", 1)
    ))
    .unwrap();
    let credentials = ck_core::config::credentials::from_config(&cfg);
    let executors = Executors {
        claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
        codex: ck_exec::codex::CodexExecutor::new().unwrap(),
        devices: Default::default(),
        openai: Default::default(),
        google: Default::default(),
    };
    let rt = Arc::new(crate::testing::runtime(cfg, credentials, executors));
    let addr = serve(router(rt.clone())).await;
    Proxy { addr, up, rt }
}

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr.to_string()
}

struct Client(wreq::ws::WebSocket);

impl Client {
    async fn open(proxy: &Proxy) -> Self {
        let response = wreq::Client::new()
            .websocket(format!("ws://{}/v1/responses", proxy.addr))
            .send()
            .await
            .unwrap();
        assert!(
            response.headers().get("x-ck-stream-id").is_none(),
            "{:?}",
            response.headers()
        );
        Self(response.into_websocket().await.unwrap())
    }

    async fn send(&mut self, frame: Value) {
        self.0.send(WsMessage::text(frame.to_string())).await.unwrap();
    }

    /// The next frame, which is never internal.
    async fn frame(&mut self) -> Value {
        let message = tokio::time::timeout(Duration::from_secs(5), self.0.next())
            .await
            .expect("a frame within 5 s")
            .expect("the socket is open")
            .unwrap();
        let WsMessage::Text(text) = message else {
            panic!("a text frame: {message:?}");
        };
        assert!(!text.as_str().contains("x_ck"), "an internal frame: {}", text.as_str());
        serde_json::from_str(text.as_str()).unwrap()
    }

    /// Frames up to the turn's last: its terminal response event or an `error`.
    async fn until_end(&mut self) -> Vec<Value> {
        let mut frames = Vec::new();
        loop {
            let frame = self.frame().await;
            let kind = frame["type"].as_str().unwrap().to_owned();
            frames.push(frame);
            if matches!(
                kind.as_str(),
                "response.completed" | "response.failed" | "response.incomplete"
            ) || (kind == "error" && frames.len() == 1)
            {
                return frames;
            }
        }
    }

    async fn turn(&mut self, request: Value) -> Vec<Value> {
        self.send(create(request)).await;
        self.until_end().await
    }

    async fn close(mut self) {
        let _ = self
            .0
            .send(WsMessage::Close(Some(WsClose {
                code: 1000.into(),
                reason: "".into(),
            })))
            .await;
    }
}

fn create(mut request: Value) -> Value {
    request["type"] = json!("response.create");
    request["model"] = json!(MODEL);
    request
}

/// The id of a turn that completed.
fn completed(frames: &[Value]) -> String {
    let last = frames.last().unwrap();
    assert_eq!(last["type"], "response.completed", "{frames:#?}");
    let id = last["response"]["id"].as_str().unwrap().to_owned();
    assert!(id.starts_with("resp_") && id != "up_1", "a cooker id: {id}");
    id
}

fn not_found(frames: &[Value]) {
    let [error] = frames else {
        panic!("one error frame: {frames:#?}");
    };
    assert_eq!(error["type"], "error", "{error}");
    assert_eq!(error["status"], 400, "{error}");
    assert_eq!(error["error"]["code"], "previous_response_not_found", "{error}");
}

/// What one upstream request carried as conversation text, in order.
fn said(request: &Value) -> String {
    request.to_string()
}

async fn wait_for(flag: &AtomicBool) -> bool {
    let started = Instant::now();
    while !flag.load(Ordering::SeqCst) {
        if started.elapsed() > Duration::from_secs(5) {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    true
}

#[derive(Clone, Copy, Debug)]
enum Row {
    /// 1. `store: false`, then a delta turn on the same socket.
    SameSocketContinues,
    /// 2. The `store: false` id on a new socket; `true` turns on `ignore-client-store-false`.
    NewSocket(bool),
    /// 3. Unknown and superseded ids, with no upstream send.
    UnknownIds,
    /// 4. A continuation fails over from `x` to `y` with the expanded history.
    ContinuationFailsOver,
    /// 5. The index cannot retain the turn.
    NotRetained,
    /// 6. Mid-stream and pre-start failures, then the next turn on the same socket.
    Failures,
    /// 7. Client close with the turn in flight; `true` runs it on an upstream socket.
    CloseCancels(bool),
    /// 8. A third `response.create` while one runs and one waits.
    Overload,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_websocket_turns() {
    for row in [
        Row::SameSocketContinues,
        Row::NewSocket(false),
        Row::NewSocket(true),
        Row::UnknownIds,
        Row::ContinuationFailsOver,
        Row::NotRetained,
        Row::Failures,
        Row::CloseCancels(false),
        Row::CloseCancels(true),
        Row::Overload,
    ] {
        run(row).await;
    }
}

async fn run(row: Row) {
    let first = json!({"input": "first question", "store": false});
    let next = |id: &str| json!({"input": "second", "store": false, "previous_response_id": id});
    match row {
        Row::SameSocketContinues => {
            let p = proxy(false, "").await;
            let mut client = Client::open(&p).await;
            let id = completed(&client.turn(first).await);
            p.up.sent();
            let frames = client.turn(next(&id)).await;
            let second = completed(&frames);
            assert_ne!(second, id, "{row:?}");
            assert!(
                frames
                    .iter()
                    .all(|f| f["response"]["id"].is_null() || f["response"]["id"] == second)
            );
            let sent = p.up.sent();
            let [(_, request)] = sent.as_slice() else {
                panic!("{row:?}: one send: {sent:?}");
            };
            let text = said(request);
            for part in ["first question", "hi", "second"] {
                assert!(text.contains(part), "{row:?}: {part} resent: {text}");
            }
            assert!(!text.contains("previous_response_id"), "{row:?}: {text}");
            client.close().await;
        }
        Row::NewSocket(ignore) => {
            let extra = if ignore {
                "responses:\n  ignore-client-store-false: true\n"
            } else {
                ""
            };
            let p = proxy(false, extra).await;
            let mut a = Client::open(&p).await;
            let frames = a.turn(first).await;
            let id = completed(&frames);
            assert_eq!(frames.last().unwrap()["response"]["store"], ignore, "{row:?}");
            // `a` stays open: its pin is still live while `b` asks for it.
            p.up.sent();
            let mut b = Client::open(&p).await;
            let frames = b.turn(next(&id)).await;
            if ignore {
                completed(&frames);
                assert_eq!(frames.last().unwrap()["response"]["store"], true, "{row:?}");
                assert_eq!(p.up.sent().len(), 1, "{row:?}");
            } else {
                not_found(&frames);
                assert!(p.up.sent().is_empty(), "{row:?}");
            }
            a.close().await;
            b.close().await;
        }
        Row::UnknownIds => {
            let p = proxy(false, "").await;
            let mut client = Client::open(&p).await;
            let one = completed(&client.turn(first.clone()).await);
            completed(&client.turn(first).await);
            p.up.sent();
            // `one` is no longer the socket's pin: a later `store: false` turn replaced it.
            for id in ["resp_00000000000000000000000000000000", "resp_unknown", one.as_str()] {
                not_found(&client.turn(next(id)).await);
            }
            assert!(p.up.sent().is_empty(), "{row:?}");
            client.close().await;
        }
        Row::ContinuationFailsOver => {
            let p = proxy(false, "").await;
            let mut client = Client::open(&p).await;
            let id = completed(&client.turn(first).await);
            assert_eq!(p.up.sent()[0].0, "x", "{row:?}");
            p.up.set("x", Script::Refuses(500));
            completed(&client.turn(next(&id)).await);
            let sent = p.up.sent();
            let keys: Vec<_> = sent.iter().map(|(key, _)| key.as_str()).collect();
            assert_eq!(keys, ["x", "y"], "{row:?}");
            for (_, request) in &sent {
                let text = said(request);
                assert!(
                    text.contains("first question") && text.contains("hi"),
                    "{row:?}: {text}"
                );
            }
            client.close().await;
        }
        Row::NotRetained => {
            let p = proxy(false, "").await;
            let mut client = Client::open(&p).await;
            // A turn first opens the store; then every index slot is taken.
            completed(&client.turn(json!({"input": "warm"})).await);
            let capacity = p.rt.streams().store().unwrap().limits().header_slots as usize;
            let index = p.rt.streams().responses();
            while index.reserve(capacity, crate::inference::responses_state::ResponseId::random(), false) {}
            let id = completed(&client.turn(json!({"input": "first question"})).await);
            not_found(&client.turn(next(&id)).await);
            client.close().await;
        }
        Row::Failures => {
            let p = proxy(false, "").await;
            let mut client = Client::open(&p).await;
            p.up.set("x", Script::FailsMidway);
            let frames = client.turn(first.clone()).await;
            let id = frames[0]["response"]["id"].as_str().unwrap().to_owned();
            assert_eq!(frames[0]["type"], "response.created", "{frames:#?}");
            let failed = frames.last().unwrap();
            assert_eq!(failed["type"], "response.failed", "{frames:#?}");
            assert_eq!(failed["response"]["id"], id.as_str());
            assert_eq!(failed["response"]["status"], "failed");
            assert!(failed["response"]["error"].is_object(), "{failed}");
            p.up.set("x", Script::Refuses(400));
            p.up.set("y", Script::Refuses(400));
            let frames = client.turn(first.clone()).await;
            let [error] = frames.as_slice() else {
                panic!("one error frame: {frames:#?}");
            };
            assert_eq!(error["type"], "error", "{error}");
            assert_eq!(error["status"], 400, "{error}");
            p.up.set("x", Script::Answer);
            p.up.set("y", Script::Answer);
            completed(&client.turn(first).await);
            client.close().await;
        }
        Row::CloseCancels(websockets) => {
            let p = proxy(websockets, "").await;
            let mut client = Client::open(&p).await;
            p.up.set("x", Script::Holds);
            client.send(create(first)).await;
            assert_eq!(client.frame().await["type"], "response.created", "{row:?}");
            client.close().await;
            assert!(
                wait_for(&p.up.cancelled).await,
                "{row:?}: the upstream turn was not cancelled"
            );
            let sent = p.up.sent();
            assert_eq!(sent[0].1["type"] == "response.create", websockets, "{row:?}: {sent:?}");
        }
        Row::Overload => {
            let p = proxy(false, "").await;
            let mut client = Client::open(&p).await;
            p.up.set("x", Script::Holds);
            client.send(create(first.clone())).await;
            assert_eq!(client.frame().await["type"], "response.created", "{row:?}");
            client.send(create(first.clone())).await;
            client.send(create(first)).await;
            let error = loop {
                let frame = client.frame().await;
                if frame["type"] == "error" {
                    break frame;
                }
            };
            assert_eq!(error["status"], 503, "{error}");
            assert_eq!(error["error"]["code"], "working_set_unavailable", "{error}");
            client.close().await;
        }
    }
}
