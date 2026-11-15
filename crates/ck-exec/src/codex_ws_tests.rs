//! Upstream WebSocket executor tests. Error-frame expectations come from Go
//! (`tests/fixtures/codex_ws_errors.json`, `tests/reference/codex/zz_rsfix_ws_errors_test.go`);
//! the lifecycle tests run the executor against a loopback axum WebSocket upstream with
//! fake credentials. Whole-handler behaviour is covered against Go fixtures in
//! ck-server's `tests/ws_e2e.rs`.

use super::*;
use std::collections::VecDeque;
use std::path::Path;

use axum::extract::State;
use axum::extract::ws::{Message as AxMessage, WebSocket as AxSocket, WebSocketUpgrade};
use axum::response::{IntoResponse, Response};
use ck_core::config::Config;
use ck_core::credential::Credential;
use ck_core::exec::{Caller, Operation};
use ck_core::format::Format;
use serde_json::Value;

#[path = "codex_ws_tests/capture.rs"]
mod capture;
#[path = "codex_ws_tests/interrupt.rs"]
mod interrupt;
#[path = "codex_ws_tests/lifecycle.rs"]
mod lifecycle;
#[path = "codex_ws_tests/transport.rs"]
mod transport_tests;

#[test]
fn error_frames_match_go() {
    let cases: Vec<Value> =
        serde_json::from_str(include_str!("../tests/fixtures/codex_ws_errors.json")).expect("fixture");
    assert_eq!(cases.len(), 15);
    for case in &cases {
        let payload = case["payload"].as_str().unwrap();
        let cooling = case["model_level_cooling"].as_bool().unwrap_or(false);
        let got = ws_error(payload, cooling);
        if !case["matched"].as_bool().unwrap() {
            assert!(got.is_none(), "{payload}");
            continue;
        }
        let got = got.unwrap_or_else(|| panic!("{payload}: not classified"));
        assert_eq!(u64::from(got.status), case["status"].as_u64().unwrap(), "{payload}");
        assert_eq!(
            String::from_utf8_lossy(&got.body),
            case["message"].as_str().unwrap(),
            "{payload}"
        );
        assert_eq!(
            got.retry_after.map(|d| d.as_millis() as i64),
            case["retry_after_ms"].as_i64(),
            "{payload}"
        );
        let credential_scoped = case["credential_scoped"].as_bool().unwrap_or(false);
        assert_eq!(
            got.scope == FailureScope::Credential && response::is_usage_limit(&String::from_utf8_lossy(&got.body)),
            credential_scoped,
            "{payload}: credential-wide usage limit"
        );
        let mut headers: Vec<(String, String)> = got
            .headers
            .iter()
            .map(|(k, v)| (k.as_str().to_owned(), v.to_str().unwrap().to_owned()))
            .collect();
        headers.sort();
        let mut want: Vec<(String, String)> = case["headers"]
            .as_object()
            .into_iter()
            .flatten()
            .map(|(k, v)| (k.to_ascii_lowercase(), v.as_str().unwrap().to_owned()))
            .collect();
        want.sort();
        assert_eq!(headers, want, "{payload}");
    }
}

/// What the scripted upstream does with each received frame.
#[derive(Clone)]
enum Act {
    Send(Vec<&'static str>),
    /// Send, then close the socket with this code.
    SendClose(Vec<&'static str>, u16),
    /// Send, then drop the connection without a close frame.
    SendDrop(Vec<&'static str>),
    /// Send one owned text frame (large payloads).
    SendOwned(String),
    SendFrames(Vec<String>),
    /// Ping `count` times, `every` apart, without any application message.
    Pings(Duration, usize),
}

#[derive(Default)]
struct Upstream {
    script: Mutex<VecDeque<Act>>,
    frames: Mutex<Vec<String>>,
    dials: Mutex<Vec<HeaderMap>>,
    /// Handshake rejection (status, body) for the next dial.
    reject: Mutex<Option<(u16, &'static str)>>,
    /// Sockets that ended (closed by either side).
    ended: Mutex<usize>,
    /// Send a binary message right after the handshake, before any request.
    binary_first: Mutex<bool>,
    /// Close the socket right after the handshake, before any request.
    close_first: Mutex<bool>,
}

async fn handler(State(up): State<Arc<Upstream>>, ws: WebSocketUpgrade, headers: axum::http::HeaderMap) -> Response {
    let mut converted = HeaderMap::new();
    for (k, v) in &headers {
        converted.insert(
            http::HeaderName::from_bytes(k.as_str().as_bytes()).unwrap(),
            http::HeaderValue::from_bytes(v.as_bytes()).unwrap(),
        );
    }
    up.dials.lock().unwrap().push(converted);
    if let Some((status, body)) = up.reject.lock().unwrap().take() {
        return (
            axum::http::StatusCode::from_u16(status).unwrap(),
            [
                ("content-type", "application/json"),
                ("x-codex-primary-used-percent", "97"),
            ],
            body,
        )
            .into_response();
    }
    ws.on_upgrade(move |socket| serve_socket(up, socket))
}

async fn serve_socket(up: Arc<Upstream>, mut socket: AxSocket) {
    if *up.binary_first.lock().unwrap() {
        let _ = socket.send(AxMessage::Binary(vec![1u8, 2, 3].into())).await;
    }
    if *up.close_first.lock().unwrap() {
        let _ = socket.send(AxMessage::Close(None)).await;
        *up.ended.lock().unwrap() += 1;
        return;
    }
    while let Some(Ok(message)) = socket.recv().await {
        let AxMessage::Text(text) = message else {
            continue;
        };
        up.frames.lock().unwrap().push(text.as_str().to_owned());
        let act = up.script.lock().unwrap().pop_front();
        let (events, close) = match act {
            Some(Act::Send(events)) => (events, None),
            Some(Act::SendDrop(events)) => {
                for event in events {
                    let _ = socket.send(AxMessage::Text(event.into())).await;
                }
                break;
            }
            Some(Act::SendClose(events, code)) => (events, Some(code)),
            Some(Act::SendOwned(text)) => {
                let _ = socket.send(AxMessage::Text(text.into())).await;
                continue;
            }
            Some(Act::SendFrames(events)) => {
                for event in events {
                    let _ = socket.send(AxMessage::Text(event.into())).await;
                }
                continue;
            }
            Some(Act::Pings(every, count)) => {
                for _ in 0..count {
                    tokio::time::sleep(every).await;
                    if socket.send(AxMessage::Ping(Bytes::new())).await.is_err() {
                        break;
                    }
                }
                continue;
            }
            None => continue,
        };
        for event in events {
            let _ = socket.send(AxMessage::Text(event.into())).await;
        }
        if let Some(code) = close {
            let _ = socket
                .send(AxMessage::Close(Some(axum::extract::ws::CloseFrame {
                    code,
                    reason: "bye".into(),
                })))
                .await;
            break;
        }
    }
    *up.ended.lock().unwrap() += 1;
}

async fn upstream(script: Vec<Act>) -> (Arc<Upstream>, String) {
    let up = Arc::new(Upstream::default());
    up.script.lock().unwrap().extend(script);
    let app = axum::Router::new().fallback(handler).with_state(up.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });
    (up, url)
}

fn credential(base_url: &str) -> Credential {
    let mut credential = Credential::from_file(
        Path::new("/fake"),
        Path::new("/fake/codex-ws.json"),
        serde_json::json!({"type": "codex"}).as_object().unwrap().clone(),
    )
    .unwrap();
    for (k, v) in [("api_key", "sk-FAKE"), ("base_url", base_url), ("websockets", "true")] {
        credential.attributes.insert(k.into(), v.into());
    }
    credential
}

fn request(body: &str) -> ExecRequest {
    let body = Bytes::from(body.to_owned());
    ExecRequest {
        operation: Operation::Generate,
        source_format: Format::OpenAIResponse,
        response_format: Format::OpenAIResponse,
        requested_model: "gpt-fixture".into(),
        model: "gpt-fixture".into(),
        original_body: body.clone(),
        body,
        stream: true,
        alt: None,
        session: None,
        execution_session: Some("conn-1".into()),
        derived_session: None,
        resolved_model: None,
        usage: Default::default(),
        request_path: String::new(),
        headers: HeaderMap::new(),
        caller: Caller {
            principal: "client-key-FAKE".into(),
            source: "authorization",
            provider: String::new(),
        },
    }
}

fn session(continuation: bool) -> ExecSession {
    ExecSession {
        id: "conn-1".into(),
        continuation,
    }
}

async fn collect(response: ExecResponse) -> Vec<Result<String, ExecError>> {
    let ResponseBody::Stream(stream) = response.body else {
        panic!("websocket turns stream");
    };
    stream
        .map(|item| item.map(|b| String::from_utf8_lossy(&b).into_owned()))
        .collect()
        .await
}

const CREATED: &str = r#"{"type":"response.created","response":{"id":"r1","output":[]}}"#;
const COMPLETED: &str = r#"{"type":"response.completed","response":{"id":"r1","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}"#;
const BODY: &str = r#"{"model":"gpt-fixture","input":[{"type":"message","role":"user","content":"hi"}]}"#;

/// A request whose capture goes to `tap`.
fn tapped(body: &str, tap: &Arc<crate::codex_testkit::Wiretap>) -> ExecRequest {
    let mut req = request(body);
    req.usage = ck_core::exec::UsageSink::default().with_capture(ck_core::exec::CaptureSink::new(tap.clone()));
    req
}

/// A pooled socket on session "conn-1" to the mock at `url`, its reader running.
async fn pooled(executor: &CodexExecutor, url: &str) -> Arc<super::Upstream> {
    let session = executor.ws.session("conn-1");
    let target = Target::new(
        &credential(url),
        ws_url(&format!("{url}/responses")).unwrap(),
        &Config::default(),
    );
    executor
        .ensure(&session, &target, &HeaderMap::new(), "gpt-fixture", false, None)
        .await
        .unwrap()
        .0
}

async fn read_text(reads: &mut mpsc::Receiver<Read>) -> String {
    match tokio::time::timeout(Duration::from_secs(5), reads.recv()).await {
        Ok(Some(Read::Text(text))) => text,
        Ok(Some(Read::Failed(error))) => panic!("read failed: {error}"),
        other => panic!("no upstream event: {}", other.is_ok()),
    }
}

/// What the mock upstream received, once `count` frames arrived.
async fn received(up: &Upstream, count: usize) -> Vec<String> {
    for _ in 0..250 {
        if up.frames.lock().unwrap().len() >= count {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    up.frames.lock().unwrap().clone()
}

const INTERRUPT_R1: &str = r#"{"type":"response.interrupt","response_id":"r1","mode":"discard_partial_items"}"#;

#[path = "codex_duplex_tests.rs"]
mod duplex;

#[path = "codex_ws_conformance_tests.rs"]
mod conformance;

/// `buildCodexResponsesWebsocketURL` answers, recorded from Go v8.0.35 with the inputs of `url.Parse` and
/// `URL.String()` (tests/reference/go_url).
#[test]
fn ws_url_follows_go() {
    let cases: Value = serde_json::from_str(include_str!("../tests/fixtures/go_url_string.json")).unwrap();
    for case in cases.as_array().unwrap() {
        let input = case["in"].as_str().unwrap();
        let want = &case["codex_ws"];
        match ws_url(input) {
            Ok(got) => assert_eq!(Some(got.as_str()), want["out"].as_str(), "{input}"),
            Err(e) => {
                assert_eq!(
                    String::from_utf8_lossy(&e.body),
                    want["err"].as_str().unwrap(),
                    "{input}"
                );
                // A plain Go error before dialing: no status, no retry round, and the
                // model cools for the credential.
                assert_eq!((e.status, e.scope), (0, ck_core::exec::FailureScope::Model), "{input}");
            }
        }
    }
}
