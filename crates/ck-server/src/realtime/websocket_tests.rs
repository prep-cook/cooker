//! Connection lifecycle under backpressure. Behavioural parity with Go is covered by
//! `tests/ws_e2e.rs` (Go fixtures); this checks the bound Rust adds where Go closes the
//! socket under a blocked writer.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::ws::{CloseFrame, Message as AxMessage, WebSocket as AxSocket, WebSocketUpgrade};
use ck_core::config::Config;
use ck_exec::Executors;
use serde_json::Value;

use crate::realtime::websocket_tools::is_retained;
use crate::router;

/// Owns each test or scenario's listeners, including graceful shutdown and joining.
struct Servers {
    tasks: tokio::task::JoinSet<()>,
    stop: tokio::sync::watch::Sender<()>,
}

impl Default for Servers {
    fn default() -> Self {
        Self {
            tasks: tokio::task::JoinSet::new(),
            stop: tokio::sync::watch::channel(()).0,
        }
    }
}

impl Servers {
    async fn serve(&mut self, app: axum::Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut stopped = self.stop.subscribe();
        self.tasks.spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = stopped.changed().await;
                })
                .await
                .unwrap();
        });
        format!("127.0.0.1:{}", addr.port())
    }

    async fn shutdown(mut self) {
        self.stop.send_replace(());
        let stopped = tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(result) = self.tasks.join_next().await {
                result.expect("mock server failed");
            }
        })
        .await;
        self.tasks.shutdown().await;
        stopped.expect("mock servers did not shut down");
    }
}

/// Answers the first request with far more than the loopback buffers hold, then closes
/// with 1009.
async fn flood_then_close(mut socket: AxSocket) {
    if socket.recv().await.is_none() {
        return;
    }
    let created = r#"{"type":"response.created","response":{"id":"r1","output":[]}}"#;
    let _ = socket.send(AxMessage::Text(created.into())).await;
    let delta = format!(
        r#"{{"type":"response.output_text.delta","delta":"{}"}}"#,
        "x".repeat(4 * 1024 * 1024)
    );
    for _ in 0..8 {
        let _ = socket.send(AxMessage::Text(delta.clone().into())).await;
    }
    let _ = socket
        .send(AxMessage::Close(Some(CloseFrame {
            code: 1009,
            reason: "too big".into(),
        })))
        .await;
}

/// The client stops reading mid-response and upstream then fails. The close frame cannot
/// be flushed behind the queued data, yet the connection must still end and release its
/// session state (Go closes the socket instead of waiting behind the writer).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upstream_loss_while_the_client_stalls_releases_the_session() {
    let mut servers = Servers::default();
    let upstream = servers
        .serve(axum::Router::new().fallback(|ws: WebSocketUpgrade| async move { ws.on_upgrade(flood_then_close) }))
        .await;
    let cfg = Config::parse(&format!(
        "api-keys:\n  codex:\n  - name: codex-1\n    base-url: http://{upstream}\n    models:\n      - name: gpt-fixture\n    keys:\n      - api-key: sk-FAKE\n        websockets: true\n"
    ))
    .unwrap();
    let credentials = ck_core::config::credentials::from_config(&cfg);
    assert_eq!(
        credentials[0].metadata["models"],
        serde_json::json!([{ "name": "gpt-fixture" }])
    );
    let executors = Executors {
        claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
        codex: ck_exec::codex::CodexExecutor::new().unwrap(),
        devices: Default::default(),
        openai: Default::default(),
        google: Default::default(),
    };
    let proxy = servers
        .serve(router(Arc::new(crate::testing::runtime(cfg, credentials, executors))))
        .await;

    let key = "stalled-client-session";
    let mut socket = wreq::Client::new()
        .websocket(format!("ws://{proxy}/v1/responses"))
        .header("session_id", key)
        .send()
        .await
        .unwrap()
        .into_websocket()
        .await
        .unwrap();
    let create =
        r#"{"type":"response.create","model":"gpt-fixture","input":[{"type":"message","role":"user","content":"hi"}]}"#;
    socket.send(wreq::ws::message::Message::text(create)).await.unwrap();

    let started = Instant::now();
    let mut seen = false;
    while started.elapsed() < Duration::from_secs(10) {
        let retained = is_retained(key);
        seen |= retained;
        if seen && !retained {
            // The client never read a byte: the server gave up on the flush and cleaned up.
            drop(socket);
            servers.shutdown().await;
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let state: Value = serde_json::json!({ "connection_seen": seen });
    panic!("connection still holds its session after 10s: {state}");
}

/// Local validation replies, like generated output, stop when the upstream is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upstream_loss_cancels_a_blocked_local_error_reply() {
    use super::{Connection, Flow, Sink};
    use futures_util::StreamExt;
    let mut servers = Servers::default();

    let close = Arc::new(tokio::sync::Notify::new());
    let ended = Arc::new(tokio::sync::Notify::new());
    let (accepted, downstream) = tokio::sync::oneshot::channel::<AxSocket>();
    let accepted = Arc::new(std::sync::Mutex::new(Some(accepted)));
    let app = axum::Router::new()
        .route(
            "/downstream",
            axum::routing::get(move |ws: WebSocketUpgrade| {
                let accepted = accepted.lock().unwrap().take().unwrap();
                async move {
                    ws.on_upgrade(move |socket| async move {
                        let _ = accepted.send(socket);
                    })
                }
            }),
        )
        .route(
            "/upstream/responses",
            axum::routing::get({
                let (close, ended) = (close.clone(), ended.clone());
                move |ws: WebSocketUpgrade| {
                    let (close, ended) = (close.clone(), ended.clone());
                    async move {
                        ws.on_upgrade(move |mut socket: AxSocket| async move {
                            assert!(matches!(socket.recv().await, Some(Ok(AxMessage::Text(_)))));
                            for event in [
                                r#"{"type":"response.created","response":{"id":"r1","output":[]}}"#,
                                r#"{"type":"response.completed","response":{"id":"r1","output":[]}}"#,
                            ] {
                                socket.send(AxMessage::Text(event.into())).await.unwrap();
                            }
                            close.notified().await;
                            let _ = socket.send(AxMessage::Close(None)).await;
                            ended.notify_one();
                        })
                    }
                }
            }),
        );
    let address = servers.serve(app).await;
    let cfg = Config::parse(&format!(
        "api-keys:\n  codex:\n  - name: codex-1\n    base-url: http://{address}/upstream\n    models:\n      - name: gpt-fixture\n    keys:\n      - api-key: sk-FAKE\n        websockets: true\n"
    )).unwrap();
    let credentials = ck_core::config::credentials::from_config(&cfg);
    let executors = Executors {
        claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
        codex: ck_exec::codex::CodexExecutor::new().unwrap(),
        devices: Default::default(),
        openai: Default::default(),
        google: Default::default(),
    };
    let rt = Arc::new(crate::testing::runtime(cfg, credentials, executors));
    let client = wreq::Client::new()
        .websocket(format!("ws://{address}/downstream"))
        .send()
        .await
        .unwrap()
        .into_websocket()
        .await
        .unwrap();
    let (writer, reader) = downstream.await.unwrap().split();
    let mut sink = Sink {
        inner: Arc::new(tokio::sync::Mutex::new(writer)),
        timeline: None,
        terminated: None,
    };
    let mut connection = Connection::new(
        rt.clone(),
        ck_core::exec::Caller {
            principal: "client-key-FAKE".into(),
            source: "authorization",
            provider: String::new(),
        },
        Default::default(),
    );
    let create = r#"{"type":"response.create","model":"gpt-fixture","input":[]}"#.to_owned();
    assert!(matches!(connection.turn(&mut sink, create).await, Flow::Next));

    // A writer already in flight backpressures this local reply without a huge allocation.
    let blocked_writer = sink.inner.clone().lock_owned().await;
    let result = {
        let turn = connection.turn(&mut sink, r#"{"type":"unsupported","model":"gpt-fixture"}"#.to_owned());
        tokio::pin!(turn);
        assert!(futures_util::poll!(&mut turn).is_pending());
        close.notify_one();
        tokio::time::timeout(Duration::from_secs(5), &mut turn).await
    };
    drop(blocked_writer);
    rt.executors.close_session(&connection.session);
    tokio::time::timeout(Duration::from_secs(5), ended.notified())
        .await
        .unwrap();
    drop((connection, sink, reader, client, rt));
    servers.shutdown().await;
    assert!(
        matches!(result, Ok(Flow::End)),
        "upstream loss did not cancel the local reply"
    );
}

/// Drops with the upstream response body: the proxy cancelled its request.
struct BodyDropped(Arc<std::sync::atomic::AtomicBool>);

impl Drop for BodyDropped {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

/// With steering configured, Go's downstream reader cancels the request context when the
/// client leaves, so a turn in flight on an HTTP credential stops its upstream request.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn steering_reader_cancels_the_turn_when_the_client_leaves() {
    use futures_util::StreamExt;
    let mut servers = Servers::default();
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = dropped.clone();
    let upstream = servers
        .serve(axum::Router::new().fallback(move || {
            let guard = BodyDropped(flag.clone());
            async move {
                let events = concat!(
                    "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"output\":[]}}\n\n",
                    "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"hi\"}\n\n",
                );
                // The response never completes; only a cancelled request releases it.
                let body =
                    futures_util::stream::once(async move { Ok::<_, std::io::Error>(axum::body::Bytes::from(events)) })
                        .chain(futures_util::stream::pending())
                        .map(move |chunk| {
                            let _ = &guard;
                            chunk
                        });
                (
                    [("content-type", "text/event-stream")],
                    axum::body::Body::from_stream(body),
                )
            }
        }))
        .await;
    let cfg = Config::parse(&format!(
        "upstream:\n  codex:\n    response-steering: true\napi-keys:\n  codex:\n  - name: codex-1\n    base-url: http://{upstream}\n    models:\n      - name: gpt-fixture\n    keys:\n      - api-key: sk-FAKE\n"
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
    let proxy = servers
        .serve(router(Arc::new(crate::testing::runtime(cfg, credentials, executors))))
        .await;

    let mut socket = wreq::Client::new()
        .websocket(format!("ws://{proxy}/v1/responses"))
        .send()
        .await
        .unwrap()
        .into_websocket()
        .await
        .unwrap();
    let create =
        r#"{"type":"response.create","model":"gpt-fixture","input":[{"type":"message","role":"user","content":"hi"}]}"#;
    socket.send(wreq::ws::message::Message::text(create)).await.unwrap();
    let first = tokio::time::timeout(Duration::from_secs(10), socket.next())
        .await
        .expect("the turn streams")
        .expect("a frame")
        .unwrap();
    let wreq::ws::message::Message::Text(first) = first else {
        panic!("text frame: {first:?}");
    };
    assert!(first.contains("response.created"), "{first}");
    assert!(
        !dropped.load(std::sync::atomic::Ordering::SeqCst),
        "upstream still streaming"
    );
    drop(socket);

    let started = Instant::now();
    while !dropped.load(std::sync::atomic::Ordering::SeqCst) {
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the upstream request outlived the client by 5s"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    servers.shutdown().await;
}

/// The request log of a Responses WebSocket connection: Go's downstream timeline
/// (`openai_responses_websocket_timeline.go`) with one `websocket.request` part per client
/// message, one `websocket.response` part per frame written and the read error that ended
/// the connection as `websocket.disconnect` (gorilla's close text), plus the upstream
/// capture of each turn under the upgrade request's log.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_log_records_the_websocket_timeline_and_each_turn() {
    use futures_util::StreamExt;
    use wreq::ws::message::{CloseFrame as WsClose, Message as WsMessage};
    let mut servers = Servers::default();
    const CREATED: &str = r#"{"type":"response.created","response":{"id":"r1","output":[]}}"#;
    const COMPLETED: &str = r#"{"type":"response.completed","response":{"id":"r1","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}"#;
    let upstream = servers
        .serve(axum::Router::new().fallback(|| async {
            (
                [("content-type", "text/event-stream")],
                format!("data: {CREATED}\n\ndata: {COMPLETED}\n\n"),
            )
        }))
        .await;
    let dir = std::env::temp_dir().join(format!("cooker-ws-timeline-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let cfg = Config::parse(&format!(
        "observability: {{logs: {{request-log: true}}}}\napi-keys:\n  codex:\n  - name: codex-1\n    base-url: http://{upstream}\n    models:\n      - name: gpt-fixture\n    keys:\n      - api-key: sk-FAKE\n"))
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
    let management = crate::management::api::Management::with_options(
        rt.clone(),
        Arc::new(crate::store::config_store::ConfigStore::memory_with(
            &ck_core::config::yaml_string(&rt.config().document).unwrap(),
        )),
        crate::management::api::Options {
            log_dir: Some(dir.clone()),
            management_password: Some(String::new()),
            ..Default::default()
        },
    );
    let proxy = servers
        .serve(crate::observability::request_logging::router(&management, router(rt)))
        .await;

    let mut socket = wreq::Client::new()
        .websocket(format!("ws://{proxy}/v1/responses"))
        .send()
        .await
        .unwrap()
        .into_websocket()
        .await
        .unwrap();
    let create = r#"{"type":"response.create","model":"gpt-fixture","input":[]}"#;
    socket.send(WsMessage::text(create)).await.unwrap();
    let mut sent = Vec::new();
    while let Some(Ok(WsMessage::Text(text))) = tokio::time::timeout(Duration::from_secs(10), socket.next())
        .await
        .unwrap()
    {
        let done = text.as_str().contains("response.completed");
        sent.push(text.as_str().to_owned());
        if done {
            break;
        }
    }
    assert_eq!(sent.len(), 2, "{sent:?}");
    socket
        .send(WsMessage::Close(Some(WsClose {
            code: 1000.into(),
            reason: "bye".into(),
        })))
        .await
        .unwrap();
    drop(socket);

    let log = written_log(&dir).await;
    servers.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
    let section = |name: &str| -> String {
        let start = log
            .find(&format!("=== {name}"))
            .unwrap_or_else(|| panic!("no {name} in\n{log}"));
        let rest = &log[start..];
        let body = &rest[rest.find('\n').unwrap() + 1..];
        body[..body.find("\n=== ").unwrap_or(body.len())].to_owned()
    };
    // Timestamps vary; the events, their order and payloads are Go's.
    let timeline: Vec<(String, String)> = section("WEBSOCKET TIMELINE ===")
        .split("\n\n")
        .filter(|part| !part.trim().is_empty())
        .map(|part| {
            let mut lines = part.lines();
            assert!(lines.next().unwrap().starts_with("Timestamp: "), "{part}");
            let event = lines.next().unwrap().trim_start_matches("Event: ").to_owned();
            (event, lines.collect::<Vec<_>>().join("\n"))
        })
        .collect();
    let mut want = vec![("websocket.request".to_owned(), create.to_owned())];
    want.extend(
        sent.iter()
            .map(|frame| ("websocket.response".to_owned(), frame.clone())),
    );
    want.push((
        "websocket.disconnect".into(),
        "websocket: close 1000 (normal): bye".into(),
    ));
    assert_eq!(timeline, want, "\n{log}");
    // The turn's upstream attempt is captured under the upgrade's log.
    let api_request = section("API REQUEST 1 ===");
    assert!(api_request.contains("Upstream URL: <redacted url>\n"), "{api_request}");
    assert!(
        section("API RESPONSE").contains(&format!("data: {COMPLETED}")),
        "\n{log}"
    );
}

/// gorilla's `ReadMessage` returns the client's close at once. With steering on and the
/// client no longer reading, its close reply cannot be flushed, yet the turn must still
/// be cancelled (Go's reader cancels the request on that error).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_close_cancels_a_stalled_steering_turn() {
    use futures_util::StreamExt;
    let mut servers = Servers::default();
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = dropped.clone();
    let upstream = servers
        .serve(axum::Router::new().fallback(move || {
            let guard = BodyDropped(flag.clone());
            async move {
                let created = axum::body::Bytes::from_static(
                    b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"output\":[]}}\n\n",
                );
                let delta = axum::body::Bytes::from(format!(
                    "data: {{\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"{}\"}}\n\n",
                    "x".repeat(1 << 20)
                ));
                // Far more than the socket buffers hold, then a response that never ends.
                let body = futures_util::stream::iter(
                    std::iter::once(created)
                        .chain(std::iter::repeat_n(delta, 32))
                        .map(Ok::<_, std::io::Error>),
                )
                .chain(futures_util::stream::pending())
                .map(move |chunk| {
                    let _ = &guard;
                    chunk
                });
                (
                    [("content-type", "text/event-stream")],
                    axum::body::Body::from_stream(body),
                )
            }
        }))
        .await;
    let cfg = Config::parse(&format!(
        "upstream:\n  codex:\n    response-steering: true\napi-keys:\n  codex:\n  - name: codex-1\n    base-url: http://{upstream}\n    models:\n      - name: gpt-fixture\n    keys:\n      - api-key: sk-FAKE\n"
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
    let proxy = servers
        .serve(router(Arc::new(crate::testing::runtime(cfg, credentials, executors))))
        .await;
    let mut socket = wreq::Client::new()
        .websocket(format!("ws://{proxy}/v1/responses"))
        .send()
        .await
        .unwrap()
        .into_websocket()
        .await
        .unwrap();
    let create =
        r#"{"type":"response.create","model":"gpt-fixture","input":[{"type":"message","role":"user","content":"hi"}]}"#;
    socket.send(wreq::ws::message::Message::text(create)).await.unwrap();
    let first = tokio::time::timeout(Duration::from_secs(10), socket.next())
        .await
        .expect("the turn streams")
        .expect("a frame")
        .unwrap();
    assert!(matches!(first, wreq::ws::message::Message::Text(_)), "{first:?}");
    // The client stops reading; the server's writes stall behind the full buffers.
    tokio::time::sleep(Duration::from_millis(500)).await;
    socket
        .send(wreq::ws::message::Message::Close(Some(wreq::ws::message::CloseFrame {
            code: 1000.into(),
            reason: "bye".into(),
        })))
        .await
        .unwrap();
    let started = Instant::now();
    while !dropped.load(std::sync::atomic::Ordering::SeqCst) {
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the turn outlived the client's close by 5s"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    drop(socket);
    servers.shutdown().await;
}

/// A Responses WebSocket proxy through the request-log layer, logging into `dir`, whose
/// one Codex key goes to an upstream that answers `status` with `body`.
async fn logging_proxy(servers: &mut Servers, dir: &std::path::Path, status: u16, body: &'static str) -> String {
    let upstream = servers
        .serve(axum::Router::new().fallback(move || async move {
            (
                axum::http::StatusCode::from_u16(status).unwrap(),
                [("content-type", "application/json")],
                body,
            )
        }))
        .await;
    let cfg = Config::parse(&format!(
        "observability: {{logs: {{request-log: true}}}}\napi-keys:\n  codex:\n  - name: codex-1\n    base-url: http://{upstream}\n    models:\n      - name: gpt-fixture\n    keys:\n      - api-key: sk-FAKE\n"))
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
    let management = crate::management::api::Management::with_options(
        rt.clone(),
        Arc::new(crate::store::config_store::ConfigStore::memory_with(
            &ck_core::config::yaml_string(&rt.config().document).unwrap(),
        )),
        crate::management::api::Options {
            log_dir: Some(dir.to_owned()),
            management_password: Some(String::new()),
            ..Default::default()
        },
    );
    servers
        .serve(crate::observability::request_logging::router(&management, router(rt)))
        .await
}

/// A client connected to `proxy` that has sent one `response.create` for `gpt-fixture`.
async fn sent_turn(proxy: &str) -> wreq::ws::WebSocket {
    let mut socket = wreq::Client::new()
        .websocket(format!("ws://{proxy}/v1/responses"))
        .send()
        .await
        .unwrap()
        .into_websocket()
        .await
        .unwrap();
    socket
        .send(wreq::ws::message::Message::text(
            r#"{"type":"response.create","model":"gpt-fixture","input":[]}"#,
        ))
        .await
        .unwrap();
    socket
}

/// Go closes the socket as soon as the connection ends and writes its request log in a
/// deferred step: a slow log delivery must not hold the client's connection open.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_socket_closes_before_the_request_log_is_delivered() {
    use futures_util::StreamExt;
    let mut servers = Servers::default();
    let dir = std::env::temp_dir().join(format!("cooker-ws-gated-log-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let gate = Arc::new(tokio::sync::Notify::new());
    crate::observability::request_logging::HELD_DELIVERIES
        .lock()
        .unwrap()
        .push((dir.clone(), gate.clone()));
    let proxy = logging_proxy(
        &mut servers,
        &dir,
        400,
        r#"{"error":{"message":"bad input","type":"invalid_request_error","code":"invalid_value"}}"#,
    )
    .await;
    let mut socket = sent_turn(&proxy).await;
    // Every frame until the connection ends, while the log delivery is still held. The
    // held delivery never ends on its own, so the bound only absorbs a loaded test host.
    let closed = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match socket.next().await {
                Some(Ok(wreq::ws::message::Message::Close(_))) | Some(Err(_)) | None => break,
                Some(Ok(_)) => {}
            }
        }
    })
    .await;
    let unwritten = !std::fs::read_dir(&dir)
        .unwrap()
        .flatten()
        .any(|e| e.path().extension().is_some_and(|e| e == "log"));
    gate.notify_one();
    drop(socket);
    let log = written_log(&dir).await;
    servers.shutdown().await;
    crate::observability::request_logging::HELD_DELIVERIES
        .lock()
        .unwrap()
        .retain(|(held, _)| *held != dir);
    let _ = std::fs::remove_dir_all(&dir);
    assert!(closed.is_ok(), "the socket stayed open while the request log was held");
    assert!(
        unwritten,
        "the request log was written before its delivery was released"
    );
    assert!(log.contains("=== WEBSOCKET TIMELINE ==="), "\n{log}");
}

/// One Responses WebSocket turn through the request-log layer against an upstream that
/// answers `status` with `body`: the text frames the client saw and the written log.
async fn logged_failure(status: u16, body: &'static str) -> (Vec<String>, String) {
    use futures_util::StreamExt;
    let mut servers = Servers::default();
    let dir = std::env::temp_dir().join(format!("cooker-ws-timeline-error-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let proxy = logging_proxy(&mut servers, &dir, status, body).await;
    let mut socket = sent_turn(&proxy).await;
    let mut frames = Vec::new();
    while let Ok(Some(Ok(message))) = tokio::time::timeout(Duration::from_secs(10), socket.next()).await {
        match message {
            wreq::ws::message::Message::Text(text) => frames.push(text.as_str().to_owned()),
            wreq::ws::message::Message::Close(_) => break,
            _ => {}
        }
    }
    drop(socket);
    let log = written_log(&dir).await;
    servers.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
    (frames, log)
}

/// The `=== WEBSOCKET TIMELINE ===` parts as (event, payload), timestamps dropped.
fn timeline_events(log: &str) -> Vec<(String, String)> {
    let start = log
        .find("=== WEBSOCKET TIMELINE ===")
        .unwrap_or_else(|| panic!("no timeline in\n{log}"));
    let rest = &log[start..];
    let body = &rest[rest.find('\n').unwrap() + 1..];
    let body = &body[..body.find("\n=== ").unwrap_or(body.len())];
    body.split("\n\n")
        .filter(|part| !part.trim().is_empty())
        .map(|part| {
            let mut lines = part.lines();
            assert!(lines.next().unwrap().starts_with("Timestamp: "), "{part}");
            let event = lines.next().unwrap().trim_start_matches("Event: ").to_owned();
            (event, lines.collect::<Vec<_>>().join("\n"))
        })
        .collect()
}

/// Go's terminal-error timeline (`writeResponsesWebsocketTerminalError`): an exposed
/// request fault is the `websocket.response` part the client received; a hidden failure
/// (quota) keeps the upstream reason as a `websocket.disconnect` part although the client
/// only sees the close. Either way the connection then ends with gorilla's "close sent".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_log_records_terminal_errors_like_go() {
    const REQUEST: &str = r#"{"type":"response.create","model":"gpt-fixture","input":[]}"#;
    const CLOSE_SENT: &str = "websocket: close sent";

    let (frames, log) = logged_failure(
        400,
        r#"{"error":{"message":"bad input","type":"invalid_request_error","code":"invalid_value"}}"#,
    )
    .await;
    assert_eq!(frames.len(), 1, "the exposed error event: {frames:?}");
    assert_eq!(
        timeline_events(&log),
        [
            ("websocket.request".to_owned(), REQUEST.to_owned()),
            ("websocket.response".to_owned(), frames[0].clone()),
            ("websocket.disconnect".to_owned(), CLOSE_SENT.to_owned()),
        ],
        "\n{log}"
    );

    const QUOTA: &str = r#"{"error":{"type":"usage_limit_reached","message":"quota exhausted"}}"#;
    let (frames, log) = logged_failure(429, QUOTA).await;
    assert!(frames.is_empty(), "a quota failure closes silently: {frames:?}");
    let events = timeline_events(&log);
    assert_eq!(events.len(), 3, "\n{log}");
    assert_eq!(events[0], ("websocket.request".to_owned(), REQUEST.to_owned()));
    assert_eq!(events[1].0, "websocket.disconnect", "\n{log}");
    assert!(
        events[1].1.contains("usage_limit_reached"),
        "the upstream reason: {}",
        events[1].1
    );
    assert_eq!(events[2], ("websocket.disconnect".to_owned(), CLOSE_SENT.to_owned()));
}

/// Go `TestResponsesInterruptDiagnosticsAreSanitized` and the outcome check of
/// `TestResponsesLateInterruptDoesNotPoisonNextCreate`: every `response.interrupt` adds an
/// `interrupt` and an `interrupt.outcome` part with the response ID hashed (first 8 bytes
/// of its SHA-256) and nothing else from the frame, not a request part; the rejection's
/// error event is not a response part.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupt_diagnostics_are_sanitized() {
    use futures_util::StreamExt;
    use wreq::ws::message::{CloseFrame as WsClose, Message as WsMessage};
    let mut servers = Servers::default();
    const CREATED: &str = r#"{"type":"response.created","response":{"id":"r-http","output":[]}}"#;
    // Releases the bootstrap buffer, which holds `response.created`. Event lines close
    // each frame, as Codex sends them.
    const ITEM: &str = r#"{"type":"response.output_item.done","output_index":0,"item":{"type":"message","role":"assistant","id":"msg_1","content":[{"type":"output_text","text":"partial"}]}}"#;
    // An HTTP turn that stays open until the proxy cancels it.
    let upstream = servers
        .serve(axum::Router::new().fallback(|| async {
            let events = futures_util::stream::once(async {
                Ok::<_, std::io::Error>(format!(
                    "event: response.created\ndata: {CREATED}\n\nevent: response.output_item.done\ndata: {ITEM}\n\n"
                ))
            })
            .chain(futures_util::stream::pending());
            (
                [("content-type", "text/event-stream")],
                axum::body::Body::from_stream(events),
            )
        }))
        .await;
    let dir = std::env::temp_dir().join(format!("cooker-ws-interrupt-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let cfg = Config::parse(&format!(
        "observability: {{logs: {{request-log: true}}}}\napi-keys:\n  codex:\n  - name: codex-1\n    base-url: http://{upstream}\n    models:\n      - name: gpt-fixture\n    keys:\n      - api-key: sk-FAKE\n"))
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
    let management = crate::management::api::Management::with_options(
        rt.clone(),
        Arc::new(crate::store::config_store::ConfigStore::memory_with(
            &ck_core::config::yaml_string(&rt.config().document).unwrap(),
        )),
        crate::management::api::Options {
            log_dir: Some(dir.clone()),
            management_password: Some(String::new()),
            ..Default::default()
        },
    );
    let proxy = servers
        .serve(crate::observability::request_logging::router(&management, router(rt)))
        .await;
    let mut socket = wreq::Client::new()
        .websocket(format!("ws://{proxy}/v1/responses"))
        .send()
        .await
        .unwrap()
        .into_websocket()
        .await
        .unwrap();
    let secret = r#"{"type":"response.interrupt","response_id":"private-id-token","mode":"secret-mode","authorization":"Bearer secret-credential","input":"private-conversation"}"#;
    socket.send(WsMessage::text(secret)).await.unwrap();
    let rejected = read_text(&mut socket).await;
    assert_eq!(
        gjson::get(&rejected, "error.message").str(),
        "no active upstream websocket for response.interrupt",
        "{rejected}"
    );
    let create = r#"{"type":"response.create","model":"gpt-fixture","input":[]}"#;
    socket.send(WsMessage::text(create)).await.unwrap();
    let created = read_text(&mut socket).await;
    assert_eq!(gjson::get(&created, "type").str(), "response.created", "{created}");
    assert_eq!(read_text(&mut socket).await, ITEM);
    socket
        .send(WsMessage::text(
            r#"{"type":"response.interrupt","response_id":"r-http"}"#,
        ))
        .await
        .unwrap();
    let incomplete = read_text(&mut socket).await;
    assert_eq!(
        gjson::get(&incomplete, "type").str(),
        "response.incomplete",
        "{incomplete}"
    );
    socket
        .send(WsMessage::Close(Some(WsClose {
            code: 1000.into(),
            reason: "bye".into(),
        })))
        .await
        .unwrap();
    drop(socket);

    let log = written_log(&dir).await;
    servers.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
    let part = |event: &str, payload: &str| (event.to_owned(), payload.to_owned());
    let hashed = |hash: &str, outcome: &str| {
        let outcome = if outcome.is_empty() {
            String::new()
        } else {
            format!(r#","outcome":"{outcome}""#)
        };
        format!(r#"{{"type":"response.interrupt","response_id_hash":"{hash}"{outcome}}}"#)
    };
    // sha256("private-id-token") and sha256("r-http"), first 8 bytes.
    let (private, http) = ("6e621ae080fb54bf", "31a0066549f97718");
    let mut events = timeline_events(&log);
    assert_eq!(events.len(), 9, "\n{log}");
    // The local outcome and the turn's incomplete event are written concurrently.
    events[6..8].sort();
    assert_eq!(
        events,
        [
            part("websocket.interrupt", &hashed(private, "")),
            part("websocket.interrupt.outcome", &hashed(private, "rejected")),
            part("websocket.request", create),
            part("websocket.response", &created),
            part("websocket.response", ITEM),
            part("websocket.interrupt", &hashed(http, "")),
            part("websocket.interrupt.outcome", &hashed(http, "local_http")),
            part("websocket.response", &incomplete),
            part("websocket.disconnect", "websocket: close 1000 (normal): bye"),
        ],
        "\n{log}"
    );
    for secret in [
        "private-id-token",
        "secret-mode",
        "secret-credential",
        "private-conversation",
    ] {
        assert!(!log.contains(secret), "the log leaked {secret}:\n{log}");
    }
}

async fn read_text(socket: &mut wreq::ws::WebSocket) -> String {
    use futures_util::StreamExt;
    match tokio::time::timeout(Duration::from_secs(10), socket.next()).await {
        Ok(Some(Ok(wreq::ws::message::Message::Text(text)))) => text.as_str().to_owned(),
        other => panic!("no text frame: {other:?}"),
    }
}

/// Turns that are not interrupted keep master's upstream bytes, here on the Claude
/// executor: a first turn and a continuation the handler merges into a full transcript.
/// The pinned bodies were recorded on master before the downstream reader handled
/// `response.interrupt` and before `response.incomplete` ended a turn; `max_tokens` is
/// the model's registered output limit since a881c036 and 0d18d3b3 (#75).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uninterrupted_claude_turns_send_masters_bytes() {
    use futures_util::StreamExt;
    use wreq::ws::message::Message as WsMessage;
    let mut servers = Servers::default();
    const FIRST: &str = r#"{"model":"claude-opus-5-5","max_tokens":128000,"messages":[{"role":"user","content":[{"type":"text","text":"hi","cache_control":{"type":"ephemeral"}}]}],"metadata":{"user_id":"2b35ba29f7cec38e9220a584fafaa8f9eb2a8d01350e9d1c3653ae728471c5bb"},"stream":true,"system":[{"type":"text","text":"be brief","cache_control":{"type":"ephemeral"}}]}"#;
    const SECOND: &str = r#"{"model":"claude-opus-5-5","max_tokens":128000,"messages":[{"role":"user","content":"hi"},{"role":"assistant","content":"hello"},{"role":"user","content":[{"type":"text","text":"again","cache_control":{"type":"ephemeral"}}]}],"metadata":{"user_id":"2b35ba29f7cec38e9220a584fafaa8f9eb2a8d01350e9d1c3653ae728471c5bb"},"stream":true,"system":[{"type":"text","text":"be brief","cache_control":{"type":"ephemeral"}}]}"#;
    let seen: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
    let upstream = {
        let seen = seen.clone();
        servers.serve(axum::Router::new().fallback(move |body: axum::body::Bytes| {
            let seen = seen.clone();
            async move {
                seen.lock().unwrap().push(String::from_utf8_lossy(&body).into_owned());
                let events = [
                    r#"{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"claude-opus-5-5","content":[],"stop_reason":null,"usage":{"input_tokens":3,"output_tokens":0}}}"#,
                    r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#,
                    r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hello"}}"#,
                    r#"{"type":"content_block_stop","index":0}"#,
                    r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}"#,
                    r#"{"type":"message_stop"}"#,
                ];
                let sse: String = events
                    .iter()
                    .map(|e| format!("event: {}\ndata: {e}\n\n", gjson::get(e, "type").str()))
                    .collect();
                ([("content-type", "text/event-stream")], sse)
            }
        }))
        .await
    };
    let dir = std::env::temp_dir().join(format!("cooker-ws-claude-{}", uuid::Uuid::new_v4()));
    let cfg = Config::parse(&format!(
        "api-keys:\n  claude:\n  - name: claude-1\n    base-url: http://{upstream}\n    models:\n      - name: claude-opus-5-5\n    keys:\n      - api-key: sk-FAKE\n"))
    .unwrap();
    let credentials = ck_core::config::credentials::from_config(&cfg);
    let executors = Executors {
        claude: ck_exec::claude::ClaudeExecutor::new(format!("http://{upstream}")).unwrap(),
        codex: ck_exec::codex::CodexExecutor::new().unwrap(),
        devices: Default::default(),
        openai: Default::default(),
        google: Default::default(),
    };
    let rt = Arc::new(crate::testing::runtime(cfg, credentials, executors));
    let proxy = servers.serve(router(rt)).await;
    let mut socket = wreq::Client::new()
        .websocket(format!("ws://{proxy}/v1/responses"))
        .send()
        .await
        .unwrap()
        .into_websocket()
        .await
        .unwrap();
    let turns = [
        r#"{"type":"response.create","model":"claude-opus-5-5","instructions":"be brief","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}"#,
        r#"{"type":"response.create","model":"claude-opus-5-5","previous_response_id":"{{id}}","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"again"}]}]}"#,
    ];
    let mut id = String::new();
    for turn in turns {
        socket.send(WsMessage::text(turn.replace("{{id}}", &id))).await.unwrap();
        loop {
            let frame = match tokio::time::timeout(Duration::from_secs(10), socket.next()).await {
                Ok(Some(Ok(WsMessage::Text(text)))) => text.as_str().to_owned(),
                other => panic!("no text frame: {other:?}"),
            };
            if gjson::get(&frame, "type").str() == "response.completed" {
                id = gjson::get(&frame, "response.id").str().to_owned();
                break;
            }
        }
    }
    drop(socket);
    servers.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen, [FIRST, SECOND]);
}

/// The request log once it is complete: the writer streams into the `.log` file, so the
/// file is read until its content stops changing.
async fn written_log(dir: &std::path::Path) -> String {
    let started = Instant::now();
    let mut last: Option<String> = None;
    loop {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "no complete request log written"
        );
        let found = std::fs::read_dir(dir)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .find(|p| p.extension().is_some_and(|e| e == "log"));
        let current = found.map(|path| std::fs::read_to_string(path).unwrap());
        match (&last, &current) {
            (Some(previous), Some(now)) if previous == now && !now.is_empty() => return now.clone(),
            _ => last = current,
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A continuation pinned to its credential whose second turn the upstream fails with
/// `failure` (after `shown`, events the client sees first). The client frames of that
/// turn, the close as `close <code> <reason>` last.
async fn pinned_continuation_failure(shown: &'static [&'static str], failure: &'static str) -> Vec<String> {
    use futures_util::StreamExt;
    use wreq::ws::message::Message as WsMessage;
    let mut servers = Servers::default();
    const FIRST: [&str; 2] = [
        r#"{"type":"response.created","response":{"id":"r1","status":"in_progress","output":[]}}"#,
        r#"{"type":"response.completed","response":{"id":"r1","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}"#,
    ];
    let upstream = servers
        .serve(axum::Router::new().fallback(move |ws: WebSocketUpgrade| async move {
            ws.on_upgrade(move |mut socket: AxSocket| async move {
                let mut turn = 0;
                while let Some(Ok(message)) = socket.recv().await {
                    if !matches!(message, AxMessage::Text(_)) {
                        continue;
                    }
                    turn += 1;
                    let events: Vec<&str> = if turn == 1 {
                        FIRST.to_vec()
                    } else {
                        shown.iter().copied().chain([failure]).collect()
                    };
                    for event in events {
                        let _ = socket.send(AxMessage::Text(event.into())).await;
                    }
                }
            })
        }))
        .await;
    let dir = std::env::temp_dir().join(format!("cooker-ws-pinned-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let cfg = Config::parse(&format!(
        "api-keys:\n  codex:\n  - name: codex-1\n    base-url: http://{upstream}\n    models:\n      - name: gpt-fixture\n    keys:\n      - api-key: sk-FAKE\n        websockets: true\n"))
    .unwrap();
    let credentials = ck_core::config::credentials::from_config(&cfg);
    let executors = Executors {
        claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
        codex: ck_exec::codex::CodexExecutor::new().unwrap(),
        devices: Default::default(),
        openai: Default::default(),
        google: Default::default(),
    };
    let proxy = servers
        .serve(router(Arc::new(crate::testing::runtime(cfg, credentials, executors))))
        .await;
    let mut socket = wreq::Client::new()
        .websocket(format!("ws://{proxy}/v1/responses"))
        .send()
        .await
        .unwrap()
        .into_websocket()
        .await
        .unwrap();
    let read = async |socket: &mut wreq::ws::WebSocket| {
        let mut frames = Vec::new();
        loop {
            match tokio::time::timeout(Duration::from_secs(10), socket.next())
                .await
                .unwrap()
            {
                Some(Ok(WsMessage::Text(text))) => {
                    let done = text.as_str().contains("response.completed");
                    frames.push(text.as_str().to_owned());
                    if done {
                        break;
                    }
                }
                Some(Ok(WsMessage::Close(frame))) => {
                    let (code, reason) = frame.map_or((1005, String::new()), |f| {
                        (u16::from(f.code), f.reason.as_str().to_owned())
                    });
                    frames.push(format!("close {code} {reason}"));
                    break;
                }
                Some(Ok(_)) => {}
                Some(Err(_)) | None => {
                    frames.push("close 1006".into());
                    break;
                }
            }
        }
        frames
    };
    let input = r#""input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]"#;
    socket
        .send(WsMessage::text(format!(
            r#"{{"type":"response.create","model":"gpt-fixture",{input}}}"#
        )))
        .await
        .unwrap();
    let first = read(&mut socket).await;
    assert_eq!(first.len(), 2, "{first:?}");
    socket
        .send(WsMessage::text(format!(
            r#"{{"type":"response.create","model":"gpt-fixture","previous_response_id":"r1",{input}}}"#
        )))
        .await
        .unwrap();
    let frames = read(&mut socket).await;
    drop(socket);
    servers.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
    frames
}

/// `replayPinnedAuthFailure`: a pinned continuation's 401 before anything was shown also
/// lost the session's socket, yet the client is told to replay over HTTP (1012).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pinned_continuation_bootstrap_auth_failure_asks_for_replay() {
    let frames = pinned_continuation_failure(
        &[],
        r#"{"type":"error","status":401,"error":{"type":"invalid_request_error","message":"bad token"}}"#,
    )
    .await;
    assert_eq!(frames, ["close 1012 upstream requires HTTP replay"]);
}

/// The same after the response started: the shown event, then the replay close.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pinned_continuation_later_quota_failure_asks_for_replay() {
    const DELTA: &str = r#"{"type":"response.output_text.delta","output_index":0,"delta":"hello"}"#;
    let frames = pinned_continuation_failure(
        &[DELTA],
        r#"{"type":"error","status":429,"error":{"type":"usage_limit_reached","message":"limit","resets_in_seconds":60}}"#,
    )
    .await;
    assert_eq!(frames, [DELTA, "close 1012 upstream requires HTTP replay"]);
}

/// Go 5dbd92a0 (TestCodexWebsocketsExecutor_UnbufferedUpstreamErrorIsDeliveredBeforeDisconnectNotify
/// and the issue 6503 conductor tests): a usage-limit error after the response started
/// loses the session's upstream socket, which closes the client, yet the error's
/// cooldown is still recorded. Here the error leaves the turn's stream in the same poll
/// that reports the lost socket, so nothing can cancel the turn in between.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_upstream_error_that_loses_the_socket_still_cools_the_credential() {
    use futures_util::StreamExt;
    use wreq::ws::message::Message as WsMessage;
    const CREATED: &str = r#"{"type":"response.created","response":{"id":"r1","status":"in_progress","output":[]}}"#;
    const DELTA: &str = r#"{"type":"response.output_text.delta","output_index":0,"delta":"hello"}"#;
    for failure in [
        r#"{"type":"error","status":429,"error":{"type":"usage_limit_reached","message":"The usage limit has been reached","resets_in_seconds":600}}"#,
        r#"{"type":"response.failed","response":{"error":{"type":"usage_limit_reached","status_code":429,"message":"The usage limit has been reached","resets_in_seconds":600}}}"#,
    ] {
        let mut servers = Servers::default();
        let upstream = servers
            .serve(axum::Router::new().fallback(move |ws: WebSocketUpgrade| async move {
                ws.on_upgrade(move |mut socket: AxSocket| async move {
                    while let Some(Ok(message)) = socket.recv().await {
                        if !matches!(message, AxMessage::Text(_)) {
                            continue;
                        }
                        for event in [CREATED, DELTA, failure] {
                            let _ = socket.send(AxMessage::Text(event.into())).await;
                        }
                    }
                })
            }))
            .await;
        let dir = std::env::temp_dir().join(format!("cooker-ws-6503-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let cfg = Config::parse(&format!(
            "api-keys:\n  codex:\n  - name: codex-1\n    base-url: http://{upstream}\n    models:\n      - name: gpt-fixture\n    keys:\n      - api-key: sk-FAKE\n        websockets: true\n"))
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
        let proxy = servers.serve(router(rt.clone())).await;
        let mut socket = wreq::Client::new()
            .websocket(format!("ws://{proxy}/v1/responses"))
            .send()
            .await
            .unwrap()
            .into_websocket()
            .await
            .unwrap();
        socket
            .send(WsMessage::text(
                r#"{"type":"response.create","model":"gpt-fixture","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}"#,
            ))
            .await
            .unwrap();
        let mut frames = Vec::new();
        while let Ok(Some(Ok(frame))) = tokio::time::timeout(Duration::from_secs(10), socket.next()).await {
            match frame {
                WsMessage::Text(text) => frames.push(text.as_str().to_owned()),
                WsMessage::Close(_) => break,
                _ => {}
            }
        }
        // Go's disconnect notifier closes the client ahead of the turn's own error.
        assert_eq!(frames, [CREATED, DELTA], "{failure}");
        let credential = rt
            .store()
            .snapshot()
            .into_iter()
            .find(|c| c.provider == "codex")
            .unwrap();
        assert!(rt.store().blocked(&credential, "gpt-fixture"), "{failure}: not cooling");
        drop(socket);
        servers.shutdown().await;
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// An uncommitted rejection or disconnect switches credentials without closing the
/// downstream socket or leaking the rejected attempt's handshake frames.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn early_upstream_failures_switch_before_downstream_output() {
    use axum::response::IntoResponse;
    use futures_util::StreamExt;
    use wreq::ws::message::Message as WsMessage;

    const CREATED: &str = r#"{"type":"response.created","response":{"id":"rejected","output":[]}}"#;
    const FAILED: &str =
        r#"{"type":"response.failed","response":{"error":{"type":"server_error","message":"upstream unavailable"}}}"#;
    const ERROR: &str =
        r#"{"type":"error","status":429,"error":{"type":"rate_limit_error","message":"try another credential"}}"#;
    const HEALTHY: &str = r#"{"type":"response.created","response":{"id":"healthy","output":[]}}"#;
    const COMPLETED: &str = r#"{"type":"response.completed","response":{"id":"healthy","output":[]}}"#;

    for (websocket, fallback_websocket, buffering, failure) in [
        (true, true, true, "close"),
        (true, true, true, "failed"),
        (true, true, false, "error"),
        (false, false, true, "close"),
        (false, false, true, "failed"),
        (true, false, true, "close"),
    ] {
        let mut servers = Servers::default();
        let attempts = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = attempts.clone();
        let upstream = servers.serve(axum::Router::new().fallback(
            move |headers: axum::http::HeaderMap,
                  ws: Result<WebSocketUpgrade, axum::extract::ws::rejection::WebSocketUpgradeRejection>| {
                let attempts = captured.clone();
                async move {
                    let key = headers["authorization"]
                        .to_str()
                        .unwrap()
                        .trim_start_matches("Bearer ")
                        .to_owned();
                    let events = if key == "bad" {
                        match failure {
                            "close" => vec![CREATED],
                            "failed" => vec![CREATED, FAILED],
                            _ => vec![ERROR],
                        }
                    } else {
                        vec![HEALTHY, COMPLETED]
                    };
                    if let Ok(ws) = ws {
                        return ws
                            .on_upgrade(move |mut socket: AxSocket| async move {
                                while let Some(Ok(AxMessage::Text(_))) = socket.recv().await {
                                    attempts.lock().unwrap().push(key.clone());
                                    for event in &events {
                                        if socket.send(AxMessage::Text((*event).into())).await.is_err() {
                                            return;
                                        }
                                    }
                                    if key == "bad" && failure == "close" {
                                        let _ = socket.send(AxMessage::Close(None)).await;
                                        return;
                                    }
                                }
                            })
                            .into_response();
                    }
                    attempts.lock().unwrap().push(key);
                    let sse = events
                        .iter()
                        .map(|event| format!("data: {event}\n\n"))
                        .collect::<String>();
                    ([("content-type", "text/event-stream")], sse).into_response()
                }
            },
        ))
        .await;
        let entry = |key: &str, priority: u8, websocket: bool| {
            format!(
                "    - base-url: http://{upstream}\n      priority: {priority}\n      models:\n        - name: gpt-fixture\n      keys:\n        - api-key: {key}\n          websockets: {websocket}\n"
            )
        };
        let cfg = Config::parse(&format!(
            "upstream:\n  codex:\n    stream-bootstrap-buffering: {buffering}\napi-keys:\n  codex:\n{}{}",
            entry("bad", 2, websocket),
            entry("good", 1, fallback_websocket),
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
        let proxy = servers.serve(router(rt)).await;
        let mut socket = wreq::Client::new()
            .websocket(format!("ws://{proxy}/v1/responses"))
            .send()
            .await
            .unwrap()
            .into_websocket()
            .await
            .unwrap();
        for turn in 0..2 {
            socket
                .send(WsMessage::text(
                    r#"{"type":"response.create","model":"gpt-fixture","input":[]}"#,
                ))
                .await
                .unwrap();
            for expected in ["response.created", "response.completed"] {
                let frame = tokio::time::timeout(Duration::from_secs(5), socket.next())
                    .await
                    .expect("failover stalled")
                    .expect("downstream closed before failover")
                    .unwrap();
                let WsMessage::Text(text) = frame else {
                    panic!("upstream ws={websocket}, fallback ws={fallback_websocket}, {failure}: {frame:?}");
                };
                let payload: Value = serde_json::from_str(text.as_str()).unwrap();
                assert_eq!(payload["type"], expected, "{failure}: {payload}");
                assert_eq!(
                    payload["response"]["id"], "healthy",
                    "{failure}: rejected attempt escaped bootstrap"
                );
            }
            if turn == 0 {
                assert_eq!(*attempts.lock().unwrap(), ["bad", "good"], "{failure}");
            }
        }
        drop(socket);
        servers.shutdown().await;
    }
}

/// Go 1e37b52b: removing a Codex credential from the store closes the upstream WebSocket
/// its session holds and no other; a sibling credential's connection keeps its socket for
/// the next turn.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn removing_a_credential_closes_only_its_upstream_sockets() {
    removal_closes_only_its_sockets("codex").await;
}

async fn removal_closes_only_its_sockets(section: &str) {
    use futures_util::StreamExt;
    use wreq::ws::message::Message as WsMessage;
    let mut servers = Servers::default();
    type Log = Arc<std::sync::Mutex<Vec<String>>>;
    let (dials, ended): (Log, Log) = Default::default();
    let upstream = {
        let (dials, ended) = (dials.clone(), ended.clone());
        servers.serve(axum::Router::new().fallback(move |headers: axum::http::HeaderMap, ws: WebSocketUpgrade| {
            let (dials, ended) = (dials.clone(), ended.clone());
            async move {
                let key = headers
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default()
                    .trim_start_matches("Bearer ")
                    .to_owned();
                dials.lock().unwrap().push(key.clone());
                ws.on_upgrade(move |mut socket| async move {
                    while let Some(Ok(message)) = socket.recv().await {
                        if !matches!(message, AxMessage::Text(_)) {
                            continue;
                        }
                        let completed = format!(
                            r#"{{"type":"response.completed","response":{{"id":"resp-{key}","output":[],"usage":{{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}}}}"#
                        );
                        if socket.send(AxMessage::Text(completed.into())).await.is_err() {
                            break;
                        }
                    }
                    ended.lock().unwrap().push(key);
                })
            }
        }))
        .await
    };
    let dir = std::env::temp_dir().join(format!("cooker-ws-remove-{}", uuid::Uuid::new_v4()));
    let entry = |key: &str| {
        format!(
            "    - base-url: http://{upstream}\n      models:\n        - name: gpt-{key}\n      keys:\n        - api-key: {key}\n          websockets: true\n"
        )
    };
    let cfg = Config::parse(&format!("api-keys:\n  {section}:\n{}{}", entry("keep"), entry("drop"))).unwrap();
    let credentials = ck_core::config::credentials::from_config(&cfg);
    let keep = credentials
        .iter()
        .find(|c| c.attributes.get("api_key").map(String::as_str) == Some("keep"))
        .cloned()
        .expect("the kept key");
    assert_eq!(credentials.len(), 2, "{section}");
    let executors = Executors {
        claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
        codex: ck_exec::codex::CodexExecutor::new().unwrap(),
        devices: Default::default(),
        openai: Default::default(),
        google: Default::default(),
    };
    let rt = Arc::new(crate::testing::runtime(cfg, credentials, executors));
    let proxy = servers.serve(router(rt.clone())).await;
    let connect = || async {
        wreq::Client::new()
            .websocket(format!("ws://{proxy}/v1/responses"))
            .send()
            .await
            .unwrap()
            .into_websocket()
            .await
            .unwrap()
    };
    async fn turn(socket: &mut wreq::ws::WebSocket, key: &str, previous: &str) {
        let create = format!(
            r#"{{"type":"response.create","model":"gpt-{key}","previous_response_id":"{previous}","input":[{{"type":"message","role":"user","content":"hi"}}]}}"#
        );
        socket.send(WsMessage::text(create)).await.unwrap();
        loop {
            let frame = match tokio::time::timeout(Duration::from_secs(10), socket.next()).await {
                Ok(Some(Ok(WsMessage::Text(text)))) => text.as_str().to_owned(),
                other => panic!("{key}: no text frame: {other:?}"),
            };
            match gjson::get(&frame, "type").str() {
                "response.completed" => return,
                "error" => panic!("{key}: {frame}"),
                _ => {}
            }
        }
    }
    let (mut kept, mut dropped) = (connect().await, connect().await);
    turn(&mut kept, "keep", "").await;
    turn(&mut dropped, "drop", "").await;
    assert_eq!(*dials.lock().unwrap(), ["keep", "drop"], "{section}");

    rt.store().reconcile(vec![keep]);
    let started = Instant::now();
    while ended.lock().unwrap().is_empty() && started.elapsed() < Duration::from_secs(5) {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        *ended.lock().unwrap(),
        ["drop"],
        "{section}: only the removed key's socket closed"
    );

    turn(&mut kept, "keep", "resp-keep").await;
    assert_eq!(
        *dials.lock().unwrap(),
        ["keep", "drop"],
        "the kept connection reused its socket"
    );
    assert_eq!(*ended.lock().unwrap(), ["drop"]);
    drop((kept, dropped));
    servers.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// Until the server runs xAI turns through `session::Turns`, an xAI credential with
/// `websockets` serves Responses WebSocket turns over HTTP: a continuation replays the
/// connection's own history upstream instead of closing the client for a replay, and a
/// warmup (`generate: false`) on the pinned connection is answered locally.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_xai_websockets_credential_continues_over_http() {
    use futures_util::StreamExt;
    use wreq::ws::message::Message as WsMessage;
    let mut servers = Servers::default();
    let uploads: Arc<std::sync::Mutex<Vec<Value>>> = Arc::default();
    let upstream = {
        let uploads = uploads.clone();
        servers
            .serve(axum::Router::new().fallback(move |body: axum::body::Bytes| {
                let uploads = uploads.clone();
                async move {
                    let n = {
                        let mut uploads = uploads.lock().unwrap();
                        uploads.push(serde_json::from_slice(&body).unwrap());
                        uploads.len()
                    };
                    let message = format!(
                        r#"{{"type":"message","id":"msg_{n}","status":"completed","role":"assistant","content":[{{"type":"output_text","text":"answer {n}","annotations":[]}}]}}"#
                    );
                    let response = |status: &str, output: &str| {
                        format!(
                            r#"{{"id":"resp_{n}","object":"response","created_at":1700000000,"status":"{status}","model":"grok-test","output":{output},"usage":{{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}}"#
                        )
                    };
                    let sse = format!(
                        "event: response.created\ndata: {{\"type\":\"response.created\",\"response\":{}}}\n\nevent: response.output_item.done\ndata: {{\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{message}}}\n\nevent: response.completed\ndata: {{\"type\":\"response.completed\",\"response\":{}}}\n\n",
                        response("in_progress", "[]"),
                        response("completed", &format!("[{message}]")),
                    );
                    ([("content-type", "text/event-stream")], sse)
                }
            }))
            .await
    };
    let cfg = Config::parse(&format!(
        "api-keys:\n  xai:\n    - base-url: http://{upstream}/v1\n      models:\n        - name: grok-test\n      keys:\n        - api-key: xai-key\n          websockets: true\n"
    ))
    .unwrap();
    let credentials = ck_core::config::credentials::from_config(&cfg);
    let executors = Executors {
        claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
        codex: Default::default(),
        devices: Default::default(),
        openai: Default::default(),
        google: Default::default(),
    };
    let rt = Arc::new(crate::testing::runtime(cfg, credentials, executors));
    let proxy = servers.serve(router(rt)).await;
    let mut socket = wreq::Client::new()
        .websocket(format!("ws://{proxy}/v1/responses"))
        .send()
        .await
        .unwrap()
        .into_websocket()
        .await
        .unwrap();
    let mut previous = String::new();
    for text in ["first", "second", "warmup"] {
        let mut create = serde_json::json!({
            "type": "response.create",
            "model": "grok-test",
            "previous_response_id": previous,
            "input": [{"type":"message","role":"user","content":text}],
        });
        if text == "warmup" {
            create["generate"] = false.into();
            create["previous_response_id"] = "".into();
        }
        socket.send(WsMessage::text(create.to_string())).await.unwrap();
        previous = loop {
            let frame = match tokio::time::timeout(Duration::from_secs(10), socket.next()).await {
                Ok(Some(Ok(WsMessage::Text(frame)))) => frame.as_str().to_owned(),
                other => panic!("{text}: no text frame: {other:?}"),
            };
            match gjson::get(&frame, "type").str() {
                "response.completed" => break gjson::get(&frame, "response.id").str().to_owned(),
                "error" => panic!("{text}: {frame}"),
                _ => {}
            }
        };
    }
    let uploads = uploads.lock().unwrap().clone();
    assert_eq!(
        uploads.len(),
        2,
        "both turns went over HTTP and the warmup stayed local"
    );
    let replayed = uploads[1]["input"].to_string();
    assert!(
        replayed.contains("first") && replayed.contains("answer 1") && replayed.contains("second"),
        "{replayed}"
    );
    drop(socket);
    servers.shutdown().await;
}
