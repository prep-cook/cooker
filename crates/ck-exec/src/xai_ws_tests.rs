//! The xAI upstream WebSocket's pure rules. Turns against a loopback xAI are in
//! `tests/exec_xai/socket.rs`.

use super::*;

use ck_core::config::Config;
use serde_json::Value;

fn credential(attrs: Value) -> Credential {
    let mut metadata = serde_json::Map::new();
    metadata.insert("type".into(), "xai".into());
    let mut c = Credential::from_file(
        std::path::Path::new("/fixture"),
        std::path::Path::new("/fixture/xai.json"),
        metadata,
    )
    .unwrap();
    for (k, v) in attrs.as_object().unwrap() {
        c.attributes.insert(k.clone(), v.as_str().unwrap().to_owned());
    }
    c
}

#[test]
fn websockets_flag_follows_go() {
    let mut c = credential(serde_json::json!({}));
    assert!(!websockets_enabled(&c));
    c.metadata.insert("websockets".into(), Value::Bool(true));
    assert!(websockets_enabled(&c));
    c.attributes.insert("websockets".into(), "false".into());
    assert!(!websockets_enabled(&c), "a parsable attribute wins");
    c.attributes.insert("websockets".into(), "maybe".into());
    assert!(websockets_enabled(&c), "an unparsable attribute falls back to metadata");
    c.metadata.insert("websockets".into(), Value::String(" TRUE ".into()));
    assert!(websockets_enabled(&c));
    c.metadata.insert("websockets".into(), Value::from(1));
    assert!(!websockets_enabled(&c), "numbers are not bools");
}

/// TestXAIWebsocketsEnabledForConfigAPIKey, directly and through the config synthesizer.
#[test]
fn websockets_enabled_for_config_api_key() {
    assert!(websockets_enabled(&credential(
        serde_json::json!({"api_key": "xai-key", "websockets": "true"})
    )));
    let cfg = Config::parse("api-keys:\n  xai:\n    - base-url: http://127.0.0.1:9/v1\n      keys:\n        - api-key: xai-key\n          websockets: true\n").unwrap();
    let synthesized = ck_core::config::credentials::from_config(&cfg);
    let xai = synthesized
        .iter()
        .find(|c| c.provider == "xai")
        .expect("xai credential");
    assert!(websockets_enabled(xai));
}

/// `buildXAIResponsesWebsocketURL` answers, recorded from Go v8.0.35 with the inputs of `url.Parse` and
/// `URL.String()` (tests/reference/go_url).
#[test]
fn ws_url_follows_go() {
    let cases: Value = serde_json::from_str(include_str!("../tests/fixtures/go_url_string.json")).unwrap();
    for case in cases.as_array().unwrap() {
        let input = case["in"].as_str().unwrap();
        let want = &case["xai_ws"];
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
                assert_eq!((e.status, e.scope), (0, FailureScope::Model), "{input}");
            }
        }
    }
}

/// Go sets no read deadline on xAI sockets; the shared reader's must outlast any session.
#[test]
fn xai_sockets_have_no_read_deadline() {
    assert!(Ws::default().pool.idle >= Duration::from_secs(365 * 24 * 3600));
}

/// A socket failure's status and text: the 413 of a close 1009 keeps its scope and gets
/// the fixed body; any other failure is a transport-scope 500 under xAI's name.
#[test]
fn pool_failures_read_as_xai_failures() {
    let too_big = pool_error(ExecError::local(
        413,
        FailureScope::Request,
        r#"{"error":{"message":"frame too large","type":"invalid_request_error","code":"message_too_big"}}"#,
    ));
    assert_eq!((too_big.status, too_big.scope), (413, FailureScope::Request));
    assert_eq!(
        String::from_utf8_lossy(&too_big.body),
        r#"{"error":{"message":"upstream websocket message too big","type":"invalid_request_error","code":"message_too_big"}}"#
    );
    let lost = pool_error(ExecError::local(
        502,
        FailureScope::Transport,
        "codex websockets executor: write failed",
    ));
    assert_eq!((lost.status, lost.scope), (500, FailureScope::Transport));
    assert_eq!(
        String::from_utf8_lossy(&lost.body),
        "xai websockets executor: write failed"
    );
}

/// TestBuildXAIWebsocketRequestBodySetsStoreAndKeepsPromptCacheKey without its `store: true`
/// (owner ruling E9c-1), and the payload rules (868e137) running on the frame after its
/// WebSocket fields, with only the type set after them.
#[test]
fn request_frame_sets_its_fields_then_runs_the_rules() {
    let body = br#"{"model":"grok-4.3","stream":true,"stream_options":{"include_usage":true},"background":true,"prompt_cache_key":"cache-1","input":[{"content":"first"},{"content":"second"}]}"#;
    let frame = request_frame(body.to_vec(), |b| b);
    assert_eq!(gj::get(&frame, "type").str(), "response.create");
    for absent in ["stream", "stream_options", "background"] {
        assert!(!gj::get(&frame, absent).exists(), "{absent}");
    }
    assert_eq!(gj::get(&frame, "prompt_cache_key").str(), "cache-1");
    assert!(
        !gj::get(&frame, "store").exists(),
        "the frame keeps the shaped body's store"
    );

    let ruled = request_frame(body.to_vec(), |mut b| {
        assert!(!gj::get(&b, "stream").exists(), "the rules see the WebSocket fields");
        gj::set_bool(&mut b, "store", true);
        gj::delete(&mut b, "type");
        gj::delete(&mut b, "input.0");
        b
    });
    assert!(gj::get(&ruled, "store").bool(), "a configured rule still wins");
    assert_eq!(gj::get(&ruled, "input.#").int(), 1);
    assert_eq!(gj::get(&ruled, "type").str(), "response.create");
}

/// Without a capture observer the turn's capture keeps no copy of the request frame.
#[test]
fn disabled_capture_keeps_no_frame() {
    struct Ignore;
    impl ck_core::exec::CaptureObserver for Ignore {
        fn record(&self, _: CaptureEvent<'_>) {}
    }
    let c = credential(serde_json::json!({"api_key": "sk-fake"}));
    let frame = br#"{"type":"response.create","input":"hi"}"#;
    let headers = HeaderMap::new();
    let url = "ws://127.0.0.1:1/v1/responses";
    let disabled = WsCapture::new(&UsageSink::default(), &c, url, &headers, frame);
    assert!(disabled.frame.is_empty() && disabled.url.is_empty());
    let usage = UsageSink::default().with_capture(CaptureSink::new(Arc::new(Ignore)));
    let enabled = WsCapture::new(&usage, &c, url, &headers, frame);
    assert_eq!(enabled.frame, frame);
}

/// TestXAIWebsockets_PingHandlerDoesNotBlockOnWriteMu: an upstream ping is answered while
/// the pooled socket's writer is held, because the reader sends the pong.
#[tokio::test]
async fn a_ping_is_answered_while_the_writer_is_held() {
    use axum::extract::ws::{Message, WebSocketUpgrade};
    let (pong_tx, mut pong_rx) = mpsc::channel::<Vec<u8>>(1);
    let app = axum::Router::new().fallback(move |upgrade: WebSocketUpgrade| {
        let pong_tx = pong_tx.clone();
        async move {
            upgrade.on_upgrade(move |mut socket| async move {
                // Ping once the test holds the writer.
                tokio::time::sleep(Duration::from_millis(300)).await;
                let _ = socket.send(Message::Ping(b"xai-keepalive-ping".to_vec().into())).await;
                while let Some(Ok(message)) = socket.recv().await {
                    if let Message::Pong(data) = message {
                        let _ = pong_tx.send(data.to_vec()).await;
                    }
                }
            })
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let credential = credential(serde_json::json!({"api_key": "xai-key"}));
    let cfg = Config::default();
    let target = Target::new(&credential, format!("ws://{addr}/v1/responses"), &cfg);
    let executor = XaiExecutor::with_client(wreq::Client::new());
    let session = executor.ws.pool.session("keepalive");
    let headers = HeaderMap::new();
    let capture = WsCapture::new(&UsageSink::default(), &credential, &target.url, &headers, b"");
    let (conn, _) = executor
        .socket(&session, (&target, &headers), (&credential, &cfg), &capture)
        .await
        .expect("the socket opens");
    let _writer = conn.sink.lock().await;
    let pong = tokio::time::timeout(Duration::from_secs(2), pong_rx.recv())
        .await
        .expect("pong starved by the held writer");
    assert_eq!(pong.as_deref(), Some(&b"xai-keepalive-ping"[..]));
    executor.ws.close("keepalive");
    server.abort();
}
