//! The `/v1/ws` relay end to end. A test websocket client plays the AI Studio browser:
//! connecting gives the runtime its `aistudio` credential, a client request for a
//! Gemini model travels to the browser and back, and the credential disappears when
//! the browser leaves. Only a key from `oauth.providers.aistudio.relay-keys` registers a
//! browser.
//! Byte-level parity of the relayed requests is checked in ck-exec (aistudio_tests.rs).

use std::sync::Arc;
use std::time::Duration;

use ck_core::config::Config;
use ck_exec::Executors;
use ck_exec::claude::ClaudeExecutor;
use ck_server::{Runtime, router};
use futures_util::StreamExt;
use serde_json::Value;
use wreq::ws::message::Message;

const ANSWER: &str = r#"{"candidates":[{"content":{"role":"model","parts":[{"text":"ok"}]},"finishReason":"STOP"}],"modelVersion":"gemini-2.5-flash"}"#;

async fn serve(rt: Arc<Runtime>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move { axum::serve(listener, router(rt)).await.unwrap() });
    addr
}

fn runtime(config: &str) -> Arc<Runtime> {
    let executors = Executors {
        claude: ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
        codex: Default::default(),
        devices: Default::default(),
        openai: Default::default(),
        google: Default::default(),
    };
    let config = Config::parse(config).unwrap();
    Arc::new(ck_server::testing::runtime(config, Vec::new(), executors))
}

/// The relay credentials the runtime holds, waiting up to five seconds for `want`.
async fn relay_credentials(rt: &Runtime, want: usize) -> Vec<String> {
    for _ in 0..500 {
        let ids: Vec<String> = rt
            .store()
            .snapshot()
            .iter()
            .filter(|c| c.provider == "aistudio")
            .map(|c| c.id.clone())
            .collect();
        if ids.len() == want {
            return ids;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("expected {want} relay credentials");
}

async fn next_text(socket: &mut wreq::ws::WebSocket) -> String {
    loop {
        match tokio::time::timeout(Duration::from_secs(5), socket.next())
            .await
            .unwrap()
        {
            Some(Ok(Message::Text(text))) => return text.as_str().to_owned(),
            Some(Ok(_)) => continue,
            other => panic!("socket ended: {other:?}"),
        }
    }
}

#[tokio::test]
async fn browser_session_serves_requests_until_it_leaves() {
    let rt = runtime(RELAY_CONFIG);
    let addr = serve(rt.clone()).await;
    let mut socket = register(&addr).await;
    let ids = relay_credentials(&rt, 1).await;
    let id = &ids[0];
    assert!(
        id.len() == 25
            && id.starts_with("aistudio-")
            && id[9..].bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit()),
        "{id}"
    );
    let credential = rt.store().get(id).unwrap();
    assert_eq!(credential.label, *id);
    assert_eq!(credential.attributes["runtime_only"], "true");

    // A Gemini request goes to the browser as an http_request message.
    let url = format!("http://{addr}/v1beta/models/gemini-2.5-flash:generateContent");
    let call = tokio::spawn(async move {
        let res = wreq::Client::new()
            .post(url)
            .header("authorization", "Bearer client-key")
            .header("content-type", "application/json")
            .body(r#"{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}"#)
            .send()
            .await
            .unwrap();
        (res.status().as_u16(), res.text().await.unwrap())
    });
    let frame = next_text(&mut socket).await;
    assert!(frame.ends_with('\n'), "WriteJSON ends the frame with a newline");
    let request: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(request["type"], "http_request");
    assert_eq!(
        request["payload"]["url"],
        "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"
    );
    let reply = serde_json::json!({
        "id": request["id"],
        "type": "http_response",
        "payload": {"status": 200, "headers": {"Content-Type": ["application/json"]}, "body": ANSWER},
    });
    socket.send(Message::text(reply.to_string())).await.unwrap();
    let (status, text) = call.await.unwrap();
    assert_eq!(status, 200, "{text}");
    // ensureColonSpacedJSON: keys sorted, a space after each colon.
    assert_eq!(
        text,
        r#"{"candidates": [{"content": {"parts": [{"text": "ok"}],"role": "model"},"finishReason": "STOP"}],"modelVersion": "gemini-2.5-flash"}"#
    );

    // Pings from the browser get pongs with the same ID.
    socket
        .send(Message::text(r#"{"id":"p1","type":"ping"}"#))
        .await
        .unwrap();
    assert_eq!(next_text(&mut socket).await, "{\"id\":\"p1\",\"type\":\"pong\"}\n");

    // Leaving removes the credential.
    socket.close(1000u16, "bye").await.unwrap();
    assert!(relay_credentials(&rt, 0).await.is_empty());
}

/// Client keys and relay keys are separate lists: clients send requests, relay keys
/// register browsers.
const RELAY_CONFIG: &str =
    "access:\n  api-keys: [client-key]\noauth:\n  providers:\n    aistudio:\n      relay-keys: [relay-key]\n";

async fn register(addr: &str) -> wreq::ws::WebSocket {
    let response = wreq::Client::new()
        .websocket(format!("ws://{addr}/v1/ws"))
        .header("authorization", "Bearer relay-key")
        .send()
        .await
        .unwrap();
    response.into_websocket().await.unwrap()
}

#[tokio::test]
async fn registration_needs_a_listed_relay_key() {
    let none = "access:\n  api-keys: [client-key]\n";
    let empty = "access:\n  api-keys: [client-key]\noauth:\n  providers:\n    aistudio:\n      relay-keys: []\n";
    let open = "oauth:\n  providers:\n    aistudio:\n      relay-keys: [relay-key]\n";
    // (config, Authorization header, expected handshake status)
    let rows: [(&str, Option<&str>, u16); 9] = [
        ("", None, 403),
        ("", Some("Bearer anything"), 403),
        (none, Some("Bearer client-key"), 403),
        (empty, Some("Bearer relay-key"), 403),
        (RELAY_CONFIG, None, 401),
        (RELAY_CONFIG, Some("Bearer client-key"), 401),
        (RELAY_CONFIG, Some("Bearer relay-key"), 101),
        // Open client access does not open registration.
        (open, None, 401),
        (open, Some("Bearer relay-key"), 101),
    ];
    for (config, authorization, want) in rows {
        let rt = runtime(config);
        let addr = serve(rt.clone()).await;
        let mut handshake = wreq::Client::new().websocket(format!("ws://{addr}/v1/ws"));
        if let Some(value) = authorization {
            handshake = handshake.header("authorization", value);
        }
        let response = handshake.send().await.unwrap();
        let status = response.status().as_u16();
        assert_eq!(status, want, "{config:?} {authorization:?}");
        let _socket = if status == 101 {
            Some(response.into_websocket().await.unwrap())
        } else {
            None
        };
        relay_credentials(&rt, usize::from(status == 101)).await;
        // The same check answers a plain GET before the handshake check (400 when admitted).
        let mut plain = wreq::Client::new().get(format!("http://{addr}/v1/ws"));
        if let Some(value) = authorization {
            plain = plain.header("authorization", value);
        }
        let plain = plain.send().await.unwrap();
        assert_eq!(plain.status().as_u16(), if want == 101 { 400 } else { want });
        let body = plain.text().await.unwrap();
        assert!(!body.contains("-key"), "errors never echo a key: {body}");
    }
}

#[tokio::test]
async fn losing_the_browser_before_the_answer_fails_over() {
    for (path, body_shape) in [
        ("generateContent", "candidates"),
        ("streamGenerateContent?alt=sse", "data: "),
    ] {
        let rt = runtime(RELAY_CONFIG);
        let addr = serve(rt.clone()).await;
        let mut first = register(&addr).await;
        let mut second = register(&addr).await;
        relay_credentials(&rt, 2).await;
        let url = format!("http://{addr}/v1beta/models/gemini-2.5-flash:{path}");
        let mut call = tokio::spawn(async move {
            let res = wreq::Client::new()
                .post(url)
                .header("authorization", "Bearer client-key")
                .header("content-type", "application/json")
                .body(r#"{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}"#)
                .send()
                .await
                .unwrap();
            (res.status().as_u16(), res.text().await.unwrap())
        });
        // Whichever browser takes the request is lost before it answers; the other one
        // must get the request again.
        let (taken, first_took) = tokio::select! {
            frame = next_text(&mut first) => (frame, true),
            frame = next_text(&mut second) => (frame, false),
        };
        let taken: Value = serde_json::from_str(&taken).unwrap();
        assert_eq!(taken["type"], "http_request");
        let (lost, mut served) = if first_took { (first, second) } else { (second, first) };
        drop(lost);
        let frame = tokio::select! {
            frame = next_text(&mut served) => frame,
            answer = &mut call => panic!("{path}: answered without failing over: {answer:?}"),
        };
        let request: Value = serde_json::from_str(&frame).unwrap();
        let reply = if path.starts_with("stream") {
            for (kind, payload) in [
                (
                    "stream_start",
                    serde_json::json!({"status": 200, "headers": {"Content-Type": ["text/event-stream"]}}),
                ),
                (
                    "stream_chunk",
                    serde_json::json!({"data": format!("data: {ANSWER}\n\n")}),
                ),
                ("stream_end", Value::Null),
            ] {
                let frame = serde_json::json!({"id": request["id"], "type": kind, "payload": payload});
                served.send(Message::text(frame.to_string())).await.unwrap();
            }
            None
        } else {
            Some(serde_json::json!({
                "id": request["id"],
                "type": "http_response",
                "payload": {"status": 200, "headers": {"Content-Type": ["application/json"]}, "body": ANSWER},
            }))
        };
        if let Some(reply) = reply {
            served.send(Message::text(reply.to_string())).await.unwrap();
        }
        let (status, text) = call.await.unwrap();
        assert_eq!(status, 200, "{path}: {text}");
        assert!(text.contains(body_shape) && text.contains("ok"), "{path}: {text}");
    }
}

#[tokio::test]
async fn sessions_end_on_bad_frames_and_relay_key_changes() {
    let rt = runtime(RELAY_CONFIG);
    let addr = serve(rt.clone()).await;
    // A plain GET is not a handshake: gorilla's 400.
    let plain = wreq::Client::new()
        .get(format!("http://{addr}/v1/ws"))
        .header("authorization", "Bearer relay-key")
        .send()
        .await
        .unwrap();
    assert_eq!(plain.status(), 400);
    assert_eq!(plain.headers()["sec-websocket-version"], "13");
    assert_eq!(plain.text().await.unwrap(), "Bad Request\n");
    let mut socket = register(&addr).await;
    relay_credentials(&rt, 1).await;
    // A frame Go cannot decode ends the session and its credential.
    socket.send(Message::text("[1]")).await.unwrap();
    assert!(relay_credentials(&rt, 0).await.is_empty());

    let _socket = register(&addr).await;
    relay_credentials(&rt, 1).await;
    // The relay credential has no file: a watcher reconcile keeps it.
    rt.store().reconcile(Vec::new());
    relay_credentials(&rt, 1).await;
    // A config publish that keeps the relay keys keeps the session.
    rt.publish_config(Config::parse(&format!("{RELAY_CONFIG}server:\n  port: 1\n")).unwrap());
    relay_credentials(&rt, 1).await;
    // Changing the relay keys ends the sessions they registered.
    rt.publish_config(Config::parse("oauth:\n  providers:\n    aistudio:\n      relay-keys: [other-key]\n").unwrap());
    assert!(relay_credentials(&rt, 0).await.is_empty());
}
