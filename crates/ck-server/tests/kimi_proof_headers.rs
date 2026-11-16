//! Kimi proof headers delivered through the actual inference router.
#[path = "support/stream_upstream.rs"]
mod upstream;

use axum::body::{Body, to_bytes};
use axum::http::Request;
use ck_core::config::Config;
use ck_core::credential::Credential;
use ck_exec::Executors;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceExt;
use upstream::{Reply, StreamUpstream};

fn runtime(url: &str) -> Arc<ck_server::Runtime> {
    runtime_with(url, "{}\n", None)
}

fn runtime_with(url: &str, yaml: &str, nonce: Option<&str>) -> Arc<ck_server::Runtime> {
    let mut cfg = Config::parse(yaml).unwrap();
    cfg.api_keys = vec!["client-test".into()];
    let dir = std::path::Path::new("/fixture");
    let mut credential = Credential::from_file(
        dir,
        &dir.join("kimi.json"),
        json!({"type":"kimi","access_token":"upstream-test","device_id":"fixture","base_url":format!("{url}/v1")})
            .as_object()
            .unwrap()
            .clone(),
    )
    .unwrap();
    if let Some(nonce) = nonce {
        credential
            .attributes
            .insert("header:X-Msh-Request-Nonce".into(), nonce.into());
    }
    let executors = Executors {
        claude: ck_exec::claude::ClaudeExecutor::new(url).unwrap(),
        codex: Default::default(),
        devices: Default::default(),
        openai: Default::default(),
        google: Default::default(),
    };
    let mut credentials = ck_core::config::credentials::from_config(&cfg);
    credentials.push(credential);
    Arc::new(ck_server::testing::runtime(cfg, credentials, executors))
}

#[tokio::test]
async fn configured_nonce_overrides_the_client_and_hop_headers_stay_filtered() {
    for passthrough in [false, true] {
        let upstream = StreamUpstream::start([answer("messages", false)
            .header("Connection", "Msh-Request-Timestamp")
            .header("Msh-Request-Timestamp", "1786338000123")
            .header("Msh-Request-Signature", "reqsigv1_fixture")
            .header("x-private", "configured-passthrough")])
        .await;
        let yaml = format!("requests:\n  passthrough-headers: {passthrough}\n");
        let rt = runtime_with(&upstream.url(), &yaml, Some("configured-nonce"));
        let request = Request::post("/v1/messages")
            .header("authorization", "Bearer client-test")
            .header("content-type", "application/json")
            .header("x-msh-request-nonce", "client-nonce")
            .body(Body::from(
                r#"{"model":"kimi-k3","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}"#,
            ))
            .unwrap();
        let response = ck_server::router(rt).oneshot(request).await.unwrap();
        assert!(response.status().is_success());
        assert_eq!(response.headers()["msh-request-signature"], "reqsigv1_fixture");
        assert!(!response.headers().contains_key("msh-request-timestamp"));
        assert_eq!(response.headers().contains_key("x-private"), passthrough);
        assert_eq!(
            upstream.captured()[0].header("x-msh-request-nonce"),
            Some("configured-nonce")
        );
    }
}

#[tokio::test]
async fn an_empty_or_repeated_nonce_is_not_forwarded() {
    for values in [vec![""], vec!["one", "two"]] {
        let upstream = StreamUpstream::start([answer("messages", false)]).await;
        let rt = runtime(&upstream.url());
        let mut request = Request::post("/v1/messages")
            .header("authorization", "Bearer client-test")
            .header("content-type", "application/json");
        for value in values {
            request = request.header("x-msh-request-nonce", value);
        }
        let response = ck_server::router(rt)
            .oneshot(
                request
                    .body(Body::from(
                        r#"{"model":"kimi-k3","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(response.status().is_success());
        assert_eq!(upstream.captured()[0].header("x-msh-request-nonce"), None);
    }
}

fn answer(protocol: &str, stream: bool) -> Reply {
    let (json, sse) = match protocol {
        "chat/completions" => (
            r#"{"id":"c1","object":"chat.completion","created":1,"model":"kimi-k3","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}"#,
            "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"kimi-k3\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n",
        ),
        "responses" => (
            r#"{"id":"r1","object":"response","created_at":1,"status":"completed","model":"kimi-k3","output":[]}"#,
            "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"object\":\"response\",\"created_at\":1,\"status\":\"completed\",\"model\":\"kimi-k3\",\"output\":[]}}\n\n",
        ),
        _ => (
            r#"{"id":"m1","type":"message","role":"assistant","model":"kimi-k3","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}"#,
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"kimi-k3\",\"content\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
        ),
    };
    if stream { Reply::sse(sse) } else { Reply::json(json) }
}

#[tokio::test]
async fn proof_headers_reach_the_client_buffered_and_streamed() {
    for protocol in ["chat/completions", "responses", "messages"] {
        for stream in [false, true] {
            let reply = answer(protocol, stream)
                .header("Msh-Request-Timestamp", "1786338000123")
                .header("Msh-Request-Signature", "reqsigv1_fixture")
                .header("Msh-Request-Secret", "hidden")
                .header("Set-Cookie", "private=value")
                .header("Authorization", "private")
                .header("x-private", "hidden")
                .first_byte_after(Duration::from_millis(1200));
            let upstream = StreamUpstream::start([reply]).await;
            let rt = runtime(&upstream.url());
            let mut body = json!({"model":"kimi-k3","stream":stream,"max_tokens":16});
            if protocol == "responses" {
                body["input"] = json!("hi");
            } else {
                body["messages"] = json!([{"role":"user","content":"hi"}]);
            }
            let request = Request::post(format!("/v1/{protocol}"))
                .header("authorization", "Bearer client-test")
                .header("content-type", "application/json")
                .header("x-msh-request-nonce", "nonce-fixture")
                .body(Body::from(body.to_string()))
                .unwrap();
            let response = ck_server::router(rt).oneshot(request).await.unwrap();
            assert!(
                response.status().is_success(),
                "{protocol} {stream}: {}",
                response.status()
            );
            assert_eq!(
                response
                    .headers()
                    .get("msh-request-signature")
                    .and_then(|v| v.to_str().ok()),
                Some("reqsigv1_fixture"),
                "{protocol} {stream}"
            );
            assert_eq!(response.headers()["msh-request-timestamp"], "1786338000123");
            for name in ["msh-request-secret", "set-cookie", "authorization", "x-private"] {
                assert!(!response.headers().contains_key(name), "{name} leaked");
            }
            assert!(!to_bytes(response.into_body(), 1 << 20).await.unwrap().is_empty());
            assert_eq!(
                upstream.captured()[0].header("x-msh-request-nonce"),
                Some("nonce-fixture"),
                "{protocol} nonce"
            );
        }
    }
}

#[tokio::test]
async fn a_slow_routed_response_keeps_the_proof() {
    let upstream = StreamUpstream::start([answer("chat/completions", false)
        .header("Msh-Request-Timestamp", "1786338000123")
        .header("Msh-Request-Signature", "reqsigv1_routed")
        .first_byte_after(Duration::from_millis(1200))])
    .await;
    let yaml = format!(
        r#"model-routers:
  - name: smart
    judge: unused-judge
    models: [kimi-k3]
api-keys:
  openai-compatibility:
    - name: decoy
      base-url: {}/v1
      keys:
        - api-key: fixture
      models:
        - name: smart
"#,
        upstream.url()
    );
    let rt = runtime_with(&upstream.url(), &yaml, Some("configured-nonce"));
    assert_eq!(rt.registry().model_providers("smart"), vec!["openai-compatible-decoy"]);
    let request = Request::post("/v1/chat/completions")
        .header("authorization", "Bearer client-test")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"model":"smart","messages":[{"role":"user","content":"hi"}]}"#,
        ))
        .unwrap();
    let response = ck_server::router(rt).oneshot(request).await.unwrap();
    assert!(response.status().is_success());
    assert_eq!(
        response
            .headers()
            .get("msh-request-signature")
            .and_then(|v| v.to_str().ok()),
        Some("reqsigv1_routed")
    );
    assert_eq!(response.headers()["msh-request-timestamp"], "1786338000123");
    let sent = upstream.captured();
    assert_eq!(sent[0].header("x-msh-request-nonce"), Some("configured-nonce"));
    let body: serde_json::Value = serde_json::from_slice(&sent[0].body).unwrap();
    assert_eq!(body["model"], "kimi-k3");
}
