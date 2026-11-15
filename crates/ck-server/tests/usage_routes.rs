//! The HTTP routes' usage records keep the billing dimensions the upstream usage reports: a
//! client request through the router to a loopback upstream, read back from the usage queue.

use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::http::Request;
use ck_core::config::Config;
use ck_exec::{Executors, claude::ClaudeExecutor};
use serde_json::{Value, json};
use tower::ServiceExt;

const JSON: &str = "application/json";
const SSE: &str = "text/event-stream";

/// Sends `request` to `endpoint` through the router, whose only credential is a `provider` key
/// on a loopback upstream that answers every request with `answer`, and returns the one usage
/// record the attempt queued.
async fn record(provider: &str, endpoint: &str, request: Value, content_type: &'static str, answer: String) -> Value {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let upstream = axum::Router::new().fallback(move || {
        let answer = answer.clone();
        async move { ([("content-type", content_type)], answer) }
    });
    let server = tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });
    let base = if provider == "claude" {
        url.clone()
    } else {
        format!("{url}/v1")
    };
    let yaml = format!(
        "access:\n  api-keys: [client-key]\nobservability:\n  usage:\n    usage-statistics-enabled: true\napi-keys:\n  {provider}:\n    - name: Local\n      base-url: {base}\n      models:\n        - name: up-model\n          alias: fast\n      keys:\n        - api-key: sk-fake\n"
    );
    let config = Config::parse(&yaml).unwrap();
    let credentials = ck_core::config::credentials::from_config(&config);
    let executors = Executors {
        claude: ClaudeExecutor::new(&url).unwrap(),
        codex: Default::default(),
        devices: Default::default(),
        openai: Default::default(),
        google: Default::default(),
    };
    let rt = Arc::new(ck_server::testing::runtime(config, credentials, executors));
    rt.usage_queue().configure(true, &rt.config());
    let response = ck_server::router(rt.clone())
        .oneshot(
            Request::post(endpoint)
                .header("authorization", "Bearer client-key")
                .header("content-type", JSON)
                .body(Body::from(request.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 1 << 20).await.unwrap();
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let records = rt.usage_queue().pop_oldest(10);
    server.abort();
    let _ = server.await;
    assert_eq!(records.len(), 1);
    serde_json::from_slice(&records[0]).unwrap()
}

fn messages(stream: bool) -> Value {
    json!({"model":"fast","max_tokens":10,"stream":stream,"messages":[{"role":"user","content":"hi"}]})
}

#[tokio::test]
async fn anthropic_tier_speed_region_and_server_tools_reach_the_record() {
    let answer = json!({"id":"msg","type":"message","role":"assistant","model":"up-model","content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":5,"output_tokens":2,"service_tier":"standard","speed":"fast","inference_geo":"us","server_tool_use":{"web_search_requests":2,"web_fetch_requests":0}}});
    let record = record("claude", "/v1/messages", messages(false), JSON, answer.to_string()).await;
    assert_eq!(record["response_service_tier"], "standard");
    assert_eq!(
        record["dimensions"],
        json!({"speed":"fast","inference_geo":"us","web_search_requests":2,"web_fetch_requests":0})
    );
}

/// `message_start` reports the tier, speed and region; `message_delta` replaces the server-tool
/// counters it reports and keeps the rest. A field the upstream never sent stays absent.
#[tokio::test]
async fn streamed_anthropic_dimensions_merge_like_the_sdk() {
    let events = [
        json!({"type":"message_start","message":{"id":"msg","type":"message","role":"assistant","model":"up-model","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":5,"output_tokens":1,"service_tier":"priority","speed":"fast","server_tool_use":{"web_search_requests":0,"web_fetch_requests":4}}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hi"}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":4,"server_tool_use":{"web_search_requests":3}}}),
        json!({"type":"message_stop"}),
    ];
    let answer: String = events
        .iter()
        .map(|event| format!("event: {}\ndata: {event}\n\n", event["type"].as_str().unwrap()))
        .collect();
    let record = record("claude", "/v1/messages", messages(true), SSE, answer).await;
    assert_eq!(record["response_service_tier"], "priority");
    assert_eq!(record["dimensions"], json!({"speed":"fast","web_search_requests":3}));
}

/// A `server_tool_use` the delta reports replaces the earlier one whole, even when it holds no
/// counter the record can name; a `null` one is not reported and keeps the earlier counters.
#[tokio::test]
async fn a_reported_empty_server_tool_use_clears_earlier_counters() {
    for (delta, dimensions) in [
        (json!({}), json!(null)),
        (json!({"web_search_requests":-1}), json!(null)),
        (json!(null), json!({"web_search_requests":2,"web_fetch_requests":4})),
    ] {
        let events = [
            json!({"type":"message_start","message":{"id":"msg","type":"message","role":"assistant","model":"up-model","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":5,"output_tokens":1,"server_tool_use":{"web_search_requests":2,"web_fetch_requests":4}}}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hi"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":4,"server_tool_use":delta}}),
            json!({"type":"message_stop"}),
        ];
        let answer: String = events
            .iter()
            .map(|event| format!("event: {}\ndata: {event}\n\n", event["type"].as_str().unwrap()))
            .collect();
        let record = record("claude", "/v1/messages", messages(true), SSE, answer).await;
        assert_eq!(
            record.get("dimensions").cloned().unwrap_or(Value::Null),
            dimensions,
            "{delta}"
        );
    }
}

#[tokio::test]
async fn chat_modality_and_prediction_details_reach_the_record() {
    let answer = json!({"id":"c","object":"chat.completion","created":1,"model":"up-model","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":8,"total_tokens":18,"prompt_tokens_details":{"audio_tokens":2,"text_tokens":5,"image_tokens":3},"completion_tokens_details":{"audio_tokens":4,"text_tokens":4,"accepted_prediction_tokens":6,"rejected_prediction_tokens":0}}});
    let record = record(
        "openai-compatibility",
        "/v1/chat/completions",
        messages(false),
        JSON,
        answer.to_string(),
    )
    .await;
    assert_eq!(
        record["dimensions"],
        json!({"input_audio_tokens":2,"input_text_tokens":5,"input_image_tokens":3,"output_audio_tokens":4,"output_text_tokens":4,"accepted_prediction_tokens":6,"rejected_prediction_tokens":0})
    );
    assert_eq!(record["tokens"]["total_tokens"], 18);
}

/// A Chat usage without details records no dimensions rather than zeros.
#[tokio::test]
async fn unreported_dimensions_stay_out_of_the_record() {
    let answer = json!({"id":"c","object":"chat.completion","created":1,"model":"up-model","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":8,"total_tokens":18}});
    let record = record(
        "openai-compatibility",
        "/v1/chat/completions",
        messages(false),
        JSON,
        answer.to_string(),
    )
    .await;
    assert_eq!(record.get("dimensions"), None);
    assert_eq!(record["tokens"].get("cache_creation_30m_tokens"), None);
}

#[tokio::test]
async fn responses_cache_write_ttls_reach_the_record() {
    let answer = json!({"id":"r","object":"response.compaction","output":[],"usage":{"input_tokens":53,"output_tokens":18,"total_tokens":71,"input_tokens_details":{"cached_tokens":0,"cache_write_tokens":7,"cache_write_tokens_details":{"ttl_30m_tokens":3,"ttl_12h_tokens":4}}}});
    let request = json!({"model":"fast","input":"hi"});
    let record = record(
        "openai-compatibility",
        "/v1/responses/compact",
        request,
        JSON,
        answer.to_string(),
    )
    .await;
    assert_eq!(record["tokens"]["cache_creation_tokens"], 7);
    assert_eq!(record["tokens"]["cache_creation_30m_tokens"], 3);
    assert_eq!(record["tokens"]["cache_creation_12h_tokens"], 4);
}
