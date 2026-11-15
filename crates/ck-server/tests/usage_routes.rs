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
    exchange(provider, endpoint, request, content_type, answer).await.0
}

/// [`record`] and the body the client received.
async fn exchange(
    provider: &str,
    endpoint: &str,
    request: Value,
    content_type: &'static str,
    answer: String,
) -> (Value, String) {
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
    (
        serde_json::from_slice(&records[0]).unwrap(),
        String::from_utf8(body.to_vec()).unwrap(),
    )
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

/// A streamed message's usage merges like the Anthropic SDK's: `message_start` reports the
/// tier, speed and region, and `message_delta` replaces the server-tool counters whenever it
/// reports a non-null `server_tool_use`, even one without a counter the record can name. Billing
/// dimensions are kept whatever the token counts, and a field never sent stays absent.
#[tokio::test]
async fn streamed_anthropic_dimensions_merge_like_the_sdk() {
    let tools = json!({"web_search_requests":2,"web_fetch_requests":4});
    for (start, delta, tier, dimensions) in [
        (
            json!({"input_tokens":5,"output_tokens":1,"service_tier":"priority","speed":"fast","server_tool_use":{"web_search_requests":0,"web_fetch_requests":4}}),
            json!({"output_tokens":4,"server_tool_use":{"web_search_requests":3}}),
            json!("priority"),
            json!({"speed":"fast","web_search_requests":3}),
        ),
        (
            json!({"input_tokens":0,"output_tokens":0,"service_tier":"priority","speed":"fast"}),
            json!({"output_tokens":4,"server_tool_use":{"web_search_requests":3}}),
            json!("priority"),
            json!({"speed":"fast","web_search_requests":3}),
        ),
        (
            json!({"input_tokens":5,"output_tokens":1,"server_tool_use":tools}),
            json!({"output_tokens":4,"server_tool_use":{}}),
            Value::Null,
            Value::Null,
        ),
        (
            json!({"input_tokens":5,"output_tokens":1,"server_tool_use":tools}),
            json!({"output_tokens":4,"server_tool_use":{"web_search_requests":-1}}),
            Value::Null,
            Value::Null,
        ),
        (
            json!({"input_tokens":5,"output_tokens":1,"server_tool_use":tools}),
            json!({"output_tokens":4,"server_tool_use":null}),
            Value::Null,
            tools.clone(),
        ),
    ] {
        let events = [
            json!({"type":"message_start","message":{"id":"msg","type":"message","role":"assistant","model":"up-model","content":[],"stop_reason":null,"stop_sequence":null,"usage":start}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hi"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":delta}),
            json!({"type":"message_stop"}),
        ];
        let answer: String = events
            .iter()
            .map(|event| format!("event: {}\ndata: {event}\n\n", event["type"].as_str().unwrap()))
            .collect();
        let record = record("claude", "/v1/messages", messages(true), SSE, answer).await;
        let field = |name| record.get(name).cloned().unwrap_or(Value::Null);
        assert_eq!(field("response_service_tier"), tier, "{start} then {delta}");
        assert_eq!(field("dimensions"), dimensions, "{start} then {delta}");
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

/// Meta segmentation bills per image and per video frame. Each count the upstream Responses
/// usage reports reaches the record as its own dimension, buffered or streamed, whatever the token
/// counts and tier beside it; an unreported or malformed one stays absent and the token counts are
/// the upstream's. The client still receives the native usage, and accounting records the counts
/// once: a later terminal event replaces an earlier one's counts rather than adding to them.
#[tokio::test]
async fn meta_segmentation_counts_reach_the_record() {
    let tokens = json!({"input_tokens":12,"input_tokens_details":{"cached_tokens":0},"output_tokens":3,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":15});
    for (counts, tier, dimensions) in [
        (json!({"images_processed":2}), None, json!({"images_processed":2})),
        (
            json!({"video_frames_processed":48}),
            None,
            json!({"video_frames_processed":48}),
        ),
        (
            json!({"images_processed":1,"video_frames_processed":0}),
            None,
            json!({"images_processed":1,"video_frames_processed":0}),
        ),
        (json!({}), None, Value::Null),
        (
            json!({"images_processed":null,"video_frames_processed":-1}),
            None,
            Value::Null,
        ),
        (
            json!({"images_processed":2,"video_frames_processed":0}),
            Some("auto"),
            json!({"images_processed":2,"video_frames_processed":0}),
        ),
        (
            json!({"input_tokens":0,"output_tokens":0,"total_tokens":0,"images_processed":2,"video_frames_processed":0}),
            Some("auto"),
            json!({"images_processed":2,"video_frames_processed":0}),
        ),
    ] {
        let response = |counts: &Value| {
            let mut usage = tokens.clone();
            usage
                .as_object_mut()
                .unwrap()
                .extend(counts.as_object().unwrap().clone());
            let mut response = json!({"id":"r","object":"response","created_at":1,"model":"up-model","status":"completed","output":[{"id":"msg","type":"message","role":"assistant","content":[{"type":"output_text","text":"mask","annotations":[]}]}],"usage":usage});
            if let Some(tier) = tier {
                response["service_tier"] = json!(tier);
            }
            response
        };
        let completed = |counts: &Value| json!({"type":"response.completed","response":response(counts)});
        let earlier = json!({"images_processed":5,"video_frames_processed":7});
        let streamed = format!(
            "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
            completed(&earlier),
            completed(&counts)
        );
        let usage = &response(&counts)["usage"];
        // The client's stream ends at its first terminal event, so it receives the earlier counts.
        for (stream, content_type, answer, delivered) in [
            (false, JSON, response(&counts).to_string(), &counts),
            (true, SSE, streamed, &earlier),
        ] {
            let request = json!({"model":"fast","input":"segment the cat","stream":stream});
            let (record, client) = exchange("meta", "/v1/responses", request, content_type, answer).await;
            let field = |name| record.get(name).cloned().unwrap_or(Value::Null);
            assert_eq!(
                field("dimensions"),
                dimensions,
                "{counts} tier={tier:?} stream={stream}"
            );
            assert_eq!(
                (
                    &record["tokens"]["input_tokens"],
                    &record["tokens"]["output_tokens"],
                    &record["tokens"]["total_tokens"]
                ),
                (&usage["input_tokens"], &usage["output_tokens"], &usage["total_tokens"]),
                "{counts} tier={tier:?} stream={stream}"
            );
            for (name, count) in delivered.as_object().unwrap() {
                assert!(
                    client.contains(&format!("\"{name}\":{count}")),
                    "{name} stream={stream}: {client}"
                );
            }
        }
    }
}
