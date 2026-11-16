use super::*;
use serde_json::{Value, json};

const ANSWER: &str =
    r#"{"id":"r1","object":"response","created_at":1,"status":"completed","model":"kimi-k3","output":[]}"#;
const MESSAGE: &str = r#"{"id":"m1","type":"message","role":"assistant","model":"kimi-k3","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}"#;

async fn upload(protocol: &str, base: &str, model: &str, body: Value) -> support::upstream::Captured {
    let reply = match protocol {
        "chat" => CHAT_JSON,
        "anthropic" => MESSAGE,
        _ => ANSWER,
    };
    let upstream = StreamUpstream::start([Reply::json(reply)]).await;
    let mut exchange = exchange(&upstream, model);
    exchange.credential = common::credential(&format!("{}{base}", upstream.url()), &Value::Null);
    if protocol == "anthropic" {
        // The server attaches the model's registry facts to a Claude-format attempt.
        let resolved = ck_core::exec::ResolvedModel {
            info: ck_core::registry::ModelInfo::from_raw(
                json!({"id":model,"thinking":{"levels":["low","high","max"]}})
                    .as_object()
                    .unwrap()
                    .clone(),
            )
            .unwrap(),
            source: ck_core::exec::ResolvedSource::ApiKey,
        };
        return old_upload_resolved(
            &upstream,
            &exchange,
            "claude",
            body.to_string().as_bytes(),
            Some(resolved),
        )
        .await;
    }
    let streams = Streams::new();
    let (started, id, _request) = run(&streams, &exchange, protocol, body.to_string().as_bytes(), 17).await;
    finish(&streams, started.unwrap(), id).await.0.unwrap();
    upstream.captured().remove(0)
}

fn input(protocol: &str, model: &str) -> Value {
    if protocol == "responses" {
        json!({"model":model,"input":"hi"})
    } else {
        json!({"model":model,"max_tokens":16,"messages":[{"role":"user","content":"hi"}]})
    }
}

#[tokio::test]
async fn models_keep_platform_ids_and_normalize_on_coding() {
    for protocol in ["chat", "responses", "anthropic"] {
        for (model, normalized) in [
            ("kimi-k3", "k3"),
            ("kimi-k3.1-pro", "k3.1-pro"),
            ("kimi-k3.1-pro-highspeed", "k3.1-pro-highspeed"),
            ("kimi-k2.6", "k2.6"),
            ("kimi-k2.7-code", "kimi-for-coding"),
        ] {
            for (base, expected) in [("/v1", model), ("/coding", normalized)] {
                let captured = upload(protocol, base, model, input(protocol, model)).await;
                let body: Value = serde_json::from_slice(&captured.body).unwrap();
                assert_eq!(body["model"], expected, "{protocol} {base}");
            }
        }
    }
}

#[tokio::test]
async fn endpoint_follows_product_and_protocol() {
    for (base, prefix) in [("/v1", ""), ("/anthropic", ""), ("/coding", "/coding")] {
        for (protocol, suffix) in [
            ("chat", "/v1/chat/completions"),
            ("responses", "/v1/responses"),
            ("anthropic", "/v1/messages"),
        ] {
            let captured = upload(protocol, base, "kimi-k3", input(protocol, "kimi-k3")).await;
            let prefix = if prefix.is_empty() && protocol == "anthropic" {
                "/anthropic"
            } else {
                prefix
            };
            assert_eq!(
                captured.path.split('?').next().unwrap(),
                format!("{prefix}{suffix}"),
                "{base} {protocol}"
            );
        }
    }
}

#[tokio::test]
async fn temperature_reaches_the_upstream_untouched() {
    for (model, temperature, thinking) in [
        ("kimi-k3", 0.2, Value::Null),
        ("kimi-k3", 2.0, Value::Null),
        ("kimi-k2.6", 0.6, json!({"type":"disabled"})),
    ] {
        let mut body = input("chat", model);
        body["temperature"] = json!(temperature);
        if !thinking.is_null() {
            body["thinking"] = thinking;
        }
        let captured = upload("chat", "/v1", model, body).await;
        let sent: Value = serde_json::from_slice(&captured.body).unwrap();
        assert_eq!(
            sent["temperature"], temperature,
            "{model}: the upstream validates temperature"
        );
        if model == "kimi-k2.6" {
            assert_eq!(sent["thinking"]["type"], "disabled");
        }
    }
}

#[tokio::test]
async fn an_invalid_temperature_is_left_to_the_upstream_refusal() {
    let error = r#"{"error":{"message":"invalid temperature: only 1 is allowed for this model","type":"invalid_request_error"}}"#;
    let upstream = StreamUpstream::start([Reply::status(400, "application/json", error)]).await;
    let exchange = exchange(&upstream, "kimi-k3");
    let streams = Streams::new();
    let body = br#"{"model":"kimi-k3","temperature":0.2,"messages":[{"role":"user","content":"hi"}]}"#;
    let (started, _, _) = run(&streams, &exchange, "chat", body, 17).await;
    let refused = started.err().unwrap();
    assert_eq!(refused.status, 400);
    assert_eq!(refused.body.as_ref(), error.as_bytes());
    let sent: Value = serde_json::from_slice(&upstream.captured()[0].body).unwrap();
    assert_eq!(sent["temperature"], 0.2);
}

#[tokio::test]
async fn custom_tools_are_sent_and_replayed_natively() {
    let tool = json!({"type":"custom","name":"apply_patch","format":{"type":"grammar","syntax":"lark","definition":"start: /.+/"}});
    let call = json!({"id":"ct1","type":"custom_tool_call","call_id":"call1","name":"apply_patch","input":"*** Begin Patch\n*** End Patch"});
    let output = json!({"type":"custom_tool_call_output","call_id":"call1","output":"ok"});
    for choice in [json!("auto"), json!({"type":"custom","name":"apply_patch"})] {
        let body = json!({"model":"kimi-k3","tools":[tool],"input":[call,output],"tool_choice":choice});
        let captured = upload("responses", "/v1", "kimi-k3", body).await;
        let sent: Value = serde_json::from_slice(&captured.body).unwrap();
        assert_eq!(sent["tools"][0], tool, "declaration");
        assert_eq!(sent["input"][0], call, "replayed call");
        assert_eq!(sent["input"][1], output, "replayed output");
        assert_eq!(sent["tool_choice"], choice, "tool_choice is forwarded as sent");
    }
}

#[tokio::test]
async fn a_custom_tool_without_a_grammar_is_refused() {
    let upstream = StreamUpstream::start([]).await;
    let exchange = exchange(&upstream, "kimi-k3");
    let streams = Streams::new();
    let body = br#"{"model":"kimi-k3","input":"hi","tools":[{"type":"custom","name":"apply_patch"}]}"#;
    let (result, _) = try_run(&streams, &exchange, "responses", Operation::Generate, body, 17).await;
    assert!(
        matches!(result, Err(codec::Error::Unsupported | codec::Error::Schema)),
        "a missing grammar must refuse, not fall back to a function bridge"
    );
    assert_eq!(upstream.requests(), 0);
}

#[tokio::test]
async fn request_nonce_is_forwarded_only_when_single_and_not_overridden() {
    for (values, configured, expected) in [
        (vec!["nonce-test"], None, Some("nonce-test")),
        (vec![], None, None),
        (vec![""], None, None),
        (vec!["one", "two"], None, None),
        (vec!["client"], Some("configured"), Some("configured")),
    ] {
        let upstream = StreamUpstream::start([Reply::json(ANSWER)]).await;
        let mut exchange = exchange(&upstream, "kimi-k3");
        for value in &values {
            exchange.headers.append("x-msh-request-nonce", value.parse().unwrap());
        }
        if let Some(value) = configured {
            exchange
                .credential
                .attributes
                .insert("header:X-Msh-Request-Nonce".into(), value.into());
        }
        let streams = Streams::new();
        let body = br#"{"model":"kimi-k3","input":"hi"}"#;
        let (started, id, _request) = run(&streams, &exchange, "responses", body, 17).await;
        finish(&streams, started.unwrap(), id).await.0.unwrap();
        assert_eq!(
            upstream.captured()[0].header("x-msh-request-nonce"),
            expected,
            "{values:?} configured {configured:?}"
        );
    }
}

/// K3 models take `reasoning_effort` and never the K2 `thinking` object; a level outside the
/// model's `low`/`high`/`max` list snaps to the nearest one, ties to the lower (Settled 59), on
/// both products and in every client dialect.
#[tokio::test]
async fn reasoning_follows_the_model_dialect_and_snaps_to_its_levels() {
    for (protocol, base, model, path, level, expected) in [
        ("chat", "/v1", "kimi-k3", "reasoning_effort", "high", Some("high")),
        ("chat", "/v1", "kimi-k3.1-pro", "reasoning_effort", "high", Some("high")),
        (
            "chat",
            "/v1",
            "kimi-k3.1-pro-highspeed",
            "reasoning_effort",
            "high",
            Some("high"),
        ),
        ("chat", "/coding", "kimi-k3", "reasoning_effort", "medium", Some("low")),
        ("chat", "/coding", "kimi-k3", "reasoning_effort", "xhigh", Some("high")),
        ("chat", "/coding", "kimi-k3", "reasoning_effort", "minimal", Some("low")),
        ("chat", "/coding", "kimi-k3", "reasoning_effort", "max", Some("max")),
        (
            "anthropic",
            "/coding",
            "kimi-k3",
            "output_config.effort",
            "medium",
            Some("low"),
        ),
        (
            "anthropic",
            "/coding",
            "kimi-k3",
            "output_config.effort",
            "xhigh",
            Some("high"),
        ),
        (
            "responses",
            "/coding",
            "kimi-k3",
            "reasoning.effort",
            "medium",
            Some("low"),
        ),
        (
            "responses",
            "/coding",
            "kimi-k3",
            "reasoning.effort",
            "xhigh",
            Some("high"),
        ),
        ("responses", "/coding", "kimi-k3", "reasoning.effort", "", None),
    ] {
        let mut body = input(protocol, model);
        if !level.is_empty() {
            let (head, tail) = path.split_once('.').map_or((path, None), |(h, t)| (h, Some(t)));
            body[head] = tail.map_or(json!(level), |t| json!({ t: level }));
            if protocol == "anthropic" {
                // Claude's effort is read only beside an adaptive or enabled thinking block.
                body["thinking"] = json!({"type":"adaptive"});
            }
        }
        let sent = serde_json::from_slice::<Value>(&upload(protocol, base, model, body).await.body).unwrap();
        assert_eq!(
            sent.pointer(&format!("/{}", path.replace('.', "/")))
                .and_then(Value::as_str),
            expected,
            "{protocol} {base} {model} {level}"
        );
        if protocol == "chat" && model.starts_with("kimi-k3") {
            assert!(sent.get("thinking").is_none(), "{model} sends no K2 thinking object");
        }
    }
}

/// A Responses custom tool call survives egress as the same item, buffered and streamed.
#[tokio::test]
async fn custom_tool_calls_survive_response_egress() {
    let call = json!({"id":"ct1","type":"custom_tool_call","call_id":"call1","name":"apply_patch","input":"*** Begin Patch\n*** End Patch","status":"completed"});
    let response =
        json!({"id":"r1","object":"response","created_at":1,"status":"completed","model":"kimi-k3","output":[call]});
    let events = [
        json!({"type":"response.output_item.added","sequence_number":0,"output_index":0,"item":{"id":"ct1","type":"custom_tool_call","call_id":"call1","name":"apply_patch","input":"","status":"in_progress"}}),
        json!({"type":"response.custom_tool_call_input.delta","sequence_number":1,"output_index":0,"item_id":"ct1","delta":call["input"]}),
        json!({"type":"response.output_item.done","sequence_number":2,"output_index":0,"item":call}),
        json!({"type":"response.completed","sequence_number":3,"response":response}),
    ];
    let sse: String = events
        .iter()
        .map(|e| format!("event: {}\ndata: {e}\n\n", e["type"].as_str().unwrap()))
        .collect();
    for stream in [false, true] {
        let reply = if stream {
            Reply::sse(sse.clone())
        } else {
            Reply::json(response.to_string())
        };
        let upstream = StreamUpstream::start([reply]).await;
        let exchange = exchange(&upstream, "kimi-k3");
        let streams = Streams::new();
        let body = json!({"model":"kimi-k3","stream":stream,"input":"hi","tool_choice":"auto","tools":[{"type":"custom","name":"apply_patch","format":{"type":"grammar","syntax":"lark","definition":"start: /.+/"}}]});
        let (started, id, _request) = run(&streams, &exchange, "responses", body.to_string().as_bytes(), 17).await;
        let (outcome, _, mut events) = finish(&streams, started.unwrap(), id).await;
        outcome.unwrap();
        let mode = if stream {
            BodyMode::Streaming
        } else {
            BodyMode::Buffered
        };
        let answer = egress(&streams, "responses", &mut events, id, mode).await;
        if stream {
            let items: Vec<Value> = answer
                .lines()
                .filter_map(|line| line.strip_prefix("data: "))
                .filter_map(|data| serde_json::from_str(data).ok())
                .collect();
            assert!(
                items
                    .iter()
                    .any(|e| e["type"] == "response.output_item.done" && e["item"] == call),
                "{answer}"
            );
            assert!(
                items
                    .iter()
                    .any(|e| e["type"] == "response.completed" && e["response"]["output"][0] == call)
            );
        } else {
            let answer: Value = serde_json::from_str(&answer).unwrap();
            assert_eq!(answer["output"][0], call);
        }
    }
}
