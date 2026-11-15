use super::*;
use ck_core::exec::ExecError;
use serde_json::json;

async fn legacy(exchange: &Exchange, body: &[u8], source: Format) -> Result<(), ExecError> {
    let request = ExecRequest {
        operation: ExecOperation::Generate,
        source_format: source,
        response_format: source,
        requested_model: "client-model".into(),
        model: exchange.model.clone(),
        original_body: Bytes::copy_from_slice(body),
        body: Bytes::copy_from_slice(body),
        stream: true,
        alt: None,
        session: None,
        execution_session: None,
        derived_session: None,
        request_path: exchange.route.clone(),
        headers: exchange.headers.clone(),
        caller: exchange.caller.clone(),
        resolved_model: None,
        usage: Default::default(),
    };
    let response = MetaExecutor::with_client(wreq::Client::new())
        .execute(&exchange.credential, request, &exchange.cfg)
        .await?;
    if let ck_core::exec::ResponseBody::Stream(mut events) = response.body {
        while let Some(event) = events.next().await {
            event?;
        }
    }
    Ok(())
}

async fn sent(old: bool, protocol: &str, options: Value) -> Value {
    let mut body = match protocol {
        "chat" => json!({"model":"client-model","messages":[{"role":"user","content":"hi"}]}),
        _ => json!({"model":"client-model","input":"hi"}),
    };
    body.as_object_mut()
        .unwrap()
        .extend(options.as_object().unwrap().clone());
    let body = serde_json::to_vec(&body).unwrap();
    let upstream = StreamUpstream::start([Reply::sse(sse())]).await;
    let exchange = exchange(&upstream);
    if old {
        let source = if protocol == "chat" {
            Format::OpenAI
        } else {
            Format::OpenAIResponse
        };
        legacy(&exchange, &body, source).await.unwrap();
    } else {
        let streams = Streams::new();
        let (started, id, _request) = run(&streams, &exchange, protocol, Operation::Generate, &body, 7).await;
        finish(&streams, started.unwrap(), id).await.0.unwrap();
    }
    json_body(&upstream.captured()[0])
}

async fn preserved(old: bool, protocol: &str, cases: &[Value]) {
    for options in cases {
        let actual = sent(old, protocol, options.clone()).await;
        for (key, expected) in options.as_object().unwrap() {
            assert_eq!(&actual[key], expected, "{protocol} {key}: {actual}");
        }
    }
}

async fn rejected(old: bool, protocol: &str, options: Value) {
    let mut body = if protocol == "chat" {
        json!({"model":"client-model","messages":[{"role":"user","content":"hi"}]})
    } else {
        json!({"model":"client-model","input":"hi"})
    };
    body.as_object_mut()
        .unwrap()
        .extend(options.as_object().unwrap().clone());
    let body = serde_json::to_vec(&body).unwrap();
    let upstream = StreamUpstream::start([Reply::sse(sse())]).await;
    if old {
        let source = if protocol == "chat" {
            Format::OpenAI
        } else {
            Format::OpenAIResponse
        };
        let error = legacy(&exchange(&upstream), &body, source)
            .await
            .expect_err("unsupported semantics");
        assert_eq!(error.status, 400);
    } else {
        assert!(
            fenced(protocol, Operation::Generate, &body).is_err(),
            "unsupported semantics accepted"
        );
    }
    assert!(upstream.captured().is_empty(), "refusal must precede sending");
}

async fn m3(old: bool) {
    preserved(old, "responses", &[json!({"max_output_tokens":64})]).await;
    for field in ["max_tokens", "max_completion_tokens"] {
        let actual = sent(old, "chat", json!({field:96})).await;
        assert_eq!(actual["max_output_tokens"], 96, "{field}: {actual}");
    }
}
async fn m4(old: bool) {
    for protocol in ["responses", "chat"] {
        preserved(old, protocol, &[json!({"temperature":0.25,"top_p":0.75})]).await;
    }
}
async fn m6(old: bool) {
    preserved(
        old,
        "responses",
        &[
            json!({"include":[]}),
            json!({"include":["web_search_call.results","message.input_image.image_url"]}),
            json!({"include":["unsupported_value"]}),
        ],
    )
    .await;
}
async fn m7(old: bool) {
    for protocol in ["responses", "chat"] {
        preserved(old, protocol, &[json!({"parallel_tool_calls":false})]).await;
        let function = json!({"name":"lookup","parameters":{"type":"object","properties":{}}});
        let tool = if protocol == "chat" {
            json!({"type":"function","function":function})
        } else {
            json!({"type":"function","name":"lookup","parameters":{"type":"object","properties":{}}})
        };
        let actual = sent(old, protocol, json!({"parallel_tool_calls":false,"tools":[tool]})).await;
        assert_eq!(actual["parallel_tool_calls"], false);
        assert_eq!(actual["tools"][0]["name"], "lookup");
    }
}
async fn m8(old: bool) {
    for protocol in ["responses", "chat"] {
        for tier in ["auto", "default", "flex", "scale", "priority"] {
            preserved(old, protocol, &[json!({"service_tier":tier})]).await;
        }
    }
}
async fn m9(old: bool) {
    rejected(old, "responses", json!({"truncation":"auto"})).await;
    preserved(old, "responses", &[json!({"truncation":"disabled"})]).await;
}
async fn m10(old: bool) {
    for protocol in ["responses", "chat"] {
        preserved(
            old,
            protocol,
            &[json!({"prompt_cache_key":"cache-1", "prompt_cache_retention":"24h",
            "safety_identifier":"user-safe", "user":"user-1"})],
        )
        .await;
    }
}
async fn m14(old: bool) {
    for options in [
        json!({"n":2}),
        json!({"logprobs":true}),
        json!({"max_output_tokens":64}),
    ] {
        rejected(old, "chat", options).await;
    }
}

macro_rules! regression {
    ($name:ident, $item:ident, $old:expr) => {
        #[tokio::test]
        async fn $name() {
            $item($old).await;
        }
    };
}
regression!(f35_m3_old, m3, true);
regression!(f35_m3_new, m3, false);
regression!(f35_m4_old, m4, true);
regression!(f35_m4_new, m4, false);
regression!(f35_m6_old, m6, true);
regression!(f35_m6_new, m6, false);
regression!(f35_m7_old, m7, true);
regression!(f35_m7_new, m7, false);
regression!(f35_m8_old, m8, true);
regression!(f35_m8_new, m8, false);
regression!(f35_m9_old, m9, true);
regression!(f35_m9_new, m9, false);
regression!(f35_m10_old, m10, true);
regression!(f35_m10_new, m10, false);
regression!(f35_m14_old, m14, true);
regression!(f35_m14_new, m14, false);

#[tokio::test]
async fn f35_absence_stays_unknown() {
    for old in [true, false] {
        for protocol in ["responses", "chat"] {
            let actual = sent(old, protocol, json!({})).await;
            for field in [
                "max_output_tokens",
                "temperature",
                "top_p",
                "include",
                "parallel_tool_calls",
                "service_tier",
                "prompt_cache_retention",
                "safety_identifier",
                "user",
                "reasoning",
            ] {
                assert!(actual.get(field).is_none(), "{protocol} old={old} {field}: {actual}");
            }
        }
    }
}

#[tokio::test]
async fn f35_invalid_options_keep_upstream_validation() {
    for old in [true, false] {
        for options in [
            json!({"temperature":5}),
            json!({"top_p":5}),
            json!({"include":["not_supported"]}),
        ] {
            let mut body = json!({"model":"client-model","input":"hi"});
            body.as_object_mut()
                .unwrap()
                .extend(options.as_object().unwrap().clone());
            let body = serde_json::to_vec(&body).unwrap();
            let reply = Reply::status(
                400,
                "application/json",
                r#"{"error":{"message":"invalid option","type":"invalid_request_error"}}"#,
            );
            let upstream = StreamUpstream::start([reply]).await;
            let exchange = exchange(&upstream);
            let error = if old {
                legacy(&exchange, &body, Format::OpenAIResponse).await.unwrap_err()
            } else {
                let streams = Streams::new();
                let (started, _, _request) = run(&streams, &exchange, "responses", Operation::Generate, &body, 7).await;
                started.err().expect("upstream validation must remain a refusal")
            };
            assert_eq!(error.status, 400);
            let actual = json_body(&upstream.captured()[0]);
            for (key, value) in options.as_object().unwrap() {
                assert_eq!(&actual[key], value);
            }
        }
    }
}

#[tokio::test]
async fn f35_patch_bridge_preserves_explicit_controls() {
    let options = json!({"tools":[{"type":"custom","name":"apply_patch"}],
        "max_output_tokens":64,"temperature":0.25,"parallel_tool_calls":false,
        "include":[],"service_tier":"flex","prompt_cache_retention":"24h","safety_identifier":"u1"});
    let actual = sent(true, "responses", options.clone()).await;
    assert_eq!(actual["tools"][0]["type"], "function");
    assert_eq!(actual["tools"][0]["name"], "apply_patch");
    for (key, value) in options.as_object().unwrap() {
        if key != "tools" {
            assert_eq!(&actual[key], value, "{key}");
        }
    }
}

#[test]
fn f35_meta_extensions_do_not_leak_to_other_destinations() {
    use ck_translate::codec::RequestEgress;
    for destination in ["meta", "responses"] {
        let streams = Streams::new();
        let mut request = streams.request();
        let mut ingress = chat::request::Ingress::new(
            &mut request,
            &streams.store,
            identity(Direction::Request),
            Operation::Generate,
            LIMIT,
        )
        .unwrap();
        let body = br#"{"model":"m","messages":[{"role":"user","content":"hi"}],"prompt_cache_retention":"24h","foreign_provider_field":42}"#;
        let capability = if destination == "meta" {
            fence(&mut ingress, &mut request, body, 7, &MetaCapability).unwrap()
        } else {
            fence(
                &mut ingress,
                &mut request,
                body,
                7,
                &responses::request::ResponsesCapability,
            )
            .unwrap()
        };
        let encoded = responses::request::Egress {
            destination,
            upstream_stream: true,
        }
        .encode(&capability, &mut request)
        .unwrap();
        let actual: Value = serde_json::from_slice(&lease_bytes(&encoded)).unwrap();
        assert_eq!(
            actual.get("prompt_cache_retention"),
            (destination == "meta").then_some(&json!("24h"))
        );
        assert!(actual.get("foreign_provider_field").is_none());
    }
}

#[tokio::test]
async fn f35_foreign_clients_keep_explicit_controls_without_reasoning_defaults() {
    for old in [true, false] {
        for (protocol, source, body) in [
            (
                "anthropic",
                Format::Claude,
                json!({"model":"m","messages":[{"role":"user","content":"hi"}],
                "max_tokens":96,"temperature":0.25,"top_p":0.75}),
            ),
            (
                "gemini",
                Format::Gemini,
                json!({"contents":[{"role":"user","parts":[{"text":"hi"}]}],
                "generationConfig":{"maxOutputTokens":96,"temperature":0.25,"topP":0.75}}),
            ),
        ] {
            let upstream = StreamUpstream::start([Reply::sse(sse())]).await;
            let exchange = exchange(&upstream);
            let body = serde_json::to_vec(&body).unwrap();
            if old {
                legacy(&exchange, &body, source).await.unwrap();
            } else {
                let streams = Streams::new();
                let (started, id, _request) = run(&streams, &exchange, protocol, Operation::Generate, &body, 7).await;
                finish(&streams, started.unwrap(), id).await.0.unwrap();
            }
            let actual = json_body(&upstream.captured()[0]);
            assert_eq!(actual["max_output_tokens"], 96, "{protocol} old={old}");
            assert_eq!(actual["temperature"], 0.25, "{protocol} old={old}");
            assert_eq!(actual["top_p"], 0.75, "{protocol} old={old}");
            assert!(actual.get("reasoning").is_none(), "{protocol} old={old}: {actual}");
        }
    }
}

#[tokio::test]
async fn f35_preserves_the_separate_legacy_interactions_converter() {
    let upstream = StreamUpstream::start([Reply::sse(sse())]).await;
    let body = br#"{"model":"m","input":"hi","generation_config":{"max_output_tokens":96,"temperature":0.25}}"#;
    legacy(&exchange(&upstream), body, Format::Interactions).await.unwrap();
    let actual = json_body(&upstream.captured()[0]);
    assert_eq!(actual["max_output_tokens"], 96);
    assert_eq!(actual["temperature"], 0.25);
    assert!(actual.get("reasoning").is_none());
}

#[tokio::test]
async fn f35_legal_chat_controls_old_and_new() {
    for old in [true, false] {
        let actual = sent(old, "chat", json!({"n":1,"logprobs":false,"max_completion_tokens":64})).await;
        assert_eq!(actual["max_output_tokens"], 64);
        assert_eq!(actual["input"][0]["content"][0]["text"], "hi");
        assert!(actual.get("n").is_none());
        assert!(actual.get("logprobs").is_none());
    }
}

#[test]
fn f35_single_choice_does_not_relax_responses_capability() {
    let streams = Streams::new();
    let mut request = streams.request();
    let mut ingress = chat::request::Ingress::new(
        &mut request,
        &streams.store,
        identity(Direction::Request),
        Operation::Generate,
        LIMIT,
    )
    .unwrap();
    let body = br#"{"model":"m","messages":[{"role":"user","content":"hi"}],"n":1,"logprobs":false}"#;
    assert!(
        fence(
            &mut ingress,
            &mut request,
            body,
            7,
            &responses::request::ResponsesCapability
        )
        .is_err()
    );
}
