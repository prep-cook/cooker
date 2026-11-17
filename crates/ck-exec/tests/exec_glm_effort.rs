//! GLM effort fidelity on the legacy and execution-contract paths.
mod support;

use bytes::Bytes;
use ck_core::exec::{ExecRequest, Operation as LegacyOperation, ResolvedModel};
use ck_core::format::Format;
use ck_core::registry::{ModelInfo, ThinkingSupport};
use ck_exec::types::Execute;
use ck_exec::{claude::ClaudeExecutor, openai_compat::OpenAICompatExecutor, pipeline};
use ck_translate::{
    anthropic,
    codec::{Operation, chat, event::Direction},
};
use serde_json::{Value, json};
use support::upstream::{Reply, StreamUpstream};
use support::{Exchange, Streams, credential, fence, identity};

const LIMIT: u64 = 1 << 20;
const RESPONSE: &str = r#"{"id":"g1","created":1,"model":"glm-5.3","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}"#;
const MESSAGE: &str = r#"{"id":"g1","type":"message","role":"assistant","model":"glm-5.3","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":1}}"#;

fn legacy(exchange: &Exchange, body: &Value, format: Format) -> ExecRequest {
    let bytes = Bytes::from(serde_json::to_vec(body).unwrap());
    ExecRequest {
        operation: LegacyOperation::Generate,
        source_format: format,
        response_format: format,
        model: exchange.model.clone(),
        requested_model: "client-model".into(),
        body: bytes.clone(),
        original_body: bytes,
        stream: false,
        alt: None,
        session: None,
        execution_session: None,
        derived_session: None,
        request_path: exchange.route.clone(),
        headers: exchange.headers.clone(),
        caller: exchange.caller.clone(),
        resolved_model: None,
        usage: Default::default(),
    }
}

fn configured(model: &str) -> ResolvedModel {
    ResolvedModel {
        info: ModelInfo {
            id: model.into(),
            kind: "openai-compatibility".into(),
            thinking: Some(ThinkingSupport {
                levels: ck_wire::thinking::glm::default_levels(model),
                ..Default::default()
            }),
            raw: Default::default(),
        },
        source: ck_core::exec::ResolvedSource::ApiKey,
    }
}

#[tokio::test]
async fn glm_effort_snaps_to_supported_levels_before_send() {
    for (model, effort, expected) in [
        ("glm-5.3", "max", Some("max")),
        ("glm-5.3", "minimal", Some("low")),
        ("glm-5.3", "medium", Some("low")),
        ("glm-5.3", "xhigh", Some("high")),
        ("glm-5.3", "ultra", None),
        ("glm-5.3(high)", "medium", Some("high")),
        ("glm-5.3(medium)", "high", Some("low")),
        ("glm-5.3(xhigh)", "low", Some("high")),
        ("glm-5.3(max)", "low", Some("max")),
        ("glm-5.2", "xhigh", Some("xhigh")),
        ("glm-5.2", "minimal", Some("minimal")),
        ("glm-5.2", "max", Some("max")),
    ] {
        for contract in [false, true] {
            let upstream = StreamUpstream::start([Reply::json(RESPONSE)]).await;
            let exchange = Exchange::new(
                credential("openai-compatible-glm", &upstream.url(), "test"),
                model,
                "/v1/chat/completions",
            );
            let body =
                json!({"model":"client-model","messages":[{"role":"user","content":"hi"}],"reasoning_effort":effort});
            let executor = OpenAICompatExecutor::with_client(wreq::Client::new());
            let resolved = configured(model);
            let result = if contract {
                chat_contract(&executor, &exchange, &body, &resolved).await
            } else {
                let mut req = legacy(&exchange, &body, Format::OpenAI);
                req.resolved_model = Some(resolved);
                executor
                    .execute(&exchange.credential, req, &exchange.cfg)
                    .await
                    .map(|_| ())
                    .map_err(|e| format!("{e:?}"))
            };
            assert_eq!(
                result.is_ok(),
                expected.is_some(),
                "{model} {effort} contract={contract}: {result:?}"
            );
            assert_eq!(upstream.requests(), usize::from(expected.is_some()));
            if let Some(expected) = expected {
                let sent: Value = serde_json::from_slice(&upstream.captured()[0].body).unwrap();
                assert_eq!(sent["reasoning_effort"], expected, "{model} contract={contract}");
            }
        }
    }
}

async fn chat_contract(
    executor: &OpenAICompatExecutor,
    exchange: &Exchange,
    body: &Value,
    resolved: &ResolvedModel,
) -> Result<(), String> {
    let streams = Streams::new();
    let mut attempt = exchange.attempt(&streams.store);
    attempt.resolved_model = Some(resolved);
    let mut upload = streams.request();
    let mut ingress = chat::request::Ingress::new(
        &mut upload,
        &streams.store,
        identity(Direction::Request),
        Operation::Generate,
        LIMIT,
    )
    .unwrap();
    let bytes = serde_json::to_vec(body).unwrap();
    let validated =
        fence(&mut ingress, &mut upload, &bytes, 1, &executor.capability(&attempt)).map_err(|e| e.to_string())?;
    let response = streams.response(&upload);
    let started = pipeline::start(
        executor,
        &attempt,
        &mut exchange.request(validated),
        &mut upload,
        response,
    )
    .await
    .map_err(|e| format!("{e:?}"))?;
    started.producer.await.unwrap().map_err(|e| format!("{e:?}"))
}

#[tokio::test]
async fn messages_explicit_high_wins_over_disabled() {
    for contract in [false, true] {
        let upstream = StreamUpstream::start([Reply::json(MESSAGE)]).await;
        let exchange = Exchange::new(credential("claude", &upstream.url(), "test"), "glm-5.3", "/v1/messages");
        let body = json!({"model":"glm-5.3","max_tokens":64,"messages":[{"role":"user","content":"hi"}],"thinking":{"type":"disabled"},"output_config":{"effort":"high"}});
        let executor = ClaudeExecutor::new("http://127.0.0.1:1").unwrap();
        if contract {
            let streams = Streams::new();
            let attempt = exchange.attempt(&streams.store);
            let mut upload = streams.request();
            let mut ingress = anthropic::request::Ingress::new(
                &mut upload,
                &streams.store,
                identity(Direction::Request),
                Operation::Generate,
                LIMIT,
            )
            .unwrap();
            let bytes = serde_json::to_vec(&body).unwrap();
            let validated = fence(&mut ingress, &mut upload, &bytes, 1, &executor.capability(&attempt)).unwrap();
            let response = streams.response(&upload);
            let started = pipeline::start(
                &executor,
                &attempt,
                &mut exchange.request_with(validated, &bytes),
                &mut upload,
                response,
            )
            .await
            .unwrap();
            started.producer.await.unwrap().unwrap();
        } else {
            executor
                .execute(
                    &exchange.credential,
                    legacy(&exchange, &body, Format::Claude),
                    &exchange.cfg,
                )
                .await
                .unwrap();
        }
        let sent: Value = serde_json::from_slice(&upstream.captured()[0].body).unwrap();
        assert_eq!(sent["output_config"]["effort"], "high", "contract={contract}");
    }
}

#[tokio::test]
async fn legacy_network_error_is_a_failed_attempt() {
    use futures_util::StreamExt;
    for reason in ["network_error", "stop", "tool_calls", "length"] {
        let chunk = json!({"id":"g1","created":1,"model":"glm-5.3","choices":[{
            "index":0,"delta":{"role":"assistant","content":"partial"},"finish_reason":reason
        }],"usage":{"prompt_tokens":13,"completion_tokens":7,"total_tokens":20}});
        let upstream = StreamUpstream::start([Reply::sse(format!("data: {chunk}\n\ndata: [DONE]\n\n"))]).await;
        let exchange = Exchange::new(
            credential("openai-compatible-glm", &upstream.url(), "test"),
            "glm-5.3",
            "/v1/chat/completions",
        );
        let body = json!({"model":"glm-5.3","stream":true,"messages":[{"role":"user","content":"hi"}]});
        let mut req = legacy(&exchange, &body, Format::OpenAI);
        req.stream = true;
        let executor = OpenAICompatExecutor::with_client(wreq::Client::new());
        let response = executor
            .execute(&exchange.credential, req, &exchange.cfg)
            .await
            .unwrap();
        let ck_core::exec::ResponseBody::Stream(mut stream) = response.body else {
            panic!("expected stream")
        };
        let mut error = false;
        let mut output = Vec::new();
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => output.extend_from_slice(&bytes),
                Err(_) => error = true,
            }
        }
        assert_eq!(error, reason == "network_error", "{reason}");
        let output = String::from_utf8(output).unwrap();
        assert!(output.contains(reason), "{output}");
        assert!(output.contains("\"prompt_tokens\":13"), "{output}");
        assert_eq!(upstream.requests(), 1);
    }
}
