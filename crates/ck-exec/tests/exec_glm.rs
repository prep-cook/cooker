//! GLM conformance against loopback upstreams only.
mod support;

use ck_exec::openai_compat::OpenAICompatExecutor;
use ck_exec::pipeline;
use ck_exec::types::Execute;
use ck_stream::Terminal;
use ck_translate::codec::chat;
use ck_translate::codec::event::Direction;
use ck_translate::codec::{self, Operation};
use serde_json::{Value, json};
use support::upstream::{Reply, StreamUpstream};
use support::{Exchange, Streams, credential, fence, identity};

const LIMIT: u64 = 1 << 20;
const MODELS: &[&str] = &[
    "glm-5.3",
    "glm-5.3-flash",
    "glm-5.3-flashx",
    "glm-5.3-flashx-legal",
    "glm-5.2",
    "glm-5.1",
    "glm-5-turbo",
    "glm-5",
    "glm-4.7",
    "glm-4.7-flash",
    "glm-4.7-flashx",
    "glm-4.6",
    "glm-4.5-air",
    "glm-4.5-flash",
    "glm-4-flash-250414",
    "glm-4.6v",
    "glm-4.6v-flash",
];

fn exchange(upstream: &StreamUpstream, model: &str) -> Exchange {
    Exchange::new(
        credential("openai-compatible-glm", &upstream.url(), "test-key"),
        model,
        "/v1/chat/completions",
    )
}

fn request() -> Value {
    json!({"model":"client-model","stream":true,"messages":[{"role":"user","content":"hi"}],"tool_choice":"auto","response_format":{"type":"json_object"}})
}

fn reply(reason: &str, usage: bool, partial_tool: bool, buffered: bool) -> Reply {
    let message = if partial_tool {
        json!({"role":"assistant","tool_calls":[{"index":0,"id":"call1","type":"function",
            "function":{"name":"f","arguments":"{\"x\":"}}]})
    } else {
        json!({"role":"assistant","content":"partial"})
    };
    let mut chunk = json!({"id":"g1","created":1,"model":"glm-5.3","choices":[{
        "index":0,"finish_reason":reason
    }]});
    chunk["choices"][0][if buffered { "message" } else { "delta" }] = message;
    if usage {
        chunk["usage"] = json!({"prompt_tokens":13,"completion_tokens":7,"total_tokens":20});
    }
    if buffered {
        Reply::json(chunk.to_string())
    } else {
        Reply::sse(format!("data: {chunk}\n\ndata: [DONE]\n\n"))
    }
}

async fn rejected_candidate(model: &str, body: Value, capable_next: bool) {
    let glm = StreamUpstream::start([]).await;
    let capable = StreamUpstream::start([reply("stop", true, false, false)]).await;
    let streams = Streams::new();
    let executor = OpenAICompatExecutor::with_client(wreq::Client::new());
    let glm_exchange = exchange(&glm, model);
    let mut next = exchange(&capable, "other-model");
    next.credential.provider = "openai-compatibility".into();
    let mut upload = streams.request();
    let mut ingress = chat::request::Ingress::new(
        &mut upload,
        &streams.store,
        identity(Direction::Request),
        Operation::Generate,
        LIMIT,
    )
    .unwrap();
    let bytes = serde_json::to_vec(&body).unwrap();
    let initial = fence(&mut ingress, &mut upload, &bytes, 1, &chat::request::ChatCapability).unwrap();
    let attempt = glm_exchange.attempt(&streams.store);
    let refused = pipeline::refence(&executor, &attempt, initial.view());
    assert!(
        matches!(refused, Err(codec::Error::Unsupported)),
        "GLM must refuse before send"
    );
    if capable_next {
        let attempt = next.attempt(&streams.store);
        let validated = pipeline::refence(&executor, &attempt, initial.view()).unwrap();
        let response = streams.response(&upload);
        let started = pipeline::start(&executor, &attempt, &mut next.request(validated), &mut upload, response)
            .await
            .unwrap();
        started.producer.await.unwrap().unwrap();
        assert_eq!(capable.requests(), 1);
        let sent: Value = serde_json::from_slice(&capable.captured()[0].body).unwrap();
        for name in ["tool_choice", "response_format"] {
            if let Some(value) = body.get(name) {
                assert_eq!(&sent[name], value);
            }
        }
    } else {
        assert!(pipeline::refence(&executor, &attempt, initial.view()).is_err());
        assert_eq!(capable.requests(), 0);
    }
    assert_eq!(glm.requests(), 0);
}

#[tokio::test]
async fn forced_tools_skip_glm_without_relaxing_choice() {
    for (model, choice) in MODELS.iter().flat_map(|model| {
        [
            json!("required"),
            json!("none"),
            json!({"type":"function","function":{"name":"f"}}),
        ]
        .map(|choice| (*model, choice))
    }) {
        for capable_next in [false, true] {
            let mut body = request();
            body["tools"] = json!([{"type":"function","function":{"name":"f","parameters":{"type":"object"}}}]);
            body["tool_choice"] = choice.clone();
            rejected_candidate(model, body, capable_next).await;
        }
    }
}

#[tokio::test]
async fn json_schema_skips_glm_without_degrading_format() {
    for (model, capable_next) in MODELS.iter().flat_map(|model| [false, true].map(|next| (*model, next))) {
        let mut body = request();
        body["response_format"] =
            json!({"type":"json_schema","json_schema":{"name":"answer","schema":{"type":"object"}}});
        rejected_candidate(model, body, capable_next).await;
    }
}

#[tokio::test]
async fn glm_terminals_preserve_native_reason_and_known_usage() {
    for (model, usage) in MODELS
        .iter()
        .flat_map(|model| [false, true].map(|usage| (*model, usage)))
    {
        for (reason, bytewise, partial_tool, buffered) in [
            ("network_error", false, false, false),
            ("network_error", true, false, false),
            ("network_error", true, true, false),
            ("network_error", false, true, true),
            ("stop", false, false, false),
            ("tool_calls", false, false, false),
            ("length", false, false, false),
        ] {
            let failed = reason == "network_error";
            let response = reply(reason, usage, partial_tool, buffered);
            let upstream = StreamUpstream::start([if bytewise { response.byte_at_a_time() } else { response }]).await;
            let streams = Streams::new();
            let exchange = exchange(&upstream, model);
            let executor = OpenAICompatExecutor::with_client(wreq::Client::new());
            let attempt = exchange.attempt(&streams.store);
            let mut upload = streams.request();
            let mut ingress = chat::request::Ingress::new(
                &mut upload,
                &streams.store,
                identity(Direction::Request),
                Operation::Generate,
                LIMIT,
            )
            .unwrap();
            let mut body = request();
            body["stream"] = json!(!buffered);
            let bytes = serde_json::to_vec(&body).unwrap();
            let validated = fence(&mut ingress, &mut upload, &bytes, 1, &executor.capability(&attempt)).unwrap();
            let response = streams.response(&upload);
            let id = response.id();
            let started = pipeline::start(
                &executor,
                &attempt,
                &mut exchange.request(validated),
                &mut upload,
                response,
            )
            .await
            .unwrap();
            assert_eq!(started.producer.await.unwrap().is_err(), failed);
            let published = streams.read_to_end(id).await;
            let terminal = if failed {
                Terminal::UpstreamError
            } else {
                Terminal::Done
            };
            assert_eq!(published.terminal, Some(terminal));
            let records = String::from_utf8(published.bytes()).unwrap();
            assert!(records.contains(reason), "{records}");
            assert_eq!(
                records.matches("event: x_ck.error").count(),
                usize::from(failed),
                "{records}"
            );
            assert_eq!(records.contains("event: x_ck.done"), !failed, "{records}");
            if usage {
                assert!(records.contains("\"input_tokens\":13"), "{records}");
                assert!(records.contains("\"output_tokens\":7"), "{records}");
            } else {
                assert!(!records.contains("\"input_tokens\":0"), "{records}");
            }
            assert_eq!(upstream.requests(), 1);
            let sent: Value = serde_json::from_slice(&upstream.captured()[0].body).unwrap();
            assert_eq!(sent["tool_choice"], "auto");
            assert_eq!(sent["response_format"], json!({"type":"json_object"}));
            if !buffered {
                let delivered = deliver(&streams, started.stream, id).await;
                assert!(
                    delivered.contains(&format!("\"finish_reason\":\"{reason}\"")),
                    "{delivered}"
                );
                assert_eq!(delivered.contains("\"error\""), failed, "{delivered}");
            }
        }
    }
}

async fn deliver(streams: &Streams, mut stream: ck_stream::Writer, id: u64) -> String {
    use ck_translate::codec::ResponseEgress;
    let mut egress = chat::egress::Egress::new(
        &mut stream,
        identity(Direction::Response),
        Operation::Generate,
        codec::BodyMode::Streaming,
        true,
        |_| {},
    )
    .unwrap();
    let mut reader = streams.reader(id);
    let mut output = Vec::new();
    while let Some(read) = reader.next().await {
        if let ck_stream::Read::Record(record) = read {
            egress
                .advance(&mut stream, record, |bytes| {
                    let mut offset = 0;
                    while offset < bytes.len() {
                        let chunk = bytes.chunk(offset)?;
                        output.extend_from_slice(chunk);
                        offset += chunk.len();
                    }
                    Ok(())
                })
                .unwrap();
        }
    }
    egress.finish().unwrap();
    String::from_utf8(output).unwrap()
}
