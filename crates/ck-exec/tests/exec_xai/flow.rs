//! Generation from every client protocol, xAI's line rules, any split of the body, the
//! local count, the native compact and the selector.

use super::*;

const CHAT: &str = r#"{"model":"grok-4.3","stream":true,"max_tokens":64,"messages":[{"role":"user","content":"hi"}]}"#;
const RESPONSES: &str = r#"{"model":"grok-4.3","stream":true,"input":"hi"}"#;
const ANTHROPIC: &str =
    r#"{"model":"grok-4.3","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
const GEMINI: &str = r#"{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}"#;

async fn legacy(upstream: &StreamUpstream, client: Client, body: &str, stream: bool) -> Result<Vec<u8>, ExecError> {
    use ck_core::{
        exec::{ExecRequest, ResponseBody},
        format::Format,
    };
    use futures_util::StreamExt;
    let format = match client {
        Client::Chat => Format::OpenAI,
        Client::Responses => Format::OpenAIResponse,
        Client::Anthropic => Format::Claude,
        Client::Gemini => Format::Gemini,
    };
    let request = ExecRequest {
        operation: ck_core::exec::Operation::Generate,
        source_format: format,
        response_format: format,
        requested_model: MODEL.into(),
        model: MODEL.into(),
        original_body: bytes::Bytes::new(),
        body: bytes::Bytes::from(body.to_owned()),
        stream,
        alt: None,
        session: None,
        execution_session: None,
        derived_session: None,
        resolved_model: None,
        usage: Default::default(),
        request_path: "/v1/responses".into(),
        headers: http::HeaderMap::new(),
        caller: ck_core::exec::Caller {
            principal: String::new(),
            source: "authorization",
            provider: String::new(),
        },
    };
    let response = executors()
        .execute(
            &credential(&format!("{}/v1", upstream.url())),
            request,
            &Config::default(),
        )
        .await?;
    match response.body {
        ResponseBody::Buffered(body) => Ok(body.to_vec()),
        ResponseBody::Stream(mut chunks) => {
            let mut body = Vec::new();
            while let Some(chunk) = chunks.next().await {
                body.extend_from_slice(&chunk?);
            }
            Ok(body)
        }
    }
}

#[tokio::test]
async fn request_controls_are_preserved_or_refused_before_send() {
    for client in [Client::Chat, Client::Responses, Client::Anthropic, Client::Gemini] {
        let controls: &[(Value, bool)] = match client {
            Client::Chat | Client::Responses => &[
                (json!({"safety_identifier":"hashed-user"}), true),
                (json!({"parallel_tool_calls":false}), true),
                (
                    json!({"search_parameters":{"mode":"on","sources":[{"type":"x"}]}}),
                    true,
                ),
                (json!({"n":2}), false),
                (json!({"seed":42}), false),
                (json!({"stop":["END"]}), false),
                (json!({"web_search_options":{"search_context_size":"high"}}), false),
            ],
            Client::Anthropic => &[(json!({"stop_sequences":["END"]}), false)],
            Client::Gemini => &[
                (json!({"generationConfig":{"stopSequences":["END"]}}), false),
                (json!({"generationConfig":{"seed":42}}), false),
                (json!({"generationConfig":{"candidateCount":2}}), false),
            ],
        };
        for (control, accepted) in controls {
            let mut body: Value = serde_json::from_str(match client {
                Client::Chat => CHAT,
                Client::Responses => RESPONSES,
                Client::Anthropic => ANTHROPIC,
                Client::Gemini => GEMINI,
            })
            .unwrap();
            if matches!(client, Client::Chat | Client::Responses) {
                body["tools"] = match client {
                    Client::Chat => json!([{"type":"function","function":{"name":"f","parameters":{"type":"object"}}}]),
                    _ => json!([{"type":"function","name":"f","parameters":{"type":"object"}}]),
                };
            }
            body.as_object_mut()
                .unwrap()
                .extend(control.as_object().unwrap().clone());
            for old in [false, true] {
                let streams = Streams::new();
                let upstream = StreamUpstream::start([Reply::sse(sse())]).await;
                let result = if old {
                    legacy(&upstream, client, &body.to_string(), true).await.map(|_| ())
                } else {
                    match run(&streams, &upstream, client, &body.to_string(), 7).await.started {
                        Ok(started) => produce(&streams, started).await.0,
                        Err(error) => Err(error),
                    }
                };
                if *accepted {
                    result.unwrap();
                    let sent = json_body(&upstream.captured()[0]);
                    for (key, value) in control.as_object().unwrap() {
                        assert_eq!(&sent[key], value, "{client:?}: {key}");
                    }
                } else {
                    assert!(result.is_err(), "{client:?}: {control}");
                    assert!(upstream.captured().is_empty(), "{client:?}: {control}");
                }
            }
        }
    }
}

#[tokio::test]
async fn function_tools_are_never_truncated() {
    for client in [Client::Chat, Client::Responses] {
        for count in [201, 1000, 1001] {
            let mut body: Value = serde_json::from_str(match client {
                Client::Chat => CHAT,
                _ => RESPONSES,
            })
            .unwrap();
            let tools: Vec<Value> = (0..count)
                .map(|i| {
                    let function = json!({"name":format!("f{i}"),"parameters":{"type":"object"}});
                    match client {
                        Client::Chat => json!({"type":"function","function":function}),
                        _ => json!({"type":"function","name":function["name"],"parameters":function["parameters"]}),
                    }
                })
                .collect();
            body["tools"] = json!(tools);
            for old in [false, true] {
                let streams = Streams::with_limits(4096, 64 * 1024 * 1024);
                let upstream = StreamUpstream::start([Reply::sse(sse())]).await;
                let result = if old {
                    legacy(&upstream, client, &body.to_string(), true).await.map(|_| ())
                } else {
                    match run(&streams, &upstream, client, &body.to_string(), 8192).await.started {
                        Ok(started) => produce(&streams, started).await.0,
                        Err(error) => Err(error),
                    }
                };
                if count > 1000 {
                    assert!(result.is_err(), "{client:?}: {count}");
                    assert!(upstream.captured().is_empty());
                } else {
                    result.unwrap();
                    let sent = json_body(&upstream.captured()[0]);
                    let names: Vec<_> = sent["tools"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|t| t["name"].as_str().unwrap())
                        .collect();
                    let expected: Vec<_> = (0..count).map(|i| format!("f{i}")).collect();
                    assert_eq!(names, expected, "{client:?}: {count}");
                }
            }
        }
    }
}

/// Every client protocol goes upstream as a streamed Responses request with the API key, and
/// every client egress accepts the canonical response, streamed and buffered. A client that
/// does not stream still gets a streamed upstream.
#[tokio::test]
async fn every_client_protocol_streams_upstream_and_comes_back() {
    for (client, body) in [
        (Client::Chat, CHAT),
        (Client::Responses, RESPONSES),
        (Client::Anthropic, ANTHROPIC),
        (Client::Gemini, GEMINI),
        (
            Client::Chat,
            r#"{"model":"m","stream":false,"messages":[{"role":"user","content":"hi"}]}"#,
        ),
    ] {
        let streams = Streams::new();
        let upstream = StreamUpstream::start([Reply::sse(sse())]).await;
        let run = run(&streams, &upstream, client, body, 5).await;
        let (outcome, published, mut owner) = produce(&streams, run.started.unwrap()).await;
        outcome.unwrap();
        assert_eq!(published.terminal, Some(Terminal::Done), "{client:?}");
        let captured = &upstream.captured()[0];
        assert_eq!(
            (captured.method.as_str(), captured.path.as_str()),
            ("POST", "/v1/responses")
        );
        assert_eq!(captured.header("authorization"), Some("Bearer xai-test-key"));
        assert_eq!(captured.header("accept"), Some("text/event-stream"));
        assert_eq!(captured.header("connection"), Some("Keep-Alive"));
        let sent = json_body(captured);
        assert_eq!(sent["model"], MODEL, "{client:?}: {sent}");
        assert_eq!(sent["stream"], true, "{client:?}: {sent}");
        assert_eq!(sent["store"], false, "{client:?}: {sent}");
        assert_eq!(
            sent["include"],
            json!(["reasoning.encrypted_content"]),
            "{client:?}: {sent}"
        );
        assert!(sent["input"].to_string().contains("hi"), "{client:?}: {sent}");
        for mode in [BodyMode::Streaming, BodyMode::Buffered] {
            assert_every_egress(&streams, &mut owner, mode);
        }
    }
}

fn assert_every_egress(streams: &Streams, owner: &mut Writer, mode: BodyMode) {
    let id = identity(Direction::Response);
    let op = Operation::Generate;
    let mut out = chat::egress::Egress::new(owner, id, op, mode, true, |_| {}).unwrap();
    let text = deliver(streams, owner, &mut out);
    assert!(text.contains("hel") && text.contains("stop"), "chat: {text}");
    let mut out = anthropic::egress::Egress::new(owner, id, op, mode, |_| {}).unwrap();
    let text = deliver(streams, owner, &mut out);
    assert!(text.contains("hel") && text.contains("end_turn"), "anthropic: {text}");
    let mut out = crate::support::responses_egress(owner, op, mode).unwrap();
    let text = deliver(streams, owner, &mut out);
    assert!(text.contains("hel") && text.contains("completed"), "responses: {text}");
    let mut out = gemini::egress::Egress::new(owner, id, op, mode, |_| {}).unwrap();
    let text = deliver(streams, owner, &mut out);
    assert!(text.contains("hel") && text.contains("STOP"), "gemini: {text}");
}

/// The canonical stream does not depend on how the body was split, a completion whose
/// `output` is empty included.
#[tokio::test]
async fn any_split_gives_the_same_records() {
    for body in [sse(), sse_with(json!([]))] {
        let mut seen = Vec::new();
        for reply in [Reply::sse(body.clone()), Reply::sse(body.clone()).byte_at_a_time()] {
            let streams = Streams::new();
            let upstream = StreamUpstream::start([reply]).await;
            let run = run(&streams, &upstream, Client::Responses, RESPONSES, 64).await;
            let (outcome, published, _) = produce(&streams, run.started.unwrap()).await;
            outcome.unwrap();
            seen.push(records(&published));
        }
        assert_eq!(seen[0], seen[1]);
    }
}

/// A completion with an empty `output` (as Grok sends it), or none, gets the announced items,
/// from the response stream's scratch, so a buffered client sees its message.
#[tokio::test]
async fn an_empty_completion_gets_the_announced_items() {
    let mut absent = response("completed", json!([]));
    absent.as_object_mut().unwrap().remove("output");
    let without = message_events() + &event(json!({"type":"response.completed","response":absent}));
    for body in [sse_with(json!([])), without] {
        let streams = Streams::new();
        let upstream = StreamUpstream::start([Reply::sse(body)]).await;
        let run = run(&streams, &upstream, Client::Responses, RESPONSES, 64).await;
        let (outcome, _, mut owner) = produce(&streams, run.started.unwrap()).await;
        outcome.unwrap();
        let out = responses_json(&streams, &mut owner);
        assert_eq!(out["output"], json!([message("hello")]), "{out}");
    }
}

/// Native reasoning text keeps its event type and content index.
#[tokio::test]
async fn reasoning_text_keeps_native_content() {
    let reasoning = json!({"type":"reasoning","id":"rs_1","summary":[],"content":[{"type":"reasoning_text","text":"think"}],"encrypted_content":"enc"});
    let body = [
        event(json!({"type":"response.created","response":response("in_progress", json!([]))})),
        event(
            json!({"type":"response.output_item.added","output_index":0,"item":{"type":"reasoning","id":"rs_1","summary":[]}}),
        ),
        event(
            json!({"type":"response.reasoning_text.delta","item_id":"rs_1","output_index":0,"content_index":0,"delta":"think"}),
        ),
        event(
            json!({"type":"response.reasoning_text.done","item_id":"rs_1","output_index":0,"content_index":0,"text":"think"}),
        ),
        event(json!({"type":"response.output_item.done","output_index":0,"item":reasoning})),
        event(json!({"type":"response.completed","response":response("completed", json!([reasoning]))})),
    ]
    .concat();
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::sse(body)]).await;
    let run = run(&streams, &upstream, Client::Responses, RESPONSES, 64).await;
    let (outcome, _, mut owner) = produce(&streams, run.started.unwrap()).await;
    outcome.unwrap();
    let out = responses_json(&streams, &mut owner);
    assert_eq!(out["output"][0]["content"], reasoning["content"], "{out}");
    let mut egress = support::responses_egress(&mut owner, Operation::Generate, BodyMode::Streaming).unwrap();
    let text = deliver(&streams, &mut owner, &mut egress);
    let events: Vec<Value> = text
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter_map(|data| serde_json::from_str(data).ok())
        .collect();
    for kind in ["response.reasoning_text.delta", "response.reasoning_text.done"] {
        let event = events.iter().find(|event| event["type"] == kind).expect(kind);
        assert_eq!(event["content_index"], 0);
        assert!(event.get("summary_index").is_none());
    }
}

#[tokio::test]
async fn x_search_traces_remain_server_tools() {
    for name in [
        "x_keyword_search",
        "x_semantic_search",
        "x_user_search",
        "x_thread_fetch",
    ] {
        for (kind, field, delta_type) in [
            ("custom_tool_call", "input", "response.custom_tool_call_input.delta"),
            ("function_call", "arguments", "response.function_call_arguments.delta"),
        ] {
            let item = json!({"type":kind,"id":"xs_1","call_id":"xs_call_1","name":name,field:"{\"query\":\"cooker\"}","status":"completed"});
            let body = [
            event(json!({"type":"response.created","response":response("in_progress", json!([]))})),
            event(json!({"type":"response.output_item.added","output_index":0,"item":{"type":kind,"id":"xs_1","call_id":"xs_call_1","name":name,field:"","status":"in_progress"}})),
            event(json!({"type":delta_type,"output_index":0,"item_id":"xs_1","delta":"{\"query\":\"cooker\"}"})),
            event(json!({"type":format!("{}done", delta_type.trim_end_matches("delta")),"output_index":0,"item_id":"xs_1",field:"{\"query\":\"cooker\"}"})),
            event(json!({"type":"response.output_item.done","output_index":0,"item":item})),
            event(json!({"type":"response.completed","response":response("completed", json!([item]))})),
        ].concat();
            let streams = Streams::new();
            let upstream = StreamUpstream::start([Reply::sse(body.clone())]).await;
            let run = run(
                &streams,
                &upstream,
                Client::Responses,
                r#"{"model":"grok-4.3","input":"search","tools":[{"type":"x_search"}]}"#,
                7,
            )
            .await;
            let (result, published, mut owner) = produce(&streams, run.started.unwrap()).await;
            result.unwrap();
            let canonical = records(&published).join("\n");
            assert!(canonical.contains("\"kind\":\"server\""), "{canonical}");
            let out = responses_json(&streams, &mut owner);
            assert_eq!(out["output"][0]["type"], kind);
            assert_eq!(out["output"][0]["name"], name);
            assert_eq!(out["output"][0][field], item[field]);
            let mut egress = support::responses_egress(&mut owner, Operation::Generate, BodyMode::Streaming).unwrap();
            let text = deliver(&streams, &mut owner, &mut egress);
            assert!(text.contains(delta_type), "{text}");
            let mut egress = chat::egress::Egress::new(
                &mut owner,
                identity(Direction::Response),
                Operation::Generate,
                BodyMode::Buffered,
                true,
                |_| {},
            )
            .unwrap();
            let out: Value = serde_json::from_str(&deliver(&streams, &mut owner, &mut egress)).unwrap();
            assert!(out["choices"][0]["message"]["tool_calls"].is_null(), "{out}");
            assert_ne!(out["choices"][0]["finish_reason"], "tool_calls");
            let mut egress = gemini::egress::Egress::new(
                &mut owner,
                identity(Direction::Response),
                Operation::Generate,
                BodyMode::Buffered,
                |_| {},
            )
            .unwrap();
            let out = deliver(&streams, &mut owner, &mut egress);
            assert!(!out.contains("functionCall"), "{out}");
            for client in [Client::Responses, Client::Chat] {
                for stream in [false, true] {
                    let upstream = StreamUpstream::start([Reply::sse(body.clone())]).await;
                    let request = match client {
                        Client::Responses => r#"{"model":"grok-4.3","input":"search","tools":[{"type":"x_search"}]}"#,
                        _ => {
                            r#"{"model":"grok-4.3","messages":[{"role":"user","content":"search"}],"search_parameters":{"mode":"on"}}"#
                        }
                    };
                    let out = String::from_utf8(legacy(&upstream, client, request, stream).await.unwrap()).unwrap();
                    if matches!(client, Client::Responses) {
                        assert!(out.contains(name), "{out}");
                        if stream {
                            assert!(out.contains(delta_type), "{out}");
                        } else {
                            let out: Value = serde_json::from_str(&out).unwrap();
                            assert_eq!(out["output"][0], item);
                        }
                    } else {
                        assert!(!out.contains(name), "server call became a client tool: {out}");
                        assert!(!out.contains("\"finish_reason\":\"tool_calls\""), "{out}");
                        let chunks: Vec<Value> = if stream {
                            out.lines()
                                .filter_map(|line| line.strip_prefix("data: "))
                                .filter_map(|data| serde_json::from_str(data).ok())
                                .collect()
                        } else {
                            vec![serde_json::from_str(&out).unwrap()]
                        };
                        for chunk in chunks {
                            assert!(chunk["choices"][0]["delta"]["tool_calls"].is_null(), "{chunk}");
                            assert!(chunk["choices"][0]["message"]["tool_calls"].is_null(), "{chunk}");
                        }
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn chat_usage_keeps_cost_and_search_extensions() {
    let extensions = json!({"cost_in_usd_ticks":1234,"cost_in_nano_usd":123,"num_sources_used":3,"num_server_side_tools_used":1,"server_side_tool_usage_details":{"x_search_calls":1,"x_posts_fetched":3}});
    let mut terminal = response("completed", json!([message("hello")]));
    terminal["usage"]
        .as_object_mut()
        .unwrap()
        .extend(extensions.as_object().unwrap().clone());
    let body = message_events() + &event(json!({"type":"response.completed","response":terminal}));
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::sse(body.clone())]).await;
    let run = run(&streams, &upstream, Client::Chat, CHAT, 7).await;
    let (result, _, mut owner) = produce(&streams, run.started.unwrap()).await;
    result.unwrap();
    for mode in [BodyMode::Buffered, BodyMode::Streaming] {
        for old in [false, true] {
            let text = if old {
                let upstream = StreamUpstream::start([Reply::sse(body.clone())]).await;
                String::from_utf8(
                    legacy(&upstream, Client::Chat, CHAT, mode == BodyMode::Streaming)
                        .await
                        .unwrap(),
                )
                .unwrap()
            } else {
                let mut egress = chat::egress::Egress::new(
                    &mut owner,
                    identity(Direction::Response),
                    Operation::Generate,
                    mode,
                    true,
                    |_| {},
                )
                .unwrap();
                deliver(&streams, &mut owner, &mut egress)
            };
            let out: Value = if mode == BodyMode::Buffered {
                serde_json::from_str(&text).unwrap()
            } else {
                text.lines()
                    .filter_map(|line| line.strip_prefix("data: "))
                    .filter_map(|data| serde_json::from_str::<Value>(data).ok())
                    .find(|event| event["usage"].is_object())
                    .unwrap()
            };
            for (key, value) in extensions.as_object().unwrap() {
                assert_eq!(&out["usage"][key], value, "{mode:?}: {key}");
            }
        }
    }
}

/// A count is local over the body generation would send: nothing goes upstream.
#[tokio::test]
async fn a_count_is_local() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([]).await;
    let body = r#"{"model":"grok-4.3","messages":[{"role":"user","content":"hello there, how are you"}]}"#;
    let input = (body, 64, Sessions::default());
    let run = run_on(
        &streams,
        &executors(),
        &exchange(&upstream),
        Client::Anthropic,
        Operation::CountTokens,
        input,
    )
    .await;
    let (outcome, published, _) = produce(&streams, run.started.unwrap()).await;
    outcome.unwrap();
    assert_eq!(upstream.requests(), 0);
    let count = records(&published)
        .into_iter()
        .find(|r| r.contains("event: x_ck.count_tokens"))
        .expect("a count record");
    assert!(!count.contains("\"input_tokens\":0"), "{count}");
}

/// Compact is xAI's native `/responses/compact`, buffered, without stream flag or tools.
#[tokio::test]
async fn compact_goes_to_the_native_compact() {
    let compaction = r#"{"id":"cmp_1","object":"response.compaction","created_at":4,"model":"grok-4.3","output":[{"id":"c","type":"compaction","encrypted_content":"secret"}],"usage":{"input_tokens":3,"output_tokens":1,"total_tokens":4}}"#;
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::json(compaction)]).await;
    let body = r#"{"model":"grok-4.3","input":[{"role":"user","content":"hi"}],"previous_response_id":"resp_0"}"#;
    let input = (body, 64, Sessions::default());
    let run = run_on(
        &streams,
        &executors(),
        &exchange(&upstream),
        Client::Responses,
        Operation::Compact,
        input,
    )
    .await;
    let (outcome, published, _) = produce(&streams, run.started.unwrap()).await;
    outcome.unwrap();
    assert_eq!(published.terminal, Some(Terminal::Done));
    let captured = &upstream.captured()[0];
    assert_eq!(captured.path, "/v1/responses/compact");
    assert_eq!(captured.header("accept"), Some("application/json"));
    let sent = json_body(captured);
    assert!(sent.get("stream").is_none() && sent.get("tools").is_none(), "{sent}");
    assert_eq!(sent["previous_response_id"], "resp_0", "{sent}");
    assert!(records(&published).iter().any(|r| r.contains("secret")));
}

/// Every usage and capture call, by name.
#[derive(Default)]
struct Observed(std::sync::Mutex<Vec<String>>);

impl Observed {
    fn push(&self, call: String) {
        self.0.lock().unwrap().push(call);
    }
}

impl ck_core::exec::UsageObserver for Observed {
    fn response_body(&self, _format: ck_core::format::Format, _body: &[u8]) {
        self.push("response_body".into());
    }
    fn response_line(&self, _format: ck_core::format::Format, _line: &[u8]) {
        self.push("response_line".into());
    }
    fn request(&self, _format: ck_core::format::Format, _payload: &[u8]) {
        self.push("request".into());
    }
    fn request_for(&self, identifier: &str, payload: &[u8]) {
        self.push(format!("request {identifier} {}", String::from_utf8_lossy(payload)));
    }
    fn upstream_model(&self, model: &str) {
        self.push(format!("upstream_model {model}"));
    }
    fn round_trip_started(&self) {
        self.push("round_trip_started".into());
    }
    fn first_byte(&self) {
        self.push("first_byte".into());
    }
}

impl ck_core::exec::CaptureObserver for Observed {
    fn record(&self, event: ck_core::exec::CaptureEvent<'_>) {
        let name = match event {
            ck_core::exec::CaptureEvent::Request(request) => format!("capture request {}", request.url),
            ck_core::exec::CaptureEvent::ResponseMetadata(status, _) => format!("capture metadata {status}"),
            ck_core::exec::CaptureEvent::ResponseChunk(_) => "capture chunk".into(),
            _ => "capture other".into(),
        };
        self.push(name);
    }
}

/// The executor reports the request-side facts (contract G4) and captures the exchange; the
/// response's usage comes from the canonical stream, never `response_*`.
#[tokio::test]
async fn request_side_facts_reach_the_usage_sink() {
    let streams = Streams::new();
    let upstream = StreamUpstream::start([Reply::sse(sse())]).await;
    let mut exchange = exchange(&upstream);
    let observed = std::sync::Arc::new(Observed::default());
    let capture = ck_core::exec::CaptureSink::new(observed.clone());
    exchange.usage = ck_core::exec::UsageSink::new(observed.clone()).with_capture(capture);
    let input = (RESPONSES, 64, Sessions::default());
    let run = run_on(
        &streams,
        &executors(),
        &exchange,
        Client::Responses,
        Operation::Generate,
        input,
    )
    .await;
    let (outcome, _, _) = produce(&streams, run.started.unwrap()).await;
    outcome.unwrap();
    let calls = observed.0.lock().unwrap().clone();
    let wire = String::from_utf8(upstream.captured()[0].body.clone()).unwrap();
    let url = format!("{}/v1/responses", upstream.url());
    for call in [
        format!("request xai {wire}"),
        format!("upstream_model {MODEL}"),
        "round_trip_started".into(),
        format!("capture request {url}"),
        "capture metadata 200".into(),
        "first_byte".into(),
        "capture chunk".into(),
    ] {
        assert!(calls.contains(&call), "{call} missing from {calls:?}");
    }
    assert!(!calls.iter().any(|c| c.starts_with("response_")), "{calls:?}");
}

/// The selector sends xAI credentials through this executor.
#[test]
fn the_selector_has_an_xai_arm() {
    let streams = Streams::new();
    let exchange = Exchange::new(credential("http://127.0.0.1:1/v1"), MODEL, "/v1/responses");
    assert_eq!(executors().destination(&exchange.attempt(&streams.store)), Some("xai"));
}

/// Grep gate: the executor files call neither the old pair registry nor the usage sink, and
/// the legacy modules over the size cap do not grow (size ratchet).
#[test]
fn the_executor_uses_no_old_translation() {
    for source in [
        include_str!("../../src/xai_exec.rs"),
        include_str!("../../src/xai_events.rs"),
        include_str!("../../src/xai_shape.rs"),
    ] {
        for old in [
            "ck_translate::pair",
            "translate_",
            "StreamTranslator",
            "ck_translate::stream::",
            ".response_",
        ] {
            assert!(!source.contains(old), "{old}");
        }
    }
    for (lines, cap) in [
        (include_str!("../../src/xai.rs").lines().count(), 1196),
        (include_str!("../../src/xai_request.rs").lines().count(), 1446),
        (include_str!("../../src/xai_response.rs").lines().count(), 910),
        (include_str!("../../src/xai_url.rs").lines().count(), 627),
    ] {
        assert!(lines <= cap, "{lines} > {cap}");
    }
}
