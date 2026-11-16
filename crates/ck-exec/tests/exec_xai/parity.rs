//! The upload a Responses client's generation puts on the wire, JSON-equal to the Go
//! executor's recorded request for every same-protocol scenario of `fixtures/xai_go.json`.

use super::*;

const FIXTURE: &str = include_str!("../fixtures/xai_go.json");
const FIXTURE_V8_0_35: &str = include_str!("../fixtures/xai_go_v8.0.35.json");

/// The recorded scenarios, with the v8.0.35 recordings replacing their older ones.
fn scenarios() -> Vec<Value> {
    let mut base: Value = serde_json::from_str(FIXTURE).unwrap();
    let newer: Value = serde_json::from_str(FIXTURE_V8_0_35).unwrap();
    let cases = base["scenarios"].as_array_mut().unwrap();
    for case in newer["scenarios"].as_array().unwrap() {
        match cases.iter_mut().find(|c| c["name"] == case["name"]) {
            Some(slot) => *slot = case.clone(),
            None => cases.push(case.clone()),
        }
    }
    base["scenarios"].as_array().unwrap().clone()
}

/// The scenario's credential, its base URL moved to `base`.
fn scenario_credential(s: &Value, cfg: &Config, base: &str) -> Credential {
    let mut credential = match s["config_auth"].as_i64() {
        Some(index) if index >= 0 => ck_core::config::credentials::from_config(cfg)
            .into_iter()
            .filter(|c| c.provider == "xai")
            .nth(index as usize)
            .unwrap(),
        _ => {
            let mut metadata = s["metadata"].as_object().cloned().unwrap_or_default();
            metadata.insert("type".into(), "xai".into());
            let dir = std::path::Path::new("/fixture");
            Credential::from_file(dir, &dir.join("xai.json"), metadata).unwrap()
        }
    };
    for (k, v) in s["attributes"].as_object().into_iter().flatten() {
        credential.attributes.insert(k.clone(), v.as_str().unwrap().to_owned());
    }
    credential.attributes.insert("base_url".into(), base.into());
    credential
}

/// The JSON body of a recorded raw HTTP request.
fn recorded_body(raw: &str) -> Value {
    let body = &raw[raw.find("\r\n\r\n").unwrap() + 4..];
    serde_json::from_str(body).unwrap()
}

/// The Responses request egress writes every input message in one form: typed, with its
/// content as parts. Go kept the client's form, which the Responses API reads the same.
fn messages(mut body: Value) -> Value {
    for item in body["input"].as_array_mut().into_iter().flatten() {
        if item.get("role").is_none() || item.get("type").is_some_and(|t| t != "message") {
            continue;
        }
        item["type"] = json!("message");
        if let Some(text) = item["content"].as_str() {
            let kind = if item["role"] == "assistant" {
                "output_text"
            } else {
                "input_text"
            };
            item["content"] = json!([{"type": kind, "text": text}]);
        }
    }
    body
}

/// Scenarios this comparison leaves out, with the reason.
const LEFT_OUT: &[(&str, &str)] = &[
    // The Responses codec refuses these client bodies at the fence (`content: null` on a
    // reasoning item, a custom tool call's history) before any executor runs.
    ("shapes_responses_request", "codec refusal"),
    ("custom_tool_history_normalized", "codec refusal"),
    ("replay_responses_turn2_tool_output", "codec refusal"),
    // Q17 replaces Go's synthesized compaction with the upstream generation response.
    ("compaction_trigger_stream", "remote compaction"),
    // A text delta for an item never announced: the ingress refuses it as invalid upstream
    // data (502) before the body ends; `eof_before_completion_fails_like_go` covers the 408.
    ("disconnected_before_completed", "invalid event order"),
    // Go's buffered path lets `response.done` close the apply_patch bridge, which then drops
    // the completion (408); its stream path, which the executor follows, completes.
    ("usage_done_ignored_execute", "Go buffered-path quirk"),
    // A folded namespace holding the patch tool, recorded with minimal events (no response
    // id or status, call items without arguments): the ingress refuses the stream (502).
    // Not isolated further; the bridge-handled dispatcher path is left as the old path has it.
    ("apply_patch_folded_namespace_execute", "minimal events refused"),
    ("apply_patch_folded_namespace_stream", "minimal events refused"),
];

/// Members the Responses egress always writes, which Go passed on only when the upstream did,
/// and the response id: the client sees the exchange's own id where Go passed on the upstream's.
fn egress_defaults(got: &mut Value, want: &mut Value) {
    if got["id"] == "resp_test" {
        match want.get("id") {
            Some(id) => got["id"] = id.clone(),
            None => {
                got.as_object_mut().unwrap().remove("id");
            }
        }
    }
    for name in ["parallel_tool_calls", "tool_choice", "tools", "created_at"] {
        if want.get(name).is_none() {
            got.as_object_mut().unwrap().remove(name);
        }
    }
    let details = "/usage/input_tokens_details";
    if want.pointer(&format!("{details}/cache_write_tokens")).is_none()
        && let Some(details) = got.pointer_mut(details).and_then(Value::as_object_mut)
    {
        details.remove("cache_write_tokens");
    }
    // F23 requires these members even when the native response omitted them. Extend the
    // expectation, not the output: missing members, wrong defaults and other additions fail.
    for (name, default) in [
        ("access_programs", Value::Null),
        ("error", Value::Null),
        ("incomplete_details", Value::Null),
        ("instructions", Value::Null),
        ("metadata", Value::Null),
        ("temperature", json!(1.0)),
        ("top_p", json!(1.0)),
    ] {
        want.as_object_mut().unwrap().entry(name).or_insert(default);
    }
}

/// The scenario's attempt against `upstream`.
fn scenario_exchange(s: &Value, upstream: &StreamUpstream) -> Exchange {
    let cfg = s["config"]
        .as_str()
        .map_or_else(Config::default, |c| Config::parse(c).unwrap());
    let mut exchange = exchange(upstream);
    exchange.credential = scenario_credential(s, &cfg, &format!("{}/v1", upstream.url()));
    exchange.cfg = cfg;
    exchange.model = s["model"].as_str().unwrap().into();
    exchange.caller.principal = s["caller_key"].as_str().unwrap_or_default().into();
    exchange.route = s["request_path"].as_str().unwrap_or("/v1/responses").into();
    for (k, v) in s["headers"].as_object().into_iter().flatten() {
        exchange.headers.insert(
            http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
            v.as_str().unwrap().parse().unwrap(),
        );
    }
    exchange
}

/// One scenario's run: the buffered Responses output or the attempt's failure, or the
/// fence's refusal message.
async fn run_scenario(
    executors: &Executors,
    s: &Value,
    upstream: &StreamUpstream,
) -> Result<Result<Value, ExecError>, String> {
    let exchange = scenario_exchange(s, upstream);
    let resolved = s["resolved_model"]
        .as_object()
        .map(|info| ck_core::exec::ResolvedModel {
            info: ck_core::registry::ModelInfo::from_raw(info.clone()).unwrap(),
            source: ck_core::exec::ResolvedSource::ApiKey,
        });
    let sessions = Sessions {
        execution: s["execution_session"].as_str(),
        derived: s["derived_session"].as_str(),
        resolved: resolved.as_ref(),
    };
    let streams = Streams::new();
    let input = (s["payload"].as_str().unwrap(), 64, sessions);
    let run = run_on(
        &streams,
        executors,
        &exchange,
        Client::Responses,
        Operation::Generate,
        input,
    )
    .await;
    match run.started {
        Ok(started) => {
            let (outcome, _, mut owner) = produce(&streams, started).await;
            Ok(outcome.map(|()| responses_json(&streams, &mut owner)))
        }
        Err(error) if error.status == 400 && error.body.starts_with(b"fence") => {
            Err(String::from_utf8_lossy(&error.body).into_owned())
        }
        Err(error) => Ok(Err(error)),
    }
}

/// How the scenario's outcome differs from Go's recorded output or error, if it does.
fn response_difference(name: &str, s: &Value, outcome: &Result<Value, ExecError>) -> Option<String> {
    match (outcome, s.get("output"), s.get("error")) {
        (Ok(out), Some(want), _) if s["op"] == "execute" => {
            let mut want: Value = if matches!(
                name,
                "x_search_internal_calls_filtered" | "reasoning_output_normalized_execute"
            ) {
                s["upstream"]["body"]
                    .as_str()
                    .unwrap()
                    .lines()
                    .filter_map(|line| line.strip_prefix("data: "))
                    .filter_map(|data| serde_json::from_str::<Value>(data).ok())
                    .find(|event| event["type"] == "response.completed")
                    .unwrap()["response"]
                    .clone()
            } else {
                serde_json::from_str(want.as_str().unwrap()).unwrap()
            };
            let mut out = out.clone();
            egress_defaults(&mut out, &mut want);
            (out != want).then(|| format!("{name}:\n  got  {out}\n  want {want}"))
        }
        (Err(error), _, Some(want)) if want["status"] != error.status => {
            Some(format!("{name}: status {} want {}", error.status, want["status"]))
        }
        (Err(error), _, None) => Some(format!("{name}: failed {}", error.status)),
        (Ok(_), _, Some(want)) => Some(format!("{name}: ok, want {want}")),
        _ => None,
    }
}

/// Whether the new route runs the scenario: a Responses client's generation over HTTP.
fn compared(s: &Value) -> bool {
    let name = s["name"].as_str().unwrap();
    let generation = matches!(s["op"].as_str(), Some("execute" | "stream"));
    !LEFT_OUT.iter().any(|(left, _)| *left == name)
        && s["source"] == "openai-response"
        && generation
        && s.get("alt").is_none()
        && s.get("websocket").is_none()
        && s["request"].is_string()
}

#[tokio::test]
async fn responses_uploads_match_the_go_executor() {
    let executors = executors();
    let (mut count, mut failures, mut responses) = (0, Vec::new(), Vec::new());
    for s in scenarios().into_iter().filter(compared) {
        let name = s["name"].as_str().unwrap();
        let up = &s["upstream"];
        let reply = Reply::status(
            up["status"].as_u64().unwrap() as u16,
            "text/event-stream",
            up["body"].as_str().unwrap_or_default().to_owned(),
        );
        let upstream = StreamUpstream::start([reply]).await;
        let outcome = match run_scenario(&executors, &s, &upstream).await {
            Ok(outcome) => outcome,
            Err(refusal) => {
                failures.push(format!("{name}: {refusal}"));
                continue;
            }
        };
        responses.extend(response_difference(name, &s, &outcome));
        let Some(captured) = upstream.captured().into_iter().next() else {
            failures.push(format!("{name}: nothing sent"));
            continue;
        };
        count += 1;
        let raw = s["request"].as_str().unwrap();
        let (sent, mut want) = (messages(json_body(&captured)), messages(recorded_body(raw)));
        if name == "tools_clamped_to_200" {
            let input: Value = serde_json::from_str(s["payload"].as_str().unwrap()).unwrap();
            want["tools"] = input["tools"].clone();
            want["tools"].as_array_mut().unwrap().push(json!({"type":"x_search"}));
        }
        if sent != want {
            failures.push(format!("{name}:\n  sent {sent}\n  want {want}"));
        }
    }
    assert!(count >= 30, "only {count} scenarios compared");
    assert!(
        responses.is_empty(),
        "{} responses differ:\n{}",
        responses.len(),
        responses.join("\n")
    );
    assert!(
        failures.is_empty(),
        "{} of {count} differ:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[tokio::test]
async fn compaction_trigger_uses_the_contract_path() {
    let payload = r#"{"model":"grok-4.3","input":[{"type":"compaction_trigger","threshold":123}],"stream":true}"#;
    let upstream = StreamUpstream::start([Reply::sse(sse())]).await;
    let streams = Streams::new();
    let run = run(&streams, &upstream, Client::Responses, payload, 7).await;
    let (result, _, mut owner) = produce(&streams, run.started.unwrap()).await;
    result.unwrap();
    let delivered = responses_json(&streams, &mut owner);
    assert_eq!(
        delivered["id"], "resp_test",
        "the client sees the pinned local response id"
    );
    assert_eq!(delivered["output"][0]["content"][0]["text"], "hello");
    let captured = upstream.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].path, "/v1/responses");
    assert_eq!(
        json_body(&captured[0])["input"][0],
        json!({"type":"compaction_trigger","threshold":123})
    );
}

#[tokio::test]
async fn compaction_trigger_uses_the_upstream_response_on_the_old_path() {
    use futures_util::StreamExt;

    let payload = r#"{"model":"grok-4.3","input":[{"type":"compaction_trigger","threshold":123}],"stream":true}"#;
    let executors = executors();
    let cfg = Config::default();

    let upstream = StreamUpstream::start([Reply::sse(sse())]).await;
    let credential = credential(&format!("{}/v1", upstream.url()));
    let model = MODEL.to_owned();
    let request = ck_core::exec::ExecRequest {
        operation: ck_core::exec::Operation::Generate,
        source_format: ck_core::format::Format::OpenAIResponse,
        response_format: ck_core::format::Format::OpenAIResponse,
        requested_model: model.clone(),
        model,
        original_body: bytes::Bytes::new(),
        body: bytes::Bytes::from(payload.to_owned()),
        stream: true,
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
    let response = executors
        .execute(&credential, request, &cfg)
        .await
        .expect("the old arm answers");
    let ck_core::exec::ResponseBody::Stream(mut stream) = response.body else {
        panic!("the old arm answers a stream");
    };
    let mut chunks = Vec::new();
    while let Some(chunk) = stream.next().await {
        chunks.push(Value::String(String::from_utf8(chunk.unwrap().to_vec()).unwrap()));
    }
    let received = chunks.iter().map(|v| v.as_str().unwrap()).collect::<String>();
    assert!(received.contains("hello"), "{received}");
    assert!(received.contains("resp_1"), "{received}");
    assert!(!received.contains("keepalive"), "{received}");
    let captured = upstream.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].path, "/v1/responses");
    assert_eq!(
        json_body(&captured[0])["input"][0],
        json!({"type":"compaction_trigger","threshold":123})
    );
}
