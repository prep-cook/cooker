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
    // Go answers a `compaction_trigger` turn with a compact it runs itself and six synthesized
    // events; the executor sends the turn to xAI as the client wrote it.
    ("compaction_trigger_stream", "no synthesized compaction"),
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

/// Members the Responses egress always writes, which Go passed on only when the upstream did.
fn egress_defaults(got: &mut Value, want: &Value) {
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
}

#[tokio::test]
async fn responses_uploads_match_the_go_executor() {
    let executors = executors();
    let mut compared = 0;
    let mut failures = Vec::new();
    let mut responses = Vec::new();
    for s in scenarios() {
        let name = s["name"].as_str().unwrap();
        if LEFT_OUT.iter().any(|(left, _)| *left == name) {
            continue;
        }
        let generation = matches!(s["op"].as_str(), Some("execute" | "stream"));
        if s["source"] != "openai-response" || !generation || s.get("alt").is_some() || s.get("websocket").is_some() {
            continue;
        }
        let Some(raw) = s["request"].as_str() else {
            continue;
        };
        let up = &s["upstream"];
        let reply = Reply::status(
            up["status"].as_u64().unwrap() as u16,
            "text/event-stream",
            up["body"].as_str().unwrap_or_default().to_owned(),
        );
        let upstream = StreamUpstream::start([reply]).await;
        let cfg = s["config"]
            .as_str()
            .map_or_else(Config::default, |c| Config::parse(c).unwrap());
        let mut exchange = exchange(&upstream);
        exchange.credential = scenario_credential(&s, &cfg, &format!("{}/v1", upstream.url()));
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
        let payload = s["payload"].as_str().unwrap();
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
        let run = run_on(
            &streams,
            &executors,
            &exchange,
            Client::Responses,
            Operation::Generate,
            (payload, 64, sessions),
        )
        .await;
        let outcome = match run.started {
            Ok(started) => {
                let (outcome, _, mut owner) = produce(&streams, started).await;
                match outcome {
                    Ok(()) => Ok(responses_json(&streams, &mut owner)),
                    Err(error) => Err(error),
                }
            }
            Err(error) if error.status == 400 && error.body.starts_with(b"fence") => {
                failures.push(format!("{name}: {}", String::from_utf8_lossy(&error.body)));
                continue;
            }
            Err(error) => Err(error),
        };
        match (&outcome, s.get("output"), s.get("error")) {
            (Ok(out), Some(want), _) if s["op"] == "execute" => {
                let want: Value = serde_json::from_str(want.as_str().unwrap()).unwrap();
                let mut out = out.clone();
                egress_defaults(&mut out, &want);
                if out != want {
                    responses.push(format!("{name}:\n  got  {out}\n  want {want}"));
                }
            }
            (Err(error), _, Some(want)) if want["status"] != error.status => {
                responses.push(format!("{name}: status {} want {}", error.status, want["status"]));
            }
            (Err(error), _, None) => responses.push(format!("{name}: failed {}", error.status)),
            (Ok(_), _, Some(want)) => responses.push(format!("{name}: ok, want {want}")),
            _ => {}
        }
        let Some(captured) = upstream.captured().into_iter().next() else {
            failures.push(format!("{name}: nothing sent"));
            continue;
        };
        compared += 1;
        let (sent, want) = (messages(json_body(&captured)), messages(recorded_body(raw)));
        if sent != want {
            failures.push(format!("{name}:\n  sent {sent}\n  want {want}"));
        }
    }
    assert!(compared >= 30, "only {compared} scenarios compared");
    assert!(
        responses.is_empty(),
        "{} responses differ:\n{}",
        responses.len(),
        responses.join("\n")
    );
    assert!(
        failures.is_empty(),
        "{} of {compared} differ:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
