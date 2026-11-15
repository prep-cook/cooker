//! Replays historical Go relay recordings against a scripted browser session.
//! Transport, errors, captures and usage accounting retain their recorded contract.
//! Native request fields and streamed Gemini usage follow the source request and
//! recorded raw upstream replies, not Go's former rewrites.

use std::sync::Arc;
use std::time::Duration;

use ck_core::exec::Caller;
use serde_json::{Value, json};

use super::*;
use crate::gemini::tests::{Captured, Captures, UsageReports, client_bytes};

#[path = "aistudio_expectations.rs"]
mod expectations;

const FIXTURE: &str = include_str!("../tests/fixtures/aistudio_go.json");
const CHANNEL: &str = "aistudio-test";

fn request(s: &Value) -> ExecRequest {
    let op = s["op"].as_str().unwrap();
    let source = Format::parse(s["source"].as_str().unwrap()).unwrap();
    let body = Bytes::from(s["payload"].as_str().unwrap().to_owned());
    let mut headers = http::HeaderMap::new();
    for (k, v) in s["headers"].as_object().into_iter().flatten() {
        headers.insert(
            http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
            v.as_str().unwrap().parse().unwrap(),
        );
    }
    let model = s["model"].as_str().unwrap().to_owned();
    ExecRequest {
        operation: if op == "count" {
            Operation::CountTokens
        } else {
            Operation::Generate
        },
        source_format: source,
        response_format: source,
        requested_model: model.clone(),
        model,
        original_body: body.clone(),
        body,
        stream: op == "stream",
        alt: s["alt"].as_str().map(str::to_owned),
        session: None,
        execution_session: None,
        derived_session: None,
        resolved_model: None,
        usage: Default::default(),
        request_path: String::new(),
        headers,
        caller: Caller {
            principal: "fake-client-key".into(),
            source: "authorization",
            provider: String::new(),
        },
    }
}

/// The frame as Go's browser recorded it: message ID and send time replaced.
fn normalized_frame(frame: &str) -> String {
    let value: Value = serde_json::from_str(frame).unwrap();
    let id = value["id"].as_str().unwrap();
    let sent = value["payload"]["sent_at"].as_str().unwrap();
    assert!(
        chrono::DateTime::parse_from_rfc3339(sent).is_ok(),
        "sent_at is RFC 3339: {sent}"
    );
    frame.replacen(id, "ID", 1).replacen(sent, "SENT_AT", 1)
}

#[tokio::test]
async fn relay_scenarios() {
    // The scenarios recorded again (or added) at v8.0.35 replace their base recordings.
    let fixture = crate::payload_barrier::with_v8_0_35(
        FIXTURE,
        include_str!("../tests/fixtures/aistudio_go_v8.0.35.json"),
        "scenarios",
    );
    let mut scenarios = fixture["scenarios"].as_array().unwrap().clone();
    assert!(scenarios.len() >= 30, "fixture lost scenarios");
    for mode in ["execute_gemini", "stream_gemini_sse", "stream_gemini_bare"] {
        let template = scenarios.iter().find(|s| s["name"] == mode).unwrap().clone();
        for (name, fields) in [
            ("omitted_defaults", json!({})),
            ("empty_safety", json!({"safetySettings": []})),
            (
                "explicit_safety",
                json!({"safetySettings": [{"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_ONLY_HIGH"}]}),
            ),
            (
                "schema_dialect",
                json!({
                    "generationConfig": {
                        "responseMimeType": "application/json",
                        "responseSchema": {"type": "OBJECT", "properties": {"tags": {"type": "ARRAY", "items": {"type": "STRING"}}}, "required": ["tags"]}
                    },
                    "tools": [{"functionDeclarations": [{"name": "tag", "parameters": {"type": "OBJECT", "properties": {"tag": {"type": "STRING"}}}}]}]
                }),
            ),
            (
                "json_schema_and_output_limit",
                json!({
                    "generationConfig": {
                        "maxOutputTokens": 73,
                        "responseMimeType": "application/json",
                        "responseJsonSchema": {"type": "object", "properties": {"answer": {"type": "string"}}, "required": ["answer"]}
                    },
                    "tools": [{"functionDeclarations": [{"name": "tag", "parametersJsonSchema": {"type": "object", "properties": {"tag": {"type": "string"}}}}]}]
                }),
            ),
        ] {
            let mut scenario = template.clone();
            scenario["name"] = json!(format!("{mode}_{name}"));
            let mut input: Value = serde_json::from_str(template["payload"].as_str().unwrap()).unwrap();
            input
                .as_object_mut()
                .unwrap()
                .extend(fields.as_object().unwrap().clone());
            scenario["payload"] = json!(input.to_string());
            let mut frame: Value = serde_json::from_str(template["frames"][0].as_str().unwrap()).unwrap();
            frame["payload"]["body"] = json!(input.to_string());
            scenario["frames"][0] = json!(frame.to_string());
            if mode != "execute_gemini" {
                let data = template["replies"][1]["payload"]["data"].as_str().unwrap();
                let mut early: Value =
                    serde_json::from_str(data.trim().strip_prefix("data:").unwrap_or(data).trim()).unwrap();
                early["usageMetadata"]
                    .as_object_mut()
                    .unwrap()
                    .remove("totalTokenCount");
                scenario["replies"][1]["payload"]["data"] = json!(if mode == "stream_gemini_sse" {
                    format!("data: {early}\n\n")
                } else {
                    early.to_string()
                });
            }
            scenarios.push(scenario);
        }
    }
    let relay = Arc::new(Relay::default());
    let (session, mut frames) = relay.connect_as(CHANNEL.into());
    let executor = AiStudioExecutor { relay: relay.clone() };
    for s in &scenarios {
        let name = s["name"].as_str().unwrap();
        let cfg = Config::parse(s["config"].as_str().unwrap_or_default()).unwrap();
        let mut credential = ck_core::credential::Credential::relay_session(if s["disconnected"] == true {
            "aistudio-missing"
        } else {
            CHANNEL
        });
        for (k, v) in s["attributes"].as_object().into_iter().flatten() {
            credential.attributes.insert(k.clone(), v.as_str().unwrap().to_owned());
        }
        let mut req = request(s);
        let reports = Arc::new(UsageReports::default());
        let captures = Arc::new(Captures::default());
        req.usage = reports.sink().with_capture(captures.sink());
        let client = req.response_format;
        let alt = req.alt.as_deref().is_some_and(|a| !a.is_empty());
        let replies = s["replies"].as_array().cloned().unwrap_or_default();
        let browser = async {
            let Ok(Some(frame)) = tokio::time::timeout(Duration::from_millis(200), frames.recv()).await else {
                return None;
            };
            let id = serde_json::from_str::<Value>(&frame).unwrap()["id"]
                .as_str()
                .unwrap()
                .to_owned();
            for reply in &replies {
                let mut message = serde_json::json!({"id": id, "type": reply["type"]});
                if let Some(payload) = reply.get("payload") {
                    message["payload"] = payload.clone();
                }
                session.receive(message.to_string().as_bytes()).await.unwrap();
            }
            Some(normalized_frame(&frame))
        };
        let run = execute_scenario(&executor, &credential, req, &cfg, s["cancel_when_idle"] == true);
        let (frame, outcome) = tokio::join!(browser, run);
        let actual_frames: Vec<_> = frame.into_iter().collect();
        let recorded_frames = s["frames"].as_array().cloned().unwrap_or_default();
        assert_eq!(actual_frames.len(), recorded_frames.len(), "{name}: frame count");
        for (actual, recorded) in actual_frames.iter().zip(&recorded_frames) {
            expectations::frame(s, actual, recorded.as_str().unwrap());
        }
        check_outcome(s, client, alt, outcome);
        reports.check(s);
        check_capture(s, &captures.events());
    }
    assert!(!session.is_closed());
}

type Outcome = (Option<String>, Vec<u8>, Option<(u16, String)>);

async fn execute_scenario(
    executor: &AiStudioExecutor,
    credential: &ck_core::credential::Credential,
    req: ExecRequest,
    cfg: &Config,
    cancel_when_idle: bool,
) -> Outcome {
    let mut output = None;
    let mut streamed = Vec::new();
    let mut error = None;
    match executor.execute(credential, req, cfg).await {
        Err(e) => error = Some((e.status, String::from_utf8_lossy(&e.body).into_owned())),
        Ok(response) => match response.body {
            ResponseBody::Buffered(bytes) => output = Some(String::from_utf8(bytes.to_vec()).unwrap()),
            ResponseBody::Stream(mut stream) => loop {
                let next = if cancel_when_idle {
                    match tokio::time::timeout(Duration::from_millis(200), stream.next()).await {
                        Ok(next) => next,
                        Err(_) => break,
                    }
                } else {
                    stream.next().await
                };
                match next {
                    Some(Ok(bytes)) => streamed.extend_from_slice(&bytes),
                    Some(Err(e)) => error = Some((e.status, String::from_utf8_lossy(&e.body).into_owned())),
                    None => break,
                }
            },
        },
    }
    (output, streamed, error)
}

fn check_outcome(s: &Value, client: Format, alt: bool, (output, streamed, error): Outcome) {
    let name = s["name"].as_str().unwrap();
    // A Go error without a status code is answered 500 by Go's handler.
    let want_error = s["error"].as_object().map(|e| {
        let status = e["status"].as_u64().unwrap() as u16;
        (
            if status == 0 { 500 } else { status },
            e["message"].as_str().unwrap().to_owned(),
        )
    });
    assert_eq!(error, want_error, "{name}: error");
    // Responses output and OpenAI chunks stamp wall-clock values.
    let created = regex::Regex::new(r#""created(_at)?": ?1[0-9]{9}([,}])"#).unwrap();
    let norm = |t: &str| created.replace_all(t, r#""created$1":<unix>$2"#).into_owned();
    assert_eq!(
        output.as_deref().map(norm),
        s["output"].as_str().map(norm),
        "{name}: output"
    );
    let chunks = expectations::chunks(s);
    let want_stream = client_bytes(client, alt, &chunks);
    if matches!(client, Format::Gemini | Format::Codex) {
        assert_eq!(
            expectations::semantic_wire(&streamed),
            expectations::semantic_wire(&want_stream),
            "{name}: stream"
        );
    } else {
        assert_eq!(
            norm(&String::from_utf8_lossy(&streamed)),
            norm(&String::from_utf8_lossy(&want_stream)),
            "{name}: stream"
        );
    }
}

/// Go's capture for a relayed attempt: the request as the browser received it, the
/// first status-bearing reply's metadata, every non-empty reply payload, and one error
/// per relay error.
fn check_capture(s: &Value, events: &[Captured]) {
    let name = s["name"].as_str().unwrap();
    if s["error"]["status"] == 501 {
        assert!(events.is_empty(), "{name}: {events:?}");
        return;
    }
    let Some(Captured::Request {
        url,
        headers,
        body,
        provider,
        auth_id,
        auth_type,
        auth_value,
    }) = events.first()
    else {
        panic!("{name}: no request capture in {events:?}");
    };
    let channel = if s["disconnected"] == true {
        "aistudio-missing"
    } else {
        CHANNEL
    };
    assert_eq!(
        (
            provider.as_str(),
            auth_id.as_str(),
            auth_type.as_str(),
            auth_value.as_str()
        ),
        ("aistudio", channel, "oauth", channel),
        "{name}: account"
    );
    if let Some(frame) = s["frames"].as_array().and_then(|f| f.first()) {
        let frame: Value = serde_json::from_str(frame.as_str().unwrap()).unwrap();
        let payload = &frame["payload"];
        assert_eq!(url, payload["url"].as_str().unwrap(), "{name}: url");
        expectations::body(s, body, payload["body"].as_str().unwrap());
        let mut want: Vec<(String, String)> = Vec::new();
        for (k, values) in payload["headers"].as_object().unwrap() {
            for v in values.as_array().unwrap() {
                want.push((k.clone(), v.as_str().unwrap().to_owned()));
            }
        }
        assert_eq!(headers, &want, "{name}: headers");
    }
    check_response_capture(s, &events[1..]);
}

fn check_response_capture(s: &Value, rest: &[Captured]) {
    let name = s["name"].as_str().unwrap();
    let replies = s["replies"].as_array().cloned().unwrap_or_default();
    let status_of = |r: &Value| match r.get("payload") {
        None => 502,
        Some(p) => p["status"].as_u64().unwrap_or(200) as u16,
    };
    let want_status = replies
        .iter()
        .find(|r| matches!(r["type"].as_str(), Some("http_response" | "stream_start")))
        .map(status_of);
    let statuses: Vec<u16> = rest
        .iter()
        .filter_map(|e| match e {
            Captured::Metadata(status) => Some(*status),
            _ => None,
        })
        .collect();
    assert_eq!(
        statuses,
        want_status.into_iter().collect::<Vec<_>>(),
        "{name}: metadata"
    );
    let chunks: Vec<u8> = rest
        .iter()
        .filter_map(|e| match e {
            Captured::Chunk(c) => Some(c.clone()),
            _ => None,
        })
        .flatten()
        .collect();
    let payloads: Vec<u8> = replies
        .iter()
        .filter_map(|r| r["payload"]["data"].as_str().or(r["payload"]["body"].as_str()))
        .flat_map(|p| p.bytes())
        .collect();
    assert_eq!(
        String::from_utf8_lossy(&chunks),
        String::from_utf8_lossy(&payloads),
        "{name}: chunks"
    );
    let errors = rest.iter().filter(|e| matches!(e, Captured::Error(_))).count();
    // Go records relay errors, a vanished relay, and (5ec31442) a non-2xx answer relayed
    // after the stream started.
    let late_status_errors = replies
        .iter()
        .skip(1)
        .filter(|r| r["type"] == "http_response")
        .filter(|r| !(200..300).contains(&r["payload"]["status"].as_i64().unwrap_or(0)))
        .count();
    // A retained Claude error event (7f2f17d7) ends the stream ahead of the relay error,
    // which Go then never records.
    let retained = s["error"]["message"]
        .as_str()
        .is_some_and(|m| m.starts_with(r#"{"type":"error""#));
    let relay_errors = if retained {
        0
    } else {
        replies.iter().filter(|r| r["type"] == "error").count()
    };
    let want_errors = relay_errors
        + usize::from(s["disconnected"] == true)
        + if s["op"] == "stream" { late_status_errors } else { 0 };
    assert_eq!(errors, want_errors, "{name}: errors in {rest:?}");
}

/// Go's `ensureColonSpacedJSON` on the shapes its comment names: escaped quotes inside
/// strings, nested containers, and non-JSON passed through.
#[test]
fn colon_spacing_matches_go_rules() {
    let spaced = ensure_colon_spaced_json(br#"{"b":[1,{"x":"a\"b: c\\"}],"a":1.50,"e":{},"f":[]}"#);
    assert_eq!(
        String::from_utf8(spaced).unwrap(),
        r#"{"a": 1.5,"b": [1,{"x": "a\"b: c\\"}],"e": {},"f": []}"#
    );
    assert_eq!(ensure_colon_spaced_json(b"data: {\"a\":1}"), b"data: {\"a\":1}");
    assert_eq!(ensure_colon_spaced_json(b"  "), b"  ");
}

/// Payload rules follow boundary-turn and thinking-level normalization; count rules
/// also follow count-request cleanup. Historical Go cases were recorded at 868e137.
#[test]
fn payload_rules_run_after_the_cleanup() {
    let cfg = Config::parse(
        r#"
requests:
  payload:
    override:
      - models: [{name: "*"}]
        params:
          "generationConfig.maxOutputTokens": 900000
          "generationConfig.thinkingConfig.thinkingLevel": low
          "safetySettings": [{category: configured}]
    filter:
      - models: [{name: "*"}]
        params: ["contents"]
"#,
    )
    .unwrap();
    let payload = r#"{"contents":[{"role":"model","parts":[{"text":"hi"}]}]}"#;
    for op in ["execute", "stream", "count"] {
        let req = request(&serde_json::json!({
            "op": op, "source": "gemini", "model": "gemini-3.1-pro-preview", "payload": payload,
        }));
        let (body, _) = translate(&req, &cfg, op == "count").unwrap();
        let text = String::from_utf8_lossy(&body);
        assert!(!gj::get(&body, "contents").exists(), "{op}: {text}");
        assert_eq!(
            gj::get(&body, "generationConfig.maxOutputTokens").int(),
            900000,
            "{op}: {text}"
        );
        assert_eq!(
            gj::get(&body, "generationConfig.thinkingConfig.thinkingLevel").str(),
            "low",
            "{op}: {text}"
        );
        assert_eq!(
            gj::get(&body, "safetySettings.0.category").str(),
            "configured",
            "{op}: {text}"
        );
    }
}

/// Go translates the original (baseline) body without checking its error: only the
/// working body's refusal refuses the request.
#[test]
fn a_refused_baseline_alone_does_not_refuse_the_request() {
    let cfg = Config::parse("").unwrap();
    let s = serde_json::json!({"op": "execute", "source": "claude", "model": "gemini-2.5-pro", "payload": r#"{"model":"gemini-2.5-pro","max_tokens":16,"messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}"#});
    let mut req = request(&s);
    req.original_body = Bytes::from_static(br#"{"model":"gemini-2.5-pro","max_tokens":16,"messages":[{"role":"user","content":[{"type":"container_upload","source":{"type":"file","file_id":"f1"}}]}]}"#);
    let (body, action) = translate(&req, &cfg, false).unwrap();
    assert_eq!(action, "generateContent");
    assert_eq!(ck_wire::json::get(&body, "contents.0.parts.0.text").str(), "hi");
    std::mem::swap(&mut req.body, &mut req.original_body);
    assert!(translate(&req, &cfg, false).is_err());
}
