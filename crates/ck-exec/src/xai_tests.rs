//! Differential tests against `tests/fixtures/xai_go.json`, produced by the real Go
//! XAIExecutor (tests/reference/xai). Scenarios run in order on one executor, as the Go
//! generator runs them in one process, so the reasoning replay cache carries from one
//! turn to the next. Each scenario replays the scripted upstream answer and compares the
//! URL the executor chose, upstream request and output. Changed xAI contracts below use
//! the native upstream events rather than Go's lossy projections.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use ck_core::exec::Caller;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

use super::*;
use crate::openai_compat_usage::{HttpAttempt, Logged, http_capture_problems, published};

const FIXTURE: &str = include_str!("../tests/fixtures/xai_go.json");
/// The scenarios the payload barrier changed, recorded at v8.0.35; they replace their
/// 6fecc6e recordings.
const FIXTURE_V8_0_35: &str = include_str!("../tests/fixtures/xai_go_v8.0.35.json");

/// Shared helpers whose real port has not landed: scenarios that need them are skipped
/// and listed, so integration can remove an entry and see the scenario run.
const PENDING: &[&str] = &[];

/// One-shot raw HTTP/1.1 capture server answering with the scripted response.
struct Mock {
    addr: String,
    raw: Arc<Mutex<Option<Vec<u8>>>>,
}

impl Mock {
    async fn start(up: &Value) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let raw: Arc<Mutex<Option<Vec<u8>>>> = Arc::default();
        let sink = raw.clone();
        let status = up["status"].as_u64().unwrap() as u16;
        let headers: Vec<(String, String)> = up["headers"]
            .as_array()
            .map(|hs| {
                hs.iter()
                    .map(|h| (h[0].as_str().unwrap().to_owned(), h[1].as_str().unwrap().to_owned()))
                    .collect()
            })
            .unwrap_or_default();
        let body = up["body"].as_str().unwrap().as_bytes().to_vec();
        tokio::spawn(async move {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            let (read, mut write) = socket.into_split();
            let mut reader = BufReader::new(read);
            let mut captured = Vec::new();
            let mut length = 0;
            loop {
                let mut line = Vec::new();
                if reader.read_until(b'\n', &mut line).await.unwrap_or(0) == 0 {
                    break;
                }
                captured.extend_from_slice(&line);
                let text = String::from_utf8_lossy(&line).to_ascii_lowercase();
                if let Some(v) = text.strip_prefix("content-length:") {
                    length = v.trim().parse().unwrap_or(0);
                }
                if line == b"\r\n" {
                    break;
                }
            }
            let mut payload = vec![0; length];
            reader.read_exact(&mut payload).await.unwrap();
            captured.extend_from_slice(&payload);
            *sink.lock().unwrap() = Some(captured);
            let reason = http::StatusCode::from_u16(status)
                .ok()
                .and_then(|s| s.canonical_reason())
                .unwrap_or("");
            let mut out = format!("HTTP/1.1 {status} {reason}\r\n").into_bytes();
            for (n, v) in &headers {
                out.extend_from_slice(format!("{n}: {v}\r\n").as_bytes());
            }
            out.extend_from_slice(format!("Content-Length: {}\r\n", body.len()).as_bytes());
            out.extend_from_slice(b"Connection: close\r\n\r\n");
            out.extend_from_slice(&body);
            let _ = write.write_all(&out).await;
            let _ = write.shutdown().await;
        });
        Self { addr, raw }
    }

    fn request(&self) -> Option<String> {
        let raw = self.raw.lock().unwrap().clone()?;
        Some(String::from_utf8_lossy(&raw).replace(&self.addr, "UPSTREAM"))
    }
}

fn credential(s: &Value, cfg: &Config) -> Credential {
    let index = s["config_auth"].as_i64().unwrap();
    if index >= 0 {
        let xai: Vec<Credential> = ck_core::config::credentials::from_config(cfg)
            .into_iter()
            .filter(|c| c.provider == "xai")
            .collect();
        return xai[index as usize].clone();
    }
    let mut metadata = s["metadata"].as_object().cloned().unwrap_or_default();
    metadata.insert("type".into(), "xai".into());
    let mut c = Credential::from_file(
        std::path::Path::new("/fixture"),
        std::path::Path::new("/fixture/xai.json"),
        metadata,
    )
    .unwrap();
    for (k, v) in s["attributes"].as_object().into_iter().flatten() {
        c.attributes.insert(k.clone(), v.as_str().unwrap().to_owned());
    }
    c
}

fn response_format(s: &Value) -> Format {
    let source = Format::parse(s["source"].as_str().unwrap()).unwrap();
    s["response"].as_str().and_then(Format::parse).unwrap_or(source)
}

/// The dispatch loop's model binding, as the generator bound it.
fn bound_model(v: &Value) -> Option<ck_core::exec::ResolvedModel> {
    Some(ck_core::exec::ResolvedModel {
        info: ck_core::registry::ModelInfo::from_raw(v.as_object()?.clone()).expect("resolved model info"),
        source: ck_core::exec::ResolvedSource::ApiKey,
    })
}

fn request(s: &Value, usage: ck_core::exec::UsageSink) -> ExecRequest {
    let op = s["op"].as_str().unwrap();
    let source = Format::parse(s["source"].as_str().unwrap()).unwrap();
    let payload = Bytes::from(s["payload"].as_str().unwrap().to_owned());
    let original = s["original"]
        .as_str()
        .map(|o| Bytes::from(o.to_owned()))
        .unwrap_or_default();
    let mut headers = http::HeaderMap::new();
    for (k, v) in s["headers"].as_object().into_iter().flatten() {
        headers.insert(
            http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
            v.as_str().unwrap().parse().unwrap(),
        );
    }
    // Go's executor-side fallback (EnsureSessionContext -> CanonicalSessionID).
    let session_body: &[u8] = if original.is_empty() { &payload } else { &original };
    let meta = ck_wire::session::Meta {
        execution_session: s["execution_session"].as_str(),
        derived: s["derived_session"].as_str(),
    };
    let session = Some(ck_wire::session::extract_session_id(&headers, session_body, &meta))
        .filter(|id| !id.is_empty())
        .map(|id| ck_wire::session::bound_session_identity(&id));
    let model = s["model"].as_str().unwrap().to_owned();
    ExecRequest {
        operation: if op == "count" {
            Operation::CountTokens
        } else {
            Operation::Generate
        },
        source_format: source,
        response_format: response_format(s),
        requested_model: s["requested_model"].as_str().unwrap_or(&model).to_owned(),
        model,
        original_body: original,
        body: payload,
        stream: op == "stream",
        alt: s["alt"].as_str().map(str::to_owned),
        session,
        execution_session: s["execution_session"].as_str().map(str::to_owned),
        derived_session: s["derived_session"].as_str().map(str::to_owned),
        resolved_model: bound_model(&s["resolved_model"]),
        usage,
        request_path: s["request_path"].as_str().unwrap_or_default().to_owned(),
        headers,
        caller: Caller {
            principal: s["caller_key"].as_str().unwrap_or_default().to_owned(),
            source: "authorization",
            provider: String::new(),
        },
    }
}

fn error_json(e: &ExecError) -> Value {
    serde_json::json!({
        "status": e.status,
        "message": String::from_utf8_lossy(&e.body),
        "retry_after_ms": e.retry_after.map_or(-1, |d| d.as_millis() as i64),
        // The server's reading of IsCredentialScoped (classify::credential_scoped).
        "credential_scoped": e.scope == FailureScope::Credential && e.status == 429,
        // A plain Go error (no status) is a transport-scoped 500 here.
        "plain": e.scope == FailureScope::Transport,
    })
}

/// `scheme://authority` of every upstream URL goes to the capture server.
fn redirect(url: &str, addr: &str) -> String {
    let rest = &url[url.find("://").map_or(0, |i| i + 3)..];
    let path = rest.find('/').map_or("", |i| &rest[i..]);
    format!("http://{addr}{path}")
}

fn native_events(s: &Value) -> Vec<Value> {
    let mut items = Vec::new();
    s["upstream"]["body"]
        .as_str()
        .unwrap()
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|data| {
            let mut event: Value = serde_json::from_str(data).unwrap();
            if event["type"] == "response.output_item.done" {
                items.push(event["item"].clone());
            }
            if event["type"] == "response.completed" && event["response"]["output"] == serde_json::json!([]) {
                event["response"]["output"] = serde_json::json!(items);
            }
            event
        })
        .collect()
}

fn frame_values(chunks: &[String]) -> Vec<Value> {
    chunks
        .iter()
        .flat_map(|chunk| chunk.lines())
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|data| serde_json::from_str(data).unwrap_or_else(|_| Value::String(data.into())))
        .collect()
}

#[tokio::test]
async fn go_reference_scenarios() {
    let fixture = crate::payload_barrier::with_v8_0_35(FIXTURE, FIXTURE_V8_0_35, "scenarios");
    let scenarios = fixture["scenarios"].as_array().unwrap();
    assert!(scenarios.len() >= 80, "fixture lost scenarios");
    // One executor for all scenarios: Go's replay cache is process-wide.
    let target: Arc<Mutex<String>> = Arc::default();
    let chosen: Arc<Mutex<Option<String>>> = Arc::default();
    let executor = {
        let (target, chosen) = (target.clone(), chosen.clone());
        XaiExecutor::default().with_url_rewrite(move |url| {
            *chosen.lock().unwrap() = Some(url.to_owned());
            redirect(url, &target.lock().unwrap())
        })
    };
    let mut skipped = Vec::new();
    let mut failures = Vec::new();
    for s in scenarios {
        let name = s["name"].as_str().unwrap();
        if name == "compaction_trigger_stream" {
            // Q17 is covered by the loopback regressions in tests/exec_xai/parity.rs.
            continue;
        }
        let needs: Vec<&str> = s["needs"]
            .as_array()
            .map(|n| n.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        if needs.iter().any(|n| PENDING.contains(n)) {
            skipped.push(name);
            continue;
        }
        let mock = match s.get("upstream").filter(|u| !u.is_null()) {
            Some(up) => Some(Mock::start(up).await),
            None => None,
        };
        let addr = mock.as_ref().map_or("127.0.0.1:9".to_owned(), |m| m.addr.clone());
        *target.lock().unwrap() = addr.clone();
        *chosen.lock().unwrap() = None;
        let cfg = Config::parse(&s["config"].as_str().unwrap_or_default().replace("UPSTREAM", &addr)).unwrap();
        let cred = credential(s, &cfg);
        let (recorder, capture, sink) = crate::openai_compat_usage::sinks();
        let req = request(s, sink);
        let op = s["op"].as_str().unwrap();
        let path = s["request_path"].as_str().unwrap_or_default().to_owned();
        let result = match op {
            "images" => executor.images(&cred, req, &path, &cfg).await,
            "videos" => {
                // Go polls any ID; cooker polls only a job it created (VideoJobs), as the
                // recorded poll's job had been here.
                let polled = gj::get(&req.body, "request_id").str().into_owned();
                if !path.ends_with("/generations") && !polled.is_empty() {
                    drop(executor.jobs.created(&polled));
                }
                executor.videos(&cred, req, &path, &cfg).await
            }
            _ => {
                executor
                    .execute(&cred, req, &cfg, s["websocket"].as_bool() == Some(true))
                    .await
            }
        };
        let mut output = None;
        let mut chunks = Vec::new();
        let mut error = None;
        match result {
            Err(e) => error = Some(error_json(&e)),
            Ok(response) => match response.body {
                ResponseBody::Buffered(bytes) => output = Some(String::from_utf8_lossy(&bytes).into_owned()),
                ResponseBody::Stream(mut stream) => {
                    while let Some(item) = stream.next().await {
                        assert!(error.is_none(), "{name}: stream item after an error");
                        match item {
                            Ok(bytes) => chunks.push(String::from_utf8_lossy(&bytes).into_owned()),
                            Err(e) => error = Some(error_json(&e)),
                        }
                    }
                }
            },
        }
        if name == "chat_completions_execute" {
            // This fixture supplies stop, which Responses cannot honor. Refuse before send.
            assert_eq!(error.as_ref().unwrap()["status"], 400);
            assert!(mock.as_ref().unwrap().request().is_none());
            continue;
        }
        let mut problems = Vec::new();
        let raw = mock.as_ref().and_then(|m| m.raw.lock().unwrap().clone());
        if let Some(mock) = &mock {
            let url = chosen.lock().unwrap().clone();
            if url.as_deref() != s["url"].as_str() {
                problems.push(format!("url: {url:?} != {:?}", s["url"]));
            }
            let got = mock.request();
            let want = s["request"].as_str().map(|raw| {
                if ck_core::registry::dynamic::auth_kind(&cred) == Some("apikey") {
                    crate::openai_compat_usage::api_key_wire(raw)
                } else {
                    raw.to_owned()
                }
            });
            let same = if matches!(name, "shapes_responses_request" | "tools_clamped_to_200") {
                let (got_head, got_body) = got.as_ref().unwrap().split_once("\r\n\r\n").unwrap();
                let (want_head, want_body) = want.as_ref().unwrap().split_once("\r\n\r\n").unwrap();
                let got_body: Value = serde_json::from_str(got_body).unwrap();
                let mut want_body: Value = serde_json::from_str(want_body).unwrap();
                let input: Value = serde_json::from_str(s["payload"].as_str().unwrap()).unwrap();
                if name == "shapes_responses_request" {
                    want_body["safety_identifier"] = input["safety_identifier"].clone();
                } else {
                    want_body["tools"] = input["tools"].clone();
                    want_body["tools"]
                        .as_array_mut()
                        .unwrap()
                        .push(serde_json::json!({"type":"x_search"}));
                }
                let headers = |head: &str| {
                    head.lines()
                        .filter(|line| !line.starts_with("Content-Length:"))
                        .map(str::to_owned)
                        .collect::<Vec<_>>()
                };
                got_body == want_body && headers(got_head) == headers(want_head)
            } else {
                got == want
            };
            if !same {
                problems.push(format!("request:\n  rust: {got:?}\n  expected: {want:?}"));
            }
        }
        if op != "count"
            && let Some(problem) = crate::openai_compat_usage::ttft_problem(
                &recorder.0.lock().unwrap(),
                raw.is_some(),
                raw.is_some() && !s["upstream"]["body"].as_str().unwrap_or_default().is_empty(),
            )
        {
            problems.push(problem);
        }
        // Capture: one logged attempt per request the upstream received, none otherwise.
        if let Some(raw) = raw {
            let up = &s["upstream"];
            let status = up["status"].as_u64().unwrap() as u16;
            let get = raw.starts_with(b"GET ");
            let attempt = HttpAttempt {
                raw: &raw,
                // The URL Go chose, as the rewrite sent it to the mock.
                url: &redirect(s["url"].as_str().unwrap_or_default(), &addr),
                provider: "xai",
                credential: &cred,
                // The video poll is a GET that logs its payload.
                logged_body: get.then(|| s["payload"].as_str().unwrap()),
                status,
                headers: up["headers"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|h| (h[0].as_str().unwrap().to_owned(), h[1].as_str().unwrap().to_owned()))
                    .collect(),
                body: up["body"].as_str().unwrap(),
                // A stream is logged line by line; a JSON answer (the compact a
                // compaction trigger runs) is read whole.
                logged: if op == "stream" && (200..300).contains(&status) && !json_reply(up) {
                    Logged::Lines
                } else {
                    Logged::Whole
                },
            };
            for problem in http_capture_problems(&capture.take(), &attempt) {
                problems.push(problem);
            }
        } else {
            let events = capture.take();
            if !events.is_empty() {
                problems.push(format!("capture without an upstream attempt: {events:?}"));
            }
        }
        if matches!(
            name,
            "x_search_internal_calls_filtered" | "reasoning_output_normalized_execute"
        ) {
            let events = native_events(s);
            let want = &events.last().unwrap()["response"];
            let got: Value = serde_json::from_str(output.as_deref().unwrap()).unwrap();
            if &got != want {
                problems.push(format!("output: {got} != {want}"));
            }
        } else if output.as_deref() != s["output"].as_str() {
            problems.push(format!("output: {output:?} != {:?}", s["output"]));
        }
        let go_chunks: Vec<&str> = s["chunks"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect();
        let want: Vec<String> = if response_format(s) == Format::OpenAIResponse {
            let mut framer = ck_translate::stream::ResponsesFramer::default();
            let mut frames: Vec<Bytes> = Vec::new();
            for chunk in &go_chunks {
                frames.extend(framer.write(chunk.as_bytes()));
            }
            frames.extend(framer.flush());
            frames.iter().map(|f| String::from_utf8_lossy(f).into_owned()).collect()
        } else {
            go_chunks
                .iter()
                .filter_map(|c| ck_translate::stream::frame(response_format(s), c.as_bytes()))
                .map(|f| String::from_utf8_lossy(&f).into_owned())
                .collect()
        };
        let want = if matches!(
            name,
            "reasoning_text_events_normalized_stream" | "replay_execution_session_without_caller"
        ) {
            native_events(s)
        } else {
            frame_values(&want)
        };
        let mut got = frame_values(&chunks);
        if name == "x_search_stream_keeps_client_same_name_tool" {
            // Compare the client call to Go's original recording. Hosted traces remain native
            // for Responses clients; exec_xai's hosted-trace table covers that separate contract.
            got.retain(|event| event["item"]["call_id"] != "xs_call");
            for event in &mut got {
                if let Some(index) = event.get_mut("output_index") {
                    *index = Value::from(index.as_u64().unwrap() - 1);
                }
                if let Some(output) = event.pointer_mut("/response/output").and_then(Value::as_array_mut) {
                    output.retain(|item| item["call_id"] != "xs_call");
                }
            }
        }
        if got != want {
            problems.push(format!("frames: {got:?} != {want:?}"));
        }
        // Go publishes at most one record per attempt; the server publishes what the
        // executor's reports say (the rules of ck-server's usage_record.rs, including
        // `discard` for an error Go raises before its reporter exists), compared with
        // Go's record or its absence. The server tracks no record for CountTokens.
        let want = s.get("usage").filter(|u| !u.is_null()).cloned();
        let reports = recorder.0.lock().unwrap();
        if op != "count" {
            let got = published(&reports, error.is_some());
            if got != want {
                problems.push(format!("usage: {got:?} != {want:?}"));
            }
        }
        drop(reports);
        let mut want_error = s["error"].clone();
        if !want_error.is_null() {
            let plain = want_error["status"] == 0;
            if plain {
                // A plain Go error: the handler answers 500.
                want_error["status"] = 500.into();
            }
            want_error["plain"] = plain.into();
        }
        let error = error.unwrap_or(Value::Null);
        if error != want_error {
            problems.push(format!("error: {error} != {want_error}"));
        }
        if !problems.is_empty() {
            failures.push(format!("{name}:\n{}", problems.join("\n")));
        }
    }
    assert!(
        failures.is_empty(),
        "{} scenarios differ:\n{}",
        failures.len(),
        failures.join("\n\n")
    );
    assert_eq!(skipped, Vec::<&str>::new());
}

/// Go marks the first response byte only on a body read that returns data
/// (`usageTTFTReadCloser`): an empty error body starts the round trip and never marks it.
#[tokio::test]
async fn empty_error_body_starts_ttft_without_first_byte() {
    let fixture: Value = serde_json::from_str(FIXTURE).unwrap();
    let s = fixture["scenarios"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == "api_key_default_base")
        .unwrap();
    let mock = Mock::start(&serde_json::json!({"status": 500, "headers": [], "body": ""})).await;
    let addr = mock.addr.clone();
    let executor = XaiExecutor::default().with_url_rewrite(move |url| redirect(url, &addr));
    let cfg = Config::default();
    let cred = credential(s, &cfg);
    let (recorder, _capture, sink) = crate::openai_compat_usage::sinks();
    let error = executor
        .execute(&cred, request(s, sink), &cfg, false)
        .await
        .err()
        .expect("the 500");
    assert_eq!(error.status, 500);
    let reports = recorder.0.lock().unwrap();
    assert_eq!(crate::openai_compat_usage::ttft_problem(&reports, true, false), None);
}

/// Go's TestXAISpeechRequestURLStaysOnOfficialAPI and
/// TestXAIExecutorExecuteSpeechUpstreamErrorScope: an OAuth credential on the chat proxy
/// still sends speech to the official `/tts`, every error keeps the upstream body, and
/// only a 404 that names no model problem is request-scoped (an explicit request scope
/// without upstream headers: neither rotated nor cooled).
#[tokio::test]
async fn speech_errors_keep_go_scope() {
    let cases: [(u16, &str, u16, bool); 15] = [
        (400, r#"{"error":"invalid language"}"#, 400, false),
        (400, r#"{"error":"requested model is not supported"}"#, 400, false),
        (404, r#"{"error":"voice not found"}"#, 404, true),
        (
            404,
            r#"{"error":{"code":"model_not_found","message":"The model grok-tts does not exist"}}"#,
            404,
            false,
        ),
        (
            404,
            r#"{"code":"model_not_found","error":"model unavailable"}"#,
            404,
            false,
        ),
        (
            404,
            r#"{"error":"The model grok-tts is not available for your account"}"#,
            404,
            false,
        ),
        (404, "model is not available", 404, false),
        (
            404,
            r#"{"error":{"message":"Unsupported model: grok-tts"}}"#,
            404,
            false,
        ),
        (422, r#"{"error":"text too long"}"#, 422, false),
        (
            422,
            r#"{"code":"model_not_supported","error":"model is not supported"}"#,
            422,
            false,
        ),
        (401, r#"{"error":"unauthorized"}"#, 401, false),
        (
            403,
            r#"{"code":"bad-credentials","error":"access token could not be validated"}"#,
            401,
            false,
        ),
        (403, r#"{"error":"forbidden"}"#, 403, false),
        (429, r#"{"error":"rate limited"}"#, 429, false),
        (500, r#"{"error":"boom"}"#, 500, false),
    ];
    let s = serde_json::json!({
        "config_auth": -1,
        "metadata": {"access_token": "xai-token"},
        "attributes": {"base_url": CLI_CHAT_PROXY_BASE_URL, "auth_kind": "oauth"},
        "op": "execute",
        "source": "openai",
        "payload": r#"{"text":"hello","voice_id":"nope","language":"auto"}"#,
        "model": "grok-tts",
    });
    let cfg = Config::default();
    for (status, body, want, scoped) in cases {
        let reply =
            serde_json::json!({"status": status, "headers": [["Content-Type", "application/json"]], "body": body});
        let mock = Mock::start(&reply).await;
        let (addr, target) = (mock.addr.clone(), Arc::new(Mutex::new(String::new())));
        let seen = target.clone();
        let executor = XaiExecutor::default().with_url_rewrite(move |url| {
            *seen.lock().unwrap() = url.to_owned();
            redirect(url, &addr)
        });
        let (_recorder, _capture, sink) = crate::openai_compat_usage::sinks();
        let error = executor
            .speech(&credential(&s, &cfg), request(&s, sink), &cfg)
            .await
            .err()
            .expect("an upstream error");
        assert_eq!(*target.lock().unwrap(), format!("{DEFAULT_API_BASE_URL}/tts"));
        assert_eq!(
            (error.status, String::from_utf8_lossy(&error.body).as_ref()),
            (want, body)
        );
        let request_scoped = error.scope == FailureScope::Request && error.headers.is_empty();
        assert_eq!(request_scoped, scoped, "{status} {body}");
    }
}

/// An image or video body reports its top-level usage as a buffered Responses body, so the
/// server reads its counts and billed units. A body without a billed value in its usage (absent,
/// null or empty), or a job's repeated report, is reported without its usage, so the record holds
/// no count. A video job is charged at the first poll whose usage
/// reports a value, an explicit zero included, and never again (E-Q13), while another job is
/// charged on its own. A poll of a job the process neither created nor charged, such as one
/// created before a restart, is refused with a request-scoped 404 before anything is sent. No
/// idempotency key reaches xAI, so replaying a create cannot bring back a charged job. A poll is
/// settled by the charge state it was admitted with: a job charged when the poll was admitted is
/// never charged again, even if its record expires while the poll is in flight, and an expired
/// job is refused.
#[tokio::test]
async fn media_usage_is_charged_once_per_response_or_job() {
    let cfg = Config::default();
    let target: Arc<Mutex<String>> = Arc::default();
    // The jobs' clock: its offset from `base` now, and the offset it moves to when the upstream
    // request is sent, after admission and before settlement.
    let base = std::time::Instant::now();
    let time: Arc<Mutex<(Duration, Option<Duration>)>> = Arc::default();
    // Two processes in turn: the second starts with none of the first's jobs.
    let processes: [XaiExecutor; 2] = std::array::from_fn(|_| {
        let (target, sent) = (target.clone(), time.clone());
        let mut executor = XaiExecutor::default().with_url_rewrite(move |url| {
            let mut time = sent.lock().unwrap();
            if let Some(next) = time.1.take() {
                time.0 = next;
            }
            redirect(url, &target.lock().unwrap())
        });
        let now = time.clone();
        executor.jobs = crate::xai_media::VideoJobs::with_clock(Arc::new(move || base + now.lock().unwrap().0));
        executor
    });
    let charged =
        r#"{"status":"done","video":{"url":"https://v.example/out.mp4"},"usage":{"cost_in_usd_ticks":5000000000}}"#;
    let image = r#"{"data":[{"b64_json":"AAAA"}],"usage":{"input_tokens":3,"output_tokens":0,"total_tokens":3,"cost_in_usd_ticks":200000000}}"#;
    const CREATE: &str = "/v1/videos/generations";
    let poll = |id: &str| (format!("/v1/videos/{id}"), format!(r#"{{"request_id":"{id}"}}"#));
    // (process, op, path, payload, upstream body, expected, clock): `Some(charged)` for a 200
    // and whether its usage was reported, `None` for a refusal that sends nothing upstream; the
    // clock's seconds at admission and at the upstream send, when the row sets them.
    type Row = (
        usize,
        &'static str,
        String,
        String,
        String,
        Option<bool>,
        Option<(u64, u64)>,
    );
    let mut rows: Vec<Row> = vec![
        (
            0,
            "images",
            "/v1/images/generations".into(),
            r#"{"prompt":"a"}"#.into(),
            image.into(),
            Some(true),
            None,
        ),
        (
            0,
            "images",
            "/v1/images/generations".into(),
            r#"{"prompt":"a"}"#.into(),
            image.into(),
            Some(true),
            None,
        ),
        (
            0,
            "images",
            "/v1/images/generations".into(),
            r#"{"prompt":"a"}"#.into(),
            r#"{"data":[{"b64_json":"AAAA"}]}"#.into(),
            Some(false),
            None,
        ),
        // A cache count without input, output or total is still a reported count.
        (
            0,
            "images",
            "/v1/images/generations".into(),
            r#"{"prompt":"a"}"#.into(),
            r#"{"data":[{"b64_json":"AAAA"}],"usage":{"input_tokens_details":{"cached_tokens":3}}}"#.into(),
            Some(true),
            None,
        ),
    ];
    let mut create = |process: usize, body: &str| {
        rows.push((
            process,
            "videos",
            CREATE.into(),
            r#"{"prompt":"a"}"#.into(),
            body.into(),
            Some(false),
            None,
        ));
    };
    for id in ["vid_9", "vid_10", "vid_11", "vid_12", "vid_30", "vid_31"] {
        create(0, &format!(r#"{{"request_id":"{id}"}}"#));
    }
    // A create answered with `id` instead of `request_id`.
    create(0, r#"{"id":"vid_13","status":"in_progress"}"#);
    create(1, r#"{"request_id":"vid_20"}"#);
    // A replay of vid_9's create, with its idempotency key, after the restart. The key never
    // reaches xAI, which documents none, so the upstream answers with a new job.
    create(1, r#"{"request_id":"vid_21"}"#);
    const DAY: u64 = 24 * 3600;
    for (process, id, body, expected, clock) in [
        (0, "vid_9", r#"{"status":"pending","usage":null}"#, Some(false), None),
        (0, "vid_9", charged, Some(true), None),
        (0, "vid_9", charged, Some(false), None),
        (0, "vid_10", charged, Some(true), None),
        // A usage without a billed value is not the job's report; its later charge still counts.
        (0, "vid_11", r#"{"status":"pending","usage":{}}"#, Some(false), None),
        (
            0,
            "vid_11",
            r#"{"status":"pending","usage":{"input_tokens_details":{},"cost_in_usd_ticks":null}}"#,
            Some(false),
            None,
        ),
        (0, "vid_11", charged, Some(true), None),
        // An explicit zero charge is a report.
        (
            0,
            "vid_12",
            r#"{"status":"done","usage":{"cost_in_usd_ticks":0}}"#,
            Some(true),
            None,
        ),
        (0, "vid_13", charged, Some(true), None),
        // Never created through this process.
        (0, "vid_99", charged, None, None),
        // After a restart: the charged job is unknown and refused; a new job is charged.
        (1, "vid_9", charged, None, None),
        (1, "vid_11", charged, None, None),
        (1, "vid_20", charged, Some(true), None),
        (1, "vid_21", charged, Some(true), None),
        (1, "vid_9", charged, None, None),
        // A day after creation and charge: vid_30 was charged at second 0. A poll admitted at
        // 86,399 s and answered at 86,401 s settles as charged; the next poll finds it expired.
        // vid_31, created at second 0 and never charged, is charged once by such a poll.
        (0, "vid_30", charged, Some(true), None),
        (0, "vid_30", charged, Some(false), Some((DAY - 1, DAY + 1))),
        (0, "vid_31", charged, Some(true), Some((DAY - 1, DAY + 1))),
        (0, "vid_30", charged, None, Some((DAY + 2, DAY + 2))),
        (0, "vid_31", charged, Some(false), Some((DAY + 2, DAY + 2))),
    ] {
        let (path, payload) = poll(id);
        rows.push((process, "videos", path, payload, body.into(), expected, clock));
    }
    for (process, op, path, payload, body, expected, clock) in rows {
        if let Some((admitted, sent)) = clock {
            *time.lock().unwrap() = (Duration::from_secs(admitted), Some(Duration::from_secs(sent)));
        }
        let mock = Mock::start(
            &serde_json::json!({"status": 200, "headers": [["Content-Type", "application/json"]], "body": body}),
        )
        .await;
        *target.lock().unwrap() = mock.addr.clone();
        let (s, recorder, req) = media_request(op, &path, &payload);
        let executor = &processes[process];
        let result = match op {
            "images" => executor.images(&credential(&s, &cfg), req, &path, &cfg).await,
            _ => executor.videos(&credential(&s, &cfg), req, &path, &cfg).await,
        };
        let row = format!("process {process} {op} {path} {body}");
        let sent = mock.request().unwrap_or_default().go_lower();
        assert!(
            !sent.contains("idempotency"),
            "{row}: no idempotency key goes upstream:\n{sent}"
        );
        let reports = recorder.0.lock().unwrap();
        let bodies: Vec<String> = reports
            .iter()
            .filter_map(|r| match r {
                crate::openai_compat_usage::Report::Body(Format::Codex, data) => {
                    Some(String::from_utf8_lossy(data).into_owned())
                }
                _ => None,
            })
            .collect();
        let charged_now = match (expected, result) {
            (Some(charged_now), Ok(_)) => charged_now,
            (None, Err(error)) => {
                assert_eq!((error.status, error.scope), (404, FailureScope::Request), "{row}");
                let message: Value = serde_json::from_slice(&error.body).expect("a JSON error");
                assert_eq!(message["error"]["type"], "invalid_request_error", "{row}");
                assert_eq!(mock.request(), None, "{row}: nothing is sent upstream");
                false
            }
            (expected, result) => panic!("{row}: expected {expected:?}, got {:?}", result.map(|_| ())),
        };
        // A charged response reports its body as is; any other answered one reports its body
        // without `usage`, so the record holds no count; a refusal reports nothing.
        let want: Vec<String> = match expected {
            Some(true) => vec![body.clone()],
            Some(false) => {
                let mut quiet = body.clone().into_bytes();
                gj::delete(&mut quiet, "usage");
                vec![String::from_utf8(quiet).unwrap()]
            }
            None => Vec::new(),
        };
        assert_eq!(bodies, want, "{row}");
        assert_eq!(charged_now, expected == Some(true), "{row}");
    }
    // Two polls of vid_40 admitted before its charge: the first is held upstream while the
    // second charges. Past the job's expiry a new poll is refused, and the held poll then settles
    // and charges nothing.
    let executor = &processes[0];
    let created = Mock::start(
        &serde_json::json!({"status": 200, "headers": [["Content-Type", "application/json"]], "body": r#"{"request_id":"vid_40"}"#}),
    )
    .await;
    *target.lock().unwrap() = created.addr.clone();
    let (s, _, req) = media_request("videos", CREATE, r#"{"prompt":"a"}"#);
    executor
        .videos(&credential(&s, &cfg), req, CREATE, &cfg)
        .await
        .expect("the create");
    let (path, payload) = poll("vid_40");
    let (held, arrived, release) = held_upstream(charged).await;
    *target.lock().unwrap() = held;
    let (s, first, req) = media_request("videos", &path, &payload);
    let held_credential = credential(&s, &cfg);
    let held_poll = executor.videos(&held_credential, req, &path, &cfg);
    tokio::pin!(held_poll);
    tokio::select! {
        _ = &mut held_poll => panic!("the held poll answered before its release"),
        _ = arrived => {}
    }
    let answered = Mock::start(
        &serde_json::json!({"status": 200, "headers": [["Content-Type", "application/json"]], "body": charged}),
    )
    .await;
    *target.lock().unwrap() = answered.addr.clone();
    let (s, second, req) = media_request("videos", &path, &payload);
    executor
        .videos(&credential(&s, &cfg), req, &path, &cfg)
        .await
        .expect("the second poll");
    time.lock().unwrap().0 += Duration::from_secs(DAY + 2);
    let (s, _, req) = media_request("videos", &path, &payload);
    let refused = executor.videos(&credential(&s, &cfg), req, &path, &cfg).await;
    assert_eq!(refused.err().map(|e| e.status), Some(404), "vid_40 expired");
    release.send(()).unwrap();
    held_poll.await.expect("the held poll");
    let charges = |recorder: &crate::openai_compat_usage::Recorder| {
        let reports = recorder.0.lock().unwrap();
        reports
            .iter()
            .filter(
                |r| matches!(r, crate::openai_compat_usage::Report::Body(_, data) if gj::get(data, "usage").exists()),
            )
            .count()
    };
    assert_eq!((charges(&second), charges(&first)), (1, 0), "vid_40 is charged once");
}

/// A media request in the shape the scenarios use; every create carries the same client
/// idempotency key.
fn media_request(
    op: &str,
    path: &str,
    payload: &str,
) -> (Value, Arc<crate::openai_compat_usage::Recorder>, ExecRequest) {
    let s = serde_json::json!({
        "config_auth": -1,
        "attributes": {"api_key": "sk-fake-xai"},
        "op": op,
        "source": "openai",
        "payload": payload,
        "model": "grok-imagine",
        "request_path": path,
        "headers": {"Idempotency-Key": "replay-1", "x-idempotency-key": "replay-1"},
    });
    let (recorder, _capture, sink) = crate::openai_compat_usage::sinks();
    let req = request(&s, sink);
    (s, recorder, req)
}

/// An upstream that answers one request with `body` once released, and reports when the request
/// arrived.
async fn held_upstream(
    body: &'static str,
) -> (
    String,
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (arrived_tx, arrived) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let (read, mut write) = socket.into_split();
        let mut reader = BufReader::new(read);
        loop {
            let mut line = Vec::new();
            if reader.read_until(b'\n', &mut line).await.unwrap_or(0) == 0 || line == b"\r\n" {
                break;
            }
        }
        let _ = arrived_tx.send(());
        let _ = released.await;
        let out = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = write.write_all(out.as_bytes()).await;
        let _ = write.shutdown().await;
    });
    (addr, arrived, release)
}

/// A video job's charge is claimed once whatever order its admitted polls settle in. Expiry
/// refuses new polls only: a record stays while an admitted poll holds it.
#[test]
fn video_job_charge_is_claimed_once_across_interleavings() {
    use crate::xai_media::VideoJobs;
    enum Step {
        Clock(u64),
        Create(&'static str),
        /// (slot, job, admitted)
        Admit(usize, &'static str, bool),
        /// (slot, charged)
        Settle(usize, bool),
        Release(usize),
    }
    use Step::*;
    const DAY: u64 = 24 * 3600;
    let rows = [
        (
            "two polls admitted uncharged; one charges, a poll past expiry is refused, the other settles",
            vec![
                Create("a"),
                Admit(0, "a", true),
                Admit(1, "a", true),
                Clock(1),
                Settle(0, true),
                Release(0),
                Clock(DAY + 2),
                Admit(2, "a", false),
                Settle(1, false),
                Release(1),
                Admit(2, "a", false),
            ],
        ),
        (
            "an expired job admitted before expiry: no new poll, its first settlement charges",
            vec![
                Create("b"),
                Admit(0, "b", true),
                Clock(DAY + 1),
                Admit(1, "b", false),
                Settle(0, true),
                Release(0),
                Admit(1, "b", true),
                Settle(1, false),
            ],
        ),
        (
            "two first settlements: one charges",
            vec![
                Create("c"),
                Admit(0, "c", true),
                Admit(1, "c", true),
                Settle(1, true),
                Settle(0, false),
            ],
        ),
        ("a job never created", vec![Admit(0, "d", false)]),
    ];
    for (name, steps) in rows {
        let time: Arc<Mutex<Duration>> = Arc::default();
        let base = std::time::Instant::now();
        let now = time.clone();
        let jobs = VideoJobs::with_clock(Arc::new(move || base + *now.lock().unwrap()));
        let mut held: [Option<crate::xai_media::Admission<'_>>; 3] = [None, None, None];
        for (i, step) in steps.into_iter().enumerate() {
            match step {
                Clock(seconds) => *time.lock().unwrap() = Duration::from_secs(seconds),
                Create(job) => drop(jobs.created(job)),
                Admit(slot, job, admitted) => {
                    held[slot] = jobs.admit(job);
                    assert_eq!(held[slot].is_some(), admitted, "{name}: step {i}");
                }
                Settle(slot, charged) => {
                    let admission = held[slot].as_ref().expect("an admitted poll");
                    assert_eq!(admission.settle(), charged, "{name}: step {i}");
                }
                Release(slot) => held[slot] = None,
            }
        }
    }
}

/// TestXAIExecutorCompactUsesCompactEndpoint (868e137): compact's deletes run before the
/// payload rules, so a configured `top_k` survives while the deleted fields stay gone.
#[tokio::test]
async fn compact_keeps_a_configured_top_k() {
    let fixture: Value = serde_json::from_str(FIXTURE).unwrap();
    let s = fixture["scenarios"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == "compact_oauth_uses_official_api")
        .unwrap();
    let mock = Mock::start(&s["upstream"]).await;
    let addr = mock.addr.clone();
    let executor = XaiExecutor::default().with_url_rewrite(move |url| redirect(url, &addr));
    let cfg = Config::parse(
        "requests:\n  payload:\n    override:\n      - models: [{name: \"*\"}]\n        params: {top_k: 10}\n",
    )
    .unwrap();
    let cred = credential(s, &cfg);
    executor
        .execute(&cred, request(s, Default::default()), &cfg, false)
        .await
        .unwrap();
    let wire = mock.request().unwrap();
    let body = wire.split_once("\r\n\r\n").unwrap().1.as_bytes().to_vec();
    assert_eq!(gj::get(&body, "top_k").int(), 10, "{wire}");
    for absent in ["stream", "tools", "max_output_tokens", "temperature", "top_p", "stop"] {
        assert!(!gj::get(&body, absent).exists(), "{absent}: {wire}");
    }
}

/// Go ignores a refusal only the original (baseline) body hits
/// (TranslateRequestPairWithAPIKeyModelCompatibilityAndUpdateIntent): the working body is
/// the one sent, and only its own refusal refuses the request.
#[tokio::test]
async fn a_refused_baseline_alone_does_not_refuse_the_request() {
    let fixture: Value = serde_json::from_str(FIXTURE).unwrap();
    let mut s = fixture["scenarios"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == "claude_execute")
        .unwrap()
        .clone();
    s["original"] = Value::String(
        r#"{"model":"grok-4.3","max_tokens":100,"messages":[{"role":"user","content":[{"type":"container_upload","source":{"type":"file","file_id":"f1"}}]}]}"#.into(),
    );
    let run = |s: Value| async move {
        let mock = Mock::start(&s["upstream"]).await;
        let addr = mock.addr.clone();
        let executor = XaiExecutor::default().with_url_rewrite(move |url| redirect(url, &addr));
        let cfg = Config::parse("").unwrap();
        let cred = credential(&s, &cfg);
        let ok = executor
            .execute(&cred, request(&s, Default::default()), &cfg, false)
            .await
            .is_ok();
        (ok, mock.request())
    };
    let (ok, wire) = run(s.clone()).await;
    assert!(ok);
    assert!(wire.unwrap().contains("hello"));
    s["payload"] = s["original"].clone();
    assert_eq!(run(s).await, (false, None));
}

/// Whether a recorded upstream reply is a JSON document rather than an event stream.
fn json_reply(upstream: &serde_json::Value) -> bool {
    upstream["headers"].as_array().into_iter().flatten().any(|h| {
        h[0].as_str().is_some_and(|n| n.eq_ignore_ascii_case("content-type"))
            && h[1].as_str().is_some_and(|v| v.starts_with("application/json"))
    })
}
