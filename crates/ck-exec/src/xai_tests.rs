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
        // The image and video scenarios record Go's media executor; the media routes run on the
        // execution contract, whose Go parity is `ck-server/tests/media_go.rs`.
        if matches!(s["op"].as_str(), Some("images" | "videos")) {
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
        let result = executor
            .execute(&cred, req, &cfg, s["websocket"].as_bool() == Some(true))
            .await;
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

/// Go's TestXAISpeechRequestURLStaysOnOfficialAPI: an OAuth credential on the CLI chat proxy
/// sends speech to the official API, and its other media to the proxy.
#[test]
fn oauth_speech_stays_on_the_official_api() {
    let s = serde_json::json!({
        "config_auth": -1,
        "metadata": {"access_token": "xai-token"},
        "attributes": {"base_url": CLI_CHAT_PROXY_BASE_URL, "auth_kind": "oauth"},
    });
    let credential = credential(&s, &Config::default());
    assert_eq!(
        crate::xai_media::media_base_url(&credential, true),
        DEFAULT_API_BASE_URL
    );
    assert_eq!(
        crate::xai_media::media_base_url(&credential, false),
        CLI_CHAT_PROXY_BASE_URL
    );
}

/// The video job table (E-Q5, E-Q13): a job's charge is claimed once whatever order its admitted
/// polls settle in; a job id is found only by the caller that created it; a record expires three
/// hours after its last create or poll, though an admitted poll keeps it; and the table holds at
/// most its capacity of jobs and pending creates, refusing the next create until one frees. Image
/// and video jobs share the table and its capacity, and a poll of one kind never finds the other.
#[test]
fn video_job_table_scopes_bounds_and_charges_once() {
    use crate::xai_media::{JobBinding, JobKind, Slot, VideoJobs};
    enum Step {
        Clock(u64),
        /// A create by caller `who` answered with the job.
        Create(usize, &'static str),
        /// (slot, caller, job, admitted)
        Admit(usize, usize, &'static str, bool),
        /// (slot, charged)
        Settle(usize, bool),
        /// Poll `a` fails to publish its charge while poll `b` settles the same job: (a, b).
        FailWhileSettling(usize, usize),
        Release(usize),
        /// A create reserves its place before sending: (pending, caller, reserved).
        Reserve(usize, usize, bool),
        /// The pending create's response names the job.
        Named(usize, &'static str),
        /// The pending create failed.
        Failed(usize),
        /// A create of an image job by caller `who` answered with the job.
        CreateImage(usize, &'static str),
        /// (slot, caller, kind, job, admitted)
        AdmitAs(usize, usize, JobKind, &'static str, bool),
        /// An image create reserves its place: (pending, caller, reserved).
        ReserveImage(usize, usize, bool),
    }
    use Step::*;
    const H: u64 = 3 * 3600;
    let rows = [
        (
            "two polls admitted uncharged; one charges, a poll past expiry is refused, the other settles",
            2,
            vec![
                Create(0, "a"),
                Admit(0, 0, "a", true),
                Admit(1, 0, "a", true),
                Clock(1),
                Settle(0, true),
                Release(0),
                Clock(H + 2),
                Admit(2, 0, "a", false),
                Settle(1, false),
                Release(1),
                Admit(2, 0, "a", false),
            ],
        ),
        (
            "a job admitted before expiry: no new poll after it, its first settlement charges",
            2,
            vec![
                Create(0, "b"),
                Admit(0, 0, "b", true),
                Clock(H + 1),
                Admit(1, 0, "b", false),
                Settle(0, true),
                Release(0),
                Admit(1, 0, "b", false),
            ],
        ),
        (
            "a charge whose publication fails while another poll settles is that poll's",
            2,
            vec![
                Create(0, "z"),
                Admit(0, 0, "z", true),
                Admit(1, 0, "z", true),
                FailWhileSettling(0, 1),
                Settle(0, false),
            ],
        ),
        (
            "two first settlements: one charges",
            2,
            vec![
                Create(0, "c"),
                Admit(0, 0, "c", true),
                Admit(1, 0, "c", true),
                Settle(1, true),
                Settle(0, false),
            ],
        ),
        ("a job never created", 2, vec![Admit(0, 0, "d", false)]),
        (
            "another caller's job is not found, and its own caller's still is",
            2,
            vec![Create(0, "e"), Admit(0, 1, "e", false), Admit(1, 0, "e", true)],
        ),
        (
            "a poll restarts expiry: three hours after creation a polled job is live, three after its poll it is gone",
            2,
            vec![
                Create(0, "f"),
                Clock(H - 1),
                Admit(0, 0, "f", true),
                Release(0),
                Clock(H + 1),
                Admit(0, 0, "f", true),
                Release(0),
                Clock(2 * H + 1),
                Admit(0, 0, "f", false),
            ],
        ),
        (
            "a full table refuses a create; a failed create frees its place",
            2,
            vec![
                Reserve(0, 0, true),
                Reserve(1, 1, true),
                Reserve(2, 0, false),
                Failed(0),
                Reserve(2, 0, true),
                Named(1, "g"),
                Reserve(0, 1, false),
            ],
        ),
        (
            "an expired job frees its place for the next create; an admitted one does not",
            2,
            vec![
                Create(0, "h"),
                Create(1, "i"),
                Admit(0, 0, "h", true),
                Clock(H),
                Reserve(0, 0, true),
                Reserve(1, 1, false),
            ],
        ),
        (
            "an image job is not found on a video poll, nor a video job on an image poll",
            2,
            vec![
                CreateImage(0, "j"),
                AdmitAs(0, 0, JobKind::Video, "j", false),
                AdmitAs(0, 0, JobKind::Image, "j", true),
                Create(0, "k"),
                AdmitAs(1, 0, JobKind::Image, "k", false),
                AdmitAs(1, 0, JobKind::Video, "k", true),
            ],
        ),
        (
            "image and video jobs share one capacity",
            2,
            vec![
                CreateImage(0, "l"),
                Reserve(0, 1, true),
                ReserveImage(1, 0, false),
                Reserve(2, 1, false),
                Failed(0),
                ReserveImage(1, 0, true),
            ],
        ),
    ];
    let callers: [ck_core::exec::Caller; 2] = std::array::from_fn(|i| ck_core::exec::Caller {
        principal: format!("client-key-{i}"),
        source: "authorization",
        provider: "config-inline".into(),
    });
    for (name, capacity, steps) in rows {
        let time: Arc<Mutex<Duration>> = Arc::default();
        let base = std::time::Instant::now();
        let now = time.clone();
        let jobs = VideoJobs::with_clock(Arc::new(move || base + *now.lock().unwrap()), capacity);
        let mut held: [Option<crate::xai_media::Admission>; 3] = [None, None, None];
        let mut pending: [Option<Slot>; 3] = [None, None, None];
        let binding = || JobBinding {
            credential: "xai-1".into(),
            model: "grok-imagine-video".into(),
        };
        for (i, step) in steps.into_iter().enumerate() {
            match step {
                Clock(seconds) => *time.lock().unwrap() = Duration::from_secs(seconds),
                Create(who, job) => drop(
                    jobs.reserve(&callers[who], JobKind::Video, binding())
                        .unwrap()
                        .created(job),
                ),
                CreateImage(who, job) => drop(
                    jobs.reserve(&callers[who], JobKind::Image, binding())
                        .unwrap()
                        .created(job),
                ),
                Admit(slot, who, job, admitted) => {
                    held[slot] = jobs.admit(&callers[who], JobKind::Video, job);
                    assert_eq!(held[slot].is_some(), admitted, "{name}: step {i}");
                    assert_eq!(
                        jobs.binding(&callers[who], JobKind::Video, job).is_some(),
                        admitted,
                        "{name}: step {i}, binding"
                    );
                }
                AdmitAs(slot, who, kind, job, admitted) => {
                    held[slot] = jobs.admit(&callers[who], kind, job);
                    assert_eq!(held[slot].is_some(), admitted, "{name}: step {i}");
                    assert_eq!(
                        jobs.binding(&callers[who], kind, job).is_some(),
                        admitted,
                        "{name}: step {i}, binding"
                    );
                }
                Settle(slot, charged) => {
                    let admission = held[slot].as_ref().expect("an admitted poll");
                    assert_eq!(admission.settle(), charged, "{name}: step {i}");
                }
                FailWhileSettling(a, b) => {
                    let other = held[b].take().expect("an admitted poll");
                    let mut racer = None;
                    let failed = held[a].as_ref().expect("an admitted poll").settle_with(|| {
                        let settling = std::thread::spawn(move || other.settle());
                        std::thread::sleep(std::time::Duration::from_millis(50));
                        assert!(!settling.is_finished(), "{name}: step {i}, the other poll waits");
                        racer = Some(settling);
                        Err(())
                    });
                    assert_eq!(failed, Err(()), "{name}: step {i}");
                    let charged = racer.unwrap().join().unwrap();
                    assert!(charged, "{name}: step {i}, the waiting poll charges the job");
                }
                Release(slot) => held[slot] = None,
                Reserve(slot, who, reserved) => {
                    pending[slot] = jobs.reserve(&callers[who], JobKind::Video, binding());
                    assert_eq!(pending[slot].is_some(), reserved, "{name}: step {i}");
                }
                ReserveImage(slot, who, reserved) => {
                    pending[slot] = jobs.reserve(&callers[who], JobKind::Image, binding());
                    assert_eq!(pending[slot].is_some(), reserved, "{name}: step {i}");
                }
                Named(slot, job) => drop(pending[slot].take().unwrap().created(job)),
                Failed(slot) => pending[slot] = None,
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

/// A deferred xAI Chat job (X11): its create returns xAI's request id, only the creator's
/// polls are admitted and only as a Chat job, pending is an empty 202, a completion is the
/// Chat response charged once however often it is polled, a failed job is an error, and an
/// expired job is not found. Nothing is retried after a send.
#[tokio::test]
async fn deferred_chat_jobs_bind_to_their_caller_kind_and_charge_once() {
    let cfg = Config::default();
    let scenario = serde_json::json!({
        "config_auth": -1, "metadata": {},
        "attributes": {"api_key":"fake-upstream", "using_api":"true"},
        "op":"execute", "source":"openai", "model":"grok-4(low)", "caller_key":"owner",
        "payload":r#"{"model":"grok-4(low)","messages":[{"role":"user","content":"hi"}],"deferred":true}"#
    });
    let target: Arc<Mutex<String>> = Arc::default();
    let sent = target.clone();
    let mut executor = XaiExecutor::default().with_url_rewrite(move |url| redirect(url, &sent.lock().unwrap()));
    let clock: Arc<Mutex<Duration>> = Arc::default();
    let time = clock.clone();
    let base = std::time::Instant::now();
    executor.jobs = crate::xai_media::VideoJobs::with_clock(
        Arc::new(move || base + *time.lock().unwrap()),
        crate::xai_media::JOB_CAPACITY,
    );
    let complete = r#"{"id":"done","object":"chat.completion","created":1,"model":"grok-4","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}"#;
    let mut job = String::new();
    // (case, upstream status/body, expected status, upstream requests)
    let rows = [
        ("create", (200, r#"{"request_id":"job/1"}"#), Ok(200), true),
        ("pending", (202, ""), Ok(202), true),
        ("completed", (200, complete), Ok(200), true),
        ("repeated", (200, complete), Ok(200), true),
        ("failed poll", (500, r#"{"error":{"message":"boom"}}"#), Err(500), true),
        (
            "failed job",
            (200, r#"{"error":{"message":"job failed"}}"#),
            Err(502),
            true,
        ),
        ("other caller", (200, ""), Err(404), false),
        ("video id", (200, ""), Err(404), false),
        ("missing id", (200, r#"{"choices":[]}"#), Err(502), true),
        ("expired", (200, ""), Err(404), false),
        ("oauth", (200, ""), Err(400), false),
        ("stream", (200, ""), Err(400), false),
    ];
    for (case, (status, reply), expected, sends) in rows {
        let mock = Mock::start(&serde_json::json!({"status": status, "body": reply})).await;
        *target.lock().unwrap() = mock.addr.clone();
        let (recorder, _capture, sink) = crate::openai_compat_usage::sinks();
        let mut req = request(&scenario, sink);
        let mut c = credential(&scenario, &cfg);
        let poll = !matches!(case, "create" | "missing id" | "oauth" | "stream");
        if poll {
            req.alt = Some(crate::DEFERRED_CHAT_POLL.into());
            req.body = Bytes::from(serde_json::json!({"request_id": &job}).to_string());
        }
        match case {
            "pending" => *clock.lock().unwrap() = Duration::from_secs(10),
            "completed" => *clock.lock().unwrap() = Duration::from_secs(20),
            "repeated" => *clock.lock().unwrap() = Duration::from_secs(30),
            "failed poll" | "failed job" => *clock.lock().unwrap() = Duration::from_secs(40),
            "other caller" => req.caller.principal = "other".into(),
            "video id" => {
                drop(executor.jobs.created(&req.caller, "video"));
                req.body = Bytes::from_static(br#"{"request_id":"video"}"#);
            }
            "expired" => *clock.lock().unwrap() = Duration::from_secs(40 + 3 * 3600 + 1),
            "oauth" => {
                c.attributes.insert("using_api".into(), "false".into());
            }
            "stream" => req.body = Bytes::from_static(br#"{"model":"grok-4","stream":true,"deferred":true}"#),
            _ => {}
        }
        let result = executor.deferred_chat(&c, req, &cfg).await;
        match result {
            Ok(response) => {
                assert_eq!(response.status, expected.unwrap(), "{case}");
                let ResponseBody::Buffered(bytes) = response.body else {
                    panic!("{case}: deferred Chat is buffered")
                };
                if case == "create" {
                    let value: Value = serde_json::from_slice(&bytes).unwrap();
                    job = value["request_id"].as_str().expect("the job id").to_owned();
                    assert_eq!(job, "job/1", "{case}");
                } else if case == "pending" {
                    assert!(bytes.is_empty(), "{case}: an empty 202, not keepalive bytes");
                } else if matches!(case, "completed" | "repeated") {
                    let value: Value = serde_json::from_slice(&bytes).unwrap();
                    assert_eq!(value["choices"][0]["message"]["content"], "ok", "{case}");
                }
            }
            Err(error) => assert_eq!(
                (error.status, error.scope),
                (expected.unwrap_err(), FailureScope::Request),
                "{case}: {error}"
            ),
        }
        let raw = mock.request();
        assert_eq!(raw.is_some(), sends, "{case}: one request or none");
        if let Some(raw) = raw {
            let path = if matches!(case, "create" | "missing id") {
                "POST /v1/chat/completions "
            } else {
                "GET /v1/chat/deferred-completion/job%2F1 "
            };
            assert!(raw.starts_with(path), "{case}: {raw}");
            if matches!(case, "create" | "missing id") {
                let sent: Value = serde_json::from_str(raw.split_once("\r\n\r\n").unwrap().1).unwrap();
                assert_eq!(sent["deferred"], true, "{case}");
                assert_eq!(sent["model"], "grok-4", "{case}");
            }
        }
        let reports = recorder.0.lock().unwrap();
        let charges = reports
            .iter()
            .filter(|report| {
                matches!(report, crate::openai_compat_usage::Report::Body(_, data) if gj::get(data, "usage").exists())
            })
            .count();
        assert_eq!(
            charges,
            usize::from(case == "completed"),
            "{case}: the completion is charged once"
        );
    }
}
