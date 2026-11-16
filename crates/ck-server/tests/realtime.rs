//! Realtime and Live through the real router against loopback upstreams. Fake tokens only.
//!
//! Go test cases (internal/client/codex/live) whose behaviour these goldens and the unit
//! tests in ck-exec `codex_live` and ck-server `realtime` reproduce:
//! - capabilities_test.go: TestHandleHangupForwardsPinnedOAuthCall, TestHandleHangupRejectsDifferentAPIPrincipal,
//!   TestUnsupportedRealtimeCapabilitiesUseStandardError.
//! - client_secret_test.go: TestCreateClientSecretMapsStandardRealtimeModel, TestStandardRealtimeCallMapsModelAndLocation,
//!   TestClientSecretStoreRejectsExpiredToken, TestNormalizeClientSecretSessionHandlesWhitespaceNullAndRejectsArrays,
//!   TestReadClientSecretBodyRejectsOversizedSession, TestCreateClientSecretRejectsUnsupportedSessionType,
//!   TestSidebandRejectsClientSecretScopeMismatch, TestSidebandRejectsStandardPrincipalScopeMismatch,
//!   TestApplyClientSecretCallSession.
//! - live_test.go: TestHandlerRewritesLiveCallAndSchedulesOAuth, TestProxyURLForAuthPrefersCredentialOverride,
//!   TestHandlerRelaysWebRTCMediaSDP, TestHandlerClosesUnretainedMediaSession, TestHandlerClosesMediaWhenResponseWriteFails,
//!   TestHandleSidebandPinsAuthAndRelaysBidirectionally, TestHandleSidebandDialErrorDoesNotForwardNonUnauthorizedBody,
//!   TestPrepareCallRequestRewritesMultipart, TestPrepareCallRequestPreservesRawSDPWhenRelayDisabled,
//!   TestMediaRelayWrapsRawSDPForCodexBackend, TestHandlerUpdatesMediaRelayConfig, TestPrepareCallRequestRejectsInvalidMultipart,
//!   TestSessionStoreClaimsAndExpiresSessions, TestSessionStoreCloseAllReleasesMediaAndResources, TestSidebandURLShapes.
//! - media_test.go: TestPionMediaRelayBridgesAudioAndDataChannel, TestIsPublicRemoteIP.
//! - websocket_test.go: TestHandleDirectWebsocketRejectsClientSecretModelMismatch,
//!   TestHandleDirectWebsocketAppliesClientSecretSession, TestHandleDirectWebsocketRelaysStandardRealtimeFrames.
//! - internal/config/codex_live_test.go: TestCodexLiveMediaRelayConfigMigratesLegacyPrivateIPSetting.
//!
//! Not reproduced: the Home dispatch cases (no Home selection in ck-server yet), request
//! logging (TestHeadersForLoggingRedactsAttestation; no request-log writer yet),
//! TestReadLimitedBodyPreservesPayloadOnReadError, TestMediaCredentialNameUsesSafeIdentity
//! beyond the label case, and the proxied media cases (TestPionMediaRelaySelectsRemoteProxyMode,
//! TestMediaForwardingStartedLogRedactsProxyCredentials, tcp_proxy_test.go): proxied relays fail closed.
//!
//! `realtime_http_go.json` comes from tests/reference/realtime/zz_rsfix_realtime_http_test.go:
//! the real Go server (routes, access manager, realtime middleware, live handler) with an
//! executor that records the upstream request instead of sending it. Each case replays
//! here in order, against the same two servers, with a mock upstream returning Go's
//! canned response.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::IntoResponse;
use ck_core::config::Config;
use ck_core::credential::Credential;
use ck_exec::Executors;
use ck_exec::codex::CodexExecutor;
use ck_exec::codex_oauth::CodexOAuth;
use ck_server::router;
use serde_json::Value;

#[derive(Default)]
struct Mock {
    next: Option<Value>,
    seen: Vec<(Uri, HeaderMap, Bytes)>,
}

type Shared = Arc<Mutex<Mock>>;

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    url
}

fn credential(id: &str, meta: Value, attrs: &[(&str, &str)]) -> Credential {
    let mut c = Credential::from_file(
        Path::new("/fake"),
        &Path::new("/fake").join(id),
        meta.as_object().unwrap().clone(),
    )
    .unwrap();
    c.attributes = attrs
        .iter()
        .map(|(k, v)| ((*k).into(), (*v).into()))
        .collect::<BTreeMap<_, _>>();
    c
}

/// A proxy whose live upstream is a loopback mock answering with `mock.next`.
async fn proxy(credentials: Vec<Credential>, mock: Shared) -> String {
    let upstream = axum::Router::new()
        .fallback(
            |State(mock): State<Shared>, uri: Uri, headers: HeaderMap, body: Bytes| async move {
                let mut mock = mock.lock().unwrap();
                mock.seen.push((uri, headers, body));
                let Some(next) = mock.next.clone() else {
                    return (StatusCode::from_u16(599).unwrap(), "unexpected upstream call").into_response();
                };
                let mut response = (
                    StatusCode::from_u16(next["status"].as_u64().unwrap() as u16).unwrap(),
                    next["body"].as_str().unwrap_or_default().to_owned(),
                )
                    .into_response();
                response.headers_mut().remove("content-type");
                if let Some(headers) = next["headers"].as_object() {
                    for (name, values) in headers {
                        for value in values.as_array().unwrap() {
                            response.headers_mut().append(
                                name.parse::<axum::http::HeaderName>().unwrap(),
                                value.as_str().unwrap().parse().unwrap(),
                            );
                        }
                    }
                }
                response
            },
        )
        .with_state(mock);
    let upstream_url = serve(upstream).await;
    let executor = CodexExecutor::with_client(wreq::Client::new(), CodexOAuth::new(wreq::Client::new()))
        .with_live_endpoints(
            format!("{upstream_url}/backend-api/codex/realtime/calls?intent=quicksilver&architecture=avas"),
            format!("ws{}/v1", upstream_url.trim_start_matches("http")),
        );
    let rt = Arc::new(ck_server::testing::runtime(
        Config::parse("access: {api-keys: [good-key, other-key]}").unwrap(),
        credentials,
        Executors {
            claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
            codex: executor,
            devices: Default::default(),
            openai: Default::default(),
            google: Default::default(),
        },
    ));
    serve(router(rt)).await
}

fn go_credentials() -> Vec<Credential> {
    vec![
        credential(
            "a-codex-apikey",
            serde_json::json!({"type":"codex"}),
            &[("api_key", "sk-must-not-be-used")],
        ),
        credential(
            "b-codex-oauth",
            serde_json::json!({"type":"codex","access_token":"oauth-token","account_id":"acct-1","email":"user@example.com"}),
            &[("header:X-Operator", "op-value")],
        ),
    ]
}

/// The masking the Go harness applies to random values.
fn mask(text: &str, aliases: &BTreeMap<String, String>) -> String {
    let mut text = text.to_owned();
    for (real, alias) in aliases {
        text = text.replace(real, alias);
    }
    let re = |pattern: &str, with: &str, text: &str| regex_lite(pattern, with, text);
    let text = re("ek_", "ek_<secret>", &text);
    let text = re("sess_", "sess_<id>", &text);
    // "expires_at":<digits>
    let mut out = String::new();
    let mut rest = text.as_str();
    while let Some(i) = rest.find("\"expires_at\":") {
        out.push_str(&rest[..i + 13]);
        rest = &rest[i + 13..];
        let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
        out.push_str(if digits > 0 { "0" } else { "" });
        rest = &rest[digits..];
    }
    out.push_str(rest);
    out
}

/// Replaces `prefix` + a base64url run of Go's random length with `with`.
fn regex_lite(prefix: &str, with: &str, text: &str) -> String {
    let len = if prefix == "ek_" { 43 } else { 24 };
    let mut out = String::new();
    let mut rest = text;
    while let Some(i) = rest.find(prefix) {
        let tail = &rest[i + prefix.len()..];
        let run = tail
            .bytes()
            .take_while(|b| b.is_ascii_alphanumeric() || *b == b'-' || *b == b'_')
            .count();
        out.push_str(&rest[..i]);
        if run == len {
            out.push_str(with);
            rest = &tail[run..];
        } else {
            out.push_str(prefix);
            rest = tail;
        }
    }
    out.push_str(rest);
    out
}

/// Headers a transport adds below Go's executor capture.
const TRANSPORT_HEADERS: [&str; 4] = ["host", "user-agent", "accept-encoding", "content-length"];
/// Response headers compared with Go (CORS and framing belong to other layers).
const COMPARED: [&str; 9] = [
    "content-type",
    "location",
    "retry-after",
    "x-request-id",
    "openai-request-id",
    "cache-control",
    "upgrade",
    "set-cookie",
    "x-live-session",
];

#[tokio::test]
async fn realtime_http_matches_go() {
    let cases: Vec<Value> = serde_json::from_str(include_str!("fixtures/realtime_http_go.json")).unwrap();
    let mocks: BTreeMap<&str, Shared> = [("main", Shared::default()), ("empty", Shared::default())].into();
    let mut servers = BTreeMap::new();
    servers.insert("main", proxy(go_credentials(), mocks["main"].clone()).await);
    servers.insert("empty", proxy(vec![], mocks["empty"].clone()).await);
    let client = wreq::Client::new();
    // Masked alias -> the token this run issued.
    let mut secrets: BTreeMap<String, String> = BTreeMap::new();
    let mut failures = Vec::new();
    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let server = case["server"].as_str().unwrap();
        let mock = &mocks[server];
        {
            let mut mock = mock.lock().unwrap();
            mock.next = case.get("upstream").filter(|u| !u.is_null()).cloned();
            mock.seen.clear();
        }
        let request = &case["request"];
        let method: wreq::Method = request["method"].as_str().unwrap().parse().unwrap();
        let mut builder = client.request(
            method,
            format!("{}{}", servers[server], request["path"].as_str().unwrap()),
        );
        if let Some(headers) = request["headers"].as_object() {
            for (header, values) in headers {
                for value in values.as_array().unwrap() {
                    let mut value = value.as_str().unwrap().to_owned();
                    for (alias, real) in &secrets {
                        value = value.replace(alias, real);
                    }
                    builder = builder.header(header.as_str(), value);
                }
            }
        }
        let response = builder
            .body(request["body"].as_str().unwrap_or_default().to_owned())
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = response.text().await.unwrap();
        let aliases: BTreeMap<String, String> = secrets.iter().map(|(a, r)| (r.clone(), a.clone())).collect();
        if name == "secret_create" {
            // Go masks this response before it names the key; later cases use the alias.
            let token = serde_json::from_str::<Value>(&body).unwrap()["value"]
                .as_str()
                .unwrap()
                .to_owned();
            secrets.insert("ek_<secret-1>".into(), token);
        }
        let mut problems = Vec::new();
        if status != case["status"].as_u64().unwrap() as u16 {
            problems.push(format!("status {status}, Go {}", case["status"]));
        }
        let go_body = case["body"].as_str().unwrap();
        if mask(&body, &aliases) != go_body {
            problems.push(format!("body {:?}\n      Go {go_body:?}", mask(&body, &aliases)));
        }
        let go_headers = case["headers"].as_object().unwrap();
        let go_header = |name: &str| -> Vec<String> {
            go_headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
                .map(|(_, v)| {
                    v.as_array()
                        .unwrap()
                        .iter()
                        .map(|s| s.as_str().unwrap().to_owned())
                        .collect()
                })
                .unwrap_or_default()
        };
        for name in COMPARED {
            let ours: Vec<String> = headers
                .get_all(name)
                .iter()
                .map(|v| mask(v.to_str().unwrap(), &aliases))
                .collect();
            let mut want = go_header(name);
            // httptest's recorder skips net/http's sniffing; a real Go server labels a
            // non-empty body without Content-Type as text.
            if name == "content-type" && want.is_empty() && !go_body.is_empty() && status != 101 {
                want = vec!["text/plain; charset=utf-8".into()];
            }
            if ours != want {
                problems.push(format!("header {name} {ours:?}, Go {want:?}"));
            }
        }
        if headers.contains_key("x-ck-trace-id") != !go_header("x-cpa-trace-id").is_empty() {
            problems.push("X-Ck-Trace-Id presence differs".into());
        }
        let seen = std::mem::take(&mut mock.lock().unwrap().seen);
        let sent = case["sent"].as_array().unwrap();
        if seen.len() != sent.len() {
            problems.push(format!("{} upstream requests, Go {}", seen.len(), sent.len()));
        }
        for ((uri, got_headers, got_body), want) in seen.iter().zip(sent) {
            let want_url = url::Url::parse(want["url"].as_str().unwrap()).unwrap();
            let want_target = format!(
                "{}{}",
                want_url.path(),
                want_url.query().map(|q| format!("?{q}")).unwrap_or_default()
            );
            if uri.to_string() != want_target {
                problems.push(format!("upstream target {uri}, Go {want_target}"));
            }
            if mask(&String::from_utf8_lossy(got_body), &aliases) != want["body"].as_str().unwrap() {
                problems.push(format!(
                    "upstream body {:?}, Go {:?}",
                    String::from_utf8_lossy(got_body),
                    want["body"]
                ));
            }
            let want_headers = want["headers"].as_object().unwrap();
            for (name, values) in want_headers {
                let ours: Vec<&str> = got_headers
                    .get_all(name.as_str())
                    .iter()
                    .map(|v| v.to_str().unwrap())
                    .collect();
                let theirs: Vec<&str> = values.as_array().unwrap().iter().map(|v| v.as_str().unwrap()).collect();
                if ours != theirs {
                    problems.push(format!("upstream header {name} {ours:?}, Go {theirs:?}"));
                }
            }
            for name in got_headers.keys() {
                let known = want_headers.keys().any(|k| k.eq_ignore_ascii_case(name.as_str()));
                if !known && !TRANSPORT_HEADERS.contains(&name.as_str()) {
                    problems.push(format!("extra upstream header {name}"));
                }
            }
        }
        if !problems.is_empty() {
            failures.push(format!("{name}:\n    {}", problems.join("\n    ")));
        }
    }
    assert!(cases.len() > 50, "fixture has {} cases", cases.len());
    assert!(
        failures.is_empty(),
        "{} cases differ from Go:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

// ---------------------------------------------------------------------------------------
// WebSockets

/// One relayed frame as both harnesses record it.
#[derive(Debug, Clone, PartialEq)]
enum Frame {
    Text(String),
    Binary(Vec<u8>),
    /// A close frame's code; reasons differ only in the automatic close echo (gorilla
    /// echoes an empty reason, tungstenite the received one), so they are not compared.
    Close(u16),
    Gone,
}

fn frame(v: &Value) -> Frame {
    let data = v["data"].as_str().unwrap_or_default();
    match v["kind"].as_str().unwrap() {
        "text" => Frame::Text(data.into()),
        "binary" => Frame::Binary(data.as_bytes().to_vec()),
        "close" => Frame::Close(v["code"].as_u64().unwrap_or(1005) as u16),
        _ => Frame::Gone,
    }
}

#[derive(Default)]
struct WsSeen {
    target: String,
    headers: HeaderMap,
    received: Vec<Frame>,
}

#[derive(Default)]
struct WsMock {
    /// Requests to the calls endpoint: their Authorization and Chatgpt-Account-Id.
    created: Vec<(String, String)>,
    seen: Option<WsSeen>,
    done: Option<Arc<tokio::sync::Notify>>,
    /// Replies to the next text messages, one entry each, instead of the echo.
    script: std::collections::VecDeque<Vec<axum::extract::ws::Message>>,
}

type WsShared = Arc<Mutex<WsMock>>;

/// The Go harness's upstream: calls answer 201 with the `Thread-Id` as call ID; sockets
/// echo with `echo:`, close with 4001 on `upstream-close`, and reject marked targets.
/// Beyond Go's: `upstream-gone` drops the connection without a close, `flood` sends
/// megabyte messages until the connection fails, and a scripted reply replaces the echo.
async fn ws_upstream(
    State(mock): State<WsShared>,
    uri: Uri,
    headers: HeaderMap,
    ws: Result<axum::extract::ws::WebSocketUpgrade, axum::extract::ws::rejection::WebSocketUpgradeRejection>,
) -> axum::response::Response {
    use axum::extract::ws::Message;
    let header = |name: &str| {
        headers
            .get(name)
            .map(|v| v.to_str().unwrap().to_owned())
            .unwrap_or_default()
    };
    let Ok(ws) = ws else {
        mock.lock()
            .unwrap()
            .created
            .push((header("authorization"), header("chatgpt-account-id")));
        let location = format!("/v1/live/{}", header("thread-id"));
        return (
            StatusCode::CREATED,
            [("location", location.as_str()), ("content-type", "application/sdp")],
            "v=0",
        )
            .into_response();
    };
    let target = uri.to_string();
    for (marker, status, content_type, body) in [
        (
            "401",
            401,
            "application/json",
            r#"{"error":{"message":"token expired"}}"#,
        ),
        ("404", 404, "text/plain", "no such call"),
        ("429", 429, "application/json", r#"{"error":"slow"}"#),
    ] {
        if target.contains(&format!("-{marker}")) {
            return (
                StatusCode::from_u16(status).unwrap(),
                [
                    ("content-type", content_type),
                    ("x-request-id", &format!("up-{marker}")),
                    ("retry-after", "9"),
                    ("x-other", "dropped"),
                ],
                body.to_owned(),
            )
                .into_response();
        }
    }
    let offered: Vec<String> = header("sec-websocket-protocol")
        .split(',')
        .map(|p| p.trim().to_owned())
        .filter(|p| !p.is_empty())
        .collect();
    let mut ws = ws;
    if let Some(last) = offered.last().filter(|p| *p != "no-select") {
        ws.set_selected_protocol(last.parse().unwrap());
    }
    ws.on_upgrade(move |mut socket| async move {
        let mut record = WsSeen {
            target,
            headers,
            received: vec![],
        };
        loop {
            let message = match socket.recv().await {
                Some(Ok(message)) => message,
                _ => {
                    record.received.push(Frame::Gone);
                    break;
                }
            };
            let reply = match message {
                Message::Text(text) => {
                    record.received.push(Frame::Text(text.as_str().into()));
                    if text.as_str().contains("upstream-close") {
                        let close = axum::extract::ws::CloseFrame {
                            code: 4001,
                            reason: "bye".into(),
                        };
                        let _ = socket.send(Message::Close(Some(close))).await;
                        let next = socket.recv().await;
                        record.received.push(match next {
                            Some(Ok(Message::Close(f))) => Frame::Close(f.map_or(1005, |f| f.code)),
                            _ => Frame::Gone,
                        });
                        break;
                    }
                    if text.as_str().contains("upstream-gone") {
                        break;
                    }
                    if text.as_str().contains("flood") {
                        let chunk = Bytes::from(vec![0u8; 1 << 20]);
                        while socket.send(Message::Binary(chunk.clone())).await.is_ok() {}
                        break;
                    }
                    let script = mock.lock().unwrap().script.pop_front();
                    if let Some(replies) = script {
                        for reply in replies {
                            let _ = socket.send(reply).await;
                        }
                        continue;
                    }
                    Message::Text(format!("echo:{}", text.as_str()).into())
                }
                Message::Binary(data) => {
                    record.received.push(Frame::Binary(data.to_vec()));
                    let mut echo = b"echo:".to_vec();
                    echo.extend_from_slice(&data);
                    Message::Binary(echo.into())
                }
                Message::Close(f) => {
                    record.received.push(Frame::Close(f.map_or(1005, |f| f.code)));
                    break;
                }
                Message::Ping(_) | Message::Pong(_) => continue,
            };
            let _ = socket.send(reply).await;
        }
        let mut mock = mock.lock().unwrap();
        mock.seen = Some(record);
        if let Some(done) = mock.done.take() {
            done.notify_one();
        }
    })
}

#[tokio::test]
async fn realtime_websockets_match_go() {
    let cases: Vec<Value> = serde_json::from_str(include_str!("fixtures/codex_live_ws_go.json")).unwrap();
    let mock = WsShared::default();
    let upstream_url = serve(axum::Router::new().fallback(ws_upstream).with_state(mock.clone())).await;
    let executor = CodexExecutor::with_client(wreq::Client::new(), CodexOAuth::new(wreq::Client::new()))
        .with_live_endpoints(
            format!("{upstream_url}/backend-api/codex/realtime/calls?intent=quicksilver&architecture=avas"),
            format!("ws{}/v1", upstream_url.trim_start_matches("http")),
        );
    let credentials = vec![
        credential(
            "a-other-oauth",
            serde_json::json!({"type":"codex","access_token":"other-token","account_id":"other-account"}),
            &[],
        ),
        credential(
            "b-pinned-oauth",
            serde_json::json!({"type":"codex","access_token":"pinned-token","account_id":"pinned-account"}),
            &[("header:X-Operator", "op-value")],
        ),
    ];
    let rt = Arc::new(ck_server::testing::runtime(
        Config::parse("access: {api-keys: [owner-key, other-key]}").unwrap(),
        credentials,
        Executors {
            claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
            codex: executor,
            devices: Default::default(),
            openai: Default::default(),
            google: Default::default(),
        },
    ));
    let proxy = serve(router(rt)).await;
    let client = wreq::Client::new();
    let secret = client
        .post(format!("{proxy}/v1/realtime/client_secrets"))
        .header("authorization", "Bearer owner-key")
        .body(r#"{"session":{"type":"realtime","model":"gpt-realtime","instructions":"<be brief>","voice":"alloy"}}"#)
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap()["value"]
        .as_str()
        .unwrap()
        .to_owned();
    let authorization = |principal: &str| match principal {
        "owner" => "Bearer owner-key".to_owned(),
        "other" => "Bearer other-key".to_owned(),
        _ => format!("Bearer {secret}"),
    };
    // Go stores each call just before its case; here the proxy creates it through the
    // calls endpoint, and the sideband must reuse whichever credential created it.
    let setup: BTreeMap<&str, (&str, &str)> = [
        ("live_sideband_relays", ("call-live", "owner")),
        ("calls_sideband_client_close", ("call-calls", "owner")),
        ("query_sideband", ("call-query", "owner")),
        ("sideband_other_principal", ("call-scope", "owner")),
        ("sideband_secret_owner", ("call-secret", "secret")),
        ("sideband_secret_wrong_call", ("call-secret2", "owner")),
        ("sideband_upstream_401", ("call-401", "owner")),
        ("live_sideband_upstream_401", ("live-401", "owner")),
        ("sideband_upstream_404", ("call-404", "owner")),
        ("live_sideband_upstream_404", ("live-404", "owner")),
        ("sideband_upstream_429", ("call-429", "owner")),
        ("live_sideband_upstream_429", ("live-429", "owner")),
        ("sideband_upstream_selects_no_protocol", ("call-noselect", "owner")),
    ]
    .into();
    let mut creators: BTreeMap<String, (String, String)> = BTreeMap::new();
    let mut failures = Vec::new();
    for case in &cases {
        let name = case["name"].as_str().unwrap();
        if let Some((call_id, principal)) = setup.get(name) {
            let path = if *principal == "secret" {
                "/v1/realtime/calls"
            } else {
                "/v1/live"
            };
            let created = client
                .post(format!("{proxy}{path}"))
                .header("authorization", authorization(principal))
                .header("content-type", "application/json")
                .header("thread-id", *call_id)
                .body(r#"{"sdp":"v=0"}"#)
                .send()
                .await
                .unwrap();
            assert_eq!(created.status().as_u16(), 201, "{name}: creating {call_id}");
            let creator = mock.lock().unwrap().created.pop().unwrap();
            creators.insert((*call_id).to_owned(), creator);
        }
        let path = case["path"].as_str().unwrap();
        let principal = case["principal"].as_str().unwrap();
        let done = Arc::new(tokio::sync::Notify::new());
        {
            let mut mock = mock.lock().unwrap();
            mock.seen = None;
            mock.done = Some(done.clone());
        }
        let ws_url = format!("ws{}{path}", proxy.trim_start_matches("http"));
        let mut builder = client
            .websocket(&ws_url)
            .header("authorization", authorization(principal));
        if let Some(headers) = case["headers"].as_object() {
            for (header, values) in headers {
                let value = values[0].as_str().unwrap();
                // A raw offer, so wreq's strict negotiation cannot hide what the proxy did.
                builder = builder.header(header.as_str(), value);
            }
        }
        let mut problems = Vec::new();
        let mut response = builder.send().await.unwrap();
        let status = response.status().as_u16();
        if status != case["status"].as_u64().unwrap() as u16 {
            problems.push(format!("status {status}, Go {}", case["status"]));
        }
        let headers = response.headers().clone();
        for (name, values) in case["response_headers"].as_object().unwrap() {
            if name == "Upgrade" {
                continue;
            }
            let ours: Vec<&str> = headers
                .get_all(name.as_str())
                .iter()
                .map(|v| v.to_str().unwrap())
                .collect();
            let theirs: Vec<&str> = values.as_array().unwrap().iter().map(|v| v.as_str().unwrap()).collect();
            if ours != theirs {
                problems.push(format!("response header {name} {ours:?}, Go {theirs:?}"));
            }
        }
        if headers.contains_key("x-other") {
            problems.push("upstream X-Other leaked".into());
        }
        let mut received = vec![];
        if status == 101 {
            let protocol = response
                .headers_mut()
                .remove("sec-websocket-protocol")
                .map(|p| p.to_str().unwrap().to_owned());
            let mut socket = response.into_websocket().await.unwrap();
            if protocol.as_deref() != case["protocol"].as_str() {
                problems.push(format!("protocol {protocol:?}, Go {:?}", case["protocol"]));
            }
            use wreq::ws::message::{CloseFrame, Message};
            for step in case["send"].as_array().unwrap() {
                let data = step["data"].as_str().unwrap_or_default().to_owned();
                let message = match step["kind"].as_str().unwrap() {
                    "read" => None,
                    "text" => Some(Message::text(data)),
                    "binary" => Some(Message::binary(data.into_bytes())),
                    "ping" => {
                        socket.send(Message::ping(data.into_bytes())).await.unwrap();
                        continue;
                    }
                    _ => Some(Message::Close(Some(CloseFrame {
                        code: (step["code"].as_u64().unwrap() as u16).into(),
                        reason: data.into(),
                    }))),
                };
                if let Some(message) = message {
                    let _ = socket.send(message).await;
                }
                let got = loop {
                    match tokio::time::timeout(std::time::Duration::from_secs(2), socket.recv()).await {
                        Ok(Some(Ok(Message::Text(t)))) => break Frame::Text(t.as_str().into()),
                        Ok(Some(Ok(Message::Binary(b)))) => break Frame::Binary(b.to_vec()),
                        Ok(Some(Ok(Message::Close(f)))) => break Frame::Close(f.map_or(1005, |f| u16::from(f.code))),
                        Ok(Some(Ok(_))) => continue,
                        _ => break Frame::Gone,
                    }
                };
                let stop = matches!(got, Frame::Close(_) | Frame::Gone);
                received.push(got);
                if stop {
                    break;
                }
            }
            drop(socket);
            let _ = tokio::time::timeout(std::time::Duration::from_secs(2), done.notified()).await;
        } else {
            let inner = std::mem::replace(
                &mut *response,
                wreq::Response::from(axum::http::Response::new(Vec::<u8>::new())),
            );
            let body = inner.text().await.unwrap();
            if body != case["body"].as_str().unwrap_or_default() {
                problems.push(format!("body {body:?}, Go {:?}", case["body"]));
            }
        }
        let want_received: Vec<Frame> = case["received"]
            .as_array()
            .map(|r| r.iter().map(frame).collect())
            .unwrap_or_default();
        if received != want_received {
            problems.push(format!("client received {received:?}, Go {want_received:?}"));
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let seen = mock.lock().unwrap().seen.take();
        match (seen, case.get("upstream").filter(|u| !u.is_null())) {
            (None, None) => {}
            (Some(seen), Some(want)) => {
                if seen.target != want["target"].as_str().unwrap() {
                    problems.push(format!("upstream target {}, Go {}", seen.target, want["target"]));
                }
                let call_id = case["path"].as_str().unwrap().rsplit(['/', '=']).next().unwrap();
                for (name, values) in want["headers"].as_object().unwrap() {
                    let mut theirs: Vec<String> = values
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|v| v.as_str().unwrap().to_owned())
                        .collect();
                    // Sidebands are pinned to the call's creator; Go stored its calls on
                    // "pinned". Direct sessions take any OAuth credential.
                    let creator = creators.get(call_id);
                    match (name.as_str(), creator) {
                        ("Authorization", Some((token, _))) => theirs = vec![token.clone()],
                        ("Chatgpt-Account-Id", Some((_, account))) => theirs = vec![account.clone()],
                        _ => {}
                    }
                    let ours: Vec<String> = seen
                        .headers
                        .get_all(name.as_str())
                        .iter()
                        .map(|v| v.to_str().unwrap().to_owned())
                        .collect();
                    let direct = creator.is_none() && matches!(name.as_str(), "Authorization" | "Chatgpt-Account-Id");
                    let ok = if direct {
                        ours.len() == 1 && (ours[0].contains("other") || ours[0].contains("pinned"))
                    } else {
                        ours == theirs
                    };
                    if !ok {
                        problems.push(format!("upstream header {name} {ours:?}, Go {theirs:?}"));
                    }
                }
                for absent in ["x-not-forwarded", "openai-alpha"] {
                    if seen.headers.contains_key(absent)
                        && !want["headers"]
                            .as_object()
                            .unwrap()
                            .keys()
                            .any(|k| k.eq_ignore_ascii_case(absent))
                    {
                        problems.push(format!("upstream got {absent}"));
                    }
                }
                let theirs: Vec<Frame> = want["received"].as_array().unwrap().iter().map(frame).collect();
                if seen.received != theirs {
                    problems.push(format!("upstream received {:?}, Go {theirs:?}", seen.received));
                }
            }
            (seen, want) => problems.push(format!("upstream socket {}, Go {}", seen.is_some(), want.is_some())),
        }
        if !problems.is_empty() {
            failures.push(format!("{name}:\n    {}", problems.join("\n    ")));
        }
    }
    // Pinning must hold even when rotation would pick the other credential.
    let tokens: std::collections::BTreeSet<&str> = creators.values().map(|(t, _)| t.as_str()).collect();
    assert_eq!(tokens.len(), 2, "calls were created with both credentials");
    assert!(
        failures.is_empty(),
        "{} cases differ from Go:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// Call state across requests (sideband.go `sessionStore`, `HandleHangup`): one sideband
/// at a time, a hangup ends a running sideband and forgets the call, and a call whose
/// credential disappeared cannot borrow another one.
#[tokio::test]
async fn sideband_claims_and_hangup_teardown() {
    use wreq::ws::message::Message;
    let mock = WsShared::default();
    let upstream_url = serve(axum::Router::new().fallback(ws_upstream).with_state(mock.clone())).await;
    let executor = CodexExecutor::with_client(wreq::Client::new(), CodexOAuth::new(wreq::Client::new()))
        .with_live_endpoints(
            format!("{upstream_url}/backend-api/codex/realtime/calls"),
            format!("ws{}/v1", upstream_url.trim_start_matches("http")),
        );
    let rt = Arc::new(ck_server::testing::runtime(
        Config::parse("access: {api-keys: [owner-key]}").unwrap(),
        vec![credential(
            "only-oauth",
            serde_json::json!({"type":"codex","access_token":"only-token"}),
            &[],
        )],
        Executors {
            claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
            codex: executor,
            devices: Default::default(),
            openai: Default::default(),
            google: Default::default(),
        },
    ));
    let proxy = serve(router(rt.clone())).await;
    let client = wreq::Client::new();
    let create = |call_id: &'static str| {
        client
            .post(format!("{proxy}/v1/live"))
            .header("authorization", "Bearer owner-key")
            .header("thread-id", call_id)
            .body(r#"{"sdp":"v=0"}"#)
            .send()
    };
    let join = |call_id: &'static str| {
        client
            .websocket(format!(
                "ws{}/v1/realtime/calls/{call_id}",
                proxy.trim_start_matches("http")
            ))
            .header("authorization", "Bearer owner-key")
            .send()
    };
    assert_eq!(create("call-a").await.unwrap().status().as_u16(), 201);
    let first = join("call-a").await.unwrap();
    assert_eq!(first.status().as_u16(), 101);
    let mut first = first.into_websocket().await.unwrap();
    let clear = r#"{"type":"input_audio_buffer.clear"}"#;
    first.send(Message::text(clear)).await.unwrap();
    let echoed = tokio::time::timeout(std::time::Duration::from_secs(2), first.recv()).await;
    assert!(matches!(echoed, Ok(Some(Ok(Message::Text(t)))) if t.as_str() == format!("echo:{clear}")));

    let second = join("call-a").await.unwrap();
    assert_eq!(second.status().as_u16(), 409, "one sideband per call");

    let hangup = client
        .post(format!("{proxy}/v1/realtime/calls/call-a/hangup"))
        .header("authorization", "Bearer owner-key")
        .send()
        .await
        .unwrap();
    assert_eq!(hangup.status().as_u16(), 201, "upstream status passes through");
    let ended = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            match first.recv().await {
                Some(Ok(Message::Text(_) | Message::Binary(_) | Message::Ping(_) | Message::Pong(_))) => continue,
                other => return other,
            }
        }
    })
    .await;
    assert!(ended.is_ok(), "hangup ends the running sideband");
    let again = join("call-a").await.unwrap();
    assert_eq!(again.status().as_u16(), 404, "the call is forgotten");

    // The call stays pinned: once its credential is gone, no other one stands in.
    assert_eq!(create("call-b").await.unwrap().status().as_u16(), 201);
    rt.store().reconcile(vec![]);
    let orphan = join("call-b").await.unwrap();
    assert_eq!(orphan.status().as_u16(), 503);
}

/// gorilla's downstream handshake checks (`Upgrader.Upgrade`), from
/// `codex_live_ws_raw_go.json`: raw requests no WebSocket client library would send. A
/// rejected upgrade releases the call; an accepted one consumes it when it ends.
#[tokio::test]
async fn raw_handshakes_match_gorilla() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let cases: Vec<Value> = serde_json::from_str(include_str!("fixtures/codex_live_ws_raw_go.json")).unwrap();
    let mock = WsShared::default();
    let upstream_url = serve(axum::Router::new().fallback(ws_upstream).with_state(mock.clone())).await;
    let executor = CodexExecutor::with_client(wreq::Client::new(), CodexOAuth::new(wreq::Client::new()))
        .with_live_endpoints(
            format!("{upstream_url}/backend-api/codex/realtime/calls"),
            format!("ws{}/v1", upstream_url.trim_start_matches("http")),
        );
    let rt = Arc::new(ck_server::testing::runtime(
        Config::parse("access: {api-keys: [owner-key]}").unwrap(),
        vec![credential(
            "only-oauth",
            serde_json::json!({"type":"codex","access_token":"only-token"}),
            &[],
        )],
        Executors {
            claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
            codex: executor,
            devices: Default::default(),
            openai: Default::default(),
            google: Default::default(),
        },
    ));
    let proxy = serve(router(rt)).await;
    let client = wreq::Client::new();
    let mut failures = Vec::new();
    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let call_id = case["call_id"].as_str().unwrap();
        let created = client
            .post(format!("{proxy}/v1/live"))
            .header("authorization", "Bearer owner-key")
            .header("thread-id", call_id)
            .body(r#"{"sdp":"v=0"}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(created.status().as_u16(), 201);
        let mut stream = tokio::net::TcpStream::connect(proxy.trim_start_matches("http://"))
            .await
            .unwrap();
        let headers: Vec<&str> = case["headers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|h| h.as_str().unwrap())
            .collect();
        let request = format!(
            "GET /v1/live/{call_id} HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer owner-key\r\n{}\r\n\r\n",
            headers.join("\r\n")
        );
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut head = vec![0u8; 1024];
        let n = tokio::time::timeout(std::time::Duration::from_secs(2), stream.read(&mut head))
            .await
            .unwrap()
            .unwrap();
        let status: u16 = String::from_utf8_lossy(&head[..n])
            .split(' ')
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        drop(stream);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        // A kept call can still be joined; a consumed one is gone.
        let rejoin = client
            .websocket(format!("ws{}/v1/live/{call_id}", proxy.trim_start_matches("http")))
            .header("authorization", "Bearer owner-key")
            .send()
            .await
            .unwrap();
        let kept = rejoin.status().as_u16() == 101;
        drop(rejoin);
        if status != case["status"].as_u64().unwrap() as u16 || kept != case["call_kept"].as_bool().unwrap() {
            failures.push(format!(
                "{name}: status {status} kept {kept}, Go {} kept {}",
                case["status"], case["call_kept"]
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// How a sideband ends when the downstream connection fails mid-session
/// (`websocketCloseDetails`), from `codex_live_ws_end_go.json`: the close frame the
/// upstream receives after a protocol violation, an abrupt close or a reset.
#[tokio::test]
async fn relay_end_close_codes_match_gorilla() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let cases: Vec<Value> = serde_json::from_str(include_str!("fixtures/codex_live_ws_end_go.json")).unwrap();
    let mock = WsShared::default();
    let upstream_url = serve(axum::Router::new().fallback(ws_upstream).with_state(mock.clone())).await;
    let executor = CodexExecutor::with_client(wreq::Client::new(), CodexOAuth::new(wreq::Client::new()))
        .with_live_endpoints(
            format!("{upstream_url}/backend-api/codex/realtime/calls"),
            format!("ws{}/v1", upstream_url.trim_start_matches("http")),
        );
    let rt = Arc::new(ck_server::testing::runtime(
        Config::parse("access: {api-keys: [owner-key]}").unwrap(),
        vec![credential(
            "only-oauth",
            serde_json::json!({"type":"codex","access_token":"only-token"}),
            &[],
        )],
        Executors {
            claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
            codex: executor,
            devices: Default::default(),
            openai: Default::default(),
            google: Default::default(),
        },
    ));
    let proxy = serve(router(rt)).await;
    let client = wreq::Client::new();
    let mut failures = Vec::new();
    for case in &cases {
        let action = case["name"].as_str().unwrap();
        let call_id = format!("call-end-{action}");
        let created = client
            .post(format!("{proxy}/v1/live"))
            .header("authorization", "Bearer owner-key")
            .header("thread-id", &call_id)
            .body(r#"{"sdp":"v=0"}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(created.status().as_u16(), 201);
        mock.lock().unwrap().seen = None;
        let stream = tokio::net::TcpStream::connect(proxy.trim_start_matches("http://"))
            .await
            .unwrap();
        let mut stream = stream;
        let request = format!(
            "GET /v1/live/{call_id} HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer owner-key\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
        );
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") {
            let mut byte = [0u8; 1];
            stream.read_exact(&mut byte).await.unwrap();
            head.push(byte[0]);
        }
        assert!(
            head.starts_with(b"HTTP/1.1 101"),
            "{action}: {}",
            String::from_utf8_lossy(&head)
        );
        match action {
            "rsv1" => {
                let mask = [1u8, 2, 3, 4];
                let mut frame = vec![0x80 | 0x40 | 0x1, 0x80 | 1];
                frame.extend_from_slice(&mask);
                frame.push(b'x' ^ mask[0]);
                stream.write_all(&frame).await.unwrap();
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                drop(stream);
            }
            "abrupt" => drop(stream),
            _ => {
                #[allow(deprecated)]
                stream.set_linger(Some(std::time::Duration::ZERO)).unwrap();
                drop(stream);
            }
        }
        let want: Vec<Frame> = case["upstream_received"]
            .as_array()
            .unwrap()
            .iter()
            .map(frame)
            .collect();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        let got = loop {
            let seen = mock
                .lock()
                .unwrap()
                .seen
                .as_ref()
                .filter(|s| s.target.contains(&call_id))
                .map(|s| s.received.clone());
            if let Some(got) = seen {
                break got;
            }
            if tokio::time::Instant::now() > deadline {
                break vec![];
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        };
        if got != want {
            failures.push(format!("{action}: upstream received {got:?}, Go {want:?}"));
        }
    }
    assert_eq!(cases.len(), 3);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Go `PrepareHttpRequest` refuses a Sign in with ChatGPT credential without its token
/// with status 401 before dialling; the direct Realtime route keeps that status
/// (`HTTPStatusFromErrorOr(errDial, 502)`) and nothing reaches the upstream.
#[tokio::test]
async fn direct_realtime_refuses_a_siwc_credential_without_its_token_with_401() {
    let mock = Shared::default();
    let siwc = credential(
        "siwc",
        serde_json::json!({"type":"codex","use_siwc":true,"access_token":"outer-token","siwc":{"refresh_token":"rt"}}),
        &[],
    );
    let proxy = proxy(vec![siwc], mock.clone()).await;
    let mut response = wreq::Client::new()
        .websocket(format!(
            "ws{}/v1/realtime?model=gpt-realtime",
            proxy.trim_start_matches("http")
        ))
        .header("authorization", "Bearer good-key")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 401);
    let inner = std::mem::replace(
        &mut *response,
        wreq::Response::from(axum::http::Response::new(Vec::<u8>::new())),
    );
    assert_eq!(
        inner.json::<Value>().await.unwrap(),
        serde_json::json!({"error": {"message": "Codex Realtime WebSocket upstream unavailable",
            "type": "authentication_error", "param": null, "code": "realtime_upstream_unauthorized"}})
    );
    assert!(mock.lock().unwrap().seen.is_empty());
}

/// A router over `ws_upstream` with one Codex credential and the access key `owner-key`.
async fn live_proxy(config: &str) -> (String, Arc<ck_server::Runtime>, WsShared) {
    let mock = WsShared::default();
    let upstream_url = serve(axum::Router::new().fallback(ws_upstream).with_state(mock.clone())).await;
    let executor = CodexExecutor::with_client(wreq::Client::new(), CodexOAuth::new(wreq::Client::new()))
        .with_live_endpoints(
            format!("{upstream_url}/backend-api/codex/realtime/calls"),
            format!("ws{}/v1", upstream_url.trim_start_matches("http")),
        );
    let rt = Arc::new(ck_server::testing::runtime(
        Config::parse(&format!("access: {{api-keys: [owner-key]}}\n{config}")).unwrap(),
        vec![credential(
            "only-oauth",
            serde_json::json!({"type":"codex","access_token":"only-token"}),
            &[],
        )],
        Executors {
            claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
            codex: executor,
            devices: Default::default(),
            openai: Default::default(),
            google: Default::default(),
        },
    ));
    let proxy = serve(router(rt.clone())).await;
    (proxy, rt, mock)
}

async fn open_realtime(proxy: &str) -> wreq::ws::WebSocket {
    let response = wreq::Client::new()
        .websocket(format!(
            "ws{}/v1/realtime?model=gpt-realtime",
            proxy.trim_start_matches("http")
        ))
        .header("authorization", "Bearer owner-key")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 101);
    response.into_websocket().await.unwrap()
}

async fn next_frame(socket: &mut wreq::ws::WebSocket) -> Frame {
    use wreq::ws::message::Message;
    loop {
        match tokio::time::timeout(std::time::Duration::from_secs(5), socket.recv()).await {
            Ok(Some(Ok(Message::Text(t)))) => return Frame::Text(t.as_str().into()),
            Ok(Some(Ok(Message::Binary(b)))) => return Frame::Binary(b.to_vec()),
            Ok(Some(Ok(Message::Close(f)))) => return Frame::Close(f.map_or(1005, |f| u16::from(f.code))),
            Ok(Some(Ok(_))) => continue,
            _ => return Frame::Gone,
        }
    }
}

/// What the mock upstream received once its socket ended.
async fn upstream_seen(mock: &WsShared) -> Vec<Frame> {
    for _ in 0..500 {
        if let Some(seen) = mock.lock().unwrap().seen.take() {
            return seen.received;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("the upstream socket never ended");
}

/// Media messages, control messages carrying media fields and plain control messages, with
/// irregular whitespace and member order, reach the other side with the same opcode, bytes
/// and order; a client event outside the session's configuration reaches nobody and gets the
/// protocol's `error` event.
#[tokio::test]
async fn live_messages_are_forwarded_byte_for_byte() {
    use axum::extract::ws::Message as Up;
    use wreq::ws::message::Message;
    let (proxy, _rt, mock) = live_proxy("").await;
    let refused = r#"{"type":"error","error":{"type":"invalid_request_error","code":"unknown_event_type","message":"This session does not accept this event type. Send a documented client event.","param":"type"}}"#;
    // (client text message, what the upstream replies, what the client then receives)
    let rows: [(&str, Option<Up>, Option<Frame>); 6] = [
        (
            "{ \"type\" : \"input_audio_buffer.append\" ,\n \"audio\":\"AAAA//8=\" }",
            Some(Up::Text(
                r#"{"type":"response.output_audio.delta","response_id":"resp_1","item_id":"i1","output_index":0,"content_index":0,"delta":"AAAA"}"#.into(),
            )),
            None,
        ),
        (
            r#"{"audio":"AQID","event_id":"e2","type":"input_audio_buffer.append"}"#,
            Some(Up::Binary(Bytes::from_static(&[0, 1, 2, 255]))),
            None,
        ),
        (
            r#"{"type":"conversation.item.create","item":{"type":"message","role":"user","content":[{"type":"input_audio","audio":"UklGRg=="},{"type":"input_image","image_url":"data:image/png;base64,iVBORw0KGgo="}]}}"#,
            Some(Up::Text("{\"type\":\"response.audio.delta\",  \"delta\":\"AQI=\"}".into())),
            None,
        ),
        (
            r#"{"type":"response.create","response":{"input":[{"type":"message","role":"user","content":[{"type":"input_audio","audio":"AAA="}]}]}}"#,
            Some(Up::Text(r#"{"delta":"AA==","type":"session.output_audio.delta"}"#.into())),
            None,
        ),
        (r#"{"type":"no.such.event","audio":"AAA="}"#, None, Some(Frame::Text(refused.into()))),
        (r#"{"type":"input_audio_buffer.commit"}"#, Some(Up::Text(r#"{"type":"input_audio_buffer.committed"}"#.into())), None),
    ];
    for (_, reply, _) in &rows {
        if let Some(reply) = reply {
            mock.lock().unwrap().script.push_back(vec![reply.clone()]);
        }
    }
    let mut socket = open_realtime(&proxy).await;
    let mut want_upstream = Vec::new();
    for (sent, reply, client) in rows {
        socket.send(Message::text(sent)).await.unwrap();
        let want = match (reply, client) {
            (Some(Up::Text(text)), None) => Frame::Text(text.as_str().into()),
            (Some(Up::Binary(data)), None) => Frame::Binary(data.to_vec()),
            (None, Some(frame)) => frame,
            _ => unreachable!(),
        };
        if !matches!(want, Frame::Text(ref t) if t.as_str() == refused) {
            want_upstream.push(Frame::Text(sent.into()));
        }
        assert_eq!(next_frame(&mut socket).await, want, "after {sent}");
    }
    socket
        .send(Message::Close(Some(wreq::ws::message::CloseFrame {
            code: 1000.into(),
            reason: "".into(),
        })))
        .await
        .unwrap();
    want_upstream.push(Frame::Close(1000));
    assert_eq!(upstream_seen(&mock).await, want_upstream);
}

/// Each response's `response.done` usage and each transcription's token usage is its own
/// usage record beside the session's own; a response without usage records nothing.
#[tokio::test]
async fn live_usage_comes_from_response_done() {
    use axum::extract::ws::Message as Up;
    use wreq::ws::message::Message;
    let (proxy, rt, mock) = live_proxy("").await;
    rt.usage_queue().configure(
        true,
        &Config::parse("observability: {usage: {usage-statistics-enabled: true}}\n").unwrap(),
    );
    let replies = [
        r#"{"type":"response.created","response":{"id":"resp_1","status":"in_progress"}}"#,
        r#"{"type":"response.done","response":{"id":"resp_1","status":"completed","usage":{"total_tokens":15,"input_tokens":10,"output_tokens":5,"input_token_details":{"audio_tokens":8,"text_tokens":2},"output_token_details":{"audio_tokens":4,"text_tokens":1}}}}"#,
        r#"{"type":"conversation.item.input_audio_transcription.completed","item_id":"item_1","content_index":0,"transcript":"hi","usage":{"type":"tokens","input_tokens":7,"output_tokens":2,"total_tokens":9}}"#,
        r#"{"type":"response.created","response":{"id":"resp_2","status":"in_progress"}}"#,
        r#"{"type":"response.done","response":{"id":"resp_2","status":"cancelled"}}"#,
    ];
    mock.lock()
        .unwrap()
        .script
        .push_back(replies.iter().map(|r| Up::Text((*r).into())).collect());
    let mut socket = open_realtime(&proxy).await;
    socket
        .send(Message::text(r#"{"type":"response.create"}"#))
        .await
        .unwrap();
    for reply in replies {
        assert_eq!(next_frame(&mut socket).await, Frame::Text(reply.into()));
    }
    socket.send(Message::Close(None)).await.unwrap();
    upstream_seen(&mock).await;
    let mut records = Vec::new();
    for _ in 0..500 {
        records.extend(rt.usage_queue().pop_oldest(10));
        if records.len() >= 3 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let mut got: Vec<(String, Option<u64>, Option<u64>, String)> = records
        .iter()
        .map(|record| {
            let record: Value = serde_json::from_slice(record).unwrap();
            // The session's own record reports no tokens.
            (
                record["usage_scope"].as_str().unwrap_or_default().to_owned(),
                record["tokens"]["input_tokens"].as_u64(),
                record["tokens"]["output_tokens"].as_u64(),
                record["endpoint"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    got.sort();
    let endpoint = "GET /v1/realtime".to_owned();
    assert_eq!(
        got,
        [
            (String::new(), None, None, endpoint.clone()),
            ("response:resp_1".into(), Some(10), Some(5), endpoint.clone()),
            ("transcription:item_1".into(), Some(7), Some(2), endpoint),
        ]
    );
}

#[derive(Clone, Default)]
struct LogCapture(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for LogCapture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl LogCapture {
    /// The `live_session_end` lines so far, each as its `key=value` fields.
    fn ends(&self) -> Vec<BTreeMap<String, String>> {
        let text = String::from_utf8(self.0.lock().unwrap().clone()).unwrap();
        text.lines()
            .filter(|line| line.contains("live_session_end"))
            .map(|line| {
                line.split_whitespace()
                    .filter_map(|pair| pair.split_once('='))
                    .map(|(k, v)| (k.to_owned(), v.trim_matches('"').to_owned()))
                    .collect()
            })
            .collect()
    }

    fn raw(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

/// How a session ends in [`live_session_end_is_logged_once_with_cause`].
#[derive(Clone, Copy, Debug)]
enum Ending {
    ClientClose,
    ClientGone,
    MissingPong,
    StalledWrite,
    UpstreamClose,
    UpstreamGone,
    Hangup,
    SocketLimit,
}

/// Every way a session ends writes one `live_session_end` with its cause, initiator and the
/// fields that explain it, and no key, token or message content; a client end closes the
/// upstream session. Timeouts run on shortened settings.
#[tokio::test]
async fn live_session_end_is_logged_once_with_cause() {
    use wreq::ws::message::{CloseFrame, Message};
    let capture = LogCapture::default();
    let writer = capture.clone();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .with_writer(move || writer.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let (proxy, _rt, mock) = live_proxy(
        "realtime: {ping-interval-seconds: 1, read-timeout-seconds: 2, write-timeout-seconds: 1, max-sockets-per-key: 1}",
    )
    .await;
    let event = |id: &str| Message::text(format!(r#"{{"type":"input_audio_buffer.clear","event_id":"{id}"}}"#));
    // (how it ends, the causes logged, the extra field the cause needs, the upstream's last frame)
    let rows = [
        (
            Ending::ClientClose,
            vec!["client_close"],
            Some("close_code"),
            Some(Frame::Close(4002)),
        ),
        (Ending::ClientGone, vec!["client_gone"], None, Some(Frame::Close(1001))),
        (
            Ending::MissingPong,
            vec!["client_read_timeout"],
            Some("idle_ms"),
            Some(Frame::Close(1001)),
        ),
        (
            Ending::StalledWrite,
            vec!["client_write_timeout"],
            Some("write_blocked_ms"),
            None,
        ),
        (
            Ending::UpstreamClose,
            vec!["upstream_close"],
            Some("close_code"),
            Some(Frame::Close(4001)),
        ),
        (Ending::UpstreamGone, vec!["upstream_gone"], None, None),
        (
            Ending::Hangup,
            vec!["hangup"],
            Some("call_id"),
            Some(Frame::Close(1001)),
        ),
        (
            Ending::SocketLimit,
            vec!["socket_limit", "client_close"],
            None,
            Some(Frame::Close(1005)),
        ),
    ];
    let client = wreq::Client::new();
    for (ending, causes, field, upstream_last) in rows {
        capture.0.lock().unwrap().clear();
        // Another test's thread may have cached the end log's callsite as uninteresting.
        tracing::callsite::rebuild_interest_cache();
        let mut socket = match ending {
            Ending::Hangup => {
                let created = client
                    .post(format!("{proxy}/v1/live"))
                    .header("authorization", "Bearer owner-key")
                    .header("thread-id", "call-hangup")
                    .body(r#"{"sdp":"v=0"}"#)
                    .send()
                    .await
                    .unwrap();
                assert_eq!(created.status().as_u16(), 201);
                let joined = client
                    .websocket(format!(
                        "ws{}/v1/realtime/calls/call-hangup",
                        proxy.trim_start_matches("http")
                    ))
                    .header("authorization", "Bearer owner-key")
                    .send()
                    .await
                    .unwrap();
                assert_eq!(joined.status().as_u16(), 101);
                joined.into_websocket().await.unwrap()
            }
            _ => open_realtime(&proxy).await,
        };
        match ending {
            Ending::ClientClose => socket
                .send(Message::Close(Some(CloseFrame {
                    code: 4002.into(),
                    // A close reason is client text: the end log keeps its code only.
                    reason: "sk-close-secret".into(),
                })))
                .await
                .unwrap(),
            Ending::ClientGone => drop(socket),
            // The socket is not read, so no pong answers the pings.
            Ending::MissingPong => {
                tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
                drop(socket);
            }
            Ending::StalledWrite => {
                socket.send(event("flood")).await.unwrap();
                tokio::time::sleep(std::time::Duration::from_millis(1800)).await;
                drop(socket);
            }
            Ending::UpstreamClose => {
                socket.send(event("upstream-close")).await.unwrap();
                assert_eq!(next_frame(&mut socket).await, Frame::Close(4001));
            }
            Ending::UpstreamGone => {
                socket.send(event("upstream-gone")).await.unwrap();
                assert_eq!(next_frame(&mut socket).await, Frame::Close(1000));
            }
            Ending::Hangup => {
                let hangup = client
                    .post(format!("{proxy}/v1/realtime/calls/call-hangup/hangup"))
                    .header("authorization", "Bearer owner-key")
                    .send()
                    .await
                    .unwrap();
                assert_eq!(hangup.status().as_u16(), 201);
                assert_eq!(next_frame(&mut socket).await, Frame::Close(1000));
            }
            Ending::SocketLimit => {
                let second = client
                    .websocket(format!(
                        "ws{}/v1/realtime?model=gpt-realtime",
                        proxy.trim_start_matches("http")
                    ))
                    .header("authorization", "Bearer owner-key")
                    .send()
                    .await
                    .unwrap();
                assert_eq!(second.status().as_u16(), 429, "a second socket of the key is refused");
                socket.send(Message::Close(None)).await.unwrap();
            }
        }
        let mut ends = capture.ends();
        for _ in 0..500 {
            if ends.len() >= causes.len() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            ends = capture.ends();
        }
        // The upstream ended too; a stalled flood or a dropped upstream records no close.
        let upstream = upstream_seen(&mock).await;
        if let Some(last) = upstream_last {
            assert_eq!(upstream.last(), Some(&last), "{ending:?}: {upstream:?}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let ends = capture.ends();
        let logged: Vec<&str> = ends.iter().map(|end| end["cause"].as_str()).collect();
        assert_eq!(logged, causes, "{ending:?}: {}", capture.raw());
        let end = ends.last().unwrap();
        for required in [
            "initiator",
            "auth_id",
            "ping_interval_s",
            "read_timeout_s",
            "write_timeout_s",
            "duration_ms",
        ]
        .into_iter()
        .chain(field)
        {
            assert!(end.contains_key(required), "{ending:?}: {required} missing in {end:?}");
        }
        let raw = capture.raw();
        let lines: Vec<&str> = raw.lines().filter(|line| line.contains("live_session_end")).collect();
        for secret in [
            "owner-key",
            "only-token",
            "input_audio_buffer",
            "flood",
            "sk-close-secret",
        ] {
            assert!(
                lines.iter().all(|line| !line.contains(secret)),
                "{ending:?}: the end log carries {secret}: {lines:?}"
            );
        }
    }
}
