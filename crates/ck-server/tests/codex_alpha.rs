//! Codex Alpha Search through the real router, against a loopback upstream. Fake tokens only.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use ck_core::config::Config;
use ck_core::credential::Credential;
use ck_exec::Executors;
use ck_exec::codex::CodexExecutor;
use ck_exec::codex_oauth::CodexOAuth;
use ck_server::router;

type Seen = Arc<Mutex<Vec<(String, HeaderMap, Bytes)>>>;
/// The upstream's answers, in the order calls arrive.
type Script = Arc<Mutex<Vec<(u16, &'static str)>>>;

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    url
}

fn credential(id: &str, meta: serde_json::Value, attrs: &[(&str, &str)]) -> Credential {
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

/// The search result the client gets back as the upstream sent it, native members included.
const RESULT: &str =
    r#"{"results":[{"title":"Rust","uri":"https://e.x/rust","text":"t","rank":1}],"next":null,"id":"s-1"}"#;
const SEARCH: &str =
    r#"{"id":"s-1","model":"gpt-5.5","query":"rust <ws>","prompt_cache_key":"k","prompt_cache_retention":"24h"}"#;

struct Case {
    name: &'static str,
    /// Credentials besides the API key without `alpha-search`, which is never a candidate.
    oauth: &'static [&'static str],
    script: &'static [(u16, &'static str)],
    body: &'static str,
    status: u16,
    /// The client's body as JSON, or a substring of an error body.
    expected: &'static str,
    /// The access tokens the upstream saw, in order.
    tokens: &'static [&'static str],
}

#[tokio::test]
async fn alpha_search_serves_eligible_credentials_and_fails_over_before_answering() {
    let cases = [
        Case {
            name: "a result reaches the client as the upstream sent it",
            oauth: &["b"],
            script: &[(200, RESULT)],
            body: SEARCH,
            status: 200,
            expected: RESULT,
            tokens: &["at-b"],
        },
        Case {
            name: "an upstream refusal fails over to the next credential",
            oauth: &["b", "c"],
            script: &[(500, r#"{"error":{"message":"down"}}"#), (200, RESULT)],
            body: SEARCH,
            status: 200,
            expected: RESULT,
            tokens: &["at-b", "at-c"],
        },
        Case {
            // Go selects with the route model: no credential registers this one.
            name: "an unknown model finds no credential",
            oauth: &["b"],
            script: &[],
            body: r#"{"id":"s-1","model":"gpt-5.4","query":"q"}"#,
            status: 503,
            expected: "auth_not_found: no auth available",
            tokens: &[],
        },
        Case {
            name: "only credentials the policy admits are candidates",
            oauth: &[],
            script: &[],
            body: SEARCH,
            status: 503,
            expected: "auth_not_found: no auth available",
            tokens: &[],
        },
        Case {
            name: "a body that is not a search request is the client's error",
            oauth: &["b"],
            script: &[],
            body: r#"{"id":"s-1","model":"gpt-5.5","query":1}"#,
            status: 400,
            expected: "Invalid request",
            tokens: &[],
        },
        Case {
            name: "a duplicate member is the client's error",
            oauth: &["b"],
            script: &[],
            body: r#"{"id":"s-1","id":"s-1","model":"gpt-5.5","query":"q"}"#,
            status: 400,
            expected: "duplicate JSON key",
            tokens: &[],
        },
        Case {
            name: "invalid JSON is the client's error",
            oauth: &["b"],
            script: &[],
            body: r#"{"query":"q""#,
            status: 400,
            expected: "Invalid request",
            tokens: &[],
        },
    ];
    for case in cases {
        let name = case.name;
        let seen: Seen = Arc::default();
        let script: Script = Arc::new(Mutex::new(case.script.to_vec()));
        let upstream = axum::Router::new()
            .fallback(
                |State((seen, script)): State<(Seen, Script)>,
                 uri: axum::http::Uri,
                 headers: HeaderMap,
                 body: Bytes| async move {
                    seen.lock().unwrap().push((uri.path().to_owned(), headers, body));
                    let (status, body) = script.lock().unwrap().remove(0);
                    (
                        StatusCode::from_u16(status).unwrap(),
                        [("content-type", "application/json")],
                        body,
                    )
                        .into_response()
                },
            )
            .with_state((seen.clone(), script));
        let upstream_url = serve(upstream).await;
        let executor = CodexExecutor::with_client(wreq::Client::new(), CodexOAuth::new(wreq::Client::new()))
            .with_alpha_base_url(format!("{upstream_url}/backend-api/codex"));
        // An API key without alpha-search would be picked first by round-robin order.
        let mut credentials = vec![credential(
            "a-key.json",
            serde_json::json!({"type":"codex"}),
            &[
                ("api_key", "sk-FAKE"),
                ("base_url", &upstream_url),
                ("auth_kind", "apikey"),
            ],
        )];
        for id in case.oauth {
            // The executor's Alpha Search base URL is the local upstream.
            credentials.push(ck_server::testing::local(credential(
                &format!("{id}-oauth.json"),
                serde_json::json!({"type":"codex","access_token":format!("at-{id}"),"account_id":"acct-FAKE"}),
                &[("auth_kind", "oauth")],
            )));
        }
        let rt = Arc::new(ck_server::testing::runtime(
            Config::parse("{}").unwrap(),
            credentials,
            Executors {
                claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
                codex: executor,
                devices: Default::default(),
                openai: Default::default(),
                google: Default::default(),
            },
        ));
        rt.usage_queue().configure(
            true,
            &Config::parse("observability: {usage: {usage-statistics-enabled: true}}\n").unwrap(),
        );
        let proxy = serve(router(rt.clone())).await;
        let response = wreq::Client::new()
            .post(format!("{proxy}/backend-api/codex/alpha/search"))
            .header("user-agent", "codex_cli_rs/0.150.0")
            .header("version", "0.150.0")
            .header("session_id", "s-1")
            .header("authorization", "Bearer client-key-not-forwarded")
            .body(case.body)
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        let content_type = response.headers()["content-type"].clone();
        let stream = response.headers().get("x-ck-stream-id").cloned();
        let text = response.text().await.unwrap();
        assert_eq!(status, case.status, "{name}: {text}");
        assert_eq!(content_type, "application/json", "{name}");
        if case.status == 200 {
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&text).unwrap(),
                serde_json::from_str::<serde_json::Value>(case.expected).unwrap(),
                "{name}"
            );
            // A resume from the start repeats the body the client got.
            let resumed = wreq::Client::new()
                .get(format!("{proxy}/v1/streams?after=v1.b.0"))
                .header("x-ck-stream-id", stream.expect("a committed stream"))
                .send()
                .await
                .unwrap();
            assert_eq!(resumed.status().as_u16(), 200, "{name}");
            assert_eq!(resumed.text().await.unwrap(), text, "{name}");
        } else {
            assert!(text.contains(case.expected), "{name}: {text}");
        }
        // Go's Alpha Search reports no usage; an attempt settles on its own task.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(
            rt.usage_queue().pop_oldest(usize::MAX).is_empty(),
            "{name}: usage record"
        );
        let seen = seen.lock().unwrap();
        let tokens: Vec<_> = seen
            .iter()
            .map(|(_, headers, _)| headers["authorization"].to_str().unwrap().trim_start_matches("Bearer "))
            .collect();
        assert_eq!(tokens, case.tokens, "{name}: client key never forwarded");
        for (path, headers, body) in seen.iter() {
            assert_eq!(path, "/backend-api/codex/alpha/search", "{name}");
            assert_eq!(
                String::from_utf8_lossy(body),
                r#"{"id":"s-1","model":"gpt-5.5","query":"rust \u003cws\u003e"}"#,
                "{name}: prompt cache fields stripped with Go's map re-marshal"
            );
            assert_eq!(headers["chatgpt-account-id"], "acct-FAKE", "{name}");
            assert_eq!(headers["originator"], "codex_cli_rs", "{name}");
            assert_eq!(headers["user-agent"], "codex_cli_rs/0.150.0", "{name}");
            assert_eq!(headers["version"], "0.150.0", "{name}");
            assert_eq!(headers["session_id"], "s-1", "{name}");
        }
    }
}
