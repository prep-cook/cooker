//! Expected values come from Go (tests/reference/claude/main.go `login` section).

use super::*;
use axum::response::IntoResponse;
use serde_json::json;
use std::sync::Mutex;

fn golden() -> Value {
    serde_json::from_str(include_str!("claude/testdata/go_executor.json")).unwrap()
}

#[tokio::test]
async fn callback_server_answers_like_go() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, mut rx) = mpsc::channel(1);
    let server = tokio::spawn(serve(listener, tx));
    let client = wreq::Client::builder()
        .redirect(wreq::redirect::Policy::none())
        .no_proxy()
        .build()
        .unwrap();
    for case in golden()["login"]["server"].as_array().unwrap() {
        let target = case["target"].as_str().unwrap();
        let method = wreq::Method::from_bytes(case["method"].as_str().unwrap().as_bytes()).unwrap();
        let response = client
            .request(method, format!("http://{addr}{target}"))
            .send()
            .await
            .unwrap();
        let header = |name: &str| {
            response
                .headers()
                .get(name)
                .map(|v| v.to_str().unwrap().to_owned())
                .unwrap_or_default()
        };
        assert_eq!(
            response.status().as_u16() as u64,
            case["status"].as_u64().unwrap(),
            "{target}"
        );
        assert_eq!(
            header("content-type"),
            case["content_type"].as_str().unwrap(),
            "{target}"
        );
        assert_eq!(
            header("location"),
            case["location"].as_str().unwrap_or_default(),
            "{target}"
        );
        assert_eq!(
            header("x-content-type-options"),
            case["nosniff"].as_str().unwrap_or_default(),
            "{target}"
        );
        let body = response.text().await.unwrap();
        assert_eq!(body, case["body"].as_str().unwrap(), "{target}");
    }
    // Go keeps only the first result (a buffered channel of one).
    assert_eq!(
        rx.recv().await.unwrap(),
        Callback {
            code: "abc#frag".into(),
            state: "xyz".into(),
            ..Default::default()
        }
    );
    assert!(rx.try_recv().is_err());
    server.abort();
}

#[test]
fn success_page_escapes_the_platform_url() {
    let page = success_html(true, "https://x.invalid/\"><script>");
    assert!(page.contains("https://x.invalid/&#34;&gt;&lt;script&gt;"));
    assert!(!page.contains("<script>\""));
}

#[test]
fn pasted_callbacks_parse_like_go() {
    for case in golden()["login"]["callback_parse"].as_array().unwrap() {
        let input = case["input"].as_str().unwrap();
        let s = |k: &str| case[k].as_str().unwrap_or_default().to_owned();
        match parse_callback_input(input) {
            Ok(None) => assert_eq!(case["nil"], json!(true), "{input}"),
            Ok(Some(c)) => assert_eq!(
                c,
                Callback {
                    code: s("code"),
                    state: s("state"),
                    error: s("error"),
                    description: s("desc"),
                    client_id: String::new(),
                },
                "{input}"
            ),
            // url.Parse error text is Go's own; only its presence is ported.
            Err(message) if s("err").starts_with("parse ") => assert!(message.starts_with("parse "), "{input}"),
            Err(message) => assert_eq!(message, s("err"), "{input}"),
        }
    }
}

#[test]
fn file_names_match_go() {
    for case in golden()["login"]["file_names"].as_array().unwrap() {
        let s = |k: &str| case[k].as_str().unwrap();
        assert_eq!(
            credential_file_name(s("email"), s("organization"), s("account")),
            s("out")
        );
    }
}

/// Token, profile and roles endpoints for the exchange.
async fn oauth_mock() -> OAuth {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let router = axum::Router::new().fallback(|request: axum::extract::Request| async move {
        match request.uri().path() {
            "/token" => axum::Json(json!({"access_token":"sk-ant-oat-login-fake","refresh_token":"fake-r",
                "expires_in":3600,"account":{"uuid":"acct-1","email_address":"a@example.invalid"},
                "organization":{"uuid":"org-1","name":"Org"}}))
            .into_response(),
            "/profile" => axum::Json(json!({"account":{"uuid":"acct-1","email":"a@example.invalid"},
                "organization":{"uuid":"org-1","name":"Org"}}))
            .into_response(),
            _ => axum::Json(json!({})).into_response(),
        }
    });
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    OAuth::with_endpoints(
        wreq::Client::builder().no_proxy().build().unwrap(),
        &format!("{base}/token"),
        &format!("{base}/profile"),
        &format!("{base}/roles"),
    )
}

/// The callback listener on a port the kernel picked, bound once and handed to the
/// login: probing a free port and binding it again races parallel tests.
async fn callback() -> (tokio::net::TcpListener, u16) {
    let listener = bind(0).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, port)
}

fn temp_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("cooker-claude-login-{}", random_hex(8).unwrap()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn state_of(url: &str) -> String {
    url::Url::parse(url)
        .unwrap()
        .query_pairs()
        .find(|(k, _)| k == "state")
        .unwrap()
        .1
        .into_owned()
}

/// An interaction whose "browser" calls the callback with `state(url)`.
fn browser(port: u16, state: fn(&str) -> String) -> Interaction {
    Interaction {
        manual_delay: Duration::from_secs(3600),
        prompt: None,
        show_url: Box::new(move |url| {
            let target = format!("http://127.0.0.1:{port}/callback?code=fake-code&state={}", state(url));
            tokio::spawn(async move {
                let client = wreq::Client::builder().no_proxy().build().unwrap();
                let _ = client.get(target).send().await;
            });
        }),
    }
}

#[tokio::test]
async fn browser_login_merges_the_existing_go_file_and_ignores_other_names() {
    let dir = temp_dir();
    // The file of the same name is merged; an email-only file of the same account is
    // neither read nor removed.
    let existing = dir.join(credential_file_name("a@example.invalid", "org-1", "acct-1"));
    std::fs::write(
        &existing,
        r#"{"type":"claude","email":"a@example.invalid","organization_uuid":"org-1","proxy_url":"socks5://keep:1","access_token":"old","note":"<kept>"}"#,
    )
    .unwrap();
    let other = dir.join("claude-a@example.invalid.json");
    let other_bytes = r#"{"type":"claude","email":"a@example.invalid","organization_uuid":"org-1","label":"other"}"#;
    std::fs::write(&other, other_bytes).unwrap();
    let mock = oauth_mock().await;
    let (listener, port) = callback().await;
    let path = login_with(&dir, listener, mock, browser(port, state_of), None)
        .await
        .unwrap();
    assert_eq!(
        path,
        dir.join(credential_file_name("a@example.invalid", "org-1", "acct-1"))
    );
    let written = std::fs::read(&path).unwrap();
    let value: Value = serde_json::from_slice(&written).unwrap();
    assert_eq!(
        value["access_token"], "sk-ant-oat-login-fake",
        "token keys never come from the existing file"
    );
    assert_eq!(value["proxy_url"], "socks5://keep:1");
    assert_eq!(value["organization_uuid"], "org-1");
    // encoding/json: sorted keys, HTML-escaped strings, trailing newline.
    let text = String::from_utf8(written).unwrap();
    assert!(text.ends_with("}\n"));
    assert!(text.contains(r#""note":"\u003ckept\u003e""#));
    let keys: Vec<&String> = value.as_object().unwrap().keys().collect();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted);
    assert!(value.get("label").is_none());
    assert_eq!(std::fs::read_to_string(&other).unwrap(), other_bytes);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn a_wrong_state_fails_the_login_without_writing() {
    let dir = temp_dir();
    let mock = oauth_mock().await;
    let (listener, port) = callback().await;
    let error = login_with(&dir, listener, mock, browser(port, |_| "forged".into()), None)
        .await
        .unwrap_err();
    assert_eq!(
        error.body,
        "invalid_state: OAuth state parameter is invalid (caused by: state mismatch)"
    );
    assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn pasted_callback_completes_the_login() {
    let dir = temp_dir();
    let url: Arc<Mutex<String>> = Arc::default();
    let seen = url.clone();
    let interaction = Interaction {
        manual_delay: Duration::from_millis(20),
        prompt: Some(Box::new(move || {
            Box::pin(async move {
                let state = state_of(&seen.lock().unwrap());
                Ok(format!("localhost:54545/callback?code=fake-code&state={state}\n"))
            })
        })),
        show_url: Box::new(move |u| *url.lock().unwrap() = u.to_owned()),
    };
    let mock = oauth_mock().await;
    let (listener, _) = callback().await;
    let path = login_with(&dir, listener, mock, interaction, None).await.unwrap();
    assert!(path.exists());
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn an_empty_paste_keeps_waiting_for_the_browser() {
    let dir = temp_dir();
    let mock = oauth_mock().await;
    let (listener, port) = callback().await;
    let mut interaction = browser(port, state_of);
    let show = interaction.show_url;
    interaction.manual_delay = Duration::from_millis(1);
    interaction.prompt = Some(Box::new(|| Box::pin(async { Ok("\n".to_owned()) })));
    interaction.show_url = Box::new(move |url| {
        // The browser arrives after the empty paste.
        let url = url.to_owned();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            show(&url);
        });
    });
    assert!(login_with(&dir, listener, mock, interaction, None).await.is_ok());
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn a_busy_port_is_go_port_in_use() {
    // The realistic holder: another Go or cooker login's dual-stack listener.
    let holder = listen_any(0).unwrap();
    let port = holder.local_addr().unwrap().port();
    let error = bind(port).await.unwrap_err();
    assert!(String::from_utf8_lossy(&error.body).starts_with(PORT_IN_USE));
    // A holder on one family: on Unix the dual-stack socket conflicts with it. Windows
    // CI binds next to an IPv6-only `[::]` or a 127.0.0.1 holder (Go's listener is the
    // same socket, so Go binds too).
    for host in ["::", "127.0.0.1"] {
        let holder = tokio::net::TcpListener::bind((host, 0)).await.unwrap();
        let port = holder.local_addr().unwrap().port();
        let result = bind(port).await;
        #[cfg(not(windows))]
        assert!(
            String::from_utf8_lossy(&result.unwrap_err().body).starts_with(PORT_IN_USE),
            "{host}"
        );
        #[cfg(windows)]
        assert!(result.is_ok(), "{host}");
    }
}

/// Review of #127, finding 5: a malformed pasted callback URL is named redacted, so its
/// authorization code never reaches the login error.
#[test]
fn malformed_callback_urls_hide_the_code() {
    for input in [
        "http://127.0.0.1:bad/callback?code=sk-review-secret",
        "127.0.0.1:bad/callback?code=sk-review-secret&state=sk-review-secret",
    ] {
        let Err(message) = parse_callback_input(input) else {
            panic!("{input} parsed");
        };
        assert!(message.starts_with("parse "), "{message}");
        assert!(!message.contains("sk-review-secret"), "{message}");
    }
}

/// Round 3, finding 5: a callback's `error_description` holding a URL reaches the login
/// error redacted.
#[tokio::test]
async fn callback_errors_hide_url_credentials() {
    let dir = temp_dir();
    let interaction = Interaction {
        manual_delay: Duration::from_millis(1),
        prompt: Some(Box::new(|| {
            Box::pin(async {
                Ok("http://127.0.0.1/callback?error=access_denied&error_description=http%3A%2F%2Fu%3Ask-review-secret%40127.0.0.1%2F%3Fcode%3Dsk-review-secret%23sk-review-secret\n".to_owned())
            })
        })),
        show_url: Box::new(|_| {}),
    };
    let mock = oauth_mock().await;
    let (listener, _) = callback().await;
    let error = login_with(&dir, listener, mock, interaction, None).await.unwrap_err();
    let body = String::from_utf8_lossy(&error.body);
    assert_eq!(body, "OAuth error access_denied: <redacted url>");
    std::fs::remove_dir_all(dir).unwrap();
}
