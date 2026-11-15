use super::*;

/// `url.Parse` then `URL.String()`, recorded from Go 1.26.4 (tests/reference/go_url).
#[test]
fn string_follows_go() {
    let cases: serde_json::Value = serde_json::from_str(include_str!("../tests/fixtures/go_url_string.json")).unwrap();
    for case in cases.as_array().unwrap() {
        let input = case["in"].as_str().unwrap();
        let want = &case["string"];
        match parse(input) {
            Ok(u) => assert_eq!(Some(string(&u).as_str()), want["out"].as_str(), "{input}"),
            Err(e) => assert_eq!(Some(e.as_str()), want["err"].as_str(), "{input}"),
        }
    }
}

#[test]
fn transport_must_agree_with_go_on_the_authority() {
    for ok in [
        "https://auth.x.ai/oauth2/token",
        "https://AUTH.X.AI:443/t",
        "https://auth.x.ai:8443/t?q=1#f",
        "https://u:p@auth.x.ai/t",
        "https://例.x.ai/t",
        "http://127.0.0.1:9/t",
        "https://[::1]:8443/t",
    ] {
        assert!(same_authority(ok), "{ok}");
    }
    for bad in [
        r"https://attacker.example\[::1%25.x.ai]/oauth2/token",
        r"https://auth.x.ai\@evil.example/t",
        "https://auth.x.ai:99999/t",
        "https://%61uth.x.ai/t",
        "https:auth.x.ai/t",
    ] {
        assert!(!same_authority(bad), "{bad}");
    }
}

/// Go 1.26.4 `http.DefaultClient.Do` of a POST to each URL, the URL redacted (Q18).
#[test]
fn transport_refusals_match_go() {
    for (raw, want) in [
        (
            "ftp://relay.example/x",
            Some(r#"Post "ftp://relay.example/x": unsupported protocol scheme "ftp""#),
        ),
        (
            "//relay.example/x",
            Some(r#"Post "//relay.example/x": unsupported protocol scheme """#),
        ),
        (
            "https:relay.example/x",
            Some(r#"Post "<redacted url>": http: no Host in request URL"#),
        ),
        ("HTTPS://relay.example/x", None),
        ("http://127.0.0.1:9/x", None),
    ] {
        let got = transport_refusal(raw).map(|e| String::from_utf8_lossy(&e.body).into_owned());
        assert_eq!(got.as_deref(), want, "{raw}");
    }
}

/// The refusals every relay arm returns and captures name no credential of the URL.
#[test]
fn refusals_never_print_url_credentials() {
    const SECRET: &str = "sk-review-secret";
    let body = |e: ck_core::exec::ExecError| String::from_utf8_lossy(&e.body).into_owned();
    for (raw, want) in [
        (
            format!("http://user:{SECRET}@127.0.0.1:bad/?api_key={SECRET}"),
            r#"parse "<redacted url>": invalid port after host"#,
        ),
        (
            format!("http://u:{SECRET}/x@h/"),
            r#"parse "<redacted url>": invalid port after host"#,
        ),
        // Go quotes the invalid escape, here the start of the password.
        (
            format!("http://u:%{SECRET}@h/"),
            r#"parse "http://h/": net/url: invalid userinfo"#,
        ),
        // Go quotes the invalid escape, here the masked fragment (review of #127, finding 4).
        (
            "http://127.0.0.1/v1#access_token=%ZZ".to_owned(),
            r#"parse "http://127.0.0.1/v1#***": invalid URL escape "***""#,
        ),
        (
            format!("http://127.0.0.1/v1#{SECRET}%{SECRET}"),
            r#"parse "http://127.0.0.1/v1#***": invalid URL escape "***""#,
        ),
    ] {
        let got = body(new_request(&raw).unwrap_err());
        assert_eq!(got, want, "{raw}");
        assert!(!got.contains(SECRET) && !got.contains("%ZZ"), "{got}");
    }
    for (raw, want) in [
        (
            format!("ftp://user:{SECRET}@127.0.0.1/?api_key={SECRET}"),
            r#"Post "ftp://127.0.0.1/?api_key=***": unsupported protocol scheme "ftp""#,
        ),
        // Query bytes never become the host (review of #127, finding 1).
        (
            format!("ftp://127.0.0.1?key=prefix@{SECRET}"),
            r#"Post "<redacted url>": unsupported protocol scheme "ftp""#,
        ),
        (
            format!("ftp://127.0.0.1/v1?key={SECRET}"),
            r#"Post "ftp://127.0.0.1/v1?key=***": unsupported protocol scheme "ftp""#,
        ),
        (
            format!("https:{SECRET}"),
            r#"Post "<redacted url>": http: no Host in request URL"#,
        ),
        (
            format!("{SECRET}/v1"),
            r#"Post "<redacted url>": unsupported protocol scheme """#,
        ),
    ] {
        let got = body(transport_refusal(&raw).unwrap());
        assert_eq!(got, want, "{raw}");
        assert!(!got.contains(SECRET), "{got}");
    }
}

/// Go 1.26.4 `url.Parse`: an IP literal must start the host. The URL is malformed, so
/// it prints as the placeholder.
#[test]
fn a_bracket_inside_the_host_is_go_s_invalid_ip_literal() {
    for (raw, want) in [
        ("http://x[::1]", r#"parse "<redacted url>": invalid IP-literal"#),
        ("http://x[::1]:80/p", r#"parse "<redacted url>": invalid IP-literal"#),
        ("http://[::1]x/", r#"parse "<redacted url>": invalid port after host"#),
    ] {
        assert_eq!(parse(raw).err().as_deref(), Some(want), "{raw}");
    }
    assert_eq!(parse("http://[::1]:80/p").map(|u| u.hostname).as_deref(), Ok("::1"));
}
