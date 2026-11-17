//! Request shaping regressions.
use super::schema::rational;
use super::*;
/// Go v8.0.35's `NormalizeCodexToolSchemas` (57d005fe bounds, patterns, re-encoding)
/// and `NormalizeCodexToolIntegerTypes` (d7914afd paths and namespaces), byte for
/// byte; see tests/reference/codex_tool_schema.
#[test]
fn tool_schema_passes_match_go() {
    let go: serde_json::Value =
        serde_json::from_str(include_str!("../../tests/fixtures/codex_tool_schema_go.json")).unwrap();
    let expected = |case: &serde_json::Value| case.get("out").unwrap_or(&case["in"]).as_str().unwrap().to_owned();
    for case in go["schemas"].as_array().unwrap() {
        let input = case["in"].as_str().unwrap().to_owned();
        assert_eq!(normalize_tool_schemas(input), expected(case), "{}", case["name"]);
    }
    for case in go["integers"].as_array().unwrap() {
        let mut headers = HeaderMap::new();
        if let Some(ua) = case["ua"].as_str() {
            headers.insert(http::header::USER_AGENT, ua.parse().unwrap());
        }
        let out =
            ck_wire::payload::normalize_codex_tool_integer_types(case["in"].as_str().unwrap().as_bytes(), &headers);
        assert_eq!(
            String::from_utf8(out.clone()).unwrap(),
            expected(case),
            "{} {:?}",
            case["name"],
            case["ua"]
        );
        // Go's tests also require a second pass to leave the output alone.
        let again = ck_wire::payload::normalize_codex_tool_integer_types(&out, &headers);
        assert_eq!(again, out, "{} is not idempotent", case["name"]);
    }
}

/// Go v8.0.35's `ensureWebSearchToolForHistory` (e7d4c731), byte for byte; see
/// tests/reference/codex_web_search_history.
#[test]
fn web_search_history_matches_go() {
    let go: serde_json::Value =
        serde_json::from_str(include_str!("../../tests/fixtures/codex_web_search_history_go.json")).unwrap();
    for case in go["cases"].as_array().unwrap() {
        let mut headers = HeaderMap::new();
        if let Some(value) = case["lite_header"].as_str() {
            // Raw bytes: a non-ASCII value such as U+00A0 reaches the Lite check as sent.
            headers.insert(LITE_HEADER, HeaderValue::from_bytes(value.as_bytes()).unwrap());
        }
        let input = case["in"].as_str().unwrap();
        let want = case.get("out").unwrap_or(&case["in"]).as_str().unwrap();
        assert_eq!(
            ensure_web_search_for_history(input.to_owned(), &headers),
            want,
            "{}",
            case["name"]
        );
    }
}

/// Config models compare names and aliases with Go's EqualFold: İ does not fold to
/// i, while σ and ς fold together.
#[test]
fn config_models_compare_names_with_go_fold() {
    let request = |model: &str| ExecRequest {
        operation: ck_core::exec::Operation::Generate,
        source_format: Format::Codex,
        response_format: Format::Codex,
        requested_model: model.into(),
        model: model.into(),
        original_body: Default::default(),
        body: Default::default(),
        stream: false,
        alt: None,
        session: None,
        execution_session: None,
        derived_session: None,
        request_path: String::new(),
        headers: Default::default(),
        caller: ck_core::exec::Caller {
            principal: String::new(),
            source: "",
            provider: String::new(),
        },
        resolved_model: None,
        usage: Default::default(),
    };
    let keyed = |name: &str| Settings {
        key_models: vec![serde_json::json!({"name": name, "is-compat": true})],
        ..Settings::default()
    };
    assert!(
        !keyed("mİ").is_compat(&Inbound::of(&request("mi"))),
        "dotted İ is not i"
    );
    assert!(keyed("ος").is_compat(&Inbound::of(&request("ΟΣ"))), "final sigma folds");
}

#[test]
fn durations_follow_go_parse_duration() {
    assert_eq!(go_duration("1m30s"), Some(Duration::from_secs(90)));
    assert_eq!(go_duration("1.5s"), Some(Duration::from_millis(1500)));
    assert_eq!(go_duration("45"), Some(Duration::from_secs(45)));
    assert_eq!(go_duration("off"), None);
    assert_eq!(go_duration("soon"), None);
    assert_eq!(go_duration("+2s"), Some(Duration::from_secs(2)));
    assert_eq!(go_duration("3\u{3bc}s"), Some(Duration::from_micros(3)));
    assert_eq!(go_duration("-1s"), None);
    assert_eq!(go_duration("+15"), Some(Duration::from_secs(15)));
}

#[test]
fn rationals_compare_like_big_rat() {
    assert_eq!(rational("8"), rational("8.0"));
    assert_eq!(rational("8"), rational("0.8e1"));
    assert_eq!(rational("-0.50"), rational("-5e-1"));
    assert_ne!(rational("8"), rational("80"));
    assert_eq!(rational("0.000"), "0");
}

/// `chatgpt-keep-alive` is off unless the config turns it on, and YAML 1.1 spellings
/// decode as Go decodes a bool field.
#[test]
fn chatgpt_keep_alive_is_off_unless_configured() {
    let cases = [
        ("", false),
        ("upstream:\n  codex:\n    disable-codex-cloaking: true\n", false),
        (
            "oauth:\n  providers:\n    codex:\n      chatgpt-keep-alive: false\n",
            false,
        ),
        (
            "oauth:\n  providers:\n    codex:\n      chatgpt-keep-alive: true\n",
            true,
        ),
        (
            "oauth:\n  providers:\n    codex:\n      chatgpt-keep-alive: yes\n",
            true,
        ),
    ];
    for (yaml, want) in cases {
        let cfg = Config::parse(yaml).unwrap();
        assert_eq!(chatgpt_keep_alive(&cfg), want, "{yaml:?}");
    }
}
