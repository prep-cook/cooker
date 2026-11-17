use super::*;

#[test]
fn upstream_model_normalization_matches_go_table() {
    // TestNormalizeKimiUpstreamModel (kimi_executor_test.go).
    for (input, want) in [
        ("kimi-k3[1m]", "k3"),
        ("Kimi-K3[1M]", "k3"),
        ("k3", "k3"),
        ("kimi-k2.6[1m]", "k2.6"),
        ("kimi-k3[1m](1024)", "k3(1024)"),
        ("kimi-k2.6(high)", "k2.6(high)"),
        ("kimi-k2.7-code", "kimi-for-coding"),
        ("Kimi-K2.7-Code", "kimi-for-coding"),
        ("kimi-k2.7-code-highspeed(high)", "kimi-for-coding-highspeed(high)"),
        ("kimi-k2.7-code[1m](high)", "kimi-for-coding(high)"),
        ("k2.8-code", "kimi-for-coding"),
        ("kimi-k2.8-preview", "kimi-for-coding"),
        ("kimi-k2.8(max)", "kimi-for-coding(max)"),
        ("Kimi-For-Coding", "kimi-for-coding"),
        ("kimi-for-coding[1m]", "kimi-for-coding"),
        ("for-coding-highspeed", "kimi-for-coding-highspeed"),
    ] {
        assert_eq!(normalize_upstream_model(input), want, "{input}");
    }
}

#[test]
fn urls_follow_go_resolution() {
    // TestResolveKimiResponsesURL / ChatURL / ClaudeBaseURL (kimi_responses_test.go).
    let cred = |base: &str| {
        let mut c = Credential::from_file(
            std::path::Path::new("/a"),
            std::path::Path::new("/a/k.json"),
            serde_json::json!({"type":"kimi"}).as_object().unwrap().clone(),
        )
        .unwrap();
        if !base.is_empty() {
            c.attributes.insert("base_url".into(), base.into());
        }
        c
    };
    assert_eq!(responses_url(&cred("")), "https://api.kimi.com/coding/v1/responses");
    assert_eq!(
        responses_url(&cred("https://api.kimi.com/coding/")),
        "https://api.kimi.com/coding/v1/responses"
    );
    assert_eq!(
        responses_url(&cred("https://api.kimi.com/coding/v1")),
        "https://api.kimi.com/coding/v1/responses"
    );
    assert_eq!(
        chat_url(&cred("https://api.kimi.ai/coding/v1/")),
        "https://api.kimi.ai/coding/v1/chat/completions"
    );
    assert_eq!(
        claude_base_url(&cred("https://api.kimi.ai/coding/v1")),
        "https://api.kimi.ai/coding"
    );
    assert_eq!(claude_base_url(&cred("")), "https://api.kimi.com/coding");
    let mut ai = cred("");
    ai.provider = "kimi-ai".into();
    ai.metadata.insert("type".into(), "kimi-ai".into());
    assert_eq!(chat_url(&ai), "https://api.kimi.ai/coding/v1/chat/completions");
}

#[test]
fn pure_functions_match_go_vectors() {
    // vectors.json: inputs run through the Go functions by zz_rsfix_vectors_test.go.
    let path = format!("{}/tests/device_fixtures/kimi/vectors.json", env!("CARGO_MANIFEST_DIR"));
    let vectors: Vec<Value> = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    assert!(vectors.len() >= 30);
    for v in &vectors {
        let input = v["in"].as_str().unwrap();
        let want = v["out"].as_str().unwrap();
        let err = v["err"].as_str();
        let bytes = input.as_bytes();
        let text = |out: Vec<u8>| String::from_utf8(out).unwrap();
        let path = || v["path"].as_str().unwrap();
        let value = || v["value"].as_str().unwrap_or_default();
        let got: Result<String, String> = match v["fn"].as_str().unwrap() {
            "schema" => Ok(text(normalize_parameters_schema(bytes))),
            "links" => normalize_tool_message_links(bytes.to_vec())
                .map(text)
                .map_err(|e| String::from_utf8_lossy(&e.body).into_owned()),
            "temperature" => continue, // Superseded by the K5 wire regression.
            "responses_input" => Ok(text(normalize_responses_input(bytes.to_vec()))),
            "thinking" => continue, // Kimi dialects are covered by the execution contract tests.
            "restore_model" => Ok(text(
                restore_response_model(bytes, v["model"].as_str().unwrap()).to_vec(),
            )),
            "sjson_delete" => Ok(text(gj::try_delete(bytes, path()).unwrap_or_else(|_| bytes.to_vec()))),
            "sjson_set_str" => gj::try_set_str(bytes, path(), value()).map(text),
            "sjson_set_raw" => gj::try_set_raw(bytes, path(), value()).map(text),
            "gjson_string" => Ok(text(gj::get(bytes, "n").bytes().into_owned())),
            other => panic!("unknown vector {other}"),
        };
        match err {
            Some(message) => assert_eq!(got, Err(message.to_owned()), "{v}"),
            // The links normalization edits the body in place and keeps the white space Go's
            // rebuilt array dropped, so its output is compared as JSON.
            None if v["fn"] == "links" => {
                let json = |s: &str| serde_json::from_str::<Value>(s).unwrap();
                assert_eq!(json(&got.unwrap()), json(want), "{v}");
            }
            None => assert_eq!(got, Ok(want.to_owned()), "{v}"),
        }
    }
}
