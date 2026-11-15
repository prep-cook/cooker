use super::*;

fn string(value: &Value, key: &str) -> String {
    value[key].as_str().unwrap_or_default().to_owned()
}
fn int(value: &Value, key: &str) -> i64 {
    value[key].as_i64().unwrap_or_default()
}
fn boolean(value: &Value, key: &str) -> bool {
    value[key].as_bool().unwrap_or_default()
}
fn err(value: &Value) -> String {
    value["err"].as_str().unwrap().to_owned()
}

#[test]
fn device_records_decode_like_go_json_unmarshal() {
    // Go json.Unmarshal into DeviceCodeResponse, TokenData and MintedKeyResponse.
    let fx = fixture("meta", "decode");
    for case in fx["extra"]["cases"].as_array().unwrap() {
        let input = case["input"].as_str().unwrap();
        let raw = input.as_bytes();

        let go = &case["device"];
        match crate::meta_auth::DeviceCode::decode_for_test(raw) {
            Ok(c) => {
                assert_eq!(err(go), "", "device {input}");
                let v = &go["value"];
                assert_eq!(
                    (
                        c.device_code,
                        c.user_code,
                        c.verification_uri,
                        c.verification_uri_complete
                    ),
                    (
                        string(v, "device_code"),
                        string(v, "user_code"),
                        string(v, "verification_uri"),
                        string(v, "verification_uri_complete")
                    ),
                    "device {input}"
                );
                assert_eq!(
                    (c.expires_in, c.interval),
                    (int(v, "expires_in"), int(v, "interval")),
                    "device {input}"
                );
            }
            Err(e) => assert_eq!(e, err(go), "device {input}"),
        }
    }
}

#[test]
fn token_records_decode_like_go_json_unmarshal() {
    let fx = fixture("meta", "decode");
    for case in fx["extra"]["cases"].as_array().unwrap() {
        let input = case["input"].as_str().unwrap();
        let raw = input.as_bytes();

        let go = &case["token"];
        let (t, result) = crate::meta_auth::TokenData::decode_for_test(raw);
        assert_eq!(result.err().unwrap_or_default(), err(go), "token {input}");
        let v = &go["value"];
        assert_eq!(
            (t.access_token, t.token_type, t.error, t.error_description),
            (
                string(v, "access_token"),
                string(v, "token_type"),
                string(v, "error"),
                string(v, "error_description")
            ),
            "token {input}"
        );
        assert_eq!(
            (t.expires_in, t.expires_at),
            (int(v, "expires_in"), int(v, "expires_at")),
            "token {input}"
        );
    }
}

#[test]
fn minted_records_decode_like_go_json_unmarshal() {
    let fx = fixture("meta", "decode");
    for case in fx["extra"]["cases"].as_array().unwrap() {
        let input = case["input"].as_str().unwrap();
        let raw = input.as_bytes();

        let go = &case["mint"];
        match MintedKey::decode(raw) {
            Ok(m) => {
                assert_eq!(err(go), "", "mint {input}");
                let v = &go["value"];
                assert_eq!(
                    (
                        m.api_key,
                        m.base_url,
                        m.user_email,
                        m.user_full_name,
                        m.subs_tier_name,
                        m.subs_tier_id
                    ),
                    (
                        string(v, "api_key"),
                        string(v, "base_url"),
                        string(v, "user_email"),
                        string(v, "user_full_name"),
                        string(v, "subs_tier_name"),
                        string(v, "subs_tier_id")
                    ),
                    "mint {input}"
                );
                assert_eq!(
                    (
                        m.is_subs_active,
                        m.has_payment_method,
                        m.require_payment,
                        m.can_subscribe
                    ),
                    (
                        boolean(v, "is_subs_active"),
                        boolean(v, "has_payment_method"),
                        boolean(v, "require_payment"),
                        boolean(v, "can_subscribe")
                    ),
                    "mint {input}"
                );
            }
            Err(e) => assert_eq!(e, err(go), "mint {input}"),
        }
    }
}

#[test]
fn syntax_errors_quote_every_byte_like_go() {
    // json.Unmarshal of each single byte: scanner messages and quoteChar.
    let fx = fixture("meta", "decode");
    for case in fx["extra"]["bytes"].as_array().unwrap() {
        let byte = case["byte"].as_u64().unwrap() as u8;
        let go = case["err"].as_str().unwrap();
        let rust = crate::meta_wire::check_valid(&[byte]);
        if go.starts_with("json: cannot unmarshal") {
            assert_eq!(rust, Ok(()), "byte {byte}");
        } else {
            assert_eq!(rust.unwrap_err(), go, "byte {byte}");
        }
    }
}

#[tokio::test]
async fn poll_errors_use_go_partial_decoding_and_key_folding() {
    // `{"error":5,"ERROR":"access_denied"}`: Go records the type error, keeps decoding,
    // and the folded key still denies access.
    let device = json!({"status":200,"body":r#"{"DEVICE_CODE":"d","User_Code":"u","interval":1}"#});
    let mock = Mock::start(&json!([device, {"status":400,"body":r#"{"error":5,"ERROR":"access_denied"}"#}])).await;
    let auth = login_auth(&mock.url);
    let code = auth.start_device_flow().await.unwrap();
    assert_eq!(
        auth.wait_for_authorization(&code).await.unwrap_err(),
        "meta auth: access was denied by user"
    );
    // A null mint body decodes to an empty record: Go's "missing api_key".
    let mock = Mock::start(
        &json!([{"status":200,"body":"null"}, {"status":200,"body":r#"{"api_key":"first","api_key":"second"}"#}]),
    )
    .await;
    let auth = login_auth(&mock.url);
    assert_eq!(
        auth.mint_api_key("dca:n").await.unwrap_err().to_string(),
        "meta auth: mint response missing api_key"
    );
    assert_eq!(auth.mint_api_key("dca:d").await.unwrap().api_key, "second");
}
#[test]
fn login_merge_skips_an_existing_file_go_cannot_decode() {
    // Manager.Login ignores an existing file whose map decode fails (float64 overflow);
    // the standalone writer keeps Go's partial map with null.
    let dir = TempDir::new();
    let bundle = meta_auth::Bundle {
        token: meta_auth::TokenData {
            access_token: "dca:x".into(),
            ..Default::default()
        },
        ..Default::default()
    };
    let name = credential_file_name("", "dca:x");
    std::fs::write(dir.path().join(&name), r#"{"type":"meta","custom":1e400,"prefix":"p"}"#).unwrap();
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-02T00:00:00Z")
        .unwrap()
        .to_utc();
    let outcome = meta_auth::save_login(dir.path(), &bundle, now, None).unwrap();
    let written = std::fs::read_to_string(&outcome.path).unwrap();
    assert!(serde_json::from_str::<Value>(&written).is_ok(), "valid JSON: {written}");
    assert!(!written.contains("prefix") && !written.contains("custom"), "{written}");
    let storage = TokenStorage {
        access_token: "a".into(),
        ..Default::default()
    };
    assert_eq!(
        encode_token_file(&storage, None, Some(br#"{"custom":1e400,"n":1.0}"#)),
        "{\n  \"access_token\": \"a\",\n  \"auth_kind\": \"oauth\",\n  \"custom\": null,\n  \"n\": 1,\n  \"type\": \"meta\"\n}\n"
    );
}

#[tokio::test]
async fn login_skips_existing_file_with_an_overwritten_overflow() {
    // Go records the 1e400 decode error even though "x" is overwritten by 0, so
    // Manager.Login merges nothing from the old file (no disabled, no prefix).
    let fx = fixture("meta", "login-existing-overflow");
    let mock = Mock::start(&fx["responses"]).await;
    let dir = TempDir::new();
    let go_file = fx["extra"]["file_name"].as_str().unwrap();
    std::fs::write(dir.path().join(go_file), fx["request"]["stale_file"].as_str().unwrap()).unwrap();
    let go_raw = fx["extra"]["file_raw"].as_str().unwrap();
    let now = chrono::DateTime::parse_from_rfc3339(&gj::get(go_raw.as_bytes(), "last_refresh").str())
        .unwrap()
        .to_utc();
    let outcome = login_with(login_auth(&mock.url).with_fixed_now(now), dir.path(), true, None)
        .await
        .unwrap();
    let written = std::fs::read_to_string(&outcome.path).unwrap();
    // dca_expired/dca_expires_at derive from the poll time, which differs per run.
    let strip = |s: &str| {
        let mut v: Value = serde_json::from_str(s).unwrap();
        for key in ["dca_expired", "dca_expires_at"] {
            v.as_object_mut().unwrap().remove(key);
        }
        v
    };
    assert_eq!(strip(&written), strip(go_raw));
}
#[tokio::test]
async fn mint_errors_follow_go() {
    let mock = Mock::start(&json!([
        {"status":200,"body":r#"{"api_key":""}"#},
        {"status":200,"body":r#"{"api_key":"k","is_subs_active":"yes"}"#},
        {"status":403,"body":" denied "},
    ]))
    .await;
    let auth = login_auth(&mock.url);
    assert_eq!(
        auth.mint_api_key("dca:1").await.unwrap_err().to_string(),
        "meta auth: mint response missing api_key"
    );
    assert!(
        auth.mint_api_key("dca:2")
            .await
            .unwrap_err()
            .to_string()
            .starts_with("meta auth: parse mint response: ")
    );
    // Go: "meta auth: mint key failed (HTTP 403): denied"; the body is withheld, in every
    // formatting of the error.
    let e = auth.mint_api_key("dca:3").await.unwrap_err();
    crate::kimi_fixture::assert_go_message_without_body(
        "mint 403",
        &e.to_string(),
        "meta auth: mint key failed (HTTP 403): denied",
    );
    assert_eq!(e.redacted(), e.to_string());
    assert!(!format!("{e:?}").contains("denied"));
    assert_eq!(
        auth.mint_api_key("  ").await.unwrap_err().to_string(),
        "meta auth: missing dca token"
    );
}
