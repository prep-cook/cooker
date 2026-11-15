use super::*;
/// The minted key the fixture's mint endpoint answered with.
fn minted(r: &Run, index: usize) -> MintedKey {
    let body = replace_origin(&r.fx["responses"][index]["body"], &r.origin, &r.url);
    MintedKey::decode(body.as_str().unwrap().as_bytes()).unwrap()
}

fn assert_metadata_after(r: &Run, name: &str, patch: &MetadataPatch) {
    let mut after = r.credential.metadata.clone();
    patch.apply(&mut after);
    let mut go = replace_origin(&r.fx["extra"]["metadata_after"], &r.origin, &r.url)
        .as_object()
        .cloned()
        .unwrap();
    let stamp = after.remove("last_refresh").and_then(|v| v.as_str().map(str::to_owned));
    assert!(
        stamp.is_some_and(|s| chrono::DateTime::parse_from_rfc3339(&s).is_ok()),
        "{name}: last_refresh is RFC3339"
    );
    go.remove("last_refresh");
    assert_eq!(
        Value::Object(after),
        Value::Object(go),
        "{name}: metadata after refresh"
    );
}

#[tokio::test]
async fn remint_patch_matches_go_metadata() {
    for name in ["remint-from-dca", "remint-access-token-dca-minted-base"] {
        let r = run(name).await;
        let dca = dca_token(&r.credential).unwrap();
        let patch = refresh_patch(&r.credential, &dca, &minted(&r, 0), "2026-10-02T00:00:00Z".into());
        assert_metadata_after(&r, name, &patch);
    }
}

#[tokio::test]
async fn prepare_remints_and_matches_go_refresh() {
    let fx = fixture("meta", "refresh-direct");
    let origin = go_origin(&fx).unwrap();
    let mock = Mock::start_replacing(&fx["responses"], Some(&origin)).await;
    let credential = meta_credential(&fx, &origin, &mock.url);
    let exec = MetaExecutor::with_client(default_client()).with_mint_url(&format!("{}/muse-code/key", mock.url));
    assert!(
        !exec.needs_prepare(&credential, &cfg()),
        "a usable key and no refresh interval: nothing to prepare (RefreshLead nil)"
    );
    let patch = exec.prepare(&credential, &cfg()).await.unwrap();
    let r = Run {
        usage: Default::default(),
        captured: mock.captured(),
        url: mock.url,
        fx,
        origin,
        credential,
        body: None,
        chunks: Vec::new(),
        error: None,
    };
    assert_upstream(&r, "refresh-direct");
    assert_metadata_after(&r, "refresh-direct", &patch);
}

#[test]
fn needs_prepare_matches_should_prepare_request_auth() {
    // Cases from meta_executor_test.go TestMetaExecutor_RequestAuthPreparer.
    let cred = |meta: Value, attrs: &[(&str, &str)]| {
        let mut c = Credential::from_file(
            std::path::Path::new("/a"),
            std::path::Path::new("/a/meta.json"),
            meta.as_object().unwrap().clone(),
        )
        .unwrap();
        c.provider = PROVIDER.into();
        c.attributes = attrs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        c
    };
    let exec = MetaExecutor::default();
    let cfg = cfg();
    assert!(!exec.needs_prepare(&cred(json!({"type":"meta"}), &[("api_key", "exists")]), &cfg));
    assert!(!exec.needs_prepare(&cred(json!({"type":"meta"}), &[]), &cfg));
    let config_dca = cred(
        json!({"type":"meta"}),
        &[("api_key", "dca:invalid-for-config"), ("source", "config:meta[0]")],
    );
    assert!(is_config_api_key(&config_dca));
    // Go `AuthSourceKind` folds with strings.ToLower and EqualFold, which map U+0130 to
    // `i` (outputs of Go's IsConfigAPIKeyAuth at ae3ee3be).
    for (attrs, want) in [
        (&[("auth_kind", "apikey"), ("source_backend", "CONFİG")][..], true),
        (&[("auth_kind", "apikey"), ("source", "CONFİG:meta[0]")][..], true),
        (&[("auth_kind", "apikey"), ("source", " Config ")][..], true),
        (
            &[("auth_kind", "apikey"), ("source", "config"), ("runtime_only", "TRUE")][..],
            false,
        ),
        (
            &[
                ("auth_kind", "apikey"),
                ("source", "config:meta[0]"),
                ("source_backend", "Fİle"),
            ][..],
            false,
        ),
    ] {
        assert_eq!(
            is_config_api_key(&cred(json!({"type":"meta"}), attrs)),
            want,
            "{attrs:?}"
        );
    }
    assert!(!exec.needs_prepare(&config_dca, &cfg));
    assert!(exec.needs_prepare(
        &cred(
            json!({"type":"meta","dca_token":"dca:valid","auth_kind":"oauth"}),
            &[("base_url", "http://x")]
        ),
        &cfg
    ));
    // A due refresh_interval re-mints even with a key (shouldRefresh's preferred interval).
    assert!(exec.needs_prepare(
        &cred(
            json!({"type":"meta","api_key":"k","dca_token":"dca:v","refresh_interval":60,"last_refresh":"2020-01-01T00:00:00Z"}),
            &[]
        ),
        &cfg
    ));
}

#[test]
fn creds_resolution_matches_go() {
    // Cases from meta_executor_test.go TestMetaExecutor_MetaCredsResolution.
    let mut c = Credential::from_file(
        std::path::Path::new("/a"),
        std::path::Path::new("/a/meta.json"),
        json!({"type":"meta"}).as_object().unwrap().clone(),
    )
    .unwrap();
    c.attributes.insert("api_key".into(), "attr-key".into());
    c.attributes
        .insert("base_url".into(), "https://custom.meta.com/v1".into());
    assert_eq!(creds(&c), ("https://custom.meta.com/v1".into(), "attr-key".into()));
    c.attributes.clear();
    c.metadata.insert("access_token".into(), "meta-oauth-token".into());
    assert_eq!(creds(&c), (DEFAULT_API_BASE_URL.into(), "meta-oauth-token".into()));
    // DCA tokens are never used as keys but are the mint source.
    c.metadata.insert("access_token".into(), " dca:x ".into());
    c.metadata.insert("api_base_url".into(), "https://alt/v1".into());
    assert_eq!(creds(&c), ("https://alt/v1".into(), String::new()));
    assert_eq!(dca_token(&c).as_deref(), Some("dca:x"));
}
#[test]
fn file_names_and_writer_match_go() {
    let fx = fixture("meta", "writer");
    for case in fx["extra"]["file_names"].as_array().unwrap() {
        assert_eq!(
            credential_file_name(case["email"].as_str().unwrap(), case["sub"].as_str().unwrap()),
            case["out"].as_str().unwrap()
        );
    }
    let dir = TempDir::new();
    for case in fx["extra"]["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let storage: TokenStorage = serde_json::from_value(case["storage"].clone()).unwrap();
        let snapshot: Option<GoMap> = case["snapshot"]
            .as_object()
            .map(|m| go_map_from_json(&serde_json::to_vec(m).unwrap()).unwrap().0);
        let disk = case["disk"].as_str().unwrap();
        let out = case["out"].as_str().unwrap();
        assert_eq!(
            encode_token_file(&storage, snapshot.as_ref(), Some(disk.as_bytes())),
            out,
            "{name}"
        );
        // The file path: disk is read only without a snapshot, then atomically replaced.
        let path = dir.path().join(format!("{name}.json"));
        std::fs::write(&path, disk).unwrap();
        meta_auth::save_token_file(&path, &storage, snapshot.as_ref()).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), out, "{name}: file");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
        }
    }
    let leftovers: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with('.'))
        .collect();
    assert!(leftovers.is_empty(), "no temporary files survive");
}

pub(super) fn login_auth(url: &str) -> MetaAuth {
    MetaAuth::new(default_client())
        .with_auth_host(url)
        .with_mint_url(&format!("{url}/muse-code/key"))
        .with_poll_unit(Duration::from_millis(10))
}

#[tokio::test]
async fn login_matches_go_requests_and_file() {
    for name in ["login", "login-no-mint"] {
        let fx = fixture("meta", name);
        let mock = Mock::start(&fx["responses"]).await;
        let dir = TempDir::new();
        let go_file = fx["extra"]["file_name"].as_str().unwrap();
        if let Some(stale) = fx["request"]["stale_file"].as_str().filter(|s| !s.is_empty()) {
            std::fs::write(dir.path().join(go_file), stale).unwrap();
        }
        let go_raw = fx["extra"]["file_raw"].as_str().unwrap();
        let stamp = gj::get(go_raw.as_bytes(), "last_refresh").str().into_owned();
        let now = chrono::DateTime::parse_from_rfc3339(&stamp).unwrap().to_utc();
        let outcome = login_with(login_auth(&mock.url).with_fixed_now(now), dir.path(), true, None)
            .await
            .unwrap();
        let go: Vec<Captured> = fx["upstream"]
            .as_array()
            .unwrap()
            .iter()
            .map(Captured::from_fixture)
            .collect();
        let rust = mock.captured();
        assert_eq!(rust.len(), go.len(), "{name}: request count");
        for (go, rust) in go.iter().zip(&rust) {
            assert_same_request(name, go, rust, &["Host"]);
        }
        assert_eq!(outcome.path, dir.path().join(go_file), "{name}: file name");
        assert_eq!(outcome.label, fx["extra"]["label"].as_str().unwrap(), "{name}: label");
        assert_eq!(
            std::fs::read_to_string(&outcome.path).unwrap(),
            go_raw,
            "{name}: file bytes"
        );
    }
}

#[tokio::test]
async fn failed_login_mint_logs_no_response_body() {
    // login-no-mint's flow, with a mint endpoint whose error body carries key material.
    let fx = fixture("meta", "login-no-mint");
    let mut responses = fx["responses"].clone();
    let secret = "sk-mint-SECRET-0123456789";
    responses[2]["body"] = json!(format!(r#"{{"error":"bad","api_key":"{secret}"}}"#));
    let mock = Mock::start(&responses).await;
    let dir = TempDir::new();
    let capture = crate::kimi_fixture::LogCapture::default();
    let guard = capture.install();
    let outcome = login_with(login_auth(&mock.url), dir.path(), true, None).await;
    drop(guard);
    assert!(outcome.is_ok(), "a failed mint does not fail the login");
    let logs = capture.0.lock().unwrap().clone();
    assert!(
        logs.iter()
            .any(|l| l.contains("could not mint api_key from dca_token: meta auth: mint key failed (HTTP 500)")),
        "the mint failure is logged: {logs:?}"
    );
    for line in &logs {
        assert!(
            !line.contains(secret) && !line.contains("\"bad\""),
            "body logged: {line}"
        );
    }
}
