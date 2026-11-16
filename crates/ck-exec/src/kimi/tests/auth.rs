use super::*;

#[tokio::test]
async fn refresh_requests_and_patches_match_go() {
    for name in [
        "refresh-rotates",
        "refresh-ai-keeps-refresh",
        "refresh-rejected",
        "refresh-500",
    ] {
        let fx = fixture("kimi", name);
        let mock = Mock::start(&fx["responses"]).await;
        let mut cred = credential("kimi", &fx, None);
        cred.provider = fx["credential"]["type"].as_str().unwrap().into();
        let exec = executor().with_oauth_host(&mock.url);
        assert!(
            exec.needs_prepare(&cred, &cfg()),
            "{name}: no expiry and no last_refresh means refresh"
        );
        let before = chrono::Utc::now().timestamp();
        let result = exec.prepare(&cred, &cfg()).await;
        let go = Captured::from_fixture(&fx["upstream"][0]);
        let rust = &mock.captured()[0];
        let mut mask = vec!["Host", "X-Msh-Device-Name", "X-Msh-Device-Model"];
        if fx["credential"]["device_id"].is_null() {
            mask.push("X-Msh-Device-Id");
        }
        assert_same_request(name, &go, rust, &mask);
        assert_eq!(rust.header("Host"), Some(mock.url.trim_start_matches("http://")));
        match fx["extra"]["metadata"].as_object() {
            None => {
                let error = result.expect_err(name);
                let go_message = fx["downstream"]["err_body"].as_str().unwrap();
                // Go appends the response body after "status N: "; it is withheld here.
                let message = String::from_utf8_lossy(&error.body).into_owned();
                assert!(
                    go_message.starts_with(&message),
                    "{name}: {message:?} vs {go_message:?}"
                );
                assert_eq!(error.status, fx["responses"][0]["status"].as_u64().unwrap() as u16);
            }
            Some(go_meta) => {
                let mut meta = cred.metadata.clone();
                result.unwrap().apply(&mut meta);
                let sorted =
                    |m: &serde_json::Map<String, Value>| m.keys().cloned().collect::<std::collections::BTreeSet<_>>();
                assert_eq!(sorted(&meta), sorted(go_meta), "{name}: keys");
                for (key, value) in go_meta {
                    match key.as_str() {
                        "last_refresh" => assert!(crate::kimi_http::parse_time(&meta[key]).is_some()),
                        "expired" => {
                            let expired = crate::kimi_http::parse_time(&meta[key]).unwrap().timestamp();
                            assert!(
                                (before + 3600..=before + 3605).contains(&expired),
                                "{name}: expired {expired}"
                            );
                            let go_before = fx["extra"]["before_unix"].as_i64().unwrap();
                            let go_expired = crate::kimi_http::parse_time(value).unwrap().timestamp();
                            assert!((go_before + 3600..=go_before + 3605).contains(&go_expired));
                        }
                        _ => assert_eq!(&meta[key], value, "{name}: {key}"),
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn concurrent_refreshes_share_one_exchange() {
    // Go: TestRefreshToken_DeduplicatesConcurrentRefreshAcrossInstances.
    let responses = serde_json::json!([{"status":200,"headers":[["Content-Type","application/json"]],
        "body":"{\"access_token\":\"once\",\"refresh_token\":\"r2\",\"expires_in\":3600}"}]);
    let mock = Mock::start(&responses).await;
    let flows: Vec<_> = (0..4)
        .map(|_| DeviceFlow::new(wreq::Client::new(), "kimi.com", "d").with_oauth_host(&mock.url))
        .collect();
    let results = futures_util::future::join_all(flows.iter().map(|f| f.refresh("shared-refresh"))).await;
    assert!(results.iter().all(|r| r.as_ref().unwrap().access_token == "once"));
    assert_eq!(mock.captured().len(), 1);
}

#[tokio::test]
async fn login_writes_go_identical_credential_file() {
    for (name, provider) in [("login-kimi", "kimi"), ("login-kimi-ai", "kimi-ai")] {
        let fx = fixture("kimi", name);
        let mock = Mock::start(&fx["responses"]).await;
        let dir = std::env::temp_dir().join(format!("kimi-login-{}", uuid::Uuid::new_v4()));
        let domain = if provider == "kimi" { "kimi.com" } else { "kimi.ai" };
        let flow = DeviceFlow::new(wreq::Client::new(), domain, "")
            .with_oauth_host(&mock.url)
            .with_min_interval(std::time::Duration::from_millis(1));
        let path = kimi_auth::login_with(flow, provider, &dir, true, None).await.unwrap();
        let captured = mock.captured();
        let go: Vec<Captured> = fx["upstream"]
            .as_array()
            .unwrap()
            .iter()
            .map(Captured::from_fixture)
            .collect();
        assert_eq!(
            captured.len(),
            go.len(),
            "{name}: device code, pending poll, success poll"
        );
        for (rust, go) in captured.iter().zip(&go) {
            assert_same_request(
                name,
                go,
                rust,
                &["Host", "X-Msh-Device-Name", "X-Msh-Device-Model", "X-Msh-Device-Id"],
            );
        }
        let device_ids: std::collections::HashSet<_> =
            captured.iter().map(|c| c.header("X-Msh-Device-Id").unwrap()).collect();
        assert_eq!(device_ids.len(), 1, "one device identity per login");

        let written = std::fs::read_to_string(&path).unwrap();
        let go_file = fx["extra"]["file"].as_object().unwrap();
        // The encoder reproduces Go's exact bytes for Go's own map.
        assert_eq!(
            kimi_auth::encode_credential(go_file),
            fx["extra"]["file_raw"].as_str().unwrap()
        );
        let ours: serde_json::Map<String, Value> = serde_json::from_str(&written).unwrap();
        assert_eq!(ours.keys().collect::<Vec<_>>(), go_file.keys().collect::<Vec<_>>());
        for (key, value) in go_file {
            match key.as_str() {
                "device_id" => assert_eq!(ours[key].as_str(), captured[0].header("X-Msh-Device-Id")),
                "timestamp" | "expired" => {}
                _ => assert_eq!(&ours[key], value, "{name}: {key}"),
            }
        }
        let timestamp = ours["timestamp"].as_i64().unwrap();
        assert_eq!(
            path.file_name().unwrap().to_str().unwrap(),
            format!("{provider}-{timestamp}.json")
        );
        let expired = crate::kimi_http::parse_time(&ours["expired"]).unwrap().timestamp();
        assert!((timestamp / 1000 + 3599..=timestamp / 1000 + 3601).contains(&expired));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
