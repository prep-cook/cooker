//! Upstream WebSocket error frames and URLs, against Go's answers
//! (`tests/fixtures/codex_ws_errors.json`, `tests/reference/codex/zz_rsfix_ws_errors_test.go`).
//! The transport's turns are covered by `tests/exec_codex/socket.rs`.

use super::*;
use serde_json::Value;

#[test]
fn error_frames_match_go() {
    let cases: Vec<Value> =
        serde_json::from_str(include_str!("../tests/fixtures/codex_ws_errors.json")).expect("fixture");
    assert_eq!(cases.len(), 15);
    for case in &cases {
        let payload = case["payload"].as_str().unwrap();
        let cooling = case["model_level_cooling"].as_bool().unwrap_or(false);
        let got = ws_error(payload, cooling);
        if !case["matched"].as_bool().unwrap() {
            assert!(got.is_none(), "{payload}");
            continue;
        }
        let got = got.unwrap_or_else(|| panic!("{payload}: not classified"));
        assert_eq!(u64::from(got.status), case["status"].as_u64().unwrap(), "{payload}");
        assert_eq!(
            String::from_utf8_lossy(&got.body),
            case["message"].as_str().unwrap(),
            "{payload}"
        );
        assert_eq!(
            got.retry_after.map(|d| d.as_millis() as i64),
            case["retry_after_ms"].as_i64(),
            "{payload}"
        );
        let credential_scoped = case["credential_scoped"].as_bool().unwrap_or(false);
        assert_eq!(
            got.scope == FailureScope::Credential && response::is_usage_limit(&String::from_utf8_lossy(&got.body)),
            credential_scoped,
            "{payload}: credential-wide usage limit"
        );
        let mut headers: Vec<(String, String)> = got
            .headers
            .iter()
            .map(|(k, v)| (k.as_str().to_owned(), v.to_str().unwrap().to_owned()))
            .collect();
        headers.sort();
        let mut want: Vec<(String, String)> = case["headers"]
            .as_object()
            .into_iter()
            .flatten()
            .map(|(k, v)| (k.to_ascii_lowercase(), v.as_str().unwrap().to_owned()))
            .collect();
        want.sort();
        assert_eq!(headers, want, "{payload}");
    }
}

/// `buildCodexResponsesWebsocketURL` answers, recorded from Go v8.0.35 with the inputs of `url.Parse` and
/// `URL.String()` (tests/reference/go_url).
#[test]
fn ws_url_follows_go() {
    let cases: Value = serde_json::from_str(include_str!("../tests/fixtures/go_url_string.json")).unwrap();
    for case in cases.as_array().unwrap() {
        let input = case["in"].as_str().unwrap();
        let want = &case["codex_ws"];
        match ws_url(input) {
            Ok(got) => assert_eq!(Some(got.as_str()), want["out"].as_str(), "{input}"),
            Err(e) => {
                assert_eq!(
                    String::from_utf8_lossy(&e.body),
                    want["err"].as_str().unwrap(),
                    "{input}"
                );
                // A plain Go error before dialing: no status, no retry round, and the
                // model cools for the credential.
                assert_eq!((e.status, e.scope), (0, ck_core::exec::FailureScope::Model), "{input}");
            }
        }
    }
}
