//! Go's reasoning replay end to end: each scenario in `tests/fixtures/codex_go.json`
//! (`replay`) ran its Claude turns through one Go `CodexExecutor` against a scripted
//! upstream. The same turns through one Rust executor must send the same upstream bodies.

use std::path::Path;

use bytes::Bytes;
use ck_core::config::Config;
use ck_core::credential::Credential;
use ck_core::exec::{Caller, ExecRequest, Operation, ResponseBody};
use ck_core::format::Format;
use futures_util::StreamExt;
use http::HeaderMap;
use serde_json::Value;

use crate::codex::CodexExecutor;
use crate::codex_oauth::CodexOAuth;
use crate::codex_testkit::{GO, Mock, Reply};

fn request(scenario: &Value, turn: &Value) -> ExecRequest {
    let mut headers = HeaderMap::new();
    for (k, v) in scenario["headers"].as_object().into_iter().flatten() {
        headers.insert(
            http::HeaderName::try_from(k.as_str()).unwrap(),
            v.as_str().unwrap().parse().unwrap(),
        );
    }
    let body = Bytes::from(turn["payload"].as_str().unwrap().to_owned());
    ExecRequest {
        operation: Operation::Generate,
        source_format: Format::Claude,
        response_format: Format::Claude,
        requested_model: scenario["model"].as_str().unwrap().into(),
        model: scenario["model"].as_str().unwrap().into(),
        original_body: body.clone(),
        body,
        stream: turn["stream"].as_bool().unwrap(),
        alt: None,
        session: None,
        headers,
        execution_session: None,
        derived_session: None,
        request_path: String::new(),
        caller: Caller {
            principal: "client-key-FAKE".into(),
            source: "authorization",
            provider: String::new(),
        },
        resolved_model: None,
        usage: Default::default(),
    }
}

#[tokio::test]
async fn replay_scenarios_match_go() {
    let scenarios = GO["replay"].as_array().unwrap();
    assert_eq!(scenarios.len(), 2);
    for scenario in scenarios {
        let name = scenario["name"].as_str().unwrap();
        let mock = Mock::start().await;
        let turns = scenario["turns"].as_array().unwrap();
        mock.script(
            "/responses",
            turns
                .iter()
                .map(|t| {
                    let status = t["upstream_status"].as_u64().unwrap() as u16;
                    let kind = if status == 200 {
                        "text/event-stream"
                    } else {
                        "application/json"
                    };
                    Reply {
                        status,
                        headers: vec![("content-type".into(), kind.into())],
                        body: t["upstream_body"].as_str().unwrap().into(),
                    }
                })
                .collect(),
        );
        let executor = CodexExecutor::with_client(wreq::Client::new(), CodexOAuth::new(wreq::Client::new()));
        let mut credential = Credential::from_file(
            Path::new("/fake"),
            Path::new("/fake/codex-replay.json"),
            serde_json::json!({"type": "codex", "access_token": "at-FAKE", "account_id": "acct-FAKE-1"})
                .as_object()
                .unwrap()
                .clone(),
        )
        .unwrap();
        credential.attributes.insert("base_url".into(), mock.url.clone());
        let cfg = Config::default();
        for (i, turn) in turns.iter().enumerate() {
            let result = executor.execute(&credential, request(scenario, turn), &cfg).await;
            let error = match result {
                Err(error) => Some(error),
                Ok(response) => match response.body {
                    ResponseBody::Stream(mut stream) => {
                        let mut failed = None;
                        while let Some(item) = stream.next().await {
                            if let Err(error) = item {
                                failed = Some(error);
                            }
                        }
                        failed
                    }
                    ResponseBody::Buffered(_) => None,
                },
            };
            let captured = mock.take();
            assert_eq!(captured.len(), 1, "{name} turn {i}: one upstream request");
            assert_eq!(
                String::from_utf8_lossy(&captured[0].body),
                turn["upstream"].as_str().unwrap(),
                "{name} turn {i}: upstream body"
            );
            match (&error, turn["error"].as_object()) {
                (Some(error), Some(go)) => {
                    assert_eq!(error.status as u64, go["status"].as_u64().unwrap(), "{name} turn {i}")
                }
                (None, None) => {}
                (rust, go) => panic!("{name} turn {i}: rust error {rust:?}, go error {go:?}"),
            }
        }
    }
}

/// Go's limits: an entry keeps at most 256 turns, dropping the oldest.
#[test]
fn entries_keep_the_newest_turns() {
    let cache = super::Cache::default();
    let call = |i: usize| format!(r#"{{"type":"function_call","call_id":"c{i}","name":"f","arguments":"{{}}"}}"#);
    for i in 0..300 {
        let marker = format!(r#"{{"type":"cpa_codex_replay_turn","id":"t{i}"}}"#);
        assert!(cache.append("m", "s", &[marker.into_bytes(), call(i).into_bytes()]));
    }
    // A repeated turn id is not stored twice.
    let again = br#"{"type":"cpa_codex_replay_turn","id":"t299"}"#.to_vec();
    cache.append("m", "s", &[again, call(299).into_bytes()]);
    let items = cache.get("m", "s").unwrap();
    assert_eq!(items.len(), 512);
    assert_eq!(items[0], br#"{"type":"cpa_codex_replay_turn","id":"t44"}"#);
    assert!(cache.get("m", "other").is_none());
}

/// One session id keys a different replay entry per caller and per upstream credential, so
/// a turn cached for one never reaches another.
#[test]
fn replay_entries_belong_to_their_caller_and_credential() {
    let scenario = serde_json::json!({"model": "gpt-5", "headers": {"x-claude-code-session-id": "cc-1"}});
    let turn = serde_json::json!({"payload": r#"{"model":"gpt-5","messages":[]}"#, "stream": true});
    let owner = request(&scenario, &turn);
    let mut other = request(&scenario, &turn);
    other.caller.principal = "client-key-OTHER".into();
    let body = r#"{"model":"gpt-5","input":[]}"#;
    let scope = |req: &ExecRequest, credential: &str| super::scope(req, credential, body.as_bytes());
    let cache = super::Cache::default();
    let mine = scope(&owner, "codex-a");
    let call = br#"{"type":"function_call","call_id":"c","name":"f","arguments":"{}"}"#;
    let marker = br#"{"type":"cpa_codex_replay_turn","id":"t"}"#;
    assert!(cache.append(&mine.model, &mine.session, &[marker.to_vec(), call.to_vec()]));
    assert_eq!(scope(&owner, "codex-a").session, mine.session);
    for theirs in [scope(&other, "codex-a"), scope(&owner, "codex-b")] {
        assert!(!theirs.session.is_empty());
        assert!(
            cache.get(&theirs.model, &theirs.session).is_none(),
            "{:?}",
            theirs.session
        );
    }
}

/// Owners are the exact identities: credential ids that differ only by whitespace, or
/// principals holding the separator a joined encoding would use, never share replay.
#[test]
fn replay_owners_are_exact_identities() {
    use crate::codex_shape::replay_owner;
    assert_ne!(replay_owner("k", "account.json"), replay_owner("k", " account.json"));
    assert_ne!(replay_owner("k ", "c"), replay_owner("k", "c"));
    assert_ne!(replay_owner("a\0b", "c"), replay_owner("a", "b\0c"));
    assert_ne!(replay_owner("a:b", "c"), replay_owner("a", "b:c"));
    assert_ne!(replay_owner("ab", "c"), replay_owner("a", "bc"));
    assert_eq!(replay_owner("a\0b", "c"), replay_owner("a\0b", "c"));
}
