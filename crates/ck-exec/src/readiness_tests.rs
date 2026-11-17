use super::{Executors, Readiness, claude};
use ck_core::config::Config;
use ck_core::credential::Credential;
use std::path::Path;

fn credential(metadata: serde_json::Value) -> Credential {
    Credential::from_file(
        Path::new("/auth"),
        Path::new("/auth/a.json"),
        metadata.as_object().unwrap().clone(),
    )
    .unwrap()
}

fn rfc3339(offset: chrono::Duration) -> String {
    (chrono::Utc::now() + offset).to_rfc3339()
}

/// Requests block only on Go `ShouldPrepareRequestAuth` (Claude: an OAuth token
/// without a canonical device pool or account UUID). Expiry, even past expiry, is a
/// background refresh.
#[test]
fn only_missing_identity_blocks_requests() {
    let executors = Executors {
        claude: claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
        codex: Default::default(),
        devices: Default::default(),
        openai: Default::default(),
        google: Default::default(),
    };
    let cfg = Config::default();
    let pool = serde_json::json!(["a".repeat(64)]);
    let identified = |expired: String| {
        credential(serde_json::json!({
            "type": "claude",
            "access_token": "sk-ant-oat01-fake",
            "refresh_token": "fake-refresh",
            "claude_device_ids": pool,
            "account_uuid": "fake-account",
            "expired": expired,
        }))
    };
    let cases = [
        (identified(rfc3339(chrono::Duration::days(2))), Readiness::Ready),
        (identified(rfc3339(chrono::Duration::hours(1))), Readiness::RefreshSoon),
        (identified(rfc3339(-chrono::Duration::hours(1))), Readiness::RefreshSoon),
        (
            credential(serde_json::json!({
                "type": "claude",
                "access_token": "sk-ant-oat01-fake",
                "refresh_token": "fake-refresh",
                "expired": rfc3339(-chrono::Duration::hours(1)),
            })),
            Readiness::PrepareNow,
        ),
        (
            credential(serde_json::json!({"type": "claude", "access_token": "sk-ant-api03-fake"})),
            Readiness::Ready,
        ),
        (
            credential(serde_json::json!({
                "type": "kimi",
                "access_token": "fake-access",
                "refresh_token": "fake-refresh",
                "expired": rfc3339(-chrono::Duration::hours(1)),
            })),
            Readiness::RefreshSoon,
        ),
    ];
    let antigravity = antigravity_cases();
    for (i, (c, expected)) in cases.iter().chain(&antigravity).enumerate() {
        assert_eq!(executors.readiness(c, &cfg), *expected, "case {i}");
    }
}

fn antigravity_cases() -> [(Credential, Readiness); 7] {
    // Antigravity requests wait for a project ID and a token valid five more minutes;
    // otherwise the background refresh starts thirty minutes before expiry.
    [
        (
            credential(serde_json::json!({
                "type": "antigravity",
                "access_token": "fake-access",
                "refresh_token": "fake-refresh",
                "expired": rfc3339(chrono::Duration::hours(5)),
            })),
            Readiness::PrepareNow,
        ),
        (
            credential(serde_json::json!({
                "type": "antigravity",
                "project_id": "proj-1",
                "access_token": "fake-access",
                "refresh_token": "fake-refresh",
                "expired": rfc3339(chrono::Duration::minutes(20)),
            })),
            Readiness::RefreshSoon,
        ),
        (
            credential(serde_json::json!({
                "type": "antigravity",
                "project_id": "proj-1",
                "access_token": "fake-access",
                "refresh_token": "fake-refresh",
                "expired": rfc3339(chrono::Duration::minutes(6)),
            })),
            Readiness::RefreshSoon,
        ),
        (
            credential(serde_json::json!({
                "type": "antigravity",
                "project_id": "proj-1",
                "access_token": "fake-access",
                "refresh_token": "fake-refresh",
                "expired": rfc3339(chrono::Duration::minutes(4)),
            })),
            Readiness::PrepareNow,
        ),
        (
            credential(serde_json::json!({
                "type": "antigravity",
                "project_id": "proj-1",
                "refresh_token": "fake-refresh",
                "expired": rfc3339(chrono::Duration::minutes(40)),
            })),
            Readiness::PrepareNow,
        ),
        (
            credential(serde_json::json!({
                "type": "antigravity",
                "project_id": "proj-1",
                "access_token": "fake-access",
            })),
            Readiness::PrepareNow,
        ),
        (
            credential(serde_json::json!({
                "type": "antigravity",
                "project_id": "proj-1",
                "access_token": "fake-access",
                "refresh_token": "fake-refresh",
                "expired": rfc3339(chrono::Duration::minutes(40)),
            })),
            Readiness::Ready,
        ),
    ]
}

/// The refresh loop sleeps until `prepare_due`: each provider's own lead before
/// expiry (Claude 4 h, Kimi 5 min), to the second, `now` once due, and nothing past
/// the horizon.
#[test]
fn prepare_due_finds_each_providers_deadline() {
    let executors = Executors {
        claude: claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
        codex: Default::default(),
        devices: Default::default(),
        openai: Default::default(),
        google: Default::default(),
    };
    let cfg = Config::default();
    let now = chrono::Utc::now();
    let horizon = chrono::Duration::hours(1);
    let with_expiry = |provider: &str, expiry: chrono::DateTime<chrono::Utc>| {
        credential(serde_json::json!({
            "type": provider,
            "access_token": "fake-access",
            "refresh_token": "fake-refresh",
            "expired": expiry.to_rfc3339(),
        }))
    };
    let near = |got: Option<chrono::DateTime<chrono::Utc>>, want: chrono::DateTime<chrono::Utc>| {
        let got = got.expect("due inside the horizon");
        assert!((got - want).num_milliseconds().abs() <= 1000, "{got} vs {want}");
    };
    let offset = chrono::Duration::seconds(1337);
    let claude = with_expiry("claude", now + chrono::Duration::hours(4) + offset);
    near(executors.prepare_due(&claude, &cfg, now, horizon), now + offset);
    let kimi = with_expiry("kimi", now + chrono::Duration::minutes(5) + offset);
    near(executors.prepare_due(&kimi, &cfg, now, horizon), now + offset);
    let due = with_expiry("claude", now - chrono::Duration::hours(1));
    assert_eq!(executors.prepare_due(&due, &cfg, now, horizon), Some(now));
    let later = with_expiry("claude", now + chrono::Duration::hours(6));
    assert_eq!(executors.prepare_due(&later, &cfg, now, horizon), None);
    let api_key = credential(serde_json::json!({"type": "claude", "access_token": "sk-ant-api03-fake"}));
    assert_eq!(executors.prepare_due(&api_key, &cfg, now, horizon), None);
}

/// Go `authHasRefreshCredential`.
#[test]
fn refresh_credentials_follow_go() {
    let executors = Executors {
        claude: claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
        codex: Default::default(),
        openai: Default::default(),
        google: Default::default(),
        devices: Default::default(),
    };
    let has = |m: serde_json::Value| executors.has_refresh_credential(&credential(m));
    assert!(has(serde_json::json!({"type": "claude", "refresh_token": "fake"})));
    assert!(has(serde_json::json!({"type": "kimi", "refreshToken": "fake"})));
    assert!(!has(serde_json::json!({"type": "claude", "refresh_token": "  "})));
    // A Sign in with ChatGPT partition needs its refresh token and client ID.
    assert!(has(
        serde_json::json!({"type": "codex", "siwc": {"refresh_token": "fake", "client_id": "c"}})
    ));
    assert!(!has(
        serde_json::json!({"type": "codex", "siwc": {"refresh_token": "fake"}})
    ));
    assert!(
        !has(serde_json::json!({"type": "claude", "siwc": {"refresh_token": "fake", "client_id": "c"}})),
        "SIWC is Codex's"
    );
    assert!(has(serde_json::json!({"type": "meta", "dca_token": "fake"})));
    assert!(
        !has(serde_json::json!({"type": "claude", "dca_token": "fake"})),
        "dca_token is Meta's"
    );
    let mut meta = credential(serde_json::json!({"type": "meta"}));
    assert!(!executors.has_refresh_credential(&meta));
    meta.attributes.insert("dca_token".into(), "fake".into());
    assert!(executors.has_refresh_credential(&meta));
}
