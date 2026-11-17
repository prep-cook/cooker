//! Kimi parity tests. Expected values come from the Go fixtures (see kimi_fixture) or
//! from Go's own test tables, never from this implementation.

use super::*;
use crate::kimi_fixture::{
    Captured, Mock, assert_same_request, credential, data_payloads, downstream, fixture, request,
};
use crate::kimi_http::{go_arch, go_os, hostname};
use serde_json::Value;

fn claude() -> ClaudeExecutor {
    ClaudeExecutor::with_client(wreq::Client::new(), crate::claude::DEFAULT_BASE_URL)
}

fn executor() -> KimiExecutor {
    KimiExecutor::with_client(wreq::Client::new())
}

fn cfg() -> Config {
    Config::parse("").unwrap()
}

/// Headers whose values depend on the machine or are random per run.
fn masked(fixture: &serde_json::Value) -> Vec<&'static str> {
    let mut masked = vec!["X-Msh-Device-Name", "X-Msh-Device-Model", "Referer"];
    if fixture["attributes"]["header:Host"].is_null() {
        masked.push("Host");
    }
    if fixture["credential"]["device_id"].as_str().is_none_or(str::is_empty) {
        masked.push("X-Msh-Device-Id");
    }
    masked
}

async fn run(name: &str) -> (serde_json::Value, Vec<Captured>, crate::kimi_fixture::Downstream) {
    let fx = fixture("kimi", name);
    let mock = Mock::start(&fx["responses"]).await;
    let cred = credential("kimi", &fx, Some(("base_url", format!("{}/coding", mock.url))));
    let exec = KimiExecutor::with_client(default_client());
    let cfg = Config::parse(fx["request"]["config"].as_str().unwrap_or_default()).unwrap();
    let result = exec.execute(&claude(), &cred, request(&fx, ""), &cfg).await;
    let down = downstream(result).await;
    (fx, mock.captured(), down)
}

// These fixtures pin removed temperature surgery, effort dialects or the native-tool bridge.
fn obsolete_upload(name: &str) -> bool {
    name.starts_with("responses-apply-patch-")
        || matches!(
            name,
            "chat-nonstream-normalize"
                | "chat-stream-suffix"
                | "chat-error-429-clamped-none"
                | "chat-payload-rules"
                | "responses-nonstream-reorder-suffix"
                | "responses-stream-clamp"
                | "responses-payload-override-after-normalize"
        )
}

#[path = "kimi/tests/auth.rs"]
mod auth;
#[path = "kimi/tests/normalize.rs"]
mod normalize;
#[path = "kimi/tests/replay.rs"]
mod replay;
#[path = "kimi/tests/streams.rs"]
mod streams;
#[path = "kimi/tests/wire.rs"]
mod wire;
