//! The reasoning replay key of the execution contract's xAI route (`xai_exec`).
//!
//! The old path joins a trimmed session id into a `kind:value` string under a 64-bit hash
//! of the caller (`xaiReasoningReplayIsolateSessionKey`), so `" k1 "` reads `"k1"`'s turns
//! and the Claude pair `("a:agent:b", "c")` reads `("a", "b:agent:c")`'s. Here every
//! component is the exact value it was chosen from, behind its length, under the full
//! SHA-256 of the caller and the credential, so two distinct sessions never share a key.
//! A value whose trimmed form is empty still counts as absent, as in Go's selection order.

use ck_core::format::Format;
use ck_wire::json::{self as gj, Res};
use http::HeaderMap;
use sha2::{Digest, Sha256};

use crate::xai_shape::Inbound;

/// The session of one request: its source kind and exact values.
struct Session<'a> {
    kind: &'static str,
    values: Vec<std::borrow::Cow<'a, [u8]>>,
}

impl<'a> Session<'a> {
    fn of(kind: &'static str, values: Vec<std::borrow::Cow<'a, [u8]>>) -> Self {
        Self { kind, values }
    }
}

/// The replay key of `inbound`, whose upstream body is `body`; empty when the request has
/// no session, or no client key and a session the client controls.
pub(crate) fn key(inbound: &Inbound<'_>, body: &[u8]) -> String {
    let Some(session) = session(inbound, body) else {
        return String::new();
    };
    // An execution session is the gateway's own, never a client-controlled key.
    if inbound.principal.is_empty() && session.kind != "execution" {
        return String::new();
    }
    let mut owner = Vec::new();
    for part in [inbound.principal, inbound.credential.unwrap_or_default()] {
        framed(&mut owner, part.as_bytes());
    }
    let mut encoded = Sha256::digest(&owner).to_vec();
    framed(&mut encoded, session.kind.as_bytes());
    for value in &session.values {
        framed(&mut encoded, value);
    }
    encoded.iter().map(|b| format!("{b:02x}")).collect()
}

/// Appends `part` behind its length.
fn framed(out: &mut Vec<u8>, part: &[u8]) {
    out.extend_from_slice(&(part.len() as u64).to_be_bytes());
    out.extend_from_slice(part);
}

/// `codexReasoningReplaySessionKey`'s order with exact values: the Claude Code session and
/// agent, the execution session, the prompt-cache or window id of the upstream body then
/// the client body, then the client's session headers.
fn session<'a>(inbound: &Inbound<'a>, body: &'a [u8]) -> Option<Session<'a>> {
    if inbound.source == Format::Claude
        && let Some(session) = claude_code(inbound.payload, inbound.headers)
    {
        return Some(session);
    }
    if let Some(execution) = inbound.execution_session.filter(|s| !s.trim().is_empty()) {
        return Some(Session::of("execution", vec![execution.as_bytes().into()]));
    }
    payload(body)
        .or_else(|| payload(inbound.payload))
        .or_else(|| headers(inbound.headers))
}

/// Whether a value counts as present: Go selects on `strings.TrimSpace`.
fn present(value: &[u8]) -> bool {
    !ck_wire::gostr::trim_space(value).is_empty()
}

/// The first header value of `name` that is present, exactly as sent.
fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a [u8]> {
    headers
        .get_all(name)
        .iter()
        .filter(|v| v.to_str().is_ok())
        .map(|v| v.as_bytes())
        .find(|v| present(v))
}

/// The value at `path` of `json` when present (`Result.String()`, exact).
fn member<'a>(json: &'a [u8], path: &str) -> Option<std::borrow::Cow<'a, [u8]>> {
    let value: Res<'a> = gj::get(json, path);
    let bytes = value.bytes();
    present(&bytes).then_some(bytes)
}

/// `claudeCodeSession` with the session and agent as two values.
fn claude_code<'a>(payload: &'a [u8], headers: &'a HeaderMap) -> Option<Session<'a>> {
    let session = match header(headers, "X-Claude-Code-Session-Id") {
        Some(session) => session.to_vec(),
        None => user_session(&gj::get(payload, "metadata.user_id").bytes())?,
    };
    let agent = header(headers, "X-Claude-Code-Agent-Id").unwrap_or(b"main");
    Some(Session::of("claude", vec![session.into(), agent.into()]))
}

/// The session a Claude Code `metadata.user_id` names: its `_session_` tail of lowercase
/// hex and dashes, else the `session_id` of a JSON user id.
fn user_session(user: &[u8]) -> Option<Vec<u8>> {
    let tail = user
        .windows(b"_session_".len())
        .rposition(|w| w == b"_session_")
        .map(|pos| &user[pos + b"_session_".len()..])
        .filter(|tail| {
            !tail.is_empty()
                && tail
                    .iter()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase() || *b == b'-')
        });
    if let Some(tail) = tail {
        return Some(tail.to_vec());
    }
    if user.first() == Some(&b'{') {
        return member(user, "session_id").map(|v| v.into_owned());
    }
    None
}

/// The prompt-cache or window id a body carries, directly or in its Codex turn metadata.
fn payload(body: &[u8]) -> Option<Session<'_>> {
    if body.is_empty() {
        return None;
    }
    if let Some(cache) = member(body, "prompt_cache_key") {
        return Some(Session::of("prompt-cache", vec![cache]));
    }
    if let Some(window) = member(body, "client_metadata.x-codex-window-id") {
        return Some(Session::of("window", vec![window]));
    }
    turn(&member(body, "client_metadata.x-codex-turn-metadata")?)
}

/// The prompt-cache or window id of Codex turn metadata, copied out of it.
fn turn(metadata: &[u8]) -> Option<Session<'static>> {
    if let Some(cache) = member(metadata, "prompt_cache_key") {
        return Some(Session::of("prompt-cache", vec![cache.into_owned().into()]));
    }
    let window = member(metadata, "window_id")?;
    Some(Session::of("window", vec![window.into_owned().into()]))
}

/// The session the client's headers carry.
fn headers(headers: &HeaderMap) -> Option<Session<'_>> {
    if let Some(session) = header(headers, "X-Codex-Turn-Metadata").and_then(turn) {
        return Some(session);
    }
    let named = [
        ("X-Codex-Window-Id", "window"),
        ("session_id", "session-id"),
        ("session-id", "session-id"),
        ("conversation_id", "conversation_id"),
    ];
    named.into_iter().find_map(|(name, kind)| {
        let value = header(headers, name)?;
        Some(Session::of(kind, vec![value.into()]))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key_of(source: Format, body: &str, headers: &[(&str, &str)], (principal, credential): (&str, &str)) -> String {
        let mut map = HeaderMap::new();
        for (name, value) in headers {
            map.insert(
                http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                value.parse().unwrap(),
            );
        }
        let inbound = Inbound {
            source,
            payload_format: Format::OpenAIResponse,
            model: "grok-4.3",
            payload: body.as_bytes(),
            original: body.as_bytes(),
            headers: &map,
            resolved: None,
            execution_session: None,
            derived_session: None,
            principal,
            credential: Some(credential),
            downstream_websocket: false,
        };
        key(&inbound, body.as_bytes())
    }

    /// Distinct sessions and owners never share a key: padding, a Claude session and agent
    /// pair whose join is the same, or a caller and credential whose join is the same.
    #[test]
    fn keys_are_injective() {
        let owner = ("sk-a", "xai-1");
        let cache = |key: &str| {
            key_of(
                Format::OpenAIResponse,
                &format!(r#"{{"prompt_cache_key":"{key}"}}"#),
                &[],
                owner,
            )
        };
        assert!(!cache("k1").is_empty());
        assert_ne!(cache("k1"), cache(" k1 "));
        let claude = |session: &str, agent: &str| {
            let headers = [("X-Claude-Code-Session-Id", session), ("X-Claude-Code-Agent-Id", agent)];
            key_of(Format::Claude, "{}", &headers, owner)
        };
        assert_ne!(claude("a:agent:b", "c"), claude("a", "b:agent:c"));
        let body = r#"{"prompt_cache_key":"k1"}"#;
        let owned = |owner| key_of(Format::OpenAIResponse, body, &[], owner);
        assert_ne!(owned(("a:b", "c")), owned(("a", "b:c")));
        assert_ne!(owned(("ab", "c")), owned(("a", "bc")));
        assert_ne!(owned(("k", "c")), owned(("k ", "c")));
        assert!(
            owned(("", "c")).is_empty(),
            "a client-controlled session needs a client key"
        );
    }
}
