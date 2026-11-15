//! The heap the thinking pass spends on a large request: an applier edits the body it is
//! given, so an unchanged request costs nothing in proportion to the body and a changed one
//! at most one output body.

use ck_core::registry::ThinkingSupport;

use super::{ModelCaps, RequestThinking, apply_request_thinking};
use crate::alloc_count::bytes as allocated;

/// About 1 MiB of message text.
fn body(fields: &str) -> Vec<u8> {
    // An encoded upload has no spare capacity.
    let mut body = format!(r#"{{"model":"m","input":"{}"{fields}}}"#, "x".repeat(1 << 20)).into_bytes();
    body.shrink_to_fit();
    body
}

/// `body` through the executor entry for `provider` with model `model`, and the bytes the
/// pass allocated.
fn think(provider: &str, model: &str, body: Vec<u8>) -> (Vec<u8>, usize) {
    let caps = ModelCaps {
        id: "m".into(),
        kind: "openai".into(),
        thinking: Some(ThinkingSupport {
            levels: ["low", "medium", "high"].map(String::from).to_vec(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let request = RequestThinking {
        payload: None,
        original: None,
        model,
        from: provider,
        to: provider,
        provider,
        resolved: Some(Some(&caps)),
        has_request_transformer: false,
        updates_changed: false,
    };
    let before = allocated();
    let out = apply_request_thinking(&request, body).unwrap();
    (out, allocated() - before)
}

#[test]
fn an_unchanged_effort_allocates_nothing_of_the_body() {
    let input = body(r#","reasoning":{"effort":"high"}"#);
    let (out, spent) = think("codex", "m(high)", input.clone());
    assert_eq!(out, input);
    assert!(
        spent < 64 << 10,
        "{spent} bytes allocated for a {}-byte body",
        input.len()
    );
}

#[test]
fn an_added_effort_makes_one_body() {
    let input = body("");
    let (out, spent) = think("codex", "m(high)", input.clone());
    assert_eq!(
        out,
        [&input[..input.len() - 1], br#","reasoning":{"effort":"high"}}"#].concat()
    );
    assert!(
        spent < input.len() + (64 << 10),
        "{spent} bytes allocated for a {}-byte body",
        input.len()
    );
}

/// Kimi's applier makes three edits (drop `reasoning_effort`, set the thinking type and
/// effort), still into one body.
#[test]
fn several_edits_make_one_body() {
    let input = body(r#","reasoning_effort":"low""#);
    let (out, spent) = think("kimi", "m(high)", input.clone());
    let kept = input.len() - r#","reasoning_effort":"low"}"#.len();
    assert_eq!(
        out,
        [&input[..kept], br#","thinking":{"type":"enabled","effort":"high"}}"#].concat()
    );
    assert!(
        spent < input.len() + (64 << 10),
        "{spent} bytes allocated for a {}-byte body",
        input.len()
    );
}
