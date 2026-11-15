//! The Meta upload shaping that needs no upstream: the payload-rule baseline and the content
//! parts written in place.

use crate::alloc_count::allocated;

/// The upload before thinking is copied only for the payload rules that read it.
#[test]
fn the_baseline_stays_only_for_default_rules() {
    let rules = |yaml: &str| ck_wire::payload::Rules::from_config(&ck_core::config::Config::parse(yaml).unwrap());
    let overrides =
        rules("requests:\n  payload:\n    override:\n      - models: [{name: \"m\"}]\n        params: {a: 1}\n");
    assert_eq!(super::baseline(&overrides, b"{}"), None);
    assert_eq!(super::baseline(&rules(""), b"{}"), None);
    let defaults =
        rules("requests:\n  payload:\n    default:\n      - models: [{name: \"m\"}]\n        params: {a: 1}\n");
    assert_eq!(super::baseline(&defaults, b"{}"), Some(b"{}".to_vec()));
}

/// Every message item's string content becomes one text part; typed parts, other items and
/// other members stay as they were.
#[test]
fn string_contents_become_text_parts() {
    let mut body = br#"{"model":"m","input":[{"type":"message","role":"user","content":"a\"b\u00e9"},{"role":"assistant","content":"hello","x":{"content":"no"}},{"type":"function_call","call_id":"c","content":"keep"},{"type":"message","role":"user","content":[{"type":"input_text","text":"t"}]},{"type":null,"role":"developer","con\u0074ent":"z"},{"role":"user","content":7}],"instructions":"i"}"#.to_vec();
    super::text_parts(&mut body);
    let got: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let want = serde_json::json!({"model": "m", "input": [
        {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "a\"b\u{e9}"}]},
        {"role": "assistant", "content": [{"type": "output_text", "text": "hello"}], "x": {"content": "no"}},
        {"type": "function_call", "call_id": "c", "content": "keep"},
        {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "t"}]},
        {"type": null, "role": "developer", "content": [{"type": "input_text", "text": "z"}]},
        {"role": "user", "content": 7},
    ], "instructions": "i"});
    assert_eq!(got, want);
}

/// The parts are written into the upload itself: a history of string messages allocates only
/// their offsets, not a copy of their text.
#[test]
fn text_parts_copy_no_text() {
    let text = "x".repeat(4_096);
    let messages: Vec<String> = (0..256)
        .map(|i| format!(r#"{{"role":"{}","content":"{text}"}}"#, ["user", "assistant"][i % 2]))
        .collect();
    let mut body = format!(r#"{{"input":[{}]}}"#, messages.join(",")).into_bytes();
    body.reserve(256 * 64);
    let before = allocated();
    super::text_parts(&mut body);
    let spent = allocated() - before;
    assert!(
        spent < 64 * 1024,
        "{spent} bytes allocated for {} bytes of text",
        256 * text.len()
    );
    let got: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(got["input"][255]["content"][0]["type"], "output_text");
    assert_eq!(got["input"][0]["content"][0]["text"], text.as_str());
}

/// The thinking takes the upload itself: an upload it leaves as is comes back in the same
/// allocation, and nothing is copied.
#[test]
fn thinking_hands_the_upload_back() {
    let caps = ck_wire::thinking::ModelCaps {
        id: "muse-spark".into(),
        kind: "openai".into(),
        ..Default::default()
    };
    let text = "x".repeat(200_000);
    let upload = format!(r#"{{"model":"muse-spark","input":[{{"type":"message","role":"user","content":"{text}"}}]}}"#)
        .into_bytes();
    let (address, len) = (upload.as_ptr(), upload.len());
    let before = allocated();
    let body = super::think("muse-spark", Some(&caps), upload).unwrap();
    let spent = allocated() - before;
    assert_eq!((body.as_ptr(), body.len()), (address, len));
    assert!(spent < len / 20, "{spent} bytes allocated for a {len}-byte upload");
}
