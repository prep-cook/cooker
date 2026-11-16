//! Error frames match Go: every vector in `tests/ws_fixtures/ws_vectors.json` is the output of
//! CLIProxyAPI's own handler helper (`zz_rsfix_ws_test.go`, `TestRSFixWSVectors`). Expected
//! values never come from this crate.

use super::*;
use serde_json::Value;
use std::sync::LazyLock;

static VECTORS: LazyLock<Vec<Value>> = LazyLock::new(|| {
    serde_json::from_str::<Value>(include_str!("../../tests/ws_fixtures/ws_vectors.json"))
        .unwrap()
        .as_array()
        .unwrap()
        .clone()
});

fn s<'a>(v: &'a Value, key: &str) -> &'a str {
    v[key].as_str().unwrap_or_default()
}

#[test]
fn error_payloads_match_go() {
    assert!(!VECTORS.is_empty());
    for v in VECTORS.iter() {
        let status = v["status"].as_u64().unwrap_or(0) as u16;
        assert_eq!(
            error_payload(status, s(v, "raw"), None),
            s(v, "out"),
            "{status} {:?}",
            s(v, "raw")
        );
    }
}
