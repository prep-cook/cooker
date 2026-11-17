//! Videos handler tests. The routes are checked against the Go server in
//! tests/media_go.rs; these cover what the goldens cannot reach in one process run.

use super::*;

#[test]
fn routing_keeps_preview_alias() {
    assert_eq!(routing("grok/grok-imagine-video-1.5-preview"), XAI_15_PREVIEW);
    assert_eq!(canonical("grok-imagine-video-1.5-preview"), XAI_15_MODEL);
    assert_eq!(routing("sora-2-pro"), XAI_MODEL);
}

#[test]
fn seconds_clamp_like_go() {
    assert_eq!(seconds(""), Ok(4));
    assert_eq!(seconds("+20"), Ok(15));
    assert_eq!(seconds("-3"), Ok(1));
    assert_eq!(seconds("1.5"), Err("seconds must be an integer".into()));
    assert_eq!(seconds("9223372036854775808"), Err("seconds must be an integer".into()));
}
