//! What the cloak's preparation leaves alone on an Anthropic compaction (D2 plan §2.7):
//! Anthropic refuses `compaction` beside `context_management`, and replays a signed
//! `compaction` block only as it sent it, so no cache marker goes into one.
use ck_core::exec::{ExecError, FailureScope};

use super::thinking_accepts_clear;
use crate::rawjson;

/// Whether the cloak may add its `context_management` to `body`: its thinking accepts
/// clearing, and it asks for no compaction.
pub(super) fn accepts_context(body: &str) -> bool {
    thinking_accepts_clear(body) && !rawjson::get(body, "compaction").exists()
}

/// Refuses a prepared body that still pairs `compaction` with `context_management`, which a
/// payload rule or the caller added; the cloak adds none ([`accepts_context`]).
pub(crate) fn refuse_context(body: String) -> Result<String, ExecError> {
    if rawjson::get(&body, "compaction").exists() && rawjson::get(&body, "context_management").exists() {
        return Err(ExecError::local(
            400,
            FailureScope::Request,
            "compaction cannot be combined with context management; remove context_management from the request",
        ));
    }
    Ok(body)
}

/// Whether a block the cloak inserts into a user turn goes behind a block of `kind`: tool
/// results open a turn that answers tool calls, and Anthropic reads a compaction block only as
/// the first content block of the first message.
pub(super) fn stays_first(kind: &str) -> bool {
    matches!(kind, "tool_result" | "compaction")
}
