//! Anthropic native compaction (owner ruling, Q7 reversal; D2 plan §2.7).
//!
//! On-demand compaction is a Messages request with `"compaction":{"type":"summarize"}`; the
//! answer is one signed `compaction` block. The canonical document carries that block as an
//! opaque Anthropic part, which only the Anthropic request egress writes and every other
//! destination refuses (owner ruling Q3). Replaying it unchanged is the only accepted form: an
//! edited block fails upstream with `compaction_content_mismatch`.
use super::is;
use crate::codec::{
    Error, Operation, RequestView, Result,
    document::{Document, Value},
    encode::Encoder,
};

/// The message index of the last compaction block the request carries: replay sends that
/// message first and leaves out every earlier conversation message.
pub(super) fn replay_from(doc: &Document) -> Result<Option<u64>> {
    let mut found = None;
    for (index, message) in doc.children(doc.required(doc.root(), "messages")?)?.enumerate() {
        if let Some(content) = doc.member(message, "content")?
            && matches!(doc.node(content)?.value, Value::Array)
        {
            for part in doc.children(content)? {
                if block(doc, part)? {
                    found = Some(index as u64);
                }
            }
        }
    }
    Ok(found)
}

/// Whether the fenced request asks Anthropic for compaction or carries a compaction block; both
/// need the compaction beta, and both run on the execution contract (`ck-exec`, Claude).
pub fn requested(view: RequestView<'_>) -> Result<bool> {
    Ok(view.operation == Operation::Compact || replay_from(view.document)?.is_some())
}

/// Refuses a compaction block Anthropic would not read as one: a block behind another part or
/// behind a tool call the client placed first, which the egress writes back ahead of the
/// content, both refused upstream as misplaced; and a block in the system prompt, which the
/// encoder would send as system text. Another protocol's `context_management` never reaches a
/// compact because the egress writes only Anthropic members; the Claude executor's cloak adds
/// none, and it refuses one a payload rule or the caller pairs with `compaction` (`ck-exec`).
pub(super) fn validate(view: RequestView<'_>) -> Result<()> {
    let doc = view.document;
    for message in doc.children(doc.required(doc.root(), "messages")?)? {
        let system = is(doc, message, "role", "system")? || is(doc, message, "role", "developer")?;
        let leads = !system && !call_first(doc, message)?;
        if let Some(content) = doc.member(message, "content")?
            && matches!(doc.node(content)?.value, Value::Array)
        {
            for (index, part) in doc.children(content)?.enumerate() {
                if (index != 0 || !leads) && block(doc, part)? {
                    return Err(Error::Unsupported);
                }
            }
        }
    }
    Ok(())
}

/// Whether the egress writes one of `message`'s tool calls ahead of its content.
fn call_first(doc: &Document, message: usize) -> Result<bool> {
    let Some(calls) = doc.member(message, "tool_calls")? else {
        return Ok(false);
    };
    for call in doc.children(calls)? {
        if super::request_encode::call_position(doc, call)? == Some(0) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Writes the compaction request member of an on-demand compaction.
pub(super) fn write_request(view: RequestView<'_>, e: &mut Encoder<'_, '_>) -> Result<()> {
    if view.operation == Operation::Compact {
        e.bytes(b",\"compaction\":{\"type\":\"summarize\"}")?;
    }
    Ok(())
}

/// Whether `part` is an Anthropic compaction block kept as an opaque Anthropic part.
fn block(doc: &Document, part: usize) -> Result<bool> {
    if !is(doc, part, "type", "opaque")? || !is(doc, part, "protocol", "anthropic")? {
        return Ok(false);
    }
    let value = doc.required(part, "value")?;
    Ok(matches!(doc.node(value)?.value, Value::Object) && is(doc, value, "type", "compaction")?)
}
