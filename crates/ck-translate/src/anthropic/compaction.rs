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

/// Refuses a compaction block that does not lead its message, which Anthropic refuses as
/// misplaced. Nothing else needs refusing here: the Responses request ingress, the only one
/// with a compact operation, refuses a streamed compact, and Anthropic egress writes only
/// Anthropic members, so another protocol's `context_management` never reaches a compact.
pub(super) fn validate(view: RequestView<'_>) -> Result<()> {
    let doc = view.document;
    for message in doc.children(doc.required(doc.root(), "messages")?)? {
        if let Some(content) = doc.member(message, "content")?
            && matches!(doc.node(content)?.value, Value::Array)
        {
            for (index, part) in doc.children(content)?.enumerate() {
                if index != 0 && block(doc, part)? {
                    return Err(Error::Unsupported);
                }
            }
        }
    }
    Ok(())
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
