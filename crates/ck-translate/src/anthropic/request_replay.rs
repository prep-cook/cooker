//! Replay signatures of thinking blocks bound for an Anthropic upstream.
use super::At;
use crate::codec::{Error, Result, document::Document};

/// Whether some replay entry has no binding: an imported document, whose entries are matched
/// by position instead.
pub(super) fn unbound(doc: &Document, x: usize) -> Result<bool> {
    match doc.member(x, "replay")? {
        Some(replay) => doc
            .children(replay)?
            .try_fold(false, |any, entry| Ok::<_, Error>(any || doc.origin(entry)?.is_none())),
        None => Ok(false),
    }
}

/// The replay signature of a reasoning part: its bound entry's, or for an imported document,
/// which carries no bindings, the entry's at the part's position.
pub(super) fn signature(doc: &Document, part: usize, at: At, block_index: u64) -> Result<Option<usize>> {
    let Some(replay) = doc.member(doc.required(doc.root(), "x_ck")?, "replay")? else {
        return Ok(None);
    };
    for entry in doc.origins(part)? {
        if doc.node(entry)?.parent == Some(replay)
            && let Some(signature) = doc.member(entry, "signature")?
        {
            return Ok(Some(signature));
        }
    }
    if at.unbound_replay {
        for entry in doc.children(replay)? {
            if doc.origin(entry)?.is_none()
                && doc.unsigned(doc.required(entry, "message_index")?)? == at.message
                && doc.unsigned(doc.required(entry, "block_index")?)? == block_index
                && let Some(signature) = doc.member(entry, "signature")?
            {
                return Ok(Some(signature));
            }
        }
    }
    Ok(None)
}

/// Whether a part is thinking with an empty signature. Anthropic clients receive such blocks for
/// another upstream's reasoning (owner ruling, O2), and Anthropic refuses them in history, so
/// the encoder leaves them out.
pub(super) fn unsigned(doc: &Document, part: usize, at: At, block_index: u64) -> Result<bool> {
    if !super::super::is(doc, part, "type", "reasoning")? {
        return Ok(false);
    }
    match signature(doc, part, at, block_index)? {
        Some(signature) => doc.string_is(signature, ""),
        None => Ok(false),
    }
}
