//! Reasoning replay of a completion the Responses ingress accepted: its output items
//! normalized (`normalizeXAIReasoningReplayItems`) straight from the ingress's parse into the
//! heap buffers the replay store keeps. An item has one representation, its normalized
//! bytes, charged to the turn's reservation before it is allocated; the only other buffer is
//! the decoded `encrypted_content` a reasoning item is checked with, charged the same way and
//! freed before its charge is returned. The rules are `xai_replay::normalize_item`'s for raw
//! items on the old route, except that a call id, name or status must be a string.

use ck_translate::codec::document::{Document, Value};
use ck_translate::codec::encode::{self, Encoder};
use ck_translate::codec::{self, Error};
use ck_wire::gostr::GoStr;
use ck_wire::signature::{MAX_GROK_ENCRYPTED_CONTENT_LEN, inspect_grok_encrypted_content};

use crate::xai_exec::object_member;
use crate::xai_replay::{self as replay, ReplayScope, Reservation, Store};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Reasoning,
    Message,
    FunctionCall,
    CustomToolCall,
}

const KINDS: [(&str, Kind); 4] = [
    ("reasoning", Kind::Reasoning),
    ("message", Kind::Message),
    ("function_call", Kind::FunctionCall),
    ("custom_tool_call", Kind::CustomToolCall),
];

/// Caches the replay items of completed `response` for `scope`. A turn replay does not keep
/// clears the session; the response is unaffected either way.
pub(crate) fn cache_completion(store: &Store, scope: &ReplayScope, doc: &Document, response: usize) {
    let mut held = store.reserve();
    match replay_items(doc, response, &mut held) {
        // `held` already bounds the items by an entry, so the store takes them.
        Ok(Some(items)) => _ = store.put(scope, items, held),
        _ => {
            drop(held);
            replay::clear(store, scope);
        }
    }
}

/// The normalized replay items of completed `response`; `None` when replay keeps nothing of
/// the turn: no output array, no item that can anchor a replay, or more than an entry or the
/// cache may take. Nothing is allocated for a turn without an anchor.
fn replay_items(doc: &Document, response: usize, held: &mut Reservation<'_>) -> codec::Result<Option<Vec<Vec<u8>>>> {
    let Some(output) = object_member(doc, response, "output")? else {
        return Ok(None);
    };
    if !matches!(doc.node(output)?.value, Value::Array) {
        return Ok(None);
    }
    let mut kept = Vec::new();
    for item in doc.children(output)? {
        let Some(kind) = kind(doc, item)? else {
            continue;
        };
        match valid(doc, item, kind, held)? {
            Some(true) => kept.push((item, kind)),
            Some(false) => {}
            None => return Ok(None),
        }
    }
    if kept.iter().all(|(_, kind)| *kind == Kind::Message) {
        return Ok(None);
    }
    let mut items = Vec::with_capacity(kept.len());
    for (item, kind) in kept {
        match encode::owned(|e| write(doc, item, kind, e), |len| held.charge(len))? {
            Some(bytes) => items.push(bytes),
            None => return Ok(None),
        }
    }
    Ok(Some(items))
}

fn kind(doc: &Document, item: usize) -> codec::Result<Option<Kind>> {
    let Some(kind) = object_member(doc, item, "type")? else {
        return Ok(None);
    };
    for (name, value) in KINDS {
        if string(doc, kind)? && doc.string_is(kind, name)? {
            return Ok(Some(value));
        }
    }
    Ok(None)
}

/// Whether `item` normalizes to a replay item; `None` when checking it would outgrow the
/// reservation.
fn valid(doc: &Document, item: usize, kind: Kind, held: &mut Reservation<'_>) -> codec::Result<Option<bool>> {
    let member = |name| object_member(doc, item, name);
    let named = |name| -> codec::Result<bool> { Ok(trimmed(doc, member(name)?)?.is_some_and(|(_, keep)| keep > 0)) };
    Ok(Some(match kind {
        Kind::Reasoning => return encrypted(doc, member("encrypted_content")?, held),
        Kind::Message => {
            let content = member("content")?;
            trimmed_is(doc, member("role")?, "assistant", true)?
                && match content {
                    Some(content) if matches!(doc.node(content)?.value, Value::Array) => part_count(doc, content)? > 0,
                    _ => false,
                }
        }
        Kind::FunctionCall => named("call_id")? && named("name")? && string_member(doc, member("arguments")?)?,
        Kind::CustomToolCall => named("call_id")? && named("name")? && member("input")?.is_some(),
    }))
}

/// Whether `value` is Grok encrypted content. Its decoded copy, and what checking it decodes
/// (base64, at most three quarters of it), are charged as twice its length.
fn encrypted(doc: &Document, value: Option<usize>, held: &mut Reservation<'_>) -> codec::Result<Option<bool>> {
    let Some(value) = value.filter(|v| string(doc, *v).unwrap_or(false)) else {
        return Ok(Some(false));
    };
    let mut chars = doc.chars(value)?;
    let mut len = 0;
    while let Some(c) = chars.next_scalar()? {
        len += c.len_utf8();
        if len > MAX_GROK_ENCRYPTED_CONTENT_LEN {
            return Ok(Some(false));
        }
    }
    let check = || -> codec::Result<bool> {
        let mut text = String::with_capacity(len);
        let mut chars = doc.chars(value)?;
        while let Some(c) = chars.next_scalar()? {
            text.push(c);
        }
        Ok(inspect_grok_encrypted_content(&text).is_ok())
    };
    held.scratch(2 * len, check).transpose()
}

/// The output-text and refusal parts message `content` keeps.
fn part_count(doc: &Document, content: usize) -> codec::Result<usize> {
    let mut count = 0;
    for part in doc.children(content)? {
        count += usize::from(part_field(doc, part)?.is_some());
    }
    Ok(count)
}

/// The part type and the string member a kept message part carries.
fn part_field(doc: &Document, part: usize) -> codec::Result<Option<(&'static str, usize)>> {
    let kind = object_member(doc, part, "type")?;
    for (name, field) in [("output_text", "text"), ("refusal", "refusal")] {
        if trimmed_is(doc, kind, name, false)? {
            let value = object_member(doc, part, field)?;
            return Ok(value.filter(|v| string(doc, *v).unwrap_or(false)).map(|v| (name, v)));
        }
    }
    Ok(None)
}

/// Writes the normalized form of `item`, which [`valid`] accepted.
fn write(doc: &Document, item: usize, kind: Kind, e: &mut Encoder<'_, '_>) -> codec::Result<()> {
    let member = |name| object_member(doc, item, name)?.ok_or(Error::Schema);
    match kind {
        Kind::Reasoning => {
            e.bytes(br#"{"type":"reasoning","summary":[],"content":null,"encrypted_content":"#)?;
            doc.write(member("encrypted_content")?, e)?;
        }
        Kind::Message => {
            e.bytes(br#"{"type":"message","role":"assistant","content":["#)?;
            let mut first = true;
            for part in doc.children(member("content")?)? {
                let Some((name, value)) = part_field(doc, part)? else {
                    continue;
                };
                e.bytes(if first { b"{\"type\":" } else { b",{\"type\":" })?;
                first = false;
                e.string(name)?;
                e.bytes(if name == "refusal" {
                    b",\"refusal\":"
                } else {
                    b",\"text\":"
                })?;
                doc.write(value, e)?;
                e.bytes(b"}")?;
            }
            e.bytes(b"]")?;
        }
        Kind::FunctionCall => {
            e.bytes(br#"{"type":"function_call","call_id":"#)?;
            write_trimmed(doc, member("call_id")?, e)?;
            e.bytes(br#","name":"#)?;
            write_trimmed(doc, member("name")?, e)?;
            e.bytes(br#","arguments":"#)?;
            doc.write(member("arguments")?, e)?;
        }
        Kind::CustomToolCall => {
            e.bytes(br#"{"type":"custom_tool_call","status":"#)?;
            match object_member(doc, item, "status")? {
                Some(status) if trimmed(doc, Some(status))?.is_some_and(|(_, keep)| keep > 0) => {
                    write_trimmed(doc, status, e)?;
                }
                _ => e.bytes(br#""completed""#)?,
            }
            e.bytes(br#","call_id":"#)?;
            write_trimmed(doc, member("call_id")?, e)?;
            e.bytes(br#","name":"#)?;
            write_trimmed(doc, member("name")?, e)?;
            e.bytes(br#","input":"#)?;
            doc.write(member("input")?, e)?;
        }
    }
    e.bytes(b"}")
}

fn string(doc: &Document, node: usize) -> codec::Result<bool> {
    Ok(matches!(doc.node(node)?.value, Value::String(_) | Value::Text(_)))
}

fn string_member(doc: &Document, node: Option<usize>) -> codec::Result<bool> {
    node.map_or(Ok(false), |n| string(doc, n))
}

/// For a string, the white space it starts with and the characters from there to its last
/// non-white one, in characters: `strings.TrimSpace` without a copy.
fn trimmed(doc: &Document, node: Option<usize>) -> codec::Result<Option<(usize, usize)>> {
    let Some(node) = node.filter(|n| string(doc, *n).unwrap_or(false)) else {
        return Ok(None);
    };
    let (mut chars, mut at, mut first, mut end) = (doc.chars(node)?, 0, None, 0);
    while let Some(c) = chars.next_scalar()? {
        at += 1;
        if !c.is_whitespace() {
            first.get_or_insert(at - 1);
            end = at;
        }
    }
    Ok(Some(first.map_or((0, 0), |first| (first, end - first))))
}

/// Whether the trimmed string `node` is `want`, or equal to it under Go's case folding when
/// `fold`. Both map one rune to one rune, so only a string of `want`'s length is read.
fn trimmed_is(doc: &Document, node: Option<usize>, want: &str, fold: bool) -> codec::Result<bool> {
    let Some((skip, keep)) = trimmed(doc, node)? else {
        return Ok(false);
    };
    if keep != want.chars().count() {
        return Ok(false);
    }
    let mut chars = doc.chars(node.ok_or(Error::Schema)?)?;
    let mut text = String::new();
    for at in 0..skip + keep {
        let c = chars.next_scalar()?.ok_or(Error::Schema)?;
        if at >= skip {
            text.push(c);
        }
    }
    Ok(if fold { text.go_eq_fold(want) } else { text == want })
}

/// Writes string `node` trimmed of white space, as a JSON string.
fn write_trimmed(doc: &Document, node: usize, e: &mut Encoder<'_, '_>) -> codec::Result<()> {
    let (skip, keep) = trimmed(doc, Some(node))?.ok_or(Error::Schema)?;
    let mut chars = doc.chars(node)?;
    e.bytes(b"\"")?;
    for at in 0..skip + keep {
        let c = chars.next_scalar()?.ok_or(Error::Schema)?;
        if at >= skip {
            e.escaped(c.encode_utf8(&mut [0; 4]))?;
        }
    }
    e.bytes(b"\"")
}

#[cfg(test)]
#[path = "xai_replay_items_tests.rs"]
mod tests;
