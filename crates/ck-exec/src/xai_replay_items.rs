//! Reasoning replay of a completion the Responses ingress accepted: its output items
//! normalized (`normalizeXAIReasoningReplayItems`) straight from the ingress's parse into the
//! heap buffers the replay store keeps. An item has one representation, its normalized
//! bytes, charged to the turn's reservation before it is allocated; the only other buffer is
//! the decoded `encrypted_content` a reasoning item is checked with, charged the same way and
//! freed before its charge is returned. The rules are `xai_replay::normalize_item`'s for raw
//! items on the old route, which reads a call's id, name and status as Go's `Result.String()`
//! does; see [`Text`].

use ck_translate::codec::document::{Document, Name, Text as Sent, Value};
use ck_translate::codec::encode::{self, Encoder};
use ck_translate::codec::source::Span;
use ck_translate::codec::{self, Error};
use ck_wire::gostr::GoStr;
use ck_wire::json as gj;
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

/// A call's `call_id`, `name` or `status` as the old route's `text()` reads it: a string
/// without its surrounding white space, a non-integer number in Go's float format, null or
/// absence as nothing, and any other value as its JSON text.
enum Text {
    Empty,
    /// A string node, the white space it starts with and the characters kept after it.
    Trimmed(usize, usize, usize),
    /// A value whose JSON text is its text: an integer, a boolean, an object or an array.
    Json(usize),
    /// An object or array, as the JSON text the upstream sent.
    Raw(Sent),
    /// A non-integer number, formatted.
    Formatted(String),
}

/// A call's identity members.
struct Ids {
    call_id: Text,
    name: Text,
    status: Text,
}

/// What normalization makes of one item.
enum Check {
    Kept(Option<Ids>),
    Dropped,
    /// Checking the item would outgrow the reservation.
    Refused,
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
            Check::Kept(ids) => kept.push((item, kind, ids)),
            Check::Dropped => {}
            Check::Refused => return Ok(None),
        }
    }
    if kept.iter().all(|(_, kind, _)| *kind == Kind::Message) {
        return Ok(None);
    }
    let mut items = Vec::with_capacity(kept.len());
    for (item, kind, ids) in &kept {
        match encode::owned(|e| write(doc, *item, *kind, ids.as_ref(), e), |len| held.charge(len))? {
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

/// What normalization makes of `item`.
fn valid(doc: &Document, item: usize, kind: Kind, held: &mut Reservation<'_>) -> codec::Result<Check> {
    let member = |name| object_member(doc, item, name);
    let kept = match kind {
        Kind::Reasoning => match encrypted(doc, member("encrypted_content")?, held)? {
            Some(kept) => kept,
            None => return Ok(Check::Refused),
        },
        Kind::Message => {
            let content = member("content")?;
            trimmed_is(doc, member("role")?, "assistant", true)?
                && match content {
                    Some(content) if matches!(doc.node(content)?.value, Value::Array) => part_count(doc, content)? > 0,
                    _ => false,
                }
        }
        Kind::FunctionCall | Kind::CustomToolCall => return call(doc, item, kind, held),
    };
    Ok(if kept { Check::Kept(None) } else { Check::Dropped })
}

/// A call is kept with a non-empty id and name, and string arguments or any input.
fn call(doc: &Document, item: usize, kind: Kind, held: &mut Reservation<'_>) -> codec::Result<Check> {
    let member = |name| object_member(doc, item, name);
    let mut read = |name| -> codec::Result<Option<Text>> { text(doc, member(name)?, held) };
    let (Some(call_id), Some(name)) = (read("call_id")?, read("name")?) else {
        return Ok(Check::Refused);
    };
    let (status, body) = match kind {
        Kind::CustomToolCall => (read("status")?, member("input")?.is_some()),
        _ => (Some(Text::Empty), string_member(doc, member("arguments")?)?),
    };
    let Some(status) = status else {
        return Ok(Check::Refused);
    };
    if matches!(call_id, Text::Empty) || matches!(name, Text::Empty) || !body {
        return Ok(Check::Dropped);
    }
    Ok(Check::Kept(Some(Ids { call_id, name, status })))
}

/// `text(member)` for `node`; `None` when reading a number would outgrow the reservation.
fn text(doc: &Document, node: Option<usize>, held: &mut Reservation<'_>) -> codec::Result<Option<Text>> {
    let Some(node) = node else {
        return Ok(Some(Text::Empty));
    };
    Ok(Some(match doc.node(node)?.value {
        Value::String(_) | Value::Text(_) => match trimmed(doc, Some(node))? {
            Some((skip, keep)) if keep > 0 => Text::Trimmed(node, skip, keep),
            _ => Text::Empty,
        },
        Value::Null => Text::Empty,
        Value::Number(_) => return number(doc, node, held),
        Value::Object | Value::Array => Text::Raw(sent(doc, node)?),
        _ => Text::Json(node),
    }))
}

/// The JSON text of member `node`, an object or array, as the upstream sent it. The parse
/// keeps no span for a container, so its source is read from the member's name up to the
/// bracket that closes the value.
fn sent(doc: &Document, node: usize) -> codec::Result<Sent> {
    let Some(Name::Mapped(name, _)) = doc.node(node)?.name else {
        return Err(Error::Schema);
    };
    let source = doc.source(name)?;
    let (mut at, mut start, mut end) = (name.span.end, None, None);
    let (mut depth, mut quoted, mut escaped) = (0u32, false, false);
    let rest = Span {
        start: name.span.end,
        end: source.len(),
    };
    let scan = source.visit(rest, |chunk| {
        for &byte in chunk {
            at += 1;
            if start.is_none() {
                match byte {
                    b'{' | b'[' => (start, depth) = (Some(at - 1), 1),
                    b':' | b' ' | b'\t' | b'\n' | b'\r' => {}
                    _ => return Err(Error::Schema),
                }
            } else if quoted {
                if escaped {
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                } else if byte == b'"' {
                    quoted = false;
                }
            } else {
                match byte {
                    b'"' => quoted = true,
                    b'{' | b'[' => depth += 1,
                    b'}' | b']' => depth -= 1,
                    _ => {}
                }
                if depth == 0 {
                    end = Some(at);
                    // Stops the visit; the value has ended.
                    return Err(Error::Closed);
                }
            }
        }
        Ok(())
    });
    match (scan, start, end) {
        (Err(Error::Closed), Some(start), Some(end)) => Ok(Sent {
            source: name.source,
            span: Span { start, end },
        }),
        (Err(error), _, _) if !matches!(error, Error::Closed) => Err(error),
        _ => Err(Error::Schema),
    }
}

/// Go's `'f'` format of an `f64` holds at most this many bytes at once: its result is at most
/// 343 bytes, built from a digit string and a run of zeros.
const FORMATTING: usize = 2048;

/// A number as `Result.String()` reads it: an integer as written, any other number in Go's
/// float format. Reading it takes a copy of its JSON text and room to format it, charged
/// before either is allocated; a formatted text stays charged until the reservation ends,
/// after the text is freed.
fn number(doc: &Document, node: usize, held: &mut Reservation<'_>) -> codec::Result<Option<Text>> {
    let json = |e: &mut Encoder<'_, '_>| doc.write(node, e);
    let mut len = 0;
    encode::owned(json, |n| {
        len = n;
        false
    })?;
    let read = || -> codec::Result<Text> {
        let raw = encode::owned(json, |_| true)?.ok_or(Error::Schema)?;
        let text = gj::parse(&raw).bytes();
        Ok(if *text == *raw {
            Text::Json(node)
        } else {
            Text::Formatted(String::from_utf8_lossy(&text).into_owned())
        })
    };
    let charge = len + FORMATTING;
    if !held.charge(charge) {
        return Ok(None);
    }
    let text = read();
    let kept = match &text {
        Ok(Text::Formatted(text)) => text.capacity().min(charge),
        _ => 0,
    };
    held.release(charge - kept);
    text.map(Some)
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
fn write(doc: &Document, item: usize, kind: Kind, ids: Option<&Ids>, e: &mut Encoder<'_, '_>) -> codec::Result<()> {
    let member = |name| object_member(doc, item, name)?.ok_or(Error::Schema);
    let ids = || ids.ok_or(Error::Schema);
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
            write_text(doc, &ids()?.call_id, e)?;
            e.bytes(br#","name":"#)?;
            write_text(doc, &ids()?.name, e)?;
            e.bytes(br#","arguments":"#)?;
            doc.write(member("arguments")?, e)?;
        }
        Kind::CustomToolCall => {
            e.bytes(br#"{"type":"custom_tool_call","status":"#)?;
            match &ids()?.status {
                Text::Empty => e.bytes(br#""completed""#)?,
                status => write_text(doc, status, e)?,
            }
            e.bytes(br#","call_id":"#)?;
            write_text(doc, &ids()?.call_id, e)?;
            e.bytes(br#","name":"#)?;
            write_text(doc, &ids()?.name, e)?;
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

/// Writes `text` as a JSON string.
fn write_text(doc: &Document, text: &Text, e: &mut Encoder<'_, '_>) -> codec::Result<()> {
    let (node, skip, keep) = match text {
        Text::Empty => return e.bytes(b"\"\""),
        Text::Json(node) => return e.quoted_json(|e| doc.write(*node, e)),
        Text::Raw(sent) => return e.quoted_json(|e| e.span(doc.source(*sent)?, sent.span)),
        Text::Formatted(text) => return e.string(text),
        Text::Trimmed(node, skip, keep) => (*node, *skip, *keep),
    };
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
