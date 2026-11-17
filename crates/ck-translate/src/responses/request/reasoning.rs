use super::super::{chat_effort, is};
use crate::codec::{
    Error, Result,
    document::{Document, Value},
    encode::Encoder,
};

/// Writes `,"reasoning":{...}` from the thinking options. Another protocol's budget, switch and
/// native config are not Responses members; its `include` is a summary setting as a string,
/// and Gemini's `includeThoughts: true` asks for `auto` summaries unless thinking is off. A
/// Chat `reasoning` object, which the fence limits to these two, gives its `effort` and
/// `summary`. Thinking switched off with no effort given is `effort: "none"`, the Responses
/// form of the disable.
pub(super) fn write_reasoning_options(
    doc: &Document,
    x: usize,
    thinking: usize,
    e: &mut Encoder<'_, '_>,
) -> Result<()> {
    let native = is(doc, x, "original_protocol", "responses")?;
    let off = match doc.member(thinking, "enabled")? {
        Some(n) => matches!(doc.node(n)?.value, Value::Bool(false)),
        None => false,
    };
    let mut open = false;
    let mut effort = false;
    for f in doc.children(thinking)? {
        let name = doc.node(f)?.name.ok_or(Error::Schema)?;
        let include = doc.name_is(name, "include")?;
        let summary = include && matches!(doc.node(f)?.value, Value::String(_) | Value::Text(_));
        let auto = include && !native && !off && matches!(doc.node(f)?.value, Value::Bool(true));
        if !native && !summary && !auto && !doc.name_is(name, "effort")? {
            continue;
        }
        effort |= !include && doc.name_is(name, "effort")?;
        key(&mut open, e)?;
        if include {
            e.string("summary")?;
        } else {
            doc.write_name(name, e)?;
        }
        e.bytes(b":")?;
        if auto {
            e.string("auto")?;
        } else {
            doc.write(f, e)?;
        }
    }
    if let Some(chat) = chat_effort(doc, x, thinking)? {
        key(&mut open, e)?;
        e.bytes(b"\"effort\":")?;
        doc.write(chat, e)?;
        effort = true;
    }
    if off && !effort {
        key(&mut open, e)?;
        e.bytes(b"\"effort\":\"none\"")?;
    }
    if is(doc, x, "original_protocol", "chat")?
        && let Some(object) = doc.member(thinking, "native")?
        && let Some(summary) = doc.member(object, "summary")?
    {
        key(&mut open, e)?;
        e.bytes(b"\"summary\":")?;
        doc.write(summary, e)?;
    }
    if native && !open {
        e.bytes(b",\"reasoning\":{}")
    } else if open {
        e.bytes(b"}")
    } else {
        Ok(())
    }
}
/// Opens the `reasoning` object before its first member, else separates the next one.
fn key(open: &mut bool, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(if *open { b"," } else { b",\"reasoning\":{" })?;
    *open = true;
    Ok(())
}
