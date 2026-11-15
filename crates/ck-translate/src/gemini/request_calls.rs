//! Tool calls and their results as Gemini `functionCall` and `functionResponse` parts.
use super::super::{
    is, member, members,
    request_schema::{answered, next_in_turn},
};
use crate::codec::{
    Error, Result,
    document::{Document, Value},
    encode::Encoder,
};

/// Writes `"functionCall":{...}`; a synthesized call id has no native field.
pub(super) fn function_call(doc: &Document, part: usize, synthetic_id: bool, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"\"functionCall\":{\"name\":")?;
    let function = doc.required(part, "function")?;
    doc.write(doc.required(function, "name")?, e)?;
    if !synthetic_id {
        member(doc, part, "id", e)?;
    }
    if let Some(args) = doc.member(function, "arguments")? {
        e.bytes(b",\"args\":")?;
        doc.write(args, e)?;
    }
    members(doc, function, &["name", "arguments"], e)?;
    e.bytes(b"}")
}
/// Writes `"functionResponse":{...}`; a linked id the source omitted stays omitted. A tool result
/// from another protocol has no `result` and answers a call of turn `run` like a Chat tool message.
pub(super) fn function_response(
    doc: &Document,
    part: usize,
    run: Option<usize>,
    synthetic_id: bool,
    e: &mut Encoder<'_, '_>,
) -> Result<()> {
    let Some(result) = doc.member(part, "result")? else {
        let call = answered(doc, run, doc.required(part, "tool_call_id")?)?;
        return function_result(doc, part, call.ok_or(Error::Schema)?, e);
    };
    e.bytes(b"\"functionResponse\":{\"name\":")?;
    doc.write(doc.required(result, "name")?, e)?;
    if !synthetic_id {
        e.bytes(b",\"id\":")?;
        doc.write(doc.required(part, "tool_call_id")?, e)?;
    }
    members(doc, result, &["name"], e)?;
    e.bytes(b"}")
}
/// Writes the Chat tool messages from `first` on as `functionResponse` parts in the order of the
/// calls of model turn `run` they answer, since Gemini before 3 pairs responses with calls by
/// position. The fence matched each to exactly one call by id, so the pairing stays the client's.
pub(super) fn function_results(
    doc: &Document,
    first: usize,
    run: Option<usize>,
    e: &mut Encoder<'_, '_>,
) -> Result<()> {
    let (mut written, mut message) = (0, run);
    while let Some(m) = message {
        for call in doc.children(doc.required(m, "tool_calls")?)? {
            let mut tool = Some(first);
            while let Some(t) = tool
                && is(doc, t, "role", "tool")?
            {
                if doc.equal_strings(doc.required(t, "tool_call_id")?, doc.required(call, "id")?)? {
                    e.bytes(if written == 0 { b"{" } else { b",{" })?;
                    function_result(doc, t, call, e)?;
                    e.bytes(b"}")?;
                    written += 1;
                }
                tool = doc.node(t)?.next;
            }
        }
        message = next_in_turn(doc, m)?;
    }
    // Every message of the group was written once.
    let (mut total, mut tool) = (0, Some(first));
    while let Some(t) = tool
        && is(doc, t, "role", "tool")?
    {
        (total, tool) = (total + 1, doc.node(t)?.next);
    }
    if written == total { Ok(()) } else { Err(Error::Schema) }
}
/// Writes a Chat tool message or a canonical tool result as `"functionResponse":{...}`: the name
/// of `call`, which it answers, its id and its text as `response.result`.
fn function_result(doc: &Document, message: usize, call: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let id = doc.required(message, "tool_call_id")?;
    e.bytes(b"\"functionResponse\":{\"name\":")?;
    doc.write(doc.required(doc.required(call, "function")?, "name")?, e)?;
    e.bytes(b",\"id\":")?;
    doc.write(id, e)?;
    e.bytes(b",\"response\":{\"result\":")?;
    match doc.member(message, "content")? {
        Some(content) if matches!(doc.node(content)?.value, Value::Array) => {
            e.bytes(b"\"")?;
            for part in doc.children(content)? {
                let mut chars = doc.chars(doc.required(part, "text")?)?;
                while let Some(c) = chars.next_scalar()? {
                    e.escaped(c.encode_utf8(&mut [0; 4]))?;
                }
            }
            e.bytes(b"\"")?;
        }
        Some(content) => doc.write(content, e)?,
        None => e.bytes(b"\"\"")?,
    }
    e.bytes(b"}}")
}
