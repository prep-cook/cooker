//! Tool calls and their results as Gemini `functionCall` and `functionResponse` parts.
use super::super::{member, members, request_schema::answered};
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
        return function_result(doc, part, run, e);
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
/// Writes a Chat tool message or a canonical tool result as `"functionResponse":{...}`: the name
/// of the call it answers, its id and its text as `response.result`.
pub(super) fn function_result(
    doc: &Document,
    message: usize,
    run: Option<usize>,
    e: &mut Encoder<'_, '_>,
) -> Result<()> {
    let id = doc.required(message, "tool_call_id")?;
    let call = answered(doc, run, id)?.ok_or(Error::Schema)?;
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
