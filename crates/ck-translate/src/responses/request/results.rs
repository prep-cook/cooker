//! Tool result parts as Responses `function_call_output` items.
use super::super::super::is;
use super::{separate, write_content};
use crate::codec::chat::request::gemini::write_outcome;
use crate::codec::{RequestView, Result, document::Value, encode::Encoder};

/// Writes each tool result part of array `content` as a `function_call_output` item ahead of the
/// turn's other items; returns whether there was one. A single text part is the output string,
/// other content a list of input parts, and a Gemini `result` its JSON text.
pub(super) fn write_results(
    view: RequestView<'_>,
    content: usize,
    first: &mut bool,
    e: &mut Encoder<'_, '_>,
) -> Result<bool> {
    let doc = view.document;
    if !matches!(doc.node(content)?.value, Value::Array) {
        return Ok(false);
    }
    let mut any = false;
    for part in doc.children(content)? {
        if !is(doc, part, "type", "tool_result")? {
            continue;
        }
        any = true;
        separate(first, e)?;
        e.bytes(b"{\"type\":\"function_call_output\",\"call_id\":")?;
        doc.write(doc.required(part, "tool_call_id")?, e)?;
        if !write_outcome(doc, part, "output", e)? {
            e.bytes(b",\"output\":")?;
            let output = doc.member(part, "content")?;
            let mut parts = output.map(|c| doc.children(c)).transpose()?.into_iter().flatten();
            match (parts.next(), parts.next(), output) {
                (Some(text), None, _) if is(doc, text, "type", "text")? => doc.write(doc.required(text, "text")?, e)?,
                (Some(_), _, Some(output)) => write_content(view, output, false, e)?,
                _ => e.bytes(b"\"\"")?,
            }
        }
        e.bytes(b"}")?;
    }
    Ok(any)
}
