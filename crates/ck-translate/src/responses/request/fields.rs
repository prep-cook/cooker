//! Canonical root fields under their Responses names and shapes.
use super::super::super::{is, member};
use super::originals;
use crate::codec::chat::request::{gemini, json_schema};
use crate::codec::{
    Error, Result,
    document::{Document, Value},
    encode::Encoder,
};

/// Writes a canonical root field under its Responses name and shape, after a comma.
pub(super) fn write_field(doc: &Document, field: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let name = doc.node(field)?.name.ok_or(Error::Schema)?;
    if doc.name_is(name, "messages")? || doc.name_is(name, "stream")? || doc.name_is(name, "x_ck")? {
        return Ok(());
    }
    // Chat's `max_tokens` is the older spelling of the same limit; `max_completion_tokens` wins.
    if doc.name_is(name, "max_tokens")? && doc.member(doc.root(), "max_completion_tokens")?.is_some() {
        return Ok(());
    }
    e.bytes(b",")?;
    write_member(doc, field, e)
}

/// Writes the request members a Responses `Response` repeats (`tools`, `tool_choice` and
/// `parallel_tool_calls`) as one object, in their Responses shapes; members the request lacks
/// are absent.
pub(in crate::responses) fn write_echo(doc: &Document, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"{")?;
    let mut first = true;
    for field in doc.children(doc.root())? {
        let name = doc.node(field)?.name.ok_or(Error::Schema)?;
        let mut echoed = false;
        for repeated in ["tools", "tool_choice", "parallel_tool_calls"] {
            echoed |= doc.name_is(name, repeated)?;
        }
        if echoed {
            e.bytes(if first { b"" } else { b"," })?;
            first = false;
            write_member(doc, field, e)?;
        }
    }
    e.bytes(b"}")
}

/// Writes `"name":value` for one canonical root field under its Responses name and shape.
fn write_member(doc: &Document, field: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let name = doc.node(field)?.name.ok_or(Error::Schema)?;
    if doc.name_is(name, "max_tokens")? || doc.name_is(name, "max_completion_tokens")? {
        e.string("max_output_tokens")?;
    } else if doc.name_is(name, "response_format")? {
        e.string("text")?;
    } else {
        doc.write_name(name, e)?;
    }
    e.bytes(b":")?;
    if doc.name_is(name, "response_format")? {
        e.bytes(b"{\"format\":")?;
        write_format(doc, field, e)?;
        if let Some(text) = provider_text(doc)? {
            for f in doc.children(text)? {
                e.bytes(b",")?;
                doc.write_name(doc.node(f)?.name.ok_or(Error::Schema)?, e)?;
                e.bytes(b":")?;
                doc.write(f, e)?;
            }
        }
        e.bytes(b"}")?;
    } else if doc.name_is(name, "tools")? {
        write_tools(doc, field, e)?;
    } else if doc.name_is(name, "tool_choice")?
        && matches!(doc.node(field)?.value, Value::Object)
        && is(doc, field, "type", "function")?
    {
        // Responses spells the function name flat; every other member, `type` first, stays.
        let mut first = true;
        for f in doc.children(field)? {
            let n = doc.node(f)?.name.ok_or(Error::Schema)?;
            if !doc.name_is(n, "function")? {
                e.bytes(if first { b"{" } else { b"," })?;
                first = false;
                doc.write_name(n, e)?;
                e.bytes(b":")?;
                doc.write(f, e)?;
            }
        }
        member(doc, doc.required(field, "function")?, "name", e)?;
        e.bytes(b"}")?;
    } else {
        doc.write(field, e)?;
    }
    Ok(())
}
/// The `text` options other than `format` that ingress kept as Responses provider parameters.
fn provider_text(doc: &Document) -> Result<Option<usize>> {
    let Some(x) = doc.member(doc.root(), "x_ck")? else {
        return Ok(None);
    };
    let Some(providers) = doc.member(x, "provider_parameters")? else {
        return Ok(None);
    };
    match doc.member(providers, "responses")? {
        Some(p) => doc.member(p, "text"),
        None => Ok(None),
    }
}
/// Writes a canonical response format; Responses spells `json_schema` members flat. Gemini JSON
/// mode is a `json_schema` format with a fixed name, or `json_object` without a schema.
fn write_format(doc: &Document, format: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    if json_schema::from_gemini(doc)? {
        let Some(schema) = gemini::json_mode(doc, format)? else {
            return e.bytes(b"{\"type\":\"json_object\"}");
        };
        e.bytes(b"{\"type\":\"json_schema\",\"name\":\"response\",\"schema\":")?;
        gemini::write_schema(doc, schema, e)?;
        return e.bytes(b"}");
    }
    let schema = match doc.node(format)?.value {
        Value::Object => doc.member(format, "json_schema")?,
        _ => None,
    };
    let Some(schema) = schema.filter(|&s| matches!(doc.node(s).map(|n| n.value), Ok(Value::Object))) else {
        return doc.write(format, e);
    };
    e.bytes(b"{")?;
    let fields = doc.children(format)?.filter(|&f| f != schema);
    for (i, f) in fields.chain(doc.children(schema)?).enumerate() {
        if i > 0 {
            e.bytes(b",")?;
        }
        doc.write_name(doc.node(f)?.name.ok_or(Error::Schema)?, e)?;
        e.bytes(b":")?;
        doc.write(f, e)?;
    }
    e.bytes(b"}")
}
/// Writes the function tools and the native tool declarations kept in `x_ck.tools`; a
/// declaration whose pointer names its position returns there, others follow the functions.
fn write_tools(doc: &Document, field: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let native = match doc.member(doc.root(), "x_ck")? {
        Some(x) => doc.member(x, "tools")?,
        None => None,
    };
    let mut natives = native
        .map(|n| doc.children(n))
        .transpose()?
        .into_iter()
        .flatten()
        .peekable();
    let mut functions = doc.children(field)?.peekable();
    let gemini = json_schema::from_gemini(doc)?;
    e.bytes(b"[")?;
    for position in 0u64.. {
        let at = match natives.peek() {
            Some(&entry) => functions.peek().is_none() || position_of(doc, entry)?.is_some_and(|p| p <= position),
            None => false,
        };
        if position > 0 && (at || functions.peek().is_some()) {
            e.bytes(b",")?;
        }
        if at && let Some(entry) = natives.next() {
            doc.write(doc.required(entry, "declaration")?, e)?;
        } else if let Some(tool) = functions.next() {
            e.bytes(b"{\"type\":\"function\"")?;
            let function = doc.required(tool, "function")?;
            if gemini {
                // A Gemini declaration's schemas become JSON Schema `parameters` and `output_schema`.
                e.bytes(b",")?;
                json_schema::write_function(doc, function, ("parameters", Some("output_schema")), e)?;
            } else {
                for f in doc.children(function)? {
                    e.bytes(b",")?;
                    doc.write_name(doc.node(f)?.name.ok_or(Error::Schema)?, e)?;
                    e.bytes(b":")?;
                    doc.write(f, e)?;
                }
            }
            originals(doc, tool, e, &["type", "function"])?;
            e.bytes(b"}")?;
        } else {
            break;
        }
    }
    e.bytes(b"]")
}
/// The position a `/tools/N` declaration pointer names; `None` for any other pointer.
fn position_of(doc: &Document, entry: usize) -> Result<Option<u64>> {
    let mut chars = doc.chars(doc.required(entry, "pointer")?)?;
    for expected in "/tools/".chars() {
        if chars.next_scalar()? != Some(expected) {
            return Ok(None);
        }
    }
    let mut position = None::<u64>;
    while let Some(c) = chars.next_scalar()? {
        let digit = u64::from(c.to_digit(10).ok_or(Error::Schema)?);
        let value = position.unwrap_or(0).checked_mul(10).and_then(|p| p.checked_add(digit));
        position = Some(value.ok_or(Error::Schema)?);
    }
    Ok(position)
}
