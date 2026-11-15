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
    // Chat's `logprobs` has no Responses member; the fence passes only its default values.
    if doc.name_is(name, "logprobs")? {
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
    // Native declarations are copied as they are, which is the Responses shape only for a
    // Responses request; another protocol's, such as Anthropic web search, has none.
    if !natives(doc)?.is_empty() && !is(doc, doc.required(doc.root(), "x_ck")?, "original_protocol", "responses")? {
        return Err(Error::Unsupported);
    }
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
    } else if doc.name_is(name, "tool_choice")? && matches!(doc.node(field)?.value, Value::Object) {
        write_choice(doc, field, e)?;
    } else {
        doc.write(field, e)?;
    }
    Ok(())
}
/// Writes a tool choice object in its Responses shape: a named tool is flat, and a nested
/// allowed-tools choice holds `mode` and `tools` beside its type. A Responses request's other
/// choices, such as a hosted tool's, are already in that shape; another protocol's have none.
fn write_choice(doc: &Document, choice: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    if let Some(carrier) = carrier(doc, choice)?
        && doc.member(choice, carrier)?.is_some()
    {
        return write_named(doc, choice, e);
    }
    let allowed = if is(doc, choice, "type", "allowed_tools")? {
        doc.member(choice, "allowed_tools")?
    } else {
        None
    };
    let Some(allowed) = allowed else {
        if is(doc, doc.required(doc.root(), "x_ck")?, "original_protocol", "responses")? {
            return doc.write(choice, e);
        }
        return Err(Error::Unsupported);
    };
    e.bytes(b"{\"type\":\"allowed_tools\"")?;
    for f in doc.children(allowed)? {
        let name = doc.node(f)?.name.ok_or(Error::Schema)?;
        e.bytes(b",")?;
        doc.write_name(name, e)?;
        e.bytes(b":")?;
        if !doc.name_is(name, "tools")? {
            doc.write(f, e)?;
            continue;
        }
        e.bytes(b"[")?;
        for (i, tool) in doc.children(f)?.enumerate() {
            e.bytes(if i == 0 { b"" } else { b"," })?;
            write_named(doc, tool, e)?;
        }
        e.bytes(b"]")?;
    }
    e.bytes(b"}")
}
/// Writes a choice naming one function or custom tool: Responses spells the name flat, and
/// every other member, `type` first, stays. Any other choice has no Responses form here.
fn write_named(doc: &Document, choice: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let carrier = carrier(doc, choice)?.ok_or(Error::Unsupported)?;
    let mut first = true;
    for f in doc.children(choice)? {
        let n = doc.node(f)?.name.ok_or(Error::Schema)?;
        if !doc.name_is(n, carrier)? {
            e.bytes(if first { b"{" } else { b"," })?;
            first = false;
            doc.write_name(n, e)?;
            e.bytes(b":")?;
            doc.write(f, e)?;
        }
    }
    member(doc, doc.required(choice, carrier)?, "name", e)?;
    e.bytes(b"}")
}
/// The member a canonical function or custom tool, or a choice of one, nests its name in;
/// `None` for any other type.
fn carrier(doc: &Document, tool: usize) -> Result<Option<&'static str>> {
    for carrier in ["function", "custom"] {
        if is(doc, tool, "type", carrier)? {
            return Ok(Some(carrier));
        }
    }
    Ok(None)
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
    let mut natives = natives(doc)?.into_iter().peekable();
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
            match carrier(doc, tool)? {
                Some("custom") => write_custom(doc, tool, e)?,
                Some(_) => write_function(doc, tool, gemini, e)?,
                // Another protocol's tool, such as an Anthropic server tool, has no Responses form.
                None => return Err(Error::Unsupported),
            }
        } else {
            break;
        }
    }
    e.bytes(b"]")
}
/// Writes a function tool flat.
fn write_function(doc: &Document, tool: usize, gemini: bool, e: &mut Encoder<'_, '_>) -> Result<()> {
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
    e.bytes(b"}")
}
/// Writes a custom tool flat; a grammar format holds its syntax and definition beside its type.
fn write_custom(doc: &Document, tool: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"{\"type\":\"custom\"")?;
    for f in doc.children(doc.required(tool, "custom")?)? {
        let name = doc.node(f)?.name.ok_or(Error::Schema)?;
        e.bytes(b",")?;
        doc.write_name(name, e)?;
        e.bytes(b":")?;
        let grammar = if doc.name_is(name, "format")? && matches!(doc.node(f)?.value, Value::Object) {
            doc.member(f, "grammar")?
        } else {
            None
        };
        match grammar {
            Some(grammar) => write_flat(doc, f, grammar, e)?,
            None => doc.write(f, e)?,
        }
    }
    originals(doc, tool, e, &["type", "custom"])?;
    e.bytes(b"}")
}
/// Writes object `node` with the members of its member `nested` in place of `nested`.
fn write_flat(doc: &Document, node: usize, nested: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"{")?;
    let fields = doc.children(node)?.filter(|&f| f != nested);
    for (i, f) in fields.chain(doc.children(nested)?).enumerate() {
        e.bytes(if i == 0 { b"" } else { b"," })?;
        doc.write_name(doc.node(f)?.name.ok_or(Error::Schema)?, e)?;
        e.bytes(b":")?;
        doc.write(f, e)?;
    }
    e.bytes(b"}")
}
/// The native declarations kept in `x_ck.tools`. An entry bound to a canonical tool, such as a
/// Chat custom tool's, is that tool's projection and is written with it, not as a declaration.
fn natives(doc: &Document) -> Result<Vec<usize>> {
    let Some(x) = doc.member(doc.root(), "x_ck")? else {
        return Ok(Vec::new());
    };
    let mut natives = Vec::new();
    for entry in doc
        .member(x, "tools")?
        .map(|n| doc.children(n))
        .transpose()?
        .into_iter()
        .flatten()
    {
        if doc.origin(entry)?.is_none() {
            natives.push(entry);
        }
    }
    Ok(natives)
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
