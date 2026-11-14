//! Gemini function declarations as JSON Schema tool definitions, for the Chat and Anthropic
//! destinations. Gemini request egress writes the native declaration unchanged.
use crate::codec::{
    Error, Result,
    document::{Document, Name, Value},
    encode::Encoder,
};

/// JSON Schema type names; Gemini's `Type` enum spells them in upper case.
const TYPES: [&str; 7] = ["string", "number", "integer", "boolean", "array", "object", "null"];
/// Gemini int64 members, which protobuf JSON may send as strings.
const COUNTS: [&str; 6] = [
    "minItems",
    "maxItems",
    "minProperties",
    "maxProperties",
    "minLength",
    "maxLength",
];

/// Whether the request came from Gemini ingress, so its tool schemas are Gemini schemas.
pub(crate) fn from_gemini(doc: &Document) -> Result<bool> {
    let x = doc.required(doc.root(), "x_ck")?;
    match doc.member(x, "original_protocol")? {
        Some(protocol) => doc.string_is(protocol, "gemini"),
        None => Ok(false),
    }
}

/// Declaration members about the function's return or scheduling, which neither destination has.
/// [`check_function`] refuses a `behavior` other than the default, BLOCKING.
const RESULT_MEMBERS: [&str; 3] = ["response", "responseJsonSchema", "behavior"];

/// Refuses a declaration with both `parameters` and `parametersJsonSchema`, a NON_BLOCKING
/// `behavior`, which changes call semantics, or a `parameters` schema [`check`] refuses.
pub(crate) fn check_function(doc: &Document, function: usize) -> Result<()> {
    let parameters = doc.member(function, "parameters")?;
    if parameters.is_some() && doc.member(function, "parametersJsonSchema")?.is_some() {
        return Err(Error::Unsupported);
    }
    if let Some(behavior) = doc.member(function, "behavior")?
        && !doc.string_is(behavior, "BLOCKING")?
    {
        return Err(Error::Unsupported);
    }
    match parameters {
        Some(parameters) => check(doc, parameters),
        None => Ok(()),
    }
}

/// Writes the members of a declaration that passed [`check_function`], without braces: either
/// schema under `schema_key`, `parameters` converted by [`write`], and no [`RESULT_MEMBERS`].
pub(crate) fn write_function(doc: &Document, function: usize, schema_key: &str, e: &mut Encoder<'_, '_>) -> Result<()> {
    let mut first = true;
    for member in doc.children(function)? {
        let name = doc.node(member)?.name.ok_or(Error::Schema)?;
        if RESULT_MEMBERS
            .iter()
            .try_fold(false, |yes, key| Ok::<_, Error>(yes || doc.name_is(name, key)?))?
        {
            continue;
        }
        e.bytes(if first { b"" } else { b"," })?;
        first = false;
        if doc.name_is(name, "parameters")? || doc.name_is(name, "parametersJsonSchema")? {
            e.string(schema_key)?;
            e.bytes(b":")?;
            if doc.name_is(name, "parameters")? {
                write(doc, member, e)?;
            } else {
                doc.write(member, e)?;
            }
        } else {
            doc.write_name(name, e)?;
            e.bytes(b":")?;
            doc.write(member, e)?;
        }
    }
    Ok(())
}

/// Refuses a Gemini schema JSON Schema cannot express: `TYPE_UNSPECIFIED` or an unknown type,
/// `nullable` without a type, or a count that is not a whole number.
pub(crate) fn check(doc: &Document, schema: usize) -> Result<()> {
    if !matches!(doc.node(schema)?.value, Value::Object) {
        return Err(Error::Unsupported);
    }
    let kind = doc.member(schema, "type")?;
    if let Some(kind) = kind
        && type_name(doc, kind)?.is_none()
    {
        return Err(Error::Unsupported);
    }
    if let Some(nullable) = doc.member(schema, "nullable")? {
        let Value::Bool(nullable) = doc.node(nullable)?.value else {
            return Err(Error::Schema);
        };
        if nullable && kind.is_none() {
            return Err(Error::Unsupported);
        }
    }
    for key in COUNTS {
        if let Some(count) = doc.member(schema, key)?
            && doc.unsigned(count).is_err()
            && !digits(doc, count)?
        {
            return Err(Error::Unsupported);
        }
    }
    if let Some(properties) = doc.member(schema, "properties")? {
        for property in doc.children(properties)? {
            check(doc, property)?;
        }
    }
    if let Some(items) = doc.member(schema, "items")? {
        check(doc, items)?;
    }
    if let Some(any) = doc.member(schema, "anyOf")? {
        for schema in doc.children(any)? {
            check(doc, schema)?;
        }
    }
    Ok(())
}

/// Writes a Gemini schema that passed [`check`] as JSON Schema: lowercase types, `nullable` as a
/// `"null"` type, counts as numbers and `example` as `examples`. `propertyOrdering` only orders
/// Gemini output and is left out; values such as `default` and `enum` stay as written.
pub(crate) fn write(doc: &Document, schema: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let nullable = match doc.member(schema, "nullable")? {
        Some(n) => matches!(doc.node(n)?.value, Value::Bool(true)),
        None => false,
    };
    e.bytes(b"{")?;
    let mut first = true;
    for member in doc.children(schema)? {
        let name = doc.node(member)?.name.ok_or(Error::Schema)?;
        if doc.name_is(name, "nullable")? || doc.name_is(name, "propertyOrdering")? {
            continue;
        }
        e.bytes(if first { b"" } else { b"," })?;
        first = false;
        if doc.name_is(name, "example")? {
            e.bytes(b"\"examples\":[")?;
            doc.write(member, e)?;
            e.bytes(b"]")?;
            continue;
        }
        doc.write_name(name, e)?;
        e.bytes(b":")?;
        if doc.name_is(name, "type")? {
            let kind = type_name(doc, member)?.ok_or(Error::Unsupported)?;
            if nullable && kind != "null" {
                e.bytes(b"[")?;
                e.string(kind)?;
                e.bytes(b",\"null\"]")?;
            } else {
                e.string(kind)?;
            }
        } else if doc.name_is(name, "properties")? {
            e.bytes(b"{")?;
            for (i, property) in doc.children(member)?.enumerate() {
                e.bytes(if i == 0 { b"" } else { b"," })?;
                doc.write_name(doc.node(property)?.name.ok_or(Error::Schema)?, e)?;
                e.bytes(b":")?;
                write(doc, property, e)?;
            }
            e.bytes(b"}")?;
        } else if doc.name_is(name, "items")? {
            write(doc, member, e)?;
        } else if doc.name_is(name, "anyOf")? {
            e.bytes(b"[")?;
            for (i, schema) in doc.children(member)?.enumerate() {
                e.bytes(if i == 0 { b"" } else { b"," })?;
                write(doc, schema, e)?;
            }
            e.bytes(b"]")?;
        } else if count(doc, name)? && matches!(doc.node(member)?.value, Value::String(_)) {
            doc.write_contents(member, e)?;
        } else {
            doc.write(member, e)?;
        }
    }
    e.bytes(b"}")
}

/// The JSON Schema name of a Gemini `type`, matched ignoring ASCII case.
fn type_name(doc: &Document, kind: usize) -> Result<Option<&'static str>> {
    if !matches!(doc.node(kind)?.value, Value::String(_)) {
        return Ok(None);
    }
    'names: for name in TYPES {
        let mut chars = doc.chars(kind)?;
        for expected in name.chars() {
            if chars.next_scalar()?.map(|c| c.to_ascii_lowercase()) != Some(expected) {
                continue 'names;
            }
        }
        if chars.next_scalar()?.is_none() {
            return Ok(Some(name));
        }
    }
    Ok(None)
}

/// Whether a string holds only ASCII digits, as a protobuf JSON int64 does.
fn digits(doc: &Document, node: usize) -> Result<bool> {
    if !matches!(doc.node(node)?.value, Value::String(_)) {
        return Ok(false);
    }
    let mut chars = doc.chars(node)?;
    let mut any = false;
    while let Some(c) = chars.next_scalar()? {
        if !c.is_ascii_digit() {
            return Ok(false);
        }
        any = true;
    }
    Ok(any)
}

fn count(doc: &Document, name: Name) -> Result<bool> {
    COUNTS
        .iter()
        .try_fold(false, |yes, key| Ok::<_, Error>(yes || doc.name_is(name, key)?))
}
