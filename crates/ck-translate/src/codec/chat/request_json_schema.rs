//! Gemini function declarations as JSON Schema tool definitions, for the Chat and Anthropic
//! destinations. Gemini request egress writes the native declaration unchanged.
use crate::codec::{
    Error, Result,
    document::{Characters, Document, Name, Value},
    encode::Encoder,
    table::Table,
};
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};

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

/// Refuses a declaration with both `parameters` and `parametersJsonSchema`, a NON_BLOCKING
/// `behavior`, which changes call semantics, or a schema [`check`] refuses. A result schema
/// (`response` or `responseJsonSchema`) needs a destination with an `output` schema member,
/// and at most one of the two.
pub(crate) fn check_function(doc: &Document, function: usize, output: bool) -> Result<()> {
    let parameters = doc.member(function, "parameters")?;
    if parameters.is_some() && doc.member(function, "parametersJsonSchema")?.is_some() {
        return Err(Error::Unsupported);
    }
    if let Some(behavior) = doc.member(function, "behavior")?
        && !doc.string_is(behavior, "BLOCKING")?
    {
        return Err(Error::Unsupported);
    }
    let response = doc.member(function, "response")?;
    match (response, doc.member(function, "responseJsonSchema")?) {
        (None, None) => (),
        (Some(_), Some(_)) => return Err(Error::Unsupported),
        _ if !output => return Err(Error::Unsupported),
        (Some(response), None) => check(doc, response)?,
        (None, Some(_)) => (),
    }
    match parameters {
        Some(parameters) => check(doc, parameters),
        None => Ok(()),
    }
}

/// Writes the members of a declaration that passed [`check_function`], without braces: either
/// parameter schema under `schema_key`, either result schema under `output_key`, Gemini
/// schemas converted by [`write`], and no `behavior`, whose only admitted value is the default.
pub(crate) fn write_function(
    doc: &Document,
    function: usize,
    (schema_key, output_key): (&str, Option<&str>),
    e: &mut Encoder<'_, '_>,
) -> Result<()> {
    let mut first = true;
    for member in doc.children(function)? {
        let name = doc.node(member)?.name.ok_or(Error::Schema)?;
        if doc.name_is(name, "behavior")? {
            continue;
        }
        e.bytes(if first { b"" } else { b"," })?;
        first = false;
        let gemini = doc.name_is(name, "parameters")? || doc.name_is(name, "response")?;
        let key = if doc.name_is(name, "parameters")? || doc.name_is(name, "parametersJsonSchema")? {
            Some(schema_key)
        } else if doc.name_is(name, "response")? || doc.name_is(name, "responseJsonSchema")? {
            Some(output_key.ok_or(Error::Unsupported)?)
        } else {
            None
        };
        match key {
            Some(key) => e.string(key)?,
            None => doc.write_name(name, e)?,
        }
        e.bytes(b":")?;
        if gemini {
            write(doc, member, e)?;
        } else {
            doc.write(member, e)?;
        }
    }
    Ok(())
}

/// Refuses a Gemini schema JSON Schema cannot express: `TYPE_UNSPECIFIED` or an unknown type,
/// `nullable` without a type, a count that is not a whole number, a `propertyOrdering` that
/// does not list each property once, or `properties`, `items`, `anyOf` or `required` of a
/// shape [`write`] cannot write.
pub(crate) fn check(doc: &Document, schema: usize) -> Result<()> {
    doc.with_table(|table| {
        let mut names = Names {
            table,
            hasher: RandomState::new(),
        };
        check_with(doc, schema, &mut names)
    })
}
fn check_with(doc: &Document, schema: usize, names: &mut Names) -> Result<()> {
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
            && count_value(doc, count)?.is_none()
        {
            return Err(Error::Unsupported);
        }
    }
    if let Some(properties) = doc.member(schema, "properties")? {
        if !matches!(doc.node(properties)?.value, Value::Object) {
            return Err(Error::Unsupported);
        }
        for property in doc.children(properties)? {
            check_with(doc, property, names)?;
        }
    }
    if let Some(ordering) = doc.member(schema, "propertyOrdering")? {
        check_ordering(doc, doc.member(schema, "properties")?, ordering, names)?;
    }
    if let Some(items) = doc.member(schema, "items")? {
        check_with(doc, items, names)?;
    }
    if let Some(any) = doc.member(schema, "anyOf")? {
        if !matches!(doc.node(any)?.value, Value::Array) {
            return Err(Error::Unsupported);
        }
        for schema in doc.children(any)? {
            check_with(doc, schema, names)?;
        }
    }
    if let Some(required) = doc.member(schema, "required")? {
        if !matches!(doc.node(required)?.value, Value::Array) {
            return Err(Error::Unsupported);
        }
        for name in doc.children(required)? {
            if doc.chars(name).is_err() {
                return Err(Error::Unsupported);
            }
        }
    }
    Ok(())
}

/// Writes a Gemini schema that passed [`check`] as JSON Schema: lowercase types, `nullable` as a
/// `"null"` type, counts as numbers and `example` as `examples`. `propertyOrdering` becomes the
/// order of `properties`, which JSON Schema keeps; values such as `default` and `enum` stay as
/// written.
pub(crate) fn write(doc: &Document, schema: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    doc.with_table(|table| {
        let mut names = Names {
            table,
            hasher: RandomState::new(),
        };
        write_with(doc, schema, &mut names, e)
    })
}
fn write_with(doc: &Document, schema: usize, names: &mut Names, e: &mut Encoder<'_, '_>) -> Result<()> {
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
            // [`check`] made the ordering name each property once; its index region stays
            // below the regions nested objects push while their properties are written.
            let ordering = doc.member(schema, "propertyOrdering")?;
            let start = ordering.map(|_| names.index(doc, member)).transpose()?;
            let mut order = ordering.map(|o| doc.children(o)).transpose()?.into_iter().flatten();
            let mut properties = doc.children(member)?;
            let mut first = true;
            loop {
                let property = match start {
                    Some(start) => match order.next() {
                        Some(name) => Some(names.table[names.find(doc, start, name)?.ok_or(Error::Schema)?].2),
                        None => None,
                    },
                    None => properties.next(),
                };
                let Some(property) = property else {
                    break;
                };
                e.bytes(if first { b"" } else { b"," })?;
                first = false;
                doc.write_name(doc.node(property)?.name.ok_or(Error::Schema)?, e)?;
                e.bytes(b":")?;
                write_with(doc, property, names, e)?;
            }
            if let Some(start) = start {
                names.release(start);
            }
            e.bytes(b"}")?;
        } else if doc.name_is(name, "items")? {
            write_with(doc, member, names, e)?;
        } else if doc.name_is(name, "anyOf")? {
            e.bytes(b"[")?;
            for (i, schema) in doc.children(member)?.enumerate() {
                e.bytes(if i == 0 { b"" } else { b"," })?;
                write_with(doc, schema, names, e)?;
            }
            e.bytes(b"]")?;
        } else if count(doc, name)? && doc.unsigned(member).is_err() {
            e.unsigned(count_string(doc, member)?.ok_or(Error::Unsupported)?)?;
        } else {
            doc.write(member, e)?;
        }
    }
    e.bytes(b"}")
}

/// Refuses a `propertyOrdering` that is not an array naming each of `properties` exactly once.
fn check_ordering(doc: &Document, properties: Option<usize>, ordering: usize, names: &mut Names) -> Result<()> {
    let properties = properties.ok_or(Error::Unsupported)?;
    if !matches!(doc.node(ordering)?.value, Value::Array) {
        return Err(Error::Unsupported);
    }
    if doc.children(ordering)?.count() != doc.children(properties)?.count() {
        return Err(Error::Unsupported);
    }
    let start = names.index(doc, properties)?;
    for name in doc.children(ordering)? {
        let at = names.find(doc, start, name)?.ok_or(Error::Unsupported)?;
        if std::mem::replace(&mut names.table[at].1, true) {
            return Err(Error::Unsupported);
        }
    }
    names.release(start);
    Ok(())
}

/// Property lookups for `propertyOrdering`: `(name hash, matched, property)` entries in one
/// region per object, sorted by hash. Regions form a stack in the document's quota-charged
/// scratch table: a nested object pushes its region above its parent's and releases it before
/// the parent continues, so each name is hashed and found once, not compared with every other.
struct Names<'a> {
    table: &'a mut Table<(u64, bool, usize)>,
    hasher: RandomState,
}
impl Names<'_> {
    /// Pushes the region of `properties`; returns its start.
    fn index(&mut self, doc: &Document, properties: usize) -> Result<usize> {
        let start = self.table.len();
        for property in doc.children(properties)? {
            let hash = self.hash(doc.name_chars(doc.node(property)?.name.ok_or(Error::Schema)?)?)?;
            self.table.push((hash, false, property))?;
        }
        self.table[start..].sort_unstable_by_key(|entry| entry.0);
        Ok(start)
    }
    /// The table position of the property in the region at `start` named by string `name`.
    fn find(&self, doc: &Document, start: usize, name: usize) -> Result<Option<usize>> {
        let hash = self.hash(doc.chars(name).map_err(|_| Error::Unsupported)?)?;
        let region = &self.table[start..];
        let first = region.partition_point(|entry| entry.0 < hash);
        for (offset, entry) in region[first..].iter().enumerate() {
            if entry.0 != hash {
                break;
            }
            let mut left = doc.name_chars(doc.node(entry.2)?.name.ok_or(Error::Schema)?)?;
            let mut right = doc.chars(name)?;
            loop {
                let (a, b) = (left.next_scalar()?, right.next_scalar()?);
                if a != b {
                    break;
                }
                if a.is_none() {
                    return Ok(Some(start + first + offset));
                }
            }
        }
        Ok(None)
    }
    fn hash(&self, mut chars: Characters<'_>) -> Result<u64> {
        let mut state = self.hasher.build_hasher();
        while let Some(c) = chars.next_scalar()? {
            state.write_u32(c as u32);
        }
        Ok(state.finish())
    }
    /// Drops the region at `start` and every region above it.
    fn release(&mut self, start: usize) {
        while self.table.len() > start {
            self.table.swap_remove(self.table.len() - 1);
        }
    }
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

/// The whole number a Gemini count member holds, as [`write`] writes it: a JSON number or a
/// protobuf JSON int64 string such as `"01"`; none for any other value.
pub(crate) fn count_value(doc: &Document, node: usize) -> Result<Option<u64>> {
    match doc.unsigned(node) {
        Ok(n) => Ok(Some(n)),
        Err(_) => count_string(doc, node),
    }
}

/// The value of a protobuf JSON int64 string of ASCII digits, such as `"01"`; none for any
/// other string or a value past `u64`.
fn count_string(doc: &Document, node: usize) -> Result<Option<u64>> {
    let Ok(mut chars) = doc.chars(node) else {
        return Ok(None);
    };
    let mut value = None;
    while let Some(c) = chars.next_scalar()? {
        let Some(digit) = c.to_digit(10) else {
            return Ok(None);
        };
        value = value
            .unwrap_or(0u64)
            .checked_mul(10)
            .and_then(|v| v.checked_add(u64::from(digit)));
        if value.is_none() {
            return Ok(None);
        }
    }
    Ok(value)
}

fn count(doc: &Document, name: Name) -> Result<bool> {
    COUNTS
        .iter()
        .try_fold(false, |yes, key| Ok::<_, Error>(yes || doc.name_is(name, key)?))
}
