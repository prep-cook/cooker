//! Gemini function responses and JSON mode in the form the Chat, Anthropic and Responses
//! destinations write them.
use super::json_schema;
use crate::codec::{
    Error, Result,
    document::{Document, Value},
    encode::Encoder,
};

/// What a Gemini function response reports.
#[derive(Clone, Copy)]
pub(crate) enum Outcome {
    /// The `response.result` value.
    Result(usize),
    /// The `response.error` value.
    Error(usize),
}

/// The outcome of tool result `part` when it carries Gemini's native `result` (the
/// `functionResponse` less its id); `None` for any other tool result. Only a `response` holding
/// exactly one `result` or `error` member has a text form; another shape, or another
/// `functionResponse` member such as `parts` or `willContinue`, is refused.
pub(crate) fn outcome(doc: &Document, part: usize) -> Result<Option<Outcome>> {
    let Some(native) = doc.member(part, "result")? else {
        return Ok(None);
    };
    if !matches!(doc.node(native)?.value, Value::Object) {
        return Err(Error::Schema);
    }
    for member in doc.children(native)? {
        let name = doc.node(member)?.name.ok_or(Error::Schema)?;
        if !doc.name_is(name, "name")? && !doc.name_is(name, "response")? {
            return Err(Error::Unsupported);
        }
    }
    let response = doc.required(native, "response")?;
    if !matches!(doc.node(response)?.value, Value::Object) {
        return Err(Error::Schema);
    }
    let mut members = doc.children(response)?;
    let (Some(only), None) = (members.next(), members.next()) else {
        return Err(Error::Unsupported);
    };
    let name = doc.node(only)?.name.ok_or(Error::Schema)?;
    if doc.name_is(name, "result")? {
        Ok(Some(Outcome::Result(only)))
    } else if doc.name_is(name, "error")? {
        Ok(Some(Outcome::Error(only)))
    } else {
        Err(Error::Unsupported)
    }
}

/// Writes `,"<key>":` and a Gemini outcome as tool text: a string `result` as it is, any other
/// value as its compact JSON serialization, then `,"is_error":true` for an error; returns false,
/// writing nothing, for any other tool result. Only the Anthropic fence admits an error.
pub(crate) fn write_outcome(doc: &Document, part: usize, key: &str, e: &mut Encoder<'_, '_>) -> Result<bool> {
    let Some(outcome) = outcome(doc, part)? else {
        return Ok(false);
    };
    let (Outcome::Result(value) | Outcome::Error(value)) = outcome;
    e.bytes(b",")?;
    e.string(key)?;
    e.bytes(b":")?;
    let text = matches!(
        doc.node(value)?.value,
        Value::String(_) | Value::Text(_) | Value::Fragments | Value::Utf8Media
    );
    if text && matches!(outcome, Outcome::Result(_)) {
        doc.write(value, e)?;
    } else {
        e.quoted_json(|e| doc.write(value, e))?;
    }
    if let Outcome::Error(_) = outcome {
        e.bytes(b",\"is_error\":true")?;
    }
    Ok(true)
}

/// The schema a Gemini `response_format` asks for, `None` for JSON mode without one. The
/// format must name `application/json` and at most one of `responseSchema` and
/// `responseJsonSchema`; another MIME type, such as `text/x.enum`, has no JSON Schema form. A
/// null member is unset, as in protobuf JSON.
pub(crate) fn json_mode(doc: &Document, format: usize) -> Result<Option<usize>> {
    let set = |key| -> Result<Option<usize>> {
        Ok(doc
            .member(format, key)?
            .filter(|&n| !matches!(doc.node(n).map(|n| n.value), Ok(Value::Null))))
    };
    match set("responseMimeType")? {
        Some(mime) if doc.string_is(mime, "application/json")? => (),
        _ => return Err(Error::Unsupported),
    }
    match (set("responseSchema")?, set("responseJsonSchema")?) {
        (Some(_), Some(_)) => Err(Error::Unsupported),
        (Some(schema), None) => {
            json_schema::check(doc, schema)?;
            Ok(Some(schema))
        }
        (None, Some(schema)) if matches!(doc.node(schema)?.value, Value::Object) => Ok(Some(schema)),
        (None, Some(_)) => Err(Error::Schema),
        (None, None) => Ok(None),
    }
}

/// Writes a schema [`json_mode`] returned as JSON Schema.
pub(crate) fn write_schema(doc: &Document, schema: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    if doc.name_is(doc.node(schema)?.name.ok_or(Error::Schema)?, "responseSchema")? {
        json_schema::write(doc, schema, e)
    } else {
        doc.write(schema, e)
    }
}
