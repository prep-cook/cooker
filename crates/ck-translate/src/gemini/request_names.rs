//! Gemini's REST API reads each proto field under its JSON name (`thinkingBudget`) or its proto
//! name (`thinking_budget`), and google-genai sends both. Ingress gives every member it maps the
//! JSON name, so later stages and the other protocols' encoders read one spelling.
use crate::codec::{
    Error, Result,
    document::{Document, Name, Value},
};

/// Whether `name` spells the field whose JSON name is `json`, in either form.
pub(crate) fn field_is(doc: &Document, name: Name, json: &str) -> Result<bool> {
    let mut chars = doc.name_chars(name)?;
    let mut snake = None;
    for expected in json.chars() {
        let mut c = chars.next_scalar()?;
        if expected.is_ascii_uppercase() {
            let underscore = c == Some('_');
            if *snake.get_or_insert(underscore) != underscore {
                return Ok(false);
            }
            if underscore {
                c = chars.next_scalar()?.filter(char::is_ascii_lowercase);
                c = c.map(|c| c.to_ascii_uppercase());
            }
        }
        if c != Some(expected) {
            return Ok(false);
        }
    }
    Ok(chars.next_scalar()?.is_none())
}

/// Gives members of `parent` spelled with a proto name the JSON name among `fields`. When both
/// spellings are present, the proto-named member stays as it came.
pub(crate) fn json_names(doc: &mut Document, parent: usize, fields: &[&'static str]) -> Result<()> {
    if !matches!(doc.node(parent)?.value, Value::Object) {
        return Ok(());
    }
    let mut next = doc.node(parent)?.first;
    while let Some(n) = next {
        next = doc.node(n)?.next;
        let name = doc.node(n)?.name.ok_or(Error::Schema)?;
        for &field in fields {
            if !doc.name_is(name, field)? && field_is(doc, name, field)? && doc.member(parent, field)?.is_none() {
                doc.rename(n, field)?;
            }
        }
    }
    Ok(())
}

/// [`Document::member`] for a Gemini field in either spelling; the member takes the JSON name.
pub(crate) fn field(doc: &mut Document, parent: usize, json: &'static str) -> Result<Option<usize>> {
    json_names(doc, parent, &[json])?;
    doc.member(parent, json)
}

/// [`Document::remove`] for a Gemini field in either spelling; the member takes the JSON name.
pub(crate) fn take(doc: &mut Document, parent: usize, json: &'static str) -> Result<Option<usize>> {
    json_names(doc, parent, &[json])?;
    doc.remove(parent, json)
}
