//! `generationConfig` members with a canonical home: sampling controls, thinking and the
//! response schema.
use super::names::{field, json_names, take};
use crate::codec::{
    Error, Result,
    document::{Document, Value},
};

/// Moves the mapped `generationConfig` members to the root, thinking to `x_ck.thinking` and
/// the response schema to `response_format`; the rest stays in the native config.
pub(super) fn generation_config(doc: &mut Document, config: usize, x: usize) -> Result<()> {
    let root = doc.root();
    super::super::object(doc, config)?;
    for &(wire, canonical) in super::CONFIG {
        if let Some(n) = take(doc, config, wire)? {
            doc.rename(n, canonical)?;
            doc.append(root, n)?;
        }
    }
    if let Some(thinking) = take(doc, config, "thinkingConfig")? {
        super::super::object(doc, thinking)?;
        let normalized = doc.set(x, "thinking", Value::Object)?;
        thinking_budget(doc, thinking, normalized)?;
        for (wire, canonical) in [("includeThoughts", "include"), ("thinkingLevel", "effort")] {
            if let Some(n) = take(doc, thinking, wire)? {
                doc.rename(n, canonical)?;
                doc.append(normalized, n)?;
            }
        }
        // Another protocol's fence refuses any native thinking config, even an empty one.
        if doc.node(thinking)?.first.is_some() {
            doc.rename(thinking, "native")?;
            doc.append(normalized, thinking)?;
        }
    }
    const SCHEMA: [&str; 3] = ["responseMimeType", "responseSchema", "responseJsonSchema"];
    json_names(doc, config, &SCHEMA)?;
    if SCHEMA.iter().try_fold(false, |found, key| {
        Ok::<_, Error>(found || doc.member(config, key)?.is_some())
    })? {
        let format = doc.set(root, "response_format", Value::Object)?;
        doc.set(format, "type", Value::Text("json_schema"))?;
        for key in SCHEMA {
            if let Some(n) = doc.remove(config, key)? {
                doc.append(format, n)?;
            }
        }
    }
    Ok(())
}
/// Maps `thinkingBudget`: a positive budget enables thinking with it, 0 disables it and -1
/// (dynamic) enables it without one. Any other value stays native.
fn thinking_budget(doc: &mut Document, thinking: usize, normalized: usize) -> Result<()> {
    let Some(budget) = field(doc, thinking, "thinkingBudget")? else {
        return Ok(());
    };
    let dynamic = match doc.node(budget)?.value {
        Value::Number(t) => {
            let mut cursor = doc.source(t)?.cursor(t.span)?;
            let (minus, one) = (cursor.next_byte()?, cursor.next_byte()?);
            (minus, one, cursor.next_byte()?) == (Some(b'-'), Some(b'1'), None)
        }
        _ => false,
    };
    let Some(tokens) = doc.unsigned(budget).ok().or(dynamic.then_some(0)) else {
        return Ok(());
    };
    doc.remove(thinking, "thinkingBudget")?;
    doc.set(normalized, "enabled", Value::Bool(dynamic || tokens > 0))?;
    if tokens > 0 {
        doc.set(normalized, "budget_tokens", Value::Unsigned(tokens))?;
    }
    Ok(())
}
