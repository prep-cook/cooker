use crate::codec::{
    self, RequestView,
    document::{Document, Value},
    encode::Encoder,
};
/// Refuse Meta-only unsupported semantics before an attempt sends anything.
pub fn validate(view: RequestView<'_>) -> codec::Result<()> {
    super::validate::validate_for(view, "meta")?;
    let doc = view.document;
    let root = doc.root();
    if let Some(x) = doc.member(root, "x_ck")? {
        if let Some(providers) = doc.member(x, "provider_parameters")?
            && let Some(responses) = doc.member(providers, "responses")?
            && let Some(truncation) = doc.member(responses, "truncation")?
            && doc.string_is(truncation, "auto")?
        {
            return Err(codec::Error::Unsupported);
        }
        if crate::boundary::is(doc, x, "original_protocol", "chat")? && original(doc, "max_output_tokens")?.is_some() {
            return Err(codec::Error::Unsupported);
        }
    }
    Ok(())
}

// Only Meta opts into these Chat extensions; foreign originals are never flattened wholesale.
pub(crate) fn write_extensions(doc: &Document, e: &mut Encoder<'_, '_>) -> codec::Result<()> {
    let Some(x) = doc.member(doc.root(), "x_ck")? else {
        return Ok(());
    };
    if !crate::boundary::is(doc, x, "original_protocol", "chat")? {
        return Ok(());
    }
    if let Some(value) = original(doc, "prompt_cache_retention")? {
        e.bytes(b",\"prompt_cache_retention\":")?;
        doc.write(value, e)?;
    }
    Ok(())
}

fn original(doc: &Document, field: &str) -> codec::Result<Option<usize>> {
    let Some(x) = doc.member(doc.root(), "x_ck")? else {
        return Ok(None);
    };
    let Some(originals) = doc.member(x, "original")? else {
        return Ok(None);
    };
    for entry in doc.origins(doc.root())? {
        if doc.node(entry)?.parent != Some(originals) || !crate::boundary::is(doc, entry, "protocol", "chat")? {
            continue;
        }
        let Value::String(pointer) = doc.node(doc.required(entry, "pointer")?)?.value else {
            return Err(codec::Error::Schema);
        };
        if doc.name_is(doc.pointer_name(pointer)?, field)? {
            return Ok(Some(doc.required(entry, "value")?));
        }
    }
    Ok(None)
}
