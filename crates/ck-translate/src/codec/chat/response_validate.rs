use super::*;

pub(super) fn validate_message(owner: &mut Writer, doc: &Document, message: usize) -> Result<()> {
    if !matches!(doc.node(message)?.value, Value::Object) {
        return Err(Error::Schema);
    }
    for field in doc.children(message)? {
        let name = doc.node(field)?.name.ok_or(Error::Schema)?;
        if ["role", "refusal", "reasoning_content"]
            .iter()
            .any(|key| doc.name_is(name, key).unwrap_or(false))
        {
            if !matches!(doc.node(field)?.value, Value::Null) {
                doc.chars(field)?;
            }
        } else if doc.name_is(name, "content")? {
            if !matches!(doc.node(field)?.value, Value::Null | Value::Array) {
                doc.chars(field)?;
            }
        } else if doc.name_is(name, "tool_calls")? {
            validate_tool_calls(doc, field)?;
        } else if doc.name_is(name, "function_call")? {
            validate_function(doc, field)?;
        } else if doc.name_is(name, "audio")? && matches!(doc.node(field)?.value, Value::Object) {
            if let Some(data) = doc.member(field, "data")? {
                let Value::String(text) = doc.node(data)?.value else {
                    return Err(Error::Schema);
                };
                media::decode_base64(doc, text, |_| Ok(()))?;
            }
            if let Some(text) = doc.member(field, "transcript")?
                && !matches!(doc.node(text)?.value, Value::Null)
            {
                doc.chars(text)?;
            }
        } else if (doc.name_is(name, "images")? || doc.name_is(name, "annotations")?)
            && !matches!(doc.node(field)?.value, Value::Array)
        {
            return Err(Error::Schema);
        }
        if (doc.name_is(name, "content")? || doc.name_is(name, "images")?)
            && matches!(doc.node(field)?.value, Value::Array)
        {
            for part in doc.children(field)? {
                if let Some(image) = doc.member(part, "image_url")?
                    && let Some(url) = doc.member(image, "url")?
                    && let Some((encoding, _)) = super::super::request_content::data_uri_metadata(doc, owner, url)?
                {
                    let Value::String(text) = doc.node(url)?.value else {
                        return Err(Error::Schema);
                    };
                    media::bytes(
                        doc,
                        media::Media {
                            node: url,
                            id: LocalId::new("validate")?,
                            data: Some(text),
                            encoding,
                            kind: "image",
                            mime: None,
                        },
                        |_| Ok(()),
                    )?;
                }
            }
        }
    }
    Ok(())
}

fn validate_tool_calls(doc: &Document, calls: usize) -> Result<()> {
    if !matches!(doc.node(calls)?.value, Value::Array) {
        return Err(Error::Schema);
    }
    for call in doc.children(calls)? {
        if let Some(index) = doc.member(call, "index")? {
            doc.unsigned(index)?;
        }
        if let Some(id) = doc.member(call, "id")? {
            doc.chars(id)?;
        }
        if let Some(kind) = doc.member(call, "type")?
            && !doc.string_is(kind, "function")?
            && !doc.string_is(kind, "custom")?
        {
            return Err(Error::Schema);
        }
        for key in ["function", "custom"] {
            if let Some(function) = doc.member(call, key)? {
                validate_function(doc, function)?;
            }
        }
    }
    Ok(())
}

fn validate_function(doc: &Document, function: usize) -> Result<()> {
    if !matches!(doc.node(function)?.value, Value::Object) {
        return Err(Error::Schema);
    }
    for key in ["name", "arguments", "input"] {
        if let Some(value) = doc.member(function, key)? {
            doc.chars(value)?;
        }
    }
    Ok(())
}
