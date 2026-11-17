//! GLM Chat restrictions, independent of OpenAI's Chat capability.
use super::{ChatCapability, gemini, json_schema};
use crate::codec::document::Value;
use crate::codec::{Capability, Error, Operation, RequestView, Result};

pub struct GlmCapability;

impl Capability for GlmCapability {
    fn destination(&self) -> &'static str {
        "glm-chat"
    }

    fn validate(&self, view: RequestView<'_>) -> Result<()> {
        ChatCapability.validate(RequestView {
            operation: Operation::Generate,
            ..view
        })?;
        if view.operation == Operation::Compact {
            return Err(Error::Unsupported);
        }
        let doc = view.document;
        let root = doc.root();
        if let Some(choice) = doc.member(root, "tool_choice")?
            && (!matches!(
                doc.node(choice)?.value,
                Value::String(_) | Value::Text(_) | Value::Id(_) | Value::Utf8Media
            ) || !doc.string_is(choice, "auto")?)
        {
            return Err(Error::Unsupported);
        }
        if let Some(format) = doc.member(root, "response_format")? {
            if json_schema::from_gemini(doc)? {
                if gemini::json_mode(doc, format)?.is_some() {
                    return Err(Error::Unsupported);
                }
            } else {
                let kind = doc.required(format, "type")?;
                if !doc.string_is(kind, "text")? && !doc.string_is(kind, "json_object")? {
                    return Err(Error::Unsupported);
                }
            }
        }
        Ok(())
    }
}
