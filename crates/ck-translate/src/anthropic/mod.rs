//! Anthropic Messages boundary codecs over the shared canonical stream.

pub mod compaction;
mod content;
pub mod egress;
mod index;
pub mod request;
mod request_encode;
mod request_schema;
pub mod response;
mod usage;

use crate::boundary::{is, mapped, member, object, string};
use crate::codec::table::Table;
use crate::codec::{Result, document::Document, encode::Encoder};

/// Anthropic's error types with the HTTP status each stands for (Anthropic "Errors").
const ERRORS: &[(&str, u16)] = &[
    ("invalid_request_error", 400),
    ("authentication_error", 401),
    ("billing_error", 402),
    ("permission_error", 403),
    ("not_found_error", 404),
    ("request_too_large", 413),
    ("rate_limit_error", 429),
    ("api_error", 500),
    ("timeout_error", 504),
    ("overloaded_error", 529),
];
/// The official error type `node` names, if it names one.
fn error_type(doc: &Document, node: Option<usize>) -> Result<Option<(&'static str, u16)>> {
    let Some(node) = node else {
        return Ok(None);
    };
    for &(kind, status) in ERRORS {
        if doc.string_is(node, kind)? {
            return Ok(Some((kind, status)));
        }
    }
    Ok(None)
}
fn unknown(doc: &Document, parent: usize, known: &[&str], e: &mut Encoder<'_, '_>) -> Result<()> {
    unknown_at(doc, &[(parent, "", known)], e)
}

fn unknown_at(doc: &Document, groups: &[(usize, &str, &[&str])], e: &mut Encoder<'_, '_>) -> Result<()> {
    crate::boundary::original(doc, "anthropic", groups, e)
}

fn server_result(doc: &Document, node: usize) -> Result<bool> {
    [
        "web_search_tool_result",
        "web_fetch_tool_result",
        "code_execution_tool_result",
        "bash_code_execution_tool_result",
        "text_editor_code_execution_tool_result",
        "tool_search_tool_result",
        "mcp_tool_result",
    ]
    .iter()
    .try_fold(false, |found, kind| Ok(found || is(doc, node, "type", kind)?))
}

fn equal_text(left: &Document, a: usize, right: &Document, b: usize) -> Result<bool> {
    let mut a = left.chars(a)?;
    let mut b = right.chars(b)?;
    loop {
        let value = a.next_scalar()?;
        if value != b.next_scalar()? {
            return Ok(false);
        }
        if value.is_none() {
            return Ok(true);
        }
    }
}
