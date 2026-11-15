use super::super::{is, string};
use crate::codec::chat::request::gemini::{Outcome, json_mode, outcome};
use crate::codec::chat::request::json_schema;
use crate::codec::chat::request_schema::only;
use crate::codec::{
    Error, Operation, RequestView, Result,
    document::{Document, Value},
    table::Table,
};
use std::collections::hash_map::RandomState;

/// Tool call IDs by hash, then message ordinal, then ID node: one sorted index replaces a
/// history rescan for every tool output.
type CallIndex = Table<(u64, u64, usize)>;
/// The call index and the ordinal of the message being validated.
#[derive(Clone, Copy)]
struct Calls<'a> {
    index: &'a CallIndex,
    hasher: &'a RandomState,
    ordinal: u64,
}

pub(super) fn validate(view: RequestView<'_>) -> Result<()> {
    if view.operation == Operation::CountTokens {
        return Err(Error::Unsupported);
    }
    let doc = view.document;
    let root = doc.root();
    string(doc, doc.required(root, "model")?)?;
    if view.operation == Operation::Compact
        && doc
            .member(root, "stream")?
            .is_some_and(|n| matches!(doc.node(n).map(|n| n.value), Ok(Value::Bool(true))))
    {
        return Err(Error::Unsupported);
    }
    for unsupported in [
        "n",
        "seed",
        "stop",
        "logit_bias",
        "frequency_penalty",
        "presence_penalty",
        "modalities",
        "audio",
    ] {
        if doc.member(root, unsupported)?.is_some() {
            return Err(Error::Unsupported);
        }
    }
    if let Some(choice) = doc.member(root, "tool_choice")?
        && matches!(doc.node(choice)?.value, Value::Object)
        && let Some(allowed) = doc.member(choice, "allowed_tools")?
    {
        nested_allowed(doc, choice, allowed)?;
    }
    if json_schema::from_gemini(doc)? {
        gemini_controls(doc)?;
    }
    let messages = doc.required(root, "messages")?;
    if !matches!(doc.node(messages)?.value, Value::Array) {
        return Err(Error::Schema);
    }
    let x = doc.member(root, "x_ck")?;
    let continued = match x {
        Some(x) => continued(doc, x)?,
        None => false,
    };
    doc.with_table(|index| {
        let hasher = RandomState::new();
        index_calls(doc, messages, index, &hasher)?;
        for (ordinal, message) in doc.children(messages)?.enumerate() {
            let calls = Calls {
                index,
                hasher: &hasher,
                ordinal: ordinal as u64,
            };
            validate_message(view, message, calls, continued)?;
        }
        Ok(())
    })?;
    if let Some(x) = x {
        validate_extensions(doc, x)?;
    }
    Ok(())
}
/// Gemini JSON mode and function declarations, which egress writes in JSON Schema. A native
/// tool group, such as `googleSearch`, has no Responses form.
fn gemini_controls(doc: &Document) -> Result<()> {
    let root = doc.root();
    if let Some(native) = doc.member(doc.required(root, "x_ck")?, "tools")?
        && doc.node(native)?.first.is_some()
    {
        return Err(Error::Unsupported);
    }
    if let Some(format) = doc.member(root, "response_format")? {
        json_mode(doc, format)?;
    }
    if let Some(tools) = doc.member(root, "tools")? {
        for tool in doc.children(tools)? {
            json_schema::check_function(doc, doc.required(tool, "function")?, true)?;
        }
    }
    Ok(())
}
/// A nested allowed-tools choice, which egress writes flat. Responses lists a function or custom
/// tool by `type` and `name` alone, so any other member is unsupported.
fn nested_allowed(doc: &Document, choice: usize, allowed: usize) -> Result<()> {
    only(doc, choice, &["type", "allowed_tools"])?;
    only(doc, allowed, &["mode", "tools"])?;
    for tool in doc.children(doc.required(allowed, "tools")?)? {
        let mut carrier = Err(Error::Unsupported);
        for kind in ["function", "custom"] {
            if is(doc, tool, "type", kind)? {
                carrier = Ok(kind);
            }
        }
        let carrier = carrier?;
        only(doc, tool, &["type", carrier])?;
        only(doc, doc.required(tool, carrier)?, &["name"])?;
    }
    Ok(())
}
/// Whether the request continues stored state (`previous_response_id` or `conversation`), whose
/// calls the upstream links tool outputs to. Only a string identity, or a conversation object with
/// a string `id`, names such state; null or a malformed value does not.
fn continued(doc: &Document, x: usize) -> Result<bool> {
    if let Some(session) = doc.member(x, "session")?
        && let Some(id) = doc.member(session, "previous_response_id")?
        && string(doc, id).is_ok()
    {
        return Ok(true);
    }
    let Some(conversation) = doc
        .member(x, "provider_parameters")?
        .map(|providers| doc.member(providers, "responses"))
        .transpose()?
        .flatten()
        .map(|responses| doc.member(responses, "conversation"))
        .transpose()?
        .flatten()
    else {
        return Ok(false);
    };
    let id = match doc.node(conversation)?.value {
        Value::Object => doc.member(conversation, "id")?,
        _ => Some(conversation),
    };
    Ok(id.is_some_and(|id| string(doc, id).is_ok()))
}
/// Indexes every call ID. A repeated ID is accepted: the Responses API documents no uniqueness
/// rule for `call_id`, and an output then links to any earlier call with it.
fn index_calls(doc: &Document, messages: usize, index: &mut CallIndex, hasher: &RandomState) -> Result<()> {
    for (ordinal, message) in doc.children(messages)?.enumerate() {
        if let Some(list) = doc.member(message, "tool_calls")? {
            for call in doc.children(list)? {
                let id = doc.required(call, "id")?;
                string(doc, id)?;
                index.push((string_hash(doc, id, hasher)?, ordinal as u64, id))?;
            }
        }
    }
    index.sort_unstable();
    Ok(())
}
/// Whether a call in a message before `calls.ordinal` has the ID `id`.
fn prior_call(doc: &Document, calls: Calls<'_>, id: usize) -> Result<bool> {
    let hash = string_hash(doc, id, calls.hasher)?;
    let first = calls.index.partition_point(|entry| entry.0 < hash);
    for &(_, _, call) in calls.index[first..]
        .iter()
        .take_while(|entry| entry.0 == hash && entry.1 < calls.ordinal)
    {
        if doc.equal_strings(call, id)? {
            return Ok(true);
        }
    }
    Ok(false)
}
fn string_hash(doc: &Document, node: usize, hasher: &RandomState) -> Result<u64> {
    use std::hash::{BuildHasher, Hasher};
    let mut state = hasher.build_hasher();
    let mut chars = doc.chars(node)?;
    while let Some(c) = chars.next_scalar()? {
        state.write_u32(c as u32);
    }
    Ok(state.finish())
}
/// Validates one message; a tool output must follow its call unless `continued` stored state
/// may own the call.
fn validate_message(view: RequestView<'_>, message: usize, calls: Calls<'_>, continued: bool) -> Result<()> {
    let doc = view.document;
    let role = doc.required(message, "role")?;
    let mut valid = false;
    for value in ["system", "developer", "user", "assistant", "tool"] {
        valid |= doc.string_is(role, value)?;
    }
    if !valid {
        return Err(Error::Unsupported);
    }
    if is(doc, message, "role", "tool")? {
        let id = doc.required(message, "tool_call_id")?;
        string(doc, id)?;
        if !continued && !prior_call(doc, calls, id)? {
            return Err(Error::Schema);
        }
    }
    if let Some(calls) = doc.member(message, "tool_calls")? {
        for call in doc.children(calls)? {
            string(doc, doc.required(call, "id")?)?;
            let function = doc.required(call, "function")?;
            string(doc, doc.required(function, "name")?)?;
        }
    }
    if let Some(content) = doc.member(message, "content")? {
        match doc.node(content)?.value {
            Value::Null | Value::String(_) | Value::Text(_) | Value::Fragments | Value::Utf8Media => (),
            Value::Array => {
                for part in doc.children(content)? {
                    if is(doc, part, "type", "tool_result")? {
                        tool_result(view, part, calls, continued)?;
                    } else {
                        validate_part(view, part)?;
                    }
                }
            }
            _ => return Err(Error::Schema),
        }
    }
    Ok(())
}
/// A tool result part, which egress writes as a `function_call_output` item: its content, or a
/// Gemini `result` as JSON text. Responses has no field for `is_error: true` or a Gemini `error`.
fn tool_result(view: RequestView<'_>, part: usize, calls: Calls<'_>, continued: bool) -> Result<()> {
    let doc = view.document;
    let id = doc.required(part, "tool_call_id")?;
    string(doc, id)?;
    if !continued && !prior_call(doc, calls, id)? {
        return Err(Error::Schema);
    }
    if let Some(error) = doc.member(part, "is_error")? {
        match doc.node(error)?.value {
            Value::Bool(false) | Value::Null => (),
            Value::Bool(true) => return Err(Error::Unsupported),
            _ => return Err(Error::Schema),
        }
    }
    match outcome(doc, part)? {
        Some(Outcome::Error(_)) => Err(Error::Unsupported),
        Some(Outcome::Result(_)) => Ok(()),
        None => {
            if let Some(content) = doc.member(part, "content")? {
                if !matches!(doc.node(content)?.value, Value::Array) {
                    return Err(Error::Schema);
                }
                for item in doc.children(content)? {
                    validate_part(view, item)?;
                }
            }
            Ok(())
        }
    }
}
fn validate_part(view: RequestView<'_>, part: usize) -> Result<()> {
    let doc = view.document;
    let kind = doc.required(part, "type")?;
    let mut supported = false;
    for k in [
        "text",
        "reasoning",
        "reasoning_summary",
        "refusal",
        "encrypted",
        "image",
        "audio",
        "document",
        "opaque",
    ] {
        supported |= doc.string_is(kind, k)?;
    }
    // Text-like kinds carry their text in `text` (§4); egress writes it as the native member.
    for k in ["text", "reasoning", "reasoning_summary", "refusal"] {
        if doc.string_is(kind, k)? {
            string(doc, doc.required(part, "text")?)?;
        }
    }
    if doc.string_is(kind, "audio")? {
        let source = doc.required(part, "source")?;
        let mime = doc.required(part, "mime")?;
        if !is(doc, source, "kind", "data")?
            || (!doc.string_is(mime, "audio/mpeg")? && !doc.string_is(mime, "audio/wav")?)
        {
            return Err(Error::Unsupported);
        }
        view.media
            .get(doc, doc.required(source, "ref")?)?
            .ok_or(Error::Position)?;
    }
    if !supported {
        return Err(Error::Unsupported);
    }
    if doc.string_is(kind, "document")? {
        document(doc, part)?;
    }
    Ok(())
}
/// A Responses `input_file` holds a file's bytes, URL or ID and a filename. An Anthropic
/// plain-text source is not file bytes, and its `title`, `context` and `citations` have no
/// Responses field, so such a document is refused rather than sent malformed or cut down.
fn document(doc: &Document, part: usize) -> Result<()> {
    if doc.member(doc.required(part, "source")?, "anthropic_text")?.is_some() {
        return Err(Error::Unsupported);
    }
    let x = doc.member(doc.root(), "x_ck")?;
    let original = x.map(|x| doc.member(x, "original")).transpose()?.flatten();
    for entry in doc.origins(part)? {
        if original.is_some() && doc.node(entry)?.parent == original && !is(doc, entry, "protocol", "responses")? {
            return Err(Error::Unsupported);
        }
    }
    Ok(())
}
/// Refuses gateway extensions a Responses upstream cannot carry.
fn validate_extensions(doc: &Document, x: usize) -> Result<()> {
    if let Some(cache) = doc.member(x, "cache_control")?
        && doc.node(cache)?.first.is_some()
    {
        return Err(Error::Unsupported);
    }
    // Another protocol's parameters, such as Anthropic `top_k`, have no Responses form.
    if let Some(providers) = doc.member(x, "provider_parameters")? {
        for group in doc.children(providers)? {
            if !doc.name_is(doc.node(group)?.name.ok_or(Error::Schema)?, "responses")?
                && doc.node(group)?.first.is_some()
            {
                return Err(Error::Unsupported);
            }
        }
    }
    if let Some(replay) = doc.member(x, "replay")? {
        for entry in doc.children(replay)? {
            if !is(doc, entry, "kind", "responses_encrypted")? {
                return Err(Error::Unsupported);
            }
        }
    }
    if let Some(thinking) = doc.member(x, "thinking")?
        && !is(doc, x, "original_protocol", "responses")?
    {
        foreign_thinking(doc, x, thinking)?;
    }
    if let Some(session) = doc.member(x, "session")? {
        for field in doc.children(session)? {
            if !doc.name_is(doc.node(field)?.name.ok_or(Error::Schema)?, "previous_response_id")? {
                return Err(Error::Unsupported);
            }
        }
    }
    Ok(())
}
/// Another protocol's thinking reaches Responses only as `effort` and `summary`: a string
/// `include` is the summary, and Gemini's `includeThoughts: true` asks for `auto`. Thinking
/// turned on without an effort, such as a bare budget, has no Responses form, so it is refused
/// rather than dropped; a disabled setting needs none.
fn foreign_thinking(doc: &Document, x: usize, thinking: usize) -> Result<()> {
    // A Chat `reasoning` object maps only through its `effort` and `summary`; any other member,
    // such as a token budget, has no Responses form even beside an effort.
    if is(doc, x, "original_protocol", "chat")?
        && let Some(object) = doc.member(thinking, "native")?
    {
        for f in doc.children(object)? {
            let name = doc.node(f)?.name.ok_or(Error::Schema)?;
            if !doc.name_is(name, "effort")? && !doc.name_is(name, "summary")? {
                return Err(Error::Unsupported);
            }
        }
        if doc.node(object)?.first.is_some() {
            return Ok(());
        }
    }
    let set = |key: &str| -> Result<Option<bool>> {
        Ok(match doc.member(thinking, key)? {
            Some(n) => Some(!matches!(doc.node(n)?.value, Value::Bool(false))),
            None => None,
        })
    };
    if let Some(include) = doc.member(thinking, "include")?
        && !matches!(
            doc.node(include)?.value,
            Value::Bool(_) | Value::Null | Value::String(_) | Value::Text(_)
        )
    {
        return Err(Error::Unsupported);
    }
    if doc.member(thinking, "effort")?.is_some() || set("enabled")? == Some(false) {
        return Ok(());
    }
    for key in ["enabled", "budget_tokens", "native"] {
        if set(key)? == Some(true) {
            return Err(Error::Unsupported);
        }
    }
    Ok(())
}
