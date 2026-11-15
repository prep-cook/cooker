use crate::codec::{
    Error, Operation, RequestView, Result,
    document::{Document, Value},
};

pub(super) fn validate(view: RequestView<'_>) -> Result<()> {
    if view.operation == Operation::Compact {
        return Err(Error::Unsupported);
    }
    let doc = view.document;
    let root = doc.root();
    super::string(doc, doc.required(root, "model")?)?;
    messages(view)?;
    controls(doc)?;
    if let Some(x) = doc.member(root, "x_ck")? {
        extension(doc, x)?;
    }
    view.media.validate_registrations(doc, 0)?;
    Ok(())
}
/// Roles, content parts and history tool calls.
fn messages(view: RequestView<'_>) -> Result<()> {
    let doc = view.document;
    let root = doc.root();
    let messages = doc.required(root, "messages")?;
    super::array(doc, messages)?;
    let mut system = false;
    let (mut run, mut last) = (None, None);
    for message in doc.children(messages)? {
        let previous = last.replace(message);
        let role = doc.required(message, "role")?;
        if doc.string_is(role, "tool")? {
            tool_message(doc, message, run)?;
            continue;
        }
        // Gemini has one system instruction; `developer` (Responses `instructions`) is one too.
        if doc.string_is(role, "system")? || doc.string_is(role, "developer")? {
            if system {
                return Err(Error::Unsupported);
            }
            system = true;
        } else if !doc.string_is(role, "user")? && !doc.string_is(role, "assistant")? {
            return Err(Error::Unsupported);
        }
        if let Some(content) = doc.member(message, "content")? {
            match doc.node(content)?.value {
                Value::String(_) | Value::Text(_) | Value::Fragments | Value::Utf8Media => (),
                // Egress orders a message's results by their calls, which keeps the place of
                // its other parts only while the results stand together.
                Value::Array => {
                    let (mut blocks, mut inside) = (0, false);
                    for part in doc.children(content)? {
                        validate_part(view, part, run)?;
                        let result = is_result(doc, part)?;
                        blocks += usize::from(result && !inside);
                        inside = result;
                    }
                    if blocks > 1 {
                        return Err(Error::Unsupported);
                    }
                }
                // OpenAI sends a tool-call message's empty content as null.
                Value::Null
                    if doc.string_is(role, "assistant")?
                        && doc
                            .member(message, "tool_calls")?
                            .map(|calls| doc.children(calls).map(|mut c| c.next().is_some()))
                            .transpose()?
                            .unwrap_or(false) => {}
                _ => return Err(Error::Unsupported),
            }
        }
        if let Some(calls) = doc.member(message, "tool_calls")? {
            super::array(doc, calls)?;
            for call in doc.children(calls)? {
                if !super::is(doc, call, "type", "function")? {
                    return Err(Error::Unsupported);
                }
                if let Some(format) = doc.member(call, "arguments_format")?
                    && !doc.string_is(format, "json")?
                {
                    return Err(Error::Unsupported);
                }
                super::string(doc, doc.required(call, "id")?)?;
                let function = doc.required(call, "function")?;
                super::string(doc, doc.required(function, "name")?)?;
                if let Some(args) = doc.member(function, "arguments")? {
                    super::object(doc, args)?;
                }
            }
        }
        run = match previous {
            Some(p) if continues(doc, p, message)? => run,
            _ => Some(message),
        };
    }
    Ok(())
}
/// Whether assistant `message` adds its calls to the model turn of assistant `previous`. A
/// Responses client sends each parallel call as its own item, which ingress wraps in its own
/// message; Gemini states them as one turn. Both carry a call, so a joined turn already has a
/// part. Gemini's own turns stay as sent.
pub(super) fn continues(doc: &Document, previous: usize, message: usize) -> Result<bool> {
    for m in [previous, message] {
        let calls = doc.member(m, "tool_calls")?;
        if !super::is(doc, m, "role", "assistant")?
            || calls.map(|c| doc.node(c)).transpose()?.and_then(|c| c.first).is_none()
            || super::origin(doc, m, "parts")?.is_some()
        {
            return Ok(false);
        }
    }
    Ok(true)
}
/// The one call with `id` in the model turn starting at message `run`; none when no call or
/// several carry it.
pub(super) fn answered(doc: &Document, run: Option<usize>, id: usize) -> Result<Option<usize>> {
    let (mut found, mut count, mut message) = (None, 0, run);
    while let Some(m) = message {
        for call in doc
            .member(m, "tool_calls")?
            .map(|c| doc.children(c))
            .transpose()?
            .into_iter()
            .flatten()
        {
            if doc.equal_strings(doc.required(call, "id")?, id)? {
                (found, count) = (Some(call), count + 1);
            }
        }
        message = next_in_turn(doc, m)?;
    }
    Ok(found.filter(|_| count == 1))
}
/// Whether `node` is a Chat tool message or a tool result part without Gemini's `result`: both
/// answer a call of the preceding model turn by id.
pub(super) fn is_result(doc: &Document, node: usize) -> Result<bool> {
    Ok(super::is(doc, node, "role", "tool")?
        || (super::is(doc, node, "type", "tool_result")? && doc.member(node, "result")?.is_none()))
}
/// The message after `message` in its model turn.
pub(super) fn next_in_turn(doc: &Document, message: usize) -> Result<Option<usize>> {
    Ok(match doc.node(message)?.next {
        Some(n) if continues(doc, message, n)? => Some(n),
        _ => None,
    })
}
/// A Chat tool message, or a tool result without Gemini's `result`, answers a call of the
/// preceding model turn and carries only text.
fn tool_message(doc: &Document, message: usize, run: Option<usize>) -> Result<()> {
    let id = doc.required(message, "tool_call_id")?;
    super::string(doc, id)?;
    if answered(doc, run, id)?.is_none() {
        return Err(Error::Unsupported);
    }
    match doc.member(message, "content")? {
        Some(content) if matches!(doc.node(content)?.value, Value::Array) => {
            for part in doc.children(content)? {
                if !super::is(doc, part, "type", "text")? {
                    return Err(Error::Unsupported);
                }
                super::string(doc, doc.required(part, "text")?)?;
            }
            Ok(())
        }
        Some(content) => match doc.node(content)?.value {
            Value::String(_) | Value::Text(_) | Value::Fragments | Value::Utf8Media => Ok(()),
            _ => Err(Error::Unsupported),
        },
        None => Ok(()),
    }
}
/// Sampling, limits, modalities and tools; Chat controls Gemini cannot express fail.
fn controls(doc: &Document) -> Result<()> {
    let root = doc.root();
    for key in ["temperature", "top_p"] {
        if let Some(n) = doc.member(root, key)?
            && !matches!(doc.node(n)?.value, Value::Number(_) | Value::Unsigned(_))
        {
            return Err(Error::Schema);
        }
    }
    for key in ["max_tokens", "max_completion_tokens", "n", "seed"] {
        if let Some(n) = doc.member(root, key)? {
            doc.unsigned(n)?;
        }
    }
    if let Some(modalities) = doc.member(root, "modalities")? {
        super::array(doc, modalities)?;
        for modality in doc.children(modalities)? {
            if !["text", "image", "audio", "video", "TEXT", "IMAGE", "AUDIO", "VIDEO"]
                .iter()
                .try_fold(false, |yes, v| Ok::<_, Error>(yes || doc.string_is(modality, v)?))?
            {
                return Err(Error::Unsupported);
            }
        }
    }
    for key in [
        "audio",
        "prediction",
        "logit_bias",
        "presence_penalty",
        "frequency_penalty",
        "service_tier",
    ] {
        if doc.member(root, key)?.is_some() {
            return Err(Error::Unsupported);
        }
    }
    // Chat and Responses log probabilities have no mapping to `responseLogprobs` and back,
    // so a request for them is refused rather than dropped. A Gemini client's own controls
    // stay in its native `generationConfig`.
    if let Some(n) = doc.member(root, "logprobs")?
        && !matches!(doc.node(n)?.value, Value::Null | Value::Bool(false))
    {
        return Err(Error::Unsupported);
    }
    if let Some(n) = doc.member(root, "top_logprobs")?
        && !matches!(doc.node(n)?.value, Value::Null)
    {
        return Err(Error::Unsupported);
    }
    // Gemini runs parallel calls and stores nothing, so only the opposite settings are refused.
    for (key, default) in [("parallel_tool_calls", true), ("store", false)] {
        if let Some(n) = doc.member(root, key)?
            && !match doc.node(n)?.value {
                Value::Null => true,
                Value::Bool(value) => value == default,
                _ => false,
            }
        {
            return Err(Error::Unsupported);
        }
    }
    if let Some(format) = doc.member(root, "response_format")?
        && !["text", "json_object", "json_schema"]
            .iter()
            .try_fold(false, |yes, kind| {
                Ok::<_, Error>(yes || super::is(doc, format, "type", kind)?)
            })?
    {
        return Err(Error::Unsupported);
    }
    // Gemini takes `allowedFunctionNames` with mode ANY or VALIDATED; only ANY, Chat's
    // `required`, has a Chat counterpart.
    if let Some(choice) = doc.member(root, "tool_choice")?
        && matches!(doc.node(choice)?.value, Value::Object)
        && let Some(allowed) = doc.member(choice, "allowed_tools")?
        && !super::is(doc, allowed, "mode", "required")?
    {
        return Err(Error::Unsupported);
    }
    if let Some(tools) = doc.member(root, "tools")? {
        super::array(doc, tools)?;
        for tool in doc.children(tools)? {
            if !super::is(doc, tool, "type", "function")? {
                return Err(Error::Unsupported);
            }
            let function = doc.required(tool, "function")?;
            super::string(doc, doc.required(function, "name")?)?;
        }
    }
    Ok(())
}
/// Session, transport, provider parameters, replay, cache directives, native tools and a Chat
/// `reasoning` object.
fn extension(doc: &Document, x: usize) -> Result<()> {
    for key in ["session", "transport"] {
        if doc.member(x, key)?.is_some() {
            return Err(Error::Unsupported);
        }
    }
    if let Some(provider) = doc.member(x, "provider_parameters")? {
        for native in doc.children(provider)? {
            let name = doc.node(native)?.name.ok_or(Error::Schema)?;
            if doc.name_is(name, "anthropic")? {
                anthropic_parameters(doc, native)?;
            } else if !doc.name_is(name, "gemini")? && doc.node(native)?.first.is_some() {
                return Err(Error::Unsupported);
            }
        }
    }
    let claude = super::is(doc, x, "original_protocol", "anthropic")?;
    if let Some(replay) = doc.member(x, "replay")? {
        for entry in doc.children(replay)? {
            // Request egress strips Claude thinking history, as at a Chat upstream (owner
            // ruling 2026-11-15).
            if claude && super::is(doc, entry, "kind", "anthropic")? {
                continue;
            }
            if !super::is(doc, entry, "kind", "gemini")? {
                return Err(Error::Unsupported);
            }
            super::string(doc, doc.required(entry, "signature")?)?;
        }
    }
    if let Some(cache) = doc.member(x, "cache_control")?
        && doc.node(cache)?.first.is_some()
    {
        return Err(Error::Unsupported);
    }
    if let Some(native) = doc.member(x, "tools")?
        && doc.node(native)?.first.is_some()
        && !super::is(doc, x, "original_protocol", "gemini")?
    {
        return Err(Error::Unsupported);
    }
    // A Chat `reasoning` object maps only through its `effort`; a summary or token budget has no
    // Gemini form and is refused rather than dropped.
    if super::is(doc, x, "original_protocol", "chat")?
        && let Some(thinking) = doc.member(x, "thinking")?
        && let Some(object) = doc.member(thinking, "native")?
    {
        for f in doc.children(object)? {
            if !doc.name_is(doc.node(f)?.name.ok_or(Error::Schema)?, "effort")? {
                return Err(Error::Unsupported);
            }
        }
    }
    Ok(())
}
/// Anthropic's `top_k` is Gemini's `topK`; its other parameters have no Gemini form.
fn anthropic_parameters(doc: &Document, native: usize) -> Result<()> {
    for f in doc.children(native)? {
        if !doc.name_is(doc.node(f)?.name.ok_or(Error::Schema)?, "top_k")? {
            return Err(Error::Unsupported);
        }
        if !matches!(doc.node(f)?.value, Value::Null) {
            doc.unsigned(f)?;
        }
    }
    Ok(())
}
fn validate_part(view: RequestView<'_>, part: usize, run: Option<usize>) -> Result<()> {
    let doc = view.document;
    let kind = doc.required(part, "type")?;
    if doc.string_is(kind, "text")? || doc.string_is(kind, "reasoning")? {
        return super::string(doc, doc.required(part, "text")?);
    }
    if doc.string_is(kind, "redacted")?
        && super::is(doc, doc.required(doc.root(), "x_ck")?, "original_protocol", "anthropic")?
    {
        return Ok(());
    }
    if doc.string_is(kind, "tool_result")? {
        if doc.member(part, "result")?.is_none() {
            return tool_message(doc, part, run);
        }
        super::string(doc, doc.required(part, "tool_call_id")?)?;
        return Ok(());
    }
    if doc.string_is(kind, "opaque")? {
        return if super::is(doc, part, "protocol", "gemini")? {
            Ok(())
        } else {
            Err(Error::Unsupported)
        };
    }
    for name in ["image", "audio", "video", "document"] {
        if doc.string_is(kind, name)? {
            let source = doc.required(part, "source")?;
            if super::is(doc, source, "kind", "uri")? {
                super::string(doc, doc.required(source, "uri")?)?;
            } else if super::is(doc, source, "kind", "data")? {
                view.media
                    .get(doc, doc.required(source, "ref")?)?
                    .ok_or(Error::Position)?;
            } else {
                return Err(Error::Unsupported);
            }
            return Ok(());
        }
    }
    Err(Error::Unsupported)
}

pub(super) fn native(doc: &Document) -> Result<Option<usize>> {
    let Some(x) = doc.member(doc.root(), "x_ck")? else {
        return Ok(None);
    };
    let Some(provider) = doc.member(x, "provider_parameters")? else {
        return Ok(None);
    };
    doc.member(provider, "gemini")
}
/// Whether `message` is the system instruction; the fence admits one `system` or `developer`.
pub(super) fn system(doc: &Document, message: usize) -> Result<bool> {
    Ok(super::is(doc, message, "role", "system")? || super::is(doc, message, "role", "developer")?)
}
/// Anthropic's `top_k`, which Gemini spells `topK`; null is unset.
pub(super) fn top_k(doc: &Document, x: Option<usize>) -> Result<Option<usize>> {
    let Some(provider) = x.map(|x| doc.member(x, "provider_parameters")).transpose()?.flatten() else {
        return Ok(None);
    };
    let top_k = doc
        .member(provider, "anthropic")?
        .map(|n| doc.member(n, "top_k"))
        .transpose()?;
    Ok(top_k
        .flatten()
        .filter(|&n| !matches!(doc.node(n).map(|n| n.value), Ok(Value::Null))))
}
