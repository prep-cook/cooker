use super::request::originals;
use crate::boundary::chat_effort;
use crate::codec::{
    Error, RequestView, Result,
    document::{Document, Value},
    encode::Encoder,
};
use calls::{function_call, function_response, function_results};

#[path = "request_calls.rs"]
mod calls;

pub(super) fn write(view: RequestView<'_>, e: &mut Encoder<'_, '_>) -> Result<()> {
    let doc = view.document;
    let root = doc.root();
    let x = doc.member(root, "x_ck")?;
    let wrapper = x
        .map(|x| doc.member(x, "gemini_count_wrapper"))
        .transpose()?
        .flatten()
        .is_some();
    if wrapper {
        e.bytes(b"{\"generateContentRequest\":")?;
    }
    e.bytes(b"{\"contents\":[")?;
    let messages = doc.required(root, "messages")?;
    let mut first = true;
    // Consecutive Chat tool messages answer the preceding model turn's calls in one user turn;
    // `run` is that turn's first message and `open` whether the next message joins the turn.
    let (mut run, mut results, mut open) = (None, false, false);
    for message in doc.children(messages)? {
        if super::request_schema::system(doc, message)? {
            continue;
        }
        let tool = super::is(doc, message, "role", "tool")?;
        if results && !tool {
            e.bytes(b"]}")?;
        }
        if !(results && tool || open) {
            comma(&mut first, e)?;
        }
        if tool && !results {
            e.bytes(b"{\"role\":\"user\",\"parts\":[")?;
            function_results(doc, message, run, e)?;
        } else if !tool {
            let joined = open;
            open = write_message(view, message, false, run, joined, e)?;
            if !joined {
                run = Some(message);
            }
        }
        results = tool;
    }
    e.bytes(if results { b"]}]" } else { b"]" })?;
    for message in doc.children(messages)? {
        if super::request_schema::system(doc, message)? {
            e.bytes(b",\"systemInstruction\":")?;
            write_message(view, message, true, None, false, e)?;
        }
    }
    let native = super::request_schema::native(doc)?;
    config(doc, x, native, e)?;
    tool_declarations(doc, x, e)?;
    tool_config(doc, native, e)?;
    if !wrapper {
        originals(doc, root, None, e)?;
    }
    for entry in doc.origins(root)? {
        if doc
            .origin(entry)?
            .is_some_and(|(_, carrier)| carrier == Some("body_model"))
        {
            e.bytes(b",\"model\":")?;
            doc.write(doc.required(entry, "value")?, e)?;
        }
    }
    e.bytes(b"}")?;
    if wrapper {
        originals(doc, root, None, e)?;
        e.bytes(b"}")?;
    }
    Ok(())
}
/// Writes `,"generationConfig":{...}` when mapped controls or a native config exist.
fn config(doc: &Document, x: Option<usize>, native: Option<usize>, e: &mut Encoder<'_, '_>) -> Result<()> {
    let root = doc.root();
    let mut native_config = native.map(|n| doc.member(n, "generationConfig")).transpose()?.flatten();
    // A native null config returns as null unless mapped controls now fill the config.
    let null_config = match native_config {
        Some(n) => matches!(doc.node(n)?.value, Value::Null),
        None => false,
    };
    if null_config {
        native_config = None;
    }
    let thinking = x.map(|x| doc.member(x, "thinking")).transpose()?.flatten();
    let top_k = super::request_schema::top_k(doc, x)?;
    let config_present = native_config.is_some()
        || thinking.is_some()
        || top_k.is_some()
        || doc.member(root, "response_format")?.is_some()
        || doc.member(root, "max_completion_tokens")?.is_some()
        || super::request::config().iter().try_fold(false, |yes, (_, key)| {
            Ok::<_, Error>(yes || doc.member(root, key)?.is_some())
        })?;
    if config_present {
        e.bytes(b",\"generationConfig\":{")?;
        generation_config(doc, (thinking, top_k), native_config, e)?;
        e.bytes(b"}")?;
    } else if null_config {
        e.bytes(b",\"generationConfig\":null")?;
    } else if x
        .map(|x| doc.member(x, "gemini_empty_config"))
        .transpose()?
        .flatten()
        .is_some()
    {
        e.bytes(b",\"generationConfig\":{}")?;
    }
    Ok(())
}
/// Writes the members of `generationConfig`: mapped controls, Anthropic `top_k`, response schema,
/// thinking and the native config, comma-separated.
fn generation_config(
    doc: &Document,
    (thinking, top_k): (Option<usize>, Option<usize>),
    native_config: Option<usize>,
    e: &mut Encoder<'_, '_>,
) -> Result<()> {
    let root = doc.root();
    let mut first = true;
    for &(wire, canonical) in super::request::config() {
        if let Some(n) = doc.member(root, canonical)? {
            comma(&mut first, e)?;
            e.string(wire)?;
            e.bytes(b":")?;
            if canonical == "modalities" {
                modalities(doc, n, e)?;
            } else if canonical == "stop" && super::string(doc, n).is_ok() {
                e.bytes(b"[")?;
                doc.write(n, e)?;
                e.bytes(b"]")?;
            } else {
                doc.write(n, e)?;
            }
        }
    }
    if doc.member(root, "max_tokens")?.is_none()
        && let Some(n) = doc.member(root, "max_completion_tokens")?
    {
        comma(&mut first, e)?;
        e.bytes(b"\"maxOutputTokens\":")?;
        doc.write(n, e)?;
    }
    if let Some(n) = top_k {
        comma(&mut first, e)?;
        e.bytes(b"\"topK\":")?;
        doc.write(n, e)?;
    }
    if let Some(format) = doc.member(root, "response_format")? {
        if super::is(doc, format, "type", "json_object")? {
            comma(&mut first, e)?;
            e.bytes(b"\"responseMimeType\":\"application/json\"")?;
        }
        for key in ["responseMimeType", "responseSchema", "responseJsonSchema"] {
            if let Some(n) = doc.member(format, key)? {
                comma(&mut first, e)?;
                e.string(key)?;
                e.bytes(b":")?;
                doc.write(n, e)?;
            }
        }
        if let Some(schema) = doc.member(format, "json_schema")? {
            comma(&mut first, e)?;
            e.bytes(b"\"responseMimeType\":\"application/json\",\"responseJsonSchema\":")?;
            let schema = doc.member(schema, "schema")?.unwrap_or(schema);
            doc.write(schema, e)?;
        }
    }
    if let Some(thinking) = thinking {
        comma(&mut first, e)?;
        thinking_config(doc, thinking, e)?;
    }
    if let Some(config) = native_config {
        for n in doc.children(config)? {
            comma(&mut first, e)?;
            doc.write_name(doc.node(n)?.name.ok_or(Error::Schema)?, e)?;
            e.bytes(b":")?;
            doc.write(n, e)?;
        }
    }
    Ok(())
}
/// Writes the modalities array in Gemini's upper-case spelling. Schema validation admits only
/// ASCII modality names.
fn modalities(doc: &Document, list: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"[")?;
    let mut first = true;
    for modality in doc.children(list)? {
        comma(&mut first, e)?;
        e.bytes(b"\"")?;
        let mut chars = doc.chars(modality)?;
        while let Some(c) = chars.next_scalar()? {
            e.escaped(c.to_ascii_uppercase().encode_utf8(&mut [0; 4]))?;
        }
        e.bytes(b"\"")?;
    }
    e.bytes(b"]")
}
/// Writes `"thinkingConfig":{...}` from the mapped controls and, for a Gemini source, the native
/// thinking config; another protocol's native members are not Gemini fields.
fn thinking_config(doc: &Document, thinking: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"\"thinkingConfig\":{")?;
    let mut first = true;
    for (canonical, wire) in [
        ("budget_tokens", "thinkingBudget"),
        ("include", "includeThoughts"),
        ("effort", "thinkingLevel"),
    ] {
        if let Some(n) = doc.member(thinking, canonical)? {
            comma(&mut first, e)?;
            e.string(wire)?;
            e.bytes(b":")?;
            doc.write(n, e)?;
        }
    }
    let disabled = match doc.member(thinking, "enabled")? {
        Some(n) => matches!(doc.node(n)?.value, Value::Bool(false)),
        None => false,
    };
    let enabled = match doc.member(thinking, "enabled")? {
        Some(n) => matches!(doc.node(n)?.value, Value::Bool(true)),
        None => false,
    };
    if (disabled || enabled) && doc.member(thinking, "budget_tokens")?.is_none() {
        // Thinking on without a budget is the dynamic budget, -1; Gemini refuses a budget
        // beside `thinkingLevel`.
        if disabled || doc.member(thinking, "effort")?.is_none() {
            comma(&mut first, e)?;
            e.bytes(if disabled {
                b"\"thinkingBudget\":0"
            } else {
                b"\"thinkingBudget\":-1"
            })?;
        }
    }
    let x = doc.required(doc.root(), "x_ck")?;
    if let Some(effort) = chat_effort(doc, x, thinking)? {
        comma(&mut first, e)?;
        e.bytes(b"\"thinkingLevel\":")?;
        doc.write(effort, e)?;
    }
    if let Some(native) = doc.member(thinking, "native")?
        && super::is(doc, x, "original_protocol", "gemini")?
    {
        for n in doc.children(native)? {
            comma(&mut first, e)?;
            doc.write_name(doc.node(n)?.name.ok_or(Error::Schema)?, e)?;
            e.bytes(b":")?;
            doc.write(n, e)?;
        }
    }
    e.bytes(b"}")
}
/// Writes `,"tools":[...]`: Chat function declarations, then native tool groups.
fn tool_declarations(doc: &Document, x: Option<usize>, e: &mut Encoder<'_, '_>) -> Result<()> {
    let Some(tools) = doc.member(doc.root(), "tools")? else {
        return Ok(());
    };
    e.bytes(b",\"tools\":[")?;
    let mut first = true;
    if doc.node(tools)?.first.is_some() {
        e.bytes(b"{\"functionDeclarations\":[")?;
        let mut first_function = true;
        for tool in doc.children(tools)? {
            comma(&mut first_function, e)?;
            doc.write(doc.required(tool, "function")?, e)?;
        }
        e.bytes(b"]}")?;
        first = false;
    }
    if let Some(native) = x.map(|x| doc.member(x, "tools")).transpose()?.flatten() {
        for tool in doc.children(native)? {
            comma(&mut first, e)?;
            doc.write(tool, e)?;
        }
    }
    e.bytes(b"]")
}
/// Writes the native members other than `generationConfig`, then `,"toolConfig":...` for a
/// canonical `tool_choice`, sharing the object with a native `toolConfig` object's members.
fn tool_config(doc: &Document, provider: Option<usize>, e: &mut Encoder<'_, '_>) -> Result<()> {
    let native = match provider.map(|n| doc.member(n, "toolConfig")).transpose()?.flatten() {
        Some(n) if matches!(doc.node(n)?.value, Value::Object) => Some(n),
        _ => None,
    };
    if let Some(provider) = provider {
        let skip: &[&str] = if native.is_some() {
            &["generationConfig", "toolConfig"]
        } else {
            &["generationConfig"]
        };
        super::members(doc, provider, skip, e)?;
    }
    let choice = doc.member(doc.root(), "tool_choice")?;
    if choice.is_none() && native.is_none() {
        return Ok(());
    }
    e.bytes(b",\"toolConfig\":{")?;
    let mut first = true;
    if let Some(choice) = choice {
        first = false;
        function_calling(doc, choice, e)?;
    }
    if let Some(native) = native {
        for n in doc.children(native)? {
            comma(&mut first, e)?;
            doc.write_name(doc.node(n)?.name.ok_or(Error::Schema)?, e)?;
            e.bytes(b":")?;
            doc.write(n, e)?;
        }
    }
    e.bytes(b"}")
}
/// Writes `"functionCallingConfig":{...}` for a canonical `tool_choice`.
fn function_calling(doc: &Document, choice: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"\"functionCallingConfig\":{")?;
    if matches!(doc.node(choice)?.value, Value::Object) {
        e.bytes(b"\"mode\":\"ANY\",\"allowedFunctionNames\":[")?;
        match doc.member(choice, "allowed_tools")? {
            Some(allowed) => {
                for (i, tool) in doc.children(doc.required(allowed, "tools")?)?.enumerate() {
                    e.bytes(if i == 0 { b"" } else { b"," })?;
                    doc.write(doc.required(doc.required(tool, "function")?, "name")?, e)?;
                }
            }
            None => doc.write(doc.required(doc.required(choice, "function")?, "name")?, e)?,
        }
        e.bytes(b"]")?;
    } else {
        e.bytes(b"\"mode\":")?;
        e.string(if doc.string_is(choice, "none")? {
            "NONE"
        } else if doc.string_is(choice, "required")? {
            "ANY"
        } else if doc.string_is(choice, "auto")? {
            "AUTO"
        } else {
            return Err(Error::Unsupported);
        })?;
    }
    e.bytes(b"}")
}
fn comma(first: &mut bool, e: &mut Encoder<'_, '_>) -> Result<()> {
    if !*first {
        e.bytes(b",")?;
    }
    *first = false;
    Ok(())
}
/// Writes a content turn, or with `joined` adds the parts to the open one; `previous` is the
/// model turn its tool results answer. Returns whether the next message joins this turn.
fn write_message(
    view: RequestView<'_>,
    message: usize,
    system: bool,
    previous: Option<usize>,
    joined: bool,
    e: &mut Encoder<'_, '_>,
) -> Result<bool> {
    let doc = view.document;
    if !joined {
        head(doc, message, system, e)?;
    }
    let mut first = !joined;
    // A null content beside tool calls carries no text.
    let content = match doc.member(message, "content")? {
        Some(n) if matches!(doc.node(n)?.value, Value::Null) => None,
        content => content,
    };
    let calls = doc.member(message, "tool_calls")?;
    let order = super::origin(doc, message, "parts")?;
    if let Some(order) = order {
        for entry in doc.children(doc.required(order, "parts")?)? {
            let (part, _) = doc.origin(entry)?.ok_or(Error::Schema)?;
            if doc.node(part)?.parent != content && doc.node(part)?.parent != calls {
                continue;
            }
            comma(&mut first, e)?;
            write_part(view, part, Some(entry), e)?;
        }
    }
    // Claude thinking history is stripped, as at a Chat upstream (owner ruling 2026-11-15).
    let claude = super::is(doc, doc.required(doc.root(), "x_ck")?, "original_protocol", "anthropic")?;
    if let Some(content) = content {
        if matches!(doc.node(content)?.value, Value::Array) {
            // A block of results is written at its first part, ordered by call.
            let mut block = false;
            for part in doc.children(content)? {
                let result = super::request_schema::is_result(doc, part)?;
                let written = result && block;
                block = result;
                if written
                    || super::origin(doc, part, "part")?.is_some()
                    || (claude
                        && (super::is(doc, part, "type", "reasoning")? || super::is(doc, part, "type", "redacted")?))
                {
                    continue;
                }
                comma(&mut first, e)?;
                if result {
                    function_results(doc, part, previous, e)?;
                } else {
                    write_part(view, part, None, e)?;
                }
            }
        } else {
            comma(&mut first, e)?;
            e.bytes(b"{\"text\":")?;
            doc.write(content, e)?;
            e.bytes(b"}")?;
        }
    }
    if let Some(calls) = calls {
        for call in doc.children(calls)? {
            if super::origin(doc, call, "part")?.is_some() {
                continue;
            }
            comma(&mut first, e)?;
            write_part(view, call, None, e)?;
        }
    }
    if super::request_schema::next_in_turn(doc, message)?.is_some() {
        return Ok(true);
    }
    // Only a Gemini client's turns carry Gemini originals, and those are never joined.
    e.bytes(b"]")?;
    originals(doc, message, None, e)?;
    e.bytes(b"}")?;
    Ok(false)
}
/// Writes `{"role":...,"parts":[` of a content turn; the source spelling wins, and a recorded
/// null role was absent.
fn head(doc: &Document, message: usize, system: bool, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"{")?;
    if let Some(entry) = super::origin(doc, message, "role")? {
        let role = doc.required(entry, "value")?;
        if !matches!(doc.node(role)?.value, Value::Null) {
            e.bytes(b"\"role\":")?;
            doc.write(role, e)?;
            e.bytes(b",")?;
        }
    } else if !system {
        e.bytes(b"\"role\":")?;
        e.string(if super::is(doc, message, "role", "assistant")? {
            "model"
        } else {
            "user"
        })?;
        e.bytes(b",")?;
    }
    e.bytes(b"\"parts\":[")
}
fn write_part(view: RequestView<'_>, part: usize, order: Option<usize>, e: &mut Encoder<'_, '_>) -> Result<()> {
    let doc = view.document;
    let kind = doc.required(part, "type")?;
    let flag = |name| -> Result<bool> { Ok(order.map(|n| doc.member(n, name)).transpose()?.flatten().is_some()) };
    e.bytes(b"{")?;
    let mut first = false;
    if doc.string_is(kind, "function")? {
        function_call(doc, part, flag("synthetic_id")?, e)?;
    } else if doc.string_is(kind, "tool_result")? {
        function_response(doc, part, flag("synthetic_id")?, e)?;
    } else if doc.string_is(kind, "text")? || doc.string_is(kind, "reasoning")? {
        first = flag("signature_only")?;
        if !first {
            e.bytes(b"\"text\":")?;
            doc.write(doc.required(part, "text")?, e)?;
            if doc.string_is(kind, "reasoning")? {
                e.bytes(b",\"thought\":true")?;
            }
        }
    } else if doc.string_is(kind, "opaque")? {
        first = true;
        let value = doc.required(part, "value")?;
        for n in doc.children(value)? {
            comma(&mut first, e)?;
            doc.write_name(doc.node(n)?.name.ok_or(Error::Schema)?, e)?;
            e.bytes(b":")?;
            doc.write(n, e)?;
        }
    } else {
        media(view, part, e)?;
    }
    for entry in doc.origins(part)? {
        if doc
            .origin(entry)?
            .is_some_and(|(_, carrier)| carrier == Some("signature"))
        {
            comma(&mut first, e)?;
            e.bytes(b"\"thoughtSignature\":")?;
            doc.write(doc.required(entry, "signature")?, e)?;
        }
    }
    // Original members are comma-prefixed; an empty native part needs a separate first member.
    for entry in doc.origins(part)? {
        if doc.origin(entry)?.is_some_and(|(_, carrier)| carrier.is_none())
            && super::is(doc, entry, "protocol", "gemini")?
        {
            let pointer = doc.required(entry, "pointer")?;
            let Value::String(text) = doc.node(pointer)?.value else {
                return Err(Error::Schema);
            };
            if doc.string_is(kind, "reasoning")? && doc.string_is(pointer, "/thought")? {
                continue;
            }
            comma(&mut first, e)?;
            doc.write_name(doc.pointer_name(text)?, e)?;
            e.bytes(b":")?;
            doc.write(doc.required(entry, "value")?, e)?;
        }
    }
    e.bytes(b"}")
}
/// Writes `"inlineData":{...}` from a registered payload or `"fileData":{...}` from a URI.
fn media(view: RequestView<'_>, part: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let doc = view.document;
    let source = doc.required(part, "source")?;
    let inline = super::is(doc, source, "kind", "data")?;
    e.bytes(if inline { b"\"inlineData\":{" } else { b"\"fileData\":{" })?;
    e.bytes(if inline { b"\"data\":" } else { b"\"fileUri\":" })?;
    if inline {
        view.media.write_base64(doc, doc.required(source, "ref")?, e)?;
    } else {
        doc.write(doc.required(source, "uri")?, e)?;
    }
    if let Some(mime) = doc.member(part, "mime")? {
        e.bytes(b",\"mimeType\":")?;
        doc.write(mime, e)?;
    }
    originals(doc, source, None, e)?;
    e.bytes(b"}")
}
