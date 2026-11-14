use crate::codec::chat::request::{gemini, json_schema};
use crate::codec::{
    Error, Operation, RequestView, Result,
    document::{Document, Value},
    table::Table,
};
use std::collections::hash_map::RandomState;

pub(super) fn validate(view: RequestView<'_>, default_max_tokens: Option<u64>) -> Result<()> {
    if view.operation == Operation::Compact {
        return Err(Error::Unsupported);
    }
    let doc = view.document;
    let root = doc.root();
    let messages = parameters(doc, root, view.operation, default_max_tokens)?;
    doc.with_table(|index| {
        let hasher = RandomState::new();
        index_calls(doc, messages, index, &hasher)?;
        roles(view, messages, (index, &hasher))
    })?;
    let x = doc.required(root, "x_ck")?;
    extensions(doc, x)?;
    tools(doc, root, x)?;
    tool_choice(doc, root, x)
}
/// Checks message roles and order, then each message against the call index.
fn roles(view: RequestView<'_>, messages: usize, (index, hasher): (&CallIndex, &RandomState)) -> Result<()> {
    let doc = view.document;
    let mut conversation = false;
    let mut system = false;
    for (ordinal, message) in doc.children(messages)?.enumerate() {
        let role = doc.required(message, "role")?;
        // Encoding writes a leading `developer` message, such as Responses `instructions`, as `system`.
        if doc.string_is(role, "system")? || doc.string_is(role, "developer")? {
            if conversation || system {
                return Err(Error::Unsupported);
            }
            system = true;
        } else if doc.string_is(role, "user")? || doc.string_is(role, "assistant")? || doc.string_is(role, "tool")? {
            conversation = true;
        } else {
            return Err(Error::Unsupported);
        }
        let calls = Calls {
            index,
            hasher,
            ordinal: ordinal as u64,
        };
        self::message(view, calls, message, role)?;
    }
    Ok(())
}
/// Checks the top-level generation parameters and returns the messages array.
fn parameters(doc: &Document, root: usize, operation: Operation, default_max_tokens: Option<u64>) -> Result<usize> {
    super::string(doc, doc.required(root, "model")?)?;
    if doc.chars(doc.required(root, "model")?)?.next_scalar()?.is_none() {
        return Err(Error::Schema);
    }
    let messages = doc.required(root, "messages")?;
    if !matches!(doc.node(messages)?.value, Value::Array) {
        return Err(Error::Schema);
    }
    let completion = doc
        .member(root, "max_completion_tokens")?
        .map(|n| doc.unsigned(n))
        .transpose()?;
    if operation == Operation::Generate {
        // Encoding writes `max_completion_tokens` as `max_tokens` unless `max_tokens` is present.
        let max_tokens = match doc.member(root, "max_tokens")? {
            Some(max_tokens) => doc.unsigned(max_tokens)?,
            None => completion.or(default_max_tokens).ok_or(Error::Schema)?,
        };
        if max_tokens == 0 {
            return Err(Error::Schema);
        }
    }
    for key in ["temperature", "top_p"] {
        if let Some(n) = doc.member(root, key)?
            && !matches!(doc.node(n)?.value, Value::Number(_) | Value::Unsigned(_))
        {
            return Err(Error::Schema);
        }
    }
    if let Some(stop) = doc.member(root, "stop")? {
        if matches!(doc.node(stop)?.value, Value::Array) {
            for value in doc.children(stop)? {
                super::string(doc, value)?;
            }
        } else {
            super::string(doc, stop)?;
        }
    }
    if let Some(stream) = doc.member(root, "stream")? {
        let Value::Bool(stream) = doc.node(stream)?.value else {
            return Err(Error::Schema);
        };
        if stream && operation == Operation::CountTokens {
            return Err(Error::Unsupported);
        }
    }
    for key in [
        "n",
        "seed",
        "logprobs",
        "top_logprobs",
        "modalities",
        "audio",
        "frequency_penalty",
        "presence_penalty",
        "logit_bias",
        "prediction",
    ] {
        if doc.member(root, key)?.is_some() {
            return Err(Error::Unsupported);
        }
    }
    output(doc, root)?;
    Ok(messages)
}
/// Checks storage and the response format: Messages stores nothing, so only `store: true` asks
/// for what it cannot do, and Gemini JSON mode needs a schema, as Anthropic has no schemaless
/// JSON output.
fn output(doc: &Document, root: usize) -> Result<()> {
    if let Some(store) = doc.member(root, "store")?
        && !matches!(doc.node(store)?.value, Value::Bool(false) | Value::Null)
    {
        return Err(Error::Unsupported);
    }
    if let Some(format) = doc.member(root, "response_format")? {
        if !super::is(doc, format, "type", "json_schema")? {
            return Err(Error::Unsupported);
        }
        if json_schema::from_gemini(doc)? {
            gemini::json_mode(doc, format)?.ok_or(Error::Unsupported)?;
        } else {
            let schema = doc.member(format, "json_schema")?.ok_or(Error::Unsupported)?;
            if !matches!(doc.node(doc.required(schema, "schema")?)?.value, Value::Object) {
                return Err(Error::Schema);
            }
        }
    }
    Ok(())
}
/// Checks one message's content parts, tool result link and tool calls.
fn message(view: RequestView<'_>, calls: Calls<'_>, message: usize, role: usize) -> Result<()> {
    let doc = view.document;
    if let Some(content) = doc.member(message, "content")? {
        match doc.node(content)?.value {
            Value::String(_) | Value::Text(_) | Value::Fragments | Value::Utf8Media => (),
            Value::Array => {
                for part in doc.children(content)? {
                    validate_part(view, calls, part)?;
                }
            }
            Value::Null if doc.member(message, "tool_calls")?.is_some() => (),
            _ => return Err(Error::Schema),
        }
    }
    if doc.string_is(role, "tool")? {
        linked(doc, calls, None, doc.required(message, "tool_call_id")?)?;
    }
    if let Some(calls) = doc.member(message, "tool_calls")? {
        if !doc.string_is(role, "assistant")? || !matches!(doc.node(calls)?.value, Value::Array) {
            return Err(Error::Schema);
        }
        for call in doc.children(calls)? {
            tool_call(doc, call)?;
        }
    }
    Ok(())
}
fn tool_call(doc: &Document, call: usize) -> Result<()> {
    super::string(doc, doc.required(call, "id")?)?;
    if !super::is(doc, call, "type", "function")? && !super::is(doc, call, "type", "server")? {
        return Err(Error::Unsupported);
    }
    if let Some(namespace) = doc.member(call, "namespace")? {
        super::string(doc, namespace)?;
    }
    if !super::is(doc, call, "arguments_format", "json")? {
        return Err(Error::Unsupported);
    }
    let function = doc.required(call, "function")?;
    super::string(doc, doc.required(function, "name")?)?;
    if let Some(input) = doc.member(function, "arguments")?
        && !matches!(doc.node(input)?.value, Value::Object)
    {
        return Err(Error::Schema);
    }
    Ok(())
}
/// Checks the canonical `x_ck` extensions this destination accepts.
fn extensions(doc: &Document, x: usize) -> Result<()> {
    if doc.member(x, "session")?.is_some() {
        return Err(Error::Unsupported);
    }
    if let Some(thinking) = doc.member(x, "thinking")? {
        if doc.member(thinking, "native")?.is_some() && !super::is(doc, x, "original_protocol", "anthropic")? {
            return Err(Error::Unsupported);
        }
        if let Some(enabled) = doc.member(thinking, "enabled")?
            && !matches!(doc.node(enabled)?.value, Value::Bool(_))
        {
            return Err(Error::Schema);
        }
        if let Some(budget) = doc.member(thinking, "budget_tokens")? {
            doc.unsigned(budget)?;
        }
    }
    if let Some(replay) = doc.member(x, "replay")? {
        for entry in doc.children(replay)? {
            if !super::is(doc, entry, "kind", "anthropic")? {
                return Err(Error::Unsupported);
            }
            doc.unsigned(doc.required(entry, "message_index")?)?;
            doc.unsigned(doc.required(entry, "block_index")?)?;
            if let Some(signature) = doc.member(entry, "signature")? {
                super::string(doc, signature)?;
            }
        }
    }
    if let Some(provider) = doc.member(x, "provider_parameters")? {
        for protocol in doc.children(provider)? {
            if !doc.name_is(doc.node(protocol)?.name.ok_or(Error::Schema)?, "anthropic")?
                && doc.node(protocol)?.first.is_some()
            {
                return Err(Error::Unsupported);
            }
        }
    }
    Ok(())
}
fn tools(doc: &Document, root: usize, x: usize) -> Result<()> {
    // A Gemini native tool group, such as `googleSearch`, has no Anthropic form.
    if json_schema::from_gemini(doc)?
        && let Some(native) = doc.member(x, "tools")?
        && doc.node(native)?.first.is_some()
    {
        return Err(Error::Unsupported);
    }
    let Some(tools) = doc.member(root, "tools")? else {
        return Ok(());
    };
    if !matches!(doc.node(tools)?.value, Value::Array) {
        return Err(Error::Schema);
    }
    for tool in doc.children(tools)? {
        if super::is(doc, tool, "type", "server")? {
            let declarations = doc.required(x, "tools")?;
            if super::request_encode::declaration(doc, declarations, tool)?.is_none() {
                return Err(Error::Unsupported);
            }
        } else {
            if !super::is(doc, tool, "type", "function")? {
                return Err(Error::Unsupported);
            }
            let function = doc.required(tool, "function")?;
            super::string(doc, doc.required(function, "name")?)?;
            let gemini = json_schema::from_gemini(doc)?;
            if gemini {
                json_schema::check_function(doc, function)?;
            }
            let schema = match doc.member(function, "parameters")? {
                Some(schema) => schema,
                None if gemini => doc.required(function, "parametersJsonSchema")?,
                None => return Err(Error::Schema),
            };
            if !matches!(doc.node(schema)?.value, Value::Object) {
                return Err(Error::Schema);
            }
        }
    }
    Ok(())
}
/// The function a tool choice object forces: Chat's `function` choice, or an `allowed_tools`
/// choice that requires its only tool. Anthropic can force one tool, not choose among several.
pub(super) fn forced_function(doc: &Document, choice: usize) -> Result<usize> {
    if super::is(doc, choice, "type", "function")? {
        return doc.required(choice, "function");
    }
    if !super::is(doc, choice, "type", "allowed_tools")? {
        return Err(Error::Unsupported);
    }
    let allowed = doc.required(choice, "allowed_tools")?;
    if !super::is(doc, allowed, "mode", "required")? {
        return Err(Error::Unsupported);
    }
    let mut tools = doc.children(doc.required(allowed, "tools")?)?;
    match (tools.next(), tools.next()) {
        (Some(tool), None) => doc.required(tool, "function"),
        _ => Err(Error::Unsupported),
    }
}
fn tool_choice(doc: &Document, root: usize, x: usize) -> Result<()> {
    let Some(choice) = doc.member(root, "tool_choice")? else {
        return Ok(());
    };
    if matches!(doc.node(choice)?.value, Value::Object) {
        let function = forced_function(doc, choice)?;
        let name = doc.required(function, "name")?;
        if doc.chars(name)?.next_scalar()?.is_none() {
            return Err(Error::Schema);
        }
        let tools = doc.required(root, "tools")?;
        let mut declared = false;
        for tool in doc.children(tools)? {
            if let Some(function) = doc.member(tool, "function")? {
                declared |= doc.equal_strings(name, doc.required(function, "name")?)?;
            }
        }
        // Server tools keep their native declaration, bound to a tools entry.
        if let Some(declarations) = doc.member(x, "tools")? {
            for entry in doc.children(declarations)? {
                let bound = doc
                    .origin(entry)?
                    .is_some_and(|(tool, _)| doc.node(tool).is_ok_and(|node| node.parent == Some(tools)));
                if bound
                    && let Some(declaration) = doc.member(entry, "declaration")?
                    && let Some(server) = doc.member(declaration, "name")?
                {
                    declared |= doc.equal_strings(name, server)?;
                }
            }
        }
        if !declared {
            return Err(Error::Schema);
        }
    } else if !["auto", "none", "required"]
        .iter()
        .try_fold(false, |yes, value| Ok::<_, Error>(yes || doc.string_is(choice, value)?))?
    {
        return Err(Error::Unsupported);
    }
    // Manual thinking allows only `auto` and `none`; adaptive thinking allows a forced tool.
    let forced = matches!(doc.node(choice)?.value, Value::Object) || doc.string_is(choice, "required")?;
    if forced
        && let Some(thinking) = doc.member(x, "thinking")?
        && thinking_type(doc, thinking)? == "enabled"
    {
        return Err(Error::Unsupported);
    }
    Ok(())
}
/// The Anthropic `thinking.type` that canonical thinking stands for.
pub(super) fn thinking_type(doc: &Document, thinking: usize) -> Result<&'static str> {
    let enabled = match doc.member(thinking, "enabled")? {
        Some(n) => matches!(doc.node(n)?.value, Value::Bool(true)),
        None => false,
    };
    // ponytail: foreign thinking is adaptive without a budget and manual with one; some models
    // (claude-haiku-5-5) refuse budgets. Phase D model capability picks the form per model.
    let adaptive = match doc.member(thinking, "native")? {
        Some(native) => super::is(doc, native, "type", "adaptive")?,
        None => doc.member(thinking, "budget_tokens")?.is_none(),
    };
    Ok(if !enabled {
        "disabled"
    } else if adaptive {
        "adaptive"
    } else {
        "enabled"
    })
}
fn validate_part(view: RequestView<'_>, calls: Calls<'_>, part: usize) -> Result<()> {
    let doc = view.document;
    let kind = doc.required(part, "type")?;
    if doc.string_is(kind, "text")? || doc.string_is(kind, "reasoning")? {
        super::string(doc, doc.required(part, "text")?)?;
    } else if doc.string_is(kind, "redacted")? {
        super::string(doc, doc.required(part, "data")?)?;
    } else if doc.string_is(kind, "image")? || doc.string_is(kind, "document")? {
        let source = doc.required(part, "source")?;
        if super::is(doc, source, "kind", "data")? {
            view.media
                .get(doc, doc.required(source, "ref")?)?
                .ok_or(Error::Position)?;
        } else if super::is(doc, source, "kind", "uri")? {
            super::string(doc, doc.required(source, "uri")?)?;
        } else if super::is(doc, source, "kind", "file")? {
            super::string(doc, doc.required(source, "file_id")?)?;
        } else {
            return Err(Error::Unsupported);
        }
    } else if doc.string_is(kind, "tool_result")? {
        linked(doc, calls, Some(part), doc.required(part, "tool_call_id")?)?;
        // A Gemini function response is written as the JSON text of its result or error.
        if gemini::outcome(doc, part)?.is_some() {
            return Ok(());
        }
        if let Some(content) = doc.member(part, "content")? {
            if !matches!(doc.node(content)?.value, Value::Array) {
                return Err(Error::Schema);
            }
            for child in doc.children(content)? {
                validate_part(view, calls, child)?;
            }
        }
        if let Some(error) = doc.member(part, "is_error")?
            && !matches!(doc.node(error)?.value, Value::Bool(_))
        {
            return Err(Error::Schema);
        }
    } else if doc.string_is(kind, "opaque")? {
        if !super::is(doc, part, "protocol", "anthropic")? {
            return Err(Error::Unsupported);
        }
    } else {
        return Err(Error::Unsupported);
    }
    Ok(())
}
/// Tool calls by ID hash, then message ordinal, then call node: one sorted index, charged to
/// the document's metadata quota, links every result without rescanning history.
type CallIndex = Table<(u64, u64, usize)>;
/// The call index and the ordinal of the message being validated.
#[derive(Clone, Copy)]
struct Calls<'a> {
    index: &'a CallIndex,
    hasher: &'a RandomState,
    ordinal: u64,
}
/// Indexes every call ID and rejects a repeated one: the Anthropic API requires unique
/// `tool_use` IDs, and a repeated ID would make result linking ambiguous.
fn index_calls(doc: &Document, messages: usize, index: &mut CallIndex, hasher: &RandomState) -> Result<()> {
    for (ordinal, message) in doc.children(messages)?.enumerate() {
        if let Some(list) = doc.member(message, "tool_calls")?
            && matches!(doc.node(list)?.value, Value::Array)
        {
            for call in doc.children(list)? {
                let id = doc.required(call, "id")?;
                super::string(doc, id)?;
                index.push((string_hash(doc, id, hasher)?, ordinal as u64, call))?;
            }
        }
    }
    index.sort_unstable();
    for (at, &(hash, _, call)) in index.iter().enumerate() {
        for &(_, _, other) in index[at + 1..].iter().take_while(|entry| entry.0 == hash) {
            if doc.equal_strings(doc.required(call, "id")?, doc.required(other, "id")?)? {
                return Err(Error::Schema);
            }
        }
    }
    Ok(())
}
/// Links a result to its call, which must precede it: in an earlier message, or in the same
/// message at an earlier block for a server result.
fn linked(doc: &Document, calls: Calls<'_>, result: Option<usize>, id: usize) -> Result<()> {
    super::string(doc, id)?;
    let result_order = result
        .map(|part| super::content::history(doc, part))
        .transpose()?
        .flatten();
    let server = result_order
        .map(|entry| doc.member(entry, "server_result"))
        .transpose()?
        .flatten()
        .map(|node| doc.node(node))
        .transpose()?
        .is_some_and(|node| matches!(node.value, Value::Bool(true)));
    let (ordinal, call) = call(doc, calls, id)?.ok_or(Error::Schema)?;
    if ordinal > calls.ordinal || (ordinal == calls.ordinal && !server) {
        return Err(Error::Schema);
    }
    if server != super::is(doc, call, "type", "server")? {
        return Err(Error::Schema);
    }
    if ordinal == calls.ordinal {
        let before = super::content::history(doc, call)?.ok_or(Error::Schema)?;
        let after = result_order.ok_or(Error::Schema)?;
        if doc.unsigned(doc.required(before, "block_index")?)? >= doc.unsigned(doc.required(after, "block_index")?)? {
            return Err(Error::Schema);
        }
    }
    Ok(())
}
/// The message ordinal and node of the call whose ID equals `id`.
fn call(doc: &Document, calls: Calls<'_>, id: usize) -> Result<Option<(u64, usize)>> {
    let hash = string_hash(doc, id, calls.hasher)?;
    let first = calls.index.partition_point(|entry| entry.0 < hash);
    for &(_, ordinal, call) in calls.index[first..].iter().take_while(|entry| entry.0 == hash) {
        if doc.equal_strings(id, doc.required(call, "id")?)? {
            return Ok(Some((ordinal, call)));
        }
    }
    Ok(None)
}
fn string_hash(doc: &Document, node: usize, hasher: &RandomState) -> Result<u64> {
    use std::hash::{BuildHasher, Hasher};
    let mut state = hasher.build_hasher();
    let mut chars = doc.chars(node)?;
    while let Some(c) = chars.next_scalar()? {
        state.write_u32(c.into());
    }
    Ok(state.finish())
}
