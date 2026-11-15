use super::request::require_string;
use crate::codec::{
    Error, Result,
    document::{Document, Value},
    media::Registry,
};

pub(super) fn validate(doc: &Document, media: &Registry) -> Result<()> {
    media.validate_document(doc)?;
    media.validate_registrations(doc, 0)?;
    let root = doc.root();
    options(doc, root)?;
    optional(doc, root, "tools", tools)?;
    optional(doc, root, "tool_choice", tool_choice)?;
    optional(doc, root, "prediction", prediction)?;
    messages(doc, media, root)?;
    x_ck(doc, root)
}

/// The documented OpenAI Chat bounds, checked at the Chat client boundary only. A Chat
/// destination may carry another protocol's valid values, and Chat-compatible upstreams such
/// as GLM define their own ranges.
pub(super) fn client(doc: &Document) -> Result<()> {
    let root = doc.root();
    if doc.children(doc.required(root, "messages")?)?.next().is_none() {
        return Err(Error::Schema);
    }
    for (key, min, max) in [
        ("temperature", 0, 2),
        ("top_p", 0, 1),
        ("presence_penalty", -2, 2),
        ("frequency_penalty", -2, 2),
        ("top_logprobs", 0, 20),
    ] {
        optional(doc, root, key, |doc, node| within(doc, node, min, max))?;
    }
    optional(doc, root, "logit_bias", |doc, node| {
        doc.children(node)?.try_for_each(|value| within(doc, value, -100, 100))
    })?;
    optional(doc, root, "stop", |doc, node| {
        let many = matches!(doc.node(node)?.value, Value::Array) && doc.children(node)?.nth(4).is_some();
        if many { Err(Error::Schema) } else { Ok(()) }
    })?;
    optional(doc, root, "modalities", |doc, node| {
        doc.children(node)?.try_for_each(|value| {
            if doc.string_is(value, "text")? || doc.string_is(value, "audio")? {
                Ok(())
            } else {
                Err(Error::Schema)
            }
        })
    })
}

/// Checks `min <= value <= max` exactly over the mapped number, in fixed memory whatever the
/// length of its spelling.
fn within(doc: &Document, node: usize, min: i64, max: i64) -> Result<()> {
    // Client JSON numbers are always `Number`; `Unsigned` is set only by codecs.
    let Value::Number(text) = doc.node(node)?.value else {
        return Err(Error::Schema);
    };
    let mut cursor = doc.source(text)?.cursor(text.span)?;
    let value = Decimal::read(|| cursor.next_byte())?;
    if value.compare(min).is_ge() && value.compare(max).is_le() {
        Ok(())
    } else {
        Err(Error::Schema)
    }
}

/// What one pass over a JSON number keeps to compare it exactly with an integer of at most
/// three digits: its sign, the decimal exponent of its first significant digit, that digit and
/// the next two, and whether any later significant digit is nonzero.
#[derive(Default)]
struct Decimal {
    negative: bool,
    digits: [u8; 3],
    significant: u64,
    rest: bool,
    lead: i64,
}

impl Decimal {
    fn read(mut next: impl FnMut() -> Result<Option<u8>>) -> Result<Self> {
        let mut value = Self::default();
        let (mut integer, mut zeros, mut fraction) = (0i64, 0i64, false);
        let (mut exponent, mut exponent_part, mut exponent_negative) = (0i64, false, false);
        while let Some(byte) = next()? {
            match byte {
                b'-' if exponent_part => exponent_negative = true,
                b'-' => value.negative = true,
                b'.' => fraction = true,
                b'e' | b'E' => exponent_part = true,
                b'+' => {}
                b'0'..=b'9' if exponent_part => {
                    exponent = exponent.saturating_mul(10).saturating_add(i64::from(byte - b'0'));
                }
                b'0'..=b'9' => {
                    integer += i64::from(!fraction);
                    let digit = byte - b'0';
                    if value.significant == 0 && digit == 0 {
                        zeros += 1;
                        continue;
                    }
                    match value.digits.get_mut(value.significant as usize) {
                        Some(slot) => *slot = digit,
                        None => value.rest |= digit != 0,
                    }
                    value.significant += 1;
                }
                _ => return Err(Error::Schema),
            }
        }
        let exponent = if exponent_negative { -exponent } else { exponent };
        value.lead = (integer - 1 - zeros).saturating_add(exponent);
        Ok(value)
    }

    fn compare(&self, bound: i64) -> std::cmp::Ordering {
        use std::cmp::Ordering::{Equal, Greater, Less};
        if self.significant == 0 {
            return 0.cmp(&bound);
        }
        match (self.negative, bound < 0) {
            (false, true) => return Greater,
            (true, false) => return Less,
            _ => {}
        }
        let magnitude = bound.unsigned_abs();
        let order = if magnitude == 0 {
            Greater
        } else {
            let lead = magnitude.ilog10() as usize;
            let bound_digits = [100, 10, 1].map(|unit| (magnitude * 10u64.pow(2 - lead as u32) / unit % 10) as u8);
            (self.lead.cmp(&(lead as i64)))
                .then_with(|| self.digits.cmp(&bound_digits))
                .then(if self.rest { Greater } else { Equal })
        };
        if self.negative { order.reverse() } else { order }
    }
}

fn options(doc: &Document, root: usize) -> Result<()> {
    for key in ["temperature", "top_p", "presence_penalty", "frequency_penalty"] {
        optional(doc, root, key, number)?;
    }
    for key in ["max_tokens", "max_completion_tokens", "n", "top_logprobs"] {
        optional(doc, root, key, |doc, node| doc.unsigned(node).map(|_| ()))?;
    }
    for key in ["stream", "parallel_tool_calls", "logprobs", "store"] {
        optional(doc, root, key, boolean)?;
    }
    for key in ["user", "safety_identifier", "prompt_cache_key", "service_tier"] {
        optional(doc, root, key, require_string)?;
    }
    optional(doc, root, "seed", integer)?;
    optional(doc, root, "metadata", object)?;
    optional(doc, root, "logit_bias", |doc, node| {
        object(doc, node)?;
        for value in doc.children(node)? {
            number(doc, value)?;
        }
        Ok(())
    })?;
    optional(doc, root, "stop", |doc, node| {
        if matches!(doc.node(node)?.value, Value::Array) {
            for value in doc.children(node)? {
                require_string(doc, value)?;
            }
            Ok(())
        } else {
            require_string(doc, node)
        }
    })?;
    optional(doc, root, "stream_options", |doc, node| {
        object(doc, node)?;
        optional(doc, node, "include_usage", boolean)?;
        optional(doc, node, "include_obfuscation", boolean)
    })?;
    optional(doc, root, "modalities", |doc, node| {
        array(doc, node)?;
        for value in doc.children(node)? {
            enumeration(doc, value, &["text", "audio", "image"])?;
        }
        Ok(())
    })?;
    optional(doc, root, "audio", |doc, node| {
        object(doc, node)?;
        require_string(doc, doc.required(node, "voice")?)?;
        require_string(doc, doc.required(node, "format")?)
    })?;
    optional(doc, root, "response_format", response_format)?;
    Ok(())
}

fn tools(doc: &Document, tools: usize) -> Result<()> {
    array(doc, tools)?;
    for tool in doc.children(tools)? {
        let kind = doc.required(tool, "type")?;
        require_string(doc, kind)?;
        if !doc.string_is(kind, "function")? && !doc.string_is(kind, "custom")? {
            continue;
        }
        let custom = doc.string_is(kind, "custom")?;
        let declaration = doc.required(tool, if custom { "custom" } else { "function" })?;
        object(doc, declaration)?;
        require_string(doc, doc.required(declaration, "name")?)?;
        optional(doc, declaration, "description", require_string)?;
        if custom {
            optional(doc, declaration, "format", |doc, format| {
                object(doc, format)?;
                let kind = doc.required(format, "type")?;
                enumeration(doc, kind, &["text", "grammar"])?;
                if doc.string_is(kind, "grammar")? {
                    let grammar = doc.required(format, "grammar")?;
                    object(doc, grammar)?;
                    enumeration(doc, doc.required(grammar, "syntax")?, &["lark", "regex"])?;
                    require_string(doc, doc.required(grammar, "definition")?)?;
                }
                Ok(())
            })?;
        } else {
            optional(doc, declaration, "parameters", schema)?;
            optional(doc, declaration, "strict", boolean)?;
        }
    }
    Ok(())
}

fn tool_choice(doc: &Document, choice: usize) -> Result<()> {
    if !matches!(doc.node(choice)?.value, Value::Object) {
        return enumeration(doc, choice, &["auto", "none", "required"]);
    }
    let kind = doc.required(choice, "type")?;
    if doc.string_is(kind, "allowed_tools")? {
        let Some(allowed) = doc.member(choice, "allowed_tools")? else {
            return flat_allowed(doc, choice);
        };
        object(doc, allowed)?;
        enumeration(doc, doc.required(allowed, "mode")?, &["auto", "required"])?;
        let list = doc.required(allowed, "tools")?;
        array(doc, list)?;
        for tool in doc.children(list)? {
            declared(doc, tool)?;
        }
        return Ok(());
    }
    declared(doc, choice)
}

/// A Responses request's flat allowed-tools choice, which Chat egress writes nested. A member
/// or listed tool without a Chat equivalent, such as a hosted tool, is unsupported.
fn flat_allowed(doc: &Document, choice: usize) -> Result<()> {
    only(doc, choice, &["type", "mode", "tools"])?;
    enumeration(doc, doc.required(choice, "mode")?, &["auto", "required"])?;
    let list = doc.required(choice, "tools")?;
    array(doc, list)?;
    for tool in doc.children(list)? {
        object(doc, tool)?;
        let kind = doc.required(tool, "type")?;
        require_string(doc, kind)?;
        if !doc.string_is(kind, "function")? && !doc.string_is(kind, "custom")? {
            return Err(Error::Unsupported);
        }
        only(doc, tool, &["type", "name"])?;
        let chosen = doc.required(tool, "name")?;
        require_string(doc, chosen)?;
        listed(doc, kind, chosen)?;
    }
    Ok(())
}

/// Refuses a member of object `node` outside `names` as unsupported.
pub(crate) fn only(doc: &Document, node: usize, names: &[&str]) -> Result<()> {
    for field in doc.children(node)? {
        let name = doc.node(field)?.name.ok_or(Error::Schema)?;
        let mut known = false;
        for allowed in names {
            known |= doc.name_is(name, allowed)?;
        }
        if !known {
            return Err(Error::Unsupported);
        }
    }
    Ok(())
}

/// Checks that `choice` names one declared function or custom tool.
fn declared(doc: &Document, choice: usize) -> Result<()> {
    let kind = doc.required(choice, "type")?;
    enumeration(doc, kind, &["function", "custom"])?;
    let name = if doc.string_is(kind, "custom")? {
        "custom"
    } else {
        "function"
    };
    let chosen = doc.required(doc.required(choice, name)?, "name")?;
    require_string(doc, chosen)?;
    listed(doc, kind, chosen)
}

/// Checks that a tool of type `kind` named `chosen` is declared in `tools`.
fn listed(doc: &Document, kind: usize, chosen: usize) -> Result<()> {
    let name = if doc.string_is(kind, "custom")? {
        "custom"
    } else {
        "function"
    };
    let tools = doc.required(doc.root(), "tools")?;
    array(doc, tools)?;
    for tool in doc.children(tools)? {
        if doc.equal_strings(kind, doc.required(tool, "type")?)?
            && doc.equal_strings(chosen, doc.required(doc.required(tool, name)?, "name")?)?
        {
            return Ok(());
        }
    }
    Err(Error::Schema)
}

fn prediction(doc: &Document, prediction: usize) -> Result<()> {
    object(doc, prediction)?;
    enumeration(doc, doc.required(prediction, "type")?, &["content"])?;
    let content = doc.required(prediction, "content")?;
    if matches!(doc.node(content)?.value, Value::Array) {
        for part in doc.children(content)? {
            enumeration(doc, doc.required(part, "type")?, &["text"])?;
            require_string(doc, doc.required(part, "text")?)?;
        }
        Ok(())
    } else {
        require_string(doc, content)
    }
}

fn messages(doc: &Document, media: &Registry, root: usize) -> Result<()> {
    for message in doc.children(doc.required(root, "messages")?)? {
        if let Some(calls) = doc.member(message, "tool_calls")? {
            for call in doc.children(calls)? {
                enumeration(doc, doc.required(call, "type")?, &["function", "custom"])?;
            }
        }
        if let Some(content) = doc.member(message, "content")? {
            if matches!(doc.node(content)?.value, Value::Array) {
                for part in doc.children(content)? {
                    content_part(doc, media, part)?;
                }
            } else if matches!(doc.node(content)?.value, Value::Object) && media.get(doc, content)?.is_none() {
                return Err(Error::Schema);
            }
        }
    }
    Ok(())
}

fn x_ck(doc: &Document, root: usize) -> Result<()> {
    let x = doc.required(root, "x_ck")?;
    optional(doc, x, "thinking", |doc, thinking| {
        object(doc, thinking)?;
        optional(doc, thinking, "effort", require_string)?;
        optional(doc, thinking, "enabled", boolean)?;
        // Responses `summary` arrives as a string; Chat has no summary setting.
        optional(doc, thinking, "include", |doc, node| match doc.node(node)?.value {
            Value::Bool(_) => Ok(()),
            _ => Err(Error::Unsupported),
        })?;
        optional(doc, thinking, "budget_tokens", |doc, node| {
            doc.unsigned(node).map(|_| ())
        })?;
        optional(doc, thinking, "native", object)
    })?;
    for key in ["session", "provider_parameters"] {
        optional(doc, x, key, object)?;
    }
    optional(doc, x, "cache_control", |doc, list| {
        array(doc, list)?;
        for entry in doc.children(list)? {
            require_string(doc, doc.required(entry, "pointer")?)?;
            if let Some(value) = doc.member(entry, "value")?
                && !matches!(doc.node(value)?.value, Value::Null)
            {
                object(doc, value)?;
                optional(doc, value, "type", require_string)?;
                optional(doc, value, "ttl", require_string)?;
            }
        }
        Ok(())
    })?;
    optional(doc, x, "replay", |doc, list| {
        array(doc, list)?;
        for entry in doc.children(list)? {
            doc.unsigned(doc.required(entry, "message_index")?)?;
            doc.unsigned(doc.required(entry, "block_index")?)?;
            for key in ["item_id", "kind"] {
                require_string(doc, doc.required(entry, key)?)?;
            }
            // Anthropic ingress records its source as `{protocol}`.
            let source = doc.required(entry, "source")?;
            if !matches!(doc.node(source)?.value, Value::Object) {
                require_string(doc, source)?;
            }
            optional(doc, entry, "signature", require_string)?;
        }
        Ok(())
    })?;
    Ok(())
}

fn content_part(doc: &Document, media: &Registry, part: usize) -> Result<()> {
    let kind = doc.required(part, "type")?;
    for text in ["text", "reasoning", "reasoning_summary", "refusal"] {
        if doc.string_is(kind, text)? {
            return require_string(doc, doc.required(part, "text")?);
        }
    }
    if doc.string_is(kind, "redacted")? || doc.string_is(kind, "encrypted")? {
        require_string(doc, doc.required(part, "data")?)?;
        let index = usize::try_from(doc.unsigned(doc.required(part, "replay_index")?)?).map_err(|_| Error::Schema)?;
        let x = doc.required(doc.root(), "x_ck")?;
        let replay = doc.required(x, "replay")?;
        let entry = doc.children(replay)?.nth(index).ok_or(Error::Schema)?;
        // Anthropic counts wire blocks, tool_use included; its own fence checks the binding.
        if doc.string_is(doc.required(entry, "kind")?, "anthropic")? {
            return Ok(());
        }
        let content = doc.node(part)?.parent.ok_or(Error::Schema)?;
        let message = doc.node(content)?.parent.ok_or(Error::Schema)?;
        let messages = doc.required(doc.root(), "messages")?;
        let message_index = doc
            .children(messages)?
            .position(|n| n == message)
            .ok_or(Error::Schema)?;
        let block_index = doc.children(content)?.position(|n| n == part).ok_or(Error::Schema)?;
        if doc.unsigned(doc.required(entry, "message_index")?)? != message_index as u64
            || doc.unsigned(doc.required(entry, "block_index")?)? != block_index as u64
        {
            return Err(Error::Schema);
        }
        return Ok(());
    }
    if doc.string_is(kind, "tool_result")? {
        return tool_result(doc, part);
    }
    if doc.string_is(kind, "opaque")? {
        enumeration(doc, doc.required(part, "protocol")?, &["chat"])?;
        doc.required(part, "value")?;
        return Ok(());
    }
    enumeration(doc, kind, &["image", "audio", "document"])?;
    let source = doc.required(part, "source")?;
    object(doc, source)?;
    let source_kind = doc.required(source, "kind")?;
    if doc.string_is(source_kind, "uri")? {
        if !doc.string_is(kind, "image")? {
            return Err(Error::Unsupported);
        }
        require_string(doc, doc.required(source, "uri")?)?;
    } else if doc.string_is(source_kind, "file")? {
        if !doc.string_is(kind, "document")? {
            return Err(Error::Unsupported);
        }
        require_string(doc, doc.required(source, "file_id")?)?;
    } else if doc.string_is(source_kind, "data")? {
        let reference = doc.required(source, "ref")?;
        media.get(doc, reference)?.ok_or(Error::Position)?;
    } else {
        return Err(Error::Unsupported);
    }
    for key in ["mime", "filename", "transcript"] {
        optional(doc, part, key, require_string)?;
    }
    optional(doc, part, "detail", |doc, node| {
        enumeration(doc, node, &["auto", "low", "high"])
    })?;
    optional(doc, part, "timing", |doc, timing| {
        object(doc, timing)?;
        number(doc, doc.required(timing, "start")?)?;
        optional(doc, timing, "end", number)?;
        enumeration(doc, doc.required(timing, "unit")?, &["seconds", "milliseconds"])
    })
}

/// A tool result in a user turn, which request egress writes as a `tool` message: text parts
/// only, or a Gemini `result` as its JSON text. Chat has no field for a Gemini `error`.
fn tool_result(doc: &Document, part: usize) -> Result<()> {
    require_string(doc, doc.required(part, "tool_call_id")?)?;
    match super::request::gemini::outcome(doc, part)? {
        Some(super::request::gemini::Outcome::Error(_)) => return Err(Error::Unsupported),
        Some(super::request::gemini::Outcome::Result(_)) => return Ok(()),
        None => (),
    }
    optional(doc, part, "is_error", boolean)?;
    optional(doc, part, "content", |doc, content| {
        array(doc, content)?;
        for item in doc.children(content)? {
            enumeration(doc, doc.required(item, "type")?, &["text"])?;
            require_string(doc, doc.required(item, "text")?)?;
        }
        Ok(())
    })
}

fn response_format(doc: &Document, node: usize) -> Result<()> {
    object(doc, node)?;
    if super::request::json_schema::from_gemini(doc)? {
        return super::request::gemini::json_mode(doc, node).map(|_| ());
    }
    // Chat ingress keeps only these members.
    for member in doc.children(node)? {
        let name = doc.node(member)?.name.ok_or(Error::Schema)?;
        if !doc.name_is(name, "type")? && !doc.name_is(name, "json_schema")? {
            return Err(Error::Unsupported);
        }
    }
    let kind = doc.required(node, "type")?;
    enumeration(doc, kind, &["text", "json_object", "json_schema"])?;
    if doc.string_is(kind, "json_schema")? {
        let value = doc.required(node, "json_schema")?;
        object(doc, value)?;
        require_string(doc, doc.required(value, "name")?)?;
        schema(doc, doc.required(value, "schema")?)?;
        optional(doc, value, "description", require_string)?;
        optional(doc, value, "strict", boolean)?;
    }
    Ok(())
}
fn optional(
    doc: &Document,
    parent: usize,
    key: &str,
    validate: impl FnOnce(&Document, usize) -> Result<()>,
) -> Result<()> {
    if let Some(node) = doc.member(parent, key)?
        && !matches!(doc.node(node)?.value, Value::Null)
    {
        validate(doc, node)?;
    }
    Ok(())
}
fn enumeration(doc: &Document, node: usize, values: &[&str]) -> Result<()> {
    require_string(doc, node)?;
    for value in values {
        if doc.string_is(node, value)? {
            return Ok(());
        }
    }
    Err(Error::Unsupported)
}
fn number(doc: &Document, node: usize) -> Result<()> {
    if matches!(doc.node(node)?.value, Value::Number(_) | Value::Unsigned(_)) {
        Ok(())
    } else {
        Err(Error::Schema)
    }
}
fn integer(doc: &Document, node: usize) -> Result<()> {
    number(doc, node)?;
    if let Value::Number(text) = doc.node(node)?.value {
        let mut cursor = doc.source(text)?.cursor(text.span)?;
        while let Some(byte) = cursor.next_byte()? {
            if matches!(byte, b'.' | b'e' | b'E') {
                return Err(Error::Schema);
            }
        }
    }
    Ok(())
}
fn boolean(doc: &Document, node: usize) -> Result<()> {
    if matches!(doc.node(node)?.value, Value::Bool(_)) {
        Ok(())
    } else {
        Err(Error::Schema)
    }
}
fn object(doc: &Document, node: usize) -> Result<()> {
    if matches!(doc.node(node)?.value, Value::Object) {
        Ok(())
    } else {
        Err(Error::Schema)
    }
}
fn array(doc: &Document, node: usize) -> Result<()> {
    if matches!(doc.node(node)?.value, Value::Array) {
        Ok(())
    } else {
        Err(Error::Schema)
    }
}
fn schema(doc: &Document, node: usize) -> Result<()> {
    if matches!(doc.node(node)?.value, Value::Object | Value::Bool(_)) {
        Ok(())
    } else {
        Err(Error::Schema)
    }
}
