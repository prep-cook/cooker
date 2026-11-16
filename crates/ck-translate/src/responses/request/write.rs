use super::super::{is, member};
use super::native::{item_type, native, typed};
use super::reasoning::write_reasoning_options;
use super::{ANTIGRAVITY, PROVEN};
use crate::codec::{
    Error, Operation, RequestView, Result,
    document::{Document, Value},
    encode::Encoder,
};
use ck_wire::signature;
pub(in crate::responses) use fields::write_echo;
use fields::write_field;
use results::write_results;
#[path = "fields.rs"]
mod fields;
#[path = "results.rs"]
mod results;

/// Writes the Responses fields of `target` that normalization kept in `x_ck.original`; fields
/// another protocol kept there mean nothing to a Responses upstream.
fn originals(doc: &Document, target: usize, e: &mut Encoder<'_, '_>, skip: &[&str]) -> Result<()> {
    let Some(x) = doc.member(doc.root(), "x_ck")? else {
        return Ok(());
    };
    let Some(original) = doc.member(x, "original")? else {
        return Ok(());
    };
    for entry in doc.origins(target)? {
        if doc.node(entry)?.parent != Some(original) || !is(doc, entry, "protocol", "responses")? {
            continue;
        }
        let pointer = doc.required(entry, "pointer")?;
        let Value::String(text) = doc.node(pointer)?.value else {
            return Err(Error::Schema);
        };
        let field_name = doc.pointer_name(text)?;
        let mut skipped = false;
        for name in skip {
            skipped |= doc.name_is(field_name, name)?;
        }
        if skipped {
            continue;
        }
        e.bytes(b",")?;
        doc.write_name(field_name, e)?;
        e.bytes(b":")?;
        doc.write(doc.required(entry, "value")?, e)?;
    }
    Ok(())
}
pub(super) fn write_request(
    view: RequestView<'_>,
    streaming: bool,
    destination: &str,
    e: &mut Encoder<'_, '_>,
) -> Result<()> {
    let doc = view.document;
    let root = doc.root();
    e.bytes(b"{\"input\":[")?;
    let mut first = true;
    let mut replay = Replay {
        next: match doc.member(root, "x_ck")? {
            Some(x) => doc
                .member(x, "replay")?
                .map(|r| doc.node(r))
                .transpose()?
                .and_then(|r| r.first),
            None => None,
        },
        index: 0,
    };
    for message in doc.children(doc.required(root, "messages")?)? {
        if !instructions(doc, message)? {
            write_message(view, message, &mut replay, destination, &mut first, e)?;
        }
    }
    e.bytes(b"]")?;
    if view.operation != Operation::Compact {
        e.bytes(b",\"stream\":")?;
        e.bytes(if streaming { b"true" } else { b"false" })?;
    }
    for message in doc.children(doc.required(root, "messages")?)? {
        if instructions(doc, message)? {
            e.bytes(b",\"instructions\":")?;
            doc.write(doc.required(message, "content")?, e)?;
        }
    }
    for field in doc.children(root)? {
        // Responses has one output; Meta's fence permits only n:1, not an n wire field.
        if destination == "meta" && doc.name_is(doc.node(field)?.name.ok_or(Error::Schema)?, "n")? {
            continue;
        }
        write_field(doc, field, e)?;
    }
    if let Some(x) = doc.member(root, "x_ck")? {
        write_extensions(doc, x, destination == "codex", e)?;
    }
    originals(doc, root, e, &[])?;
    if destination == "meta" {
        super::meta::write_extensions(doc, e)?;
    }
    e.bytes(b"}")
}
/// Writes the reasoning options, the continuation and the Responses provider parameters.
fn write_extensions(doc: &Document, x: usize, codex: bool, e: &mut Encoder<'_, '_>) -> Result<()> {
    if let Some(thinking) = doc.member(x, "thinking")? {
        write_reasoning_options(doc, x, thinking, e)?;
    }
    if let Some(session) = doc.member(x, "session")? {
        member(doc, session, "previous_response_id", e)?;
    }
    let providers = doc.member(x, "provider_parameters")?;
    let codex = if codex {
        providers.map(|p| doc.member(p, "codex")).transpose()?.flatten()
    } else {
        None
    };
    let responses = providers.map(|p| doc.member(p, "responses")).transpose()?.flatten();
    let summary = codex.map(|p| doc.member(p, "stream_options")).transpose()?.flatten();
    let summary = summary
        .map(|p| doc.member(p, "reasoning_summary_delivery"))
        .transpose()?
        .flatten();
    if let Some(p) = responses {
        let format = doc.member(doc.root(), "response_format")?.is_some();
        for f in doc.children(p)? {
            let name = doc.node(f)?.name.ok_or(Error::Schema)?;
            if summary.is_some() && doc.name_is(name, "stream_options")? {
                continue;
            }
            if format && doc.name_is(name, "text")? {
                // Written beside `format` in the same `text` object.
                continue;
            }
            e.bytes(b",")?;
            doc.write_name(name, e)?;
            e.bytes(b":")?;
            doc.write(f, e)?;
        }
    }
    if let Some(summary) = summary {
        write_codex_stream_options(doc, summary, responses, e)?;
    }
    Ok(())
}

fn write_codex_stream_options(
    doc: &Document,
    summary: usize,
    responses: Option<usize>,
    e: &mut Encoder<'_, '_>,
) -> Result<()> {
    e.bytes(b",\"stream_options\":{")?;
    e.bytes(b"\"reasoning_summary_delivery\":")?;
    doc.write(summary, e)?;
    if let Some(options) = responses
        .map(|p| doc.member(p, "stream_options"))
        .transpose()?
        .flatten()
    {
        for field in doc.children(options)? {
            e.bytes(b",")?;
            doc.write_name(doc.node(field)?.name.ok_or(Error::Schema)?, e)?;
            e.bytes(b":")?;
            doc.write(field, e)?;
        }
    }
    e.bytes(b"}")
}
/// True for the developer message that carries the Responses `instructions` field.
fn instructions(doc: &Document, message: usize) -> Result<bool> {
    Ok(native(doc, message, "instructions")?.is_some() && is(doc, message, "role", "developer")?)
}
/// Starts the next input item.
fn separate(first: &mut bool, e: &mut Encoder<'_, '_>) -> Result<()> {
    if !*first {
        e.bytes(b",")?;
    }
    *first = false;
    Ok(())
}
/// Writes the input items of one message: a tool output; or its reasoning item, its message
/// item and its calls, in that order.
fn write_message(
    view: RequestView<'_>,
    message: usize,
    replay: &mut Replay,
    destination: &str,
    first: &mut bool,
    e: &mut Encoder<'_, '_>,
) -> Result<()> {
    let doc = view.document;
    if is(doc, message, "role", "tool")? {
        separate(first, e)?;
        return write_tool_output(view, message, e);
    }
    let calls = doc.member(message, "tool_calls")?;
    if let Some(content) = doc.member(message, "content")? {
        let kind = item_type(doc, message)?;
        let native = typed(doc, kind, "reasoning")? || typed(doc, kind, "compaction")?;
        if let Some(part) = doc.node(content)?.first
            && kind.is_some()
            && !native
            && is(doc, part, "type", "opaque")?
        {
            separate(first, e)?;
            return doc.write(doc.required(part, "value")?, e);
        }
        let results = write_results(view, content, first, e)?;
        let reasoning = native || any_part(doc, content, true)?;
        let provenance = provenance(doc, message, content, replay)?;
        // Settled 62: reasoning no producer vouches for, in a form only Antigravity reads, is left
        // out at every other destination.
        if reasoning && (destination == ANTIGRAVITY || provenance != Provenance::AntigravityForm) {
            separate(first, e)?;
            // Tells the Antigravity steps that a producer tag proves the item theirs.
            let proven = destination == ANTIGRAVITY && provenance == Provenance::Producer;
            write_reasoning(doc, message, content, replay, proven, e)?;
        }
        let text = match doc.node(content)?.value {
            Value::Array => any_part(doc, content, false)? || (!reasoning && !results && calls.is_none()),
            Value::Null => calls.is_none(),
            _ => calls.is_none() || doc.chars(content)?.next_scalar()?.is_some(),
        };
        if text {
            separate(first, e)?;
            write_message_item(view, message, content, e)?;
        }
    }
    if let Some(calls) = calls {
        for call in doc.children(calls)? {
            separate(first, e)?;
            write_call(doc, call, e)?;
        }
    }
    Ok(())
}
/// Who vouches for the encrypted reasoning of a message.
#[derive(Clone, Copy, Eq, PartialEq)]
enum Provenance {
    /// No encrypted part, or one no producer tagged in a form any destination may read.
    None,
    /// A producer tag: history the server stored and `codec::provenance::admit` admitted for
    /// this attempt's account (Settled 73).
    Producer,
    /// No producer tag, and a form only the Antigravity steps read.
    AntigravityForm,
}

/// The provenance of `message`'s encrypted parts. A client replaying an item itself sends no
/// tag (the owner cut the signed envelope on 2026-11-15).
fn provenance(doc: &Document, message: usize, content: usize, replay: &mut Replay) -> Result<Provenance> {
    if !matches!(doc.node(content)?.value, Value::Array) {
        return Ok(Provenance::None);
    }
    let mut provenance = Provenance::None;
    for part in doc.children(content)? {
        if is(doc, part, "type", "encrypted")? {
            let entry = replay.seek(doc, doc.unsigned(doc.required(part, "replay_index")?)?, message)?;
            if doc.member(entry, "producer")?.is_some() {
                provenance = Provenance::Producer;
            } else if antigravity_reasoning(doc, doc.required(entry, "signature")?)? {
                return Ok(Provenance::AntigravityForm);
            }
        }
    }
    Ok(provenance)
}

/// Whether string `value` is Antigravity reasoning: a Claude thinking signature, which reaches a
/// Responses client only from Antigravity, or one of CLIProxyAPI's Antigravity carriers and
/// compaction capsules. Only a value that can be a Claude signature is copied out to be checked.
fn antigravity_reasoning(doc: &Document, value: usize) -> Result<bool> {
    const PREFIXES: [&str; 3] = [
        "cpa-gemini-responses-carrier-v1:",
        "cpa-gemini-carrier-v1:",
        "cpa-ag-compact-v1:",
    ];
    let mut chars = doc.chars(value)?;
    let mut text = String::new();
    while text.trim_start().len() < PREFIXES[0].len() {
        match chars.next_scalar()? {
            Some(c) => text.push(c),
            None => break,
        }
    }
    let head = text.trim_start();
    if PREFIXES.iter().any(|p| head.starts_with(p)) {
        return Ok(true);
    }
    if !head.starts_with("claude#") && !head.starts_with(['C', 'E', 'Q', 'R']) {
        return Ok(false);
    }
    while let Some(c) = chars.next_scalar()? {
        text.push(c);
    }
    Ok(signature::detect_provider(&text) == signature::Provider::Claude)
}
/// True when array `content` has a reasoning part (`reasoning`) or a message part.
fn any_part(doc: &Document, content: usize, reasoning: bool) -> Result<bool> {
    if !matches!(doc.node(content)?.value, Value::Array) {
        return Ok(false);
    }
    for part in doc.children(content)? {
        let kind = doc.required(part, "type")?;
        if doc.string_is(kind, "tool_result")? {
            continue;
        }
        let found = doc.string_is(kind, "reasoning")?
            || doc.string_is(kind, "reasoning_summary")?
            || doc.string_is(kind, "encrypted")?;
        if found == reasoning {
            return Ok(true);
        }
    }
    Ok(false)
}
fn write_call(doc: &Document, call: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let custom = is(doc, call, "arguments_format", "text")?;
    let verbatim = custom || is(doc, call, "arguments_format", "invalid_json")?;
    e.bytes(b"{\"type\":")?;
    e.string(if custom { "custom_tool_call" } else { "function_call" })?;
    e.bytes(b",\"call_id\":")?;
    doc.write(doc.required(call, "id")?, e)?;
    let function = doc.required(call, "function")?;
    member(doc, function, "name", e)?;
    member(doc, function, "namespace", e)?;
    if let Some(arguments) = doc.member(function, "arguments")? {
        e.bytes(b",")?;
        e.string(if custom { "input" } else { "arguments" })?;
        e.bytes(b":")?;
        if verbatim {
            doc.write(arguments, e)?;
        } else {
            e.quoted_json(|e| doc.write(arguments, e))?;
        }
    }
    originals(doc, call, e, &[])?;
    e.bytes(b"}")
}
fn write_tool_output(view: RequestView<'_>, message: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let doc = view.document;
    e.bytes(b"{\"type\":")?;
    e.string(if is(doc, message, "name", "custom")? {
        "custom_tool_call_output"
    } else {
        "function_call_output"
    })?;
    e.bytes(b",\"call_id\":")?;
    doc.write(doc.required(message, "tool_call_id")?, e)?;
    e.bytes(b",\"output\":")?;
    write_content(view, doc.required(message, "content")?, false, e)?;
    originals(doc, message, e, &["type"])?;
    e.bytes(b"}")
}
/// Writes string content as is and array content as Responses parts.
fn write_content(view: RequestView<'_>, content: usize, assistant: bool, e: &mut Encoder<'_, '_>) -> Result<()> {
    let doc = view.document;
    if !matches!(doc.node(content)?.value, Value::Array) {
        return doc.write(content, e);
    }
    e.bytes(b"[")?;
    for (i, part) in doc.children(content)?.enumerate() {
        if i > 0 {
            e.bytes(b",")?;
        }
        write_part(view, part, assistant, e)?;
    }
    e.bytes(b"]")
}
/// Writes a message item with the content parts that are not reasoning.
fn write_message_item(view: RequestView<'_>, message: usize, content: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let doc = view.document;
    let assistant = is(doc, message, "role", "assistant")?;
    // A Responses client's untyped message item goes out untyped, as sent; its string `input`
    // became a typed one.
    let untyped = is(doc, doc.required(doc.root(), "x_ck")?, "original_protocol", "responses")?
        && native(doc, message, "type")?.is_none()
        && native(doc, content, "input")?.is_none();
    e.bytes(if untyped {
        b"{\"role\":"
    } else {
        b"{\"type\":\"message\",\"role\":"
    })?;
    doc.write(doc.required(message, "role")?, e)?;
    e.bytes(b",\"content\":")?;
    if matches!(doc.node(content)?.value, Value::Array) {
        e.bytes(b"[")?;
        let mut first = true;
        for part in doc.children(content)? {
            let kind = doc.required(part, "type")?;
            if doc.string_is(kind, "reasoning")?
                || doc.string_is(kind, "reasoning_summary")?
                || doc.string_is(kind, "encrypted")?
                || doc.string_is(kind, "tool_result")?
            {
                continue;
            }
            separate(&mut first, e)?;
            write_part(view, part, assistant, e)?;
        }
        e.bytes(b"]")?;
    } else {
        doc.write(content, e)?;
    }
    originals(doc, message, e, &["type", "instructions"])?;
    e.bytes(b"}")
}
/// A cursor over `x_ck.replay`. Encrypted parts name their entries in increasing order, so one
/// walk serves the whole request; entries whose part was deleted are passed over.
struct Replay {
    next: Option<usize>,
    index: u64,
}
impl Replay {
    /// Entry `wanted`, which must be bound to `message`.
    fn seek(&mut self, doc: &Document, wanted: u64, message: usize) -> Result<usize> {
        while self.index < wanted {
            self.next = doc.node(self.next.ok_or(Error::Schema)?)?.next;
            self.index += 1;
        }
        let entry = self.next.filter(|_| self.index == wanted).ok_or(Error::Schema)?;
        if doc.origin(entry)?.map(|(parent, _)| parent) != Some(message) {
            return Err(Error::Schema);
        }
        Ok(entry)
    }
}
/// Writes the reasoning or compaction item of `message` with its summary, full reasoning and
/// the encrypted content its surviving encrypted parts name.
fn write_reasoning(
    doc: &Document,
    message: usize,
    content: usize,
    replay: &mut Replay,
    proven: bool,
    e: &mut Encoder<'_, '_>,
) -> Result<()> {
    let compaction = typed(doc, item_type(doc, message)?, "compaction")?;
    e.bytes(b"{\"type\":")?;
    e.string(if compaction { "compaction" } else { "reasoning" })?;
    let has = |kind: &str| -> Result<bool> {
        if !matches!(doc.node(content)?.value, Value::Array) {
            return Ok(false);
        }
        for part in doc.children(content)? {
            if is(doc, part, "type", kind)? {
                return Ok(true);
            }
        }
        Ok(false)
    };
    if !compaction || has("reasoning_summary")? {
        write_reasoning_parts(doc, content, ("summary", "reasoning_summary", "summary_text"), e)?;
    }
    if has("reasoning")? {
        write_reasoning_parts(doc, content, ("content", "reasoning", "reasoning_text"), e)?;
    }
    let parts = matches!(doc.node(content)?.value, Value::Array).then_some(content);
    let mut held = None;
    for part in parts.map(|p| doc.children(p)).transpose()?.into_iter().flatten() {
        if is(doc, part, "type", "encrypted")? {
            let index = doc.unsigned(doc.required(part, "replay_index")?)?;
            let entry = replay.seek(doc, index, message)?;
            e.bytes(b",\"encrypted_content\":")?;
            doc.write(doc.required(entry, "signature")?, e)?;
            held = Some(index);
        }
    }
    // The replay index names this item, so a call restores only the bytes of the item it pairs
    // with. Request ingress drops a client's member of that name; a stored answer's original
    // member of that name never passes as proof either.
    if let Some(index) = held.filter(|_| proven && !compaction) {
        e.bytes(b",\"")?;
        e.bytes(PROVEN.as_bytes())?;
        e.bytes(b"\":")?;
        e.unsigned(index)?;
    }
    originals(doc, message, e, &["type", PROVEN])?;
    e.bytes(b"}")
}
/// Writes member `field` listing the `kind` parts of `content` as `native` parts.
fn write_reasoning_parts(
    doc: &Document,
    content: usize,
    (field, kind, native): (&str, &str, &str),
    e: &mut Encoder<'_, '_>,
) -> Result<()> {
    e.bytes(b",")?;
    e.string(field)?;
    e.bytes(b":[")?;
    let mut first = true;
    if matches!(doc.node(content)?.value, Value::Array) {
        for part in doc.children(content)? {
            if is(doc, part, "type", kind)? {
                separate(&mut first, e)?;
                e.bytes(b"{\"type\":")?;
                e.string(native)?;
                e.bytes(b",\"text\":")?;
                doc.write(doc.required(part, "text")?, e)?;
                originals(doc, part, e, &[])?;
                e.bytes(b"}")?;
            }
        }
    }
    e.bytes(b"]")
}
fn write_part(view: RequestView<'_>, part: usize, assistant: bool, e: &mut Encoder<'_, '_>) -> Result<()> {
    let doc = view.document;
    if is(doc, part, "type", "opaque")? {
        return doc.write(doc.required(part, "value")?, e);
    }
    if is(doc, part, "type", "audio")? {
        let source = doc.required(part, "source")?;
        if !is(doc, source, "kind", "data")? {
            return Err(Error::Unsupported);
        }
        let mime = doc.required(part, "mime")?;
        let format = if doc.string_is(mime, "audio/mpeg")? {
            "mp3"
        } else if doc.string_is(mime, "audio/wav")? {
            "wav"
        } else {
            return Err(Error::Unsupported);
        };
        e.bytes(b"{\"type\":\"input_audio\",\"input_audio\":{\"data\":")?;
        // `format` names the media type, so the data stays plain base64.
        view.media.write_base64(doc, doc.required(source, "ref")?, e)?;
        e.bytes(b",\"format\":")?;
        e.string(format)?;
        originals(doc, source, e, &["format"])?;
        e.bytes(b"}")?;
        originals(doc, part, e, &[])?;
        return e.bytes(b"}");
    }
    if is(doc, part, "type", "image")? || is(doc, part, "type", "document")? {
        let image = is(doc, part, "type", "image")?;
        e.bytes(b"{\"type\":")?;
        e.string(if image { "input_image" } else { "input_file" })?;
        let source = doc.required(part, "source")?;
        if is(doc, source, "kind", "uri")? {
            e.bytes(b",")?;
            e.string(if image { "image_url" } else { "file_url" })?;
            e.bytes(b":")?;
            doc.write(doc.required(source, "uri")?, e)?;
        } else if is(doc, source, "kind", "file")? {
            member(doc, source, "file_id", e)?;
        } else if is(doc, source, "kind", "data")? {
            e.bytes(b",")?;
            e.string(if image { "image_url" } else { "file_data" })?;
            e.bytes(b":")?;
            let mime = doc.member(part, "mime")?;
            view.media
                .write_value(doc, doc.required(source, "ref")?, (mime, true), e)?;
        } else {
            return Err(Error::Unsupported);
        }
        member(doc, part, "detail", e)?;
        member(doc, part, "filename", e)?;
    } else {
        let refusal = is(doc, part, "type", "refusal")?;
        e.bytes(b"{\"type\":")?;
        e.string(if refusal {
            "refusal"
        } else if assistant {
            "output_text"
        } else {
            "input_text"
        })?;
        e.bytes(b",")?;
        e.string(if refusal { "refusal" } else { "text" })?;
        e.bytes(b":")?;
        doc.write(doc.required(part, "text")?, e)?;
    }
    originals(doc, part, e, &[])?;
    e.bytes(b"}")
}
