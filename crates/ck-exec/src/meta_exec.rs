//! The Meta Muse executor over the internal stream (`docs/EXECUTION-CONTRACT.md`): generation
//! goes to `{base}/responses` through the Responses request egress, and a token count is
//! local. Meta has no native compact.
//!
//! Request controls follow Meta policy, not Codex defaults. Upstream events pass through
//! the Codex event layer with Meta's failure and completion rules. Credential enrichment,
//! inline mint, headers and transport remain shared with the old path.
//!
//! Codex's freeform `apply_patch` tool goes upstream as the JSON function Meta takes, and the
//! apply_patch bridge in the event layer turns the calls back into the custom tool. Only a
//! Responses client's request takes this path; any other client's keeps the old route, whose
//! request translation prefers a Chat client's ordinary `apply_patch` function.

use bytes::Bytes;
use ck_core::exec::{ExecError, FailureScope, UsageSink};
use ck_core::format::Format;
use ck_translate::apply_patch_responses as apply_patch;
use ck_translate::codec::document::{Document, Value};
use ck_translate::codec::{self, BodyMode, Capability, Operation, RequestEgress, RequestFence, RequestView};
use ck_translate::responses::request::Egress;
use ck_translate::responses::response::Ingress;
use ck_wire::json as gj;
use ck_wire::thinking::{ModelCaps, RequestThinking, apply_request_thinking, parse_suffix};

use crate::codex_events::{self as events, Backend, Events};
use crate::codex_exec::{copy, encoding, source, streaming};
use crate::kimi_http::{capture_chunk, capture_error, capture_metadata, capture_request, read_all_strict};
use crate::meta::{self, LINE_LIMIT, MetaExecutor, PROVIDER};
use crate::proxy::{MAX_ERROR_BODY, Proxy, send};
use crate::types::{Attempt, Body, Counted, Execute, Request, Streams, Upstream};

/// The capability key of Meta credentials.
pub const DESTINATION: &str = "meta";

/// Responses generation and its token count.
pub struct MetaCapability;

impl Capability for MetaCapability {
    fn destination(&self) -> &'static str {
        DESTINATION
    }

    fn validate(&self, view: RequestView<'_>) -> codec::Result<()> {
        let generate = match view.operation {
            Operation::Generate => view,
            // A count is valid exactly when the generation it projects is.
            Operation::CountTokens => RequestView {
                operation: Operation::Generate,
                ..view
            },
            Operation::Compact => return Err(codec::Error::Unsupported),
            Operation::Media(_) | Operation::AlphaSearch => return Err(codec::Error::Unsupported),
        };
        ck_translate::responses::request::validate_meta(generate)?;
        if custom_apply_patch(view.document)? && source(view.document)? != Format::OpenAIResponse {
            return Err(codec::Error::Destination);
        }
        Ok(())
    }
}

impl Execute for MetaExecutor {
    type Capability = MetaCapability;
    type Ingress = Ingress;

    fn capability(&self, _attempt: &Attempt<'_>) -> MetaCapability {
        MetaCapability
    }

    async fn execute(
        &self,
        attempt: &Attempt<'_>,
        request: &mut Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<Ingress>, ExecError> {
        let enriched = self.ensure_auth(attempt.credential, attempt.cfg).await?;
        let doc = request.validated().view().document;
        // Go tracks only the round trip of a streaming client's response.
        let packet_only = streaming(doc).map_err(encoding)?;
        let upload = encode(&request.validated(), streams.request, true)?;
        let (body, bridge) = shape(attempt, request, upload)?;
        let usage = request.client.usage;
        usage.request(Format::Codex, &body);
        let (base, token) = meta::creds(&enriched);
        if base.trim().is_empty() {
            return Err(ExecError::local(
                401,
                FailureScope::Credential,
                "meta executor: missing provider baseURL",
            ));
        }
        let scratch = streams.response.allocator();
        let mut ingress = Ingress::new(
            streams.response,
            attempt.identity,
            Operation::Generate,
            BodyMode::Streaming,
            LINE_LIMIT as u64,
        )
        .map_err(encoding)?;
        // The Meta event rules (`codex_events`) classify its failure events.
        ingress.classified_failures();
        ingress.done_sentinel();
        let url = format!("{}/responses", base.strip_suffix('/').unwrap_or(&base));
        let client = self.clients.get(&Proxy::effective(&enriched, attempt.cfg));
        let headers = meta::headers(
            attempt.credential,
            request.client.headers,
            request.client.session,
            &token,
        );
        let capture = usage.capture();
        let (auth_type, auth_value) = ck_core::registry::dynamic::account_info(&enriched);
        capture_request(
            capture,
            &enriched,
            &url,
            &headers,
            &body,
            PROVIDER,
            (auth_type, &auth_value),
        );
        usage.round_trip_started();
        let mut upstream = send(&client, &url, headers, Bytes::from(body), None)
            .await
            .inspect_err(|e| capture_error(capture, e))?;
        capture_metadata(capture, upstream.status, &upstream.headers);
        upstream.body = crate::kimi_http::track_first_byte(upstream.body, usage, packet_only);
        if !(200..300).contains(&upstream.status) {
            // Go returns a failed error-body read as is, never classified by status.
            let error_body = read_all_strict(upstream.body, MAX_ERROR_BODY)
                .await
                .inspect_err(|e| capture_error(capture, e))?;
            capture_chunk(capture, &error_body);
            let mut error = meta::upstream_error(upstream.status, &error_body);
            error.headers = Box::new(upstream.headers);
            return Err(error);
        }
        let rules = Events::new(scratch, false, Backend::Meta, None).bridged(bridge);
        Ok(Upstream {
            status: upstream.status,
            headers: upstream.headers,
            body: observed(upstream.body, usage, rules),
            ingress,
        })
    }

    async fn count_tokens(
        &self,
        attempt: &Attempt<'_>,
        request: &mut Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Counted<Ingress>, ExecError> {
        self.ensure_auth(attempt.credential, attempt.cfg).await?;
        let view = RequestView {
            operation: Operation::Generate,
            ..request.validated().view()
        };
        let generate = RequestFence::default().end(view, &MetaCapability).map_err(encoding)?;
        let upload = encode(&generate, streams.request, false)?;
        let (body, _) = shape(attempt, request, upload)?;
        let count =
            crate::codex_tokens::count_input_tokens(crate::tokenizer::Encoding::O200kBase, &body).map_err(|e| {
                ExecError::local(
                    500,
                    FailureScope::Request,
                    format!("meta executor: tokenizer init failed: {e}"),
                )
            })?;
        Ok(Counted::Local {
            input_tokens: u64::try_from(count).unwrap_or_default(),
        })
    }

    async fn compact(
        &self,
        _attempt: &Attempt<'_>,
        _request: &mut Request<'_>,
        _streams: Streams<'_>,
    ) -> Result<Upstream<Ingress>, ExecError> {
        // Meta has no native compact; [`MetaCapability`] refuses it at the fence.
        Err(encoding(codec::Error::Unsupported))
    }
}

/// The upload request egress encodes for Meta, as one body.
fn encode(
    request: &codec::ValidatedRequest<'_>,
    stream: &mut ck_stream::Writer,
    upstream_stream: bool,
) -> Result<Vec<u8>, ExecError> {
    let egress = Egress {
        destination: DESTINATION,
        upstream_stream,
    };
    copy(&egress.encode(request, stream).map_err(encoding)?)
}

/// `prepareResponsesRequest` on the encoded upload, whose `stream` the egress already wrote:
/// true for generation, false for the body a count measures. The upload is the one body, edited
/// in place. The apply_patch bridge for the response comes with it.
fn shape(
    attempt: &Attempt<'_>,
    request: &Request<'_>,
    upload: Vec<u8>,
) -> Result<(Vec<u8>, apply_patch::State), ExecError> {
    let document = request.validated().view().document;
    let source = source(document).map_err(encoding)?;
    let mut body = crate::codex_upload::normalize(upload);
    gj::set_bool(&mut body, "store", false);
    if source != Format::OpenAIResponse || string_input(document).map_err(encoding)? {
        text_parts(&mut body);
    }
    let requested = gj::get(&body, "model").str().trim().to_owned();
    let rules = ck_wire::payload::Rules::from_config(attempt.cfg);
    let base_model = parse_suffix(attempt.model).model_name;
    let caps = attempt.resolved_model.map(|r| ModelCaps::from(&r.info));
    let original = rules.baseline("", &body);
    let mut body = think(attempt.model, caps.as_ref(), body)?;
    meta::set_string_if_different(&mut body, "model", &base_model);
    for key in ["generate", "stream_options", "client_metadata"] {
        gj::delete(&mut body, key);
    }
    // The upload's tools decide the bridge; a Responses client declares no Chat function to
    // prefer over the custom tool.
    let bridge = apply_patch::State::new(Format::OpenAIResponse, &[], &body);
    let mut body = apply_patch::normalize_executor_request(&body, None)
        .map_err(|e| ExecError::local(500, FailureScope::Request, e))?;
    let client = &request.client;
    ck_wire::codex_client::normalize_codex_instructions(&mut body);
    body = crate::openai_compat_payload::sanitize_reasoning_encrypted_content(body);
    meta::sanitize_web_search_tools(&mut body);
    // The payload rules are the last change to the body (868e137); for a Codex client they also
    // set its known tools' integer parameter types.
    let requested = match requested.as_str() {
        "" => attempt.model.trim(),
        requested => requested,
    };
    let body = ck_wire::payload::apply(
        &rules,
        &ck_wire::payload::Request {
            target_executor: "",
            model: &base_model,
            requested_model: requested,
            protocol: PROVIDER,
            from_protocol: source.as_str(),
            root: "",
            original: original.as_deref().unwrap_or_default(),
            request_path: client.route,
            headers: Some(client.headers),
        },
        body,
    );
    Ok((body, bridge))
}

/// The model suffix's thinking on the upload, which is also the request it reads: the upload
/// comes back edited, or as it was in the same allocation.
fn think(model: &str, caps: Option<&ModelCaps>, upload: Vec<u8>) -> Result<Vec<u8>, ExecError> {
    let request = RequestThinking {
        payload: None,
        original: None,
        model,
        from: Format::Codex.as_str(),
        to: Format::Codex.as_str(),
        provider: PROVIDER,
        resolved: caps.map(Some),
        has_request_transformer: false,
        updates_changed: false,
    };
    apply_request_thinking(&request, upload).map_err(|e| ExecError::local(e.status(), FailureScope::Request, e.message))
}

/// The old path's content parts (its request translators): every message item's string content
/// becomes one text part, `output_text` for the assistant and `input_text` otherwise. The body
/// grows once and its bytes move back to front; only the contents' offsets are collected.
fn text_parts(body: &mut Vec<u8>) {
    const CLOSE: &[u8] = b"}]";
    let open = |assistant: bool| -> &'static [u8] {
        if assistant {
            br#"[{"type":"output_text","text":"#
        } else {
            br#"[{"type":"input_text","text":"#
        }
    };
    let base = body.as_ptr().addr();
    let mut contents = Vec::new();
    gj::get(body, "input").each(|_, item| {
        if let Some((content, assistant)) = string_content(item.raw()) {
            let start = content.as_ptr().addr() - base;
            contents.push((start, start + content.len(), assistant));
        }
        true
    });
    let mut read = body.len();
    let grown: usize = contents.iter().map(|&(.., a)| open(a).len() + CLOSE.len()).sum();
    body.resize(read + grown, 0);
    let mut write = body.len();
    for &(start, end, assistant) in contents.iter().rev() {
        write -= read - end;
        body.copy_within(end..read, write);
        for piece in [Err(CLOSE), Ok(start..end), Err(open(assistant))] {
            match piece {
                Ok(text) => {
                    write -= text.len();
                    body.copy_within(text, write);
                }
                Err(literal) => {
                    write -= literal.len();
                    body[write..write + literal.len()].copy_from_slice(literal);
                }
            }
        }
        read = start;
    }
    debug_assert_eq!(read, write);
}

/// The string content of message item `item`, untyped or of type `message`, and whether the
/// assistant sent it; read in place.
fn string_content(item: &[u8]) -> Option<(&[u8], bool)> {
    let (mut kind, mut role, mut content) = (None, None, None);
    gj::object_members(item, |key, escaped, value| {
        let key: std::borrow::Cow<'_, [u8]> = if escaped { gj::unescape(key).into() } else { key.into() };
        // The first of duplicate members wins, as the old path's reads.
        let slot = match &*key {
            b"type" => &mut kind,
            b"role" => &mut role,
            b"content" => &mut content,
            _ => return true,
        };
        slot.get_or_insert(value);
        true
    });
    let message = kind.is_none_or(|k| k == b"null" || text_is_raw(k, b"message") || text_is_raw(k, b""));
    let content = content.filter(|c| c.first() == Some(&b'"'))?;
    message.then(|| (content, role.is_some_and(|r| text_is_raw(r, b"assistant"))))
}

/// Whether the raw JSON value `raw` is the string `want`.
fn text_is_raw(raw: &[u8], want: &[u8]) -> bool {
    let Some(inner) = raw.strip_prefix(b"\"").and_then(|r| r.strip_suffix(b"\"")) else {
        return false;
    };
    if inner.contains(&b'\\') {
        gj::unescape(inner) == want
    } else {
        inner == want
    }
}

/// Whether a Responses client sent `input` as a string: Responses ingress marks the content
/// that string became with an `/input` original bound to it.
fn string_input(doc: &Document) -> codec::Result<bool> {
    let Some(x) = doc.member(doc.root(), "x_ck")? else {
        return Ok(false);
    };
    let Some(original) = doc.member(x, "original")? else {
        return Ok(false);
    };
    for entry in doc.children(original)? {
        if let Some((target, _)) = doc.origin(entry)?
            && let (Some(protocol), Some(pointer)) = (doc.member(entry, "protocol")?, doc.member(entry, "pointer")?)
            && doc.string_is(protocol, "responses")?
            && doc.string_is(pointer, "/input")?
            && let Some(name) = doc.node(target)?.name
            && doc.name_is(name, "content")?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// `body` captured as it arrives, through Meta's event rules, with the failure that ends it
/// captured too.
fn observed(body: Body, usage: &UsageSink, rules: Events) -> Body {
    let capture = usage.capture().clone();
    let logs = capture.logs_responses();
    let raw = Box::pin(futures_util::StreamExt::inspect(body, {
        let capture = capture.clone();
        move |chunk| {
            if let Ok(chunk) = chunk
                && logs
                && !chunk.is_empty()
            {
                capture_chunk(&capture, chunk);
            }
        }
    }));
    Box::pin(futures_util::StreamExt::inspect(
        events::body(raw, rules),
        move |item| {
            if let Err(error) = item {
                capture_error(&capture, error);
            }
        },
    ))
}

/// Whether the request declares, chooses or replays Codex's freeform `apply_patch` tool. Only
/// the carriers the old bridge rewrites are read: tool declarations (top level, the native
/// tools a Responses client keeps, `additional_tools` items, and namespace children), the tool
/// choice (and the tools an allowed-tools choice lists), and earlier calls. Other data, such as
/// call arguments, a schema `default` or a tool's own extension members, may hold the same
/// object without declaring anything.
fn custom_apply_patch(doc: &Document) -> codec::Result<bool> {
    let root = doc.root();
    let mut tools = Vec::new();
    tools.extend(doc.member(root, "tools")?);
    if let Some(x) = doc.member(root, "x_ck")?
        && let Some(native) = doc.member(x, "tools")?
    {
        for entry in objects(doc, Some(native))? {
            tools.extend(doc.member(entry, "declaration")?);
        }
    }
    let mut calls = Vec::new();
    for message in objects(doc, doc.member(root, "messages")?)? {
        calls.extend(objects(doc, doc.member(message, "tool_calls")?)?);
        for part in objects(doc, doc.member(message, "content")?)? {
            if let Some(value) = doc.member(part, "value")?
                && matches!(doc.node(value)?.value, Value::Object)
                && let Some(kind) = doc.member(value, "type")?
                && text_is(doc, kind, "additional_tools", false)?
            {
                tools.extend(doc.member(value, "tools")?);
            }
        }
    }
    for call in calls {
        if custom_patch(doc, call)? {
            return Ok(true);
        }
    }
    let mut pending: Vec<_> = tools.into_iter().map(|t| (t, Carrier::Declaration)).collect();
    pending.extend(doc.member(root, "tool_choice")?.map(|c| (c, Carrier::Choice)));
    while let Some((index, carrier)) = pending.pop() {
        match doc.node(index)?.value {
            Value::Array => pending.extend(doc.children(index)?.map(|child| (child, carrier))),
            Value::Object if custom_patch(doc, index)? => return Ok(true),
            Value::Object => pending.extend(carrier.lists(doc, index)?.into_iter().map(|list| (list, carrier))),
            _ => {}
        }
    }
    Ok(false)
}

/// What a fenced object stands for, which decides the lists it holds.
#[derive(Clone, Copy)]
enum Carrier {
    Declaration,
    Choice,
}

impl Carrier {
    /// The lists of tools `object` holds by its type: a namespace declaration's `tools` or,
    /// without that array, its `children` (as the old bridge reads them), and an allowed-tools
    /// choice's tools, flat (Responses) or under `allowed_tools` (Chat).
    fn lists(self, doc: &Document, object: usize) -> codec::Result<Vec<usize>> {
        let Some(kind) = doc.member(object, "type")? else {
            return Ok(Vec::new());
        };
        let array = |list: Option<usize>| -> codec::Result<Option<usize>> {
            Ok(match list {
                Some(list) if matches!(doc.node(list)?.value, Value::Array) => Some(list),
                _ => None,
            })
        };
        let mut lists = Vec::new();
        match self {
            Carrier::Declaration if text_is(doc, kind, "namespace", false)? => {
                let tools = array(doc.member(object, "tools")?)?;
                lists.extend(match tools {
                    Some(tools) => Some(tools),
                    None => array(doc.member(object, "children")?)?,
                });
            }
            Carrier::Choice if text_is(doc, kind, "allowed_tools", false)? => {
                lists.extend(doc.member(object, "tools")?);
                if let Some(allowed) = doc.member(object, "allowed_tools")?
                    && matches!(doc.node(allowed)?.value, Value::Object)
                {
                    lists.extend(doc.member(allowed, "tools")?);
                }
            }
            _ => {}
        }
        Ok(lists)
    }
}

/// The object elements of array `list`, if it is one.
fn objects(doc: &Document, list: Option<usize>) -> codec::Result<Vec<usize>> {
    let Some(list) = list.filter(|&l| matches!(doc.node(l).map(|n| n.value), Ok(Value::Array))) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for child in doc.children(list)? {
        if matches!(doc.node(child)?.value, Value::Object) {
            out.push(child);
        }
    }
    Ok(out)
}

/// A custom tool, choice or call named `apply_patch` (Go trims the name) in any form the codecs
/// keep: flat, or under `custom` or `function`.
fn custom_patch(doc: &Document, object: usize) -> codec::Result<bool> {
    let Some(kind) = doc.member(object, "type")? else {
        return Ok(false);
    };
    if !(text_is(doc, kind, "custom", false)? || text_is(doc, kind, "custom_tool_call", false)?) {
        return Ok(false);
    }
    for holder in [
        Some(object),
        doc.member(object, "custom")?,
        doc.member(object, "function")?,
    ]
    .into_iter()
    .flatten()
    {
        if matches!(doc.node(holder)?.value, Value::Object)
            && let Some(name) = doc.member(holder, "name")?
            && text_is(doc, name, "apply_patch", true)?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Whether node `index` is the string `want`, with surrounding white space when `trim`.
fn text_is(doc: &Document, index: usize, want: &str, trim: bool) -> codec::Result<bool> {
    if !matches!(doc.node(index)?.value, Value::String(_) | Value::Text(_)) {
        return Ok(false);
    }
    let mut chars = doc.chars(index)?;
    let mut next = chars.next_scalar()?;
    while trim && next.is_some_and(char::is_whitespace) {
        next = chars.next_scalar()?;
    }
    for expected in want.chars() {
        if next != Some(expected) {
            return Ok(false);
        }
        next = chars.next_scalar()?;
    }
    while trim && next.is_some_and(char::is_whitespace) {
        next = chars.next_scalar()?;
    }
    Ok(next.is_none())
}

#[cfg(test)]
#[path = "meta_exec_tests.rs"]
mod tests;
