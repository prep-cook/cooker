//! The Meta Muse executor over the internal stream (`docs/EXECUTION-CONTRACT.md`): generation
//! goes to `{base}/responses` through the Responses request egress, and a token count is
//! local. Meta has no native compact.
//!
//! The executor adjusts its own encoded upload in the old path's order: the Codex backend's
//! required members (`codex_upload`), the model suffix's thinking, the upstream model, the
//! members Meta refuses, the Codex instructions, encrypted reasoning and web search tools, and
//! the payload rules last, which also give a Codex client's known tool parameters their integer
//! types. The egress writes `stream`: generation always streams upstream. Upstream events pass
//! through the Codex event layer with Meta's failure and completion rules
//! (`codex_events::Backend::Meta`) into the Responses response ingress. The credential
//! enrichment, inline mint, headers and transport are the old path's.
//!
//! A request carrying Codex's freeform `apply_patch` tool keeps the old route: only the old
//! path's apply_patch bridge sends it as the JSON function Meta takes and turns the calls back.

use bytes::Bytes;
use ck_core::exec::{ExecError, FailureScope, UsageSink};
use ck_core::format::Format;
use ck_translate::codec::document::{Document, Value};
use ck_translate::codec::{self, BodyMode, Capability, Operation, RequestEgress, RequestFence, RequestView};
use ck_translate::responses::request::{Egress, ResponsesCapability};
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
        };
        ResponsesCapability.validate(generate)?;
        if custom_apply_patch(view.document)? {
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
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<Ingress>, ExecError> {
        let enriched = self.ensure_auth(attempt.credential, attempt.cfg).await?;
        let doc = request.validated.view().document;
        // Go tracks only the round trip of a streaming client's response.
        let packet_only = streaming(doc).map_err(encoding)?;
        let upload = encode(&request.validated, streams.request, true)?;
        let body = shape(attempt, request, upload)?;
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
        let ingress = Ingress::new(
            streams.response,
            attempt.identity,
            Operation::Generate,
            BodyMode::Streaming,
            LINE_LIMIT as u64,
        )
        .map_err(encoding)?;
        let url = format!("{}/responses", base.strip_suffix('/').unwrap_or(&base));
        let client = self.clients.get(&Proxy::effective(&enriched, attempt.cfg));
        let headers = meta::headers(&enriched, request.client.headers, request.client.session, &token);
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
        let rules = Events::new(scratch, false, Backend::Meta, None);
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
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Counted<Ingress>, ExecError> {
        self.ensure_auth(attempt.credential, attempt.cfg).await?;
        let view = RequestView {
            operation: Operation::Generate,
            ..request.validated.view()
        };
        let generate = RequestFence::default().end(view, &MetaCapability).map_err(encoding)?;
        let upload = encode(&generate, streams.request, false)?;
        let body = shape(attempt, request, upload)?;
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
        _request: &Request<'_>,
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
/// true for generation, false for the body a count measures.
fn shape(attempt: &Attempt<'_>, request: &Request<'_>, upload: Vec<u8>) -> Result<Vec<u8>, ExecError> {
    let document = request.validated.view().document;
    let source = source(document).map_err(encoding)?;
    let mut original = crate::codex_upload::require(upload);
    content_parts(&mut original, document, source).map_err(encoding)?;
    let requested = gj::get(&original, "model").str().trim().to_owned();
    let rules = ck_wire::payload::Rules::from_config(attempt.cfg);
    let base_model = parse_suffix(attempt.model).model_name;
    let caps = attempt.resolved_model.map(|r| ModelCaps::from(&r.info));
    let mut body = apply_request_thinking(&RequestThinking {
        body: &original,
        payload: &original,
        original: &original,
        model: attempt.model,
        from: Format::Codex.as_str(),
        to: Format::Codex.as_str(),
        provider: PROVIDER,
        resolved: caps.as_ref().map(Some),
        has_request_transformer: false,
        updates_changed: false,
    })
    .map_err(|e| ExecError::local(e.status(), FailureScope::Request, e.message))?;
    // ponytail: `apply_request_thinking` takes the body by reference and returns a copy, so two
    // bodies live while it runs; an owned-body ck-wire API would edit the upload in place.
    let original = baseline(&rules, original);
    meta::set_string_if_different(&mut body, "model", &base_model);
    for key in [
        "generate",
        "prompt_cache_retention",
        "safety_identifier",
        "stream_options",
        "client_metadata",
    ] {
        gj::delete(&mut body, key);
    }
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
    Ok(ck_wire::payload::apply(
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
    ))
}

/// The upload before thinking, kept only for the payload rules that read it (`default` and
/// `default-raw`); otherwise it is released before the rest of the shaping.
fn baseline(rules: &ck_wire::payload::Rules, original: Vec<u8>) -> Option<Vec<u8>> {
    rules.has_defaults().then_some(original)
}

/// The old path's content parts (its request translators): every message's string content
/// becomes one text part, `output_text` for the assistant and `input_text` otherwise. A
/// Responses client's own messages stay as sent; only its string `input` became a part.
fn content_parts(body: &mut Vec<u8>, doc: &Document, source: Format) -> codec::Result<()> {
    if source == Format::OpenAIResponse && !string_input(doc)? {
        return Ok(());
    }
    let mut parts = Vec::new();
    for (i, item) in gj::get(body, "input").array().iter().enumerate() {
        let content = item.get("content");
        if content.kind != gj::Kind::String || !matches!(&*item.get("type").bytes(), b"message" | b"") {
            continue;
        }
        let kind = if &*item.get("role").bytes() == b"assistant" {
            "output_text"
        } else {
            "input_text"
        };
        let mut part = format!(r#"[{{"type":"{kind}","text":""}}]"#).into_bytes();
        gj::set_str(&mut part, "0.text", content.bytes());
        parts.push((format!("input.{i}.content"), part));
    }
    for (path, part) in parts {
        gj::set_raw(body, &path, part);
    }
    Ok(())
}

/// Whether a Responses client sent `input` as a string: its one message besides the
/// instructions is the user's string, which no item `type` was kept for.
// ponytail: an untyped one-item `input` array holding the user's string has the same canonical
// form, so it gets the part too; the old path kept it a string, which Meta reads the same.
fn string_input(doc: &Document) -> codec::Result<bool> {
    let root = doc.root();
    let (mut typed, mut instructions) = (Vec::new(), Vec::new());
    if let Some(x) = doc.member(root, "x_ck")?
        && let Some(original) = doc.member(x, "original")?
    {
        for entry in doc.children(original)? {
            // An entry names the member it keeps with a pointer into its message.
            if let Some((target, _)) = doc.origin(entry)?
                && let Some(pointer) = doc.member(entry, "pointer")?
            {
                if doc.string_is(pointer, "/type")? {
                    typed.push(target);
                }
                if doc.string_is(pointer, "/instructions")? {
                    instructions.push(target);
                }
            }
        }
    }
    let messages = objects(doc, doc.member(root, "messages")?)?;
    let rest: Vec<usize> = messages.into_iter().filter(|m| !instructions.contains(m)).collect();
    let [message] = rest[..] else {
        return Ok(false);
    };
    let user = match doc.member(message, "role")? {
        Some(role) => doc.string_is(role, "user")?,
        None => false,
    };
    let string = match doc.member(message, "content")? {
        Some(content) => matches!(doc.node(content)?.value, Value::String(_) | Value::Text(_)),
        None => false,
    };
    Ok(user && string && !typed.contains(&message))
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
/// choice (and the tools it allows), and earlier calls. Other data, such as call arguments or a
/// schema `default`, may hold the same object without declaring anything.
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
    // Declarations: a list of tools, a tool, or a namespace's children.
    let mut pending = tools;
    pending.extend(doc.member(root, "tool_choice")?);
    while let Some(index) = pending.pop() {
        match doc.node(index)?.value {
            Value::Array => pending.extend(doc.children(index)?),
            Value::Object if custom_patch(doc, index)? => return Ok(true),
            Value::Object => {
                for key in ["tools", "children"] {
                    pending.extend(doc.member(index, key)?);
                }
            }
            _ => {}
        }
    }
    Ok(false)
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
mod tests {
    /// The upload before thinking outlives it only for the payload rules that read it.
    #[test]
    fn the_baseline_stays_only_for_default_rules() {
        let rules = |yaml: &str| ck_wire::payload::Rules::from_config(&ck_core::config::Config::parse(yaml).unwrap());
        let overrides =
            rules("requests:\n  payload:\n    override:\n      - models: [{name: \"m\"}]\n        params: {a: 1}\n");
        assert_eq!(super::baseline(&overrides, b"{}".to_vec()), None);
        assert_eq!(super::baseline(&rules(""), b"{}".to_vec()), None);
        let defaults =
            rules("requests:\n  payload:\n    default:\n      - models: [{name: \"m\"}]\n        params: {a: 1}\n");
        assert_eq!(super::baseline(&defaults, b"{}".to_vec()), Some(b"{}".to_vec()));
    }
}
