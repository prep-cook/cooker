//! The Kimi executor over the internal stream (`docs/EXECUTION-CONTRACT.md`): Chat and Gemini
//! clients go to `/v1/chat/completions` through the Chat request egress, Responses clients to
//! the native `/v1/responses` through the Responses request egress, and the upstream body
//! goes into the matching response ingress.
//!
//! The executor shapes its own encoded upload, one heap body, in the old path's order: the
//! normalized upstream model, the model suffix's thinking, Kimi's message, tool and temperature
//! normalizations, and the payload rules last. A Responses upload first gets back a string
//! `input`, since the old path forwarded the client's body. Headers, capture,
//! transport and error classification are the old path's (`kimi`), and so are the line limits
//! of the response ingress. A Chat stream that ends cleanly without `[DONE]` completes, as the
//! old path's synthetic `[DONE]` completes it.
//!
//! These requests keep the old route through a fence `Err(Destination)`:
//! - Claude Messages clients and every token count. The old path delegates them to the
//!   Claude executor at Kimi's Anthropic-compatible base, with model normalization,
//!   response-model restoration and Kimi's reasoning replay; the contract's Claude path builds
//!   its context with the default delegation and has no seam for any of the three.
//! - Responses requests that declare, choose or replay Codex's freeform `apply_patch` tool:
//!   only the old path's bridge sends it as the JSON function Kimi takes and turns the calls
//!   back.
//!
//! Kimi has no native compact; the capability refuses it.

use ck_core::exec::{ExecError, FailureScope, UsageSink};
use ck_core::format::Format;
use ck_translate::codec::chat::request::{ChatCapability, Egress as ChatEgress};
use ck_translate::codec::chat::response::Ingress as ChatIngress;
use ck_translate::codec::document::{Document, Value};
use ck_translate::codec::{self, BodyMode, Capability, Operation, RequestEgress, RequestView};
use ck_translate::responses::request::{Egress as ResponsesEgress, ResponsesCapability};
use ck_translate::responses::response::Ingress as ResponsesIngress;
use ck_wire::json as gj;
use ck_wire::thinking::{ModelCaps, RequestThinking, apply_request_thinking, parse_suffix};
use futures_util::StreamExt;

use crate::gemini_exec::{copy, encoding, streaming};
use crate::kimi::{self, KimiExecutor};
use crate::kimi_http::{capture_chunk, capture_error};
use crate::openai_compat_exec::RelayIngress;
use crate::types::{Attempt, Body, Counted, Execute, Request, Streams, Upstream};
use crate::{kimi_edit, kimi_sse};

/// The capability key of Kimi credentials.
pub const DESTINATION: &str = "kimi";

/// Chat generation for Chat and Gemini clients, native Responses generation for Responses
/// clients; everything else is refused or sent back to the old route.
pub struct KimiCapability;

impl Capability for KimiCapability {
    fn destination(&self) -> &'static str {
        DESTINATION
    }

    fn validate(&self, view: RequestView<'_>) -> codec::Result<()> {
        match view.operation {
            Operation::Generate => {}
            // The old path counts upstream through the Claude delegation.
            Operation::CountTokens => return Err(codec::Error::Destination),
            Operation::Compact => return Err(codec::Error::Unsupported),
        }
        match client(view.document)? {
            Format::Claude => Err(codec::Error::Destination),
            Format::OpenAIResponse => {
                ResponsesCapability.validate(view)?;
                if custom_apply_patch(view.document)? {
                    return Err(codec::Error::Destination);
                }
                Ok(())
            }
            _ => ChatCapability.validate(view),
        }
    }
}

impl Execute for KimiExecutor {
    type Capability = KimiCapability;
    type Ingress = RelayIngress;

    fn capability(&self, _attempt: &Attempt<'_>) -> KimiCapability {
        KimiCapability
    }

    async fn execute(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<RelayIngress>, ExecError> {
        let document = request.validated.view().document;
        let stream = streaming(document).map_err(encoding)?;
        let shape = Shape::new(attempt, request)?;
        let mode = if stream {
            BodyMode::Streaming
        } else {
            BodyMode::Buffered
        };
        let (url, body, ingress) = if shape.source == Format::OpenAIResponse {
            let egress = ResponsesEgress {
                destination: DESTINATION,
                upstream_stream: stream,
            };
            let upload = copy(&egress.encode(&request.validated, streams.request).map_err(encoding)?)?;
            let body = shape.responses(upload)?;
            let limit = kimi::RESPONSES_LINE_LIMIT as u64;
            let ingress = ResponsesIngress::new(streams.response, attempt.identity, Operation::Generate, mode, limit)
                .map_err(encoding)?;
            let url = kimi::responses_url(attempt.credential);
            (url, body, RelayIngress::Responses(Box::new(ingress)))
        } else {
            let egress = ChatEgress {
                destination: DESTINATION,
                upstream_stream: stream,
            };
            let upload = copy(&egress.encode(&request.validated, streams.request).map_err(encoding)?)?;
            let body = shape.chat(upload, stream)?;
            let (store, identity, limit) = (attempt.store, attempt.identity, kimi::CHAT_LINE_LIMIT as u64);
            let ingress = ChatIngress::new(streams.response, store, identity, Operation::Generate, mode, limit)
                .map_err(encoding)?;
            (
                kimi::chat_url(attempt.credential),
                body,
                RelayIngress::Chat(Box::new(ingress)),
            )
        };
        let (credential, client) = (attempt.credential, &request.client);
        let headers = kimi::headers(credential, client.headers, client.session, stream);
        let http = self.client(credential, attempt.cfg);
        let upstream = kimi::post(&http, credential, client.usage, &url, headers, body).await?;
        let mut body = observed(upstream.body, client.usage);
        if stream && matches!(ingress, RelayIngress::Chat(_)) {
            body = kimi_sse::completed(body);
        }
        Ok(Upstream {
            status: upstream.status,
            headers: upstream.headers,
            body,
            ingress,
        })
    }

    async fn count_tokens(
        &self,
        _attempt: &Attempt<'_>,
        _request: &Request<'_>,
        _streams: Streams<'_>,
    ) -> Result<Counted<RelayIngress>, ExecError> {
        // [`KimiCapability`] sends every count back to the old route at the fence.
        Err(encoding(codec::Error::Destination))
    }

    async fn compact(
        &self,
        _attempt: &Attempt<'_>,
        _request: &Request<'_>,
        _streams: Streams<'_>,
    ) -> Result<Upstream<RelayIngress>, ExecError> {
        // Kimi has no native compact; [`KimiCapability`] refuses it at the fence.
        Err(encoding(codec::Error::Unsupported))
    }
}

/// What the upload shaping reads besides the upload itself.
struct Shape<'r> {
    attempt: &'r Attempt<'r>,
    request: &'r Request<'r>,
    /// The client's protocol, named as payload rules name it.
    source: Format,
    /// The upstream model without its thinking suffix, as payload rules match it.
    base_model: String,
    /// Whether the client's request has a `stream` member.
    stream_member: bool,
}

impl<'r> Shape<'r> {
    fn new(attempt: &'r Attempt<'r>, request: &'r Request<'r>) -> Result<Self, ExecError> {
        let document = request.validated.view().document;
        Ok(Self {
            attempt,
            request,
            source: client(document).map_err(encoding)?,
            base_model: parse_suffix(attempt.model).model_name,
            stream_member: document.member(document.root(), "stream").map_err(encoding)?.is_some(),
        })
    }

    /// The old path's Chat Completions body (`execute_chat`).
    fn chat(&self, upload: Vec<u8>, stream: bool) -> Result<Vec<u8>, ExecError> {
        let (baseline, requested) = self.baseline(&upload);
        let mut body = self.model(upload);
        // The egress writes `stream`; the old path sends it only when the client did.
        if !(stream || self.stream_member) {
            drop_stream(&mut body);
        }
        // No translator targets Kimi's Chat dialect, so the thinking target is `kimi`.
        body = self.thinking(body, Format::OpenAI.as_str(), "kimi")?;
        if stream && !gj::set_raw(&mut body, "stream_options.include_usage", "true") {
            return Err(ExecError::local(
                500,
                FailureScope::Request,
                "kimi executor: failed to set stream_options in payload",
            ));
        }
        body = kimi::normalize_tool_message_links(body)?;
        body = kimi::normalize_tools(body);
        body = kimi::normalize_temperature(body);
        Ok(self.rules(Format::OpenAI, body, baseline, &requested))
    }

    /// The old path's native Responses body (`execute_responses`), which forwarded the
    /// client's own body.
    fn responses(&self, mut upload: Vec<u8>) -> Result<Vec<u8>, ExecError> {
        client_input(&mut upload, self.request.validated.view().document).map_err(encoding)?;
        let (baseline, requested) = self.baseline(&upload);
        // The egress wrote `stream` as the old path set it.
        let mut body = self.model(upload);
        body = self.thinking(body, Format::OpenAIResponse.as_str(), Format::Codex.as_str())?;
        body = kimi::normalize_responses_input(body);
        body = kimi::normalize_tools(body);
        body = kimi::normalize_temperature(body);
        Ok(self.rules(Format::OpenAIResponse, body, baseline, &requested))
    }

    /// The client's model, and the client's request as the old path's default rules read it
    /// when a default rule does: the upload without the `stream` the egress wrote for a Chat or
    /// Responses client that sent none (a Gemini client's old baseline had one). Only default
    /// rules check a baseline, so only they pay for a copy.
    // ponytail: with default rules the baseline is a second heap body until the rules ran,
    // because ck_wire::payload reads it as one slice; a payload reader over the upload's scratch
    // lease would keep it in pool storage.
    fn baseline(&self, upload: &[u8]) -> (Option<Vec<u8>>, String) {
        let requested = gj::get(upload, "model").str().trim().to_owned();
        let baseline = ck_wire::payload::Rules::of(self.attempt.cfg).has_defaults().then(|| {
            let mut baseline = upload.to_vec();
            if !self.stream_member && self.source != Format::Gemini {
                drop_stream(&mut baseline);
            }
            baseline
        });
        (baseline, requested)
    }

    /// Names the normalized upstream model and reports it.
    fn model(&self, mut body: Vec<u8>) -> Vec<u8> {
        let model = kimi::normalize_upstream_model(&self.base_model);
        gj::set_str(&mut body, "model", &model);
        self.request.client.usage.upstream_model(&model);
        body
    }

    /// `helps.ApplyRequestThinking` on the upload, which is already in protocol `from`; the
    /// body is edited, not copied.
    fn thinking(&self, body: Vec<u8>, from: &str, to: &str) -> Result<Vec<u8>, ExecError> {
        let caps = self.attempt.resolved_model.map(|r| ModelCaps::from(&r.info));
        let request = RequestThinking {
            payload: None,
            original: None,
            model: self.attempt.model,
            from,
            to,
            provider: "kimi",
            resolved: caps.as_ref().map(Some),
            has_request_transformer: false,
            updates_changed: false,
        };
        apply_request_thinking(&request, body)
            .map_err(|e| ExecError::local(e.status(), FailureScope::Request, e.message))
    }

    /// Reports the body sent upstream, then applies the configured payload rules, the last
    /// change to it (868e137). `requested` is the client's model, which every request fence
    /// requires.
    fn rules(&self, protocol: Format, body: Vec<u8>, baseline: Option<Vec<u8>>, requested: &str) -> Vec<u8> {
        let client = &self.request.client;
        client.usage.request_for("kimi", &body);
        ck_wire::payload::apply(
            &ck_wire::payload::Rules::of(self.attempt.cfg),
            &ck_wire::payload::Request {
                target_executor: "",
                model: &self.base_model,
                requested_model: requested,
                protocol: protocol.as_str(),
                from_protocol: self.source.as_str(),
                root: "",
                original: baseline.as_deref().unwrap_or_default(),
                request_path: client.route,
                headers: Some(client.headers),
            },
            body,
        )
    }
}

/// The client's protocol, as `Format` names it.
fn client(doc: &Document) -> codec::Result<Format> {
    let x = doc.required(doc.root(), "x_ck")?;
    let protocol = doc.required(x, "original_protocol")?;
    for (name, format) in [
        ("chat", Format::OpenAI),
        ("responses", Format::OpenAIResponse),
        ("anthropic", Format::Claude),
        ("gemini", Format::Gemini),
    ] {
        if doc.string_is(protocol, name)? {
            return Ok(format);
        }
    }
    Err(codec::Error::Schema)
}

/// The upstream body captured as it arrives, with the failure that ends it.
fn observed(body: Body, usage: &UsageSink) -> Body {
    let capture = usage.capture().clone();
    body.inspect(move |item| match item {
        Ok(chunk) if !chunk.is_empty() && capture.logs_responses() => capture_chunk(&capture, chunk),
        Err(error) => capture_error(&capture, error),
        _ => {}
    })
    .boxed()
}

/// Removes the root `stream` member the egress wrote, in place.
fn drop_stream(body: &mut Vec<u8>) {
    let stream = gj::get(body, "stream");
    if stream.exists() {
        let value = kimi_edit::span(&stream);
        kimi_edit::remove_member(body, value);
    }
}

/// A Responses client's string `input`, which the egress writes as the one message it became,
/// goes as the string, as the old path forwarded it: the string moves into place in the body.
/// Responses ingress marks that message's content with an `/input` original.
fn client_input(body: &mut Vec<u8>, doc: &Document) -> codec::Result<()> {
    if !string_input(doc)? {
        return Ok(());
    }
    let input = kimi_edit::span(&gj::get(body, "input"));
    let content = kimi_edit::span(&gj::get(body, "input.0.content"));
    let end = input.start + content.len();
    body.copy_within(content, input.start);
    body.drain(end..input.end);
    Ok(())
}

/// Whether a Responses client sent `input` as a string: Responses ingress binds an `/input`
/// original to the content that string became.
fn string_input(doc: &Document) -> codec::Result<bool> {
    let Some(x) = member(doc, doc.root(), "x_ck")? else {
        return Ok(false);
    };
    let Some(original) = member(doc, x, "original")? else {
        return Ok(false);
    };
    for entry in doc.children(original)? {
        if let Some(pointer) = member(doc, entry, "pointer")?
            && text_is(doc, pointer, "/input", false)?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Whether a Responses request declares, chooses or replays Codex's freeform `apply_patch`
/// tool. Only the carriers Go's bridge rewrites are read: the native tool declarations
/// (`x_ck.tools`), the root `tool_choice`, and each turn's tool calls and `additional_tools`
/// items. Arguments, schemas and unknown members are never searched.
fn custom_apply_patch(doc: &Document) -> codec::Result<bool> {
    let root = doc.root();
    if let Some(x) = member(doc, root, "x_ck")?
        && let Some(tools) = member(doc, x, "tools")?
    {
        for entry in doc.children(tools)? {
            if let Some(declaration) = member(doc, entry, "declaration")?
                && declares(doc, declaration)?
            {
                return Ok(true);
            }
        }
    }
    if let Some(choice) = member(doc, root, "tool_choice")?
        && chooses(doc, choice)?
    {
        return Ok(true);
    }
    let Some(messages) = member(doc, root, "messages")? else {
        return Ok(false);
    };
    for message in doc.children(messages)? {
        if replays(doc, message)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// A custom `apply_patch` tool, or a namespace whose first tool list holds one.
fn declares(doc: &Document, tool: usize) -> codec::Result<bool> {
    if named_patch(doc, tool, "custom")? {
        return Ok(true);
    }
    if !kind_is(doc, tool, "namespace")? {
        return Ok(false);
    }
    for key in ["tools", "children"] {
        if let Some(list) = member(doc, tool, key)?
            && matches!(doc.node(list)?.value, Value::Array)
        {
            for child in doc.children(list)? {
                if declares(doc, child)? {
                    return Ok(true);
                }
            }
            return Ok(false);
        }
    }
    Ok(false)
}

/// A custom `apply_patch` choice, alone or in the tools an `allowed_tools` choice allows. Other
/// members of a choice are not choices.
fn chooses(doc: &Document, choice: usize) -> codec::Result<bool> {
    if named_patch(doc, choice, "custom")? {
        return Ok(true);
    }
    if !kind_is(doc, choice, "allowed_tools")? {
        return Ok(false);
    }
    let Some(tools) = member(doc, choice, "tools")? else {
        return Ok(false);
    };
    for child in doc.children(tools)? {
        if named_patch(doc, child, "custom")? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// A turn that calls the custom `apply_patch` tool, or an `additional_tools` item (kept as an
/// opaque Responses part) that declares it.
fn replays(doc: &Document, message: usize) -> codec::Result<bool> {
    if let Some(calls) = member(doc, message, "tool_calls")? {
        for call in doc.children(calls)? {
            if kind_is(doc, call, "custom")?
                && let Some(function) = member(doc, call, "function")?
                && let Some(name) = member(doc, function, "name")?
                && text_is(doc, name, "apply_patch", true)?
            {
                return Ok(true);
            }
        }
    }
    let Some(content) = member(doc, message, "content")? else {
        return Ok(false);
    };
    for part in doc.children(content)? {
        if kind_is(doc, part, "opaque")?
            && let Some(item) = member(doc, part, "value")?
            && kind_is(doc, item, "additional_tools")?
            && let Some(tools) = member(doc, item, "tools")?
        {
            for tool in doc.children(tools)? {
                if declares(doc, tool)? {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}

/// An object of type `kind` whose `name` is `apply_patch` (Go trims the name).
fn named_patch(doc: &Document, object: usize, kind: &str) -> codec::Result<bool> {
    if !kind_is(doc, object, kind)? {
        return Ok(false);
    }
    match member(doc, object, "name")? {
        Some(name) => text_is(doc, name, "apply_patch", true),
        None => Ok(false),
    }
}

/// Whether node `object` is an object whose `type` is the string `kind`.
fn kind_is(doc: &Document, object: usize, kind: &str) -> codec::Result<bool> {
    match member(doc, object, "type")? {
        Some(value) => text_is(doc, value, kind, false),
        None => Ok(false),
    }
}

/// Member `name` of node `object`; `None` when the node is not an object.
fn member(doc: &Document, object: usize, name: &str) -> codec::Result<Option<usize>> {
    if !matches!(doc.node(object)?.value, Value::Object) {
        return Ok(None);
    }
    doc.member(object, name)
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
