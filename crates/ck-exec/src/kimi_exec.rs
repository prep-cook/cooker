//! Kimi execution over the internal stream. Chat and Gemini clients use Chat egress;
//! Responses clients use native Responses egress, including grammar custom tools.
//!
//! Upload shaping applies the credential product, model-specific reasoning, message and
//! tool normalization, then configured payload rules. Temperature reaches upstream unchanged.
//! Messages and token counts retain the delegated path until its contract seam lands.
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
use ck_wire::thinking::{ModelCaps, RequestThinking, parse_suffix};
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
            Operation::Media(_) | Operation::AlphaSearch => return Err(codec::Error::Unsupported),
        }
        match client(view.document)? {
            Format::Claude => Err(codec::Error::Destination),
            Format::OpenAIResponse => {
                ResponsesCapability.validate(view)?;
                validate_custom_tools(view.document)
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
        request: &mut Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<RelayIngress>, ExecError> {
        let document = request.validated().view().document;
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
            let upload = copy(&egress.encode(&request.validated(), streams.request).map_err(encoding)?)?;
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
            let upload = copy(&egress.encode(&request.validated(), streams.request).map_err(encoding)?)?;
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
        _request: &mut Request<'_>,
        _streams: Streams<'_>,
    ) -> Result<Counted<RelayIngress>, ExecError> {
        // [`KimiCapability`] sends every count back to the old route at the fence.
        Err(encoding(codec::Error::Destination))
    }

    async fn compact(
        &self,
        _attempt: &Attempt<'_>,
        _request: &mut Request<'_>,
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
        let document = request.validated().view().document;
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
        // Kimi policy chooses the model's Chat reasoning dialect.
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
        Ok(self.rules(Format::OpenAI, body, baseline, &requested))
    }

    /// The old path's native Responses body (`execute_responses`), which forwarded the
    /// client's own body.
    fn responses(&self, mut upload: Vec<u8>) -> Result<Vec<u8>, ExecError> {
        client_input(&mut upload, self.request.validated().view().document).map_err(encoding)?;
        let (baseline, requested) = self.baseline(&upload);
        // The egress wrote `stream` as the old path set it.
        let mut body = self.model(upload);
        body = self.thinking(body, Format::OpenAIResponse.as_str(), Format::Codex.as_str())?;
        body = kimi::normalize_responses_input(body);
        body = kimi::normalize_tools(body);
        Ok(self.rules(Format::OpenAIResponse, body, baseline, &requested))
    }

    /// The client's model, and the client's request as the old path's default rules read it
    /// (`Rules::baseline`, when a default rule is configured): the upload without the `stream`
    /// the egress wrote for a Chat or Responses client that sent none (a Gemini client's old
    /// baseline had one).
    fn baseline(&self, upload: &[u8]) -> (Option<Vec<u8>>, String) {
        let requested = gj::get(upload, "model").str().trim().to_owned();
        let mut baseline = ck_wire::payload::Rules::of(self.attempt.cfg).baseline("", upload);
        if let Some(baseline) = baseline.as_mut()
            && !self.stream_member
            && self.source != Format::Gemini
        {
            drop_stream(baseline);
        }
        (baseline, requested)
    }

    /// Names the normalized upstream model and reports it.
    fn model(&self, mut body: Vec<u8>) -> Vec<u8> {
        let model = kimi::upstream_model(self.attempt.credential, &self.base_model);
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
        kimi::apply_kimi_thinking(self.attempt.credential, &request, body)
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

/// Validate declarations only; native choices and replay items retain their wire shapes.
fn validate_custom_tools(doc: &Document) -> codec::Result<()> {
    let Some(x) = member(doc, doc.root(), "x_ck")? else {
        return Ok(());
    };
    let Some(tools) = member(doc, x, "tools")? else {
        return Ok(());
    };
    for entry in doc.children(tools)? {
        if let Some(tool) = member(doc, entry, "declaration")? {
            validate_custom_tool(doc, tool)?;
        }
    }
    Ok(())
}

fn validate_custom_tool(doc: &Document, tool: usize) -> codec::Result<()> {
    if kind_is(doc, tool, "custom")? {
        let format = doc.required(tool, "format")?;
        if !kind_is(doc, format, "grammar")?
            || !text_is(doc, doc.required(format, "syntax")?, "lark", false)?
            || !matches!(
                doc.node(doc.required(format, "definition")?)?.value,
                Value::String(_) | Value::Text(_)
            )
        {
            return Err(codec::Error::Schema);
        }
    }
    if kind_is(doc, tool, "namespace")? {
        for key in ["tools", "children"] {
            if let Some(list) = member(doc, tool, key)? {
                for child in doc.children(list)? {
                    validate_custom_tool(doc, child)?;
                }
            }
        }
    }
    Ok(())
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
