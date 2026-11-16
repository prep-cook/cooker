//! The OpenAI-compatible executor over the internal stream (`docs/EXECUTION-CONTRACT.md`):
//! generation goes to `/chat/completions` through Chat request egress, a native compact to
//! `/responses/compact` through Responses request egress, and a token count is local.
//!
//! The executor adjusts its own encoded upload for the relay, never the client body, in Go's
//! order: the upstream model, the model suffix's thinking, the configured compatibility
//! rewrites, the prompt cache key, `stream_options`, and the payload rules last.

use bytes::Bytes;
use ck_core::exec::{ExecError, FailureScope};
use ck_core::format::Format;
use ck_core::gostr::GoStr;
use ck_stream::{ScratchLease, Writer};
use ck_translate::codec::chat::request::{ChatCapability, Egress as ChatEgress};
use ck_translate::codec::chat::response::Ingress as ChatIngress;
use ck_translate::codec::document::{Document, Value};
use ck_translate::codec::{self, BodyMode, Capability, Media, Operation, RequestEgress, RequestFence, RequestView};
use ck_translate::responses::request::{Egress as ResponsesEgress, ResponsesCapability};
use ck_translate::responses::response::Ingress as ResponsesIngress;
use ck_wire::json as gj;
use ck_wire::thinking::{self, ModelCaps, RequestThinking, parse_suffix};

use crate::openai_compat::{self as compat, OpenAICompatExecutor};
use crate::openai_compat_http as wire;
use crate::openai_compat_payload as payload;
use crate::types::{Attempt, Counted, Execute, Request, Streams, Upstream};
use crate::upstream::{record_answer, record_failure, record_send};

/// The capability key of OpenAI-compatible relays.
pub const DESTINATION: &str = "openai-compat";

/// The Chat generation path under a relay's base URL.
pub(crate) const CHAT_PATH: &str = "/chat/completions";

/// The native compact path under a relay's base URL.
const COMPACT_PATH: &str = "/responses/compact";

/// Chat generation, its token count, the Responses native compact, and image generation and
/// edits (`openai_compat_media`).
pub struct RelayCapability;

impl Capability for RelayCapability {
    fn destination(&self) -> &'static str {
        DESTINATION
    }

    fn admits(&self, operation: Operation) -> bool {
        operation.is_text() || matches!(operation, Operation::Media(Media::ImageGenerate | Media::ImageEdit))
    }

    fn validate(&self, view: RequestView<'_>) -> codec::Result<()> {
        match view.operation {
            Operation::Generate => ChatCapability.validate(view),
            // A count is valid exactly when the generation it projects is.
            Operation::CountTokens => ChatCapability.validate(RequestView {
                operation: Operation::Generate,
                ..view
            }),
            Operation::Compact => ResponsesCapability.validate(view),
            Operation::Media(Media::ImageGenerate | Media::ImageEdit) => {
                crate::media_exec::check_images(view, ck_translate::images::Destination::OpenAi)
            }
            Operation::Media(_) | Operation::AlphaSearch => Err(codec::Error::Unsupported),
        }
    }
}

/// Model-specific relay semantics, selected before any attempt sends bytes.
pub struct ModelCapability {
    glm: bool,
}

impl ModelCapability {
    fn new(attempt: &Attempt<'_>) -> Self {
        let (model, _) = thinking::split_suffix(attempt.model);
        Self {
            glm: thinking::glm::is_model(model),
        }
    }
}

impl Capability for ModelCapability {
    fn destination(&self) -> &'static str {
        if self.glm { "glm-chat" } else { DESTINATION }
    }

    fn admits(&self, operation: Operation) -> bool {
        if self.glm {
            operation.is_text()
        } else {
            RelayCapability.admits(operation)
        }
    }

    fn validate(&self, view: RequestView<'_>) -> codec::Result<()> {
        if self.glm {
            ck_translate::codec::chat::request::GlmCapability.validate(view)
        } else {
            RelayCapability.validate(view)
        }
    }
}

/// The response decoder of the upstream protocol the operation used. Boxed: the producer
/// task moves it once, and the variants differ in size.
pub enum RelayIngress {
    Chat(Box<ChatIngress>),
    Responses(Box<ResponsesIngress>),
    Media(crate::media_exec::MediaIngress),
}

impl codec::ResponseIngress for RelayIngress {
    fn flush_deadline(&self) -> Option<std::time::Instant> {
        match self {
            Self::Chat(ingress) => ingress.flush_deadline(),
            Self::Responses(ingress) => ingress.flush_deadline(),
            Self::Media(ingress) => ingress.flush_deadline(),
        }
    }

    fn flush(&mut self, owner: &mut Writer) -> codec::Result<()> {
        match self {
            Self::Chat(ingress) => ingress.flush(owner),
            Self::Responses(ingress) => ingress.flush(owner),
            Self::Media(ingress) => ingress.flush(owner),
        }
    }

    fn advance(&mut self, owner: &mut Writer, bytes: &[u8]) -> codec::Result<()> {
        match self {
            Self::Chat(ingress) => ingress.advance(owner, bytes),
            Self::Responses(ingress) => ingress.advance(owner, bytes),
            Self::Media(ingress) => ingress.advance(owner, bytes),
        }
    }

    fn finish(&mut self, owner: &mut Writer) -> codec::Result<()> {
        match self {
            Self::Chat(ingress) => ingress.finish(owner),
            Self::Responses(ingress) => ingress.finish(owner),
            Self::Media(ingress) => ingress.finish(owner),
        }
    }

    fn fail(&mut self, owner: &mut Writer, status: u16, scope: &'static str) -> codec::Result<()> {
        match self {
            Self::Chat(ingress) => ingress.fail(owner, status, scope),
            Self::Responses(ingress) => ingress.fail(owner, status, scope),
            Self::Media(ingress) => ingress.fail(owner, status, scope),
        }
    }
}

impl Execute for OpenAICompatExecutor {
    type Capability = ModelCapability;
    type Ingress = RelayIngress;

    fn capability(&self, attempt: &Attempt<'_>) -> ModelCapability {
        ModelCapability::new(attempt)
    }

    async fn execute(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<RelayIngress>, ExecError> {
        if let Operation::Media(media) = request.validated.view().operation {
            return crate::openai_compat_media::images(attempt, request, streams, self.route(attempt)?, media).await;
        }
        let upstream = chat(attempt, request, streams, self.route(attempt)?).await?;
        Ok(Upstream {
            status: upstream.status,
            headers: upstream.headers,
            body: upstream.body,
            ingress: RelayIngress::Chat(Box::new(upstream.ingress)),
        })
    }

    async fn count_tokens(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Counted<RelayIngress>, ExecError> {
        Ok(Counted::Local {
            input_tokens: count(attempt, request, streams)?,
        })
    }

    async fn compact(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<RelayIngress>, ExecError> {
        let route = self.route(attempt)?;
        let egress = ResponsesEgress {
            destination: DESTINATION,
            upstream_stream: false,
        };
        let upload = copy(&egress.encode(&request.validated, streams.request).map_err(encoding)?)?;
        let relay = Relay::new(attempt, request)?;
        let body = relay.compact(upload, route.compat.as_ref())?;
        let ingress = ResponsesIngress::new(
            streams.response,
            attempt.identity,
            Operation::Compact,
            BodyMode::Buffered,
            compat::MAX_LINE as u64,
        )
        .map_err(encoding)?;
        let upstream = send(&route, attempt, request, COMPACT_PATH, &relay.base_model, body, false).await?;
        Ok(Upstream {
            status: upstream.status,
            headers: upstream.headers,
            body: upstream.body,
            ingress: RelayIngress::Responses(Box::new(ingress)),
        })
    }
}

impl OpenAICompatExecutor {
    /// The relay's own route: the credential's base URL and key, and its compatibility config.
    fn route(&self, attempt: &Attempt<'_>) -> Result<Route, ExecError> {
        let credential = attempt.credential;
        let (base_url, key) = compat::credentials(credential);
        if base_url.is_empty() {
            return Err(compat::missing_base_url());
        }
        Ok(Route {
            client: self.client(credential, attempt.cfg),
            base_url,
            key,
            api_auth: true,
            compat: payload::resolve_compat(credential, attempt.cfg),
        })
    }
}

/// Where a relay request goes: the HTTP client, the base URL and bearer key, and the
/// compatibility config, which another executor's route (Vertex `endpoints/openapi`) has not.
pub(crate) struct Route {
    pub client: wreq::Client,
    pub base_url: String,
    pub key: String,
    pub api_auth: bool,
    pub compat: Option<payload::Compat>,
}

/// A Chat generation on `route`, encoded with the Chat request egress for [`DESTINATION`].
pub(crate) async fn chat(
    attempt: &Attempt<'_>,
    request: &Request<'_>,
    streams: Streams<'_>,
    route: Route,
) -> Result<Upstream<ChatIngress>, ExecError> {
    let document = request.validated.view().document;
    let stream = streaming(document).map_err(encoding)?;
    let egress = ChatEgress {
        destination: request.validated.destination(),
        upstream_stream: stream,
    };
    let upload = copy(&egress.encode(&request.validated, streams.request).map_err(encoding)?)?;
    let relay = Relay::new(attempt, request)?;
    let body = relay.chat(upload, stream, route.compat.as_ref())?;
    let mode = if stream {
        BodyMode::Streaming
    } else {
        BodyMode::Buffered
    };
    let ingress = ChatIngress::new(
        streams.response,
        attempt.store,
        attempt.identity,
        Operation::Generate,
        mode,
        compat::MAX_LINE as u64,
    )
    .map_err(encoding)?
    .with_glm(thinking::glm::is_model(&relay.base_model));
    let upstream = send(&route, attempt, request, CHAT_PATH, &relay.base_model, body, stream).await?;
    Ok(Upstream {
        status: upstream.status,
        headers: upstream.headers,
        body: upstream.body,
        ingress,
    })
}

/// The local token count of a count request: the Chat body its generation would send,
/// before the relay's compatibility rewrites.
pub(crate) fn count(attempt: &Attempt<'_>, request: &Request<'_>, streams: Streams<'_>) -> Result<u64, ExecError> {
    let view = RequestView {
        operation: Operation::Generate,
        ..request.validated.view()
    };
    let generate = RequestFence::default()
        .end(view, &ModelCapability::new(attempt))
        .map_err(encoding)?;
    let egress = ChatEgress {
        destination: generate.destination(),
        upstream_stream: false,
    };
    let upload = copy(&egress.encode(&generate, streams.request).map_err(encoding)?)?;
    let relay = Relay::new(attempt, request)?;
    let body = relay.count(upload)?;
    let count = payload::count_chat_tokens(&relay.base_model, &body).map_err(|e| {
        ExecError::local(
            500,
            FailureScope::Request,
            format!("openai compat executor: token counting failed: {e}"),
        )
    })?;
    Ok(u64::try_from(count).unwrap_or_default())
}

/// Posts `body` to `path` under the route's base URL with its key and the credential's custom
/// headers, recording the request-side facts of `model`'s call (contract G4); a non-2xx answer
/// is the attempt's failure with the upstream's status and body as received.
pub(crate) async fn send(
    route: &Route,
    attempt: &Attempt<'_>,
    request: &Request<'_>,
    path: &str,
    model: &str,
    body: Vec<u8>,
    stream: bool,
) -> Result<wire::Upstream, ExecError> {
    let mut headers = crate::openai_compat_headers::base_headers(&route.key, "application/json", route.api_auth);
    let session = ck_wire::session::cpa_session_id(request.client.session);
    for (name, value) in ck_wire::headers::custom_headers(
        &attempt.credential.attributes,
        request.client.headers,
        session.as_deref(),
    ) {
        headers.set(&name, value);
    }
    if route.api_auth {
        headers.set("User-Agent", compat::USER_AGENT);
    }
    if stream {
        headers.set("Accept", "text/event-stream");
        headers.set("Cache-Control", "no-cache");
    }
    let url = compat::endpoint(&route.base_url, path);
    // Refused before anything is sent, so the key never reaches a host the url crate
    // reads differently from Go (`openai_compat::chat_with`).
    crate::xai_url::new_request(&url)?;
    if let Some(error) = crate::xai_url::transport_refusal(&url) {
        return Err(error);
    }
    let usage = request.client.usage;
    if usage.enabled() {
        let target = if path == COMPACT_PATH {
            Format::OpenAIResponse
        } else {
            Format::OpenAI
        };
        usage.request(target, &body);
    }
    usage.upstream_model(model);
    let body = Bytes::from(body);
    let provider = &attempt.credential.provider;
    record_send(
        usage,
        true,
        attempt.credential,
        provider,
        &url,
        (headers.pairs(), &body),
    );
    let upstream = wire::send(&route.client, &url, headers, body)
        .await
        .inspect_err(|error| record_failure(usage, error))?;
    let upstream = wire::Upstream {
        body: record_answer(usage, true, upstream.status, &upstream.headers, upstream.body),
        ..upstream
    };
    if (200..300).contains(&upstream.status) {
        return Ok(upstream);
    }
    let (status, headers, body) = wire::read_error_body(upstream).await;
    Err(compat::status_error(status, headers, body, true))
}

/// What `prepare` returns: the body, the payload-rule baseline and the client's model.
type Prepared = (Vec<u8>, Option<Vec<u8>>, String);

/// What the upload adjustments read besides the upload itself.
struct Relay<'r> {
    attempt: &'r Attempt<'r>,
    request: &'r Request<'r>,
    /// The upstream model without its thinking suffix.
    base_model: String,
    /// The client's protocol, named as payload rules and cache keys name it.
    source: &'static str,
}

impl<'r> Relay<'r> {
    fn new(attempt: &'r Attempt<'r>, request: &'r Request<'r>) -> Result<Self, ExecError> {
        Ok(Self {
            attempt,
            request,
            base_model: parse_suffix(attempt.model).model_name.trim().to_owned(),
            source: source(request.validated.view().document).map_err(encoding)?,
        })
    }

    /// A `/chat/completions` body.
    fn chat(&self, upload: Vec<u8>, stream: bool, compat: Option<&payload::Compat>) -> Result<Vec<u8>, ExecError> {
        let (mut body, original, requested) = self.prepare(upload, Format::OpenAI)?;
        if payload::excludes_images(compat, &self.base_model, &requested) {
            body = payload::normalize_tool_results_text_only(body);
        }
        let mct = payload::uses_max_completion_tokens(compat, &self.base_model, &requested);
        body = payload::normalize_max_tokens(body, mct);
        self.prompt_cache_key(compat, &mut body);
        if stream {
            payload::set_bool_if_different(&mut body, "stream_options.include_usage", true);
        }
        Ok(self.rules(Format::OpenAI, original, &requested, body))
    }

    /// A `/responses/compact` body.
    fn compact(&self, upload: Vec<u8>, compat: Option<&payload::Compat>) -> Result<Vec<u8>, ExecError> {
        let (mut body, original, requested) = self.prepare(upload, Format::OpenAIResponse)?;
        if payload::excludes_images(compat, &self.base_model, &requested) {
            body = payload::normalize_tool_results_text_only(body);
        }
        body = payload::sanitize_reasoning_encrypted_content(body);
        Ok(self.rules(Format::OpenAIResponse, original, &requested, body))
    }

    /// The Chat body a local count measures: the generation's body before the relay's
    /// compatibility rewrites.
    fn count(&self, upload: Vec<u8>) -> Result<Vec<u8>, ExecError> {
        let (body, original, requested) = self.prepare(upload, Format::OpenAI)?;
        Ok(self.rules(Format::OpenAI, original, &requested, body))
    }

    /// Sets the upstream model and applies the suffix's thinking. Returns the body, the
    /// payload-rule baseline (`Rules::baseline` of the upload with the upstream model) and the
    /// client's model.
    fn prepare(&self, mut upload: Vec<u8>, target: Format) -> Result<Prepared, ExecError> {
        let requested = gj::get(&upload, "model").str().trim().to_owned();
        let requested = if requested.is_empty() {
            self.attempt.model.trim().to_owned()
        } else {
            requested
        };
        payload::set_str_if_different(&mut upload, "model", &self.base_model);
        let baseline = ck_wire::payload::Rules::of(self.attempt.cfg).baseline("", &upload);
        let caps = self.attempt.resolved_model.map(|r| ModelCaps::from(&r.info));
        let body = thinking::apply_request_thinking(
            &RequestThinking {
                payload: None,
                original: None,
                model: self.attempt.model,
                // The upload is already in the target protocol.
                from: target.as_str(),
                to: target.as_str(),
                provider: &self.attempt.credential.provider,
                resolved: caps.as_ref().map(Some),
                has_request_transformer: false,
                updates_changed: false,
            },
            upload,
        )
        .map_err(|e| ExecError::local(400, FailureScope::Request, e.message))?;
        Ok((body, baseline, requested))
    }

    /// `applyPromptCacheKey`: the request's own key, else a Claude Code session's, else one
    /// derived from the provider session.
    fn prompt_cache_key(&self, compat: Option<&payload::Compat>, body: &mut Vec<u8>) {
        if !compat.is_some_and(|c| c.support_prompt_cache_key) {
            return;
        }
        let explicit = gj::get(body, "prompt_cache_key").str().trim().to_owned();
        let model = gj::get(body, "model").str().trim().to_owned();
        let client = &self.request.client;
        let key = if explicit.is_empty() {
            (self.source == Format::Claude.as_str())
                .then(|| payload::claude_code_prompt_cache(&model, body, client.headers))
                .flatten()
                .or_else(|| {
                    let sessions = (client.execution_session, client.derived_session);
                    session_cache_key(&self.attempt.credential.provider, &model, self.source, sessions)
                })
        } else {
            Some(explicit)
        };
        if let Some(key) = key {
            payload::set_str_if_different(body, "prompt_cache_key", &key);
        }
    }

    /// The configured payload rules, the last change to the body.
    fn rules(&self, target: Format, original: Option<Vec<u8>>, requested: &str, body: Vec<u8>) -> Vec<u8> {
        let rules = ck_wire::payload::Rules::of(self.attempt.cfg);
        ck_wire::payload::apply(
            &rules,
            &ck_wire::payload::Request {
                target_executor: "",
                model: &self.base_model,
                requested_model: requested,
                protocol: target.as_str(),
                from_protocol: self.source,
                root: "",
                original: original.as_deref().unwrap_or_default(),
                request_path: self.request.client.route,
                headers: Some(self.request.client.headers),
            },
            body,
        )
    }
}

/// The prompt cache key of a provider session: a stable UUID over the provider, the model, the
/// client protocol and [`provider_session_uuid`]; `None` without a session.
pub(crate) fn session_cache_key(
    provider: &str,
    model: &str,
    source: &str,
    sessions: (Option<&str>, Option<&str>),
) -> Option<String> {
    let session = provider_session_uuid(provider, sessions)?;
    let identity = format!(
        "cli-proxy-api:openai-compat:prompt-cache\0{}\0{}\0{}\0{session}",
        provider.trim().go_lower(),
        model.go_lower(),
        source.trim().go_lower()
    );
    Some(uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, identity.as_bytes()).to_string())
}

/// `helps.ProviderSessionUUID`: the execution session, else the derived session identity,
/// as a provider-scoped stable UUID.
fn provider_session_uuid(provider: &str, (execution, derived): (Option<&str>, Option<&str>)) -> Option<String> {
    let nonempty = |s: Option<&str>| s.map(str::trim).filter(|s| !s.is_empty()).map(str::to_owned);
    let (kind, value) = match nonempty(execution) {
        Some(execution) => ("execution-session", execution),
        None => ("derived-session", nonempty(derived)?),
    };
    let provider = provider.trim().go_lower();
    if provider.is_empty() {
        return None;
    }
    let identity = format!("cli-proxy-api\0{provider}\0{kind}\0{value}");
    Some(uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, identity.as_bytes()).to_string())
}

/// The client's protocol, as `Format` names it.
fn source(doc: &Document) -> codec::Result<&'static str> {
    let x = doc.required(doc.root(), "x_ck")?;
    let protocol = doc.required(x, "original_protocol")?;
    for (name, format) in [
        ("chat", Format::OpenAI),
        ("responses", Format::OpenAIResponse),
        ("anthropic", Format::Claude),
        ("gemini", Format::Gemini),
    ] {
        if doc.string_is(protocol, name)? {
            return Ok(format.as_str());
        }
    }
    Err(codec::Error::Schema)
}

/// Whether the upstream streams: as the client's `stream` member says. A Gemini client
/// states it only in its route, which the request handle does not carry, so it streams.
fn streaming(doc: &Document) -> codec::Result<bool> {
    match doc.member(doc.root(), "stream")? {
        Some(node) => Ok(matches!(doc.node(node)?.value, Value::Bool(true))),
        None => Ok(source(doc)? == Format::Gemini.as_str()),
    }
}

/// The encoded upload as one body; the HTTP client takes ownership of it.
fn copy(lease: &ScratchLease) -> Result<Vec<u8>, ExecError> {
    let mut body = Vec::with_capacity(lease.len());
    while body.len() < lease.len() {
        let chunk = lease
            .chunk(body.len())
            .map_err(|e| encoding(codec::Error::Storage(e)))?;
        body.extend_from_slice(chunk);
    }
    Ok(body)
}

/// A failure before anything was sent (contract §9): storage rows by the inner error;
/// anything else is a gateway defect, since the request passed this destination's fence.
pub(crate) fn encoding(error: codec::Error) -> ExecError {
    match error {
        codec::Error::Storage(ck_stream::Error::StorageExhausted | ck_stream::Error::Io(_)) => {
            ExecError::local(503, FailureScope::Request, "stream.storage_exhausted")
        }
        codec::Error::Storage(ck_stream::Error::Gone) => ExecError::local(410, FailureScope::Request, "stream.gone"),
        error => ExecError::local(500, FailureScope::Request, format!("request encoding failed: {error}")),
    }
}
