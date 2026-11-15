//! The Codex executor over the internal stream (`docs/EXECUTION-CONTRACT.md`): generation
//! goes to `/responses` and a native compact to `/responses/compact` through the Responses
//! request egress, and a token count is local (the Codex backend has no count endpoint).
//!
//! The executor adjusts its own encoded upload, never the client body, in Go's order: the
//! Codex backend's required members (`codex_upload`), the model suffix's thinking, the Codex
//! body rules (`codex_request::shape_body`), the reasoning replay of Claude clients, the
//! prompt cache key, the input ids, and the payload rules last. Generation always streams
//! upstream, as the backend requires; the client's own `stream` only picks Go's rule set.
//! Upstream events pass through the Codex event rules (`codex_events`) into the Responses
//! response ingress. The HTTP transport, headers and quota snapshots are the old path's
//! (`CodexExecutor::open`, `codex_request::http_headers`).

use std::sync::Arc;

use ck_core::exec::{ExecError, FailureScope};
use ck_core::format::Format;
use ck_stream::ScratchLease;
use ck_translate::codec::document::{Document, Value};
use ck_translate::codec::{self, BodyMode, Capability, Operation, RequestEgress, RequestFence, RequestView};
use ck_translate::responses::request::{Egress, ResponsesCapability};
use ck_translate::responses::response::Ingress;

use crate::codex::{CodexExecutor, ErrorBody};
use crate::codex_capture::Wire;
use crate::codex_events::{self as events, Events};
use crate::codex_replay::Scope;
use crate::codex_request::{self as request, Call, Settings, View};
use crate::codex_response::SCANNER_MAX_LINE;
use crate::codex_shape::{self, Inbound};
use crate::types::{Attempt, Counted, Execute, Request, Streams, Upstream};

/// The capability key of Codex credentials.
pub const DESTINATION: &str = "codex";

/// Responses generation, its token count, and the Responses native compact.
pub struct CodexCapability;

impl Capability for CodexCapability {
    fn destination(&self) -> &'static str {
        DESTINATION
    }

    fn validate(&self, view: RequestView<'_>) -> codec::Result<()> {
        match view.operation {
            Operation::Generate | Operation::Compact => ResponsesCapability.validate(view),
            // A count is valid exactly when the generation it projects is.
            Operation::CountTokens => ResponsesCapability.validate(RequestView {
                operation: Operation::Generate,
                ..view
            }),
        }
    }
}

impl Execute for CodexExecutor {
    type Capability = CodexCapability;
    type Ingress = Ingress;

    fn capability(&self, _attempt: &Attempt<'_>) -> CodexCapability {
        CodexCapability
    }

    async fn execute(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<Ingress>, ExecError> {
        crate::codex_siwc::require_token(attempt.credential)?;
        let stream = streaming(request.validated.view().document).map_err(encoding)?;
        let upload = encode(&request.validated, streams.request, true)?;
        let shaped = Shape::new(attempt, request, &upload)?;
        let call = if stream { Call::Stream } else { Call::NonStream };
        let scratch = streams.response.allocator();
        let ingress = Ingress::new(
            streams.response,
            attempt.identity,
            Operation::Generate,
            BodyMode::Streaming,
            SCANNER_MAX_LINE as u64,
        )
        .map_err(encoding)?;
        let (view, settings) = shaped.view();
        let body = shaped.generate(&view, &settings, call, &self.replay)?;
        let upstream = self.post_shaped(&view, &settings, &body, call).await?;
        let replay = Some((self.replay.clone(), body.scope));
        let rules = Events::new(scratch, body.optimized, settings.model_level_cooling, replay);
        Ok(Upstream {
            status: upstream.status,
            headers: upstream.headers,
            body: events::body(upstream.body, rules),
            ingress,
        })
    }

    async fn count_tokens(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Counted<Ingress>, ExecError> {
        let view = RequestView {
            operation: Operation::Generate,
            ..request.validated.view()
        };
        let generate = RequestFence::default().end(view, &CodexCapability).map_err(encoding)?;
        let upload = encode(&generate, streams.request, false)?;
        let body = Shape::new(attempt, request, &upload)?.count()?;
        let model = request::base_model(attempt.model);
        let encoding = crate::codex_tokens::encoding_for_model(&model);
        let count = crate::codex_tokens::count_input_tokens(encoding, &body).map_err(|e| {
            ExecError::local(
                500,
                FailureScope::Request,
                format!("codex executor: tokenizer init failed: {e}"),
            )
        })?;
        Ok(Counted::Local {
            input_tokens: u64::try_from(count).unwrap_or_default(),
        })
    }

    async fn compact(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<Ingress>, ExecError> {
        crate::codex_siwc::require_token(attempt.credential)?;
        let upload = encode(&request.validated, streams.request, false)?;
        let shaped = Shape::new(attempt, request, &upload)?;
        let ingress = Ingress::new(
            streams.response,
            attempt.identity,
            Operation::Compact,
            BodyMode::Buffered,
            SCANNER_MAX_LINE as u64,
        )
        .map_err(encoding)?;
        let (view, settings) = shaped.view();
        let body = shaped.compact(&view, &settings)?;
        let upstream = self
            .post_shaped(&view, &settings, &body, Call::Compact)
            .await
            .map_err(crate::codex::compact_error)?;
        let restored = events::compaction(upstream.body, body.optimized);
        Ok(Upstream {
            status: upstream.status,
            headers: upstream.headers,
            body: Box::pin(futures_util::TryStreamExt::map_err(
                restored,
                crate::codex::compact_error,
            )),
            ingress,
        })
    }
}

impl CodexExecutor {
    /// Posts a shaped body with the old path's headers and transport. A non-2xx answer is
    /// the attempt's failure, classified as Go does.
    async fn post_shaped(
        &self,
        view: &View<'_>,
        settings: &Settings,
        body: &Shaped<'_>,
        call: Call,
    ) -> Result<crate::proxy::Upstream, ExecError> {
        let generate = call != Call::Compact;
        let headers = request::http_headers(
            view,
            settings,
            body.headers,
            &body.text,
            &body.base_model,
            body.cache.as_deref(),
            generate,
        );
        let (path, error_body) = match call {
            Call::Stream => ("responses", ErrorBody::Strict),
            Call::NonStream => ("responses", ErrorBody::Lossy),
            _ => ("responses/compact", ErrorBody::Lossy),
        };
        let url = format!("{}/{path}", view.base_url);
        // ponytail: the contract carries no capture sink yet, so nothing is recorded for
        // request logging; D3 owns that observability.
        let wire = Wire::default();
        self.open(
            view,
            settings,
            url,
            headers,
            body.text.clone(),
            &body.scope,
            &wire,
            error_body,
        )
        .await
    }
}

/// A finished upstream body and what sending it and reading its answer need.
struct Shaped<'r> {
    text: String,
    headers: &'r http::HeaderMap,
    base_model: String,
    cache: Option<String>,
    /// The `collaboration` namespace was renamed upstream.
    optimized: bool,
    scope: Scope,
}

/// What the Codex body rules read from one attempt, with the encoded upload as the
/// client-side payload (lite marker, explicit instructions, prompt cache keys).
struct Shape<'r> {
    attempt: &'r Attempt<'r>,
    request: &'r Request<'r>,
    upload: &'r [u8],
    source: Format,
    requested: String,
}

impl<'r> Shape<'r> {
    fn new(attempt: &'r Attempt<'r>, request: &'r Request<'r>, upload: &'r [u8]) -> Result<Self, ExecError> {
        let requested = gjson::get(&String::from_utf8_lossy(upload), "model")
            .str()
            .trim()
            .to_owned();
        Ok(Self {
            attempt,
            request,
            upload,
            source: source(request.validated.view().document).map_err(encoding)?,
            requested,
        })
    }

    fn inbound(&self) -> Inbound<'_> {
        let client = &self.request.client;
        Inbound {
            model: self.attempt.model,
            requested_model: &self.requested,
            source: self.source,
            response: self.source,
            payload: self.upload,
            headers: client.headers,
            request_path: client.route,
            resolved: self.attempt.resolved_model,
            execution_session: client.execution_session,
            derived_session: client.derived_session,
            principal: &client.caller.principal,
        }
    }

    /// The credential's view with the request's explicit session, and its settings.
    fn view(&self) -> (View<'r>, Settings) {
        let session = ck_wire::session::cpa_session_id(self.request.client.session);
        let view = View::for_request(self.attempt.credential, self.attempt.cfg).with_session(session);
        let settings = Settings::scoped(self.attempt.cfg, &view);
        (view, settings)
    }

    /// The model suffix's thinking on `body`, already in `target`'s protocol.
    fn think(&self, body: &[u8], target: Format) -> Result<String, ExecError> {
        let caps = self
            .attempt
            .resolved_model
            .map(|r| ck_wire::thinking::ModelCaps::from(&r.info));
        let thought = ck_wire::thinking::apply_request_thinking(&ck_wire::thinking::RequestThinking {
            body,
            payload: body,
            original: body,
            model: self.attempt.model,
            from: target.as_str(),
            to: target.as_str(),
            provider: "codex",
            resolved: caps.as_ref().map(Some),
            has_request_transformer: false,
            updates_changed: false,
        })
        .map_err(|e| ExecError::local(e.status(), FailureScope::Request, e.message))?;
        String::from_utf8(thought)
            .map_err(|_| ExecError::local(400, FailureScope::Request, "request body is not valid UTF-8 JSON"))
    }

    /// A `/responses` body: Go's stream and non-stream rules, the replay, the prompt cache
    /// key, the input ids and the payload rules.
    fn generate(
        &self,
        view: &View<'_>,
        settings: &Settings,
        call: Call,
        replay: &Arc<crate::codex_replay::Cache>,
    ) -> Result<Shaped<'r>, ExecError> {
        let original = crate::codex_upload::require(self.upload.to_vec());
        let thought = self.think(&original, Format::Codex)?;
        let inbound = self.inbound();
        let shaped = codex_shape::shape_body(&inbound, view, settings, call, thought, original);
        let (body, scope) = if self.source == Format::Claude {
            let client = &self.request.client;
            let session = codex_shape::replay_session(shaped.body.as_bytes(), client.headers, client.execution_session);
            let owner = codex_shape::replay_owner(&client.caller.principal, &self.attempt.credential.id);
            let scope = Scope::of(self.attempt.model, shaped.body.as_bytes(), &session, &owner);
            crate::codex_replay::insert(replay, scope, shaped.body)
        } else {
            (shaped.body, Scope::default())
        };
        Ok(self.finish(&inbound, settings, &shaped.finalize, body, shaped.optimized, scope))
    }

    /// A `/responses/compact` body: the upload is already the Responses request compact sends.
    fn compact(&self, view: &View<'_>, settings: &Settings) -> Result<Shaped<'r>, ExecError> {
        let thought = self.think(self.upload, Format::OpenAIResponse)?;
        let inbound = self.inbound();
        let shaped = codex_shape::shape_body(&inbound, view, settings, Call::Compact, thought, self.upload.to_vec());
        Ok(self.finish(
            &inbound,
            settings,
            &shaped.finalize,
            shaped.body,
            shaped.optimized,
            Scope::default(),
        ))
    }

    /// The prompt cache key, the input ids and the payload rules.
    fn finish(
        &self,
        inbound: &Inbound<'_>,
        settings: &Settings,
        finalize: &request::Finalizer,
        body: String,
        optimized: bool,
        scope: Scope,
    ) -> Shaped<'r> {
        let (body, cache) = codex_shape::prompt_cache_for(inbound, body, None, false);
        let text = finalize.finish(inbound, settings, request::sanitize_input_ids(body));
        Shaped {
            text,
            headers: self.request.client.headers,
            base_model: request::base_model(self.attempt.model),
            cache,
            optimized,
            scope,
        }
    }

    /// The body a local count measures (`codex_executor_tokens.go`): the generation's
    /// upload with the Codex requirements and thinking, as a buffered call, and the payload
    /// rules.
    fn count(&self) -> Result<Vec<u8>, ExecError> {
        use crate::codex_json::{delete, set_bool_if_different, set_str_if_different};
        let original = crate::codex_upload::require(self.upload.to_vec());
        let mut body = self.think(&original, Format::Codex)?;
        body = set_str_if_different(body, "model", &request::base_model(self.attempt.model));
        for key in [
            "previous_response_id",
            "generate",
            "prompt_cache_retention",
            "safety_identifier",
            "stream_options",
        ] {
            body = delete(&body, key);
        }
        body = set_bool_if_different(body, "stream", false);
        let mut body = body.into_bytes();
        let inbound = self.inbound();
        if !inbound.native() {
            ck_wire::codex_client::normalize_codex_instructions(&mut body);
        }
        let requested = match inbound.requested_model.trim() {
            "" => self.attempt.model.trim(),
            requested => requested,
        };
        let rules = ck_wire::payload::Rules::of(self.attempt.cfg);
        Ok(ck_wire::payload::apply(
            &rules,
            &ck_wire::payload::Request {
                target_executor: "codex",
                model: &request::base_model(self.attempt.model),
                requested_model: requested,
                protocol: Format::Codex.as_str(),
                from_protocol: self.source.as_str(),
                root: "",
                original: &original,
                request_path: inbound.request_path,
                headers: Some(inbound.headers),
            },
            body,
        ))
    }
}

/// The upload request egress encodes for this destination, as one body.
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

/// The client's protocol, as `Format` names it.
fn source(doc: &Document) -> codec::Result<Format> {
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

/// Whether the client streams, which picks Go's stream or non-stream body rules: as its
/// `stream` member says. A Gemini client states it only in its route, which the request
/// handle does not carry, so it counts as streaming.
fn streaming(doc: &Document) -> codec::Result<bool> {
    match doc.member(doc.root(), "stream")? {
        Some(node) => Ok(matches!(doc.node(node)?.value, Value::Bool(true))),
        None => Ok(source(doc)? == Format::Gemini),
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
fn encoding(error: codec::Error) -> ExecError {
    match error {
        codec::Error::Storage(ck_stream::Error::StorageExhausted | ck_stream::Error::Io(_)) => {
            ExecError::local(503, FailureScope::Request, "stream.storage_exhausted")
        }
        codec::Error::Storage(ck_stream::Error::Gone) => ExecError::local(410, FailureScope::Request, "stream.gone"),
        error => ExecError::local(500, FailureScope::Request, format!("request encoding failed: {error}")),
    }
}
