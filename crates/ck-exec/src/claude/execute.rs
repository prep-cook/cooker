//! The Claude executor over the internal stream (`docs/EXECUTION-CONTRACT.md`): Messages
//! generation and count_tokens through the Anthropic codecs.
//!
//! The upload is the Anthropic request egress of the attempt's fence. The pipeline in
//! `claude.rs` prepares it as it prepares a Claude client's body: the cloak, identity, cache
//! policy, betas and headers are those of the old path. The response reaches
//! `anthropic::response::Ingress` chunk by chunk. Messages has no native compact, so the
//! capability refuses one (owner question 7).
//!
//! Requests the OAuth cloak or the Claude Code identity would shape stay on the old route
//! ([`ClaudeExecutor::old_route_only`]): the cloak must not change until owner question 13
//! settles how `Execute` sees the client's own body.

use std::borrow::Cow;

use ck_core::exec::{CaptureEvent, ExecError, FailureScope, ResolvedModel, ResponseBody, UsageSink};
use ck_core::format::Format;
use ck_stream::{ScratchLease, Writer};
use ck_translate::anthropic::request::{AnthropicCapability, Budget, Egress};
use ck_translate::anthropic::response::Ingress;
use ck_translate::codec::document::{Document, Value};
use ck_translate::codec::{self, BodyMode, Operation, RequestEgress, RequestFence, RequestView, ValidatedRequest};
use futures_util::StreamExt;

use super::{
    ClaudeExecutor, Ctx, Delegation, Input, Prepared, RawResponse, base_model, cloak, fast_request_error, header_value,
    rebuild_mid_system, replay, stream, telemetry,
};
use crate::types::{Attempt, Body, Counted, Execute, Request, Streams, Upstream};

/// The capability key of the Anthropic Messages API.
pub const DESTINATION: &str = "anthropic";

/// The refusal of a compaction, which Messages has no native form of (owner question 7). It
/// names no upstream: members see it.
pub(super) const NO_COMPACT: &str = "This model does not support compaction; choose a model that does.";

impl Execute for ClaudeExecutor {
    type Capability = AnthropicCapability;
    type Ingress = Ingress;

    fn capability(&self, attempt: &Attempt<'_>) -> AnthropicCapability {
        capability(attempt.model, attempt.resolved_model)
    }

    async fn execute(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<Ingress>, ExecError> {
        // Boxed like the old path: the prepared request future is large.
        Box::pin(self.messages(attempt, request, streams)).await
    }

    async fn count_tokens(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Counted<Ingress>, ExecError> {
        Box::pin(self.count(attempt, request, streams)).await
    }

    async fn compact(
        &self,
        _attempt: &Attempt<'_>,
        _request: &Request<'_>,
        _streams: Streams<'_>,
    ) -> Result<Upstream<Ingress>, ExecError> {
        // The capability refuses compact at the fence, so no request reaches this.
        Err(ExecError::local(400, FailureScope::Model, NO_COMPACT))
    }
}

/// The destination policy for `model`. `default_max_tokens` is Go's `ensureModelMaxTokens`:
/// a model a Claude credential registered gets its completion limit, else 1024; any other
/// model has none. The thinking budget follows the model's registry `thinking` data.
pub(super) fn capability(model: &str, resolved: Option<&ResolvedModel>) -> AnthropicCapability {
    let model = base_model(model);
    let claude = |p: &String| p.eq_ignore_ascii_case("claude");
    let default_max_tokens = ck_core::registry::model_providers(model.trim())
        .iter()
        .any(claude)
        .then(|| {
            ck_core::registry::registered_model(model.trim(), Some("claude"))
                .and_then(|info| {
                    info.raw
                        .get("max_completion_tokens")
                        .and_then(serde_json::Value::as_u64)
                })
                .filter(|n| *n > 0)
                .unwrap_or(super::DEFAULT_MAX_TOKENS as u64)
        });
    let thinking = match resolved {
        Some(resolved) => resolved.info.thinking.clone(),
        None => ck_core::registry::lookup_model(&model, Some("claude")).and_then(|info| info.thinking),
    };
    let budget = match thinking {
        Some(t) if t.min > 0 || t.max > 0 => Budget::Manual {
            min: u64::try_from(t.min).unwrap_or(0),
        },
        Some(t) if t.dynamic_allowed || !t.levels.is_empty() => Budget::Adaptive,
        _ => Budget::Unknown,
    };
    AnthropicCapability {
        default_max_tokens,
        budget,
    }
}

impl ClaudeExecutor {
    /// Whether `attempt` stays on the old route: a request carrying any Claude Code client
    /// signal (contract G6), see [`Ctx::old_route_only`], and every credential thinking
    /// replay serves. Replay stores a response only once it is known
    /// complete, which `Execute` cannot see: the producer validates the body after the
    /// executor returned. The selector answers no destination for it, so the server keeps
    /// the old `claude` arm (Q1).
    pub fn old_route_only(&self, attempt: &Attempt<'_>) -> bool {
        if super::detect::claude_code_signals(attempt.headers) {
            return true;
        }
        let cfg = scoped(attempt);
        let facts = (attempt.model, attempt.resolved_model, None);
        let ctx = Ctx::new(self, attempt.credential, facts, &cfg, Delegation::default());
        ctx.old_route_only() || replay::eligible(&gate(&ctx))
    }

    async fn messages(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<Ingress>, ExecError> {
        let doc = request.validated.view().document;
        let facts = Facts::of(doc).map_err(encoding)?;
        let stream = streaming(doc, facts.client_format).map_err(encoding)?;
        let default_max_tokens = capability(attempt.model, attempt.resolved_model).default_max_tokens;
        let upload = encode(&request.validated, streams.request, stream, default_max_tokens)?;
        let cfg = scoped(attempt);
        let ctx = context(self, attempt, request, &cfg);
        let usage = request.client.usage;
        let input = facts.input(request, attempt, &upload, stream);
        let body: &[u8] = &upload;
        // As the old path: a foreign client's upstream always streams.
        let upstream_stream = stream || facts.client_format != Format::Claude;
        let mut prepared = ctx.prepare_messages(&input, body, body, upstream_stream).await?;
        let mode = if prepared.stream {
            BodyMode::Streaming
        } else {
            BodyMode::Buffered
        };
        let limit = stream::MAX_LINE as u64;
        let ingress =
            Ingress::new(streams.response, attempt.identity, Operation::Generate, mode, limit).map_err(encoding)?;
        report(usage, &ctx, &input, &prepared);
        let capture = usage.capture();
        let sent = self
            .send(&ctx, &mut prepared, "/v1/messages", Some(usage), capture, None)
            .await;
        if prepared.recovered {
            // The final attempt, not the rejected first body, owns final-wire usage.
            report(usage, &ctx, &input, &prepared);
        }
        let raw = sent?;
        let (status, headers) = (raw.status, raw.headers.clone());
        let body = relay(&ctx, &mut prepared, raw, usage);
        Ok(Upstream {
            status,
            headers,
            body,
            ingress,
        })
    }

    /// The upstream count on Anthropic's origin with a key, else the local tokenizer over the
    /// request's generation projection.
    async fn count(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Counted<Ingress>, ExecError> {
        let cfg = scoped(attempt);
        let ctx = context(self, attempt, request, &cfg);
        let facts = Facts::of(request.validated.view().document).map_err(encoding)?;
        if ctx.api_key.trim().is_empty() || !ctx.first_party {
            let view = RequestView {
                operation: Operation::Generate,
                ..request.validated.view()
            };
            // A count carries no `max_tokens`; the projection only needs one to pass the fence.
            let mut capability = capability(attempt.model, attempt.resolved_model);
            capability.default_max_tokens = capability.default_max_tokens.or(Some(u64::MAX));
            let generate = RequestFence::default().end(view, &capability).map_err(encoding)?;
            let upload = encode(&generate, streams.request, false, capability.default_max_tokens)?;
            let input = facts.input(request, attempt, &upload, false);
            return Ok(Counted::Local {
                input_tokens: local_count(&ctx, &input)?,
            });
        }
        let upload = encode(&request.validated, streams.request, false, None)?;
        let input = facts.input(request, attempt, &upload, false);
        let mut prepared = ctx.prepare_count(&input, &upload, &upload).await?;
        let limit = stream::MAX_LINE as u64;
        let ingress = Ingress::new(
            streams.response,
            attempt.identity,
            Operation::CountTokens,
            BodyMode::Buffered,
            limit,
        )
        .map_err(encoding)?;
        let capture = request.client.usage.capture();
        let raw = self
            .send(&ctx, &mut prepared, "/v1/messages/count_tokens", None, capture, None)
            .await?;
        Ok(Counted::Upstream(Upstream {
            status: raw.status,
            headers: raw.headers,
            body: recorded(body_of(raw.body), capture.clone()),
            ingress,
        }))
    }
}

/// The local count of what the contract path would send: the generation projection, the
/// operator's thinking suffix, mid-system rebuild and count rules.
fn local_count(ctx: &Ctx<'_>, input: &Input<'_>) -> Result<u64, ExecError> {
    let mut body = super::utf8(input.body).into_owned();
    if ctx.legacy_thinking(input) {
        body = ctx.apply_thinking(input, body)?;
    }
    if ctx.rebuild_mid_system() {
        body = rebuild_mid_system(&body);
    }
    let body = cloak::strip_prompt_cache_options(ctx.count_rules(input, body, input.body));
    let counted = crate::tokens::count(body.as_bytes())?;
    let count = crate::rawjson::get(&super::utf8(&counted), "input_tokens").i64();
    Ok(u64::try_from(count).unwrap_or_default())
}

/// The prepared request's response body: whole native events after alias restore when the
/// upstream streams, which also completes continuity and the thread; the bytes as read
/// otherwise. Request logs record every chunk.
fn relay(ctx: &Ctx<'_>, prepared: &mut Prepared, raw: RawResponse, usage: &UsageSink) -> Body {
    let fast = ctx.first_party && prepared.fast;
    let capture = usage.capture().clone();
    let body = body_of(raw.body);
    if !prepared.stream {
        return recorded(body, capture)
            .map(move |chunk| chunk.map_err(|e| fast_request_error(fast, e)))
            .boxed();
    }
    let completion = ctx.completion(prepared);
    let request_id = header_value(&raw.headers, "request-id");
    let done: stream::OnComplete = Box::new(move |message_id| completion.complete(&message_id, &request_id));
    stream::relay(body, prepared.reverse.clone(), done, None, capture)
        .map(move |chunk| chunk.map_err(|e| fast_request_error(fast, e)))
        .boxed()
}

/// Records each chunk of a body the executor passes on as read.
fn recorded(body: Body, capture: ck_core::exec::CaptureSink) -> Body {
    body.inspect(move |chunk| {
        if let Ok(chunk) = chunk {
            capture.record(CaptureEvent::ResponseChunk(chunk));
        }
    })
    .boxed()
}

fn body_of(body: ResponseBody) -> Body {
    match body {
        ResponseBody::Stream(body) => body,
        ResponseBody::Buffered(bytes) => futures_util::stream::once(async move { Ok(bytes) }).boxed(),
    }
}

/// The request-side facts the usage tracker reads (contract §8, G4): the body sent, for the
/// reasoning effort, and the content-free cache telemetry.
fn report(usage: &UsageSink, ctx: &Ctx<'_>, input: &Input<'_>, prepared: &Prepared) {
    if usage.enabled() {
        usage.request(Format::Claude, prepared.body.as_bytes());
    }
    if usage.wants_claude_cache()
        && let Some(telemetry) = telemetry::collect(ctx, input, prepared)
    {
        usage.claude_cache(telemetry);
    }
}

/// Go `executorForAuth`: an API-key credential runs with the config's API-key view.
fn scoped<'a>(attempt: &Attempt<'a>) -> Cow<'a, ck_core::config::Config> {
    if ck_core::registry::dynamic::auth_kind(attempt.credential) == Some("apikey") {
        attempt.cfg.for_api_key()
    } else {
        Cow::Borrowed(attempt.cfg)
    }
}

fn context<'a>(
    exec: &ClaudeExecutor,
    attempt: &Attempt<'a>,
    request: &Request<'_>,
    cfg: &ck_core::config::Config,
) -> Ctx<'a> {
    let facts = (attempt.model, attempt.resolved_model, request.client.execution_session);
    let mut ctx = Ctx::new(exec, attempt.credential, facts, cfg, Delegation::default());
    ctx.contract = true;
    ctx
}

fn gate<'a>(ctx: &'a Ctx<'_>) -> replay::Gate<'a> {
    replay::Gate {
        credential: ctx.credential,
        api_key: &ctx.api_key,
        base_url: ctx
            .credential
            .attributes
            .get("base_url")
            .map(String::as_str)
            .unwrap_or_default(),
        base_model: &ctx.base_model,
        is_compat: ctx.is_compat,
        oauth_token: ctx.oauth_token,
    }
}

/// What the pipeline reads from the fenced document besides the encoded body.
struct Facts {
    requested_model: String,
    client_format: Format,
}

impl Facts {
    fn of(doc: &Document) -> codec::Result<Self> {
        let root = doc.root();
        let mut requested_model = String::new();
        let mut chars = doc.chars(doc.required(root, "model")?)?;
        while let Some(c) = chars.next_scalar()? {
            requested_model.push(c);
        }
        let protocol = doc.required(doc.required(root, "x_ck")?, "original_protocol")?;
        let mut client_format = None;
        for (name, format) in [
            ("chat", Format::OpenAI),
            ("responses", Format::OpenAIResponse),
            ("anthropic", Format::Claude),
            ("gemini", Format::Gemini),
        ] {
            if doc.string_is(protocol, name)? {
                client_format = Some(format);
            }
        }
        Ok(Self {
            requested_model,
            client_format: client_format.ok_or(codec::Error::Schema)?,
        })
    }

    /// The encoded Messages body stands for both the client body and the translated one.
    fn input<'a>(
        &'a self,
        request: &'a Request<'_>,
        attempt: &'a Attempt<'_>,
        body: &'a [u8],
        stream: bool,
    ) -> Input<'a> {
        let client = &request.client;
        Input {
            headers: client.headers,
            original_body: body,
            body,
            source_format: Format::Claude,
            client_format: self.client_format,
            transformed: false,
            model: attempt.model,
            requested_model: &self.requested_model,
            request_path: client.route,
            principal: &client.caller.principal,
            session: client.session,
            derived_session: client.derived_session,
            execution_session: client.execution_session,
            stream,
        }
    }
}

/// Whether the upstream streams: as the client's `stream` member says. A Gemini client
/// states it only in its route, which the request handle does not carry, so it streams.
fn streaming(doc: &Document, client: Format) -> codec::Result<bool> {
    match doc.member(doc.root(), "stream")? {
        Some(node) => Ok(matches!(doc.node(node)?.value, Value::Bool(true))),
        None => Ok(client == Format::Gemini),
    }
}

/// Encodes `request` into the request stream's scratch and copies it out as the upload; the
/// pipeline edits the Messages body as text.
fn encode(
    request: &ValidatedRequest<'_>,
    owner: &mut Writer,
    upstream_stream: bool,
    default_max_tokens: Option<u64>,
) -> Result<Vec<u8>, ExecError> {
    let egress = Egress {
        destination: DESTINATION,
        upstream_stream,
        default_max_tokens,
    };
    copy(&egress.encode(request, owner).map_err(encoding)?)
}

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

/// A codec failure after the fence: storage per contract §9, anything else a gateway defect.
fn encoding(error: codec::Error) -> ExecError {
    match error {
        codec::Error::Storage(ck_stream::Error::StorageExhausted | ck_stream::Error::Io(_)) => {
            ExecError::local(503, FailureScope::Request, "stream.storage_exhausted")
        }
        codec::Error::Storage(ck_stream::Error::Gone) => ExecError::local(410, FailureScope::Request, "stream.gone"),
        error => ExecError::local(500, FailureScope::Request, format!("request encoding failed: {error}")),
    }
}
