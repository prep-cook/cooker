//! The Claude executor over the internal stream (`docs/EXECUTION-CONTRACT.md`): Messages
//! generation and count_tokens through the Anthropic codecs.
//!
//! The upload is the Anthropic request egress of the attempt's fence. The pipeline in
//! `claude.rs` prepares it as it prepares a Claude client's body: the cloak, identity, cache
//! policy, betas and headers are those of the old path. The response reaches
//! `anthropic::response::Ingress` chunk by chunk. Messages has no native compact, so the
//! capability refuses one (owner question 7).
//!
//! Contract G5: an Anthropic request the OAuth cloak or the Claude Code identity shapes is
//! prepared from the client's own body ([`crate::types::Client::original`]) exactly as the old
//! route prepares it, because detection reads the client's member order and `cch` signs the
//! bytes the cloak built from them. The fence still validates it first.

use std::borrow::Cow;
use std::sync::{Arc, Mutex};

use ck_core::config::Config;
use ck_core::exec::{CaptureEvent, ExecError, FailureScope, ResolvedModel, ResponseBody, UsageSink};
use ck_core::format::Format;
use ck_stream::{ScratchLease, Writer};
use ck_translate::anthropic::request::{AnthropicCapability, Budget, Egress};
use ck_translate::anthropic::response::Ingress;
use ck_translate::codec::document::{Document, Value};
use ck_translate::codec::{
    self, BodyMode, Operation, RequestEgress, RequestFence, RequestView, ResponseIngress, ValidatedRequest,
};
use futures_util::StreamExt;

use super::{
    ClaudeExecutor, Completion, Ctx, Delegation, Input, Prepared, RawResponse, base_model, cloak, detect,
    fast_request_error, header_value, rebuild_mid_system, replay, sanitize_for_upstream, stream, telemetry,
};
use crate::types::{Attempt, Body, Counted, Execute, Request, Streams, Upstream};

/// The capability key of the Anthropic Messages API.
pub const DESTINATION: &str = "anthropic";

/// The refusal of a compaction, which Messages has no native form of (owner question 7). It
/// names no upstream: members see it.
pub(super) const NO_COMPACT: &str = "This model does not support compaction; choose a model that does.";

impl Execute for ClaudeExecutor {
    type Capability = AnthropicCapability;
    type Ingress = ClaudeIngress;

    fn capability(&self, attempt: &Attempt<'_>) -> AnthropicCapability {
        capability(attempt.cfg, attempt.model, attempt.resolved_model)
    }

    async fn execute(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<ClaudeIngress>, ExecError> {
        // Boxed like the old path: the prepared request future is large.
        Box::pin(self.messages(attempt, request, streams)).await
    }

    async fn count_tokens(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Counted<ClaudeIngress>, ExecError> {
        Box::pin(self.count(attempt, request, streams)).await
    }

    async fn compact(
        &self,
        _attempt: &Attempt<'_>,
        _request: &Request<'_>,
        _streams: Streams<'_>,
    ) -> Result<Upstream<ClaudeIngress>, ExecError> {
        // The capability refuses compact at the fence, so no request reaches this.
        Err(ExecError::local(400, FailureScope::Model, NO_COMPACT))
    }
}

/// The `max_tokens` Messages gets for a request without one: the `model-catalog` override, else
/// the `max_completion_tokens` of a model a Claude credential registered. `None` makes the fence
/// refuse such a request, so the scheduler skips this candidate instead of sending a guessed
/// value (owner ruling Q-H).
fn default_max_tokens(cfg: &Config, model: &str) -> Option<u64> {
    let model = base_model(model);
    let model = model.trim();
    ck_core::config::model_catalog::default_max_tokens(cfg, model, || {
        ck_core::registry::model_providers(model)
            .iter()
            .any(|p| p.eq_ignore_ascii_case("claude"))
            .then(|| ck_core::registry::registered_model(model, Some("claude")))
            .flatten()
            .and_then(|info| {
                info.raw
                    .get("max_completion_tokens")
                    .and_then(serde_json::Value::as_u64)
            })
    })
}

/// The destination policy for `model`. The thinking budget follows the model's registry
/// `thinking` data.
pub(super) fn capability(cfg: &Config, model: &str, resolved: Option<&ResolvedModel>) -> AnthropicCapability {
    let default_max_tokens = default_max_tokens(cfg, model);
    let model = base_model(model);
    let model = model.trim();
    let thinking = match resolved {
        Some(resolved) => resolved.info.thinking.clone(),
        None => ck_core::registry::lookup_model(model, Some("claude")).and_then(|info| info.thinking),
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
    /// Whether `attempt` stays on the old route: every credential thinking replay serves.
    /// Replay stores a response only once it is known complete, which `Execute` cannot see:
    /// the producer validates the body after the executor returned. The selector answers no
    /// destination for it, so the server keeps the old `claude` arm (Q1).
    pub fn old_route_only(&self, attempt: &Attempt<'_>) -> bool {
        replay::eligible(&gate(&attempt_ctx(self, attempt, &scoped(attempt))))
    }

    async fn messages(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<ClaudeIngress>, ExecError> {
        let doc = request.validated.view().document;
        let facts = Facts::of(doc).map_err(encoding)?;
        let stream = streaming(doc, facts.client_format).map_err(encoding)?;
        let cfg = scoped(attempt);
        let mut ctx = context(self, attempt, request, &cfg);
        let (encoded, translated);
        let (original, body): (&[u8], &[u8]) = match shaped(&mut ctx, request, facts.client_format, false) {
            Some(original) => {
                translated = codex_tools(original, request.client.headers);
                (original, &translated)
            }
            None => {
                let default_max_tokens =
                    capability(attempt.cfg, attempt.model, attempt.resolved_model).default_max_tokens;
                encoded = encode(&request.validated, streams.request, stream, default_max_tokens)?;
                (&encoded, &encoded)
            }
        };
        let usage = request.client.usage;
        let input = facts.input(request, attempt, original, stream);
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
        let (body, pending) = relay(&ctx, &mut prepared, raw, usage, streams.response);
        Ok(Upstream {
            status,
            headers,
            body,
            ingress: ClaudeIngress {
                inner: ingress,
                pending,
            },
        })
    }

    /// The upstream count on Anthropic's origin with a key, else the local tokenizer over the
    /// request's generation projection.
    async fn count(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Counted<ClaudeIngress>, ExecError> {
        let cfg = scoped(attempt);
        let mut ctx = context(self, attempt, request, &cfg);
        let facts = Facts::of(request.validated.view().document).map_err(encoding)?;
        if let Some(original) = shaped(&mut ctx, request, facts.client_format, true) {
            let translated = codex_tools(original, request.client.headers);
            let input = facts.input(request, attempt, original, false);
            if ctx.api_key.trim().is_empty() || !ctx.first_party {
                let input_tokens = local_count(&ctx, &input, &translated)?;
                return Ok(Counted::Local { input_tokens });
            }
            let prepared = ctx.prepare_count(&input, &translated, &translated).await?;
            return self.count_upstream(&ctx, attempt, request, streams, prepared).await;
        }
        if ctx.api_key.trim().is_empty() || !ctx.first_party {
            let view = RequestView {
                operation: Operation::Generate,
                ..request.validated.view()
            };
            // A count carries no `max_tokens`; the projection only needs one to pass the fence.
            let mut capability = capability(attempt.cfg, attempt.model, attempt.resolved_model);
            capability.default_max_tokens = capability.default_max_tokens.or(Some(u64::MAX));
            let generate = RequestFence::default().end(view, &capability).map_err(encoding)?;
            let upload = encode(&generate, streams.request, false, capability.default_max_tokens)?;
            let input = facts.input(request, attempt, &upload, false);
            return Ok(Counted::Local {
                input_tokens: local_count(&ctx, &input, &upload)?,
            });
        }
        let upload = encode(&request.validated, streams.request, false, None)?;
        let input = facts.input(request, attempt, &upload, false);
        let prepared = ctx.prepare_count(&input, &upload, &upload).await?;
        self.count_upstream(&ctx, attempt, request, streams, prepared).await
    }

    /// Sends a prepared count to Anthropic's origin.
    async fn count_upstream(
        &self,
        ctx: &Ctx<'_>,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
        mut prepared: Prepared,
    ) -> Result<Counted<ClaudeIngress>, ExecError> {
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
            .send(ctx, &mut prepared, "/v1/messages/count_tokens", None, capture, None)
            .await?;
        Ok(Counted::Upstream(Upstream {
            status: raw.status,
            headers: raw.headers,
            body: recorded(body_of(raw.body), capture.clone()),
            ingress: ClaudeIngress {
                inner: ingress,
                pending: None,
            },
        }))
    }
}

/// The local count of what the request would send, `translated` being its Messages body: the
/// operator's thinking suffix, mid-system rebuild, the old sanitizing for a request prepared as
/// on the old path, and count rules.
fn local_count(ctx: &Ctx<'_>, input: &Input<'_>, translated: &[u8]) -> Result<u64, ExecError> {
    let mut body = super::utf8(translated).into_owned();
    if ctx.legacy_thinking(input) {
        body = ctx.apply_thinking(input, body)?;
    }
    if ctx.rebuild_mid_system() {
        body = rebuild_mid_system(&body);
    }
    if ctx.legacy_body() {
        body = sanitize_for_upstream(body, &ctx.base_model, ctx.is_compat);
    }
    let body = cloak::strip_prompt_cache_options(ctx.count_rules(input, body, translated));
    let counted = crate::tokens::count(body.as_bytes())?;
    let count = crate::rawjson::get(&super::utf8(&counted), "input_tokens").i64();
    Ok(u64::try_from(count).unwrap_or_default())
}

/// The prepared request's response body: whole native events after alias restore when the
/// upstream streams, with the completion its `message_stop` arms; a buffered cloaked body
/// collected and restored (owner ruling Q20); the bytes as read otherwise. Request logs record
/// every chunk.
fn relay(
    ctx: &Ctx<'_>,
    prepared: &mut Prepared,
    raw: RawResponse,
    usage: &UsageSink,
    response: &mut Writer,
) -> (Body, Option<Pending>) {
    let fast = ctx.first_party && prepared.fast;
    let capture = usage.capture().clone();
    let body = body_of(raw.body);
    let cloaked = ctx.wire_policy(false).0;
    if !prepared.stream && !cloaked {
        let body = recorded(body, capture).map(move |chunk| chunk.map_err(|e| fast_request_error(fast, e)));
        return (body.boxed(), None);
    }
    let pending = Pending {
        completion: ctx.completion(prepared),
        request_id: header_value(&raw.headers, "request-id"),
        message_id: Arc::default(),
    };
    let seen = pending.message_id.clone();
    let body = if prepared.stream {
        let done: stream::OnComplete = Box::new(move |message_id| *seen.lock().expect("message id") = Some(message_id));
        stream::relay(body, prepared.reverse.clone(), done, None, capture)
    } else {
        stream::collected(recorded(body, capture), response, prepared.reverse.clone(), seen)
    };
    let body = body.map(move |chunk| chunk.map_err(|e| fast_request_error(fast, e)));
    (body.boxed(), Some(pending))
}

/// The Anthropic response ingress of a Claude attempt. A streamed or buffered cloaked turn
/// records its thread aliases and session continuity only once this ingress published `done`:
/// the relay reads the message ID before the canonical ingress judges the response, which it
/// may still reject.
pub struct ClaudeIngress {
    inner: Ingress,
    pending: Option<Pending>,
}

/// A completion waiting for the canonical verdict; the relay sets the message ID at
/// `message_stop`, or once it collected a buffered body, before it passes the bytes on.
struct Pending {
    completion: Completion,
    request_id: String,
    message_id: Arc<Mutex<Option<String>>>,
}

impl ClaudeIngress {
    fn settle(&mut self, step: codec::Result<()>) -> codec::Result<()> {
        if self.inner.completed()
            && let Some(pending) = self.pending.take()
            && let Some(id) = pending.message_id.lock().expect("message id").take()
        {
            pending.completion.complete(&id, &pending.request_id);
        }
        step
    }
}

impl ResponseIngress for ClaudeIngress {
    fn flush_deadline(&self) -> Option<std::time::Instant> {
        self.inner.flush_deadline()
    }

    fn flush(&mut self, owner: &mut Writer) -> codec::Result<()> {
        let step = self.inner.flush(owner);
        self.settle(step)
    }

    fn advance(&mut self, owner: &mut Writer, bytes: &[u8]) -> codec::Result<()> {
        let step = self.inner.advance(owner, bytes);
        self.settle(step)
    }

    fn finish(&mut self, owner: &mut Writer) -> codec::Result<()> {
        let step = self.inner.finish(owner);
        self.settle(step)
    }

    fn fail(&mut self, owner: &mut Writer, status: u16, scope: &'static str) -> codec::Result<()> {
        self.inner.fail(owner, status, scope)
    }
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

/// The attempt's context for a routing decision, before any request exists.
fn attempt_ctx<'a>(exec: &ClaudeExecutor, attempt: &Attempt<'a>, cfg: &ck_core::config::Config) -> Ctx<'a> {
    let facts = (attempt.model, attempt.resolved_model, None);
    Ctx::new(exec, attempt.credential, facts, cfg, Delegation::default())
}

/// The old route's translation of a client's Messages body (`translate::request`), before
/// the cloak renames its tools: a Codex client's integer tool parameters
/// (`normalize_codex_tool_integer_types`). The preparation sets the model itself.
fn codex_tools<'b>(original: &'b [u8], headers: &http::HeaderMap) -> Cow<'b, [u8]> {
    if ck_wire::payload::is_codex_user_agent(headers) {
        Cow::Owned(ck_wire::payload::normalize_codex_tool_integer_types(original, headers))
    } else {
        Cow::Borrowed(original)
    }
}

/// Contract G5. A request the cloak or the Claude Code identity shapes is prepared as the old
/// route prepares it, so this clears `ctx.contract`; for an Anthropic client it returns the
/// client's own body, which detection, session identity and the cloak read. A foreign
/// client's shaped request is prepared from its encoded Messages body. `None` for any other
/// request, which goes upstream as the fence accepted it.
fn shaped<'r>(ctx: &mut Ctx<'_>, request: &Request<'r>, client: Format, count_tokens: bool) -> Option<&'r [u8]> {
    let (headers, original) = (request.client.headers, request.client.original);
    let anthropic = client == Format::Claude;
    // Confirmation needs the `x-app` header, so a request without a Claude Code header
    // signal skips the body scan.
    let confirmed = anthropic
        && detect::claude_code_signals(headers)
        && detect::detect(headers, &super::utf8(original), count_tokens, &ctx.settings).confirmed;
    if !confirmed && !ctx.may_cloak() {
        return None;
    }
    ctx.contract = false;
    anthropic.then_some(original)
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
