//! The Claude executor over the internal stream (`docs/EXECUTION-CONTRACT.md`): Messages
//! generation, count_tokens and compaction through the Anthropic codecs.
//!
//! The upload is the Anthropic request egress of the attempt's fence. The pipeline in
//! `claude.rs` prepares it as it prepares a Claude client's body: the cloak, identity, cache
//! policy, betas and headers are those of the old path. The response reaches
//! `anthropic::response::Ingress` chunk by chunk. A compact is Anthropic's native compaction
//! (owner ruling, Q7 reversal): a buffered Messages call with `"compaction":{"type":"summarize"}`
//! and the compaction beta, whose one signed block later requests replay unchanged.
//!
//! Contract G5: an Anthropic request the OAuth cloak or the Claude Code identity shapes is
//! prepared from the client's own body ([`crate::types::Client::original`]) exactly as the old
//! route prepares it, because detection reads the client's member order and `cch` signs the
//! bytes the cloak built from them. The fence still validates it first.

use std::borrow::Cow;
use std::sync::{Arc, Mutex};

use ck_core::config::Config;
use ck_core::exec::{CaptureEvent, ExecError, FailureScope, ResponseBody, UsageSink};
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
use crate::types::{Attempt, Body, Client, Counted, Execute, Request, Streams, Upstream};

/// The capability key of the Anthropic Messages API.
pub const DESTINATION: &str = "anthropic";

/// The old route's refusal of a compaction, which only the contract path sends natively. It
/// names no upstream: members see it.
pub(super) const NO_COMPACT: &str = "This model does not support compaction; choose a model that does.";

impl Execute for ClaudeExecutor {
    type Capability = AnthropicCapability;
    type Ingress = ClaudeIngress;

    fn capability(&self, attempt: &Attempt<'_>) -> AnthropicCapability {
        capability(attempt)
    }

    async fn execute(
        &self,
        attempt: &Attempt<'_>,
        request: &mut Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<ClaudeIngress>, ExecError> {
        // Boxed like the old path: the prepared request future is large.
        Box::pin(self.messages(attempt, request, streams)).await
    }

    async fn count_tokens(
        &self,
        attempt: &Attempt<'_>,
        request: &mut Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Counted<ClaudeIngress>, ExecError> {
        Box::pin(self.count(attempt, request, streams)).await
    }

    async fn compact(
        &self,
        attempt: &Attempt<'_>,
        request: &mut Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<ClaudeIngress>, ExecError> {
        Box::pin(self.messages(attempt, request, streams)).await
    }
}

/// The `max_tokens` Messages gets for a request without one: the `model-catalog` override, else
/// the `max_completion_tokens` of a model a Claude credential registered, else the value a
/// `requests.payload.default` rule sets for this model. `None` makes the fence refuse such a
/// request, so the scheduler skips this candidate instead of sending a guessed value (owner
/// ruling Q-H).
fn default_max_tokens(cfg: &Config, model: &str, headers: &http::HeaderMap) -> Option<u64> {
    max_tokens_for(&ck_core::config::model_catalog::catalog(cfg), model)
        .or_else(|| payload_max_tokens(&ck_wire::payload::Rules::of(cfg), model, headers))
}

/// The configured or registered limit alone: the old path's payload rules run on the real body
/// and fill their own default.
pub(super) fn max_tokens_for(catalog: &ck_core::config::model_catalog::ModelCatalog, model: &str) -> Option<u64> {
    let model = base_model(model);
    let model = model.trim();
    catalog.resolve(model, || {
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

/// The `max_tokens` the payload default rules set on an empty Claude body for `model`: the
/// fence's precheck, which cannot see the encoded body. A rule gated on body conditions may
/// pass here and not match the request; `Ctx::prepare_messages` refuses that request before
/// sending. A rule gated on the source protocol or the requested alias does not pass here.
pub(super) fn payload_max_tokens(
    rules: &ck_wire::payload::Rules,
    model: &str,
    headers: &http::HeaderMap,
) -> Option<u64> {
    if !rules.has_defaults() {
        return None;
    }
    let model = base_model(model);
    let request = ck_wire::payload::Request {
        target_executor: "claude",
        model: model.trim(),
        protocol: Format::Claude.as_str(),
        headers: Some(headers),
        ..Default::default()
    };
    let body = ck_wire::payload::apply(rules, &request, b"{}".to_vec());
    serde_json::from_slice::<serde_json::Value>(&body).ok()?["max_tokens"]
        .as_u64()
        .filter(|n| *n > 0)
}

/// The destination policy for the attempt's model. The thinking budget follows the model's
/// registry `thinking` data.
pub(super) fn capability(attempt: &Attempt<'_>) -> AnthropicCapability {
    let default_max_tokens = default_max_tokens(attempt.cfg, attempt.model, attempt.headers);
    let budget = match thinking(attempt) {
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

/// The attempt model's thinking support: the dispatch loop's binding, else the registry.
fn thinking(attempt: &Attempt<'_>) -> Option<ck_core::registry::ThinkingSupport> {
    match attempt.resolved_model {
        Some(resolved) => resolved.info.thinking.clone(),
        None => {
            let model = base_model(attempt.model);
            ck_core::registry::lookup_model(model.trim(), Some("claude")).and_then(|info| info.thinking)
        }
    }
}

/// Moves `output_config.effort` to the nearest level the model supports (owner ruling
/// 2026-11-15, Settled 59): the fence passes any client effort, and Messages refuses one
/// outside the model's levels.
fn snap_effort(attempt: &Attempt<'_>, mut body: Vec<u8>) -> Vec<u8> {
    let effort = ck_wire::json::get(&body, "output_config.effort");
    if effort.kind != ck_wire::json::Kind::String {
        return body;
    }
    let Some(levels) = thinking(attempt).map(|t| t.levels) else {
        return body;
    };
    let effort = effort.str().into_owned();
    let snapped = ck_wire::thinking::snap_level(&effort, &levels);
    if snapped != effort {
        ck_wire::json::set_str(&mut body, "output_config.effort", snapped);
    }
    body
}

impl ClaudeExecutor {
    /// Whether the fenced `view` runs on the contract path for `attempt`; the selector's fence,
    /// re-fence and start refuse it with `Destination` otherwise, and the server keeps the old
    /// `claude` arm. A request on a credential thinking replay serves stays on the old arm:
    /// replay stores a response only once it is known complete, which `Execute` cannot see,
    /// because the producer validates the body after the executor returned (Q1). A compact and
    /// a request carrying a compaction block run here anyway, since the old arm has no native
    /// compaction and drops the block; thinking replay does not record their turns.
    pub fn contract_path(&self, attempt: &Attempt<'_>, view: RequestView<'_>) -> bool {
        ck_translate::anthropic::compaction::requested(view).unwrap_or(false)
            || !replay::eligible(&gate(&attempt_ctx(self, attempt, &scoped(attempt))))
    }

    async fn messages(
        &self,
        attempt: &Attempt<'_>,
        request: &mut Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<ClaudeIngress>, ExecError> {
        let (doc, operation) = (
            request.validated().view().document,
            request.validated().view().operation,
        );
        let facts = Facts::of(doc).map_err(encoding)?;
        // The fence refuses a streamed compaction.
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
                // A limit only a payload default rule sets is left to the rules, which see this body.
                let default_max_tokens =
                    max_tokens_for(&ck_core::config::model_catalog::catalog(attempt.cfg), attempt.model);
                encoded = encode(
                    attempt,
                    &request.validated(),
                    streams.request,
                    stream,
                    default_max_tokens,
                )?;
                (&encoded, &encoded)
            }
        };
        let usage = request.client.usage;
        let compaction = ck_translate::anthropic::compaction::requested(request.validated().view()).unwrap_or(false);
        // Everything below reads the encoded or client body: the request document is freed
        // before the preparation copies and the wait for the upstream.
        request.validated.release();
        let input = facts.client_input(&request.client, attempt, original, stream, compaction);
        // As the old path: a foreign client's generation always streams upstream.
        let upstream_stream = stream || (operation == Operation::Generate && facts.client_format != Format::Claude);
        let mut prepared = ctx.prepare_messages(&input, body, body, upstream_stream).await?;
        let mode = if prepared.stream {
            BodyMode::Streaming
        } else {
            BodyMode::Buffered
        };
        let limit = stream::MAX_LINE as u64;
        let ingress = Ingress::new(streams.response, attempt.identity, operation, mode, limit).map_err(encoding)?;
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
        request: &mut Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Counted<ClaudeIngress>, ExecError> {
        let cfg = scoped(attempt);
        let mut ctx = context(self, attempt, request, &cfg);
        let facts = Facts::of(request.validated().view().document).map_err(encoding)?;
        let local = ctx.api_key.trim().is_empty() || !ctx.first_party;
        let (encoded, translated);
        let (original, body): (&[u8], &[u8]) = match shaped(&mut ctx, request, facts.client_format, true) {
            Some(original) => {
                translated = codex_tools(original, request.client.headers);
                (original, &translated)
            }
            None if local => {
                let view = RequestView {
                    operation: Operation::Generate,
                    ..request.validated().view()
                };
                // A count carries no `max_tokens`; the projection only needs one to pass the fence.
                let mut capability = capability(attempt);
                capability.default_max_tokens = capability.default_max_tokens.or(Some(u64::MAX));
                let generate = RequestFence::default().end(view, &capability).map_err(encoding)?;
                encoded = encode(
                    attempt,
                    &generate,
                    streams.request,
                    false,
                    capability.default_max_tokens,
                )?;
                (&encoded, &encoded)
            }
            None => {
                encoded = encode(attempt, &request.validated(), streams.request, false, None)?;
                (&encoded, &encoded)
            }
        };
        let compaction = ck_translate::anthropic::compaction::requested(request.validated().view()).unwrap_or(false);
        // As for a generation: the count reads only the encoded or client body from here on.
        request.validated.release();
        let input = facts.client_input(&request.client, attempt, original, false, compaction);
        if local {
            return Ok(Counted::Local {
                input_tokens: local_count(&ctx, &input, body)?,
            });
        }
        let prepared = ctx.prepare_count(&input, body, body).await?;
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

    /// The request input the preparation reads: `body` stands for both the client body and the
    /// translated one. It holds only client facts, so the caller may release the document first.
    fn client_input<'a>(
        &'a self,
        client: &'a Client<'_>,
        attempt: &'a Attempt<'_>,
        body: &'a [u8],
        stream: bool,
        compaction: bool,
    ) -> Input<'a> {
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
            compaction,
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

/// Encodes `request` into the request stream's scratch and copies it out as the upload with
/// its effort snapped; the pipeline edits the Messages body as text.
fn encode(
    attempt: &Attempt<'_>,
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
    Ok(snap_effort(
        attempt,
        copy(&egress.encode(request, owner).map_err(encoding)?)?,
    ))
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
