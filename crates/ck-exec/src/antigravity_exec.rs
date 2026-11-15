//! The Antigravity executor over the internal stream (`docs/EXECUTION-CONTRACT.md`).
//!
//! Antigravity keeps CLIProxyAPI's behaviour (owner rulings 65-67, `docs/OPEN-QUESTIONS.md`):
//! its translators send signed Claude thinking back and cache the signatures, its reasoning
//! replay ledger is keyed by model and client session, and compact is answered from a local
//! summary. An attempt therefore re-fences the client view against the client's own protocol
//! and encodes it with that protocol's request egress, which writes the request the client
//! sent (JSON-equal). The Antigravity request, response and compaction steps
//! (`antigravity.rs`) run on that body. Their output is in the client's protocol, and that
//! protocol's response ingress decodes it into the canonical stream.

use std::sync::Arc;

use bytes::Bytes;
use ck_core::exec::{
    ClaudeCacheTelemetry, ExecError, ExecRequest, Operation as ExecOperation, ResponseBody, UpstreamStage,
    UsageObserver, UsageSink,
};
use ck_core::format::Format;
use ck_stream::{Store, Writer};
use ck_translate::anthropic::request::{AnthropicCapability, Budget};
use ck_translate::codec::chat::request::ChatCapability;
use ck_translate::codec::event::Identity;
use ck_translate::codec::{
    self, BodyMode, Capability, Operation, RequestEgress, RequestFence, RequestView, ResponseIngress,
};
use ck_translate::gemini::request::GeminiCapability;
use ck_translate::responses::request::ResponsesCapability;
use ck_translate::{anthropic, codec::chat, gemini as gemini_codec, responses};
use futures_util::StreamExt;

use crate::antigravity::AntigravityExecutor;
use crate::codex_exec::{copy, encoding, source, streaming};
use crate::gemini::{COMPACT_ALT, MAX_LINE};
use crate::types::{Attempt, Body, Counted, Execute, Request, Streams, Upstream};

/// The capability key of Antigravity credentials.
pub const DESTINATION: &str = "antigravity";

/// Whatever the client's own protocol accepts: the request reaches the Antigravity steps in
/// that protocol.
pub struct AntigravityCapability;

impl Capability for AntigravityCapability {
    fn destination(&self) -> &'static str {
        DESTINATION
    }

    fn validate(&self, view: RequestView<'_>) -> codec::Result<()> {
        match Protocol::of(view.document)? {
            Protocol::Chat => ChatCapability.validate(view),
            Protocol::Responses => ResponsesCapability.validate(view),
            Protocol::Anthropic => ANTHROPIC.validate(view),
            Protocol::Gemini => GeminiCapability.validate(view),
        }
    }
}

/// An Anthropic client's request as it sent it: no `max_tokens` default, and the official
/// thinking budget rules.
const ANTHROPIC: AnthropicCapability = AnthropicCapability {
    default_max_tokens: None,
    budget: Budget::Unknown,
};

/// The client's protocol, which the Antigravity steps read and write.
#[derive(Clone, Copy)]
enum Protocol {
    Chat,
    Responses,
    Anthropic,
    Gemini,
}

impl Protocol {
    fn of(doc: &codec::document::Document) -> codec::Result<Self> {
        Ok(match source(doc)? {
            Format::OpenAI => Self::Chat,
            Format::OpenAIResponse => Self::Responses,
            Format::Claude => Self::Anthropic,
            _ => Self::Gemini,
        })
    }

    fn format(self) -> Format {
        match self {
            Self::Chat => Format::OpenAI,
            Self::Responses => Format::OpenAIResponse,
            Self::Anthropic => Format::Claude,
            Self::Gemini => Format::Gemini,
        }
    }

    /// The client request in its own protocol, re-fenced for that protocol's egress.
    fn encode(self, view: RequestView<'_>, stream: bool, owner: &mut Writer) -> codec::Result<ck_stream::ScratchLease> {
        match self {
            Self::Chat => chat::request::Egress {
                destination: ChatCapability.destination(),
                upstream_stream: stream,
            }
            .encode(&RequestFence::default().end(view, &ChatCapability)?, owner),
            Self::Responses => responses::request::Egress {
                destination: ResponsesCapability.destination(),
                upstream_stream: stream,
            }
            .encode(&RequestFence::default().end(view, &ResponsesCapability)?, owner),
            Self::Anthropic => anthropic::request::Egress {
                destination: ANTHROPIC.destination(),
                upstream_stream: stream,
                default_max_tokens: None,
            }
            .encode(&RequestFence::default().end(view, &ANTHROPIC)?, owner),
            Self::Gemini => gemini_codec::request::Egress {
                destination: GeminiCapability.destination(),
                upstream_stream: stream,
            }
            .encode(&RequestFence::default().end(view, &GeminiCapability)?, owner),
        }
    }

    /// The decoder of an answer in this protocol.
    fn ingress(
        self,
        owner: &mut Writer,
        store: &Store,
        identity: Identity,
        operation: Operation,
        mode: BodyMode,
    ) -> codec::Result<AntigravityIngress> {
        let limit = MAX_LINE as u64;
        Ok(match self {
            Self::Chat => AntigravityIngress::Chat(Box::new(chat::response::Ingress::new(
                owner, store, identity, operation, mode, limit,
            )?)),
            Self::Responses => AntigravityIngress::Responses(Box::new(responses::response::Ingress::new(
                owner, identity, operation, mode, limit,
            )?)),
            Self::Anthropic => AntigravityIngress::Anthropic(Box::new(anthropic::response::Ingress::new(
                owner, identity, operation, mode, limit,
            )?)),
            Self::Gemini => AntigravityIngress::Gemini(Box::new(gemini_codec::response::Ingress::new(
                owner, identity, operation, mode, limit,
            )?)),
        })
    }
}

/// The response decoder of the client's protocol. Boxed: the producer task moves it once, and
/// the variants differ in size.
pub enum AntigravityIngress {
    Chat(Box<chat::response::Ingress>),
    Responses(Box<responses::response::Ingress>),
    Anthropic(Box<anthropic::response::Ingress>),
    Gemini(Box<gemini_codec::response::Ingress>),
}

macro_rules! each {
    ($self:ident, $ingress:ident => $call:expr) => {
        match $self {
            Self::Chat($ingress) => $call,
            Self::Responses($ingress) => $call,
            Self::Anthropic($ingress) => $call,
            Self::Gemini($ingress) => $call,
        }
    };
}

impl ResponseIngress for AntigravityIngress {
    fn flush_deadline(&self) -> Option<std::time::Instant> {
        each!(self, ingress => ingress.flush_deadline())
    }

    fn flush(&mut self, owner: &mut Writer) -> codec::Result<()> {
        each!(self, ingress => ingress.flush(owner))
    }

    fn advance(&mut self, owner: &mut Writer, bytes: &[u8]) -> codec::Result<()> {
        each!(self, ingress => ingress.advance(owner, bytes))
    }

    fn finish(&mut self, owner: &mut Writer) -> codec::Result<()> {
        each!(self, ingress => ingress.finish(owner))
    }

    fn fail(&mut self, owner: &mut Writer, status: u16, scope: &'static str) -> codec::Result<()> {
        each!(self, ingress => ingress.fail(owner, status, scope))
    }
}

impl Execute for AntigravityExecutor {
    type Capability = AntigravityCapability;
    type Ingress = AntigravityIngress;

    fn capability(&self, _attempt: &Attempt<'_>) -> AntigravityCapability {
        AntigravityCapability
    }

    async fn execute(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<AntigravityIngress>, ExecError> {
        self.run(attempt, request, streams).await
    }

    async fn count_tokens(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Counted<AntigravityIngress>, ExecError> {
        self.run(attempt, request, streams).await.map(Counted::Upstream)
    }

    /// Ruling 67: the local summary CLIProxyAPI answers compact with.
    async fn compact(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<AntigravityIngress>, ExecError> {
        self.run(attempt, request, streams).await
    }
}

impl AntigravityExecutor {
    /// Encodes the client's request in its own protocol, runs the Antigravity steps on it and
    /// decodes their answer with the client protocol's ingress.
    async fn run(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<AntigravityIngress>, ExecError> {
        let view = request.validated.view();
        let operation = view.operation;
        let protocol = Protocol::of(view.document).map_err(encoding)?;
        // Compact is answered unstreamed, as CLIProxyAPI answers its route.
        let stream = operation == Operation::Generate && streaming(view.document).map_err(encoding)?;
        let body = Bytes::from(copy(
            &protocol.encode(view, stream, streams.request).map_err(encoding)?,
        )?);
        let client = &request.client;
        let exec = ExecRequest {
            operation: match operation {
                Operation::CountTokens => ExecOperation::CountTokens,
                _ => ExecOperation::Generate,
            },
            source_format: protocol.format(),
            response_format: protocol.format(),
            requested_model: crate::gemini_exec::requested_model(view.document).map_err(encoding)?,
            model: attempt.model.to_owned(),
            original_body: body.clone(),
            body,
            stream,
            alt: (operation == Operation::Compact).then(|| COMPACT_ALT.to_owned()),
            session: client.session.map(str::to_owned),
            execution_session: client.execution_session.map(str::to_owned),
            derived_session: client.derived_session.map(str::to_owned),
            request_path: client.route.to_owned(),
            headers: client.headers.clone(),
            caller: client.caller.clone(),
            resolved_model: attempt.resolved_model.cloned(),
            usage: UsageSink::new(Arc::new(RequestSide(client.usage.clone())))
                .with_capture(client.usage.capture().clone()),
        };
        let answer = AntigravityExecutor::execute(self, attempt.credential, exec, attempt.cfg).await?;
        let (mode, body): (BodyMode, Body) = match answer.body {
            // The Chat handler, not the Antigravity steps, ends a clean Chat stream with
            // `[DONE]` (`ck-server/src/inference/openai.rs`, `ChatSse::end`; Go
            // `handleStreamResult`). A stream without a finish reason still fails at the ingress.
            ResponseBody::Stream(body) if matches!(protocol, Protocol::Chat) => (
                BodyMode::Streaming,
                body.chain(futures_util::stream::once(async {
                    Ok(Bytes::from_static(b"data: [DONE]\n\n"))
                }))
                .boxed(),
            ),
            ResponseBody::Stream(body) => (BodyMode::Streaming, body),
            ResponseBody::Buffered(body) => (
                BodyMode::Buffered,
                futures_util::stream::once(async { Ok(body) }).boxed(),
            ),
        };
        let ingress = protocol
            .ingress(streams.response, attempt.store, attempt.identity, operation, mode)
            .map_err(encoding)?;
        Ok(Upstream {
            status: answer.status,
            headers: answer.headers,
            body,
            ingress,
        })
    }
}

/// The request-side half of the attempt's usage sink (contract §8, G4). The Antigravity steps
/// also report the upstream's response usage; the canonical stream carries that instead, so it
/// stops here.
struct RequestSide(UsageSink);

impl UsageObserver for RequestSide {
    fn wants_claude_cache(&self) -> bool {
        self.0.wants_claude_cache()
    }
    fn claude_cache(&self, telemetry: ClaudeCacheTelemetry) {
        self.0.claude_cache(telemetry);
    }
    fn response_body(&self, _: Format, _: &[u8]) {}
    fn response_line(&self, _: Format, _: &[u8]) {}
    fn request(&self, format: Format, payload: &[u8]) {
        self.0.request(format, payload);
    }
    fn upstream_model(&self, model: &str) {
        self.0.upstream_model(model);
    }
    fn round_trip_started(&self) {
        self.0.round_trip_started();
    }
    fn first_byte(&self) {
        self.0.first_byte();
    }
    fn upstream_stage(&self, stage: UpstreamStage) {
        self.0.upstream_stage(stage);
    }
}
