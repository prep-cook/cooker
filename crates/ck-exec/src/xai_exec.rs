//! The xAI executor over the internal stream (`docs/EXECUTION-CONTRACT.md`).
//!
//! Generation always streams upstream: `/responses` with `stream: true` under the chat base
//! URL, through the Responses request egress; the upstream body passes xAI's line rules
//! (`xai_events`) into the Responses response ingress. A native compact goes buffered to
//! `/responses/compact` on the official API. A token count is local: xAI's API counts no
//! Responses request. The executor shapes its own encoded upload, never the client body
//! (`xai_shape`), with the payload rules last. Compaction triggers are sent to xAI as
//! ordinary generation input; only xAI performs compaction.

use std::borrow::Cow;

use bytes::Bytes;
use ck_core::config::Config;
use ck_core::exec::{ExecError, FailureScope};
use ck_core::format::Format;
use ck_stream::ScratchLease;
use ck_translate::codec::document::{Document, Value};
use ck_translate::codec::{self, BodyMode, Capability, Operation, RequestEgress, RequestFence, RequestView};
use ck_translate::responses::request::{Egress, ResponsesCapability};
use ck_translate::responses::response::Ingress;
use ck_wire::json::{self as gj, Kind};
use ck_wire::thinking::parse_suffix;

use crate::openai_compat_http::{self as wire, GoHeaders};
use crate::types::{Attempt, Counted, Execute, Request, Streams, Upstream};
use crate::xai::MAX_LINE;
use crate::xai::{self, XaiExecutor};
use crate::xai_events::{Compaction, Events, XaiIngress};
use crate::xai_request::Prepared;
use crate::xai_response::{self as response, text};
use crate::xai_shape::{self as shape, Inbound};

/// The capability key of xAI credentials.
pub const DESTINATION: &str = "xai";

/// Responses generation, its token count, and the Responses native compact.
pub struct XaiCapability;

impl Capability for XaiCapability {
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

/// Member `name` of `node` when `node` is an object.
pub(crate) fn object_member(doc: &Document, node: usize, name: &str) -> codec::Result<Option<usize>> {
    if !matches!(doc.node(node)?.value, Value::Object) {
        return Ok(None);
    }
    doc.member(node, name)
}

impl Execute for XaiExecutor {
    type Capability = XaiCapability;
    type Ingress = XaiIngress;

    fn capability(&self, _attempt: &Attempt<'_>) -> XaiCapability {
        XaiCapability
    }

    async fn execute(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<XaiIngress>, ExecError> {
        let upload = encode(&request.validated, true, streams.request)?;
        let call = Call::new(attempt, request)?;
        let mut prepared = call.shape(&upload, &self.replay, true, Format::Codex)?;
        call.report(&prepared.body);
        let body = call.rules(Format::Codex, &upload, std::mem::take(&mut prepared.body));
        let headers = call.headers(&prepared, true, true);
        let url = xai::endpoint(&xai::chat_base_url(attempt.credential), "/responses");
        let ingress = Ingress::new(
            streams.response,
            attempt.identity,
            Operation::Generate,
            BodyMode::Streaming,
            MAX_LINE as u64,
        )
        .map_err(encoding)?;
        let events = Events::new(ingress, prepared, self.replay.clone());
        let upstream = self.send(&call, url, headers, body).await?;
        Ok(Upstream {
            status: upstream.status,
            headers: upstream.headers,
            body: upstream.body,
            ingress: XaiIngress::Generate(Box::new(events)),
        })
    }

    /// `countXAIInputTokens` over the body generation would send.
    async fn count_tokens(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Counted<XaiIngress>, ExecError> {
        let view = RequestView {
            operation: Operation::Generate,
            ..request.validated.view()
        };
        let generate = RequestFence::default().end(view, &XaiCapability).map_err(encoding)?;
        let upload = encode(&generate, false, streams.request)?;
        let call = Call::new(attempt, request)?;
        let prepared = call.shape(&upload, &self.replay, false, Format::Codex)?;
        let body = call.rules(Format::Codex, &upload, prepared.body);
        let count = xai::count_input_tokens(&body).map_err(|e| {
            ExecError::local(
                500,
                FailureScope::Request,
                format!("xai executor: tokenizer init failed: {e}"),
            )
        })?;
        Ok(Counted::Local {
            input_tokens: u64::try_from(count).unwrap_or_default(),
        })
    }

    /// `executeCompactRequest` on the official API; a success clears the session's replay.
    async fn compact(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<XaiIngress>, ExecError> {
        let upload = encode(&request.validated, false, streams.request)?;
        let previous = text(&gj::get(&upload, "previous_response_id"));
        let call = Call::new(attempt, request)?;
        let mut prepared = call.shape(&upload, &self.replay, false, Format::OpenAIResponse)?;
        let body = shape::compact_body(std::mem::take(&mut prepared.body), &previous);
        call.report(&body);
        let body = call.rules(Format::OpenAIResponse, &upload, body);
        let headers = call.headers(&prepared, false, false);
        let url = xai::endpoint(&xai::compact_base_url(attempt.credential), "/responses/compact");
        let ingress = Ingress::new(
            streams.response,
            attempt.identity,
            Operation::Compact,
            BodyMode::Buffered,
            MAX_LINE as u64,
        )
        .map_err(encoding)?;
        let scope = prepared.replay_scope;
        let upstream = self.send(&call, url, headers, body).await?;
        Ok(Upstream {
            status: upstream.status,
            headers: upstream.headers,
            body: upstream.body,
            ingress: XaiIngress::Compact(Box::new(Compaction {
                ingress,
                replay: scope.valid().then(|| (self.replay.clone(), scope)),
            })),
        })
    }
}

impl XaiExecutor {
    /// Posts `body` to `url` through the credential's client; a non-2xx answer is the
    /// attempt's failure with `xaiStatusErr`'s status and retry hint and the upstream's body
    /// and headers as received. The request, the answer's metadata and every body chunk go
    /// to the capture, and the round trip and first byte to the usage tracker.
    async fn send(
        &self,
        call: &Call<'_>,
        url: String,
        headers: GoHeaders,
        body: Vec<u8>,
    ) -> Result<wire::Upstream, ExecError> {
        let url = self.url(url);
        // Refused before anything is sent, so the token never reaches a host the url crate
        // reads differently from Go.
        crate::xai_url::new_request(&url)?;
        if let Some(error) = crate::xai_url::transport_refusal(&url) {
            return Err(error);
        }
        let client = self.clients.for_credential(call.attempt.credential, &call.cfg);
        let usage = call.request.client.usage;
        let (credential, pairs) = (call.attempt.credential, headers.pairs());
        let capture = wire::Capture::on(
            usage.capture().clone(),
            credential,
            xai::PROVIDER,
            &url,
            "POST",
            pairs,
            &body,
        );
        usage.round_trip_started();
        let upstream = wire::send(&client, &url, headers, Bytes::from(body)).await;
        capture.sent(&upstream);
        let mut upstream = xai::track_first_byte(upstream?, usage);
        if (200..300).contains(&upstream.status) {
            upstream.body = capture.stream(upstream.body);
            return Ok(upstream);
        }
        let (status, headers, body) = wire::read_error_body(upstream).await;
        capture.chunk(&body);
        let mut error = response::status_error(status, &body);
        error.body = body;
        error.headers = Box::new(headers);
        Err(error)
    }
}

/// One attempt's facts the upload shaping and the send read.
struct Call<'r> {
    attempt: &'r Attempt<'r>,
    request: &'r Request<'r>,
    /// The attempt's config, as Go binds it for the credential (`cfg.ForAPIKey()`).
    cfg: Cow<'r, Config>,
    /// The client's protocol.
    source: Format,
    /// The model the client asked for, as payload rules match it.
    requested: String,
}

impl<'r> Call<'r> {
    fn new(attempt: &'r Attempt<'r>, request: &'r Request<'r>) -> Result<Self, ExecError> {
        let document = request.validated.view().document;
        Ok(Self {
            attempt,
            request,
            cfg: xai::scoped(attempt.credential, attempt.cfg),
            source: source(document).map_err(encoding)?,
            requested: requested_model(document).map_err(encoding)?,
        })
    }

    /// The upload shaped for xAI. The upload is already Responses, so it is also the payload,
    /// the original and the apply_patch declarations.
    fn shape(
        &self,
        upload: &[u8],
        store: &crate::xai_replay::Store,
        stream: bool,
        to: Format,
    ) -> Result<Prepared, ExecError> {
        let client = &self.request.client;
        let inbound = Inbound {
            source: self.source,
            payload_format: Format::OpenAIResponse,
            model: self.attempt.model,
            payload: upload,
            original: upload,
            headers: client.headers,
            resolved: self.attempt.resolved_model,
            execution_session: client.execution_session,
            derived_session: client.derived_session,
            principal: &client.caller.principal,
            credential: Some(&self.attempt.credential.id),
            downstream_websocket: false,
        };
        shape::shape(&inbound, &self.cfg, store, stream, to, required_members(upload), upload)
    }

    /// The request-side usage facts (contract G4): the shaped body before the payload rules,
    /// for the reasoning effort, as the old path reports it, and the model xAI should serve.
    fn report(&self, body: &[u8]) {
        let usage = self.request.client.usage;
        usage.request_for(xai::PROVIDER, body);
        usage.upstream_model(&parse_suffix(self.attempt.model).model_name);
    }

    /// The configured payload rules, the last change to the body (Go `finalizePayload`).
    fn rules(&self, protocol: Format, original: &[u8], body: Vec<u8>) -> Vec<u8> {
        let rules = ck_wire::payload::Rules::of(&self.cfg);
        ck_wire::payload::apply(
            &rules,
            &ck_wire::payload::Request {
                target_executor: xai::PROVIDER,
                model: &parse_suffix(self.attempt.model).model_name,
                requested_model: &self.requested,
                protocol: protocol.as_str(),
                from_protocol: self.source.as_str(),
                root: "",
                original,
                request_path: self.request.client.route,
                headers: Some(self.request.client.headers),
            },
            body,
        )
    }

    /// The old path's headers: `applyXAIChatHeaders` for generation, `applyXAIHeaders` for a
    /// compact.
    fn headers(&self, prepared: &Prepared, stream: bool, chat: bool) -> GoHeaders {
        let (token, _) = xai::creds(self.attempt.credential);
        let client = &self.request.client;
        xai::request_headers(
            self.attempt.credential,
            (&token, stream, &prepared.session_id),
            (client.headers, client.session),
            chat,
        )
    }
}

/// The members Go's xAI path sends whatever the client asked, from its Codex request shape:
/// nothing stored upstream, the encrypted reasoning that replay carries to the next turn,
/// and parallel tool calls (dropped again without tools).
fn required_members(upload: &[u8]) -> Vec<u8> {
    const SOURCES: &[u8] = b"web_search_call.action.sources";
    let mut body = upload.to_vec();
    for (path, value) in [("store", false), ("parallel_tool_calls", true)] {
        if gj::get(&body, path).kind != if value { Kind::True } else { Kind::False } {
            gj::set_bool(&mut body, path, value);
        }
    }
    let include = gj::get(&body, "include");
    let sources = include.is_array()
        && include
            .array()
            .iter()
            .any(|v| v.kind == Kind::String && *v.bytes() == *SOURCES);
    let required = if sources {
        r#"["reasoning.encrypted_content","web_search_call.action.sources"]"#
    } else {
        r#"["reasoning.encrypted_content"]"#
    };
    if include.raw() != required.as_bytes() {
        gj::set_raw(&mut body, "include", required);
    }
    body
}

/// Encodes the fenced request with the Responses request egress; the HTTP client takes the
/// encoded upload as one body.
fn encode(
    request: &codec::ValidatedRequest<'_>,
    stream: bool,
    owner: &mut ck_stream::Writer,
) -> Result<Vec<u8>, ExecError> {
    let egress = Egress {
        destination: DESTINATION,
        upstream_stream: stream,
    };
    copy(&egress.encode(request, owner).map_err(encoding)?)
}

/// The client's protocol.
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

/// The request's `model`: the client's model, or a Gemini client's URL model.
fn requested_model(doc: &Document) -> codec::Result<String> {
    let mut chars = doc.chars(doc.required(doc.root(), "model")?)?;
    let mut model = String::new();
    while let Some(c) = chars.next_scalar()? {
        model.push(c);
    }
    Ok(model.trim().to_owned())
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
