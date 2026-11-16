//! The execution contract over the internal stream (`docs/EXECUTION-CONTRACT.md`).
//!
//! Every provider module implements [`Execute`] once; the selector in `lib.rs` dispatches to
//! it statically by `Credential::provider`. An executor encodes only a [`ValidatedRequest`]
//! fenced for its own destination, sends it, and hands back an [`Upstream`] whose body a
//! runtime-owned producer decodes into the attempt's response stream (`pipeline`). Executors
//! never write client bytes, parse usage or decide commitment.

use std::future::Future;

use bytes::Bytes;
use ck_core::config::Config;
use ck_core::credential::Credential;
use ck_core::exec::{Caller, ExecError, ResolvedModel, UsageSink};
use ck_stream::{Store, Writer};
use ck_translate::codec::event::Identity;
use ck_translate::codec::{Capability, Fenced, OwnedRequest, ResponseIngress, ValidatedRequest};
use futures_util::stream::BoxStream;
use http::HeaderMap;

/// One routing attempt: the selected credential and upstream model under the config snapshot
/// taken when the request started.
pub struct Attempt<'a> {
    pub credential: &'a Credential,
    pub cfg: &'a Config,
    /// Upstream model after alias resolution, thinking suffix included.
    pub model: &'a str,
    /// The client's request headers, for routing decisions only (contract G6); the server
    /// fills it on every attempt. Request execution reads `Client::headers`.
    pub headers: &'a HeaderMap,
    /// Capabilities the dispatch loop bound to this attempt; `None` means the registry entry.
    pub resolved_model: Option<&'a ResolvedModel>,
    /// The process stream store; response ingress constructors read their own stream from it.
    pub store: &'a Store,
    /// Codec identity of this attempt's response stream (direction `Response`, this attempt's
    /// number), created by the server. The response ingress and a local count publish with it.
    pub identity: Identity,
}

/// Client facts an executor may need besides the request document. Holds client
/// credentials: never log it or forward it wholesale.
pub struct Client<'a> {
    pub caller: &'a Caller,
    pub headers: &'a HeaderMap,
    /// Matched route template (Go `request_path`), or the URI path when none matched.
    pub route: &'a str,
    /// The route's `alt` query value (Go `opts.Alt`), for executors whose upstream URL carries
    /// it; `None` when the client sent none.
    pub alt: Option<&'a str>,
    /// Whether the route itself asks for a stream, as a Gemini `:streamGenerateContent` or
    /// `:generateContent` action does; `None` when only the body's `stream` member says so.
    pub streaming: Option<bool>,
    /// Session key for affinity and provider session identity (`ck_wire::session`).
    pub session: Option<&'a str>,
    /// Long-lived execution session of the transport that owns one (Responses WebSocket).
    pub execution_session: Option<&'a str>,
    /// Derived session (`ctx:v1:<sha256>`), set only when no explicit session exists.
    pub derived_session: Option<&'a str>,
    /// Request-side usage facts and upstream capture (contract §3, G4). An executor calls only
    /// `request`, `upstream_model`, `claude_cache`, `delegated_executor`, `round_trip_started`,
    /// `first_byte` and the capture calls; response usage comes from the canonical stream, never `response_*`.
    pub usage: &'a UsageSink,
    /// The client's request body as the server received it (contract G5). Only the Claude
    /// executor reads it, and only for an Anthropic request the OAuth cloak or the Claude
    /// Code identity shapes; every other executor encodes the fenced view.
    pub original: &'a [u8],
}

/// The request handle an attempt executes. [`Request::validated`] is this attempt's fence: the
/// client request view re-fenced against the attempt's own destination (`RequestFence::end`),
/// so request egress encodes only what that destination accepted. Its `view()` is also the
/// request view client response egress reads (the Responses echo of `tools`), which never
/// authorizes encoding.
pub struct Request<'a> {
    pub(crate) validated: Validated<'a>,
    pub client: Client<'a>,
    /// The client takes only the destination's own count: a local estimate is refused.
    upstream_count: bool,
}

// One per attempt on the stack; boxing `Fenced` would add an allocation per attempt.
#[allow(clippy::large_enum_variant)]
pub(crate) enum Validated<'a> {
    Borrowed(ValidatedRequest<'a>),
    Owned(Fenced),
    Released,
}

impl<'a> Request<'a> {
    /// A request whose fenced view the caller keeps.
    pub fn new(validated: ValidatedRequest<'a>, client: Client<'a>) -> Self {
        Self {
            validated: Validated::Borrowed(validated),
            client,
            upstream_count: false,
        }
    }

    /// A request that owns its fenced document, so the executor can [`Self::release`] it.
    pub fn owned(fenced: Fenced, client: Client<'a>) -> Self {
        Self {
            validated: Validated::Owned(fenced),
            client,
            upstream_count: false,
        }
    }

    /// Refuses a local token count with [`crate::pipeline::LOCAL_COUNT`]: the client, such as
    /// Responses `input_tokens`, presents the count as the destination's own.
    #[must_use]
    pub fn upstream_count(mut self) -> Self {
        self.upstream_count = true;
        self
    }

    pub(crate) fn upstream_count_only(&self) -> bool {
        self.upstream_count
    }

    /// This attempt's fence.
    ///
    /// # Panics
    ///
    /// After [`Self::release`]: an executor releases the document only once it reads it no more.
    pub fn validated(&self) -> ValidatedRequest<'_> {
        match &self.validated {
            Validated::Borrowed(validated) => *validated,
            Validated::Owned(fenced) => fenced.validated(),
            Validated::Released => panic!("the request document was released"),
        }
    }

    /// Frees an owned request document. An executor calls it once the upload is encoded and
    /// before it waits for the upstream, so a request waiting for its first byte holds no
    /// decoded copy of itself (the request stream keeps the records).
    pub fn release(&mut self) {
        self.validated.release();
    }

    /// The owned request document, unless the executor released it.
    pub fn into_request(self) -> Option<OwnedRequest> {
        match self.validated {
            Validated::Owned(fenced) => Some(fenced.into_request()),
            Validated::Borrowed(_) | Validated::Released => None,
        }
    }
}

impl Validated<'_> {
    /// [`Request::release`], callable while the request's client facts are borrowed.
    pub(crate) fn release(&mut self) {
        if matches!(self, Self::Owned(_)) {
            *self = Self::Released;
        }
    }
}

/// The two streams one attempt writes.
pub struct Streams<'w> {
    /// The exchange's request stream. The fenced document belongs to it, so request egress
    /// encodes the upstream body into its scratch; every attempt's upload is charged to it.
    pub request: &'w mut Writer,
    /// This attempt's fresh response stream, created with `Store::create_dependent(request)`
    /// so its records and metadata charge the exchange quota. The response ingress is
    /// created on it.
    pub response: &'w mut Writer,
}

/// Upstream response bytes in network order. An `Err` item is terminal. Chunks are passed
/// on as read and never collected into one body.
pub type Body = BoxStream<'static, Result<Bytes, ExecError>>;

/// An upstream that answered with a success status. The runtime producer feeds `body`
/// into `ingress`, which publishes canonical events to the attempt's writer.
pub struct Upstream<I> {
    pub status: u16,
    /// Upstream headers; the server decides which, if any, reach the client.
    pub headers: HeaderMap,
    pub body: Body,
    /// Response decoder for this upstream's protocol, bound to the attempt's writer.
    pub ingress: I,
}

/// A token count: from the upstream, or computed locally.
pub enum Counted<I> {
    /// The upstream counted; its response ingress publishes the canonical `count_tokens`.
    Upstream(Upstream<I>),
    /// Counted locally over the request's `Generate` projection, encoded by the destination's
    /// request egress; the producer publishes the canonical `count_tokens` result.
    Local { input_tokens: u64 },
}

/// A provider's execution. Implementations hold no per-request state; one value serves
/// every credential of its providers.
///
/// Errors: every `Err` is an attempt failure whose `ExecError::scope` drives failover and
/// cooldown; it happens before any byte of this attempt reaches the client.
pub trait Execute: Send + Sync {
    /// Destination capability; distinct policies use distinct `Capability::destination` keys.
    type Capability: Capability + Send + Sync;
    /// Response ingress of the upstream protocol; a provider speaking several is an enum.
    type Ingress: ResponseIngress + Send + 'static;

    /// The capability `credential` and `model` reach. Pure: no network, no preparation.
    fn capability(&self, attempt: &Attempt<'_>) -> Self::Capability;

    /// Encodes `request` with the destination's request egress into `streams.request`
    /// scratch, creates the response ingress on `streams.response`, sends the request and
    /// returns when the upstream answered. Covers `Operation::Generate`, and the media and
    /// alpha search operations for a capability that admits them (`Capability::admits`).
    fn execute(
        &self,
        attempt: &Attempt<'_>,
        request: &mut Request<'_>,
        streams: Streams<'_>,
    ) -> impl Future<Output = Result<Upstream<Self::Ingress>, ExecError>> + Send;

    /// [`Self::execute`] for `Operation::CountTokens`. A local count re-fences the request
    /// view with `operation: Generate` and counts what request egress encodes for it; its
    /// capability validates a count as that projection. A count is never generation usage.
    fn count_tokens(
        &self,
        attempt: &Attempt<'_>,
        request: &mut Request<'_>,
        streams: Streams<'_>,
    ) -> impl Future<Output = Result<Counted<Self::Ingress>, ExecError>> + Send;

    /// [`Self::execute`] for `Operation::Compact`: the provider's native compact only; the
    /// capability refuses compact where none exists, never substituting a summary.
    fn compact(
        &self,
        attempt: &Attempt<'_>,
        request: &mut Request<'_>,
        streams: Streams<'_>,
    ) -> impl Future<Output = Result<Upstream<Self::Ingress>, ExecError>> + Send;
}

/// `statusErr{code, msg}`: no retry hint, no headers; an empty message reads `status N`.
pub(crate) fn status_err(status: u16, message: impl Into<Bytes>) -> ExecError {
    let mut body: Bytes = message.into();
    if body.is_empty() {
        body = Bytes::from(format!("status {status}"));
    }
    ExecError {
        status,
        scope: crate::upstream::scope_for(status),
        body,
        headers: Box::default(),
        retry_after: None,
        direct: false,
        addon: None,
        upstream: false,
    }
}
