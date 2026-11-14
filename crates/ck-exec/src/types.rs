//! The execution contract over the internal stream (`docs/EXECUTION-CONTRACT.md`).
//!
//! Every provider module implements [`Execute`] once; the selector in `lib.rs` dispatches to
//! it statically by `Credential::provider`. An executor encodes only a [`ValidatedRequest`]
//! for its own destination, sends it, and hands back an [`Upstream`] whose body a
//! runtime-owned producer decodes into the attempt's stream (`pipeline`). Executors never
//! write client bytes, parse usage or decide commitment.

use std::future::Future;

use bytes::Bytes;
use ck_core::config::Config;
use ck_core::credential::Credential;
use ck_core::exec::{Caller, ExecError, ResolvedModel};
use ck_stream::Writer;
use ck_translate::codec::{Capability, ResponseIngress, ValidatedRequest};
use futures_util::stream::BoxStream;
use http::HeaderMap;

/// One routing attempt: the selected credential and upstream model under the config snapshot
/// taken when the request started.
pub struct Attempt<'a> {
    pub credential: &'a Credential,
    pub cfg: &'a Config,
    /// Upstream model after alias resolution, thinking suffix included.
    pub model: &'a str,
    /// Capabilities the dispatch loop bound to this attempt; `None` means the registry entry.
    pub resolved_model: Option<&'a ResolvedModel>,
}

/// Client facts an executor may need besides the request document. Holds client
/// credentials: never log it or forward it wholesale.
pub struct Client<'a> {
    pub caller: &'a Caller,
    pub headers: &'a HeaderMap,
    /// Matched route template (Go `request_path`), or the URI path when none matched.
    pub route: &'a str,
    /// Session key for affinity and provider session identity (`ck_wire::session`).
    pub session: Option<&'a str>,
    /// Long-lived execution session of the transport that owns one (Responses WebSocket).
    pub execution_session: Option<&'a str>,
    /// Derived session (`ctx:v1:<sha256>`), set only when no explicit session exists.
    pub derived_session: Option<&'a str>,
}

/// The request handle an attempt executes. `validated` is the request fence: request
/// egress encodes only through it, after `ValidatedRequest::for_destination` matched the
/// executor's capability. Its `view()` is also the request view client response egress
/// reads (for example the Responses echo of `tools`), which never authorizes encoding.
pub struct Request<'a> {
    pub validated: ValidatedRequest<'a>,
    pub client: Client<'a>,
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

/// A provider's execution. Implementations hold no per-request state; one value serves
/// every credential of its providers.
///
/// Errors: every `Err` is an attempt failure whose `ExecError::scope` drives failover and
/// cooldown; it happens before any byte of this attempt reaches the client. A local
/// operation (a tokenizer count) returns an [`Upstream`] whose body holds the provider-shaped
/// result, so one producer path decodes every operation.
pub trait Execute: Send + Sync {
    /// Destination capability; distinct policies use distinct `Capability::destination` keys.
    type Capability: Capability + Send + Sync;
    /// Response ingress of the upstream protocol; a provider speaking several is an enum.
    type Ingress: ResponseIngress + Send + 'static;

    /// The capability `credential` and `model` reach. Pure: no network, no preparation.
    fn capability(&self, attempt: &Attempt<'_>) -> Self::Capability;

    /// Encodes `request` with the destination's request egress into `writer` scratch,
    /// sends it and returns when the upstream answered. `writer` is the attempt's
    /// response stream. Covers `Operation::Generate`.
    fn execute(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        writer: &mut Writer,
    ) -> impl Future<Output = Result<Upstream<Self::Ingress>, ExecError>> + Send;

    /// [`Self::execute`] for `Operation::CountTokens`. The ingress publishes a
    /// `count_tokens` result, never generation usage.
    fn count_tokens(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        writer: &mut Writer,
    ) -> impl Future<Output = Result<Upstream<Self::Ingress>, ExecError>> + Send;

    /// [`Self::execute`] for `Operation::Compact`: the provider's native compact only; the
    /// capability refuses compact where none exists, never substituting a summary.
    fn compact(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        writer: &mut Writer,
    ) -> impl Future<Output = Result<Upstream<Self::Ingress>, ExecError>> + Send;
}
