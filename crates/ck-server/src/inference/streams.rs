//! Client exchanges and their retained response streams (`docs/EXECUTION-CONTRACT.md`,
//! `docs/INTERNAL-STREAM.md` §6.6–6.7).
//!
//! An [`Exchange`] is one client request. Its request stream holds the client request
//! records and every attempt's encoded upload. Each routing attempt writes its own response
//! stream, which holds the request stream. The attempt whose egress commits becomes the
//! exchange's [`StreamHandle`], registered under a fresh [`StreamId`]. Resume
//! (`GET /v1/streams`) authorizes the caller against the handle's owner before any cursor
//! lookup and re-encodes with the handle's output.

use ck_core::exec::Caller;
use ck_stream::Writer;

use crate::http::egress::Output;

/// The resume identity: 128 random bits, sent only in the `X-Ck-Stream-Id` header. It has
/// no `Debug` or `Display`, so it cannot reach a log through formatting.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct StreamId(pub [u8; 16]);

/// The identity that created an exchange. A resume must match both parts.
#[derive(Clone, PartialEq, Eq)]
pub struct Owner {
    pub caller: Caller,
    /// The call scope (`sess_...`) of an ephemeral client key; `None` for any other key.
    pub scope: Option<String>,
}

/// One client request from ingress to commitment. Consumed by commitment into a
/// [`StreamHandle`].
pub struct Exchange {
    pub owner: Owner,
    pub output: Output,
    /// The request stream's producer. Request ingress publishes the client request into it,
    /// and each attempt's request egress encodes its upload into its scratch. At commitment
    /// the exchange finishes it with `Terminal::Done` and keeps an allocation handle.
    pub request: Writer,
}

/// A committed response retained for delivery and resume until the response stream expires
/// or is evicted. A client disconnect releases only its reader.
pub struct StreamHandle {
    pub id: StreamId,
    pub owner: Owner,
    pub output: Output,
    /// Allocation handle on the committed response stream. Egress decodes and encodes with
    /// it; readers attach through `Store::reader(response.id(), ..)`; `finish(Cancelled)` is
    /// the explicit client cancel.
    pub response: Writer,
    /// Allocation handle on the request stream, which the response stream holds. Resume
    /// rebuilds the request view from its records (`codec::chat::request::read_request`).
    pub request: Writer,
}
