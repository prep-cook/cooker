//! Client exchanges and their retained response streams (`docs/EXECUTION-CONTRACT.md`,
//! `docs/INTERNAL-STREAM.md` §6.6–6.7).
//!
//! An [`Exchange`] is one client request. Each routing attempt writes its own ck-stream;
//! the attempt whose egress commits becomes the exchange's [`StreamHandle`], registered
//! under a fresh [`StreamId`]. Resume (`GET /v1/streams`) authorizes the caller against
//! the handle's owner before any cursor lookup and re-encodes with the handle's output.

use ck_core::exec::Caller;

use crate::http::egress::Output;

/// The resume identity: 128 random bits, sent only in the `X-Ck-Stream-Id` header. It has
/// no `Debug` or `Display`, so it cannot reach a log through formatting.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct StreamId(pub [u8; 16]);

/// One client request from ingress to commitment. Created before the first attempt;
/// consumed by commitment into a [`StreamHandle`].
pub struct Exchange {
    /// The creating client. Only this identity may resume the committed stream.
    pub owner: Caller,
    pub output: Output,
}

/// A committed response retained for delivery and resume until expiry or eviction. A
/// client disconnect releases only its reader.
pub struct StreamHandle {
    pub id: StreamId,
    pub owner: Caller,
    pub output: Output,
    /// ck-stream id of the committed attempt's stream (`ck_stream::Store::reader`).
    pub stream: u64,
}
