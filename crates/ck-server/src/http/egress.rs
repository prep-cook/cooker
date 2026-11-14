//! Client response egress (`docs/EXECUTION-CONTRACT.md`). The client protocol's
//! `ResponseEgress` replaces the route writers in `respond.rs`.
//!
//! Commitment is the first deliverable byte the client codec emits for a response. Before
//! it, nothing reaches the client: no status line, success header or keepalive, and an
//! attempt failure may fail over to a fresh attempt. Once deliverable bytes are scheduled
//! to the client, failover is disabled even if the socket write then fails, and a later
//! failure reaches the client as the stream's `error` terminal in its own protocol.

use ck_translate::codec::Operation;

/// Client protocols served over the internal stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Protocol {
    Anthropic,
    Chat,
    Responses,
    Gemini,
}

/// Client body framing. `Json` is a buffered body; `Sse` and `JsonArray` stream, the latter
/// as Gemini's `streamGenerateContent` without `alt=sse`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Framing {
    Json,
    Sse,
    JsonArray,
}

/// What response egress encodes with. Fixed when the exchange starts; a resume reuses it
/// and cannot replace it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Output {
    pub protocol: Protocol,
    pub operation: Operation,
    pub framing: Framing,
}
