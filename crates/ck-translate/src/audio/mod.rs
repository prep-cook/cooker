//! Bounded `/v1/audio/speech` requests and audio-file responses only.
//!
//! Request documents use `model`, `text` and `x_ck.provider_parameters.audio`;
//! unknown wire members remain provenance. Like the search document codec, request
//! decoding/encoding owns no transport or execution fence. The boundary must admit
//! the complete encoded request before sending it. Live audio and SSE speech are
//! unsupported. Response `finish` means successful, complete HTTP entity framing;
//! a body read failure must call `fail`, even without a declared content length.

mod request;
mod response;

pub use request::{Decoder, Destination, RequestFailure, check, encode};
pub use response::{Egress, Ingress};
