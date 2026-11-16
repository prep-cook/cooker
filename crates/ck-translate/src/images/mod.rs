//! OpenAI Images, xAI images and Codex `image_generation` tool events (§5.19-5.20 of
//! INTERNAL-STREAM).
//!
//! Requests are position-backed documents like the speech and search codecs: [`Decoder`]
//! validates one JSON body, [`from_form`] reads the E1 multipart document, and [`encode`] writes
//! one upstream body for a [`Destination`]. A control the destination cannot express fails with
//! [`Error::Unsupported`](crate::codec::Error::Unsupported) before any byte is produced; unknown
//! members reach only a destination of their own protocol (§3.7).
//!
//! Responses publish one media lifetime per image and one `image.result` operation per finished
//! image, with the response's single usage snapshot first. [`Ingress`] reads buffered JSON (OpenAI,
//! xAI) and SSE (OpenAI streaming, Codex tool events); [`Egress`] writes buffered JSON or SSE, so
//! one canonical stream serves both client forms. Executors, routes and credentials stay with
//! E6 and E7.

mod request;
mod request_form;
mod request_multipart;
mod response;
mod response_egress;

pub use request::{Decoder, Destination, Mode, RequestFailure, encode};
pub use request_form::from_form;
pub use response::{Ingress, Wire};
pub use response_egress::{Delivery, Dropped, Egress, Format};
