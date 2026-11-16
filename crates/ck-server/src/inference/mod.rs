//! Inference endpoints, model discovery and upstream dispatch.

pub mod aa;
pub(crate) mod claude;
pub(crate) mod codex_alpha;
mod codex_models;
pub mod dispatch;
pub(crate) mod errors;
pub(crate) mod gemini;
pub(crate) mod images;
pub mod keepalive;
mod media;
pub(crate) mod model_router;
pub mod model_updater;
pub(crate) mod models;
pub(crate) mod openai;
pub mod responses_state;
pub(crate) mod sanitize;
pub mod sessions;
pub(crate) mod speech;
pub mod streams;
pub(crate) mod videos;
pub(crate) mod xai_deferred;
