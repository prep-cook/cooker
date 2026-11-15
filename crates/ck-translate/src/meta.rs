//! Meta request policy, independent of Codex transport defaults.
use crate::{
    Error, RequestCtx,
    responses_policy::{Policy, validate_meta},
};
use ck_core::format::Format;

/// Translate request structure without imposing Codex's generation controls.
pub fn translate_request(from: Format, ctx: &RequestCtx<'_>, body: &[u8], compat: bool) -> Result<Vec<u8>, Error> {
    validate_meta(from, body)?;
    crate::translate_registered(from, Format::Codex, ctx, body, |body| match from {
        Format::OpenAIResponse | Format::Codex => Ok(crate::codex_responses::convert_with_policy(body, Policy::Meta)),
        Format::OpenAI => crate::codex_chat_request::convert_with_policy(ctx.model, body, ctx.stream, Policy::Meta),
        Format::Claude => crate::codex_claude::convert_with_policy(ctx.model, body, compat, Policy::Meta),
        Format::Gemini => Ok(crate::codex_gemini::convert_with_policy(ctx.model, body, Policy::Meta)),
        Format::Interactions => (crate::codex_interactions::PAIR.pair.request)(ctx, body),
        _ => Err(Error("Meta request translation is not registered".into())),
    })
}
