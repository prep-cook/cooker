//! Codex Images API (codex_openai_images.go) model routing, which `codex_media` serves on the
//! execution contract.
//!
//! A gpt-image-* model goes straight to the backend's own `/images/*` endpoint
//! ("direct"). Any other model becomes a Responses request that forces the
//! `image_generation` tool on the images main model (`multimedia.gpt-image-2-base-model`
//! when it names a `gpt-` model, else gpt-5.4-mini).

use ck_core::exec::ExecError;
use ck_core::gostr::GoStr;

use crate::codex_json::{delete, set_bool_if_different, set_str_if_different};
use crate::codex_request::{self as request, Settings};

/// `codexOpenAIImagesMainModel`.
const MAIN_MODEL: &str = "gpt-5.4-mini";
/// `codexDefaultImageToolModel`.
const DEFAULT_TOOL_MODEL: &str = "gpt-image-2";
/// Models the backend serves on its own Images endpoints (`codexIsDirectOpenAIImageModel`).
const DIRECT_MODELS: [&str; 5] = [
    "gpt-image-1.5",
    "gpt-image-2",
    "gpt-image-2.5-flare",
    "gpt-image-2.5-sunburst",
    "gpt-image-2.5",
];
/// The Images API's source format name in payload rules and thinking.
pub(crate) const SOURCE: &str = "openai-image";

/// The first of `models` that names a gpt-image-* model the backend serves directly, as its
/// base model.
pub(crate) fn direct_route<'a>(models: impl IntoIterator<Item = &'a str>) -> Option<String> {
    models
        .into_iter()
        .map(image_base_model)
        .find(|m| DIRECT_MODELS.contains(&m.as_str()))
}

/// `directOpenAIImageModel`: the upstream model of a direct route `route`, the gpt-image-* model
/// a route named, for the execution model `model`. A name the credential's config lists is sent
/// as it is; otherwise a gpt-image-* model loses its routing prefix and case (51bde0ff).
pub(crate) fn resolve_direct(route: String, model: &str, settings: &Settings) -> String {
    let model = request::base_model(model).trim().to_owned();
    if model.is_empty() {
        return route;
    }
    let configured = settings.key_models.iter().any(|m| {
        let field = |k: &str| m.get(k).and_then(serde_json::Value::as_str).unwrap_or_default().trim();
        let name = Some(field("name"))
            .filter(|n| !n.is_empty())
            .unwrap_or_else(|| field("alias"));
        request::base_model(name).trim().go_eq_fold(&model)
    });
    if configured {
        return model;
    }
    let base = image_base_model(&model);
    if DIRECT_MODELS.contains(&base.as_str()) {
        base
    } else {
        model
    }
}

/// `codexOpenAIImageBaseModel`: no thinking suffix, no `provider/` prefix, lower case.
fn image_base_model(model: &str) -> String {
    let base = request::base_model(model);
    let mut base = base.trim();
    if let Some(i) = base.rfind('/')
        && i < base.len() - 1
    {
        base = base[i + 1..].trim();
    }
    base.trim().go_lower()
}

/// `resolveGPTImage2BaseModel`.
pub(crate) fn main_model(configured: &str) -> &str {
    if !configured.is_empty() && configured.go_lower().starts_with("gpt-") {
        configured
    } else {
        MAIN_MODEL
    }
}

/// `codexOpenAIImageToolModel`.
pub(crate) fn tool_model(request_model: &str, route_model: &str) -> String {
    [request_model.trim(), route_model.trim()]
        .into_iter()
        .find(|m| !m.is_empty())
        .unwrap_or(DEFAULT_TOOL_MODEL)
        .to_owned()
}

/// `prepareCodexOpenAIImageBody`: thinking, then the Codex request rules.
pub(crate) fn tool_body(body: &str, main_model: &str, siwc: bool) -> Result<String, ExecError> {
    let thought = ck_wire::thinking::apply_thinking_with_source_payload(
        body.as_bytes(),
        body.as_bytes(),
        body.as_bytes(),
        main_model,
        SOURCE,
        "codex",
        "codex",
        false,
    )
    .map_err(|e| ExecError::local(e.status(), ck_core::exec::FailureScope::Request, e.message))?;
    let mut out = String::from_utf8_lossy(&thought).into_owned();
    out = set_str_if_different(out, "model", main_model);
    out = set_bool_if_different(out, "stream", true);
    for key in [
        "previous_response_id",
        "prompt_cache_retention",
        "safety_identifier",
        "stream_options",
    ] {
        out = delete(&out, key);
    }
    if siwc {
        return Ok(request::siwc_body(out, request::Call::Stream, false, false));
    }
    let mut bytes = out.into_bytes();
    ck_wire::codex_client::normalize_codex_instructions(&mut bytes);
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `TestCodexImagePayloadPreservesResolvedAlias` and the model column of
    /// `TestCodexImageResolvedModelOnWire` (its payload-rule rows need the media payload
    /// finalizer, 868e137): a configured upstream name is sent as it is, and only an
    /// unconfigured gpt-image-* route loses its prefix and case. The route comes from the
    /// client's model, else the execution model, as `codex_media` finds it.
    #[test]
    fn direct_images_keep_the_resolved_upstream_model() {
        let direct = |requested: &str, resolved: &str, settings: &Settings| {
            direct_route([requested, resolved]).map(|route| resolve_direct(route, resolved, settings))
        };
        assert_eq!(
            direct("gpt-image-2", "openai/gpt-image-2-cpr", &Settings::default()).as_deref(),
            Some("openai/gpt-image-2-cpr")
        );
        for (requested, resolved, configured, want) in [
            (
                "gpt-image-2",
                "openai/gpt-image-2-cpr",
                "openai/gpt-image-2-cpr",
                "openai/gpt-image-2-cpr",
            ),
            (
                "gpt-image-2",
                "openai/gpt-image-2",
                "openai/gpt-image-2",
                "openai/gpt-image-2",
            ),
            (
                "gpt-image-2",
                "OpenAI/GPT-Image-2",
                "OpenAI/GPT-Image-2",
                "OpenAI/GPT-Image-2",
            ),
            ("gpt-image-2", "gpt-image-1.5", "gpt-image-1.5", "gpt-image-1.5"),
            (
                "gpt-image-2",
                "codex/gpt-image-2",
                "codex/gpt-image-2",
                "codex/gpt-image-2",
            ),
            (
                "openai/gpt-image-2",
                "openai/gpt-image-2",
                "openai/gpt-image-2",
                "openai/gpt-image-2",
            ),
            (
                "tenant/gpt-image-2(high)",
                "OpenAI/GPT-Image-2(high)",
                "OpenAI/GPT-Image-2",
                "OpenAI/GPT-Image-2",
            ),
            (
                "codex/gpt-image-1.5(high)",
                "codex/gpt-image-1.5(high)",
                "",
                "gpt-image-1.5",
            ),
            ("CODEX/GPT-Image-2(high)", "CODEX/GPT-Image-2(high)", "", "gpt-image-2"),
        ] {
            let settings = Settings {
                key_models: if configured.is_empty() {
                    Vec::new()
                } else {
                    vec![serde_json::json!({"name": configured, "alias": "gpt-image-2"})]
                },
                ..Settings::default()
            };
            assert_eq!(
                direct(requested, resolved, &settings).as_deref(),
                Some(want),
                "{requested} -> {resolved}"
            );
        }
    }
}
