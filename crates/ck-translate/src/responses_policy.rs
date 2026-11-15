use ck_core::format::Format;
use ck_wire::json::{self as gj, Kind};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Policy {
    Codex,
    Meta,
}

impl Policy {
    pub(crate) fn codex(self) -> bool {
        self == Self::Codex
    }
}

pub(crate) fn meta_controls(out: &mut Vec<u8>, raw: &[u8], source: Format) {
    let budget = match source {
        Format::OpenAI => {
            if gj::get(raw, "max_completion_tokens").exists() {
                "max_completion_tokens"
            } else {
                "max_tokens"
            }
        }
        Format::Claude => "max_tokens",
        Format::Gemini => "generationConfig.maxOutputTokens",
        _ => "max_output_tokens",
    };
    copy(out, raw, budget, "max_output_tokens");
    for key in ["temperature", "top_p"] {
        let path = match (source, key) {
            (Format::Gemini, "temperature") => "generationConfig.temperature",
            (Format::Gemini, _) => "generationConfig.topP",
            _ => key,
        };
        copy(out, raw, path, key);
    }
    if matches!(source, Format::OpenAI | Format::OpenAIResponse | Format::Codex) {
        for key in [
            "parallel_tool_calls",
            "service_tier",
            "prompt_cache_key",
            "prompt_cache_retention",
            "safety_identifier",
            "user",
        ] {
            copy(out, raw, key, key);
        }
    }
}

fn copy(out: &mut Vec<u8>, raw: &[u8], source: &str, target: &str) {
    let value = gj::get(raw, source);
    if value.exists() {
        gj::set_raw(out, target, &value.raw);
    }
}

pub(crate) fn validate_meta(source: Format, body: &[u8]) -> Result<(), crate::Error> {
    let invalid_chat = source == Format::OpenAI
        && (gj::get(body, "n").int() > 1
            || gj::get(body, "logprobs").kind == Kind::True
            || gj::get(body, "max_output_tokens").exists());
    if invalid_chat || gj::get(body, "truncation").str() == "auto" {
        return Err(crate::Error("unsupported request option for this destination".into()));
    }
    Ok(())
}
