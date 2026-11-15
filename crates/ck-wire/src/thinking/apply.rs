//! The canonical pipeline (apply.go) and the executor entry point
//! (executor/helps/model_capabilities.go, helps/thinking.go).

use super::providers::applier;
use super::source::SourceReads;
use super::summary::{
    SummaryConfig, SummaryMode, apply_summary_config_for_model, apply_summary_config_for_provider,
    extract_explicit_summary_config, extract_summary_config, extract_translated_summary_config,
    strip_inferred_claude_summary_activation,
};
use super::validate::{is_budget_capable_provider, validate_config};
use super::{
    Config, Error, LEVEL_AUTO, LEVEL_NONE, Mode, ModelCaps, SuffixResult, convert_budget_to_level,
    convert_level_to_budget, extract_configuration_update_config, is_responses_format, is_user_defined_model,
    lookup_model_info, parse_level_suffix, parse_numeric_suffix, parse_special_suffix, parse_suffix,
    strip_configuration_updates, strip_responses_effort, strip_thinking_config, with_str,
};
use crate::gostr::GoStr;
use crate::json;

/// `ApplyThinking`: summary intent comes from the target body itself.
pub fn apply_thinking(body: &[u8], model: &str, from: &str, to: &str, provider: &str) -> Result<Vec<u8>, Error> {
    let summary = extract_summary_config(body, to);
    run(
        body.to_vec(),
        Some(b""),
        model,
        from,
        to,
        provider,
        None,
        summary,
        false,
    )
}

/// `ApplyThinkingWithSummary`.
pub fn apply_thinking_with_summary(
    body: &[u8],
    model: &str,
    from: &str,
    to: &str,
    provider: &str,
    summary: SummaryConfig,
) -> Result<Vec<u8>, Error> {
    run(
        body.to_vec(),
        Some(b""),
        model,
        from,
        to,
        provider,
        None,
        summary,
        false,
    )
}

/// `ApplyThinkingWithSourceAndSummary`.
#[allow(clippy::too_many_arguments)]
pub fn apply_thinking_with_source_and_summary(
    body: &[u8],
    source: &[u8],
    model: &str,
    from: &str,
    to: &str,
    provider: &str,
    summary: SummaryConfig,
    updates_changed: bool,
) -> Result<Vec<u8>, Error> {
    run(
        body.to_vec(),
        Some(source),
        model,
        from,
        to,
        provider,
        None,
        summary,
        updates_changed,
    )
}

/// `ApplyThinkingWithModelInfo`: the exact model definition bound to this attempt.
pub fn apply_thinking_with_model_info(
    body: &[u8],
    source: &[u8],
    model: &str,
    from: &str,
    to: &str,
    provider: &str,
    info: Option<&ModelCaps>,
) -> Result<Vec<u8>, Error> {
    let summary = if source.is_empty() {
        extract_summary_config(body, to)
    } else {
        extract_summary_config(source, from)
    };
    apply_thinking_with_model_info_and_summary(body, source, model, from, to, provider, info, summary, false)
}

/// `ApplyThinkingWithModelInfoAndSummary`.
#[allow(clippy::too_many_arguments)]
pub fn apply_thinking_with_model_info_and_summary(
    body: &[u8],
    source: &[u8],
    model: &str,
    from: &str,
    to: &str,
    provider: &str,
    info: Option<&ModelCaps>,
    summary: SummaryConfig,
    updates_changed: bool,
) -> Result<Vec<u8>, Error> {
    run(
        body.to_vec(),
        Some(source),
        model,
        from,
        to,
        provider,
        Some(info),
        summary,
        updates_changed,
    )
}

/// Inputs to `helps.ApplyRequestThinking` besides the translated provider body.
pub struct RequestThinking<'a> {
    /// The request payload that was translated (`req.Payload`); `None` when it is the body.
    pub payload: Option<&'a [u8]>,
    /// The inbound body before interceptors (`opts.OriginalRequest`); `None` when it is the
    /// body.
    pub original: Option<&'a [u8]>,
    /// Requested model, possibly with a `(suffix)`.
    pub model: &'a str,
    pub from: &'a str,
    pub to: &'a str,
    pub provider: &'a str,
    /// Capabilities bound to this attempt (Go `cliproxyauth.ResolvedModelInfo`), when
    /// the scheduler resolved an API-key or Home model definition.
    pub resolved: Option<Option<&'a ModelCaps>>,
    /// Whether a request translator is registered for `from -> to`
    /// (`sdktranslator.HasRequestTransformer`).
    pub has_request_transformer: bool,
    pub updates_changed: bool,
}

/// `helps.ApplyRequestThinking` on the translated provider `body`, which it edits and returns:
/// unchanged, in the same allocation, when the thinking leaves it as is.
pub fn apply_request_thinking(req: &RequestThinking<'_>, body: Vec<u8>) -> Result<Vec<u8>, Error> {
    let payload = req.payload.unwrap_or(&body);
    let original = match req.original.unwrap_or(&body) {
        [] => payload,
        original => original,
    };
    let summary = translated_request_summary_config(
        &body,
        payload,
        original,
        req.model,
        req.from,
        req.to,
        req.has_request_transformer,
    );
    let source = if payload.is_empty() { req.original } else { req.payload };
    run(
        body,
        source,
        req.model,
        req.from,
        req.to,
        req.provider,
        req.resolved,
        summary,
        req.updates_changed,
    )
}

/// `helps.ApplyThinkingWithSourcePayload`.
#[allow(clippy::too_many_arguments)]
pub fn apply_thinking_with_source_payload(
    body: &[u8],
    current_source: &[u8],
    original_source: &[u8],
    model: &str,
    from: &str,
    to: &str,
    provider: &str,
    has_request_transformer: bool,
) -> Result<Vec<u8>, Error> {
    let summary = translated_request_summary_config(
        body,
        current_source,
        original_source,
        model,
        from,
        to,
        has_request_transformer,
    );
    apply_thinking_with_summary(body, model, from, to, provider, summary)
}

/// `helps.translatedRequestSummaryConfig`: the translated body wins, so a normalizer can
/// remove or rewrite the summary; the source is consulted only when the body lost the
/// intent or cannot represent it until the model-aware pass.
pub fn translated_request_summary_config(
    body: &[u8],
    current_source: &[u8],
    original_source: &[u8],
    model: &str,
    from: &str,
    to: &str,
    has_request_transformer: bool,
) -> SummaryConfig {
    let from = from.trim().go_lower();
    let to = to.trim().go_lower();
    let target = if from == to {
        extract_summary_config(body, &to)
    } else {
        extract_explicit_summary_config(body, &to)
    };
    if target.mode != SummaryMode::Unspecified {
        return target;
    }
    let current = extract_translated_summary_config(current_source, &from, &to);
    let original = extract_translated_summary_config(original_source, &from, &to);
    if current.mode == SummaryMode::Unspecified {
        return original;
    }
    if !has_request_transformer {
        return SummaryConfig::default();
    }
    let candidate = apply_summary_config_for_model(body, &to, model, current.clone());
    if extract_explicit_summary_config(&candidate, &to).mode != SummaryMode::Unspecified {
        return SummaryConfig::default();
    }
    current
}

/// The thinking pipeline on an owned `body`; `source` is `None` when it is the body itself.
#[allow(clippy::too_many_arguments)]
pub(super) fn run(
    body: Vec<u8>,
    source: Option<&[u8]>,
    model: &str,
    from: &str,
    to: &str,
    provider: &str,
    resolved: Option<Option<&ModelCaps>>,
    summary: SummaryConfig,
    updates_changed: bool,
) -> Result<Vec<u8>, Error> {
    let mut target = to.trim().go_lower();
    if target == "openai-response" {
        target = "codex".into();
    }
    let mut provider = provider.trim().go_lower();
    if provider.is_empty() {
        provider.clone_from(&target);
    }
    let mut from = from.trim().go_lower();
    if from.is_empty() {
        from.clone_from(&target);
    }
    let suffix = parse_suffix(model);
    let base = suffix.model_name.clone();
    let resolved_flag = resolved.is_some();
    let looked_up;
    let info: Option<&ModelCaps> = match resolved {
        Some(info) => info,
        None => {
            looked_up = lookup_model_info(&base, &provider);
            looked_up.as_ref()
        }
    };

    let apply_fn = applier(&target);
    let reads = SourceReads {
        source: source.unwrap_or(&body),
        body: &body,
        from: &from,
        target: &target,
    }
    .read(&suffix, updates_changed, resolved_flag, info, apply_fn.is_some());
    let response_target = target == "codex" || target == "xai";
    let supports_updates = info.is_some_and(|i| i.support_configuration_update);
    let mut body = body;
    if response_target && !supports_updates {
        body = strip_configuration_updates(body);
    }
    let native_responses = response_target && is_responses_format(&from) && supports_updates;

    let Some(apply_fn) = apply_fn else {
        return Ok(body);
    };
    if reads.keeps_update {
        return Ok(body);
    }
    if native_responses && !suffix.has_suffix {
        return Ok(body);
    }
    if is_user_defined_model(info) {
        return apply_user_defined_model(
            body,
            info,
            &from,
            &target,
            &provider,
            &suffix,
            reads.config,
            native_responses,
            summary,
        );
    }
    let info = info.expect("user-defined handles None");
    if info.thinking.is_none() {
        let config = extract_thinking_config(&body, &target);
        if config.is_set() || summary.mode != SummaryMode::Unspecified {
            return Ok(if response_target {
                strip_responses_effort(body)
            } else {
                strip_thinking_config(body, &target)
            });
        }
        return Ok(body);
    }

    let config = if suffix.has_suffix {
        parse_suffix_to_config(&suffix.raw_suffix)
    } else {
        let mut config = reads.config;
        if !config.is_set() {
            config = reads.thinking;
        }
        if !config.is_set() {
            config = extract_thinking_config(&body, &target);
        }
        config
    };

    if !config.is_set() {
        if native_responses {
            return Ok(body);
        }
        if reads.summary_enabled {
            body = strip_inferred_claude_summary_activation(body, Some(info));
        }
        return Ok(apply_summary_config_for_provider(
            body,
            &target,
            &base,
            &provider,
            Some(info),
            summary,
        ));
    }
    let requested_off = config.mode == Mode::None || (config.mode == Mode::Budget && config.budget == 0);
    let validated = validate_config(config, Some(info), &from, &target, suffix.has_suffix).map_err(|mut e| {
        e.body = Some(body.clone());
        e
    })?;
    let mut applied = apply_fn(body, &validated, Some(info))?;
    if fully_disabled(&validated) || native_responses {
        return Ok(applied);
    }
    let mut summary = summary;
    if target == "claude" && requested_off {
        // The model cannot turn thinking off, so validation fell back to its lowest
        // level: drop a stale block_binding and keep the caller's intent of seeing no
        // thinking, since without an explicit omitted display cloaking would add
        // progress updates (Go ad9e456d).
        applied = super::without(applied, "thinking.block_binding");
        summary = SummaryConfig {
            mode: SummaryMode::Disabled,
            ..Default::default()
        };
    }
    Ok(apply_summary_config_for_provider(
        applied,
        &target,
        &base,
        &provider,
        Some(info),
        summary,
    ))
}

fn fully_disabled(config: &Config) -> bool {
    config.mode == Mode::None && config.budget == 0 && config.level.is_empty()
}

pub(super) fn extract_source_thinking_config(body: &[u8], provider: &str) -> Config {
    let provider = provider.trim().go_lower();
    if provider == "openai-response" {
        return extract_codex_config(body);
    }
    extract_thinking_config(body, &provider)
}

/// `parseSuffixToConfig`: special values, then levels, then budgets.
fn parse_suffix_to_config(raw: &str) -> Config {
    match parse_special_suffix(raw) {
        Some(Mode::None) => return Config::none(),
        Some(Mode::Auto) => return Config::auto(),
        _ => {}
    }
    if let Some(level) = parse_level_suffix(raw) {
        return Config::level(level);
    }
    match parse_numeric_suffix(raw) {
        Some(0) => Config::none(),
        Some(budget) => Config::budget(budget),
        None => Config::default(),
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_user_defined_model(
    body: Vec<u8>,
    info: Option<&ModelCaps>,
    from: &str,
    to: &str,
    provider: &str,
    suffix: &SuffixResult,
    source_config: Config,
    native_responses: bool,
    summary: SummaryConfig,
) -> Result<Vec<u8>, Error> {
    let model_id = info.map_or(suffix.model_name.as_str(), |i| i.id.as_str()).to_owned();
    let config = if suffix.has_suffix {
        parse_suffix_to_config(&suffix.raw_suffix)
    } else {
        let mut config = source_config;
        if !config.is_set() {
            config = extract_thinking_config(&body, from);
        }
        if !config.is_set() && from != to {
            config = extract_thinking_config(&body, to);
        }
        config
    };
    if !config.is_set() {
        return Ok(apply_summary_config_for_provider(
            body, to, &model_id, provider, info, summary,
        ));
    }
    let Some(apply_fn) = applier(to) else {
        return Ok(body);
    };
    let config = normalize_user_defined_config(config, to);
    let applied = apply_fn(body, &config, info)?;
    if fully_disabled(&config) || native_responses {
        return Ok(applied);
    }
    Ok(apply_summary_config_for_provider(
        applied, to, &model_id, provider, info, summary,
    ))
}

fn normalize_user_defined_config(config: Config, to: &str) -> Config {
    if config.mode != Mode::Level || to == "claude" || !is_budget_capable_provider(to) {
        return config;
    }
    match convert_level_to_budget(&config.level) {
        Some(budget) => Config::budget(budget),
        None => config,
    }
}

/// `extractThinkingConfig`: the canonical config a provider body carries.
pub fn extract_thinking_config(body: &[u8], provider: &str) -> Config {
    if body.is_empty() || !json::valid(body) {
        return Config::default();
    }
    match provider {
        "claude" => extract_claude_config(body),
        "gemini" | "antigravity" => extract_gemini_config(body, provider),
        "interactions" => extract_interactions_config(body),
        "openai" => extract_openai_config(body),
        "codex" | "xai" => extract_codex_config(body),
        "kimi" | "kimi-ai" | "kimi.ai" | "kimi.com" => extract_kimi_config(body),
        _ => Config::default(),
    }
}

/// `ExtractReasoningEffort`: the source request's effort for usage reporting.
pub fn extract_reasoning_effort(body: &[u8], provider: &str, model: &str) -> String {
    let provider = provider.trim().go_lower();
    if is_responses_format(&provider) {
        let effort = effort_from_config(&extract_configuration_update_config(body));
        if !effort.is_empty() {
            return effort;
        }
    }
    let suffix = parse_suffix(model);
    if suffix.has_suffix {
        let effort = effort_from_config(&parse_suffix_to_config(&suffix.raw_suffix));
        if !effort.is_empty() {
            return effort;
        }
    }
    let mut config = extract_for_usage(body, &provider);
    if !config.is_set() && (provider == "openai-response" || provider == "openai") {
        config = extract_codex_usage_config(body);
    }
    effort_from_config(&config)
}

/// `HasThinkingSuffix`: the model carries a suffix that parses to a thinking config.
pub fn has_thinking_suffix(model: &str) -> bool {
    let suffix = parse_suffix(model);
    suffix.has_suffix && parse_suffix_to_config(&suffix.raw_suffix).is_set()
}

/// `StripConfigurationUpdates`: drops `configuration_update` input items, reasoning untouched.
pub fn strip_configuration_update_items(body: &[u8]) -> Vec<u8> {
    strip_configuration_updates(body.to_vec())
}

/// `CollapseConfigurationUpdates`: drops `configuration_update` input items and, unless
/// `keep_top_level_effort`, sets the last item's effort as the top-level `reasoning.effort`.
pub fn collapse_configuration_updates(body: &[u8], keep_top_level_effort: bool) -> Vec<u8> {
    let updated = strip_configuration_updates(body.to_vec());
    if keep_top_level_effort {
        return updated;
    }
    let effort = effort_from_config(&extract_configuration_update_config(body));
    if effort.is_empty() {
        return updated;
    }
    with_str(updated, "reasoning.effort", &effort)
}

/// `ExtractTranslatedReasoningEffort`: the final provider payload's effort.
pub fn extract_translated_reasoning_effort(body: &[u8], provider: &str) -> String {
    let provider = provider.trim().go_lower();
    let mut config = extract_for_usage(body, &provider);
    if !config.is_set() && (provider == "openai" || provider == "openai-response") {
        config = extract_codex_usage_config(body);
        if !config.is_set() {
            config = extract_openai_config(body);
        }
    }
    effort_from_config(&config)
}

fn extract_for_usage(body: &[u8], provider: &str) -> Config {
    match provider.trim().go_lower().as_str() {
        "codex" | "xai" | "openai-response" => extract_codex_usage_config(body),
        // between_tools is how claude-sonnet-5-5 turns thinking off.
        "claude" if json::get(body, "thinking.type").str() == "between_tools" => Config::none(),
        p => extract_thinking_config(body, p),
    }
}

fn effort_from_config(config: &Config) -> String {
    if !config.is_set() {
        return String::new();
    }
    match config.mode {
        Mode::None => LEVEL_NONE.into(),
        Mode::Auto => LEVEL_AUTO.into(),
        Mode::Level => config.level.trim().go_lower(),
        Mode::Budget => convert_budget_to_level(config.budget).unwrap_or_default().into(),
    }
}

fn level_or_special(value: &str) -> Config {
    match value {
        "none" => Config::none(),
        "auto" => Config::auto(),
        _ => Config::level(value),
    }
}

fn budget_config(value: i64) -> Config {
    match value {
        0 => Config::none(),
        -1 => Config::auto(),
        _ => Config::budget(value),
    }
}

/// Claude: `thinking.type` disabled wins, adaptive uses `output_config.effort`, then
/// `thinking.budget_tokens`, then enabled with effort or auto.
fn extract_claude_config(body: &[u8]) -> Config {
    let kind = json::get(body, "thinking.type").str().into_owned();
    let effort = || {
        let effort = json::get(body, "output_config.effort");
        (effort.kind == json::Kind::String).then(|| effort.str().trim().go_lower())
    };
    if kind == "disabled" {
        return Config::none();
    }
    if kind == "adaptive" || kind == "auto" {
        return match effort() {
            Some(value) if value.is_empty() => Config::default(),
            Some(value) => level_or_special(&value),
            None => Config::default(),
        };
    }
    let budget = json::get(body, "thinking.budget_tokens");
    if budget.exists() {
        return budget_config(budget.int());
    }
    if kind == "enabled" {
        if let Some(value) = effort().filter(|v| !v.is_empty()) {
            return level_or_special(&value);
        }
        return Config::auto();
    }
    Config::default()
}

/// Gemini/Antigravity: `thinkingLevel` (Gemini 3) before `thinkingBudget` (2.5).
fn extract_gemini_config(body: &[u8], provider: &str) -> Config {
    let prefix = if provider == "antigravity" {
        "request.generationConfig.thinkingConfig"
    } else {
        "generationConfig.thinkingConfig"
    };
    for key in ["thinkingLevel", "thinking_level"] {
        let path = format!("{prefix}.{key}");
        let level = json::get(body, &path);
        if level.exists() {
            return level_or_special(&level.str().trim().go_lower());
        }
    }
    for key in ["thinkingBudget", "thinking_budget"] {
        let path = format!("{prefix}.{key}");
        let budget = json::get(body, &path);
        if budget.exists() {
            return budget_config(budget.int());
        }
    }
    Config::default()
}

fn extract_interactions_config(body: &[u8]) -> Config {
    for path in [
        "generation_config.thinking_level",
        "generation_config.thinkingLevel",
        "generation_config.thinking_config.thinking_level",
        "generation_config.thinking_config.thinkingLevel",
        "generation_config.thinkingConfig.thinking_level",
        "generation_config.thinkingConfig.thinkingLevel",
    ] {
        let level = json::get(body, path);
        if level.exists() {
            return level_or_special(&level.str().trim().go_lower());
        }
    }
    for path in [
        "generation_config.thinking_budget",
        "generation_config.thinkingBudget",
        "generation_config.thinking_config.thinking_budget",
        "generation_config.thinking_config.thinkingBudget",
        "generation_config.thinkingConfig.thinking_budget",
        "generation_config.thinkingConfig.thinkingBudget",
    ] {
        let budget = json::get(body, path);
        if budget.exists() {
            return budget_config(budget.int());
        }
    }
    Config::default()
}

fn extract_openai_config(body: &[u8]) -> Config {
    let effort = json::get(body, "reasoning_effort");
    if !effort.exists() {
        return Config::default();
    }
    let value = effort.str();
    if value == "none" {
        Config::none()
    } else {
        Config::level(value)
    }
}

/// Kimi: native `thinking` fields win over `reasoning_effort`.
fn extract_kimi_config(body: &[u8]) -> Config {
    let kind = json::get(body, "thinking.type");
    if kind.exists() {
        match kind.str().trim().go_lower().as_str() {
            "disabled" => return Config::none(),
            "enabled" if !json::get(body, "thinking.effort").exists() => return Config::default(),
            _ => {}
        }
    }
    let effort = json::get(body, "thinking.effort");
    if effort.exists() {
        let value = effort.str().trim().go_lower();
        return if value.is_empty() {
            Config::default()
        } else {
            level_or_special(&value)
        };
    }
    if kind.exists() {
        return Config::default();
    }
    extract_openai_config(body)
}

fn extract_codex_config(body: &[u8]) -> Config {
    let effort = json::get(body, "reasoning.effort");
    if !effort.exists() {
        return Config::default();
    }
    let value = effort.str();
    if value == "none" {
        Config::none()
    } else {
        Config::level(value)
    }
}

pub(super) fn extract_codex_usage_config(body: &[u8]) -> Config {
    if body.is_empty() || !json::valid(body) {
        return Config::default();
    }
    let config = extract_configuration_update_config(body);
    if config.is_set() {
        return config;
    }
    extract_codex_config(body)
}
