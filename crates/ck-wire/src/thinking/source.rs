//! What the thinking pipeline reads from the source request, before it edits the body.

use super::apply::{extract_codex_usage_config, extract_source_thinking_config};
use super::summary::{SummaryMode, extract_summary_config};
use super::{
    Config, ModelCaps, SuffixResult, extract_configuration_update_config, is_responses_format, is_user_defined_model,
};
use crate::json;

/// The source request and the body as [`run`](super::apply::run) received them, before any
/// edit: the source may be the body itself.
pub(super) struct SourceReads<'a> {
    pub(super) source: &'a [u8],
    pub(super) body: &'a [u8],
    pub(super) from: &'a str,
    pub(super) target: &'a str,
}

/// What [`run`](super::apply::run) reads from the source, each read only where `run` would
/// use it.
pub(super) struct Read {
    /// The source's Codex usage config.
    pub(super) config: Config,
    /// The body is invalid JSON and the source carries a configuration update: `run` returns
    /// the body as is. Stripping configuration updates keeps a body's validity.
    pub(super) keeps_update: bool,
    /// The source's thinking config, for a resolved model without a suffix.
    pub(super) thinking: Config,
    /// A resolved Claude target whose source enables the summary.
    pub(super) summary_enabled: bool,
}

impl SourceReads<'_> {
    pub(super) fn read(
        &self,
        suffix: &SuffixResult,
        updates_changed: bool,
        resolved: bool,
        info: Option<&ModelCaps>,
        applies: bool,
    ) -> Read {
        let (source, from, target) = (self.source, self.from, self.target);
        let mut config = Config::default();
        if is_responses_format(from) {
            let request = if !updates_changed && !source.is_empty() {
                source
            } else {
                self.body
            };
            if !updates_changed || target == "codex" || target == "xai" {
                config = extract_codex_usage_config(request);
            }
        }
        let keeps_update = applies
            && !suffix.has_suffix
            && !source.is_empty()
            && is_responses_format(from)
            && !json::valid(self.body)
            && extract_configuration_update_config(source).is_set();
        let thinking = if !suffix.has_suffix
            && !config.is_set()
            && !updates_changed
            && resolved
            && !source.is_empty()
            && !is_user_defined_model(info)
            && info.is_some_and(|i| i.thinking.is_some())
        {
            extract_source_thinking_config(source, from)
        } else {
            Config::default()
        };
        let summary_enabled = resolved
            && target == "claude"
            && from != target
            && extract_summary_config(source, from).mode == SummaryMode::Enabled;
        Read {
            config,
            keeps_update,
            thinking,
            summary_enabled,
        }
    }
}
