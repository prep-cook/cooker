//! `model-catalog`: per-model settings chosen by glob over the upstream model name.
//!
//! Each entry has a `match` glob and `overrides`. `*` stays inside one `/`-separated
//! segment and `**` spans `/`; everything else is literal and case-sensitive. Every
//! matching entry applies in file order and overrides merge as an RFC 7396 patch, so a
//! later entry replaces an earlier value and `null` removes it.
//!
//! Only `default-max-tokens` exists: the `max_tokens` an Anthropic Messages destination
//! sends for a request that carries no limit. An override beats the registry's
//! `max_completion_tokens`; with neither, no value resolves and the candidate is skipped
//! (owner ruling Q-H), never filled with a guess.

use serde_yaml_ng::Value;

use super::Config;

/// Whether a rule leaves `default-max-tokens` alone, sets it or removes it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Patch {
    Keep,
    Set(u64),
    Clear,
}

#[derive(Debug)]
struct Rule {
    pattern: Vec<Token>,
    default_max_tokens: Patch,
}

#[derive(Debug, PartialEq, Eq)]
enum Token {
    Literal(char),
    /// `*`: any run inside one segment.
    Segment,
    /// `**`: any run, `/` included.
    Any,
}

/// The rules of one config snapshot.
#[derive(Debug, Default)]
pub struct ModelCatalog(Vec<Rule>);

impl ModelCatalog {
    fn from_config(cfg: &Config) -> Self {
        let entries = cfg.document.get("model-catalog").and_then(Value::as_sequence);
        Self(entries.into_iter().flatten().filter_map(Rule::parse).collect())
    }

    fn default_max_tokens(&self, model: &str) -> Option<u64> {
        let model: Vec<char> = model.chars().collect();
        self.0
            .iter()
            .filter(|rule| matches(&rule.pattern, &model))
            .fold(None, |value, rule| match rule.default_max_tokens {
                Patch::Keep => value,
                Patch::Set(n) => Some(n),
                Patch::Clear => None,
            })
    }
}

impl Rule {
    /// An entry the schema and [`check`] accepted; anything else is skipped.
    fn parse(entry: &Value) -> Option<Self> {
        let pattern = entry.get("match")?.as_str()?;
        let default_max_tokens = match entry
            .get("overrides")
            .and_then(|o| o.as_mapping()?.get("default-max-tokens"))
        {
            None => Patch::Keep,
            Some(Value::Null) => Patch::Clear,
            Some(value) => Patch::Set(value.as_u64().filter(|n| *n > 0)?),
        };
        Some(Self {
            pattern: tokens(pattern),
            default_max_tokens,
        })
    }
}

/// Rejects an entry [`ModelCatalog`] would otherwise skip silently.
pub(super) fn check(doc: &Value) -> anyhow::Result<()> {
    let entries = doc.get("model-catalog").and_then(Value::as_sequence);
    for (i, entry) in entries.into_iter().flatten().enumerate() {
        if entry.get("match").and_then(Value::as_str).is_none_or(str::is_empty) {
            anyhow::bail!("model-catalog[{i}].match must be a non-empty glob");
        }
        if Rule::parse(entry).is_none() {
            anyhow::bail!("model-catalog[{i}].overrides.default-max-tokens must be a positive integer or null");
        }
    }
    Ok(())
}

/// The `max_tokens` to send for `model` when the client sent none: the catalog's
/// `default-max-tokens`, else `registry` (the model's registered `max_completion_tokens`),
/// else `None`. The catalog is parsed once per config snapshot.
pub fn default_max_tokens(cfg: &Config, model: &str, registry: impl FnOnce() -> Option<u64>) -> Option<u64> {
    cfg.derived(ModelCatalog::from_config)
        .default_max_tokens(model)
        .or_else(registry)
        .filter(|n| *n > 0)
}

fn tokens(pattern: &str) -> Vec<Token> {
    let mut out = Vec::new();
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        out.push(match c {
            '*' if chars.next_if_eq(&'*').is_some() => {
                // Runs of stars are one `**`.
                while chars.next_if_eq(&'*').is_some() {}
                Token::Any
            }
            '*' => Token::Segment,
            c => Token::Literal(c),
        });
    }
    out
}

/// Whether `pattern` matches all of `text`, in time linear in both.
fn matches(pattern: &[Token], text: &[char]) -> bool {
    // `row[j]`: the pattern prefix read so far matches `text[..j]`.
    let mut row = vec![false; text.len() + 1];
    row[0] = true;
    for token in pattern {
        let mut next = vec![false; text.len() + 1];
        match token {
            Token::Literal(c) => {
                for j in 0..text.len() {
                    next[j + 1] = row[j] && text[j] == *c;
                }
            }
            Token::Segment | Token::Any => {
                next[0] = row[0];
                for j in 0..text.len() {
                    let crosses = *token == Token::Segment && text[j] == '/';
                    next[j + 1] = row[j + 1] || (next[j] && !crosses);
                }
            }
        }
        row = next;
    }
    row[text.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    const RULES: &str = r#"
model-catalog:
  - match: "claude-*"
    overrides: {default-max-tokens: 8192}
  - match: "claude-haiku-*"
    overrides: {default-max-tokens: 4096}
  - match: "claude-opus-*"
    overrides: {default-max-tokens: null}
  - match: "vendor/**"
    overrides: {default-max-tokens: 2000}
  - match: "pool/*"
    overrides: {}
  - match: "pool/fixed"
    overrides: {default-max-tokens: 512}
"#;

    #[test]
    fn the_last_matching_rule_decides_and_the_registry_fills_the_gap() {
        // (config, model, registry value, expected)
        let rows: [(&str, &str, Option<u64>, Option<u64>); 14] = [
            (RULES, "claude-sonnet-4-5", None, Some(8192)),
            // A later, narrower rule replaces the value.
            (RULES, "claude-haiku-4-5", None, Some(4096)),
            // An override beats the registry.
            (RULES, "claude-haiku-4-5", Some(64_000), Some(4096)),
            // `null` removes the value; the registry then answers, else nothing does.
            (RULES, "claude-opus-4-7", Some(32_000), Some(32_000)),
            (RULES, "claude-opus-4-7", None, None),
            (RULES, "gpt-6", None, None),
            (RULES, "gpt-6", Some(1000), Some(1000)),
            // `*` stays in one segment, `**` crosses.
            (RULES, "vendor/claude-x", None, Some(2000)),
            (RULES, "vendor/a/b", None, Some(2000)),
            (
                "model-catalog:\n  - match: \"a/*\"\n    overrides: {default-max-tokens: 9}\n",
                "a/b/c",
                None,
                None,
            ),
            // An entry without the setting leaves the earlier value.
            (RULES, "pool/other", None, None),
            (RULES, "pool/fixed", None, Some(512)),
            // No catalog: the registry alone, and a zero registry value is no value.
            ("", "claude-sonnet-4-5", Some(8000), Some(8000)),
            ("", "claude-sonnet-4-5", Some(0), None),
        ];
        for (yaml, model, registry, expected) in rows {
            let cfg = Config::parse(yaml).unwrap();
            assert_eq!(
                default_max_tokens(&cfg, model, || registry),
                expected,
                "{model} in {yaml}"
            );
        }
    }

    #[test]
    fn globs_follow_segment_rules() {
        let rows = [
            ("*", "abc", true),
            ("*", "a/b", false),
            ("**", "a/b", true),
            ("a*c", "abc", true),
            ("a*c", "ac", true),
            ("a*c", "a/c", false),
            ("a**c", "a/b/c", true),
            ("a***c", "a/c", true),
            ("claude-?", "claude-?", true),
            ("claude-sonnet", "Claude-Sonnet", false),
            ("", "", true),
            ("", "a", false),
        ];
        for (pattern, text, expected) in rows {
            let text: Vec<char> = text.chars().collect();
            assert_eq!(matches(&tokens(pattern), &text), expected, "{pattern}");
        }
    }

    #[test]
    fn a_catalog_entry_that_would_be_skipped_is_refused() {
        let rows = [
            ("model-catalog:\n  - match: \"\"\n", false),
            ("model-catalog:\n  - overrides: {default-max-tokens: 5}\n", false),
            (
                "model-catalog:\n  - {match: m, overrides: {default-max-tokens: 0}}\n",
                false,
            ),
            (
                "model-catalog:\n  - {match: m, overrides: {default-max-tokens: -3}}\n",
                false,
            ),
            (
                "model-catalog:\n  - {match: m, overrides: {default-max-tokens: x}}\n",
                false,
            ),
            (
                "model-catalog:\n  - {match: m, overrides: {default-max-tokens: 5}}\n",
                true,
            ),
            (
                "model-catalog:\n  - {match: m, overrides: {default-max-tokens: null}}\n",
                true,
            ),
            ("model-catalog:\n  - {match: m}\n", true),
        ];
        for (yaml, accepted) in rows {
            assert_eq!(Config::parse(yaml).is_ok(), accepted, "{yaml}");
        }
    }

    #[test]
    fn the_strict_schema_knows_the_catalog_fields_only() {
        let rows = [
            ("model-catalog: [{match: m, overrides: {default-max-tokens: 5}}]", true),
            (
                "model-catalog: [{match: m, overrides: {default-max-tokens: null}}]",
                true,
            ),
            ("model-catalog: [{match: m, overrides: {default-max-token: 5}}]", false),
            (
                "model-catalog: [{match: m, overrides: {default-max-tokens: many}}]",
                false,
            ),
            ("model-catalog: [{match: m, override: {}}]", false),
            ("model-catalog: {match: m}", false),
        ];
        for (yaml, accepted) in rows {
            let doc = serde_yaml_ng::from_str::<Value>(&format!("config-version: 8\n{yaml}\n")).unwrap();
            assert_eq!(
                crate::config::validate_config_fields(&doc, true).is_ok(),
                accepted,
                "{yaml}"
            );
        }
    }
}
