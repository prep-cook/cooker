//! Usage records for the usage queue, one per upstream attempt (Go
//! sdk/cliproxy/usage/accounting.go, internal/runtime/executor/helps/usage_helpers.go
//! and response_model.go, internal/redisqueue/plugin.go `HandleUsage`).
//!
//! Go's executors parse usage from the upstream response in its own format. Executors
//! that forward those payloads through `ExecRequest::usage` get the same parse here;
//! otherwise the dispatch loop parses the client-format response, which matches Go
//! whenever the upstream speaks the client's format.
//!
//! The queued record adds what Go's lacks, after Go's fields: where its `session_id`
//! came from, a [`prompt_prefix`] hash, per-stage [`Timings`] and byte counts
//! (docs/DIFFERENCES-FROM-GO.md).

use ck_core::format::Format;
use ck_wire::gostr::GoStr;
use ck_wire::json::{self as gj, Kind, Res};

use super::usage_snapshot::{self, CanonicalUsage, Dimension, ResponseFacts, Unit};

/// Go `usage.TokenAccountingQuality`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Quality {
    Complete,
    Inconsistent,
    Unclassified,
}

/// Go `usage.TokenBreakdown` (schema version 2), always valid when present.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Breakdown {
    quality: Quality,
    total: i64,
    /// total, uncached, cache read, cache write.
    input: [i64; 4],
    /// total, non-reasoning, reasoning.
    output: [i64; 3],
    unclassified: i64,
}

/// Go `usage.Detail`. `breakdown: None` is Go's zero (invalid) breakdown.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Detail {
    pub input: i64,
    pub output: i64,
    pub reasoning: i64,
    pub cached: i64,
    pub cache_read: i64,
    pub cache_creation: i64,
    pub cache_creation_5m: Option<i64>,
    pub cache_creation_1h: Option<i64>,
    pub cache_creation_30m: Option<i64>,
    pub cache_creation_12h: Option<i64>,
    pub total: i64,
    pub breakdown: Option<Breakdown>,
    pub response_service_tier: String,
    pub unreported: Unreported,
    /// Billing dimensions besides the token counts (`usage_snapshot::DIMENSIONS`).
    pub dimensions: Vec<(&'static str, Dimension)>,
    /// The usage members holding dimensions that the parsed usage reported, present and not
    /// null, so a streamed update replaces exactly those ([`merge_dimensions`]).
    pub dimension_members: Vec<&'static str>,
}

/// Counts an attempt's canonical usage did not report (`docs/EXECUTION-CONTRACT.md` §8):
/// the queued record omits them instead of writing zero. The legacy per-protocol parsers
/// report every count.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Unreported {
    pub input: bool,
    pub output: bool,
    pub reasoning: bool,
    pub cache_read: bool,
    pub cache_creation: bool,
    pub total: bool,
}

impl Unreported {
    pub const ALL: Self = Self {
        input: true,
        output: true,
        reasoning: true,
        cache_read: true,
        cache_creation: true,
        total: true,
    };
}

impl Detail {
    /// The record of an attempt's canonical usage. Canonical counts are inclusive, cache
    /// reads and writes inside input and reasoning inside output, so the breakdown is Go's
    /// subset one whatever the provider.
    pub fn canonical(usage: &CanonicalUsage, facts: &ResponseFacts) -> Self {
        let count = |n: Option<u64>| n.map_or(0, |n| i64::try_from(n).unwrap_or(i64::MAX));
        let (input, output, total) = (
            count(usage.input_tokens),
            count(usage.output_tokens),
            count(usage.total_tokens),
        );
        let (cache_read, cache_write, reasoning) = (
            count(usage.cache_read_tokens),
            count(usage.cache_write_tokens),
            count(usage.reasoning_tokens),
        );
        // ponytail: an unreported cache or reasoning count enters the breakdown as none, as
        // Go's accounting did; the stored counts keep it absent.
        let breakdown = if usage.input_tokens.is_some() && usage.output_tokens.is_some() {
            subset(input, cache_read, cache_write, output, reasoning, total)
        } else {
            unclassified(total)
        };
        Self {
            input,
            output,
            reasoning,
            cached: cache_read,
            cache_read,
            cache_creation: cache_write,
            cache_creation_5m: usage.cache_write_5m_tokens.map(|n| count(Some(n))),
            cache_creation_1h: usage.cache_write_1h_tokens.map(|n| count(Some(n))),
            cache_creation_30m: usage.cache_write_30m_tokens.map(|n| count(Some(n))),
            cache_creation_12h: usage.cache_write_12h_tokens.map(|n| count(Some(n))),
            total,
            breakdown: Some(breakdown),
            response_service_tier: facts
                .service_tier
                .clone()
                .or_else(|| usage.service_tier.clone())
                .unwrap_or_default(),
            unreported: Unreported {
                input: usage.input_tokens.is_none(),
                output: usage.output_tokens.is_none(),
                reasoning: usage.reasoning_tokens.is_none(),
                cache_read: usage.cache_read_tokens.is_none(),
                cache_creation: usage.cache_write_tokens.is_none(),
                total: usage.total_tokens.is_none(),
            },
            dimensions: usage.dimensions.clone(),
            dimension_members: Vec::new(),
        }
    }
}

/// Go `nonNegativeSum`.
fn sum(values: &[i64]) -> Option<i64> {
    values
        .iter()
        .try_fold(0i64, |acc, v| if *v < 0 { None } else { acc.checked_add(*v) })
}

/// Go `resolveAccountingTotal`.
fn resolve_total(total: i64, expected: i64) -> (i64, bool) {
    if total < 0 || expected < 0 {
        (0, false)
    } else if total == 0 {
        (expected, true)
    } else {
        (total, total == expected)
    }
}

fn inconsistent(total: i64, fallback: i64) -> Breakdown {
    let mut resolved = if total <= 0 { fallback } else { total };
    if resolved < 0 {
        resolved = 0;
    }
    Breakdown {
        quality: Quality::Inconsistent,
        total: resolved,
        input: [0; 4],
        output: [0; 3],
        unclassified: resolved,
    }
}

fn complete(total: i64, input: [i64; 4], output: [i64; 3]) -> Breakdown {
    Breakdown {
        quality: Quality::Complete,
        total,
        input,
        output,
        unclassified: 0,
    }
}

/// Go `NewSubsetTokenBreakdown`: cache inside input, reasoning inside output.
fn subset(input: i64, cache_read: i64, cache_write: i64, output: i64, reasoning: i64, total: i64) -> Breakdown {
    let cache = sum(&[cache_read, cache_write]);
    let expected = sum(&[input, output]);
    let expected_or_zero = expected.unwrap_or(0);
    let Some(cache) = cache.filter(|c| expected.is_some() && reasoning >= 0 && *c <= input && reasoning <= output)
    else {
        return inconsistent(total, expected_or_zero);
    };
    let (resolved, ok) = resolve_total(total, expected_or_zero);
    if !ok {
        return inconsistent(total, expected_or_zero);
    }
    complete(
        resolved,
        [input, input - cache, cache_read, cache_write],
        [output, output - reasoning, reasoning],
    )
}

/// Go `NewPartialSubsetTokenBreakdown`: an authoritative remainder is unclassified.
fn partial_subset(input: i64, cache_read: i64, cache_write: i64, output: i64, reasoning: i64, total: i64) -> Breakdown {
    let cache = sum(&[cache_read, cache_write]);
    let expected = sum(&[input, output]);
    let expected_or_zero = expected.unwrap_or(0);
    let Some(cache) = cache.filter(|c| {
        expected.is_some()
            && input >= 0
            && output >= 0
            && reasoning >= 0
            && *c <= input
            && reasoning <= output
            && total >= 0
    }) else {
        return inconsistent(total, expected_or_zero);
    };
    let resolved = if total == 0 { expected_or_zero } else { total };
    if resolved < expected_or_zero {
        return inconsistent(total, expected_or_zero);
    }
    let unclassified = resolved - expected_or_zero;
    Breakdown {
        quality: if unclassified > 0 {
            Quality::Unclassified
        } else {
            Quality::Complete
        },
        total: resolved,
        input: [input, input - cache, cache_read, cache_write],
        output: [output, output - reasoning, reasoning],
        unclassified,
    }
}

/// Go `NewIndependentTokenBreakdown`: every bucket separate.
fn independent(
    uncached: i64,
    cache_read: i64,
    cache_write: i64,
    non_reasoning: i64,
    reasoning: i64,
    total: i64,
) -> Breakdown {
    let input = sum(&[uncached, cache_read, cache_write]);
    let output = sum(&[non_reasoning, reasoning]);
    let expected = sum(&[input.unwrap_or(0), output.unwrap_or(0)]);
    let expected_or_zero = expected.unwrap_or(0);
    let (Some(input), Some(output), Some(_)) = (input, output, expected) else {
        return inconsistent(total, expected_or_zero);
    };
    let (resolved, ok) = resolve_total(total, expected_or_zero);
    if !ok {
        return inconsistent(total, expected_or_zero);
    }
    complete(
        resolved,
        [input, uncached, cache_read, cache_write],
        [output, non_reasoning, reasoning],
    )
}

/// Go `NewSeparateReasoningTokenBreakdown`: cache inside input, reasoning separate.
fn separate_reasoning(
    input: i64,
    cache_read: i64,
    cache_write: i64,
    non_reasoning: i64,
    reasoning: i64,
    total: i64,
) -> Breakdown {
    let Some(cache) = sum(&[cache_read, cache_write]).filter(|c| input >= 0 && *c <= input) else {
        return inconsistent(total, 0);
    };
    let output = sum(&[non_reasoning, reasoning]);
    let expected = sum(&[input, output.unwrap_or(0)]);
    let expected_or_zero = expected.unwrap_or(0);
    let (Some(output), Some(_)) = (output, expected) else {
        return inconsistent(total, expected_or_zero);
    };
    let (resolved, ok) = resolve_total(total, expected_or_zero);
    if !ok {
        return inconsistent(total, expected_or_zero);
    }
    complete(
        resolved,
        [input, input - cache, cache_read, cache_write],
        [output, non_reasoning, reasoning],
    )
}

/// Go `NewUnclassifiedTokenBreakdown`.
fn unclassified(total: i64) -> Breakdown {
    if total <= 0 {
        let mut b = complete(0, [0; 4], [0; 3]);
        if total < 0 {
            b.quality = Quality::Inconsistent;
        }
        return b;
    }
    Breakdown {
        quality: Quality::Unclassified,
        total,
        input: [0; 4],
        output: [0; 3],
        unclassified: total,
    }
}

/// Go `invalidUsageTokenBreakdown`.
fn invalid(total: i64) -> Breakdown {
    let total = total.max(0);
    Breakdown {
        quality: Quality::Inconsistent,
        total,
        input: [0; 4],
        output: [0; 3],
        unclassified: total,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Semantics {
    Unknown,
    Subset,
    Independent,
    SeparateReasoning,
}

/// Go `tokenAccountingSemanticsFor`.
fn semantics(provider: &str, executor_type: &str) -> Semantics {
    let provider = provider.trim().go_lower();
    let executor = executor_type.trim().go_lower();
    let value = format!("{provider} {executor}");
    let value = value.trim();
    if value.is_empty() || value == "unknown" || value == "unknown unknown" {
        return Semantics::Unknown;
    }
    if executor == "openaicompatexecutor"
        || provider == "openai-compatibility"
        || provider.starts_with("openai-compatible-")
    {
        return Semantics::Subset;
    }
    if value.contains("claude") || value.contains("anthropic") {
        return Semantics::Independent;
    }
    if ["gemini", "aistudio", "antigravity", "vertex", "interaction"]
        .iter()
        .any(|m| value.contains(m))
    {
        return Semantics::SeparateReasoning;
    }
    if [
        "openai",
        "codex",
        "xai",
        "grok",
        "kimi",
        "qwen",
        "deepseek",
        "openrouter",
    ]
    .iter()
    .any(|m| value.contains(m))
    {
        return Semantics::Subset;
    }
    Semantics::Unknown
}

/// Go `unclassifiedTokenLowerBound`.
fn lower_bound(d: &Detail) -> Option<i64> {
    let cache = sum(&[d.cache_read, d.cache_creation])?;
    if d.input < 0 || d.output < 0 || d.reasoning < 0 || d.cached < 0 {
        return None;
    }
    let input = d.input.max(cache).max(d.cached);
    let output = d.output.max(d.reasoning);
    sum(&[input, output])
}

/// Go `tokenBreakdownForSemantics`.
fn breakdown_for(d: &Detail, s: Semantics) -> Breakdown {
    if d.total == 0 && d.input == 0 && d.output == 0 {
        match lower_bound(d) {
            None => return inconsistent(d.total, 0),
            Some(total)
                if total > 0
                    && (matches!(s, Semantics::Unknown | Semantics::Subset)
                        || (s == Semantics::SeparateReasoning
                            && (d.cache_read > 0 || d.cache_creation > 0 || d.cached > 0))) =>
            {
                return unclassified(total);
            }
            Some(_) => {}
        }
    }
    match s {
        Semantics::Subset => subset(d.input, d.cache_read, d.cache_creation, d.output, d.reasoning, d.total),
        Semantics::Independent => independent(d.input, d.cache_read, d.cache_creation, d.output, d.reasoning, d.total),
        Semantics::SeparateReasoning => {
            separate_reasoning(d.input, d.cache_read, d.cache_creation, d.output, d.reasoning, d.total)
        }
        Semantics::Unknown => {
            let total = if d.total == 0 {
                match lower_bound(d) {
                    Some(t) => t,
                    None => return inconsistent(d.total, 0),
                }
            } else {
                d.total
            };
            unclassified(total)
        }
    }
}

/// Go `EnsureTokenBreakdownForProvider`.
pub fn ensure_breakdown(mut d: Detail, provider: &str, executor_type: &str) -> Detail {
    if d.breakdown.is_none() {
        let s = semantics(provider, executor_type);
        if d.cache_read == 0
            && d.cached > 0
            && d.input == 0
            && d.output == 0
            && d.reasoning == 0
            && d.cache_creation == 0
            && d.total == 0
            && matches!(s, Semantics::Subset | Semantics::SeparateReasoning)
        {
            d.cache_read = d.cached;
        }
        d.breakdown = Some(breakdown_for(&d, s));
    }
    if d.total == 0 {
        d.total = d.breakdown.map_or(0, |b| b.total);
    }
    d
}

/// Go `hasNonZeroTokenUsage`.
/// The detail reports billing evidence besides token counts. Only the tier reaches the parsers
/// that drop tokenless updates: Gemini and Interactions have no dimension rows.
fn has_billing(d: &Detail) -> bool {
    !d.response_service_tier.trim().is_empty()
}

fn has_tokens(d: &Detail) -> bool {
    d.input != 0
        || d.output != 0
        || d.reasoning != 0
        || d.cached != 0
        || d.cache_read != 0
        || d.cache_creation != 0
        || d.total != 0
        || d.breakdown.is_some_and(|b| b.total != 0)
}

// ---- parsers -------------------------------------------------------------------

/// Go `jsonPayload`: the JSON object of an SSE `data:` line or a bare JSON line.
fn json_payload(line: &[u8]) -> Option<&[u8]> {
    let mut trimmed = line.trim_ascii();
    if trimmed.is_empty() || trimmed == b"[DONE]" || trimmed.starts_with(b"event:") {
        return None;
    }
    if let Some(rest) = trimmed.strip_prefix(b"data:") {
        trimmed = rest.trim_ascii();
    }
    (trimmed.first() == Some(&b'{')).then_some(trimmed)
}

fn first<'a>(root: &Res<'a>, paths: &[&str]) -> Res<'a> {
    paths.iter().map(|p| root.get(*p)).find(Res::exists).unwrap_or_default()
}

/// Go `extractResponseServiceTierFromValidJSON`.
fn service_tier_valid(payload: &[u8]) -> String {
    ["response.service_tier", "service_tier", "interaction.service_tier"]
        .iter()
        .map(|p| gj::get(payload, *p).str().trim().to_owned())
        .find(|t| !t.is_empty())
        .unwrap_or_default()
}

/// Go `extractResponseServiceTier`.
fn service_tier(payload: &[u8]) -> String {
    if payload.is_empty() || !gj::valid(payload) {
        return String::new();
    }
    service_tier_valid(payload)
}

fn has_openai_bucket_fields(node: &Res<'_>) -> bool {
    [
        "prompt_tokens",
        "input_tokens",
        "completion_tokens",
        "output_tokens",
        "prompt_tokens_details.cached_tokens",
        "input_tokens_details.cached_tokens",
        "prompt_tokens_details.cache_write_tokens",
        "prompt_tokens_details.cache_creation_tokens",
        "input_tokens_details.cache_write_tokens",
        "input_tokens_details.cache_creation_tokens",
        "completion_tokens_details.reasoning_tokens",
        "output_tokens_details.reasoning_tokens",
    ]
    .iter()
    .any(|p| node.get(*p).exists())
}

/// Go `hasOpenAIStyleUsageTokenFields`.
fn has_openai_token_fields(node: &Res<'_>) -> bool {
    node.exists() && node.is_object() && (node.get("total_tokens").exists() || has_openai_bucket_fields(node))
}

/// The unsigned integer at `path`. Absent, null, a string, a negative or fractional number and
/// one beyond `u64` are unknown: their raw text does not parse as `u64`.
fn uint_at(node: &Res<'_>, path: &str) -> Option<u64> {
    std::str::from_utf8(node.get(path).raw()).ok()?.parse().ok()
}

/// The `usage_snapshot::dimension_rows` of `protocol` read from an upstream usage object, as the
/// canonical reader reads them from a snapshot's native value.
fn native_dimensions(protocol: &str, node: &Res<'_>) -> Vec<(&'static str, Dimension)> {
    usage_snapshot::dimension_rows(protocol)
        .filter_map(|(path, name, unit)| {
            let path = path.join(".");
            let value = match unit {
                Unit::Count => uint_at(node, &path).map(Dimension::Count),
                Unit::Decimal => {
                    let value = node.get(path.as_str());
                    let raw = std::str::from_utf8(value.raw()).ok();
                    raw.filter(|_| value.kind == Kind::Number)
                        .map(|raw| Dimension::Decimal(raw.to_owned()))
                }
                Unit::Text => {
                    let value = node.get(path.as_str());
                    (value.kind == Kind::String).then(|| Dimension::Text(value.str().into_owned()))
                }
            };
            value.map(|value| (name, value))
        })
        .collect()
}

/// The usage members of `protocol`'s dimension rows that `node` reports, present and not null.
fn reported_members(protocol: &str, node: &Res<'_>) -> Vec<&'static str> {
    let mut members = Vec::new();
    for (path, _, _) in usage_snapshot::dimension_rows(protocol) {
        let reported = node.get(path[0]);
        if reported.exists() && reported.kind != Kind::Null && !members.contains(&path[0]) {
            members.push(path[0]);
        }
    }
    members
}

/// The service tier inside an upstream usage object of `protocol`, or empty.
fn native_tier(protocol: &str, node: &Res<'_>) -> String {
    usage_snapshot::tier_member(protocol)
        .map(|member| node.get(member))
        .filter(|tier| tier.kind == Kind::String)
        .map(|tier| tier.str().trim().to_owned())
        .unwrap_or_default()
}

/// Go `parseOpenAIStyleUsageNode`.
fn parse_openai_node(node: &Res<'_>) -> Detail {
    let input = first(node, &["prompt_tokens", "input_tokens"]);
    let output = first(node, &["completion_tokens", "output_tokens"]);
    let mut d = Detail {
        input: input.int(),
        output: output.int(),
        total: node.get("total_tokens").int(),
        ..Detail::default()
    };
    let cached = first(
        node,
        &[
            "prompt_tokens_details.cached_tokens",
            "input_tokens_details.cached_tokens",
        ],
    );
    if cached.exists() {
        d.cached = cached.int();
        d.cache_read = cached.int();
    }
    let creation = first(
        node,
        &[
            "input_tokens_details.cache_creation_tokens",
            "input_tokens_details.cache_write_tokens",
            "prompt_tokens_details.cache_creation_tokens",
            "prompt_tokens_details.cache_write_tokens",
        ],
    );
    if creation.exists() {
        d.cache_creation = creation.int();
    }
    let ttl = |name: &str| {
        uint_at(node, &format!("input_tokens_details.cache_write_tokens_details.{name}"))
            .map(|n| i64::try_from(n).unwrap_or(i64::MAX))
    };
    d.cache_creation_30m = ttl("ttl_30m_tokens");
    d.cache_creation_12h = ttl("ttl_12h_tokens");
    d.dimensions = native_dimensions("chat", node);
    d.dimension_members = reported_members("chat", node);
    let reasoning = first(
        node,
        &[
            "completion_tokens_details.reasoning_tokens",
            "output_tokens_details.reasoning_tokens",
        ],
    );
    if reasoning.exists() {
        d.reasoning = reasoning.int();
    }
    d.breakdown = Some(if has_openai_bucket_fields(node) {
        if input.exists() && output.exists() {
            subset(d.input, d.cache_read, d.cache_creation, d.output, d.reasoning, d.total)
        } else {
            let (cache_read, cache_creation) = if input.exists() {
                (d.cache_read, d.cache_creation)
            } else {
                (0, 0)
            };
            let reasoning = if output.exists() { d.reasoning } else { 0 };
            partial_subset(d.input, cache_read, cache_creation, d.output, reasoning, d.total)
        }
    } else {
        unclassified(d.total)
    });
    if d.total == 0 {
        d.total = d.breakdown.map_or(0, |b| b.total);
    }
    d
}

/// Go `ParseOpenAIUsage`.
pub fn parse_openai(body: &[u8]) -> Detail {
    parse_usage(body, parse_openai_node)
}

/// The paths each token count is read from, first present wins, as `parse_openai_node` reads them.
const INPUT: &[&str] = &["prompt_tokens", "input_tokens"];
const OUTPUT: &[&str] = &["completion_tokens", "output_tokens"];
const REASONING: &[&str] = &[
    "completion_tokens_details.reasoning_tokens",
    "output_tokens_details.reasoning_tokens",
];
const CACHED: &[&str] = &[
    "prompt_tokens_details.cached_tokens",
    "input_tokens_details.cached_tokens",
];
const CACHE_CREATION: &[&str] = &[
    "input_tokens_details.cache_creation_tokens",
    "input_tokens_details.cache_write_tokens",
    "prompt_tokens_details.cache_creation_tokens",
    "prompt_tokens_details.cache_write_tokens",
];

/// A Responses usage object's token counts and billed units. A count exists only when the usage
/// gives it as an integer number; any other count is unreported rather than zero, and never
/// enters a derived total or the breakdown. A total the usage omits is the sum of its input and
/// output, as the canonical reader derives it. Without both, the breakdown classifies nothing.
/// `None` when the usage has no token member and no billed unit.
fn responses_usage(node: &Res<'_>, tier: String) -> Option<Detail> {
    if !node.is_object() {
        return None;
    }
    let count = |paths: &[&str]| {
        let value = first(node, paths);
        (value.kind == Kind::Number)
            .then(|| std::str::from_utf8(value.raw()).ok()?.parse::<i64>().ok())
            .flatten()
    };
    let mut dimensions = native_dimensions("chat", node);
    dimensions.extend(native_dimensions("responses", node));
    if !has_openai_token_fields(node) && dimensions.is_empty() {
        return None;
    }
    let (input, output, total) = (count(INPUT), count(OUTPUT), count(&["total_tokens"]));
    let (reasoning, cached, creation) = (count(REASONING), count(CACHED), count(CACHE_CREATION));
    let breakdown = match (input, output) {
        (Some(input), Some(output)) => subset(
            input,
            cached.unwrap_or(0),
            creation.unwrap_or(0),
            output,
            reasoning.unwrap_or(0),
            total.unwrap_or(0),
        ),
        _ => unclassified(total.unwrap_or(0)),
    };
    let ttl = |name: &str| {
        uint_at(node, &format!("input_tokens_details.cache_write_tokens_details.{name}"))
            .map(|n| i64::try_from(n).unwrap_or(i64::MAX))
    };
    Some(Detail {
        input: input.unwrap_or(0),
        output: output.unwrap_or(0),
        reasoning: reasoning.unwrap_or(0),
        cached: cached.unwrap_or(0),
        cache_read: cached.unwrap_or(0),
        cache_creation: creation.unwrap_or(0),
        cache_creation_30m: ttl("ttl_30m_tokens"),
        cache_creation_12h: ttl("ttl_12h_tokens"),
        total: total.unwrap_or(breakdown.total),
        breakdown: Some(breakdown),
        response_service_tier: tier,
        unreported: Unreported {
            input: input.is_none(),
            output: output.is_none(),
            reasoning: reasoning.is_none(),
            cache_read: cached.is_none(),
            cache_creation: creation.is_none(),
            total: total.is_none() && (input.is_none() || output.is_none()),
        },
        dimensions,
        // A Responses stream accounts its first terminal usage alone: no later update merges.
        dimension_members: reported_members("chat", node),
        ..Detail::default()
    })
}

/// A buffered Responses body's usage through [`responses_usage`]. A body without a usage that
/// reports a count or unit, such as a pending video job's, records no count.
fn parse_responses(body: &[u8]) -> Detail {
    let tier = service_tier(body);
    responses_usage(&gj::parse(body).get("usage"), tier.clone()).unwrap_or(Detail {
        response_service_tier: tier,
        unreported: Unreported::ALL,
        ..Detail::default()
    })
}

fn parse_usage(body: &[u8], parse: fn(&Res<'_>) -> Detail) -> Detail {
    let tier = service_tier(body);
    let node = gj::parse(body).get("usage");
    if !has_openai_token_fields(&node) {
        return Detail {
            response_service_tier: tier,
            ..Detail::default()
        };
    }
    let mut d = parse(&node);
    d.response_service_tier = tier;
    d
}

/// Go `ParseCodexUsage`, through [`responses_usage`].
pub fn parse_codex(data: &[u8]) -> Option<Detail> {
    let tier = service_tier(data);
    let node = gj::parse(data).get("response.usage");
    responses_usage(&node, tier.clone()).or_else(|| {
        (!tier.is_empty()).then(|| Detail {
            response_service_tier: tier,
            ..Detail::default()
        })
    })
}

/// Go `parseClaudeUsageNode`: cache buckets are separate from `input_tokens`;
/// thinking is part of `output_tokens`.
fn parse_claude_node(node: &Res<'_>) -> Detail {
    let cache_read = node.get("cache_read_input_tokens").int();
    let cache_creation = node.get("cache_creation_input_tokens").int();
    let output = node.get("output_tokens").int();
    let reasoning = first(
        node,
        &[
            "output_tokens_details.thinking_tokens",
            "output_tokens_details.reasoning_tokens",
            "thinking_tokens",
        ],
    )
    .int()
    .max(0);
    let non_reasoning = if reasoning > 0 && reasoning <= output {
        output - reasoning
    } else if reasoning > output {
        0
    } else {
        output
    };
    let mut d = Detail {
        input: node.get("input_tokens").int(),
        output,
        reasoning,
        cached: cache_read,
        cache_read,
        cache_creation,
        cache_creation_5m: node
            .get("cache_creation.ephemeral_5m_input_tokens")
            .exists()
            .then(|| node.get("cache_creation.ephemeral_5m_input_tokens").int()),
        cache_creation_1h: node
            .get("cache_creation.ephemeral_1h_input_tokens")
            .exists()
            .then(|| node.get("cache_creation.ephemeral_1h_input_tokens").int()),
        response_service_tier: native_tier("anthropic", node),
        dimensions: native_dimensions("anthropic", node),
        dimension_members: reported_members("anthropic", node),
        ..Detail::default()
    };
    if d.cached == 0 {
        d.cached = d.cache_creation;
    }
    d.total = d
        .input
        .wrapping_add(output)
        .wrapping_add(d.cache_read)
        .wrapping_add(d.cache_creation);
    d.breakdown = Some(independent(
        d.input,
        d.cache_read,
        d.cache_creation,
        non_reasoning,
        d.reasoning,
        d.total,
    ));
    d
}

/// Go `ParseClaudeUsage`.
pub fn parse_claude(body: &[u8]) -> Detail {
    let node = gj::parse(body).get("usage");
    if !node.exists() {
        return Detail::default();
    }
    parse_claude_node(&node)
}

/// Go `ParseClaudeStreamUsage`.
fn parse_claude_stream(line: &[u8]) -> Option<Detail> {
    let payload = json_payload(line).filter(|p| gj::valid(p))?;
    let node = first(&gj::parse(payload), &["usage", "message.usage"]);
    node.exists().then(|| parse_claude_node(&node))
}

/// Go `safeUsageTokenSum`.
fn safe_sum(values: &[i64]) -> Option<i64> {
    sum(values)
}

/// Go `parseGeminiFamilyUsageDetail`.
fn parse_gemini_node(node: &Res<'_>) -> Detail {
    let cached = node.get("cachedContentTokenCount").int();
    let tool_use = first(node, &["toolUsePromptTokenCount", "tool_use_prompt_token_count"]).int();
    let input = safe_sum(&[node.get("promptTokenCount").int(), tool_use]);
    let mut d = Detail {
        input: input.unwrap_or(0),
        output: node.get("candidatesTokenCount").int(),
        reasoning: node.get("thoughtsTokenCount").int(),
        total: node.get("totalTokenCount").int(),
        cached,
        cache_read: cached,
        response_service_tier: native_tier("gemini", node),
        ..Detail::default()
    };
    if input.is_none() {
        d.breakdown = Some(invalid(d.total));
        return d;
    }
    if d.total == 0 {
        match safe_sum(&[d.input, d.output, d.reasoning]) {
            Some(total) => d.total = total,
            None => {
                d.total = 0;
                d.breakdown = Some(invalid(0));
                return d;
            }
        }
    }
    d.breakdown = Some(separate_reasoning(
        d.input,
        d.cache_read,
        d.cache_creation,
        d.output,
        d.reasoning,
        d.total,
    ));
    d
}

/// Go `ParseGeminiUsage`.
pub fn parse_gemini(body: &[u8]) -> Detail {
    let node = first(&gj::parse(body), &["usageMetadata", "usage_metadata"]);
    if !node.exists() {
        return Detail::default();
    }
    parse_gemini_node(&node)
}

/// Go `ParseGeminiStreamUsage`. The Gemini-family executors report the line Go parses
/// (the Gemini executor's `FilterSSEUsageMetadata` payload, Vertex's raw line), so a
/// non-terminal chunk's usage counts when it is reported.
fn parse_gemini_stream(line: &[u8]) -> Option<Detail> {
    let payload = json_payload(line).filter(|p| gj::valid(p))?;
    let node = first(&gj::parse(payload), &["usageMetadata", "usage_metadata"]);
    if !node.exists() {
        return None;
    }
    let d = parse_gemini_node(&node);
    (has_tokens(&d) || has_billing(&d)).then_some(d)
}

/// Go `ParseAntigravityStreamUsage`: the wrapped `response.usageMetadata` first, and
/// zero usage still counts.
fn parse_antigravity_stream(line: &[u8]) -> Option<Detail> {
    let payload = json_payload(line).filter(|p| gj::valid(p))?;
    let node = first(
        &gj::parse(payload),
        &["response.usageMetadata", "usageMetadata", "usage_metadata"],
    );
    node.exists().then(|| parse_gemini_node(&node))
}

/// Go `parseInteractionsUsageDetail`.
fn parse_interactions_node(node: &Res<'_>) -> Detail {
    let cache_read = first(node, &["cache_read_tokens", "cacheReadTokens"]);
    let tool_use = first(
        node,
        &[
            "tool_use_tokens",
            "total_tool_use_tokens",
            "toolUseTokens",
            "totalToolUseTokens",
        ],
    )
    .int();
    let input = safe_sum(&[
        first(node, &["input_tokens", "prompt_tokens", "total_input_tokens"]).int(),
        tool_use,
    ]);
    let mut d = Detail {
        input: input.unwrap_or(0),
        output: first(node, &["output_tokens", "completion_tokens", "total_output_tokens"]).int(),
        reasoning: first(
            node,
            &["reasoning_tokens", "thoughtsTokenCount", "total_thought_tokens"],
        )
        .int(),
        total: first(node, &["total_tokens", "totalTokenCount"]).int(),
        cached: first(
            node,
            &["cached_tokens", "cachedContentTokenCount", "total_cached_tokens"],
        )
        .int(),
        cache_read: cache_read.int(),
        cache_creation: first(
            node,
            &[
                "cache_creation_tokens",
                "cacheCreationTokens",
                "cache_write_tokens",
                "cacheWriteTokens",
            ],
        )
        .int(),
        ..Detail::default()
    };
    if input.is_none() {
        d.breakdown = Some(invalid(d.total));
        return d;
    }
    if !cache_read.exists() && d.cached > 0 {
        d.cache_read = d.cached;
    }
    if d.total == 0 {
        match safe_sum(&[d.input, d.output, d.reasoning]) {
            Some(total) => d.total = total,
            None => {
                d.total = 0;
                d.breakdown = Some(invalid(0));
                return d;
            }
        }
    }
    d.breakdown = Some(separate_reasoning(
        d.input,
        d.cache_read,
        d.cache_creation,
        d.output,
        d.reasoning,
        d.total,
    ));
    d
}

/// Go `ParseInteractionsUsage`.
pub fn parse_interactions(body: &[u8]) -> Detail {
    let root = gj::parse(body);
    let node = first(
        &root,
        &[
            "usage",
            "total_usage",
            "metadata.total_usage",
            "metadata.usage",
            "usageMetadata",
            "usage_metadata",
            "interaction.usage",
            "interaction.total_usage",
            "interaction.metadata.total_usage",
        ],
    );
    if !node.exists() {
        return Detail::default();
    }
    let mut d = if node.get("promptTokenCount").exists() || node.get("candidatesTokenCount").exists() {
        parse_gemini_node(&node)
    } else {
        parse_interactions_node(&node)
    };
    d.response_service_tier = service_tier(body);
    d
}

/// Go `ParseInteractionsStreamUsage`.
fn parse_interactions_stream(line: &[u8]) -> Option<Detail> {
    let payload = json_payload(line).unwrap_or(line);
    if payload.is_empty() || !gj::valid(payload) {
        return None;
    }
    let d = parse_interactions(payload);
    (has_tokens(&d) || has_billing(&d)).then_some(d)
}

/// The buffered-response parser for a format (Go's executor for that format).
// Q1 (temporary): the per-protocol parsers and `StreamUsage` serve executors that still
// report through `UsageSink::response_*` and dispatch's `Tracker::body`/`event`. Each
// provider unit (P1–P9) removes its executor's calls and D2 removes dispatch's; the last
// of them deletes these parsers.
pub fn parse_body(format: Format, body: &[u8]) -> Detail {
    match format {
        Format::Claude => parse_claude(body),
        Format::OpenAI => parse_openai(body),
        // A buffered Responses body is the completed response object: usage and
        // service_tier are top-level, as `ParseOpenAIUsage` reads them.
        Format::OpenAIResponse | Format::Codex => parse_responses(body),
        Format::Gemini => parse_gemini(body),
        Format::Antigravity => {
            let node = first(
                &gj::parse(body),
                &["response.usageMetadata", "usageMetadata", "usage_metadata"],
            );
            if node.exists() {
                parse_gemini_node(&node)
            } else {
                Detail::default()
            }
        }
        Format::Interactions => parse_interactions(body),
    }
}

/// Keeps the earlier dimensions of every usage member `update` did not report, so the tier,
/// speed and region of `message_start` survive `message_delta`. A reported member replaces all
/// its earlier dimensions, even with none the record can name, as the Anthropic SDK replaces a
/// non-null `server_tool_use` whole.
fn merge_dimensions(update: &mut Detail, existing: &Detail) {
    for (name, value) in &existing.dimensions {
        if usage_snapshot::dimension_member(name).is_none_or(|member| !update.dimension_members.contains(&member)) {
            update.dimensions.push((name, value.clone()));
        }
    }
}

/// Go `StreamUsageBuffer`, fed line by line in one format the way that format's Go
/// executor feeds it.
#[derive(Debug, Default)]
pub struct StreamUsage {
    detail: Detail,
    ok: bool,
}

impl StreamUsage {
    /// Every stream observation lands here. Token counts follow Go's
    /// `StreamUsageBuffer.Observe`: the latest usage wins, and a tier-only update keeps the
    /// counts and replaces the tier. Billing dimensions are kept apart from that rule: they merge
    /// ([`merge_dimensions`]) whether or not the update carries tokens.
    fn observe(&mut self, mut detail: Detail) {
        merge_dimensions(&mut detail, &self.detail);
        let tier = detail.response_service_tier.trim().to_owned();
        if tier.is_empty() || has_tokens(&detail) {
            let preserved = std::mem::take(&mut self.detail.response_service_tier);
            self.detail = detail;
            if self.detail.response_service_tier.is_empty() {
                self.detail.response_service_tier = preserved;
            }
        } else {
            self.detail.response_service_tier = tier;
            self.detail.dimensions = detail.dimensions;
            // No earlier counts to keep: the update's unreported counts stay unreported, so a
            // tiered billed-only terminal writes no zero tokens.
            if !self.ok {
                self.detail.unreported = detail.unreported;
            }
        }
        self.ok = true;
    }

    /// Go `ObserveMergedStreamUsage` with `MergeStreamUsageDetail`.
    fn observe_merged(&mut self, update: Detail) {
        if !self.ok {
            self.observe(update);
            return;
        }
        let existing = &self.detail;
        let mut m = update;
        for (field, old) in [
            (&mut m.input, existing.input),
            (&mut m.cached, existing.cached),
            (&mut m.cache_read, existing.cache_read),
            (&mut m.cache_creation, existing.cache_creation),
            (&mut m.output, existing.output),
            (&mut m.reasoning, existing.reasoning),
        ] {
            if *field == 0 && old > 0 {
                *field = old;
            }
        }
        if m.response_service_tier.is_empty() {
            m.response_service_tier = existing.response_service_tier.clone();
        }
        m.cache_creation_5m = m.cache_creation_5m.or(existing.cache_creation_5m);
        m.cache_creation_1h = m.cache_creation_1h.or(existing.cache_creation_1h);
        let mut cached = m.cache_read + m.cache_creation;
        if cached == 0 {
            cached = m.cached;
        }
        let calculated = m.input + m.output + cached;
        if m.total == 0 || m.total < calculated {
            m.total = calculated;
        }
        let non_reasoning = (m.output - m.reasoning).max(0);
        m.breakdown = Some(independent(
            m.input,
            m.cache_read,
            m.cache_creation,
            non_reasoning,
            m.reasoning,
            m.total,
        ));
        self.observe(m);
    }

    /// Go `ObserveOpenAIStream`.
    fn observe_openai(&mut self, line: &[u8]) {
        let Some(payload) = json_payload(line) else { return };
        let contains = |needle: &[u8]| payload.windows(needle.len()).any(|w| w == needle);
        let usage_candidate = contains(b"\"usage\"");
        let need_tier = self.detail.response_service_tier.is_empty() || usage_candidate;
        let tier_candidate = need_tier && contains(b"\"service_tier\"");
        if (!usage_candidate && !tier_candidate) || !gj::valid(payload) {
            return;
        }
        let mut detail = Detail::default();
        let mut usage_ok = false;
        if usage_candidate {
            let node = gj::get(payload, "usage");
            if has_openai_token_fields(&node) {
                detail = parse_openai_node(&node);
                usage_ok = true;
            }
        }
        if tier_candidate {
            detail.response_service_tier = service_tier_valid(payload);
        }
        if usage_ok || !detail.response_service_tier.is_empty() {
            self.observe(detail);
        }
    }

    /// One client-visible stream line of `format`.
    pub fn line(&mut self, format: Format, line: &[u8]) {
        match format {
            Format::Claude => {
                if let Some(d) = parse_claude_stream(line) {
                    self.observe_merged(d);
                }
            }
            Format::OpenAI => self.observe_openai(line),
            // Go's Codex executor publishes the first terminal event's usage.
            Format::OpenAIResponse | Format::Codex => {
                let Some(payload) = json_payload(line) else { return };
                let kind = gj::get(payload, "type");
                if !matches!(
                    kind.str().as_ref(),
                    "response.completed" | "response.incomplete" | "response.done"
                ) || self.ok
                {
                    return;
                }
                if let Some(d) = parse_codex(payload) {
                    self.observe(d);
                }
            }
            Format::Gemini => {
                if let Some(d) = parse_gemini_stream(line) {
                    self.observe(d);
                }
            }
            Format::Antigravity => {
                if let Some(d) = parse_antigravity_stream(line) {
                    self.observe(d);
                }
            }
            Format::Interactions => {
                if let Some(d) = parse_interactions_stream(line) {
                    self.observe(d);
                }
            }
        }
    }

    /// Go `StreamUsageBuffer.Detail`.
    pub fn detail(&self) -> Option<&Detail> {
        self.ok.then_some(&self.detail)
    }
}

// ---- response model ------------------------------------------------------------

const MAX_RESPONSE_MODEL: usize = 128;

fn string_at(data: &[u8], path: &str) -> Option<String> {
    let r = gj::get(data, path);
    (r.kind == Kind::String).then(|| r.str().trim().to_owned())
}

fn bounded(model: String) -> Option<String> {
    (model.len() <= MAX_RESPONSE_MODEL).then_some(model)
}

fn interactions_terminal(event: &str, status: &str) -> bool {
    matches!(
        event,
        "interaction.completed" | "interaction.done" | "interaction.failed" | "interaction.cancelled"
    ) || matches!(status, "completed" | "incomplete" | "cancelled" | "failed")
}

fn event_type(data: &[u8]) -> String {
    let event = gj::get(data, "event_type").str().into_owned();
    if event.is_empty() {
        gj::get(data, "type").str().into_owned()
    } else {
        event
    }
}

/// Go `extractResponseModelEvent`: the model an upstream frame reports serving and
/// whether the frame ends the response.
fn response_model_event(line: &[u8], provider: &str) -> (String, bool) {
    let Some(data) = json_payload(line) else {
        return (String::new(), false);
    };
    match provider.trim().go_lower().as_str() {
        "codex" => {
            let (carries, terminal) = match gj::get(data, "type").str().trim() {
                "response.created" | "response.in_progress" => (true, false),
                "response.completed" | "response.incomplete" | "response.done" => (true, true),
                _ => (false, false),
            };
            if !carries || !gj::valid(data) {
                return (String::new(), false);
            }
            let model = string_at(data, "response.model").and_then(bounded).unwrap_or_default();
            (model, terminal)
        }
        "claude" => match gj::get(data, "type").str().as_ref() {
            "message_start" => (
                string_at(data, "message.model").and_then(bounded).unwrap_or_default(),
                false,
            ),
            "message_stop" => (String::new(), true),
            "message" => (string_at(data, "model").and_then(bounded).unwrap_or_default(), true),
            _ => {
                if !gj::valid(data) {
                    return (String::new(), false);
                }
                let model = match string_at(data, "message.model") {
                    Some(m) => bounded(m),
                    None => string_at(data, "model").and_then(bounded),
                };
                (model.unwrap_or_default(), false)
            }
        },
        "gemini" | "gemini-interactions" | "vertex" | "aistudio" | "antigravity" => {
            if !gj::valid(data) {
                return (String::new(), false);
            }
            let model = ["response.modelVersion", "modelVersion", "interaction.model", "model"]
                .iter()
                .find_map(|p| string_at(data, p))
                .and_then(bounded)
                .unwrap_or_default();
            let finish = first(
                &gj::parse(data),
                &["candidates.0.finishReason", "response.candidates.0.finishReason"],
            );
            // d9451cb6: Vertex's endpoints/openapi route answers in Chat Completions.
            let terminal = (finish.exists() && !finish.str().is_empty())
                || gj::get(data, "object").str() == "chat.completion"
                || !gj::get(data, "choices.0.finish_reason").str().is_empty()
                || interactions_terminal(&event_type(data), &gj::get(data, "interaction.status").str());
            (model, terminal)
        }
        _ => generic_response_model(data),
    }
}

/// Go `extractGenericResponseModelEvent`.
fn generic_response_model(data: &[u8]) -> (String, bool) {
    if !gj::valid(data) {
        return (String::new(), false);
    }
    if let Some(m) = string_at(data, "response.model").and_then(bounded) {
        let kind = gj::get(data, "type").str().into_owned();
        let terminal = matches!(
            kind.as_str(),
            "response.completed" | "response.done" | "response.incomplete"
        );
        return (m, terminal);
    }
    if let Some(m) = string_at(data, "interaction.model").and_then(bounded) {
        let status = gj::get(data, "interaction.status").str().into_owned();
        return (m, interactions_terminal(&event_type(data), &status));
    }
    for (path, finish) in [
        ("modelVersion", "candidates.0.finishReason"),
        ("response.modelVersion", "response.candidates.0.finishReason"),
    ] {
        if let Some(m) = string_at(data, path).and_then(bounded) {
            let f = gj::get(data, finish);
            return (m, f.exists() && !f.str().is_empty());
        }
    }
    if let Some(m) = string_at(data, "message.model").and_then(bounded) {
        return (m, false);
    }
    if let Some(m) = string_at(data, "model").and_then(bounded) {
        let object = gj::get(data, "object").str().into_owned();
        let finish = gj::get(data, "choices.0.finish_reason").str().into_owned();
        let status = gj::get(data, "status").str().into_owned();
        let terminal =
            object == "chat.completion" || !finish.is_empty() || status == "completed" || status == "incomplete";
        return (m, terminal);
    }
    let kind = event_type(data);
    let status = gj::get(data, "interaction.status").str().into_owned();
    (
        String::new(),
        interactions_terminal(&kind, &status) || kind == "message_stop",
    )
}

/// Go `UsageReporter.ObserveResponseModel` state.
#[derive(Debug, Default)]
pub struct ResponseModel {
    model: String,
    final_: bool,
}

impl ResponseModel {
    pub fn observe(&mut self, line: &[u8], provider: &str) {
        if self.final_ {
            return;
        }
        let (served, terminal) = response_model_event(line, provider);
        if !served.is_empty() {
            self.model = served;
        }
        if terminal {
            self.final_ = true;
        }
    }

    /// Go `SetResponseModel`: trimmed and bounded, unless a terminal event fixed it.
    pub fn set(&mut self, model: &str) {
        let model = model.trim();
        if self.final_ || model.is_empty() || model.len() > MAX_RESPONSE_MODEL {
            return;
        }
        self.model = model.to_owned();
    }

    pub fn get(&self) -> &str {
        &self.model
    }
}

/// Go `normalizeModelName`: lower case without the thinking suffix.
fn normalize_model_name(model: &str) -> String {
    let lower = model.trim().go_lower();
    ck_wire::thinking::parse_suffix(&lower).model_name.trim().to_owned()
}

/// Go `isDatedModelAlias`: `dated` is `base` plus a date or a three-digit version.
fn is_dated_model_alias(base: &str, dated: &str) -> bool {
    let Some(suffix) = dated.strip_prefix(base).and_then(|s| s.strip_prefix('-')) else {
        return false;
    };
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    match suffix.len() {
        10 => {
            let b = suffix.as_bytes();
            b[4] == b'-' && b[7] == b'-' && digits(&suffix[..4]) && digits(&suffix[5..7]) && digits(&suffix[8..])
        }
        8 | 3 => digits(suffix),
        _ => false,
    }
}

/// Go `IsModelSubstituted`: whether the upstream served another model than the
/// requested one; dated aliases, provider prefixes and `-latest` are the same model.
pub fn is_model_substituted(requested: &str, served: &str) -> bool {
    let served = normalize_model_name(served);
    let requested = normalize_model_name(requested);
    if served.is_empty() || requested.is_empty() || requested == served {
        return false;
    }
    let dated = |a: &str, b: &str| is_dated_model_alias(a, b) || is_dated_model_alias(b, a);
    if dated(&requested, &served) {
        return false;
    }
    // Go `stripModelProviderPrefix`: after the last `/`, unless it ends the name.
    let strip = |m: &str| match m.rfind('/') {
        Some(i) if i < m.len() - 1 => m[i + 1..].to_owned(),
        _ => m.to_owned(),
    };
    let (req, srv) = (strip(&requested), strip(&served));
    if req == srv || dated(&req, &srv) {
        return false;
    }
    let (req, srv) = (
        req.strip_suffix("-latest").unwrap_or(&req),
        srv.strip_suffix("-latest").unwrap_or(&srv),
    );
    !(req == srv || dated(req, srv))
}

/// Provider, credential ID, normalized requested and served model.
type SubstitutionKey = (String, String, String, String);

/// Go `codexModelSubstitutionWarns`: one warning per provider, credential and model
/// pair per ten minutes, at most 1024 pairs remembered.
fn substitution_warning_allowed(key: SubstitutionKey) -> bool {
    use std::collections::HashMap;
    use std::time::{Duration, Instant};
    const WINDOW: Duration = Duration::from_secs(600);
    const MAX: usize = 1024;
    static LAST: std::sync::Mutex<Option<HashMap<SubstitutionKey, Instant>>> = std::sync::Mutex::new(None);
    let mut guard = LAST.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let last = guard.get_or_insert_with(HashMap::new);
    let now = Instant::now();
    if last.get(&key).is_some_and(|at| now.duration_since(*at) < WINDOW) {
        return false;
    }
    if last.len() >= MAX {
        last.retain(|_, at| now.duration_since(*at) < WINDOW);
        if last.len() >= MAX {
            last.clear();
        }
    }
    last.insert(key, now);
    true
}

/// Go `warnModelSubstitution`, after an attempt's record is published.
fn warn_model_substitution(record: &Record, upstream_model: &str, auth_id: &str) {
    let served = record.response_model.as_str();
    let expected = if upstream_model.is_empty() {
        record.model.as_str()
    } else {
        upstream_model
    };
    if served.is_empty() || !is_model_substituted(expected, served) {
        return;
    }
    if !record.model.is_empty() && !is_model_substituted(&record.model, served) {
        return;
    }
    let provider = if record.provider.is_empty() {
        "codex"
    } else {
        record.provider.as_str()
    };
    let key = (
        provider.to_owned(),
        auth_id.to_owned(),
        normalize_model_name(expected),
        normalize_model_name(served),
    );
    if !substitution_warning_allowed(key) {
        return;
    }
    let index = if record.auth_index.trim().is_empty() {
        "nil"
    } else {
        record.auth_index.trim()
    };
    tracing::warn!(
        "{provider} executor: upstream served model {} for requested model {} (auth_index={index})",
        ck_wire::gostr::quote(served),
        ck_wire::gostr::quote(&record.model),
    );
}

// ---- the queued record ---------------------------------------------------------

/// Downstream request facts (Go `ClientRequestMetadata`, request ID, endpoint).
#[derive(Debug, Clone, Default)]
pub struct Client {
    pub client_ip: String,
    pub resolved_client_ip: String,
    pub x_forwarded_for: String,
    pub user_agent: String,
    /// The bound canonical session (Go `syncMetadataSessionToContext`).
    pub session_id: String,
    pub parent_session_id: String,
    pub is_fork: bool,
    /// Go `NodeKind` and `IsCompaction`: set only by an LCP pick (`fork`, `compaction`).
    pub node_kind: String,
    pub is_compaction: bool,
    /// Where `session_id` came from (`crate::scheduling::session::Session::source`), or `lcp` for an
    /// LCP pick's session. Not in Go.
    pub session_source: &'static str,
    pub request_id: String,
    /// `METHOD /route/template`.
    pub endpoint: String,
    /// The matched client key (Go gin `userApiKey`).
    pub api_key: String,
    /// The access provider that accepted it (Go gin `accessProvider`, 37fe2608).
    pub access_provider: String,
}

/// One attempt's record before Go's queue normalization (Go `usage.Record` as the
/// executor's reporter builds it).
#[derive(Debug, Clone, Default)]
pub struct Record {
    /// RFC 3339 with nanoseconds, Go `time.Time` JSON.
    pub timestamp: String,
    pub latency_ms: i64,
    pub ttft_ms: i64,
    pub execution_id: String,
    pub provider: String,
    pub executor_type: String,
    pub model: String,
    pub alias: String,
    pub source: String,
    pub auth_index: String,
    pub access_token_sha256: String,
    pub auth_type: String,
    pub reasoning_effort: String,
    pub service_tier: String,
    pub generate: bool,
    pub stream: bool,
    pub failed: bool,
    pub fail_status: i64,
    pub fail_body: String,
    pub detail: Detail,
    pub response_model: String,
    /// Upstream response headers, names as received.
    pub response_headers: Vec<(String, Vec<String>)>,
    /// [`prompt_prefix`] of the client request; empty when it has none. Not in Go.
    pub prompt_prefix_hash: String,
    pub claude_cache: Option<ck_core::exec::ClaudeCacheTelemetry>,
    /// The usage scope of a record that is not the attempt's own (`tool:<model>`, `job:<id>`,
    /// an upstream response); empty for the attempt's. Not in Go.
    pub usage_scope: String,
    /// Present only for additional telemetry-only records that Go omits.
    pub usage_observed: Option<bool>,
    pub timings: Timings,
}

/// Where an attempt's time went, in milliseconds from when the server accepted the
/// client request, and the bytes it moved. Not in Go's record: cooker adds these
/// fields (docs/DIFFERENCES-FROM-GO.md), and leaves out each one it did not see. The
/// upstream stages come from the shared HTTP client ([`ck_core::exec::UpstreamStage`]),
/// which the Claude executor times; other executors leave them out.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Timings {
    /// The client request body was read to its end.
    pub client_body_received_ms: Option<i64>,
    /// The upstream request was about to be sent.
    pub upstream_started_ms: Option<i64>,
    /// The upstream client had a connection and started writing the request.
    pub upstream_connected_ms: Option<i64>,
    /// `true`: the upstream request went out on a pooled connection. Left out when a
    /// connection was dialled while it waited: the client may still have used a pooled
    /// one that came free first, and does not say which.
    pub upstream_connection_reused: Option<bool>,
    /// The upstream client took the last of the request body into its write buffer.
    /// Handed over, not acknowledged: the client's buffer (it takes another piece below
    /// 408 KiB, so under 472 KiB) and the kernel's socket buffer may still hold the end
    /// of the body.
    pub upstream_body_sent_ms: Option<i64>,
    /// The upstream response head arrived.
    pub upstream_headers_ms: Option<i64>,
    /// The first upstream response body bytes arrived.
    pub upstream_first_byte_ms: Option<i64>,
    /// The request's first response bytes, a keep-alive included, were handed to the
    /// client's connection before the record was published: a stream's first event, or
    /// a keep-alive sent while a buffered response waited.
    pub client_first_byte_ms: Option<i64>,
    /// The client request body, as received.
    pub client_request_bytes: Option<i64>,
    /// The upstream request body of the last hop: its size once handed over, 0 for a
    /// hop without one (a redirect followed as a GET).
    pub upstream_request_bytes: Option<i64>,
    /// The upstream response body, as received (before any content decoding).
    pub upstream_response_bytes: Option<i64>,
}

impl Timings {
    /// The fields after Go's: a `timings` object and the byte counts.
    fn write(&self, w: &mut Writer) {
        let stages = [
            ("client_body_received_ms", self.client_body_received_ms),
            ("upstream_started_ms", self.upstream_started_ms),
            ("upstream_connected_ms", self.upstream_connected_ms),
            ("upstream_body_sent_ms", self.upstream_body_sent_ms),
            ("upstream_headers_ms", self.upstream_headers_ms),
            ("upstream_first_byte_ms", self.upstream_first_byte_ms),
            ("client_first_byte_ms", self.client_first_byte_ms),
        ];
        if stages.iter().any(|(_, v)| v.is_some()) || self.upstream_connection_reused.is_some() {
            w.object("timings", |t| {
                for (key, value) in stages {
                    if let Some(value) = value {
                        t.int(key, value);
                    }
                }
                if let Some(reused) = self.upstream_connection_reused {
                    t.bool("upstream_connection_reused", reused);
                }
            });
        }
        for (key, value) in [
            ("client_request_bytes", self.client_request_bytes),
            ("upstream_request_bytes", self.upstream_request_bytes),
            ("upstream_response_bytes", self.upstream_response_bytes),
        ] {
            if let Some(value) = value {
                w.int(key, value);
            }
        }
    }
}

struct Writer(Vec<u8>, bool);

impl Writer {
    fn key(&mut self, k: &str) {
        if self.1 {
            self.0.push(b',');
        }
        self.1 = true;
        ck_wire::json::marshal_str(&mut self.0, k.as_bytes(), true);
        self.0.push(b':');
    }
    fn str(&mut self, k: &str, v: &str) {
        self.key(k);
        ck_wire::json::marshal_str(&mut self.0, v.as_bytes(), true);
    }
    fn opt_str(&mut self, k: &str, v: &str) {
        if !v.is_empty() {
            self.str(k, v);
        }
    }
    fn int(&mut self, k: &str, v: i64) {
        use std::io::Write;
        self.key(k);
        // Straight into the record: no string per number.
        let _ = write!(self.0, "{v}");
    }
    /// `v` is a JSON number literal both usage readers validated.
    fn number(&mut self, k: &str, v: &str) {
        self.key(k);
        self.0.extend_from_slice(v.as_bytes());
    }
    fn bool(&mut self, k: &str, v: bool) {
        self.key(k);
        self.0.extend_from_slice(if v { b"true" } else { b"false" });
    }
    fn object(&mut self, k: &str, f: impl FnOnce(&mut Writer)) {
        self.key(k);
        let mut inner = Writer(std::mem::take(&mut self.0), false);
        inner.0.push(b'{');
        f(&mut inner);
        inner.0.push(b'}');
        self.0 = inner.0;
    }
}

/// Go `isHierarchyParent` is irrelevant here: `HandleUsage` restores the client
/// metadata's parent whenever the reporter dropped it.
fn sessions(client: &Client) -> (String, String) {
    let session = ck_wire::session::normalize_to_canonical_uuid(&client.session_id);
    let mut parent = ck_wire::session::normalize_to_canonical_uuid(&client.parent_session_id);
    if session.is_empty() || session == parent {
        parent.clear();
    }
    (session, parent)
}

/// Go `usage.IsNativeAccessProvider`: CPA's own client-key providers.
pub(crate) fn is_native_access_provider(provider: &str) -> bool {
    use ck_wire::gostr::GoStr;
    let provider = provider.trim();
    provider.go_eq_fold("config-inline") || provider.go_eq_fold("config-api-key")
}

/// The record's `dimensions` object, left out when the usage reported none.
fn write_dimensions(w: &mut Writer, dimensions: &[(&'static str, Dimension)]) {
    if dimensions.is_empty() {
        return;
    }
    w.object("dimensions", |d| {
        for (name, value) in dimensions {
            match value {
                Dimension::Count(n) => d.int(name, i64::try_from(*n).unwrap_or(i64::MAX)),
                Dimension::Decimal(number) => d.number(name, number),
                Dimension::Text(text) => d.str(name, text),
            }
        }
    });
}

pub fn queued(record: &Record, client: &Client) -> Vec<u8> {
    let or = |v: &str, d: &str| -> String {
        let v = v.trim();
        if v.is_empty() { d.to_owned() } else { v.to_owned() }
    };
    let model = or(&record.model, "unknown");
    let alias = or(&record.alias, &model);
    let provider = or(&record.provider, "unknown");
    let executor_type = or(&record.executor_type, "unknown");
    let auth_type = or(&record.auth_type, "unknown");
    let request_id = client.request_id.trim();
    let execution_id = or(&record.execution_id, "");
    let execution_id = if execution_id.is_empty() {
        uuid::Uuid::new_v4().to_string()
    } else {
        execution_id
    };
    let service_tier = or(&record.service_tier, "default");
    let (session, parent) = sessions(client);
    let detail = ensure_breakdown(record.detail.clone(), &record.provider, &record.executor_type);
    let b = detail.breakdown.unwrap_or_else(|| unclassified(0));
    let (fail_status, fail_body) = if record.failed {
        let status = if record.fail_status > 0 {
            record.fail_status
        } else {
            500
        };
        (status, record.fail_body.trim().to_owned())
    } else {
        (200, String::new())
    };
    let mut w = Writer(Vec::with_capacity(1024), false);
    w.0.push(b'{');
    w.str("timestamp", &record.timestamp);
    w.int("latency_ms", record.latency_ms);
    w.int("ttft_ms", record.ttft_ms);
    w.str("source", &record.source);
    w.str("auth_index", &record.auth_index);
    w.opt_str("access_token_sha256", &record.access_token_sha256);
    w.str("client_ip", &client.client_ip);
    w.str("resolved_client_ip", &client.resolved_client_ip);
    w.str("x_forwarded_for", &client.x_forwarded_for);
    w.str("user_agent", &client.user_agent);
    let unreported = detail.unreported;
    w.object("tokens", |t| {
        for (name, value, absent) in [
            ("input_tokens", detail.input, unreported.input),
            ("output_tokens", detail.output, unreported.output),
            ("reasoning_tokens", detail.reasoning, unreported.reasoning),
            ("cached_tokens", detail.cached, unreported.cache_read),
            ("cache_read_tokens", detail.cache_read, unreported.cache_read),
        ] {
            if !absent {
                t.int(name, value);
            }
        }
        t.bool("cache_read_tokens_present", !unreported.cache_read);
        if !unreported.cache_creation {
            t.int("cache_creation_tokens", detail.cache_creation);
        }
        for (name, value) in [
            ("cache_creation_5m_tokens", detail.cache_creation_5m),
            ("cache_creation_1h_tokens", detail.cache_creation_1h),
            ("cache_creation_30m_tokens", detail.cache_creation_30m),
            ("cache_creation_12h_tokens", detail.cache_creation_12h),
        ] {
            if let Some(n) = value {
                t.int(name, n);
            }
        }
        if !unreported.total {
            t.int("total_tokens", detail.total);
        }
    });
    write_dimensions(&mut w, &detail.dimensions);
    w.bool("failed", record.failed);
    if let Some(observed) = record.usage_observed {
        w.bool("usage_observed", observed);
    }
    w.bool("generate", record.generate);
    w.bool("stream", record.stream);
    w.object("fail", |f| {
        f.int("status_code", fail_status);
        f.str("body", &fail_body);
    });
    if !record.response_headers.is_empty() {
        let mut headers = record.response_headers.clone();
        headers.sort_by(|a, b| a.0.cmp(&b.0));
        w.object("response_headers", |h| {
            for (name, values) in &headers {
                h.key(name);
                h.0.push(b'[');
                for (i, v) in values.iter().enumerate() {
                    if i > 0 {
                        h.0.push(b',');
                    }
                    ck_wire::json::marshal_str(&mut h.0, v.as_bytes(), true);
                }
                h.0.push(b']');
            }
        });
    }
    w.int("accounting_version", 2);
    w.object("token_breakdown", |t| {
        t.int("schema_version", 2);
        t.str(
            "quality",
            match b.quality {
                Quality::Complete => "complete",
                Quality::Inconsistent => "inconsistent",
                Quality::Unclassified => "unclassified",
            },
        );
        t.int("total_tokens", b.total);
        t.object("input", |i| {
            i.int("total_tokens", b.input[0]);
            i.int("uncached_tokens", b.input[1]);
            i.int("cache_read_tokens", b.input[2]);
            i.int("cache_write_tokens", b.input[3]);
        });
        t.object("output", |o| {
            o.int("total_tokens", b.output[0]);
            o.int("non_reasoning_tokens", b.output[1]);
            o.int("reasoning_tokens", b.output[2]);
        });
        t.int("unclassified_tokens", b.unclassified);
    });
    w.str("provider", &provider);
    w.str("executor_type", &executor_type);
    w.str("model", &model);
    w.str("alias", &alias);
    w.str("endpoint", client.endpoint.trim());
    w.str("auth_type", &auth_type);
    w.str("api_key", client.api_key.trim());
    w.bool("is_native_key", is_native_access_provider(&client.access_provider));
    w.opt_str("access_provider", client.access_provider.trim());
    w.str("request_id", request_id);
    w.opt_str("execution_id", &execution_id);
    w.opt_str("trace_id", request_id);
    w.opt_str("session_id", &session);
    w.opt_str("parent_session_id", &parent);
    w.opt_str("node_kind", client.node_kind.trim());
    if client.is_fork {
        w.bool("is_fork", true);
    }
    if client.is_compaction {
        w.bool("is_compaction", true);
    }
    w.str("reasoning_effort", record.reasoning_effort.trim());
    w.str("service_tier", &service_tier);
    w.opt_str("response_service_tier", detail.response_service_tier.trim());
    w.opt_str("response_model", record.response_model.trim());
    // Not in Go's record (docs/DIFFERENCES-FROM-GO.md).
    w.opt_str("session_source", client.session_source);
    w.opt_str("prompt_prefix_hash", &record.prompt_prefix_hash);
    w.opt_str("usage_scope", &record.usage_scope);
    if let Some(cache) = &record.claude_cache {
        w.key("claude_cache");
        serde_json::to_writer(&mut w.0, cache).expect("cache telemetry contains only JSON primitives");
    }
    record.timings.write(&mut w);
    w.0.push(b'}');
    w.0
}

// ---- per-attempt tracking ------------------------------------------------------

/// Request-level facts shared by every attempt of one request (Go's handler context:
/// client metadata, requested alias, reasoning effort, service tier, generate, stream).
#[derive(Debug, Clone)]
pub struct Facts {
    pub client: Client,
    /// Client response format, for parsing the client-visible response.
    pub format: Format,
    pub alias: String,
    pub reasoning_effort: String,
    pub service_tier: String,
    pub generate: bool,
    pub stream: bool,
    /// When the request arrived: the origin of every attempt's [`Timings`].
    pub clock: std::sync::Arc<crate::observability::api::Clock>,
    /// [`prompt_prefix`] of the client body.
    pub prompt_prefix_hash: String,
    /// Shared by failover/refresh attempts, independent of credential changes.
    pub attempts: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl Facts {
    /// Go `setReasoningEffortMetadata`, `setServiceTierMetadata` and
    /// `setGenerateMetadata` over the client body; the alias is the client's model. The
    /// request's clock and prompt prefix come from its `trace`.
    pub fn new(
        client: Client,
        entry: Format,
        response: Format,
        model: &str,
        body: &[u8],
        stream: bool,
        trace: &crate::inference::dispatch::Trace,
    ) -> Self {
        let tier = gj::get(body, "service_tier");
        let tier = tier.str().trim().to_owned();
        let generate = gj::get(body, "generate");
        Self {
            client,
            format: response,
            alias: model.trim().to_owned(),
            reasoning_effort: ck_wire::thinking::extract_reasoning_effort(body, entry.as_str(), model),
            service_tier: if tier.is_empty() { "auto".into() } else { tier },
            generate: !(generate.is_bool() && !generate.bool()),
            stream,
            clock: trace.clock(),
            prompt_prefix_hash: trace.prompt_prefix(entry, body),
            attempts: Default::default(),
        }
    }
}

/// A fingerprint of the client's system prompt and tool definitions as it sent them
/// (before translation or cloaking, `cache_control` markers included): the first 64
/// bits of a SHA-256 over the raw `tools` and system values, in hex. Requests with the
/// same value share their cacheable prefix up to the messages; a different mode or a
/// subagent shows as another value. Nothing of the text itself is kept. Empty when the
/// body has neither value, or for a format whose system prompt sits among its messages.
/// Cost: one walk over the body's top-level members (large values are skipped by
/// bracket matching; nothing is decoded or copied), SHA-256 over the two values, and
/// one 16-byte string.
// ponytail: Chat Completions, Antigravity and Interactions bodies get none (their system
// prompt is a message or sits in an envelope); add a lookup when a client of theirs
// needs one.
// Q1 (temporary): D2 switches dispatch to `usage_snapshot::prompt_prefix` over the request
// view and deletes this.
pub(crate) fn prompt_prefix(format: Format, body: &[u8]) -> String {
    use sha2::Digest;
    let system: &[&[u8]] = match format {
        Format::Claude => &[b"system"],
        Format::OpenAIResponse | Format::Codex => &[b"instructions"],
        Format::Gemini => &[b"systemInstruction", b"system_instruction"],
        _ => return String::new(),
    };
    let (mut tools, mut instructions) = (None, None);
    gj::object_members(body, |key, escaped, value| {
        if !escaped && key == b"tools" {
            tools.get_or_insert(value);
        } else if !escaped && system.contains(&key) {
            instructions.get_or_insert(value);
        }
        tools.is_none() || instructions.is_none()
    });
    if tools.is_none() && instructions.is_none() {
        return String::new();
    }
    let mut hash = sha2::Sha256::new();
    hash.update(b"cpa:prompt-prefix:v1\0");
    for value in [tools, instructions] {
        // Length first, so the boundary between the two values is unambiguous.
        let value = value.unwrap_or_default();
        hash.update((value.len() as u64).to_le_bytes());
        hash.update(value);
    }
    let digest = hash.finalize();
    format!("{:016x}", u64::from_be_bytes(digest[..8].try_into().expect("8 bytes")))
}

/// Go executor identity for a credential's provider: `Identifier()` and the
/// executor's type name (both reach the record and pick the token semantics).
// ponytail: Codex attempts over the upstream WebSocket report `CodexExecutor`; Go
// names them `CodexWebsocketsExecutor`.
fn executor_identity(c: &ck_core::credential::Credential) -> (String, &'static str) {
    let provider = c.provider.trim().go_lower();
    let executor = match provider.as_str() {
        "claude" => "ClaudeExecutor",
        "codex" => "CodexExecutor",
        "gemini" | "gemini-interactions" => "GeminiExecutor",
        "vertex" => "GeminiVertexExecutor",
        "aistudio" => "AIStudioExecutor",
        "antigravity" => "AntigravityExecutor",
        "kimi" => "KimiExecutor",
        "meta" => "MetaExecutor",
        "xai" => "XAIExecutor",
        "devin" => "DevinExecutor",
        _ => {
            return (ck_core::registry::dynamic::provider_key(c), "OpenAICompatExecutor");
        }
    };
    (provider, executor)
}

/// Go's TTFT marks (`StartResponseTTFT`, `MarkFirstResponseByte`,
/// `ObserveTokenEvent`), taken only when the executor reports them.
#[derive(Default)]
struct Ttft {
    /// The executor reports marks; the server's own approximation is not used.
    tracked: bool,
    start: Option<std::time::Instant>,
    ttft: Option<std::time::Duration>,
    first_packet: Option<std::time::Duration>,
}

impl Ttft {
    fn start(&mut self) {
        self.tracked = true;
        if self.ttft.is_none() && self.start.is_none() {
            self.start = Some(std::time::Instant::now());
        }
    }

    fn first_byte(&mut self) {
        self.tracked = true;
        if self.ttft.is_none()
            && let Some(start) = self.start.take()
        {
            self.ttft = Some(start.elapsed());
        }
    }

    fn token(&mut self, is_token: bool) {
        self.tracked = true;
        let Some(start) = self.start.filter(|_| self.ttft.is_none()) else {
            return;
        };
        if !is_token && self.first_packet.is_some() {
            return;
        }
        let elapsed = start.elapsed();
        self.first_packet.get_or_insert(elapsed);
        if is_token {
            self.ttft = Some(elapsed);
            self.start = None;
        }
    }

    /// Go `ttftDuration`: TTFT, else the first packet, else zero.
    fn get(&self) -> std::time::Duration {
        self.ttft.or(self.first_packet).unwrap_or_default()
    }
}

/// An attempt outcome the executor published itself (Go `Publish`/`PublishFailure`),
/// with what Go's `buildRecord` reads at that moment.
struct Outcome {
    failed: bool,
    status: i64,
    body: String,
    detail: Detail,
    model: String,
    latency: std::time::Duration,
    ttft: std::time::Duration,
}

/// The upstream exchange's stages as the shared HTTP client reported them. Apart from
/// its start, they describe the last hop sent: a new hop starts over, and what an
/// earlier hop reports late is dropped.
#[derive(Default)]
struct Exchange {
    started: Option<std::time::Instant>,
    hop: u32,
    connected: Option<std::time::Instant>,
    body_sent: Option<std::time::Instant>,
    headers: Option<std::time::Instant>,
    first_byte: Option<std::time::Instant>,
    /// The request went out on a pooled connection; unknown after a dial.
    reused: Option<bool>,
    request_bytes: Option<u64>,
    response_bytes: Option<u64>,
}

impl Exchange {
    fn mark(&mut self, stage: ck_core::exec::UpstreamStage, now: std::time::Instant) {
        use ck_core::exec::UpstreamStage;
        match stage {
            UpstreamStage::Started => {
                self.started.get_or_insert(now);
            }
            UpstreamStage::Hop { hop, body } => {
                *self = Self {
                    started: self.started,
                    hop,
                    // A hop without a body sends none; one with a body says how much
                    // once it is handed over.
                    request_bytes: (!body).then_some(0),
                    ..Self::default()
                };
            }
            UpstreamStage::Connected { hop } if hop == self.hop => self.connected = Some(now),
            UpstreamStage::BodySent { hop, bytes } if hop == self.hop => {
                self.body_sent = Some(now);
                self.request_bytes = Some(bytes);
            }
            // An earlier hop's body, taken after its redirect arrived.
            UpstreamStage::Connected { .. } | UpstreamStage::BodySent { .. } => {}
            UpstreamStage::Headers { dialled } => {
                self.headers = Some(now);
                self.reused = (!dialled).then_some(true);
            }
            UpstreamStage::FirstByte => self.first_byte = Some(now),
            UpstreamStage::BodyRead { bytes } => self.response_bytes = Some(bytes),
        }
    }
}

/// What an executor reported through `ExecRequest::usage`.
#[derive(Default)]
struct Reported {
    seen: bool,
    body: Option<Detail>,
    stream: StreamUsage,
    model: ResponseModel,
    effort: Option<String>,
    upstream_model: String,
    ttft: Ttft,
    usage_required: bool,
    outcome: Option<Outcome>,
    /// Go created no reporter for this attempt: publish nothing.
    discarded: bool,
    exchange: Exchange,
    claude_cache: Option<ck_core::exec::ClaudeCacheTelemetry>,
    /// The executor type and model of the built-in executor the attempt was handed to.
    delegated: Option<(String, String)>,
}

impl Reported {
    /// The usage reported so far: a body, else the stream's last usage.
    fn usage(&self) -> Option<Detail> {
        self.body.clone().or_else(|| self.stream.detail().cloned())
    }
}

/// The executor-facing observer; `provider` picks the response-model extractor.
struct Observer {
    provider: String,
    started: std::time::Instant,
    cache_telemetry: bool,
    state: std::sync::Mutex<Reported>,
}

impl Observer {
    fn lock(&self) -> std::sync::MutexGuard<'_, Reported> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// `keep_usage`: a failure that keeps the usage reported so far.
    fn settle(&self, failed: bool, keep_usage: bool, status: i64, body: &str) {
        let mut r = self.lock();
        if r.outcome.is_some() {
            return;
        }
        let detail = if failed && !keep_usage {
            Detail::default()
        } else {
            r.usage().unwrap_or_default()
        };
        r.outcome = Some(Outcome {
            failed,
            status,
            body: body.trim().to_owned(),
            detail,
            model: r.model.get().to_owned(),
            latency: self.started.elapsed(),
            ttft: r.ttft.get(),
        });
    }
}

impl ck_core::exec::UsageObserver for Observer {
    fn wants_claude_cache(&self) -> bool {
        self.cache_telemetry
    }

    fn claude_cache(&self, telemetry: ck_core::exec::ClaudeCacheTelemetry) {
        self.lock().claude_cache = Some(telemetry);
    }

    fn response_body(&self, format: Format, body: &[u8]) {
        let mut r = self.lock();
        r.seen = true;
        r.body = Some(parse_body(format, body));
        r.model.observe(body, &self.provider);
    }

    fn response_line(&self, format: Format, line: &[u8]) {
        let mut r = self.lock();
        r.seen = true;
        r.stream.line(format, line);
        r.model.observe(line, &self.provider);
    }

    fn response_usage_line(&self, format: Format, line: &[u8]) {
        let mut r = self.lock();
        r.seen = true;
        r.stream.line(format, line);
    }

    fn delegated_executor(&self, executor_type: &str, model: &str) {
        self.lock().delegated = Some((executor_type.to_owned(), model.to_owned()));
    }

    fn request(&self, format: Format, payload: &[u8]) {
        self.request_for(format.as_str(), payload);
    }

    fn request_for(&self, identifier: &str, payload: &[u8]) {
        self.lock().effort = Some(ck_wire::thinking::extract_translated_reasoning_effort(
            payload, identifier,
        ));
    }

    fn upstream_model(&self, model: &str) {
        self.lock().upstream_model = model.trim().to_owned();
    }

    fn response_model(&self, model: &str) {
        self.lock().model.set(model);
    }

    fn response_model_line(&self, line: &[u8]) {
        self.lock().model.observe(line, &self.provider);
    }

    fn round_trip_started(&self) {
        self.lock().ttft.start();
    }

    fn first_byte(&self) {
        self.lock().ttft.first_byte();
    }

    fn token_event(&self, is_token: bool) {
        self.lock().ttft.token(is_token);
    }

    fn publish(&self) {
        self.settle(false, false, 0, "");
    }

    fn publish_failure(&self, status: u16, body: &str) {
        self.settle(true, false, i64::from(status), body);
    }

    fn publish_failure_with_usage(&self, status: u16, body: &str) {
        self.settle(true, true, i64::from(status), body);
    }

    fn usage_required(&self) {
        self.lock().usage_required = true;
    }

    fn discard(&self) {
        self.lock().discarded = true;
    }

    fn upstream_stage(&self, stage: ck_core::exec::UpstreamStage) {
        let now = std::time::Instant::now();
        self.lock().exchange.mark(stage, now);
    }
}

/// How the attempt ended for the server.
enum Ending<'a> {
    /// Success, or a stream the client dropped (Go's deferred publish).
    Done,
    Failed(&'a ck_core::exec::ExecError),
}

/// One upstream attempt's record in progress. It publishes at most once: what the
/// executor published itself, else on success, on failure, or when dropped
/// mid-stream (Go's deferred publish).
pub struct Tracker {
    queue: std::sync::Arc<crate::Runtime>,
    facts: std::sync::Arc<Facts>,
    record: Record,
    auth_id: String,
    started: std::time::Instant,
    observer: std::sync::Arc<Observer>,
    stream: StreamUsage,
    body: Option<Detail>,
    /// The attempt's canonical usage ([`Tracker::canonical`]). It is the record's usage on
    /// success and on failure alike, whatever the provider.
    canonical: Option<Detail>,
    /// The last usage of each named scope ([`Tracker::usage`]), each its own record.
    scoped: Vec<(String, Detail)>,
    model: ResponseModel,
    first: Option<std::time::Duration>,
    published: bool,
    /// The request's client facts under this attempt's LCP pick's session, when it has
    /// its own.
    client: Option<Client>,
    /// Go `requestedAt`.
    requested_at: chrono::DateTime<chrono::FixedOffset>,
    attempt: usize,
}

impl Tracker {
    /// The attempt's session is its LCP pick's (Go `syncMetadataSessionToContext`), not
    /// the request's.
    pub fn lcp_session(&mut self, m: &crate::scheduling::lcp::Match) {
        let bound = ck_wire::session::bound_session_identity;
        let mut client = self.facts.client.clone();
        client.session_id = bound(&m.session);
        client.parent_session_id = if m.parent.is_empty() {
            String::new()
        } else {
            bound(&m.parent)
        };
        let (node_kind, fork, compaction) = m.node();
        client.node_kind = node_kind.into();
        client.is_fork = fork;
        client.is_compaction = compaction;
        client.session_source = "lcp";
        self.client = Some(client);
    }

    /// Starts the record for one attempt of `credential` on `upstream_model`.
    pub fn start(
        rt: &std::sync::Arc<crate::Runtime>,
        facts: &std::sync::Arc<Facts>,
        credential: &ck_core::credential::Credential,
        upstream_model: &str,
    ) -> Self {
        let (provider, executor_type) = executor_identity(credential);
        let now = chrono::Local::now();
        let record = Record {
            timestamp: go_timestamp(&now),
            execution_id: uuid::Uuid::new_v4().to_string(),
            executor_type: executor_type.to_owned(),
            model: ck_wire::thinking::parse_suffix(upstream_model).model_name,
            alias: facts.alias.clone(),
            source: source(credential, &facts.client.api_key),
            auth_index: ck_core::config::credentials::auth_index(credential),
            access_token_sha256: access_token_sha256(credential),
            auth_type: ck_core::registry::dynamic::auth_kind(credential)
                .unwrap_or_default()
                .to_owned(),
            reasoning_effort: facts.reasoning_effort.clone(),
            service_tier: facts.service_tier.clone(),
            generate: facts.generate,
            stream: facts.stream,
            provider: provider.clone(),
            ..Record::default()
        };
        let started = std::time::Instant::now();
        Self {
            queue: rt.clone(),
            facts: facts.clone(),
            record,
            auth_id: credential.id.clone(),
            started,
            observer: std::sync::Arc::new(Observer {
                provider,
                started,
                cache_telemetry: rt.usage_queue().wants_claude_cache(),
                state: std::sync::Mutex::default(),
            }),
            stream: StreamUsage::default(),
            body: None,
            canonical: None,
            scoped: Vec::new(),
            model: ResponseModel::default(),
            first: None,
            published: false,
            client: None,
            requested_at: now.fixed_offset(),
            attempt: facts.attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        }
    }

    /// The sink to hand the executor.
    pub fn sink(&self) -> ck_core::exec::UsageSink {
        ck_core::exec::UsageSink::new(self.observer.clone())
    }

    /// The upstream answered; its headers are the record's `response_headers`.
    // ponytail: without the executor's TTFT marks (`UsageSink::round_trip_started` and
    // `first_byte`), TTFT is measured to the executor's response (buffered) or first
    // event (streams); Go measures from the upstream request to its first body byte.
    pub fn arrived(&mut self, headers: &axum::http::HeaderMap) {
        self.first.get_or_insert_with(|| self.started.elapsed());
        self.record.response_headers = go_headers(headers);
    }

    /// The buffered client-format body (or the joined events of a stream the client
    /// did not ask to stream).
    // Q1 (temporary): D2 removes `body` and `event` with dispatch's byte-level accounting.
    pub fn body(&mut self, body: &[u8]) {
        if body.trim_ascii_start().first() == Some(&b'{') {
            self.body = Some(parse_body(self.facts.format, body));
            self.model.observe(body, &self.observer.provider);
        } else {
            for line in body.split(|b| *b == b'\n') {
                self.line(line);
            }
        }
    }

    /// One client-format stream event.
    pub fn event(&mut self, event: &[u8]) {
        self.first.get_or_insert_with(|| self.started.elapsed());
        for line in event.split(|b| *b == b'\n') {
            self.line(line);
        }
    }

    fn line(&mut self, line: &[u8]) {
        if line.trim_ascii().is_empty() {
            return;
        }
        self.stream.line(self.facts.format, line);
        self.model.observe(line, &self.observer.provider);
    }

    /// The attempt's canonical usage and response facts (`usage_snapshot::read`); they
    /// replace what the legacy parsers saw. Without a snapshot every count is unreported.
    pub fn canonical(&mut self, usage: Option<&CanonicalUsage>, facts: &ResponseFacts) {
        self.canonical = Some(Detail::canonical(usage.unwrap_or(&CanonicalUsage::default()), facts));
        if let Some(model) = &facts.model {
            self.model.set(model);
        }
    }

    /// The attempt's usage by scope (`usage_snapshot::read`): the attempt's own is its record's
    /// ([`Tracker::canonical`]), and each named scope's is a record of its own.
    pub fn usage(&mut self, usage: &usage_snapshot::Usage, facts: &ResponseFacts) {
        self.canonical(usage.attempt(), facts);
        self.scoped = usage
            .scoped()
            .map(|(scope, usage)| (scope.to_owned(), Detail::canonical(usage, facts)))
            .collect();
    }

    pub fn succeed(mut self) {
        self.publish(Ending::Done);
    }

    /// Go `PublishFailure`: the error's status and body without usage. Claude's
    /// executor fails through its stream buffer (`StreamUsageBuffer.PublishFailure`),
    /// which keeps the usage seen so far.
    pub fn fail(mut self, error: &ck_core::exec::ExecError) {
        self.publish(Ending::Failed(error));
    }

    fn publish(&mut self, ending: Ending<'_>) {
        if std::mem::replace(&mut self.published, true) {
            return;
        }
        let rt = self.queue.clone();
        let queue = rt.usage_queue();
        if !queue.accepts() {
            return;
        }
        let mut reported = std::mem::take(&mut *self.observer.lock());
        if reported.discarded {
            return;
        }
        // Go can publish a tokenless failure after observing a Claude response.
        // Keep the returned TTL buckets as observations, not inferred accounting.
        let returned_cache = reported.usage().map(|d| (d.cache_creation_5m, d.cache_creation_1h));
        let mut record = std::mem::take(&mut self.record);
        if let Some((executor_type, model)) = reported.delegated.clone() {
            (record.executor_type, record.model) = (executor_type, model);
        }
        let (latency, ttft) = match reported.outcome.take() {
            Some(outcome) => {
                record.failed = outcome.failed;
                record.fail_status = outcome.status;
                record.fail_body = outcome.body;
                record.detail = outcome.detail;
                record.response_model = outcome.model;
                (outcome.latency, outcome.ttft)
            }
            None => match self.observed(&reported, ending, &mut record) {
                Some(times) => times,
                None => return,
            },
        };
        record.latency_ms = latency.as_millis() as i64;
        record.ttft_ms = ttft.as_millis() as i64;
        if let Some((five, hour)) = returned_cache {
            record.detail.cache_creation_5m = five;
            record.detail.cache_creation_1h = hour;
        }
        // Canonical usage, once recorded, is the attempt's usage over every executor outcome
        // and legacy observation; failure still sets the status and body above.
        if let Some(canonical) = self.canonical.take() {
            record.detail = canonical;
        }
        record.timings = self.timings(&reported.exchange);
        record.prompt_prefix_hash = self.facts.prompt_prefix_hash.clone();
        record.claude_cache = reported
            .claude_cache
            .map(|cache| self.claude_cache(cache, &reported.exchange, record.latency_ms));
        if let Some(effort) = reported.effort {
            record.reasoning_effort = effort;
        }
        let client = self.client.as_ref().unwrap_or(&self.facts.client);
        if queue.accepts() {
            queue.enqueue(queued(&record, client));
        }
        // One record per named scope beside the attempt's (E-Q13). A video job's scope is published
        // only by the poll its job table charged (`ck_exec` `VideoJobs`), so it is recorded as is.
        for (scope, detail) in std::mem::take(&mut self.scoped) {
            let scoped = Record {
                detail,
                usage_scope: scope,
                claude_cache: None,
                ..record.clone()
            };
            if queue.accepts() {
                queue.enqueue(queued(&scoped, client));
            }
        }
        warn_model_substitution(&record, &reported.upstream_model, &self.auth_id);
    }
}

impl Tracker {
    /// The record's usage, failure and model from what the attempt observed, when the
    /// executor published no outcome of its own; its latency and TTFT. `None` publishes
    /// nothing: a stream that had to report usage ended without it.
    fn observed(
        &mut self,
        reported: &Reported,
        ending: Ending<'_>,
        record: &mut Record,
    ) -> Option<(std::time::Duration, std::time::Duration)> {
        let usage = if reported.seen {
            reported.usage()
        } else {
            self.body.take().or_else(|| self.stream.detail().cloned())
        };
        record.detail = match ending {
            Ending::Done if reported.usage_required && usage.is_none() && self.canonical.is_none() => {
                reported.claude_cache.as_ref()?;
                // Keep activity and timestamps even when a Claude stream never
                // returns usage.
                record.usage_observed = Some(false);
                Detail::default()
            }
            Ending::Done => usage.unwrap_or_default(),
            Ending::Failed(error) => {
                record.failed = true;
                record.fail_status = i64::from(crate::scheduling::classify::go_status(error));
                record.fail_body = crate::scheduling::classify::error_text(error);
                if record.response_headers.is_empty() {
                    record.response_headers = go_headers(&error.headers);
                }
                if self.observer.provider == "claude" {
                    usage.unwrap_or_default()
                } else {
                    Detail::default()
                }
            }
        };
        record.response_model = if reported.seen || !reported.model.get().is_empty() {
            reported.model.get().to_owned()
        } else {
            self.model.get().to_owned()
        };
        let ttft = if reported.ttft.tracked {
            reported.ttft.get()
        } else {
            self.first.unwrap_or_default()
        };
        Some((self.started.elapsed(), ttft))
    }

    /// Claude cache telemetry with this attempt's wall-clock marks, number and request type.
    fn claude_cache(
        &self,
        mut cache: ck_core::exec::ClaudeCacheTelemetry,
        exchange: &Exchange,
        latency_ms: i64,
    ) -> ck_core::exec::ClaudeCacheTelemetry {
        let at = |instant: Option<std::time::Instant>| {
            instant.map(|instant| {
                let offset = instant.saturating_duration_since(self.started).as_millis() as i64;
                self.requested_at.timestamp_millis().saturating_add(offset)
            })
        };
        cache.request_start_unix_ms = at(exchange.started);
        cache.first_response_byte_unix_ms = at(exchange.first_byte);
        // Completion of this upstream attempt, not the downstream socket flush.
        cache.end_unix_ms = Some(self.requested_at.timestamp_millis().saturating_add(latency_ms));
        cache.attempt = self.attempt;
        if self.client.as_ref().unwrap_or(&self.facts.client).is_compaction {
            cache.base_request_type = "compaction".into();
            if cache.request_type != "retry" {
                cache.request_type = "compaction".into();
            }
        }
        if self.attempt > 0 {
            cache.request_type = "retry".into();
        }
        cache
    }

    /// The record's [`Timings`]: each mark as an offset from the request's arrival.
    fn timings(&self, exchange: &Exchange) -> Timings {
        let clock = &self.facts.clock;
        let accepted = clock.accepted();
        let ms = |at: Option<std::time::Instant>| {
            at.map(|at| i64::try_from(at.saturating_duration_since(accepted).as_millis()).unwrap_or(i64::MAX))
        };
        let bytes = |n: Option<u64>| n.map(|n| i64::try_from(n).unwrap_or(i64::MAX));
        let body = clock.body_received();
        Timings {
            client_body_received_ms: ms(body.map(|(at, _)| at)),
            upstream_started_ms: ms(exchange.started),
            upstream_connected_ms: ms(exchange.connected),
            upstream_connection_reused: exchange.reused,
            upstream_body_sent_ms: ms(exchange.body_sent),
            upstream_headers_ms: ms(exchange.headers),
            upstream_first_byte_ms: ms(exchange.first_byte),
            client_first_byte_ms: ms(clock.response_started()),
            client_request_bytes: bytes(body.map(|(_, n)| n)),
            upstream_request_bytes: bytes(exchange.request_bytes),
            upstream_response_bytes: bytes(exchange.response_bytes),
        }
    }
}

impl Drop for Tracker {
    fn drop(&mut self) {
        self.publish(Ending::Done);
    }
}

/// Go's `http.Header` view of upstream headers: canonical names, values in order.
fn go_headers(headers: &axum::http::HeaderMap) -> Vec<(String, Vec<String>)> {
    let mut out: Vec<(String, Vec<String>)> = Vec::new();
    for name in headers.keys() {
        let values = headers
            .get_all(name)
            .iter()
            .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
            .collect();
        out.push((canonical_header(name.as_str()), values));
    }
    out
}

/// Go `textproto.CanonicalMIMEHeaderKey` for a valid token: upper-case the first
/// letter and each letter after `-`, lower-case the rest.
fn canonical_header(name: &str) -> String {
    let mut upper = true;
    name.chars()
        .map(|c| {
            let out = if upper {
                c.to_ascii_uppercase()
            } else {
                c.to_ascii_lowercase()
            };
            upper = c == '-';
            out
        })
        .collect()
}

/// Go `resolveUsageSource`: a Vertex project, the account (OAuth email or API key),
/// the metadata email or `api_key` attribute, else the client key.
pub fn source(c: &ck_core::credential::Credential, client_key: &str) -> String {
    let meta = |k: &str| c.str(k).map(str::trim).filter(|v| !v.is_empty());
    let attr = |k: &str| c.attributes.get(k).map(|v| v.trim()).filter(|v| !v.is_empty());
    if c.provider.trim().go_eq_fold("vertex")
        && let Some(project) = meta("project_id").or_else(|| meta("project"))
    {
        return project.to_owned();
    }
    let (_, account) = ck_core::registry::dynamic::account_info(c);
    Some(account.as_str())
        .filter(|a| !a.is_empty())
        .or_else(|| meta("email"))
        .or_else(|| attr("api_key"))
        .unwrap_or(client_key.trim())
        .to_owned()
}

/// Go `AccessTokenSHA256`.
pub fn access_token_sha256(c: &ck_core::credential::Credential) -> String {
    ck_core::config::credentials::access_token_sha256(c)
}

/// Go `time.Time.MarshalJSON` (RFC 3339 with trailing-zero-trimmed nanoseconds).
pub fn go_timestamp<Tz: chrono::TimeZone>(t: &chrono::DateTime<Tz>) -> String
where
    Tz::Offset: std::fmt::Display,
{
    use chrono::{Offset, Timelike};
    let mut out = t.format("%Y-%m-%dT%H:%M:%S").to_string();
    let nanos = t.nanosecond() % 1_000_000_000;
    if nanos != 0 {
        let frac = format!("{nanos:09}");
        out.push('.');
        out.push_str(frac.trim_end_matches('0'));
    }
    let offset = t.offset().fix().local_minus_utc();
    if offset == 0 {
        out.push('Z');
    } else {
        let sign = if offset < 0 { '-' } else { '+' };
        let abs = offset.abs();
        out.push_str(&format!("{sign}{:02}:{:02}", abs / 3600, (abs % 3600) / 60));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    const FIXTURE: &str = include_str!("../../tests/fixtures/server_go.json");

    /// Go `resolveUsageSource` after d9451cb6: without a project, a Vertex service account
    /// is named by `AccountInfo`, its trimmed `client_email`, not by the client key.
    #[test]
    fn a_service_account_without_a_project_is_its_own_usage_source() {
        let metadata = serde_json::json!({
            "type": "vertex",
            "service_account": {"client_email": " sa@p.iam.gserviceaccount.com "},
        });
        let credential = ck_core::credential::Credential::from_file(
            std::path::Path::new("/auth"),
            std::path::Path::new("/auth/vertex-sa.json"),
            metadata.as_object().unwrap().clone(),
        )
        .unwrap();
        assert_eq!(source(&credential, "client-key"), "sa@p.iam.gserviceaccount.com");
    }

    #[test]
    fn go_timestamps() {
        use chrono::TimeZone;
        let t = chrono::Utc.with_ymd_and_hms(2026, 10, 3, 8, 0, 0).unwrap();
        assert_eq!(go_timestamp(&t), "2026-10-03T08:00:00Z");
        let t = t + chrono::Duration::nanoseconds(123_456_000);
        assert_eq!(go_timestamp(&t), "2026-10-03T08:00:00.123456Z");
        let east = chrono::FixedOffset::east_opt(5 * 3600 + 1800).unwrap();
        assert_eq!(
            go_timestamp(&t.with_timezone(&east)),
            "2026-10-03T13:30:00.123456+05:30"
        );
    }

    /// Claude's executor-published outcomes through the queue: an apply_patch
    /// rejection is Go's `reporter.PublishFailure(err)` after usage was observed (no
    /// tokens, response model kept, and the later failure the server sees changes
    /// nothing); `discard` is Go returning before it created a reporter (no record).
    #[test]
    fn canonical_usage_is_the_last_snapshot_and_keeps_absent_counts_absent() {
        let rt = usage_runtime();
        let facts = std::sync::Arc::new(Facts::new(
            Client::default(),
            Format::OpenAI,
            Format::OpenAI,
            "m",
            b"{}",
            true,
            &crate::inference::dispatch::Trace::default(),
        ));
        let mut tracker = Tracker::start(&rt, &facts, &credential_for("openai-compatibility"), "m");
        let response = ResponseFacts {
            model: Some("m-served".into()),
            service_tier: Some("flex".into()),
        };
        let early = CanonicalUsage {
            input_tokens: Some(10),
            output_tokens: Some(5),
            cache_read_tokens: Some(4),
            ..CanonicalUsage::default()
        };
        tracker.canonical(Some(&early), &response);
        let last = CanonicalUsage {
            input_tokens: Some(10),
            output_tokens: Some(20),
            final_snapshot: true,
            ..CanonicalUsage::default()
        };
        tracker.canonical(Some(&last), &response);
        tracker.succeed();
        let raw = rt.usage_queue().pop_oldest(10);
        assert_eq!(raw.len(), 1, "one record per attempt");
        let record: Value = serde_json::from_slice(&raw[0]).unwrap();
        // The last snapshot replaces the first; nothing is summed, and its absent counts
        // (cache, reasoning, total) are not written as zero.
        assert_eq!(
            record["tokens"],
            serde_json::json!({"input_tokens": 10, "output_tokens": 20, "cache_read_tokens_present": false})
        );
        assert_eq!(record["token_breakdown"]["total_tokens"], 30);
        assert_eq!(record["response_model"], "m-served");
        assert_eq!(record["response_service_tier"], "flex");
    }

    #[test]
    fn canonical_usage_survives_failure_and_a_missing_snapshot_is_unreported() {
        use ck_core::exec::{ExecError, FailureScope};
        let rt = usage_runtime();
        let facts = std::sync::Arc::new(Facts::new(
            Client::default(),
            Format::OpenAI,
            Format::OpenAI,
            "m",
            b"{}",
            true,
            &crate::inference::dispatch::Trace::default(),
        ));
        let error = ExecError::local(502, FailureScope::Transport, "upstream");
        let usage = CanonicalUsage {
            input_tokens: Some(10),
            output_tokens: Some(20),
            total_tokens: Some(30),
            ..CanonicalUsage::default()
        };
        let response = ResponseFacts::default();
        // Not a Claude credential: the legacy failure path would drop the counts.
        let mut tracker = Tracker::start(&rt, &facts, &credential_for("openai-compatibility"), "m");
        tracker.canonical(Some(&usage), &response);
        tracker.fail(&error);
        let mut none = Tracker::start(&rt, &facts, &credential_for("openai-compatibility"), "m");
        none.canonical(None, &response);
        none.succeed();
        let raw = rt.usage_queue().pop_oldest(10);
        let records: Vec<Value> = raw.iter().map(|r| serde_json::from_slice(r).unwrap()).collect();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["failed"], true);
        assert_eq!(
            records[0]["tokens"],
            serde_json::json!({
                "input_tokens": 10, "output_tokens": 20, "total_tokens": 30,
                "cache_read_tokens_present": false,
            })
        );
        assert_eq!(
            records[1]["tokens"],
            serde_json::json!({"cache_read_tokens_present": false}),
            "no snapshot is unknown usage, not zero"
        );
    }

    #[test]
    fn canonical_usage_wins_over_executor_outcomes_and_legacy_cache_buckets() {
        use ck_core::exec::{ExecError, FailureScope};
        let rt = usage_runtime();
        let facts = std::sync::Arc::new(Facts::new(
            Client::default(),
            Format::Claude,
            Format::Claude,
            "m",
            b"{}",
            true,
            &crate::inference::dispatch::Trace::default(),
        ));
        let error = ExecError::local(502, FailureScope::Transport, "upstream");
        let response = ResponseFacts::default();
        let usage = CanonicalUsage {
            input_tokens: Some(10),
            output_tokens: Some(20),
            total_tokens: Some(30),
            ..CanonicalUsage::default()
        };
        // An executor-published failure carries its own zero usage.
        for provider in ["openai-compatibility", "claude", "gemini", "codex"] {
            let mut tracker = Tracker::start(&rt, &facts, &credential_for(provider), "m");
            tracker.sink().publish_failure(502, "upstream");
            tracker.canonical(Some(&usage), &response);
            tracker.fail(&error);
            let raw = rt.usage_queue().pop_oldest(10);
            let record: Value = serde_json::from_slice(&raw[0]).unwrap();
            assert_eq!(record["failed"], true);
            assert_eq!(
                record["tokens"],
                serde_json::json!({
                    "input_tokens": 10, "output_tokens": 20, "total_tokens": 30,
                    "cache_read_tokens_present": false,
                }),
                "{provider}"
            );
        }
        // An executor that requires usage and saw none still gets the canonical record.
        let mut tracker = Tracker::start(&rt, &facts, &credential_for("claude"), "m");
        tracker.sink().usage_required();
        tracker.canonical(Some(&usage), &response);
        tracker.succeed();
        let raw = rt.usage_queue().pop_oldest(10);
        let record: Value = serde_json::from_slice(&raw[0]).unwrap();
        assert_eq!(record["tokens"]["total_tokens"], 30);
        // The Claude observer saw 5m/1h buckets 7/5; the canonical snapshot reports 4 and no 1h.
        let mut tracker = Tracker::start(&rt, &facts, &credential_for("claude"), "m");
        tracker.sink().response_line(
            Format::Claude,
            br#"data: {"type":"message_start","message":{"model":"m","usage":{"input_tokens":9,"output_tokens":1,"cache_creation_input_tokens":12,"cache_creation":{"ephemeral_5m_input_tokens":7,"ephemeral_1h_input_tokens":5}}}}"#,
        );
        let cached = CanonicalUsage {
            cache_write_tokens: Some(4),
            cache_write_5m_tokens: Some(4),
            ..usage
        };
        tracker.canonical(Some(&cached), &response);
        tracker.fail(&error);
        let raw = rt.usage_queue().pop_oldest(10);
        let record: Value = serde_json::from_slice(&raw[0]).unwrap();
        assert_eq!(record["tokens"]["cache_creation_tokens"], 4);
        assert_eq!(record["tokens"]["cache_creation_5m_tokens"], 4);
        assert_eq!(record["tokens"].get("cache_creation_1h_tokens"), None);
    }

    /// The record carries the snapshot's 30-minute and 12-hour writes, its other billing
    /// dimensions and the tier its usage reported; a snapshot without them writes none.
    #[test]
    fn canonical_billing_dimensions_reach_the_record() {
        use super::super::usage_snapshot::Dimension;
        let rt = usage_runtime();
        let facts = std::sync::Arc::new(Facts::new(
            Client::default(),
            Format::OpenAI,
            Format::OpenAI,
            "m",
            b"{}",
            true,
            &crate::inference::dispatch::Trace::default(),
        ));
        let usage = CanonicalUsage {
            input_tokens: Some(10),
            output_tokens: Some(2),
            cache_write_tokens: Some(7),
            cache_write_30m_tokens: Some(3),
            cache_write_12h_tokens: Some(4),
            service_tier: Some("priority".into()),
            dimensions: vec![
                ("speed", Dimension::Text("fast".into())),
                ("web_search_requests", Dimension::Count(2)),
                ("web_fetch_requests", Dimension::Count(0)),
            ],
            ..CanonicalUsage::default()
        };
        let mut tracker = Tracker::start(&rt, &facts, &credential_for("claude"), "m");
        tracker.canonical(Some(&usage), &ResponseFacts::default());
        tracker.succeed();
        let mut bare = Tracker::start(&rt, &facts, &credential_for("claude"), "m");
        bare.canonical(Some(&CanonicalUsage::default()), &ResponseFacts::default());
        bare.succeed();
        // A chunk's `service_tier` is the response tier ahead of the one in usage.
        let mut chunk = Tracker::start(&rt, &facts, &credential_for("claude"), "m");
        let tiered = ResponseFacts {
            model: None,
            service_tier: Some("flex".into()),
        };
        chunk.canonical(Some(&usage), &tiered);
        chunk.succeed();
        let raw = rt.usage_queue().pop_oldest(10);
        let records: Vec<Value> = raw.iter().map(|r| serde_json::from_slice(r).unwrap()).collect();
        assert_eq!(records[0]["tokens"]["cache_creation_30m_tokens"], 3);
        assert_eq!(records[0]["tokens"]["cache_creation_12h_tokens"], 4);
        assert_eq!(records[0]["tokens"].get("cache_creation_5m_tokens"), None);
        assert_eq!(
            records[0]["dimensions"],
            serde_json::json!({"speed": "fast", "web_search_requests": 2, "web_fetch_requests": 0})
        );
        assert_eq!(records[0]["response_service_tier"], "priority");
        assert_eq!(records[1].get("dimensions"), None);
        assert_eq!(records[1].get("response_service_tier"), None);
        assert_eq!(records[1]["tokens"].get("cache_creation_30m_tokens"), None);
        assert_eq!(records[2]["response_service_tier"], "flex");
    }

    /// One record per usage scope (E-Q13): the attempt's, and one per tool model, upstream
    /// response or video job beside it. A snapshot replaces only its own scope's, and every
    /// published scope is recorded: whether a job's report is its charge is the executor's job
    /// table's decision, made before the scope is published.
    #[test]
    fn usage_is_recorded_once_per_scope() {
        // (label, each attempt's snapshots in stream order as (scope, input tokens), expected
        // records as (usage_scope, input tokens)).
        type Case = (
            &'static str,
            Vec<Vec<(Option<&'static str>, u64)>>,
            Vec<(&'static str, Option<u64>)>,
        );
        let cases: Vec<Case> = vec![
            (
                "a single scope is one record",
                vec![vec![(None, 10)]],
                vec![("", Some(10))],
            ),
            (
                "two scopes give two records",
                vec![vec![(None, 10), (Some("tool:gpt-image-2"), 50)]],
                vec![("", Some(10)), ("tool:gpt-image-2", Some(50))],
            ),
            (
                "a snapshot replaces only its own scope",
                vec![vec![(None, 1), (Some("tool:a"), 2), (None, 3), (Some("tool:a"), 4)]],
                vec![("", Some(3)), ("tool:a", Some(4))],
            ),
            (
                "a published job scope is its own record and the poll's has no usage",
                vec![vec![(Some("job:vid_1"), 7)], vec![]],
                vec![("", None), ("job:vid_1", Some(7)), ("", None)],
            ),
            (
                "65 response scopes and the attempt's are all recorded",
                vec![
                    (0..=64)
                        .map(|n| (Some(&*format!("response:{n}").leak()), n + 1))
                        .chain(std::iter::once((None, 1000)))
                        .collect(),
                ],
                std::iter::once(("", Some(1000)))
                    .chain((0..=64).map(|n| (&*format!("response:{n}").leak(), Some(n + 1))))
                    .collect(),
            ),
        ];
        for (label, attempts, expected) in cases {
            let rt = usage_runtime();
            let facts = std::sync::Arc::new(Facts::new(
                Client::default(),
                Format::OpenAI,
                Format::OpenAI,
                "m",
                b"{}",
                true,
                &crate::inference::dispatch::Trace::default(),
            ));
            for snapshots in attempts {
                let mut usage = usage_snapshot::Usage::default();
                for (scope, input) in snapshots {
                    let snapshot = CanonicalUsage {
                        input_tokens: Some(input),
                        ..CanonicalUsage::default()
                    };
                    usage.replace(scope.map(str::to_owned), snapshot);
                }
                let mut tracker = Tracker::start(&rt, &facts, &credential_for("xai"), "m");
                tracker.usage(&usage, &ResponseFacts::default());
                tracker.succeed();
            }
            let records: Vec<(String, Option<u64>)> = rt
                .usage_queue()
                .pop_oldest(100)
                .iter()
                .map(|raw| {
                    let record: Value = serde_json::from_slice(raw).unwrap();
                    (
                        record["usage_scope"].as_str().unwrap_or_default().to_owned(),
                        record["tokens"]["input_tokens"].as_u64(),
                    )
                })
                .collect();
            let expected: Vec<(String, Option<u64>)> = expected.into_iter().map(|(s, n)| (s.to_owned(), n)).collect();
            assert_eq!(records, expected, "{label}");
        }
    }

    /// A malformed legacy dimension or tier is unknown; the token counts still count.
    #[test]
    fn legacy_malformed_dimensions_stay_absent() {
        let detail = parse_claude(
            br#"{"usage":{"input_tokens":5,"output_tokens":2,"service_tier":7,"speed":42,"server_tool_use":{"web_search_requests":-1,"web_fetch_requests":"3"}}}"#,
        );
        assert_eq!((detail.input, detail.output), (5, 2));
        assert_eq!(detail.response_service_tier, "");
        assert_eq!(detail.dimensions, Vec::new());
        let detail = parse_openai(
            br#"{"usage":{"input_tokens":5,"output_tokens":2,"input_tokens_details":{"cache_write_tokens_details":{"ttl_30m_tokens":1.5,"ttl_12h_tokens":null}}}}"#,
        );
        assert_eq!((detail.cache_creation_30m, detail.cache_creation_12h), (None, None));
    }

    /// Each provider's rows read only that provider's usage: Anthropic fields in an OpenAI usage
    /// object are not Anthropic dimensions.
    #[test]
    fn provider_fields_stay_with_their_provider() {
        let detail = parse_openai(
            br#"{"usage":{"prompt_tokens":5,"completion_tokens":2,"speed":"fast","server_tool_use":{"web_search_requests":1}}}"#,
        );
        assert_eq!(detail.dimensions, Vec::new());
        let detail = parse_claude(
            br#"{"usage":{"input_tokens":5,"output_tokens":2,"prompt_tokens_details":{"audio_tokens":2}}}"#,
        );
        assert_eq!(detail.dimensions, Vec::new());
        // Meta's segmentation counts are Responses usage members, in a buffered body too.
        let segmentation = br#"{"usage":{"input_tokens":5,"output_tokens":2,"total_tokens":7,"images_processed":2,"video_frames_processed":0}}"#;
        assert_eq!(parse_body(Format::OpenAI, segmentation).dimensions, Vec::new());
        assert_eq!(
            parse_body(Format::Codex, segmentation).dimensions,
            vec![
                ("images_processed", Dimension::Count(2)),
                ("video_frames_processed", Dimension::Count(0)),
            ]
        );
    }

    /// The billed units of a Responses usage an executor reports, streamed or as a buffered
    /// body, reach the record as their own dimensions in their own units: xAI ticks, nano-USD
    /// and tool counts as integers, Codex rollout budget units as the number the upstream wrote.
    /// A token count is recorded only when the usage reports it as a number: an absent, null or
    /// non-numeric count is left out and never enters a derived total or the breakdown, and
    /// without both input and output the breakdown classifies nothing. A body without usage, or
    /// with an empty one, records no count.
    #[test]
    fn responses_billed_units_reach_the_record() {
        let rt = usage_runtime();
        let facts = std::sync::Arc::new(Facts::new(
            Client::default(),
            Format::OpenAI,
            Format::OpenAI,
            "m",
            b"{}",
            false,
            &crate::inference::dispatch::Trace::default(),
        ));
        let terminal = |usage: &str| format!(r#"data: {{"type":"response.completed","response":{{"usage":{usage}}}}}"#);
        let body = |usage: &str| format!(r#"{{"data":[{{"b64_json":"AAAA"}}],"usage":{usage}}}"#);
        let ticks = serde_json::json!({"cost_in_usd_ticks":200000000});
        let none = serde_json::json!({});
        for (provider, streamed, upstream, dimensions, tokens, literal) in [
            (
                "xai",
                true,
                terminal(
                    r#"{"input_tokens":5,"output_tokens":2,"total_tokens":7,"cost_in_usd_ticks":158100000,"cost_in_nano_usd":15810000,"num_server_side_tools_used":1,"server_side_tool_usage_details":{"x_posts_fetched":7}}"#,
                ),
                serde_json::json!({"cost_in_usd_ticks":158100000,"cost_in_nano_usd":15810000,"num_server_side_tools_used":1,"x_posts_fetched":7}),
                serde_json::json!({"input_tokens":5,"output_tokens":2,"total_tokens":7}),
                "",
            ),
            (
                "codex",
                true,
                terminal(r#"{"input_tokens":100,"input_tokens_details":{"cached_tokens":40},"output_tokens":10,"output_tokens_details":{"reasoning_tokens":4},"total_tokens":110,"codex_rollout_budget_units":2.50}"#),
                serde_json::json!({"codex_rollout_budget_units":2.5}),
                serde_json::json!({"input_tokens":100,"output_tokens":10,"reasoning_tokens":4,"cached_tokens":40,"cache_read_tokens":40,"total_tokens":110}),
                r#""codex_rollout_budget_units":2.50"#,
            ),
            (
                "xai",
                false,
                body(r#"{"input_tokens":3,"output_tokens":0,"total_tokens":3,"cost_in_usd_ticks":200000000}"#),
                ticks.clone(),
                serde_json::json!({"input_tokens":3,"output_tokens":0,"total_tokens":3}),
                "",
            ),
            (
                "xai",
                false,
                r#"{"status":"done","video":{"url":"https://v.example/out.mp4"},"usage":{"cost_in_usd_ticks":5000000000}}"#.to_owned(),
                serde_json::json!({"cost_in_usd_ticks":5000000000_u64}),
                none.clone(),
                "",
            ),
            ("xai", false, r#"{"status":"pending"}"#.to_owned(), Value::Null, none.clone(), ""),
            ("xai", false, r#"{"status":"pending","usage":{}}"#.to_owned(), Value::Null, none.clone(), ""),
            (
                "codex",
                true,
                terminal(r#"{"codex_rollout_budget_units":2.50}"#),
                serde_json::json!({"codex_rollout_budget_units":2.5}),
                none.clone(),
                r#""codex_rollout_budget_units":2.50"#,
            ),
            (
                "xai",
                true,
                terminal(r#"{"cost_in_usd_ticks":5000000000}"#),
                serde_json::json!({"cost_in_usd_ticks":5000000000_u64}),
                none.clone(),
                "",
            ),
            (
                "xai",
                true,
                r#"data: {"type":"response.completed","response":{"service_tier":"priority","usage":{"cost_in_usd_ticks":5000000000}}}"#.to_owned(),
                serde_json::json!({"cost_in_usd_ticks":5000000000_u64}),
                none.clone(),
                r#""response_service_tier":"priority""#,
            ),
            // A total the usage omits is its input and output's sum, as the canonical reader
            // derives it.
            (
                "xai",
                false,
                body(r#"{"cost_in_usd_ticks":200000000,"input_tokens":4,"output_tokens":1}"#),
                ticks.clone(),
                serde_json::json!({"input_tokens":4,"output_tokens":1,"total_tokens":5}),
                "",
            ),
            // A cache count without input, output or total is still a reported count.
            (
                "xai",
                false,
                body(r#"{"input_tokens_details":{"cached_tokens":3}}"#),
                Value::Null,
                serde_json::json!({"cached_tokens":3,"cache_read_tokens":3}),
                "",
            ),
            // Partial, null and non-numeric token counts: only the numbers are recorded, and only
            // they enter a derived total or the breakdown.
            (
                "xai",
                false,
                body(r#"{"input_tokens":3,"output_tokens":2,"total_tokens":"999","cost_in_usd_ticks":100}"#),
                serde_json::json!({"cost_in_usd_ticks":100}),
                serde_json::json!({"input_tokens":3,"output_tokens":2,"total_tokens":5}),
                "",
            ),
            (
                "xai",
                false,
                body(r#"{"input_tokens":3,"output_tokens":2,"input_tokens_details":{"cached_tokens":"2"},"cost_in_usd_ticks":100}"#),
                serde_json::json!({"cost_in_usd_ticks":100}),
                serde_json::json!({"input_tokens":3,"output_tokens":2,"total_tokens":5}),
                "",
            ),
            (
                "xai",
                false,
                body(r#"{"cost_in_usd_ticks":200000000,"input_tokens":3}"#),
                ticks.clone(),
                serde_json::json!({"input_tokens":3}),
                "",
            ),
            (
                "xai",
                false,
                body(r#"{"cost_in_usd_ticks":200000000,"input_tokens":null}"#),
                ticks.clone(),
                none.clone(),
                "",
            ),
            (
                "xai",
                false,
                body(r#"{"cost_in_usd_ticks":200000000,"input_tokens":"3","output_tokens":2,"total_tokens":null,"input_tokens_details":{"cached_tokens":null}}"#),
                ticks.clone(),
                serde_json::json!({"output_tokens":2}),
                "",
            ),
            (
                "xai",
                true,
                terminal(r#"{"input_tokens":3,"output_tokens":null,"cost_in_usd_ticks":200000000}"#),
                ticks.clone(),
                serde_json::json!({"input_tokens":3}),
                "",
            ),
        ] {
            let tracker = Tracker::start(&rt, &facts, &credential_for(provider), "m");
            if streamed {
                tracker.sink().response_line(Format::Codex, upstream.as_bytes());
            } else {
                tracker.sink().response_body(Format::Codex, upstream.as_bytes());
            }
            tracker.succeed();
            let raw = rt.usage_queue().pop_oldest(10);
            let record: Value = serde_json::from_slice(&raw[0]).unwrap();
            let got = record.get("dimensions").cloned().unwrap_or(Value::Null);
            assert_eq!(got, dimensions, "{upstream}");
            let counts: serde_json::Map<String, Value> = record["tokens"]
                .as_object()
                .unwrap()
                .iter()
                .filter(|(name, value)| value.is_number() && !name.contains("_tokens_") && !name.contains("_5m") && !name.contains("_1h"))
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect();
            assert_eq!(Value::Object(counts), tokens, "{upstream}");
            // The breakdown holds only recorded counts: its total is the record's, or zero when
            // unreported, and with both input and output so are its cache reads.
            let breakdown = &record["token_breakdown"];
            let recorded = |name: &str| tokens.get(name).cloned().unwrap_or(Value::from(0));
            assert_eq!(breakdown["total_tokens"], recorded("total_tokens"), "{upstream}");
            if tokens.get("input_tokens").is_some() && tokens.get("output_tokens").is_some() {
                assert_eq!(
                    breakdown["input"]["cache_read_tokens"],
                    recorded("cache_read_tokens"),
                    "{upstream}"
                );
            }
            // Without both input and output the breakdown classifies nothing.
            if tokens.get("input_tokens").is_none() || tokens.get("output_tokens").is_none() {
                let breakdown = &record["token_breakdown"];
                assert_eq!(
                    (&breakdown["input"]["total_tokens"], &breakdown["output"]["total_tokens"]),
                    (&Value::from(0), &Value::from(0)),
                    "{upstream}"
                );
            }
            assert!(String::from_utf8_lossy(&raw[0]).contains(literal), "{upstream}");
        }
    }

    /// Gemini reports its tier inside `usageMetadata`, on the buffered and streamed paths.
    #[test]
    fn gemini_usage_tier_is_the_response_tier() {
        let body = br#"{"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":1,"totalTokenCount":4,"serviceTier":"priority"}}"#;
        assert_eq!(parse_gemini(body).response_service_tier, "priority");
        let mut stream = StreamUsage::default();
        stream.line(Format::Gemini, &[b"data: ".as_slice(), body].concat());
        assert_eq!(stream.detail().unwrap().response_service_tier, "priority");
    }

    /// Billing evidence in a stream is kept whatever the token counts: a chunk with a tier but
    /// no tokens keeps its tier for the later counts, and each usage member's latest dimensions
    /// replace its earlier ones while an unreported member keeps them.
    #[test]
    fn streamed_billing_evidence_is_independent_of_token_counts() {
        use super::super::usage_snapshot::Dimension::Count;
        let gemini_counts = r#"{"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":1,"totalTokenCount":4}}"#;
        let chat = |details: &str| format!(r#"{{"usage":{{"prompt_tokens":3,"completion_tokens":1{details}}}}}"#);
        for (format, lines, tier, dimensions) in [
            (
                Format::Gemini,
                vec![
                    r#"{"usageMetadata":{"serviceTier":"priority"}}"#.to_owned(),
                    gemini_counts.to_owned(),
                ],
                "priority",
                vec![],
            ),
            (
                Format::Interactions,
                vec![
                    r#"{"interaction":{"service_tier":"priority","usage":{}}}"#.to_owned(),
                    gemini_counts.to_owned(),
                ],
                "priority",
                vec![],
            ),
            (
                Format::OpenAI,
                vec![
                    chat(r#","prompt_tokens_details":{"audio_tokens":2}"#),
                    chat(r#","prompt_tokens_details":{"audio_tokens":5}"#),
                ],
                "",
                vec![("input_audio_tokens", Count(5))],
            ),
            (
                Format::OpenAI,
                vec![chat(r#","prompt_tokens_details":{"audio_tokens":2}"#), chat("")],
                "",
                vec![("input_audio_tokens", Count(2))],
            ),
        ] {
            let mut stream = StreamUsage::default();
            for line in &lines {
                stream.line(format, format!("data: {line}").as_bytes());
            }
            let detail = stream.detail().unwrap();
            assert!(has_tokens(detail), "{lines:?}");
            assert_eq!(
                (detail.response_service_tier.as_str(), &detail.dimensions),
                (tier, &dimensions),
                "{lines:?}"
            );
        }
    }

    #[test]
    fn claude_executor_outcomes_reach_the_queue() {
        use ck_core::exec::{ExecError, FailureScope};
        let rt = usage_runtime();
        let facts = std::sync::Arc::new(Facts::new(
            Client::default(),
            Format::OpenAIResponse,
            Format::OpenAIResponse,
            "claude-sonnet-4-6",
            b"{}",
            true,
            &crate::inference::dispatch::Trace::default(),
        ));
        let error = ExecError::local(502, FailureScope::Credential, "Invalid apply_patch tool arguments");
        let tracker = Tracker::start(&rt, &facts, &credential_for("claude"), "claude-sonnet-4-6");
        let sink = tracker.sink();
        sink.claude_cache(ck_core::exec::ClaudeCacheTelemetry {
            request_type: "compaction".into(),
            base_request_type: "compaction".into(),
            ..Default::default()
        });
        sink.usage_required();
        sink.response_line(
            Format::Claude,
            br#"data: {"type":"message_start","message":{"model":"claude-upstream","usage":{"input_tokens":9,"output_tokens":1,"cache_creation_input_tokens":12,"cache_creation":{"ephemeral_5m_input_tokens":7,"ephemeral_1h_input_tokens":5}}}}"#,
        );
        sink.publish_failure(502, "Invalid apply_patch tool arguments");
        tracker.fail(&error);
        let raw = rt.usage_queue().pop_oldest(10);
        let observation: Value = serde_json::from_slice(&raw[0]).unwrap();
        assert_eq!(
            observation["tokens"]["cache_creation_tokens"], 0,
            "Go failure accounting unchanged"
        );
        assert_eq!(observation["tokens"]["cache_creation_5m_tokens"], 7);
        assert_eq!(observation["tokens"]["cache_creation_1h_tokens"], 5);
        assert_eq!(observation["claude_cache"]["request_type"], "compaction");
        assert_eq!(observation["claude_cache"]["attempt"], 0);
        let got: Vec<Value> = raw.iter().map(|q| summary(q)).collect();
        assert_eq!(
            got,
            [serde_json::json!({
                "failed": true, "fail_status": 502, "fail_body": "Invalid apply_patch tool arguments",
                "input": 0, "output": 0, "total": 0, "response_model": "claude-upstream",
                "reasoning_effort": "", "model": "claude-sonnet-4-6",
            })]
        );
        // Without the executor's report the server keeps Claude's buffered usage
        // (StreamUsageBuffer.PublishFailure), so the report above is what drops it.
        let tracker = Tracker::start(&rt, &facts, &credential_for("claude"), "claude-sonnet-4-6");
        tracker.sink().claude_cache(ck_core::exec::ClaudeCacheTelemetry {
            request_type: "compaction".into(),
            base_request_type: "compaction".into(),
            ..Default::default()
        });
        tracker.sink().response_line(
            Format::Claude,
            br#"data: {"type":"message_start","message":{"model":"claude-upstream","usage":{"input_tokens":9,"output_tokens":1}}}"#,
        );
        tracker.fail(&error);
        let got = rt.usage_queue().pop_oldest(10);
        assert_eq!(summary(&got[0])["input"], 9);
        let observation: Value = serde_json::from_slice(&got[0]).unwrap();
        assert_eq!(observation["claude_cache"]["request_type"], "retry");
        assert_eq!(observation["claude_cache"]["base_request_type"], "compaction");
        assert_eq!(observation["claude_cache"]["attempt"], 1);
        assert!(observation["tokens"].get("cache_creation_5m_tokens").is_none());
        let tracker = Tracker::start(&rt, &facts, &credential_for("claude"), "claude-sonnet-4-6");
        tracker.sink().usage_required();
        tracker.sink().claude_cache(ck_core::exec::ClaudeCacheTelemetry {
            request_type: "main".into(),
            base_request_type: "main".into(),
            ..Default::default()
        });
        tracker.succeed();
        let got = rt.usage_queue().pop_oldest(10);
        assert_eq!(got.len(), 1, "tokenless Claude activity is still observed");
        let observation: Value = serde_json::from_slice(&got[0]).unwrap();
        assert_eq!(observation["tokens"]["total_tokens"], 0);
        assert!(observation["claude_cache"].is_object());
        assert_eq!(observation["usage_observed"], false);
        assert_eq!(
            observation["failed"], false,
            "absence of usage is not a successful completion signal"
        );
        let tracker = Tracker::start(&rt, &facts, &credential_for("claude"), "claude-sonnet-4-6");
        tracker.sink().usage_required();
        tracker
            .sink()
            .claude_cache(ck_core::exec::ClaudeCacheTelemetry::default());
        drop(tracker); // Same path as a client dropping its stream before usage.
        let got = rt.usage_queue().pop_oldest(10);
        let observation: Value = serde_json::from_slice(&got[0]).unwrap();
        assert_eq!(observation["usage_observed"], false);
        for fail in [true, false] {
            let tracker = Tracker::start(&rt, &facts, &credential_for("claude"), "claude-sonnet-4-6");
            tracker.sink().discard();
            if fail {
                tracker.fail(&ExecError::local(
                    501,
                    FailureScope::Request,
                    "/responses/compact not supported",
                ));
            } else {
                tracker.succeed();
            }
            assert!(
                rt.usage_queue().pop_oldest(10).is_empty(),
                "discarded attempt (fail: {fail})"
            );
        }
    }

    /// Records from Go's real parsers, `UsageReporter` and `usageQueuePlugin`
    /// (tests/reference/server/main.go `usageRecords`), byte for byte.
    #[test]
    fn queued_records_match_go() {
        let fixture: Value = serde_json::from_str(FIXTURE).unwrap();
        let cases = fixture["usage"].as_array().unwrap();
        assert_eq!(cases.len(), 14);
        for (index, case) in cases.iter().enumerate() {
            let name = case["name"].as_str().unwrap();
            let s = |k: &str| case[k].as_str().unwrap_or_default().to_owned();
            let format = Format::parse(&s("format")).unwrap();
            let lines: Vec<&str> = case["lines"]
                .as_array()
                .unwrap()
                .iter()
                .map(|l| l.as_str().unwrap())
                .collect();
            let detail = if case["stream"].as_bool().unwrap() {
                let mut stream = StreamUsage::default();
                for line in &lines {
                    stream.line(format, line.as_bytes());
                }
                stream.detail().cloned().unwrap_or_default()
            } else {
                parse_body(format, lines[0].as_bytes())
            };
            let mut response_model = ResponseModel::default();
            for line in &lines {
                response_model.observe(line.as_bytes(), &s("provider"));
            }
            let mut metadata = case["metadata"].as_object().cloned().unwrap_or_default();
            metadata.insert("type".into(), Value::from(s("auth_provider")));
            let path = format!("/auth/{}", s("auth_id"));
            let mut credential = ck_core::credential::Credential::from_file(
                std::path::Path::new("/auth"),
                std::path::Path::new(&path),
                metadata,
            )
            .unwrap();
            // Go's fixture auth has no `type` in metadata unless it set one.
            if case["metadata"].get("type").is_none() {
                credential.metadata.remove("type");
            }
            credential.attributes = case["attributes"]
                .as_object()
                .map(|a| {
                    a.iter()
                        .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_owned()))
                        .collect()
                })
                .unwrap_or_default();
            let reasoning = if s("translated_payload").is_empty() {
                s("reasoning_effort")
            } else {
                ck_wire::thinking::extract_translated_reasoning_effort(
                    s("translated_payload").as_bytes(),
                    &s("translated_format"),
                )
            };
            let record = Record {
                timestamp: s("requested_at"),
                latency_ms: case["latency_ms"].as_i64().unwrap(),
                ttft_ms: case["ttft_ms"].as_i64().unwrap(),
                execution_id: s("execution_id"),
                provider: s("provider"),
                executor_type: s("executor_type"),
                model: s("model"),
                alias: s("alias"),
                source: source(&credential, &s("client_key")),
                auth_index: ck_core::config::credentials::auth_index(&credential),
                access_token_sha256: access_token_sha256(&credential),
                auth_type: ck_core::registry::dynamic::auth_kind(&credential)
                    .unwrap_or_default()
                    .to_owned(),
                reasoning_effort: reasoning,
                service_tier: s("service_tier"),
                generate: case["generate"].as_bool().unwrap(),
                stream: case["stream"].as_bool().unwrap(),
                failed: case["failed"].as_bool().unwrap(),
                fail_status: case["fail_status"].as_i64().unwrap(),
                fail_body: s("fail_body"),
                detail,
                response_model: response_model.get().to_owned(),
                response_headers: case["response_headers"]
                    .as_object()
                    .map(|h| {
                        h.iter()
                            .map(|(k, v)| {
                                let values = v.as_array().unwrap().iter().map(|x| x.as_str().unwrap().to_owned());
                                (k.clone(), values.collect())
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                // Go's record has none of these: none are written.
                prompt_prefix_hash: String::new(),
                claude_cache: None,
                usage_scope: String::new(),
                usage_observed: None,
                timings: Timings::default(),
            };
            let client = Client {
                client_ip: s("client_ip"),
                resolved_client_ip: s("resolved_client_ip"),
                x_forwarded_for: s("x_forwarded_for"),
                user_agent: s("user_agent"),
                session_id: s("session_id"),
                parent_session_id: s("parent_session_id"),
                is_fork: false,
                node_kind: String::new(),
                is_compaction: false,
                session_source: "",
                request_id: s("request_id"),
                endpoint: s("endpoint"),
                api_key: s("client_key"),
                access_provider: String::new(),
            };
            let got = queued(&record, &client);
            // Go's exact bytes: the fixture re-indents the queued payload; compacting
            // restores it.
            let raw = gj::get(FIXTURE.as_bytes(), &format!("usage.{index}.queued"));
            let want = String::from_utf8(gj::compact(raw.raw(), false)).unwrap();
            // Go 37fe2608, after the fixture's 6fecc6e, always writes `is_native_key`
            // after `api_key`; with no access provider it is false and
            // `access_provider` is omitted.
            let at = want.find(",\"request_id\":").unwrap();
            let want = format!("{},\"is_native_key\":false{}", &want[..at], &want[at..]);
            assert_eq!(String::from_utf8_lossy(&got), want, "{name}");
        }
    }

    /// Go `TestUsageQueuePluginPayloadDistinguishesNativeKey` and
    /// `TestIsNativeAccessProvider` (37fe2608): `is_native_key` is always written right
    /// after `api_key`, true only for CPA's own client-key providers (trimmed, any case);
    /// `access_provider` follows it unless empty.
    #[test]
    fn queued_payload_distinguishes_native_key() {
        for (provider, key, want) in [
            (
                "config-inline",
                "sk-native-key-123",
                r#""api_key":"sk-native-key-123","is_native_key":true,"access_provider":"config-inline","request_id""#,
            ),
            (
                "  CONFIG-API-KEY ",
                "k",
                r#""api_key":"k","is_native_key":true,"access_provider":"CONFIG-API-KEY","request_id""#,
            ),
            (
                "custom-auth-provider",
                "external-principal-456",
                r#""api_key":"external-principal-456","is_native_key":false,"access_provider":"custom-auth-provider","request_id""#,
            ),
            (
                "realtime-client-secret",
                "p",
                r#""api_key":"p","is_native_key":false,"access_provider":"realtime-client-secret","request_id""#,
            ),
            ("", "", r#""api_key":"","is_native_key":false,"request_id""#),
        ] {
            let client = Client {
                api_key: key.into(),
                access_provider: provider.into(),
                ..Default::default()
            };
            let got = String::from_utf8(queued(&Record::default(), &client)).unwrap();
            assert!(got.contains(want), "{provider:?}: {got}");
        }
    }

    /// A runtime whose usage queue keeps records.
    fn usage_runtime() -> std::sync::Arc<crate::Runtime> {
        let cfg =
            ck_core::config::Config::parse("observability:\n  usage:\n    usage-statistics-enabled: true\n").unwrap();
        let executors = ck_exec::Executors {
            claude: ck_exec::claude::ClaudeExecutor::new("http://127.0.0.1:1").unwrap(),
            codex: Default::default(),
            devices: Default::default(),
            openai: Default::default(),
            google: Default::default(),
        };
        let rt = std::sync::Arc::new(crate::testing::runtime(cfg.clone(), Vec::new(), executors));
        rt.usage_queue().configure(true, &cfg);
        rt
    }

    fn credential_for(provider: &str) -> ck_core::credential::Credential {
        let mut metadata = serde_json::Map::new();
        metadata.insert("type".into(), provider.into());
        let mut c = ck_core::credential::Credential::from_file(
            std::path::Path::new("/auth"),
            std::path::Path::new("/auth/a.json"),
            metadata,
        )
        .unwrap();
        c.attributes.insert("auth_kind".into(), "oauth".into());
        c
    }

    /// The fields Go's reporter goldens compare, from one queued record.
    fn summary(queued: &[u8]) -> Value {
        let v: Value = serde_json::from_slice(queued).unwrap();
        let tokens = &v["tokens"];
        serde_json::json!({
            "failed": v["failed"],
            "fail_status": v["fail"]["status_code"].as_i64().unwrap_or(0),
            "fail_body": v["fail"]["body"].as_str().unwrap_or(""),
            "input": tokens["input_tokens"],
            "output": tokens["output_tokens"],
            "total": tokens["total_tokens"],
            "response_model": v["response_model"].as_str().unwrap_or(""),
            "reasoning_effort": v["reasoning_effort"].as_str().unwrap_or(""),
            "model": v["model"],
        })
    }

    /// Go `UsageReporter` call sequences as Go's executors make them
    /// (tests/reference/server/main.go `reporterSequences`), replayed through the
    /// tracker and the executor sink: what Go publishes, and when it publishes nothing.
    #[test]
    fn reporter_sequences_match_go() {
        use ck_core::exec::{ExecError, FailureScope};
        let fixture: Value = serde_json::from_str(FIXTURE).unwrap();
        let cases = fixture["reporter"].as_array().unwrap();
        let gemini = [
            r#"data: {"candidates":[{"content":{"parts":[{"text":"a"}]}}],"usageMetadata":{"promptTokenCount":4,"candidatesTokenCount":1,"totalTokenCount":5}}"#,
            r#"data: {"candidates":[{"content":{"parts":[{"text":"b"}]}}],"usageMetadata":{"promptTokenCount":4,"candidatesTokenCount":3,"totalTokenCount":7}}"#,
        ];
        let claude = [
            r#"data: {"type":"message_start","message":{"model":"claude-sonnet-4-6","usage":{"input_tokens":9,"output_tokens":1}}}"#,
            r#"data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":6}}"#,
        ];
        let kimi_chunk = r#"data: {"id":"c","object":"chat.completion.chunk","model":"kimi-k2","choices":[{"index":0,"delta":{"content":"x"}}]}"#;
        let kimi_usage = r#"data: {"id":"c","object":"chat.completion.chunk","model":"kimi-k2","choices":[],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7}}"#;
        let gemini_body = br#"{"usageMetadata":{"promptTokenCount":2,"candidatesTokenCount":2,"totalTokenCount":4}}"#;
        let error = |status: u16, text: &str| ExecError::local(status, FailureScope::Request, text);
        let rt = usage_runtime();
        for case in cases {
            let name = case["name"].as_str().unwrap();
            let (provider, model) = match name.split(' ').next().unwrap() {
                "gemini" | "publish" | "failure" => ("gemini", "gemini-2.5-pro"),
                "claude" => ("claude", "claude-sonnet-4-6"),
                "kimi" => ("kimi", "kimi-k2"),
                "devin" => ("devin", "devin-chat"),
                _ => ("codex", "gpt-5.5"),
            };
            let facts = std::sync::Arc::new(Facts::new(
                Client::default(),
                Format::OpenAI,
                Format::OpenAI,
                model,
                b"{}",
                true,
                &crate::inference::dispatch::Trace::default(),
            ));
            let tracker = Tracker::start(&rt, &facts, &credential_for(provider), model);
            let sink = tracker.sink();
            let lines = |format: Format, lines: &[&str]| {
                for line in lines {
                    sink.response_line(format, line.as_bytes());
                }
            };
            match name {
                "gemini stream usage then scan error" => {
                    lines(Format::Gemini, &gemini);
                    tracker.fail(&error(0, "read: connection reset"));
                }
                "gemini stream usage" => {
                    lines(Format::Gemini, &gemini);
                    tracker.succeed();
                }
                "claude stream usage then scan error" => {
                    lines(Format::Claude, &claude);
                    tracker.fail(&error(502, "stream broke"));
                }
                "kimi stream without usage" => {
                    sink.usage_required();
                    lines(Format::OpenAI, &[kimi_chunk, "data: [DONE]"]);
                    tracker.succeed();
                }
                "kimi stream with usage" => {
                    sink.usage_required();
                    lines(Format::OpenAI, &[kimi_chunk, kimi_usage, "data: [DONE]"]);
                    tracker.succeed();
                }
                // The executor reports no body because Go's condition did not publish.
                "kimi native nonstream without usage" => {
                    sink.usage_required();
                    tracker.succeed();
                }
                _ if name.starts_with("kimi translated effort ") => {
                    let payload = name.trim_start_matches("kimi translated effort ");
                    sink.request_for("kimi", payload.as_bytes());
                    sink.response_body(
                        Format::OpenAI,
                        br#"{"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#,
                    );
                    sink.publish();
                    tracker.succeed();
                }
                "devin set response model" => {
                    sink.response_model("  devin-model-x ");
                    tracker.succeed();
                }
                // Go's deferred EnsurePublished wins over the stream error the client sees.
                "devin truncated before EOS" => {
                    sink.publish();
                    tracker.fail(&error(0, "devin stream terminated prematurely before EOS trailer"));
                }
                "devin trailer error" => {
                    sink.publish_failure(0, "devin trailer: permission_denied");
                    tracker.fail(&error(403, "devin trailer: permission_denied"));
                }
                "publish then failure" => {
                    sink.response_body(Format::Gemini, gemini_body);
                    sink.publish();
                    tracker.fail(&error(500, "late"));
                }
                "failure then publish" => {
                    sink.publish_failure(429, "slow down");
                    sink.response_body(Format::Gemini, gemini_body);
                    sink.publish();
                    tracker.succeed();
                }
                "terminal response model then set" => {
                    sink.response_line(
                        Format::OpenAIResponse,
                        br#"data: {"type":"response.completed","response":{"model":"gpt-5.5-2026-01-01"}}"#,
                    );
                    sink.response_model("other-model");
                    sink.publish();
                    tracker.succeed();
                }
                other => panic!("unscripted Go case {other}"),
            }
            let got: Vec<Value> = rt.usage_queue().pop_oldest(10).iter().map(|q| summary(q)).collect();
            let want: Vec<Value> = case["records"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| {
                    // The golden holds Go's record before the queue; Go's queue plugin
                    // (redisqueue/plugin.go `failDetail`) reports 200 for a success and
                    // 500 for a failure without status, as `queued` does.
                    let status = match (r["failed"].as_bool().unwrap(), r["fail_status"].as_i64().unwrap()) {
                        (false, _) => 200,
                        (true, s) if s <= 0 => 500,
                        (true, s) => s,
                    };
                    serde_json::json!({
                        "failed": r["failed"], "fail_status": status, "fail_body": r["fail_body"],
                        "input": r["input"], "output": r["output"], "total": r["total"],
                        "response_model": r["response_model"], "reasoning_effort": r["reasoning_effort"],
                        "model": r["model"],
                    })
                })
                .collect();
            assert_eq!(got, want, "{name}");
        }
    }

    /// Go `IsModelSubstituted` (tests/reference/server/main.go `substitutions`).
    #[test]
    fn model_substitution_matches_go() {
        let fixture: Value = serde_json::from_str(FIXTURE).unwrap();
        for case in fixture["substitution"].as_array().unwrap() {
            let (requested, served) = (case["requested"].as_str().unwrap(), case["served"].as_str().unwrap());
            assert_eq!(
                is_model_substituted(requested, served),
                case["substituted"].as_bool().unwrap(),
                "{requested:?} -> {served:?}"
            );
        }
    }

    /// A loopback upstream for redirected exchanges, on raw sockets with a small receive
    /// window so an unread body stays with the sender. `/see-other` reads the body and
    /// answers 303 to `/landing`, which answers `hello`; `/temporary` reads the body and
    /// answers 307 to `/cut`, which reads the head and 64 KiB of the body, then closes.
    async fn redirecting_upstream() -> String {
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.set_recv_buffer_size(4096).unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = socket.listen(16).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            while let Ok((conn, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut conn = tokio::io::BufReader::new(conn);
                    loop {
                        let (mut path, mut length) = (String::new(), 0);
                        loop {
                            let mut line = String::new();
                            if conn.read_line(&mut line).await.unwrap_or(0) == 0 {
                                return;
                            }
                            let line = line.trim_end().to_ascii_lowercase();
                            if line.is_empty() {
                                break;
                            }
                            if path.is_empty() {
                                path = line.split(' ').nth(1).unwrap_or_default().to_owned();
                            } else if let Some(value) = line.strip_prefix("content-length:") {
                                length = value.trim().parse().unwrap();
                            }
                        }
                        let mut body = vec![0; if path == "/cut" { 64 << 10 } else { length }];
                        if conn.read_exact(&mut body).await.is_err() {
                            return;
                        }
                        let reply: &[u8] = match path.as_str() {
                            "/see-other" => {
                                b"HTTP/1.1 303 See Other\r\nLocation: /landing\r\nContent-Length: 0\r\n\r\n"
                            }
                            "/temporary" => {
                                b"HTTP/1.1 307 Temporary Redirect\r\nLocation: /cut\r\nContent-Length: 0\r\n\r\n"
                            }
                            "/landing" => b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello",
                            // Closed with the rest of the upload unread.
                            _ => return,
                        };
                        conn.get_mut().write_all(reply).await.unwrap();
                    }
                });
            }
        });
        url
    }

    /// The published record of one timed POST of `body` to `url` through the shared
    /// HTTP redirect path, within `timeout` if any. Bound socket buffering so the
    /// partial-upload case does not depend on the host's TCP autotuning limits.
    async fn timed_exchange(
        rt: &std::sync::Arc<crate::Runtime>,
        url: &str,
        body: bytes::Bytes,
        timeout: Option<std::time::Duration>,
    ) -> Value {
        use futures_util::StreamExt;
        let facts = std::sync::Arc::new(Facts::new(
            Client::default(),
            Format::Claude,
            Format::Claude,
            "m",
            b"{}",
            true,
            &crate::inference::dispatch::Trace::default(),
        ));
        let tracker = Tracker::start(rt, &facts, &credential_for("claude"), "m");
        let sink = tracker.sink();
        let client = wreq::Client::builder()
            .no_proxy()
            .tcp_send_buffer_size(Some(64 << 10))
            .build()
            .unwrap();
        let sent = ck_exec::proxy::send_routed(
            &|_| {
                Ok(ck_exec::proxy::Route {
                    client: client.clone(),
                    order: None,
                })
            },
            url,
            ck_exec::proxy::GoHeaders::new(),
            body,
            timeout,
            Some(&sink),
        )
        .await;
        match sent {
            Ok(upstream) => {
                let chunks: Vec<_> = upstream.body.collect().await;
                assert!(chunks.iter().all(Result::is_ok));
                tracker.succeed();
            }
            Err(error) => tracker.fail(&error),
        }
        let queued = rt.usage_queue().pop_oldest(10);
        assert_eq!(queued.len(), 1);
        serde_json::from_slice(&queued[0]).unwrap()
    }

    /// A redirect starts the upload measurements over. A POST answered with 303 and
    /// followed as a GET reports no upload and no request body, not the POST's. A 307
    /// whose second upload is cut short reports that hop's connection and no upload,
    /// not the first hop's finished one. A request the deadline stops before it is sent
    /// reports no request body.
    #[tokio::test]
    async fn redirects_start_the_upload_measurements_over() {
        let rt = usage_runtime();
        let upstream = redirecting_upstream().await;
        let record = timed_exchange(
            &rt,
            &format!("{upstream}/see-other"),
            bytes::Bytes::from_static(b"{}"),
            None,
        )
        .await;
        let timings = &record["timings"];
        assert_eq!(record["failed"], false, "{record}");
        assert_eq!(record["upstream_request_bytes"], 0, "{record}");
        assert_eq!(record["upstream_response_bytes"], 5, "{record}");
        assert!(timings["upstream_headers_ms"].is_i64(), "{record}");
        for stale in ["upstream_connected_ms", "upstream_body_sent_ms"] {
            assert!(timings.get(stale).is_none(), "{stale}: {record}");
        }

        // More than the client and the sockets hold, so the second upload cannot finish.
        let body = bytes::Bytes::from(vec![b'x'; 6 << 20]);
        let record = timed_exchange(&rt, &format!("{upstream}/temporary"), body, None).await;
        let timings = &record["timings"];
        assert_eq!(record["failed"], true, "{record}");
        assert!(timings["upstream_connected_ms"].is_i64(), "{record}");
        assert!(timings.get("upstream_body_sent_ms").is_none(), "{record}");
        assert!(record.get("upstream_request_bytes").is_none(), "{record}");

        let expired = Some(std::time::Duration::ZERO);
        let record = timed_exchange(&rt, &format!("{upstream}/landing"), bytes::Bytes::new(), expired).await;
        assert_eq!(record["failed"], true, "{record}");
        assert!(record.get("upstream_request_bytes").is_none(), "{record}");
    }

    /// A late report from an earlier hop (its body, taken after its redirect arrived)
    /// does not fill in the hop being sent.
    #[test]
    fn a_late_hop_report_is_dropped() {
        use ck_core::exec::UpstreamStage;
        let start = std::time::Instant::now();
        let at = |ms| start + std::time::Duration::from_millis(ms);
        let mut exchange = Exchange::default();
        for (ms, stage) in [
            (0, UpstreamStage::Started),
            (0, UpstreamStage::Hop { hop: 0, body: true }),
            (1, UpstreamStage::Connected { hop: 0 }),
            (2, UpstreamStage::Hop { hop: 1, body: true }),
            (3, UpstreamStage::Connected { hop: 1 }),
            (4, UpstreamStage::Connected { hop: 0 }),
            (5, UpstreamStage::BodySent { hop: 0, bytes: 9 }),
        ] {
            exchange.mark(stage, at(ms));
        }
        assert_eq!((exchange.started, exchange.connected), (Some(at(0)), Some(at(3))));
        assert_eq!((exchange.body_sent, exchange.request_bytes), (None, None));
    }

    /// Go's TTFT: from the upstream request to the first body byte; the first packet
    /// stands in until a token event; nothing without a start.
    #[test]
    fn ttft_marks_follow_go() {
        let mut t = Ttft::default();
        t.first_byte();
        assert!(t.tracked);
        assert_eq!(t.get(), std::time::Duration::ZERO);

        let mut t = Ttft::default();
        t.start();
        std::thread::sleep(std::time::Duration::from_millis(5));
        t.token(false);
        let packet = t.get();
        assert!(packet >= std::time::Duration::from_millis(5));
        std::thread::sleep(std::time::Duration::from_millis(5));
        t.token(false);
        assert_eq!(t.get(), packet, "a second non-token frame changes nothing");
        t.token(true);
        let ttft = t.get();
        assert!(ttft > packet);
        t.first_byte();
        t.start();
        assert_eq!(t.get(), ttft, "the first TTFT wins");
    }
}
