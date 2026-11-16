/// Which Go executor's stream usage rules a [`defer_usage`] tap follows.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum UsageRule {
    /// Kimi chat (`StreamUsageBuffer.ObserveOpenAIStream` on every line).
    OpenAIStream,
    /// Kimi native Responses: terminal `data:` events, Codex usage with tokens, else the
    /// top-level OpenAI usage with tokens.
    KimiResponses,
    /// Meta: `response.completed` / `response.incomplete` `data:` events with Codex usage
    /// (or a tier).
    MetaResponses,
}

/// Go's `StreamUsageBuffer` for the Responses rules: the merged detail as the usage node
/// it was parsed from (and that node's format) plus the response service tier.
#[derive(Default)]
struct Merged {
    ok: bool,
    usage: Option<(ck_core::format::Format, Vec<u8>)>,
    tier: String,
}

impl Merged {
    /// `StreamUsageBuffer.Observe(detail, true)`: a detail with billing evidence (a nonzero token
    /// or a billed unit) or without a tier replaces the buffer, keeping the old tier when it has
    /// none; a tier-only detail updates just the tier.
    fn observe(&mut self, usage: Option<(ck_core::format::Format, Vec<u8>)>, tier: String, evidence: bool) {
        if tier.is_empty() || evidence {
            self.usage = usage;
            if !tier.is_empty() {
                self.tier = tier;
            }
        } else {
            self.tier = tier;
        }
        self.ok = true;
    }

    /// The merged detail as one payload of its format, without a response model.
    fn payload(&self) -> Option<(ck_core::format::Format, Vec<u8>)> {
        use ck_core::format::Format;
        if !self.ok {
            return None;
        }
        let (format, node) = match &self.usage {
            Some((format, node)) => (*format, Some(node.as_slice())),
            None => (Format::Codex, None),
        };
        let mut body = b"{".to_vec();
        if let Some(node) = node {
            body.extend_from_slice(b"\"usage\":");
            body.extend_from_slice(node);
        }
        if !self.tier.is_empty() {
            if node.is_some() {
                body.push(b',');
            }
            body.extend_from_slice(b"\"service_tier\":");
            body.extend_from_slice(serde_json::to_string(&self.tier).unwrap_or_default().as_bytes());
        }
        body.push(b'}');
        let payload = match format {
            Format::OpenAI => body,
            _ => [&b"{\"type\":\"response.completed\",\"response\":"[..], &body, b"}"].concat(),
        };
        Some((format, payload))
    }
}

#[derive(Default)]
struct Held {
    /// Kimi chat: the usage and tier lines, in order, without their response models.
    lines: Vec<(ck_core::format::Format, Vec<u8>)>,
    /// The Responses rules' merged buffer.
    merged: Merged,
}

/// Usage held back until the response completes (Go's deferred `StreamUsageBuffer.Publish`;
/// a failure publishes no usage, `PublishFailure` uses an empty detail). Call [`Self::commit`]
/// exactly when the stream ends without an error. Committed payloads carry no response
/// model, so they never replace the model observed live.
#[derive(Clone, Default)]
pub(crate) struct DeferredUsage {
    sink: ck_core::exec::UsageSink,
    held: std::sync::Arc<std::sync::Mutex<Held>>,
}

impl DeferredUsage {
    /// Go `StreamUsageBuffer.PublishFailure`: reports the held usage and publishes
    /// `error` with it (status 0 for a transport fault, which Go's error has none of).
    pub(crate) fn publish_failure(&self, error: &ck_core::exec::ExecError) {
        self.commit();
        let status = if error.scope == ck_core::exec::FailureScope::Transport {
            0
        } else {
            error.status
        };
        self.sink
            .publish_failure_with_usage(status, &String::from_utf8_lossy(&error.body));
    }

    /// Reports the held usage.
    pub(crate) fn commit(&self) {
        let held = std::mem::take(&mut *self.held.lock().unwrap_or_else(std::sync::PoisonError::into_inner));
        for (format, payload) in held.lines.into_iter().chain(held.merged.payload()) {
            self.sink.response_line(format, &[&b"data: "[..], &payload].concat());
        }
    }
}

/// Go `jsonPayload`: the JSON payload of a `data:` line (or a bare JSON line), trimmed
/// with `bytes.TrimSpace`.
fn line_payload(line: &[u8]) -> Option<&[u8]> {
    use ck_wire::gostr::trim_space;
    let trimmed = trim_space(line);
    let payload = trimmed.strip_prefix(b"data:").map_or(trimmed, trim_space);
    (payload.first() == Some(&b'{')).then_some(payload)
}

/// A payload without its usage and service tier, so reporting it only feeds the
/// response model (Go `ObserveResponseModel`).
pub(crate) fn model_only(payload: &[u8]) -> Vec<u8> {
    strip_paths(
        payload,
        &[
            "usage",
            "service_tier",
            "response.usage",
            "response.service_tier",
            "interaction.service_tier",
        ],
    )
}

/// The model fields Go's generic response-model extractor reads.
const MODEL_PATHS: [&str; 6] = [
    "response.model",
    "interaction.model",
    "modelVersion",
    "response.modelVersion",
    "message.model",
    "model",
];

fn strip_paths(payload: &[u8], paths: &[&str]) -> Vec<u8> {
    let mut out = payload.to_vec();
    for path in paths {
        if ck_wire::json::get(&out, path).exists() {
            ck_wire::json::delete(&mut out, path);
        }
    }
    out
}

/// Go's `TotalTokens > 0 || InputTokens > 0` for a parsed OpenAI-style usage node
/// (`parseOpenAIStyleUsageNode`). A `total_tokens` of exactly 0 falls back to the token
/// breakdown's total, which is input + output when both are non-negative; with input 0
/// that is the output count.
pub(crate) fn usage_has_tokens(node: &ck_wire::json::Res<'_>) -> bool {
    if !node.is_object() {
        return false;
    }
    let total = node.get("total_tokens").int();
    let input = pick(node, &["prompt_tokens", "input_tokens"]);
    let output = pick(node, &["completion_tokens", "output_tokens"]);
    total > 0 || input > 0 || (total == 0 && input == 0 && output > 0)
}

/// The first existing path's integer (Go's `Exists()` fallbacks in parseOpenAIStyleUsageNode).
fn pick(node: &ck_wire::json::Res<'_>, paths: &[&str]) -> i64 {
    paths
        .iter()
        .map(|p| node.get(p))
        .find(|r| r.exists())
        .map_or(0, |r| r.int())
}

/// Go `hasNonZeroTokenUsage` for the detail parsed from `node`.
fn usage_nonzero(node: &ck_wire::json::Res<'_>) -> bool {
    [
        &["prompt_tokens", "input_tokens"][..],
        &["completion_tokens", "output_tokens"],
        &["total_tokens"],
        &[
            "prompt_tokens_details.cached_tokens",
            "input_tokens_details.cached_tokens",
        ],
        &[
            "input_tokens_details.cache_creation_tokens",
            "input_tokens_details.cache_write_tokens",
            "prompt_tokens_details.cache_creation_tokens",
            "prompt_tokens_details.cache_write_tokens",
        ],
        &[
            "completion_tokens_details.reasoning_tokens",
            "output_tokens_details.reasoning_tokens",
        ],
    ]
    .iter()
    .any(|paths| pick(node, paths) != 0)
}

/// Go `hasOpenAIStyleUsageTokenFields`.
fn usage_has_fields(node: &ck_wire::json::Res<'_>) -> bool {
    node.is_object()
        && [
            "total_tokens",
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
        .any(|f| node.get(f).exists())
}

/// Go `extractResponseServiceTier`: the first non-blank tier of a valid payload.
pub(crate) fn response_tier(payload: &[u8]) -> String {
    if !ck_wire::json::valid(payload) {
        return String::new();
    }
    ["response.service_tier", "service_tier", "interaction.service_tier"]
        .iter()
        .map(|p| ck_wire::json::get(payload, p).str().into_owned())
        .map(|t| String::from_utf8_lossy(ck_wire::gostr::trim_space(t.as_bytes())).into_owned())
        .find(|t| !t.is_empty())
        .unwrap_or_default()
}

/// One `StreamUsageBuffer.Observe` of Go `ParseCodexUsage(payload)` (format Codex) or
/// `ParseOpenAIUsage(payload)` (format OpenAI); false when Go's `ok` is false.
fn observe_parsed(merged: &mut Merged, format: ck_core::format::Format, payload: &[u8]) -> bool {
    let path = if format == ck_core::format::Format::OpenAI {
        "usage"
    } else {
        "response.usage"
    };
    let node = ck_wire::json::get(payload, path);
    let tier = response_tier(payload);
    if !usage_has_fields(&node) {
        if tier.is_empty() {
            return false;
        }
        merged.observe(None, tier, false);
        return true;
    }
    let evidence = usage_nonzero(&node) || reports_billed_units(&node);
    merged.observe(Some((format, node.raw().to_vec())), tier, evidence);
    true
}

/// Usage members billed in their own unit rather than in tokens: Meta segmentation's images and
/// video frames, the `responses` rows of ck-server's `usage_snapshot::DIMENSIONS`.
const BILLED_UNITS: [&str; 2] = ["images_processed", "video_frames_processed"];

/// The usage reports a [`BILLED_UNITS`] count the usage record keeps (an unsigned integer). Such
/// a usage is billing evidence with every token count zero, so a tier beside it must not drop it.
fn reports_billed_units(node: &ck_wire::json::Res<'_>) -> bool {
    BILLED_UNITS
        .iter()
        .any(|member| std::str::from_utf8(node.get(member).raw()).is_ok_and(|raw| raw.parse::<u64>().is_ok()))
}

/// Taps the upstream lines for usage the way `rule`'s Go executor observes them: each
/// line's response model is reported as it arrives; usage is held in the returned
/// [`DeferredUsage`] until the caller commits it on success.
pub(crate) fn defer_usage(
    lines: ck_core::exec::ExecStream,
    usage: &ck_core::exec::UsageSink,
    rule: UsageRule,
) -> (ck_core::exec::ExecStream, DeferredUsage) {
    use ck_core::format::Format;
    use futures_util::StreamExt;
    let deferred = DeferredUsage {
        sink: usage.clone(),
        held: Default::default(),
    };
    if !usage.enabled() {
        return (lines, deferred);
    }
    let (sink, held) = (usage.clone(), deferred.clone());
    let model_format = if rule == UsageRule::OpenAIStream {
        Format::OpenAI
    } else {
        Format::Codex
    };
    let tapped = lines
        .inspect(move |item| {
            let Ok(line) = item else { return };
            // Go matches `data:` on the raw scanned line for Responses usage, and Meta
            // observes nothing else.
            let is_data = line.starts_with(b"data:");
            if rule == UsageRule::MetaResponses && !is_data {
                return;
            }
            let Some(payload) = line_payload(line) else { return };
            let mut model_line = b"data: ".to_vec();
            model_line.extend_from_slice(&model_only(payload));
            sink.response_line(model_format, &model_line);
            let mut held = held.held.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            let kind = ck_wire::json::get(payload, "type").bytes().into_owned();
            match rule {
                UsageRule::OpenAIStream => {
                    let has = |n: &[u8]| payload.windows(n.len()).any(|w| w == n);
                    if has(b"\"usage\"") || has(b"\"service_tier\"") {
                        let payload = if ck_wire::json::valid(payload) {
                            strip_paths(payload, &MODEL_PATHS)
                        } else {
                            payload.to_vec()
                        };
                        held.lines.push((Format::OpenAI, payload));
                    }
                }
                UsageRule::KimiResponses => {
                    if !is_data
                        || !matches!(
                            kind.as_slice(),
                            b"response.completed" | b"response.incomplete" | b"response.done"
                        )
                    {
                        return;
                    }
                    // Only details with tokens are observed, so each replaces the buffer.
                    if usage_has_tokens(&ck_wire::json::get(payload, "response.usage")) {
                        observe_parsed(&mut held.merged, Format::Codex, payload);
                    } else if usage_has_tokens(&ck_wire::json::get(payload, "usage")) {
                        observe_parsed(&mut held.merged, Format::OpenAI, payload);
                    }
                }
                UsageRule::MetaResponses => {
                    if matches!(kind.as_slice(), b"response.completed" | b"response.incomplete") {
                        observe_parsed(&mut held.merged, Format::Codex, payload);
                    }
                }
            }
        })
        .boxed();
    (tapped, deferred)
}

/// Go `ObserveResponseModel(body)` before a translation that may still fail: only the
/// response model is reported; usage follows once the response succeeded.
pub(crate) fn report_model(sink: &ck_core::exec::UsageSink, format: ck_core::format::Format, body: &[u8]) {
    if !sink.enabled() {
        return;
    }
    // Go's generic model extraction needs the whole payload to be one valid JSON value,
    // so a multi-event SSE body names no model.
    match line_payload(body).filter(|p| ck_wire::json::valid(p)) {
        Some(payload) => sink.response_line(format, &model_only(payload)),
        None => sink.response_line(format, b"{}"),
    }
}
