//! xAI namespace restoration, encrypted-content sanitizing, completed-output reconstruction
//! and status errors. Native reasoning and server-tool traces retain their wire identities.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use ck_core::exec::ExecError;
use ck_wire::gostr::GoStr;
use ck_wire::json::{self as gj, Kind, Res};

use crate::openai_compat_payload::ensure_responses_usage_details;

pub(crate) const FUNCTION: &str = "function";
pub(crate) const CUSTOM: &str = "custom";
pub(crate) const NAMESPACE: &str = "namespace";
pub(crate) const WEB_SEARCH: &str = "web_search";

/// `xaiNamespaceToolRef`: what a flattened or folded tool name stands for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NamespaceRef {
    pub namespace: String,
    pub name: String,
    pub dispatcher: bool,
}

pub(crate) type NamespaceRefs = HashMap<String, NamespaceRef>;

/// gjson `Result.String()` trimmed like Go's `strings.TrimSpace`.
pub(crate) fn text(r: &Res<'_>) -> String {
    r.str().trim().to_owned()
}

/// `json.Marshal([]json.RawMessage)`: each item compacted with HTML escaping.
pub(crate) fn marshal_raw(items: &[Vec<u8>]) -> Vec<u8> {
    let compacted: Vec<Vec<u8>> = items.iter().map(|i| gj::compact(i, true)).collect();
    gj::join(&compacted)
}

pub(crate) fn request_has_native_x_search(body: &[u8]) -> bool {
    // `tools.#(type=="x_search")`, then any `additional_tools` input item declaring it.
    let has =
        |tools: &Res<'_>| tools.is_array() && tools.array().iter().any(|t| &*t.get("type").bytes() == b"x_search");
    if has(&gj::get(body, "tools")) {
        return true;
    }
    let input = gj::get(body, "input");
    input.is_array()
        && input
            .array()
            .iter()
            .any(|item| &*item.get("type").bytes() == b"additional_tools" && has(&item.get("tools")))
}

/// `qualifyXAINamespaceToolName`.
pub(crate) fn qualify(namespace: &str, tool: &str) -> String {
    let (namespace, tool) = (namespace.trim(), tool.trim());
    if namespace.is_empty() || tool.is_empty() || tool.starts_with("mcp__") {
        return tool.to_owned();
    }
    let prefix = if namespace.ends_with("__") {
        namespace.to_owned()
    } else {
        format!("{namespace}__")
    };
    if tool.starts_with(&prefix) {
        tool.to_owned()
    } else {
        format!("{prefix}{tool}")
    }
}

/// `xaiNamespaceRestorer`: flattened (`ns__tool`) or folded (dispatcher) calls back to
/// the client's namespaced shape.
pub(crate) struct NamespaceRestorer {
    refs: NamespaceRefs,
    dispatcher_items: HashMap<String, String>,
}

impl NamespaceRestorer {
    pub(crate) fn new(refs: NamespaceRefs) -> Self {
        Self {
            refs,
            dispatcher_items: HashMap::new(),
        }
    }

    pub(crate) fn restore(&mut self, mut data: Vec<u8>) -> Vec<u8> {
        if self.refs.is_empty() || data.is_empty() || !gj::valid(&data) {
            return data;
        }
        match &*gj::get(&data, "type").bytes() {
            b"response.output_item.added" => {
                let item = gj::get(&data, "item");
                if &*item.get("type").bytes() == b"function_call" {
                    let name = text(&item.get("name"));
                    let id = text(&item.get("id"));
                    if let Some(r) = self.refs.get(&name).filter(|r| r.dispatcher) {
                        if !id.is_empty() {
                            self.dispatcher_items.insert(id, r.namespace.clone());
                        }
                        let namespace = r.namespace.clone();
                        gj::set_str(&mut data, "item.namespace", namespace);
                    }
                }
                data
            }
            b"response.function_call_arguments.done" => {
                let id = text(&gj::get(&data, "item_id"));
                if let Some(namespace) = self.dispatcher_items.get(&id) {
                    let raw = gj::get(&data, "arguments").str().into_owned();
                    if let Some((_, child)) = unwrap_dispatcher_arguments(&raw, namespace, &self.refs) {
                        gj::set_str(&mut data, "arguments", child);
                    }
                }
                data
            }
            _ => {
                data = restore_at_path(data, "item", &self.refs);
                let output = gj::get(&data, "response.output");
                if output.is_array() {
                    let count = output.array().len();
                    for index in 0..count {
                        data = restore_at_path(data, &format!("response.output.{index}"), &self.refs);
                    }
                }
                data
            }
        }
    }
}

/// `restoreAtPath`.
fn restore_at_path(mut data: Vec<u8>, path: &str, refs: &NamespaceRefs) -> Vec<u8> {
    if &*gj::get(&data, &format!("{path}.type")).bytes() != b"function_call" {
        return data;
    }
    let qualified = text(&gj::get(&data, &format!("{path}.name")));
    let Some(r) = refs.get(&qualified) else {
        return data;
    };
    if r.dispatcher {
        let raw = gj::get(&data, &format!("{path}.arguments")).str().into_owned();
        let (child_name, child_args) = match unwrap_dispatcher_arguments(&raw, &r.namespace, refs) {
            Some((name, args)) => (name, args),
            None => (r.name.clone(), Vec::new()),
        };
        if !gj::set_str(&mut data, &format!("{path}.namespace"), &r.namespace) {
            return data;
        }
        if !child_name.is_empty() {
            gj::set_str(&mut data, &format!("{path}.name"), child_name);
        }
        if !child_args.is_empty() {
            gj::set_str(&mut data, &format!("{path}.arguments"), child_args);
        }
        return data;
    }
    let original = data.clone();
    if !gj::set_str(&mut data, &format!("{path}.name"), &r.name)
        || !gj::set_str(&mut data, &format!("{path}.namespace"), &r.namespace)
    {
        return original;
    }
    data
}

/// `unwrapXAIDispatcherArguments`: the child tool name and arguments of a folded call.
pub(crate) fn unwrap_dispatcher_arguments(
    raw: &str,
    namespace: &str,
    refs: &NamespaceRefs,
) -> Option<(String, Vec<u8>)> {
    let raw = raw.as_bytes();
    if !gj::valid(raw) {
        return None;
    }
    let parsed = gj::parse(raw);
    let name = parsed.get("name");
    if name.kind != Kind::String {
        return None;
    }
    let child = text(&name);
    if child.is_empty() {
        return None;
    }
    if !namespace.is_empty() {
        if refs.get(&qualify(namespace, &child)).is_some_and(|r| r.dispatcher) {
            return None;
        }
    } else {
        let is_child = refs
            .values()
            .any(|r| r.dispatcher && (r.name == child || r.namespace == child));
        if !is_child && !parsed.get("arguments").exists() {
            return None;
        }
    }
    let arguments = parsed.get("arguments");
    let mut args = if arguments.exists() {
        if arguments.kind == Kind::String {
            arguments.bytes().into_owned()
        } else {
            arguments.raw().to_vec()
        }
    } else {
        match gj::try_delete(raw, "name") {
            Ok(cleaned) if !cleaned.is_empty() && cleaned != b"{}" => cleaned,
            _ => b"{}".to_vec(),
        }
    };
    if args.is_empty() {
        args = b"{}".to_vec();
    }
    Some((child, args))
}

/// `restoreXAIClientWebSearchName`: the client's `web_search` function back from its alias.
pub(crate) fn restore_web_search_name(mut data: Vec<u8>, alias: &str) -> Vec<u8> {
    if alias.is_empty() || !data.windows(alias.len()).any(|w| w == alias.as_bytes()) || !gj::valid(&data) {
        return data;
    }
    if text(&gj::get(&data, "item.namespace")).is_empty() {
        for path in ["item.name", "item.function.name"] {
            if text(&gj::get(&data, path)) == alias {
                gj::set_str(&mut data, path, WEB_SEARCH);
            }
        }
    }
    for base in ["response.output", "output"] {
        let output = gj::get(&data, base);
        if !output.is_array() {
            continue;
        }
        let items: Vec<(bool, String, String)> = output
            .array()
            .iter()
            .map(|item| {
                (
                    !text(&item.get("namespace")).is_empty(),
                    text(&item.get("name")),
                    text(&item.get("function.name")),
                )
            })
            .collect();
        for (index, (namespaced, name, function_name)) in items.into_iter().enumerate() {
            if namespaced {
                continue;
            }
            if name == alias {
                gj::set_str(&mut data, &format!("{base}.{index}.name"), WEB_SEARCH);
            }
            if function_name == alias {
                gj::set_str(&mut data, &format!("{base}.{index}.function.name"), WEB_SEARCH);
            }
        }
    }
    if text(&gj::get(&data, "namespace")).is_empty() && text(&gj::get(&data, "name")) == alias {
        gj::set_str(&mut data, "name", WEB_SEARCH);
    }
    data
}

/// `sanitizeXAIInputEncryptedContent`: reasoning blobs that are not Grok encrypted
/// content lose `encrypted_content`; such compaction items are dropped.
pub(crate) fn sanitize_input_encrypted_content(body: Vec<u8>) -> Vec<u8> {
    let input = gj::get(&body, "input");
    if !input.is_array() {
        return body;
    }
    let mut items = Vec::new();
    let mut changed = false;
    for item in input.array() {
        let item_type = text(&item.get("type"));
        if item_type != "reasoning" && item_type != "compaction" {
            items.push(item.raw().to_vec());
            continue;
        }
        let encrypted = item.get("encrypted_content");
        if !encrypted.exists() {
            items.push(item.raw().to_vec());
            continue;
        }
        let invalid = match encrypted.kind {
            Kind::String => ck_wire::signature::inspect_grok_encrypted_content(&*encrypted.bytes()).is_err(),
            _ => true,
        };
        if !invalid {
            items.push(item.raw().to_vec());
            continue;
        }
        if item_type == "compaction" {
            changed = true;
            continue;
        }
        match gj::try_delete(item.raw(), "encrypted_content") {
            Ok(next) => {
                items.push(next);
                changed = true;
            }
            Err(_) => items.push(item.raw().to_vec()),
        }
    }
    if !changed {
        return body;
    }
    let raw = marshal_raw(&items);
    let Ok(updated) = gj::try_set_raw(&body, "input", raw) else {
        return body;
    };
    merge_adjacent_reasoning_summaries(updated)
}

/// `normalizeXAIInputReasoningItems`: null `content` and `encrypted_content` removed from
/// reasoning input items, then adjacent summaries merged.
pub(crate) fn normalize_input_reasoning_items(body: Vec<u8>) -> Vec<u8> {
    let input = gj::get(&body, "input");
    if !input.is_array() {
        return body;
    }
    let reasoning: Vec<usize> = input
        .array()
        .iter()
        .enumerate()
        .filter(|(_, item)| &*item.get("type").bytes() == b"reasoning")
        .map(|(i, _)| i)
        .collect();
    let mut updated = body.clone();
    for i in reasoning {
        for field in ["content", "encrypted_content"] {
            let path = format!("input.{i}.{field}");
            let value = gj::get(&updated, &path);
            if value.exists() && value.kind == Kind::Null {
                match gj::try_delete(&updated, &path) {
                    Ok(next) => updated = next,
                    Err(_) => return body,
                }
            }
        }
    }
    merge_adjacent_reasoning_summaries(updated)
}

/// `mergeAdjacentXAIInputReasoningSummaries`: a reasoning item carrying only a summary
/// joins the previous reasoning item's summary.
fn merge_adjacent_reasoning_summaries(body: Vec<u8>) -> Vec<u8> {
    let input = gj::get(&body, "input");
    if !input.is_array() {
        return body;
    }
    let mut changed = false;
    let mut items: Vec<Vec<u8>> = Vec::new();
    for item in input.array() {
        if let Some(previous) = items.last()
            && can_merge_summary(previous, &item)
            && let Some(merged) = append_summary(previous, &item.get("summary").array())
        {
            *items.last_mut().expect("present") = merged;
            changed = true;
            continue;
        }
        items.push(item.raw().to_vec());
    }
    if !changed {
        return body;
    }
    gj::try_set_raw(&body, "input", marshal_raw(&items)).unwrap_or(body)
}

fn can_merge_summary(previous: &[u8], current: &Res<'_>) -> bool {
    let previous = gj::parse(previous);
    if &*previous.get("type").bytes() != b"reasoning" || &*current.get("type").bytes() != b"reasoning" {
        return false;
    }
    if !previous.get("summary").is_array() || !current.get("summary").is_array() {
        return false;
    }
    if current.get("summary").array().is_empty() {
        return false;
    }
    current
        .map()
        .iter()
        .all(|(name, _)| name.as_slice() == b"type" || name.as_slice() == b"summary")
}

fn append_summary(previous: &[u8], summary: &[Res<'_>]) -> Option<Vec<u8>> {
    let existing = gj::get(previous, "summary");
    if !existing.is_array() {
        return None;
    }
    let next = existing.array().len();
    let mut updated = previous.to_vec();
    for (i, item) in summary.iter().enumerate() {
        updated = gj::try_set_raw(&updated, &format!("summary.{}", next + i), item.raw()).ok()?;
    }
    Some(updated)
}

/// `xaiCollectOutputItemDone` and `xaiPatchCompletedOutput`: completed responses get
/// the streamed output items when their own output is empty.
#[derive(Default)]
pub(crate) struct OutputItems {
    by_index: BTreeMap<i64, Vec<u8>>,
    fallback: Vec<Vec<u8>>,
}

impl OutputItems {
    pub(crate) fn collect(&mut self, event: &[u8]) {
        let item = gj::get(event, "item");
        if !item.exists() || item.kind != Kind::Json {
            return;
        }
        let index = gj::get(event, "output_index");
        if index.exists() {
            self.by_index.insert(index.int(), item.raw().to_vec());
        } else {
            self.fallback.push(item.raw().to_vec());
        }
    }

    pub(crate) fn patch(&self, event: &[u8]) -> Vec<u8> {
        let mut event = ensure_responses_usage_details(event);
        let output = gj::get(&event, "response.output");
        let empty = !output.is_array() || output.array().is_empty();
        if !empty || (self.by_index.is_empty() && self.fallback.is_empty()) {
            return event;
        }
        let items: Vec<&Vec<u8>> = self.by_index.values().chain(self.fallback.iter()).collect();
        gj::set_raw(&mut event, "response.output", gj::join(&items));
        event
    }
}

/// `xaiFreeUsageExhaustedCooldown`.
const FREE_USAGE_COOLDOWN: Duration = Duration::from_secs(24 * 3600);

/// `xaiStatusErr`: a 403 for an invalidated access token becomes 401 (so the refresh and
/// retry runs) and free-tier exhaustion carries a 24-hour retry hint.
pub(crate) fn status_error(status: u16, body: &[u8]) -> ExecError {
    let message = String::from_utf8_lossy(body).into_owned();
    if body.is_empty() {
        return crate::openai_compat::status_err(status, message).from_upstream();
    }
    if status == 403 && is_bad_credentials(body) {
        return crate::openai_compat::status_err(401, message).from_upstream();
    }
    let mut error = crate::openai_compat::status_err(status, message).from_upstream();
    if status != 429 {
        return error;
    }
    let code = gj::get(body, "code").str().go_lower();
    let mut msg = gj::get(body, "error").str().go_lower();
    if msg.is_empty() {
        msg = String::from_utf8_lossy(body).go_lower();
    }
    if code.contains("free-usage-exhausted")
        || msg.contains("free-usage-exhausted")
        || msg.contains("included free usage")
    {
        error.retry_after = Some(FREE_USAGE_COOLDOWN);
    }
    error
}

/// `isXAIBadCredentialsBody`.
pub(crate) fn is_bad_credentials(body: &[u8]) -> bool {
    if ["code", "error.code", "body.error.code"]
        .iter()
        .any(|p| gj::get(body, p).str().go_lower().contains("bad-credentials"))
    {
        return true;
    }
    if ["error", "error.message", "message", "body.error", "body.error.message"]
        .iter()
        .any(|p| {
            gj::get(body, p)
                .str()
                .go_lower()
                .contains("access token could not be validated")
        })
    {
        return true;
    }
    let raw = String::from_utf8_lossy(body).go_lower();
    raw.contains("bad-credentials") || raw.contains("access token could not be validated")
}

// --- usage observation (helps/usage_helpers.go) ------------------------------------------

/// `ParseCodexUsage`'s `ok`: `response.usage` carries OpenAI-style token fields
/// (`hasOpenAIStyleUsageTokenFields`), or the event names a service tier.
pub(crate) fn codex_usage_ok(event: &[u8]) -> bool {
    let node = gj::get(event, "response.usage");
    let fields = [
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
    ];
    if node.is_object() && fields.iter().any(|f| node.get(f).exists()) {
        return true;
    }
    // extractResponseServiceTier.
    gj::std_valid(event)
        && ["response.service_tier", "service_tier", "interaction.service_tier"]
            .iter()
            .any(|p| !gj::get(event, p).str().trim().is_empty())
}

/// The terminal Responses events whose usage the shared Codex line parser counts.
const TERMINAL_EVENTS: [&str; 3] = ["response.completed", "response.incomplete", "response.done"];

/// An event as one Go path's usage observation sees it. Each xAI path observes usage on
/// its own terminal events (HTTP: completed and incomplete; WebSocket: completed and
/// done) while observing the response model on every event, so a terminal event outside
/// `observed` is reported without its usage and service tier.
pub(crate) fn usage_line<'a>(event: &'a [u8], observed: &[&str]) -> std::borrow::Cow<'a, [u8]> {
    let kind = gj::get(event, "type").str().into_owned();
    if !TERMINAL_EVENTS.contains(&kind.as_str()) || observed.contains(&kind.as_str()) {
        return std::borrow::Cow::Borrowed(event);
    }
    let mut out = event.to_vec();
    for path in [
        "response.usage",
        "response.service_tier",
        "service_tier",
        "interaction.service_tier",
    ] {
        gj::delete(&mut out, path);
    }
    std::borrow::Cow::Owned(out)
}
