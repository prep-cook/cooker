//! What the Codex backend requires of a Responses request (`ConvertOpenAIResponsesRequestToCodex`,
//! internal/translator/codex/openai/responses), applied by the executor to its own encoded
//! upload for every client protocol: `store` off, parallel tool calls on, encrypted reasoning
//! included, the sampling and limit members the backend refuses removed, the service tier
//! normalized, prompt cache breakpoints stripped, system input as developer input, and the
//! preview web search tools under their current name.
//!
//! The Responses request egress always writes `input` as an array and `stream` as the call
//! needs, and every request codec refuses blank call arguments, so Go's string-input wrap,
//! `stream` rule and empty-arguments rewrite have no counterpart here.

use ck_core::gostr::GoStr;
use ck_wire::json::{self as gj, Kind};

/// The Codex backend's rules on a Responses `upload`.
pub(crate) fn require(mut raw: Vec<u8>) -> Vec<u8> {
    set_required_bool(&mut raw, "store", false);
    set_required_bool(&mut raw, "parallel_tool_calls", true);
    set_required_include(&mut raw);
    delete_fields(
        &mut raw,
        &["max_output_tokens", "max_completion_tokens", "temperature", "top_p"],
    );
    service_tier(&mut raw);
    delete_fields(
        &mut raw,
        &["truncation", "prompt_cache_options", "prompt_cache_retention"],
    );
    delete_fields(&mut raw, &["user"]);
    normalize(raw)
}

/// Shared input/tool normalization without changing generation controls.
pub(crate) fn normalize(mut raw: Vec<u8>) -> Vec<u8> {
    raw = strip_cache_breakpoints(raw);
    delete_fields(&mut raw, &["context_management"]);
    raw = system_role_to_developer(raw);
    normalize_builtin_tools(raw)
}

fn set_required_bool(raw: &mut Vec<u8>, path: &str, value: bool) {
    let current = gj::get(raw, path).kind;
    if (value && current == Kind::True) || (!value && current == Kind::False) {
        return;
    }
    gj::set_bool(raw, path, value);
}

/// Go `setCodexRequiredInclude`: `reasoning.encrypted_content`, plus
/// `web_search_call.action.sources` when the client asked for it.
fn set_required_include(raw: &mut Vec<u8>) {
    const REASONING: &[u8] = b"reasoning.encrypted_content";
    const SOURCES: &[u8] = b"web_search_call.action.sources";
    let current = gj::get(raw, "include");
    let mut sources = false;
    if current.is_array() {
        let values = current.array();
        let is = |i: usize, want: &[u8]| values[i].kind == Kind::String && &*values[i].bytes() == want;
        sources = (0..values.len()).any(|i| is(i, SOURCES));
        if (!sources && values.len() == 1 && is(0, REASONING))
            || (sources && values.len() == 2 && is(0, REASONING) && is(1, SOURCES))
        {
            return;
        }
    }
    let encoded = if sources {
        r#"["reasoning.encrypted_content","web_search_call.action.sources"]"#
    } else {
        r#"["reasoning.encrypted_content"]"#
    };
    gj::set_raw(raw, "include", encoded);
}

/// `priority` (also spelled `fast`) and `ultrafast` are the tiers the backend serves; any
/// other value is dropped.
fn service_tier(raw: &mut Vec<u8>) {
    let tier = gj::get(raw, "service_tier");
    if !tier.exists() {
        return;
    }
    let canonical = match tier.kind {
        Kind::String => match tier.str().trim().go_lower().as_str() {
            "priority" | "fast" => Some("priority"),
            "ultrafast" => Some("ultrafast"),
            _ => None,
        },
        _ => None,
    };
    match canonical {
        Some(value) if &*tier.bytes() != value.as_bytes() => {
            gj::set_str(raw, "service_tier", value);
        }
        Some(_) => {}
        None => delete_fields(raw, &["service_tier"]),
    }
}

fn delete_fields(raw: &mut Vec<u8>, paths: &[&str]) {
    for path in paths {
        if gj::get(raw, path).exists() {
            gj::delete(raw, path);
        }
    }
}

/// Rebuilds `input` with `edit` applied to each item; unchanged when nothing changed.
fn rebuild_input(raw: Vec<u8>, mut edit: impl FnMut(&gj::Res<'_>, &mut Vec<u8>) -> bool) -> Vec<u8> {
    let input = gj::get(&raw, "input");
    if !input.is_array() {
        return raw;
    }
    let items = input.array();
    let mut changed = false;
    let mut rebuilt = Vec::with_capacity(items.len());
    for item in &items {
        let mut item_raw = item.raw.to_vec();
        changed |= edit(item, &mut item_raw);
        rebuilt.push(item_raw);
    }
    if !changed {
        return raw;
    }
    let mut out = raw.clone();
    gj::set_raw(&mut out, "input", gj::join(&rebuilt));
    out
}

fn strip_cache_breakpoints(raw: Vec<u8>) -> Vec<u8> {
    if !raw.windows(25).any(|w| w == b"\"prompt_cache_breakpoint\"") {
        return raw;
    }
    rebuild_input(raw, |item, item_raw| {
        let mut changed = false;
        for path in ["content", "output"] {
            let array = item.get(path);
            if !array.is_array() {
                continue;
            }
            let parts = array.array();
            if !parts.iter().any(|p| p.get("prompt_cache_breakpoint").exists()) {
                continue;
            }
            let mut part_changed = false;
            let rebuilt: Vec<Vec<u8>> = parts
                .iter()
                .map(|p| {
                    let mut part = p.raw.to_vec();
                    if p.get("prompt_cache_breakpoint").exists() {
                        part_changed |= gj::delete(&mut part, "prompt_cache_breakpoint");
                    }
                    part
                })
                .collect();
            if part_changed && gj::set_raw(item_raw, path, gj::join(&rebuilt)) {
                changed = true;
            }
        }
        if item.get("prompt_cache_breakpoint").exists() && gj::delete(item_raw, "prompt_cache_breakpoint") {
            changed = true;
        }
        changed
    })
}

/// convertSystemRoleToDeveloper. Go re-marshals the input through `[]json.RawMessage`,
/// which compacts every item and HTML-escapes it, and gives up if any item is invalid.
fn system_role_to_developer(raw: Vec<u8>) -> Vec<u8> {
    let input = gj::get(&raw, "input");
    if !input.is_array() {
        return raw;
    }
    let items = input.array();
    let system = |item: &gj::Res<'_>| item.is_object() && &*item.get("role").bytes() == b"system";
    if !items.iter().any(system) {
        return raw;
    }
    let mut rebuilt = Vec::with_capacity(items.len());
    for item in &items {
        let mut item_raw = item.raw.to_vec();
        if system(item) && !gj::set_raw(&mut item_raw, "role", r#""developer""#) {
            return raw;
        }
        if !gj::valid(&item_raw) {
            return raw;
        }
        rebuilt.push(item_raw);
    }
    let mut out = raw.clone();
    gj::set_raw(&mut out, "input", gj::compact(&gj::join(&rebuilt), true));
    out
}

fn builtin_tool_type(kind: &[u8]) -> Option<&'static str> {
    matches!(kind, b"web_search_preview" | b"web_search_preview_2025_03_11").then_some("web_search")
}

fn normalize_builtin_tools(raw: Vec<u8>) -> Vec<u8> {
    let mut raw = normalize_tool_array(raw, "tools");
    if let Some(kind) = builtin_tool_type(&gj::get(&raw, "tool_choice.type").bytes()) {
        gj::set_str(&mut raw, "tool_choice.type", kind);
    }
    normalize_tool_array(raw, "tool_choice.tools")
}

fn normalize_tool_array(raw: Vec<u8>, path: &str) -> Vec<u8> {
    let tools = gj::get(&raw, path);
    if !tools.is_array() {
        return raw;
    }
    let mut changed = false;
    let mut items = vec![];
    tools.each(|_, tool| {
        let mut item = tool.raw.to_vec();
        if let Some(kind) = builtin_tool_type(&tool.get("type").bytes())
            && gj::set_str(&mut item, "type", kind)
        {
            changed = true;
        }
        items.push(item);
        true
    });
    if !changed {
        return raw;
    }
    let mut out = raw.clone();
    gj::set_raw(&mut out, path, gj::join(&items));
    out
}
