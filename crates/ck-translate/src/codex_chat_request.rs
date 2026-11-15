//! OpenAI Chat Completions request -> Codex (OpenAI Responses) request
//! (internal/translator/codex/openai/chat-completions/codex_openai_request.go).

use crate::{
    Registered, apply_patch, codex_chat_response as response,
    common::{UserTurnDrops, go_lower, sanitize, trim_space},
};
use ck_wire::json::{self as gj, AnyValue, Kind, Res};
use std::collections::{HashMap, HashSet};
#[path = "codex_chat_input.rs"]
mod input;
use crate::responses_policy::{Policy, meta_controls};
use ck_core::format::Format;

pub static PAIR: Registered = registered!(
    OpenAI -> Codex,
    request: |ctx, body| convert_with_policy(ctx.model, body, ctx.stream, Policy::Codex),
    non_stream: response::non_stream,
    go_stream: response::go_stream,
    token_count: None,
);

const NAME_LIMIT: usize = 64;

/// shortenNameIfNeeded: sanitized, then at most 64 bytes, keeping `mcp__` plus the last
/// `__` segment when possible.
pub(crate) fn shorten_name(name: &[u8]) -> Vec<u8> {
    let sanitized = sanitize(name);
    if sanitized.len() <= NAME_LIMIT {
        return sanitized;
    }
    if sanitized.starts_with(b"mcp__")
        && let Some(idx) = sanitized.windows(2).rposition(|w| w == b"__")
        && idx > 0
    {
        let mut candidate = [&b"mcp__"[..], &sanitized[idx + 2..]].concat();
        candidate.truncate(NAME_LIMIT);
        return candidate;
    }
    sanitized[..NAME_LIMIT].to_vec()
}

/// collectRequestToolNames: declared tools, the tool choice, then assistant tool calls.
pub(crate) fn request_tool_names(raw: &[u8]) -> Vec<Vec<u8>> {
    let mut names: Vec<Vec<u8>> = vec![];
    let mut add = |name: Vec<u8>| {
        if !name.is_empty() && !names.contains(&name) {
            names.push(name);
        }
    };
    let tools = gj::get(raw, "tools");
    if tools.is_array() {
        for tool in tools.array() {
            match tool.get("type").bytes().as_ref() {
                b"function" => add(tool.get("function.name").bytes().into_owned()),
                b"custom" => add(tool.get("name").bytes().into_owned()),
                _ => {}
            }
        }
    }
    let choice = gj::get(raw, "tool_choice");
    if choice.is_object() {
        match choice.get("type").bytes().as_ref() {
            b"function" => {
                let mut name = choice.get("function.name").bytes().into_owned();
                if name.is_empty() {
                    name = choice.get("name").bytes().into_owned();
                }
                add(name);
            }
            b"custom" => add(choice.get("name").bytes().into_owned()),
            _ => {}
        }
    }
    let messages = gj::get(raw, "messages");
    if messages.is_array() {
        for message in messages.array() {
            if message.get("role").bytes().as_ref() != b"assistant" {
                continue;
            }
            let calls = message.get("tool_calls");
            if calls.is_array() {
                for call in calls.array() {
                    let name = call.get("function.name").bytes().into_owned();
                    if name.is_empty() {
                        add(call.get("custom.name").bytes().into_owned());
                    } else {
                        add(name);
                    }
                }
            }
        }
    }
    names
}

/// buildShortNameMap: unique short names, suffixing `_1`, `_2`, ... on collisions.
pub(crate) fn short_name_map(names: &[Vec<u8>]) -> HashMap<Vec<u8>, Vec<u8>> {
    let mut used: HashSet<Vec<u8>> = HashSet::new();
    let mut map = HashMap::new();
    for name in names {
        let candidate = shorten_name(name);
        let unique = if used.contains(&candidate) {
            (1..)
                .map(|i| {
                    let suffix = format!("_{i}");
                    let allowed = NAME_LIMIT.saturating_sub(suffix.len());
                    let mut tmp = candidate[..candidate.len().min(allowed)].to_vec();
                    tmp.extend_from_slice(suffix.as_bytes());
                    tmp
                })
                .find(|tmp| !used.contains(tmp))
                .unwrap()
        } else {
            candidate
        };
        used.insert(unique.clone());
        map.insert(name.clone(), unique);
    }
    map
}

/// normalizeCodexServiceTier.
pub(crate) fn service_tier(r: &Res<'_>) -> &'static str {
    if r.kind != Kind::String {
        return "";
    }
    match go_lower(trim_space(&r.s)).as_slice() {
        b"fast" | b"priority" => "priority",
        b"ultrafast" => "ultrafast",
        _ => "",
    }
}

struct PendingCall {
    call_id: Vec<u8>,
    source_id: Vec<u8>,
    custom: bool,
    consumed: bool,
}

pub(crate) fn convert_with_policy(
    model: &str,
    raw: &[u8],
    stream: bool,
    policy: Policy,
) -> Result<Vec<u8>, crate::Error> {
    let mut out = br#"{"instructions":""}"#.to_vec();
    gj::set_bool(&mut out, "stream", stream);
    controls(&mut out, raw, policy);
    gj::set_str(&mut out, "model", model);
    let declarations = gj::get(raw, "tools");
    let tool_list = declarations.array();
    let names = input::Names::new(raw, &tool_list);
    let items = input::convert(raw, &names)?;
    if items.is_empty() {
        gj::set_raw(&mut out, "input", b"[]");
    } else {
        gj::set_items(&mut out, "input", &items);
    }
    out = text_format(out, raw);
    out = tools(out, &declarations, &tool_list, &names);
    out = tool_choice(out, raw, &names);
    gj::set_bool(&mut out, "store", false);
    Ok(out)
}

fn controls(out: &mut Vec<u8>, raw: &[u8], policy: Policy) {
    let effort = gj::get(raw, "reasoning_effort");
    if effort.exists() {
        AnyValue::from_res(&effort).set(out, "reasoning.effort");
    } else if policy.codex() {
        gj::set_str(out, "reasoning.effort", "medium");
    }
    if policy.codex() {
        let tier = service_tier(&gj::get(raw, "service_tier"));
        if !tier.is_empty() {
            gj::set_str(out, "service_tier", tier);
        }
        gj::set_bool(out, "parallel_tool_calls", true);
        gj::set_strs(out, "include", &["reasoning.encrypted_content"]);
    } else {
        meta_controls(out, raw, Format::OpenAI);
    }
}

fn text_format(mut out: Vec<u8>, raw: &[u8]) -> Vec<u8> {
    let format = gj::get(raw, "response_format");
    let text = gj::get(raw, "text");
    if format.exists() {
        if !gj::get(&out, "text").exists() {
            gj::set_raw(&mut out, "text", b"{}");
        }
        match format.get("type").bytes().as_ref() {
            b"text" => {
                gj::set_str(&mut out, "text.format.type", "text");
            }
            b"json_schema" => {
                let schema = format.get("json_schema");
                if schema.exists() {
                    gj::set_str(&mut out, "text.format.type", "json_schema");
                    for (from, to) in [("name", "text.format.name"), ("strict", "text.format.strict")] {
                        let v = schema.get(from);
                        if v.exists() {
                            AnyValue::from_res(&v).set(&mut out, to);
                        }
                    }
                    let s = schema.get("schema");
                    if s.exists() {
                        gj::set_raw(&mut out, "text.format.schema", &s.raw);
                    }
                }
            }
            _ => {}
        }
        let verbosity = text.get("verbosity");
        if text.exists() && verbosity.exists() {
            AnyValue::from_res(&verbosity).set(&mut out, "text.verbosity");
        }
    } else if text.exists() {
        let verbosity = text.get("verbosity");
        if verbosity.exists() {
            if !gj::get(&out, "text").exists() {
                gj::set_raw(&mut out, "text", b"{}");
            }
            AnyValue::from_res(&verbosity).set(&mut out, "text.verbosity");
        }
    }
    out
}

fn tools(mut out: Vec<u8>, tools: &Res<'_>, tool_list: &[Res<'_>], names: &input::Names) -> Vec<u8> {
    let short = |name: &[u8]| names.short(name);

    if tools.is_array() && !tool_list.is_empty() {
        let mut tool_items = vec![];
        for t in tool_list {
            let kind = t.get("type").bytes().into_owned();
            if kind == b"custom" {
                let mut item = t.raw.to_vec();
                gj::set_str(&mut item, "name", short(&t.get("name").bytes()));
                tool_items.push(item);
                continue;
            }
            if !kind.is_empty() && kind != b"function" && t.is_object() {
                tool_items.push(t.raw.to_vec());
                continue;
            }
            if kind != b"function" {
                continue;
            }
            let mut item = b"{}".to_vec();
            gj::set_str(&mut item, "type", "function");
            let f = t.get("function");
            if f.exists() {
                let name = f.get("name");
                if name.exists() {
                    gj::set_str(&mut item, "name", short(&name.bytes()));
                }
                let description = f.get("description");
                if description.exists() {
                    AnyValue::from_res(&description).set(&mut item, "description");
                }
                let parameters = f.get("parameters");
                if parameters.exists() {
                    gj::set_raw(&mut item, "parameters", &parameters.raw);
                }
                let strict = f.get("strict");
                if strict.exists() {
                    AnyValue::from_res(&strict).set(&mut item, "strict");
                } else {
                    gj::set_bool(&mut item, "strict", false);
                }
            }
            tool_items.push(item);
        }
        gj::set_raw(&mut out, "tools", gj::join(&tool_items));
    }
    out
}

fn tool_choice(mut out: Vec<u8>, raw: &[u8], names: &input::Names) -> Vec<u8> {
    let short = |name: &[u8]| names.short(name);

    let choice = gj::get(raw, "tool_choice");
    if choice.kind == Kind::String {
        gj::set_str(&mut out, "tool_choice", &choice.s);
    } else if choice.is_object() {
        let mut kind = choice.get("type").bytes().into_owned();
        if kind == b"function" || kind == b"custom" {
            let mut name = choice.get("name").bytes().into_owned();
            if kind == b"function" {
                name = choice.get("function.name").bytes().into_owned();
                if names.custom(&name) {
                    kind = b"custom".to_vec();
                }
            }
            if !name.is_empty() {
                name = short(&name);
            }
            let mut tc = b"{}".to_vec();
            gj::set_str(&mut tc, "type", &kind);
            if !name.is_empty() {
                gj::set_str(&mut tc, "name", &name);
            }
            gj::set_raw(&mut out, "tool_choice", tc);
        } else if !kind.is_empty() {
            gj::set_raw(&mut out, "tool_choice", &choice.raw);
        }
    }
    out
}

/// `shortenCodexCallIDIfNeeded`: an ID over 64 bytes keeps its first bytes (cut without
/// regard to UTF-8, as Go slices the string) and ends in `_` plus 8 bytes of its SHA-256.
fn shorten_call_id(id: &[u8]) -> Vec<u8> {
    const LIMIT: usize = 64;
    if id.len() <= LIMIT {
        return id.to_vec();
    }
    use sha2::{Digest, Sha256};
    with_hash_suffix(id, &Sha256::digest(id))
}

/// The first 47 bytes of `id` (over 64 bytes), `_` and the first 8 bytes of `sum` in hex.
fn with_hash_suffix(id: &[u8], sum: &[u8]) -> Vec<u8> {
    let suffix: String = std::iter::once("_".to_owned())
        .chain(sum[..8].iter().map(|b| format!("{b:02x}")))
        .collect();
    [&id[..64 - suffix.len()], suffix.as_bytes()].concat()
}

/// setToolCallOutputContent.
fn set_tool_output(output: &mut Vec<u8>, content: &Res<'_>) {
    if content.kind == Kind::String {
        let structured = gj::parse(&content.s).into_owned();
        if has_image_part(&structured) {
            return set_tool_output(output, &structured);
        }
        gj::set_str(output, "output", &content.s);
    } else if content.is_array() {
        let parts: Vec<Vec<u8>> = content.array().iter().map(output_part).collect();
        gj::set_raw(output, "output", gj::join(&parts));
    } else {
        let fallback = if content.raw.is_empty() {
            content.bytes().into_owned()
        } else {
            content.raw.to_vec()
        };
        gj::set_str(output, "output", fallback);
    }
}

fn input_text(text: &[u8]) -> Vec<u8> {
    let mut part = b"{}".to_vec();
    gj::set_str(&mut part, "type", "input_text");
    gj::set_str(&mut part, "text", text);
    part
}

fn fallback_part(item: &Res<'_>) -> Vec<u8> {
    let text = if item.raw.is_empty() {
        item.bytes().into_owned()
    } else {
        item.raw.to_vec()
    };
    input_text(&text)
}

/// toolOutputContentPart.
fn output_part(item: &Res<'_>) -> Vec<u8> {
    let kind = item.get("type").bytes().into_owned();
    match kind.as_slice() {
        b"text" | b"input_text" | b"output_text" => input_text(&item.get("text").bytes()),
        b"image_url" | b"input_image" => {
            let input = kind == b"input_image";
            let (url, file_id, detail) = if input {
                (item.get("image_url"), item.get("file_id"), item.get("detail"))
            } else {
                (
                    item.get("image_url.url"),
                    item.get("image_url.file_id"),
                    item.get("image_url.detail"),
                )
            };
            let (url, file_id, detail) = (url.bytes(), file_id.bytes(), detail.bytes());
            if url.is_empty() && file_id.is_empty() {
                return fallback_part(item);
            }
            let mut part = b"{}".to_vec();
            gj::set_str(&mut part, "type", "input_image");
            if !url.is_empty() {
                gj::set_str(&mut part, "image_url", &url);
            }
            if !file_id.is_empty() {
                gj::set_str(&mut part, "file_id", &file_id);
            }
            if !detail.is_empty() {
                gj::set_str(&mut part, "detail", &detail);
            }
            part
        }
        b"file" => input_file_part(item).unwrap_or_else(|| fallback_part(item)),
        _ => fallback_part(item),
    }
}

/// codexInputFilePart: a chat `file` part as an input_file, or None when it names no
/// file_id, file_data or file_url.
fn input_file_part(item: &Res<'_>) -> Option<Vec<u8>> {
    let (id, data, url) = (
        item.get("file.file_id").bytes(),
        item.get("file.file_data").bytes(),
        item.get("file.file_url").bytes(),
    );
    if id.is_empty() && data.is_empty() && url.is_empty() {
        return None;
    }
    let mut part = b"{}".to_vec();
    gj::set_str(&mut part, "type", "input_file");
    for (key, value) in [("file_id", &id), ("file_data", &data), ("file_url", &url)] {
        if !value.is_empty() {
            gj::set_str(&mut part, key, value.as_ref());
        }
    }
    let filename = item.get("file.filename").bytes();
    if !filename.is_empty() {
        gj::set_str(&mut part, "filename", &filename);
    }
    Some(part)
}

/// hasToolOutputImagePart.
fn has_image_part(content: &Res<'_>) -> bool {
    content.is_array()
        && content
            .array()
            .iter()
            .any(|item| match item.get("type").bytes().as_ref() {
                b"image_url" => {
                    !item.get("image_url.url").bytes().is_empty() || !item.get("image_url.file_id").bytes().is_empty()
                }
                b"input_image" => !item.get("image_url").bytes().is_empty() || !item.get("file_id").bytes().is_empty(),
                _ => false,
            })
}

#[cfg(test)]
mod tests {
    /// Go v8.0.35 `ConvertOpenAIRequestToCodex` on over-length tool call IDs (b0debb35):
    /// see tests/reference/codex_chat.
    #[test]
    fn over_length_call_ids_match_go() {
        let go: serde_json::Value = serde_json::from_str(include_str!("../tests/fixtures/codex_chat_go.json")).unwrap();
        for case in go["requests"].as_array().unwrap() {
            let out = super::convert_with_policy(
                case["model"].as_str().unwrap(),
                case["in"].as_str().unwrap().as_bytes(),
                case["stream"].as_bool().unwrap(),
                super::Policy::Codex,
            )
            .unwrap();
            assert_eq!(
                String::from_utf8_lossy(&out),
                case["out"].as_str().unwrap(),
                "{}",
                case["name"]
            );
        }
    }
}
