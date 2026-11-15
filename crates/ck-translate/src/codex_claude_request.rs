use super::*;
use crate::responses_policy::{Policy, meta_controls};
use ck_core::format::Format;
/// convertClaudeRequestToCodex. A user turn left empty because its only attachment
/// cannot be sent is refused.
pub(crate) fn convert_with_policy(model: &str, raw: &[u8], preserve: bool, policy: Policy) -> Result<Vec<u8>, Error> {
    let mut out = br#"{"model":"","instructions":"","input":[]}"#.to_vec();
    let root = gj::parse(raw);
    let names = original_to_short(raw);
    gj::set_str(&mut out, "model", model);
    let mut state = Messages {
        model,
        preserve,
        names: &names,
        items: vec![],
        pending_tool_uses: vec![],
        pending_reminders: vec![],
        drops: UserTurnDrops::default(),
    };

    system(&root, &mut state);

    let messages = root.get("messages");
    if messages.is_array() {
        for message in messages.array() {
            state.message(&message);
        }
        state.flush_reminders();
    }

    let (body, has_web_search, tool_items) = tools(out, &root, &names);
    out = controls(body, &root, policy, has_web_search);
    if !policy.codex() {
        meta_controls(&mut out, raw, Format::Claude);
    }

    out = format(out, &root);
    if root.get("tools").is_array() {
        gj::set_raw(&mut out, "tools", gj::join(&tool_items));
    }
    gj::set_items(&mut out, "input", &state.items);
    state.drops.result()?;
    Ok(out)
}

fn system(root: &Res<'_>, state: &mut Messages<'_>) {
    let system = root.get("system");
    if system.exists() {
        let mut parts: Vec<Vec<u8>> = vec![];
        let mut add = |text: &[u8]| {
            if !text.is_empty() && !common::is_claude_code_attribution_text(text) {
                let mut part = br#"{"type":"input_text","text":""}"#.to_vec();
                gj::set_str(&mut part, "text", text);
                parts.push(part);
            }
        };
        if system.kind == Kind::String {
            add(&system.s);
        } else if system.is_array() {
            for block in system.array() {
                if block.get("type").bytes().as_ref() == b"text" {
                    add(&block.get("text").bytes());
                }
            }
        }
        if !parts.is_empty() {
            let mut message = br#"{"type":"message","role":"developer"}"#.to_vec();
            gj::set_raw(&mut message, "content", gj::join(&parts));
            state.items.push(message);
        }
    }
}

fn tools(mut out: Vec<u8>, root: &Res<'_>, names: &HashMap<Vec<u8>, Vec<u8>>) -> (Vec<u8>, bool, Vec<Vec<u8>>) {
    let tools = root.get("tools");
    let mut has_web_search = false;
    let mut tool_items: Vec<Vec<u8>> = vec![];
    if tools.is_array() {
        let web_search: HashSet<Vec<u8>> = tools
            .array()
            .iter()
            .filter(|t| is_web_search_tool(&t.get("type").bytes()))
            .map(|t| t.get("name").bytes().into_owned())
            .filter(|n| !n.is_empty())
            .collect();
        gj::set_raw(
            &mut out,
            "tool_choice",
            tool_choice(&root.get("tool_choice"), names, &web_search),
        );
        for tool in tools.array() {
            if is_web_search_tool(&tool.get("type").bytes()) {
                has_web_search = true;
                tool_items.push(web_search_tool(&tool));
                continue;
            }
            let mut item = tool.raw.to_vec();
            let kind = tool.get("type");
            if kind.kind != Kind::String || kind.s.as_ref() != b"function" {
                gj::set_str(&mut item, "type", "function");
            }
            let name = tool.get("name");
            if name.exists() {
                let original = name.bytes().into_owned();
                let short = mapped_name(names, &original);
                if name.kind != Kind::String || short != original {
                    gj::set_str(&mut item, "name", &short);
                }
            }
            gj::set_raw(
                &mut item,
                "parameters",
                normalize_tool_parameters(&tool.get("input_schema").raw),
            );
            for path in ["input_schema", "parameters.$schema", "cache_control", "defer_loading"] {
                if gj::get(&item, path).exists() {
                    gj::delete(&mut item, path);
                }
            }
            if gj::get(&item, "strict").kind != Kind::False {
                gj::set_bool(&mut item, "strict", false);
            }
            tool_items.push(item);
        }
    }
    (out, has_web_search, tool_items)
}

fn controls(mut out: Vec<u8>, root: &Res<'_>, policy: Policy, has_web_search: bool) -> Vec<u8> {
    let disable_parallel = root.get("tool_choice.disable_parallel_tool_use");
    if policy.codex() || disable_parallel.exists() {
        gj::set_bool(&mut out, "parallel_tool_calls", !disable_parallel.bool());
    }
    let effort = effort(root, policy);
    if !effort.is_empty() {
        gj::set_str(&mut out, "reasoning.effort", effort);
    }
    if !policy.codex() {
        gj::set_bool(&mut out, "stream", true);
        gj::set_bool(&mut out, "store", false);
        return out;
    }
    let mut tier = service_tier(&root.get("service_tier"));
    let speed = root.get("speed");
    if speed.kind == Kind::String && speed.s.as_ref() == b"fast" {
        tier = "priority";
    }
    if !tier.is_empty() {
        gj::set_str(&mut out, "service_tier", tier);
    }
    gj::set_bool(&mut out, "stream", true);
    gj::set_bool(&mut out, "store", false);
    if has_web_search {
        gj::set_strs(
            &mut out,
            "include",
            &["reasoning.encrypted_content", "web_search_call.action.sources"],
        );
    } else {
        gj::set_strs(&mut out, "include", &["reasoning.encrypted_content"]);
    }
    out
}

fn effort(root: &Res<'_>, policy: Policy) -> Vec<u8> {
    let mut effort = if policy.codex() { b"medium".to_vec() } else { Vec::new() };
    let thinking = root.get("thinking");
    if thinking.is_object() {
        match thinking.get("type").bytes().as_ref() {
            b"enabled" => {
                let budget = thinking.get("budget_tokens");
                if budget.exists()
                    && let Some(level) = ck_wire::thinking::convert_budget_to_level(budget.int())
                    && !level.is_empty()
                {
                    effort = level.as_bytes().to_vec();
                }
            }
            b"adaptive" | b"auto" => {
                // Claude 4.6 adaptive thinking may carry its effort in output_config.
                let explicit = root.get("output_config.effort");
                let explicit = if explicit.kind == Kind::String {
                    go_lower(trim_space(&explicit.s))
                } else {
                    vec![]
                };
                effort = if explicit.is_empty() && policy.codex() {
                    b"xhigh".to_vec()
                } else {
                    explicit
                };
            }
            b"disabled" => {
                if let Some(level) = ck_wire::thinking::convert_budget_to_level(0)
                    && !level.is_empty()
                {
                    effort = level.as_bytes().to_vec();
                }
            }
            _ => {}
        }
    }
    effort
}

fn format(mut out: Vec<u8>, root: &Res<'_>) -> Vec<u8> {
    let format = root.get("output_config.format");
    if format.is_object() && format.get("type").bytes().as_ref() == b"json_schema" && format.get("schema").is_object() {
        let mut name = format.get("name").bytes().into_owned();
        if name.is_empty() {
            name = b"cli_proxy_structured_output".to_vec();
        }
        let schema = format.get("schema");
        let strict = format.get("strict").kind != Kind::False && !misses_required(&schema);
        let mut translated = br#"{"type":"json_schema","name":"","strict":true,"schema":{}}"#.to_vec();
        gj::set_str(&mut translated, "name", &name);
        gj::set_bool(&mut translated, "strict", strict);
        gj::set_raw(&mut translated, "schema", &schema.raw);
        gj::set_raw(&mut out, "text.format", translated);
    }
    out
}
