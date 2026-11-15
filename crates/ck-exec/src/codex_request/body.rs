//! Codex request body rules, including SIWC and tool selection.
use super::*;
/// The Sign in with ChatGPT body rules for `call` (`applyCodexSIWCRequestBodyRules`,
/// `applyCodexSIWCWebsocketBodyRules`, `applyCodexSIWCCompactBodyRules`). They replace
/// the instructions default and the image tool; WebSocket callers apply them after the
/// prompt cache key, as Go does. `explicit_instructions` is Go's
/// `hasExplicitInstructions` of the client payload; `keep_effort` its
/// `HasThinkingSuffix(req.Model)`.
pub(crate) fn siwc_body(mut body: String, call: Call, explicit_instructions: bool, keep_effort: bool) -> String {
    match call {
        Call::Stream | Call::NonStream => {
            body = set_bool_if_different(body, "store", false);
            body = set_bool_if_different(body, "stream", true);
        }
        Call::Websocket => {
            body = set_bool_if_different(body, "store", false);
            body = delete(&body, "stream");
            body = delete(&body, "background");
        }
        Call::Compact => {
            body = delete(&body, "stream");
            body = delete(&body, "store");
        }
    }
    // stripCodexSIWCUnsupportedFields: SIWC rejects configuration_update items, so they
    // collapse into the top-level effort unless a model suffix already set it.
    let collapsed = ck_wire::thinking::collapse_configuration_updates(body.as_bytes(), keep_effort);
    if collapsed != body.as_bytes() {
        body = String::from_utf8(collapsed).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned());
    }
    for key in [
        "prompt_cache_retention",
        "max_output_tokens",
        "max_completion_tokens",
        "temperature",
    ] {
        body = delete(&body, key);
    }
    if !explicit_instructions {
        let instructions = gjson::get(&body, "instructions");
        let blank = match instructions.kind() {
            Kind::Null => instructions.exists(),
            Kind::String => instructions.str().trim().is_empty(),
            _ => false,
        };
        if blank {
            body = delete(&body, "instructions");
        }
    }
    body
}

/// Go's `hasExplicitInstructions`: the client payload's `instructions` is present and
/// its text is not blank.
pub(crate) fn explicit_instructions(payload: &[u8]) -> bool {
    let value = ck_wire::json::get(payload, "instructions");
    match value.kind {
        ck_wire::json::Kind::Null => false,
        ck_wire::json::Kind::String => !value.str().trim().is_empty(),
        _ => value.exists(),
    }
}

/// [`siwc_body`] with the request's own instruction and suffix flags.
pub(crate) fn siwc_request_body(body: String, req: &ExecRequest, call: Call) -> String {
    siwc_body(
        body,
        call,
        explicit_instructions(&req.body),
        ck_wire::thinking::has_thinking_suffix(&req.model),
    )
}

/// `ensureWebSearchToolForHistory` (e7d4c731): upstream rejects replayed
/// `web_search_call` input items without a web search tool ("response protection is
/// unavailable"). A standard request gets `{"type":"web_search"}`; a Responses Lite
/// request, which carries no top-level hosted tools, drops the replayed items instead.
pub(crate) fn ensure_web_search_for_history(body: String, headers: &HeaderMap) -> String {
    use ck_wire::json as gj;
    if !body.contains("web_search_call") {
        return body;
    }
    let input = gj::get(body.as_bytes(), "input");
    if !input.is_array() {
        return body;
    }
    let is_call = |item: &gj::Res<'_>| item.get("type").str().trim() == "web_search_call";
    // Detection walks with ForEach and the rebuild with Array(), as Go does: on a malformed
    // array such as `[{}{...}]`, ForEach stops after the first element.
    let mut has_call = false;
    input.each(|_, item| {
        has_call = is_call(&item);
        !has_call
    });
    if !has_call {
        return body;
    }
    let tools = gj::get(body.as_bytes(), "tools");
    if tools.is_array()
        && tools.array().iter().any(|t| {
            matches!(
                t.get("type").str().trim(),
                "web_search" | "web_search_preview" | "web_search_preview_2025_03_11"
            )
        })
    {
        return body;
    }
    if is_lite(&body, headers) {
        let items = input.array();
        let kept: Vec<&[u8]> = items.iter().filter(|i| !is_call(i)).map(|i| i.raw()).collect();
        let joined = kept.join(&b","[..]);
        return set_raw(&body, "input", &format!("[{}]", String::from_utf8_lossy(&joined)));
    }
    if !tools.is_array() {
        return set_raw(&body, "tools", r#"[{"type":"web_search"}]"#);
    }
    set_raw(&body, "tools.-1", r#"{"type":"web_search"}"#)
}

pub(crate) fn ensure_image_tool(body: String, model: &str, view: &View<'_>, headers: &HeaderMap) -> String {
    const TOOL: &str = r#"{"type":"image_generation","output_format":"png"}"#;
    if is_lite(&body, headers)
        || model.ends_with("spark")
        || (view.credential.provider.eq_ignore_ascii_case("codex") && view.plan_type().eq_ignore_ascii_case("free"))
    {
        return body;
    }
    let tools = gjson::get(&body, "tools");
    if !tools.exists() || tools.kind() != Kind::Array {
        return set_raw(&body, "tools", &format!("[{TOOL}]"));
    }
    let present = tools.array().iter().any(|t| {
        let kind = t.get("type");
        let name = t.get("name");
        match kind.str() {
            "image_generation" => true,
            "function" => name.str() == "image_gen.imagegen",
            "namespace" => {
                name.str() == "image_gen"
                    && t.get("tools")
                        .array()
                        .iter()
                        .any(|n| n.get("type").str() == "function" && n.get("name").str() == "imagegen")
            }
            _ => false,
        }
    });
    if present {
        body
    } else {
        set_raw(&body, "tools.-1", TOOL)
    }
}

pub(crate) fn normalize_parallel_tool_calls(body: String, headers: &HeaderMap) -> String {
    if is_lite(&body, headers) {
        return set_bool_if_different(body, "parallel_tool_calls", false);
    }
    let parallel = gjson::get(&body, "parallel_tool_calls");
    if !parallel.exists() || parallel.kind() == Kind::False {
        return body;
    }
    let tools = gjson::get(&body, "tools");
    if tools.kind() == Kind::Array && !tools.array().is_empty() {
        return body;
    }
    delete(&body, "parallel_tool_calls")
}

/// `sanitizeOpenAIResponsesReasoningEncryptedContentWithCompat`. Compat models
/// (third-party Responses upstreams) keep reasoning `content` and orphan ids; invalid
/// `encrypted_content` is dropped either way.
pub(crate) fn sanitize_reasoning(body: String, is_compat: bool) -> String {
    let input = gjson::get(&body, "input");
    if input.kind() != Kind::Array {
        return body;
    }
    let strip_orphan_ids = !gjson::get(&body, "store").bool();
    let items = input.array();
    let mut rebuilt: Option<Vec<String>> = None;
    for (index, item) in items.iter().enumerate() {
        let mut next = item.json().to_owned();
        let mut changed = false;
        if item.get("type").str().trim() == "reasoning" {
            let content = item.get("content");
            if !is_compat && content.kind() == Kind::Array && !content.array().is_empty() {
                let summary = item.get("summary");
                let empty_summary = !summary.exists()
                    || summary.kind() == Kind::Null
                    || (summary.kind() == Kind::Array && summary.array().is_empty());
                if empty_summary {
                    let parts: Vec<String> = content
                        .array()
                        .iter()
                        .filter(|p| p.get("type").str().trim() == "reasoning_text" && !p.get("text").str().is_empty())
                        .map(|p| set_str(r#"{"type":"summary_text"}"#, "text", p.get("text").str()))
                        .collect();
                    if !parts.is_empty() {
                        next = set_raw(&next, "summary", &format!("[{}]", parts.join(",")));
                    }
                }
                next = set_raw(&next, "content", "[]");
                changed = true;
            }
            let encrypted = item.get("encrypted_content");
            let has_id = item.get("id").exists();
            if !encrypted.exists() {
                if !is_compat && strip_orphan_ids && has_id {
                    next = delete(&next, "id");
                    changed = true;
                }
            } else {
                let valid = encrypted.kind() == Kind::String
                    && encrypted.str() == encrypted.str().trim()
                    && ck_wire::signature::is_valid_gpt_reasoning_signature(encrypted.str());
                if !valid {
                    next = delete(&next, "encrypted_content");
                    changed = true;
                    if !is_compat && strip_orphan_ids && has_id {
                        next = delete(&next, "id");
                    }
                }
            }
        }
        if changed && rebuilt.is_none() {
            rebuilt = Some(items[..index].iter().map(|i| i.json().to_owned()).collect());
        }
        if let Some(rebuilt) = rebuilt.as_mut() {
            rebuilt.push(if changed { next } else { item.json().to_owned() });
        }
    }
    match rebuilt {
        Some(items) => set_raw(&body, "input", &format!("[{}]", items.join(","))),
        None => body,
    }
}
