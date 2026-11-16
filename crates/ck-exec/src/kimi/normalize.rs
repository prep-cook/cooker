use super::*;

pub(crate) fn strip_kimi_prefix(model: &str) -> &str {
    let model = model.trim();
    if model.go_lower().starts_with("kimi-") {
        &model[5..]
    } else {
        model
    }
}

/// `normalizeKimiUpstreamModel`: strip `kimi-` and `[1m]`, map K2.7/K2.8 Code aliases,
/// keep a thinking suffix.
pub(crate) fn normalize_upstream_model(model: &str) -> String {
    let parsed = parse_suffix(model.trim());
    let mut base = parsed.model_name.trim().go_lower();
    if let Some(stripped) = base.strip_suffix("[1m]") {
        base = stripped.to_owned();
    }
    let normalized = match base.as_str() {
        "kimi-k2.8" | "k2.8" | "kimi-k2.8-code" | "k2.8-code" | "kimi-k2.8-preview" | "k2.8-preview"
        | "kimi-k2.7-code" | "k2.7-code" | "kimi-for-coding" | "for-coding" => "kimi-for-coding".to_owned(),
        "kimi-k2.7-code-highspeed" | "k2.7-code-highspeed" | "kimi-for-coding-highspeed" | "for-coding-highspeed" => {
            "kimi-for-coding-highspeed".to_owned()
        }
        _ => strip_kimi_prefix(&base).to_owned(),
    };
    if parsed.has_suffix {
        format!("{normalized}({})", parsed.raw_suffix)
    } else {
        normalized
    }
}

pub(crate) fn usable_reasoning(reasoning: &str) -> bool {
    let trimmed = reasoning.trim();
    !trimmed.is_empty() && trimmed != REASONING_UNAVAILABLE
}

/// `strings.TrimSpace(r.String())`, as bytes.
pub(crate) fn trimmed(r: &Res<'_>) -> Vec<u8> {
    go_trim_space(&r.bytes()).to_vec()
}

pub(crate) fn is_empty_object(r: &Res<'_>) -> bool {
    go_trim_space(r.raw()) == b"{}"
}

pub(crate) fn content_part_empty(part: &Res<'_>) -> bool {
    match part.kind {
        Kind::Null => true,
        Kind::String => trimmed(part).is_empty(),
        Kind::Json if part.is_object() => {
            let text = part.get("text");
            if text.exists() {
                return trimmed(&text).is_empty();
            }
            trimmed(&part.get("type")) == b"text" || is_empty_object(part)
        }
        _ => false,
    }
}

pub(crate) fn should_drop_assistant(msg: &Res<'_>) -> bool {
    if trimmed(&msg.get("role")) != b"assistant" {
        return false;
    }
    let tool_calls = msg.get("tool_calls");
    let has_tool_calls = tool_calls.is_array() && !tool_calls.array().is_empty();
    let call = msg.get("function_call");
    let has_function_call = call.exists() && call.kind != Kind::Null && !(call.is_object() && is_empty_object(&call));
    let has_reasoning = {
        let r = msg.get("reasoning_content");
        r.exists() && !trimmed(&r).is_empty()
    };
    if has_tool_calls || has_function_call || has_reasoning {
        return false;
    }
    let content = msg.get("content");
    match content.kind {
        _ if !content.exists() => true,
        Kind::Null => true,
        Kind::String => trimmed(&content).is_empty(),
        Kind::Json if content.is_array() => content.array().iter().all(content_part_empty),
        _ => false,
    }
}

pub(crate) fn fallback_reasoning(msg: &Res<'_>, latest: Option<&[u8]>) -> Vec<u8> {
    if let Some(latest) = latest.filter(|l| usable_reasoning(&String::from_utf8_lossy(l))) {
        return latest.to_vec();
    }
    let content = msg.get("content");
    if content.kind == Kind::String && !trimmed(&content).is_empty() {
        return trimmed(&content);
    }
    if content.is_array() {
        let parts: Vec<Vec<u8>> = content
            .array()
            .iter()
            .map(|item| trimmed(&item.get("text")))
            .filter(|t| !t.is_empty())
            .collect();
        if !parts.is_empty() {
            return parts.join(&b'\n');
        }
    }
    REASONING_UNAVAILABLE.as_bytes().to_vec()
}

/// `normalizeKimiToolMessageLinks`: drop empty assistant turns, repair tool_call_id links
/// and give tool-calling assistant turns reasoning_content.
pub(crate) fn normalize_tool_message_links(body: Vec<u8>) -> Result<Vec<u8>, ExecError> {
    if body.is_empty() || !gj::valid(&body) {
        return Ok(body);
    }
    let messages = gj::get(&body, "messages");
    if !messages.is_array() {
        return Ok(body);
    }
    let msgs = messages.array();
    let mut dropped = vec![false; msgs.len()];
    let mut patches: Vec<LinkPatch<'_>> = Vec::new();
    let mut pending: Vec<Vec<u8>> = Vec::new();
    let mut latest: Option<Vec<u8>> = None;
    for (index, msg) in msgs.iter().enumerate() {
        if should_drop_assistant(msg) {
            dropped[index] = true;
            continue;
        }
        match trimmed(&msg.get("role")).as_slice() {
            b"assistant" => {
                let reasoning = msg.get("reasoning_content");
                let usable = reasoning.exists() && usable_reasoning(&reasoning.str());
                if usable {
                    latest = Some(reasoning.bytes().into_owned());
                }
                let calls = msg.get("tool_calls");
                if calls.is_array() && !calls.array().is_empty() {
                    if !usable {
                        patches.push((
                            index,
                            "reasoning_content",
                            fallback_reasoning(msg, latest.as_deref()),
                            "failed to set assistant reasoning_content",
                        ));
                    }
                    for call in calls.array() {
                        let id = trimmed(&call.get("id"));
                        if !id.is_empty() {
                            pending.push(id);
                        }
                    }
                }
            }
            b"tool" => {
                let mut id = trimmed(&msg.get("tool_call_id"));
                if id.is_empty() {
                    id = trimmed(&msg.get("call_id"));
                    if !id.is_empty() {
                        patches.push((
                            index,
                            "tool_call_id",
                            id.clone(),
                            "failed to set tool_call_id from call_id",
                        ));
                    }
                }
                if id.is_empty() && pending.len() == 1 {
                    id = pending[0].clone();
                    patches.push((index, "tool_call_id", id.clone(), "failed to infer tool_call_id"));
                }
                if let Some(pos) = pending.iter().position(|p| *p == id).filter(|_| !id.is_empty()) {
                    pending.remove(pos);
                }
            }
            _ => {}
        }
    }
    if !dropped.contains(&true) && patches.is_empty() {
        return Ok(body);
    }
    let edits = link_edits(&msgs, &dropped, &patches)?;
    drop((msgs, messages));
    let mut body = body;
    kimi_edit::splice(&mut body, edits);
    Ok(body)
}

/// A link patch: message index, member path, value and the error context.
type LinkPatch<'p> = (usize, &'p str, Vec<u8>, &'p str);

/// The in-place edits that patch messages and remove the `dropped` ones; only a patched
/// message is copied, to set its members.
pub(crate) fn link_edits(
    msgs: &[Res<'_>],
    dropped: &[bool],
    patches: &[LinkPatch<'_>],
) -> Result<Vec<kimi_edit::Edit>, ExecError> {
    let spans: Vec<_> = msgs.iter().map(kimi_edit::span).collect();
    let mut edits: Vec<_> = kimi_edit::removals(&spans, dropped)
        .into_iter()
        .map(|range| (range, Vec::new()))
        .collect();
    let mut next = patches.iter().peekable();
    for (index, msg) in msgs.iter().enumerate() {
        let mut patched: Option<Vec<u8>> = None;
        while let Some((_, path, value, context)) = next.next_if(|p| p.0 == index) {
            let raw = patched.as_deref().unwrap_or(msg.raw());
            let set = gj::try_set_str(raw, path, value);
            patched = Some(set.map_err(|e| internal_error(format!("kimi executor: {context}: {e}")))?);
        }
        if let Some(raw) = patched {
            edits.push((spans[index].clone(), raw));
        }
    }
    Ok(edits)
}

/// `normalizeKimiTools`: inline local `$ref`s and default the root schema type.
pub(crate) fn normalize_tools(body: Vec<u8>) -> Vec<u8> {
    if body.is_empty() {
        return body;
    }
    let body = normalize_tool_list(body, "tools", true);
    normalize_tool_list(body, "functions", false)
}

pub(crate) fn normalize_tool_list(mut body: Vec<u8>, key: &str, is_tools: bool) -> Vec<u8> {
    let items = gj::get(&body, key);
    if !items.is_array() {
        return body;
    }
    // Only a schema that changes is copied, to normalize it; it replaces its span in place.
    let mut edits = Vec::new();
    for item in &items.array() {
        let path = if is_tools && item.get("function.parameters").exists() {
            Some("function.parameters")
        } else if item.get("parameters").exists() {
            Some("parameters")
        } else {
            None
        };
        if let Some(path) = path {
            let params = item.get(path);
            if params.is_object() {
                let normalized = normalize_parameters_schema(params.raw());
                if normalized != params.raw() {
                    edits.push((kimi_edit::span(&params), normalized));
                }
            }
        }
    }
    drop(items);
    kimi_edit::splice(&mut body, edits);
    body
}

pub(crate) fn normalize_parameters_schema(raw: &[u8]) -> Vec<u8> {
    if go_trim_space(raw).is_empty() {
        return raw.to_vec();
    }
    let mut params = ck_wire::gemini_schema::inline_local_refs(raw);
    for container in ["$defs", "definitions"] {
        if gj::get(&params, container).exists() {
            gj::delete(&mut params, container);
        }
    }
    if !gj::get(&params, "type").exists() {
        gj::set_str(&mut params, "type", "object");
    }
    params
}

/// items in between move after the outputs.
pub(crate) fn normalize_responses_input(mut body: Vec<u8>) -> Vec<u8> {
    if body.is_empty() || !gj::valid(&body) {
        return body;
    }
    let input = gj::get(&body, "input");
    if !input.is_array() {
        return body;
    }
    let moves = output_moves(&input.array());
    drop(input);
    for (front, unit) in moves {
        body[front..unit.end].rotate_left(unit.start - front);
    }
    body
}

fn responses_call_id(item: &Res<'_>) -> Vec<u8> {
    for key in ["call_id", "tool_call_id", "callId"] {
        let id = trimmed(&item.get(key));
        if !id.is_empty() {
            return id;
        }
    }
    let id = trimmed(&item.get("id"));
    if id.starts_with(b"fco_") { Vec::new() } else { id }
}

fn is_tool_call(item: &Res<'_>) -> bool {
    matches!(
        trimmed(&item.get("type")).as_slice(),
        b"function_call" | b"custom_tool_call"
    )
}

fn is_tool_output(item: &Res<'_>) -> bool {
    matches!(
        trimmed(&item.get("type")).as_slice(),
        b"function_call_output" | b"custom_tool_call_output"
    )
}

/// Where [`normalize_responses_input`] moves each tool output, in the order to apply them: the
/// output with the separator before it (`unit`), to `front`, the start of the items that came
/// between it and its calls. A moved item always follows a call, so it has a separator, and a
/// move leaves the offsets before `front` and after the output as they were.
pub(crate) fn output_moves(items: &[Res<'_>]) -> Vec<(usize, std::ops::Range<usize>)> {
    let item_end = |k: usize| items[k].index + items[k].raw.len();
    let mut moves = Vec::new();
    let mut i = 0;
    while i < items.len() {
        if !is_tool_call(&items[i]) {
            i += 1;
            continue;
        }
        let mut end = i;
        let mut ids: std::collections::HashMap<Vec<u8>, usize> = std::collections::HashMap::new();
        let mut count = 0;
        while end < items.len() && is_tool_call(&items[end]) {
            let id = responses_call_id(&items[end]);
            if !id.is_empty() {
                *ids.entry(id).or_default() += 1;
                count += 1;
            }
            end += 1;
        }
        let last = (count > 0).then(|| matched_outputs(items, end, &ids, count)).flatten();
        let Some(last) = last else {
            i = end;
            continue;
        };
        let mut consumed = ids;
        let mut front: Option<usize> = None;
        for (k, item) in items.iter().enumerate().take(last + 1).skip(end) {
            let matched = is_tool_output(item)
                && match consumed.get_mut(&responses_call_id(item)) {
                    Some(n) if *n > 0 => {
                        *n -= 1;
                        true
                    }
                    _ => false,
                };
            let unit = item_end(k - 1)..item_end(k);
            match (matched, front) {
                (false, None) => front = Some(unit.start),
                (true, Some(at)) => {
                    front = Some(at + unit.len());
                    moves.push((at, unit));
                }
                _ => {}
            }
        }
        i = last + 1;
    }
    moves
}

/// The index of the last of the outputs answering every call in `ids` (`count` in all) after
/// the calls ending at `end`, before the next call; `None` if some call has no output there.
pub(crate) fn matched_outputs(
    items: &[Res<'_>],
    end: usize,
    ids: &std::collections::HashMap<Vec<u8>, usize>,
    count: usize,
) -> Option<usize> {
    let mut needed = ids.clone();
    let mut remaining = count;
    let mut last = None;
    for (j, item) in items.iter().enumerate().skip(end) {
        if remaining == 0 || is_tool_call(item) {
            break;
        }
        if is_tool_output(item) {
            let id = responses_call_id(item);
            if let Some(n) = needed.get_mut(&id).filter(|n| **n > 0) {
                *n -= 1;
                remaining -= 1;
                last = Some(j);
            }
        }
    }
    last.filter(|_| remaining == 0)
}
