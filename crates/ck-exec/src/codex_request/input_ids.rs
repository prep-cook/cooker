//! Codex input item identity normalization.
use super::*;

const LIMIT: usize = 64;
const OCCUPIED: u8 = 1;
const PRESERVED: u8 = 2;

/// `helps.SanitizeCodexInputItemIDs`: type prefixes, the 64-char limit, collision suffixes
/// and dropping over-long encrypted reasoning items.
pub(crate) fn sanitize_input_ids(body: String) -> String {
    let input = gjson::get(&body, "input");
    if input.kind() != Kind::Array {
        return body;
    }
    let items = input.array();
    let mut states = occupied(&items);
    let mut shortened: std::collections::HashMap<String, String> = Default::default();
    let mut collisions: std::collections::HashMap<String, String> = Default::default();
    let mut rebuilt = Vec::with_capacity(items.len());
    let mut changed = false;
    for item in &items {
        if dropped(item) {
            changed = true;
            continue;
        }
        let mut raw = item.json().to_owned();
        let id = item.get("id");
        if id.kind() == Kind::String {
            let original = id.str();
            let mut next = normalize(item, original);
            if next != original && states.get(&next).is_some_and(|s| s & PRESERVED != 0) {
                next = match collisions.get(&next) {
                    Some(c) => c.clone(),
                    None => {
                        let mut attempt = 0;
                        let candidate = loop {
                            let candidate = hashed(&next, attempt);
                            if states.get(&candidate).is_some_and(|s| s & OCCUPIED != 0) {
                                attempt += 1;
                                continue;
                            }
                            break candidate;
                        };
                        collisions.insert(next.clone(), candidate.clone());
                        *states.entry(candidate.clone()).or_default() |= OCCUPIED;
                        candidate
                    }
                };
            }
            if next.chars().count() > LIMIT {
                next = match shortened.get(&next) {
                    Some(s) => s.clone(),
                    None => {
                        let mut candidate = hashed(&next, 0);
                        let mut attempt = 1;
                        while states.get(&candidate).is_some_and(|s| s & OCCUPIED != 0) {
                            candidate = hashed(&next, attempt);
                            attempt += 1;
                        }
                        shortened.insert(next.clone(), candidate.clone());
                        *states.entry(candidate.clone()).or_default() |= OCCUPIED;
                        candidate
                    }
                };
            }
            if next != original {
                raw = set_str(&raw, "id", &next);
                changed = true;
            }
        }
        rebuilt.push(raw);
    }
    if changed {
        set_raw(&body, "input", &format!("[{}]", rebuilt.join(",")))
    } else {
        body
    }
}

fn normalize(item: &gjson::Value<'_>, id: &str) -> String {
    let prefix = match item.get("type").str() {
        "message" => "msg",
        "reasoning" => "rs",
        "function_call" => "fc",
        "custom_tool_call" => "ctc",
        "custom_tool_call_output" => "ctco",
        _ => return id.to_owned(),
    };
    if id.is_empty() || id.starts_with(prefix) {
        id.to_owned()
    } else {
        format!("{prefix}_{id}")
    }
}

fn dropped(item: &gjson::Value<'_>) -> bool {
    let id = item.get("id");
    let enc = item.get("encrypted_content");
    item.get("type").str() == "reasoning"
        && id.kind() == Kind::String
        && id.str().chars().count() > LIMIT
        && enc.kind() == Kind::String
        && !enc.str().is_empty()
}

fn occupied(items: &[gjson::Value<'_>]) -> std::collections::HashMap<String, u8> {
    let mut states = std::collections::HashMap::new();
    for item in items {
        let id = item.get("id");
        if dropped(item) || id.kind() != Kind::String {
            continue;
        }
        let normalized = normalize(item, id.str());
        let mut state = states.get(&normalized).copied().unwrap_or(0);
        if normalized == id.str() {
            state |= PRESERVED;
        }
        if normalized.chars().count() <= LIMIT {
            state |= OCCUPIED;
        }
        if state != 0 {
            states.insert(normalized, state);
        }
    }
    states
}

fn hashed(id: &str, attempt: usize) -> String {
    let mut input = id.to_owned();
    if attempt > 0 {
        input.push('\0');
        input.push_str(&attempt.to_string());
    }
    let digest = Sha256::digest(input.as_bytes());
    let suffix = format!(
        "_{}",
        digest[..8].iter().map(|b| format!("{b:02x}")).collect::<String>()
    );
    let keep = (LIMIT - suffix.len()).min(id.chars().count());
    format!("{}{suffix}", id.chars().take(keep).collect::<String>())
}
