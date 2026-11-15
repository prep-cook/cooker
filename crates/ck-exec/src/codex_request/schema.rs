//! Codex tool schema normalization.
use super::*;
/// `helps.NormalizeCodexToolSchemas` (codex_tool_schema.go): strip what strict upstream
/// validators reject, then collapse large pure-`const` unions into `enum`. Only the Codex
/// executors call it, so it lives here rather than in `ck_wire::payload`.
pub(crate) fn normalize_tool_schemas(body: String) -> String {
    let tools = gjson::get(&body, "tools");
    match normalize_tool_list(&tools) {
        Some(updated) => set_raw(&body, "tools", &updated),
        None => body,
    }
}

fn normalize_tool_list(tools: &gjson::Value<'_>) -> Option<String> {
    if tools.kind() != Kind::Array {
        return None;
    }
    let raw = tools.json();
    let mut out = String::new();
    let mut offset = 0;
    let mut changed = false;
    tools.each(|_, tool| {
        if let Some(updated) = normalize_tool(&tool) {
            let start = tool.json().as_ptr() as usize - raw.as_ptr() as usize;
            out.push_str(&raw[offset..start]);
            out.push_str(&updated);
            offset = start + tool.json().len();
            changed = true;
        }
        true
    });
    changed.then(|| {
        out.push_str(&raw[offset..]);
        out
    })
}

fn normalize_tool(tool: &gjson::Value<'_>) -> Option<String> {
    let kind = tool.get("type");
    match kind.str() {
        "namespace" => {
            let inner = normalize_tool_list(&tool.get("tools"))?;
            Some(set_raw(tool.json(), "tools", &inner))
        }
        "function" | "custom" => {
            let params = tool.get("parameters");
            if params.kind() != Kind::Object {
                return None;
            }
            let stripped = strip_incompatible(params.json());
            let mut changed = stripped.is_some();
            let mut raw = stripped.unwrap_or_else(|| params.json().to_owned());
            let params = gjson::parse(&raw);
            let mut updates = Vec::new();
            let properties = params.get("properties");
            if properties.kind() == Kind::Object {
                properties.each(|name, prop| {
                    if let Some(updated) = normalize_property(&prop) {
                        updates.push((name.str().to_owned(), updated));
                    }
                    true
                });
            }
            for (name, updated) in updates {
                let key = name.replace('\\', "\\\\").replace('.', "\\.").replace(':', "\\:");
                raw = set_raw(&raw, &format!("properties.{key}"), &updated);
                changed = true;
            }
            changed.then(|| set_raw(tool.json(), "parameters", &raw))
        }
        _ => None,
    }
}

/// `stripIncompatiblePatternsFromJSON`: drops `pattern`s (and `patternProperties` keys)
/// with unsupported regex escapes and integer numeric bounds outside int64 (57d005fe),
/// visiting only subschema locations. A changed schema is re-encoded as Go's Encoder
/// writes a `map[string]any` (sorted keys, no HTML escaping, literal numbers).
fn strip_incompatible(raw: &str) -> Option<String> {
    // Go's fast path: a JSON-escaped `\\p{` contains `\p{`; `\\0`; any `\u` escape;
    // the four bound keywords.
    if !["\\p{", "\\P{", "\\u", "\\\\0", "inimum", "aximum"]
        .iter()
        .any(|needle| raw.contains(needle))
    {
        return None;
    }
    let mut root = ck_wire::json::GoValue::parse(raw.as_bytes())?;
    if root == ck_wire::json::GoValue::Null || !strip_schema(&mut root) {
        return None;
    }
    String::from_utf8(root.marshal_no_html()).ok()
}

/// `codexNumericBoundKeywords`.
const NUMERIC_BOUNDS: [&str; 4] = ["minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum"];

/// `stripIncompatiblePatterns`.
fn strip_schema(value: &mut ck_wire::json::GoValue) -> bool {
    use ck_wire::json::GoValue;
    let escape = |s: &str| ck_wire::schema::has_unsupported_unicode_property_escape(s.as_bytes());
    let mut changed = false;
    match value {
        GoValue::Object(map) => {
            if matches!(map.get("pattern"), Some(GoValue::String(p)) if escape(p)) {
                map.remove("pattern");
                changed = true;
            }
            for key in NUMERIC_BOUNDS {
                if matches!(map.get(key), Some(GoValue::Number(n)) if bound_out_of_int64(n)) {
                    map.remove(key);
                    changed = true;
                }
            }
            if let Some(GoValue::Object(patterns)) = map.get_mut("patternProperties") {
                patterns.retain(|key, sub| {
                    if escape(key) {
                        changed = true;
                        return false;
                    }
                    changed |= strip_schema(sub);
                    true
                });
            }
            for key in ck_wire::schema::SCHEMA_MAP_KEYWORDS {
                let key = std::str::from_utf8(key).unwrap_or_default();
                if key == "patternProperties" {
                    continue;
                }
                if let Some(GoValue::Object(sub)) = map.get_mut(key) {
                    for schema in sub.values_mut() {
                        changed |= strip_schema(schema);
                    }
                }
            }
            for key in ck_wire::schema::SCHEMA_VALUE_KEYWORDS {
                match map.get_mut(std::str::from_utf8(key).unwrap_or_default()) {
                    Some(sub @ GoValue::Object(_)) => changed |= strip_schema(sub),
                    Some(GoValue::Array(items)) => {
                        for item in items {
                            changed |= strip_schema(item);
                        }
                    }
                    _ => {}
                }
            }
        }
        GoValue::Array(items) => {
            for item in items {
                changed |= strip_schema(item);
            }
        }
        _ => {}
    }
    changed
}

/// `isNumericBoundOutOfInt64`: whether a JSON number is an integer outside int64. A
/// fraction or a negative effective exponent is not an integer and stays.
fn bound_out_of_int64(number: &str) -> bool {
    use std::num::IntErrorKind::{NegOverflow, PosOverflow};
    let overflows = |text: &str| matches!(text.parse::<i64>(), Err(e) if matches!(e.kind(), PosOverflow | NegOverflow));
    let raw = number.trim();
    if raw.is_empty() {
        return false;
    }
    if !raw.contains(['.', 'e', 'E']) {
        return overflows(raw);
    }
    let (negative, s) = match raw.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, raw.strip_prefix('+').unwrap_or(raw)),
    };
    let (mantissa, exp) = match s.find(['e', 'E']) {
        Some(at) => {
            let (mantissa, exp) = (&s[..at], &s[at + 1..]);
            match exp.parse::<i64>() {
                Ok(exp) => (mantissa, exp),
                Err(e) if matches!(e.kind(), PosOverflow | NegOverflow) => {
                    return !exp.starts_with('-') && !mantissa.trim_matches(['0', '.']).is_empty();
                }
                Err(_) => return false,
            }
        }
        None => (s, 0),
    };
    let (int_part, frac_part) = match mantissa.split_once('.') {
        Some((_, frac)) if frac.contains('.') => return false,
        Some(parts) => parts,
        None => (mantissa, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return false;
    }
    if !int_part.bytes().chain(frac_part.bytes()).all(|b| b.is_ascii_digit()) {
        return false;
    }
    let digits = format!("{int_part}{frac_part}");
    let significant = digits.trim_start_matches('0');
    if significant.is_empty() {
        return false;
    }
    let len = raw.len() as i64;
    if exp < -len {
        return false;
    }
    if exp > len + 20 {
        return true;
    }
    let trimmed = significant.trim_end_matches('0');
    let trailing_zeros = (significant.len() - trimmed.len()) as i64;
    let effective = exp - frac_part.len() as i64 + trailing_zeros;
    if effective < 0 {
        return false;
    }
    if effective > 19 || trimmed.len() as i64 + effective > 19 {
        return true;
    }
    let sign = if negative { "-" } else { "" };
    overflows(&format!("{sign}{trimmed}{}", "0".repeat(effective as usize)))
}

fn normalize_property(prop: &gjson::Value<'_>) -> Option<String> {
    if prop.kind() != Kind::Object {
        return None;
    }
    let name = match (prop.get("oneOf").exists(), prop.get("anyOf").exists()) {
        (true, false) => "oneOf",
        (false, true) => "anyOf",
        _ => return None,
    };
    let union = prop.get(name);
    let branches = union.array();
    if union.kind() != Kind::Array || branches.len() < 8 {
        return None;
    }
    let mut keys = Vec::new();
    let mut raws = Vec::new();
    for branch in &branches {
        if branch.kind() != Kind::Object {
            return None;
        }
        let mut only_const = true;
        branch.each(|k, _| {
            only_const &= matches!(k.str(), "const" | "description" | "title");
            true
        });
        let value = branch.get("const");
        if !only_const || !value.exists() {
            return None;
        }
        let key = canonical_key(&value)?;
        if keys.contains(&key) {
            return None;
        }
        keys.push(key);
        raws.push(value.json().to_owned());
    }
    let existing = prop.get("enum");
    if existing.exists() && existing.kind() == Kind::Array {
        let mut existing_keys = Vec::new();
        for v in existing.array() {
            existing_keys.push(canonical_key(&v)?);
        }
        let mut unique = existing_keys.clone();
        unique.sort();
        unique.dedup();
        let mut wanted = keys.clone();
        wanted.sort();
        let same = existing_keys.len() == keys.len() && unique.len() == existing_keys.len() && unique == wanted;
        return same.then(|| delete(prop.json(), name));
    }
    let with_enum = set_raw(prop.json(), "enum", &format!("[{}]", raws.join(",")));
    Some(delete(&with_enum, name))
}

/// `canonicalJSONValueKey`: numbers compare as exact rationals (`1` == `1.0` == `1e0`).
fn canonical_key(value: &gjson::Value<'_>) -> Option<String> {
    Some(match value.kind() {
        Kind::String => format!("s:{}", value.str()),
        Kind::Number => format!("n:{}", rational(value.json().trim())),
        Kind::True => "b:true".into(),
        Kind::False => "b:false".into(),
        Kind::Null => "null".into(),
        _ => return None,
    })
}

/// Exact decimal normal form of a JSON number: sign, significant digits, exponent.
pub(super) fn rational(raw: &str) -> String {
    let (mantissa, exp) = match raw.find(['e', 'E']) {
        Some(i) => (&raw[..i], raw[i + 1..].parse::<i64>().unwrap_or(0)),
        None => (raw, 0),
    };
    let negative = mantissa.starts_with('-');
    let mantissa = mantissa.trim_start_matches(['-', '+']);
    let (int, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits = format!("{int}{frac}");
    let exp = exp - frac.len() as i64;
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return "0".into();
    }
    let trimmed = digits.trim_end_matches('0');
    let exp = exp + (digits.len() - trimmed.len()) as i64;
    format!("{}{trimmed}e{exp}", if negative { "-" } else { "" })
}
