//! Claude Messages request -> Codex (OpenAI Responses) request
//! (internal/translator/codex/claude/codex_claude_request.go).

use std::collections::{HashMap, HashSet};

use ck_wire::json::{self as gj, GoValue, Kind, Res};
use ck_wire::signature::{self as sig, BlockKind, Provider};
use sha2::{Digest, Sha256};

use crate::common::{self, UserTurnDrops, go_lower, trim_space};
use crate::{Error, Registered, RequestCtx, codex_claude_response as response, openai_claude};
#[path = "codex_claude_request.rs"]
mod request;
use crate::responses_policy::Policy;
pub(crate) use request::convert_with_policy;

pub static PAIR: Registered = registered!(
    Claude -> Codex,
    request: |ctx, body| convert_with_policy(ctx.model, body, false, Policy::Codex),
    non_stream: response::non_stream,
    go_stream: response::go_stream,
    token_count: Some(openai_claude::claude_input_tokens),
);

/// ConvertClaudeRequestToCodexWithCompat: assistant thinking blocks with empty or
/// unknown-format signatures are kept for compatibility endpoints.
pub fn request_with_compat(ctx: &RequestCtx<'_>, body: &[u8]) -> Result<Vec<u8>, Error> {
    convert_with_policy(ctx.model, body, true, Policy::Codex)
}

const NAME_LIMIT: usize = 64;

/// shortenNameIfNeeded: at most 64 bytes, keeping `mcp__` plus the last `__` segment.
fn shorten_name(name: &[u8]) -> Vec<u8> {
    if name.len() <= NAME_LIMIT {
        return name.to_vec();
    }
    if name.starts_with(b"mcp__")
        && let Some(idx) = name.windows(2).rposition(|w| w == b"__")
        && idx > 0
    {
        let mut candidate = [&b"mcp__"[..], &name[idx + 2..]].concat();
        candidate.truncate(NAME_LIMIT);
        return candidate;
    }
    name[..NAME_LIMIT].to_vec()
}

/// buildShortNameMap: unique short names, suffixing `_1`, `_2`, ... on collisions.
fn short_name_map(names: &[Vec<u8>]) -> HashMap<Vec<u8>, Vec<u8>> {
    let mut used: HashSet<Vec<u8>> = HashSet::new();
    let mut map = HashMap::new();
    for name in names {
        let candidate = shorten_name(name);
        let unique = if used.contains(&candidate) {
            (1..)
                .map(|i| {
                    let suffix = format!("_{i}");
                    let allowed = NAME_LIMIT.saturating_sub(suffix.len());
                    [&candidate[..candidate.len().min(allowed)], suffix.as_bytes()].concat()
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

fn tool_names(raw: &[u8]) -> Vec<Vec<u8>> {
    let tools = gj::get(raw, "tools");
    if !tools.is_array() {
        return vec![];
    }
    tools
        .array()
        .iter()
        .map(|t| t.get("name").bytes().into_owned())
        .filter(|n| !n.is_empty())
        .collect()
}

/// buildReverseMapFromClaudeOriginalToShort: original tool name -> short name.
fn original_to_short(raw: &[u8]) -> HashMap<Vec<u8>, Vec<u8>> {
    short_name_map(&tool_names(raw))
}

/// buildReverseMapFromClaudeOriginalShortToOriginal: short name -> original tool name.
pub(crate) fn short_to_original(raw: &[u8]) -> HashMap<Vec<u8>, Vec<u8>> {
    original_to_short(raw)
        .into_iter()
        .map(|(orig, short)| (short, orig))
        .collect()
}

fn mapped_name(map: &HashMap<Vec<u8>, Vec<u8>>, name: &[u8]) -> Vec<u8> {
    map.get(name).cloned().unwrap_or_else(|| shorten_name(name))
}

/// shortenCodexCallIDIfNeeded: call IDs within the Responses 64-byte limit, with a
/// sha256-derived suffix.
pub(crate) fn shorten_call_id(id: &[u8]) -> Vec<u8> {
    if id.len() <= NAME_LIMIT {
        return id.to_vec();
    }
    let sum = Sha256::digest(id);
    let suffix = format!("_{}", common::hex(&sum[..8]));
    [&id[..NAME_LIMIT - suffix.len()], suffix.as_bytes()].concat()
}

/// normalizeCodexServiceTier.
fn service_tier(r: &Res<'_>) -> &'static str {
    if r.kind != Kind::String {
        return "";
    }
    match go_lower(trim_space(&r.s)).as_slice() {
        b"fast" | b"priority" => "priority",
        _ => "",
    }
}

fn is_web_search_tool(kind: &[u8]) -> bool {
    kind == b"web_search_20250305" || kind == b"web_search_20260209"
}

/// convertClaudeToolChoiceToCodex.
fn tool_choice(choice: &Res<'_>, names: &HashMap<Vec<u8>, Vec<u8>>, web_search: &HashSet<Vec<u8>>) -> Vec<u8> {
    if !choice.exists() || choice.kind == Kind::Null {
        return br#""auto""#.to_vec();
    }
    let mut kind = choice.get("type").bytes().into_owned();
    if kind.is_empty() && choice.kind == Kind::String {
        kind = choice.s.to_vec();
    }
    match kind.as_slice() {
        b"any" => br#""required""#.to_vec(),
        b"none" => br#""none""#.to_vec(),
        b"tool" => {
            let name = choice.get("name").bytes().into_owned();
            if web_search.contains(&name) {
                return br#"{"type":"web_search"}"#.to_vec();
            }
            let name = mapped_name(names, &name);
            if name.is_empty() {
                return br#""auto""#.to_vec();
            }
            let mut out = br#"{"type":"function","name":""}"#.to_vec();
            gj::set_str(&mut out, "name", &name);
            out
        }
        _ => br#""auto""#.to_vec(),
    }
}

/// convertClaudeWebSearchToolToCodex.
fn web_search_tool(tool: &Res<'_>) -> Vec<u8> {
    let mut out = br#"{"type":"web_search"}"#.to_vec();
    let domains = tool.get("allowed_domains");
    if domains.is_array() {
        gj::set_raw(&mut out, "filters.allowed_domains", &domains.raw);
    }
    let location = tool.get("user_location");
    if location.is_object() {
        gj::set_raw(&mut out, "user_location", &location.raw);
    }
    out
}

fn strip_dialect_keywords(value: &mut GoValue) {
    let has_escape = |s: &str| common::has_unsupported_unicode_property_escape(s.as_bytes());
    match value {
        GoValue::Object(map) => {
            map.remove("$schema");
            map.remove("$id");
            if matches!(map.get("pattern"), Some(GoValue::String(p)) if has_escape(p)) {
                map.remove("pattern");
            }
            if let Some(GoValue::Object(patterns)) = map.get_mut("patternProperties") {
                patterns.retain(|k, _| !has_escape(k));
                patterns.values_mut().for_each(strip_dialect_keywords);
            }
            for key in common::SCHEMA_MAP_KEYWORDS {
                let key = std::str::from_utf8(key).unwrap_or_default();
                if key == "patternProperties" {
                    continue;
                }
                if let Some(GoValue::Object(sub)) = map.get_mut(key) {
                    sub.values_mut().for_each(strip_dialect_keywords);
                }
            }
            for key in common::SCHEMA_VALUE_KEYWORDS {
                match map.get_mut(std::str::from_utf8(key).unwrap_or_default()) {
                    Some(sub @ GoValue::Object(_)) => strip_dialect_keywords(sub),
                    Some(GoValue::Array(items)) => items.iter_mut().for_each(strip_dialect_keywords),
                    _ => {}
                }
            }
        }
        GoValue::Array(items) => items.iter_mut().for_each(strip_dialect_keywords),
        _ => {}
    }
}

/// normalizeToolParameters: an object schema with `properties`, without dialect keywords
/// or unsupported regex escapes, re-encoded without HTML escaping.
pub(crate) fn normalize_tool_parameters(raw: &[u8]) -> Vec<u8> {
    const DEFAULT: &[u8] = br#"{"type":"object","properties":{}}"#;
    let raw = trim_space(raw);
    if raw.is_empty() || raw == b"null" || !gj::valid(raw) {
        return DEFAULT.to_vec();
    }
    let Some(mut root) = GoValue::parse(raw).filter(|v| matches!(v, GoValue::Object(_))) else {
        return DEFAULT.to_vec();
    };
    strip_dialect_keywords(&mut root);
    let GoValue::Object(map) = &mut root else {
        return DEFAULT.to_vec();
    };
    let is_object = match map.get("type") {
        None | Some(GoValue::Null) => true,
        Some(GoValue::String(t)) if t.is_empty() => true,
        Some(GoValue::String(t)) => t == "object",
        Some(GoValue::Array(items)) => items.iter().any(|i| matches!(i, GoValue::String(s) if s == "object")),
        _ => false,
    };
    if matches!(map.get("type"), None | Some(GoValue::Null))
        || matches!(map.get("type"), Some(GoValue::String(t)) if t.is_empty())
    {
        map.insert("type".into(), GoValue::String("object".into()));
    }
    if is_object && matches!(map.get("properties"), None | Some(GoValue::Null)) {
        map.insert("properties".into(), GoValue::Object(Default::default()));
    }
    root.marshal_no_html()
}

/// codexSchemaMissesRequired: a declared property missing from its sibling `required`.
fn misses_required(schema: &Res<'_>) -> bool {
    if !schema.is_object() {
        return schema.is_array() && schema.array().iter().any(misses_required);
    }
    let properties = schema.get("properties");
    if properties.is_object() {
        let required = schema.get("required");
        if !required.is_array() {
            return !properties.map().is_empty();
        }
        let names: HashSet<Vec<u8>> = required
            .array()
            .iter()
            .filter(|i| i.kind == Kind::String)
            .map(|i| i.s.to_vec())
            .collect();
        if properties.map().iter().any(|(name, _)| !names.contains(name)) {
            return true;
        }
    }
    for key in common::SCHEMA_MAP_KEYWORDS {
        let children = schema.get(key);
        if children.is_object() {
            let mut miss = false;
            children.each(|_, child| {
                miss = misses_required(&child);
                !miss
            });
            if miss {
                return true;
            }
        }
    }
    common::SCHEMA_VALUE_KEYWORDS.iter().any(|key| {
        let child = schema.get(*key);
        child.exists() && misses_required(&child)
    })
}

/// codexClaudeTargetAcceptsGrokSignature.
fn accepts_grok_signature(model: &str) -> bool {
    let base = ck_wire::thinking::parse_suffix(model).model_name;
    go_lower(trim_space(base.as_bytes())).windows(4).any(|w| w == b"grok")
}

fn data_url(media_type: &[u8], data: &[u8]) -> Vec<u8> {
    [b"data:", media_type, b";base64,", data].concat()
}

/// An image source as a data URL: `data` or `base64`, with `media_type` or `mime_type`.
fn image_data_url(source: &Res<'_>) -> Option<Vec<u8>> {
    if !source.exists() {
        return None;
    }
    let mut data = source.get("data").bytes().into_owned();
    if data.is_empty() {
        data = source.get("base64").bytes().into_owned();
    }
    if data.is_empty() {
        return None;
    }
    let mut media = source.get("media_type").bytes().into_owned();
    if media.is_empty() {
        media = source.get("mime_type").bytes().into_owned();
    }
    if media.is_empty() {
        media = b"application/octet-stream".to_vec();
    }
    Some(data_url(&media, &data))
}

/// claudeImageInputPart: inline bytes as a data URL, an http(s) url source, or a file id.
fn image_input_part(source: &Res<'_>) -> Option<Vec<u8>> {
    if let Some(url) = image_data_url(source) {
        return Some(input_image(&url));
    }
    let kind = source.get("type").bytes();
    let url = source.get("url").bytes();
    if &*kind == b"url" && common::is_http_url(&url) {
        return Some(input_image(trim_space(&url)));
    }
    let file_id = source.get("file_id").bytes();
    if &*kind == b"file" && !file_id.is_empty() {
        let mut part = br#"{"type":"input_image"}"#.to_vec();
        gj::set_str(&mut part, "file_id", &file_id);
        return Some(part);
    }
    None
}

/// claudeDocumentDataURL: an inline PDF as a Codex input_file URL. Anything else, a
/// file id included, cannot be represented.
fn document_data_url(part: &Res<'_>) -> Option<Vec<u8>> {
    let source = part.get("source");
    if source.get("type").bytes().as_ref() != b"base64" {
        return None;
    }
    let media = trim_space(&source.get("media_type").bytes()).to_vec();
    if !eq_fold(&media, "application/pdf") {
        return None;
    }
    let mut data = source.get("data").bytes().into_owned();
    if data.is_empty() {
        data = source.get("base64").bytes().into_owned();
    }
    (!data.is_empty()).then(|| data_url(&media, &data))
}

fn input_image(url: &[u8]) -> Vec<u8> {
    let mut part = br#"{"type":"input_image","image_url":""}"#.to_vec();
    gj::set_str(&mut part, "image_url", url);
    part
}

/// Message conversion state (the `messages` loop).
struct Messages<'m> {
    model: &'m str,
    preserve: bool,
    names: &'m HashMap<Vec<u8>, Vec<u8>>,
    items: Vec<Vec<u8>>,
    pending_tool_uses: Vec<Vec<u8>>,
    pending_reminders: Vec<Vec<u8>>,
    drops: UserTurnDrops,
}

impl Messages<'_> {
    fn flush_reminders(&mut self) {
        self.items.append(&mut self.pending_reminders);
    }

    fn message(&mut self, message: &Res<'_>) {
        let role = message.get("role").bytes().into_owned();
        if role == b"system" {
            self.system_message(message);
            return;
        }
        let content = message.get("content");
        let mut parts = if content.is_array() { content.array() } else { vec![] };
        if role == b"user" && !self.pending_tool_uses.is_empty() && content.is_array() {
            parts = common::align_claude_tool_results(parts, &self.pending_tool_uses);
        }
        self.pending_tool_uses.clear();
        let mut current: Vec<Vec<u8>> = vec![];
        // Counts only what this turn itself sends, not reminders flushed beside it. Empty
        // text is forwarded but does not count: it can hide an emptied attachment.
        let mut sendable = 0;
        let user = role == b"user";
        if content.is_array() {
            for part in &parts {
                let kind = part.get("type").bytes();
                match kind.as_ref() {
                    b"text" => {
                        self.flush_reminders();
                        let text = part.get("text").bytes();
                        sendable += usize::from(!text.is_empty());
                        current.push(Self::text_part(&role, &text));
                    }
                    b"thinking" => {
                        self.thinking_part(&role, part, &mut current);
                    }
                    b"image" => {
                        self.flush_reminders();
                        match image_input_part(&part.get("source")) {
                            Some(image) => {
                                current.push(image);
                                sendable += 1;
                            }
                            None if user => self.drops.drop_part("image"),
                            None => {}
                        }
                    }
                    b"document" | b"container_upload" => {
                        self.flush_reminders();
                        match document_data_url(part) {
                            Some(url) => {
                                let mut file =
                                    br#"{"type":"input_file","file_data":"","filename":"document.pdf"}"#.to_vec();
                                gj::set_str(&mut file, "file_data", url);
                                current.push(file);
                                sendable += 1;
                            }
                            None if user => self.drops.drop_part(&String::from_utf8_lossy(&kind)),
                            None => {}
                        }
                    }
                    b"tool_use" => {
                        self.tool_use(&role, part, &mut current);
                    }
                    b"tool_result" => {
                        self.flush_message(&role, &mut current);
                        self.items.push(tool_result(part));
                        sendable += 1;
                    }
                    _ => {}
                }
            }
            self.flush_message(&role, &mut current);
            self.flush_reminders();
        } else if content.kind == Kind::String {
            sendable += usize::from(!content.s.is_empty());
            current.push(Self::text_part(&role, &content.s));
            self.flush_message(&role, &mut current);
            self.flush_reminders();
        }
        if user {
            self.drops.end_turn(sendable);
        }
    }

    fn system_message(&mut self, message: &Res<'_>) {
        if let Some(text) = common::claude_message_system_reminder_text(&message.get("content")) {
            let mut item = br#"{"type":"message","role":"user","content":[{"type":"input_text","text":""}]}"#.to_vec();
            gj::set_str(&mut item, "content.0.text", &text);
            if self.pending_tool_uses.is_empty() {
                self.items.push(item);
            } else {
                self.pending_reminders.push(item);
            }
        }
    }

    fn flush_message(&mut self, role: &[u8], current: &mut Vec<Vec<u8>>) {
        if !current.is_empty() {
            let mut item = br#"{"type":"message","role":""}"#.to_vec();
            gj::set_str(&mut item, "role", role);
            gj::set_raw(&mut item, "content", gj::join(current));
            self.items.push(item);
            current.clear();
        }
    }

    fn text_part(role: &[u8], text: &[u8]) -> Vec<u8> {
        let kind = if role == b"assistant" {
            "output_text"
        } else {
            "input_text"
        };
        let mut part = br#"{"type":"","text":""}"#.to_vec();
        gj::set_str(&mut part, "type", kind);
        gj::set_str(&mut part, "text", text);
        part
    }

    fn thinking_part(&mut self, role: &[u8], part: &Res<'_>, current: &mut Vec<Vec<u8>>) {
        if role != b"assistant" {
            return;
        }
        let Some(signature) = self.reasoning_signature(part) else {
            return;
        };
        self.flush_message(role, current);
        let mut item = br#"{"type":"reasoning","summary":[],"content":null}"#.to_vec();
        gj::set_str(&mut item, "encrypted_content", &signature);
        self.items.push(item);
    }

    fn tool_use(&mut self, role: &[u8], part: &Res<'_>, current: &mut Vec<Vec<u8>>) {
        self.flush_message(role, current);
        let id = part.get("id").bytes().into_owned();
        if !id.is_empty() {
            self.pending_tool_uses.push(id.clone());
        }
        let mut call = br#"{"type":"function_call"}"#.to_vec();
        gj::set_str(&mut call, "call_id", shorten_call_id(&id));
        gj::set_str(&mut call, "name", mapped_name(self.names, &part.get("name").bytes()));
        gj::set_str(&mut call, "arguments", &part.get("input").raw);
        self.items.push(call);
    }

    /// appendReasoningContent's signature choice: GPT-compatible signatures, then (compat)
    /// empty or unknown-format ones, then Grok encrypted content for Grok targets.
    fn reasoning_signature(&self, part: &Res<'_>) -> Option<Vec<u8>> {
        let raw = part.get("signature").bytes().into_owned();
        if let Some(signature) = sig::compatible_signature_for_provider(Provider::Gpt, &raw) {
            return Some(signature.into_bytes());
        }
        let blank = trim_space(&raw).is_empty();
        if self.preserve
            && part.get("signature").kind == Kind::String
            && !blank
            && sig::detect_provider_for_block(&raw, BlockKind::ClaudeThinking) == Provider::Unknown
        {
            return Some(raw);
        }
        if self.preserve && blank {
            return Some(raw);
        }
        if !accepts_grok_signature(self.model) || sig::inspect_grok_encrypted_content(&raw).is_err() {
            return None;
        }
        Some(raw)
    }
}

/// A `tool_result` block as a `function_call_output` item.
fn tool_result(part: &Res<'_>) -> Vec<u8> {
    let mut output = br#"{"type":"function_call_output"}"#.to_vec();
    gj::set_str(
        &mut output,
        "call_id",
        shorten_call_id(&part.get("tool_use_id").bytes()),
    );
    let content = part.get("content");
    let mut items = vec![];
    if content.is_array() {
        for block in content.array() {
            match block.get("type").bytes().as_ref() {
                b"image" => {
                    if let Some(url) = image_data_url(&block.get("source")) {
                        items.push(input_image(&url));
                    }
                }
                b"text" => {
                    let mut text = br#"{"type":"input_text","text":""}"#.to_vec();
                    gj::set_str(&mut text, "text", block.get("text").bytes());
                    items.push(text);
                }
                _ => {}
            }
        }
    }
    if items.is_empty() {
        gj::set_str(&mut output, "output", content.bytes());
    } else {
        gj::set_raw(&mut output, "output", gj::join(&items));
    }
    output
}

fn eq_fold(value: &[u8], token: &str) -> bool {
    use ck_wire::gostr::GoStr;
    String::from_utf8_lossy(value).go_eq_fold(token)
}
