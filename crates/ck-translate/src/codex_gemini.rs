//! Gemini request -> Codex (OpenAI Responses) request, and Codex events back to Gemini
//! responses (internal/translator/codex/gemini).

use std::collections::{HashMap, HashSet};

use ck_wire::json::{self as gj, Kind, Res};
use sha2::{Digest, Sha256};

use crate::common::{format_rfc3339_utc, go_lower, is_gemini_thought_part, trim_space};
use crate::stream::GoStream;
use crate::{Error, Registered, ResponseCtx, gemini_gemini};
#[path = "codex_gemini_request.rs"]
mod request;
use crate::responses_policy::Policy;
pub(crate) use request::convert_with_policy;

pub static PAIR: Registered = registered!(
    Gemini -> Codex,
    request: |ctx, body| Ok(convert_with_policy(ctx.model, body, Policy::Codex)),
    non_stream: non_stream,
    go_stream: go_stream,
    token_count: Some(gemini_gemini::token_count),
);

const NAME_LIMIT: usize = 64;

/// shortenNameIfNeeded.
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

/// buildShortNameMap over every declared function name (empty names included).
fn short_name_map(raw: &[u8]) -> HashMap<Vec<u8>, Vec<u8>> {
    let mut names: Vec<Vec<u8>> = vec![];
    let tools = gj::get(raw, "tools");
    if tools.is_array() {
        for tool in tools.array() {
            let declarations = tool.get("functionDeclarations");
            if declarations.is_array() {
                for declaration in declarations.array() {
                    let name = declaration.get("name");
                    if name.exists() {
                        names.push(name.bytes().into_owned());
                    }
                }
            }
        }
    }
    let mut used: HashSet<Vec<u8>> = HashSet::new();
    let mut map = HashMap::new();
    for name in names {
        let candidate = shorten_name(&name);
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
        map.insert(name, unique);
    }
    map
}

fn mapped_name(map: &HashMap<Vec<u8>, Vec<u8>>, name: &[u8]) -> Vec<u8> {
    map.get(name).cloned().unwrap_or_else(|| shorten_name(name))
}

fn message_with_part(role: &[u8], part: &[u8]) -> Vec<u8> {
    let mut message = br#"{"type":"message","role":"","content":[]}"#.to_vec();
    gj::set_str(&mut message, "role", role);
    gj::set_raw(&mut message, "content", gj::join(&[part]));
    message
}

fn typed_text(kind: &str, text: &[u8]) -> Vec<u8> {
    let mut part = b"{}".to_vec();
    gj::set_str(&mut part, "type", kind);
    gj::set_str(&mut part, "text", text);
    part
}

fn first_of<'a>(value: &Res<'a>, paths: [&str; 2]) -> Res<'a> {
    let first = value.get(paths[0]);
    if first.exists() { first } else { value.get(paths[1]) }
}

fn audio_format(mime: &[u8]) -> &'static str {
    match go_lower(trim_space(mime)).as_slice() {
        b"audio/wav" | b"audio/wave" | b"audio/x-wav" => "wav",
        b"audio/flac" => "flac",
        b"audio/opus" | b"audio/ogg" => "opus",
        b"audio/pcm" | b"audio/l16" => "pcm16",
        _ => "mp3",
    }
}

fn file_name(mime: &[u8]) -> &'static str {
    let lower = go_lower(trim_space(mime));
    match lower.as_slice() {
        b"application/pdf" => "document.pdf",
        b"text/plain" => "document.txt",
        b"text/csv" => "document.csv",
        b"application/json" => "document.json",
        b"application/xml" | b"text/xml" => "document.xml",
        _ if lower.starts_with(b"video/") => "video",
        _ => "document",
    }
}

/// codexContentPartFromGeminiInlineData.
fn inline_data_part(part: &Res<'_>) -> Option<Vec<u8>> {
    let inline = first_of(part, ["inlineData", "inline_data"]);
    if !inline.exists() {
        return None;
    }
    let mut mime = inline.get("mimeType").bytes().into_owned();
    if mime.is_empty() {
        mime = inline.get("mime_type").bytes().into_owned();
    }
    let data = inline.get("data").bytes().into_owned();
    if mime.is_empty() || data.is_empty() {
        return None;
    }
    let lower = go_lower(&mime);
    Some(if lower.starts_with(b"image/") {
        let mut out = br#"{"type":"input_image","image_url":""}"#.to_vec();
        gj::set_str(
            &mut out,
            "image_url",
            [b"data:", &mime[..], b";base64,", &data[..]].concat(),
        );
        out
    } else if lower.starts_with(b"audio/") {
        let mut out = br#"{"type":"input_audio","input_audio":{"data":"","format":""}}"#.to_vec();
        gj::set_str(&mut out, "input_audio.data", &data);
        gj::set_str(&mut out, "input_audio.format", audio_format(&mime));
        out
    } else {
        let mut out = br#"{"type":"input_file","file_data":"","filename":""}"#.to_vec();
        gj::set_str(&mut out, "file_data", &data);
        gj::set_str(&mut out, "filename", file_name(&mime));
        out
    })
}

/// codexContentPartFromGeminiFileData.
fn file_data_part(part: &Res<'_>) -> Option<Vec<u8>> {
    let file = first_of(part, ["fileData", "file_data"]);
    if !file.exists() {
        return None;
    }
    let mut uri = file.get("fileUri").bytes().into_owned();
    if uri.is_empty() {
        uri = file.get("file_uri").bytes().into_owned();
    }
    if uri.is_empty() {
        return None;
    }
    let mut mime = file.get("mimeType").bytes().into_owned();
    if mime.is_empty() {
        mime = file.get("mime_type").bytes().into_owned();
    }
    let lower = go_lower(&mime);
    if lower.starts_with(b"image/") {
        let mut out = br#"{"type":"input_image","image_url":""}"#.to_vec();
        gj::set_str(&mut out, "image_url", &uri);
        return Some(out);
    }
    if lower.starts_with(b"video/") || lower.starts_with(b"application/") || lower.starts_with(b"text/") {
        let mut out = br#"{"type":"input_file","file_url":"","filename":""}"#.to_vec();
        gj::set_str(&mut out, "file_url", &uri);
        gj::set_str(&mut out, "filename", file_name(&mime));
        return Some(out);
    }
    let mut info = [&b"File: "[..], &uri].concat();
    if !mime.is_empty() {
        info.extend_from_slice(b" (Type: ");
        info.extend_from_slice(&mime);
        info.push(b')');
    }
    Some(typed_text("input_text", &info))
}

/// getGeminiCallID.
fn call_id(value: &Res<'_>) -> Vec<u8> {
    let id = trim_space(&value.get("id").bytes()).to_vec();
    if !id.is_empty() {
        return id;
    }
    trim_space(&value.get("call_id").bytes()).to_vec()
}

/// cleanGeminiCodexToolParameters: no `$schema`, `additionalProperties: false`.
pub(crate) fn clean_parameters(parameters: &Res<'_>) -> Vec<u8> {
    let mut cleaned = parameters.raw.to_vec();
    if parameters.get("$schema").exists() {
        gj::delete(&mut cleaned, "$schema");
    }
    if parameters.get("additionalProperties").kind != Kind::False {
        gj::set_bool(&mut cleaned, "additionalProperties", false);
    }
    cleaned
}

/// setCodexToolChoiceFromGeminiToolConfig.
fn set_tool_choice(out: &mut Vec<u8>, config: &Res<'_>) {
    if !config.exists() {
        return;
    }
    match config.get("mode").bytes().as_ref() {
        b"NONE" => {
            gj::set_str(out, "tool_choice", "none");
        }
        b"AUTO" => {
            let current = gj::get(out, "tool_choice");
            if current.kind != Kind::String || current.s.as_ref() != b"auto" {
                gj::set_str(out, "tool_choice", "auto");
            }
        }
        b"ANY" => {
            let allowed = config.get("allowedFunctionNames");
            let items = allowed.array();
            if allowed.is_array() && items.len() == 1 {
                let mut choice = br#"{"type":"function","name":""}"#.to_vec();
                gj::set_str(&mut choice, "name", shorten_name(&items[0].bytes()));
                gj::set_raw(out, "tool_choice", choice);
            } else {
                gj::set_str(out, "tool_choice", "required");
            }
        }
        _ => {}
    }
}

fn effort_of(level: &Res<'_>) -> Vec<u8> {
    go_lower(trim_space(&level.bytes()))
}

// ---------------------------------------------------------------------------------------
// Responses (codex_gemini_response.go)

#[derive(Default)]
struct State {
    model: String,
    original: Vec<u8>,
    response_id: Vec<u8>,
    stored: Option<Vec<u8>>,
    has_text_delta: bool,
    image_hashes: HashMap<Vec<u8>, [u8; 32]>,
}

pub fn go_stream(ctx: &ResponseCtx<'_>) -> Box<dyn GoStream> {
    Box::new(State {
        model: ctx.model.to_owned(),
        original: ctx.original_request.to_vec(),
        ..Default::default()
    })
}

/// buildReverseMapFromGeminiOriginal: short name -> declared name.
fn short_to_original(raw: &[u8]) -> HashMap<Vec<u8>, Vec<u8>> {
    short_name_map(raw)
        .into_iter()
        .map(|(orig, short)| (short, orig))
        .collect()
}

/// setGeminiFunctionCallID.
fn set_call_id(part: &mut Vec<u8>, item: &Res<'_>) {
    for key in ["call_id", "id"] {
        let id = trim_space(&item.get(key).bytes()).to_vec();
        if !id.is_empty() {
            gj::set_str(part, "functionCall.id", &id);
            return;
        }
    }
}

fn function_call_part(template: &[u8], item: &Res<'_>, original: &[u8]) -> Vec<u8> {
    let mut part = template.to_vec();
    let name = item.get("name").bytes().into_owned();
    let name = short_to_original(original).get(&name).cloned().unwrap_or(name);
    gj::set_str(&mut part, "functionCall.name", &name);
    let arguments = item.get("arguments").bytes();
    if !arguments.is_empty() && gj::parse(&arguments).is_object() {
        gj::set_raw(&mut part, "functionCall.args", &arguments);
    }
    set_call_id(&mut part, item);
    part
}

/// mimeTypeFromCodexOutputFormat.
fn image_mime(format: &[u8]) -> Vec<u8> {
    if format.is_empty() {
        return b"image/png".to_vec();
    }
    if format.contains(&b'/') {
        return format.to_vec();
    }
    match go_lower(format).as_slice() {
        b"jpg" | b"jpeg" => b"image/jpeg".to_vec(),
        b"webp" => b"image/webp".to_vec(),
        b"gif" => b"image/gif".to_vec(),
        _ => b"image/png".to_vec(),
    }
}

fn image_part(data: &[u8], format: &[u8]) -> Vec<u8> {
    let mut part = br#"{"inlineData":{"data":"","mimeType":""}}"#.to_vec();
    gj::set_str(&mut part, "inlineData.data", data);
    gj::set_str(&mut part, "inlineData.mimeType", image_mime(format));
    part
}

/// codexGeminiIncompleteFinishReason.
fn incomplete_reason(reason: &[u8]) -> &'static str {
    match reason {
        b"max_tokens" | b"max_output_tokens" => "MAX_TOKENS",
        b"content_filter" => "SAFETY",
        _ => "OTHER",
    }
}

impl State {
    /// A repeated image (same item ID and payload) is sent once.
    fn image_seen(&mut self, id: &[u8], data: &[u8]) -> bool {
        if id.is_empty() {
            return false;
        }
        let hash: [u8; 32] = Sha256::digest(data).into();
        if self.image_hashes.get(id) == Some(&hash) {
            return true;
        }
        self.image_hashes.insert(id.to_vec(), hash);
        false
    }

    fn translate(&mut self, line: &[u8]) -> Vec<Vec<u8>> {
        let Some(rest) = line.strip_prefix(b"data:") else {
            return vec![];
        };
        let root = gj::parse(trim_space(rest));
        let kind = root.get("type").bytes().into_owned();
        let mut out = br#"{"candidates":[{"content":{"role":"model","parts":[]}}],"usageMetadata":{"trafficType":"PROVISIONED_THROUGHPUT"},"modelVersion":"gemini-2.5-pro","createTime":"2025-08-15T02:52:03.884209Z","responseId":"06CeaPH7NaCU48APvNXDyA4"}"#.to_vec();
        gj::set_str(&mut out, "modelVersion", &self.model);
        let created_at = root.get("response.created_at");
        if created_at.exists() {
            gj::set_str(&mut out, "createTime", format_rfc3339_utc(created_at.int()));
        }
        gj::set_str(&mut out, "responseId", &self.response_id);

        if kind == b"response.image_generation_call.partial_image" {
            let data = root.get("partial_image_b64").bytes().into_owned();
            if data.is_empty() || self.image_seen(&root.get("item_id").bytes(), &data) {
                return vec![];
            }
            gj::set_items(
                &mut out,
                "candidates.0.content.parts",
                &[image_part(&data, &root.get("output_format").bytes())],
            );
            return vec![out];
        }
        let item = root.get("item");
        if kind == b"response.output_item.done" {
            match item.get("type").bytes().as_ref() {
                b"image_generation_call" => {
                    let data = item.get("result").bytes().into_owned();
                    if data.is_empty() || self.image_seen(&item.get("id").bytes(), &data) {
                        return vec![];
                    }
                    gj::set_items(
                        &mut out,
                        "candidates.0.content.parts",
                        &[image_part(&data, &item.get("output_format").bytes())],
                    );
                    return vec![out];
                }
                b"function_call" => {
                    let part = function_call_part(br#"{"functionCall":{"name":"","args":{}}}"#, &item, &self.original);
                    gj::set_items(&mut out, "candidates.0.content.parts", &[part]);
                    gj::set_str(&mut out, "candidates.0.finishReason", "STOP");
                    self.stored = Some(out);
                    return vec![];
                }
                _ => {}
            }
        }

        self.translate_event(&root, &kind, &item, out)
    }

    fn translate_event(&mut self, root: &Res<'_>, kind: &[u8], item: &Res<'_>, mut out: Vec<u8>) -> Vec<Vec<u8>> {
        match kind {
            b"response.created" => {
                gj::set_str(&mut out, "modelVersion", root.get("response.model").bytes());
                gj::set_str(&mut out, "responseId", root.get("response.id").bytes());
                self.response_id = root.get("response.id").bytes().into_owned();
            }
            b"response.reasoning_summary_text.delta" => {
                let mut part = br#"{"thought":true,"text":""}"#.to_vec();
                gj::set_str(&mut part, "text", root.get("delta").bytes());
                gj::set_items(&mut out, "candidates.0.content.parts", &[part]);
            }
            b"response.output_text.delta" => {
                self.has_text_delta = true;
                let mut part = br#"{"text":""}"#.to_vec();
                gj::set_str(&mut part, "text", root.get("delta").bytes());
                gj::set_items(&mut out, "candidates.0.content.parts", &[part]);
            }
            b"response.output_item.done" => {
                // Final message text when no deltas arrived.
                let content = item.get("content");
                if item.get("type").bytes().as_ref() != b"message" || self.has_text_delta || !content.is_array() {
                    return vec![];
                }
                let mut wrote = false;
                content.each(|_, part| {
                    let text = part.get("text").bytes();
                    if part.get("type").bytes().as_ref() == b"output_text" && !text.is_empty() {
                        let mut text_part = br#"{"text":""}"#.to_vec();
                        gj::set_str(&mut text_part, "text", &text);
                        gj::set_raw(&mut out, "candidates.0.content.parts.-1", text_part);
                        wrote = true;
                    }
                    true
                });
                if !wrote {
                    return vec![];
                }
                self.has_text_delta = true;
                return vec![out];
            }
            b"response.completed" | b"response.incomplete" => {
                let input = root.get("response.usage.input_tokens").int();
                let output = root.get("response.usage.output_tokens").int();
                gj::set_int(&mut out, "usageMetadata.promptTokenCount", input);
                gj::set_int(&mut out, "usageMetadata.candidatesTokenCount", output);
                gj::set_int(&mut out, "usageMetadata.totalTokenCount", input.wrapping_add(output));
                if kind == b"response.incomplete" {
                    gj::set_str(
                        &mut out,
                        "candidates.0.finishReason",
                        incomplete_reason(&root.get("response.incomplete_details.reason").bytes()),
                    );
                }
            }
            _ => return vec![],
        }
        match self.stored.take() {
            Some(stored) if !stored.is_empty() => vec![stored, out],
            _ => vec![out],
        }
    }
}

impl GoStream for State {
    fn line(&mut self, line: &[u8]) -> Result<Vec<Vec<u8>>, Error> {
        Ok(self.translate(line))
    }
}

/// ConvertCodexResponseToGeminiNonStream.
pub fn non_stream(ctx: &ResponseCtx<'_>, body: &[u8]) -> Result<Vec<u8>, Error> {
    let root = gj::parse(body);
    let kind = root.get("type").bytes();
    if kind.as_ref() != b"response.completed" && kind.as_ref() != b"response.incomplete" {
        return Ok(vec![]);
    }
    let mut out = br#"{"candidates":[{"content":{"role":"model","parts":[]},"finishReason":"STOP"}],"usageMetadata":{"trafficType":"PROVISIONED_THROUGHPUT"},"modelVersion":"","createTime":"","responseId":""}"#.to_vec();
    gj::set_str(&mut out, "modelVersion", ctx.model);
    let response = root.get("response");
    if !response.exists() {
        return Ok(out);
    }
    if kind.as_ref() == b"response.incomplete" {
        gj::set_str(
            &mut out,
            "candidates.0.finishReason",
            incomplete_reason(&response.get("incomplete_details.reason").bytes()),
        );
    }
    non_stream_metadata(&mut out, &response);
    let output = response.get("output");
    if output.is_array() {
        let mut parts: Vec<Vec<u8>> = vec![];
        let mut calls: Vec<Vec<u8>> = vec![];
        output.each(|_, item| {
            match item.get("type").bytes().as_ref() {
                b"reasoning" => {
                    parts.append(&mut calls);
                    let content = item.get("content");
                    if content.exists() {
                        let mut part = br#"{"text":"","thought":true}"#.to_vec();
                        gj::set_str(&mut part, "text", content.bytes());
                        parts.push(part);
                    }
                }
                b"message" => {
                    parts.append(&mut calls);
                    let content = item.get("content");
                    if content.is_array() {
                        content.each(|_, c| {
                            let text = c.get("text");
                            if c.get("type").bytes().as_ref() == b"output_text" && text.exists() {
                                let mut part = br#"{"text":""}"#.to_vec();
                                gj::set_str(&mut part, "text", text.bytes());
                                parts.push(part);
                            }
                            true
                        });
                    }
                }
                b"image_generation_call" => {
                    parts.append(&mut calls);
                    let data = item.get("result").bytes();
                    if !data.is_empty() {
                        parts.push(image_part(&data, &item.get("output_format").bytes()));
                    }
                }
                b"function_call" => {
                    calls.push(function_call_part(
                        br#"{"functionCall":{"args":{},"name":""}}"#,
                        &item,
                        ctx.original_request,
                    ));
                }
                _ => {}
            }
            true
        });
        parts.append(&mut calls);
        if !parts.is_empty() {
            gj::set_raw(&mut out, "candidates.0.content.parts", gj::join(&parts));
        }
    }
    Ok(out)
}

fn non_stream_metadata(out: &mut Vec<u8>, response: &Res<'_>) {
    let id = response.get("id");
    if id.exists() {
        gj::set_str(out, "responseId", id.bytes());
    }
    let created_at = response.get("created_at");
    if created_at.exists() {
        gj::set_str(out, "createTime", format_rfc3339_utc(created_at.int()));
    }
    let usage = response.get("usage");
    if usage.exists() {
        let input = usage.get("input_tokens").int();
        let output = usage.get("output_tokens").int();
        gj::set_int(out, "usageMetadata.promptTokenCount", input);
        gj::set_int(out, "usageMetadata.candidatesTokenCount", output);
        gj::set_int(out, "usageMetadata.totalTokenCount", input.wrapping_add(output));
    }
}
