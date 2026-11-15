use serde_json::Value;

/// Historical transport fields stay exact; request policy comes from the client input.
pub(super) fn frame(s: &Value, actual: &str, recorded: &str) {
    let mut actual: Value = serde_json::from_str(actual).unwrap();
    let mut recorded: Value = serde_json::from_str(recorded).unwrap();
    body(
        s,
        actual["payload"]["body"].as_str().unwrap().as_bytes(),
        recorded["payload"]["body"].as_str().unwrap(),
    );
    actual["payload"].as_object_mut().unwrap().remove("body");
    recorded["payload"].as_object_mut().unwrap().remove("body");
    assert_eq!(actual, recorded, "{}: relayed transport frame", s["name"]);
}

pub(super) fn body(s: &Value, actual: &[u8], recorded: &str) {
    let mut actual: Value = serde_json::from_slice(actual).unwrap();
    let mut recorded: Value = serde_json::from_str(recorded).unwrap();
    let input: Value = serde_json::from_str(s["payload"].as_str().unwrap()).unwrap();
    if s["op"] != "count" {
        if s["source"] == "gemini" {
            for key in ["safetySettings", "tools"] {
                assert_eq!(actual.get(key), input.get(key), "{}: {key}", s["name"]);
                actual.as_object_mut().unwrap().remove(key);
                recorded.as_object_mut().unwrap().remove(key);
            }
            for key in [
                "maxOutputTokens",
                "responseMimeType",
                "responseSchema",
                "responseJsonSchema",
            ] {
                assert_eq!(
                    actual["generationConfig"].get(key),
                    input["generationConfig"].get(key),
                    "{}: {key}",
                    s["name"]
                );
                remove_control(&mut actual, key);
                remove_control(&mut recorded, key);
            }
        } else if s["source"] == "claude" {
            assert_eq!(
                actual["generationConfig"].get("maxOutputTokens"),
                input.get("max_tokens"),
                "{}: maxOutputTokens",
                s["name"]
            );
            remove_control(&mut actual, "maxOutputTokens");
            remove_control(&mut recorded, "maxOutputTokens");
        }
    }
    assert_eq!(actual, recorded, "{}: unchanged request fields", s["name"]);
}

fn remove_control(body: &mut Value, key: &str) {
    if let Some(config) = body.get_mut("generationConfig").and_then(Value::as_object_mut) {
        config.remove(key);
        if config.is_empty() {
            body.as_object_mut().unwrap().remove("generationConfig");
        }
    }
}

/// Gemini chunks use the recorded upstream fields, including interim usageMetadata.
pub(super) fn chunks(s: &Value) -> Vec<Value> {
    let unregistered = s["source"] == "codex";
    if (s["source"] != "gemini" && !unregistered) || s["op"] != "stream" || !s["error"].is_null() {
        return s["chunks"].as_array().cloned().unwrap_or_default();
    }
    let mut chunks: Vec<Value> = s["replies"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|reply| reply["type"] == "stream_chunk")
        .map(|reply| {
            let data = reply["payload"]["data"].as_str().unwrap().trim();
            Value::String(data.strip_prefix("data:").unwrap_or(data).trim().to_owned())
        })
        .collect();
    if unregistered {
        chunks.push(Value::String("[DONE]".into()));
    }
    chunks
}

#[derive(Debug, PartialEq)]
pub(super) enum WireToken {
    Framing(u8),
    Object(Value),
}

/// Canonicalize JSON objects only; retain SSE/array framing and non-JSON bytes.
pub(super) fn semantic_wire(bytes: &[u8]) -> Vec<WireToken> {
    let mut out = Vec::new();
    let mut cursor = 0;
    while cursor < bytes.len() {
        if bytes[cursor] != b'{' {
            out.push(WireToken::Framing(bytes[cursor]));
            cursor += 1;
            continue;
        }
        let mut documents = serde_json::Deserializer::from_slice(&bytes[cursor..]).into_iter::<Value>();
        let value = documents.next().unwrap().unwrap();
        cursor += documents.byte_offset();
        out.push(WireToken::Object(value));
    }
    out
}
