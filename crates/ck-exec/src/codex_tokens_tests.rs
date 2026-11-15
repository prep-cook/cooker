//! Replays tests/fixtures/codex_tokens_go.json: Go's `CodexExecutor.CountTokens` at
//! 6fecc6e (tests/reference/codex_client/README.md), through the Rust executor.

use super::*;
use bytes::Bytes;
use ck_core::config::Config;
use ck_core::credential::{Credential, Source};
use ck_core::exec::{Caller, ExecRequest, Operation, ResponseBody};
use ck_core::format::Format;
use serde_json::Value;

fn request(v: &Value) -> ExecRequest {
    let source = Format::parse(v["from"].as_str().unwrap()).unwrap();
    let mut headers = http::HeaderMap::new();
    for (k, val) in v["headers"].as_object().into_iter().flatten() {
        headers.insert(
            http::HeaderName::try_from(k.as_str()).unwrap(),
            val.as_str().unwrap().parse().unwrap(),
        );
    }
    let body = Bytes::from(v["in"].as_str().unwrap().to_owned());
    ExecRequest {
        operation: Operation::CountTokens,
        source_format: source,
        response_format: Format::parse(v["response"].as_str().unwrap()).unwrap(),
        requested_model: v["model"].as_str().unwrap().into(),
        model: v["model"].as_str().unwrap().into(),
        original_body: body.clone(),
        body,
        stream: false,
        alt: None,
        session: None,
        headers,
        execution_session: None,
        derived_session: None,
        request_path: String::new(),
        resolved_model: None,
        usage: Default::default(),
        caller: Caller {
            principal: "client-key-FAKE".into(),
            source: "authorization",
            provider: String::new(),
        },
    }
}

#[tokio::test]
async fn count_tokens_matches_go() {
    let vectors: Vec<Value> = serde_json::from_str(include_str!("../tests/fixtures/codex_tokens_go.json")).unwrap();
    assert_eq!(vectors.len(), 30);
    // Nothing is sent: the base URL is unroutable and the token stays unused.
    let credential = Credential {
        id: "codex-tokens.json".into(),
        provider: "codex".into(),
        source: Source::File("/fake/codex-tokens.json".into()),
        disabled: false,
        label: "codex".into(),
        attributes: [("base_url".to_owned(), "http://127.0.0.1:1".to_owned())].into(),
        metadata: Default::default(),
        revision: 0,
    };
    let executor = crate::codex::CodexExecutor::new().unwrap();
    let cfg = Config::default();
    for v in &vectors {
        assert!(v["err"].is_null(), "Go failed: {v}");
        let response = executor.execute(&credential, request(v), &cfg).await.unwrap();
        let ResponseBody::Buffered(body) = response.body else {
            panic!("buffered count expected")
        };
        assert_eq!(
            std::str::from_utf8(&body).unwrap(),
            v["out"].as_str().unwrap(),
            "{} {}",
            v["from"],
            v["model"]
        );
    }
}

#[test]
fn encodings_follow_go_model_prefixes() {
    for (model, want) in [
        ("gpt-5.5", Encoding::O200kBase),
        (" GPT-4.1-mini", Encoding::O200kBase),
        ("gpt-4o", Encoding::O200kBase),
        ("gpt-4", Encoding::Cl100kBase),
        ("gpt-3.5-turbo", Encoding::Cl100kBase),
        ("o3", Encoding::Cl100kBase),
        ("", Encoding::Cl100kBase),
    ] {
        assert_eq!(encoding_for_model(model), want, "{model}");
    }
}

/// Go `TestCodexExecutorCountTokensPreservesToolNumberSchemas`: a Codex client's
/// `number` tool parameters stay numbers in the counted body, from Responses and Claude,
/// with and without an is-compat binding. The integer normalization is for non-Codex
/// executors only.
/// A message whose content is one string counts like the same text in one part.
#[test]
fn string_message_content_counts_like_a_part() {
    let count = |body: &str| count_input_tokens(Encoding::O200kBase, body.as_bytes()).unwrap();
    let string = count(r#"{"input":[{"type":"message","role":"user","content":"hello there"}]}"#);
    let parts =
        count(r#"{"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hello there"}]}]}"#);
    assert_eq!(string, parts);
    assert!(string > 0);
}

#[test]
fn counted_body_keeps_tool_number_schemas() {
    use ck_core::exec::{ResolvedModel, ResolvedSource};
    use ck_core::registry::ModelInfo;
    let properties =
        r#"{"yield_time_ms":{"type":"number"},"timeout_ms":{"type":"number"},"max_output_tokens":{"type":"number"}}"#;
    let sources = [
        (
            Format::OpenAIResponse,
            format!(
                r#"{{"input":"hi","tools":[{{"type":"function","name":"exec_command","parameters":{{"type":"object","properties":{properties}}}}}]}}"#
            ),
        ),
        (
            Format::Claude,
            format!(
                r#"{{"messages":[{{"role":"user","content":"hi"}}],"tools":[{{"name":"exec_command","input_schema":{{"type":"object","properties":{properties}}}}}]}}"#
            ),
        ),
    ];
    for (source, payload) in sources {
        for compat in [false, true] {
            let mut req = request(&serde_json::json!({
                "from": source.as_str(),
                "response": source.as_str(),
                "model": "gpt-5.4",
                "in": payload,
                "headers": {"user-agent": "codex_cli_rs/0.1"},
            }));
            let mut raw = serde_json::Map::new();
            if compat {
                raw.insert("is_compat".into(), Value::Bool(true));
            }
            req.resolved_model = Some(ResolvedModel {
                info: ModelInfo {
                    id: "gpt-5.4".into(),
                    kind: String::new(),
                    thinking: None,
                    raw,
                },
                source: ResolvedSource::ApiKey,
            });
            let body = crate::codex::count_tokens_body(&req, &Config::default(), "gpt-5.4").unwrap();
            let body: Value = serde_json::from_slice(&body).unwrap();
            let counted = &body["tools"][0]["parameters"]["properties"];
            for field in ["yield_time_ms", "timeout_ms", "max_output_tokens"] {
                assert_eq!(
                    counted[field]["type"], "number",
                    "{source:?} compat={compat} {field}: {body}"
                );
            }
        }
    }
}
