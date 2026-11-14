//! Gemini and Responses requests bound for Chat or Anthropic, and Chat reasoning bound for
//! Gemini, from the 2026-11-15 live tests.
use super::*;
use crate::anthropic::request::AnthropicCapability;

enum To {
    Chat,
    Anthropic,
}

/// Runs `ingress` over `input` and encodes for `to`; a fence refusal is the error.
fn encode(mapped: &mut Mapped, mut ingress: impl RequestIngress, input: &Value, to: To) -> codec::Result<Value> {
    for chunk in serde_json::to_vec(input).unwrap().chunks(7) {
        ingress.advance(&mut mapped.writer, chunk).unwrap();
    }
    let output = match to {
        To::Chat => {
            let ready = ingress.finish(&mut mapped.writer, &codec::chat::request::ChatCapability)?;
            codec::chat::request::Egress {
                destination: "chat",
                upstream_stream: false,
            }
            .encode(&ready, &mut mapped.writer)
        }
        To::Anthropic => {
            let capability = AnthropicCapability {
                default_max_tokens: Some(4096),
            };
            let ready = ingress.finish(&mut mapped.writer, &capability)?;
            anthropic::request::Egress {
                destination: "anthropic",
                upstream_stream: false,
                default_max_tokens: capability.default_max_tokens,
            }
            .encode(&ready, &mut mapped.writer)
        }
    }
    .unwrap();
    Ok(serde_json::from_slice(&bytes(&output)).unwrap())
}
fn from_gemini(input: &Value, to: To) -> codec::Result<Value> {
    let mut mapped = Mapped::new();
    let ingress = gemini::request::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        "gemini-test",
        1024 * 1024,
    )
    .unwrap();
    encode(&mut mapped, ingress, input, to)
}
fn from_responses(input: &Value) -> codec::Result<Value> {
    let mut mapped = Mapped::new();
    let ingress = responses::request::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        1024 * 1024,
    )
    .unwrap();
    encode(&mut mapped, ingress, input, To::Anthropic)
}

/// A5r: `instructions` becomes the system prompt and `max_output_tokens` becomes `max_tokens`.
#[test]
fn responses_instructions_and_output_limit_reach_anthropic() {
    let input = json!({"model":"claude-haiku-5-5","instructions":"Be brief.","input":[{"role":"user","content":[{"type":"input_text","text":"Name one ocean in one word."}]}],"max_output_tokens":64});
    let out = from_responses(&input).unwrap();
    assert_eq!(out["system"], "Be brief.");
    assert_eq!(out["max_tokens"], 64);
    assert_eq!(
        out["messages"],
        json!([{"role":"user","content":[{"type":"text","text":"Name one ocean in one word."}]}])
    );
    assert!(out.get("max_completion_tokens").is_none(), "{out}");
}

/// A5g: a plain Gemini request carries no provider parameters, so Chat and Anthropic accept it;
/// a Gemini-only member is still refused there.
#[test]
fn plain_gemini_request_reaches_chat_and_anthropic() {
    let input = json!({"systemInstruction":{"parts":[{"text":"Be brief."}]},"contents":[{"role":"user","parts":[{"text":"Name one planet in one word."}]}],"generationConfig":{"maxOutputTokens":64,"stopSequences":["ZZZ"],"temperature":1}});
    let chat = from_gemini(&input, To::Chat).unwrap();
    assert_eq!(chat["messages"][0]["role"], "system");
    assert_eq!(chat["max_tokens"], 64);
    assert_eq!(chat["stop"], json!(["ZZZ"]));
    assert_eq!(chat["temperature"], 1);
    let anthropic = from_gemini(&input, To::Anthropic).unwrap();
    assert_eq!(anthropic["max_tokens"], 64);
    assert_eq!(anthropic["stop_sequences"], json!(["ZZZ"]));
    assert_eq!(anthropic["temperature"], 1);
    let mut native = input.clone();
    native["generationConfig"]["presencePenalty"] = json!(0.5);
    assert!(matches!(from_gemini(&native, To::Chat), Err(codec::Error::Unsupported)));
    assert!(matches!(
        from_gemini(&native, To::Anthropic),
        Err(codec::Error::Unsupported)
    ));
}

/// The Chat `reasoning` object's effort becomes the same `thinkingLevel` as `reasoning_effort`.
#[test]
fn chat_reasoning_effort_object_reaches_gemini_thinking_level() {
    use super::regressions::chat_to_gemini;
    let out =
        chat_to_gemini(br#"{"model":"m","messages":[{"role":"user","content":"hi"}],"reasoning":{"effort":"low"}}"#);
    assert_eq!(
        out,
        json!({"contents":[{"role":"user","parts":[{"text":"hi"}]}],"generationConfig":{"thinkingConfig":{"thinkingLevel":"low"}}})
    );
}

/// `thinkingBudget` maps through canonical fields only: a positive budget enables thinking with
/// it, 0 disables it and -1 (dynamic) enables it without one. Gemini gets the budget back.
#[test]
fn gemini_thinking_budget_reaches_anthropic_through_canonical_fields() {
    let input = |budget: i64| json!({"contents":[{"role":"user","parts":[{"text":"hi"}]}],"generationConfig":{"maxOutputTokens":4096,"thinkingConfig":{"thinkingBudget":budget}}});
    for (budget, thinking) in [
        (2048, json!({"type":"enabled","budget_tokens":2048})),
        (0, json!({"type":"disabled"})),
        (-1, json!({"type":"adaptive"})),
    ] {
        assert_eq!(
            from_gemini(&input(budget), To::Anthropic).unwrap()["thinking"],
            thinking
        );
        let chat = from_gemini(&input(budget), To::Chat).unwrap();
        assert!(chat.get("reasoning").is_none(), "{chat}");
        assert_eq!(request(&input(budget), Operation::Generate).0, input(budget));
    }
}

/// `functionCallingConfig` modes map to `tool_choice`; ANY with names maps to Chat's
/// `allowed_tools`, and to Anthropic only with one name. Gemini gets the config back.
#[test]
fn gemini_function_calling_config_reaches_chat_and_anthropic() {
    let input = |config: Value| json!({"contents":[{"role":"user","parts":[{"text":"hi"}]}],"tools":[{"functionDeclarations":[{"name":"f","parameters":{"type":"object"}},{"name":"g","parameters":{"type":"object"}}]}],"toolConfig":config});
    let calling = |mode: &str| json!({"functionCallingConfig":{"mode":mode}});
    let allowed = |names: Value| json!({"functionCallingConfig":{"mode":"ANY","allowedFunctionNames":names}});
    let function = |name: &str| json!({"type":"function","function":{"name":name}});
    for (config, chat, anthropic) in [
        (calling("AUTO"), json!("auto"), json!({"type":"auto"})),
        (calling("ANY"), json!("required"), json!({"type":"any"})),
        (calling("NONE"), json!("none"), json!({"type":"none"})),
        (
            allowed(json!(["f"])),
            json!({"type":"allowed_tools","allowed_tools":{"mode":"required","tools":[function("f")]}}),
            json!({"type":"tool","name":"f"}),
        ),
    ] {
        assert_eq!(
            from_gemini(&input(config.clone()), To::Chat).unwrap()["tool_choice"],
            chat
        );
        assert_eq!(
            from_gemini(&input(config.clone()), To::Anthropic).unwrap()["tool_choice"],
            anthropic
        );
        assert_eq!(request(&input(config.clone()), Operation::Generate).0, input(config));
    }
    let two = input(allowed(json!(["f", "g"])));
    assert_eq!(
        from_gemini(&two, To::Chat).unwrap()["tool_choice"]["allowed_tools"]["tools"],
        json!([function("f"), function("g")])
    );
    assert!(matches!(
        from_gemini(&two, To::Anthropic),
        Err(codec::Error::Unsupported)
    ));
    assert_eq!(request(&two, Operation::Generate).0, two);
    // Members without a canonical field stay native and keep foreign fences closed.
    for config in [
        calling("VALIDATED"),
        json!({"functionCallingConfig":{"mode":"AUTO","allowedFunctionNames":["f"]}}),
        json!({"functionCallingConfig":{"mode":"AUTO"},"retrievalConfig":{"languageCode":"en"}}),
    ] {
        assert!(matches!(
            from_gemini(&input(config.clone()), To::Chat),
            Err(codec::Error::Unsupported)
        ));
        assert_eq!(
            request(&input(config.clone()), Operation::Generate).0,
            input(config.clone())
        );
        assert_eq!(gemini_raw(&input(config)).matches("\"toolConfig\"").count(), 1);
    }
}

/// Encodes a Gemini request for Gemini as raw text, so a repeated member shows.
fn gemini_raw(input: &Value) -> String {
    let mut mapped = Mapped::new();
    let mut ingress = gemini::request::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        "gemini-test",
        1024 * 1024,
    )
    .unwrap();
    ingress
        .advance(&mut mapped.writer, &serde_json::to_vec(input).unwrap())
        .unwrap();
    let ready = ingress
        .finish(&mut mapped.writer, &gemini::request::GeminiCapability)
        .unwrap();
    let output = gemini::request::Egress {
        destination: "gemini",
        upstream_stream: true,
    }
    .encode(&ready, &mut mapped.writer)
    .unwrap();
    String::from_utf8(bytes(&output)).unwrap()
}
