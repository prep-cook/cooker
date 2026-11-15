//! Request bodies built by the official SDKs (anthropic, openai, google-genai), from the
//! 2026-11-15 offline SDK check: each must translate as the official API would read it.
use super::*;
use crate::anthropic::request::AnthropicCapability;

#[derive(Clone, Copy)]
enum P {
    Anthropic,
    Chat,
    Responses,
    Gemini,
}

/// Translates `input` from one protocol to another; an ingress or fence refusal is the error.
fn translate(from: P, to: P, input: &Value) -> codec::Result<Value> {
    let mut m = Mapped::new();
    let (id, op, limit) = (identity(Direction::Request), Operation::Generate, 1024 * 1024);
    match from {
        P::Anthropic => {
            let ingress = anthropic::request::Ingress::new(&mut m.writer, &m.store, id, op, limit).unwrap();
            run(&mut m, ingress, input, to)
        }
        P::Chat => {
            let ingress = codec::chat::request::Ingress::new(&mut m.writer, &m.store, id, op, limit).unwrap();
            run(&mut m, ingress, input, to)
        }
        P::Responses => {
            let ingress = responses::request::Ingress::new(&mut m.writer, &m.store, id, op, limit).unwrap();
            run(&mut m, ingress, input, to)
        }
        P::Gemini => {
            let ingress = gemini::request::Ingress::new(&mut m.writer, &m.store, id, op, "gemini-x", limit).unwrap();
            run(&mut m, ingress, input, to)
        }
    }
}

fn run(m: &mut Mapped, mut ingress: impl RequestIngress, input: &Value, to: P) -> codec::Result<Value> {
    for chunk in serde_json::to_vec(input).unwrap().chunks(7) {
        ingress.advance(&mut m.writer, chunk)?;
    }
    let output = match to {
        P::Anthropic => {
            let capability = AnthropicCapability {
                default_max_tokens: Some(4096),
                ..Default::default()
            };
            let ready = ingress.finish(&mut m.writer, &capability)?;
            anthropic::request::Egress {
                destination: "anthropic",
                upstream_stream: false,
                default_max_tokens: capability.default_max_tokens,
            }
            .encode(&ready, &mut m.writer)
        }
        P::Chat => {
            let ready = ingress.finish(&mut m.writer, &codec::chat::request::ChatCapability)?;
            codec::chat::request::Egress {
                destination: "chat",
                upstream_stream: false,
            }
            .encode(&ready, &mut m.writer)
        }
        P::Responses => {
            let ready = ingress.finish(&mut m.writer, &responses::request::ResponsesCapability)?;
            responses::request::Egress {
                destination: "responses",
                upstream_stream: false,
            }
            .encode(&ready, &mut m.writer)
        }
        P::Gemini => {
            let ready = ingress.finish(&mut m.writer, &gemini::request::GeminiCapability)?;
            gemini::request::Egress {
                destination: "gemini",
                upstream_stream: false,
            }
            .encode(&ready, &mut m.writer)
        }
    }?;
    Ok(serde_json::from_slice(&bytes(&output)).unwrap())
}

const PNG_URL_SAFE: &str =
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGNg-M_A8B8ABQUCAaXz0fMAAAAASUVORK5CYII=";
const PNG: &str = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGNg+M/A8B8ABQUCAaXz0fMAAAAASUVORK5CYII=";

fn gm_tools(parameters: &str) -> Value {
    json!({"contents":[{"parts":[{"text":"weather?"}],"role":"user"}],
        "tools":[{"functionDeclarations":[{"description":"Weather","name":"get_weather",
            (parameters):{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}}]}],
        "toolConfig":{"functionCallingConfig":{"mode":"AUTO"}},"generationConfig":{}})
}

/// google-genai sends `inlineData.data` in the URL-safe alphabet beside `mime_type`. Every
/// destination gets the decoded bytes in the standard alphabet and the media type.
#[test]
fn url_safe_inline_data_reaches_every_destination() {
    let input = json!({"contents":[{"parts":[{"inlineData":{"data":PNG_URL_SAFE,"mime_type":"image/png"}},{"text":"describe"}],"role":"user"}]});
    let gemini = translate(P::Gemini, P::Gemini, &input).unwrap();
    assert_eq!(
        gemini["contents"][0]["parts"][0],
        json!({"inlineData":{"data":PNG,"mimeType":"image/png"}})
    );
    let chat = translate(P::Gemini, P::Chat, &input).unwrap();
    assert_eq!(
        chat["messages"][0]["content"][0]["image_url"]["url"],
        format!("data:image/png;base64,{PNG}")
    );
    let anthropic = translate(P::Gemini, P::Anthropic, &input).unwrap();
    assert_eq!(anthropic["messages"][0]["content"][0]["source"]["data"], PNG);
    assert!(translate(P::Gemini, P::Responses, &input).is_ok());
    // A payload long enough to stream is classified by its proto-named carrier as well.
    let long = "_-8A".repeat(2048);
    let input = json!({"contents":[{"parts":[{"inline_data":{"mime_type":"image/png","data":long}}],"role":"user"}]});
    let chat = translate(P::Gemini, P::Chat, &input).unwrap();
    assert_eq!(
        chat["messages"][0]["content"][0]["image_url"]["url"],
        format!("data:image/png;base64,{}", "/+8A".repeat(2048))
    );
}

/// google-genai sends `generationConfig: {}` beside tools. It carries nothing, so Chat and
/// Anthropic take the request; Gemini gets the empty config back.
#[test]
fn empty_generation_config_does_not_block_foreign_destinations() {
    let input = gm_tools("parametersJsonSchema");
    let chat = translate(P::Gemini, P::Chat, &input).unwrap();
    assert_eq!(chat["tool_choice"], "auto");
    assert!(translate(P::Gemini, P::Anthropic, &input).is_ok());
    assert_eq!(translate(P::Gemini, P::Gemini, &input).unwrap(), input);
    // A function response reaches Chat and Anthropic as its result's JSON text.
    let mut result = input.clone();
    result["contents"] = json!([{"parts":[{"text":"weather?"}],"role":"user"},
        {"parts":[{"functionCall":{"id":"fc_1","args":{"city":"Paris"},"name":"get_weather"}}],"role":"model"},
        {"parts":[{"functionResponse":{"id":"fc_1","name":"get_weather","response":{"result":"19C clear"}}}],"role":"user"}]);
    for to in [P::Chat, P::Anthropic] {
        assert!(translate(P::Gemini, to, &result).is_ok());
    }
    assert_eq!(translate(P::Gemini, P::Gemini, &result).unwrap(), result);
    let mut native = input.clone();
    native["generationConfig"]["presencePenalty"] = json!(0.5);
    assert!(matches!(
        translate(P::Gemini, P::Chat, &native),
        Err(codec::Error::Unsupported)
    ));
}

/// Gemini's REST API takes a field's proto name (`thinking_budget`) like its JSON name
/// (`thinkingBudget`), and google-genai sends both forms.
#[test]
fn snake_case_gemini_members_map_like_camel_case() {
    let thinking = json!({"contents":[{"parts":[{"text":"2+2?"}],"role":"user"}],"generationConfig":{"thinkingConfig":{"include_thoughts":true,"thinking_budget":1024}}});
    let anthropic = translate(P::Gemini, P::Anthropic, &thinking).unwrap();
    assert_eq!(anthropic["thinking"], json!({"type":"enabled","budget_tokens":1024}));
    let gemini = translate(P::Gemini, P::Gemini, &thinking).unwrap();
    assert_eq!(
        gemini["generationConfig"],
        json!({"thinkingConfig":{"thinkingBudget":1024,"includeThoughts":true}})
    );
    let schema = json!({"type":"object","properties":{"city":{"type":"string"}},"required":["city"]});
    let tools = gm_tools("parameters_json_schema");
    let chat = translate(P::Gemini, P::Chat, &tools).unwrap();
    assert_eq!(chat["tools"][0]["function"]["parameters"], schema);
    let anthropic = translate(P::Gemini, P::Anthropic, &tools).unwrap();
    assert_eq!(anthropic["tools"][0]["input_schema"], schema);
}

/// Gemini's enum mode has no Chat or Anthropic form: the fence refuses it as unsupported, not
/// as malformed.
#[test]
fn gemini_enum_response_schema_is_unsupported_at_chat_and_anthropic() {
    let input = json!({"contents":[{"parts":[{"text":"json"}],"role":"user"}],"generationConfig":{"responseMimeType":"text/x.enum","responseSchema":{"enum":["a"],"type":"STRING"}}});
    for to in [P::Chat, P::Anthropic] {
        assert!(matches!(
            translate(P::Gemini, to, &input),
            Err(codec::Error::Unsupported)
        ));
    }
    assert_eq!(translate(P::Gemini, P::Gemini, &input).unwrap(), input);
}

/// Responses `reasoning.summary` and encrypted reasoning history have no Chat form.
#[test]
fn responses_reasoning_summary_is_unsupported_at_chat() {
    let reasoning = json!({"model":"gpt-x","input":"2+2?","reasoning":{"effort":"low","summary":"auto"},"store":false});
    let history = json!({"model":"gpt-x","input":[{"role":"user","content":"2+2?"},
        {"type":"reasoning","id":"rs_1","summary":[{"type":"summary_text","text":"Add."}],"encrypted_content":"EqoBCkgIBxAB"},
        {"type":"message","role":"assistant","content":[{"type":"output_text","text":"4"}]},
        {"role":"user","content":"and 3+3?"}],"store":false});
    for input in [reasoning, history] {
        let result = translate(P::Responses, P::Chat, &input);
        assert!(matches!(result, Err(codec::Error::Unsupported)), "{result:?}");
    }
}

/// Responses `reasoning` takes `effort` and `summary` only: another protocol's budget, switch
/// and native config stay out. Thinking turned on without an effort has no Responses form and
/// is refused, never dropped; an effort or a disabled setting passes. A Responses source keeps
/// its own object.
#[test]
fn foreign_thinking_writes_only_responses_reasoning_members() {
    let anthropic = |thinking: Value| json!({"model":"claude-x","max_tokens":2048,"thinking":thinking,"messages":[{"role":"user","content":"2+2?"}]});
    let budget = anthropic(json!({"type":"enabled","budget_tokens":1024}));
    assert!(matches!(
        translate(P::Anthropic, P::Responses, &budget),
        Err(codec::Error::Unsupported)
    ));
    let disabled = translate(P::Anthropic, P::Responses, &anthropic(json!({"type":"disabled"}))).unwrap();
    assert!(disabled.get("reasoning").is_none(), "{disabled}");
    let gemini = json!({"contents":[{"parts":[{"text":"2+2?"}],"role":"user"}],"generationConfig":{"thinkingConfig":{"thinkingBudget":1024}}});
    assert!(matches!(
        translate(P::Gemini, P::Responses, &gemini),
        Err(codec::Error::Unsupported)
    ));
    let gemini = json!({"contents":[{"parts":[{"text":"2+2?"}],"role":"user"}],"generationConfig":{"thinkingConfig":{"thinkingLevel":"low"}}});
    let out = translate(P::Gemini, P::Responses, &gemini).unwrap();
    assert_eq!(out["reasoning"], json!({"effort":"low"}));
    let chat = json!({"model":"gpt-x","messages":[{"role":"user","content":"2+2?"}],"reasoning_effort":"low"});
    let out = translate(P::Chat, P::Responses, &chat).unwrap();
    assert_eq!(out["reasoning"], json!({"effort":"low"}));
    let chat =
        |reasoning: Value| json!({"model":"gpt-x","messages":[{"role":"user","content":"2+2?"}],"reasoning":reasoning});
    let out = translate(P::Chat, P::Responses, &chat(json!({"effort":"low"}))).unwrap();
    assert_eq!(out["reasoning"], json!({"effort":"low"}));
    let mut both = chat(json!({"effort":"high"}));
    both["reasoning_effort"] = json!("low");
    let out = translate(P::Chat, P::Responses, &both).unwrap();
    assert_eq!(out["reasoning"], json!({"effort":"low"}));
    let out = translate(P::Chat, P::Responses, &chat(json!({"effort":"low","summary":"auto"}))).unwrap();
    assert_eq!(out["reasoning"], json!({"effort":"low","summary":"auto"}));
    let out = translate(P::Chat, P::Responses, &chat(json!({"summary":"auto"}))).unwrap();
    assert_eq!(out["reasoning"], json!({"summary":"auto"}));
    for reasoning in [json!({"max_tokens":1024}), json!({"effort":"low","max_tokens":1024})] {
        assert!(matches!(
            translate(P::Chat, P::Responses, &chat(reasoning)),
            Err(codec::Error::Unsupported)
        ));
    }
    let responses = json!({"model":"gpt-x","input":"2+2?","reasoning":{"effort":"low","summary":"auto"}});
    let out = translate(P::Responses, P::Responses, &responses).unwrap();
    assert_eq!(out["reasoning"], json!({"effort":"low","summary":"auto"}));
}

/// A Chat `reasoning` object reaches Gemini only through its `effort`; any other member is refused
/// rather than dropped.
#[test]
fn chat_reasoning_object_reaches_gemini_only_as_effort() {
    let chat =
        |reasoning: Value| json!({"model":"gpt-x","messages":[{"role":"user","content":"2+2?"}],"reasoning":reasoning});
    let out = translate(P::Chat, P::Gemini, &chat(json!({"effort":"low"}))).unwrap();
    assert_eq!(
        out["generationConfig"]["thinkingConfig"],
        json!({"thinkingLevel":"low"})
    );
    for reasoning in [
        json!({"effort":"low","summary":"auto"}),
        json!({"effort":"low","max_tokens":1024}),
        json!({"max_tokens":1024}),
        json!({"summary":"auto"}),
    ] {
        assert!(
            matches!(
                translate(P::Chat, P::Gemini, &chat(reasoning.clone())),
                Err(codec::Error::Unsupported)
            ),
            "{reasoning}"
        );
    }
}

/// The openai SDK's accumulated assistant message carries `tool_calls: null` and
/// `function_call: null`; null means absent.
#[test]
fn null_tool_calls_in_chat_history_are_absent() {
    let input = json!({"model":"gpt-x","messages":[{"role":"user","content":"hi"},
        {"content":"Hello","role":"assistant","function_call":null,"tool_calls":null,"refusal":null,"annotations":null,"audio":null,"parsed":null},
        {"role":"user","content":"again"}]});
    let chat = translate(P::Chat, P::Chat, &input).unwrap();
    assert_eq!(chat["messages"][1]["content"], "Hello");
    assert!(chat["messages"][1].get("tool_calls").is_none(), "{chat}");
    let anthropic = translate(P::Chat, P::Anthropic, &input).unwrap();
    assert_eq!(anthropic["messages"][1]["role"], "assistant");
}

/// Anthropic refuses thinking with an empty signature in history, so the Anthropic encoder drops
/// such blocks; signed blocks stay, beside text and beside tool calls.
#[test]
fn empty_signature_thinking_is_dropped_for_anthropic() {
    let input = json!({"model":"claude-x","max_tokens":1024,"messages":[
        {"role":"user","content":"weather?"},
        {"role":"assistant","content":[{"type":"thinking","thinking":"Look it up.","signature":""},
            {"type":"tool_use","id":"toolu_1","name":"get_weather","input":{"city":"Paris"}}]},
        {"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"19C"}]},
        {"role":"assistant","content":[{"type":"thinking","thinking":"Unsigned.","signature":""},
            {"type":"thinking","thinking":"Signed.","signature":"c2ln"},{"type":"text","text":"Mild."}]},
        {"role":"user","content":"thanks"}],
        "tools":[{"name":"get_weather","input_schema":{"type":"object"}}]});
    let out = translate(P::Anthropic, P::Anthropic, &input).unwrap();
    assert_eq!(
        out["messages"][1]["content"],
        json!([{"type":"tool_use","id":"toolu_1","name":"get_weather","input":{"city":"Paris"}}])
    );
    assert_eq!(
        out["messages"][3]["content"],
        json!([{"type":"thinking","thinking":"Signed.","signature":"c2ln"},{"type":"text","text":"Mild."}])
    );
}
