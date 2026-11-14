//! Requests another protocol can express faithfully translate; the rest are refused as
//! Unsupported, never dropped (owner rulings; p4 unsupported audit). Inputs are the SDK-built
//! bodies of the offline SDK check.
// Test-only inclusion, as in the other codec targets.
#[path = "../src/anthropic/mod.rs"]
pub mod anthropic;
#[allow(dead_code)]
#[path = "../src/boundary.rs"]
pub mod boundary;
#[path = "../src/codec/mod.rs"]
pub mod codec;
#[path = "../src/gemini/boundary.rs"]
pub mod gemini;
#[path = "../src/responses/mod.rs"]
pub mod responses;
#[allow(dead_code)]
#[path = "semantic/mod.rs"]
mod semantic;

use codec::{
    Error, Operation, RequestEgress, RequestIngress,
    event::{Direction, Identity, LocalId},
};
use semantic::Mapped;
use serde_json::{Value, json};

#[derive(Clone, Copy, Debug)]
enum P {
    Anthropic,
    Chat,
    Responses,
    Gemini,
}

fn identity() -> Identity {
    Identity {
        request_id: LocalId::new("request").unwrap(),
        stream_id: LocalId::new("stream").unwrap(),
        attempt: 1,
        direction: Direction::Request,
    }
}
fn bytes(lease: &ck_stream::ScratchLease) -> Vec<u8> {
    let mut out = Vec::new();
    while out.len() < lease.len() {
        out.extend_from_slice(lease.chunk(out.len()).unwrap());
    }
    out
}

/// The upstream body, or the ingress or fence refusal, of `input` fed in `chunks`; egress must
/// encode whatever the fence passed.
fn drive(
    m: &mut Mapped,
    mut ingress: impl RequestIngress,
    to: P,
    chunks: &mut dyn Iterator<Item = &[u8]>,
) -> Result<Value, Error> {
    for chunk in chunks {
        ingress.advance(&mut m.writer, chunk)?;
    }
    let output = match to {
        P::Anthropic => {
            let capability = anthropic::request::AnthropicCapability {
                default_max_tokens: Some(4096),
            };
            let ready = ingress.finish(&mut m.writer, &capability)?;
            anthropic::request::Egress {
                destination: "anthropic",
                upstream_stream: false,
                default_max_tokens: Some(4096),
            }
            .encode(&ready, &mut m.writer)
            .expect("a request the fence passed encodes")
        }
        P::Chat => {
            let ready = ingress.finish(&mut m.writer, &codec::chat::request::ChatCapability)?;
            codec::chat::request::Egress {
                destination: "chat",
                upstream_stream: false,
            }
            .encode(&ready, &mut m.writer)
            .expect("a request the fence passed encodes")
        }
        P::Responses => {
            let ready = ingress.finish(&mut m.writer, &responses::request::ResponsesCapability)?;
            responses::request::Egress {
                destination: "responses",
                upstream_stream: false,
            }
            .encode(&ready, &mut m.writer)
            .expect("a request the fence passed encodes")
        }
        P::Gemini => {
            let ready = ingress.finish(&mut m.writer, &gemini::request::GeminiCapability)?;
            gemini::request::Egress {
                destination: "gemini",
                upstream_stream: false,
            }
            .encode(&ready, &mut m.writer)
            .expect("a request the fence passed encodes")
        }
    };
    Ok(serde_json::from_slice(&bytes(&output)).unwrap())
}
fn once(from: P, to: P, chunks: &mut dyn Iterator<Item = &[u8]>) -> Result<Value, Error> {
    let mut m = Mapped::new();
    let (w, s, id, op, limit) = (&mut m.writer, &m.store, identity(), Operation::Generate, 1 << 20);
    match from {
        P::Anthropic => {
            let ingress = anthropic::request::Ingress::new(w, s, id, op, limit).unwrap();
            drive(&mut m, ingress, to, chunks)
        }
        P::Chat => {
            let ingress = codec::chat::request::Ingress::new(w, s, id, op, limit).unwrap();
            drive(&mut m, ingress, to, chunks)
        }
        P::Responses => {
            let ingress = responses::request::Ingress::new(w, s, id, op, limit).unwrap();
            drive(&mut m, ingress, to, chunks)
        }
        P::Gemini => {
            let ingress = gemini::request::Ingress::new(w, s, id, op, "gemini-x", limit).unwrap();
            drive(&mut m, ingress, to, chunks)
        }
    }
}
fn same(a: &Result<Value, Error>, b: &Result<Value, Error>) -> bool {
    match (a, b) {
        (Ok(a), Ok(b)) => a == b,
        (Err(a), Err(b)) => format!("{a:?}") == format!("{b:?}"),
        _ => false,
    }
}
/// Translates `input` from `from` to `to`; a 1-byte feed and a 7-byte feed must agree with
/// the whole body.
fn translate(from: P, to: P, input: &Value) -> Result<Value, Error> {
    let raw = serde_json::to_vec(input).unwrap();
    let whole = once(from, to, &mut std::iter::once(raw.as_slice()));
    for width in [1, 7] {
        let fed = once(from, to, &mut raw.chunks(width));
        assert!(
            same(&whole, &fed),
            "{from:?}->{to:?} width {width}: {whole:?} vs {fed:?}"
        );
    }
    whole
}
fn unsupported(result: Result<Value, Error>) {
    assert!(matches!(result, Err(Error::Unsupported)), "{result:?}");
}
/// The input items of a Responses body other than its plain messages.
fn items(out: &Value, kind: &str) -> Vec<Value> {
    out["input"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|i| i["type"] == kind)
        .cloned()
        .collect()
}

fn an_tool_result(content: Value) -> Value {
    json!({"max_tokens":100,"model":"claude-x","messages":[
        {"role":"user","content":"weather?"},
        {"role":"assistant","content":[{"type":"tool_use","id":"toolu_01B","name":"get_weather","input":{"city":"Oslo"}}]},
        {"role":"user","content":[content,{"type":"text","text":"try again"}]}],
        "tools":[{"name":"get_weather","description":"Weather","input_schema":{"type":"object","properties":{"city":{"type":"string"}}}}]})
}
fn gm_tool_result(response: Value) -> Value {
    json!({"contents":[
        {"role":"user","parts":[{"text":"weather?"}]},
        {"role":"model","parts":[{"functionCall":{"id":"fc_1","name":"get_weather","args":{"city":"Paris"}}}]},
        {"role":"user","parts":[{"functionResponse":{"id":"fc_1","name":"get_weather","response":response}}]}],
        "tools":[{"functionDeclarations":[{"name":"get_weather","parametersJsonSchema":{"type":"object"}}]}]})
}

/// Audit #4, #5: an Anthropic tool result is a `function_call_output`; only `is_error: true`
/// has no Responses form.
#[test]
fn anthropic_tool_result_reaches_responses_as_function_call_output() {
    let text = json!({"type":"tool_result","tool_use_id":"toolu_01B","content":"19C clear"});
    let out = translate(P::Anthropic, P::Responses, &an_tool_result(text)).unwrap();
    assert_eq!(
        items(&out, "function_call_output"),
        [json!({"type":"function_call_output","call_id":"toolu_01B","output":"19C clear"})]
    );
    assert_eq!(
        items(&out, "message").last().unwrap()["content"],
        json!([{"type":"input_text","text":"try again"}])
    );
    let blocks = json!([{"type":"text","text":"upstream"},{"type":"text","text":" down"}]);
    let part = json!({"type":"tool_result","tool_use_id":"toolu_01B","is_error":false,"content":blocks});
    let out = translate(P::Anthropic, P::Responses, &an_tool_result(part)).unwrap();
    assert_eq!(
        items(&out, "function_call_output")[0]["output"],
        json!([{"type":"input_text","text":"upstream"},{"type":"input_text","text":" down"}])
    );
    // Anthropic ingress refuses a null `is_error` for every destination.
    let part = json!({"type":"tool_result","tool_use_id":"toolu_01B","is_error":null,"content":blocks});
    assert!(matches!(
        translate(P::Anthropic, P::Responses, &an_tool_result(part)),
        Err(Error::Schema)
    ));
    let error = json!({"type":"tool_result","tool_use_id":"toolu_01B","is_error":true,"content":blocks});
    unsupported(translate(P::Anthropic, P::Responses, &an_tool_result(error)));
    // A long result arrives as streamed text media.
    let long = "x".repeat(5000);
    let part = json!({"type":"tool_result","tool_use_id":"toolu_01B","content":long});
    let out = translate(P::Anthropic, P::Responses, &an_tool_result(part)).unwrap();
    assert_eq!(items(&out, "function_call_output")[0]["output"], json!(long));
}

/// Audit #24: a Gemini function `result` is the output's JSON text; an `error` has no
/// Responses form.
#[test]
fn gemini_function_result_reaches_responses_as_json_text() {
    let out = translate(P::Gemini, P::Responses, &gm_tool_result(json!({"result":"19C clear"}))).unwrap();
    assert_eq!(
        items(&out, "function_call_output"),
        [json!({"type":"function_call_output","call_id":"fc_1","output":"19C clear"})]
    );
    // The turn holds only the result, so no empty message item follows it.
    assert_eq!(items(&out, "message").len(), 1, "{out}");
    // An output must answer an earlier call.
    let mut unlinked = gm_tool_result(json!({"result":1}));
    unlinked["contents"][2]["parts"][0]["functionResponse"]["id"] = json!("other");
    assert!(matches!(
        translate(P::Gemini, P::Responses, &unlinked),
        Err(Error::Schema)
    ));
    unsupported(translate(
        P::Gemini,
        P::Responses,
        &gm_tool_result(json!({"error":"down"})),
    ));
    unsupported(translate(
        P::Gemini,
        P::Responses,
        &gm_tool_result(json!({"output":"x"})),
    ));
}

/// Audit #25: `includeThoughts` alone asks for summaries; a token budget has no Responses form.
#[test]
fn gemini_include_thoughts_reaches_responses_as_summary() {
    let input = |thinking: Value| json!({"contents":[{"role":"user","parts":[{"text":"2+2?"}]}],"generationConfig":{"thinkingConfig":thinking}});
    let out = translate(P::Gemini, P::Responses, &input(json!({"include_thoughts":true}))).unwrap();
    assert_eq!(out["reasoning"], json!({"summary":"auto"}));
    let out = translate(
        P::Gemini,
        P::Responses,
        &input(json!({"includeThoughts":true,"thinkingLevel":"low"})),
    )
    .unwrap();
    assert_eq!(out["reasoning"], json!({"summary":"auto","effort":"low"}));
    for off in [
        json!({"includeThoughts":false}),
        json!({"includeThoughts":true,"thinkingBudget":0}),
    ] {
        let out = translate(P::Gemini, P::Responses, &input(off)).unwrap();
        assert!(out.get("reasoning").is_none(), "{out}");
    }
    unsupported(translate(
        P::Gemini,
        P::Responses,
        &input(json!({"include_thoughts":true,"thinking_budget":1024})),
    ));
}

/// Audit aside: Anthropic `top_k` was dropped on the way to Responses.
#[test]
fn anthropic_top_k_is_refused_by_responses() {
    let input = json!({"max_tokens":100,"messages":[{"role":"user","content":"hi"}],"model":"claude-x","top_k":5});
    unsupported(translate(P::Anthropic, P::Responses, &input));
}

/// Audit #12, #14: `developer` and Responses `instructions` are the system instruction; a
/// second system message still has no Gemini form.
#[test]
fn developer_and_instructions_reach_gemini_as_system_instruction() {
    let input = json!({"model":"gpt-x","messages":[{"role":"developer","content":[{"type":"text","text":"Dev rule."}]},{"role":"user","content":"hi"}]});
    let out = translate(P::Chat, P::Gemini, &input).unwrap();
    assert_eq!(out["systemInstruction"], json!({"parts":[{"text":"Dev rule."}]}));
    assert_eq!(out["contents"], json!([{"role":"user","parts":[{"text":"hi"}]}]));
    let input = json!({"model":"gpt-x","input":"hi","instructions":"You are terse.","max_output_tokens":100});
    let out = translate(P::Responses, P::Gemini, &input).unwrap();
    assert_eq!(out["systemInstruction"], json!({"parts":[{"text":"You are terse."}]}));
    assert_eq!(out["contents"].as_array().unwrap().len(), 1, "{out}");
    let input = json!({"model":"gpt-x","messages":[{"role":"system","content":"a"},{"role":"developer","content":"b"},{"role":"user","content":"hi"}]});
    unsupported(translate(P::Chat, P::Gemini, &input));
}

/// Audit #13, #17: parallel calls are Gemini's default and nothing is stored, so `true` and
/// `store: false` carry no field; `false` and `store: true` have no Gemini form.
#[test]
fn gemini_accepts_default_parallel_calls_and_no_storage() {
    let input = |extra: Value| {
        let mut body = json!({"model":"gpt-x","messages":[{"role":"user","content":"hi"}]});
        body.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
        body
    };
    for extra in [
        json!({"parallel_tool_calls":true}),
        json!({"parallel_tool_calls":null}),
        json!({"store":false}),
        json!({"store":null}),
    ] {
        let out = translate(P::Chat, P::Gemini, &input(extra.clone())).unwrap();
        assert_eq!(
            out,
            json!({"contents":[{"role":"user","parts":[{"text":"hi"}]}]}),
            "{extra}"
        );
    }
    unsupported(translate(
        P::Chat,
        P::Gemini,
        &input(json!({"parallel_tool_calls":false})),
    ));
    unsupported(translate(P::Chat, P::Gemini, &input(json!({"store":true}))));
    let input = json!({"model":"gpt-x","input":"2+2?","store":false});
    assert!(translate(P::Responses, P::Gemini, &input).is_ok());
}

/// Audit #10: Anthropic `top_k` is Gemini's `topK`.
#[test]
fn anthropic_top_k_reaches_gemini() {
    let input = json!({"max_tokens":100,"messages":[{"role":"user","content":"hi"}],"model":"claude-x","top_k":5});
    let out = translate(P::Anthropic, P::Gemini, &input).unwrap();
    assert_eq!(out["generationConfig"], json!({"maxOutputTokens":100,"topK":5}));
}

/// Audit #8: Claude thinking history is stripped as at a Chat upstream, not sent as thoughts.
#[test]
fn claude_thinking_history_is_stripped_for_gemini() {
    let input = json!({"max_tokens":2048,"model":"claude-x","messages":[
        {"role":"user","content":"weather?"},
        {"role":"assistant","content":[
            {"type":"thinking","thinking":"Need tool.","signature":"EqoBCkgIBxABGAIiQKfake=="},
            {"type":"redacted_thinking","data":"opaque"},
            {"type":"tool_use","id":"toolu_01C","name":"get_weather","input":{"city":"Rome"}}]},
        {"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_01C","content":"25C"}]}],
        "tools":[{"name":"get_weather","description":"Weather","input_schema":{"type":"object"}}]});
    let out = translate(P::Anthropic, P::Gemini, &input).unwrap();
    assert_eq!(
        out["contents"][1],
        json!({"role":"model","parts":[{"functionCall":{"name":"get_weather","id":"toolu_01C","args":{"city":"Rome"}}}]})
    );
    let text = out.to_string();
    assert!(
        !text.contains("EqoB") && !text.contains("Need tool") && !text.contains("opaque"),
        "{text}"
    );
}

/// Audit #15, #18: Messages stores nothing, so `store: false` carries no field.
#[test]
fn anthropic_accepts_store_false() {
    let input = |store: Value| json!({"model":"gpt-x","input":"2+2?","store":store,"max_output_tokens":10});
    for store in [json!(false), Value::Null] {
        let out = translate(P::Responses, P::Anthropic, &input(store)).unwrap();
        assert!(out.get("store").is_none(), "{out}");
    }
    unsupported(translate(P::Responses, P::Anthropic, &input(json!(true))));
}

fn gm_json(config: Value) -> Value {
    json!({"contents":[{"role":"user","parts":[{"text":"json"}]}],"generationConfig":config})
}

/// Audit #29: a Gemini response schema is the Anthropic JSON-schema output format; JSON mode
/// without a schema has no Anthropic form.
#[test]
fn gemini_response_schema_reaches_anthropic() {
    let schema = json!({"properties":{"a":{"type":"INTEGER"}},"type":"OBJECT"});
    let config = json!({"responseMimeType":"application/json","responseSchema":schema});
    let out = translate(P::Gemini, P::Anthropic, &gm_json(config)).unwrap();
    assert_eq!(
        out["output_config"],
        json!({"format":{"type":"json_schema","schema":{"properties":{"a":{"type":"integer"}},"type":"object"}}})
    );
    let raw = json!({"type":"object","properties":{"a":{"type":"integer"}}});
    let config = json!({"response_mime_type":"application/json","response_json_schema":raw});
    let out = translate(P::Gemini, P::Anthropic, &gm_json(config)).unwrap();
    assert_eq!(
        out["output_config"],
        json!({"format":{"type":"json_schema","schema":raw}})
    );
    unsupported(translate(
        P::Gemini,
        P::Anthropic,
        &gm_json(json!({"responseMimeType":"application/json"})),
    ));
    let both = json!({"responseMimeType":"application/json","responseSchema":schema,"responseJsonSchema":raw});
    unsupported(translate(P::Gemini, P::Anthropic, &gm_json(both)));
    // A member sent in both spellings keeps its proto name in the native config.
    let twice = json!({"responseMimeType":"application/json","responseSchema":schema,"response_schema":schema});
    unsupported(translate(P::Gemini, P::Anthropic, &gm_json(twice)));
    unsupported(translate(
        P::Gemini,
        P::Anthropic,
        &gm_json(json!({"responseMimeType":"text/x.enum","responseSchema":schema})),
    ));
}

/// Audit #22: a Gemini `result` is the tool result's JSON text, an `error` the same with
/// `is_error`.
#[test]
fn gemini_function_response_reaches_anthropic_as_text() {
    let out = translate(
        P::Gemini,
        P::Anthropic,
        &gm_tool_result(json!({"result":{"temp":19,"sky":"clear"}})),
    )
    .unwrap();
    assert_eq!(
        out["messages"][2]["content"],
        json!([{"type":"tool_result","tool_use_id":"fc_1","content":"{\"temp\":19,\"sky\":\"clear\"}"}])
    );
    let out = translate(P::Gemini, P::Anthropic, &gm_tool_result(json!({"error":"down"}))).unwrap();
    assert_eq!(
        out["messages"][2]["content"],
        json!([{"type":"tool_result","tool_use_id":"fc_1","content":"down","is_error":true}])
    );
    unsupported(translate(
        P::Gemini,
        P::Anthropic,
        &gm_tool_result(json!({"result":1,"error":2})),
    ));
    let mut more = gm_tool_result(json!({"result":1}));
    more["contents"][2]["parts"][0]["functionResponse"]["willContinue"] = json!(true);
    unsupported(translate(P::Gemini, P::Anthropic, &more));
    let long = "y".repeat(5000);
    let out = translate(P::Gemini, P::Anthropic, &gm_tool_result(json!({"result":long}))).unwrap();
    assert_eq!(out["messages"][2]["content"][0]["content"], json!(long));
}

/// Audit #23: a Gemini `result` is the tool message's JSON text; Chat has no error flag.
#[test]
fn gemini_function_result_reaches_chat_as_text() {
    let out = translate(P::Gemini, P::Chat, &gm_tool_result(json!({"result":"19C clear"}))).unwrap();
    assert_eq!(
        out["messages"][2],
        json!({"role":"tool","tool_call_id":"fc_1","content":"19C clear"})
    );
    unsupported(translate(P::Gemini, P::Chat, &gm_tool_result(json!({"error":"down"}))));
}

/// Audit #30: Gemini JSON mode is `json_schema` with a schema, else `json_object`.
#[test]
fn gemini_json_mode_reaches_chat() {
    let schema = json!({"properties":{"a":{"type":"INTEGER"}},"type":"OBJECT"});
    let config = json!({"responseMimeType":"application/json","responseSchema":schema});
    let out = translate(P::Gemini, P::Chat, &gm_json(config)).unwrap();
    assert_eq!(
        out["response_format"],
        json!({"type":"json_schema","json_schema":{"name":"response","schema":{"properties":{"a":{"type":"integer"}},"type":"object"}}})
    );
    let out = translate(
        P::Gemini,
        P::Chat,
        &gm_json(json!({"responseMimeType":"application/json"})),
    )
    .unwrap();
    assert_eq!(out["response_format"], json!({"type":"json_object"}));
    unsupported(translate(
        P::Gemini,
        P::Chat,
        &gm_json(json!({"responseMimeType":"text/plain","responseSchema":schema})),
    ));
}

/// Every two-way split of a request that exercises several new mappings gives one body.
#[test]
fn every_split_gives_the_same_body() {
    let cases = [
        (P::Gemini, P::Anthropic, gm_tool_result(json!({"error":{"code":5}}))),
        (
            P::Gemini,
            P::Chat,
            gm_json(json!({"responseMimeType":"application/json","responseSchema":{"type":"STRING"}})),
        ),
        (
            P::Anthropic,
            P::Responses,
            an_tool_result(json!({"type":"tool_result","tool_use_id":"toolu_01B","content":"ok"})),
        ),
    ];
    for (from, to, input) in cases {
        let raw = serde_json::to_vec(&input).unwrap();
        let whole = once(from, to, &mut std::iter::once(raw.as_slice()));
        assert!(whole.is_ok(), "{whole:?}");
        for split in 1..raw.len() {
            let (a, b) = raw.split_at(split);
            let fed = once(from, to, &mut [a, b].into_iter());
            assert!(same(&whole, &fed), "split {split}");
        }
    }
}

/// A string `result` is the tool text as is; any other value is its compact JSON text.
#[test]
fn gemini_string_result_is_raw_text_and_other_values_are_json() {
    let out = translate(P::Gemini, P::Responses, &gm_tool_result(json!({"result":25}))).unwrap();
    assert_eq!(items(&out, "function_call_output")[0]["output"], "25");
    let out = translate(P::Gemini, P::Chat, &gm_tool_result(json!({"result":[1,true,null]}))).unwrap();
    assert_eq!(out["messages"][2]["content"], "[1,true,null]");
    let out = translate(
        P::Gemini,
        P::Anthropic,
        &gm_tool_result(json!({"result":"19C \"clear\""})),
    )
    .unwrap();
    assert_eq!(out["messages"][2]["content"][0]["content"], "19C \"clear\"");
}

/// A Gemini model turn with only calls has no text: Chat gets `""`, as for Claude history,
/// never an empty content array. A Chat client's own `[]` is unchanged.
#[test]
fn gemini_call_only_turn_reaches_chat_with_empty_text() {
    let out = translate(P::Gemini, P::Chat, &gm_tool_result(json!({"result":"ok"}))).unwrap();
    assert_eq!(out["messages"][1]["content"], "", "{out}");
    let input = json!({"model":"gpt-x","messages":[{"role":"user","content":"hi"},
        {"role":"assistant","content":[],"tool_calls":[{"id":"c1","type":"function","function":{"name":"f","arguments":"{}"}}]},
        {"role":"tool","tool_call_id":"c1","content":"x"}]});
    assert_eq!(
        translate(P::Chat, P::Chat, &input).unwrap()["messages"][1]["content"],
        json!([])
    );
}

/// Gemini JSON mode is a Responses `text.format`: a named `json_schema` with the converted
/// schema, else `json_object`; no Gemini member is copied.
#[test]
fn gemini_json_mode_reaches_responses() {
    let schema = json!({"properties":{"a":{"type":"INTEGER"}},"type":"OBJECT"});
    let config = json!({"responseMimeType":"application/json","responseSchema":schema});
    let out = translate(P::Gemini, P::Responses, &gm_json(config)).unwrap();
    assert_eq!(
        out["text"],
        json!({"format":{"type":"json_schema","name":"response","schema":{"properties":{"a":{"type":"integer"}},"type":"object"}}})
    );
    let out = translate(
        P::Gemini,
        P::Responses,
        &gm_json(json!({"responseMimeType":"application/json"})),
    )
    .unwrap();
    assert_eq!(out["text"], json!({"format":{"type":"json_object"}}));
    let enumeration = json!({"responseMimeType":"text/x.enum","responseSchema":{"type":"STRING","enum":["a"]}});
    unsupported(translate(P::Gemini, P::Responses, &gm_json(enumeration)));
}

/// Gemini function declarations are Responses function tools with a JSON Schema `parameters`.
#[test]
fn gemini_tools_reach_responses_as_function_tools() {
    let tools = |function: Value| json!({"contents":[{"role":"user","parts":[{"text":"hi"}]}],"tools":[{"functionDeclarations":[function]}]});
    let want = json!([{"type":"function","name":"f","description":"d","parameters":{"type":"object","properties":{"a":{"type":"integer"}}}}]);
    for function in [
        json!({"name":"f","description":"d","parametersJsonSchema":{"type":"object","properties":{"a":{"type":"integer"}}}}),
        json!({"name":"f","description":"d","parameters":{"type":"OBJECT","properties":{"a":{"type":"INTEGER"}}},"behavior":"BLOCKING","response":{"type":"STRING"}}),
    ] {
        let out = translate(P::Gemini, P::Responses, &tools(function)).unwrap();
        assert_eq!(out["tools"], want);
    }
    unsupported(translate(
        P::Gemini,
        P::Responses,
        &tools(json!({"name":"f","behavior":"NON_BLOCKING"})),
    ));
}

/// A string `error` is the error text as is, like a string `result`; other values are JSON.
#[test]
fn gemini_string_error_is_raw_text() {
    let out = translate(P::Gemini, P::Anthropic, &gm_tool_result(json!({"error":{"code":5}}))).unwrap();
    assert_eq!(out["messages"][2]["content"][0]["content"], "{\"code\":5}");
}

/// A Gemini native tool group has no Responses, Chat or Anthropic form: the fence refuses it as
/// unsupported, not as malformed and not at egress.
#[test]
fn gemini_native_tool_groups_are_unsupported_elsewhere() {
    for group in [json!({"googleSearch":{}}), json!({"codeExecution":{}})] {
        let input = json!({"contents":[{"role":"user","parts":[{"text":"hi"}]}],"tools":[group]});
        for to in [P::Responses, P::Chat, P::Anthropic] {
            unsupported(translate(P::Gemini, to, &input));
        }
        assert!(translate(P::Gemini, P::Gemini, &input).is_ok());
    }
}
