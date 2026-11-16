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
#[path = "overreach/harness.rs"]
mod harness;
#[path = "../src/responses/mod.rs"]
pub mod responses;
#[allow(dead_code)]
#[path = "semantic/mod.rs"]
mod semantic;

use codec::Error;
use harness::*;
use serde_json::{Value, json};

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
    let out = translate(P::Gemini, P::Responses, &input(json!({"includeThoughts":false}))).unwrap();
    assert!(out.get("reasoning").is_none(), "{out}");
    for off in [
        json!({"thinkingBudget":0}),
        json!({"includeThoughts":true,"thinkingBudget":0}),
    ] {
        let out = translate(P::Gemini, P::Responses, &input(off)).unwrap();
        assert_eq!(out["reasoning"], json!({"effort":"none"}), "{out}");
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
        (
            P::Chat,
            P::Chat,
            json!({"model":"gpt-x","messages":[{"role":"user","content":"hi"}],"logprobs":true,"top_logprobs":3}),
        ),
        (
            P::Chat,
            P::Responses,
            json!({"model":"gpt-x","messages":[{"role":"user","content":"hi"}],
                "tools":[{"type":"function","function":{"name":"f","parameters":{"type":"object"}}}],
                "tool_choice":{"type":"allowed_tools","allowed_tools":{"mode":"auto","tools":[{"type":"function","function":{"name":"f"}}]}}}),
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
        json!({"name":"f","description":"d","parameters":{"type":"OBJECT","properties":{"a":{"type":"INTEGER"}}},"behavior":"BLOCKING"}),
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

/// Responses `top_logprobs` is the canonical root member Chat uses, so each destination judges
/// it: Gemini and Anthropic have no form for it, Chat takes it only beside `logprobs: true`,
/// which a Responses request has no member for, and Responses takes the integer as it is.
#[test]
fn responses_top_logprobs_is_judged_by_each_destination() {
    let input = json!({"model":"gpt-x","input":"hi","top_logprobs":3});
    for to in [P::Gemini, P::Anthropic, P::Chat] {
        unsupported(translate(P::Responses, to, &input));
    }
    assert_eq!(
        translate(P::Responses, P::Responses, &input).unwrap(),
        json!({"input":[{"type":"message","role":"user","content":"hi"}],"stream":false,"model":"gpt-x","top_logprobs":3})
    );
}

/// A Chat client's nested allowed-tools choice reaches Responses flat; a member Responses has
/// no place for is refused at the fence, not dropped or copied.
#[test]
fn chat_allowed_tools_reach_responses_flat() {
    let request = |allowed: Value| {
        json!({"model":"gpt-x","messages":[{"role":"user","content":"hi"}],
            "tools":[{"type":"function","function":{"name":"f","parameters":{"type":"object"}}},
                {"type":"custom","custom":{"name":"c"}}],
            "tool_choice":{"type":"allowed_tools","allowed_tools":allowed}})
    };
    let listed = json!([{"type":"function","function":{"name":"f"}},{"type":"custom","custom":{"name":"c"}}]);
    let out = translate(
        P::Chat,
        P::Responses,
        &request(json!({"mode":"required","tools":listed})),
    )
    .unwrap();
    assert_eq!(
        out["tool_choice"],
        json!({"type":"allowed_tools","mode":"required","tools":[{"type":"function","name":"f"},{"type":"custom","name":"c"}]})
    );
    for allowed in [
        json!({"mode":"auto","tools":[{"type":"function","function":{"name":"f","strict":true}}]}),
        json!({"mode":"auto","tools":[{"type":"function","function":{"name":"f"},"extra":1}]}),
        json!({"mode":"auto","tools":[{"type":"function","function":{"name":"f"}}],"extra":1}),
    ] {
        unsupported(translate(P::Chat, P::Responses, &request(allowed)));
    }
    let mut beside = request(json!({"mode":"auto","tools":[{"type":"function","function":{"name":"f"}}]}));
    beside["tool_choice"]["function"] = json!({"name":"f"});
    unsupported(translate(P::Chat, P::Responses, &beside));
}

/// Chat takes `top_logprobs` only beside `logprobs: true`; a Chat client's own pair passes
/// unchanged.
#[test]
fn chat_top_logprobs_needs_logprobs() {
    let input = json!({"model":"gpt-x","messages":[{"role":"user","content":"hi"}],"logprobs":true,"top_logprobs":2});
    let out = translate(P::Chat, P::Chat, &input).unwrap();
    assert_eq!((&out["logprobs"], &out["top_logprobs"]), (&json!(true), &json!(2)));
    for logprobs in [json!({"logprobs":false}), json!({"logprobs":null}), json!({})] {
        let mut input = input.clone();
        input.as_object_mut().unwrap().remove("logprobs");
        input
            .as_object_mut()
            .unwrap()
            .extend(logprobs.as_object().unwrap().clone());
        unsupported(translate(P::Chat, P::Chat, &input));
    }
    let null = json!({"model":"gpt-x","messages":[{"role":"user","content":"hi"}],"top_logprobs":null});
    assert!(translate(P::Chat, P::Chat, &null).is_ok());
}

/// Chat `logprobs` has no Responses member: `true` needs a response translation that does not
/// exist, so it is refused; `false` and null ask for the default and are not written. Responses
/// takes `top_logprobs` alone, so it reaches the upstream without `logprobs`.
#[test]
fn chat_logprobs_are_judged_by_responses() {
    let request = |extra: Value| {
        let mut input = json!({"model":"gpt-x","messages":[{"role":"user","content":"hi"}]});
        input
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        input
    };
    unsupported(translate(
        P::Chat,
        P::Responses,
        &request(json!({"logprobs":true,"top_logprobs":2})),
    ));
    for logprobs in [json!(false), Value::Null] {
        let out = translate(P::Chat, P::Responses, &request(json!({"logprobs":logprobs}))).unwrap();
        assert_eq!(out.get("logprobs"), None, "{out}");
    }
    let out = translate(P::Chat, P::Responses, &request(json!({"top_logprobs":2}))).unwrap();
    assert_eq!(out["top_logprobs"], 2);
    assert_eq!(out.get("logprobs"), None, "{out}");
}

fn has_key(value: &Value, key: &str) -> bool {
    match value {
        Value::Object(map) => map.iter().any(|(k, v)| k == key || has_key(v, key)),
        Value::Array(items) => items.iter().any(|v| has_key(v, key)),
        _ => false,
    }
}

/// (source, destination, input, member, restored location and value).
type IsolationRow<'a> = (P, P, &'a Value, &'a str, Option<(&'a str, Value)>);

/// Owner ruling Q3 (2026-11-16): a provider's own members live in its isolated map. Only that
/// provider's request egress restores them; every other destination accepts the request and
/// never sees them. Rows: (source, destination, input, member, restored location and value, or
/// `None` when the member must appear nowhere in the upstream body).
#[test]
fn provider_members_reach_only_their_provider() {
    let breakpoint = json!({"type":"ephemeral","ttl":"1h"});
    let an = json!({"model":"claude-x","max_tokens":100,
        "system":[{"type":"text","text":"s","cache_control":breakpoint}],
        "messages":[{"role":"user","content":[{"type":"text","text":"hi","cache_control":breakpoint}]}]});
    let ch = json!({"model":"m","messages":[{"role":"user","content":[{"type":"text","text":"hi","cache_control":breakpoint}]}]});
    let ch_vendor =
        json!({"model":"m","messages":[{"role":"user","content":[{"type":"text","text":"hi","vendor_hint":1}]}]});
    let gm_vendor = json!({"contents":[{"role":"user","parts":[{"text":"hi","partMetadata":{"k":"v"}}]}]});
    let rs_vendor = json!({"model":"m","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi","vendor_hint":1}]}]});
    let ch_root = json!({"model":"m","messages":[{"role":"user","content":"hi"}],"cache_control":breakpoint});
    let xai_chat = json!({"model":"grok-4.3","max_tokens":32,"messages":[{"role":"user","content":"hi"}],
        "search_parameters":{"mode":"on","sources":[{"type":"x"}]},"compaction_trigger":{"threshold":123}});
    let xai_responses = json!({"model":"grok-4.3","max_output_tokens":32,"input":"hi",
        "search_parameters":{"mode":"on","sources":[{"type":"x"}]},"compaction_trigger":{"threshold":123}});
    let part = "/messages/0/content/0/cache_control";
    let rows: [IsolationRow; 19] = [
        (
            P::Anthropic,
            P::Anthropic,
            &an,
            "cache_control",
            Some((part, breakpoint.clone())),
        ),
        (
            P::Anthropic,
            P::Anthropic,
            &an,
            "cache_control",
            Some(("/system/0/cache_control", breakpoint.clone())),
        ),
        (P::Anthropic, P::Chat, &an, "cache_control", None),
        (P::Anthropic, P::Responses, &an, "cache_control", None),
        (P::Anthropic, P::Gemini, &an, "cache_control", None),
        (
            P::Chat,
            P::Anthropic,
            &ch,
            "cache_control",
            Some((part, breakpoint.clone())),
        ),
        (P::Chat, P::Chat, &ch, "cache_control", None),
        (P::Chat, P::Responses, &ch, "cache_control", None),
        (P::Chat, P::Gemini, &ch, "cache_control", None),
        (
            P::Chat,
            P::Anthropic,
            &ch_root,
            "cache_control",
            Some(("/cache_control", breakpoint.clone())),
        ),
        (P::Chat, P::Chat, &ch_root, "cache_control", None),
        (
            P::Chat,
            P::Chat,
            &ch_vendor,
            "vendor_hint",
            Some(("/messages/0/content/0/vendor_hint", json!(1))),
        ),
        (P::Chat, P::Anthropic, &ch_vendor, "vendor_hint", None),
        (
            P::Gemini,
            P::Gemini,
            &gm_vendor,
            "partMetadata",
            Some(("/contents/0/parts/0/partMetadata", json!({"k":"v"}))),
        ),
        (P::Gemini, P::Anthropic, &gm_vendor, "partMetadata", None),
        (P::Gemini, P::Chat, &gm_vendor, "partMetadata", None),
        (
            P::Responses,
            P::Responses,
            &rs_vendor,
            "vendor_hint",
            Some(("/input/0/content/0/vendor_hint", json!(1))),
        ),
        (P::Responses, P::Anthropic, &rs_vendor, "vendor_hint", None),
        (P::Responses, P::Gemini, &rs_vendor, "vendor_hint", None),
    ];
    let mut failures = Vec::new();
    let xai_rows = [(P::Chat, &xai_chat), (P::Responses, &xai_responses)]
        .into_iter()
        .flat_map(|(from, input)| {
            [P::Chat, P::Responses, P::Anthropic, P::Gemini]
                .into_iter()
                .flat_map(move |to| {
                    ["search_parameters", "compaction_trigger"].map(|member| (from, to, input, member, None))
                })
        });
    for (from, to, input, member, expected) in rows.into_iter().chain(xai_rows) {
        let row = format!("{from:?}->{to:?} {member}");
        match (translate(from, to, input), expected) {
            (Ok(out), Some((pointer, value))) if out.pointer(pointer) != Some(&value) => {
                failures.push(format!("{row}: {pointer} missing in {out}"));
            }
            (Ok(out), None) if has_key(&out, member) => failures.push(format!("{row}: leaked into {out}")),
            (Ok(_), _) => {}
            (Err(error), _) => failures.push(format!("{row}: refused with {error:?}")),
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}
