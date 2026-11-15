//! Review findings on PR #108: a translated request keeps every constraint the client set, or
//! is refused as Unsupported. Inputs are the reviewer's.
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
#[allow(dead_code)]
#[path = "overreach/harness.rs"]
mod harness;
#[path = "../src/responses/mod.rs"]
pub mod responses;
#[allow(dead_code)]
#[path = "semantic/mod.rs"]
mod semantic;

use harness::*;
use serde_json::{Value, json};

/// Anthropic structured outputs refuse numeric, string and array bounds and an
/// `additionalProperties` other than false; such a Gemini schema is refused, never stripped.
#[test]
fn anthropic_refuses_schema_keywords_structured_outputs_lack() {
    let input = |schema: Value| gm_json(json!({"responseMimeType":"application/json","responseJsonSchema":schema}));
    let base =
        json!({"type":"object","properties":{"n":{"type":"integer"}},"required":["n"],"additionalProperties":false});
    let out = translate(P::Gemini, P::Anthropic, &input(base.clone())).unwrap();
    assert_eq!(out["output_config"]["format"]["schema"], base);
    for (path, value) in [
        ("/properties/n/minimum", json!(10)),
        ("/properties/n/maximum", json!(10)),
        ("/properties/n/multipleOf", json!(2)),
        ("/properties/s", json!({"type":"string","maxLength":3})),
        ("/properties/s", json!({"type":"string","format":"regex"})),
        ("/properties/a", json!({"type":"array","minItems":2})),
        (
            "/properties/a",
            json!({"type":"array","items":{"type":"integer","exclusiveMinimum":0}}),
        ),
        ("/properties/e", json!({"enum":[{"a":1}]})),
        ("/properties/r", json!({"$ref":"https://example.com/s.json"})),
        ("/additionalProperties", json!(true)),
    ] {
        let mut schema = base.clone();
        let (parent, key) = path.rsplit_once('/').unwrap();
        schema.pointer_mut(if parent.is_empty() { "" } else { parent }).unwrap()[key] = value;
        unsupported(translate(P::Gemini, P::Anthropic, &input(schema)));
    }
    // A Gemini `Schema` is checked the same way after conversion.
    let gemini = json!({"responseMimeType":"application/json","responseSchema":{"type":"OBJECT","properties":{"n":{"type":"INTEGER","minimum":10}}}});
    unsupported(translate(P::Gemini, P::Anthropic, &gm_json(gemini)));
    let gemini = json!({"responseMimeType":"application/json","responseSchema":{"type":"ARRAY","minItems":"1","items":{"type":"STRING","format":"date-time"}}});
    assert!(translate(P::Gemini, P::Anthropic, &gm_json(gemini)).is_ok());
}

/// A plain-text document in a tool result has no Responses file form; a base64 PDF is a data
/// URL file, and a title, which Responses cannot carry, is refused rather than dropped.
#[test]
fn responses_tool_result_documents_map_or_are_refused() {
    let input = |document: Value| {
        json!({"model":"m","max_tokens":100,"messages":[
            {"role":"assistant","content":[{"type":"tool_use","id":"c","name":"f","input":{}}]},
            {"role":"user","content":[{"type":"tool_result","tool_use_id":"c","content":[document]}]}]})
    };
    let text =
        json!({"type":"document","source":{"type":"text","media_type":"text/plain","data":"hello"},"title":"report"});
    unsupported(translate(P::Anthropic, P::Responses, &input(text)));
    let text = json!({"type":"document","source":{"type":"text","media_type":"text/plain","data":"hello"}});
    unsupported(translate(P::Anthropic, P::Responses, &input(text)));
    let pdf =
        json!({"type":"document","source":{"type":"base64","media_type":"application/pdf","data":"JVBERi0xLjQK"}});
    let out = translate(P::Anthropic, P::Responses, &input(pdf.clone())).unwrap();
    assert_eq!(
        items(&out, "function_call_output")[0]["output"],
        json!([{"type":"input_file","file_data":"data:application/pdf;base64,JVBERi0xLjQK"}])
    );
    let mut titled = pdf;
    titled["title"] = json!("report");
    unsupported(translate(P::Anthropic, P::Responses, &input(titled)));
}

fn gm_tools(function: Value) -> Value {
    json!({"contents":[{"role":"user","parts":[{"text":"hi"}]}],"tools":[{"functionDeclarations":[function]}]})
}

/// A Gemini declaration's result schema is the Responses tool's `output_schema`; Chat and
/// Anthropic tools have no such member, so they refuse it.
#[test]
fn gemini_result_schema_maps_to_responses_output_schema() {
    let out = translate(
        P::Gemini,
        P::Responses,
        &gm_tools(json!({"name":"f","parameters":{"type":"OBJECT"},"response":{"type":"STRING"}})),
    )
    .unwrap();
    assert_eq!(
        out["tools"],
        json!([{"type":"function","name":"f","parameters":{"type":"object"},"output_schema":{"type":"string"}}])
    );
    let raw = json!({"type":"object","properties":{"t":{"type":"number"}}});
    let out = translate(
        P::Gemini,
        P::Responses,
        &gm_tools(json!({"name":"f","responseJsonSchema":raw})),
    )
    .unwrap();
    assert_eq!(out["tools"][0]["output_schema"], raw);
    let both = json!({"name":"f","response":{"type":"STRING"},"responseJsonSchema":raw});
    unsupported(translate(P::Gemini, P::Responses, &gm_tools(both)));
    for function in [
        json!({"name":"f","response":{"type":"STRING"}}),
        json!({"name":"f","responseJsonSchema":raw}),
    ] {
        for to in [P::Chat, P::Anthropic] {
            unsupported(translate(P::Gemini, to, &gm_tools(function.clone())));
        }
    }
}

/// `propertyOrdering` orders the written `properties` at every depth; one that does not list
/// exactly the object's properties is refused.
#[test]
fn property_ordering_orders_properties_or_is_refused() {
    let schema = json!({"type":"OBJECT","properties":{"a":{"type":"STRING"},"b":{"type":"OBJECT","properties":{"x":{"type":"STRING"},"y":{"type":"STRING"}},"propertyOrdering":["y","x"]}},"propertyOrdering":["b","a"]});
    let config = json!({"responseMimeType":"application/json","responseSchema":schema});
    let written = |out: &Value, pointer: &str| out.pointer(pointer).unwrap().to_string();
    let want = r#"{"type":"object","properties":{"b":{"type":"object","properties":{"y":{"type":"string"},"x":{"type":"string"}}},"a":{"type":"string"}}}"#;
    let out = translate(P::Gemini, P::Chat, &gm_json(config.clone())).unwrap();
    assert_eq!(written(&out, "/response_format/json_schema/schema"), want);
    let out = translate(P::Gemini, P::Anthropic, &gm_json(config.clone())).unwrap();
    assert_eq!(written(&out, "/output_config/format/schema"), want);
    let out = translate(P::Gemini, P::Responses, &gm_json(config)).unwrap();
    assert_eq!(written(&out, "/text/format/schema"), want);
    for ordering in [json!(["a"]), json!(["a", "c"]), json!(["a", "a"]), json!("a")] {
        let schema = json!({"type":"OBJECT","properties":{"a":{"type":"STRING"},"b":{"type":"STRING"}},"propertyOrdering":ordering});
        let config = json!({"responseMimeType":"application/json","responseSchema":schema});
        for to in [P::Chat, P::Anthropic, P::Responses] {
            unsupported(translate(P::Gemini, to, &gm_json(config.clone())));
        }
    }
    // An ordering that is not a list is refused even beside no properties.
    let schema = json!({"type":"OBJECT","properties":{},"propertyOrdering":""});
    let config = json!({"responseMimeType":"application/json","responseSchema":schema});
    unsupported(translate(P::Gemini, P::Chat, &gm_json(config)));
}

/// Structured outputs refuse `allOf` with `$ref` even without recursion; `minItems` is judged on
/// its converted count, so a Gemini `"01"` is 1.
#[test]
fn anthropic_judges_all_of_refs_and_converted_counts() {
    let schema = json!({"type":"object","properties":{"n":{"allOf":[{"$ref":"#/$defs/N"}]}},"additionalProperties":false,"$defs":{"N":{"type":"integer"}}});
    let config = json!({"responseMimeType":"application/json","responseJsonSchema":schema});
    unsupported(translate(P::Gemini, P::Anthropic, &gm_json(config)));
    let schema = json!({"type":"object","properties":{"n":{"$ref":"#/$defs/N","allOf":[{"type":"integer"}]}},"additionalProperties":false,"$defs":{"N":{"type":"integer"}}});
    let config = json!({"responseMimeType":"application/json","responseJsonSchema":schema});
    unsupported(translate(P::Gemini, P::Anthropic, &gm_json(config)));
    let schema = json!({"type":"object","properties":{"n":{"$ref":"#/$defs/N"}},"additionalProperties":false,"$defs":{"N":{"type":"integer"}}});
    let config = json!({"responseMimeType":"application/json","responseJsonSchema":schema});
    assert!(translate(P::Gemini, P::Anthropic, &gm_json(config)).is_ok());
    for (count, ok) in [("01", true), ("1", true), ("02", false)] {
        let schema = json!({"type":"ARRAY","items":{"type":"STRING"},"minItems":count});
        let config = json!({"responseMimeType":"application/json","responseSchema":schema});
        let out = translate(P::Gemini, P::Anthropic, &gm_json(config));
        assert_eq!(out.is_ok(), ok, "{count}: {out:?}");
    }
}

/// A large `propertyOrdering` is matched through an index: thousands of properties translate
/// well inside a budget the quadratic matching it replaced overran (about 27 s in a debug build).
#[test]
fn property_ordering_scales_linearly() {
    let names: Vec<String> = (0..2048).map(|i| format!("p{i:04}")).collect();
    let properties: serde_json::Map<String, Value> =
        names.iter().map(|n| (n.clone(), json!({"type":"STRING"}))).collect();
    let ordering: Vec<&String> = names.iter().rev().collect();
    let schema = json!({"type":"OBJECT","properties":properties,"propertyOrdering":ordering});
    let input = gm_json(json!({"responseMimeType":"application/json","responseSchema":schema}));
    let raw = serde_json::to_vec(&input).unwrap();
    let started = std::time::Instant::now();
    let out = once(P::Gemini, P::Responses, &mut std::iter::once(raw.as_slice())).unwrap();
    assert!(
        started.elapsed() < std::time::Duration::from_secs(10),
        "{:?}",
        started.elapsed()
    );
    let written: Vec<&String> = out["text"]["format"]["schema"]["properties"]
        .as_object()
        .unwrap()
        .keys()
        .collect();
    assert_eq!(written, ordering);
}

/// A Responses client sends parallel calls as one item each; Gemini receives them as one model
/// turn and their outputs, in the order of their calls, as one user turn.
fn rs_parallel(calls: &[(&str, &str)], outputs: &[&str]) -> Value {
    let mut input = vec![json!({"type":"message","role":"user","content":"go"})];
    for (id, name) in calls {
        input.push(json!({"type":"function_call","call_id":id,"name":name,"arguments":"{}"}));
    }
    for id in outputs {
        input.push(json!({"type":"function_call_output","call_id":id,"output":format!("out {id}")}));
    }
    let tools: Vec<Value> = calls
        .iter()
        .map(|(_, name)| json!({"type":"function","name":name,"parameters":{"type":"object"}}))
        .collect();
    json!({"model":"gemini-x","input":input,"tools":tools})
}
fn gm_parallel(calls: &[(&str, &str)]) -> Value {
    let results: Vec<Value> = calls
        .iter()
        .map(|(id, name)| json!({"functionResponse":{"name":name,"id":id,"response":{"result":format!("out {id}")}}}))
        .collect();
    let calls: Vec<Value> = calls
        .iter()
        .map(|(id, name)| json!({"functionCall":{"name":name,"id":id,"args":{}}}))
        .collect();
    json!([{"role":"user","parts":[{"text":"go"}]},{"role":"model","parts":calls},{"role":"user","parts":results}])
}

#[test]
fn responses_parallel_calls_reach_gemini_as_one_turn() {
    for (calls, outputs) in [
        (&[("a", "f"), ("b", "g")][..], &["a", "b"][..]),
        (&[("a", "f"), ("b", "g")], &["b", "a"]),
        (&[("a", "f"), ("b", "g"), ("c", "h")], &["a", "b", "c"]),
        (&[("a", "f"), ("b", "g"), ("c", "h")], &["c", "a", "b"]),
    ] {
        let out = translate(P::Responses, P::Gemini, &rs_parallel(calls, outputs)).unwrap();
        assert_eq!(out["contents"], gm_parallel(calls), "{calls:?} {outputs:?}");
    }
}

/// Gemini before 3 pairs responses with calls by position, so outputs in completion order are
/// written in call order; with one function name the city's result stays with its call.
#[test]
fn responses_outputs_reach_gemini_in_call_order() {
    let input = json!({"model":"gemini-x","input":[
        {"type":"function_call","call_id":"a","name":"weather","arguments":"{\"city\":\"Paris\"}"},
        {"type":"function_call","call_id":"b","name":"weather","arguments":"{\"city\":\"London\"}"},
        {"type":"function_call_output","call_id":"b","output":"London: rain"},
        {"type":"function_call_output","call_id":"a","output":"Paris: sun"}]});
    let out = translate(P::Responses, P::Gemini, &input).unwrap();
    assert_eq!(
        out["contents"],
        json!([
            {"role":"model","parts":[
                {"functionCall":{"name":"weather","id":"a","args":{"city":"Paris"}}},
                {"functionCall":{"name":"weather","id":"b","args":{"city":"London"}}}]},
            {"role":"user","parts":[
                {"functionResponse":{"name":"weather","id":"a","response":{"result":"Paris: sun"}}},
                {"functionResponse":{"name":"weather","id":"b","response":{"result":"London: rain"}}}]}])
    );
}

/// An output whose id two calls of the turn share has no single call to pair with.
#[test]
fn responses_output_for_a_shared_call_id_stays_unsupported_at_gemini() {
    unsupported(translate(
        P::Responses,
        P::Gemini,
        &rs_parallel(&[("a", "f"), ("a", "g")], &["a"]),
    ));
}

/// An output answering a call of an earlier model turn has no Gemini form.
#[test]
fn responses_output_for_an_earlier_turn_stays_unsupported_at_gemini() {
    let mut input = rs_parallel(&[("a", "f"), ("b", "g")], &["a"]);
    let items = input["input"].as_array_mut().unwrap();
    items.push(json!({"type":"function_call","call_id":"c","name":"h","arguments":"{}"}));
    items.push(json!({"type":"function_call_output","call_id":"b","output":"late"}));
    unsupported(translate(P::Responses, P::Gemini, &input));
}

/// Only assistant messages with calls join a model turn: a Chat user message's calls and the
/// calls after an assistant message without any keep their own turn.
#[test]
fn only_assistant_calls_join_a_gemini_turn() {
    let call = |id: &str| json!([{"type":"function","id":id,"function":{"name":"f","arguments":"{}"}}]);
    for (head, next) in [
        (
            json!({"role":"assistant","content":null,"tool_calls":call("a")}),
            json!({"role":"user","content":"and","tool_calls":call("b")}),
        ),
        (
            json!({"role":"assistant","content":[],"tool_calls":[]}),
            json!({"role":"assistant","content":null,"tool_calls":call("b")}),
        ),
    ] {
        let chat = json!({"model":"gemini-x","messages":[{"role":"user","content":"go"}, head, next]});
        let out = translate(P::Chat, P::Gemini, &chat).unwrap();
        assert_eq!(out["contents"].as_array().unwrap().len(), 3, "{out}");
    }
}

/// Same-protocol output keeps the client's turns and items as sent.
#[test]
fn parallel_calls_keep_their_shape_within_one_protocol() {
    let rs = rs_parallel(&[("a", "f"), ("b", "g")], &["b", "a"]);
    let out = translate(P::Responses, P::Responses, &rs).unwrap();
    assert_eq!(out["input"], rs["input"]);
    let contents = json!([
        {"role":"user","parts":[{"text":"go"}]},
        {"role":"model","parts":[{"functionCall":{"name":"f","id":"a","args":{}}}]},
        {"role":"model","parts":[{"functionCall":{"name":"g","id":"b","args":{}}}]},
        {"role":"user","parts":[{"functionResponse":{"name":"f","id":"a","response":{"result":"1"}}}]}]);
    let out = translate(P::Gemini, P::Gemini, &json!({"contents":contents})).unwrap();
    assert_eq!(out["contents"], contents);
}
