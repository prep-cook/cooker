//! The official Chat request and response contract at the OpenAI client boundary.
use super::{
    ChatCapability, Egress, Ingress, Mapped, Operation, RequestEgress, RequestIngress, bytes, codec, identity,
};
use ck_translate::{
    anthropic::request::AnthropicCapability,
    gemini::request::GeminiCapability,
    responses::request::{Egress as ResponsesEgress, ResponsesCapability},
};
use codec::{BodyMode, Capability, ResponseIngress, event::Direction};
use serde_json::{Value, json};

/// Runs Chat request ingress against `destination` and returns the validated request's
/// upload for that destination.
fn fence(input: &Value, destination: &impl Capability) -> codec::Result<Value> {
    fence_raw(&input.to_string(), destination)
}

fn fence_raw(input: &str, destination: &impl Capability) -> codec::Result<Value> {
    let mut mapped = Mapped::new();
    let mut ingress = Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        1024 * 1024,
    )
    .unwrap();
    ingress.advance(&mut mapped.writer, input.as_bytes())?;
    let ready = ingress.finish(&mut mapped.writer, destination)?;
    let lease = match destination.destination() {
        "responses" => ResponsesEgress {
            destination: "responses",
            upstream_stream: false,
        }
        .encode(&ready, &mut mapped.writer)?,
        name => Egress {
            destination: name,
            upstream_stream: false,
        }
        .encode(&ready, &mut mapped.writer)?,
    };
    Ok(serde_json::from_slice(&bytes(&lease)).unwrap())
}

fn with(member: &str, value: Value) -> Value {
    let mut input = json!({"model":"m","messages":[{"role":"user","content":"hi"}]});
    input[member] = value;
    input
}

#[test]
fn empty_messages_fail_the_client_fence() {
    let input = json!({"model":"m","messages":[]});
    assert!(matches!(fence(&input, &ChatCapability), Err(codec::Error::Schema)));
}

#[test]
fn out_of_range_options_fail_the_client_fence() {
    let stops = |n: usize| json!(vec!["s"; n]);
    for (member, value) in [
        ("temperature", json!(2.5)),
        ("temperature", json!(-0.1)),
        ("top_p", json!(1.01)),
        ("top_p", json!(-1)),
        ("presence_penalty", json!(2.5)),
        ("frequency_penalty", json!(-3)),
        ("logit_bias", json!({"50256": 101})),
        ("logit_bias", json!({"50256": -100.5})),
        ("stop", stops(5)),
        ("modalities", json!(["text", "image"])),
    ] {
        let input = with(member, value);
        assert!(
            matches!(fence(&input, &ChatCapability), Err(codec::Error::Schema)),
            "{input}"
        );
    }
    let mut input = with("logprobs", json!(true));
    input["top_logprobs"] = json!(21);
    assert!(matches!(fence(&input, &ChatCapability), Err(codec::Error::Schema)));
}

/// Bounds compare the number as spelled, exactly, whatever its length or exponent.
#[test]
fn bounds_compare_the_exact_decimal_value() {
    let long_zero = format!("0.{}", "0".repeat(4096));
    for (member, literal, accepted) in [
        ("temperature", "2.0000000000000000000001", false),
        ("temperature", "-1e-400", false),
        ("temperature", "1e400", false),
        ("temperature", "200e-2", true),
        ("temperature", "0.2E+1", true),
        ("temperature", "-0.0e5", true),
        ("temperature", long_zero.as_str(), true),
        ("top_p", "0.00000000000000000000001e23", true),
        ("top_p", "1.00000000000000000000000000", true),
        ("top_p", "0.99999999999999999999999999", true),
        ("presence_penalty", "-2.00000000000000000001", false),
        ("presence_penalty", "-1.5", true),
        ("frequency_penalty", "1.5", true),
    ] {
        let input = format!(r#"{{"model":"m","messages":[{{"role":"user","content":"hi"}}],"{member}":{literal}}}"#);
        let result = fence_raw(&input, &ChatCapability);
        if accepted {
            assert!(result.is_ok(), "{member}: {literal}");
        } else {
            assert!(matches!(result, Err(codec::Error::Schema)), "{member}: {literal}");
        }
    }
    let bias =
        r#"{"model":"m","messages":[{"role":"user","content":"hi"}],"logit_bias":{"1":-100.00000000000000000001}}"#;
    assert!(matches!(fence_raw(bias, &ChatCapability), Err(codec::Error::Schema)));
}

#[test]
fn documented_limits_pass_unchanged() {
    let mut input = json!({"model":"m","messages":[{"role":"user","content":"hi"}],"temperature":2,"top_p":0,"presence_penalty":-2,"frequency_penalty":2,"logit_bias":{"1":-100,"2":100},"stop":["a","b","c","d"],"modalities":["text"],"logprobs":true,"top_logprobs":20});
    let output = fence(&input, &ChatCapability).unwrap();
    for member in [
        "temperature",
        "top_p",
        "presence_penalty",
        "frequency_penalty",
        "logit_bias",
        "stop",
        "modalities",
        "top_logprobs",
    ] {
        assert_eq!(output[member], input[member], "{member}: {output}");
    }
    input["stop"] = json!("one");
    assert_eq!(fence(&input, &ChatCapability).unwrap()["stop"], "one");
}

/// Official function arguments are a string the model generated, which may not be JSON.
#[test]
fn invalid_json_arguments_replay_unchanged_where_the_destination_takes_a_string() {
    let messages = json!([
        {"role":"user","content":"hi"},
        {"role":"assistant","content":null,"tool_calls":[
            {"id":"bad","type":"function","function":{"name":"f","arguments":"{\"a\":1,"}},
            {"id":"dup","type":"function","function":{"name":"f","arguments":"{\"a\":1,\"a\":2}"}},
            {"id":"ok","type":"function","function":{"name":"f","arguments":"{\"a\": [1, 2]}"}}
        ]},
        {"role":"tool","tool_call_id":"bad","content":"r"},
        {"role":"tool","tool_call_id":"dup","content":"r"},
        {"role":"tool","tool_call_id":"ok","content":"r"}
    ]);
    let input = with("messages", messages);
    let output = fence(&input, &ChatCapability).unwrap();
    let calls = &output["messages"][1]["tool_calls"];
    assert_eq!(calls[0]["function"]["arguments"], "{\"a\":1,", "{output}");
    assert_eq!(calls[1]["function"]["arguments"], "{\"a\":1,\"a\":2}", "{output}");
    assert_eq!(calls[2]["function"]["arguments"], "{\"a\":[1,2]}", "{output}");
    assert!(calls[0].get("arguments_format").is_none(), "{output}");

    let output = fence(&input, &ResponsesCapability).unwrap();
    let items = output["input"].as_array().unwrap();
    let call = items.iter().find(|item| item["call_id"] == "bad").unwrap();
    assert_eq!(call["arguments"], "{\"a\":1,", "{output}");

    let legacy = with(
        "messages",
        json!([{"role":"assistant","content":null,"function_call":{"name":"f","arguments":"{\"a\""}}]),
    );
    let output = fence(&legacy, &ChatCapability).unwrap();
    assert_eq!(
        output["messages"][0]["function_call"]["arguments"], "{\"a\"",
        "{output}"
    );

    // Destinations whose call arguments are a JSON object refuse rather than guess.
    let anthropic = AnthropicCapability {
        default_max_tokens: Some(16),
        budget: Default::default(),
    };
    assert!(matches!(fence(&input, &anthropic), Err(codec::Error::Unsupported)));
    assert!(matches!(
        fence(&input, &GeminiCapability),
        Err(codec::Error::Unsupported)
    ));
}

fn canonical_response(input: &str) -> (String, Vec<u8>) {
    let mut mapped = Mapped::new();
    let mut ingress = codec::chat::response::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Response),
        Operation::Generate,
        BodyMode::Streaming,
        1024 * 1024,
    )
    .unwrap();
    ingress.advance(&mut mapped.writer, input.as_bytes()).unwrap();
    ingress.finish(&mut mapped.writer).unwrap();
    let text = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
    let (client, _) = super::response(input.as_bytes(), BodyMode::Streaming, BodyMode::Streaming);
    (text, client)
}

/// An upstream `tool_calls` finish with malformed generated arguments stays deliverable.
#[test]
fn invalid_generated_arguments_finish_and_reach_the_client() {
    for fragments in [
        vec!["{\"a\":1,", "}"],
        vec!["{\"a\"", ":1}}"],
        vec!["not json"],
        vec!["{\"a\":"],
    ] {
        let mut input = String::from(
            "data: {\"id\":\"r\",\"model\":\"m\",\"created\":1,\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"tool_calls\":[{\"index\":0,\"id\":\"c\",\"type\":\"function\",\"function\":{\"name\":\"f\",\"arguments\":\"\"}}]},\"finish_reason\":null}]}\n\n",
        );
        for fragment in &fragments {
            let chunk = json!({"id":"r","model":"m","created":1,"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":fragment}}]},"finish_reason":null}]});
            input.push_str(&format!("data: {chunk}\n\n"));
        }
        input.push_str("data: {\"id\":\"r\",\"model\":\"m\",\"created\":1,\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n");
        let (canonical, client) = canonical_response(&input);
        assert!(!canonical.contains("event: x_ck.error\n"), "{canonical}");
        assert_eq!(canonical.matches("event: x_ck.done\n").count(), 1, "{canonical}");
        let end = canonical
            .lines()
            .skip_while(|line| *line != "event: x_ck.tool.end")
            .find_map(|line| line.strip_prefix("data: "))
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .unwrap();
        assert_eq!(end["status"], "completed", "{end}");
        assert_eq!(end["valid_json"], false, "{end}");
        let arguments: String = std::str::from_utf8(&client)
            .unwrap()
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter_map(|chunk| {
                chunk["choices"][0]["delta"]["tool_calls"][0]["function"]["arguments"]
                    .as_str()
                    .map(str::to_owned)
            })
            .collect();
        assert_eq!(arguments, fragments.concat());
    }
}

/// A buffered upstream completion with malformed arguments is delivered as well.
#[test]
fn invalid_buffered_arguments_reach_the_client() {
    let body = json!({"id":"r","object":"chat.completion","model":"m","created":1,"choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"c","type":"function","function":{"name":"f","arguments":"{\"a\":"}}]},"finish_reason":"tool_calls"}]});
    let (client, _) = super::response(body.to_string().as_bytes(), BodyMode::Buffered, BodyMode::Buffered);
    let client: Value = serde_json::from_slice(&client).unwrap();
    assert_eq!(
        client["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"], "{\"a\":",
        "{client}"
    );
}

/// The OpenAI bounds belong to the Chat client boundary: a Chat destination keeps what
/// another protocol's client validly sent, here five Anthropic stop sequences.
#[test]
fn chat_destination_keeps_another_protocols_valid_options() {
    let mut mapped = Mapped::new();
    let mut ingress = ck_translate::anthropic::request::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        1024 * 1024,
    )
    .unwrap();
    let input = json!({"model":"m","max_tokens":8,"stop_sequences":["a","b","c","d","e"],"messages":[{"role":"user","content":"hi"}]});
    ingress
        .advance(&mut mapped.writer, input.to_string().as_bytes())
        .unwrap();
    let ready = ingress.finish(&mut mapped.writer, &ChatCapability).unwrap();
    let lease = Egress {
        destination: "chat",
        upstream_stream: false,
    }
    .encode(&ready, &mut mapped.writer)
    .unwrap();
    let output: Value = serde_json::from_slice(&bytes(&lease)).unwrap();
    assert_eq!(output["stop"], input["stop_sequences"], "{output}");
}
