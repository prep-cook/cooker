//! The Responses echo of a request's tools, tool choice and parallel flag, for every tool and
//! choice form the relay fence admits: each comes out in its official Responses shape, and a
//! form without one is refused rather than dropped.

mod support;

use ck_exec::openai_compat_exec::RelayCapability;
use ck_stream::{Read, Reader, ScratchLease, Terminal, Writer};
use ck_translate::codec::event::Direction;
use ck_translate::codec::{
    self, BodyMode, Capability, Operation, RequestEgress, RequestIngress, RequestView, ResponseEgress, ResponseIngress,
};
use ck_translate::responses::request::ResponsesCapability;
use ck_translate::{anthropic, codec::chat, gemini, responses};
use serde_json::{Value, json};
use support::{Streams, fence, identity};

const LIMIT: u64 = 1 << 20;
const SSE: &[u8] = b"data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"up-model\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"hi\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";

fn lease_bytes(lease: &ScratchLease) -> Vec<u8> {
    let mut out = Vec::with_capacity(lease.len());
    while out.len() < lease.len() {
        out.extend_from_slice(lease.chunk(out.len()).unwrap());
    }
    out
}

/// What a client egress writes for the whole response stream.
async fn deliver<E: ResponseEgress>(mut egress: E, stream: &mut Writer, mut reader: Reader) -> String {
    let mut out = Vec::new();
    while let Some(read) = reader.next().await {
        match read {
            Read::Record(lease) => egress
                .advance(stream, lease, |bytes| {
                    out.extend_from_slice(&lease_bytes(&bytes));
                    Ok(())
                })
                .unwrap(),
            Read::Terminal(_) => egress.finish().unwrap(),
        }
    }
    String::from_utf8(out).unwrap()
}

/// A destination that admits every request, to reach echo with forms no relay fence admits.
struct Open;

impl Capability for Open {
    fn destination(&self) -> &'static str {
        "open"
    }
    fn validate(&self, _request: RequestView<'_>) -> codec::Result<()> {
        Ok(())
    }
}

/// Fences `body` for `destination`, answers it with one Chat stream and returns the Responses
/// object echo and egress write for it.
async fn echo_with<I: RequestIngress>(
    streams: &Streams,
    ingress: &mut I,
    request: &mut Writer,
    body: &[u8],
    destination: &impl Capability,
) -> Result<Value, codec::Error> {
    let validated = fence(ingress, request, body, 64, destination)?;
    let mut response = streams.response(request);
    let id = response.id();
    let mut stream = response.allocator();
    let mut upstream = chat::response::Ingress::new(
        &mut response,
        &streams.store,
        identity(Direction::Response),
        Operation::Generate,
        BodyMode::Streaming,
        LIMIT,
    )
    .unwrap();
    upstream.advance(&mut response, SSE).unwrap();
    upstream.finish(&mut response).unwrap();
    response.finish(Terminal::Done);
    let mut egress =
        responses::egress::Egress::new(&mut stream, Operation::Generate, BodyMode::Buffered, |_, _| {}).unwrap();
    egress.echo(&mut stream, validated.view())?;
    Ok(serde_json::from_str(&deliver(egress, &mut stream, streams.reader(id)).await).unwrap())
}

/// Echo for a `protocol` client request fenced for `destination`.
async fn echo_for(protocol: &str, body: &[u8], destination: &impl Capability) -> Result<Value, codec::Error> {
    let streams = Streams::new();
    let mut request = streams.request();
    let (store, id) = (&streams.store, identity(Direction::Request));
    match protocol {
        "chat" => {
            echo_with(
                &streams,
                &mut chat::request::Ingress::new(&mut request, store, id, Operation::Generate, LIMIT).unwrap(),
                &mut request,
                body,
                destination,
            )
            .await
        }
        "responses" => {
            echo_with(
                &streams,
                &mut responses::request::Ingress::new(&mut request, store, id, Operation::Generate, LIMIT).unwrap(),
                &mut request,
                body,
                destination,
            )
            .await
        }
        "anthropic" => {
            echo_with(
                &streams,
                &mut anthropic::request::Ingress::new(&mut request, store, id, Operation::Generate, LIMIT).unwrap(),
                &mut request,
                body,
                destination,
            )
            .await
        }
        "gemini" => {
            echo_with(
                &streams,
                &mut gemini::request::Ingress::new(&mut request, store, id, Operation::Generate, "m", LIMIT).unwrap(),
                &mut request,
                body,
                destination,
            )
            .await
        }
        other => panic!("{other}"),
    }
}

/// Echo for a `protocol` client request the relay fence admitted.
async fn echoed(protocol: &str, body: &str) -> Value {
    echo_for(protocol, body.as_bytes(), &RelayCapability)
        .await
        .expect("echo projects every form the relay admits")
}

const FUNCTION: &str = r#"{"type":"function","function":{"name":"f","parameters":{"type":"object","properties":{}}}}"#;

fn chat(tools: &str, choice: &str) -> String {
    format!(r#"{{"model":"m","messages":[{{"role":"user","content":"hi"}}],"tools":[{tools}],"tool_choice":{choice}}}"#)
}

/// A Chat custom tool and a custom choice come out flat, with a grammar's syntax and definition
/// beside its type; a text format and a bare declaration keep their members.
#[tokio::test]
async fn custom_tools_and_choices_echo_flat() {
    let grammar = r#"{"type":"custom","custom":{"name":"c","description":"d","format":{"type":"grammar","grammar":{"syntax":"lark","definition":"x"}}}}"#;
    let text = r#"{"type":"custom","custom":{"name":"t","format":{"type":"text"}}}"#;
    let bare = r#"{"type":"custom","custom":{"name":"b"}}"#;
    let tools = format!("{grammar},{FUNCTION},{text},{bare}");
    let out = echoed("chat", &chat(&tools, r#"{"type":"custom","custom":{"name":"c"}}"#)).await;
    assert_eq!(
        out["tools"],
        json!([
            {"type":"custom","name":"c","description":"d","format":{"type":"grammar","syntax":"lark","definition":"x"}},
            {"type":"function","name":"f","parameters":{"type":"object","properties":{}}},
            {"type":"custom","name":"t","format":{"type":"text"}},
            {"type":"custom","name":"b"},
        ]),
        "{out}"
    );
    assert_eq!(out["tool_choice"], json!({"type":"custom","name":"c"}), "{out}");
}

/// A restricted choice, from Chat or from Gemini's `ANY` mode with allowed names, comes out with
/// `mode` and `tools` beside its type and each tool named flat.
#[tokio::test]
async fn allowed_tools_echo_in_the_responses_shape() {
    let custom = r#"{"type":"custom","custom":{"name":"c"}}"#;
    let choice = r#"{"type":"allowed_tools","allowed_tools":{"mode":"auto","tools":[{"type":"function","function":{"name":"f"}},{"type":"custom","custom":{"name":"c"}}]}}"#;
    let out = echoed("chat", &chat(&format!("{FUNCTION},{custom}"), choice)).await;
    assert_eq!(
        out["tool_choice"],
        json!({"type":"allowed_tools","mode":"auto","tools":[{"type":"function","name":"f"},{"type":"custom","name":"c"}]}),
        "{out}"
    );
    let gemini = r#"{"contents":[{"role":"user","parts":[{"text":"hi"}]}],"tools":[{"functionDeclarations":[{"name":"f","parameters":{"type":"OBJECT"}},{"name":"g"}]}],"toolConfig":{"functionCallingConfig":{"mode":"ANY","allowedFunctionNames":["f"]}}}"#;
    let out = echoed("gemini", gemini).await;
    assert_eq!(
        out["tool_choice"],
        json!({"type":"allowed_tools","mode":"required","tools":[{"type":"function","name":"f"}]}),
        "{out}"
    );
}

/// The remaining admitted choices: the three strings, a named function, and an Anthropic tool
/// choice with parallel use turned off.
#[tokio::test]
async fn plain_choices_echo_as_sent() {
    for choice in ["auto", "none", "required"] {
        let out = echoed("chat", &chat(FUNCTION, &format!("\"{choice}\""))).await;
        assert_eq!(out["tool_choice"], choice, "{out}");
    }
    let out = echoed(
        "chat",
        &chat(FUNCTION, r#"{"type":"function","function":{"name":"f"}}"#),
    )
    .await;
    assert_eq!(out["tool_choice"], json!({"type":"function","name":"f"}), "{out}");
    let anthropic = r#"{"model":"m","max_tokens":5,"messages":[{"role":"user","content":"hi"}],"tools":[{"name":"f","input_schema":{"type":"object"}}],"tool_choice":{"type":"tool","name":"f","disable_parallel_tool_use":true}}"#;
    let out = echoed("anthropic", anthropic).await;
    assert_eq!(out["tool_choice"], json!({"type":"function","name":"f"}), "{out}");
    assert_eq!(out["parallel_tool_calls"], false, "{out}");
}

/// Tool forms without a Responses projection never reach echo: the relay fence refuses them for
/// every client. Echo itself refuses another protocol's tool instead of copying it, and keeps a
/// Responses request's own hosted tool and choice as they are.
#[tokio::test]
async fn unprojectable_tools_are_refused() {
    let refused = [
        (
            "anthropic",
            r#"{"model":"m","max_tokens":5,"messages":[{"role":"user","content":"hi"}],"tools":[{"type":"web_search_20250305","name":"web_search"}]}"#,
        ),
        (
            "gemini",
            r#"{"contents":[{"role":"user","parts":[{"text":"hi"}]}],"tools":[{"googleSearch":{}}]}"#,
        ),
        (
            "responses",
            r#"{"model":"m","input":"hi","tools":[{"type":"web_search"}],"tool_choice":{"type":"web_search"}}"#,
        ),
        (
            "responses",
            r#"{"model":"m","input":"hi","tools":[{"type":"custom","name":"c"}]}"#,
        ),
    ];
    for (protocol, body) in refused {
        let error = echo_for(protocol, body.as_bytes(), &RelayCapability).await.unwrap_err();
        assert!(
            matches!(error, codec::Error::Unsupported),
            "{protocol}: {body}: {error:?}"
        );
    }
    for (protocol, body) in &refused[..2] {
        let error = echo_for(protocol, body.as_bytes(), &Open).await.unwrap_err();
        assert!(
            matches!(error, codec::Error::Unsupported),
            "{protocol}: {body}: {error:?}"
        );
    }
    let out = echo_for("responses", refused[2].1.as_bytes(), &Open).await.unwrap();
    assert_eq!(out["tools"], json!([{"type":"web_search"}]), "{out}");
    assert_eq!(out["tool_choice"], json!({"type":"web_search"}), "{out}");
}

const FLAT: &str = r#"{"model":"m","input":"hi","tools":[{"type":"function","name":"f","parameters":{"type":"object"}}],"tool_choice":{"type":"allowed_tools","mode":"required","tools":[{"type":"function","name":"f"}]}}"#;

/// A flat allowed-tools choice member or listed tool without a Chat equivalent is refused by
/// the relay fence as unsupported. A Chat client's flat choice stays malformed Chat.
#[tokio::test]
async fn an_unprojectable_allowed_tools_member_is_unsupported() {
    let hosted = FLAT.replace(
        r#""tools":[{"type":"function","name":"f"}]}"#,
        r#""tools":[{"type":"web_search"}]}"#,
    );
    let extra = FLAT.replace(r#""mode":"required""#, r#""mode":"required","extra":1"#);
    let named = FLAT.replace(
        r#"{"type":"function","name":"f"}]}"#,
        r#"{"type":"function","name":"f","strict":true}]}"#,
    );
    for body in [hosted, extra, named] {
        assert_ne!(body, FLAT);
        let error = echo_for("responses", body.as_bytes(), &RelayCapability)
            .await
            .unwrap_err();
        assert!(matches!(error, codec::Error::Unsupported), "{body}: {error:?}");
    }
    let chat = chat(
        FUNCTION,
        r#"{"type":"allowed_tools","mode":"auto","tools":[{"type":"function","name":"f"}]}"#,
    );
    let error = echo_for("chat", chat.as_bytes(), &RelayCapability).await.unwrap_err();
    assert!(matches!(error, codec::Error::Schema), "{error:?}");
}

/// The flat choice stays as sent on the way to a Responses upstream.
#[tokio::test]
async fn a_flat_allowed_tools_choice_reaches_responses_unchanged() {
    let streams = Streams::new();
    let mut request = streams.request();
    let mut ingress = responses::request::Ingress::new(
        &mut request,
        &streams.store,
        identity(Direction::Request),
        Operation::Generate,
        LIMIT,
    )
    .unwrap();
    let validated = fence(&mut ingress, &mut request, FLAT.as_bytes(), 64, &ResponsesCapability).unwrap();
    let egress = responses::request::Egress {
        destination: "responses",
        upstream_stream: false,
    };
    let sent: Value = serde_json::from_slice(&lease_bytes(&egress.encode(&validated, &mut request).unwrap())).unwrap();
    let flat: Value = serde_json::from_str(FLAT).unwrap();
    assert_eq!(sent["tool_choice"], flat["tool_choice"], "{sent}");
    assert_eq!(sent["tools"], flat["tools"], "{sent}");
}
