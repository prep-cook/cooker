//! Request ingress binds each node to the pointer and value its `request.field` record holds,
//! from the coordinates it wrote. These requests check, for every protocol and every split of
//! the input, that member names needing JSON escapes or RFC 6901 `~0`/`~1`, string versus
//! number scalars, null members and late duplicate keys keep their meaning.
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
#[path = "semantic/mod.rs"]
mod semantic;

use codec::{
    Operation, RequestEgress, RequestIngress,
    event::{Direction, Identity, LocalId},
};
use semantic::Mapped;
use serde_json::Value;

/// Schema properties whose names need escapes, and whose values are a number, a string with
/// the same digits, a null, a boolean, empty containers and a negative zero.
const PROPERTIES: &str = r#"{"a/b":{"type":"number","default":1.50},"c~d":{"type":"string","default":"1.50"},"ef":{"default":null},"g\"h":{"type":"boolean","default":false},"i\/j":{"type":"array","items":{}},"~1":{"type":"object","properties":{}},"é":{"type":"integer","default":-0}}"#;
/// The names above as encoded pointer segments.
const SEGMENTS: [&str; 7] = ["a~1b", "c~0d", "ef", "g\"h", "i~1j", "~01", "é"];

#[derive(Clone, Copy, Debug)]
enum Protocol {
    Chat,
    Anthropic,
    Responses,
    Gemini,
}

impl Protocol {
    const ALL: [Self; 4] = [Self::Chat, Self::Anthropic, Self::Responses, Self::Gemini];

    fn body(self, properties: &str) -> String {
        let schema = format!(r#"{{"type":"object","properties":{properties}}}"#);
        match self {
            Self::Chat => format!(
                r#"{{"model":"m","messages":[{{"role":"user","content":"q"}}],"tools":[{{"type":"function","function":{{"name":"f","parameters":{schema}}}}}]}}"#
            ),
            Self::Anthropic => format!(
                r#"{{"model":"m","max_tokens":8,"messages":[{{"role":"user","content":"q"}}],"tools":[{{"name":"f","input_schema":{schema}}}]}}"#
            ),
            Self::Responses => format!(
                r#"{{"model":"m","input":"q","tools":[{{"type":"function","name":"f","parameters":{schema}}}]}}"#
            ),
            Self::Gemini => format!(
                r#"{{"contents":[{{"role":"user","parts":[{{"text":"q"}}]}}],"tools":[{{"functionDeclarations":[{{"name":"f","parametersJsonSchema":{schema}}}]}}]}}"#
            ),
        }
    }

    /// The tool schema's properties in this protocol's request.
    fn properties(self, request: &Value) -> &Value {
        match self {
            Self::Chat => &request["tools"][0]["function"]["parameters"]["properties"],
            Self::Anthropic => &request["tools"][0]["input_schema"]["properties"],
            Self::Responses => &request["tools"][0]["parameters"]["properties"],
            Self::Gemini => &request["tools"][0]["functionDeclarations"][0]["parametersJsonSchema"]["properties"],
        }
    }
}

fn identity() -> Identity {
    Identity {
        request_id: LocalId::new("request").unwrap(),
        stream_id: LocalId::new("stream").unwrap(),
        attempt: 1,
        direction: Direction::Request,
    }
}

/// Feeds `parts` to `ingress`, then validates and encodes the request for its own protocol.
fn same_protocol<I: RequestIngress>(
    mapped: &mut Mapped,
    mut ingress: I,
    parts: &[&[u8]],
    capability: &impl codec::Capability,
    egress: &impl RequestEgress,
) -> codec::Result<Vec<u8>> {
    for part in parts {
        ingress.advance(&mut mapped.writer, part)?;
    }
    let ready = ingress.finish(&mut mapped.writer, capability)?;
    let lease = egress.encode(&ready, &mut mapped.writer)?;
    let mut bytes = Vec::new();
    while bytes.len() < lease.len() {
        bytes.extend_from_slice(lease.chunk(bytes.len()).unwrap());
    }
    Ok(bytes)
}

/// The same-protocol request, or the codec error, and the canonical stream ingress published.
fn translate(protocol: Protocol, parts: &[&[u8]]) -> (codec::Result<Vec<u8>>, Vec<u8>) {
    let mut mapped = Mapped::new();
    let (writer, store, operation, limit) = (&mut mapped.writer, &mapped.store, Operation::Generate, 1 << 20);
    let output = match protocol {
        Protocol::Chat => {
            let ingress = codec::chat::request::Ingress::new(writer, store, identity(), operation, limit).unwrap();
            let egress = codec::chat::request::Egress {
                destination: "chat",
                upstream_stream: false,
            };
            same_protocol(
                &mut mapped,
                ingress,
                parts,
                &codec::chat::request::ChatCapability,
                &egress,
            )
        }
        Protocol::Anthropic => {
            let ingress = anthropic::request::Ingress::new(writer, store, identity(), operation, limit).unwrap();
            let egress = anthropic::request::Egress {
                destination: "anthropic",
                upstream_stream: false,
                default_max_tokens: None,
            };
            let capability = anthropic::request::AnthropicCapability::default();
            same_protocol(&mut mapped, ingress, parts, &capability, &egress)
        }
        Protocol::Responses => {
            let ingress = responses::request::Ingress::new(writer, store, identity(), operation, limit).unwrap();
            let egress = responses::request::Egress {
                destination: "responses",
                upstream_stream: false,
            };
            let capability = responses::request::ResponsesCapability;
            same_protocol(&mut mapped, ingress, parts, &capability, &egress)
        }
        Protocol::Gemini => {
            let ingress = gemini::request::Ingress::new(writer, store, identity(), operation, "m", limit).unwrap();
            let egress = gemini::request::Egress {
                destination: "gemini",
                upstream_stream: false,
            };
            same_protocol(&mut mapped, ingress, parts, &gemini::request::GeminiCapability, &egress)
        }
    };
    let canonical = mapped.output(&mapped.writer);
    (output, canonical)
}

/// The whole input, its 1-byte feed and every split into two advances.
fn feeds(input: &[u8]) -> Vec<Vec<&[u8]>> {
    let mut feeds = vec![vec![input], input.chunks(1).collect()];
    feeds.extend((1..input.len()).map(|at| vec![&input[..at], &input[at..]]));
    feeds
}

fn pointers(canonical: &[u8]) -> Vec<String> {
    std::str::from_utf8(canonical)
        .unwrap()
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter_map(|line| {
            serde_json::from_str::<Value>(line).unwrap()["pointer"]
                .as_str()
                .map(str::to_owned)
        })
        .collect()
}

#[test]
fn escaped_names_and_scalars_bind_on_every_protocol_and_split() {
    let expected: Value = serde_json::from_str(PROPERTIES).unwrap();
    for protocol in Protocol::ALL {
        let body = protocol.body(PROPERTIES);
        let (whole, canonical) = translate(protocol, &[body.as_bytes()]);
        let whole = whole.unwrap_or_else(|error| panic!("{protocol:?}: {error:?}"));
        let request: Value = serde_json::from_slice(&whole).unwrap();
        assert_eq!(protocol.properties(&request), &expected, "{protocol:?}");
        let text = std::str::from_utf8(&whole).unwrap();
        for literal in [
            r#""default":1.50"#,
            r#""default":"1.50""#,
            r#""default":null"#,
            r#""default":-0"#,
        ] {
            assert!(text.contains(literal), "{protocol:?} lost {literal}: {text}");
        }
        let pointers = pointers(&canonical);
        for segment in SEGMENTS {
            assert!(
                pointers
                    .iter()
                    .any(|pointer| pointer.split('/').any(|part| part == segment)),
                "{protocol:?}: no pointer names {segment:?} in {pointers:?}"
            );
        }
        for parts in feeds(body.as_bytes()) {
            let (output, _) = translate(protocol, &parts);
            assert_eq!(output.unwrap(), whole, "{protocol:?} split {:?}", parts[0].len());
        }
    }
}

/// A duplicate spelled with escapes still meets the first name after that member's field is
/// published and its name rebound to the record's pointer.
#[test]
fn late_escaped_duplicate_names_fail_on_every_protocol_and_split() {
    let filler = "x".repeat(600);
    let properties = format!(r#"{{"a/b":{{"type":"string"}},"c":{{"description":"{filler}"}},"a\/b":{{}}}}"#);
    for protocol in Protocol::ALL {
        let body = protocol.body(&properties);
        for parts in feeds(body.as_bytes()) {
            let (output, canonical) = translate(protocol, &parts);
            assert!(
                matches!(output, Err(codec::Error::DuplicateKey)),
                "{protocol:?} split {}: {output:?}",
                parts[0].len()
            );
            assert!(
                !std::str::from_utf8(&canonical)
                    .unwrap()
                    .contains("event: x_ck.request.end\n")
            );
        }
    }
}

/// A name copied from storage writes the segment that decoding it would write, and a pointer
/// name is checked as RFC 6901 decoding checks it.
#[test]
fn copied_pointer_segments_match_decoded_names() {
    use codec::document::{Document, Value};
    let mut mapped = Mapped::new();
    let mut source = codec::source::Source::new(&mut mapped.writer).unwrap();
    let body = br#"{"valid":"/x/a~1b~0","invalid":"/x/a~2","escaped":"e\u0066~/"}"#;
    source.push_bytes(&mut mapped.writer, body).unwrap();
    let doc = Document::parse(&mut mapped.writer, source, 1024).unwrap();
    let mut write = |member: &str, pointer: bool| {
        let node = doc.member(doc.root(), member).unwrap().unwrap();
        let Value::String(text) = doc.node(node).unwrap().value else {
            panic!("{member} is not a string");
        };
        let name = if pointer {
            doc.pointer_name(text)?
        } else {
            doc.mapped_name(text)?
        };
        let lease = codec::encode::scratch(&mut mapped.writer, |e| {
            codec::provenance::write_pointer_segment(&doc, name, e)
        })?;
        Ok::<_, codec::Error>(lease.chunk(0).unwrap().to_vec())
    };
    assert_eq!(write("valid", true).unwrap(), b"a~1b~0");
    assert!(matches!(write("invalid", true), Err(codec::Error::Schema)));
    assert_eq!(write("escaped", false).unwrap(), b"ef~0~1");
}
