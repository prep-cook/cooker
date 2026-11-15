#[allow(dead_code, reason = "shared mapped codec harness")]
mod semantic;

use base64::Engine;
use ck_stream::{Read, ScratchLease};
use ck_translate::{
    codec::{
        BodyMode, Capability, Error, Media, Operation, RequestIngress, RequestView, ResponseEgress, ResponseIngress,
        event::{Direction, Identity, LocalId},
        multipart::{Boundary, Ingress as FormIngress, Limits},
    },
    images::{self, Decoder, Delivery, Destination, Egress, Format, Ingress, Mode, Wire},
};
use semantic::Mapped;
use serde_json::{Value, json};

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn bytes(lease: &ScratchLease) -> Vec<u8> {
    let mut out = Vec::new();
    while out.len() < lease.len() {
        out.extend_from_slice(lease.chunk(out.len()).unwrap());
    }
    out
}

fn identity(direction: Direction) -> Identity {
    Identity {
        request_id: LocalId::new("images-request").unwrap(),
        stream_id: LocalId::new("images-stream").unwrap(),
        attempt: 0,
        direction,
    }
}

/// Image bytes that are not text: every byte value, CR LF runs and a boundary-like line.
fn image(len: usize) -> Vec<u8> {
    let mut out = b"\x89PNG\r\n\x1a\n--xyz\r\n".to_vec();
    out.extend((0..len).map(|i| (i * 7 + i / 251) as u8));
    out
}

// ---- requests ----

fn decode(
    mapped: &mut Mapped,
    mode: Mode,
    raw: &[u8],
    chunk: usize,
) -> Result<ck_translate::codec::document::Document, u16> {
    let mut decoder = Decoder::new(&mut mapped.writer, mode, raw.len() as u64).map_err(|e| e.status())?;
    for part in raw.chunks(chunk) {
        decoder.advance(&mut mapped.writer, part).map_err(|e| e.status())?;
    }
    decoder.finish(&mut mapped.writer).map_err(|e| e.status())
}

fn destination(label: &str) -> Destination<'static> {
    match label {
        "openai" => Destination::OpenAi,
        "xai" => Destination::Xai,
        "codex" => Destination::Codex("gpt-5.4-mini"),
        other => panic!("destination {other}"),
    }
}

const PNG: &str = "data:image/png;base64,AAEC";

#[test]
fn image_requests_encode_for_each_destination_or_are_refused() {
    let codex_tool = |action: &str, extra: Value| {
        let mut tool = json!({"type":"image_generation","action":action,"model":"gpt-image-2"});
        tool.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
        tool
    };
    let codex = |tool: Value, content: Vec<Value>| {
        json!({
            "instructions":"","stream":true,"reasoning":{"effort":"medium","summary":"auto"},
            "parallel_tool_calls":true,"include":["reasoning.encrypted_content"],"model":"gpt-5.4-mini",
            "store":false,"tool_choice":{"type":"image_generation"},"tools":[tool],
            "input":[{"type":"message","role":"user","content":content}]
        })
    };
    let generation = json!({"model":"gpt-image-2","prompt":"a cat","n":1,"size":"1024x1024","quality":"high",
        "background":"opaque","output_format":"webp","output_compression":80,"moderation":"low",
        "partial_images":2,"stream":true,"response_format":"b64_json","user":"u","future":{"a/b":[null,1]},
        "x_ck":{"seq":9}});
    let edit = json!({"model":"gpt-image-2","prompt":"hat","images":[{"image_url":PNG},{"file_id":"file-1"}],
        "mask":{"image_url":PNG},"input_fidelity":"high","size":"auto"});
    let single = json!({"prompt":"hat","images":[{"image_url":PNG}],"response_format":"url"});
    let xai = json!({"model":"grok-imagine","prompt":"a cat","n":2,"response_format":"url","aspect_ratio":"16:9","resolution":"2k"});
    // (mode, wire request, destination, expected upstream body or the refusal)
    let cases: Vec<(Mode, Value, &str, Result<Value, ()>)> = vec![
        (Mode::Generate, generation.clone(), "openai", Ok(generation.clone())),
        (Mode::Edit, edit.clone(), "openai", Ok(edit.clone())),
        (Mode::Generate, xai.clone(), "xai", Ok(xai.clone())),
        (Mode::Generate, xai.clone(), "openai", Err(())),
        (Mode::Generate, generation.clone(), "xai", Err(())),
        (
            Mode::Edit,
            single.clone(),
            "xai",
            Ok(json!({"prompt":"hat","response_format":"url","image":{"url":PNG}})),
        ),
        (
            Mode::Edit,
            json!({"prompt":"hat","images":[{"image_url":PNG},{"file_id":"f","image_url":null}]}),
            "xai",
            Ok(json!({"prompt":"hat","images":[{"url":PNG},{"file_id":"f"}]})),
        ),
        (
            Mode::Edit,
            json!({"prompt":"hat","image":{"url":"https://x/a.png","type":"image_url"}}),
            "openai",
            Ok(json!({"prompt":"hat","images":[{"image_url":"https://x/a.png","type":"image_url"}]})),
        ),
        (
            Mode::Edit,
            json!({"prompt":"hat","images":[{"file_id":"f","image_url":null},{"url":"https://x/b.png"}]}),
            "openai",
            Ok(json!({"prompt":"hat","images":[{"file_id":"f"},{"image_url":"https://x/b.png"}]})),
        ),
        (
            Mode::Edit,
            json!({"prompt":"hat","image":{"url":"https://x/a.png"}}),
            "xai",
            Ok(json!({"prompt":"hat","image":{"url":"https://x/a.png"}})),
        ),
        (
            Mode::Edit,
            json!({"prompt":"hat","images":[{"url":"https://x/a.png"}]}),
            "codex",
            Ok(codex(
                codex_tool("edit", json!({})),
                vec![
                    json!({"type":"input_text","text":"hat"}),
                    json!({"type":"input_image","image_url":"https://x/a.png"}),
                ],
            )),
        ),
        // xAI: `stream` only selects the client's delivery, `user` is a documented xAI member.
        (
            Mode::Generate,
            json!({"model":"grok-imagine-image","prompt":"a cat","stream":true,"user":"u"}),
            "xai",
            Ok(json!({"model":"grok-imagine-image","prompt":"a cat","user":"u"})),
        ),
        // Nullable members are absent (OpenAI `n`, `model`, `size`, `quality`; xAI `n`, `model`).
        (
            Mode::Generate,
            json!({"model":null,"prompt":"p","n":null,"stream":null,"size":null,"quality":null}),
            "openai",
            Ok(json!({"prompt":"p"})),
        ),
        (Mode::Edit, edit.clone(), "xai", Err(())),
        (
            Mode::Generate,
            json!({"prompt":"a cat","quality":"low","stream":true,"partial_images":1,"future":1}),
            "codex",
            Ok(codex(
                codex_tool("generate", json!({"quality":"low","partial_images":1})),
                vec![json!({"type":"input_text","text":"a cat"})],
            )),
        ),
        (
            Mode::Edit,
            edit.clone(),
            "codex",
            Ok(codex(
                codex_tool(
                    "edit",
                    json!({"size":"auto","input_fidelity":"high","input_image_mask":{"image_url":PNG}}),
                ),
                vec![
                    json!({"type":"input_text","text":"hat"}),
                    json!({"type":"input_image","image_url":PNG}),
                    json!({"type":"input_image","file_id":"file-1"}),
                ],
            )),
        ),
        (Mode::Generate, json!({"prompt":"x","n":2}), "codex", Err(())),
        (Mode::Generate, json!({"prompt":"x","style":"vivid"}), "codex", Err(())),
        (
            Mode::Generate,
            json!({"prompt":"x","input_fidelity":"high"}),
            "codex",
            Err(()),
        ),
    ];
    for (mode, wire, label, expected) in cases {
        let raw = serde_json::to_vec_pretty(&wire).unwrap();
        for chunk in [1, raw.len()] {
            let mut mapped = Mapped::new();
            let doc = decode(&mut mapped, mode, &raw, chunk).unwrap();
            let result = images::encode(&mut mapped.writer, &doc, destination(label));
            match &expected {
                Ok(expected) => {
                    let body: Value = serde_json::from_slice(&bytes(&result.unwrap())).unwrap();
                    assert_eq!(&body, expected, "{label} {wire}");
                }
                Err(()) => {
                    let error = result.err().unwrap_or_else(|| panic!("{label} accepted {wire}"));
                    assert!(matches!(error.error, Error::Unsupported), "{label} {wire}");
                    assert_eq!(error.status(), 400);
                }
            }
        }
    }
}

#[test]
fn image_request_rejections_produce_no_upstream_body() {
    let cases: &[(Mode, &str, u16)] = &[
        (Mode::Generate, r#"[]"#, 400),
        (Mode::Generate, r#"{}"#, 400),
        (Mode::Generate, r#"{"prompt":""}"#, 400),
        (Mode::Generate, r#"{"prompt":"p","n":0}"#, 400),
        (Mode::Generate, r#"{"prompt":"p","n":-1}"#, 400),
        (Mode::Generate, r#"{"prompt":"p","size":1024}"#, 400),
        (Mode::Generate, r#"{"prompt":"p","stream":"true"}"#, 400),
        (Mode::Generate, r#"{"prompt":"p","partial_images":-1}"#, 400),
        (Mode::Generate, r#"{"prompt":"p","images":[{"image_url":"u"}]}"#, 400),
        (Mode::Generate, r#"{"prompt":"p","prompt":"q"}"#, 400),
        (Mode::Generate, r#"{"prompt":"unfinished"#, 400),
        (Mode::Edit, r#"{"prompt":"p"}"#, 400),
        (Mode::Edit, r#"{"prompt":"p","images":[]}"#, 400),
        (Mode::Edit, r#"{"prompt":"p","images":[{}]}"#, 400),
        (Mode::Edit, r#"{"prompt":"p","images":{"image_url":"u"}}"#, 400),
        (Mode::Edit, r#"{"prompt":"p","image":[{"image_url":"u"}]}"#, 400),
        (
            Mode::Edit,
            r#"{"prompt":"p","image":{"image_url":"u"},"mask":"m"}"#,
            400,
        ),
        (Mode::Edit, r#"{"prompt":"p","image":{"image_url":"u"}}"#, 200),
        // Official shapes only: xAI `image`/`images` are objects of `url`/`image_url`/`file_id`,
        // `n` an integer; bare strings, nested `image_url` objects and `2.0` are not documented.
        (Mode::Generate, r#"{"prompt":null}"#, 400),
        (Mode::Generate, r#"{"prompt":"p","n":2.0}"#, 400),
        (
            Mode::Generate,
            r#"{"prompt":"p","n":null,"model":null,"stream":null}"#,
            200,
        ),
        (Mode::Edit, r#"{"prompt":"p","image":"https://x/a.png"}"#, 400),
        (Mode::Edit, r#"{"prompt":"p","images":["https://x/a.png"]}"#, 400),
        (
            Mode::Edit,
            r#"{"prompt":"p","images":[{"image_url":{"url":"u"}}]}"#,
            400,
        ),
        (
            Mode::Edit,
            r#"{"prompt":"p","image":{"url":"u"},"images":[{"url":"v"}]}"#,
            400,
        ),
        (
            Mode::Edit,
            r#"{"prompt":"p","images":[{"file_id":null,"image_url":"u"}]}"#,
            200,
        ),
        (Mode::Edit, r#"{"prompt":"p","images":[{"file_id":null}]}"#, 400),
        // A reference names one address (`url` or `image_url`, not both) or one file id.
        (
            Mode::Edit,
            r#"{"prompt":"p","images":[{"url":"u","image_url":"v"}]}"#,
            400,
        ),
        (
            Mode::Edit,
            r#"{"prompt":"p","images":[{"url":"u","file_id":"f"}]}"#,
            400,
        ),
    ];
    for &(mode, raw, status) in cases {
        for chunk in [1, raw.len()] {
            let mut mapped = Mapped::new();
            let got = decode(&mut mapped, mode, raw.as_bytes(), chunk)
                .map(|_| 200)
                .unwrap_or_else(|s| s);
            assert_eq!(got, status, "{raw}");
        }
    }
    // A data URL becomes a form file only when it is one complete base64 payload: padding ends
    // it, and a media type and the `;base64` marker are present.
    for (reference, status) in [
        (json!({"image_url":"data:image/png;base64,AAEC"}), 200),
        (json!({"url":"data:image/png;base64,AAEC","filename":null}), 200),
        (json!({"image_url":"data:image/png;base64,AAA="}), 200),
        (json!({"image_url":"data:image/png;base64,AA==AAEC"}), 400),
        (json!({"image_url":"data:image/png;base64,AAE"}), 400),
        (json!({"image_url":"data:image/png,AAEC"}), 400),
        (json!({"image_url":"https://x/a.png"}), 400),
        (json!({"file_id":"f"}), 400),
    ] {
        let url = reference.to_string();
        let raw = serde_json::to_vec(&json!({"prompt":"p","images":[reference]})).unwrap();
        let mut mapped = Mapped::new();
        let doc = decode(&mut mapped, Mode::Edit, &raw, raw.len()).unwrap();
        let got = images::encode(
            &mut mapped.writer,
            &doc,
            Destination::OpenAiForm("b".repeat(32).as_str()),
        )
        .map(|_| 200)
        .unwrap_or_else(|e| e.status());
        assert_eq!(got, status, "{url}");
    }
    let raw = br#"{"prompt":"p"}"#;
    let mut mapped = Mapped::new();
    let mut decoder = Decoder::new(&mut mapped.writer, Mode::Generate, raw.len() as u64 - 1).unwrap();
    let error = decoder.advance(&mut mapped.writer, raw).err().unwrap();
    assert_eq!(error.status(), 413);
    assert!(decoder.advance(&mut mapped.writer, b" ").is_err());
    assert!(decoder.finish(&mut mapped.writer).is_err());
}

struct Admit;
impl Capability for Admit {
    fn destination(&self) -> &'static str {
        "images-test"
    }
    fn admits(&self, operation: Operation) -> bool {
        operation == Operation::Media(Media::ImageEdit)
    }
    fn validate(&self, _: RequestView<'_>) -> ck_translate::codec::Result<()> {
        Ok(())
    }
}

fn part(headers: &str, data: &[u8]) -> Vec<u8> {
    let mut out = format!("--xyz\r\n{headers}\r\n\r\n").into_bytes();
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
    out
}

fn text(name: &str, value: &str) -> Vec<u8> {
    part(
        &format!("Content-Disposition: form-data; name=\"{name}\""),
        value.as_bytes(),
    )
}

fn file(name: &str, filename: &str, mime: Option<&str>, data: &[u8]) -> Vec<u8> {
    let mut headers = format!("Content-Disposition: form-data; name=\"{name}\"; filename=\"{filename}\"");
    if let Some(mime) = mime {
        headers.push_str(&format!("\r\nContent-Type: {mime}"));
    }
    part(&headers, data)
}

/// The multipart body of an edit, read by the E1 ingress and then by `from_form`.
fn form(parts: &[Vec<u8>], chunk: usize, destination: Destination<'_>) -> Result<Vec<u8>, u16> {
    let mut body = parts.concat();
    body.extend_from_slice(b"--xyz--\r\n");
    let mut mapped = Mapped::new();
    let limits = Limits {
        body_bytes: 1 << 20,
        part_bytes: 1 << 20,
        parts: 16,
    };
    let boundary = Boundary::new(&mut mapped.writer, "multipart/form-data; boundary=xyz").map_err(|_| 400u16)?;
    let mut ingress = FormIngress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Media(Media::ImageEdit),
        &boundary,
        limits,
    )
    .map_err(|_| 400u16)?;
    for piece in body.chunks(chunk) {
        ingress.advance(&mut mapped.writer, piece).map_err(|_| 400u16)?;
    }
    let ready = ingress.finish(&mut mapped.writer, &Admit).map_err(|_| 400u16)?;
    let view = ready.for_destination("images-test").map_err(|_| 400u16)?;
    let doc = images::from_form(&mut mapped.writer, view).map_err(|e| e.status())?;
    let encoded = images::encode(&mut mapped.writer, &doc, destination).map_err(|e| e.status())?;
    Ok(bytes(&encoded))
}

#[test]
fn image_edit_forms_keep_order_bytes_and_unknown_fields_for_every_destination() {
    let first = image(300);
    let second = image(5);
    let mask = vec![0u8, 255, 13, 10];
    let parts = vec![
        text("model", "gpt-image-2"),
        file("image[]", "a \\\"1\\\".png", Some("image/png"), &first),
        text("prompt", "put a hat on it\nplease"),
        file("image[]", "b.jpg", Some("image/jpeg"), &second),
        text("n", "2"),
        text("stream", "true"),
        text("future", "kept"),
        file("mask", "m.png", Some("image/png"), &mask),
    ];
    let boundary = "b".repeat(32);
    let data_url = |mime: &str, data: &[u8]| format!("data:{mime};base64,{}", b64(data));
    let mut whole = None;
    for chunk in [1, 7, 1 << 20] {
        // xAI cannot express a mask, a count above its own or a stream flag.
        assert_eq!(form(&parts, chunk, Destination::Xai), Err(400));
        let openai = form(&parts, chunk, Destination::OpenAi).unwrap();
        let body: Value = serde_json::from_slice(&openai).unwrap();
        assert_eq!(
            body,
            json!({
                "model":"gpt-image-2","prompt":"put a hat on it\nplease","n":2,"stream":true,
                "images":[{"image_url":data_url("image/png",&first)},{"image_url":data_url("image/jpeg",&second)}],
                "mask":{"image_url":data_url("image/png",&mask)},"future":"kept"
            })
        );
        let rebuilt = form(&parts, chunk, Destination::OpenAiForm(&boundary)).unwrap();
        match &whole {
            None => whole = Some(rebuilt),
            Some(whole) => assert_eq!(&rebuilt, whole),
        }
    }
    let rebuilt = whole.unwrap();
    // Re-read the rebuilt form: same parts, bytes and filenames; `image[]` for several inputs.
    let mut expected = Vec::new();
    let head =
        |name: &str, extra: &str| format!("--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"{extra}\r\n");
    for (name, value) in [
        ("model", "gpt-image-2"),
        ("prompt", "put a hat on it\nplease"),
        ("n", "2"),
        ("stream", "true"),
    ] {
        expected.extend_from_slice(format!("{}\r\n{value}\r\n", head(name, "")).as_bytes());
    }
    for (name, filename, mime, data) in [
        ("image[]", "a %221%22.png", "image/png", &first),
        ("image[]", "b.jpg", "image/jpeg", &second),
        ("mask", "m.png", "image/png", &mask),
    ] {
        expected.extend_from_slice(
            format!(
                "{}Content-Type: {mime}\r\n\r\n",
                head(name, &format!("; filename=\"{filename}\""))
            )
            .as_bytes(),
        );
        expected.extend_from_slice(data);
        expected.extend_from_slice(b"\r\n");
    }
    expected.extend_from_slice(format!("{}\r\nkept\r\n--{boundary}--\r\n", head("future", "")).as_bytes());
    assert_eq!(rebuilt, expected);

    // xAI and Codex take the same inputs as data URLs, without the mask xAI lacks.
    let plain = vec![
        text("prompt", "p"),
        file("image", "one.png", Some("image/png"), &first),
        text("response_format", "url"),
    ];
    let xai: Value = serde_json::from_slice(&form(&plain, 3, Destination::Xai).unwrap()).unwrap();
    assert_eq!(
        xai,
        json!({"prompt":"p","response_format":"url","image":{"url":data_url("image/png",&first)}})
    );
    let codex: Value = serde_json::from_slice(&form(&plain, 3, Destination::Codex("m")).unwrap()).unwrap();
    assert_eq!(
        codex["input"][0]["content"][1],
        json!({"type":"input_image","image_url":data_url("image/png",&first)})
    );
    let one = String::from_utf8_lossy(&form(&plain, 1, Destination::OpenAiForm(&boundary)).unwrap()).into_owned();
    assert!(one.contains("name=\"image\"; filename=\"one.png\""));
}

#[test]
fn image_edit_forms_with_invalid_fields_are_client_errors() {
    let png = file("image", "a.png", Some("image/png"), &image(3));
    let cases: Vec<(&str, Vec<Vec<u8>>)> = vec![
        ("no prompt", vec![png.clone()]),
        ("no image", vec![text("prompt", "p")]),
        (
            "duplicate prompt",
            vec![text("prompt", "p"), text("prompt", "q"), png.clone()],
        ),
        (
            "n not a number",
            vec![text("prompt", "p"), text("n", "two"), png.clone()],
        ),
        (
            "images part",
            vec![
                text("prompt", "p"),
                text("images", r#"[{"image_url":"u"}]"#),
                png.clone(),
            ],
        ),
        (
            "n leading zero",
            vec![text("prompt", "p"), text("n", "02"), png.clone()],
        ),
        (
            "stream not a bool",
            vec![text("prompt", "p"), text("stream", "yes"), png.clone()],
        ),
        (
            "image without type",
            vec![text("prompt", "p"), file("image", "a.png", None, b"x")],
        ),
        (
            "prompt not utf-8",
            vec![
                part("Content-Disposition: form-data; name=\"prompt\"", &[0xff, 0xfe]),
                png.clone(),
            ],
        ),
        (
            "unknown part not utf-8",
            vec![
                text("prompt", "p"),
                part("Content-Disposition: form-data; name=\"x\"", &[0xc3]),
                png.clone(),
            ],
        ),
    ];
    for (label, parts) in cases {
        assert_eq!(form(&parts, 5, Destination::OpenAi), Err(400), "{label}");
    }
}

// ---- responses ----

struct Run {
    /// Subjects the egress reported as dropped.
    dropped: Vec<&'static str>,
    canonical: Vec<u8>,
    client: Vec<u8>,
    ingress: Result<(), Error>,
    egress: Result<(), Error>,
}

fn drain(
    mapped: &mut Mapped,
    egress: &mut Egress,
    reader: &mut ck_stream::Reader,
    client: &mut Vec<u8>,
) -> Result<(), Error> {
    while let std::task::Poll::Ready(Some(Read::Record(lease))) = reader.try_next() {
        egress.advance(&mut mapped.writer, lease, |out| {
            client.extend(bytes(&out));
            Ok(())
        })?;
    }
    Ok(())
}

fn run(wire: Wire, mode: BodyMode, body: &[u8], chunk: usize, format: Format, delivery: Delivery) -> Run {
    let mut mapped = Mapped::new();
    let limit = body.len() as u64 * 2 + 4096;
    let mut ingress = Ingress::new(&mut mapped.writer, identity(Direction::Response), wire, mode, limit).unwrap();
    let dropped = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = dropped.clone();
    let mut egress = Egress::new(
        &mut mapped.writer,
        Some(identity(Direction::Response)),
        format,
        delivery,
    )
    .unwrap()
    .on_drop(move |d| sink.lock().unwrap().push(d.subject));
    let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
    let mut client = Vec::new();
    let mut ingress_result = Ok(());
    let mut egress_result = Ok(());
    for piece in body.chunks(chunk) {
        ingress_result = ingress.advance(&mut mapped.writer, piece);
        if ingress_result.is_err() {
            break;
        }
        egress_result = egress_result.and_then(|()| drain(&mut mapped, &mut egress, &mut reader, &mut client));
    }
    if ingress_result.is_ok() {
        ingress_result = ingress.finish(&mut mapped.writer);
    }
    egress_result = egress_result.and_then(|()| drain(&mut mapped, &mut egress, &mut reader, &mut client));
    if egress_result.is_ok() {
        egress_result = egress.finish();
    }
    Run {
        dropped: dropped.lock().unwrap().clone(),
        canonical: mapped.output(&mapped.writer),
        client,
        ingress: ingress_result,
        egress: egress_result,
    }
}

/// Canonical records as (event name, data JSON).
fn records(canonical: &[u8]) -> Vec<(String, Value)> {
    let text = std::str::from_utf8(canonical).unwrap();
    text.split("\n\n")
        .filter(|record| !record.is_empty())
        .map(|record| {
            assert!(record.len() < 50_000, "record of {} bytes", record.len());
            let mut name = String::new();
            let mut data = Value::Null;
            for line in record.lines() {
                if let Some(value) = line.strip_prefix("event: ") {
                    name = value.to_owned();
                } else if let Some(value) = line.strip_prefix("data: ") {
                    data = serde_json::from_str(value).unwrap();
                }
            }
            (name, data)
        })
        .collect()
}

fn count(canonical: &[u8], event: &str) -> usize {
    records(canonical).iter().filter(|(name, _)| name == event).count()
}

fn sse(events: &[Value]) -> Vec<u8> {
    let mut out = String::new();
    for event in events {
        out.push_str(&format!(
            "event: {}\ndata: {event}\n\n",
            event["type"].as_str().unwrap_or("message")
        ));
    }
    out.into_bytes()
}

fn client_json(run: &Run) -> Value {
    serde_json::from_slice(&run.client).unwrap()
}

fn sse_events(client: &[u8]) -> Vec<(String, Value)> {
    std::str::from_utf8(client)
        .unwrap()
        .split("\n\n")
        .filter(|event| !event.is_empty())
        .map(|event| {
            let (name, data) = event.strip_prefix("event: ").unwrap().split_once("\ndata: ").unwrap();
            (name.to_owned(), serde_json::from_str(data).unwrap())
        })
        .collect()
}

const USAGE: fn() -> Value = || {
    json!({"input_tokens":50,"output_tokens":1056,"total_tokens":1106,
        "input_tokens_details":{"text_tokens":50,"image_tokens":0},"output_tokens_details":{"text_tokens":0,"image_tokens":1056}})
};

#[test]
fn image_responses_become_one_canonical_stream_and_return_in_the_clients_form() {
    let first = image(100);
    let second = image(70_000);
    let xai_usage = json!({"cost_in_usd_ticks":700000000i64,"input_tokens":10,"output_tokens":20});
    let codex_events = |first_b64: &str, second_b64: &str| {
        sse(&[
            json!({"type":"response.created","response":{"created_at":1700000000}}),
            json!({"type":"response.in_progress","response":{}}),
            json!({"type":"response.image_generation_call.partial_image","partial_image_index":0,
                "partial_image_b64":"AAEC","output_format":"png"}),
            json!({"type":"response.output_item.done","item":{"id":"ig_1","type":"image_generation_call","status":"completed",
                "result":first_b64,"revised_prompt":"a cat","output_format":"png","size":"1024x1024","quality":"high","background":"opaque"}}),
            json!({"type":"response.output_item.done","item":{"id":"ig_2","type":"image_generation_call","status":"completed",
                "result":second_b64,"output_format":"png","size":"1024x1024","quality":"high","background":"opaque"}}),
            json!({"type":"response.completed","response":{"created_at":1700000000,
                "usage":{"input_tokens":5,"output_tokens":6,"total_tokens":11},
                "tool_usage":{"image_gen":USAGE()},"tools":[{"type":"image_generation","model":"gpt-image-2"}]}}),
        ])
    };
    let (f, s) = (b64(&first), b64(&second));
    let expected_pair = json!({"created":1700000000,"background":"opaque","output_format":"png","quality":"high",
        "size":"1024x1024","usage":USAGE(),
        "data":[{"revised_prompt":"a cat","b64_json":f},{"b64_json":s}]});
    // (name, wire, body mode, body, client form, delivery, expected client JSON)
    type Case = (&'static str, Wire, BodyMode, Vec<u8>, Delivery, Value);
    let cases: Vec<Case> = vec![
        (
            "OpenAI buffered base64 with unknown members",
            Wire::OpenAi,
            BodyMode::Buffered,
            serde_json::to_vec_pretty(
                &json!({"created":1700000001,"size":"1024x1024","usage":USAGE(),"future":{"k":[1]},
                "data":[{"b64_json":f,"revised_prompt":"a cat","extra":null}]}),
            )
            .unwrap(),
            Delivery::Base64,
            json!({"created":1700000001,"size":"1024x1024","usage":USAGE(),"future":{"k":[1]},
                "data":[{"revised_prompt":"a cat","b64_json":f,"extra":null}]}),
        ),
        (
            "OpenAI buffered URL",
            Wire::OpenAi,
            BodyMode::Buffered,
            serde_json::to_vec(&json!({"created":5,"data":[{"url":"https://example.invalid/a.png"}]})).unwrap(),
            Delivery::Url,
            json!({"created":5,"data":[{"url":"https://example.invalid/a.png"}]}),
        ),
        (
            "base64 delivered as a data URL",
            Wire::OpenAi,
            BodyMode::Buffered,
            serde_json::to_vec(&json!({"created":5,"output_format":"webp","data":[{"b64_json":"AAEC"}]})).unwrap(),
            Delivery::Url,
            json!({"created":5,"output_format":"webp","data":[{"url":"data:image/webp;base64,AAEC"}]}),
        ),
        (
            "xAI buffered with cost usage",
            Wire::Xai,
            BodyMode::Buffered,
            serde_json::to_vec(
                &json!({"data":[{"b64_json":f,"mime_type":"image/jpeg","storage_error":null}],"usage":xai_usage}),
            )
            .unwrap(),
            Delivery::Base64,
            json!({"created":0,"usage":xai_usage,"data":[{"b64_json":f}]}),
        ),
        (
            "OpenAI SSE with previews and repeated usage",
            Wire::OpenAi,
            BodyMode::Streaming,
            sse(&[
                json!({"type":"image_generation.partial_image","partial_image_index":0,"b64_json":"AAEC","created_at":1700000000,"size":"1024x1024"}),
                json!({"type":"image_generation.completed","b64_json":f,"created_at":1700000000,"size":"1024x1024","quality":"high",
                    "background":"opaque","output_format":"png","usage":USAGE()}),
                json!({"type":"image_generation.completed","b64_json":s,"created_at":1700000000,"size":"1024x1024","quality":"high",
                    "background":"opaque","output_format":"png","usage":USAGE()}),
            ]),
            Delivery::Base64,
            json!({"created":1700000000,"background":"opaque","output_format":"png","quality":"high","size":"1024x1024",
                "usage":USAGE(),"data":[{"b64_json":f},{"b64_json":s}]}),
        ),
        (
            "Codex tool events",
            Wire::Codex,
            BodyMode::Streaming,
            codex_events(&f, &s),
            Delivery::Base64,
            expected_pair.clone(),
        ),
        // Null optional members are absent, as in the Images and xAI references (`url`,
        // `b64_json`, `revised_prompt`, `mime_type` and `created` are nullable).
        (
            "null optional members",
            Wire::Xai,
            BodyMode::Buffered,
            serde_json::to_vec(&json!({"created":null,"data":[{"url":"https://x/a.png","revised_prompt":null,
                "b64_json":null,"mime_type":null}]}))
            .unwrap(),
            Delivery::Url,
            json!({"created":0,"data":[{"url":"https://x/a.png"}]}),
        ),
        (
            "base64 without a media type is a data URL without one",
            Wire::Xai,
            BodyMode::Buffered,
            serde_json::to_vec(&json!({"created":1,"data":[{"b64_json":"AAEC","mime_type":null}]})).unwrap(),
            Delivery::Url,
            json!({"created":1,"data":[{"url":"data:;base64,AAEC"}]}),
        ),
        (
            "URL results keep the response settings",
            Wire::Xai,
            BodyMode::Buffered,
            serde_json::to_vec(&json!({"created":5,"size":"1024x1024","quality":"high","output_format":"png",
                "data":[{"url":"https://x/a.png","mime_type":"image/jpeg"}]}))
            .unwrap(),
            Delivery::Url,
            json!({"created":5,"output_format":"png","quality":"high","size":"1024x1024",
                "data":[{"url":"https://x/a.png"}]}),
        ),
        (
            "settings come from the first result that has them",
            Wire::OpenAi,
            BodyMode::Buffered,
            serde_json::to_vec(&json!({"created":5,"data":[{"b64_json":"AAEC"},
                {"url":"https://x/a.png","size":"1024x1024"},{"url":"https://x/b.png","size":"512x512"}]}))
            .unwrap(),
            Delivery::Url,
            json!({"created":5,"size":"1024x1024",
                "data":[{"url":"data:;base64,AAEC"},{"url":"https://x/a.png"},{"url":"https://x/b.png"}]}),
        ),
        (
            "unknown members of a streamed image reach a client of the same protocol",
            Wire::OpenAi,
            BodyMode::Streaming,
            b"id: 42\nretry: 7\n: keep\nevent: image_generation.completed\ndata: {\"type\":\"image_generation.completed\",\"b64_json\":\"AAEC\",\"future\":1}\n\n".to_vec(),
            Delivery::Base64,
            json!({"created":0,"data":[{"b64_json":"AAEC","future":1}]}),
        ),
        (
            "xAI media type reaches a data URL",
            Wire::Xai,
            BodyMode::Buffered,
            serde_json::to_vec(&json!({"created":1,"data":[{"b64_json":"AAEC","mime_type":"image/jpeg"}]})).unwrap(),
            Delivery::Url,
            json!({"created":1,"data":[{"url":"data:image/jpeg;base64,AAEC"}]}),
        ),
        (
            "Codex creation time from the first event, and a call without a result yet",
            Wire::Codex,
            BodyMode::Streaming,
            sse(&[
                json!({"type":"response.created","response":{"created_at":1700000003}}),
                json!({"type":"response.output_item.done","item":{"type":"image_generation_call","result":null}}),
                json!({"type":"response.output_item.done","item":{"type":"image_generation_call","result":"AAEC"}}),
                json!({"type":"response.completed","response":{}}),
            ]),
            Delivery::Base64,
            json!({"created":1700000003,"data":[{"b64_json":"AAEC"}]}),
        ),
        (
            "a later creation time reaches the client",
            Wire::Codex,
            BodyMode::Streaming,
            sse(&[
                json!({"type":"response.image_generation_call.partial_image","partial_image_index":0,"partial_image_b64":"AAEC"}),
                json!({"type":"response.output_item.done","item":{"type":"image_generation_call","result":"AAEC"}}),
                json!({"type":"response.completed","response":{"created_at":1700000009}}),
            ]),
            Delivery::Base64,
            json!({"created":1700000009,"data":[{"b64_json":"AAEC"}]}),
        ),
    ];
    for (label, wire, mode, body, delivery, expected) in cases {
        let mut whole: Option<(Vec<u8>, Vec<u8>)> = None;
        for chunk in [body.len(), 1] {
            let result = run(wire, mode, &body, chunk, Format::Json, delivery);
            result.ingress.as_ref().unwrap_or_else(|e| panic!("{label}: {e}"));
            result.egress.as_ref().unwrap_or_else(|e| panic!("{label}: {e}"));
            assert_eq!(client_json(&result), expected, "{label}");
            match &whole {
                None => whole = Some((result.canonical, result.client)),
                Some((canonical, client)) => {
                    assert_eq!(&result.canonical, canonical, "{label}: one-byte feed differs");
                    assert_eq!(&result.client, client, "{label}: one-byte feed differs at egress");
                }
            }
        }
    }
    // §3.1: the source's SSE `id`, `retry` and comment ride on the `image.result` they came with
    // (the event name is the grammar's own); the client has no place for them, which is reported.
    let wire = b"id: 42\nretry: 7\n: keep\nevent: image_generation.completed\n\
        data: {\"type\":\"image_generation.completed\",\"b64_json\":\"AAEC\"}\n\n";
    let result = run(
        Wire::OpenAi,
        BodyMode::Streaming,
        wire,
        3,
        Format::Json,
        Delivery::Base64,
    );
    result.ingress.as_ref().unwrap();
    result.egress.as_ref().unwrap();
    let operation = records(&result.canonical)
        .into_iter()
        .find(|(name, _)| name == "x_ck.operation")
        .unwrap()
        .1;
    let fields: Vec<(&str, &str, bool)> = operation["x_ck"]["source"]["sse"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| {
            (
                f["name"].as_str().unwrap(),
                f["value"].as_str().unwrap(),
                f["comment"].as_bool().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        fields,
        [
            ("id", "42", false),
            ("retry", "7", false),
            ("", "keep", true),
            ("event", "image_generation.completed", false)
        ]
    );
    assert_eq!(operation["x_ck"]["source"]["protocol"], "openai-images");
    assert_eq!(result.dropped, vec!["SSE id, retry and comment fields"]);
    // A preview has no `image.result`: its media carries the SSE record. A bare event name is the
    // grammar's own and reports nothing, an `id` alone does.
    for (wire, dropped) in [
        (
            &b"id: 9\nevent: image_generation.partial_image\ndata: {\"type\":\"image_generation.partial_image\",\"b64_json\":\"AAEC\"}\n\n\
                event: image_generation.completed\ndata: {\"type\":\"image_generation.completed\",\"b64_json\":\"AAEC\"}\n\n"[..],
            vec!["SSE id, retry and comment fields"],
        ),
        (
            &b"event: image_generation.completed\ndata: {\"type\":\"image_generation.completed\",\"b64_json\":\"AAEC\"}\n\n"[..],
            vec![],
        ),
    ] {
        let result = run(Wire::OpenAi, BodyMode::Streaming, wire, 3, Format::Json, Delivery::Base64);
        result.egress.as_ref().unwrap();
        assert_eq!(result.dropped, dropped);
        if !dropped.is_empty() {
            let start = records(&result.canonical)
                .into_iter()
                .find(|(name, _)| name == "x_ck.media.start")
                .unwrap()
                .1;
            assert_eq!(start["x_ck"]["source"]["sse"][0]["name"], "id");
        }
    }
    // Members of the xAI source have no place in an OpenAI Images response either.
    let xai = br#"{"data":[{"b64_json":"AAEC","storage_error":null}],"future":1}"#;
    let result = run(Wire::Xai, BodyMode::Buffered, xai, 7, Format::Json, Delivery::Base64);
    result.egress.as_ref().unwrap();
    assert_eq!(client_json(&result), json!({"created":0,"data":[{"b64_json":"AAEC"}]}));
    assert_eq!(result.dropped, vec!["members of another protocol"; 2]);
}

#[test]
fn image_usage_is_published_once_before_the_images() {
    let b = b64(&image(10));
    let xai = serde_json::to_vec(&json!({"created":1,"data":[{"url":"u1"},{"url":"u2"}],
        "usage":{"cost_in_usd_ticks":7,"input_tokens":1}}))
    .unwrap();
    let two = sse(&[
        json!({"type":"image_edit.completed","b64_json":b,"usage":USAGE()}),
        json!({"type":"image_edit.completed","b64_json":b,"usage":USAGE()}),
    ]);
    let codex = sse(&[
        json!({"type":"response.output_item.done","item":{"type":"image_generation_call","result":b}}),
        json!({"type":"response.completed","response":{"usage":{"input_tokens":1},"tool_usage":{"image_gen":USAGE()}}}),
    ]);
    for (label, wire, mode, body, usages) in [
        ("xAI", Wire::Xai, BodyMode::Buffered, xai, 1),
        ("two completions", Wire::OpenAi, BodyMode::Streaming, two, 1),
        ("Codex main and tool", Wire::Codex, BodyMode::Streaming, codex, 2),
    ] {
        let result = run(wire, mode, &body, 3, Format::Json, Delivery::Base64);
        result.ingress.unwrap_or_else(|e| panic!("{label}: {e}"));
        let records = records(&result.canonical);
        assert_eq!(
            records.iter().filter(|(n, _)| n == "x_ck.usage").count(),
            usages,
            "{label}"
        );
        let usage = records.iter().position(|(n, _)| n == "x_ck.usage").unwrap();
        let first = records
            .iter()
            .position(|(n, _)| n == "x_ck.media.start" || n == "x_ck.operation")
            .unwrap();
        assert!(usage < first, "{label}");
    }
    // The event is `{usage: UsageSnapshot}`: the accounting reader requires that member, the
    // counts are canonical, and the provider's own fields (`cost_in_usd_ticks`) stay in `native`.
    let snapshot = |wire: Wire, body: Value, mode: BodyMode| {
        let body = if mode == BodyMode::Streaming {
            sse(&[body])
        } else {
            serde_json::to_vec(&body).unwrap()
        };
        let result = run(wire, mode, &body, 4, Format::Json, Delivery::Url);
        result.ingress.unwrap();
        records(&result.canonical)
            .into_iter()
            .filter(|(n, _)| n == "x_ck.usage")
            .map(|(_, record)| record)
            .collect::<Vec<_>>()
    };
    let xai_usage = json!({"cost_in_usd_ticks":7,"input_tokens":10,"output_tokens":20,"total_tokens":30,
        "input_tokens_details":{"cached_tokens":4,"image_tokens":1,"text_tokens":9},
        "output_tokens_details":{"image_tokens":15,"reasoning_tokens":2,"text_tokens":3}});
    let usage = snapshot(
        Wire::Xai,
        json!({"created":1,"data":[{"url":"u"}],"usage":xai_usage}),
        BodyMode::Buffered,
    );
    assert_eq!(usage.len(), 1);
    assert!(usage[0].get("native").is_none() && usage[0].get("final").is_none());
    assert_eq!(
        usage[0]["usage"],
        json!({"native":{"protocol":"xai-images","value":xai_usage},"final":true,
            "input_tokens":10,"output_tokens":20,"total_tokens":30,"cache_read_tokens":4,"reasoning_tokens":2,
            "details":{"input_text_tokens":9,"input_image_tokens":1,"output_text_tokens":3,"output_image_tokens":15}})
    );
    // A null count stays out of the canonical counts; the native value keeps it.
    let usage = snapshot(
        Wire::OpenAi,
        json!({"type":"image_generation.completed","b64_json":"AAEC","usage":{"input_tokens":null,"output_tokens":3}}),
        BodyMode::Streaming,
    );
    assert_eq!(usage[0]["usage"]["output_tokens"], 3);
    assert!(usage[0]["usage"].get("input_tokens").is_none());
    assert_eq!(usage[0]["usage"]["native"]["value"]["input_tokens"], Value::Null);
    let tool = records(
        &run(
            Wire::Codex,
            BodyMode::Streaming,
            &sse(&[
                json!({"type":"response.output_item.done","item":{"type":"image_generation_call","result":"AAEC"}}),
                json!({"type":"response.completed","response":{"tools":[{"type":"image_generation","model":"gpt-image-2"}],
            "tool_usage":{"image_gen":USAGE()}}}),
            ]),
            5,
            Format::Json,
            Delivery::Base64,
        )
        .canonical,
    )
    .into_iter()
    .find(|(n, _)| n == "x_ck.usage")
    .unwrap()
    .1;
    assert_eq!(tool["scope"], "tool:gpt-image-2");
    assert_eq!(tool["usage"]["details"]["output_image_tokens"], 1056);
}

#[test]
fn streaming_clients_get_created_and_revised_prompt_from_buffered_and_streamed_results() {
    let data = image(40);
    let b = b64(&data);
    let buffered = serde_json::to_vec(&json!({"created":1700000002,"quality":"low","usage":USAGE(),
        "data":[{"b64_json":b,"revised_prompt":"revised"}]}))
    .unwrap();
    let streamed = sse(&[
        json!({"type":"image_generation.partial_image","partial_image_index":2,"b64_json":"AAEC","created_at":1700000002,"future":1}),
        json!({"type":"image_generation.completed","b64_json":b,"created_at":1700000002,"quality":"low","usage":USAGE()}),
    ]);
    let completed = json!({"type":"image_generation.completed","created_at":1700000002,"b64_json":b,"quality":"low","usage":USAGE()});
    for chunk in [buffered.len(), 1] {
        let result = run(
            Wire::OpenAi,
            BodyMode::Buffered,
            &buffered,
            chunk,
            Format::GenerationEvents,
            Delivery::Base64,
        );
        result.egress.as_ref().unwrap();
        let events = sse_events(&result.client);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "image_generation.completed");
        let mut expected = completed.clone();
        expected["revised_prompt"] = json!("revised");
        assert_eq!(
            events[0].1, expected,
            "L15: created and revised prompt reach emulated completed events"
        );
    }
    // No creation time at the source: the synthetic zero is written, as buffered JSON does.
    let result = run(
        Wire::Xai,
        BodyMode::Buffered,
        &serde_json::to_vec(&json!({"data":[{"b64_json":"AAEC","revised_prompt":"r"}]})).unwrap(),
        3,
        Format::GenerationEvents,
        Delivery::Base64,
    );
    assert_eq!(
        sse_events(&result.client),
        vec![(
            "image_generation.completed".to_owned(),
            json!({"type":"image_generation.completed","created_at":0,"revised_prompt":"r","b64_json":"AAEC"})
        )]
    );
    for chunk in [streamed.len(), 1] {
        let result = run(
            Wire::OpenAi,
            BodyMode::Streaming,
            &streamed,
            chunk,
            Format::EditEvents,
            Delivery::Base64,
        );
        result.egress.as_ref().unwrap();
        let events = sse_events(&result.client);
        assert_eq!(
            events,
            vec![
                (
                    "image_edit.partial_image".to_owned(),
                    json!({"type":"image_edit.partial_image","partial_image_index":2,"created_at":1700000002,"b64_json":"AAEC","future":1})
                ),
                (
                    "image_edit.completed".to_owned(),
                    json!({"type":"image_edit.completed","created_at":1700000002,"b64_json":b,"quality":"low","usage":USAGE()})
                ),
            ]
        );
    }
}

#[test]
fn buffered_and_event_results_carry_the_same_image_bytes() {
    let data = image(200_000);
    let b = b64(&data);
    let buffered = serde_json::to_vec(&json!({"created":9,"output_format":"png","data":[{"b64_json":b}]})).unwrap();
    let streamed =
        sse(&[json!({"type":"image_generation.completed","b64_json":b,"created_at":9,"output_format":"png"})]);
    let from_buffered = run(
        Wire::OpenAi,
        BodyMode::Buffered,
        &buffered,
        4096,
        Format::Json,
        Delivery::Base64,
    );
    let from_events = run(
        Wire::OpenAi,
        BodyMode::Streaming,
        &streamed,
        4096,
        Format::Json,
        Delivery::Base64,
    );
    for result in [&from_buffered, &from_events] {
        result.ingress.as_ref().unwrap();
        result.egress.as_ref().unwrap();
        let json = client_json(result);
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(json["data"][0]["b64_json"].as_str().unwrap())
                .unwrap(),
            data
        );
    }
    assert_eq!(client_json(&from_buffered), client_json(&from_events));
    // The canonical media carries the exact bytes in records under the ceiling, after one start.
    let mut raw = Vec::new();
    let mut ids = std::collections::HashSet::new();
    for (name, record) in records(&from_events.canonical) {
        if name == "x_ck.data" {
            assert_eq!(record["offset"], raw.len());
            raw.extend(
                base64::engine::general_purpose::STANDARD
                    .decode(record["bytes"].as_str().unwrap())
                    .unwrap(),
            );
            ids.insert(record["x_ck"]["media_id"].as_str().unwrap().to_owned());
        }
    }
    assert_eq!(raw, data);
    assert_eq!(ids.len(), 1);
    assert_eq!(count(&from_events.canonical, "x_ck.media.end"), 1);
}

#[test]
fn incomplete_or_failed_image_responses_end_in_error_never_success() {
    let b = "AAEC";
    let completed = json!({"type":"image_generation.completed","b64_json":b});
    let cases: Vec<(&str, Wire, BodyMode, Vec<u8>)> = vec![
        (
            "previews only",
            Wire::OpenAi,
            BodyMode::Streaming,
            sse(&[json!({"type":"image_generation.partial_image","partial_image_index":0,"b64_json":b})]),
        ),
        ("no events", Wire::OpenAi, BodyMode::Streaming, Vec::new()),
        (
            "error event",
            Wire::OpenAi,
            BodyMode::Streaming,
            sse(&[json!({"type":"error","error":{"message":"x"}})]),
        ),
        (
            "error member",
            Wire::OpenAi,
            BodyMode::Streaming,
            sse(&[json!({"type":"image_generation.completed","error":{}})]),
        ),
        (
            "error member beside an image",
            Wire::OpenAi,
            BodyMode::Streaming,
            sse(&[json!({"type":"image_generation.completed","b64_json":b,"error":{}})]),
        ),
        (
            "error without a type, before an image",
            Wire::OpenAi,
            BodyMode::Streaming,
            [
                b"data: {\"error\":{\"message\":\"x\"}}\n\n".to_vec(),
                sse(std::slice::from_ref(&completed)),
            ]
            .concat(),
        ),
        (
            "preview with a URL",
            Wire::OpenAi,
            BodyMode::Streaming,
            [
                sse(&[json!({"type":"image_generation.partial_image","url":"https://x/a.png"})]),
                sse(std::slice::from_ref(&completed)),
            ]
            .concat(),
        ),
        (
            "Codex event of a known family that is not JSON",
            Wire::Codex,
            BodyMode::Streaming,
            [
                b"event: response.output_item.done\ndata: nope\n\n".to_vec(),
                sse(&[
                    json!({"type":"response.output_item.done","item":{"type":"image_generation_call","result":b}}),
                    json!({"type":"response.completed","response":{}}),
                ]),
            ]
            .concat(),
        ),
        (
            "cut event",
            Wire::OpenAi,
            BodyMode::Streaming,
            sse(std::slice::from_ref(&completed))[..40].to_vec(),
        ),
        (
            "neither bytes nor URL",
            Wire::OpenAi,
            BodyMode::Streaming,
            sse(&[json!({"type":"image_generation.completed"})]),
        ),
        (
            "both bytes and URL",
            Wire::OpenAi,
            BodyMode::Streaming,
            sse(&[json!({"type":"image_generation.completed","b64_json":b,"url":"u"})]),
        ),
        (
            "empty base64",
            Wire::OpenAi,
            BodyMode::Buffered,
            br#"{"created":1,"data":[{"b64_json":""}]}"#.to_vec(),
        ),
        (
            "empty URL",
            Wire::Xai,
            BodyMode::Buffered,
            br#"{"created":1,"data":[{"url":""}]}"#.to_vec(),
        ),
        (
            "null bytes and no URL",
            Wire::OpenAi,
            BodyMode::Buffered,
            br#"{"created":1,"data":[{"b64_json":null,"url":null}]}"#.to_vec(),
        ),
        (
            "bad base64",
            Wire::OpenAi,
            BodyMode::Streaming,
            sse(&[json!({"type":"image_generation.completed","b64_json":"AA*C"})]),
        ),
        (
            "empty data",
            Wire::OpenAi,
            BodyMode::Buffered,
            br#"{"created":1,"data":[]}"#.to_vec(),
        ),
        (
            "missing data",
            Wire::OpenAi,
            BodyMode::Buffered,
            br#"{"created":1}"#.to_vec(),
        ),
        (
            "upstream error body",
            Wire::Xai,
            BodyMode::Buffered,
            br#"{"error":{"code":"internal_error","message":"m"}}"#.to_vec(),
        ),
        (
            "truncated JSON",
            Wire::OpenAi,
            BodyMode::Buffered,
            br#"{"created":1,"data":[{"b64_json":"AA"#.to_vec(),
        ),
        (
            "Codex without completion",
            Wire::Codex,
            BodyMode::Streaming,
            sse(&[json!({"type":"response.output_item.done","item":{"type":"image_generation_call","result":b}})]),
        ),
        (
            "Codex completion without images",
            Wire::Codex,
            BodyMode::Streaming,
            sse(&[json!({"type":"response.completed","response":{}})]),
        ),
        (
            "Codex failure",
            Wire::Codex,
            BodyMode::Streaming,
            sse(&[json!({"type":"response.failed","response":{}})]),
        ),
    ];
    for (label, wire, mode, body) in cases {
        for chunk in [body.len().max(1), 1] {
            let result = run(wire, mode, &body, chunk, Format::Json, Delivery::Base64);
            assert!(result.ingress.is_err(), "{label}: accepted");
            assert!(result.egress.is_err(), "{label}: egress completed");
            assert!(result.client.is_empty(), "{label}: body before completion");
            assert_eq!(count(&result.canonical, "x_ck.error"), 1, "{label}");
            assert_eq!(count(&result.canonical, "x_ck.done"), 0, "{label}");
        }
    }
    // A body read failure ends the stream in error whatever the ingress holds.
    let mut mapped = Mapped::new();
    let mut ingress = Ingress::new(
        &mut mapped.writer,
        identity(Direction::Response),
        Wire::OpenAi,
        BodyMode::Streaming,
        4096,
    )
    .unwrap();
    ingress
        .advance(
            &mut mapped.writer,
            b"event: image_generation.completed\ndata: {\"type\"",
        )
        .unwrap();
    ingress.fail(&mut mapped.writer, 502, "upstream").unwrap();
    assert!(matches!(ingress.advance(&mut mapped.writer, b" "), Err(Error::Closed)));
    assert_eq!(count(&mapped.output(&mapped.writer), "x_ck.error"), 1);
}

#[test]
fn urls_are_not_base64_and_unknown_events_never_reach_the_client() {
    let url = serde_json::to_vec(&json!({"created":1,"data":[{"url":"https://example.invalid/a.png"}]})).unwrap();
    let result = run(
        Wire::OpenAi,
        BodyMode::Buffered,
        &url,
        5,
        Format::Json,
        Delivery::Base64,
    );
    result.ingress.unwrap();
    assert!(matches!(result.egress, Err(Error::Unsupported)));
    assert!(result.client.is_empty());

    let events = sse(&[
        json!({"type":"image_generation.future","x":1}),
        json!({"type":"image_generation.completed","b64_json":"AAEC"}),
    ]);
    let result = run(
        Wire::OpenAi,
        BodyMode::Streaming,
        &events,
        6,
        Format::Json,
        Delivery::Base64,
    );
    result.ingress.unwrap();
    result.egress.unwrap();
    assert_eq!(count(&result.canonical, "x_ck.opaque"), 1);
    // `{name?, format, value}` (§3.1), with the source's SSE fields in `X.source.sse`; text and
    // non-object data are opaque too, and a client never sees them.
    let wire = b"id: 7\nevent: image_generation.future\ndata: {\"type\":\"image_generation.future\",\"x\":1}\n\n\
        event: ping\ndata: hello\n\ndata: [1,2]\n\n\
        event: image_generation.completed\ndata: {\"type\":\"image_generation.completed\",\"b64_json\":\"AAEC\"}\n\n";
    let result = run(
        Wire::OpenAi,
        BodyMode::Streaming,
        wire,
        5,
        Format::Json,
        Delivery::Base64,
    );
    result.ingress.as_ref().unwrap();
    result.egress.as_ref().unwrap();
    let opaque: Vec<Value> = records(&result.canonical)
        .into_iter()
        .filter(|(name, _)| name == "x_ck.opaque")
        .map(|(_, record)| record)
        .collect();
    assert_eq!(opaque.len(), 3);
    assert_eq!(opaque[0]["name"], "image_generation.future");
    assert_eq!(opaque[0]["format"], "json");
    assert_eq!(opaque[0]["value"], json!({"type":"image_generation.future","x":1}));
    assert!(opaque[0].get("protocol").is_none());
    assert_eq!(opaque[0]["x_ck"]["source"]["sse"][0]["name"], "id");
    assert_eq!(
        (&opaque[1]["format"], &opaque[1]["value"]),
        (&json!("text"), &json!("hello"))
    );
    assert_eq!(
        (&opaque[2]["format"], &opaque[2]["value"]),
        (&json!("json"), &json!([1, 2]))
    );
    assert_eq!(client_json(&result), json!({"created":0,"data":[{"b64_json":"AAEC"}]}));
    // §3.7: each event the client grammar cannot hold is reported without its content.
    assert_eq!(result.dropped, vec!["an event without an Images form"; 3]);
    assert_eq!(
        client_json(&run(
            Wire::OpenAi,
            BodyMode::Streaming,
            &events,
            6,
            Format::Json,
            Delivery::Base64
        ))["data"],
        json!([{"b64_json":"AAEC"}])
    );
}

#[test]
fn image_response_ingress_refuses_foreign_writers_and_unsupported_framings() {
    let mut mapped = Mapped::new();
    assert!(
        Ingress::new(
            &mut mapped.writer,
            identity(Direction::Request),
            Wire::OpenAi,
            BodyMode::Buffered,
            10
        )
        .is_err()
    );
    assert!(
        Ingress::new(
            &mut mapped.writer,
            identity(Direction::Response),
            Wire::Xai,
            BodyMode::Streaming,
            10
        )
        .is_err()
    );
    assert!(
        Ingress::new(
            &mut mapped.writer,
            identity(Direction::Response),
            Wire::Codex,
            BodyMode::Buffered,
            10
        )
        .is_err()
    );
    let mut other = Mapped::new();
    let mut ingress = Ingress::new(
        &mut mapped.writer,
        identity(Direction::Response),
        Wire::OpenAi,
        BodyMode::Buffered,
        10,
    )
    .unwrap();
    assert!(ingress.advance(&mut other.writer, b"{").is_err());
    assert!(matches!(
        ingress.advance(&mut mapped.writer, b"01234567890123"),
        Err(Error::EventTooLarge)
    ));
    assert!(matches!(ingress.advance(&mut mapped.writer, b"{"), Err(Error::Closed)));
}
