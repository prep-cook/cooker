use super::*;

const INPUT: &[u8] = br#"{"model":"m","input":"hi","prompt_cache_options":{"ttl":"24h"},"stream_options":{"reasoning_summary_delivery":"sequential_cutoff","include_obfuscation":false}}"#;

#[test]
fn codex_options_do_not_reach_public_responses() {
    let mut m = Mapped::new();
    let mut ingress = responses::request::Ingress::new(
        &mut m.writer,
        &m.store,
        identity(Direction::Request),
        Operation::Generate,
        1 << 20,
    )
    .unwrap();
    ingress.advance(&mut m.writer, INPUT).unwrap();
    assert!(
        matches!(
            ingress.finish(&mut m.writer, &responses::request::ResponsesCapability),
            Err(codec::Error::Unsupported)
        ),
        "Codex-only controls must not authorize public Responses upload"
    );
}

#[test]
fn codex_options_have_an_isolated_provider_map() {
    struct Inspect;
    impl codec::Capability for Inspect {
        fn destination(&self) -> &'static str {
            "inspect"
        }
        fn validate(&self, _: codec::RequestView<'_>) -> codec::Result<()> {
            Ok(())
        }
    }
    let mut m = Mapped::new();
    let mut ingress = responses::request::Ingress::new(
        &mut m.writer,
        &m.store,
        identity(Direction::Request),
        Operation::Generate,
        1 << 20,
    )
    .unwrap();
    ingress.advance(&mut m.writer, INPUT).unwrap();
    let ready = ingress.finish(&mut m.writer, &Inspect).unwrap();
    let doc = ready.view().document;
    let x = doc.required(doc.root(), "x_ck").unwrap();
    let providers = doc
        .required(x, "provider_parameters")
        .expect("provider-specific options");
    let codex = doc.required(providers, "codex").expect("Codex isolation");
    let responses = doc.required(providers, "responses").unwrap();
    let cache = doc.required(responses, "prompt_cache_options").unwrap();
    assert!(doc.string_is(doc.required(cache, "ttl").unwrap(), "24h").unwrap());
    let stream = doc.required(codex, "stream_options").unwrap();
    assert!(
        doc.string_is(
            doc.required(stream, "reasoning_summary_delivery").unwrap(),
            "sequential_cutoff"
        )
        .unwrap()
    );
    assert!(doc.member(stream, "include_obfuscation").unwrap().is_none());
}

#[test]
fn public_cache_and_stream_options_remain_native() {
    let (output, _) = request(
        &json!({"model":"m","input":"hi","prompt_cache_options":{"ttl":"12h"},"stream_options":{"include_obfuscation":false}}),
        Operation::Generate,
    );
    assert_eq!(output["stream_options"], json!({"include_obfuscation":false}));
    assert_eq!(output["prompt_cache_options"], json!({"ttl":"12h"}));
}

#[test]
fn codex_restores_its_options_without_losing_public_stream_options() {
    let mut m = Mapped::new();
    let mut ingress = responses::request::Ingress::new(
        &mut m.writer,
        &m.store,
        identity(Direction::Request),
        Operation::Generate,
        1 << 20,
    )
    .unwrap();
    ingress.advance(&mut m.writer, INPUT).unwrap();
    let ready = ingress
        .finish(&mut m.writer, &responses::request::CodexCapability)
        .unwrap();
    let output = responses::request::Egress {
        destination: "codex",
        upstream_stream: true,
    }
    .encode(&ready, &mut m.writer)
    .unwrap();
    let output: Value = serde_json::from_slice(&bytes(&output)).unwrap();
    assert_eq!(output["prompt_cache_options"], json!({"ttl":"24h"}));
    assert_eq!(
        output["stream_options"],
        json!({"reasoning_summary_delivery":"sequential_cutoff","include_obfuscation":false})
    );
}
