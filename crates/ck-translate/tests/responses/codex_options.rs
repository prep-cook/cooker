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

/// Settled 62: reasoning only Antigravity can read (a Claude signature, CLIProxyAPI's Gemini
/// carriers, its compaction capsule) reaches only the Antigravity destination; every other
/// Responses destination receives the request without that reasoning item. Other encrypted
/// reasoning stays where it was.
#[test]
fn reasoning_reaches_only_a_destination_that_can_read_it() {
    const SINGLE: &str = "EhkKFwgMEAIyEWNsYXVkZS1zb25uZXQtNC02GAE=";
    const DOUBLE: &str = "RWhrS0Z3Z01FQUl5RVdOc1lYVmtaUzF6YjI1dVpYUXROQzAyR0FFPQ==";
    const OPENAI: &str = "gAAAAABpAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    let prefixed = format!("claude#{SINGLE}");
    let carrier = format!("cpa-gemini-responses-carrier-v1:standalone:text:{}", "UlhGelEy");
    let capsule = "cpa-ag-compact-v1:AAAA".to_owned();
    let antigravity_only = [SINGLE.to_owned(), DOUBLE.to_owned(), prefixed, carrier, capsule];
    let rows = antigravity_only
        .iter()
        .map(|v| (v.as_str(), false))
        .chain([(OPENAI, true), ("opaque", true)]);
    for (encrypted, everywhere) in rows {
        let input = json!({"model": "m", "input": [
            {"role": "user", "content": "q"},
            {"type": "reasoning", "summary": [{"type": "summary_text", "text": "reason"}], "encrypted_content": encrypted},
            {"role": "assistant", "content": [{"type": "output_text", "text": "ok"}]},
            {"role": "user", "content": "continue"}
        ]});
        for destination in ["responses", "codex", "antigravity"] {
            let mut m = Mapped::new();
            let mut ingress = responses::request::Ingress::new(
                &mut m.writer,
                &m.store,
                identity(Direction::Request),
                Operation::Generate,
                1 << 20,
            )
            .unwrap();
            ingress.advance(&mut m.writer, input.to_string().as_bytes()).unwrap();
            let ready = match destination {
                "responses" => ingress.finish(&mut m.writer, &responses::request::ResponsesCapability),
                "codex" => ingress.finish(&mut m.writer, &responses::request::CodexCapability),
                _ => ingress.finish(&mut m.writer, &responses::request::AntigravityCapability),
            }
            .unwrap();
            let output = responses::request::Egress {
                destination,
                upstream_stream: true,
            }
            .encode(&ready, &mut m.writer)
            .unwrap();
            let output: Value = serde_json::from_slice(&bytes(&output)).unwrap();
            let reasoning: Vec<&Value> = output["input"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|i| i["type"] == "reasoning")
                .collect();
            let kept = everywhere || destination == "antigravity";
            let want: Vec<Value> = if kept { vec![json!(encrypted)] } else { vec![] };
            let got: Vec<Value> = reasoning.iter().map(|r| r["encrypted_content"].clone()).collect();
            assert_eq!(got, want, "{destination}: {encrypted}: {output}");
            let texts: Vec<&Value> = output["input"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|i| i["role"] == "user")
                .collect();
            assert_eq!(texts.len(), 2, "{destination}: the conversation stays: {output}");
        }
    }
}
