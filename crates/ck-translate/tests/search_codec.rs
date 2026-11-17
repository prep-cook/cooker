#[allow(dead_code, reason = "these tests use only the mapped store and writer")]
mod semantic;

use ck_stream::Read;
use ck_translate::{
    codec::{
        Error, ResponseEgress, ResponseIngress,
        document::{Document, Value},
        encode,
        event::{Direction, Identity, LocalId},
        provenance,
        source::Source,
    },
    search::{self, Decoder, Phase},
};
use semantic::Mapped;
use serde_json::json;

fn bytes(lease: &ck_stream::ScratchLease) -> Vec<u8> {
    let mut bytes = Vec::new();
    while bytes.len() < lease.len() {
        bytes.extend_from_slice(lease.chunk(bytes.len()).unwrap());
    }
    bytes
}

fn decode(mapped: &mut Mapped, input: &[u8], phase: Phase, chunk: usize) -> Document {
    let mut decoder = Decoder::new(&mut mapped.writer, phase, input.len() as u64).unwrap();
    for bytes in input.chunks(chunk) {
        decoder.advance(&mut mapped.writer, bytes).unwrap();
    }
    decoder.finish(&mut mapped.writer).unwrap()
}

fn identity() -> Identity {
    Identity {
        request_id: LocalId::new("search-request").unwrap(),
        stream_id: LocalId::new("search-stream").unwrap(),
        attempt: 1,
        direction: Direction::Response,
    }
}

/// `body` as the upstream's answer through the search ingress and the client's egress: what the
/// client got and how the egress ended.
fn through_stream(body: &[u8], chunk: usize) -> (Vec<u8>, Result<(), Error>) {
    let mut mapped = Mapped::new();
    let mut ingress = search::Ingress::new(&mut mapped.writer, identity(), body.len().max(1) as u64).unwrap();
    let _ = body
        .chunks(chunk)
        .try_for_each(|bytes| ingress.advance(&mut mapped.writer, bytes))
        .and_then(|()| ingress.finish(&mut mapped.writer));
    let mut egress = search::ResultEgress::new(&mapped.writer, Some(identity()));
    let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
    let mut client = Vec::new();
    let mut result = Ok(());
    while result.is_ok()
        && let std::task::Poll::Ready(Some(Read::Record(lease))) = reader.try_next()
    {
        result = egress.advance(&mut mapped.writer, lease, |out| {
            client.extend(bytes(&out));
            Ok(())
        });
    }
    (client, result.and_then(|()| egress.finish()))
}

#[test]
fn search_round_trip_preserves_native_fields_through_canonical_storage() {
    let fixtures: serde_json::Value = serde_json::from_str(include_str!("fixtures/alpha_search.json")).unwrap();
    for fixture in fixtures.as_array().unwrap() {
        let phase = if fixture["phase"] == "request" {
            Phase::Request
        } else {
            Phase::Result
        };
        let wire = serde_json::to_vec_pretty(&fixture["wire"]).unwrap();
        for chunk in [1, 7, wire.len()] {
            let mut mapped = Mapped::new();
            let operation = decode(&mut mapped, &wire, phase, chunk);
            let stored = encode::scratch(&mut mapped.writer, |e| operation.write(operation.root(), e)).unwrap();
            let canonical: serde_json::Value = serde_json::from_slice(&bytes(&stored)).unwrap();
            assert_eq!(canonical["kind"], "alpha_search");
            assert_eq!(canonical["value"], fixture["value"]);
            assert_eq!(canonical["x_ck"]["v"], 1);
            assert_eq!(canonical["x_ck"]["original_protocol"], "codex");
            drop(operation);
            let mut source = Source::new(&mut mapped.writer).unwrap();
            source.push_scratch(stored).unwrap();
            let len = source.len();
            let mut restored = Document::parse(&mut mapped.writer, source, len).unwrap();
            provenance::restore_bindings(&mut restored).unwrap();
            let output = bytes(&search::encode(&mut mapped.writer, &restored).unwrap());
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&output).unwrap(),
                fixture["wire"]
            );
            // An upstream result reaches the client as the body the upstream sent.
            if phase == Phase::Result {
                let (client, result) = through_stream(&wire, chunk);
                result.unwrap();
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&client).unwrap(),
                    fixture["wire"]
                );
            }
        }
    }
}

#[test]
fn search_rejects_invalid_or_incomplete_documents() {
    let cases: &[(&[u8], Phase)] = &[
        (br#"{}"#, Phase::Request),
        (br#"{"query":null}"#, Phase::Request),
        (br#"{"query":"q","limit":-1}"#, Phase::Request),
        (br#"{"query":"q","limit":1.5}"#, Phase::Request),
        (br#"{"query":"q","limit":18446744073709551616}"#, Phase::Request),
        (br#"{"query":"q","qu\u0065ry":"hidden"}"#, Phase::Request),
        (br#"{"query":"q"}{}"#, Phase::Request),
        (br#"{"query":"unfinished"#, Phase::Request),
        (b"{\"query\":\"\xff\"}", Phase::Request),
        (br#"{}"#, Phase::Result),
        (br#"{"results":null}"#, Phase::Result),
        (br#"{"results":[null]}"#, Phase::Result),
        (br#"{"results":[{"title":false}]}"#, Phase::Result),
        (br#"{"results":[{"uri":1}]}"#, Phase::Result),
        (br#"{"results":[{"text":[]}]}"#, Phase::Result),
        (br#"{"results":[{}]"#, Phase::Result),
    ];
    for &(input, phase) in cases {
        for chunk in [1, input.len()] {
            let mut mapped = Mapped::new();
            let mut decoder = Decoder::new(&mut mapped.writer, phase, input.len() as u64).unwrap();
            let result = input
                .chunks(chunk)
                .try_for_each(|bytes| decoder.advance(&mut mapped.writer, bytes));
            if result.is_err() {
                assert!(decoder.advance(&mut mapped.writer, b" ").is_err());
            } else {
                assert!(decoder.finish(&mut mapped.writer).is_err(), "accepted {input:?}");
            }
            // An upstream answer that is not a whole result gives the client nothing: the egress
            // reads the upstream failure.
            if phase == Phase::Result {
                let (client, result) = through_stream(input, chunk);
                assert!(client.is_empty(), "{input:?}");
                assert!(matches!(result, Err(Error::Truncated)), "{input:?}: {result:?}");
            }
        }
    }
}

#[test]
fn search_edits_use_current_values_and_keep_extras_with_their_result() {
    let cases = [
        (
            Phase::Request,
            json!({"query":"before","limit":5,"future":null}),
            json!({"query":"after","future":null}),
        ),
        (
            Phase::Result,
            json!({"results":[{"title":"removed","rank":1},{"title":"before","rank":2}],"next":null}),
            json!({"results":[{"title":"after","rank":2}],"next":null}),
        ),
    ];
    for (phase, input, expected) in cases {
        let mut mapped = Mapped::new();
        let mut operation = decode(&mut mapped, &serde_json::to_vec(&input).unwrap(), phase, 1);
        let value = operation.required(operation.root(), "value").unwrap();
        match phase {
            Phase::Request => {
                operation.set(value, "query", Value::Text("after")).unwrap();
                operation.remove(value, "limit").unwrap();
            }
            Phase::Result => {
                let results = operation.required(value, "results").unwrap();
                let first = operation.children(results).unwrap().next().unwrap();
                operation.detach(results, first).unwrap();
                let remaining = operation.children(results).unwrap().next().unwrap();
                operation.set(remaining, "title", Value::Text("after")).unwrap();
            }
        }
        let output = bytes(&search::encode(&mut mapped.writer, &operation).unwrap());
        assert_eq!(serde_json::from_slice::<serde_json::Value>(&output).unwrap(), expected);
    }
}

#[test]
fn search_body_limit_refuses_extra_bytes_without_recovery() {
    for phase in [Phase::Request, Phase::Result] {
        let mut mapped = Mapped::new();
        let mut decoder = Decoder::new(&mut mapped.writer, phase, 1).unwrap();
        decoder.advance(&mut mapped.writer, b"{").unwrap();
        assert!(matches!(
            decoder.advance(&mut mapped.writer, b"}"),
            Err(ck_translate::codec::Error::EventTooLarge)
        ));
        assert!(matches!(
            decoder.advance(&mut mapped.writer, b""),
            Err(ck_translate::codec::Error::Closed)
        ));
        assert!(decoder.finish(&mut mapped.writer).is_err());
    }
}
