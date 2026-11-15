//! The canonical reader: the one-pass contiguous JSON reader must build exactly the tree the
//! scanner builds or decline, records that straddle a frame boundary decode like any other,
//! and an egress binding checks identity once and `seq` on every record.

#[path = "../src/codec/mod.rs"]
pub mod codec;
#[allow(dead_code, reason = "these tests use only the mapped store and writer")]
#[path = "semantic/mod.rs"]
mod semantic;

use codec::{
    Error,
    document::Document,
    event::{Direction, Emitter, Identity, Kind, LocalId, Scope},
    internal::{self, Binding},
    source::Source,
};
use semantic::Mapped;
use std::task::Poll;

fn identity(request: &str) -> Identity {
    Identity {
        request_id: LocalId::new(request).unwrap(),
        stream_id: LocalId::new("stream").unwrap(),
        attempt: 1,
        direction: Direction::Response,
    }
}

/// Every node of `doc`, with its private links, names and coordinates.
fn tree(doc: &Document) -> Vec<String> {
    (0..)
        .map_while(|index| doc.node(index).ok())
        .map(|node| format!("{node:?}"))
        .collect()
}

/// The tree or the error of `bytes`, read by `parse` from a source over a copy of them.
fn outcome(
    mapped: &mut Mapped,
    bytes: &[u8],
    max: u64,
    parse: fn(&mut ck_stream::Writer, Source, u64) -> codec::Result<Document>,
) -> Result<Vec<String>, String> {
    let mut source = Source::new(&mut mapped.writer).unwrap();
    if !bytes.is_empty() {
        source.push_bytes(&mut mapped.writer, bytes).unwrap();
    }
    parse(&mut mapped.writer, source, max)
        .map(|doc| tree(&doc))
        .map_err(|error| format!("{error:?}"))
}

fn same_as_scanner(mapped: &mut Mapped, bytes: &[u8]) {
    for max in [bytes.len() as u64, (bytes.len() as u64).saturating_sub(1)] {
        assert_eq!(
            outcome(mapped, bytes, max, Document::parse_internal),
            outcome(mapped, bytes, max, Document::parse),
            "{:?} max {max}",
            String::from_utf8_lossy(bytes)
        );
    }
}

fn members(count: usize) -> String {
    let members: Vec<_> = (0..count).map(|i| format!("\"m{i}\":{i}")).collect();
    format!("{{{}}}", members.join(","))
}

fn nested(depth: usize) -> String {
    "[".repeat(depth) + &"]".repeat(depth)
}

const RECORD: &str = r#"{"x_ck":{"v":1,"request_id":"r","stream_id":"s","attempt":0,"seq":3,"direction":"response","choice_index":0,"original":[{"protocol":"gemini","pointer":"/a~1b","value":null}]},"choices":[{"index":0,"delta":{"content":"h\u00e9\n\"x\\ \ud83d\ude80 \/","role":"assistant"},"finish_reason":null,"logprobs":[]}],"usage":{"a":1.5e-3,"b":-0,"c":true,"d":false,"e":[],"f":{},"g":[1,[2,{"h":[]}]],"i":0.0E+1}}"#;

#[test]
fn contiguous_reader_builds_the_scanner_tree_or_declines() {
    let mut mapped = Mapped::new();
    for case in &cases() {
        same_as_scanner(&mut mapped, case);
    }
}

/// Valid and invalid JSON at every rule the scanner enforces.
fn cases() -> Vec<Vec<u8>> {
    let mut cases: Vec<Vec<u8>> = [
        RECORD,
        " \t\r\n{} \n",
        "[]",
        "\"root\"",
        "-12.5e+7",
        "true",
        "null",
        "{\"a\":1,\"b\":{\"a\":2}}",
        "{\"a\":1,\"a\":2}",
        "{\"a\":1,\"\\u0061\":2}",
        "{\"\\n\":1}",
        "{\"é\":1,\"\u{1F680}\":[\"\u{7f}\"]}",
        "",
        "  ",
        "{",
        "{\"a\"}",
        "{\"a\":}",
        "{\"a\" 1}",
        "{\"a\":1,}",
        "[1,]",
        "[,1]",
        "{,}",
        "[1}",
        "{\"a\":1]",
        "{} {}",
        "1 2",
        "01",
        "-",
        "1.",
        ".5",
        "1e",
        "1e+",
        "+1",
        "tru",
        "truex",
        "nul",
        "\"\\x\"",
        "\"\\u12\"",
        "\"\\u12g4\"",
        "\"\\ud800\"",
        "\"\\udc00\"",
        "\"\\ud800\\u0041\"",
        "\"\\ud800x\"",
        "\"\\uD83D\\uDE80\"",
        "\"a\u{1}b\"",
        "\"tab\tin\"",
        "\"unterminated",
        "\"\\",
        "1\"",
        "[1:2]",
        "{\"a\"::1}",
    ]
    .iter()
    .map(|case| case.as_bytes().to_vec())
    .collect();
    cases.extend([
        b"\"\xff\"".to_vec(),
        b"\"\xc3\"".to_vec(),
        b"\"\xed\xa0\x80\"".to_vec(),
        b"[1,\xc3\xa9]".to_vec(),
    ]);
    for count in [63, 64, 65, 100] {
        cases.push(members(count).into_bytes());
    }
    for depth in [127, 128, 129] {
        cases.push(nested(depth).into_bytes());
    }
    cases
}

/// Deterministic mutations of a record: replaced, inserted and removed bytes.
#[test]
fn mutated_records_read_like_the_scanner() {
    let mut mapped = Mapped::new();
    let alphabet = b"{}[]:,\"\\ \nu0aeE+-.tfn\x01\xc3\xa9";
    let mut state = 0x2545_f491_4f6c_dd1du64;
    let mut next = |bound: usize| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        (state % bound as u64) as usize
    };
    for _ in 0..20_000 {
        let mut bytes = RECORD.as_bytes().to_vec();
        for _ in 0..1 + next(3) {
            let at = next(bytes.len());
            let byte = alphabet[next(alphabet.len())];
            match next(3) {
                0 => bytes[at] = byte,
                1 => bytes.insert(at, byte),
                _ => {
                    bytes.remove(at);
                }
            }
        }
        same_as_scanner(&mut mapped, &bytes);
    }
}

/// Publishes a response start padded to `start_len` bytes, then an opaque record, and returns
/// both leases.
fn straddle(mapped: &mut Mapped, start_len: usize) -> (ck_stream::Lease, ck_stream::Lease) {
    let mut emitter = Emitter::new(identity("request"));
    let base = codec::encode::measure(|e| {
        e.bytes(b"event: x_ck.response.start\nid: 0\ndata: {\"x_ck\":{\"v\":1,\"request_id\":\"request\",\"stream_id\":\"stream\",\"attempt\":1,\"seq\":0,\"direction\":\"response\"},\"pad\":\"\"}\n\n")
    })
    .unwrap();
    let pad = "a".repeat(start_len - base);
    let start = emitter
        .emit(
            &mut mapped.writer,
            Kind::ResponseStart,
            Scope::default(),
            |e| {
                e.bytes(b",\"pad\":")?;
                e.string(&pad)
            },
            |_| Ok(()),
        )
        .unwrap();
    assert_eq!(start.len(), start_len as u64);
    emitter
        .emit(
            &mut mapped.writer,
            Kind::Opaque,
            Scope::default(),
            |e| e.bytes(br#","name":"t","value":{"text":"h\u00e9\n\"x\\","n":[1,2.5,true,null],"o":{}}"#),
            |_| Ok(()),
        )
        .unwrap();
    let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
    let mut lease = || match reader.try_next() {
        Poll::Ready(Some(ck_stream::Read::Record(lease))) => lease,
        _ => panic!("missing record"),
    };
    (lease(), lease())
}

/// The data JSON of a record.
fn data(lease: &ck_stream::Lease) -> Vec<u8> {
    let mut bytes = Vec::new();
    lease.visit(|chunk| bytes.extend_from_slice(chunk)).unwrap();
    let start = bytes.windows(6).position(|w| w == b"data: ").unwrap() + 6;
    bytes[start..bytes.len() - 2].to_vec()
}

#[test]
fn records_straddling_a_frame_boundary_decode_like_contiguous_ones() {
    let frame = ck_stream::FRAME_BYTES;
    let mut straddled = 0;
    for before in 1..240 {
        let mut mapped = Mapped::configured(std::sync::Arc::new(ck_stream::ManualClock::default()), 16);
        let (start, record) = straddle(&mut mapped, frame - before);
        let r = record.record();
        straddled += usize::from(r.start.frame != r.end.frame && r.end.offset != 0);
        let expected = data(&record);
        let reference = outcome(&mut mapped, &expected, expected.len() as u64, Document::parse).unwrap();
        let mut binding = Binding::new(&mapped.writer, Some(identity("request")));
        binding.decode(&mut mapped.writer, start).unwrap();
        let (kind, doc) = binding.decode(&mut mapped.writer, record).unwrap();
        assert_eq!(kind, Kind::Opaque);
        assert_eq!(
            tree(&doc),
            reference,
            "record starts {before} bytes before the frame end"
        );
    }
    assert!(straddled > 200, "{straddled}");
}

#[test]
fn binding_checks_identity_once_and_seq_on_every_record() {
    let records = |mapped: &mut Mapped| straddle(mapped, 200);
    // The first record establishes the identity the egress was built with.
    let mut mapped = Mapped::new();
    let (start, _) = records(&mut mapped);
    let mut binding = Binding::new(&mapped.writer, Some(identity("other")));
    assert!(matches!(binding.decode(&mut mapped.writer, start), Err(Error::Schema)));
    // Without an expected identity, the first record still binds the stream.
    let mut mapped = Mapped::new();
    let (start, record) = records(&mut mapped);
    let mut binding = Binding::new(&mapped.writer, None);
    binding.decode(&mut mapped.writer, start).unwrap();
    assert_eq!(binding.decode(&mut mapped.writer, record).unwrap().0, Kind::Opaque);
    // A record out of sequence is refused.
    let mut mapped = Mapped::new();
    let (_, record) = records(&mut mapped);
    let mut binding = Binding::new(&mapped.writer, Some(identity("request")));
    assert!(matches!(binding.decode(&mut mapped.writer, record), Err(Error::Schema)));
    // A request stream never binds a response egress, even one without an expected identity.
    let mut mapped = Mapped::new();
    let mut emitter = Emitter::new(Identity {
        direction: Direction::Request,
        ..identity("request")
    });
    emitter
        .emit(
            &mut mapped.writer,
            Kind::Error,
            Scope::default(),
            |_| Ok(()),
            |_| Ok(()),
        )
        .unwrap();
    let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
    let Poll::Ready(Some(ck_stream::Read::Record(error))) = reader.try_next() else {
        panic!("missing record");
    };
    let mut binding = Binding::new(&mapped.writer, None);
    assert!(matches!(binding.decode(&mut mapped.writer, error), Err(Error::Schema)));
    // Plain decoding, used by ingress replay, keeps no binding.
    let mut mapped = Mapped::new();
    let (_, record) = records(&mut mapped);
    assert_eq!(internal::decode(&mut mapped.writer, record).unwrap().0, Kind::Opaque);
}
