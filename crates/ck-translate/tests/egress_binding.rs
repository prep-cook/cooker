//! Every response egress reads its canonical stream through a binding: the first record must
//! carry the identity the egress was built with, and every record the next `seq`.

// Test-only inclusion until the framework owner declares the production modules.
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
#[allow(dead_code, reason = "these tests use only the mapped store and writer")]
#[path = "semantic/mod.rs"]
mod semantic;

use codec::{
    BodyMode, Error, Operation, ResponseEgress, ResponseIngress,
    event::{Direction, Identity, Kind, LocalId},
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

#[derive(Clone, Copy, Debug)]
enum Client {
    Chat,
    Anthropic,
    Responses,
    Gemini,
}

/// Translates a streamed Chat response into canonical records and returns their leases.
fn records(m: &mut Mapped) -> Vec<ck_stream::Lease> {
    open_records(m, true)
}

/// [`records`]; without `finish`, the stream stops after the content deltas, still open.
fn open_records(m: &mut Mapped, finish: bool) -> Vec<ck_stream::Lease> {
    let chunk = |delta: &str, finish: &str| {
        format!(
            "data: {{\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{{\"index\":0,\"delta\":{delta},\"finish_reason\":{finish}}}]}}\n\n"
        )
    };
    let body = chunk(r#"{"role":"assistant","content":"a"}"#, "null")
        + &chunk(r#"{"content":"b"}"#, "null")
        + &chunk(r#"{"content":"c"}"#, "null")
        + &if finish {
            chunk("{}", "\"stop\"") + "data: [DONE]\n\n"
        } else {
            String::new()
        };
    let mut ingress = codec::chat::response::Ingress::new(
        &mut m.writer,
        &m.store,
        identity("request"),
        Operation::Generate,
        BodyMode::Streaming,
        1 << 20,
    )
    .unwrap();
    ingress.advance(&mut m.writer, body.as_bytes()).unwrap();
    if finish {
        ingress.finish(&mut m.writer).unwrap();
    }
    let mut reader = m.store.reader(m.writer.id(), 0).unwrap();
    let mut leases = Vec::new();
    while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
        leases.push(lease);
    }
    leases
}

/// Feeds `leases` to a new `client` egress built with `identity`; returns the first error.
fn feed(m: &mut Mapped, client: Client, identity: Identity, leases: Vec<ck_stream::Lease>) -> Option<Error> {
    fn run(
        egress: &mut impl ResponseEgress,
        w: &mut ck_stream::Writer,
        leases: Vec<ck_stream::Lease>,
    ) -> Option<Error> {
        leases
            .into_iter()
            .find_map(|lease| egress.advance(w, lease, |_| Ok(())).err())
    }
    let (w, mode, op) = (&mut m.writer, BodyMode::Streaming, Operation::Generate);
    match client {
        Client::Chat => run(
            &mut codec::chat::egress::Egress::new(w, identity, op, mode, true, |_| {}).unwrap(),
            w,
            leases,
        ),
        Client::Anthropic => run(
            &mut anthropic::egress::Egress::new(w, identity, op, mode, |_| {}).unwrap(),
            w,
            leases,
        ),
        Client::Responses => run(
            &mut responses::egress::Egress::new(w, op, mode, |_, _| {}).unwrap(),
            w,
            leases,
        ),
        Client::Gemini => run(
            &mut gemini::egress::Egress::new(w, identity, op, mode, |_| {}).unwrap(),
            w,
            leases,
        ),
    }
}

#[test]
fn every_egress_reads_the_bound_stream_in_sequence() {
    for client in [Client::Chat, Client::Anthropic, Client::Responses, Client::Gemini] {
        let mut m = Mapped::new();
        let leases = records(&mut m);
        assert!(leases.len() > 4);
        assert!(
            feed(&mut m, client, identity("request"), leases).is_none(),
            "{client:?}"
        );
        // A record missing from the middle of the stream stops the egress at the next one.
        let mut m = Mapped::new();
        let mut leases = records(&mut m);
        leases.remove(3);
        assert!(
            matches!(feed(&mut m, client, identity("request"), leases), Some(Error::Schema)),
            "{client:?}"
        );
    }
}

#[test]
fn egresses_built_with_an_identity_refuse_a_stream_of_another() {
    for client in [Client::Chat, Client::Anthropic, Client::Gemini] {
        let mut m = Mapped::new();
        let leases = records(&mut m);
        assert!(
            matches!(feed(&mut m, client, identity("other"), leases), Some(Error::Schema)),
            "{client:?}"
        );
    }
}

/// A record any holder of the writer appends with another stream's identity, after valid ones
/// from the ingress emitter, never reaches a client: every record's identity is checked.
#[test]
fn every_egress_refuses_a_record_with_another_identity() {
    for client in [Client::Chat, Client::Anthropic, Client::Responses, Client::Gemini] {
        let mut m = Mapped::new();
        let count = open_records(&mut m, false).len();
        let forged = format!(
            "id: {count}\ndata: {{\"x_ck\":{{\"v\":1,\"request_id\":\"OTHER\",\"stream_id\":\"OTHER\",\"attempt\":99,\"seq\":{count},\"direction\":\"request\"}},\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{{\"index\":0,\"delta\":{{\"content\":\"INJECTED\"}},\"finish_reason\":null}}]}}\n\n"
        );
        codec::encode::publish(&mut m.writer, Kind::Chunk as u32, 0, |e| e.bytes(forged.as_bytes())).unwrap();
        let mut reader = m.store.reader(m.writer.id(), 0).unwrap();
        let mut leases = Vec::new();
        while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
            leases.push(lease);
        }
        assert_eq!(leases.len(), count + 1);
        assert!(
            matches!(feed(&mut m, client, identity("request"), leases), Some(Error::Schema)),
            "{client:?}"
        );
    }
}
