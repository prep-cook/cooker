//! Names compare by their decoded text whatever the stored spelling, the source layout or a
//! hash collision.

use ck_stream::{FRAME_BYTES, Store, Writer};

use super::{Document, NameHash, Source, Value};
use crate::codec::{Error, encode::scratch};

struct Fixture {
    store: Store,
    writer: Writer,
    directory: std::path::PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!("ck-name-{}", uuid::Uuid::new_v4()));
        let mut config = ck_stream::Config::new(directory.clone());
        config.pool_bytes = Some(8 * FRAME_BYTES);
        config.max_event_bytes = FRAME_BYTES;
        config.header_slots = 4;
        config.workers = 1;
        let store =
            ck_stream::Store::with_clock(config, std::sync::Arc::new(ck_stream::ManualClock::default())).unwrap();
        let writer = store.create().unwrap();
        Self {
            store,
            writer,
            directory,
        }
    }

    fn parse(&mut self, body: &[u8]) -> Document {
        let mut source = Source::new(&mut self.writer).unwrap();
        source.push_bytes(&mut self.writer, body).unwrap();
        Document::parse(&mut self.writer, source, body.len() as u64).unwrap()
    }

    fn finish(mut self, doc: Document) {
        drop((doc, self.writer));
        self.store.shutdown().unwrap();
        std::fs::remove_dir_all(&self.directory).unwrap();
    }
}

/// Names of one length that share their first and last four bytes have the same [`NameHash`].
const COLLIDING: (&str, &str) = ("head_one_tail", "head_two_tail");

#[test]
fn mapped_names_compare_decoded_text() {
    assert_eq!(NameHash::of(COLLIDING.0), NameHash::of(COLLIDING.1));
    let mut fixture = Fixture::new();
    let doc = fixture.parse(br#"{"plain":0,"\u0065sc":1,"\u00e9":2,"a\"b":3,"head_one_tail":4,"\/":5}"#);
    let root = doc.root();
    let found = |name: &str| doc.member(root, name).unwrap().map(|child| child - root);
    for (index, name) in ["plain", "esc", "é", "a\"b", COLLIDING.0, "/"].into_iter().enumerate() {
        assert_eq!(found(name), Some(index + 1), "{name}");
    }
    for name in ["plai", "plainx", "\\u0065sc", "e", "a\\\"b", COLLIDING.1, "\\/"] {
        assert_eq!(found(name), None, "{name}");
    }
    // The escaped spelling of a colliding name fails verification too.
    let other = fixture.parse(br#"{"\u0068ead_one_tail":0}"#);
    let escaped = other.node(other.root() + 1).unwrap().name.unwrap();
    assert!(other.name_is(escaped, COLLIDING.0).unwrap());
    assert!(!other.name_is(escaped, COLLIDING.1).unwrap());
    drop(other);
    fixture.finish(doc);
}

/// A key that crosses a frame boundary is compared through the source, not one slice.
#[test]
fn names_split_across_frames_compare_whole() {
    for key in [r#""straddle""#, r#""str\u0061ddle""#] {
        let mut fixture = Fixture::new();
        let prefix = r#"{"pad":""#.len() + r#"","#.len();
        let pad = "x".repeat(FRAME_BYTES - prefix - 4);
        let body = format!(r#"{{"pad":"{pad}",{key}:1}}"#);
        let doc = fixture.parse(body.as_bytes());
        let straddle = doc.member(doc.root(), "straddle").unwrap().unwrap();
        let super::Name::Mapped(text, _) = doc.node(straddle).unwrap().name.unwrap() else {
            panic!("{key} is not a mapped name");
        };
        assert_eq!(doc.source(text).unwrap().slice(text.span).unwrap(), None);
        assert_eq!(doc.member(doc.root(), "straddlf").unwrap(), None);
        assert_eq!(doc.member(doc.root(), "straddl").unwrap(), None);
        fixture.finish(doc);
    }
}

/// A pointer name is its last segment: split at a decoded `/`, then `~1` and `~0` decoded.
#[test]
fn pointer_names_are_their_decoded_last_segment() {
    let mut fixture = Fixture::new();
    let cases = [
        (r#""/x/a~1b~0""#, "a/b~", "a~1b~0"),
        (r#""/x/a\/b""#, "b", "b"),
        (r#""/x/\u0062c""#, "bc", "bc"),
        (r#""abc""#, "abc", "abc"),
        (r#""/x/""#, "", ""),
        (r#""/a~2/plain""#, "plain", "plain"),
        (r#""/x/head_one_tail""#, COLLIDING.0, COLLIDING.0),
    ];
    let body = format!(
        "[{},\"/x/a~2\",\"/x/a~\"]",
        cases.iter().map(|case| case.0).collect::<Vec<_>>().join(",")
    );
    let doc = fixture.parse(body.as_bytes());
    let texts: Vec<_> = doc
        .children(doc.root())
        .unwrap()
        .map(|child| match doc.node(child).unwrap().value {
            Value::String(text) => text,
            _ => unreachable!(),
        })
        .collect();
    for ((stored, decoded, segment), text) in cases.into_iter().zip(&texts) {
        let name = doc.pointer_name(*text).unwrap();
        assert!(doc.name_is(name, decoded).unwrap(), "{stored}");
        assert!(!doc.name_is(name, &format!("{decoded}x")).unwrap(), "{stored}");
        assert!(!doc.name_is(name, COLLIDING.1).unwrap(), "{stored}");
        let mapped = doc.mapped_name(*text).unwrap();
        assert_eq!(
            doc.names_equal(name, mapped).unwrap(),
            decoded == &stored[1..stored.len() - 1]
        );
        let written = scratch(&mut fixture.writer, |e| doc.write_name(name, e)).unwrap();
        assert_eq!(
            written.chunk(0).unwrap(),
            serde_json::to_vec(decoded).unwrap(),
            "{stored}"
        );
        let copied = scratch(&mut fixture.writer, |e| {
            e.bytes(b"<")?;
            crate::codec::provenance::write_pointer_segment(&doc, name, e)?;
            e.bytes(b">")
        })
        .unwrap();
        assert_eq!(copied.chunk(0).unwrap(), format!("<{segment}>").as_bytes(), "{stored}");
    }
    for text in &texts[cases.len()..] {
        assert!(matches!(doc.pointer_name(*text), Err(Error::Schema)));
    }
    fixture.finish(doc);
}

/// A stored name's digest, built one scalar at a time, matches the hash of the same lookup
/// name at every length around the four-byte head and tail.
#[test]
fn streamed_and_whole_hashes_agree() {
    let text = "abcdefghij\u{e9}\u{1f600}";
    for (end, _) in text.char_indices().chain([(text.len(), ' ')]) {
        let name = &text[..end];
        let mut hash = NameHash::default();
        name.chars().for_each(|c| hash.push(c));
        assert!(hash.finish(name.len() as u64).hash_is(NameHash::of(name)), "{name}");
    }
}
