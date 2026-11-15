//! The one-pass reader behind [`Document::parse_internal`] for JSON held in one contiguous
//! slice. It accepts only input that [`json::Scanner`](crate::codec::json::Scanner) accepts
//! and builds the nodes `Document::import` would build from it. It declines everything else,
//! including valid forms it leaves to the scanner (escaped member names, objects with many
//! members), so the scanner returns the exact error or tree.

use super::{Document, Name, NameHash, Node, Text, Value};
use crate::codec::{Result, source::Span, table::charge};

/// Containers open at once; the scanner refuses deeper input.
const DEPTH: usize = 128;
/// Members of one object checked for a duplicate by a linear scan. A larger object goes to the
/// scanner's keyed index, so crafted names cannot make the check quadratic.
const MEMBERS: usize = 64;
/// Member names of all open objects that the counting pass holds for its duplicate check.
const NAMES: usize = 256;

impl Document {
    /// Adds the nodes of `bytes`, the whole of source 0, to this empty document. Returns false,
    /// leaving no nodes and no charge, when the scanner must read `bytes` instead.
    pub(super) fn read_contiguous(&mut self, bytes: &[u8]) -> Result<bool> {
        if std::str::from_utf8(bytes).is_err() || u32::try_from(bytes.len()).is_err() {
            return Ok(false);
        }
        // The first pass validates and counts without storing nodes, so the document charges
        // only for input it reads, and exactly the nodes the scanner path would charge after
        // the scanner's own tables; it never needs more quota than that path.
        let mut count = Count::new();
        if (Reader {
            sink: &mut count,
            bytes,
            at: 0,
        })
        .run()
        .is_none()
        {
            return Ok(false);
        }
        self.reserve(count.values, 1, 0)?;
        let mut build = Build { nodes: &mut self.nodes };
        if (Reader {
            sink: &mut build,
            bytes,
            at: 0,
        })
        .run()
        .is_none()
        {
            // Unreachable after a successful count; release the charge before the scanner runs.
            (self.nodes, self.sources) = (Vec::new(), Vec::new());
            charge(&mut self.metadata, 0)?;
            return Ok(false);
        }
        Ok(true)
    }
}

/// Where the reader puts the values it reads. A handle names an open container.
trait Sink {
    /// Adds a value as the last child of `parent`; `None` declines the input.
    fn push(&mut self, parent: Option<usize>, name: Option<Name>, value: Value) -> Option<usize>;
    /// The container that holds open container `node`.
    fn parent(&self, node: usize) -> Option<usize>;
    fn object(&self, node: usize) -> bool;
    /// Accepts member name `stored` of open `object`; `None` for a duplicate or too many names.
    fn member(&mut self, object: usize, stored: Span, bytes: &[u8]) -> Option<()>;
    fn close(&mut self, node: usize);
}

/// Counts values and checks member names, holding only the open containers.
struct Count {
    values: usize,
    /// Per depth: whether the open container is an object, and its first name in `names`.
    open: [(bool, u16); DEPTH],
    names: [(u32, u32); NAMES],
    named: usize,
}

impl Count {
    fn new() -> Self {
        Self {
            values: 0,
            open: [(false, 0); DEPTH],
            names: [(0, 0); NAMES],
            named: 0,
        }
    }
}

impl Sink for Count {
    fn push(&mut self, parent: Option<usize>, _: Option<Name>, value: Value) -> Option<usize> {
        self.values += 1;
        let depth = parent.map_or(0, |parent| parent + 1);
        if let Value::Object | Value::Array = value {
            // No slot past `DEPTH` declines the input, as the scanner refuses it.
            *self.open.get_mut(depth)? = (matches!(value, Value::Object), self.named as u16);
        }
        Some(depth)
    }

    fn parent(&self, node: usize) -> Option<usize> {
        node.checked_sub(1)
    }

    fn object(&self, node: usize) -> bool {
        self.open[node].0
    }

    fn member(&mut self, object: usize, stored: Span, bytes: &[u8]) -> Option<()> {
        let first = usize::from(self.open[object].1);
        if self.named - first == MEMBERS || self.named == NAMES {
            return None;
        }
        let name = |(start, end): (u32, u32)| &bytes[start as usize + 1..end as usize - 1];
        let new = (stored.start as u32, stored.end as u32);
        // Every name in this input is unescaped, so equal names have equal bytes.
        if self.names[first..self.named]
            .iter()
            .any(|&other| name(other) == name(new))
        {
            return None;
        }
        self.names[self.named] = new;
        self.named += 1;
        Some(())
    }

    fn close(&mut self, node: usize) {
        self.named = usize::from(self.open[node].1);
    }
}

/// Stores the nodes of input the counting pass accepted, within the capacity charged for it.
struct Build<'a> {
    nodes: &'a mut Vec<Node>,
}

impl Sink for Build<'_> {
    fn push(&mut self, parent: Option<usize>, name: Option<Name>, value: Value) -> Option<usize> {
        if self.nodes.len() == self.nodes.capacity() {
            return None;
        }
        let index = self.nodes.len();
        self.nodes.push(Node {
            name,
            value,
            first: None,
            next: None,
            parent,
            revision: 0,
            last: None,
            origin: None,
            origin_next: None,
            origins: None,
            last_origin: None,
            published: false,
            published_through: 0,
            input_key: match name {
                Some(Name::Mapped(text, _)) => Some(text.span),
                _ => None,
            },
        });
        if let Some(parent) = parent {
            match self.nodes[parent].last {
                Some(last) => self.nodes[last].next = Some(index),
                None => self.nodes[parent].first = Some(index),
            }
            self.nodes[parent].last = Some(index);
        }
        Some(index)
    }

    fn parent(&self, node: usize) -> Option<usize> {
        self.nodes[node].parent
    }

    fn object(&self, node: usize) -> bool {
        matches!(self.nodes[node].value, Value::Object)
    }

    /// The counting pass already checked every name of the same bytes.
    fn member(&mut self, _: usize, _: Span, _: &[u8]) -> Option<()> {
        Some(())
    }

    fn close(&mut self, _: usize) {}
}

/// Reads one JSON value into `sink`; every method returns `None` to decline the input.
struct Reader<'a, S> {
    sink: &'a mut S,
    bytes: &'a [u8],
    at: usize,
}

impl<S: Sink> Reader<'_, S> {
    fn run(&mut self) -> Option<()> {
        let mut parent: Option<usize> = None;
        let mut name = None;
        loop {
            self.skip_whitespace();
            let start = self.at;
            let value = match *self.bytes.get(start)? {
                b'{' => Value::Object,
                b'[' => Value::Array,
                b'"' => {
                    self.string()?;
                    Value::String(self.text(start))
                }
                b'-' | b'0'..=b'9' | b't' | b'f' | b'n' => self.atom()?,
                _ => return None,
            };
            let node = self.sink.push(parent, name.take(), value)?;
            if let Value::Object | Value::Array = value {
                parent = Some(node);
                self.at += 1;
                self.skip_whitespace();
                match (value, *self.bytes.get(self.at)?) {
                    (Value::Object, b'}') | (Value::Array, b']') => {}
                    (Value::Object, _) => {
                        name = Some(self.key(node)?);
                        continue;
                    }
                    _ => continue,
                }
            }
            // After a value: close containers until the next member or the end of the input.
            loop {
                self.skip_whitespace();
                let Some(open) = parent else {
                    return (self.at == self.bytes.len()).then_some(());
                };
                let object = self.sink.object(open);
                match (object, *self.bytes.get(self.at)?) {
                    (_, b',') => {
                        self.at += 1;
                        if object {
                            name = Some(self.key(open)?);
                        }
                        break;
                    }
                    (true, b'}') | (false, b']') => {
                        self.at += 1;
                        self.sink.close(open);
                        parent = self.sink.parent(open);
                    }
                    _ => return None,
                }
            }
        }
    }

    fn skip_whitespace(&mut self) {
        while let Some(b' ' | b'\t' | b'\r' | b'\n') = self.bytes.get(self.at) {
            self.at += 1;
        }
    }

    fn text(&self, start: usize) -> Text {
        Text {
            source: 0,
            span: Span {
                start: start as u64,
                end: self.at as u64,
            },
        }
    }

    /// Reads the member name and colon that open the next member of `object`.
    fn key(&mut self, object: usize) -> Option<Name> {
        self.skip_whitespace();
        let start = self.at;
        if self.bytes.get(start) != Some(&b'"') || self.string()? {
            return None;
        }
        let text = self.text(start);
        self.sink.member(object, text.span, self.bytes)?;
        let digest = NameHash::plain(&self.bytes[start + 1..self.at - 1]);
        self.skip_whitespace();
        if self.bytes.get(self.at) != Some(&b':') {
            return None;
        }
        self.at += 1;
        Some(Name::Mapped(text, digest))
    }

    /// Steps over the string opening at the current byte; returns whether it holds an escape.
    fn string(&mut self) -> Option<bool> {
        let mut at = self.at + 1;
        let mut escaped = false;
        loop {
            match *self.bytes.get(at)? {
                b'"' => break,
                b'\\' => {
                    escaped = true;
                    at = self.escape(at + 1)?;
                }
                0..=0x1f => return None,
                _ => at += 1,
            }
        }
        self.at = at + 1;
        Some(escaped)
    }

    /// Checks the escape whose letter is at `at` and returns the offset after it. A `\u`
    /// surrogate must be a high half followed at once by an escaped low half.
    fn escape(&self, at: usize) -> Option<usize> {
        match *self.bytes.get(at)? {
            b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => Some(at + 1),
            b'u' => match self.hex(at + 1)? {
                0xd800..=0xdbff => {
                    let low = (self.bytes.get(at + 5..at + 7)? == b"\\u")
                        .then(|| self.hex(at + 7))
                        .flatten()?;
                    (0xdc00..=0xdfff).contains(&low).then_some(at + 11)
                }
                0xdc00..=0xdfff => None,
                _ => Some(at + 5),
            },
            _ => None,
        }
    }

    fn hex(&self, at: usize) -> Option<u32> {
        self.bytes
            .get(at..at + 4)?
            .iter()
            .try_fold(0, |code, &byte| Some(code * 16 + char::from(byte).to_digit(16)?))
    }

    /// Reads `true`, `false`, `null` or a number up to the next delimiter, as the scanner does.
    fn atom(&mut self) -> Option<Value> {
        let start = self.at;
        while let Some(&byte) = self.bytes.get(self.at) {
            if matches!(byte, b' ' | b'\t' | b'\r' | b'\n' | b',' | b']' | b'}') {
                break;
            }
            self.at += 1;
        }
        let atom = &self.bytes[start..self.at];
        Some(match atom {
            b"true" | b"false" => Value::Bool(atom == b"true"),
            b"null" => Value::Null,
            _ if number(atom) => Value::Number(self.text(start)),
            _ => return None,
        })
    }
}

/// Whether `atom` is a JSON number.
fn number(atom: &[u8]) -> bool {
    let digits = |at: &mut usize| {
        let start = *at;
        while atom.get(*at).is_some_and(u8::is_ascii_digit) {
            *at += 1;
        }
        *at > start
    };
    let mut at = usize::from(atom.first() == Some(&b'-'));
    match atom.get(at) {
        Some(b'0') => at += 1,
        Some(b'1'..=b'9') => {
            digits(&mut at);
        }
        _ => return false,
    }
    if atom.get(at) == Some(&b'.') {
        at += 1;
        if !digits(&mut at) {
            return false;
        }
    }
    if let Some(b'e' | b'E') = atom.get(at) {
        at += 1;
        if let Some(b'+' | b'-') = atom.get(at) {
            at += 1;
        }
        if !digits(&mut at) {
            return false;
        }
    }
    at == atom.len()
}
