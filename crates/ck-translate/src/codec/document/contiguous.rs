//! The one-pass reader behind [`Document::parse_internal`] for JSON held in one contiguous
//! slice. It accepts only input that [`json::Scanner`](crate::codec::json::Scanner) accepts
//! and builds the nodes `Document::import` would build from it. It declines everything else,
//! including valid forms it leaves to the scanner (escaped member names, objects with many
//! members), so the scanner returns the exact error or tree.

use super::{Document, Name, NameHash, Node, Text, Value};
use crate::codec::{Result, source::Span};

/// Containers open at once; the scanner refuses deeper input.
const DEPTH: usize = 128;
/// Members of one object checked for a duplicate by a linear scan. A larger object goes to the
/// scanner's keyed index, so crafted names cannot make the check quadratic.
const MEMBERS: usize = 64;

impl Document {
    /// Adds the nodes of `bytes`, the whole of source 0, to this empty document. Returns false,
    /// leaving no nodes, when the scanner must read `bytes` instead.
    pub(super) fn read_contiguous(&mut self, bytes: &[u8]) -> Result<bool> {
        let Some(bound) = std::str::from_utf8(bytes).ok().and_then(|_| node_bound(bytes)) else {
            return Ok(false);
        };
        self.reserve(bound, 1, 0)?;
        let mut reader = Reader {
            nodes: &mut self.nodes,
            bytes,
            at: 0,
        };
        if reader.run().is_none() {
            self.nodes.clear();
            return Ok(false);
        }
        Ok(true)
    }
}

/// An upper bound on the values in `bytes`: the root, plus one per `{`, `[` and `,` outside
/// strings, since each of those opens at most one value. `None` for an unterminated string.
fn node_bound(bytes: &[u8]) -> Option<usize> {
    let (mut bound, mut at) = (1usize, 0);
    while at < bytes.len() {
        match bytes[at] {
            b'"' => loop {
                at += 1;
                match *bytes.get(at)? {
                    b'"' => break,
                    b'\\' => at += 1,
                    _ => {}
                }
            },
            b'{' | b'[' | b',' => bound += 1,
            _ => {}
        }
        at += 1;
    }
    Some(bound)
}

/// Reads one JSON value; every method returns `None` to decline the input.
struct Reader<'a> {
    nodes: &'a mut Vec<Node>,
    bytes: &'a [u8],
    at: usize,
}

impl Reader<'_> {
    fn run(&mut self) -> Option<()> {
        let mut parent: Option<usize> = None;
        let mut depth = 0;
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
            let node = self.push(parent, name.take(), value)?;
            if let Value::Object | Value::Array = value {
                if depth == DEPTH {
                    return None;
                }
                depth += 1;
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
                let object = matches!(self.nodes[open].value, Value::Object);
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
                        parent = self.nodes[open].parent;
                        depth -= 1;
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

    /// Appends a node as the last child of `parent`, within the capacity already charged.
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

    /// Reads the member name and colon that open the next member of `object`.
    fn key(&mut self, object: usize) -> Option<Name> {
        self.skip_whitespace();
        let start = self.at;
        if self.bytes.get(start) != Some(&b'"') || self.string()? {
            return None;
        }
        let text = self.text(start);
        let stored = &self.bytes[start + 1..self.at - 1];
        let mut sibling = self.nodes[object].first;
        for _ in 0..MEMBERS {
            let Some(index) = sibling else {
                self.skip_whitespace();
                if self.bytes.get(self.at) != Some(&b':') {
                    return None;
                }
                self.at += 1;
                return Some(Name::Mapped(text, NameHash::plain(stored)));
            };
            // Every name in this input is unescaped, so equal names have equal bytes.
            let Some(Name::Mapped(other, _)) = self.nodes[index].name else {
                return None;
            };
            if &self.bytes[other.span.start as usize + 1..other.span.end as usize - 1] == stored {
                return None;
            }
            sibling = self.nodes[index].next;
        }
        None
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
