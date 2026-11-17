//! Member names and lookup. A stored name carries a digest of its decoded form, so a lookup
//! compares one integer per member and reads stored bytes only for a candidate.

use super::{Characters, Document, Name, Text, Value};
use crate::codec::{Error, Result, json, source::Span};

/// A hash of a decoded name's UTF-8 bytes in the upper 31 bits, and in the lowest bit whether
/// the stored bytes are something else: a JSON escape or, in a pointer segment, `~0` or `~1`.
/// Every such escape is longer than what it decodes to, so a name is escaped exactly when its
/// stored length differs from its decoded one. Four bytes keep [`Name`] at 32 bytes.
#[derive(Clone, Copy, Debug, Default)]
pub struct Digest(u32);

impl Digest {
    pub(super) fn hash_is(self, hash: u32) -> bool {
        (self.0 ^ hash) >> 1 == 0
    }

    fn escaped(self) -> bool {
        self.0 & 1 != 0
    }
}

/// Builds a [`Digest`] one decoded scalar at a time. The hash mixes the name's length and its
/// first and last four bytes, so hashing a lookup name costs the same at any length. Names that
/// share all three only cost a comparison of the stored bytes; the scan stays linear, so the hash
/// need not resist flooding.
#[derive(Clone, Copy, Default)]
pub(crate) struct NameHash {
    head: u32,
    tail: u32,
    length: u64,
}

impl NameHash {
    pub(crate) fn push(&mut self, scalar: char) {
        for &byte in scalar.encode_utf8(&mut [0; 4]).as_bytes() {
            self.byte(byte);
        }
    }

    fn byte(&mut self, byte: u8) {
        if self.length < 4 {
            self.head |= u32::from(byte) << (8 * self.length);
        }
        self.tail = self.tail << 8 | u32::from(byte);
        self.length += 1;
    }

    fn hash(self) -> u32 {
        (self.head ^ self.tail.rotate_left(16) ^ (self.length as u32).wrapping_mul(0x9e37_79b9))
            .wrapping_mul(0x27d4_eb2d)
    }

    pub(crate) fn of(name: &str) -> u32 {
        Self::of_bytes(name.as_bytes())
    }

    /// The digest of a name stored without escapes, whose stored bytes are its UTF-8.
    pub(super) fn plain(stored: &[u8]) -> Digest {
        Digest(Self::of_bytes(stored) & !1)
    }

    fn of_bytes(bytes: &[u8]) -> u32 {
        let mut hash = Self::default();
        for &byte in &bytes[..bytes.len().min(4)] {
            hash.byte(byte);
        }
        for &byte in &bytes[bytes.len().saturating_sub(4).max(bytes.len().min(4))..] {
            hash.byte(byte);
        }
        hash.length = bytes.len() as u64;
        hash.hash()
    }

    /// The digest of the pushed name, whose stored form is `stored` bytes long.
    pub(crate) fn finish(self, stored: u64) -> Digest {
        Digest(self.hash() & !1 | u32::from(self.length != stored))
    }
}

pub(super) fn chars_equal(mut a: Characters<'_>, mut b: Characters<'_>) -> Result<bool> {
    loop {
        let c = a.next_scalar()?;
        if c != b.next_scalar()? {
            return Ok(false);
        }
        if c.is_none() {
            return Ok(true);
        }
    }
}

impl Document {
    /// Names a member by the quoted JSON string at `text`.
    pub fn mapped_name(&self, text: Text) -> Result<Name> {
        let mut decoded = json::Decoded::new(self.source(text)?, text.span)?;
        let mut hash = NameHash::default();
        while let Some(scalar) = decoded.next_scalar()? {
            hash.push(scalar);
        }
        Ok(Name::Mapped(text, hash.finish(text.span.len() - 2)))
    }

    /// Names a member by the last segment of the RFC 6901 pointer in the quoted JSON string at
    /// `text`. Segments end at a decoded `/`, which a JSON escape may spell; the last segment
    /// must hold only valid `~0` and `~1` escapes.
    pub fn pointer_name(&self, text: Text) -> Result<Name> {
        let mut decoded = json::Decoded::new(self.source(text)?, text.span)?;
        let mut start = decoded.offset();
        let mut hash = NameHash::default();
        let (mut tilde, mut invalid) = (false, false);
        while let Some(scalar) = decoded.next_scalar()? {
            match (tilde, scalar) {
                (_, '/') => {
                    start = decoded.offset();
                    hash = NameHash::default();
                    (tilde, invalid) = (false, false);
                    continue;
                }
                (true, '0') => hash.push('~'),
                (true, '1') => hash.push('/'),
                (true, _) => invalid = true,
                (false, '~') => {}
                (false, scalar) => hash.push(scalar),
            }
            tilde = !tilde && scalar == '~';
        }
        if tilde || invalid {
            return Err(Error::Schema);
        }
        let span = Span {
            start,
            end: text.span.end - 1,
        };
        let segment = Text {
            source: text.source,
            span,
        };
        Ok(Name::Pointer(segment, hash.finish(span.len())))
    }

    pub fn names_equal(&self, a: Name, b: Name) -> Result<bool> {
        chars_equal(self.name_chars(a)?, self.name_chars(b)?)
    }

    pub fn name_is(&self, name: Name, value: &str) -> Result<bool> {
        self.name_matches(name, value, NameHash::of(value))
    }

    /// [`Self::name_is`] with `hash`, the [`NameHash::of`] `value`, computed by the caller.
    fn name_matches(&self, name: Name, value: &str, hash: u32) -> Result<bool> {
        let (text, span, digest) = match name {
            Name::Static(name) => return Ok(name == value),
            Name::Mapped(text, digest) => (
                text,
                Span {
                    start: text.span.start + 1,
                    end: text.span.end - 1,
                },
                digest,
            ),
            Name::Pointer(text, digest) => (text, text.span, digest),
        };
        if !digest.hash_is(hash) {
            return Ok(false);
        }
        if !digest.escaped() {
            // The stored bytes are the decoded name's UTF-8.
            return self.source(text)?.equals(span, value.as_bytes());
        }
        let mut chars = self.name_chars(name)?;
        for expected in value.chars() {
            if chars.next_scalar()? != Some(expected) {
                return Ok(false);
            }
        }
        Ok(chars.next_scalar()?.is_none())
    }

    pub fn name_chars(&self, name: Name) -> Result<Characters<'_>> {
        match name {
            Name::Static(s) => Ok(Characters::Static(s.chars())),
            Name::Mapped(t, _) => Ok(Characters::Mapped(json::Decoded::new(self.source(t)?, t.span)?)),
            Name::Pointer(t, _) => Ok(Characters::Pointer(json::Decoded::fragment(self.source(t)?, t.span)?)),
        }
    }

    pub fn member(&self, parent: usize, name: &str) -> Result<Option<usize>> {
        if !matches!(self.node(parent)?.value, Value::Object) {
            return Err(Error::Schema);
        }
        let hash = NameHash::of(name);
        for child in self.children(parent)? {
            if self.name_matches(self.nodes[child].name.ok_or(Error::Schema)?, name, hash)? {
                return Ok(Some(child));
            }
        }
        Ok(None)
    }
}
