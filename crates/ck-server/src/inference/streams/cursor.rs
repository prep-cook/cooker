//! Resume cursors and client body units (`docs/INTERNAL-STREAM.md` §6.7).

use bytes::Bytes;
use ck_stream::ScratchLease;

use crate::http::egress::Framing;

/// A position in a client response body. Cursors hold positions only, never identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cursor {
    /// `v1.e.<n>`: the client consumed the first `n` complete SSE events.
    Events(u64),
    /// `v1.b.<n>`: the client consumed the first `n` entity-body bytes of a JSON or raw body.
    Bytes(u64),
}

impl Cursor {
    /// The explicit beginning of a body framed as `framing`.
    pub fn start(framing: Framing) -> Self {
        match framing {
            Framing::Sse => Self::Events(0),
            Framing::Json | Framing::JsonArray | Framing::Raw => Self::Bytes(0),
        }
    }

    /// `v1.e.<n>` or `v1.b.<n>` with an unsigned 64-bit decimal `n`; `None` for anything else.
    pub fn parse(text: &[u8]) -> Option<Self> {
        let (kind, digits): (fn(u64) -> Self, _) = if let Some(rest) = text.strip_prefix(b"v1.e.") {
            (Self::Events, rest)
        } else {
            (Self::Bytes, text.strip_prefix(b"v1.b.")?)
        };
        if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
            return None;
        }
        std::str::from_utf8(digits).ok()?.parse().ok().map(kind)
    }

    /// The cursor kind a body framed as `framing` resumes from.
    pub fn fits(self, framing: Framing) -> bool {
        matches!(
            (self, framing),
            (Self::Events(_), Framing::Sse) | (Self::Bytes(_), Framing::Json | Framing::JsonArray | Framing::Raw)
        )
    }

    fn position(self) -> u64 {
        match self {
            Self::Events(n) | Self::Bytes(n) => n,
        }
    }
}

impl std::fmt::Display for Cursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Events(n) => write!(f, "v1.e.{n}"),
            Self::Bytes(n) => write!(f, "v1.b.{n}"),
        }
    }
}

/// Turns response egress output into client body bytes, counting units from the start of
/// the body and dropping every unit before the cursor, so a body sent after cursor `c`
/// is exactly the suffix of the full body after `c`. Every complete SSE event gets
/// `id: v1.e.<n>`, the cursor after it, as its last field; other framings count bytes.
pub struct Units {
    events: bool,
    after: u64,
    /// Events or bytes produced so far.
    produced: u64,
    /// The last SSE byte was a line feed, so a second one ends the event.
    newline: bool,
}

impl Units {
    /// `after` must fit `framing` ([`Cursor::fits`]).
    pub fn new(framing: Framing, after: Cursor) -> Self {
        Self {
            events: framing == Framing::Sse,
            after: after.position(),
            produced: 0,
            newline: false,
        }
    }

    /// Every unit before the cursor has been produced, so later output is deliverable.
    pub fn reached(&self) -> bool {
        self.produced >= self.after
    }

    /// Appends the deliverable client bytes of one egress output to `out`.
    pub fn push(&mut self, output: &ScratchLease, out: &mut Vec<Bytes>) -> Result<(), ck_stream::Error> {
        let mut offset = 0;
        while offset < output.len() {
            let chunk = output.chunk(offset)?;
            offset += chunk.len();
            if self.events {
                self.events_chunk(chunk, out);
            } else {
                let skip = self.after.saturating_sub(self.produced).min(chunk.len() as u64) as usize;
                if skip < chunk.len() {
                    out.push(Bytes::copy_from_slice(&chunk[skip..]));
                }
                self.produced += chunk.len() as u64;
            }
        }
        Ok(())
    }

    fn events_chunk(&mut self, chunk: &[u8], out: &mut Vec<Bytes>) {
        let mut start = 0;
        for (i, &byte) in chunk.iter().enumerate() {
            let ends = byte == b'\n' && self.newline;
            self.newline = byte == b'\n' && !ends;
            if !ends {
                continue;
            }
            // The event's blank line: its last field is the cursor after it.
            self.produced += 1;
            if self.produced > self.after {
                out.push(Bytes::copy_from_slice(&chunk[start..i]));
                out.push(Bytes::from(format!("id: v1.e.{}\n\n", self.produced)));
            }
            start = i + 1;
        }
        if start < chunk.len() && self.produced >= self.after {
            out.push(Bytes::copy_from_slice(&chunk[start..]));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursors_parse_only_their_grammar() {
        assert_eq!(Cursor::parse(b"v1.e.0"), Some(Cursor::Events(0)));
        assert_eq!(
            Cursor::parse(b"v1.b.18446744073709551615"),
            Some(Cursor::Bytes(u64::MAX))
        );
        for bad in [
            &b"v1.e."[..],
            b"v1.e.-1",
            b"v1.e.+1",
            b"v1.b.18446744073709551616",
            b"v2.e.1",
            b"v1.x.1",
            b"v1.e.1 ",
        ] {
            assert_eq!(Cursor::parse(bad), None, "{}", String::from_utf8_lossy(bad));
        }
        assert_eq!(Cursor::Events(7).to_string(), "v1.e.7");
        assert!(Cursor::Bytes(0).fits(Framing::JsonArray) && !Cursor::Bytes(0).fits(Framing::Sse));
        assert!(Cursor::Bytes(3).fits(Framing::Raw) && !Cursor::Events(3).fits(Framing::Raw));
    }
}
