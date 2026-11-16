//! The word rule behind [`super::bytes`], [`super::copy`], [`super::write`] and
//! [`super::Pieces`]. It parses no URL structure, so it fails closed by construction
//! (owner ruling Settled 60): a word that may hold a credential prints as
//! [`UNPRINTABLE`] whole.

use super::UNPRINTABLE;
use std::io::{self, Read, Write};

/// The longest word held back. A longer word prints as [`UNPRINTABLE`] and the rest of
/// it is dropped, on every path (owner ruling R-7, 2026-11-16).
pub(super) const HOLD: usize = 1 << 20;

/// The most [`copy`] reads, and [`write`] takes, at once.
const READ: usize = 64 << 10;

/// Redacts what `input` yields into `out`, holding back only the word being read.
pub(super) fn copy(input: &mut dyn Read, out: &mut dyn Write, hold: usize) -> io::Result<()> {
    let mut words = Words::new(hold);
    let mut piece = vec![0; READ];
    let mut written = Vec::new();
    loop {
        match input.read(&mut piece) {
            Ok(0) => {
                words.finish(&mut written);
                return out.write_all(&written);
            }
            Ok(n) => {
                words.push(&piece[..n], &mut written);
                out.write_all(&written)?;
                written.clear();
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

/// Redacts `text` into `out` as [`copy`] does, a [`READ`] at a time.
pub(super) fn write(text: &[u8], out: &mut dyn Write, hold: usize) -> io::Result<()> {
    let mut words = Words::new(hold);
    let mut written = Vec::new();
    for piece in text.chunks(READ) {
        words.push(piece, &mut written);
        out.write_all(&written)?;
        written.clear();
    }
    words.finish(&mut written);
    out.write_all(&written)
}

/// Text split into words at whitespace and `"`, each held until its end. A `\` makes
/// the byte after it part of the word, so an escaped quote or space (`\"`) never ends
/// one, across reads and cuts too. A [`suspect`] word, and a word longer than `hold`,
/// prints as [`UNPRINTABLE`]. Such a word may start a quoted value with spaces in it
/// (`"callback?code=a key"`, `<https://h/?q=a key>`). When the word is inside a `"`
/// string or follows a `"`, and ends at whitespace, the text is dropped up to the next
/// unescaped `"`, line breaks included. When it holds `'`, a backtick, `<` or an escaped
/// `"`, and ends at
/// whitespace other than a line break, the text is dropped up to the next unescaped `"`
/// or line break.
pub(super) struct Words {
    word: Vec<u8>,
    state: State,
    /// The word follows a `"`, with or without whitespace between.
    quoted: bool,
    /// An odd number of unescaped `"` came before the word: it is inside a string.
    string: bool,
    /// The word, its dropped part included, holds `'`, a backtick, `<` or an escaped `"`.
    opens: bool,
    /// The last byte read is a `\` that escapes the next one.
    escaped: bool,
    hold: usize,
}

#[derive(Clone, Copy, PartialEq)]
enum State {
    Word,
    /// The word passed the hold limit or a cut: its placeholder is written and the rest
    /// dropped.
    Long,
    /// Dropped up to the next unescaped `"`.
    Quote,
    /// Dropped up to the next unescaped `"` or line break.
    Line,
}

impl Words {
    pub(super) fn new(hold: usize) -> Self {
        Self {
            word: Vec::new(),
            state: State::Word,
            quoted: false,
            string: false,
            opens: false,
            escaped: false,
            hold,
        }
    }

    pub(super) fn push(&mut self, mut bytes: &[u8], out: &mut Vec<u8>) {
        while !bytes.is_empty() {
            let ends: &[u8] = match self.state {
                State::Quote => b"\"",
                State::Line => b"\"\n",
                _ => b"",
            };
            if !ends.is_empty() {
                let Some(n) = self.find(bytes, |b| ends.contains(&b)) else {
                    return;
                };
                self.state = State::Word;
                bytes = &bytes[n..];
            }
            match self.find(bytes, stop) {
                Some(n) => {
                    self.extend(&bytes[..n], out);
                    self.end(Some(bytes[n]), out);
                    bytes = &bytes[n + 1..];
                }
                None => {
                    self.extend(bytes, out);
                    return;
                }
            }
        }
    }

    /// The first byte of `bytes` that `end` accepts and no `\` escapes. The escape state
    /// goes on past the bytes it passes.
    fn find(&mut self, bytes: &[u8], end: impl Fn(u8) -> bool) -> Option<usize> {
        for (i, &b) in bytes.iter().enumerate() {
            if std::mem::take(&mut self.escaped) {
                continue;
            }
            if b == b'\\' {
                self.escaped = true;
            } else if end(b) {
                return Some(i);
            }
        }
        None
    }

    fn extend(&mut self, bytes: &[u8], out: &mut Vec<u8>) {
        // A `"` inside a word is escaped.
        self.opens |= bytes.iter().any(|b| matches!(b, b'\'' | b'`' | b'<' | b'"'));
        if self.state == State::Long || bytes.is_empty() {
            return;
        }
        if bytes.len() > self.hold - self.word.len() {
            out.extend_from_slice(UNPRINTABLE.as_bytes());
            self.word.clear();
            self.state = State::Long;
        } else {
            self.word.extend_from_slice(bytes);
        }
    }

    /// Ends the word at `stop`, or at the end of the text.
    fn end(&mut self, stop: Option<u8>, out: &mut Vec<u8>) {
        let long = self.state == State::Long;
        let redacted = long || suspect(&self.word);
        if !redacted {
            out.extend_from_slice(&self.word);
        } else if !long {
            out.extend_from_slice(UNPRINTABLE.as_bytes());
        }
        let empty = self.word.is_empty() && !long;
        let quoted = self.quoted || self.string;
        let opens = std::mem::take(&mut self.opens);
        self.word.clear();
        self.state = State::Word;
        self.quoted = match stop {
            Some(b'"') => true,
            Some(b'\n') | None => false,
            Some(_) => empty && self.quoted,
        };
        match stop {
            Some(b) if redacted && quoted && b != b'"' => self.state = State::Quote,
            Some(b) if redacted && opens && !matches!(b, b'"' | b'\n') => self.state = State::Line,
            Some(b) => {
                self.string ^= b == b'"';
                out.push(b);
            }
            None => {}
        }
    }

    pub(super) fn finish(&mut self, out: &mut Vec<u8>) {
        if matches!(self.state, State::Word | State::Long) {
            self.end(None, out);
        }
    }

    /// Ends a piece of a text whose pieces reach the log with other text between them
    /// ([`super::Pieces`]). The next piece may go on with the word this one ends in,
    /// whatever its last byte, so that word prints as the placeholder and the next piece
    /// is dropped up to the end of its first word.
    pub(super) fn cut(&mut self, out: &mut Vec<u8>) {
        if self.state != State::Word || self.word.is_empty() {
            return;
        }
        out.extend_from_slice(UNPRINTABLE.as_bytes());
        self.word.clear();
        self.state = State::Long;
    }
}

pub(super) fn stop(b: u8) -> bool {
    b.is_ascii_whitespace() || b == b'"'
}

/// Whether `word` may hold a credential: it holds an `@`, a `?`, a `#` or `://`, or a
/// scheme (a letter, then letters, digits, `+`, `-` or `.`) and `:` with at least one
/// byte after them (`https:key`, `user:key`, `example.com:443`). The scheme may start
/// inside the run of scheme bytes before the `:`, at a letter after `+`, `-` or `.`
/// (`1-https:key`). A letter after a digit does not start one, so times
/// (`2026-11-16T05:00:00Z`) and IPv4 `host:port` print.
fn suspect(word: &[u8]) -> bool {
    word.iter().any(|b| matches!(b, b'@' | b'?' | b'#'))
        || word.windows(3).any(|w| w == b"://")
        || (1..word.len().saturating_sub(1)).any(|i| {
            // Each search stops at the previous `:`, so the scan stays linear.
            word[i] == b':' && {
                let start = word[..i]
                    .iter()
                    .rposition(|b| !super::scheme_byte(*b))
                    .map_or(0, |p| p + 1);
                (start..i)
                    .any(|j| word[j].is_ascii_alphabetic() && (j == start || matches!(word[j - 1], b'+' | b'-' | b'.')))
            }
        })
}
