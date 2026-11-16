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

/// Text split into words at whitespace and `"`, each held until its end. A [`suspect`]
/// word, and a word longer than `hold`, prints as [`UNPRINTABLE`]. Such a word may start
/// a quoted value with spaces in it (`"callback?code=a key"`, `<https://h/?q=a key>`).
/// When the word follows a `"`, with or without whitespace between, and ends at
/// whitespace, the text is dropped up to the next `"`, line breaks included. When it holds `'`, a backtick or
/// `<`, and ends at whitespace other than a line break, the text is dropped up to the
/// next `"` or line break.
pub(super) struct Words {
    word: Vec<u8>,
    state: State,
    /// The word follows a `"`, with or without whitespace between.
    quoted: bool,
    /// The word, its dropped part included, holds `'`, a backtick or `<`.
    opens: bool,
    hold: usize,
}

#[derive(Clone, Copy, PartialEq)]
enum State {
    Word,
    /// The word passed the hold limit: its placeholder is written and the rest dropped.
    Long,
    /// Dropped up to the next `"`.
    Quote,
    /// Dropped up to the next `"` or line break.
    Line,
}

impl Words {
    pub(super) fn new(hold: usize) -> Self {
        Self {
            word: Vec::new(),
            state: State::Word,
            quoted: false,
            opens: false,
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
                let Some(n) = bytes.iter().position(|b| ends.contains(b)) else {
                    return;
                };
                self.state = State::Word;
                bytes = &bytes[n..];
            }
            match bytes.iter().position(|b| stop(*b)) {
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

    fn extend(&mut self, bytes: &[u8], out: &mut Vec<u8>) {
        self.opens |= bytes.iter().any(|b| matches!(b, b'\'' | b'`' | b'<'));
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
        let quoted = self.quoted;
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
            Some(b) => out.push(b),
            None => {}
        }
    }

    pub(super) fn finish(&mut self, out: &mut Vec<u8>) {
        if matches!(self.state, State::Word | State::Long) {
            self.end(None, out);
        }
    }

    /// Ends a piece of a text whose pieces reach the log with other text between them
    /// ([`super::Pieces`]). The next piece may go on with the word this one ends in, so a
    /// word that is suspect or whose last byte can be part of a URL prints as the
    /// placeholder, and the next piece is dropped up to that word's end. Any other word
    /// ends with the piece.
    pub(super) fn cut(&mut self, out: &mut Vec<u8>) {
        if self.state != State::Word || self.word.is_empty() {
            return;
        }
        if suspect(&self.word) || self.word.last().is_some_and(|b| url_byte(*b)) {
            out.extend_from_slice(UNPRINTABLE.as_bytes());
            self.state = State::Long;
        } else {
            out.extend_from_slice(&self.word);
        }
        self.word.clear();
    }
}

pub(super) fn stop(b: u8) -> bool {
    b.is_ascii_whitespace() || b == b'"'
}

/// Whether `word` may hold a credential: it holds an `@`, a `?`, a `#` or `://`, or a
/// scheme byte and `:` with at least one byte after them (`https:key`, `user:key`).
fn suspect(word: &[u8]) -> bool {
    word.iter().any(|b| matches!(b, b'@' | b'?' | b'#'))
        || word.windows(3).any(|w| w == b"://")
        || (1..word.len().saturating_sub(1)).any(|i| word[i] == b':' && super::scheme_byte(word[i - 1]))
}

/// Whether `b` can be part of a URL: unreserved, `%`, the delimiters a userinfo, path,
/// query or fragment hold, or non-ASCII text.
fn url_byte(b: u8) -> bool {
    b >= 0x80 || b.is_ascii_alphanumeric() || b"-._~!$&'()*+,;=%:@/?#".contains(&b)
}
