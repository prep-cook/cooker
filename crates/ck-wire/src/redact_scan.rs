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

/// How far past the end of a redacted line the drop may run while it looks for a line
/// closing what the redacted line may have left open. Past it, the rest of the text is
/// dropped.
const CARRY: usize = 64 << 10;

/// Text split into words at whitespace and `"`, each held until its end. A `\` makes
/// the byte after it part of the word, so an escaped quote or space (`\"`) never ends
/// one, across reads and cuts too. A [`suspect`] word, and a word longer than `hold`,
/// prints as [`UNPRINTABLE`], and so does the text after it that may still belong to
/// the same value:
/// - inside a `"` string, where JSON escapes every inner quote, the text up to the
///   closing unescaped `"`. A closing quote followed by a byte that cannot follow a JSON
///   string was not one, and the drop goes on as below;
/// - anywhere else, the rest of the line. When the line holds a quote, backtick, `<` or
///   `>`, escaped or not, a value may go on past it: the following lines go too, up to
///   and including the first line holding one of the same kinds. When that line may
///   hold a trigger as well, it is taken as a redacted line of its own. Only the line
///   breaks of these lines print. The drop runs at most [`CARRY`] bytes past the first
///   line end; then the rest of the text goes.
pub(super) struct Words {
    word: Vec<u8>,
    state: State,
    /// An odd number of unescaped `"` came before on this line: inside a string. A JSON
    /// string holds no raw line break, so each line starts outside.
    string: bool,
    /// The [`kind`]s of the quotes and brackets on this line.
    seen: u8,
    /// The kinds a dropped line held, which a later line must hold to end the drop.
    open: u8,
    /// The current line holds one of the [`Words::open`] kinds.
    closes: bool,
    /// The current dropped line holds a byte a trigger needs (`:`, `@`, `?`, `#`, `/`).
    marks: bool,
    /// The last byte read is a `\` that escapes the next one.
    escaped: bool,
    /// The last byte closed a string whose rest was dropped.
    closed: bool,
    /// Bytes dropped past the end of a redacted line.
    carried: Option<usize>,
    hold: usize,
}

#[derive(Clone, Copy, PartialEq)]
enum State {
    Word,
    /// The word passed the hold limit or a cut: its placeholder is written and the rest
    /// dropped.
    Long,
    /// Dropped up to the closing unescaped `"` of the string.
    Quote,
    /// Dropped up to the end of a line that closes what was left open.
    Line,
    /// Dropped to the end of the text.
    Rest,
}

impl Words {
    pub(super) fn new(hold: usize) -> Self {
        Self {
            word: Vec::new(),
            state: State::Word,
            string: false,
            seen: 0,
            open: 0,
            closes: false,
            marks: false,
            escaped: false,
            closed: false,
            carried: None,
            hold,
        }
    }

    pub(super) fn push(&mut self, mut bytes: &[u8], out: &mut Vec<u8>) {
        while !bytes.is_empty() {
            match self.state {
                State::Rest => return,
                State::Quote | State::Line => {
                    let n = self.drop(bytes, out);
                    bytes = &bytes[n..];
                    continue;
                }
                State::Word | State::Long => {}
            }
            if std::mem::take(&mut self.closed) && !after_string(bytes[0]) {
                self.state = State::Line;
                continue;
            }
            match self.find(bytes) {
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

    /// The first word stop in `bytes` that no `\` escapes. The escape state goes on past
    /// the bytes it passes, and so do the quotes and brackets they hold.
    fn find(&mut self, bytes: &[u8]) -> Option<usize> {
        for (i, &b) in bytes.iter().enumerate() {
            self.seen |= kind(b);
            if std::mem::take(&mut self.escaped) {
                continue;
            }
            if b == b'\\' {
                self.escaped = true;
            } else if stop(b) {
                return Some(i);
            }
        }
        None
    }

    /// Drops `bytes` in the [`State::Quote`] or [`State::Line`] state, up to the byte
    /// that ends it, and returns how many it read.
    fn drop(&mut self, bytes: &[u8], out: &mut Vec<u8>) -> usize {
        for (i, &b) in bytes.iter().enumerate() {
            if let Some(carried) = self.carried.as_mut() {
                *carried += 1;
                if *carried > CARRY {
                    self.state = State::Rest;
                    return bytes.len();
                }
            }
            self.seen |= kind(b);
            self.closes |= kind(b) & self.open != 0;
            self.marks |= matches!(b, b':' | b'@' | b'?' | b'#' | b'/');
            if std::mem::take(&mut self.escaped) {
                continue;
            }
            if b == b'\\' {
                self.escaped = true;
            } else if self.state == State::Quote {
                if b == b'"' {
                    out.push(b);
                    self.string = false;
                    self.closed = true;
                    self.carried = None;
                    self.state = State::Word;
                    return i + 1;
                }
                if b == b'\n' {
                    self.carried.get_or_insert(0);
                }
            } else if b == b'\n' {
                // The redacted line goes on when it holds a quote or bracket. A later line
                // that holds one of the same kinds ends the drop, unless it may hold a
                // trigger too: then it may open a value of its own.
                let first = self.carried.is_none();
                let goes_on = if first {
                    self.seen != 0
                } else {
                    !self.closes || self.marks && self.seen != 0
                };
                if !goes_on {
                    out.push(b);
                    self.next_line();
                    self.state = State::Word;
                    return i + 1;
                }
                // The line break stays, so the log keeps its lines.
                out.push(b);
                if first || self.closes {
                    self.open = self.seen;
                }
                self.seen = 0;
                self.closes = false;
                self.marks = false;
                self.carried.get_or_insert(0);
            } else if b == b'"' {
                self.string = !self.string;
            }
        }
        bytes.len()
    }

    fn next_line(&mut self) {
        self.string = false;
        self.seen = 0;
        self.open = 0;
        self.closes = false;
        self.marks = false;
        self.carried = None;
    }

    fn extend(&mut self, bytes: &[u8], out: &mut Vec<u8>) {
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
        self.word.clear();
        self.state = State::Word;
        let Some(b) = stop else { return };
        if redacted && self.string && b == b'"' {
            out.push(b);
            self.string = false;
            self.closed = true;
        } else if redacted && self.string {
            self.state = State::Quote;
            if b == b'\n' {
                self.carried = Some(0);
            }
        } else if redacted {
            self.state = State::Line;
            self.drop(&[b], out);
        } else {
            if b == b'\n' {
                self.next_line();
            } else if b == b'"' {
                self.string = !self.string;
            }
            out.push(b);
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
    /// is dropped as the rest of a redacted word is.
    pub(super) fn cut(&mut self, out: &mut Vec<u8>) {
        if self.state != State::Word || self.word.is_empty() {
            return;
        }
        out.extend_from_slice(UNPRINTABLE.as_bytes());
        self.word.clear();
        self.state = State::Long;
    }
}

/// A bit for each kind of quote or bracket that may hold a value across a line break:
/// `"`, `'`, a backtick, and `<` with `>`.
fn kind(b: u8) -> u8 {
    match b {
        b'"' => 1,
        b'\'' => 2,
        b'`' => 4,
        b'<' | b'>' => 8,
        _ => 0,
    }
}

/// Whether `b` may follow the closing quote of a JSON string.
fn after_string(b: u8) -> bool {
    b.is_ascii_whitespace() || matches!(b, b',' | b':' | b'}' | b']')
}

pub(super) fn stop(b: u8) -> bool {
    b.is_ascii_whitespace() || b == b'"'
}

/// Whether `word` may hold a credential: it holds an `@`, a `?`, a `#`, `://` or a `//`
/// at its start or after punctuation (`//user:key`, `x=//host`), or a
/// scheme (a letter, then letters, digits, `+`, `-` or `.`) and `:` with at least one
/// byte after them (`https:key`, `user:key`, `example.com:443`). The scheme may start
/// inside the run of scheme bytes before the `:`, at a letter after `+`, `-` or `.`
/// (`1-https:key`). A letter after a digit does not start one, so times
/// (`2026-11-16T05:00:00Z`) and IPv4 `host:port` print.
fn suspect(word: &[u8]) -> bool {
    word.iter().any(|b| matches!(b, b'@' | b'?' | b'#'))
        || word.windows(3).any(|w| w == b"://")
        // A scheme-relative reference: `//` at the start or after punctuation.
        || (0..word.len().saturating_sub(1))
            .any(|i| word[i..i + 2] == *b"//" && (i == 0 || !(word[i - 1].is_ascii_alphanumeric() || word[i - 1] == b'/')))
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
