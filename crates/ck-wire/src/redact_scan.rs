//! The scanner behind [`super::bytes`], which finds every URL reference in a whole
//! text, and [`copy`], which redacts a text read in pieces with bounded memory. Both
//! pass what the scan writes through [`Words`].

use super::{UNPRINTABLE, scheme_byte, suspect, url};
use std::io::{self, Read, Write};

/// The longest unfinished reference, and the longest word, [`copy`] holds back. A longer
/// one prints as [`UNPRINTABLE`] and the rest of it is dropped, which fails closed.
pub(super) const HOLD: usize = 1 << 20;

/// The most [`copy`] reads, and [`write`] scans, at once.
const PIECE: usize = 64 << 10;

/// Redacts the whole of `text` into `out`.
pub(super) fn whole(text: &[u8], out: &mut Vec<u8>) {
    let mut scanned = Vec::with_capacity(text.len());
    scan(text, true, None, &mut scanned);
    let mut words = Words::new(usize::MAX);
    words.push(&scanned, out);
    words.end(out);
}

/// `text`, written by [`super::url`], with every [`suspect`] word replaced.
pub(super) fn guarded(text: String) -> String {
    if !text.bytes().any(|b| matches!(b, b'/' | b'@')) {
        return text;
    }
    let mut out = Vec::with_capacity(text.len());
    let mut words = Words::new(usize::MAX);
    words.push(text.as_bytes(), &mut out);
    words.end(&mut out);
    // Words are cut at ASCII bytes, so UTF-8 stays valid.
    String::from_utf8(out).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
}

/// Redacts what `input` yields into `out` as [`whole`] redacts the whole text. Only a
/// reference whose end has not been read yet, and the word being written, are held back,
/// so memory stays within [`PIECE`] and twice `hold`.
pub(super) fn copy(input: &mut dyn Read, out: &mut dyn Write, hold: usize) -> io::Result<()> {
    let mut copy = Copy::new(hold);
    let mut piece = vec![0; PIECE];
    loop {
        match input.read(&mut piece) {
            Ok(0) => return copy.finish(out),
            Ok(n) => copy.push(&piece[..n], out)?,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

/// Redacts `text` into `out` as [`copy`] redacts it, a [`PIECE`] at a time.
pub(super) fn write(text: &[u8], out: &mut dyn Write, hold: usize) -> io::Result<()> {
    let mut copy = Copy::new(hold);
    for piece in text.chunks(PIECE) {
        copy.push(piece, out)?;
    }
    copy.finish(out)
}

/// The words of redacted text (the bytes between whitespace and `"`), each held until
/// its end, so that a [`suspect`] one prints as [`UNPRINTABLE`] whole. A word longer
/// than `hold` prints as the placeholder and the rest of it is dropped.
struct Words {
    word: Vec<u8>,
    dropping: bool,
    hold: usize,
}

impl Words {
    fn new(hold: usize) -> Self {
        Self {
            word: Vec::new(),
            dropping: false,
            hold,
        }
    }

    fn push(&mut self, mut bytes: &[u8], out: &mut Vec<u8>) {
        while let Some(n) = bytes.iter().position(|b| stop(*b)) {
            self.extend(&bytes[..n], out);
            self.end(out);
            out.push(bytes[n]);
            bytes = &bytes[n + 1..];
        }
        self.extend(bytes, out);
    }

    fn extend(&mut self, bytes: &[u8], out: &mut Vec<u8>) {
        if self.dropping || bytes.is_empty() {
            return;
        }
        if bytes.len() > self.hold - self.word.len() {
            out.extend_from_slice(UNPRINTABLE.as_bytes());
            self.word.clear();
            self.dropping = true;
        } else {
            self.word.extend_from_slice(bytes);
        }
    }

    /// Ends the word being held.
    fn end(&mut self, out: &mut Vec<u8>) {
        if !std::mem::take(&mut self.dropping) {
            out.extend_from_slice(if suspect(&self.word) {
                UNPRINTABLE.as_bytes()
            } else {
                &self.word
            });
        }
        self.word.clear();
    }
}

struct Copy {
    /// Bytes read but not yet scanned to the end: a reference whose end was not read yet,
    /// and what follows it.
    held: Vec<u8>,
    /// The byte before `held`, which decides whether a token can start at `held[0]`.
    before: Option<u8>,
    /// The reference past the hold limit whose rest is being dropped.
    skip: Option<Skip>,
    /// What the scan wrote, on its way through `words`.
    scanned: Vec<u8>,
    words: Words,
    written: Vec<u8>,
    hold: usize,
}

impl Copy {
    fn new(hold: usize) -> Self {
        Self {
            held: Vec::new(),
            before: None,
            skip: None,
            scanned: Vec::new(),
            words: Words::new(hold),
            written: Vec::new(),
            hold,
        }
    }

    fn push(&mut self, input: &[u8], out: &mut dyn Write) -> io::Result<()> {
        self.scan(input);
        self.words.push(&self.scanned, &mut self.written);
        self.scanned.clear();
        out.write_all(&self.written)?;
        self.written.clear();
        Ok(())
    }

    fn finish(mut self, out: &mut dyn Write) -> io::Result<()> {
        if self.skip.is_none() {
            scan(&self.held, true, self.before, &mut self.scanned);
        }
        self.words.push(&self.scanned, &mut self.written);
        self.words.end(&mut self.written);
        out.write_all(&self.written)
    }

    fn scan(&mut self, mut input: &[u8]) {
        if let Some(skip) = &mut self.skip {
            let Some(end) = skip.end(input) else { return };
            self.before = skip.last;
            self.skip = None;
            input = &input[end..];
        }
        self.held.extend_from_slice(input);
        while let Some(at) = scan(&self.held, false, self.before, &mut self.scanned) {
            if at > 0 {
                self.before = Some(self.held[at - 1]);
            }
            if self.held.len() - at <= self.hold {
                self.held.drain(..at);
                return;
            }
            // Past the hold limit a reference, or a token that may still become one,
            // prints as the placeholder, after a span's opener and the spaces that follow
            // it, and is dropped up to its end.
            let waiting = &self.held[at..];
            let kind = DELIMITERS.iter().position(|(open, _)| *open == waiting[0]);
            let keep = match kind {
                Some(_) => 1 + waiting[1..].iter().take_while(|b| **b == b' ').count(),
                None => 0,
            };
            self.scanned.extend_from_slice(&waiting[..keep]);
            self.scanned.extend_from_slice(UNPRINTABLE.as_bytes());
            let mut skip = Skip::new(kind, keep.checked_sub(1).map(|k| waiting[k]).or(self.before));
            match skip.end(&self.held[at + keep..]) {
                Some(end) => {
                    self.before = skip.last;
                    self.held.drain(..at + keep + end);
                }
                None => {
                    self.held = Vec::new();
                    self.skip = Some(skip);
                    return;
                }
            }
        }
        if let Some(last) = self.held.last() {
            self.before = Some(*last);
        }
        self.held.clear();
    }
}

/// The end of a dropped reference: its span's closer, or the end of its token. As
/// [`Next::unhidden`] does, a reference span opened inside the dropped part extends it
/// through that span's closer, where the scan starts again.
struct Skip {
    /// A token, which also ends at whitespace or `"`, rather than a span.
    token: bool,
    /// Each kind of span ([`DELIMITERS`]) opened in the dropped part.
    open: [Open; 4],
    /// A reference span closed in the dropped part.
    closed: bool,
    /// The last byte was a backslash escaping the next one in a `"` or `'` span.
    escaped: bool,
    /// The last byte skipped.
    last: Option<u8>,
}

impl Skip {
    fn new(kind: Option<usize>, last: Option<u8>) -> Self {
        Self {
            token: kind.is_none(),
            open: std::array::from_fn(|k| if kind == Some(k) { Open::Ref } else { Open::No }),
            closed: false,
            escaped: false,
            last,
        }
    }

    /// Where the skipped part ends in `bytes`; that byte itself is scanned again.
    fn end(&mut self, bytes: &[u8]) -> Option<usize> {
        for (i, &b) in bytes.iter().enumerate() {
            if self.closed && !self.pending() {
                return Some(i);
            }
            let escaped = std::mem::take(&mut self.escaped);
            let closer = DELIMITERS
                .iter()
                .position(|(_, close)| *close == b)
                .filter(|k| !escaped && !matches!(self.open[*k], Open::No));
            for (k, open) in self.open.iter_mut().enumerate() {
                if Some(k) != closer {
                    open.step(b);
                }
            }
            if escaped {
            } else if b == b'\\' && !(matches!(self.open[0], Open::No) && matches!(self.open[1], Open::No)) {
                self.escaped = true;
            } else {
                if let Some(kind) = closer {
                    if matches!(std::mem::replace(&mut self.open[kind], Open::No), Open::Ref) {
                        self.closed = true;
                        if !self.pending() {
                            return Some(i);
                        }
                    }
                } else if self.token && !self.pending() && stop(b) {
                    return Some(i);
                }
                // Every opener is checked, the closer of a span too, as `unhidden` does.
                if let Some(kind) = DELIMITERS.iter().position(|(open, _)| *open == b)
                    && matches!(self.open[kind], Open::No)
                {
                    self.open[kind] = Open::Head {
                        started: false,
                        scheme: 0,
                        alpha: false,
                        marks: 0,
                    };
                }
            }
            self.last = Some(b);
        }
        None
    }

    fn pending(&self) -> bool {
        self.open.iter().any(|o| !matches!(o, Open::No))
    }
}

/// A span opened in a dropped part, and whether its contents are a reference.
#[derive(Clone, Copy)]
enum Open {
    No,
    /// Not known yet: the contents read so far after the leading spaces are scheme bytes
    /// (`alpha` if the first is a letter) and `marks` bytes of `://`.
    Head {
        started: bool,
        scheme: usize,
        alpha: bool,
        marks: usize,
    },
    Ref,
}

impl Open {
    /// Reads the next byte of the contents, as [`reference`] would.
    fn step(&mut self, b: u8) {
        let Open::Head {
            started,
            scheme,
            alpha,
            marks,
        } = self
        else {
            return;
        };
        if !*started && b == b' ' {
            return;
        }
        if !*started && b == b'/' {
            *self = Open::Ref;
            return;
        }
        *started = true;
        if *marks == 0 && scheme_byte(b) {
            if *scheme == 0 {
                *alpha = b.is_ascii_alphabetic();
            }
            *scheme += 1;
        } else if b"://"[*marks] == b {
            *marks += 1;
            if *marks == 3 {
                *self = if *scheme == 0 || *alpha { Open::Ref } else { Open::No };
            }
        } else {
            *self = Open::No;
        }
    }
}

/// Scans `text`, which follows the byte `before`, into `out`. Unless the text is
/// `complete`, a reference whose end is not in `text` stops the scan: its start is
/// returned and nothing from there on is written.
fn scan(text: &[u8], complete: bool, before: Option<u8>, out: &mut Vec<u8>) -> Option<usize> {
    let (mut copied, mut i) = (0, 0);
    let mut next = Next::default();
    while i < text.len() {
        let step = match DELIMITERS.iter().position(|(open, _)| *open == text[i]) {
            Some(kind) => next.span(text, i, kind, complete),
            None if boundary(text, i, before) => next.token(text, i, complete),
            None => Step::No,
        };
        match step {
            Step::No => i += 1,
            Step::Ref(start, end, rendered) => {
                if rendered.as_bytes() != &text[start..end] {
                    out.extend_from_slice(&text[copied..start]);
                    out.extend_from_slice(rendered.as_bytes());
                    copied = end;
                }
                i = end;
            }
            Step::Wait => {
                out.extend_from_slice(&text[copied..i]);
                return Some(i);
            }
        }
    }
    out.extend_from_slice(&text[copied..]);
    None
}

enum Step {
    No,
    /// A reference: the range it replaces and what it prints as.
    Ref(usize, usize, String),
    /// A reference, or what may still become one, that ends after the scanned text.
    Wait,
}

const DELIMITERS: [(u8, u8); 4] = [(b'"', b'"'), (b'\'', b'\''), (b'<', b'>'), (b'`', b'`')];

/// Bytes after which a token can start, besides whitespace and non-ASCII text. Every
/// span closer is one, since a scan that passes over a span resumes at its closer.
const DELIMITER_BYTES: &[u8] = b"\"'`(<>[{=,;:";

/// The next position at or after the scan of each byte [`scan`] looks for, so every
/// search covers its bytes once and the scan stays linear.
#[derive(Default)]
struct Next {
    closers: [Option<usize>; 4],
    stop: Option<usize>,
    mark: Option<usize>,
}

impl Next {
    /// The reference delimited by the opener at `i`.
    fn span(&mut self, text: &[u8], i: usize, kind: usize, complete: bool) -> Step {
        match self.reach(text, i, kind, complete) {
            Span::Ref(start, close) => self.unhidden(text, start, close, complete, render_bytes(&text[start..close])),
            Span::No => Step::No,
            Span::Wait => Step::Wait,
        }
    }

    /// Whether the span opened at `i` is a reference, without rendering it. An unclosed
    /// span is none in a complete text.
    fn reach(&mut self, text: &[u8], i: usize, kind: usize, complete: bool) -> Span {
        // A closer found for an earlier opener also closes this one: the opener lies
        // inside that span, so it is escaped there and the escape parity matches.
        let close = match self.closers[kind] {
            Some(at) if at > i => at,
            _ => *self.closers[kind].insert(closing(text, i + 1, DELIMITERS[kind].1).unwrap_or(text.len())),
        };
        let end = close.min(text.len());
        let start = i + 1 + text[i + 1..end].iter().take_while(|b| **b == b' ').count();
        let contents = &text[start..end];
        if close < text.len() {
            return match reference(contents) {
                true => Span::Ref(start, close),
                false => Span::No,
            };
        }
        if !complete && (reference(contents) || reference_prefix(contents)) {
            Span::Wait
        } else {
            Span::No
        }
    }

    /// The reference over `start..end`, unless it hides the opener of a reference span
    /// that ends after it (`https://h/x<https://h/?code=a key>`): then both print as one
    /// [`UNPRINTABLE`], since neither alone is the whole URL.
    fn unhidden(&mut self, text: &[u8], start: usize, mut end: usize, complete: bool, rendered: String) -> Step {
        let mut hidden = false;
        let mut j = start + 1;
        while j < end {
            if let Some(kind) = DELIMITERS.iter().position(|(open, _)| *open == text[j]) {
                match self.reach(text, j, kind, complete) {
                    Span::Ref(_, close) if close > end => {
                        end = close;
                        hidden = true;
                    }
                    Span::Wait => return Step::Wait,
                    Span::Ref(..) | Span::No => {}
                }
            }
            j += 1;
        }
        Step::Ref(start, end, if hidden { UNPRINTABLE.to_owned() } else { rendered })
    }

    /// The reference or relative reference starting at `i`.
    fn token(&mut self, text: &[u8], i: usize, complete: bool) -> Step {
        let end = seek(text, i, &mut self.stop, stop);
        let open = end == text.len() && !complete;
        let token = &text[i..end];
        if reference(token) || (open && reference_prefix(token)) {
            return if open {
                Step::Wait
            } else {
                self.unhidden(text, i, end, complete, render_bytes(token))
            };
        }
        let mark = seek(text, i, &mut self.mark, |b| matches!(b, b'?' | b'#'));
        // A later delimiter before the `?` or `#` starts the token that is checked
        // instead, so a reference such as `url=//…` keeps its name.
        if (i + 1..mark.min(end)).any(|j| boundary(text, j, None)) {
            return Step::No;
        }
        if open {
            return Step::Wait;
        }
        // `what?` and a Markdown `##` carry no query or fragment.
        if mark >= end || text[mark..end].iter().all(|b| matches!(b, b'?' | b'#')) {
            return Step::No;
        }
        self.unhidden(text, i, end, complete, UNPRINTABLE.to_owned())
    }
}

enum Span {
    No,
    /// A reference: where its contents start and its closer.
    Ref(usize, usize),
    /// What may still become a reference, closed after the scanned text.
    Wait,
}

/// Whether more bytes could still make `text` a reference: scheme bytes and part of `://`.
fn reference_prefix(text: &[u8]) -> bool {
    let scheme = text.iter().take_while(|b| scheme_byte(**b)).count();
    let rest = &text[scheme..];
    rest.len() < 3 && b"://".starts_with(rest) && (scheme == 0 || text[0].is_ascii_alphabetic())
}

/// The first position at or after `from` holding a byte that `hit`s, or `text.len()`.
/// `cache` holds an earlier answer, which stays right while it is not before `from`.
fn seek(text: &[u8], from: usize, cache: &mut Option<usize>, hit: impl Fn(u8) -> bool) -> usize {
    match *cache {
        Some(at) if at >= from => at,
        _ => *cache.insert(
            text[from..]
                .iter()
                .position(|b| hit(*b))
                .map_or(text.len(), |n| from + n),
        ),
    }
}

/// The position of `close` searched from `i`, a backslash escaping the next byte in a
/// `"` or `'` span.
fn closing(text: &[u8], mut i: usize, close: u8) -> Option<usize> {
    while i < text.len() {
        match text[i] {
            b'\\' if matches!(close, b'"' | b'\'') => i += 2,
            b if b == close => return Some(i),
            _ => i += 1,
        }
    }
    None
}

fn stop(b: u8) -> bool {
    b.is_ascii_whitespace() || b == b'"'
}

/// Whether a token can start at `i` of a text that follows `before` (none at the start
/// of the whole text): an ASCII byte (so every cut falls between UTF-8 characters) after
/// whitespace, a delimiter or non-ASCII text such as `请访问http://…`.
fn boundary(text: &[u8], i: usize, before: Option<u8>) -> bool {
    let previous = if i == 0 { before } else { Some(text[i - 1]) };
    text[i].is_ascii() && previous.is_none_or(starts_after)
}

fn starts_after(b: u8) -> bool {
    b.is_ascii_whitespace() || !b.is_ascii() || DELIMITER_BYTES.contains(&b)
}

/// Whether `text` starts with `scheme://`, `://`, `//` or `/`.
fn reference(text: &[u8]) -> bool {
    if text.first() == Some(&b'/') {
        return true;
    }
    let scheme = text.iter().take_while(|b| scheme_byte(**b)).count();
    text[scheme..].starts_with(b"://") && (scheme == 0 || text[0].is_ascii_alphabetic())
}

fn render_bytes(reference: &[u8]) -> String {
    std::str::from_utf8(reference).map_or_else(|_| UNPRINTABLE.to_owned(), url)
}
