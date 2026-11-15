//! The scanner behind [`super::bytes`], which finds every URL reference in a whole
//! text, and [`copy`], which redacts a text read in pieces with bounded memory.

use super::{UNPRINTABLE, scheme_byte, url};
use std::io::{self, Read, Write};

/// The longest unfinished reference [`copy`] holds back. A longer one prints as
/// [`UNPRINTABLE`] and the rest of it is dropped, which fails closed; a longer token that
/// cannot become a reference (no `?`, `#` or delimiter, and no `scheme://` or `/` start)
/// is written as it is.
pub(super) const HOLD: usize = 1 << 20;

/// The most [`copy`] reads at once.
const PIECE: usize = 64 << 10;

/// Redacts the whole of `text` into `out`.
pub(super) fn whole(text: &[u8], out: &mut Vec<u8>) {
    scan(text, true, out);
}

/// Redacts what `input` yields into `out` as [`whole`] redacts the whole text. Only a
/// reference whose end has not been read yet is held back, so memory stays within
/// [`PIECE`] and `hold`.
pub(super) fn copy(input: &mut dyn Read, out: &mut dyn Write, hold: usize) -> io::Result<()> {
    let mut copy = Copy {
        held: Vec::new(),
        skip: None,
        out: Vec::new(),
        hold,
    };
    let mut piece = vec![0; PIECE];
    loop {
        let n = match input.read(&mut piece) {
            Ok(n) => n,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        if n == 0 {
            if copy.skip.is_none() {
                whole(&copy.held, &mut copy.out);
            }
            return out.write_all(&copy.out);
        }
        copy.push(&piece[..n]);
        out.write_all(&copy.out)?;
        copy.out.clear();
    }
}

struct Copy {
    /// Bytes read but not yet written: a reference whose end was not read yet, and
    /// what follows it.
    held: Vec<u8>,
    /// The token past the hold limit whose rest is being dropped or written.
    skip: Option<Skip>,
    out: Vec<u8>,
    hold: usize,
}

impl Copy {
    fn push(&mut self, mut input: &[u8]) {
        if let Some(skip) = &mut self.skip {
            let end = skip.end(input);
            if skip.write {
                self.out.extend_from_slice(&input[..end.unwrap_or(input.len())]);
            }
            let Some(at) = end else { return };
            input = &input[at..];
            self.skip = None;
        }
        self.held.extend_from_slice(input);
        while let Some(at) = scan(&self.held, false, &mut self.out) {
            if self.held.len() - at <= self.hold {
                self.held.drain(..at);
                return;
            }
            // Past the hold limit a reference prints as the placeholder, after a span's
            // opener and the spaces that follow it, and is dropped up to its end. A token
            // that cannot become one is written up to where a token could start again.
            let waiting = &self.held[at..];
            let close = DELIMITERS.iter().find(|(open, _)| *open == waiting[0]).map(|d| d.1);
            let mut skip = Skip {
                close,
                escaped: false,
                write: close.is_none() && !reference(waiting) && !waiting.iter().any(|b| matches!(b, b'?' | b'#')),
            };
            // A scheme followed by part of `://` keeps that part, which is scanned again
            // as the start of a token, so the bytes after it cannot slip past as text.
            let scheme = waiting.iter().take_while(|b| scheme_byte(**b)).count();
            if skip.write && scheme < waiting.len() && reference_prefix(waiting) {
                self.out.extend_from_slice(&waiting[..scheme]);
                self.held.drain(..at + scheme);
                continue;
            }
            let keep = match close {
                _ if skip.write => waiting.len(),
                Some(_) => 1 + waiting[1..].iter().take_while(|b| **b == b' ').count(),
                None => 0,
            };
            self.out.extend_from_slice(&waiting[..keep]);
            if !skip.write {
                self.out.extend_from_slice(UNPRINTABLE.as_bytes());
            }
            match skip.end(&self.held[at + keep..]) {
                Some(end) => drop(self.held.drain(..at + keep + end)),
                None => {
                    self.held = Vec::new();
                    self.skip = Some(skip);
                    return;
                }
            }
        }
        self.held.clear();
    }
}

/// The end of a dropped reference (its span's closer, or the end of its token), or of a
/// written token (where a token could start again).
struct Skip {
    close: Option<u8>,
    /// The last byte was a backslash escaping the next one in a `"` or `'` span.
    escaped: bool,
    /// The token is written, not dropped.
    write: bool,
}

impl Skip {
    /// Where the skipped part ends in `bytes`; that byte itself is scanned again.
    fn end(&mut self, bytes: &[u8]) -> Option<usize> {
        if self.write {
            return bytes.iter().position(|b| starts_after(*b) || matches!(b, b'?' | b'#'));
        }
        for (i, &b) in bytes.iter().enumerate() {
            match self.close {
                None if stop(b) => return Some(i),
                None => {}
                Some(_) if std::mem::take(&mut self.escaped) => {}
                Some(close) if b == b'\\' && matches!(close, b'"' | b'\'') => self.escaped = true,
                Some(close) if b == close => return Some(i),
                Some(_) => {}
            }
        }
        None
    }
}

/// Scans `text` into `out`. Unless the text is `complete`, a reference whose end is not
/// in `text` stops the scan: its start is returned and nothing from there on is written.
fn scan(text: &[u8], complete: bool, out: &mut Vec<u8>) -> Option<usize> {
    let (mut copied, mut i) = (0, 0);
    let mut next = Next::default();
    while i < text.len() {
        let step = match DELIMITERS.iter().position(|(open, _)| *open == text[i]) {
            Some(kind) => next.span(text, i, kind, complete),
            None if boundary(text, i) => next.token(text, i, complete),
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

/// Bytes after which a token can start, besides whitespace and non-ASCII text.
const DELIMITER_BYTES: &[u8] = b"\"'`(<[{=,;:";

/// The next position at or after the scan of each byte [`scan`] looks for, so every
/// search covers its bytes once and the scan stays linear.
#[derive(Default)]
struct Next {
    closers: [Option<usize>; 4],
    stop: Option<usize>,
    mark: Option<usize>,
}

impl Next {
    /// The reference delimited by the opener at `i`. An unclosed span is none in a
    /// complete text.
    fn span(&mut self, text: &[u8], i: usize, kind: usize, complete: bool) -> Step {
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
                true => Step::Ref(start, close, render_bytes(contents)),
                false => Step::No,
            };
        }
        if !complete && (reference(contents) || reference_prefix(contents)) {
            Step::Wait
        } else {
            Step::No
        }
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
                Step::Ref(i, end, render_bytes(token))
            };
        }
        let mark = seek(text, i, &mut self.mark, |b| matches!(b, b'?' | b'#'));
        // A later delimiter before the `?` or `#` starts the token that is checked
        // instead, so a reference such as `url=//…` keeps its name.
        if (i + 1..mark.min(end)).any(|j| boundary(text, j)) {
            return Step::No;
        }
        if open {
            return Step::Wait;
        }
        // `what?` and a Markdown `##` carry no query or fragment.
        if mark >= end || text[mark..end].iter().all(|b| matches!(b, b'?' | b'#')) {
            return Step::No;
        }
        Step::Ref(i, end, UNPRINTABLE.to_owned())
    }
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

/// Whether a token can start at `i`: an ASCII byte (so every cut falls between UTF-8
/// characters) after whitespace, a delimiter or non-ASCII text such as `请访问http://…`.
fn boundary(text: &[u8], i: usize) -> bool {
    text[i].is_ascii() && (i == 0 || starts_after(text[i - 1]))
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
