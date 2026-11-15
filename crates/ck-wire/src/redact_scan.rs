//! The scanner behind [`super::bytes`]: it finds every URL reference in a whole text.

use super::{UNPRINTABLE, scheme_byte, url};

/// Redacts the whole of `text` into `out`.
pub(super) fn whole(text: &[u8], out: &mut Vec<u8>) {
    let (mut copied, mut i) = (0, 0);
    let mut next = Next::default();
    while i < text.len() {
        let found = match DELIMITERS.iter().position(|(open, _)| *open == text[i]) {
            Some(kind) => next.span(text, i, kind),
            None if boundary(text, i) => next.token(text, i),
            None => None,
        };
        match found {
            None => i += 1,
            Some(Found { start, end, rendered }) => {
                if rendered.as_bytes() != &text[start..end] {
                    out.extend_from_slice(&text[copied..start]);
                    out.extend_from_slice(rendered.as_bytes());
                    copied = end;
                }
                i = end;
            }
        }
    }
    out.extend_from_slice(&text[copied..]);
}

/// A reference: the range it replaces and what it prints as.
struct Found {
    start: usize,
    end: usize,
    rendered: String,
}

const DELIMITERS: [(u8, u8); 4] = [(b'"', b'"'), (b'\'', b'\''), (b'<', b'>'), (b'`', b'`')];

/// Bytes after which a token can start, besides whitespace and non-ASCII text.
const DELIMITER_BYTES: &[u8] = b"\"'`(<[{=,;:";

/// The next position at or after the scan of each byte [`whole`] looks for, so every
/// search covers its bytes once and the scan stays linear.
#[derive(Default)]
struct Next {
    closers: [Option<usize>; 4],
    stop: Option<usize>,
    mark: Option<usize>,
}

impl Next {
    /// The reference delimited by the opener at `i`; an unclosed span is none.
    fn span(&mut self, text: &[u8], i: usize, kind: usize) -> Option<Found> {
        // A closer found for an earlier opener also closes this one: the opener lies
        // inside that span, so it is escaped there and the escape parity matches.
        let close = match self.closers[kind] {
            Some(at) if at > i => at,
            _ => *self.closers[kind].insert(closing(text, i + 1, DELIMITERS[kind].1).unwrap_or(text.len())),
        };
        if close >= text.len() {
            return None;
        }
        let start = i + 1 + text[i + 1..close].iter().take_while(|b| **b == b' ').count();
        reference(&text[start..close]).then(|| Found {
            start,
            end: close,
            rendered: render_bytes(&text[start..close]),
        })
    }

    /// The reference or relative reference starting at `i`.
    fn token(&mut self, text: &[u8], i: usize) -> Option<Found> {
        let end = seek(text, i, &mut self.stop, stop);
        if reference(&text[i..end]) {
            return Some(Found {
                start: i,
                end,
                rendered: render_bytes(&text[i..end]),
            });
        }
        let mark = seek(text, i, &mut self.mark, |b| matches!(b, b'?' | b'#'));
        // A later delimiter before the `?` or `#` starts the token that is checked
        // instead, so a reference such as `url=//…` keeps its name.
        if (i + 1..mark.min(end)).any(|j| boundary(text, j)) {
            return None;
        }
        // `what?` and a Markdown `##` carry no query or fragment.
        if mark >= end || text[mark..end].iter().all(|b| matches!(b, b'?' | b'#')) {
            return None;
        }
        Some(Found {
            start: i,
            end,
            rendered: UNPRINTABLE.to_owned(),
        })
    }
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
