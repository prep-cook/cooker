//! The scanner behind [`super::bytes`], and [`Stream`] and [`Writer`] for text that
//! arrives in pieces: a reference split across pieces is held back until it ends.

use super::{UNPRINTABLE, scheme_byte, url};

/// The longest reference a [`Stream`] holds back; a longer one prints as [`UNPRINTABLE`]
/// and the rest of it is dropped.
pub const HOLD_LIMIT: usize = 1 << 20;

/// [`super::bytes`] for text that arrives in pieces: a reference split across pieces is held
/// back until it ends, so the output equals [`super::bytes`] of the whole text while no
/// reference passes [`HOLD_LIMIT`].
#[derive(Default)]
pub struct Stream {
    /// Bytes not yet written: an unfinished reference at the start, and what follows it.
    /// Everything before it was decided, so a token can start at `held[0]`.
    held: Vec<u8>,
    state: State,
}

#[derive(Default, Clone, Copy)]
enum State {
    #[default]
    Scan,
    Wait(Wait),
    /// A reference past [`HOLD_LIMIT`] is dropped up to its closer (or the end of the
    /// token when `close` is `None`), searched up to `from`.
    Drop {
        close: Option<u8>,
        from: usize,
    },
    /// A token past [`HOLD_LIMIT`] that was no reference was written; the rest of it is
    /// written up to a delimiter, a `?` or a `#`, where the scan resumes.
    Tail,
}

/// What decides the reference at the start of the held bytes, searched for up to `from`.
#[derive(Clone, Copy)]
struct Wait {
    on: On,
    from: usize,
}

#[derive(Clone, Copy)]
enum On {
    /// A byte other than a scheme byte, or a space in the span of that delimiter kind:
    /// whether the text is a reference.
    Prefix(Option<usize>),
    /// The closer of the `DELIMITERS[kind]` span.
    Closer(usize),
    /// The end of a token, or with `any` also a delimiter, `?` or `#`.
    Stop { any: bool },
}

impl On {
    fn wakes(self, b: u8) -> bool {
        match self {
            On::Prefix(span) => !(scheme_byte(b) || (span.is_some() && b == b' ')),
            On::Closer(kind) => b == DELIMITERS[kind].1,
            On::Stop { any } => stop(b) || (any && (starts_after(b) || matches!(b, b'?' | b'#'))),
        }
    }
}

impl Wait {
    /// Whether `held` now holds what decides the reference; otherwise records how far it
    /// was searched, so each byte is searched once.
    fn woken(&mut self, held: &[u8]) -> bool {
        self.from = match self.on {
            On::Closer(kind) => match closing(held, self.from, DELIMITERS[kind].1) {
                Ok(_) => return true,
                Err(next) => next,
            },
            on => {
                if held[self.from.min(held.len())..].iter().any(|b| on.wakes(*b)) {
                    return true;
                }
                held.len()
            }
        };
        false
    }
}

impl Stream {
    /// Redacts `input`, the next piece of the text, into `out`.
    pub fn push(&mut self, input: &[u8], out: &mut Vec<u8>) {
        self.held.extend_from_slice(input);
        self.run(false, out);
    }

    /// Whether the text so far ends inside a reference: a separator put here would cut it,
    /// and the part after the separator would print as plain text.
    pub fn inside_reference(&self) -> bool {
        matches!(
            self.state,
            State::Drop { .. }
                | State::Wait(Wait {
                    on: On::Closer(_) | On::Stop { any: false },
                    ..
                })
        )
    }

    /// Ends the text: what is held back is redacted into `out` as the end of the text.
    pub fn finish(&mut self, out: &mut Vec<u8>) {
        self.run(true, out);
        *self = Self::default();
    }

    fn run(&mut self, complete: bool, out: &mut Vec<u8>) {
        loop {
            match self.state {
                State::Drop { close, from } => match terminator(&self.held, from, close) {
                    Ok(end) => {
                        self.drain(end);
                        self.state = State::Scan;
                    }
                    Err(next) => {
                        let all = self.held.len();
                        self.state = State::Drop {
                            close,
                            from: next - all,
                        };
                        self.drain(all);
                        return;
                    }
                },
                State::Tail => {
                    // A `?` or `#` here starts a token of its own, so the scan prints it as
                    // a relative reference.
                    let at = self
                        .held
                        .iter()
                        .position(|b| starts_after(*b) || matches!(b, b'?' | b'#'));
                    let end = at.unwrap_or(self.held.len());
                    out.extend_from_slice(&self.held[..end]);
                    self.drain(end);
                    if at.is_none() {
                        return;
                    }
                    self.state = State::Scan;
                }
                State::Wait(mut wait) if !complete => {
                    if !wait.woken(&self.held) {
                        if self.hold(wait, out) {
                            continue;
                        }
                        return;
                    }
                }
                _ => {}
            }
            match scan(&self.held, complete, out) {
                None => {
                    let all = self.held.len();
                    self.drain(all);
                    self.state = State::Scan;
                    return;
                }
                Some((at, wait)) => {
                    self.drain(at);
                    if !self.hold(wait, out) {
                        return;
                    }
                }
            }
        }
    }

    /// Waits for `wait`, or past [`HOLD_LIMIT`] stops holding (returning `true`): a
    /// reference prints as [`UNPRINTABLE`] and the rest of it is dropped, and anything
    /// that is no reference yet, which holds no `?`, `#` or delimiter, is written.
    fn hold(&mut self, wait: Wait, out: &mut Vec<u8>) -> bool {
        if self.held.len() <= HOLD_LIMIT {
            self.state = State::Wait(wait);
            return false;
        }
        let (keep, close) = match wait.on {
            On::Prefix(_) | On::Stop { any: true } => {
                out.extend_from_slice(&self.held);
                let all = self.held.len();
                self.drain(all);
                self.state = State::Tail;
                return true;
            }
            On::Closer(kind) => (
                1 + self.held[1..].iter().take_while(|b| **b == b' ').count(),
                Some(DELIMITERS[kind].1),
            ),
            On::Stop { any: false } => (0, None),
        };
        out.extend_from_slice(&self.held[..keep]);
        out.extend_from_slice(UNPRINTABLE.as_bytes());
        self.state = State::Drop { close, from: keep };
        true
    }

    /// Forgets the first `n` held bytes, which were written or dropped. The room a long
    /// reference took is given back once nothing is held.
    fn drain(&mut self, n: usize) {
        self.held.drain(..n);
        if self.held.is_empty() && self.held.capacity() > 4 * PIECE {
            self.held = Vec::new();
        }
    }
}

/// Redacts the whole of `text` into `out`.
pub(super) fn whole(text: &[u8], out: &mut Vec<u8>) {
    scan(text, true, out);
}

/// Scans `text` into `out`. Unless the text is `complete`, a
/// reference whose end is not in `text` stops the scan: its start and what it waits for
/// are returned, and nothing from there on is written.
fn scan(text: &[u8], complete: bool, out: &mut Vec<u8>) -> Option<(usize, Wait)> {
    let (mut copied, mut i) = (0, 0);
    let mut next = Next::default();
    while i < text.len() {
        let found = match DELIMITERS.iter().position(|(open, _)| *open == text[i]) {
            Some(kind) => next.span(text, i, kind, complete),
            None if boundary(text, i) => next.token(text, i, complete),
            None => Found::No,
        };
        match found {
            Found::No => i += 1,
            Found::Ref(start, end, rendered) => {
                if rendered.as_bytes() != &text[start..end] {
                    out.extend_from_slice(&text[copied..start]);
                    out.extend_from_slice(rendered.as_bytes());
                    copied = end;
                }
                i = end;
            }
            Found::Wait(on, from) => {
                out.extend_from_slice(&text[copied..i]);
                return Some((i, Wait { on, from: from - i }));
            }
        }
    }
    out.extend_from_slice(&text[copied..]);
    None
}

enum Found {
    No,
    /// A reference: the range it replaces and what it prints as.
    Ref(usize, usize, String),
    /// A reference that ends after the scanned text: what decides it, and where the
    /// search for that resumes.
    Wait(On, usize),
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
    /// The reference delimited by the opener at `i`.
    fn span(&mut self, text: &[u8], i: usize, kind: usize, complete: bool) -> Found {
        // A closer found for an earlier opener also closes this one: the opener lies
        // inside that span, so it is escaped there and the escape parity matches.
        let close = match self.closers[kind] {
            Some(at) if at > i => at,
            _ => *self.closers[kind].insert(closing(text, i + 1, DELIMITERS[kind].1).unwrap_or(text.len())),
        };
        let end = close.min(text.len());
        let start = i + 1 + text[i + 1..end].iter().take_while(|b| **b == b' ').count();
        let reference = reference(&text[start..end]);
        if close < text.len() {
            return match reference {
                Some(true) => Found::Ref(start, close, render_bytes(&text[start..close])),
                _ => Found::No,
            };
        }
        match reference {
            _ if complete => Found::No,
            Some(false) => Found::No,
            None => Found::Wait(On::Prefix(Some(kind)), end),
            Some(true) => Found::Wait(
                On::Closer(kind),
                closing(text, i + 1, DELIMITERS[kind].1).unwrap_or_else(|resume| resume),
            ),
        }
    }

    /// The reference or relative reference starting at `i`.
    fn token(&mut self, text: &[u8], i: usize, complete: bool) -> Found {
        let end = seek(text, i, &mut self.stop, stop);
        let open = end == text.len() && !complete;
        match reference(&text[i..end]) {
            None if open => return Found::Wait(On::Prefix(None), end),
            Some(true) if open => return Found::Wait(On::Stop { any: false }, end),
            Some(true) => return Found::Ref(i, end, render_bytes(&text[i..end])),
            _ => {}
        }
        let mark = seek(text, i, &mut self.mark, |b| matches!(b, b'?' | b'#'));
        // A later delimiter before the `?` or `#` starts the token that is checked
        // instead, so a reference such as `url=//…` keeps its name.
        if (i + 1..mark.min(end)).any(|j| boundary(text, j)) {
            return Found::No;
        }
        if open {
            Found::Wait(On::Stop { any: mark >= end }, end)
        } else if mark >= end || text[mark..end].iter().all(|b| matches!(b, b'?' | b'#')) {
            // `what?` and a Markdown `##` carry no query or fragment.
            Found::No
        } else {
            Found::Ref(i, end, UNPRINTABLE.to_owned())
        }
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
/// `"` or `'` span; when it is missing, the position the search would resume at.
fn closing(text: &[u8], mut i: usize, close: u8) -> Result<usize, usize> {
    while i < text.len() {
        match text[i] {
            b'\\' if matches!(close, b'"' | b'\'') => i += 2,
            b if b == close => return Ok(i),
            _ => i += 1,
        }
    }
    Err(i)
}

/// Where a dropped reference ends: its closer, or the end of its token.
fn terminator(text: &[u8], from: usize, close: Option<u8>) -> Result<usize, usize> {
    match close {
        Some(close) => closing(text, from, close),
        None => text[from.min(text.len())..]
            .iter()
            .position(|b| stop(*b))
            .map(|n| from + n)
            .ok_or(from.max(text.len())),
    }
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

/// Whether `text` starts with `scheme://`, `://`, `//` or `/`; `None` while it could still
/// grow into one.
fn reference(text: &[u8]) -> Option<bool> {
    if text.first() == Some(&b'/') {
        return Some(true);
    }
    let scheme = text.iter().take_while(|b| scheme_byte(**b)).count();
    let rest = &text[scheme..];
    if rest.len() < 3 && b"://".starts_with(rest) {
        return None;
    }
    Some(rest.starts_with(b"://") && (scheme == 0 || text[0].is_ascii_alphabetic()))
}

fn render_bytes(reference: &[u8]) -> String {
    std::str::from_utf8(reference).map_or_else(|_| UNPRINTABLE.to_owned(), url)
}

/// The most a [`Writer`] passes to its [`Stream`] at once.
const PIECE: usize = 8 << 10;

/// A [`std::io::Write`] that passes what it writes through a [`Stream`] before `out`, so
/// no URL reaches `out` unredacted, however the writes split it.
pub struct Writer<W: std::io::Write> {
    out: W,
    stream: Stream,
    buffer: Vec<u8>,
    /// Bytes written to `out` and bytes accepted.
    written: u64,
    accepted: u64,
    ended: bool,
}

impl<W: std::io::Write> Writer<W> {
    pub fn new(out: W) -> Self {
        Self {
            out,
            stream: Stream::default(),
            buffer: Vec::new(),
            written: 0,
            accepted: 0,
            ended: false,
        }
    }

    /// Bytes that reached the inner writer.
    pub fn written(&self) -> u64 {
        self.written
    }

    /// Whether nothing was written to this writer.
    pub fn is_empty(&self) -> bool {
        self.accepted == 0
    }

    /// Ends the text, writing what is held back. Later writes fail, so a reference cut
    /// by the end never reaches `out` in a second piece.
    pub fn end(&mut self) -> std::io::Result<()> {
        if !self.ended {
            self.ended = true;
            self.buffer.clear();
            self.stream.finish(&mut self.buffer);
            self.out.write_all(&self.buffer)?;
            self.written += self.buffer.len() as u64;
        }
        Ok(())
    }

    /// [`Writer::end`], then the inner writer.
    pub fn finish(mut self) -> std::io::Result<W> {
        self.end()?;
        Ok(self.out)
    }
}

impl<W: std::io::Write> std::io::Write for Writer<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.ended {
            return Err(std::io::Error::other("redacted text already ended"));
        }
        // Bounded pieces keep the stream's buffers small for a large write; the stream
        // keeps each reference whole across them.
        for piece in buf.chunks(PIECE) {
            self.buffer.clear();
            self.stream.push(piece, &mut self.buffer);
            self.out.write_all(&self.buffer)?;
            self.written += self.buffer.len() as u64;
            self.accepted += piece.len() as u64;
        }
        Ok(buf.len())
    }

    /// Flushes the inner writer; a held reference stays held until [`Writer::end`].
    fn flush(&mut self) -> std::io::Result<()> {
        self.out.flush()
    }
}
