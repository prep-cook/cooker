//! URLs in error and log text. A configured URL can carry credentials in its userinfo
//! (`https://user:key@host`), its query (`?key=`) or its fragment. [`url`] renders a
//! URL the caller names with only its scheme, host, port and path, every query value and
//! a fragment masked (owner ruling Q18). Free text ([`bytes`], [`copy`], [`Pieces`]) is
//! redacted word by word: a word that may hold a credential prints as [`UNPRINTABLE`]
//! whole (owner ruling Settled 60, 2026-11-16).

use std::borrow::Cow;

#[path = "redact_scan.rs"]
mod scan;

/// What each query value and a fragment print as.
pub const MASK: &str = "***";

/// What a URL prints as when it cannot be split into those parts safely.
pub const UNPRINTABLE: &str = "<redacted url>";

/// `raw` for error and log text: the userinfo dropped, and every query value and a
/// non-empty fragment masked. A path-absolute reference (`/v1?key=`) keeps its path.
/// [`UNPRINTABLE`] replaces anything else that is not a `scheme://` or `//` URL with a
/// host and numeric port: control characters, a bare key or host, an opaque URL
/// (`https:key`), a malformed authority, and an '@' after the authority but before the
/// path, where a password holding an unescaped '?' or '#' would leave its start as the
/// host. A `//` before the query or fragment other than the one that opens `raw` may
/// start a URL of its own (`/r/https://user:key@host/`, `https://user:key://host`), so
/// `raw` prints as [`UNPRINTABLE`] then too.
pub fn url(raw: &str) -> String {
    let end = raw.find(['?', '#']).unwrap_or(raw.len());
    let body = raw.trim_start_matches(' ');
    let opening = match body.find("//") {
        Some(i) if i == 0 || body[..i].strip_suffix(':').is_some_and(is_scheme) => raw.len() - body.len() + i + 2,
        _ => 0,
    };
    if raw.get(opening..end).is_some_and(|rest| rest.contains("//")) {
        return UNPRINTABLE.to_owned();
    }
    render(raw).unwrap_or_else(|| UNPRINTABLE.to_owned())
}

/// `text` redacted word by word; see [`bytes`].
pub fn urls_in(text: &str) -> Cow<'_, str> {
    match bytes(text.as_bytes()) {
        Cow::Borrowed(_) => Cow::Borrowed(text),
        // Words are cut and replaced at ASCII bytes, so UTF-8 stays valid.
        Cow::Owned(out) => {
            Cow::Owned(String::from_utf8(out).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned()))
        }
    }
}

/// `text` (any bytes) redacted word by word, for errors, captures and logs (owner ruling
/// Settled 60, 2026-11-16). A word is the bytes between whitespace and `"`. A word prints
/// as [`UNPRINTABLE`] whole when it holds `://`, an `@`, a `?` or a `#`, or a scheme (a
/// letter, then letters, digits, `+`, `-` or `.`) and `:` with a byte after them
/// (`https:key`, `example.com:443`), and when it is longer than 1 MiB. Such
/// a word may be part of a quoted value with spaces in it, so when it follows a `"` and
/// ends at whitespace, the text up to the next `"` is dropped; when it holds `'`, a
/// backtick or `<`, the text up to the next `"` or line break. No URL structure is
/// parsed, so an e-mail address, `user@host`, `mailto:…`, `name:port` and a word such as
/// `what?` or `C#` print as the placeholder too.
pub fn bytes(text: &[u8]) -> Cow<'_, [u8]> {
    if text.len() <= scan::HOLD && !text.iter().any(|b| matches!(b, b':' | b'@' | b'?' | b'#')) {
        return Cow::Borrowed(text);
    }
    let mut out = Vec::with_capacity(text.len());
    let mut words = scan::Words::new(scan::HOLD);
    words.push(text, &mut out);
    words.finish(&mut out);
    if out == text {
        Cow::Borrowed(text)
    } else {
        Cow::Owned(out)
    }
}

/// Copies `input` to `out` redacted as [`bytes`] redacts it, holding back only the word
/// being read, for a capture too large to hold in memory.
pub fn copy(input: &mut dyn std::io::Read, out: &mut dyn std::io::Write) -> std::io::Result<()> {
    scan::copy(input, out, scan::HOLD)
}

/// Writes `text` to `out` redacted as [`bytes`] redacts it, without a copy of the whole
/// text.
pub fn write(text: &[u8], out: &mut dyn std::io::Write) -> std::io::Result<()> {
    scan::write(text, out, scan::HOLD)
}

/// Redacts a text that reaches a log in pieces with other text between them, such as
/// the messages of a WebSocket timeline between their event headers. The pieces are
/// redacted as one text, as [`copy`] redacts it, except at each [`Pieces::cut`]: the
/// next piece may go on with the word a piece ends in, so that word prints as
/// [`UNPRINTABLE`] when it is suspect or its last byte can be part of a URL, and the
/// next piece is dropped up to its end. The text between pieces is redacted on its own,
/// with [`write`].
pub struct Pieces {
    words: scan::Words,
    written: Vec<u8>,
}

impl Default for Pieces {
    fn default() -> Self {
        Self::holding(scan::HOLD)
    }
}

impl Pieces {
    fn holding(hold: usize) -> Self {
        Self {
            words: scan::Words::new(hold),
            written: Vec::new(),
        }
    }

    /// More bytes of the current piece.
    pub fn push(&mut self, bytes: &[u8], out: &mut dyn std::io::Write) -> std::io::Result<()> {
        self.words.push(bytes, &mut self.written);
        self.flush(out)
    }

    /// Ends the current piece; the next [`Pieces::push`] starts the next one.
    pub fn cut(&mut self, out: &mut dyn std::io::Write) -> std::io::Result<()> {
        self.words.cut(&mut self.written);
        self.flush(out)
    }

    /// Ends the text.
    pub fn finish(mut self, out: &mut dyn std::io::Write) -> std::io::Result<()> {
        self.words.finish(&mut self.written);
        self.flush(out)
    }

    fn flush(&mut self, out: &mut dyn std::io::Write) -> std::io::Result<()> {
        out.write_all(&self.written)?;
        self.written.clear();
        Ok(())
    }
}

#[cfg(test)]
fn copy_holding(input: &mut dyn std::io::Read, out: &mut dyn std::io::Write, hold: usize) -> std::io::Result<()> {
    scan::copy(input, out, hold)
}

/// A raw query (without its `?`) with every value masked: `name=value` prints as
/// `name=***`, and an entry with no value (`token`, `token=`) prints as `***`, since a
/// bare entry can itself be the key. Empty entries stay empty.
pub fn query(raw: &str) -> String {
    raw.split('&')
        .map(|part| match part.split_once('=') {
            Some((name, value)) if !value.is_empty() => format!("{name}={MASK}"),
            _ if part.is_empty() => String::new(),
            _ => MASK.to_owned(),
        })
        .collect::<Vec<_>>()
        .join("&")
}

fn render(raw: &str) -> Option<String> {
    if raw.bytes().any(|b| b < b' ' || b == 0x7f) {
        return None;
    }
    // Leading spaces print as written: Go's errors quote such a URL unchanged.
    let body = raw.trim_start_matches(' ');
    let lead = &raw[..raw.len() - body.len()];
    if body.starts_with('/') && !body.starts_with("//") {
        let (path, tail) = body.split_at(body.find(['?', '#']).unwrap_or(body.len()));
        return Some([lead, path, &masked_tail(tail)].concat());
    }
    let (prefix, rest) = match body.find("://") {
        Some(i) if is_scheme(&body[..i]) => body.split_at(i + 3),
        _ if body.starts_with("//") => body.split_at(2),
        _ => return None,
    };
    // The authority ends at the first '/', '?' or '#', as Go and WHATWG read it; query
    // and fragment bytes never become the host.
    let (authority, tail) = rest.split_at(rest.find(['/', '?', '#']).unwrap_or(rest.len()));
    if tail[..tail.find('/').unwrap_or(tail.len())].contains('@') {
        return None;
    }
    let host = authority.rfind('@').map_or(authority, |at| &authority[at + 1..]);
    if !valid_authority(host) {
        return None;
    }
    let (path, tail) = tail.split_at(tail.find(['?', '#']).unwrap_or(tail.len()));
    Some([lead, prefix, host, path, &masked_tail(tail)].concat())
}

/// A `?query#fragment` tail with every query value and a non-empty fragment masked.
fn masked_tail(tail: &str) -> String {
    let (tail, fragment) = tail.split_once('#').map_or((tail, None), |(t, f)| (t, Some(f)));
    let mut out = String::new();
    if let Some(raw) = tail.strip_prefix('?') {
        out.push('?');
        out.push_str(&query(raw));
    }
    if fragment.is_some_and(|f| !f.is_empty()) {
        out.push('#');
        out.push_str(MASK);
    }
    out
}

fn scheme_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'+' | b'-' | b'.')
}

fn is_scheme(s: &str) -> bool {
    s.as_bytes().first().is_some_and(u8::is_ascii_alphabetic) && s.bytes().all(scheme_byte)
}

/// A host and an optional numeric port. The host is a bracketed IPv6 address (with an
/// optional `%25` zone), or a name or IPv4 address whose escapes decode only to
/// unreserved or non-ASCII bytes. Anything else, such as `user:password` left by a
/// password holding an unescaped `/`, `[SECRET]` or `SECRET%40host`, is refused.
fn valid_authority(authority: &str) -> bool {
    let (host_ok, port) = match authority.strip_prefix('[') {
        Some(inner) => match inner.split_once(']') {
            Some((ip, port)) => (ipv6_literal(ip), port),
            None => return false,
        },
        None => {
            let (host, port) = authority.split_at(authority.find(':').unwrap_or(authority.len()));
            (reg_name(host), port)
        }
    };
    let port_ok = port.is_empty()
        || port
            .strip_prefix(':')
            .is_some_and(|p| p.bytes().all(|b| b.is_ascii_digit()));
    host_ok && port_ok
}

fn ipv6_literal(ip: &str) -> bool {
    let (addr, zone) = ip.split_once("%25").unwrap_or((ip, ""));
    addr.parse::<std::net::Ipv6Addr>().is_ok() && zone.bytes().all(unreserved)
}

fn reg_name(host: &str) -> bool {
    let bytes = host.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'%' {
            let Some(hex) = bytes.get(i + 1..i + 3).filter(|h| h.iter().all(u8::is_ascii_hexdigit)) else {
                return false;
            };
            let decoded = hex
                .iter()
                .fold(0u8, |v, h| (v << 4) | (*h as char).to_digit(16).unwrap_or(0) as u8);
            if !(decoded >= 0x80 || unreserved(decoded)) {
                return false;
            }
            i += 3;
        } else if b >= 0x80 || unreserved(b) || b"!$&'()*+,;=".contains(&b) {
            i += 1;
        } else {
            return false;
        }
    }
    true
}

fn unreserved(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~')
}

#[cfg(test)]
#[path = "redact_tests.rs"]
mod tests;
