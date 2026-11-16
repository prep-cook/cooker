//! URLs in error and log text. A configured URL can carry credentials in its userinfo
//! (`https://user:key@host`), its query (`?key=`) or its fragment, so every error,
//! capture and request log that names a URL shows only its scheme, host, port and path,
//! with every query value and a fragment masked (owner ruling Q18, 2026-11-16).

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
/// host.
///
/// A `//` before the query or fragment that does not open `raw` starts a URL of its own
/// (`/r/https://user:key@host/`), so `raw` is cut where that URL's scheme starts, but
/// never inside the authority of the part before it, and each part is rendered alone; a
/// part that cannot be rendered prints as [`UNPRINTABLE`]. A result whose word could
/// still carry a credential prints as [`UNPRINTABLE`] too ([`bytes`]).
pub fn url(raw: &str) -> String {
    let end = raw.find(['?', '#']).unwrap_or(raw.len());
    let mut out = String::new();
    let mut from = 0;
    let mut at = 0;
    while let Some(found) = raw[at..end].find("//") {
        let slashes = at + found;
        at = slashes + 2;
        let (opening, authority_end) = authority(&raw[from..end]);
        let start = match raw[..slashes].strip_suffix(':') {
            Some(head) => head.len() - head.bytes().rev().take_while(|b| scheme_byte(*b)).count(),
            None => slashes,
        };
        let start = start.max(from + authority_end);
        // The `//` that opens the part is its own.
        if slashes < from + opening || start <= from {
            continue;
        }
        out.push_str(&rendered(&raw[from..start]));
        from = start;
    }
    out.push_str(&rendered(&raw[from..]));
    scan::guarded(out)
}

/// Where the `//` opening `part` (after its leading spaces, alone or after scheme bytes,
/// valid scheme or not) ends, and where the authority after it ends; both 0 for a part
/// with no authority.
fn authority(part: &str) -> (usize, usize) {
    let body = part.trim_start_matches(' ');
    let opening = match body.find("//") {
        Some(i)
            if body[..i]
                .strip_suffix(':')
                .unwrap_or(&body[..i])
                .bytes()
                .all(scheme_byte) =>
        {
            part.len() - body.len() + i + 2
        }
        _ => return (0, 0),
    };
    let end = part[opening..]
        .find(['/', '?', '#'])
        .map_or(part.len(), |n| opening + n);
    (opening, end)
}

fn rendered(raw: &str) -> String {
    render(raw).unwrap_or_else(|| UNPRINTABLE.to_owned())
}

/// `text` with every URL reference rendered by [`url`]; see [`bytes`].
pub fn urls_in(text: &str) -> Cow<'_, str> {
    match bytes(text.as_bytes()) {
        Cow::Borrowed(_) => Cow::Borrowed(text),
        // Replacements are ASCII and cut only at ASCII bytes, so UTF-8 stays valid.
        Cow::Owned(out) => {
            Cow::Owned(String::from_utf8(out).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned()))
        }
    }
}

/// `text` (any bytes) with every URL reference rendered by [`url`], for errors, captures
/// and logs. A reference is found two ways:
///
/// - A span delimited by `"…"`, `'…'`, `<…>` or `` `…` `` whose contents, after leading spaces,
///   start with `scheme://`, `//` or `/`: the whole contents are the reference, so a
///   quoted URL holding spaces is redacted whole, and one holding a line break prints as
///   [`UNPRINTABLE`].
/// - Otherwise a token running to the next whitespace or `"` that starts after whitespace,
///   non-ASCII text or a delimiter (`` "'`(<>[{=,;: ``) with `scheme://`, `://`, `//` or `/`. A token that is
///   none of these but carries a query or fragment (`callback?code=…`, `next#…`, `#…`)
///   is a relative reference and prints as [`UNPRINTABLE`] from the last delimiter
///   before its `?` or `#`.
///
/// A reference that is not UTF-8 prints as [`UNPRINTABLE`]. Last, a word of the result
/// (the bytes between whitespace and `"`) that could still carry a credential prints as
/// [`UNPRINTABLE`] whole, whatever the scan found in it: one holding a `//` not followed
/// by a host and port ([`valid_authority`]), or an `@` after a `:` in the same path
/// segment (`redis:key@host`). Other bytes are unchanged. The scan is linear in `text`.
/// A second pass over redacted text can replace more of it with the placeholder.
pub fn bytes(text: &[u8]) -> Cow<'_, [u8]> {
    if !text.iter().any(|b| matches!(b, b'/' | b'?' | b'#' | b'@')) {
        return Cow::Borrowed(text);
    }
    let mut out = Vec::with_capacity(text.len());
    scan::whole(text, &mut out);
    if out == text {
        Cow::Borrowed(text)
    } else {
        Cow::Owned(out)
    }
}

/// Copies `input` to `out` redacted as [`bytes`] redacts the whole text, for a capture
/// too large to hold in memory. Memory stays within a fixed read size and twice the hold
/// limit (1 MiB): a reference or word still unfinished past the limit prints as
/// [`UNPRINTABLE`] and the rest of it is dropped.
pub fn copy(input: &mut dyn std::io::Read, out: &mut dyn std::io::Write) -> std::io::Result<()> {
    scan::copy(input, out, scan::HOLD)
}

/// Writes `text` to `out` redacted as [`copy`] redacts it, without a copy of the whole
/// text.
pub fn write(text: &[u8], out: &mut dyn std::io::Write) -> std::io::Result<()> {
    scan::write(text, out, scan::HOLD)
}

/// Redacts a text that reaches a log in pieces with other text between them, such as
/// the messages of a WebSocket timeline between their event headers. The pieces are
/// redacted as one text, as [`copy`] redacts it, except at each [`Pieces::cut`]: what a
/// piece ends in may go on in the next piece, so a reference, or a word that could still
/// carry a credential, prints as [`UNPRINTABLE`] there, and the start of the next piece
/// is dropped up to where it would end. The text between pieces is redacted on its own,
/// with [`write`].
pub struct Pieces(scan::Copy);

impl Default for Pieces {
    fn default() -> Self {
        Self(scan::Copy::new(scan::HOLD))
    }
}

impl Pieces {
    /// More bytes of the current piece.
    pub fn push(&mut self, bytes: &[u8], out: &mut dyn std::io::Write) -> std::io::Result<()> {
        bytes.chunks(scan::PIECE).try_for_each(|piece| self.0.push(piece, out))
    }

    /// Ends the current piece; the next [`Pieces::push`] starts the next one.
    pub fn cut(&mut self, out: &mut dyn std::io::Write) -> std::io::Result<()> {
        self.0.cut(out)
    }

    /// Ends the text.
    pub fn finish(self, out: &mut dyn std::io::Write) -> std::io::Result<()> {
        self.0.finish(out)
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

/// Whether a word of redacted text could still carry a credential (see [`bytes`]).
fn suspect(word: &[u8]) -> bool {
    let mut colon = false;
    for (i, &b) in word.iter().enumerate() {
        match b {
            b'/' => {
                colon = false;
                if word.get(i + 1) == Some(&b'/') {
                    let rest = &word[i + 2..];
                    let host = &rest[..rest
                        .iter()
                        .position(|b| matches!(b, b'/' | b'?' | b'#'))
                        .unwrap_or(rest.len())];
                    if !std::str::from_utf8(host).is_ok_and(valid_authority) {
                        return true;
                    }
                }
            }
            b':' => colon = true,
            b'@' if colon => return true,
            _ => {}
        }
    }
    false
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
