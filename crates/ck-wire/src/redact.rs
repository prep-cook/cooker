//! URLs in error and log text. A configured URL can carry credentials in its userinfo
//! (`https://user:key@host`), its query (`?key=`) or its fragment, so every error,
//! capture and request log that names a URL shows only its scheme, host, port and path,
//! with every query value and a fragment masked (owner ruling Q18, 2026-11-16).

use std::borrow::Cow;

#[path = "redact_stream.rs"]
mod stream;
pub use stream::{HOLD_LIMIT, Stream, Writer};

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
pub fn url(raw: &str) -> String {
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
///   non-ASCII text or a delimiter (`` "'`(<[{=,;: ``) with `scheme://`, `://`, `//` or `/`. A token that is
///   none of these but carries a query or fragment (`callback?code=…`, `next#…`, `#…`)
///   is a relative reference and prints as [`UNPRINTABLE`] from the last delimiter
///   before its `?` or `#`.
///
/// A reference that is not UTF-8 prints as [`UNPRINTABLE`]; other bytes are unchanged.
/// The scan is linear in `text`, and a reference is passed over whole, so text that is
/// already redacted comes back unchanged.
pub fn bytes(text: &[u8]) -> Cow<'_, [u8]> {
    if !text.iter().any(|b| matches!(b, b'/' | b'?' | b'#')) {
        return Cow::Borrowed(text);
    }
    let mut out = Vec::with_capacity(text.len());
    stream::whole(text, &mut out);
    if out == text {
        Cow::Borrowed(text)
    } else {
        Cow::Owned(out)
    }
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
