//! URLs in error and log text. A configured URL can carry credentials in its userinfo
//! (`https://user:key@host`), its query (`?key=`) or its fragment, so every error,
//! capture and request log that names a URL shows only its scheme, host, port and path,
//! with every query value and a fragment masked (owner ruling Q18, 2026-11-16).

use std::borrow::Cow;

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

/// `text` with every URL rendered by [`url`]: each `scheme://` token, and each `//`
/// token at the start of the text or after whitespace, `"`, `'`, `(` or `<`. A token
/// opened by `"` runs to the closing `"` (Go quotes URLs in its errors, and a quoted URL
/// can hold spaces); any other token runs to the next whitespace or `"`, so a URL
/// followed by other punctuation loses that punctuation rather than printing part of a
/// secret.
pub fn urls_in(text: &str) -> Cow<'_, str> {
    if !text.contains("//") {
        return Cow::Borrowed(text);
    }
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = next_url(rest) {
        let quoted = start > 0 && rest.as_bytes()[start - 1] == b'"';
        let len = if quoted {
            closing_quote(&rest[start..])
        } else {
            rest[start..].find(|c: char| c.is_whitespace() || c == '"')
        };
        let end = len.map_or(rest.len(), |len| start + len);
        out.push_str(&rest[..start]);
        out.push_str(&url(&rest[start..end]));
        rest = &rest[end..];
    }
    out.push_str(rest);
    Cow::Owned(out)
}

/// Where the next URL token in `text` starts.
fn next_url(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut from = 0;
    while let Some(i) = text[from..].find("//").map(|i| from + i) {
        if i > 0 && bytes[i - 1] == b':' {
            let mut start = i - 1;
            while start > 0 && scheme_byte(bytes[start - 1]) {
                start -= 1;
            }
            return Some(start);
        }
        if i == 0 || bytes[i - 1].is_ascii_whitespace() || matches!(bytes[i - 1], b'"' | b'\'' | b'(' | b'<') {
            return Some(i);
        }
        from = i + 2;
    }
    None
}

/// The length of a Go-quoted string's contents: up to the first `"` not escaped by `\`.
fn closing_quote(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => i += 2,
            b'"' => return Some(i),
            _ => i += 1,
        }
    }
    None
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

/// The logged copy of header `name` (lowercase) when its value is or holds a URL:
/// `Location`, `Content-Location`, `Referer`, `Origin` and the rewrite headers as
/// [`url`] renders them, each `<target>` and URL-like parameter of a `Link`, and the URL
/// of a `Refresh`. `None` for every other header. A value that is not UTF-8, or one whose
/// syntax cannot be followed, prints as [`UNPRINTABLE`].
pub fn url_header(name: &str, value: &[u8]) -> Option<String> {
    let render: fn(&str) -> String = match name {
        "location" | "content-location" | "referer" | "origin" | "x-original-url" | "x-rewrite-url"
        | "x-forwarded-uri" => url,
        "link" => link,
        "refresh" => refresh,
        _ => return None,
    };
    Some(std::str::from_utf8(value).map_or_else(|_| UNPRINTABLE.to_owned(), render))
}

/// `Link`: every `<target>`, and every parameter value that looks like a URL (`anchor`).
fn link(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut rest = value;
    while let Some(i) = rest.find(['<', '"', '=']) {
        out.push_str(&rest[..=i]);
        let after = &rest[i + 1..];
        let (field, close, tail) = match rest.as_bytes()[i] {
            b'<' => match after.find('>') {
                Some(end) => (url(&after[..end]), ">", &after[end + 1..]),
                None => return out + UNPRINTABLE,
            },
            b'"' => match after.find('"') {
                Some(end) if !after[..end].contains('\\') => (link_param(&after[..end]), "\"", &after[end + 1..]),
                _ => return out + UNPRINTABLE,
            },
            _ => {
                let value = after.trim_start();
                if value.starts_with('"') {
                    out.push_str(&after[..after.len() - value.len()]);
                    rest = value;
                    continue;
                }
                let end = after.find([';', ',']).unwrap_or(after.len());
                (link_param(&after[..end]), "", &after[end..])
            }
        };
        out.push_str(&field);
        out.push_str(close);
        rest = tail;
    }
    out + rest
}

fn link_param(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.contains(['?', '#', '@']) || trimmed.contains("//") || trimmed.starts_with('/') {
        let lead = &value[..value.len() - value.trim_start().len()];
        format!("{lead}{}", url(trimmed))
    } else {
        value.to_owned()
    }
}

/// `Refresh: <delay>[; url=<target>]`, written back as `<delay>; url=<target>` with the
/// target rendered by [`url`]. Spaces around `url` and `=`, and quotes, are accepted.
fn refresh(value: &str) -> String {
    let Some(i) = value.find([';', ',']) else {
        let delay = value.trim().bytes().all(|b| b.is_ascii_digit() || b == b'.');
        return if delay {
            value.to_owned()
        } else {
            UNPRINTABLE.to_owned()
        };
    };
    let mut target = value[i + 1..].trim_start();
    if target.get(..3).is_some_and(|t| t.eq_ignore_ascii_case("url"))
        && let Some(rest) = target[3..].trim_start().strip_prefix('=')
    {
        target = rest;
    }
    let target = target.trim().trim_matches(['"', '\'']);
    format!("{}; url={}", &value[..i], url(target))
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
