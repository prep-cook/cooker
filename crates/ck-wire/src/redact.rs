//! URLs in error and log text. A configured URL can carry credentials in its userinfo
//! (`https://user:key@host`), its query (`?key=`) or its fragment, so every error,
//! capture and request log that names a URL shows only its scheme, host, port and path,
//! with every query value and a fragment masked (owner ruling Q18, 2026-11-15).

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

/// `text` with every `scheme://` token rendered by [`url`]. A token runs to the next
/// whitespace or `"`, so a URL followed by other punctuation loses that punctuation
/// rather than printing part of a secret.
pub fn urls_in(text: &str) -> Cow<'_, str> {
    if !text.contains("://") {
        return Cow::Borrowed(text);
    }
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(i) = rest.find("://") {
        let bytes = rest.as_bytes();
        let mut start = i;
        while start > 0 && scheme_byte(bytes[start - 1]) {
            start -= 1;
        }
        let end = rest[i..]
            .find(|c: char| c.is_whitespace() || c == '"')
            .map_or(rest.len(), |e| i + e);
        out.push_str(&rest[..start]);
        out.push_str(&url(&rest[start..end]));
        rest = &rest[end..];
    }
    out.push_str(rest);
    Cow::Owned(out)
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
/// [`url`] renders them, each `<target>` of a `Link`, and the `url=` of a `Refresh`.
/// `None` for every other header. A value that is not UTF-8 prints as [`UNPRINTABLE`].
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

fn link(value: &str) -> String {
    let mut out = String::new();
    let mut rest = value;
    while let Some(open) = rest.find('<') {
        let Some(len) = rest[open..].find('>') else {
            out.push_str(&urls_in(&rest[..open]));
            out.push_str(UNPRINTABLE);
            return out;
        };
        out.push_str(&urls_in(&rest[..=open]));
        out.push_str(&url(&rest[open + 1..open + len]));
        out.push('>');
        rest = &rest[open + len + 1..];
    }
    out.push_str(&urls_in(rest));
    out
}

fn refresh(value: &str) -> String {
    match value.to_ascii_lowercase().find("url=") {
        Some(i) => {
            let target = value[i + 4..].trim_matches(|c| matches!(c, ' ' | '"' | '\''));
            format!("{}{}", &value[..i + 4], url(target))
        }
        None => value.to_owned(),
    }
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

/// A host (name, IPv4 or bracketed IPv6) and an optional numeric port. Anything else,
/// such as `user:password` left by a password holding an unescaped `/`, is refused.
fn valid_authority(authority: &str) -> bool {
    let (host, port) = match authority.strip_prefix('[') {
        Some(inner) => {
            let Some((ip, after)) = inner.split_once(']') else {
                return false;
            };
            let ip_ok = ip
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b':' | b'.' | b'%' | b'-' | b'_'));
            if !ip_ok {
                return false;
            }
            ("", after)
        }
        None => authority.find(':').map_or((authority, ""), |i| authority.split_at(i)),
    };
    let host_ok = host
        .bytes()
        .all(|b| b >= 0x80 || b.is_ascii_alphanumeric() || b"-._~%!$&'()*+,;=".contains(&b));
    let port_ok = port.is_empty()
        || port
            .strip_prefix(':')
            .is_some_and(|p| p.bytes().all(|b| b.is_ascii_digit()));
    host_ok && port_ok
}

#[cfg(test)]
#[path = "redact_tests.rs"]
mod tests;
