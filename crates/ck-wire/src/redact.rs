//! URLs in error and log text. A configured URL can carry credentials in its userinfo
//! (`https://user:key@host`) or query (`?api_key=`), so every error, capture and
//! management response that names a URL shows only its scheme, host, port and path.

use std::borrow::Cow;

/// What each query value and a fragment print as.
pub const MASK: &str = "***";

/// What a URL prints as when it cannot be split into those parts safely.
pub const UNPRINTABLE: &str = "<redacted url>";

/// `raw` for error and log text: the userinfo dropped, and every query value and the
/// fragment masked. [`UNPRINTABLE`] replaces a URL with control characters, one whose
/// authority is not a host and numeric port while the URL holds an '@', and one without
/// a `scheme://` or `//` authority that holds an '@' before its query (Go reads
/// `user:key@host` as scheme `user`).
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

fn render(raw: &str) -> Option<String> {
    if raw.bytes().any(|b| b < b' ' || b == 0x7f) {
        return None;
    }
    // Leading spaces print as written: Go's errors quote such a URL unchanged.
    let body = raw.trim_start_matches(' ');
    let lead = &raw[..raw.len() - body.len()];
    let (prefix, rest) = match body.find("://") {
        Some(i) if is_scheme(&body[..i]) => body.split_at(i + 3),
        _ if body.starts_with("//") => body.split_at(2),
        // No authority (`relay.example/v1`, `https:relay.example`): nothing to drop
        // unless an '@' stands where Go reads an opaque `user:key@host`.
        _ => {
            let head = &body[..body.find(['?', '#']).unwrap_or(body.len())];
            return (!head.contains('@')).then(|| [lead, head, &masked_tail(&body[head.len()..])].concat());
        }
    };
    // The userinfo ends at the last '@' before the path. The search runs past '?' and
    // '#': a password holding them unescaped is still dropped, and dropping too much is
    // safer than printing a secret.
    let path_start = rest.find('/').unwrap_or(rest.len());
    let rest = rest[..path_start].rfind('@').map_or(rest, |at| &rest[at + 1..]);
    let (authority, tail) = rest.split_at(rest.find(['/', '?', '#']).unwrap_or(rest.len()));
    // Without an '@' nothing can be userinfo, so an invalid port prints as written
    // (`https://auth.x.ai:80:90`); with one, `user:key/x@host` leaves `user:key` here.
    if raw.contains('@') && !valid_authority(authority) {
        return None;
    }
    let (path, tail) = tail.split_at(tail.find(['?', '#']).unwrap_or(tail.len()));
    Some([lead, prefix, authority, path, &masked_tail(tail)].concat())
}

/// A `?query#fragment` tail with every query value and the fragment masked.
fn masked_tail(tail: &str) -> String {
    let (tail, fragment) = tail.split_once('#').map_or((tail, None), |(t, f)| (t, Some(f)));
    let mut out = String::new();
    if let Some(query) = tail.strip_prefix('?') {
        out.push('?');
        for (i, part) in query.split('&').enumerate() {
            if i > 0 {
                out.push('&');
            }
            match part.split_once('=') {
                Some((name, _)) => {
                    out.push_str(name);
                    out.push('=');
                    out.push_str(MASK);
                }
                // A bare entry is all value.
                None if !part.is_empty() => out.push_str(MASK),
                None => {}
            }
        }
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
mod tests {
    use super::*;

    const SECRET: &str = "sk-review-secret";

    #[test]
    fn credentials_never_print() {
        for (raw, want) in [
            (
                "https://user:sk-review-secret@host.example:8443/v1",
                "https://host.example:8443/v1",
            ),
            ("https://:sk-review-secret@host.example/v1", "https://host.example/v1"),
            ("https://sk-review-secret@host.example", "https://host.example"),
            (
                "http://127.0.0.1:9/v1?api_key=sk-review-secret&alt=sse&sk-review-secret&&x=",
                "http://127.0.0.1:9/v1?api_key=***&alt=***&***&&x=***",
            ),
            (
                "https://host.example/cb#access_token=sk-review-secret",
                "https://host.example/cb#***",
            ),
            ("https://host.example/p?#", "https://host.example/p?"),
            ("//host.example/x?k=sk-review-secret", "//host.example/x?k=***"),
            ("https://[::1]:8443/x", "https://[::1]:8443/x"),
            // A password holding unescaped '?', '#' or '@' is still userinfo.
            ("https://u:sk?review#sec@ret@host.example/x", "https://host.example/x"),
            // Path segments keep their '@' (Vertex model versions).
            (
                "https://h.example/models/claude@001:rawPredict",
                "https://h.example/models/claude@001:rawPredict",
            ),
            (" HTTP://127.0.0.1:9/v1 ", " HTTP://127.0.0.1:9/v1 "),
            // Without an '@' nothing is userinfo: Go's own refusals print as written.
            (
                "https:relay.example/x?key=sk-review-secret",
                "https:relay.example/x?key=***",
            ),
            ("relay.example/v1#sk-review-secret", "relay.example/v1#***"),
            ("https://auth.x.ai:80:90/t", "https://auth.x.ai:80:90/t"),
        ] {
            assert_eq!(url(raw), want, "{raw}");
        }
    }

    #[test]
    fn unparseable_urls_print_the_placeholder() {
        for raw in [
            "http://user:sk-review-secret@127.0.0.1:bad/?api_key=sk-review-secret",
            // Go reads host `u`, port `sk-review-secret`: the password before a '/'.
            "http://u:sk-review-secret/x@host.example/",
            "https:sk-review-secret@relay.example",
            "user:sk-review-secret@relay.example/v1",
            "http://h.example/\nsk-review-secret",
            "http://u@[::1/sk-review-secret",
        ] {
            assert_eq!(url(raw), UNPRINTABLE, "{raw:?}");
        }
    }

    #[test]
    fn urls_inside_text_are_rendered_in_place() {
        let text = format!(
            r#"Post "http://u:{SECRET}@127.0.0.1:1/v1?key={SECRET}": EOF; retry ftp://{SECRET}@h/ see ://{SECRET}@h"#
        );
        let got = urls_in(&text);
        assert_eq!(
            got,
            r#"Post "http://127.0.0.1:1/v1?key=***": EOF; retry ftp://h/ see <redacted url>"#
        );
        assert!(!got.contains(SECRET));
        assert!(matches!(urls_in("no urls here"), Cow::Borrowed(_)));
        // The token runs past ')' and ':' so no part of the query value survives.
        let wreq = format!("error sending request for uri (http://h/?api_key={SECRET}/x): client error");
        assert_eq!(
            urls_in(&wreq),
            "error sending request for uri (http://h/?api_key=*** client error"
        );
    }
}
