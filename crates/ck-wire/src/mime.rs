//! Go's `mime.ParseMediaType` and `mime.FormatMediaType`, ported from Go 1.26
//! `mime/mediatype.go`.

use std::collections::BTreeMap;

/// Go `isTokenChar`: an RFC 1521 token byte.
pub fn token_char(c: u8) -> bool {
    c.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`{|}~".contains(&c)
}

fn tspecial(c: u8) -> bool {
    b"()<>@,;:\\\"/[]?=".contains(&c)
}

fn consume_token(v: &str) -> (&str, &str) {
    let end = v.bytes().position(|c| !token_char(c)).unwrap_or(v.len());
    v.split_at(end)
}

/// `consumeValue`: a token or a quoted-string (with MSIE's unescaped backslashes).
fn consume_value(v: &str) -> Option<(String, &str)> {
    let bytes = v.as_bytes();
    if bytes.first() != Some(&b'"') {
        let (token, rest) = consume_token(v);
        return Some((token.to_owned(), rest));
    }
    let mut out = Vec::new();
    let mut i = 1;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => return Some((String::from_utf8_lossy(&out).into_owned(), &v[i + 1..])),
            b'\\' if i + 1 < bytes.len() && tspecial(bytes[i + 1]) => {
                out.push(bytes[i + 1]);
                i += 2;
                continue;
            }
            b'\r' | b'\n' => return None,
            c => out.push(c),
        }
        i += 1;
    }
    None
}

fn trim_left_space(v: &str) -> &str {
    v.trim_start_matches(char::is_whitespace)
}

/// `consumeMediaParam`.
fn consume_param(v: &str) -> Option<(String, String, &str)> {
    let rest = trim_left_space(v).strip_prefix(';')?;
    let (param, rest) = consume_token(trim_left_space(rest));
    if param.is_empty() {
        return None;
    }
    let rest = trim_left_space(trim_left_space(rest).strip_prefix('=')?);
    let (value, rest2) = consume_value(rest)?;
    if value.is_empty() && rest2.len() == rest.len() {
        return None;
    }
    Some((param.to_ascii_lowercase(), value, rest2))
}

fn unhex(c: u8) -> Option<u8> {
    (c as char).to_digit(16).map(|d| d as u8)
}

/// `percentHexUnescape`, as bytes.
fn percent_unescape(s: &str) -> Option<Vec<u8>> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            let (hi, lo) = (unhex(*b.get(i + 1)?)?, unhex(*b.get(i + 2)?)?);
            out.push(hi << 4 | lo);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    Some(out)
}

/// `decode2231Enc`, as bytes.
fn decode_2231(v: &str) -> Option<Vec<u8>> {
    let (charset, rest) = v.split_once('\'')?;
    let (_, value) = rest.split_once('\'')?;
    matches!(charset.to_ascii_lowercase().as_str(), "us-ascii" | "utf-8")
        .then(|| percent_unescape(value))
        .flatten()
}

/// Outcome of `mime.ParseMediaType`.
#[derive(Debug, PartialEq, Eq)]
pub enum MediaType {
    /// No usable media type, with Go's error text.
    Invalid(&'static str),
    /// The media type parsed but its parameters did not (Go returns the type and an error).
    BadParams(String),
    Ok(String, BTreeMap<String, String>),
}

/// `mime.ParseMediaType`.
pub fn parse_media_type(v: &str) -> MediaType {
    let base = v.split(';').next().unwrap_or_default();
    let media = base.to_ascii_lowercase().trim().to_owned();
    // Go `checkMediaTypeDisposition`.
    let (main, rest) = consume_token(&media);
    if main.is_empty() {
        return MediaType::Invalid("mime: no media type");
    }
    if !rest.is_empty() {
        let Some(rest) = rest.strip_prefix('/') else {
            return MediaType::Invalid("mime: expected slash after first token");
        };
        let (sub, tail) = consume_token(rest);
        if sub.is_empty() {
            return MediaType::Invalid("mime: expected token after slash");
        }
        if !tail.is_empty() {
            return MediaType::Invalid("mime: unexpected content after media subtype");
        }
    }
    let mut params = BTreeMap::new();
    let mut continuation: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    let mut v = &v[base.len()..];
    loop {
        v = trim_left_space(v);
        if v.is_empty() {
            break;
        }
        let Some((key, value, rest)) = consume_param(v) else {
            if v.trim() == ";" {
                break;
            }
            return MediaType::BadParams(media);
        };
        let map = match key.split_once('*') {
            Some((base_name, _)) => continuation.entry(base_name.to_owned()).or_default(),
            None => &mut params,
        };
        if map.get(&key).is_some_and(|existing| *existing != value) {
            return MediaType::Invalid("mime: duplicate parameter name");
        }
        map.insert(key, value);
        v = rest;
    }
    // ponytail: Go keeps parameter bytes as they are; values that are not UTF-8 after
    // RFC 2231 decoding are stored lossily here.
    for (key, pieces) in continuation {
        if let Some(v) = pieces.get(&format!("{key}*")) {
            if let Some(decoded) = decode_2231(v) {
                params.insert(key, String::from_utf8_lossy(&decoded).into_owned());
            }
            continue;
        }
        let mut buf = Vec::new();
        let mut valid = false;
        for n in 0.. {
            let simple = format!("{key}*{n}");
            if let Some(v) = pieces.get(&simple) {
                valid = true;
                buf.extend_from_slice(v.as_bytes());
                continue;
            }
            let Some(v) = pieces.get(&format!("{simple}*")) else {
                break;
            };
            valid = true;
            if n == 0 {
                if let Some(decoded) = decode_2231(v) {
                    buf.extend_from_slice(&decoded);
                }
            } else {
                buf.extend_from_slice(&percent_unescape(v).unwrap_or_default());
            }
        }
        if valid {
            params.insert(key, String::from_utf8_lossy(&buf).into_owned());
        }
    }
    MediaType::Ok(media, params)
}

/// `mime.FormatMediaType` for a valid type: parameters sorted, tokens bare, other values
/// quoted, and values outside printable ASCII RFC 2231-encoded. `None` where Go returns "".
pub fn format_media_type(media: &str, params: &BTreeMap<&str, &[u8]>) -> Option<String> {
    let token = |v: &[u8]| !v.is_empty() && v.iter().all(|&c| token_char(c));
    let mut out = media.to_ascii_lowercase();
    for (key, value) in params {
        if !token(key.as_bytes()) {
            return None;
        }
        out.push_str("; ");
        out.push_str(&key.to_ascii_lowercase());
        // `needsEncoding` ranges over runes: invalid UTF-8 decodes as U+FFFD.
        let encode = String::from_utf8_lossy(value)
            .chars()
            .any(|c| !(' '..='~').contains(&c) && c != '\t');
        if encode {
            out.push_str("*=utf-8''");
            for &c in value.iter() {
                if c <= b' ' || c >= 0x7f || b"*'%".contains(&c) || tspecial(c) {
                    out.push_str(&format!("%{c:02X}"));
                } else {
                    out.push(char::from(c));
                }
            }
        } else if token(value) {
            out.push('=');
            out.push_str(&String::from_utf8_lossy(value));
        } else {
            out.push_str("=\"");
            for &c in value.iter() {
                if c == b'"' || c == b'\\' {
                    out.push('\\');
                }
                out.push(char::from(c));
            }
            out.push('"');
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc2231_and_quoted_parameters() {
        assert!(matches!(
            parse_media_type(r#"form-data; name="a\"b"; filename*0="x"; filename*1*=%41"#),
            MediaType::Ok(_, p) if p["name"] == "a\"b" && p["filename"] == "xA"
        ));
    }
}
