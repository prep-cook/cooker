//! The errors Go's `http.Client.Do` reports for a probe request: refused before
//! dialing, failed in transport, or failed while reading the body.

use ck_exec::proxy::SendError;
use ck_exec::xai_url::{self as go_url, GoUrl};
use ck_wire::gostr::quote;

use super::gojson::Header;

/// Go `Transport.roundTrip`'s checks before anything is dialed, in its order: header
/// names and values, the scheme, a host. The error is the `*url.Error` cause.
// ponytail: a URL whose host Go and the WHATWG parser read differently is refused
// instead of being sent to a host Go would not contact.
pub(super) fn go_preflight(url: &str, parsed: &GoUrl, headers: &Header) -> Result<(), String> {
    let is_http = matches!(parsed.scheme.as_str(), "http" | "https");
    if is_http && let Some(invalid) = invalid_header(headers) {
        return Err(format!("net/http: invalid header {invalid}"));
    }
    if !is_http {
        return Err(format!("unsupported protocol scheme {}", quote(&parsed.scheme)));
    }
    if parsed.hostname.is_empty() && parsed.port.is_empty() {
        return Err("http: no Host in request URL".into());
    }
    if !go_url::same_authority(url) {
        return Err("unsupported URL: hosts differ between parsers".into());
    }
    Ok(())
}

/// The `*url.Error` text for a request Go's client refused: `Get "url": cause`. The
/// URL is redacted ([`ck_wire::redact::url`]) where Go masks only a password: a probe
/// URL carries the credential's token after `$TOKEN$` substitution.
pub(super) fn go_url_error_text(method: &str, url: &str, cause: &str) -> String {
    format!("{} {}: {cause}", url_error_op(method), quote(ck_wire::redact::url(url)))
}

/// The error Go's response body reader reports for `err`.
pub(super) fn go_read_error_text(err: &std::io::Error) -> String {
    go_read_error(err)
}

/// The `*url.Error` text Go's `http.Client.Do` returns for a failed send
/// (`Get "url": cause`), URL redacted as in [`go_url_error_text`].
pub(super) fn go_url_error(method: &str, err: SendError) -> String {
    let (url, cause) = send_error_parts(err);
    go_url_error_text(method, &url, &cause)
}

/// Go `urlErrorOp`: the method with only its first letter upper case.
fn url_error_op(method: &str) -> String {
    if method.is_empty() {
        return "Get".into();
    }
    if method.bytes().all(|b| (0x20..0x7f).contains(&b)) {
        let lower = method.to_ascii_lowercase();
        return format!("{}{}", &method[..1], &lower[1..]);
    }
    method.to_owned()
}

/// Go `validateHeaders` on the request: the first invalid name (or value, named by
/// its key) in key order.
fn invalid_header(headers: &Header) -> Option<String> {
    let token = |b: u8| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b);
    headers.iter().find_map(|(name, values)| {
        if name.is_empty() || !name.bytes().all(token) {
            return Some(format!("field name {}", quote(name)));
        }
        values
            .iter()
            .any(|v| v.bytes().any(|b| (b < 0x20 && b != b'\t') || b == 0x7f))
            .then(|| format!("field value for {}", quote(name)))
    })
}

/// The error sources below `err`, looking through `io::Error` wrappers.
fn error_chain<'a>(err: &'a (dyn std::error::Error + 'static)) -> Vec<&'a (dyn std::error::Error + 'static)> {
    let mut out = Vec::new();
    let mut next = Some(err);
    while let Some(e) = next {
        out.push(e);
        next = match e.downcast_ref::<std::io::Error>().and_then(std::io::Error::get_ref) {
            Some(inner) => Some(inner as &(dyn std::error::Error + 'static)),
            None => e.source(),
        };
    }
    out
}

/// The URL Go's client names for a failed exchange, and the cause it reports.
// ponytail: the common transport failures are spelled as Go spells them (a refused
// dial, a connection closed before the response); any other reads as the transport's
// innermost error. A refused dial names the hop's host, which for a host name is not
// the address Go dialed.
fn send_error_parts(err: SendError) -> (String, String) {
    let (err, hop) = match err {
        SendError::Local { error, url } => return (url, String::from_utf8_lossy(&error.body).into_owned()),
        // What `transport_cause` reads in the transport's own timeout (wreq's text).
        SendError::Timeout { url } => return (url, "operation timed out".into()),
        SendError::Transport { error, url } => (error, url),
    };
    let cause = transport_cause(&err, go_url::parse(&hop).ok().as_ref());
    (hop, cause)
}

pub(super) fn transport_cause(err: &wreq::Error, url: Option<&GoUrl>) -> String {
    let chain = error_chain(err);
    for e in &chain {
        if let Some(url) = url
            && e.downcast_ref::<std::io::Error>()
                .is_some_and(|io| io.kind() == std::io::ErrorKind::ConnectionRefused)
        {
            let port = if url.port.is_empty() {
                if url.scheme == "https" { "443" } else { "80" }
            } else {
                url.port.as_str()
            };
            let host = if url.hostname.contains(':') {
                format!("[{}]", url.hostname)
            } else {
                url.hostname.clone()
            };
            return format!("dial tcp {host}:{port}: connect: connection refused");
        }
        if e.to_string().contains("connection closed before message completed") {
            return "EOF".into();
        }
    }
    chain.last().map_or_else(|| err.to_string(), ToString::to_string)
}

/// The error Go's body reader reports.
// ponytail: a truncated body (`unexpected EOF`) and a bad gzip header are spelled as Go
// spells them; any other failure reads as the innermost error.
pub(super) fn go_read_error(err: &std::io::Error) -> String {
    let chain = error_chain(err);
    for e in &chain {
        if e.downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::UnexpectedEof)
        {
            return "unexpected EOF".into();
        }
        if e.to_string().contains("invalid gzip header") {
            return "gzip: invalid header".into();
        }
    }
    chain.last().map_or_else(|| err.to_string(), ToString::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_errors_name_the_method_and_redact_the_url() {
        assert_eq!(
            go_url_error_text("POST", "http://user:secret@127.0.0.1:1/q?t=sk-review-secret", "boom"),
            r#"Post "http://127.0.0.1:1/q?t=***": boom"#
        );
        assert_eq!(
            go_url_error_text("", "http://user@h/x", "boom"),
            r#"Get "http://h/x": boom"#
        );
    }
}
