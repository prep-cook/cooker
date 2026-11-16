use super::*;

/// Go's TTFT tracking on the response body (`usageTTFTReadCloser`): the first read that
/// returns bytes marks the first response byte, or with `packet_only` (Go
/// `TrackHTTPClientRoundTripOnly`) a first non-token frame. Call
/// `usage.round_trip_started()` before sending the request.
pub(crate) fn track_first_byte(
    body: futures_util::stream::BoxStream<'static, Result<bytes::Bytes, ExecError>>,
    usage: &ck_core::exec::UsageSink,
    packet_only: bool,
) -> futures_util::stream::BoxStream<'static, Result<bytes::Bytes, ExecError>> {
    use futures_util::StreamExt;
    if !usage.enabled() {
        return body;
    }
    let (usage, mut marked) = (usage.clone(), false);
    body.inspect(move |item| {
        if !marked && item.as_ref().is_ok_and(|chunk| !chunk.is_empty()) {
            marked = true;
            if packet_only {
                usage.token_event(false);
            } else {
                usage.first_byte();
            }
        }
    })
    .boxed()
}

/// Go `http.Response.Header` for an HTTP/1.1 upstream response (net/http
/// `ReadResponse`): canonical names, every value in order, with `Transfer-Encoding`
/// moved out; `Connection` dropped when it carries `close`; `Trailer` and
/// `Content-Length` dropped from chunked responses (Content-Length kept for statuses
/// without a body); identical duplicate `Content-Length` values collapsed; and
/// `Cache-Control: no-cache` added for `Pragma: no-cache`.
// ponytail: the capture's view only; proxy::Upstream keeps the transport's headers for
// every other use.
pub(crate) fn go_response_headers(status: u16, headers: &http::HeaderMap) -> Vec<(String, String)> {
    use http::header::{CACHE_CONTROL, CONNECTION, CONTENT_LENGTH, PRAGMA, TRAILER, TRANSFER_ENCODING};
    let chunked = headers.contains_key(TRANSFER_ENCODING);
    let close = headers.get_all(CONNECTION).iter().any(|v| {
        v.as_bytes()
            .split(|b| *b == b',')
            .any(|t| t.trim_ascii().eq_ignore_ascii_case(b"close"))
    });
    let has_body = !(status / 100 == 1 || status == 204 || status == 304);
    let mut length_seen = false;
    let mut out: Vec<(String, String)> = headers
        .iter()
        .filter(|(name, _)| match *name {
            n if n == TRANSFER_ENCODING => false,
            n if n == CONNECTION => !close,
            n if n == TRAILER => !chunked,
            n if n == CONTENT_LENGTH => !(chunked && has_body) && !std::mem::replace(&mut length_seen, true),
            _ => true,
        })
        .map(|(name, value)| {
            (
                crate::proxy::canonical_header(name.as_str()),
                String::from_utf8_lossy(value.as_bytes()).into_owned(),
            )
        })
        .collect();
    if headers.get(PRAGMA).is_some_and(|v| v.as_bytes() == b"no-cache") && !headers.contains_key(CACHE_CONTROL) {
        out.push(("Cache-Control".into(), "no-cache".into()));
    }
    out
}

/// Go `helps.RecordAPIRequest` at an executor's send site: `headers` as Go's request
/// holds them, the auth fields as that call site fills them.
pub(crate) fn capture_request(
    capture: &ck_core::exec::CaptureSink,
    credential: &Credential,
    url: &str,
    headers: &crate::proxy::GoHeaders,
    body: &[u8],
    provider: &str,
    (auth_type, auth_value): (&str, &str),
) {
    if !capture.enabled() {
        return;
    }
    capture.record(ck_core::exec::CaptureEvent::Request(ck_core::exec::UpstreamRequest {
        url,
        method: "POST",
        headers: headers.pairs(),
        body,
        provider,
        auth_id: &credential.id,
        auth_label: &credential.label,
        auth_type,
        auth_value,
    }));
}

/// Go `helps.RecordAPIResponseMetadata(status, header)`.
pub(crate) fn capture_metadata(capture: &ck_core::exec::CaptureSink, status: u16, headers: &http::HeaderMap) {
    if capture.enabled() {
        let headers = go_response_headers(status, headers);
        capture.record(ck_core::exec::CaptureEvent::ResponseMetadata(status, &headers));
    }
}

/// Go `helps.AppendAPIResponseChunk`.
pub(crate) fn capture_chunk(capture: &ck_core::exec::CaptureSink, chunk: &[u8]) {
    capture.record(ck_core::exec::CaptureEvent::ResponseChunk(chunk));
}

/// Go `helps.RecordAPIResponseError(err)` with the error's text.
// ponytail: transport errors carry this crate's message, not Go's `Post "url": ...`
// wording; upgrade by keeping the client error text on ExecError.
pub(crate) fn capture_error(capture: &ck_core::exec::CaptureSink, error: &ExecError) {
    if capture.enabled() {
        capture.record(ck_core::exec::CaptureEvent::ResponseError(&String::from_utf8_lossy(
            &error.body,
        )));
    }
}

/// Go's per-line `AppendAPIResponseChunk(line)` in a scanner loop, and the scan error
/// it records after the loop.
// ponytail: the scan error is recorded when the line reader reports it; Go skips the
// record when an apply_patch end check already failed the stream (both failures in one
// stream).
pub(crate) fn capture_lines(
    lines: ck_core::exec::ExecStream,
    capture: &ck_core::exec::CaptureSink,
) -> ck_core::exec::ExecStream {
    use futures_util::StreamExt;
    if !capture.enabled() {
        return lines;
    }
    let capture = capture.clone();
    lines
        .inspect(move |item| match item {
            Ok(line) => capture_chunk(&capture, line),
            Err(error) => capture_error(&capture, error),
        })
        .boxed()
}

/// A control-plane failure that must not echo upstream bodies (they may contain tokens).
pub(crate) fn auth_error(status: u16, message: impl Into<String>) -> ExecError {
    ExecError::local(status, FailureScope::Credential, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_headers_take_go_response_header_shape() {
        // Expected values from Go 1.26 http.ReadResponse on the same raw responses.
        let check = |status: u16, raw: &[(&str, &str)], want: &[&str]| {
            let mut h = http::HeaderMap::new();
            for (n, v) in raw {
                h.append(http::HeaderName::from_bytes(n.as_bytes()).unwrap(), v.parse().unwrap());
            }
            let mut got: Vec<String> = go_response_headers(status, &h)
                .into_iter()
                .map(|(n, v)| format!("{n}: {v}"))
                .collect();
            got.sort();
            assert_eq!(got, want, "{raw:?}");
        };
        check(
            503,
            &[
                ("transfer-encoding", "chunked"),
                ("content-length", "1"),
                ("trailer", "X-Checksum"),
                ("connection", "close"),
                ("pragma", "no-cache"),
                ("x-id", "a"),
            ],
            &["Cache-Control: no-cache", "Pragma: no-cache", "X-Id: a"],
        );
        check(
            503,
            &[
                ("content-length", "1"),
                ("content-length", "1"),
                ("connection", "keep-alive"),
                ("trailer", "X-Checksum"),
                ("pragma", "no-cache"),
                ("cache-control", "max-age=1"),
                ("set-cookie", "s=1"),
                ("set-cookie", "t=2"),
            ],
            &[
                "Cache-Control: max-age=1",
                "Connection: keep-alive",
                "Content-Length: 1",
                "Pragma: no-cache",
                "Set-Cookie: s=1",
                "Set-Cookie: t=2",
                "Trailer: X-Checksum",
            ],
        );
        check(
            204,
            &[
                ("transfer-encoding", "chunked"),
                ("content-length", "0"),
                ("connection", "Keep-Alive, Close"),
            ],
            &["Content-Length: 0"],
        );
    }

    fn credential(meta: Value) -> Credential {
        Credential::from_file(
            std::path::Path::new("/fake"),
            std::path::Path::new("/fake/a.json"),
            meta.as_object().unwrap().clone(),
        )
        .unwrap()
    }

    #[test]
    fn refresh_due_uses_jwt_exp_before_metadata_and_lead() {
        let now = Utc.timestamp_opt(1_790_000_000, 0).unwrap();
        let lead = Some(chrono::Duration::minutes(5));
        use base64::Engine;
        let exp = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(br#"{"exp":1790000100}"#);
        let jwt = format!("h.{exp}.s");
        // JWT exp (100s away) wins over a far metadata expiry.
        let c = credential(serde_json::json!({"type":"kimi","access_token":jwt,"expired":"2099-01-01T00:00:00Z"}));
        assert!(refresh_due(&c, lead, now));
        let c = credential(serde_json::json!({"type":"kimi","access_token":"opaque","expired":"2099-01-01T00:00:00Z"}));
        assert!(!refresh_due(&c, lead, now));
        // No expiry: last refresh decides; none recorded means refresh now.
        let c = credential(serde_json::json!({"type":"kimi","access_token":"opaque"}));
        assert!(refresh_due(&c, lead, now));
        let c = credential(serde_json::json!({"type":"kimi","access_token":"opaque","last_refresh":1_789_999_900}));
        assert!(!refresh_due(&c, lead, now));
        assert!(!refresh_due(&c, None, now), "no lead means no scheduled refresh");
        // expires_in is relative to timestamp; values above 1e12 are milliseconds.
        let c = credential(
            serde_json::json!({"type":"kimi","access_token":"o","expires_in":200,"timestamp":1_789_999_900_000u64}),
        );
        assert!(refresh_due(&c, lead, now));
        let c = credential(
            serde_json::json!({"type":"kimi","access_token":"o","expires_in":900,"timestamp":1_789_999_900_000u64}),
        );
        assert!(!refresh_due(&c, lead, now));
        // refresh_interval accepts Go compound durations and plain seconds.
        let c = credential(
            serde_json::json!({"type":"kimi","access_token":"o","refresh_interval":"1h30m","last_refresh":1_790_000_000 - 1200}),
        );
        assert!(!refresh_due(&c, lead, now));
        let c = credential(
            serde_json::json!({"type":"kimi","access_token":"o","refresh_interval":"600","last_refresh":1_790_000_000 - 1200}),
        );
        assert!(refresh_due(&c, lead, now));
        let ms = chrono::Duration::milliseconds;
        assert_eq!(parse_duration_string("1h30m"), Some(chrono::Duration::seconds(5400)));
        assert_eq!(parse_duration_string("1.5h2.5s"), Some(ms(5_402_500)));
        assert_eq!(parse_duration_string("1001ms"), Some(ms(1001)));
        assert_eq!(parse_duration_string("1005ms"), Some(ms(1005)));
        assert_eq!(parse_duration_string("10"), Some(chrono::Duration::seconds(10)));
        assert_eq!(parse_duration_string("-1s"), None);
        assert_eq!(parse_duration_string(" 1h "), Some(chrono::Duration::hours(1)));
        assert_eq!(parse_duration_string("1.5"), Some(ms(1500)));
        assert_eq!(go_duration("10"), None);
        assert_eq!(go_duration("h"), None);
    }
}
