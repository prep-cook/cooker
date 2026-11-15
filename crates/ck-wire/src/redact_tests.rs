use super::*;

const SECRET: &str = "sk-review-secret";

fn assert_no_secret(got: &str, raw: &str) {
    assert!(!got.contains(SECRET), "{raw:?} printed {got:?}");
}

#[test]
fn credentials_never_print() {
    for (raw, want) in [
        (
            "https://user:sk-review-secret@host.example:8443/v1",
            "https://host.example:8443/v1",
        ),
        ("https://:sk-review-secret@host.example/v1", "https://host.example/v1"),
        ("https://sk-review-secret@host.example", "https://host.example"),
        ("https://u%40x:sk-review-secret@host.example/", "https://host.example/"),
        (
            "http://127.0.0.1:9/v1?api_key=sk-review-secret&alt=sse&sk-review-secret&&x=",
            "http://127.0.0.1:9/v1?api_key=***&alt=***&***&&***",
        ),
        ("http://127.0.0.1/v1?sk-review-secret=", "http://127.0.0.1/v1?***"),
        (
            "https://host.example/cb#access_token=sk-review-secret",
            "https://host.example/cb#***",
        ),
        ("http://127.0.0.1/v1#sk-review-secret", "http://127.0.0.1/v1#***"),
        ("https://host.example/p?#", "https://host.example/p?"),
        ("//host.example/x?k=sk-review-secret", "//host.example/x?k=***"),
        ("https://[::1]:8443/x", "https://[::1]:8443/x"),
        (
            "wss://u:sk-review-secret@[::1]:1/r?t=sk-review-secret",
            "wss://[::1]:1/r?t=***",
        ),
        (
            "/v1/models?key=sk-review-secret#sk-review-secret",
            "/v1/models?key=***#***",
        ),
        // Path segments keep their '@' (Vertex model versions).
        (
            "https://h.example/models/claude@001:rawPredict?key=sk-review-secret",
            "https://h.example/models/claude@001:rawPredict?key=***",
        ),
        (" HTTP://127.0.0.1:9/v1 ", " HTTP://127.0.0.1:9/v1 "),
        ("https:///v1?k=sk-review-secret", "https:///v1?k=***"),
    ] {
        let got = url(raw);
        assert_eq!(got, want, "{raw}");
        assert_no_secret(&got, raw);
    }
}

/// Query and fragment bytes never become the host (review of #127, finding 1).
#[test]
fn the_authority_ends_before_the_query_and_fragment() {
    for raw in [
        "ftp://127.0.0.1?key=prefix@sk-review-secret",
        "https://127.0.0.1#prefix@sk-review-secret",
        "https://127.0.0.1?a=1#b@sk-review-secret/x",
        // A password holding an unescaped '?', '#' or '@' leaves its start before the
        // query: the URL is refused rather than printing it as the host.
        "https://sk-review-secret?x@host.example/x",
        "https://u:sk?review#sec@ret@host.example/x",
    ] {
        let got = url(raw);
        assert_eq!(got, UNPRINTABLE, "{raw}");
        assert_no_secret(&got, raw);
    }
    assert_eq!(
        url("https://host.example/x?email=a@b.example"),
        "https://host.example/x?email=***"
    );
}

/// Owner ruling Q18: a URL without '@' is not printed as written.
#[test]
fn malformed_urls_print_the_placeholder() {
    for raw in [
        "http://user:sk-review-secret@127.0.0.1:bad/?api_key=sk-review-secret",
        // Go reads host `u`, port `sk-review-secret`: the password before a '/'.
        "http://u:sk-review-secret/x@host.example/",
        "https:sk-review-secret@relay.example",
        "user:sk-review-secret@relay.example/v1",
        "http://h.example/\nsk-review-secret",
        "http://u@[::1/sk-review-secret",
        "sk-review-secret",
        "relay.example/v1?key=sk-review-secret",
        "https:sk-review-secret",
        "https://host:sk-review-secret/v1",
        "https://[::1/sk-review-secret",
        "https://sk review secret/v1",
        // Review of #127, finding 7: a bracketed host that is not IPv6, and escapes that
        // decode to delimiters.
        "http://[sk-review-secret]/",
        "http://[v1.sk-review-secret]/",
        "http://sk-review-secret%40127.0.0.1/",
        "http://sk-review-secret%2f127.0.0.1/",
        "http://sk-review-secret%/",
        "http://[::1%sk-review-secret]/",
    ] {
        let got = url(raw);
        assert_eq!(got, UNPRINTABLE, "{raw:?}");
        assert_no_secret(&got, raw);
    }
    for raw in [
        "http://[::1%25eth0]:1/x",
        "https://%61pi.example/x",
        "http://xn--r8j.example/",
    ] {
        assert_eq!(url(raw), raw);
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
    assert_no_secret(&got, &text);
    assert!(matches!(urls_in("no urls here"), Cow::Borrowed(_)));
    // The token runs past ')' and ':' so no part of the query value survives.
    let wreq = format!("error sending request for uri (http://h/?api_key={SECRET}/x): client error");
    assert_eq!(
        urls_in(&wreq),
        "error sending request for uri (http://h/?api_key=*** client error"
    );
}

/// Review of #127, finding 2: a quoted URL runs to its closing quote, and a
/// scheme-relative URL is a URL too.
#[test]
fn quoted_and_scheme_relative_urls_are_whole() {
    for (text, want) in [
        (
            format!(r#"Post "http://127.0.0.1/?code=prefix {SECRET}": EOF"#),
            r#"Post "http://127.0.0.1/?code=***": EOF"#,
        ),
        (
            format!("Post \"http://127.0.0.1/?code=prefix\r\n{SECRET}\": EOF"),
            r#"Post "<redacted url>": EOF"#,
        ),
        (
            format!(r#"Post "http://127.0.0.1/?code=a\"{SECRET}": EOF"#),
            r#"Post "http://127.0.0.1/?code=***": EOF"#,
        ),
        (
            format!(r#"Post "//user:{SECRET}@127.0.0.1/?code={SECRET}": unsupported protocol scheme """#),
            r#"Post "//127.0.0.1/?code=***": unsupported protocol scheme """#,
        ),
        (
            format!("see //user:{SECRET}@127.0.0.1/ or (//127.0.0.1/?k={SECRET})"),
            "see //127.0.0.1/ or (//127.0.0.1/?k=***",
        ),
        ("a/b//c and path//x".to_owned(), "a/b//c and path//x"),
    ] {
        let got = urls_in(&text);
        assert_eq!(got, want, "{text:?}");
        assert_no_secret(&got, &text);
    }
}

#[test]
fn url_headers_are_rendered_for_logs() {
    for (name, value, want) in [
        (
            "location",
            "http://127.0.0.1/v1?key=sk-review-secret",
            "http://127.0.0.1/v1?key=***",
        ),
        ("location", "/v1/next?key=sk-review-secret", "/v1/next?key=***"),
        ("location", "next?key=sk-review-secret", UNPRINTABLE),
        (
            "content-location",
            "https://u:sk-review-secret@h.example/x",
            "https://h.example/x",
        ),
        (
            "referer",
            "https://h.example/cb#sk-review-secret",
            "https://h.example/cb#***",
        ),
        ("x-original-url", "/v1?token=sk-review-secret", "/v1?token=***"),
        (
            "link",
            r#"<https://h.example/p?key=sk-review-secret>; rel="next", </p?page=2>; rel="last""#,
            r#"<https://h.example/p?key=***>; rel="next", </p?page=***>; rel="last""#,
        ),
        ("link", "</p?key=sk-review-secret", "<<redacted url>"),
        // Review of #127, finding 1: URL parameters, and spaces around `url =`.
        (
            "link",
            r#"</next>; anchor="/callback?code=sk-review-secret"; rel="next""#,
            r#"</next>; anchor="/callback?code=***"; rel="next""#,
        ),
        (
            "link",
            "</next>; anchor = /cb?code=sk-review-secret; title=\"a\\\"sk-review-secret\"",
            "</next>; anchor = /cb?code=***; title=\"<redacted url>",
        ),
        (
            "refresh",
            "0; URL='https://h.example/?key=sk-review-secret'",
            "0; url=https://h.example/?key=***",
        ),
        (
            "refresh",
            "0; url = http://127.0.0.1/?code=sk-review-secret",
            "0; url=http://127.0.0.1/?code=***",
        ),
        ("refresh", "5", "5"),
        ("refresh", "http://127.0.0.1/?code=sk-review-secret", UNPRINTABLE),
    ] {
        let got = url_header(name, value.as_bytes()).unwrap();
        assert_eq!(got, want, "{name}: {value}");
        assert_no_secret(&got, value);
    }
    assert_eq!(url_header("location", b"/x?k=\xff"), Some(UNPRINTABLE.to_owned()));
    assert_eq!(url_header("content-type", b"text/plain"), None);
}

#[test]
fn every_query_value_is_masked() {
    for (raw, want) in [
        ("", ""),
        ("a=1&&b=2&", "a=***&&b=***&"),
        ("token&key=", "***&***"),
        ("k=v=w;x", "k=***"),
    ] {
        assert_eq!(query(raw), want, "{raw}");
    }
}
