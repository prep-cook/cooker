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

/// Query and fragment bytes never become the host.
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
        // A bracketed host that is not IPv6, and escapes that
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
    let redacted = "error sending request for uri (http://127.0.0.1:1/?api_key=*** EOF";
    assert_eq!(urls_in(redacted), redacted);
}

/// Each search covers its bytes once, so a long minified body (an embedding array) does
/// not make a log line quadratic.
#[test]
fn the_scan_is_linear() {
    let n = 1 << 17;
    for text in [
        format!("/x [{}]", "1,".repeat(n)),
        format!("/x {}", "a?b,".repeat(n)),
        format!("/x {}", "x?y=".repeat(n)),
        format!("/x {}>", "<".repeat(2 * n)),
        format!("/x {}", "\"a".repeat(n)),
    ] {
        let started = std::time::Instant::now();
        let _ = bytes(text.as_bytes());
        assert!(started.elapsed() < std::time::Duration::from_secs(5), "{}", &text[..12]);
    }
}

/// A quoted URL runs to its closing quote, and a
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

/// Single quotes, a space after the opening quote, a reference
/// after `=`, path-absolute references, and URL-bearing header values whatever the
/// header's name or grammar.
#[test]
fn every_reference_shape_is_redacted() {
    for (text, want) in [
        (
            "Post 'http://127.0.0.1/?code=prefix sk-review-secret': EOF",
            "Post 'http://127.0.0.1/?code=***': EOF",
        ),
        (
            "Post \" http://127.0.0.1/?code=prefix sk-review-secret\": EOF",
            "Post \" http://127.0.0.1/?code=***\": EOF",
        ),
        (
            "request failed: url=//user:sk-review-secret@127.0.0.1/?code=sk-review-secret",
            "request failed: url=//127.0.0.1/?code=***",
        ),
        (
            "request failed: /callback?code=sk-review-secret#sk-review-secret",
            "request failed: /callback?code=***#***",
        ),
        (
            "redirect_uri=callback?code=sk-review-secret",
            "redirect_uri=<redacted url>",
        ),
        (
            "Refresh: http://127.0.0.1/?code=sk-review-secret; url=/next",
            "Refresh: http://127.0.0.1/?code=*** url=/next",
        ),
        (
            "Refresh: 0; url = http://127.0.0.1/?code=sk-review-secret",
            "Refresh: 0; url = http://127.0.0.1/?code=***",
        ),
        (
            "Link: http://127.0.0.1/?code=sk-review-secret",
            "Link: http://127.0.0.1/?code=***",
        ),
        (
            r#"Link: <https://h.example/p?key=sk-review-secret>; rel="next", </p?page=2>; rel="last""#,
            r#"Link: <https://h.example/p?key=***>; rel="next", </p?page=***>; rel="last""#,
        ),
        (
            r#"Link: </next>; anchor="/callback?code=sk-review-secret"; rel="next""#,
            r#"Link: </next>; anchor="/callback?code=***"; rel="next""#,
        ),
        ("Link: </p?key=sk-review-secret", "Link: </p?key=***"),
        (
            r#"Reporting-Endpoints: default="http://127.0.0.1/reports?key=sk-review-secret""#,
            r#"Reporting-Endpoints: default="http://127.0.0.1/reports?key=***""#,
        ),
        ("Location: next?key=sk-review-secret", "Location: <redacted url>"),
        (
            "Content-Location: https://u:sk-review-secret@h.example/x",
            "Content-Location: https://h.example/x",
        ),
        (
            r#"{"error":{"message":"request failed for http://u:sk-review-secret@127.0.0.1/?code=sk-review-secret#sk-review-secret"}}"#,
            r#"{"error":{"message":"request failed for http://127.0.0.1/?code=***#***"}}"#,
        ),
        (
            r#"{"url":"https://h.example/v1?key=sk-review-secret","n":1}"#,
            r#"{"url":"https://h.example/v1?key=***","n":1}"#,
        ),
        (
            "don't use http://h/?k=sk-review-secret, it's bad",
            "don't use http://h/?k=*** it's bad",
        ),
        // Relative references carrying a fragment, and URLs after a
        // backtick or non-ASCII text.
        ("Location: #sk-review-secret", "Location: <redacted url>"),
        (
            "Content-Location: next#sk-review-secret",
            "Content-Location: <redacted url>",
        ),
        (
            r#"Link: <next#sk-review-secret>; rel="next""#,
            r#"Link: <<redacted url> rel="next""#,
        ),
        (
            "upstream redirect: next#sk-review-secret",
            "upstream redirect: <redacted url>",
        ),
        ("Location: next?sk-review-secret", "Location: <redacted url>"),
        ("issue #1", "issue <redacted url>"),
        // A token starts at an ASCII byte, so the cut never splits a character.
        ("请#sk-review-secret", "请<redacted url>"),
        (
            "request failed for `http://u:sk-review-secret@127.0.0.1/path`",
            "request failed for `http://127.0.0.1/path`",
        ),
        (
            "see `http://127.0.0.1/?code=prefix sk-review-secret`",
            "see `http://127.0.0.1/?code=***`",
        ),
        (
            "请访问http://127.0.0.1/?k=sk-review-secret",
            "请访问http://127.0.0.1/?k=***",
        ),
        ("**http://127.0.0.1/?k=sk-review-secret**", "**http://127.0.0.1/?k=***"),
    ] {
        let got = urls_in(text);
        assert_eq!(got, want, "{text}");
        assert_no_secret(&got, text);
        // The log writer scans error lines that were redacted already.
        assert_eq!(urls_in(&got), got, "{text}");
    }
    for unchanged in [
        "Content-Type: application/json; charset=utf-8",
        "User-Agent: Mozilla/5.0 (X11; Linux x86_64)",
        "open /etc/hosts: permission denied",
        "and/or what? C# ## heading",
        r#"{"image":"/9j/4AAQSkZJRgABAQ=="}"#,
    ] {
        assert_eq!(urls_in(unchanged), unchanged);
    }
    assert_eq!(bytes(b"/x?k=\xffsk-review-secret"), &b"<redacted url>"[..]);
    assert_eq!(bytes(b"\xff\xfe http://h/?k=v"), &b"\xff\xfe http://h/?k=***"[..]);
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
