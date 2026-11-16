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
        // A `scheme://` inside the path starts a URL of its own.
        (
            "http://h.example/r/https://u:sk-review-secret@x.example/a?k=sk-review-secret",
            "http://h.example/r/https://x.example/a?k=***",
        ),
        ("/p_https://u:sk-review-secret@x.example/a", "/p_https://x.example/a"),
        (
            "http://h.example/9://u:sk-review-secret@x/",
            "http://h.example/<redacted url>",
        ),
        // A `scheme://` or `//` inside the authority does not end it.
        ("https://user:sk-review-secret://host", "<redacted url>//host"),
        ("//user:sk-review-secret://host", "<redacted url>//host"),
        ("https://h.example://u:sk-review-secret@x/", "https://h.example://x/"),
        (
            "https://h.example/p].https://user:sk-review-secret://host",
            "https://h.example/p]<redacted url>//host",
        ),
        // Inside the query it is a masked value.
        (
            "http://h.example/a?next=https://u:sk-review-secret@x/",
            "http://h.example/a?next=***",
        ),
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
        // A token holding `://` anywhere is a URL from its scheme on.
        (
            "x_https://user:sk-review-secret@host.example/",
            "x_https://host.example/",
        ),
        (
            "see /p_https://user:sk-review-secret@host.example/a end",
            "see /p_https://host.example/a end",
        ),
        (
            "see \"http://h.example/x https://u:sk-review-secret@y.example/\" end",
            "see \"http://h.example/x https://y.example/\" end",
        ),
        // A scan that passes over a span resumes at its closer, where a token can start.
        (
            "see a<http://h.example/?k=v#x>&#sk-review-secret end",
            "see a<http://h.example/?k=***#***><redacted url> end",
        ),
        // A word that could still carry a credential prints as the placeholder whole.
        ("dsn=redis:sk-review-secret@host end", "<redacted url> end"),
        ("see foo//user:sk-review-secret@host end", "see <redacted url> end"),
        // A reference that hides the opener of a reference span ending after it.
        (
            "see /a`<http://h.example/?code=a sk-review-secret> end",
            "see <redacted url>> end",
        ),
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

/// Reads that end where the test says.
struct Pieces<'a>(std::collections::VecDeque<&'a [u8]>);

impl std::io::Read for Pieces<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // An empty read would mean the end.
        self.0.retain(|piece| !piece.is_empty());
        let Some(piece) = self.0.pop_front() else { return Ok(0) };
        let n = piece.len().min(buf.len());
        buf[..n].copy_from_slice(&piece[..n]);
        if n < piece.len() {
            self.0.push_front(&piece[n..]);
        }
        Ok(n)
    }
}

fn copied(pieces: &[&[u8]], hold: usize) -> String {
    let mut out = Vec::new();
    copy_holding(&mut Pieces(pieces.iter().copied().collect()), &mut out, hold).unwrap();
    String::from_utf8(out).unwrap()
}

/// A text copied in pieces is redacted as [`bytes`] redacts it whole, wherever a read
/// ends, so a secret split across reads at any byte never prints. A reference longer
/// than the hold limit prints as the placeholder instead.
#[test]
fn copied_text_is_redacted_wherever_reads_end() {
    const BIG: usize = 1 << 20;
    // (text, hold limit, what one-byte reads give; `None` is what `bytes` gives)
    for (text, hold, bytewise) in [
        (
            "Post \"http://127.0.0.1/?code=prefix\nsk-review-secret\": EOF\nLocation: http://127.0.0.1/?co=sk-review-secret\n",
            BIG,
            None,
        ),
        (
            "see http://u:sk-review-secret@127.0.0.1/p?k=sk-review-secret end",
            BIG,
            None,
        ),
        (
            "x_https://user:sk-review-secret@host.example/ /p_https://u:sk-review-secret@x/a",
            BIG,
            None,
        ),
        (
            "\"/cb?code=sk-review-secret\" 'http://h/?k=sk-review-secret' <http://h/#sk-review-secret> `//u:sk-review-secret@h/`",
            BIG,
            None,
        ),
        ("don't stop callback?code=sk-review-secret tail", BIG, None),
        // A read that ends right after a reference does not make the next byte a token
        // start.
        ("see x<http://h/>what?yes tail", BIG, None),
        ("请访问http://127.0.0.1/?k=sk-review-secret <b \"a\\\"b\" c", BIG, None),
        (
            "see http://127.0.0.1/?code=xxxxxxxxsk-review-secret tail",
            16,
            Some("see <redacted url> tail"),
        ),
        (
            "see \"http://127.0.0.1/?code=xx sk-review-secret\" tail",
            16,
            Some("see \"<redacted url>\" tail"),
        ),
        // A word longer than the hold limit prints as the placeholder, even when its
        // `?` comes last.
        (
            "see xxxxxxxxxxxxxxxxxxxx?code=sk-review-secret tail",
            16,
            Some("see <redacted url> tail"),
        ),
        (
            "see qqqqqqqqqqqqqqqqqqqqqqqq/b+c= tail",
            16,
            Some("see <redacted url> tail"),
        ),
        // A dropped token that opens a reference span is dropped through its closer,
        // whatever closed before it; other spans it opens end nothing.
        (
            "see /aaaaaaaaaaaaaaaaaaaa`<http://h.example/?code=a sk-review-secret> tail",
            16,
            Some("see <redacted url>> tail"),
        ),
        (
            "see /aaaaaaaaaaaaaaaaaaaa``http://h.example/?code=a sk-review-secret` tail",
            16,
            Some("see <redacted url>` tail"),
        ),
        ("see /aaaaaaaaaaaaaaaaaaaa'x tail", 16, Some("see <redacted url> tail")),
        // A word the scan writes as it goes is held too, up to the same limit.
        ("see a=b=c=d=e=f=g=h=i=j tail", 16, Some("see <redacted url> tail")),
    ] {
        let whole = urls_in(text).into_owned();
        let bytes_read: Vec<&[u8]> = text.as_bytes().chunks(1).collect();
        assert_eq!(copied(&bytes_read, hold), bytewise.unwrap_or(&whole), "{text}");
        for cut in 0..=text.len() {
            let got = copied(&[&text.as_bytes()[..cut], &text.as_bytes()[cut..]], hold);
            assert_no_secret(&got, text);
            if hold == BIG {
                assert_eq!(got, whole, "{text} cut at {cut}");
            }
        }
    }
    let long = format!(" sk-review-secret{}?k=v end", "x".repeat(BIG + (64 << 10)));
    assert_eq!(copied(&[long.as_bytes()], BIG), " <redacted url> end");
}

/// splitmix64, so every run generates the same texts.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[self.below(items.len())]
    }
}

/// URLs whose `S` and `T` stand for secrets: userinfo, query values and fragments.
const SHAPES: &[&str] = &[
    "https://user:S@host.example/p",
    "https://S@host.example/",
    "https://u:S@T@h.example:8443/p",
    "https://u%3AS@h.example/",
    "//user:S@host.example/p",
    "http://host.example/p?k=S&j=T",
    "/v1/x?key=S#T",
    "https://host.example/p#S",
    "a/b?k=S",
    "callback?code=S",
    "?code=S",
    "#S",
    "redis:S@host.example",
    "mailto:S@host.example",
    "https://host.example/r/https://u:S@x.example/a",
    "/r/https://u:S@x.example/a",
    "https://host.example/a//u:S@x.example/",
    "https://user:S://host",
    "//user:S://host",
    "https://user:S//host",
    "https://host.example/?next=https://u:S@x.example/",
    "foo//user:S@host",
    "x.y/https://u:S@h.example",
    "https://u:S/x@h.example/",
    "https://u:S?x@h.example/",
    "https://u:S#x@h.example/",
    "http://u:S@[::1]:80/",
    "https://h.example/p:S@x",
];

/// Shapes holding a space, which only a span keeps in one reference.
const SPACED: &[&str] = &["http://h.example/?code=prefix S", " //u:S@h.example/a b"];

/// Spans first, then the other wrappings.
const WRAPS: &[(&str, &str)] = &[
    ("\"", "\""),
    ("'", "'"),
    ("<", ">"),
    ("`", "`"),
    ("", ""),
    ("\" ", "\""),
    ("(", ")"),
    ("[", "]"),
    ("{", "}"),
    ("x=", ""),
];

const SEPARATORS: &[&str] = &[
    "", " ", "\n", "\t", "\r\n", ",", ";", ":", "=", "\"", "'", "(", ")", "<", ">", "[", "]", "{", "}", "`", "é", "中",
    "?", "#", "/", "@", "\\", "&", ".", "-",
];

/// A random text of words, long tokens, separators and secret-bearing URLs. Secrets are
/// the only upper-case letters in it.
fn generated(rng: &mut Rng) -> String {
    let mut text = String::new();
    for _ in 0..1 + rng.below(8) {
        match rng.below(5) {
            0 | 1 => {
                let (shape, (open, close)) = match rng.below(6) {
                    0 => (rng.pick(SPACED), rng.pick(&WRAPS[..4])),
                    _ => (rng.pick(SHAPES), rng.pick(WRAPS)),
                };
                let mut url = String::new();
                for c in shape.chars() {
                    match c {
                        'S' | 'T' => url.extend((0..4 + rng.below(12)).map(|_| (b'A' + rng.below(26) as u8) as char)),
                        c => url.push(c),
                    }
                }
                text.extend([open, &url, close]);
            }
            2 => {
                const LONG: &[u8] = b"abcdefqrstuvwxyz0123456789+/=._-";
                text.extend((0..100 + rng.below(1900)).map(|_| rng.pick(LONG) as char));
            }
            _ => {
                const WORD: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
                text.extend((0..1 + rng.below(8)).map(|_| rng.pick(WORD) as char));
            }
        }
        text.push_str(rng.pick(SEPARATORS));
    }
    text
}

/// Generated texts with secrets in URLs of every shape, wrapping and neighbour: copying
/// a text in pieces gives what redacting it whole gives, wherever reads end, and no
/// secret byte prints either way. Under a 16-byte hold limit only the second holds.
/// `CK_REDACT_CASES` sets the number of texts for a longer run.
#[test]
fn copying_in_pieces_equals_redacting_whole() {
    let cases = std::env::var("CK_REDACT_CASES").map_or(48, |n| n.parse().unwrap());
    let mut rng = Rng(0x5eed_c0de_2026_1116);
    for case in 0..cases {
        let text = generated(&mut rng);
        let bytes_text = text.as_bytes();
        let whole = urls_in(&text).into_owned();
        let leaks = |got: &str| got.bytes().any(|b| b.is_ascii_uppercase());
        assert!(!leaks(&whole), "case {case}: {text:?} printed {whole:?}");
        let one_byte: Vec<&[u8]> = bytes_text.chunks(1).collect();
        assert_eq!(
            copied(&one_byte, scan::HOLD),
            whole,
            "case {case}: {text:?} byte by byte"
        );
        let small = copied(&one_byte, 16);
        assert!(!leaks(&small), "case {case}: {text:?} printed {small:?} under hold 16");
        for cut in 0..=bytes_text.len() {
            let pieces = [&bytes_text[..cut], &bytes_text[cut..]];
            assert_eq!(copied(&pieces, scan::HOLD), whole, "case {case}: {text:?} cut at {cut}");
            let small = copied(&pieces, 16);
            assert!(!leaks(&small), "case {case}: {text:?} cut at {cut} printed {small:?}");
        }
    }
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
