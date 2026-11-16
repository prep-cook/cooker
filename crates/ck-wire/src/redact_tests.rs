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
        // A `//` before the query, other than the opening one, may start a URL of its
        // own: in the path or inside the authority.
        (
            "http://h.example/r/https://u:sk-review-secret@x.example/a?k=sk-review-secret",
            UNPRINTABLE,
        ),
        ("/p_https://u:sk-review-secret@x.example/a", UNPRINTABLE),
        ("http://h.example/9://u:sk-review-secret@x/", UNPRINTABLE),
        ("https://h.example/a//u:sk-review-secret@x/", UNPRINTABLE),
        ("https://user:sk-review-secret://host", UNPRINTABLE),
        ("//user:sk-review-secret://host", UNPRINTABLE),
        ("https://h.example://u:sk-review-secret@x/", UNPRINTABLE),
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

/// Free text is redacted word by word: a word that may hold a credential prints as the
/// placeholder whole, and a quoted value that goes on past whitespace is dropped up to
/// its closing `"` or the line end. The cost is that e-mail addresses, `user@host`,
/// `mailto:`, words holding `?` or `#`, and `word:word` print as the placeholder too.
#[test]
fn words_that_may_hold_a_credential_print_as_the_placeholder() {
    for (text, want) in [
        (
            r#"Post "http://u:sk-review-secret@127.0.0.1:1/v1?key=sk-review-secret": EOF"#,
            r#"Post "<redacted url>": EOF"#,
        ),
        (
            "see http://u:sk-review-secret@127.0.0.1/p?k=sk-review-secret end",
            "see <redacted url> end",
        ),
        (
            "retry ftp://sk-review-secret@h/ see ://sk-review-secret@h",
            "retry <redacted url> see <redacted url>",
        ),
        (
            "error sending request for uri (http://h/?api_key=sk-review-secret/x): client error",
            "error sending request for uri <redacted url> client error",
        ),
        (
            "request failed: url=//user:sk-review-secret@127.0.0.1/",
            "request failed: <redacted url>",
        ),
        (
            "request failed: /callback?code=sk-review-secret#x",
            "request failed: <redacted url>",
        ),
        (
            "Location: http://127.0.0.1/?co=sk-review-secret\nnext",
            "Location: <redacted url>\nnext",
        ),
        (
            "x_https://user:sk-review-secret@host.example/ end",
            "<redacted url> end",
        ),
        ("see foo//user:sk-review-secret@host end", "see <redacted url> end"),
        ("dsn=redis:sk-review-secret@host end", "<redacted url> end"),
        ("go https:sk-review-secret@relay.example", "go <redacted url>"),
        // A `?` or `#` anywhere, and an opaque URL, whatever follows.
        ("go next#sk-review-secret", "go <redacted url>"),
        ("go callback?sk-review-secret", "go <redacted url>"),
        ("go https:sk-review-secret", "go <redacted url>"),
        ("go a1:sk-review-secret", "go <redacted url>"),
        ("go 1-https:sk-review-secret} x", "go <redacted url> x"),
        ("go 0.9+redis:sk-review-secret", "go <redacted url>"),
        ("go 1a,b+c:sk-review-secret", "go <redacted url>"),
        ("dial 9://sk-review-secret/x", "dial <redacted url>"),
        ("open https:relay.example//sk-review-secret", "open <redacted url>"),
        ("see x<http://h/>what?yes tail", "see <redacted url>"),
        ("中文http://127.0.0.1/?k=sk-review-secret 。", "<redacted url> 。"),
        // A quoted value runs to its closing `"`, however it starts.
        (
            r#"Post "http://127.0.0.1/?code=prefix sk-review-secret": EOF"#,
            r#"Post "<redacted url>": EOF"#,
        ),
        (r#""callback?code=prefix sk-review-secret""#, r#""<redacted url>""#),
        (
            "\"\thttp://127.0.0.1/?k=prefix sk-review-secret\"",
            "\"\t<redacted url>\"",
        ),
        (
            "Post \" http://127.0.0.1/?code=prefix sk-review-secret\": EOF",
            "Post \" <redacted url>\": EOF",
        ),
        (
            "Post \"http://127.0.0.1/?code=prefix\r\nsk-review-secret\": EOF",
            "Post \"<redacted url>\": EOF",
        ),
        // With no closing `"`, the rest of the text.
        ("\"http://h/?a=b c\nd", "\"<redacted url>"),
        // `'`, a backtick and `<` are URL bytes, so they open a quoted value inside the
        // word; it runs to the next `"` or line end.
        (
            "Post 'http://127.0.0.1/?code=prefix sk-review-secret': EOF\nnext",
            "Post <redacted url>\nnext",
        ),
        ("x <http://h.example/?code=a sk-review-secret> y", "x <redacted url>"),
        ("`//u:sk-review-secret@h/ b` c", "<redacted url>"),
        // A URL whose query value opens a quote: the whole word, and the rest of the line.
        (
            "http://127.0.0.1/?k='http://127.0.0.1/'sk-review-secret end",
            "<redacted url>",
        ),
        // Accepted over-redaction.
        ("write to a@b.example now", "write to <redacted url> now"),
        ("ssh user@host", "ssh <redacted url>"),
        ("mailto:a@b.example", "<redacted url>"),
        ("try ?a=b", "try <redacted url>"),
        (
            "and/or what? C# ## heading what?yes",
            "and/or <redacted url> <redacted url> <redacted url> heading <redacted url>",
        ),
        ("listen example.com:443", "listen <redacted url>"),
        ("dsn redis:key", "dsn <redacted url>"),
        ("models/claude@001:rawPredict", "<redacted url>"),
    ] {
        let got = urls_in(text);
        assert_eq!(got, want, "{text:?}");
        assert_no_secret(&got, text);
        // The log writer scans error lines that were redacted already; they come back
        // unchanged.
        assert_eq!(urls_in(&got), got, "{text:?}");
    }
    for unchanged in [
        "no urls here",
        "Content-Type: application/json; charset=utf-8",
        "User-Agent: Mozilla/5.0 (X11; Linux x86_64)",
        "open /etc/hosts: permission denied",
        "note: x",
        "see [1]:2",
        r#"{"type":"x","n":[1,2]}"#,
        "error: EOF a/b//c and path//x",
        "time: 12 :30 1: 2",
        // A scheme starts with a letter: times and IPv4 `host:port` print.
        "ratio 1:2 at 12:30",
        "at 2026-11-16T05:00:00Z and 2026-11-16T15:01:52+11:00",
        "dial tcp 1.2.3.4:443 and 127.0.0.1:8080",
        r#"{"image":"/9j/4AAQSkZJRgABAQ=="}"#,
        "\u{fffd}\u{fffd} plain",
    ] {
        assert!(matches!(urls_in(unchanged), Cow::Borrowed(_)), "{unchanged}");
    }
    assert_eq!(bytes(b"/x?k=\xffsk-review-secret"), &b"<redacted url>"[..]);
    assert_eq!(bytes(b"\xff\xfe http://h/?k=v"), &b"\xff\xfe <redacted url>"[..]);
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

/// Reads that end where the test says.
struct Reads<'a>(std::collections::VecDeque<&'a [u8]>);

impl std::io::Read for Reads<'_> {
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
    copy_holding(&mut Reads(pieces.iter().copied().collect()), &mut out, hold).unwrap();
    String::from_utf8(out).unwrap()
}

/// A text copied in pieces is redacted as [`bytes`] redacts it whole, wherever a read
/// ends. A word longer than the hold limit prints as the placeholder on every path
/// (owner ruling R-7).
#[test]
fn copied_text_is_redacted_wherever_reads_end() {
    // (text, hold limit, what every split gives; `None` is what `bytes` gives)
    for (text, hold, want) in [
        (
            "Post \"http://127.0.0.1/?code=prefix sk-review-secret\": EOF\nLocation: http://127.0.0.1/?co=sk-review-secret\n",
            scan::HOLD,
            None,
        ),
        ("don't stop callback?code=sk-review-secret tail", scan::HOLD, None),
        ("x <http://h/?q=a sk-review-secret> y\n\"z\" 'b", scan::HOLD, None),
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
        ("see qqqqqqqqqqqqqqqqqqqqqqqq tail", 16, Some("see <redacted url> tail")),
        (
            "see 'qqqqqqqqqqqqqqqqqqqqqqqq tail\nx",
            16,
            Some("see <redacted url>\nx"),
        ),
    ] {
        let whole = urls_in(text).into_owned();
        let want = want.unwrap_or(&whole);
        let bytes_read: Vec<&[u8]> = text.as_bytes().chunks(1).collect();
        assert_eq!(copied(&bytes_read, hold), want, "{text}");
        for cut in 0..=text.len() {
            let got = copied(&[&text.as_bytes()[..cut], &text.as_bytes()[cut..]], hold);
            assert_eq!(got, want, "{text} cut at {cut}");
        }
    }
    // A word over the hold limit: the URL that a quote inside it opens runs on past it.
    let over = format!(
        "http://127.0.0.1/?k={}'http://127.0.0.1/'sk-review-secret end",
        "x".repeat(scan::HOLD + (64 << 10))
    );
    let plain = format!("a {} b", "x".repeat(scan::HOLD + 1));
    for (text, want) in [(&over, "<redacted url>"), (&plain, "a <redacted url> b")] {
        assert_eq!(bytes(text.as_bytes()), want.as_bytes(), "bytes");
        let mut out = Vec::new();
        write(text.as_bytes(), &mut out).unwrap();
        assert_eq!(out, want.as_bytes(), "write");
        assert_eq!(copied(&[text.as_bytes()], scan::HOLD), want, "copy");
    }
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
    "next?S",
    // An opaque `scheme:key` is a scheme only where its run of scheme bytes starts with a
    // letter, so these start a word.
    " https:S",
    " a1:S",
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

/// Shapes holding a space, which only a quoted value keeps in one word.
const SPACED: &[&str] = &["http://h.example/?code=prefix S", " //u:S@h.example/a b"];

/// Quotes first, then the other wrappings.
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

/// `parts` of one text as a log writes them: [`Pieces`] with an event header between
/// them, under the hold limit `hold`.
fn parted(parts: &[&[u8]], hold: usize) -> String {
    let mut out = Vec::new();
    let mut pieces = Pieces::holding(hold);
    for (i, part) in parts.iter().enumerate() {
        if i > 0 {
            pieces.cut(&mut out).unwrap();
            write(b"\n\ntimestamp: t\nevent: x\n", &mut out).unwrap();
        }
        pieces.push(part, &mut out).unwrap();
    }
    pieces.finish(&mut out).unwrap();
    // A cut may split a character; the secrets are ASCII.
    String::from_utf8_lossy(&out).into_owned()
}

/// A text in parts fails closed where a part ends in a word that the next part may carry
/// on into a credential, and nowhere else.
#[test]
fn parts_fail_closed_where_a_credential_may_go_on() {
    const HEADER: &str = "\n\ntimestamp: t\nevent: x\n";
    for (parts, want) in [
        (&["wss://user:", "sk-review-secret@host/ x"][..], "<redacted url>|| x"),
        (
            &["see https://h.example/?k=", "sk-review-secret end"],
            "see <redacted url>|| end",
        ),
        (&["dsn=redis:sk-rev", "iew-secret@host end"], "<redacted url>|| end"),
        (&["callback?code=", "sk-review-secret"], "<redacted url>||"),
        (&["a//user", ":sk-review-secret@h b"], "<redacted url>|| b"),
        // A quote in the next part opens a value that runs to the line end.
        (
            &["http://127.0.0.1/?k=", "'http://127.0.0.1/'sk-review-secret end"],
            "<redacted url>||",
        ),
        (
            &["x <http://h.example/?code=a", " sk-review-secret> y\nz"],
            "x <redacted url>||\nz",
        ),
        // So does a word that ends in a byte a URL can hold.
        (&["total 12", "x y"], "total <redacted url>|| y"),
        // JSON messages and words that end with their part stay.
        (&[r#"{"input":[]}"#, r#"{"type":"x"}"#], r#"{"input":[]}||{"type":"x"}"#),
        (&["total 12}", "x"], "total 12}||x"),
        (&["see http://h.example/ ", "x"], "see <redacted url> ||x"),
    ] {
        let parts: Vec<&[u8]> = parts.iter().map(|p| p.as_bytes()).collect();
        assert_eq!(parted(&parts, scan::HOLD), want.replace("||", HEADER), "{parts:?}");
    }
}

/// Generated texts with secrets in URLs of every shape, wrapping and neighbour: copying
/// a text in pieces gives what redacting it whole gives, wherever reads end, and no
/// secret byte prints either way. Under a 16-byte hold limit only the second holds. The
/// same text cut into parts with an event header between them gives the whole text's
/// output when the cut falls between words, and prints no secret byte at any cut or
/// random set of cuts. `CK_REDACT_CASES` sets the number of texts for a longer run.
#[test]
fn copying_in_pieces_equals_redacting_whole() {
    const HEADER: &str = "\n\ntimestamp: t\nevent: x\n";
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
        assert_eq!(
            parted(&[bytes_text], scan::HOLD),
            whole,
            "case {case}: {text:?} in one part"
        );
        for cut in 0..=bytes_text.len() {
            let pieces = [&bytes_text[..cut], &bytes_text[cut..]];
            assert_eq!(copied(&pieces, scan::HOLD), whole, "case {case}: {text:?} cut at {cut}");
            let small = copied(&pieces, 16);
            assert!(!leaks(&small), "case {case}: {text:?} cut at {cut} printed {small:?}");
            let parts = parted(&pieces, scan::HOLD);
            assert!(
                !leaks(&parts),
                "case {case}: {text:?} parted at {cut} printed {parts:?}"
            );
            if cut == 0 || scan::stop(bytes_text[cut - 1]) {
                assert_eq!(
                    parts.replacen(HEADER, "", 1),
                    whole,
                    "case {case}: {text:?} parted between words at {cut}"
                );
            }
        }
        let mut cuts: Vec<usize> = (0..1 + rng.below(6)).map(|_| rng.below(bytes_text.len() + 1)).collect();
        cuts.sort_unstable();
        let parts: Vec<&[u8]> = [0]
            .iter()
            .chain(&cuts)
            .zip(cuts.iter().chain([&bytes_text.len()]))
            .map(|(from, to)| &bytes_text[*from..*to])
            .collect();
        for hold in [scan::HOLD, 16] {
            let got = parted(&parts, hold);
            assert!(!leaks(&got), "case {case}: {parts:?} under hold {hold} printed {got:?}");
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
