use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::Arc,
    time::Duration,
};

use ck_translate::codec::{
    Capability, Error, Operation, RequestIngress, RequestView, Result, ValidatedRequest, encode,
    event::{Direction, Identity, LocalId},
    multipart::{Boundary, Ingress, Limits},
};
use serde_json::{Value, json};

use super::semantic::Mapped;

const CONTENT_TYPE: &str = "multipart/form-data; boundary=xyz";
const LIMITS: Limits = Limits {
    body_bytes: 1_000_000,
    part_bytes: 900_000,
    parts: 16,
};

struct Destination;
impl Capability for Destination {
    fn destination(&self) -> &'static str {
        "multipart-test"
    }
    fn validate(&self, request: RequestView<'_>) -> Result<()> {
        let doc = request.document;
        let x = doc.required(doc.root(), "x_ck")?;
        let transport = doc.required(x, "transport")?;
        for part in doc.children(doc.required(transport, "parts")?)? {
            request
                .media
                .get(doc, doc.required(part, "data")?)?
                .ok_or(Error::Schema)?;
        }
        Ok(())
    }
}

fn identity() -> Identity {
    Identity {
        request_id: LocalId::new("request").unwrap(),
        stream_id: LocalId::new("stream").unwrap(),
        attempt: 1,
        direction: Direction::Request,
    }
}

fn bytes(lease: &ck_stream::ScratchLease) -> Vec<u8> {
    let mut output = Vec::new();
    while output.len() < lease.len() {
        output.extend_from_slice(lease.chunk(output.len()).unwrap());
    }
    output
}

/// The loopback sender accepts only an admitted request, just as request egress does.
fn send(ready: &ValidatedRequest<'_>, owner: &mut ck_stream::Writer, address: std::net::SocketAddr) -> Result<()> {
    let view = ready.for_destination("multipart-test")?;
    let doc = view.document;
    let x = doc.required(doc.root(), "x_ck")?;
    let transport = doc.required(x, "transport")?;
    let parts = doc.required(transport, "parts")?;
    let output = encode::scratch(owner, |e| {
        e.bytes(b"[")?;
        let mut first = true;
        for part in doc.children(parts)? {
            if !first {
                e.bytes(b",")?;
            }
            first = false;
            e.bytes(b"{")?;
            for name in ["name", "filename", "filename_star", "content_type"] {
                if let Some(node) = doc.member(part, name)? {
                    e.string(name)?;
                    e.bytes(b":")?;
                    doc.write(node, e)?;
                    e.bytes(b",")?;
                }
            }
            e.bytes(b"\"data\":")?;
            view.media.write_base64(doc, doc.required(part, "data")?, e)?;
            e.bytes(b"}")?;
        }
        e.bytes(b"]")
    })?;
    let mut socket = TcpStream::connect(address).unwrap();
    let mut offset = 0;
    while offset < output.len() {
        let chunk = output.chunk(offset)?;
        socket.write_all(chunk).unwrap();
        offset += chunk.len();
    }
    Ok(())
}

fn run(input: &[u8], content_type: &str, limits: Limits, chunk: usize, frames: usize) -> (u16, usize, Value, Vec<u8>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let mut mapped = Mapped::configured(Arc::new(ck_stream::ManualClock::default()), frames);
    let result = (|| {
        let boundary = Boundary::new(&mut mapped.writer, content_type)?;
        let mut ingress = Ingress::new(
            &mut mapped.writer,
            &mapped.store,
            identity(),
            Operation::Generate,
            &boundary,
            limits,
        )?;
        for piece in input.chunks(chunk) {
            ingress.advance(&mut mapped.writer, piece)?;
        }
        let ready = ingress.finish(&mut mapped.writer, &Destination)?;
        send(&ready, &mut mapped.writer, listener.local_addr().unwrap())
    })();
    let status = match result {
        Ok(()) => 200,
        Err(Error::Limit | Error::EventTooLarge) => 413,
        Err(Error::Storage(_)) => 503,
        Err(_) => 400,
    };
    let (sends, parts) = match listener.accept() {
        Ok((mut socket, _)) => {
            socket.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
            let mut body = Vec::new();
            socket.read_to_end(&mut body).unwrap();
            (1, serde_json::from_slice(&body).unwrap())
        }
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => (0, Value::Null),
        Err(error) => panic!("loopback accept: {error}"),
    };
    (status, sends, parts, mapped.output(&mapped.writer))
}

fn form(headers: &str, data: &[u8]) -> Vec<u8> {
    let mut body = format!("--xyz\r\n{headers}\r\n\r\n").into_bytes();
    body.extend_from_slice(data);
    body.extend_from_slice(b"\r\n--xyz--\r\n");
    body
}

#[test]
fn multipart_preserves_order_filenames_and_bytes_across_fragmentation() {
    use base64::Engine;
    let many = (0..100_003).map(|i| (i % 251) as u8).collect::<Vec<_>>();
    let repeated = b"--xyz\r\nContent-Disposition: form-data; name=prompt\r\n\r\nhello\r\n--xyz\r\nContent-Disposition: form-data; name=\"image[]\"; filename=\"a%22b.png\"\r\nContent-Type: image/png\r\nAuthorization: not-retained\r\n\r\n\x00\xff\r\n--xyz\r\nContent-Disposition: form-data; name=\"image[]\"; filename=\"\"\r\n\r\n\r\n--xyz--\r\nepilogue";
    let mut cases = vec![
        (
            form("Content-Disposition: form-data; name=f", b"abc"),
            json!([{"name":"f","data":"YWJj"}]),
        ),
        (b"--xyz--".to_vec(), json!([])),
        (
            repeated.to_vec(),
            json!([
                {"name":"prompt","data":"aGVsbG8="},
                {"name":"image[]","filename":"a\"b.png","content_type":"image/png","data":"AP8="},
                {"name":"image[]","filename":"","data":""}
            ]),
        ),
        (
            form("Content-Disposition: form-data; name=f; filename=\"資料.bin\"", &many),
            json!([
                {"name":"f","filename":"資料.bin","data":base64::engine::general_purpose::STANDARD.encode(&many)}
            ]),
        ),
        (
            form("Content-Disposition: form-data; name=f", b"a\r\n--xyzX\r\n--xy\x00z"),
            json!([
                {"name":"f","data":base64::engine::general_purpose::STANDARD.encode(b"a\r\n--xyzX\r\n--xy\x00z")}
            ]),
        ),
    ];
    for suffix in ["", " \t", " \t\r\n", "\r\nepilogue", " \t\r\nepilogue"] {
        cases.push((
            format!("--xyz\r\nContent-Disposition: form-data; name=f\r\n\r\nabc\r\n--xyz--{suffix}").into_bytes(),
            json!([{"name":"f","data":"YWJj"}]),
        ));
    }
    for (input, expected) in cases {
        for content_type in [
            CONTENT_TYPE,
            "multipart/form-data; boundary=\"xyz\"",
            r#"multipart/form-data; boundary="x\yz""#,
            r#"multipart/form-data; other="a\"b\\c"; boundary="\x\y\z""#,
        ] {
            let whole = run(&input, content_type, LIMITS, input.len(), 64);
            assert_eq!((whole.0, whole.1, &whole.2), (200, 1, &expected));
            assert!(!String::from_utf8_lossy(&whole.3).contains("not-retained"));
            for chunk in [1, 2, 7, 83, 8192] {
                assert_eq!(run(&input, content_type, LIMITS, chunk, 64), whole, "chunk={chunk}");
            }
        }
    }
}

#[test]
fn multipart_rejects_malformed_or_oversize_requests_before_sending() {
    let valid = form("Content-Disposition: form-data; name=f; filename=a.bin", b"abc");
    let mut too_many_headers = String::from("Content-Disposition: form-data; name=f");
    for _ in 0..32 {
        too_many_headers.push_str("\r\nX-Extra: ignored");
    }
    let mut cases = vec![
        (valid.clone(), "multipart/form-data", LIMITS, 400),
        (valid.clone(), "multipart/form-data; boundary=\"\"", LIMITS, 400),
        (valid.clone(), r#"multipart/form-data; boundary="x\"yz""#, LIMITS, 400),
        (valid.clone(), r#"multipart/form-data; boundary="x\\yz""#, LIMITS, 400),
        (valid.clone(), r#"multipart/form-data; boundary="xyz\ ""#, LIMITS, 400),
        (
            b"--xyz\r\nContent-Disposition: form-data; name=f\r\n\r\nabc\r\n--xyz--NOT-A-DELIMITER\r\nremaining-file-data".to_vec(),
            CONTENT_TYPE,
            LIMITS,
            400,
        ),
        (
            b"--xyz\r\nContent-Disposition: form-data; name=f\r\n\r\nabc".to_vec(),
            CONTENT_TYPE,
            LIMITS,
            400,
        ),
        (
            form("Content-Disposition form-data; name=f", b"a"),
            CONTENT_TYPE,
            LIMITS,
            400,
        ),
        (form("Content-Disposition: form-data", b"a"), CONTENT_TYPE, LIMITS, 400),
        (
            form("Content-Disposition: attachment; name=f", b"a"),
            CONTENT_TYPE,
            LIMITS,
            400,
        ),
        (
            form(
                "Content-Disposition: form-data; name=f\r\nContent-Disposition: form-data; name=g",
                b"a",
            ),
            CONTENT_TYPE,
            LIMITS,
            400,
        ),
        (
            form(
                "Content-Disposition: form-data; name=f\r\nContent-Type: x\r\nContent-Type: y",
                b"a",
            ),
            CONTENT_TYPE,
            LIMITS,
            400,
        ),
        (
            form(
                &format!("Content-Disposition: form-data; name=\"{}\"", "a".repeat(4096)),
                b"a",
            ),
            CONTENT_TYPE,
            LIMITS,
            413,
        ),
        (form(&too_many_headers, b"a"), CONTENT_TYPE, LIMITS, 413),
        (
            valid.clone(),
            CONTENT_TYPE,
            Limits {
                body_bytes: valid.len() as u64 - 1,
                ..LIMITS
            },
            413,
        ),
        (
            valid.clone(),
            CONTENT_TYPE,
            Limits {
                part_bytes: 2,
                ..LIMITS
            },
            413,
        ),
        (valid, CONTENT_TYPE, Limits { parts: 0, ..LIMITS }, 413),
    ];
    for suffix in ["\r", "\n", " \t\r", "\rX", " \tX", "-", "\x0b\r\n"] {
        for prefix in ["", "--xyz\r\nContent-Disposition: form-data; name=f\r\n\r\nabc\r\n"] {
            cases.push((
                format!("{prefix}--xyz--{suffix}").into_bytes(),
                CONTENT_TYPE,
                LIMITS,
                400,
            ));
        }
    }
    for (index, (input, content_type, limits, expected)) in cases.iter().enumerate() {
        for chunk in [1, 17, input.len()] {
            let (status, sends, _, events) = run(input, content_type, *limits, chunk, 64);
            assert_eq!((status, sends), (*expected, 0), "case={index}, chunk={chunk}");
            assert!(!String::from_utf8_lossy(&events).contains("event: x_ck.request.end"));
        }
    }
    let body = form("Content-Disposition: form-data; name=f", &vec![0; 900_000]);
    let (status, sends, _, _) = run(&body, CONTENT_TYPE, LIMITS, 8192, 24);
    assert_eq!((status, sends), (503, 0));
}

#[test]
fn multipart_flush_publishes_ready_bytes_without_admitting_the_request() {
    let clock = Arc::new(ck_stream::ManualClock::default());
    let mut mapped = Mapped::with_clock(clock.clone());
    let boundary = Boundary::new(&mut mapped.writer, CONTENT_TYPE).unwrap();
    let mut ingress = Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(),
        Operation::Generate,
        &boundary,
        LIMITS,
    )
    .unwrap();
    ingress
        .advance(
            &mut mapped.writer,
            b"--xyz\r\nContent-Disposition: form-data; name=f\r\n\r\na",
        )
        .unwrap();
    let deadline = ingress.flush_deadline().unwrap();
    clock.advance(Duration::from_millis(50));
    ingress.advance(&mut mapped.writer, b"b").unwrap();
    assert_eq!(ingress.flush_deadline(), Some(deadline));
    clock.advance(Duration::from_millis(50));
    ingress.flush(&mut mapped.writer).unwrap();
    assert_eq!(ingress.flush_deadline(), None);
    let events = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
    assert!(events.contains("\"bytes\":\"YWI=\""));
    assert!(!events.contains("event: x_ck.media.end"));
    assert!(!events.contains("event: x_ck.request.end"));
    ingress.advance(&mut mapped.writer, b"c\r\n--xyz--").unwrap();
    let ready = ingress.finish(&mut mapped.writer, &Destination).unwrap();
    let view = ready.view();
    let media = view.media.entries().next().unwrap();
    let output = encode::scratch(&mut mapped.writer, |e| {
        view.media.write_base64(view.document, media.node, e)
    })
    .unwrap();
    assert_eq!(bytes(&output), b"\"YWJj\"");
}
