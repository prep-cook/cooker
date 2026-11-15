#[allow(dead_code, reason = "these tests use only the mapped store and writer")]
mod semantic;

use base64::Engine;
use ck_translate::{
    codec::{
        Error,
        document::Document,
        encode,
        event::{Direction, Identity, LocalId},
        provenance,
        source::Source,
    },
    video::{Action, Decoded, Decoder, Dialect, Phase, Usage, content::Download, encode as encode_video},
};
use semantic::Mapped;
use serde_json::{Value, json};

fn bytes(lease: &ck_stream::ScratchLease) -> Vec<u8> {
    let mut bytes = Vec::new();
    while bytes.len() < lease.len() {
        bytes.extend_from_slice(lease.chunk(bytes.len()).unwrap());
    }
    bytes
}

fn dialect(name: &str) -> Dialect {
    match name {
        "openai" => Dialect::OpenAi,
        "xai" => Dialect::Xai,
        other => panic!("unknown dialect {other}"),
    }
}

fn phase(name: &str) -> Phase {
    match name {
        "create" => Phase::Request(Action::Create),
        "edit" => Phase::Request(Action::Edit),
        "extend" => Phase::Request(Action::Extend),
        "retrieve" => Phase::Request(Action::Retrieve),
        "content" => Phase::Request(Action::Content),
        "job" => Phase::Job,
        "accepted" => Phase::Accepted,
        other => panic!("unknown phase {other}"),
    }
}

fn decode(
    mapped: &mut Mapped,
    dialect: Dialect,
    phase: Phase,
    job_id: Option<&str>,
    input: &[u8],
    chunk: usize,
) -> Result<Decoded, Error> {
    let mut decoder = Decoder::new(&mut mapped.writer, dialect, phase, job_id, input.len() as u64 + 1)?;
    for bytes in input.chunks(chunk) {
        decoder.advance(&mut mapped.writer, bytes)?;
    }
    decoder.finish(&mut mapped.writer)
}

fn wire_bytes(wire: &Value) -> Vec<u8> {
    if wire.is_null() {
        Vec::new()
    } else {
        serde_json::to_vec_pretty(wire).unwrap()
    }
}

#[test]
fn video_json_round_trips_through_canonical_storage() {
    let rows: Vec<Value> = serde_json::from_str(include_str!("fixtures/video.json")).unwrap();
    for row in rows {
        let name = row["name"].as_str().unwrap();
        let (dialect, phase) = (
            dialect(row["dialect"].as_str().unwrap()),
            phase(row["phase"].as_str().unwrap()),
        );
        let wire = wire_bytes(&row["wire"]);
        for chunk in [1, 7, wire.len().max(1)] {
            let mut mapped = Mapped::new();
            let decoded = decode(&mut mapped, dialect, phase, row["job_id"].as_str(), &wire, chunk).expect(name);
            let operation = decoded.operation;
            let stored = encode::scratch(&mut mapped.writer, |e| operation.write(operation.root(), e)).unwrap();
            let canonical: Value = serde_json::from_slice(&bytes(&stored)).unwrap();
            assert_eq!(canonical["kind"], row["kind"], "{name}");
            assert_eq!(canonical["value"], row["value"], "{name}");
            assert_eq!(canonical["x_ck"]["v"], 1, "{name}");
            assert_eq!(canonical["x_ck"]["original_protocol"], row["dialect"], "{name}");
            drop(operation);
            let mut source = Source::new(&mut mapped.writer).unwrap();
            source.push_scratch(stored).unwrap();
            let len = source.len();
            let mut restored = Document::parse(&mut mapped.writer, source, len).unwrap();
            provenance::restore_bindings(&mut restored).unwrap();
            let output = encode_video(&mut mapped.writer, &restored, dialect).expect(name);
            match output {
                None => assert!(row["wire"].is_null(), "{name}"),
                Some(lease) => {
                    assert_eq!(
                        serde_json::from_slice::<Value>(&bytes(&lease)).unwrap(),
                        row["wire"],
                        "{name}"
                    )
                }
            }
        }
    }
}

#[test]
fn video_rejects_invalid_or_incomplete_documents() {
    use Action::{Create, Edit, Retrieve};
    let cases: &[(Phase, Dialect, Option<&str>, &[u8])] = &[
        (Phase::Request(Create), Dialect::OpenAi, None, b""),
        (Phase::Request(Create), Dialect::OpenAi, None, br#"{"prompt":false}"#),
        (Phase::Request(Create), Dialect::OpenAi, None, br#"{"seconds":[]}"#),
        (Phase::Request(Create), Dialect::Xai, None, br#"{"image":"https://x"}"#),
        (
            Phase::Request(Create),
            Dialect::Xai,
            None,
            br#"{"prompt":"a","prompt":"b"}"#,
        ),
        (Phase::Request(Create), Dialect::Xai, None, br#"{"prompt":"a"}{}"#),
        (Phase::Request(Create), Dialect::Xai, None, br#"{"prompt":"unfinished"#),
        (Phase::Request(Create), Dialect::Xai, None, b"{\"prompt\":\"\xff\"}"),
        (Phase::Request(Create), Dialect::Xai, None, b"[]"),
        (Phase::Request(Edit), Dialect::Xai, Some("r"), br#"{"video":"x"}"#),
        (Phase::Request(Retrieve), Dialect::Xai, Some("r"), br#"{"x":"#),
        (Phase::Job, Dialect::Xai, Some("r"), br#"{}"#),
        (Phase::Job, Dialect::Xai, Some("r"), br#"{"status":"mystery"}"#),
        (Phase::Job, Dialect::Xai, Some("r"), br#"{"status":7}"#),
        (
            Phase::Job,
            Dialect::Xai,
            Some("r"),
            br#"{"status":"done","progress":101}"#,
        ),
        (
            Phase::Job,
            Dialect::Xai,
            Some("r"),
            br#"{"status":"done","progress":1.5}"#,
        ),
        (Phase::Job, Dialect::OpenAi, None, br#"{"status":"queued"}"#),
        (Phase::Job, Dialect::OpenAi, None, br#"{"id":1,"status":"queued"}"#),
        (
            Phase::Job,
            Dialect::Xai,
            None,
            br#"{"id":"a","request_id":"b","status":"done"}"#,
        ),
        (
            Phase::Job,
            Dialect::OpenAi,
            None,
            br#"{"id":"a","status":"queued","created_at":-1}"#,
        ),
        (
            Phase::Job,
            Dialect::OpenAi,
            None,
            br#"{"id":"a","status":"failed","error":{"code":1,"message":"m"}}"#,
        ),
        (Phase::Job, Dialect::Xai, Some("r"), br#"{"status":"done","usage":[]}"#),
        (Phase::Accepted, Dialect::Xai, None, br#"{}"#),
    ];
    for &(phase, dialect, job_id, input) in cases {
        for chunk in [1, input.len().max(1)] {
            let mut mapped = Mapped::new();
            assert!(
                decode(&mut mapped, dialect, phase, job_id, input, chunk).is_err(),
                "accepted {input:?}"
            );
        }
    }
    let mut mapped = Mapped::new();
    for (phase, job_id) in [
        (Phase::Request(Create), Some("v")),
        (Phase::Request(Edit), None),
        (Phase::Request(Retrieve), Some("")),
    ] {
        assert!(Decoder::new(&mut mapped.writer, Dialect::Xai, phase, job_id, 8).is_err());
    }
}

#[test]
fn video_body_limit_refuses_extra_bytes_without_recovery() {
    let mut mapped = Mapped::new();
    let mut decoder = Decoder::new(
        &mut mapped.writer,
        Dialect::Xai,
        Phase::Request(Action::Create),
        None,
        1,
    )
    .unwrap();
    decoder.advance(&mut mapped.writer, b"{").unwrap();
    assert!(matches!(
        decoder.advance(&mut mapped.writer, b"}"),
        Err(Error::EventTooLarge)
    ));
    assert!(matches!(decoder.advance(&mut mapped.writer, b""), Err(Error::Closed)));
    assert!(decoder.finish(&mut mapped.writer).is_err());
}

fn encode_job(source: Dialect, phase: Phase, wire: &Value, destination: Dialect) -> Result<Value, Error> {
    let mut mapped = Mapped::new();
    let operation = decode(
        &mut mapped,
        source,
        phase,
        Some("video_1"),
        &serde_json::to_vec(wire).unwrap(),
        3,
    )?
    .operation;
    let stored = encode::scratch(&mut mapped.writer, |e| operation.write(operation.root(), e))?;
    let mut input = Source::new(&mut mapped.writer)?;
    input.push_scratch(stored)?;
    let len = input.len();
    let mut restored = Document::parse(&mut mapped.writer, input, len)?;
    provenance::restore_bindings(&mut restored)?;
    let lease = encode_video(&mut mapped.writer, &restored, destination)?.unwrap();
    Ok(serde_json::from_slice(&bytes(&lease)).unwrap())
}

#[test]
fn video_job_egress_keeps_status_and_identity_and_refuses_what_a_dialect_lacks() {
    use Dialect::{OpenAi, Xai};
    // The same job in each dialect's shape; `video_1` is also the id the route supplies.
    let openai = |status| json!({"id": "video_1", "status": status});
    let xai = |status| json!({"request_id": "video_1", "status": status});
    // OpenAI shape produced from an xAI job: gains `object`.
    let from_xai = |status| json!({"id": "video_1", "object": "video", "status": status});
    // (source, phase, wire, destination, expected wire; None means refused)
    let cases = [
        (OpenAi, Phase::Job, openai("queued"), OpenAi, Some(openai("queued"))),
        (
            OpenAi,
            Phase::Job,
            openai("in_progress"),
            OpenAi,
            Some(openai("in_progress")),
        ),
        (OpenAi, Phase::Job, openai("in_progress"), Xai, Some(xai("pending"))),
        (OpenAi, Phase::Job, openai("completed"), Xai, Some(xai("done"))),
        (OpenAi, Phase::Job, openai("failed"), Xai, Some(xai("failed"))),
        (OpenAi, Phase::Job, openai("expired"), Xai, Some(xai("expired"))),
        (
            OpenAi,
            Phase::Job,
            openai("cancelled"),
            OpenAi,
            Some(openai("cancelled")),
        ),
        (OpenAi, Phase::Job, openai("cancelled"), Xai, None),
        (Xai, Phase::Job, xai("pending"), OpenAi, Some(from_xai("queued"))),
        (Xai, Phase::Job, xai("done"), OpenAi, Some(from_xai("completed"))),
        (Xai, Phase::Job, xai("done"), Xai, Some(xai("done"))),
        (Xai, Phase::Job, xai("expired"), OpenAi, Some(from_xai("expired"))),
        (Xai, Phase::Job, xai("expired"), Xai, Some(xai("expired"))),
        (Xai, Phase::Job, xai("failed"), OpenAi, Some(from_xai("failed"))),
        (Xai, Phase::Job, xai("canceled"), OpenAi, Some(from_xai("cancelled"))),
        // xAI's GET body names neither the job nor, for an acknowledgement, a status. They are
        // written for another dialect, whose job object requires both, and stay implied for xAI.
        (
            Xai,
            Phase::Job,
            json!({"status": "pending"}),
            OpenAi,
            Some(from_xai("queued")),
        ),
        (
            Xai,
            Phase::Job,
            json!({"status": "pending"}),
            Xai,
            Some(json!({"status": "pending"})),
        ),
        (
            Xai,
            Phase::Accepted,
            json!({"request_id": "video_1"}),
            OpenAi,
            Some(from_xai("queued")),
        ),
        (
            Xai,
            Phase::Accepted,
            json!({"request_id": "video_1"}),
            Xai,
            Some(json!({"request_id": "video_1"})),
        ),
    ];
    for (source, phase, wire, destination, expected) in cases {
        let result = encode_job(source, phase, &wire, destination);
        match expected {
            Some(expected) => assert_eq!(result.unwrap(), expected, "{wire} {phase:?} to {destination:?}"),
            None => assert!(matches!(result, Err(Error::Unsupported)), "{wire}"),
        }
    }
}

#[test]
fn video_options_a_destination_lacks_are_refused_not_dropped() {
    use Action::{Content, Create, Edit, Extend};
    use Dialect::{OpenAi, Xai};
    // (action, source, wire, destination, accepted)
    let cases = [
        (Create, OpenAi, json!({"prompt": "p", "seconds": "8"}), Xai, true),
        (Create, OpenAi, json!({"prompt": "p", "size": "1280x720"}), Xai, false),
        (
            Create,
            OpenAi,
            json!({"prompt": "p", "input_reference": {"image_url": "u"}}),
            Xai,
            false,
        ),
        (Create, Xai, json!({"prompt": "p", "duration": 6}), OpenAi, false),
        (
            Create,
            Xai,
            json!({"prompt": "p", "reference_images": [{"url": "u"}]}),
            OpenAi,
            false,
        ),
        (Create, Xai, json!({"prompt": "p", "resolution": "720p"}), OpenAi, false),
        (Create, Xai, json!({"prompt": "p", "model": "m"}), OpenAi, true),
        (
            Edit,
            Xai,
            json!({"prompt": "p", "output": {"upload_url": "u"}}),
            OpenAi,
            false,
        ),
        (Edit, OpenAi, json!({"prompt": "p", "video": {"id": "v"}}), Xai, true),
        // xAI's extension takes `duration`, not `seconds` (docs.x.ai videos.md, POST /v1/videos/extensions).
        (
            Extend,
            OpenAi,
            json!({"prompt": "p", "seconds": "4", "video": {"id": "v"}}),
            Xai,
            false,
        ),
        (Extend, Xai, json!({"prompt": "p", "duration": 4}), OpenAi, false),
        (
            Extend,
            Xai,
            json!({"prompt": "p", "duration": 4, "video": {"url": "u"}}),
            Xai,
            true,
        ),
        (Extend, Xai, json!({"prompt": "p", "resolution": "720p"}), Xai, false),
        (Extend, Xai, json!({"prompt": "p", "seconds": "4"}), Xai, false),
        (Extend, Xai, json!({"prompt": "p", "user": "u"}), Xai, false),
        // xAI's edit takes neither `duration`, `aspect_ratio` nor `resolution`.
        (Edit, Xai, json!({"prompt": "p", "resolution": "720p"}), Xai, false),
        (Edit, Xai, json!({"prompt": "p", "duration": 4}), Xai, false),
        (Edit, Xai, json!({"prompt": "p", "aspect_ratio": "16:9"}), Xai, false),
        (
            Edit,
            Xai,
            json!({"prompt": "p", "model": "m", "user": "u", "video": {"url": "u"}}),
            Xai,
            true,
        ),
        // OpenAI's edit and extension bodies list no `model`.
        (
            Edit,
            OpenAi,
            json!({"prompt": "p", "model": "sora-2", "video": {"id": "v"}}),
            OpenAi,
            false,
        ),
        (
            Extend,
            OpenAi,
            json!({"prompt": "p", "model": "sora-2", "video": {"id": "v"}}),
            OpenAi,
            false,
        ),
        (
            Extend,
            OpenAi,
            json!({"prompt": "p", "seconds": "4", "video": {"id": "v"}}),
            OpenAi,
            true,
        ),
        (Create, Xai, json!({"prompt": "p", "user": "u"}), Xai, true),
        (Create, Xai, json!({"prompt": "p", "user": "u"}), OpenAi, false),
        (Content, OpenAi, json!({"variant": "video"}), Xai, false),
        (Content, OpenAi, json!({"variant": "video"}), OpenAi, true),
    ];
    for (action, source, wire, destination, accepted) in cases {
        let mut mapped = Mapped::new();
        let id = (action != Create).then_some("v");
        let operation = decode(
            &mut mapped,
            source,
            Phase::Request(action),
            id,
            &serde_json::to_vec(&wire).unwrap(),
            5,
        )
        .unwrap()
        .operation;
        let stored = encode::scratch(&mut mapped.writer, |e| operation.write(operation.root(), e)).unwrap();
        let mut input = Source::new(&mut mapped.writer).unwrap();
        input.push_scratch(stored).unwrap();
        let len = input.len();
        let mut restored = Document::parse(&mut mapped.writer, input, len).unwrap();
        provenance::restore_bindings(&mut restored).unwrap();
        let result = encode_video(&mut mapped.writer, &restored, destination);
        if accepted {
            assert_eq!(
                serde_json::from_slice::<Value>(&bytes(&result.unwrap().unwrap())).unwrap(),
                wire
            );
        } else {
            assert!(matches!(result, Err(Error::Unsupported)), "{wire}");
        }
    }
}

#[test]
fn video_usage_is_recorded_from_the_first_poll_that_reports_it_and_never_twice() {
    let usage = |ticks, input, output, total| Usage {
        cost_in_usd_ticks: ticks,
        input_tokens: input,
        output_tokens: output,
        total_tokens: total,
    };
    // (wire, job already charged, expected). xAI's `usage` has no status condition, so a pending
    // poll may be the first to report it (E-Q13); absent, null or empty usage reports nothing.
    let cases = [
        (
            json!({"status": "done", "usage": {"cost_in_usd_ticks": 120000000}}),
            false,
            Some(usage(Some(120000000), None, None, None)),
        ),
        (
            json!({"status": "done", "usage": {"cost_in_usd_ticks": 5, "input_tokens": 1, "output_tokens": 2, "total_tokens": 3}}),
            false,
            Some(usage(Some(5), Some(1), Some(2), Some(3))),
        ),
        (
            json!({"status": "failed", "usage": {"cost_in_usd_ticks": 0}}),
            false,
            Some(usage(Some(0), None, None, None)),
        ),
        (
            json!({"status": "pending", "usage": {"cost_in_usd_ticks": 7}}),
            false,
            Some(usage(Some(7), None, None, None)),
        ),
        (
            json!({"status": "pending", "usage": {"cost_in_usd_ticks": 7}}),
            true,
            None,
        ),
        (json!({"status": "done", "usage": {"cost_in_usd_ticks": 7}}), true, None),
        (json!({"status": "done"}), false, None),
        (json!({"status": "done", "usage": null}), false, None),
        (json!({"status": "done", "usage": {}}), false, None),
        (
            json!({"status": "done", "usage": {"cost_in_usd_ticks": null}}),
            false,
            None,
        ),
        (json!({"status": "pending"}), false, None),
    ];
    for (wire, charged, expected) in cases {
        let mut mapped = Mapped::new();
        let mut decoder = Decoder::new(
            &mut mapped.writer,
            Dialect::Xai,
            Phase::Job,
            Some("r"),
            serde_json::to_vec(&wire).unwrap().len() as u64,
        )
        .unwrap()
        .usage_charged(charged);
        decoder
            .advance(&mut mapped.writer, &serde_json::to_vec(&wire).unwrap())
            .unwrap();
        let decoded = decoder.finish(&mut mapped.writer).unwrap();
        assert_eq!(decoded.usage, expected, "{wire} charged={charged}");
    }
}

fn pattern(len: usize) -> Vec<u8> {
    let mut state = 0x2545_f491_4f6c_dd1d_u64;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 24) as u8
        })
        .collect()
}

fn response_identity() -> Identity {
    Identity {
        request_id: LocalId::new("request").unwrap(),
        stream_id: LocalId::new("stream").unwrap(),
        attempt: 1,
        direction: Direction::Response,
    }
}

/// `(event name, data object, record length)` per published record.
fn records(text: &[u8]) -> Vec<(String, Value, usize)> {
    std::str::from_utf8(text)
        .unwrap()
        .split_terminator("\n\n")
        .map(|block| {
            let name = block.lines().find_map(|l| l.strip_prefix("event: ")).unwrap_or("");
            let data = block.lines().find_map(|l| l.strip_prefix("data: ")).unwrap();
            (name.to_owned(), serde_json::from_str(data).unwrap(), block.len() + 2)
        })
        .collect()
}

fn download(content: &[u8], mime: Option<&str>, declared: Option<u64>, chunk: usize) -> (Vec<u8>, Result<u64, Error>) {
    let mut mapped = Mapped::new();
    let mut download = Download::new(&mut mapped.writer, response_identity(), "req-1", mime, declared).unwrap();
    for bytes in content.chunks(chunk) {
        download.advance(&mut mapped.writer, bytes).unwrap();
    }
    let result = download.finish(&mut mapped.writer);
    (mapped.output(&mapped.writer), result)
}

#[test]
fn video_content_is_published_byte_for_byte_with_its_mime_at_any_feed_size() {
    let content = pattern(70_000);
    // A MIME is kept exactly as sent; none sent means none published, never a guess.
    for mime in [Some("video/mp4; codecs=\"avc1.42E01E\""), None] {
        let mut baseline = None;
        for chunk in [1, 4097, content.len()] {
            let (output, result) = download(&content, mime, Some(content.len() as u64), chunk);
            assert_eq!(result.unwrap(), content.len() as u64);
            let events = records(&output);
            let names: Vec<&str> = events.iter().map(|(name, ..)| name.as_str()).collect();
            assert_eq!(names.first(), Some(&"x_ck.response.start"));
            assert_eq!(names[1], "x_ck.media.start");
            assert_eq!(names[names.len() - 2], "x_ck.media.end");
            assert_eq!(names.last(), Some(&"x_ck.done"));
            // The canonical `response.start` payload: the job id, with the two fields no
            // upstream reports marked synthetic.
            let start = &events[0].1;
            assert_eq!(
                (&start["id"], &start["model"], &start["created"]),
                (&json!("req-1"), &json!(""), &json!(0))
            );
            assert_eq!(start["x_ck"]["synthetic"], json!(["model", "created"]));
            assert_eq!(events[1].1.get("mime").and_then(Value::as_str), mime, "{mime:?}");
            assert_eq!(events[1].1["kind"], "video");
            let mut joined = Vec::new();
            for (name, data, length) in &events {
                assert!(*length <= 50_000, "{name} record is {length} bytes");
                if name == "x_ck.data" {
                    assert_eq!(data["offset"], joined.len());
                    joined.extend(
                        base64::engine::general_purpose::STANDARD
                            .decode(data["bytes"].as_str().unwrap())
                            .unwrap(),
                    );
                }
            }
            assert_eq!(joined, content);
            assert_eq!(events[names.len() - 2].1["bytes"], content.len());
            match &baseline {
                None => baseline = Some(output),
                Some(whole) => assert_eq!(&output, whole, "feed size {chunk}"),
            }
        }
    }
}

#[test]
fn video_content_without_a_declared_length_ends_at_finish() {
    let (output, result) = download(&pattern(10), Some("video/mp4"), None, 3);
    assert_eq!(result.unwrap(), 10);
    assert_eq!(records(&output).last().unwrap().0, "x_ck.done");
    let (output, result) = download(&[], None, Some(0), 1);
    assert_eq!(result.unwrap(), 0);
    assert_eq!(records(&output).last().unwrap().0, "x_ck.done");
}

#[test]
fn video_content_truncation_is_an_error_not_a_shorter_success() {
    let content = pattern(10);
    // (bytes fed, declared length)
    for (fed, declared) in [(6, 10), (0, 10), (11, 10)] {
        let mut mapped = Mapped::new();
        let mut download = Download::new(
            &mut mapped.writer,
            response_identity(),
            "req-1",
            Some("video/mp4"),
            Some(declared),
        )
        .unwrap();
        let fed_result = download.advance(&mut mapped.writer, &pattern(11)[..fed]);
        // A byte past the declared length is refused when it arrives, not at the end.
        assert_eq!(fed_result.is_err(), fed > declared as usize, "fed {fed} of {declared}");
        let result = fed_result.and_then(|()| download.finish(&mut mapped.writer));
        assert!(result.is_err(), "fed {fed} of {declared}");
        let names: Vec<String> = records(&mapped.output(&mapped.writer))
            .into_iter()
            .map(|(n, ..)| n)
            .collect();
        assert_eq!(names.last().unwrap(), "x_ck.error", "{names:?}");
        assert!(!names.iter().any(|n| n == "x_ck.media.end" || n == "x_ck.done"));
    }
    // A transport failure mid-body is the same error.
    let mut mapped = Mapped::new();
    let mut download = Download::new(
        &mut mapped.writer,
        response_identity(),
        "req-1",
        Some("video/mp4"),
        None,
    )
    .unwrap();
    download.advance(&mut mapped.writer, &content[..4]).unwrap();
    assert!(matches!(download.abort(&mut mapped.writer), Error::Truncated));
    let names: Vec<String> = records(&mapped.output(&mapped.writer))
        .into_iter()
        .map(|(n, ..)| n)
        .collect();
    assert_eq!(names.last().unwrap(), "x_ck.error");
}

#[test]
fn video_content_refuses_an_unusable_mime_direction_or_foreign_writer() {
    let mut mapped = Mapped::new();
    for mime in ["", "video", "video/mp4\r\nx: y", "video/mp4;;;\u{0}"] {
        assert!(
            Download::new(&mut mapped.writer, response_identity(), "r", Some(mime), None).is_err(),
            "{mime:?}"
        );
    }
    let mut request = response_identity();
    request.direction = Direction::Request;
    assert!(Download::new(&mut mapped.writer, request, "r", Some("video/mp4"), None).is_err());
    // Records go only to the stream whose writer opened the download.
    let mut download = Download::new(&mut mapped.writer, response_identity(), "r", None, None).unwrap();
    let mut other = Mapped::new();
    assert!(matches!(
        download.advance(&mut other.writer, b"x"),
        Err(Error::Position)
    ));
    assert!(matches!(download.abort(&mut other.writer), Error::Position));
}
