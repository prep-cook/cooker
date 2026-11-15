#[allow(dead_code, reason = "shared mapped codec harness")]
mod semantic;

use std::{sync::Arc, task::Poll, time::Duration};

use ck_stream::{Read, ScratchLease};
use ck_translate::{
    audio::{self, Decoder, Destination, Egress, Ingress},
    codec::{
        Error, ResponseEgress, ResponseIngress,
        document::Value,
        event::{Direction, Emitter, Identity, Kind, LocalId, Scope},
    },
};
use semantic::Mapped;
use serde_json::json;

fn bytes(lease: &ScratchLease) -> Vec<u8> {
    let mut out = Vec::new();
    while out.len() < lease.len() {
        out.extend_from_slice(lease.chunk(out.len()).unwrap());
    }
    out
}

fn identity() -> Identity {
    Identity {
        request_id: LocalId::new("speech-request").unwrap(),
        stream_id: LocalId::new("speech-response").unwrap(),
        attempt: 0,
        direction: Direction::Response,
    }
}

#[test]
fn speech_requests_preserve_fields_and_project_current_values() {
    let cases = [
        (
            json!({"model":"tts-1", "input":"Hello 🎵", "voice":"alloy"}),
            json!({"text":"Edited", "voice_id":"ara"}),
        ),
        (
            json!({"model":"gpt-4o-mini-tts", "input":"Line\nTwo", "voice":"nova",
                "instructions":"Speak softly", "speed":1.25, "language":"zh",
                "response_format":"wav", "stream_format":"audio", "future":{"a/b":[null,1]},
                "x_ck":{"operation":"realtime"}}),
            json!({"text":"Edited", "voice_id":"eve", "speed":1.25, "language":"zh",
                "output_format":{"codec":"wav", "sample_rate":24000}}),
        ),
        (
            json!({"model":"tts-1", "input":"hello", "voice":"alloy",
                "output_format":{"codec":"wav", "sample_rate":48000}}),
            json!({"text":"Edited", "voice_id":"ara",
                "output_format":{"codec":"wav", "sample_rate":48000}}),
        ),
        (
            json!({"model":"tts-1", "input":"hello", "voice":"alloy",
                "output_format":{"codec":"mp3", "sample_rate":44100, "bit_rate":192000}}),
            json!({"text":"Edited", "voice_id":"ara",
                "output_format":{"codec":"mp3", "sample_rate":44100, "bit_rate":192000}}),
        ),
    ];
    for (wire, expected) in cases {
        let raw = serde_json::to_vec(&wire).unwrap();
        for chunk in [1, raw.len()] {
            let mut mapped = Mapped::new();
            let mut decoder = Decoder::new(&mut mapped.writer, raw.len() as u64).unwrap();
            for part in raw.chunks(chunk) {
                decoder.advance(&mut mapped.writer, part).unwrap();
            }
            let mut doc = decoder.finish(&mut mapped.writer).unwrap();
            // OpenAI has no `output_format`; that refusal is a row of the rejection table.
            if wire.get("output_format").is_none() {
                let encoded = audio::encode(&mut mapped.writer, &doc, Destination::OpenAi).unwrap();
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&bytes(&encoded)).unwrap(),
                    wire
                );
            }
            let x = doc.required(doc.root(), "x_ck").unwrap();
            let params = doc.required(x, "provider_parameters").unwrap();
            let audio = doc.required(params, "audio").unwrap();
            if wire.get("instructions").is_some() {
                let error = audio::encode(&mut mapped.writer, &doc, Destination::Xai).err().unwrap();
                assert_eq!(error.status(), 400);
                assert!(matches!(error.error, Error::Unsupported));
                doc.remove(audio, "instructions").unwrap();
            }
            doc.set(doc.root(), "text", Value::Text("Edited")).unwrap();
            let projected = audio::encode(&mut mapped.writer, &doc, Destination::Xai).unwrap();
            let projected: serde_json::Value = serde_json::from_slice(&bytes(&projected)).unwrap();
            assert_eq!(projected, expected);
            assert!(
                doc.string_is(doc.required(audio, "voice").unwrap(), wire["voice"].as_str().unwrap())
                    .unwrap()
            );
        }
    }
}

#[test]
fn speech_request_rejections_never_send_an_upstream_body() {
    let valid = br#"{"model":"tts-1","input":"hello","voice":"alloy"}"#;
    let cases: &[(&[u8], u64, Destination, u16)] = &[
        (valid, valid.len() as u64 - 1, Destination::OpenAi, 413),
        (valid, valid.len() as u64, Destination::OpenAi, 200),
        (
            br#"{"model":"m","input":"x","voice":"nova","instructions":"quiet"}"#,
            1024,
            Destination::Xai,
            400,
        ),
        (
            br#"{"model":"m","input":"x","voice":"nova","stream_format":"sse"}"#,
            1024,
            Destination::OpenAi,
            400,
        ),
        (
            br#"{"model":"m","input":"x","voice":"nova","response_format":"opus"}"#,
            1024,
            Destination::Xai,
            400,
        ),
        (
            br#"{"model":"m","input":"x","voice":"nova","output_format":{"codec":"wav"}}"#,
            1024,
            Destination::OpenAi,
            400,
        ),
        (
            br#"{"model":"m","input":"x","voice":"nova","output_format":{"codec":"flac"}}"#,
            1024,
            Destination::Xai,
            400,
        ),
        (
            br#"{"model":"m","input":"x","voice":"nova","output_format":{"codec":"WAV"}}"#,
            1024,
            Destination::Xai,
            400,
        ),
        (
            br#"{"model":"m","input":"x","voice":"nova","response_format":"opus","output_format":{"codec":"wav"}}"#,
            1024,
            Destination::Xai,
            400,
        ),
        (
            br#"{"model":"m","input":"x","voice":"nova","response_format":"mp3","output_format":{"codec":"wav"}}"#,
            1024,
            Destination::OpenAi,
            400,
        ),
        (
            br#"{"model":"m","input":"x","voice":"nova","output_format":{"codec":"wav","future":{"a":1}}}"#,
            1024,
            Destination::Xai,
            400,
        ),
        (
            br#"{"model":"m","input":"x","voice":"nova","output_format":{"codec":"wav","sample_rate":0}}"#,
            1024,
            Destination::Xai,
            400,
        ),
        (
            br#"{"model":"m","input":"x","voice":"nova","output_format":{"codec":"mp3","bit_rate":-1}}"#,
            1024,
            Destination::Xai,
            400,
        ),
        (
            br#"{"model":"m","input":"x","voice":"nova","speed":null}"#,
            1024,
            Destination::OpenAi,
            400,
        ),
        (
            br#"{"model":"m","input":"x","voice":"nova","input":"y"}"#,
            1024,
            Destination::OpenAi,
            400,
        ),
        (br#"{"model":"m","input":"x"}"#, 1024, Destination::OpenAi, 400),
        (b"{", 1024, Destination::OpenAi, 400),
        (b"{\"model\":\"\xff\"}", 1024, Destination::OpenAi, 400),
    ];
    for &(raw, limit, destination, expected) in cases {
        for chunk in [1, raw.len()] {
            let mut mapped = Mapped::new();
            let mut sends = Vec::new();
            let result = (|| {
                let mut decoder = Decoder::new(&mut mapped.writer, limit)?;
                for part in raw.chunks(chunk) {
                    decoder.advance(&mut mapped.writer, part)?;
                }
                let doc = decoder.finish(&mut mapped.writer)?;
                let body = audio::encode(&mut mapped.writer, &doc, destination)?;
                sends.push(bytes(&body));
                Ok::<_, audio::RequestFailure>(())
            })();
            assert_eq!(result.err().map_or(200, |error| error.status()), expected, "{raw:?}");
            if expected == 200 {
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&sends[0]).unwrap(),
                    serde_json::from_slice::<serde_json::Value>(raw).unwrap()
                );
            } else {
                assert!(sends.is_empty());
            }
        }
    }
}

fn drain(mapped: &mut Mapped, egress: &mut Egress, reader: &mut ck_stream::Reader) -> Result<Vec<u8>, Error> {
    let mut out = Vec::new();
    while let Poll::Ready(Some(Read::Record(lease))) = reader.try_next() {
        egress.advance(&mut mapped.writer, lease, |lease| {
            out.extend(bytes(&lease));
            Ok(())
        })?;
    }
    Ok(out)
}

fn content_type(egress: &Egress) -> Option<String> {
    egress.content_type().unwrap().map(|(doc, node)| {
        let mut chars = doc.chars(node).unwrap();
        let mut value = String::new();
        while let Some(c) = chars.next_scalar().unwrap() {
            value.push(c);
        }
        value
    })
}

#[test]
fn speech_audio_is_byte_exact_and_keeps_content_type_across_fragmentation() {
    let cases = [
        (Vec::new(), None),
        (vec![0, 255, 254, 13, 10, 0], Some("application/octet-stream")),
        (
            (0..75_000).map(|n| (n % 251) as u8).collect(),
            Some("audio/ogg; codecs=opus"),
        ),
    ];
    for (raw, mime) in cases {
        let mut whole = None;
        for chunk in [raw.len().max(1), 1] {
            let mut mapped = Mapped::new();
            let mut ingress = Ingress::new(
                &mut mapped.writer,
                identity(),
                mime,
                Some(raw.len() as u64),
                raw.len() as u64,
            )
            .unwrap();
            let mut egress = Egress::new(&mapped.writer, Some(identity()));
            let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
            let mut output = Vec::new();
            for part in raw.chunks(chunk) {
                ingress.advance(&mut mapped.writer, part).unwrap();
                output.extend(drain(&mut mapped, &mut egress, &mut reader).unwrap());
            }
            ingress.finish(&mut mapped.writer).unwrap();
            output.extend(drain(&mut mapped, &mut egress, &mut reader).unwrap());
            egress.finish().unwrap();
            assert_eq!(output, raw);
            assert_eq!(content_type(&egress).as_deref(), mime);
            let canonical = mapped.output(&mapped.writer);
            if let Some(whole) = &whole {
                assert_eq!(&canonical, whole);
            } else {
                whole = Some(canonical);
            }
        }
    }
}

#[test]
fn speech_partial_audio_flushes_without_extending_the_deadline_or_completing() {
    for empty_advance in [false, true] {
        let clock = Arc::new(ck_stream::ManualClock::default());
        let mut mapped = Mapped::with_clock(clock.clone());
        let mut ingress = Ingress::new(&mut mapped.writer, identity(), Some("audio/pcm"), None, 100).unwrap();
        let mut egress = Egress::new(&mapped.writer, None);
        let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
        ingress.advance(&mut mapped.writer, b"a").unwrap();
        let deadline = ingress.flush_deadline().unwrap();
        clock.advance(Duration::from_millis(99));
        ingress.advance(&mut mapped.writer, b"b").unwrap();
        assert_eq!(ingress.flush_deadline(), Some(deadline));
        assert!(drain(&mut mapped, &mut egress, &mut reader).unwrap().is_empty());
        clock.advance(Duration::from_millis(1));
        if empty_advance {
            ingress.advance(&mut mapped.writer, b"").unwrap();
        } else {
            ingress.flush(&mut mapped.writer).unwrap();
        }
        assert_eq!(drain(&mut mapped, &mut egress, &mut reader).unwrap(), b"ab");
        assert!(ingress.flush_deadline().is_none());
        ingress.advance(&mut mapped.writer, b"c").unwrap();
        ingress.finish(&mut mapped.writer).unwrap();
        assert_eq!(drain(&mut mapped, &mut egress, &mut reader).unwrap(), b"c");
        egress.finish().unwrap();
    }
}

#[test]
fn speech_incomplete_or_oversized_audio_ends_with_error_not_success() {
    // declared length, actual bytes, ceiling, explicit HTTP body failure
    for (declared, raw, limit, fail) in [
        (Some(4), b"abc".as_slice(), 100, false),
        (Some(2), b"abc".as_slice(), 100, false),
        (None, b"abc".as_slice(), 2, false),
        (Some(3), b"abc".as_slice(), 100, true),
        (None, b"abc".as_slice(), 100, true),
        (Some(4), b"".as_slice(), 3, false),
    ] {
        let mut mapped = Mapped::new();
        if let Ok(mut ingress) = Ingress::new(&mut mapped.writer, identity(), None, declared, limit) {
            let fed = ingress.advance(&mut mapped.writer, raw);
            if fed.is_ok() {
                if fail {
                    ingress.fail(&mut mapped.writer, 502, "upstream").unwrap();
                } else {
                    assert!(ingress.finish(&mut mapped.writer).is_err());
                }
            }
            assert!(matches!(ingress.finish(&mut mapped.writer), Err(Error::Closed)));
        }
        let canonical = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
        assert!(canonical.contains("event: x_ck.error\n"));
        assert!(!canonical.contains("event: x_ck.done\n"));
        assert!(!canonical.contains("event: x_ck.media.end\n"));
        let mut egress = Egress::new(&mapped.writer, None);
        let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
        assert!(drain(&mut mapped, &mut egress, &mut reader).is_err());
        assert!(egress.finish().is_err());
    }
}

#[test]
fn speech_egress_refuses_broken_media_order_and_identity_before_output() {
    for (index, offset, id, payload, wrong_identity) in [
        (1, 0, "m0", "YWJj", false),
        (0, 1, "m0", "YWJj", false),
        (0, 0, "other", "YWJj", false),
        (0, 0, "m0", "YWJj!", false),
        (0, 0, "m0", "YWJj", true),
    ] {
        let mut mapped = Mapped::new();
        let mut emitter = Emitter::new(identity());
        emitter
            .emit(
                &mut mapped.writer,
                Kind::ResponseStart,
                Scope::default(),
                |e| e.bytes(b",\"id\":\"r\",\"model\":\"m\",\"created\":0"),
                |_| Ok(()),
            )
            .unwrap();
        emitter
            .emit(
                &mut mapped.writer,
                Kind::MediaStart,
                Scope {
                    media_id: Some(LocalId::new("m0").unwrap()),
                    ..Scope::default()
                },
                |e| e.bytes(b",\"kind\":\"audio\",\"encoding\":\"base64\""),
                |_| Ok(()),
            )
            .unwrap();
        emitter
            .emit(
                &mut mapped.writer,
                Kind::Data,
                Scope {
                    media_id: Some(LocalId::new(id).unwrap()),
                    ..Scope::default()
                },
                |e| {
                    e.bytes(b",\"index\":")?;
                    e.unsigned(index)?;
                    e.bytes(b",\"offset\":")?;
                    e.unsigned(offset)?;
                    e.bytes(b",\"bytes\":")?;
                    e.string(payload)
                },
                |_| Ok(()),
            )
            .unwrap();
        let expected = Identity {
            attempt: u64::from(wrong_identity),
            ..identity()
        };
        let mut egress = Egress::new(&mapped.writer, Some(expected));
        let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
        let mut sends = Vec::new();
        let result = (|| {
            while let Poll::Ready(Some(Read::Record(lease))) = reader.try_next() {
                egress.advance(&mut mapped.writer, lease, |lease| {
                    sends.push(bytes(&lease));
                    Ok(())
                })?;
            }
            Ok::<_, Error>(())
        })();
        assert!(result.is_err());
        assert!(sends.is_empty());
        assert!(egress.finish().is_err());
    }
}
