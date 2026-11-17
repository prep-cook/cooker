use super::{Fault, Reader};
use crate::realtime::envelope::{At, Base64, Count, N, T};

/// Messages covering every reader state: escapes split across feeds, surrogate pairs,
/// multi-byte UTF-8, numbers in every form, media runs, nested and skipped containers.
const MESSAGES: [&str; 8] = [
    r#"{"type":"response.done","event_id":"e\u0031","response":{"id":"resp_1","status":"completed","status_details":null,"usage":{"total_tokens":3,"input_tokens":1,"output_tokens":2,"input_token_details":{"cached_tokens":0,"audio_tokens":1},"output_token_details":{"audio_tokens":2}},"output":[{"content":[{"transcript":"h\u00e9 \ud83c\udfb5 é 🎵"}]}]}}"#,
    r#" { "audio" : "QU\/D==" , "type":"input_audio_buffer.append" , "n" : [ -0.5e-3 , 1E+2 , 0 , true , false , null , { } , [ ] ] } "#,
    r#"{"type":"rate_limits.updated","rate_limits":[{"name":"requests","limit":10,"remaining":9,"reset_seconds":1.2345},{"name":"tokens","remaining":5,"reset_seconds":7},3,"x"]}"#,
    r#"{"type":"conversation.item.create","item":{"id":"item_1","content":[{"type":"input_image","image_url":"data:image/png;base64,QUJDRA=="},{"type":"input_audio","audio":"QUJD"}]}}"#,
    r#"{"type":"error","error":{"type":"invalid_request_error","code":null,"message":"m","event_id":"c1"}}"#,
    r#"{"type":"session.update","session":{"model":"gpt-realtime","model":"x"}}"#,
    r#"{"type":"input_audio_buffer.commit","x":tru}"#,
    r#"[{"type":"input_audio_buffer.commit"}]"#,
];

fn whole(message: &[u8]) -> Result<super::Envelope, Fault> {
    Reader::new().read(message, false).cloned()
}

/// Reads `message` whole and one byte at a time; both must agree.
fn both(message: &str, validate: bool) -> Result<super::Envelope, Fault> {
    let mut reader = Reader::new();
    reader.start(validate);
    let fed = message
        .as_bytes()
        .iter()
        .try_for_each(|byte| reader.feed(std::slice::from_ref(byte)))
        .and_then(|()| reader.end().cloned());
    let read = Reader::new().read(message.as_bytes(), validate).cloned();
    assert_eq!(fed, read, "{message}");
    read
}

#[test]
fn one_byte_feed_equals_whole_feed() {
    for message in MESSAGES {
        both(message, false).ok();
        both(message, true).ok();
    }
}

#[test]
fn numbers_of_any_length_are_read_in_constant_state() {
    let digits = |len: usize| "1234567890".chars().cycle().take(len).collect::<String>();
    let mut numbers = Vec::new();
    for len in [1, 2, 3, 4, 19, 20, 21, 255, 256, 257, 1000, 4096] {
        let d = digits(len);
        numbers.extend([
            d.clone(),
            format!("-{d}"),
            format!("0.{d}"),
            format!("{d}.{d}"),
            format!("1e{d}"),
            format!("1.5E-{d}"),
            format!("-0.{d}e+{d}"),
        ]);
    }
    for number in &numbers {
        let skipped = format!(r#"{{"type":"input_audio_buffer.commit","x":{number}}}"#);
        assert!(both(&skipped, true).is_ok(), "{number}");
        let checked = format!(
            r#"{{"type":"session.update","session":{{"type":"realtime","audio":{{"input":{{"turn_detection":{{"threshold":{number},"prefix_padding_ms":{number}}}}}}}}}}}"#
        );
        let integer = !number.contains(['.', 'e', 'E']);
        let fault = both(&checked, true).unwrap().subtree[0];
        assert_eq!(fault.is_none(), integer, "{number}");
        let limits = format!(
            r#"{{"type":"rate_limits.updated","rate_limits":[{{"name":"t","remaining":1,"reset_seconds":{number}}}]}}"#
        );
        assert!(both(&limits, false).is_ok(), "{number}");
        let index = format!(r#"{{"type":"x","content_index":{number}}}"#);
        let expected = number
            .parse::<u64>()
            .map(Count::Known)
            .map_err(|_| Fault::Kind("content_index"));
        assert_eq!(
            both(&index, false).map(|e| e.count(N::ContentIndex)),
            expected,
            "{number}"
        );
    }
    let reset = |number: String| {
        let limits = format!(
            r#"{{"type":"rate_limits.updated","rate_limits":[{{"name":"t","remaining":1,"reset_seconds":{number}}}]}}"#
        );
        both(&limits, false).unwrap().limits()[0].reset_ms
    };
    let rows = [
        ("5".to_owned(), Some(5000)),
        ("1.2345".to_owned(), Some(1234)),
        ("0.0009".to_owned(), Some(0)),
        (format!("0.{}", "1".repeat(256)), Some(111)),
        ("1".repeat(19), None),
        ("2.5e3".to_owned(), None),
        ("-1".to_owned(), None),
    ];
    for (number, expected) in rows {
        assert_eq!(reset(number.clone()), expected, "{number}");
    }
}

#[test]
fn the_reader_keeps_control_fields_and_media_sizes() {
    let done = whole(MESSAGES[0].as_bytes()).unwrap();
    assert_eq!(done.text(T::EventId).get(), Some("e1"));
    assert_eq!(done.text(T::ResponseObjectId).get(), Some("resp_1"));
    assert_eq!(done.count(N::Total), Count::Known(3));
    assert_eq!(done.count(N::OutAudio), Count::Known(2));
    assert!(done.entered(At::Usage) && done.null(At::Details));

    let append = whole(MESSAGES[1].as_bytes()).unwrap();
    assert_eq!(append.root_audio, Some(Base64 { chars: 6, padding: 2 }));

    let limits = whole(MESSAGES[2].as_bytes()).unwrap();
    let read: Vec<_> = limits.limits().iter().map(|l| (l.complete(), l.reset_ms)).collect();
    assert_eq!(
        read,
        [(Some(("requests", 9)), Some(1234)), (Some(("tokens", 5)), Some(7000))]
    );

    assert_eq!(whole(MESSAGES[3].as_bytes()).unwrap().media, 4 + 3);
    assert_eq!(whole(MESSAGES[4].as_bytes()).unwrap().count(N::Input), Count::Absent);
    assert_eq!(whole(MESSAGES[5].as_bytes()), Err(Fault::Duplicate("session.model")));
    assert_eq!(whole(MESSAGES[6].as_bytes()), Err(Fault::Json));
    assert_eq!(whole(MESSAGES[7].as_bytes()), Err(Fault::NotObject));
}
