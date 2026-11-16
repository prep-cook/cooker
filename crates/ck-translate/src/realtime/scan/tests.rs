use super::{Fault, Reader};
use crate::realtime::envelope::{Base64, Count, N, T};

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
    Reader::new().read(message).cloned()
}

#[test]
fn one_byte_feed_equals_whole_feed() {
    for message in MESSAGES {
        let mut reader = Reader::new();
        let fed = message
            .as_bytes()
            .iter()
            .try_for_each(|byte| reader.feed(std::slice::from_ref(byte)))
            .and_then(|()| reader.end().cloned());
        assert_eq!(fed, whole(message.as_bytes()), "{message}");
    }
}

#[test]
fn the_reader_keeps_control_fields_and_media_sizes() {
    let done = whole(MESSAGES[0].as_bytes()).unwrap();
    assert_eq!(done.text(T::EventId).get(), Some("e1"));
    assert_eq!(done.text(T::ResponseObjectId).get(), Some("resp_1"));
    assert_eq!(done.count(N::Total), Count::Known(3));
    assert_eq!(done.count(N::OutAudio), Count::Known(2));
    let (start, end) = done.usage.unwrap();
    assert!(MESSAGES[0][start..end].starts_with("{\"total_tokens\"") && MESSAGES[0][..end].ends_with("2}}"));

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
