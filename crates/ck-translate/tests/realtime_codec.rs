//! The OpenAI Realtime projector and checker (`ck_translate::realtime`): client admission,
//! the metadata projection, per-response usage and the final counters.

#[allow(dead_code, reason = "shared mapped codec harness")]
mod semantic;

use ck_translate::{
    codec::{
        Error,
        event::{Direction, Identity, LocalId},
        live::{Opcode, Projector, Sender},
    },
    realtime::{Admission, Counts, Events, Session, Start},
};
use semantic::Mapped;
use serde_json::{Value, json};

fn identity() -> Identity {
    Identity {
        request_id: LocalId::new("live-request").unwrap(),
        stream_id: LocalId::new("live-session").unwrap(),
        attempt: 0,
        direction: Direction::Response,
    }
}

fn open(mapped: &mut Mapped, events: Events) -> Session {
    let start = Start {
        id: "rt_1",
        model: "gpt-realtime",
        created: 7,
    };
    let admission = Admission {
        events,
        model: Some(LocalId::new("gpt-realtime").unwrap()),
    };
    Session::new(&mut mapped.writer, identity(), &start, admission).unwrap()
}

/// Every published record as `{"event": name, "source": x_ck.source, ...payload}`; the rest
/// of the envelope is the emitter's and is checked by its own tests.
fn records(mapped: &Mapped) -> Vec<Value> {
    let output = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
    output
        .split_terminator("\n\n")
        .map(|record| {
            let event = record.lines().find_map(|l| l.strip_prefix("event: ")).unwrap();
            let data = record.lines().find_map(|l| l.strip_prefix("data: ")).unwrap();
            let mut data: Value = serde_json::from_str(data).unwrap();
            let x_ck = data.as_object_mut().unwrap().remove("x_ck").unwrap();
            data["event"] = json!(event);
            if let Some(source) = x_ck.get("source") {
                data["source"] = source.clone();
            }
            data
        })
        .collect()
}

fn text(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).unwrap()
}

/// A refusal's code and param.
type Reason = (&'static str, Option<&'static str>);

#[test]
fn client_messages_pass_unchanged_or_are_refused_with_a_reason() {
    use Events::{Realtime, Sideband};
    let documented_session = json!({"type":"session.update","session":{"type":"realtime","model":"gpt-realtime",
        "output_modalities":["audio"],"max_output_tokens":4096,"tool_choice":"auto","tracing":null,"truncation":"auto",
        "audio":{"input":{"format":{"type":"audio/pcm","rate":24000},"turn_detection":null,"transcription":{"model":"whisper-1"}},
            "output":{"voice":{"id":"v"},"speed":1.0}},
        "tools":[{"type":"mcp","server_label":"s","require_approval":"never"}]}})
    .to_string();
    // A documented number with more fraction digits than any counter holds.
    let long_threshold = format!(
        "{{\"type\":\"session.update\",\"session\":{{\"type\":\"realtime\",\"audio\":{{\"input\":{{\"turn_detection\":{{\"type\":\"server_vad\",\"threshold\":0.{}}}}}}}}}}}",
        "1".repeat(256)
    );
    let long_type = format!("{{\"type\":\"session.update{}\",\"session\":{{}}}}", "x".repeat(60));
    let deep = format!(
        "{{\"type\":\"session.update\",\"x\":{}{}}}",
        "[".repeat(40),
        "]".repeat(40)
    );
    let rows: Vec<(Events, Opcode, Vec<u8>, Option<Reason>)> = vec![
        (Realtime, Opcode::Text, br#"{"type":"input_audio_buffer.append","audio":"AAAA"}"#.to_vec(), None),
        (
            Realtime,
            Opcode::Text,
            " {\n \"audio\" : \"QU\\/D\", \"type\" :\"input_audio_buffer.append\", \"x\":[1.5e3,true,null,\"\u{1F3B5}\"] } \n"
                .as_bytes()
                .to_vec(),
            None,
        ),
        (Realtime, Opcode::Text, br#"{"type":"session.update","session":{"type":"realtime","model":"gpt-realtime","instructions":"hi"}}"#.to_vec(), None),
        (Realtime, Opcode::Text, br#"{"type":"session.update","session":{"type":"realtime","model":null}}"#.to_vec(), Some(("invalid_type", Some("session.model")))),
        (Realtime, Opcode::Text, br#"{"type":"session.update","session":{"type":"realtime","model":"gpt-other"}}"#.to_vec(), Some(("invalid_value", Some("session.model")))),
        (Realtime, Opcode::Text, br#"{"type":"session.update","session":{"type":"realtime","mod\u0065l":"gpt-other"}}"#.to_vec(), Some(("invalid_value", Some("session.model")))),
        (Realtime, Opcode::Text, br#"{"type":"session.update","session":{"model":"gpt-realtime","model":"gpt-other"}}"#.to_vec(), Some(("duplicate_parameter", Some("session.model")))),
        (Realtime, Opcode::Text, br#"{"type":"session.update","session":{"model":"gpt-realtime"},"session":{}}"#.to_vec(), Some(("duplicate_parameter", Some("session")))),
        (Realtime, Opcode::Text, br#"{"type":"input_audio_buffer.commit","type":"session.update"}"#.to_vec(), Some(("duplicate_parameter", Some("type")))),
        (Realtime, Opcode::Text, br#"{"type":"input_audio_buffer.commit","t\u0079pe":"session.update"}"#.to_vec(), Some(("duplicate_parameter", Some("type")))),
        (Realtime, Opcode::Text, br#"{"type":"session.update"}"#.to_vec(), Some(("missing_required_parameter", Some("session")))),
        (Realtime, Opcode::Text, br#"{"type":"session.update","session":"x"}"#.to_vec(), Some(("invalid_type", Some("session")))),
        (Realtime, Opcode::Text, br#"{"type":"input_audio_buffer.append"}"#.to_vec(), Some(("missing_required_parameter", Some("audio")))),
        (Realtime, Opcode::Text, br#"{"type":"input_audio_buffer.append","audio":null}"#.to_vec(), Some(("invalid_type", Some("audio")))),
        (Realtime, Opcode::Text, br#"{"type":"input_audio_buffer.append","audio":7}"#.to_vec(), Some(("invalid_type", Some("audio")))),
        (Realtime, Opcode::Text, br#"{"type":"conversation.item.create","item":[]}"#.to_vec(), Some(("invalid_type", Some("item")))),
        (Realtime, Opcode::Text, br#"{"type":"conversation.item.retrieve","item_id":5}"#.to_vec(), Some(("invalid_type", Some("item_id")))),
        (Realtime, Opcode::Text, br#"{"type":"conversation.item.truncate","item_id":"i","content_index":0}"#.to_vec(), Some(("missing_required_parameter", Some("audio_end_ms")))),
        (Realtime, Opcode::Text, br#"{"type":"conversation.item.truncate","item_id":"i","content_index":-1,"audio_end_ms":0}"#.to_vec(), Some(("invalid_type", Some("content_index")))),
        (Realtime, Opcode::Text, br#"{"type":"conversation.item.truncate","item_id":"i","content_index":0,"audio_end_ms":10}"#.to_vec(), None),
        (Realtime, Opcode::Text, br#"{"type":5}"#.to_vec(), Some(("invalid_type", Some("type")))),
        (Realtime, Opcode::Text, br#"{"event_id":"e"}"#.to_vec(), Some(("missing_required_parameter", Some("type")))),
        (Realtime, Opcode::Text, br#"{"type":"input_audio_buffer.flush"}"#.to_vec(), Some(("unknown_event_type", Some("type")))),
        (Realtime, Opcode::Text, long_type.into_bytes(), Some(("unknown_event_type", Some("type")))),
        (Realtime, Opcode::Text, br#"{"type":"conversation.item.create","item":true}"#.to_vec(), Some(("invalid_type", Some("item")))),
        (Realtime, Opcode::Text, "{\"type\":\"session.update\",\"session\":{\"type\":\"realtime\",\"model\":\"gpt-realtim\u{e9}\"}}".as_bytes().to_vec(), Some(("invalid_value", Some("session.model")))),
        (Realtime, Opcode::Text, br#"{"type":"session.close"}"#.to_vec(), Some(("unknown_event_type", Some("type")))),
        (Sideband, Opcode::Text, br#"{"type":"session.close"}"#.to_vec(), None),
        (Realtime, Opcode::Text, br#"{"type":"response.cancel","response_id":"resp_1"}"#.to_vec(), None),
        (Realtime, Opcode::Text, br#"{"type":"response.cancel","event_id":{}}"#.to_vec(), Some(("invalid_type", Some("event_id")))),
        (Realtime, Opcode::Text, br#"{"type":"input_audio_buffer.commit","event_id":null}"#.to_vec(), Some(("invalid_type", Some("event_id")))),
        (Realtime, Opcode::Text, br#"{"type":"transcription_session.update"}"#.to_vec(), Some(("missing_required_parameter", Some("session")))),
        (Realtime, Opcode::Text, br#"{"type":"session.update","session":{"model":"gpt-realtime"}}"#.to_vec(), Some(("missing_required_parameter", Some("session.type")))),
        (Realtime, Opcode::Text, br#"{"type":"session.update","session":{"type":"chat"}}"#.to_vec(), Some(("invalid_value", Some("session.type")))),
        (Realtime, Opcode::Text, br#"{"type":"session.update","session":{"type":"realtime","tools":false,"tool_choice":7,"audio":false}}"#.to_vec(), Some(("invalid_type", Some("session.tools")))),
        (Realtime, Opcode::Text, br#"{"type":"session.update","session":{"type":"realtime","tools":[{"type":"mcp"}]}}"#.to_vec(), Some(("missing_required_parameter", Some("tools[].server_label")))),
        (Realtime, Opcode::Text, br#"{"type":"session.update","session":{"type":"realtime","truncation":{"type":"retention_ratio"}}}"#.to_vec(), Some(("missing_required_parameter", Some("truncation.retention_ratio")))),
        (Realtime, Opcode::Text, br#"{"type":"session.update","session":{"type":"realtime","audio":{"input":{"turn_detection":{"type":"server_vad","prefix_padding_ms":1.5}}}}}"#.to_vec(), Some(("invalid_type", Some("turn_detection.prefix_padding_ms")))),
        (Realtime, Opcode::Text, documented_session.into_bytes(), None),
        (Realtime, Opcode::Text, long_threshold.into_bytes(), None),
        (Realtime, Opcode::Text, br#"{"type":"conversation.item.create","item":{}}"#.to_vec(), Some(("missing_required_parameter", Some("item.type")))),
        (Realtime, Opcode::Text, br#"{"type":"conversation.item.create","item":{"type":"note"}}"#.to_vec(), Some(("invalid_value", Some("item.type")))),
        (Realtime, Opcode::Text, br#"{"type":"conversation.item.create","item":{"type":"message","role":7,"content":false}}"#.to_vec(), Some(("invalid_type", Some("item.role")))),
        (Realtime, Opcode::Text, br#"{"type":"conversation.item.create","item":{"type":"message","role":"bot","content":[]}}"#.to_vec(), Some(("invalid_value", Some("item.role")))),
        (Realtime, Opcode::Text, br#"{"type":"conversation.item.create","item":{"type":"message","role":"user"}}"#.to_vec(), Some(("missing_required_parameter", Some("item.content")))),
        (Realtime, Opcode::Text, br#"{"type":"conversation.item.create","item":{"type":"message","type":"function_call","role":"user","content":[]}}"#.to_vec(), Some(("duplicate_parameter", Some("item.type")))),
        (Realtime, Opcode::Text, br#"{"type":"conversation.item.create","item":{"type":"function_call_output","call_id":"c"}}"#.to_vec(), Some(("missing_required_parameter", Some("item.output")))),
        (Realtime, Opcode::Text, br#"{"type":"conversation.item.create","item":{"type":"function_call_output","call_id":"c","output":"{}"}}"#.to_vec(), None),
        (Realtime, Opcode::Text, br#"{"type":"response.create","response":{"output_modalities":7,"tools":false}}"#.to_vec(), Some(("invalid_type", Some("response.output_modalities")))),
        (Realtime, Opcode::Text, br#"{"type":"response.create","response":{"tool_choice":{"type":"function"}}}"#.to_vec(), Some(("missing_required_parameter", Some("tool_choice.name")))),
        (Realtime, Opcode::Text, br#"{"type":"response.create","response":{"input":[{"type":"message","role":"user","content":[{"type":"input_text"}]}]}}"#.to_vec(), Some(("missing_required_parameter", Some("response.input[].content[].text")))),
        (Realtime, Opcode::Text, br#"{"type":"response.create","response":{"input":[{"type":"item_reference","id":"i"}],"conversation":"none","max_output_tokens":"inf","metadata":null,"tool_choice":{"type":"function","name":"f"},"tools":[{"type":"function","name":"f","parameters":{"type":"object"}}]}}"#.to_vec(), None),
        (Realtime, Opcode::Text, br#"{"type":"session.update","session":{"type":"realtime","audio":{"output":{"voice":{}}}}}"#.to_vec(), Some(("missing_required_parameter", Some("voice.id")))),
        (Realtime, Opcode::Text, br#"{"type":"response.create","response":{"prompt":{"version":"1"}}}"#.to_vec(), Some(("missing_required_parameter", Some("prompt.id")))),
        (Realtime, Opcode::Text, br#"{"type":"response.create","item":{}}"#.to_vec(), None),
        (Sideband, Opcode::Text, br#"{"type":"response.item.create","item":{}}"#.to_vec(), None),
        (Sideband, Opcode::Text, br#"{"type":"response.item.create"}"#.to_vec(), Some(("missing_required_parameter", Some("item")))),
        (Realtime, Opcode::Binary, b"\x00\x01".to_vec(), Some(("binary_not_allowed", None))),
        (Realtime, Opcode::Text, br#"{"type":"input_audio_buffer.commit""#.to_vec(), Some(("invalid_json", None))),
        (Realtime, Opcode::Text, br#"{"type":"input_audio_buffer.commit"} {}"#.to_vec(), Some(("invalid_json", None))),
        (Realtime, Opcode::Text, br#"{"type":"input_audio_buffer.commit",}"#.to_vec(), Some(("invalid_json", None))),
        (Realtime, Opcode::Text, br#"{"type":"input_audio_buffer.commit","n":01}"#.to_vec(), Some(("invalid_json", None))),
        (Realtime, Opcode::Text, br#"{"type":"input_audio_buffer.commit","s":"\ud800"}"#.to_vec(), Some(("invalid_json", None))),
        (Realtime, Opcode::Text, b"{\"type\":\"input_audio_buffer.commit\",\"s\":\"\xff\"}".to_vec(), Some(("invalid_json", None))),
        (Realtime, Opcode::Text, br#"[{"type":"input_audio_buffer.commit"}]"#.to_vec(), Some(("invalid_event", None))),
        (Realtime, Opcode::Text, deep.into_bytes(), Some(("invalid_event", None))),
    ];
    for (events, opcode, payload, expected) in rows {
        let mut mapped = Mapped::new();
        let mut session = open(&mut mapped, events);
        let sent = payload.clone();
        let got = session.admit(opcode, &payload).map_err(|refusal| {
            assert!(!refusal.message.is_empty());
            (refusal.code, refusal.param)
        });
        assert_eq!(got, expected.map_or(Ok(()), Err), "{}", String::from_utf8_lossy(&sent));
        assert_eq!(payload, sent, "the checker never edits the message");
        let refused = u64::from(expected.is_some());
        assert_eq!(session.counts(Sender::Client).refused, refused);
    }
}

#[test]
fn projection_carries_metadata_never_media_or_text() {
    let secret_audio = "U0VDUkVUQVVESU8="; // 11 decoded bytes
    let script: Vec<(Sender, Opcode, Vec<u8>)> = vec![
        (
            Sender::Client,
            Opcode::Text,
            text(&json!({"type":"session.update","event_id":"c1",
            "session":{"type":"realtime","model":"gpt-realtime","instructions":"SECRET-INSTRUCTIONS","tools":[]}})),
        ),
        (
            Sender::Upstream,
            Opcode::Text,
            text(&json!({"type":"session.created","event_id":"e1",
            "session":{"id":"sess_1","model":"gpt-realtime","instructions":"SECRET-INSTRUCTIONS"}})),
        ),
        (
            Sender::Client,
            Opcode::Text,
            text(&json!({"type":"input_audio_buffer.append","audio":secret_audio})),
        ),
        (
            Sender::Client,
            Opcode::Text,
            text(&json!({"type":"input_audio_buffer.commit","event_id":"c2"})),
        ),
        (
            Sender::Client,
            Opcode::Text,
            text(&json!({"type":"conversation.item.create","event_id":"c3",
            "item":{"id":"item_1","type":"message","role":"user","content":[
                {"type":"input_text","text":"SECRET-TEXT"},
                {"type":"input_audio","audio":secret_audio},
                {"type":"input_image","image_url":"data:image/png;base64,U0VDUkVU"}]}})),
        ),
        (
            Sender::Client,
            Opcode::Text,
            text(&json!({"type":"response.create","event_id":"c4",
            "response":{"instructions":"SECRET-INSTRUCTIONS"}})),
        ),
        (
            Sender::Upstream,
            Opcode::Text,
            text(&json!({"type":"rate_limits.updated","event_id":"e2","rate_limits":[
            {"name":"requests","limit":1000,"remaining":999,"reset_seconds":0.06},
            {"name":"tokens","limit":50000,"remaining":49000,"reset_seconds":1.5},{"limit":5}]})),
        ),
        (
            Sender::Upstream,
            Opcode::Text,
            text(&json!({"type":"response.created","event_id":"e3",
            "response":{"id":"resp_1","status":"in_progress","output":[]}})),
        ),
        (
            Sender::Upstream,
            Opcode::Text,
            text(&json!({"type":"response.output_audio.delta","event_id":"e4",
            "response_id":"resp_1","item_id":"item_2","output_index":0,"content_index":0,"delta":secret_audio})),
        ),
        (
            Sender::Upstream,
            Opcode::Text,
            text(&json!({"type":"response.output_audio_transcript.delta","event_id":"e5",
            "response_id":"resp_1","item_id":"item_2","output_index":0,"content_index":0,"delta":"SECRET-TRANSCRIPT"})),
        ),
        (
            Sender::Upstream,
            Opcode::Text,
            text(&json!({"type":"response.function_call_arguments.done","event_id":"e6",
            "response_id":"resp_1","item_id":"item_3","output_index":1,"call_id":"call_1","arguments":"{\"SECRET\":1}"})),
        ),
        (
            Sender::Upstream,
            Opcode::Text,
            text(&json!({"type":"error","event_id":"e7","error":{
            "type":"invalid_request_error","code":"invalid_value","message":"SECRET-MESSAGE","param":"x","event_id":"c4"}})),
        ),
        (
            Sender::Upstream,
            Opcode::Text,
            text(&json!({"type":"response.done","event_id":"e8","response":{
            "id":"resp_1","status":"incomplete","status_details":{"type":"incomplete","reason":"max_output_tokens"},
            "output":[{"type":"message","content":[{"type":"output_audio","transcript":"SECRET-TRANSCRIPT"}]}],
            "usage":{"total_tokens":300,"input_tokens":120,"output_tokens":180,
                "input_token_details":{"text_tokens":20,"audio_tokens":100,"image_tokens":0,"cached_tokens":64,
                    "cached_tokens_details":{"text_tokens":0,"audio_tokens":64,"image_tokens":0}},
                "output_token_details":{"text_tokens":30,"audio_tokens":150}}}})),
        ),
        (Sender::Upstream, Opcode::Binary, b"\x00\x01\x02".to_vec()),
        (Sender::Upstream, Opcode::Text, b"SECRET not json".to_vec()),
        // A repeated member makes the message unreadable: no usage from either value.
        (
            Sender::Upstream,
            Opcode::Text,
            br#"{"type":"response.done","response":{"id":"resp_x","usage":{"input_tokens":1,"input_tokens":9}}}"#
                .to_vec(),
        ),
        (
            Sender::Upstream,
            Opcode::Text,
            text(&json!({"type":"future.event","event_id":"e9","item_id":"item_9","note":"SECRET"})),
        ),
        (
            Sender::Upstream,
            Opcode::Text,
            text(&json!({"type":"response.created","event_id":"e10","response":{"id":"resp_2"}})),
        ),
        (
            Sender::Upstream,
            Opcode::Text,
            text(&json!({"type":"response.output_audio.delta","event_id":"e11",
            "response_id":"resp_2","delta":"U0VD"})),
        ),
    ];
    let mut mapped = Mapped::new();
    let mut session = open(&mut mapped, Events::Realtime);
    let (mut client, mut upstream) = (Counts::default(), Counts::default());
    for (sender, opcode, payload) in &script {
        if *sender == Sender::Client {
            session.admit(*opcode, payload).unwrap();
        }
        session.project(&mut mapped.writer, *sender, *opcode, payload).unwrap();
        let counts = if *sender == Sender::Client {
            &mut client
        } else {
            &mut upstream
        };
        counts.messages += 1;
        counts.bytes += payload.len() as u64;
    }
    session.finish(&mut mapped.writer).unwrap();
    let (append, deltas) = (&script[2].2, [&script[8].2, &script[18].2]);
    client.media_messages = 1;
    client.media_bytes = append.len() as u64;
    client.decoded_bytes = 11 + 11 + 6;
    upstream.media_messages = 3;
    upstream.media_bytes = (deltas[0].len() + deltas[1].len() + 3) as u64;
    upstream.decoded_bytes = 11 + 3 + 3;
    upstream.unreadable = 2;
    assert_eq!(session.counts(Sender::Client), client);
    assert_eq!(session.counts(Sender::Upstream), upstream);

    let output = mapped.output(&mapped.writer);
    for needle in [&b"SECRET"[..], b"U0VD"] {
        assert!(
            !output.windows(needle.len()).any(|w| w == needle),
            "{}",
            String::from_utf8_lossy(&output)
        );
    }
    let source = |event: &str, sender: &str, ids: Value| json!({"protocol":"openai_realtime","event":event,"sender":sender,"ids":ids});
    let counts = |c: Counts| {
        json!({"messages":c.messages,"bytes":c.bytes,"media_messages":c.media_messages,
            "media_bytes":c.media_bytes,"decoded_bytes":c.decoded_bytes,"refused":c.refused,"unreadable":c.unreadable})
    };
    let mut transcript_source = source(
        "response.output_audio_transcript.delta",
        "upstream",
        json!({"event_id":"e5","item_id":"item_2","response_id":"resp_1"}),
    );
    transcript_source["output_index"] = json!(0);
    transcript_source["content_index"] = json!(0);
    let mut arguments_source = source(
        "response.function_call_arguments.done",
        "upstream",
        json!({"event_id":"e6","item_id":"item_3","response_id":"resp_1","call_id":"call_1"}),
    );
    arguments_source["output_index"] = json!(1);
    let expected = vec![
        json!({"event":"x_ck.response.start","id":"rt_1","model":"gpt-realtime","created":7}),
        json!({"event":"x_ck.operation","kind":"realtime.control",
            "value":{"action":"session.configure","config":{"model":"gpt-realtime"}},
            "source":source("session.update","client",json!({"event_id":"c1"}))}),
        json!({"event":"x_ck.operation","kind":"realtime.control",
            "value":{"action":"session.created","config":{"model":"gpt-realtime"}},
            "source":source("session.created","upstream",json!({"event_id":"e1","session_id":"sess_1"}))}),
        json!({"event":"x_ck.operation","kind":"realtime.control",
            "value":{"action":"audio.commit","track_id":"input"},
            "source":source("input_audio_buffer.commit","client",json!({"event_id":"c2"}))}),
        json!({"event":"x_ck.opaque","name":"conversation.item.create","format":"json","value":{},
            "source":source("conversation.item.create","client",json!({"event_id":"c3","item_id":"item_1"}))}),
        json!({"event":"x_ck.operation","kind":"realtime.control",
            "value":{"action":"response.create","request_id":"5"},
            "source":source("response.create","client",json!({"event_id":"c4"}))}),
        json!({"event":"x_ck.operation","kind":"realtime.rates","value":{"limits":[
                {"name":"requests","remaining":999,"limit":1000,"reset_after_ms":60},
                {"name":"tokens","remaining":49000,"limit":50000,"reset_after_ms":1500}]},
            "source":source("rate_limits.updated","upstream",json!({"event_id":"e2"}))}),
        json!({"event":"x_ck.opaque","name":"response.created","format":"json","value":{"status":"in_progress"},
            "source":source("response.created","upstream",json!({"event_id":"e3","response_id":"resp_1"}))}),
        json!({"event":"x_ck.opaque","name":"response.output_audio_transcript.delta","format":"json","value":{},
            "source":transcript_source}),
        json!({"event":"x_ck.opaque","name":"response.function_call_arguments.done","format":"json","value":{},
            "source":arguments_source}),
        json!({"event":"x_ck.opaque","name":"error","format":"json",
            "value":{"error":{"type":"invalid_request_error","code":"invalid_value","event_id":"c4"}},
            "source":source("error","upstream",json!({"event_id":"e7"}))}),
        json!({"event":"x_ck.usage","usage":{
                "native":{"protocol":"openai_realtime","value":{"total_tokens":300,"input_tokens":120,"output_tokens":180,
                    "input_token_details":{"text_tokens":20,"audio_tokens":100,"image_tokens":0,"cached_tokens":64,
                        "cached_tokens_details":{"text_tokens":0,"audio_tokens":64,"image_tokens":0}},
                    "output_token_details":{"text_tokens":30,"audio_tokens":150}}},
                "final":true,"input_tokens":120,"output_tokens":180,"total_tokens":300,"cache_read_tokens":64,
                "input_audio_tokens":100,"output_audio_tokens":150,
                "details":{"input_text_tokens":20,"output_text_tokens":30,"input_image_tokens":0}},
            "source":source("response.done","upstream",json!({"event_id":"e8","response_id":"resp_1"}))}),
        json!({"event":"x_ck.opaque","name":"response.done","format":"json",
            "value":{"status":"incomplete","reason":"max_output_tokens","output_audio_bytes":11},
            "source":source("response.done","upstream",json!({"event_id":"e8","response_id":"resp_1"}))}),
        json!({"event":"x_ck.opaque","name":"future.event","format":"json","value":{},
            "source":source("future.event","upstream",json!({"event_id":"e9","item_id":"item_9"}))}),
        json!({"event":"x_ck.opaque","name":"response.created","format":"json","value":{},
            "source":source("response.created","upstream",json!({"event_id":"e10","response_id":"resp_2"}))}),
        json!({"event":"x_ck.operation","kind":"realtime.counters","value":{
            "client":counts(client),"upstream":counts(upstream),
            "responses":[{"response_id":"resp_2","output_audio_bytes":3}]}}),
    ];
    assert_eq!(records(&mapped), expected);
}

const SERVER_EVENTS: [&str; 59] = [
    "error",
    "session.created",
    "session.updated",
    "transcription_session.created",
    "transcription_session.updated",
    "conversation.created",
    "conversation.item.created",
    "conversation.item.added",
    "conversation.item.done",
    "conversation.item.retrieved",
    "conversation.item.input_audio_transcription.completed",
    "conversation.item.input_audio_transcription.delta",
    "conversation.item.input_audio_transcription.segment",
    "conversation.item.input_audio_transcription.failed",
    "conversation.item.truncated",
    "conversation.item.deleted",
    "input_audio_buffer.committed",
    "input_audio_buffer.cleared",
    "input_audio_buffer.speech_started",
    "input_audio_buffer.speech_stopped",
    "input_audio_buffer.timeout_triggered",
    "input_audio_buffer.dtmf_event_received",
    "output_audio_buffer.started",
    "output_audio_buffer.stopped",
    "output_audio_buffer.cleared",
    "response.created",
    "response.done",
    "response.output_item.added",
    "response.output_item.done",
    "response.content_part.added",
    "response.content_part.done",
    "response.output_text.delta",
    "response.output_text.done",
    "response.output_audio_transcript.delta",
    "response.output_audio_transcript.done",
    "response.output_audio.delta",
    "response.output_audio.done",
    "response.function_call_arguments.delta",
    "response.function_call_arguments.done",
    "response.mcp_call_arguments.delta",
    "response.mcp_call_arguments.done",
    "response.mcp_call.in_progress",
    "response.mcp_call.completed",
    "response.mcp_call.failed",
    "mcp_list_tools.in_progress",
    "mcp_list_tools.completed",
    "mcp_list_tools.failed",
    "rate_limits.updated",
    "response.text.delta",
    "response.text.done",
    "response.audio_transcript.delta",
    "response.audio_transcript.done",
    "response.audio.delta",
    "session.input_audio.append",
    "session.output_audio.delta",
    "session.input_transcript.delta",
    "session.output_transcript.delta",
    "session.instructions.appended",
    "session.closed",
];

/// Audio-carrying event types: counted, never projected.
const MEDIA_EVENTS: [&str; 5] = [
    "input_audio_buffer.append",
    "response.output_audio.delta",
    "response.audio.delta",
    "session.input_audio.append",
    "session.output_audio.delta",
];

#[test]
fn every_documented_event_type_is_projected_or_counted() {
    let client = [
        json!({"type":"session.update","session":{"type":"realtime"}}),
        json!({"type":"transcription_session.update","session":{}}),
        json!({"type":"input_audio_buffer.append","audio":"QUJD"}),
        json!({"type":"input_audio_buffer.commit"}),
        json!({"type":"input_audio_buffer.clear"}),
        json!({"type":"output_audio_buffer.clear"}),
        json!({"type":"conversation.item.create","item":{"type":"message","role":"user","content":[]}}),
        json!({"type":"conversation.item.retrieve","item_id":"i"}),
        json!({"type":"conversation.item.truncate","item_id":"i","content_index":0,"audio_end_ms":0}),
        json!({"type":"conversation.item.delete","item_id":"i"}),
        json!({"type":"response.create"}),
        json!({"type":"response.cancel"}),
        json!({"type":"session.instructions.append","instructions":"x"}),
        json!({"type":"session.thinking.append","text":"x"}),
        json!({"type":"session.commentary.append","text":"x"}),
        json!({"type":"session.input_audio.mute"}),
        json!({"type":"session.input_audio.unmute"}),
        json!({"type":"session.close"}),
        json!({"type":"response.item.create","item":{}}),
    ];
    let server = SERVER_EVENTS.map(|event| json!({"type":event,"event_id":"e","delta":"QUJD","audio":"QUJD"}));
    let messages = client
        .iter()
        .map(|m| (Sender::Client, m))
        .chain(server.iter().map(|m| (Sender::Upstream, m)));
    for (sender, message) in messages {
        let event = message["type"].as_str().unwrap();
        let payload = text(message);
        let mut mapped = Mapped::new();
        let mut session = open(&mut mapped, Events::Sideband);
        if sender == Sender::Client {
            session
                .admit(Opcode::Text, &payload)
                .unwrap_or_else(|r| panic!("{event}: {r:?}"));
        }
        session
            .project(&mut mapped.writer, sender, Opcode::Text, &payload)
            .unwrap();
        let records = records(&mapped);
        let media = MEDIA_EVENTS.contains(&event);
        assert_eq!(session.counts(sender).media_messages, u64::from(media), "{event}");
        assert_eq!(session.counts(sender).unreadable, 0, "{event}");
        let projected: Vec<_> = records.iter().filter(|r| r["source"]["event"] == event).collect();
        assert_eq!(projected.len(), usize::from(!media), "{event}: {records:?}");
    }
}

#[test]
fn usage_is_read_once_from_each_response_done() {
    let undocumented = json!({"input_tokens":1,"output_tokens":2,"total_tokens":3,
        "instructions":"SECRET-INSTRUCTIONS","arguments":"SECRET-ARGUMENTS","audio":"U0VDUkVU",
        "input_token_details":{"note":"SECRET-NOTE","cached_tokens_details":{"audio_tokens":1,"text":"SECRET"}},
        "output_token_details":null});
    // (usage sent, Some((native value kept, canonical snapshot)) or None for no record)
    let rows = [
        (
            json!({"input_tokens":10,"output_tokens":5,"output_token_details":{"audio_tokens":4}}),
            Some((
                json!({"input_tokens":10,"output_tokens":5,"output_token_details":{"audio_tokens":4}}),
                json!({"final":true,"input_tokens":10,"output_tokens":5,"total_tokens":15,
                    "output_audio_tokens":4,"derived":["total_tokens"]}),
            )),
        ),
        (
            json!({"input_tokens":null,"output_tokens":5,"total_tokens":9}),
            Some((
                json!({"input_tokens":null,"output_tokens":5,"total_tokens":9}),
                json!({"final":true,"output_tokens":5,"total_tokens":9}),
            )),
        ),
        (
            undocumented,
            Some((
                json!({"input_tokens":1,"output_tokens":2,"total_tokens":3,
                    "input_token_details":{"cached_tokens_details":{"audio_tokens":1}},"output_token_details":null}),
                json!({"final":true,"input_tokens":1,"output_tokens":2,"total_tokens":3}),
            )),
        ),
        (json!({}), Some((json!({}), json!({"final":true})))),
        (json!(null), None),
    ];
    for (usage, expected) in rows {
        let mut mapped = Mapped::new();
        let mut session = open(&mut mapped, Events::Realtime);
        // `response.created` may carry the same object; only `response.done` is read.
        let messages = [
            json!({"type":"response.created","response":{"id":"resp_1","status":"in_progress","usage":usage}}),
            json!({"type":"response.done","response":{"id":"resp_1","status":"completed","usage":usage}}),
            json!({"type":"response.done","response":{"id":"resp_2","status":"cancelled"}}),
        ];
        for message in &messages {
            let payload = serde_json::to_vec_pretty(message).unwrap();
            session
                .project(&mut mapped.writer, Sender::Upstream, Opcode::Text, &payload)
                .unwrap();
        }
        let output = mapped.output(&mapped.writer);
        assert!(!output.windows(6).any(|w| w == b"SECRET"), "{usage}");
        let usages: Vec<(Value, Value)> = records(&mapped)
            .into_iter()
            .filter(|r| r["event"] == "x_ck.usage")
            .map(|mut r| {
                let native = r["usage"].as_object_mut().unwrap().remove("native").unwrap();
                assert_eq!(native["protocol"], "openai_realtime");
                (native["value"].clone(), r["usage"].take())
            })
            .collect();
        assert_eq!(usages, expected.into_iter().collect::<Vec<_>>(), "{usage}");
    }
}

#[test]
fn media_counts_use_the_base64_length_without_decoding() {
    let rows = [
        (json!({"type":"input_audio_buffer.append","audio":"QQ=="}), 1),
        (json!({"type":"input_audio_buffer.append","audio":"QUI="}), 2),
        (json!({"type":"input_audio_buffer.append","audio":"QUJD"}), 3),
        (json!({"type":"input_audio_buffer.append","audio":"QUJDRA"}), 4),
        (json!({"type":"input_audio_buffer.append","audio":""}), 0),
        (json!({"type":"input_audio_buffer.append","audio":"QU/D"}), 3),
        (
            json!({"type":"conversation.item.create","item":{"type":"message","role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,QUJD"}]}}),
            3,
        ),
        (
            json!({"type":"conversation.item.create","item":{"type":"message","role":"user","content":[{"type":"input_image","image_url":"DATA:image/png;base64,QQ=="}]}}),
            1,
        ),
        (
            json!({"type":"conversation.item.create","item":{"type":"message","role":"user","content":[{"type":"input_image","image_url":"https://example.com/a.png"}]}}),
            0,
        ),
        (
            json!({"type":"response.create","response":{"input":[{"type":"message","role":"user","content":[{"type":"input_audio","audio":"QUJDRA=="}]}]}}),
            4,
        ),
    ];
    for (message, decoded) in rows {
        let mut payload = text(&message);
        // An escaped solidus is one base64 character.
        if let Some(at) = payload.windows(4).position(|w| w == b"QU/D") {
            payload.splice(at + 2..at + 3, *b"\\/");
        }
        let mut mapped = Mapped::new();
        let mut session = open(&mut mapped, Events::Realtime);
        session.admit(Opcode::Text, &payload).unwrap();
        session
            .project(&mut mapped.writer, Sender::Client, Opcode::Text, &payload)
            .unwrap();
        assert_eq!(session.counts(Sender::Client).decoded_bytes, decoded, "{message}");
    }
}

#[test]
fn a_session_publishes_its_counters_once_and_only_to_its_own_stream() {
    let mut mapped = Mapped::new();
    let mut session = open(&mut mapped, Events::Realtime);
    let commit = br#"{"type":"input_audio_buffer.commit"}"#;
    let mut other = mapped.store.create().unwrap();
    assert!(matches!(
        session.project(&mut other, Sender::Client, Opcode::Text, commit),
        Err(Error::Position)
    ));
    session.finish(&mut mapped.writer).unwrap();
    assert!(matches!(session.finish(&mut mapped.writer), Err(Error::Closed)));
    assert!(matches!(
        session.project(&mut mapped.writer, Sender::Client, Opcode::Text, commit),
        Err(Error::Closed)
    ));
    let records = records(&mapped);
    assert_eq!(records.len(), 2);
    assert_eq!(records[1]["kind"], "realtime.counters");

    // Output audio is tallied for at most 16 open responses; the next one fails visibly.
    let mut mapped = Mapped::new();
    let mut session = open(&mut mapped, Events::Realtime);
    for n in 0..=16 {
        let created = text(&json!({"type":"response.created","response":{"id":format!("resp_{n}")}}));
        let projected = session.project(&mut mapped.writer, Sender::Upstream, Opcode::Text, &created);
        assert_eq!(projected.is_err_and(|e| matches!(e, Error::Limit)), n == 16, "{n}");
    }
}
