use super::*;

/// A store whose streams hold at most `max_frames` frames, and its directory.
fn store(max_frames: u32) -> (ck_stream::Store, std::path::PathBuf) {
    let directory = std::env::temp_dir().join(format!("ck-exec-events-{}", uuid::Uuid::new_v4()));
    let mut config = ck_stream::Config::new(directory.clone());
    config.pool_bytes = Some(32 * ck_stream::FRAME_BYTES);
    config.max_event_bytes = 4 * ck_stream::FRAME_BYTES;
    config.max_frames = Some(max_frames);
    config.header_slots = 4;
    config.workers = 1;
    (ck_stream::Store::open(config).expect("open the test store"), directory)
}

fn done(index: usize, item: &str) -> String {
    format!("data: {{\"type\":\"response.output_item.done\",\"output_index\":{index},\"item\":{item}}}\n\n")
}

fn drain(events: &mut Events) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(piece) = events.take() {
        out.extend_from_slice(&piece.expect("queued output"));
    }
    out
}

/// Announced items live in the response stream's scratch, so an item past its quota fails
/// the attempt instead of growing the heap.
#[test]
fn announced_items_count_against_the_stream_quota() {
    let (store, directory) = store(4);
    let writer = store.create().unwrap();
    let mut events = Events::new(writer.allocator(), false, false, None);
    let text = "x".repeat(5 * ck_stream::FRAME_BYTES);
    let item = format!(r#"{{"type":"message","id":"m","content":[{{"type":"output_text","text":"{text}"}}]}}"#);
    let error = events.push(done(0, &item).as_bytes()).unwrap_err();
    assert_eq!((error.status, error.scope), (503, FailureScope::Request));
    drop((events, writer, store));
    std::fs::remove_dir_all(&directory).ok();
}

/// An empty completed output is passed on with the announced items, in index order, read
/// back from scratch across frame boundaries.
#[test]
fn an_empty_completed_output_gets_the_announced_items() {
    let (store, directory) = store(16);
    let writer = store.create().unwrap();
    let mut events = Events::new(writer.allocator(), false, false, None);
    let long = "y".repeat(ck_stream::FRAME_BYTES + 7);
    let first = r#"{"type":"message","id":"a","content":[]}"#;
    let second = format!(r#"{{"type":"message","id":"b","content":[{{"type":"output_text","text":"{long}"}}]}}"#);
    let mut body = done(1, &second) + &done(0, first);
    body += "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"output\":[]}}\n\n";
    events.push(body.as_bytes()).unwrap();
    assert!(events.completed());
    let out = String::from_utf8(drain(&mut events)).unwrap();
    let completed = out.lines().rfind(|line| line.starts_with("data: ")).unwrap();
    let output = gjson::get(&completed["data: ".len()..], "response.output");
    assert_eq!(output.json(), format!("[{first},{second}]"));
    drop((events, writer, store));
    std::fs::remove_dir_all(&directory).ok();
}

/// A listed output keeps its items; one without an id takes the id announced at its index.
#[test]
fn a_listed_output_gets_missing_ids() {
    let (store, directory) = store(16);
    let writer = store.create().unwrap();
    let mut events = Events::new(writer.allocator(), false, false, None);
    let mut body = done(0, r#"{"type":"message","id":"msg_1","content":[]}"#);
    body +=
        "data: {\"type\":\"response.completed\",\"response\":{\"output\":[{\"type\":\"message\",\"content\":[]}]}}\n\n";
    events.push(body.as_bytes()).unwrap();
    let out = String::from_utf8(drain(&mut events)).unwrap();
    let completed = out.lines().rfind(|line| line.starts_with("data: ")).unwrap();
    assert_eq!(
        gjson::get(&completed["data: ".len()..], "response.output.0.id").str(),
        "msg_1"
    );
    drop((events, writer, store));
    std::fs::remove_dir_all(&directory).ok();
}

/// A renamed compaction that is not UTF-8 fails instead of being repaired.
#[tokio::test]
async fn a_renamed_compaction_must_be_utf8() {
    let chunk: Result<Bytes, ExecError> = Ok(Bytes::from_static(b"{\"output\":\"\xff\"}"));
    let mut body = compaction(futures_util::stream::iter([chunk]).boxed(), true);
    let error = body.next().await.unwrap().unwrap_err();
    assert_eq!(error.status, 502);
}
