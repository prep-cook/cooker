use std::sync::{Arc, Barrier};

use ck_translate::codec::source::Source;

use super::*;
use crate::test_alloc;

/// Grok's encrypted reasoning, as the Go recording has it.
const ENC: &str = "TKwE++a7qP4CXIXITJIj2DYyQCSyR3JuUSrOe758M0CwwzrC2cbGVuOaEQaBqIrN6dvHWTX7FJ0iqroON2AsUyUnrtPc/TZpMRjPoWUeMN4m8PpoZ7WEOcq9ouGU9GXp1dV2okgnwdVvFfliii45xpY2Vr+czC4aqEgQFOmfOkP/1/2QzRooWeLd0i2AVLOOckBWoRQ1jCCa1racBhwpsfYf7Ax5lb393La7xuA7EJm6/n9nslYW3GJXW4GKnsKysoL7Ml0tLwoN1m+lBumus/5uivtkjccC5s5Jrf6Q41NADO7Q4yeiDfkUs7Ro4NVGD/ehf4+8qBtI28X3Gx54Fw";

/// A store whose streams hold the test documents, and its directory.
fn pool() -> (ck_stream::Store, std::path::PathBuf) {
    let directory = std::env::temp_dir().join(format!("ck-exec-replay-items-{}", uuid::Uuid::new_v4()));
    let mut config = ck_stream::Config::new(directory.clone());
    config.pool_bytes = Some(512 * ck_stream::FRAME_BYTES);
    config.header_slots = 8;
    config.workers = 1;
    (ck_stream::Store::open(config).expect("open the test store"), directory)
}

fn parse(owner: &mut ck_stream::Writer, json: &str) -> Document {
    let mut source = Source::new(owner).unwrap();
    source.push_bytes(owner, json.as_bytes()).unwrap();
    let len = source.len();
    Document::parse(owner, source, len).unwrap()
}

/// What this module makes of the single item .
fn normalized(owner: &mut ck_stream::Writer, json: &str) -> Option<serde_json::Value> {
    let doc = parse(owner, json);
    let (store, item) = (Store::default(), doc.root());
    let mut held = store.reserve();
    let kind = kind(&doc, item).unwrap()?;
    let Check::Kept(ids) = valid(&doc, item, kind, &mut held).unwrap() else {
        return None;
    };
    let bytes = encode::owned(|e| write(&doc, item, kind, ids.as_ref(), e), |_| true)
        .unwrap()
        .unwrap();
    Some(serde_json::from_slice(&bytes).unwrap())
}

/// Items normalize as the old route normalizes their raw bytes, up to JSON equality.
#[test]
fn items_normalize_as_raw_items_do() {
    let (pool, directory) = pool();
    let mut owner = pool.create().unwrap();
    let text = r#""a \"quote\" <\n é""#;
    let items = [
        format!(r#"{{"type":"reasoning","id":"rs","summary":[],"encrypted_content":"{ENC}","status":"completed"}}"#),
        format!(r#"{{"type":"reasoning","encrypted_content":" {ENC}"}}"#),
        r#"{"type":"reasoning","encrypted_content":"short"}"#.to_owned(),
        format!(
            r#"{{"type":"message","id":"m","role":" Assistant ","content":[{{"type":"output_text","text":{text},"annotations":[]}},{{"type":"image"}},{{"type":" refusal","refusal":"no"}},{{"type":"output_text","text":5}}]}}"#
        ),
        r#"{"type":"message","role":"user","content":[{"type":"output_text","text":"x"}]}"#.to_owned(),
        r#"{"type":"message","role":"assistant","content":[{"type":"image"}]}"#.to_owned(),
        r#"{"type":"message","role":"assistant","content":[]}"#.to_owned(),
        r#"{"type":"function_call","call_id":" call_1 ","name":"tool","arguments":"{\"a\":1}","id":"fc"}"#.to_owned(),
        r#"{"type":"function_call","call_id":"call_1","name":"  ","arguments":"{}"}"#.to_owned(),
        r#"{"type":"function_call","call_id":"call_1","name":"tool"}"#.to_owned(),
        r#"{"type":"custom_tool_call","call_id":"c","name":"patch","input":{"a":[1, 2]},"status":" in_progress "}"#
            .to_owned(),
        r#"{"type":"custom_tool_call","call_id":"c","name":"patch","input":"text","status":""}"#.to_owned(),
        r#"{"type":"custom_tool_call","call_id":"c","name":"patch"}"#.to_owned(),
        r#"{"type":5,"call_id":"c"}"#.to_owned(),
    ];
    let structured = [r#"[1, 2]"#, r#"{ "a": [1, { "b": "\u00e9\"]" }] }"#, "[ ]"];
    let mut raw = Vec::new();
    for value in structured {
        raw.push(format!(
            r#"{{"type":"function_call","call_id":{value},"name":"t","arguments":"{{}}"}}"#
        ));
        raw.push(format!(
            r#"{{"type":"function_call","call_id":"c","name":{value},"arguments":"{{}}"}}"#
        ));
        raw.push(format!(
            r#"{{"type":"custom_tool_call","call_id":"c","name":"t","input":"x","status":{value}}}"#
        ));
    }
    for item in items.into_iter().chain(scalar_items()).chain(raw) {
        let raw = crate::xai_replay::normalize_item(item.as_bytes());
        let want = raw.map(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).unwrap());
        assert_eq!(normalized(&mut owner, &item), want, "{item}");
    }
    drop(owner);
    drop(pool);
    let _ = std::fs::remove_dir_all(directory);
}

/// Calls whose `call_id`, `name` and `status` are each, in turn, every kind of JSON value
/// but a string: the old route reads them as Go's `Result.String()` does.
fn scalar_items() -> Vec<String> {
    let values = [
        "77",
        "-3",
        "0",
        "1.5",
        "1e3",
        "1.0",
        "-0.25",
        "true",
        "false",
        "null",
        r#"{"a":1}"#,
        r#"[1,"x"]"#,
    ];
    let mut items = Vec::new();
    for value in values {
        for field in ["call_id", "name"] {
            let mut call = serde_json::json!({"type":"function_call","call_id":"c","name":"t","arguments":"{}"});
            call[field] = serde_json::from_str(value).unwrap();
            items.push(call.to_string());
            let mut custom = serde_json::json!({"type":"custom_tool_call","call_id":"c","name":"t","input":"x"});
            custom[field] = serde_json::from_str(value).unwrap();
            items.push(custom.to_string());
        }
        let mut custom = serde_json::json!({"type":"custom_tool_call","call_id":"c","name":"t","input":"x"});
        custom["status"] = serde_json::from_str(value).unwrap();
        items.push(custom.to_string());
    }
    items
}

/// The replay items of the completion `json` under a store of `limits`.
fn items(owner: &mut ck_stream::Writer, json: &str, limits: (usize, usize)) -> Option<Vec<serde_json::Value>> {
    let doc = parse(owner, json);
    let store = Store::with_limits(limits.0, limits.1);
    let mut held = store.reserve();
    let items = replay_items(&doc, doc.root(), &mut held).unwrap()?;
    Some(items.iter().map(|i| serde_json::from_slice(i).unwrap()).collect())
}

/// Checking a reasoning item takes its charge while it runs and returns it after: the turn
/// fails to fit only while the check would not.
#[test]
fn checking_reasoning_is_charged_while_it_runs() {
    let (pool, directory) = pool();
    let mut owner = pool.create().unwrap();
    let turn = format!(r#"{{"output":[{{"type":"reasoning","encrypted_content":"{ENC}"}}]}}"#);
    let item = 2 * ENC.len() - 10;
    assert!(
        items(&mut owner, &turn, (item, item)).is_none(),
        "the check needs twice the content"
    );
    assert_eq!(
        items(&mut owner, &turn, (2 * ENC.len(), 2 * ENC.len())).map(|i| i.len()),
        Some(1)
    );
    drop(owner);
    drop(pool);
    let _ = std::fs::remove_dir_all(directory);
}

/// Reading a number takes a charge of its length and room to format it: a long one that
/// formats short fits an entry only while its text does.
#[test]
fn reading_a_number_is_charged() {
    let (pool, directory) = pool();
    let mut owner = pool.create().unwrap();
    let id = format!("1.{}", "0".repeat(5_000));
    let turn = format!(r#"{{"output":[{{"type":"function_call","call_id":{id},"name":"t","arguments":"{{}}"}}]}}"#);
    let call = serde_json::json!({"type":"function_call","call_id":"1","name":"t","arguments":"{}"});
    assert!(items(&mut owner, &turn, (1_000, 1_000)).is_none());
    assert_eq!(items(&mut owner, &turn, (8_000, 8_000)), Some(vec![call]));
    drop(owner);
    drop(pool);
    let _ = std::fs::remove_dir_all(directory);
}

/// A number's formatted text stays charged while it is kept: ten calls whose id formats to
/// 301 digits fit an entry only with room for those texts beside their items.
#[test]
fn formatted_numbers_stay_charged() {
    let (pool, directory) = pool();
    let mut owner = pool.create().unwrap();
    let call = r#"{"type":"function_call","call_id":1e300,"name":"t","arguments":"{}"}"#;
    let turn = format!(r#"{{"output":[{}]}}"#, [call; 10].join(","));
    assert!(items(&mut owner, &turn, (6_000, 6_000)).is_none());
    assert_eq!(items(&mut owner, &turn, (8_000, 8_000)).map(|i| i.len()), Some(10));
    drop(owner);
    drop(pool);
    let _ = std::fs::remove_dir_all(directory);
}

/// Encrypted content past Grok's limit drops its item unchecked; the turn keeps the rest.
#[test]
fn oversized_reasoning_is_dropped_unchecked() {
    let (pool, directory) = pool();
    let mut owner = pool.create().unwrap();
    let content = "A".repeat(MAX_GROK_ENCRYPTED_CONTENT_LEN + 1);
    let call = r#"{"type":"function_call","call_id":"c","name":"t","arguments":"{}"}"#;
    let turn = format!(r#"{{"output":[{{"type":"reasoning","encrypted_content":"{content}"}},{call}]}}"#);
    let limits = (8 << 20, 256 << 20);
    let kept = items(&mut owner, &turn, limits).expect("the call is kept");
    assert_eq!(kept, vec![serde_json::from_str::<serde_json::Value>(call).unwrap()]);
    drop(owner);
    drop(pool);
    let _ = std::fs::remove_dir_all(directory);
}

/// A completed response: a function call to anchor it and 27 assistant messages of 70,000
/// characters, about 1.9 MB normalized.
fn completion() -> String {
    let message = format!(
        r#"{{"type":"message","role":"assistant","content":[{{"type":"output_text","text":"{}"}}]}}"#,
        "x".repeat(70_000)
    );
    let call = r#"{"type":"function_call","call_id":"call_1","name":"tool","arguments":"{}"}"#;
    format!(
        r#"{{"status":"completed","output":[{call},{}]}}"#,
        vec![message; 27].join(",")
    )
}

fn scope(session: usize) -> ReplayScope {
    ReplayScope {
        model_name: "grok-4.3".into(),
        session_key: session.to_string(),
    }
}

/// Caches the completion `json` for `session` once `start` lets every caller go, with this
/// thread's large allocations counted while it does.
fn cache(store: &Store, pool: &ck_stream::Store, json: &str, session: usize, start: &Barrier) {
    let mut owner = pool.create().unwrap();
    let doc = parse(&mut owner, json);
    start.wait();
    test_alloc::track(true);
    cache_completion(store, &scope(session), &doc, doc.root());
    test_alloc::track(false);
}

/// Live replay allocations never pass the cache's bound: a full cache, then two turns built
/// at once, each charged before it is allocated and released only once it is freed.
#[test]
fn concurrent_turns_stay_within_the_cache() {
    const TOTAL: usize = 8 << 20;
    let store = Arc::new(Store::with_limits(2 << 20, TOTAL));
    let (pool, directory) = pool();
    let pool = Arc::new(pool);
    let json = Arc::new(completion());
    for session in 0..5 {
        cache(&store, &pool, &json, session, &Barrier::new(1));
    }
    let start = Arc::new(Barrier::new(2));
    let builders: Vec<_> = (100..102)
        .map(|session| {
            let (store, pool, json, start) = (store.clone(), pool.clone(), json.clone(), start.clone());
            std::thread::spawn(move || cache(&store, &pool, &json, session, &start))
        })
        .collect();
    for builder in builders {
        builder.join().unwrap();
    }
    assert!(store.get(&scope(100)).is_some() || store.get(&scope(101)).is_some());
    let peak = test_alloc::peak();
    assert!(peak <= TOTAL, "{peak} live bytes");
    drop(pool);
    let _ = std::fs::remove_dir_all(directory);
}
