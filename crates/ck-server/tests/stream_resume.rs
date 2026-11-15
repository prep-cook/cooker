//! `GET /v1/streams` (`docs/INTERNAL-STREAM.md` §6.6–6.7, `docs/EXECUTION-CONTRACT.md` §6–7):
//! exact suffixes from finished and live streams, one 410 for every unusable identity,
//! cursor errors, expiry, eviction, explicit cancel, client disconnect and shutdown.

mod stream_fixture;

use std::time::Duration;

use axum::http::StatusCode;
use ck_server::http::egress::Framing;
use ck_server::inference::streams::StreamId;
use ck_stream::Terminal;
use stream_fixture::*;

/// Reads `response` until its text contains `needle`; returns that text and the rest.
async fn read_until(response: axum::response::Response, needle: &str) -> (String, axum::body::BodyDataStream) {
    use futures_util::StreamExt;
    let mut stream = response.into_body().into_data_stream();
    let mut text = String::new();
    while !text.contains(needle) {
        let bytes = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        text.push_str(std::str::from_utf8(&bytes).unwrap());
    }
    (text, stream)
}

async fn rest(mut stream: axum::body::BodyDataStream) -> (String, bool) {
    use futures_util::StreamExt;
    let mut text = String::new();
    while let Some(frame) = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
    {
        match frame {
            Ok(bytes) => text.push_str(std::str::from_utf8(&bytes).unwrap()),
            Err(_) => return (text, false),
        }
    }
    (text, true)
}

#[tokio::test]
async fn finished_sse_resumes_with_the_exact_suffix_after_every_cursor() {
    let fixture = Fixture::new(|_| {});
    let app = fixture.app();
    let mut live = commit(&fixture, owner(KEY), Framing::Sse, STREAMED);
    answer(&mut live);
    assert_eq!(live.stopped().await, Terminal::Done);

    let full = resume(&app, &live, "v1.e.0").await;
    assert_eq!(full.status(), StatusCode::OK);
    let headers = full.headers().clone();
    assert_eq!(headers["content-type"], "text/event-stream");
    assert_eq!(headers["cache-control"], "no-store");
    assert_eq!(headers["x-ck-stream-id"], live.id.header_value());
    assert_eq!(headers["x-ck-stream-cursor"], "v1.e.0");
    let (full, clean) = body(full).await;
    assert!(clean);
    let events = events(&full);
    assert!(events.len() >= 4, "{full}");
    for (index, event) in events.iter().enumerate() {
        assert!(
            event.ends_with(&format!("\nid: v1.e.{}\n\n", index + 1)),
            "event {index} carries the cursor after it: {event:?}"
        );
    }
    assert!(full.contains("hel") && full.contains("[DONE]"));

    for after in 0..=events.len() {
        let response = resume(&app, &live, &format!("v1.e.{after}")).await;
        assert_eq!(
            response.headers()["x-ck-stream-cursor"],
            format!("v1.e.{after}").as_str()
        );
        let (suffix, clean) = body(response).await;
        assert!(clean);
        assert_eq!(suffix, events[after..].concat(), "after {after}");
    }
    // `Last-Event-ID` replaces `after` for SSE and must agree with it when both appear.
    let header = live.header();
    let by_event_id = get(
        &app,
        KEY,
        "",
        &[("x-ck-stream-id", &header), ("last-event-id", "v1.e.2")],
    )
    .await;
    assert_eq!(body(by_event_id).await.0, events[2..].concat());
    let agreeing = get(
        &app,
        KEY,
        "?after=v1.e.2",
        &[("x-ck-stream-id", &header), ("last-event-id", "v1.e.2")],
    )
    .await;
    assert_eq!(agreeing.status(), StatusCode::OK);
    for (query, extra) in [
        (format!("?after=v1.e.{}", events.len() + 1), None),
        ("?after=v1.b.0".into(), None),
        ("?after=v1.e.1".into(), Some(("last-event-id", "v1.e.2"))),
        ("?after=v1.e.1&after=v1.e.1".into(), None),
        ("?after=v1.e.x".into(), None),
        (String::new(), None),
    ] {
        let mut headers = vec![("x-ck-stream-id", header.as_str())];
        headers.extend(extra);
        let (status, _) = status_body(get(&app, KEY, &query, &headers).await).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query} {extra:?}");
    }
}

#[tokio::test]
async fn finished_json_resumes_with_the_exact_byte_suffix() {
    let fixture = Fixture::new(|_| {});
    let app = fixture.app();
    let mut live = commit(&fixture, owner(KEY), Framing::Json, BUFFERED);
    answer(&mut live);
    live.stopped().await;

    let full = resume(&app, &live, "v1.b.0").await;
    assert_eq!(full.headers()["content-type"], "application/json");
    let (full, clean) = body(full).await;
    assert!(clean);
    let document: serde_json::Value = serde_json::from_str(&full).unwrap();
    assert_eq!(document["choices"][0]["message"]["content"], "hello");
    for after in [1, full.len() / 2, full.len()] {
        let (suffix, clean) = body(resume(&app, &live, &format!("v1.b.{after}")).await).await;
        assert!(clean);
        assert_eq!(suffix, full[after..], "after {after}");
    }
    for cursor in [format!("v1.b.{}", full.len() + 1), "v1.e.0".into()] {
        let (status, _) = status_body(resume(&app, &live, &cursor).await).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{cursor}");
    }
}

#[tokio::test]
async fn live_resume_follows_the_producer_without_gaps_or_repeats() {
    let fixture = Fixture::new(|_| {});
    let app = fixture.app();
    let mut live = commit(&fixture, owner(KEY), Framing::Sse, STREAMED);
    live.send(chunk(r#"{"role":"assistant","content":"hel"}"#, "null"));
    let (first_event, from_start) = read_until(resume(&app, &live, "v1.e.0").await, "id: v1.e.1\n\n").await;
    // A cursor past the published events is a client error while the stream runs.
    let (status, _) = status_body(resume(&app, &live, "v1.e.9").await).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let after_first = resume(&app, &live, "v1.e.1").await;
    assert_eq!(after_first.status(), StatusCode::OK);
    live.send(chunk(r#"{"content":"lo"}"#, "null"));
    live.send(chunk("{}", r#""stop""#));
    live.send(done());
    live.end();
    let (tail, clean) = rest(from_start).await;
    assert!(clean);
    let (after_first, clean) = body(after_first).await;
    assert!(clean);
    assert_eq!(after_first, tail, "the live suffix has no gap and no repeat");
    let (full, _) = body(resume(&app, &live, "v1.e.0").await).await;
    assert_eq!(full, format!("{first_event}{tail}"));
    assert_eq!(live.stopped().await, Terminal::Done);
}

#[tokio::test]
async fn every_unusable_identity_gets_the_same_410() {
    let fixture = Fixture::new(|_| {});
    let app = fixture.app();
    let mut live = commit(&fixture, owner(KEY), Framing::Sse, STREAMED);
    answer(&mut live);
    let mut scoped_owner = owner(KEY);
    scoped_owner.scope = Some("sess_other".into());
    let mut scoped = commit(&fixture, scoped_owner, Framing::Sse, STREAMED);
    answer(&mut scoped);
    let header = live.header();
    let unknown = StreamId::random().header_value().to_str().unwrap().to_owned();

    let reference = status_body(get(&app, KEY, "?after=v1.e.0", &[("x-ck-stream-id", &unknown)]).await).await;
    assert_eq!(reference.0, StatusCode::GONE);
    assert!(reference.1.contains("stream.gone"), "{}", reference.1);
    for (key, id) in [
        (OTHER, header.as_str()),
        (KEY, &scoped.header()),
        (KEY, "not-a-stream-id"),
        (KEY, &header.to_ascii_uppercase()),
    ] {
        // Unknown, foreign and malformed ids alike, whatever the cursor says.
        for query in ["?after=v1.e.0", "?after=v1.b.0", "?after=v1.e.99", "?after=bad", ""] {
            let got = status_body(get(&app, key, query, &[("x-ck-stream-id", id)]).await).await;
            assert_eq!(got, reference, "{key} {id} {query}");
        }
    }
    let (status, _) = status_body(get(&app, KEY, "?after=v1.e.0", &[]).await).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "the id is required");
    let (status, _) = status_body(
        get(
            &app,
            KEY,
            "?after=v1.e.0",
            &[("x-ck-stream-id", &header), ("x-ck-stream-id", &header)],
        )
        .await,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "a repeated id is refused");
    let (status, _) = status_body(get(&app, "", "?after=v1.e.0", &[("x-ck-stream-id", &header)]).await).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(resume(&app, &live, "v1.e.0").await.status(), StatusCode::OK);
}

#[tokio::test]
async fn an_unread_stream_expires_and_an_attached_reader_keeps_its_stream() {
    let fixture = Fixture::new(|config| config.cache_ttl = Duration::from_secs(60));
    let app = fixture.app();
    let mut idle = commit(&fixture, owner(KEY), Framing::Sse, STREAMED);
    answer(&mut idle);
    idle.stopped().await;
    let mut read = commit(&fixture, owner(KEY), Framing::Sse, STREAMED);
    read.send(chunk(r#"{"role":"assistant","content":"hel"}"#, "null"));
    let (_, attached) = read_until(resume(&app, &read, "v1.e.0").await, "hel").await;

    fixture.advance(Duration::from_secs(30));
    assert_eq!(resume(&app, &idle, "v1.e.0").await.status(), StatusCode::OK);
    fixture.advance(Duration::from_secs(61));
    fixture.advance(Duration::from_secs(1));
    // The expired handle is still registered; whatever the cursor, it answers as an
    // unknown id does.
    let unknown = StreamId::random().header_value().to_str().unwrap().to_owned();
    for query in ["?after=v1.e.0", "?after=v1.b.0", "?after=bad", ""] {
        let expired = status_body(get(&app, KEY, query, &[("x-ck-stream-id", &idle.header())]).await).await;
        let reference = status_body(get(&app, KEY, query, &[("x-ck-stream-id", &unknown)]).await).await;
        assert_eq!(expired, reference, "{query}");
        assert_eq!(expired.0, StatusCode::GONE);
    }

    // The waiting reader counts as reading: the live stream outlives the TTL.
    fixture.advance(Duration::from_secs(7200));
    read.send(chunk(r#"{"content":"lo"}"#, "null"));
    read.send(chunk("{}", r#""stop""#));
    read.send(done());
    read.end();
    let (tail, clean) = rest(attached).await;
    assert!(clean && tail.contains("lo"), "{tail}");
    assert_eq!(read.stopped().await, Terminal::Done);
}

#[tokio::test]
async fn admission_evicts_the_least_recently_read_exchange_whole() {
    // Two exchanges of two streams each fill the header slots.
    let fixture = Fixture::new(|config| config.header_slots = 4);
    let app = fixture.app();
    let mut first = commit(&fixture, owner(KEY), Framing::Sse, STREAMED);
    answer(&mut first);
    first.stopped().await;
    fixture.advance(Duration::from_secs(1));
    let mut second = commit(&fixture, owner(KEY), Framing::Sse, STREAMED);
    answer(&mut second);
    second.stopped().await;
    fixture.advance(Duration::from_secs(1));
    // Reading the first makes the second the least recently read.
    assert!(body(resume(&app, &first, "v1.e.0").await).await.1);
    fixture.advance(Duration::from_secs(1));

    let mut third = commit(&fixture, owner(KEY), Framing::Sse, STREAMED);
    answer(&mut third);
    third.stopped().await;
    assert_eq!(resume(&app, &second, "v1.e.0").await.status(), StatusCode::GONE);
    assert!(body(resume(&app, &first, "v1.e.0").await).await.1);
    assert!(body(resume(&app, &third, "v1.e.0").await).await.1);
}

#[tokio::test]
async fn explicit_cancel_stops_the_producer_and_keeps_the_prefix() {
    let fixture = Fixture::new(|_| {});
    let app = fixture.app();
    let mut live = commit(&fixture, owner(KEY), Framing::Sse, STREAMED);
    live.send(chunk(r#"{"role":"assistant","content":"hel"}"#, "null"));
    let (prefix, attached) = read_until(resume(&app, &live, "v1.e.0").await, "hel").await;

    assert!(
        !fixture.rt.streams().cancel(&live.id, &owner(OTHER)),
        "only the owner cancels"
    );
    assert!(fixture.rt.streams().cancel(&live.id, &owner(KEY)));
    assert_eq!(live.stopped().await, Terminal::Cancelled);
    // The attached reader and a later resume get the published prefix, then an abnormal
    // close: the client never sees a clean end of an incomplete response.
    let (tail, clean) = rest(attached).await;
    assert!(!clean);
    let (resumed, clean) = body(resume(&app, &live, "v1.e.0").await).await;
    assert_eq!((resumed, clean), (format!("{prefix}{tail}"), false));
    assert!(!prefix.contains("[DONE]") && !tail.contains("[DONE]"));
}

#[tokio::test]
async fn a_client_disconnect_releases_only_its_reader() {
    let fixture = Fixture::new(|_| {});
    let app = fixture.app();
    let mut live = commit(&fixture, owner(KEY), Framing::Sse, STREAMED);
    live.send(chunk(r#"{"role":"assistant","content":"hel"}"#, "null"));
    let (_, attached) = read_until(resume(&app, &live, "v1.e.0").await, "hel").await;
    drop(attached);
    live.send(chunk(r#"{"content":"lo"}"#, "null"));
    live.send(chunk("{}", r#""stop""#));
    live.send(done());
    live.end();
    assert_eq!(live.stopped().await, Terminal::Done);
    let (full, clean) = body(resume(&app, &live, "v1.e.0").await).await;
    assert!(clean && full.contains("hel") && full.contains("[DONE]"), "{full}");
}

#[tokio::test]
async fn shutdown_stops_streams_and_refuses_new_ones() {
    let fixture = Fixture::new(|_| {});
    let app = fixture.app();
    let mut live = commit(&fixture, owner(KEY), Framing::Sse, STREAMED);
    live.send(chunk(r#"{"role":"assistant","content":"hel"}"#, "null"));
    let (_, attached) = read_until(resume(&app, &live, "v1.e.0").await, "hel").await;

    fixture.rt.begin_shutdown();
    assert_eq!(live.stopped().await, Terminal::Shutdown);
    assert!(
        !rest(attached).await.1,
        "the shutdown terminal closes the body abnormally"
    );
    assert_eq!(resume(&app, &live, "v1.e.0").await.status(), StatusCode::GONE);
    // A commitment racing shutdown stops at once.
    let mut late = commit(&fixture, owner(KEY), Framing::Sse, STREAMED);
    assert_eq!(late.stopped().await, Terminal::Shutdown);
    assert_eq!(resume(&app, &late, "v1.e.0").await.status(), StatusCode::GONE);

    fixture.rt.finish_shutdown().await.unwrap();
    assert!(matches!(
        fixture.rt.streams().with_store(|_| ()),
        Err(ck_stream::Error::Closed)
    ));
}
