use super::*;

/// A rejected handshake reports the upstream status with Go's classification and the
/// quota headers are observed.
#[tokio::test]
async fn handshake_rejection_is_classified() {
    let (up, url) = upstream(vec![]).await;
    *up.reject.lock().unwrap() = Some((401, r#"{"error":{"message":"bad key","type":"invalid_request_error"}}"#));
    let executor = CodexExecutor::new().unwrap();
    let error = executor
        .execute_in_session(&credential(&url), request(BODY), &Config::default(), &session(false))
        .await
        .err()
        .expect("401 handshake");
    assert_eq!(error.status, 401);
    assert_eq!(error.scope, FailureScope::Credential, "{error}");
    assert!(String::from_utf8_lossy(&error.body).contains("bad key"));
    assert_eq!(
        error
            .headers
            .get("x-codex-primary-used-percent")
            .map(|v| v.to_str().unwrap()),
        Some("97")
    );
}

/// With bootstrap buffering, an overload rejection before any output fails the attempt
/// over without telling the downstream session its socket was lost.
#[tokio::test]
async fn bootstrap_overload_fails_over_without_disconnect_notice() {
    const FAILED: &str = r#"{"type":"response.failed","response":{"id":"r1","error":{"code":"server_is_overloaded","message":"overloaded"}}}"#;
    let (_up, url) = upstream(vec![Act::Send(vec![CREATED, FAILED])]).await;
    let executor = CodexExecutor::new().unwrap();
    // Legacy top-level spelling: global, so it applies to this API key too.
    let cfg = Config::parse("upstream:\n  codex:\n    stream-bootstrap-buffering: true\n").unwrap();
    let error = executor
        .execute_in_session(&credential(&url), request(BODY), &cfg, &session(false))
        .await
        .err()
        .expect("overload fails the attempt");
    assert_ne!(error.scope, FailureScope::Request, "another credential may serve it");
    let closed = executor.session_closed("conn-1");
    assert!(
        tokio::time::timeout(Duration::from_millis(200), closed).await.is_err(),
        "overload must not close the downstream session"
    );
}

/// An upstream close with 1009 reaches the turn as Go's fixed request-scoped 413
/// (`mapCodexWebsocketReadError`), while the session's loss keeps the upstream reason
/// (gorilla's close error, which the downstream handler turns into close 1009).
#[tokio::test]
async fn close_1009_is_a_request_scoped_413_and_notifies() {
    let (_up, url) = upstream(vec![Act::SendClose(vec![CREATED], 1009)]).await;
    let executor = CodexExecutor::new().unwrap();
    let closed = executor.session_closed("conn-1");
    let response = executor
        .execute_in_session(&credential(&url), request(BODY), &Config::default(), &session(false))
        .await
        .unwrap();
    let events = collect(response).await;
    let error = events.last().unwrap().as_ref().expect_err("turn ends with the close");
    assert_eq!((error.status, error.scope), (413, FailureScope::Request));
    assert_eq!(
        String::from_utf8_lossy(&error.body),
        r#"{"error":{"message":"upstream websocket message too big","type":"invalid_request_error","code":"message_too_big"}}"#
    );
    let notified = tokio::time::timeout(Duration::from_secs(2), closed)
        .await
        .expect("session notified");
    assert_eq!(notified.status, 413);
    assert_eq!(
        gjson::get(&String::from_utf8_lossy(&notified.body), "error.message").str(),
        "bye"
    );
    assert_eq!(executor.session_loss("conn-1").map(|e| e.body), Some(notified.body));
}

/// gorilla/websocket reads a peer that vanishes without a close frame as close 1006,
/// and Go keeps that text.
#[tokio::test]
async fn a_socket_dropped_without_a_close_frame_reads_as_close_1006() {
    let (_up, url) = upstream(vec![Act::SendDrop(vec![CREATED])]).await;
    let executor = CodexExecutor::new().unwrap();
    let response = executor
        .execute_in_session(&credential(&url), request(BODY), &Config::default(), &session(false))
        .await
        .unwrap();
    let events = tokio::time::timeout(Duration::from_secs(5), collect(response))
        .await
        .expect("the turn ends with the dropped socket");
    let error = events.last().unwrap().as_ref().expect_err("the turn fails");
    assert_eq!(error.scope, FailureScope::Transport, "{error}");
    assert!(
        String::from_utf8_lossy(&error.body).contains("websocket: close 1006 (abnormal closure): unexpected EOF"),
        "{error}"
    );
}

/// Go keeps one read deadline per application message: pings answered while waiting do
/// not extend it, so a stalled upstream that only pings still times out.
#[tokio::test]
async fn pings_do_not_extend_the_idle_deadline() {
    let (_up, url) = upstream(vec![Act::Pings(Duration::from_millis(100), 30)]).await;
    let mut executor = CodexExecutor::new().unwrap();
    executor.ws = Arc::new(Pool::with_idle(Duration::from_millis(400)));
    let response = executor
        .execute_in_session(&credential(&url), request(BODY), &Config::default(), &session(false))
        .await
        .unwrap();
    let events = tokio::time::timeout(Duration::from_secs(2), collect(response))
        .await
        .expect("idle deadline fires despite pings");
    let error = events
        .last()
        .unwrap()
        .as_ref()
        .expect_err("the turn ends with the timeout");
    assert_eq!(error.scope, FailureScope::Transport);
    assert!(String::from_utf8_lossy(&error.body).contains("idle timeout"), "{error}");
}

/// A socket whose upstream stopped reading (half-open) fails the write after the write
/// bound instead of holding the turn until the read deadline; the one retry on a fresh
/// socket is bounded the same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_the_upstream_never_reads_times_out() {
    let dials = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = dials.clone();
    let app = axum::Router::new().fallback(move |ws: WebSocketUpgrade| {
        counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        async move {
            ws.on_upgrade(|socket| async move {
                // Hold the socket open without reading from it.
                tokio::time::sleep(Duration::from_secs(120)).await;
                drop(socket);
            })
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });

    let mut executor = CodexExecutor::new().unwrap();
    executor.ws = Arc::new(Pool::with_write(WriteLimit {
        floor: Duration::from_millis(300),
        rate: usize::MAX,
    }));
    // Far larger than loopback socket buffers, so the write cannot finish.
    let body = format!(
        r#"{{"model":"gpt-fixture","input":[{{"type":"message","role":"user","content":"{}"}}]}}"#,
        "x".repeat(32 << 20)
    );
    // Preparing a 32 MiB body takes seconds in a debug build; without the bound the
    // turn would wait for the upstream to drop the socket (120 s).
    let result = tokio::time::timeout(
        Duration::from_secs(60),
        executor.execute_in_session(&credential(&url), request(&body), &Config::default(), &session(false)),
    )
    .await
    .expect("the write bound ends the turn");
    let Err(error) = result else {
        panic!("a write that never completes fails the turn");
    };
    assert_eq!(error.scope, FailureScope::Transport);
    assert!(
        String::from_utf8_lossy(&error.body).contains("write timed out"),
        "{error}"
    );
    assert_eq!(
        dials.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "one retry on a fresh socket"
    );
    assert_eq!(
        WRITE_LIMIT.deadline(1 << 20),
        Duration::from_secs(10),
        "small frames get the floor"
    );
    assert_eq!(
        WRITE_LIMIT.deadline(5 << 20),
        Duration::from_secs(40),
        "5 MiB at 128 KiB/s"
    );
    // Partial seconds round up: just under 11 x 128 KiB needs almost 11 s.
    assert_eq!(WRITE_LIMIT.deadline(11 * (128 << 10) - 1), Duration::from_secs(11));
}

/// A timed-out write maps like a failed one: after the upstream closed with 1009 it is
/// the request-scoped 413 (no retry on a fresh socket), else a transport error.
#[test]
fn write_errors_keep_the_message_too_big_413() {
    let too_big = message_too_big();
    for message in [
        "codex websockets executor: write timed out",
        "codex websockets executor: write failed",
    ] {
        let error = write_error(Some(&too_big), message);
        assert_eq!((error.status, error.scope), (413, FailureScope::Request), "{message}");
        let other = transport("codex websockets executor: read idle timeout");
        for lost in [None, Some(&other)] {
            let error = write_error(lost, message);
            assert_eq!(error.scope, FailureScope::Transport, "{message}");
            assert!(String::from_utf8_lossy(&error.body).contains(message), "{message}");
        }
    }
}

/// A reader that fails before the first turn activates (binary message right after the
/// handshake) must fail that turn instead of leaving it waiting on a dead socket.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_failure_before_the_turn_activates_fails_the_turn() {
    for _ in 0..20 {
        let (up, url) = upstream(vec![]).await;
        *up.binary_first.lock().unwrap() = true;
        let executor = CodexExecutor::new().unwrap();
        let (cred, cfg, sess) = (credential(&url), Config::default(), session(false));
        let turn = executor.execute_in_session(&cred, request(BODY), &cfg, &sess);
        let outcome = tokio::time::timeout(Duration::from_secs(2), async {
            match turn.await {
                Ok(response) => collect(response).await.into_iter().find_map(Result::err),
                Err(error) => Some(error),
            }
        })
        .await
        .expect("the turn must not hang on a dead socket");
        let error = outcome.expect("the turn fails");
        assert_eq!(error.scope, FailureScope::Transport, "{error}");
    }
}

/// With model-level cooling, a usage-limit handshake rejection cools only the model
/// (`newCodexStatusErrWithCooling`); without it, the whole credential. The setting is
/// shared (`upstream.codex`, Go 52d5507d), so every spelling applies to API keys.
#[tokio::test]
async fn handshake_usage_limit_honours_model_level_cooling() {
    const LIMIT: &str = r#"{"error":{"type":"usage_limit_reached","message":"limit","resets_in_seconds":3600}}"#;
    for (yaml, scope) in [
        ("{}\n", FailureScope::Credential),
        (
            "upstream:\n  codex:\n    model-level-cooling: true\n",
            FailureScope::Model,
        ),
    ] {
        let (up, url) = upstream(vec![]).await;
        *up.reject.lock().unwrap() = Some((429, LIMIT));
        let cfg = Config::parse(yaml).unwrap();
        let error = CodexExecutor::new()
            .unwrap()
            .execute_in_session(&credential(&url), request(BODY), &cfg, &session(false))
            .await
            .err()
            .expect("429 handshake");
        assert_eq!((error.status, error.scope), (429, scope), "{yaml}");
        assert_eq!(error.retry_after, Some(Duration::from_secs(3600)));
    }
}

/// Upstream frames above tungstenite's 16 MiB default reach the client (Go sets no read
/// limit; the executor allows 64 MiB).
#[tokio::test]
async fn large_upstream_frames_are_delivered() {
    let delta = format!(
        r#"{{"type":"response.output_text.delta","delta":"{}"}}"#,
        "a".repeat(17 * 1024 * 1024)
    );
    let len = delta.len();
    let (up, url) = upstream(vec![Act::SendOwned(delta)]).await;
    up.script.lock().unwrap().push_back(Act::Send(vec![]));
    let executor = CodexExecutor::new().unwrap();
    let response = executor
        .execute_in_session(&credential(&url), request(BODY), &Config::default(), &session(false))
        .await
        .unwrap();
    let ResponseBody::Stream(mut stream) = response.body else {
        panic!("websocket turns stream");
    };
    let first = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .expect("frame arrives")
        .expect("stream item")
        .expect("not an error");
    assert_eq!(first.len(), len);
}

/// One final server text frame compressed like gorilla's writer (sync flush, the
/// `00 00 ff ff` tail removed).
fn compressed_frame(text: &str) -> Vec<u8> {
    let mut deflate = flate2::Compress::new(flate2::Compression::fast(), false);
    let mut payload = Vec::with_capacity(text.len() + 64);
    deflate
        .compress_vec(text.as_bytes(), &mut payload, flate2::FlushCompress::Sync)
        .unwrap();
    assert!(payload.ends_with(&[0, 0, 0xff, 0xff]));
    payload.truncate(payload.len() - 4);
    let mut frame = vec![0xc1];
    match payload.len() {
        n if n < 126 => frame.push(n as u8),
        n => {
            frame.push(126);
            frame.extend_from_slice(&(n as u16).to_be_bytes());
        }
    }
    frame.extend_from_slice(&payload);
    frame
}

/// permessage-deflate end to end: the dial offers gorilla's extension, an upstream that
/// agrees compresses its events, and the turn yields exactly what the same events give
/// uncompressed.
#[tokio::test]
async fn compressed_upstream_events_read_like_plain_ones() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut tcp, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buf = [0u8; 4096];
        while !request.windows(4).any(|w| w == b"\r\n\r\n") {
            let n = tcp.read(&mut buf).await.unwrap();
            request.extend_from_slice(&buf[..n]);
        }
        let request = String::from_utf8(request).unwrap();
        let header = |name: &str| {
            request.lines().find_map(|line| {
                let (k, v) = line.split_once(':')?;
                k.trim().eq_ignore_ascii_case(name).then(|| v.trim().to_owned())
            })
        };
        let accept = tokio_tungstenite::tungstenite::handshake::derive_accept_key(
            header("sec-websocket-key").unwrap().as_bytes(),
        );
        let response = format!(
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
             Sec-WebSocket-Accept: {accept}\r\nSec-WebSocket-Extensions: {}\r\n\r\n",
            deflate::OFFER
        );
        tcp.write_all(response.as_bytes()).await.unwrap();
        // The turn's response.create, then the compressed events.
        let _ = tcp.read(&mut buf).await.unwrap();
        for event in [CREATED, COMPLETED] {
            tcp.write_all(&compressed_frame(event)).await.unwrap();
        }
        let _ = tcp.read(&mut buf).await;
        header("sec-websocket-extensions")
    });
    let executor = CodexExecutor::new().unwrap();
    let response = executor
        .execute_in_session(&credential(&url), request(BODY), &Config::default(), &session(false))
        .await
        .unwrap();
    let compressed: Vec<String> = collect(response).await.into_iter().map(Result::unwrap).collect();
    executor.close_session("conn-1");
    assert_eq!(server.await.unwrap().as_deref(), Some(deflate::OFFER));

    let (_up, plain_url) = upstream(vec![Act::Send(vec![CREATED, COMPLETED])]).await;
    let executor = CodexExecutor::new().unwrap();
    let response = executor
        .execute_in_session(
            &credential(&plain_url),
            request(BODY),
            &Config::default(),
            &session(false),
        )
        .await
        .unwrap();
    let plain: Vec<String> = collect(response).await.into_iter().map(Result::unwrap).collect();
    assert_eq!(compressed.len(), 2, "{compressed:?}");
    assert_eq!(compressed, plain);
}

/// A close 1009 that arrives while the turn's channel is full cannot be queued; the turn
/// still ends with Go's 413 from the session's recorded loss, not a generic channel error.
#[tokio::test]
async fn close_1009_on_a_full_turn_channel_keeps_the_413() {
    const DELTA: &str = r#"{"type":"response.output_text.delta","output_index":0,"delta":"x"}"#;
    let mut events = vec![CREATED];
    events.extend(std::iter::repeat_n(DELTA, TURN_BUFFER - 1));
    let (_up, url) = upstream(vec![Act::SendClose(events, 1009)]).await;
    let executor = CodexExecutor::new().unwrap();
    let closed = executor.session_closed("conn-1");
    let response = executor
        .execute_in_session(&credential(&url), request(BODY), &Config::default(), &session(false))
        .await
        .unwrap();
    // Nothing reads the turn until the reader has filled the channel and seen the close.
    tokio::time::timeout(Duration::from_secs(5), closed)
        .await
        .expect("session notified");
    let events = collect(response).await;
    assert_eq!(events.len(), TURN_BUFFER + 1, "every queued event, then the error");
    let error = events.last().unwrap().as_ref().expect_err("turn ends with the close");
    assert_eq!(error.status, 413);
    assert_eq!(
        gjson::get(&String::from_utf8_lossy(&error.body), "error.message").str(),
        "upstream websocket message too big"
    );
}

/// Go's duplex writes upstream from its own goroutine: a steer stuck in the network (the
/// upstream stopped reading) must not hold back the upstream's next event.
#[tokio::test]
async fn duplex_delivers_upstream_events_while_a_write_is_blocked() {
    const DELTA: &str = r#"{"type":"response.output_text.delta","output_index":0,"delta":"x"}"#;
    let app = axum::Router::new().fallback(|ws: WebSocketUpgrade| async move {
        ws.on_upgrade(|mut socket| async move {
            // The bootstrap create, then no more reads for the socket's life.
            let _ = socket.recv().await;
            let _ = socket.send(AxMessage::Text(CREATED.into())).await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            let _ = socket.send(AxMessage::Text(DELTA.into())).await;
            tokio::time::sleep(Duration::from_secs(30)).await;
        })
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });

    let executor = CodexExecutor::new().unwrap();
    let (frames, input) = tokio::sync::mpsc::channel(4);
    let input = Arc::new(tokio::sync::Mutex::new(input));
    executor.attach_steering("conn-1", SteeringInput::new(input, |_| true));
    let cfg = Config::parse("upstream:\n  codex:\n    response-steering: true\n").unwrap();
    let response = executor
        .execute_in_session(&credential(&url), request(BODY), &cfg, &session(false))
        .await
        .unwrap();
    let ResponseBody::Stream(mut stream) = response.body else {
        panic!("websocket turns stream");
    };
    let created = stream.next().await.unwrap().unwrap();
    assert_eq!(
        gjson::get(&String::from_utf8_lossy(&created), "type").str(),
        "response.created"
    );
    // Far larger than loopback socket buffers: the write cannot finish while upstream
    // does not read.
    let steer = format!(
        r#"{{"type":"response.steer","previous_response_id":"r1","input":"{}"}}"#,
        "x".repeat(32 << 20)
    );
    frames.send(Bytes::from(steer)).await.unwrap();
    let next = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("the delta arrives while the steer is still being written");
    assert_eq!(String::from_utf8_lossy(&next.unwrap().unwrap()), DELTA);
}
