use super::*;

/// Go `TestWebsocketActivationAfterUpstreamDisconnect` (d31b61cb): a turn that activates
/// after the socket's reader stopped, on a close or a binary message, gets the reader's
/// error at once instead of waiting forever. The xAI executor shares this reader.
#[tokio::test]
async fn a_turn_activating_after_the_reader_stopped_gets_its_error() {
    for (name, binary, close) in [("close", false, true), ("binary", true, false)] {
        let (up, url) = upstream(vec![]).await;
        *up.binary_first.lock().unwrap() = binary;
        *up.close_first.lock().unwrap() = close;
        let executor = CodexExecutor::new().unwrap();
        let session = executor.ws.session("conn-1");
        let target = Target::new(
            &credential(&url),
            ws_url(&format!("{url}/responses")).unwrap(),
            &Config::default(),
        );
        let (conn, _) = executor
            .ensure(&session, &target, &HeaderMap::new(), "gpt-fixture", false, None)
            .await
            .unwrap_or_else(|e| panic!("{name}: {e:?}"));
        tokio::time::timeout(Duration::from_secs(5), async {
            while conn.link.lock().unwrap().lost.is_none() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{name}: the disconnect was not processed"));
        let mut reads = conn.activate();
        let read = tokio::time::timeout(Duration::from_secs(1), reads.recv())
            .await
            .unwrap_or_else(|_| panic!("{name}: activation waited"));
        assert!(matches!(read, Some(Read::Failed(_))), "{name}");
    }
}

/// A retired reader cannot publish its loss after the replacement attempt clears it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clearing_loss_waits_for_the_retired_readers_notification() {
    let (_up, url) = upstream(vec![]).await;
    let executor = CodexExecutor::new().unwrap();
    let state = executor.ws.session("conn-1");
    let old = pooled(&executor, &url).await;
    let (entered, notification) = std::sync::mpsc::channel();
    let (resume, released) = std::sync::mpsc::channel();
    *state.before_notify.lock().unwrap() = Some((entered, released));
    let (started, clearing) = std::sync::mpsc::channel();
    let (cleared, completed) = std::sync::mpsc::channel();
    let (retiring, retired, pool) = (state.clone(), old.clone(), executor.ws.clone());
    let runtime = tokio::runtime::Handle::current();
    let cleared_early = std::thread::scope(move |threads| {
        threads.spawn(move || {
            let _runtime = runtime.enter();
            retiring.invalidate(&retired, &transport("old upstream EOF"), true);
        });
        notification.recv_timeout(Duration::from_secs(5)).unwrap();
        threads.spawn(move || {
            started.send(()).unwrap();
            pool.clear_loss("conn-1");
            cleared.send(()).unwrap();
        });
        clearing.recv_timeout(Duration::from_secs(5)).unwrap();
        let cleared_early = completed.recv_timeout(Duration::from_millis(100)).is_ok();
        let _ = resume.send(());
        (cleared_early, completed)
    });
    let (cleared_early, _completion) = cleared_early;
    let replacement = pooled(&executor, &url).await;
    // A late callback from the old socket must not poison or remove the replacement.
    state.invalidate(&old, &transport("late old EOF"), true);
    assert!(executor.session_loss("conn-1").is_none());
    assert!(state.current().is_some_and(|conn| Arc::ptr_eq(&conn, &replacement)));
    let closed = executor.session_closed("conn-1");
    state.invalidate(&replacement, &closed_too_big("replacement"), true);
    let loss = tokio::time::timeout(Duration::from_secs(5), closed).await.unwrap();
    executor.close_session("conn-1");
    assert!(!cleared_early, "the reset overtook the retired reader's notification");
    assert_eq!(loss.status, 413, "the replacement's own loss must still be delivered");
}

#[tokio::test]
async fn continuation_without_a_socket_requires_replay() {
    let (up, url) = upstream(vec![]).await;
    let executor = CodexExecutor::new().unwrap();
    let error = executor
        .execute_in_session(&credential(&url), request(BODY), &Config::default(), &session(true))
        .await
        .err()
        .expect("no socket to continue on");
    assert!(error.is_replay_required(), "{error}");
    assert!(up.dials.lock().unwrap().is_empty(), "a continuation never dials");
}

/// Two turns share one dial; the continuation turn reuses it, and closing the session
/// closes the upstream socket and drops the pooled session.
#[tokio::test]
async fn turns_reuse_the_socket_and_close_session_releases_it() {
    let (up, url) = upstream(vec![
        Act::Send(vec![CREATED, COMPLETED]),
        Act::Send(vec![CREATED, COMPLETED]),
    ])
    .await;
    let executor = CodexExecutor::new().unwrap();
    let cfg = Config::default();
    let cred = credential(&url);
    for continuation in [false, true] {
        let response = executor
            .execute_in_session(&cred, request(BODY), &cfg, &session(continuation))
            .await
            .unwrap();
        let events = collect(response).await;
        assert_eq!(events.len(), 2, "{events:?}");
        assert!(events.iter().all(Result::is_ok));
    }
    assert_eq!(up.dials.lock().unwrap().len(), 1);
    assert_eq!(up.frames.lock().unwrap().len(), 2);
    assert_eq!(executor.ws.len(), 1);
    executor.close_session("conn-1");
    assert_eq!(executor.ws.len(), 0);
    for _ in 0..50 {
        if *up.ended.lock().unwrap() == 1 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("upstream socket still open after close_session");
}

/// Go 1e37b52b: after a reload that changes only `requests.proxy-url`, the session's
/// open socket keeps serving its credential (continuation and new turn alike), while a
/// new session dials through the new proxy and a credential's own proxy still re-targets.
#[tokio::test]
async fn a_global_proxy_reload_keeps_the_open_socket() {
    let (up, url) = upstream(vec![Act::Send(vec![CREATED, COMPLETED]); 3]).await;
    let executor = CodexExecutor::new().unwrap();
    let cred = credential(&url);
    // Nothing listens on port 1: a dial through this proxy fails.
    let reloaded = Config::parse("requests:\n  proxy-url: http://127.0.0.1:1\n").unwrap();
    for (cfg, continuation) in [(&Config::default(), false), (&reloaded, true), (&reloaded, false)] {
        let response = executor
            .execute_in_session(&cred, request(BODY), cfg, &session(continuation))
            .await
            .unwrap_or_else(|e| panic!("continuation={continuation}: {e:?}"));
        let events = collect(response).await;
        assert!(events.len() == 2 && events.iter().all(Result::is_ok), "{events:?}");
    }
    assert_eq!(up.dials.lock().unwrap().len(), 1, "every turn kept the first socket");

    let mut other = request(BODY);
    other.execution_session = Some("conn-2".into());
    let fresh = ExecSession {
        id: "conn-2".into(),
        ..session(false)
    };
    let error = executor
        .execute_in_session(&cred, other, &reloaded, &fresh)
        .await
        .err()
        .expect("a new socket dials through the reloaded proxy");
    assert_eq!(error.scope, FailureScope::Transport, "{error}");

    let mut own = cred.clone();
    own.attributes.insert("proxy_url".into(), "http://127.0.0.1:1".into());
    let error = executor
        .execute_in_session(&own, request(BODY), &reloaded, &session(true))
        .await
        .err()
        .expect("the credential's own proxy no longer matches the socket");
    assert!(error.is_replay_required(), "{error}");
    assert_eq!(up.dials.lock().unwrap().len(), 1);
}

/// Go `websocketSessionTargetMatches` compares the trimmed proxy settings, not parsed
/// proxies: `direct` and `none`, or two unparseable settings, are different targets. A
/// continuation that changes `(global, own)` after the first turn keeps the socket or
/// needs a replay as Go decides.
#[tokio::test]
async fn proxy_settings_match_as_go_compares_them() {
    type Row = (
        &'static str,
        Option<&'static str>,
        &'static str,
        Option<&'static str>,
        bool,
    );
    let rows: [Row; 6] = [
        ("", None, "http://127.0.0.1:1", None, true),
        ("direct", Some("none"), "http://127.0.0.1:1", None, false),
        ("", Some("direct"), "", Some("none"), false),
        ("", Some("bogus://a"), "", Some("bogus://b"), false),
        ("", Some("direct"), "", Some(" direct "), true),
        ("none", None, "NONE", None, true),
    ];
    for (dial_global, dial_own, global, own, reuse) in rows {
        let row = format!("{dial_global:?} {dial_own:?} -> {global:?} {own:?}");
        let (up, url) = upstream(vec![Act::Send(vec![CREATED, COMPLETED]); 2]).await;
        let executor = CodexExecutor::new().unwrap();
        let turn = |global: &str, own: Option<&str>, continuation: bool| {
            let cfg = Config::parse(&format!("requests:\n  proxy-url: {global:?}\n")).unwrap();
            let mut cred = credential(&url);
            if let Some(own) = own {
                cred.attributes.insert("proxy_url".into(), own.into());
            }
            let executor = &executor;
            async move {
                let response = executor
                    .execute_in_session(&cred, request(BODY), &cfg, &session(continuation))
                    .await?;
                let events = collect(response).await;
                assert!(events.len() == 2 && events.iter().all(Result::is_ok), "{events:?}");
                Ok::<(), ExecError>(())
            }
        };
        turn(dial_global, dial_own, false)
            .await
            .unwrap_or_else(|e| panic!("{row}: first turn {e:?}"));
        match turn(global, own, true).await {
            Ok(()) => assert!(reuse, "{row}: kept the socket"),
            Err(e) => assert!(!reuse && e.is_replay_required(), "{row}: {e:?}"),
        }
        assert_eq!(up.dials.lock().unwrap().len(), 1, "{row}");
    }
}
