use super::*;

/// Go `TestInterruptExecutionSessionRequiresActiveRead`: a socket kept between turns is
/// not the current upstream (the HTTP fallback may cancel locally); once a turn reads,
/// the interrupt reaches the socket byte for byte.
#[tokio::test]
async fn interrupt_needs_a_turn_reading_on_the_socket() {
    let (up, url) = upstream(vec![]).await;
    let executor = CodexExecutor::new().unwrap();
    assert_eq!(
        executor
            .interrupt_session("conn-1", INTERRUPT_R1.as_bytes(), |_| true)
            .await,
        Err(InterruptError::NoActive),
        "no session"
    );
    let conn = pooled(&executor, &url).await;
    assert_eq!(
        executor
            .interrupt_session("conn-1", INTERRUPT_R1.as_bytes(), |_| true)
            .await,
        Err(InterruptError::NoActive),
        "idle socket"
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(up.frames.lock().unwrap().is_empty(), "the idle socket was written");

    let _reads = conn.activate();
    let mut checked = Vec::new();
    let check = |id: &str| {
        checked.push(id.to_owned());
        false
    };
    assert_eq!(
        executor
            .interrupt_session("conn-1", INTERRUPT_R1.as_bytes(), check)
            .await,
        Err(InterruptError::Rejected(
            "websocket credential is no longer enabled".into()
        )),
        "disabled credential"
    );
    assert_eq!(checked, ["codex-ws.json"], "the check gets the socket's credential");
    executor
        .interrupt_session("conn-1", INTERRUPT_R1.as_bytes(), |_| true)
        .await
        .unwrap();
    assert_eq!(received(&up, 1).await, [INTERRUPT_R1]);
}

/// A client that goes away mid-turn drops the turn: the socket stays pooled but no turn
/// reads on it, so a later `response.interrupt` finds no current upstream and is not
/// written to the socket.
#[tokio::test]
async fn a_dropped_turn_stops_reading_on_its_socket() {
    let (up, url) = upstream(vec![Act::Send(vec![CREATED])]).await;
    let executor = CodexExecutor::new().unwrap();
    let response = executor
        .execute_in_session(&credential(&url), request(BODY), &Config::default(), &session(false))
        .await
        .unwrap();
    let ResponseBody::Stream(mut stream) = response.body else {
        panic!("websocket turns stream");
    };
    let first = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap();
    assert!(matches!(first, Some(Ok(_))), "{first:?}");
    let conn = executor.ws.session("conn-1").current().expect("the socket is pooled");
    assert!(conn.link.lock().unwrap().active.is_some(), "the turn reads");
    drop(stream);
    assert!(
        conn.link.lock().unwrap().active.is_none(),
        "the dropped turn still reads"
    );
    assert_eq!(
        executor
            .interrupt_session("conn-1", INTERRUPT_R1.as_bytes(), |_| true)
            .await,
        Err(InterruptError::NoActive)
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        up.frames.lock().unwrap().len(),
        1,
        "only the turn's request reached the socket"
    );
}

/// Go `TestInterruptExecutionSessionCompletesDuringAuthCheck`: the response finishes
/// while the account check runs. The interrupt is then done, not forwarded: the socket's
/// state is read after the check.
#[tokio::test]
async fn interrupt_for_a_response_finished_during_the_auth_check_is_done() {
    let (up, url) = upstream(vec![Act::Send(vec![CREATED, COMPLETED])]).await;
    let executor = CodexExecutor::new().unwrap();
    let conn = pooled(&executor, &url).await;
    let mut reads = conn.activate();
    conn.send(r#"{"type":"response.create"}"#.into()).await.unwrap();
    read_text(&mut reads).await;
    read_text(&mut reads).await;
    // The turn is still reading when the check starts and done when it returns.
    let finish = || {
        conn.deactivate();
        true
    };
    executor
        .interrupt_session("conn-1", br#"{"type":"response.interrupt","response_id":"r1"}"#, |_| {
            finish()
        })
        .await
        .unwrap();
    conn.send("barrier".into()).await.unwrap();
    assert_eq!(received(&up, 2).await, [r#"{"type":"response.create"}"#, "barrier"]);
}

/// Go `TestInterruptExecutionSessionAfterTerminal`: a late interrupt for a finished
/// response is done, also while the next response runs, and never reaches upstream; an
/// unknown ID on an idle socket falls through to the HTTP fallback; after the socket is
/// lost there is no active upstream.
#[tokio::test]
async fn late_interrupts_for_finished_responses_are_done() {
    for terminal in ["response.completed", "response.incomplete", "response.failed"] {
        let finished = format!(r#"{{"type":"{terminal}","response":{{"id":"r1","output":[]}}}}"#);
        let (up, url) = upstream(vec![
            Act::SendFrames(vec![CREATED.into(), finished]),
            Act::Send(vec![r#"{"type":"response.incomplete","response":{"id":"r2"}}"#]),
            Act::SendClose(vec![], 1000),
        ])
        .await;
        let executor = CodexExecutor::new().unwrap();
        let conn = pooled(&executor, &url).await;
        let interrupt = |id: &str| format!(r#"{{"type":"response.interrupt","response_id":"{id}"}}"#);
        let send = |id: &'static str| {
            let executor = &executor;
            async move {
                executor
                    .interrupt_session("conn-1", interrupt(id).as_bytes(), |_| true)
                    .await
            }
        };
        let mut reads = conn.activate();
        conn.send(r#"{"type":"response.create"}"#.into()).await.unwrap();
        read_text(&mut reads).await;
        read_text(&mut reads).await;
        conn.deactivate();
        for _ in 0..2 {
            assert_eq!(send("r1").await, Ok(()), "{terminal}: finished r1");
        }
        assert_eq!(
            send("unknown").await,
            Err(InterruptError::NoActive),
            "{terminal}: unknown"
        );

        let mut reads = conn.activate();
        assert_eq!(send("r1").await, Ok(()), "{terminal}: r1 while r2 runs");
        assert_eq!(send("r2").await, Ok(()), "{terminal}: active r2");
        read_text(&mut reads).await;
        conn.deactivate();
        for id in ["r1", "r2"] {
            assert_eq!(send(id).await, Ok(()), "{terminal}: finished {id}");
        }
        assert_eq!(
            received(&up, 2).await,
            [r#"{"type":"response.create"}"#.to_owned(), interrupt("r2")],
            "{terminal}: only the active response's interrupt reached upstream"
        );
        conn.send("bye".into()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), executor.ws.closed("conn-1"))
            .await
            .unwrap_or_else(|_| panic!("{terminal}: the disconnect was not reported"));
        assert_eq!(
            send("r2").await,
            Err(InterruptError::NoActive),
            "{terminal}: lost socket"
        );
    }
}
