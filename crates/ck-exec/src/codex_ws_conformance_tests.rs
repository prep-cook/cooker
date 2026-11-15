use super::*;

const DONE: &str = r#"{"type":"response.done","response":{"id":"r1","status":"completed","output":[]}}"#;
const INCOMPLETE: &str = r#"{"type":"response.incomplete","response":{"id":"r1","status":"incomplete","output":[],"usage":{"output_tokens":0},"incomplete_details":{"reason":"max_output_tokens"}}}"#;

async fn terminal_frames(terminal: &'static str, steering: bool) -> Vec<Result<String, ExecError>> {
    let (_, url) = upstream(vec![Act::SendDrop(vec![CREATED, terminal])]).await;
    let executor = CodexExecutor::new().unwrap();
    let (tx, rx) = mpsc::channel(1);
    let cfg = if steering {
        executor.attach_steering(
            "conn-1",
            SteeringInput::new(Arc::new(tokio::sync::Mutex::new(rx)), |_| true),
        );
        Config::parse("upstream:\n  codex:\n    response-steering: true\n").unwrap()
    } else {
        Config::default()
    };
    let response = executor
        .execute_in_session(&credential(&url), request(BODY), &cfg, &session(false))
        .await;
    let frames = match response {
        Ok(response) => tokio::time::timeout(Duration::from_secs(5), collect(response))
            .await
            .unwrap(),
        Err(error) => vec![Err(error)],
    };
    drop(tx);
    executor.close_session("conn-1");
    frames
}

#[tokio::test]
async fn c11_ws_done_only_eof_is_not_completion() {
    let frames = terminal_frames(DONE, false).await;
    assert!(frames.iter().any(Result::is_err), "done-only EOF succeeded: {frames:?}");
    assert!(
        !frames
            .iter()
            .filter_map(|f| f.as_ref().ok())
            .any(|f| f.contains("response.completed"))
    );
}

#[tokio::test]
async fn c11_duplex_done_only_eof_is_not_completion() {
    let frames = terminal_frames(DONE, true).await;
    assert!(frames.iter().any(Result::is_err), "done-only EOF succeeded: {frames:?}");
    assert!(
        !frames
            .iter()
            .filter_map(|f| f.as_ref().ok())
            .any(|f| f.contains("response.completed")),
        "{frames:?}"
    );
}

#[tokio::test]
async fn c11_ws_completed_and_incomplete_keep_wire_status() {
    for steering in [false, true] {
        for (terminal, kind) in [(COMPLETED, "response.completed"), (INCOMPLETE, "response.incomplete")] {
            let frames = terminal_frames(terminal, steering).await;
            let event = frames
                .iter()
                .filter_map(|f| f.as_ref().ok())
                .find(|f| f.contains(kind))
                .expect("terminal event");
            if kind == "response.incomplete" {
                assert!(event.contains("\"status\":\"incomplete\""), "{event}");
                assert!(event.contains("max_output_tokens"), "{event}");
            }
            if !steering {
                assert!(frames.iter().all(Result::is_ok), "{frames:?}");
            }
        }
    }
}

async fn read_terminal(response: ExecResponse, kind: &str) {
    let ResponseBody::Stream(mut stream) = response.body else {
        panic!("stream required")
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(frame) = stream.next().await {
            let frame = frame.unwrap();
            if String::from_utf8_lossy(&frame).contains(kind) {
                return;
            }
        }
        panic!("missing {kind}");
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn c11_ws_replay_requires_completed_including_duplex() {
    const SIG: &str =
        "gAAAAAAAAAAABwcHBwcHBwcHBwcHBwcHBwkJCQkJCQkJCQkJCQkJCQkDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAw==";
    const FIRST: &str = r#"{"model":"m","max_tokens":64,"messages":[{"role":"user","content":"hi"}]}"#;
    const NEXT: &str = r#"{"model":"m","max_tokens":64,"messages":[{"role":"user","content":"hi"},{"role":"assistant","content":"hello"},{"role":"user","content":"more"}]}"#;
    for steering in [false, true] {
        for status in ["completed", "incomplete"] {
            let kind = format!("response.{status}");
            let event = serde_json::json!({"type":kind,"response":{"id":"r1","status":status,"output":[
                {"id":"rs_1","type":"reasoning","encrypted_content":SIG,"summary":[]},
                {"id":"msg_1","type":"message","role":"assistant","content":[{"type":"output_text","text":"hello"}]}
            ]}})
            .to_string();
            let (up, url) = upstream(vec![
                Act::SendFrames(vec![CREATED.into(), event]),
                Act::Send(vec![CREATED, COMPLETED]),
            ])
            .await;
            let executor = CodexExecutor::new().unwrap();
            let (tx, rx) = mpsc::channel(1);
            let cfg = if steering {
                executor.attach_steering(
                    "conn-1",
                    SteeringInput::new(Arc::new(tokio::sync::Mutex::new(rx)), |_| true),
                );
                Config::parse("upstream:\n  codex:\n    response-steering: true\n").unwrap()
            } else {
                Config::default()
            };
            for (body, terminal) in [(FIRST, kind.as_str()), (NEXT, "response.completed")] {
                let mut req = request(body);
                req.source_format = Format::Claude;
                let response = executor
                    .execute_in_session(&credential(&url), req, &cfg, &session(false))
                    .await
                    .unwrap();
                read_terminal(response, terminal).await;
            }
            let frames = up.frames.lock().unwrap();
            let sent: Value = serde_json::from_str(&frames[1]).unwrap();
            let replayed = sent["input"]
                .as_array()
                .unwrap()
                .iter()
                .any(|i| i["type"] == "reasoning");
            assert_eq!(replayed, status == "completed", "steering {steering}, {status}: {sent}");
            drop(frames);
            drop(tx);
            executor.close_session("conn-1");
        }
    }
}

#[tokio::test]
async fn c11_done_does_not_finish_interrupt_tracking() {
    let (up, url) = upstream(vec![Act::Send(vec![CREATED, DONE]), Act::Send(vec![COMPLETED])]).await;
    let executor = CodexExecutor::new().unwrap();
    let conn = pooled(&executor, &url).await;
    let mut reads = conn.activate();
    conn.send(r#"{"type":"response.create"}"#.into()).await.unwrap();
    read_text(&mut reads).await;
    read_text(&mut reads).await;
    executor
        .interrupt_session("conn-1", INTERRUPT_R1.as_bytes(), |_| true)
        .await
        .unwrap();
    assert_eq!(read_text(&mut reads).await, COMPLETED);
    assert_eq!(up.frames.lock().unwrap().last().unwrap(), INTERRUPT_R1);
    executor.close_session("conn-1");
}

#[test]
fn xai_keeps_its_shared_reader_terminal_policy() {
    let mut xai = credential("http://127.0.0.1");
    xai.provider = "xai".into();
    let target = Target::new(&xai, "ws://127.0.0.1".into(), &Config::default());
    assert_eq!(terminal_response_id(DONE, target.accepts_done).as_deref(), Some("r1"));
}
