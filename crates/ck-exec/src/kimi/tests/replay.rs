use super::*;

#[tokio::test]
async fn claude_replay_sequence_matches_go() {
    // Six Claude-format turns on one executor (zz_rsfix_kimi_test.go TestRSFixKimiReplay):
    // cache from JSON and from SSE, replay across K3 variants, clear after a 400, and no
    // replay into another model family. The replayed assistant turn is Kimi-owned; the rest
    // of each body belongs to the Claude executor.
    let fx = fixture("kimi", "claude-replay-sequence");
    let mock = Mock::start(&fx["responses"]).await;
    let cred = credential("kimi", &fx, Some(("base_url", format!("{}/coding", mock.url))));
    let exec = executor();
    let claude = claude();
    let steps = fx["request"]["steps"].as_array().unwrap();
    let go_down = fx["extra"]["downstream"].as_array().unwrap();
    for (i, step) in steps.iter().enumerate() {
        let model = step["model"].as_str().unwrap();
        let body = Bytes::from(
            step["body"]
                .as_str()
                .unwrap()
                .replacen("\"M\"", &format!("\"{model}\""), 1),
        );
        let mut headers = http::HeaderMap::new();
        headers.insert("x-claude-code-session-id", "sess-1".parse().unwrap());
        let req = ExecRequest {
            operation: Operation::Generate,
            source_format: Format::Claude,
            response_format: Format::Claude,
            requested_model: model.into(),
            model: model.into(),
            original_body: body.clone(),
            body,
            stream: step["stream"].as_bool().unwrap(),
            alt: None,
            session: None,
            execution_session: None,
            derived_session: None,
            resolved_model: None,
            usage: Default::default(),
            request_path: String::new(),
            headers,
            caller: ck_core::exec::Caller {
                principal: "client-key-1".into(),
                source: "authorization",
                provider: String::new(),
            },
        };
        let down = downstream(exec.execute(&claude, &cred, req, &cfg()).await).await;
        let go = &go_down[i];
        match go["err_status"].as_u64() {
            Some(status) => assert_eq!(down.err_status, Some(status as u16), "step {i}"),
            None if step["stream"].as_bool().unwrap() => {
                let go_chunks: Vec<String> = go["chunks"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|c| c.as_str().unwrap().to_owned())
                    .collect();
                assert_eq!(data_payloads(&down.chunks), data_payloads(&go_chunks), "step {i}");
            }
            None => assert_eq!(down.body.as_deref(), go["body"].as_str(), "step {i}"),
        }
    }
    let captured = mock.captured();
    let go_upstream = fx["upstream"].as_array().unwrap();
    assert_eq!(captured.len(), go_upstream.len());
    for (i, (rust, go)) in captured.iter().zip(go_upstream).enumerate() {
        let go = Captured::from_fixture(go);
        let content = |c: &Captured| gj::canonical(gj::get(&c.body, "messages.1.content").raw());
        assert_eq!(content(rust), content(&go), "step {i}: replayed assistant content");
        assert_eq!(
            gj::get(&rust.body, "model").str(),
            gj::get(&go.body, "model").str(),
            "step {i}: model"
        );
    }
}
