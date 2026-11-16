use super::*;

#[tokio::test]
async fn redirect_hop_carries_previous_url_as_referer() {
    let (_, captured, _) = run("transport-redirect-307").await;
    assert_eq!(captured.len(), 2);
    let host = captured[0].header("Host").unwrap();
    assert_eq!(captured[1].target, "/coding/v1/chat/completions-next");
    assert_eq!(
        captured[1].header("Referer"),
        Some(format!("http://{host}/coding/v1/chat/completions").as_str())
    );
    assert_eq!(captured[1].body, captured[0].body, "307 resends the body");
}

use crate::kimi_fixture::PatchProbe;

async fn probe(
    lines: Vec<Result<&'static str, ()>>,
    end: StreamEnd,
    fail_on_finalize: bool,
) -> (Vec<String>, Vec<String>) {
    let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let translator = Box::new(PatchProbe {
        failed: false,
        fail_on_finalize,
        log: log.clone(),
    });
    let upstream = futures_util::stream::iter(lines.into_iter().map(|l| match l {
        Ok(text) => Ok(Bytes::from_static(text.as_bytes())),
        Err(()) => Err(ExecError::local(502, FailureScope::Transport, "scan failed")),
    }))
    .boxed();
    let out: Vec<String> = translate_lines(upstream, translator, end, Default::default())
        .map(|item| match item {
            Ok(b) => String::from_utf8(b.to_vec()).unwrap(),
            Err(e) => format!("ERR {} {}", e.status, String::from_utf8_lossy(&e.body)),
        })
        .collect()
        .await;
    let log = log.lock().unwrap().clone();
    (out, log)
}

#[tokio::test]
async fn apply_patch_failures_end_streams_like_go() {
    let patch = format!("ERR 502 {}", ck_translate::APPLY_PATCH_UPSTREAM_ERROR);
    // StopApplyPatchStream: the rejected line's frames, the route's flush, then a 502;
    // nothing after (no [DONE], no finish).
    let (out, log) = probe(vec![Ok("a"), Ok("BAD_PATCH"), Ok("c")], StreamEnd::Chat, false).await;
    assert_eq!(out, ["frame a", "frame BAD_PATCH", "flushed", patch.as_str()]);
    assert_eq!(log, ["event a", "event BAD_PATCH"]);
    // EndApplyPatchStream: finalize before [DONE]; a failure there stops with a 502.
    let (out, log) = probe(vec![Ok("a")], StreamEnd::Chat, true).await;
    assert_eq!(out, ["frame a", "response.failed", "flushed", patch.as_str()]);
    assert_eq!(log, ["event a", "finalize"]);
    // Clean Chat end: finalize, a synthetic [DONE], then finish.
    let (out, log) = probe(vec![Ok("a")], StreamEnd::Chat, false).await;
    assert_eq!(out, ["frame a", "frame [DONE]", "finished"]);
    assert_eq!(log, ["event a", "finalize", "event [DONE]", "finish"]);
    // A scan error (4f2e5270): finalize, then flush and the error; no [DONE], no finish.
    let (out, log) = probe(vec![Ok("a"), Err(())], StreamEnd::Chat, false).await;
    assert_eq!(out, ["frame a", "flushed", "ERR 502 scan failed"]);
    assert_eq!(log, ["event a", "finalize"]);
    // Native Responses translated for another client: no finalize and no [DONE].
    let (out, log) = probe(vec![Ok("a")], StreamEnd::Responses, true).await;
    assert_eq!(out, ["frame a", "finished"]);
    assert_eq!(log, ["event a", "finish"]);
}

#[tokio::test]
async fn usage_reports_match_go_records() {
    for name in [
        "chat-nonstream-normalize",
        "chat-stream-suffix",
        "chat-stream-gemini-client",
        "chat-nonstream-gemini-client",
        "chat-disabled-thinking-temperature",
        "chat-kimi-ai-metadata-base",
        "chat-error-429-clamped-none",
        "responses-nonstream-reorder-suffix",
        "responses-stream-clamp",
        "responses-nonstream-nested-usage",
        "responses-stream-incomplete-then-done",
        "responses-stream-tier-merge",
        "chat-stream-usage-then-model",
        "chat-stream-read-error",
        "chat-stream-read-error-interactions-client",
        "responses-stream-data-only-frames",
        "chat-payload-rules",
        "responses-payload-rules",
        "transport-gzip-response",
    ] {
        let fx = fixture("kimi", name);
        let mock = Mock::start(&fx["responses"]).await;
        let cred = credential("kimi", &fx, Some(("base_url", format!("{}/coding", mock.url))));
        let cfg = Config::parse(fx["request"]["config"].as_str().unwrap_or_default()).unwrap();
        let log = std::sync::Arc::new(crate::kimi_fixture::UsageLog::default());
        let mut req = request(&fx, "");
        req.usage = log.sink();
        let result = KimiExecutor::with_client(default_client())
            .execute(&claude(), &cred, req, &cfg)
            .await;
        let down = downstream(result).await;
        crate::kimi_fixture::assert_usage_like_go(name, &fx, &log, down.failure.as_ref());
    }
}

#[tokio::test]
async fn request_capture_matches_go() {
    // kimi_executor.go's four RecordAPIRequest sites, their response metadata, chunks
    // and errors, rendered as Go's request log and compared with Go's text.
    let names = crate::kimi_fixture::captured_fixtures("kimi");
    assert!(names.len() >= 30, "{names:?}");
    for name in &names {
        let fx = fixture("kimi", name);
        if fx["request"]["source"] == "claude" || obsolete_upload(name) {
            // Claude clients go through the Claude executor, whose capture sites
            // (claude_executor_*.go) belong to the Claude owner.
            continue;
        }
        let mock = Mock::start(&fx["responses"]).await;
        let cred = credential("kimi", &fx, Some(("base_url", format!("{}/coding", mock.url))));
        let cfg = Config::parse(fx["request"]["config"].as_str().unwrap_or_default()).unwrap();
        let capture = std::sync::Arc::new(crate::kimi_fixture::CaptureLog::default());
        let mut req = request(&fx, "");
        req.usage = ck_core::exec::UsageSink::default().with_capture(capture.sink());
        let result = KimiExecutor::with_client(default_client())
            .execute(&claude(), &cred, req, &cfg)
            .await;
        let _ = downstream(result).await;
        crate::kimi_fixture::assert_capture_like_go(name, &fx, &capture, &mock.url, &masked(&fx));
    }
}
