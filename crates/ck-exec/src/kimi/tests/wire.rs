use super::*;

#[tokio::test]
async fn upstream_requests_match_go_byte_for_byte() {
    for name in [
        "chat-nonstream-normalize",
        "chat-stream-suffix",
        "chat-stream-gemini-client",
        "chat-stream-interactions-client",
        "chat-nonstream-gemini-client",
        "chat-error-429-clamped-none",
        "chat-disabled-thinking-temperature",
        "chat-kimi-ai-metadata-base",
        "responses-nonstream-reorder-suffix",
        "responses-stream-clamp",
        "responses-nonstream-nested-usage",
        "responses-stream-incomplete-then-done",
        "responses-stream-tier-merge",
        "chat-stream-usage-then-model",
        "chat-stream-read-error",
        "chat-stream-read-error-interactions-client",
        "chat-payload-rules",
        "responses-payload-rules",
        "chat-payload-override-after-normalize",
        "responses-payload-override-after-normalize",
        "responses-compact-rejected",
        "transport-custom-headers",
        "transport-redirect-307",
        "transport-gzip-response",
        "transport-explicit-gzip-not-decoded",
        "transport-json-typed-stream",
        "transport-stream-options-not-object",
    ] {
        if obsolete_upload(name) {
            continue;
        }
        let (fx, captured, _) = run(name).await;
        let go: Vec<Captured> = fx["upstream"]
            .as_array()
            .map(|u| u.iter().map(Captured::from_fixture).collect())
            .unwrap_or_default();
        assert_eq!(captured.len(), go.len(), "{name}: upstream request count");
        for (rust, go) in captured.iter().zip(&go) {
            assert_same_request(name, go, rust, &masked(&fx));
            assert_eq!(
                rust.header("X-Msh-Device-Name"),
                Some(hostname().unwrap_or_else(|| "unknown".into()).as_str())
            );
            assert_eq!(
                rust.header("X-Msh-Device-Model"),
                Some(format!("{} {}", go_os(), go_arch()).as_str())
            );
        }
    }
}

#[tokio::test]
async fn upstream_429_cools_the_model_without_a_retry_hint() {
    // kimi_executor.go returns statusErr{code, msg} for upstream errors: no retryAfter
    // (Retry-After is ignored) and not credential-scoped.
    let fx = fixture("kimi", "chat-error-429-clamped-none");
    let mock = Mock::start(&fx["responses"]).await;
    let cred = credential("kimi", &fx, Some(("base_url", format!("{}/coding", mock.url))));
    let error = executor()
        .execute(&claude(), &cred, request(&fx, ""), &cfg())
        .await
        .err()
        .unwrap();
    assert_eq!(error.status, 429);
    assert_eq!(error.scope, FailureScope::Model);
    assert_eq!(error.retry_after, None);
}

/// Replaces RFC 3339 UTC timestamps (`2026-10-03T07:27:50Z`) that translators stamp with
/// the current time.
fn mask_timestamps(text: &str) -> String {
    let b = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    let shape = b"dddd-dd-ddTdd:dd:ddZ";
    while i < b.len() {
        let fits = i + shape.len() <= b.len()
            && shape.iter().zip(&b[i..]).all(|(s, c)| match s {
                b'd' => c.is_ascii_digit(),
                s => s == c,
            });
        if fits {
            out.push_str("<time>");
            i += shape.len();
        } else {
            let ch = text[i..].chars().next().unwrap();
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    out
}

#[tokio::test]
async fn downstream_results_match_go() {
    for name in [
        "chat-nonstream-normalize",
        "chat-stream-suffix",
        "chat-stream-gemini-client",
        "chat-stream-interactions-client",
        "chat-nonstream-gemini-client",
        "chat-error-429-clamped-none",
        "chat-disabled-thinking-temperature",
        "chat-kimi-ai-metadata-base",
        "responses-nonstream-reorder-suffix",
        "responses-stream-clamp",
        "responses-nonstream-nested-usage",
        "responses-stream-incomplete-then-done",
        "responses-stream-tier-merge",
        "chat-stream-usage-then-model",
        "chat-stream-read-error",
        "chat-stream-read-error-interactions-client",
        "responses-stream-data-only-frames",
        "responses-compact-rejected",
        "transport-custom-headers",
        "transport-redirect-307",
        "transport-gzip-response",
        "transport-json-typed-stream",
        "transport-stream-options-not-object",
    ] {
        assert_downstream(name).await;
    }
}

async fn assert_downstream(name: &str) {
    let (fx, _, down) = run(name).await;
    let go = &fx["downstream"];
    // A plain Go error (status -1 in the fixture) is answered with 500 by the handler.
    if go["err_status"].as_i64() == Some(-1) {
        assert_eq!(down.err_status, Some(500), "{name}: error status");
        assert_eq!(down.err_body.as_deref(), go["err_body"].as_str(), "{name}: error body");
        return;
    }
    if let Some(status) = go["err_status"].as_u64() {
        let go_err = go["stream_err"]
            .as_str()
            .filter(|e| !e.is_empty())
            .or(go["err_body"].as_str());
        assert_eq!(down.err_status, Some(status as u16), "{name}: error status");
        assert_eq!(down.err_body.as_deref(), go_err, "{name}: error body");
        // A stream that fails after output: the frames Go's route wrote before the error.
        if let Some(frames) = go["frames"].as_array() {
            let frames: Vec<&str> = frames.iter().filter_map(|f| f.as_str()).collect();
            assert_eq!(down.chunks, frames, "{name}: frames before the error");
        }
        return;
    }
    if let Some(body) = go["body"].as_str() {
        assert_eq!(down.body.as_deref(), Some(body), "{name}: body");
        return;
    }
    let chunks: Vec<String> = go["chunks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c.as_str().unwrap().to_owned())
        .collect();
    if fx["request"]["source"] == "openai-response" {
        // Go emits each upstream line plus "\n" and its Responses route joins them into
        // frames (`frames`, recorded from Go's responsesSSEFramer before route repair).
        let frames: Vec<String> = go["frames"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c.as_str().unwrap().to_owned())
            .collect();
        assert_eq!(down.chunks, frames, "{name}: stream frames");
    } else {
        // Each non-empty translated chunk as the client's Go route writes it.
        let client = crate::kimi_fixture::format(fx["request"]["source"].as_str().unwrap());
        let go_frames: Vec<String> = chunks
            .iter()
            .filter_map(|c| ck_translate::stream::frame(client, c.as_bytes()))
            .map(|f| mask_timestamps(&String::from_utf8(f).unwrap()))
            .collect();
        let rust: Vec<String> = down.chunks.iter().map(|c| mask_timestamps(c)).collect();
        assert_eq!(rust, go_frames, "{name}: stream frames");
    }
    // A statusless stream error (a read error) follows the chunks.
    let go_err = go["stream_err"].as_str().filter(|e| !e.is_empty());
    assert_eq!(down.err_body.as_deref(), go_err, "{name}: stream error");
}

#[tokio::test]
async fn explicit_accept_encoding_returns_raw_gzip_like_go() {
    // Go decodes only gzip its transport asked for; a configured Accept-Encoding passes
    // the compressed bytes through unchanged.
    let (fx, _, down) = run("transport-explicit-gzip-not-decoded").await;
    assert_eq!(down.raw, crate::kimi_fixture::response_body(&fx, 0));
    assert_eq!(&down.raw[..2], &[0x1f, 0x8b]);
}

#[tokio::test]
async fn claude_delegation_keeps_kimi_owned_wire_parts() {
    // Kimi owns the base URL, model normalization, token and response-model restoration.
    // The rest of the Messages wire (headers, cache_control, stream flag) is the Claude
    // executor's; full parity is `claude_delegation_full_wire_parity` below.
    let (fx, captured, down) = run("claude-nonstream-delegated").await;
    let go = Captured::from_fixture(&fx["upstream"][0]);
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].target, go.target);
    assert_eq!(captured[0].header("Authorization"), go.header("Authorization"));
    assert_eq!(
        gj::get(&captured[0].body, "model").str(),
        gj::get(&go.body, "model").str()
    );
    assert_eq!(
        down.body.as_deref(),
        fx["downstream"]["body"].as_str(),
        "model restored to the client's"
    );
}

#[tokio::test]
async fn claude_delegation_full_wire_parity() {
    let (fx, captured, _) = run("claude-nonstream-delegated").await;
    assert_same_request(
        "claude-nonstream-delegated",
        &Captured::from_fixture(&fx["upstream"][0]),
        &captured[0],
        &["Host"],
    );
}

#[tokio::test]
async fn count_tokens_goes_upstream_like_go() {
    let (fx, captured, down) = run("claude-count-tokens-upstream").await;
    assert_same_request(
        "claude-count-tokens-upstream",
        &Captured::from_fixture(&fx["upstream"][0]),
        &captured[0],
        &["Host"],
    );
    assert_eq!(down.body.as_deref(), fx["downstream"]["body"].as_str());
}
