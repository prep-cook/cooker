use super::*;
/// The ExecuteStream scan loop: SSE lines grouped into frames, Go's error detection on
/// every frame, and the terminal rules for `[DONE]` and EOF.
struct Frames {
    translator: Box<dyn StreamTranslator>,
    /// `TranslateStreamWithClaudeInputTokens` (Go f3e36f19): a Claude client's
    /// `message_start` gets the estimate of its request when the upstream reports none.
    claude: ClaudeInputTokens,
    responses: bool,
    claude_client: bool,
    glm: bool,
    event: String,
    data: Vec<Vec<u8>>,
    ready: VecDeque<Result<Bytes, ExecError>>,
    seen_done: bool,
    /// A data frame carried a finish_reason (Go 4f2e5270 `openAICompatChunkHasFinishReason`).
    saw_finish_reason: bool,
    failed: bool,
    capture: wire::Capture,
    usage: ck_core::exec::UsageSink,
}

impl Frames {
    /// `publishStreamError(err, false)`: the error is logged and published as a failure
    /// before the client gets it.
    fn fail(&mut self, status: u16, message: impl Into<String>) -> bool {
        let error = status_err(status, message);
        self.capture.error(&error);
        wire::publish_failure(&self.usage, &error);
        self.terminal(error);
        true
    }

    /// `publishStreamError(err, true)`: the upstream's error payload is neither logged nor
    /// published; a fixed text stands in for it.
    fn fail_payload(&mut self, status: u16, message: impl Into<String>) -> bool {
        let logged = status_err(status, "upstream stream returned an error payload");
        self.capture.error(&logged);
        wire::publish_failure(&self.usage, &logged);
        self.terminal(status_err(status, message));
        true
    }

    /// A terminal error: a Responses client first gets the frame its Go framer would
    /// flush (`responsesSSEFramer.Flush`), then the error.
    fn terminal(&mut self, error: ExecError) {
        let mut flushed = self.translator.flush_frames();
        self.usage_details(&mut flushed);
        self.ready.extend(flushed.into_iter().map(Ok));
        self.ready.push_back(Err(error));
        self.failed = true;
    }

    /// TranslateStreamWithClaudeInputTokens: Responses usage gets its detail fields
    /// unless the apply_patch bridge failed. Go patches before the SSE framer, so frames
    /// the framer flushes later get them too.
    fn usage_details(&self, out: &mut [Bytes]) {
        if self.responses && !self.translator.tool_input_failed() {
            for chunk in out {
                *chunk = Bytes::from(payload::ensure_responses_usage_details(chunk));
            }
        }
    }

    /// One Go `TranslateStream` call per frame's joined `data:` line; no blank line
    /// follows it.
    fn translate(&mut self, line: &[u8]) -> bool {
        match self.translator.event(line) {
            Ok(mut out) => {
                self.usage_details(&mut out);
                self.claude.apply(&mut out);
                self.ready.extend(out.into_iter().map(Ok));
                false
            }
            Err(e) => self.fail(502, e.to_string()),
        }
    }

    /// `processFrame`; true ends the scan.
    fn frame(&mut self) -> bool {
        let event = std::mem::take(&mut self.event);
        let data = std::mem::take(&mut self.data);
        if data.is_empty() {
            return payload::error_event(&event) && self.fail(502, "upstream error event ended without data");
        }
        if data.len() > 1 && data.iter().any(|d| trim(d) == b"[DONE]") {
            return self.fail(502, "upstream stream ended with incomplete data before [DONE]");
        }
        let joined = data.join(&b"\n"[..]);
        let payload_bytes = trim(&joined);
        let done = payload_bytes == b"[DONE]";
        if done && payload::error_event(&event) {
            return self.fail(502, "upstream error event ended before [DONE]");
        }
        if !done && !go::json_valid(payload_bytes) {
            return self.fail(502, "upstream stream ended with incomplete SSE data frame");
        }
        if !done && let Some(status) = payload::stream_data_error(payload_bytes, &event) {
            return self.fail_payload(status, String::from_utf8_lossy(payload_bytes));
        }
        if !done && !self.saw_finish_reason {
            self.saw_finish_reason = has_finish_reason(payload_bytes);
        }
        // ObserveChatTokenEvent (Go f8e4a840): the served model comes from frames that
        // passed these checks, not from every line.
        self.usage.response_model_line(payload_bytes);
        let mut line = b"data: ".to_vec();
        line.extend_from_slice(payload_bytes);
        if self.translate(&line) {
            return true;
        }
        if self.glm && network_error(payload_bytes) {
            let error = ExecError::local(502, FailureScope::Transport, "upstream generation interrupted");
            self.capture.error(&error);
            self.usage
                .publish_failure_with_usage(error.status, "upstream generation interrupted");
            self.terminal(error);
            return true;
        }
        // helps.ApplyPatchTranslationError: the frame's chunks go out, then the 502.
        if self.translator.tool_input_failed() {
            return self.fail(502, EMPTY_TRANSLATION);
        }
        if done {
            self.seen_done = true;
        }
        done
    }

    /// One scanned line; true ends the scan.
    fn line(&mut self, line: &[u8]) -> bool {
        let line = trim(line);
        if line.is_empty() {
            return self.frame();
        }
        if let Some(rest) = line.strip_prefix(b"data:") {
            self.data.push(trim(rest).to_vec());
        } else if let Some(rest) = line.strip_prefix(b"event:") {
            self.event = String::from_utf8_lossy(trim(rest)).into_owned();
        } else if line.starts_with(b":") || line.starts_with(b"id:") || line.starts_with(b"retry:") {
        } else if line.starts_with(b"{") || line.starts_with(b"[") {
            return self.fail_payload(502, String::from_utf8_lossy(line));
        }
        false
    }

    /// `helps.EndApplyPatchStream` when the transport ends: the translator's closing
    /// apply_patch frames, then the 502 when its tool input failed. True ends the stream.
    fn end_tool_input(&mut self) -> bool {
        let out = self.translator.finalize_tool_input();
        self.ready.extend(out.into_iter().map(Ok));
        if self.translator.tool_input_failed() {
            // RecordApplyPatchStreamFailure publishes; nothing is logged.
            let error = status_err(502, EMPTY_TRANSLATION);
            wire::publish_failure(&self.usage, &error);
            self.terminal(error);
            return true;
        }
        false
    }

    /// Clean EOF: flush a pending frame, then Go's terminal rule per client format.
    fn eof(&mut self) {
        if !self.seen_done && !self.failed && !self.data.is_empty() {
            self.frame();
        }
        if self.failed || self.end_tool_input() {
            return;
        }
        if !self.seen_done {
            // 0ea4e1dc: a finish reason that left every item done completes the stream.
            if self.responses && self.translator.can_finalize_response_stream() {
                if self.translate(b"data: [DONE]") {
                    return;
                }
                self.seen_done = !self.translator.can_finalize_response_stream();
            }
        }
        if !self.seen_done {
            // 4f2e5270: a Claude client's EOF fails too unless a chunk carried a finish
            // reason; both failures publish the usage seen so far.
            if self.responses || (self.claude_client && !self.saw_finish_reason) {
                let error = status_err(502, "upstream stream closed before [DONE]");
                self.capture.error(&error);
                self.usage
                    .publish_failure_with_usage(error.status, &String::from_utf8_lossy(&error.body));
                self.terminal(error);
                return;
            }
            if self.translate(b"data: [DONE]") {
                return;
            }
        }
        self.finish();
    }

    fn finish(&mut self) {
        match self.translator.finish() {
            Ok(mut out) => {
                self.usage_details(&mut out);
                self.ready.extend(out.into_iter().map(Ok));
            }
            Err(e) => {
                self.fail(502, e.to_string());
            }
        }
    }
}

/// Go `openAICompatChunkHasFinishReason`: a choice whose `finish_reason` is not null and
/// not blank after `strings.TrimSpace`.
fn has_finish_reason(payload: &[u8]) -> bool {
    gj::get(payload, "choices").array().iter().any(|choice| {
        let reason = choice.get("finish_reason");
        reason.kind != gj::Kind::Null && !go::trim_space(&reason.bytes()).is_empty()
    })
}

pub(super) fn network_error(payload: &[u8]) -> bool {
    gj::get(payload, "choices")
        .array()
        .iter()
        .any(|choice| choice.get("finish_reason").str() == "network_error")
}

pub(super) fn frames(
    lines: ExecStream,
    translator: Box<dyn StreamTranslator>,
    claude: ClaudeInputTokens,
    client: Format,
    capture: wire::Capture,
    usage: ck_core::exec::UsageSink,
    glm: bool,
) -> ExecStream {
    let state = Frames {
        translator,
        claude,
        responses: client == Format::OpenAIResponse,
        claude_client: client == Format::Claude,
        glm,
        event: String::new(),
        data: Vec::new(),
        ready: VecDeque::new(),
        seen_done: false,
        saw_finish_reason: false,
        failed: false,
        capture,
        usage,
    };
    futures_util::stream::unfold((lines, state, false), |(mut lines, mut state, mut ended)| async move {
        loop {
            if let Some(item) = state.ready.pop_front() {
                if item.is_err() {
                    state.ready.clear();
                    ended = true;
                }
                return Some((item, (lines, state, ended)));
            }
            if ended {
                return None;
            }
            match lines.next().await {
                Some(Ok(line)) => {
                    // AppendAPIResponseChunk per scanned line.
                    state.capture.line(&line);
                    if state.line(&line) {
                        if !state.failed && !state.end_tool_input() {
                            state.finish();
                        }
                        ended = true;
                    }
                }
                Some(Err(error)) => {
                    // Go ends apply_patch input before reporting the scan error.
                    if !state.end_tool_input() {
                        state.capture.error(&error);
                        wire::publish_failure(&state.usage, &error);
                        state.terminal(error);
                    }
                    ended = true;
                }
                None => {
                    state.eof();
                    ended = true;
                }
            }
        }
    })
    .boxed()
}
