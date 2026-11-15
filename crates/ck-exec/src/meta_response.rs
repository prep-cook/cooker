use super::*;
/// `metaAsCompletedEvent`: a whole JSON body that is a terminal event or a bare response.
fn as_completed_event(data: &[u8]) -> Option<Vec<u8>> {
    let trimmed = go_trim_space(data);
    if !gj::valid(trimmed) {
        return None;
    }
    let kind = gj::get(trimmed, "type").bytes();
    if &*kind == b"response.completed" || &*kind == b"response.incomplete" {
        return Some(trimmed.to_vec());
    }
    if &*gj::get(trimmed, "object").bytes() == b"response" || gj::get(trimmed, "output").exists() {
        return gj::try_set_raw(br#"{"type":"response.completed"}"#, "response", trimmed).ok();
    }
    None
}

fn data_payload(line: &[u8]) -> Option<&[u8]> {
    line.strip_prefix(b"data:").map(go_trim_space)
}

fn lossy(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

/// translateMetaCompleted: the first terminal event of a buffered SSE body, with output
/// items rebuilt from `response.output_item.done`, then a whole-body JSON fallback. Every
/// event goes through the apply_patch bridge first; a bridge failure is the 502.
pub(super) fn collect_completed(
    data: &[u8],
    apply_patch: &mut apply_patch_responses::State,
    translate: impl Fn(&[u8]) -> Result<Vec<u8>, ExecError>,
) -> Result<Vec<u8>, ExecError> {
    let bridge_error = || ExecError::local(502, FailureScope::Request, ck_translate::APPLY_PATCH_UPSTREAM_ERROR);
    let mut items = OutputItems::default();
    for line in data.split(|b| *b == b'\n') {
        let Some(event) = data_payload(line) else {
            continue;
        };
        if let Some(error) = stream_event_error(event) {
            return Err(error);
        }
        let (events, error) = apply_patch.transform(event);
        if error.is_some() {
            return Err(bridge_error());
        }
        for event in events {
            let kind = gj::get(&event, "type").bytes();
            match &*kind {
                b"response.output_item.done" => items.collect(&lossy(&event)),
                b"response.completed" | b"response.incomplete" => {
                    return translate(items.patch(lossy(&event)).as_bytes());
                }
                _ => {}
            }
        }
    }
    if let Some(completed) = as_completed_event(data) {
        let patched = items.patch(lossy(&completed));
        let bridged = apply_patch
            .bridge
            .transform_non_stream(patched.as_bytes())
            .map_err(|_| bridge_error())?;
        return translate(&bridged);
    }
    if apply_patch.finish().is_err() {
        return Err(bridge_error());
    }
    Err(ExecError::local(408, FailureScope::Credential, DISCONNECTED))
}

/// The ExecuteStream loop: every scanned line goes through the apply_patch bridge and then
/// the pair's translator (`data:` lines normalized to `data: <trimmed>`), terminal events get
/// their collected output, and an error event, a bridge failure or a scan error ends the
/// stream. At EOF the bridge's `finish_stream` runs before a scan error is reported. The
/// translator emits frames the way the client's Go route writes them; usage is committed
/// only when the stream ends cleanly.
///
/// Responses clients get EnsureResponsesUsageDetails per line before translation; Go
/// applies it to each translated chunk, and the Responses translator only adds
/// `response.model` to creation events, so the two edits commute.
pub(super) fn stream_events(
    upstream: ExecStream,
    translator: Box<dyn StreamTranslator>,
    claude: crate::gemini_stream::ClaudeInputTokens,
    responses_client: bool,
    bridge: apply_patch_responses::State,
    usage: DeferredUsage,
    capture: ck_core::exec::CaptureSink,
) -> ExecStream {
    futures_util::stream::unfold(
        State {
            upstream,
            translator,
            claude,
            bridge,
            usage,
            capture,
            items: OutputItems::default(),
            ready: VecDeque::new(),
            done: false,
            responses_client,
        },
        |mut st| async move {
            loop {
                if let Some(item) = st.ready.pop_front() {
                    return Some((item, st));
                }
                if st.done {
                    return None;
                }
                match st.upstream.next().await {
                    Some(Ok(line)) => st.line(&line),
                    Some(Err(error)) => st.end(Some(error)),
                    None => st.end(None),
                }
            }
        },
    )
    .boxed()
}
struct State {
    upstream: ExecStream,
    translator: Box<dyn StreamTranslator>,
    /// `TranslateStreamWithClaudeInputTokens`' estimate for a Claude client.
    claude: crate::gemini_stream::ClaudeInputTokens,
    bridge: apply_patch_responses::State,
    usage: DeferredUsage,
    capture: ck_core::exec::CaptureSink,
    items: OutputItems,
    ready: VecDeque<Result<Bytes, ExecError>>,
    done: bool,
    responses_client: bool,
}
fn bridge_error() -> ExecError {
    ExecError::local(502, FailureScope::Request, ck_translate::APPLY_PATCH_UPSTREAM_ERROR)
}
impl State {
    /// Translates bridge output lines; false when a translator error ended the stream.
    fn translate_all(&mut self, lines: Vec<Vec<u8>>) -> bool {
        for line in lines {
            // Bridge frames end in a blank line; Go hands the translator the whole
            // chunk, which it trims, and a Responses route's framer ends the frame at
            // that blank line.
            let complete = line.ends_with(b"\n\n");
            let line = line.strip_suffix(b"\n\n").unwrap_or(&line);
            let line = if self.responses_client {
                ensure_responses_usage_details(line)
            } else {
                line.to_vec()
            };
            match self.translator.event(&line) {
                Ok(mut frames) => {
                    if !self.translator.tool_input_failed() {
                        self.claude.apply(&mut frames);
                    }
                    if complete && self.responses_client {
                        frames.extend(self.translator.flush_frames());
                    }
                    self.ready.extend(frames.into_iter().filter(|f| !f.is_empty()).map(Ok));
                }
                Err(error) => {
                    self.done = true;
                    self.ready
                        .push_back(Err(ExecError::local(502, FailureScope::Request, error.0)));
                    return false;
                }
            }
        }
        true
    }

    /// emitTranslatedLine: the bridge, the translator, StopApplyPatchStream, then the
    /// bridge's own failure.
    fn emit(&mut self, line: Vec<u8>) {
        let (lines, bridge_failed) = self.bridge.stream(&line);
        if !self.translate_all(lines) {
            return;
        }
        // StopApplyPatchStream: the rejected call's frames, then a 502. Go's Meta loop
        // does not finalize tool input at EOF.
        if self.translator.tool_input_failed() || bridge_failed.is_some() {
            self.fail(bridge_error());
        }
    }

    /// Ends the stream with `error`. Go stops translating; the Responses route first
    /// flushes the frame it is still joining (other routes hold nothing back).
    fn fail(&mut self, error: ExecError) {
        self.done = true;
        let flushed = self.translator.flush_frames();
        self.ready.extend(flushed.into_iter().filter(|f| !f.is_empty()).map(Ok));
        self.ready.push_back(Err(error));
    }

    fn line(&mut self, line: &[u8]) {
        let Some(event) = data_payload(line) else {
            return self.emit(line.to_vec());
        };
        if let Some(error) = stream_event_error(event) {
            crate::kimi_http::capture_error(&self.capture, &error);
            return self.fail(error);
        }
        let kind = gj::get(event, "type").bytes().into_owned();
        let event = match kind.as_slice() {
            b"response.output_item.done" => {
                self.items.collect(&lossy(event));
                event.to_vec()
            }
            b"response.completed" | b"response.incomplete" => self.items.patch(lossy(event)).into_bytes(),
            _ => event.to_vec(),
        };
        let mut out = b"data: ".to_vec();
        out.extend_from_slice(&event);
        self.emit(out);
    }

    /// The end of the upstream lines: `FinishStream`, then the scan error or a clean end.
    fn end(&mut self, scan_error: Option<ExecError>) {
        self.done = true;
        let (events, bridge_failed) = self.bridge.finish_stream();
        if !self.translate_all(events) {
            return;
        }
        if bridge_failed.is_some() {
            return self.fail(bridge_error());
        }
        if let Some(error) = scan_error {
            return self.fail(error);
        }
        match self.translator.finish() {
            Ok(frames) => self.ready.extend(frames.into_iter().filter(|f| !f.is_empty()).map(Ok)),
            Err(error) => {
                self.ready
                    .push_back(Err(ExecError::local(502, FailureScope::Request, error.0)));
                return;
            }
        }
        self.usage.commit();
    }
}

pub(super) async fn finish(
    req: &ExecRequest,
    body: &[u8],
    upstream: crate::proxy::Upstream,
    response: &'static Pair,
    apply_patch: apply_patch_responses::State,
) -> Result<ExecResponse, ExecError> {
    use crate::kimi_http::{capture_chunk, capture_error};
    let capture = req.capture();
    let ctx = response_ctx(req, body);
    let responses_client = req.response_format == Format::OpenAIResponse;
    let out = if req.stream {
        // Go publishes stream usage only through its buffer (no EnsurePublished).
        req.usage.usage_required();
        // Go observes `data:` lines: their response model, and usage once completed.
        let lines = crate::kimi_http::capture_lines(lines(upstream.body, LINE_LIMIT), capture);
        let (tapped, usage) = defer_usage(lines, &req.usage, UsageRule::MetaResponses);
        ResponseBody::Stream(stream_events(
            tapped,
            (response.stream)(&ctx),
            crate::gemini_stream::ClaudeInputTokens::new(
                req.source_format,
                Format::Codex,
                req.response_format,
                original(req).clone(),
            ),
            responses_client,
            apply_patch,
            usage,
            capture.clone(),
        ))
    } else {
        let data = read_all(upstream.body, usize::MAX, false)
            .await
            .inspect_err(|e| capture_error(capture, e))?;
        capture_chunk(capture, &data);
        report_model(&req.usage, Format::Codex, &data);
        let source = std::cell::RefCell::new(Vec::new());
        let mut apply_patch = apply_patch;
        let completed = collect_completed(&data, &mut apply_patch, |event| {
            source.borrow_mut().clear();
            source.borrow_mut().extend_from_slice(event);
            // A translator error or empty output is Go's apply_patch 502.
            (response.non_stream)(&ctx, event)
                .ok()
                .filter(|out| !out.is_empty())
                .ok_or_else(|| ExecError::local(502, FailureScope::Request, ck_translate::APPLY_PATCH_UPSTREAM_ERROR))
        })?;
        // The terminal event Go reads model and Codex usage from, once translated.
        req.usage.response_line(Format::Codex, &source.into_inner());
        ResponseBody::Buffered(Bytes::from(if responses_client {
            ensure_responses_usage_details(&completed)
        } else {
            completed
        }))
    };
    Ok(ExecResponse {
        status: upstream.status,
        headers: upstream.headers,
        body: out,
    })
}
