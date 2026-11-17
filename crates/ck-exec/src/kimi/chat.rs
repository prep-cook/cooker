use super::*;

fn chat_upload(credential: &Credential, req: &ExecRequest, cfg: &Config) -> Result<Vec<u8>, ExecError> {
    let base_model = parse_suffix(&req.model).model_name;
    if ck_translate::pair(req.source_format, Format::OpenAI).is_none() {
        return Err(not_registered("Kimi request"));
    }
    // Go: helps.TranslateRequestWithCodexMultiAgentV2 (no target executor, not compat).
    let codex_client = crate::codex_client::Client::new(&req.headers, cfg, "", false);
    let translate = |body: &[u8], stream: bool| {
        crate::codex_client::translate_request(
            req.source_format,
            Format::OpenAI,
            &RequestCtx {
                model: &base_model,
                stream,
                tool_search: false,
            },
            body,
            &codex_client,
        )
        .map_err(|e| request_error(e.to_string()))
    };
    // Go translates the original request first (stream false) for payload-rule
    // defaults, then the working one.
    let original_translated = translate(original(req), false);
    let translated = translate(&req.body, req.stream)?;
    let original_translated = crate::translate::baseline_or_working(original_translated, &translated);
    let upstream_model = upstream_model(credential, &base_model);
    let mut body = gj::try_set_str(&translated, "model", &upstream_model)
        .map_err(|e| internal_error(format!("kimi executor: failed to set model in payload: {e}")))?;
    body = thinking(&body, req, credential, "kimi")?;
    if req.stream {
        body = gj::try_set_raw(&body, "stream_options.include_usage", "true")
            .map_err(|e| internal_error(format!("kimi executor: failed to set stream_options in payload: {e}")))?;
    }
    body = normalize_tool_message_links(body)?;
    body = normalize_tools(body);
    req.usage.upstream_model(&upstream_model);
    req.usage.request_for("kimi", &body);
    // The payload rules are the last change to the body (868e137).
    Ok(payload_rules(
        cfg,
        req,
        &base_model,
        "openai",
        body,
        &original_translated,
    ))
}

pub(crate) async fn execute_chat(
    client: &wreq::Client,
    credential: &Credential,
    req: ExecRequest,
    cfg: &Config,
) -> Result<ExecResponse, ExecError> {
    let Some(response_pair) = ck_translate::pair(req.response_format, Format::OpenAI) else {
        return Err(not_registered("Kimi response"));
    };
    let body = chat_upload(credential, &req, cfg)?;
    if req.stream {
        // Go's stream publishes only through its usage buffer (no EnsurePublished).
        req.usage.usage_required();
    }
    let upstream = post(
        client,
        credential,
        &req.usage,
        &chat_url(credential),
        headers(credential, &req.headers, req.session.as_deref(), req.stream),
        body.clone(),
    )
    .await?;
    let translated = Bytes::from(body);
    let ctx = ResponseCtx {
        model: &req.model,
        original_request: original(&req),
        translated_request: &translated,
        tool_search: false,
    };
    let body = if req.stream {
        let (tapped, usage) = defer_usage(
            crate::kimi_http::capture_lines(lines(upstream.body, CHAT_LINE_LIMIT), req.capture()),
            &req.usage,
            UsageRule::OpenAIStream,
        );
        ResponseBody::Stream(translate_lines(
            tapped,
            (response_pair.stream)(&ctx),
            StreamEnd::Chat,
            usage,
        ))
    } else {
        let data = read_body(upstream.body, req.capture()).await?;
        // Go observes the response model now and publishes the usage once translated.
        report_model(&req.usage, Format::OpenAI, &data);
        // A translator error or empty output is Go's apply_patch 502.
        let out = (response_pair.non_stream)(&ctx, &data)
            .ok()
            .filter(|out| !out.is_empty())
            .ok_or_else(apply_patch_error)?;
        req.usage.response_body(Format::OpenAI, &data);
        ResponseBody::Buffered(Bytes::from(if req.response_format == Format::OpenAIResponse {
            ensure_responses_usage_details(&out)
        } else {
            out
        }))
    };
    Ok(ExecResponse {
        status: upstream.status,
        headers: upstream.headers,
        body,
    })
}

/// How the Go loop ends a translated stream.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamEnd {
    /// Chat Completions (executeStream): EndApplyPatchStream, then a synthetic `[DONE]`.
    Chat,
    /// Native Responses translated for another client (executeResponsesStream): neither.
    Responses,
}

/// The 502 Go returns when a translator rejects upstream apply_patch input.
pub(crate) fn apply_patch_error() -> ExecError {
    ExecError::local(502, FailureScope::Request, ck_translate::APPLY_PATCH_UPSTREAM_ERROR)
}

/// Go's per-line stream loop: each non-empty scanned line goes through the translator.
/// A translator that rejects apply_patch input ends the stream after that line's frames
/// with a 502 (StopApplyPatchStream). At the end Chat streams finalize tool input
/// (EndApplyPatchStream), then end with a scan error (published with the usage seen so
/// far) or translate a synthetic `[DONE]`. Before any terminal error the Responses route
/// flushes the frame it is still joining.
struct TranslationState {
    upstream: ExecStream,
    translator: Box<dyn StreamTranslator>,
    ready: VecDeque<Result<Bytes, ExecError>>,
    done: bool,
    end: StreamEnd,
    usage: DeferredUsage,
    failed: bool,
}
impl TranslationState {
    fn fail(&mut self, error: ExecError) {
        self.done = true;
        self.failed = true;
        let flushed = self.translator.flush_frames();
        self.ready.extend(flushed.into_iter().map(Ok));
        self.ready.push_back(Err(error));
    }

    /// One translated line; false when the stream has ended. Bridge frames end in a
    /// blank line; Go hands the translator the whole chunk, which it trims.
    fn translate(&mut self, line: &[u8]) -> bool {
        let line = line.strip_suffix(b"\n\n").unwrap_or(line);
        match self.translator.event(line) {
            Ok(events) => self.ready.extend(events.into_iter().map(Ok)),
            Err(error) => {
                self.fail(translate_error(error));
                return false;
            }
        }
        true
    }

    fn finish(&mut self, end: Option<Result<Bytes, ExecError>>) {
        self.done = true;
        if self.end == StreamEnd::Chat {
            let finalized = self.translator.finalize_tool_input();
            self.ready.extend(finalized.into_iter().map(Ok));
            if self.translator.tool_input_failed() {
                return self.fail(apply_patch_error());
            }
            // 4f2e5270: a scan error ends the stream before the synthetic [DONE], and
            // its failure keeps the usage seen so far.
            if let Some(Err(error)) = end {
                self.usage.publish_failure(&error);
                return self.fail(error);
            }
            if !self.translate(b"[DONE]") {
                return;
            }
        }
        match self.translator.finish() {
            Ok(events) => self.ready.extend(events.into_iter().map(Ok)),
            Err(error) => return self.fail(translate_error(error)),
        }
        if let Some(Err(error)) = end {
            return self.fail(error);
        }
        if !self.failed {
            self.usage.commit();
        }
    }
}

pub(crate) fn translate_lines(
    upstream: ExecStream,
    translator: Box<dyn StreamTranslator>,
    end: StreamEnd,
    usage: DeferredUsage,
) -> ExecStream {
    futures_util::stream::unfold(
        TranslationState {
            upstream,
            translator,
            ready: VecDeque::new(),
            done: false,
            end,
            usage,
            failed: false,
        },
        move |mut st| async move {
            loop {
                if let Some(item) = st.ready.pop_front() {
                    return Some((item, st));
                }
                if st.done {
                    return None;
                }
                match st.upstream.next().await {
                    Some(Ok(line)) if line.is_empty() => {}
                    Some(Ok(line)) => {
                        if st.translate(&line) && st.translator.tool_input_failed() {
                            st.fail(apply_patch_error());
                        }
                    }
                    end => st.finish(end),
                }
            }
        },
    )
    .boxed()
}

pub(crate) fn translate_error(error: ck_translate::Error) -> ExecError {
    ExecError::local(502, FailureScope::Request, error.to_string())
}

/// Native Responses streaming: Go writes every scanned line plus `\n` as one chunk and
/// the Responses route joins chunks into frames (ck_translate's ResponsesFramer),
/// flushing what is pending at the end and before a terminal error.
pub(crate) fn responses_frames(lines: ExecStream, usage: DeferredUsage) -> ExecStream {
    struct State {
        lines: ExecStream,
        joiner: ck_translate::stream::ResponsesFramer,
        ready: VecDeque<Result<Bytes, ExecError>>,
        done: bool,
        usage: DeferredUsage,
    }
    futures_util::stream::unfold(
        State {
            lines,
            joiner: Default::default(),
            ready: VecDeque::new(),
            done: false,
            usage,
        },
        |mut st| async move {
            loop {
                if let Some(item) = st.ready.pop_front() {
                    return Some((item, st));
                }
                if st.done {
                    return None;
                }
                match st.lines.next().await {
                    Some(Ok(line)) => {
                        let mut chunk = line.to_vec();
                        chunk.push(b'\n');
                        let frames = st.joiner.write(&chunk);
                        st.ready.extend(frames.into_iter().map(Ok));
                    }
                    end => {
                        st.done = true;
                        let frames = st.joiner.flush();
                        st.ready.extend(frames.into_iter().map(Ok));
                        match end {
                            Some(Err(error)) => st.ready.push_back(Err(error)),
                            _ => st.usage.commit(),
                        }
                    }
                }
            }
        },
    )
    .boxed()
}

pub(crate) fn native_usage_body(data: &[u8]) -> Option<Vec<u8>> {
    use crate::kimi_http::usage_has_tokens;
    if usage_has_tokens(&gj::get(data, "response.usage")) {
        let mut inner = gj::get(data, "response").raw().to_vec();
        let tier = crate::kimi_http::response_tier(data);
        if tier.is_empty() {
            gj::delete(&mut inner, "service_tier");
        } else {
            gj::set_str(&mut inner, "service_tier", tier);
        }
        return Some(inner);
    }
    usage_has_tokens(&gj::get(data, "usage")).then(|| data.to_vec())
}
