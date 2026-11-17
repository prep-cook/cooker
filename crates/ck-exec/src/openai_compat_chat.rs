use super::*;

/// The legacy Chat transport, also used by Vertex's endpoints/openapi route.
pub(crate) async fn chat_with(
    client: &wreq::Client,
    credential: &Credential,
    req: ExecRequest,
    cfg: &Config,
    (base_url, api_key, api_auth): (&str, &str, bool),
    compat: Option<&payload::Compat>,
) -> Result<ExecResponse, ExecError> {
    let base_model = parse_suffix(&req.model).model_name;
    if base_url.is_empty() {
        return Err(missing_base_url());
    }
    let compact = req.alt.as_deref() == Some(COMPACT_ALT);
    let (target, path) = if compact && !req.stream {
        (Format::OpenAIResponse, "/responses/compact")
    } else {
        (Format::OpenAI, "/chat/completions")
    };
    let pair = ck_translate::pair(req.response_format, target);
    if pair.is_none() && req.response_format != target {
        return Err(not_registered("response", req.response_format, target));
    }
    let body = prepare(&req, cfg, credential, target, &base_model, compat, compact)?;
    let mut headers = base_headers(api_key, "application/json", api_auth);
    apply_custom(&mut headers, credential, &req);
    if api_auth {
        headers.set("User-Agent", USER_AGENT);
    }
    if req.stream {
        headers.set("Accept", "text/event-stream");
        headers.set("Cache-Control", "no-cache");
    }
    let url = endpoint(base_url, path);
    crate::xai_url::new_request(&url)?;
    let capture = wire::Capture::request(
        &req,
        credential,
        &credential.provider,
        &url,
        "POST",
        headers.pairs(),
        &body,
    );
    let upstream = match crate::xai_url::transport_refusal(&url) {
        Some(error) => Err(error),
        None => wire::send(client, &url, headers, Bytes::from(body.clone())).await,
    };
    capture.sent(&upstream);
    let upstream = upstream?;
    if !(200..300).contains(&upstream.status) {
        return Err(upstream_error(upstream, &capture).await);
    }
    let glm = thinking::glm::is_model(&base_model);
    let ctx = ResponseCtx {
        model: &req.model,
        original_request: original_payload(&req),
        translated_request: &body,
        tool_search: false,
    };
    if req.stream {
        Ok(streamed(upstream, capture, &req, pair, &ctx, glm))
    } else {
        buffered(upstream, capture, &req, pair, &ctx, target, glm).await
    }
}

fn prepare(
    req: &ExecRequest,
    cfg: &Config,
    credential: &Credential,
    target: Format,
    model: &str,
    compat: Option<&payload::Compat>,
    compact: bool,
) -> Result<Vec<u8>, ExecError> {
    let caps = req.resolved_model.as_ref().map(|r| ModelCaps::from(&r.info));
    let is_compat = req.resolved_model.as_ref().is_some_and(|r| r.is_compat());
    let original = translate_body(req, cfg, target, model, req.stream, original_payload(req), is_compat);
    let mut body = translate_body(req, cfg, target, model, req.stream, &req.body, is_compat)?;
    let original = crate::translate::baseline_or_working(original, &body);
    body = apply_thinking(body, req, target, &credential.provider, caps.as_ref())?;
    let requested = route_model(req);
    if payload::excludes_images(compat, model, requested) {
        body = payload::normalize_tool_results_text_only(body);
    }
    if !compact {
        let mct = payload::uses_max_completion_tokens(compat, model, requested);
        body = payload::normalize_max_tokens(body, mct);
        body = prompt_cache_key(compat, &credential.provider, req, model, body);
    }
    if compact && !req.stream {
        gj::delete(&mut body, "stream");
        body = payload::sanitize_reasoning_encrypted_content(body);
    }
    if req.stream {
        payload::set_bool_if_different(&mut body, "stream_options.include_usage", true);
    }
    if req.usage.enabled() {
        req.usage.request(target, &body);
    }
    Ok(apply_payload_rules(cfg, req, model, target, &original, body))
}

fn streamed(
    upstream: wire::Upstream,
    capture: wire::Capture,
    req: &ExecRequest,
    pair: Option<&ck_translate::Pair>,
    ctx: &ResponseCtx<'_>,
    glm: bool,
) -> ExecResponse {
    let headers = upstream.headers.clone();
    let translator = match pair {
        Some(pair) => (pair.stream)(ctx),
        None => Box::new(Identity),
    };
    let mut lines = wire::lines(upstream, MAX_LINE);
    if req.usage.enabled() {
        let usage = req.usage.clone();
        lines = lines
            .inspect(move |line| {
                if let Ok(line) = line {
                    usage.response_usage_line(Format::OpenAI, line);
                }
            })
            .boxed();
    }
    let original = if req.original_body.is_empty() {
        req.body.clone()
    } else {
        req.original_body.clone()
    };
    let claude = ClaudeInputTokens::new(req.source_format, Format::OpenAI, req.response_format, original);
    ExecResponse {
        status: 200,
        headers,
        body: ResponseBody::Stream(frames(
            lines,
            translator,
            claude,
            req.response_format,
            capture,
            req.usage.clone(),
            glm,
        )),
    }
}

async fn buffered(
    upstream: wire::Upstream,
    capture: wire::Capture,
    req: &ExecRequest,
    pair: Option<&ck_translate::Pair>,
    ctx: &ResponseCtx<'_>,
    target: Format,
    glm: bool,
) -> Result<ExecResponse, ExecError> {
    let headers = upstream.headers.clone();
    let raw = wire::read_all(upstream).await;
    capture.read(&raw);
    let raw = raw?;
    if req.usage.enabled() {
        req.usage.response_body(target, &raw);
    }
    if glm && stream::network_error(&raw) {
        let error = ExecError::local(502, FailureScope::Transport, "upstream generation interrupted");
        capture.error(&error);
        req.usage
            .publish_failure_with_usage(error.status, "upstream generation interrupted");
        return Err(error);
    }
    let mut out = match pair {
        Some(pair) => {
            (pair.non_stream)(ctx, &raw).map_err(|_| ExecError::local(502, FailureScope::Request, EMPTY_TRANSLATION))?
        }
        None => raw.to_vec(),
    };
    if out.is_empty() {
        return Err(ExecError::local(502, FailureScope::Request, EMPTY_TRANSLATION));
    }
    if req.response_format == Format::OpenAIResponse {
        out = payload::ensure_responses_usage_details(&out);
    }
    Ok(ExecResponse {
        status: 200,
        headers,
        body: ResponseBody::Buffered(Bytes::from(out)),
    })
}
