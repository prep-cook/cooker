use super::*;

/// `SetBoolIfDifferent`.
pub(crate) fn set_bool_if_different(mut body: Vec<u8>, path: &str, value: bool) -> Vec<u8> {
    let kind = gj::get(&body, path).kind;
    if (value && kind == Kind::True) || (!value && kind == Kind::False) {
        return body;
    }
    gj::set_bool(&mut body, path, value);
    body
}

/// Native Responses path (executeResponses / executeResponsesStream).
pub(crate) async fn execute_responses(
    client: &wreq::Client,
    credential: &Credential,
    req: ExecRequest,
    cfg: &Config,
) -> Result<ExecResponse, ExecError> {
    let response_pair = ck_translate::pair(req.response_format, Format::OpenAIResponse);
    if response_pair.is_none() && req.response_format != Format::OpenAIResponse {
        return Err(not_registered("Kimi Responses"));
    }
    let body = responses_upload(credential, &req, cfg)?;
    // Go publishes native Responses usage only when it has tokens (stream buffer, or the
    // non-stream Codex/OpenAI usage check); there is no EnsurePublished.
    req.usage.usage_required();
    let upstream = post(
        client,
        credential,
        &req.usage,
        &responses_url(credential),
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
    let out = if req.stream {
        let (tapped, usage) = defer_usage(
            crate::kimi_http::capture_lines(lines(upstream.body, RESPONSES_LINE_LIMIT), req.capture()),
            &req.usage,
            UsageRule::KimiResponses,
        );
        match response_pair {
            Some(pair) => ResponseBody::Stream(translate_lines(
                tapped,
                (pair.stream)(&ctx),
                StreamEnd::Responses,
                usage,
            )),
            // Native Responses clients get every line back, joined into frames.
            None => ResponseBody::Stream(responses_frames(tapped, usage)),
        }
    } else {
        let data = read_body(upstream.body, req.capture()).await?;
        report_model(&req.usage, Format::Codex, &data);
        let out = match response_pair {
            Some(pair) => (pair.non_stream)(&ctx, &data)
                .ok()
                .filter(|out| !out.is_empty())
                .ok_or_else(apply_patch_error)?,
            None => data.to_vec(),
        };
        if let Some(usage) = native_usage_body(&data) {
            req.usage.response_body(Format::Codex, &usage);
        }
        ResponseBody::Buffered(Bytes::from(out))
    };
    Ok(ExecResponse {
        status: upstream.status,
        headers: upstream.headers,
        body: out,
    })
}

fn responses_upload(credential: &Credential, req: &ExecRequest, cfg: &Config) -> Result<Vec<u8>, ExecError> {
    if req.alt.as_deref() == Some("responses/compact") {
        return Err(if req.stream {
            ExecError::local(
                400,
                FailureScope::Request,
                "streaming not supported for /responses/compact",
            )
        } else {
            ExecError::local(501, FailureScope::Request, "/responses/compact not supported")
        });
    }
    let base_model = parse_suffix(&req.model).model_name;
    let upstream_model = upstream_model(credential, &base_model);
    let mut body = gj::try_set_str(&req.body, "model", &upstream_model)
        .map_err(|e| internal_error(format!("kimi executor: failed to set model in payload: {e}")))?;
    body = set_bool_if_different(body, "stream", req.stream);
    validate_custom_tools(&body)?;
    body = thinking(&body, req, credential, "codex")?;
    body = normalize_responses_input(body);
    body = normalize_tools(body);
    req.usage.upstream_model(&upstream_model);
    req.usage.request_for("kimi", &body);
    // The payload rules are the last change (868e137). Go passes req.Payload as the
    // original here, not OriginalRequest.
    Ok(payload_rules(cfg, req, &base_model, "openai-response", body, &req.body))
}
