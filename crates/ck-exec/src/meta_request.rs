use super::*;
pub(super) struct Prepared {
    pub(super) body: Vec<u8>,
    /// Transforms for the client's response format against the Codex upstream.
    pub(super) response: &'static Pair,
    /// Go's `ApplyPatchResponsesState` for this request.
    pub(super) apply_patch: apply_patch_responses::State,
}

fn not_registered(what: &str) -> ExecError {
    ExecError::local(
        501,
        FailureScope::Request,
        format!("{what} translation pair is not registered"),
    )
}

/// Prepare Meta controls before the shared apply_patch bridge and payload rules.
pub(super) fn prepare(req: &ExecRequest, cfg: &Config, stream: bool) -> Result<Prepared, ExecError> {
    let Some(response) = ck_translate::pair(req.response_format, Format::Codex) else {
        return Err(not_registered("Meta response"));
    };
    let has_request_transformer = ck_translate::pair(req.source_format, Format::Codex).is_some();
    if !has_request_transformer {
        return Err(not_registered("Meta request"));
    }
    let base_model = parse_suffix(&req.model).model_name;
    let is_compat = req.resolved_model.as_ref().is_some_and(|r| r.is_compat());
    let settings = ck_wire::codex_client::Settings::from_config(cfg);
    let translate = |body: &[u8]| {
        let mut body = ck_wire::payload::normalize_codex_tool_integer_types(body, &req.headers);
        if req.source_format == Format::OpenAIResponse {
            body =
                ck_wire::codex_client::rewrite_orphan_delegation_input(&req.headers, &body, settings.orphan_delegation);
        }
        ck_translate::meta::translate_request(
            req.source_format,
            &RequestCtx {
                model: &base_model,
                stream,
                tool_search: false,
            },
            &body,
            is_compat,
        )
        .map_err(|e| ExecError::local(400, FailureScope::Request, e.0))
    };
    // Go translates the original request first, for payload-rule defaults, then the
    // working one, even when they are equal.
    let original_translated = translate(original(req));
    let mut body = translate(&req.body)?;
    let original_translated = crate::translate::baseline_or_working(original_translated, &body);
    // Go `cliproxyauth.ResolvedModelInfo`: capabilities bound to this attempt.
    let caps = req.resolved_model.as_ref().map(|r| ModelCaps::from(&r.info));
    let thinking = apply_request_thinking(
        &RequestThinking {
            payload: Some(&req.body[..]),
            original: Some(&original(req)[..]),
            model: &req.model,
            from: req.source_format.as_str(),
            to: "codex",
            provider: PROVIDER,
            resolved: caps.as_ref().map(Some),
            has_request_transformer,
            updates_changed: false,
        },
        body,
    )
    .map_err(|e| ExecError::local(e.status(), FailureScope::Request, e.message))?;
    body = thinking;
    set_string_if_different(&mut body, "model", &base_model);
    set_bool_if_different(&mut body, "stream", stream);
    for key in ["generate", "stream_options", "client_metadata"] {
        gj::delete(&mut body, key);
    }
    let apply_patch = apply_patch_responses::State::new(req.source_format, original(req), &original_translated);
    body = apply_patch_responses::normalize_executor_request(&body, Some(original(req)))
        .map_err(|e| ExecError::local(500, FailureScope::Request, e))?;
    normalize_codex_instructions(&mut body);
    body = sanitize_reasoning_encrypted_content(body);
    sanitize_web_search_tools(&mut body);
    body = ck_wire::payload::normalize_codex_tool_integer_types(&body, &req.headers);
    // The payload rules are the last change to the body (868e137).
    body = payload_rules(cfg, req, &base_model, PROVIDER, body, &original_translated);
    Ok(Prepared {
        body,
        response,
        apply_patch,
    })
}
/// `SanitizeMetaWebSearchTools`: Meta rejects `search_content_types` on `web_search` tools,
/// top level or inside a namespace.
pub(crate) fn sanitize_web_search_tools(body: &mut Vec<u8>) {
    let tools = gj::get(body, "tools");
    if !tools.is_array() {
        return;
    }
    let strip =
        |tool: &gj::Res<'_>| &*tool.get("type").bytes() == b"web_search" && tool.get("search_content_types").exists();
    let mut paths = Vec::new();
    for (i, tool) in tools.array().iter().enumerate() {
        if strip(tool) {
            paths.push(format!("tools.{i}.search_content_types"));
        }
        let nested = tool.get("tools");
        if &*tool.get("type").bytes() == b"namespace" && nested.is_array() {
            for (j, sub) in nested.array().iter().enumerate() {
                if strip(sub) {
                    paths.push(format!("tools.{i}.tools.{j}.search_content_types"));
                }
            }
        }
    }
    for path in paths {
        gj::delete(body, &path);
    }
}
