use super::*;

/// Product identity comes from the credential endpoint, never the requested model.
pub(crate) fn coding(credential: &Credential) -> bool {
    url::Url::parse(&base_url(credential))
        .ok()
        .is_some_and(|url| url.path().trim_end_matches('/').trim_end_matches("/v1") == "/coding")
}

pub(crate) fn upstream_model(credential: &Credential, model: &str) -> String {
    if coding(credential) {
        normalize_upstream_model(model)
    } else {
        model.to_owned()
    }
}

fn effort_model(model: &str) -> bool {
    matches!(
        normalize_upstream_model(model).as_str(),
        "k3" | "k3-256k" | "k3.1-pro" | "k3.1-pro-highspeed" | "kimi-for-coding"
    )
}

/// K3-family models take the OpenAI `reasoning_effort` dialect; the shared clamp then snaps
/// the level to the model's list (`ck-wire` thinking, Settled 59).
pub(crate) fn apply_kimi_thinking(
    credential: &Credential,
    request: &RequestThinking<'_>,
    body: Vec<u8>,
) -> Result<Vec<u8>, ExecError> {
    let suffix = parse_suffix(request.model);
    let supports_effort = effort_model(&suffix.model_name)
        && (coding(credential) || normalize_upstream_model(&suffix.model_name) != "kimi-for-coding");
    let to = if request.to == "kimi" && supports_effort {
        "openai"
    } else {
        request.to
    };
    apply_request_thinking(&RequestThinking { to, ..*request }, body)
        .map_err(|e| ExecError::local(e.status(), FailureScope::Request, e.message))
}

pub(crate) fn request_nonce(headers: &http::HeaderMap) -> Option<&str> {
    let mut values = headers.get_all("x-msh-request-nonce").iter();
    let value = values.next()?.to_str().ok()?;
    (!value.trim().is_empty() && values.next().is_none()).then_some(value)
}

/// Use the delegation's existing custom-header seam without changing Claude policy.
pub(crate) fn delegate_nonce(credential: &mut Credential, headers: &http::HeaderMap) {
    if request_nonce(headers).is_some()
        && !credential
            .attributes
            .keys()
            .any(|k| k.eq_ignore_ascii_case("header:X-Msh-Request-Nonce"))
    {
        credential
            .attributes
            .insert("header:X-Msh-Request-Nonce".into(), "$X-Msh-Request-Nonce".into());
    }
}

pub(crate) fn validate_custom_tools(body: &[u8]) -> Result<(), ExecError> {
    for tool in gj::get(body, "tools").array() {
        if tool.get("type").str() == "custom"
            && (tool.get("format.type").str() != "grammar"
                || tool.get("format.syntax").str() != "lark"
                || tool.get("format.definition").kind != Kind::String)
        {
            return Err(request_error(
                "Kimi custom tools require a grammar format with lark syntax and definition",
            ));
        }
    }
    Ok(())
}
