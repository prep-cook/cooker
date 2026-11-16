use super::*;

pub(crate) fn refresh_token(credential: &Credential) -> Option<&str> {
    credential.str("refresh_token").filter(|s| !s.trim().is_empty())
}

/// `ResolveKimiBaseURL`: attribute, metadata, then the domain default.
pub(crate) fn base_url(credential: &Credential) -> String {
    let attr = credential
        .attributes
        .get("base_url")
        .map(|s| s.trim().trim_end_matches('/'))
        .unwrap_or_default();
    if !attr.is_empty() {
        return attr.to_owned();
    }
    let meta = credential.str("base_url").map(str::trim).unwrap_or_default();
    if !meta.is_empty() {
        return meta.trim_end_matches('/').to_owned();
    }
    kimi_auth::api_base(kimi_auth::resolve_domain(credential)).to_owned()
}

pub(crate) fn chat_url(credential: &Credential) -> String {
    let base = api_base(credential);
    if base.ends_with("/v1") {
        format!("{base}/chat/completions")
    } else {
        format!("{base}/v1/chat/completions")
    }
}

pub(crate) fn responses_url(credential: &Credential) -> String {
    let base = api_base(credential);
    if base.ends_with("/v1") {
        format!("{base}/responses")
    } else {
        format!("{base}/v1/responses")
    }
}

pub(crate) fn claude_base_url(credential: &Credential) -> String {
    let base = api_base(credential);
    let base = base.strip_suffix("/v1").unwrap_or(&base);
    if coding(credential) {
        base.to_owned()
    } else {
        format!("{base}/anthropic")
    }
}

fn api_base(credential: &Credential) -> String {
    let base = base_url(credential);
    let base = base.strip_suffix("/v1").unwrap_or(&base);
    base.strip_suffix("/anthropic").unwrap_or(base).to_owned()
}

/// `kimiCreds`: metadata access token, then attribute access token or API key.
pub(crate) fn access_token(credential: &Credential) -> String {
    if let Some(token) = credential.str("access_token").filter(|s| !s.trim().is_empty()) {
        return token.to_owned();
    }
    ["access_token", "api_key"]
        .iter()
        .find_map(|k| credential.attributes.get(*k).filter(|s| !s.is_empty()))
        .cloned()
        .unwrap_or_default()
}

/// The executor's device identity: credential `device_id`, else kimi-cli's stored ID.
pub(crate) fn device_id(credential: &Credential) -> String {
    if let Some(id) = credential.str("device_id").map(str::trim).filter(|s| !s.is_empty()) {
        return id.to_owned();
    }
    let Some(home) = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")) else {
        return "cli-proxy-api-device".into();
    };
    let home = std::path::PathBuf::from(home);
    let dir = match go_os() {
        "darwin" => home.join("Library/Application Support/kimi"),
        "windows" => std::env::var_os("APPDATA")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| home.join("AppData/Roaming"))
            .join("kimi"),
        _ => home.join(".local/share/kimi"),
    };
    std::fs::read_to_string(dir.join("device_id"))
        .map(|s| s.trim().to_owned())
        .unwrap_or_else(|_| "cli-proxy-api-device".into())
}

/// `applyKimiHeadersWithAuth` plus credential custom headers; `client` and `session` are the
/// client's request headers and canonical session.
pub(crate) fn headers(
    credential: &Credential,
    client: &http::HeaderMap,
    session: Option<&str>,
    stream: bool,
) -> GoHeaders {
    let mut h = GoHeaders::new();
    h.set("Content-Type", "application/json");
    h.set("Authorization", format!("Bearer {}", access_token(credential)));
    h.set("User-Agent", format!("CLIProxyAPI/{BUILD_VERSION}"));
    h.set("X-Msh-Platform", "CLIProxyAPI");
    h.set("X-Msh-Version", BUILD_VERSION);
    h.set("X-Msh-Device-Name", hostname().unwrap_or_else(|| "unknown".into()));
    h.set("X-Msh-Device-Model", format!("{} {}", go_os(), go_arch()));
    h.set("X-Msh-Device-Id", device_id(credential));
    h.set(
        "Accept",
        if stream {
            "text/event-stream"
        } else {
            "application/json"
        },
    );
    if let Some(nonce) = request_nonce(client) {
        h.set("X-Msh-Request-Nonce", nonce);
    }
    for (name, value) in client_headers(credential, client, session) {
        h.set(&name, value);
    }
    if ck_core::registry::dynamic::is_api_key(credential) {
        h.set("User-Agent", "cooker");
    }
    h
}

pub(crate) fn request_error(message: impl Into<String>) -> ExecError {
    ExecError::local(400, FailureScope::Request, message)
}

/// A plain Go error from the executor: the handler answers 500 with its text.
pub(crate) fn internal_error(message: impl Into<String>) -> ExecError {
    ExecError::local(500, FailureScope::Request, message)
}

pub(crate) fn not_registered(what: &str) -> ExecError {
    ExecError::local(
        501,
        FailureScope::Request,
        format!("{what} translation pair is not registered"),
    )
}

/// `opts.OriginalRequest`, falling back to the request payload when empty.
pub(crate) fn original(req: &ExecRequest) -> &Bytes {
    if req.original_body.is_empty() {
        &req.body
    } else {
        &req.original_body
    }
}

/// `helps.ApplyRequestThinking` for Kimi. `to` is Go's target format name: `kimi` for
/// Chat Completions (no translator targets it) and `codex` for native Responses.
pub(crate) fn thinking(
    body: &[u8],
    req: &ExecRequest,
    credential: &Credential,
    to: &str,
) -> Result<Vec<u8>, ExecError> {
    let has_request_transformer = Format::parse(to).is_some_and(|t| ck_translate::pair(req.source_format, t).is_some());
    // Go `cliproxyauth.ResolvedModelInfo`: capabilities bound to this attempt.
    let caps = req.resolved_model.as_ref().map(|r| ModelCaps::from(&r.info));
    apply_kimi_thinking(
        credential,
        &RequestThinking {
            payload: Some(&req.body[..]),
            original: Some(&original(req)[..]),
            model: &req.model,
            from: req.source_format.as_str(),
            to,
            provider: "kimi",
            resolved: caps.as_ref().map(Some),
            has_request_transformer,
            updates_changed: false,
        },
        body.to_vec(),
    )
}

/// Go's bufio.Scanner limits: 1 MiB for Chat Completions lines, 50 MiB for Responses.
pub(crate) const CHAT_LINE_LIMIT: usize = 1_048_576;
pub(crate) const RESPONSES_LINE_LIMIT: usize = 52_428_800;

/// One upstream send (kimi_executor.go's four `RecordAPIRequest` sites): the capture of
/// the request, its response metadata and a rejected response's body, then the send.
pub(crate) async fn post(
    client: &wreq::Client,
    credential: &Credential,
    usage: &UsageSink,
    url: &str,
    headers: GoHeaders,
    body: Vec<u8>,
) -> Result<Upstream, ExecError> {
    use crate::kimi_http::{capture_chunk, capture_error, capture_metadata, capture_request};
    let capture = usage.capture();
    let (auth_type, auth_value) = ck_core::registry::dynamic::account_info(credential);
    capture_request(
        capture,
        credential,
        url,
        &headers,
        &body,
        "kimi",
        (auth_type, &auth_value),
    );
    // Go's TrackHTTPClient: the TTFT runs from the request to the first body byte.
    usage.round_trip_started();
    // Go reads every Kimi body with a scanner or io.ReadAll: one cut short fails with
    // `unexpected EOF`.
    let mut upstream = send_scanned(client, url, headers, Bytes::from(body), None)
        .await
        .inspect_err(|e| capture_error(capture, e))?;
    capture_metadata(capture, upstream.status, &upstream.headers);
    upstream.body = crate::kimi_http::track_first_byte(upstream.body, usage, false);
    if !(200..300).contains(&upstream.status) {
        let error = status_error(upstream).await;
        // ponytail: the error body is the MAX_ERROR_BODY prefix (proxy.rs ceiling); Go
        // logs the whole body.
        capture_chunk(capture, &error.body);
        return Err(error);
    }
    Ok(upstream)
}

/// Go's non-stream `io.ReadAll` with its capture: a read error, else the whole body.
pub(crate) async fn read_body(
    body: futures_util::stream::BoxStream<'static, Result<Bytes, ExecError>>,
    capture: &ck_core::exec::CaptureSink,
) -> Result<Bytes, ExecError> {
    let data = read_all(body, usize::MAX, false)
        .await
        .inspect_err(|e| crate::kimi_http::capture_error(capture, e))?;
    crate::kimi_http::capture_chunk(capture, &data);
    Ok(data)
}
