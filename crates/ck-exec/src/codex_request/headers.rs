//! Codex HTTP and WebSocket request headers.
use super::*;
fn set(headers: &mut HeaderMap, name: &str, value: &str) {
    if let (Ok(name), Ok(value)) = (HeaderName::try_from(name), HeaderValue::from_str(value)) {
        headers.insert(name, value);
    }
}

/// `misc.EnsureHeader`: the client's value wins, then what is already set.
fn ensure_from_client(headers: &mut HeaderMap, client: &HeaderMap, name: &str) {
    let value = header(client, name).trim();
    if !value.is_empty() {
        set(headers, name, value);
    }
}

/// `ensureHeaderWithConfigPrecedence`: existing, config, client, fallback.
fn ensure_config_first(headers: &mut HeaderMap, client: &HeaderMap, name: &str, config: &str, fallback: &str) {
    if !header(headers, name).trim().is_empty() {
        return;
    }
    for value in [config, header(client, name), fallback] {
        if !value.trim().is_empty() {
            set(headers, name, value.trim());
            return;
        }
    }
}

/// `util.ApplyCustomHeadersFromAttrs` (`ck_wire::headers`) with the view's explicit
/// `$CPA-SESSION-ID`.
pub(crate) fn custom_headers(view: &View<'_>, client: &HeaderMap) -> Vec<(String, String)> {
    ck_wire::headers::custom_headers(&view.credential.attributes, client, view.session.as_deref())
}

/// `applyModelHeaderOverrides`: models.json `config.override_header` for the model.
fn model_overrides(model: &str) -> Vec<(String, String)> {
    let Some(info) = ck_core::registry::pinned().lookup(model) else {
        return Vec::new();
    };
    info.raw
        .get("config")
        .and_then(|c| c.get("override_header"))
        .and_then(serde_json::Value::as_object)
        .map(|m| {
            m.iter()
                .filter(|(k, _)| !k.trim().is_empty())
                .filter_map(|(k, v)| Some((k.trim().to_owned(), v.as_str()?.to_owned())))
                .collect()
        })
        .unwrap_or_default()
}

/// HTTP upstream headers (`applyCodexHeadersFromSources`, routing hint, model overrides).
pub(crate) fn http_headers(
    view: &View<'_>,
    settings: &Settings,
    client: &HeaderMap,
    body: &str,
    model: &str,
    cache_id: Option<&str>,
    stream: bool,
) -> HeaderMap {
    let mut h = source_headers(view, settings, client, cache_id, stream);
    routing_hint(&mut h, view, client, body, model);
    model_header_overrides(&mut h, model, view.api_key);
    h
}

/// Images API upstream headers: `applyCodexHeaders` (or `applyCodexDirectImageHeaders`,
/// whose caller drops the client's User-Agent) and `applyModelHeaderOverrides`, without
/// the routing hint.
pub(crate) fn image_headers(
    view: &View<'_>,
    settings: &Settings,
    client: &HeaderMap,
    model: &str,
    cache_id: Option<&str>,
    stream: bool,
) -> HeaderMap {
    let mut h = source_headers(view, settings, client, cache_id, stream);
    model_header_overrides(&mut h, model, view.api_key);
    h
}

/// `cacheHelper`'s `Session-Id`, then `applyCodexHeadersFromSources`.
fn source_headers(
    view: &View<'_>,
    settings: &Settings,
    client: &HeaderMap,
    cache_id: Option<&str>,
    stream: bool,
) -> HeaderMap {
    if view.siwc {
        return siwc_http_headers(view, settings, client, cache_id, stream);
    }
    let mut h = HeaderMap::new();
    if let Some(id) = cache_id {
        set(&mut h, "session-id", id);
    }
    set(&mut h, "content-type", "application/json");
    if !view.token.trim().is_empty() {
        set(&mut h, "authorization", &format!("Bearer {}", view.token));
    }
    for name in [
        "x-codex-beta-features",
        "version",
        "x-codex-turn-metadata",
        "x-codex-turn-state",
        "x-client-request-id",
        "x-codex-window-id",
        "thread-id",
        "session-id",
        LITE_HEADER,
    ] {
        ensure_from_client(&mut h, client, name);
    }
    if view.api_key {
        set(&mut h, "user-agent", "cooker");
    } else {
        ensure_config_first(&mut h, client, "user-agent", &settings.default_user_agent, USER_AGENT);
    }
    set(
        &mut h,
        "accept",
        if stream {
            "text/event-stream"
        } else {
            "application/json"
        },
    );
    set(&mut h, "connection", "Keep-Alive");
    apply_identity(&mut h, view, settings, client);
    h
}

/// The Sign in with ChatGPT branch of `applyCodexHeadersFromSources`: the session as
/// `session_id` and the client request id, the SIWC User-Agent and operator headers; no
/// originator, account id, client Codex headers or cloaking.
fn siwc_http_headers(
    view: &View<'_>,
    settings: &Settings,
    client: &HeaderMap,
    cache_id: Option<&str>,
    stream: bool,
) -> HeaderMap {
    let mut h = HeaderMap::new();
    set(&mut h, "content-type", "application/json");
    if !view.token.trim().is_empty() {
        set(&mut h, "authorization", &format!("Bearer {}", view.token));
    }
    if let Some(id) = cache_id.map(str::trim).filter(|id| !id.is_empty()) {
        set(&mut h, "session_id", id);
        set(&mut h, "x-client-request-id", id);
    }
    set(&mut h, "user-agent", &settings.siwc_user_agent);
    set(
        &mut h,
        "accept",
        if stream {
            "text/event-stream"
        } else {
            "application/json"
        },
    );
    set(&mut h, "connection", "Keep-Alive");
    for (name, value) in custom_headers(view, client) {
        set(&mut h, &name, &value);
    }
    h
}

/// Originator, account header, operator headers and cloaking, shared by HTTP and WebSocket.
fn apply_identity(h: &mut HeaderMap, view: &View<'_>, settings: &Settings, client: &HeaderMap) {
    let originator = header(client, "originator").trim();
    if !originator.is_empty() {
        set(h, "originator", originator);
    } else if !view.api_key {
        set(h, "originator", ORIGINATOR);
    }
    if !view.api_key
        && let Some(account) = view.credential.str("account_id")
    {
        set(h, "chatgpt-account-id", account.trim());
    }
    for (name, value) in custom_headers(view, client) {
        set(h, &name, &value);
    }
    if !view.cloaking_disabled(settings) {
        set(h, "user-agent", USER_AGENT);
        set(h, "originator", ORIGINATOR);
    }
    if view.api_key {
        set(h, "user-agent", "cooker");
    }
}

/// `applyCodexRoutingHint`: `model=<slug>[;tier=<service_tier>]` for OAuth credentials.
fn routing_hint(h: &mut HeaderMap, view: &View<'_>, client: &HeaderMap, body: &str, model: &str) {
    if view.api_key || view.siwc {
        return;
    }
    h.remove(ROUTING_HINT);
    let operator = custom_headers(view, client)
        .into_iter()
        .find(|(n, _)| n.eq_ignore_ascii_case(ROUTING_HINT))
        .map(|(_, v)| v);
    if let Some(value) = operator.filter(|v| !v.trim().is_empty()) {
        set(h, ROUTING_HINT, value.trim());
        return;
    }
    let model = model.trim();
    if model.is_empty() {
        return;
    }
    let mut hint = format!("model={model}");
    let tier = gjson::get(body, "service_tier");
    if tier.kind() == Kind::String && !tier.str().trim().is_empty() {
        hint.push_str(";tier=");
        hint.push_str(tier.str().trim());
    }
    set(h, ROUTING_HINT, &hint);
}

fn model_header_overrides(h: &mut HeaderMap, model: &str, api_key: bool) {
    let overrides = model_overrides(model);
    if overrides.is_empty() {
        return;
    }
    for (k, v) in overrides {
        if !api_key || !k.eq_ignore_ascii_case("user-agent") {
            set(h, &k, &v);
        }
    }
    let has_session = ["session-id", "session_id"]
        .iter()
        .any(|n| !header(h, n).trim().is_empty());
    if header(h, "user-agent").contains("Mac OS") && !has_session {
        set(h, "session_id", &random_uuid());
    }
}

/// WebSocket handshake headers (`applyCodexPromptCacheHeadersWithContext`,
/// `applyCodexWebsocketHeaders`, routing hint, model overrides).
pub(crate) fn ws_headers(
    view: &View<'_>,
    settings: &Settings,
    client: &HeaderMap,
    body: &str,
    model: &str,
    cache_id: Option<&str>,
    native: bool,
) -> HeaderMap {
    let mut h = HeaderMap::new();
    if let Some(id) = cache_id {
        set(&mut h, "session_id", id);
        set(&mut h, "conversation_id", id);
    }
    if !view.token.trim().is_empty() {
        set(&mut h, "authorization", &format!("Bearer {}", view.token));
    }
    if view.siwc {
        siwc_ws_headers(&mut h, view, settings, client);
        model_header_overrides(&mut h, model, view.api_key);
        return h;
    }
    let (default_ua, default_beta) = if view.api_key {
        ("", "")
    } else {
        (
            settings.default_user_agent.as_str(),
            settings.default_beta_features.as_str(),
        )
    };
    // ensureHeaderWithPriority: existing, client, config.
    for value in [header(client, "x-codex-beta-features"), default_beta] {
        if !value.trim().is_empty() {
            set(&mut h, "x-codex-beta-features", value.trim());
            break;
        }
    }
    for name in [
        "x-codex-turn-state",
        "x-codex-turn-metadata",
        "x-client-request-id",
        "x-responsesapi-include-timing-metrics",
        "version",
    ] {
        ensure_from_client(&mut h, client, name);
    }
    if native {
        ensure_from_client(&mut h, client, LITE_HEADER);
    }
    if view.api_key {
        set(&mut h, "user-agent", "cooker");
    } else {
        ensure_config_first(&mut h, client, "user-agent", default_ua, USER_AGENT);
    }
    let mut beta = header(client, "openai-beta").trim().to_owned();
    if !beta.contains("responses_websockets=") {
        beta = WS_BETA.to_owned();
    }
    set(&mut h, "openai-beta", &beta);
    websocket_session(&mut h, view, settings, client, native);
    apply_identity(&mut h, view, settings, client);
    routing_hint(&mut h, view, client, body, model);
    model_header_overrides(&mut h, model, view.api_key);
    h
}

fn websocket_session(h: &mut HeaderMap, view: &View<'_>, settings: &Settings, client: &HeaderMap, native: bool) {
    // Existing, client, then a fresh id for Mac OS user agents.
    let mut session = header(h, "session_id").trim().to_owned();
    if session.is_empty() {
        session = ["session-id", "session_id"]
            .iter()
            .map(|n| header(client, n).trim())
            .find(|v| !v.is_empty())
            .unwrap_or_default()
            .to_owned();
    }
    if session.is_empty() && header(h, "user-agent").contains("Mac OS") {
        session = random_uuid();
    }
    if !session.is_empty() {
        set(h, "session_id", &session);
    }
    h.remove("session-id");
    if native && view.cloaking_disabled(settings) {
        h.remove("session_id");
        h.remove("conversation_id");
        for name in [
            "session-id",
            "session_id",
            "conversation_id",
            "thread-id",
            ROUTING_HINT,
            "x-codex-window-id",
        ] {
            for value in client.get_all(name) {
                h.append(HeaderName::from_static(name), value.clone());
            }
        }
    }
}

/// The Sign in with ChatGPT branch of `applyCodexWebsocketHeaders`: no conversation
/// header, the session (prompt cache or client) also as the client request id, the
/// SIWC User-Agent, the client's WebSocket beta header when it names one, then operator
/// headers.
fn siwc_ws_headers(h: &mut HeaderMap, view: &View<'_>, settings: &Settings, client: &HeaderMap) {
    h.remove("conversation_id");
    let existing = header(h, "session_id").trim().to_owned();
    let session = if existing.is_empty() {
        ["session-id", "session_id"]
            .iter()
            .map(|n| header(client, n).trim())
            .find(|v| !v.is_empty())
            .unwrap_or_default()
            .to_owned()
    } else {
        existing
    };
    if session.is_empty() {
        ensure_from_client(h, client, "x-client-request-id");
    } else {
        set(h, "session_id", &session);
        set(h, "x-client-request-id", &session);
    }
    set(h, "user-agent", &settings.siwc_user_agent);
    let beta = header(client, "openai-beta").trim();
    let beta = if beta.contains("responses_websockets=") {
        beta
    } else {
        WS_BETA
    };
    set(h, "openai-beta", beta);
    for (name, value) in custom_headers(view, client) {
        set(h, &name, &value);
    }
}
