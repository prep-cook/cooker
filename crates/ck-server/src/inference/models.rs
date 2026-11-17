//! Model listing routes: unified `GET /v1/models` and the Gemini catalog
//! (internal/api/server_routes.go, internal/registry/model_registry.go).

use std::sync::Arc;

use axum::extract::{OriginalUri, Path, State};
use axum::http::HeaderMap;
use axum::response::Response;
use ck_wire::gostr::GoStr;
use serde_json::{Map, Value, json};

use crate::scheduling::registry::Spec;
use crate::{
    Runtime,
    http::{gojson, respond},
    inference::errors,
};

const CLAUDE_MAX_INPUT: i64 = 200_000;
const CLAUDE_MAX_OUTPUT: i64 = 64_000;

/// `GET /v1/models`: Grok Shell (a `grok-shell` User-Agent) gets its own catalog, Codex
/// clients (any `client_version` query key) the Codex client catalog, Anthropic clients
/// (an `Anthropic-Version` header or a `claude-cli` User-Agent) the Anthropic catalog, and
/// everyone else the OpenAI one.
pub async fn unified(State(rt): State<Arc<Runtime>>, uri: OriginalUri, headers: HeaderMap) -> Response {
    list(rt, uri, headers, None).await
}

async fn list(rt: Arc<Runtime>, OriginalUri(uri): OriginalUri, headers: HeaderMap, detail: Option<&str>) -> Response {
    let header = |name: &str| {
        headers
            .get(name)
            .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
    };
    let anthropic = header("anthropic-version").is_some_and(|v| !v.is_empty())
        || header("user-agent").is_some_and(|ua| ua.starts_with("claude-cli"));
    let disable_cloaking = || {
        rt.config()
            .document
            .get("upstream")
            .and_then(|u| u.get("claude"))
            .and_then(|c| c.get("disable-cloaking-model-list"))
            .and_then(serde_yaml_ng::Value::as_bool)
            .unwrap_or(false)
    };
    if header("user-agent").is_some_and(|ua| ua.go_lower().contains("grok-shell")) {
        let registry = rt.registry();
        let list = grok_list(registry.available_with(|c, m| rt.suspension(c, m)));
        return answer(respond::gin_json(200, list), detail).await;
    }
    if let Some(version) = query_value(&uri, "client_version") {
        return answer(crate::inference::codex_models::response(&rt, &version), detail).await;
    }
    if anthropic {
        // Detail lookups see the full catalog; a cursor would hide the requested model.
        let page = match detail.is_none().then(|| ClaudePage::parse(&uri)).transpose() {
            Ok(page) => page,
            Err(message) => return respond::gin_json(400, errors::claude_body(400, &message)),
        };
        let registry = rt.registry();
        // Model routers (cooker only) list after the registry's models.
        let routers = crate::inference::model_router::listed(&rt.config());
        let models = registry.available_with(|c, m| rt.suspension(c, m)).chain(&routers);
        match claude_list(models, disable_cloaking(), page.as_ref()) {
            Ok(body) => answer(respond::gin_json(200, body), detail).await,
            Err(message) => respond::gin_json(400, errors::claude_body(400, &message)),
        }
    } else {
        let registry = rt.registry();
        // Model routers (cooker only) list after the registry's models.
        let routers = crate::inference::model_router::listed(&rt.config());
        let models = registry.available_with(|c, m| rt.suspension(c, m)).chain(&routers);
        answer(respond::gin_json(200, openai_list(models)), detail).await
    }
}

/// The Anthropic list's cursor/limit query parameters, parsed and validated at the
/// route boundary.
#[derive(Debug)]
struct ClaudePage {
    before_id: Option<String>,
    after_id: Option<String>,
    limit: usize,
}

impl ClaudePage {
    /// Parses `before_id`, `after_id` and `limit` (default 20, 1..=1000); `Err` is the
    /// message of the `invalid_request_error` to answer, as the official API does for an
    /// out-of-range limit or a cursor the catalog does not contain.
    fn parse(uri: &axum::http::Uri) -> Result<Self, String> {
        let before = query_value(uri, "before_id");
        let after = query_value(uri, "after_id");
        let limit = match query_value(uri, "limit") {
            None => 20,
            Some(raw) => match raw.parse::<usize>() {
                Ok(limit) => limit,
                Err(_) => {
                    return Err(format!(
                        "limit: Input should be a valid integer, e.g. `20`. Input was '{raw}'."
                    ));
                }
            },
        };
        if !(1..=1000).contains(&limit) {
            return Err(format!(
                "limit: Input should be between 1 and 1000, e.g. `20`. Input was '{limit}'."
            ));
        }
        Ok(Self {
            before_id: before,
            after_id: after,
            limit,
        })
    }
}

/// Go `WriteModelListResponse`: with `detail`, the answer is that model's entry from the
/// final list.
async fn answer(response: Response, detail: Option<&str>) -> Response {
    let Some(id) = detail else {
        return response;
    };
    if response.status() != axum::http::StatusCode::OK {
        return response;
    }
    let (mut parts, body) = response.into_parts();
    let Ok(body) = axum::body::to_bytes(body, usize::MAX).await else {
        return Response::from_parts(parts, axum::body::Body::empty());
    };
    let (status, body) = model_detail(body, id);
    parts.headers.remove(axum::http::header::CONTENT_LENGTH);
    parts.status = axum::http::StatusCode::from_u16(status).unwrap_or_default();
    Response::from_parts(parts, axum::body::Body::from(body))
}

/// The status and body Go writes for a model detail lookup in a final model list.
fn model_detail(body: bytes::Bytes, id: &str) -> (u16, bytes::Bytes) {
    match select_detail(&body, id) {
        Ok(Some(entry)) => (200, body.slice_ref(entry)),
        Ok(None) => (
            404,
            bytes::Bytes::from_static(
                br#"{"error":{"code":"model_not_found","message":"Model not found","type":"invalid_request_error"}}"#,
            ),
        ),
        Err(()) => (
            502,
            bytes::Bytes::from_static(br#"{"error":{"message":"Invalid model catalog","type":"api_error"}}"#),
        ),
    }
}

/// `GET /v1/models/*model`: the entry for `model` from the catalog `GET /v1/models` would
/// answer this client (Go `ModelDetailIDContextKey`).
pub async fn detail(
    State(rt): State<Arc<Runtime>>,
    model: Option<Path<String>>,
    uri: OriginalUri,
    headers: HeaderMap,
) -> Response {
    let id = model.map(|Path(model)| model).unwrap_or_default();
    list(rt, uri, headers, Some(&id)).await
}

/// Go `WriteModelListResponse` with a detail ID: the first entry of `data`, then
/// `models`, whose `id` (or `slug` when the ID is empty) equals `id`, as its raw bytes.
/// `Err` when the catalog does not decode as Go's `{data, models []json.RawMessage}`.
pub(crate) fn select_detail<'a>(body: &'a [u8], id: &str) -> Result<Option<&'a [u8]>, ()> {
    use ck_wire::json::{self, Kind};
    // encoding/json: a key matches a field case-insensitively, each match is decoded in
    // document order (so the last one wins), and one mismatched type fails the decode.
    fn fields<'a>(object: &'a [u8], names: [&str; 2], mut f: impl FnMut(usize, &'a [u8])) {
        json::object_members(object, |key, _, value| {
            if let Some(key) = json::go_unquote(&[b"\"", key, b"\""].concat()) {
                (0..2).filter(|&i| key.go_eq_fold(names[i])).for_each(|i| f(i, value));
            }
            true
        });
    }
    if !json::std_valid(body) {
        return Err(());
    }
    let top = json::parse(body);
    if top.kind == Kind::Null {
        return Ok(None);
    }
    if !top.is_object() {
        return Err(());
    }
    let (mut lists, mut mismatched) = ([None, None], false);
    fields(body, ["data", "models"], |i, value| match json::parse(value) {
        items if items.is_array() => lists[i] = Some(items),
        items if items.kind == Kind::Null => lists[i] = None,
        _ => mismatched = true,
    });
    if mismatched {
        return Err(());
    }
    for item in lists.iter().flatten().flat_map(|list| list.array()) {
        let std::borrow::Cow::Borrowed(raw) = item.raw else {
            continue;
        };
        if !item.is_object() {
            continue;
        }
        // A string sets the field and null leaves it alone; anything else fails the entry.
        let (mut found, mut mismatched) = ([String::new(), String::new()], false);
        fields(raw, ["id", "slug"], |i, value| match json::go_unquote(value) {
            Some(text) => found[i] = text,
            None => mismatched |= value != b"null",
        });
        let [entry_id, slug] = found;
        let entry_id = if entry_id.is_empty() { slug } else { entry_id };
        if !mismatched && !entry_id.is_empty() && entry_id == id {
            return Ok(Some(raw));
        }
    }
    Ok(None)
}

/// Go `c.Request.URL.Query()[key]` present, with `c.Query(key)`'s first value.
fn query_value(uri: &axum::http::Uri, key: &str) -> Option<String> {
    let axum::extract::Query(pairs) = axum::extract::Query::<Vec<(String, String)>>::try_from_uri(uri).ok()?;
    pairs.into_iter().find(|(k, _)| k == key).map(|(_, v)| v)
}

/// The positions of the cursor ids in the served catalog. `Err` names the first cursor
/// the catalog does not contain, as an unknown cursor is an `invalid_request_error`.
fn cursor_positions(
    data: &[(String, String, Value)],
    before_id: &Option<String>,
    after_id: &Option<String>,
) -> Result<(Option<usize>, Option<usize>), String> {
    let pos = |id: Option<&String>| id.map(|id| data.iter().position(|(_, cid, _)| cid == id));
    let (before, after) = (pos(before_id.as_ref()), pos(after_id.as_ref()));
    let missing = |name: &str, id: &str| format!("{name}: Value not found for `{name}`: {id}. Input was '{id}'.");
    match (before, after) {
        (_, Some(None)) => Err(missing("after_id", after_id.as_deref().unwrap_or_default())),
        (Some(None), _) => Err(missing("before_id", before_id.as_deref().unwrap_or_default())),
        (before, after) => Ok((before.flatten(), after.flatten())),
    }
}

/// Go `OpenAIModels`: id, object, created and owned_by only.
pub fn openai_list<'a>(models: impl Iterator<Item = &'a Spec>) -> String {
    let data: Vec<Value> = models
        .map(|m| {
            let mut entry = Map::new();
            entry.insert("id".into(), m.id.clone().into());
            entry.insert("object".into(), "model".into());
            entry.insert("owned_by".into(), m.owned_by.clone().into());
            if m.created > 0 {
                entry.insert("created".into(), m.created.into());
            }
            Value::Object(entry)
        })
        .collect();
    gojson::sorted(&json!({ "object": "list", "data": data }))
}

/// Go `grokbuild.BuildResponse` over `GetAvailableModelInfos` (sorted by trimmed ID),
/// marshalled in struct field order.
pub fn grok_list<'a>(models: impl Iterator<Item = &'a Spec>) -> String {
    let mut models: Vec<&Spec> = models.collect();
    models.sort_by(|a, b| a.id.trim().cmp(b.id.trim()));
    let data: Vec<String> = models
        .into_iter()
        .map(|m| {
            let name = if m.display_name.is_empty() {
                &m.id
            } else {
                &m.display_name
            };
            let mut entry = gojson::Obj::new()
                .str("id", &m.id)
                .str("model", &m.id)
                .str("name", name);
            if m.context_length > 0 {
                entry = entry.raw("context_window", &m.context_length.to_string());
            }
            entry = entry.str("api_backend", "responses").raw("supported_in_api", "true");
            let efforts: Vec<String> = m
                .thinking
                .iter()
                .flat_map(|t| &t.levels)
                .map(|level| level.trim())
                .filter(|level| !level.is_empty())
                .map(|level| gojson::Obj::new().str("value", level).finish())
                .collect();
            if !efforts.is_empty() {
                entry = entry.raw("reasoning_efforts", &format!("[{}]", efforts.join(",")));
            }
            entry.finish()
        })
        .collect();
    gojson::Obj::new()
        .str("object", "list")
        .raw("data", &format!("[{}]", data.join(",")))
        .finish()
}

/// Go `convertModelToMap(model, "claude")` plus `claudemodels.BuildResponse`. With
/// `page`, the answer is that slice of the ordered catalog: `after_id` returns the page
/// after that entry, `before_id` the page ending just before it, and `has_more`
/// reports whether results remain in the requested direction. Every entry carries
/// `created_at` (epoch when unknown) as the documented field set requires.
fn claude_list<'a>(
    models: impl Iterator<Item = &'a Spec>,
    disable_cloaking: bool,
    page: Option<&ClaudePage>,
) -> Result<String, String> {
    let mut data: Vec<(String, String, Value)> = models
        .map(|m| {
            let id = if disable_cloaking {
                m.id.clone()
            } else {
                crate::inference::claude::ensure_dd(&m.id)
            };
            let display = if m.display_name.is_empty() {
                m.id.clone()
            } else {
                m.display_name.clone()
            };
            let mut entry = Map::new();
            entry.insert("id".into(), id.clone().into());
            entry.insert("object".into(), "model".into());
            entry.insert("owned_by".into(), m.owned_by.clone().into());
            entry.insert("created_at".into(), rfc3339(m.created.max(0)).into());
            entry.insert("type".into(), "model".into());
            entry.insert("display_name".into(), display.clone().into());
            let input = if m.context_length > 0 {
                m.context_length
            } else {
                CLAUDE_MAX_INPUT
            };
            let output = if m.max_completion_tokens > 0 {
                m.max_completion_tokens
            } else {
                CLAUDE_MAX_OUTPUT
            };
            entry.insert("max_input_tokens".into(), input.into());
            entry.insert("max_tokens".into(), output.into());
            (display, id, Value::Object(entry))
        })
        .collect();
    data.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    let (data, has_more) = match page {
        None => (data, false),
        Some(page) => {
            let (before, after) = cursor_positions(&data, &page.before_id, &page.after_id)?;
            let (start, end, more) = match (before, after) {
                // A page ending just before `before_id`, taken from its end backwards and, with
                // `after_id` too, starting no earlier than just after it.
                (Some(before), after) => {
                    let floor = after.map_or(0, |after| after + 1).min(before);
                    let start = before.saturating_sub(page.limit).max(floor);
                    (start, before, start > floor)
                }
                // A page starting just after `after_id`.
                (None, Some(after)) => {
                    let start = (after + 1).min(data.len());
                    let end = (start + page.limit).min(data.len());
                    (start, end, end < data.len())
                }
                // No cursor: the first page.
                (None, None) => {
                    let end = page.limit.min(data.len());
                    (0, end, end < data.len())
                }
            };
            (data[start..end].to_vec(), more)
        }
    };
    let first = data.first().map(|d| d.1.clone()).map_or(Value::Null, Value::String);
    let last = data.last().map(|d| d.1.clone()).map_or(Value::Null, Value::String);
    let data: Vec<Value> = data.into_iter().map(|d| d.2).collect();
    Ok(gojson::sorted(&json!({
        "data": data,
        "has_more": has_more,
        "first_id": first,
        "last_id": last
    })))
}

/// `time.Unix(s, 0).UTC().Format(time.RFC3339)`.
pub fn rfc3339(seconds: i64) -> String {
    let days = seconds.div_euclid(86_400);
    let secs = seconds.rem_euclid(86_400);
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        secs / 3600,
        secs / 60 % 60,
        secs % 60
    )
}

/// Go `convertModelToMap(model, "gemini")`.
fn gemini_entry(m: &Spec) -> Map<String, Value> {
    let mut entry = Map::new();
    entry.insert(
        "name".into(),
        if m.name.is_empty() {
            m.id.clone()
        } else {
            m.name.clone()
        }
        .into(),
    );
    let mut put = |k: &str, v: Value, keep: bool| {
        if keep {
            entry.insert(k.into(), v);
        }
    };
    put("version", m.version.clone().into(), !m.version.is_empty());
    put("displayName", m.display_name.clone().into(), !m.display_name.is_empty());
    put("description", m.description.clone().into(), !m.description.is_empty());
    put("inputTokenLimit", m.input_token_limit.into(), m.input_token_limit > 0);
    put(
        "outputTokenLimit",
        m.output_token_limit.into(),
        m.output_token_limit > 0,
    );
    put(
        "supportedGenerationMethods",
        m.supported_generation_methods.clone().into(),
        !m.supported_generation_methods.is_empty(),
    );
    put(
        "supportedInputModalities",
        m.supported_input_modalities.clone().into(),
        !m.supported_input_modalities.is_empty(),
    );
    put(
        "supportedOutputModalities",
        m.supported_output_modalities.clone().into(),
        !m.supported_output_modalities.is_empty(),
    );
    entry
}

/// `GET /v1beta/models` (Go `GeminiModels`).
pub async fn gemini_list(State(rt): State<Arc<Runtime>>) -> Response {
    let registry = rt.registry();
    let models: Vec<Value> = registry
        .available_with(|c, m| rt.suspension(c, m))
        .map(|m| {
            let mut entry = gemini_entry(m);
            if let Some(name) = entry.get("name").and_then(Value::as_str).map(str::to_owned)
                && !name.is_empty()
            {
                if !name.starts_with("models/") {
                    entry.insert("name".into(), format!("models/{name}").into());
                }
                for key in ["displayName", "description"] {
                    if entry.get(key).and_then(Value::as_str).is_none_or(str::is_empty) {
                        entry.insert(key.into(), name.clone().into());
                    }
                }
            }
            entry
                .entry("supportedGenerationMethods")
                .or_insert_with(|| json!(["generateContent"]));
            Value::Object(entry)
        })
        .collect();
    respond::gin_json(200, gojson::sorted(&json!({ "models": models })))
}

/// `GET /v1beta/models/*action` (Go `GeminiGetHandler`).
pub async fn gemini_get(State(rt): State<Arc<Runtime>>, Path(action): Path<String>) -> Response {
    let action = action.trim_start_matches('/');
    let registry = rt.registry();
    let found = registry
        .available_with(|c, m| rt.suspension(c, m))
        .map(gemini_entry)
        .find(|entry| {
            let name = entry.get("name").and_then(Value::as_str).unwrap_or_default();
            name == action || name == format!("models/{action}")
        });
    match found {
        Some(mut entry) => {
            if let Some(name) = entry.get("name").and_then(Value::as_str).map(str::to_owned)
                && !name.is_empty()
                && !name.starts_with("models/")
            {
                entry.insert("name".into(), format!("models/{name}").into());
            }
            respond::gin_json(200, gojson::sorted(&Value::Object(entry)))
        }
        None => respond::error_detail(404, "Not Found", "not_found"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_matches_go() {
        assert_eq!(rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(rfc3339(1_759_276_800), "2025-10-01T00:00:00Z");
        assert_eq!(rfc3339(951_782_400), "2000-02-29T00:00:00Z");
        assert_eq!(rfc3339(1_709_210_096), "2024-02-29T12:34:56Z");
    }

    #[test]
    fn list_shapes_follow_go_marshal() {
        let spec = Spec {
            id: "gpt-x".into(),
            owned_by: "openai".into(),
            created: 5,
            display_name: "B".into(),
            ..Spec::default()
        };
        let claude = Spec {
            id: "claude-a".into(),
            owned_by: "anthropic".into(),
            display_name: "A".into(),
            context_length: 10,
            ..Spec::default()
        };
        assert_eq!(
            openai_list([&spec].into_iter()),
            r#"{"data":[{"created":5,"id":"gpt-x","object":"model","owned_by":"openai"}],"object":"list"}"#
        );
        assert_eq!(
            claude_list([&spec, &claude].into_iter(), false, None),
            Ok(r#"{"data":[{"created_at":"1970-01-01T00:00:00Z","display_name":"A","id":"claude-a","max_input_tokens":10,"max_tokens":64000,"object":"model","owned_by":"anthropic","type":"model"},{"created_at":"1970-01-01T00:00:05Z","display_name":"B","id":"claude-fable-5-dd-x-tpg","max_input_tokens":200000,"max_tokens":64000,"object":"model","owned_by":"openai","type":"model"}],"first_id":"claude-a","has_more":false,"last_id":"claude-fable-5-dd-x-tpg"}"#.to_owned())
        );
    }

    /// `GET /v1/models` in the Anthropic dialect pages the ordered catalog with
    /// `after_id`/`before_id`/`limit`, reports `has_more` in the page direction, and
    /// rejects an unknown cursor or an out-of-range limit.
    #[test]
    fn anthropic_pages_follow_the_catalog_order() {
        let specs: Vec<Spec> = ["a", "b", "c"]
            .into_iter()
            .map(|n| Spec {
                id: format!("m-{n}"),
                display_name: n.to_ascii_uppercase(),
                ..Spec::default()
            })
            .collect();
        let ids = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
        for (query, want) in [
            // Default limit 20 covers the whole catalog.
            ("", Ok((ids(&["m-a", "m-b", "m-c"]), Some("m-a"), Some("m-c"), false))),
            (
                "limit=1000",
                Ok((ids(&["m-a", "m-b", "m-c"]), Some("m-a"), Some("m-c"), false)),
            ),
            (
                "limit=3",
                Ok((ids(&["m-a", "m-b", "m-c"]), Some("m-a"), Some("m-c"), false)),
            ),
            ("limit=2", Ok((ids(&["m-a", "m-b"]), Some("m-a"), Some("m-b"), true))),
            ("limit=1", Ok((ids(&["m-a"]), Some("m-a"), Some("m-a"), true))),
            // Forward pages start just after `after_id`.
            (
                "after_id=m-a&limit=1",
                Ok((ids(&["m-b"]), Some("m-b"), Some("m-b"), true)),
            ),
            ("after_id=m-c&limit=1", Ok((ids(&[]), None, None, false))),
            // Backward pages end just before `before_id`.
            (
                "before_id=m-c&limit=1",
                Ok((ids(&["m-b"]), Some("m-b"), Some("m-b"), true)),
            ),
            ("before_id=m-a&limit=1", Ok((ids(&[]), None, None, false))),
            ("before_id=m-a&limit=2", Ok((ids(&[]), None, None, false))),
            // Both cursors bound one page: only what lies between them, whatever the limit.
            (
                "after_id=m-a&before_id=m-c&limit=1",
                Ok((ids(&["m-b"]), Some("m-b"), Some("m-b"), false)),
            ),
            (
                "after_id=m-a&before_id=m-c&limit=10",
                Ok((ids(&["m-b"]), Some("m-b"), Some("m-b"), false)),
            ),
            ("after_id=m-c&before_id=m-a&limit=10", Ok((ids(&[]), None, None, false))),
            // An unknown cursor or limit is an invalid_request_error message.
            (
                "after_id=m-x",
                Err("after_id: Value not found for `after_id`: m-x. Input was 'm-x'."),
            ),
            (
                "before_id=m-x",
                Err("before_id: Value not found for `before_id`: m-x. Input was 'm-x'."),
            ),
            (
                "before_id=m-c&after_id=m-x&limit=2",
                Err("after_id: Value not found for `after_id`: m-x. Input was 'm-x'."),
            ),
            (
                "after_id=m-x&before_id=m-y",
                Err("after_id: Value not found for `after_id`: m-x. Input was 'm-x'."),
            ),
            (
                "limit=0",
                Err("limit: Input should be between 1 and 1000, e.g. `20`. Input was '0'."),
            ),
            (
                "limit=1001",
                Err("limit: Input should be between 1 and 1000, e.g. `20`. Input was '1001'."),
            ),
            (
                "limit=abc",
                Err("limit: Input should be a valid integer, e.g. `20`. Input was 'abc'."),
            ),
        ] {
            let uri: axum::http::Uri = format!("http://local/v1/models?{query}").parse().unwrap();
            let got = ClaudePage::parse(&uri).and_then(|page| {
                claude_list(specs.iter(), true, Some(&page)).map(|body| {
                    let v: Value = serde_json::from_str(&body).unwrap();
                    (
                        v["data"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|e| e["id"].as_str().unwrap().to_owned())
                            .collect::<Vec<_>>(),
                        v["first_id"].as_str().map(str::to_owned),
                        v["last_id"].as_str().map(str::to_owned),
                        v["has_more"].as_bool().unwrap(),
                    )
                })
            });
            let want =
                want.map(|(ids, first, last, more)| (ids, first.map(str::to_owned), last.map(str::to_owned), more));
            assert_eq!(got, want.map_err(str::to_owned), "{query}");
        }
    }

    /// A detail lookup answers the entry, a 404 for a missing model and a 502 for a
    /// catalog Go cannot decode; a failed list passes through untouched.
    #[tokio::test]
    async fn detail_answers_follow_the_final_catalog() {
        let respond = |status: u16, body: &'static str, detail: Option<&'static str>| async move {
            let response = Response::builder()
                .status(status)
                .header(axum::http::header::CONTENT_LENGTH, body.len())
                .body(axum::body::Body::from(body))
                .unwrap();
            let response = answer(response, detail).await;
            let status = response.status().as_u16();
            let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
            (status, String::from_utf8(body.to_vec()).unwrap())
        };
        let catalog = r#"{"data":[{"id":"a","n":1}]}"#;
        assert_eq!(
            respond(200, catalog, Some("a")).await,
            (200, r#"{"id":"a","n":1}"#.into())
        );
        let (status, body) = respond(200, catalog, Some("b")).await;
        assert_eq!((status, body.contains("model_not_found")), (404, true), "{body}");
        let (status, body) = respond(200, r#"{"data":"x"}"#, Some("a")).await;
        assert_eq!((status, body.contains("Invalid model catalog")), (502, true), "{body}");
        assert_eq!(respond(500, "down", Some("a")).await, (500, "down".into()));
        assert_eq!(respond(200, catalog, None).await, (200, catalog.into()));
    }

    /// Go `WriteModelListResponse`'s detail selection, recorded with encoding/json (Go
    /// 1.26): keys fold, the last match wins, a mistyped field fails its whole decode.
    #[test]
    fn detail_selection_decodes_the_catalog_like_go() {
        for (body, id, want) in [
            (
                r#"{"Data":[{"id":"a" , "n":1.50}]}"#,
                "a",
                Ok(Some(r#"{"id":"a" , "n":1.50}"#)),
            ),
            (r#"{"data":[{"id":"a"}],"DATA":null}"#, "a", Ok(None)),
            (r#"{"data":"x","models":[{"id":"a"}]}"#, "a", Err(())),
            (
                r#"{"data":[{"id":1},{"ID":"a","id":null}]}"#,
                "a",
                Ok(Some(r#"{"ID":"a","id":null}"#)),
            ),
            (
                r#"{"models":[{"id":"","slug":"s"}],"data":[{"slug":"s","id":"t"}]}"#,
                "s",
                Ok(Some(r#"{"id":"","slug":"s"}"#)),
            ),
            (r#"{"data":[{"id":"a","slug":7}]}"#, "a", Ok(None)),
            ("[]", "a", Err(())),
            ("null", "a", Ok(None)),
            (r#"{"data":[{"id":"\u0061"}]}"#, "a", Ok(Some(r#"{"id":"\u0061"}"#))),
            (r#"{"data":[{"id":"k"}]}"#, "\u{212a}", Ok(None)),
            (r#"{"dat\u0061":[{"id":"a"}]}"#, "a", Ok(Some(r#"{"id":"a"}"#))),
        ] {
            let got = select_detail(body.as_bytes(), id).map(|e| e.map(|e| std::str::from_utf8(e).unwrap()));
            assert_eq!(got, want, "{body}");
        }
    }
}
