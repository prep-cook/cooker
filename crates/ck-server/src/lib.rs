//! HTTP surface: routes and client-key auth over an axum-independent [`runtime`].

pub mod http;
pub mod inference;
pub mod management;
pub mod net;
pub mod observability;
mod realtime;
pub mod runtime;
pub mod scheduling;
pub mod store;
#[cfg(any(test, feature = "testing"))]
#[doc(hidden)]
pub mod testing;

use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, OriginalUri, State};
use axum::http::{HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Router, middleware};

use http::{gojson, respond};
use inference::{claude, codex_alpha, gemini, images, models, openai, speech, videos};
use net::access;
use observability::request_logging;
use realtime::{aistudio_relay, websocket};
use scheduling::registry;

pub use runtime::state::Runtime;

/// Ceiling for non-Anthropic client routes.
pub const MAX_REQUEST_BYTES: usize = 64 * 1024 * 1024;

/// The API routes alone; unregistered paths get gin's bare NoRoute 404.
pub fn router(rt: Arc<Runtime>) -> Router {
    gin(api(rt))
}

/// The served app: the API routes, with `rest` (the management router, which owns gin's
/// NoRoute) answering every path the API does not register. axum
/// allows one fallback per merged router, so `rest` sits behind the API instead of being
/// merged into it.
pub fn app(rt: Arc<Runtime>, rest: Router) -> Router {
    gin(api(rt).fallback_service(rest.layer(middleware::from_fn(request_logging::response))))
}

/// Around the whole router: axum sets `Allow` outside per-route layers.
fn gin(api: Router) -> Router {
    Router::new()
        .fallback_service(api)
        .layer(middleware::from_fn(no_route_allow))
}

fn api(rt: Arc<Runtime>) -> Router {
    let auth = || middleware::from_fn_with_state(rt.clone(), access::require_client_key);
    let v1 = Router::new()
        .route("/v1/models", get(models::unified))
        // Gin's `/models/*model` also matches the bare trailing slash, with an empty ID.
        .route("/v1/models/", get(models::detail))
        .route("/v1/models/{*model}", get(models::detail))
        .route("/v1/chat/completions", post(openai::chat_completions))
        .route(
            "/v1/chat/deferred-completion/{request_id}",
            get(inference::xai_deferred::retrieve),
        )
        .route("/v1/completions", post(openai::completions))
        .route("/v1/images/generations", post(images::generations))
        .route("/v1/images/edits", post(images::edits))
        .route("/v1/images/{request_id}", get(images::retrieve))
        .route("/v1/audio/speech", post(speech::speech))
        .route("/v1/tts", post(speech::speech))
        .route("/v1/videos", post(videos::native_post))
        .route("/v1/videos/generations", post(videos::native_post))
        .route("/v1/videos/edits", post(videos::native_post))
        .route("/v1/videos/extensions", post(videos::native_post))
        .route("/v1/videos/{request_id}", get(videos::native_retrieve))
        .route("/openai/v1/videos", post(videos::create))
        .route("/openai/v1/videos/{video_id}/content", get(videos::content))
        .route("/openai/v1/videos/{video_id}", get(videos::retrieve))
        .route(
            "/v1/messages",
            post(claude::messages).layer(DefaultBodyLimit::max(http::errors::ANTHROPIC_MAX_REQUEST_BYTES)),
        )
        .route(
            "/v1/messages/count_tokens",
            post(claude::count_tokens).layer(DefaultBodyLimit::max(http::errors::ANTHROPIC_MAX_REQUEST_BYTES)),
        )
        // Resume of a committed response; the stream identity travels only in `X-Ck-Stream-Id`.
        .route("/v1/streams", get(inference::streams::resume))
        .route("/v1/responses", post(openai::responses))
        .route("/v1/responses/compact", post(openai::compact))
        .route("/v1/responses/input_tokens", post(openai::input_tokens))
        // Local state only: an id is never looked up upstream (`responses_state`).
        .route(
            "/v1/responses/{response_id}",
            get(inference::responses_state::retrieve).delete(inference::responses_state::delete),
        )
        .route(
            "/v1/responses/{response_id}/cancel",
            post(inference::responses_state::cancel),
        )
        .route("/backend-api/codex/responses", post(openai::responses))
        .route("/backend-api/codex/responses/compact", post(openai::compact))
        .route("/v1beta/models", get(models::gemini_list))
        .route("/v1beta/interactions", post(gemini::interactions))
        .route("/v1beta/models/{*action}", post(gemini::action).get(models::gemini_get))
        .merge(codex_alpha::routes())
        .merge(websocket::routes())
        // Only matched routes: gin's NoRoute runs no group middleware.
        .route_layer(auth());
    Router::new()
        .route("/healthz", get(healthz).head(healthz))
        .route("/", get(root))
        .route("/anthropic/callback", get(callback))
        .route("/codex/callback", get(callback))
        .route("/antigravity/callback", get(callback))
        .route("/callback", get(devin_callback))
        .route("/devin/callback", get(devin_callback))
        .merge(v1)
        .merge(realtime::api::routes(&rt))
        .merge(aistudio_relay::routes(&rt))
        // Matched API routes only: management answers its own HEADs.
        .route_layer(middleware::from_fn(head_only_healthz))
        .method_not_allowed_fallback(|| async { StatusCode::NOT_FOUND })
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .layer(middleware::from_fn(go_framing))
        .layer(middleware::from_fn(request_logging::response))
        .with_state(rt)
}

/// gin registers HEAD for `/healthz` only, so a HEAD never falls back to a GET handler;
/// it is NoRoute's bare 404.
async fn head_only_healthz(req: axum::extract::Request, next: middleware::Next) -> Response {
    if req.method() == Method::HEAD && req.uri().path() != "/healthz" {
        return StatusCode::NOT_FOUND.into_response();
    }
    next.run(req).await
}

/// gin runs without `HandleMethodNotAllowed`: an unregistered method on a known path
/// is NoRoute, whose handler aborts with a bare 404 (no 405, no `Allow`).
async fn no_route_allow(req: axum::extract::Request, next: middleware::Next) -> Response {
    let mut res = next.run(req).await;
    if res.status() == StatusCode::NOT_FOUND {
        res.headers_mut().remove(header::ALLOW);
    }
    res
}

/// Go net/http framing for handlers that set no Content-Length (gin's `c.JSON` and
/// `c.Data`): a body over 2048 bytes (`bufferBeforeChunkingSize`) goes out chunked, and
/// a HEAD response with no body carries neither Content-Length nor Transfer-Encoding.
async fn go_framing(req: axum::extract::Request, next: middleware::Next) -> Response {
    use axum::body::{Body, HttpBody};
    let head = req.method() == Method::HEAD;
    let res = next.run(req).await;
    if res.headers().contains_key(header::CONTENT_LENGTH) || res.headers().contains_key(header::TRANSFER_ENCODING) {
        return res;
    }
    // Streams have no exact size and are already chunked.
    let Some(len) = res.body().size_hint().exact() else {
        return res;
    };
    if len <= 2048 && !(head && len == 0) {
        return res;
    }
    let (parts, body) = res.into_parts();
    let Ok(bytes) = axum::body::to_bytes(body, usize::MAX).await else {
        return Response::from_parts(parts, Body::empty());
    };
    let chunks = futures_util::stream::iter((!bytes.is_empty()).then_some(Ok::<_, std::convert::Infallible>(bytes)));
    Response::from_parts(parts, Body::from_stream(chunks))
}

/// Installs the runtime's model registry as the translators' capability lookup
/// (`ck_core::registry::lookup_model`). Call once at startup.
pub fn install_registry(rt: &Arc<Runtime>) {
    ck_core::registry::install_overlay(Some(Arc::new(registry::Overlay(Arc::downgrade(rt)))));
}

async fn healthz(method: Method) -> Response {
    if method == Method::HEAD {
        return StatusCode::OK.into_response();
    }
    respond::gin_json(200, r#"{"status":"ok"}"#.into())
}

async fn root() -> Response {
    respond::gin_json(
        200,
        r#"{"endpoints":["POST /v1/chat/completions","POST /v1/completions","GET /v1/models"],"message":"CLI Proxy API Server"}"#.into(),
    )
}

const CALLBACK_HTML: &str = r#"<html><head><meta charset="utf-8"><title>Authentication successful</title><script>setTimeout(function(){window.close();},5000);</script></head><body><h1>Authentication successful!</h1><p>You can close this window.</p><p>This window will close automatically in 5 seconds.</p></body></html>"#;

fn html() -> Response {
    (
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/html; charset=utf-8"),
        )],
        CALLBACK_HTML,
    )
        .into_response()
}

/// `code`, `state` and `error` (else `error_description`) of an OAuth redirect. Go's
/// Devin handler trims each value before the fallback; the other handlers do not
/// (server_routes.go).
fn callback_query(query: &str, trim: bool) -> (String, String, String) {
    let get = |name| {
        let value = access::query_get(query, name)
            .map(|v| String::from_utf8_lossy(&v).into_owned())
            .unwrap_or_default();
        if trim { gojson::trim(&value).to_owned() } else { value }
    };
    let mut error = get("error");
    if error.is_empty() {
        error = get("error_description");
    }
    (get("code"), get("state"), error)
}

/// Provider OAuth redirects on the main port: the code is handed to a pending
/// management login if one exists; the browser always sees the success page.
async fn callback(State(rt): State<Arc<Runtime>>, OriginalUri(uri): OriginalUri) -> Response {
    let provider = match uri.path() {
        "/anthropic/callback" => "anthropic",
        "/codex/callback" => "codex",
        _ => "antigravity",
    };
    let (code, state, error) = callback_query(uri.query().unwrap_or_default(), false);
    if !state.is_empty() {
        let delivery = rt.deliver_oauth_callback(&runtime::state::OAuthCallback {
            provider,
            state,
            code,
            error,
        });
        // Go v8.0.35 `ae3ee3be`: only the Codex route reports the refusal.
        if provider == "codex" && delivery == runtime::state::OAuthDelivery::RequiresV8 {
            return respond::gin_json(
                409,
                r#"{"error":"SIWC callbacks require /v8/management/oauth/callback"}"#.into(),
            );
        }
    }
    html()
}

async fn devin_callback(State(rt): State<Arc<Runtime>>, OriginalUri(uri): OriginalUri) -> Response {
    let (code, state, error) = callback_query(uri.query().unwrap_or_default(), true);
    let no_store = |mut res: Response| {
        res.headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        res
    };
    if code.is_empty() && error.is_empty() {
        return no_store(respond::gin_json(
            400,
            r#"{"error":"code or error is required"}"#.into(),
        ));
    }
    let delivered = rt.deliver_oauth_callback(&runtime::state::OAuthCallback {
        provider: "devin",
        state,
        code,
        error,
    });
    if delivered != runtime::state::OAuthDelivery::Delivered {
        return no_store(respond::gin_json(
            400,
            r#"{"error":"invalid or expired OAuth callback"}"#.into(),
        ));
    }
    no_store(html())
}
