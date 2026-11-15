//! Authentication and default identity for OpenAI-compatible requests.

use crate::openai_compat::USER_AGENT;
use crate::openai_compat_http::GoHeaders;

pub(crate) fn base_headers(api_key: &str, content_type: &str, api_auth: bool) -> GoHeaders {
    let mut headers = GoHeaders::new();
    headers.set("Content-Type", content_type);
    if !api_key.is_empty() {
        headers.set("Authorization", format!("Bearer {api_key}"));
    }
    headers.set(
        "User-Agent",
        if api_auth {
            USER_AGENT
        } else {
            "cli-proxy-openai-compat"
        },
    );
    headers
}
