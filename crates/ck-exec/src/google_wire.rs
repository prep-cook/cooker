//! Wire helpers shared by the Gemini, Vertex and Antigravity executors (Phase D, P4).
//!
//! A Google generative-language model resource is addressed as `{models}/{model}:{method}`,
//! where `{models}` is the provider's model collection URL (the Gemini API's
//! `{base}/v1beta/models`). A streamed generation always asks for SSE framing (`alt=sse`),
//! the framing the Gemini response ingress decodes.

use crate::proxy::GoHeaders;

/// A model method of the generative-language API.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Method {
    /// `generateContent`: one JSON response.
    Generate,
    /// `streamGenerateContent` with `alt=sse`: one SSE event per response chunk.
    StreamGenerate,
    /// `countTokens`: one JSON response with `totalTokens`.
    CountTokens,
}

/// `{models}/{model}:{method}`, with `?alt=sse` for [`Method::StreamGenerate`]. `models` is
/// the model collection URL without a trailing slash; `model` is the upstream model id,
/// without the `models/` prefix or a thinking suffix.
pub(crate) fn model_url(models: &str, model: &str, method: Method) -> String {
    let (action, query) = match method {
        Method::Generate => ("generateContent", ""),
        Method::StreamGenerate => ("streamGenerateContent", "?alt=sse"),
        Method::CountTokens => ("countTokens", ""),
    };
    format!("{models}/{model}:{action}{query}")
}

/// The model's resource name, as request bodies name it (`models/{model}`).
pub(crate) fn model_name(model: &str) -> String {
    format!("models/{model}")
}

/// Places an API key in `x-goog-api-key`; an empty key sends no header.
pub(crate) fn api_key(headers: &mut GoHeaders, key: &str) {
    if !key.is_empty() {
        headers.set("x-goog-api-key", key);
    }
}

#[cfg(test)]
mod tests {
    use super::{Method, model_url};

    #[test]
    fn model_urls_name_the_method_and_stream_framing() {
        let models = "https://host/v1beta/models";
        assert_eq!(
            model_url(models, "gemini-2.5-pro", Method::Generate),
            "https://host/v1beta/models/gemini-2.5-pro:generateContent"
        );
        assert_eq!(
            model_url(models, "gemini-2.5-pro", Method::StreamGenerate),
            "https://host/v1beta/models/gemini-2.5-pro:streamGenerateContent?alt=sse"
        );
        assert_eq!(
            model_url(models, "gemini-2.5-pro", Method::CountTokens),
            "https://host/v1beta/models/gemini-2.5-pro:countTokens"
        );
    }
}
