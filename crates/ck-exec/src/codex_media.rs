//! Codex images under the execution contract (`docs/EXECUTION-CONTRACT.md`): a gpt-image-*
//! execution model goes to the backend's own Images endpoint ("direct"), any other becomes a
//! Responses request that forces the `image_generation` tool on the images main model. The
//! images codec encodes both bodies from this destination's fence; the executor adds what the
//! old path adds (the direct model, the prompt cache key, the tool body's thinking and Codex
//! rules) and the payload rules last. The answer streams through the images ingress, which
//! publishes the main model's usage and the tool's under its `tool:<model>` scope.

use ck_core::exec::ExecError;
use ck_core::format::Format;
use ck_translate::codec::{BodyMode, Media};
use ck_translate::images;

use crate::codex::{CodexExecutor, ErrorBody};
use crate::codex_capture::Wire;
use crate::codex_exec::{CodexIngress, DESTINATION, encoding, observed};
use crate::codex_images::{SOURCE, image_base_model, main_model, resolve_direct, tool_body};
use crate::codex_json::set_str_if_different;
use crate::codex_replay::Scope;
use crate::codex_request::{self as request, Settings, View};
use crate::media_exec;
use crate::types::{Attempt, Request, Streams, Upstream};

/// One upstream call: where it goes, what it sends, and how its answer reads.
struct ImageCall {
    url: String,
    headers: http::HeaderMap,
    body: String,
    wire: images::Wire,
    mode: BodyMode,
    error_body: ErrorBody,
}

impl CodexExecutor {
    /// Sends the fenced image request and returns the upstream answer with the images ingress on
    /// the attempt's response stream. `direct` is the capability's choice, so the body encoded
    /// here is the one the fence checked.
    pub(crate) async fn serve_images(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
        media: Media,
        direct: bool,
    ) -> Result<Upstream<CodexIngress>, ExecError> {
        crate::codex_siwc::require_token(attempt.credential)?;
        let session = ck_wire::session::cpa_session_id(request.client.session);
        let view = View::for_request(attempt.credential, attempt.cfg).with_session(session);
        let settings = Settings::scoped(attempt.cfg, &view);
        let shape = Shape {
            attempt,
            request,
            view: &view,
            settings: &settings,
            cache: crate::codex_shape::sessions_uuid(request.client.execution_session, request.client.derived_session),
        };
        let call = if direct {
            shape.direct(streams.request, media)?
        } else {
            shape.tool(streams.request)?
        };
        let usage = request.client.usage;
        let wire = Wire::new(usage.capture(), attempt.credential);
        usage.round_trip_started();
        let mut upstream = self
            .open(
                &view,
                &settings,
                call.url,
                call.headers,
                call.body,
                &Scope::default(),
                &wire,
                call.error_body,
                Some(usage),
            )
            .await?;
        upstream.body = observed(upstream.body, usage.clone(), wire);
        // After the send, so an attempt the upstream refused leaves its stream empty.
        let ingress = images::Ingress::new(
            streams.response,
            attempt.identity,
            call.wire,
            call.mode,
            media_exec::IMAGES_LIMIT,
        )
        .map_err(encoding)?;
        Ok(Upstream {
            status: upstream.status,
            headers: upstream.headers,
            body: upstream.body,
            ingress: CodexIngress::Images(Box::new(ingress)),
        })
    }
}

/// What both image bodies read from one attempt.
struct Shape<'r> {
    attempt: &'r Attempt<'r>,
    request: &'r Request<'r>,
    view: &'r View<'r>,
    settings: &'r Settings,
    /// The session's prompt cache identity.
    cache: Option<String>,
}

impl Shape<'_> {
    /// The backend's own Images endpoint: the JSON request with the resolved gpt-image-* model
    /// and the prompt cache key; an event stream when the client asked for one.
    fn direct(&self, owner: &mut ck_stream::Writer, media: Media) -> Result<ImageCall, ExecError> {
        let document = self.request.validated.view().document;
        let stream = media_exec::images_stream(document).map_err(encoding)?;
        let upload = utf8(media_exec::images_upload(
            self.request,
            DESTINATION,
            images::Destination::OpenAi,
            owner,
        )?)?;
        let model = resolve_direct(image_base_model(self.attempt.model), self.attempt.model, self.settings);
        let mut body = set_str_if_different(upload.clone(), "model", &model);
        let usage = self.request.client.usage;
        if usage.enabled() {
            usage.request(Format::OpenAI, body.as_bytes());
        }
        if let Some(id) = &self.cache {
            body = set_str_if_different(body, "prompt_cache_key", id);
        }
        let body = self.rules(&model, "openai", &upload, request::sanitize_input_ids(body))?;
        let headers = request::image_headers(
            self.view,
            self.settings,
            self.request.client.headers,
            &model,
            self.cache.as_deref(),
            stream,
        );
        let endpoint = if media == Media::ImageEdit {
            "/images/edits"
        } else {
            "/images/generations"
        };
        Ok(ImageCall {
            url: format!("{}{endpoint}", self.view.base_url),
            headers,
            body,
            wire: images::Wire::OpenAi,
            mode: if stream {
                BodyMode::Streaming
            } else {
                BodyMode::Buffered
            },
            error_body: ErrorBody::Lossy,
        })
    }

    /// The Responses request forcing the `image_generation` tool on the images main model,
    /// always streamed, as the backend requires.
    fn tool(&self, owner: &mut ck_stream::Writer) -> Result<ImageCall, ExecError> {
        let main = main_model(&self.settings.image_base_model);
        let upload = utf8(media_exec::images_upload(
            self.request,
            DESTINATION,
            images::Destination::Codex(main),
            owner,
        )?)?;
        let mut body = tool_body(&upload, main, self.view.siwc)?;
        if let Some(id) = &self.cache {
            body = set_str_if_different(body, "prompt_cache_key", id);
        }
        let body = self.rules(main, "codex", &upload, request::sanitize_input_ids(body))?;
        let usage = self.request.client.usage;
        if usage.enabled() {
            usage.request(Format::Codex, body.as_bytes());
        }
        let headers = request::image_headers(
            self.view,
            self.settings,
            self.request.client.headers,
            main,
            self.cache.as_deref(),
            true,
        );
        Ok(ImageCall {
            url: format!("{}/responses", self.view.base_url),
            headers,
            body,
            wire: images::Wire::Codex,
            mode: BodyMode::Streaming,
            error_body: ErrorBody::Strict,
        })
    }

    /// The payload rules, the last change to the body (868e137), as the codex executor, with the
    /// encoded upload as the original.
    fn rules(&self, model: &str, protocol: &str, original: &str, body: String) -> Result<String, ExecError> {
        let requested = media_exec::requested_model(self.request.validated.view().document).map_err(encoding)?;
        let requested = match requested.trim() {
            "" => self.attempt.model.trim(),
            requested => requested,
        };
        let client = &self.request.client;
        let rules = ck_wire::payload::Request {
            target_executor: "codex",
            model,
            requested_model: requested,
            protocol,
            from_protocol: SOURCE,
            root: "",
            original: original.as_bytes(),
            request_path: client.route,
            headers: Some(client.headers),
        };
        utf8(ck_wire::payload::apply(
            &self.settings.payload,
            &rules,
            body.into_bytes(),
        ))
    }
}

fn utf8(body: Vec<u8>) -> Result<String, ExecError> {
    String::from_utf8(body).map_err(|_| encoding(ck_translate::codec::Error::Schema))
}
