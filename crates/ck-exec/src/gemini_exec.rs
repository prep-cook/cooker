//! The Gemini API-key executor over the internal stream (`docs/EXECUTION-CONTRACT.md`).
//!
//! Generation goes to `generateContent`, or `streamGenerateContent?alt=sse` when the upstream
//! streams, and a token count to `countTokens`, all under `{base}/v1beta/models/{model}`
//! through the Gemini request egress. The executor adjusts its own encoded upload, never the
//! client body: the model suffix's thinking, the body `model` when the upload names one, the
//! count envelope, and the payload rules last. `gemini-interactions` credentials keep the old
//! path until an Interactions codec exists (Q7).

use bytes::Bytes;
use ck_core::exec::{ExecError, FailureScope};
use ck_core::format::Format;
use ck_stream::ScratchLease;
use ck_translate::codec::document::{Document, Value};
use ck_translate::codec::{self, BodyMode, Operation, RequestEgress};
use ck_translate::gemini::request::{Egress, GeminiCapability};
use ck_translate::gemini::response::Ingress;
use ck_wire::json as gj;
use ck_wire::thinking::{self, ModelCaps, RequestThinking, parse_suffix};

use crate::gemini::{self, API_VERSION, GeminiExecutor, MAX_LINE};
use crate::google_wire::{self as wire, Method};
use crate::proxy::{self, GoHeaders};
use crate::types::{Attempt, Counted, Execute, Request, Streams, Upstream, status_err};

/// The provider this executor serves through the pipeline.
pub const PROVIDER: &str = "gemini";

/// The request egress and capability key of the Gemini API.
const DESTINATION: &str = "gemini";

/// The member a `countTokens` body carries a whole generation request in.
const COUNT_WRAPPER: &str = "generateContentRequest";

impl Execute for GeminiExecutor {
    type Capability = GeminiCapability;
    type Ingress = Ingress;

    fn capability(&self, _attempt: &Attempt<'_>) -> GeminiCapability {
        GeminiCapability
    }

    async fn execute(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<Ingress>, ExecError> {
        let stream = streaming(request.validated.view().document).map_err(encoding)?;
        let egress = Egress {
            destination: DESTINATION,
            upstream_stream: stream,
        };
        let upload = copy(&egress.encode(&request.validated, streams.request).map_err(encoding)?)?;
        let call = Call::new(attempt, request)?;
        let body = call.generate(upload)?;
        let mode = if stream {
            BodyMode::Streaming
        } else {
            BodyMode::Buffered
        };
        let ingress = Ingress::new(
            streams.response,
            attempt.identity,
            Operation::Generate,
            mode,
            MAX_LINE as u64,
        )
        .map_err(encoding)?;
        let method = if stream {
            Method::StreamGenerate
        } else {
            Method::Generate
        };
        let upstream = self.send(&call, method, body).await?;
        Ok(Upstream {
            status: upstream.status,
            headers: upstream.headers,
            body: upstream.body,
            ingress,
        })
    }

    async fn count_tokens(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Counted<Ingress>, ExecError> {
        let egress = Egress {
            destination: DESTINATION,
            upstream_stream: false,
        };
        let upload = copy(&egress.encode(&request.validated, streams.request).map_err(encoding)?)?;
        let call = Call::new(attempt, request)?;
        let body = call.count(upload);
        let ingress = Ingress::new(
            streams.response,
            attempt.identity,
            Operation::CountTokens,
            BodyMode::Buffered,
            MAX_LINE as u64,
        )
        .map_err(encoding)?;
        let upstream = self.send(&call, Method::CountTokens, body).await?;
        Ok(Counted::Upstream(Upstream {
            status: upstream.status,
            headers: upstream.headers,
            body: upstream.body,
            ingress,
        }))
    }

    /// Gemini has no native compact; [`GeminiCapability`] refuses it at the fence.
    async fn compact(
        &self,
        _attempt: &Attempt<'_>,
        _request: &Request<'_>,
        _streams: Streams<'_>,
    ) -> Result<Upstream<Ingress>, ExecError> {
        Err(encoding(codec::Error::Unsupported))
    }
}

impl GeminiExecutor {
    /// Posts `body` to the model's `method` with the credential's key and custom headers; a
    /// non-2xx answer is the attempt's failure with the upstream's status and body.
    async fn send(&self, call: &Call<'_>, method: Method, body: Vec<u8>) -> Result<proxy::Upstream, ExecError> {
        let credential = call.attempt.credential;
        let models = format!("{}/{API_VERSION}/models", gemini::base_url(credential));
        let url = wire::model_url(&models, &call.base_model, method);
        let mut headers = GoHeaders::new();
        headers.set("Content-Type", "application/json");
        headers.set("User-Agent", wire::USER_AGENT);
        wire::api_key(&mut headers, gemini::api_key(credential));
        let client = &call.request.client;
        let session = ck_wire::session::cpa_session_id(client.session);
        for (name, value) in
            ck_wire::headers::custom_headers(&credential.attributes, client.headers, session.as_deref())
        {
            headers.set(&name, value);
        }
        // Refused before anything is sent, so the key never reaches a host the url crate
        // reads differently from Go.
        crate::xai_url::new_request(&url)?;
        if let Some(error) = crate::xai_url::transport_refusal(&url) {
            return Err(error);
        }
        let http = self.client(credential, call.attempt.cfg);
        let upstream = proxy::send(&http, &url, headers, Bytes::from(body), None).await?;
        if (200..300).contains(&upstream.status) {
            return Ok(upstream);
        }
        let status = upstream.status;
        let body = proxy::read_all(upstream.body, proxy::MAX_ERROR_BODY, true)
            .await
            .unwrap_or_default();
        Err(status_err(status, body))
    }
}

/// One attempt's facts the upload adjustments and the send read.
struct Call<'r> {
    attempt: &'r Attempt<'r>,
    request: &'r Request<'r>,
    /// The upstream model without its thinking suffix.
    base_model: String,
    /// The model the client asked for, as payload rules match it.
    requested: String,
    /// The client's protocol, named as payload rules name it.
    source: &'static str,
}

impl<'r> Call<'r> {
    fn new(attempt: &'r Attempt<'r>, request: &'r Request<'r>) -> Result<Self, ExecError> {
        let document = request.validated.view().document;
        Ok(Self {
            attempt,
            request,
            base_model: parse_suffix(attempt.model).model_name.trim().to_owned(),
            requested: requested_model(document).map_err(encoding)?,
            source: source(document).map_err(encoding)?,
        })
    }

    /// A generation body: the suffix's thinking, the upstream model, then the payload rules.
    fn generate(&self, upload: Vec<u8>) -> Result<Vec<u8>, ExecError> {
        let caps = self.attempt.resolved_model.map(|r| ModelCaps::from(&r.info));
        let mut body = thinking::apply_request_thinking(&RequestThinking {
            body: &upload,
            payload: &upload,
            original: &upload,
            model: self.attempt.model,
            // The upload is already in the target protocol.
            from: Format::Gemini.as_str(),
            to: Format::Gemini.as_str(),
            provider: &self.attempt.credential.provider,
            resolved: caps.as_ref().map(Some),
            has_request_transformer: false,
            updates_changed: false,
        })
        .map_err(|e| ExecError::local(e.status(), FailureScope::Request, e.message))?;
        if gj::get(&body, "model").exists() {
            self.name_model(&mut body, "model");
        }
        Ok(self.rules("", &upload, body))
    }

    /// A `countTokens` body. Only `contents` may stand alone; a count of anything more (a
    /// system instruction, tools, configuration) carries the whole generation request in
    /// `generateContentRequest`, which names its model.
    fn count(&self, upload: Vec<u8>) -> Vec<u8> {
        if gj::get(&upload, COUNT_WRAPPER).exists() {
            let mut body = self.rules(COUNT_WRAPPER, &upload, upload.clone());
            self.name_model(&mut body, &format!("{COUNT_WRAPPER}.model"));
            return body;
        }
        let body = self.rules("", &upload, upload.clone());
        let mut contents_only = true;
        gj::parse(&body).each(|key, _| {
            contents_only = key.str() == "contents";
            contents_only
        });
        if contents_only {
            return body;
        }
        let mut request = body;
        self.name_model(&mut request, "model");
        [format!("{{\"{COUNT_WRAPPER}\":").as_bytes(), &request, b"}"].concat()
    }

    /// Names the upstream model at `path`, as the URL does.
    fn name_model(&self, body: &mut Vec<u8>, path: &str) {
        let name = wire::model_name(&self.base_model);
        if gj::get(body, path).str() != name {
            gj::set_str(body, path, name);
        }
    }

    /// The configured payload rules under `root`, the last change to the body.
    fn rules(&self, root: &str, original: &[u8], body: Vec<u8>) -> Vec<u8> {
        let rules = ck_wire::payload::Rules::of(self.attempt.cfg);
        ck_wire::payload::apply(
            &rules,
            &ck_wire::payload::Request {
                target_executor: PROVIDER,
                model: &self.base_model,
                requested_model: &self.requested,
                protocol: Format::Gemini.as_str(),
                from_protocol: self.source,
                root,
                original,
                request_path: self.request.client.route,
                headers: Some(self.request.client.headers),
            },
            body,
        )
    }
}

/// The client's protocol, as `Format` names it.
fn source(doc: &Document) -> codec::Result<&'static str> {
    let x = doc.required(doc.root(), "x_ck")?;
    let protocol = doc.required(x, "original_protocol")?;
    for (name, format) in [
        ("chat", Format::OpenAI),
        ("responses", Format::OpenAIResponse),
        ("anthropic", Format::Claude),
        ("gemini", Format::Gemini),
    ] {
        if doc.string_is(protocol, name)? {
            return Ok(format.as_str());
        }
    }
    Err(codec::Error::Schema)
}

/// The request's `model`: the client's model, or a Gemini client's URL model.
fn requested_model(doc: &Document) -> codec::Result<String> {
    let mut chars = doc.chars(doc.required(doc.root(), "model")?)?;
    let mut model = String::new();
    while let Some(c) = chars.next_scalar()? {
        model.push(c);
    }
    Ok(model.trim().to_owned())
}

/// Whether the upstream streams: as the client's `stream` member says. A Gemini client
/// states it only in its route, which the request handle does not carry, so it streams.
fn streaming(doc: &Document) -> codec::Result<bool> {
    match doc.member(doc.root(), "stream")? {
        Some(node) => Ok(matches!(doc.node(node)?.value, Value::Bool(true))),
        None => Ok(source(doc)? == Format::Gemini.as_str()),
    }
}

/// The encoded upload as one body; the HTTP client takes ownership of it.
fn copy(lease: &ScratchLease) -> Result<Vec<u8>, ExecError> {
    let mut body = Vec::with_capacity(lease.len());
    while body.len() < lease.len() {
        let chunk = lease
            .chunk(body.len())
            .map_err(|e| encoding(codec::Error::Storage(e)))?;
        body.extend_from_slice(chunk);
    }
    Ok(body)
}

/// A failure before anything was sent (contract §9): storage rows by the inner error;
/// anything else is a gateway defect, since the request passed this destination's fence.
fn encoding(error: codec::Error) -> ExecError {
    match error {
        codec::Error::Storage(ck_stream::Error::StorageExhausted | ck_stream::Error::Io(_)) => {
            ExecError::local(503, FailureScope::Request, "stream.storage_exhausted")
        }
        codec::Error::Storage(ck_stream::Error::Gone) => ExecError::local(410, FailureScope::Request, "stream.gone"),
        error => ExecError::local(500, FailureScope::Request, format!("request encoding failed: {error}")),
    }
}
