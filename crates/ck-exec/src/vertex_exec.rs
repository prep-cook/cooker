//! The Vertex AI executor over the internal stream (`docs/EXECUTION-CONTRACT.md`).
//!
//! Google models go through the Gemini request egress to the publisher model's
//! `generateContent`, or `streamGenerateContent?alt=sse` when the upstream streams, and a token
//! count to its `countTokens`. The upload is adjusted as the Gemini API executor adjusts its
//! own ([`Call`]): the model suffix's thinking, the body `model` when the upload names one,
//! and the payload rules last. Function call and response ids are not sent: Vertex refuses
//! them, and its calls and responses pair by order and name. Partner and open models, and credentials that opt in, take the
//! OpenAI-compatible `endpoints/openapi` route through the relay executor's Chat generation and
//! local count. Imagen models keep the old path until media operations have codecs (Phase E),
//! and neither route has a native compact.

use bytes::Bytes;
use ck_core::exec::ExecError;
use ck_core::format::Format;
use ck_stream::Writer;
use ck_translate::codec::chat::response::Ingress as ChatIngress;
use ck_translate::codec::document::{Document, Value};
use ck_translate::codec::{self, BodyMode, Capability, Operation, RequestEgress, RequestView, ResponseIngress};
use ck_translate::gemini::request::{Egress, GeminiCapability};
use ck_translate::gemini::response::Ingress as GeminiIngress;
use ck_wire::json as gj;
use ck_wire::thinking::parse_suffix;

use crate::gemini::MAX_LINE;
use crate::gemini_exec::{COUNT_WRAPPER, Call, copy, encoding, refused, streaming};
use crate::google_wire::{self as wire, Method};
use crate::openai_compat_exec::{self as relay, CHAT_PATH, RelayCapability, Route};
use crate::proxy::{self, GoHeaders};
use crate::types::{Attempt, Counted, Execute, Request, Streams, Upstream};
use crate::upstream::{record_answer, record_failure, record_send};
use crate::vertex::{self, Auth, VertexExecutor};
use crate::vertex_openapi;

/// Members of Vertex's `CountTokensRequest` that a Gemini generation request can carry.
const COUNT_MEMBERS: [&str; 5] = ["model", "contents", "systemInstruction", "tools", "generationConfig"];

/// The attempt's capability key; `None` for an Imagen model, which keeps the old path.
pub(crate) fn destination(executor: &VertexExecutor, attempt: &Attempt<'_>) -> Option<&'static str> {
    (!vertex::is_imagen(&parse_suffix(attempt.model).model_name)).then(|| executor.capability(attempt).destination())
}

/// The route an attempt's model takes: the Gemini request codec, or the relay's Chat codec
/// without its native compact.
pub enum VertexCapability {
    Gemini,
    OpenApi,
}

impl Capability for VertexCapability {
    fn destination(&self) -> &'static str {
        match self {
            Self::Gemini => GeminiCapability.destination(),
            Self::OpenApi => RelayCapability.destination(),
        }
    }

    fn validate(&self, view: RequestView<'_>) -> codec::Result<()> {
        match self {
            // Vertex's count request has no cached content, which changes the count.
            Self::Gemini if view.operation == Operation::CountTokens && cached_content(view.document)? => {
                Err(codec::Error::Unsupported)
            }
            Self::Gemini => GeminiCapability.validate(view),
            Self::OpenApi if view.operation == Operation::Compact => Err(codec::Error::Unsupported),
            Self::OpenApi => RelayCapability.validate(view),
        }
    }
}

/// The response decoder of the route the attempt took. Boxed: the producer task moves it
/// once, and the variants differ in size.
pub enum VertexIngress {
    Gemini(Box<GeminiIngress>),
    Chat(Box<ChatIngress>),
}

impl ResponseIngress for VertexIngress {
    fn flush_deadline(&self) -> Option<std::time::Instant> {
        match self {
            Self::Gemini(ingress) => ingress.flush_deadline(),
            Self::Chat(ingress) => ingress.flush_deadline(),
        }
    }

    fn flush(&mut self, owner: &mut Writer) -> codec::Result<()> {
        match self {
            Self::Gemini(ingress) => ingress.flush(owner),
            Self::Chat(ingress) => ingress.flush(owner),
        }
    }

    fn advance(&mut self, owner: &mut Writer, bytes: &[u8]) -> codec::Result<()> {
        match self {
            Self::Gemini(ingress) => ingress.advance(owner, bytes),
            Self::Chat(ingress) => ingress.advance(owner, bytes),
        }
    }

    fn finish(&mut self, owner: &mut Writer) -> codec::Result<()> {
        match self {
            Self::Gemini(ingress) => ingress.finish(owner),
            Self::Chat(ingress) => ingress.finish(owner),
        }
    }

    fn fail(&mut self, owner: &mut Writer, status: u16, scope: &'static str) -> codec::Result<()> {
        match self {
            Self::Gemini(ingress) => ingress.fail(owner, status, scope),
            Self::Chat(ingress) => ingress.fail(owner, status, scope),
        }
    }
}

impl Execute for VertexExecutor {
    type Capability = VertexCapability;
    type Ingress = VertexIngress;

    fn capability(&self, attempt: &Attempt<'_>) -> VertexCapability {
        if vertex_openapi::should_execute(attempt.credential, attempt.model) {
            VertexCapability::OpenApi
        } else {
            VertexCapability::Gemini
        }
    }

    async fn execute(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<VertexIngress>, ExecError> {
        match self.capability(attempt) {
            VertexCapability::Gemini => self.google(attempt, request, streams, Operation::Generate).await,
            VertexCapability::OpenApi => self.endpoints_openapi(attempt, request, streams).await,
        }
    }

    async fn count_tokens(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Counted<VertexIngress>, ExecError> {
        match self.capability(attempt) {
            VertexCapability::Gemini => self
                .google(attempt, request, streams, Operation::CountTokens)
                .await
                .map(Counted::Upstream),
            VertexCapability::OpenApi => {
                let model = openapi_model(attempt.model);
                let input_tokens = relay::count(&with_model(attempt, &model), request, streams)?;
                Ok(Counted::Local { input_tokens })
            }
        }
    }

    /// Vertex has no native compact; [`VertexCapability`] refuses it at the fence.
    async fn compact(
        &self,
        _attempt: &Attempt<'_>,
        _request: &Request<'_>,
        _streams: Streams<'_>,
    ) -> Result<Upstream<VertexIngress>, ExecError> {
        Err(encoding(codec::Error::Unsupported))
    }
}

impl VertexExecutor {
    /// A Google model's generation or token count through the Gemini request codec.
    async fn google(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
        operation: Operation,
    ) -> Result<Upstream<VertexIngress>, ExecError> {
        let count = operation == Operation::CountTokens;
        let stream = !count && streaming(request.validated.view().document).map_err(encoding)?;
        let egress = Egress {
            destination: GeminiCapability.destination(),
            upstream_stream: stream,
        };
        let upload = copy(&egress.encode(&request.validated, streams.request).map_err(encoding)?)?;
        let target = Target::new(attempt)?;
        let call = Call::new(attempt, request, vertex::PROVIDER, |model| target.resource(model))?;
        let (body, method) = match (count, stream) {
            (true, _) => (count_body(&call, upload), Method::CountTokens),
            (false, true) => (call.generate(vertex::strip_call_ids(upload))?, Method::StreamGenerate),
            (false, false) => (call.generate(vertex::strip_call_ids(upload))?, Method::Generate),
        };
        let mode = if stream {
            BodyMode::Streaming
        } else {
            BodyMode::Buffered
        };
        let ingress = GeminiIngress::new(streams.response, attempt.identity, operation, mode, MAX_LINE as u64)
            .map_err(encoding)?;
        let upstream = self.send(attempt, request, &target, &call, method, body).await?;
        Ok(Upstream {
            status: upstream.status,
            headers: upstream.headers,
            body: upstream.body,
            ingress: VertexIngress::Gemini(Box::new(ingress)),
        })
    }

    /// Posts `body` to the model's `method` with the credential (an API key, or a fresh access
    /// token) and the custom headers, recording the call's request-side facts (contract G4); a
    /// non-2xx answer is the attempt's failure with the upstream's status and body as received.
    async fn send(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        target: &Target,
        call: &Call<'_>,
        method: Method,
        body: Vec<u8>,
    ) -> Result<proxy::Upstream, ExecError> {
        let credential = attempt.credential;
        let url = &wire::model_url(&target.models, &call.base_model, method);
        // Refused before the token exchange and before anything is sent, so no credential
        // reaches a host the url crate reads differently from Go.
        vertex::check_url(url)?;
        if let Some(error) = crate::xai_url::transport_refusal(url) {
            return Err(error);
        }
        let client = self.client(credential, attempt.cfg);
        let mut headers = GoHeaders::new();
        headers.set("Content-Type", "application/json");
        match &target.auth {
            Auth::ApiKey { key, .. } => wire::api_key(&mut headers, key),
            Auth::ServiceAccount { account, .. } => {
                let token = self.mint(&client, account).await?;
                // An empty token sends no Authorization, as Go's token source does.
                if !token.is_empty() {
                    headers.set("Authorization", format!("Bearer {token}"));
                }
            }
        }
        let client_side = &request.client;
        let session = ck_wire::session::cpa_session_id(client_side.session);
        for (name, value) in
            ck_wire::headers::custom_headers(&credential.attributes, client_side.headers, session.as_deref())
        {
            headers.set(&name, value);
        }
        // A count is not generation usage (contract §3).
        let (usage, generation) = (client_side.usage, method != Method::CountTokens);
        if generation {
            if usage.enabled() {
                usage.request(Format::Gemini, &body);
            }
            usage.upstream_model(&call.base_model);
        }
        let body = Bytes::from(body);
        let sent = (headers.pairs(), &body);
        record_send(usage, generation, credential, vertex::PROVIDER, url, sent);
        let upstream = proxy::send(&client, url, headers, body, None)
            .await
            .inspect_err(|error| record_failure(usage, error))?;
        let upstream = proxy::Upstream {
            body: record_answer(usage, generation, upstream.status, &upstream.headers, upstream.body),
            ..upstream
        };
        if (200..300).contains(&upstream.status) {
            return Ok(upstream);
        }
        Err(refused(upstream).await)
    }

    /// A partner or open model's Chat generation at the credential's `endpoints/openapi` base,
    /// as the relay executor sends it, without a compatibility config.
    async fn endpoints_openapi(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<VertexIngress>, ExecError> {
        let (base_url, key) = vertex::openapi_route(attempt.credential)?;
        // Refused before the token exchange, as the relay refuses it before its send.
        let url = crate::openai_compat::endpoint(&base_url, CHAT_PATH);
        crate::xai_url::new_request(&url)?;
        if let Some(error) = crate::xai_url::transport_refusal(&url) {
            return Err(error);
        }
        let client = self.client(attempt.credential, attempt.cfg);
        let key = self.openapi_key(&client, key).await?;
        let route = Route {
            client,
            base_url,
            key,
            compat: None,
        };
        let model = openapi_model(attempt.model);
        let upstream = relay::chat(&with_model(attempt, &model), request, streams, route).await?;
        Ok(Upstream {
            status: upstream.status,
            headers: upstream.headers,
            body: upstream.body,
            ingress: VertexIngress::Chat(Box::new(upstream.ingress)),
        })
    }
}

/// Where a Google model's requests go and how they authenticate.
struct Target {
    auth: Auth,
    /// The publisher's model collection URL.
    models: String,
}

impl Target {
    fn new(attempt: &Attempt<'_>) -> Result<Self, ExecError> {
        let auth = vertex::auth_for(attempt.credential)?;
        let models = vertex::models_url(&auth);
        Ok(Self { auth, models })
    }

    /// The model's resource name, as Vertex request bodies name it: under the service
    /// account's project and location, or under the publisher an API key's URL names.
    fn resource(&self, model: &str) -> String {
        match &self.auth {
            Auth::ServiceAccount { project, location, .. } => {
                format!("projects/{project}/locations/{location}/publishers/google/models/{model}")
            }
            Auth::ApiKey { .. } => {
                let at = self.models.find("/publishers/").map_or(0, |at| at + 1);
                format!("{}/{model}", &self.models[at..])
            }
        }
    }
}

/// A `countTokens` body. Vertex's count request is flat: a Gemini client's
/// `generateContentRequest` unwraps into it. The generation members it has no field for and
/// that do not change the count (`safetySettings`, `toolConfig`, `labels`, and a null
/// `cachedContent`; the fence refuses any other), which Vertex refuses as unknown names, are
/// not sent, nor are function call and response ids. Then, as for generation, the upstream
/// model and the payload rules last.
fn count_body(call: &Call<'_>, upload: Vec<u8>) -> Vec<u8> {
    let mut projected = Vec::with_capacity(upload.len());
    {
        let wrapped = gj::get(&upload, COUNT_WRAPPER);
        let request = if wrapped.exists() { wrapped.raw() } else { &upload[..] };
        projected.push(b'{');
        gj::parse(request).each(|key, value| {
            if COUNT_MEMBERS.iter().any(|m| key.bytes().as_ref() == m.as_bytes()) {
                if projected.len() > 1 {
                    projected.push(b',');
                }
                projected.extend_from_slice(key.raw());
                projected.push(b':');
                projected.extend_from_slice(value.raw());
            }
            true
        });
        projected.push(b'}');
    }
    drop(upload);
    let original = vertex::strip_call_ids(projected);
    let mut body = original.clone();
    if gj::get(&body, "model").exists() {
        call.name_model(&mut body, "model");
    }
    call.rules("", &original, body)
}

/// Whether the request names cached content: a Gemini client's `cachedContent`, kept as its
/// native parameter. A null one is unset in protobuf JSON.
fn cached_content(doc: &Document) -> codec::Result<bool> {
    let mut node = doc.root();
    for name in ["x_ck", "provider_parameters", "gemini", "cachedContent"] {
        match doc.member(node, name)? {
            Some(member) => node = member,
            None => return Ok(false),
        }
    }
    Ok(!matches!(doc.node(node)?.value, Value::Null))
}

/// The `<publisher>/<model>` upstream model of the `endpoints/openapi` route, keeping a
/// thinking suffix.
fn openapi_model(model: &str) -> String {
    match vertex_openapi::normalize_model(model) {
        normalized if normalized.is_empty() => model.to_owned(),
        normalized => normalized,
    }
}

/// `attempt` with another upstream model.
fn with_model<'a>(attempt: &Attempt<'a>, model: &'a str) -> Attempt<'a> {
    Attempt {
        credential: attempt.credential,
        cfg: attempt.cfg,
        model,
        headers: attempt.headers,
        resolved_model: attempt.resolved_model,
        store: attempt.store,
        identity: attempt.identity,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Map;

    use super::{Auth, Target, vertex};

    fn target(auth: Auth) -> Target {
        let models = vertex::models_url(&auth);
        Target { auth, models }
    }

    #[test]
    fn service_accounts_address_their_project_and_location() {
        let target = target(Auth::ServiceAccount {
            project: "proj".into(),
            location: "europe-west4".into(),
            account: Map::new(),
        });
        assert_eq!(
            target.models,
            "https://europe-west4-aiplatform.googleapis.com/v1/projects/proj/locations/europe-west4/publishers/google/models"
        );
        assert_eq!(
            target.resource("gemini-2.5-pro"),
            "projects/proj/locations/europe-west4/publishers/google/models/gemini-2.5-pro"
        );
        let global = super::Target {
            models: String::new(),
            auth: Auth::ServiceAccount {
                project: "p".into(),
                location: "global".into(),
                account: Map::new(),
            },
        };
        assert_eq!(
            vertex::models_url(&global.auth),
            "https://aiplatform.googleapis.com/v1/projects/p/locations/global/publishers/google/models"
        );
    }

    #[test]
    fn api_keys_name_the_publisher_model() {
        for (base, models) in [
            ("", "https://aiplatform.googleapis.com/v1/publishers/google/models"),
            (
                "https://relay/v1beta1/",
                "https://relay/v1beta1/publishers/google/models",
            ),
            (
                "https://relay/x/publishers/acme/models",
                "https://relay/x/publishers/acme/models",
            ),
        ] {
            let target = target(Auth::ApiKey {
                key: "k".into(),
                base: base.into(),
            });
            assert_eq!(target.models, models, "{base}");
            let publisher = &models[models.find("publishers/").unwrap()..];
            assert_eq!(target.resource("m"), format!("{publisher}/m"), "{base}");
        }
    }
}
