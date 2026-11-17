//! Codex Alpha Search under the execution contract (`docs/EXECUTION-CONTRACT.md`): the
//! request document is encoded back to Codex search JSON by the search codec, sent with the
//! Alpha Search headers, and the answer streams through the search ingress into one
//! `operation` record. An upstream refusal is the attempt's failure, so the next candidate
//! is tried; the old route's pass-through of the upstream status is gone with its handler.

use ck_core::config::Config;
use ck_core::credential::Credential;
use ck_core::exec::ExecError;
use ck_translate::codec::RequestEgress;
use ck_translate::search;

use crate::codex::{ALPHA_SEARCH_MAX_RESPONSE, AlphaCall, CodexExecutor, ErrorBody};
use crate::codex_capture::Wire;
use crate::codex_exec::{CodexIngress, copy, encoding, observed};
use crate::codex_replay::Scope;
use crate::codex_request::{self as request, Settings, View};
use crate::types::{Attempt, Request, Streams, Upstream};

impl CodexExecutor {
    /// Whether `credential` may serve Alpha Search (credential policy `codex_alpha_search_v1`):
    /// an OAuth login, or an API key that opted in and has a base URL to send to.
    pub fn alpha_allowed(&self, credential: &Credential, cfg: &Config) -> bool {
        match ck_core::registry::dynamic::auth_kind(credential) {
            Some("oauth") => true,
            Some("apikey") => {
                credential
                    .attributes
                    .get("codex_alpha_search")
                    .is_some_and(|v| v.trim().eq_ignore_ascii_case("true"))
                    && self.alpha_search_url_for(&View::for_request(credential, cfg)).is_some()
            }
            _ => false,
        }
    }

    /// Sends the fenced search request and returns the upstream answer with the search
    /// ingress on the attempt's response stream. Called only for a capability that admitted
    /// the operation, so a refusal never reaches this point.
    pub(crate) async fn alpha(
        &self,
        attempt: &Attempt<'_>,
        request: &Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<CodexIngress>, ExecError> {
        let egress = search::Egress {
            destination: crate::codex_exec::DESTINATION,
        };
        let upload = copy(&egress.encode(&request.validated(), streams.request).map_err(encoding)?)?;
        let session = ck_wire::session::cpa_session_id(request.client.session);
        let view = View::for_request(attempt.credential, attempt.cfg).with_session(session);
        let settings = Settings::scoped(attempt.cfg, &view);
        let model = request::base_model(attempt.model);
        let AlphaCall { url, headers, body } = self.alpha_call(&view, &upload, request.client.headers, &model)?;
        let body = String::from_utf8(body).map_err(|_| encoding(ck_translate::codec::Error::Schema))?;
        let usage = request.client.usage;
        let wire = Wire::new(usage.capture(), attempt.credential);
        usage.round_trip_started();
        let mut upstream = self
            .open(
                &view,
                &settings,
                url,
                headers,
                body,
                &Scope::default(),
                &wire,
                ErrorBody::Lossy,
                Some(usage),
            )
            .await?;
        upstream.body = observed(upstream.body, usage.clone(), wire);
        // After the send, so an attempt the upstream refused leaves its stream empty.
        let ingress = search::Ingress::new(streams.response, attempt.identity, ALPHA_SEARCH_MAX_RESPONSE as u64)
            .map_err(encoding)?;
        Ok(Upstream {
            status: upstream.status,
            headers: upstream.headers,
            body: upstream.body,
            ingress: CodexIngress::Alpha(Box::new(ingress)),
        })
    }
}
