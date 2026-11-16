//! Native deferred Chat Completions (X11): xAI's `deferred:true` create and the
//! `chat/deferred-completion/{request_id}` poll. The job is recorded in the shared
//! [`crate::xai_media::VideoJobs`] table as a Chat job, bound to the caller and credential,
//! and its usage is charged once. No path falls back to a synchronous completion.
//!
//! <https://docs.x.ai/developers/advanced-api-usage/deferred-chat-completions>

use bytes::Bytes;
use ck_core::config::Config;
use ck_core::credential::Credential;
use ck_core::exec::{ExecError, ExecRequest, ExecResponse, FailureScope, ResponseBody};
use ck_core::format::Format;
use ck_wire::json as gj;
use ck_wire::thinking::parse_suffix;

use crate::payload_barrier::Barrier;
use crate::xai::{
    PROVIDER, XaiExecutor, chat_base_url, creds, endpoint, path_escape, plain_headers, scoped, using_api,
};
use crate::xai_media::{JobBinding, JobKind, jobs_full, reports_usage, unknown_job};
use crate::xai_response as response;

/// The poll marker the deferred Chat route puts in `ExecRequest::alt`.
const POLL: &str = crate::DEFERRED_CHAT_POLL;

/// A failure before or after a deferred request was sent. A sent create may already have
/// created a billable job and a sent poll may have consumed its result, so it is never retried
/// on another credential.
fn failure(status: u16, message: impl Into<String>) -> ExecError {
    ExecError::local(status, FailureScope::Request, message)
}

/// The upstream's failure, kept for the client but marked request-scoped so dispatch does not
/// retry it.
fn upstream_failure(status: u16, data: &[u8]) -> ExecError {
    let mut error = response::status_error(status, data);
    error.scope = FailureScope::Request;
    error
}

impl XaiExecutor {
    /// The deferred Chat create or poll on the official API. A create reserves a place in the
    /// job table before anything is sent and returns xAI's `request_id`; a poll is admitted
    /// only for the caller that created the job, sends `GET` on the recorded credential's
    /// model, and answers pending as an empty 202 body.
    pub(crate) async fn deferred_chat(
        &self,
        credential: &Credential,
        req: ExecRequest,
        cfg: &Config,
    ) -> Result<ExecResponse, ExecError> {
        if !using_api(credential) {
            return Err(failure(400, "Deferred Chat requires a native xAI API credential."));
        }
        let cfg = scoped(credential, cfg);
        let poll = req.alt.as_deref() == Some(POLL);
        let (path, body, slot, admission) = if poll {
            let id = gj::get(&req.body, "request_id").str().into_owned();
            let admission = self
                .jobs
                .admit(&req.caller, JobKind::Chat, &id)
                .ok_or_else(|| unknown_job(JobKind::Chat, &id))?;
            (format!("/{POLL}/{}", path_escape(&id)), None, None, Some(admission))
        } else {
            let model = parse_suffix(&req.model).model_name;
            let mut body = req.body.to_vec();
            gj::set_str(&mut body, "model", &model);
            let body = Barrier {
                executor: PROVIDER,
                model: &model,
                protocol: "openai",
                root: "",
                original: &req.body,
            }
            .apply(cfg.as_ref(), &req, body);
            if gj::get(&body, "deferred").kind != gj::Kind::True || gj::get(&body, "stream").bool() {
                return Err(failure(
                    400,
                    "Deferred Chat cannot be streamed and must keep deferred:true; poll the returned request_id.",
                ));
            }
            let binding = JobBinding {
                credential: credential.id.clone(),
                model: req.model.trim().to_owned(),
            };
            let slot = self
                .jobs
                .reserve(&req.caller, JobKind::Chat, binding)
                .ok_or_else(|| jobs_full(JobKind::Chat))?;
            req.usage.request_for(PROVIDER, &body);
            ("/chat/completions".to_owned(), Some(body), Some(slot), None)
        };
        req.usage.usage_required();
        let (token, _) = creds(credential);
        let headers = plain_headers(credential, &token, false, "", &req);
        let url = self.url(endpoint(&chat_base_url(credential), &path));
        let (status, headers, data) = self
            .send_buffered(
                credential,
                &req,
                cfg.as_ref(),
                if poll { wreq::Method::GET } else { wreq::Method::POST },
                &url,
                headers,
                body,
                None,
            )
            .await
            .map_err(|mut error| {
                error.scope = FailureScope::Request;
                error
            })?;
        if poll && status == 202 {
            return Ok(ExecResponse {
                status,
                headers,
                body: ResponseBody::Buffered(Bytes::new()),
            });
        }
        if !(200..300).contains(&status) {
            return Err(upstream_failure(status, &data));
        }
        if let Some(admission) = admission {
            if !gj::get(&data, "choices").is_array() {
                return Err(failure(
                    502,
                    "Deferred completion failed or returned an invalid response.",
                ));
            }
            if req.usage.enabled() {
                // The job's usage is charged by the first poll that settles it; a later poll
                // reports the completion without a count so the server's body read cannot
                // record it a second time.
                if reports_usage(&data) && admission.settle() {
                    req.usage.response_body(Format::OpenAI, &data);
                } else {
                    let mut quiet = data.to_vec();
                    gj::delete(&mut quiet, "usage");
                    req.usage.response_body(Format::OpenAI, &quiet);
                }
            }
        } else {
            let id = gj::get(&data, "request_id");
            if id.kind != gj::Kind::String || id.str().trim().is_empty() {
                return Err(failure(
                    502,
                    "Deferred completion creation did not return a request_id.",
                ));
            }
            let id = id.str().into_owned();
            if let Some(slot) = slot {
                drop(slot.created(&id));
            }
        }
        Ok(ExecResponse {
            status,
            headers,
            body: ResponseBody::Buffered(data),
        })
    }
}
