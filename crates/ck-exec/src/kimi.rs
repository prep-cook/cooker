//! Kimi transport and the delegated Messages execution path.
//!
//! Credential endpoints distinguish Platform from Coding. Platform preserves native model
//! IDs and uses `/anthropic/v1/messages`; Coding retains its model aliases and `/coding` base.
//! Chat and Responses use the execution contract (`kimi_exec`). Responses custom tools stay
//! native; the legacy entry point follows the same policy until dispatch removes that arm.
//! Messages delegates to Claude with Kimi-owned endpoint, nonce, model and reasoning replay.

use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use ck_core::config::Config;
use ck_core::credential::{Credential, MetadataPatch};
use ck_core::exec::{
    ExecError, ExecRequest, ExecResponse, ExecStream, FailureScope, Operation, ResponseBody, UsageSink,
};
use ck_core::format::Format;
use ck_translate::{RequestCtx, ResponseCtx, StreamTranslator};
use ck_wire::gostr::GoStr;
use ck_wire::json::{self as gj, Kind, Res};
use ck_wire::thinking::{ModelCaps, RequestThinking, apply_request_thinking, parse_suffix};
use futures_util::StreamExt;

use crate::claude::{ClaudeExecutor, Delegation};
use crate::kimi_auth::{self, DeviceFlow};
use crate::kimi_edit;
use crate::kimi_http::{
    BUILD_VERSION, client_headers, go_arch, go_os, hostname, payload_rules, refresh_due, rfc3339_local_now,
    status_error,
};
use crate::kimi_http::{DeferredUsage, UsageRule, defer_usage, report_model};
use crate::kimi_replay::{self, ReplayCache};
use crate::meta_codex::go_trim_space;
use crate::openai_compat_payload::ensure_responses_usage_details;
use crate::proxy::{GoClients, GoHeaders, Proxy, Upstream, default_client, lines, read_all, send_scanned};

mod transport;
pub(crate) use transport::*;
mod chat;
pub(crate) use chat::*;
mod responses;
pub(crate) use responses::*;
mod normalize;
pub(crate) use normalize::*;
mod policy;
pub(crate) use policy::*;

const REASONING_UNAVAILABLE: &str = "[reasoning unavailable]";

/// Provider strings served by this executor.
pub const PROVIDERS: [&str; 4] = ["kimi", "kimi-ai", "kimi.ai", "kimi.com"];

/// Claude-format traffic goes through the shared [`ClaudeExecutor`] passed to
/// [`KimiExecutor::execute`]; Go embeds one in the Kimi executor for the same purpose.
pub struct KimiExecutor {
    clients: GoClients,
    oauth_host: Option<String>,
    replay: Arc<ReplayCache>,
}

impl Default for KimiExecutor {
    fn default() -> Self {
        Self::with_client(default_client())
    }
}

impl KimiExecutor {
    /// Uses a caller-built client (tests point it at local mocks).
    pub fn with_client(client: wreq::Client) -> Self {
        Self {
            clients: GoClients::with_default(client),
            oauth_host: None,
            replay: Arc::default(),
        }
    }

    /// The HTTP client for `credential`'s effective proxy.
    pub(crate) fn client(&self, credential: &Credential, cfg: &Config) -> wreq::Client {
        self.clients.get(&Proxy::effective(credential, cfg))
    }

    /// Sends token refreshes to a local mock instead of auth.kimi.com / auth.kimi.ai.
    pub fn with_oauth_host(mut self, host: &str) -> Self {
        self.oauth_host = Some(host.to_owned());
        self
    }

    pub fn needs_prepare(&self, credential: &Credential, cfg: &Config) -> bool {
        self.needs_prepare_at(credential, cfg, chrono::Utc::now())
    }

    pub fn needs_prepare_at(&self, credential: &Credential, _cfg: &Config, now: chrono::DateTime<chrono::Utc>) -> bool {
        // Go's refresh loop never schedules API-key-kind credentials.
        !ck_core::registry::dynamic::is_api_key(credential)
            && refresh_token(credential).is_some()
            && refresh_due(
                credential,
                chrono::Duration::from_std(kimi_auth::REFRESH_LEAD).ok(),
                now,
            )
    }

    /// Refresh grant; Kimi has no other preparation.
    pub async fn prepare(&self, credential: &Credential, cfg: &Config) -> Result<MetadataPatch, ExecError> {
        let Some(refresh) = refresh_token(credential) else {
            return Ok(MetadataPatch::default());
        };
        let domain = kimi_auth::resolve_domain(credential);
        let device_id = credential.str("device_id").unwrap_or_default();
        let mut flow = DeviceFlow::new(self.clients.get(&Proxy::effective(credential, cfg)), domain, device_id);
        if let Some(host) = &self.oauth_host {
            flow = flow.with_oauth_host(host);
        }
        let tokens = flow.refresh(refresh).await?;
        Ok(kimi_auth::refresh_patch(
            credential,
            &tokens,
            &base_url(credential),
            rfc3339_local_now(),
        ))
    }

    pub async fn execute(
        &self,
        claude: &ClaudeExecutor,
        credential: &Credential,
        req: ExecRequest,
        cfg: &Config,
    ) -> Result<ExecResponse, ExecError> {
        if req.operation == Operation::CountTokens || req.source_format == Format::Claude {
            return self.execute_claude(claude, credential, req, cfg).await;
        }
        let client = self.clients.get(&Proxy::effective(credential, cfg));
        if req.source_format == Format::OpenAIResponse {
            return execute_responses(&client, credential, req, cfg).await;
        }
        execute_chat(&client, credential, req, cfg).await
    }

    async fn execute_claude(
        &self,
        claude: &ClaudeExecutor,
        credential: &Credential,
        mut req: ExecRequest,
        cfg: &Config,
    ) -> Result<ExecResponse, ExecError> {
        let mut delegated = credential.clone();
        delegate_nonce(&mut delegated, &req.headers);
        delegated
            .attributes
            .insert("base_url".into(), claude_base_url(credential));
        let client_model = req.model.clone();
        // NewKimiExecutor's embedded ClaudeExecutor: normalized upstream model, count_tokens
        // always upstream (KimiExecutor.CountTokens -> countTokensUpstream), and request
        // logs naming kimi (requestLogProvider).
        let delegation = Delegation {
            upstream_model: coding(credential).then_some(normalize_upstream_model),
            count_upstream: true,
            request_log_provider: Some("kimi"),
        };
        if req.operation == Operation::CountTokens {
            return claude.execute_delegated(&delegated, req, cfg, delegation).await;
        }
        let scope = kimi_replay::prepare(&self.replay, &mut req);
        let streaming = req.stream;
        let mut response = match claude.execute_delegated(&delegated, req, cfg, delegation).await {
            Ok(response) => response,
            Err(error) => {
                if scope.applied && kimi_replay::clears_after(&error) {
                    scope.clear();
                }
                return Err(error);
            }
        };
        response.body = match response.body {
            ResponseBody::Buffered(body) => {
                let body = restore_response_model(&body, &client_model);
                if !streaming {
                    scope.store_response(&body);
                }
                ResponseBody::Buffered(body)
            }
            ResponseBody::Stream(stream) => {
                let restored = stream
                    .map(move |event| event.map(|e| restore_response_model(&e, &client_model)))
                    .boxed();
                ResponseBody::Stream(crate::replay::wrap_stream(restored, scope))
            }
        };
        Ok(response)
    }
}

/// `restoreClaudeResponseModel`: put the client's model back on a Claude response body or
/// on the `data:` line of an SSE event.
fn restore_response_model(payload: &[u8], model: &str) -> Bytes {
    if model.trim().is_empty() {
        return Bytes::copy_from_slice(payload);
    }
    if let Some(updated) = set_model(payload, model) {
        return Bytes::from(updated);
    }
    let mut changed = false;
    let lines: Vec<Vec<u8>> = payload
        .split(|b| *b == b'\n')
        .map(|line| {
            let (content, cr) = match line.strip_suffix(b"\r") {
                Some(content) => (content, &b"\r"[..]),
                None => (line, &b""[..]),
            };
            if let Some(json) = content.strip_prefix(b"data:")
                && let Some(updated) = set_model(go_trim_space(json), model)
            {
                changed = true;
                return [&b"data: "[..], &updated, cr].concat();
            }
            line.to_vec()
        })
        .collect();
    if changed {
        Bytes::from(lines.join(&b'\n'))
    } else {
        Bytes::copy_from_slice(payload)
    }
}

fn set_model(json: &[u8], model: &str) -> Option<Vec<u8>> {
    if !gj::valid(json) {
        return None;
    }
    let mut out = json.to_vec();
    let mut changed = false;
    for path in ["model", "message.model"] {
        if gj::get(&out, path).exists() && gj::set_str(&mut out, path, model) {
            changed = true;
        }
    }
    changed.then_some(out)
}

#[cfg(test)]
#[path = "kimi_tests.rs"]
mod tests;
