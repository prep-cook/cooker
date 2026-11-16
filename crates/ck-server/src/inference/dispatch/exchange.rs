//! Routes whose client protocol has a request codec: Chat Completions, Anthropic Messages
//! and Gemini (D2-PLAN D2-3a). The route's request ingress decodes the body into the
//! exchange's request stream and closes it against the `"client"` capability before any
//! selection (§3.1), so a malformed request sends nothing. Each candidate then takes the
//! contract arm or the old arm (§3.2, §4).
//!
//! Nothing reaches the client before commitment (§5.1): these routes have no pre-commit
//! SSE keepalive, no non-stream keepalive and no bootstrap retry rounds. A contract-arm
//! answer is already the client's response; an old-arm answer or a failure is rendered by
//! the route.

use std::future::Future;
use std::sync::Arc;

use axum::response::Response;
use ck_core::exec::{ExecError, FailureScope};
use ck_stream::{Store, Writer};
use ck_translate::codec::event::{Direction, Identity, LocalId};
use ck_translate::codec::{self, ClientCapability, RequestIngress, RequestView};

use super::attempt::{Codec, stored};
use super::{Call, Done, Failure, Served, Trace, run_exchange, traced, upstream_headers};
use crate::http::egress::{Output, Protocol};
use crate::inference::streams::{Exchange, Owner};
use crate::runtime::state::Runtime;

/// Serves `call` on its codec: `render` answers an old-arm result or a failure in the route's
/// shape, and a committed contract-arm response goes out as it is.
pub async fn serve<F, Fut>(rt: &Arc<Runtime>, call: Call, output: Output, render: F) -> Response
where
    F: FnOnce(Result<Done, Failure>) -> Fut,
    Fut: Future<Output = Response>,
{
    let trace = Trace::with_request_id(crate::observability::api::current_request_id());
    let result = run(rt, call, output, &trace).await;
    let result = match result {
        Ok(Served::Committed(response)) => return traced(rt, &trace, response, None),
        Ok(Served::Old(done)) => Ok(done),
        Err(failure) => Err(failure),
    };
    let upstream = upstream_headers(&rt.config(), &result, trace.served_by_kimi(rt));
    traced(rt, &trace, render(result).await, upstream)
}

/// Decodes `call` and runs it through the model router and the selection loop. A router's
/// `fallback-models` decodes the client's request again into a new exchange.
pub(crate) async fn run(rt: &Arc<Runtime>, call: Call, output: Output, trace: &Trace) -> Result<Served, Failure> {
    let store = rt.streams().store().map_err(|error| Failure::Exec(stored(&error)))?;
    // Client errors answer before selection, and before a router's judge, with nothing sent.
    let mut first = Decoded::new(&store, &call, output)?;
    let mut fenced = Some(first.codec()?);
    let mut call = call;
    let mut picked = None;
    if call.forced_provider.is_none()
        && let Some(decision) = crate::inference::model_router::resolve(rt, &call).await
    {
        trace.set_routed_model(&decision.model);
        call.model = decision.model.clone();
        picked = Some(decision);
    }
    let original = picked.as_ref().is_some_and(|p| p.falls_back()).then(|| call.clone());
    loop {
        let mut again;
        let codec = match fenced.take() {
            Some(codec) => codec,
            None => {
                again = Decoded::new(&store, &call, output)?;
                again.codec()?
            }
        };
        if call.operation == ck_core::exec::Operation::Generate && rt.usage_queue().accepts() {
            trace.prompt_prefix(call.entry, &call.body);
        }
        let scope = picked.as_ref().map(|p| p.scope.clone());
        let result = run_exchange(rt, &rt.executors, call, trace, codec, scope)
            .await
            .map_err(|e| e.failure);
        if let (Ok(_), Some(picked)) = (&result, &picked) {
            picked.commit(rt);
        }
        // Only a failure before commitment moves to another model, never one a `stop` rule
        // ended.
        let next = match (&result, picked.as_mut(), &original) {
            (Err(failure), Some(picked), Some(original))
                if !trace.stopped() && crate::inference::model_router::fails_over(failure) =>
            {
                crate::inference::model_router::fall_back(rt, picked).map(|model| (model, original.clone()))
            }
            _ => None,
        };
        let Some((model, original)) = next else {
            return result;
        };
        trace.set_routed_model(&model);
        call = original;
        call.model = model;
    }
}

/// A request decoded into its exchange: the request stream holds the client request, and
/// the ingress holds the view every attempt re-fences.
struct Decoded {
    exchange: Option<Exchange>,
    ingress: Ingress,
    request_id: LocalId,
}

enum Ingress {
    Chat(codec::chat::request::Ingress),
    Anthropic(ck_translate::anthropic::request::Ingress),
    Gemini(ck_translate::gemini::request::Ingress),
}

impl Decoded {
    fn new(store: &Store, call: &Call, output: Output) -> Result<Self, Failure> {
        // The same owner a resume presents (`streams/resume.rs`).
        let owner = Owner {
            caller: call.caller.clone(),
            scope: None,
        };
        let mut exchange = Exchange::new(owner, output, store).map_err(|error| Failure::Exec(stored(&error)))?;
        let request_id = random_id();
        let identity = Identity {
            request_id,
            stream_id: random_id(),
            attempt: 0,
            direction: Direction::Request,
        };
        let request = &mut exchange.request;
        // The HTTP body limit (`http::body_limit`) already bounded the body, up to 128 MB on
        // the Anthropic routes; the ingress admits what it admitted.
        let limit = call.body.len().max(crate::MAX_REQUEST_BYTES) as u64;
        let operation = output.operation;
        let ingress = match output.protocol {
            Protocol::Chat => {
                codec::chat::request::Ingress::new(request, store, identity, operation, limit).map(Ingress::Chat)
            }
            Protocol::Anthropic => {
                ck_translate::anthropic::request::Ingress::new(request, store, identity, operation, limit)
                    .map(Ingress::Anthropic)
            }
            Protocol::Gemini => {
                ck_translate::gemini::request::Ingress::new(request, store, identity, operation, &call.model, limit)
                    .map(Ingress::Gemini)
            }
            Protocol::Responses | Protocol::Speech | Protocol::Images(_) => Err(codec::Error::Unsupported),
        };
        let mut decoded = Self {
            exchange: Some(exchange),
            ingress: ingress.map_err(refused)?,
            request_id,
        };
        decoded.advance(&call.body).map_err(refused)?;
        Ok(decoded)
    }

    fn advance(&mut self, body: &[u8]) -> codec::Result<()> {
        let request = &mut self.exchange.as_mut().ok_or(codec::Error::Closed)?.request;
        match &mut self.ingress {
            Ingress::Chat(ingress) => ingress.advance(request, body),
            Ingress::Anthropic(ingress) => ingress.advance(request, body),
            Ingress::Gemini(ingress) => ingress.advance(request, body),
        }
    }

    /// Closes the client fence (contract G8) and hands the exchange to the selection loop.
    fn codec(&mut self) -> Result<Codec<'_>, Failure> {
        let mut exchange = self.exchange.take().ok_or_else(|| refused(codec::Error::Closed))?;
        let view = match &mut self.ingress {
            Ingress::Chat(ingress) => fence(ingress, &mut exchange.request),
            Ingress::Anthropic(ingress) => fence(ingress, &mut exchange.request),
            Ingress::Gemini(ingress) => fence(ingress, &mut exchange.request),
        }
        .map_err(refused)?;
        Ok(Codec {
            exchange,
            view,
            request_id: self.request_id,
        })
    }
}

fn fence<'i>(ingress: &'i mut impl RequestIngress, request: &mut Writer) -> codec::Result<RequestView<'i>> {
    Ok(ingress.finish(request, &ClientCapability)?.view())
}

fn random_id() -> LocalId {
    LocalId::new(&uuid::Uuid::new_v4().simple().to_string()).expect("32 ASCII digits")
}

/// Contract §3.1: what request ingress refused, in client wording. Malformed input is 400 and
/// an oversized document or event 413; a storage failure follows contract §9, and anything
/// else is a gateway defect.
fn refused(error: codec::Error) -> Failure {
    let error = match error {
        codec::Error::Json
        | codec::Error::Utf8
        | codec::Error::DuplicateKey
        | codec::Error::Truncated
        | codec::Error::Schema
        | codec::Error::MaxTokensRequired
        | codec::Error::Unsupported => {
            ExecError::local(400, FailureScope::Request, format!("Invalid request: {error}"))
        }
        codec::Error::Limit | codec::Error::EventTooLarge => {
            ExecError::local(413, FailureScope::Request, format!("Invalid request: {error}"))
        }
        codec::Error::Storage(error) => stored(&error),
        _ => {
            tracing::error!(target: "ck_dispatch", %error, "request ingress defect");
            ExecError::local(500, FailureScope::Request, "stream.producer_lost")
        }
    };
    Failure::Exec(error)
}
