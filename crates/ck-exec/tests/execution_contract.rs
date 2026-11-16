//! The execution contract compiles as frozen: an executor can hold the request handle and
//! both of an attempt's writers across an await and still return a `Send` future, and the
//! handle, the store and every protocol's ingress can move between runtime threads.

use ck_core::exec::ExecError;
use ck_exec::types::{Attempt, Counted, Execute, Request, Streams, Upstream};
use ck_stream::{Store, Writer};
use ck_translate::codec::chat::{request::ChatCapability, response::Ingress};

struct Probe;

impl Execute for Probe {
    type Capability = ChatCapability;
    type Ingress = Ingress;

    fn capability(&self, _attempt: &Attempt<'_>) -> ChatCapability {
        ChatCapability
    }

    async fn execute(
        &self,
        attempt: &Attempt<'_>,
        request: &mut Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<Ingress>, ExecError> {
        tokio::task::yield_now().await;
        let _held = (
            attempt.model,
            attempt.store.used_frames(),
            request.validated().view(),
            streams.request.id(),
            streams.response.id(),
        );
        Err(ExecError::replay_required())
    }

    async fn count_tokens(
        &self,
        _attempt: &Attempt<'_>,
        _request: &mut Request<'_>,
        _streams: Streams<'_>,
    ) -> Result<Counted<Ingress>, ExecError> {
        tokio::task::yield_now().await;
        Ok(Counted::Local { input_tokens: 1 })
    }

    async fn compact(
        &self,
        attempt: &Attempt<'_>,
        request: &mut Request<'_>,
        streams: Streams<'_>,
    ) -> Result<Upstream<Ingress>, ExecError> {
        self.execute(attempt, request, streams).await
    }
}

fn sends<T: Send>(_: &T) {}

/// A server task awaiting an attempt while it keeps the request handle stays `Send`.
#[allow(dead_code, reason = "type-checked, never run")]
fn server_task(attempt: &Attempt<'_>, request: &mut Request<'_>, mut upload: Writer, mut response: Writer) {
    let task = async move {
        let streams = Streams {
            request: &mut upload,
            response: &mut response,
        };
        let upstream = Probe.execute(attempt, request, streams).await;
        let _view = request.validated().view();
        upstream.map(|upstream| upstream.status)
    };
    sends(&task);
}

#[test]
fn contract_types_cross_threads() {
    fn send<T: Send>() {}
    fn sync<T: Sync>() {}
    sync::<Request<'static>>();
    sync::<Attempt<'static>>();
    sync::<Store>();
    send::<Writer>();
    send::<ck_translate::codec::chat::request::Ingress>();
    send::<ck_translate::anthropic::request::Ingress>();
    send::<ck_translate::responses::request::Ingress>();
    send::<ck_translate::gemini::request::Ingress>();
    send::<ck_translate::codec::chat::response::Ingress>();
    send::<ck_translate::anthropic::response::Ingress>();
    send::<ck_translate::responses::response::Ingress>();
    send::<ck_translate::gemini::response::Ingress>();
}
