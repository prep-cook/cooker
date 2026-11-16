//! A one-shot media request decoded into its exchange (E-PLAN §2.3): the route's codec writes
//! the client's request into the exchange's request stream, the client fence closes it
//! (contract G8), and the selection loop serves it on the execution contract.

use std::future::Future;

use axum::response::Response;
use ck_core::exec::Caller;
use ck_stream::{Store, Writer};
use ck_translate::codec::document::Document;
use ck_translate::codec::event::LocalId;
use ck_translate::codec::media::Registry;
use ck_translate::codec::{ClientCapability, RequestFence, RequestView};

use crate::Runtime;
use crate::http::egress::Output;
use crate::inference::dispatch::{self, Call, Done, Failure};
use crate::inference::streams::{Exchange, Owner};

pub(crate) struct Decoded {
    exchange: Exchange,
    document: Document,
    registry: Registry,
    request_id: LocalId,
}

impl Decoded {
    /// A new exchange for `caller` answered as `output`, whose request `decode` writes into the
    /// request stream. A document the client fence refuses fails as `refused` maps it.
    pub(crate) fn new(
        rt: &Runtime,
        caller: &Caller,
        output: Output,
        decode: impl FnOnce(&mut Writer, &Store) -> Result<Document, Failure>,
        refused: impl Fn(ck_translate::codec::Error) -> Failure,
    ) -> Result<Self, Failure> {
        let stored = |error: ck_stream::Error| Failure::Exec(dispatch::stored(&error));
        let store = rt.streams().store().map_err(stored)?;
        // The same owner a resume presents (`streams/resume.rs`).
        let owner = Owner {
            caller: caller.clone(),
            scope: None,
        };
        let mut exchange = Exchange::new(owner, output, &store).map_err(stored)?;
        let document = decode(&mut exchange.request, &store)?;
        let registry = Registry::new(&mut exchange.request).map_err(&refused)?;
        let view = RequestView {
            document: &document,
            media: &registry,
            operation: output.operation,
        };
        RequestFence::default().end(view, &ClientCapability).map_err(&refused)?;
        Ok(Self {
            exchange,
            document,
            registry,
            request_id: LocalId::new(&uuid::Uuid::new_v4().simple().to_string()).expect("32 ASCII digits"),
        })
    }

    /// Serves the request ([`dispatch::serve_exchange`]); `render` answers an old-arm result or a
    /// failure.
    pub(crate) async fn serve<F, Fut>(self, rt: &std::sync::Arc<Runtime>, call: Call, render: F) -> Response
    where
        F: FnOnce(Result<Done, Failure>) -> Fut,
        Fut: Future<Output = Response>,
    {
        let Self {
            exchange,
            document,
            registry,
            request_id,
        } = self;
        let operation = exchange.output.operation;
        let codec = dispatch::Codec {
            exchange,
            view: RequestView {
                document: &document,
                media: &registry,
                operation,
            },
            request_id,
        };
        dispatch::serve_exchange(rt, call, codec, render).await
    }
}
