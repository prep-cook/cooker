//! A client delivery and the producer: a committed response never holds the producer back,
//! and a client egress reader waiting at the published end demands publication.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_committed_response_never_holds_back_the_producer() {
    #[derive(Clone, Copy, Debug)]
    enum Client {
        /// Reads nothing more after commitment.
        Throttled,
        /// The first write fails: the connection is gone.
        Disconnected,
    }
    let fixture = Fixture::new(|_| {});
    let app = fixture.app();
    for client in [Client::Throttled, Client::Disconnected] {
        let (result, mut live, _) = delivered(&fixture, output(Protocol::Chat, Framing::Sse), async |live, _| {
            live.send(chunk(r#"{"role":"assistant","content":"hel"}"#, "null"));
        })
        .await;
        let Ok(response) = result else {
            panic!("{client:?}: the attempt did not commit");
        };
        live.id = StreamId::parse(response.headers()["x-ck-stream-id"].as_bytes()).unwrap();
        let held = match client {
            Client::Throttled => Some(response),
            Client::Disconnected => None,
        };
        answer_rest(&mut live);
        assert_eq!(
            live.stopped().await,
            Terminal::Done,
            "{client:?}: the upstream finishes"
        );
        let (text, clean) = body(resume(&app, &live, "v1.e.0").await).await;
        assert!(
            clean && text.contains("lo") && text.contains("[DONE]"),
            "{client:?}: {text}"
        );
        if let Some(response) = held {
            assert_eq!(body(response).await, (text, true), "{client:?}");
        }
    }
}

/// Resolves once a client egress reader of `writer`'s stream begins a wait after `served`.
async fn demanded(writer: &Writer, served: u64) -> Option<u64> {
    tokio::time::timeout(Duration::from_secs(5), writer.demand(served))
        .await
        .ok()
}

/// A live delivery and a resume wait at the published end as client egress readers, so the
/// producer may publish what it holds at once (`Writer::demand`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_deliveries_demand_publication_while_they_wait() {
    let fixture = Fixture::new(|_| {});
    let app = fixture.app();
    let mut served = 0;
    let (result, mut live, _) = delivered(
        &fixture,
        output(Protocol::Chat, Framing::Sse),
        async |live, delivery| {
            assert!(waiting(delivery).await);
            served = demanded(&live.response, 0)
                .await
                .expect("the live delivery waits as client egress");
            live.send(chunk(r#"{"role":"assistant","content":"hel"}"#, "null"));
        },
    )
    .await;
    let Ok(response) = result else {
        panic!("the attempt did not commit");
    };
    live.id = StreamId::parse(response.headers()["x-ck-stream-id"].as_bytes()).expect("the stream id");
    drop(response);
    let reading = tokio::spawn(body(resume(&app, &live, zero(Framing::Sse)).await));
    assert!(
        demanded(&live.response, served).await.is_some(),
        "the resume waits as client egress"
    );
    answer_rest(&mut live);
    let (text, clean) = reading.await.unwrap();
    assert!(clean && text.contains("hel"), "{text}");
}
