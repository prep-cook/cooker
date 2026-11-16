//! Requests the Gemini fence refuses before any upstream request.

use super::*;

#[tokio::test]
async fn compact_is_refused_at_the_fence() {
    let streams = Streams::new();
    let mut writer = streams.request();
    let mut ingress = responses::request::Ingress::new(
        &mut writer,
        &streams.store,
        identity(Direction::Request),
        Operation::Compact,
        LIMIT,
    )
    .unwrap();
    ingress
        .advance(&mut writer, br#"{"model":"gpt-x","input":"hi"}"#)
        .unwrap();
    let exchange = Exchange::new(credential("gemini", "http://127.0.0.1:1", KEY), MODEL, "/client");
    let attempt = exchange.attempt(&streams.store);
    assert!(
        ingress
            .finish(&mut writer, &ck_translate::codec::ClientCapability)
            .and_then(|fenced| executors().refence(&attempt, fenced.view()))
            .is_err()
    );
}

/// Closes the client fence for the Gemini destination, without an upstream.
fn fenced<I: RequestIngress>(
    streams: &Streams,
    mut writer: Writer,
    mut ingress: I,
    body: &str,
) -> Result<(), codec::Error> {
    let exchange = Exchange::new(credential("gemini", "http://127.0.0.1:1", KEY), MODEL, "/client");
    ingress.advance(&mut writer, body.as_bytes())?;
    ingress
        .finish(&mut writer, &ck_translate::codec::ClientCapability)
        .and_then(|fenced| executors().refence(&exchange.attempt(&streams.store), fenced.view()))
        .map(drop)
}

fn chat_fence(body: &str) -> Result<(), codec::Error> {
    let streams = Streams::new();
    let mut writer = streams.request();
    let id = identity(Direction::Request);
    let ingress = chat::request::Ingress::new(&mut writer, &streams.store, id, Operation::Generate, LIMIT).unwrap();
    fenced(&streams, writer, ingress, body)
}

/// Gemini's log probabilities have no Chat mapping yet, so a request for them is refused.
#[test]
fn chat_log_probabilities_are_refused() {
    let messages = r#""messages":[{"role":"user","content":"hi"}]"#;
    for controls in [
        r#""logprobs":true"#,
        r#""logprobs":true,"top_logprobs":3"#,
        r#""top_logprobs":0"#,
    ] {
        let refused = chat_fence(&format!(r#"{{"model":"m",{controls},{messages}}}"#));
        assert!(matches!(refused, Err(codec::Error::Unsupported)), "{controls}");
    }
    for controls in [r#""logprobs":false"#, r#""logprobs":null"#, r#""top_logprobs":null"#] {
        chat_fence(&format!(r#"{{"model":"m",{controls},{messages}}}"#)).expect(controls);
    }
}

/// A Responses client asks for log probabilities through `include`, which has no Gemini form.
#[test]
fn responses_log_probabilities_are_refused() {
    let streams = Streams::new();
    let mut writer = streams.request();
    let id = identity(Direction::Request);
    let ingress =
        responses::request::Ingress::new(&mut writer, &streams.store, id, Operation::Generate, LIMIT).unwrap();
    let refused = fenced(
        &streams,
        writer,
        ingress,
        r#"{"model":"m","input":"hi","include":["message.output_text.logprobs"]}"#,
    );
    assert!(matches!(refused, Err(codec::Error::Unsupported)), "{refused:?}");
}
