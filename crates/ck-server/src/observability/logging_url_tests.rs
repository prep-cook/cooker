//! Log lines drop URL credentials whatever field carries them (owner ruling Q18).

use super::GoFormat;
use super::tests::SharedWriter;
use std::sync::{Arc, Mutex};
use tracing_subscriber::layer::SubscriberExt;

#[test]
fn log_lines_hide_url_credentials() {
    let written = Arc::new(Mutex::new(Vec::<u8>::new()));
    let writer = {
        let written = written.clone();
        move || SharedWriter(written.clone())
    };
    let subscriber = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .event_format(GoFormat)
            .with_writer(writer),
    );
    tracing::subscriber::with_default(subscriber, || {
        let proxy = "http://u:sk-review-secret@127.0.0.1:9/?code=sk-review-secret#sk-review-secret";
        tracing::warn!(proxy = %proxy, "proxy client failed");
        tracing::warn!("request to {proxy} failed");
    });
    let written = String::from_utf8(written.lock().unwrap().clone()).unwrap();
    assert!(!written.contains("sk-review-secret"), "{written}");
    assert_eq!(
        written.matches("http://127.0.0.1:9/?code=***#***").count(),
        2,
        "{written}"
    );
}
