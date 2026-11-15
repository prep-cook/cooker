use super::*;

/// Upstream capture of a WebSocket turn (`codex_websockets_stream.go`): the request
/// frame with the dial's URL and headers before dialing, the handshake after a new dial
/// only, then every upstream message as received.
#[tokio::test]
async fn capture_records_websocket_turns_as_go_does() {
    use crate::codex_testkit::{Tap, Wiretap};
    let (_up, url) = upstream(vec![
        Act::Send(vec![CREATED, COMPLETED]),
        Act::Send(vec![CREATED, COMPLETED]),
    ])
    .await;
    let executor = CodexExecutor::new().unwrap();
    let cred = credential(&url);
    let ws_url = format!("{}/responses", url.replacen("http://", "ws://", 1));
    for continuation in [false, true] {
        let tap = Arc::new(Wiretap::default());
        let response = executor
            .execute_in_session(&cred, tapped(BODY, &tap), &Config::default(), &session(continuation))
            .await
            .unwrap();
        collect(response).await;
        let taps = tap.taps();
        let Tap::WsRequest {
            url,
            headers,
            body,
            account,
        } = &taps[0]
        else {
            panic!("{taps:?}");
        };
        assert_eq!(url, &ws_url);
        assert_eq!(gjson::get(body, "type").str(), "response.create", "the frame as sent");
        assert!(
            headers.contains(&("Authorization".into(), "Bearer sk-FAKE".into())),
            "{headers:?}"
        );
        assert_eq!(
            account,
            &["codex", &cred.id, &cred.label, "api_key", "sk-FAKE"].map(str::to_owned)
        );
        let mut want = vec![];
        if !continuation {
            want.push(Tap::WsHandshake(101));
        }
        want.extend([Tap::WsResponse(CREATED.into()), Tap::WsResponse(COMPLETED.into())]);
        assert_eq!(taps[1..], want[..], "continuation={continuation}");
    }
}

/// A refused upgrade is recorded as an HTTP attempt (`RecordAPIWebsocketUpgradeRejection`):
/// a GET to the HTTP form of the URL with Connection and Upgrade, the status and the body.
#[tokio::test]
async fn capture_records_a_refused_upgrade_as_an_http_attempt() {
    use crate::codex_testkit::{Tap, Wiretap};
    const BODY_401: &str = r#"{"error":{"message":"bad key","type":"invalid_request_error"}}"#;
    let (up, url) = upstream(vec![]).await;
    *up.reject.lock().unwrap() = Some((401, BODY_401));
    let tap = Arc::new(Wiretap::default());
    let _ = CodexExecutor::new()
        .unwrap()
        .execute_in_session(
            &credential(&url),
            tapped(BODY, &tap),
            &Config::default(),
            &session(false),
        )
        .await
        .err()
        .expect("401 handshake");
    let taps = tap.taps();
    assert!(matches!(&taps[0], Tap::WsRequest { .. }), "{taps:?}");
    let Tap::Request {
        url: upgrade,
        method,
        headers,
        body,
        ..
    } = &taps[1]
    else {
        panic!("{taps:?}");
    };
    assert_eq!(
        (upgrade.as_str(), method.as_str(), body.as_str()),
        (format!("{url}/responses").as_str(), "GET", "")
    );
    for pair in [("Connection", "Upgrade"), ("Upgrade", "websocket")] {
        assert!(headers.contains(&(pair.0.into(), pair.1.into())), "{headers:?}");
    }
    assert!(matches!(&taps[2], Tap::Metadata(401, _)), "{taps:?}");
    assert_eq!(
        taps[3..],
        [Tap::Chunk(BODY_401.into())],
        "no dial error after a rejection"
    );
}

/// A dial that never reaches an HTTP response is `api.websocket.error` stage `dial`.
#[tokio::test]
async fn capture_records_a_dial_failure() {
    use crate::codex_testkit::{Tap, Wiretap};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let tap = Arc::new(Wiretap::default());
    let error = CodexExecutor::new()
        .unwrap()
        .execute_in_session(
            &credential(&url),
            tapped(BODY, &tap),
            &Config::default(),
            &session(false),
        )
        .await
        .err()
        .expect("nothing listens");
    let taps = tap.taps();
    assert!(matches!(&taps[0], Tap::WsRequest { .. }), "{taps:?}");
    assert_eq!(
        taps[1..],
        [Tap::WsError(
            "dial".into(),
            String::from_utf8_lossy(&error.body).into_owned()
        )]
    );
}

/// An upstream `error` event is recorded as received, then as stage `upstream_error`
/// with the turn's error.
#[tokio::test]
async fn capture_records_an_upstream_error_event() {
    use crate::codex_testkit::{Tap, Wiretap};
    const ERROR: &str =
        r#"{"type":"error","status":400,"error":{"type":"invalid_request_error","message":"bad input"}}"#;
    let (_up, url) = upstream(vec![Act::Send(vec![ERROR])]).await;
    let tap = Arc::new(Wiretap::default());
    let response = CodexExecutor::new()
        .unwrap()
        .execute_in_session(
            &credential(&url),
            tapped(BODY, &tap),
            &Config::default(),
            &session(false),
        )
        .await
        .unwrap();
    let error = collect(response)
        .await
        .pop()
        .expect("the turn ends")
        .expect_err("with the upstream error");
    let taps = tap.taps();
    assert_eq!(
        taps[2..],
        [
            Tap::WsResponse(ERROR.into()),
            Tap::WsError(
                "upstream_error".into(),
                String::from_utf8_lossy(&error.body).into_owned()
            ),
        ]
    );
}
