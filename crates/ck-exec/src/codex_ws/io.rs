use super::*;

/// tungstenite's reports of a peer that went away without a close frame, which
/// gorilla/websocket reads as close 1006 "unexpected EOF".
fn ends_without_close(error: &tokio_tungstenite::tungstenite::Error) -> bool {
    use tokio_tungstenite::tungstenite::Error;
    use tokio_tungstenite::tungstenite::error::ProtocolError;
    match error {
        Error::Protocol(ProtocolError::ResetWithoutClosingHandshake) => true,
        Error::Io(io) => io.kind() == std::io::ErrorKind::UnexpectedEof,
        _ => false,
    }
}

/// The socket's single reader. Exits on error, idle timeout, or abort.
///
/// Like Go's read deadline, `idle` starts when the read for the next application message
/// begins; control frames answered meanwhile do not extend it.
pub(crate) async fn read_loop(
    mut stream: SplitStream<WebSocket>,
    session: Weak<Session>,
    conn: Arc<Upstream>,
    idle: Duration,
) {
    let error = 'read: loop {
        let deadline = tokio::time::Instant::now() + idle;
        let text = loop {
            let message = match tokio::time::timeout_at(deadline, stream.next()).await {
                Err(_) => break 'read transport("codex websockets executor: read idle timeout"),
                // gorilla/websocket reports a connection that ends without a close frame
                // as close 1006; Go surfaces its text, which marks a lifecycle failure.
                Ok(None) => break 'read transport(&close_error_text(1006, "unexpected EOF")),
                Ok(Some(Err(error))) if ends_without_close(&error) => {
                    break 'read transport(&close_error_text(1006, "unexpected EOF"));
                }
                Ok(Some(Err(_))) => break 'read transport("codex websockets executor: read failed"),
                Ok(Some(Ok(message))) => message,
            };
            match message {
                Message::Text(text) => break text.as_str().to_owned(),
                Message::Binary(_) => break 'read transport(UNEXPECTED_BINARY),
                Message::Close(frame) => {
                    if let Some(frame) = frame.as_ref().filter(|f| u16::from(f.code) == 1009) {
                        break 'read closed_too_big(frame.reason.as_str());
                    }
                    let text = match frame {
                        Some(frame) => close_error_text(u16::from(frame.code), frame.reason.as_str()),
                        None => close_error_text(1005, ""),
                    };
                    break 'read transport(&text);
                }
                Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => continue,
            }
        };
        // The live response and terminal IDs are recorded before delivery: the handler may
        // still be forwarding the turn after its read channel was released, and a cancel
        // must find a response the turn has not read yet.
        let terminal = terminal_response_id(text.trim(), conn.target.accepts_done);
        let created = created_response_id(text.trim());
        let active = {
            let mut link = conn.link.lock().expect("link");
            if let Some(id) = created {
                link.live = Some(id);
                link.awaiting = false;
            }
            if let Some(id) = terminal {
                link.live = None;
                link.terminal.insert(id);
            }
            link.active.clone()
        };
        if let Some(tx) = active {
            // Waits while the turn is busy: backpressure instead of unbounded queueing.
            let _ = tx.send(Read::Text(text)).await;
        }
    };
    let active = {
        let mut link = conn.link.lock().expect("link");
        link.lost = Some(error.clone());
        link.active.clone()
    };
    // The session records the loss before the turn reads its error, so a handler that
    // sees the turn fail also finds the loss (Go's notifier closes the client first).
    match session.upgrade() {
        Some(session) => session.invalidate(&conn),
        None => conn.deactivate(),
    }
    if let Some(tx) = active {
        let _ = tx.try_send(Read::Failed(turn_error(&error)));
    }
}

/// Why [`upgrade`] refused a 101 answer.
pub(crate) enum UpgradeError {
    /// gorilla `Dial`'s checks failed (Upgrade, Connection, Sec-WebSocket-Accept) or the
    /// connection could not be taken over.
    Handshake,
    /// The permessage-deflate answer does not match the offer.
    Compression,
}

/// gorilla `Dialer` with `EnableCompression`: offers permessage-deflate on the request
/// and returns the `Sec-WebSocket-Key` to send with it. Shared by the Codex and xAI
/// WebSocket executors (Go's xAI dialer also enables compression).
pub(crate) fn offer_compression(headers: &mut HeaderMap) -> String {
    headers.insert(
        http::header::SEC_WEBSOCKET_EXTENSIONS,
        http::HeaderValue::from_static(deflate::OFFER),
    );
    tokio_tungstenite::tungstenite::handshake::client::generate_key()
}

/// gorilla `Dial`'s checks on a 101 answer, then its permessage-deflate agreement;
/// wraps the upgraded connection for [`Upstream`] and [`read_loop`].
pub(crate) async fn upgrade(
    res: &mut wreq::ws::WebSocketResponse,
    key: &str,
    handshake: &HeaderMap,
) -> Result<WebSocket, UpgradeError> {
    let accept = tokio_tungstenite::tungstenite::handshake::derive_accept_key(key.as_bytes());
    if !token_list_contains(handshake, http::header::UPGRADE, "websocket")
        || !token_list_contains(handshake, http::header::CONNECTION, "upgrade")
        || handshake.get(http::header::SEC_WEBSOCKET_ACCEPT).map(|v| v.as_bytes()) != Some(accept.as_bytes())
    {
        return Err(UpgradeError::Handshake);
    }
    let compressed = deflate::negotiated(handshake).map_err(|()| UpgradeError::Compression)?;
    let response = std::mem::replace(&mut **res, wreq::Response::from(http::Response::new(Vec::<u8>::new())));
    let upgraded = response.upgrade().await.map_err(|_| UpgradeError::Handshake)?;
    let config = WebSocketConfig::default()
        .max_frame_size(Some(MAX_UPSTREAM_MESSAGE))
        .max_message_size(Some(MAX_UPSTREAM_MESSAGE));
    Ok(WebSocketStream::from_raw_socket(
        Inflate::new(upgraded, compressed, MAX_UPSTREAM_MESSAGE),
        Role::Client,
        Some(config),
    )
    .await)
}

/// gorilla `tokenListContainsValue`: a comma-separated token list holds `value`
/// (ASCII case-insensitive).
fn token_list_contains(headers: &HeaderMap, name: http::HeaderName, value: &str) -> bool {
    headers.get_all(name).iter().any(|v| {
        v.to_str().is_ok_and(|v| {
            v.split(',')
                .any(|t| t.trim_matches([' ', '\t']).eq_ignore_ascii_case(value))
        })
    })
}

/// The id of a `response.created`.
fn created_response_id(payload: &str) -> Option<String> {
    (gjson::get(payload, "type").str() == "response.created")
        .then(|| gjson::get(payload, "response.id").str().to_owned())
}
