use super::*;

impl CodexExecutor {
    /// Opens (or reuses) the session's socket for `target`. Returns the handshake
    /// headers when a new socket was dialed (`ensureUpstreamConn`).
    /// `wire` records a refused upgrade and a dial failure (the first dial of a turn;
    /// Go's retry records only `dial_retry`).
    pub(super) async fn ensure(
        &self,
        session: &Arc<Session>,
        target: &Target,
        headers: &HeaderMap,
        model: &str,
        model_level_cooling: bool,
        wire: Option<&crate::codex_capture::Wire>,
    ) -> Result<(Arc<Upstream>, Option<HeaderMap>), ExecError> {
        if let Some(current) = session.current() {
            if current.target.serves(target) {
                return Ok((current, None));
            }
            // target_changed: another credential, URL or proxy now serves this session.
            let mut slot = session.conn.lock().expect("session conn");
            if slot.as_ref().is_some_and(|c| Arc::ptr_eq(c, &current)) {
                *slot = None;
            }
            drop(slot);
            current.shutdown();
        }
        let (socket, handshake) = self.dial(target, headers, model, model_level_cooling, wire).await?;
        let (sink, stream) = socket.split();
        let conn = Upstream::new(target.clone(), sink, self.ws.write);
        // Publish before the reader runs, so a reader that fails at once still finds its
        // socket current and invalidates it; holding the handle slot keeps a concurrent
        // shutdown from missing the abort handle.
        let mut reader = conn.reader.lock().expect("reader handle");
        session.publish(conn.clone());
        let task = tokio::spawn(read_loop(stream, Arc::downgrade(session), conn.clone(), self.ws.idle));
        *reader = Some(task.abort_handle());
        drop(reader);
        Ok((conn, Some(handshake)))
    }

    async fn dial(
        &self,
        target: &Target,
        request: &HeaderMap,
        model: &str,
        model_level_cooling: bool,
        wire: Option<&crate::codex_capture::Wire>,
    ) -> Result<(WebSocket, HeaderMap), ExecError> {
        // `newProxyAwareWebsocketDialer`: Go's standard dialer, environment proxies
        // included when none is configured, with `EnableCompression`.
        let mut headers = request.clone();
        let key = offer_compression(&mut headers);
        let builder = self
            .transport
            .standard(&target.proxy)
            .websocket(&target.url)
            .headers(headers)
            .accept_key(key.clone());
        let mut rejected = false;
        let attempt = async {
            let mut res = builder
                .send()
                .await
                .map_err(|_| transport("codex websockets executor: dial failed"))?;
            let status = res.status().as_u16();
            let handshake = res.headers().clone();
            if status != 101 {
                let inner = std::mem::replace(&mut *res, wreq::Response::from(http::Response::new(Vec::<u8>::new())));
                let mut body = Vec::new();
                let mut stream = inner.bytes_stream();
                while let Some(Ok(chunk)) = stream.next().await {
                    body.extend_from_slice(&chunk[..chunk.len().min(MAX_HANDSHAKE_BODY - body.len())]);
                    if body.len() >= MAX_HANDSHAKE_BODY {
                        break;
                    }
                }
                self.quota_observe(&target.credential, model, &handshake);
                rejected = true;
                if let Some(wire) = wire {
                    wire.upgrade_rejection(&target.url, request, status, &handshake, &body);
                }
                if status == 426 {
                    // A plain status error: the turn falls back to HTTP on it (`codex_ws::turns`).
                    let text = String::from_utf8_lossy(&body).into_owned();
                    return Err(response::status_error_raw(status, &text, handshake, false));
                }
                return Err(response::status_error(status, &body, handshake, model_level_cooling));
            }
            // ponytail: Go reports a failed negotiation (and a bad handshake) as a status
            // error carrying 101; here both are transport failures.
            let socket = upgrade(&mut res, &key, &handshake).await.map_err(|e| match e {
                UpgradeError::Handshake => transport("codex websockets executor: websocket handshake failed"),
                UpgradeError::Compression => {
                    transport("codex websockets executor: websocket: invalid compression negotiation")
                }
            })?;
            Ok((socket, handshake))
        };
        let dialed = tokio::time::timeout(HANDSHAKE_TIMEOUT, attempt)
            .await
            .unwrap_or_else(|_| Err(transport("codex websockets executor: handshake timed out")));
        // A refused upgrade was recorded as an HTTP attempt; anything else is a dial error.
        if let (Err(error), Some(wire)) = (&dialed, wire)
            && !rejected
        {
            wire.ws_exec_error("dial", error);
        }
        dialed
    }

    fn quota_observe(&self, credential: &str, model: &str, headers: &HeaderMap) {
        self.quota().observe(credential, model, headers);
    }
}
