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
                    // Go returns a plain statusErr for 426 on downstream WebSockets.
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

    /// One Responses turn over the session's upstream socket (`ExecuteStream` on
    /// `CodexWebsocketsExecutor` with a downstream WebSocket).
    pub(crate) async fn stream_ws(
        &self,
        view: &View<'_>,
        settings: &Settings,
        req: ExecRequest,
        exec_session: &ExecSession,
        cfg: &Config,
    ) -> Result<ExecResponse, ExecError> {
        let model = request::base_model(&req.model).to_owned();
        let shaped = request::shape(&req, view, settings, Call::Websocket)?;
        let (optimized, conflict) = (shaped.optimized, shaped.conflict);
        let (body, replay) = Replay::apply(&self.replay, &req, &view.credential.id, shaped.body);
        let (body, cache) = request::prompt_cache(&req, body, Some(&exec_session.id), true);
        let body = turn_body(view, &req, settings, &shaped.finalize, body);
        crate::codex::report_request(&req, ck_core::format::Format::Codex, &body);
        let native = request::is_native(&req);
        let headers = request::ws_headers(view, settings, &req.headers, &body, &model, cache.as_deref(), native);
        let target = Target::new(view.credential, ws_url(&format!("{}/responses", view.base_url))?, cfg);
        let session = self.ws.session(&exec_session.id);
        // `WebsocketInputFromContext(ctx) != nil && cfg.Codex.ResponseSteering`.
        let steering = settings
            .response_steering
            .then(|| session.steering())
            .flatten()
            .map(|input| {
                let initial =
                    duplex::Prepared::new(&body, &req.original_body, optimized, conflict, native, replay.clone());
                (input, initial)
            });
        let guard = session.turn.clone().lock_owned().await;
        let frame = request_frame(body);
        let wire = crate::codex_capture::Wire::new(req.capture(), view.credential);
        wire.ws_request(&target.url, &headers, frame.as_bytes());
        let opened = Opening {
            session: &session,
            target: &target,
            headers: &headers,
            model: &model,
            cooling: settings.model_level_cooling,
            wire: &wire,
        }
        .send(self, frame, exec_session.continuation, optimized, conflict)
        .await?;
        opened.conn.note_multi_agent(optimized, conflict);
        let observed = opened.handshake.clone().unwrap_or_default();
        if !observed.is_empty() {
            self.quota_observe(&view.credential.id, &model, &observed);
        }
        let turn = Turn {
            rx: opened.rx,
            conn: opened.conn,
            session,
            _turn: Some(guard),
            items: OutputItems::default(),
            native,
            cooling: settings.model_level_cooling,
            quota: self.quota_handle(),
            credential: view.credential.id.clone(),
            observed,
            restore: opened.restore,
            usage: req.usage.clone(),
            model: model.clone(),
            replay,
            wire,
        };
        let stream = if let Some((input, initial)) = steering {
            // Steering owns the connection; bootstrap buffering does not apply.
            duplex::Duplex::start(turn, input, initial, req, view, settings, &exec_session.id).await
        } else if settings.bootstrap_buffering {
            bootstrap(turn, settings.bootstrap_timeout, opened.started).await?
        } else {
            rest(turn)
        };
        Ok(ExecResponse {
            status: 200,
            headers: opened.handshake.unwrap_or_default(),
            body: ResponseBody::Stream(stream),
        })
    }
}

struct Opening<'a> {
    session: &'a Arc<Session>,
    target: &'a Target,
    headers: &'a HeaderMap,
    model: &'a str,
    cooling: bool,
    wire: &'a crate::codex_capture::Wire,
}

struct Opened {
    conn: Arc<Upstream>,
    handshake: Option<HeaderMap>,
    rx: mpsc::Receiver<Read>,
    restore: bool,
    started: Instant,
}

impl Opening<'_> {
    async fn send(
        &self,
        executor: &CodexExecutor,
        frame: String,
        continuation: bool,
        optimized: bool,
        conflict: bool,
    ) -> Result<Opened, ExecError> {
        let (mut conn, mut handshake) = if continuation {
            match self.session.current().filter(|c| c.target.serves(self.target)) {
                Some(conn) => (conn, None),
                None => return Err(ExecError::replay_required()),
            }
        } else {
            executor
                .ensure(
                    self.session,
                    self.target,
                    self.headers,
                    self.model,
                    self.cooling,
                    Some(self.wire),
                )
                .await?
        };
        if let Some(handshake) = &handshake {
            self.wire.ws_handshake(101, handshake);
        }
        let started = Instant::now();
        let mut rx = conn.activate();
        let mut restore = conn.restores(optimized, conflict);
        if let Err(error) = conn.send(frame.clone()).await {
            self.wire.ws_exec_error("send", &error);
            // Request-scoped failures (413) never retry; continuations require replay.
            let retry = error.scope != FailureScope::Request;
            if continuation {
                self.session.invalidate(&conn, &error, false);
                return Err(if retry { ExecError::replay_required() } else { error });
            }
            self.session.invalidate(&conn, &error, true);
            if !retry {
                return Err(error);
            }
            // Retry once: upstream may have closed the pooled socket between turns.
            let (fresh, fresh_handshake) = executor
                .ensure(self.session, self.target, self.headers, self.model, self.cooling, None)
                .await
                .inspect_err(|error| {
                    self.wire.ws_exec_error("dial_retry", error);
                })?;
            rx = fresh.activate();
            restore = fresh.restores(optimized, conflict);
            self.wire.ws_request(&self.target.url, self.headers, frame.as_bytes());
            if let Some(handshake) = &fresh_handshake {
                self.wire.ws_handshake(101, handshake);
            }
            if let Err(error) = fresh.send(frame).await {
                self.wire.ws_exec_error("send_retry", &error);
                self.session.invalidate(&fresh, &error, true);
                return Err(error);
            }
            conn = fresh;
            handshake = fresh_handshake;
        }
        Ok(Opened {
            conn,
            handshake,
            rx,
            restore,
            started,
        })
    }
}
