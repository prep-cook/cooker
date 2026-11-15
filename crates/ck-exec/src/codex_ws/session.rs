use super::*;

impl Upstream {
    /// A socket for `target`, not yet published to a session and without a reader.
    pub(crate) fn new(target: Target, sink: SplitSink<WebSocket, Message>, write: WriteLimit) -> Arc<Self> {
        Arc::new(Self {
            target,
            write,
            sink: tokio::sync::Mutex::new(sink),
            link: Mutex::default(),
            reader: Mutex::default(),
            multi_agent: Default::default(),
        })
    }

    /// `!conflict && (optimized || isMultiAgentV2Optimized(conn))`.
    pub(super) fn restores(&self, optimized: bool, conflict: bool) -> bool {
        !conflict && (optimized || self.multi_agent.load(std::sync::atomic::Ordering::Acquire))
    }

    /// `setMultiAgentV2Optimized` after a request that optimized or conflicted.
    pub(super) fn note_multi_agent(&self, optimized: bool, conflict: bool) {
        if optimized || conflict {
            self.multi_agent
                .store(optimized && !conflict, std::sync::atomic::Ordering::Release);
        }
    }

    pub(crate) fn activate(&self) -> mpsc::Receiver<Read> {
        let (tx, rx) = mpsc::channel(TURN_BUFFER);
        let mut link = self.link.lock().expect("link");
        match &link.lost {
            Some(error) => {
                let _ = tx.try_send(Read::Failed(turn_error(error)));
            }
            None => link.active = Some(tx),
        }
        rx
    }

    pub(crate) fn deactivate(&self) {
        self.link.lock().expect("link").active.take();
    }

    /// `writeCodexWebsocketMessage` + `mapCodexWebsocketWriteError`: a write after the
    /// upstream closed with 1009 reports the request-scoped 413 instead. A write that
    /// outlasts [`WriteLimit`] fails like any other write: the caller invalidates the
    /// socket, so a partly written frame is never followed by another. Cost: one timer
    /// entry while a write is in flight.
    pub(crate) async fn send(&self, frame: String) -> Result<(), ExecError> {
        let len = frame.len();
        self.write_message(Message::text(frame), len).await
    }

    /// [`Self::send`] for a client's own bytes: one text frame holding them unchanged,
    /// UTF-8 or not, as gorilla writes a `TextMessage`.
    pub(crate) async fn send_raw(&self, payload: Bytes) -> Result<(), ExecError> {
        use tokio_tungstenite::tungstenite::protocol::frame::{
            Frame,
            coding::{Data, OpCode},
        };
        let len = payload.len();
        self.write_message(
            Message::Frame(Frame::message(payload, OpCode::Data(Data::Text), true)),
            len,
        )
        .await
    }

    async fn write_message(&self, message: Message, len: usize) -> Result<(), ExecError> {
        let deadline = self.write.deadline(len);
        let sent = tokio::time::timeout(deadline, async { self.sink.lock().await.send(message).await }).await;
        match sent {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(write_error(
                self.link.lock().expect("link").lost.as_ref(),
                "codex websockets executor: write failed",
            )),
            Err(_) => Err(write_error(
                self.link.lock().expect("link").lost.as_ref(),
                "codex websockets executor: write timed out",
            )),
        }
    }

    /// Stops the reader and closes the socket. Idempotent.
    pub(crate) fn shutdown(self: &Arc<Self>) {
        if let Some(reader) = self.reader.lock().expect("reader handle").take() {
            reader.abort();
        }
        self.deactivate();
        let this = self.clone();
        tokio::spawn(async move {
            let _ = tokio::time::timeout(Duration::from_secs(1), async {
                let mut sink = this.sink.lock().await;
                let _ = sink.send(Message::Close(None)).await;
                let _ = sink.close().await;
            })
            .await;
        });
    }
}
impl Session {
    /// Makes `conn` the session's socket.
    pub(crate) fn publish(&self, conn: Arc<Upstream>) {
        let mut slot = self.conn.lock().expect("session conn");
        conn.target
            .credential
            .clone_into(&mut self.credential.lock().expect("session credential"));
        *slot = Some(conn);
    }

    pub(crate) fn current(&self) -> Option<Arc<Upstream>> {
        self.conn.lock().expect("session conn").clone()
    }

    pub(super) fn steering(&self) -> Option<Arc<SteeringInput>> {
        self.steering.lock().expect("session steering").clone()
    }

    /// `invalidateUpstreamConn`: only the session's current socket is dropped, so a stale
    /// reader cannot tear down its replacement. `notify` tells the downstream handler.
    pub(crate) fn invalidate(&self, conn: &Arc<Upstream>, error: &ExecError, notify: bool) {
        {
            let mut current = self.conn.lock().expect("session conn");
            if !current.as_ref().is_some_and(|c| Arc::ptr_eq(c, conn)) {
                return;
            }
            *current = None;
            if notify {
                #[cfg(test)]
                if let Some((entered, resume)) = self.before_notify.lock().expect("notify hook").take() {
                    let _ = entered.send(());
                    let _ = resume.recv();
                }
                self.closed.send_if_modified(|slot| {
                    let first = slot.is_none();
                    if first {
                        *slot = Some(error.clone());
                    }
                    first
                });
            }
        }
        conn.shutdown();
    }
}
impl Default for Pool {
    fn default() -> Self {
        Self {
            sessions: Mutex::default(),
            idle: IDLE_TIMEOUT,
            write: WRITE_LIMIT,
        }
    }
}

impl Pool {
    #[cfg(test)]
    pub fn with_idle(idle: Duration) -> Self {
        Self {
            idle,
            ..Self::default()
        }
    }

    #[cfg(test)]
    pub(crate) fn with_write(write: WriteLimit) -> Self {
        Self {
            write,
            ..Self::default()
        }
    }

    pub(crate) fn session(&self, id: &str) -> Arc<Session> {
        self.sessions
            .lock()
            .expect("sessions")
            .entry(id.to_owned())
            .or_insert_with(|| {
                Arc::new(Session {
                    turn: Arc::default(),
                    conn: Mutex::default(),
                    closed: watch::channel(None).0,
                    #[cfg(test)]
                    before_notify: Mutex::default(),
                    steering: Mutex::default(),
                    credential: Mutex::default(),
                })
            })
            .clone()
    }

    /// The session, without creating one.
    fn get(&self, id: &str) -> Option<Arc<Session>> {
        self.sessions.lock().expect("sessions").get(id).cloned()
    }

    /// `WithWebsocketInput`: binds the downstream connection's frames to its session.
    pub fn attach_steering(&self, id: &str, input: SteeringInput) {
        *self.session(id).steering.lock().expect("session steering") = Some(Arc::new(input));
    }

    /// `CloseExecutionSession`: the downstream connection ended.
    pub fn close(&self, id: &str) {
        let session = self.sessions.lock().expect("sessions").remove(id);
        if let Some(conn) = session.and_then(|s| s.conn.lock().expect("session conn").take()) {
            conn.shutdown();
        }
    }

    /// `closeCodexWebsocketSessionsInStoreForAuthID`: ends the sessions whose last socket
    /// was dialed for `credential` (a removed credential), like [`Self::close`] for each,
    /// and leaves every other session open.
    pub(crate) fn close_credential(&self, credential: &str) {
        self.close_credential_with(credential, |_| {});
    }

    /// [`Self::close_credential`]; `detached(id)` runs under the pool lock as each session
    /// leaves it, so state keyed by the session id goes with it before the id can be
    /// reused (Go deletes xAI's ID state under the session-store mutex). Sockets shut down
    /// after the lock is released.
    pub(crate) fn close_credential_with(&self, credential: &str, mut detached: impl FnMut(&str)) {
        let sessions: Vec<_> = {
            let sessions = self.sessions.lock().expect("sessions");
            sessions.iter().map(|(id, s)| (id.clone(), s.clone())).collect()
        };
        let matching: Vec<_> = sessions
            .into_iter()
            .filter(|(_, s)| *s.credential.lock().expect("session credential") == credential)
            .collect();
        let mut closing = Vec::new();
        {
            let mut sessions = self.sessions.lock().expect("sessions");
            for (id, session) in matching {
                // A session closed or replaced since the snapshot is not this one.
                if !sessions.get(&id).is_some_and(|s| Arc::ptr_eq(s, &session)) {
                    continue;
                }
                sessions.remove(&id);
                detached(&id);
                closing.push(session);
            }
        }
        for session in closing {
            if let Some(conn) = session.conn.lock().expect("session conn").take() {
                conn.shutdown();
            }
        }
    }

    /// The error that lost the session's upstream socket, if it was lost.
    pub fn loss(&self, id: &str) -> Option<ExecError> {
        let session = self.get(id)?;
        session.closed.borrow().clone()
    }

    /// A failed attempt must not close the next credential's downstream connection.
    pub(crate) fn clear_loss(&self, id: &str) {
        if let Some(session) = self.get(id) {
            // Pair with invalidate: a retired reader must publish before this reset.
            let _current = session.conn.lock().expect("session conn");
            session.closed.send_if_modified(|loss| loss.take().is_some());
        }
    }

    /// `InterruptExecutionSession`: writes the client's `response.interrupt` unchanged
    /// to the socket of the session's turn in progress. It never dials, replays or
    /// selects another credential. `enabled(credential_id)` is the live account check,
    /// called without holding a session lock.
    pub async fn interrupt(
        &self,
        id: &str,
        payload: &[u8],
        enabled: impl FnOnce(&str) -> bool,
    ) -> Result<(), InterruptError> {
        let session = Some(id.trim())
            .filter(|id| !id.is_empty())
            .and_then(|id| self.get(id))
            .ok_or(InterruptError::NoActive)?;
        let conn = session.current().ok_or(InterruptError::NoActive)?;
        let auth_enabled = enabled(&conn.target.credential);
        let response_id = ck_wire::json::get(payload, "response_id").bytes();
        {
            // One snapshot of the socket's state: a completion landing between the
            // checks would otherwise turn a late interrupt into a stale error.
            let current = session.conn.lock().expect("session conn");
            if !current.as_ref().is_some_and(|c| Arc::ptr_eq(c, &conn)) {
                return Err(InterruptError::NoActive);
            }
            let link = conn.link.lock().expect("link");
            if let Some(lost) = &link.lost {
                return Err(InterruptError::Rejected(
                    String::from_utf8_lossy(&lost.body).into_owned(),
                ));
            }
            if std::str::from_utf8(&response_id).is_ok_and(|id| link.terminal.contains(id)) {
                return Ok(());
            }
            // A socket kept from an earlier turn is not the current upstream.
            if link.active.is_none() {
                return Err(InterruptError::NoActive);
            }
        }
        if !auth_enabled {
            return Err(InterruptError::Rejected(
                "websocket credential is no longer enabled".into(),
            ));
        }
        // A failed or timed-out write may leave a partial frame: the socket is dropped
        // like after any failed turn write (Go leaves that to its reader).
        if let Err(error) = conn.send_raw(Bytes::copy_from_slice(payload)).await {
            session.invalidate(&conn, &error, true);
            return Err(InterruptError::Rejected(
                String::from_utf8_lossy(&error.body).into_owned(),
            ));
        }
        Ok(())
    }

    /// Resolves with the error that lost the session's upstream socket.
    pub fn closed(&self, id: &str) -> impl std::future::Future<Output = ExecError> + Send + 'static {
        let mut rx = self.session(id).closed.subscribe();
        async move {
            loop {
                if let Some(error) = rx.borrow_and_update().clone() {
                    return error;
                }
                if rx.changed().await.is_err() {
                    return std::future::pending().await;
                }
            }
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.sessions.lock().expect("sessions").len()
    }
}
