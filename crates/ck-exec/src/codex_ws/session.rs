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
        })
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

    /// `invalidateUpstreamConn`: only the session's current socket is dropped, so a stale
    /// reader cannot tear down its replacement.
    pub(crate) fn invalidate(&self, conn: &Arc<Upstream>) {
        {
            let mut current = self.conn.lock().expect("session conn");
            if !current.as_ref().is_some_and(|c| Arc::ptr_eq(c, conn)) {
                return;
            }
            *current = None;
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
    pub(crate) fn session(&self, id: &str) -> Arc<Session> {
        self.sessions
            .lock()
            .expect("sessions")
            .entry(id.to_owned())
            .or_insert_with(|| {
                Arc::new(Session {
                    turn: Arc::default(),
                    conn: Mutex::default(),
                    credential: Mutex::default(),
                })
            })
            .clone()
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
}
