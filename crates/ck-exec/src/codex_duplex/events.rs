use super::*;

impl Duplex {
    /// One upstream frame (the reader goroutine).
    pub(super) fn upstream(&mut self, read: Option<Read>) {
        let text = match read {
            Some(Read::Text(text)) => text,
            // Go's duplex wraps the raw read error, not `mapCodexWebsocketReadError`'s 413.
            Some(Read::Failed(error)) => {
                let error = self.writer_error().or_else(|| self.turn.loss()).unwrap_or(error);
                return self.fail(error);
            }
            None => {
                let error = self
                    .writer_error()
                    .or_else(|| self.turn.loss())
                    .unwrap_or_else(|| transport("codex websockets executor: session read channel closed"));
                return self.fail(error);
            }
        };
        let payload = text.trim();
        if payload.is_empty() {
            return;
        }
        let kind = gjson::get(payload, "type").str().to_owned();
        let establishing = self.first_response && kind == "response.created";
        if kind == "response.created" && !self.response_created(payload) {
            return;
        }
        self.turn.observe(payload);
        self.turn.wire.ws_response(payload);
        self.observed(payload, &kind, establishing);
    }

    /// The rest of an upstream message's handling.
    fn observed(&mut self, payload: &str, kind: &str, establishing: bool) {
        // Steering acknowledgements, pending notices and failures are opaque: IDs, input,
        // sequence numbers and event types reach the client byte for byte.
        if kind.starts_with("response.steer.") {
            self.steer_event(kind, payload);
            self.outbox.push_back(Ok(Bytes::from(payload.to_owned())));
            return;
        }
        let failure = kind == "error" || kind == "response.failed";
        let cooling = self.turn.cooling;
        let classify = |payload: &str| {
            ws_error(payload, cooling).or_else(|| response::terminal_failure(payload, cooling).map(|(e, _)| e))
        };
        if !self.first_response && failure {
            // Account health does not depend on which queued request failed: the stream
            // ends with the original classification and is not replayed elsewhere.
            if let Some(error) = classify(payload).filter(|e| matches!(e.status, 401 | 403 | 429)) {
                self.outbox.push_back(Ok(Bytes::from(payload.to_owned())));
                self.outbox.push_back(Err(error));
                self.done = true;
                return;
            }
        }
        // The request this event belongs to: a rejected pending create, else the running one.
        let mut rejected = None;
        if !self.first_response && failure {
            let mut failed = gjson::get(payload, "response.id").str().to_owned();
            if failed.is_empty() {
                failed = gjson::get(payload, "response_id").str().to_owned();
            }
            // A failure of the running response must not consume a queued create; a
            // rejection before `response.created` belongs to the oldest pending create.
            let current_failure = !failed.is_empty() && failed == self.response_id;
            let ambiguous = failed.is_empty()
                && ((!self.pending.is_empty() && self.response_active) || !self.unacknowledged.is_empty());
            if ambiguous {
                // Guessing could corrupt either request: keep the event, end the socket.
                self.outbox.push_back(Ok(Bytes::from(payload.to_owned())));
                self.fail(local(
                    "cannot associate websocket failure with a response or pending create",
                ));
                return;
            }
            match self.pending.pop_front() {
                Some(create) if !current_failure => rejected = Some(create),
                popped => {
                    if let Some(popped) = popped {
                        self.pending.push_front(popped);
                    }
                    self.response_active = false;
                    self.automatic_active = false;
                }
            }
        }
        let event = rejected.as_ref().unwrap_or(&self.current);
        let payload = response::restore(payload, self.restores(event));
        // Every rejection clears its own request's replay; only the first fails the
        // stream (and can enter bootstrap retry), later ones are events the client may
        // correct on this socket.
        let terminal = classify(&payload);
        if let Some(error) = &terminal {
            event.replay.clear(&payload, cooling);
            self.turn.wire.ws_exec_error("upstream_error", error);
        }
        if self.first_response
            && let Some(error) = terminal
        {
            self.outbox.push_back(Err(error));
            self.done = true;
            return;
        }
        self.deliver(payload.into_owned(), kind, establishing);
    }

    fn deliver(&mut self, mut out: String, kind: &str, establishing: bool) {
        if kind == "response.output_item.done" {
            self.turn.items.collect(&out);
        }
        if matches!(kind, "response.completed" | "response.incomplete") {
            self.response_active = false;
            self.automatic_active = false;
            if !self.current.native {
                out = self.turn.items.patch(out);
            }
            if kind != "response.incomplete" {
                self.current.replay.completed(&out);
            }
        }
        self.outbox
            .push_back(Ok(Bytes::from(response::ensure_usage_details(out))));
        if establishing {
            // Delivered before any locally generated error, so the handler also sees the
            // bootstrap succeed first.
            self.input_ready = true;
        }
    }

    /// `response.created`: which request it serves and what it retains. False ends the
    /// stream (an automatic successor without retained parent settings).
    fn response_created(&mut self, payload: &str) -> bool {
        let mut parent = gjson::get(payload, "response.previous_response_id").str().to_owned();
        if parent.is_empty() {
            parent.clone_from(&self.response_id);
        }
        let automatic = !self.first_response && self.pending.is_empty();
        if automatic {
            let settings = self
                .steering_settings
                .get(&parent)
                .or_else(|| self.response_settings.get(&parent))
                .cloned();
            let Some(settings) = settings else {
                self.fail(local("automatic successor has no retained parent settings"));
                return false;
            };
            self.current = settings;
        }
        self.accepted.retain(|_, target| *target != parent);
        self.waiting_parent.clear();
        self.automatic_active = automatic;
        if let Some(next) = self.pending.pop_front() {
            self.current = next;
        }
        self.response_id = gjson::get(payload, "response.id").str().to_owned();
        self.response_settings
            .insert(self.response_id.clone(), self.current.retained());
        self.response_order.push_back(self.response_id.clone());
        if self.response_order.len() > RETAINED_RESPONSES
            && let Some(oldest) = self.response_order.pop_front()
        {
            self.response_settings.remove(&oldest);
        }
        self.release_steering_settings(&parent);
        self.first_response = false;
        self.response_active = true;
        self.turn.items = Default::default();
        true
    }

    /// `response.steer.*`: acknowledgement bookkeeping.
    fn steer_event(&mut self, kind: &str, payload: &str) {
        let id = gjson::get(payload, "steer.id").str().to_owned();
        let mut parent = gjson::get(payload, "steer.previous_response_id").str().to_owned();
        if parent.is_empty() {
            parent.clone_from(&self.response_id);
        }
        let consume = |unacknowledged: &mut Vec<String>| {
            if let Some(i) = unacknowledged.iter().position(|t| *t == parent || t.is_empty()) {
                unacknowledged.remove(i);
            }
        };
        match kind {
            "response.steer.accepted" => {
                consume(&mut self.unacknowledged);
                self.accepted.insert(id, parent);
            }
            "response.steer.failed" => {
                if self.accepted.remove(&id).is_none() {
                    consume(&mut self.unacknowledged);
                }
                self.release_steering_settings(&parent);
            }
            // Tool results may already wait in the queue; no automatic successor can start
            // until an explicit continuation supplies them.
            "response.steer.pending" => self.waiting_parent = parent,
            _ => {}
        }
    }
}
