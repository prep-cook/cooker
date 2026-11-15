use super::*;

impl Turn {
    async fn frame(&mut self) -> Frame {
        let frame = self.read().await;
        if let Frame::Failed { error, read: true, .. } = &frame {
            let binary = error.body.as_ref() == UNEXPECTED_BINARY.as_bytes();
            self.wire
                .ws_exec_error(if binary { "unexpected_binary" } else { "read" }, error);
        }
        frame
    }

    async fn read(&mut self) -> Frame {
        let text = match self.rx.recv().await {
            Some(Read::Text(text)) => text,
            Some(Read::Failed(error)) => {
                return Frame::Failed {
                    error,
                    overload: false,
                    read: true,
                };
            }
            // A full channel can drop the reader's terminal error; the session still
            // recorded the loss (Go's reader waits to deliver it).
            None => {
                return Frame::Failed {
                    error: self.loss().map_or_else(
                        || transport("codex websockets executor: session read channel closed"),
                        |l| turn_error(&l),
                    ),
                    overload: false,
                    read: true,
                };
            }
        };
        let payload = text.trim();
        if payload.is_empty() {
            return Frame::Skip;
        }
        self.observe(payload);
        self.wire.ws_response(payload);
        let raw_len = payload.len();
        let payload = response::restore(payload, self.restore);
        let payload = payload.as_ref();
        if let Some(error) = ws_error(payload, self.cooling) {
            self.replay.clear(payload, self.cooling);
            self.wire.ws_exec_error("upstream_error", &error);
            return Frame::Failed {
                error,
                overload: false,
                read: false,
            };
        }
        if let Some((error, body)) = response::terminal_failure(payload, self.cooling) {
            self.replay.clear(payload, self.cooling);
            self.wire.ws_exec_error("upstream_error", &error);
            return Frame::Failed {
                error,
                overload: response::is_overload(&body),
                read: false,
            };
        }
        let kind = gjson::get(payload, "type");
        let kind = kind.str().to_owned();
        let bufferable = response::bufferable(payload);
        let mut out = payload.to_owned();
        let terminal = matches!(kind.as_str(), "response.completed" | "response.incomplete");
        if kind == "response.output_item.done" {
            self.items.collect(payload);
        }
        if terminal {
            if !self.native {
                out = self.items.patch(out);
            }
            if kind != "response.incomplete" {
                self.replay.completed(&out);
            }
        }
        Frame::Event {
            raw_len,
            out: Bytes::from(response::ensure_usage_details(out)),
            bufferable,
            terminal,
        }
    }

    /// Quota headers and usage from one upstream event, observed before collaboration
    /// names are restored (as Go does).
    pub(super) fn observe(&mut self, payload: &str) {
        if let Some(headers) = codex_quota::event_headers(payload) {
            codex_quota::merge(&mut self.observed, &headers);
            self.quota.observe(&self.credential, &self.model, &self.observed);
        }
        if self.usage.enabled() {
            self.usage
                .response_line(ck_core::format::Format::Codex, payload.as_bytes());
        }
    }

    pub(super) fn invalidate(&self, error: &ExecError, notify: bool) {
        self.session.invalidate(&self.conn, error, notify);
    }

    /// The raw error that lost the session's socket, as the reader recorded it (the
    /// duplex wraps gorilla's close error unmapped).
    pub(super) fn loss(&self) -> Option<ExecError> {
        self.session.closed.borrow().clone()
    }
}

/// The rest of a turn as a stream; ends after the terminal event or the first error.
pub(super) fn rest(turn: Turn) -> ExecStream {
    futures_util::stream::unfold(Some(turn), |turn| async move {
        let mut turn = turn?;
        loop {
            match turn.frame().await {
                Frame::Skip => continue,
                Frame::Event { out, terminal, .. } => {
                    return Some((Ok(out), (!terminal).then_some(turn)));
                }
                Frame::Failed { error, read, .. } => {
                    if !read {
                        turn.invalidate(&error, true);
                    }
                    return Some((Err(error), None));
                }
            }
        }
    })
    .boxed()
}

/// Bootstrap buffering over WebSocket messages (codex_websockets_stream.go): every
/// message read counts toward the 48-frame window, held frames are bounded by 1 MiB.
pub(super) async fn bootstrap(
    mut turn: Turn,
    timeout: Option<Duration>,
    started: Instant,
) -> Result<ExecStream, ExecError> {
    let mut held: Vec<Result<Bytes, ExecError>> = Vec::new();
    let (mut frames, mut bytes) = (0usize, 0usize);
    loop {
        let frame = turn.frame().await;
        frames += 1;
        let timed_out = timeout.is_some_and(|t| started.elapsed() >= t);
        let window_open = frames <= response::BOOTSTRAP_MAX_FRAMES && !timed_out;
        match frame {
            Frame::Skip if window_open => continue,
            Frame::Skip => break,
            Frame::Failed { error, read: true, .. } => return Err(error),
            Frame::Failed { error, overload, .. } => {
                turn.invalidate(&error, timed_out);
                if !timed_out {
                    // No buffered frame reached the client: let dispatch classify and retry.
                    return Err(if overload {
                        response::bootstrap_overload(&String::from_utf8_lossy(&error.body))
                    } else {
                        error
                    });
                }
                held.push(Err(error));
                return Ok(futures_util::stream::iter(held).boxed());
            }
            Frame::Event {
                out,
                raw_len,
                bufferable,
                terminal,
            } => {
                let size = raw_len + out.len();
                if window_open && bufferable && !terminal && bytes + size <= response::BOOTSTRAP_MAX_BYTES {
                    bytes += size;
                    held.push(Ok(out));
                    continue;
                }
                held.push(Ok(out));
                if terminal {
                    return Ok(futures_util::stream::iter(held).boxed());
                }
                break;
            }
        }
    }
    Ok(futures_util::stream::iter(held).chain(rest(turn)).boxed())
}
