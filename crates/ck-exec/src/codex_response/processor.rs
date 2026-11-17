//! Codex event processing and bootstrap budgets.
use super::*;
/// Output items seen in `response.output_item.done`, used to patch an empty
/// `response.completed.response.output` (`collectCodexOutputItemDone`,
/// `patchCodexCompletedOutput`).
#[derive(Default)]
pub(crate) struct OutputItems {
    by_index: BTreeMap<i64, String>,
    fallback: Vec<String>,
}

impl OutputItems {
    pub fn len(&self) -> usize {
        self.by_index.len() + self.fallback.len()
    }

    pub fn collect(&mut self, payload: &str) {
        let item = gjson::get(payload, "item");
        if !matches!(item.kind(), Kind::Object | Kind::Array) {
            return;
        }
        let index = gjson::get(payload, "output_index");
        if index.exists() {
            self.by_index.insert(index.i64(), item.json().to_owned());
        } else {
            self.fallback.push(item.json().to_owned());
        }
    }

    pub fn patch(&self, payload: String) -> String {
        let output = gjson::get(&payload, "response.output");
        let outputs = output.array();
        if output.kind() == Kind::Array && !outputs.is_empty() {
            // hydrateCodexCompletedOutputItemIDs: fill missing ids from done items.
            let mut patched = payload.clone();
            for (i, item) in outputs.iter().enumerate() {
                let id = item.get("id");
                let has_id = id.exists()
                    && id.kind() != Kind::Null
                    && (id.kind() != Kind::String || !id.str().trim().is_empty());
                if has_id {
                    continue;
                }
                let Some(done) = self.by_index.get(&(i as i64)) else {
                    continue;
                };
                let done_id = gjson::get(done, "id");
                if done_id.kind() == Kind::String && !done_id.str().trim().is_empty() {
                    patched = set_raw(&patched, &format!("response.output.{i}.id"), done_id.json());
                }
            }
            return patched;
        }
        if self.len() == 0 {
            return payload;
        }
        let items: Vec<&str> = self
            .by_index
            .values()
            .chain(self.fallback.iter())
            .map(String::as_str)
            .collect();
        set_raw(&payload, "response.output", &format!("[{}]", items.join(",")))
    }
}

/// Go's 50 MB scanner line limit on the streaming path.
pub(crate) const SCANNER_MAX_LINE: usize = 52_428_800;

/// Processed lines as one SSE frame; `None` for no lines.
fn framed(lines: &[String]) -> Option<Bytes> {
    if lines.is_empty() {
        return None;
    }
    let mut out = lines.join("\n");
    out.push_str("\n\n");
    Some(Bytes::from(out))
}

/// What one upstream event means for the stream.
pub(crate) enum Step {
    /// Forward this (normalised) event.
    Event(Bytes),
    /// Forward this event; it ends the response.
    Terminal(Bytes),
    /// The upstream rejected or broke the response; nothing more is forwarded after
    /// `before`, the event's lines that preceded the failure (Go sends each line as read).
    Fail {
        error: ExecError,
        body: Option<String>,
        before: Option<Bytes>,
    },
}

/// Per-response SSE processing shared by streaming and buffered paths.
pub(crate) struct Processor {
    items: OutputItems,
    preserve_native: bool,
    model_level_cooling: bool,
    /// The last completed payload (response.completed/incomplete), for buffered callers.
    pub completed: Option<String>,
    /// The request renamed the `collaboration` namespace (multi-agent v2).
    restore: bool,
    /// Claude clients' reasoning replay: cached from completed turns, cleared on an
    /// invalid-signature failure.
    replay: Option<(std::sync::Arc<crate::codex_replay::Cache>, crate::codex_replay::Scope)>,
    /// Usage records see every upstream payload in Codex format (`observeCodexTokenEvent`).
    usage: ck_core::exec::UsageSink,
    /// Grok Build clients get keepalive events as SSE comments (`grokbuild`).
    grok_keepalive: bool,
    /// The attempt's wire capture: every scanned line and each stream failure.
    wire: crate::codex_capture::Wire,
    /// Joins an event's data split across `data:` lines (`CodexSSEScanner`, 50 MB lines).
    sse: crate::codex_sse::DataLines,
}

impl Processor {
    pub fn new(preserve_native: bool, model_level_cooling: bool) -> Self {
        Self {
            items: OutputItems::default(),
            preserve_native,
            model_level_cooling,
            completed: None,
            restore: false,
            replay: None,
            usage: Default::default(),
            grok_keepalive: false,
            wire: Default::default(),
            sse: crate::codex_sse::DataLines::new(SCANNER_MAX_LINE),
        }
    }

    /// Records every scanned line and each stream failure (Go `AppendAPIResponseChunk`
    /// and `RecordAPIResponseError` in the stream loops).
    pub fn capturing(mut self, wire: crate::codex_capture::Wire) -> Self {
        self.wire = wire;
        self
    }

    /// `RecordAPIResponseError` for a failure the stream reports.
    pub(super) fn record(&self, error: &ExecError) {
        self.wire.exec_error(error);
    }

    /// `grokbuild.TransformKeepaliveSSELine` for a Grok Build client (`User-Agent` with
    /// `grok-pager` or `grok-shell`).
    pub fn grok_keepalive(mut self, headers: &http::HeaderMap) -> Self {
        self.grok_keepalive = is_grok_client(headers);
        self
    }

    /// Reports each upstream payload to the attempt's usage record.
    // ponytail: Go also publishes `response.tool_usage.image_gen` as a second record under
    // the request's image_generation tool model (`publishCodexImageToolUsage`, default
    // gpt-image-2); `UsageSink` has no per-model record yet (a Server contract change).
    pub fn reporting(mut self, usage: ck_core::exec::UsageSink) -> Self {
        self.usage = usage;
        self
    }

    /// Caches completed turns and clears on invalid signatures for this replay scope.
    pub fn replaying(
        mut self,
        cache: std::sync::Arc<crate::codex_replay::Cache>,
        scope: crate::codex_replay::Scope,
    ) -> Self {
        self.replay = Some((cache, scope));
        self
    }

    fn replay_failure(&self, status: u16, body: &str) {
        if let Some((cache, scope)) = &self.replay {
            crate::codex_replay::clear_on_invalid_signature(cache, scope, status, body.as_bytes());
        }
    }

    fn replay_completed(&self, payload: &str) {
        if let Some((cache, scope)) = &self.replay {
            crate::codex_replay::cache_completed(cache, scope, payload.as_bytes());
        }
    }

    /// Restores the client's `collaboration` names in every payload before anything
    /// reads it, as Go does right after trimming each `data:` line.
    pub fn restoring(mut self, restore: bool) -> Self {
        self.restore = restore;
        self
    }

    /// One framed SSE event in. Go scans lines: `data:` payloads are trimmed and
    /// re-prefixed with `data: `, other lines pass through, CR is dropped.
    pub fn event(&mut self, event: &[u8]) -> Step {
        self.event_with(event, None)
    }

    pub(super) fn event_with(&mut self, event: &[u8], gate: Option<&mut Gate>) -> Step {
        let step = self.event_step(event, gate);
        if let Step::Fail { error, .. } = &step {
            self.record(error);
        }
        step
    }

    /// Processes `event` line by line, in Go's order. While `gate` holds the stream, each
    /// delivered line spends its budgets before the next is read, and the gate releases
    /// at the line that passes them; the rest of the event is processed as streamed.
    fn event_step(&mut self, event: &[u8], mut gate: Option<&mut Gate>) -> Step {
        let mut lines = Vec::new();
        let mut terminal = false;
        // Go writes each keepalive comment as its own SSE frame.
        let mut after_comment = false;
        let mut physical = event.split(|b| *b == b'\n');
        let mut scanned = Vec::new();
        let mut read_all = false;
        'read: while !read_all {
            match physical.next() {
                Some(raw) => {
                    let raw = raw.strip_suffix(b"\r").unwrap_or(raw);
                    // Go records each physical line as read, and none after a terminal.
                    self.wire.line(raw);
                    self.sse.push(raw, &mut scanned);
                    let held = self.sse.holding();
                    if held.0 > 0 && raw.starts_with(b"data:") {
                        // Go's budget check inside the scanner, on a data line it holds.
                        if let Some(gate) = gate.as_deref_mut() {
                            gate.assembling(held);
                        }
                    }
                }
                None => {
                    self.sse.finish(&mut scanned);
                    read_all = true;
                }
            }
            let held = self.sse.holding();
            for item in scanned.drain(..) {
                let line = String::from_utf8_lossy(&item.line);
                let line = line.as_ref();
                if line.is_empty() {
                    continue;
                }
                let before = lines.len();
                if after_comment {
                    lines.push(String::new());
                }
                after_comment = self.grok_keepalive && is_keepalive_line(line);
                let handshake = if after_comment {
                    lines.push(": keepalive".to_owned());
                    true
                } else if let Some(data) = line.strip_prefix("data:") {
                    let (payload, handshake, completed) = match self.payload(data) {
                        Ok(result) => result,
                        Err((error, body)) => return payload_failure(error, body, &lines),
                    };
                    terminal = completed;
                    lines.push(format!("data: {payload}"));
                    if terminal {
                        break 'read;
                    }
                    handshake
                } else {
                    lines.push(line.to_owned());
                    true
                };
                if let Some(gate) = gate.as_deref_mut() {
                    let translated = lines[before..].iter().map(String::len).sum();
                    gate.line(handshake, held, (item.raw_lines, item.raw_bytes), translated);
                }
            }
        }
        if !terminal && let Some(gate) = gate {
            // The blank line that ends the event (Go delivers it as a line too).
            gate.line(true, (0, 0), (1, 0), 0);
        }
        let out = framed(&lines).unwrap_or_else(|| Bytes::from_static(b"\n\n"));
        if terminal {
            Step::Terminal(out)
        } else {
            Step::Event(out)
        }
    }

    fn payload(&mut self, data: &str) -> Result<(String, bool, bool), (ExecError, String)> {
        let payload = restore(data.trim(), self.restore);
        if self.usage.enabled() {
            self.usage
                .response_line(ck_core::format::Format::Codex, payload.as_bytes());
        }
        if let Some((error, body)) = terminal_failure(&payload, self.model_level_cooling) {
            self.replay_failure(error.status, &body);
            return Err((error, body));
        }
        let handshake = bufferable(&payload);
        let mut payload = payload.into_owned();
        let mut terminal = false;
        match gjson::get(&payload, "type").str() {
            "response.output_item.done" => self.items.collect(&payload),
            kind @ ("response.completed" | "response.incomplete") => {
                let incomplete = kind == "response.incomplete";
                terminal = true;
                if !self.preserve_native {
                    payload = self.items.patch(payload);
                }
                if !incomplete {
                    self.replay_completed(&payload);
                }
                self.completed = Some(payload.clone());
            }
            _ => {}
        }
        Ok((payload, handshake, terminal))
    }

    /// Buffered (`Execute`) handling of one event's data payloads: always patches output.
    pub fn buffered(&mut self, event: &[u8]) -> Result<Option<String>, ExecError> {
        // Go scans the whole body with a line limit one past its length.
        let mut sse = crate::codex_sse::DataLines::new(event.len() + 1);
        let mut physical = event.split(|b| *b == b'\n');
        let mut scanned = Vec::new();
        let mut read_all = false;
        while !read_all {
            match physical.next() {
                Some(raw) => sse.push(raw.strip_suffix(b"\r").unwrap_or(raw), &mut scanned),
                None => {
                    sse.finish(&mut scanned);
                    read_all = true;
                }
            }
            for line in scanned.drain(..) {
                let line = String::from_utf8_lossy(&line.line);
                let Some(data) = line.strip_prefix("data:") else {
                    continue;
                };
                let payload = restore(data.trim(), self.restore);
                let payload = payload.as_ref();
                if self.usage.enabled() {
                    self.usage
                        .response_line(ck_core::format::Format::Codex, payload.as_bytes());
                }
                if let Some((error, body)) = terminal_failure(payload, self.model_level_cooling) {
                    self.replay_failure(error.status, &body);
                    return Err(error);
                }
                match gjson::get(payload, "type").str() {
                    "response.output_item.done" => self.items.collect(payload),
                    kind @ ("response.completed" | "response.incomplete") => {
                        let completed = self.items.patch(payload.to_owned());
                        if kind == "response.completed" {
                            self.replay_completed(&completed);
                        }
                        return Ok(Some(completed));
                    }
                    _ => {}
                }
            }
        }
        Ok(None)
    }
}

/// `grokbuild.IsGrokClientHeaders`: any `User-Agent` value naming Grok Pager or Shell.
pub(crate) fn is_grok_client(headers: &http::HeaderMap) -> bool {
    headers.get_all(http::header::USER_AGENT).iter().any(|v| {
        let ua = String::from_utf8_lossy(v.as_bytes()).go_lower();
        ua.contains("grok-pager") || ua.contains("grok-shell")
    })
}

/// `grokbuild.IsKeepaliveSSELine`: `event: keepalive` or a `keepalive` data payload.
fn is_keepalive_line(line: &str) -> bool {
    let trimmed = line.trim();
    if let Some(name) = trimmed.strip_prefix("event:") {
        return name.trim() == "keepalive";
    }
    trimmed
        .strip_prefix("data:")
        .is_some_and(|data| gjson::get(data.trim(), "type").str() == "keepalive")
}

/// `RestoreCodexMultiAgentV2Response` on one upstream payload when the request was
/// optimized.
pub(crate) fn restore(payload: &str, optimized: bool) -> std::borrow::Cow<'_, str> {
    if !optimized {
        return std::borrow::Cow::Borrowed(payload);
    }
    match String::from_utf8(ck_wire::codex_client::restore_response(payload.as_bytes(), true)) {
        Ok(restored) => std::borrow::Cow::Owned(restored),
        Err(_) => std::borrow::Cow::Borrowed(payload),
    }
}
/// Go's bootstrap budgets (codex_executor_stream.go): the lines held so far, their raw
/// and translated bytes, and whether the stream has been released.
pub(crate) struct Gate {
    frames: usize,
    bytes: usize,
    timeout: Option<Duration>,
    started: Instant,
    pub(super) released: bool,
}

impl Gate {
    pub(super) fn new(timeout: Option<Duration>, started: Instant) -> Self {
        Self {
            frames: 0,
            bytes: 0,
            timeout,
            started,
            released: false,
        }
    }

    pub(super) fn timed_out(&self) -> bool {
        self.timeout.is_some_and(|t| self.started.elapsed() >= t)
    }

    /// The scanner's budget check while it holds `held` (lines, bytes) of an unfinished
    /// event: the stream is released at the limit itself.
    fn assembling(&mut self, held: (usize, usize)) {
        if !self.released
            && (self.timed_out()
                || self.frames + held.0 >= BOOTSTRAP_MAX_FRAMES
                || self.bytes + held.1 >= BOOTSTRAP_MAX_BYTES)
        {
            self.released = true;
        }
    }

    /// One delivered line read from `raw` (lines, bytes) and `translated` bytes out, with
    /// `held` (lines, bytes) still collected: a handshake within the budgets, which it may
    /// fill exactly, stays held; anything else releases the stream.
    // ponytail: `translated` is the Codex-format line: Go's translator output for a Codex
    // client and for Responses events it leaves alone (it inserts a missing
    // `response.model`); Claude and Chat sizes differ. That matters only when a total
    // straddles 1 MiB. Exact needs client translation inside bootstrap.
    fn line(&mut self, handshake: bool, held: (usize, usize), raw: (usize, usize), translated: usize) {
        if self.released {
            return;
        }
        let frames = self.frames + held.0 + raw.0;
        let bytes = self.bytes + held.1 + raw.1 + translated;
        if handshake && !self.timed_out() && frames <= BOOTSTRAP_MAX_FRAMES && bytes <= BOOTSTRAP_MAX_BYTES {
            self.frames += raw.0;
            self.bytes += raw.1 + translated;
        } else {
            self.released = true;
        }
    }
}

fn payload_failure(error: ExecError, body: String, lines: &[String]) -> Step {
    Step::Fail {
        error,
        body: Some(body),
        before: framed(lines),
    }
}
