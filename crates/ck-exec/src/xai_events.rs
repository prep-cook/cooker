//! The xAI upstream body on its way into the Responses response ingress (`xai_exec`):
//! Go's per-line xAI rules (xai_executor.go `ExecuteStream`), applied as the body arrives.
//!
//! - Grok's reasoning-text events and items take the reasoning-summary shape.
//! - The namespace calls, the aliased client `web_search` function and the internal X Search
//!   trace the request rewrote are restored or dropped.
//! - The apply_patch bridge turns function calls back into the client's custom tool; its
//!   failure ends the response with Go's 502.
//! - A completion whose `output` is empty or absent gets the items
//!   `response.output_item.done` announced. They wait in scratch leases of the response
//!   stream, never on the heap.
//! - A completed response is cached for reasoning replay once its SSE event has ended and
//!   the ingress accepted it, so a cut or refused completion caches nothing, nor one after
//!   an upstream failure, whose input the ingress ignores. The kept items wait in scratch
//!   leases and reach the replay cache one item at a time.
//! - The body ending before the completion boundary fails with Go's 408.
//!
//! One SSE event goes out per upstream event, in order; the ingress decodes the result. A
//! line that is not UTF-8 passes unchanged, so the ingress refuses it.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use ck_stream::{ScratchLease, Writer};
use ck_translate::apply_patch_responses as apply_patch;
use ck_translate::codec::encode::{self, Encoder};
use ck_translate::codec::{self, ResponseIngress};
use ck_translate::responses::response::Ingress;
use ck_wire::gostr::trim_space;
use ck_wire::json::{self as gj, Kind};

use crate::xai::MAX_LINE;
use crate::xai_replay::{self as replay, ReplayScope};
use crate::xai_request::Prepared;
use crate::xai_response::{self as response, NamespaceRefs, NamespaceRestorer, XSearchFilter, text};
use crate::xai_sse::Failures;

/// The response decoder of one xAI attempt: generation through the xAI line rules, or a
/// buffered compact whose success clears the session's replay.
pub enum XaiIngress {
    Generate(Box<Events>),
    Compact(Box<Compaction>),
}

/// A compact's ingress and the replay its success clears.
pub struct Compaction {
    pub(crate) ingress: Ingress,
    pub(crate) replay: Option<(Arc<replay::Store>, ReplayScope)>,
}

impl ResponseIngress for XaiIngress {
    fn flush_deadline(&self) -> Option<std::time::Instant> {
        match self {
            Self::Generate(events) => events.inner.flush_deadline(),
            Self::Compact(compact) => compact.ingress.flush_deadline(),
        }
    }

    fn flush(&mut self, owner: &mut Writer) -> codec::Result<()> {
        match self {
            Self::Generate(events) => events.inner.flush(owner),
            Self::Compact(compact) => compact.ingress.flush(owner),
        }
    }

    fn advance(&mut self, owner: &mut Writer, bytes: &[u8]) -> codec::Result<()> {
        match self {
            Self::Generate(events) => events.advance(owner, bytes),
            Self::Compact(compact) => compact.ingress.advance(owner, bytes),
        }
    }

    fn finish(&mut self, owner: &mut Writer) -> codec::Result<()> {
        match self {
            Self::Generate(events) => events.finish(owner),
            Self::Compact(compact) => {
                compact.ingress.finish(owner)?;
                // `clearXAIReasoningReplayAfterCompaction`, once the compaction is complete.
                if let Some((store, scope)) = &compact.replay {
                    replay::clear(store, scope);
                }
                Ok(())
            }
        }
    }

    fn fail(&mut self, owner: &mut Writer, status: u16, scope: &'static str) -> codec::Result<()> {
        match self {
            Self::Generate(events) => events.inner.fail(owner, status, scope),
            Self::Compact(compact) => compact.ingress.fail(owner, status, scope),
        }
    }
}

/// The xAI rules over one generation body, in front of its Responses ingress.
pub struct Events {
    inner: Ingress,
    /// The physical line still waiting for its end.
    line: Vec<u8>,
    /// An `event:` line held for the data line it names.
    pending: Option<Vec<u8>>,
    bridge: apply_patch::State,
    items: Items,
    filter: XSearchFilter,
    restorer: NamespaceRestorer,
    /// The flattened and folded namespace tool names the request sent.
    refs: NamespaceRefs,
    /// Folded calls in flight, by item id.
    folded: HashSet<String>,
    alias: String,
    replay: Option<(Arc<replay::Store>, ReplayScope)>,
    /// The upstream failures the ingress was handed; it ignores what follows one.
    failures: Failures,
    /// The completion's output items replay keeps, waiting for the blank line that ends it;
    /// `None` when the completion has no output array.
    completed: Option<Vec<ScratchLease>>,
}

impl Events {
    pub(crate) fn new(inner: Ingress, prepared: Prepared, store: Arc<replay::Store>) -> Self {
        let scope = prepared.replay_scope;
        Self {
            inner,
            line: Vec::new(),
            pending: None,
            bridge: prepared.apply_patch,
            items: Items::default(),
            filter: XSearchFilter::new(prepared.filter_internal_x_search, prepared.client_declared_tools),
            refs: prepared.namespace_tools.clone(),
            restorer: NamespaceRestorer::new(prepared.namespace_tools),
            folded: HashSet::new(),
            alias: prepared.web_search_alias,
            replay: scope.valid().then_some((store, scope)),
            failures: Failures::default(),
            completed: None,
        }
    }

    fn advance(&mut self, owner: &mut Writer, mut bytes: &[u8]) -> codec::Result<()> {
        while let Some(end) = bytes.iter().position(|b| *b == b'\n') {
            if self.line.len() + end > MAX_LINE {
                return self.inner.fail(owner, 500, "upstream");
            }
            self.line.extend_from_slice(&bytes[..end]);
            bytes = &bytes[end + 1..];
            let mut line = std::mem::take(&mut self.line);
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            let more = self.line_in(owner, &line)?;
            line.clear();
            self.line = line;
            if !more {
                return Ok(());
            }
        }
        if self.line.len() + bytes.len() > MAX_LINE {
            return self.inner.fail(owner, 500, "upstream");
        }
        self.line.extend_from_slice(bytes);
        Ok(())
    }

    /// The end of the body before the completion boundary, which would have ended the
    /// producer: the last line and a held event line go out, then the failure, Go's 502 when
    /// the apply_patch bridge refuses the cut response and its 408 otherwise.
    fn finish(&mut self, owner: &mut Writer) -> codec::Result<()> {
        let line = std::mem::take(&mut self.line);
        if !line.is_empty() && !self.line_in(owner, &line)? {
            return Ok(());
        }
        if let Some(pending) = self.pending.take()
            && !self.emit(owner, response::summary_event_line(&pending, ""))?
        {
            return Ok(());
        }
        let (_, refused) = self.bridge.finish_stream();
        let status = if refused.is_some() { 502 } else { 408 };
        self.inner.fail(owner, status, "upstream")
    }

    /// One upstream line (Go's scanner loop); false once a failure ended the response.
    fn line_in(&mut self, owner: &mut Writer, line: &[u8]) -> codec::Result<bool> {
        if std::str::from_utf8(line).is_err() {
            self.put(owner, line)?;
            return Ok(true);
        }
        if line.starts_with(b"event:") {
            return match self.pending.replace(line.to_vec()) {
                Some(pending) => self.emit(owner, response::summary_event_line(&pending, "")),
                None => Ok(true),
            };
        }
        let Some(rest) = line.strip_prefix(b"data:") else {
            if let Some(pending) = self.pending.take()
                && !self.emit(owner, response::summary_event_line(&pending, ""))?
            {
                return Ok(false);
            }
            return self.emit(owner, line.to_vec());
        };
        let had_pending = self.pending.is_some();
        let events = response::normalize_summary_data_events(trim_space(rest).to_vec());
        for (i, event) in events.into_iter().enumerate() {
            self.bridge.remember_dispatcher_event(&event);
            let Some(event) = self.unfold(event) else {
                if had_pending && i == 0 {
                    self.pending = None;
                }
                continue;
            };
            let mut event = self.restorer.restore(event);
            if !self.alias.is_empty() {
                event = response::restore_web_search_name(event, &self.alias);
            }
            let Some(event) = self.filter.apply(event).filter(|e| !e.is_empty()) else {
                if had_pending && i == 0 {
                    self.pending = None;
                }
                continue;
            };
            if i > 0 {
                // Each event of a split data line is its own SSE event.
                self.put(owner, b"")?;
            }
            if had_pending {
                let name = gj::get(&event, "type").str().into_owned();
                let event_line = match self.pending.take() {
                    Some(pending) if i == 0 => response::summary_event_line(&pending, &name),
                    _ => format!("event: {name}").into_bytes(),
                };
                if !self.emit(owner, event_line)? {
                    return Ok(false);
                }
            }
            if !self.emit(owner, [&b"data: "[..], &event].concat())? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Keeps a namespace call's identity the same from its first event to its last, as the
    /// ingress requires. A flattened call takes the client's name and namespace when it is
    /// announced. A folded call (the namespace's dispatcher, whose arguments name the tool)
    /// is passed on only as its closing item, which the restorer unwraps: its announcement
    /// and argument events carry the dispatcher instead. `None` drops the event.
    fn unfold(&mut self, mut event: Vec<u8>) -> Option<Vec<u8>> {
        if self.refs.is_empty() || self.bridge.active() || !gj::valid(&event) {
            return Some(event);
        }
        let kind = gj::get(&event, "type").bytes().into_owned();
        match kind.as_slice() {
            b"response.output_item.added" if &*gj::get(&event, "item.type").bytes() == b"function_call" => {
                let Some(tool) = self.refs.get(&text(&gj::get(&event, "item.name"))) else {
                    return Some(event);
                };
                if tool.dispatcher {
                    self.folded.insert(text(&gj::get(&event, "item.id")));
                    return None;
                }
                let (name, namespace) = (tool.name.clone(), tool.namespace.clone());
                gj::set_str(&mut event, "item.name", name);
                gj::set_str(&mut event, "item.namespace", namespace);
                Some(event)
            }
            b"response.function_call_arguments.delta" | b"response.function_call_arguments.done"
                if self.folded.contains(&text(&gj::get(&event, "item_id"))) =>
            {
                None
            }
            _ => Some(event),
        }
    }

    /// `emitTranslatedLine`: the line through the apply_patch bridge, then to the ingress. A
    /// bridge failure ends the response with Go's 502; false then.
    fn emit(&mut self, owner: &mut Writer, line: Vec<u8>) -> codec::Result<bool> {
        let (lines, refused) = self.bridge.stream(&line);
        if refused.is_some() {
            self.inner.fail(owner, 502, "upstream")?;
            return Ok(false);
        }
        let last = lines.len().saturating_sub(1);
        for (i, out) in lines.iter().enumerate() {
            // A converted payload comes as a whole frame; the upstream's own blank line ends
            // the last one, so the completion boundary stays the upstream's.
            let content = out.strip_suffix(b"\n\n").unwrap_or(out);
            match content.strip_prefix(b"data:") {
                Some(data) => self.data(owner, content, trim_space(data))?,
                None => self.put(owner, content)?,
            }
            if content.len() < out.len() && i < last {
                self.put(owner, b"")?;
            }
        }
        Ok(true)
    }

    /// A data line after the bridge: an announced item is kept for a completion without
    /// output, and a completion is passed on with them.
    fn data(&mut self, owner: &mut Writer, line: &[u8], event: &[u8]) -> codec::Result<()> {
        let kind = gj::get(event, "type").bytes().into_owned();
        match kind.as_slice() {
            b"response.output_item.done" => {
                self.items.collect(owner, event)?;
                self.put(owner, line)
            }
            b"response.completed" | b"response.incomplete" => {
                let replay = kind == b"response.completed" && self.replay.is_some() && !self.failures.seen;
                let Some(splice) = self.items.splice(event) else {
                    if replay {
                        self.completed = kept(owner, event)?;
                    }
                    return self.put(owner, line);
                };
                let lease = encode::scratch(owner, |e| {
                    e.bytes(b"data: ")?;
                    e.bytes(&event[..splice.at])?;
                    e.bytes(splice.lead)?;
                    self.items.write(e)?;
                    e.bytes(&event[splice.end..])
                })?;
                let mut at = 0;
                while at < lease.len() {
                    let chunk = lease.chunk(at).map_err(codec::Error::Storage)?;
                    self.feed(owner, chunk)?;
                    at += chunk.len();
                }
                self.feed(owner, b"\n")?;
                if replay {
                    self.completed = Some(self.items.take());
                }
                Ok(())
            }
            _ => self.put(owner, line),
        }
    }

    /// Passes one line and its end to the ingress. A blank line ends the event before it; when
    /// that is the completion, the ingress has accepted it and replay caches it.
    fn put(&mut self, owner: &mut Writer, content: &[u8]) -> codec::Result<()> {
        if !content.is_empty() {
            self.feed(owner, content)?;
            return self.feed(owner, b"\n");
        }
        self.feed(owner, b"\n")?;
        if let (Some(leases), Some((store, scope))) = (self.completed.take(), &self.replay) {
            let mut items = Vec::with_capacity(leases.len());
            for lease in &leases {
                let mut item = Vec::with_capacity(lease.len());
                while item.len() < lease.len() {
                    item.extend_from_slice(lease.chunk(item.len()).map_err(codec::Error::Storage)?);
                }
                if replay::kept(&gj::parse(&item)) {
                    items.push(item);
                }
            }
            replay::cache_items(store, scope, &items);
        }
        Ok(())
    }

    /// Hands `bytes` to the ingress, and to the failure reader while a completion may still
    /// be cached.
    fn feed(&mut self, owner: &mut Writer, bytes: &[u8]) -> codec::Result<()> {
        self.inner.advance(owner, bytes)?;
        let keep = self.replay.is_some() && !self.failures.seen;
        self.failures.read(bytes, keep);
        Ok(())
    }
}

/// The output items of a completion replay keeps, each in its own scratch lease; `None`
/// when the completion has no output array.
fn kept(owner: &mut Writer, event: &[u8]) -> codec::Result<Option<Vec<ScratchLease>>> {
    let output = gj::get(event, "response.output");
    if !output.is_array() {
        return Ok(None);
    }
    let mut leases = Vec::new();
    for item in output.array().iter().filter(|item| replay::kept(item)) {
        leases.push(encode::scratch(owner, |e| e.bytes(item.raw()))?);
    }
    Ok(Some(leases))
}

/// Where a terminal event's `output` takes the collected items: `event[at..end]` becomes
/// `lead` and the items' array.
struct Splice {
    at: usize,
    end: usize,
    lead: &'static [u8],
}

/// The items `response.output_item.done` announced (`xaiCollectOutputItemDone`), by output
/// index, then those without one, held in scratch leases of the response stream.
#[derive(Default)]
struct Items {
    by_index: BTreeMap<i64, ScratchLease>,
    unindexed: Vec<ScratchLease>,
}

impl Items {
    fn collect(&mut self, owner: &mut Writer, event: &[u8]) -> codec::Result<()> {
        let item = gj::get(event, "item");
        if !item.exists() || item.kind != Kind::Json {
            return Ok(());
        }
        let lease = encode::scratch(owner, |e| e.bytes(item.raw()))?;
        let index = gj::get(event, "output_index");
        if index.exists() {
            self.by_index.insert(index.int(), lease);
        } else {
            self.unindexed.push(lease);
        }
        Ok(())
    }

    /// `xaiPatchCompletedOutput`: where a terminal event's `output` takes the collected
    /// items, when that output is empty, not an array or absent and items were announced.
    fn splice(&self, event: &[u8]) -> Option<Splice> {
        if self.by_index.is_empty() && self.unindexed.is_empty() {
            return None;
        }
        let output = gj::get(event, "response.output");
        if output.is_array() && !output.array().is_empty() {
            return None;
        }
        if output.exists() {
            let (at, end) = (output.index, output.index + output.raw().len());
            return (event.get(at..end) == Some(output.raw())).then_some(Splice { at, end, lead: b"" });
        }
        // An absent `output` goes in before the response object's closing brace.
        let response = gj::get(event, "response");
        let (start, len) = (response.index, response.raw().len());
        if !response.is_object() || event.get(start..start + len) != Some(response.raw()) {
            return None;
        }
        let inner = trim_space(&response.raw()[1..len - 1]);
        let lead: &[u8] = if inner.is_empty() {
            b"\"output\":"
        } else {
            b",\"output\":"
        };
        let at = start + len - 1;
        Some(Splice { at, end: at, lead })
    }

    /// The collected items, in output order, for replay once the completion is passed on.
    fn take(&mut self) -> Vec<ScratchLease> {
        let indexed = std::mem::take(&mut self.by_index).into_values();
        indexed.chain(std::mem::take(&mut self.unindexed)).collect()
    }

    /// The collected items as one JSON array.
    fn write(&self, e: &mut Encoder<'_, '_>) -> codec::Result<()> {
        e.bytes(b"[")?;
        for (i, lease) in self.by_index.values().chain(&self.unindexed).enumerate() {
            if i > 0 {
                e.bytes(b",")?;
            }
            let mut at = 0;
            while at < lease.len() {
                let chunk = lease.chunk(at).map_err(codec::Error::Storage)?;
                e.bytes(chunk)?;
                at += chunk.len();
            }
        }
        e.bytes(b"]")
    }
}
