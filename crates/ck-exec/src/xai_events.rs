//! The xAI upstream body on its way into the Responses response ingress (`xai_exec`):
//! Go's per-line xAI rules (xai_executor.go `ExecuteStream`), applied as the body arrives.
//!
//! - Native reasoning events and server-tool traces keep their types and indices.
//! - Namespace calls and the aliased client `web_search` function are restored.
//! - The apply_patch bridge turns function calls back into the client's custom tool; its
//!   failure ends the response with Go's 502.
//! - A completion whose `output` is empty or absent gets the items
//!   `response.output_item.done` announced. They wait in scratch leases of the response
//!   stream, never on the heap.
//! - A completed response is cached for reasoning replay from the ingress's own parse of it,
//!   once the ingress accepted it as the end of the stream: a cut, refused or failed
//!   completion caches nothing, nor one after an upstream failure, whose input the ingress
//!   ignores. Replay copies no event into the response stream, so it cannot cost the
//!   response its storage.
//! - The body ending before the completion boundary fails with Go's 408, and an upstream
//!   failure with its own status; either one cutting off an apply_patch call fails as the
//!   bridge's 502.
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

use crate::media_exec::MediaIngress;
use crate::xai::MAX_LINE;
use crate::xai_replay::{self as replay, ReplayScope};
use crate::xai_replay_items;
use crate::xai_request::Prepared;
use crate::xai_response::{self as response, NamespaceRefs, NamespaceRestorer, text};

/// The response decoder of one xAI attempt: generation through the xAI line rules, a
/// buffered compact whose success clears the session's replay, or a media operation.
pub enum XaiIngress {
    Generate(Box<Events>),
    Compact(Box<Compaction>),
    Media(MediaIngress),
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
            Self::Media(media) => media.flush_deadline(),
        }
    }

    fn flush(&mut self, owner: &mut Writer) -> codec::Result<()> {
        match self {
            Self::Generate(events) => events.inner.flush(owner),
            Self::Compact(compact) => compact.ingress.flush(owner),
            Self::Media(media) => media.flush(owner),
        }
    }

    fn advance(&mut self, owner: &mut Writer, bytes: &[u8]) -> codec::Result<()> {
        match self {
            Self::Generate(events) => events.advance(owner, bytes),
            Self::Compact(compact) => compact.ingress.advance(owner, bytes),
            Self::Media(media) => media.advance(owner, bytes),
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
            Self::Media(media) => media.finish(owner),
        }
    }

    fn fail(&mut self, owner: &mut Writer, status: u16, scope: &'static str) -> codec::Result<()> {
        match self {
            Self::Generate(events) => events.fail(owner, status, scope),
            Self::Compact(compact) => compact.ingress.fail(owner, status, scope),
            Self::Media(media) => media.fail(owner, status, scope),
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
    restorer: NamespaceRestorer,
    /// The flattened and folded namespace tool names the request sent.
    refs: NamespaceRefs,
    /// Folded calls in flight, by item id.
    folded: HashSet<String>,
    alias: String,
}

impl Events {
    pub(crate) fn new(mut inner: Ingress, prepared: Prepared, store: Arc<replay::Store>, declarations: &[u8]) -> Self {
        inner.xai(declarations);
        let scope = prepared.replay_scope;
        if scope.valid() {
            inner.on_completed(Box::new(move |doc, response| {
                xai_replay_items::cache_completion(&store, &scope, doc, response);
            }));
        }
        Self {
            inner,
            line: Vec::new(),
            pending: None,
            bridge: prepared.apply_patch,
            items: Items::default(),
            refs: prepared.namespace_tools.clone(),
            restorer: NamespaceRestorer::new(prepared.namespace_tools),
            folded: HashSet::new(),
            alias: prepared.web_search_alias,
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
            && !self.emit(owner, pending)?
        {
            return Ok(());
        }
        self.fail(owner, 408, "upstream")
    }

    /// The upstream failed, or its body ended, before the completion: an apply_patch call
    /// still open fails as the bridge's 502, as Go reports a cut patch, and any other cut as
    /// `status`.
    fn fail(&mut self, owner: &mut Writer, status: u16, scope: &'static str) -> codec::Result<()> {
        let (_, refused) = self.bridge.finish_stream();
        let status = if refused.is_some() { 502 } else { status };
        self.inner.fail(owner, status, scope)
    }

    /// One upstream line (Go's scanner loop); false once a failure ended the response.
    fn line_in(&mut self, owner: &mut Writer, line: &[u8]) -> codec::Result<bool> {
        if std::str::from_utf8(line).is_err() {
            self.put(owner, line)?;
            return Ok(true);
        }
        if line.starts_with(b"event:") {
            return match self.pending.replace(line.to_vec()) {
                Some(pending) => self.emit(owner, pending),
                None => Ok(true),
            };
        }
        let Some(rest) = line.strip_prefix(b"data:") else {
            if let Some(pending) = self.pending.take()
                && !self.emit(owner, pending)?
            {
                return Ok(false);
            }
            return self.emit(owner, line.to_vec());
        };
        let event = trim_space(rest).to_vec();
        self.bridge.remember_dispatcher_event(&event);
        let Some(event) = self.unfold(event) else {
            self.pending = None;
            return Ok(true);
        };
        let mut event = self.restorer.restore(event);
        if !self.alias.is_empty() {
            event = response::restore_web_search_name(event, &self.alias);
        }
        if let Some(pending) = self.pending.take()
            && !self.emit(owner, pending)?
        {
            return Ok(false);
        }
        self.emit(owner, [&b"data: "[..], &event].concat())
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
                let Some(splice) = self.items.splice(event) else {
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
                self.feed(owner, b"\n")
            }
            _ => self.put(owner, line),
        }
    }

    /// Passes one line and its end to the ingress.
    fn put(&mut self, owner: &mut Writer, content: &[u8]) -> codec::Result<()> {
        if !content.is_empty() {
            self.feed(owner, content)?;
        }
        self.feed(owner, b"\n")
    }

    fn feed(&mut self, owner: &mut Writer, bytes: &[u8]) -> codec::Result<()> {
        self.inner.advance(owner, bytes)
    }
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
