//! Responses history projection (D2-PLAN §2.3, §2.4 and §2.7, as cut by the owner on
//! 2026-11-15: no cooker envelope, provider signatures pass through unchanged).
//!
//! [`project`] appends one stored canonical response to a Responses client's canonical request
//! as input messages, one per output item: text, readable reasoning and summaries, tool calls
//! and compaction items. Every opaque provider member becomes an `x_ck.replay` entry holding its
//! native bytes: an Anthropic thinking signature or redacted block, a Gemini thought signature,
//! or Responses encrypted reasoning or compaction. Each entry is tagged with the response's
//! [`Producer`], so `codec::provenance::admit` restores it only to that upstream account; a
//! history that passed through X, Y and Z keeps each entry's own tag.
//!
//! [`truncate`] drops the history a compaction item replaces.
use super::request::native;
use crate::codec::{
    Error, Result,
    document::{Document, Value},
    event::{Kind, LocalId},
    identity, internal,
    provenance::{self, Producer},
    table::Table,
};
use ck_stream::{Reader, Writer};

/// A canonical content part a stored block becomes.
#[derive(Clone, Copy, Eq, PartialEq)]
enum Part {
    Text,
    Reasoning,
    Summary,
    Encrypted,
    Redacted,
}

impl Part {
    fn name(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Reasoning => "reasoning",
            Self::Summary => "reasoning_summary",
            Self::Encrypted => "encrypted",
            Self::Redacted => "redacted",
        }
    }
}

/// One output item. Consecutive message items of one role share a message, so a Gemini
/// candidate, whose parts arrive as items of their own, stays one turn.
struct Item {
    local: Option<LocalId>,
    role: &'static str,
    compaction: bool,
    /// A `message` item, which may share its message with the item before it.
    message: bool,
    /// The message this item's content goes to.
    group: usize,
}

/// One content block of an item. `part` is `None` for a tool call's block.
struct Block {
    item: usize,
    block: u64,
    part: Option<Part>,
    /// Staging fragments of the text, reasoning or redacted data.
    text: usize,
    /// The replay kind and staging fragments of the block's signature.
    signature: Option<(&'static str, usize)>,
}

struct Call {
    item: usize,
    local: LocalId,
    block: Option<u64>,
    /// Staging copies of the client-facing call id and the name.
    id: usize,
    name: usize,
    custom: bool,
    arguments: usize,
}

/// A replay entry of the projection: which message and slot it belongs to, and what it binds.
struct Entry {
    message: usize,
    slot: Slot,
    /// Bound to the message (Responses reasoning and compaction) instead of its slot's node.
    whole: bool,
    carrier: Option<&'static str>,
    kind: &'static str,
    signature: Option<usize>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Slot {
    Part(usize),
    Call(usize),
}

/// Appends the stored response that `reader` reads from its start to `doc`, a Responses
/// client's canonical request. `stored` is an allocation handle of the stored response stream;
/// `owner` is the writer of `doc`'s stream. Opaque members are tagged with `producer`.
///
/// Only choice 0 is projected. A response that did not end with `done`, or holds content a
/// Responses request cannot carry back (server tool results, code execution, media, refusals),
/// is refused rather than shortened.
pub fn project(
    owner: &mut Writer,
    doc: &mut Document,
    stored: &mut Writer,
    reader: &mut Reader,
    producer: Producer<'_>,
) -> Result<()> {
    let mut reduction = Reduction {
        staging: Document::streaming(stored)?,
        items: Table::new(stored)?,
        blocks: Table::new(stored)?,
        calls: Table::new(stored)?,
    };
    loop {
        let (kind, mut event) = internal::next(stored, reader)?;
        match kind {
            Kind::Done => break,
            Kind::Error | Kind::Cancelled => return Err(Error::Schema),
            _ => reduction.record(kind, &mut event)?,
        }
    }
    reduction.splice(owner, doc, producer)
}

/// Drops every message before the last compaction item except system and developer messages:
/// the compaction item stands for them, and Anthropic refuses messages before it (§2.7).
pub fn truncate(owner: &mut Writer, doc: &mut Document) -> Result<()> {
    let messages = doc.required(doc.root(), "messages")?;
    let mut last = None;
    for message in doc.children(messages)? {
        if native::typed(doc, native::item_type(doc, message)?, "compaction")? {
            last = Some(message);
        }
    }
    let Some(last) = last else {
        return Ok(());
    };
    let mut next = doc.node(messages)?.first;
    while let Some(message) = next.filter(|&m| m != last) {
        next = doc.node(message)?.next;
        let role = doc.required(message, "role")?;
        if !doc.string_is(role, "system")? && !doc.string_is(role, "developer")? {
            doc.detach(messages, message)?;
        }
    }
    provenance::admit::prune(owner, doc, |_| false)
}

struct Reduction {
    /// Holds copies of the stored records' texts, in the stored stream's generation.
    staging: Document,
    items: Table<Item>,
    blocks: Table<Block>,
    calls: Table<Call>,
}

impl Reduction {
    fn record(&mut self, kind: Kind, event: &mut Document) -> Result<()> {
        let x = event.required(event.root(), "x_ck")?;
        if let Some(choice) = event.member(x, "choice_index")?
            && event.unsigned(choice)? != 0
        {
            return Ok(());
        }
        match kind {
            Kind::ItemStart => self.item(event, x),
            Kind::BlockStart => self.block(event, x),
            Kind::Chunk => self.chunk(event, x),
            Kind::ReasoningSignature => self.signature(event, x),
            Kind::ReasoningRedacted => {
                let block = self.find_block(event, x)?;
                let data = event.required(event.root(), "data")?;
                self.copy_into(event, &[(self.blocks[block].text, data)])
            }
            Kind::ToolStart => self.tool(event, x),
            Kind::Compact => self.compact(event),
            // Lifecycle, accounting and metadata records carry nothing a request sends back.
            Kind::ResponseStart
            | Kind::ItemEnd
            | Kind::BlockEnd
            | Kind::ToolEnd
            | Kind::Usage
            | Kind::Heartbeat
            | Kind::Annotation
            | Kind::Grounding
            | Kind::UrlContext
            | Kind::Opaque => Ok(()),
            _ => Err(Error::Unsupported),
        }
    }

    fn item(&mut self, event: &Document, x: usize) -> Result<()> {
        let local = identity::local(event, event.required(x, "item_id")?)?;
        let kind = event.required(event.root(), "kind")?;
        if !["message", "reasoning", "function_call", "custom_tool_call"]
            .into_iter()
            .try_fold(false, |any, name| Ok::<_, Error>(any || event.string_is(kind, name)?))?
        {
            return Err(Error::Unsupported);
        }
        let role = match event.member(event.root(), "role")? {
            Some(role) => role_name(event, role)?,
            None => "assistant",
        };
        let message = event.string_is(kind, "message")?;
        self.push_item(Some(local), role, false, message)?;
        Ok(())
    }

    fn push_item(
        &mut self,
        local: Option<LocalId>,
        role: &'static str,
        compaction: bool,
        message: bool,
    ) -> Result<usize> {
        let group = match self.items.last() {
            Some(last) if message && last.message && last.role == role => last.group,
            Some(last) => last.group + 1,
            None => 0,
        };
        self.items.push(Item {
            local,
            role,
            compaction,
            message,
            group,
        })
    }

    fn block(&mut self, event: &Document, x: usize) -> Result<()> {
        let item = self.find_item(event, x)?;
        let block = event.unsigned(event.required(x, "block_index")?)?;
        let root = event.root();
        let kind = event.required(root, "kind")?;
        let encrypted = match event.member(root, "attributes")? {
            Some(attributes) => match event.member(attributes, "type")? {
                Some(kind) => event.string_is(kind, "encrypted")?,
                None => false,
            },
            None => false,
        };
        let part = if event.string_is(kind, "text")? {
            Some(Part::Text)
        } else if event.string_is(kind, "thinking")? || event.string_is(kind, "reasoning")? {
            Some(if encrypted { Part::Encrypted } else { Part::Reasoning })
        } else if event.string_is(kind, "reasoning_summary")? {
            Some(Part::Summary)
        } else if event.string_is(kind, "redacted")? {
            Some(Part::Redacted)
        } else if event.string_is(kind, "function")? {
            None
        } else {
            return Err(Error::Unsupported);
        };
        let text = self.staging.add(None, Value::Fragments)?;
        self.blocks.push(Block {
            item,
            block,
            part,
            text,
            signature: None,
        })?;
        Ok(())
    }

    /// Appends a chunk's content, reasoning and call argument deltas to their blocks and calls.
    fn chunk(&mut self, event: &mut Document, x: usize) -> Result<()> {
        let Some(delta) = delta(event)? else {
            return Ok(());
        };
        let mut copies = [(0, 0); 3];
        let mut count = 0;
        for key in ["content", "reasoning_content"] {
            if let Some(text) = event.member(delta, key)?
                && !matches!(event.node(text)?.value, Value::Null)
            {
                copies[count] = (self.blocks[self.find_block(event, x)?].text, text);
                count += 1;
            }
        }
        if let Some(refusal) = event.member(delta, "refusal")?
            && !matches!(event.node(refusal)?.value, Value::Null)
        {
            return Err(Error::Unsupported);
        }
        if let Some(calls) = event.member(delta, "tool_calls")? {
            for call in event.children(calls)? {
                if let Some(function) = event.member(call, "function")?
                    && let Some(arguments) = event.member(function, "arguments")?
                {
                    let target = self.find_call(event, x)?;
                    *copies.get_mut(count).ok_or(Error::Schema)? = (self.calls[target].arguments, arguments);
                    count += 1;
                }
            }
        }
        self.copy_into(event, &copies[..count])
    }

    fn signature(&mut self, event: &mut Document, x: usize) -> Result<()> {
        let block = self.find_block(event, x)?;
        let root = event.root();
        let kind = event.required(root, "kind")?;
        let kind = ["anthropic", "gemini", "responses_encrypted"]
            .into_iter()
            .find(|name| event.string_is(kind, name).unwrap_or(false))
            .ok_or(Error::Schema)?;
        let fragments = match self.blocks[block].signature {
            Some((existing, fragments)) if existing == kind => fragments,
            Some(_) => return Err(Error::Schema),
            None => {
                let fragments = self.staging.add(None, Value::Fragments)?;
                self.blocks[block].signature = Some((kind, fragments));
                fragments
            }
        };
        let fragment = event.required(root, "fragment")?;
        self.copy_into(event, &[(fragments, fragment)])
    }

    fn tool(&mut self, event: &mut Document, x: usize) -> Result<()> {
        let item = self.find_item(event, x)?;
        let root = event.root();
        let local = identity::local(event, event.required(x, "tool_call_id")?)?;
        let kind = event.required(root, "kind")?;
        let custom = event.string_is(kind, "custom")?;
        if !custom && !event.string_is(kind, "function")? || event.member(root, "namespace")?.is_some() {
            return Err(Error::Unsupported);
        }
        let block = event.member(x, "block_index")?.map(|b| event.unsigned(b)).transpose()?;
        // The call id a Responses client was shown (`responses::egress`): the source's own id,
        // else the canonical one.
        let ids = match event.member(x, "source")? {
            Some(source) => event.member(source, "ids")?,
            None => None,
        };
        let id = match ids {
            Some(ids) => event.member(ids, "call_id")?.or(event.member(ids, "id")?),
            None => None,
        }
        .unwrap_or(event.required(x, "tool_call_id")?);
        let name = event.required(root, "name")?;
        let adopted = self.staging.adopt_record(event)?;
        let id = self.staging.copy_adopted(event, id, adopted, None)?;
        let name = self.staging.copy_adopted(event, name, adopted, None)?;
        let arguments = self.staging.add(None, Value::Fragments)?;
        self.calls.push(Call {
            item,
            local,
            block,
            id,
            name,
            custom,
            arguments,
        })?;
        Ok(())
    }

    /// A compact output item: a compaction item, or a message the compaction kept.
    fn compact(&mut self, event: &mut Document) -> Result<()> {
        let item = event.required(event.root(), "item")?;
        let kind = event.required(item, "type")?;
        let mut copies = [(0, 0); 8];
        let mut count = 0;
        if event.string_is(kind, "compaction")? {
            let data = event.required(item, "encrypted_content")?;
            let index = self.compact_item("assistant", true)?;
            let fragments = self.staging.add(None, Value::Fragments)?;
            self.push_block(index, Part::Encrypted, Some(("responses_encrypted", fragments)))?;
            copies[0] = (fragments, data);
            count = 1;
        } else if event.string_is(kind, "message")? {
            let role = role_name(event, event.required(item, "role")?)?;
            let index = self.compact_item(role, false)?;
            for part in event.children(event.required(item, "content")?)? {
                let text = event.required(part, "text")?;
                let block = self.push_block(index, Part::Text, None)?;
                *copies.get_mut(count).ok_or(Error::Unsupported)? = (self.blocks[block].text, text);
                count += 1;
            }
        } else {
            return Err(Error::Unsupported);
        }
        self.copy_into(event, &copies[..count])
    }

    fn compact_item(&mut self, role: &'static str, compaction: bool) -> Result<usize> {
        self.push_item(None, role, compaction, !compaction)
    }

    fn push_block(&mut self, item: usize, part: Part, signature: Option<(&'static str, usize)>) -> Result<usize> {
        let text = self.staging.add(None, Value::Fragments)?;
        let block = self.blocks.iter().filter(|b| b.item == item).count() as u64;
        self.blocks.push(Block {
            item,
            block,
            part: Some(part),
            text,
            signature,
        })
    }

    /// Adopts the record once and appends a copy of each `(fragments, text)` text.
    fn copy_into(&mut self, event: &mut Document, copies: &[(usize, usize)]) -> Result<()> {
        if copies.is_empty() {
            return Ok(());
        }
        let adopted = self.staging.adopt_record(event)?;
        for &(fragments, text) in copies {
            let copy = self.staging.copy_adopted(event, text, adopted, None)?;
            self.staging.append(fragments, copy)?;
        }
        Ok(())
    }

    fn find_item(&self, event: &Document, x: usize) -> Result<usize> {
        let local = identity::local(event, event.required(x, "item_id")?)?;
        self.items
            .iter()
            .rposition(|item| item.local == Some(local))
            .ok_or(Error::Schema)
    }

    fn find_block(&self, event: &Document, x: usize) -> Result<usize> {
        let item = self.find_item(event, x)?;
        let block = event.unsigned(event.required(x, "block_index")?)?;
        self.blocks
            .iter()
            .rposition(|b| b.item == item && b.block == block)
            .ok_or(Error::Schema)
    }

    fn find_call(&self, event: &Document, x: usize) -> Result<usize> {
        let local = identity::local(event, event.required(x, "tool_call_id")?)?;
        self.calls.iter().rposition(|c| c.local == local).ok_or(Error::Schema)
    }
}

mod splice;

/// The first choice's `delta` of a chunk, if any.
fn delta(event: &Document) -> Result<Option<usize>> {
    let Some(choices) = event.member(event.root(), "choices")? else {
        return Ok(None);
    };
    match event.children(choices)?.next() {
        Some(choice) => event.member(choice, "delta"),
        None => Ok(None),
    }
}

fn role_name(doc: &Document, role: usize) -> Result<&'static str> {
    ["assistant", "user", "system", "developer"]
        .into_iter()
        .find(|name| doc.string_is(role, name).unwrap_or(false))
        .ok_or(Error::Schema)
}
