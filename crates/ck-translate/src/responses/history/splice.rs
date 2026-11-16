//! Writes a reduced response as canonical input messages and replay entries, imports them into
//! the request document in one piece, and binds every entry to the node it describes.
use super::{Block, Call, Entry, Part, Reduction, Slot};
use crate::codec::{
    Error, Result,
    document::{Document, Value},
    encode::{self, Encoder},
    provenance::Producer,
    table::Table,
};
use ck_stream::Writer;

/// One projected message: the item group whose blocks and calls it holds.
struct Message {
    group: usize,
    role: &'static str,
    compaction: bool,
}

impl Reduction {
    pub(super) fn splice(self, owner: &mut Writer, doc: &mut Document, producer: Producer<'_>) -> Result<()> {
        let messages = self.messages(owner)?;
        let entries = self.entries(owner, &messages)?;
        let root = doc.root();
        let list = doc.required(root, "messages")?;
        let x = doc.required(root, "x_ck")?;
        let base = Base {
            message: doc.children(list)?.count() as u64,
            replay: match doc.member(x, "replay")? {
                Some(replay) => doc.children(replay)?.count() as u64,
                None => 0,
            },
        };
        let lease = encode::scratch(owner, |e| self.write(e, &messages, &entries, base, producer))?;
        let imported = doc.import_json(owner, lease)?;
        let replay = list_of(doc, x, "replay")?;
        let original = list_of(doc, x, "original")?;
        let moved = Moved {
            messages: move_all(owner, doc, doc.required(imported, "messages")?, list)?,
            entries: move_all(owner, doc, doc.required(imported, "replay")?, replay)?,
            originals: move_all(owner, doc, doc.required(imported, "original")?, original)?,
        };
        self.bind(owner, doc, &messages, &entries, &moved)
    }

    /// Item groups that hold a part or a call, in output order.
    fn messages(&self, owner: &mut Writer) -> Result<Table<Message>> {
        let mut messages: Table<Message> = Table::new(owner)?;
        for item in self.items.iter() {
            if messages.last().is_some_and(|m| m.group == item.group) {
                continue;
            }
            if self.parts(item.group).next().is_some() || self.item_calls(item.group).next().is_some() {
                messages.push(Message {
                    group: item.group,
                    role: item.role,
                    compaction: item.compaction,
                })?;
            }
        }
        Ok(messages)
    }

    fn parts(&self, group: usize) -> impl Iterator<Item = (&Block, Part)> {
        self.blocks
            .iter()
            .filter(move |b| self.items[b.item].group == group)
            .filter_map(|b| b.part.map(|part| (b, part)))
    }

    fn item_calls(&self, group: usize) -> impl Iterator<Item = &Call> {
        self.calls.iter().filter(move |c| self.items[c.item].group == group)
    }

    /// Every replay entry, in the order parts and calls name them.
    fn entries(&self, owner: &mut Writer, messages: &[Message]) -> Result<Table<Entry>> {
        let mut entries = Table::new(owner)?;
        for (message, m) in messages.iter().enumerate() {
            for (part_index, (block, part)) in self.parts(m.group).enumerate() {
                let slot = Slot::Part(part_index);
                let entry = match (part, block.signature) {
                    (Part::Encrypted, Some(("responses_encrypted", signature))) => Entry {
                        message,
                        slot,
                        whole: true,
                        carrier: None,
                        kind: "responses_encrypted",
                        signature: Some(signature),
                    },
                    (Part::Encrypted, _) => return Err(Error::Schema),
                    (Part::Redacted, None) => Entry {
                        message,
                        slot,
                        whole: false,
                        carrier: None,
                        kind: "anthropic",
                        signature: None,
                    },
                    (Part::Reasoning, Some(("anthropic", signature))) => Entry {
                        message,
                        slot,
                        whole: false,
                        carrier: None,
                        kind: "anthropic",
                        signature: Some(signature),
                    },
                    (Part::Text | Part::Reasoning | Part::Summary, Some(("gemini", signature))) => Entry {
                        message,
                        slot,
                        whole: false,
                        carrier: Some("signature"),
                        kind: "gemini",
                        signature: Some(signature),
                    },
                    (_, None) => continue,
                    _ => return Err(Error::Schema),
                };
                entries.push(entry)?;
            }
            for (call_index, call) in self.item_calls(m.group).enumerate() {
                let block = call.block.and_then(|b| {
                    self.blocks
                        .iter()
                        .find(|x| x.item == call.item && x.block == b && x.part.is_none())
                });
                match block.and_then(|b| b.signature) {
                    Some(("gemini", signature)) => entries.push(Entry {
                        message,
                        slot: Slot::Call(call_index),
                        whole: false,
                        carrier: Some("signature"),
                        kind: "gemini",
                        signature: Some(signature),
                    })?,
                    Some(_) => return Err(Error::Schema),
                    None => continue,
                };
            }
        }
        Ok(entries)
    }

    fn write(
        &self,
        e: &mut Encoder<'_, '_>,
        messages: &[Message],
        entries: &[Entry],
        base: Base,
        producer: Producer<'_>,
    ) -> Result<()> {
        e.bytes(b"{\"messages\":[")?;
        for (index, m) in messages.iter().enumerate() {
            comma(e, index)?;
            self.write_message(e, index, m, entries, base)?;
        }
        e.bytes(b"],\"replay\":[")?;
        for (index, entry) in entries.iter().enumerate() {
            comma(e, index)?;
            self.write_entry(e, entry, messages, base, producer)?;
        }
        e.bytes(b"],\"original\":[")?;
        for (index, _) in messages.iter().filter(|m| m.compaction).enumerate() {
            comma(e, index)?;
            e.bytes(b"{\"protocol\":\"responses\",\"pointer\":\"/type\",\"value\":\"compaction\"}")?;
        }
        e.bytes(b"]}")
    }

    fn write_message(
        &self,
        e: &mut Encoder<'_, '_>,
        at: usize,
        m: &Message,
        entries: &[Entry],
        base: Base,
    ) -> Result<()> {
        e.bytes(b"{\"role\":")?;
        e.string(m.role)?;
        if self.parts(m.group).next().is_some() {
            e.bytes(b",\"content\":[")?;
            for (index, (block, part)) in self.parts(m.group).enumerate() {
                comma(e, index)?;
                e.bytes(b"{\"type\":")?;
                e.string(part.name())?;
                match part {
                    Part::Encrypted => {}
                    Part::Redacted => {
                        e.bytes(b",\"data\":")?;
                        self.staging.write(block.text, e)?;
                    }
                    _ => {
                        e.bytes(b",\"text\":")?;
                        self.staging.write(block.text, e)?;
                    }
                }
                if matches!(part, Part::Encrypted | Part::Redacted) {
                    let entry = entries
                        .iter()
                        .position(|n| n.message == at && n.slot == Slot::Part(index))
                        .ok_or(Error::Schema)?;
                    e.bytes(b",\"replay_index\":")?;
                    e.unsigned(base.replay + entry as u64)?;
                }
                e.bytes(b"}")?;
            }
            e.bytes(b"]")?;
        }
        if self.item_calls(m.group).next().is_some() {
            e.bytes(b",\"tool_calls\":[")?;
            for (index, call) in self.item_calls(m.group).enumerate() {
                comma(e, index)?;
                e.bytes(b"{\"type\":")?;
                e.string(if call.custom { "custom" } else { "function" })?;
                e.bytes(b",\"id\":")?;
                self.staging.write(call.id, e)?;
                e.bytes(b",\"arguments_format\":")?;
                e.string(if call.custom { "text" } else { "json" })?;
                e.bytes(b",\"function\":{\"name\":")?;
                self.staging.write(call.name, e)?;
                // JSON arguments are imported as a value of their own (`bind`).
                if call.custom {
                    e.bytes(b",\"arguments\":")?;
                    self.staging.write(call.arguments, e)?;
                }
                e.bytes(b"}}")?;
            }
            e.bytes(b"]")?;
        }
        e.bytes(b"}")
    }

    fn write_entry(
        &self,
        e: &mut Encoder<'_, '_>,
        entry: &Entry,
        messages: &[Message],
        base: Base,
        producer: Producer<'_>,
    ) -> Result<()> {
        let parts = self.parts(messages[entry.message].group).count() as u64;
        let message = base.message + entry.message as u64;
        e.bytes(b"{\"message_index\":")?;
        e.unsigned(message)?;
        e.bytes(b",\"block_index\":")?;
        e.unsigned(match entry.slot {
            Slot::Part(index) => index as u64,
            Slot::Call(index) => parts + index as u64,
        })?;
        e.bytes(b",\"item_id\":\"h")?;
        e.unsigned(message)?;
        e.bytes(b"\",\"kind\":")?;
        e.string(entry.kind)?;
        e.bytes(b",\"source\":{\"protocol\":")?;
        e.string(match entry.kind {
            "responses_encrypted" => "responses",
            kind => kind,
        })?;
        e.bytes(b"}")?;
        if let Some(signature) = entry.signature {
            e.bytes(b",\"signature\":")?;
            self.staging.write(signature, e)?;
        }
        if messages[entry.message].compaction {
            e.bytes(b",\"compaction\":true")?;
        }
        e.bytes(b",\"producer\":{\"provider\":")?;
        e.string(producer.provider)?;
        e.bytes(b",\"credential\":")?;
        e.string(producer.credential)?;
        e.bytes(b"}}")
    }

    /// Binds entries and originals to their nodes and imports each call's JSON arguments.
    fn bind(
        &self,
        owner: &mut Writer,
        doc: &mut Document,
        messages: &[Message],
        entries: &[Entry],
        moved: &Moved,
    ) -> Result<()> {
        for (entry, node) in entries.iter().zip(moved.entries.iter()) {
            let message = moved.messages[entry.message];
            let target = if entry.whole {
                message
            } else {
                match entry.slot {
                    Slot::Part(index) => nth(doc, doc.required(message, "content")?, index)?,
                    Slot::Call(index) => nth(doc, doc.required(message, "tool_calls")?, index)?,
                }
            };
            doc.bind_origin(*node, target, entry.carrier)?;
        }
        let compactions = messages.iter().enumerate().filter(|(_, m)| m.compaction);
        for ((index, _), node) in compactions.zip(moved.originals.iter()) {
            doc.bind_origin(*node, moved.messages[index], None)?;
        }
        for (index, m) in messages.iter().enumerate() {
            for (call_index, call) in self.item_calls(m.group).enumerate() {
                if call.custom || self.staging.node(call.arguments)?.first.is_none() {
                    continue;
                }
                let lease = encode::scratch(owner, |e| {
                    let mut chars = self.staging.chars(call.arguments)?;
                    while let Some(c) = chars.next_scalar()? {
                        e.bytes(c.encode_utf8(&mut [0; 4]).as_bytes())?;
                    }
                    Ok(())
                })?;
                let arguments = doc.import_json(owner, lease)?;
                doc.rename(arguments, "arguments")?;
                let target = nth(doc, doc.required(moved.messages[index], "tool_calls")?, call_index)?;
                doc.append(doc.required(target, "function")?, arguments)?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct Base {
    message: u64,
    replay: u64,
}

/// The nodes the import moved into the request, in order.
struct Moved {
    messages: Table<usize>,
    entries: Table<usize>,
    originals: Table<usize>,
}

/// The `x_ck` list `name`, created on first use.
fn list_of(doc: &mut Document, x: usize, name: &'static str) -> Result<usize> {
    match doc.member(x, name)? {
        Some(list) if matches!(doc.node(list)?.value, Value::Array) => Ok(list),
        Some(_) => Err(Error::Schema),
        None => doc.set(x, name, Value::Array),
    }
}

/// Moves every child of `from` to the end of `to`; the moved nodes, in order.
fn move_all(owner: &mut Writer, doc: &mut Document, from: usize, to: usize) -> Result<Table<usize>> {
    let mut moved = Table::new(owner)?;
    while let Some(child) = doc.node(from)?.first {
        doc.detach(from, child)?;
        doc.append(to, child)?;
        moved.push(child)?;
    }
    Ok(moved)
}

fn nth(doc: &Document, list: usize, index: usize) -> Result<usize> {
    doc.children(list)?.nth(index).ok_or(Error::Schema)
}

fn comma(e: &mut Encoder<'_, '_>, index: usize) -> Result<()> {
    if index > 0 {
        e.bytes(b",")?;
    }
    Ok(())
}
