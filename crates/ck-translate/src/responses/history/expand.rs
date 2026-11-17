//! Continuation expansion (D2-PLAN §2.3): one expanded canonical request, published as the
//! request records of a fresh stream, from the stored turn and the client's new request.
//!
//! The expanded request keeps the new request's options, tools and instructions. Its messages
//! are, in order: the new instructions, the stored request's messages without its instructions,
//! the stored response's output projected by [`super::project`], and the new input. The
//! continuation itself goes: no `previous_response_id`, and `store: false` upstream. A
//! compaction item in the result truncates the history before it ([`super::truncate`]).
//!
//! The records are trusted canonical records, so `codec::chat::request::read_request` rebuilds
//! the same document, provenance tags and bindings included, for every attempt and for resume.
use crate::codec::{
    ClientCapability, Error, RequestFence, RequestView, Result,
    document::{Document, Name, Value},
    encode::{self, Encoder},
    event::{Emitter, Identity, Kind, Scope, write_field_value},
    media::Registry,
    provenance::{self, Producer},
};
use ck_stream::{Reader, Writer};

/// A stored turn: allocation handles and readers from the start of its request and response
/// streams, and the account that produced the response.
pub struct Stored<'a> {
    pub request: (&'a mut Writer, &'a mut Reader),
    pub response: (&'a mut Writer, &'a mut Reader),
    pub producer: Producer<'a>,
}

/// Publishes the expansion of `new` over `stored` into `target`, a fresh request stream, as
/// request records ending with `done`. `identity` is the exchange's request identity.
///
/// A request that carries media in either turn is refused with `Unsupported`.
// ponytail: media payloads are not copied between streams; a continuation carrying images
// or files is refused until a media-bearing client needs it (republish each payload with
// `media::publish` before its reference).
pub fn expand(target: &mut Writer, identity: Identity, new: RequestView<'_>, stored: Stored<'_>) -> Result<()> {
    let (request, records) = stored.request;
    let (previous, media) = crate::codec::chat::request::read_request(request, records)?;
    if new.media.entries().next().is_some() || media.entries().next().is_some() {
        return Err(Error::Unsupported);
    }
    let mut doc = Document::streaming(target)?;
    let root = doc.root();
    let copy = import(target, &mut doc, new.document)?;
    doc.transplant(root, copy)?;
    provenance::restore_bindings(&mut doc)?;
    let earlier = import(target, &mut doc, &previous)?;
    drop(previous);
    let x = doc.required(root, "x_ck")?;
    let earlier_x = doc.required(earlier, "x_ck")?;
    provenance::restore_bindings_from(&mut doc, earlier, earlier_x)?;

    let messages = doc.required(root, "messages")?;
    let current: Vec<usize> = doc.children(messages)?.collect();
    // Each turn numbers its replay entries in its own list; name them by node before the lists
    // merge, then number them again in the final message order.
    let mut named = Vec::new();
    replayed(&doc, &current, doc.member(x, "replay")?, &mut named)?;
    let stored_messages: Vec<usize> = doc.children(doc.required(earlier, "messages")?)?.collect();
    replayed(&doc, &stored_messages, doc.member(earlier_x, "replay")?, &mut named)?;
    for &message in &current {
        doc.detach(messages, message)?;
    }
    let original = doc.member(x, "original")?;
    let mut input = Vec::new();
    for message in current {
        if matches!(original, Some(list) if marked(&doc, list, message)?) {
            doc.append(messages, message)?;
        } else {
            input.push(message);
        }
    }
    adopt(&mut doc, earlier, earlier_x, x, messages)?;
    let before = doc.children(messages)?.count();
    let (response, reader) = stored.response;
    super::project(target, &mut doc, response, reader, stored.producer)?;
    let projected: Vec<usize> = doc.children(messages)?.skip(before).collect();
    replayed(&doc, &projected, doc.member(x, "replay")?, &mut named)?;
    for message in input {
        doc.append(messages, message)?;
    }
    renumber(&mut doc, x, messages, &named)?;

    doc.remove(x, "session")?;
    match doc.member(root, "store")? {
        Some(store) => doc.replace(store, Value::Bool(false))?,
        None => {
            doc.set(root, "store", Value::Bool(false))?;
        }
    }
    super::truncate(target, &mut doc)?;
    for list in provenance::bound_lists(&doc, x)?.into_iter().flatten() {
        for entry in doc.children(list)?.collect::<Vec<_>>() {
            doc.remove(entry, "binding")?;
            doc.remove(entry, "carrier")?;
        }
    }
    provenance::bindings(&mut doc, target)?;
    publish(target, identity, &doc, new)
}

/// Appends `(part, entry)` for every content part of `messages` that names an entry of `replay`
/// by its `replay_index`.
fn replayed(doc: &Document, messages: &[usize], replay: Option<usize>, out: &mut Vec<(usize, usize)>) -> Result<()> {
    for &message in messages {
        let Some(content) = doc.member(message, "content")? else {
            continue;
        };
        for part in doc.children(content)? {
            if !matches!(doc.node(part)?.value, Value::Object) {
                continue;
            }
            if let Some(index) = doc.member(part, "replay_index")? {
                let index = usize::try_from(doc.unsigned(index)?).map_err(|_| Error::Schema)?;
                let list = replay.ok_or(Error::Schema)?;
                out.push((part, doc.children(list)?.nth(index).ok_or(Error::Schema)?));
            }
        }
    }
    Ok(())
}

/// Orders `x_ck.replay` by the final position of the message each entry describes, keeping the
/// order within a message, and points every part in `named` at its entry's new position. The
/// request egress walks the entries once in increasing order and checks each one's binding.
fn renumber(doc: &mut Document, x: usize, messages: usize, named: &[(usize, usize)]) -> Result<()> {
    let Some(replay) = doc.member(x, "replay")? else {
        return Ok(());
    };
    let position: std::collections::HashMap<usize, usize> =
        doc.children(messages)?.enumerate().map(|(i, m)| (m, i)).collect();
    let mut entries = Vec::new();
    for entry in doc.children(replay)? {
        let mut key = usize::MAX;
        if let Some((mut node, _)) = doc.origin(entry)? {
            while let Some(parent) = doc.node(node)?.parent {
                if parent == messages {
                    key = position.get(&node).copied().unwrap_or(usize::MAX);
                    break;
                }
                node = parent;
            }
        }
        entries.push((key, entry));
    }
    entries.sort_by_key(|&(key, _)| key);
    let mut index = std::collections::HashMap::with_capacity(entries.len());
    for (i, &(_, entry)) in entries.iter().enumerate() {
        doc.detach(replay, entry)?;
        index.insert(entry, i);
    }
    for &(_, entry) in &entries {
        doc.append(replay, entry)?;
    }
    for &(part, entry) in named {
        match index.get(&entry) {
            Some(&i) => {
                let field = doc.required(part, "replay_index")?;
                doc.replace(field, Value::Unsigned(i as u64))?;
            }
            // A part of the stored instructions left with its message.
            None if !provenance::attached(doc, part)? => {}
            None => return Err(Error::Schema),
        }
    }
    Ok(())
}

/// Copies the canonical request `from` into `doc` as a detached subtree of `owner`.
fn import(owner: &mut Writer, doc: &mut Document, from: &Document) -> Result<usize> {
    let lease = encode::scratch(owner, |e| from.write(from.root(), e))?;
    doc.import_json(owner, lease)
}

/// Whether the developer message `message` carries the Responses `instructions`, as the
/// `x_ck.original` list `original` records it.
fn marked(doc: &Document, original: usize, message: usize) -> Result<bool> {
    for entry in doc.origins(message)? {
        if doc.node(entry)?.parent != Some(original) {
            continue;
        }
        let Some(pointer) = doc.member(entry, "pointer")? else {
            continue;
        };
        let Value::String(text) = doc.node(pointer)?.value else {
            return Err(Error::Schema);
        };
        if doc.name_is(doc.pointer_name(text)?, "instructions")? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Moves the stored request's messages, without its instructions, to the end of `messages`,
/// with every provenance entry bound inside them into the same list of `x`. Entries bound
/// elsewhere in the stored request (its tools, its options) stay behind with it.
fn adopt(doc: &mut Document, earlier: usize, earlier_x: usize, x: usize, messages: usize) -> Result<()> {
    let from = doc.required(earlier, "messages")?;
    let original = doc.member(earlier_x, "original")?;
    let mut kept = Vec::new();
    for message in doc.children(from)?.collect::<Vec<_>>() {
        if !matches!(original, Some(list) if marked(doc, list, message)?) {
            kept.push(message);
        }
    }
    let lists = provenance::bound_lists(doc, earlier_x)?;
    for (slot, list) in lists.into_iter().enumerate() {
        let Some(list) = list else {
            continue;
        };
        for entry in doc.children(list)?.collect::<Vec<_>>() {
            let Some((node, _)) = doc.origin(entry)? else {
                continue;
            };
            if !within(doc, node, from, &kept)? {
                continue;
            }
            let target = target_list(doc, x, slot)?;
            doc.detach(list, entry)?;
            doc.append(target, entry)?;
        }
    }
    for message in kept {
        doc.detach(from, message)?;
        doc.append(messages, message)?;
    }
    Ok(())
}

/// Whether `node` is one of `kept`, children of `messages`, or lies inside one.
fn within(doc: &Document, mut node: usize, messages: usize, kept: &[usize]) -> Result<bool> {
    loop {
        let Some(parent) = doc.node(node)?.parent else {
            return Ok(false);
        };
        if parent == messages {
            return Ok(kept.contains(&node));
        }
        node = parent;
    }
}

/// The list of `x` matching slot `slot` of [`provenance::bound_lists`], created on first use.
fn target_list(doc: &mut Document, x: usize, slot: usize) -> Result<usize> {
    match provenance::BOUND_LISTS.get(slot) {
        Some(&key) => match doc.member(x, key)? {
            Some(list) => Ok(list),
            None => doc.set(x, key, Value::Array),
        },
        None => {
            let (provider, key) = provenance::ISOLATED_LISTS[slot - provenance::BOUND_LISTS.len()];
            provenance::isolated_list(doc, provider, key)
        }
    }
}

/// One path component of a published field.
#[derive(Clone, Copy)]
enum Component {
    Name(Name),
    Index(u64),
}

/// Publishes `doc` as request records: start, one field per node in document order, end and
/// `done`. The client fence closes the view, as at ingress (contract G8).
fn publish(target: &mut Writer, identity: Identity, doc: &Document, new: RequestView<'_>) -> Result<()> {
    let media = Registry::new(target)?;
    let view = RequestView {
        document: doc,
        media: &media,
        operation: new.operation,
    };
    let mut emitter = Emitter::new(identity);
    emitter.request_start(target, view)?;
    let mut path = Vec::new();
    fields(target, &mut emitter, doc, doc.root(), &mut path)?;
    let ready = RequestFence::default().end(view, &ClientCapability)?;
    emitter.request_end(target, &ready)?;
    emitter.emit(target, Kind::Done, Scope::default(), |_| Ok(()), |_| Ok(()))?;
    Ok(())
}

fn fields(
    target: &mut Writer,
    emitter: &mut Emitter,
    doc: &Document,
    node: usize,
    path: &mut Vec<Component>,
) -> Result<()> {
    if path.len() == 128 {
        return Err(Error::Limit);
    }
    emitter.emit_field(
        target,
        |e| pointer(doc, path, e),
        |e| write_field_value(doc, node, None, e),
    )?;
    let array = matches!(doc.node(node)?.value, Value::Array);
    for (index, child) in doc.children(node)?.enumerate() {
        path.push(if array {
            Component::Index(index as u64)
        } else {
            Component::Name(doc.node(child)?.name.ok_or(Error::Schema)?)
        });
        fields(target, emitter, doc, child, path)?;
        path.pop();
    }
    Ok(())
}

fn pointer(doc: &Document, path: &[Component], e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"\"")?;
    for component in path {
        e.bytes(b"/")?;
        match *component {
            Component::Index(index) => e.unsigned(index)?,
            Component::Name(name) => provenance::write_pointer_segment(doc, name, e)?,
        }
    }
    e.bytes(b"\"")
}
