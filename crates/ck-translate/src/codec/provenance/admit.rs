//! Per-attempt admission of tagged opaque items (D2-PLAN §2.4; owner ruling Q-C, 2026-11-16).
//!
//! History projection tags every opaque provider member of a stored response, such as a thinking
//! signature, encrypted reasoning or a compaction item, with the upstream account that produced
//! it. Only that account can read such a member. Before every attempt the server asks [`admit`]
//! what the candidate may receive:
//! - a compaction item from another producer refuses the candidate, which is skipped;
//! - reasoning or a signature from another producer is removed by [`drop_foreign`], and the
//!   conversation goes on without it;
//! - an untagged member, such as one a client replayed in full input, is left to the
//!   destination's fence, which admits only its own format.
//!
//! A tag is trusted because only the server writes one: client ingress builds `x_ck` itself and
//! never copies a client's `producer`. The owner cut the signed envelope (2026-11-15), so a
//! member a client replays from an earlier response carries no tag.
use super::bound_lists;
use crate::codec::{
    Error, Result,
    document::{Document, Value},
    table::Table,
};
use ck_stream::Writer;

/// The upstream account that produced a stored response: the credential's provider and auth
/// index. A replay entry carries it as `producer: {provider, credential}`, plus
/// `compaction: true` for a compaction item.
#[derive(Clone, Copy, Debug)]
pub struct Producer<'a> {
    pub provider: &'a str,
    pub credential: &'a str,
}

/// What one candidate may receive of a request's tagged opaque items.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Admission {
    /// Every tagged item is the candidate's own, or there is none.
    Keep,
    /// Reasoning or signatures from another producer: send the document [`drop_foreign`] leaves.
    Drop,
    /// A compaction item from another producer: the candidate cannot read it.
    Refuse,
}

/// Decides what the candidate whose account is `producer` may receive of `doc`.
pub fn admit(doc: &Document, producer: Producer<'_>) -> Result<Admission> {
    let Some(replay) = replay(doc)? else {
        return Ok(Admission::Keep);
    };
    let mut admission = Admission::Keep;
    for entry in doc.children(replay)? {
        let Some(tag) = doc.member(entry, "producer")? else {
            continue;
        };
        if own(doc, tag, producer)? {
            continue;
        }
        if doc.member(tag, "compaction")?.is_some() {
            return Ok(Admission::Refuse);
        }
        admission = Admission::Drop;
    }
    Ok(admission)
}

/// Removes every member another producer tagged and what it signs: a reasoning or redacted part,
/// or a message's reasoning parts. A signature riding on readable content, such as a Gemini text
/// or call part, leaves that content. A message left with nothing goes too.
pub fn drop_foreign(owner: &mut Writer, doc: &mut Document, producer: Producer<'_>) -> Result<()> {
    let Some(replay) = replay(doc)? else {
        return Ok(());
    };
    let mut foreign = Table::new(owner)?;
    for (index, entry) in doc.children(replay)?.enumerate() {
        if let Some(tag) = doc.member(entry, "producer")?
            && !own(doc, tag, producer)?
        {
            if doc.member(tag, "compaction")?.is_some() {
                return Err(Error::Unsupported);
            }
            foreign.push((index, entry))?;
        }
    }
    for &(_, entry) in foreign.iter() {
        if let Some((node, carrier)) = doc.origin(entry)? {
            withdraw(doc, node, carrier.is_some())?;
        }
    }
    prune(owner, doc, |index| foreign.iter().any(|&(at, _)| at == index))
}

/// Removes the content a foreign entry bound. `carried`: the entry is a signature member of
/// `node`, which stays unless it is reasoning.
fn withdraw(doc: &mut Document, node: usize, carried: bool) -> Result<()> {
    if !attached(doc, node)? {
        return Ok(());
    }
    let parent = doc.node(node)?.parent.ok_or(Error::Schema)?;
    if doc.member(node, "role")?.is_none() {
        if !carried || is(doc, node, "reasoning")? {
            doc.detach(parent, node)?;
            vacate(doc, doc.node(parent)?.parent)?;
        }
        return Ok(());
    }
    // A message-bound entry is a Responses reasoning or compaction item: all its reasoning goes.
    if let Some(content) = doc.member(node, "content")?
        && matches!(doc.node(content)?.value, Value::Array)
    {
        let mut next = doc.node(content)?.first;
        while let Some(part) = next {
            next = doc.node(part)?.next;
            if is(doc, part, "reasoning")? || is(doc, part, "reasoning_summary")? || is(doc, part, "encrypted")? {
                doc.detach(content, part)?;
            }
        }
    }
    vacate(doc, Some(node))
}

/// Removes `message` once it holds neither content parts nor calls.
fn vacate(doc: &mut Document, message: Option<usize>) -> Result<()> {
    let Some(message) = message.filter(|&m| doc.node(m).is_ok_and(|n| n.parent.is_some())) else {
        return Ok(());
    };
    for key in ["content", "tool_calls"] {
        if let Some(list) = doc.member(message, key)?
            && (!matches!(doc.node(list)?.value, Value::Array) || doc.node(list)?.first.is_some())
        {
            return Ok(());
        }
    }
    let messages = doc.node(message)?.parent.ok_or(Error::Schema)?;
    doc.detach(messages, message)
}

/// Removes bound-list entries whose node left the document, and the replay entries `removed`
/// names by position, then renumbers the `replay_index` of every surviving part.
pub(crate) fn prune(owner: &mut Writer, doc: &mut Document, removed: impl Fn(usize) -> bool) -> Result<()> {
    let x = doc.required(doc.root(), "x_ck")?;
    let replay = doc.member(x, "replay")?;
    let mut renumbered = Table::new(owner)?;
    let mut changed = false;
    for list in bound_lists(doc, x)?.into_iter().flatten() {
        let mut next = doc.node(list)?.first;
        let (mut index, mut kept) = (0, 0);
        while let Some(entry) = next {
            next = doc.node(entry)?.next;
            let orphan = match doc.origin(entry)? {
                Some((node, _)) => !attached(doc, node)?,
                None => false,
            };
            let gone = orphan || (Some(list) == replay && removed(index));
            if Some(list) == replay {
                renumbered.push((!gone).then_some(kept))?;
                changed |= gone;
            }
            if gone {
                doc.detach(list, entry)?;
            } else {
                kept += 1;
            }
            index += 1;
        }
    }
    if changed {
        renumber(doc, &renumbered)?;
    }
    Ok(())
}

/// Rewrites each part's `replay_index` through `renumbered` (old position to new).
fn renumber(doc: &mut Document, renumbered: &[Option<usize>]) -> Result<()> {
    let messages = doc.required(doc.root(), "messages")?;
    let mut message = doc.node(messages)?.first;
    while let Some(current) = message {
        message = doc.node(current)?.next;
        let Some(content) = doc.member(current, "content")? else {
            continue;
        };
        let mut next = doc.node(content)?.first;
        while let Some(part) = next {
            next = doc.node(part)?.next;
            if !matches!(doc.node(part)?.value, Value::Object) {
                continue;
            }
            if let Some(index) = doc.member(part, "replay_index")? {
                let old = usize::try_from(doc.unsigned(index)?).map_err(|_| Error::Schema)?;
                let new = renumbered.get(old).copied().flatten().ok_or(Error::Schema)?;
                doc.replace(index, Value::Unsigned(new as u64))?;
            }
        }
    }
    Ok(())
}

/// Whether `node` is still reachable from the root.
pub(crate) fn attached(doc: &Document, mut node: usize) -> Result<bool> {
    while node != doc.root() {
        let Some(parent) = doc.node(node)?.parent else {
            return Ok(false);
        };
        if !doc.children(parent)?.any(|child| child == node) {
            return Ok(false);
        }
        node = parent;
    }
    Ok(true)
}

fn replay(doc: &Document) -> Result<Option<usize>> {
    match doc.member(doc.root(), "x_ck")? {
        Some(x) => doc.member(x, "replay"),
        None => Ok(None),
    }
}

fn own(doc: &Document, tag: usize, producer: Producer<'_>) -> Result<bool> {
    Ok(doc.string_is(doc.required(tag, "provider")?, producer.provider)?
        && doc.string_is(doc.required(tag, "credential")?, producer.credential)?)
}

fn is(doc: &Document, part: usize, kind: &str) -> Result<bool> {
    match doc.member(part, "type")? {
        Some(node) => doc.string_is(node, kind),
        None => Ok(false),
    }
}
