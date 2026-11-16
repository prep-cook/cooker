use crate::codec::table::Table;
use crate::codec::{
    Error, Result,
    document::{Document, Name, Value},
    encode::Encoder,
};
use ck_stream::Writer;

#[derive(Default)]
pub(super) struct Usage {
    native: Option<Document>,
}
impl Usage {
    pub fn update(&mut self, owner: &mut Writer, doc: &Document, node: usize) -> Result<()> {
        if !matches!(doc.node(node)?.value, Value::Object | Value::Null) {
            return Err(Error::Schema);
        }
        let supplied = index(owner, doc, Some(node))?;
        let nested = index(owner, doc, object_member(doc, node, "cache_creation")?)?;
        let next = super::mapped(owner, |e| {
            merge(
                self.native.as_ref().map(|d| (d, d.root())),
                (doc, node),
                (&supplied, Some(&nested)),
                e,
            )
        })?;
        // Validate every supplied count before publishing authoritative accounting.
        for key in BILLED {
            count(&next, next.root(), key)?;
        }
        reported_total(&next, next.root())?;
        if let Some(ttl) = object_member(&next, next.root(), "cache_creation")? {
            for key in ["ephemeral_5m_input_tokens", "ephemeral_1h_input_tokens"] {
                count(&next, ttl, key)?;
            }
        }
        self.native = Some(next);
        Ok(())
    }
    pub fn seen(&self) -> bool {
        self.native.is_some()
    }
    pub fn write(&self, e: &mut Encoder<'_, '_>, final_snapshot: bool) -> Result<()> {
        let doc = self.native.as_ref().ok_or(Error::Schema)?;
        let root = doc.root();
        e.bytes(b"{\"native\":{\"protocol\":\"anthropic\",\"value\":")?;
        doc.write(root, e)?;
        e.bytes(b"},\"final\":")?;
        e.bytes(if final_snapshot { b"true" } else { b"false" })?;
        let uncached = billed(doc, root, "input_tokens")?;
        let read = billed(doc, root, "cache_read_input_tokens")?;
        let write = billed(doc, root, "cache_creation_input_tokens")?;
        let output = billed(doc, root, "output_tokens")?;
        let reasoning = reasoning(doc, root)?;
        // An omitted cache component counts as 0 (owner ruling, 2026-11-15).
        let input = match uncached {
            Some(a) => Some(
                a.checked_add(read.unwrap_or(0))
                    .and_then(|v| v.checked_add(write.unwrap_or(0)))
                    .ok_or(Error::Schema)?,
            ),
            None => None,
        };
        let reported = reported_total(doc, root)?;
        let total = match (reported, input, output) {
            (Some(total), _, _) => Some(total),
            (None, Some(a), Some(b)) => Some(a.checked_add(b).ok_or(Error::Schema)?),
            _ => None,
        };
        for (name, value) in [
            ("input_uncached_tokens", uncached),
            ("cache_read_tokens", read),
            ("cache_write_tokens", write),
            ("input_tokens", input),
            ("output_tokens", output),
            ("total_tokens", total),
            ("reasoning_tokens", reasoning),
        ] {
            if let Some(value) = value {
                e.bytes(b",")?;
                e.string(name)?;
                e.bytes(b":")?;
                e.unsigned(value)?;
            }
        }
        let mut first = true;
        for (native, seconds) in [
            ("ephemeral_5m_input_tokens", "300"),
            ("ephemeral_1h_input_tokens", "3600"),
        ] {
            if let Some(value) = billed_ttl(doc, root, native)? {
                e.bytes(if first { b",\"cache_write_by_ttl\":{" } else { b"," })?;
                first = false;
                e.string(seconds)?;
                e.bytes(b":")?;
                e.unsigned(value)?;
            }
        }
        if !first {
            e.bytes(b"}")?;
        }
        if input.is_some() {
            e.bytes(b",\"derived\":[\"input_tokens\"")?;
            if total.is_some() && reported.is_none() {
                e.bytes(b",\"total_tokens\"")?;
            }
            e.bytes(b"]")?;
        }
        e.bytes(b"}")
    }
}
/// A total the source reported beside the Anthropic counts, which have no member for it. Only
/// the gateway's Antigravity bridge writes `x_ck.total_tokens`; Anthropic egress drops `x_ck`.
fn reported_total(doc: &Document, root: usize) -> Result<Option<u64>> {
    match object_member(doc, root, "x_ck")? {
        Some(x) => count(doc, x, "total_tokens"),
        None => Ok(None),
    }
}
/// The token counts a response bills.
const BILLED: [&str; 4] = [
    "input_tokens",
    "output_tokens",
    "cache_read_input_tokens",
    "cache_creation_input_tokens",
];
/// A billed count. A response that lists `iterations` bills their sum: an on-demand compaction
/// reports its tokens only there and leaves the top level at 0, and with server-side compaction
/// the top level leaves the compaction iterations out. Otherwise the top-level count.
fn billed(doc: &Document, root: usize, key: &str) -> Result<Option<u64>> {
    summed(doc, root, |node| count(doc, node, key))
}
/// A cache-write TTL bucket of `cache_creation`, from the usage objects [`billed`] reads.
fn billed_ttl(doc: &Document, root: usize, key: &str) -> Result<Option<u64>> {
    summed(doc, root, |node| match object_member(doc, node, "cache_creation")? {
        Some(ttl) => count(doc, ttl, key),
        None => Ok(None),
    })
}
/// `read` of the top level, or the sum of what it reads in each of the usage's `iterations`.
fn summed(doc: &Document, root: usize, read: impl Fn(usize) -> Result<Option<u64>>) -> Result<Option<u64>> {
    let Some(iterations) = iterations(doc, root)? else {
        return read(root);
    };
    let mut total: Option<u64> = None;
    for entry in doc.children(iterations)? {
        if let Some(value) = read(entry)? {
            total = Some(total.unwrap_or(0).checked_add(value).ok_or(Error::Schema)?);
        }
    }
    Ok(total)
}
/// The non-empty `iterations` list of a usage object.
fn iterations(doc: &Document, root: usize) -> Result<Option<usize>> {
    if matches!(doc.node(root)?.value, Value::Null) {
        return Ok(None);
    }
    match doc.member(root, "iterations")? {
        Some(n) if matches!(doc.node(n)?.value, Value::Array) => Ok(doc.node(n)?.first.map(|_| n)),
        Some(n) if matches!(doc.node(n)?.value, Value::Null) => Ok(None),
        Some(_) => Err(Error::Schema),
        None => Ok(None),
    }
}
fn object_member(doc: &Document, node: usize, key: &str) -> Result<Option<usize>> {
    if matches!(doc.node(node)?.value, Value::Null) {
        return Ok(None);
    }
    match doc.member(node, key)? {
        Some(n) if matches!(doc.node(n)?.value, Value::Null) => Ok(None),
        Some(n) if matches!(doc.node(n)?.value, Value::Object) => Ok(Some(n)),
        Some(_) => Err(Error::Schema),
        None => Ok(None),
    }
}
/// Thinking tokens, a subset of the output tokens.
fn reasoning(doc: &Document, root: usize) -> Result<Option<u64>> {
    match object_member(doc, root, "output_tokens_details")? {
        Some(details) => count(doc, details, "thinking_tokens"),
        None => Ok(None),
    }
}
fn count(doc: &Document, node: usize, key: &str) -> Result<Option<u64>> {
    if matches!(doc.node(node)?.value, Value::Null) {
        return Ok(None);
    }
    match doc.member(node, key)? {
        Some(n) if !matches!(doc.node(n)?.value, Value::Null) => Ok(Some(doc.unsigned(n)?)),
        _ => Ok(None),
    }
}
/// Supplied members as (name hash, node), sorted by hash.
type Index = [(u64, usize)];
/// Supplied member names by hash, so merging looks each retained member up once.
fn index(owner: &mut Writer, doc: &Document, node: Option<usize>) -> Result<Table<(u64, usize)>> {
    let mut table = Table::new(owner)?;
    if let Some(node) = node.filter(|&n| matches!(doc.node(n).map(|n| n.value), Ok(Value::Object))) {
        for field in doc.children(node)? {
            table.push((name_hash(doc, doc.node(field)?.name.ok_or(Error::Schema)?)?, field))?;
        }
    }
    table.sort_unstable();
    Ok(table)
}
fn name_hash(doc: &Document, name: Name) -> Result<u64> {
    use std::hash::Hasher;
    let mut hasher = std::hash::DefaultHasher::new();
    let mut chars = doc.name_chars(name)?;
    while let Some(c) = chars.next_scalar()? {
        hasher.write_u32(c.into());
    }
    Ok(hasher.finish())
}
/// Whether the update reports `name` with a value other than `null`.
fn supplied(previous: &Document, name: Name, (doc, index): (&Document, &Index)) -> Result<bool> {
    let hash = name_hash(previous, name)?;
    let start = index.partition_point(|&(h, _)| h < hash);
    for &(_, next) in index[start..].iter().take_while(|&&(h, _)| h == hash) {
        let mut a = previous.name_chars(name)?;
        let mut b = doc.name_chars(doc.node(next)?.name.ok_or(Error::Schema)?)?;
        loop {
            let c = a.next_scalar()?;
            if c != b.next_scalar()? {
                break;
            }
            if c.is_none() {
                return Ok(!matches!(doc.node(next)?.value, Value::Null));
            }
        }
    }
    Ok(false)
}
/// Whether `object` in `previous` has a member named like `name` in `doc`.
fn has_member(previous: &Document, object: usize, doc: &Document, name: Name) -> Result<bool> {
    for field in previous.children(object)? {
        let mut a = previous.name_chars(previous.node(field)?.name.ok_or(Error::Schema)?)?;
        let mut b = doc.name_chars(name)?;
        loop {
            let c = a.next_scalar()?;
            if c != b.next_scalar()? {
                break;
            }
            if c.is_none() {
                return Ok(true);
            }
        }
    }
    Ok(false)
}
/// Retains old members the update omits or reports as `null`, as the Anthropic SDK's message
/// accumulator does; `cache_creation` merges one level deep.
fn merge(
    old: Option<(&Document, usize)>,
    (doc, node): (&Document, usize),
    (index, nested): (&Index, Option<&Index>),
    e: &mut Encoder<'_, '_>,
) -> Result<()> {
    if !matches!(doc.node(node)?.value, Value::Object) {
        return doc.write(node, e);
    }
    e.bytes(b"{")?;
    let old = old.filter(|(d, n)| matches!(d.node(*n).map(|v| v.value), Ok(Value::Object)));
    let mut first = true;
    if let Some((previous, root)) = old {
        for field in previous.children(root)? {
            let name = previous.node(field)?.name.ok_or(Error::Schema)?;
            if !supplied(previous, name, (doc, index))? {
                if !first {
                    e.bytes(b",")?;
                }
                first = false;
                previous.write_name(name, e)?;
                e.bytes(b":")?;
                previous.write(field, e)?;
            }
        }
    }
    for field in doc.children(node)? {
        let name = doc.node(field)?.name.ok_or(Error::Schema)?;
        if matches!(doc.node(field)?.value, Value::Null)
            && let Some((previous, root)) = old
            && has_member(previous, root, doc, name)?
        {
            continue;
        }
        if !first {
            e.bytes(b",")?;
        }
        first = false;
        doc.write_name(name, e)?;
        e.bytes(b":")?;
        if let Some(nested) = nested
            && doc.name_is(name, "cache_creation")?
        {
            let prior = match old {
                Some((d, n)) => d.member(n, "cache_creation")?.map(|n| (d, n)),
                None => None,
            };
            merge(prior, (doc, field), (nested, None), e)?;
        } else {
            doc.write(field, e)?;
        }
    }
    e.bytes(b"}")
}
