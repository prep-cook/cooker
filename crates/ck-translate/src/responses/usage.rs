use crate::codec::{
    Result,
    document::{Document, Value},
    encode::Encoder,
};
use ck_stream::Writer;

#[derive(Default)]
pub(super) struct Usage {
    native: Option<Document>,
}
impl Usage {
    pub fn update(&mut self, owner: &mut Writer, doc: &Document, node: usize) -> Result<()> {
        let incoming = super::mapped(owner, |e| doc.write(node, e))?;
        if let Some(current) = &mut self.native {
            let index = current.absorb(incoming)?;
            if matches!(current.node(index)?.value, Value::Object)
                && matches!(current.node(current.root())?.value, Value::Object)
            {
                super::merge(current, current.root(), index)?;
            } else {
                current.transplant(current.root(), index)?;
            }
        } else {
            self.native = Some(incoming);
        }
        Ok(())
    }
    pub fn seen(&self) -> bool {
        self.native.is_some()
    }
    pub fn write(&self, e: &mut Encoder<'_, '_>, final_snapshot: bool) -> Result<()> {
        let doc = self.native.as_ref().ok_or(crate::codec::Error::Schema)?;
        let root = doc.root();
        e.bytes(b"{\"native\":{\"protocol\":\"responses\",\"value\":")?;
        doc.write(root, e)?;
        e.bytes(b"},\"final\":")?;
        e.bytes(if final_snapshot { b"true" } else { b"false" })?;
        if matches!(doc.node(root)?.value, Value::Object) {
            for name in [
                "input_tokens",
                "output_tokens",
                "total_tokens",
                "cache_write_tokens",
                "input_uncached_tokens",
            ] {
                if let Some(n) = doc.member(root, name)?
                    && !matches!(doc.node(n)?.value, Value::Null)
                {
                    doc.unsigned(n)?;
                    e.bytes(b",")?;
                    e.string(name)?;
                    e.bytes(b":")?;
                    doc.write(n, e)?;
                }
            }
            if doc.member(root, "total_tokens")?.is_none()
                && let (Some(a), Some(b)) = (doc.member(root, "input_tokens")?, doc.member(root, "output_tokens")?)
                && !matches!(doc.node(a)?.value, Value::Null)
                && !matches!(doc.node(b)?.value, Value::Null)
            {
                let total = doc
                    .unsigned(a)?
                    .checked_add(doc.unsigned(b)?)
                    .ok_or(crate::codec::Error::Schema)?;
                e.bytes(b",\"total_tokens\":")?;
                e.unsigned(total)?;
                e.bytes(b",\"derived\":[\"total_tokens\"]")?;
            }
            write_details(doc, root, e)?;
            if !write_ttl(doc, root, e)?
                && let Some(ttl) = doc.member(root, "cache_write_by_ttl")?
            {
                e.bytes(b",\"cache_write_by_ttl\":")?;
                doc.write(ttl, e)?;
            }
        }
        e.bytes(b"}")
    }
}

/// Writes the counts nested in the native token details under their canonical names; a
/// native top-level `cache_write_tokens` wins over the nested one.
fn write_details(doc: &Document, root: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    for (details, fields) in [
        (
            "input_tokens_details",
            &[
                ("cached_tokens", "cache_read_tokens"),
                ("audio_tokens", "input_audio_tokens"),
                ("cache_write_tokens", "cache_write_tokens"),
            ][..],
        ),
        (
            "output_tokens_details",
            &[
                ("reasoning_tokens", "reasoning_tokens"),
                ("audio_tokens", "output_audio_tokens"),
            ][..],
        ),
    ] {
        if let Some(details) = doc.member(root, details)?
            && matches!(doc.node(details)?.value, Value::Object)
        {
            for (from, to) in fields {
                if let Some(n) = doc.member(details, from)?
                    && !matches!(doc.node(n)?.value, Value::Null)
                    && (to != &"cache_write_tokens"
                        || doc
                            .member(root, to)?
                            .is_none_or(|n| matches!(doc.node(n).map(|n| n.value), Ok(Value::Null))))
                {
                    doc.unsigned(n)?;
                    e.bytes(b",")?;
                    e.string(to)?;
                    e.bytes(b":")?;
                    doc.write(n, e)?;
                }
            }
        }
    }
    Ok(())
}

/// Writes OpenAI's cache-write TTL counts (`input_tokens_details.cache_write_tokens_details`) as
/// canonical buckets keyed by their duration in seconds; a null or omitted count, or one that is
/// not an unsigned integer, stays absent. Returns whether the source reported the details object.
fn write_ttl(doc: &Document, root: usize, e: &mut Encoder<'_, '_>) -> Result<bool> {
    let Some(details) = doc
        .member(root, "input_tokens_details")?
        .filter(|&d| matches!(doc.node(d).map(|n| n.value), Ok(Value::Object)))
        .map(|d| doc.member(d, "cache_write_tokens_details"))
        .transpose()?
        .flatten()
        .filter(|&d| matches!(doc.node(d).map(|n| n.value), Ok(Value::Object)))
    else {
        return Ok(false);
    };
    let mut first = true;
    for (native, seconds) in TTL {
        if let Some(count) = ttl_count(doc, details, native)? {
            e.bytes(if first { b",\"cache_write_by_ttl\":{" } else { b"," })?;
            first = false;
            e.string(seconds)?;
            e.bytes(b":")?;
            e.unsigned(count)?;
        }
    }
    if !first {
        e.bytes(b"}")?;
    }
    Ok(true)
}

/// OpenAI's cache-write TTL members and the canonical bucket each one maps to.
const TTL: [(&str, &str); 2] = [("ttl_30m_tokens", "1800"), ("ttl_12h_tokens", "43200")];

/// The TTL count `name` of `details` when it is an unsigned integer.
fn ttl_count(doc: &Document, details: usize, name: &str) -> Result<Option<u64>> {
    match doc.member(details, name)? {
        Some(n) => match doc.unsigned(n) {
            Ok(count) => Ok(Some(count)),
            Err(crate::codec::Error::Schema) => Ok(None),
            Err(error) => Err(error),
        },
        None => Ok(None),
    }
}

/// Writes the canonical TTL buckets over the native value's official
/// `input_tokens_details.cache_write_tokens_details`, so a normalized change or deletion wins as
/// for the other counts; a native count the snapshot never mapped (null or malformed) is left
/// alone. Buckets of other durations have no Responses member.
fn overlay_ttl(doc: &mut Document, usage: usize, value: usize) -> Result<()> {
    let object = |doc: &Document, n: usize| matches!(doc.node(n).map(|n| n.value), Ok(Value::Object));
    let buckets = doc.member(usage, "cache_write_by_ttl")?.filter(|&n| object(doc, n));
    for (field, seconds) in TTL {
        let canonical = match buckets {
            Some(buckets) => doc
                .member(buckets, seconds)?
                .filter(|&n| !matches!(doc.node(n).map(|n| n.value), Ok(Value::Null))),
            None => None,
        };
        let details = doc.member(value, "input_tokens_details")?.filter(|&n| object(doc, n));
        let parent = match details {
            Some(d) => doc.member(d, "cache_write_tokens_details")?.filter(|&n| object(doc, n)),
            None => None,
        };
        match (canonical, parent) {
            (Some(count), parent) => {
                let parent = match (parent, details) {
                    (Some(p), _) => p,
                    (None, Some(d)) => doc.set(d, "cache_write_tokens_details", Value::Object)?,
                    (None, None) => {
                        let d = doc.set(value, "input_tokens_details", Value::Object)?;
                        doc.set(d, "cache_write_tokens_details", Value::Object)?
                    }
                };
                let target = doc.set(parent, field, Value::Null)?;
                doc.transplant(target, count)?;
            }
            (None, Some(parent)) => {
                if ttl_count(doc, parent, field)?.is_some() {
                    doc.remove(parent, field)?;
                }
            }
            (None, None) => {}
        }
    }
    Ok(())
}

/// Responses usage for canonical snapshot `usage`: a Responses source's native value, or the
/// mapped counts.
pub(super) fn project(doc: &mut Document, usage: usize) -> Result<usize> {
    if let Some(native) = doc.member(usage, "native")?
        && super::is(doc, native, "protocol", "responses")?
    {
        let value = doc.required(native, "value")?;
        overlay(doc, usage, value)?;
        if matches!(doc.node(value)?.value, Value::Object) {
            cache_details(doc, value)?;
        }
        return Ok(value);
    }
    let result = doc.add(None, Value::Object)?;
    for name in ["input_tokens", "output_tokens", "total_tokens", "input_uncached_tokens"] {
        super::move_field(doc, usage, name, result, name)?;
    }
    for (name, fields) in [
        (
            "input_tokens_details",
            &[
                ("cache_read_tokens", "cached_tokens"),
                ("input_audio_tokens", "audio_tokens"),
                ("cache_write_tokens", "cache_write_tokens"),
            ][..],
        ),
        (
            "output_tokens_details",
            &[
                ("reasoning_tokens", "reasoning_tokens"),
                ("output_audio_tokens", "audio_tokens"),
            ][..],
        ),
    ] {
        let mut details = None;
        for &(from, to) in fields {
            if doc.member(usage, from)?.is_some() {
                let target = match details {
                    Some(target) => target,
                    None => *details.insert(doc.set(result, name, Value::Object)?),
                };
                super::move_field(doc, usage, from, target, to)?;
            }
        }
    }
    cache_details(doc, result)?;
    Ok(result)
}

/// Puts both cache counts of Responses `usage` where the API does, in `input_tokens_details`. A
/// top-level `cache_write_tokens` moves there and wins over a nested one, as in `write_details`;
/// the schema requires both counts, so an unreported one is 0.
fn cache_details(doc: &mut Document, usage: usize) -> Result<()> {
    let details = match doc
        .member(usage, "input_tokens_details")?
        .filter(|&d| matches!(doc.node(d).map(|n| n.value), Ok(Value::Object)))
    {
        Some(details) => details,
        None => doc.set(usage, "input_tokens_details", Value::Object)?,
    };
    if let Some(write) = doc.remove(usage, "cache_write_tokens")?
        && !matches!(doc.node(write)?.value, Value::Null)
    {
        doc.remove(details, "cache_write_tokens")?;
        doc.append(details, write)?;
    }
    for name in ["cached_tokens", "cache_write_tokens"] {
        if doc
            .member(details, name)?
            .is_none_or(|n| matches!(doc.node(n).map(|n| n.value), Ok(Value::Null)))
        {
            doc.set(details, name, Value::Unsigned(0))?;
        }
    }
    Ok(())
}

/// Canonical counts and the native Responses member each one maps to: `(canonical, details
/// object, native name)`. TTL buckets have no top-level Responses member; [`overlay_ttl`] writes
/// them into `cache_write_tokens_details`.
const NATIVE_COUNTS: &[(&str, Option<&str>, &str)] = &[
    ("input_tokens", None, "input_tokens"),
    ("output_tokens", None, "output_tokens"),
    ("total_tokens", None, "total_tokens"),
    ("input_uncached_tokens", None, "input_uncached_tokens"),
    ("cache_read_tokens", Some("input_tokens_details"), "cached_tokens"),
    ("input_audio_tokens", Some("input_tokens_details"), "audio_tokens"),
    ("reasoning_tokens", Some("output_tokens_details"), "reasoning_tokens"),
    ("output_audio_tokens", Some("output_tokens_details"), "audio_tokens"),
];

/// Writes the canonical counts of `usage` over the preserved native `value`, so a normalized
/// change or deletion wins (§3.7). Native nulls the snapshot never mapped and derived counts the
/// source did not report are left alone.
fn overlay(doc: &mut Document, usage: usize, value: usize) -> Result<()> {
    if !matches!(doc.node(value)?.value, Value::Object) {
        return Ok(());
    }
    let nested_write = doc.member(value, "cache_write_tokens")?.is_none()
        && doc
            .member(value, "input_tokens_details")?
            .map(|d| doc.member(d, "cache_write_tokens"))
            .transpose()?
            .flatten()
            .is_some();
    let write = (
        "cache_write_tokens",
        nested_write.then_some("input_tokens_details"),
        "cache_write_tokens",
    );
    for &(name, details, field) in NATIVE_COUNTS.iter().chain([&write]) {
        let mut derived = false;
        if let Some(list) = doc.member(usage, "derived")? {
            for entry in doc.children(list)? {
                derived |= doc.string_is(entry, name)?;
            }
        }
        let parent = match details {
            Some(d) => doc
                .member(value, d)?
                .filter(|&p| matches!(doc.node(p).map(|n| n.value), Ok(Value::Object))),
            None => Some(value),
        };
        let canonical = doc
            .member(usage, name)?
            .filter(|&c| !matches!(doc.node(c).map(|n| n.value), Ok(Value::Null)));
        match canonical {
            _ if derived => {}
            Some(count) => {
                let parent = match (parent, details) {
                    (Some(p), _) => p,
                    (None, Some(d)) => doc.set(value, d, Value::Object)?,
                    (None, None) => return Err(crate::codec::Error::Schema),
                };
                let target = doc.set(parent, field, Value::Null)?;
                doc.transplant(target, count)?;
            }
            None => {
                if let Some(p) = parent
                    && let Some(n) = doc.member(p, field)?
                    && !matches!(doc.node(n)?.value, Value::Null)
                {
                    doc.remove(p, field)?;
                }
            }
        }
    }
    overlay_ttl(doc, usage, value)
}
