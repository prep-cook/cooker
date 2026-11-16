//! Protocol-neutral JSON pointers and trusted origin bindings.
//!
//! A protocol codec binds `x_ck` provenance entries to the normalized nodes they
//! describe. [`bindings`] serializes those links as `binding` pointers (plus an
//! optional native `carrier`) before canonical fields are published, and
//! [`restore_bindings`] re-binds them on a trusted reconstructed document.
//! Interpreting a provenance entry's own native `pointer` stays with the protocol
//! that wrote it.
use super::{
    Error, Result,
    document::{Characters, Document, Name, Value},
    encode::{self, Encoder},
    source::Span,
};
use ck_stream::Writer;

pub(crate) mod admit;
pub(crate) use admit::attached;
pub use admit::{Admission, Producer, admit, drop_foreign};

/// `x_ck` lists whose entries may carry a trusted `binding`.
pub const BOUND_LISTS: [&str; 5] = ["original", "replay", "tools", "chat_legacy_calls", "anthropic_order"];

/// Bound lists inside `x_ck.isolated`, as (provider, list). A provider's isolated map holds its
/// own members that other protocols cannot express (owner ruling Q3, 2026-11-16): only that
/// provider's request egress reads it, and no other destination writes or refuses it.
pub const ISOLATED_LISTS: [(&str, &str); 2] = [("anthropic", "cache_control"), ("xai", "parameters")];

/// The isolated list `key` of `provider`, if ingress recorded one.
pub fn isolated(doc: &Document, provider: &str, key: &str) -> Result<Option<usize>> {
    let Some(x) = doc.member(doc.root(), "x_ck")? else {
        return Ok(None);
    };
    let Some(map) = doc.member(x, "isolated")? else {
        return Ok(None);
    };
    match doc.member(map, provider)? {
        Some(own) => doc.member(own, key),
        None => Ok(None),
    }
}

/// The isolated list `key` of `provider`, created on first use so a request without such
/// members carries no map.
pub fn isolated_list(doc: &mut Document, provider: &'static str, key: &'static str) -> Result<usize> {
    let x = doc.required(doc.root(), "x_ck")?;
    let map = match doc.member(x, "isolated")? {
        Some(map) => map,
        None => doc.set(x, "isolated", Value::Object)?,
    };
    let own = match doc.member(map, provider)? {
        Some(own) => own,
        None => doc.set(map, provider, Value::Object)?,
    };
    match doc.member(own, key)? {
        Some(list) => Ok(list),
        None => doc.set(own, key, Value::Array),
    }
}

/// Capture xAI-only root controls before generic protocol provenance can forward them
/// to another provider speaking the same protocol.
pub(crate) fn capture_xai(doc: &mut Document, owner: &mut Writer, field: usize) -> Result<Option<usize>> {
    let root = doc.root();
    if doc.node(field)?.parent != Some(root) {
        return Ok(None);
    }
    let name = doc.node(field)?.name.ok_or(Error::Schema)?;
    if !doc.name_is(name, "search_parameters")? && !doc.name_is(name, "compaction_trigger")? {
        return Ok(None);
    }
    let lease = encode::scratch(owner, |e| pointer(doc, field, e))?;
    let path = doc.import_json(owner, lease)?;
    let list = isolated_list(doc, "xai", "parameters")?;
    let entry = doc.add(None, Value::Object)?;
    doc.append(list, entry)?;
    doc.bind_origin(entry, root, None)?;
    doc.rename(path, "pointer")?;
    doc.append(entry, path)?;
    doc.detach(root, field)?;
    doc.rename(field, "value")?;
    doc.append(entry, field)?;
    Ok(Some(entry))
}

/// Every bound list present in `x`: [`BOUND_LISTS`], then [`ISOLATED_LISTS`].
pub fn bound_lists(doc: &Document, x: usize) -> Result<[Option<usize>; BOUND_LISTS.len() + ISOLATED_LISTS.len()]> {
    let mut lists = [None; BOUND_LISTS.len() + ISOLATED_LISTS.len()];
    for (slot, key) in lists.iter_mut().zip(BOUND_LISTS) {
        *slot = doc.member(x, key)?;
    }
    let map = doc.member(x, "isolated")?;
    for (slot, (provider, key)) in lists[BOUND_LISTS.len()..].iter_mut().zip(ISOLATED_LISTS) {
        if let Some(own) = map.map(|map| doc.member(map, provider)).transpose()?.flatten() {
            *slot = doc.member(own, key)?;
        }
    }
    Ok(lists)
}

/// Native carriers a binding may name. `shape` marks a native result object shape.
pub const CARRIERS: [&str; 4] = ["image_url", "input_audio", "file", "shape"];

/// Write a node's absolute JSON pointer without copying its path to the heap.
pub fn pointer(doc: &Document, mut node: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let mut path = [None; 128];
    let mut depth = 0;
    while let Some(parent) = doc.node(node)?.parent {
        *path.get_mut(depth).ok_or(Error::Limit)? = Some((parent, node));
        depth += 1;
        node = parent;
    }
    e.bytes(b"\"")?;
    for entry in path[..depth].iter().rev() {
        let (parent, node) = entry.ok_or(Error::Schema)?;
        e.bytes(b"/")?;
        if matches!(doc.node(parent)?.value, Value::Array) {
            // ponytail: walks the array per pointer, quadratic in elements with preserved
            // fields. The default metadata quota limits the element count; 0.14 ms is one sampled
            // shape, not a ceiling. Index positions if that quota grows (docs/DEFERRED-OPTIONS.md).
            let index = doc
                .children(parent)?
                .position(|child| child == node)
                .ok_or(Error::Schema)?;
            e.unsigned(index as u64)?;
        } else {
            write_pointer_segment(doc, doc.node(node)?.name.ok_or(Error::Schema)?, e)?;
        }
    }
    e.bytes(b"\"")
}

/// Write a member name as one RFC 6901 segment inside a JSON pointer string. A name whose
/// stored bytes are contiguous and need no JSON escape is copied in unescaped runs; any other
/// name is decoded one scalar at a time.
pub(crate) fn write_pointer_segment(doc: &Document, name: Name, e: &mut Encoder<'_, '_>) -> Result<()> {
    let encoded = matches!(name, Name::Pointer(..));
    let raw = match name {
        Name::Static(name) => Some(name.as_bytes()),
        Name::Mapped(text, _) if text.span.len() >= 2 => doc.source(text)?.slice(Span {
            start: text.span.start + 1,
            end: text.span.end - 1,
        })?,
        Name::Pointer(text, _) => doc.source(text)?.slice(text.span)?,
        _ => None,
    };
    if let Some(bytes) = raw.filter(|bytes| plain(bytes, encoded)) {
        if encoded {
            // A pointer name is the stored pointer's last segment, already encoded.
            return e.bytes(bytes);
        }
        let mut start = 0;
        for (index, &byte) in bytes.iter().enumerate() {
            let escape: &[u8] = match byte {
                b'~' => b"~0",
                b'/' => b"~1",
                _ => continue,
            };
            e.bytes(&bytes[start..index])?;
            e.bytes(escape)?;
            start = index + 1;
        }
        return e.bytes(&bytes[start..]);
    }
    let mut chars = doc.name_chars(name)?;
    while let Some(c) = chars.next_scalar()? {
        match c {
            '~' => e.bytes(b"~0")?,
            '/' => e.bytes(b"~1")?,
            c => e.escaped(c.encode_utf8(&mut [0; 4]))?,
        }
    }
    Ok(())
}

/// Whether `bytes` hold no JSON escape and no byte that needs one and, once `encoded`, only
/// valid `~0` and `~1` sequences, so that copying them writes what decoding would.
fn plain(bytes: &[u8], encoded: bool) -> bool {
    let mut tilde = false;
    for &byte in bytes {
        if byte == b'"' || byte == b'\\' || byte < 0x20 || (tilde && byte != b'0' && byte != b'1') {
            return false;
        }
        tilde = encoded && byte == b'~';
    }
    !tilde
}

#[cfg(test)]
thread_local! {
    /// Member names [`Segment::matches`] compared on this thread, so tests can bound lookups.
    pub(crate) static COMPARED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// One escaped pointer segment, decoded lazily from document storage.
#[derive(Clone)]
pub struct Segment<'a> {
    chars: Characters<'a>,
    remaining: usize,
}
impl Segment<'_> {
    pub fn index(mut self) -> Result<usize> {
        let mut index = 0usize;
        let mut any = false;
        while let Some(c) = self.next()? {
            if !c.is_ascii_digit() {
                return Err(Error::Schema);
            }
            index = index
                .checked_mul(10)
                .and_then(|n| n.checked_add(c as usize - '0' as usize))
                .ok_or(Error::Limit)?;
            any = true;
        }
        if any { Ok(index) } else { Err(Error::Schema) }
    }
    pub(crate) fn next(&mut self) -> Result<Option<char>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        let c = self.chars.next_scalar()?.ok_or(Error::Schema)?;
        if c != '~' {
            return Ok(Some(c));
        }
        if self.remaining == 0 {
            return Err(Error::Schema);
        }
        self.remaining -= 1;
        match self.chars.next_scalar()? {
            Some('0') => Ok(Some('~')),
            Some('1') => Ok(Some('/')),
            _ => Err(Error::Schema),
        }
    }
    pub fn is(&self, expected: &str) -> Result<bool> {
        let mut chars = self.clone();
        for c in expected.chars() {
            if chars.next()? != Some(c) {
                return Ok(false);
            }
        }
        Ok(chars.next()?.is_none())
    }
    pub fn matches(&self, doc: &Document, name: Name) -> Result<bool> {
        #[cfg(test)]
        COMPARED.with(|compared| compared.set(compared.get() + 1));
        let mut chars = self.clone();
        let mut name = doc.name_chars(name)?;
        loop {
            let c = chars.next()?;
            if c != name.next_scalar()? {
                return Ok(false);
            }
            if c.is_none() {
                return Ok(true);
            }
        }
    }
}

/// Split the next segment after a consumed `/`; the flag is true for the last one.
pub fn segment<'a>(chars: &mut Characters<'a>) -> Result<(Segment<'a>, bool)> {
    let start = chars.clone();
    let mut remaining = 0;
    let last = loop {
        match chars.next_scalar()? {
            Some('/') => break false,
            None => break true,
            Some(_) => remaining += 1,
        }
    };
    Ok((
        Segment {
            chars: start,
            remaining,
        },
        last,
    ))
}

/// The last segment of a non-empty pointer.
pub fn leaf(mut chars: Characters<'_>) -> Result<Segment<'_>> {
    if chars.next_scalar()? != Some('/') {
        return Err(Error::Schema);
    }
    loop {
        let (name, last) = segment(&mut chars)?;
        if last {
            return Ok(name);
        }
    }
}

/// Write a segment as an unescaped JSON object key.
pub fn write_segment(mut name: Segment<'_>, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"\"")?;
    while let Some(c) = name.next()? {
        e.escaped(c.encode_utf8(&mut [0; 4]))?;
    }
    e.bytes(b"\"")
}

/// The node a pointer names; the empty pointer is the root.
pub fn resolve(doc: &Document, pointer: usize) -> Result<usize> {
    resolve_to(doc, doc.root(), doc.chars(pointer)?, false, &mut |_, _| Ok(None))
}

/// The container that holds the node a pointer names.
pub fn resolve_parent(doc: &Document, pointer: usize) -> Result<usize> {
    resolve_to(doc, doc.root(), doc.chars(pointer)?, true, &mut |_, _| Ok(None))
}

/// Like [`resolve_parent`]; `child(container, segment)` may name the child a segment selects
/// without a walk or scan, and `None` falls back to one.
pub(crate) fn resolve_parent_indexed(
    doc: &Document,
    pointer: Characters<'_>,
    child: &mut dyn FnMut(usize, &Segment<'_>) -> Result<Option<usize>>,
) -> Result<usize> {
    resolve_to(doc, doc.root(), pointer, true, child)
}

/// Resolves `chars` from `base`, the root of the document the pointer was written for.
fn resolve_to(
    doc: &Document,
    base: usize,
    mut chars: Characters<'_>,
    parent_only: bool,
    child: &mut dyn FnMut(usize, &Segment<'_>) -> Result<Option<usize>>,
) -> Result<usize> {
    match chars.next_scalar()? {
        None => return Ok(base),
        Some('/') => (),
        _ => return Err(Error::Schema),
    }
    let mut parent = base;
    loop {
        let (name, last) = segment(&mut chars)?;
        if last && parent_only {
            return Ok(parent);
        }
        let value = doc.node(parent)?.value;
        if !matches!(value, Value::Object | Value::Array) {
            return Err(Error::Schema);
        }
        if let Some(found) = child(parent, &name)? {
            parent = found;
        } else if matches!(value, Value::Object) {
            // Without an index, as in binding restore, scan the members.
            let mut found = None;
            for member in doc.children(parent)? {
                if name.matches(doc, doc.node(member)?.name.ok_or(Error::Schema)?)? {
                    found = Some(member);
                    break;
                }
            }
            parent = found.ok_or(Error::Schema)?;
        } else {
            // ponytail: an element other than the indexed last one is reached by walking;
            // binding restore pays this per entry. The default metadata quota limits the
            // entries; 0.06 ms is one sampled shape, not a ceiling (docs/DEFERRED-OPTIONS.md).
            parent = doc.children(parent)?.nth(name.index()?).ok_or(Error::Schema)?;
        }
        if last {
            return Ok(parent);
        }
    }
}

/// Serialize trusted node bindings of every [`BOUND_LISTS`] entry before the
/// canonical request fields are published.
pub fn bindings(doc: &mut Document, owner: &mut Writer) -> Result<()> {
    let x = doc.required(doc.root(), "x_ck")?;
    for list in bound_lists(doc, x)?.into_iter().flatten() {
        let mut next = doc.node(list)?.first;
        while let Some(entry) = next {
            next = doc.node(entry)?.next;
            let Some((parent, carrier)) = doc.origin(entry)? else {
                continue;
            };
            let lease = encode::scratch(owner, |e| pointer(doc, parent, e))?;
            let binding = doc.import_json(owner, lease)?;
            doc.rename(binding, "binding")?;
            doc.append(entry, binding)?;
            if let Some(carrier) = carrier {
                doc.set(entry, "carrier", Value::Text(carrier))?;
            }
        }
    }
    Ok(())
}

/// Re-bind serialized bindings. Call only on a trusted reconstructed canonical document.
pub fn restore_bindings(doc: &mut Document) -> Result<()> {
    let x = doc.required(doc.root(), "x_ck")?;
    restore_bindings_from(doc, doc.root(), x)
}

/// [`restore_bindings`] for a canonical request imported as the subtree `base`, whose `x_ck` is
/// `x`: each binding resolves inside that subtree.
pub(crate) fn restore_bindings_from(doc: &mut Document, base: usize, x: usize) -> Result<()> {
    let replay = doc.member(x, "replay")?;
    for list in bound_lists(doc, x)?.into_iter().flatten() {
        let mut next = doc.node(list)?.first;
        while let Some(entry) = next {
            next = doc.node(entry)?.next;
            let Some(binding) = doc.member(entry, "binding")? else {
                continue;
            };
            let parent = resolve_to(doc, base, doc.chars(binding)?, false, &mut |_, _| Ok(None))?;
            let carrier = match doc.member(entry, "carrier")? {
                Some(carrier) => Some(carrier_name(doc, carrier, (Some(list) == replay).then_some(entry))?),
                None => None,
            };
            doc.bind_origin(entry, parent, carrier)?;
        }
    }
    Ok(())
}

/// The carrier `carrier` names. `replay`: the entry, when it sits in `x_ck.replay`.
fn carrier_name(doc: &Document, carrier: usize, replay: Option<usize>) -> Result<&'static str> {
    if let Some(entry) = replay
        && doc.string_is(carrier, GEMINI_SIGNATURE)?
        && produced_gemini_signature(doc, entry)?
    {
        return Ok(GEMINI_SIGNATURE);
    }
    for name in CARRIERS {
        if doc.string_is(carrier, name)? {
            return Ok(name);
        }
    }
    Err(Error::Schema)
}

/// Carrier of a Gemini `thoughtSignature` that history projection binds to the part it rides on.
const GEMINI_SIGNATURE: &str = "signature";

/// Whether replay `entry` is a Gemini signature tagged with the account that produced it. Only
/// such an entry may name [`GEMINI_SIGNATURE`]: admission returns it to that account alone and
/// withholds it from every other (Settled 73).
fn produced_gemini_signature(doc: &Document, entry: usize) -> Result<bool> {
    let Some(kind) = doc.member(entry, "kind")? else {
        return Ok(false);
    };
    Ok(doc.string_is(kind, "gemini")?
        && doc.member(entry, "signature")?.is_some()
        && doc.member(entry, "producer")?.is_some())
}
