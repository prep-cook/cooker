//! Canonical input for usage accounting (`docs/EXECUTION-CONTRACT.md` §8).

use ck_stream::{Read, Reader, Writer};
use ck_translate::codec::document::{Document, Value};
use ck_translate::codec::event::Kind;
use ck_translate::codec::{self, RequestView, encode, internal};

/// The counts `usage_record::Tracker` records: one attempt's latest canonical `x_ck.usage`
/// snapshot (`docs/INTERNAL-STREAM.md` §3.5), read from the attempt's stream by a
/// runtime-owned reader, independently of client delivery and resume. A later snapshot
/// replaces the whole value; snapshots are never summed. `None` is an absent count, never zero.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CanonicalUsage {
    /// Inclusive input.
    pub input_tokens: Option<u64>,
    /// Inclusive output, reasoning included.
    pub output_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
    pub input_uncached_tokens: Option<u64>,
    pub cache_read_tokens: Option<u64>,
    pub cache_write_tokens: Option<u64>,
    /// `cache_write_by_ttl["300"]`.
    pub cache_write_5m_tokens: Option<u64>,
    /// `cache_write_by_ttl["3600"]`.
    pub cache_write_1h_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
    /// The snapshot is the scope's last authoritative one.
    pub final_snapshot: bool,
}

/// Response facts the usage record keeps besides counts, read from the same stream: the
/// `response.start` model and the last chunk `service_tier`. A later value replaces an earlier.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResponseFacts {
    pub model: Option<String>,
    pub service_tier: Option<String>,
}

/// Reads one attempt's response stream from its first record to its terminal: the last
/// usage snapshot, if any, and the response facts. `stream` is an allocation handle on the
/// stream and `reader` attaches at its start. The runtime runs this once per attempt
/// (`Registry::spawn`), so no client delivery or resume accounts usage.
pub async fn read(mut stream: Writer, mut reader: Reader) -> (Option<CanonicalUsage>, ResponseFacts) {
    let mut usage = None;
    let mut facts = ResponseFacts::default();
    while let Some(Read::Record(lease)) = reader.next().await {
        let observed = internal::decode(&mut stream, lease).and_then(|(kind, doc)| match kind {
            Kind::Usage => snapshot(&doc).map(|snapshot| usage = Some(snapshot)),
            Kind::ResponseStart => text(&doc, "model").map(|model| {
                if let Some(model) = model {
                    facts.model = model;
                }
            }),
            Kind::Chunk => text(&doc, "service_tier").map(|tier| {
                if let Some(tier) = tier {
                    facts.service_tier = tier;
                }
            }),
            _ => Ok(()),
        });
        if let Err(error) = observed {
            // The producer validated every record it published; only storage loss gets here.
            tracing::warn!(%error, "usage reader stopped before the stream's terminal");
            break;
        }
    }
    (usage, facts)
}

fn snapshot(doc: &Document) -> codec::Result<CanonicalUsage> {
    let usage = doc.required(doc.root(), "usage")?;
    let count = |parent: usize, name: &str| -> codec::Result<Option<u64>> {
        match doc.member(parent, name)? {
            Some(node) if !matches!(doc.node(node)?.value, Value::Null) => doc.unsigned(node).map(Some),
            _ => Ok(None),
        }
    };
    let ttl = doc
        .member(usage, "cache_write_by_ttl")?
        .filter(|&node| doc.node(node).is_ok_and(|n| matches!(n.value, Value::Object)));
    let by_ttl = |seconds: &str| ttl.map_or(Ok(None), |ttl| count(ttl, seconds));
    Ok(CanonicalUsage {
        input_tokens: count(usage, "input_tokens")?,
        output_tokens: count(usage, "output_tokens")?,
        total_tokens: count(usage, "total_tokens")?,
        input_uncached_tokens: count(usage, "input_uncached_tokens")?,
        cache_read_tokens: count(usage, "cache_read_tokens")?,
        cache_write_tokens: count(usage, "cache_write_tokens")?,
        cache_write_5m_tokens: by_ttl("300")?,
        cache_write_1h_tokens: by_ttl("3600")?,
        reasoning_tokens: count(usage, "reasoning_tokens")?,
        final_snapshot: match doc.member(usage, "final")? {
            Some(node) => matches!(doc.node(node)?.value, Value::Bool(true)),
            None => false,
        },
    })
}

/// The root member `name`: `None` when absent, `Some(None)` for `null`, else its text.
fn text(doc: &Document, name: &str) -> codec::Result<Option<Option<String>>> {
    let Some(node) = doc.member(doc.root(), name)? else {
        return Ok(None);
    };
    if matches!(doc.node(node)?.value, Value::Null) {
        return Ok(Some(None));
    }
    let mut text = String::new();
    let mut chars = doc.chars(node)?;
    while let Some(c) = chars.next_scalar()? {
        text.push(c);
    }
    Ok(Some(Some(text)))
}

/// A hash naming the request's cacheable prefix: its tools and its leading system and
/// developer messages. Requests sharing both share the value; nothing of the text is kept.
/// Empty when the request has neither. `owner` is an allocation handle on the request
/// stream, whose scratch holds the hashed encoding until this returns.
pub fn prompt_prefix(request: RequestView<'_>, owner: &mut Writer) -> codec::Result<String> {
    use sha2::Digest;
    let doc = request.document;
    let root = doc.root();
    let tools = doc.member(root, "tools")?;
    let mut system = Vec::new();
    if let Some(messages) = doc.member(root, "messages")? {
        for message in doc.children(messages)? {
            match doc.member(message, "role")? {
                Some(role) if doc.string_is(role, "system")? || doc.string_is(role, "developer")? => {
                    system.push(message);
                }
                _ => break,
            }
        }
    }
    if tools.is_none() && system.is_empty() {
        return Ok(String::new());
    }
    let encoded = encode::scratch(owner, |e| {
        e.bytes(b"[")?;
        match tools {
            Some(tools) => doc.write(tools, e)?,
            None => e.bytes(b"null")?,
        }
        for &message in &system {
            e.bytes(b",")?;
            doc.write(message, e)?;
        }
        e.bytes(b"]")
    })?;
    let mut hash = sha2::Sha256::new();
    hash.update(b"ck:prompt-prefix:v2\0");
    let mut offset = 0;
    while offset < encoded.len() {
        let chunk = encoded.chunk(offset)?;
        hash.update(chunk);
        offset += chunk.len();
    }
    let digest = hash.finalize();
    Ok(format!(
        "{:016x}",
        u64::from_be_bytes(digest[..8].try_into().expect("8 bytes"))
    ))
}
