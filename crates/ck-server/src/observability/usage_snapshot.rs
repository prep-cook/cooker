//! Canonical input for usage accounting (`docs/EXECUTION-CONTRACT.md` §8).

use ck_stream::{Read, Reader, Writer};
use ck_translate::codec::document::{Document, Value};
use ck_translate::codec::encode::{self, Encoder};
use ck_translate::codec::event::Kind;
use ck_translate::codec::{self, RequestView, internal};

/// The counts `usage_record::Tracker` records: one usage scope's latest canonical `x_ck.usage`
/// snapshot (`docs/INTERNAL-STREAM.md` §3.5, [`Usage`]), read from the attempt's stream by a
/// runtime-owned reader, independently of client delivery and resume. A later snapshot
/// replaces the whole value; snapshots are never summed. `None` is an absent count, never zero.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
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
    /// `cache_write_by_ttl["1800"]`.
    pub cache_write_30m_tokens: Option<u64>,
    /// `cache_write_by_ttl["43200"]`.
    pub cache_write_12h_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
    /// The tier the source's own usage reports ([`TIERS`]).
    pub service_tier: Option<String>,
    /// The billed units besides the token counts that the snapshot reports, in [`DIMENSIONS`]
    /// order. An unreported dimension is not listed.
    pub dimensions: Vec<(&'static str, Dimension)>,
    /// The snapshot is the scope's last authoritative one.
    pub final_snapshot: bool,
}

/// One billed unit's reported value. Each dimension keeps its own unit: counts of different
/// dimensions are never added or priced alike.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Dimension {
    Count(u64),
    /// A unit that can be fractional, such as Codex's rollout budget units: the JSON number as
    /// the upstream spelled it, never rounded or converted to another unit.
    Decimal(String),
    Text(String),
}

/// What one [`DIMENSIONS`] row holds, and so which [`Dimension`] it records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Unit {
    /// An unsigned integer: a token, call or tick count.
    Count,
    /// Any JSON number ([`Dimension::Decimal`]).
    Decimal,
    Text,
}

/// The billing dimensions accounting records besides the token counts: `(source protocol, path
/// in its native usage, record name, unit)`. Both accounting paths read these rows:
/// the canonical snapshot's `native` value, which keeps provider fields isolated (Q3), and the
/// legacy parsers' upstream usage object. A provider unit adds a row for each billed unit its
/// usage reports.
const DIMENSIONS: &[(&str, &[&str], &str, Unit)] = &[
    (
        "chat",
        &["prompt_tokens_details", "audio_tokens"],
        "input_audio_tokens",
        Unit::Count,
    ),
    (
        "chat",
        &["prompt_tokens_details", "text_tokens"],
        "input_text_tokens",
        Unit::Count,
    ),
    (
        "chat",
        &["prompt_tokens_details", "image_tokens"],
        "input_image_tokens",
        Unit::Count,
    ),
    (
        "chat",
        &["completion_tokens_details", "audio_tokens"],
        "output_audio_tokens",
        Unit::Count,
    ),
    (
        "chat",
        &["completion_tokens_details", "text_tokens"],
        "output_text_tokens",
        Unit::Count,
    ),
    (
        "chat",
        &["completion_tokens_details", "image_tokens"],
        "output_image_tokens",
        Unit::Count,
    ),
    (
        "chat",
        &["completion_tokens_details", "accepted_prediction_tokens"],
        "accepted_prediction_tokens",
        Unit::Count,
    ),
    (
        "chat",
        &["completion_tokens_details", "rejected_prediction_tokens"],
        "rejected_prediction_tokens",
        Unit::Count,
    ),
    ("anthropic", &["speed"], "speed", Unit::Text),
    ("anthropic", &["inference_geo"], "inference_geo", Unit::Text),
    (
        "anthropic",
        &["server_tool_use", "web_search_requests"],
        "web_search_requests",
        Unit::Count,
    ),
    (
        "anthropic",
        &["server_tool_use", "web_fetch_requests"],
        "web_fetch_requests",
        Unit::Count,
    ),
    // Meta segmentation: per-image and per-frame billing quantities.
    ("responses", &["images_processed"], "images_processed", Unit::Count),
    (
        "responses",
        &["video_frames_processed"],
        "video_frames_processed",
        Unit::Count,
    ),
    // xAI: the charge in its two currency units (10^10 ticks or 10^9 nano-USD per dollar), kept
    // apart and as reported, and the server-side tool counts billed per call or item.
    ("responses", &["cost_in_usd_ticks"], "cost_in_usd_ticks", Unit::Count),
    ("responses", &["cost_in_nano_usd"], "cost_in_nano_usd", Unit::Count),
    ("responses", &["num_sources_used"], "num_sources_used", Unit::Count),
    (
        "responses",
        &["num_server_side_tools_used"],
        "num_server_side_tools_used",
        Unit::Count,
    ),
    (
        "responses",
        &["server_side_tool_usage_details", "web_search_calls"],
        "web_search_calls",
        Unit::Count,
    ),
    (
        "responses",
        &["server_side_tool_usage_details", "x_search_calls"],
        "x_search_calls",
        Unit::Count,
    ),
    (
        "responses",
        &["server_side_tool_usage_details", "x_posts_fetched"],
        "x_posts_fetched",
        Unit::Count,
    ),
    (
        "responses",
        &["server_side_tool_usage_details", "x_users_fetched"],
        "x_users_fetched",
        Unit::Count,
    ),
    (
        "responses",
        &["server_side_tool_usage_details", "code_interpreter_calls"],
        "code_interpreter_calls",
        Unit::Count,
    ),
    (
        "responses",
        &["server_side_tool_usage_details", "file_search_calls"],
        "file_search_calls",
        Unit::Count,
    ),
    (
        "responses",
        &["server_side_tool_usage_details", "document_search_calls"],
        "document_search_calls",
        Unit::Count,
    ),
    (
        "responses",
        &["server_side_tool_usage_details", "image_generation_calls"],
        "image_generation_calls",
        Unit::Count,
    ),
    (
        "responses",
        &["server_side_tool_usage_details", "mcp_calls"],
        "mcp_calls",
        Unit::Count,
    ),
    // Codex: rollout budget units, a quota unit that is neither tokens nor currency.
    (
        "responses",
        &["codex_rollout_budget_units"],
        "codex_rollout_budget_units",
        Unit::Decimal,
    ),
    // xAI images and video jobs: the charge, the one billed unit a video job reports.
    ("xai-images", &["cost_in_usd_ticks"], "cost_in_usd_ticks", Unit::Count),
    ("xai", &["cost_in_usd_ticks"], "cost_in_usd_ticks", Unit::Count),
];

/// The service tier inside a source's native usage: `(protocol, member)`.
const TIERS: &[(&str, &str)] = &[("anthropic", "service_tier"), ("gemini", "serviceTier")];

/// The [`DIMENSIONS`] rows of one source protocol: `(path, record name, unit)`.
pub(crate) fn dimension_rows(protocol: &str) -> impl Iterator<Item = (&'static [&'static str], &'static str, Unit)> {
    DIMENSIONS
        .iter()
        .filter(move |row| row.0 == protocol)
        .map(|&(_, path, name, unit)| (path, name, unit))
}

/// The usage member, the first segment of its [`DIMENSIONS`] path, holding the dimension `name`.
pub(crate) fn dimension_member(name: &str) -> Option<&'static str> {
    DIMENSIONS.iter().find(|row| row.2 == name).map(|row| row.1[0])
}

/// The usage member holding one source protocol's service tier ([`TIERS`]).
pub(crate) fn tier_member(protocol: &str) -> Option<&'static str> {
    TIERS.iter().find(|row| row.0 == protocol).map(|row| row.1)
}

/// Response facts the usage record keeps besides counts, read from the same stream: the
/// `response.start` model and the last chunk `service_tier`. A later value replaces an earlier.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResponseFacts {
    pub model: Option<String>,
    pub service_tier: Option<String>,
}

/// One attempt's usage by scope (E-Q13): the attempt's own, and one per upstream response, tool
/// model or video job a codec reported apart from it (`scope` beside `usage`, such as
/// `tool:<model>` or `job:<id>`). A snapshot replaces only its own scope's; scopes are never
/// summed or merged. Every published scope is kept: each needs its own usage record in the
/// attempt's stream, so the stream's storage limit bounds the map, and no reported usage is
/// dropped.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Usage {
    /// `(scope, last snapshot)` in first-reported order; `None` is the attempt's own scope.
    scopes: Vec<(Option<String>, CanonicalUsage)>,
    /// Each scope's slot in `scopes`.
    slots: std::collections::HashMap<Option<String>, usize>,
}

impl Usage {
    /// The attempt's own last snapshot.
    pub fn attempt(&self) -> Option<&CanonicalUsage> {
        self.slots.get(&None).map(|&slot| &self.scopes[slot].1)
    }

    /// The last snapshot of every named scope, in first-reported order.
    pub fn scoped(&self) -> impl Iterator<Item = (&str, &CanonicalUsage)> {
        self.scopes
            .iter()
            .filter_map(|(scope, usage)| scope.as_deref().map(|scope| (scope, usage)))
    }

    /// Replaces `scope`'s snapshot, or opens the scope.
    pub(crate) fn replace(&mut self, scope: Option<String>, usage: CanonicalUsage) {
        match self.slots.get(&scope) {
            Some(&slot) => self.scopes[slot].1 = usage,
            None => {
                self.slots.insert(scope.clone(), self.scopes.len());
                self.scopes.push((scope, usage));
            }
        }
    }
}

/// Reads one attempt's response stream from its first record to its terminal: the last
/// usage snapshot of each scope, and the response facts. `stream` is an allocation handle on
/// the stream and `reader` attaches at its start. The runtime runs this once per attempt
/// (`Registry::spawn`), so no client delivery or resume accounts usage.
pub async fn read(mut stream: Writer, mut reader: Reader) -> (Usage, ResponseFacts) {
    let mut usage = Usage::default();
    let mut facts = ResponseFacts::default();
    while let Some(Read::Record(lease)) = reader.next().await {
        let observed = internal::decode(&mut stream, lease).and_then(|(kind, doc)| match kind {
            Kind::Usage => observe(&doc, &mut usage),
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

/// Replaces the snapshot of the scope a `usage` record names with the record's.
pub(crate) fn observe(doc: &Document, usage: &mut Usage) -> codec::Result<()> {
    let snapshot = snapshot(doc)?;
    let scope = text(doc, "scope")?.flatten();
    usage.replace(scope, snapshot);
    Ok(())
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
        cache_write_30m_tokens: by_ttl("1800")?,
        cache_write_12h_tokens: by_ttl("43200")?,
        reasoning_tokens: count(usage, "reasoning_tokens")?,
        service_tier: native_tier(doc, usage)?,
        dimensions: dimensions(doc, usage)?,
        final_snapshot: match doc.member(usage, "final")? {
            Some(node) => matches!(doc.node(node)?.value, Value::Bool(true)),
            None => false,
        },
    })
}

/// The node at `path` below `node`, through objects only.
fn at(doc: &Document, mut node: usize, path: &[&str]) -> codec::Result<Option<usize>> {
    for name in path {
        if !matches!(doc.node(node)?.value, Value::Object) {
            return Ok(None);
        }
        match doc.member(node, name)? {
            Some(child) => node = child,
            None => return Ok(None),
        }
    }
    Ok(Some(node))
}

/// The value of one dimension in its row's unit. Null, a count that is not an unsigned integer
/// and a value of another kind report nothing the record can name, so the dimension stays absent.
fn dimension(doc: &Document, node: usize, unit: Unit) -> codec::Result<Option<Dimension>> {
    match (doc.node(node)?.value, unit) {
        (Value::Unsigned(_) | Value::Number(_), Unit::Count) => match doc.unsigned(node) {
            Ok(n) => Ok(Some(Dimension::Count(n))),
            Err(codec::Error::Schema) => Ok(None),
            Err(error) => Err(error),
        },
        (Value::Number(number), Unit::Decimal) => {
            let mut cursor = doc.source(number)?.cursor(number.span)?;
            let mut text = String::new();
            while let Some(b) = cursor.next_byte()? {
                text.push(char::from(b));
            }
            Ok(Some(Dimension::Decimal(text)))
        }
        (Value::String(_) | Value::Text(_), Unit::Text) => string(doc, node).map(|text| Some(Dimension::Text(text))),
        _ => Ok(None),
    }
}

/// The native value of `usage` and its source protocol.
fn native(doc: &Document, usage: usize) -> codec::Result<Option<(usize, String)>> {
    let Some(native) = doc.member(usage, "native")? else {
        return Ok(None);
    };
    match (doc.member(native, "protocol")?, doc.member(native, "value")?) {
        (Some(protocol), Some(value)) if matches!(doc.node(protocol)?.value, Value::String(_) | Value::Text(_)) => {
            Ok(Some((value, string(doc, protocol)?)))
        }
        _ => Ok(None),
    }
}

fn dimensions(doc: &Document, usage: usize) -> codec::Result<Vec<(&'static str, Dimension)>> {
    let mut found = Vec::new();
    if let Some((value, protocol)) = native(doc, usage)? {
        for (path, name, unit) in dimension_rows(&protocol) {
            if let Some(node) = at(doc, value, path)?
                && let Some(value) = dimension(doc, node, unit)?
            {
                found.push((name, value));
            }
        }
    }
    Ok(found)
}

fn native_tier(doc: &Document, usage: usize) -> codec::Result<Option<String>> {
    if let Some((value, protocol)) = native(doc, usage)?
        && let Some(member) = tier_member(&protocol)
        && let Some(node) = at(doc, value, &[member])?
        && let Some(Dimension::Text(tier)) = dimension(doc, node, Unit::Text)?
    {
        return Ok(Some(tier));
    }
    Ok(None)
}

/// The root member `name`: `None` when absent, `Some(None)` for `null`, else its text.
fn text(doc: &Document, name: &str) -> codec::Result<Option<Option<String>>> {
    let Some(node) = doc.member(doc.root(), name)? else {
        return Ok(None);
    };
    if matches!(doc.node(node)?.value, Value::Null) {
        return Ok(Some(None));
    }
    string(doc, node).map(|text| Some(Some(text)))
}

fn string(doc: &Document, node: usize) -> codec::Result<String> {
    let mut text = String::new();
    let mut chars = doc.chars(node)?;
    while let Some(c) = chars.next_scalar()? {
        text.push(c);
    }
    Ok(text)
}

/// A hash naming the request's cacheable prefix: its tools and its leading system and
/// developer messages. Requests sharing both share the value, whatever their client protocol
/// and member order: the hash covers a canonical encoding ([`write_sorted`]). Nothing of
/// the text is kept.
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
            Some(tools) => write_sorted(doc, tools, e)?,
            None => e.bytes(b"null")?,
        }
        for &message in &system {
            e.bytes(b",")?;
            write_sorted(doc, message, e)?;
        }
        e.bytes(b"]")
    })?;
    let mut hash = sha2::Sha256::new();
    hash.update(b"ck:prompt-prefix:v3\0");
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

/// Writes `node` with every object's members sorted by name, recursively, array order kept
/// and strings re-escaped from their decoded text, so equal values encode equally whatever
/// member order or escapes their source used.
fn write_sorted(doc: &Document, node: usize, e: &mut Encoder<'_, '_>) -> codec::Result<()> {
    match doc.node(node)?.value {
        Value::Object => {
            let mut members = Vec::new();
            for child in doc.children(node)? {
                let mut name = String::new();
                let mut chars = doc.name_chars(doc.node(child)?.name.ok_or(codec::Error::Schema)?)?;
                while let Some(c) = chars.next_scalar()? {
                    name.push(c);
                }
                members.push((name, child));
            }
            members.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            e.bytes(b"{")?;
            for (index, (name, child)) in members.iter().enumerate() {
                e.bytes(if index == 0 { b"" } else { b"," })?;
                e.string(name)?;
                e.bytes(b":")?;
                write_sorted(doc, *child, e)?;
            }
            e.bytes(b"}")
        }
        Value::Array => {
            e.bytes(b"[")?;
            for (index, child) in doc.children(node)?.enumerate() {
                e.bytes(if index == 0 { b"" } else { b"," })?;
                write_sorted(doc, child, e)?;
            }
            e.bytes(b"]")
        }
        Value::String(_) | Value::Text(_) => e.quoted_json(|e| {
            let mut chars = doc.chars(node)?;
            while let Some(c) = chars.next_scalar()? {
                e.bytes(c.encode_utf8(&mut [0; 4]).as_bytes())?;
            }
            Ok(())
        }),
        _ => doc.write(node, e),
    }
}
