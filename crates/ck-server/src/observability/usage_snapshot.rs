//! Canonical usage input for accounting (`docs/EXECUTION-CONTRACT.md` §5).

/// The input of `usage_record::Tracker`: one attempt's latest canonical `x_ck.usage`
/// snapshot (`docs/INTERNAL-STREAM.md` §3.5), read from the attempt's stream by a
/// runtime-owned reader, independently of client delivery and resume. A later snapshot replaces the whole
/// value; snapshots are never summed. `None` is an absent count, never zero.
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
