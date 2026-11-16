//! `config.yaml` in the current (v8) layout (`server.port`, `access.api-keys`,
//! `oauth.auth-auto-refresh-workers`), decoded the way CLIProxyAPI v8 decodes it
//! (internal/config/config_v8.go, config_load.go). A root section the schema does not
//! know is an error, and so is a present section that is not a mapping, so
//! `access: null` is rejected instead of silently opening the proxy.

use std::path::PathBuf;

use anyhow::{Context, bail};
use serde::{Deserialize, Serialize};
use serde_yaml_ng::{Mapping, Value};

mod binary;
pub mod credentials;
mod document;
pub mod go_url;
pub mod model_catalog;
mod realtime;
mod schema;
mod timestamp;
mod trusted;
mod validate;
pub use document::ConfigDocument;
pub use realtime::Realtime;
pub use schema::ValidationPath;
pub use schema::validate as validate_config_fields;
pub use schema::{coerce_typed_scalars, go_bool, go_int};
pub use timestamp::{AsJson, TAG as TIMESTAMP_TAG};

/// `text` parsed as serde_yaml_ng does, except for plain scalars it reads otherwise
/// than yaml.v3: an integer beyond int64 and uint64, which serde_yaml_ng refuses when
/// it fits 128 bits (yaml.v3 reads a decimal one as a float and any other, `0x…` or
/// `0o…`, as text), and a number with underscores (`1_000`) or a leading zero (`007`,
/// `08`), which serde_yaml_ng reads as text and yaml.v3 as a number. Those scalars are
/// read as yaml.v3 reads them, and a timestamp (`2001-12-14`), which yaml.v3 reads into
/// an untyped value as a `time.Time`, is its RFC 3339 text tagged [`TIMESTAMP_TAG`]. A
/// string setting still gets the scalar's text from the literal walk, which reads `text`
/// itself; such a mapping key stays text, as yaml.v3 reads it into a string-keyed map.
pub fn parse_yaml(text: &str) -> Result<Value, serde_yaml_ng::Error> {
    // serde_yaml_ng, like libyaml with its encoding set, reads a leading BOM as part of
    // the first line, which then fails a second top-level key; yaml.v3 skips it.
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let parse = |text: &str| serde_yaml_ng::from_str::<Value>(text);
    // Without an underscore or a leading zero, only a failed parse needs a second look;
    // it keeps its error when no scalar is rewritten.
    let err = if may_resolve(text) {
        None
    } else {
        match parse(text) {
            Ok(value) if !needs_scan(&value) => return Ok(value),
            Ok(_) => None,
            Err(err) => Some(err),
        }
    };
    let as_is = |err: Option<serde_yaml_ng::Error>| err.map_or_else(|| parse(text), Err);
    let Ok(scalars) = binary::plain_scalars(text) else {
        return as_is(err);
    };
    // One pass over the scalars, then one parse.
    let mut out = String::with_capacity(text.len());
    let mut last = 0;
    for plain in scalars.into_iter().filter(|p| p.timestamp_tag.is_none()) {
        let read = std::str::from_utf8(&plain.value)
            .ok()
            .and_then(|s| go_read(s, plain.key));
        let (Some(read), Some(before)) = (read, text.get(last..plain.span.start)) else {
            continue;
        };
        out.push_str(before);
        out.push_str(&read);
        last = plain.span.end;
    }
    if last == 0 {
        return as_is(err);
    }
    out.push_str(&text[last.min(text.len())..]);
    parse(&out)
}

/// `value` as serde_yaml_ng writes it, except that a string [`parse_yaml`] would read
/// back as a number or timestamp when written plain (`1_000`, `0_8`, `0b-1`,
/// `2001-12-14`) is double-quoted, as yaml.v3 quotes it, and a [`TIMESTAMP_TAG`] value
/// is its text, plain, as yaml.v3 writes a `time.Time`. serde_yaml_ng quotes only the
/// strings it reads as something else itself.
pub fn yaml_string(value: &Value) -> Result<String, serde_yaml_ng::Error> {
    let text = serde_yaml_ng::to_string(value)?;
    // A number serde_yaml_ng writes has no underscore or leading zero, so each scalar
    // flagged here is a string. It quotes a string beyond 64 bits (`'18446744073709551616'`)
    // itself.
    if !may_resolve(&text) && !needs_scan(value) {
        return Ok(text);
    }
    let Ok(scalars) = binary::plain_scalars(&text) else {
        return Ok(text);
    };
    let mut out = String::with_capacity(text.len() + 2);
    let mut last = 0;
    for plain in scalars {
        if let Some(tag) = plain.timestamp_tag {
            out.push_str(&text[last..tag]);
            out.push_str(&text[plain.span.clone()]);
            last = plain.span.end;
            continue;
        }
        // A key is checked as a value: yaml.v3 reads a plain `1_000:` key as a number,
        // which in an untyped map is no longer the string key it was.
        let flagged = std::str::from_utf8(&plain.value).ok().and_then(|s| go_read(s, false));
        if flagged.is_none() {
            continue;
        }
        // The flagged spellings hold only ASCII letters, digits, spaces and `_.,:+-`.
        out.push_str(&text[last..plain.span.start]);
        out.push('"');
        out.push_str(&text[plain.span.clone()]);
        out.push('"');
        last = plain.span.end;
    }
    out.push_str(&text[last..]);
    Ok(out)
}

/// Whether `text` may hold a plain scalar with an underscore or a leading zero, which
/// [`go_read`] rewrites without a parse error to point at it.
fn may_resolve(text: &str) -> bool {
    let zero = |w: &[u8]| w[0] == b'0' && (w[1].is_ascii_digit() || matches!(w[1], b'b' | b'o' | b'X' | b'O' | b'B'));
    text.contains('_')
        || text.as_bytes().starts_with(b"0")
        || text
            .as_bytes()
            .windows(3)
            .any(|w| zero(&w[1..]) && !(w[0].is_ascii_alphanumeric() || w[0] == b'.'))
}

/// Whether `value` holds a string, key or value, that may be a plain timestamp, a
/// [`TIMESTAMP_TAG`] value, or an integer past int64, which `+` makes a float in Go.
fn needs_scan(value: &Value) -> bool {
    match value {
        Value::String(s) => timestamp::go_timestamp(s).is_some(),
        Value::Number(n) => n.as_i64().is_none() && n.as_u64().is_some(),
        Value::Sequence(items) => items.iter().any(needs_scan),
        Value::Mapping(m) => m.iter().any(|(k, v)| needs_scan(k) || needs_scan(v)),
        Value::Tagged(t) => t.tag == timestamp::TAG || needs_scan(&t.value),
        _ => false,
    }
}

/// What [`parse_yaml`] writes for a plain scalar so that serde_yaml_ng reads it as
/// yaml.v3 does; `None` where they agree. A mapping key keeps its text unless
/// serde_yaml_ng would refuse it.
fn go_read(scalar: &str, key: bool) -> Option<String> {
    if !key && let Some(time) = timestamp::go_timestamp(scalar) {
        // Verbatim, so a `%TAG !` directive cannot redefine it.
        return Some(format!("!<{}> {time}", timestamp::TAG));
    }
    let unsigned = scalar.strip_prefix(['+', '-']).unwrap_or(scalar).as_bytes();
    let leading_zero = unsigned.first() == Some(&b'0')
        && unsigned
            .get(1)
            .is_some_and(|b| b.is_ascii_digit() || matches!(b, b'b' | b'o' | b'X' | b'O' | b'B'));
    if scalar.contains('_') || leading_zero {
        // A `0o` or `0b` integer beyond 64 bits that Go reads as text.
        (!key)
            .then(|| go_resolved(scalar))
            .flatten()
            .or_else(|| go_big_integer(scalar, key))
    } else {
        go_big_integer(scalar, key)
    }
}

/// yaml.v3's reading of a plain scalar with underscores or a leading zero (its
/// `resolve`), written so that serde_yaml_ng reads the same; `None` where yaml.v3 reads
/// text or a timestamp (`0001-01-01`), which serde_yaml_ng reads as text. A scalar that
/// starts with a digit or sign drops its underscores and reads as an integer (`0x`,
/// `0o`, `0b` or a leading `0` for octal), else as a float in YAML's float syntax; one
/// that starts with `.` reads as a float when each underscore sits between digits.
fn go_resolved(text: &str) -> Option<String> {
    let plain = text.replace('_', "");
    let float = |s: &str| {
        let f = go_yaml_float(s)?;
        Some(format!("{f:e}"))
    };
    match text.as_bytes().first()? {
        b'.' => {
            let bytes = text.as_bytes();
            let between_digits = bytes.iter().enumerate().all(|(i, &b)| {
                b != b'_'
                    || (i > 0 && bytes[i - 1].is_ascii_digit() && bytes.get(i + 1).is_some_and(u8::is_ascii_digit))
            });
            if between_digits { float(&plain) } else { None }
        }
        b'0'..=b'9' | b'+' | b'-' => {
            let negative = plain.starts_with('-');
            let signed = plain.starts_with(['+', '-']);
            let unsigned = plain.strip_prefix(['+', '-']).unwrap_or(&plain);
            let (radix, digits) = match unsigned.get(..2) {
                Some("0x" | "0X") => (16, &unsigned[2..]),
                Some("0o" | "0O") => (8, &unsigned[2..]),
                Some("0b" | "0B") => (2, &unsigned[2..]),
                _ if unsigned.len() > 1 && unsigned.starts_with('0') => (8, &unsigned[1..]),
                _ => (10, unsigned),
            };
            if !digits.is_empty() && digits.chars().all(|c| c.is_digit(radix)) {
                // Go's ParseInt, then ParseUint (which takes no sign).
                if let Ok(n) = u64::from_str_radix(digits, radix) {
                    if negative && n <= 1 << 63 {
                        return Some(format!("-{n}"));
                    }
                    if !negative && (n <= i64::MAX as u64 || !signed) {
                        return Some(n.to_string());
                    }
                    // `+0x…`, `+0o…` or `+0b…` past int64: ParseInt overflows and
                    // ParseUint takes no sign, so Go reads text; serde_yaml_ng a u64.
                    if !negative && unsigned.as_bytes().get(1).is_some_and(u8::is_ascii_alphabetic) {
                        return Some(format!("'{text}'"));
                    }
                }
            }
            if let Some(f) = float(&plain) {
                return Some(f);
            }
            // Go's last resort: `0b` or `0o` then ParseInt of the rest, which takes a
            // sign (`0b-1` is -1), then ParseUint.
            let (radix, rest) = match plain.get(..2)? {
                "0b" => (2, &plain[2..]),
                "0o" => (8, &plain[2..]),
                _ => return None,
            };
            let n = i64::from_str_radix(rest, radix).map(|n| n.to_string());
            let unsigned = || {
                (!rest.starts_with('+'))
                    .then(|| u64::from_str_radix(rest, radix).ok())
                    .flatten()
            };
            n.ok().or_else(|| unsigned().map(|n| n.to_string()))
        }
        _ => None,
    }
}

/// `s` as a float when it has yaml.v3's float syntax and Go parses it in range.
fn go_yaml_float(s: &str) -> Option<f64> {
    let digits = |s: &str| s.len() - s.trim_start_matches(|c: char| c.is_ascii_digit()).len();
    let rest = s.strip_prefix(['+', '-']).unwrap_or(s);
    let (mantissa, exponent) = match rest.find(['e', 'E']) {
        Some(at) => (&rest[..at], Some(&rest[at + 1..])),
        None => (rest, None),
    };
    let mantissa_ok = match mantissa.split_once('.') {
        Some(("", frac)) => !frac.is_empty() && digits(frac) == frac.len(),
        Some((int, frac)) => !int.is_empty() && digits(int) == int.len() && digits(frac) == frac.len(),
        None => !mantissa.is_empty() && digits(mantissa) == mantissa.len(),
    };
    let exponent_ok = exponent.is_none_or(|e| {
        let e = e.strip_prefix(['+', '-']).unwrap_or(e);
        !e.is_empty() && digits(e) == e.len()
    });
    if !(mantissa_ok && exponent_ok) {
        return None;
    }
    s.parse::<f64>().ok().filter(|f| f.is_finite())
}

/// yaml.v3's reading of a plain integer beyond int64 and uint64 that serde_yaml_ng
/// refuses (one that fits u128, or i128 when negative, and is not a decimal with a
/// leading zero), written so that serde_yaml_ng reads the same: a decimal one as its
/// float, any other quoted. A value with a `+` past int64 reads so too: Go's ParseUint
/// takes no sign, where serde_yaml_ng reads a u64. `None` for any other scalar, which
/// serde_yaml_ng reads.
fn go_big_integer(text: &str, key: bool) -> Option<String> {
    let negative = text.starts_with('-');
    let unsigned = text.strip_prefix(['+', '-']).unwrap_or(text);
    let (radix, digits) = match unsigned.get(..2) {
        Some("0x" | "0X") => (16, &unsigned[2..]),
        Some("0o" | "0O") => (8, &unsigned[2..]),
        Some("0b" | "0B") => (2, &unsigned[2..]),
        _ => (10, unsigned),
    };
    if digits.is_empty() || (radix == 10 && digits.len() > 1 && digits.starts_with('0')) {
        return None;
    }
    let magnitude = u128::from_str_radix(digits, radix).ok()?;
    if negative && magnitude > 1 << 127 {
        return None;
    }
    let fits = if negative {
        i64::from_str_radix(&format!("-{digits}"), radix).is_ok()
    } else if text.starts_with('+') && !key {
        i64::from_str_radix(digits, radix).is_ok()
    } else {
        u64::from_str_radix(digits, radix).is_ok()
    };
    if fits {
        return None;
    }
    Some(if radix == 10 {
        format!("{:e}", text.parse::<f64>().ok()?)
    } else {
        format!("'{text}'")
    })
}

/// A config text with its tagged scalars as yaml.v3 decodes them (`!!binary` values
/// base64-decoded, aliases of them included), merges not yet expanded. The management
/// document reads config text through here and the loader through [`config_value`], so
/// a view or a save candidate sees the tagged values the loader sees.
pub fn yaml_value(text: &str) -> anyhow::Result<Value> {
    let mut value = parse_yaml(text)?;
    timestamp::refuse_wide_offsets(&value)?;
    binary::decode(text, &mut value, None)?;
    Ok(value)
}

/// [`yaml_value`] as Go's typed decode reads it into the config: a plain scalar in a Go
/// string setting is its text (`1.50`, `0x1F`, `true`), where serde_yaml_ng reads a
/// number or boolean. The management views keep [`yaml_value`]'s YAML types, as Go's
/// read the file's nodes.
pub fn config_value(text: &str) -> anyhow::Result<Value> {
    let mut value = parse_yaml(text)?;
    timestamp::refuse_wide_offsets(&value)?;
    binary::decode(text, &mut value, Some(&[]))?;
    Ok(value)
}

/// yaml.v3's error for a merge value that is not a mapping or a list of mappings.
const MERGE_NOT_MAPS: &str = "yaml: map merge requires map or sequence of maps as the value";

/// Go `expandConfigAliases` plus yaml.v3's merge rules: `<<` keys expand bottom-up
/// (nested merges first); explicit keys win, then earlier merged mappings. A merge
/// value that is not a mapping or a list of mappings is yaml.v3's decode error, even in
/// a merge source an explicit key or an earlier source shadows, which yaml.v3 never
/// decodes.
/// (Aliases are already expanded by the parser.) The parsed value no longer says whether
/// a key was quoted or a merge operand an alias, so [`yaml_value`] refuses a quoted
/// `<<` key (yaml.v3 reads it as a plain key) and fails an alias of a sequence as a
/// merge operand. A JSON management body, which is not read through it, keeps `<<`
/// literal until the completed candidate is expanded, as Go's save ends up writing it plain.
pub fn expand_merges(value: &mut Value) -> anyhow::Result<()> {
    match value {
        Value::Mapping(map) => {
            for (_, child) in map.iter_mut() {
                expand_merges(child)?;
            }
            if let Some(merge) = map.shift_remove("<<") {
                let sources: Vec<Mapping> = match untagged(merge) {
                    Value::Mapping(m) => vec![m],
                    Value::Sequence(items) => items
                        .into_iter()
                        .map(|item| match untagged(item) {
                            Value::Mapping(m) => Ok(m),
                            _ => Err(anyhow::anyhow!(MERGE_NOT_MAPS)),
                        })
                        .collect::<anyhow::Result<_>>()?,
                    _ => bail!(MERGE_NOT_MAPS),
                };
                for source in sources {
                    for (key, item) in source {
                        map.entry(key).or_insert(item);
                    }
                }
            }
        }
        Value::Sequence(items) => {
            for item in items {
                expand_merges(item)?;
            }
        }
        Value::Tagged(tagged) => expand_merges(&mut tagged.value)?,
        _ => {}
    }
    Ok(())
}

/// A merge operand without its local tags: yaml.v3 merges a tagged mapping by its kind.
fn untagged(value: Value) -> Value {
    match value {
        Value::Tagged(tagged) => untagged(tagged.value),
        other => other,
    }
}
pub use trusted::{TrustedProxies, go_trim_space};
pub use validate::{catalog_sources as validate_catalog_sources, go_is_abs, parse_duration};

/// The port a fresh volume's first start imports, and the management base URL's
/// fallback. The loader itself leaves an omitted port at 0.
pub const DEFAULT_PORT: u16 = 8317;

#[derive(Clone, PartialEq, Eq)]
pub struct Config {
    /// Bind address. Empty binds all interfaces.
    pub host: String,
    /// 0 when omitted, like Go: the listener then gets an ephemeral port.
    pub port: u16,
    /// Client keys for the proxy API, trimmed and de-duplicated. Empty disables client auth.
    pub api_keys: Vec<String>,
    /// `server.trusted-proxies`, validated. Go applies it at startup only.
    pub trusted_proxies: Vec<String>,
    /// Empty after parsing. A server or CLI command sets it to the credential working
    /// directory of its database.
    pub auth_dir: PathBuf,
    pub routing: RoutingConfig,
    pub management: ManagementConfig,
    /// Presence-preserving v8 view, including fields not yet wired into executors.
    pub document: Value,
    /// `oauth.providers.*` leaf paths present in the source text (Go
    /// `Config.OAuthOnlyFields`, keyed by path). They apply to OAuth credentials only;
    /// see [`Config::for_api_key`].
    pub oauth_only: std::collections::BTreeSet<String>,
    /// Values derived from this snapshot, built on first use ([`Config::derived`]).
    pub derived: Derived,
}

/// A per-snapshot cache of values computed from a [`Config`] (for example parsed payload
/// rules), one per type. Everyone holding the same snapshot shares them. A clone starts
/// empty and the cache never affects equality. Derive from configs that will not be
/// mutated afterwards; the runtime's published snapshots are immutable.
#[derive(Default)]
pub struct Derived(std::sync::Mutex<Vec<std::sync::Arc<dyn std::any::Any + Send + Sync>>>);

impl Clone for Derived {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl PartialEq for Derived {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl Eq for Derived {}

impl Config {
    /// `models.catalog`, `models.codex-catalog` and `models.devin-catalog` (Go
    /// `CatalogSources`); empty when unset.
    pub fn catalog_sources(&self) -> [String; 3] {
        ["catalog", "codex-catalog", "devin-catalog"].map(|name| {
            self.document
                .get("models")
                .and_then(|models| models.get(name))
                .filter(|v| !v.is_null())
                .map(go_string)
                .unwrap_or_default()
        })
    }

    /// The value of type `T` derived from this snapshot, computing it with `init` on first
    /// use. Concurrent first uses may both compute; the first stored value wins.
    pub fn derived<T: std::any::Any + Send + Sync>(&self, init: impl FnOnce(&Config) -> T) -> std::sync::Arc<T> {
        let find = |slots: &[std::sync::Arc<dyn std::any::Any + Send + Sync>]| {
            slots.iter().find_map(|v| v.clone().downcast::<T>().ok())
        };
        let lock = || self.derived.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(found) = find(&lock()) {
            return found;
        }
        let value = std::sync::Arc::new(init(self));
        let mut slots = lock();
        if let Some(found) = find(&slots) {
            return found;
        }
        slots.push(value.clone());
        value
    }
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("routing", &self.routing)
            .finish_non_exhaustive()
    }
}

/// yaml.v3 decodes any scalar into a Go `string` field; serde would reject numbers.
fn de_go_string<'de, D: serde::Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    Ok(go_string(&Value::deserialize(d)?))
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct RoutingConfig {
    #[serde(deserialize_with = "de_go_string")]
    pub strategy: String,
    pub force_model_prefix: bool,
    pub session_affinity: bool,
    #[serde(deserialize_with = "de_go_string")]
    pub session_affinity_ttl: String,
    pub session_affinity_subagents: Option<bool>,
    pub retry: RetryConfig,
    pub cooldown: CooldownConfig,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct RetryConfig {
    pub request_retry: i64,
    pub max_retry_credentials: i64,
    pub max_retry_interval: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct CooldownConfig {
    pub disable_cooling: bool,
    pub save_cooldown_status: bool,
    pub transient_error_cooldown_seconds: i64,
    /// cooker only: the longest quota reset trusted at first, as a duration
    /// (`"1h"`) or seconds; empty means one hour, `0` trusts every reset (Go).
    #[serde(deserialize_with = "de_go_string", skip_serializing_if = "String::is_empty")]
    pub max_trusted_cooldown: String,
}

// Do not derive Debug: the key must never appear in diagnostic output.
#[derive(Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct ManagementConfig {
    pub allow_remote: bool,
    #[serde(deserialize_with = "de_go_string")]
    pub secret_key: String,
    pub disable_control_panel: bool,
    pub disable_auto_update_panel: bool,
    #[serde(deserialize_with = "de_go_string")]
    pub panel_github_repository: String,
}

impl std::fmt::Debug for ManagementConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagementConfig")
            .field("allow_remote", &self.allow_remote)
            .field("disable_control_panel", &self.disable_control_panel)
            .finish_non_exhaustive()
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::parse("{}").expect("empty mapping is valid")
    }
}

/// Which Go entry point [`decode`] follows.
#[derive(Clone, Copy, PartialEq)]
enum Decode {
    /// `LoadConfig`: the credential preflight (`validateCredentialWeightYAML`), then the
    /// typed decode.
    Load,
    /// `ParseConfigBytes`: as `Load`, with typed-decode errors wrapped as
    /// `parse config payload: ...` (Go wraps its `yaml.Unmarshal`).
    Payload,
}

/// The decoding part of [`Config::parse`]: the YAML, its layout and the setting types,
/// before the validators. Its errors name settings, not their values. The layout checks
/// come first, unwrapped, as in Go.
fn decode(text: &str, mode: Decode) -> anyhow::Result<(Mapping, ConfigDocument)> {
    let mut parsed = config_value(text)?;
    // Before the shape checks, as Go expands aliases first.
    expand_merges(&mut parsed)?;
    let root = match parsed {
        Value::Null if text.trim().is_empty() => Mapping::new(),
        Value::Mapping(m) => m,
        _ => bail!("config must be a mapping"),
    };
    validate_shape(&root)?;
    if let Some(e) = document::group_error(&root) {
        bail!(e);
    }
    let mut document = ConfigDocument::from_mapping(root.clone())?;
    document.coerce_typed_scalars();
    schema::validate(document.value(), false)
        .and_then(|()| validate::decoded(document.value()))
        .map_err(|e| {
            if mode == Decode::Payload {
                anyhow::anyhow!("parse config payload: {e}")
            } else {
                e
            }
        })?;
    Ok((root, document))
}

impl Config {
    /// Go `ParseConfigBytes`, as the v8 config writer reports it: [`Config::parse`] with
    /// typed-decode errors wrapped as `parse config payload: ...` (see [`decode`]).
    pub fn parse_payload(text: &str) -> anyhow::Result<Self> {
        Self::parse_as(text, Decode::Payload)
    }

    pub fn parse(text: &str) -> anyhow::Result<Self> {
        Self::parse_as(text, Decode::Load)
    }

    fn parse_as(text: &str, mode: Decode) -> anyhow::Result<Self> {
        let (root, document) = decode(text, mode)?;
        let canonical = document.value().as_mapping().expect("document is a mapping");
        let routing = decode_section::<RoutingConfig>(canonical, "routing")?;
        let mut routing = routing;
        routing.retry.max_retry_credentials = routing.retry.max_retry_credentials.max(0);
        let management = decode_section::<ManagementConfig>(canonical, "management")?;
        let host = string_or_empty(lookup(&root, "server.host"), "server.host")?;
        let port = match lookup(&root, "server.port") {
            None | Some(Value::Null) => 0,
            // yaml.v3 truncates a float into Go's int port.
            Some(v) => go_int(v)
                .and_then(|p| u16::try_from(p).ok())
                .context("server.port must be an integer port number")?,
        };
        let api_keys = match lookup(&root, "access.api-keys") {
            None | Some(Value::Null) => Vec::new(),
            Some(Value::Sequence(seq)) => seq
                .iter()
                .map(|k| {
                    k.as_str()
                        .map(str::to_owned)
                        .context("access.api-keys must be a list of strings")
                })
                .collect::<anyhow::Result<_>>()?,
            Some(_) => bail!("access.api-keys must be a list of strings"),
        };
        let trusted_proxies: Vec<String> = match lookup(canonical, "server.trusted-proxies") {
            Some(Value::Sequence(seq)) => seq.iter().map(go_string).collect(),
            _ => Vec::new(),
        };
        trusted::validate(&trusted_proxies)?;
        validate::go_custom(document.value())?;
        realtime::Realtime::read(document.value().get("realtime"))?;
        let oauth_only = schema::oauth_only_paths(&Value::Mapping(root.clone()));
        Ok(Config {
            host,
            port,
            api_keys: normalize_keys(api_keys),
            trusted_proxies,
            auth_dir: PathBuf::new(),
            routing,
            management,
            document: document.into_value(),
            oauth_only,
            derived: Derived::default(),
        })
    }

    /// `server.max-request-body-bytes`, Go
    /// `SDKConfig.MaxRequestBodyBytes`: the request body limit in bytes. `None` when
    /// omitted or not positive, which disables it (Go's default 0).
    pub fn max_request_body_bytes(&self) -> Option<u64> {
        let value = self.document.get("server")?.get("max-request-body-bytes")?;
        u64::try_from(go_int(value)?).ok().filter(|&n| n > 0)
    }

    /// Go `Config.ForAPIKey`: the view an API-key credential sees, with the v8
    /// OAuth-only provider settings set to their typed zero. Borrowed when there are
    /// none.
    pub fn for_api_key(&self) -> std::borrow::Cow<'_, Config> {
        if self.oauth_only.is_empty() {
            return std::borrow::Cow::Borrowed(self);
        }
        let mut view = self.clone();
        for path in &self.oauth_only {
            schema::zero_at(&mut view.document, path);
        }
        view.oauth_only.clear();
        std::borrow::Cow::Owned(view)
    }
}

/// Go `looksLikeBcrypt`.
pub fn is_bcrypt(key: &str) -> bool {
    looks_like_bcrypt(key.as_bytes())
}

fn looks_like_bcrypt(key: &[u8]) -> bool {
    key.len() > 4 && [b"$2a$", b"$2b$", b"$2y$"].iter().any(|p| key.starts_with(*p))
}

fn decode_section<T: serde::de::DeserializeOwned + Default>(root: &Mapping, name: &str) -> anyhow::Result<T> {
    match lookup(root, name) {
        None | Some(Value::Null) => Ok(T::default()),
        Some(value) => Ok(serde_yaml_ng::from_value(null_scalars_to_zero(value.clone()))?),
    }
}

// Go YAML decoding null into a non-pointer field leaves its zero/default value.
// Omit nulls when decoding the typed runtime view; the document keeps them intact.
fn null_scalars_to_zero(mut value: Value) -> Value {
    if let Value::Mapping(ref mut map) = value {
        map.retain(|_, v| !v.is_null());
        for (_, v) in map {
            *v = null_scalars_to_zero(v.clone());
        }
    }
    value
}

fn validate_shape(root: &Mapping) -> anyhow::Result<()> {
    if let Some(e) = document::shape_error(root) {
        bail!(e);
    }
    if let Some(version) = lookup(root, "config-version")
        && version.as_u64() != Some(8)
    {
        bail!("unsupported config-version (expected 8)");
    }
    Ok(())
}

/// Value at a dotted path, or `None` when any segment is absent or a parent is not a
/// mapping. Present-but-null returns `Some(Value::Null)`.
fn lookup<'a>(root: &'a Mapping, path: &str) -> Option<&'a Value> {
    let mut parts = path.split('.');
    let mut cur = root.get(parts.next()?)?;
    for part in parts {
        cur = cur.as_mapping()?.get(part)?;
    }
    Some(cur)
}

/// A YAML scalar decoded into a Go `string` field (yaml.v3 accepts numbers/bools).
pub fn go_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => String::new(),
    }
}

fn string_or_empty(value: Option<&Value>, name: &str) -> anyhow::Result<String> {
    match value {
        None | Some(Value::Null) => Ok(String::new()),
        Some(Value::String(s)) => Ok(s.clone()),
        // yaml.v3 decodes any scalar into a Go string field as its text.
        Some(v @ (Value::Number(_) | Value::Bool(_))) => Ok(go_string(v)),
        Some(_) => bail!("{name} must be a string"),
    }
}

fn normalize_keys(keys: Vec<String>) -> Vec<String> {
    let mut out: Vec<String> = Vec::with_capacity(keys.len());
    for key in keys {
        let key = key.trim();
        if !key.is_empty() && !out.iter().any(|k| k == key) {
            out.push(key.to_owned());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn present_values_win_even_when_null_or_empty() {
        let cfg = Config::parse(
            "server: {port: 9100, host: null}\naccess: {api-keys: []}\n\
             management: {allow-remote: false, disable-auto-update-panel: true}\n",
        )
        .unwrap();
        assert_eq!((cfg.port, cfg.host.as_str()), (9100, ""));
        assert!(cfg.api_keys.is_empty());
        assert!(!cfg.management.allow_remote && cfg.management.disable_auto_update_panel);
    }

    /// Pre-v8 keys and historical v8 spellings are unknown settings to the strict check
    /// every import and write runs, and a client key list at the root is not the
    /// `api-keys` provider map.
    #[test]
    fn pre_v8_keys_are_rejected() {
        for (yaml, want) in [
            ("port: 9000\n", "unknown setting config.port"),
            ("host: 127.0.0.1\n", "unknown setting config.host"),
            ("auth-dir: /x\n", "unknown setting config.auth-dir"),
            ("oauth: {auth-dir: /x}\n", "unknown setting config.oauth.auth-dir"),
            (
                "remote-management: {allow-remote: true}\n",
                "unknown setting config.remote-management",
            ),
            ("request-retry: 4\n", "unknown setting config.request-retry"),
            (
                "claude-api-key: [{api-key: k}]\n",
                "unknown setting config.claude-api-key",
            ),
            (
                "codex: {disable-codex-cloaking: true}\n",
                "unknown setting config.codex",
            ),
            (
                "oauth: {providers: {codex: {disable-codex-cloaking: true}}}\n",
                "unknown setting config.oauth.providers.codex.disable-codex-cloaking",
            ),
            (
                "oauth: {providers: {xai: {inject-x-search: true}}}\n",
                "unknown setting config.oauth.providers.xai",
            ),
            // Plugins were removed: their section is an ordinary unknown setting.
            (
                "plugins: {configs: {p: {enabled: true}}}\n",
                "unknown setting config.plugins",
            ),
            ("plugins: {}\n", "unknown setting config.plugins"),
        ] {
            let doc = ConfigDocument::parse(yaml).unwrap();
            let err = schema::validate(doc.value(), true).unwrap_err();
            assert_eq!(err.to_string(), want, "{yaml:?}");
        }
        assert_eq!(
            Config::parse("api-keys: [legacy]\n").unwrap_err().to_string(),
            "api-keys must be a mapping"
        );
    }

    /// `worker-threads` is cooker only: a whole-file upload through the Management
    /// API keeps it (Go's v8 upload refuses unknown sections), and its type is checked.
    #[test]
    fn worker_threads_is_a_known_int() {
        let doc = |text: &str| serde_yaml_ng::from_str::<serde_yaml_ng::Value>(text).unwrap();
        validate_config_fields(&doc("config-version: 8\nworker-threads: 3\n"), true).unwrap();
        assert!(validate_config_fields(&doc("worker-threads: many\n"), true).is_err());
        assert!(validate_config_fields(&doc("worker-thread: 3\n"), true).is_err());
    }

    #[test]
    fn client_keys_are_trimmed_and_deduplicated() {
        let cfg =
            Config::parse("server: {host: 127.0.0.1, port: 9000}\naccess: {api-keys: [' a ', a, '', b]}\n").unwrap();
        assert_eq!((cfg.host.as_str(), cfg.port), ("127.0.0.1", 9000));
        assert_eq!(cfg.api_keys, ["a", "b"]);
    }

    #[test]
    fn null_or_scalar_parents_are_rejected() {
        for (yaml, want) in [
            ("access: null\n", "access must be a mapping"),
            ("access: [a]\n", "access must be a mapping"),
            ("server: 5\n", "server must be a mapping"),
            ("oauth:\n  providers: null\n", "oauth.providers must be a mapping"),
            ("config-version: 7\n", "unsupported config-version (expected 8)"),
            ("- a\n", "config must be a mapping"),
        ] {
            assert_eq!(Config::parse(yaml).unwrap_err().to_string(), want, "{yaml:?}");
        }
        assert!(
            Config::parse("routing: null\nconfig-version: 8\n").is_ok(),
            "routing: null means defaults"
        );
    }

    #[test]
    fn image_generation_mode_reads_go_spellings() {
        let mode = |yaml: &str| {
            Config::parse(&format!("multimedia:\n  disable-image-generation: {yaml}\n"))
                .map(|c| c.document["multimedia"]["disable-image-generation"].clone())
                .map_err(|e| e.to_string())
        };
        assert_eq!(mode("' Off '"), Ok(Value::Bool(false)));
        assert_eq!(mode("~"), Ok(Value::Bool(false)));
        assert_eq!(mode("YES"), Ok(Value::Bool(true)));
        assert_eq!(mode("' Chat'"), Ok(Value::from("chat")));
        assert_eq!(
            mode("\"\\u00dc\\u0001\""),
            Err(
                "invalid disable-image-generation value \"\u{fc}\\x01\" (allowed: true, false, chat, passthrough)"
                    .into()
            )
        );
    }

    #[test]
    fn v8_upstream_key_map_is_not_client_keys() {
        let cfg = Config::parse("api-keys:\n  claude:\n    - keys: [{api-key: fake-key}]\n").unwrap();
        assert!(cfg.api_keys.is_empty());
        for yaml in [
            "api-keys: {claude: [{api-key: fake-key}]}",
            "api-keys: {claude: null}",
            "api-keys: {claude: [{keys: null}]}",
            "api-keys: {claude: [{weight: 2, keys: []}]}",
            "api-keys: {claude: [{keys: [{base-url: https://example.invalid}]}]}",
            "api-keys: {claude: [{keys: [{weight: 1000001}]}]}",
        ] {
            assert!(Config::parse(yaml).is_err(), "{yaml}");
        }
        assert!(Config::parse("api-keys: {claude: [{keys: [{weight: 0}]}]}").is_ok());
    }

    #[test]
    fn routing_settings_decode_into_one_typed_view() {
        let cfg = Config::parse(
            "routing:\n  strategy: fill-first\n  force-model-prefix: true\n  session-affinity: true\n\
             \x20 session-affinity-ttl: 37m\n  session-affinity-subagents: false\n\
             \x20 retry: {request-retry: 0, max-retry-credentials: -2, max-retry-interval: 41}\n\
             \x20 cooldown: {disable-cooling: false, save-cooldown-status: true, transient-error-cooldown-seconds: -3}\n",
        )
        .unwrap();
        assert_eq!(cfg.routing.strategy, "fill-first");
        assert!(cfg.routing.force_model_prefix && cfg.routing.session_affinity);
        assert_eq!(cfg.routing.session_affinity_ttl, "37m");
        assert_eq!(cfg.routing.session_affinity_subagents, Some(false));
        assert_eq!(cfg.routing.retry.request_retry, 0);
        assert_eq!(cfg.routing.retry.max_retry_credentials, 0);
        assert_eq!(cfg.routing.retry.max_retry_interval, 41);
        assert!(!cfg.routing.cooldown.disable_cooling);
        assert!(cfg.routing.cooldown.save_cooldown_status);
        assert_eq!(cfg.routing.cooldown.transient_error_cooldown_seconds, -3);
        // cooker's own cooldown key: an int or a duration, kept as written, and
        // part of the schema so management writes do not archive it.
        for (yaml, expect) in [("0", "0"), ("90m", "90m")] {
            let text = format!("routing:\n  cooldown:\n    max-trusted-cooldown: {yaml}\n");
            let cfg = Config::parse(&text).unwrap();
            assert_eq!(cfg.routing.cooldown.max_trusted_cooldown, expect);
        }
        assert!(Config::parse("routing: {cooldown: {max-trusted-cooldown: [1]}}").is_err());
        let empty = Config::default();
        assert_eq!(empty.routing, RoutingConfig::default());
        assert_eq!(empty.routing.session_affinity_subagents, None);
        let null = Config::parse("routing: {retry: {request-retry: null}}\n").unwrap();
        assert_eq!(null.routing.retry.request_retry, 0);
    }

    /// Go `shared_upstream_test.go` (52d5507d): the shared `upstream` settings load the
    /// same for OAuth and API-key views, and the Codex header defaults stay OAuth-only.
    #[test]
    fn shared_upstream_settings_reach_both_views() {
        let keys = "api-keys: {codex: [{base-url: https://example.invalid, keys: [{api-key: test-key, disable-codex-cloaking: false}]}]}\n";
        let codex = "{disable-codex-cloaking: true, stream-bootstrap-buffering: true, stream-bootstrap-timeout: 10s, orphan-delegation-compatibility: true, model-level-cooling: true, response-steering: true";
        let headers = "{user-agent: client-agent, package-version: 1.2.3, runtime-version: v22.1.0, os: Linux, arch: arm64, timeout: '123', timezone: Asia/Shanghai, stabilize-device-profile: true}";
        let oauth = "oauth: {auth-auto-refresh-workers: 3, providers: {codex: {header-defaults: {user-agent: oauth-agent, beta-features: oauth-beta}}}}\n";
        let raw = format!(
            "upstream:\n  codex: {codex}}}\n  claude:\n    model-level-cooling: true\n    disable-cloaking-model-list: true\n    disable-claude-cloak-mode: true\n    header-defaults: {headers}\n  xai: {{inject-x-search: true}}\n{oauth}{keys}"
        );
        let doc = ConfigDocument::parse(&raw).unwrap();
        schema::validate(doc.value(), true).unwrap();
        let expected = Config::parse(&raw).unwrap().document;
        assert_eq!(
            expected["upstream"]["claude"]["header-defaults"]["timezone"],
            "Asia/Shanghai"
        );
        for text in [raw.clone(), yaml_string(doc.value()).unwrap()] {
            let cfg = Config::parse(&text).unwrap();
            for view in [&cfg, &*cfg.for_api_key()] {
                assert_eq!(view.document["upstream"], expected["upstream"], "{text}");
                assert_eq!(view.document["oauth"]["auth-auto-refresh-workers"], 3);
                let key = &view.document["api-keys"]["codex"][0]["keys"][0];
                assert_eq!(key["disable-codex-cloaking"], Value::Bool(false));
            }
            let user_agent =
                |c: &Config| c.document["oauth"]["providers"]["codex"]["header-defaults"]["user-agent"].clone();
            assert_eq!(user_agent(&cfg), "oauth-agent");
            assert_eq!(user_agent(&cfg.for_api_key()), "");
        }
    }

    #[test]
    fn defaults_match_go_loader() {
        let cfg = Config::parse("").unwrap();
        assert_eq!((cfg.port, cfg.host.as_str()), (0, ""));
        assert_eq!(cfg.auth_dir, PathBuf::new());
    }

    /// Plain numbers that are no model name, alias or exclusion cost one step each: 16
    /// times the entries must cost far less than 256 times the time (a search from the
    /// root per number). Both sizes run in this process, so the ratio does not depend on
    /// the machine.
    #[test]
    fn yaml_literals_scale_linearly() {
        let time = |n: usize| {
            let params: Vec<String> = (0..n).map(|i| format!("k{i}: 1.50")).collect();
            let text = format!(
                "x: &a 1\nrequests: {{payload: {{override: [{{models: [{{name: 0x1F}}], params: {{{}}}}}]}}}}\n",
                params.join(", ")
            );
            (0..3)
                .map(|_| {
                    let start = std::time::Instant::now();
                    let doc = config_value(&text).unwrap();
                    let rule = &doc["requests"]["payload"]["override"][0];
                    assert_eq!(rule["models"][0]["name"], Value::from("0x1F"));
                    assert_eq!(rule["params"][format!("k{}", n - 1).as_str()], Value::from(1.5));
                    start.elapsed()
                })
                .min()
                .unwrap()
        };
        let (small, large) = (time(1000), time(16000));
        let ratio = large.as_secs_f64() / small.as_secs_f64().max(1e-9);
        assert!(
            ratio < 64.0,
            "16x the entries took {ratio:.0}x the time ({small:?} to {large:?})"
        );
    }

    /// The aliases of an anchored list of numbers count toward the alias limit only where
    /// they land in a model name, alias or exclusion. (yaml.v3 refuses this many aliases
    /// as excessive aliasing; the limit here is the copies they make.)
    #[test]
    fn unused_number_aliases_do_not_expand_too_far() {
        let numbers: Vec<String> = (0..1024).map(|i| i.to_string()).collect();
        let text = format!(
            "a: &l [{}]\nb: [{}]\noauth: {{excluded-models: {{c: *l}}}}\n",
            numbers.join(", "),
            vec!["*l"; 1024].join(", ")
        );
        let doc = config_value(&text).unwrap();
        assert_eq!(doc["b"][1023][1023], Value::from(1023));
        assert_eq!(doc["oauth"]["excluded-models"]["c"][7], Value::from("7"));
    }

    /// Aliases whose copies no string setting keeps are bounded too: 2,048 aliases of a
    /// 2,049-number anchor outside any string setting visit just over the limit and are
    /// refused, where each would otherwise copy the whole anchor. Go refuses such a file
    /// as excessive aliasing.
    #[test]
    fn dropped_alias_copies_are_bounded() {
        let numbers = vec!["0"; 2049].join(", ");
        let text = format!("x: &x [{numbers}]\nzz-ignored: [{}]\n", vec!["*x"; 2048].join(", "));
        let err = config_value(&text).unwrap_err();
        assert_eq!(err.to_string(), "config YAML aliases expand too far");
    }

    /// Only alias copies count toward the alias limit: a list of more numbers than that,
    /// anchored (so kept for its aliases) or an exclusion list, loads as in Go.
    #[test]
    fn written_numbers_do_not_count_toward_the_alias_limit() {
        let numbers = vec!["0"; (1 << 20) + 1].join(", ");
        let doc = config_value(&format!(
            "x: &l [{numbers}]
oauth: {{excluded-models: {{c: [{numbers}]}}}}
"
        ))
        .unwrap();
        assert_eq!(doc["x"][1 << 20], Value::from(0));
        assert_eq!(doc["oauth"]["excluded-models"]["c"][1 << 20], Value::from("0"));
    }

    /// yaml.v3 (v8.0.35) reads a plain integer beyond int64 and uint64 as a float when it
    /// is decimal and as text otherwise, and a string setting as its text; serde_yaml_ng
    /// refused one that fits 128 bits, failing the load.
    #[test]
    fn integers_beyond_64_bits_read_as_in_go() {
        let token = "123456789012345678901234567890";
        let cfg = Config::parse(&format!("server:\n  github-token: {token}\n")).unwrap();
        assert_eq!(cfg.document["server"]["github-token"].as_str(), Some(token));
        let any = yaml_value("x: [18446744073709551616, -9223372036854775809, 0x1FFFFFFFFFFFFFFFFF]\n").unwrap();
        let want: Vec<Value> = vec![
            1.8446744073709552e19.into(),
            (-9.223372036854776e18).into(),
            "0x1FFFFFFFFFFFFFFFFF".into(),
        ];
        assert_eq!(any["x"].as_sequence().unwrap(), &want);
        // Any number of them, read in one pass; marks after a BOM still line up.
        let many = yaml_value(&format!("x: [{}]\n", vec!["-18446744073709551616"; 100].join(", "))).unwrap();
        assert!(
            many["x"]
                .as_sequence()
                .unwrap()
                .iter()
                .all(|v| v.as_f64() == Some(-1.8446744073709552e19))
        );
        // An anchored one keeps its anchor, on its line or the line above.
        for text in [
            "a: &n 18446744073709551616\nb: *n\n",
            "a: &n\n  18446744073709551616\nb: *n\n",
        ] {
            let anchored = yaml_value(text).unwrap();
            assert_eq!(anchored["b"].as_f64(), Some(1.8446744073709552e19), "{text}");
        }
        // Integers serde_yaml_ng reads are read the same beside one it refuses: past
        // f64's range the text, as in Go, and a decimal with a leading zero.
        let huge = format!("1{}", "0".repeat(400));
        for other in [huge.as_str(), "01000000000000000000000"] {
            let both = yaml_value(&format!("x: [18446744073709551616, {other}]\n")).unwrap();
            assert_eq!(
                both["x"][1],
                yaml_value(&format!("x: {other}\n")).unwrap()["x"],
                "{other}"
            );
        }
        assert_eq!(
            yaml_value(&format!("x: {huge}\n")).unwrap()["x"].as_str(),
            Some(huge.as_str())
        );
        let bom = yaml_value("\u{feff}a: [0x1FFFFFFFFFFFFFFFFF, x]\n").unwrap();
        assert_eq!(bom["a"], yaml_value("['0x1FFFFFFFFFFFFFFFFF', x]").unwrap());
        // Go: cannot unmarshal !!float into int.
        assert!(Config::parse("routing: {retry: {request-retry: 18446744073709551616}}\n").is_err());
    }

    /// yaml.v3 (v3.0.1, Go 1.26.4) drops a plain number's underscores before resolving
    /// it and reads a leading zero as octal (or decimal, failing that); serde_yaml_ng
    /// read each of these as text.
    #[test]
    fn underscore_numbers_read_as_in_go() {
        let go: [(&str, Value); 20] = [
            ("1_000", 1000.into()),
            ("+_1", 1.into()),
            ("-1_000", (-1000).into()),
            ("1__0", 10.into()),
            ("1_", 1.into()),
            ("0_x1F", 31.into()),
            ("-0x1_F", (-31).into()),
            ("007_1", 57.into()),
            ("-0b1_0", (-2).into()),
            ("18_446_744_073_709_551_615", u64::MAX.into()),
            ("1_0.5", 10.5.into()),
            ("1_0e1_0", 1e11.into()),
            ("1._5", 1.5.into()),
            (".5_0", 0.5.into()),
            ("-.5_0", (-0.5).into()),
            ("1_000_000_000_000_000_000_000", 1e21.into()),
            ("_1", "_1".into()),
            (".5_", ".5_".into()),
            ("._5", "._5".into()),
            ("0x1_FFFF_FFFF_FFFF_FFFF_FF", "0x1_FFFF_FFFF_FFFF_FFFF_FF".into()),
        ];
        let leading_zero: [(&str, Value); 10] = [
            ("007", 7.into()),
            ("-007", (-7).into()),
            ("00", 0.into()),
            ("08", 8.0.into()),
            ("09.5", 9.5.into()),
            ("07e1", 70.0.into()),
            ("01000000000000000000000", 9223372036854775808u64.into()),
            ("01777777777777777777777", u64::MAX.into()),
            ("02000000000000000000000", 2e21.into()),
            ("0111111111111111111111111", 1.1111111111111111e23.into()),
        ];
        // Go's last resort reads a sign after `0b` or `0o`.
        let signed: [(&str, Value); 7] = [
            ("0b_-1", (-1).into()),
            ("0o_+17", 15.into()),
            ("0o_-7", (-7).into()),
            ("0b-1", (-1).into()),
            ("0o+17", 15.into()),
            ("+0b-1", "+0b-1".into()),
            (
                "0b+1111111111111111111111111111111111111111111111111111111111111111",
                "0b+1111111111111111111111111111111111111111111111111111111111111111".into(),
            ),
        ];
        // Go's text: beyond 64 bits, or a `+` past int64.
        let zeros = "0".repeat(64);
        let wide = [
            "0o2000000000000000000000".to_owned(),
            "+0o2000000000000000000000".to_owned(),
            format!("-0b1{zeros}"),
            "+0o1000000000000000000000".to_owned(),
            "+0b1_000000000000000000000000000000000000000000000000000000000000000".to_owned(),
        ];
        let wide: Vec<(&str, Value)> = wide.iter().map(|t| (t.as_str(), Value::from(t.as_str()))).collect();
        // Upper-case prefixes, which Go's ParseInt takes, and a `+` past int64, which its
        // ParseUint does not, so a decimal one is a float and any other text.
        let prefixes: [(&str, Value); 16] = [
            ("0X1F", 31.into()),
            ("-0X1F", (-31).into()),
            ("+0X1F", 31.into()),
            ("0O17", 15.into()),
            ("0B10", 2.into()),
            ("-0B10", (-2).into()),
            ("0X", "0X".into()),
            ("0O8", "0O8".into()),
            ("0B2", "0B2".into()),
            ("0X8000000000000000", (1u64 << 63).into()),
            ("0XFFFFFFFFFFFFFFFF", u64::MAX.into()),
            ("0X10000000000000000", "0X10000000000000000".into()),
            ("+9223372036854775807", i64::MAX.into()),
            ("+9223372036854775808", 9223372036854775808f64.into()),
            ("+18446744073709551615", 18446744073709551615f64.into()),
            ("+0x8000000000000000", "+0x8000000000000000".into()),
        ];
        let go = go
            .into_iter()
            .chain(leading_zero)
            .chain(signed)
            .chain(wide)
            .chain(prefixes)
            .collect::<Vec<_>>();
        let list = go.iter().map(|(text, _)| *text).collect::<Vec<_>>().join(", ");
        let any = yaml_value(&format!("x: [{list}]\n")).unwrap();
        for ((text, want), got) in go.iter().zip(any["x"].as_sequence().unwrap()) {
            assert_eq!(got, want, "{text}");
        }
        // Without an underscore in the file too.
        assert_eq!(yaml_value("x: 007\n").unwrap()["x"], Value::from(7));
        assert_eq!(yaml_value("x: 0X1F\n").unwrap()["x"], Value::from(31));
        assert_eq!(
            yaml_value("x: +9223372036854775808\n").unwrap()["x"],
            Value::from(9223372036854775808f64)
        );
        assert_eq!(
            yaml_value("x: 9223372036854775808\n").unwrap()["x"],
            Value::from(1u64 << 63)
        );
        let cfg = Config::parse("server: {github-token: 0X1F}\nrouting: {retry: {request-retry: 0X1F}}\n").unwrap();
        assert_eq!(cfg.document["server"]["github-token"].as_str(), Some("0X1F"));
        assert_eq!(cfg.routing.retry.request_retry, 31);
        assert_eq!(yaml_value("007").unwrap(), Value::from(7));
        // A string setting keeps the text, an int setting gets the number, and a key
        // stays text, as yaml.v3 reads it into a string-keyed map.
        let cfg = Config::parse(
            "server: {github-token: 1_000}\nrouting: {retry: {request-retry: 1_0}}\n\
             oauth: {excluded-models: {1_0: [a], 007: [b]}}\n",
        )
        .unwrap();
        assert_eq!(cfg.document["server"]["github-token"].as_str(), Some("1_000"));
        assert_eq!(cfg.routing.retry.request_retry, 10);
        assert!(cfg.document["oauth"]["excluded-models"].get("1_0").is_some());
        assert!(cfg.document["oauth"]["excluded-models"].get("007").is_some());
        let key = yaml_value(&format!("0o2000000000000000000000: a\n-0b1{zeros}: b\n")).unwrap();
        assert_eq!(key["0o2000000000000000000000"], Value::from("a"));
        assert_eq!(key[format!("-0b1{zeros}").as_str()], Value::from("b"));
    }

    /// yaml.v3 reads a plain timestamp in an untyped value as a `time.Time`, which Go
    /// marshals as RFC 3339 text (recorded with yaml.v3 v3.0.1 and Go 1.26.4); a string
    /// setting and a mapping key keep the text.
    #[test]
    fn plain_timestamps_read_as_in_go() {
        let go = [
            ("2001-12-14", Some("2001-12-14T00:00:00Z")),
            ("2001-1-2", Some("2001-01-02T00:00:00Z")),
            ("2001-12-14 21:59:43.10", Some("2001-12-14T21:59:43.1Z")),
            ("2001-12-14T21:59:43.10-05:00", Some("2001-12-14T21:59:43.1-05:00")),
            ("2001-12-14t21:59:43,5+00:60", Some("2001-12-14T21:59:43.5+01:00")),
            (
                "2001-12-14T21:59:43.123456789123Z",
                Some("2001-12-14T21:59:43.123456789Z"),
            ),
            ("2001-12-14T21:59:43-00:00", Some("2001-12-14T21:59:43Z")),
            ("2004-2-29   1:2:3", Some("2004-02-29T01:02:03Z")),
            ("0001-01-01", Some("0001-01-01T00:00:00Z")),
            ("2001-12-14 21:59:43.10 -5", None),
            ("2001-12-14x", None),
            ("2001-02-29", None),
            ("2001-12-14t21:59:43.10z", None),
            ("2001-12-14 21:59:43.10Z", None),
            ("2001-12-14T24:00:00Z", None),
            ("2001-12-14 \t21:59:43", None),
        ];
        // A block list: a flow one would split `43,5` at the comma.
        let list: String = go.iter().map(|(text, _)| format!("  - {text}\n")).collect();
        let any = yaml_value(&format!("x:\n{list}  - '2001-12-14'\n")).unwrap();
        let json = serde_json::to_value(AsJson(&any)).unwrap();
        for (i, (text, want)) in go.iter().enumerate() {
            let tagged = matches!(&any["x"][i], Value::Tagged(t) if t.tag == TIMESTAMP_TAG);
            assert_eq!(tagged, want.is_some(), "{text}");
            assert_eq!(json["x"][i], want.unwrap_or(text), "{text}");
        }
        assert_eq!(any["x"][go.len()], Value::from("2001-12-14"));
        // Written back plain, as yaml.v3 writes a `time.Time`, and read back the same.
        let text = yaml_string(&any).unwrap();
        assert!(text.contains("\n- 2001-12-14T00:00:00Z\n"), "{text}");
        assert_eq!(yaml_value(&text).unwrap(), any, "{text}");
        let cfg =
            Config::parse("server: {github-token: 2001-12-14}\noauth: {excluded-models: {2001-12-14: [a]}}\n").unwrap();
        assert_eq!(cfg.document["server"]["github-token"].as_str(), Some("2001-12-14"));
        assert!(cfg.document["oauth"]["excluded-models"].get("2001-12-14").is_some());
        // Folded over lines, under a `%TAG !` directive, and inside a local tag, which
        // Go's `any` prints as its value (all recorded with yaml.v3).
        for (text, want) in [
            (
                "x: 2001-12-14\n  21:59:43\n",
                serde_json::json!({"x": "2001-12-14T21:59:43Z"}),
            ),
            (
                "x: [2001-12-14\n  21:59:43]\n",
                serde_json::json!({"x": ["2001-12-14T21:59:43Z"]}),
            ),
            // Anchored too, with the anchor on the scalar's line or its own.
            (
                "x: &a 2001-12-14\n  21:59:43\ny: *a\n",
                serde_json::json!({"x": "2001-12-14T21:59:43Z", "y": "2001-12-14T21:59:43Z"}),
            ),
            (
                "x: &a\n  2001-12-14\n  21:59:43\ny: *a\n",
                serde_json::json!({"x": "2001-12-14T21:59:43Z", "y": "2001-12-14T21:59:43Z"}),
            ),
            (
                "x: &a # c\n  # d\n  2001-12-14\n  21:59:43\ny: *a\n",
                serde_json::json!({"x": "2001-12-14T21:59:43Z", "y": "2001-12-14T21:59:43Z"}),
            ),
            (
                "x: &a\u{2028}  2001-12-14\n  21:59:43\ny: *a\n",
                serde_json::json!({"x": "2001-12-14T21:59:43Z", "y": "2001-12-14T21:59:43Z"}),
            ),
            (
                "x: &a\n  # c\n  2001-12-14\n  21:59:43\ny: *a\n",
                serde_json::json!({"x": "2001-12-14T21:59:43Z", "y": "2001-12-14T21:59:43Z"}),
            ),
            (
                "x: &a\u{2029}  2001-12-14\n  21:59:43\ny: *a\n",
                serde_json::json!({"x": "2001-12-14T21:59:43Z", "y": "2001-12-14T21:59:43Z"}),
            ),
            (
                "x: &a\u{85}  2001-12-14\n  21:59:43\ny: *a\n",
                serde_json::json!({"x": "2001-12-14T21:59:43Z", "y": "2001-12-14T21:59:43Z"}),
            ),
            (
                "x: [&a 2001-12-14\n  21:59:43, *a]\n",
                serde_json::json!({"x": ["2001-12-14T21:59:43Z", "2001-12-14T21:59:43Z"]}),
            ),
            (
                "%TAG ! tag:example.com,2000:\n---\nx: 2001-12-14\n",
                serde_json::json!({"x": "2001-12-14T00:00:00Z"}),
            ),
            (
                "x: !y {at: 2001-12-14}\n",
                serde_json::json!({"x": {"at": "2001-12-14T00:00:00Z"}}),
            ),
        ] {
            let json = serde_json::to_value(AsJson(&yaml_value(text).unwrap())).unwrap();
            assert_eq!(json, want, "{text}");
        }
        let directive = yaml_value("%TAG ! tag:example.com,2000:\n---\nx: 2001-12-14\n").unwrap();
        assert!(
            matches!(&directive["x"], Value::Tagged(t) if t.tag == TIMESTAMP_TAG),
            "{directive:?}"
        );
        // A string key that reads as a timestamp is quoted, as Go's Marshal quotes it.
        let key = yaml_string(&yaml_value("{'2001-12-14': a}").unwrap()).unwrap();
        assert_eq!(key, "\"2001-12-14\": a\n");
        // Go cannot marshal an offset of 24 hours or more as JSON. The config decode
        // refuses it; a reader of other settings, such as discovery's, still parses.
        let wide = "x: 2001-12-14T21:59:43+24:00\n";
        let err = yaml_value(wide).unwrap_err();
        assert!(format!("{err:#}").contains("24 hours"), "{err:#}");
        assert!(config_value(wide).is_err() && parse_yaml(wide).is_ok());
    }

    /// yaml.v3 skips a leading BOM; serde_yaml_ng and libyaml read it as part of the
    /// first line, so a second top-level key failed the load.
    #[test]
    fn a_leading_bom_is_skipped() {
        let cfg = Config::parse(
            "\u{feff}server: {github-token: !!binary dG9r}\nrouting: {retry: {request-retry: 3}}\n\
             x: [18446744073709551616, 007]\n",
        )
        .unwrap();
        assert_eq!(cfg.document["server"]["github-token"].as_str(), Some("tok"));
        assert_eq!(cfg.routing.retry.request_retry, 3);
        let x = yaml_value("\u{feff}a: b\nx: [18446744073709551616, 007]\n").unwrap();
        assert_eq!(x["x"], yaml_value("[1.8446744073709552e19, 7]").unwrap());
    }

    /// A string that would read back as a number when written plain is double-quoted
    /// where a write re-emits it, as yaml.v3 quotes it, so it stays that string.
    #[test]
    fn re_emitted_strings_read_back_as_strings() {
        let strings = [
            "1_000",
            "0_7",
            "0_8",
            "1_0.5",
            "0x_1F",
            ".5_0",
            "007",
            "0b-1",
            "0o+17",
            "0X1F",
            "0O17",
            "+9223372036854775808",
            "18446744073709551616",
            "-18446744073709551616",
            "2001-12-14",
            "2001-12-14 21:59:43.10",
        ];
        let value = Value::Sequence(strings.iter().map(|s| Value::from(*s)).collect());
        let text = yaml_string(&value).unwrap();
        assert_eq!(parse_yaml(&text).unwrap(), value, "{text}");
        // String keys too, which Go would otherwise read as numbers.
        let keys = yaml_value("{'1_000': a, '007': b, 7: c}").unwrap();
        let text = yaml_string(&keys).unwrap();
        assert_eq!(text, "\"1_000\": a\n'007': b\n7: c\n");
        assert_eq!(parse_yaml(&text).unwrap(), keys, "{text}");
        assert_eq!(
            yaml_string(&yaml_value("[7, 1000, 0.5]").unwrap()).unwrap(),
            "- 7\n- 1000\n- 0.5\n"
        );
    }

    /// yaml.v3 reads a plain scalar into any Go string setting as its text, so a setting
    /// written as a number or boolean (`0x1F`, `1.50`, `007`, `true`) keeps it, in the v8
    /// and legacy layouts and through an anchored key (tests/reference/scalar_settings).
    #[test]
    fn scalar_settings_keep_their_yaml_text() {
        let go: serde_json::Value =
            serde_json::from_str(include_str!("../tests/fixtures/scalar_settings_go.json")).unwrap();
        for case in go["cases"].as_array().unwrap() {
            let cfg = Config::parse(case["yaml"].as_str().unwrap()).unwrap();
            for check in case["checks"].as_array().unwrap() {
                let path = check["rust"].as_str().unwrap();
                // The recording predates the removal of `oauth.auth-dir` and of plugins.
                if path == "oauth.auth-dir" || path.starts_with("plugins.") {
                    continue;
                }
                let mut node = &cfg.document;
                for part in path.split('.') {
                    node = match part.parse::<usize>() {
                        Ok(i) => &node[i],
                        Err(_) => &node[part],
                    };
                }
                assert_eq!(node.as_str(), check["value"].as_str(), "{}: {path}", case["name"]);
            }
        }
        // A model with a display name written as a number registers under its prefix,
        // both as Go reads them.
        let cfg = Config::parse(go["cases"][0]["yaml"].as_str().unwrap()).unwrap();
        let aliases = crate::registry::dynamic::global_aliases(&cfg);
        let models: Vec<(String, String)> = credentials::from_config(&cfg)
            .iter()
            .flat_map(|c| crate::registry::dynamic::models_for(&cfg, &aliases, c, 0))
            .map(|m| (m.id, m.display_name))
            .collect();
        assert!(models.contains(&("0x1F/a".into(), "1.50".into())), "{models:?}");
    }
}
