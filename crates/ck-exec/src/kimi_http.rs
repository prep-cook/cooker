//! Helpers shared by the Kimi, Meta and Devin executors: Go runtime facts, per-credential
//! custom headers and Go's credential expiry rules. Clients and Go net/http wire
//! behaviour live in crate::proxy.
//!
//! ponytail: provider-neutral (M4-0030 custom headers, M4-0027 refresh timing); hoist
//! when the shared executor helpers land. TLS is wreq's default profile, not Go
//! crypto/tls; these upstreams are not known to fingerprint ClientHello.

use chrono::{DateTime, SecondsFormat, TimeZone, Utc};
use ck_core::credential::Credential;
use ck_core::exec::{ExecError, FailureScope};
use serde_json::Value;

/// Go `runtime.GOOS`.
pub(crate) fn go_os() -> &'static str {
    match std::env::consts::OS {
        "macos" => "darwin",
        other => other,
    }
}

/// Go `runtime.GOARCH`.
pub(crate) fn go_arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        "x86" => "386",
        "powerpc64" => "ppc64",
        other => other,
    }
}

/// Go `os.Hostname()`, or `None` when it fails.
pub(crate) fn hostname() -> Option<String> {
    #[cfg(target_os = "linux")]
    if let Ok(name) = std::fs::read_to_string("/proc/sys/kernel/hostname") {
        let name = name.trim_end_matches('\n');
        if !name.is_empty() {
            return Some(name.to_owned());
        }
    }
    platform_hostname()
}

#[cfg(unix)]
fn platform_hostname() -> Option<String> {
    let mut buf = [0u8; 256];
    // SAFETY: gethostname writes at most buf.len() bytes into the provided buffer.
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) };
    if rc != 0 {
        return None;
    }
    let end = buf.iter().position(|b| *b == 0).unwrap_or(buf.len());
    Some(String::from_utf8_lossy(&buf[..end]).into_owned())
}

/// Go's Windows `os.Hostname`: `GetComputerNameExW(ComputerNamePhysicalDnsHostname)`,
/// growing the buffer while the call answers `ERROR_MORE_DATA` with a larger size.
#[cfg(windows)]
fn platform_hostname() -> Option<String> {
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetComputerNameExW(name_type: i32, buffer: *mut u16, size: *mut u32) -> i32;
        fn GetLastError() -> u32;
    }
    const COMPUTER_NAME_PHYSICAL_DNS_HOSTNAME: i32 = 5;
    const ERROR_MORE_DATA: u32 = 234;
    let mut n: u32 = 64;
    loop {
        let mut buf = vec![0u16; n as usize];
        // SAFETY: `buf` holds `n` UTF-16 units; the call writes at most that many and
        // stores the written (or required) length in `n`.
        let ok = unsafe { GetComputerNameExW(COMPUTER_NAME_PHYSICAL_DNS_HOSTNAME, buf.as_mut_ptr(), &mut n) };
        if ok != 0 {
            buf.truncate(n as usize);
            // syscall.UTF16ToString: up to the first NUL, invalid surrogates as U+FFFD.
            let end = buf.iter().position(|&u| u == 0).unwrap_or(buf.len());
            return Some(String::from_utf16_lossy(&buf[..end]));
        }
        // SAFETY: no preconditions.
        if unsafe { GetLastError() } != ERROR_MORE_DATA || n as usize <= buf.len() {
            return None;
        }
    }
}

#[cfg(not(any(unix, windows)))]
fn platform_hostname() -> Option<String> {
    None
}

/// Go `ApplyPayloadConfigWithRequest` (no target executor) for the device providers:
/// `requests.payload` rules for `model`, `protocol` the target format, `original` the
/// client's original request translated to that target.
// ponytail: the rules are parsed per request (the runtime has no per-snapshot slot), and
// ExecRequest has no inbound route path, so path-gated image-generation rules see "".
pub(crate) fn payload_rules(
    cfg: &ck_core::config::Config,
    req: &ck_core::exec::ExecRequest,
    model: &str,
    protocol: &str,
    body: Vec<u8>,
    original: &[u8],
) -> Vec<u8> {
    use ck_wire::payload;
    let rules = payload::Rules::from_config(cfg);
    // PayloadRequestedModel: the client's model, else req.Model.
    let requested = match req.requested_model.trim() {
        "" => req.model.trim(),
        requested => requested,
    };
    payload::apply(
        &rules,
        &payload::Request {
            target_executor: "",
            model,
            requested_model: requested,
            protocol,
            from_protocol: req.source_format.as_str(),
            root: "",
            original,
            request_path: &req.request_path,
            headers: Some(&req.headers),
        },
        body,
    )
}

/// `util.ApplyCustomHeadersFromAttrs` through ck_wire::headers, with Go's
/// `$CPA-SESSION-ID`: the canonical session bound to the request (`req.session`).
pub(crate) fn credential_headers(
    credential: &Credential,
    req: &ck_core::exec::ExecRequest,
    _original: &[u8],
) -> Vec<(String, String)> {
    client_headers(credential, &req.headers, req.session.as_deref())
}

/// [`credential_headers`] from the client's request headers and canonical session.
pub(crate) fn client_headers(
    credential: &Credential,
    client: &http::HeaderMap,
    session: Option<&str>,
) -> Vec<(String, String)> {
    let session = ck_wire::session::cpa_session_id(session);
    ck_wire::headers::custom_headers(&credential.attributes, client, session.as_deref())
}

/// Version reported where Go sends `buildinfo.Version`.
pub(crate) const BUILD_VERSION: &str = env!("CARGO_PKG_VERSION");

/// `statusErr{code, msg: body}`. Go's Kimi errors carry no retry hint (Retry-After is not
/// read) and are not credential-scoped, so a 429 cools only the model: the scheduler
/// reserves credential-wide quota cooldowns for credential-scoped 429s.
pub(crate) async fn status_error(upstream: crate::proxy::Upstream) -> ExecError {
    let body = crate::proxy::read_all(upstream.body, crate::proxy::MAX_ERROR_BODY, true)
        .await
        .unwrap_or_default();
    let scope = match upstream.status {
        429 => FailureScope::Model,
        401 | 402 | 403 | 408 | 500.. => FailureScope::Credential,
        _ => FailureScope::Request,
    };
    ExecError {
        status: upstream.status,
        scope,
        body,
        headers: Box::new(upstream.headers),
        retry_after: None,
        direct: false,
        addon: None,
        upstream: true,
    }
}

/// Go `io.ReadAll` for a body whose read failure must surface: the first `keep` bytes are
/// kept and the rest is drained, so a read error anywhere in the body is returned.
// ponytail: Go keeps the whole body; bytes past `keep` are discarded to bound memory.
// Provider-neutral; hoist into crate::proxy if others need it.
pub(crate) async fn read_all_strict(
    mut body: futures_util::stream::BoxStream<'static, Result<bytes::Bytes, ExecError>>,
    keep: usize,
) -> Result<bytes::Bytes, ExecError> {
    use futures_util::StreamExt;
    let mut out = bytes::BytesMut::new();
    while let Some(chunk) = body.next().await {
        let chunk = chunk?;
        let room = keep.saturating_sub(out.len());
        out.extend_from_slice(&chunk[..chunk.len().min(room)]);
    }
    Ok(out.freeze())
}

/// Go `time.Time.Format(time.RFC3339)` in UTC.
pub(crate) fn rfc3339_utc(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// Go `time.Now().Format(time.RFC3339)`: local zone, `Z` only at UTC.
pub(crate) fn rfc3339_local_now() -> String {
    chrono::Local::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn normalise_unix(raw: i64) -> Option<DateTime<Utc>> {
    if raw <= 0 {
        return None;
    }
    if raw > 1_000_000_000_000 {
        Utc.timestamp_millis_opt(raw).single()
    } else {
        Utc.timestamp_opt(raw, 0).single()
    }
}

/// Go `parseTimeValue`.
pub(crate) fn parse_time(value: &Value) -> Option<DateTime<Utc>> {
    match value {
        Value::String(s) => {
            let s = s.trim();
            if s.is_empty() {
                return None;
            }
            if let Ok(t) = DateTime::parse_from_rfc3339(s) {
                return Some(t.with_timezone(&Utc));
            }
            for layout in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"] {
                if let Ok(t) = chrono::NaiveDateTime::parse_from_str(s, layout) {
                    return Some(t.and_utc());
                }
            }
            s.parse::<i64>().ok().and_then(normalise_unix)
        }
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_f64().map(|f| f as i64))
            .and_then(normalise_unix),
        _ => None,
    }
}

fn jwt_exp(token: &str) -> Option<DateTime<Utc>> {
    use base64::Engine;
    let parts: Vec<&str> = token.trim().split('.').collect();
    if parts.len() != 3 {
        return None;
    }
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(parts[1].trim_end_matches('='))
        .ok()?;
    let claims: Value = serde_json::from_slice(&payload).ok()?;
    match claims.get("exp")? {
        Value::Number(n) => n.as_f64().filter(|v| *v > 0.0).and_then(|v| normalise_unix(v as i64)),
        Value::String(s) => s.trim().parse::<i64>().ok().filter(|v| *v > 0).and_then(normalise_unix),
        _ => None,
    }
}

fn expiration_from_map(meta: &serde_json::Map<String, Value>) -> Option<DateTime<Utc>> {
    for key in ["expired", "expire", "expires_at", "expiresAt", "expiry", "expires"] {
        if let Some(t) = meta.get(key).and_then(parse_time) {
            return Some(t);
        }
    }
    let seconds = ["expires_in", "expiresIn"].iter().find_map(|k| {
        let v = meta.get(*k)?;
        let n = v.as_i64().or_else(|| v.as_str().and_then(|s| s.trim().parse().ok()))?;
        (n > 0).then_some(n)
    });
    if let Some(seconds) = seconds
        && let Some(issued) = ["timestamp", "issued_at", "issuedAt"]
            .iter()
            .find_map(|k| meta.get(*k).and_then(parse_time))
    {
        return Some(issued + chrono::Duration::seconds(seconds));
    }
    for nested in ["token", "Token"] {
        if let Some(Value::Object(inner)) = meta.get(nested)
            && let Some(t) = expiration_from_map(inner)
        {
            return Some(t);
        }
    }
    None
}

/// Go `Auth.ExpirationTime`: access-token JWT `exp`, then metadata expiry fields.
pub fn expiration(credential: &Credential) -> Option<DateTime<Utc>> {
    let token = credential
        .str("access_token")
        .filter(|s| !s.is_empty())
        .or_else(|| credential.str("accessToken"))
        .unwrap_or_default();
    jwt_exp(token).or_else(|| expiration_from_map(&credential.metadata))
}

pub fn last_refresh(credential: &Credential) -> Option<DateTime<Utc>> {
    ["last_refresh", "lastRefresh", "last_refreshed_at", "lastRefreshedAt"]
        .iter()
        .find_map(|k| credential.metadata.get(*k).and_then(parse_time))
        .or_else(|| {
            ["last_refresh", "lastRefresh", "last_refreshed_at", "lastRefreshedAt"]
                .iter()
                .find_map(|k| {
                    credential
                        .attributes
                        .get(*k)
                        .and_then(|v| parse_time(&Value::String(v.clone())))
                })
        })
}

pub fn preferred_interval(credential: &Credential) -> Option<chrono::Duration> {
    const KEYS: [&str; 4] = [
        "refresh_interval_seconds",
        "refreshIntervalSeconds",
        "refresh_interval",
        "refreshInterval",
    ];
    let from_metadata = KEYS.iter().find_map(|k| match credential.metadata.get(*k)? {
        Value::Number(n) => n.as_f64().and_then(seconds),
        Value::String(s) => parse_duration_string(s),
        _ => None,
    });
    from_metadata.or_else(|| {
        KEYS.iter()
            .find_map(|k| parse_duration_string(credential.attributes.get(*k)?))
    })
}

/// Go `time.ParseDuration`, kept to the nanosecond; non-positive is absent.
fn go_duration(s: &str) -> Option<chrono::Duration> {
    ck_core::config::parse_duration(s)
        .filter(|nanos| *nanos > 0)
        .map(chrono::Duration::nanoseconds)
}

/// Plain seconds as Go converts them, `time.Duration(secs * float64(time.Second))`;
/// non-positive is absent.
fn seconds(value: f64) -> Option<chrono::Duration> {
    (value > 0.0).then(|| chrono::Duration::nanoseconds((value * 1e9) as i64))
}

/// `parseDurationString`: a Go duration, else plain seconds; non-positive is absent.
fn parse_duration_string(s: &str) -> Option<chrono::Duration> {
    let s = s.trim();
    go_duration(s).or_else(|| s.parse::<f64>().ok().and_then(seconds))
}

/// Go `shouldRefresh` timing for one credential (backoff and lifecycle gates belong to the
/// runtime). `lead` is the provider's SDK refresh lead; `None` means no scheduled refresh.
pub(crate) fn refresh_due(credential: &Credential, lead: Option<chrono::Duration>, now: DateTime<Utc>) -> bool {
    let expiry = expiration(credential);
    if let Some(interval) = preferred_interval(credential) {
        if let Some(expiry) = expiry
            && (expiry <= now || expiry - now <= interval)
        {
            return true;
        }
        return last_refresh(credential).is_none_or(|last| now - last >= interval);
    }
    let Some(lead) = lead else {
        return false;
    };
    if let Some(expiry) = expiry {
        return expiry - now <= lead;
    }
    last_refresh(credential).is_none_or(|last| now - last >= lead)
}

mod usage;
pub(crate) use usage::*;
mod capture;
pub(crate) use capture::*;
