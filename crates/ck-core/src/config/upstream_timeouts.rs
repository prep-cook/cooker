//! `requests.upstream-timeouts.*`: how long one upstream attempt may wait for response
//! headers, between response body chunks, and in total. Live sessions (Realtime, Live,
//! WebRTC, Responses WebSocket) keep their own liveness rules (`realtime.*`).

use std::time::Duration;

use anyhow::{Result, bail};
use serde_yaml_ng::Value;

use super::Config;

/// Deadlines of one upstream attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UpstreamTimeouts {
    /// From the attempt's start until the upstream's response headers.
    pub header: Duration,
    /// Between two response body chunks.
    pub idle: Duration,
    /// From the attempt's start until its response body ends.
    pub total: Duration,
}

impl Default for UpstreamTimeouts {
    /// A non-streaming answer arrives with its headers, so `header` covers a whole
    /// non-streaming generation: Anthropic directs requests expected to run past 10 minutes
    /// to streaming. `idle` is Codex CLI's stream idle timeout (300 s); bifrost uses 120 s.
    /// `total` bounds an attempt that keeps trickling without finishing.
    fn default() -> Self {
        Self {
            header: Duration::from_secs(600),
            idle: Duration::from_secs(300),
            total: Duration::from_secs(3600),
        }
    }
}

impl UpstreamTimeouts {
    /// The settings `document` states, after the startup checks.
    pub(super) fn read(document: &Value) -> Result<Self> {
        let section = document.get("requests").and_then(|r| r.get("upstream-timeouts"));
        let defaults = Self::default();
        let seconds = |key: &str, default: Duration| -> Result<Duration> {
            match section.and_then(|s| s.get(key)).filter(|v| !v.is_null()) {
                None => Ok(default),
                Some(v) => match super::go_int(v) {
                    Some(n) if n > 0 => Ok(Duration::from_secs(n as u64)),
                    Some(_) => bail!("requests.upstream-timeouts.{key} must be greater than 0"),
                    None => bail!("requests.upstream-timeouts.{key} must be an integer"),
                },
            }
        };
        Ok(Self {
            header: seconds("header-seconds", defaults.header)?,
            idle: seconds("idle-seconds", defaults.idle)?,
            total: seconds("total-seconds", defaults.total)?,
        })
    }
}

impl Config {
    /// The `requests.upstream-timeouts.*` settings; [`Config::parse`] already refused invalid ones.
    pub fn upstream_timeouts(&self) -> UpstreamTimeouts {
        UpstreamTimeouts::read(&self.document).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upstream_timeouts_are_configurable_and_validated() {
        let secs = Duration::from_secs;
        // (config, settings or the error)
        let cases: [(&str, Result<UpstreamTimeouts, &str>); 5] = [
            ("", Ok(UpstreamTimeouts::default())),
            (
                "requests: {upstream-timeouts: {header-seconds: 5, idle-seconds: 7, total-seconds: 90}}",
                Ok(UpstreamTimeouts {
                    header: secs(5),
                    idle: secs(7),
                    total: secs(90),
                }),
            ),
            (
                "requests: {upstream-timeouts: {idle-seconds: 30}}",
                Ok(UpstreamTimeouts {
                    idle: secs(30),
                    ..UpstreamTimeouts::default()
                }),
            ),
            (
                "requests: {upstream-timeouts: {total-seconds: 0}}",
                Err("requests.upstream-timeouts.total-seconds must be greater than 0"),
            ),
            (
                "requests: {upstream-timeouts: {header-seconds: -1}}",
                Err("requests.upstream-timeouts.header-seconds must be greater than 0"),
            ),
        ];
        for (text, want) in cases {
            match (Config::parse(text), want) {
                (Ok(cfg), Ok(want)) => assert_eq!(cfg.upstream_timeouts(), want, "{text}"),
                (Err(error), Err(want)) => assert!(format!("{error:#}").contains(want), "{text}: {error:#}"),
                (got, want) => panic!("{text}: got {:?}, want {want:?}", got.map(|c| c.upstream_timeouts())),
            }
        }
    }
}
