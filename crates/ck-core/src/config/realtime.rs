//! `realtime.*`: liveness and limits of realtime client sockets (Settled 58,
//! `REQUIREMENTS.md`, "Deploy-only realtime settings"). Read once at startup.

use std::time::Duration;

use anyhow::{Result, bail};
use serde_yaml_ng::Value;

use super::Config;

/// Realtime client socket settings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Realtime {
    /// Between the pings the gateway sends to the client.
    pub ping_interval: Duration,
    /// How long the client may go without a pong; only a pong extends it.
    pub read_timeout: Duration,
    /// How long one write to the client may block.
    pub write_timeout: Duration,
    /// Concurrent sockets per client key; 0 is no limit.
    pub max_sockets_per_key: u32,
}

impl Default for Realtime {
    fn default() -> Self {
        Self {
            ping_interval: Duration::from_secs(30),
            read_timeout: Duration::from_secs(60),
            write_timeout: Duration::from_secs(10),
            max_sockets_per_key: 0,
        }
    }
}

impl Realtime {
    /// The settings of `section`, the `realtime` mapping when present, after the startup checks.
    pub(super) fn read(section: Option<&Value>) -> Result<Self> {
        let defaults = Self::default();
        let value = |key: &str, default: u64| -> Result<i64> {
            match section.and_then(|s| s.get(key)).filter(|v| !v.is_null()) {
                None => Ok(default as i64),
                Some(v) => match super::go_int(v) {
                    Some(n) => Ok(n),
                    None => bail!("realtime.{key} must be an integer"),
                },
            }
        };
        let seconds = |key: &str, default: Duration| -> Result<Duration> {
            let n = value(key, default.as_secs())?;
            if n <= 0 {
                bail!("realtime.{key} must be greater than 0");
            }
            Ok(Duration::from_secs(n as u64))
        };
        let ping_interval = seconds("ping-interval-seconds", defaults.ping_interval)?;
        let read_timeout = seconds("read-timeout-seconds", defaults.read_timeout)?;
        let write_timeout = seconds("write-timeout-seconds", defaults.write_timeout)?;
        if ping_interval >= read_timeout {
            bail!("realtime.ping-interval-seconds must be less than realtime.read-timeout-seconds");
        }
        let max_sockets_per_key = match u32::try_from(value("max-sockets-per-key", 0)?) {
            Ok(n) => n,
            Err(_) => bail!("realtime.max-sockets-per-key must be between 0 and 4294967295"),
        };
        Ok(Self {
            ping_interval,
            read_timeout,
            write_timeout,
            max_sockets_per_key,
        })
    }
}

impl Config {
    /// The `realtime.*` settings; [`Config::parse`] already refused invalid ones.
    pub fn realtime(&self) -> Realtime {
        Realtime::read(self.document.get("realtime")).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn realtime_socket_settings_are_configurable_and_validated() {
        let secs = Duration::from_secs;
        // (config, settings or the error)
        let cases: [(&str, Result<Realtime, &str>); 7] = [
            ("", Ok(Realtime::default())),
            (
                "realtime: {ping-interval-seconds: 5, read-timeout-seconds: 12, write-timeout-seconds: 3, max-sockets-per-key: 2}",
                Ok(Realtime {
                    ping_interval: secs(5),
                    read_timeout: secs(12),
                    write_timeout: secs(3),
                    max_sockets_per_key: 2,
                }),
            ),
            (
                "realtime: {ping-interval-seconds: 60}",
                Err("realtime.ping-interval-seconds must be less than realtime.read-timeout-seconds"),
            ),
            (
                "realtime: {read-timeout-seconds: 20}",
                Err("realtime.ping-interval-seconds must be less than realtime.read-timeout-seconds"),
            ),
            (
                "realtime: {write-timeout-seconds: 0}",
                Err("realtime.write-timeout-seconds must be greater than 0"),
            ),
            (
                "realtime: {ping-interval-seconds: -1}",
                Err("realtime.ping-interval-seconds must be greater than 0"),
            ),
            (
                "realtime: {max-sockets-per-key: -1}",
                Err("realtime.max-sockets-per-key must be between 0 and 4294967295"),
            ),
        ];
        for (text, want) in cases {
            match (Config::parse(text), want) {
                (Ok(cfg), Ok(want)) => assert_eq!(cfg.realtime(), want, "{text}"),
                (Err(error), Err(want)) => assert!(format!("{error:#}").contains(want), "{text}: {error:#}"),
                (got, want) => panic!("{text}: got {:?}, want {want:?}", got.map(|c| c.realtime())),
            }
        }
    }
}
