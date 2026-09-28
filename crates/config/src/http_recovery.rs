//! Explicit, opt-in confirmed-block recovery through the local metered broker.
use anyhow::{ensure, Result};
use serde::Deserialize;
use std::fmt;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpRecoveryConfig {
    pub broker_url: String,
    pub broker_token: String,
    /// Per getBlocks chunk, rather than a cap on the entire outage.
    pub range_slots: u64,
    pub max_response_bytes: usize,
    pub timeout_ms: u64,
    pub fetch_concurrency: usize,
}
impl HttpRecoveryConfig {
    pub fn validate(&self) -> Result<()> {
        let authority = self
            .broker_url
            .strip_prefix("http://")
            .and_then(|s| s.split('/').next());
        ensure!(
            authority.is_some_and(|a| {
                a.strip_prefix("127.0.0.1:")
                    .or_else(|| a.strip_prefix("[::1]:"))
                    .is_some_and(|p| p.parse::<u16>().is_ok_and(|n| n > 0))
            }),
            "http_recovery_requires_loopback_broker"
        );
        ensure!(
            !self.broker_token.contains("REPLACE_ME"),
            "http_recovery_placeholder_token"
        );
        ensure!(
            self.range_slots > 0 && self.range_slots <= 500_000,
            "http_recovery_get_blocks_range"
        );
        ensure!(
            self.max_response_bytes > 0
                && self.max_response_bytes <= u32::MAX as usize
                && self.timeout_ms > 0
                && self.fetch_concurrency > 0,
            "http_recovery_explicit_response_timeout_bounds"
        );
        Ok(())
    }
}
impl fmt::Debug for HttpRecoveryConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpRecoveryConfig")
            .field("broker_url", &"<local broker>")
            .field("broker_token", &"<redacted>")
            .field("range_slots", &self.range_slots)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("timeout_ms", &self.timeout_ms)
            .field("fetch_concurrency", &self.fetch_concurrency)
            .finish()
    }
}
