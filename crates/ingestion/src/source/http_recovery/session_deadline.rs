//! The broker's original owner clock is a ceiling, never a renewed local lease.
use anyhow::{ensure, Context, Result};
use reqwest::header::HeaderMap;
use std::{
    sync::Mutex,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Default)]
pub(super) struct SessionDeadline(Mutex<Option<u64>>);
impl SessionDeadline {
    pub fn observe(&self, headers: &HeaderMap) -> Result<()> {
        let Some(value) = headers.get("X-Copybot-Session-Deadline-Unix-Ms") else {
            // Generic local brokers do not grant an owner clock. No absence
            // creates a new session; the metered broker still gates every POST.
            return Ok(());
        };
        let value = value
            .to_str()
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|n| *n > 0)
            .context("http_recovery_invalid_session_deadline")?;
        let mut saved = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("http_recovery_deadline_lock"))?;
        ensure!(
            saved.is_none_or(|original| original == value),
            "http_recovery_session_deadline_changed"
        );
        *saved = Some(value);
        Ok(())
    }
    pub fn remaining(&self) -> Result<Option<Duration>> {
        let saved = *self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("http_recovery_deadline_lock"))?;
        let Some(deadline) = saved else {
            return Ok(None);
        };
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("http_recovery_system_clock_before_epoch")?;
        let remaining = Duration::from_millis(deadline)
            .checked_sub(elapsed)
            .filter(|left| !left.is_zero())
            .context("http_recovery_session_deadline_exhausted")?;
        Ok(Some(remaining))
    }
}
