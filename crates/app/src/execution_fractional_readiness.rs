//! Bounded acquisition inside one existing quote claim; never a lease renewal.
use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{
    association_inbox::InboxLimits, ordered_sell_quote::QuoteClaim, SqliteStore,
};
use std::{future::Future, pin::Pin, time::Duration};

pub(crate) const OBSERVATIONS: usize = 3;
pub(crate) const WAIT: Duration = Duration::from_secs(6);
pub(crate) type Sleep<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
/// Local acquisition clock seam; the daemon always supplies WallClock.
pub(crate) trait Clock: Send {
    fn now(&self) -> DateTime<Utc>;
    fn sleep(&mut self, duration: Duration) -> Sleep<'_>;
}
pub(crate) struct WallClock;
impl Clock for WallClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
    fn sleep(&mut self, duration: Duration) -> Sleep<'_> {
        Box::pin(tokio::time::sleep(duration))
    }
}
pub(crate) fn cutoff(c: &ExecutionConfig, claim: &QuoteClaim) -> DateTime<Utc> {
    // Existing quote timeout plus a positive completion margin remain in the lease.
    claim.lease_until
        - chrono::Duration::milliseconds(c.quote_canary_timeout_ms.clamp(1, 10_000) as i64 + 1_500)
}
pub(crate) fn guard(c: &ExecutionConfig, claim: &QuoteClaim, clock: &impl Clock) -> Result<()> {
    ensure!(
        !std::path::Path::new(&c.canary_kill_switch_path).exists(),
        "kill_switch_active"
    );
    ensure!(
        clock.now() < cutoff(c, claim),
        "fraction_collection_deadline"
    );
    if let Some(cohort) = c.technical_cohort.as_ref().filter(|p| p.activate) {
        let activated = DateTime::parse_from_rfc3339(&cohort.activated_at)?.with_timezone(&Utc);
        let deadline = DateTime::parse_from_rfc3339(&cohort.deadline)?.with_timezone(&Utc);
        ensure!(
            clock.now() >= activated && clock.now() < deadline,
            "technical_cohort_deadline"
        );
    }
    Ok(())
}
pub(crate) async fn wait(
    store: &mut SqliteStore,
    c: &ExecutionConfig,
    claim: &QuoteClaim,
    limits: InboxLimits,
    clock: &mut impl Clock,
) -> Result<()> {
    let until = clock.now() + chrono::Duration::from_std(WAIT)?;
    ensure!(until < cutoff(c, claim), "fraction_readiness_wait_deadline");
    // Fixed number of sleeps remains bounded even if a wall clock moves backwards.
    for _ in 0..=WAIT.as_secs() {
        guard(c, claim, clock)?;
        store.recheck_fractional_collection(claim, limits, clock.now())?;
        let remaining = (until - clock.now()).to_std().unwrap_or_default();
        if remaining.is_zero() {
            return Ok(());
        }
        clock.sleep(remaining.min(Duration::from_secs(1))).await;
    }
    anyhow::bail!("fraction_readiness_wait_clock")
}
