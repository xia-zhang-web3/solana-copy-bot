//! Collect immutable finalized operands before starting the price quote's5s clock.
use super::{rpc, submit};
use anyhow::{ensure, Result};
use chrono::Utc;
use copybot_config::ExecutionConfig;
use copybot_storage_core::{
    association_inbox::InboxLimits, ordered_sell_quote::QuoteClaim, SqliteStore,
};

pub(crate) struct Prequote {
    pub authority: rpc::Authority,
    pub rent: Option<u64>,
}
pub(crate) async fn collect(
    store: &mut SqliteStore,
    c: &ExecutionConfig,
    claim: &QuoteClaim,
    limits: InboxLimits,
    live: &submit::guard::Live,
) -> Result<Option<Prequote>> {
    if c.real_session.is_none() {
        return Ok(None);
    }
    // Preserve the old finalized collector's closed redirect boundary.
    let http = rpc::body::closed_client()?;
    let snapshot = store.owned_sell_snapshot(&claim.binding, limits)?;
    let mut check = || {
        let store = &mut *store;
        submit::guard::continuity(live)?;
        ensure!(
            live.0.load(std::sync::atomic::Ordering::SeqCst),
            "owned_sell_runner_cancelled"
        );
        crate::execution_technical_cohort::before_deadline(c)?;
        ensure!(
            !std::path::Path::new(&c.canary_kill_switch_path).exists(),
            "kill_switch_active"
        );
        ensure!(
            store.recheck_strict_sell_quote(claim, limits, Utc::now())?,
            "owned_sell_prequote_claim_changed"
        );
        Ok(())
    };
    check()?;
    let authority = rpc::collect(&http, c, &snapshot, &mut check).await?;
    let rent = if c.controlled_receipt_lot.is_some() {
        Some(crate::execution_wsol_lifecycle::rent(&http, c, &mut check).await?)
    } else {
        None
    };
    check()?;
    // The runner re-reads the full graph and verifies Authority::binding after
    // quote creation and again before ownership/signing. No mutable fact is cached.
    Ok(Some(Prequote { authority, rent }))
}
