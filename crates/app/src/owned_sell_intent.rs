use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_core_types::SwapEvent;
use copybot_shadow::{
    FollowSnapshot, ShadowDropReason, ShadowProcessOutcome, ShadowService, ShadowSignalResult,
};
use copybot_storage_core::{ExecutionSellIntentOutcome, ExecutionSellIntentReject, SqliteStore};

pub(super) fn process_swap(
    shadow: &ShadowService,
    store: &SqliteStore,
    swap: &SwapEvent,
    follow: &FollowSnapshot,
    now: DateTime<Utc>,
    rejection: &mut Option<ExecutionSellIntentReject>,
) -> Result<ShadowProcessOutcome> {
    *rejection = None;
    let outcome = shadow.process_swap(store, swap, follow, now)?;
    if !matches!(
        outcome,
        ShadowProcessOutcome::Dropped(
            ShadowDropReason::BelowNotional | ShadowDropReason::LagExceeded
        )
    ) || !matches!(
        crate::swap_classification::classify_swap_side(swap),
        Some(crate::shadow_scheduler::ShadowSwapSide::Sell)
    ) || !follow.is_active(&swap.wallet)
        || !follow.is_followed_at(&swap.wallet, swap.ts_utc)
    {
        return Ok(outcome);
    }
    match store.record_execution_sell_intent(swap)? {
        ExecutionSellIntentOutcome::Inserted(signal) => {
            Ok(ShadowProcessOutcome::Recorded(ShadowSignalResult {
                signal_id: signal.signal_id,
                wallet_id: signal.wallet_id,
                side: signal.side,
                token: signal.token,
                notional_sol: signal.notional_sol,
                latency_ms: (now - swap.ts_utc).num_milliseconds(),
                closed_qty: 0.0,
                realized_pnl_sol: 0.0,
                has_open_lots_after_signal: None,
            }))
        }
        // State changed between the entry-gate result and the atomic intent check.
        ExecutionSellIntentOutcome::Rejected(ExecutionSellIntentReject::ShadowRiskPresent) => {
            shadow.process_swap(store, swap, follow, now)
        }
        ExecutionSellIntentOutcome::Rejected(reason) => {
            *rejection = Some(reason);
            Ok(outcome)
        }
    }
}
