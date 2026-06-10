use crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary;
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{SqliteStore, EXECUTION_STATUS_CANARY_EXPIRED};

const STALE_TINY_SUBMIT_CANDIDATE_REASON: &str = "stale_candidate_no_build_metadata";

pub(super) fn expire_stale_tiny_submit_candidates(
    config: &ExecutionConfig,
    store: &SqliteStore,
    now: DateTime<Utc>,
    limit: u32,
    summary: &mut ExecutionCanaryStateMachineSummary,
) -> Result<bool> {
    let max_signal_age_seconds = config
        .canary_max_signal_age_seconds
        .max(1)
        .min(i64::MAX as u64) as i64;
    let submitted_before = now - chrono::Duration::seconds(max_signal_age_seconds);
    let orders = store.list_stale_not_run_execution_canary_candidates_for_route(
        &config.canary_route,
        submitted_before,
        limit,
    )?;
    for order in orders {
        summary.existing += 1;
        summary.last_order_id = Some(order.order_id.clone());
        let expired = store.mark_execution_canary_expired(
            &order.order_id,
            now,
            STALE_TINY_SUBMIT_CANDIDATE_REASON,
        )?;
        if expired.status == EXECUTION_STATUS_CANARY_EXPIRED {
            summary.expired += 1;
            summary.skipped_reason = Some(STALE_TINY_SUBMIT_CANDIDATE_REASON);
        }
    }
    Ok(summary.expired > 0)
}
