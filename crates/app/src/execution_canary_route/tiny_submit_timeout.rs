use super::tiny_submit_expiry::{
    retry_budget_exhausted_reason, TINY_SUBMIT_RETRY_BUDGET_EXHAUSTED,
};
use crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary;
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use copybot_storage_core::{
    SqliteStore, EXECUTION_CANARY_CONFIRM_DECISION_EXPIRE_UNSAFE,
    EXECUTION_CANARY_CONFIRM_DECISION_NOT_SUBMITTED, EXECUTION_CANARY_CONFIRM_DECISION_RETRY,
    EXECUTION_CANARY_CONFIRM_DECISION_WAIT, EXECUTION_STATUS_CANARY_EXPIRED,
    EXECUTION_STATUS_CANARY_SIMULATED,
};

pub(super) fn process_tiny_submit_timeout(
    store: &SqliteStore,
    order_id: &str,
    timeout_ms: u64,
    max_submit_attempts: u32,
    now: DateTime<Utc>,
    summary: &mut ExecutionCanaryStateMachineSummary,
) -> Result<()> {
    let timeout = Duration::milliseconds(i64::try_from(timeout_ms).unwrap_or(i64::MAX));
    summary.submit_timeout_candidates += 1;
    summary.last_order_id = Some(order_id.to_string());
    let decision = store.execution_canary_confirm_timeout_decision(order_id, now, timeout)?;
    summary.last_confirm_decision = Some(decision.decision_status.clone());
    summary.last_confirm_reason = Some(decision.decision_reason.clone());
    summary.last_elapsed_seconds = decision.elapsed_seconds;
    summary.last_timeout_seconds = decision.timeout_seconds;
    match decision.decision_status.as_str() {
        EXECUTION_CANARY_CONFIRM_DECISION_WAIT => {
            summary.submit_timeout_wait += 1;
        }
        EXECUTION_CANARY_CONFIRM_DECISION_NOT_SUBMITTED => {
            summary.submit_timeout_not_submitted += 1;
            summary.skipped_reason = Some("not_submitted");
        }
        EXECUTION_CANARY_CONFIRM_DECISION_RETRY => {
            if decision.order.attempt >= max_submit_attempts.max(1) {
                let reason = retry_budget_exhausted_reason(&decision.order);
                let order = store.mark_execution_canary_expired(order_id, now, &reason)?;
                if order.status == EXECUTION_STATUS_CANARY_EXPIRED {
                    summary.expired += 1;
                    summary.skipped_reason = Some(TINY_SUBMIT_RETRY_BUDGET_EXHAUSTED);
                    summary.last_confirm_reason = Some(reason.clone());
                    summary.last_error = Some(reason);
                }
                return Ok(());
            }
            let order = store.mark_execution_canary_retry_after_submit_timeout(
                order_id,
                now,
                timeout,
                "retry_after_unknown_submit_timeout",
            )?;
            if order.status == EXECUTION_STATUS_CANARY_SIMULATED {
                summary.submit_timeout_retry += 1;
                summary.simulated += 1;
            }
        }
        EXECUTION_CANARY_CONFIRM_DECISION_EXPIRE_UNSAFE => {
            let order = store.mark_execution_canary_expired(
                order_id,
                now,
                "confirm_timeout_with_signature_retry_unsafe",
            )?;
            if order.status == EXECUTION_STATUS_CANARY_EXPIRED {
                summary.submit_timeout_expire_unsafe += 1;
                summary.expired += 1;
            }
        }
        _ => {
            summary.skipped_reason = Some("unsupported_submit_timeout_decision");
        }
    }
    Ok(())
}
