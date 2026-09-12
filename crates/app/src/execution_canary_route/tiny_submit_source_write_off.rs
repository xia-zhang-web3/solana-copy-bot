use super::tiny_submit_sell_retry::record_terminal_write_off_summary;
use crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary;
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_storage_core::{
    ExecutionSourceSellWriteOffKind as Kind, ExecutionSourceSellWriteOffOutcome as Outcome,
    SqliteStore,
};

/// Only NotPromoted authorizes the existing legacy branch. Refusals are local to A.
pub(super) fn apply_source_write_off(
    store: &SqliteStore,
    order_id: &str,
    kind: Kind,
    now: DateTime<Utc>,
    summary: &mut ExecutionCanaryStateMachineSummary,
) -> Result<bool> {
    let refusal = match store.write_off_execution_source_sell(order_id, kind, now) {
        Ok(Outcome::NotPromoted) => return Ok(false),
        Ok(Outcome::Refused(reason)) => reason,
        Err(error) if crate::execution_canary_summary::is_local_source_write_off_error(&error) => {
            "source_sell_write_off_unavailable"
        }
        Err(error) => return Err(error),
        Ok(Outcome::WrittenOff {
            close_result,
            order,
            ..
        }) => {
            summary.last_order_id = Some(order_id.to_owned());
            record_terminal_write_off_summary(
                store,
                summary,
                &order,
                vec![close_result],
                kind.reason(),
            )?;
            return Ok(true);
        }
    };
    summary
        .source_sell_write_off_refusals
        .record(order_id, refusal);
    summary.last_order_id = Some(order_id.to_owned());
    summary.skipped_reason = Some(refusal);
    // Keep the diagnostic ID with its reason even if later summaries name a successful B.
    summary.last_error = Some(format!("{refusal}: order_id={order_id}"));
    summary.open_positions = store.execution_canary_open_position_count()?;
    Ok(true)
}
