use super::tiny_submit::reconcile_existing_tiny_submit_order;
use super::tiny_submit_retry::TINY_SUBMIT_RETRY_AFTER_UNKNOWN_SUBMIT_TIMEOUT_REASON as UNKNOWN;
use crate::execution_canary_state_machine::ExecutionCanaryStateMachineSummary;
use crate::execution_canary_submit_contract::TINY_SUBMIT_RETRY_AFTER_RPC_NOT_SENT_REASON as NOT_SENT;
use crate::execution_source_sell_continuation::{
    Continuation, Family, SOURCE_REFUSAL_VISIT_BUDGET,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::SqliteStore;
use std::collections::HashSet;

pub(super) async fn reconcile_selected(
    config: &ExecutionConfig,
    store: &SqliteStore,
    now: DateTime<Utc>,
    blocked_buy: Option<u32>,
    progress: &Continuation,
    summary: &mut ExecutionCanaryStateMachineSummary,
) -> Result<()> {
    let limit = config.canary_batch_limit.max(1) as usize;
    let groups = if blocked_buy.is_some() {
        vec![(Family::Combined, UNKNOWN, Some(NOT_SENT))]
    } else {
        vec![
            (Family::UnknownSubmit, UNKNOWN, None),
            (Family::NotSent, NOT_SENT, None),
        ]
    };
    let mut seen = HashSet::new();
    let mut handled = 0;
    for (family, reason, extra) in groups {
        if handled >= limit {
            break;
        }
        // A refused prefix in one retry family must not spend another family's
        // opportunity to make progress. The valid-handler limit remains shared.
        let mut refused = 0;
        let after = progress.after(family);
        // Read one bounded page before any await, preserving the existing receipt age order.
        let rows = store.list_reconcilable_execution_canary_orders_page(
            &config.canary_route,
            reason,
            extra,
            (limit - handled)
                .saturating_add(SOURCE_REFUSAL_VISIT_BUDGET - refused)
                .min(u32::MAX as usize) as u32,
            blocked_buy,
            after.as_ref(),
        )?;
        if rows.is_empty() {
            progress.wrap(family);
            continue;
        }
        for row in rows {
            if handled >= limit || refused >= SOURCE_REFUSAL_VISIT_BUDGET {
                break;
            }
            if !seen.insert(row.order.order_id.clone()) {
                continue;
            }
            let before = summary.source_sell_refusals.count();
            summary.existing += 1;
            summary.last_order_id = Some(row.order.order_id.clone());
            reconcile_existing_tiny_submit_order(config, store, &row.order, now, summary).await?;
            let local = summary.source_sell_refusals.count() > before;
            progress.visited(family, row.cursor, local);
            if local {
                refused += 1;
            } else {
                handled += 1;
            }
        }
    }
    Ok(())
}
