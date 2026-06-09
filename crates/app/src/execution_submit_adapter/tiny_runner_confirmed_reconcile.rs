use super::{
    build_execution_confirmed_fill_from_request, build_tiny_submit_reconciliation_request,
    ExecutionTinySubmitConfirmPathOutcome,
};
use crate::execution_submit_adapter::record_confirmed_fill_accounting;
use chrono::{DateTime, Utc};
use copybot_config::ExecutionConfig;
use copybot_storage_core::{ExecutionCanaryOrder, SqliteStore};

pub(crate) fn reconcile_already_confirmed_tiny_submit_fill(
    store: &SqliteStore,
    config: &ExecutionConfig,
    order: &ExecutionCanaryOrder,
    now: DateTime<Utc>,
    reason: &str,
) -> ExecutionTinySubmitConfirmPathOutcome {
    let mut outcome = ExecutionTinySubmitConfirmPathOutcome {
        confirmation_confirmed: 1,
        tx_signature: order.tx_signature.clone(),
        reason: Some(reason.to_string()),
        ..ExecutionTinySubmitConfirmPathOutcome::default()
    };
    match build_tiny_submit_reconciliation_request(store, config, order)
        .and_then(|request| build_execution_confirmed_fill_from_request(store, &request, now))
        .and_then(|fill| record_confirmed_fill_accounting(store, fill))
    {
        Ok(accounting) => {
            outcome.buy_opened = accounting.buy_opened;
            outcome.buy_existing = accounting.buy_existing;
            outcome.sell_closed = accounting.sell_closed;
            outcome.sell_partial = accounting.sell_partial;
            outcome.sell_dust_closed = accounting.sell_dust_closed;
            outcome.sell_no_position = accounting.sell_no_position;
        }
        Err(error) => {
            outcome.error = Some(format!(
                "confirmed_fill_accounting_backfill_failed: {error:#}"
            ));
        }
    }
    outcome
}
