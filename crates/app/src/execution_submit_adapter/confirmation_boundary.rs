use super::{
    build_confirmation_request_from_order, fetch_rpc_signature_confirmation,
    record_confirmation_tracker_outcome, ExecutionConfirmedFill,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_storage_core::{
    SqliteStore, EXECUTION_ERROR_CONFIRMATION_FAILED, EXECUTION_STATUS_CANARY_FAILED,
};

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ExecutionConfirmationBoundaryOutcome {
    pub(crate) confirmed: usize,
    pub(crate) pending: usize,
    pub(crate) failed: usize,
    pub(crate) reason: Option<String>,
    pub(crate) error: Option<String>,
    pub(crate) confirmation_status: Option<String>,
    pub(crate) slot: Option<u64>,
    pub(crate) buy_opened: usize,
    pub(crate) buy_existing: usize,
    pub(crate) sell_closed: usize,
    pub(crate) sell_partial: usize,
    pub(crate) sell_dust_closed: usize,
    pub(crate) sell_no_position: usize,
    pub(crate) close_status: Option<String>,
    pub(crate) closed_qty: f64,
    pub(crate) pnl_sol: f64,
}

pub(crate) async fn record_execution_rpc_confirmation_boundary(
    store: &SqliteStore,
    http: &reqwest::Client,
    rpc_url: &str,
    order_id: &str,
    fill: ExecutionConfirmedFill,
    now: DateTime<Utc>,
    timeout_ms: u64,
) -> Result<ExecutionConfirmationBoundaryOutcome> {
    let request = build_confirmation_request_from_order(store, order_id, now)?;
    let tracker_outcome =
        match fetch_rpc_signature_confirmation(http, rpc_url, &request, now, timeout_ms).await {
            Ok(outcome) => outcome,
            Err(error) if is_rpc_confirmation_transaction_error(&error) => {
                let error = format!("{error:#}");
                let order = store.mark_execution_canary_failed(
                    order_id,
                    now,
                    EXECUTION_ERROR_CONFIRMATION_FAILED,
                    &error,
                )?;
                return Ok(ExecutionConfirmationBoundaryOutcome {
                    failed: usize::from(order.status == EXECUTION_STATUS_CANARY_FAILED),
                    reason: Some(EXECUTION_ERROR_CONFIRMATION_FAILED.to_string()),
                    error: Some(error),
                    ..ExecutionConfirmationBoundaryOutcome::default()
                });
            }
            Err(error) => return Err(error),
        };
    let record = record_confirmation_tracker_outcome(store, &request, tracker_outcome, fill)?;
    let mut outcome = ExecutionConfirmationBoundaryOutcome {
        confirmed: record.confirmed,
        pending: record.pending,
        reason: record.reason,
        confirmation_status: record.confirmation_status,
        slot: record.slot,
        ..ExecutionConfirmationBoundaryOutcome::default()
    };
    if let Some(fill) = record.fill_accounting {
        outcome.buy_opened = fill.buy_opened;
        outcome.buy_existing = fill.buy_existing;
        outcome.sell_closed = fill.sell_closed;
        outcome.sell_partial = fill.sell_partial;
        outcome.sell_dust_closed = fill.sell_dust_closed;
        outcome.sell_no_position = fill.sell_no_position;
        outcome.close_status = fill.close_status;
        outcome.closed_qty = fill.closed_qty;
        outcome.pnl_sol = fill.pnl_sol;
    }
    Ok(outcome)
}

fn is_rpc_confirmation_transaction_error(error: &anyhow::Error) -> bool {
    format!("{error:#}").contains("confirmation RPC transaction_error")
}
