//! Synchronous authoritative submit checks, repeated across the funding await.
use crate::execution_canary_submit_contract::ExecutionSubmitPlanOutcome;
use crate::execution_submit_adapter::ExecutionSubmitRequest;
use copybot_core_types::CopySignalRow;
use copybot_storage_core::{ExecutionCanaryOrder, SqliteStore, EXECUTION_STATUS_CANARY_SIMULATED};

#[path = "execution_tiny_budget.rs"]
pub(crate) mod budget;
#[path = "execution_dispatch.rs"]
pub(crate) mod dispatch;

pub(crate) struct SubmitState {
    order: ExecutionCanaryOrder,
    signal: CopySignalRow,
}

pub(crate) fn eligible(
    store: &SqliteStore,
    request: &ExecutionSubmitRequest,
) -> Result<SubmitState, &'static str> {
    let order = store
        .load_execution_canary_order(&request.order_id)
        .map_err(|_| "tiny_submit_state_unavailable")?
        .ok_or("tiny_submit_order_missing")?;
    if order.status != EXECUTION_STATUS_CANARY_SIMULATED
        || order
            .tx_signature
            .as_deref()
            .is_some_and(|s| !s.trim().is_empty())
    {
        return Err("tiny_order_not_submit_eligible");
    }
    let signal = store
        .load_copy_signal_by_signal_id(&order.signal_id)
        .map_err(|_| "tiny_submit_state_unavailable")?
        .ok_or("tiny_submit_signal_missing")?;
    if request.signal_id != order.signal_id
        || request.client_order_id != order.client_order_id
        || request.attempt != order.attempt
        || request.route != order.route
        || request.token != signal.token
        || !request.side.eq_ignore_ascii_case(&signal.side)
    {
        return Err("tiny_submit_identity_mismatch");
    }
    if request.side.eq_ignore_ascii_case("buy")
        && store
            .execution_canary_unresolved_buy()
            .map_err(|_| "tiny_submit_state_unavailable")?
    {
        return Err(copybot_storage_core::EXECUTION_UNRESOLVED_BUY_REASON);
    }
    if let Some(reason) = store
        .execution_canary_receipt_submit_block_reason(
            &request.order_id,
            &request.token,
            &request.side,
        )
        .map_err(|_| "tiny_submit_state_unavailable")?
    {
        return Err(reason);
    }
    if let Some(reason) = store
        .execution_sell_intent_position_block_reason(&signal)
        .map_err(|_| "tiny_submit_state_unavailable")?
    {
        return Err(reason);
    }
    Ok(SubmitState { order, signal })
}

pub(crate) fn unchanged(
    before: &SubmitState,
    store: &SqliteStore,
    request: &ExecutionSubmitRequest,
) -> Result<(), &'static str> {
    let after = eligible(store, request).map_err(|reason| {
        if reason == "tiny_submit_state_unavailable" {
            reason
        } else {
            "initial_sol_order_changed"
        }
    })?;
    let (a, b) = (&before.signal, &after.signal);
    if before.order != after.order
        || a.signal_id != b.signal_id
        || a.wallet_id != b.wallet_id
        || a.side != b.side
        || a.token != b.token
        || a.status != b.status
        || a.ts != b.ts
        || a.notional_sol.to_bits() != b.notional_sol.to_bits()
        || a.notional_lamports != b.notional_lamports
        || a.notional_origin != b.notional_origin
    {
        return Err("initial_sol_order_changed");
    }
    Ok(())
}

pub(crate) fn reject(reason: &'static str) -> ExecutionSubmitPlanOutcome {
    ExecutionSubmitPlanOutcome {
        submit_ready_rejected: 1,
        skipped_reason: Some(reason),
        reason: Some(reason.into()),
        ..Default::default()
    }
}
