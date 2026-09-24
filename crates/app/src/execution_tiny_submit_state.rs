//! Synchronous authoritative submit checks, repeated across the funding await.
use crate::execution_canary_submit_contract::ExecutionSubmitPlanOutcome;
use crate::execution_submit_adapter::ExecutionSubmitRequest;
use copybot_core_types::CopySignalRow;
use copybot_storage_core::{ExecutionCanaryOrder, SqliteStore, EXECUTION_STATUS_CANARY_SIMULATED};

#[path = "execution_tiny_budget.rs"]
pub(crate) mod budget;
#[path = "execution_dispatch.rs"]
pub(crate) mod dispatch;

pub(crate) enum SubmitState {
    OwnerExit {
        order: ExecutionCanaryOrder,
        intent: copybot_storage_core::OwnerExitIntent,
    },
    OwnerTechnicalBuy {
        order: ExecutionCanaryOrder,
        intent: copybot_storage_core::OwnerTechnicalBuyIntent,
    },
    Legacy {
        order: ExecutionCanaryOrder,
        signal: CopySignalRow,
    },
    Owned(copybot_storage_core::rpc_owned_sell_handoff::dispatch::Prepared),
}

pub(crate) fn eligible(
    store: &SqliteStore,
    request: &ExecutionSubmitRequest,
) -> Result<SubmitState, &'static str> {
    if request.metadata.rpc_owned_sell.is_some() {
        return crate::execution_owned_sell_prepare::submit::guard::request(store, request)
            .map(SubmitState::Owned)
            .map_err(|_| "owned_sell_submit_state_changed");
    }
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
    if request.signal_id.starts_with("owner-buy:") {
        let intent = crate::execution_owner_buy_authority::request(
            store, request, &[EXECUTION_STATUS_CANARY_SIMULATED],
        )
        .map_err(|_| "owner_buy_submit_identity")?
        .ok_or("owner_buy_origin_missing")?;
        if store.execution_canary_unresolved_buy()
            .map_err(|_| "tiny_submit_state_unavailable")?
        {
            return Err(copybot_storage_core::EXECUTION_UNRESOLVED_BUY_REASON);
        }
        if store.execution_canary_receipt_submit_block_reason(
            &request.order_id, &request.token, "buy",
        ).map_err(|_| "tiny_submit_state_unavailable")?.is_some() {
            return Err("owner_buy_receipt_pending");
        }
        return Ok(SubmitState::OwnerTechnicalBuy { order, intent });
    }
    if request.signal_id.starts_with("owner-exit:") {
        let intent = crate::execution_owner_exit_authority::request(
            store, request, &[EXECUTION_STATUS_CANARY_SIMULATED],
        ).map_err(|_| "owner_exit_submit_identity")?
            .ok_or("owner_exit_origin_missing")?;
        if store.execution_canary_receipt_submit_block_reason(
            &request.order_id, &request.token, "sell",
        ).map_err(|_| "tiny_submit_state_unavailable")?.is_some() {
            return Err("owner_exit_receipt_pending");
        }
        return Ok(SubmitState::OwnerExit { order, intent });
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
    Ok(SubmitState::Legacy { order, signal })
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
    if let (SubmitState::OwnerTechnicalBuy { order: a, intent: x },
        SubmitState::OwnerTechnicalBuy { order: b, intent: y }) = (before, &after) {
        return if a == b && x == y { Ok(()) } else { Err("initial_sol_order_changed") };
    }
    if let (SubmitState::OwnerExit { order: a, intent: x },
        SubmitState::OwnerExit { order: b, intent: y }) = (before, &after) {
        return if a == b && x == y { Ok(()) } else { Err("owner_exit_order_changed") };
    }
    let (before_order, a, after_order, b) = match (before, &after) {
        (SubmitState::Owned(a), SubmitState::Owned(b)) if a == b => return Ok(()),
        (
            SubmitState::Legacy {
                order: a,
                signal: s,
            },
            SubmitState::Legacy {
                order: b,
                signal: t,
            },
        ) => (a, s, b, t),
        _ => return Err("initial_sol_order_changed"),
    };
    if before_order != after_order
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
