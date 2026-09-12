use super::confirmation_boundary::{pending_accounting, ExecutionConfirmationBoundaryOutcome};
use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use copybot_storage_core::{ExecutionCanaryReceiptFacts, ExecutionCanaryReceiptProof, SqliteStore};

pub(super) fn account(
    store: &SqliteStore,
    facts: &ExecutionCanaryReceiptFacts,
    proof: &mut ExecutionCanaryReceiptProof,
    now: DateTime<Utc>,
) -> Result<ExecutionConfirmationBoundaryOutcome> {
    match store.apply_execution_canary_sell_settlement(facts, now) {
        Ok(result) => {
            let settlement = result.settlement;
            let partial = settlement.remaining_quantity.raw() != 0;
            Ok(ExecutionConfirmationBoundaryOutcome {
                confirmed: 1,
                confirmation_status: Some(proof.confirmation_status.clone()),
                slot: proof.slot,
                sell_closed: usize::from(!result.already_accounted),
                sell_partial: usize::from(partial && !result.already_accounted),
                close_status: Some(if partial { "partial" } else { "closed" }.into()),
                closed_qty: settlement.sold_quantity.as_f64(),
                cash_settlement: Some(settlement),
                ..Default::default()
            })
        }
        Err(error) => {
            let unsupported = error.is::<copybot_storage_core::SellSettlementUnsupported>()
                || error.is::<copybot_storage_core::ReceiptFactsIdentityRejection>();
            proof.reason = if let Some(reason) =
                error.downcast_ref::<copybot_storage_core::SellSettlementUnsupported>()
            {
                format!("receipt_sell_unsupported:{reason:?}")
            } else if let Some(reason) =
                error.downcast_ref::<copybot_storage_core::ReceiptFactsIdentityRejection>()
            {
                format!("receipt_sell_identity:{reason:?}")
            } else {
                "receipt_accounting_write_failed".into()
            };
            store.mark_execution_canary_confirmed_unreconciled(&facts.order_id, proof, now)?;
            if unsupported {
                Ok(pending_accounting(proof))
            } else if is_local_write_rejection(&error) {
                verify_pending_after_rollback(store, &facts.order_id, proof)?;
                crate::telemetry::record_sell_accounting_write_failure(&facts.order_id);
                Ok(ExecutionConfirmationBoundaryOutcome {
                    error: Some(proof.reason.clone()),
                    ..pending_accounting(proof)
                })
            } else {
                Err(error)
            }
        }
    }
}

fn is_local_write_rejection(error: &anyhow::Error) -> bool {
    // Only a transaction-local constraint rejection is eligible. I/O, busy,
    // corrupt/schema failures and unclassified errors still stop the tick.
    !copybot_storage_core::is_fatal_sqlite_anyhow_error(error)
        && matches!(error.downcast_ref::<rusqlite::Error>(),
            Some(rusqlite::Error::SqliteFailure(code, _))
                if code.code == rusqlite::ErrorCode::ConstraintViolation)
}

fn verify_pending_after_rollback(
    store: &SqliteStore,
    order_id: &str,
    proof: &ExecutionCanaryReceiptProof,
) -> Result<()> {
    // The preceding pending write starts and commits a NEW IMMEDIATE transaction:
    // it cannot succeed with an unrolled-back accounting transaction. Read back
    // the proof as well, since a trigger can silently ignore an attempted write.
    ensure!(
        store
            .load_execution_canary_receipt_proof(order_id)?
            .as_ref()
            == Some(proof),
        "SELL accounting failure pending proof not preserved"
    );
    // Reuse the read-only planner's durable facts/identity/no-fill/pending checks.
    ensure!(
        matches!(
            store.plan_execution_canary_sell_settlement(order_id)?,
            copybot_storage_core::ExecutionCanarySellSettlement::Ready(_)
        ),
        "SELL accounting failure no longer has a pending settlement"
    );
    ensure!(
        store.execution_canary_accounting_pending()?
            && store.execution_canary_token_accounting_pending(&proof.token)?,
        "SELL accounting failure risk blockers not preserved"
    );
    Ok(())
}
