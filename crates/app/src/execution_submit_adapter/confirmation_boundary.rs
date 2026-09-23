use super::{
    build_confirmation_request_from_order, fetch_rpc_signature_confirmation,
    record_confirmed_fill_accounting_and_status,
    rpc_confirmed_fill::confirmed_fill_from_facts,
    rpc_receipt_facts::{fetch_confirmed_receipt_facts, ReceiptTransactionFailed},
    ExecutionConfirmationTrackerOutcome,
};
use anyhow::{anyhow, ensure, Result};
use chrono::{DateTime, Utc};
use copybot_storage_core::{
    ExecutionCanaryReceiptProof, SqliteStore, EXECUTION_STATUS_CANARY_CONFIRMED,
    EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED,
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
    pub(crate) pnl_sol: Option<f64>,
    pub(crate) cash_settlement: Option<copybot_storage_core::ExecutionCanaryCashSettlement>,
}

pub(crate) async fn record_execution_rpc_confirmation_boundary(
    store: &SqliteStore,
    http: &reqwest::Client,
    rpc_url: &str,
    order_id: &str,
    wallet_pubkey: &str,
    now: DateTime<Utc>,
    timeout_ms: u64,
) -> Result<ExecutionConfirmationBoundaryOutcome> {
    #[cfg(test)]
    return record_execution_rpc_confirmation_boundary_inner(
        store, http, rpc_url, order_id, wallet_pubkey, now, timeout_ms, None,
    )
    .await;
    #[cfg(not(test))]
    record_execution_rpc_confirmation_boundary_inner(
        store, http, rpc_url, order_id, wallet_pubkey, now, timeout_ms,
    )
    .await
}

#[cfg(test)]
pub(crate) async fn record_execution_rpc_confirmation_boundary_mock(
    store: &SqliteStore,
    http: &reqwest::Client,
    rpc_url: &str,
    order_id: &str,
    wallet_pubkey: &str,
    now: DateTime<Utc>,
    timeout_ms: u64,
    mock: &crate::execution_canary_route::NativeBuyMockIo,
) -> Result<ExecutionConfirmationBoundaryOutcome> {
    record_execution_rpc_confirmation_boundary_inner(
        store, http, rpc_url, order_id, wallet_pubkey, now, timeout_ms, Some(mock),
    )
    .await
}

async fn record_execution_rpc_confirmation_boundary_inner(
    store: &SqliteStore,
    http: &reqwest::Client,
    rpc_url: &str,
    order_id: &str,
    wallet_pubkey: &str,
    now: DateTime<Utc>,
    timeout_ms: u64,
    #[cfg(test)] mock: Option<&crate::execution_canary_route::NativeBuyMockIo>,
) -> Result<ExecutionConfirmationBoundaryOutcome> {
    // Historical fills are immutable, including those created by older accounting code.
    if store.execution_canary_fill_exists(order_id)? {
        store.validate_execution_canary_cash_settlement_replay(order_id, wallet_pubkey)?;
        return Ok(ExecutionConfirmationBoundaryOutcome {
            confirmed: 1,
            cash_settlement: store.load_execution_canary_cash_settlement(order_id)?,
            reason: Some("receipt_already_accounted".into()),
            ..Default::default()
        });
    }
    store.visit_execution_canary_reconciliation(order_id, wallet_pubkey, now)?;
    let order = store
        .load_execution_canary_order(order_id)?
        .ok_or_else(|| anyhow!("receipt order missing"))?;
    let request = build_confirmation_request_from_order(store, order_id, now)?;
    let mut proof = if let Some(proof) = store.load_execution_canary_receipt_proof(order_id)? {
        ensure!(
            proof.tx_signature == request.tx_signature && proof.wallet_pubkey == wallet_pubkey,
            "stored receipt signature or configured wallet mismatch"
        );
        proof
    } else {
        let (token, side) = store.execution_receipt_token_side(order_id)?;
        let (confirmation_status, slot, confirmed_at) = if order.status
            == EXECUTION_STATUS_CANARY_CONFIRMED
        {
            (
                "legacy_confirmed".to_string(),
                None,
                order.confirm_ts.unwrap_or(now),
            )
        } else {
            ensure!(
                order.status != EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED,
                "durable receipt proof missing"
            );
            #[cfg(test)]
            let status = if let Some(mock) = mock {
                mock.count(|counts| counts.confirmation += 1);
                tokio::task::yield_now().await;
                super::rpc_confirmation::rpc_signature_confirmation_from_json(
                    &request.tx_signature,
                    now,
                    mock.confirmation.clone(),
                )
            } else {
                fetch_rpc_signature_confirmation(http, rpc_url, &request, now, timeout_ms).await
            };
            #[cfg(not(test))]
            let status = fetch_rpc_signature_confirmation(http, rpc_url, &request, now, timeout_ms).await;
            match status {
                Ok(ExecutionConfirmationTrackerOutcome::Pending { reason, .. }) => {
                    return Ok(ExecutionConfirmationBoundaryOutcome {
                        pending: 1,
                        reason: Some(reason),
                        ..Default::default()
                    })
                }
                Ok(ExecutionConfirmationTrackerOutcome::Confirmed(proof)) => {
                    (proof.confirmation_status, proof.slot, proof.confirmed_at)
                }
                Err(error) if error.is::<super::rpc_confirmation::SignatureTransactionFailed>() => {
                    let failure = error
                        .downcast_ref::<super::rpc_confirmation::SignatureTransactionFailed>()
                        .unwrap();
                    #[cfg(test)]
                    let mock_receipt = mock.map(|value| &value.receipt);
                    #[cfg(not(test))]
                    let mock_receipt = None;
                    return super::failed_expense::detected(
                        store,
                        http,
                        rpc_url,
                        order_id,
                        wallet_pubkey,
                        "signature_status",
                        &failure.commitment,
                        failure.slot,
                        &failure.error,
                        now,
                        timeout_ms,
                        mock_receipt,
                    )
                    .await;
                }
                Err(_) => {
                    return Ok(ExecutionConfirmationBoundaryOutcome {
                        pending: 1,
                        reason: Some("signature_rpc_unavailable".into()),
                        ..Default::default()
                    })
                }
            }
        };
        ExecutionCanaryReceiptProof {
            tx_signature: request.tx_signature,
            wallet_pubkey: wallet_pubkey.into(),
            token,
            side: side.to_ascii_lowercase(),
            confirmation_status,
            slot,
            confirmed_at,
            reason: "receipt_not_fetched".into(),
        }
    };
    // Persist network confirmation BEFORE any receipt I/O or accounting attempt.
    store.mark_execution_canary_confirmed_unreconciled(order_id, &proof, now)?;
    #[cfg(test)]
    let receipt = if let Some(mock) = mock {
        mock.count(|counts| counts.receipt += 1);
        tokio::task::yield_now().await;
        super::rpc_receipt_facts::facts_from_transaction_json(order_id, &proof, &mock.receipt)
    } else {
        fetch_confirmed_receipt_facts(http, rpc_url, order_id, &proof, timeout_ms).await
    };
    #[cfg(not(test))]
    let receipt = fetch_confirmed_receipt_facts(http, rpc_url, order_id, &proof, timeout_ms).await;
    let bundle =
        match receipt {
            Ok(facts) => facts,
            Err(error) if error.is::<ReceiptTransactionFailed>() => {
                let failure = error.downcast_ref::<ReceiptTransactionFailed>().unwrap();
                if store
                    .load_execution_canary_receipt_facts(order_id)?
                    .is_some()
                {
                    store.detect_failed_expense(
                        order_id,
                        wallet_pubkey,
                        "receipt_meta",
                        "confirmed",
                        Some(failure.slot),
                        &failure.receipt["result"]["meta"]["err"],
                        now,
                    )?;
                    proof.reason = "receipt_facts_execution_conflict".into();
                    store.mark_execution_canary_confirmed_unreconciled(order_id, &proof, now)?;
                    return Ok(pending_accounting(&proof));
                }
                return super::failed_expense::detected(
                    store,
                    http,
                    rpc_url,
                    order_id,
                    wallet_pubkey,
                    "receipt_meta",
                    "confirmed",
                    Some(failure.slot),
                    &failure.receipt["result"]["meta"]["err"],
                    now,
                    timeout_ms,
                    Some(&failure.receipt),
                )
                .await;
            }
            Err(error) => {
                proof.reason = error.to_string();
                store.mark_execution_canary_confirmed_unreconciled(order_id, &proof, now)?;
                return Ok(pending_accounting(&proof));
            }
        };
    let facts = &bundle.facts;
    if store.success_conflicts_with_failed_expense(&facts.tx_signature)? {
        proof.reason = "receipt_failed_expense_conflict".into();
        store.mark_execution_canary_confirmed_unreconciled(order_id, &proof, now)?;
        return Ok(pending_accounting(&proof));
    }
    // No fill conversion or accounting may precede durable exact observations.
    if let Err(error) = store.record_receipt_observation_bundle(&bundle, now) {
        proof.reason = if error.to_string() == "native_observation_conflict" {
            "native_observation_conflict"
        } else {
            "receipt_facts_write_failed"
        }
        .into();
        store.mark_execution_canary_confirmed_unreconciled(order_id, &proof, now)?;
        if copybot_storage_core::is_fatal_sqlite_anyhow_error(&error)
            || error.downcast_ref::<rusqlite::Error>().is_some_and(|e| !matches!(e,
                rusqlite::Error::SqliteFailure(code, _) if code.code == rusqlite::ErrorCode::ConstraintViolation))
        {
            return Err(error);
        }
        // A new pending transaction and readback are required before allowing
        // an unrelated mint's exit after a local observation write rejection.
        ensure!(
            store
                .load_execution_canary_receipt_proof(order_id)?
                .as_ref()
                == Some(&proof)
                && store
                    .load_execution_canary_order(order_id)?
                    .is_some_and(|o| o.status == EXECUTION_STATUS_CANARY_CONFIRMED_UNRECONCILED)
                && !store.execution_canary_fill_exists(order_id)?
                && store.execution_canary_accounting_pending()?
                && store.execution_canary_token_accounting_pending(&proof.token)?,
            "receipt observation failure pending proof not preserved"
        );
        return Ok(pending_accounting(&proof));
    }
    if facts.side == "sell" {
        return super::receipt_sell_accounting::account(store, &facts, &mut proof, now);
    }
    let fill = match confirmed_fill_from_facts(&facts, &proof) {
        Ok(fill) => fill,
        Err(error) => {
            proof.reason = error.to_string();
            store.mark_execution_canary_confirmed_unreconciled(order_id, &proof, now)?;
            return Ok(pending_accounting(&proof));
        }
    };
    let (_, fill) = match record_confirmed_fill_accounting_and_status(
        store,
        fill.fill,
        proof.confirmed_at,
        Some(fill.net_lamports),
    ) {
        Ok(result) => result,
        Err(error) if error.is::<copybot_storage_core::ReceiptAccountingUnsupported>() => {
            proof.reason = error
                .downcast_ref::<copybot_storage_core::ReceiptAccountingUnsupported>()
                .expect("matched accounting error")
                .0
                .to_string();
            store.mark_execution_canary_confirmed_unreconciled(order_id, &proof, now)?;
            return Ok(pending_accounting(&proof));
        }
        Err(error) => {
            proof.reason = "receipt_accounting_write_failed".into();
            store.mark_execution_canary_confirmed_unreconciled(order_id, &proof, now)?;
            // Surface the DB failure; durable proof and position/fill transaction remain intact.
            return Err(error);
        }
    };
    Ok(ExecutionConfirmationBoundaryOutcome {
        confirmed: 1,
        confirmation_status: Some(proof.confirmation_status),
        slot: proof.slot,
        buy_opened: fill.buy_opened,
        buy_existing: fill.buy_existing,
        sell_closed: fill.sell_closed,
        sell_partial: fill.sell_partial,
        sell_dust_closed: fill.sell_dust_closed,
        sell_no_position: fill.sell_no_position,
        close_status: fill.close_status,
        closed_qty: fill.closed_qty,
        pnl_sol: Some(fill.pnl_sol),
        ..Default::default()
    })
}

pub(super) fn pending_accounting(
    proof: &ExecutionCanaryReceiptProof,
) -> ExecutionConfirmationBoundaryOutcome {
    ExecutionConfirmationBoundaryOutcome {
        pending: 1,
        reason: Some(proof.reason.clone()),
        confirmation_status: Some(proof.confirmation_status.clone()),
        slot: proof.slot,
        ..Default::default()
    }
}
