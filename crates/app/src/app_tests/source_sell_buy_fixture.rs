use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_core_types::{CopySignalRow, Lamports, SignedLamports, TokenQuantity};
use copybot_storage_core::*;

pub(super) fn proven_buy(
    store: &SqliteStore,
    id: &str,
    source: &str,
    now: DateTime<Utc>,
) -> Result<String> {
    let wallet = source;
    let side = "buy";
    let token = "mint";
    let execution_wallet = "execution-wallet";
    let signature = format!("receipt:{id}");
    store.insert_copy_signal(&CopySignalRow {
        signal_id: id.into(),
        wallet_id: wallet.into(),
        token: token.into(),
        side: side.into(),
        notional_sol: 0.000001,
        notional_lamports: Some(Lamports::new(1000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
        ts: now,
        status: "shadow_recorded".into(),
    })?;
    let id = store
        .reserve_execution_canary_order(id, "tiny", now)?
        .order
        .order_id;
    store.mark_execution_canary_built(&id, now)?;
    store.mark_execution_canary_simulated(&id, now, EXECUTION_SIMULATION_STATUS_PASSED, None)?;
    store.mark_execution_canary_submitted(&id, now, &signature.clone())?;
    store.mark_execution_canary_confirmed_unreconciled(
        &id,
        &ExecutionCanaryReceiptProof {
            tx_signature: signature.clone(),
            wallet_pubkey: execution_wallet.into(),
            token: token.into(),
            side: side.into(),
            confirmation_status: "confirmed".into(),
            slot: Some(42),
            confirmed_at: now,
            reason: "receipt_not_fetched".into(),
        },
        now,
    )?;
    store.record_execution_canary_receipt_facts(
        &ExecutionCanaryReceiptFacts {
            order_id: id.clone(),
            tx_signature: signature.clone(),
            wallet_pubkey: execution_wallet.into(),
            token: token.into(),
            side: side.into(),
            slot: 42,
            wallet_native_pre: Lamports::new(2000),
            wallet_native_post: Lamports::new(if side == "buy" { 1000 } else { 2500 }),
            wallet_native_delta: SignedLamports::new(if side == "buy" { -1000 } else { 500 }),
            transaction_fee: Some(Lamports::new(50)),
            fee_coverage: ReceiptFeeCoverage::Known,
            fee_payer: Some(execution_wallet.into()),
            token_delta: Some(ReceiptTokenDelta {
                raw: if side == "buy" { 7000 } else { -7000 },
                decimals: 3,
            }),
            token_coverage: ReceiptTokenCoverage::PairedBalances,
            token_coverage_reason: None,
            wsol_coverage: ReceiptWsolCoverage::Unresolved,
            block_time: Some(now.timestamp()),
            decomposition: ReceiptDecomposition::Unresolved,
        },
        now,
    )?;
    store.confirm_execution_canary_buy_fill(
        &id,
        token,
        7.0,
        Some(TokenQuantity::new(7000, 3)),
        0.000001,
        now,
        now,
        Some(Lamports::new(1000)),
    )?;
    Ok(id)
}
