use super::association_fixture::Db;
use anyhow::Result;
use copybot_core_types::{CopySignalRow, Lamports, SignedLamports, TokenQuantity};
use copybot_storage_core::*;
use serde_json::Value;
pub fn seed(db: &Db, m: &Value) -> Result<()> {
    let text = |v: &str, k: &str| m[v][k].as_str().unwrap().to_owned();
    let token = text("our", "token_out");
    let leader = text("source", "signer");
    let source = text("source", "signature");
    let our = text("our", "signature");
    let wallet = text("our", "signer");
    let signal = format!("shadow:{source}:{leader}:buy:{token}");
    let at = m["oracle"]["buy_time"].as_str().unwrap().parse()?;
    let raw = m["our"]["exact_amounts"]["amount_out_raw"]
        .as_str()
        .unwrap()
        .parse::<u64>()?;
    let decimals = m["our"]["exact_amounts"]["amount_out_decimals"]
        .as_u64()
        .unwrap() as u8;
    let qty = TokenQuantity::new(raw, decimals);
    let pre = m["our"]["receipt_native_pre"].as_u64().unwrap();
    let post = m["our"]["receipt_native_post"].as_u64().unwrap();
    let debit = pre.checked_sub(post).unwrap();
    assert!(debit > 0);
    let cost = debit as f64 / 1_000_000_000.0;
    db.store.insert_copy_signal(&CopySignalRow {
        signal_id: signal.clone(),
        wallet_id: leader,
        token: token.clone(),
        side: "buy".into(),
        notional_sol: cost,
        notional_lamports: Some(Lamports::new(debit)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
        ts: at,
        status: "shadow_recorded".into(),
    })?;
    let id = db
        .store
        .reserve_execution_canary_order(&signal, "tiny", at)?
        .order
        .order_id;
    db.store.mark_execution_canary_built(&id, at)?;
    db.store
        .mark_execution_canary_simulated(&id, at, EXECUTION_SIMULATION_STATUS_PASSED, None)?;
    db.store.mark_execution_canary_submitted(&id, at, &our)?;
    let slot = m["our"]["slot"].as_u64().unwrap();
    db.store.mark_execution_canary_confirmed_unreconciled(
        &id,
        &ExecutionCanaryReceiptProof {
            tx_signature: our.clone(),
            wallet_pubkey: wallet.clone(),
            token: token.clone(),
            side: "buy".into(),
            confirmation_status: "confirmed".into(),
            slot: Some(slot),
            confirmed_at: at,
            reason: "receipt_not_fetched".into(),
        },
        at,
    )?;
    db.store.record_execution_canary_receipt_facts(
        &ExecutionCanaryReceiptFacts {
            order_id: id.clone(),
            tx_signature: our,
            wallet_pubkey: wallet.clone(),
            token: token.clone(),
            side: "buy".into(),
            slot,
            wallet_native_pre: Lamports::new(pre),
            wallet_native_post: Lamports::new(post),
            wallet_native_delta: SignedLamports::new(-i128::from(debit)),
            transaction_fee: Some(Lamports::new(m["our"]["receipt_fee"].as_u64().unwrap())),
            fee_coverage: ReceiptFeeCoverage::Known,
            fee_payer: Some(wallet),
            token_delta: Some(ReceiptTokenDelta {
                raw: i128::from(raw),
                decimals,
            }),
            token_coverage: ReceiptTokenCoverage::PairedBalances,
            token_coverage_reason: None,
            wsol_coverage: ReceiptWsolCoverage::Unresolved,
            block_time: None,
            decomposition: ReceiptDecomposition::Unresolved,
        },
        at,
    )?;
    db.store.confirm_execution_canary_buy_fill(
        &id,
        &token,
        m["our"]["amount_out"].as_f64().unwrap(),
        Some(qty),
        cost,
        at,
        at,
        Some(Lamports::new(debit)),
    )?;
    Ok(())
}
