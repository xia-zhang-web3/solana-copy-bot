use super::association_fixture as f;
use super::b93_fixture::token;
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_core_types::{CopySignalRow, Lamports, SignedLamports};
use copybot_storage_core::*;
use serde_json::Value;
pub fn submitted_receipt(
    db: &f::Db,
    m: &Value,
    name: &str,
    sold: u64,
    sig: &str,
) -> Result<ExecutionCanaryReceiptFacts> {
    let time: DateTime<Utc> = "2026-09-09T00:00:05Z".parse()?;
    let sig = sig.to_owned();
    let signal = format!("b93-{name}");
    db.store.insert_copy_signal(&CopySignalRow {
        signal_id: signal.clone(),
        wallet_id: m["source"]["signer"].as_str().unwrap().into(),
        token: token(m).into(),
        side: "sell".into(),
        notional_sol: 0.1,
        notional_lamports: Some(Lamports::new(100_000_000)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
        ts: time,
        status: "shadow_recorded".into(),
    })?;
    // Settlement fixture precondition, not a parallel daemon admission claim.
    let id = db
        .store
        .reserve_execution_canary_sell_order_unless_token_in_flight(
            &signal,
            super::source_write_off_fixture::ROUTE,
            time,
        )?
        .order
        .order_id;
    db.store.mark_execution_canary_built(&id, time)?;
    db.store.mark_execution_canary_simulated(
        &id,
        time,
        EXECUTION_SIMULATION_STATUS_PASSED,
        None,
    )?;
    db.store.mark_execution_canary_submitted(&id, time, &sig)?;
    let wallet = m["our"]["signer"].as_str().unwrap();
    Ok(ExecutionCanaryReceiptFacts {
        order_id: id,
        tx_signature: sig,
        wallet_pubkey: wallet.into(),
        token: token(m).into(),
        side: "sell".into(),
        slot: 140,
        wallet_native_pre: Lamports::new(1_000_000_000),
        wallet_native_post: Lamports::new(1_100_000_000),
        wallet_native_delta: SignedLamports::new(100_000_000),
        transaction_fee: Some(Lamports::new(10000)),
        fee_coverage: ReceiptFeeCoverage::Known,
        fee_payer: Some(wallet.into()),
        token_delta: Some(ReceiptTokenDelta {
            raw: -i128::from(sold),
            decimals: 3,
        }),
        token_coverage: ReceiptTokenCoverage::PairedBalances,
        token_coverage_reason: None,
        wsol_coverage: ReceiptWsolCoverage::Unresolved,
        block_time: None,
        decomposition: ReceiptDecomposition::Unresolved,
    })
}
