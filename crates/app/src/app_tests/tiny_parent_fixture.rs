//! Explicit synthetic historical parent, not evidence of current BUY admission.
//! Build canonical receipt/fill lineage and one durable reservation before testing
//! SELL recovery. Imported positions may coexist; no runtime hold is removed.
use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::{CopySignalRow, Lamports, SignedLamports, TokenQuantity};
use copybot_storage_core::*;
use rusqlite::{params, Connection};

pub(super) fn seed(
    store: &SqliteStore,
    sql: &Connection,
    id: &str,
    leader: &str,
    token: &str,
    wallet: &str,
    quantity: TokenQuantity,
    cost: u64,
    at: DateTime<Utc>,
) -> Result<String> {
    ensure!(
        cost > 50 && cost <= TINY_BUY_LAMPORTS,
        "synthetic parent cost bounds"
    );
    ensure!(
        store.load_tiny_experiment(at)?.is_none(),
        "parent must not rearm experiment"
    );
    let signal = CopySignalRow {
        signal_id: id.into(),
        wallet_id: leader.into(),
        token: token.into(),
        side: "buy".into(),
        notional_sol: cost as f64 / 1e9,
        notional_lamports: Some(Lamports::new(cost)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
        ts: at,
        status: "shadow_recorded".into(),
    };
    store.insert_copy_signal(&signal)?;
    let order = store.reserve_execution_canary_order(id, "tiny", at)?.order;
    let id = order.order_id;
    let signature = format!("synthetic-parent:{id}");
    store.mark_execution_canary_built(&id, at)?;
    store.mark_execution_canary_simulated(&id, at, EXECUTION_SIMULATION_STATUS_PASSED, None)?;
    store.activate_tiny_experiment("local-variant-a", wallet, at)?;
    let tx_hash = "a".repeat(64);
    let message_hash = "b".repeat(64);
    sql.execute("INSERT INTO execution_canary_dispatch(order_id,signal_id,client_order_id,route,attempt,wallet,token,side,tx_signature,transaction_sha256,message_sha256,claimed_at) VALUES(?1,?2,?3,?4,?5,?6,?7,'buy',?8,?9,?10,?11)", params![id,order.signal_id,order.client_order_id,order.route,order.attempt,wallet,token,signature,tx_hash,message_hash,at.to_rfc3339()])?;
    sql.execute("INSERT INTO execution_tiny_reservations(order_id,experiment_id,tx_signature,wallet,side,message_sha256,transaction_sha256,buy_lamports,fee_bound,priority_fee,fee_slot,reserved_at) VALUES(?1,'local-variant-a',?2,?3,'buy',?4,?5,?6,50,0,'42',?7)", params![id,signature,wallet,message_hash,tx_hash,cost-50,at.to_rfc3339()])?;
    sql.execute("UPDATE execution_tiny_experiment SET buy_order_id=?1,token=?2,position_id=?3 WHERE singleton=1", params![id,token,format!("exec-canary-pos:{id}")])?;
    store.mark_execution_canary_submitted(&id, at, &signature)?;
    store.mark_execution_canary_confirmed_unreconciled(
        &id,
        &ExecutionCanaryReceiptProof {
            tx_signature: signature.clone(),
            wallet_pubkey: wallet.into(),
            token: token.into(),
            side: "buy".into(),
            confirmation_status: "confirmed".into(),
            slot: Some(42),
            confirmed_at: at,
            reason: "synthetic_parent".into(),
        },
        at,
    )?;
    store.record_execution_canary_receipt_facts(
        &ExecutionCanaryReceiptFacts {
            order_id: id.clone(),
            tx_signature: signature,
            wallet_pubkey: wallet.into(),
            token: token.into(),
            side: "buy".into(),
            slot: 42,
            wallet_native_pre: Lamports::new(100_000_000),
            wallet_native_post: Lamports::new(100_000_000 - cost),
            wallet_native_delta: SignedLamports::new(-i128::from(cost)),
            transaction_fee: Some(Lamports::new(50)),
            fee_coverage: ReceiptFeeCoverage::Known,
            fee_payer: Some(wallet.into()),
            token_delta: Some(ReceiptTokenDelta {
                raw: i128::from(quantity.raw()),
                decimals: quantity.decimals(),
            }),
            token_coverage: ReceiptTokenCoverage::PairedBalances,
            token_coverage_reason: None,
            wsol_coverage: ReceiptWsolCoverage::Unresolved,
            block_time: Some(at.timestamp()),
            decomposition: ReceiptDecomposition::Unresolved,
        },
        at,
    )?;
    store.confirm_execution_canary_buy_fill(
        &id,
        token,
        quantity.raw() as f64 / 10f64.powi(quantity.decimals().into()),
        Some(quantity),
        cost as f64 / 1e9,
        at,
        at,
        Some(Lamports::new(cost)),
    )?;
    ensure!(
        store.execution_canary_fill_exists(&id)?,
        "canonical parent fill"
    );
    Ok(id)
}
