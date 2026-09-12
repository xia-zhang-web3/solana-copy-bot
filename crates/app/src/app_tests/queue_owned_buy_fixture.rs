//! Synthetic historical BUY precondition for queue tests, not a daemon admission claim.
//! The parent predates the pending queue: seed its exact durable identity, then use
//! canonical receipt/fill APIs. Never delete reservations or rearm an experiment.
use super::{buy_retry_queue_fixture::SELL_TOKEN, fresh_buy_size_runtime_fixture::RuntimeFixture};
use anyhow::Result;
use chrono::Duration;
use copybot_core_types::{Lamports, SignedLamports, TokenQuantity};
use copybot_storage_core::*;
use rusqlite::{params, Connection};
pub(super) fn seed(f: &RuntimeFixture) -> Result<()> {
    let at = f.now - Duration::seconds(30);
    let id = super::receipt_reconciliation_fixture::add_order(
        &f.store,
        "b12-owned-buy",
        "buy",
        SELL_TOKEN,
        at,
        false,
    )?;
    let experiment = f.config.tiny_experiment.id.as_deref().unwrap();
    let wallet = &f.config.canary_wallet_pubkey;
    f.store.activate_tiny_experiment(experiment, wallet, at)?;
    let order = f.store.load_execution_canary_order(&id)?.unwrap();
    let signature = "synthetic-owned-buy-receipt";
    let sql = Connection::open(&f.db_path)?;
    sql.execute("INSERT INTO execution_canary_dispatch(order_id,signal_id,client_order_id,route,attempt,wallet,token,side,tx_signature,transaction_sha256,message_sha256,claimed_at) VALUES(?1,?2,?3,?4,?5,?6,?7,'buy',?8,?9,?10,?11)", params![id,order.signal_id,order.client_order_id,order.route,order.attempt,wallet,SELL_TOKEN,signature,"a".repeat(64),"b".repeat(64),at.to_rfc3339()])?;
    sql.execute("INSERT INTO execution_tiny_reservations(order_id,experiment_id,tx_signature,wallet,side,message_sha256,transaction_sha256,buy_lamports,fee_bound,priority_fee,fee_slot,reserved_at) VALUES(?1,?2,?3,?4,'buy',?5,?6,8993000,100000,2000,'42',?7)",params![id,experiment,signature,wallet,"b".repeat(64),"a".repeat(64),at.to_rfc3339()])?;
    sql.execute("UPDATE execution_tiny_experiment SET buy_order_id=?1,token=?2,position_id=?3 WHERE singleton=1",params![id,SELL_TOKEN,format!("exec-canary-pos:{id}")])?;
    f.store
        .mark_execution_canary_submitted(&id, at, signature)?;
    f.store.mark_execution_canary_confirmed_unreconciled(
        &id,
        &ExecutionCanaryReceiptProof {
            tx_signature: signature.into(),
            wallet_pubkey: wallet.clone(),
            token: SELL_TOKEN.into(),
            side: "buy".into(),
            confirmation_status: "confirmed".into(),
            slot: Some(42),
            confirmed_at: at,
            reason: "synthetic_parent".into(),
        },
        at,
    )?;
    f.store.record_execution_canary_receipt_facts(
        &ExecutionCanaryReceiptFacts {
            order_id: id.clone(),
            tx_signature: signature.into(),
            wallet_pubkey: wallet.clone(),
            token: SELL_TOKEN.into(),
            side: "buy".into(),
            slot: 42,
            wallet_native_pre: Lamports::new(100_000_000),
            wallet_native_post: Lamports::new(91_000_000),
            wallet_native_delta: SignedLamports::new(-9_000_000),
            transaction_fee: Some(Lamports::new(7000)),
            fee_coverage: ReceiptFeeCoverage::Known,
            fee_payer: Some(wallet.clone()),
            token_delta: Some(ReceiptTokenDelta {
                raw: 100,
                decimals: 0,
            }),
            token_coverage: ReceiptTokenCoverage::PairedBalances,
            token_coverage_reason: None,
            wsol_coverage: ReceiptWsolCoverage::Unresolved,
            block_time: Some(at.timestamp()),
            decomposition: ReceiptDecomposition::Unresolved,
        },
        at,
    )?;
    f.store.confirm_execution_canary_buy_fill(
        &id,
        SELL_TOKEN,
        100.0,
        Some(TokenQuantity::new(100, 0)),
        0.009,
        at,
        at,
        Some(Lamports::new(9_000_000)),
    )?;
    let pinned = f.store.load_tiny_experiment(at)?.unwrap();
    let position = f
        .store
        .load_execution_canary_open_position(SELL_TOKEN)?
        .unwrap();
    assert_eq!(pinned.buy_order_id.as_deref(), Some(id.as_str()));
    assert_eq!(
        pinned.position_id.as_deref(),
        Some(position.position_id.as_str())
    );
    assert!(f.store.execution_canary_fill_exists(&id)?);
    Ok(())
}
