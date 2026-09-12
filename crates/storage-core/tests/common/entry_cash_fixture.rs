#![allow(dead_code)]
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_core_types::{Lamports, SignedLamports};
use copybot_storage_core::*;
use rusqlite::{params, Connection};

pub fn inventory(
    conn: &Connection,
    id: &str,
    token: &str,
    raw: u64,
    cost: i64,
    at: DateTime<Utc>,
) -> Result<()> {
    conn.execute("INSERT INTO positions(position_id,token,qty,cost_sol,opened_ts,state,accounting_bucket,qty_raw,qty_decimals,cost_lamports,pnl_lamports)
        VALUES(?1,?2,?3,?4,?5,'open',?6,?7,0,?8,0)", params![id,token,raw as f64,cost as f64/1e9,at.to_rfc3339(),EXECUTION_CANARY_POSITION_ACCOUNTING_BUCKET,raw.to_string(),cost])?;
    Ok(())
}

pub fn claim(
    store: &SqliteStore,
    conn: &Connection,
    id: &str,
    signature: &str,
    wallet: &str,
    token: &str,
    sold: u64,
    cash: i128,
    at: DateTime<Utc>,
) -> Result<ExecutionCanaryReceiptFacts> {
    conn.execute(
        "INSERT INTO copy_signals(signal_id,wallet_id,token,side,notional_sol,ts,status)
        VALUES(?1,'leader',?2,'sell',0,?3,'shadow_recorded')",
        params![id, token, at.to_rfc3339()],
    )?;
    conn.execute("INSERT INTO orders(order_id,signal_id,route,submit_ts,status,tx_signature,client_order_id,simulation_status,attempt,err_code)
        VALUES(?1,?1,'tiny',?2,?3,?4,?1,'passed',1,?5)",params![id,at.to_rfc3339(),EXECUTION_STATUS_CANARY_SUBMITTED,signature,EXECUTION_ACCOUNTING_PENDING_REASON])?;
    store.mark_execution_canary_confirmed_unreconciled(
        id,
        &ExecutionCanaryReceiptProof {
            tx_signature: signature.into(),
            wallet_pubkey: wallet.into(),
            token: token.into(),
            side: "sell".into(),
            confirmation_status: "confirmed".into(),
            slot: Some(42),
            confirmed_at: at,
            reason: "awaiting".into(),
        },
        at,
    )?;
    let magnitude = u64::try_from(cash.unsigned_abs())?;
    let facts = ExecutionCanaryReceiptFacts {
        order_id: id.into(),
        tx_signature: signature.into(),
        wallet_pubkey: wallet.into(),
        token: token.into(),
        side: "sell".into(),
        slot: 42,
        wallet_native_pre: Lamports::new(if cash < 0 { magnitude } else { 0 }),
        wallet_native_post: Lamports::new(if cash < 0 { 0 } else { magnitude }),
        wallet_native_delta: SignedLamports::new(cash),
        transaction_fee: None,
        fee_coverage: ReceiptFeeCoverage::Missing,
        fee_payer: None,
        token_delta: Some(ReceiptTokenDelta {
            raw: -i128::from(sold),
            decimals: 0,
        }),
        token_coverage: ReceiptTokenCoverage::PairedBalances,
        token_coverage_reason: None,
        wsol_coverage: ReceiptWsolCoverage::Unresolved,
        block_time: Some(at.timestamp()),
        decomposition: ReceiptDecomposition::Unresolved,
    };
    store.record_execution_canary_receipt_facts(&facts, at)?;
    Ok(facts)
}

pub fn settle(
    store: &SqliteStore,
    conn: &Connection,
    id: &str,
    signature: &str,
    wallet: &str,
    token: &str,
    sold: u64,
    cash: i128,
    at: DateTime<Utc>,
) -> Result<ExecutionCanaryCashSettlement> {
    let facts = claim(store, conn, id, signature, wallet, token, sold, cash, at)?;
    let result = store.apply_execution_canary_sell_settlement(&facts, at)?;
    assert!(!result.already_accounted);
    Ok(result.settlement)
}
