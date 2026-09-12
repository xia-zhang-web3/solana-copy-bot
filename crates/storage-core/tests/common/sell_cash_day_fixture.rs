#![allow(dead_code)]
#[path = "sell_settlement_fixture.rs"]
mod settlement;
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_storage_core::*;
use rusqlite::params;
pub use settlement::*;

pub fn time(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .unwrap()
        .with_timezone(&Utc)
}
pub fn as_of() -> DateTime<Utc> {
    time("2026-09-06T12:00:00Z")
}
pub fn first(db: &Db, at: DateTime<Utc>) -> Result<ExecutionCanaryCashSettlement> {
    let facts = db
        .store
        .load_execution_canary_receipt_facts(ORDER)?
        .unwrap();
    Ok(db
        .store
        .apply_execution_canary_sell_settlement(&facts, at)?
        .settlement)
}
pub fn prepare(db: &Db, id: &str, sold: u64, native: i128) -> Result<ExecutionCanaryReceiptFacts> {
    let mut fresh = db.facts(sold, native);
    fresh.order_id = id.into();
    fresh.tx_signature = format!("sig:{id}");
    prepare_identity(db, fresh, "tiny")
}
pub fn prepare_identity(
    db: &Db,
    fresh: ExecutionCanaryReceiptFacts,
    route: &str,
) -> Result<ExecutionCanaryReceiptFacts> {
    let id = &fresh.order_id;
    db.conn()?.execute(
        "INSERT INTO copy_signals(signal_id,wallet_id,token,side,notional_sol,ts,status)
        VALUES(?1,'leader',?2,?3,0,?4,'shadow_recorded')",
        params![id, fresh.token, fresh.side, db.now.to_rfc3339()],
    )?;
    db.conn()?.execute("INSERT INTO orders(order_id,signal_id,route,submit_ts,status,tx_signature,client_order_id,simulation_status,attempt,err_code)
        VALUES(?1,?1,?6,?2,?3,?4,?1,'passed',1,?5)",
        params![id,db.now.to_rfc3339(),EXECUTION_STATUS_CANARY_SUBMITTED,fresh.tx_signature,EXECUTION_ACCOUNTING_PENDING_REASON,route])?;
    db.store.mark_execution_canary_confirmed_unreconciled(
        id,
        &ExecutionCanaryReceiptProof {
            tx_signature: fresh.tx_signature.clone(),
            wallet_pubkey: fresh.wallet_pubkey.clone(),
            token: fresh.token.clone(),
            side: fresh.side.clone(),
            confirmation_status: "confirmed".into(),
            slot: Some(fresh.slot),
            confirmed_at: db.now,
            reason: "awaiting".into(),
        },
        db.now,
    )?;
    db.store
        .record_execution_canary_receipt_facts(&fresh, db.now)?;
    Ok(fresh)
}
pub fn next(
    db: &Db,
    id: &str,
    sold: u64,
    native: i128,
    at: DateTime<Utc>,
) -> Result<ExecutionCanaryCashSettlement> {
    let facts = prepare(db, id, sold, native)?;
    Ok(db
        .store
        .apply_execution_canary_sell_settlement(&facts, at)?
        .settlement)
}
pub fn view(db: &Db, at: DateTime<Utc>) -> Result<ExecutionCanarySellCashDay> {
    let before = snapshot(&db.conn()?)?;
    let store = SqliteStore::open_read_only(&db.path)?;
    let result = store.execution_canary_sell_cash_day(at)?;
    assert_eq!(snapshot(&db.conn()?)?, before, "read API changed data");
    assert!(result.full_day_cash_result_lamports.is_none());
    assert!(result.economic_pnl_lamports.is_none());
    assert_eq!(result.decomposition, "unresolved");
    Ok(result)
}
pub fn sums(
    db: &Db,
    at: DateTime<Utc>,
    count: u64,
    net: i128,
    gross: u128,
) -> Result<ExecutionCanarySellCashDay> {
    let v = view(db, at)?;
    assert_eq!(v.known_events.events, count);
    assert_eq!(
        v.known_events.signed_net_cash_result_lamports,
        net.to_string()
    );
    assert_eq!(
        v.known_events.gross_negative_cash_result_lamports,
        gross.to_string()
    );
    let json = serde_json::to_value(&v)?;
    assert!(json["known_events"]["signed_net_cash_result_lamports"].is_string());
    assert!(json["known_events"]["gross_negative_cash_result_lamports"].is_string());
    Ok(v)
}
// Import/legacy/invalid-state controls only; positive cash events use the atomic writer.
pub fn obligation(db: &Db, id: &str, side: &str, status: &str, sig: Option<&str>) -> Result<()> {
    db.conn()?.execute(
        "INSERT INTO copy_signals(signal_id,wallet_id,token,side,notional_sol,ts,status)
        VALUES(?1,'leader','other',?2,0,'not-an-event-date','shadow_recorded')",
        params![id, side],
    )?;
    db.conn()?.execute("INSERT INTO orders(order_id,signal_id,route,submit_ts,confirm_ts,status,tx_signature,client_order_id,attempt)
        VALUES(?1,?1,'unrelated-route','bad-undated-submit','bad-undated-confirm',?2,?3,?1,1)",params![id,status,sig])?;
    Ok(())
}
pub fn legacy_fill(db: &Db, id: &str) -> Result<()> {
    db.conn()?.execute(
        "INSERT INTO fills(order_id,token,qty,avg_price,fee) VALUES(?1,'other',1,999,999)",
        [id],
    )?;
    Ok(())
}
