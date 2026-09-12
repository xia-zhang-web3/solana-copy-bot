#![allow(dead_code)]
#[path = "buy_attribution_review_fixture.rs"]
mod base;
#[path = "sell_settlement_fixture.rs"]
mod rows;
use anyhow::Result;
pub use base::Db;
use copybot_core_types::{CopySignalRow, Lamports, TokenQuantity};
use copybot_storage_core::*;

#[derive(Clone, Copy, Debug)]
pub enum Api {
    Confirm,
    Confirmed,
}

pub fn database(lots: usize) -> Result<Db> {
    let db = Db::new()?;
    // Inventory is actual distinct BUY accounting, with known native cash basis.
    for i in 0..lots {
        let id = db.seed(&format!("inventory-{i}"), "leader", "buy")?;
        db.buy(&id)?;
    }
    Ok(db)
}

pub fn seed(db: &Db, name: &str, api: Api) -> Result<String> {
    let id = db.seed(name, "leader", "sell")?;
    if matches!(api, Api::Confirmed) {
        import_confirmed(db, &id)?;
    }
    Ok(id)
}

pub fn import_confirmed(db: &Db, id: &str) -> Result<()> {
    // Explicit already-confirmed/no-fill historical import after valid API setup.
    // This status setup is not a daemon lifecycle or completed accounting claim.
    db.conn()?.execute(
        "UPDATE orders SET status=?3,confirm_ts=?2 WHERE order_id=?1",
        rusqlite::params![id, db.now.to_rfc3339(), EXECUTION_STATUS_CANARY_CONFIRMED],
    )?;
    Ok(())
}

pub fn apply(db: &Db, api: Api, id: &str) -> Result<ExecutionCanaryPositionCloseResult> {
    apply_token(db, api, id, "mint")
}
pub fn apply_token(
    db: &Db,
    api: Api,
    id: &str,
    token: &str,
) -> Result<ExecutionCanaryPositionCloseResult> {
    match api {
        Api::Confirm => Ok(db
            .store
            .confirm_execution_canary_sell_fill(
                id,
                token,
                7.0,
                Some(TokenQuantity::new(7000, 3)),
                0.0000005 / 7.0,
                0.0,
                db.now,
                db.now,
                Some(Lamports::new(500)),
            )?
            .1),
        Api::Confirmed => db.store.close_execution_canary_confirmed_sell_fill(
            id,
            token,
            7.0,
            Some(TokenQuantity::new(7000, 3)),
            0.0000005 / 7.0,
            0.0,
            db.now,
        ),
    }
}

pub fn snapshot(db: &Db) -> Result<Vec<(String, Vec<String>)>> {
    let conn = db.conn()?;
    let mut all = rows::snapshot(&conn)?;
    let schema = conn
        .prepare("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name")?
        .query_map([], |r| {
            Ok(format!(
                "{:?}",
                (
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, Option<String>>(3)?
                )
            ))
        })?
        .collect::<rusqlite::Result<_>>()?;
    all.push(("schema".into(), schema));
    Ok(all)
}

pub fn rejected(db: &Db, api: Api, id: &str) -> Result<()> {
    let before = snapshot(db)?;
    let money_before = money(db)?;
    let result = apply(db, api, id);
    let money_after = money(db)?;
    println!("LEGACY_SELL api={api:?} id={id} before={money_before:?} after={money_after:?} result={result:?}");
    let error = result.expect_err("another receipt owner cannot change inventory or add a fill");
    let reason = error
        .downcast_ref::<SellSettlementUnsupported>()
        .expect("typed ownership refusal");
    assert_eq!(*reason, SellSettlementUnsupported::ReceiptAlreadyClaimed);
    assert_eq!(snapshot(db)?, before);
    assert!(!db.store.execution_canary_fill_exists(id)?);
    Ok(())
}

pub fn legacy_order(db: &Db, name: &str, api: Api) -> Result<String> {
    db.store.insert_copy_signal(&CopySignalRow {
        signal_id: name.into(),
        wallet_id: "leader".into(),
        token: "mint".into(),
        side: "sell".into(),
        notional_sol: 0.0000005,
        notional_lamports: Some(Lamports::new(500)),
        notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.into(),
        ts: db.now,
        status: "shadow_recorded".into(),
    })?;
    let id = db
        .store
        .reserve_execution_canary_order(name, "tiny", db.now)?
        .order
        .order_id;
    db.store.mark_execution_canary_built(&id, db.now)?;
    db.store.mark_execution_canary_simulated(
        &id,
        db.now,
        EXECUTION_SIMULATION_STATUS_PASSED,
        None,
    )?;
    db.store
        .mark_execution_canary_submitted(&id, db.now, "shared-signature")?;
    if matches!(api, Api::Confirmed) {
        db.store.mark_execution_canary_confirmed(&id, db.now)?;
    }
    Ok(id)
}

fn money(db: &Db) -> Result<(Vec<(String, String, i64, Option<i64>)>, i64)> {
    let conn = db.conn()?;
    let positions=conn.prepare("SELECT position_id,qty_raw,cost_lamports,pnl_lamports FROM positions ORDER BY position_id")?
        .query_map([],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?
        .collect::<rusqlite::Result<_>>()?;
    Ok((
        positions,
        conn.query_row("SELECT COUNT(*) FROM fills", [], |r| r.get(0))?,
    ))
}
