#![allow(dead_code)]
#[path = "failed_expense_fixture.rs"]
mod failed_fixture;
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_core_types::TokenQuantity;
pub use failed_fixture::*;
use rusqlite::params;

pub fn time(raw: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(raw)
        .unwrap()
        .with_timezone(&Utc)
}
pub fn fixed() -> Result<Db> {
    let mut db = Db::new()?;
    db.now = time("2026-09-06T12:00:00Z");
    db.conn()?
        .execute("UPDATE orders SET submit_ts=?1", [db.now.to_rfc3339()])?;
    db.conn()?
        .execute("UPDATE copy_signals SET ts=?1", [db.now.to_rfc3339()])?;
    Ok(db)
}
pub fn complete(db: &Db, id: &str, fee: u64) -> Result<()> {
    db.detect(id, "signature_status")?;
    db.store
        .apply_failed_expense(id, &db.facts(id, fee)?, db.now)
}
pub fn closed(
    db: &Db,
    id: &str,
    raw: Option<i64>,
    sol: Option<f64>,
    state: &str,
    at: DateTime<Utc>,
) -> Result<()> {
    db.store.record_execution_canary_open_position(
        id,
        id,
        1.0,
        Some(TokenQuantity::new(1, 0)),
        0.03,
        at,
    )?;
    db.conn()?.execute(
        "UPDATE positions SET state=?2,closed_ts=?3,pnl_lamports=?4,pnl_sol=?5 WHERE token=?1",
        params![id, state, at.to_rfc3339(), raw, sol],
    )?;
    Ok(())
}
pub fn cost(db: &Db) -> Result<copybot_storage_core::ExecutionCanaryEntryCost> {
    db.store
        .execution_canary_entry_cost(db.now + chrono::Duration::seconds(1))
}
pub fn snapshot(db: &Db) -> Result<serde_json::Value> {
    let conn = db.conn()?;
    let mut q = conn.prepare("SELECT name FROM sqlite_master WHERE type='table' AND (name IN ('orders','copy_signals','positions','fills') OR name LIKE 'execution_%') ORDER BY name")?;
    let names = q
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut result = serde_json::Map::new();
    for name in names {
        let mut stmt = conn.prepare(&format!("SELECT * FROM {name} ORDER BY rowid"))?;
        let count = stmt.column_count();
        let rows = stmt
            .query_map([], |r| {
                (0..count)
                    .map(|i| r.get_ref(i).map(|v| format!("{v:?}")))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        result.insert(name, serde_json::json!(rows));
    }
    Ok(result.into())
}
