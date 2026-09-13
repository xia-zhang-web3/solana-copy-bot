//! Construct historical rows from canonical current fixtures, without invoking a
//! modern receipt writer against a schema predating its required budget tables.
#![allow(dead_code)]
#[path = "buy_attribution_review_fixture.rs"]
mod canonical;
use anyhow::Result;
use chrono::{DateTime, Utc};
use rusqlite::{params_from_iter, Connection};
use std::path::Path;

pub fn prefix(destination: &Path, boundary: &str) -> Result<()> {
    std::fs::create_dir(destination)?;
    for entry in std::fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../../migrations"))? {
        let entry = entry?;
        let name = entry.file_name();
        if name.to_string_lossy().ends_with(".sql") && name.to_string_lossy().as_ref() < boundary {
            std::fs::copy(entry.path(), destination.join(name))?;
        }
    }
    Ok(())
}

pub fn buy(
    target: &Connection,
    id: &str,
    source: &str,
    signature: &str,
    now: DateTime<Utc>,
) -> Result<()> {
    let mut db = canonical::Db::new()?;
    db.now = now;
    let order = db.seed_claim(id, source, "buy", "mint", "execution-wallet", signature)?;
    db.buy(&order)?;
    project_financial_rows(target, &db.conn()?)
}

pub fn project_financial_rows(target: &Connection, source: &Connection) -> Result<()> {
    let tx = target.unchecked_transaction()?;
    for table in [
        "copy_signals",
        "orders",
        "execution_canary_receipt_proofs",
        "execution_canary_receipt_facts",
        "positions",
        "fills",
    ] {
        assert_eq!(
            tx.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r
                .get::<_, i64>(0))?,
            0
        );
        let columns = tx
            .prepare(&format!("PRAGMA table_info({table})"))?
            .query_map([], |r| r.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let names = columns
            .iter()
            .map(|c| format!("\"{c}\""))
            .collect::<Vec<_>>()
            .join(",");
        let rows = source
            .prepare(&format!("SELECT {names} FROM {table}"))?
            .query_map([], |r| {
                (0..columns.len())
                    .map(|i| r.get::<_, rusqlite::types::Value>(i))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        assert_eq!(rows.len(), 1, "one canonical historical {table} row");
        let placeholders = vec!["?"; columns.len()].join(",");
        assert_eq!(
            tx.execute(
                &format!("INSERT INTO {table} ({names}) VALUES ({placeholders})"),
                params_from_iter(&rows[0])
            )?,
            1
        );
        let restored = tx.query_row(&format!("SELECT {names} FROM {table}"), [], |r| {
            (0..columns.len())
                .map(|i| r.get::<_, rusqlite::types::Value>(i))
                .collect::<rusqlite::Result<Vec<_>>>()
        })?;
        assert_eq!(restored, rows[0], "historical {table} values exact");
    }
    assert!(!tx.prepare("PRAGMA foreign_key_check")?.exists([])?);
    tx.commit()?;
    Ok(())
}
