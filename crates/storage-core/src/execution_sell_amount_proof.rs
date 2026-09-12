//! Nullable unsigned SELL provenance, atomically stored by the build metadata writer.
use crate::SqliteDiscoveryStore;
use anyhow::{bail, Result};
use rusqlite::{Connection, OptionalExtension};

const TABLE: &str = "execution_canary_build_plan_metadata";
const COLUMN: &str = "owned_sell_amount_proof_json";
const MIGRATION: &str = "0070_owned_sell_amount_proof.sql";

pub(super) fn available(conn: &Connection) -> Result<bool> {
    let migrations: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_migrations')",
        [], |r| r.get(0),
    )?;
    let applied = migrations
        && conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=?1)",
            [MIGRATION],
            |r| r.get::<_, bool>(0),
        )?;
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({TABLE})"))?;
    let columns = stmt
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let present = columns.iter().any(|c| c == COLUMN);
    if applied && !present {
        bail!("applied 0070 SELL amount proof schema is incomplete");
    }
    Ok(present)
}

impl SqliteDiscoveryStore {
    /// Old unsigned metadata has no proof; readers must never synthesize a current snapshot.
    pub fn load_execution_canary_sell_amount_proof(
        &self,
        order_id: &str,
    ) -> Result<Option<String>> {
        if !available(&self.conn)? {
            return Ok(None);
        }
        Ok(self.conn.query_row(
            "SELECT owned_sell_amount_proof_json FROM execution_canary_build_plan_metadata WHERE order_id=?1",
            [order_id], |r| r.get::<_, Option<String>>(0),
        ).optional()?.flatten())
    }
}
