//! Separate observation-only database. No runtime followlist or financial authority.
mod consumer;
mod events;
use anyhow::{ensure, Result};
use rusqlite::{Connection, OpenFlags};
use std::path::Path;
use std::time::Duration;

pub const SCHEMA: &str = include_str!("schema.sql");
pub struct CaptureStore {
    pub(super) db: Connection,
    pub(super) epoch: i64,
}
#[derive(Debug)]
pub struct CaptureReceipt {
    pub seq: i64,
    pub stage: String,
}
impl CaptureStore {
    /// The controller must explicitly create this separate database with limits.
    /// Never create a missing database or migrate the runtime/virtual ledger here.
    pub fn open(path: &Path) -> Result<Self> {
        let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        db.busy_timeout(Duration::from_secs(2))?;
        db.execute_batch("PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;")?;
        let (rows, bytes): (i64, i64) = db.query_row(
            "SELECT max_rows,max_bytes FROM capture_meta WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        ensure!(
            rows > 0 && rows <= 1_000_000 && bytes > 0 && bytes <= 1_073_741_824,
            "capture bounds invalid"
        );
        Ok(Self { db, epoch: 0 })
    }
    pub fn pending(&self) -> Result<Vec<(i64, Vec<u8>, Option<f64>)>> {
        // A single in-flight receive is allowed. Restore it before installing new scopes.
        let mut q = self.db.prepare(
            "SELECT seq,raw,source_at FROM capture_events WHERE stage='RECEIVED' ORDER BY seq LIMIT 2")?;
        let rows = q
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        ensure!(rows.len() <= 1, "multiple pending capture receives");
        Ok(rows)
    }
}
