use crate::SqliteDiscoveryStore;
use anyhow::{Context, Result};

impl SqliteDiscoveryStore {
    pub fn wal_autocheckpoint_pages(&self) -> Result<i64> {
        self.conn
            .query_row("PRAGMA wal_autocheckpoint", [], |row| row.get(0))
            .context("failed to read sqlite wal_autocheckpoint")
    }

    pub fn set_wal_autocheckpoint_pages(&self, pages: i64) -> Result<()> {
        self.conn
            .pragma_update(None, "wal_autocheckpoint", pages)
            .with_context(|| format!("failed to set sqlite wal_autocheckpoint={pages} pages"))?;
        Ok(())
    }
}
