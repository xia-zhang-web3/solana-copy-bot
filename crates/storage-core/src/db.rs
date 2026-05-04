use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use std::path::Path;
use std::time::Duration;

pub struct SqliteDiscoveryStore {
    pub(crate) conn: Connection,
}

impl SqliteDiscoveryStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path.as_ref()).with_context(|| {
            format!(
                "failed opening discovery sqlite store {}",
                path.as_ref().display()
            )
        })?;
        configure_connection(&conn, false)?;
        Ok(Self { conn })
    }

    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI;
        let conn = Connection::open_with_flags(path.as_ref(), flags).with_context(|| {
            format!(
                "failed opening read-only discovery sqlite store {}",
                path.as_ref().display()
            )
        })?;
        configure_connection(&conn, true)?;
        Ok(Self { conn })
    }

    pub(crate) fn sqlite_table_exists(&self, table: &str) -> Result<bool> {
        let exists = self
            .conn
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1 LIMIT 1",
                [table],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .with_context(|| format!("failed checking sqlite table existence: {table}"))?;
        Ok(exists.is_some())
    }
}

fn configure_connection(conn: &Connection, read_only: bool) -> Result<()> {
    conn.busy_timeout(Duration::from_secs(30))
        .context("failed setting sqlite busy timeout")?;
    if read_only {
        conn.pragma_update(None, "query_only", true)
            .context("failed setting sqlite query_only")?;
    }
    Ok(())
}
