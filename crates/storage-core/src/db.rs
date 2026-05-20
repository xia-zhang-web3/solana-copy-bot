use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use std::fs;
use std::path::Path;
use std::time::Duration;

const OPERATOR_SCAN_CACHE_KIB: i64 = -524_288;
const OPERATOR_SCAN_MMAP_BYTES: i64 = 2_147_483_648;

pub struct SqliteDiscoveryStore {
    pub(crate) conn: Connection,
}

impl SqliteDiscoveryStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        if let Some(parent) = path.as_ref().parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create sqlite parent dir: {}", parent.display())
            })?;
        }
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

    pub fn tune_for_operator_scans(&self) -> Result<()> {
        self.conn
            .pragma_update(None, "temp_store", "MEMORY")
            .context("failed setting sqlite temp_store=MEMORY for operator scan")?;
        self.conn
            .pragma_update(None, "cache_size", OPERATOR_SCAN_CACHE_KIB)
            .context("failed setting sqlite cache_size for operator scan")?;
        self.conn
            .pragma_update(None, "mmap_size", OPERATOR_SCAN_MMAP_BYTES)
            .context("failed setting sqlite mmap_size for operator scan")?;
        Ok(())
    }
}

fn configure_connection(conn: &Connection, read_only: bool) -> Result<()> {
    conn.busy_timeout(Duration::from_secs(5))
        .context("failed setting sqlite busy timeout")?;
    if read_only {
        conn.pragma_update(None, "foreign_keys", "ON")
            .context("failed enabling sqlite foreign keys")?;
        conn.pragma_update(None, "query_only", true)
            .context("failed setting sqlite query_only")?;
    } else {
        conn.pragma_update(None, "journal_mode", "WAL")
            .context("failed setting sqlite journal mode WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")
            .context("failed setting sqlite synchronous NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .context("failed enabling sqlite foreign keys")?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS schema_migrations (
                version TEXT PRIMARY KEY,
                applied_at TEXT NOT NULL
            );",
        )
        .context("failed creating schema_migrations table")?;
    }
    Ok(())
}
