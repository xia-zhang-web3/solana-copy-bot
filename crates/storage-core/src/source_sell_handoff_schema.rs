//! Shared main-DB capability validation. An unmigrated journal is explicitly separate.
use anyhow::{ensure, Context, Result};
use rusqlite::{Connection, OptionalExtension};

pub const MIGRATION: &str = "0065_source_sell_handoff.sql";
const DDL: &str = include_str!("../../../migrations/0065_source_sell_handoff.sql");
// SQLite omits the final statement terminator in sqlite_master. Preserve every
// interior byte: whitespace in a quoted literal changes trigger semantics.
fn normalized(sql: &str) -> &str {
    sql.trim().trim_end_matches(';').trim_end()
}

pub fn available(conn: &Connection) -> Result<bool> {
    let registry: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='schema_migrations')",
        [],
        |r| r.get(0),
    )?;
    let recorded = registry
        && conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=?1)",
            [MIGRATION],
            |r| r.get::<_, bool>(0),
        )?;
    for object in DDL.split("-- object ").skip(1) {
        let (name, expected) = object
            .split_once('\n')
            .context("handoff schema definition")?;
        let actual: Option<String> = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE name=?1 COLLATE NOCASE",
                [name],
                |r| r.get(0),
            )
            .optional()?;
        if !recorded {
            ensure!(
                actual.is_none(),
                "unrecorded source SELL handoff object {name}"
            );
        } else {
            ensure!(
                actual
                    .as_deref()
                    .is_some_and(|sql| normalized(sql) == normalized(expected)),
                "missing or corrupt source SELL handoff object {name} after 0065"
            );
        }
    }
    Ok(recorded)
}

pub(crate) fn required(conn: &Connection) -> Result<()> {
    ensure!(
        available(conn)?,
        "source SELL handoff requires migration 0065"
    );
    Ok(())
}
