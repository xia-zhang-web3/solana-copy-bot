use anyhow::Result;
use rusqlite::{Connection, OptionalExtension};

pub const VERSION: &str = "0055_execution_receipt_cash_settlement.sql";

/// Preserve additional deployed indexes/triggers across the nullable fills rebuild.
pub fn apply(conn: &Connection, sql: &str) -> Result<()> {
    let definitions = conn
        .prepare(
            "SELECT name,sql FROM sqlite_master WHERE tbl_name='fills'
         AND type IN ('index','trigger') AND sql IS NOT NULL ORDER BY type,name",
        )?
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    conn.execute_batch(
        "CREATE TEMP TABLE execution_receipt_cash_rebuild_guard(enabled INTEGER);
        INSERT INTO execution_receipt_cash_rebuild_guard VALUES(1);",
    )?;
    conn.execute_batch(sql)?;
    conn.execute_batch("DROP TABLE execution_receipt_cash_rebuild_guard")?;
    for (name, definition) in definitions {
        let exists = conn
            .query_row("SELECT 1 FROM sqlite_master WHERE name=?1", [name], |r| {
                r.get::<_, i64>(0)
            })
            .optional()?
            .is_some();
        if !exists {
            conn.execute_batch(&definition)?;
        }
    }
    Ok(())
}

pub fn required(conn: &Connection, files: &[std::path::PathBuf]) -> Result<bool> {
    if !files
        .iter()
        .any(|p| p.file_name().and_then(|v| v.to_str()) == Some(VERSION))
    {
        return Ok(false);
    }
    Ok(conn
        .query_row(
            "SELECT 1 FROM schema_migrations WHERE version=?1",
            [VERSION],
            |r| r.get::<_, i64>(0),
        )
        .optional()?
        .is_none())
}

/// DROP TABLE can run incoming CASCADE/RESTRICT actions even with deferred checks.
/// Suspend those actions only for this rebuild, then check before committing and restore
/// enforcement on both success and failure. No runtime connection is returned unchecked.
pub fn with_constraints<T>(
    conn: &mut Connection,
    rebuild: bool,
    run: impl FnOnce(&mut Connection) -> Result<T>,
) -> Result<T> {
    if !rebuild {
        return run(conn);
    }
    anyhow::ensure!(
        conn.is_autocommit(),
        "fill rebuild requires an outer transaction boundary"
    );
    let enabled: bool = conn.pragma_query_value(None, "foreign_keys", |r| r.get(0))?;
    conn.pragma_update(None, "foreign_keys", false)?;
    let result = run(conn);
    conn.pragma_update(None, "foreign_keys", enabled)?;
    result
}

pub fn check_foreign_keys(conn: &Connection) -> Result<()> {
    let violation = conn.prepare("PRAGMA foreign_key_check")?.exists([])?;
    anyhow::ensure!(
        !violation,
        "foreign key violation during fill schema rebuild"
    );
    Ok(())
}
