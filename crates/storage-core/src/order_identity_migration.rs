//! Change only the order source FK; preserve every historical operand and custom DDL.
use anyhow::{ensure, Context, Result};
use rusqlite::{Connection, OptionalExtension};
pub const VERSION: &str = "0080_rpc_owned_sell_dispatch.sql";
pub fn required(c: &Connection, files: &[std::path::PathBuf]) -> Result<bool> {
    if !files
        .iter()
        .any(|p| p.file_name().and_then(|s| s.to_str()) == Some(VERSION))
    {
        return Ok(false);
    }
    Ok(c.query_row(
        "SELECT 1 FROM schema_migrations WHERE version=?1",
        [VERSION],
        |r| r.get::<_, i32>(0),
    )
    .optional()?
    .is_none())
}
pub fn apply(c: &Connection, sql: &str) -> Result<()> {
    ensure!(
        !c.pragma_query_value(None, "foreign_keys", |r| r.get::<_, bool>(0))?,
        "owned source migration requires protected rebuild runner"
    );
    let original: String = c.query_row(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='orders'",
        [],
        |r| r.get(0),
    )?;
    let old = "REFERENCES copy_signals(signal_id) ON DELETE RESTRICT";
    ensure!(
        original.matches(old).count() == 1 && original.starts_with("CREATE TABLE orders ("),
        "owned source migration orders schema unsupported"
    );
    let definitions=c.prepare("SELECT sql FROM sqlite_master WHERE tbl_name='orders' AND type IN ('index','trigger') AND sql IS NOT NULL ORDER BY type,name")?.query_map([],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
    let ddl = original
        .replacen("CREATE TABLE orders (", "CREATE TABLE orders_0080 (", 1)
        .replacen(
            old,
            "REFERENCES execution_order_sources(identity_id) ON DELETE RESTRICT",
            1,
        );
    let columns = c
        .prepare("PRAGMA table_info(orders)")?
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let columns = columns
        .iter()
        .map(|s| format!("\"{}\"", s.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(",");
    let legacy: bool = c.pragma_query_value(None, "legacy_alter_table", |r| r.get(0))?;
    c.pragma_update(None, "legacy_alter_table", true)?;
    let result = (|| {
        c.execute_batch("CREATE TEMP TABLE rpc_owned_sell_rebuild_guard(enabled INTEGER);INSERT INTO rpc_owned_sell_rebuild_guard VALUES(1);")?;
        c.execute_batch(sql)?;
        c.execute_batch(&ddl)?;
        c.execute_batch(&format!(
            "INSERT INTO orders_0080({columns}) SELECT {columns} FROM orders;"
        ))?;
        // Bidirectional EXCEPT proves typed values before the old table is removed.
        ensure!(
            !c.prepare("SELECT * FROM orders EXCEPT SELECT * FROM orders_0080")?
                .exists([])?
                && !c
                    .prepare("SELECT * FROM orders_0080 EXCEPT SELECT * FROM orders")?
                    .exists([])?,
            "owned source migration row mismatch"
        );
        c.execute_batch("DROP TABLE orders;ALTER TABLE orders_0080 RENAME TO orders;")?;
        for ddl in definitions {
            c.execute_batch(&ddl)
                .context("restore order indexes/triggers")?;
        }
        c.execute_batch("DROP TABLE rpc_owned_sell_rebuild_guard")?;
        Ok(())
    })();
    c.pragma_update(None, "legacy_alter_table", legacy)?;
    result
}
