//! Scoped zero reserve needs the existing protected table-rebuild transaction.
use anyhow::{ensure, Context, Result};
use rusqlite::{Connection, OptionalExtension};
pub const VERSION: &str = "0091_controlled_receipt_lot_custody.sql";
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
fn rebuild(c: &Connection, table: &str) -> Result<()> {
    ensure!(
        matches!(table, "owner_technical_buy_intents" | "owner_exit_intents"),
        "receipt_lot_migration_table"
    );
    let old: String = c.query_row(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?1",
        [table],
        |r| r.get(0),
    )?;
    let plain = format!("CREATE TABLE {table} (");
    let quoted = format!("CREATE TABLE \"{table}\" (");
    let prefix = if old.starts_with(&plain) {
        plain
    } else {
        quoted
    };
    ensure!(
        old.starts_with(&prefix) && old.matches("CHECK(min_reserve_lamports>0)").count() == 1,
        "receipt_lot_reserve_schema_unsupported"
    );
    let objects=c.prepare("SELECT sql FROM sqlite_master WHERE tbl_name=?1 AND type IN ('index','trigger') AND sql IS NOT NULL ORDER BY type,name")?.query_map([table],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
    let cols = c
        .prepare(&format!("PRAGMA table_info({table})"))?
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .map(|s| format!("\"{}\"", s.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(",");
    let next = format!("{table}_0091");
    let ddl = old
        .replacen(&prefix, &format!("CREATE TABLE {next} ("), 1)
        .replacen(
            "CHECK(min_reserve_lamports>0)",
            "CHECK(min_reserve_lamports>=0)",
            1,
        );
    c.execute_batch(&ddl)?;
    c.execute_batch(&format!(
        "INSERT INTO {next}({cols}) SELECT {cols} FROM {table};"
    ))?;
    ensure!(
        !c.prepare(&format!(
            "SELECT {cols} FROM {table} EXCEPT SELECT {cols} FROM {next}"
        ))?
        .exists([])?
            && !c
                .prepare(&format!(
                    "SELECT {cols} FROM {next} EXCEPT SELECT {cols} FROM {table}"
                ))?
                .exists([])?,
        "receipt_lot_migration_rows_changed"
    );
    c.execute_batch(&format!(
        "DROP TABLE {table};ALTER TABLE {next} RENAME TO {table};"
    ))?;
    for object in objects {
        c.execute_batch(&object)
            .context("restore owner identity trigger")?;
    }
    c.execute_batch(&format!("CREATE TRIGGER {table}_zero_scope BEFORE INSERT ON {table} WHEN NEW.min_reserve_lamports=0 AND NOT EXISTS(SELECT 1 FROM controlled_receipt_lot_scope WHERE wallet=NEW.wallet AND run_id=NEW.run_id AND json_extract(scope_json,'$.source_wallet')=NEW.wallet AND json_extract(scope_json,'$.source_run_id')=NEW.run_id) BEGIN SELECT RAISE(ABORT,'receipt lot zero reserve requires immutable scope');END;"))?;
    Ok(())
}
pub fn apply(c: &Connection, sql: &str) -> Result<()> {
    ensure!(
        !c.pragma_query_value(None, "foreign_keys", |r| r.get::<_, bool>(0))?,
        "receipt lot requires protected rebuild"
    );
    let legacy: bool = c.pragma_query_value(None, "legacy_alter_table", |r| r.get(0))?;
    c.pragma_update(None, "legacy_alter_table", true)?;
    let result = (|| {
        c.execute_batch("CREATE TEMP TABLE receipt_lot_rebuild_guard(enabled INTEGER);INSERT INTO receipt_lot_rebuild_guard VALUES(1);")?;
        c.execute_batch(sql)?;
        rebuild(c, "owner_technical_buy_intents")?;
        rebuild(c, "owner_exit_intents")?;
        c.execute_batch("DROP TABLE receipt_lot_rebuild_guard")?;
        Ok(())
    })();
    c.pragma_update(None, "legacy_alter_table", legacy)?;
    result
}
