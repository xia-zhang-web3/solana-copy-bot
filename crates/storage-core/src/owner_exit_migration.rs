//! Protected additive owner EXIT origin, preserving all historical order sources.
use anyhow::{ensure, Context, Result};
use rusqlite::{Connection, OptionalExtension};

pub const VERSION: &str = "0086_owner_exit_intent.sql";

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
        "owner EXIT source migration requires protected rebuild runner"
    );
    let original: String = c.query_row(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='execution_order_sources'",
        [],
        |r| r.get(0),
    )?;
    let old_buy = "owner_buy_intent_id TEXT UNIQUE REFERENCES owner_technical_buy_intents(intent_id) ON DELETE RESTRICT,";
    let old_check = "CHECK ((copy_signal_id IS NOT NULL AND owned_sell_intent_id IS NULL AND owner_buy_intent_id IS NULL AND identity_id=copy_signal_id)\n        OR (copy_signal_id IS NULL AND owned_sell_intent_id IS NOT NULL AND owner_buy_intent_id IS NULL AND identity_id=owned_sell_intent_id)\n        OR (copy_signal_id IS NULL AND owned_sell_intent_id IS NULL AND owner_buy_intent_id IS NOT NULL AND identity_id='owner-buy:'||owner_buy_intent_id))";
    ensure!(
        original.starts_with("CREATE TABLE \"execution_order_sources\" (")
            && original.matches(old_buy).count() == 1
            && original.matches(old_check).count() == 1,
        "owner EXIT source schema unsupported"
    );
    let definitions = c.prepare("SELECT sql FROM sqlite_master WHERE tbl_name='execution_order_sources' AND type IN ('index','trigger') AND sql IS NOT NULL ORDER BY type,name")?
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let ddl = original
        .replacen("CREATE TABLE \"execution_order_sources\" (", "CREATE TABLE execution_order_sources_0086 (", 1)
        .replacen(old_buy, &format!("{old_buy}\n    owner_exit_intent_id TEXT UNIQUE REFERENCES owner_exit_intents(intent_id) ON DELETE RESTRICT,"), 1)
        .replacen(old_check,
            "CHECK ((copy_signal_id IS NOT NULL AND owned_sell_intent_id IS NULL AND owner_buy_intent_id IS NULL AND owner_exit_intent_id IS NULL AND identity_id=copy_signal_id)\n        OR (copy_signal_id IS NULL AND owned_sell_intent_id IS NOT NULL AND owner_buy_intent_id IS NULL AND owner_exit_intent_id IS NULL AND identity_id=owned_sell_intent_id)\n        OR (copy_signal_id IS NULL AND owned_sell_intent_id IS NULL AND owner_buy_intent_id IS NOT NULL AND owner_exit_intent_id IS NULL AND identity_id='owner-buy:'||owner_buy_intent_id)\n        OR (copy_signal_id IS NULL AND owned_sell_intent_id IS NULL AND owner_buy_intent_id IS NULL AND owner_exit_intent_id IS NOT NULL AND identity_id='owner-exit:'||owner_exit_intent_id))", 1);
    let columns = c
        .prepare("PRAGMA table_info(execution_order_sources)")?
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .map(|n| format!("\"{}\"", n.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(",");
    let legacy: bool = c.pragma_query_value(None, "legacy_alter_table", |r| r.get(0))?;
    c.pragma_update(None, "legacy_alter_table", true)?;
    let result = (|| {
        c.execute_batch("CREATE TEMP TABLE owner_exit_rebuild_guard(enabled INTEGER);INSERT INTO owner_exit_rebuild_guard VALUES(1);")?;
        c.execute_batch(sql)?;
        c.execute_batch(&ddl)?;
        c.execute_batch(&format!("INSERT INTO execution_order_sources_0086({columns}) SELECT {columns} FROM execution_order_sources;"))?;
        ensure!(!c.prepare(&format!("SELECT {columns} FROM execution_order_sources EXCEPT SELECT {columns} FROM execution_order_sources_0086"))?.exists([])?
            && !c.prepare(&format!("SELECT {columns} FROM execution_order_sources_0086 EXCEPT SELECT {columns} FROM execution_order_sources"))?.exists([])?,
            "owner EXIT source rows changed");
        c.execute_batch("DROP TABLE execution_order_sources;ALTER TABLE execution_order_sources_0086 RENAME TO execution_order_sources;")?;
        for definition in definitions {
            c.execute_batch(&definition)
                .context("restore source index/trigger")?;
        }
        c.execute_batch("DROP TABLE owner_exit_rebuild_guard")?;
        Ok(())
    })();
    c.pragma_update(None, "legacy_alter_table", legacy)?;
    result
}
