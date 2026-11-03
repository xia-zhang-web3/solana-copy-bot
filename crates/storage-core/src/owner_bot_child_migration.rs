//! Preserve all failed parents and existing children while admitting one BOT child.
use anyhow::{ensure, Context, Result};
use rusqlite::{Connection, OptionalExtension};
pub const VERSION: &str = "0098_bot_receipt_exit_child.sql";

pub fn required(c: &Connection, files: &[std::path::PathBuf]) -> Result<bool> {
    Ok(files.iter().any(|p| p.file_name().and_then(|s| s.to_str()) == Some(VERSION))
        && c.query_row("SELECT 1 FROM schema_migrations WHERE version=?1", [VERSION],
            |r| r.get::<_, i32>(0)).optional()?.is_none())
}

pub fn apply(c: &Connection, sql: &str) -> Result<()> {
    ensure!(!c.pragma_query_value(None, "foreign_keys", |r| r.get::<_, bool>(0))?,
        "bot child requires protected rebuild");
    let old: String = c.query_row("SELECT sql FROM sqlite_master WHERE name='owner_exit_child_attempts'",
        [], |r| r.get(0))?;
    let prefix = "CREATE TABLE owner_exit_child_attempts (";
    let identity = "child_intent_id='source-child:'||substr(parent_intent_id,17)";
    ensure!(old.starts_with(prefix) && old.matches(identity).count()==1,
        "bot child ledger schema unsupported");
    let ddl = old.replacen(prefix, "CREATE TABLE owner_exit_child_attempts_0098 (", 1)
        .replacen(identity, &format!("({identity} OR (parent_intent_id LIKE 'bot-receipt-exit:%' AND child_intent_id='bot-receipt-child:'||substr(parent_intent_id,18)))"), 1);
    let objects = c.prepare("SELECT sql FROM sqlite_master WHERE tbl_name='owner_exit_child_attempts' AND type IN ('index','trigger') AND sql IS NOT NULL ORDER BY type,name")?
        .query_map([], |r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
    let view: String = c.query_row("SELECT sql FROM sqlite_master WHERE name='owner_exit_effective_intents'",
        [], |r|r.get(0))?;
    let columns = c.prepare("PRAGMA table_info(owner_exit_child_attempts)")?
        .query_map([], |r|r.get::<_,String>(1))?.collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter().map(|n|format!("\"{}\"", n.replace('"', "\"\""))).collect::<Vec<_>>().join(",");
    let legacy: bool = c.pragma_query_value(None, "legacy_alter_table", |r|r.get(0))?;
    c.pragma_update(None, "legacy_alter_table", true)?;
    let result = (|| {
        c.execute_batch("CREATE TEMP TABLE bot_receipt_child_rebuild_guard(enabled INTEGER);INSERT INTO bot_receipt_child_rebuild_guard VALUES(1);")?;
        c.execute_batch(sql)?;
        c.execute_batch(&ddl)?;
        c.execute_batch(&format!("INSERT INTO owner_exit_child_attempts_0098({columns}) SELECT {columns} FROM owner_exit_child_attempts;"))?;
        ensure!(!c.prepare(&format!("SELECT {columns} FROM owner_exit_child_attempts EXCEPT SELECT {columns} FROM owner_exit_child_attempts_0098"))?.exists([])?
            && !c.prepare(&format!("SELECT {columns} FROM owner_exit_child_attempts_0098 EXCEPT SELECT {columns} FROM owner_exit_child_attempts"))?.exists([])?,
            "bot child migration rows changed");
        c.execute_batch("DROP VIEW owner_exit_effective_intents;DROP TABLE owner_exit_child_attempts;ALTER TABLE owner_exit_child_attempts_0098 RENAME TO owner_exit_child_attempts;")?;
        for object in objects { c.execute_batch(&object).context("restore child index/trigger")?; }
        c.execute_batch(&view)?;
        c.execute_batch("DROP TABLE bot_receipt_child_rebuild_guard")?;
        Ok(())
    })();
    c.pragma_update(None, "legacy_alter_table", legacy)?;
    result
}
