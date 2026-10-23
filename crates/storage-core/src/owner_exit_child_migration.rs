//! Add one explicit child origin while retaining every parent constraint and source row.
use anyhow::{ensure, Context, Result};
use rusqlite::{Connection, OptionalExtension};
pub const VERSION: &str = "0092_owner_exit_child_attempt.sql";
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
        "child source requires protected rebuild"
    );
    let old: String = c.query_row(
        "SELECT sql FROM sqlite_master WHERE name='execution_order_sources' AND type='table'",
        [],
        |r| r.get(0),
    )?;
    let plain = "CREATE TABLE execution_order_sources (";
    let quoted = "CREATE TABLE \"execution_order_sources\" (";
    let prefix = if old.starts_with(plain) {
        plain
    } else {
        quoted
    };
    let old_check = "CHECK ((copy_signal_id IS NOT NULL AND owned_sell_intent_id IS NULL AND owner_buy_intent_id IS NULL AND owner_exit_intent_id IS NULL AND identity_id=copy_signal_id)\n        OR (copy_signal_id IS NULL AND owned_sell_intent_id IS NOT NULL AND owner_buy_intent_id IS NULL AND owner_exit_intent_id IS NULL AND identity_id=owned_sell_intent_id)\n        OR (copy_signal_id IS NULL AND owned_sell_intent_id IS NULL AND owner_buy_intent_id IS NOT NULL AND owner_exit_intent_id IS NULL AND identity_id='owner-buy:'||owner_buy_intent_id)\n        OR (copy_signal_id IS NULL AND owned_sell_intent_id IS NULL AND owner_buy_intent_id IS NULL AND owner_exit_intent_id IS NOT NULL AND identity_id='owner-exit:'||owner_exit_intent_id))";
    ensure!(
        old.starts_with(prefix) && old.matches(old_check).count() == 1,
        "child source schema unsupported"
    );
    let columns = c
        .prepare("PRAGMA table_info(execution_order_sources)")?
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ensure!(
        columns.starts_with(&[
            "identity_id".into(),
            "copy_signal_id".into(),
            "owned_sell_intent_id".into(),
            "owner_buy_intent_id".into(),
            "owner_exit_intent_id".into()
        ]) && !columns.iter().any(|n| n == "owner_exit_child_id"),
        "child source columns unsupported"
    );
    let objects=c.prepare("SELECT sql FROM sqlite_master WHERE tbl_name='execution_order_sources' AND type IN ('index','trigger') AND sql IS NOT NULL ORDER BY type,name")?
        .query_map([],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
    let mut check = String::from("owner_exit_child_id TEXT UNIQUE REFERENCES owner_exit_child_attempts(child_intent_id) ON DELETE RESTRICT,\nCHECK (");
    let origins = [
        ("copy_signal_id", "identity_id=copy_signal_id"),
        ("owned_sell_intent_id", "identity_id=owned_sell_intent_id"),
        (
            "owner_buy_intent_id",
            "identity_id='owner-buy:'||owner_buy_intent_id",
        ),
        (
            "owner_exit_intent_id",
            "identity_id='owner-exit:'||owner_exit_intent_id",
        ),
        (
            "owner_exit_child_id",
            "identity_id='owner-exit:'||owner_exit_child_id",
        ),
    ];
    check.push_str(
        &origins
            .iter()
            .map(|(selected, binding)| {
                let mut clauses = origins
                    .iter()
                    .map(|(name, _)| {
                        format!(
                            "{name} IS {}NULL",
                            if name == selected { "NOT " } else { "" }
                        )
                    })
                    .collect::<Vec<_>>();
                clauses.push(binding.to_string());
                format!("({})", clauses.join(" AND "))
            })
            .collect::<Vec<_>>()
            .join(" OR "),
    );
    check.push(')');
    let ddl = old
        .replacen(prefix, "CREATE TABLE execution_order_sources_0092 (", 1)
        .replacen(old_check, &check, 1);
    let legacy: bool = c.pragma_query_value(None, "legacy_alter_table", |r| r.get(0))?;
    c.pragma_update(None, "legacy_alter_table", true)?;
    let outcome = (|| {
        c.execute_batch("CREATE TEMP TABLE owner_exit_child_rebuild_guard(enabled INTEGER);INSERT INTO owner_exit_child_rebuild_guard VALUES(1);")?;
        c.execute_batch(sql)?;
        c.execute_batch(&ddl)?;
        let names = columns
            .iter()
            .map(|n| format!("\"{}\"", n.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(",");
        c.execute_batch(&format!("INSERT INTO execution_order_sources_0092({names}) SELECT {names} FROM execution_order_sources;"))?;
        ensure!(!c.prepare(&format!("SELECT {names} FROM execution_order_sources EXCEPT SELECT {names} FROM execution_order_sources_0092"))?.exists([])?
            && !c.prepare(&format!("SELECT {names} FROM execution_order_sources_0092 EXCEPT SELECT {names} FROM execution_order_sources"))?.exists([])?,"child source history changed");
        c.execute_batch("DROP TABLE execution_order_sources;ALTER TABLE execution_order_sources_0092 RENAME TO execution_order_sources;")?;
        for object in objects {
            c.execute_batch(&object)
                .context("restore immutable origin")?;
        }
        c.execute_batch("DROP TABLE owner_exit_child_rebuild_guard;")?;
        Ok(())
    })();
    c.pragma_update(None, "legacy_alter_table", legacy)?;
    outcome
}
