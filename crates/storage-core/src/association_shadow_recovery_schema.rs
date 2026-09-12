//! Required only for the explicit durable consumer/close API; never auto-repair.
use anyhow::{ensure, Context, Result};
use rusqlite::{Connection, OptionalExtension};
const VERSION: &str = "0075_shadow_sell_recovery.sql";
const DDL: &str = include_str!("../../../migrations/0075_shadow_sell_recovery.sql");
pub(crate) fn required(c: &Connection) -> Result<()> {
    ensure!(
        c.query_row(
            "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=?1)",
            [VERSION],
            |r| r.get::<_, bool>(0)
        )?,
        "Shadow SELL recovery migration0075 required"
    );
    for object in DDL.split("-- object ").skip(1) {
        let (name, expected) = object.split_once('\n').context("Shadow recovery DDL")?;
        let actual: Option<String> = c
            .query_row("SELECT sql FROM sqlite_master WHERE name=?1", [name], |r| {
                r.get(0)
            })
            .optional()?;
        ensure!(
            actual.as_deref().map(|s| s.trim().trim_end_matches(';'))
                == Some(expected.trim().trim_end_matches(';')),
            "Shadow SELL recovery schema missing/changed: {name}"
        );
    }
    Ok(())
}
