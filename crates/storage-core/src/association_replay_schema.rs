use anyhow::{ensure, Context, Result};
use rusqlite::{Connection, OptionalExtension};
pub(super) const MIGRATION: &str = "0089_association_replay_cursor.sql";
const DDL: &str = include_str!("../../../migrations/0089_association_replay_cursor.sql");
pub(super) fn available(c: &Connection) -> Result<bool> {
    Ok(c.query_row(
        "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=?1)",
        [MIGRATION],
        |r| r.get(0),
    )?)
}
pub(super) fn required(c: &Connection) -> Result<()> {
    ensure!(available(c)?, "association_replay_migration_required");
    for object in DDL.split("-- object ").skip(1) {
        let (name, expected) = object.split_once('\n').context("replay DDL")?;
        let actual: Option<String> = c
            .query_row("SELECT sql FROM sqlite_master WHERE name=?1", [name], |r| {
                r.get(0)
            })
            .optional()?;
        ensure!(
            actual.as_deref().map(|v| v.trim().trim_end_matches(';'))
                == Some(expected.trim().trim_end_matches(';')),
            "association_replay_schema:{name}"
        );
    }
    Ok(())
}
