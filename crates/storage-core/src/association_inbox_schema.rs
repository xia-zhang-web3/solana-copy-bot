use anyhow::{ensure, Context, Result};
use rusqlite::{Connection, OptionalExtension};
pub const MIGRATION: &str = "0067_association_inbox.sql";
pub const DDL: &str = include_str!("../../../migrations/0067_association_inbox.sql");
pub(crate) fn required(c: &Connection) -> Result<()> {
    let recorded: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=?1)",
        [MIGRATION],
        |r| r.get(0),
    )?;
    ensure!(recorded, "association inbox migration0067 required");
    for object in DDL.split("-- object ").skip(1) {
        let (name, expected) = object.split_once('\n').context("inbox DDL")?;
        let actual: Option<String> = c
            .query_row("SELECT sql FROM sqlite_master WHERE name=?1", [name], |r| {
                r.get(0)
            })
            .optional()?;
        ensure!(
            actual.as_deref().map(|s| s.trim().trim_end_matches(';'))
                == Some(expected.trim().trim_end_matches(';')),
            "missing/corrupt association inbox schema {name}"
        );
    }
    Ok(())
}
