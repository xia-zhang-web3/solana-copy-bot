use anyhow::{ensure, Context, Result};
use rusqlite::{Connection, OptionalExtension};
pub const MIGRATION: &str = "0074_ordered_sell_quote_only.sql";
const DDL: &str = include_str!("../../../migrations/0074_ordered_sell_quote_only.sql");
pub(crate) fn required(c: &Connection) -> Result<()> {
    ensure!(
        c.query_row(
            "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=?1)",
            [MIGRATION],
            |r| r.get::<_, bool>(0)
        )?,
        "strict quote migration0074 required"
    );
    for object in DDL.split("-- object ").skip(1) {
        let (name, expected) = object.split_once('\n').context("strict quote DDL")?;
        let actual: Option<String> = c
            .query_row("SELECT sql FROM sqlite_master WHERE name=?1", [name], |r| {
                r.get(0)
            })
            .optional()?;
        ensure!(
            actual.as_deref().map(|s| s.trim().trim_end_matches(';'))
                == Some(expected.trim().trim_end_matches(';')),
            "strict quote schema missing/changed: {name}"
        );
    }
    Ok(())
}

/// Strict observation/claim commits use a FULL-sync writer connection, like
/// durable intake. The app owns a separate connection for each quote job.
pub(crate) fn durable_writer(c: &Connection) -> Result<()> {
    c.pragma_update(None, "synchronous", "FULL")?;
    ensure!(
        c.query_row("PRAGMA synchronous", [], |r| r.get::<_, i64>(0))? == 2,
        "strict quote FULL synchronous mode required"
    );
    Ok(())
}
