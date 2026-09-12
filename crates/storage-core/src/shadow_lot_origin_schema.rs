use anyhow::{ensure, Context, Result};
use rusqlite::{Connection, OptionalExtension};
pub const MIGRATION: &str = "0071_shadow_lot_origins.sql";
pub const DDL: &str = include_str!("../../../migrations/0071_shadow_lot_origins.sql");
pub(crate) fn required(c: &Connection) -> Result<()> {
    ensure!(
        c.query_row(
            "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=?1)",
            [MIGRATION],
            |r| r.get::<_, bool>(0)
        )?,
        "Shadow origin migration0071 required"
    );
    for object in DDL.split("-- object ").skip(1) {
        let (name, expected) = object.split_once('\n').context("Shadow origin DDL")?;
        let actual: Option<String> = c
            .query_row("SELECT sql FROM sqlite_master WHERE name=?1", [name], |r| {
                r.get(0)
            })
            .optional()?;
        ensure!(
            actual.as_deref().map(|s| s.trim().trim_end_matches(';'))
                == Some(expected.trim().trim_end_matches(';')),
            "missing/corrupt Shadow origin schema {name}"
        );
    }
    Ok(())
}

/// Runtime registries may migrate a historical subset; validate 0071 if recorded.
pub fn check_if_applied(c: &Connection) -> Result<()> {
    if c.query_row(
        "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=?1)",
        [MIGRATION],
        |r| r.get::<_, bool>(0),
    )? {
        required(c)?;
    }
    Ok(())
}
