use anyhow::{ensure, Context, Result};
use rusqlite::{Connection, OptionalExtension};
pub const MIGRATION: &str = "0072_ordered_source_sell_intents.sql";
pub const DDL: &str = include_str!("../../../migrations/0072_ordered_source_sell_intents.sql");
pub(crate) fn required(c: &Connection) -> Result<()> {
    ensure!(recorded(c)?, "ordered SELL migration0072 required");
    for object in DDL.split("-- object ").skip(1) {
        let (name, expected) = object.split_once('\n').context("ordered SELL DDL")?;
        let actual: Option<String> = c
            .query_row("SELECT sql FROM sqlite_master WHERE name=?1", [name], |r| {
                r.get(0)
            })
            .optional()?;
        ensure!(
            actual.as_deref().map(|s| s.trim().trim_end_matches(';'))
                == Some(expected.trim().trim_end_matches(';')),
            "missing/corrupt ordered SELL schema {name}"
        );
    }
    Ok(())
}
fn recorded(c: &Connection) -> Result<bool> {
    Ok(c.query_row(
        "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=?1)",
        [MIGRATION],
        |r| r.get(0),
    )?)
}
/// Legacy-only databases remain compatible. Partial/new schema never falls back.
pub fn check_if_applied(c: &Connection) -> Result<bool> {
    let applied = recorded(c)?;
    let present: bool = c.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name IN ('source_sell_signature_claims','ordered_source_sell_intents'))", [], |r| r.get(0))?;
    if applied || present {
        required(c)?;
    }
    Ok(applied)
}
