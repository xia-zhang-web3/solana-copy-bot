use anyhow::{ensure, Context, Result};
use rusqlite::{Connection, OptionalExtension};
pub const MIGRATION: &str = "0069_association_parent_graph.sql";
pub const DDL: &str = include_str!("../../../migrations/0069_association_parent_graph.sql");
pub(crate) fn required(c: &Connection) -> Result<()> {
    ensure!(
        c.query_row(
            "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=?1)",
            [MIGRATION],
            |r| r.get::<_, bool>(0)
        )?,
        "parent graph migration0069 required"
    );
    for object in DDL.split("-- object ").skip(1) {
        let (name, expected) = object.split_once('\n').context("parent graph DDL")?;
        let actual: Option<String> = c
            .query_row("SELECT sql FROM sqlite_master WHERE name=?1", [name], |r| {
                r.get(0)
            })
            .optional()?;
        ensure!(
            actual.as_deref().map(|s| s.trim().trim_end_matches(';'))
                == Some(expected.trim().trim_end_matches(';')),
            "missing/corrupt parent graph schema {name}"
        );
    }
    Ok(())
}
