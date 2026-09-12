use anyhow::{ensure, Context, Result};
use rusqlite::{Connection, OptionalExtension};

pub(super) const MIGRATION: &str = "0063_observed_retention_boundary.sql";

pub(super) fn table(conn: &Connection, name: &str) -> Result<bool> {
    let kind: Option<String> = conn
        .query_row(
            "SELECT type FROM sqlite_master WHERE name=?1 COLLATE NOCASE",
            [name],
            |r| r.get(0),
        )
        .optional()
        .context("read retention schema metadata")?;
    ensure!(
        kind.as_deref().is_none_or(|k| k == "table"),
        "retention object {name} is not a table"
    );
    Ok(kind.is_some())
}

pub(super) fn recorded(conn: &Connection, migration: &str) -> Result<bool> {
    if !table(conn, "schema_migrations")? {
        return Ok(false);
    }
    Ok(conn
        .query_row(
            "SELECT 1 FROM schema_migrations WHERE version=?1",
            [migration],
            |r| r.get::<_, i64>(0),
        )
        .optional()
        .context("read retention migration metadata")?
        .is_some())
}

pub(super) fn has_staging(conn: &Connection) -> Result<bool> {
    let modern = recorded(conn, "0058_execution_source_sell_intents.sql")?;
    let staged = table(conn, "execution_source_sell_intents")?;
    ensure!(
        !modern || staged,
        "staged SELL table missing after recorded 0058"
    );
    if staged {
        ensure!(
            table(conn, "positions")?,
            "positions table missing for staged SELL retention"
        );
        // Prepare validates both schemas even when there are no candidate rows.
        conn.prepare(
            "SELECT s.event_signature,p.state FROM execution_source_sell_intents s
            JOIN positions p ON p.position_id=s.position_id WHERE 0",
        )?;
    }
    Ok(staged)
}
