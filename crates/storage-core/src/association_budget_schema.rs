//! Optional additive upgrade: wholly absent 0073 retains the observation API.
//! A recorded, partial, or modified accounting schema must never fall back.
use anyhow::{ensure, Context, Result};
use rusqlite::{Connection, OptionalExtension};
const VERSION: &str = "0073_association_budget_indexes.sql";
const DDL: &str = include_str!("../../../migrations/0073_association_budget_indexes.sql");
pub(super) fn available(c: &Connection) -> Result<bool> {
    let recorded: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=?1)",
        [VERSION],
        |r| r.get(0),
    )?;
    let mut present = 0;
    for object in DDL.split("-- object ").skip(1) {
        let (name, expected) = object.split_once('\n').context("budget DDL")?;
        let actual: Option<String> = c
            .query_row("SELECT sql FROM sqlite_master WHERE name=?1", [name], |r| {
                r.get(0)
            })
            .optional()?;
        if actual.is_some() {
            present += 1;
        }
        ensure!(
            (!recorded && actual.is_none())
                || actual.as_deref().map(|s| s.trim().trim_end_matches(';'))
                    == Some(expected.trim().trim_end_matches(';')),
            "missing/corrupt association budget schema {name}"
        );
    }
    ensure!(
        recorded || present == 0,
        "unrecorded/partial association budget schema"
    );
    Ok(recorded)
}
pub(super) fn integrity(c: &Connection) -> Result<()> {
    // Fixed twelve domains; checking each table also verifies all of its indexes.
    for table in [
        "association_inbox_identities",
        "association_inbox_events",
        "association_sell_preparations",
        "association_sell_dependencies",
        "association_sell_work",
        "association_sell_bootstrap",
        "association_parent_blocks",
        "association_parent_hashes",
        "association_parent_dependencies",
        "association_parent_work",
        "ordered_source_sell_intents",
        "source_sell_signature_claims",
    ] {
        let result: String = c.query_row(&format!("PRAGMA integrity_check({table})"), [], |r| {
            r.get(0)
        })?;
        ensure!(
            result == "ok",
            "corrupt association budget index for {table}"
        );
    }
    Ok(())
}
