//! Exact logical charges. SQLite maintains expression indexes in the same write
//! transaction as their source rows, including writes from other connections.
use super::ConsumerMode;
use anyhow::Result;
use rusqlite::Connection;
#[path = "association_budget_queries.rs"]
mod queries;
#[path = "association_budget_schema.rs"]
mod schema;

pub(super) fn validate_open(c: &Connection) -> Result<()> {
    if schema::available(c)? {
        // One startup check against source rows catches a corrupt index before
        // recovery can ACK. No shadow totals, journal, or per-turn payload scan.
        schema::integrity(c)?;
    }
    Ok(())
}
pub(super) fn usage(c: &Connection, mode: ConsumerMode) -> Result<(usize, usize)> {
    if c.is_autocommit() {
        // Public observation reads must also see all domains in one snapshot.
        let tx = c.unchecked_transaction()?;
        let result = in_snapshot(&tx, mode)?;
        tx.commit()?;
        Ok(result)
    } else {
        in_snapshot(c, mode)
    }
}
fn in_snapshot(c: &Connection, mode: ConsumerMode) -> Result<(usize, usize)> {
    let queries = if schema::available(c)? {
        &queries::INDEXED
    } else {
        &queries::SLOW
    };
    let domains = if mode == ConsumerMode::ObservationOnly {
        2
    } else {
        3
    };
    let (mut count, mut bytes) = (0usize, 0usize);
    for sql in &queries[..domains] {
        let (n, b): (i64, i64) = c.query_row(sql, [], |r| Ok((r.get(0)?, r.get(1)?)))?;
        count = count
            .checked_add(n.try_into()?)
            .ok_or_else(|| anyhow::anyhow!("inbox count overflow"))?;
        bytes = bytes
            .checked_add(b.try_into()?)
            .ok_or_else(|| anyhow::anyhow!("inbox bytes overflow"))?;
    }
    if mode == ConsumerMode::ProviderOrderStrictV1 {
        let (n, b) = crate::association_sell_preparation::shadow_recovery::usage(c)?;
        count = count
            .checked_add(n)
            .ok_or_else(|| anyhow::anyhow!("inbox count overflow"))?;
        bytes = bytes
            .checked_add(b)
            .ok_or_else(|| anyhow::anyhow!("inbox bytes overflow"))?;
    }
    Ok((count, bytes))
}
