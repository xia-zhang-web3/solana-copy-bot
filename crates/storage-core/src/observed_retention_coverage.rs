use super::schema;
use crate::RecentRawJournalStateRow;
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension};

fn timestamp(raw: &str) -> Result<DateTime<Utc>> {
    let value = DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc);
    ensure!(
        value.to_rfc3339() == raw,
        "retention coverage timestamp is not canonical UTC"
    );
    Ok(value)
}

pub(super) fn floor(conn: &Connection) -> Result<Option<DateTime<Utc>>> {
    let recorded = schema::recorded(conn, schema::MIGRATION)?;
    if !schema::table(conn, "observed_retention_boundary")? {
        ensure!(!recorded, "retention boundary missing after recorded 0063");
        return Ok(None);
    }
    let raw: Option<String> = conn
        .query_row(
            "SELECT floor_ts FROM observed_retention_boundary WHERE id=1",
            [],
            |r| r.get(0),
        )
        .context("read durable retention boundary singleton")?;
    raw.as_deref().map(timestamp).transpose()
}

pub(super) fn ensure_schema(conn: &Connection) -> Result<()> {
    floor(conn)?; // Validate before any bootstrap; never repair missing modern state.
    if !schema::table(conn, "observed_retention_boundary")? {
        conn.execute_batch(include_str!(
            "../../../migrations/0063_observed_retention_boundary.sql"
        ))?;
    }
    floor(conn)?;
    // Writable store bootstrap already supplies schema_migrations, including discovery-only DBs.
    // Record just this metadata migration so loss of the boundary cannot look pre-migration.
    if !schema::recorded(conn, schema::MIGRATION)? {
        let changed = conn.execute(
            "INSERT INTO schema_migrations(version,applied_at)
            VALUES(?1,strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))",
            [schema::MIGRATION],
        )?;
        ensure!(
            changed == 1 && schema::recorded(conn, schema::MIGRATION)?,
            "retention bootstrap migration was not recorded"
        );
    }
    Ok(())
}

pub(super) fn advance(conn: &Connection, cutoff: DateTime<Utc>) -> Result<()> {
    let next = floor(conn)?.map_or(cutoff, |old| old.max(cutoff));
    let changed = conn.execute(
        "UPDATE observed_retention_boundary SET floor_ts=?1 WHERE id=1",
        [next.to_rfc3339()],
    )?;
    ensure!(
        changed == 1 && floor(conn)? == Some(next),
        "retention boundary update was not preserved"
    );
    Ok(())
}

/// Physical old pins cannot establish a continuous range before the durable floor.
/// This is read-only and uses the time index, independently of current pin eligibility.
pub fn earliest_covered(conn: &Connection) -> Result<Option<DateTime<Utc>>> {
    let floor = floor(conn)?.map(|x| x.to_rfc3339()).unwrap_or_default();
    let raw: Option<String> = conn
        .query_row(
            "SELECT ts FROM observed_swaps WHERE ts>=?1 ORDER BY ts ASC LIMIT 1",
            [floor],
            |r| r.get(0),
        )
        .optional()?;
    raw.as_deref().map(timestamp).transpose()
}

pub fn restrict_coverage(conn: &Connection, state: &mut RecentRawJournalStateRow) -> Result<()> {
    if floor(conn)?.is_some() {
        // Ignored batch operands are not persisted evidence. Re-read indexed endpoints
        // after retention, including when only old pins or duplicates remain.
        state.covered_since = earliest_covered(conn)?;
        state.covered_through_cursor = latest_cursor(conn)?;
    }
    Ok(())
}

/// A pin can survive after the previously highest observed row is deleted.
/// Keep the physical cursor exact without recounting the journal on each slice.
pub(super) fn latest_cursor(conn: &Connection) -> Result<Option<crate::DiscoveryRuntimeCursor>> {
    let raw = conn.query_row(
        "SELECT ts,slot,signature FROM observed_swaps ORDER BY ts DESC,slot DESC,signature DESC LIMIT 1",
        [], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?, r.get::<_, String>(2)?)),
    ).optional()?;
    raw.map(|(ts, slot, signature)| {
        Ok(crate::DiscoveryRuntimeCursor {
            ts_utc: timestamp(&ts)?,
            slot: u64::try_from(slot).context("negative observed retention cursor slot")?,
            signature,
        })
    })
    .transpose()
}
