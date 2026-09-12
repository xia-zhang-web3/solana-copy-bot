//! Shared raw retention for core and legacy callers; call inside their IMMEDIATE transaction.
#[path = "observed_retention_coverage.rs"]
mod coverage;
#[path = "observed_retention_schema.rs"]
mod schema;
use crate::recent_raw::helpers::{
    recent_raw_journal_state_cached_query, recent_raw_journal_state_query,
    recent_raw_journal_state_row_exists, upsert_recent_raw_journal_state_on_conn,
};
use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
pub use coverage::{earliest_covered, restrict_coverage};
use rusqlite::{params, Connection};

const PIN: &str = "AND NOT EXISTS (
    SELECT 1 FROM execution_source_sell_intents s
    JOIN positions p ON p.position_id=s.position_id
    WHERE s.event_signature=observed_swaps.signature AND p.state='open'
)";

pub fn validate_schema(conn: &Connection) -> Result<()> {
    schema::has_staging(conn)?;
    crate::source_sell_handoff_schema::available(conn)?;
    coverage::floor(conn)?;
    Ok(())
}

pub fn delete_before_batch(
    conn: &Connection,
    cutoff: DateTime<Utc>,
    limit: i64,
    pruned_at: Option<DateTime<Utc>>,
) -> Result<usize> {
    ensure!(limit > 0, "retention slice limit must be positive");
    ensure!(!conn.is_autocommit(), "retention requires a transaction");
    let pin = if schema::has_staging(conn)? { PIN } else { "" };
    let pending = if crate::source_sell_handoff_schema::available(conn)? {
        "AND NOT EXISTS (SELECT 1 FROM source_sell_handoffs h WHERE h.signature=observed_swaps.signature AND h.disposition='pending')"
    } else {
        ""
    };
    crate::observed_timestamp::ensure_observed_swaps_timestamps_canonical_utc_read_only(conn)?;
    coverage::ensure_schema(conn)?;
    let has_journal = schema::table(conn, "recent_raw_journal_state")?;
    let mut state = if has_journal && recent_raw_journal_state_row_exists(conn)? {
        Some(recent_raw_journal_state_cached_query(conn)?)
    } else if has_journal && pruned_at.is_some() {
        // Bootstrap a journal which has raw rows but no cached state, once; normal prune is O(1) metadata.
        Some(recent_raw_journal_state_query(conn)?)
    } else {
        None
    };
    let sql = format!(
        "DELETE FROM observed_swaps WHERE rowid IN (
        SELECT rowid FROM observed_swaps WHERE ts<?1 {pin} {pending}
        ORDER BY ts ASC,slot ASC,signature ASC LIMIT ?2)"
    );
    let deleted = conn
        .execute(&sql, params![cutoff.to_rfc3339(), limit])
        .context("delete unpinned observed retention slice")?;
    if deleted > 0 {
        coverage::advance(conn, cutoff)?;
    }
    if let Some(ref mut state) = state {
        state.row_count = state
            .row_count
            .checked_sub(deleted)
            .context("retention cached row count is smaller than deleted rows")?;
        if state.row_count == 0 {
            state.covered_since = None;
            state.covered_through_cursor = None;
        } else if deleted > 0 {
            state.covered_since = earliest_covered(conn)?;
            state.covered_through_cursor = coverage::latest_cursor(conn)?;
        }
        restrict_coverage(conn, state)?;
        if let Some(at) = pruned_at {
            state.last_pruned_rows = deleted;
            state.last_pruned_at = Some(at);
            state.updated_at = Some(at);
        }
        upsert_recent_raw_journal_state_on_conn(conn, state)?;
        ensure!(
            recent_raw_journal_state_cached_query(conn)? == *state,
            "retention journal state update was not preserved"
        );
    }
    Ok(deleted)
}
