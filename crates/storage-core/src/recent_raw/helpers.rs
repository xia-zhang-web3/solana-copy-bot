use super::{
    helpers_cursor::{cursor_cmp, parse_cursor, parse_optional_rfc3339_utc, parse_rfc3339_utc},
    BULK_INSERT_HARD_CAP_ROWS, BULK_INSERT_PARAMS_PER_ROW,
};
use crate::{DiscoveryRuntimeCursor, RecentRawJournalStateRow, RecentRawJournalWriteSummary};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::SwapEvent;
use rusqlite::{params, types::Value as SqlValue, Connection, OptionalExtension};

pub(super) fn bulk_insert_sql(row_count: usize) -> String {
    let placeholders = std::iter::repeat_n("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", row_count)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT OR IGNORE INTO observed_swaps(
            signature, wallet_id, dex, token_in, token_out, qty_in, qty_out,
            qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals, slot, ts
         ) VALUES {placeholders}"
    )
}

pub(super) fn effective_bulk_insert_chunk_rows(sqlite_variable_limit: usize) -> usize {
    let sqlite_limit_rows = sqlite_variable_limit / BULK_INSERT_PARAMS_PER_ROW;
    BULK_INSERT_HARD_CAP_ROWS.min(sqlite_limit_rows).max(1)
}

pub(super) fn sqlite_variable_limit(conn: &Connection) -> usize {
    unsafe {
        rusqlite::ffi::sqlite3_limit(
            conn.handle(),
            rusqlite::ffi::SQLITE_LIMIT_VARIABLE_NUMBER,
            -1,
        )
    }
    .max(0) as usize
}

pub(super) fn push_bulk_insert_values(values: &mut Vec<SqlValue>, swap: &SwapEvent) {
    values.push(SqlValue::Text(swap.signature.clone()));
    values.push(SqlValue::Text(swap.wallet.clone()));
    values.push(SqlValue::Text(swap.dex.clone()));
    values.push(SqlValue::Text(swap.token_in.clone()));
    values.push(SqlValue::Text(swap.token_out.clone()));
    values.push(SqlValue::Real(swap.amount_in));
    values.push(SqlValue::Real(swap.amount_out));
    values.push(
        swap.exact_amounts
            .as_ref()
            .map(|value| SqlValue::Text(value.amount_in_raw.clone()))
            .unwrap_or(SqlValue::Null),
    );
    values.push(
        swap.exact_amounts
            .as_ref()
            .map(|value| SqlValue::Integer(i64::from(value.amount_in_decimals)))
            .unwrap_or(SqlValue::Null),
    );
    values.push(
        swap.exact_amounts
            .as_ref()
            .map(|value| SqlValue::Text(value.amount_out_raw.clone()))
            .unwrap_or(SqlValue::Null),
    );
    values.push(
        swap.exact_amounts
            .as_ref()
            .map(|value| SqlValue::Integer(i64::from(value.amount_out_decimals)))
            .unwrap_or(SqlValue::Null),
    );
    values.push(SqlValue::Integer(swap.slot as i64));
    values.push(SqlValue::Text(swap.ts_utc.to_rfc3339()));
}

pub(super) fn recent_raw_journal_state_query(
    conn: &Connection,
) -> Result<RecentRawJournalStateRow> {
    let (row_count, covered_since, covered_through_cursor) = coverage_snapshot_on_conn(conn)?;
    let cached = recent_raw_journal_state_cached_query(conn)?;
    Ok(RecentRawJournalStateRow {
        covered_since,
        covered_through_cursor,
        row_count,
        ..cached
    })
}

pub(super) fn recent_raw_journal_state_cached_query(
    conn: &Connection,
) -> Result<RecentRawJournalStateRow> {
    let row = conn
        .query_row(
            "SELECT covered_since_ts, covered_through_cursor_ts, covered_through_cursor_slot,
                    covered_through_cursor_signature, row_count, last_batch_rows,
                    last_batch_completed_at, last_pruned_rows, last_pruned_at, updated_at
             FROM recent_raw_journal_state
             WHERE id = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                ))
            },
        )
        .optional()
        .context("failed reading cached recent raw journal state")?;
    let Some((
        since,
        cursor_ts,
        cursor_slot,
        cursor_sig,
        rows,
        batch_rows,
        batch_at,
        pruned,
        pruned_at,
        updated_at,
    )) = row
    else {
        return Ok(RecentRawJournalStateRow::default());
    };
    Ok(RecentRawJournalStateRow {
        covered_since: parse_optional_rfc3339_utc(
            since,
            "recent_raw_journal_state.covered_since_ts",
        )?,
        covered_through_cursor: parse_cursor(cursor_ts, cursor_slot, cursor_sig)?,
        row_count: rows.max(0) as usize,
        last_batch_rows: batch_rows.max(0) as usize,
        last_batch_completed_at: parse_optional_rfc3339_utc(
            batch_at,
            "recent_raw_journal_state.last_batch_completed_at",
        )?,
        last_pruned_rows: pruned.max(0) as usize,
        last_pruned_at: parse_optional_rfc3339_utc(
            pruned_at,
            "recent_raw_journal_state.last_pruned_at",
        )?,
        updated_at: parse_optional_rfc3339_utc(updated_at, "recent_raw_journal_state.updated_at")?,
    })
}

pub(super) fn coverage_snapshot_on_conn(
    conn: &Connection,
) -> Result<(usize, Option<DateTime<Utc>>, Option<DiscoveryRuntimeCursor>)> {
    let row_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM observed_swaps", [], |row| row.get(0))
        .context("failed counting recent raw journal observed_swaps rows")?;
    let covered_since_raw: Option<String> = conn
        .query_row("SELECT MIN(ts) FROM observed_swaps", [], |row| row.get(0))
        .optional()
        .context("failed loading recent raw journal covered_since timestamp")?
        .flatten();
    let cursor_raw = conn
        .query_row(
            "SELECT ts, slot, signature
             FROM observed_swaps
             ORDER BY ts DESC, slot DESC, signature DESC
             LIMIT 1",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?, row.get(2)?)),
        )
        .optional()
        .context("failed loading recent raw journal covered_through cursor")?;
    let cursor = cursor_raw
        .map(|(ts, slot, sig)| -> Result<_> {
            Ok(DiscoveryRuntimeCursor {
                ts_utc: parse_rfc3339_utc(&ts, "observed_swaps.ts")?,
                slot: slot.max(0) as u64,
                signature: sig,
            })
        })
        .transpose()?;
    Ok((
        row_count.max(0) as usize,
        parse_optional_rfc3339_utc(
            covered_since_raw,
            "recent_raw_journal_state.covered_since_ts",
        )?,
        cursor,
    ))
}

pub(super) fn recent_raw_journal_state_row_exists(conn: &Connection) -> Result<bool> {
    Ok(conn
        .query_row(
            "SELECT 1 FROM recent_raw_journal_state WHERE id = 1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .context("failed checking cached recent raw journal state row presence")?
        .is_some())
}

pub(super) fn upsert_recent_raw_journal_state_on_conn(
    conn: &Connection,
    state: &RecentRawJournalStateRow,
) -> Result<()> {
    conn.execute(
        "INSERT INTO recent_raw_journal_state(
            id, covered_since_ts, covered_through_cursor_ts, covered_through_cursor_slot,
            covered_through_cursor_signature, row_count, last_batch_rows,
            last_batch_completed_at, last_pruned_rows, last_pruned_at, updated_at
         ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
         ON CONFLICT(id) DO UPDATE SET
            covered_since_ts = excluded.covered_since_ts,
            covered_through_cursor_ts = excluded.covered_through_cursor_ts,
            covered_through_cursor_slot = excluded.covered_through_cursor_slot,
            covered_through_cursor_signature = excluded.covered_through_cursor_signature,
            row_count = excluded.row_count,
            last_batch_rows = excluded.last_batch_rows,
            last_batch_completed_at = excluded.last_batch_completed_at,
            last_pruned_rows = excluded.last_pruned_rows,
            last_pruned_at = excluded.last_pruned_at,
            updated_at = excluded.updated_at",
        params![
            state.covered_since.map(|ts| ts.to_rfc3339()),
            state
                .covered_through_cursor
                .as_ref()
                .map(|cursor| cursor.ts_utc.to_rfc3339()),
            state
                .covered_through_cursor
                .as_ref()
                .map(|cursor| cursor.slot as i64),
            state
                .covered_through_cursor
                .as_ref()
                .map(|cursor| cursor.signature.as_str()),
            state.row_count as i64,
            state.last_batch_rows as i64,
            state.last_batch_completed_at.map(|ts| ts.to_rfc3339()),
            state.last_pruned_rows as i64,
            state.last_pruned_at.map(|ts| ts.to_rfc3339()),
            state.updated_at.map(|ts| ts.to_rfc3339()),
        ],
    )
    .context("failed upserting recent raw journal state")?;
    Ok(())
}

pub(super) fn recent_raw_journal_write_summary(
    state: &RecentRawJournalStateRow,
    batch_rows: usize,
    inserted_rows: usize,
) -> RecentRawJournalWriteSummary {
    RecentRawJournalWriteSummary {
        batch_rows,
        inserted_rows,
        covered_since: state.covered_since,
        covered_through_cursor: state.covered_through_cursor.clone(),
        row_count: state.row_count,
        last_batch_completed_at: state.last_batch_completed_at,
        ..RecentRawJournalWriteSummary::default()
    }
}

pub(super) fn advance_recent_raw_journal_state_for_batch(
    state: &mut RecentRawJournalStateRow,
    processed_swaps: &[SwapEvent],
    inserted_rows: usize,
    completed_at: DateTime<Utc>,
) {
    if processed_swaps.is_empty() {
        return;
    }
    if inserted_rows > 0 {
        if let Some(first_swap) = processed_swaps.first() {
            state.covered_since = Some(match state.covered_since {
                Some(existing) if existing <= first_swap.ts_utc => existing,
                _ => first_swap.ts_utc,
            });
        }
        state.row_count = state.row_count.saturating_add(inserted_rows);
    }
    if let Some(last_swap) = processed_swaps.last() {
        let last_cursor = DiscoveryRuntimeCursor {
            ts_utc: last_swap.ts_utc,
            slot: last_swap.slot,
            signature: last_swap.signature.clone(),
        };
        let should_advance = state
            .covered_through_cursor
            .as_ref()
            .map(|cursor| cursor_cmp(&last_cursor, cursor).is_gt())
            .unwrap_or(state.row_count > 0 || inserted_rows > 0);
        if should_advance {
            state.covered_through_cursor = Some(last_cursor);
        }
    }
    state.last_batch_rows = inserted_rows;
    state.last_batch_completed_at = Some(completed_at);
    state.updated_at = Some(completed_at);
}
