use super::{
    helpers::{
        advance_recent_raw_journal_state_for_batch, bulk_insert_sql,
        effective_bulk_insert_chunk_rows, push_bulk_insert_values,
        recent_raw_journal_state_cached_query, recent_raw_journal_state_query,
        recent_raw_journal_state_row_exists, recent_raw_journal_write_summary,
        sqlite_variable_limit, upsert_recent_raw_journal_state_on_conn,
    },
    helpers_cursor::{elapsed_ms, far_deadline, row_to_swap_event},
    BULK_INSERT_HARD_CAP_ROWS, BULK_INSERT_PARAMS_PER_ROW,
};
use crate::{
    ObservedSwapCursorPage, RecentRawJournalStateRow, RecentRawJournalWriteSummary,
    SqliteDiscoveryStore,
};
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::SwapEvent;
use rusqlite::{params, params_from_iter, types::Value as SqlValue, Connection};
use std::time::Instant;

impl SqliteDiscoveryStore {
    pub fn ensure_recent_raw_journal_tables(&self) -> Result<()> {
        ensure_recent_raw_journal_tables_on_conn(&self.conn)
    }

    pub fn recent_raw_journal_state_read_only(&self) -> Result<RecentRawJournalStateRow> {
        if !self.sqlite_table_exists("observed_swaps")?
            || !self.sqlite_table_exists("recent_raw_journal_state")?
        {
            return Ok(RecentRawJournalStateRow::default());
        }
        recent_raw_journal_state_query(&self.conn)
    }

    pub fn recent_raw_journal_state(&self) -> Result<RecentRawJournalStateRow> {
        self.ensure_recent_raw_journal_tables()?;
        recent_raw_journal_state_query(&self.conn)
    }

    pub fn recent_raw_journal_state_cached_read_only_required(
        &self,
    ) -> Result<RecentRawJournalStateRow> {
        if !self.sqlite_table_exists("recent_raw_journal_state")? {
            bail!("cached recent raw journal state table recent_raw_journal_state is missing");
        }
        if !recent_raw_journal_state_row_exists(&self.conn)? {
            bail!("cached recent raw journal state row id=1 is missing");
        }
        recent_raw_journal_state_cached_query(&self.conn)
    }

    pub fn recent_raw_journal_state_cached(&self) -> Result<RecentRawJournalStateRow> {
        self.ensure_recent_raw_journal_tables()?;
        recent_raw_journal_state_cached_query(&self.conn)
    }

    pub fn insert_recent_raw_journal_batch(
        &self,
        swaps: &[SwapEvent],
        completed_at: DateTime<Utc>,
    ) -> Result<RecentRawJournalWriteSummary> {
        let (summary, _) = self.insert_recent_raw_journal_batch_bulk_with_deadline(
            swaps,
            completed_at,
            far_deadline(),
        )?;
        Ok(summary)
    }

    pub fn insert_recent_raw_journal_batch_bulk_with_deadline(
        &self,
        swaps: &[SwapEvent],
        completed_at: DateTime<Utc>,
        deadline: Instant,
    ) -> Result<(RecentRawJournalWriteSummary, bool)> {
        self.ensure_recent_raw_journal_tables()?;
        if swaps.is_empty() {
            let state = self.recent_raw_journal_state_cached()?;
            return Ok((recent_raw_journal_write_summary(&state, 0, 0), false));
        }
        let started = Instant::now();
        let write_result = self
            .with_immediate_transaction_retry("recent raw journal bulk batch write", |conn| {
                write_recent_raw_batch_on_conn(conn, swaps, completed_at, deadline)
            });
        match write_result {
            Ok((mut summary, exhausted)) => {
                summary.recent_raw_bulk_transaction_duration_ms =
                    started.elapsed().as_millis().min(u64::MAX as u128) as u64;
                Ok((summary, exhausted))
            }
            Err(error) => Err(error),
        }
    }

    pub fn prune_recent_raw_journal_before_batch(
        &self,
        cutoff: DateTime<Utc>,
        batch_size: usize,
        pruned_at: DateTime<Utc>,
    ) -> Result<usize> {
        self.ensure_recent_raw_journal_tables()?;
        let cutoff_ts = cutoff.to_rfc3339();
        let batch_limit = batch_size.max(1).min(i64::MAX as usize) as i64;
        self.with_immediate_transaction_retry("recent raw journal retention prune", |conn| {
            ensure_recent_raw_journal_tables_on_conn(conn)?;
            let deleted = conn
                .execute(
                    "DELETE FROM observed_swaps
                     WHERE rowid IN (
                        SELECT rowid
                        FROM observed_swaps
                        WHERE ts < ?1
                        ORDER BY ts ASC, slot ASC, signature ASC
                        LIMIT ?2
                     )",
                    params![&cutoff_ts, batch_limit],
                )
                .context("failed deleting recent raw journal retention slice")?;
            let mut state = recent_raw_journal_state_query(conn)?;
            state.last_pruned_rows = deleted.max(0) as usize;
            state.last_pruned_at = Some(pruned_at);
            state.updated_at = Some(pruned_at);
            upsert_recent_raw_journal_state_on_conn(conn, &state)?;
            Ok(deleted.max(0) as usize)
        })
    }

    pub fn for_each_observed_swap_since_with_budget<F>(
        &self,
        since: DateTime<Utc>,
        limit: usize,
        deadline: Instant,
        on_swap: F,
    ) -> Result<ObservedSwapCursorPage>
    where
        F: FnMut(SwapEvent) -> Result<()>,
    {
        self.for_each_observed_swap_page(
            "WHERE ts >= ?1",
            vec![since.to_rfc3339().into()],
            limit,
            deadline,
            on_swap,
        )
    }

    pub fn for_each_observed_swap_after_cursor_with_budget<F>(
        &self,
        cursor_ts: DateTime<Utc>,
        cursor_slot: u64,
        cursor_signature: &str,
        limit: usize,
        deadline: Instant,
        on_swap: F,
    ) -> Result<ObservedSwapCursorPage>
    where
        F: FnMut(SwapEvent) -> Result<()>,
    {
        self.for_each_observed_swap_page(
            "WHERE (ts, slot, signature) > (?1, ?2, ?3)",
            vec![
                cursor_ts.to_rfc3339().into(),
                (cursor_slot as i64).into(),
                cursor_signature.to_string().into(),
            ],
            limit,
            deadline,
            on_swap,
        )
    }

    fn for_each_observed_swap_page<F>(
        &self,
        where_sql: &str,
        mut values: Vec<SqlValue>,
        limit: usize,
        deadline: Instant,
        mut on_swap: F,
    ) -> Result<ObservedSwapCursorPage>
    where
        F: FnMut(SwapEvent) -> Result<()>,
    {
        if limit == 0 {
            return Ok(ObservedSwapCursorPage::default());
        }
        if Instant::now() >= deadline {
            return Ok(ObservedSwapCursorPage {
                rows_seen: 0,
                time_budget_exhausted: true,
            });
        }
        let limit = limit.min(i64::MAX as usize) as i64;
        values.push(limit.into());
        let sql = format!(
            "SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                    qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals
             FROM observed_swaps
             {where_sql}
             ORDER BY ts ASC, slot ASC, signature ASC
             LIMIT ?{}",
            values.len()
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let mut rows = stmt.query(params_from_iter(values))?;
        let mut seen = 0usize;
        while let Some(row) = rows.next()? {
            if Instant::now() >= deadline {
                return Ok(ObservedSwapCursorPage {
                    rows_seen: seen,
                    time_budget_exhausted: true,
                });
            }
            on_swap(row_to_swap_event(row)?)?;
            seen = seen.saturating_add(1);
        }
        Ok(ObservedSwapCursorPage {
            rows_seen: seen,
            time_budget_exhausted: false,
        })
    }
}

fn ensure_recent_raw_journal_tables_on_conn(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS observed_swaps (
            signature TEXT PRIMARY KEY,
            wallet_id TEXT NOT NULL,
            dex TEXT NOT NULL,
            token_in TEXT NOT NULL,
            token_out TEXT NOT NULL,
            qty_in REAL NOT NULL,
            qty_out REAL NOT NULL,
            qty_in_raw TEXT,
            qty_in_decimals INTEGER,
            qty_out_raw TEXT,
            qty_out_decimals INTEGER,
            slot INTEGER NOT NULL,
            ts TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_observed_swaps_ts_slot_signature
            ON observed_swaps(ts, slot, signature);
        CREATE TABLE IF NOT EXISTS recent_raw_journal_state (
            id INTEGER PRIMARY KEY CHECK(id = 1),
            covered_since_ts TEXT,
            covered_through_cursor_ts TEXT,
            covered_through_cursor_slot INTEGER,
            covered_through_cursor_signature TEXT,
            row_count INTEGER NOT NULL DEFAULT 0,
            last_batch_rows INTEGER NOT NULL DEFAULT 0,
            last_batch_completed_at TEXT,
            last_pruned_rows INTEGER NOT NULL DEFAULT 0,
            last_pruned_at TEXT,
            updated_at TEXT NOT NULL
        );",
    )
    .context("failed ensuring recent raw journal tables exist")
}

fn write_recent_raw_batch_on_conn(
    conn: &Connection,
    swaps: &[SwapEvent],
    completed_at: DateTime<Utc>,
    deadline: Instant,
) -> Result<(RecentRawJournalWriteSummary, bool)> {
    ensure_recent_raw_journal_tables_on_conn(conn)?;
    let sqlite_variable_limit = sqlite_variable_limit(conn);
    let chunk_rows = effective_bulk_insert_chunk_rows(sqlite_variable_limit);
    let mut statement_count = 0usize;
    let mut inserted_rows = 0usize;
    let mut processed_rows = 0usize;
    let mut exhausted = false;
    let mut before_statement = false;
    let mut execute_ms = 0u64;
    for chunk in swaps.chunks(chunk_rows) {
        if Instant::now() >= deadline {
            before_statement = true;
            exhausted = true;
            break;
        }
        let sql = bulk_insert_sql(chunk.len());
        let mut values = Vec::with_capacity(chunk.len() * BULK_INSERT_PARAMS_PER_ROW);
        for swap in chunk {
            push_bulk_insert_values(&mut values, swap);
        }
        statement_count = statement_count.saturating_add(1);
        let execute_started = Instant::now();
        let changed = conn
            .prepare_cached(&sql)
            .context("failed to prepare recent raw journal bulk insert statement")?
            .execute(params_from_iter(values))
            .context("failed to bulk insert observed swaps into recent raw journal batch")?;
        execute_ms = execute_ms.saturating_add(elapsed_ms(execute_started));
        processed_rows = processed_rows.saturating_add(chunk.len());
        inserted_rows = inserted_rows.saturating_add(changed);
    }
    let mut state = recent_raw_journal_state_cached_query(conn)?;
    advance_recent_raw_journal_state_for_batch(
        &mut state,
        &swaps[..processed_rows],
        inserted_rows,
        completed_at,
    );
    if processed_rows > 0 {
        upsert_recent_raw_journal_state_on_conn(conn, &state)?;
    }
    let mut summary = recent_raw_journal_write_summary(&state, processed_rows, inserted_rows);
    summary.recent_raw_bulk_sqlite_variable_limit = sqlite_variable_limit;
    summary.recent_raw_bulk_statement_params_per_row = BULK_INSERT_PARAMS_PER_ROW;
    summary.recent_raw_bulk_statement_chunk_row_cap = BULK_INSERT_HARD_CAP_ROWS;
    summary.recent_raw_bulk_effective_statement_chunk_rows = chunk_rows;
    summary.recent_raw_bulk_statement_count = statement_count;
    summary.recent_raw_bulk_rows_processed = processed_rows;
    summary.recent_raw_bulk_rows_inserted = inserted_rows;
    summary.recent_raw_bulk_execute_duration_ms = execute_ms;
    summary.recent_raw_bulk_deadline_exhausted_before_statement = before_statement;
    Ok((summary, exhausted))
}
