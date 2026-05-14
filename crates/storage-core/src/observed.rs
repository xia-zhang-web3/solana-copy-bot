use crate::observed_budget::{interrupted_after_deadline, sqlite_progress_deadline};
use crate::observed_row::{parse_sqlite_slot, row_to_swap_event};
use crate::observed_timestamp::{
    ensure_observed_swaps_timestamps_canonical_utc_read_only, parse_rfc3339_utc,
};
use crate::{DiscoveryRuntimeCursor, ObservedSwapCursorPage, SqliteDiscoveryStore};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::SwapEvent;
use rusqlite::{params, params_from_iter, types::Value as SqlValue, OptionalExtension};
use std::time::Instant;

impl SqliteDiscoveryStore {
    pub fn observed_swaps_tail_cursor_read_only(&self) -> Result<Option<DiscoveryRuntimeCursor>> {
        if !self.sqlite_table_exists("observed_swaps")? {
            return Ok(None);
        }
        ensure_observed_swaps_timestamps_canonical_utc_read_only(&self.conn)?;
        let raw = self
            .conn
            .query_row(
                "SELECT ts, slot, signature
                 FROM observed_swaps
                 ORDER BY ts DESC, slot DESC, signature DESC
                 LIMIT 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .context("failed loading observed_swaps tail cursor")?;
        raw.map(|(ts, slot, signature)| {
            Ok(DiscoveryRuntimeCursor {
                ts_utc: parse_rfc3339_utc(&ts, "observed_swaps.ts")?,
                slot: parse_sqlite_slot(slot, "observed_swaps.tail.slot")?,
                signature,
            })
        })
        .transpose()
    }

    pub fn observed_swaps_tail_cursor_at_or_before_read_only(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Option<DiscoveryRuntimeCursor>> {
        if !self.sqlite_table_exists("observed_swaps")? {
            return Ok(None);
        }
        ensure_observed_swaps_timestamps_canonical_utc_read_only(&self.conn)?;
        let now_raw = now.to_rfc3339();
        let raw = self
            .conn
            .query_row(
                "SELECT ts, slot, signature
                 FROM observed_swaps
                 WHERE ts <= ?1
                 ORDER BY ts DESC, slot DESC, signature DESC
                 LIMIT 1",
                params![now_raw],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .context("failed loading observed_swaps non-future tail cursor")?;
        raw.map(|(ts, slot, signature)| {
            Ok(DiscoveryRuntimeCursor {
                ts_utc: parse_rfc3339_utc(&ts, "observed_swaps.ts")?,
                slot: parse_sqlite_slot(slot, "observed_swaps.tail_at_or_before.slot")?,
                signature,
            })
        })
        .transpose()
    }

    pub fn observed_swaps_coverage_start_read_only(&self) -> Result<Option<SwapEvent>> {
        if !self.sqlite_table_exists("observed_swaps")? {
            return Ok(None);
        }
        ensure_observed_swaps_timestamps_canonical_utc_read_only(&self.conn)?;
        let mut stmt = self
            .conn
            .prepare(
                "SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                        qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals
                 FROM observed_swaps
                 ORDER BY ts ASC, slot ASC, signature ASC
                 LIMIT 1",
            )
            .context("failed preparing observed_swaps coverage start")?;
        let mut rows = stmt
            .query([])
            .context("failed querying observed_swaps coverage start")?;
        let Some(row) = rows
            .next()
            .context("failed reading observed_swaps coverage start")?
        else {
            return Ok(None);
        };
        Ok(Some(row_to_swap_event(row)?))
    }

    pub fn load_recent_observed_swaps_since(
        &self,
        since: DateTime<Utc>,
        limit: usize,
    ) -> Result<(Vec<SwapEvent>, bool)> {
        if limit == 0 || !self.sqlite_table_exists("observed_swaps")? {
            return Ok((Vec::new(), false));
        }
        ensure_observed_swaps_timestamps_canonical_utc_read_only(&self.conn)?;
        let retained_limit = limit.min(i64::MAX as usize);
        let query_limit = retained_limit.saturating_add(1).min(i64::MAX as usize) as i64;
        let since_raw = since.to_rfc3339();
        let mut stmt = self.conn.prepare(
            "SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                    qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals
             FROM observed_swaps
             WHERE ts >= ?1
             ORDER BY ts DESC, slot DESC, signature DESC
             LIMIT ?2",
        )?;
        let mut rows = stmt.query(params![since_raw, query_limit])?;
        let mut swaps = Vec::new();
        while let Some(row) = rows.next()? {
            swaps.push(row_to_swap_event(row)?);
        }
        let truncated = swaps.len() > retained_limit;
        if truncated {
            swaps.truncate(retained_limit);
        }
        swaps.reverse();
        Ok((swaps, truncated))
    }

    pub fn load_observed_swaps_since(&self, since: DateTime<Utc>) -> Result<Vec<SwapEvent>> {
        if !self.sqlite_table_exists("observed_swaps")? {
            return Ok(Vec::new());
        }
        ensure_observed_swaps_timestamps_canonical_utc_read_only(&self.conn)?;
        let since_raw = since.to_rfc3339();
        let mut stmt = self.conn.prepare(
            "SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                    qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals
             FROM observed_swaps
             WHERE ts >= ?1
             ORDER BY ts ASC, slot ASC, signature ASC",
        )?;
        let mut rows = stmt.query(params![since_raw])?;
        let mut swaps = Vec::new();
        while let Some(row) = rows.next()? {
            swaps.push(row_to_swap_event(row)?);
        }
        Ok(swaps)
    }

    pub fn load_recent_observed_swaps_in_window(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        limit: usize,
    ) -> Result<(Vec<SwapEvent>, bool)> {
        if limit == 0 || !self.sqlite_table_exists("observed_swaps")? {
            return Ok((Vec::new(), false));
        }
        ensure_observed_swaps_timestamps_canonical_utc_read_only(&self.conn)?;
        let retained_limit = limit.min(i64::MAX as usize);
        let query_limit = retained_limit.saturating_add(1).min(i64::MAX as usize) as i64;
        let since_raw = since.to_rfc3339();
        let until_raw = until.to_rfc3339();
        let mut stmt = self.conn.prepare(
            "SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                    qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals
             FROM observed_swaps
             WHERE ts >= ?1 AND ts <= ?2
             ORDER BY ts DESC, slot DESC, signature DESC
             LIMIT ?3",
        )?;
        let mut rows = stmt.query(params![since_raw, until_raw, query_limit])?;
        let mut swaps = Vec::new();
        while let Some(row) = rows.next()? {
            swaps.push(row_to_swap_event(row)?);
        }
        let truncated = swaps.len() > retained_limit;
        if truncated {
            swaps.truncate(retained_limit);
        }
        swaps.reverse();
        Ok((swaps, truncated))
    }

    pub fn load_recent_observed_swaps_in_window_with_budget(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        limit: usize,
        deadline: Instant,
    ) -> Result<(Vec<SwapEvent>, bool, bool)> {
        if limit == 0
            || Instant::now() >= deadline
            || !self.sqlite_table_exists("observed_swaps")?
        {
            return Ok((Vec::new(), false, Instant::now() >= deadline));
        }
        ensure_observed_swaps_timestamps_canonical_utc_read_only(&self.conn)?;
        let retained_limit = limit.min(i64::MAX as usize);
        let query_limit = retained_limit.saturating_add(1).min(i64::MAX as usize) as i64;
        let since_raw = since.to_rfc3339();
        let until_raw = until.to_rfc3339();
        let _deadline_guard = sqlite_progress_deadline(&self.conn, deadline);
        let mut stmt = match self.conn.prepare(
            "SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                    qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals
             FROM observed_swaps
             WHERE ts >= ?1 AND ts <= ?2
             ORDER BY ts DESC, slot DESC, signature DESC
             LIMIT ?3",
        ) {
            Ok(stmt) => stmt,
            Err(error) if interrupted_after_deadline(&error, deadline) => {
                return Ok((Vec::new(), false, true));
            }
            Err(error) => {
                return Err(error).context("failed preparing observed_swaps window query")
            }
        };
        let mut rows = match stmt.query(params![since_raw, until_raw, query_limit]) {
            Ok(rows) => rows,
            Err(error) if interrupted_after_deadline(&error, deadline) => {
                return Ok((Vec::new(), false, true));
            }
            Err(error) => return Err(error).context("failed querying observed_swaps window"),
        };
        let mut swaps = Vec::new();
        loop {
            if Instant::now() >= deadline {
                swaps.reverse();
                return Ok((swaps, false, true));
            }
            let Some(row) = (match rows.next() {
                Ok(row) => row,
                Err(error) if interrupted_after_deadline(&error, deadline) => {
                    swaps.reverse();
                    return Ok((swaps, false, true));
                }
                Err(error) => {
                    return Err(error).context("failed reading observed_swaps window row")
                }
            }) else {
                break;
            };
            swaps.push(row_to_swap_event(row)?);
        }
        let truncated = swaps.len() > retained_limit;
        if truncated {
            swaps.truncate(retained_limit);
        }
        swaps.reverse();
        Ok((swaps, truncated, false))
    }

    pub fn for_each_observed_swap_in_window_after_cursor_with_budget<F>(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        cursor: Option<&DiscoveryRuntimeCursor>,
        limit: usize,
        deadline: Instant,
        on_swap: F,
    ) -> Result<ObservedSwapCursorPage>
    where
        F: FnMut(SwapEvent) -> Result<()>,
    {
        self.for_each_observed_swap_in_window_after_cursor_with_budget_filtered(
            since, until, cursor, limit, deadline, None, on_swap,
        )
    }

    pub fn for_each_sol_leg_observed_swap_in_window_after_cursor_with_budget<F>(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        cursor: Option<&DiscoveryRuntimeCursor>,
        limit: usize,
        deadline: Instant,
        on_swap: F,
    ) -> Result<ObservedSwapCursorPage>
    where
        F: FnMut(SwapEvent) -> Result<()>,
    {
        self.for_each_observed_swap_in_window_after_cursor_with_budget_filtered(
            since,
            until,
            cursor,
            limit,
            deadline,
            Some(()),
            on_swap,
        )
    }

    fn for_each_observed_swap_in_window_after_cursor_with_budget_filtered<F>(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        cursor: Option<&DiscoveryRuntimeCursor>,
        limit: usize,
        deadline: Instant,
        sol_leg_only: Option<()>,
        mut on_swap: F,
    ) -> Result<ObservedSwapCursorPage>
    where
        F: FnMut(SwapEvent) -> Result<()>,
    {
        if limit == 0
            || Instant::now() >= deadline
            || !self.sqlite_table_exists("observed_swaps")?
        {
            return Ok(ObservedSwapCursorPage {
                rows_seen: 0,
                time_budget_exhausted: Instant::now() >= deadline,
            });
        }
        ensure_observed_swaps_timestamps_canonical_utc_read_only(&self.conn)?;
        let limit = limit.min(i64::MAX as usize) as i64;
        let since_raw = since.to_rfc3339();
        let until_raw = until.to_rfc3339();
        let (query, values): (&str, Vec<SqlValue>) = match (cursor, sol_leg_only) {
            (Some(cursor), Some(())) => (
                "SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                        qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals
                 FROM observed_swaps INDEXED BY idx_observed_swaps_sol_leg_ts_slot_signature
                 WHERE ts >= ?1
                   AND ts <= ?2
                   AND (token_in = 'So11111111111111111111111111111111111111112'
                        OR token_out = 'So11111111111111111111111111111111111111112')
                   AND (ts, slot, signature) > (?3, ?4, ?5)
                 ORDER BY ts ASC, slot ASC, signature ASC
                 LIMIT ?6",
                vec![
                    since_raw.clone().into(),
                    until_raw.clone().into(),
                    cursor.ts_utc.to_rfc3339().into(),
                    (cursor.slot as i64).into(),
                    cursor.signature.clone().into(),
                    limit.into(),
                ],
            ),
            (None, Some(())) => (
                "SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                        qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals
                 FROM observed_swaps INDEXED BY idx_observed_swaps_sol_leg_ts_slot_signature
                 WHERE ts >= ?1
                   AND ts <= ?2
                   AND (token_in = 'So11111111111111111111111111111111111111112'
                        OR token_out = 'So11111111111111111111111111111111111111112')
                 ORDER BY ts ASC, slot ASC, signature ASC
                 LIMIT ?3",
                vec![
                    since_raw.clone().into(),
                    until_raw.clone().into(),
                    limit.into(),
                ],
            ),
            (Some(cursor), None) => (
                "SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                        qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals
                 FROM observed_swaps
                 WHERE ts >= ?1
                   AND ts <= ?2
                   AND (ts, slot, signature) > (?3, ?4, ?5)
                 ORDER BY ts ASC, slot ASC, signature ASC
                 LIMIT ?6",
                vec![
                    since_raw.clone().into(),
                    until_raw.clone().into(),
                    cursor.ts_utc.to_rfc3339().into(),
                    (cursor.slot as i64).into(),
                    cursor.signature.clone().into(),
                    limit.into(),
                ],
            ),
            (None, None) => (
                "SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                        qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals
                 FROM observed_swaps
                 WHERE ts >= ?1 AND ts <= ?2
                 ORDER BY ts ASC, slot ASC, signature ASC
                 LIMIT ?3",
                vec![since_raw.into(), until_raw.into(), limit.into()],
            ),
        };
        let _deadline_guard = sqlite_progress_deadline(&self.conn, deadline);
        let mut stmt = match self.conn.prepare(query) {
            Ok(stmt) => stmt,
            Err(error) if interrupted_after_deadline(&error, deadline) => {
                return Ok(ObservedSwapCursorPage {
                    rows_seen: 0,
                    time_budget_exhausted: true,
                })
            }
            Err(error) => {
                return Err(error).context("failed preparing observed_swaps cursor query")
            }
        };
        let mut rows = match stmt.query(params_from_iter(values)) {
            Ok(rows) => rows,
            Err(error) if interrupted_after_deadline(&error, deadline) => {
                return Ok(ObservedSwapCursorPage {
                    rows_seen: 0,
                    time_budget_exhausted: true,
                })
            }
            Err(error) => return Err(error).context("failed querying observed_swaps cursor"),
        };
        let mut seen = 0usize;
        loop {
            if Instant::now() >= deadline {
                return Ok(ObservedSwapCursorPage {
                    rows_seen: seen,
                    time_budget_exhausted: true,
                });
            }
            let Some(row) = (match rows.next() {
                Ok(row) => row,
                Err(error) if interrupted_after_deadline(&error, deadline) => {
                    return Ok(ObservedSwapCursorPage {
                        rows_seen: seen,
                        time_budget_exhausted: true,
                    });
                }
                Err(error) => {
                    return Err(error).context("failed reading observed_swaps cursor row")
                }
            }) else {
                break;
            };
            on_swap(row_to_swap_event(row)?)?;
            seen = seen.saturating_add(1);
        }
        Ok(ObservedSwapCursorPage {
            rows_seen: seen,
            time_budget_exhausted: false,
        })
    }

    pub fn insert_observed_swap(&self, swap: &SwapEvent) -> Result<bool> {
        let in_raw = swap
            .exact_amounts
            .as_ref()
            .map(|value| value.amount_in_raw.as_str());
        let in_decimals = swap
            .exact_amounts
            .as_ref()
            .map(|value| i64::from(value.amount_in_decimals));
        let out_raw = swap
            .exact_amounts
            .as_ref()
            .map(|value| value.amount_out_raw.as_str());
        let out_decimals = swap
            .exact_amounts
            .as_ref()
            .map(|value| i64::from(value.amount_out_decimals));
        let slot = swap.slot as i64;
        let ts = swap.ts_utc.to_rfc3339();
        let changed = self.conn.execute(
            "INSERT OR IGNORE INTO observed_swaps(
                signature, wallet_id, dex, token_in, token_out, qty_in, qty_out,
                qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals, slot, ts
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                &swap.signature,
                &swap.wallet,
                &swap.dex,
                &swap.token_in,
                &swap.token_out,
                swap.amount_in,
                swap.amount_out,
                in_raw,
                in_decimals,
                out_raw,
                out_decimals,
                slot,
                ts,
            ],
        )?;
        Ok(changed > 0)
    }

    pub fn insert_observed_swaps_batch(&self, swaps: &[SwapEvent]) -> Result<Vec<bool>> {
        swaps
            .iter()
            .map(|swap| self.insert_observed_swap(swap))
            .collect()
    }
}
