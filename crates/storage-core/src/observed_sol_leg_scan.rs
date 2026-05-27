use crate::observed_budget::{interrupted_after_deadline, sqlite_progress_deadline};
use crate::observed_row::parse_sqlite_slot;
use crate::observed_timestamp::{
    ensure_observed_swaps_timestamps_canonical_utc_read_only, parse_rfc3339_utc,
};
use crate::{
    DiscoveryRuntimeCursor, ObservedSolLegSwap, ObservedSwapCursorPage, SqliteDiscoveryStore,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::SwapEvent;
use rusqlite::{params_from_iter, types::Value as SqlValue};
use std::time::{Duration, Instant};

const OBSERVED_SOL_LEG_CURSOR_QUERY_PAGE_LIMIT: usize = 2_048;
const OBSERVED_SOL_LEG_CURSOR_PAGE_IDLE_MS: u64 = 10;

impl SqliteDiscoveryStore {
    pub fn for_each_sol_leg_observed_swap_in_window_after_cursor_with_budget<F>(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        cursor: Option<&DiscoveryRuntimeCursor>,
        limit: usize,
        deadline: Instant,
        mut on_swap: F,
    ) -> Result<ObservedSwapCursorPage>
    where
        F: FnMut(SwapEvent) -> Result<()>,
    {
        self.for_each_sol_leg_swap_in_window_after_cursor_with_budget(
            since,
            until,
            cursor,
            limit,
            deadline,
            |swap| on_swap(swap.into_swap_event()),
        )
    }

    pub fn for_each_sol_leg_swap_in_window_after_cursor_with_budget<F>(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        cursor: Option<&DiscoveryRuntimeCursor>,
        limit: usize,
        deadline: Instant,
        mut on_swap: F,
    ) -> Result<ObservedSwapCursorPage>
    where
        F: FnMut(ObservedSolLegSwap) -> Result<()>,
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

        let use_projection = self.observed_sol_leg_projection_covers_window(since, until)?;
        let mut total_rows_seen = 0usize;
        let mut time_budget_exhausted = false;
        let mut page_cursor = cursor.cloned();

        while total_rows_seen < limit && Instant::now() < deadline {
            let page_limit = limit
                .saturating_sub(total_rows_seen)
                .min(OBSERVED_SOL_LEG_CURSOR_QUERY_PAGE_LIMIT);
            let mut next_cursor = page_cursor.clone();
            let page = if use_projection {
                self.for_each_observed_sol_leg_projection_in_window_after_cursor_with_budget(
                    since,
                    until,
                    page_cursor.as_ref(),
                    page_limit,
                    deadline,
                    |swap| {
                        next_cursor = Some(DiscoveryRuntimeCursor {
                            ts_utc: swap.ts_utc,
                            slot: swap.slot,
                            signature: swap.signature.clone(),
                        });
                        on_swap(swap)
                    },
                )?
            } else {
                self.for_each_sol_leg_swap_from_observed_with_budget(
                    since,
                    until,
                    page_cursor.as_ref(),
                    page_limit,
                    deadline,
                    |swap| {
                        next_cursor = Some(DiscoveryRuntimeCursor {
                            ts_utc: swap.ts_utc,
                            slot: swap.slot,
                            signature: swap.signature.clone(),
                        });
                        on_swap(swap)
                    },
                )?
            };
            total_rows_seen = total_rows_seen.saturating_add(page.rows_seen);
            time_budget_exhausted |= page.time_budget_exhausted;
            if page.time_budget_exhausted || page.rows_seen < page_limit {
                break;
            }
            page_cursor = next_cursor;
            self.release_sol_leg_cursor_page_reader();
        }

        if Instant::now() >= deadline && total_rows_seen < limit {
            time_budget_exhausted = true;
        }

        Ok(ObservedSwapCursorPage {
            rows_seen: total_rows_seen,
            time_budget_exhausted,
        })
    }

    fn for_each_sol_leg_swap_from_observed_with_budget<F>(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        cursor: Option<&DiscoveryRuntimeCursor>,
        limit: usize,
        deadline: Instant,
        mut on_swap: F,
    ) -> Result<ObservedSwapCursorPage>
    where
        F: FnMut(ObservedSolLegSwap) -> Result<()>,
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
        let (query, values) = sol_leg_query_params(since_raw, until_raw, cursor, limit);
        let _deadline_guard = sqlite_progress_deadline(&self.conn, deadline);
        let mut stmt = match self.conn.prepare(query) {
            Ok(stmt) => stmt,
            Err(error) if interrupted_after_deadline(&error, deadline) => {
                return Ok(ObservedSwapCursorPage {
                    rows_seen: 0,
                    time_budget_exhausted: true,
                });
            }
            Err(error) => {
                return Err(error).context("failed preparing observed SOL-leg cursor query")
            }
        };
        let mut rows = match stmt.query(params_from_iter(values)) {
            Ok(rows) => rows,
            Err(error) if interrupted_after_deadline(&error, deadline) => {
                return Ok(ObservedSwapCursorPage {
                    rows_seen: 0,
                    time_budget_exhausted: true,
                });
            }
            Err(error) => return Err(error).context("failed querying observed SOL-leg cursor"),
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
                    return Err(error).context("failed reading observed SOL-leg cursor row")
                }
            }) else {
                break;
            };
            on_swap(observed_sol_leg_row_to_swap(row)?)?;
            seen = seen.saturating_add(1);
        }
        Ok(ObservedSwapCursorPage {
            rows_seen: seen,
            time_budget_exhausted: false,
        })
    }

    fn release_sol_leg_cursor_page_reader(&self) {
        if !self.is_read_only() {
            let _ = self.checkpoint_wal_passive();
        }
        if OBSERVED_SOL_LEG_CURSOR_PAGE_IDLE_MS > 0 {
            std::thread::sleep(Duration::from_millis(OBSERVED_SOL_LEG_CURSOR_PAGE_IDLE_MS));
        }
    }
}

fn sol_leg_query_params(
    since_raw: String,
    until_raw: String,
    cursor: Option<&DiscoveryRuntimeCursor>,
    limit: i64,
) -> (&'static str, Vec<SqlValue>) {
    if let Some(cursor) = cursor {
        return (
            SOL_LEG_QUERY_AFTER_CURSOR,
            vec![
                since_raw.into(),
                until_raw.into(),
                cursor.ts_utc.to_rfc3339().into(),
                (cursor.slot as i64).into(),
                cursor.signature.clone().into(),
                limit.into(),
            ],
        );
    }
    (
        SOL_LEG_QUERY,
        vec![since_raw.into(), until_raw.into(), limit.into()],
    )
}

fn observed_sol_leg_row_to_swap(row: &rusqlite::Row<'_>) -> Result<ObservedSolLegSwap> {
    let is_buy_raw: i64 = row.get(2)?;
    let is_buy = match is_buy_raw {
        1 => true,
        0 => false,
        _ => anyhow::bail!("observed_swaps SOL-leg is_buy is invalid: {is_buy_raw}"),
    };
    let slot_raw: i64 = row.get(6)?;
    let ts_raw: String = row.get(7)?;
    Ok(ObservedSolLegSwap {
        signature: row.get(0)?,
        wallet_id: row.get(1)?,
        is_buy,
        token_mint: row.get(3)?,
        token_qty: row.get(4)?,
        sol_notional: row.get(5)?,
        slot: parse_sqlite_slot(slot_raw, "observed_swaps.sol_leg.slot")?,
        ts_utc: parse_rfc3339_utc(&ts_raw, "observed_swaps.sol_leg.ts")?,
    })
}

const SOL_LEG_QUERY: &str = "
SELECT signature, wallet_id,
       CASE WHEN token_in = 'So11111111111111111111111111111111111111112' THEN 1 ELSE 0 END AS is_buy,
       CASE WHEN token_in = 'So11111111111111111111111111111111111111112' THEN token_out ELSE token_in END AS token_mint,
       CASE WHEN token_in = 'So11111111111111111111111111111111111111112' THEN qty_out ELSE qty_in END AS token_qty,
       CASE WHEN token_in = 'So11111111111111111111111111111111111111112' THEN qty_in ELSE qty_out END AS sol_notional,
       slot, ts
FROM observed_swaps INDEXED BY idx_observed_swaps_sol_leg_ts_slot_signature
WHERE ts >= ?1
  AND ts <= ?2
  AND (token_in = 'So11111111111111111111111111111111111111112'
       OR token_out = 'So11111111111111111111111111111111111111112')
ORDER BY ts ASC, slot ASC, signature ASC
LIMIT ?3";

const SOL_LEG_QUERY_AFTER_CURSOR: &str = "
SELECT signature, wallet_id,
       CASE WHEN token_in = 'So11111111111111111111111111111111111111112' THEN 1 ELSE 0 END AS is_buy,
       CASE WHEN token_in = 'So11111111111111111111111111111111111111112' THEN token_out ELSE token_in END AS token_mint,
       CASE WHEN token_in = 'So11111111111111111111111111111111111111112' THEN qty_out ELSE qty_in END AS token_qty,
       CASE WHEN token_in = 'So11111111111111111111111111111111111111112' THEN qty_in ELSE qty_out END AS sol_notional,
       slot, ts
FROM observed_swaps INDEXED BY idx_observed_swaps_sol_leg_ts_slot_signature
WHERE ts >= ?1
  AND ts <= ?2
  AND (token_in = 'So11111111111111111111111111111111111111112'
       OR token_out = 'So11111111111111111111111111111111111111112')
  AND (ts, slot, signature) > (?3, ?4, ?5)
ORDER BY ts ASC, slot ASC, signature ASC
LIMIT ?6";
