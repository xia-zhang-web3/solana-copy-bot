use crate::observed_budget::{interrupted_after_deadline, sqlite_progress_deadline};
use crate::observed_row::row_to_swap_event;
use crate::observed_timestamp::ensure_observed_swaps_timestamps_canonical_utc_read_only;
use crate::{ObservedSwapCursorPage, SqliteDiscoveryStore};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::SwapEvent;
use rusqlite::{params, params_from_iter, types::Value as SqlValue};
use std::time::Instant;

impl SqliteDiscoveryStore {
    pub fn discovery_v2_buy_gate_wallet_ids_in_window_with_budget(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        min_buy_count: u32,
        min_buy_notional_sol: f64,
        deadline: Instant,
    ) -> Result<(Vec<String>, bool)> {
        if Instant::now() >= deadline || !self.sqlite_table_exists("observed_swaps")? {
            return Ok((Vec::new(), Instant::now() >= deadline));
        }
        ensure_observed_swaps_timestamps_canonical_utc_read_only(&self.conn)?;
        let _deadline_guard = sqlite_progress_deadline(&self.conn, deadline);
        let mut stmt = match self.conn.prepare(
            "SELECT wallet_id
             FROM observed_swaps INDEXED BY idx_observed_swaps_sol_leg_ts_slot_signature
             WHERE ts >= ?1
               AND ts <= ?2
               AND token_in = 'So11111111111111111111111111111111111111112'
               AND token_out <> 'So11111111111111111111111111111111111111112'
               AND qty_in > 0.0
               AND qty_out > 0.0
             GROUP BY wallet_id
             HAVING COUNT(*) >= ?3
                AND MAX(qty_in) + 1e-12 >= ?4",
        ) {
            Ok(stmt) => stmt,
            Err(error) if interrupted_after_deadline(&error, deadline) => {
                return Ok((Vec::new(), true));
            }
            Err(error) => {
                return Err(error).context("failed preparing discovery v2 buy-gate wallet query")
            }
        };
        let mut rows = match stmt.query(params![
            since.to_rfc3339(),
            until.to_rfc3339(),
            i64::from(min_buy_count),
            min_buy_notional_sol.max(0.0),
        ]) {
            Ok(rows) => rows,
            Err(error) if interrupted_after_deadline(&error, deadline) => {
                return Ok((Vec::new(), true));
            }
            Err(error) => {
                return Err(error).context("failed querying discovery v2 buy-gate wallets")
            }
        };
        let mut wallet_ids = Vec::new();
        loop {
            if Instant::now() >= deadline {
                return Ok((Vec::new(), true));
            }
            let Some(row) = (match rows.next() {
                Ok(row) => row,
                Err(error) if interrupted_after_deadline(&error, deadline) => {
                    return Ok((Vec::new(), true));
                }
                Err(error) => {
                    return Err(error).context("failed reading discovery v2 buy-gate wallet row")
                }
            }) else {
                break;
            };
            wallet_ids.push(row.get::<_, String>(0)?);
        }
        Ok((wallet_ids, false))
    }

    pub fn for_each_sol_leg_observed_swap_in_window_for_wallets_with_budget<F>(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        wallet_ids: &[String],
        limit: usize,
        deadline: Instant,
        on_swap: F,
    ) -> Result<ObservedSwapCursorPage>
    where
        F: FnMut(SwapEvent) -> Result<()>,
    {
        if wallet_ids.is_empty() {
            return Ok(ObservedSwapCursorPage {
                rows_seen: 0,
                time_budget_exhausted: Instant::now() >= deadline,
            });
        }
        let variable_limit = sqlite_variable_limit(&self.conn).saturating_sub(4);
        if wallet_ids.len() > variable_limit {
            anyhow::bail!(
                "discovery v2 wallet-filter query needs {} variables but sqlite limit is {}",
                wallet_ids.len(),
                variable_limit
            );
        }
        self.for_each_sol_leg_observed_swap_in_window_for_wallets_unchecked(
            since, until, wallet_ids, limit, deadline, on_swap,
        )
    }

    fn for_each_sol_leg_observed_swap_in_window_for_wallets_unchecked<F>(
        &self,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        wallet_ids: &[String],
        limit: usize,
        deadline: Instant,
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
        let placeholders = std::iter::repeat_n("?", wallet_ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!(
            "SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                    qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals
             FROM observed_swaps INDEXED BY idx_observed_swaps_sol_leg_ts_slot_signature
             WHERE ts >= ?1
               AND ts <= ?2
               AND (token_in = 'So11111111111111111111111111111111111111112'
                    OR token_out = 'So11111111111111111111111111111111111111112')
               AND wallet_id IN ({placeholders})
             ORDER BY ts ASC, slot ASC, signature ASC
             LIMIT ?{}",
            wallet_ids.len() + 3
        );
        let mut values = Vec::with_capacity(wallet_ids.len() + 3);
        values.push(SqlValue::Text(since.to_rfc3339()));
        values.push(SqlValue::Text(until.to_rfc3339()));
        values.extend(wallet_ids.iter().cloned().map(SqlValue::Text));
        values.push(SqlValue::Integer(limit.min(i64::MAX as usize) as i64));

        let _deadline_guard = sqlite_progress_deadline(&self.conn, deadline);
        let mut stmt = match self.conn.prepare(&query) {
            Ok(stmt) => stmt,
            Err(error) if interrupted_after_deadline(&error, deadline) => {
                return Ok(ObservedSwapCursorPage {
                    rows_seen: 0,
                    time_budget_exhausted: true,
                });
            }
            Err(error) => {
                return Err(error).context("failed preparing discovery v2 wallet-filter scan")
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
            Err(error) => {
                return Err(error).context("failed querying discovery v2 wallet-filter scan")
            }
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
                    return Err(error).context("failed reading discovery v2 wallet-filter row")
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
}

fn sqlite_variable_limit(conn: &rusqlite::Connection) -> usize {
    unsafe {
        rusqlite::ffi::sqlite3_limit(
            conn.handle(),
            rusqlite::ffi::SQLITE_LIMIT_VARIABLE_NUMBER,
            -1,
        )
    }
    .max(0) as usize
}
