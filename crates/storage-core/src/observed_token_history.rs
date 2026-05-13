use crate::observed_budget::{interrupted_after_deadline, sqlite_progress_deadline};
use crate::observed_row::row_to_swap_event;
use crate::observed_timestamp::ensure_observed_swaps_timestamps_canonical_utc_read_only;
use crate::{ObservedSwapCursorPage, SqliteDiscoveryStore};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::SwapEvent;
use rusqlite::params;
use std::time::Instant;

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

impl SqliteDiscoveryStore {
    pub fn for_each_sol_leg_observed_swap_for_tokens_in_window_with_budget<F>(
        &self,
        tokens: &[String],
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        limit: usize,
        deadline: Instant,
        mut on_swap: F,
    ) -> Result<ObservedSwapCursorPage>
    where
        F: FnMut(SwapEvent) -> Result<()>,
    {
        if tokens.is_empty()
            || limit == 0
            || Instant::now() >= deadline
            || !self.sqlite_table_exists("observed_swaps")?
        {
            return Ok(ObservedSwapCursorPage {
                rows_seen: 0,
                time_budget_exhausted: Instant::now() >= deadline,
            });
        }
        ensure_observed_swaps_timestamps_canonical_utc_read_only(&self.conn)?;
        let _deadline_guard = sqlite_progress_deadline(&self.conn, deadline);
        let since_raw = since.to_rfc3339();
        let until_raw = until.to_rfc3339();
        let mut seen = 0usize;

        for token in tokens {
            if token == SOL_MINT || token.is_empty() {
                continue;
            }
            if Instant::now() >= deadline {
                return Ok(ObservedSwapCursorPage {
                    rows_seen: seen,
                    time_budget_exhausted: true,
                });
            }
            let remaining = limit.saturating_sub(seen);
            if remaining == 0 {
                break;
            }
            let query_limit = remaining.min(i64::MAX as usize) as i64;
            let mut stmt = match self.conn.prepare(
                "SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                        qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals
                 FROM (
                    SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                           qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals
                    FROM observed_swaps INDEXED BY idx_observed_swaps_token_out_in_ts
                    WHERE token_out = ?1
                      AND token_in = ?2
                      AND ts >= ?3
                      AND ts <= ?4
                    UNION ALL
                    SELECT signature, wallet_id, dex, token_in, token_out, qty_in, qty_out, slot, ts,
                           qty_in_raw, qty_in_decimals, qty_out_raw, qty_out_decimals
                    FROM observed_swaps INDEXED BY idx_observed_swaps_token_in_out_ts
                    WHERE token_in = ?1
                      AND token_out = ?2
                      AND ts >= ?3
                      AND ts <= ?4
                 )
                 ORDER BY ts ASC, slot ASC, signature ASC
                 LIMIT ?5",
            ) {
                Ok(stmt) => stmt,
                Err(error) if interrupted_after_deadline(&error, deadline) => {
                    return Ok(ObservedSwapCursorPage {
                        rows_seen: seen,
                        time_budget_exhausted: true,
                    })
                }
                Err(error) => {
                    return Err(error)
                        .context("failed preparing observed_swaps token history query")
                }
            };
            let mut rows = match stmt.query(params![
                token,
                SOL_MINT,
                &since_raw,
                &until_raw,
                query_limit
            ]) {
                Ok(rows) => rows,
                Err(error) if interrupted_after_deadline(&error, deadline) => {
                    return Ok(ObservedSwapCursorPage {
                        rows_seen: seen,
                        time_budget_exhausted: true,
                    })
                }
                Err(error) => {
                    return Err(error).context("failed querying observed_swaps token history")
                }
            };
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
                        return Err(error)
                            .context("failed reading observed_swaps token history row")
                    }
                }) else {
                    break;
                };
                on_swap(row_to_swap_event(row)?)?;
                seen = seen.saturating_add(1);
                if seen >= limit {
                    return Ok(ObservedSwapCursorPage {
                        rows_seen: seen,
                        time_budget_exhausted: false,
                    });
                }
            }
        }
        Ok(ObservedSwapCursorPage {
            rows_seen: seen,
            time_budget_exhausted: false,
        })
    }
}
