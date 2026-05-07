use crate::{
    money::{
        merge_position_qty_exact_on_sell, shadow_lot_cost_lamports, signed_lamports_to_sol,
        signed_lamports_to_sql_i64, sol_to_lamports_ceil, sol_to_lamports_floor,
        split_token_quantity_pro_rata, u64_to_sql_i64,
    },
    shadow_lots::{read_shadow_lot_row_sql, reject_zero_raw_exact_qty, to_sql_conversion_error},
    ShadowCloseOutcome, ShadowLotRow, SqliteDiscoveryStore, SHADOW_CLOSE_CONTEXT_MARKET,
    SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY, SHADOW_LOT_OPEN_EPS,
    SHADOW_RISK_CONTEXT_QUARANTINED_LEGACY,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::{SignedLamports, TokenQuantity};
use rusqlite::params;
use std::time::Duration as StdDuration;

const SQLITE_WRITE_MAX_RETRIES: usize = 3;
const SQLITE_WRITE_RETRY_BACKOFF_MS: [u64; SQLITE_WRITE_MAX_RETRIES] = [100, 300, 700];

impl SqliteDiscoveryStore {
    pub fn close_shadow_lots_fifo_atomic_exact(
        &self,
        signal_id: &str,
        wallet_id: &str,
        token: &str,
        target_qty: f64,
        target_qty_exact: Option<TokenQuantity>,
        exit_price_sol: f64,
        closed_ts: DateTime<Utc>,
    ) -> Result<ShadowCloseOutcome> {
        self.close_shadow_lots_fifo_atomic_exact_with_context(
            signal_id,
            wallet_id,
            token,
            target_qty,
            target_qty_exact,
            exit_price_sol,
            SHADOW_CLOSE_CONTEXT_MARKET,
            closed_ts,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn close_shadow_lots_fifo_atomic_exact_with_context(
        &self,
        signal_id: &str,
        wallet_id: &str,
        token: &str,
        target_qty: f64,
        target_qty_exact: Option<TokenQuantity>,
        exit_price_sol: f64,
        close_context: &str,
        closed_ts: DateTime<Utc>,
    ) -> Result<ShadowCloseOutcome> {
        let target_qty_exact =
            reject_zero_raw_exact_qty(target_qty_exact, "shadow fifo close target qty")?;
        if target_qty <= SHADOW_LOT_OPEN_EPS {
            return Ok(ShadowCloseOutcome {
                has_open_lots_after: self.has_shadow_lots(wallet_id, token)?,
                ..ShadowCloseOutcome::default()
            });
        }
        for attempt in 0..=SQLITE_WRITE_MAX_RETRIES {
            match self.close_shadow_lots_fifo_atomic_once(
                signal_id,
                wallet_id,
                token,
                target_qty,
                target_qty_exact,
                exit_price_sol,
                close_context,
                closed_ts,
            ) {
                Ok(outcome) => return Ok(outcome),
                Err(error) => {
                    let retryable = crate::sqlite_retry::is_retryable_sqlite_error(&error);
                    if attempt < SQLITE_WRITE_MAX_RETRIES && retryable {
                        std::thread::sleep(StdDuration::from_millis(
                            SQLITE_WRITE_RETRY_BACKOFF_MS[attempt],
                        ));
                        continue;
                    }
                    return Err(error).context("failed to close shadow fifo lots atomically");
                }
            }
        }
        unreachable!("retry loop must return on success or terminal error");
    }

    #[allow(clippy::too_many_arguments)]
    fn close_shadow_lots_fifo_atomic_once(
        &self,
        signal_id: &str,
        wallet_id: &str,
        token: &str,
        target_qty: f64,
        target_qty_exact: Option<TokenQuantity>,
        exit_price_sol: f64,
        close_context: &str,
        closed_ts: DateTime<Utc>,
    ) -> rusqlite::Result<ShadowCloseOutcome> {
        self.conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;
        let close_result = self.close_shadow_lots_fifo_in_transaction(
            signal_id,
            wallet_id,
            token,
            target_qty,
            target_qty_exact,
            exit_price_sol,
            close_context,
            closed_ts,
        );
        match close_result {
            Ok(outcome) => match self.conn.execute_batch("COMMIT") {
                Ok(()) => Ok(outcome),
                Err(error) => {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    Err(error)
                }
            },
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn close_shadow_lots_fifo_in_transaction(
        &self,
        signal_id: &str,
        wallet_id: &str,
        token: &str,
        target_qty: f64,
        target_qty_exact: Option<TokenQuantity>,
        exit_price_sol: f64,
        close_context: &str,
        closed_ts: DateTime<Utc>,
    ) -> rusqlite::Result<ShadowCloseOutcome> {
        let mut qty_remaining = target_qty;
        let mut qty_remaining_exact = target_qty_exact;
        let mut closed_qty = 0.0;
        let mut realized_pnl_sol = 0.0;
        let lots = self.load_fifo_lots_for_close(wallet_id, token)?;
        for lot in lots {
            if qty_remaining <= SHADOW_LOT_OPEN_EPS {
                break;
            }
            if lot.qty <= SHADOW_LOT_OPEN_EPS {
                self.conn
                    .execute("DELETE FROM shadow_lots WHERE id = ?1", params![lot.id])?;
                continue;
            }
            let take_qty = qty_remaining.min(lot.qty);
            let final_segment = (qty_remaining - take_qty) <= SHADOW_LOT_OPEN_EPS;
            let current_qty_exact = lot.qty_exact;
            let (segment_qty_exact, next_remaining_qty_exact) = match qty_remaining_exact {
                Some(total_exact) => split_token_quantity_pro_rata(
                    total_exact,
                    take_qty,
                    qty_remaining,
                    final_segment,
                    "shadow fifo close qty_exact",
                )
                .map_err(to_sql_conversion_error)?,
                None => (None, None),
            };
            let segment_qty_exact =
                reject_zero_raw_exact_qty(segment_qty_exact, "shadow fifo close segment qty")
                    .map_err(to_sql_conversion_error)?;
            let closed_qty_exact = match (current_qty_exact, segment_qty_exact) {
                (Some(current), Some(segment)) if current.decimals() == segment.decimals() => {
                    let raw = current.raw().min(segment.raw());
                    (raw > 0).then(|| TokenQuantity::new(raw, current.decimals()))
                }
                _ => None,
            };
            let entry_cost_sol = lot.cost_sol * (take_qty / lot.qty);
            let remaining_qty = (lot.qty - take_qty).max(0.0);
            let remaining_cost = (lot.cost_sol - entry_cost_sol).max(0.0);
            let lot_cost_lamports = shadow_lot_cost_lamports(
                lot.cost_sol,
                lot.cost_lamports.map(|value| value.as_u64() as i64),
                "shadow fifo close lot",
            )
            .map_err(to_sql_conversion_error)?;
            let entry_cost_lamports = if remaining_qty <= SHADOW_LOT_OPEN_EPS {
                lot_cost_lamports
            } else {
                sol_to_lamports_ceil(entry_cost_sol, "shadow closed trade entry_cost_sol")
                    .map(|estimated| estimated.min(lot_cost_lamports))
                    .map_err(to_sql_conversion_error)?
            };
            let remaining_cost_lamports = lot_cost_lamports
                .checked_sub(entry_cost_lamports)
                .ok_or_else(|| to_sql_conversion_error(anyhow!("shadow lot cost underflow")))?;
            let remaining_qty_exact = merge_position_qty_exact_on_sell(
                current_qty_exact,
                closed_qty_exact,
                remaining_qty <= SHADOW_LOT_OPEN_EPS,
            )
            .map_err(to_sql_conversion_error)?;
            if remaining_qty <= SHADOW_LOT_OPEN_EPS {
                self.conn
                    .execute("DELETE FROM shadow_lots WHERE id = ?1", params![lot.id])?;
            } else {
                let remaining_qty_exact = reject_zero_raw_exact_qty(
                    remaining_qty_exact,
                    "shadow fifo close remaining qty",
                )
                .map_err(to_sql_conversion_error)?;
                self.conn.execute(
                    "UPDATE shadow_lots SET qty = ?1, qty_raw = ?2, qty_decimals = ?3, cost_sol = ?4, cost_lamports = ?5 WHERE id = ?6",
                    params![
                        remaining_qty,
                        remaining_qty_exact.as_ref().map(|value| value.raw().to_string()),
                        remaining_qty_exact.as_ref().map(|value| i64::from(value.decimals())),
                        remaining_cost,
                        u64_to_sql_i64(
                            "shadow_lots.cost_lamports",
                            remaining_cost_lamports.as_u64(),
                        )
                        .map_err(to_sql_conversion_error)?,
                        lot.id
                    ],
                )?;
            }
            let exit_value_sol = take_qty * exit_price_sol;
            let exit_value_lamports =
                sol_to_lamports_floor(exit_value_sol, "shadow closed trade exit_value_sol")
                    .map_err(to_sql_conversion_error)?;
            let effective_context = if close_context == SHADOW_CLOSE_CONTEXT_MARKET
                && lot.risk_context == SHADOW_RISK_CONTEXT_QUARANTINED_LEGACY
            {
                SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY
            } else {
                close_context
            };
            let pnl_lamports = SignedLamports::new(
                i128::from(exit_value_lamports.as_u64()) - i128::from(entry_cost_lamports.as_u64()),
            );
            self.conn.execute(
                "INSERT INTO shadow_closed_trades(
                    signal_id, wallet_id, token, close_context, accounting_bucket,
                    qty, qty_raw, qty_decimals, entry_cost_sol, entry_cost_lamports,
                    exit_value_sol, exit_value_lamports, pnl_sol, pnl_lamports,
                    opened_ts, closed_ts
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
                params![
                    signal_id,
                    wallet_id,
                    token,
                    effective_context,
                    lot.accounting_bucket,
                    take_qty,
                    closed_qty_exact
                        .as_ref()
                        .map(|value| value.raw().to_string()),
                    closed_qty_exact
                        .as_ref()
                        .map(|value| i64::from(value.decimals())),
                    entry_cost_sol,
                    u64_to_sql_i64(
                        "shadow_closed_trades.entry_cost_lamports",
                        entry_cost_lamports.as_u64()
                    )
                    .map_err(to_sql_conversion_error)?,
                    exit_value_sol,
                    u64_to_sql_i64(
                        "shadow_closed_trades.exit_value_lamports",
                        exit_value_lamports.as_u64()
                    )
                    .map_err(to_sql_conversion_error)?,
                    exit_value_sol - entry_cost_sol,
                    signed_lamports_to_sql_i64("shadow_closed_trades.pnl_lamports", pnl_lamports)
                        .map_err(to_sql_conversion_error)?,
                    lot.opened_ts.to_rfc3339(),
                    closed_ts.to_rfc3339(),
                ],
            )?;
            qty_remaining -= take_qty;
            qty_remaining_exact = next_remaining_qty_exact;
            closed_qty += take_qty;
            realized_pnl_sol += signed_lamports_to_sol(pnl_lamports);
        }
        Ok(ShadowCloseOutcome {
            closed_qty,
            realized_pnl_sol,
            has_open_lots_after: self.has_shadow_lots_sql(wallet_id, token)?,
        })
    }

    fn load_fifo_lots_for_close(
        &self,
        wallet_id: &str,
        token: &str,
    ) -> rusqlite::Result<Vec<ShadowLotRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, wallet_id, token, accounting_bucket, risk_context, qty, qty_raw, qty_decimals, cost_sol, cost_lamports, opened_ts
             FROM shadow_lots
             WHERE wallet_id = ?1 AND token = ?2
             ORDER BY id ASC",
        )?;
        let mut rows = stmt.query(params![wallet_id, token])?;
        let mut lots = Vec::new();
        while let Some(row) = rows.next()? {
            lots.push(read_shadow_lot_row_sql(row)?);
        }
        Ok(lots)
    }

    fn has_shadow_lots_sql(&self, wallet_id: &str, token: &str) -> rusqlite::Result<bool> {
        let remaining_lots: i64 = self.conn.query_row(
            "SELECT COUNT(*)
             FROM shadow_lots
             WHERE wallet_id = ?1 AND token = ?2 AND qty > ?3",
            params![wallet_id, token, SHADOW_LOT_OPEN_EPS],
            |row| row.get(0),
        )?;
        Ok(remaining_lots > 0)
    }
}
