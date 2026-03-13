use crate::sqlite_retry::is_retryable_sqlite_error;
use crate::{
    POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER, POSITION_ACCOUNTING_BUCKET_LEGACY_PRE_CUTOVER,
    SHADOW_CLOSE_CONTEXT_MARKET, SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY,
    SHADOW_RISK_CONTEXT_MARKET, SHADOW_RISK_CONTEXT_QUARANTINED_LEGACY, SQLITE_WRITE_MAX_RETRIES,
    SQLITE_WRITE_RETRY_BACKOFF_MS, ShadowCloseOutcome, ShadowLotRow, SqliteStore,
    merge_position_qty_exact_on_sell, note_sqlite_busy_error, note_sqlite_write_retry,
    shadow_lot_cost_lamports, signed_lamports_to_sol, signed_lamports_to_sql_i64,
    sol_to_lamports_ceil_storage, sol_to_lamports_floor_storage, split_token_quantity_pro_rata,
    token_quantity_from_sql, u64_to_sql_i64,
};
use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use copybot_core_types::TokenQuantity;
use rusqlite::{OptionalExtension, params};
use std::collections::HashSet;
use std::io;
use std::time::Duration as StdDuration;

pub(crate) const SHADOW_LOT_OPEN_EPS: f64 = 1e-12;

fn to_sql_conversion_error(error: anyhow::Error) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(io::Error::other(error.to_string())))
}

fn reject_zero_raw_exact_qty(
    qty_exact: Option<TokenQuantity>,
    context: &str,
) -> Result<Option<TokenQuantity>> {
    match qty_exact {
        Some(qty_exact) if qty_exact.raw() == 0 => {
            Err(anyhow!("zero-raw exact quantity is invalid in {}", context))
        }
        other => Ok(other),
    }
}

fn shadow_accounting_bucket_for_qty_exact(qty_exact: Option<TokenQuantity>) -> &'static str {
    if qty_exact.is_some() {
        POSITION_ACCOUNTING_BUCKET_EXACT_POST_CUTOVER
    } else {
        POSITION_ACCOUNTING_BUCKET_LEGACY_PRE_CUTOVER
    }
}

fn validate_shadow_risk_context(risk_context: &str) -> Result<()> {
    match risk_context {
        SHADOW_RISK_CONTEXT_MARKET | SHADOW_RISK_CONTEXT_QUARANTINED_LEGACY => Ok(()),
        other => Err(anyhow!("unsupported shadow risk_context: {other}")),
    }
}

impl SqliteStore {
    pub fn insert_shadow_lot(
        &self,
        wallet_id: &str,
        token: &str,
        qty: f64,
        cost_sol: f64,
        opened_ts: DateTime<Utc>,
    ) -> Result<i64> {
        self.insert_shadow_lot_exact_with_risk_context(
            wallet_id,
            token,
            qty,
            None,
            cost_sol,
            SHADOW_RISK_CONTEXT_MARKET,
            opened_ts,
        )
    }

    pub fn insert_shadow_lot_exact(
        &self,
        wallet_id: &str,
        token: &str,
        qty: f64,
        qty_exact: Option<TokenQuantity>,
        cost_sol: f64,
        opened_ts: DateTime<Utc>,
    ) -> Result<i64> {
        self.insert_shadow_lot_exact_with_risk_context(
            wallet_id,
            token,
            qty,
            qty_exact,
            cost_sol,
            SHADOW_RISK_CONTEXT_MARKET,
            opened_ts,
        )
    }

    pub fn insert_shadow_lot_exact_with_risk_context(
        &self,
        wallet_id: &str,
        token: &str,
        qty: f64,
        qty_exact: Option<TokenQuantity>,
        cost_sol: f64,
        risk_context: &str,
        opened_ts: DateTime<Utc>,
    ) -> Result<i64> {
        let qty_exact = reject_zero_raw_exact_qty(qty_exact, "insert shadow lot")?;
        validate_shadow_risk_context(risk_context)?;
        self.execute_with_retry_result(|conn| {
            let cost_lamports = sol_to_lamports_ceil_storage(cost_sol, "shadow lot cost_sol")
                .map_err(to_sql_conversion_error)?;
            let accounting_bucket = shadow_accounting_bucket_for_qty_exact(qty_exact);
            conn.execute(
                "INSERT INTO shadow_lots(
                    wallet_id,
                    token,
                    accounting_bucket,
                    risk_context,
                    qty,
                    qty_raw,
                    qty_decimals,
                    cost_sol,
                    cost_lamports,
                    opened_ts
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    wallet_id,
                    token,
                    accounting_bucket,
                    risk_context,
                    qty,
                    qty_exact.as_ref().map(|value| value.raw().to_string()),
                    qty_exact.as_ref().map(|value| i64::from(value.decimals())),
                    cost_sol,
                    u64_to_sql_i64("shadow_lots.cost_lamports", cost_lamports.as_u64())
                        .map_err(to_sql_conversion_error)?,
                    opened_ts.to_rfc3339()
                ],
            )?;
            Ok(conn.last_insert_rowid())
        })
        .context("failed to insert shadow lot")
    }

    pub fn list_shadow_lots(&self, wallet_id: &str, token: &str) -> Result<Vec<ShadowLotRow>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, wallet_id, token, accounting_bucket, risk_context, qty, qty_raw, qty_decimals, cost_sol, cost_lamports, opened_ts
                 FROM shadow_lots
                 WHERE wallet_id = ?1 AND token = ?2
                 ORDER BY id ASC",
            )
            .context("failed to prepare shadow lot query")?;
        let mut rows = stmt
            .query(params![wallet_id, token])
            .context("failed querying shadow lots")?;

        let mut lots = Vec::new();
        while let Some(row) = rows.next().context("failed iterating shadow lots")? {
            let opened_raw: String = row
                .get(10)
                .context("failed reading shadow_lots.opened_ts")?;
            let opened_ts = DateTime::parse_from_rfc3339(&opened_raw)
                .map(|dt| dt.with_timezone(&Utc))
                .with_context(|| {
                    format!("invalid shadow_lots.opened_ts rfc3339 value: {opened_raw}")
                })?;
            let qty_exact = token_quantity_from_sql(
                row.get(6).context("failed reading shadow_lots.qty_raw")?,
                row.get(7)
                    .context("failed reading shadow_lots.qty_decimals")?,
                "listing shadow lots",
            )?;
            let cost_lamports_raw: Option<i64> = row
                .get(9)
                .context("failed reading shadow_lots.cost_lamports")?;
            let cost_lamports = match cost_lamports_raw {
                Some(raw) if raw < 0 => {
                    return Err(anyhow!(
                        "invalid negative shadow_lots.cost_lamports={} while listing lots",
                        raw
                    ));
                }
                Some(raw) => Some(crate::Lamports::new(raw as u64)),
                None => None,
            };
            lots.push(ShadowLotRow {
                id: row.get(0).context("failed reading shadow_lots.id")?,
                wallet_id: row.get(1).context("failed reading shadow_lots.wallet_id")?,
                token: row.get(2).context("failed reading shadow_lots.token")?,
                accounting_bucket: row
                    .get(3)
                    .context("failed reading shadow_lots.accounting_bucket")?,
                risk_context: row
                    .get(4)
                    .context("failed reading shadow_lots.risk_context")?,
                qty: row.get(5).context("failed reading shadow_lots.qty")?,
                qty_exact,
                cost_sol: row.get(8).context("failed reading shadow_lots.cost_sol")?,
                cost_lamports,
                opened_ts,
            });
        }
        Ok(lots)
    }

    pub fn list_open_shadow_lots_older_than(
        &self,
        cutoff: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<ShadowLotRow>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, wallet_id, token, accounting_bucket, risk_context, qty, qty_raw, qty_decimals, cost_sol, cost_lamports, opened_ts
                 FROM shadow_lots
                 WHERE qty > ?1
                   AND opened_ts <= ?2
                 ORDER BY opened_ts ASC, id ASC
                 LIMIT ?3",
            )
            .context("failed to prepare stale shadow lot query")?;
        let mut rows = stmt
            .query(params![
                SHADOW_LOT_OPEN_EPS,
                cutoff.to_rfc3339(),
                limit.max(1) as i64
            ])
            .context("failed querying stale shadow lots")?;

        let mut lots = Vec::new();
        while let Some(row) = rows.next().context("failed iterating stale shadow lots")? {
            let opened_raw: String = row
                .get(10)
                .context("failed reading shadow_lots.opened_ts")?;
            let opened_ts = DateTime::parse_from_rfc3339(&opened_raw)
                .map(|dt| dt.with_timezone(&Utc))
                .with_context(|| {
                    format!("invalid shadow_lots.opened_ts rfc3339 value: {opened_raw}")
                })?;
            let qty_exact = token_quantity_from_sql(
                row.get(6).context("failed reading shadow_lots.qty_raw")?,
                row.get(7)
                    .context("failed reading shadow_lots.qty_decimals")?,
                "listing stale shadow lots",
            )?;
            let cost_lamports_raw: Option<i64> = row
                .get(9)
                .context("failed reading shadow_lots.cost_lamports")?;
            let cost_lamports = match cost_lamports_raw {
                Some(raw) if raw < 0 => {
                    return Err(anyhow!(
                        "invalid negative shadow_lots.cost_lamports={} while listing stale lots",
                        raw
                    ));
                }
                Some(raw) => Some(crate::Lamports::new(raw as u64)),
                None => None,
            };
            lots.push(ShadowLotRow {
                id: row.get(0).context("failed reading shadow_lots.id")?,
                wallet_id: row.get(1).context("failed reading shadow_lots.wallet_id")?,
                token: row.get(2).context("failed reading shadow_lots.token")?,
                accounting_bucket: row
                    .get(3)
                    .context("failed reading shadow_lots.accounting_bucket")?,
                risk_context: row
                    .get(4)
                    .context("failed reading shadow_lots.risk_context")?,
                qty: row.get(5).context("failed reading shadow_lots.qty")?,
                qty_exact,
                cost_sol: row.get(8).context("failed reading shadow_lots.cost_sol")?,
                cost_lamports,
                opened_ts,
            });
        }

        Ok(lots)
    }

    pub fn update_shadow_lot_risk_context(&self, id: i64, risk_context: &str) -> Result<()> {
        validate_shadow_risk_context(risk_context)?;
        self.execute_with_retry(|conn| {
            conn.execute(
                "UPDATE shadow_lots SET risk_context = ?1 WHERE id = ?2",
                params![risk_context, id],
            )
        })
        .context("failed to update shadow lot risk context")?;
        Ok(())
    }

    pub fn has_shadow_lots(&self, wallet_id: &str, token: &str) -> Result<bool> {
        let has_lots: i64 = self
            .conn
            .query_row(
                "SELECT EXISTS(
                    SELECT 1
                    FROM shadow_lots
                    WHERE wallet_id = ?1
                      AND token = ?2
                      AND qty > ?3
                )",
                params![wallet_id, token, SHADOW_LOT_OPEN_EPS],
                |row| row.get(0),
            )
            .context("failed querying shadow lots existence")?;
        Ok(has_lots > 0)
    }

    pub fn list_shadow_open_pairs(&self) -> Result<HashSet<(String, String)>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT DISTINCT wallet_id, token
                 FROM shadow_lots
                 WHERE qty > ?1",
            )
            .context("failed to prepare shadow open lots query")?;
        let mut rows = stmt
            .query(params![SHADOW_LOT_OPEN_EPS])
            .context("failed querying shadow open lots")?;

        let mut pairs = HashSet::new();
        while let Some(row) = rows.next().context("failed iterating shadow open lots")? {
            let wallet_id: String = row
                .get(0)
                .context("failed reading shadow_lots.wallet_id in open lots query")?;
            let token: String = row
                .get(1)
                .context("failed reading shadow_lots.token in open lots query")?;
            pairs.insert((wallet_id, token));
        }
        Ok(pairs)
    }

    pub fn close_shadow_lots_fifo_atomic(
        &self,
        signal_id: &str,
        wallet_id: &str,
        token: &str,
        target_qty: f64,
        exit_price_sol: f64,
        closed_ts: DateTime<Utc>,
    ) -> Result<ShadowCloseOutcome> {
        self.close_shadow_lots_fifo_atomic_with_context(
            signal_id,
            wallet_id,
            token,
            target_qty,
            exit_price_sol,
            SHADOW_CLOSE_CONTEXT_MARKET,
            closed_ts,
        )
    }

    pub fn close_shadow_lots_fifo_atomic_with_context(
        &self,
        signal_id: &str,
        wallet_id: &str,
        token: &str,
        target_qty: f64,
        exit_price_sol: f64,
        close_context: &str,
        closed_ts: DateTime<Utc>,
    ) -> Result<ShadowCloseOutcome> {
        self.close_shadow_lots_fifo_atomic_exact_with_context(
            signal_id,
            wallet_id,
            token,
            target_qty,
            None,
            exit_price_sol,
            close_context,
            closed_ts,
        )
    }

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
        const EPS: f64 = SHADOW_LOT_OPEN_EPS;
        let target_qty_exact =
            reject_zero_raw_exact_qty(target_qty_exact, "shadow fifo close target qty")?;

        if target_qty <= EPS {
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
                    let retryable = is_retryable_sqlite_error(&error);
                    if retryable {
                        note_sqlite_busy_error();
                    }
                    if attempt < SQLITE_WRITE_MAX_RETRIES && retryable {
                        note_sqlite_write_retry();
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
        const EPS: f64 = SHADOW_LOT_OPEN_EPS;

        self.conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;
        let close_result = (|| -> rusqlite::Result<ShadowCloseOutcome> {
            let mut qty_remaining = target_qty;
            let mut qty_remaining_exact = target_qty_exact;
            let mut closed_qty = 0.0;
            let mut realized_pnl_sol = 0.0;

            let mut stmt = self.conn.prepare(
                "SELECT id, qty, qty_raw, qty_decimals, cost_sol, cost_lamports, opened_ts, accounting_bucket, risk_context
                 FROM shadow_lots
                 WHERE wallet_id = ?1 AND token = ?2
                 ORDER BY id ASC",
            )?;
            let mut rows = stmt.query(params![wallet_id, token])?;
            let mut lots: Vec<(
                i64,
                f64,
                Option<String>,
                Option<i64>,
                f64,
                Option<i64>,
                String,
                String,
                String,
            )> = Vec::new();
            while let Some(row) = rows.next()? {
                lots.push((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                ));
            }
            drop(rows);
            drop(stmt);

            for (
                lot_id,
                lot_qty,
                lot_qty_raw,
                lot_qty_decimals,
                lot_cost_sol,
                lot_cost_lamports_raw,
                lot_opened_ts,
                lot_accounting_bucket,
                lot_risk_context,
            ) in lots
            {
                if qty_remaining <= EPS {
                    break;
                }

                if lot_qty <= EPS {
                    self.conn
                        .execute("DELETE FROM shadow_lots WHERE id = ?1", params![lot_id])?;
                    continue;
                }

                let take_qty = qty_remaining.min(lot_qty);
                let final_segment = (qty_remaining - take_qty) <= EPS;
                let current_qty_exact = token_quantity_from_sql(
                    lot_qty_raw,
                    lot_qty_decimals,
                    "shadow fifo close lot qty",
                )
                .map_err(to_sql_conversion_error)?;
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
                        if raw == 0 {
                            None
                        } else {
                            Some(TokenQuantity::new(raw, current.decimals()))
                        }
                    }
                    _ => None,
                };
                let entry_cost_sol = lot_cost_sol * (take_qty / lot_qty);
                let remaining_qty = (lot_qty - take_qty).max(0.0);
                let remaining_cost = (lot_cost_sol - entry_cost_sol).max(0.0);
                let lot_cost_lamports = shadow_lot_cost_lamports(
                    lot_cost_sol,
                    lot_cost_lamports_raw,
                    "shadow fifo close lot",
                )
                .map_err(to_sql_conversion_error)?;
                let entry_cost_lamports = if remaining_qty <= EPS {
                    lot_cost_lamports
                } else {
                    let estimated = sol_to_lamports_ceil_storage(
                        entry_cost_sol,
                        "shadow closed trade entry_cost_sol",
                    )
                    .map_err(to_sql_conversion_error)?;
                    if estimated > lot_cost_lamports {
                        lot_cost_lamports
                    } else {
                        estimated
                    }
                };
                let remaining_cost_lamports = lot_cost_lamports
                    .checked_sub(entry_cost_lamports)
                    .ok_or_else(|| {
                        to_sql_conversion_error(anyhow!(
                            "shadow lot lamport cost underflow after close"
                        ))
                    })?;
                let remaining_qty_exact = merge_position_qty_exact_on_sell(
                    current_qty_exact,
                    closed_qty_exact,
                    remaining_qty <= EPS,
                )
                .map_err(to_sql_conversion_error)?;

                if remaining_qty <= EPS {
                    self.conn
                        .execute("DELETE FROM shadow_lots WHERE id = ?1", params![lot_id])?;
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
                            remaining_qty_exact
                                .as_ref()
                                .map(|value| value.raw().to_string()),
                            remaining_qty_exact
                                .as_ref()
                                .map(|value| i64::from(value.decimals())),
                            remaining_cost,
                            u64_to_sql_i64(
                                "shadow_lots.cost_lamports",
                                remaining_cost_lamports.as_u64()
                            )
                            .map_err(to_sql_conversion_error)?,
                            lot_id
                        ],
                    )?;
                }

                let exit_value_sol = take_qty * exit_price_sol;
                let exit_value_lamports = sol_to_lamports_floor_storage(
                    exit_value_sol,
                    "shadow closed trade exit_value_sol",
                )
                .map_err(to_sql_conversion_error)?;
                let effective_close_context =
                    if close_context == SHADOW_CLOSE_CONTEXT_MARKET
                        && lot_risk_context == SHADOW_RISK_CONTEXT_QUARANTINED_LEGACY
                    {
                        SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY
                    } else {
                        close_context
                    };
                let pnl_lamports = crate::SignedLamports::new(
                    i128::from(exit_value_lamports.as_u64())
                        - i128::from(entry_cost_lamports.as_u64()),
                );
                let pnl_sol = exit_value_sol - entry_cost_sol;
                self.conn.execute(
                    "INSERT INTO shadow_closed_trades(
                        signal_id,
                        wallet_id,
                        token,
                        close_context,
                        accounting_bucket,
                        qty,
                        qty_raw,
                        qty_decimals,
                        entry_cost_sol,
                        entry_cost_lamports,
                        exit_value_sol,
                        exit_value_lamports,
                        pnl_sol,
                        pnl_lamports,
                        opened_ts,
                        closed_ts
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
                    params![
                        signal_id,
                        wallet_id,
                        token,
                        effective_close_context,
                        lot_accounting_bucket,
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
                        pnl_sol,
                        signed_lamports_to_sql_i64(
                            "shadow_closed_trades.pnl_lamports",
                            pnl_lamports
                        )
                        .map_err(to_sql_conversion_error)?,
                        lot_opened_ts,
                        closed_ts.to_rfc3339(),
                    ],
                )?;

                qty_remaining -= take_qty;
                qty_remaining_exact = next_remaining_qty_exact;
                closed_qty += take_qty;
                realized_pnl_sol += signed_lamports_to_sol(pnl_lamports);
            }

            let remaining_lots: i64 = self.conn.query_row(
                "SELECT COUNT(*)
                 FROM shadow_lots
                 WHERE wallet_id = ?1 AND token = ?2 AND qty > ?3",
                params![wallet_id, token, SHADOW_LOT_OPEN_EPS],
                |row| row.get(0),
            )?;

            Ok(ShadowCloseOutcome {
                closed_qty,
                realized_pnl_sol,
                has_open_lots_after: remaining_lots > 0,
            })
        })();

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

    pub fn update_shadow_lot(&self, id: i64, qty: f64, cost_sol: f64) -> Result<()> {
        self.update_shadow_lot_exact(id, qty, None, cost_sol)
    }

    pub fn update_shadow_lot_exact(
        &self,
        id: i64,
        qty: f64,
        qty_exact: Option<TokenQuantity>,
        cost_sol: f64,
    ) -> Result<()> {
        let qty_exact = reject_zero_raw_exact_qty(qty_exact, "update shadow lot")?;
        self.execute_with_retry(|conn| {
            let cost_lamports = sol_to_lamports_ceil_storage(cost_sol, "shadow lot cost_sol")
                .map_err(to_sql_conversion_error)?;
            conn.execute(
                "UPDATE shadow_lots SET qty = ?1, qty_raw = ?2, qty_decimals = ?3, cost_sol = ?4, cost_lamports = ?5 WHERE id = ?6",
                params![
                    qty,
                    qty_exact.as_ref().map(|value| value.raw().to_string()),
                    qty_exact.as_ref().map(|value| i64::from(value.decimals())),
                    cost_sol,
                    u64_to_sql_i64("shadow_lots.cost_lamports", cost_lamports.as_u64())
                        .map_err(to_sql_conversion_error)?,
                    id
                ],
            )
        })
        .context("failed to update shadow lot")?;
        Ok(())
    }

    pub fn delete_shadow_lot(&self, id: i64) -> Result<()> {
        self.execute_with_retry(|conn| {
            conn.execute("DELETE FROM shadow_lots WHERE id = ?1", params![id])
        })
        .context("failed to delete shadow lot")?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn insert_shadow_closed_trade(
        &self,
        signal_id: &str,
        wallet_id: &str,
        token: &str,
        qty: f64,
        entry_cost_sol: f64,
        exit_value_sol: f64,
        pnl_sol: f64,
        opened_ts: DateTime<Utc>,
        closed_ts: DateTime<Utc>,
    ) -> Result<()> {
        self.insert_shadow_closed_trade_exact(
            signal_id,
            wallet_id,
            token,
            qty,
            None,
            entry_cost_sol,
            exit_value_sol,
            pnl_sol,
            opened_ts,
            closed_ts,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn insert_shadow_closed_trade_exact(
        &self,
        signal_id: &str,
        wallet_id: &str,
        token: &str,
        qty: f64,
        qty_exact: Option<TokenQuantity>,
        entry_cost_sol: f64,
        exit_value_sol: f64,
        pnl_sol: f64,
        opened_ts: DateTime<Utc>,
        closed_ts: DateTime<Utc>,
    ) -> Result<()> {
        self.insert_shadow_closed_trade_exact_with_context(
            signal_id,
            wallet_id,
            token,
            qty,
            qty_exact,
            entry_cost_sol,
            exit_value_sol,
            pnl_sol,
            SHADOW_CLOSE_CONTEXT_MARKET,
            opened_ts,
            closed_ts,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn insert_shadow_closed_trade_exact_with_context(
        &self,
        signal_id: &str,
        wallet_id: &str,
        token: &str,
        qty: f64,
        qty_exact: Option<TokenQuantity>,
        entry_cost_sol: f64,
        exit_value_sol: f64,
        pnl_sol: f64,
        close_context: &str,
        opened_ts: DateTime<Utc>,
        closed_ts: DateTime<Utc>,
    ) -> Result<()> {
        let qty_exact = reject_zero_raw_exact_qty(qty_exact, "insert shadow closed trade")?;
        self.execute_with_retry(|conn| {
            let entry_cost_lamports =
                sol_to_lamports_ceil_storage(entry_cost_sol, "shadow closed trade entry_cost_sol")
                    .map_err(to_sql_conversion_error)?;
            let exit_value_lamports =
                sol_to_lamports_floor_storage(exit_value_sol, "shadow closed trade exit_value_sol")
                    .map_err(to_sql_conversion_error)?;
            let pnl_lamports = crate::SignedLamports::new(
                i128::from(exit_value_lamports.as_u64()) - i128::from(entry_cost_lamports.as_u64()),
            );
            let accounting_bucket = shadow_accounting_bucket_for_qty_exact(qty_exact);
            conn.execute(
                "INSERT INTO shadow_closed_trades(
                    signal_id,
                    wallet_id,
                    token,
                    close_context,
                    accounting_bucket,
                    qty,
                    qty_raw,
                    qty_decimals,
                    entry_cost_sol,
                    entry_cost_lamports,
                    exit_value_sol,
                    exit_value_lamports,
                    pnl_sol,
                    pnl_lamports,
                    opened_ts,
                    closed_ts
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
                params![
                    signal_id,
                    wallet_id,
                    token,
                    close_context,
                    accounting_bucket,
                    qty,
                    qty_exact.as_ref().map(|value| value.raw().to_string()),
                    qty_exact.as_ref().map(|value| i64::from(value.decimals())),
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
                    pnl_sol,
                    signed_lamports_to_sql_i64("shadow_closed_trades.pnl_lamports", pnl_lamports)
                        .map_err(to_sql_conversion_error)?,
                    opened_ts.to_rfc3339(),
                    closed_ts.to_rfc3339(),
                ],
            )
        })
        .context("failed to insert shadow closed trade")?;
        Ok(())
    }

    pub fn shadow_closed_trade_qty_exact(&self, signal_id: &str) -> Result<Option<TokenQuantity>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT qty_raw, qty_decimals
                 FROM shadow_closed_trades
                 WHERE signal_id = ?1",
            )
            .context("failed to prepare shadow closed trade exact qty query")?;
        let mut rows = stmt
            .query(params![signal_id])
            .context("failed to query shadow closed trade exact qty")?;
        let Some(row) = rows
            .next()
            .context("failed to read shadow closed trade exact qty row")?
        else {
            return Ok(None);
        };

        token_quantity_from_sql(
            row.get(0)
                .context("failed reading shadow_closed_trades.qty_raw")?,
            row.get(1)
                .context("failed reading shadow_closed_trades.qty_decimals")?,
            "shadow_closed_trades",
        )
    }

    pub fn shadow_closed_trade_accounting_bucket(&self, signal_id: &str) -> Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT accounting_bucket
                 FROM shadow_closed_trades
                 WHERE signal_id = ?1
                 LIMIT 1",
                params![signal_id],
                |row| row.get(0),
            )
            .optional()
            .context("failed reading shadow closed trade accounting bucket")
    }

    pub fn shadow_closed_trade_close_context(&self, signal_id: &str) -> Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT close_context
                 FROM shadow_closed_trades
                 WHERE signal_id = ?1
                 LIMIT 1",
                params![signal_id],
                |row| row.get(0),
            )
            .optional()
            .context("failed reading shadow closed trade close context")
    }
}
