use crate::sqlite_retry::is_retryable_sqlite_error;
use crate::{
    note_sqlite_busy_error, note_sqlite_write_retry, ShadowCloseOutcome, ShadowLotRow, SqliteStore,
    SQLITE_WRITE_MAX_RETRIES, SQLITE_WRITE_RETRY_BACKOFF_MS,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::params;
use std::collections::HashSet;
use std::time::Duration as StdDuration;

impl SqliteStore {
    pub fn insert_shadow_lot(
        &self,
        wallet_id: &str,
        token: &str,
        qty: f64,
        cost_sol: f64,
        opened_ts: DateTime<Utc>,
    ) -> Result<i64> {
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO shadow_lots(wallet_id, token, qty, cost_sol, opened_ts)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![wallet_id, token, qty, cost_sol, opened_ts.to_rfc3339()],
            )
        })
        .context("failed to insert shadow lot")?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn list_shadow_lots(&self, wallet_id: &str, token: &str) -> Result<Vec<ShadowLotRow>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, wallet_id, token, qty, cost_sol, opened_ts
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
            let opened_raw: String = row.get(5).context("failed reading shadow_lots.opened_ts")?;
            let opened_ts = DateTime::parse_from_rfc3339(&opened_raw)
                .map(|dt| dt.with_timezone(&Utc))
                .with_context(|| {
                    format!("invalid shadow_lots.opened_ts rfc3339 value: {opened_raw}")
                })?;
            lots.push(ShadowLotRow {
                id: row.get(0).context("failed reading shadow_lots.id")?,
                wallet_id: row.get(1).context("failed reading shadow_lots.wallet_id")?,
                token: row.get(2).context("failed reading shadow_lots.token")?,
                qty: row.get(3).context("failed reading shadow_lots.qty")?,
                cost_sol: row.get(4).context("failed reading shadow_lots.cost_sol")?,
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
                "SELECT id, wallet_id, token, qty, cost_sol, opened_ts
                 FROM shadow_lots
                 WHERE qty > 0
                   AND opened_ts <= ?1
                 ORDER BY opened_ts ASC, id ASC
                 LIMIT ?2",
            )
            .context("failed to prepare stale shadow lot query")?;
        let mut rows = stmt
            .query(params![cutoff.to_rfc3339(), limit.max(1) as i64])
            .context("failed querying stale shadow lots")?;

        let mut lots = Vec::new();
        while let Some(row) = rows.next().context("failed iterating stale shadow lots")? {
            let opened_raw: String = row.get(5).context("failed reading shadow_lots.opened_ts")?;
            let opened_ts = DateTime::parse_from_rfc3339(&opened_raw)
                .map(|dt| dt.with_timezone(&Utc))
                .with_context(|| {
                    format!("invalid shadow_lots.opened_ts rfc3339 value: {opened_raw}")
                })?;
            lots.push(ShadowLotRow {
                id: row.get(0).context("failed reading shadow_lots.id")?,
                wallet_id: row.get(1).context("failed reading shadow_lots.wallet_id")?,
                token: row.get(2).context("failed reading shadow_lots.token")?,
                qty: row.get(3).context("failed reading shadow_lots.qty")?,
                cost_sol: row.get(4).context("failed reading shadow_lots.cost_sol")?,
                opened_ts,
            });
        }

        Ok(lots)
    }

    pub fn has_shadow_lots(&self, wallet_id: &str, token: &str) -> Result<bool> {
        let count: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM shadow_lots WHERE wallet_id = ?1 AND token = ?2",
                params![wallet_id, token],
                |row| row.get(0),
            )
            .context("failed querying shadow lots existence")?;
        Ok(count > 0)
    }

    pub fn list_shadow_open_pairs(&self) -> Result<HashSet<(String, String)>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT DISTINCT wallet_id, token
                 FROM shadow_lots
                 WHERE qty > 0",
            )
            .context("failed to prepare shadow open lots query")?;
        let mut rows = stmt.query([]).context("failed querying shadow open lots")?;

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
        const EPS: f64 = 1e-12;

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
                exit_price_sol,
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
        exit_price_sol: f64,
        closed_ts: DateTime<Utc>,
    ) -> rusqlite::Result<ShadowCloseOutcome> {
        const EPS: f64 = 1e-12;

        self.conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;
        let close_result = (|| -> rusqlite::Result<ShadowCloseOutcome> {
            let mut qty_remaining = target_qty;
            let mut closed_qty = 0.0;
            let mut realized_pnl_sol = 0.0;

            let mut stmt = self.conn.prepare(
                "SELECT id, qty, cost_sol, opened_ts
                 FROM shadow_lots
                 WHERE wallet_id = ?1 AND token = ?2
                 ORDER BY id ASC",
            )?;
            let mut rows = stmt.query(params![wallet_id, token])?;
            let mut lots: Vec<(i64, f64, f64, String)> = Vec::new();
            while let Some(row) = rows.next()? {
                lots.push((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?));
            }
            drop(rows);
            drop(stmt);

            for (lot_id, lot_qty, lot_cost_sol, lot_opened_ts) in lots {
                if qty_remaining <= EPS {
                    break;
                }

                if lot_qty <= EPS {
                    self.conn
                        .execute("DELETE FROM shadow_lots WHERE id = ?1", params![lot_id])?;
                    continue;
                }

                let take_qty = qty_remaining.min(lot_qty);
                let entry_cost_sol = lot_cost_sol * (take_qty / lot_qty);
                let remaining_qty = (lot_qty - take_qty).max(0.0);
                let remaining_cost = (lot_cost_sol - entry_cost_sol).max(0.0);

                if remaining_qty <= EPS {
                    self.conn
                        .execute("DELETE FROM shadow_lots WHERE id = ?1", params![lot_id])?;
                } else {
                    self.conn.execute(
                        "UPDATE shadow_lots SET qty = ?1, cost_sol = ?2 WHERE id = ?3",
                        params![remaining_qty, remaining_cost, lot_id],
                    )?;
                }

                let exit_value_sol = take_qty * exit_price_sol;
                let pnl_sol = exit_value_sol - entry_cost_sol;
                self.conn.execute(
                    "INSERT INTO shadow_closed_trades(
                        signal_id,
                        wallet_id,
                        token,
                        qty,
                        entry_cost_sol,
                        exit_value_sol,
                        pnl_sol,
                        opened_ts,
                        closed_ts
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                    params![
                        signal_id,
                        wallet_id,
                        token,
                        take_qty,
                        entry_cost_sol,
                        exit_value_sol,
                        pnl_sol,
                        lot_opened_ts,
                        closed_ts.to_rfc3339(),
                    ],
                )?;

                qty_remaining -= take_qty;
                closed_qty += take_qty;
                realized_pnl_sol += pnl_sol;
            }

            let remaining_lots: i64 = self.conn.query_row(
                "SELECT COUNT(*)
                 FROM shadow_lots
                 WHERE wallet_id = ?1 AND token = ?2 AND qty > 0",
                params![wallet_id, token],
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
        self.execute_with_retry(|conn| {
            conn.execute(
                "UPDATE shadow_lots SET qty = ?1, cost_sol = ?2 WHERE id = ?3",
                params![qty, cost_sol, id],
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
        self.execute_with_retry(|conn| {
            conn.execute(
                "INSERT INTO shadow_closed_trades(
                    signal_id,
                    wallet_id,
                    token,
                    qty,
                    entry_cost_sol,
                    exit_value_sol,
                    pnl_sol,
                    opened_ts,
                    closed_ts
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    signal_id,
                    wallet_id,
                    token,
                    qty,
                    entry_cost_sol,
                    exit_value_sol,
                    pnl_sol,
                    opened_ts.to_rfc3339(),
                    closed_ts.to_rfc3339(),
                ],
            )
        })
        .context("failed to insert shadow closed trade")?;
        Ok(())
    }
}
