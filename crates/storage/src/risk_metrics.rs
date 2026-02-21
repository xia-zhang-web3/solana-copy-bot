use super::SqliteStore;
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::params;

impl SqliteStore {
    pub fn shadow_open_lots_count(&self) -> Result<u64> {
        let value: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM shadow_lots", [], |row| row.get(0))
            .context("failed querying shadow open lots count")?;
        Ok(value.max(0) as u64)
    }

    pub fn shadow_open_notional_sol(&self) -> Result<f64> {
        let notional: f64 = self
            .conn
            .query_row(
                "SELECT COALESCE(SUM(cost_sol), 0.0) FROM shadow_lots",
                [],
                |row| row.get(0),
            )
            .context("failed querying shadow open notional")?;
        Ok(notional.max(0.0))
    }

    pub fn shadow_realized_pnl_since(&self, since: DateTime<Utc>) -> Result<(u64, f64)> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT COUNT(*) as trades, COALESCE(SUM(pnl_sol), 0.0) as pnl
                 FROM shadow_closed_trades
                 WHERE closed_ts >= ?1",
            )
            .context("failed to prepare shadow pnl query")?;
        let (trades, pnl): (i64, f64) = stmt
            .query_row(params![since.to_rfc3339()], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .context("failed querying shadow pnl summary")?;
        Ok((trades.max(0) as u64, pnl))
    }

    pub fn live_realized_pnl_since(&self, since: DateTime<Utc>) -> Result<(u64, f64)> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT COUNT(*) as trades, COALESCE(SUM(COALESCE(pnl_sol, 0.0)), 0.0) as pnl
                 FROM positions
                 WHERE state = 'closed'
                   AND closed_ts IS NOT NULL
                   AND closed_ts >= ?1",
            )
            .context("failed to prepare live pnl query")?;
        let (trades, pnl): (i64, f64) = stmt
            .query_row(params![since.to_rfc3339()], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .context("failed querying live pnl summary")?;
        Ok((trades.max(0) as u64, pnl))
    }

    pub fn live_max_drawdown_since(&self, since: DateTime<Utc>) -> Result<f64> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT COALESCE(pnl_sol, 0.0)
                 FROM positions
                 WHERE state = 'closed'
                   AND closed_ts IS NOT NULL
                   AND closed_ts >= ?1
                 ORDER BY
                    julianday(closed_ts) ASC,
                    closed_ts ASC,
                    rowid ASC",
            )
            .context("failed to prepare live drawdown query")?;
        let pnl_rows = stmt
            .query_map(params![since.to_rfc3339()], |row| row.get::<_, f64>(0))
            .context("failed querying live drawdown rows")?;

        let mut cumulative_pnl = 0.0_f64;
        let mut peak_pnl = 0.0_f64;
        let mut max_drawdown_sol = 0.0_f64;
        for pnl_row in pnl_rows {
            let pnl = pnl_row.context("failed reading live drawdown pnl row")?;
            if !pnl.is_finite() {
                return Err(anyhow!(
                    "non-finite closed position pnl in live drawdown series"
                ));
            }
            cumulative_pnl += pnl;
            if cumulative_pnl > peak_pnl {
                peak_pnl = cumulative_pnl;
            }
            let drawdown = (peak_pnl - cumulative_pnl).max(0.0);
            if drawdown > max_drawdown_sol {
                max_drawdown_sol = drawdown;
            }
        }

        Ok(max_drawdown_sol)
    }

    pub fn live_unrealized_pnl_sol(&self, as_of: DateTime<Utc>) -> Result<(f64, u64)> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT token, qty, cost_sol
                 FROM positions
                 WHERE state = 'open'
                   AND qty > 0
                   AND cost_sol >= 0",
            )
            .context("failed to prepare live open positions query")?;
        let mut rows = stmt
            .query([])
            .context("failed querying live open positions")?;

        let mut unrealized_pnl_sol = 0.0_f64;
        let mut missing_price_count = 0_u64;
        while let Some(row) = rows
            .next()
            .context("failed iterating live open positions")?
        {
            let token: String = row.get(0).context("failed reading positions.token")?;
            let qty: f64 = row.get(1).context("failed reading positions.qty")?;
            let cost_sol: f64 = row.get(2).context("failed reading positions.cost_sol")?;
            if !qty.is_finite() || !cost_sol.is_finite() || qty <= 0.0 || cost_sol < 0.0 {
                return Err(anyhow!(
                    "invalid open position row for unrealized pnl token={} qty={} cost_sol={}",
                    token,
                    qty,
                    cost_sol
                ));
            }

            if let Some(price_sol) =
                self.reliable_token_sol_price_for_live_unrealized(&token, as_of)?
            {
                let mark_value_sol = qty * price_sol;
                if !mark_value_sol.is_finite() {
                    return Err(anyhow!(
                        "non-finite mark value for unrealized pnl token={} qty={} price_sol={}",
                        token,
                        qty,
                        price_sol
                    ));
                }
                unrealized_pnl_sol += mark_value_sol - cost_sol;
            } else {
                missing_price_count = missing_price_count.saturating_add(1);
            }
        }

        Ok((unrealized_pnl_sol, missing_price_count))
    }

    pub fn live_max_drawdown_with_unrealized_since(
        &self,
        since: DateTime<Utc>,
        unrealized_pnl_sol: f64,
    ) -> Result<f64> {
        if !unrealized_pnl_sol.is_finite() {
            return Err(anyhow!(
                "invalid unrealized_pnl_sol for drawdown calculation: {}",
                unrealized_pnl_sol
            ));
        }
        let mut stmt = self
            .conn
            .prepare(
                "SELECT COALESCE(pnl_sol, 0.0)
                 FROM positions
                 WHERE state = 'closed'
                   AND closed_ts IS NOT NULL
                   AND closed_ts >= ?1
                 ORDER BY
                    julianday(closed_ts) ASC,
                    closed_ts ASC,
                    rowid ASC",
            )
            .context("failed to prepare live drawdown query")?;
        let pnl_rows = stmt
            .query_map(params![since.to_rfc3339()], |row| row.get::<_, f64>(0))
            .context("failed querying live drawdown rows")?;

        let mut cumulative_pnl = 0.0_f64;
        let mut peak_pnl = 0.0_f64;
        let mut max_drawdown_sol = 0.0_f64;
        for pnl_row in pnl_rows {
            let pnl = pnl_row.context("failed reading live drawdown pnl row")?;
            if !pnl.is_finite() {
                return Err(anyhow!(
                    "non-finite closed position pnl in live drawdown series"
                ));
            }
            cumulative_pnl += pnl;
            if cumulative_pnl > peak_pnl {
                peak_pnl = cumulative_pnl;
            }
            let drawdown = (peak_pnl - cumulative_pnl).max(0.0);
            if drawdown > max_drawdown_sol {
                max_drawdown_sol = drawdown;
            }
        }

        let terminal_pnl = cumulative_pnl + unrealized_pnl_sol;
        let terminal_drawdown = (peak_pnl - terminal_pnl).max(0.0);
        if terminal_drawdown > max_drawdown_sol {
            max_drawdown_sol = terminal_drawdown;
        }

        Ok(max_drawdown_sol)
    }

    pub fn shadow_rug_loss_count_since(
        &self,
        since: DateTime<Utc>,
        return_threshold: f64,
    ) -> Result<u64> {
        let count: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*)
                 FROM shadow_closed_trades
                 WHERE closed_ts >= ?1
                   AND entry_cost_sol > 0
                   AND pnl_sol <= entry_cost_sol * ?2",
                params![since.to_rfc3339(), return_threshold],
                |row| row.get(0),
            )
            .context("failed querying shadow rug-loss count since window")?;
        Ok(count.max(0) as u64)
    }

    pub fn shadow_rug_loss_rate_recent(
        &self,
        since: DateTime<Utc>,
        sample_size: u64,
        return_threshold: f64,
    ) -> Result<(u64, u64, f64)> {
        let limit = sample_size.max(1).min(i64::MAX as u64) as i64;
        let (rug_count, total_count): (i64, i64) = self
            .conn
            .query_row(
                "SELECT
                    COALESCE(
                        SUM(
                            CASE
                                WHEN entry_cost_sol > 0
                                     AND pnl_sol <= entry_cost_sol * ?2
                                THEN 1
                                ELSE 0
                            END
                        ),
                        0
                    ) AS rug_count,
                    COUNT(*) AS total_count
                     FROM (
                        SELECT entry_cost_sol, pnl_sol
                        FROM shadow_closed_trades
                        WHERE closed_ts >= ?1
                        ORDER BY closed_ts DESC, id DESC
                        LIMIT ?3
                     )",
                params![since.to_rfc3339(), return_threshold, limit],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .context("failed querying shadow rug-loss recent sample")?;
        let rug_count = rug_count.max(0) as u64;
        let total_count = total_count.max(0) as u64;
        let rug_rate = if total_count > 0 {
            rug_count as f64 / total_count as f64
        } else {
            0.0
        };
        Ok((rug_count, total_count, rug_rate))
    }
}
