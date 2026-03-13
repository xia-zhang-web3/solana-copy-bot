use super::{
    LIVE_POSITION_OPEN_EPS, SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE,
    SHADOW_RISK_CONTEXT_MARKET, SqliteStore, lamports_to_sol, position_pnl_lamports,
    shadow::SHADOW_LOT_OPEN_EPS, shadow_closed_trade_entry_cost_lamports,
    shadow_closed_trade_pnl_lamports, shadow_lot_cost_lamports, signed_lamports_to_sol,
    sol_to_signed_lamports_conservative_storage, token_quantity_from_sql,
};
use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use copybot_core_types::{Lamports, SignedLamports};
use rusqlite::params;

impl SqliteStore {
    fn shadow_realized_pnl_lamports_since_internal(
        &self,
        since: DateTime<Utc>,
        exclude_stale_terminal_zero: bool,
    ) -> Result<(u64, copybot_core_types::SignedLamports)> {
        let query = if exclude_stale_terminal_zero {
            "SELECT pnl_sol, pnl_lamports
             FROM shadow_closed_trades
             WHERE closed_ts >= ?1
               AND COALESCE(close_context, 'market') != ?2"
        } else {
            "SELECT pnl_sol, pnl_lamports
             FROM shadow_closed_trades
             WHERE closed_ts >= ?1"
        };
        let mut stmt = self
            .conn
            .prepare(query)
            .context("failed to prepare shadow pnl query")?;
        let mut rows = if exclude_stale_terminal_zero {
            stmt.query(params![
                since.to_rfc3339(),
                SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE
            ])
            .context("failed querying shadow pnl rows")?
        } else {
            stmt.query(params![since.to_rfc3339()])
                .context("failed querying shadow pnl rows")?
        };
        let mut trades = 0_u64;
        let mut pnl = copybot_core_types::SignedLamports::new(0);
        while let Some(row) = rows.next().context("failed iterating shadow pnl rows")? {
            let pnl_sol: f64 = row
                .get(0)
                .context("failed reading shadow_closed_trades.pnl_sol")?;
            let pnl_lamports_raw: Option<i64> = row
                .get(1)
                .context("failed reading shadow_closed_trades.pnl_lamports")?;
            let pnl_lamports =
                shadow_closed_trade_pnl_lamports(pnl_sol, pnl_lamports_raw, "shadow realized pnl")?;
            trades = trades.saturating_add(1);
            pnl = pnl.checked_add(pnl_lamports).ok_or_else(|| {
                anyhow!("shadow realized pnl lamports overflow while summing closed trades")
            })?;
        }
        Ok((trades, pnl))
    }

    pub fn shadow_open_lots_count(&self) -> Result<u64> {
        let value: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM shadow_lots WHERE qty > ?1",
                params![SHADOW_LOT_OPEN_EPS],
                |row| row.get(0),
            )
            .context("failed querying shadow open lots count")?;
        Ok(value.max(0) as u64)
    }

    fn shadow_open_notional_lamports_internal(
        &self,
        risk_only: bool,
    ) -> Result<copybot_core_types::Lamports> {
        let query = if risk_only {
            "SELECT cost_sol, cost_lamports
             FROM shadow_lots
             WHERE qty > ?1
               AND risk_context = ?2"
        } else {
            "SELECT cost_sol, cost_lamports
             FROM shadow_lots
             WHERE qty > ?1"
        };
        let mut stmt = self
            .conn
            .prepare(query)
            .context("failed to prepare shadow open notional query")?;
        let mut rows = if risk_only {
            stmt.query(params![SHADOW_LOT_OPEN_EPS, SHADOW_RISK_CONTEXT_MARKET])
                .context("failed querying shadow risk open notional rows")?
        } else {
            stmt.query(params![SHADOW_LOT_OPEN_EPS])
                .context("failed querying shadow open notional rows")?
        };

        let mut total = copybot_core_types::Lamports::ZERO;
        while let Some(row) = rows
            .next()
            .context("failed iterating shadow open notional rows")?
        {
            let cost_sol: f64 = row.get(0).context("failed reading shadow_lots.cost_sol")?;
            let cost_lamports_raw: Option<i64> = row
                .get(1)
                .context("failed reading shadow_lots.cost_lamports")?;
            let cost_lamports =
                shadow_lot_cost_lamports(cost_sol, cost_lamports_raw, "shadow open notional")?;
            total = total.checked_add(cost_lamports).ok_or_else(|| {
                anyhow!("shadow open notional lamports overflow while summing lots")
            })?;
        }

        Ok(total)
    }

    pub fn shadow_open_notional_lamports(&self) -> Result<copybot_core_types::Lamports> {
        self.shadow_open_notional_lamports_internal(false)
    }

    pub fn shadow_open_notional_sol(&self) -> Result<f64> {
        Ok(lamports_to_sol(self.shadow_open_notional_lamports()?))
    }

    pub fn shadow_risk_open_notional_lamports(&self) -> Result<copybot_core_types::Lamports> {
        self.shadow_open_notional_lamports_internal(true)
    }

    pub fn shadow_risk_open_notional_sol(&self) -> Result<f64> {
        Ok(lamports_to_sol(self.shadow_risk_open_notional_lamports()?))
    }

    pub fn shadow_realized_pnl_lamports_since(
        &self,
        since: DateTime<Utc>,
    ) -> Result<(u64, copybot_core_types::SignedLamports)> {
        self.shadow_realized_pnl_lamports_since_internal(since, false)
    }

    pub fn shadow_realized_pnl_since(&self, since: DateTime<Utc>) -> Result<(u64, f64)> {
        let (trades, pnl_lamports) = self.shadow_realized_pnl_lamports_since(since)?;
        Ok((trades, signed_lamports_to_sol(pnl_lamports)))
    }

    pub fn shadow_risk_realized_pnl_lamports_since(
        &self,
        since: DateTime<Utc>,
    ) -> Result<(u64, copybot_core_types::SignedLamports)> {
        self.shadow_realized_pnl_lamports_since_internal(since, true)
    }

    pub fn live_realized_pnl_since(&self, since: DateTime<Utc>) -> Result<(u64, f64)> {
        let (trades, pnl) = self.live_realized_pnl_lamports_since(since)?;
        Ok((trades, signed_lamports_to_sol(pnl)))
    }

    pub fn live_realized_pnl_lamports_since(
        &self,
        since: DateTime<Utc>,
    ) -> Result<(u64, SignedLamports)> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT pnl_sol, pnl_lamports
                 FROM positions
                 WHERE state = 'closed'
                   AND closed_ts IS NOT NULL
                   AND closed_ts >= ?1
                 ORDER BY
                    julianday(closed_ts) ASC,
                    closed_ts ASC,
                    rowid ASC",
            )
            .context("failed to prepare live pnl query")?;
        let mut rows = stmt
            .query(params![since.to_rfc3339()])
            .context("failed querying live pnl rows")?;
        let mut trades = 0_u64;
        let mut pnl = SignedLamports::new(0);
        while let Some(row) = rows.next().context("failed iterating live pnl rows")? {
            let pnl_sol: Option<f64> = row.get(0).context("failed reading positions.pnl_sol")?;
            let pnl_lamports_raw: Option<i64> = row
                .get(1)
                .context("failed reading positions.pnl_lamports")?;
            let position_pnl = position_pnl_lamports(
                pnl_sol.unwrap_or(0.0),
                pnl_lamports_raw,
                "live realized pnl",
            )?;
            trades = trades.saturating_add(1);
            pnl = pnl.checked_add(position_pnl).ok_or_else(|| {
                anyhow!("live realized pnl lamports overflow while summing positions")
            })?;
        }
        Ok((trades, pnl))
    }

    pub fn live_max_drawdown_since(&self, since: DateTime<Utc>) -> Result<f64> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT pnl_sol, pnl_lamports
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
        let mut rows = stmt
            .query(params![since.to_rfc3339()])
            .context("failed querying live drawdown rows")?;

        let mut cumulative_pnl = copybot_core_types::SignedLamports::new(0);
        let mut peak_pnl = copybot_core_types::SignedLamports::new(0);
        let mut max_drawdown = copybot_core_types::Lamports::ZERO;
        while let Some(row) = rows.next().context("failed iterating live drawdown rows")? {
            let pnl_sol: Option<f64> = row.get(0).context("failed reading positions.pnl_sol")?;
            let pnl_lamports_raw: Option<i64> = row
                .get(1)
                .context("failed reading positions.pnl_lamports")?;
            let pnl =
                position_pnl_lamports(pnl_sol.unwrap_or(0.0), pnl_lamports_raw, "live drawdown")?;
            cumulative_pnl = cumulative_pnl
                .checked_add(pnl)
                .ok_or_else(|| anyhow!("live drawdown cumulative pnl overflow"))?;
            if cumulative_pnl > peak_pnl {
                peak_pnl = cumulative_pnl;
            }
            if cumulative_pnl < peak_pnl {
                let drawdown = peak_pnl
                    .checked_sub(cumulative_pnl)
                    .and_then(|value| value.checked_abs_lamports())
                    .ok_or_else(|| anyhow!("live drawdown lamports overflow"))?;
                if drawdown > max_drawdown {
                    max_drawdown = drawdown;
                }
            }
        }

        Ok(lamports_to_sol(max_drawdown))
    }

    pub fn live_unrealized_pnl_sol(&self, as_of: DateTime<Utc>) -> Result<(f64, u64)> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT token, qty, qty_raw, qty_decimals, cost_sol, cost_lamports
                 FROM positions
                 WHERE state = 'open'
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
            let qty_raw: Option<String> = row.get(2).context("failed reading positions.qty_raw")?;
            let qty_decimals: Option<i64> = row
                .get(3)
                .context("failed reading positions.qty_decimals")?;
            let cost_sol: f64 = row.get(4).context("failed reading positions.cost_sol")?;
            let cost_lamports_raw: Option<i64> = row
                .get(5)
                .context("failed reading positions.cost_lamports")?;
            let qty = super::position_qty_sol(qty, qty_raw, qty_decimals, "live unrealized pnl")?;
            if !qty.is_finite() || !cost_sol.is_finite() || cost_sol < 0.0 {
                return Err(anyhow!(
                    "invalid open position row for unrealized pnl token={} qty={} cost_sol={}",
                    token,
                    qty,
                    cost_sol
                ));
            }
            if qty <= LIVE_POSITION_OPEN_EPS {
                continue;
            }
            let cost_sol = super::lamports_to_sol(super::position_cost_lamports(
                cost_sol,
                cost_lamports_raw,
                "live unrealized pnl",
            )?);

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

    pub fn live_open_positions_are_exact_ready(&self) -> Result<bool> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT qty, qty_raw, qty_decimals, cost_lamports
                 FROM positions
                 WHERE state = 'open'
                   AND cost_sol >= 0",
            )
            .context("failed to prepare live exact readiness query")?;
        let mut rows = stmt
            .query([])
            .context("failed querying live exact readiness rows")?;

        while let Some(row) = rows
            .next()
            .context("failed iterating live exact readiness rows")?
        {
            let qty: f64 = row.get(0).context("failed reading positions.qty")?;
            let qty_raw: Option<String> = row.get(1).context("failed reading positions.qty_raw")?;
            let qty_decimals: Option<i64> = row
                .get(2)
                .context("failed reading positions.qty_decimals")?;
            let cost_lamports_raw: Option<i64> = row
                .get(3)
                .context("failed reading positions.cost_lamports")?;
            let qty = super::position_qty_sol(
                qty,
                qty_raw.clone(),
                qty_decimals,
                "live exact readiness",
            )?;
            if qty <= LIVE_POSITION_OPEN_EPS {
                continue;
            }
            if qty_raw.is_none() || qty_decimals.is_none() || cost_lamports_raw.is_none() {
                return Ok(false);
            }
            if let Some(cost_lamports_raw) = cost_lamports_raw {
                if cost_lamports_raw < 0 {
                    return Err(anyhow!(
                        "invalid negative positions.cost_lamports={} in live exact readiness",
                        cost_lamports_raw
                    ));
                }
            }
        }

        Ok(true)
    }

    pub fn live_unrealized_pnl_lamports(
        &self,
        as_of: DateTime<Utc>,
    ) -> Result<(SignedLamports, u64)> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT token, qty_raw, qty_decimals, cost_lamports
                 FROM positions
                 WHERE state = 'open'
                   AND cost_sol >= 0",
            )
            .context("failed to prepare live exact open positions query")?;
        let mut rows = stmt
            .query([])
            .context("failed querying live exact open positions")?;

        let mut unrealized_pnl_lamports = SignedLamports::ZERO;
        let mut missing_price_count = 0_u64;
        while let Some(row) = rows
            .next()
            .context("failed iterating live exact open positions")?
        {
            let token: String = row.get(0).context("failed reading positions.token")?;
            let qty_raw: Option<String> = row.get(1).context("failed reading positions.qty_raw")?;
            let qty_decimals: Option<i64> = row
                .get(2)
                .context("failed reading positions.qty_decimals")?;
            let cost_lamports_raw: Option<i64> = row
                .get(3)
                .context("failed reading positions.cost_lamports")?;
            let Some(qty_exact) =
                token_quantity_from_sql(qty_raw, qty_decimals, "live unrealized pnl exact qty")?
            else {
                missing_price_count = missing_price_count.saturating_add(1);
                continue;
            };
            if qty_exact.raw() == 0 {
                continue;
            }
            let Some(cost_lamports_raw) = cost_lamports_raw else {
                missing_price_count = missing_price_count.saturating_add(1);
                continue;
            };
            if cost_lamports_raw < 0 {
                return Err(anyhow!(
                    "invalid negative positions.cost_lamports={} in live unrealized pnl exact path",
                    cost_lamports_raw
                ));
            }
            let cost_lamports = Lamports::new(cost_lamports_raw as u64);

            if let Some(exact_quote) =
                self.reliable_exact_token_sol_price_for_live_unrealized(&token, as_of)?
            {
                let mark_value_lamports =
                    exact_quote
                        .mark_value_lamports(qty_exact)
                        .with_context(|| {
                            format!("failed exact mark valuation for live unrealized token={token}")
                        })?;
                let pnl = SignedLamports::from(mark_value_lamports)
                    .checked_sub(SignedLamports::from(cost_lamports))
                    .ok_or_else(|| anyhow!("live unrealized pnl lamports overflow"))?;
                unrealized_pnl_lamports =
                    unrealized_pnl_lamports.checked_add(pnl).ok_or_else(|| {
                        anyhow!("live unrealized pnl lamports overflow while summing positions")
                    })?;
            } else {
                missing_price_count = missing_price_count.saturating_add(1);
            }
        }

        Ok((unrealized_pnl_lamports, missing_price_count))
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
                "SELECT pnl_sol, pnl_lamports
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
        let mut rows = stmt
            .query(params![since.to_rfc3339()])
            .context("failed querying live drawdown rows")?;

        let mut cumulative_pnl = copybot_core_types::SignedLamports::new(0);
        let mut peak_pnl = copybot_core_types::SignedLamports::new(0);
        let mut max_drawdown = copybot_core_types::Lamports::ZERO;
        while let Some(row) = rows.next().context("failed iterating live drawdown rows")? {
            let pnl_sol: Option<f64> = row.get(0).context("failed reading positions.pnl_sol")?;
            let pnl_lamports_raw: Option<i64> = row
                .get(1)
                .context("failed reading positions.pnl_lamports")?;
            let pnl = position_pnl_lamports(
                pnl_sol.unwrap_or(0.0),
                pnl_lamports_raw,
                "live drawdown with unrealized",
            )?;
            cumulative_pnl = cumulative_pnl
                .checked_add(pnl)
                .ok_or_else(|| anyhow!("live drawdown cumulative pnl overflow"))?;
            if cumulative_pnl > peak_pnl {
                peak_pnl = cumulative_pnl;
            }
            if cumulative_pnl < peak_pnl {
                let drawdown = peak_pnl
                    .checked_sub(cumulative_pnl)
                    .and_then(|value| value.checked_abs_lamports())
                    .ok_or_else(|| anyhow!("live drawdown lamports overflow"))?;
                if drawdown > max_drawdown {
                    max_drawdown = drawdown;
                }
            }
        }

        let unrealized_pnl =
            sol_to_signed_lamports_conservative_storage(unrealized_pnl_sol, "unrealized_pnl_sol")?;
        let terminal_pnl = cumulative_pnl
            .checked_add(unrealized_pnl)
            .ok_or_else(|| anyhow!("terminal live drawdown pnl overflow"))?;
        if terminal_pnl > peak_pnl {
            peak_pnl = terminal_pnl;
        }
        if terminal_pnl < peak_pnl {
            let terminal_drawdown = peak_pnl
                .checked_sub(terminal_pnl)
                .and_then(|value| value.checked_abs_lamports())
                .ok_or_else(|| anyhow!("terminal live drawdown lamports overflow"))?;
            if terminal_drawdown > max_drawdown {
                max_drawdown = terminal_drawdown;
            }
        }

        Ok(lamports_to_sol(max_drawdown))
    }

    pub fn live_max_drawdown_with_unrealized_lamports_since(
        &self,
        since: DateTime<Utc>,
        unrealized_pnl: SignedLamports,
    ) -> Result<Lamports> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT pnl_sol, pnl_lamports
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
        let mut rows = stmt
            .query(params![since.to_rfc3339()])
            .context("failed querying live drawdown rows")?;

        let mut cumulative_pnl = SignedLamports::new(0);
        let mut peak_pnl = SignedLamports::new(0);
        let mut max_drawdown = Lamports::ZERO;
        while let Some(row) = rows.next().context("failed iterating live drawdown rows")? {
            let pnl_sol: Option<f64> = row.get(0).context("failed reading positions.pnl_sol")?;
            let pnl_lamports_raw: Option<i64> = row
                .get(1)
                .context("failed reading positions.pnl_lamports")?;
            let pnl = position_pnl_lamports(
                pnl_sol.unwrap_or(0.0),
                pnl_lamports_raw,
                "live drawdown with unrealized",
            )?;
            cumulative_pnl = cumulative_pnl
                .checked_add(pnl)
                .ok_or_else(|| anyhow!("live drawdown cumulative pnl overflow"))?;
            if cumulative_pnl > peak_pnl {
                peak_pnl = cumulative_pnl;
            }
            if cumulative_pnl < peak_pnl {
                let drawdown = peak_pnl
                    .checked_sub(cumulative_pnl)
                    .and_then(|value| value.checked_abs_lamports())
                    .ok_or_else(|| anyhow!("live drawdown lamports overflow"))?;
                if drawdown > max_drawdown {
                    max_drawdown = drawdown;
                }
            }
        }

        let terminal_pnl = cumulative_pnl
            .checked_add(unrealized_pnl)
            .ok_or_else(|| anyhow!("terminal live drawdown pnl overflow"))?;
        if terminal_pnl > peak_pnl {
            peak_pnl = terminal_pnl;
        }
        if terminal_pnl < peak_pnl {
            let terminal_drawdown = peak_pnl
                .checked_sub(terminal_pnl)
                .and_then(|value| value.checked_abs_lamports())
                .ok_or_else(|| anyhow!("terminal live drawdown lamports overflow"))?;
            if terminal_drawdown > max_drawdown {
                max_drawdown = terminal_drawdown;
            }
        }

        Ok(max_drawdown)
    }

    pub fn shadow_rug_loss_count_since(
        &self,
        since: DateTime<Utc>,
        return_threshold: f64,
    ) -> Result<u64> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT entry_cost_sol, entry_cost_lamports, pnl_sol, pnl_lamports
                 FROM shadow_closed_trades
                 WHERE closed_ts >= ?1
                   AND COALESCE(close_context, 'market') != ?2",
            )
            .context("failed to prepare shadow rug-loss count query")?;
        let mut rows = stmt
            .query(params![
                since.to_rfc3339(),
                SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE
            ])
            .context("failed querying shadow rug-loss count rows")?;
        let mut count = 0_u64;
        while let Some(row) = rows
            .next()
            .context("failed iterating shadow rug-loss count rows")?
        {
            let entry_cost_sol: f64 = row
                .get(0)
                .context("failed reading shadow_closed_trades.entry_cost_sol")?;
            let entry_cost_lamports_raw: Option<i64> = row
                .get(1)
                .context("failed reading shadow_closed_trades.entry_cost_lamports")?;
            let pnl_sol: f64 = row
                .get(2)
                .context("failed reading shadow_closed_trades.pnl_sol")?;
            let pnl_lamports_raw: Option<i64> = row
                .get(3)
                .context("failed reading shadow_closed_trades.pnl_lamports")?;
            let entry_cost = lamports_to_sol(shadow_closed_trade_entry_cost_lamports(
                entry_cost_sol,
                entry_cost_lamports_raw,
                "shadow rug-loss count",
            )?);
            if entry_cost <= 0.0 {
                continue;
            }
            let pnl = signed_lamports_to_sol(shadow_closed_trade_pnl_lamports(
                pnl_sol,
                pnl_lamports_raw,
                "shadow rug-loss count",
            )?);
            if pnl <= entry_cost * return_threshold {
                count = count.saturating_add(1);
            }
        }
        Ok(count)
    }

    pub fn shadow_rug_loss_rate_recent(
        &self,
        since: DateTime<Utc>,
        sample_size: u64,
        return_threshold: f64,
    ) -> Result<(u64, u64, f64)> {
        let limit = sample_size.max(1).min(i64::MAX as u64) as i64;
        let mut stmt = self
            .conn
            .prepare(
                "SELECT entry_cost_sol, entry_cost_lamports, pnl_sol, pnl_lamports
                 FROM shadow_closed_trades
                 WHERE closed_ts >= ?1
                   AND COALESCE(close_context, 'market') != ?2
                 ORDER BY closed_ts DESC, id DESC
                 LIMIT ?3",
            )
            .context("failed to prepare shadow rug-loss recent sample query")?;
        let mut rows = stmt
            .query(params![
                since.to_rfc3339(),
                SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE,
                limit
            ])
            .context("failed querying shadow rug-loss recent sample rows")?;
        let mut rug_count = 0_u64;
        let mut total_count = 0_u64;
        while let Some(row) = rows
            .next()
            .context("failed iterating shadow rug-loss recent sample rows")?
        {
            total_count = total_count.saturating_add(1);
            let entry_cost_sol: f64 = row
                .get(0)
                .context("failed reading shadow_closed_trades.entry_cost_sol")?;
            let entry_cost_lamports_raw: Option<i64> = row
                .get(1)
                .context("failed reading shadow_closed_trades.entry_cost_lamports")?;
            let pnl_sol: f64 = row
                .get(2)
                .context("failed reading shadow_closed_trades.pnl_sol")?;
            let pnl_lamports_raw: Option<i64> = row
                .get(3)
                .context("failed reading shadow_closed_trades.pnl_lamports")?;
            let entry_cost = lamports_to_sol(shadow_closed_trade_entry_cost_lamports(
                entry_cost_sol,
                entry_cost_lamports_raw,
                "shadow rug-loss recent sample",
            )?);
            if entry_cost <= 0.0 {
                continue;
            }
            let pnl = signed_lamports_to_sol(shadow_closed_trade_pnl_lamports(
                pnl_sol,
                pnl_lamports_raw,
                "shadow rug-loss recent sample",
            )?);
            if pnl <= entry_cost * return_threshold {
                rug_count = rug_count.saturating_add(1);
            }
        }
        let rug_rate = if total_count > 0 {
            rug_count as f64 / total_count as f64
        } else {
            0.0
        };
        Ok((rug_count, total_count, rug_rate))
    }
}
