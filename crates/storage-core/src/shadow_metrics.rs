use crate::{
    money::{
        lamports_to_sol, shadow_closed_trade_entry_cost_lamports, shadow_closed_trade_pnl_lamports,
        shadow_lot_cost_lamports, signed_lamports_to_sol,
    },
    SqliteDiscoveryStore, SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY,
    SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE,
    SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE, SHADOW_LOT_OPEN_EPS,
    SHADOW_RISK_CONTEXT_MARKET,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::{Lamports, SignedLamports};
use rusqlite::params;

impl SqliteDiscoveryStore {
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

    pub fn shadow_open_notional_lamports(&self) -> Result<Lamports> {
        self.shadow_open_notional_lamports_internal(false)
    }

    pub fn shadow_open_notional_sol(&self) -> Result<f64> {
        Ok(lamports_to_sol(self.shadow_open_notional_lamports()?))
    }

    pub fn shadow_risk_open_notional_lamports(&self) -> Result<Lamports> {
        self.shadow_open_notional_lamports_internal(true)
    }

    pub fn shadow_risk_open_notional_sol(&self) -> Result<f64> {
        Ok(lamports_to_sol(self.shadow_risk_open_notional_lamports()?))
    }

    fn shadow_open_notional_lamports_internal(&self, risk_only: bool) -> Result<Lamports> {
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
        let mut total = Lamports::ZERO;
        while let Some(row) = rows
            .next()
            .context("failed iterating shadow open notional rows")?
        {
            let cost = shadow_lot_cost_lamports(
                row.get(0).context("failed reading shadow_lots.cost_sol")?,
                row.get(1)
                    .context("failed reading shadow_lots.cost_lamports")?,
                "shadow open notional",
            )?;
            total = total.checked_add(cost).ok_or_else(|| {
                anyhow!("shadow open notional lamports overflow while summing lots")
            })?;
        }
        Ok(total)
    }

    pub fn shadow_realized_pnl_lamports_since(
        &self,
        since: DateTime<Utc>,
    ) -> Result<(u64, SignedLamports)> {
        self.shadow_realized_pnl_lamports_since_internal(since, false)
    }

    pub fn shadow_realized_pnl_since(&self, since: DateTime<Utc>) -> Result<(u64, f64)> {
        let (trades, pnl_lamports) = self.shadow_realized_pnl_lamports_since(since)?;
        Ok((trades, signed_lamports_to_sol(pnl_lamports)))
    }

    pub fn shadow_risk_realized_pnl_lamports_since(
        &self,
        since: DateTime<Utc>,
    ) -> Result<(u64, SignedLamports)> {
        self.shadow_realized_pnl_lamports_since_internal(since, true)
    }

    fn shadow_realized_pnl_lamports_since_internal(
        &self,
        since: DateTime<Utc>,
        exclude_non_risk_context: bool,
    ) -> Result<(u64, SignedLamports)> {
        let query = if exclude_non_risk_context {
            "SELECT pnl_sol, pnl_lamports
             FROM shadow_closed_trades
             WHERE closed_ts >= ?1
               AND COALESCE(close_context, 'market') NOT IN (?2, ?3, ?4)"
        } else {
            "SELECT pnl_sol, pnl_lamports
             FROM shadow_closed_trades
             WHERE closed_ts >= ?1"
        };
        let mut stmt = self
            .conn
            .prepare(query)
            .context("failed to prepare shadow pnl query")?;
        let mut rows = if exclude_non_risk_context {
            stmt.query(params![
                since.to_rfc3339(),
                SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE,
                SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY,
                SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE
            ])
            .context("failed querying shadow pnl rows")?
        } else {
            stmt.query(params![since.to_rfc3339()])
                .context("failed querying shadow pnl rows")?
        };
        let mut trades = 0_u64;
        let mut pnl = SignedLamports::new(0);
        while let Some(row) = rows.next().context("failed iterating shadow pnl rows")? {
            let pnl_lamports = shadow_closed_trade_pnl_lamports(
                row.get(0)
                    .context("failed reading shadow_closed_trades.pnl_sol")?,
                row.get(1)
                    .context("failed reading shadow_closed_trades.pnl_lamports")?,
                "shadow realized pnl",
            )?;
            trades = trades.saturating_add(1);
            pnl = pnl.checked_add(pnl_lamports).ok_or_else(|| {
                anyhow!("shadow realized pnl lamports overflow while summing closed trades")
            })?;
        }
        Ok((trades, pnl))
    }

    pub fn shadow_rug_loss_count_since(
        &self,
        since: DateTime<Utc>,
        return_threshold: f64,
    ) -> Result<u64> {
        Ok(self
            .shadow_rug_loss_samples(since, u64::MAX, return_threshold)?
            .0)
    }

    pub fn shadow_rug_loss_rate_recent(
        &self,
        since: DateTime<Utc>,
        sample_size: u64,
        return_threshold: f64,
    ) -> Result<(u64, u64, f64)> {
        let (rug_count, total_count) =
            self.shadow_rug_loss_samples(since, sample_size.max(1), return_threshold)?;
        let rate = if total_count == 0 {
            0.0
        } else {
            rug_count as f64 / total_count as f64
        };
        Ok((rug_count, total_count, rate))
    }

    fn shadow_rug_loss_samples(
        &self,
        since: DateTime<Utc>,
        sample_size: u64,
        return_threshold: f64,
    ) -> Result<(u64, u64)> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT entry_cost_sol, entry_cost_lamports, pnl_sol, pnl_lamports
                 FROM shadow_closed_trades
                 WHERE closed_ts >= ?1
                   AND COALESCE(close_context, 'market') NOT IN (?2, ?3, ?4)
                 ORDER BY closed_ts DESC, id DESC
                 LIMIT ?5",
            )
            .context("failed to prepare shadow rug-loss sample query")?;
        let limit = sample_size.min(i64::MAX as u64) as i64;
        let mut rows = stmt
            .query(params![
                since.to_rfc3339(),
                SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE,
                SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY,
                SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE,
                limit
            ])
            .context("failed querying shadow rug-loss sample rows")?;
        let mut rug_count = 0_u64;
        let mut total_count = 0_u64;
        while let Some(row) = rows
            .next()
            .context("failed iterating shadow rug-loss sample rows")?
        {
            total_count = total_count.saturating_add(1);
            let entry_cost = lamports_to_sol(shadow_closed_trade_entry_cost_lamports(
                row.get(0)
                    .context("failed reading shadow_closed_trades.entry_cost_sol")?,
                row.get(1)
                    .context("failed reading shadow_closed_trades.entry_cost_lamports")?,
                "shadow rug-loss sample",
            )?);
            if entry_cost <= 0.0 {
                continue;
            }
            let pnl = signed_lamports_to_sol(shadow_closed_trade_pnl_lamports(
                row.get(2)
                    .context("failed reading shadow_closed_trades.pnl_sol")?,
                row.get(3)
                    .context("failed reading shadow_closed_trades.pnl_lamports")?,
                "shadow rug-loss sample",
            )?);
            if pnl <= entry_cost * return_threshold {
                rug_count = rug_count.saturating_add(1);
            }
        }
        Ok((rug_count, total_count))
    }
}
