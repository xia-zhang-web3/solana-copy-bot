use crate::{
    money::{
        lamports_to_sol, shadow_closed_trade_entry_cost_lamports, shadow_closed_trade_pnl_lamports,
        shadow_lot_cost_lamports, signed_lamports_to_sol,
    },
    ShadowTokenLossCooldown, ShadowWalletFeedback, SqliteDiscoveryStore,
    SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY, SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE,
    SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE, SHADOW_LOT_OPEN_EPS,
    SHADOW_RISK_CONTEXT_MARKET,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::{Lamports, SignedLamports};
use rusqlite::params;
use std::collections::HashMap;

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

    pub fn shadow_wallet_feedback_since(
        &self,
        since: DateTime<Utc>,
        max_fast_loss_hold_seconds: i64,
    ) -> Result<HashMap<String, ShadowWalletFeedback>> {
        if !self.sqlite_table_exists("shadow_closed_trades")? {
            return Ok(HashMap::new());
        }
        let mut stmt = self
            .conn
            .prepare(
                "SELECT wallet_id, signal_id, entry_cost_sol, entry_cost_lamports,
                        pnl_sol, pnl_lamports, opened_ts, closed_ts,
                        COALESCE(close_context, 'market')
                 FROM shadow_closed_trades
                 WHERE closed_ts >= ?1
                   AND COALESCE(close_context, 'market') NOT IN (?2, ?3)",
            )
            .context("failed to prepare shadow wallet feedback query")?;
        let mut rows = stmt
            .query(params![
                since.to_rfc3339(),
                SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY,
                SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE
            ])
            .context("failed querying shadow wallet feedback rows")?;
        let mut feedback = HashMap::<String, ShadowWalletFeedback>::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating shadow wallet feedback rows")?
        {
            let wallet_id: String = row
                .get(0)
                .context("failed reading shadow_closed_trades.wallet_id")?;
            let signal_id: String = row
                .get(1)
                .context("failed reading shadow_closed_trades.signal_id")?;
            let entry_cost = lamports_to_sol(shadow_closed_trade_entry_cost_lamports(
                row.get(2)
                    .context("failed reading shadow_closed_trades.entry_cost_sol")?,
                row.get(3)
                    .context("failed reading shadow_closed_trades.entry_cost_lamports")?,
                "shadow wallet feedback",
            )?);
            let pnl = signed_lamports_to_sol(shadow_closed_trade_pnl_lamports(
                row.get(4)
                    .context("failed reading shadow_closed_trades.pnl_sol")?,
                row.get(5)
                    .context("failed reading shadow_closed_trades.pnl_lamports")?,
                "shadow wallet feedback",
            )?);
            let opened_ts = parse_shadow_feedback_ts(
                row.get(6)
                    .context("failed reading shadow_closed_trades.opened_ts")?,
                "opened_ts",
            )?;
            let closed_ts = parse_shadow_feedback_ts(
                row.get(7)
                    .context("failed reading shadow_closed_trades.closed_ts")?,
                "closed_ts",
            )?;
            let close_context: String = row
                .get(8)
                .context("failed reading shadow_closed_trades.close_context")?;
            if close_context == SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE {
                continue;
            }
            let entry = feedback.entry(wallet_id).or_default();
            entry.record_risk_trade(entry_cost, pnl);
            let hold_seconds = (closed_ts - opened_ts).num_seconds();
            if hold_seconds >= 0 {
                if hold_seconds <= max_fast_loss_hold_seconds {
                    entry.record_fast_loss(entry_cost, pnl, hold_seconds);
                }
                if signal_id.starts_with("stale-close-") {
                    entry.record_stale_priced_loss(entry_cost, pnl, hold_seconds);
                }
            }
        }
        Ok(feedback)
    }

    pub fn shadow_token_loss_cooldown_since(
        &self,
        token: &str,
        since: DateTime<Utc>,
        return_threshold: f64,
        count_threshold: u64,
        catastrophe_min_entry_sol: f64,
        catastrophe_return_threshold: f64,
    ) -> Result<Option<ShadowTokenLossCooldown>> {
        if count_threshold == 0 || !self.sqlite_table_exists("shadow_closed_trades")? {
            return Ok(None);
        }
        let mut stmt = self
            .conn
            .prepare(
                "SELECT entry_cost_sol, entry_cost_lamports, pnl_sol, pnl_lamports, closed_ts,
                        COALESCE(close_context, 'market')
                 FROM shadow_closed_trades
                 WHERE token = ?1
                   AND closed_ts >= ?2
                   AND COALESCE(close_context, 'market') NOT IN (?3, ?4)
                 ORDER BY closed_ts DESC, id DESC",
            )
            .context("failed to prepare shadow token loss cooldown query")?;
        let mut rows = stmt
            .query(params![
                token,
                since.to_rfc3339(),
                SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY,
                SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE
            ])
            .context("failed querying shadow token loss cooldown rows")?;
        let mut cooldown = ShadowTokenLossCooldown {
            token: token.to_string(),
            ..ShadowTokenLossCooldown::default()
        };
        while let Some(row) = rows
            .next()
            .context("failed iterating shadow token loss cooldown rows")?
        {
            cooldown.sampled_trades = cooldown.sampled_trades.saturating_add(1);
            let entry_cost = lamports_to_sol(shadow_closed_trade_entry_cost_lamports(
                row.get(0)
                    .context("failed reading shadow_closed_trades.entry_cost_sol")?,
                row.get(1)
                    .context("failed reading shadow_closed_trades.entry_cost_lamports")?,
                "shadow token loss cooldown",
            )?);
            let pnl = signed_lamports_to_sol(shadow_closed_trade_pnl_lamports(
                row.get(2)
                    .context("failed reading shadow_closed_trades.pnl_sol")?,
                row.get(3)
                    .context("failed reading shadow_closed_trades.pnl_lamports")?,
                "shadow token loss cooldown",
            )?);
            if entry_cost <= 0.0 {
                continue;
            }
            let roi = pnl / entry_cost;
            cooldown.entry_cost_sol += entry_cost;
            cooldown.pnl_sol += pnl;
            cooldown.worst_roi = Some(cooldown.worst_roi.map_or(roi, |worst: f64| worst.min(roi)));
            let close_context: String = row
                .get(5)
                .context("failed reading shadow_closed_trades.close_context")?;
            if entry_cost >= catastrophe_min_entry_sol
                && (roi <= catastrophe_return_threshold
                    || close_context == SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE)
            {
                cooldown.catastrophe_count = cooldown.catastrophe_count.saturating_add(1);
                cooldown.catastrophe_entry_cost_sol += entry_cost;
                cooldown.catastrophe_worst_roi = Some(
                    cooldown
                        .catastrophe_worst_roi
                        .map_or(roi, |worst: f64| worst.min(roi)),
                );
            }
            let raw_closed_ts: String = row
                .get(4)
                .context("failed reading shadow_closed_trades.closed_ts")?;
            let closed_ts = DateTime::parse_from_rfc3339(&raw_closed_ts)
                .with_context(|| {
                    format!("invalid shadow_closed_trades.closed_ts: {raw_closed_ts}")
                })?
                .with_timezone(&Utc);
            cooldown.last_closed_ts = Some(
                cooldown
                    .last_closed_ts
                    .map_or(closed_ts, |last: DateTime<Utc>| last.max(closed_ts)),
            );
            if pnl <= entry_cost * return_threshold {
                cooldown.loss_count = cooldown.loss_count.saturating_add(1);
            }
        }
        let aggregate_loss = cooldown.sampled_trades >= count_threshold
            && cooldown
                .aggregate_roi()
                .is_some_and(|roi| roi <= return_threshold);
        if cooldown.catastrophe_count > 0
            || cooldown.loss_count >= count_threshold
            || aggregate_loss
        {
            Ok(Some(cooldown))
        } else {
            Ok(None)
        }
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
                   AND COALESCE(close_context, 'market') NOT IN (?2, ?3)
                 ORDER BY closed_ts DESC, id DESC
                 LIMIT ?4",
            )
            .context("failed to prepare shadow rug-loss sample query")?;
        let limit = sample_size.min(i64::MAX as u64) as i64;
        let mut rows = stmt
            .query(params![
                since.to_rfc3339(),
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

fn parse_shadow_feedback_ts(raw: String, field: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(&raw)
        .with_context(|| format!("invalid shadow_closed_trades.{field}: {raw}"))?
        .with_timezone(&Utc))
}
