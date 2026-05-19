use crate::{
    money::{
        lamports_to_sol, shadow_closed_trade_entry_cost_lamports, shadow_closed_trade_pnl_lamports,
        signed_lamports_to_sol,
    },
    ShadowWalletFeedback, ShadowWalletTokenFastLossCooldown, SqliteDiscoveryStore,
    SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY, SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::params;

impl SqliteDiscoveryStore {
    pub fn shadow_wallet_loss_feedback_since(
        &self,
        wallet_id: &str,
        since: DateTime<Utc>,
    ) -> Result<Option<ShadowWalletFeedback>> {
        if !self.sqlite_table_exists("shadow_closed_trades")? {
            return Ok(None);
        }
        let mut stmt = self
            .conn
            .prepare(
                "SELECT entry_cost_sol, entry_cost_lamports, pnl_sol, pnl_lamports
                 FROM shadow_closed_trades
                 WHERE wallet_id = ?1
                   AND closed_ts >= ?2
                   AND COALESCE(close_context, 'market') NOT IN (?3, ?4)",
            )
            .context("failed to prepare shadow wallet runtime feedback query")?;
        let mut rows = stmt
            .query(params![
                wallet_id,
                since.to_rfc3339(),
                SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY,
                SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE
            ])
            .context("failed querying shadow wallet runtime feedback rows")?;
        let mut feedback = ShadowWalletFeedback::default();
        while let Some(row) = rows
            .next()
            .context("failed iterating shadow wallet runtime feedback rows")?
        {
            let entry_cost = lamports_to_sol(shadow_closed_trade_entry_cost_lamports(
                row.get(0)
                    .context("failed reading shadow_closed_trades.entry_cost_sol")?,
                row.get(1)
                    .context("failed reading shadow_closed_trades.entry_cost_lamports")?,
                "shadow wallet runtime feedback",
            )?);
            let pnl = signed_lamports_to_sol(shadow_closed_trade_pnl_lamports(
                row.get(2)
                    .context("failed reading shadow_closed_trades.pnl_sol")?,
                row.get(3)
                    .context("failed reading shadow_closed_trades.pnl_lamports")?,
                "shadow wallet runtime feedback",
            )?);
            feedback.closed_trades = feedback.closed_trades.saturating_add(1);
            feedback.entry_cost_sol += entry_cost;
            feedback.pnl_sol += pnl;
        }
        Ok((feedback.closed_trades > 0).then_some(feedback))
    }

    pub fn shadow_wallet_token_fast_loss_since(
        &self,
        wallet_id: &str,
        token: &str,
        since: DateTime<Utc>,
        min_entry_sol: f64,
        max_hold_seconds: u64,
        max_roi: f64,
    ) -> Result<Option<ShadowWalletTokenFastLossCooldown>> {
        if !self.sqlite_table_exists("shadow_closed_trades")? {
            return Ok(None);
        }
        let mut stmt = self
            .conn
            .prepare(
                "SELECT entry_cost_sol, entry_cost_lamports, pnl_sol, pnl_lamports, opened_ts, closed_ts
                 FROM shadow_closed_trades
                 WHERE wallet_id = ?1
                   AND token = ?2
                   AND closed_ts >= ?3
                   AND COALESCE(close_context, 'market') NOT IN (?4, ?5)
                 ORDER BY closed_ts DESC",
            )
            .context("failed to prepare shadow wallet/token fast loss query")?;
        let mut rows = stmt
            .query(params![
                wallet_id,
                token,
                since.to_rfc3339(),
                SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY,
                SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE
            ])
            .context("failed querying shadow wallet/token fast loss rows")?;
        while let Some(row) = rows
            .next()
            .context("failed iterating shadow wallet/token fast loss rows")?
        {
            let entry_cost = lamports_to_sol(shadow_closed_trade_entry_cost_lamports(
                row.get(0)
                    .context("failed reading shadow_closed_trades.entry_cost_sol")?,
                row.get(1)
                    .context("failed reading shadow_closed_trades.entry_cost_lamports")?,
                "shadow wallet/token fast loss",
            )?);
            if entry_cost < min_entry_sol {
                continue;
            }
            let pnl = signed_lamports_to_sol(shadow_closed_trade_pnl_lamports(
                row.get(2)
                    .context("failed reading shadow_closed_trades.pnl_sol")?,
                row.get(3)
                    .context("failed reading shadow_closed_trades.pnl_lamports")?,
                "shadow wallet/token fast loss",
            )?);
            if entry_cost <= 0.0 {
                continue;
            }
            let roi = pnl / entry_cost;
            if roi > max_roi {
                continue;
            }
            let opened_ts: String = row
                .get(4)
                .context("failed reading shadow_closed_trades.opened_ts")?;
            let closed_ts: String = row
                .get(5)
                .context("failed reading shadow_closed_trades.closed_ts")?;
            let opened_ts = DateTime::parse_from_rfc3339(&opened_ts)
                .context("invalid shadow_closed_trades.opened_ts")?
                .with_timezone(&Utc);
            let closed_ts = DateTime::parse_from_rfc3339(&closed_ts)
                .context("invalid shadow_closed_trades.closed_ts")?
                .with_timezone(&Utc);
            let hold_seconds = (closed_ts - opened_ts).num_seconds();
            if hold_seconds < 0 || hold_seconds > max_hold_seconds as i64 {
                continue;
            }
            return Ok(Some(ShadowWalletTokenFastLossCooldown {
                wallet_id: wallet_id.to_string(),
                token: token.to_string(),
                entry_cost_sol: entry_cost,
                pnl_sol: pnl,
                roi,
                hold_seconds,
                closed_ts,
            }));
        }
        Ok(None)
    }
}
