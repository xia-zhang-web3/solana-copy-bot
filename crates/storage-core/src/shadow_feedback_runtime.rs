use crate::{
    money::{
        lamports_to_sol, shadow_closed_trade_entry_cost_lamports, shadow_closed_trade_pnl_lamports,
        signed_lamports_to_sol,
    },
    ShadowWalletFeedback, SqliteDiscoveryStore, SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY,
    SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE,
    SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE,
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
                   AND COALESCE(close_context, 'market') NOT IN (?3, ?4, ?5)",
            )
            .context("failed to prepare shadow wallet runtime feedback query")?;
        let mut rows = stmt
            .query(params![
                wallet_id,
                since.to_rfc3339(),
                SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE,
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
}
