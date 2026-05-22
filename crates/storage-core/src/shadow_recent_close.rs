use crate::{
    money::{
        lamports_to_sol, shadow_closed_trade_entry_cost_lamports, shadow_closed_trade_pnl_lamports,
        signed_lamports_to_sol,
    },
    ShadowTokenRecentClose, SqliteDiscoveryStore, SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY,
    SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::params;

impl SqliteDiscoveryStore {
    pub fn shadow_token_recent_close_since(
        &self,
        token: &str,
        since: DateTime<Utc>,
    ) -> Result<Option<ShadowTokenRecentClose>> {
        if !self.sqlite_table_exists("shadow_closed_trades")? {
            return Ok(None);
        }
        let mut stmt = self
            .conn
            .prepare(
                "SELECT entry_cost_sol, entry_cost_lamports, pnl_sol, pnl_lamports, closed_ts
                 FROM shadow_closed_trades
                 WHERE token = ?1
                   AND closed_ts >= ?2
                   AND COALESCE(close_context, 'market') NOT IN (?3, ?4)
                 ORDER BY closed_ts DESC, id DESC
                 LIMIT 1",
            )
            .context("failed to prepare shadow token recent close query")?;
        let mut rows = stmt
            .query(params![
                token,
                since.to_rfc3339(),
                SHADOW_CLOSE_CONTEXT_QUARANTINED_LEGACY,
                SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE
            ])
            .context("failed querying shadow token recent close")?;
        let Some(row) = rows
            .next()
            .context("failed iterating shadow token recent close rows")?
        else {
            return Ok(None);
        };
        let entry_cost = lamports_to_sol(shadow_closed_trade_entry_cost_lamports(
            row.get(0)
                .context("failed reading shadow_closed_trades.entry_cost_sol")?,
            row.get(1)
                .context("failed reading shadow_closed_trades.entry_cost_lamports")?,
            "shadow token recent close",
        )?);
        let pnl = signed_lamports_to_sol(shadow_closed_trade_pnl_lamports(
            row.get(2)
                .context("failed reading shadow_closed_trades.pnl_sol")?,
            row.get(3)
                .context("failed reading shadow_closed_trades.pnl_lamports")?,
            "shadow token recent close",
        )?);
        let raw_closed_ts: String = row
            .get(4)
            .context("failed reading shadow_closed_trades.closed_ts")?;
        let closed_ts = DateTime::parse_from_rfc3339(&raw_closed_ts)
            .with_context(|| format!("invalid shadow_closed_trades.closed_ts: {raw_closed_ts}"))?
            .with_timezone(&Utc);
        Ok(Some(ShadowTokenRecentClose {
            token: token.to_string(),
            entry_cost_sol: entry_cost,
            pnl_sol: pnl,
            roi: (entry_cost > 0.0).then_some(pnl / entry_cost),
            closed_ts,
        }))
    }
}
