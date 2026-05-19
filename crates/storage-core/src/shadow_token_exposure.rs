use crate::{
    money::shadow_lot_cost_lamports, SqliteDiscoveryStore, SHADOW_LOT_OPEN_EPS,
    SHADOW_RISK_CONTEXT_MARKET,
};
use anyhow::{anyhow, Context, Result};
use copybot_core_types::Lamports;
use rusqlite::params;

impl SqliteDiscoveryStore {
    pub fn shadow_risk_open_lot_count_for_token(&self, token: &str) -> Result<u64> {
        let open_lots: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*)
                 FROM shadow_lots
                 WHERE token = ?1
                   AND qty > ?2
                   AND risk_context = ?3",
                params![token, SHADOW_LOT_OPEN_EPS, SHADOW_RISK_CONTEXT_MARKET],
                |row| row.get(0),
            )
            .context("failed querying shadow token open lot count")?;
        u64::try_from(open_lots).context("shadow token open lot count was negative")
    }

    pub fn shadow_risk_open_notional_lamports_for_token(&self, token: &str) -> Result<Lamports> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT cost_sol, cost_lamports
                 FROM shadow_lots
                 WHERE token = ?1
                   AND qty > ?2
                   AND risk_context = ?3",
            )
            .context("failed to prepare shadow token open notional query")?;
        let mut rows = stmt
            .query(params![
                token,
                SHADOW_LOT_OPEN_EPS,
                SHADOW_RISK_CONTEXT_MARKET
            ])
            .context("failed querying shadow token open notional rows")?;
        let mut total = Lamports::ZERO;
        while let Some(row) = rows
            .next()
            .context("failed iterating shadow token open notional rows")?
        {
            let cost = shadow_lot_cost_lamports(
                row.get(0).context("failed reading shadow_lots.cost_sol")?,
                row.get(1)
                    .context("failed reading shadow_lots.cost_lamports")?,
                "shadow token open notional",
            )?;
            total = total.checked_add(cost).ok_or_else(|| {
                anyhow!("shadow token open notional lamports overflow while summing lots")
            })?;
        }
        Ok(total)
    }
}
