use crate::{
    execution_quote_canary::ensure_execution_quote_canary_tables,
    observed_timestamp::parse_rfc3339_utc, ExecutionCanaryCloseCandidate, SqliteDiscoveryStore,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::params;

impl SqliteDiscoveryStore {
    pub fn list_market_exit_shadow_quote_candidates(
        &self,
        since: DateTime<Utc>,
        event_prefix: &str,
        limit: u32,
    ) -> Result<Vec<ExecutionCanaryCloseCandidate>> {
        ensure_execution_quote_canary_tables(self)?;
        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    id,
                    signal_id,
                    wallet_id,
                    token,
                    qty,
                    qty_raw,
                    qty_decimals,
                    exit_value_sol,
                    closed_ts
                 FROM shadow_closed_trades
                 WHERE closed_ts >= ?1
                   AND close_context = 'market'
                   AND signal_id NOT LIKE 'stale-close-%'
                   AND NOT EXISTS (
                        SELECT 1 FROM execution_quote_canary_events
                        WHERE execution_quote_canary_events.event_id =
                              ?2 || shadow_closed_trades.id
                   )
                 ORDER BY closed_ts ASC, id ASC
                 LIMIT ?3",
            )
            .context("failed to prepare market exit shadow quote candidate query")?;
        let mut rows = stmt
            .query(params![
                since.to_rfc3339(),
                event_prefix,
                limit.max(1) as i64
            ])
            .context("failed querying market exit shadow quote candidates")?;

        let mut candidates = Vec::new();
        while let Some(row) = rows
            .next()
            .context("failed iterating market exit shadow quote candidates")?
        {
            candidates.push(close_candidate_from_row(row)?);
        }
        Ok(candidates)
    }
}

fn close_candidate_from_row(row: &rusqlite::Row<'_>) -> Result<ExecutionCanaryCloseCandidate> {
    let qty_decimals_raw: Option<i64> = row
        .get(6)
        .context("failed reading shadow_closed_trades.qty_decimals")?;
    let qty_decimals = qty_decimals_raw
        .map(|value| {
            u8::try_from(value)
                .with_context(|| format!("invalid shadow_closed_trades.qty_decimals: {value}"))
        })
        .transpose()?;
    let closed_ts_raw: String = row
        .get(8)
        .context("failed reading shadow_closed_trades.closed_ts")?;
    Ok(ExecutionCanaryCloseCandidate {
        id: row
            .get(0)
            .context("failed reading shadow_closed_trades.id")?,
        signal_id: row
            .get(1)
            .context("failed reading shadow_closed_trades.signal_id")?,
        wallet_id: row
            .get(2)
            .context("failed reading shadow_closed_trades.wallet_id")?,
        token: row
            .get(3)
            .context("failed reading shadow_closed_trades.token")?,
        qty: row
            .get(4)
            .context("failed reading shadow_closed_trades.qty")?,
        qty_raw: row
            .get(5)
            .context("failed reading shadow_closed_trades.qty_raw")?,
        qty_decimals,
        exit_value_sol: row
            .get(7)
            .context("failed reading shadow_closed_trades.exit_value_sol")?,
        closed_ts: parse_rfc3339_utc(&closed_ts_raw, "shadow_closed_trades.closed_ts")?,
    })
}
