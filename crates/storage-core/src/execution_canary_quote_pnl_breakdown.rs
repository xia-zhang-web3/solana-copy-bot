use crate::{
    ExecutionCanaryShadowCloseBreakdown, ExecutionCanaryShadowCloseContextSummary,
    SqliteDiscoveryStore, SHADOW_CLOSE_CONTEXT_MARKET,
    SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE, SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE,
    SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::params;

impl SqliteDiscoveryStore {
    pub(crate) fn execution_canary_shadow_close_breakdown(
        &self,
        since: DateTime<Utc>,
    ) -> Result<ExecutionCanaryShadowCloseBreakdown> {
        if !self.sqlite_table_exists("shadow_closed_trades")? {
            return Ok(ExecutionCanaryShadowCloseBreakdown::default());
        }

        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    COALESCE(close_context, 'market') AS close_context,
                    COUNT(*) AS closed_trades,
                    COALESCE(SUM(CASE WHEN pnl_sol > 0 THEN 1 ELSE 0 END), 0) AS win_count,
                    COALESCE(SUM(CASE WHEN pnl_sol < 0 THEN 1 ELSE 0 END), 0) AS loss_count,
                    COALESCE(SUM(pnl_sol), 0.0) AS pnl_sol
                 FROM shadow_closed_trades
                 WHERE closed_ts >= ?1
                 GROUP BY COALESCE(close_context, 'market')
                 ORDER BY
                    CASE
                        WHEN COALESCE(close_context, 'market') = 'market' THEN 0
                        WHEN COALESCE(close_context, 'market') LIKE 'stale_%' THEN 1
                        ELSE 2
                    END,
                    COALESCE(close_context, 'market')",
            )
            .context("failed to prepare shadow close context breakdown query")?;
        let rows = stmt
            .query_map(params![since.to_rfc3339()], read_context_summary)
            .context("failed querying shadow close context breakdown")?;

        let mut breakdown = ExecutionCanaryShadowCloseBreakdown::default();
        for row in rows {
            record_context_summary(&mut breakdown, row?);
        }
        Ok(breakdown)
    }
}

fn read_context_summary(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ExecutionCanaryShadowCloseContextSummary> {
    Ok(ExecutionCanaryShadowCloseContextSummary {
        close_context: row.get(0)?,
        closed_trades: read_u64(row, 1)?,
        win_count: read_u64(row, 2)?,
        loss_count: read_u64(row, 3)?,
        pnl_sol: row.get(4)?,
    })
}

fn read_u64(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<u64> {
    let raw: i64 = row.get(index)?;
    u64::try_from(raw).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })
}

fn record_context_summary(
    breakdown: &mut ExecutionCanaryShadowCloseBreakdown,
    context: ExecutionCanaryShadowCloseContextSummary,
) {
    breakdown.total_closed_trades += context.closed_trades;
    breakdown.total_win_count += context.win_count;
    breakdown.total_loss_count += context.loss_count;
    breakdown.total_pnl_sol += context.pnl_sol;

    if context.close_context == SHADOW_CLOSE_CONTEXT_MARKET {
        breakdown.market_closed_trades += context.closed_trades;
        breakdown.market_pnl_sol += context.pnl_sol;
    } else {
        breakdown.non_market_closed_trades += context.closed_trades;
        breakdown.non_market_pnl_sol += context.pnl_sol;
    }

    if is_stale_context(&context.close_context) {
        breakdown.stale_closed_trades += context.closed_trades;
        breakdown.stale_pnl_sol += context.pnl_sol;
    }

    breakdown.contexts.push(context);
}

fn is_stale_context(close_context: &str) -> bool {
    matches!(
        close_context,
        SHADOW_CLOSE_CONTEXT_STALE_QUOTE_PRICE
            | SHADOW_CLOSE_CONTEXT_STALE_TERMINAL_ZERO_PRICE
            | SHADOW_CLOSE_CONTEXT_RECOVERY_TERMINAL_ZERO_PRICE
    )
}
