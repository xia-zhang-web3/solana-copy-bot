use crate::execution_canary_quote_pnl_compute::{compute_quote_pnl, QuotePnlAmounts};
use crate::{ExecutableWalletFeedback, SqliteDiscoveryStore, SHADOW_CLOSE_CONTEXT_MARKET};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::params;
use std::collections::HashMap;

impl SqliteDiscoveryStore {
    pub fn executable_wallet_feedback_since(
        &self,
        since: DateTime<Utc>,
    ) -> Result<HashMap<String, ExecutableWalletFeedback>> {
        if !self.sqlite_table_exists("shadow_closed_trades")?
            || !self.sqlite_table_exists("execution_quote_canary_events")?
        {
            return Ok(HashMap::new());
        }

        let mut stmt = self
            .conn
            .prepare(
                "SELECT
                    closed.wallet_id,
                    closed.pnl_sol,
                    buy.quote_in_amount_raw,
                    buy.quote_out_amount_raw,
                    sell.quote_in_amount_raw,
                    sell.quote_out_amount_raw,
                    buy.priority_fee_lamports,
                    sell.priority_fee_lamports
                 FROM shadow_closed_trades closed
                 JOIN execution_quote_canary_events sell
                   ON sell.shadow_closed_trade_id = closed.id
                  AND sell.side = 'sell'
                 JOIN execution_quote_canary_events buy
                   ON buy.event_id = (
                        SELECT candidate.event_id
                        FROM execution_quote_canary_events candidate
                        WHERE candidate.side = 'buy'
                          AND candidate.wallet_id = closed.wallet_id
                          AND candidate.token = closed.token
                          AND substr(candidate.signal_ts, 1, 19) = substr(closed.opened_ts, 1, 19)
                        ORDER BY candidate.request_ts DESC, candidate.event_id DESC
                        LIMIT 1
                   )
                 WHERE closed.closed_ts >= ?1
                   AND COALESCE(closed.close_context, 'market') = ?2
                   AND closed.signal_id NOT LIKE 'stale-close-%'
                   AND buy.quote_status = 'ok'
                   AND sell.quote_status = 'ok'
                   AND buy.decision_status = 'would_execute'
                   AND sell.decision_status IN ('would_execute', 'would_force_exit')",
            )
            .context("failed to prepare executable wallet feedback query")?;

        let mut rows = stmt
            .query(params![since.to_rfc3339(), SHADOW_CLOSE_CONTEXT_MARKET])
            .context("failed to query executable wallet feedback")?;
        let mut feedback = HashMap::<String, ExecutableWalletFeedback>::new();
        while let Some(row) = rows
            .next()
            .context("failed reading executable wallet feedback row")?
        {
            let wallet_id: String = row.get(0).context("failed reading feedback wallet_id")?;
            let shadow_pnl_sol: f64 = row.get(1).context("failed reading feedback pnl_sol")?;
            let entry_in_raw: Option<String> = row.get(2)?;
            let entry_out_raw: Option<String> = row.get(3)?;
            let exit_in_raw: Option<String> = row.get(4)?;
            let exit_out_raw: Option<String> = row.get(5)?;
            let buy_priority_fee = optional_i64_to_u64(row.get(6)?)?;
            let sell_priority_fee = optional_i64_to_u64(row.get(7)?)?;
            let pnl = compute_quote_pnl(QuotePnlAmounts {
                entry_in_raw: entry_in_raw.as_deref(),
                entry_out_raw: entry_out_raw.as_deref(),
                exit_in_raw: exit_in_raw.as_deref(),
                exit_out_raw: exit_out_raw.as_deref(),
                buy_priority_fee_lamports: buy_priority_fee,
                sell_priority_fee_lamports: sell_priority_fee,
            })?;
            let Some(pnl) = pnl else {
                continue;
            };
            feedback.entry(wallet_id).or_default().record(
                shadow_pnl_sol,
                pnl.quote_adjusted_pnl_after_priority_fee_sol,
            );
        }
        Ok(feedback)
    }
}

fn optional_i64_to_u64(value: Option<i64>) -> Result<Option<u64>> {
    value
        .map(|raw| u64::try_from(raw).context("negative priority fee lamports"))
        .transpose()
}
