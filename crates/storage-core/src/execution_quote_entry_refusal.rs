//! A terminal owner decision on an existing canonical entry, separate from provider facts.
use crate::{
    execution_quote_canary::ensure_execution_quote_canary_tables, ExecutionQuoteCanaryEventInsert,
    SqliteDiscoveryStore,
};
use anyhow::{ensure, Context, Result};
use rusqlite::params;

pub fn execution_quote_entry_is_refused(event: &ExecutionQuoteCanaryEventInsert) -> bool {
    event
        .decision_reason
        .as_deref()
        .is_some_and(|r| r.starts_with("hot_buy_refused:"))
}

impl SqliteDiscoveryStore {
    pub fn execution_quote_entry_refused(&self, signal_id: &str) -> Result<bool> {
        Ok(self
            .load_execution_quote_canary_event_by_id(&format!("quote:entry:{signal_id}"))?
            .as_ref()
            .is_some_and(execution_quote_entry_is_refused))
    }

    /// Only decision fields change. The first refusal survives later completions and fee refreshes.
    pub fn mark_execution_quote_entry_refused(
        &self,
        event: &ExecutionQuoteCanaryEventInsert,
        reason: &str,
    ) -> Result<()> {
        ensure_execution_quote_canary_tables(self)?;
        let signal = event
            .signal_id
            .as_deref()
            .context("entry refusal signal missing")?;
        ensure!(
            event.event_id == format!("quote:entry:{signal}")
                && event.side == "buy"
                && event.shadow_closed_trade_id.is_none()
                && event.signal_ts.is_some(),
            "entry refusal is not a canonical BUY"
        );
        let updated = self.execute_with_retry(|conn| conn.execute(
            "UPDATE execution_quote_canary_events
             SET decision_status = CASE WHEN quote_status = 'error' THEN 'unknown' ELSE 'would_skip' END,
                 decision_reason = CASE WHEN decision_reason GLOB 'hot_buy_refused:*'
                    THEN decision_reason ELSE ?7 END
             WHERE event_id = ?1 AND signal_id = ?2 AND wallet_id = ?3 AND token = ?4
               AND side = 'buy' AND shadow_closed_trade_id IS NULL
               AND signal_ts = ?5 AND request_ts = ?6",
            params![event.event_id, signal, event.wallet_id, event.token,
                event.signal_ts.map(|ts| ts.to_rfc3339()), event.request_ts.to_rfc3339(),
                format!("hot_buy_refused:{reason}")],
        )).context("failed persisting canonical entry refusal")?;
        ensure!(updated == 1, "entry refusal identity changed or missing");
        Ok(())
    }
}
