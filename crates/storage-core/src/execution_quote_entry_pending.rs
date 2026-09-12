//! Network quotes are not owner permission. Only a committed owner decision opens a pending entry.
use crate::{
    execution_quote_canary::ensure_execution_quote_canary_tables, execution_quote_entry_is_refused,
    ExecutionQuoteCanaryEventInsert, SqliteDiscoveryStore,
};
use anyhow::{ensure, Context, Result};
use rusqlite::params;

pub const EXECUTION_QUOTE_OWNER_PENDING: &str = "owner_pending";

pub fn execution_quote_entry_is_closed(event: &ExecutionQuoteCanaryEventInsert) -> bool {
    execution_quote_entry_is_refused(event)
        || event.decision_status.as_deref() == Some(EXECUTION_QUOTE_OWNER_PENDING)
}

pub fn pend_execution_quote_entry(event: &mut ExecutionQuoteCanaryEventInsert) -> Result<()> {
    ensure!(
        !execution_quote_entry_is_refused(event),
        "terminal entry refusal"
    );
    if event.decision_status.as_deref() != Some(EXECUTION_QUOTE_OWNER_PENDING) {
        event.decision_reason = Some(serde_json::to_string(&(
            event.decision_status.clone(),
            event.decision_reason.clone(),
        ))?);
        event.decision_status = Some(EXECUTION_QUOTE_OWNER_PENDING.into());
    }
    Ok(())
}

impl SqliteDiscoveryStore {
    pub fn execution_quote_entry_blocked(&self, signal: &str, require_quote: bool) -> Result<bool> {
        Ok(self
            .load_execution_quote_canary_event_by_id(&format!("quote:entry:{signal}"))?
            .as_ref()
            .map_or(require_quote, execution_quote_entry_is_closed))
    }

    /// Close an existing priority-retry entry before dispatching the owner's network work.
    pub fn pend_execution_quote_entry(
        &self,
        expected: &ExecutionQuoteCanaryEventInsert,
    ) -> Result<ExecutionQuoteCanaryEventInsert> {
        let mut pending = expected.clone();
        pend_execution_quote_entry(&mut pending)?;
        self.replace_entry_owner_decision(expected, &pending)?;
        Ok(pending)
    }

    /// Called only after the main-loop owner revalidates its still-owned origin.
    /// CAS makes a duplicate, late completion or different refusal unable to grant permission.
    pub fn complete_execution_quote_entry_owner(
        &self,
        expected: &ExecutionQuoteCanaryEventInsert,
    ) -> Result<()> {
        ensure!(
            expected.decision_status.as_deref() == Some(EXECUTION_QUOTE_OWNER_PENDING),
            "entry owner decision is not pending"
        );
        let (status, reason): (Option<String>, Option<String>) = serde_json::from_str(
            expected
                .decision_reason
                .as_deref()
                .context("pending quote decision missing")?,
        )?;
        let mut decided = expected.clone();
        decided.decision_status = status;
        decided.decision_reason = reason;
        ensure!(
            !execution_quote_entry_is_closed(&decided),
            "invalid pending quote decision"
        );
        self.replace_entry_owner_decision(expected, &decided)
    }

    fn replace_entry_owner_decision(
        &self,
        expected: &ExecutionQuoteCanaryEventInsert,
        decided: &ExecutionQuoteCanaryEventInsert,
    ) -> Result<()> {
        ensure_execution_quote_canary_tables(self)?;
        let signal = expected
            .signal_id
            .as_deref()
            .context("owner entry signal missing")?;
        ensure!(
            expected.event_id == format!("quote:entry:{signal}")
                && expected.side == "buy"
                && expected.shadow_closed_trade_id.is_none()
                && expected.signal_ts.is_some(),
            "owner entry identity invalid"
        );
        self.with_immediate_transaction_retry("commit entry owner decision", |conn| {
            let current = self.load_execution_quote_canary_event_by_id(&expected.event_id)?;
            ensure!(
                current.as_ref() == Some(expected),
                "entry owner decision changed"
            );
            ensure!(
                !execution_quote_entry_is_refused(expected),
                "terminal entry refusal"
            );
            let changed = conn.execute(
                "UPDATE execution_quote_canary_events
                SET decision_status=?2,decision_reason=?3 WHERE event_id=?1",
                params![
                    expected.event_id,
                    decided.decision_status,
                    decided.decision_reason
                ],
            )?;
            ensure!(changed == 1, "entry owner decision missing");
            Ok(())
        })
    }
}
