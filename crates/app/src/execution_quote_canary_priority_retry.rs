use super::{apply_decision_summary, ExecutionQuoteCanaryRunner, ExecutionQuoteCanaryTickSummary};
use crate::execution_quote_canary_helpers::{
    PriorityFeeSample, QUOTE_STATUS_ERROR, QUOTE_STATUS_OK,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_storage_core::{ExecutionQuoteCanaryEventInsert, SqliteStore};

impl ExecutionQuoteCanaryRunner {
    pub(super) async fn process_entry_priority_fee_retry_candidates(
        &self,
        store: &SqliteStore,
        copy_signal_status: &str,
        since: DateTime<Utc>,
        batch_limit: u32,
        priority_fee_sample: &mut Option<PriorityFeeSample>,
        summary: &mut ExecutionQuoteCanaryTickSummary,
    ) -> Result<()> {
        let signals = store
            .list_execution_quote_canary_entry_priority_fee_retry_candidates(
                copy_signal_status,
                since,
                batch_limit,
            )
            .context("failed loading execution quote canary priority fee retry candidates")?;
        summary.entry_candidates += signals.len();
        if signals.is_empty() {
            return Ok(());
        }
        let priority = self
            .priority_fee_sample_if_needed(priority_fee_sample)
            .await;
        let Some(priority) = priority.filter(|sample| priority_fee_sample_is_usable(sample)) else {
            return Ok(());
        };
        for signal in signals {
            self.refresh_existing_entry_priority_fee(store, &signal.signal_id, priority, summary)?;
        }
        Ok(())
    }

    pub(super) async fn record_existing_entry_event_if_present(
        &self,
        store: &SqliteStore,
        signal_id: &str,
        priority_fee_sample: &mut Option<PriorityFeeSample>,
        summary: &mut ExecutionQuoteCanaryTickSummary,
    ) -> Result<bool> {
        let Some(mut event) = store
            .load_latest_execution_quote_canary_entry_event(signal_id)
            .with_context(|| {
                format!("failed loading existing execution entry quote event for {signal_id}")
            })?
        else {
            return Ok(false);
        };
        if entry_event_needs_priority_fee_retry(&event) {
            if let Some(priority) = self
                .priority_fee_sample_if_needed(priority_fee_sample)
                .await
                .filter(|sample| priority_fee_sample_is_usable(sample))
            {
                self.mark_event_priority_fee_ok(store, &mut event, priority)?;
            }
        }
        if event.quote_status == QUOTE_STATUS_ERROR {
            summary.entry_errors += 1;
        }
        apply_decision_summary(&event, summary);
        summary.entry_existing += 1;
        summary.last_event_id = Some(event.event_id);
        Ok(true)
    }

    fn refresh_existing_entry_priority_fee(
        &self,
        store: &SqliteStore,
        signal_id: &str,
        priority: &PriorityFeeSample,
        summary: &mut ExecutionQuoteCanaryTickSummary,
    ) -> Result<()> {
        let Some(mut event) = store.load_latest_execution_quote_canary_entry_event(signal_id)?
        else {
            return Ok(());
        };
        if !entry_event_needs_priority_fee_retry(&event) {
            return Ok(());
        }
        self.mark_event_priority_fee_ok(store, &mut event, priority)?;
        apply_decision_summary(&event, summary);
        summary.entry_existing += 1;
        summary.last_event_id = Some(event.event_id);
        Ok(())
    }

    fn mark_event_priority_fee_ok(
        &self,
        store: &SqliteStore,
        event: &mut ExecutionQuoteCanaryEventInsert,
        priority: &PriorityFeeSample,
    ) -> Result<()> {
        let Some(lamports) = priority.lamports else {
            return Ok(());
        };
        store.mark_execution_quote_canary_priority_fee_ok(
            &event.event_id,
            lamports,
            priority.json.as_deref(),
        )?;
        event.priority_fee_status = Some(QUOTE_STATUS_OK.to_string());
        event.priority_fee_lamports = Some(lamports);
        event.priority_fee_json = priority.json.clone();
        event.error = None;
        Ok(())
    }
}

fn entry_event_needs_priority_fee_retry(event: &ExecutionQuoteCanaryEventInsert) -> bool {
    event.quote_status == QUOTE_STATUS_OK
        && (event.priority_fee_lamports.is_none()
            || event.priority_fee_status.as_deref() != Some(QUOTE_STATUS_OK))
}

fn priority_fee_sample_is_usable(sample: &PriorityFeeSample) -> bool {
    sample.status == QUOTE_STATUS_OK && sample.lamports.is_some()
}
