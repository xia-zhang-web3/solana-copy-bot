//! Legacy dry-run candidate recording remains separate from owner BUY.
use super::{ExecutionCanaryRunner, ExecutionCanaryTickSummary, CANARY_COPY_SIGNAL_STATUS};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use copybot_core_types::CopySignalRow;
use copybot_storage_core::{ExecutionDryRunRecordOutcome, SqliteStore};

impl ExecutionCanaryRunner {
    pub(super) fn process_dry_run_orders(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        since: DateTime<Utc>,
        summary: &mut ExecutionCanaryTickSummary,
    ) -> Result<Vec<CopySignalRow>> {
        let signals = store
            .list_execution_canary_ready_candidates(
                CANARY_COPY_SIGNAL_STATUS,
                since,
                self.config.canary_batch_limit.max(1),
                self.quote_canary.is_enabled(),
            )
            .context("failed loading execution canary candidates")?
            .into_iter()
            .filter(|s| !self.quote_canary.entry_pending(&s.signal_id))
            .collect::<Vec<_>>();
        summary.candidates = signals.len();
        for signal in &signals {
            let outcome = store
                .record_execution_dry_run_order(&signal.signal_id, &self.config.canary_route, now)
                .with_context(|| {
                    format!(
                        "failed recording execution dry-run order for signal {}",
                        signal.signal_id
                    )
                })?;
            summary.last_signal_id = Some(signal.signal_id.clone());
            match outcome {
                ExecutionDryRunRecordOutcome::Inserted => summary.inserted += 1,
                ExecutionDryRunRecordOutcome::Existing => summary.existing += 1,
            }
        }
        Ok(signals)
    }
}
