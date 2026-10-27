//! Shared genuine owner-exit tick; no source/discovery/ingress initialization.
use super::{ExecutionCanaryRunner, ExecutionCanaryTickSummary};
use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_storage_core::SqliteStore;

impl ExecutionCanaryRunner {
    pub(super) async fn owner_exit_tick(
        &self,
        store: &SqliteStore,
        now: DateTime<Utc>,
        mut summary: ExecutionCanaryTickSummary,
    ) -> Result<ExecutionCanaryTickSummary> {
        #[cfg(test)]
        if let Some(adapter) = self.owner_exit_adapter.as_ref() {
            let owner = crate::execution_owner_exit::tick_with_adapter(
                &self.config, store, now, adapter.as_ref()).await?;
            crate::execution_canary_summary::apply_state_machine_summary(&mut summary, owner);
            return Ok(summary);
        }
        let owner = crate::execution_owner_exit::tick(&self.config, store, now).await?;
        crate::execution_canary_summary::apply_state_machine_summary(&mut summary, owner);
        Ok(summary)
    }
}
