use anyhow::{Context, Result};

use super::*;

pub(super) fn shutdown_app_loop_tasks(
    observed_swap_retention_handle: &mut Option<
        JoinHandle<Result<ObservedSwapRetentionMaintenanceSummary>>,
    >,
    shadow_scheduler: &mut ShadowScheduler,
    observed_swap_writer: ObservedSwapWriter,
    system_event_store: &copybot_storage_core::SqliteStore,
) -> Result<()> {
    if let Some(handle) = observed_swap_retention_handle.take() {
        handle.abort();
    }
    if let Some(handle) = shadow_scheduler.shadow_snapshot_handle.take() {
        handle.abort();
    }
    observed_swap_writer
        .shutdown()
        .context("failed to shut down observed swap writer")?;

    system_event_store
        .record_heartbeat("copybot-app", "shutdown")
        .context("failed to write shutdown heartbeat")?;
    Ok(())
}
