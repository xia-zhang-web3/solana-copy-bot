pub(super) fn handle_risk_refresh_tick(
    store: &SqliteStore,
    ingestion: &IngestionService,
    shadow_risk_guard: &mut ShadowRiskGuard,
) -> Result<()> {
    let now = Utc::now();
    shadow_risk_guard
        .observe_ingestion_snapshot(store, now, ingestion.runtime_snapshot())
        .context("shadow risk ingestion snapshot infra event failed with fatal sqlite I/O")?;
    if !shadow_risk_guard.config.shadow_killswitch_enabled {
        return Ok(());
    }
    if let Err(error) = shadow_risk_guard.maybe_refresh_db_state(store, now) {
        if shadow_risk_background_refresh_error_requires_restart(&error) {
            return Err(error)
                .context("shadow risk background refresh failed with fatal sqlite I/O");
        }
        if shadow_risk_guard.on_risk_refresh_error(now) {
            warn!(error = %error, "shadow risk background refresh failed");
        }
    }
    Ok(())
}

pub(super) fn handle_observed_swap_retention_join(
    observed_swap_retention_join: Option<
        std::result::Result<
            Result<ObservedSwapRetentionMaintenanceSummary>,
            tokio::task::JoinError,
        >,
    >,
    observed_swap_retention_handle: &mut Option<
        JoinHandle<Result<ObservedSwapRetentionMaintenanceSummary>>,
    >,
    observed_swap_retention_sweep_interval: Duration,
    last_observed_swap_retention_sweep: &mut StdInstant,
) -> Result<()> {
    *observed_swap_retention_handle = None;
    match observed_swap_retention_join {
        Some(Ok(Ok(summary))) => {
            info!(
                maintenance = SqliteMaintenanceTask::ObservedSwapRetention.as_str(),
                nominal_observed_swap_cutoff = %summary.nominal_cutoff,
                effective_observed_swap_cutoff = %summary.effective_cutoff,
                deleted_observed_swap_rows = summary.raw_deleted_rows,
                observed_swap_delete_batches = summary.raw_delete_batches,
                completed_full_sweep = summary.completed_full_sweep,
                stop_reason = ?summary.stop_reason,
                wal_checkpoint_mode = summary.checkpoint.mode,
                wal_checkpoint_busy = summary.checkpoint.busy,
                wal_log_frames = summary.checkpoint.log_frames,
                wal_checkpointed_frames = summary.checkpoint.checkpointed_frames,
                duration_ms = summary.duration_ms,
                "sqlite maintenance task completed"
            );
            if !summary.completed_full_sweep {
                *last_observed_swap_retention_sweep = StdInstant::now()
                    .checked_sub(
                        observed_swap_retention_sweep_interval
                            .saturating_sub(OBSERVED_SWAP_RETENTION_PARTIAL_RETRY_INTERVAL),
                    )
                    .unwrap_or_else(StdInstant::now);
            }
        }
        Some(Ok(Err(error))) => {
            if observed_swap_retention_error_requires_restart(&error) {
                return Err(error)
                    .context("observed swap retention maintenance failed with fatal sqlite I/O");
            }
            warn!(error = %error, "observed swap retention maintenance failed");
        }
        Some(Err(error)) => {
            warn!(error = %error, "observed swap retention maintenance task join failed");
        }
        None => {}
    }
    Ok(())
}
