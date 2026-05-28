use super::*;

pub(crate) fn run_observed_swap_retention_maintenance_once(
    sqlite_path: &str,
    config: ObservedSwapRetentionConfig,
    runtime_health: Option<ObservedSwapRetentionRuntimeHealthHandle>,
) -> Result<ObservedSwapRetentionMaintenanceSummary> {
    let store = SqliteStore::open(Path::new(sqlite_path)).with_context(|| {
        format!("failed to open sqlite db for observed swap retention maintenance: {sqlite_path}")
    })?;
    run_observed_swap_retention_maintenance(&store, config, runtime_health.as_ref())
}

pub(in crate::observed_swap_writer) fn run_observed_swap_retention_maintenance(
    store: &SqliteStore,
    config: ObservedSwapRetentionConfig,
    runtime_health: Option<&ObservedSwapRetentionRuntimeHealthHandle>,
) -> Result<ObservedSwapRetentionMaintenanceSummary> {
    let maintenance_started = Instant::now();
    let now = Utc::now();
    let nominal_cutoff = observed_swap_retention_nominal_cutoff(now, config);
    let effective_cutoff = nominal_cutoff;
    let mut last_sqlite_contention = sqlite_contention_snapshot();
    let mut raw_delete_summary = SqliteBatchedDeleteSummary::default();
    let mut completed_full_sweep = true;
    let mut stop_reason = None;

    loop {
        if let Some(reason) = observed_swap_retention_should_stop(
            runtime_health,
            &mut last_sqlite_contention,
            maintenance_started,
        ) {
            completed_full_sweep = false;
            stop_reason = Some(reason);
            break;
        }
        if raw_delete_summary.batches >= OBSERVED_SWAP_RETENTION_MAX_RAW_DELETE_BATCHES_PER_RUN {
            completed_full_sweep = false;
            stop_reason = Some("raw_batch_budget");
            break;
        }
        let deleted = store
            .delete_observed_swaps_before_batch(
                effective_cutoff,
                OBSERVED_SWAP_RETENTION_DELETE_BATCH_SIZE,
            )
            .with_context(|| {
                format!(
                    "observed swap retention sweep failed retention_days={} nominal_cutoff={} effective_cutoff={}",
                    config.retention_days, nominal_cutoff, effective_cutoff
                )
            })?;
        if deleted == 0 {
            break;
        }
        raw_delete_summary.deleted_rows += deleted;
        raw_delete_summary.batches += 1;
        thread::sleep(OBSERVED_SWAP_RETENTION_INTER_BATCH_PAUSE);
    }

    if let Some(reason) = stop_reason {
        info!(
            stop_reason = reason,
            retention_days = config.retention_days,
            nominal_observed_swap_cutoff = %nominal_cutoff,
            effective_observed_swap_cutoff = %effective_cutoff,
            deleted_observed_swap_rows = raw_delete_summary.deleted_rows,
            observed_swap_delete_batches = raw_delete_summary.batches,
            "observed swap retention paused before exhausting the backlog"
        );
    }

    let checkpoint = if should_checkpoint_after_observed_swap_retention(
        completed_full_sweep,
        stop_reason,
        raw_delete_summary,
    ) {
        run_retention_wal_checkpoint(
            store,
            config,
            nominal_cutoff,
            effective_cutoff,
            raw_delete_summary,
        )?
    } else {
        ObservedSwapRetentionCheckpointSummary {
            mode: "skipped_bounded_run",
            busy: 0,
            log_frames: 0,
            checkpointed_frames: 0,
        }
    };
    Ok(ObservedSwapRetentionMaintenanceSummary {
        nominal_cutoff,
        effective_cutoff,
        raw_deleted_rows: raw_delete_summary.deleted_rows,
        raw_delete_batches: raw_delete_summary.batches,
        completed_full_sweep,
        stop_reason,
        checkpoint,
        duration_ms: elapsed_ms_ceil(maintenance_started.elapsed()),
    })
}

fn should_checkpoint_after_observed_swap_retention(
    completed_full_sweep: bool,
    stop_reason: Option<&'static str>,
    raw_delete_summary: SqliteBatchedDeleteSummary,
) -> bool {
    completed_full_sweep
        || (raw_delete_summary.deleted_rows > 0 && stop_reason != Some("runtime_pressure"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_duration_budget_delete_still_attempts_wal_checkpoint() {
        let summary = SqliteBatchedDeleteSummary {
            deleted_rows: 10_000,
            batches: 1,
        };

        assert!(should_checkpoint_after_observed_swap_retention(
            false,
            Some("duration_budget"),
            summary
        ));
    }

    #[test]
    fn runtime_pressure_partial_delete_skips_wal_checkpoint() {
        let summary = SqliteBatchedDeleteSummary {
            deleted_rows: 10_000,
            batches: 1,
        };

        assert!(!should_checkpoint_after_observed_swap_retention(
            false,
            Some("runtime_pressure"),
            summary
        ));
    }

    #[test]
    fn completed_sweep_keeps_periodic_wal_checkpoint_even_without_deletes() {
        assert!(should_checkpoint_after_observed_swap_retention(
            true,
            None,
            SqliteBatchedDeleteSummary::default()
        ));
    }
}
