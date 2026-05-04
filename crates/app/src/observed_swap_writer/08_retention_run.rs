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

fn run_observed_swap_retention_maintenance(
    store: &SqliteStore,
    config: ObservedSwapRetentionConfig,
    runtime_health: Option<&ObservedSwapRetentionRuntimeHealthHandle>,
) -> Result<ObservedSwapRetentionMaintenanceSummary> {
    let maintenance_started = Instant::now();
    let now = Utc::now();
    let nominal_cutoff = observed_swap_retention_nominal_cutoff(now, config);
    let effective_cutoff = resolve_observed_swap_retention_effective_cutoff(config, now, |now| {
        store.load_discovery_scoring_backfill_protected_since(now)
    })?;
    let aggregate_cutoff = if config.aggregate_writes_enabled {
        Some(now - ChronoDuration::days(config.aggregate_retention_days.max(1) as i64))
    } else {
        None
    };
    let mut last_sqlite_contention = sqlite_contention_snapshot();
    let mut raw_delete_summary = SqliteBatchedDeleteSummary::default();
    let mut scoring_delete_summary = SqliteBatchedDeleteSummary::default();
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

    if completed_full_sweep {
        if let Some(aggregate_cutoff) = aggregate_cutoff {
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
                if scoring_delete_summary.batches
                    >= OBSERVED_SWAP_RETENTION_MAX_SCORING_DELETE_BATCHES_PER_RUN
                {
                    completed_full_sweep = false;
                    stop_reason = Some("scoring_batch_budget");
                    break;
                }
                let deleted = store
                    .prune_discovery_scoring_before_batch(
                        aggregate_cutoff,
                        DISCOVERY_SCORING_RETENTION_DELETE_BATCH_SIZE,
                    )
                    .with_context(|| {
                        format!(
                            "discovery scoring retention sweep failed aggregate_retention_days={} aggregate_cutoff={}",
                            config.aggregate_retention_days, aggregate_cutoff
                        )
                    })?;
                if deleted == 0 {
                    break;
                }
                scoring_delete_summary.deleted_rows += deleted;
                scoring_delete_summary.batches += 1;
                thread::sleep(OBSERVED_SWAP_RETENTION_INTER_BATCH_PAUSE);
            }
        }
    }

    if let Some(reason) = stop_reason {
        warn!(
            stop_reason = reason,
            retention_days = config.retention_days,
            aggregate_retention_days = config.aggregate_retention_days,
            nominal_observed_swap_cutoff = %nominal_cutoff,
            effective_observed_swap_cutoff = %effective_cutoff,
            aggregate_scoring_cutoff = ?aggregate_cutoff,
            deleted_observed_swap_rows = raw_delete_summary.deleted_rows,
            observed_swap_delete_batches = raw_delete_summary.batches,
            deleted_scoring_rows = scoring_delete_summary.deleted_rows,
            discovery_scoring_delete_batches = scoring_delete_summary.batches,
            "observed swap retention paused before exhausting the backlog"
        );
    }

    let checkpoint = if completed_full_sweep {
        run_retention_wal_checkpoint(
            store,
            config,
            nominal_cutoff,
            effective_cutoff,
            raw_delete_summary,
            scoring_delete_summary,
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
        aggregate_cutoff,
        raw_deleted_rows: raw_delete_summary.deleted_rows,
        raw_delete_batches: raw_delete_summary.batches,
        scoring_deleted_rows: scoring_delete_summary.deleted_rows,
        scoring_delete_batches: scoring_delete_summary.batches,
        completed_full_sweep,
        stop_reason,
        checkpoint,
        duration_ms: elapsed_ms_ceil(maintenance_started.elapsed()),
    })
}
