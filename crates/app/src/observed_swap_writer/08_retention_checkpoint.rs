fn percentile_from_deque(values: &VecDeque<u64>, q: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.iter().copied().collect::<Vec<_>>();
    sorted.sort_unstable();
    let idx = ((sorted.len() - 1) as f64 * q.clamp(0.0, 1.0)).round() as usize;
    sorted[idx]
}

fn elapsed_ms_ceil(duration: StdDuration) -> u64 {
    let micros = duration.as_micros();
    if micros == 0 {
        0
    } else {
        micros.div_ceil(1000).min(u128::from(u64::MAX)) as u64
    }
}

fn run_retention_wal_checkpoint(
    store: &SqliteStore,
    config: ObservedSwapRetentionConfig,
    nominal_cutoff: chrono::DateTime<Utc>,
    effective_cutoff: chrono::DateTime<Utc>,
    raw_delete_summary: SqliteBatchedDeleteSummary,
) -> Result<ObservedSwapRetentionCheckpointSummary> {
    match store.checkpoint_wal_passive() {
        Ok((busy, log_frames, checkpointed_frames)) => {
            let checkpoint_summary = ObservedSwapRetentionCheckpointSummary {
                mode: "passive_runtime",
                busy,
                log_frames,
                checkpointed_frames,
            };
            if raw_delete_summary.deleted_rows > 0 {
                info!(
                    retention_days = config.retention_days,
                    nominal_observed_swap_cutoff = %nominal_cutoff,
                    effective_observed_swap_cutoff = %effective_cutoff,
                    deleted_observed_swap_rows = raw_delete_summary.deleted_rows,
                    observed_swap_delete_batches = raw_delete_summary.batches,
                    wal_checkpoint_mode = checkpoint_summary.mode,
                    wal_checkpoint_busy = busy,
                    wal_log_frames = log_frames,
                    wal_checkpointed_frames = checkpointed_frames,
                    "observed swap retention sweep attempted runtime passive wal checkpoint"
                );
            } else {
                info!(
                    retention_days = config.retention_days,
                    nominal_observed_swap_cutoff = %nominal_cutoff,
                    effective_observed_swap_cutoff = %effective_cutoff,
                    deleted_observed_swap_rows = 0,
                    wal_checkpoint_mode = checkpoint_summary.mode,
                    wal_checkpoint_busy = busy,
                    wal_log_frames = log_frames,
                    wal_checkpointed_frames = checkpointed_frames,
                    "observed swap retention sweep attempted periodic passive wal checkpoint"
                );
            }
            Ok(checkpoint_summary)
        }
        Err(error) => {
            if observed_swap_retention_checkpoint_error_requires_abort(None, Some(&error)) {
                return Err(error).context(
                    "observed swap retention periodic wal checkpoint failed with fatal sqlite I/O",
                );
            }
            warn!(
                error = %error,
                retention_days = config.retention_days,
                nominal_observed_swap_cutoff = %nominal_cutoff,
                effective_observed_swap_cutoff = %effective_cutoff,
                deleted_observed_swap_rows = raw_delete_summary.deleted_rows,
                observed_swap_delete_batches = raw_delete_summary.batches,
                wal_checkpoint_mode = "passive_runtime_failed",
                "observed swap retention sweep passive wal checkpoint failed"
            );
            Ok(ObservedSwapRetentionCheckpointSummary {
                mode: "passive_runtime_failed",
                busy: 0,
                log_frames: 0,
                checkpointed_frames: 0,
            })
        }
    }
}
