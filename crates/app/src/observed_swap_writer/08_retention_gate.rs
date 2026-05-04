fn observed_swap_retention_should_stop(
    runtime_health: Option<&ObservedSwapRetentionRuntimeHealthHandle>,
    last_sqlite_contention: &mut SqliteContentionSnapshot,
    maintenance_started: Instant,
) -> Option<&'static str> {
    if maintenance_started.elapsed() >= OBSERVED_SWAP_RETENTION_MAX_DURATION_PER_RUN {
        return Some("duration_budget");
    }
    let Some(runtime_health) = runtime_health else {
        *last_sqlite_contention = sqlite_contention_snapshot();
        return None;
    };
    let writer_snapshot = runtime_health.writer_snapshot();
    if writer_snapshot.pending_requests > 0 {
        return Some("runtime_pressure");
    }
    if writer_snapshot.aggregate_queue_depth_batches > 0 {
        return Some("runtime_pressure");
    }
    if writer_snapshot.aggregate_overflow_depth_batches > 0 {
        return Some("runtime_pressure");
    }
    if writer_snapshot.journal_queue_depth_batches > 0 {
        return Some("runtime_pressure");
    }
    let sqlite_contention_current = sqlite_contention_snapshot();
    let sqlite_write_retry_delta = sqlite_contention_current
        .write_retry_total
        .saturating_sub(last_sqlite_contention.write_retry_total);
    let sqlite_busy_error_delta = sqlite_contention_current
        .busy_error_total
        .saturating_sub(last_sqlite_contention.busy_error_total);
    *last_sqlite_contention = sqlite_contention_current;
    if sqlite_write_retry_delta > 0 || sqlite_busy_error_delta > 0 {
        return Some("runtime_pressure");
    }
    if runtime_health.ingestion_snapshot().is_some_and(|snapshot| {
        snapshot.yellowstone_output_queue_capacity > 0
            && snapshot.yellowstone_output_queue_fill_ratio
                >= SQLITE_MAINTENANCE_MAX_YELLOWSTONE_OUTPUT_QUEUE_FILL_RATIO
    }) {
        return Some("runtime_pressure");
    }
    None
}

fn observed_swap_retention_nominal_cutoff(
    now: chrono::DateTime<Utc>,
    config: ObservedSwapRetentionConfig,
) -> chrono::DateTime<Utc> {
    now - ChronoDuration::days(config.retention_days.max(1) as i64)
}

fn resolve_observed_swap_retention_effective_cutoff<F>(
    config: ObservedSwapRetentionConfig,
    now: chrono::DateTime<Utc>,
    load_protected_since: F,
) -> Result<chrono::DateTime<Utc>>
where
    F: FnOnce(chrono::DateTime<Utc>) -> Result<Option<chrono::DateTime<Utc>>>,
{
    let nominal_cutoff = observed_swap_retention_nominal_cutoff(now, config);
    match load_protected_since(now) {
        Ok(Some(protected_since)) => Ok(nominal_cutoff.min(protected_since)),
        Ok(None) => Ok(nominal_cutoff),
        Err(error) => {
            if observed_swap_retention_protection_load_error_requires_abort(&error) {
                return Err(error).context(
                    "observed swap retention source protection lookup failed with fatal sqlite I/O",
                );
            }
            warn!(
                error = %error,
                retention_days = config.retention_days,
                "failed loading discovery scoring backfill source protection; using nominal observed swap retention cutoff"
            );
            Ok(nominal_cutoff)
        }
    }
}
