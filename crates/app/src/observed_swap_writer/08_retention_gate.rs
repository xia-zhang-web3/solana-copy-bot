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
