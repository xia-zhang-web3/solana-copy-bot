use crate::*;

#[cfg(test)]
pub(crate) fn startup_sqlite_wal_checkpoint_error_requires_abort(
    primary_error: Option<&anyhow::Error>,
    fallback_error: Option<&anyhow::Error>,
) -> bool {
    primary_error.is_some_and(is_fatal_sqlite_anyhow_error)
        || fallback_error.is_some_and(is_fatal_sqlite_anyhow_error)
}

pub(crate) fn runtime_sqlite_write_error_requires_restart(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

pub(crate) fn history_retention_error_requires_restart(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

pub(crate) fn observed_swap_retention_error_requires_restart(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SqliteMaintenanceTask {
    HistoryRetention,
    ObservedSwapRetention,
}

impl SqliteMaintenanceTask {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::HistoryRetention => "history_retention",
            Self::ObservedSwapRetention => "observed_swap_retention",
        }
    }
}

pub(crate) fn sqlite_maintenance_block_reason(
    _task: SqliteMaintenanceTask,
    app_started_at: StdInstant,
    now: StdInstant,
    observed_swap_writer_snapshot: &ObservedSwapWriterSnapshot,
    sqlite_contention_previous: SqliteContentionSnapshot,
    sqlite_contention_current: SqliteContentionSnapshot,
    ingestion_runtime_snapshot: Option<IngestionRuntimeSnapshot>,
) -> Option<String> {
    let startup_grace = Duration::from_secs(
        OBSERVED_SWAP_RETENTION_STARTUP_GRACE_INTERVAL
            .as_secs()
            .max(1),
    );
    let elapsed_since_start = now.saturating_duration_since(app_started_at);
    if elapsed_since_start < startup_grace {
        return Some(format!(
            "startup_grace_remaining_ms={}",
            startup_grace
                .saturating_sub(elapsed_since_start)
                .as_millis()
                .min(u128::from(u64::MAX))
        ));
    }

    if observed_swap_writer_snapshot.pending_requests > 0 {
        return Some(format!(
            "writer_pending_requests={}",
            observed_swap_writer_snapshot.pending_requests
        ));
    }

    if observed_swap_writer_snapshot.journal_queue_depth_batches > 0 {
        return Some(format!(
            "journal_queue_depth_batches={}",
            observed_swap_writer_snapshot.journal_queue_depth_batches
        ));
    }
    if observed_swap_writer_snapshot.journal_overflow_depth_batches > 0 {
        return Some(format!(
            "journal_overflow_depth_batches={}",
            observed_swap_writer_snapshot.journal_overflow_depth_batches
        ));
    }

    let sqlite_write_retry_delta = sqlite_contention_current
        .write_retry_total
        .saturating_sub(sqlite_contention_previous.write_retry_total);
    let sqlite_busy_error_delta = sqlite_contention_current
        .busy_error_total
        .saturating_sub(sqlite_contention_previous.busy_error_total);
    if sqlite_write_retry_delta > 0 || sqlite_busy_error_delta > 0 {
        return Some(format!(
            "sqlite_contention_delta write_retry_delta={} busy_error_delta={}",
            sqlite_write_retry_delta, sqlite_busy_error_delta
        ));
    }

    if let Some(snapshot) = ingestion_runtime_snapshot {
        if snapshot.yellowstone_output_queue_capacity > 0
            && snapshot.yellowstone_output_queue_fill_ratio
                >= SQLITE_MAINTENANCE_MAX_YELLOWSTONE_OUTPUT_QUEUE_FILL_RATIO
        {
            return Some(format!(
                "yellowstone_output_queue_fill_ratio={:.4} depth={} capacity={} oldest_age_ms={}",
                snapshot.yellowstone_output_queue_fill_ratio,
                snapshot.yellowstone_output_queue_depth,
                snapshot.yellowstone_output_queue_capacity,
                snapshot.yellowstone_output_oldest_age_ms
            ));
        }
    }

    None
}

pub(crate) fn sqlite_maintenance_block_reason_key(reason: &str) -> &'static str {
    if reason.starts_with("startup_grace_remaining_ms=") {
        "startup_grace_remaining_ms"
    } else if reason.starts_with("writer_pending_requests=") {
        "writer_pending_requests"
    } else if reason.starts_with("journal_queue_depth_batches=") {
        "journal_queue_depth_batches"
    } else if reason.starts_with("journal_overflow_depth_batches=") {
        "journal_overflow_depth_batches"
    } else if reason.starts_with("sqlite_contention_delta ") {
        "sqlite_contention_delta"
    } else if reason.starts_with("yellowstone_output_queue_fill_ratio=") {
        "yellowstone_output_queue_fill_ratio"
    } else {
        "other"
    }
}

pub(crate) fn stale_lot_cleanup_error_requires_restart(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

pub(crate) fn alert_delivery_error_requires_restart(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

#[cfg(test)]
pub(crate) fn discovery_task_error_requires_restart(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}
