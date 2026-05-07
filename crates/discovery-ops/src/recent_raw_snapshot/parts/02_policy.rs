use super::*;

pub(crate) fn summary_reason(summary: &SqliteSnapshotSummary) -> String {
    summary
        .retry_exhausted_reason
        .map(|reason| reason.as_str().to_string())
        .unwrap_or_else(|| "busy".to_string())
}

pub(crate) fn deferred_summary_reason(summary: &SqliteSnapshotSummary) -> String {
    summary
        .deferred_reason
        .map(|reason| reason.as_str().to_string())
        .unwrap_or_else(|| "deferred".to_string())
}

pub(crate) fn staged_attempt_budget_reason(progress: &StagedSnapshotProgress) -> String {
    progress
        .terminal_phase
        .map(StagedSnapshotTerminalPhase::budget_reason)
        .unwrap_or("attempt_duration_budget_exhausted")
        .to_string()
}

pub(crate) fn snapshot_terminal_reason(
    state: SnapshotState,
    summary: Option<&SqliteSnapshotSummary>,
) -> Option<String> {
    match summary {
        Some(summary) => {
            if let Some(reason) = summary.deferred_reason {
                Some(reason.as_str().to_string())
            } else if let Some(reason) = summary.retry_exhausted_reason {
                Some(reason.as_str().to_string())
            } else if state == SnapshotState::Written {
                Some("written".to_string())
            } else {
                None
            }
        }
        None if state == SnapshotState::Written => Some("written".to_string()),
        _ => None,
    }
}

pub(crate) fn snapshot_policy_max_attempt_duration_ms(
    policy: &SqliteSnapshotPolicy,
) -> Option<u64> {
    policy
        .max_attempt_duration
        .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
}

pub(crate) fn state_exit_code(output: &SnapshotOutput) -> i32 {
    match output.state.as_str() {
        "written" => SnapshotState::Written.exit_code(),
        "skipped_not_due" => SnapshotState::SkippedNotDue.exit_code(),
        "self_healed_latest_surface" => SnapshotState::SelfHealedLatestSurface.exit_code(),
        "retryable_busy" => SnapshotState::RetryableBusy.exit_code(),
        "deferred" => SnapshotState::Deferred.exit_code(),
        "hard_failure" => SnapshotState::HardFailure.exit_code(),
        _ => 1,
    }
}

pub(crate) fn scheduled_duration_budget_contract(
    latest_surface_status: LatestSurfaceStatus,
    reason: &str,
) -> (LatestSurfaceAction, Option<String>) {
    if latest_surface_status == LatestSurfaceStatus::Healthy {
        (
            LatestSurfaceAction::DeferredDueToAttemptBudget,
            Some(format!("healthy_latest_surface_retained_after_{reason}")),
        )
    } else {
        (
            LatestSurfaceAction::UnchangedDueToAttemptBudget,
            Some(reason.to_string()),
        )
    }
}

pub(crate) fn snapshot_source_wal_path(source_db_path: &Path) -> PathBuf {
    let mut wal_path = source_db_path.as_os_str().to_os_string();
    wal_path.push("-wal");
    PathBuf::from(wal_path)
}

pub(crate) fn snapshot_source_stats(
    source_db_path: &Path,
    source_store: &SqliteStore,
) -> Result<SnapshotSourceStats> {
    let source_metrics: SqliteSnapshotSourceMetrics = source_store.snapshot_source_metrics()?;
    let source_db_bytes = fs::metadata(source_db_path)
        .with_context(|| format!("failed stat {}", source_db_path.display()))?
        .len();
    let wal_path = snapshot_source_wal_path(source_db_path);
    let source_wal_bytes = match fs::metadata(&wal_path) {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => {
            return Err(error).with_context(|| format!("failed stat {}", wal_path.display()));
        }
    };
    Ok(SnapshotSourceStats {
        source_db_bytes,
        source_wal_bytes,
        source_page_size_bytes: source_metrics.page_size_bytes,
        source_page_count: source_metrics.page_count,
    })
}

pub(crate) fn adaptive_snapshot_policy(source_stats: &SnapshotSourceStats) -> SqliteSnapshotPolicy {
    let mut policy = SqliteSnapshotPolicy::default();
    let total_bytes = source_stats.source_total_bytes().max(
        (source_stats.source_page_size_bytes as u64)
            .saturating_mul(source_stats.source_page_count as u64),
    );
    let (target_steps, pause_ms, max_attempt_duration_ms) =
        if total_bytes >= SNAPSHOT_HUGE_SOURCE_TOTAL_BYTES {
            (
                SNAPSHOT_HUGE_TARGET_STEPS,
                SNAPSHOT_LARGE_PAUSE_BETWEEN_STEPS_MS,
                SNAPSHOT_HUGE_MAX_ATTEMPT_DURATION_MS,
            )
        } else if total_bytes >= SNAPSHOT_LARGE_SOURCE_TOTAL_BYTES {
            (
                SNAPSHOT_LARGE_TARGET_STEPS,
                SNAPSHOT_LARGE_PAUSE_BETWEEN_STEPS_MS,
                SNAPSHOT_LARGE_MAX_ATTEMPT_DURATION_MS,
            )
        } else if total_bytes >= SNAPSHOT_SMALL_SOURCE_TOTAL_BYTES {
            (
                SNAPSHOT_MEDIUM_TARGET_STEPS,
                SNAPSHOT_MEDIUM_PAUSE_BETWEEN_STEPS_MS,
                SNAPSHOT_MEDIUM_MAX_ATTEMPT_DURATION_MS,
            )
        } else {
            (
                SNAPSHOT_SMALL_TARGET_STEPS,
                SNAPSHOT_SMALL_PAUSE_BETWEEN_STEPS_MS,
                SNAPSHOT_SMALL_MAX_ATTEMPT_DURATION_MS,
            )
        };
    let page_count = source_stats.source_page_count.max(1);
    let pages_per_step = page_count
        .div_ceil(target_steps)
        .clamp(SNAPSHOT_MIN_PAGES_PER_STEP, SNAPSHOT_MAX_PAGES_PER_STEP)
        as i32;
    policy.pages_per_step = pages_per_step;
    policy.pause_between_steps = StdDuration::from_millis(pause_ms);
    policy.max_attempt_duration = Some(StdDuration::from_millis(max_attempt_duration_ms));
    policy
}

pub(crate) fn snapshot_context(
    source_db_path: &Path,
    source_store: &SqliteStore,
) -> Result<SnapshotContext> {
    let source_stats = snapshot_source_stats(source_db_path, source_store)?;
    Ok(SnapshotContext {
        policy: adaptive_snapshot_policy(&source_stats),
        source_stats,
    })
}

pub(crate) fn discovery_runtime_cursor_cmp(
    left: &DiscoveryRuntimeCursor,
    right: &DiscoveryRuntimeCursor,
) -> std::cmp::Ordering {
    left.ts_utc
        .cmp(&right.ts_utc)
        .then_with(|| left.slot.cmp(&right.slot))
        .then_with(|| left.signature.cmp(&right.signature))
}

pub(crate) fn snapshot_resume_batch_size(policy: &SqliteSnapshotPolicy) -> usize {
    (policy.pages_per_step.max(1) as usize)
        .saturating_mul(SNAPSHOT_RESUME_ROW_BATCH_MULTIPLIER)
        .min(SNAPSHOT_RESUME_ROW_BATCH_MAX_ROWS)
        .max(1)
}

pub(crate) fn staged_write_rows_per_second(progress: &StagedSnapshotProgress) -> Option<f64> {
    if progress.rows_inserted_during_attempt == 0 {
        return None;
    }
    let duration_ms = progress.staged_write_duration_ms.max(1);
    Some(progress.rows_inserted_during_attempt as f64 * 1_000.0 / duration_ms as f64)
}

pub(crate) fn record_staged_write_summary(
    progress: &mut StagedSnapshotProgress,
    summary: &RecentRawJournalWriteSummary,
) {
    if summary.recent_raw_bulk_sqlite_variable_limit > 0 {
        progress.staged_write_sqlite_variable_limit = summary.recent_raw_bulk_sqlite_variable_limit;
    }
    if summary.recent_raw_bulk_statement_params_per_row > 0 {
        progress.staged_write_statement_params_per_row =
            summary.recent_raw_bulk_statement_params_per_row;
    }
    if summary.recent_raw_bulk_statement_chunk_row_cap > 0 {
        progress.staged_write_statement_chunk_row_cap =
            summary.recent_raw_bulk_statement_chunk_row_cap;
    }
    if summary.recent_raw_bulk_effective_statement_chunk_rows > 0 {
        progress.staged_write_effective_statement_chunk_rows =
            summary.recent_raw_bulk_effective_statement_chunk_rows;
    }
    progress.staged_write_statement_count = progress
        .staged_write_statement_count
        .saturating_add(summary.recent_raw_bulk_statement_count);
    progress.staged_write_rows_processed = progress
        .staged_write_rows_processed
        .saturating_add(summary.recent_raw_bulk_rows_processed);
    progress.staged_write_rows_inserted = progress
        .staged_write_rows_inserted
        .saturating_add(summary.recent_raw_bulk_rows_inserted);
    progress.staged_write_value_build_duration_ms = progress
        .staged_write_value_build_duration_ms
        .saturating_add(summary.recent_raw_bulk_value_build_duration_ms);
    progress.staged_write_prepare_duration_ms = progress
        .staged_write_prepare_duration_ms
        .saturating_add(summary.recent_raw_bulk_prepare_duration_ms);
    progress.staged_write_execute_duration_ms = progress
        .staged_write_execute_duration_ms
        .saturating_add(summary.recent_raw_bulk_execute_duration_ms);
    progress.staged_write_state_refresh_duration_ms = progress
        .staged_write_state_refresh_duration_ms
        .saturating_add(summary.recent_raw_bulk_state_refresh_duration_ms);
    progress.staged_write_state_upsert_duration_ms = progress
        .staged_write_state_upsert_duration_ms
        .saturating_add(summary.recent_raw_bulk_state_upsert_duration_ms);
    progress.staged_write_transaction_duration_ms = progress
        .staged_write_transaction_duration_ms
        .saturating_add(summary.recent_raw_bulk_transaction_duration_ms);
    progress.staged_write_deadline_exhausted_before_statement |=
        summary.recent_raw_bulk_deadline_exhausted_before_statement;
    progress.staged_write_deadline_exhausted_during_execute |=
        summary.recent_raw_bulk_deadline_exhausted_during_execute;
}
