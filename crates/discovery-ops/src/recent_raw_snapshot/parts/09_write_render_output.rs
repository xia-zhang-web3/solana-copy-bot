use super::*;

pub(super) fn render_output(
    state: SnapshotState,
    latest_surface_status: LatestSurfaceStatus,
    latest_surface_action: LatestSurfaceAction,
    snapshot_context: &SnapshotContext,
    config_path: &Path,
    source_db_path: &Path,
    snapshot_path: &Path,
    metadata_path: &Path,
    archive_path: Option<&Path>,
    cadence_minutes: Option<u64>,
    retention: Option<usize>,
    manifest: Option<&RecentRawJournalSnapshotManifest>,
    snapshot_summary: Option<&SqliteSnapshotSummary>,
    retryable_reason: Option<String>,
    deferred_reason: Option<String>,
    hard_failure_reason: Option<String>,
    output_context: SnapshotOutputContext,
) -> SnapshotOutput {
    SnapshotOutput {
        event: "discovery_recent_raw_snapshot".to_string(),
        state: state.as_str().to_string(),
        latest_surface_status: latest_surface_status.as_str().to_string(),
        latest_surface_action: latest_surface_action.as_str().to_string(),
        config_path: config_path.display().to_string(),
        source_db_path: source_db_path.display().to_string(),
        snapshot_path: snapshot_path.display().to_string(),
        metadata_path: metadata_path.display().to_string(),
        archive_path: archive_path.map(|path| path.display().to_string()),
        staged_snapshot_path: output_context
            .staged_progress
            .staged_snapshot_path
            .as_ref()
            .map(|path| path.display().to_string()),
        staged_metadata_path: output_context
            .staged_progress
            .staged_metadata_path
            .as_ref()
            .map(|path| path.display().to_string()),
        cadence_minutes,
        retention,
        pruned_snapshot_paths: output_context
            .archive_maintenance
            .pruned_snapshot_paths
            .iter()
            .map(|path| path.display().to_string())
            .collect(),
        cleanup_removed_paths: output_context
            .archive_maintenance
            .cleanup_removed_paths
            .iter()
            .map(|path| path.display().to_string())
            .collect(),
        archive_promoted: output_context.archive_promoted,
        archive_set_count_before: output_context.archive_maintenance.archive_set_count_before,
        archive_set_count_after: output_context.archive_maintenance.archive_set_count_after,
        staged_progress_resumed: output_context.staged_progress.resumed_from_existing_stage,
        staged_seeded_from_latest_surface: output_context
            .staged_progress
            .seeded_from_latest_surface,
        staged_progress_preserved_for_retry: output_context.staged_progress.preserved_for_retry,
        staged_progress_advanced: output_context.staged_progress.advanced_during_attempt,
        staged_completed_batches: output_context.staged_progress.completed_batches,
        staged_source_rows_loaded: output_context.staged_progress.source_rows_loaded,
        staged_rows_processed: output_context.staged_progress.rows_processed_during_attempt,
        staged_rows_inserted: output_context.staged_progress.rows_inserted_during_attempt,
        staged_row_count_before_attempt: output_context.staged_progress.row_count_before_attempt,
        staged_row_count_after_attempt: output_context.staged_progress.row_count_after_attempt,
        staged_covered_through_cursor_before_attempt: output_context
            .staged_progress
            .covered_through_cursor_before_attempt
            .clone(),
        staged_covered_through_cursor_after_attempt: output_context
            .staged_progress
            .covered_through_cursor_after_attempt
            .clone(),
        staged_terminal_phase: output_context
            .staged_progress
            .terminal_phase
            .map(StagedSnapshotTerminalPhase::as_str)
            .map(str::to_string),
        staged_source_read_duration_ms: output_context.staged_progress.source_read_duration_ms,
        staged_write_duration_ms: output_context.staged_progress.staged_write_duration_ms,
        staged_write_rows_per_second: staged_write_rows_per_second(&output_context.staged_progress),
        staged_write_batch_count: output_context.staged_progress.completed_batches,
        staged_write_batch_rows: output_context.staged_progress.write_batch_rows,
        staged_write_sqlite_variable_limit: output_context
            .staged_progress
            .staged_write_sqlite_variable_limit,
        staged_write_statement_params_per_row: output_context
            .staged_progress
            .staged_write_statement_params_per_row,
        staged_write_statement_chunk_row_cap: output_context
            .staged_progress
            .staged_write_statement_chunk_row_cap,
        staged_write_effective_statement_chunk_rows: output_context
            .staged_progress
            .staged_write_effective_statement_chunk_rows,
        staged_write_statement_count: output_context.staged_progress.staged_write_statement_count,
        staged_write_rows_processed: output_context.staged_progress.staged_write_rows_processed,
        staged_write_rows_inserted: output_context.staged_progress.staged_write_rows_inserted,
        staged_write_value_build_duration_ms: output_context
            .staged_progress
            .staged_write_value_build_duration_ms,
        staged_write_prepare_duration_ms: output_context
            .staged_progress
            .staged_write_prepare_duration_ms,
        staged_write_execute_duration_ms: output_context
            .staged_progress
            .staged_write_execute_duration_ms,
        staged_write_state_refresh_duration_ms: output_context
            .staged_progress
            .staged_write_state_refresh_duration_ms,
        staged_write_state_upsert_duration_ms: output_context
            .staged_progress
            .staged_write_state_upsert_duration_ms,
        staged_write_transaction_duration_ms: output_context
            .staged_progress
            .staged_write_transaction_duration_ms,
        staged_write_deadline_exhausted_before_statement: output_context
            .staged_progress
            .staged_write_deadline_exhausted_before_statement,
        staged_write_deadline_exhausted_during_execute: output_context
            .staged_progress
            .staged_write_deadline_exhausted_during_execute,
        source_db_bytes: snapshot_context.source_stats.source_db_bytes,
        source_wal_bytes: snapshot_context.source_stats.source_wal_bytes,
        source_total_bytes: snapshot_context.source_stats.source_total_bytes(),
        source_page_size_bytes: snapshot_context.source_stats.source_page_size_bytes,
        source_page_count: snapshot_context.source_stats.source_page_count,
        snapshot_pages_per_step: snapshot_context.policy.pages_per_step,
        snapshot_pause_between_steps_ms: snapshot_context
            .policy
            .pause_between_steps
            .as_millis()
            .min(u64::MAX as u128) as u64,
        snapshot_max_attempt_duration_ms: snapshot_policy_max_attempt_duration_ms(
            &snapshot_context.policy,
        ),
        attempt_duration_ms: output_context
            .attempt_duration_ms_override
            .or_else(|| snapshot_summary.map(|summary| summary.duration_ms))
            .unwrap_or(0),
        backup_step_count: snapshot_summary
            .map(|summary| summary.backup_step_count)
            .unwrap_or(0),
        backup_retry_count: snapshot_summary
            .map(|summary| summary.backup_retry_count)
            .unwrap_or(0),
        busy_retry_count: snapshot_summary
            .map(|summary| summary.busy_retry_count)
            .unwrap_or(0),
        locked_retry_count: snapshot_summary
            .map(|summary| summary.locked_retry_count)
            .unwrap_or(0),
        backup_total_page_count: snapshot_summary.map(|summary| summary.total_page_count),
        backup_remaining_page_count: snapshot_summary.map(|summary| summary.remaining_page_count),
        backup_copied_page_count: snapshot_summary.map(|summary| summary.copied_page_count),
        terminal_reason: output_context
            .terminal_reason_override
            .or_else(|| snapshot_terminal_reason(state, snapshot_summary)),
        retryable_reason,
        deferred_reason,
        hard_failure_reason,
        created_at: manifest.map(|manifest| manifest.created_at),
        row_count: manifest.map(|manifest| manifest.row_count),
        covered_since: manifest.and_then(|manifest| manifest.covered_since),
        covered_through_cursor: manifest
            .and_then(|manifest| manifest.covered_through_cursor.clone()),
        last_batch_completed_at: manifest.and_then(|manifest| manifest.last_batch_completed_at),
        snapshot_bytes: manifest.map(|manifest| manifest.snapshot_bytes),
    }
}
