use super::*;

pub(super) fn advance_staged_snapshot(
    source_db_path: &Path,
    source_store: &SqliteStore,
    staged_store: &SqliteStore,
    staged_snapshot_path: &Path,
    staged_created_at: DateTime<Utc>,
    source_state: &RecentRawJournalStateRow,
    before_state: &RecentRawJournalStateRow,
    mut progress: StagedSnapshotProgress,
    now: DateTime<Utc>,
    started: Instant,
    deadline: Option<Instant>,
    batch_limit: usize,
) -> Result<(RecentRawJournalStateRow, StagedSnapshotProgress, bool), StagedSnapshotAttemptResult>
{
    let mut completed_batches = 0usize;
    let mut current_state = before_state.clone();
    let mut budget_exhausted = false;

    while source_state.row_count > current_state.row_count {
        let deadline =
            deadline.unwrap_or_else(|| Instant::now() + StdDuration::from_secs(24 * 60 * 60));
        progress.terminal_phase = Some(StagedSnapshotTerminalPhase::SourceRead);
        if Instant::now() >= deadline {
            budget_exhausted = true;
            break;
        }

        let source_read_started = Instant::now();
        let mut batch = Vec::with_capacity(batch_limit);
        let page = if let Some(cursor) = current_state.covered_through_cursor.as_ref() {
            source_store.for_each_observed_swap_after_cursor_with_budget(
                cursor.ts_utc,
                cursor.slot,
                &cursor.signature,
                batch_limit,
                deadline,
                |swap| {
                    batch.push(swap);
                    Ok(())
                },
            )
        } else if let Some(covered_since) = source_state.covered_since {
            source_store.for_each_observed_swap_since_with_budget(
                covered_since,
                batch_limit,
                deadline,
                |swap| {
                    batch.push(swap);
                    Ok(())
                },
            )
        } else {
            break;
        };
        let page = match page {
            Ok(page) => page,
            Err(error) => {
                let attempt_duration_ms =
                    started.elapsed().as_millis().min(u64::MAX as u128) as u64;
                let manifest = staged_manifest_for_state(
                    source_db_path,
                    staged_snapshot_path,
                    staged_created_at,
                    &current_state,
                )
                .ok();
                return Err(StagedSnapshotAttemptResult::HardFailure {
                    manifest,
                    progress,
                    attempt_duration_ms,
                    reason: format!(
                        "failed loading bounded recent_raw source rows into staged snapshot {}: {error}",
                        staged_snapshot_path.display()
                    ),
                });
            }
        };
        progress.source_read_duration_ms = progress.source_read_duration_ms.saturating_add(
            source_read_started
                .elapsed()
                .as_millis()
                .min(u64::MAX as u128) as u64,
        );

        if batch.is_empty() {
            if page.time_budget_exhausted {
                budget_exhausted = true;
            }
            break;
        }
        progress.source_rows_loaded = progress.source_rows_loaded.saturating_add(batch.len());

        progress.terminal_phase = Some(StagedSnapshotTerminalPhase::StagedWrite);
        let staged_write_started = Instant::now();
        let staged_write_failure = invoke_staged_write_failure_hook(completed_batches);
        let force_budget_context = staged_write_failure
            .as_ref()
            .is_some_and(|failure| failure.force_budget_context);
        let write_result = match staged_write_failure {
            Some(failure) => Err(failure.error),
            None => staged_store
                .insert_recent_raw_journal_batch_bulk_with_deadline(&batch, now, deadline),
        };
        progress.staged_write_duration_ms = progress.staged_write_duration_ms.saturating_add(
            staged_write_started
                .elapsed()
                .as_millis()
                .min(u64::MAX as u128) as u64,
        );
        let (write_summary, write_budget_exhausted) = match write_result {
            Ok(result) => result,
            Err(error)
                if recent_raw_staged_write_error_is_budget_exhaustion(
                    &error,
                    force_budget_context || Instant::now() >= deadline,
                ) =>
            {
                budget_exhausted = true;
                break;
            }
            Err(error) => {
                let attempt_duration_ms =
                    started.elapsed().as_millis().min(u64::MAX as u128) as u64;
                let manifest = staged_manifest_for_state(
                    source_db_path,
                    staged_snapshot_path,
                    staged_created_at,
                    &current_state,
                )
                .ok();
                return Err(StagedSnapshotAttemptResult::HardFailure {
                    manifest,
                    progress,
                    attempt_duration_ms,
                    reason: format!(
                        "failed persisting bounded recent_raw batch into staged snapshot {}: {error}",
                        staged_snapshot_path.display()
                    ),
                });
            }
        };

        current_state = match staged_store.recent_raw_journal_state_cached() {
            Ok(state) => state,
            Err(error) => {
                let attempt_duration_ms =
                    started.elapsed().as_millis().min(u64::MAX as u128) as u64;
                let manifest = staged_manifest_for_state(
                    source_db_path,
                    staged_snapshot_path,
                    staged_created_at,
                    before_state,
                )
                .ok();
                return Err(StagedSnapshotAttemptResult::HardFailure {
                    manifest,
                    progress,
                    attempt_duration_ms,
                    reason: format!(
                        "failed refreshing staged recent_raw snapshot state from {}: {error}",
                        staged_snapshot_path.display()
                    ),
                });
            }
        };
        completed_batches = completed_batches.saturating_add(1);
        progress.completed_batches = completed_batches;
        record_staged_write_summary(&mut progress, &write_summary);
        progress.rows_processed_during_attempt = progress
            .rows_processed_during_attempt
            .saturating_add(write_summary.batch_rows);
        progress.rows_inserted_during_attempt = progress
            .rows_inserted_during_attempt
            .saturating_add(write_summary.inserted_rows);
        progress.advanced_during_attempt |= current_state.row_count > before_state.row_count
            || current_state.covered_through_cursor != before_state.covered_through_cursor;

        if write_budget_exhausted {
            budget_exhausted = true;
            break;
        }
        if resumable_snapshot_progress_hook_requests_budget_exhaustion(
            completed_batches,
            current_state.row_count,
        ) {
            budget_exhausted = true;
            break;
        }
        if page.time_budget_exhausted || Instant::now() >= deadline {
            budget_exhausted = true;
            break;
        }
        if page.rows_seen < batch_limit {
            break;
        }
    }

    Ok((current_state, progress, budget_exhausted))
}
