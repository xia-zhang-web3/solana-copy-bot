use super::*;

pub(in crate::observed_swap_writer) fn write_recent_raw_journal_batch_with_deadline(
    store: &SqliteStore,
    config: &ObservedSwapRecentRawJournalConfig,
    telemetry: &ObservedSwapWriterTelemetry,
    inserted_swaps: &[SwapEvent],
    request_batches: usize,
    coalesce_elapsed_ms: u64,
    coalesce_limit_rows: usize,
    completed_at: DateTime<Utc>,
) -> Result<()> {
    let batch_phase_started = Instant::now();
    let batch_rows = inserted_swaps.len();
    log_recent_raw_journal_phase(
        RECENT_RAW_JOURNAL_PHASE_WRITE_START,
        None,
        batch_rows,
        request_batches,
        coalesce_elapsed_ms,
        coalesce_limit_rows,
        0,
        telemetry,
    );
    let write_started = Instant::now();
    let write_result = write_recent_raw_journal_batch_with_deadline_attempts(
        telemetry,
        inserted_swaps,
        || Instant::now() + OBSERVED_SWAP_RECENT_RAW_JOURNAL_WRITE_DEADLINE,
        |suffix, deadline| {
            store.insert_recent_raw_journal_batch_bulk_with_deadline(suffix, completed_at, deadline)
        },
    );
    log_recent_raw_journal_phase(
        RECENT_RAW_JOURNAL_PHASE_WRITE_END,
        None,
        batch_rows,
        request_batches,
        coalesce_elapsed_ms,
        coalesce_limit_rows,
        elapsed_ms_ceil(write_started.elapsed()),
        telemetry,
    );
    write_result?;

    let prune_check_started = Instant::now();
    log_recent_raw_journal_phase(
        RECENT_RAW_JOURNAL_PHASE_PRUNE_CHECK_START,
        None,
        batch_rows,
        request_batches,
        coalesce_elapsed_ms,
        coalesce_limit_rows,
        0,
        telemetry,
    );
    let prune_skip_reason = if config.skip_prune_while_backlogged {
        Some(RECENT_RAW_JOURNAL_HOT_WRITER_PRUNE_DEFERRED)
    } else {
        recent_raw_journal_prune_backlog_skip_reason(config, telemetry)
    };
    let prune_due = if prune_skip_reason.is_some() {
        false
    } else {
        recent_raw_journal_prune_due(store, config, telemetry, completed_at)?
    };
    log_recent_raw_journal_phase(
        RECENT_RAW_JOURNAL_PHASE_PRUNE_CHECK_END,
        prune_skip_reason,
        batch_rows,
        request_batches,
        coalesce_elapsed_ms,
        coalesce_limit_rows,
        elapsed_ms_ceil(prune_check_started.elapsed()),
        telemetry,
    );
    if prune_due {
        let prune_started = Instant::now();
        log_recent_raw_journal_phase(
            RECENT_RAW_JOURNAL_PHASE_PRUNE_START,
            None,
            batch_rows,
            request_batches,
            coalesce_elapsed_ms,
            coalesce_limit_rows,
            0,
            telemetry,
        );
        let _summary =
            prune_recent_raw_journal_with_budget(store, config.retention_days, completed_at)?;
        log_recent_raw_journal_phase(
            RECENT_RAW_JOURNAL_PHASE_PRUNE_END,
            None,
            batch_rows,
            request_batches,
            coalesce_elapsed_ms,
            coalesce_limit_rows,
            elapsed_ms_ceil(prune_started.elapsed()),
            telemetry,
        );
    } else {
        log_recent_raw_journal_phase(
            RECENT_RAW_JOURNAL_PHASE_PRUNE_SKIPPED,
            prune_skip_reason,
            batch_rows,
            request_batches,
            coalesce_elapsed_ms,
            coalesce_limit_rows,
            elapsed_ms_ceil(prune_check_started.elapsed()),
            telemetry,
        );
    }
    log_recent_raw_journal_phase(
        RECENT_RAW_JOURNAL_PHASE_BATCH_DONE,
        None,
        batch_rows,
        request_batches,
        coalesce_elapsed_ms,
        coalesce_limit_rows,
        elapsed_ms_ceil(batch_phase_started.elapsed()),
        telemetry,
    );
    Ok(())
}

pub(in crate::observed_swap_writer) fn write_recent_raw_journal_batch_with_deadline_attempts<
    DeadlineFn,
    WriteFn,
>(
    telemetry: &ObservedSwapWriterTelemetry,
    inserted_swaps: &[SwapEvent],
    mut deadline_for_attempt: DeadlineFn,
    mut write_attempt: WriteFn,
) -> Result<()>
where
    DeadlineFn: FnMut() -> Instant,
    WriteFn: FnMut(&[SwapEvent], Instant) -> Result<(RecentRawJournalWriteSummary, bool)>,
{
    let batch_rows = inserted_swaps.len();
    let mut batch_offset = 0usize;
    let mut attempt_index = 0usize;
    while batch_offset < batch_rows {
        attempt_index = attempt_index.saturating_add(1);
        let suffix = &inserted_swaps[batch_offset..];
        let write_started = Instant::now();
        let (summary, time_budget_exhausted) = write_attempt(suffix, deadline_for_attempt())?;
        let elapsed_ms = elapsed_ms_ceil(write_started.elapsed());
        let processed_rows = recent_raw_journal_summary_processed_rows(&summary).min(suffix.len());
        if time_budget_exhausted {
            let committed_rows = batch_offset.saturating_add(processed_rows);
            let unproven_rows = batch_rows.saturating_sub(committed_rows);
            warn!(
                reason = OBSERVED_SWAP_RECENT_RAW_JOURNAL_WRITE_DEADLINE_EXHAUSTED,
                attempt_index,
                batch_rows,
                batch_offset,
                suffix_rows = suffix.len(),
                processed_rows,
                committed_rows,
                inserted_rows = summary.inserted_rows,
                unproven_rows,
                elapsed_ms,
                queue_depth_batches = telemetry.journal_queue_depth_batches(),
                queue_row_debt = telemetry.journal_queue_row_debt(),
                overflow_depth_batches = telemetry
                    .journal_overflow_depth_batches
                    .load(Ordering::Relaxed),
                overflow_row_debt = telemetry.journal_overflow_row_debt.load(Ordering::Relaxed),
                bulk_statement_count = summary.recent_raw_bulk_statement_count,
                bulk_rows_processed = summary.recent_raw_bulk_rows_processed,
                bulk_rows_inserted = summary.recent_raw_bulk_rows_inserted,
                bulk_value_build_duration_ms = summary.recent_raw_bulk_value_build_duration_ms,
                bulk_prepare_duration_ms = summary.recent_raw_bulk_prepare_duration_ms,
                bulk_execute_duration_ms = summary.recent_raw_bulk_execute_duration_ms,
                bulk_state_refresh_duration_ms = summary.recent_raw_bulk_state_refresh_duration_ms,
                bulk_state_upsert_duration_ms = summary.recent_raw_bulk_state_upsert_duration_ms,
                bulk_transaction_duration_ms = summary.recent_raw_bulk_transaction_duration_ms,
                bulk_deadline_exhausted_before_statement =
                    summary.recent_raw_bulk_deadline_exhausted_before_statement,
                bulk_deadline_exhausted_during_execute =
                    summary.recent_raw_bulk_deadline_exhausted_during_execute,
                "recent raw journal write deadline exhausted"
            );
            if processed_rows == 0 {
                return Err(anyhow!(
                    "{OBSERVED_SWAP_RECENT_RAW_JOURNAL_WRITE_DEADLINE_EXHAUSTED}: batch_rows={batch_rows} committed_rows={committed_rows} inserted_rows={} unproven_rows={unproven_rows} elapsed_ms={elapsed_ms}",
                    summary.inserted_rows
                ));
            }
            batch_offset = committed_rows;
            if batch_offset < batch_rows {
                thread::sleep(OBSERVED_SWAP_WRITER_RETRYABLE_LOCK_BACKOFF);
            }
            continue;
        }
        if processed_rows != suffix.len() {
            let committed_rows = batch_offset.saturating_add(processed_rows);
            let unproven_rows = batch_rows.saturating_sub(committed_rows);
            return Err(anyhow!(
                "observed_swap_writer_recent_raw_journal_write_partial_without_deadline_exhaustion: batch_rows={batch_rows} committed_rows={committed_rows} inserted_rows={} unproven_rows={unproven_rows} elapsed_ms={elapsed_ms}",
                summary.inserted_rows
            ));
        }
        batch_offset = batch_rows;
        debug!(
            attempt_index,
            batch_rows,
            committed_rows = batch_offset,
            inserted_rows = summary.inserted_rows,
            elapsed_ms,
            bulk_statement_count = summary.recent_raw_bulk_statement_count,
            bulk_rows_processed = summary.recent_raw_bulk_rows_processed,
            bulk_rows_inserted = summary.recent_raw_bulk_rows_inserted,
            "recent raw journal batch write completed within deadline"
        );
    }
    Ok(())
}

pub(in crate::observed_swap_writer) fn recent_raw_journal_summary_processed_rows(
    summary: &RecentRawJournalWriteSummary,
) -> usize {
    summary
        .recent_raw_bulk_rows_processed
        .max(summary.batch_rows)
}
