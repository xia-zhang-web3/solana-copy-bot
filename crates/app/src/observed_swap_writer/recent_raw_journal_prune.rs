use super::*;

pub(in crate::observed_swap_writer) fn prune_recent_raw_journal_with_budget(
    store: &SqliteStore,
    retention_days: u32,
    now: DateTime<Utc>,
) -> Result<SqliteBatchedDeleteSummary> {
    let cutoff = now - ChronoDuration::days(retention_days.max(1) as i64);
    let mut summary = SqliteBatchedDeleteSummary::default();
    let started = Instant::now();
    loop {
        if summary.batches >= RECENT_RAW_JOURNAL_RETENTION_MAX_DELETE_BATCHES_PER_RUN {
            break;
        }
        if started.elapsed() >= RECENT_RAW_JOURNAL_RETENTION_MAX_DURATION_PER_RUN {
            break;
        }
        let deleted = store.prune_recent_raw_journal_before_batch(
            cutoff,
            RECENT_RAW_JOURNAL_RETENTION_DELETE_BATCH_SIZE,
            now,
        )?;
        if deleted == 0 {
            break;
        }
        summary.deleted_rows = summary.deleted_rows.saturating_add(deleted);
        summary.batches = summary.batches.saturating_add(1);
    }
    Ok(summary)
}

pub(in crate::observed_swap_writer) fn recent_raw_journal_prune_backlog_skip_reason(
    config: &ObservedSwapRecentRawJournalConfig,
    telemetry: &ObservedSwapWriterTelemetry,
) -> Option<&'static str> {
    if !config.skip_prune_while_backlogged {
        return None;
    }
    let pending_requests = telemetry.pending_requests.load(Ordering::Relaxed);
    let queue_depth_batches = telemetry.journal_queue_depth_batches();
    let queue_row_debt = telemetry.journal_queue_row_debt();
    let overflow_depth_batches = telemetry
        .journal_overflow_depth_batches
        .load(Ordering::Relaxed);
    let overflow_row_debt = telemetry.journal_overflow_row_debt.load(Ordering::Relaxed);
    let inflight_rows = telemetry
        .journal_writer_inflight_rows
        .load(Ordering::Relaxed);
    if pending_requests > 0 {
        return Some("pending_requests");
    }
    if queue_depth_batches > 0 {
        return Some("journal_queue_depth_batches");
    }
    if queue_row_debt > 0 {
        return Some("journal_queue_row_debt");
    }
    if overflow_depth_batches > 0 {
        return Some("journal_overflow_depth_batches");
    }
    if overflow_row_debt > 0 {
        return Some("journal_overflow_row_debt");
    }
    if inflight_rows > 0
        && (queue_depth_batches > 0
            || queue_row_debt > 0
            || overflow_depth_batches > 0
            || overflow_row_debt > 0)
    {
        return Some("journal_writer_inflight_rows");
    }
    None
}

pub(in crate::observed_swap_writer) fn recent_raw_journal_prune_due(
    store: &SqliteStore,
    config: &ObservedSwapRecentRawJournalConfig,
    telemetry: &ObservedSwapWriterTelemetry,
    now: DateTime<Utc>,
) -> Result<bool> {
    if recent_raw_journal_prune_backlog_skip_reason(config, telemetry).is_some() {
        return Ok(false);
    }
    if !config.skip_prune_while_backlogged {
        return Ok(true);
    }
    let state = store.recent_raw_journal_state_cached()?;
    Ok(state.last_pruned_at.map_or(true, |last_pruned_at| {
        (now - last_pruned_at).to_std().unwrap_or_default()
            >= OBSERVED_SWAP_RECENT_RAW_JOURNAL_PRUNE_RETRY_INTERVAL
    }))
}
