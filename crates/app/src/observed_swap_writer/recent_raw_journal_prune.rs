use super::*;

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
    let queue_pressure_threshold = config
        .writer_queue_capacity_batches
        .max(1)
        .saturating_div(2)
        .max(1);
    let queue_row_pressure_threshold = queue_pressure_threshold
        .saturating_mul(OBSERVED_SWAP_RECENT_RAW_JOURNAL_ADAPTIVE_COALESCE_MAX_ROWS_CAP);
    if pending_requests >= OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY / 2 {
        return Some("pending_requests");
    }
    if queue_depth_batches >= queue_pressure_threshold {
        return Some("journal_queue_depth_batches");
    }
    if queue_row_debt >= queue_row_pressure_threshold {
        return Some("journal_queue_row_debt");
    }
    if overflow_depth_batches > 0 {
        return Some("journal_overflow_depth_batches");
    }
    if overflow_row_debt > 0 {
        return Some("journal_overflow_row_debt");
    }
    if inflight_rows > 0
        && (queue_depth_batches >= queue_pressure_threshold
            || queue_row_debt >= queue_row_pressure_threshold
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

pub(in crate::observed_swap_writer) fn maybe_prune_recent_raw_journal(
    store: &SqliteStore,
    config: &ObservedSwapRecentRawJournalConfig,
    telemetry: &ObservedSwapWriterTelemetry,
    now: DateTime<Utc>,
) -> Result<Option<usize>> {
    if !recent_raw_journal_prune_due(store, config, telemetry, now)? {
        return Ok(None);
    }
    let cutoff = now - ChronoDuration::days(config.retention_days.max(1) as i64);
    let deleted = store.prune_recent_raw_journal_before_batch(
        cutoff,
        OBSERVED_SWAP_RECENT_RAW_JOURNAL_PRUNE_BATCH_SIZE,
        now,
    )?;
    Ok(Some(deleted))
}
