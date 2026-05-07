use super::*;

pub(in crate::observed_swap_writer) fn drain_recent_raw_journal_overflow_nonblocking(
    journal_sender: &std_mpsc::SyncSender<RecentRawJournalWriteRequest>,
    journal_overflow: &mut VecDeque<RecentRawJournalWriteRequest>,
    max_coalesced_batches: usize,
    telemetry: &ObservedSwapWriterTelemetry,
) -> Result<()> {
    while let Some(drain_batch) = collect_recent_raw_journal_overflow_drain_batch(
        journal_overflow,
        max_coalesced_batches,
        telemetry,
    ) {
        let request_rows = drain_batch.request.row_count();
        let request_batches = drain_batch.request_batches;
        debug!(
            journal_request_batches = request_batches,
            journal_batch_rows = request_rows,
            journal_overflow_depth_batches = journal_overflow.len(),
            journal_overflow_row_debt = telemetry.journal_overflow_row_debt.load(Ordering::Relaxed),
            "recent raw journal overflow drain coalesced bounded request before channel send"
        );
        match journal_sender.try_send(drain_batch.request) {
            Ok(()) => telemetry.note_journal_queue_enqueued(request_rows),
            Err(std_mpsc::TrySendError::Full(request)) => {
                journal_overflow.push_front(request);
                telemetry.note_journal_overflow_enqueued(request_rows);
                break;
            }
            Err(std_mpsc::TrySendError::Disconnected(request)) => {
                journal_overflow.push_front(request);
                telemetry.note_journal_overflow_enqueued(request_rows);
                return Err(anyhow!("recent raw journal writer channel closed"));
            }
        }
    }
    Ok(())
}

pub(in crate::observed_swap_writer) struct RecentRawJournalOverflowDrainBatch {
    pub(in crate::observed_swap_writer) request: RecentRawJournalWriteRequest,
    pub(in crate::observed_swap_writer) request_batches: usize,
}

pub(in crate::observed_swap_writer) fn collect_recent_raw_journal_overflow_drain_batch(
    journal_overflow: &mut VecDeque<RecentRawJournalWriteRequest>,
    max_coalesced_batches: usize,
    telemetry: &ObservedSwapWriterTelemetry,
) -> Option<RecentRawJournalOverflowDrainBatch> {
    let mut request = journal_overflow.pop_front()?;
    let mut request_batches = 1usize;
    let request_rows = request.row_count();
    telemetry.note_journal_overflow_dequeued(request_rows);
    let max_coalesced_batches = max_coalesced_batches.max(1);
    while request_batches < max_coalesced_batches {
        let Some(next_request) = journal_overflow.pop_front() else {
            break;
        };
        let next_rows = next_request.row_count();
        telemetry.note_journal_overflow_dequeued(next_rows);
        request.inserted_swaps.extend(next_request.inserted_swaps);
        request_batches = request_batches.saturating_add(1);
    }
    Some(RecentRawJournalOverflowDrainBatch {
        request,
        request_batches,
    })
}

pub(in crate::observed_swap_writer) fn flush_recent_raw_journal_overflow_blocking(
    journal_sender: &std_mpsc::SyncSender<RecentRawJournalWriteRequest>,
    journal_overflow: &mut VecDeque<RecentRawJournalWriteRequest>,
    telemetry: &ObservedSwapWriterTelemetry,
) -> Result<()> {
    while let Some(request) = journal_overflow.pop_front() {
        let request_rows = request.row_count();
        telemetry.note_journal_overflow_dequeued(request_rows);
        journal_sender
            .send(request)
            .map_err(|error| anyhow!("recent raw journal writer channel closed: {error}"))?;
        telemetry.note_journal_queue_enqueued(request_rows);
    }
    Ok(())
}

pub(in crate::observed_swap_writer) fn log_recent_raw_journal_phase(
    current_phase: &'static str,
    reason: Option<&'static str>,
    batch_rows: usize,
    request_batches: usize,
    coalesce_elapsed_ms: u64,
    coalesce_limit_rows: usize,
    phase_elapsed_ms: u64,
    telemetry: &ObservedSwapWriterTelemetry,
) {
    #[cfg(test)]
    if let Ok(mut events) = RECENT_RAW_JOURNAL_PHASE_EVENTS_FOR_TEST.lock() {
        events.push(current_phase);
    }
    info!(
        current_phase,
        reason,
        journal_batch_rows = batch_rows,
        journal_request_batches = request_batches,
        journal_coalesce_elapsed_ms = coalesce_elapsed_ms,
        journal_coalesce_limit_rows = coalesce_limit_rows,
        journal_queue_depth_batches = telemetry.journal_queue_depth_batches(),
        journal_queue_row_debt = telemetry.journal_queue_row_debt(),
        journal_overflow_depth_batches = telemetry
            .journal_overflow_depth_batches
            .load(Ordering::Relaxed),
        journal_overflow_row_debt = telemetry.journal_overflow_row_debt.load(Ordering::Relaxed),
        journal_writer_inflight_rows = telemetry
            .journal_writer_inflight_rows
            .load(Ordering::Relaxed),
        elapsed_ms = phase_elapsed_ms,
        "recent raw journal writer phase"
    );
}
