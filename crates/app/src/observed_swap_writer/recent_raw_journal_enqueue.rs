use super::*;

pub(in crate::observed_swap_writer) fn enqueue_recent_raw_journal_request(
    journal_sender: &std_mpsc::SyncSender<RecentRawJournalWriteRequest>,
    journal_overflow: &mut VecDeque<RecentRawJournalWriteRequest>,
    journal_overflow_capacity_batches: usize,
    journal_overflow_row_capacity: usize,
    journal_overflow_drain_coalesce_max_batches: usize,
    request: RecentRawJournalWriteRequest,
    telemetry: &ObservedSwapWriterTelemetry,
) -> Result<()> {
    drain_recent_raw_journal_overflow_nonblocking(
        journal_sender,
        journal_overflow,
        journal_overflow_drain_coalesce_max_batches,
        telemetry,
    )?;
    let request_rows = request.row_count();
    if journal_overflow.is_empty() {
        match journal_sender.try_send(request) {
            Ok(()) => {
                telemetry.note_journal_queue_enqueued(request_rows);
                return Ok(());
            }
            Err(std_mpsc::TrySendError::Full(request)) => {
                if journal_overflow_capacity_batches == 0 {
                    journal_sender.send(request).map_err(|error| {
                        anyhow!("recent raw journal writer channel closed: {error}")
                    })?;
                    telemetry.note_journal_queue_enqueued(request_rows);
                    return Ok(());
                }
                ensure_recent_raw_journal_overflow_row_capacity(
                    journal_overflow,
                    request_rows,
                    journal_overflow_row_capacity,
                )?;
                journal_overflow.push_back(request);
                telemetry.note_journal_overflow_enqueued(request_rows);
                return Ok(());
            }
            Err(std_mpsc::TrySendError::Disconnected(_request)) => {
                return Err(anyhow!("recent raw journal writer channel closed"));
            }
        }
    }

    if journal_overflow.len() < journal_overflow_capacity_batches {
        ensure_recent_raw_journal_overflow_row_capacity(
            journal_overflow,
            request_rows,
            journal_overflow_row_capacity,
        )?;
        journal_overflow.push_back(request);
        telemetry.note_journal_overflow_enqueued(request_rows);
        return Ok(());
    }

    coalesce_recent_raw_journal_overflow_tail(
        journal_overflow,
        request,
        journal_overflow_row_capacity,
        telemetry,
    )?;
    Ok(())
}

pub(in crate::observed_swap_writer) fn coalesce_recent_raw_journal_overflow_tail(
    journal_overflow: &mut VecDeque<RecentRawJournalWriteRequest>,
    request: RecentRawJournalWriteRequest,
    journal_overflow_row_capacity: usize,
    telemetry: &ObservedSwapWriterTelemetry,
) -> Result<()> {
    let request_rows = request.row_count();
    ensure_recent_raw_journal_overflow_row_capacity(
        journal_overflow,
        request_rows,
        journal_overflow_row_capacity,
    )?;
    if let Some(tail) = journal_overflow.back_mut() {
        tail.inserted_swaps.extend(request.inserted_swaps);
    } else {
        journal_overflow.push_back(request);
        telemetry.note_journal_overflow_enqueued(request_rows);
        return Ok(());
    }
    telemetry.note_journal_overflow_rows_coalesced(request_rows);
    debug!(
        coalesced_rows = request_rows,
        journal_overflow_row_debt = telemetry.snapshot().journal_overflow_row_debt,
        journal_overflow_row_debt_capacity = journal_overflow_row_capacity,
        journal_overflow_depth_batches = journal_overflow.len(),
        "recent raw journal overflow row debt coalesced while downstream writer is backpressured"
    );
    Ok(())
}

pub(in crate::observed_swap_writer) fn ensure_recent_raw_journal_overflow_row_capacity(
    journal_overflow: &VecDeque<RecentRawJournalWriteRequest>,
    request_rows: usize,
    journal_overflow_row_capacity: usize,
) -> Result<()> {
    if journal_overflow_row_capacity == 0 {
        return Ok(());
    }
    let current_row_debt = recent_raw_journal_overflow_row_debt(journal_overflow);
    let next_row_debt = current_row_debt.saturating_add(request_rows);
    if next_row_debt > journal_overflow_row_capacity {
        anyhow::bail!(
            "{OBSERVED_SWAP_RECENT_RAW_JOURNAL_OVERFLOW_ROW_DEBT_CAP_EXCEEDED}: current_row_debt={current_row_debt} request_rows={request_rows} row_debt_capacity={journal_overflow_row_capacity}"
        );
    }
    Ok(())
}

pub(in crate::observed_swap_writer) fn recent_raw_journal_overflow_row_debt(
    journal_overflow: &VecDeque<RecentRawJournalWriteRequest>,
) -> usize {
    journal_overflow
        .iter()
        .map(RecentRawJournalWriteRequest::row_count)
        .sum()
}
