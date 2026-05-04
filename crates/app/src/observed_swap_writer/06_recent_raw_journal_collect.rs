#[derive(Debug)]
struct RecentRawJournalCollectedWriteBatch {
    inserted_swaps: Vec<SwapEvent>,
    request_batches: usize,
    coalesce_elapsed_ms: u64,
    coalesce_limit_rows: usize,
}

fn collect_recent_raw_journal_write_batch(
    receiver: &std_mpsc::Receiver<RecentRawJournalWriteRequest>,
    first_request: RecentRawJournalWriteRequest,
    max_batches: usize,
    adaptive_max_batches: usize,
    adaptive_max_rows: usize,
    telemetry: &ObservedSwapWriterTelemetry,
) -> RecentRawJournalCollectedWriteBatch {
    let coalesce_started = Instant::now();
    let first_request_rows = first_request.row_count();
    telemetry.note_journal_queue_dequeued(first_request_rows);
    let mut inserted_swaps = first_request.inserted_swaps;
    let drain_limit = adaptive_max_batches.max(max_batches.max(1));
    let row_limit = adaptive_max_rows.max(1);
    let mut drained_batches = 1usize;
    while drained_batches < drain_limit && inserted_swaps.len() < row_limit {
        match receiver.try_recv() {
            Ok(request) => {
                let request_rows = request.row_count();
                telemetry.note_journal_queue_dequeued(request_rows);
                inserted_swaps.extend(request.inserted_swaps);
                drained_batches = drained_batches.saturating_add(1);
            }
            Err(std_mpsc::TryRecvError::Empty) => {
                if !recent_raw_journal_adaptive_coalesce_pressure(telemetry) {
                    break;
                }
                let elapsed = coalesce_started.elapsed();
                if elapsed >= OBSERVED_SWAP_RECENT_RAW_JOURNAL_ADAPTIVE_COALESCE_WINDOW {
                    break;
                }
                let remaining = OBSERVED_SWAP_RECENT_RAW_JOURNAL_ADAPTIVE_COALESCE_WINDOW - elapsed;
                let wait_for =
                    remaining.min(OBSERVED_SWAP_RECENT_RAW_JOURNAL_ADAPTIVE_COALESCE_POLL);
                match receiver.recv_timeout(wait_for) {
                    Ok(request) => {
                        let request_rows = request.row_count();
                        telemetry.note_journal_queue_dequeued(request_rows);
                        inserted_swaps.extend(request.inserted_swaps);
                        drained_batches = drained_batches.saturating_add(1);
                    }
                    Err(std_mpsc::RecvTimeoutError::Timeout)
                        if recent_raw_journal_adaptive_coalesce_pressure(telemetry) =>
                    {
                        continue;
                    }
                    Err(std_mpsc::RecvTimeoutError::Timeout) => break,
                    Err(std_mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
            Err(std_mpsc::TryRecvError::Disconnected) => break,
        }
    }
    RecentRawJournalCollectedWriteBatch {
        inserted_swaps,
        request_batches: drained_batches,
        coalesce_elapsed_ms: elapsed_ms_ceil(coalesce_started.elapsed()),
        coalesce_limit_rows: row_limit,
    }
}

fn collect_discovery_aggregate_write_batch(
    receiver: &std_mpsc::Receiver<DiscoveryAggregateWriteRequest>,
    first_request: DiscoveryAggregateWriteRequest,
    max_batches: usize,
    telemetry: &ObservedSwapWriterTelemetry,
) -> DiscoveryAggregateWriteRequest {
    telemetry.note_aggregate_queue_dequeued();
    let mut inserted_swaps = first_request.inserted_swaps;
    let batch_started = first_request.batch_started;
    let drain_limit = max_batches.max(1);
    let mut drained_batches = 1usize;
    while drained_batches < drain_limit {
        match receiver.try_recv() {
            Ok(request) => {
                telemetry.note_aggregate_queue_dequeued();
                inserted_swaps.extend(request.inserted_swaps);
                drained_batches = drained_batches.saturating_add(1);
            }
            Err(std_mpsc::TryRecvError::Empty | std_mpsc::TryRecvError::Disconnected) => break,
        }
    }
    DiscoveryAggregateWriteRequest {
        inserted_swaps,
        batch_started,
    }
}
