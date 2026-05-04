fn collect_observed_swap_write_batch(
    first_request: ObservedSwapWriteRequest,
    receiver: &mut mpsc::Receiver<ObservedSwapWriteRequest>,
    batch_max_size: usize,
) -> Vec<ObservedSwapWriteRequest> {
    let mut batch = vec![first_request];
    while batch.len() < batch_max_size {
        match receiver.try_recv() {
            Ok(request) => batch.push(request),
            Err(mpsc::error::TryRecvError::Empty) => break,
            Err(mpsc::error::TryRecvError::Disconnected) => break,
        }
    }
    batch
}

fn unpack_observed_swap_write_batch(
    batch: Vec<ObservedSwapWriteRequest>,
) -> (
    Vec<SwapEvent>,
    Vec<Option<oneshot::Sender<Result<bool>>>>,
    Vec<Instant>,
) {
    let mut swaps = Vec::with_capacity(batch.len());
    let mut replies = Vec::with_capacity(batch.len());
    let mut queued_at = Vec::with_capacity(batch.len());
    for request in batch {
        swaps.push(request.swap);
        replies.push(request.reply_tx);
        queued_at.push(request.enqueued_at);
    }
    (swaps, replies, queued_at)
}

fn send_observed_swap_write_error_replies(
    replies: Vec<Option<oneshot::Sender<Result<bool>>>>,
    message: &str,
) {
    for reply_tx in replies {
        if let Some(reply_tx) = reply_tx {
            let _ = reply_tx.send(Err(anyhow!(message.to_string())));
        }
    }
}

fn flush_observed_swap_writer_downstream_overflow_on_shutdown(
    aggregate_sender: Option<&std_mpsc::SyncSender<DiscoveryAggregateWriteRequest>>,
    journal_sender: Option<&std_mpsc::SyncSender<RecentRawJournalWriteRequest>>,
    aggregate_overflow: &mut VecDeque<DiscoveryAggregateWriteRequest>,
    journal_overflow: &mut VecDeque<RecentRawJournalWriteRequest>,
    telemetry: &ObservedSwapWriterTelemetry,
) -> Result<()> {
    if let Some(aggregate_sender) = aggregate_sender {
        flush_discovery_aggregate_overflow_blocking(
            aggregate_sender,
            aggregate_overflow,
            telemetry,
        )?;
    }
    if let Some(journal_sender) = journal_sender {
        flush_recent_raw_journal_overflow_blocking(journal_sender, journal_overflow, telemetry)?;
    }
    Ok(())
}
