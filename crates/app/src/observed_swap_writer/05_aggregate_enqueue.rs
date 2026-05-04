fn enqueue_discovery_aggregate_request(
    aggregate_sender: &std_mpsc::SyncSender<DiscoveryAggregateWriteRequest>,
    aggregate_overflow: &mut VecDeque<DiscoveryAggregateWriteRequest>,
    aggregate_overflow_capacity_batches: usize,
    aggregate_gap_fallback_enabled: bool,
    request: DiscoveryAggregateWriteRequest,
    telemetry: &ObservedSwapWriterTelemetry,
) -> Result<DiscoveryAggregateEnqueueOutcome> {
    drain_discovery_aggregate_overflow_nonblocking(
        aggregate_sender,
        aggregate_overflow,
        telemetry,
    )?;
    if aggregate_gap_fallback_enabled
        && (telemetry.aggregate_gap_active()
            || telemetry.aggregate_worker_busy()
            || telemetry
                .aggregate_queue_depth_batches
                .load(Ordering::Relaxed)
                > 0
            || telemetry
                .aggregate_overflow_depth_batches
                .load(Ordering::Relaxed)
                > 0
            || !aggregate_overflow.is_empty())
    {
        return Ok(DiscoveryAggregateEnqueueOutcome::DeferredToMaterializationGap(request));
    }
    if aggregate_overflow.is_empty() {
        match aggregate_sender.try_send(request) {
            Ok(()) => {
                telemetry.note_aggregate_queue_enqueued();
                return Ok(DiscoveryAggregateEnqueueOutcome::Enqueued);
            }
            Err(std_mpsc::TrySendError::Full(request)) => {
                if aggregate_overflow_capacity_batches == 0 && !aggregate_gap_fallback_enabled {
                    aggregate_sender.send(request).map_err(|error| {
                        anyhow!("discovery aggregate writer channel closed: {error}")
                    })?;
                    telemetry.note_aggregate_queue_enqueued();
                    return Ok(DiscoveryAggregateEnqueueOutcome::Enqueued);
                }
                if aggregate_overflow_capacity_batches == 0 {
                    return Ok(
                        DiscoveryAggregateEnqueueOutcome::DeferredToMaterializationGap(request),
                    );
                }
                aggregate_overflow.push_back(request);
                telemetry.note_aggregate_overflow_enqueued();
                return Ok(DiscoveryAggregateEnqueueOutcome::Enqueued);
            }
            Err(std_mpsc::TrySendError::Disconnected(_request)) => {
                return Err(anyhow!("discovery aggregate writer channel closed"));
            }
        }
    }

    if aggregate_overflow.len() < aggregate_overflow_capacity_batches {
        aggregate_overflow.push_back(request);
        telemetry.note_aggregate_overflow_enqueued();
        return Ok(DiscoveryAggregateEnqueueOutcome::Enqueued);
    }

    if aggregate_gap_fallback_enabled {
        return Ok(DiscoveryAggregateEnqueueOutcome::DeferredToMaterializationGap(request));
    }

    flush_discovery_aggregate_overflow_blocking(aggregate_sender, aggregate_overflow, telemetry)?;
    while !aggregate_overflow.is_empty() {
        flush_discovery_aggregate_overflow_blocking(
            aggregate_sender,
            aggregate_overflow,
            telemetry,
        )?;
    }
    aggregate_sender
        .send(request)
        .map_err(|error| anyhow!("discovery aggregate writer channel closed: {error}"))?;
    telemetry.note_aggregate_queue_enqueued();
    Ok(DiscoveryAggregateEnqueueOutcome::Enqueued)
}

fn drain_discovery_aggregate_overflow_nonblocking(
    aggregate_sender: &std_mpsc::SyncSender<DiscoveryAggregateWriteRequest>,
    aggregate_overflow: &mut VecDeque<DiscoveryAggregateWriteRequest>,
    telemetry: &ObservedSwapWriterTelemetry,
) -> Result<()> {
    while let Some(request) = aggregate_overflow.pop_front() {
        telemetry.note_aggregate_overflow_dequeued();
        match aggregate_sender.try_send(request) {
            Ok(()) => telemetry.note_aggregate_queue_enqueued(),
            Err(std_mpsc::TrySendError::Full(request)) => {
                aggregate_overflow.push_front(request);
                telemetry.note_aggregate_overflow_enqueued();
                break;
            }
            Err(std_mpsc::TrySendError::Disconnected(_request)) => {
                return Err(anyhow!("discovery aggregate writer channel closed"));
            }
        }
    }
    Ok(())
}

fn flush_discovery_aggregate_overflow_blocking(
    aggregate_sender: &std_mpsc::SyncSender<DiscoveryAggregateWriteRequest>,
    aggregate_overflow: &mut VecDeque<DiscoveryAggregateWriteRequest>,
    telemetry: &ObservedSwapWriterTelemetry,
) -> Result<()> {
    while let Some(request) = aggregate_overflow.pop_front() {
        telemetry.note_aggregate_overflow_dequeued();
        aggregate_sender
            .send(request)
            .map_err(|error| anyhow!("discovery aggregate writer channel closed: {error}"))?;
        telemetry.note_aggregate_queue_enqueued();
    }
    Ok(())
}
