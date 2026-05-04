fn discovery_catch_up_block_reason(
    discovery_output: &DiscoveryTaskOutput,
    shadow_queue_full: bool,
    observed_swap_writer_snapshot: &ObservedSwapWriterSnapshot,
    ingestion_runtime_snapshot: Option<&IngestionRuntimeSnapshot>,
) -> Option<DiscoveryCatchUpBlockReason> {
    if !discovery_output.persisted_stream_catch_up_requested {
        return None;
    }
    if shadow_queue_full {
        return Some(DiscoveryCatchUpBlockReason::ShadowQueueFull);
    }
    if observed_swap_writer_snapshot.aggregate_queue_depth_batches > 0 {
        return Some(DiscoveryCatchUpBlockReason::WriterAggregateQueueDepth);
    }
    if observed_swap_writer_snapshot.aggregate_overflow_depth_batches > 0 {
        return Some(DiscoveryCatchUpBlockReason::WriterAggregateOverflowDepth);
    }
    if observed_swap_writer_snapshot.journal_queue_depth_batches > 0 {
        return Some(DiscoveryCatchUpBlockReason::WriterJournalQueueDepth);
    }
    if observed_swap_writer_snapshot.journal_overflow_depth_batches > 0 {
        return Some(DiscoveryCatchUpBlockReason::WriterJournalOverflowDepth);
    }
    if discovery_catch_up_has_ingestion_pressure(ingestion_runtime_snapshot) {
        return Some(DiscoveryCatchUpBlockReason::YellowstoneOutputQueueFill);
    }
    if discovery_output.persisted_stream_catch_up_pressure_override_requested {
        return None;
    }
    if discovery_catch_up_pending_requests_is_only_raw_plateau(
        observed_swap_writer_snapshot,
        ingestion_runtime_snapshot,
    ) {
        return None;
    }
    if observed_swap_writer_snapshot.pending_requests
        >= DISCOVERY_CATCH_UP_WRITER_PENDING_REQUESTS_THRESHOLD
    {
        return Some(DiscoveryCatchUpBlockReason::WriterPendingRequests);
    }
    None
}

fn should_schedule_discovery_catch_up(
    discovery_output: &DiscoveryTaskOutput,
    shadow_queue_full: bool,
    observed_swap_writer_snapshot: &ObservedSwapWriterSnapshot,
    ingestion_runtime_snapshot: Option<&IngestionRuntimeSnapshot>,
) -> bool {
    if !discovery_output.persisted_stream_catch_up_requested {
        return false;
    }
    discovery_catch_up_block_reason(
        discovery_output,
        shadow_queue_full,
        observed_swap_writer_snapshot,
        ingestion_runtime_snapshot,
    )
    .is_none()
}

fn spawn_logged_discovery_task(
    sqlite_path: String,
    recent_raw_journal_path: String,
    recent_raw_replay_batch_size: usize,
    discovery: DiscoveryService,
    now: DateTime<Utc>,
    trigger: &'static str,
) -> JoinHandle<Result<DiscoveryTaskOutput>> {
    info!(
        discovery_task_trigger = trigger,
        discovery_task_cycle_ts = %now,
        "discovery task scheduled"
    );
    tokio::task::spawn_blocking(move || {
        info!(
            discovery_task_trigger = trigger,
            discovery_task_cycle_ts = %now,
            "discovery task started"
        );
        let result = spawn_discovery_task(
            sqlite_path,
            recent_raw_journal_path,
            recent_raw_replay_batch_size,
            discovery,
            now,
        )();
        match &result {
            Ok(output) => {
                info!(
                    discovery_task_trigger = trigger,
                    discovery_task_cycle_ts = %now,
                    discovery_task_result = "success",
                    discovery_runtime_mode = output.runtime_mode.as_str(),
                    discovery_scoring_source = output.scoring_source,
                    discovery_published = output.published,
                    "discovery task completed"
                );
            }
            Err(error) => {
                warn!(
                    discovery_task_trigger = trigger,
                    discovery_task_cycle_ts = %now,
                    discovery_task_result = "error",
                    error = %error,
                    "discovery task completed with error"
                );
            }
        }
        result
    })
}

struct DiscoveryTaskOutput {
    active_wallets: HashSet<String>,
    cycle_ts: DateTime<Utc>,
    eligible_wallets: usize,
    active_follow_wallets: usize,
    published: bool,
    runtime_mode: DiscoveryRuntimeMode,
    scoring_source: &'static str,
    raw_window_cap_truncated: bool,
    cap_truncation_deactivation_guard_active: bool,
    cap_truncation_deactivation_guard_reason: Option<&'static str>,
    cap_truncation_deactivation_guard_started_at: Option<DateTime<Utc>>,
    cap_truncation_floor_ts_utc: Option<DateTime<Utc>>,
    cap_truncation_floor_signature: Option<String>,
    persisted_stream_catch_up_requested: bool,
    persisted_stream_catch_up_pressure_override_requested: bool,
}
