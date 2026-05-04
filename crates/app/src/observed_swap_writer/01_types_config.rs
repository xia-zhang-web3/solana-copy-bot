#[derive(Clone)]
struct ObservedSwapWriterConfig {
    channel_capacity: usize,
    batch_max_size: usize,
    normal_try_enqueue_soft_limit_override: Option<usize>,
    aggregate_writes_enabled: bool,
    aggregate_write_config: DiscoveryAggregateWriteConfig,
    aggregate_write_coalesce_max_batches: usize,
    aggregate_overflow_capacity_batches: usize,
    aggregate_gap_fallback_enabled: bool,
    aggregate_idle_replay_max_pages: usize,
    recent_raw_journal: Option<ObservedSwapRecentRawJournalConfig>,
}

#[derive(Clone, Debug)]
pub(crate) struct ObservedSwapRecentRawJournalConfig {
    pub sqlite_path: String,
    pub retention_days: u32,
    pub writer_queue_capacity_batches: usize,
    pub write_coalesce_max_batches: usize,
    pub overflow_capacity_batches: usize,
    pub skip_prune_while_backlogged: bool,
    pub skip_startup_prune: bool,
}

impl ObservedSwapWriterConfig {
    fn production(
        aggregate_writes_enabled: bool,
        aggregate_write_config: DiscoveryAggregateWriteConfig,
        recent_raw_journal: Option<ObservedSwapRecentRawJournalConfig>,
    ) -> Self {
        Self {
            channel_capacity: OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY,
            batch_max_size: OBSERVED_SWAP_BATCH_MAX_SIZE,
            normal_try_enqueue_soft_limit_override: None,
            aggregate_writes_enabled,
            aggregate_write_config,
            aggregate_write_coalesce_max_batches:
                OBSERVED_SWAP_DISCOVERY_AGGREGATE_WRITE_COALESCE_MAX_BATCHES,
            aggregate_overflow_capacity_batches:
                observed_swap_writer_default_aggregate_overflow_capacity_batches(
                    OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY,
                    OBSERVED_SWAP_BATCH_MAX_SIZE,
                    aggregate_writes_enabled,
                ),
            aggregate_gap_fallback_enabled: true,
            aggregate_idle_replay_max_pages:
                OBSERVED_SWAP_DISCOVERY_AGGREGATE_IDLE_REPLAY_MAX_PAGES,
            recent_raw_journal,
        }
    }

    #[cfg(test)]
    fn for_test(
        channel_capacity: usize,
        batch_max_size: usize,
        aggregate_writes_enabled: bool,
        aggregate_write_config: DiscoveryAggregateWriteConfig,
        recent_raw_journal: Option<ObservedSwapRecentRawJournalConfig>,
    ) -> Self {
        Self::for_test_with_aggregate_tuning(
            channel_capacity,
            batch_max_size,
            aggregate_writes_enabled,
            aggregate_write_config,
            OBSERVED_SWAP_DISCOVERY_AGGREGATE_WRITE_COALESCE_MAX_BATCHES,
            observed_swap_writer_default_aggregate_overflow_capacity_batches(
                channel_capacity,
                batch_max_size,
                aggregate_writes_enabled,
            ),
            true,
            OBSERVED_SWAP_DISCOVERY_AGGREGATE_IDLE_REPLAY_MAX_PAGES,
            recent_raw_journal,
        )
    }

    #[cfg(test)]
    fn for_test_with_aggregate_tuning(
        channel_capacity: usize,
        batch_max_size: usize,
        aggregate_writes_enabled: bool,
        aggregate_write_config: DiscoveryAggregateWriteConfig,
        aggregate_write_coalesce_max_batches: usize,
        aggregate_overflow_capacity_batches: usize,
        aggregate_gap_fallback_enabled: bool,
        aggregate_idle_replay_max_pages: usize,
        recent_raw_journal: Option<ObservedSwapRecentRawJournalConfig>,
    ) -> Self {
        Self {
            channel_capacity,
            batch_max_size,
            normal_try_enqueue_soft_limit_override: None,
            aggregate_writes_enabled,
            aggregate_write_config,
            aggregate_write_coalesce_max_batches,
            aggregate_overflow_capacity_batches,
            aggregate_gap_fallback_enabled,
            aggregate_idle_replay_max_pages,
            recent_raw_journal,
        }
    }

    #[cfg(test)]
    fn with_normal_try_enqueue_soft_limit(mut self, limit: usize) -> Self {
        self.normal_try_enqueue_soft_limit_override = Some(limit.max(1));
        self
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ObservedSwapRetentionConfig {
    pub retention_days: u32,
    pub aggregate_retention_days: u32,
    pub aggregate_writes_enabled: bool,
}

impl ObservedSwapRetentionConfig {
    pub(crate) fn production(
        retention_days: u32,
        aggregate_retention_days: u32,
        aggregate_writes_enabled: bool,
    ) -> Self {
        Self {
            retention_days,
            aggregate_retention_days,
            aggregate_writes_enabled,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ObservedSwapRetentionCheckpointSummary {
    pub mode: &'static str,
    pub busy: i64,
    pub log_frames: i64,
    pub checkpointed_frames: i64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ObservedSwapRetentionMaintenanceSummary {
    pub nominal_cutoff: DateTime<Utc>,
    pub effective_cutoff: DateTime<Utc>,
    pub aggregate_cutoff: Option<DateTime<Utc>>,
    pub raw_deleted_rows: usize,
    pub raw_delete_batches: usize,
    pub scoring_deleted_rows: usize,
    pub scoring_delete_batches: usize,
    pub completed_full_sweep: bool,
    pub stop_reason: Option<&'static str>,
    pub checkpoint: ObservedSwapRetentionCheckpointSummary,
    pub duration_ms: u64,
}

#[derive(Clone)]
pub(crate) struct ObservedSwapWriterHealthHandle {
    telemetry: Arc<ObservedSwapWriterTelemetry>,
}

impl ObservedSwapWriterHealthHandle {
    pub(crate) fn snapshot(&self) -> ObservedSwapWriterSnapshot {
        self.telemetry.snapshot()
    }
}

#[derive(Clone)]
pub(crate) struct ObservedSwapRetentionRuntimeHealthHandle {
    writer: ObservedSwapWriterHealthHandle,
    ingestion_runtime_snapshot: Arc<Mutex<Option<IngestionRuntimeSnapshot>>>,
}

impl ObservedSwapRetentionRuntimeHealthHandle {
    pub(crate) fn new(
        writer: ObservedSwapWriterHealthHandle,
        ingestion_runtime_snapshot: Arc<Mutex<Option<IngestionRuntimeSnapshot>>>,
    ) -> Self {
        Self {
            writer,
            ingestion_runtime_snapshot,
        }
    }

    fn writer_snapshot(&self) -> ObservedSwapWriterSnapshot {
        self.writer.snapshot()
    }

    fn ingestion_snapshot(&self) -> Option<IngestionRuntimeSnapshot> {
        self.ingestion_runtime_snapshot
            .lock()
            .ok()
            .and_then(|snapshot| *snapshot)
    }
}

struct ObservedSwapWriteRequest {
    swap: SwapEvent,
    reply_tx: Option<oneshot::Sender<Result<bool>>>,
    enqueued_at: Instant,
}

struct DiscoveryAggregateWriteRequest {
    inserted_swaps: Vec<SwapEvent>,
    batch_started: Instant,
}

enum DiscoveryAggregateEnqueueOutcome {
    Enqueued,
    DeferredToMaterializationGap(DiscoveryAggregateWriteRequest),
}

struct RecentRawJournalWriteRequest {
    inserted_swaps: Vec<SwapEvent>,
}

impl RecentRawJournalWriteRequest {
    fn row_count(&self) -> usize {
        self.inserted_swaps.len()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ObservedSwapWriterSnapshot {
    pub pending_requests: usize,
    pub write_latency_ms_p95: u64,
    pub raw_batch_write_ms_p95: u64,
    pub observed_swaps_insert_ms_p95: u64,
    pub wallet_activity_days_ms_p95: u64,
    pub discovery_scoring_ms_p95: u64,
    pub journal_enqueue_wait_ms_p95: u64,
    pub journal_batch_write_ms_p95: u64,
    pub worker_busy_ms_p95: u64,
    pub aggregate_queue_depth_batches: usize,
    pub aggregate_queue_capacity_batches: usize,
    pub aggregate_overflow_depth_batches: usize,
    pub aggregate_overflow_capacity_batches: usize,
    pub journal_queue_depth_batches: usize,
    pub journal_queue_row_debt: usize,
    pub journal_queue_capacity_batches: usize,
    pub journal_overflow_depth_batches: usize,
    pub journal_overflow_row_debt: usize,
    pub journal_overflow_capacity_batches: usize,
    pub journal_overflow_row_debt_capacity: usize,
    pub journal_writer_inflight_rows: usize,
    pub journal_sqlite_write_retry_total: u64,
    pub journal_sqlite_busy_error_total: u64,
}
