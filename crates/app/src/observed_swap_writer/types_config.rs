use super::*;

#[derive(Clone)]
pub(in crate::observed_swap_writer) struct ObservedSwapWriterConfig {
    pub(in crate::observed_swap_writer) channel_capacity: usize,
    pub(in crate::observed_swap_writer) batch_max_size: usize,
    pub(in crate::observed_swap_writer) normal_try_enqueue_soft_limit_override: Option<usize>,
    pub(in crate::observed_swap_writer) recent_raw_journal:
        Option<ObservedSwapRecentRawJournalConfig>,
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
    pub(in crate::observed_swap_writer) fn production(
        recent_raw_journal: Option<ObservedSwapRecentRawJournalConfig>,
    ) -> Self {
        Self {
            channel_capacity: OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY,
            batch_max_size: OBSERVED_SWAP_BATCH_MAX_SIZE,
            normal_try_enqueue_soft_limit_override: None,
            recent_raw_journal,
        }
    }

    #[cfg(test)]
    pub(in crate::observed_swap_writer) fn for_test(
        channel_capacity: usize,
        batch_max_size: usize,
        recent_raw_journal: Option<ObservedSwapRecentRawJournalConfig>,
    ) -> Self {
        Self {
            channel_capacity,
            batch_max_size,
            normal_try_enqueue_soft_limit_override: None,
            recent_raw_journal,
        }
    }

    #[cfg(test)]
    pub(in crate::observed_swap_writer) fn with_normal_try_enqueue_soft_limit(
        mut self,
        limit: usize,
    ) -> Self {
        self.normal_try_enqueue_soft_limit_override = Some(limit.max(1));
        self
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ObservedSwapRetentionConfig {
    pub retention_days: u32,
}

impl ObservedSwapRetentionConfig {
    pub(crate) fn production(retention_days: u32) -> Self {
        Self { retention_days }
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
    pub raw_deleted_rows: usize,
    pub raw_delete_batches: usize,
    pub completed_full_sweep: bool,
    pub stop_reason: Option<&'static str>,
    pub checkpoint: ObservedSwapRetentionCheckpointSummary,
    pub duration_ms: u64,
}

#[derive(Clone)]
pub(crate) struct ObservedSwapWriterHealthHandle {
    pub(in crate::observed_swap_writer) telemetry: Arc<ObservedSwapWriterTelemetry>,
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

    pub(in crate::observed_swap_writer) fn writer_snapshot(&self) -> ObservedSwapWriterSnapshot {
        self.writer.snapshot()
    }

    pub(in crate::observed_swap_writer) fn ingestion_snapshot(
        &self,
    ) -> Option<IngestionRuntimeSnapshot> {
        self.ingestion_runtime_snapshot
            .lock()
            .ok()
            .and_then(|snapshot| *snapshot)
    }
}

pub(in crate::observed_swap_writer) struct ObservedSwapWriteRequest {
    pub(in crate::observed_swap_writer) swap: SwapEvent,
    pub(in crate::observed_swap_writer) reply_tx: Option<oneshot::Sender<Result<bool>>>,
    pub(in crate::observed_swap_writer) enqueued_at: Instant,
}

pub(in crate::observed_swap_writer) struct RecentRawJournalWriteRequest {
    pub(in crate::observed_swap_writer) inserted_swaps: Vec<SwapEvent>,
}

impl RecentRawJournalWriteRequest {
    pub(in crate::observed_swap_writer) fn row_count(&self) -> usize {
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
    pub journal_enqueue_wait_ms_p95: u64,
    pub journal_batch_write_ms_p95: u64,
    pub worker_busy_ms_p95: u64,
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
