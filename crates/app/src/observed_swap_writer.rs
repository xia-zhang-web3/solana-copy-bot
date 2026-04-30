use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use copybot_core_types::SwapEvent;
use copybot_ingestion::IngestionRuntimeSnapshot;
use copybot_storage::{
    is_fatal_sqlite_anyhow_error, is_retryable_sqlite_anyhow_error, sqlite_contention_snapshot,
    DiscoveryAggregateWriteConfig, DiscoveryRuntimeCursor, RecentRawJournalWriteSummary,
    SqliteBatchedDeleteSummary, SqliteContentionSnapshot, SqliteStore,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc as std_mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration as StdDuration, Instant};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

pub(crate) const OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY: usize = 4096;
const OBSERVED_SWAP_BATCH_MAX_SIZE: usize = 128;
const OBSERVED_SWAP_WRITER_DISCOVERY_CRITICAL_RESERVED_BATCHES: usize = 1;
const OBSERVED_SWAP_WRITER_NONCRITICAL_IRRELEVANT_MAX_QUEUED_BATCHES: usize = 1;
pub(crate) const OBSERVED_SWAP_DISCOVERY_AGGREGATE_WRITE_COALESCE_MAX_BATCHES: usize = 16;
pub(crate) const OBSERVED_SWAP_DISCOVERY_AGGREGATE_OVERFLOW_CAPACITY_MULTIPLIER: usize = 4;
const OBSERVED_SWAP_DISCOVERY_AGGREGATE_IDLE_REPLAY_POLL_INTERVAL: StdDuration =
    StdDuration::from_millis(25);
const OBSERVED_SWAP_DISCOVERY_AGGREGATE_IDLE_REPLAY_MAX_PAGES: usize = 8;
pub(crate) const OBSERVED_SWAP_RECENT_RAW_JOURNAL_WRITE_COALESCE_MAX_BATCHES: usize = 32;
pub(crate) const OBSERVED_SWAP_RECENT_RAW_JOURNAL_OVERFLOW_CAPACITY_MULTIPLIER: usize = 4;
const OBSERVED_SWAP_RECENT_RAW_JOURNAL_ADAPTIVE_COALESCE_MAX_BATCHES_MULTIPLIER: usize = 8;
const OBSERVED_SWAP_RECENT_RAW_JOURNAL_ADAPTIVE_COALESCE_MAX_BATCHES_CAP: usize = 512;
const OBSERVED_SWAP_RECENT_RAW_JOURNAL_ADAPTIVE_COALESCE_MAX_ROWS_CAP: usize = 16_384;
const OBSERVED_SWAP_RECENT_RAW_JOURNAL_ADAPTIVE_COALESCE_WINDOW: StdDuration =
    StdDuration::from_millis(10);
const OBSERVED_SWAP_RECENT_RAW_JOURNAL_ADAPTIVE_COALESCE_POLL: StdDuration =
    StdDuration::from_millis(2);
const OBSERVED_SWAP_RECENT_RAW_JOURNAL_OVERFLOW_ROW_DEBT_CAP_EXCEEDED: &str =
    "observed_swap_writer_recent_raw_journal_overflow_row_debt_capacity_exceeded";
const OBSERVED_SWAP_RECENT_RAW_JOURNAL_WRITE_DEADLINE: StdDuration = StdDuration::from_secs(10);
const OBSERVED_SWAP_RECENT_RAW_JOURNAL_WRITE_DEADLINE_EXHAUSTED: &str =
    "observed_swap_writer_recent_raw_journal_write_deadline_exhausted";
const OBSERVED_SWAP_WRITER_DISCOVERY_SCORING_REPLAY_APPLY_SQLITE_LOCK_RETRYABLE: &str =
    "observed_swap_writer_discovery_scoring_replay_apply_sqlite_lock_retryable";
const OBSERVED_SWAP_WRITER_DISCOVERY_SCORING_COVERED_THROUGH_UPDATE_SQLITE_LOCK_RETRYABLE: &str =
    "observed_swap_writer_discovery_scoring_covered_through_update_sqlite_lock_retryable";
const OBSERVED_SWAP_WRITER_DISCOVERY_SCORING_RUG_FINALIZE_SQLITE_LOCK_RETRYABLE: &str =
    "observed_swap_writer_discovery_scoring_rug_finalize_sqlite_lock_retryable";
const RECENT_RAW_JOURNAL_PHASE_BATCH_COLLECTED: &str = "batch_collected";
const RECENT_RAW_JOURNAL_PHASE_WRITE_START: &str = "write_start";
const RECENT_RAW_JOURNAL_PHASE_WRITE_END: &str = "write_end";
const RECENT_RAW_JOURNAL_PHASE_PRUNE_CHECK_START: &str = "prune_check_start";
const RECENT_RAW_JOURNAL_PHASE_PRUNE_CHECK_END: &str = "prune_check_end";
const RECENT_RAW_JOURNAL_PHASE_PRUNE_START: &str = "prune_start";
const RECENT_RAW_JOURNAL_PHASE_PRUNE_END: &str = "prune_end";
const RECENT_RAW_JOURNAL_PHASE_PRUNE_SKIPPED: &str = "prune_skipped";
const RECENT_RAW_JOURNAL_PHASE_BATCH_DONE: &str = "batch_done";
const RECENT_RAW_JOURNAL_HOT_WRITER_PRUNE_DEFERRED: &str =
    "recent_raw_journal_hot_writer_prune_deferred";
pub(crate) const OBSERVED_SWAP_RETENTION_SWEEP_INTERVAL: StdDuration =
    StdDuration::from_secs(15 * 60);
pub(crate) const OBSERVED_SWAP_RETENTION_STARTUP_GRACE_INTERVAL: StdDuration =
    StdDuration::from_secs(30 * 60);
const OBSERVED_SWAP_RECENT_RAW_JOURNAL_PRUNE_RETRY_INTERVAL: StdDuration =
    StdDuration::from_secs(5 * 60);
const OBSERVED_SWAP_WRITER_LATENCY_SAMPLE_CAPACITY: usize = 512;
const OBSERVED_SWAP_RETENTION_DELETE_BATCH_SIZE: usize = 500;
const DISCOVERY_SCORING_RETENTION_DELETE_BATCH_SIZE: usize = 250;
const OBSERVED_SWAP_RETENTION_MAX_RAW_DELETE_BATCHES_PER_RUN: usize = 8;
const OBSERVED_SWAP_RETENTION_MAX_SCORING_DELETE_BATCHES_PER_RUN: usize = 4;
const OBSERVED_SWAP_RETENTION_MAX_DURATION_PER_RUN: StdDuration = StdDuration::from_secs(2);
const OBSERVED_SWAP_RETENTION_INTER_BATCH_PAUSE: StdDuration = StdDuration::from_millis(50);
const RECENT_RAW_JOURNAL_RETENTION_DELETE_BATCH_SIZE: usize = 500;
const RECENT_RAW_JOURNAL_RETENTION_MAX_DELETE_BATCHES_PER_RUN: usize = 8;
const OBSERVED_SWAP_WRITER_RETRYABLE_LOCK_BACKOFF: StdDuration = StdDuration::from_millis(250);
const OBSERVED_SWAP_WRITER_RETRYABLE_LOCK_LOG_INTERVAL: StdDuration = StdDuration::from_secs(5);

#[cfg(test)]
static RECENT_RAW_JOURNAL_PHASE_EVENTS_FOR_TEST: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());

pub(crate) const OBSERVED_SWAP_RETENTION_PARTIAL_RETRY_INTERVAL: StdDuration =
    StdDuration::from_secs(60);
const SQLITE_MAINTENANCE_MAX_YELLOWSTONE_OUTPUT_QUEUE_FILL_RATIO: f64 = 0.25;
pub(crate) const OBSERVED_SWAP_WRITER_CHANNEL_CLOSED_CONTEXT: &str =
    "observed swap writer channel closed";
pub(crate) const OBSERVED_SWAP_WRITER_REPLY_CLOSED_CONTEXT: &str =
    "observed swap writer reply channel closed";
pub(crate) const OBSERVED_SWAP_WRITER_TERMINAL_FAILURE_CONTEXT: &str =
    "observed swap writer terminal failure";

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

#[derive(Debug, Default)]
struct ObservedSwapWriterTelemetry {
    pending_requests: AtomicUsize,
    last_write_latency_ms_p95: AtomicU64,
    last_raw_batch_write_ms_p95: AtomicU64,
    last_observed_swaps_insert_ms_p95: AtomicU64,
    last_wallet_activity_days_ms_p95: AtomicU64,
    last_discovery_scoring_ms_p95: AtomicU64,
    last_journal_enqueue_wait_ms_p95: AtomicU64,
    last_journal_batch_write_ms_p95: AtomicU64,
    last_worker_busy_ms_p95: AtomicU64,
    aggregate_queue_depth_batches: AtomicUsize,
    aggregate_queue_capacity_batches: AtomicUsize,
    aggregate_overflow_depth_batches: AtomicUsize,
    aggregate_overflow_capacity_batches: AtomicUsize,
    journal_queue_enqueued_batches: AtomicUsize,
    journal_queue_dequeued_batches: AtomicUsize,
    journal_queue_enqueued_rows: AtomicUsize,
    journal_queue_dequeued_rows: AtomicUsize,
    journal_queue_capacity_batches: AtomicUsize,
    journal_overflow_depth_batches: AtomicUsize,
    journal_overflow_capacity_batches: AtomicUsize,
    journal_overflow_row_debt: AtomicUsize,
    journal_overflow_row_debt_capacity: AtomicUsize,
    journal_writer_inflight_rows: AtomicUsize,
    journal_sqlite_write_retry_total: AtomicU64,
    journal_sqlite_busy_error_total: AtomicU64,
    aggregate_gap_active: AtomicBool,
    aggregate_worker_busy: AtomicBool,
    write_latency_ms_samples: Mutex<VecDeque<u64>>,
    raw_batch_write_ms_samples: Mutex<VecDeque<u64>>,
    observed_swaps_insert_ms_samples: Mutex<VecDeque<u64>>,
    wallet_activity_days_ms_samples: Mutex<VecDeque<u64>>,
    discovery_scoring_ms_samples: Mutex<VecDeque<u64>>,
    journal_enqueue_wait_ms_samples: Mutex<VecDeque<u64>>,
    journal_batch_write_ms_samples: Mutex<VecDeque<u64>>,
    worker_busy_ms_samples: Mutex<VecDeque<u64>>,
}

impl ObservedSwapWriterTelemetry {
    fn note_enqueued(&self) {
        self.pending_requests.fetch_add(1, Ordering::Relaxed);
    }

    fn note_batch_completed(&self, queued_at: &[Instant]) {
        if queued_at.is_empty() {
            return;
        }
        let now = Instant::now();
        if let Ok(mut samples) = self.write_latency_ms_samples.lock() {
            for queued_at in queued_at {
                let latency_ms = now
                    .duration_since(*queued_at)
                    .as_millis()
                    .min(u128::from(u64::MAX)) as u64;
                if samples.len() >= OBSERVED_SWAP_WRITER_LATENCY_SAMPLE_CAPACITY {
                    let _ = samples.pop_front();
                }
                samples.push_back(latency_ms);
            }
            self.last_write_latency_ms_p95
                .store(percentile_from_deque(&samples, 0.95), Ordering::Relaxed);
        }
        let _ =
            self.pending_requests
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    Some(current.saturating_sub(queued_at.len()))
                });
    }

    fn note_raw_batch_completed(&self, duration_ms: u64) {
        self.note_phase_sample(
            &self.raw_batch_write_ms_samples,
            &self.last_raw_batch_write_ms_p95,
            duration_ms,
        );
    }

    fn note_discovery_scoring_completed(&self, duration_ms: u64) {
        self.note_phase_sample(
            &self.discovery_scoring_ms_samples,
            &self.last_discovery_scoring_ms_p95,
            duration_ms,
        );
    }

    fn note_journal_enqueue_wait_completed(&self, duration_ms: u64) {
        self.note_phase_sample(
            &self.journal_enqueue_wait_ms_samples,
            &self.last_journal_enqueue_wait_ms_p95,
            duration_ms,
        );
    }

    fn note_journal_batch_write_completed(&self, duration_ms: u64) {
        self.note_phase_sample(
            &self.journal_batch_write_ms_samples,
            &self.last_journal_batch_write_ms_p95,
            duration_ms,
        );
    }

    fn note_observed_swaps_insert_completed(&self, duration_ms: u64) {
        self.note_phase_sample(
            &self.observed_swaps_insert_ms_samples,
            &self.last_observed_swaps_insert_ms_p95,
            duration_ms,
        );
    }

    fn note_wallet_activity_days_completed(&self, duration_ms: u64) {
        self.note_phase_sample(
            &self.wallet_activity_days_ms_samples,
            &self.last_wallet_activity_days_ms_p95,
            duration_ms,
        );
    }

    fn note_worker_busy_completed(&self, duration_ms: u64) {
        self.note_phase_sample(
            &self.worker_busy_ms_samples,
            &self.last_worker_busy_ms_p95,
            duration_ms,
        );
    }

    fn note_aggregate_queue_enqueued(&self) {
        self.aggregate_queue_depth_batches
            .fetch_add(1, Ordering::Relaxed);
    }

    fn note_aggregate_queue_dequeued(&self) {
        let _ = self.aggregate_queue_depth_batches.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_sub(1)),
        );
    }

    fn note_aggregate_overflow_enqueued(&self) {
        self.aggregate_overflow_depth_batches
            .fetch_add(1, Ordering::Relaxed);
    }

    fn note_aggregate_overflow_dequeued(&self) {
        let _ = self.aggregate_overflow_depth_batches.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_sub(1)),
        );
    }

    fn note_journal_queue_enqueued(&self, rows: usize) {
        self.journal_queue_enqueued_batches
            .fetch_add(1, Ordering::Relaxed);
        self.journal_queue_enqueued_rows
            .fetch_add(rows, Ordering::Relaxed);
    }

    fn note_journal_queue_dequeued(&self, rows: usize) {
        self.journal_queue_dequeued_batches
            .fetch_add(1, Ordering::Relaxed);
        self.journal_queue_dequeued_rows
            .fetch_add(rows, Ordering::Relaxed);
    }

    fn note_journal_overflow_enqueued(&self, rows: usize) {
        self.journal_overflow_depth_batches
            .fetch_add(1, Ordering::Relaxed);
        self.journal_overflow_row_debt
            .fetch_add(rows, Ordering::Relaxed);
    }

    fn note_journal_overflow_dequeued(&self, rows: usize) {
        let _ = self.journal_overflow_depth_batches.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_sub(1)),
        );
        let _ = self.journal_overflow_row_debt.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_sub(rows)),
        );
    }

    fn note_journal_overflow_rows_coalesced(&self, rows: usize) {
        self.journal_overflow_row_debt
            .fetch_add(rows, Ordering::Relaxed);
    }

    fn note_journal_writer_inflight_started(&self, rows: usize) {
        self.journal_writer_inflight_rows
            .store(rows, Ordering::Relaxed);
    }

    fn note_journal_writer_inflight_finished(&self) {
        self.journal_writer_inflight_rows
            .store(0, Ordering::Relaxed);
    }

    fn note_journal_sqlite_contention_delta(
        &self,
        before: SqliteContentionSnapshot,
        after: SqliteContentionSnapshot,
    ) {
        self.journal_sqlite_write_retry_total.fetch_add(
            after
                .write_retry_total
                .saturating_sub(before.write_retry_total),
            Ordering::Relaxed,
        );
        self.journal_sqlite_busy_error_total.fetch_add(
            after
                .busy_error_total
                .saturating_sub(before.busy_error_total),
            Ordering::Relaxed,
        );
    }

    fn set_aggregate_gap_active(&self, active: bool) {
        self.aggregate_gap_active.store(active, Ordering::Relaxed);
    }

    fn aggregate_gap_active(&self) -> bool {
        self.aggregate_gap_active.load(Ordering::Relaxed)
    }

    fn set_aggregate_worker_busy(&self, busy: bool) {
        self.aggregate_worker_busy.store(busy, Ordering::Relaxed);
    }

    fn aggregate_worker_busy(&self) -> bool {
        self.aggregate_worker_busy.load(Ordering::Relaxed)
    }

    fn note_phase_sample(
        &self,
        samples_lock: &Mutex<VecDeque<u64>>,
        last_p95: &AtomicU64,
        duration_ms: u64,
    ) {
        if let Ok(mut samples) = samples_lock.lock() {
            if samples.len() >= OBSERVED_SWAP_WRITER_LATENCY_SAMPLE_CAPACITY {
                let _ = samples.pop_front();
            }
            samples.push_back(duration_ms);
            last_p95.store(percentile_from_deque(&samples, 0.95), Ordering::Relaxed);
        }
    }

    fn snapshot(&self) -> ObservedSwapWriterSnapshot {
        let write_latency_ms_p95 = self
            .write_latency_ms_samples
            .lock()
            .ok()
            .map(|samples| percentile_from_deque(&samples, 0.95))
            .unwrap_or_else(|| self.last_write_latency_ms_p95.load(Ordering::Relaxed));
        let raw_batch_write_ms_p95 = self
            .raw_batch_write_ms_samples
            .lock()
            .ok()
            .map(|samples| percentile_from_deque(&samples, 0.95))
            .unwrap_or_else(|| self.last_raw_batch_write_ms_p95.load(Ordering::Relaxed));
        let observed_swaps_insert_ms_p95 = self
            .observed_swaps_insert_ms_samples
            .lock()
            .ok()
            .map(|samples| percentile_from_deque(&samples, 0.95))
            .unwrap_or_else(|| {
                self.last_observed_swaps_insert_ms_p95
                    .load(Ordering::Relaxed)
            });
        let wallet_activity_days_ms_p95 = self
            .wallet_activity_days_ms_samples
            .lock()
            .ok()
            .map(|samples| percentile_from_deque(&samples, 0.95))
            .unwrap_or_else(|| {
                self.last_wallet_activity_days_ms_p95
                    .load(Ordering::Relaxed)
            });
        let discovery_scoring_ms_p95 = self
            .discovery_scoring_ms_samples
            .lock()
            .ok()
            .map(|samples| percentile_from_deque(&samples, 0.95))
            .unwrap_or_else(|| self.last_discovery_scoring_ms_p95.load(Ordering::Relaxed));
        let journal_enqueue_wait_ms_p95 = self
            .journal_enqueue_wait_ms_samples
            .lock()
            .ok()
            .map(|samples| percentile_from_deque(&samples, 0.95))
            .unwrap_or_else(|| {
                self.last_journal_enqueue_wait_ms_p95
                    .load(Ordering::Relaxed)
            });
        let journal_batch_write_ms_p95 = self
            .journal_batch_write_ms_samples
            .lock()
            .ok()
            .map(|samples| percentile_from_deque(&samples, 0.95))
            .unwrap_or_else(|| self.last_journal_batch_write_ms_p95.load(Ordering::Relaxed));
        let worker_busy_ms_p95 = self
            .worker_busy_ms_samples
            .lock()
            .ok()
            .map(|samples| percentile_from_deque(&samples, 0.95))
            .unwrap_or_else(|| self.last_worker_busy_ms_p95.load(Ordering::Relaxed));
        ObservedSwapWriterSnapshot {
            pending_requests: self.pending_requests.load(Ordering::Relaxed),
            write_latency_ms_p95,
            raw_batch_write_ms_p95,
            observed_swaps_insert_ms_p95,
            wallet_activity_days_ms_p95,
            discovery_scoring_ms_p95,
            journal_enqueue_wait_ms_p95,
            journal_batch_write_ms_p95,
            worker_busy_ms_p95,
            aggregate_queue_depth_batches: self
                .aggregate_queue_depth_batches
                .load(Ordering::Relaxed),
            aggregate_queue_capacity_batches: self
                .aggregate_queue_capacity_batches
                .load(Ordering::Relaxed),
            aggregate_overflow_depth_batches: self
                .aggregate_overflow_depth_batches
                .load(Ordering::Relaxed),
            aggregate_overflow_capacity_batches: self
                .aggregate_overflow_capacity_batches
                .load(Ordering::Relaxed),
            journal_queue_depth_batches: self.journal_queue_depth_batches(),
            journal_queue_row_debt: self.journal_queue_row_debt(),
            journal_queue_capacity_batches: self
                .journal_queue_capacity_batches
                .load(Ordering::Relaxed),
            journal_overflow_depth_batches: self
                .journal_overflow_depth_batches
                .load(Ordering::Relaxed),
            journal_overflow_row_debt: self.journal_overflow_row_debt.load(Ordering::Relaxed),
            journal_overflow_capacity_batches: self
                .journal_overflow_capacity_batches
                .load(Ordering::Relaxed),
            journal_overflow_row_debt_capacity: self
                .journal_overflow_row_debt_capacity
                .load(Ordering::Relaxed),
            journal_writer_inflight_rows: self.journal_writer_inflight_rows.load(Ordering::Relaxed),
            journal_sqlite_write_retry_total: self
                .journal_sqlite_write_retry_total
                .load(Ordering::Relaxed),
            journal_sqlite_busy_error_total: self
                .journal_sqlite_busy_error_total
                .load(Ordering::Relaxed),
        }
    }

    fn journal_queue_depth_batches(&self) -> usize {
        let enqueued = self.journal_queue_enqueued_batches.load(Ordering::Relaxed);
        let dequeued = self.journal_queue_dequeued_batches.load(Ordering::Relaxed);
        let depth = enqueued.saturating_sub(dequeued);
        let capacity = self.journal_queue_capacity_batches.load(Ordering::Relaxed);
        if capacity == 0 {
            depth
        } else {
            depth.min(capacity)
        }
    }

    fn journal_queue_row_debt(&self) -> usize {
        self.journal_queue_enqueued_rows
            .load(Ordering::Relaxed)
            .saturating_sub(self.journal_queue_dequeued_rows.load(Ordering::Relaxed))
    }
}

pub(crate) struct ObservedSwapWriter {
    sender: mpsc::Sender<ObservedSwapWriteRequest>,
    normal_try_enqueue_soft_limit: usize,
    raw_worker: Option<thread::JoinHandle<Result<()>>>,
    aggregate_worker: Option<thread::JoinHandle<Result<()>>>,
    journal_worker: Option<thread::JoinHandle<Result<()>>>,
    telemetry: Arc<ObservedSwapWriterTelemetry>,
    terminal_failure_message: Arc<Mutex<Option<String>>>,
}

fn observed_swap_writer_normal_try_enqueue_soft_limit(config: &ObservedSwapWriterConfig) -> usize {
    if let Some(limit) = config.normal_try_enqueue_soft_limit_override {
        return limit.max(1);
    }

    let discovery_critical_reserve_requests =
        observed_swap_writer_discovery_critical_reserve_requests(config);
    let normal_capacity = config
        .channel_capacity
        .saturating_sub(discovery_critical_reserve_requests)
        .max(1);
    if config.aggregate_writes_enabled {
        return normal_capacity;
    }

    // `try_enqueue()` is only used by non-critical irrelevant swaps. When aggregate writes
    // are disabled, one queued raw batch is enough to preserve best-effort persistence without
    // letting this lowest-priority class consume the entire normal writer budget before
    // backpressure starts.
    let noncritical_irrelevant_budget =
        OBSERVED_SWAP_WRITER_NONCRITICAL_IRRELEVANT_MAX_QUEUED_BATCHES
            .saturating_mul(config.batch_max_size.max(1))
            .max(1);

    noncritical_irrelevant_budget.min(normal_capacity)
}

impl ObservedSwapWriter {
    fn terminal_failure_error(&self) -> Option<anyhow::Error> {
        self.terminal_failure_message
            .lock()
            .ok()
            .and_then(|message| message.clone())
            .map(|message| anyhow!(message).context(OBSERVED_SWAP_WRITER_TERMINAL_FAILURE_CONTEXT))
    }

    pub(crate) fn ensure_running(&self) -> Result<()> {
        if let Some(error) = self.terminal_failure_error() {
            return Err(error);
        }
        Ok(())
    }

    async fn send_request(&self, request: ObservedSwapWriteRequest) -> Result<()> {
        self.ensure_running()?;
        let permit = self
            .sender
            .reserve()
            .await
            .context(OBSERVED_SWAP_WRITER_CHANNEL_CLOSED_CONTEXT)?;
        self.telemetry.note_enqueued();
        permit.send(request);
        Ok(())
    }

    pub(crate) fn start(
        sqlite_path: String,
        aggregate_writes_enabled: bool,
        aggregate_write_config: DiscoveryAggregateWriteConfig,
    ) -> Result<Self> {
        Self::start_with_recent_raw_journal(
            sqlite_path,
            aggregate_writes_enabled,
            aggregate_write_config,
            None,
        )
    }

    pub(crate) fn start_with_recent_raw_journal(
        sqlite_path: String,
        aggregate_writes_enabled: bool,
        aggregate_write_config: DiscoveryAggregateWriteConfig,
        recent_raw_journal: Option<ObservedSwapRecentRawJournalConfig>,
    ) -> Result<Self> {
        Self::start_with_config(
            sqlite_path,
            ObservedSwapWriterConfig::production(
                aggregate_writes_enabled,
                aggregate_write_config,
                recent_raw_journal,
            ),
        )
    }

    fn start_with_config(sqlite_path: String, config: ObservedSwapWriterConfig) -> Result<Self> {
        let (sender, receiver) = mpsc::channel(config.channel_capacity);
        let telemetry = Arc::new(ObservedSwapWriterTelemetry::default());
        let terminal_failure_message = Arc::new(Mutex::new(None));
        let normal_try_enqueue_soft_limit =
            observed_swap_writer_normal_try_enqueue_soft_limit(&config);
        let aggregate_queue_capacity_batches =
            observed_swap_writer_aggregate_queue_capacity(&config);
        telemetry
            .aggregate_queue_capacity_batches
            .store(aggregate_queue_capacity_batches, Ordering::Relaxed);
        telemetry.aggregate_overflow_capacity_batches.store(
            config.aggregate_overflow_capacity_batches,
            Ordering::Relaxed,
        );
        let journal_queue_capacity_batches = config
            .recent_raw_journal
            .as_ref()
            .map(|journal| journal.writer_queue_capacity_batches.max(1))
            .unwrap_or(0);
        telemetry
            .journal_queue_capacity_batches
            .store(journal_queue_capacity_batches, Ordering::Relaxed);
        let journal_overflow_capacity_batches = config
            .recent_raw_journal
            .as_ref()
            .map(|journal| journal.overflow_capacity_batches)
            .unwrap_or(0);
        telemetry
            .journal_overflow_capacity_batches
            .store(journal_overflow_capacity_batches, Ordering::Relaxed);
        let journal_overflow_row_capacity = config
            .recent_raw_journal
            .as_ref()
            .map(|journal| recent_raw_journal_overflow_row_capacity(config.batch_max_size, journal))
            .unwrap_or(0);
        telemetry
            .journal_overflow_row_debt_capacity
            .store(journal_overflow_row_capacity, Ordering::Relaxed);
        let aggregate_channel = config.aggregate_writes_enabled.then(|| {
            std_mpsc::sync_channel::<DiscoveryAggregateWriteRequest>(
                aggregate_queue_capacity_batches,
            )
        });
        let aggregate_sender = aggregate_channel.as_ref().map(|(sender, _)| sender.clone());
        let aggregate_receiver = aggregate_channel.map(|(_, receiver)| receiver);
        let aggregate_startup_channel = config
            .aggregate_writes_enabled
            .then(std_mpsc::channel::<std::result::Result<(), String>>);
        let aggregate_startup_sender = aggregate_startup_channel
            .as_ref()
            .map(|(sender, _)| sender.clone());
        let aggregate_startup_receiver = aggregate_startup_channel.map(|(_, receiver)| receiver);
        let journal_channel = config.recent_raw_journal.as_ref().map(|journal| {
            std_mpsc::sync_channel::<RecentRawJournalWriteRequest>(
                journal.writer_queue_capacity_batches.max(1),
            )
        });
        let journal_sender = journal_channel.as_ref().map(|(sender, _)| sender.clone());
        let journal_receiver = journal_channel.map(|(_, receiver)| receiver);
        let journal_startup_channel = config
            .recent_raw_journal
            .as_ref()
            .map(|_| std_mpsc::channel::<std::result::Result<(), String>>());
        let journal_startup_sender = journal_startup_channel
            .as_ref()
            .map(|(sender, _)| sender.clone());
        let journal_startup_receiver = journal_startup_channel.map(|(_, receiver)| receiver);
        let raw_worker_config = config.clone();
        let raw_worker_sqlite_path = sqlite_path.clone();

        let raw_worker_telemetry = Arc::clone(&telemetry);
        let raw_worker_terminal_failure_message = Arc::clone(&terminal_failure_message);
        let raw_worker = thread::Builder::new()
            .name("copybot-observed-swap-writer".to_string())
            .spawn(move || {
                let result = observed_swap_writer_loop(
                    raw_worker_sqlite_path,
                    receiver,
                    aggregate_sender,
                    aggregate_startup_receiver,
                    journal_sender,
                    journal_startup_receiver,
                    raw_worker_config,
                    raw_worker_telemetry,
                    Arc::clone(&raw_worker_terminal_failure_message),
                );
                if let Err(error) = &result {
                    set_terminal_failure_message(
                        &raw_worker_terminal_failure_message,
                        format!("{error:#}"),
                    );
                }
                result
            })
            .context("failed to spawn observed swap writer thread")?;

        let aggregate_worker = if let Some(receiver) = aggregate_receiver {
            let aggregate_worker_telemetry = Arc::clone(&telemetry);
            let aggregate_worker_terminal_failure_message = Arc::clone(&terminal_failure_message);
            let aggregate_sqlite_path = sqlite_path.clone();
            let aggregate_worker_config = config.clone();
            let startup_sender = aggregate_startup_sender
                .ok_or_else(|| anyhow!("missing discovery aggregate startup sender"))?;
            Some(
                thread::Builder::new()
                    .name("copybot-discovery-aggregate-writer".to_string())
                    .spawn(move || {
                        let result = discovery_aggregate_writer_loop(
                            aggregate_sqlite_path,
                            receiver,
                            startup_sender,
                            aggregate_worker_config,
                            aggregate_worker_telemetry,
                        );
                        if let Err(error) = &result {
                            set_terminal_failure_message(
                                &aggregate_worker_terminal_failure_message,
                                format!("{error:#}"),
                            );
                        }
                        result
                    })
                    .context("failed to spawn discovery aggregate writer thread")?,
            )
        } else {
            None
        };

        let journal_worker = if let Some(receiver) = journal_receiver {
            let journal_worker_telemetry = Arc::clone(&telemetry);
            let journal_worker_terminal_failure_message = Arc::clone(&terminal_failure_message);
            let journal_config = config
                .recent_raw_journal
                .clone()
                .ok_or_else(|| anyhow!("missing recent raw journal config"))?;
            let startup_sender = journal_startup_sender
                .ok_or_else(|| anyhow!("missing recent raw journal startup sender"))?;
            Some(
                thread::Builder::new()
                    .name("copybot-recent-raw-journal-writer".to_string())
                    .spawn(move || {
                        let result = recent_raw_journal_writer_loop(
                            receiver,
                            startup_sender,
                            journal_config,
                            journal_worker_telemetry,
                        );
                        if let Err(error) = &result {
                            set_terminal_failure_message(
                                &journal_worker_terminal_failure_message,
                                format!("{error:#}"),
                            );
                        }
                        result
                    })
                    .context("failed to spawn recent raw journal writer thread")?,
            )
        } else {
            None
        };

        Ok(Self {
            sender,
            normal_try_enqueue_soft_limit,
            raw_worker: Some(raw_worker),
            aggregate_worker,
            journal_worker,
            telemetry,
            terminal_failure_message,
        })
    }

    #[cfg(test)]
    pub(crate) fn start_for_test(
        sqlite_path: String,
        channel_capacity: usize,
        batch_max_size: usize,
        aggregate_writes_enabled: bool,
        aggregate_write_config: DiscoveryAggregateWriteConfig,
    ) -> Result<Self> {
        Self::start_with_config(
            sqlite_path,
            ObservedSwapWriterConfig::for_test(
                channel_capacity,
                batch_max_size,
                aggregate_writes_enabled,
                aggregate_write_config,
                None,
            ),
        )
    }

    #[cfg(test)]
    pub(crate) fn start_for_test_with_normal_try_enqueue_soft_limit(
        sqlite_path: String,
        channel_capacity: usize,
        batch_max_size: usize,
        aggregate_writes_enabled: bool,
        aggregate_write_config: DiscoveryAggregateWriteConfig,
        normal_try_enqueue_soft_limit: usize,
    ) -> Result<Self> {
        Self::start_with_config(
            sqlite_path,
            ObservedSwapWriterConfig::for_test(
                channel_capacity,
                batch_max_size,
                aggregate_writes_enabled,
                aggregate_write_config,
                None,
            )
            .with_normal_try_enqueue_soft_limit(normal_try_enqueue_soft_limit),
        )
    }

    #[allow(dead_code)]
    pub(crate) async fn enqueue(&self, swap: &SwapEvent) -> Result<()> {
        self.send_request(ObservedSwapWriteRequest {
            swap: swap.clone(),
            reply_tx: None,
            enqueued_at: Instant::now(),
        })
        .await
    }

    #[allow(dead_code)]
    pub(crate) fn try_enqueue(&self, swap: &SwapEvent) -> Result<bool> {
        if self.telemetry.pending_requests.load(Ordering::Relaxed)
            >= self.normal_try_enqueue_soft_limit
        {
            return Ok(false);
        }
        self.try_enqueue_without_soft_limit(swap)
    }

    pub(crate) fn try_enqueue_discovery_critical(&self, swap: &SwapEvent) -> Result<bool> {
        self.try_enqueue_without_soft_limit(swap)
    }

    fn try_enqueue_without_soft_limit(&self, swap: &SwapEvent) -> Result<bool> {
        self.ensure_running()?;
        match self.sender.try_reserve() {
            Ok(permit) => {
                self.telemetry.note_enqueued();
                permit.send(ObservedSwapWriteRequest {
                    swap: swap.clone(),
                    reply_tx: None,
                    enqueued_at: Instant::now(),
                });
                Ok(true)
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => Ok(false),
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                Err(anyhow!(OBSERVED_SWAP_WRITER_CHANNEL_CLOSED_CONTEXT))
                    .context(OBSERVED_SWAP_WRITER_CHANNEL_CLOSED_CONTEXT)
            }
        }
    }

    pub(crate) async fn write(&self, swap: &SwapEvent) -> Result<bool> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.send_request(ObservedSwapWriteRequest {
            swap: swap.clone(),
            reply_tx: Some(reply_tx),
            enqueued_at: Instant::now(),
        })
        .await?;
        reply_rx
            .await
            .context(OBSERVED_SWAP_WRITER_REPLY_CLOSED_CONTEXT)?
    }

    pub(crate) fn snapshot(&self) -> ObservedSwapWriterSnapshot {
        self.telemetry.snapshot()
    }

    pub(crate) fn health_handle(&self) -> ObservedSwapWriterHealthHandle {
        ObservedSwapWriterHealthHandle {
            telemetry: Arc::clone(&self.telemetry),
        }
    }

    pub(crate) fn shutdown(mut self) -> Result<()> {
        drop(self.sender);
        let raw_result = if let Some(raw_worker) = self.raw_worker.take() {
            Some(
                raw_worker
                    .join()
                    .map_err(|payload| anyhow!(panic_payload_to_string(payload.as_ref())))
                    .context("observed swap writer thread panicked")?,
            )
        } else {
            None
        };
        let aggregate_result = if let Some(aggregate_worker) = self.aggregate_worker.take() {
            Some(
                aggregate_worker
                    .join()
                    .map_err(|payload| anyhow!(panic_payload_to_string(payload.as_ref())))
                    .context("discovery aggregate writer thread panicked")?,
            )
        } else {
            None
        };
        let journal_result = if let Some(journal_worker) = self.journal_worker.take() {
            Some(
                journal_worker
                    .join()
                    .map_err(|payload| anyhow!(panic_payload_to_string(payload.as_ref())))
                    .context("recent raw journal writer thread panicked")?,
            )
        } else {
            None
        };
        if let Some(result) = raw_result {
            result.context("observed swap writer thread failed")?;
        }
        if let Some(result) = aggregate_result {
            result.context("discovery aggregate writer thread failed")?;
        }
        if let Some(result) = journal_result {
            result.context("recent raw journal writer thread failed")?;
        }
        Ok(())
    }
}

fn poll_observed_swap_writer_downstream_startups(
    aggregate_startup_receiver: &mut Option<std_mpsc::Receiver<std::result::Result<(), String>>>,
    journal_startup_receiver: &mut Option<std_mpsc::Receiver<std::result::Result<(), String>>>,
) -> Result<()> {
    poll_observed_swap_writer_startup_receiver(
        aggregate_startup_receiver,
        "observed swap writer stopping after aggregate startup replay failure",
        "discovery aggregate writer startup channel closed",
        "observed swap writer stopping after aggregate startup replay channel closed",
    )?;
    poll_observed_swap_writer_startup_receiver(
        journal_startup_receiver,
        "observed swap writer stopping after recent raw journal startup failure",
        "recent raw journal writer startup channel closed",
        "observed swap writer stopping after recent raw journal startup channel closed",
    )
}

fn poll_observed_swap_writer_startup_receiver(
    startup_receiver: &mut Option<std_mpsc::Receiver<std::result::Result<(), String>>>,
    failure_context: &'static str,
    closed_message: &'static str,
    closed_context: &'static str,
) -> Result<()> {
    let poll_result = match startup_receiver.as_ref() {
        Some(receiver) => receiver.try_recv(),
        None => return Ok(()),
    };
    match poll_result {
        Ok(Ok(())) => {
            *startup_receiver = None;
            Ok(())
        }
        Ok(Err(message)) => Err(anyhow!(message)).context(failure_context),
        Err(std_mpsc::TryRecvError::Empty) => Ok(()),
        Err(std_mpsc::TryRecvError::Disconnected) => {
            Err(anyhow!("{closed_message}: receiving on a closed channel")).context(closed_context)
        }
    }
}

fn observed_swap_writer_downstream_startup_pending(
    aggregate_startup_receiver: &Option<std_mpsc::Receiver<std::result::Result<(), String>>>,
    journal_startup_receiver: &Option<std_mpsc::Receiver<std::result::Result<(), String>>>,
) -> bool {
    aggregate_startup_receiver.is_some() || journal_startup_receiver.is_some()
}

fn observed_swap_writer_loop(
    sqlite_path: String,
    mut receiver: mpsc::Receiver<ObservedSwapWriteRequest>,
    aggregate_sender: Option<std_mpsc::SyncSender<DiscoveryAggregateWriteRequest>>,
    aggregate_startup_receiver: Option<std_mpsc::Receiver<std::result::Result<(), String>>>,
    journal_sender: Option<std_mpsc::SyncSender<RecentRawJournalWriteRequest>>,
    journal_startup_receiver: Option<std_mpsc::Receiver<std::result::Result<(), String>>>,
    config: ObservedSwapWriterConfig,
    telemetry: Arc<ObservedSwapWriterTelemetry>,
    terminal_failure_message: Arc<Mutex<Option<String>>>,
) -> Result<()> {
    let store = SqliteStore::open(Path::new(&sqlite_path)).with_context(|| {
        format!("failed to open sqlite db for observed swap writer: {sqlite_path}")
    })?;
    let mut aggregate_overflow =
        VecDeque::with_capacity(config.aggregate_overflow_capacity_batches);
    let journal_overflow_capacity_batches = config
        .recent_raw_journal
        .as_ref()
        .map(|journal| journal.overflow_capacity_batches)
        .unwrap_or(0);
    let journal_overflow_row_capacity = config
        .recent_raw_journal
        .as_ref()
        .map(|journal| recent_raw_journal_overflow_row_capacity(config.batch_max_size, journal))
        .unwrap_or(0);
    let journal_overflow_drain_coalesce_max_batches = config
        .recent_raw_journal
        .as_ref()
        .map(recent_raw_journal_adaptive_write_coalesce_max_batches)
        .unwrap_or(1);
    let mut journal_overflow = VecDeque::with_capacity(journal_overflow_capacity_batches);
    let mut aggregate_startup_receiver = aggregate_startup_receiver;
    let mut journal_startup_receiver = journal_startup_receiver;

    loop {
        poll_observed_swap_writer_downstream_startups(
            &mut aggregate_startup_receiver,
            &mut journal_startup_receiver,
        )?;

        let first_request = match receiver.try_recv() {
            Ok(request) => request,
            Err(mpsc::error::TryRecvError::Empty) => {
                if let Some(aggregate_sender) = aggregate_sender.as_ref() {
                    if !aggregate_overflow.is_empty() && aggregate_startup_receiver.is_none() {
                        flush_discovery_aggregate_overflow_blocking(
                            aggregate_sender,
                            &mut aggregate_overflow,
                            &telemetry,
                        )?;
                        continue;
                    }
                }
                if let Some(journal_sender) = journal_sender.as_ref() {
                    if !journal_overflow.is_empty() && journal_startup_receiver.is_none() {
                        drain_recent_raw_journal_overflow_nonblocking(
                            journal_sender,
                            &mut journal_overflow,
                            journal_overflow_drain_coalesce_max_batches,
                            &telemetry,
                        )?;
                        if journal_overflow.is_empty() {
                            continue;
                        }
                        thread::sleep(OBSERVED_SWAP_DISCOVERY_AGGREGATE_IDLE_REPLAY_POLL_INTERVAL);
                        continue;
                    }
                }
                if observed_swap_writer_downstream_startup_pending(
                    &aggregate_startup_receiver,
                    &journal_startup_receiver,
                ) {
                    thread::sleep(OBSERVED_SWAP_DISCOVERY_AGGREGATE_IDLE_REPLAY_POLL_INTERVAL);
                    continue;
                }
                match receiver.blocking_recv() {
                    Some(request) => request,
                    None => break,
                }
            }
            Err(mpsc::error::TryRecvError::Disconnected) => break,
        };
        let mut batch = vec![first_request];
        while batch.len() < config.batch_max_size {
            match receiver.try_recv() {
                Ok(request) => batch.push(request),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }

        let mut swaps = Vec::with_capacity(batch.len());
        let mut replies = Vec::with_capacity(batch.len());
        let mut queued_at = Vec::with_capacity(batch.len());
        for request in batch {
            swaps.push(request.swap);
            replies.push(request.reply_tx);
            queued_at.push(request.enqueued_at);
        }

        if let Some(message) = load_terminal_failure_message(&terminal_failure_message) {
            for reply_tx in replies {
                if let Some(reply_tx) = reply_tx {
                    let _ = reply_tx.send(Err(anyhow!(message.clone())));
                }
            }
            telemetry.note_batch_completed(&queued_at);
            return Err(anyhow!(message))
                .context("observed swap writer stopping after aggregate worker terminal failure");
        }

        let batch_started = Instant::now();
        let mut retryable_lock_started_at: Option<Instant> = None;
        let mut last_retryable_lock_log_at: Option<Instant> = None;
        let mut retryable_lock_attempts = 0u64;
        loop {
            if let Some(message) = load_terminal_failure_message(&terminal_failure_message) {
                for reply_tx in replies {
                    if let Some(reply_tx) = reply_tx {
                        let _ = reply_tx.send(Err(anyhow!(message.clone())));
                    }
                }
                telemetry.note_batch_completed(&queued_at);
                return Err(anyhow!(message)).context(
                    "observed swap writer stopping after aggregate worker terminal failure while raw batch was pending",
                );
            }

            let raw_batch_started = Instant::now();
            match store.insert_observed_swaps_batch_with_activity_days_measured(&swaps) {
                Ok(batch_metrics) => {
                    telemetry
                        .note_raw_batch_completed(elapsed_ms_ceil(raw_batch_started.elapsed()));
                    telemetry.note_observed_swaps_insert_completed(
                        batch_metrics.observed_swaps_insert_ms,
                    );
                    telemetry.note_wallet_activity_days_completed(
                        batch_metrics.wallet_activity_days_upsert_ms,
                    );
                    if let Some(retryable_lock_started_at) = retryable_lock_started_at {
                        info!(
                            batch_swaps = swaps.len(),
                            retryable_lock_attempts,
                            retryable_lock_elapsed_ms =
                                elapsed_ms_ceil(retryable_lock_started_at.elapsed()),
                            "observed swap writer recovered after retryable sqlite lock pressure"
                        );
                    }
                    let results = batch_metrics.inserted;
                    for (reply_tx, inserted) in replies.into_iter().zip(results.iter().copied()) {
                        if let Some(reply_tx) = reply_tx {
                            let _ = reply_tx.send(Ok(inserted));
                        }
                    }
                    telemetry.note_batch_completed(&queued_at);
                    let inserted_swaps: Vec<SwapEvent> = swaps
                        .iter()
                        .zip(results.iter())
                        .filter_map(|(swap, inserted)| inserted.then_some(swap.clone()))
                        .collect();
                    if let Some(aggregate_sender) = aggregate_sender.as_ref() {
                        if !inserted_swaps.is_empty() {
                            let aggregate_request = DiscoveryAggregateWriteRequest {
                                inserted_swaps: inserted_swaps.clone(),
                                batch_started,
                            };
                            let aggregate_outcome = if aggregate_startup_receiver.is_some()
                                && config.aggregate_gap_fallback_enabled
                                && store
                                    .load_discovery_scoring_covered_through_cursor()?
                                    .is_some()
                            {
                                Ok(
                                    DiscoveryAggregateEnqueueOutcome::DeferredToMaterializationGap(
                                        aggregate_request,
                                    ),
                                )
                            } else {
                                enqueue_discovery_aggregate_request(
                                    aggregate_sender,
                                    &mut aggregate_overflow,
                                    config.aggregate_overflow_capacity_batches,
                                    config.aggregate_gap_fallback_enabled,
                                    aggregate_request,
                                    &telemetry,
                                )
                            };
                            match aggregate_outcome {
                                Ok(DiscoveryAggregateEnqueueOutcome::Enqueued) => {}
                                Ok(
                                    DiscoveryAggregateEnqueueOutcome::DeferredToMaterializationGap(
                                        request,
                                    ),
                                ) => {
                                    if let Err(error) =
                                        latch_discovery_scoring_materialization_gap_from_swaps(
                                            &store,
                                            &request.inserted_swaps,
                                        )
                                    {
                                        telemetry.note_worker_busy_completed(elapsed_ms_ceil(
                                            batch_started.elapsed(),
                                        ));
                                        return Err(error).context(
                                            "observed swap writer stopping after aggregate backpressure gap latching failed",
                                        );
                                    }
                                    telemetry.set_aggregate_gap_active(true);
                                }
                                Err(error) => {
                                    telemetry.note_worker_busy_completed(elapsed_ms_ceil(
                                        batch_started.elapsed(),
                                    ));
                                    return Err(anyhow!(
                                        "discovery aggregate writer channel closed: {}",
                                        error
                                    ))
                                    .context(
                                        "observed swap writer stopping after aggregate writer channel closed",
                                    );
                                }
                            }
                        }
                    }
                    if let Some(journal_sender) = journal_sender.as_ref() {
                        if !inserted_swaps.is_empty() {
                            let journal_enqueue_started = Instant::now();
                            if let Err(error) = enqueue_recent_raw_journal_request(
                                journal_sender,
                                &mut journal_overflow,
                                journal_overflow_capacity_batches,
                                journal_overflow_row_capacity,
                                journal_overflow_drain_coalesce_max_batches,
                                RecentRawJournalWriteRequest { inserted_swaps },
                                &telemetry,
                            ) {
                                telemetry.note_journal_enqueue_wait_completed(elapsed_ms_ceil(
                                    journal_enqueue_started.elapsed(),
                                ));
                                telemetry.note_worker_busy_completed(elapsed_ms_ceil(
                                    batch_started.elapsed(),
                                ));
                                return Err(error).context(
                                    "observed swap writer stopping after recent raw journal enqueue failure",
                                );
                            }
                            telemetry.note_journal_enqueue_wait_completed(elapsed_ms_ceil(
                                journal_enqueue_started.elapsed(),
                            ));
                        }
                    }
                    telemetry.note_worker_busy_completed(elapsed_ms_ceil(batch_started.elapsed()));
                    break;
                }
                Err(error) => {
                    telemetry
                        .note_raw_batch_completed(elapsed_ms_ceil(raw_batch_started.elapsed()));
                    if is_retryable_sqlite_anyhow_error(&error) {
                        retryable_lock_attempts = retryable_lock_attempts.saturating_add(1);
                        let retryable_lock_started_at =
                            *retryable_lock_started_at.get_or_insert_with(Instant::now);
                        let should_log = last_retryable_lock_log_at
                            .map(|logged_at| {
                                logged_at.elapsed()
                                    >= OBSERVED_SWAP_WRITER_RETRYABLE_LOCK_LOG_INTERVAL
                            })
                            .unwrap_or(true);
                        if should_log {
                            let contention = sqlite_contention_snapshot();
                            warn!(
                                error = %error,
                                batch_swaps = swaps.len(),
                                retryable_lock_attempts,
                                retryable_lock_elapsed_ms =
                                    elapsed_ms_ceil(retryable_lock_started_at.elapsed()),
                                sqlite_write_retry_total = contention.write_retry_total,
                                sqlite_busy_error_total = contention.busy_error_total,
                                "observed swap writer raw batch blocked by retryable sqlite lock; keeping writer alive and retrying"
                            );
                            last_retryable_lock_log_at = Some(Instant::now());
                        }
                        thread::sleep(OBSERVED_SWAP_WRITER_RETRYABLE_LOCK_BACKOFF);
                        continue;
                    }

                    telemetry.note_worker_busy_completed(elapsed_ms_ceil(batch_started.elapsed()));
                    let message = format!("{error:#}");
                    warn!(
                        error = %error,
                        batch_swaps = swaps.len(),
                        "failed to insert observed swap batch with activity days"
                    );
                    for reply_tx in replies {
                        if let Some(reply_tx) = reply_tx {
                            let _ = reply_tx.send(Err(anyhow!(message.clone())));
                        }
                    }
                    telemetry.note_batch_completed(&queued_at);
                    return Err(anyhow!(message))
                        .context("observed swap writer stopping after raw batch insert failure");
                }
            }
        }
    }

    if let Some(aggregate_sender) = aggregate_sender.as_ref() {
        flush_discovery_aggregate_overflow_blocking(
            aggregate_sender,
            &mut aggregate_overflow,
            &telemetry,
        )?;
    }
    if let Some(journal_sender) = journal_sender.as_ref() {
        flush_recent_raw_journal_overflow_blocking(
            journal_sender,
            &mut journal_overflow,
            &telemetry,
        )?;
    }

    Ok(())
}

fn discovery_aggregate_writer_loop(
    sqlite_path: String,
    receiver: std_mpsc::Receiver<DiscoveryAggregateWriteRequest>,
    startup_sender: std_mpsc::Sender<std::result::Result<(), String>>,
    config: ObservedSwapWriterConfig,
    telemetry: Arc<ObservedSwapWriterTelemetry>,
) -> Result<()> {
    let store = SqliteStore::open(Path::new(&sqlite_path)).with_context(|| {
        format!("failed to open sqlite db for discovery aggregate writer: {sqlite_path}")
    })?;
    match run_aggregate_startup_replay(&store, &config) {
        Ok(()) => {
            telemetry.set_aggregate_gap_active(
                store
                    .load_discovery_scoring_materialization_gap_cursor()?
                    .is_some(),
            );
            let _ = startup_sender.send(Ok(()));
        }
        Err(error) => {
            let _ = startup_sender.send(Err(format!("{error:#}")));
            return Err(error);
        }
    }
    let mut gap_repair_epoch = DiscoveryAggregateGapRepairEpoch::default();

    loop {
        run_discovery_aggregate_gap_repair_slice(
            &sqlite_path,
            &store,
            &config,
            &telemetry,
            true,
            &mut gap_repair_epoch,
        )?;

        match receiver.recv_timeout(OBSERVED_SWAP_DISCOVERY_AGGREGATE_IDLE_REPLAY_POLL_INTERVAL) {
            Ok(request) => {
                let request = collect_discovery_aggregate_write_batch(
                    &receiver,
                    request,
                    config.aggregate_write_coalesce_max_batches,
                    &telemetry,
                );
                telemetry.set_aggregate_worker_busy(true);
                let result = process_discovery_aggregate_write_request(
                    &store,
                    &request.inserted_swaps,
                    request.batch_started,
                    &config.aggregate_write_config,
                    &telemetry,
                );
                telemetry.set_aggregate_worker_busy(false);
                result?;
            }
            Err(std_mpsc::RecvTimeoutError::Timeout) => {
                run_discovery_aggregate_gap_repair_slice(
                    &sqlite_path,
                    &store,
                    &config,
                    &telemetry,
                    false,
                    &mut gap_repair_epoch,
                )?;
            }
            Err(std_mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    Ok(())
}

fn enqueue_recent_raw_journal_request(
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

fn coalesce_recent_raw_journal_overflow_tail(
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

fn ensure_recent_raw_journal_overflow_row_capacity(
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

fn recent_raw_journal_overflow_row_debt(
    journal_overflow: &VecDeque<RecentRawJournalWriteRequest>,
) -> usize {
    journal_overflow
        .iter()
        .map(RecentRawJournalWriteRequest::row_count)
        .sum()
}

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

fn drain_recent_raw_journal_overflow_nonblocking(
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

struct RecentRawJournalOverflowDrainBatch {
    request: RecentRawJournalWriteRequest,
    request_batches: usize,
}

fn collect_recent_raw_journal_overflow_drain_batch(
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

fn flush_recent_raw_journal_overflow_blocking(
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

fn log_recent_raw_journal_phase(
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

#[cfg(test)]
fn clear_recent_raw_journal_phase_events_for_test() {
    if let Ok(mut events) = RECENT_RAW_JOURNAL_PHASE_EVENTS_FOR_TEST.lock() {
        events.clear();
    }
}

#[cfg(test)]
fn recent_raw_journal_phase_events_for_test() -> Vec<&'static str> {
    RECENT_RAW_JOURNAL_PHASE_EVENTS_FOR_TEST
        .lock()
        .map(|events| events.clone())
        .unwrap_or_default()
}

fn recent_raw_journal_writer_loop(
    receiver: std_mpsc::Receiver<RecentRawJournalWriteRequest>,
    startup_sender: std_mpsc::Sender<std::result::Result<(), String>>,
    config: ObservedSwapRecentRawJournalConfig,
    telemetry: Arc<ObservedSwapWriterTelemetry>,
) -> Result<()> {
    let journal_path = Path::new(&config.sqlite_path);
    if let Some(parent) = journal_path.parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed creating recent raw journal parent directory {}",
                parent.display()
            )
        })?;
    }
    let store = SqliteStore::open(journal_path).with_context(|| {
        format!(
            "failed to open sqlite db for recent raw journal writer: {}",
            journal_path.display()
        )
    })?;
    match store.ensure_recent_raw_journal_tables() {
        Ok(()) => {
            if !config.skip_startup_prune {
                debug!(
                    retention_days = config.retention_days,
                    "recent raw journal startup prune deferred until post-write bounded prune"
                );
            }
            let _ = startup_sender.send(Ok(()));
        }
        Err(error) => {
            let _ = startup_sender.send(Err(format!("{error:#}")));
            return Err(error);
        }
    }

    let adaptive_coalesce_max_batches =
        recent_raw_journal_adaptive_write_coalesce_max_batches(&config);
    let adaptive_coalesce_max_rows = recent_raw_journal_adaptive_write_coalesce_max_rows(&config);
    while let Ok(request) = receiver.recv() {
        let collected_batch = collect_recent_raw_journal_write_batch(
            &receiver,
            request,
            config.write_coalesce_max_batches,
            adaptive_coalesce_max_batches,
            adaptive_coalesce_max_rows,
            &telemetry,
        );
        let inserted_swaps = collected_batch.inserted_swaps;
        let journal_batch_started = Instant::now();
        let contention_before = sqlite_contention_snapshot();
        let completed_at = Utc::now();
        telemetry.note_journal_writer_inflight_started(inserted_swaps.len());
        log_recent_raw_journal_phase(
            RECENT_RAW_JOURNAL_PHASE_BATCH_COLLECTED,
            None,
            inserted_swaps.len(),
            collected_batch.request_batches,
            collected_batch.coalesce_elapsed_ms,
            collected_batch.coalesce_limit_rows,
            collected_batch.coalesce_elapsed_ms,
            &telemetry,
        );
        let write_result = (|| -> Result<()> {
            write_recent_raw_journal_batch_with_deadline(
                &store,
                &config,
                &telemetry,
                &inserted_swaps,
                collected_batch.request_batches,
                collected_batch.coalesce_elapsed_ms,
                collected_batch.coalesce_limit_rows,
                completed_at,
            )
        })();
        telemetry.note_journal_writer_inflight_finished();
        write_result?;
        let contention_after = sqlite_contention_snapshot();
        telemetry.note_journal_sqlite_contention_delta(contention_before, contention_after);
        telemetry
            .note_journal_batch_write_completed(elapsed_ms_ceil(journal_batch_started.elapsed()));
    }

    Ok(())
}

fn write_recent_raw_journal_batch_with_deadline(
    store: &SqliteStore,
    config: &ObservedSwapRecentRawJournalConfig,
    telemetry: &ObservedSwapWriterTelemetry,
    inserted_swaps: &[SwapEvent],
    request_batches: usize,
    coalesce_elapsed_ms: u64,
    coalesce_limit_rows: usize,
    completed_at: DateTime<Utc>,
) -> Result<()> {
    let batch_phase_started = Instant::now();
    let batch_rows = inserted_swaps.len();
    log_recent_raw_journal_phase(
        RECENT_RAW_JOURNAL_PHASE_WRITE_START,
        None,
        batch_rows,
        request_batches,
        coalesce_elapsed_ms,
        coalesce_limit_rows,
        0,
        telemetry,
    );
    let write_started = Instant::now();
    let write_result = write_recent_raw_journal_batch_with_deadline_attempts(
        telemetry,
        inserted_swaps,
        || Instant::now() + OBSERVED_SWAP_RECENT_RAW_JOURNAL_WRITE_DEADLINE,
        |suffix, deadline| {
            store.insert_recent_raw_journal_batch_bulk_with_deadline(suffix, completed_at, deadline)
        },
    );
    log_recent_raw_journal_phase(
        RECENT_RAW_JOURNAL_PHASE_WRITE_END,
        None,
        batch_rows,
        request_batches,
        coalesce_elapsed_ms,
        coalesce_limit_rows,
        elapsed_ms_ceil(write_started.elapsed()),
        telemetry,
    );
    write_result?;

    let prune_check_started = Instant::now();
    log_recent_raw_journal_phase(
        RECENT_RAW_JOURNAL_PHASE_PRUNE_CHECK_START,
        None,
        batch_rows,
        request_batches,
        coalesce_elapsed_ms,
        coalesce_limit_rows,
        0,
        telemetry,
    );
    let prune_skip_reason = if config.skip_prune_while_backlogged {
        Some(RECENT_RAW_JOURNAL_HOT_WRITER_PRUNE_DEFERRED)
    } else {
        recent_raw_journal_prune_backlog_skip_reason(config, telemetry)
    };
    let prune_due = if prune_skip_reason.is_some() {
        false
    } else {
        recent_raw_journal_prune_due(store, config, telemetry, completed_at)?
    };
    log_recent_raw_journal_phase(
        RECENT_RAW_JOURNAL_PHASE_PRUNE_CHECK_END,
        prune_skip_reason,
        batch_rows,
        request_batches,
        coalesce_elapsed_ms,
        coalesce_limit_rows,
        elapsed_ms_ceil(prune_check_started.elapsed()),
        telemetry,
    );
    if prune_due {
        let prune_started = Instant::now();
        log_recent_raw_journal_phase(
            RECENT_RAW_JOURNAL_PHASE_PRUNE_START,
            None,
            batch_rows,
            request_batches,
            coalesce_elapsed_ms,
            coalesce_limit_rows,
            0,
            telemetry,
        );
        let _summary =
            prune_recent_raw_journal_with_budget(store, config.retention_days, completed_at)?;
        log_recent_raw_journal_phase(
            RECENT_RAW_JOURNAL_PHASE_PRUNE_END,
            None,
            batch_rows,
            request_batches,
            coalesce_elapsed_ms,
            coalesce_limit_rows,
            elapsed_ms_ceil(prune_started.elapsed()),
            telemetry,
        );
    } else {
        log_recent_raw_journal_phase(
            RECENT_RAW_JOURNAL_PHASE_PRUNE_SKIPPED,
            prune_skip_reason,
            batch_rows,
            request_batches,
            coalesce_elapsed_ms,
            coalesce_limit_rows,
            elapsed_ms_ceil(prune_check_started.elapsed()),
            telemetry,
        );
    }
    log_recent_raw_journal_phase(
        RECENT_RAW_JOURNAL_PHASE_BATCH_DONE,
        None,
        batch_rows,
        request_batches,
        coalesce_elapsed_ms,
        coalesce_limit_rows,
        elapsed_ms_ceil(batch_phase_started.elapsed()),
        telemetry,
    );
    Ok(())
}

fn write_recent_raw_journal_batch_with_deadline_attempts<DeadlineFn, WriteFn>(
    telemetry: &ObservedSwapWriterTelemetry,
    inserted_swaps: &[SwapEvent],
    mut deadline_for_attempt: DeadlineFn,
    mut write_attempt: WriteFn,
) -> Result<()>
where
    DeadlineFn: FnMut() -> Instant,
    WriteFn: FnMut(&[SwapEvent], Instant) -> Result<(RecentRawJournalWriteSummary, bool)>,
{
    let batch_rows = inserted_swaps.len();
    let mut batch_offset = 0usize;
    let mut attempt_index = 0usize;
    while batch_offset < batch_rows {
        attempt_index = attempt_index.saturating_add(1);
        let suffix = &inserted_swaps[batch_offset..];
        let write_started = Instant::now();
        let (summary, time_budget_exhausted) = write_attempt(suffix, deadline_for_attempt())?;
        let elapsed_ms = elapsed_ms_ceil(write_started.elapsed());
        let processed_rows = recent_raw_journal_summary_processed_rows(&summary).min(suffix.len());
        if time_budget_exhausted {
            let committed_rows = batch_offset.saturating_add(processed_rows);
            let unproven_rows = batch_rows.saturating_sub(committed_rows);
            warn!(
                reason = OBSERVED_SWAP_RECENT_RAW_JOURNAL_WRITE_DEADLINE_EXHAUSTED,
                attempt_index,
                batch_rows,
                batch_offset,
                suffix_rows = suffix.len(),
                processed_rows,
                committed_rows,
                inserted_rows = summary.inserted_rows,
                unproven_rows,
                elapsed_ms,
                queue_depth_batches = telemetry.journal_queue_depth_batches(),
                queue_row_debt = telemetry.journal_queue_row_debt(),
                overflow_depth_batches = telemetry
                    .journal_overflow_depth_batches
                    .load(Ordering::Relaxed),
                overflow_row_debt = telemetry.journal_overflow_row_debt.load(Ordering::Relaxed),
                bulk_statement_count = summary.recent_raw_bulk_statement_count,
                bulk_rows_processed = summary.recent_raw_bulk_rows_processed,
                bulk_rows_inserted = summary.recent_raw_bulk_rows_inserted,
                bulk_value_build_duration_ms = summary.recent_raw_bulk_value_build_duration_ms,
                bulk_prepare_duration_ms = summary.recent_raw_bulk_prepare_duration_ms,
                bulk_execute_duration_ms = summary.recent_raw_bulk_execute_duration_ms,
                bulk_state_refresh_duration_ms = summary.recent_raw_bulk_state_refresh_duration_ms,
                bulk_state_upsert_duration_ms = summary.recent_raw_bulk_state_upsert_duration_ms,
                bulk_transaction_duration_ms = summary.recent_raw_bulk_transaction_duration_ms,
                bulk_deadline_exhausted_before_statement =
                    summary.recent_raw_bulk_deadline_exhausted_before_statement,
                bulk_deadline_exhausted_during_execute =
                    summary.recent_raw_bulk_deadline_exhausted_during_execute,
                "recent raw journal write deadline exhausted"
            );
            if processed_rows == 0 {
                return Err(anyhow!(
                    "{OBSERVED_SWAP_RECENT_RAW_JOURNAL_WRITE_DEADLINE_EXHAUSTED}: batch_rows={batch_rows} committed_rows={committed_rows} inserted_rows={} unproven_rows={unproven_rows} elapsed_ms={elapsed_ms}",
                    summary.inserted_rows
                ));
            }
            batch_offset = committed_rows;
            if batch_offset < batch_rows {
                thread::sleep(OBSERVED_SWAP_WRITER_RETRYABLE_LOCK_BACKOFF);
            }
            continue;
        }
        if processed_rows != suffix.len() {
            let committed_rows = batch_offset.saturating_add(processed_rows);
            let unproven_rows = batch_rows.saturating_sub(committed_rows);
            return Err(anyhow!(
                "observed_swap_writer_recent_raw_journal_write_partial_without_deadline_exhaustion: batch_rows={batch_rows} committed_rows={committed_rows} inserted_rows={} unproven_rows={unproven_rows} elapsed_ms={elapsed_ms}",
                summary.inserted_rows
            ));
        }
        batch_offset = batch_rows;
        debug!(
            attempt_index,
            batch_rows,
            committed_rows = batch_offset,
            inserted_rows = summary.inserted_rows,
            elapsed_ms,
            bulk_statement_count = summary.recent_raw_bulk_statement_count,
            bulk_rows_processed = summary.recent_raw_bulk_rows_processed,
            bulk_rows_inserted = summary.recent_raw_bulk_rows_inserted,
            "recent raw journal batch write completed within deadline"
        );
    }
    Ok(())
}

fn recent_raw_journal_summary_processed_rows(summary: &RecentRawJournalWriteSummary) -> usize {
    summary
        .recent_raw_bulk_rows_processed
        .max(summary.batch_rows)
}

#[derive(Debug)]
struct RecentRawJournalCollectedWriteBatch {
    inserted_swaps: Vec<SwapEvent>,
    request_batches: usize,
    coalesce_elapsed_ms: u64,
    coalesce_limit_rows: usize,
}

fn recent_raw_journal_adaptive_write_coalesce_max_batches(
    config: &ObservedSwapRecentRawJournalConfig,
) -> usize {
    config
        .write_coalesce_max_batches
        .max(1)
        .saturating_mul(OBSERVED_SWAP_RECENT_RAW_JOURNAL_ADAPTIVE_COALESCE_MAX_BATCHES_MULTIPLIER)
        .min(OBSERVED_SWAP_RECENT_RAW_JOURNAL_ADAPTIVE_COALESCE_MAX_BATCHES_CAP)
        .max(config.write_coalesce_max_batches.max(1))
}

fn recent_raw_journal_adaptive_write_coalesce_max_rows(
    config: &ObservedSwapRecentRawJournalConfig,
) -> usize {
    OBSERVED_SWAP_BATCH_MAX_SIZE
        .max(1)
        .saturating_mul(recent_raw_journal_adaptive_write_coalesce_max_batches(
            config,
        ))
        .min(OBSERVED_SWAP_RECENT_RAW_JOURNAL_ADAPTIVE_COALESCE_MAX_ROWS_CAP)
        .max(OBSERVED_SWAP_BATCH_MAX_SIZE.max(1))
}

fn recent_raw_journal_adaptive_coalesce_pressure(telemetry: &ObservedSwapWriterTelemetry) -> bool {
    telemetry.journal_queue_depth_batches() > 0
        || telemetry.journal_queue_row_debt() > 0
        || telemetry
            .journal_overflow_depth_batches
            .load(Ordering::Relaxed)
            > 0
        || telemetry.journal_overflow_row_debt.load(Ordering::Relaxed) > 0
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

fn prune_recent_raw_journal_with_budget(
    store: &SqliteStore,
    retention_days: u32,
    now: DateTime<Utc>,
) -> Result<SqliteBatchedDeleteSummary> {
    let cutoff = now - ChronoDuration::days(retention_days.max(1) as i64);
    let mut summary = SqliteBatchedDeleteSummary::default();
    loop {
        if summary.batches >= RECENT_RAW_JOURNAL_RETENTION_MAX_DELETE_BATCHES_PER_RUN {
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

fn recent_raw_journal_prune_backlog_skip_reason(
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

fn recent_raw_journal_prune_due(
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

fn process_discovery_aggregate_write_request(
    store: &SqliteStore,
    inserted_swaps: &[SwapEvent],
    batch_started: Instant,
    aggregate_write_config: &DiscoveryAggregateWriteConfig,
    telemetry: &ObservedSwapWriterTelemetry,
) -> Result<()> {
    if inserted_swaps.is_empty() {
        telemetry.note_worker_busy_completed(elapsed_ms_ceil(batch_started.elapsed()));
        return Ok(());
    }

    let aggregate_started = Instant::now();
    let aggregate_result =
        store.apply_discovery_scoring_batch(inserted_swaps, aggregate_write_config);
    telemetry.note_discovery_scoring_completed(elapsed_ms_ceil(aggregate_started.elapsed()));
    if let Err(error) = aggregate_result {
        if let Some(first_gap_swap) = inserted_swaps.iter().min_by(|a, b| {
            a.ts_utc
                .cmp(&b.ts_utc)
                .then_with(|| a.slot.cmp(&b.slot))
                .then_with(|| a.signature.cmp(&b.signature))
        }) {
            if let Err(gap_error) =
                store.set_discovery_scoring_materialization_gap_cursor(&DiscoveryRuntimeCursor {
                    ts_utc: first_gap_swap.ts_utc,
                    slot: first_gap_swap.slot,
                    signature: first_gap_swap.signature.clone(),
                })
            {
                if observed_swap_writer_discovery_scoring_error_requires_abort(&gap_error) {
                    telemetry.note_worker_busy_completed(elapsed_ms_ceil(batch_started.elapsed()));
                    return Err(gap_error).context(
                        "observed swap writer stopping after fatal discovery scoring gap cursor failure",
                    );
                }
                warn!(
                    error = %gap_error,
                    gap_since = %first_gap_swap.ts_utc,
                    "failed to latch discovery scoring materialization gap after aggregate batch failure"
                );
            }
        }
        if observed_swap_writer_discovery_scoring_error_requires_abort(&error) {
            telemetry.note_worker_busy_completed(elapsed_ms_ceil(batch_started.elapsed()));
            return Err(error).context(
                "observed swap writer stopping after fatal discovery scoring materialization failure",
            );
        }
        warn!(
            error = %error,
            inserted_swaps = inserted_swaps.len(),
            "observed swap batch inserted raw rows but discovery scoring materialization failed"
        );
    } else if let Some(max_swap) = inserted_swaps.iter().max_by(|a, b| {
        a.ts_utc
            .cmp(&b.ts_utc)
            .then_with(|| a.slot.cmp(&b.slot))
            .then_with(|| a.signature.cmp(&b.signature))
    }) {
        if let Err(error) =
            store.set_discovery_scoring_covered_through_cursor(&DiscoveryRuntimeCursor {
                ts_utc: max_swap.ts_utc,
                slot: max_swap.slot,
                signature: max_swap.signature.clone(),
            })
        {
            if observed_swap_writer_discovery_scoring_covered_through_update_error_is_retryable(
                &error,
            ) {
                if let Err(gap_error) =
                    latch_discovery_scoring_materialization_gap_from_swaps(store, inserted_swaps)
                {
                    if observed_swap_writer_discovery_scoring_error_requires_abort(&gap_error) {
                        telemetry
                            .note_worker_busy_completed(elapsed_ms_ceil(batch_started.elapsed()));
                        return Err(gap_error).context(
                            "observed swap writer stopping after fatal discovery scoring covered_through retry gap cursor failure",
                        );
                    }
                    warn!(
                        reason = OBSERVED_SWAP_WRITER_DISCOVERY_SCORING_COVERED_THROUGH_UPDATE_SQLITE_LOCK_RETRYABLE,
                        error = %gap_error,
                        "failed to latch discovery scoring materialization gap after retryable covered_through cursor update failure",
                    );
                }
                warn!(
                    reason = OBSERVED_SWAP_WRITER_DISCOVERY_SCORING_COVERED_THROUGH_UPDATE_SQLITE_LOCK_RETRYABLE,
                    error = %error,
                    covered_through = %max_swap.ts_utc,
                    "discovery scoring covered_through cursor update hit retryable sqlite lock; leaving coverage watermark unchanged",
                );
            } else {
                telemetry.note_worker_busy_completed(elapsed_ms_ceil(batch_started.elapsed()));
                return Err(error).context(
                    "observed swap writer stopping after fatal discovery scoring coverage watermark failure",
                );
            }
        }
    }

    telemetry.note_worker_busy_completed(elapsed_ms_ceil(batch_started.elapsed()));
    Ok(())
}

fn set_terminal_failure_message(
    terminal_failure_message: &Arc<Mutex<Option<String>>>,
    message: String,
) {
    if let Ok(mut guard) = terminal_failure_message.lock() {
        if guard.is_none() {
            *guard = Some(message);
        }
    }
}

fn load_terminal_failure_message(
    terminal_failure_message: &Arc<Mutex<Option<String>>>,
) -> Option<String> {
    terminal_failure_message
        .lock()
        .ok()
        .and_then(|message| message.clone())
}

pub(crate) fn run_observed_swap_retention_maintenance_once(
    sqlite_path: &str,
    config: ObservedSwapRetentionConfig,
    runtime_health: Option<ObservedSwapRetentionRuntimeHealthHandle>,
) -> Result<ObservedSwapRetentionMaintenanceSummary> {
    let store = SqliteStore::open(Path::new(sqlite_path)).with_context(|| {
        format!("failed to open sqlite db for observed swap retention maintenance: {sqlite_path}")
    })?;
    run_observed_swap_retention_maintenance(&store, config, runtime_health.as_ref())
}

fn run_observed_swap_retention_maintenance(
    store: &SqliteStore,
    config: ObservedSwapRetentionConfig,
    runtime_health: Option<&ObservedSwapRetentionRuntimeHealthHandle>,
) -> Result<ObservedSwapRetentionMaintenanceSummary> {
    let maintenance_started = Instant::now();
    let now = Utc::now();
    let nominal_cutoff = observed_swap_retention_nominal_cutoff(now, config);
    let effective_cutoff = resolve_observed_swap_retention_effective_cutoff(config, now, |now| {
        store.load_discovery_scoring_backfill_protected_since(now)
    })?;
    let aggregate_cutoff = if config.aggregate_writes_enabled {
        Some(now - ChronoDuration::days(config.aggregate_retention_days.max(1) as i64))
    } else {
        None
    };
    let mut last_sqlite_contention = sqlite_contention_snapshot();
    let mut raw_delete_summary = SqliteBatchedDeleteSummary::default();
    let mut scoring_delete_summary = SqliteBatchedDeleteSummary::default();
    let mut completed_full_sweep = true;
    let mut stop_reason = None;

    loop {
        if let Some(reason) = observed_swap_retention_should_stop(
            runtime_health,
            &mut last_sqlite_contention,
            maintenance_started,
        ) {
            completed_full_sweep = false;
            stop_reason = Some(reason);
            break;
        }
        if raw_delete_summary.batches >= OBSERVED_SWAP_RETENTION_MAX_RAW_DELETE_BATCHES_PER_RUN {
            completed_full_sweep = false;
            stop_reason = Some("raw_batch_budget");
            break;
        }
        let deleted = store
            .delete_observed_swaps_before_batch(
                effective_cutoff,
                OBSERVED_SWAP_RETENTION_DELETE_BATCH_SIZE,
            )
            .with_context(|| {
                format!(
                    "observed swap retention sweep failed retention_days={} nominal_cutoff={} effective_cutoff={}",
                    config.retention_days, nominal_cutoff, effective_cutoff
                )
            })?;
        if deleted == 0 {
            break;
        }
        raw_delete_summary.deleted_rows += deleted;
        raw_delete_summary.batches += 1;
        thread::sleep(OBSERVED_SWAP_RETENTION_INTER_BATCH_PAUSE);
    }

    if completed_full_sweep {
        if let Some(aggregate_cutoff) = aggregate_cutoff {
            loop {
                if let Some(reason) = observed_swap_retention_should_stop(
                    runtime_health,
                    &mut last_sqlite_contention,
                    maintenance_started,
                ) {
                    completed_full_sweep = false;
                    stop_reason = Some(reason);
                    break;
                }
                if scoring_delete_summary.batches
                    >= OBSERVED_SWAP_RETENTION_MAX_SCORING_DELETE_BATCHES_PER_RUN
                {
                    completed_full_sweep = false;
                    stop_reason = Some("scoring_batch_budget");
                    break;
                }
                let deleted = store
                    .prune_discovery_scoring_before_batch(
                        aggregate_cutoff,
                        DISCOVERY_SCORING_RETENTION_DELETE_BATCH_SIZE,
                    )
                    .with_context(|| {
                        format!(
                            "discovery scoring retention sweep failed aggregate_retention_days={} aggregate_cutoff={}",
                            config.aggregate_retention_days, aggregate_cutoff
                        )
                    })?;
                if deleted == 0 {
                    break;
                }
                scoring_delete_summary.deleted_rows += deleted;
                scoring_delete_summary.batches += 1;
                thread::sleep(OBSERVED_SWAP_RETENTION_INTER_BATCH_PAUSE);
            }
        }
    }

    if let Some(reason) = stop_reason {
        warn!(
            stop_reason = reason,
            retention_days = config.retention_days,
            aggregate_retention_days = config.aggregate_retention_days,
            nominal_observed_swap_cutoff = %nominal_cutoff,
            effective_observed_swap_cutoff = %effective_cutoff,
            aggregate_scoring_cutoff = ?aggregate_cutoff,
            deleted_observed_swap_rows = raw_delete_summary.deleted_rows,
            observed_swap_delete_batches = raw_delete_summary.batches,
            deleted_scoring_rows = scoring_delete_summary.deleted_rows,
            discovery_scoring_delete_batches = scoring_delete_summary.batches,
            "observed swap retention paused before exhausting the backlog"
        );
    }

    let checkpoint = if completed_full_sweep {
        run_retention_wal_checkpoint(
            store,
            config,
            nominal_cutoff,
            effective_cutoff,
            raw_delete_summary,
            scoring_delete_summary,
        )?
    } else {
        ObservedSwapRetentionCheckpointSummary {
            mode: "skipped_bounded_run",
            busy: 0,
            log_frames: 0,
            checkpointed_frames: 0,
        }
    };
    Ok(ObservedSwapRetentionMaintenanceSummary {
        nominal_cutoff,
        effective_cutoff,
        aggregate_cutoff,
        raw_deleted_rows: raw_delete_summary.deleted_rows,
        raw_delete_batches: raw_delete_summary.batches,
        scoring_deleted_rows: scoring_delete_summary.deleted_rows,
        scoring_delete_batches: scoring_delete_summary.batches,
        completed_full_sweep,
        stop_reason,
        checkpoint,
        duration_ms: elapsed_ms_ceil(maintenance_started.elapsed()),
    })
}

fn observed_swap_retention_should_stop(
    runtime_health: Option<&ObservedSwapRetentionRuntimeHealthHandle>,
    last_sqlite_contention: &mut SqliteContentionSnapshot,
    maintenance_started: Instant,
) -> Option<&'static str> {
    if maintenance_started.elapsed() >= OBSERVED_SWAP_RETENTION_MAX_DURATION_PER_RUN {
        return Some("duration_budget");
    }
    let Some(runtime_health) = runtime_health else {
        *last_sqlite_contention = sqlite_contention_snapshot();
        return None;
    };
    let writer_snapshot = runtime_health.writer_snapshot();
    if writer_snapshot.pending_requests > 0 {
        return Some("runtime_pressure");
    }
    if writer_snapshot.aggregate_queue_depth_batches > 0 {
        return Some("runtime_pressure");
    }
    if writer_snapshot.aggregate_overflow_depth_batches > 0 {
        return Some("runtime_pressure");
    }
    if writer_snapshot.journal_queue_depth_batches > 0 {
        return Some("runtime_pressure");
    }
    let sqlite_contention_current = sqlite_contention_snapshot();
    let sqlite_write_retry_delta = sqlite_contention_current
        .write_retry_total
        .saturating_sub(last_sqlite_contention.write_retry_total);
    let sqlite_busy_error_delta = sqlite_contention_current
        .busy_error_total
        .saturating_sub(last_sqlite_contention.busy_error_total);
    *last_sqlite_contention = sqlite_contention_current;
    if sqlite_write_retry_delta > 0 || sqlite_busy_error_delta > 0 {
        return Some("runtime_pressure");
    }
    if runtime_health.ingestion_snapshot().is_some_and(|snapshot| {
        snapshot.yellowstone_output_queue_capacity > 0
            && snapshot.yellowstone_output_queue_fill_ratio
                >= SQLITE_MAINTENANCE_MAX_YELLOWSTONE_OUTPUT_QUEUE_FILL_RATIO
    }) {
        return Some("runtime_pressure");
    }
    None
}

fn observed_swap_retention_nominal_cutoff(
    now: chrono::DateTime<Utc>,
    config: ObservedSwapRetentionConfig,
) -> chrono::DateTime<Utc> {
    now - ChronoDuration::days(config.retention_days.max(1) as i64)
}

fn resolve_observed_swap_retention_effective_cutoff<F>(
    config: ObservedSwapRetentionConfig,
    now: chrono::DateTime<Utc>,
    load_protected_since: F,
) -> Result<chrono::DateTime<Utc>>
where
    F: FnOnce(chrono::DateTime<Utc>) -> Result<Option<chrono::DateTime<Utc>>>,
{
    let nominal_cutoff = observed_swap_retention_nominal_cutoff(now, config);
    match load_protected_since(now) {
        Ok(Some(protected_since)) => Ok(nominal_cutoff.min(protected_since)),
        Ok(None) => Ok(nominal_cutoff),
        Err(error) => {
            if observed_swap_retention_protection_load_error_requires_abort(&error) {
                return Err(error).context(
                    "observed swap retention source protection lookup failed with fatal sqlite I/O",
                );
            }
            warn!(
                error = %error,
                retention_days = config.retention_days,
                "failed loading discovery scoring backfill source protection; using nominal observed swap retention cutoff"
            );
            Ok(nominal_cutoff)
        }
    }
}

fn percentile_from_deque(values: &VecDeque<u64>, q: f64) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.iter().copied().collect::<Vec<_>>();
    sorted.sort_unstable();
    let idx = ((sorted.len() - 1) as f64 * q.clamp(0.0, 1.0)).round() as usize;
    sorted[idx]
}

fn elapsed_ms_ceil(duration: StdDuration) -> u64 {
    let micros = duration.as_micros();
    if micros == 0 {
        0
    } else {
        micros.div_ceil(1000).min(u128::from(u64::MAX)) as u64
    }
}

fn run_retention_wal_checkpoint(
    store: &SqliteStore,
    config: ObservedSwapRetentionConfig,
    nominal_cutoff: chrono::DateTime<Utc>,
    effective_cutoff: chrono::DateTime<Utc>,
    raw_delete_summary: SqliteBatchedDeleteSummary,
    scoring_delete_summary: SqliteBatchedDeleteSummary,
) -> Result<ObservedSwapRetentionCheckpointSummary> {
    match store.checkpoint_wal_passive() {
        Ok((busy, log_frames, checkpointed_frames)) => {
            let checkpoint_summary = ObservedSwapRetentionCheckpointSummary {
                mode: "passive_runtime",
                busy,
                log_frames,
                checkpointed_frames,
            };
            if raw_delete_summary.deleted_rows > 0 || scoring_delete_summary.deleted_rows > 0 {
                info!(
                    retention_days = config.retention_days,
                    aggregate_retention_days = config.aggregate_retention_days,
                    nominal_observed_swap_cutoff = %nominal_cutoff,
                    effective_observed_swap_cutoff = %effective_cutoff,
                    deleted_observed_swap_rows = raw_delete_summary.deleted_rows,
                    observed_swap_delete_batches = raw_delete_summary.batches,
                    deleted_scoring_rows = scoring_delete_summary.deleted_rows,
                    discovery_scoring_delete_batches = scoring_delete_summary.batches,
                    wal_checkpoint_mode = checkpoint_summary.mode,
                    wal_checkpoint_busy = busy,
                    wal_log_frames = log_frames,
                    wal_checkpointed_frames = checkpointed_frames,
                    "observed swap retention sweep attempted runtime passive wal checkpoint"
                );
            } else {
                info!(
                    retention_days = config.retention_days,
                    aggregate_retention_days = config.aggregate_retention_days,
                    nominal_observed_swap_cutoff = %nominal_cutoff,
                    effective_observed_swap_cutoff = %effective_cutoff,
                    deleted_observed_swap_rows = 0,
                    deleted_scoring_rows = 0,
                    wal_checkpoint_mode = checkpoint_summary.mode,
                    wal_checkpoint_busy = busy,
                    wal_log_frames = log_frames,
                    wal_checkpointed_frames = checkpointed_frames,
                    "observed swap retention sweep attempted periodic passive wal checkpoint"
                );
            }
            Ok(checkpoint_summary)
        }
        Err(error) => {
            if observed_swap_retention_checkpoint_error_requires_abort(None, Some(&error)) {
                return Err(error).context(
                    "observed swap retention periodic wal checkpoint failed with fatal sqlite I/O",
                );
            }
            warn!(
                error = %error,
                retention_days = config.retention_days,
                aggregate_retention_days = config.aggregate_retention_days,
                nominal_observed_swap_cutoff = %nominal_cutoff,
                effective_observed_swap_cutoff = %effective_cutoff,
                deleted_observed_swap_rows = raw_delete_summary.deleted_rows,
                observed_swap_delete_batches = raw_delete_summary.batches,
                deleted_scoring_rows = scoring_delete_summary.deleted_rows,
                discovery_scoring_delete_batches = scoring_delete_summary.batches,
                wal_checkpoint_mode = "passive_runtime_failed",
                "observed swap retention sweep passive wal checkpoint failed"
            );
            Ok(ObservedSwapRetentionCheckpointSummary {
                mode: "passive_runtime_failed",
                busy: 0,
                log_frames: 0,
                checkpointed_frames: 0,
            })
        }
    }
}

fn observed_swap_retention_checkpoint_error_requires_abort(
    primary_error: Option<&anyhow::Error>,
    fallback_error: Option<&anyhow::Error>,
) -> bool {
    primary_error.is_some_and(is_fatal_sqlite_anyhow_error)
        || fallback_error.is_some_and(is_fatal_sqlite_anyhow_error)
}

fn observed_swap_writer_discovery_scoring_error_requires_abort(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

fn observed_swap_writer_discovery_scoring_replay_apply_error_is_retryable(
    error: &anyhow::Error,
) -> bool {
    is_retryable_sqlite_anyhow_error(error) && !is_fatal_sqlite_anyhow_error(error)
}

fn observed_swap_writer_discovery_scoring_covered_through_update_error_is_retryable(
    error: &anyhow::Error,
) -> bool {
    is_retryable_sqlite_anyhow_error(error) && !is_fatal_sqlite_anyhow_error(error)
}

fn observed_swap_writer_discovery_scoring_rug_finalize_error_is_retryable(
    error: &anyhow::Error,
) -> bool {
    is_retryable_sqlite_anyhow_error(error) && !is_fatal_sqlite_anyhow_error(error)
}

fn observed_swap_writer_aggregate_queue_capacity(config: &ObservedSwapWriterConfig) -> usize {
    if !config.aggregate_writes_enabled {
        return 0;
    }
    config
        .channel_capacity
        .max(1)
        .div_ceil(config.batch_max_size.max(1))
}

fn observed_swap_writer_default_aggregate_overflow_capacity_batches(
    channel_capacity: usize,
    batch_max_size: usize,
    aggregate_writes_enabled: bool,
) -> usize {
    if !aggregate_writes_enabled {
        return 0;
    }
    channel_capacity
        .max(1)
        .div_ceil(batch_max_size.max(1))
        .saturating_mul(OBSERVED_SWAP_DISCOVERY_AGGREGATE_OVERFLOW_CAPACITY_MULTIPLIER)
}

fn recent_raw_journal_overflow_row_capacity(
    batch_max_size: usize,
    journal_config: &ObservedSwapRecentRawJournalConfig,
) -> usize {
    journal_config
        .overflow_capacity_batches
        .max(1)
        .saturating_mul(batch_max_size.max(1))
        .saturating_mul(journal_config.write_coalesce_max_batches.max(1))
}

fn observed_swap_writer_discovery_critical_reserve_requests(
    config: &ObservedSwapWriterConfig,
) -> usize {
    let reserved_requests = OBSERVED_SWAP_WRITER_DISCOVERY_CRITICAL_RESERVED_BATCHES
        .max(1)
        .saturating_mul(config.batch_max_size.max(1));
    reserved_requests.min(config.channel_capacity.saturating_sub(1))
}

fn observed_swap_retention_protection_load_error_requires_abort(error: &anyhow::Error) -> bool {
    is_fatal_sqlite_anyhow_error(error)
}

#[derive(Default)]
struct DiscoveryAggregateGapRepairEpoch {
    gap_cursor: Option<DiscoveryRuntimeCursor>,
    repair_target_cursor: Option<DiscoveryRuntimeCursor>,
    gap_cursor_observed: bool,
    resume_after_cursor: Option<DiscoveryRuntimeCursor>,
}

impl DiscoveryAggregateGapRepairEpoch {
    fn reset(&mut self) {
        self.gap_cursor = None;
        self.repair_target_cursor = None;
        self.gap_cursor_observed = false;
        self.resume_after_cursor = None;
    }

    fn reset_for_gap(
        &mut self,
        gap_cursor: DiscoveryRuntimeCursor,
        repair_target_cursor: Option<DiscoveryRuntimeCursor>,
    ) {
        self.gap_cursor = Some(gap_cursor);
        self.repair_target_cursor = repair_target_cursor;
        self.gap_cursor_observed = false;
        self.resume_after_cursor = None;
    }

    fn matches_gap(&self, gap_cursor: &DiscoveryRuntimeCursor) -> bool {
        self.gap_cursor.as_ref().is_some_and(|current| {
            compare_discovery_runtime_cursors(current, gap_cursor) == std::cmp::Ordering::Equal
        })
    }
}

fn run_discovery_aggregate_gap_repair_slice(
    sqlite_path: &str,
    store: &SqliteStore,
    config: &ObservedSwapWriterConfig,
    telemetry: &ObservedSwapWriterTelemetry,
    require_gap_cursor: bool,
    repair_epoch: &mut DiscoveryAggregateGapRepairEpoch,
) -> Result<bool> {
    if !config.aggregate_gap_fallback_enabled || config.aggregate_idle_replay_max_pages == 0 {
        return Ok(false);
    }

    let current_gap_cursor = store.load_discovery_scoring_materialization_gap_cursor()?;
    if current_gap_cursor.is_none() {
        repair_epoch.reset();
        if require_gap_cursor {
            telemetry.set_aggregate_gap_active(false);
            return Ok(false);
        }
    }

    if let Some(current_gap_cursor) = current_gap_cursor.as_ref() {
        if !repair_epoch.matches_gap(current_gap_cursor) {
            let repair_target_cursor = load_observed_swaps_tail_cursor(sqlite_path)?;
            debug!(
                materialization_gap_ts = %current_gap_cursor.ts_utc,
                materialization_gap_slot = current_gap_cursor.slot,
                materialization_gap_signature = %current_gap_cursor.signature,
                repair_target_ts = repair_target_cursor.as_ref().map(|cursor| cursor.ts_utc.to_rfc3339()),
                repair_target_slot = repair_target_cursor.as_ref().map(|cursor| cursor.slot),
                repair_target_signature = repair_target_cursor.as_ref().map(|cursor| cursor.signature.as_str()),
                "discovery aggregate materialization gap repair epoch started"
            );
            repair_epoch.reset_for_gap(current_gap_cursor.clone(), repair_target_cursor);
        }
    }

    let resume_after_cursor = repair_epoch.resume_after_cursor.as_ref();
    let repair_target_cursor = current_gap_cursor
        .as_ref()
        .and_then(|_| repair_epoch.repair_target_cursor.as_ref());

    telemetry.set_aggregate_worker_busy(true);
    let result = run_aggregate_gap_replay_with_resume(
        store,
        config,
        Some(config.aggregate_idle_replay_max_pages),
        resume_after_cursor,
        repair_epoch.gap_cursor_observed,
        repair_target_cursor,
    );
    telemetry.set_aggregate_worker_busy(false);
    let replay_progress = result?;

    repair_epoch.gap_cursor_observed |= replay_progress.gap_cursor_observed;
    let remaining_gap_cursor = store.load_discovery_scoring_materialization_gap_cursor()?;
    let latch_cleared = remaining_gap_cursor.is_none() && current_gap_cursor.is_some();
    debug!(
        materialization_gap_ts = current_gap_cursor
            .as_ref()
            .map(|cursor| cursor.ts_utc.to_rfc3339()),
        materialization_gap_slot = current_gap_cursor.as_ref().map(|cursor| cursor.slot),
        materialization_gap_signature = current_gap_cursor
            .as_ref()
            .map(|cursor| cursor.signature.as_str()),
        repair_target_ts = repair_target_cursor.map(|cursor| cursor.ts_utc.to_rfc3339()),
        repair_target_slot = repair_target_cursor.map(|cursor| cursor.slot),
        repair_target_signature = repair_target_cursor.map(|cursor| cursor.signature.as_str()),
        exact_gap_row_observed = repair_epoch.gap_cursor_observed,
        repair_target_reached = replay_progress.reached_repair_target,
        tail_caught_up = replay_progress.caught_up_to_tail,
        latch_cleared,
        replay_pages = replay_progress.page_count,
        "discovery aggregate materialization gap repair slice completed"
    );
    if let Some(remaining_gap_cursor) = remaining_gap_cursor {
        if let Some(last_replay_cursor) = replay_progress.last_replay_cursor {
            if repair_epoch.matches_gap(&remaining_gap_cursor) {
                repair_epoch.resume_after_cursor = Some(last_replay_cursor);
            } else {
                repair_epoch.reset();
            }
        }
    } else {
        repair_epoch.reset();
    }
    telemetry.set_aggregate_gap_active(
        store
            .load_discovery_scoring_materialization_gap_cursor()?
            .is_some(),
    );
    Ok(true)
}

fn load_observed_swaps_tail_cursor(sqlite_path: &str) -> Result<Option<DiscoveryRuntimeCursor>> {
    let conn = Connection::open_with_flags(sqlite_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| {
            format!("failed to open sqlite db read-only for observed_swaps tail: {sqlite_path}")
        })?;
    conn.busy_timeout(StdDuration::from_millis(250))
        .context("failed setting observed_swaps tail read-only busy timeout")?;
    conn.pragma_update(None, "query_only", true)
        .context("failed enabling observed_swaps tail read-only query_only mode")?;
    let cursor_raw = conn
        .query_row(
            "SELECT ts, slot, signature
             FROM observed_swaps INDEXED BY idx_observed_swaps_ts_slot_signature
             ORDER BY ts DESC, slot DESC, signature DESC
             LIMIT 1",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()
        .context("failed loading observed_swaps bounded tail cursor")?;
    cursor_raw
        .map(
            |(ts_raw, slot_raw, signature)| -> Result<DiscoveryRuntimeCursor> {
                Ok(DiscoveryRuntimeCursor {
                    ts_utc: DateTime::parse_from_rfc3339(&ts_raw)
                        .with_context(|| {
                            format!("invalid observed_swaps tail ts rfc3339 value: {ts_raw}")
                        })?
                        .with_timezone(&Utc),
                    slot: slot_raw.max(0) as u64,
                    signature,
                })
            },
        )
        .transpose()
}

#[derive(Default)]
struct AggregateReplayProgress {
    page_count: usize,
    gap_cursor_loaded: bool,
    gap_cursor_observed: bool,
    caught_up_to_tail: bool,
    reached_repair_target: bool,
    last_replay_cursor: Option<DiscoveryRuntimeCursor>,
}

fn compare_discovery_runtime_cursors(
    left: &DiscoveryRuntimeCursor,
    right: &DiscoveryRuntimeCursor,
) -> std::cmp::Ordering {
    left.ts_utc
        .cmp(&right.ts_utc)
        .then_with(|| left.slot.cmp(&right.slot))
        .then_with(|| left.signature.cmp(&right.signature))
}

fn aggregate_replay_cursor_before(cursor: &DiscoveryRuntimeCursor) -> DiscoveryRuntimeCursor {
    if cursor.slot > 0 {
        return DiscoveryRuntimeCursor {
            ts_utc: cursor.ts_utc,
            slot: cursor.slot - 1,
            signature: String::new(),
        };
    }

    DiscoveryRuntimeCursor {
        ts_utc: cursor
            .ts_utc
            .checked_sub_signed(ChronoDuration::nanoseconds(1))
            .unwrap_or(cursor.ts_utc),
        slot: u64::MAX,
        signature: String::new(),
    }
}

fn run_aggregate_startup_replay(
    store: &SqliteStore,
    config: &ObservedSwapWriterConfig,
) -> Result<()> {
    loop {
        let progress = run_aggregate_gap_replay(store, config, None)?;
        let Some(_current_gap_cursor) =
            store.load_discovery_scoring_materialization_gap_cursor()?
        else {
            return Ok(());
        };
        if progress.gap_cursor_loaded {
            return Ok(());
        }
    }
}

fn run_aggregate_gap_replay(
    store: &SqliteStore,
    config: &ObservedSwapWriterConfig,
    max_pages: Option<usize>,
) -> Result<AggregateReplayProgress> {
    run_aggregate_gap_replay_with_resume(store, config, max_pages, None, false, None)
}

fn run_aggregate_gap_replay_with_resume(
    store: &SqliteStore,
    config: &ObservedSwapWriterConfig,
    max_pages: Option<usize>,
    resume_after_cursor: Option<&DiscoveryRuntimeCursor>,
    gap_cursor_observed_before: bool,
    repair_target_cursor: Option<&DiscoveryRuntimeCursor>,
) -> Result<AggregateReplayProgress> {
    if !config.aggregate_writes_enabled {
        return Ok(AggregateReplayProgress::default());
    }

    let covered_since = store.load_discovery_scoring_covered_since()?;
    let mut cursor = match store.load_discovery_scoring_covered_through_cursor()? {
        Some(cursor) => cursor,
        None => {
            if covered_since.is_some() {
                return Err(anyhow!(
                    "aggregate writes require an exact covered_through cursor for safe startup replay"
                ));
            }
            return Ok(AggregateReplayProgress::default());
        }
    };
    let gap_cursor = store.load_discovery_scoring_materialization_gap_cursor()?;
    let mut progress = AggregateReplayProgress {
        gap_cursor_loaded: gap_cursor.is_some(),
        ..AggregateReplayProgress::default()
    };
    if let Some(resume_after_cursor) = resume_after_cursor {
        cursor = resume_after_cursor.clone();
    } else if let Some(gap_cursor) = gap_cursor.as_ref() {
        if compare_discovery_runtime_cursors(gap_cursor, &cursor) != std::cmp::Ordering::Greater {
            cursor = aggregate_replay_cursor_before(gap_cursor);
        }
    }
    let mut reached_repair_target = repair_target_cursor.is_some_and(|target| {
        compare_discovery_runtime_cursors(&cursor, target) != std::cmp::Ordering::Less
    });
    let mut gap_cursor_observed = gap_cursor_observed_before;

    loop {
        if reached_repair_target {
            break;
        }
        if max_pages.is_some_and(|limit| progress.page_count >= limit.max(1)) {
            break;
        }
        let mut page = Vec::with_capacity(config.batch_max_size);
        let mut saw_row_after_repair_target = false;
        let rows_seen = store.for_each_observed_swap_after_cursor(
            cursor.ts_utc,
            cursor.slot,
            cursor.signature.as_str(),
            config.batch_max_size,
            |swap| {
                let swap_cursor = DiscoveryRuntimeCursor {
                    ts_utc: swap.ts_utc,
                    slot: swap.slot,
                    signature: swap.signature.clone(),
                };
                if let Some(repair_target_cursor) = repair_target_cursor {
                    match compare_discovery_runtime_cursors(&swap_cursor, repair_target_cursor) {
                        std::cmp::Ordering::Greater => {
                            saw_row_after_repair_target = true;
                            return Ok(());
                        }
                        std::cmp::Ordering::Equal => {
                            reached_repair_target = true;
                        }
                        std::cmp::Ordering::Less => {}
                    }
                }
                if gap_cursor.as_ref().is_some_and(|gap_cursor| {
                    gap_cursor.ts_utc == swap.ts_utc
                        && gap_cursor.slot == swap.slot
                        && gap_cursor.signature == swap.signature
                }) {
                    gap_cursor_observed = true;
                }
                page.push(swap);
                Ok(())
            },
        )?;
        if page.is_empty() {
            progress.caught_up_to_tail = !saw_row_after_repair_target;
            break;
        }

        if let Err(error) =
            store.apply_discovery_scoring_batch(&page, &config.aggregate_write_config)
        {
            let retryable_replay_apply_lock =
                observed_swap_writer_discovery_scoring_replay_apply_error_is_retryable(&error);
            let mut retryable_gap_latched = false;
            if let Some(first_gap_swap) = page.iter().min_by(|a, b| {
                a.ts_utc
                    .cmp(&b.ts_utc)
                    .then_with(|| a.slot.cmp(&b.slot))
                    .then_with(|| a.signature.cmp(&b.signature))
            }) {
                if let Err(gap_error) = store.set_discovery_scoring_materialization_gap_cursor(
                    &DiscoveryRuntimeCursor {
                        ts_utc: first_gap_swap.ts_utc,
                        slot: first_gap_swap.slot,
                        signature: first_gap_swap.signature.clone(),
                    },
                ) {
                    if observed_swap_writer_discovery_scoring_error_requires_abort(&gap_error) {
                        return Err(gap_error).context(
                            "observed swap writer startup replay stopping after fatal discovery scoring gap cursor failure",
                        );
                    }
                    if retryable_replay_apply_lock {
                        return Err(gap_error).context(
                            "observed swap writer startup replay stopping after retryable discovery scoring apply failure could not latch gap cursor",
                        );
                    } else {
                        warn!(
                            error = %gap_error,
                            gap_since = %first_gap_swap.ts_utc,
                            "failed to latch discovery scoring materialization gap during aggregate-writer startup replay",
                        );
                    }
                } else {
                    retryable_gap_latched = true;
                }
            }
            if retryable_replay_apply_lock {
                warn!(
                    reason = OBSERVED_SWAP_WRITER_DISCOVERY_SCORING_REPLAY_APPLY_SQLITE_LOCK_RETRYABLE,
                    error = %error,
                    gap_latched = retryable_gap_latched,
                    "discovery scoring replay apply hit retryable sqlite lock; leaving coverage watermark unchanged",
                );
                progress.gap_cursor_loaded |= retryable_gap_latched;
                progress.gap_cursor_observed = gap_cursor_observed;
                return Ok(progress);
            }
            return Err(error).context(
                "failed replaying discovery scoring rows during aggregate-writer startup catch-up",
            );
        }

        let last_swap = page
            .last()
            .cloned()
            .ok_or_else(|| anyhow!("aggregate startup replay page unexpectedly empty"))?;
        if let Err(error) = store.finalize_discovery_scoring_rug_facts(last_swap.ts_utc) {
            if observed_swap_writer_discovery_scoring_rug_finalize_error_is_retryable(&error) {
                if let Some(first_gap_swap) = page.iter().min_by(|a, b| {
                    a.ts_utc
                        .cmp(&b.ts_utc)
                        .then_with(|| a.slot.cmp(&b.slot))
                        .then_with(|| a.signature.cmp(&b.signature))
                }) {
                    if let Err(gap_error) = store.set_discovery_scoring_materialization_gap_cursor(
                        &DiscoveryRuntimeCursor {
                            ts_utc: first_gap_swap.ts_utc,
                            slot: first_gap_swap.slot,
                            signature: first_gap_swap.signature.clone(),
                        },
                    ) {
                        if observed_swap_writer_discovery_scoring_error_requires_abort(&gap_error) {
                            return Err(gap_error).context(
                                "observed swap writer startup replay stopping after fatal discovery scoring rug finalize gap cursor failure",
                            );
                        }
                        warn!(
                            reason = OBSERVED_SWAP_WRITER_DISCOVERY_SCORING_RUG_FINALIZE_SQLITE_LOCK_RETRYABLE,
                            error = %gap_error,
                            gap_since = %first_gap_swap.ts_utc,
                            "failed to latch discovery scoring materialization gap after retryable rug finalize failure",
                        );
                    }
                }
                warn!(
                    reason = OBSERVED_SWAP_WRITER_DISCOVERY_SCORING_RUG_FINALIZE_SQLITE_LOCK_RETRYABLE,
                    error = %error,
                    watermark_ts = %last_swap.ts_utc,
                    "discovery scoring rug finalize hit retryable sqlite lock; leaving coverage watermark unchanged",
                );
                progress.gap_cursor_observed = gap_cursor_observed;
                return Ok(progress);
            }
            return Err(error).context("failed to run discovery scoring rug finalize");
        }
        cursor = DiscoveryRuntimeCursor {
            ts_utc: last_swap.ts_utc,
            slot: last_swap.slot,
            signature: last_swap.signature.clone(),
        };
        if let Err(error) = store.set_discovery_scoring_covered_through_cursor(&cursor) {
            if observed_swap_writer_discovery_scoring_covered_through_update_error_is_retryable(
                &error,
            ) {
                if let Err(gap_error) =
                    latch_discovery_scoring_materialization_gap_from_swaps(store, &page)
                {
                    if observed_swap_writer_discovery_scoring_error_requires_abort(&gap_error) {
                        return Err(gap_error).context(
                            "observed swap writer startup replay stopping after fatal discovery scoring covered_through retry gap cursor failure",
                        );
                    }
                    warn!(
                        reason = OBSERVED_SWAP_WRITER_DISCOVERY_SCORING_COVERED_THROUGH_UPDATE_SQLITE_LOCK_RETRYABLE,
                        error = %gap_error,
                        "failed to latch discovery scoring materialization gap after retryable covered_through cursor update failure",
                    );
                }
                warn!(
                    reason = OBSERVED_SWAP_WRITER_DISCOVERY_SCORING_COVERED_THROUGH_UPDATE_SQLITE_LOCK_RETRYABLE,
                    error = %error,
                    covered_through = %cursor.ts_utc,
                    "discovery scoring covered_through cursor update hit retryable sqlite lock during replay; leaving coverage watermark unchanged",
                );
                progress.gap_cursor_observed = gap_cursor_observed;
                return Ok(progress);
            }
            return Err(error)
                .context("failed to run discovery scoring covered_through cursor update");
        }
        progress.last_replay_cursor = Some(cursor.clone());
        progress.page_count = progress.page_count.saturating_add(1);

        if repair_target_cursor.is_some_and(|target| {
            compare_discovery_runtime_cursors(&cursor, target) != std::cmp::Ordering::Less
        }) {
            reached_repair_target = true;
            break;
        }

        if rows_seen < config.batch_max_size {
            progress.caught_up_to_tail = true;
            break;
        }
    }

    progress.reached_repair_target = reached_repair_target;
    let clear_ready = gap_cursor_observed
        && repair_target_cursor.map_or(progress.caught_up_to_tail, |_| reached_repair_target);
    if clear_ready {
        if let Some(gap_cursor) = gap_cursor.as_ref() {
            store.clear_discovery_scoring_materialization_gap_if_cursor_observed(gap_cursor)?;
        }
    }

    progress.gap_cursor_observed = gap_cursor_observed;
    Ok(progress)
}

fn latch_discovery_scoring_materialization_gap_from_swaps(
    store: &SqliteStore,
    inserted_swaps: &[SwapEvent],
) -> Result<()> {
    let Some(first_gap_swap) = inserted_swaps.iter().min_by(|a, b| {
        a.ts_utc
            .cmp(&b.ts_utc)
            .then_with(|| a.slot.cmp(&b.slot))
            .then_with(|| a.signature.cmp(&b.signature))
    }) else {
        return Ok(());
    };
    store.set_discovery_scoring_materialization_gap_cursor(&DiscoveryRuntimeCursor {
        ts_utc: first_gap_swap.ts_utc,
        slot: first_gap_swap.slot,
        signature: first_gap_swap.signature.clone(),
    })?;
    Ok(())
}

fn panic_payload_to_string(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        return (*message).to_string();
    }
    "unknown panic payload".to_string()
}

#[cfg(test)]
mod tests {
    use super::{
        recent_raw_journal_prune_due, ObservedSwapRecentRawJournalConfig, ObservedSwapWriter,
        ObservedSwapWriterConfig, ObservedSwapWriterTelemetry,
        OBSERVED_SWAP_DISCOVERY_AGGREGATE_IDLE_REPLAY_MAX_PAGES,
        OBSERVED_SWAP_DISCOVERY_AGGREGATE_WRITE_COALESCE_MAX_BATCHES,
    };
    use anyhow::{anyhow, Context, Result};
    use chrono::{DateTime, Duration as ChronoDuration, Utc};
    use copybot_core_types::SwapEvent;
    use copybot_storage::{
        sqlite_contention_snapshot, DiscoveryAggregateWriteConfig, DiscoveryRuntimeCursor,
        RecentRawJournalWriteSummary, SqliteContentionSnapshot, SqliteStore,
    };
    use rusqlite::Connection;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{mpsc as std_mpsc, Arc, Mutex};
    use std::thread;
    use std::time::{Duration as StdDuration, Instant};
    use tokio::runtime::Builder;
    use tokio::time::{sleep, timeout, Duration};

    fn aggregate_write_config() -> DiscoveryAggregateWriteConfig {
        DiscoveryAggregateWriteConfig::default()
    }

    fn migrated_observed_swap_writer_test_db(prefix: &str) -> Result<PathBuf> {
        let unique = format!(
            "{prefix}-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;
        Ok(db_path)
    }

    fn remove_sqlite_test_files(db_path: &Path) {
        let _ = std::fs::remove_file(db_path);
        let _ = std::fs::remove_file(format!("{}-wal", db_path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", db_path.display()));
    }

    fn startup_gate_test_swap(signature: &str, slot: u64) -> SwapEvent {
        SwapEvent {
            wallet: "wallet-startup-gate-test".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: format!("token-{signature}"),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: signature.to_string(),
            slot,
            ts_utc: DateTime::parse_from_rfc3339("2026-04-28T12:30:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        }
    }

    fn aggregate_gap_replay_test_swap(signature: &str, slot: u64, ts_utc: &str) -> SwapEvent {
        SwapEvent {
            wallet: "wallet-aggregate-gap-replay-test".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-aggregate-gap-replay-test".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: signature.to_string(),
            slot,
            ts_utc: DateTime::parse_from_rfc3339(ts_utc)
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        }
    }

    fn hot_aggregate_repair_request(idx: usize) -> super::DiscoveryAggregateWriteRequest {
        super::DiscoveryAggregateWriteRequest {
            inserted_swaps: vec![SwapEvent {
                wallet: format!("wallet-hot-aggregate-repair-{idx:05}"),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: format!("token-hot-aggregate-repair-{idx:05}"),
                amount_in: 1.0,
                amount_out: 10.0 + idx as f64,
                signature: format!("sig-hot-aggregate-repair-{idx:05}"),
                slot: 100_000 + idx as u64,
                ts_utc: DateTime::parse_from_rfc3339("2026-04-28T13:30:00Z")
                    .expect("timestamp")
                    .with_timezone(&Utc)
                    + ChronoDuration::milliseconds(idx as i64),
                exact_amounts: None,
            }],
            batch_started: Instant::now(),
        }
    }

    fn start_discovery_aggregate_writer_loop_for_test(
        db_path: &Path,
        config: ObservedSwapWriterConfig,
        channel_capacity_batches: usize,
    ) -> Result<(
        std_mpsc::SyncSender<super::DiscoveryAggregateWriteRequest>,
        thread::JoinHandle<Result<()>>,
    )> {
        let (aggregate_sender, aggregate_receiver) = std_mpsc::sync_channel::<
            super::DiscoveryAggregateWriteRequest,
        >(channel_capacity_batches);
        let (startup_sender, startup_receiver) =
            std_mpsc::channel::<std::result::Result<(), String>>();
        let telemetry = Arc::new(ObservedSwapWriterTelemetry::default());
        let aggregate_sqlite_path = db_path
            .to_str()
            .context("sqlite path must be valid utf-8")?
            .to_string();
        let worker = thread::Builder::new()
            .name("copybot-discovery-aggregate-writer-test".to_string())
            .spawn(move || {
                super::discovery_aggregate_writer_loop(
                    aggregate_sqlite_path,
                    aggregate_receiver,
                    startup_sender,
                    config,
                    telemetry,
                )
            })
            .context("failed to spawn discovery aggregate writer test thread")?;
        match startup_receiver
            .recv_timeout(StdDuration::from_secs(5))
            .context("timed out waiting for aggregate writer startup")?
        {
            Ok(()) => Ok((aggregate_sender, worker)),
            Err(message) => Err(anyhow!(message)).context("aggregate writer startup failed"),
        }
    }

    fn start_hot_aggregate_repair_traffic(
        sender: std_mpsc::SyncSender<super::DiscoveryAggregateWriteRequest>,
        stop: Arc<AtomicBool>,
        sent_count: Arc<AtomicUsize>,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let mut idx = 0usize;
            while !stop.load(Ordering::Relaxed) {
                match sender.try_send(hot_aggregate_repair_request(idx)) {
                    Ok(()) => {
                        idx = idx.saturating_add(1);
                        sent_count.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(std_mpsc::TrySendError::Full(_request)) => {
                        std::thread::sleep(StdDuration::from_millis(1));
                    }
                    Err(std_mpsc::TrySendError::Disconnected(_request)) => break,
                }
            }
        })
    }

    fn wait_for_hot_aggregate_repair_traffic(sent_count: &AtomicUsize) -> Result<()> {
        let started = Instant::now();
        loop {
            if sent_count.load(Ordering::Relaxed) > 0 {
                return Ok(());
            }
            if started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!("timed out waiting for hot aggregate repair traffic");
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }
    }

    fn wait_for_materialization_gap_clear(store: &SqliteStore) -> Result<()> {
        let started = Instant::now();
        loop {
            if store
                .load_discovery_scoring_materialization_gap_cursor()?
                .is_none()
            {
                return Ok(());
            }
            if started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!("timed out waiting for materialization gap cursor to clear");
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }
    }

    fn start_observed_swap_writer_loop_for_startup_test(
        db_path: &Path,
        config: ObservedSwapWriterConfig,
        aggregate_sender: Option<std_mpsc::SyncSender<super::DiscoveryAggregateWriteRequest>>,
        aggregate_startup_receiver: Option<std_mpsc::Receiver<std::result::Result<(), String>>>,
        journal_sender: Option<std_mpsc::SyncSender<super::RecentRawJournalWriteRequest>>,
        journal_startup_receiver: Option<std_mpsc::Receiver<std::result::Result<(), String>>>,
    ) -> Result<ObservedSwapWriter> {
        let (sender, receiver) = tokio::sync::mpsc::channel(config.channel_capacity);
        let telemetry = Arc::new(ObservedSwapWriterTelemetry::default());
        let terminal_failure_message = Arc::new(Mutex::new(None));
        let normal_try_enqueue_soft_limit =
            super::observed_swap_writer_normal_try_enqueue_soft_limit(&config);
        let raw_worker_sqlite_path = db_path
            .to_str()
            .context("sqlite path must be valid utf-8")?
            .to_string();
        let raw_worker_telemetry = Arc::clone(&telemetry);
        let raw_worker_terminal_failure_message = Arc::clone(&terminal_failure_message);
        let raw_worker = thread::Builder::new()
            .name("copybot-observed-swap-writer-startup-test".to_string())
            .spawn(move || {
                let result = super::observed_swap_writer_loop(
                    raw_worker_sqlite_path,
                    receiver,
                    aggregate_sender,
                    aggregate_startup_receiver,
                    journal_sender,
                    journal_startup_receiver,
                    config,
                    raw_worker_telemetry,
                    Arc::clone(&raw_worker_terminal_failure_message),
                );
                if let Err(error) = &result {
                    super::set_terminal_failure_message(
                        &raw_worker_terminal_failure_message,
                        format!("{error:#}"),
                    );
                }
                result
            })
            .context("failed to spawn observed swap writer startup test thread")?;

        Ok(ObservedSwapWriter {
            sender,
            normal_try_enqueue_soft_limit,
            raw_worker: Some(raw_worker),
            aggregate_worker: None,
            journal_worker: None,
            telemetry,
            terminal_failure_message,
        })
    }

    fn wait_for_observed_swap_signatures(
        db_path: &Path,
        expected_signatures: &[&str],
    ) -> Result<Vec<SwapEvent>> {
        let started = Instant::now();
        let since = DateTime::parse_from_rfc3339("2026-04-28T12:29:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        loop {
            let verify_store = SqliteStore::open(db_path)?;
            let swaps = verify_store.load_observed_swaps_since(since)?;
            if expected_signatures
                .iter()
                .all(|signature| swaps.iter().any(|swap| swap.signature == **signature))
            {
                return Ok(swaps);
            }
            if started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!(
                    "timed out waiting for observed swap signatures {:?}; saw {:?}",
                    expected_signatures,
                    swaps
                        .iter()
                        .map(|swap| swap.signature.as_str())
                        .collect::<Vec<_>>()
                );
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }
    }

    fn wait_for_writer_terminal_failure(writer: &ObservedSwapWriter) -> Result<String> {
        let started = Instant::now();
        loop {
            match writer.ensure_running() {
                Ok(()) => {}
                Err(error) => return Ok(format!("{error:#}")),
            }
            if started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!("timed out waiting for observed swap writer terminal failure");
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }
    }

    fn wait_for_observed_swap_writer_snapshot(
        writer: &ObservedSwapWriter,
        description: &str,
        predicate: impl Fn(&super::ObservedSwapWriterSnapshot) -> bool,
    ) -> Result<super::ObservedSwapWriterSnapshot> {
        let started = Instant::now();
        loop {
            let snapshot = writer.snapshot();
            if predicate(&snapshot) {
                return Ok(snapshot);
            }
            if started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!(
                    "timed out waiting for observed swap writer {description}; snapshot={snapshot:?}"
                );
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }
    }

    fn wait_for_discovery_scoring_covered_through_at_least(
        db_path: &Path,
        expected_ts: DateTime<Utc>,
    ) -> Result<()> {
        let started = Instant::now();
        loop {
            let verify_store = SqliteStore::open(db_path)?;
            if verify_store
                .load_discovery_scoring_covered_through()?
                .is_some_and(|covered_through| covered_through >= expected_ts)
            {
                return Ok(());
            }
            if started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!(
                    "timed out waiting for discovery scoring coverage through {expected_ts}"
                );
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }
    }

    fn wait_for_discovery_scoring_materialization_gap_cursor(
        db_path: &Path,
    ) -> Result<DiscoveryRuntimeCursor> {
        let started = Instant::now();
        loop {
            let verify_store = SqliteStore::open(db_path)?;
            if let Some(cursor) =
                verify_store.load_discovery_scoring_materialization_gap_cursor()?
            {
                return Ok(cursor);
            }
            if started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!("timed out waiting for discovery scoring materialization gap cursor");
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }
    }

    fn seed_discovery_scoring_rug_finalize_replay_fixture(
        db_path: &Path,
    ) -> Result<(SwapEvent, SwapEvent, SwapEvent)> {
        let seed_store = SqliteStore::open(db_path)?;
        let covered_swap = SwapEvent {
            wallet: "wallet-rug-finalize-replay".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-rug-finalize-covered".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-rug-finalize-replay-covered".to_string(),
            slot: 300,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-16T10:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };
        let first_replay_swap = SwapEvent {
            wallet: "wallet-rug-finalize-replay".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-rug-finalize-first".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-rug-finalize-replay-first".to_string(),
            slot: 301,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-16T10:00:01Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };
        let tail_replay_swap = SwapEvent {
            wallet: "wallet-rug-finalize-replay".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-rug-finalize-tail".to_string(),
            amount_in: 2.0,
            amount_out: 20.0,
            signature: "sig-rug-finalize-replay-tail".to_string(),
            slot: 302,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-16T10:00:03Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };
        seed_store.insert_observed_swaps_batch(&[
            covered_swap.clone(),
            first_replay_swap.clone(),
            tail_replay_swap.clone(),
        ])?;
        seed_store
            .apply_discovery_scoring_batch(&[covered_swap.clone()], &aggregate_write_config())?;
        seed_store.set_discovery_scoring_covered_through_cursor(&DiscoveryRuntimeCursor {
            ts_utc: covered_swap.ts_utc,
            slot: covered_swap.slot,
            signature: covered_swap.signature.clone(),
        })?;
        Ok((covered_swap, first_replay_swap, tail_replay_swap))
    }

    #[derive(Debug, Clone, Copy)]
    struct RecentRawJournalBackpressureSummary {
        baseline_rows_persisted: usize,
        pending_requests_after_load: usize,
        journal_queue_depth_after_load: usize,
        journal_overflow_depth_after_load: usize,
        max_pending_requests: usize,
        max_journal_queue_depth_batches: usize,
        max_journal_overflow_depth_batches: usize,
        persisted_rows_after_load: usize,
        runtime_wal_bytes_after_load: u64,
        sqlite_write_retry_delta: u64,
        sqlite_busy_error_delta: u64,
    }

    #[derive(Debug, Clone, Copy)]
    struct DiscoveryAggregateBackpressureSummary {
        baseline_rows_persisted: usize,
        pending_requests_after_load: usize,
        pending_requests_after_idle: usize,
        aggregate_queue_depth_after_load: usize,
        aggregate_queue_depth_after_idle: usize,
        aggregate_overflow_depth_after_load: usize,
        aggregate_overflow_depth_after_idle: usize,
        max_pending_requests: usize,
        max_aggregate_queue_depth_batches: usize,
        max_aggregate_overflow_depth_batches: usize,
        persisted_rows_after_load: usize,
        runtime_wal_bytes_after_load: u64,
        sqlite_write_retry_delta: u64,
        sqlite_busy_error_delta: u64,
        gap_cursor_present_after_load: bool,
        gap_cursor_cleared_after_idle: bool,
        covered_through_reached_tail_after_idle: bool,
    }

    fn recent_raw_journal_backpressure_swap(idx: usize, ts: DateTime<Utc>) -> SwapEvent {
        SwapEvent {
            wallet: format!("wallet-journal-backpressure-{:03}", idx % 32),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: format!("token-journal-backpressure-{:03}", idx % 64),
            amount_in: 1.0,
            amount_out: 10.0 + idx as f64,
            signature: format!("sig-journal-backpressure-{idx:05}"),
            slot: 10_000 + idx as u64,
            ts_utc: ts + ChronoDuration::milliseconds(idx as i64),
            exact_amounts: None,
        }
    }

    fn recent_raw_journal_write_request_for_test(
        start_idx: usize,
        rows: usize,
        ts: DateTime<Utc>,
    ) -> super::RecentRawJournalWriteRequest {
        super::RecentRawJournalWriteRequest {
            inserted_swaps: (start_idx..start_idx.saturating_add(rows))
                .map(|idx| recent_raw_journal_backpressure_swap(idx, ts))
                .collect(),
        }
    }

    fn recent_raw_journal_write_summary_for_test(
        processed_rows: usize,
        inserted_rows: usize,
    ) -> RecentRawJournalWriteSummary {
        RecentRawJournalWriteSummary {
            batch_rows: processed_rows,
            inserted_rows,
            recent_raw_bulk_rows_processed: processed_rows,
            recent_raw_bulk_rows_inserted: inserted_rows,
            ..RecentRawJournalWriteSummary::default()
        }
    }

    fn discovery_aggregate_backpressure_swap(idx: usize, ts: DateTime<Utc>) -> SwapEvent {
        SwapEvent {
            wallet: format!("wallet-aggregate-backpressure-{idx:05}"),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: format!("token-aggregate-backpressure-{idx:05}"),
            amount_in: 1.0,
            amount_out: 20.0 + idx as f64,
            signature: format!("sig-aggregate-backpressure-{idx:05}"),
            slot: 20_000 + idx as u64,
            ts_utc: ts + ChronoDuration::milliseconds(idx as i64),
            exact_amounts: None,
        }
    }

    fn seed_recent_raw_journal_prune_backlog(
        journal_store: &SqliteStore,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let stale_rows = (0..16_384usize)
            .map(|idx| SwapEvent {
                wallet: format!("wallet-journal-stale-{:03}", idx % 16),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: format!("token-journal-stale-{idx:04}"),
                amount_in: 1.0,
                amount_out: 5.0,
                signature: format!("sig-journal-stale-{idx:05}"),
                slot: 1_000 + idx as u64,
                ts_utc: now - ChronoDuration::days(14) + ChronoDuration::seconds(idx as i64),
                exact_amounts: None,
            })
            .collect::<Vec<_>>();
        journal_store
            .insert_recent_raw_journal_batch(&stale_rows, now - ChronoDuration::days(14))?;
        Ok(())
    }

    fn seed_discovery_aggregate_storage_backpressure(db_path: &Path) -> Result<()> {
        let conn = Connection::open(db_path)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS aggregate_backpressure_pad(
                id INTEGER PRIMARY KEY,
                payload BLOB NOT NULL
             );
             CREATE TRIGGER slow_discovery_scoring_state_insert
             AFTER INSERT ON discovery_scoring_state
             BEGIN
                 INSERT INTO aggregate_backpressure_pad(payload)
                 WITH RECURSIVE cnt(x) AS (
                     SELECT 1
                     UNION ALL
                     SELECT x + 1 FROM cnt WHERE x < 16
                 )
                 SELECT randomblob(4096) FROM cnt;
             END;",
        )?;
        Ok(())
    }

    fn run_recent_raw_journal_backpressure_scenario(
        skip_prune_while_backlogged: bool,
        write_coalesce_max_batches: usize,
        overflow_capacity_batches: usize,
    ) -> Result<RecentRawJournalBackpressureSummary> {
        let unique = format!(
            "copybot-app-recent-raw-journal-backpressure-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let runtime_db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let journal_db_path = std::env::temp_dir().join(format!("{unique}-recent-raw.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        runtime_store.run_migrations(&migration_dir)?;
        let journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
        let scenario_now = DateTime::parse_from_rfc3339("2026-04-08T09:30:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        seed_recent_raw_journal_prune_backlog(&journal_store, scenario_now)?;
        journal_store.checkpoint_wal_truncate()?;
        drop(journal_store);

        let writer = ObservedSwapWriter::start_with_config(
            runtime_db_path
                .to_str()
                .context("runtime sqlite path must be valid utf-8")?
                .to_string(),
            ObservedSwapWriterConfig::for_test(
                512,
                1,
                false,
                aggregate_write_config(),
                Some(ObservedSwapRecentRawJournalConfig {
                    sqlite_path: journal_db_path
                        .to_str()
                        .context("journal sqlite path must be valid utf-8")?
                        .to_string(),
                    retention_days: 8,
                    writer_queue_capacity_batches: 16,
                    write_coalesce_max_batches,
                    overflow_capacity_batches,
                    skip_prune_while_backlogged,
                    skip_startup_prune: true,
                }),
            ),
        )?;
        runtime_store.checkpoint_wal_truncate()?;

        let contention_before = sqlite_contention_snapshot();
        let runtime = Builder::new_current_thread().enable_all().build()?;
        for idx in 0..64usize {
            runtime.block_on(async {
                writer
                    .enqueue(&recent_raw_journal_backpressure_swap(idx, scenario_now))
                    .await
            })?;
            std::thread::sleep(StdDuration::from_millis(1));
        }

        let baseline_started = Instant::now();
        let baseline_rows_persisted = loop {
            let rows = runtime_store
                .load_observed_swaps_since(scenario_now - ChronoDuration::minutes(1))?
                .len();
            if rows >= 32 {
                break rows;
            }
            if baseline_started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!("writer failed to establish clean post-checkpoint throughput before recent_raw journal backpressure scenario");
            }
            std::thread::sleep(StdDuration::from_millis(10));
        };

        let max_pending_requests = Arc::new(AtomicUsize::new(0));
        let max_journal_queue_depth_batches = Arc::new(AtomicUsize::new(0));
        let max_journal_overflow_depth_batches = Arc::new(AtomicUsize::new(0));
        for idx in 64..4_096usize {
            runtime.block_on(async {
                writer
                    .enqueue(&recent_raw_journal_backpressure_swap(idx, scenario_now))
                    .await
            })?;
            if idx % 8 == 0 {
                std::thread::sleep(StdDuration::from_millis(1));
            }
            let snapshot = writer.snapshot();
            max_pending_requests.fetch_max(snapshot.pending_requests, Ordering::Relaxed);
            max_journal_queue_depth_batches
                .fetch_max(snapshot.journal_queue_depth_batches, Ordering::Relaxed);
            max_journal_overflow_depth_batches
                .fetch_max(snapshot.journal_overflow_depth_batches, Ordering::Relaxed);
        }
        let snapshot_after_load = writer.snapshot();

        let drain_started = Instant::now();
        while drain_started.elapsed() < StdDuration::from_millis(500) {
            let snapshot = writer.snapshot();
            max_pending_requests.fetch_max(snapshot.pending_requests, Ordering::Relaxed);
            max_journal_queue_depth_batches
                .fetch_max(snapshot.journal_queue_depth_batches, Ordering::Relaxed);
            max_journal_overflow_depth_batches
                .fetch_max(snapshot.journal_overflow_depth_batches, Ordering::Relaxed);
            std::thread::sleep(StdDuration::from_millis(10));
        }

        let persisted_rows_after_load = runtime_store
            .load_observed_swaps_since(scenario_now - ChronoDuration::minutes(1))?
            .len();
        let runtime_wal_bytes_after_load =
            std::fs::metadata(format!("{}-wal", runtime_db_path.display()))
                .map(|metadata| metadata.len())
                .unwrap_or(0);
        writer.shutdown()?;
        let contention_after = sqlite_contention_snapshot();

        let _ = std::fs::remove_file(runtime_db_path);
        let _ = std::fs::remove_file(journal_db_path);
        Ok(RecentRawJournalBackpressureSummary {
            baseline_rows_persisted,
            pending_requests_after_load: snapshot_after_load.pending_requests,
            journal_queue_depth_after_load: snapshot_after_load.journal_queue_depth_batches,
            journal_overflow_depth_after_load: snapshot_after_load.journal_overflow_depth_batches,
            max_pending_requests: max_pending_requests.load(Ordering::Relaxed),
            max_journal_queue_depth_batches: max_journal_queue_depth_batches
                .load(Ordering::Relaxed),
            max_journal_overflow_depth_batches: max_journal_overflow_depth_batches
                .load(Ordering::Relaxed),
            persisted_rows_after_load,
            runtime_wal_bytes_after_load,
            sqlite_write_retry_delta: contention_after
                .write_retry_total
                .saturating_sub(contention_before.write_retry_total),
            sqlite_busy_error_delta: contention_after
                .busy_error_total
                .saturating_sub(contention_before.busy_error_total),
        })
    }

    fn run_discovery_aggregate_backpressure_scenario(
        aggregate_write_coalesce_max_batches: usize,
        aggregate_overflow_capacity_batches: usize,
        aggregate_gap_fallback_enabled: bool,
        aggregate_idle_replay_max_pages: usize,
    ) -> Result<DiscoveryAggregateBackpressureSummary> {
        let unique = format!(
            "copybot-app-discovery-aggregate-backpressure-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let runtime_db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        runtime_store.run_migrations(&migration_dir)?;
        seed_discovery_aggregate_storage_backpressure(&runtime_db_path)?;
        runtime_store.checkpoint_wal_truncate()?;

        let writer = ObservedSwapWriter::start_with_config(
            runtime_db_path
                .to_str()
                .context("runtime sqlite path must be valid utf-8")?
                .to_string(),
            ObservedSwapWriterConfig::for_test_with_aggregate_tuning(
                512,
                8,
                true,
                aggregate_write_config(),
                aggregate_write_coalesce_max_batches,
                aggregate_overflow_capacity_batches,
                aggregate_gap_fallback_enabled,
                aggregate_idle_replay_max_pages,
                None,
            ),
        )?;
        runtime_store.checkpoint_wal_truncate()?;

        let scenario_now = DateTime::parse_from_rfc3339("2026-04-08T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let contention_before = sqlite_contention_snapshot();
        let runtime = Builder::new_current_thread().enable_all().build()?;
        let last_swap = discovery_aggregate_backpressure_swap(4_095, scenario_now);
        for idx in 0..64usize {
            runtime.block_on(async {
                writer
                    .enqueue(&discovery_aggregate_backpressure_swap(idx, scenario_now))
                    .await
            })?;
            std::thread::sleep(StdDuration::from_millis(1));
        }

        let baseline_started = Instant::now();
        let baseline_rows_persisted = loop {
            let rows = runtime_store
                .load_observed_swaps_since(scenario_now - ChronoDuration::minutes(1))?
                .len();
            if rows >= 32 {
                break rows;
            }
            if baseline_started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!("writer failed to establish clean post-checkpoint throughput before discovery aggregate backpressure scenario");
            }
            std::thread::sleep(StdDuration::from_millis(10));
        };

        let max_pending_requests = Arc::new(AtomicUsize::new(0));
        let max_aggregate_queue_depth_batches = Arc::new(AtomicUsize::new(0));
        let max_aggregate_overflow_depth_batches = Arc::new(AtomicUsize::new(0));
        for idx in 64..4_096usize {
            runtime.block_on(async {
                writer
                    .enqueue(&discovery_aggregate_backpressure_swap(idx, scenario_now))
                    .await
            })?;
            if idx % 8 == 0 {
                std::thread::sleep(StdDuration::from_millis(1));
            }
            let snapshot = writer.snapshot();
            max_pending_requests.fetch_max(snapshot.pending_requests, Ordering::Relaxed);
            max_aggregate_queue_depth_batches
                .fetch_max(snapshot.aggregate_queue_depth_batches, Ordering::Relaxed);
            max_aggregate_overflow_depth_batches
                .fetch_max(snapshot.aggregate_overflow_depth_batches, Ordering::Relaxed);
        }
        let snapshot_after_load = writer.snapshot();

        let drain_started = Instant::now();
        while drain_started.elapsed() < StdDuration::from_millis(500) {
            let snapshot = writer.snapshot();
            max_pending_requests.fetch_max(snapshot.pending_requests, Ordering::Relaxed);
            max_aggregate_queue_depth_batches
                .fetch_max(snapshot.aggregate_queue_depth_batches, Ordering::Relaxed);
            max_aggregate_overflow_depth_batches
                .fetch_max(snapshot.aggregate_overflow_depth_batches, Ordering::Relaxed);
            std::thread::sleep(StdDuration::from_millis(10));
        }

        let persisted_rows_after_load = runtime_store
            .load_observed_swaps_since(scenario_now - ChronoDuration::minutes(1))?
            .len();
        let gap_cursor_present_after_load = runtime_store
            .load_discovery_scoring_materialization_gap_cursor()?
            .is_some();
        let runtime_wal_bytes_after_load =
            std::fs::metadata(format!("{}-wal", runtime_db_path.display()))
                .map(|metadata| metadata.len())
                .unwrap_or(0);

        let idle_deadline = Instant::now() + StdDuration::from_secs(5);
        let mut gap_cursor_cleared_after_idle = false;
        let mut covered_through_reached_tail_after_idle = false;
        let mut snapshot_after_idle = writer.snapshot();
        while Instant::now() < idle_deadline {
            snapshot_after_idle = writer.snapshot();
            let gap_cursor = runtime_store.load_discovery_scoring_materialization_gap_cursor()?;
            let covered_through = runtime_store.load_discovery_scoring_covered_through_cursor()?;
            gap_cursor_cleared_after_idle = gap_cursor.is_none();
            covered_through_reached_tail_after_idle =
                covered_through.as_ref().is_some_and(|cursor| {
                    cursor.ts_utc == last_swap.ts_utc
                        && cursor.slot == last_swap.slot
                        && cursor.signature == last_swap.signature
                });
            if gap_cursor_cleared_after_idle
                && covered_through_reached_tail_after_idle
                && snapshot_after_idle.pending_requests == 0
                && snapshot_after_idle.aggregate_queue_depth_batches == 0
                && snapshot_after_idle.aggregate_overflow_depth_batches == 0
            {
                break;
            }
            std::thread::sleep(StdDuration::from_millis(25));
        }
        writer.shutdown()?;
        let contention_after = sqlite_contention_snapshot();

        let _ = std::fs::remove_file(&runtime_db_path);
        let _ = std::fs::remove_file(format!("{}-wal", runtime_db_path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", runtime_db_path.display()));
        Ok(DiscoveryAggregateBackpressureSummary {
            baseline_rows_persisted,
            pending_requests_after_load: snapshot_after_load.pending_requests,
            pending_requests_after_idle: snapshot_after_idle.pending_requests,
            aggregate_queue_depth_after_load: snapshot_after_load.aggregate_queue_depth_batches,
            aggregate_queue_depth_after_idle: snapshot_after_idle.aggregate_queue_depth_batches,
            aggregate_overflow_depth_after_load: snapshot_after_load
                .aggregate_overflow_depth_batches,
            aggregate_overflow_depth_after_idle: snapshot_after_idle
                .aggregate_overflow_depth_batches,
            max_pending_requests: max_pending_requests.load(Ordering::Relaxed),
            max_aggregate_queue_depth_batches: max_aggregate_queue_depth_batches
                .load(Ordering::Relaxed),
            max_aggregate_overflow_depth_batches: max_aggregate_overflow_depth_batches
                .load(Ordering::Relaxed),
            persisted_rows_after_load,
            runtime_wal_bytes_after_load,
            sqlite_write_retry_delta: contention_after
                .write_retry_total
                .saturating_sub(contention_before.write_retry_total),
            sqlite_busy_error_delta: contention_after
                .busy_error_total
                .saturating_sub(contention_before.busy_error_total),
            gap_cursor_present_after_load,
            gap_cursor_cleared_after_idle,
            covered_through_reached_tail_after_idle,
        })
    }

    #[test]
    fn observed_swap_writer_does_not_block_runtime_under_sqlite_lock() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-writer-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let blocker_conn = Connection::open(Path::new(&db_path))
            .context("failed to open blocker sqlite connection")?;
        blocker_conn
            .busy_timeout(StdDuration::from_millis(1))
            .context("failed to shorten blocker busy timeout")?;
        blocker_conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let sqlite_path = db_path
            .to_str()
            .context("sqlite path must be valid utf-8")?
            .to_string();
        let swap = SwapEvent {
            wallet: "wallet-async".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-async".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-observed-swap-async".to_string(),
            slot: 123,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T12:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };

        let runtime_handle = thread::spawn(move || -> Result<bool> {
            let runtime = Builder::new_current_thread().enable_all().build()?;
            runtime.block_on(async move {
                let writer =
                    ObservedSwapWriter::start(sqlite_path.clone(), true, aggregate_write_config())?;
                let swap_for_task = swap.clone();
                let insert_task = tokio::spawn(async move { writer.write(&swap_for_task).await });

                timeout(Duration::from_millis(50), sleep(Duration::from_millis(10)))
                    .await
                    .context(
                        "current-thread runtime stalled while observed swap writer was blocked",
                    )?;

                insert_task
                    .await
                    .context("observed swap task join failed")?
            })
        });

        std::thread::sleep(StdDuration::from_millis(250));
        blocker_conn.execute_batch("COMMIT")?;

        let inserted = runtime_handle
            .join()
            .expect("runtime thread panicked")
            .context("observed swap write should succeed after lock release")?;
        assert!(inserted, "observed swap insert should report a fresh write");

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let swaps = verify_store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-06T11:59:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(swaps.len(), 1);
        assert_eq!(swaps[0].signature, "sig-observed-swap-async");
        let _ = std::fs::remove_file(db_path);

        Ok(())
    }

    #[test]
    fn observed_swap_writer_enqueue_returns_before_locked_batch_commits() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-enqueue-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let blocker_conn = Connection::open(Path::new(&db_path))
            .context("failed to open blocker sqlite connection")?;
        blocker_conn
            .busy_timeout(StdDuration::from_millis(1))
            .context("failed to shorten blocker busy timeout")?;
        blocker_conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let sqlite_path = db_path
            .to_str()
            .context("sqlite path must be valid utf-8")?
            .to_string();
        let swap = SwapEvent {
            wallet: "wallet-enqueue".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-enqueue".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-observed-swap-enqueue".to_string(),
            slot: 124,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-14T12:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };

        let runtime = Builder::new_current_thread().enable_all().build()?;
        let writer = runtime.block_on(async move {
            let writer = ObservedSwapWriter::start(sqlite_path, true, aggregate_write_config())?;
            timeout(Duration::from_millis(50), writer.enqueue(&swap))
                .await
                .context("observed swap enqueue should not wait for batch commit")??;
            Ok::<ObservedSwapWriter, anyhow::Error>(writer)
        })?;
        let pending_snapshot = writer.snapshot();
        assert_eq!(
            pending_snapshot.pending_requests, 1,
            "snapshot should expose the locked batch as one pending observed swap write"
        );

        let verify_before_commit = SqliteStore::open(Path::new(&db_path))?;
        let before_swaps = verify_before_commit.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-14T11:59:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert!(
            before_swaps.is_empty(),
            "enqueue should not imply the batch has already committed under sqlite lock"
        );

        std::thread::sleep(StdDuration::from_millis(50));
        blocker_conn.execute_batch("COMMIT")?;
        std::thread::sleep(StdDuration::from_millis(50));
        let committed_snapshot = writer.snapshot();
        assert_eq!(
            committed_snapshot.pending_requests, 0,
            "snapshot should clear pending depth after the blocked batch commits"
        );
        assert!(
            committed_snapshot.write_latency_ms_p95 >= 40,
            "snapshot should retain queue+commit latency once the blocked batch completes"
        );
        assert!(
            committed_snapshot.raw_batch_write_ms_p95 >= 40,
            "snapshot should expose raw batch latency separately when sqlite lock blocks the batch commit"
        );
        writer.shutdown()?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let swaps = verify_store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-14T11:59:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(swaps.len(), 1);
        assert_eq!(swaps[0].signature, "sig-observed-swap-enqueue");
        let _ = std::fs::remove_file(db_path);

        Ok(())
    }

    #[test]
    fn observed_swap_writer_retries_retryable_raw_lock_without_terminal_failure() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-retryable-lock-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let control_conn = Connection::open(Path::new(&db_path))
            .context("failed to open retryable-lock control sqlite connection")?;
        control_conn.execute_batch(
            "CREATE TABLE raw_write_lock_gate(locked INTEGER NOT NULL);
             INSERT INTO raw_write_lock_gate(locked) VALUES (1);
             CREATE TRIGGER block_observed_swap_insert_retryable
             BEFORE INSERT ON observed_swaps
             WHEN (SELECT locked FROM raw_write_lock_gate LIMIT 1) = 1
             BEGIN
                 SELECT RAISE(FAIL, 'database is locked');
             END;",
        )?;

        let sqlite_path = db_path
            .to_str()
            .context("sqlite path must be valid utf-8")?
            .to_string();
        let swap = SwapEvent {
            wallet: "wallet-retryable-lock".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-retryable-lock".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-observed-swap-retryable-lock".to_string(),
            slot: 127,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-18T10:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };

        let runtime_handle = thread::spawn(move || -> Result<()> {
            let runtime = Builder::new_current_thread().enable_all().build()?;
            runtime.block_on(async move {
                let writer = ObservedSwapWriter::start(
                    sqlite_path,
                    false,
                    aggregate_write_config(),
                )?;
                timeout(Duration::from_millis(50), writer.enqueue(&swap))
                    .await
                    .context("retryable raw lock enqueue should not block runtime")??;
                timeout(Duration::from_millis(50), sleep(Duration::from_millis(10)))
                    .await
                    .context("current-thread runtime stalled while raw writer retried retryable lock")?;
                timeout(Duration::from_secs(5), async {
                    loop {
                        writer.ensure_running()?;
                        if writer.snapshot().pending_requests == 0 {
                            break Ok::<(), anyhow::Error>(());
                        }
                        sleep(Duration::from_millis(20)).await;
                    }
                })
                .await
                .context("retryable raw lock batch should eventually drain without terminal writer failure")??;
                writer.ensure_running()?;
                writer.shutdown()?;
                Ok::<(), anyhow::Error>(())
            })
        });

        std::thread::sleep(StdDuration::from_millis(250));
        control_conn.execute("UPDATE raw_write_lock_gate SET locked = 0", [])?;

        runtime_handle
            .join()
            .expect("runtime thread panicked")
            .context("observed swap writer should survive retryable raw lock and recover")?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let swaps = verify_store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-18T09:59:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(swaps.len(), 1);
        assert_eq!(swaps[0].signature, "sig-observed-swap-retryable-lock");
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_try_enqueue_returns_false_when_channel_is_full() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-try-enqueue-full-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let blocker_conn = Connection::open(Path::new(&db_path))
            .context("failed to open blocker sqlite connection")?;
        blocker_conn
            .busy_timeout(StdDuration::from_millis(1))
            .context("failed to shorten blocker busy timeout")?;
        blocker_conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let writer = ObservedSwapWriter::start_with_config(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?
                .to_string(),
            ObservedSwapWriterConfig::for_test(1, 1, true, aggregate_write_config(), None),
        )?;

        let first_swap = SwapEvent {
            wallet: "wallet-try-enqueue".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-try-enqueue-a".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-try-enqueue-a".to_string(),
            slot: 125,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-14T12:10:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };
        assert!(writer.try_enqueue(&first_swap)?);
        let mut saw_full = false;
        for idx in 0..32u64 {
            let swap = SwapEvent {
                token_out: format!("token-try-enqueue-{idx}"),
                signature: format!("sig-try-enqueue-{idx}"),
                slot: 126 + idx,
                ts_utc: DateTime::parse_from_rfc3339("2026-03-14T12:10:01Z")
                    .expect("timestamp")
                    .with_timezone(&Utc),
                ..first_swap.clone()
            };
            if !writer.try_enqueue(&swap)? {
                saw_full = true;
                break;
            }
        }
        assert!(
            saw_full,
            "non-blocking try_enqueue should report a full channel instead of waiting once the bounded queue saturates"
        );

        blocker_conn.execute_batch("COMMIT")?;
        std::thread::sleep(StdDuration::from_millis(50));
        writer.shutdown()?;
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_discovery_critical_enqueue_uses_reserved_capacity_before_full_stage1(
    ) -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-critical-reserve-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let blocker_conn = Connection::open(Path::new(&db_path))
            .context("failed to open blocker sqlite connection")?;
        blocker_conn
            .busy_timeout(StdDuration::from_millis(1))
            .context("failed to shorten blocker busy timeout")?;
        blocker_conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let writer = ObservedSwapWriter::start_with_config(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?
                .to_string(),
            ObservedSwapWriterConfig::for_test(2, 1, false, aggregate_write_config(), None),
        )?;

        let normal_swap = SwapEvent {
            wallet: "wallet-critical-reserve".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-critical-reserve-normal".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-critical-reserve-normal".to_string(),
            slot: 125,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-14T12:12:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };
        let discovery_critical_swap = SwapEvent {
            token_out: "token-critical-reserve-priority".to_string(),
            signature: "sig-critical-reserve-priority".to_string(),
            slot: 126,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-14T12:12:01Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            ..normal_swap.clone()
        };

        assert!(writer.try_enqueue(&normal_swap)?);
        assert!(
            !writer.try_enqueue(&SwapEvent {
                token_out: "token-critical-reserve-blocked".to_string(),
                signature: "sig-critical-reserve-blocked".to_string(),
                slot: 127,
                ts_utc: DateTime::parse_from_rfc3339("2026-03-14T12:12:02Z")
                    .expect("timestamp")
                    .with_timezone(&Utc),
                ..normal_swap.clone()
            })?,
            "normal best-effort enqueue should yield once the reserved discovery-critical capacity is the only space left"
        );
        assert!(
            writer.try_enqueue_discovery_critical(&discovery_critical_swap)?,
            "discovery-critical enqueue should still claim the reserved writer slot"
        );

        blocker_conn.execute_batch("COMMIT")?;
        std::thread::sleep(StdDuration::from_millis(50));
        writer.shutdown()?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let swaps = verify_store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-14T12:11:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(swaps.len(), 2);
        assert!(
            swaps
                .iter()
                .any(|swap| swap.signature == "sig-critical-reserve-priority"),
            "the discovery-critical swap should still persist after the raw writer unblocks"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_try_enqueue_aggregate_enabled_uses_normal_capacity_and_preserves_discovery_reserve_stage1(
    ) -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-aggregate-enabled-normal-capacity-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let writer = ObservedSwapWriter::start_with_config(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?
                .to_string(),
            ObservedSwapWriterConfig::for_test(4, 1, true, aggregate_write_config(), None),
        )?;
        std::thread::sleep(StdDuration::from_millis(50));

        let blocker_conn = Connection::open(Path::new(&db_path))
            .context("failed to open blocker sqlite connection")?;
        blocker_conn
            .busy_timeout(StdDuration::from_millis(1))
            .context("failed to shorten blocker busy timeout")?;
        blocker_conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let base_swap = SwapEvent {
            wallet: "wallet-aggregate-enabled-normal-capacity".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-aggregate-enabled-normal-capacity-0".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-aggregate-enabled-normal-capacity-0".to_string(),
            slot: 500,
            ts_utc: DateTime::parse_from_rfc3339("2026-04-28T12:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };
        let config = ObservedSwapWriterConfig::for_test(4, 1, true, aggregate_write_config(), None);
        let discovery_critical_reserve =
            super::observed_swap_writer_discovery_critical_reserve_requests(&config);
        let normal_capacity = 4usize.saturating_sub(discovery_critical_reserve);
        assert_eq!(normal_capacity, 3);

        for idx in 0..normal_capacity {
            assert!(
                writer.try_enqueue(&SwapEvent {
                    token_out: format!("token-aggregate-enabled-normal-capacity-{idx}"),
                    signature: format!("sig-aggregate-enabled-normal-capacity-{idx}"),
                    slot: 500 + idx as u64,
                    ..base_swap.clone()
                })?,
                "aggregate-enabled normal try_enqueue should accept every non-reserved writer slot"
            );
        }
        assert!(
            !writer.try_enqueue(&SwapEvent {
                token_out: "token-aggregate-enabled-normal-capacity-blocked".to_string(),
                signature: "sig-aggregate-enabled-normal-capacity-blocked".to_string(),
                slot: 600,
                ..base_swap.clone()
            })?,
            "one more normal try_enqueue must yield before consuming discovery-critical reserve"
        );
        assert!(
            writer.try_enqueue_discovery_critical(&SwapEvent {
                token_out: "token-aggregate-enabled-normal-capacity-critical".to_string(),
                signature: "sig-aggregate-enabled-normal-capacity-critical".to_string(),
                slot: 601,
                ..base_swap.clone()
            })?,
            "discovery-critical enqueue must still claim the reserved writer slot"
        );

        blocker_conn.execute_batch("COMMIT")?;
        let drain_started = Instant::now();
        while writer.snapshot().pending_requests > 0 {
            if drain_started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!(
                    "writer failed to drain after aggregate-enabled normal-capacity try_enqueue test"
                );
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }
        writer.shutdown()?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let swaps = verify_store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-04-28T11:59:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(
            swaps.len(),
            normal_capacity + 1,
            "accepted normal swaps plus the reserved discovery-critical swap should persist"
        );
        assert!(
            swaps
                .iter()
                .any(|swap| { swap.signature == "sig-aggregate-enabled-normal-capacity-critical" }),
            "the reserved discovery-critical swap should be persisted"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_file(format!("{}-wal", db_path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", db_path.display()));
        Ok(())
    }

    #[test]
    fn observed_swap_writer_aggregate_startup_pending_does_not_block_raw_insert_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-aggregate-startup-pending",
        )?;
        let (startup_sender, startup_receiver) =
            std_mpsc::channel::<std::result::Result<(), String>>();
        let (aggregate_sender, _aggregate_receiver) =
            std_mpsc::sync_channel::<super::DiscoveryAggregateWriteRequest>(4);
        let writer = start_observed_swap_writer_loop_for_startup_test(
            &db_path,
            ObservedSwapWriterConfig::for_test(4, 1, true, aggregate_write_config(), None),
            Some(aggregate_sender),
            Some(startup_receiver),
            None,
            None,
        )?;

        let swap = startup_gate_test_swap("sig-aggregate-startup-pending-raw-insert", 700);
        assert!(
            writer.try_enqueue(&swap)?,
            "raw enqueue should be accepted while aggregate startup is pending"
        );
        let swaps = wait_for_observed_swap_signatures(&db_path, &[&swap.signature])?;
        assert!(
            swaps
                .iter()
                .any(|persisted| persisted.signature == swap.signature),
            "raw observed_swaps insert should not wait for aggregate startup replay"
        );

        startup_sender
            .send(Ok(()))
            .context("failed sending aggregate startup ok")?;
        writer.shutdown()?;
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_recent_raw_startup_pending_does_not_block_raw_insert_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-recent-raw-startup-pending",
        )?;
        let (startup_sender, startup_receiver) =
            std_mpsc::channel::<std::result::Result<(), String>>();
        let (journal_sender, _journal_receiver) =
            std_mpsc::sync_channel::<super::RecentRawJournalWriteRequest>(4);
        let writer = start_observed_swap_writer_loop_for_startup_test(
            &db_path,
            ObservedSwapWriterConfig::for_test(4, 1, false, aggregate_write_config(), None),
            None,
            None,
            Some(journal_sender),
            Some(startup_receiver),
        )?;

        let swap = startup_gate_test_swap("sig-recent-raw-startup-pending-raw-insert", 701);
        assert!(
            writer.try_enqueue(&swap)?,
            "raw enqueue should be accepted while recent_raw startup is pending"
        );
        let swaps = wait_for_observed_swap_signatures(&db_path, &[&swap.signature])?;
        assert!(
            swaps
                .iter()
                .any(|persisted| persisted.signature == swap.signature),
            "raw observed_swaps insert should not wait for recent_raw journal startup/prune"
        );

        startup_sender
            .send(Ok(()))
            .context("failed sending recent_raw startup ok")?;
        writer.shutdown()?;
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_downstream_startup_error_stops_fail_closed_stage1() -> Result<()> {
        let db_path =
            migrated_observed_swap_writer_test_db("copybot-app-observed-swap-startup-error")?;
        let (startup_sender, startup_receiver) =
            std_mpsc::channel::<std::result::Result<(), String>>();
        let writer = start_observed_swap_writer_loop_for_startup_test(
            &db_path,
            ObservedSwapWriterConfig::for_test(4, 1, true, aggregate_write_config(), None),
            None,
            Some(startup_receiver),
            None,
            None,
        )?;

        startup_sender
            .send(Err("aggregate startup failed for test".to_string()))
            .context("failed sending aggregate startup error")?;
        let terminal_failure = wait_for_writer_terminal_failure(&writer)?;
        assert!(
            terminal_failure
                .contains("observed swap writer stopping after aggregate startup replay failure"),
            "terminal failure should include aggregate startup context: {terminal_failure}"
        );
        assert!(
            terminal_failure.contains("aggregate startup failed for test"),
            "terminal failure should include downstream startup error: {terminal_failure}"
        );

        let shutdown_error = writer
            .shutdown()
            .expect_err("startup failure should make writer shutdown fail closed");
        let shutdown_error_text = format!("{shutdown_error:#}");
        assert!(
            shutdown_error_text
                .contains("observed swap writer stopping after aggregate startup replay failure"),
            "shutdown error should preserve startup context: {shutdown_error_text}"
        );
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_aggregate_gap_replay_clears_gap_behind_covered_through_after_observing_exact_row_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-gap-behind-covered-clear",
        )?;
        let store = SqliteStore::open(Path::new(&db_path))?;
        let gap_swap = aggregate_gap_replay_test_swap(
            "sig-gap-behind-covered-exact",
            800,
            "2026-04-28T13:00:00Z",
        );
        let tail_swap = aggregate_gap_replay_test_swap(
            "sig-gap-behind-covered-tail",
            801,
            "2026-04-28T13:00:05Z",
        );
        store.insert_observed_swaps_batch(&[gap_swap.clone(), tail_swap.clone()])?;
        let gap_cursor = DiscoveryRuntimeCursor {
            ts_utc: gap_swap.ts_utc,
            slot: gap_swap.slot,
            signature: gap_swap.signature.clone(),
        };
        let tail_cursor = DiscoveryRuntimeCursor {
            ts_utc: tail_swap.ts_utc,
            slot: tail_swap.slot,
            signature: tail_swap.signature.clone(),
        };
        store.set_discovery_scoring_covered_through_cursor(&tail_cursor)?;
        store.set_discovery_scoring_materialization_gap_cursor(&gap_cursor)?;

        let progress = super::run_aggregate_gap_replay(
            &store,
            &ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
            None,
        )?;

        assert!(
            progress.gap_cursor_observed,
            "gap replay must observe the exact latched row before clearing"
        );
        assert!(
            progress.caught_up_to_tail,
            "unbounded gap replay should catch up to tail before clearing"
        );
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_cursor()?,
            None,
            "gap cursor behind covered_through should clear only after exact row observation and tail catch-up"
        );
        assert_eq!(
            store.load_discovery_scoring_covered_through_cursor()?,
            Some(tail_cursor),
            "gap replay must not move covered_through backwards"
        );
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_aggregate_gap_replay_keeps_gap_behind_covered_through_when_exact_row_missing_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-gap-behind-covered-missing",
        )?;
        let store = SqliteStore::open(Path::new(&db_path))?;
        let missing_gap = aggregate_gap_replay_test_swap(
            "sig-gap-behind-covered-missing",
            810,
            "2026-04-28T13:05:00Z",
        );
        let tail_swap = aggregate_gap_replay_test_swap(
            "sig-gap-behind-covered-missing-tail",
            811,
            "2026-04-28T13:05:05Z",
        );
        store.insert_observed_swaps_batch(&[tail_swap.clone()])?;
        let gap_cursor = DiscoveryRuntimeCursor {
            ts_utc: missing_gap.ts_utc,
            slot: missing_gap.slot,
            signature: missing_gap.signature.clone(),
        };
        let tail_cursor = DiscoveryRuntimeCursor {
            ts_utc: tail_swap.ts_utc,
            slot: tail_swap.slot,
            signature: tail_swap.signature.clone(),
        };
        store.set_discovery_scoring_covered_through_cursor(&tail_cursor)?;
        store.set_discovery_scoring_materialization_gap_cursor(&gap_cursor)?;

        let progress = super::run_aggregate_gap_replay(
            &store,
            &ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
            None,
        )?;

        assert!(
            !progress.gap_cursor_observed,
            "gap replay must not claim observation when the exact latched row is absent"
        );
        assert!(
            progress.caught_up_to_tail,
            "replay can catch tail while still keeping missing exact-gap evidence latched"
        );
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_cursor()?,
            Some(gap_cursor),
            "missing exact latched row must keep materialization gap fail-closed"
        );
        assert_eq!(
            store.load_discovery_scoring_covered_through_cursor()?,
            Some(tail_cursor),
            "missing-gap replay must not regress covered_through"
        );
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_aggregate_gap_replay_partial_gap_behind_covered_through_does_not_move_coverage_backwards_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-gap-behind-covered-partial",
        )?;
        let store = SqliteStore::open(Path::new(&db_path))?;
        let gap_swap = aggregate_gap_replay_test_swap(
            "sig-gap-behind-covered-partial",
            820,
            "2026-04-28T13:10:00Z",
        );
        let tail_swap = aggregate_gap_replay_test_swap(
            "sig-gap-behind-covered-partial-tail",
            821,
            "2026-04-28T13:10:05Z",
        );
        store.insert_observed_swaps_batch(&[gap_swap.clone(), tail_swap.clone()])?;
        let gap_cursor = DiscoveryRuntimeCursor {
            ts_utc: gap_swap.ts_utc,
            slot: gap_swap.slot,
            signature: gap_swap.signature.clone(),
        };
        let tail_cursor = DiscoveryRuntimeCursor {
            ts_utc: tail_swap.ts_utc,
            slot: tail_swap.slot,
            signature: tail_swap.signature.clone(),
        };
        store.set_discovery_scoring_covered_through_cursor(&tail_cursor)?;
        store.set_discovery_scoring_materialization_gap_cursor(&gap_cursor)?;

        let progress = super::run_aggregate_gap_replay(
            &store,
            &ObservedSwapWriterConfig::for_test(16, 1, true, aggregate_write_config(), None),
            Some(1),
        )?;

        assert!(
            progress.gap_cursor_observed,
            "partial replay should still observe the exact latched row when starting from the gap boundary"
        );
        assert!(
            !progress.caught_up_to_tail,
            "one-row bounded replay should not claim tail catch-up"
        );
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_cursor()?,
            Some(gap_cursor),
            "partial replay must keep gap latched until tail catch-up"
        );
        assert_eq!(
            store.load_discovery_scoring_covered_through_cursor()?,
            Some(tail_cursor),
            "bounded replay from behind covered_through must not regress covered_through"
        );
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_aggregate_gap_repair_clears_at_frozen_target_while_tail_moves_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-gap-frozen-target-tail-moves",
        )?;
        let store = SqliteStore::open(Path::new(&db_path))?;
        let gap_swap = aggregate_gap_replay_test_swap(
            "sig-gap-frozen-target-exact",
            825,
            "2026-04-28T13:12:00Z",
        );
        let target_swap = aggregate_gap_replay_test_swap(
            "sig-gap-frozen-target-target",
            826,
            "2026-04-28T13:12:05Z",
        );
        store.insert_observed_swaps_batch(&[gap_swap.clone(), target_swap.clone()])?;
        let gap_cursor = DiscoveryRuntimeCursor {
            ts_utc: gap_swap.ts_utc,
            slot: gap_swap.slot,
            signature: gap_swap.signature.clone(),
        };
        let target_cursor = DiscoveryRuntimeCursor {
            ts_utc: target_swap.ts_utc,
            slot: target_swap.slot,
            signature: target_swap.signature.clone(),
        };
        store.set_discovery_scoring_covered_through_cursor(&target_cursor)?;
        store.set_discovery_scoring_materialization_gap_cursor(&gap_cursor)?;
        let config = ObservedSwapWriterConfig::for_test_with_aggregate_tuning(
            16,
            1,
            true,
            aggregate_write_config(),
            1,
            16,
            true,
            1,
            None,
        );
        let mut repair_epoch = super::DiscoveryAggregateGapRepairEpoch::default();

        super::run_discovery_aggregate_gap_repair_slice(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?,
            &store,
            &config,
            &ObservedSwapWriterTelemetry::default(),
            true,
            &mut repair_epoch,
        )?;
        assert_eq!(
            repair_epoch.repair_target_cursor,
            Some(target_cursor.clone()),
            "repair epoch must freeze the observed_swaps tail as its target"
        );
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_cursor()?,
            Some(gap_cursor.clone()),
            "first bounded slice observed the exact gap but has not reached the frozen target"
        );

        let later_swap = aggregate_gap_replay_test_swap(
            "sig-gap-frozen-target-later-live-tail",
            827,
            "2026-04-28T13:12:10Z",
        );
        let later_cursor = DiscoveryRuntimeCursor {
            ts_utc: later_swap.ts_utc,
            slot: later_swap.slot,
            signature: later_swap.signature.clone(),
        };
        store.insert_observed_swaps_batch(&[later_swap])?;
        store.set_discovery_scoring_covered_through_cursor(&later_cursor)?;

        super::run_discovery_aggregate_gap_repair_slice(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?,
            &store,
            &config,
            &ObservedSwapWriterTelemetry::default(),
            true,
            &mut repair_epoch,
        )?;
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_cursor()?,
            None,
            "repair must clear after reaching the frozen target without chasing the moving live tail"
        );
        assert_eq!(
            store.load_discovery_scoring_covered_through_cursor()?,
            Some(later_cursor),
            "bounded repair must not regress covered_through when live traffic already advanced it"
        );
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_aggregate_gap_repair_resets_epoch_when_gap_cursor_changes_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-gap-repair-epoch-reset",
        )?;
        let store = SqliteStore::open(Path::new(&db_path))?;
        let earlier_gap_swap = aggregate_gap_replay_test_swap(
            "sig-gap-epoch-reset-earlier",
            824,
            "2026-04-28T13:11:55Z",
        );
        let first_gap_swap = aggregate_gap_replay_test_swap(
            "sig-gap-epoch-reset-first",
            825,
            "2026-04-28T13:12:00Z",
        );
        let target_swap = aggregate_gap_replay_test_swap(
            "sig-gap-epoch-reset-target",
            826,
            "2026-04-28T13:12:05Z",
        );
        store.insert_observed_swaps_batch(&[
            earlier_gap_swap.clone(),
            first_gap_swap.clone(),
            target_swap.clone(),
        ])?;
        let earlier_gap_cursor = DiscoveryRuntimeCursor {
            ts_utc: earlier_gap_swap.ts_utc,
            slot: earlier_gap_swap.slot,
            signature: earlier_gap_swap.signature.clone(),
        };
        let first_gap_cursor = DiscoveryRuntimeCursor {
            ts_utc: first_gap_swap.ts_utc,
            slot: first_gap_swap.slot,
            signature: first_gap_swap.signature.clone(),
        };
        let target_cursor = DiscoveryRuntimeCursor {
            ts_utc: target_swap.ts_utc,
            slot: target_swap.slot,
            signature: target_swap.signature.clone(),
        };
        store.set_discovery_scoring_covered_through_cursor(&target_cursor)?;
        store.set_discovery_scoring_materialization_gap_cursor(&first_gap_cursor)?;
        let config = ObservedSwapWriterConfig::for_test_with_aggregate_tuning(
            16,
            1,
            true,
            aggregate_write_config(),
            1,
            16,
            true,
            1,
            None,
        );
        let mut repair_epoch = super::DiscoveryAggregateGapRepairEpoch::default();

        super::run_discovery_aggregate_gap_repair_slice(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?,
            &store,
            &config,
            &ObservedSwapWriterTelemetry::default(),
            true,
            &mut repair_epoch,
        )?;
        assert_eq!(repair_epoch.gap_cursor, Some(first_gap_cursor));
        assert_eq!(
            repair_epoch
                .resume_after_cursor
                .as_ref()
                .map(|cursor| cursor.signature.as_str()),
            Some("sig-gap-epoch-reset-first")
        );

        store.set_discovery_scoring_materialization_gap_cursor(&earlier_gap_cursor)?;
        super::run_discovery_aggregate_gap_repair_slice(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?,
            &store,
            &config,
            &ObservedSwapWriterTelemetry::default(),
            true,
            &mut repair_epoch,
        )?;
        assert_eq!(
            repair_epoch.gap_cursor,
            Some(earlier_gap_cursor.clone()),
            "changed gap cursor must discard the previous repair epoch"
        );
        assert_eq!(
            repair_epoch
                .resume_after_cursor
                .as_ref()
                .map(|cursor| cursor.signature.as_str()),
            Some("sig-gap-epoch-reset-earlier"),
            "new epoch must resume from the new gap cursor, not the stale old cursor"
        );
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_cursor()?,
            Some(earlier_gap_cursor),
            "one bounded slice must not clear before the frozen target is reached"
        );
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_aggregate_gap_repair_partial_slices_keep_latch_until_target_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-gap-repair-partial-target",
        )?;
        let store = SqliteStore::open(Path::new(&db_path))?;
        let gap_swap = aggregate_gap_replay_test_swap(
            "sig-gap-partial-target-exact",
            825,
            "2026-04-28T13:12:00Z",
        );
        let middle_swap = aggregate_gap_replay_test_swap(
            "sig-gap-partial-target-middle",
            826,
            "2026-04-28T13:12:05Z",
        );
        let target_swap = aggregate_gap_replay_test_swap(
            "sig-gap-partial-target-target",
            827,
            "2026-04-28T13:12:10Z",
        );
        store.insert_observed_swaps_batch(&[
            gap_swap.clone(),
            middle_swap.clone(),
            target_swap.clone(),
        ])?;
        let gap_cursor = DiscoveryRuntimeCursor {
            ts_utc: gap_swap.ts_utc,
            slot: gap_swap.slot,
            signature: gap_swap.signature.clone(),
        };
        let target_cursor = DiscoveryRuntimeCursor {
            ts_utc: target_swap.ts_utc,
            slot: target_swap.slot,
            signature: target_swap.signature.clone(),
        };
        store.set_discovery_scoring_covered_through_cursor(&target_cursor)?;
        store.set_discovery_scoring_materialization_gap_cursor(&gap_cursor)?;
        let config = ObservedSwapWriterConfig::for_test_with_aggregate_tuning(
            16,
            1,
            true,
            aggregate_write_config(),
            1,
            16,
            true,
            1,
            None,
        );
        let mut repair_epoch = super::DiscoveryAggregateGapRepairEpoch::default();

        for expected_resume_signature in [
            "sig-gap-partial-target-exact",
            "sig-gap-partial-target-middle",
        ] {
            super::run_discovery_aggregate_gap_repair_slice(
                db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?,
                &store,
                &config,
                &ObservedSwapWriterTelemetry::default(),
                true,
                &mut repair_epoch,
            )?;
            assert_eq!(
                store.load_discovery_scoring_materialization_gap_cursor()?,
                Some(gap_cursor.clone()),
                "partial bounded repair must keep latch until the frozen target is reached"
            );
            assert_eq!(
                repair_epoch
                    .resume_after_cursor
                    .as_ref()
                    .map(|cursor| cursor.signature.as_str()),
                Some(expected_resume_signature)
            );
        }

        super::run_discovery_aggregate_gap_repair_slice(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?,
            &store,
            &config,
            &ObservedSwapWriterTelemetry::default(),
            true,
            &mut repair_epoch,
        )?;
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_cursor()?,
            None,
            "repair should clear only after the frozen target row is replayed"
        );
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_aggregate_gap_repair_runs_under_continuous_hot_traffic_stage1(
    ) -> Result<()> {
        let db_path =
            migrated_observed_swap_writer_test_db("copybot-app-observed-swap-hot-gap-repair")?;
        let store = SqliteStore::open(Path::new(&db_path))?;
        let gap_swap =
            aggregate_gap_replay_test_swap("sig-hot-gap-repair-exact", 830, "2026-04-28T13:15:00Z");
        let tail_swap =
            aggregate_gap_replay_test_swap("sig-hot-gap-repair-tail", 831, "2026-04-28T13:15:05Z");
        store.insert_observed_swaps_batch(&[gap_swap.clone(), tail_swap.clone()])?;
        let gap_cursor = DiscoveryRuntimeCursor {
            ts_utc: gap_swap.ts_utc,
            slot: gap_swap.slot,
            signature: gap_swap.signature.clone(),
        };
        let tail_cursor = DiscoveryRuntimeCursor {
            ts_utc: tail_swap.ts_utc,
            slot: tail_swap.slot,
            signature: tail_swap.signature.clone(),
        };
        store.set_discovery_scoring_covered_through_cursor(&tail_cursor)?;
        let (aggregate_sender, aggregate_worker) = start_discovery_aggregate_writer_loop_for_test(
            &db_path,
            ObservedSwapWriterConfig::for_test_with_aggregate_tuning(
                64,
                1,
                true,
                aggregate_write_config(),
                1,
                64,
                true,
                1,
                None,
            ),
            128,
        )?;
        store.set_discovery_scoring_materialization_gap_cursor(&gap_cursor)?;

        let stop = Arc::new(AtomicBool::new(false));
        let sent_count = Arc::new(AtomicUsize::new(0));
        let traffic = start_hot_aggregate_repair_traffic(
            aggregate_sender.clone(),
            Arc::clone(&stop),
            Arc::clone(&sent_count),
        );
        wait_for_hot_aggregate_repair_traffic(&sent_count)?;
        wait_for_materialization_gap_clear(&store)?;
        stop.store(true, Ordering::Relaxed);
        drop(aggregate_sender);
        traffic
            .join()
            .expect("hot aggregate traffic thread panicked");
        aggregate_worker
            .join()
            .expect("aggregate writer test thread panicked")?;

        assert!(
            sent_count.load(Ordering::Relaxed) > 0,
            "test must keep hot aggregate requests flowing while repair clears the gap"
        );
        let covered_through = store
            .load_discovery_scoring_covered_through_cursor()?
            .context("covered_through cursor should remain present")?;
        assert!(
            super::compare_discovery_runtime_cursors(&covered_through, &tail_cursor)
                != std::cmp::Ordering::Less,
            "bounded hot-traffic repair must not regress covered_through"
        );
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_aggregate_gap_repair_under_hot_traffic_preserves_monotonic_covered_through_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-hot-gap-repair-monotonic",
        )?;
        let store = SqliteStore::open(Path::new(&db_path))?;
        let gap_swap = aggregate_gap_replay_test_swap(
            "sig-hot-gap-repair-monotonic-exact",
            840,
            "2026-04-28T13:20:00Z",
        );
        let tail_swap = aggregate_gap_replay_test_swap(
            "sig-hot-gap-repair-monotonic-tail",
            842,
            "2026-04-28T13:20:05Z",
        );
        store.insert_observed_swaps_batch(&[gap_swap.clone(), tail_swap.clone()])?;
        let gap_cursor = DiscoveryRuntimeCursor {
            ts_utc: gap_swap.ts_utc,
            slot: gap_swap.slot,
            signature: gap_swap.signature.clone(),
        };
        let tail_cursor = DiscoveryRuntimeCursor {
            ts_utc: tail_swap.ts_utc,
            slot: tail_swap.slot,
            signature: tail_swap.signature.clone(),
        };
        store.set_discovery_scoring_covered_through_cursor(&tail_cursor)?;
        let (aggregate_sender, aggregate_worker) = start_discovery_aggregate_writer_loop_for_test(
            &db_path,
            ObservedSwapWriterConfig::for_test_with_aggregate_tuning(
                64,
                1,
                true,
                aggregate_write_config(),
                1,
                64,
                true,
                1,
                None,
            ),
            128,
        )?;
        store.set_discovery_scoring_materialization_gap_cursor(&gap_cursor)?;

        let stop = Arc::new(AtomicBool::new(false));
        let sent_count = Arc::new(AtomicUsize::new(0));
        let traffic = start_hot_aggregate_repair_traffic(
            aggregate_sender.clone(),
            Arc::clone(&stop),
            Arc::clone(&sent_count),
        );
        wait_for_hot_aggregate_repair_traffic(&sent_count)?;
        wait_for_materialization_gap_clear(&store)?;
        stop.store(true, Ordering::Relaxed);
        drop(aggregate_sender);
        traffic
            .join()
            .expect("hot aggregate traffic thread panicked");
        aggregate_worker
            .join()
            .expect("aggregate writer test thread panicked")?;

        let covered_through = store
            .load_discovery_scoring_covered_through_cursor()?
            .context("covered_through cursor should remain present")?;
        assert!(
            super::compare_discovery_runtime_cursors(&covered_through, &tail_cursor)
                != std::cmp::Ordering::Less,
            "continuous hot requests plus bounded repair must keep covered_through monotonic"
        );
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_aggregate_gap_repair_under_hot_traffic_keeps_missing_exact_gap_latched_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-hot-gap-repair-missing",
        )?;
        let store = SqliteStore::open(Path::new(&db_path))?;
        let missing_gap = aggregate_gap_replay_test_swap(
            "sig-hot-gap-repair-missing",
            850,
            "2026-04-28T13:25:00Z",
        );
        let tail_swap = aggregate_gap_replay_test_swap(
            "sig-hot-gap-repair-missing-tail",
            851,
            "2026-04-28T13:25:05Z",
        );
        store.insert_observed_swaps_batch(&[tail_swap.clone()])?;
        let gap_cursor = DiscoveryRuntimeCursor {
            ts_utc: missing_gap.ts_utc,
            slot: missing_gap.slot,
            signature: missing_gap.signature.clone(),
        };
        let tail_cursor = DiscoveryRuntimeCursor {
            ts_utc: tail_swap.ts_utc,
            slot: tail_swap.slot,
            signature: tail_swap.signature.clone(),
        };
        store.set_discovery_scoring_covered_through_cursor(&tail_cursor)?;
        let (aggregate_sender, aggregate_worker) = start_discovery_aggregate_writer_loop_for_test(
            &db_path,
            ObservedSwapWriterConfig::for_test_with_aggregate_tuning(
                64,
                1,
                true,
                aggregate_write_config(),
                1,
                64,
                true,
                1,
                None,
            ),
            128,
        )?;
        store.set_discovery_scoring_materialization_gap_cursor(&gap_cursor)?;

        let stop = Arc::new(AtomicBool::new(false));
        let sent_count = Arc::new(AtomicUsize::new(0));
        let traffic = start_hot_aggregate_repair_traffic(
            aggregate_sender.clone(),
            Arc::clone(&stop),
            Arc::clone(&sent_count),
        );
        wait_for_hot_aggregate_repair_traffic(&sent_count)?;
        std::thread::sleep(StdDuration::from_millis(250));
        stop.store(true, Ordering::Relaxed);
        drop(aggregate_sender);
        traffic
            .join()
            .expect("hot aggregate traffic thread panicked");
        aggregate_worker
            .join()
            .expect("aggregate writer test thread panicked")?;

        assert!(
            sent_count.load(Ordering::Relaxed) > 0,
            "test must keep hot aggregate requests flowing while missing-gap repair runs"
        );
        assert_eq!(
            store.load_discovery_scoring_materialization_gap_cursor()?,
            Some(gap_cursor),
            "missing exact latched gap row must remain fail-closed under hot traffic"
        );
        let covered_through = store
            .load_discovery_scoring_covered_through_cursor()?
            .context("covered_through cursor should remain present")?;
        assert!(
            super::compare_discovery_runtime_cursors(&covered_through, &tail_cursor)
                != std::cmp::Ordering::Less,
            "missing-gap repair under hot traffic must still preserve monotonic covered_through"
        );
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_snapshot_clears_pending_after_fast_successful_enqueue() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-fast-snapshot-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let runtime = Builder::new_current_thread().enable_all().build()?;
        let writer = runtime.block_on(async {
            ObservedSwapWriter::start(
                db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                true,
                aggregate_write_config(),
            )
        })?;

        for idx in 0..8 {
            let swap = SwapEvent {
                wallet: "wallet-fast-snapshot".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: format!("token-fast-snapshot-{idx}"),
                amount_in: 1.0,
                amount_out: 10.0 + idx as f64,
                signature: format!("sig-observed-swap-fast-snapshot-{idx}"),
                slot: 300 + idx as u64,
                ts_utc: DateTime::parse_from_rfc3339("2026-03-14T14:00:00Z")
                    .expect("timestamp")
                    .with_timezone(&Utc),
                exact_amounts: None,
            };
            runtime.block_on(async { writer.enqueue(&swap).await })?;
        }

        let snapshot = wait_for_observed_swap_writer_snapshot(
            &writer,
            "fast aggregate batch drain",
            |snapshot| snapshot.pending_requests == 0 && snapshot.discovery_scoring_ms_p95 >= 1,
        )?;
        assert_eq!(
            snapshot.pending_requests, 0,
            "fast successful batches must not leave phantom pending writer requests"
        );
        assert!(
            snapshot.write_latency_ms_p95 < 250,
            "fast successful batches should not report lock-scale writer latency"
        );
        assert!(
            snapshot.raw_batch_write_ms_p95 >= 1,
            "fast successful batches should still report a non-zero raw batch phase latency sample"
        );
        assert!(
            snapshot.observed_swaps_insert_ms_p95 >= 1,
            "fast successful batches should separately report the observed_swaps insert phase"
        );
        assert!(
            snapshot.wallet_activity_days_ms_p95 >= 1,
            "fast successful batches should separately report the wallet_activity_days upsert phase"
        );
        assert!(
            snapshot.discovery_scoring_ms_p95 >= 1,
            "aggregate-enabled writer batches should report aggregate phase latency separately"
        );
        assert!(
            snapshot.worker_busy_ms_p95 >= snapshot.raw_batch_write_ms_p95,
            "full writer occupancy should be at least as large as the raw batch phase"
        );
        assert!(
            snapshot.worker_busy_ms_p95 >= snapshot.discovery_scoring_ms_p95,
            "full writer occupancy should also cover the discovery scoring phase when aggregates are enabled"
        );
        assert!(
            snapshot.aggregate_queue_depth_batches <= 1,
            "startup-decoupled aggregate batches should leave at most a bounded transient aggregate queue sample behind"
        );
        assert_eq!(
            snapshot.aggregate_queue_capacity_batches, 32,
            "production aggregate queue capacity should be bounded to the raw queue expressed in batches"
        );

        writer.shutdown()?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let swaps = verify_store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-14T13:59:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(swaps.len(), 8);
        let _ = std::fs::remove_file(db_path);

        Ok(())
    }

    #[test]
    fn observed_swap_writer_snapshot_keeps_discovery_scoring_latency_zero_when_aggregates_disabled(
    ) -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-no-aggregate-telemetry-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let runtime = Builder::new_current_thread().enable_all().build()?;
        let writer = runtime.block_on(async {
            ObservedSwapWriter::start_with_config(
                db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                ObservedSwapWriterConfig::for_test(16, 8, false, aggregate_write_config(), None),
            )
        })?;

        let swap = SwapEvent {
            wallet: "wallet-no-aggregate-telemetry".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-no-aggregate-telemetry".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-no-aggregate-telemetry".to_string(),
            slot: 330,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-15T09:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };

        runtime.block_on(async { writer.enqueue(&swap).await })?;
        std::thread::sleep(StdDuration::from_millis(50));

        let snapshot = writer.snapshot();
        assert!(
            snapshot.raw_batch_write_ms_p95 >= 1,
            "aggregate-disabled writer batches should still report raw batch phase latency"
        );
        assert!(
            snapshot.observed_swaps_insert_ms_p95 >= 1,
            "aggregate-disabled writer batches should still report the observed_swaps insert phase"
        );
        assert!(
            snapshot.wallet_activity_days_ms_p95 >= 1,
            "aggregate-disabled writer batches should still report the wallet_activity_days upsert phase"
        );
        assert_eq!(
            snapshot.discovery_scoring_ms_p95, 0,
            "aggregate-disabled writer batches must keep discovery scoring latency at zero"
        );
        assert!(
            snapshot.worker_busy_ms_p95 >= snapshot.raw_batch_write_ms_p95,
            "aggregate-disabled full writer occupancy should still cover the raw batch phase"
        );
        assert_eq!(
            snapshot.aggregate_queue_depth_batches, 0,
            "aggregate-disabled writer must report no aggregate queue backlog"
        );
        assert_eq!(
            snapshot.aggregate_queue_capacity_batches, 0,
            "aggregate-disabled writer must report zero aggregate queue capacity"
        );

        writer.shutdown()?;
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_closes_channel_after_async_batch_insert_failure() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-async-fail-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;
        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for async failure trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_wallet_activity_days_insert
             BEFORE INSERT ON wallet_activity_days
             BEGIN
                 SELECT RAISE(FAIL, 'forced async observed swap failure');
             END;",
        )?;

        let runtime = Builder::new_current_thread().enable_all().build()?;
        let writer = runtime.block_on(async {
            ObservedSwapWriter::start_with_config(
                db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
            )
        })?;

        let failing_swap = SwapEvent {
            wallet: "wallet-async-fail".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-async-fail".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-observed-swap-async-fail".to_string(),
            slot: 200,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-14T13:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };
        let subsequent_swap = SwapEvent {
            signature: "sig-observed-swap-after-fail".to_string(),
            slot: 201,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-14T13:01:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            ..failing_swap.clone()
        };

        runtime.block_on(async { writer.enqueue(&failing_swap).await })?;
        std::thread::sleep(StdDuration::from_millis(50));

        let error = runtime
            .block_on(async { writer.enqueue(&subsequent_swap).await })
            .expect_err("writer channel should close after async raw batch insert failure");
        let error_chain = format!("{error:#}");
        assert!(
            error_chain.contains(super::OBSERVED_SWAP_WRITER_CHANNEL_CLOSED_CONTEXT)
                || error_chain.contains(super::OBSERVED_SWAP_WRITER_TERMINAL_FAILURE_CONTEXT),
            "unexpected enqueue-after-failure error: {error_chain}"
        );

        let shutdown_error = writer
            .shutdown()
            .expect_err("shutdown should surface async raw batch insert failure");
        let shutdown_chain = format!("{shutdown_error:#}");
        assert!(
            shutdown_chain.contains("forced async observed swap failure"),
            "unexpected shutdown error: {shutdown_chain}"
        );
        drop(trigger_conn);

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let swaps = verify_store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-14T12:59:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert!(
            swaps.is_empty(),
            "failed async batch insert must not leave partially persisted observed swaps"
        );
        let _ = std::fs::remove_file(db_path);

        Ok(())
    }

    #[test]
    fn observed_swap_writer_reports_terminal_failure_after_async_batch_insert_failure() -> Result<()>
    {
        let unique = format!(
            "copybot-app-observed-swap-async-health-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;
        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for async failure trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_wallet_activity_days_insert
             BEFORE INSERT ON wallet_activity_days
             BEGIN
                 SELECT RAISE(FAIL, 'forced async observed swap failure');
             END;",
        )?;

        let runtime = Builder::new_current_thread().enable_all().build()?;
        let writer = runtime.block_on(async {
            ObservedSwapWriter::start_with_config(
                db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
            )
        })?;

        let failing_swap = SwapEvent {
            wallet: "wallet-async-health".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-async-health".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-observed-swap-async-health".to_string(),
            slot: 210,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-14T13:10:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };

        runtime.block_on(async { writer.enqueue(&failing_swap).await })?;

        let error_chain = wait_for_writer_terminal_failure(&writer)?;
        assert!(
            error_chain.contains(super::OBSERVED_SWAP_WRITER_TERMINAL_FAILURE_CONTEXT),
            "unexpected terminal failure health-check error: {error_chain}"
        );
        assert!(
            error_chain.contains("forced async observed swap failure"),
            "unexpected terminal failure health-check chain: {error_chain}"
        );

        let shutdown_error = writer
            .shutdown()
            .expect_err("shutdown should surface async raw batch insert failure");
        let shutdown_chain = format!("{shutdown_error:#}");
        assert!(
            shutdown_chain.contains("forced async observed swap failure"),
            "unexpected shutdown error: {shutdown_chain}"
        );
        drop(trigger_conn);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_keeps_raw_path_live_when_discovery_scoring_failure_is_nonfatal(
    ) -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-aggregate-nonfatal-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;
        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for aggregate nonfatal trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_wallet_scoring_days_insert
             BEFORE INSERT ON wallet_scoring_days
             BEGIN
                 SELECT RAISE(FAIL, 'database is locked');
             END;",
        )?;

        let runtime = Builder::new_current_thread().enable_all().build()?;
        runtime.block_on(async {
            let writer = ObservedSwapWriter::start_with_config(
                db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
            )?;

            let first_swap = SwapEvent {
                wallet: "wallet-aggregate-nonfatal".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-aggregate-nonfatal-a".to_string(),
                amount_in: 1.0,
                amount_out: 10.0,
                signature: "sig-observed-swap-aggregate-nonfatal-a".to_string(),
                slot: 223,
                ts_utc: DateTime::parse_from_rfc3339("2026-03-14T13:23:00Z")
                    .expect("timestamp")
                    .with_timezone(&Utc),
                exact_amounts: None,
            };
            let second_swap = SwapEvent {
                wallet: "wallet-aggregate-nonfatal".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-aggregate-nonfatal-b".to_string(),
                amount_in: 2.0,
                amount_out: 20.0,
                signature: "sig-observed-swap-aggregate-nonfatal-b".to_string(),
                slot: 224,
                ts_utc: DateTime::parse_from_rfc3339("2026-03-14T13:24:00Z")
                    .expect("timestamp")
                    .with_timezone(&Utc),
                exact_amounts: None,
            };

            assert!(
                writer.write(&first_swap).await?,
                "first raw insert should succeed despite non-fatal aggregate failure"
            );
            sleep(Duration::from_millis(50)).await;
            writer
                .ensure_running()
                .context("non-fatal aggregate failure must not latch terminal writer failure")?;

            assert!(
                writer.write(&second_swap).await?,
                "second raw insert should still succeed after non-fatal aggregate failure"
            );
            sleep(Duration::from_millis(50)).await;
            writer.ensure_running().context(
                "subsequent non-fatal aggregate failures must leave the raw writer healthy",
            )?;

            writer.shutdown()?;
            Ok::<(), anyhow::Error>(())
        })?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let rows = verify_store.load_observed_swaps_since(
            DateTime::parse_from_rfc3339("2026-03-14T00:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(
            rows.iter()
                .filter(|swap| {
                    swap.signature == "sig-observed-swap-aggregate-nonfatal-a"
                        || swap.signature == "sig-observed-swap-aggregate-nonfatal-b"
                })
                .count(),
            2,
            "non-fatal aggregate failures must not block raw observed-swap persistence"
        );

        drop(trigger_conn);
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_discovery_scoring_rug_finalize_sqlite_lock_is_retryable_stage1(
    ) -> Result<()> {
        let db_path =
            migrated_observed_swap_writer_test_db("copybot-app-observed-swap-rug-lock-retryable")?;
        let (covered_swap, first_replay_swap, _tail_replay_swap) =
            seed_discovery_scoring_rug_finalize_replay_fixture(&db_path)?;
        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for rug finalize retryable trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_rug_finalize_update_retryable
             BEFORE UPDATE OF rug_volume_lookahead_sol ON wallet_scoring_buy_facts
             WHEN OLD.buy_signature = 'sig-rug-finalize-replay-first'
             BEGIN
                 SELECT RAISE(FAIL, 'database is locked');
             END;",
        )?;

        let writer = ObservedSwapWriter::start_with_config(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?
                .to_string(),
            ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
        )?;

        let gap_cursor = wait_for_discovery_scoring_materialization_gap_cursor(&db_path)?;
        assert_eq!(
            gap_cursor,
            DiscoveryRuntimeCursor {
                ts_utc: first_replay_swap.ts_utc,
                slot: first_replay_swap.slot,
                signature: first_replay_swap.signature.clone(),
            },
            "retryable rug-finalize lock must latch replay gap instead of advancing coverage"
        );
        std::thread::sleep(StdDuration::from_millis(50));
        writer
            .ensure_running()
            .context("retryable rug-finalize sqlite lock must not become terminal")?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        assert_eq!(
            verify_store.load_discovery_scoring_covered_through_cursor()?,
            Some(DiscoveryRuntimeCursor {
                ts_utc: covered_swap.ts_utc,
                slot: covered_swap.slot,
                signature: covered_swap.signature.clone(),
            }),
            "coverage must not advance as if retryable rug finalization completed"
        );

        writer.shutdown()?;
        drop(trigger_conn);
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_discovery_scoring_replay_apply_sqlite_lock_is_retryable_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-replay-apply-lock-retryable",
        )?;
        let (covered_swap, first_replay_swap, _tail_replay_swap) =
            seed_discovery_scoring_rug_finalize_replay_fixture(&db_path)?;
        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for replay apply retryable trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_replay_apply_buy_fact_retryable
             BEFORE INSERT ON wallet_scoring_buy_facts
             WHEN NEW.buy_signature = 'sig-rug-finalize-replay-first'
             BEGIN
                 SELECT RAISE(FAIL, 'database is locked');
             END;",
        )?;

        let writer = ObservedSwapWriter::start_with_config(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?
                .to_string(),
            ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
        )?;

        let gap_cursor = wait_for_discovery_scoring_materialization_gap_cursor(&db_path)?;
        assert_eq!(
            gap_cursor,
            DiscoveryRuntimeCursor {
                ts_utc: first_replay_swap.ts_utc,
                slot: first_replay_swap.slot,
                signature: first_replay_swap.signature.clone(),
            },
            "retryable replay apply lock must latch the first failed replay row"
        );
        std::thread::sleep(StdDuration::from_millis(50));
        writer
            .ensure_running()
            .context("retryable replay apply sqlite lock must not become terminal")?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        assert_eq!(
            verify_store.load_discovery_scoring_covered_through_cursor()?,
            Some(DiscoveryRuntimeCursor {
                ts_utc: covered_swap.ts_utc,
                slot: covered_swap.slot,
                signature: covered_swap.signature.clone(),
            }),
            "coverage must not advance as if retryable replay apply completed"
        );

        writer.shutdown()?;
        drop(trigger_conn);
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_discovery_scoring_replay_apply_unknown_error_remains_terminal_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-replay-apply-unknown-fatal",
        )?;
        let (covered_swap, first_replay_swap, _tail_replay_swap) =
            seed_discovery_scoring_rug_finalize_replay_fixture(&db_path)?;
        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for replay apply terminal trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_replay_apply_buy_fact_terminal
             BEFORE INSERT ON wallet_scoring_buy_facts
             WHEN NEW.buy_signature = 'sig-rug-finalize-replay-first'
             BEGIN
                 SELECT RAISE(FAIL, 'forced replay apply failure');
             END;",
        )?;

        let writer = ObservedSwapWriter::start_with_config(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?
                .to_string(),
            ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
        )?;

        let error_chain = wait_for_writer_terminal_failure(&writer)?;
        assert!(
            error_chain.contains(
                "failed replaying discovery scoring rows during aggregate-writer startup catch-up"
            ),
            "unknown replay apply errors must keep explicit terminal context: {error_chain}"
        );
        assert!(
            error_chain.contains("forced replay apply failure"),
            "unknown replay apply error detail must remain visible: {error_chain}"
        );

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        assert_eq!(
            verify_store.load_discovery_scoring_materialization_gap_cursor()?,
            Some(DiscoveryRuntimeCursor {
                ts_utc: first_replay_swap.ts_utc,
                slot: first_replay_swap.slot,
                signature: first_replay_swap.signature.clone(),
            }),
            "terminal replay apply failure should keep explicit gap evidence"
        );
        assert_eq!(
            verify_store.load_discovery_scoring_covered_through_cursor()?,
            Some(DiscoveryRuntimeCursor {
                ts_utc: covered_swap.ts_utc,
                slot: covered_swap.slot,
                signature: covered_swap.signature.clone(),
            }),
            "terminal replay apply failure must not advance coverage"
        );

        let shutdown_error = writer
            .shutdown()
            .expect_err("shutdown should surface terminal replay apply failure");
        let shutdown_chain = format!("{shutdown_error:#}");
        assert!(
            shutdown_chain.contains(
                "failed replaying discovery scoring rows during aggregate-writer startup catch-up"
            ),
            "unexpected shutdown error: {shutdown_chain}"
        );
        drop(trigger_conn);
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_discovery_scoring_covered_through_update_sqlite_lock_is_retryable_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-covered-through-lock-retryable",
        )?;
        let (covered_swap, first_replay_swap, _tail_replay_swap) =
            seed_discovery_scoring_rug_finalize_replay_fixture(&db_path)?;
        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for covered-through retryable trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_covered_through_update_retryable
             BEFORE UPDATE ON discovery_scoring_state
             WHEN OLD.state_key = 'covered_through_ts'
             BEGIN
                 SELECT RAISE(FAIL, 'database is locked');
             END;",
        )?;

        let writer = ObservedSwapWriter::start_with_config(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?
                .to_string(),
            ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
        )?;

        let gap_cursor = wait_for_discovery_scoring_materialization_gap_cursor(&db_path)?;
        assert_eq!(
            gap_cursor,
            DiscoveryRuntimeCursor {
                ts_utc: first_replay_swap.ts_utc,
                slot: first_replay_swap.slot,
                signature: first_replay_swap.signature.clone(),
            },
            "retryable covered-through update lock must latch the first replay row"
        );
        std::thread::sleep(StdDuration::from_millis(50));
        writer
            .ensure_running()
            .context("retryable covered-through sqlite lock must not become terminal")?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        assert_eq!(
            verify_store.load_discovery_scoring_covered_through_cursor()?,
            Some(DiscoveryRuntimeCursor {
                ts_utc: covered_swap.ts_utc,
                slot: covered_swap.slot,
                signature: covered_swap.signature.clone(),
            }),
            "coverage must not advance when covered-through cursor update is retryably locked"
        );

        writer.shutdown()?;
        drop(trigger_conn);
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_discovery_scoring_covered_through_update_unknown_error_remains_terminal_stage1(
    ) -> Result<()> {
        let db_path = migrated_observed_swap_writer_test_db(
            "copybot-app-observed-swap-covered-through-unknown-fatal",
        )?;
        let (covered_swap, _first_replay_swap, _tail_replay_swap) =
            seed_discovery_scoring_rug_finalize_replay_fixture(&db_path)?;
        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for covered-through terminal trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_covered_through_update_terminal
             BEFORE UPDATE ON discovery_scoring_state
             WHEN OLD.state_key = 'covered_through_ts'
             BEGIN
                 SELECT RAISE(FAIL, 'forced covered_through update failure');
             END;",
        )?;

        let writer = ObservedSwapWriter::start_with_config(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?
                .to_string(),
            ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
        )?;

        let error_chain = wait_for_writer_terminal_failure(&writer)?;
        assert!(
            error_chain.contains("failed to run discovery scoring covered_through cursor update"),
            "unknown covered-through update errors must keep explicit terminal context: {error_chain}"
        );
        assert!(
            error_chain.contains("forced covered_through update failure"),
            "unknown covered-through update error detail must remain visible: {error_chain}"
        );

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        assert_eq!(
            verify_store.load_discovery_scoring_covered_through_cursor()?,
            Some(DiscoveryRuntimeCursor {
                ts_utc: covered_swap.ts_utc,
                slot: covered_swap.slot,
                signature: covered_swap.signature.clone(),
            }),
            "terminal covered-through update failure must not advance coverage"
        );

        let shutdown_error = writer
            .shutdown()
            .expect_err("shutdown should surface terminal covered-through update failure");
        let shutdown_chain = format!("{shutdown_error:#}");
        assert!(
            shutdown_chain
                .contains("failed to run discovery scoring covered_through cursor update"),
            "unexpected shutdown error: {shutdown_chain}"
        );
        drop(trigger_conn);
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_discovery_scoring_rug_finalize_unknown_error_remains_terminal_stage1(
    ) -> Result<()> {
        let db_path =
            migrated_observed_swap_writer_test_db("copybot-app-observed-swap-rug-unknown-fatal")?;
        let (covered_swap, _first_replay_swap, _tail_replay_swap) =
            seed_discovery_scoring_rug_finalize_replay_fixture(&db_path)?;
        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for rug finalize terminal trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_rug_finalize_update_terminal
             BEFORE UPDATE OF rug_volume_lookahead_sol ON wallet_scoring_buy_facts
             WHEN OLD.buy_signature = 'sig-rug-finalize-replay-first'
             BEGIN
                 SELECT RAISE(FAIL, 'forced rug finalize failure');
             END;",
        )?;

        let writer = ObservedSwapWriter::start_with_config(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?
                .to_string(),
            ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
        )?;

        let error_chain = wait_for_writer_terminal_failure(&writer)?;
        assert!(
            error_chain.contains("failed to run discovery scoring rug finalize"),
            "unknown rug-finalize errors must keep explicit terminal context: {error_chain}"
        );
        assert!(
            error_chain.contains("forced rug finalize failure"),
            "unknown rug-finalize error detail must remain visible: {error_chain}"
        );

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        assert_eq!(
            verify_store.load_discovery_scoring_covered_through_cursor()?,
            Some(DiscoveryRuntimeCursor {
                ts_utc: covered_swap.ts_utc,
                slot: covered_swap.slot,
                signature: covered_swap.signature.clone(),
            }),
            "terminal rug-finalize failure must not advance coverage"
        );

        let shutdown_error = writer
            .shutdown()
            .expect_err("shutdown should surface terminal rug-finalize failure");
        let shutdown_chain = format!("{shutdown_error:#}");
        assert!(
            shutdown_chain.contains("failed to run discovery scoring rug finalize"),
            "unexpected shutdown error: {shutdown_chain}"
        );
        drop(trigger_conn);
        remove_sqlite_test_files(&db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_reports_terminal_failure_after_fatal_discovery_scoring_materialization_failure(
    ) -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-aggregate-fatal-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;
        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for aggregate fatal trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_wallet_scoring_days_insert
             BEFORE INSERT ON wallet_scoring_days
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let runtime = Builder::new_current_thread().enable_all().build()?;
        let writer = runtime.block_on(async {
            ObservedSwapWriter::start_with_config(
                db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
            )
        })?;

        let failing_swap = SwapEvent {
            wallet: "wallet-aggregate-fatal".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-aggregate-fatal".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-observed-swap-aggregate-fatal".to_string(),
            slot: 220,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-14T13:20:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };

        runtime.block_on(async { writer.enqueue(&failing_swap).await })?;

        let error_chain = wait_for_writer_terminal_failure(&writer)?;
        assert!(
            error_chain.contains(super::OBSERVED_SWAP_WRITER_TERMINAL_FAILURE_CONTEXT),
            "unexpected terminal aggregate failure error: {error_chain}"
        );
        assert!(
            error_chain.contains("fatal discovery scoring materialization failure"),
            "missing aggregate fatal context: {error_chain}"
        );
        assert!(
            error_chain.contains("xShmMap"),
            "missing fatal sqlite marker: {error_chain}"
        );

        let shutdown_error = writer
            .shutdown()
            .expect_err("shutdown should surface fatal aggregate materialization failure");
        let shutdown_chain = format!("{shutdown_error:#}");
        assert!(
            shutdown_chain.contains("fatal discovery scoring materialization failure"),
            "unexpected shutdown error: {shutdown_chain}"
        );

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        assert_eq!(
            verify_store
                .load_discovery_scoring_materialization_gap_cursor()?
                .expect("fatal aggregate failure should still latch materialization gap"),
            DiscoveryRuntimeCursor {
                ts_utc: failing_swap.ts_utc,
                slot: failing_swap.slot,
                signature: failing_swap.signature.clone(),
            }
        );
        drop(trigger_conn);
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_reports_terminal_failure_after_fatal_discovery_scoring_gap_cursor_failure(
    ) -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-gap-fatal-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;
        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for gap fatal trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_wallet_scoring_days_insert
             BEFORE INSERT ON wallet_scoring_days
             BEGIN
                 SELECT RAISE(FAIL, 'forced discovery scoring failure');
             END;
             CREATE TRIGGER fail_discovery_scoring_state_insert
             BEFORE INSERT ON discovery_scoring_state
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let runtime = Builder::new_current_thread().enable_all().build()?;
        let writer = runtime.block_on(async {
            ObservedSwapWriter::start_with_config(
                db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
            )
        })?;

        let failing_swap = SwapEvent {
            wallet: "wallet-gap-fatal".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-gap-fatal".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-observed-swap-gap-fatal".to_string(),
            slot: 221,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-14T13:21:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };

        runtime.block_on(async { writer.enqueue(&failing_swap).await })?;

        let error_chain = wait_for_writer_terminal_failure(&writer)?;
        assert!(
            error_chain.contains(super::OBSERVED_SWAP_WRITER_TERMINAL_FAILURE_CONTEXT),
            "unexpected terminal gap-cursor failure error: {error_chain}"
        );
        assert!(
            error_chain.contains("fatal discovery scoring gap cursor failure"),
            "missing gap-cursor fatal context: {error_chain}"
        );
        assert!(
            error_chain.contains("xShmMap"),
            "missing fatal sqlite marker: {error_chain}"
        );

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        assert_eq!(
            verify_store.load_discovery_scoring_materialization_gap_cursor()?,
            None,
            "fatal gap cursor failure must leave the materialization gap cursor unset"
        );

        let shutdown_error = writer
            .shutdown()
            .expect_err("shutdown should surface fatal gap cursor failure");
        let shutdown_chain = format!("{shutdown_error:#}");
        assert!(
            shutdown_chain.contains("fatal discovery scoring gap cursor failure"),
            "unexpected shutdown error: {shutdown_chain}"
        );
        drop(trigger_conn);
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_reports_terminal_failure_after_fatal_discovery_scoring_coverage_watermark_failure(
    ) -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-covered-through-fatal-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;
        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for covered-through fatal trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_discovery_scoring_state_insert
             BEFORE INSERT ON discovery_scoring_state
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let runtime = Builder::new_current_thread().enable_all().build()?;
        let writer = runtime.block_on(async {
            ObservedSwapWriter::start_with_config(
                db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
            )
        })?;

        let failing_swap = SwapEvent {
            wallet: "wallet-covered-through-fatal".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-covered-through-fatal".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-observed-swap-covered-through-fatal".to_string(),
            slot: 222,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-14T13:22:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };

        runtime.block_on(async { writer.enqueue(&failing_swap).await })?;

        let error_chain = wait_for_writer_terminal_failure(&writer)?;
        assert!(
            error_chain.contains(super::OBSERVED_SWAP_WRITER_TERMINAL_FAILURE_CONTEXT),
            "unexpected terminal coverage watermark failure error: {error_chain}"
        );
        assert!(
            error_chain.contains("fatal discovery scoring coverage watermark failure"),
            "missing coverage watermark fatal context: {error_chain}"
        );
        assert!(
            error_chain.contains("xShmMap"),
            "missing fatal sqlite marker: {error_chain}"
        );

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        assert_eq!(
            verify_store.load_discovery_scoring_covered_through_cursor()?,
            None,
            "fatal coverage watermark failure must leave covered-through cursor unset"
        );

        let shutdown_error = writer
            .shutdown()
            .expect_err("shutdown should surface fatal coverage watermark failure");
        let shutdown_chain = format!("{shutdown_error:#}");
        assert!(
            shutdown_chain.contains("fatal discovery scoring coverage watermark failure"),
            "unexpected shutdown error: {shutdown_chain}"
        );
        drop(trigger_conn);
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_keeps_retention_out_of_inline_batch_path() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-retention-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let runtime = Builder::new_current_thread().enable_all().build()?;
        runtime.block_on(async {
            let writer = ObservedSwapWriter::start_with_config(
                db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
            )?;

            let stale_swap = SwapEvent {
                wallet: "wallet-old".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-old".to_string(),
                amount_in: 1.0,
                amount_out: 10.0,
                signature: "sig-observed-swap-old".to_string(),
                slot: 100,
                ts_utc: Utc::now() - ChronoDuration::days(3),
                exact_amounts: None,
            };
            let fresh_swap = SwapEvent {
                wallet: "wallet-new".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-new".to_string(),
                amount_in: 2.0,
                amount_out: 20.0,
                signature: "sig-observed-swap-new".to_string(),
                slot: 101,
                ts_utc: Utc::now(),
                exact_amounts: None,
            };

            writer.write(&stale_swap).await?;
            writer.write(&fresh_swap).await?;
            writer.shutdown()?;
            Ok::<(), anyhow::Error>(())
        })?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let swaps_before_maintenance =
            verify_store.load_observed_swaps_since(Utc::now() - ChronoDuration::days(7))?;
        assert_eq!(
            swaps_before_maintenance.len(),
            2,
            "writer should no longer prune stale rows inline while inserting fresh observed swaps"
        );

        let summary = super::run_observed_swap_retention_maintenance_once(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?,
            super::ObservedSwapRetentionConfig::production(1, 7, true),
            None,
        )?;
        assert_eq!(summary.raw_deleted_rows, 1);
        assert_eq!(summary.raw_delete_batches, 1);
        assert_eq!(summary.checkpoint.mode, "passive_runtime");

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let swaps_after_maintenance =
            verify_store.load_observed_swaps_since(Utc::now() - ChronoDuration::days(7))?;
        assert_eq!(swaps_after_maintenance.len(), 1);
        assert_eq!(
            swaps_after_maintenance[0].signature,
            "sig-observed-swap-new"
        );
        let _ = std::fs::remove_file(db_path);

        Ok(())
    }

    #[test]
    fn observed_swap_retention_maintenance_respects_backfill_source_protection() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-retention-protect-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let stale_swap = SwapEvent {
            wallet: "wallet-protected-old".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-protected-old".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-protected-old".to_string(),
            slot: 100,
            ts_utc: Utc::now() - ChronoDuration::days(3),
            exact_amounts: None,
        };
        seed_store.insert_observed_swaps_batch(&[stale_swap.clone()])?;
        seed_store.set_discovery_scoring_backfill_source_protection(
            Utc::now() - ChronoDuration::days(4),
            Utc::now() + ChronoDuration::hours(1),
        )?;

        let summary = super::run_observed_swap_retention_maintenance_once(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?,
            super::ObservedSwapRetentionConfig::production(1, 7, false),
            None,
        )?;
        assert_eq!(summary.raw_deleted_rows, 0);
        assert_eq!(summary.raw_delete_batches, 0);
        assert_eq!(summary.checkpoint.mode, "passive_runtime");

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let stale_rows = verify_store
            .load_observed_swaps_since(Utc::now() - ChronoDuration::days(7))?
            .into_iter()
            .filter(|swap| swap.signature == "sig-protected-old")
            .count();
        assert_eq!(
            stale_rows, 1,
            "source protection must defer raw retention pruning"
        );
        let _ = std::fs::remove_file(db_path);

        Ok(())
    }

    #[test]
    fn observed_swap_retention_maintenance_stops_after_raw_batch_budget_and_skips_checkpoint(
    ) -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-retention-bounded-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let stale_ts = Utc::now() - ChronoDuration::days(3);
        let stale_rows = super::OBSERVED_SWAP_RETENTION_DELETE_BATCH_SIZE
            * super::OBSERVED_SWAP_RETENTION_MAX_RAW_DELETE_BATCHES_PER_RUN
            + 1;
        for idx in 0..stale_rows {
            seed_store.insert_observed_swap(&SwapEvent {
                wallet: "wallet-bounded-retention".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: format!("token-bounded-{idx}"),
                amount_in: 1.0,
                amount_out: 10.0,
                signature: format!("sig-bounded-retention-{idx}"),
                slot: idx as u64 + 1,
                ts_utc: stale_ts + ChronoDuration::seconds(idx as i64),
                exact_amounts: None,
            })?;
        }

        let summary = super::run_observed_swap_retention_maintenance_once(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?,
            super::ObservedSwapRetentionConfig::production(1, 7, false),
            None,
        )?;
        assert_eq!(
            summary.raw_deleted_rows,
            super::OBSERVED_SWAP_RETENTION_DELETE_BATCH_SIZE
                * super::OBSERVED_SWAP_RETENTION_MAX_RAW_DELETE_BATCHES_PER_RUN
        );
        assert_eq!(
            summary.raw_delete_batches,
            super::OBSERVED_SWAP_RETENTION_MAX_RAW_DELETE_BATCHES_PER_RUN
        );
        assert_eq!(summary.scoring_deleted_rows, 0);
        assert!(!summary.completed_full_sweep);
        assert_eq!(summary.stop_reason, Some("raw_batch_budget"));
        assert_eq!(summary.checkpoint.mode, "skipped_bounded_run");

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let remaining = verify_store
            .load_observed_swaps_since(Utc::now() - ChronoDuration::days(7))?
            .len();
        assert_eq!(remaining, 1);

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_retention_should_stop_on_runtime_pressure() {
        let telemetry = Arc::new(ObservedSwapWriterTelemetry::default());
        telemetry.pending_requests.store(1, Ordering::Relaxed);
        let runtime_health = super::ObservedSwapRetentionRuntimeHealthHandle::new(
            super::ObservedSwapWriterHealthHandle { telemetry },
            Arc::new(Mutex::new(None)),
        );
        let mut last_sqlite_contention = SqliteContentionSnapshot::default();

        let stop_reason = super::observed_swap_retention_should_stop(
            Some(&runtime_health),
            &mut last_sqlite_contention,
            Instant::now(),
        );
        assert_eq!(stop_reason, Some("runtime_pressure"));
    }

    #[test]
    fn observed_swap_retention_checkpoint_error_requires_abort_on_xshmmap_io_failure() {
        let primary = anyhow!("database is locked");
        let fallback =
            anyhow!("disk I/O error: Error code 4874: I/O error within the xShmMap method");
        assert!(
            !super::observed_swap_retention_checkpoint_error_requires_abort(Some(&primary), None)
        );
        assert!(
            super::observed_swap_retention_checkpoint_error_requires_abort(
                Some(&primary),
                Some(&fallback)
            )
        );
    }

    #[test]
    fn observed_swap_retention_checkpoint_warn_failure_mode_is_distinct() {
        let summary = super::ObservedSwapRetentionCheckpointSummary {
            mode: "passive_runtime_failed",
            busy: 0,
            log_frames: 0,
            checkpointed_frames: 0,
        };
        assert_eq!(summary.mode, "passive_runtime_failed");
    }

    #[test]
    fn observed_swap_writer_discovery_scoring_error_requires_abort_on_xshmmap_io_failure() {
        let error = anyhow!("disk I/O error: Error code 4874: I/O error within the xShmMap method");
        assert!(super::observed_swap_writer_discovery_scoring_error_requires_abort(&error));
    }

    #[test]
    fn observed_swap_writer_discovery_scoring_error_does_not_require_abort_on_busy_lock() {
        let error = anyhow!("database is locked");
        assert!(!super::observed_swap_writer_discovery_scoring_error_requires_abort(&error));
    }

    #[test]
    fn observed_swap_writer_discovery_scoring_rug_finalize_retryable_classifier_matches_lock_only()
    {
        let replay_apply_retryable = anyhow!(
            "failed replaying discovery scoring rows during aggregate-writer startup catch-up: failed to run discovery scoring batch: failed to open discovery scoring batch transaction: database is locked"
        );
        assert!(
            super::observed_swap_writer_discovery_scoring_replay_apply_error_is_retryable(
                &replay_apply_retryable
            )
        );

        let replay_apply_fatal = anyhow!(
            "failed replaying discovery scoring rows during aggregate-writer startup catch-up: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(
            !super::observed_swap_writer_discovery_scoring_replay_apply_error_is_retryable(
                &replay_apply_fatal
            )
        );

        let replay_apply_unknown =
            anyhow!("failed replaying discovery scoring rows: forced replay apply failure");
        assert!(
            !super::observed_swap_writer_discovery_scoring_replay_apply_error_is_retryable(
                &replay_apply_unknown
            )
        );

        let covered_update_retryable = anyhow!(
            "failed to run discovery scoring covered_through cursor update: failed to open discovery scoring covered_through cursor update transaction: database is locked"
        );
        assert!(
            super::observed_swap_writer_discovery_scoring_covered_through_update_error_is_retryable(
                &covered_update_retryable
            )
        );

        let covered_update_fatal = anyhow!(
            "failed to run discovery scoring covered_through cursor update: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(
            !super::observed_swap_writer_discovery_scoring_covered_through_update_error_is_retryable(
                &covered_update_fatal
            )
        );

        let covered_update_unknown = anyhow!(
            "failed to run discovery scoring covered_through cursor update: malformed cursor"
        );
        assert!(
            !super::observed_swap_writer_discovery_scoring_covered_through_update_error_is_retryable(
                &covered_update_unknown
            )
        );

        let retryable = anyhow!(
            "failed to run discovery scoring rug finalize: failed to open discovery scoring rug finalize transaction: database is locked"
        );
        assert!(
            super::observed_swap_writer_discovery_scoring_rug_finalize_error_is_retryable(
                &retryable
            )
        );

        let fatal = anyhow!(
            "failed to run discovery scoring rug finalize: disk I/O error: Error code 4874: I/O error within the xShmMap method"
        );
        assert!(
            !super::observed_swap_writer_discovery_scoring_rug_finalize_error_is_retryable(&fatal)
        );

        let unknown = anyhow!("failed to run discovery scoring rug finalize: malformed rug fact");
        assert!(
            !super::observed_swap_writer_discovery_scoring_rug_finalize_error_is_retryable(
                &unknown
            )
        );
    }

    #[test]
    fn observed_swap_writer_aggregate_queue_capacity_tracks_raw_queue_in_batches() {
        assert_eq!(
            super::observed_swap_writer_aggregate_queue_capacity(
                &ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None,),
            ),
            2
        );
        assert_eq!(
            super::observed_swap_writer_aggregate_queue_capacity(
                &ObservedSwapWriterConfig::for_test(16, 8, false, aggregate_write_config(), None,),
            ),
            0
        );
    }

    #[test]
    fn observed_swap_writer_normal_try_enqueue_soft_limit_stays_one_batch_when_aggregates_disabled_stage1(
    ) {
        let config = ObservedSwapWriterConfig::for_test(
            super::OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY,
            super::OBSERVED_SWAP_BATCH_MAX_SIZE,
            false,
            aggregate_write_config(),
            None,
        );

        assert_eq!(
            super::observed_swap_writer_normal_try_enqueue_soft_limit(&config),
            super::OBSERVED_SWAP_BATCH_MAX_SIZE,
            "aggregate-disabled non-critical irrelevant swaps must keep the old one-batch try_enqueue budget"
        );
    }

    #[test]
    fn observed_swap_writer_normal_try_enqueue_soft_limit_uses_normal_capacity_when_aggregates_enabled_stage1(
    ) {
        let config = ObservedSwapWriterConfig::for_test(
            super::OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY,
            super::OBSERVED_SWAP_BATCH_MAX_SIZE,
            true,
            aggregate_write_config(),
            None,
        );
        let discovery_critical_reserve =
            super::observed_swap_writer_discovery_critical_reserve_requests(&config);
        let expected_normal_capacity =
            super::OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY.saturating_sub(discovery_critical_reserve);

        assert_eq!(
            super::observed_swap_writer_normal_try_enqueue_soft_limit(&config),
            expected_normal_capacity,
            "aggregate-enabled non-critical irrelevant swaps need the full normal writer budget so observed_swaps can continue feeding aggregate coverage"
        );
        assert!(
            expected_normal_capacity > super::OBSERVED_SWAP_BATCH_MAX_SIZE,
            "the aggregate-enabled budget must be above the old one-batch plateau"
        );
    }

    #[test]
    fn observed_swap_retention_effective_cutoff_requires_abort_on_fatal_protection_load_failure() {
        let now = Utc::now();
        let config = super::ObservedSwapRetentionConfig::production(1, 7, false);
        let error = super::resolve_observed_swap_retention_effective_cutoff(config, now, |_| {
            Err(anyhow!(
                "failed querying discovery_scoring_state.backfill_protect_since_ts: disk I/O error: Error code 4874: I/O error within the xShmMap method"
            ))
        })
        .expect_err("fatal protection load failure must not fall back to nominal cutoff");
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains("source protection lookup failed with fatal sqlite I/O"),
            "expected fatal protection lookup context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite I/O marker to survive error chain, got: {error_text}"
        );
    }

    #[test]
    fn observed_swap_retention_effective_cutoff_falls_back_on_busy_protection_load_failure() {
        let now = Utc::now();
        let config = super::ObservedSwapRetentionConfig::production(1, 7, false);
        let effective_cutoff =
            super::resolve_observed_swap_retention_effective_cutoff(config, now, |_| {
                Err(anyhow!("database is locked"))
            })
            .expect("busy protection load failure should keep nominal fallback behavior");
        assert_eq!(
            effective_cutoff,
            super::observed_swap_retention_nominal_cutoff(now, config)
        );
    }

    #[test]
    fn observed_swap_retention_maintenance_returns_error_on_fatal_raw_delete_failure() -> Result<()>
    {
        let unique = format!(
            "copybot-app-observed-swap-retention-fatal-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;
        let stale_swap = SwapEvent {
            wallet: "wallet-fatal-delete".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-fatal-delete".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-fatal-delete".to_string(),
            slot: 100,
            ts_utc: Utc::now() - ChronoDuration::days(3),
            exact_amounts: None,
        };
        seed_store.insert_observed_swaps_batch(&[stale_swap])?;

        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_observed_swap_retention_delete
             BEFORE DELETE ON observed_swaps
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let error = super::run_observed_swap_retention_maintenance_once(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?,
            super::ObservedSwapRetentionConfig::production(1, 7, false),
            None,
        )
        .expect_err("fatal raw delete failure must propagate out of retention maintenance");
        let error_text = format!("{error:#}");
        assert!(
            error_text.contains("failed to delete observed swap retention slice"),
            "expected retention delete failure context, got: {error_text}"
        );
        assert!(
            error_text.contains("xShmMap"),
            "expected fatal sqlite I/O marker to survive error chain, got: {error_text}"
        );

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_startup_replay_clears_observed_materialization_gap() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-gap-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;
        let covered_swap = SwapEvent {
            wallet: "wallet-gap".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-gap".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-gap-covered".to_string(),
            slot: 99,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T09:55:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };
        seed_store.insert_observed_swaps_batch(&[covered_swap.clone()])?;
        seed_store.apply_discovery_scoring_batch(
            std::slice::from_ref(&covered_swap),
            &aggregate_write_config(),
        )?;
        seed_store.set_discovery_scoring_covered_through_cursor(&DiscoveryRuntimeCursor {
            ts_utc: covered_swap.ts_utc,
            slot: covered_swap.slot,
            signature: covered_swap.signature.clone(),
        })?;
        let failed_swap = SwapEvent {
            wallet: "wallet-gap".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-gap".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-gap-failed".to_string(),
            slot: 100,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };
        seed_store.insert_observed_swaps_batch(&[failed_swap.clone()])?;
        seed_store.set_discovery_scoring_materialization_gap_cursor(&DiscoveryRuntimeCursor {
            ts_utc: failed_swap.ts_utc,
            slot: failed_swap.slot,
            signature: failed_swap.signature.clone(),
        })?;

        let runtime = Builder::new_current_thread().enable_all().build()?;
        runtime.block_on(async {
            let writer = ObservedSwapWriter::start_with_config(
                db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
            )?;

            let successful_swap = SwapEvent {
                wallet: "wallet-gap".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-gap".to_string(),
                amount_in: 1.0,
                amount_out: 10.0,
                signature: "sig-gap-success".to_string(),
                slot: 101,
                ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:05:00Z")
                    .expect("timestamp")
                    .with_timezone(&Utc),
                exact_amounts: None,
            };
            writer.write(&successful_swap).await?;
            wait_for_discovery_scoring_covered_through_at_least(&db_path, successful_swap.ts_utc)?;
            writer.shutdown()?;
            Ok::<(), anyhow::Error>(())
        })?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        assert_eq!(
            verify_store.load_discovery_scoring_materialization_gap_cursor()?,
            None,
            "startup replay must clear a latched continuity gap once it reprocesses the exact failed row"
        );
        assert_eq!(
            verify_store.load_discovery_scoring_covered_through()?,
            Some(
                DateTime::parse_from_rfc3339("2026-03-06T10:05:00Z")
                    .expect("timestamp")
                    .with_timezone(&Utc)
            )
        );
        let days = verify_store.load_wallet_scoring_days_since(
            DateTime::parse_from_rfc3339("2026-03-06T00:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(days.len(), 1);
        assert_eq!(
            days[0].trades, 3,
            "startup replay must materialize the previously failed row before live writes resume"
        );
        let _ = std::fs::remove_file(db_path);

        Ok(())
    }

    #[test]
    fn observed_swap_writer_reports_terminal_failure_after_fatal_startup_replay_gap_cursor_failure(
    ) -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-startup-gap-fatal-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let covered_swap = SwapEvent {
            wallet: "wallet-startup-gap-fatal".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-startup-gap-fatal".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-startup-gap-covered".to_string(),
            slot: 100,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-15T10:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };
        let tail_swap = SwapEvent {
            wallet: "wallet-startup-gap-fatal".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-startup-gap-fatal".to_string(),
            amount_in: 2.0,
            amount_out: 20.0,
            signature: "sig-startup-gap-tail".to_string(),
            slot: 101,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-15T10:05:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };
        seed_store.insert_observed_swaps_batch(&[covered_swap.clone(), tail_swap.clone()])?;
        seed_store
            .apply_discovery_scoring_batch(&[covered_swap.clone()], &aggregate_write_config())?;
        seed_store.set_discovery_scoring_covered_through_cursor(&DiscoveryRuntimeCursor {
            ts_utc: covered_swap.ts_utc,
            slot: covered_swap.slot,
            signature: covered_swap.signature.clone(),
        })?;

        let trigger_conn = Connection::open(Path::new(&db_path))
            .context("failed to open sqlite db for startup gap fatal trigger")?;
        trigger_conn.execute_batch(
            "CREATE TRIGGER fail_wallet_scoring_days_insert
             BEFORE INSERT ON wallet_scoring_days
             BEGIN
                 SELECT RAISE(FAIL, 'forced discovery scoring failure');
             END;
             CREATE TRIGGER fail_discovery_scoring_state_insert
             BEFORE INSERT ON discovery_scoring_state
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let writer = ObservedSwapWriter::start_with_config(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?
                .to_string(),
            ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
        )?;
        std::thread::sleep(StdDuration::from_millis(50));

        let error = writer.ensure_running().expect_err(
            "fatal startup replay gap cursor failure must latch before writer accepts live work",
        );
        let error_chain = format!("{error:#}");
        assert!(
            error_chain.contains(super::OBSERVED_SWAP_WRITER_TERMINAL_FAILURE_CONTEXT),
            "unexpected terminal startup replay failure error: {error_chain}"
        );
        assert!(
            error_chain.contains("fatal discovery scoring gap cursor failure"),
            "missing startup replay gap-cursor fatal context: {error_chain}"
        );
        assert!(
            error_chain.contains("xShmMap"),
            "missing fatal sqlite marker: {error_chain}"
        );
        assert!(
            !error_chain.contains("failed replaying discovery scoring rows during aggregate-writer startup catch-up"),
            "fatal gap cursor failure should not be masked by aggregate replay context: {error_chain}"
        );

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        assert_eq!(
            verify_store.load_discovery_scoring_materialization_gap_cursor()?,
            None,
            "fatal startup replay gap cursor failure must leave the materialization gap cursor unset"
        );

        let shutdown_error = writer
            .shutdown()
            .expect_err("shutdown should surface fatal startup replay gap cursor failure");
        let shutdown_chain = format!("{shutdown_error:#}");
        assert!(
            shutdown_chain.contains("fatal discovery scoring gap cursor failure"),
            "unexpected shutdown error: {shutdown_chain}"
        );

        drop(trigger_conn);
        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_writer_replays_tail_gap_before_accepting_live_writes() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-startup-replay-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let covered_swap = SwapEvent {
            wallet: "wallet-startup-replay".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-startup-replay".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-startup-covered".to_string(),
            slot: 100,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };
        let tail_swap = SwapEvent {
            wallet: "wallet-startup-replay".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-startup-replay".to_string(),
            amount_in: 2.0,
            amount_out: 20.0,
            signature: "sig-startup-tail".to_string(),
            slot: 101,
            ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:05:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
            exact_amounts: None,
        };
        seed_store.insert_observed_swaps_batch(&[covered_swap.clone(), tail_swap.clone()])?;
        seed_store
            .apply_discovery_scoring_batch(&[covered_swap.clone()], &aggregate_write_config())?;
        seed_store.set_discovery_scoring_covered_through_cursor(&DiscoveryRuntimeCursor {
            ts_utc: covered_swap.ts_utc,
            slot: covered_swap.slot,
            signature: covered_swap.signature.clone(),
        })?;

        let writer = ObservedSwapWriter::start_with_config(
            db_path
                .to_str()
                .context("sqlite path must be valid utf-8")?
                .to_string(),
            ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
        )?;
        writer.shutdown()?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let days = verify_store.load_wallet_scoring_days_since(
            DateTime::parse_from_rfc3339("2026-03-06T00:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(days.len(), 1);
        assert_eq!(
            days[0].trades, 2,
            "startup replay must materialize raw tail gap"
        );
        assert_eq!(
            verify_store.load_discovery_scoring_covered_through_cursor()?,
            Some(DiscoveryRuntimeCursor {
                ts_utc: tail_swap.ts_utc,
                slot: tail_swap.slot,
                signature: tail_swap.signature.clone(),
            })
        );
        let _ = std::fs::remove_file(db_path);

        Ok(())
    }

    #[test]
    fn observed_swap_writer_upserts_wallet_activity_days_for_inserted_swaps() -> Result<()> {
        let unique = format!(
            "copybot-app-observed-swap-activity-days-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");

        let mut seed_store = SqliteStore::open(Path::new(&db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let runtime = Builder::new_current_thread().enable_all().build()?;
        runtime.block_on(async {
            let writer = ObservedSwapWriter::start_with_config(
                db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                ObservedSwapWriterConfig::for_test(16, 8, true, aggregate_write_config(), None),
            )?;

            let swap_day_one = SwapEvent {
                wallet: "wallet-activity".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-activity".to_string(),
                amount_in: 1.0,
                amount_out: 10.0,
                signature: "sig-observed-swap-day-1".to_string(),
                slot: 100,
                ts_utc: DateTime::parse_from_rfc3339("2026-03-06T10:00:00Z")
                    .expect("timestamp")
                    .with_timezone(&Utc),
                exact_amounts: None,
            };
            let swap_day_two = SwapEvent {
                wallet: "wallet-activity".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-activity".to_string(),
                amount_in: 2.0,
                amount_out: 20.0,
                signature: "sig-observed-swap-day-2".to_string(),
                slot: 101,
                ts_utc: DateTime::parse_from_rfc3339("2026-03-07T11:00:00Z")
                    .expect("timestamp")
                    .with_timezone(&Utc),
                exact_amounts: None,
            };

            writer.write(&swap_day_one).await?;
            writer.write(&swap_day_two).await?;
            wait_for_discovery_scoring_covered_through_at_least(&db_path, swap_day_two.ts_utc)?;
            writer.shutdown()?;
            Ok::<(), anyhow::Error>(())
        })?;

        let verify_store = SqliteStore::open(Path::new(&db_path))?;
        let counts = verify_store.wallet_active_day_counts_since(
            &["wallet-activity".to_string()],
            DateTime::parse_from_rfc3339("2026-03-06T00:00:00Z")
                .expect("timestamp")
                .with_timezone(&Utc),
        )?;
        assert_eq!(counts.get("wallet-activity"), Some(&2));
        let covered_through = verify_store.load_discovery_scoring_covered_through()?;
        assert_eq!(
            covered_through,
            Some(
                DateTime::parse_from_rfc3339("2026-03-07T11:00:00Z")
                    .expect("timestamp")
                    .with_timezone(&Utc)
            )
        );
        let _ = std::fs::remove_file(db_path);

        Ok(())
    }

    #[test]
    fn recent_raw_journal_writer_persists_inserted_observed_swaps_and_reports_telemetry(
    ) -> Result<()> {
        let unique = format!(
            "copybot-app-recent-raw-journal-writer-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let runtime_db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let journal_db_path = std::env::temp_dir().join(format!("{unique}-recent-raw.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut seed_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let runtime = Builder::new_current_thread().enable_all().build()?;
        let recent_ts = Utc::now() - ChronoDuration::days(1);
        runtime.block_on(async {
            let writer = ObservedSwapWriter::start_with_config(
                runtime_db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                ObservedSwapWriterConfig::for_test(
                    16,
                    8,
                    false,
                    aggregate_write_config(),
                    Some(ObservedSwapRecentRawJournalConfig {
                        sqlite_path: journal_db_path
                            .to_str()
                            .context("journal sqlite path must be valid utf-8")?
                            .to_string(),
                        retention_days: 9,
                        writer_queue_capacity_batches: 8,
                        write_coalesce_max_batches:
                            super::OBSERVED_SWAP_RECENT_RAW_JOURNAL_WRITE_COALESCE_MAX_BATCHES,
                        overflow_capacity_batches: 32,
                        skip_prune_while_backlogged: true,
                        skip_startup_prune: false,
                    }),
                ),
            )?;
            let swap = SwapEvent {
                wallet: "wallet-journal".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-journal".to_string(),
                amount_in: 1.0,
                amount_out: 10.0,
                signature: "sig-recent-raw-journal".to_string(),
                slot: 500,
                ts_utc: recent_ts,
                exact_amounts: None,
            };

            writer.write(&swap).await?;
            std::thread::sleep(StdDuration::from_millis(50));
            let snapshot = writer.snapshot();
            assert_eq!(snapshot.journal_queue_capacity_batches, 8);
            assert_eq!(snapshot.journal_queue_depth_batches, 0);
            writer.shutdown()?;
            Ok::<(), anyhow::Error>(())
        })?;

        let journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
        let journal_rows =
            journal_store.load_observed_swaps_since(recent_ts - ChronoDuration::hours(1))?;
        assert_eq!(journal_rows.len(), 1);
        assert_eq!(journal_rows[0].signature, "sig-recent-raw-journal");
        let journal_state = journal_store.recent_raw_journal_state()?;
        assert_eq!(journal_state.row_count, 1);
        assert_eq!(journal_state.last_batch_rows, 1);
        let _ = std::fs::remove_file(runtime_db_path);
        let _ = std::fs::remove_file(journal_db_path);
        Ok(())
    }

    #[test]
    fn recent_raw_journal_startup_prune_is_deferred_until_after_live_write_stage1() -> Result<()> {
        let unique = format!(
            "copybot-app-recent-raw-journal-deferred-startup-prune-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let journal_db_path = std::env::temp_dir().join(format!("{unique}-recent-raw.db"));
        let journal_now = Utc::now();
        let stale_swap = SwapEvent {
            wallet: "wallet-journal-startup-prune-stale".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-journal-startup-prune-stale".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-recent-raw-journal-startup-prune-stale".to_string(),
            slot: 450,
            ts_utc: journal_now - ChronoDuration::days(10),
            exact_amounts: None,
        };
        let journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
        journal_store.insert_recent_raw_journal_batch(
            std::slice::from_ref(&stale_swap),
            stale_swap.ts_utc,
        )?;
        journal_store.checkpoint_wal_truncate()?;
        drop(journal_store);

        let (journal_sender, journal_receiver) =
            std_mpsc::sync_channel::<super::RecentRawJournalWriteRequest>(8);
        let (startup_sender, startup_receiver) =
            std_mpsc::channel::<std::result::Result<(), String>>();
        let telemetry = Arc::new(ObservedSwapWriterTelemetry::default());
        let config = ObservedSwapRecentRawJournalConfig {
            sqlite_path: journal_db_path
                .to_str()
                .context("journal sqlite path must be valid utf-8")?
                .to_string(),
            retention_days: 8,
            writer_queue_capacity_batches: 8,
            write_coalesce_max_batches:
                super::OBSERVED_SWAP_RECENT_RAW_JOURNAL_WRITE_COALESCE_MAX_BATCHES,
            overflow_capacity_batches: 32,
            skip_prune_while_backlogged: true,
            skip_startup_prune: false,
        };
        let writer_handle = thread::spawn(move || {
            super::recent_raw_journal_writer_loop(
                journal_receiver,
                startup_sender,
                config,
                telemetry,
            )
        });
        startup_receiver
            .recv_timeout(StdDuration::from_secs(2))
            .context("recent_raw journal writer did not signal startup before pruning")?
            .map_err(|error| anyhow!(error))?;

        let journal_store_after_startup = SqliteStore::open(Path::new(&journal_db_path))?;
        let rows_after_startup = journal_store_after_startup
            .load_observed_swaps_since(journal_now - ChronoDuration::days(30))?;
        assert_eq!(
            rows_after_startup.len(),
            1,
            "startup must only open/ensure journal tables and must not prune before the writer can receive live rows"
        );
        assert_eq!(
            rows_after_startup[0].signature,
            "sig-recent-raw-journal-startup-prune-stale"
        );
        drop(journal_store_after_startup);

        let fresh_swap = SwapEvent {
            wallet: "wallet-journal-startup-prune-fresh".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: "token-journal-startup-prune-fresh".to_string(),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: "sig-recent-raw-journal-startup-prune-fresh".to_string(),
            slot: 451,
            ts_utc: journal_now - ChronoDuration::seconds(30),
            exact_amounts: None,
        };
        journal_sender.send(super::RecentRawJournalWriteRequest {
            inserted_swaps: vec![fresh_swap],
        })?;
        drop(journal_sender);

        let live_write_started = Instant::now();
        loop {
            let verify_store = SqliteStore::open(Path::new(&journal_db_path))?;
            let rows =
                verify_store.load_observed_swaps_since(journal_now - ChronoDuration::days(30))?;
            if rows
                .iter()
                .any(|row| row.signature == "sig-recent-raw-journal-startup-prune-fresh")
            {
                break;
            }
            if live_write_started.elapsed() > StdDuration::from_secs(2) {
                anyhow::bail!(
                    "recent_raw journal writer did not persist first live row after startup signal"
                );
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }

        writer_handle
            .join()
            .map_err(|payload| anyhow!(super::panic_payload_to_string(payload.as_ref())))??;
        let journal_store_after_write = SqliteStore::open(Path::new(&journal_db_path))?;
        let rows_after_write = journal_store_after_write
            .load_observed_swaps_since(journal_now - ChronoDuration::days(30))?;
        assert_eq!(
            rows_after_write.len(),
            2,
            "hot recent_raw writer must defer retention prune when skip_prune_while_backlogged=true"
        );
        assert!(rows_after_write
            .iter()
            .any(|row| row.signature == "sig-recent-raw-journal-startup-prune-fresh"));
        let journal_state = journal_store_after_write.recent_raw_journal_state()?;
        assert_eq!(
            journal_state.row_count, 2,
            "journal state must reflect committed hot writer rows plus retained rows when prune is deferred"
        );
        assert!(
            journal_state.last_pruned_at.is_none(),
            "hot writer must not mark retention prune when prune is deferred"
        );

        remove_sqlite_test_files(&journal_db_path);
        Ok(())
    }

    #[test]
    fn recent_raw_journal_writer_phase_telemetry_orders_write_and_prune_stage1() -> Result<()> {
        super::clear_recent_raw_journal_phase_events_for_test();
        let unique = format!(
            "copybot-app-recent-raw-journal-phase-order-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let journal_db_path = std::env::temp_dir().join(format!("{unique}-recent-raw.db"));
        let (journal_sender, journal_receiver) =
            std_mpsc::sync_channel::<super::RecentRawJournalWriteRequest>(8);
        let (startup_sender, startup_receiver) =
            std_mpsc::channel::<std::result::Result<(), String>>();
        let telemetry = Arc::new(ObservedSwapWriterTelemetry::default());
        let config = ObservedSwapRecentRawJournalConfig {
            sqlite_path: journal_db_path
                .to_str()
                .context("journal sqlite path must be valid utf-8")?
                .to_string(),
            retention_days: 8,
            writer_queue_capacity_batches: 8,
            write_coalesce_max_batches: 4,
            overflow_capacity_batches: 8,
            skip_prune_while_backlogged: false,
            skip_startup_prune: true,
        };
        let writer_handle = thread::spawn(move || {
            super::recent_raw_journal_writer_loop(
                journal_receiver,
                startup_sender,
                config,
                telemetry,
            )
        });
        startup_receiver
            .recv_timeout(StdDuration::from_secs(2))
            .context("recent_raw journal writer did not signal startup")?
            .map_err(|error| anyhow!(error))?;

        let scenario_now = DateTime::parse_from_rfc3339("2026-04-29T18:50:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        journal_sender.send(recent_raw_journal_write_request_for_test(
            0,
            3,
            scenario_now,
        ))?;
        drop(journal_sender);
        writer_handle
            .join()
            .map_err(|payload| anyhow!(super::panic_payload_to_string(payload.as_ref())))??;

        let events = super::recent_raw_journal_phase_events_for_test();
        let expected = vec![
            super::RECENT_RAW_JOURNAL_PHASE_BATCH_COLLECTED,
            super::RECENT_RAW_JOURNAL_PHASE_WRITE_START,
            super::RECENT_RAW_JOURNAL_PHASE_WRITE_END,
            super::RECENT_RAW_JOURNAL_PHASE_PRUNE_CHECK_START,
            super::RECENT_RAW_JOURNAL_PHASE_PRUNE_CHECK_END,
            super::RECENT_RAW_JOURNAL_PHASE_PRUNE_START,
            super::RECENT_RAW_JOURNAL_PHASE_PRUNE_END,
            super::RECENT_RAW_JOURNAL_PHASE_BATCH_DONE,
        ];
        assert_eq!(
            events, expected,
            "recent_raw journal phase telemetry must expose exact write/prune order"
        );
        let journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
        let journal_state = journal_store.recent_raw_journal_state()?;
        assert_eq!(
            journal_state.row_count, 3,
            "phase telemetry must not replace commit-only journal state advancement"
        );
        remove_sqlite_test_files(&journal_db_path);
        Ok(())
    }

    #[test]
    fn recent_raw_journal_hot_writer_deferred_prune_emits_skipped_without_prune_start_stage1(
    ) -> Result<()> {
        super::clear_recent_raw_journal_phase_events_for_test();
        let unique = format!(
            "copybot-app-recent-raw-journal-phase-skip-prune-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let journal_db_path = std::env::temp_dir().join(format!("{unique}-recent-raw.db"));
        let (journal_sender, journal_receiver) =
            std_mpsc::sync_channel::<super::RecentRawJournalWriteRequest>(8);
        let (startup_sender, startup_receiver) =
            std_mpsc::channel::<std::result::Result<(), String>>();
        let telemetry = Arc::new(ObservedSwapWriterTelemetry::default());
        let config = ObservedSwapRecentRawJournalConfig {
            sqlite_path: journal_db_path
                .to_str()
                .context("journal sqlite path must be valid utf-8")?
                .to_string(),
            retention_days: 8,
            writer_queue_capacity_batches: 8,
            write_coalesce_max_batches: 4,
            overflow_capacity_batches: 8,
            skip_prune_while_backlogged: true,
            skip_startup_prune: true,
        };
        let writer_handle = thread::spawn(move || {
            super::recent_raw_journal_writer_loop(
                journal_receiver,
                startup_sender,
                config,
                telemetry,
            )
        });
        startup_receiver
            .recv_timeout(StdDuration::from_secs(2))
            .context("recent_raw journal writer did not signal startup")?
            .map_err(|error| anyhow!(error))?;

        let scenario_now = DateTime::parse_from_rfc3339("2026-04-29T18:55:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        journal_sender.send(recent_raw_journal_write_request_for_test(
            0,
            3,
            scenario_now,
        ))?;
        drop(journal_sender);
        writer_handle
            .join()
            .map_err(|payload| anyhow!(super::panic_payload_to_string(payload.as_ref())))??;

        let events = super::recent_raw_journal_phase_events_for_test();
        assert!(
            events.contains(&super::RECENT_RAW_JOURNAL_PHASE_PRUNE_SKIPPED),
            "hot writer should emit prune_skipped when prune is deferred: {events:?}"
        );
        assert!(
            !events.contains(&super::RECENT_RAW_JOURNAL_PHASE_PRUNE_START),
            "hot writer must not enter prune_start when skip_prune_while_backlogged=true: {events:?}"
        );
        let journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
        let journal_state = journal_store.recent_raw_journal_state()?;
        assert_eq!(journal_state.row_count, 3);
        assert!(journal_state.last_pruned_at.is_none());
        remove_sqlite_test_files(&journal_db_path);
        Ok(())
    }

    #[test]
    fn recent_raw_journal_write_deadline_exhaustion_fails_closed_without_unproven_state_stage1(
    ) -> Result<()> {
        let unique = format!(
            "copybot-app-recent-raw-journal-write-deadline-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let journal_db_path = std::env::temp_dir().join(format!("{unique}-recent-raw.db"));
        let store = SqliteStore::open(Path::new(&journal_db_path))?;
        store.ensure_recent_raw_journal_tables()?;
        let completed_at = Utc::now();
        let swaps = (0..4usize)
            .map(|idx| SwapEvent {
                wallet: format!("wallet-journal-deadline-{idx}"),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: format!("token-journal-deadline-{idx}"),
                amount_in: 1.0,
                amount_out: 10.0,
                signature: format!("sig-recent-raw-journal-deadline-{idx}"),
                slot: 1_000 + idx as u64,
                ts_utc: completed_at + ChronoDuration::milliseconds(idx as i64),
                exact_amounts: None,
            })
            .collect::<Vec<_>>();
        let telemetry = ObservedSwapWriterTelemetry::default();

        let error = super::write_recent_raw_journal_batch_with_deadline_attempts(
            &telemetry,
            &swaps,
            || Instant::now(),
            |_suffix, _deadline| Ok((recent_raw_journal_write_summary_for_test(0, 0), true)),
        )
        .expect_err("expired recent_raw journal write deadline must fail closed");
        let error_message = format!("{error:#}");
        assert!(
            error_message
                .contains(super::OBSERVED_SWAP_RECENT_RAW_JOURNAL_WRITE_DEADLINE_EXHAUSTED),
            "deadline failure must expose stable fail-closed reason; error={error_message}"
        );

        let rows = store.load_observed_swaps_since(completed_at - ChronoDuration::seconds(1))?;
        assert_eq!(
            rows.len(),
            0,
            "rows not proven committed before deadline exhaustion must not appear in the journal"
        );
        let journal_state = store.recent_raw_journal_state_cached()?;
        assert_eq!(journal_state.row_count, 0);
        assert_eq!(journal_state.last_batch_rows, 0);
        assert!(journal_state.covered_through_cursor.is_none());
        std::thread::sleep(StdDuration::from_millis(50));
        let delayed_rows =
            store.load_observed_swaps_since(completed_at - ChronoDuration::seconds(1))?;
        let delayed_state = store.recent_raw_journal_state_cached()?;
        assert_eq!(
            delayed_rows.len(),
            0,
            "deadline path must not leave any detached writer that can commit rows after fail-closed return"
        );
        assert_eq!(
            delayed_state.row_count, 0,
            "deadline path must not leave any detached writer that can advance journal state after fail-closed return"
        );

        remove_sqlite_test_files(&journal_db_path);
        Ok(())
    }

    #[test]
    fn recent_raw_journal_partial_deadline_exhaustion_retries_unprocessed_suffix_stage1(
    ) -> Result<()> {
        let telemetry = ObservedSwapWriterTelemetry::default();
        let scenario_now = Utc::now();
        let swaps = (0..5usize)
            .map(|idx| recent_raw_journal_backpressure_swap(idx, scenario_now))
            .collect::<Vec<_>>();
        let mut attempt_signatures = Vec::<Vec<String>>::new();
        let mut attempt_index = 0usize;

        super::write_recent_raw_journal_batch_with_deadline_attempts(
            &telemetry,
            &swaps,
            || Instant::now() + StdDuration::from_secs(5),
            |suffix, _deadline| {
                attempt_index = attempt_index.saturating_add(1);
                attempt_signatures.push(
                    suffix
                        .iter()
                        .map(|swap| swap.signature.clone())
                        .collect::<Vec<_>>(),
                );
                match attempt_index {
                    1 => Ok((recent_raw_journal_write_summary_for_test(2, 2), true)),
                    2 => Ok((recent_raw_journal_write_summary_for_test(3, 3), false)),
                    _ => Err(anyhow!("unexpected extra recent_raw journal write attempt")),
                }
            },
        )?;

        assert_eq!(attempt_signatures.len(), 2);
        assert_eq!(
            attempt_signatures[0],
            swaps
                .iter()
                .map(|swap| swap.signature.clone())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            attempt_signatures[1],
            swaps[2..]
                .iter()
                .map(|swap| swap.signature.clone())
                .collect::<Vec<_>>(),
            "deadline exhaustion after a committed prefix must retry only the unprocessed suffix"
        );
        Ok(())
    }

    #[test]
    fn recent_raw_journal_deadline_exhaustion_without_suffix_progress_blocks_later_batches_stage1(
    ) -> Result<()> {
        let telemetry = ObservedSwapWriterTelemetry::default();
        let scenario_now = Utc::now();
        let swaps = (0..5usize)
            .map(|idx| recent_raw_journal_backpressure_swap(idx, scenario_now))
            .collect::<Vec<_>>();
        let mut attempt_signatures = Vec::<Vec<String>>::new();
        let mut attempt_index = 0usize;

        let error = super::write_recent_raw_journal_batch_with_deadline_attempts(
            &telemetry,
            &swaps,
            || Instant::now() + StdDuration::from_secs(5),
            |suffix, _deadline| {
                attempt_index = attempt_index.saturating_add(1);
                attempt_signatures.push(
                    suffix
                        .iter()
                        .map(|swap| swap.signature.clone())
                        .collect::<Vec<_>>(),
                );
                match attempt_index {
                    1 => Ok((recent_raw_journal_write_summary_for_test(2, 2), true)),
                    2 => Ok((recent_raw_journal_write_summary_for_test(0, 0), true)),
                    _ => Err(anyhow!(
                        "writer must not process future journal batches while suffix ownership is unresolved"
                    )),
                }
            },
        )
        .expect_err("zero-progress suffix deadline exhaustion must fail closed");
        let error_message = format!("{error:#}");
        assert!(
            error_message
                .contains(super::OBSERVED_SWAP_RECENT_RAW_JOURNAL_WRITE_DEADLINE_EXHAUSTED),
            "zero-progress suffix failure must expose stable deadline reason; error={error_message}"
        );
        assert_eq!(
            attempt_signatures.len(),
            2,
            "writer must stop on unprocessed suffix instead of advancing to any later journal batch"
        );
        assert_eq!(
            attempt_signatures[1],
            swaps[2..]
                .iter()
                .map(|swap| swap.signature.clone())
                .collect::<Vec<_>>()
        );
        Ok(())
    }

    #[test]
    fn recent_raw_journal_hot_writer_defers_rows_older_than_retention_horizon() -> Result<()> {
        let unique = format!(
            "copybot-app-recent-raw-journal-prune-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let runtime_db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let journal_db_path = std::env::temp_dir().join(format!("{unique}-recent-raw.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut seed_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        seed_store.run_migrations(&migration_dir)?;

        let journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
        let journal_now = Utc::now();
        journal_store.insert_recent_raw_journal_batch(
            &[SwapEvent {
                wallet: "wallet-journal-old".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-journal-old".to_string(),
                amount_in: 1.0,
                amount_out: 10.0,
                signature: "sig-recent-raw-journal-old".to_string(),
                slot: 400,
                ts_utc: journal_now - ChronoDuration::days(10),
                exact_amounts: None,
            }],
            journal_now - ChronoDuration::days(10),
        )?;

        let runtime = Builder::new_current_thread().enable_all().build()?;
        runtime.block_on(async {
            let writer = ObservedSwapWriter::start_with_config(
                runtime_db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string(),
                ObservedSwapWriterConfig::for_test(
                    16,
                    8,
                    false,
                    aggregate_write_config(),
                    Some(ObservedSwapRecentRawJournalConfig {
                        sqlite_path: journal_db_path
                            .to_str()
                            .context("journal sqlite path must be valid utf-8")?
                            .to_string(),
                        retention_days: 8,
                        writer_queue_capacity_batches: 8,
                        write_coalesce_max_batches:
                            super::OBSERVED_SWAP_RECENT_RAW_JOURNAL_WRITE_COALESCE_MAX_BATCHES,
                        overflow_capacity_batches: 32,
                        skip_prune_while_backlogged: true,
                        skip_startup_prune: false,
                    }),
                ),
            )?;
            let fresh_swap = SwapEvent {
                wallet: "wallet-journal-fresh".to_string(),
                dex: "raydium".to_string(),
                token_in: "So11111111111111111111111111111111111111112".to_string(),
                token_out: "token-journal-fresh".to_string(),
                amount_in: 1.0,
                amount_out: 10.0,
                signature: "sig-recent-raw-journal-fresh".to_string(),
                slot: 401,
                ts_utc: journal_now - ChronoDuration::days(1),
                exact_amounts: None,
            };

            writer.write(&fresh_swap).await?;
            writer.shutdown()?;
            Ok::<(), anyhow::Error>(())
        })?;

        let journal_rows =
            journal_store.load_observed_swaps_since(journal_now - ChronoDuration::days(30))?;
        assert_eq!(
            journal_rows.len(),
            2,
            "hot recent_raw writer must not run retention prune when skip_prune_while_backlogged=true"
        );
        assert!(journal_rows
            .iter()
            .any(|row| row.signature == "sig-recent-raw-journal-fresh"));
        let journal_state = journal_store.recent_raw_journal_state()?;
        assert_eq!(journal_state.row_count, 2);
        assert!(journal_state.last_pruned_at.is_none());
        let _ = std::fs::remove_file(runtime_db_path);
        let _ = std::fs::remove_file(journal_db_path);
        Ok(())
    }

    #[test]
    fn recent_raw_journal_per_request_write_recreates_post_checkpoint_saturation_without_large_wal_stage1(
    ) -> Result<()> {
        let summary = run_recent_raw_journal_backpressure_scenario(true, 1, 0)?;
        assert!(
            summary.baseline_rows_persisted >= 32,
            "clean checkpoint baseline should permit immediate raw persistence before downstream journal pressure accumulates: {summary:?}"
        );
        assert!(
            summary.max_pending_requests >= 4,
            "current per-request recent_raw journal writes should still recreate observable upstream pending request pressure on the same clean-start workload, even if raw persistence is no longer startup-gated: {summary:?}"
        );
        assert!(
            summary.pending_requests_after_load > 0,
            "the reduced incident class should still leave the raw writer behind the ingestion stream immediately after the modeled load, not just in a transient internal sample: {summary:?}"
        );
        assert!(
            summary.journal_queue_depth_after_load > 0 || summary.max_journal_queue_depth_batches > 0,
            "the repro must still exercise a real downstream recent_raw journal backlog during the modeled load, rather than only raw-path pressure: {summary:?}"
        );
        assert!(
            summary.max_journal_queue_depth_batches > 0,
            "the repro must still exercise downstream recent_raw journal backlog rather than a raw-path-only slowdown: {summary:?}"
        );
        assert!(
            summary.sqlite_write_retry_delta <= 16,
            "this recurrence class should not require material retryable sqlite lock growth to re-saturate; it is enough that the downstream recent_raw journal hot path falls behind: {summary:?}"
        );
        assert!(
            summary.sqlite_busy_error_delta <= 16,
            "the repro should demonstrate queue saturation without relying on material busy-error churn: {summary:?}"
        );
        assert_eq!(
            summary.journal_overflow_depth_after_load, 0,
            "current per-request path should not have any separate overflow safety valve; the raw writer is coupled directly to the bounded journal queue today: {summary:?}"
        );
        assert_eq!(
            summary.max_journal_overflow_depth_batches, 0,
            "the current path should reproduce the incident without any extra hidden backlog layer: {summary:?}"
        );
        assert!(
            summary.runtime_wal_bytes_after_load < 64 * 1024 * 1024,
            "the reduced incident class should still saturate while runtime WAL stays modest rather than exploding into multi-gigabyte debt again: {summary:?}"
        );
        Ok(())
    }

    #[test]
    fn recent_raw_journal_coalesced_writes_reduce_post_checkpoint_saturation_without_hiding_backlog_stage1(
    ) -> Result<()> {
        let old = run_recent_raw_journal_backpressure_scenario(true, 1, 0)?;
        let new = run_recent_raw_journal_backpressure_scenario(
            true,
            super::OBSERVED_SWAP_RECENT_RAW_JOURNAL_WRITE_COALESCE_MAX_BATCHES,
            64,
        )?;

        assert!(
            old.max_pending_requests >= 4,
            "old per-request journal path must still recreate observable raw pending pressure for the A/B proof to be meaningful, even if the exact peak is scheduler-sensitive: old={old:?}"
        );
        assert!(
            new.persisted_rows_after_load >= old.persisted_rows_after_load,
            "the fix should not reduce pending depth by simply persisting fewer raw rows under the same modeled workload: old={old:?} new={new:?}"
        );
        assert!(
            new.journal_queue_depth_after_load < 16,
            "the fix may leave a scheduler-sensitive visible journal batch at the end of the modeled load, but it must stay bounded rather than hiding saturation downstream: old={old:?} new={new:?}"
        );
        assert!(
            new.max_journal_queue_depth_batches <= 16,
            "the fix may let the downstream journal queue absorb more visible work temporarily, but it must keep that backlog bounded at the modeled channel capacity without phantom queue growth: old={old:?} new={new:?}"
        );
        assert!(
            new.journal_overflow_depth_after_load <= 64,
            "the new overflow valve must remain bounded and visible at the end of the same modeled load instead of silently growing without limit: old={old:?} new={new:?}"
        );
        assert!(
            new.max_journal_overflow_depth_batches <= 64,
            "the new overflow valve must remain bounded and visible at peak load instead of silently growing without limit: old={old:?} new={new:?}"
        );
        assert!(
            new.runtime_wal_bytes_after_load <= old.runtime_wal_bytes_after_load * 2 + 1,
            "the fix should not merely trade queue pressure for runaway runtime WAL growth: old={old:?} new={new:?}"
        );
        assert!(
            new.sqlite_write_retry_delta <= 16,
            "the new path should improve throughput without hiding material lock contention behind retry growth: new={new:?}"
        );
        assert!(
            new.sqlite_busy_error_delta <= 16,
            "the new path should improve throughput without introducing material busy-error churn: new={new:?}"
        );
        Ok(())
    }

    #[test]
    fn recent_raw_journal_full_overflow_coalesces_without_blocking_raw_persistence_stage1(
    ) -> Result<()> {
        let unique = format!(
            "copybot-app-recent-raw-journal-full-overflow-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let runtime_db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let journal_db_path = std::env::temp_dir().join(format!("{unique}-recent-raw.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        runtime_store.run_migrations(&migration_dir)?;
        let mut journal_seed_store = SqliteStore::open(Path::new(&journal_db_path))?;
        journal_seed_store.run_migrations(&migration_dir)?;
        drop(journal_seed_store);
        let scenario_now = DateTime::parse_from_rfc3339("2026-04-28T20:05:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let runtime = Builder::new_current_thread().enable_all().build()?;
        let writer = ObservedSwapWriter::start_with_config(
            runtime_db_path
                .to_str()
                .context("runtime sqlite path must be valid utf-8")?
                .to_string(),
            ObservedSwapWriterConfig::for_test(
                32,
                1,
                false,
                aggregate_write_config(),
                Some(ObservedSwapRecentRawJournalConfig {
                    sqlite_path: journal_db_path
                        .to_str()
                        .context("journal sqlite path must be valid utf-8")?
                        .to_string(),
                    retention_days: 8,
                    writer_queue_capacity_batches: 1,
                    write_coalesce_max_batches: 32,
                    overflow_capacity_batches: 2,
                    skip_prune_while_backlogged: true,
                    skip_startup_prune: true,
                }),
            ),
        )?;

        runtime.block_on(async {
            writer
                .write(&recent_raw_journal_backpressure_swap(0, scenario_now))
                .await
        })?;
        let journal_baseline_started = Instant::now();
        loop {
            let journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
            let journal_rows = journal_store
                .load_observed_swaps_since(scenario_now - ChronoDuration::minutes(1))?
                .len();
            if journal_rows >= 1 {
                break;
            }
            if journal_baseline_started.elapsed() > StdDuration::from_secs(2) {
                anyhow::bail!(
                    "recent_raw journal writer did not persist baseline before pressure test"
                );
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }

        let blocker_conn = Connection::open(Path::new(&journal_db_path))
            .context("failed to open journal blocker connection")?;
        blocker_conn
            .busy_timeout(StdDuration::from_millis(1))
            .context("failed setting journal blocker busy timeout")?;
        blocker_conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let write_result = (1..42usize).try_for_each(|idx| -> Result<()> {
            let swap = recent_raw_journal_backpressure_swap(idx, scenario_now);
            match runtime.block_on(async {
                timeout(StdDuration::from_millis(250), writer.write(&swap)).await
            }) {
                Ok(Ok(inserted)) => {
                    assert!(inserted, "pressure test swap should insert once: {idx}");
                    Ok(())
                }
                Ok(Err(error)) => Err(error).with_context(|| {
                    format!("raw write failed while recent_raw journal was blocked at idx={idx}")
                }),
                Err(_) => anyhow::bail!(
                    "raw write timed out behind recent_raw journal backpressure at idx={idx}"
                ),
            }
        });

        let runtime_rows_while_blocked = runtime_store
            .load_observed_swaps_since(scenario_now - ChronoDuration::minutes(1))?
            .len();
        let backlog_visible_started = Instant::now();
        let mut snapshot_while_blocked = writer.snapshot();
        while snapshot_while_blocked.journal_queue_depth_batches == 0
            && snapshot_while_blocked.journal_overflow_depth_batches == 0
            && snapshot_while_blocked.journal_queue_row_debt == 0
            && snapshot_while_blocked.journal_overflow_row_debt == 0
            && snapshot_while_blocked.journal_writer_inflight_rows == 0
            && backlog_visible_started.elapsed() < StdDuration::from_secs(2)
        {
            std::thread::sleep(StdDuration::from_millis(10));
            snapshot_while_blocked = writer.snapshot();
        }
        let journal_store_while_blocked = SqliteStore::open(Path::new(&journal_db_path))?;
        let journal_rows_while_blocked = journal_store_while_blocked
            .load_observed_swaps_since(scenario_now - ChronoDuration::minutes(1))?
            .len();
        let journal_state_while_blocked = journal_store_while_blocked.recent_raw_journal_state()?;
        blocker_conn.execute_batch("COMMIT")?;
        drop(blocker_conn);

        write_result?;
        assert!(
            runtime_rows_while_blocked >= 42,
            "raw observed_swaps should keep advancing while recent_raw journal is blocked; runtime_rows={runtime_rows_while_blocked}"
        );
        assert!(
            snapshot_while_blocked.journal_queue_depth_batches > 0
                || snapshot_while_blocked.journal_overflow_depth_batches > 0
                || snapshot_while_blocked.journal_writer_inflight_rows > 0,
            "journal backlog should remain visible while downstream writer is blocked, whether it is still in overflow or has moved into queue/inflight: snapshot={snapshot_while_blocked:?}"
        );
        assert!(
            snapshot_while_blocked.journal_overflow_depth_batches
                <= snapshot_while_blocked.journal_overflow_capacity_batches,
            "journal overflow batch depth must stay bounded while adaptive coalescing moves backlog into writes: snapshot={snapshot_while_blocked:?}"
        );
        assert!(
            snapshot_while_blocked.journal_queue_row_debt
                + snapshot_while_blocked.journal_writer_inflight_rows
                + snapshot_while_blocked.journal_overflow_row_debt
                > 0,
            "snapshot should expose row debt that is still queued, overflowed, or inflight: snapshot={snapshot_while_blocked:?}"
        );
        assert!(
            snapshot_while_blocked.journal_overflow_row_debt
                <= snapshot_while_blocked.journal_overflow_row_debt_capacity,
            "journal overflow row debt must stay below the explicit cap: snapshot={snapshot_while_blocked:?}"
        );
        assert_eq!(
            snapshot_while_blocked.journal_overflow_row_debt_capacity, 64,
            "test fixture should prove the explicit row-debt cap derived from overflow batches * raw batch size * journal coalesce limit"
        );
        assert_eq!(
            journal_rows_while_blocked, 1,
            "journal rows must not be marked persisted before the blocked writer actually commits"
        );
        assert_eq!(
            journal_state_while_blocked.row_count, 1,
            "recent_raw journal coverage/state must not advance beyond actually written rows"
        );

        let journal_catchup_started = Instant::now();
        loop {
            let journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
            let journal_rows = journal_store
                .load_observed_swaps_since(scenario_now - ChronoDuration::minutes(1))?
                .len();
            if journal_rows >= runtime_rows_while_blocked {
                break;
            }
            if journal_catchup_started.elapsed() > StdDuration::from_secs(5) {
                anyhow::bail!(
                    "recent_raw journal did not catch up after blocker release; journal_rows={journal_rows} runtime_rows={runtime_rows_while_blocked}"
                );
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }

        writer.shutdown()?;
        let journal_store_after = SqliteStore::open(Path::new(&journal_db_path))?;
        let journal_rows_after = journal_store_after
            .load_observed_swaps_since(scenario_now - ChronoDuration::minutes(1))?
            .len();
        let runtime_rows_after = runtime_store
            .load_observed_swaps_since(scenario_now - ChronoDuration::minutes(1))?
            .len();
        assert_eq!(
            journal_rows_after, runtime_rows_after,
            "coalesced overflow rows must be written to recent_raw journal after pressure clears"
        );

        remove_sqlite_test_files(&runtime_db_path);
        remove_sqlite_test_files(&journal_db_path);
        Ok(())
    }

    #[test]
    fn recent_raw_journal_row_debt_moves_from_overflow_to_queue_and_inflight_stage1() -> Result<()>
    {
        let telemetry = ObservedSwapWriterTelemetry::default();
        telemetry
            .journal_overflow_row_debt_capacity
            .store(64, Ordering::Relaxed);
        let (journal_sender, journal_receiver) =
            std_mpsc::sync_channel::<super::RecentRawJournalWriteRequest>(2);
        let scenario_now = DateTime::parse_from_rfc3339("2026-04-28T20:10:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let mut journal_overflow = std::collections::VecDeque::new();
        let overflow_request = super::RecentRawJournalWriteRequest {
            inserted_swaps: (0..7usize)
                .map(|idx| recent_raw_journal_backpressure_swap(idx, scenario_now))
                .collect(),
        };
        let overflow_rows = overflow_request.row_count();
        journal_overflow.push_back(overflow_request);
        telemetry.note_journal_overflow_enqueued(overflow_rows);
        let overflow_snapshot = telemetry.snapshot();
        assert_eq!(overflow_snapshot.journal_overflow_depth_batches, 1);
        assert_eq!(overflow_snapshot.journal_overflow_row_debt, 7);
        assert_eq!(overflow_snapshot.journal_queue_row_debt, 0);
        assert_eq!(overflow_snapshot.journal_writer_inflight_rows, 0);

        super::drain_recent_raw_journal_overflow_nonblocking(
            &journal_sender,
            &mut journal_overflow,
            32,
            &telemetry,
        )?;
        let queued_snapshot = telemetry.snapshot();
        assert_eq!(queued_snapshot.journal_overflow_depth_batches, 0);
        assert_eq!(queued_snapshot.journal_overflow_row_debt, 0);
        assert_eq!(queued_snapshot.journal_queue_depth_batches, 1);
        assert_eq!(queued_snapshot.journal_queue_row_debt, 7);
        assert_eq!(queued_snapshot.journal_writer_inflight_rows, 0);

        let first_request = journal_receiver.recv()?;
        let collected_batch = super::collect_recent_raw_journal_write_batch(
            &journal_receiver,
            first_request,
            32,
            32,
            64,
            &telemetry,
        );
        let inserted_swaps = collected_batch.inserted_swaps;
        assert_eq!(inserted_swaps.len(), 7);
        let dequeued_snapshot = telemetry.snapshot();
        assert_eq!(dequeued_snapshot.journal_queue_depth_batches, 0);
        assert_eq!(dequeued_snapshot.journal_queue_row_debt, 0);
        assert_eq!(dequeued_snapshot.journal_writer_inflight_rows, 0);

        telemetry.note_journal_writer_inflight_started(inserted_swaps.len());
        let inflight_snapshot = telemetry.snapshot();
        assert_eq!(inflight_snapshot.journal_writer_inflight_rows, 7);
        telemetry.note_journal_writer_inflight_finished();
        assert_eq!(telemetry.snapshot().journal_writer_inflight_rows, 0);
        Ok(())
    }

    #[test]
    fn recent_raw_journal_overflow_drain_coalesces_fat_request_when_channel_has_capacity_stage1(
    ) -> Result<()> {
        let telemetry = ObservedSwapWriterTelemetry::default();
        telemetry
            .journal_overflow_row_debt_capacity
            .store(64, Ordering::Relaxed);
        let (journal_sender, journal_receiver) =
            std_mpsc::sync_channel::<super::RecentRawJournalWriteRequest>(4);
        let scenario_now = DateTime::parse_from_rfc3339("2026-04-28T20:20:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let mut journal_overflow = std::collections::VecDeque::new();
        for (start_idx, rows) in [(0usize, 2usize), (10, 3), (20, 4)] {
            let request = recent_raw_journal_write_request_for_test(start_idx, rows, scenario_now);
            telemetry.note_journal_overflow_enqueued(request.row_count());
            journal_overflow.push_back(request);
        }

        super::drain_recent_raw_journal_overflow_nonblocking(
            &journal_sender,
            &mut journal_overflow,
            8,
            &telemetry,
        )?;

        assert!(
            journal_overflow.is_empty(),
            "all overflow requests should be drained into one bounded fat channel request"
        );
        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.journal_overflow_depth_batches, 0);
        assert_eq!(snapshot.journal_overflow_row_debt, 0);
        assert_eq!(snapshot.journal_queue_depth_batches, 1);
        assert_eq!(snapshot.journal_queue_row_debt, 9);

        let request = journal_receiver.recv()?;
        assert_eq!(
            request.row_count(),
            9,
            "overflow drain should send one fat request instead of replaying tiny batches"
        );
        let signatures = request
            .inserted_swaps
            .iter()
            .map(|swap| swap.signature.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            signatures,
            vec![
                "sig-journal-backpressure-00000",
                "sig-journal-backpressure-00001",
                "sig-journal-backpressure-00010",
                "sig-journal-backpressure-00011",
                "sig-journal-backpressure-00012",
                "sig-journal-backpressure-00020",
                "sig-journal-backpressure-00021",
                "sig-journal-backpressure-00022",
                "sig-journal-backpressure-00023",
            ],
            "coalesced overflow drain must preserve source order"
        );
        Ok(())
    }

    #[test]
    fn recent_raw_journal_overflow_drain_channel_full_restores_coalesced_front_without_loss_stage1(
    ) -> Result<()> {
        let telemetry = ObservedSwapWriterTelemetry::default();
        telemetry
            .journal_overflow_row_debt_capacity
            .store(64, Ordering::Relaxed);
        let (journal_sender, journal_receiver) =
            std_mpsc::sync_channel::<super::RecentRawJournalWriteRequest>(1);
        let scenario_now = DateTime::parse_from_rfc3339("2026-04-28T20:21:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);

        let prefilled_request = recent_raw_journal_write_request_for_test(99, 1, scenario_now);
        let prefilled_rows = prefilled_request.row_count();
        journal_sender
            .try_send(prefilled_request)
            .map_err(|error| anyhow!("failed to prefill recent_raw journal channel: {error}"))?;
        telemetry.note_journal_queue_enqueued(prefilled_rows);

        let mut journal_overflow = std::collections::VecDeque::new();
        for (start_idx, rows) in [(0usize, 2usize), (10, 3), (20, 4)] {
            let request = recent_raw_journal_write_request_for_test(start_idx, rows, scenario_now);
            telemetry.note_journal_overflow_enqueued(request.row_count());
            journal_overflow.push_back(request);
        }
        let before = telemetry.snapshot();
        assert_eq!(before.journal_overflow_depth_batches, 3);
        assert_eq!(before.journal_overflow_row_debt, 9);

        super::drain_recent_raw_journal_overflow_nonblocking(
            &journal_sender,
            &mut journal_overflow,
            8,
            &telemetry,
        )?;

        let blocked_snapshot = telemetry.snapshot();
        assert_eq!(
            blocked_snapshot.journal_queue_depth_batches, 1,
            "full channel should keep the original queued request visible"
        );
        assert_eq!(
            blocked_snapshot.journal_overflow_depth_batches, 1,
            "full channel should restore one coalesced request to the overflow front"
        );
        assert_eq!(
            blocked_snapshot.journal_overflow_row_debt, 9,
            "full channel path must not lose overflow rows"
        );
        assert_eq!(
            super::recent_raw_journal_overflow_row_debt(&journal_overflow),
            9,
            "in-memory overflow row debt must match telemetry after coalesced restore"
        );

        let prefilled = journal_receiver.recv()?;
        telemetry.note_journal_queue_dequeued(prefilled.row_count());
        assert_eq!(prefilled.row_count(), 1);
        super::drain_recent_raw_journal_overflow_nonblocking(
            &journal_sender,
            &mut journal_overflow,
            8,
            &telemetry,
        )?;
        assert!(
            journal_overflow.is_empty(),
            "coalesced overflow request should drain after channel capacity returns"
        );
        let restored = journal_receiver.recv()?;
        assert_eq!(
            restored.row_count(),
            9,
            "all rows from the full-channel coalesced request must remain retryable"
        );
        let final_snapshot = telemetry.snapshot();
        assert_eq!(final_snapshot.journal_overflow_depth_batches, 0);
        assert_eq!(final_snapshot.journal_overflow_row_debt, 0);
        Ok(())
    }

    #[test]
    fn recent_raw_journal_adaptive_coalesce_collects_refilling_small_requests_stage1() -> Result<()>
    {
        let telemetry = ObservedSwapWriterTelemetry::default();
        telemetry
            .journal_queue_capacity_batches
            .store(64, Ordering::Relaxed);
        telemetry
            .journal_overflow_depth_batches
            .store(1, Ordering::Relaxed);
        let (journal_sender, journal_receiver) =
            std_mpsc::sync_channel::<super::RecentRawJournalWriteRequest>(64);
        let scenario_now = DateTime::parse_from_rfc3339("2026-04-28T20:12:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let first_request = super::RecentRawJournalWriteRequest {
            inserted_swaps: vec![recent_raw_journal_backpressure_swap(0, scenario_now)],
        };
        let first_rows = first_request.row_count();
        journal_sender.send(first_request)?;
        telemetry.note_journal_queue_enqueued(first_rows);

        let refill_sender = journal_sender.clone();
        let (refill_started_tx, refill_started_rx) = std_mpsc::channel::<()>();
        let producer = std::thread::spawn(move || -> Result<()> {
            let _ = refill_started_tx.send(());
            std::thread::sleep(StdDuration::from_millis(1));
            for idx in 1..17usize {
                let request = super::RecentRawJournalWriteRequest {
                    inserted_swaps: vec![recent_raw_journal_backpressure_swap(idx, scenario_now)],
                };
                refill_sender.send(request)?;
            }
            Ok(())
        });
        refill_started_rx.recv()?;

        let first_request = journal_receiver.recv()?;
        let collected_batch = super::collect_recent_raw_journal_write_batch(
            &journal_receiver,
            first_request,
            1,
            16,
            16,
            &telemetry,
        );
        producer
            .join()
            .expect("refill producer thread panicked")
            .context("refill producer failed")?;

        assert!(
            collected_batch.request_batches > 1,
            "adaptive recent_raw journal coalescing must collect requests that arrive shortly after the first dequeue: {collected_batch:?}"
        );
        assert_eq!(
            collected_batch.inserted_swaps.len(),
            collected_batch.request_batches
        );
        assert!(
            collected_batch.request_batches <= 16,
            "adaptive coalescing must remain bounded by the explicit adaptive limit: {collected_batch:?}"
        );
        assert!(
            collected_batch.coalesce_elapsed_ms
                <= super::OBSERVED_SWAP_RECENT_RAW_JOURNAL_ADAPTIVE_COALESCE_WINDOW
                    .as_millis()
                    .saturating_add(5) as u64,
            "adaptive coalescing must use only the tiny bounded wait window: {collected_batch:?}"
        );
        Ok(())
    }

    #[test]
    fn recent_raw_journal_writer_coalesces_queued_tiny_requests_to_row_cap_under_pressure_stage1(
    ) -> Result<()> {
        let telemetry = ObservedSwapWriterTelemetry::default();
        telemetry
            .journal_queue_capacity_batches
            .store(128, Ordering::Relaxed);
        telemetry
            .journal_overflow_depth_batches
            .store(1, Ordering::Relaxed);
        let (journal_sender, journal_receiver) =
            std_mpsc::sync_channel::<super::RecentRawJournalWriteRequest>(128);
        let scenario_now = DateTime::parse_from_rfc3339("2026-04-28T20:22:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        for idx in 0..64usize {
            let request = recent_raw_journal_write_request_for_test(idx, 1, scenario_now);
            let request_rows = request.row_count();
            journal_sender.send(request)?;
            telemetry.note_journal_queue_enqueued(request_rows);
        }

        let first_request = journal_receiver.recv()?;
        let collected_batch = super::collect_recent_raw_journal_write_batch(
            &journal_receiver,
            first_request,
            4,
            64,
            32,
            &telemetry,
        );

        assert_eq!(
            collected_batch.inserted_swaps.len(),
            32,
            "writer-side coalescing under pressure should drain queued tiny requests into a fat bounded row batch"
        );
        assert_eq!(collected_batch.request_batches, 32);
        assert_eq!(collected_batch.coalesce_limit_rows, 32);
        let snapshot = telemetry.snapshot();
        assert_eq!(
            snapshot.journal_queue_depth_batches, 32,
            "row-capped writer drain should leave the remaining queued requests visible"
        );
        assert_eq!(snapshot.journal_queue_row_debt, 32);
        Ok(())
    }

    #[test]
    fn recent_raw_journal_writer_no_pressure_path_collects_only_available_request_stage1(
    ) -> Result<()> {
        let telemetry = ObservedSwapWriterTelemetry::default();
        telemetry
            .journal_queue_capacity_batches
            .store(8, Ordering::Relaxed);
        let (journal_sender, journal_receiver) =
            std_mpsc::sync_channel::<super::RecentRawJournalWriteRequest>(8);
        let scenario_now = DateTime::parse_from_rfc3339("2026-04-28T20:23:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let first_request = recent_raw_journal_write_request_for_test(0, 1, scenario_now);
        let first_rows = first_request.row_count();
        journal_sender.send(first_request)?;
        telemetry.note_journal_queue_enqueued(first_rows);

        let first_request = journal_receiver.recv()?;
        let collected_batch = super::collect_recent_raw_journal_write_batch(
            &journal_receiver,
            first_request,
            4,
            64,
            32,
            &telemetry,
        );

        assert_eq!(
            collected_batch.request_batches, 1,
            "without queue/overflow pressure and without immediately queued work, writer coalescing should not wait for more requests"
        );
        assert_eq!(collected_batch.inserted_swaps.len(), 1);
        assert_eq!(telemetry.snapshot().journal_queue_depth_batches, 0);
        Ok(())
    }

    #[test]
    fn recent_raw_journal_queue_accounting_tolerates_recv_before_enqueue_note_stage1() {
        let telemetry = ObservedSwapWriterTelemetry::default();
        telemetry
            .journal_queue_capacity_batches
            .store(16, Ordering::Relaxed);

        telemetry.note_journal_queue_dequeued(7);
        let before_enqueue_snapshot = telemetry.snapshot();
        assert_eq!(before_enqueue_snapshot.journal_queue_depth_batches, 0);
        assert_eq!(before_enqueue_snapshot.journal_queue_row_debt, 0);

        telemetry.note_journal_queue_enqueued(7);
        let after_out_of_order_pair_snapshot = telemetry.snapshot();
        assert_eq!(
            after_out_of_order_pair_snapshot.journal_queue_depth_batches,
            0
        );
        assert_eq!(after_out_of_order_pair_snapshot.journal_queue_row_debt, 0);

        let capacity_telemetry = ObservedSwapWriterTelemetry::default();
        capacity_telemetry
            .journal_queue_capacity_batches
            .store(16, Ordering::Relaxed);
        for _ in 0..17 {
            capacity_telemetry.note_journal_queue_enqueued(1);
        }
        assert_eq!(
            capacity_telemetry.snapshot().journal_queue_depth_batches,
            16
        );
    }

    #[test]
    fn recent_raw_journal_overflow_row_debt_cap_exceeded_fails_closed_stage1() -> Result<()> {
        let unique = format!(
            "copybot-app-recent-raw-journal-row-debt-cap-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let runtime_db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let journal_db_path = std::env::temp_dir().join(format!("{unique}-recent-raw.db"));
        let migration_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        let mut runtime_store = SqliteStore::open(Path::new(&runtime_db_path))?;
        runtime_store.run_migrations(&migration_dir)?;
        let mut journal_seed_store = SqliteStore::open(Path::new(&journal_db_path))?;
        journal_seed_store.run_migrations(&migration_dir)?;
        drop(journal_seed_store);
        let scenario_now = DateTime::parse_from_rfc3339("2026-04-28T20:15:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let runtime = Builder::new_current_thread().enable_all().build()?;
        let writer = ObservedSwapWriter::start_with_config(
            runtime_db_path
                .to_str()
                .context("runtime sqlite path must be valid utf-8")?
                .to_string(),
            ObservedSwapWriterConfig::for_test(
                16,
                1,
                false,
                aggregate_write_config(),
                Some(ObservedSwapRecentRawJournalConfig {
                    sqlite_path: journal_db_path
                        .to_str()
                        .context("journal sqlite path must be valid utf-8")?
                        .to_string(),
                    retention_days: 8,
                    writer_queue_capacity_batches: 1,
                    write_coalesce_max_batches: 1,
                    overflow_capacity_batches: 1,
                    skip_prune_while_backlogged: true,
                    skip_startup_prune: true,
                }),
            ),
        )?;
        assert_eq!(
            writer.snapshot().journal_overflow_row_debt_capacity,
            1,
            "cap fixture should be small enough to prove terminal row-debt overflow"
        );

        runtime.block_on(async {
            writer
                .write(&recent_raw_journal_backpressure_swap(0, scenario_now))
                .await
        })?;
        let journal_baseline_started = Instant::now();
        loop {
            let journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
            let journal_rows = journal_store
                .load_observed_swaps_since(scenario_now - ChronoDuration::minutes(1))?
                .len();
            if journal_rows >= 1 {
                break;
            }
            if journal_baseline_started.elapsed() > StdDuration::from_secs(2) {
                anyhow::bail!("recent_raw journal writer did not persist baseline before cap test");
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }

        let blocker_conn = Connection::open(Path::new(&journal_db_path))
            .context("failed to open journal blocker connection")?;
        blocker_conn
            .busy_timeout(StdDuration::from_millis(1))
            .context("failed setting journal blocker busy timeout")?;
        blocker_conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        for idx in 1..128usize {
            let swap = recent_raw_journal_backpressure_swap(idx, scenario_now);
            let _ = runtime.block_on(async {
                timeout(StdDuration::from_millis(100), writer.write(&swap)).await
            });
            if writer.ensure_running().is_err() {
                break;
            }
        }

        let error_chain = wait_for_writer_terminal_failure(&writer)?;
        assert!(
            error_chain
                .contains(super::OBSERVED_SWAP_RECENT_RAW_JOURNAL_OVERFLOW_ROW_DEBT_CAP_EXCEEDED),
            "row-debt cap breach must fail closed with explicit reason: {error_chain}"
        );
        let snapshot_at_failure = writer.snapshot();
        assert!(
            snapshot_at_failure.journal_overflow_row_debt
                <= snapshot_at_failure.journal_overflow_row_debt_capacity,
            "the rejected request must not inflate in-memory row debt beyond the cap: {snapshot_at_failure:?}"
        );
        let journal_store_while_blocked = SqliteStore::open(Path::new(&journal_db_path))?;
        let journal_state_while_blocked = journal_store_while_blocked.recent_raw_journal_state()?;
        assert_eq!(
            journal_state_while_blocked.row_count, 1,
            "journal state must not advance while the journal DB write is blocked, even when raw writer fails closed"
        );

        blocker_conn.execute_batch("COMMIT")?;
        drop(blocker_conn);
        let shutdown_error = writer
            .shutdown()
            .expect_err("shutdown should surface explicit row-debt cap failure");
        let shutdown_chain = format!("{shutdown_error:#}");
        assert!(
            shutdown_chain
                .contains(super::OBSERVED_SWAP_RECENT_RAW_JOURNAL_OVERFLOW_ROW_DEBT_CAP_EXCEEDED),
            "shutdown should preserve explicit terminal cap reason: {shutdown_chain}"
        );

        remove_sqlite_test_files(&runtime_db_path);
        remove_sqlite_test_files(&journal_db_path);
        Ok(())
    }

    #[test]
    fn discovery_aggregate_per_request_write_recreates_post_checkpoint_saturation_even_without_recent_raw_journal_stage1(
    ) -> Result<()> {
        let summary = run_discovery_aggregate_backpressure_scenario(1, 0, false, 0)?;
        assert!(
            summary.baseline_rows_persisted >= 32,
            "clean checkpoint baseline should permit immediate raw persistence before aggregate pressure accumulates on the same runtime DB: {summary:?}"
        );
        assert!(
            summary.max_pending_requests >= 32,
            "current per-request aggregate path should recreate meaningful upstream pending request saturation on the same clean-start workload even when the recent_raw journal path is absent: {summary:?}"
        );
        assert!(
            summary.pending_requests_after_load > 0 || summary.aggregate_queue_depth_after_load > 0,
            "the reduced incident class should still leave visible post-load pressure without depending on a scheduler-sensitive raw-pending sample: {summary:?}"
        );
        assert!(
            summary.aggregate_queue_depth_after_load > 0
                || summary.max_aggregate_queue_depth_batches > 0,
            "the repro must still exercise real downstream aggregate backlog during the modeled load, even if a scheduler-sensitive final sample drains before observation: {summary:?}"
        );
        assert!(
            summary.max_aggregate_queue_depth_batches > 0,
            "the repro must exercise downstream aggregate backlog rather than a raw-path-only slowdown: {summary:?}"
        );
        assert_eq!(
            summary.aggregate_overflow_depth_after_load, 0,
            "the current per-request aggregate path should not have any separate overflow safety valve; raw persistence is coupled directly to the bounded aggregate queue today: {summary:?}"
        );
        assert_eq!(
            summary.max_aggregate_overflow_depth_batches, 0,
            "the current path should reproduce the incident without any extra hidden aggregate backlog layer: {summary:?}"
        );
        assert!(
            summary.runtime_wal_bytes_after_load > 1_000_000,
            "the reduced incident class should regrow real runtime WAL debt on the same clean-start workload: {summary:?}"
        );
        assert!(
            summary.sqlite_write_retry_delta <= 16,
            "the aggregate recurrence repro should not require material retryable sqlite lock growth; queue coupling on the shared runtime DB is sufficient: {summary:?}"
        );
        assert!(
            summary.sqlite_busy_error_delta <= 16,
            "the aggregate recurrence repro should not require material busy-error churn; bounded downstream aggregate backlog is enough to recreate the incident class: {summary:?}"
        );
        assert!(
            !summary.gap_cursor_present_after_load,
            "current hot-path aggregate coupling should reproduce the incident without falling back to an explicit materialization gap: {summary:?}"
        );
        Ok(())
    }

    #[test]
    fn discovery_aggregate_gap_fallback_keeps_raw_ingestion_live_and_replays_gap_after_pressure_stage1(
    ) -> Result<()> {
        let old = run_discovery_aggregate_backpressure_scenario(1, 0, false, 0)?;
        let new = run_discovery_aggregate_backpressure_scenario(
            OBSERVED_SWAP_DISCOVERY_AGGREGATE_WRITE_COALESCE_MAX_BATCHES,
            64,
            true,
            OBSERVED_SWAP_DISCOVERY_AGGREGATE_IDLE_REPLAY_MAX_PAGES,
        )?;

        assert!(
            old.max_pending_requests >= 32,
            "old per-request aggregate path must recreate meaningful raw pending pressure for the A/B proof to be meaningful: old={old:?}"
        );
        assert!(
            new.pending_requests_after_idle < 64,
            "the new path should not leave the raw writer in a large sustained backlog state after the same pressure wave clears and idle replay has run: old={old:?} new={new:?}"
        );
        assert!(
            new.persisted_rows_after_load >= old.persisted_rows_after_load,
            "the fix should not reduce pending depth by simply persisting fewer raw rows under the same modeled workload: old={old:?} new={new:?}"
        );
        assert!(
            old.aggregate_queue_depth_after_load > 0 || old.max_aggregate_queue_depth_batches > 0,
            "old path must still exercise real downstream aggregate backlog during the modeled load so the A/B proof exercises the same shared-db choke, even if the final sample drains before observation: old={old:?}"
        );
        assert!(
            new.aggregate_queue_depth_after_idle == 0 && new.aggregate_overflow_depth_after_idle == 0,
            "the new path should convert sustained shared-db aggregate pressure into an explicit materialization gap/replay path instead of leaving bounded aggregate backlog after idle replay: old={old:?} new={new:?}"
        );
        assert!(
            new.gap_cursor_present_after_load
                || (new.gap_cursor_cleared_after_idle
                    && new.covered_through_reached_tail_after_idle),
            "the new path must either expose a materialization gap after load or clear it through bounded replay before the snapshot; skipped aggregate work must not be silently dropped: old={old:?} new={new:?}"
        );
        assert!(
            new.max_aggregate_queue_depth_batches < old.max_aggregate_queue_depth_batches,
            "the explicit gap fallback should keep the aggregate queue from remaining in the same sustained post-load backlog shape: old={old:?} new={new:?}"
        );
        assert!(
            new.max_aggregate_overflow_depth_batches < 64,
            "the bounded overflow valve must remain visible and bounded while the raw path stays live: old={old:?} new={new:?}"
        );
        assert!(
            new.runtime_wal_bytes_after_load <= old.runtime_wal_bytes_after_load * 2 + 1,
            "the fix should not merely trade queue pressure for runaway runtime WAL growth: old={old:?} new={new:?}"
        );
        assert!(
            new.gap_cursor_cleared_after_idle,
            "once the sustained load stops, the aggregate worker should replay the explicit gap instead of requiring a process restart: old={old:?} new={new:?}"
        );
        assert!(
            new.covered_through_reached_tail_after_idle,
            "idle replay should drive discovery aggregate coverage back to the exact raw tail after the pressure wave clears: old={old:?} new={new:?}"
        );
        Ok(())
    }

    #[test]
    fn recent_raw_journal_prune_due_stays_paused_while_overflow_backlog_exists_stage1() -> Result<()>
    {
        let unique = format!(
            "copybot-app-recent-raw-journal-prune-overflow-{}-{}",
            std::process::id(),
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(Utc::now().timestamp_micros() * 1000)
        );
        let journal_db_path = std::env::temp_dir().join(format!("{unique}.db"));
        let journal_store = SqliteStore::open(Path::new(&journal_db_path))?;
        journal_store.ensure_recent_raw_journal_tables()?;
        let now = DateTime::parse_from_rfc3339("2026-04-08T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let telemetry = ObservedSwapWriterTelemetry::default();
        telemetry
            .journal_overflow_depth_batches
            .store(3, Ordering::Relaxed);
        let should_prune = recent_raw_journal_prune_due(
            &journal_store,
            &ObservedSwapRecentRawJournalConfig {
                sqlite_path: journal_db_path
                    .to_str()
                    .context("journal sqlite path must be valid utf-8")?
                    .to_string(),
                retention_days: 8,
                writer_queue_capacity_batches: 16,
                write_coalesce_max_batches: 1,
                overflow_capacity_batches: 64,
                skip_prune_while_backlogged: true,
                skip_startup_prune: true,
            },
            &telemetry,
            now,
        )?;
        assert!(
            !should_prune,
            "recent_raw journal prune must stay paused while overflow backlog still represents unflushed hot-path work"
        );
        let inflight_telemetry = ObservedSwapWriterTelemetry::default();
        inflight_telemetry.note_journal_queue_enqueued(1);
        inflight_telemetry.note_journal_writer_inflight_started(4);
        let should_prune_while_inflight = recent_raw_journal_prune_due(
            &journal_store,
            &ObservedSwapRecentRawJournalConfig {
                sqlite_path: journal_db_path
                    .to_str()
                    .context("journal sqlite path must be valid utf-8")?
                    .to_string(),
                retention_days: 8,
                writer_queue_capacity_batches: 16,
                write_coalesce_max_batches: 1,
                overflow_capacity_batches: 64,
                skip_prune_while_backlogged: true,
                skip_startup_prune: true,
            },
            &inflight_telemetry,
            now,
        )?;
        assert!(
            !should_prune_while_inflight,
            "recent_raw journal prune/check must stay paused while a hot-path journal write is still inflight"
        );
        let _ = std::fs::remove_file(journal_db_path);
        Ok(())
    }
}
