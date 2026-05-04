use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use copybot_core_types::SwapEvent;
use copybot_ingestion::IngestionRuntimeSnapshot;
use copybot_storage::{
    is_retryable_sqlite_anyhow_error, sqlite_contention_snapshot, DiscoveryAggregateWriteConfig,
    DiscoveryRuntimeCursor, RecentRawJournalWriteSummary, SqliteBatchedDeleteSummary,
    SqliteContentionSnapshot, SqliteStore,
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

mod capacity;
mod sqlite_retry;

#[cfg(test)]
use capacity::observed_swap_writer_discovery_critical_reserve_requests;
use capacity::{
    observed_swap_writer_aggregate_queue_capacity,
    observed_swap_writer_default_aggregate_overflow_capacity_batches,
    observed_swap_writer_normal_try_enqueue_soft_limit,
    recent_raw_journal_adaptive_coalesce_pressure,
    recent_raw_journal_adaptive_write_coalesce_max_batches,
    recent_raw_journal_adaptive_write_coalesce_max_rows, recent_raw_journal_overflow_row_capacity,
};
use sqlite_retry::{
    observed_swap_retention_checkpoint_error_requires_abort,
    observed_swap_retention_protection_load_error_requires_abort,
    observed_swap_writer_discovery_scoring_covered_through_update_error_is_retryable,
    observed_swap_writer_discovery_scoring_error_requires_abort,
    observed_swap_writer_discovery_scoring_replay_apply_error_is_retryable,
    observed_swap_writer_discovery_scoring_rug_finalize_error_is_retryable,
    run_discovery_scoring_replay_stage_with_sqlite_lock_retry,
};

pub(crate) const OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY: usize = 4096;
const OBSERVED_SWAP_BATCH_MAX_SIZE: usize = 128;
const OBSERVED_SWAP_WRITER_DISCOVERY_CRITICAL_RESERVED_BATCHES: usize = 1;
const OBSERVED_SWAP_WRITER_NONCRITICAL_IRRELEVANT_MAX_QUEUED_BATCHES: usize = 1;
pub(crate) const OBSERVED_SWAP_DISCOVERY_AGGREGATE_WRITE_COALESCE_MAX_BATCHES: usize = 16;
pub(crate) const OBSERVED_SWAP_DISCOVERY_AGGREGATE_OVERFLOW_CAPACITY_MULTIPLIER: usize = 4;
const OBSERVED_SWAP_DISCOVERY_AGGREGATE_IDLE_REPLAY_POLL_INTERVAL: StdDuration =
    StdDuration::from_millis(25);
const OBSERVED_SWAP_DISCOVERY_AGGREGATE_IDLE_REPLAY_MAX_PAGES: usize = 64;
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
const OBSERVED_SWAP_DISCOVERY_SCORING_REPLAY_SQLITE_LOCK_MAX_ATTEMPTS: usize = 3;
const OBSERVED_SWAP_DISCOVERY_SCORING_REPLAY_SQLITE_LOCK_INITIAL_BACKOFF: StdDuration =
    StdDuration::from_millis(50);
const OBSERVED_SWAP_DISCOVERY_SCORING_REPLAY_SQLITE_LOCK_MAX_BACKOFF: StdDuration =
    StdDuration::from_millis(400);
const DISCOVERY_AGGREGATE_PHASE_STARTUP_REPLAY_START: &str = "aggregate_startup_replay_start";
const DISCOVERY_AGGREGATE_PHASE_STARTUP_REPLAY_END: &str = "aggregate_startup_replay_end";
const DISCOVERY_AGGREGATE_PHASE_STARTUP_REPLAY_SKIPPED: &str = "aggregate_startup_replay_skipped";
const DISCOVERY_AGGREGATE_PHASE_GAP_REPAIR_SLICE_START: &str = "aggregate_gap_repair_slice_start";
const DISCOVERY_AGGREGATE_PHASE_GAP_REPAIR_SLICE_END: &str = "aggregate_gap_repair_slice_end";
const DISCOVERY_AGGREGATE_PHASE_PAGE_FETCH_START: &str = "page_fetch_start";
const DISCOVERY_AGGREGATE_PHASE_PAGE_FETCH_END: &str = "page_fetch_end";
const DISCOVERY_AGGREGATE_PHASE_APPLY_START: &str = "apply_start";
const DISCOVERY_AGGREGATE_PHASE_APPLY_END: &str = "apply_end";
const DISCOVERY_AGGREGATE_PHASE_RUG_FINALIZE_START: &str = "rug_finalize_start";
const DISCOVERY_AGGREGATE_PHASE_RUG_FINALIZE_END: &str = "rug_finalize_end";
const DISCOVERY_AGGREGATE_PHASE_COVERED_THROUGH_UPDATE_START: &str = "covered_through_update_start";
const DISCOVERY_AGGREGATE_PHASE_COVERED_THROUGH_UPDATE_END: &str = "covered_through_update_end";
const DISCOVERY_AGGREGATE_REASON_AGGREGATE_WRITES_DISABLED: &str = "aggregate_writes_disabled";
const DISCOVERY_AGGREGATE_REASON_MATERIALIZATION_GAP_LATCHED: &str = "materialization_gap_latched";
const DISCOVERY_AGGREGATE_REPAIR_RESUME_SOURCE_GAP_CURSOR: &str = "gap_cursor";
const DISCOVERY_AGGREGATE_REPAIR_RESUME_SOURCE_PERSISTED_COVERED_THROUGH: &str =
    "persisted_covered_through";
const DISCOVERY_AGGREGATE_REPAIR_TARGET_SOURCE_PERSISTED: &str = "persisted";
const DISCOVERY_AGGREGATE_REPAIR_TARGET_SOURCE_NEWLY_FROZEN: &str = "newly_frozen";
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
#[cfg(test)]
static DISCOVERY_AGGREGATE_PHASE_EVENTS_FOR_TEST: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());

pub(crate) const OBSERVED_SWAP_RETENTION_PARTIAL_RETRY_INTERVAL: StdDuration =
    StdDuration::from_secs(60);
const SQLITE_MAINTENANCE_MAX_YELLOWSTONE_OUTPUT_QUEUE_FILL_RATIO: f64 = 0.25;
pub(crate) const OBSERVED_SWAP_WRITER_CHANNEL_CLOSED_CONTEXT: &str =
    "observed swap writer channel closed";
pub(crate) const OBSERVED_SWAP_WRITER_REPLY_CLOSED_CONTEXT: &str =
    "observed swap writer reply channel closed";
pub(crate) const OBSERVED_SWAP_WRITER_TERMINAL_FAILURE_CONTEXT: &str =
    "observed swap writer terminal failure";

include!("observed_swap_writer/01_types_config.rs");
include!("observed_swap_writer/02_telemetry.rs");
include!("observed_swap_writer/03_writer_api.rs");
include!("observed_swap_writer/04_raw_writer_loop.rs");
include!("observed_swap_writer/05_aggregate_overflow.rs");
include!("observed_swap_writer/06_recent_raw_journal.rs");
include!("observed_swap_writer/07_aggregate_apply.rs");
include!("observed_swap_writer/08_retention.rs");
include!("observed_swap_writer/09_gap_epoch.rs");
include!("observed_swap_writer/10_gap_repair.rs");
include!("observed_swap_writer/11_aggregate_replay.rs");
include!("observed_swap_writer/12_misc.rs");

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

    include!("observed_swap_writer/tests_00_helpers_core.rs");
    include!("observed_swap_writer/tests_01_helpers_scenarios.rs");
    include!("observed_swap_writer/tests_10_raw_basic_a.rs");
    include!("observed_swap_writer/tests_11_raw_basic_b.rs");
    include!("observed_swap_writer/tests_20_gap_repair_a.rs");
    include!("observed_swap_writer/tests_21_gap_repair_b.rs");
    include!("observed_swap_writer/tests_30_scoring_retry_a.rs");
    include!("observed_swap_writer/tests_31_scoring_retry_b.rs");
    include!("observed_swap_writer/tests_40_retention_capacity_a.rs");
    include!("observed_swap_writer/tests_41_retention_capacity_b.rs");
    include!("observed_swap_writer/tests_50_startup_recent_raw_a.rs");
    include!("observed_swap_writer/tests_51_startup_recent_raw_b.rs");
    include!("observed_swap_writer/tests_60_recent_raw_deadline.rs");
    include!("observed_swap_writer/tests_61_recent_raw_overflow_a.rs");
    include!("observed_swap_writer/tests_62_recent_raw_overflow_b.rs");
    include!("observed_swap_writer/tests_62_prune_due.rs");
}
