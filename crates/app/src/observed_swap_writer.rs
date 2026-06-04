use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use copybot_core_types::SwapEvent;
use copybot_ingestion::IngestionRuntimeSnapshot;
use copybot_storage_core::{
    is_retryable_sqlite_anyhow_error, sqlite_contention_snapshot, RecentRawJournalWriteSummary,
    SqliteBatchedDeleteSummary, SqliteContentionSnapshot, SqliteStore,
};
use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc as std_mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration as StdDuration, Instant};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

mod capacity;
mod misc;
mod raw_writer_loop;
mod recent_raw_journal;
mod recent_raw_journal_drain;
mod recent_raw_journal_enqueue;
mod retention;
mod sqlite_retry;
mod telemetry;
mod types_config;
mod writer_api;

use capacity::{
    observed_swap_writer_normal_try_enqueue_pressure_active,
    observed_swap_writer_normal_try_enqueue_soft_limit,
    recent_raw_journal_adaptive_coalesce_pressure,
    recent_raw_journal_adaptive_write_coalesce_max_batches,
    recent_raw_journal_adaptive_write_coalesce_max_rows, recent_raw_journal_overflow_row_capacity,
};
use misc::*;
use raw_writer_loop::*;
use recent_raw_journal::*;
use recent_raw_journal_drain::*;
use recent_raw_journal_enqueue::*;
pub(crate) use retention::run_observed_swap_retention_maintenance_once;
use retention::*;
use sqlite_retry::observed_swap_retention_checkpoint_error_requires_abort;
use telemetry::*;
use types_config::*;
pub(crate) use types_config::{
    ObservedSwapRecentRawJournalConfig, ObservedSwapRetentionConfig,
    ObservedSwapRetentionMaintenanceSummary, ObservedSwapRetentionRuntimeHealthHandle,
    ObservedSwapWriterSnapshot,
};
pub(crate) use writer_api::ObservedSwapWriter;

pub(crate) const OBSERVED_SWAP_WRITER_CHANNEL_CAPACITY: usize = 4096;
const OBSERVED_SWAP_BATCH_MAX_SIZE: usize = 128;
const OBSERVED_SWAP_WRITER_DISCOVERY_CRITICAL_RESERVED_BATCHES: usize = 1;
const OBSERVED_SWAP_WRITER_NONCRITICAL_IRRELEVANT_MAX_QUEUED_BATCHES: usize = 1;
pub(crate) const OBSERVED_SWAP_RECENT_RAW_JOURNAL_WRITE_COALESCE_MAX_BATCHES: usize = 32;
pub(crate) const OBSERVED_SWAP_RECENT_RAW_JOURNAL_OVERFLOW_CAPACITY_MULTIPLIER: usize = 4;
const OBSERVED_SWAP_RECENT_RAW_JOURNAL_ADAPTIVE_COALESCE_MAX_BATCHES_MULTIPLIER: usize = 8;
const OBSERVED_SWAP_RECENT_RAW_JOURNAL_ADAPTIVE_COALESCE_MAX_BATCHES_CAP: usize = 512;
const OBSERVED_SWAP_RECENT_RAW_JOURNAL_ADAPTIVE_COALESCE_MAX_ROWS_CAP: usize = 4_096;
const OBSERVED_SWAP_RECENT_RAW_JOURNAL_ADAPTIVE_COALESCE_WINDOW: StdDuration =
    StdDuration::from_millis(10);
const OBSERVED_SWAP_RECENT_RAW_JOURNAL_ADAPTIVE_COALESCE_POLL: StdDuration =
    StdDuration::from_millis(2);
const OBSERVED_SWAP_RECENT_RAW_JOURNAL_STARTUP_TIMEOUT: StdDuration = StdDuration::from_secs(30);
const OBSERVED_SWAP_RECENT_RAW_JOURNAL_OVERFLOW_ROW_DEBT_CAP_EXCEEDED: &str =
    "observed_swap_writer_recent_raw_journal_overflow_row_debt_capacity_exceeded";
const OBSERVED_SWAP_RECENT_RAW_JOURNAL_WRITE_DEADLINE: StdDuration = StdDuration::from_secs(10);
const OBSERVED_SWAP_RECENT_RAW_JOURNAL_WRITE_DEADLINE_EXHAUSTED: &str =
    "observed_swap_writer_recent_raw_journal_write_deadline_exhausted";
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
const OBSERVED_SWAP_RETENTION_DELETE_BATCH_SIZE: usize = 10_000;
const OBSERVED_SWAP_RETENTION_MAX_RAW_DELETE_BATCHES_PER_RUN: usize = 10;
const OBSERVED_SWAP_RETENTION_MAX_DURATION_PER_RUN: StdDuration = StdDuration::from_secs(15);
const OBSERVED_SWAP_RETENTION_INTER_BATCH_PAUSE: StdDuration = StdDuration::from_millis(25);
const RECENT_RAW_JOURNAL_RETENTION_DELETE_BATCH_SIZE: usize = 10_000;
const RECENT_RAW_JOURNAL_RETENTION_MAX_DELETE_BATCHES_PER_RUN: usize = 10;
const RECENT_RAW_JOURNAL_RETENTION_MAX_DURATION_PER_RUN: StdDuration = StdDuration::from_secs(5);
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

#[cfg(test)]
mod tests;
