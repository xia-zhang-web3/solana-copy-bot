use super::{
    clear_recent_raw_journal_phase_events_for_test, collect_recent_raw_journal_write_batch,
    drain_recent_raw_journal_overflow_nonblocking, panic_payload_to_string,
    recent_raw_journal_overflow_row_debt, recent_raw_journal_phase_events_for_test,
    recent_raw_journal_prune_due, recent_raw_journal_writer_loop,
    should_checkpoint_after_observed_swap_retention,
    write_recent_raw_journal_batch_with_deadline_attempts, ObservedSwapRecentRawJournalConfig,
    ObservedSwapWriter, ObservedSwapWriterConfig, ObservedSwapWriterTelemetry,
    RecentRawJournalWriteRequest, OBSERVED_SWAP_RECENT_RAW_JOURNAL_ADAPTIVE_COALESCE_WINDOW,
    OBSERVED_SWAP_RECENT_RAW_JOURNAL_WRITE_COALESCE_MAX_BATCHES,
    OBSERVED_SWAP_RECENT_RAW_JOURNAL_WRITE_DEADLINE_EXHAUSTED,
    OBSERVED_SWAP_RETENTION_DELETE_BATCH_SIZE, OBSERVED_SWAP_RETENTION_MAX_DURATION_PER_RUN,
    OBSERVED_SWAP_RETENTION_MAX_RAW_DELETE_BATCHES_PER_RUN,
    RECENT_RAW_JOURNAL_PHASE_BATCH_COLLECTED, RECENT_RAW_JOURNAL_PHASE_BATCH_DONE,
    RECENT_RAW_JOURNAL_PHASE_WRITE_END, RECENT_RAW_JOURNAL_PHASE_WRITE_START,
};
use crate::app_tests::sqlite_contention_delta_test_guard;
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use copybot_core_types::SwapEvent;
use copybot_storage_core::{
    ensure_discovery_v2_schema, sqlite_contention_snapshot, RecentRawJournalWriteSummary,
    SqliteStore,
};
use rusqlite::Connection;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc as std_mpsc, Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration as StdDuration, Instant};
use tokio::runtime::Builder;
use tokio::time::{sleep, timeout, Duration};

static RECENT_RAW_JOURNAL_PHASE_TEST_LOCK: Mutex<()> = Mutex::new(());

fn recent_raw_journal_phase_test_guard() -> MutexGuard<'static, ()> {
    RECENT_RAW_JOURNAL_PHASE_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn prepare_observed_writer_store_for_test(path: &Path) -> Result<SqliteStore> {
    let store = SqliteStore::open(path)?;
    ensure_discovery_v2_schema(&store)?;
    store.ensure_observed_swap_writer_tables()?;
    Ok(store)
}

fn prepare_recent_raw_journal_store_for_test(path: &Path) -> Result<SqliteStore> {
    let store = SqliteStore::open(path)?;
    ensure_discovery_v2_schema(&store)?;
    store.ensure_recent_raw_journal_tables()?;
    Ok(store)
}

#[path = "tests_00_helpers_core.rs"]
mod helpers_core;
#[path = "tests_01_helpers_scenarios.rs"]
mod helpers_scenarios;
#[path = "tests_62_prune_due.rs"]
mod prune_due;
#[path = "tests_10_raw_basic_a.rs"]
mod raw_basic_a;
#[path = "tests_60_recent_raw_deadline.rs"]
mod recent_raw_deadline;
#[path = "tests_61_recent_raw_overflow_a.rs"]
mod recent_raw_overflow_a;
#[path = "tests_63_retention_run.rs"]
mod retention_run;
#[path = "tests_51_startup_recent_raw_b.rs"]
mod startup_recent_raw_b;

use helpers_core::*;
use helpers_scenarios::*;
