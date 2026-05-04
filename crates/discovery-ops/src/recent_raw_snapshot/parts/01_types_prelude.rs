use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::load_from_path;
use copybot_runtime_artifacts::{
    copy_atomic, journal_snapshot_archive_path, journal_snapshot_latest_metadata_path,
    journal_snapshot_latest_path, journal_snapshot_metadata_path, load_json, resolve_db_path,
    resolve_relative_to_config, write_json_atomic, JOURNAL_SNAPSHOT_ARCHIVE_PREFIX,
    JOURNAL_SNAPSHOT_ARCHIVE_SUFFIX,
};
use copybot_storage_core::{
    is_fatal_sqlite_anyhow_error, DiscoveryRuntimeCursor, RecentRawJournalStateRow,
    RecentRawJournalWriteSummary, SqliteSnapshotOutcome, SqliteSnapshotPolicy,
    SqliteSnapshotSourceMetrics, SqliteSnapshotSummary, SqliteStore,
};
use rusqlite::ErrorCode;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process;
use std::time::{Duration as StdDuration, Instant, SystemTime, UNIX_EPOCH};
use tracing::warn;

const USAGE: &str = "usage: discovery_recent_raw_snapshot --config <path> [--journal-db-path <path>] (--output <path> | --scheduled) [--force] [--json] [--now <rfc3339>]";
const SNAPSHOT_SMALL_SOURCE_TOTAL_BYTES: u64 = 128 * 1024 * 1024;
const SNAPSHOT_LARGE_SOURCE_TOTAL_BYTES: u64 = 512 * 1024 * 1024;
const SNAPSHOT_HUGE_SOURCE_TOTAL_BYTES: u64 = 1024 * 1024 * 1024;
const SNAPSHOT_SMALL_TARGET_STEPS: usize = 512;
const SNAPSHOT_MEDIUM_TARGET_STEPS: usize = 384;
const SNAPSHOT_LARGE_TARGET_STEPS: usize = 320;
const SNAPSHOT_HUGE_TARGET_STEPS: usize = 256;
const SNAPSHOT_MIN_PAGES_PER_STEP: usize = 64;
const SNAPSHOT_MAX_PAGES_PER_STEP: usize = 1024;
const SNAPSHOT_SMALL_PAUSE_BETWEEN_STEPS_MS: u64 = 5;
const SNAPSHOT_MEDIUM_PAUSE_BETWEEN_STEPS_MS: u64 = 2;
const SNAPSHOT_LARGE_PAUSE_BETWEEN_STEPS_MS: u64 = 1;
const SNAPSHOT_SMALL_MAX_ATTEMPT_DURATION_MS: u64 = 45_000;
const SNAPSHOT_MEDIUM_MAX_ATTEMPT_DURATION_MS: u64 = 60_000;
const SNAPSHOT_LARGE_MAX_ATTEMPT_DURATION_MS: u64 = 90_000;
const SNAPSHOT_HUGE_MAX_ATTEMPT_DURATION_MS: u64 = 120_000;
const SNAPSHOT_RESUME_ROW_BATCH_MULTIPLIER: usize = 64;
const SNAPSHOT_RESUME_ROW_BATCH_MAX_ROWS: usize = 65_536;
const STAGED_ARCHIVE_SNAPSHOT_SUFFIX: &str = ".archive-staged";
const STAGED_ARCHIVE_METADATA_SUFFIX: &str = ".archive-staged.json";
const LATEST_ATTEMPT_TELEMETRY_FILE_NAME: &str =
    "discovery_recent_raw_snapshot_attempt_latest.json";

pub fn main_entry() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let execution = run(config)?;
    println!("{}", execution.rendered_output);
    if execution.exit_code != 0 {
        process::exit(execution.exit_code);
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    config_path: PathBuf,
    journal_db_path: Option<PathBuf>,
    output_path: Option<PathBuf>,
    scheduled: bool,
    force: bool,
    json: bool,
    now: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RecentRawJournalSnapshotManifest {
    created_at: DateTime<Utc>,
    source_db_path: String,
    snapshot_path: String,
    row_count: usize,
    covered_since: Option<DateTime<Utc>>,
    covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    last_batch_completed_at: Option<DateTime<Utc>>,
    updated_at: Option<DateTime<Utc>>,
    snapshot_bytes: u64,
}
