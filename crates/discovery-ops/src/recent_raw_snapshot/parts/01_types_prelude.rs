use super::*;

pub(super) use anyhow::{anyhow, bail, Context, Result};
pub(super) use chrono::{DateTime, Duration, Utc};
pub(super) use copybot_config::load_from_path;
pub(super) use copybot_runtime_artifacts::{
    copy_atomic, journal_snapshot_archive_path, journal_snapshot_latest_metadata_path,
    journal_snapshot_latest_path, journal_snapshot_metadata_path, load_json, resolve_db_path,
    resolve_relative_to_config, write_json_atomic, JOURNAL_SNAPSHOT_ARCHIVE_PREFIX,
    JOURNAL_SNAPSHOT_ARCHIVE_SUFFIX,
};
pub(super) use copybot_storage_core::{
    is_fatal_sqlite_anyhow_error, DiscoveryRuntimeCursor, RecentRawJournalStateRow,
    RecentRawJournalWriteSummary, SqliteSnapshotOutcome, SqliteSnapshotPolicy,
    SqliteSnapshotSourceMetrics, SqliteSnapshotSummary, SqliteStore,
};
pub(super) use rusqlite::ErrorCode;
pub(super) use serde::{Deserialize, Serialize};
pub(super) use std::env;
pub(super) use std::fs;
pub(super) use std::path::{Path, PathBuf};
pub(super) use std::process;
pub(super) use std::time::{Duration as StdDuration, Instant, SystemTime, UNIX_EPOCH};
pub(super) use tracing::warn;

pub(super) const USAGE: &str = "usage: discovery_recent_raw_snapshot --config <path> [--journal-db-path <path>] (--output <path> | --scheduled) [--force] [--json] [--now <rfc3339>]";
pub(super) const SNAPSHOT_SMALL_SOURCE_TOTAL_BYTES: u64 = 128 * 1024 * 1024;
pub(super) const SNAPSHOT_LARGE_SOURCE_TOTAL_BYTES: u64 = 512 * 1024 * 1024;
pub(super) const SNAPSHOT_HUGE_SOURCE_TOTAL_BYTES: u64 = 1024 * 1024 * 1024;
pub(super) const SNAPSHOT_SMALL_TARGET_STEPS: usize = 512;
pub(super) const SNAPSHOT_MEDIUM_TARGET_STEPS: usize = 384;
pub(super) const SNAPSHOT_LARGE_TARGET_STEPS: usize = 320;
pub(super) const SNAPSHOT_HUGE_TARGET_STEPS: usize = 256;
pub(super) const SNAPSHOT_MIN_PAGES_PER_STEP: usize = 64;
pub(super) const SNAPSHOT_MAX_PAGES_PER_STEP: usize = 1024;
pub(super) const SNAPSHOT_SMALL_PAUSE_BETWEEN_STEPS_MS: u64 = 5;
pub(super) const SNAPSHOT_MEDIUM_PAUSE_BETWEEN_STEPS_MS: u64 = 2;
pub(super) const SNAPSHOT_LARGE_PAUSE_BETWEEN_STEPS_MS: u64 = 1;
pub(super) const SNAPSHOT_SMALL_MAX_ATTEMPT_DURATION_MS: u64 = 45_000;
pub(super) const SNAPSHOT_MEDIUM_MAX_ATTEMPT_DURATION_MS: u64 = 60_000;
pub(super) const SNAPSHOT_LARGE_MAX_ATTEMPT_DURATION_MS: u64 = 90_000;
pub(super) const SNAPSHOT_HUGE_MAX_ATTEMPT_DURATION_MS: u64 = 120_000;
pub(super) const SNAPSHOT_RESUME_ROW_BATCH_MULTIPLIER: usize = 64;
pub(super) const SNAPSHOT_RESUME_ROW_BATCH_MAX_ROWS: usize = 65_536;
pub(super) const STAGED_ARCHIVE_SNAPSHOT_SUFFIX: &str = ".archive-staged";
pub(super) const STAGED_ARCHIVE_METADATA_SUFFIX: &str = ".archive-staged.json";
pub(super) const LATEST_ATTEMPT_TELEMETRY_FILE_NAME: &str =
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
pub(super) struct Config {
    pub(super) config_path: PathBuf,
    pub(super) journal_db_path: Option<PathBuf>,
    pub(super) output_path: Option<PathBuf>,
    pub(super) scheduled: bool,
    pub(super) force: bool,
    pub(super) json: bool,
    pub(super) now: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct RecentRawJournalSnapshotManifest {
    pub(super) created_at: DateTime<Utc>,
    pub(super) source_db_path: String,
    pub(super) snapshot_path: String,
    pub(super) row_count: usize,
    pub(super) covered_since: Option<DateTime<Utc>>,
    pub(super) covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    pub(super) last_batch_completed_at: Option<DateTime<Utc>>,
    pub(super) updated_at: Option<DateTime<Utc>>,
    pub(super) snapshot_bytes: u64,
}
