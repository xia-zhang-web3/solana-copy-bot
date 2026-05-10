use super::*;

pub(crate) use anyhow::{anyhow, bail, Context, Result};
pub(crate) use chrono::{DateTime, Duration, Utc};
pub(crate) use copybot_config::load_from_path;
pub(crate) use copybot_runtime_artifacts::{
    copy_atomic, journal_snapshot_archive_path, journal_snapshot_latest_metadata_path,
    journal_snapshot_latest_path, journal_snapshot_metadata_path, load_json, resolve_db_path,
    resolve_relative_to_config, write_json_atomic, JOURNAL_SNAPSHOT_ARCHIVE_PREFIX,
    JOURNAL_SNAPSHOT_ARCHIVE_SUFFIX,
};
pub(crate) use copybot_storage_core::{
    ensure_discovery_v2_schema, is_fatal_sqlite_anyhow_error, DiscoveryRuntimeCursor,
    RecentRawJournalStateRow, RecentRawJournalWriteSummary, SqliteSnapshotOutcome,
    SqliteSnapshotPolicy, SqliteSnapshotSourceMetrics, SqliteSnapshotSummary, SqliteStore,
};
pub(crate) use rusqlite::ErrorCode;
pub(crate) use serde::{Deserialize, Serialize};
pub(crate) use std::env;
pub(crate) use std::fs;
pub(crate) use std::path::{Path, PathBuf};
pub(crate) use std::process;
pub(crate) use std::time::{Duration as StdDuration, Instant, SystemTime, UNIX_EPOCH};
pub(crate) use tracing::warn;

pub(crate) const USAGE: &str = "usage: discovery_recent_raw_snapshot --config <path> [--journal-db-path <path>] (--output <path> | --scheduled) [--force] [--json] [--now <rfc3339>]";
pub(crate) const SNAPSHOT_SMALL_SOURCE_TOTAL_BYTES: u64 = 128 * 1024 * 1024;
pub(crate) const SNAPSHOT_LARGE_SOURCE_TOTAL_BYTES: u64 = 512 * 1024 * 1024;
pub(crate) const SNAPSHOT_HUGE_SOURCE_TOTAL_BYTES: u64 = 1024 * 1024 * 1024;
pub(crate) const SNAPSHOT_SMALL_TARGET_STEPS: usize = 512;
pub(crate) const SNAPSHOT_MEDIUM_TARGET_STEPS: usize = 384;
pub(crate) const SNAPSHOT_LARGE_TARGET_STEPS: usize = 320;
pub(crate) const SNAPSHOT_HUGE_TARGET_STEPS: usize = 256;
pub(crate) const SNAPSHOT_MIN_PAGES_PER_STEP: usize = 64;
pub(crate) const SNAPSHOT_MAX_PAGES_PER_STEP: usize = 1024;
pub(crate) const SNAPSHOT_SMALL_PAUSE_BETWEEN_STEPS_MS: u64 = 5;
pub(crate) const SNAPSHOT_MEDIUM_PAUSE_BETWEEN_STEPS_MS: u64 = 2;
pub(crate) const SNAPSHOT_LARGE_PAUSE_BETWEEN_STEPS_MS: u64 = 1;
pub(crate) const SNAPSHOT_SMALL_MAX_ATTEMPT_DURATION_MS: u64 = 45_000;
pub(crate) const SNAPSHOT_MEDIUM_MAX_ATTEMPT_DURATION_MS: u64 = 60_000;
pub(crate) const SNAPSHOT_LARGE_MAX_ATTEMPT_DURATION_MS: u64 = 90_000;
pub(crate) const SNAPSHOT_HUGE_MAX_ATTEMPT_DURATION_MS: u64 = 120_000;
pub(crate) const SNAPSHOT_RESUME_ROW_BATCH_MULTIPLIER: usize = 64;
pub(crate) const SNAPSHOT_RESUME_ROW_BATCH_MAX_ROWS: usize = 65_536;
pub(crate) const STAGED_ARCHIVE_SNAPSHOT_SUFFIX: &str = ".archive-staged";
pub(crate) const STAGED_ARCHIVE_METADATA_SUFFIX: &str = ".archive-staged.json";
pub(crate) const LATEST_ATTEMPT_TELEMETRY_FILE_NAME: &str =
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
pub(crate) struct Config {
    pub(crate) config_path: PathBuf,
    pub(crate) journal_db_path: Option<PathBuf>,
    pub(crate) output_path: Option<PathBuf>,
    pub(crate) scheduled: bool,
    pub(crate) force: bool,
    pub(crate) json: bool,
    pub(crate) now: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RecentRawJournalSnapshotManifest {
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) source_db_path: String,
    pub(crate) snapshot_path: String,
    pub(crate) row_count: usize,
    pub(crate) covered_since: Option<DateTime<Utc>>,
    pub(crate) covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    pub(crate) last_batch_completed_at: Option<DateTime<Utc>>,
    pub(crate) updated_at: Option<DateTime<Utc>>,
    pub(crate) snapshot_bytes: u64,
}
