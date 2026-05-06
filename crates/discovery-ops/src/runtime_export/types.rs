use chrono::{DateTime, Utc};
use serde::Serialize;
use std::path::PathBuf;

pub(super) const USAGE: &str = "usage:
  discovery_runtime_export --config <path> [--db-path <path>] (--output <path> | --scheduled) [--force] [--json] [--now <rfc3339>]";

#[derive(Debug)]
pub(super) enum Command {
    Export(ExportConfig),
}

#[derive(Debug)]
pub(super) struct ExportConfig {
    pub(super) config_path: PathBuf,
    pub(super) db_path: Option<PathBuf>,
    pub(super) output_path: Option<PathBuf>,
    pub(super) scheduled: bool,
    pub(super) force: bool,
    pub(super) json: bool,
    pub(super) now: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub(super) struct ExportOutput {
    pub(super) event: String,
    pub(super) state: String,
    pub(super) config_path: String,
    pub(super) db_path: String,
    pub(super) output_path: String,
    pub(super) archive_path: Option<String>,
    pub(super) cadence_minutes: Option<u64>,
    pub(super) retention: Option<usize>,
    pub(super) pruned_archive_paths: Vec<String>,
    pub(super) exported_at: DateTime<Utc>,
    pub(super) publication_runtime_mode: String,
    pub(super) publication_reason: String,
    pub(super) publication_truth_complete: bool,
    pub(super) publication_identity_matches: bool,
    pub(super) published_scoring_source: Option<String>,
    pub(super) expected_scoring_source: Option<String>,
    pub(super) publication_policy_fingerprint: Option<String>,
    pub(super) expected_policy_fingerprint: Option<String>,
    pub(super) fresh_under_export_gate: bool,
    pub(super) last_published_at: Option<DateTime<Utc>>,
    pub(super) last_published_window_start: Option<DateTime<Utc>>,
    pub(super) published_wallet_count: usize,
    pub(super) wallet_metrics_snapshot_rows: usize,
    pub(super) fresh_under_current_gate: bool,
    pub(super) runtime_cursor_ts: DateTime<Utc>,
    pub(super) runtime_cursor_slot: u64,
    pub(super) runtime_cursor_signature: String,
}
