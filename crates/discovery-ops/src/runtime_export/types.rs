const USAGE: &str = "usage:
  discovery_runtime_export --config <path> [--db-path <path>] (--output <path> | --scheduled) [--force] [--json] [--now <rfc3339>]";

#[derive(Debug)]
enum Command {
    Export(ExportConfig),
}

#[derive(Debug)]
struct ExportConfig {
    config_path: PathBuf,
    db_path: Option<PathBuf>,
    output_path: Option<PathBuf>,
    scheduled: bool,
    force: bool,
    json: bool,
    now: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct ExportOutput {
    event: String,
    state: String,
    config_path: String,
    db_path: String,
    output_path: String,
    archive_path: Option<String>,
    cadence_minutes: Option<u64>,
    retention: Option<usize>,
    pruned_archive_paths: Vec<String>,
    exported_at: DateTime<Utc>,
    publication_runtime_mode: String,
    publication_reason: String,
    publication_truth_complete: bool,
    fresh_under_export_gate: bool,
    last_published_at: Option<DateTime<Utc>>,
    last_published_window_start: Option<DateTime<Utc>>,
    published_wallet_count: usize,
    wallet_metrics_snapshot_rows: usize,
    fresh_under_current_gate: bool,
    runtime_cursor_ts: DateTime<Utc>,
    runtime_cursor_slot: u64,
    runtime_cursor_signature: String,
}
