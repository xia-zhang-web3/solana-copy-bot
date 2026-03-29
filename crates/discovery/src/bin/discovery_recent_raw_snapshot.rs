use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::load_from_path;
use copybot_discovery::runtime_restore_ops::{
    copy_atomic, journal_snapshot_archive_path, journal_snapshot_latest_metadata_path,
    journal_snapshot_latest_path, journal_snapshot_metadata_path, load_json, resolve_db_path,
    resolve_relative_to_config, write_json_atomic, JOURNAL_SNAPSHOT_ARCHIVE_PREFIX,
    JOURNAL_SNAPSHOT_ARCHIVE_SUFFIX,
};
use copybot_storage::{
    DiscoveryRuntimeCursor, RecentRawJournalStateRow, SqliteSnapshotOutcome, SqliteSnapshotPolicy,
    SqliteSnapshotSourceMetrics, SqliteSnapshotSummary, SqliteStore,
};
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process;
use std::time::{Duration as StdDuration, Instant, SystemTime, UNIX_EPOCH};

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
const SNAPSHOT_RESUME_ROW_BATCH_MULTIPLIER: usize = 8;
const STAGED_ARCHIVE_SNAPSHOT_SUFFIX: &str = ".archive-staged";
const STAGED_ARCHIVE_METADATA_SUFFIX: &str = ".archive-staged.json";

fn main() -> Result<()> {
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

#[derive(Debug, Clone, Serialize)]
struct SnapshotOutput {
    event: String,
    state: String,
    latest_surface_status: String,
    latest_surface_action: String,
    config_path: String,
    source_db_path: String,
    snapshot_path: String,
    metadata_path: String,
    archive_path: Option<String>,
    staged_snapshot_path: Option<String>,
    staged_metadata_path: Option<String>,
    cadence_minutes: Option<u64>,
    retention: Option<usize>,
    pruned_snapshot_paths: Vec<String>,
    cleanup_removed_paths: Vec<String>,
    archive_promoted: bool,
    archive_set_count_before: Option<usize>,
    archive_set_count_after: Option<usize>,
    staged_progress_resumed: bool,
    staged_progress_preserved_for_retry: bool,
    staged_progress_advanced: bool,
    staged_row_count_before_attempt: Option<usize>,
    staged_row_count_after_attempt: Option<usize>,
    staged_covered_through_cursor_before_attempt: Option<DiscoveryRuntimeCursor>,
    staged_covered_through_cursor_after_attempt: Option<DiscoveryRuntimeCursor>,
    source_db_bytes: u64,
    source_wal_bytes: u64,
    source_total_bytes: u64,
    source_page_size_bytes: usize,
    source_page_count: usize,
    snapshot_pages_per_step: i32,
    snapshot_pause_between_steps_ms: u64,
    snapshot_max_attempt_duration_ms: Option<u64>,
    attempt_duration_ms: u64,
    backup_step_count: usize,
    backup_retry_count: usize,
    busy_retry_count: usize,
    locked_retry_count: usize,
    backup_total_page_count: Option<usize>,
    backup_remaining_page_count: Option<usize>,
    backup_copied_page_count: Option<usize>,
    terminal_reason: Option<String>,
    retryable_reason: Option<String>,
    deferred_reason: Option<String>,
    hard_failure_reason: Option<String>,
    created_at: Option<DateTime<Utc>>,
    row_count: Option<usize>,
    covered_since: Option<DateTime<Utc>>,
    covered_through_cursor: Option<DiscoveryRuntimeCursor>,
    last_batch_completed_at: Option<DateTime<Utc>>,
    snapshot_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LatestSurfaceStatus {
    NotApplicable,
    Healthy,
    MissingLatestSnapshot,
    MissingLatestMetadata,
    MissingBoth,
    InvalidLatestMetadata,
}

impl LatestSurfaceStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::NotApplicable => "not_applicable",
            Self::Healthy => "healthy",
            Self::MissingLatestSnapshot => "missing_latest_snapshot",
            Self::MissingLatestMetadata => "missing_latest_metadata",
            Self::MissingBoth => "missing_both",
            Self::InvalidLatestMetadata => "invalid_latest_metadata",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LatestSurfaceAction {
    ExplicitOutput,
    ExplicitOutputDeferred,
    HealthySkip,
    RefreshedFromSource,
    RecreatedLatestSnapshotFromArchive,
    RewroteLatestMetadataFromArchive,
    RewroteLatestMetadataFromLatestSqlite,
    RecreatedLatestSurfaceFromSource,
    DeferredDueToAttemptBudget,
    UnchangedDueToRetryableBusy,
    UnchangedDueToAttemptBudget,
    UnchangedDueToHardFailure,
}

impl LatestSurfaceAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::ExplicitOutput => "explicit_output",
            Self::ExplicitOutputDeferred => "explicit_output_deferred",
            Self::HealthySkip => "healthy_skip",
            Self::RefreshedFromSource => "refreshed_from_source",
            Self::RecreatedLatestSnapshotFromArchive => "recreated_latest_snapshot_from_archive",
            Self::RewroteLatestMetadataFromArchive => "rewrote_latest_metadata_from_archive",
            Self::RewroteLatestMetadataFromLatestSqlite => {
                "rewrote_latest_metadata_from_latest_sqlite"
            }
            Self::RecreatedLatestSurfaceFromSource => "recreated_latest_surface_from_source",
            Self::DeferredDueToAttemptBudget => "deferred_due_to_attempt_budget",
            Self::UnchangedDueToRetryableBusy => "unchanged_due_to_retryable_busy",
            Self::UnchangedDueToAttemptBudget => "unchanged_due_to_attempt_budget",
            Self::UnchangedDueToHardFailure => "unchanged_due_to_hard_failure",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotState {
    Written,
    SkippedNotDue,
    SelfHealedLatestSurface,
    RetryableBusy,
    Deferred,
    HardFailure,
}

impl SnapshotState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Written => "written",
            Self::SkippedNotDue => "skipped_not_due",
            Self::SelfHealedLatestSurface => "self_healed_latest_surface",
            Self::RetryableBusy => "retryable_busy",
            Self::Deferred => "deferred",
            Self::HardFailure => "hard_failure",
        }
    }

    fn exit_code(self) -> i32 {
        match self {
            Self::Written | Self::SkippedNotDue | Self::SelfHealedLatestSurface => 0,
            Self::RetryableBusy | Self::Deferred => 75,
            Self::HardFailure => 1,
        }
    }
}

#[derive(Debug, Clone)]
struct LatestSurfaceAssessment {
    status: LatestSurfaceStatus,
    manifest: Option<RecentRawJournalSnapshotManifest>,
}

#[derive(Debug, Clone)]
struct SnapshotSourceStats {
    source_db_bytes: u64,
    source_wal_bytes: u64,
    source_page_size_bytes: usize,
    source_page_count: usize,
}

impl SnapshotSourceStats {
    fn source_total_bytes(&self) -> u64 {
        self.source_db_bytes.saturating_add(self.source_wal_bytes)
    }
}

#[derive(Debug, Clone)]
struct SnapshotContext {
    source_stats: SnapshotSourceStats,
    policy: SqliteSnapshotPolicy,
}

#[derive(Debug, Clone)]
struct SnapshotExecution {
    rendered_output: String,
    exit_code: i32,
}

#[derive(Debug, Clone, Default)]
struct SnapshotArchiveMaintenance {
    cleanup_removed_paths: Vec<PathBuf>,
    pruned_snapshot_paths: Vec<PathBuf>,
    archive_set_count_before: Option<usize>,
    archive_set_count_after: Option<usize>,
}

impl SnapshotArchiveMaintenance {
    fn record_pass(
        &mut self,
        archive_set_count_before: usize,
        archive_set_count_after: usize,
        cleanup_removed_paths: Vec<PathBuf>,
        pruned_snapshot_paths: Vec<PathBuf>,
    ) {
        if self.archive_set_count_before.is_none() {
            self.archive_set_count_before = Some(archive_set_count_before);
        }
        self.archive_set_count_after = Some(archive_set_count_after);
        self.cleanup_removed_paths.extend(cleanup_removed_paths);
        self.pruned_snapshot_paths.extend(pruned_snapshot_paths);
    }
}

#[derive(Debug, Clone, Default)]
struct SnapshotOutputContext {
    archive_promoted: bool,
    archive_maintenance: SnapshotArchiveMaintenance,
    staged_progress: StagedSnapshotProgress,
    attempt_duration_ms_override: Option<u64>,
    terminal_reason_override: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct StagedSnapshotProgress {
    staged_snapshot_path: Option<PathBuf>,
    staged_metadata_path: Option<PathBuf>,
    resumed_from_existing_stage: bool,
    preserved_for_retry: bool,
    advanced_during_attempt: bool,
    row_count_before_attempt: Option<usize>,
    row_count_after_attempt: Option<usize>,
    covered_through_cursor_before_attempt: Option<DiscoveryRuntimeCursor>,
    covered_through_cursor_after_attempt: Option<DiscoveryRuntimeCursor>,
    terminal_phase: Option<StagedSnapshotTerminalPhase>,
}

#[derive(Debug, Clone)]
struct StagedSnapshotAttempt {
    manifest: RecentRawJournalSnapshotManifest,
    progress: StagedSnapshotProgress,
    attempt_duration_ms: u64,
}

#[derive(Debug, Clone)]
enum StagedSnapshotAttemptResult {
    Completed(StagedSnapshotAttempt),
    Deferred(StagedSnapshotAttempt),
    HardFailure {
        manifest: Option<RecentRawJournalSnapshotManifest>,
        progress: StagedSnapshotProgress,
        attempt_duration_ms: u64,
        reason: String,
    },
}

#[derive(Debug, Clone)]
enum SnapshotWriteError {
    RetryableBusy {
        summary: SqliteSnapshotSummary,
    },
    Deferred {
        summary: SqliteSnapshotSummary,
    },
    HardFailure {
        summary: Option<SqliteSnapshotSummary>,
        reason: String,
    },
}

#[derive(Debug, Clone, Copy)]
enum StagedSnapshotTerminalPhase {
    SourceRead,
    StagedWrite,
}

impl StagedSnapshotTerminalPhase {
    fn budget_reason(self) -> &'static str {
        match self {
            Self::SourceRead => "source_read_attempt_duration_budget_exhausted",
            Self::StagedWrite => "staged_write_attempt_duration_budget_exhausted",
        }
    }
}

fn summary_reason(summary: &SqliteSnapshotSummary) -> String {
    summary
        .retry_exhausted_reason
        .map(|reason| reason.as_str().to_string())
        .unwrap_or_else(|| "busy".to_string())
}

fn deferred_summary_reason(summary: &SqliteSnapshotSummary) -> String {
    summary
        .deferred_reason
        .map(|reason| reason.as_str().to_string())
        .unwrap_or_else(|| "deferred".to_string())
}

fn staged_attempt_budget_reason(progress: &StagedSnapshotProgress) -> String {
    progress
        .terminal_phase
        .map(StagedSnapshotTerminalPhase::budget_reason)
        .unwrap_or("attempt_duration_budget_exhausted")
        .to_string()
}

fn snapshot_terminal_reason(
    state: SnapshotState,
    summary: Option<&SqliteSnapshotSummary>,
) -> Option<String> {
    match summary {
        Some(summary) => {
            if let Some(reason) = summary.deferred_reason {
                Some(reason.as_str().to_string())
            } else if let Some(reason) = summary.retry_exhausted_reason {
                Some(reason.as_str().to_string())
            } else if state == SnapshotState::Written {
                Some("written".to_string())
            } else {
                None
            }
        }
        None if state == SnapshotState::Written => Some("written".to_string()),
        _ => None,
    }
}

fn snapshot_policy_max_attempt_duration_ms(policy: &SqliteSnapshotPolicy) -> Option<u64> {
    policy
        .max_attempt_duration
        .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
}

fn state_exit_code(output: &SnapshotOutput) -> i32 {
    match output.state.as_str() {
        "written" => SnapshotState::Written.exit_code(),
        "skipped_not_due" => SnapshotState::SkippedNotDue.exit_code(),
        "self_healed_latest_surface" => SnapshotState::SelfHealedLatestSurface.exit_code(),
        "retryable_busy" => SnapshotState::RetryableBusy.exit_code(),
        "deferred" => SnapshotState::Deferred.exit_code(),
        "hard_failure" => SnapshotState::HardFailure.exit_code(),
        _ => 1,
    }
}

fn scheduled_duration_budget_contract(
    latest_surface_status: LatestSurfaceStatus,
    reason: &str,
) -> (LatestSurfaceAction, Option<String>) {
    if latest_surface_status == LatestSurfaceStatus::Healthy {
        (
            LatestSurfaceAction::DeferredDueToAttemptBudget,
            Some(format!("healthy_latest_surface_retained_after_{reason}")),
        )
    } else {
        (
            LatestSurfaceAction::UnchangedDueToAttemptBudget,
            Some(reason.to_string()),
        )
    }
}

fn snapshot_source_wal_path(source_db_path: &Path) -> PathBuf {
    let mut wal_path = source_db_path.as_os_str().to_os_string();
    wal_path.push("-wal");
    PathBuf::from(wal_path)
}

fn snapshot_source_stats(
    source_db_path: &Path,
    source_store: &SqliteStore,
) -> Result<SnapshotSourceStats> {
    let source_metrics: SqliteSnapshotSourceMetrics = source_store.snapshot_source_metrics()?;
    let source_db_bytes = fs::metadata(source_db_path)
        .with_context(|| format!("failed stat {}", source_db_path.display()))?
        .len();
    let wal_path = snapshot_source_wal_path(source_db_path);
    let source_wal_bytes = match fs::metadata(&wal_path) {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => {
            return Err(error).with_context(|| format!("failed stat {}", wal_path.display()));
        }
    };
    Ok(SnapshotSourceStats {
        source_db_bytes,
        source_wal_bytes,
        source_page_size_bytes: source_metrics.page_size_bytes,
        source_page_count: source_metrics.page_count,
    })
}

fn adaptive_snapshot_policy(source_stats: &SnapshotSourceStats) -> SqliteSnapshotPolicy {
    let mut policy = SqliteSnapshotPolicy::default();
    let total_bytes = source_stats.source_total_bytes().max(
        (source_stats.source_page_size_bytes as u64)
            .saturating_mul(source_stats.source_page_count as u64),
    );
    let (target_steps, pause_ms, max_attempt_duration_ms) =
        if total_bytes >= SNAPSHOT_HUGE_SOURCE_TOTAL_BYTES {
            (
                SNAPSHOT_HUGE_TARGET_STEPS,
                SNAPSHOT_LARGE_PAUSE_BETWEEN_STEPS_MS,
                SNAPSHOT_HUGE_MAX_ATTEMPT_DURATION_MS,
            )
        } else if total_bytes >= SNAPSHOT_LARGE_SOURCE_TOTAL_BYTES {
            (
                SNAPSHOT_LARGE_TARGET_STEPS,
                SNAPSHOT_LARGE_PAUSE_BETWEEN_STEPS_MS,
                SNAPSHOT_LARGE_MAX_ATTEMPT_DURATION_MS,
            )
        } else if total_bytes >= SNAPSHOT_SMALL_SOURCE_TOTAL_BYTES {
            (
                SNAPSHOT_MEDIUM_TARGET_STEPS,
                SNAPSHOT_MEDIUM_PAUSE_BETWEEN_STEPS_MS,
                SNAPSHOT_MEDIUM_MAX_ATTEMPT_DURATION_MS,
            )
        } else {
            (
                SNAPSHOT_SMALL_TARGET_STEPS,
                SNAPSHOT_SMALL_PAUSE_BETWEEN_STEPS_MS,
                SNAPSHOT_SMALL_MAX_ATTEMPT_DURATION_MS,
            )
        };
    let page_count = source_stats.source_page_count.max(1);
    let pages_per_step = page_count
        .div_ceil(target_steps)
        .clamp(SNAPSHOT_MIN_PAGES_PER_STEP, SNAPSHOT_MAX_PAGES_PER_STEP)
        as i32;
    policy.pages_per_step = pages_per_step;
    policy.pause_between_steps = StdDuration::from_millis(pause_ms);
    policy.max_attempt_duration = Some(StdDuration::from_millis(max_attempt_duration_ms));
    policy
}

fn snapshot_context(source_db_path: &Path, source_store: &SqliteStore) -> Result<SnapshotContext> {
    let source_stats = snapshot_source_stats(source_db_path, source_store)?;
    Ok(SnapshotContext {
        policy: adaptive_snapshot_policy(&source_stats),
        source_stats,
    })
}

fn discovery_runtime_cursor_cmp(
    left: &DiscoveryRuntimeCursor,
    right: &DiscoveryRuntimeCursor,
) -> std::cmp::Ordering {
    left.ts_utc
        .cmp(&right.ts_utc)
        .then_with(|| left.slot.cmp(&right.slot))
        .then_with(|| left.signature.cmp(&right.signature))
}

fn snapshot_resume_batch_size(policy: &SqliteSnapshotPolicy) -> usize {
    (policy.pages_per_step.max(1) as usize)
        .saturating_mul(SNAPSHOT_RESUME_ROW_BATCH_MULTIPLIER)
        .max(1)
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut config_path: Option<PathBuf> = None;
    let mut journal_db_path: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut scheduled = false;
    let mut force = false;
    let mut json = false;
    let mut now: Option<DateTime<Utc>> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--journal-db-path" => {
                journal_db_path = Some(PathBuf::from(parse_string_arg(
                    "--journal-db-path",
                    args.next(),
                )?))
            }
            "--output" => {
                output_path = Some(PathBuf::from(parse_string_arg("--output", args.next())?))
            }
            "--scheduled" => scheduled = true,
            "--force" => force = true,
            "--json" => json = true,
            "--now" => now = Some(parse_ts_arg("--now", args.next())?),
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    if scheduled == output_path.is_some() {
        bail!("exactly one of --output or --scheduled must be provided");
    }

    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        journal_db_path,
        output_path,
        scheduled,
        force,
        json,
        now: now.unwrap_or_else(Utc::now),
    }))
}

fn parse_ts_arg(flag: &str, value: Option<String>) -> Result<DateTime<Utc>> {
    let raw = parse_string_arg(flag, value)?;
    DateTime::parse_from_rfc3339(&raw)
        .map(|ts| ts.with_timezone(&Utc))
        .with_context(|| format!("invalid {flag} rfc3339 timestamp: {raw}"))
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    let raw = value.ok_or_else(|| anyhow!("missing value for {flag}"))?;
    let trimmed = raw.trim().to_string();
    if trimmed.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(trimmed)
}

fn run(config: Config) -> Result<SnapshotExecution> {
    run_with_snapshot_policy_override(config, None)
}

fn run_with_snapshot_policy_override(
    config: Config,
    snapshot_policy_override: Option<SqliteSnapshotPolicy>,
) -> Result<SnapshotExecution> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let source_db_path = resolve_db_path(
        &config.config_path,
        config.journal_db_path.as_deref(),
        &loaded_config.recent_raw_journal.path,
    );
    if !source_db_path.exists() {
        bail!(
            "recent raw journal db does not exist: {}",
            source_db_path.display()
        );
    }
    let source_store = SqliteStore::open_read_only(&source_db_path).with_context(|| {
        format!(
            "failed opening recent raw journal db {}",
            source_db_path.display()
        )
    })?;
    let mut snapshot_context = snapshot_context(&source_db_path, &source_store)?;
    if let Some(snapshot_policy_override) = snapshot_policy_override {
        snapshot_context.policy = snapshot_policy_override;
    }

    let output = if config.scheduled {
        run_scheduled(
            &config,
            &source_db_path,
            &source_store,
            &loaded_config.runtime_restore_ops.journal_snapshot_dir,
            loaded_config
                .runtime_restore_ops
                .journal_snapshot_cadence_minutes,
            loaded_config.runtime_restore_ops.journal_snapshot_retention,
            &snapshot_context,
        )?
    } else {
        let snapshot_path = resolve_relative_to_config(
            &config.config_path,
            config
                .output_path
                .as_deref()
                .expect("validated explicit output path"),
        );
        let metadata_path = journal_snapshot_metadata_path(&snapshot_path);
        match write_snapshot_with_policy(
            &source_db_path,
            &source_store,
            &snapshot_path,
            config.now,
            &snapshot_context.policy,
        ) {
            Ok((manifest, summary)) => {
                match write_json_atomic(&metadata_path, &manifest)
                    .with_context(|| format!("failed writing {}", metadata_path.display()))
                {
                    Ok(()) => render_output(
                        SnapshotState::Written,
                        LatestSurfaceStatus::NotApplicable,
                        LatestSurfaceAction::ExplicitOutput,
                        &snapshot_context,
                        &config.config_path,
                        &source_db_path,
                        &snapshot_path,
                        &metadata_path,
                        None,
                        None,
                        None,
                        Some(&manifest),
                        Some(&summary),
                        None,
                        None,
                        None,
                        SnapshotOutputContext::default(),
                    ),
                    Err(error) => render_output(
                        SnapshotState::HardFailure,
                        LatestSurfaceStatus::NotApplicable,
                        LatestSurfaceAction::UnchangedDueToHardFailure,
                        &snapshot_context,
                        &config.config_path,
                        &source_db_path,
                        &snapshot_path,
                        &metadata_path,
                        None,
                        None,
                        None,
                        Some(&manifest),
                        Some(&summary),
                        None,
                        None,
                        Some(error.to_string()),
                        SnapshotOutputContext::default(),
                    ),
                }
            }
            Err(SnapshotWriteError::RetryableBusy { summary }) => render_output(
                SnapshotState::RetryableBusy,
                LatestSurfaceStatus::NotApplicable,
                LatestSurfaceAction::UnchangedDueToRetryableBusy,
                &snapshot_context,
                &config.config_path,
                &source_db_path,
                &snapshot_path,
                &metadata_path,
                None,
                None,
                None,
                None,
                Some(&summary),
                Some(summary_reason(&summary)),
                None,
                None,
                SnapshotOutputContext::default(),
            ),
            Err(SnapshotWriteError::Deferred { summary }) => render_output(
                SnapshotState::Deferred,
                LatestSurfaceStatus::NotApplicable,
                LatestSurfaceAction::ExplicitOutputDeferred,
                &snapshot_context,
                &config.config_path,
                &source_db_path,
                &snapshot_path,
                &metadata_path,
                None,
                None,
                None,
                None,
                Some(&summary),
                None,
                Some(deferred_summary_reason(&summary)),
                None,
                SnapshotOutputContext::default(),
            ),
            Err(SnapshotWriteError::HardFailure { summary, reason }) => render_output(
                SnapshotState::HardFailure,
                LatestSurfaceStatus::NotApplicable,
                LatestSurfaceAction::UnchangedDueToHardFailure,
                &snapshot_context,
                &config.config_path,
                &source_db_path,
                &snapshot_path,
                &metadata_path,
                None,
                None,
                None,
                None,
                summary.as_ref(),
                None,
                None,
                Some(reason),
                SnapshotOutputContext::default(),
            ),
        }
    };

    let rendered_output = if config.json {
        serde_json::to_string_pretty(&output).context("failed serializing journal snapshot json")?
    } else {
        render_human(&output)
    };
    Ok(SnapshotExecution {
        rendered_output,
        exit_code: state_exit_code(&output),
    })
}

fn run_scheduled(
    config: &Config,
    source_db_path: &Path,
    source_store: &SqliteStore,
    configured_snapshot_dir: &str,
    cadence_minutes: u64,
    retention: usize,
    snapshot_context: &SnapshotContext,
) -> Result<SnapshotOutput> {
    let snapshot_dir =
        resolve_relative_to_config(&config.config_path, Path::new(configured_snapshot_dir));
    let staged_paths = vec![
        staged_snapshot_archive_path(&snapshot_dir),
        staged_snapshot_metadata_path(&snapshot_dir),
    ];
    let latest_snapshot_path = journal_snapshot_latest_path(&snapshot_dir);
    let latest_metadata_path = journal_snapshot_latest_metadata_path(&snapshot_dir);
    let cadence = Duration::minutes(cadence_minutes.max(1) as i64);
    let latest_surface = assess_latest_surface(&latest_snapshot_path, &latest_metadata_path)
        .with_context(|| {
            format!(
                "failed assessing latest snapshot surface in {}",
                snapshot_dir.display()
            )
        })?;
    let latest_reference_manifest = reference_manifest_for_cadence(
        source_db_path,
        &snapshot_dir,
        &latest_snapshot_path,
        &latest_surface,
    )?;
    let latest_is_due = latest_reference_manifest
        .as_ref()
        .map(|manifest| config.now.signed_duration_since(manifest.created_at) >= cadence)
        .unwrap_or(true);

    if !config.force && latest_surface.status == LatestSurfaceStatus::Healthy && !latest_is_due {
        let archive_maintenance =
            enforce_snapshot_archive_retention(&snapshot_dir, retention, &staged_paths)?;
        let manifest = latest_surface
            .manifest
            .as_ref()
            .expect("healthy latest surface includes manifest");
        return Ok(render_output(
            SnapshotState::SkippedNotDue,
            latest_surface.status,
            LatestSurfaceAction::HealthySkip,
            snapshot_context,
            &config.config_path,
            source_db_path,
            &latest_snapshot_path,
            &latest_metadata_path,
            None,
            Some(cadence_minutes),
            Some(retention),
            Some(manifest),
            None,
            None,
            None,
            None,
            SnapshotOutputContext {
                archive_promoted: false,
                archive_maintenance,
                ..SnapshotOutputContext::default()
            },
        ));
    }

    if !config.force && latest_surface.status != LatestSurfaceStatus::Healthy && !latest_is_due {
        if let Some((manifest, action)) = try_self_heal_latest_surface(
            source_db_path,
            &snapshot_dir,
            &latest_snapshot_path,
            &latest_metadata_path,
            latest_surface.clone(),
        )? {
            let archive_maintenance =
                enforce_snapshot_archive_retention(&snapshot_dir, retention, &staged_paths)?;
            return Ok(render_output(
                SnapshotState::SelfHealedLatestSurface,
                latest_surface.status,
                action,
                snapshot_context,
                &config.config_path,
                source_db_path,
                &latest_snapshot_path,
                &latest_metadata_path,
                None,
                Some(cadence_minutes),
                Some(retention),
                Some(&manifest),
                None,
                None,
                None,
                None,
                SnapshotOutputContext {
                    archive_promoted: false,
                    archive_maintenance,
                    ..SnapshotOutputContext::default()
                },
            ));
        }
    }

    let archive_maintenance =
        enforce_snapshot_archive_retention(&snapshot_dir, retention, &staged_paths)?;

    let action = if latest_surface.status == LatestSurfaceStatus::Healthy {
        LatestSurfaceAction::RefreshedFromSource
    } else {
        LatestSurfaceAction::RecreatedLatestSurfaceFromSource
    };
    write_fresh_scheduled_snapshot(
        &config.config_path,
        source_db_path,
        source_store,
        &latest_snapshot_path,
        &latest_metadata_path,
        config.now,
        cadence_minutes,
        retention,
        latest_surface.status,
        action,
        &snapshot_dir,
        snapshot_context,
        latest_surface.manifest.as_ref(),
        archive_maintenance,
    )
}

fn assess_latest_surface(
    latest_snapshot_path: &Path,
    latest_metadata_path: &Path,
) -> Result<LatestSurfaceAssessment> {
    let latest_snapshot_exists = latest_snapshot_path.exists();
    let latest_metadata_exists = latest_metadata_path.exists();
    match (latest_metadata_exists, latest_snapshot_exists) {
        (true, true) => match load_json::<RecentRawJournalSnapshotManifest>(latest_metadata_path) {
            Ok(manifest) => Ok(LatestSurfaceAssessment {
                status: LatestSurfaceStatus::Healthy,
                manifest: Some(manifest),
            }),
            Err(_) => Ok(LatestSurfaceAssessment {
                status: LatestSurfaceStatus::InvalidLatestMetadata,
                manifest: None,
            }),
        },
        (true, false) => {
            match load_json::<RecentRawJournalSnapshotManifest>(latest_metadata_path) {
                Ok(manifest) => Ok(LatestSurfaceAssessment {
                    status: LatestSurfaceStatus::MissingLatestSnapshot,
                    manifest: Some(manifest),
                }),
                Err(_) => Ok(LatestSurfaceAssessment {
                    status: LatestSurfaceStatus::InvalidLatestMetadata,
                    manifest: None,
                }),
            }
        }
        (false, true) => Ok(LatestSurfaceAssessment {
            status: LatestSurfaceStatus::MissingLatestMetadata,
            manifest: None,
        }),
        (false, false) => Ok(LatestSurfaceAssessment {
            status: LatestSurfaceStatus::MissingBoth,
            manifest: None,
        }),
    }
}

fn try_self_heal_latest_surface(
    source_db_path: &Path,
    snapshot_dir: &Path,
    latest_snapshot_path: &Path,
    latest_metadata_path: &Path,
    latest_surface: LatestSurfaceAssessment,
) -> Result<Option<(RecentRawJournalSnapshotManifest, LatestSurfaceAction)>> {
    match latest_surface.status {
        LatestSurfaceStatus::NotApplicable => Ok(None),
        LatestSurfaceStatus::MissingLatestSnapshot => {
            if let Some(archive_path) =
                archive_candidate(snapshot_dir, latest_surface.manifest.as_ref())
            {
                copy_atomic(&archive_path, latest_snapshot_path).with_context(|| {
                    format!("failed restoring {}", latest_snapshot_path.display())
                })?;
                let manifest = manifest_for_existing_snapshot(source_db_path, &archive_path)?;
                write_json_atomic(latest_metadata_path, &manifest).with_context(|| {
                    format!("failed writing {}", latest_metadata_path.display())
                })?;
                Ok(Some((
                    manifest,
                    LatestSurfaceAction::RecreatedLatestSnapshotFromArchive,
                )))
            } else {
                Ok(None)
            }
        }
        LatestSurfaceStatus::MissingLatestMetadata | LatestSurfaceStatus::InvalidLatestMetadata => {
            if !latest_snapshot_path.exists() {
                return Ok(None);
            }
            if let Some(archive_path) =
                archive_candidate(snapshot_dir, latest_surface.manifest.as_ref())
            {
                let manifest = manifest_for_existing_snapshot(source_db_path, &archive_path)?;
                write_json_atomic(latest_metadata_path, &manifest).with_context(|| {
                    format!("failed writing {}", latest_metadata_path.display())
                })?;
                Ok(Some((
                    manifest,
                    LatestSurfaceAction::RewroteLatestMetadataFromArchive,
                )))
            } else {
                let manifest =
                    manifest_for_existing_snapshot(source_db_path, latest_snapshot_path)?;
                write_json_atomic(latest_metadata_path, &manifest).with_context(|| {
                    format!("failed writing {}", latest_metadata_path.display())
                })?;
                Ok(Some((
                    manifest,
                    LatestSurfaceAction::RewroteLatestMetadataFromLatestSqlite,
                )))
            }
        }
        LatestSurfaceStatus::Healthy | LatestSurfaceStatus::MissingBoth => Ok(None),
    }
}

fn reference_manifest_for_cadence(
    source_db_path: &Path,
    snapshot_dir: &Path,
    latest_snapshot_path: &Path,
    latest_surface: &LatestSurfaceAssessment,
) -> Result<Option<RecentRawJournalSnapshotManifest>> {
    match latest_surface.status {
        LatestSurfaceStatus::NotApplicable => Ok(None),
        LatestSurfaceStatus::Healthy | LatestSurfaceStatus::MissingLatestSnapshot => {
            Ok(latest_surface.manifest.clone())
        }
        LatestSurfaceStatus::MissingLatestMetadata | LatestSurfaceStatus::InvalidLatestMetadata => {
            if !latest_snapshot_path.exists() {
                return Ok(None);
            }
            if let Some(archive_path) =
                archive_candidate(snapshot_dir, latest_surface.manifest.as_ref())
            {
                return manifest_for_existing_snapshot(source_db_path, &archive_path).map(Some);
            }
            manifest_for_existing_snapshot(source_db_path, latest_snapshot_path).map(Some)
        }
        LatestSurfaceStatus::MissingBoth => Ok(None),
    }
}

fn write_fresh_scheduled_snapshot(
    config_path: &Path,
    source_db_path: &Path,
    source_store: &SqliteStore,
    latest_snapshot_path: &Path,
    latest_metadata_path: &Path,
    now: DateTime<Utc>,
    cadence_minutes: u64,
    retention: usize,
    latest_surface_status: LatestSurfaceStatus,
    action: LatestSurfaceAction,
    snapshot_dir: &Path,
    snapshot_context: &SnapshotContext,
    latest_manifest: Option<&RecentRawJournalSnapshotManifest>,
    mut archive_maintenance: SnapshotArchiveMaintenance,
) -> Result<SnapshotOutput> {
    let archive_path = journal_snapshot_archive_path(snapshot_dir, now);
    let archive_metadata_path = journal_snapshot_metadata_path(&archive_path);
    let staged_archive_path = staged_snapshot_archive_path(snapshot_dir);
    let staged_archive_metadata_path = staged_snapshot_metadata_path(snapshot_dir);
    let staged_preserve_paths = vec![
        staged_archive_path.clone(),
        staged_archive_metadata_path.clone(),
    ];
    let staged_attempt = match resume_staged_snapshot_with_policy(
        source_db_path,
        source_store,
        snapshot_dir,
        now,
        &snapshot_context.policy,
    ) {
        StagedSnapshotAttemptResult::Completed(attempt) => attempt,
        StagedSnapshotAttemptResult::Deferred(attempt) => {
            let reason = staged_attempt_budget_reason(&attempt.progress);
            let (latest_action, deferred_reason) =
                scheduled_duration_budget_contract(latest_surface_status, &reason);
            return Ok(render_output(
                SnapshotState::Deferred,
                latest_surface_status,
                latest_action,
                snapshot_context,
                config_path,
                source_db_path,
                latest_snapshot_path,
                latest_metadata_path,
                None,
                Some(cadence_minutes),
                Some(retention),
                latest_manifest,
                None,
                None,
                deferred_reason,
                None,
                SnapshotOutputContext {
                    archive_promoted: false,
                    archive_maintenance,
                    staged_progress: attempt.progress,
                    attempt_duration_ms_override: Some(attempt.attempt_duration_ms),
                    terminal_reason_override: Some(reason),
                },
            ));
        }
        StagedSnapshotAttemptResult::HardFailure {
            manifest,
            progress,
            attempt_duration_ms,
            reason,
        } => {
            return Ok(render_output(
                SnapshotState::HardFailure,
                latest_surface_status,
                LatestSurfaceAction::UnchangedDueToHardFailure,
                snapshot_context,
                config_path,
                source_db_path,
                latest_snapshot_path,
                latest_metadata_path,
                None,
                Some(cadence_minutes),
                Some(retention),
                manifest.as_ref().or(latest_manifest),
                None,
                None,
                None,
                Some(reason),
                SnapshotOutputContext {
                    archive_promoted: false,
                    archive_maintenance,
                    staged_progress: progress,
                    attempt_duration_ms_override: Some(attempt_duration_ms),
                    terminal_reason_override: None,
                },
            ));
        }
    };

    match enforce_snapshot_archive_retention(
        snapshot_dir,
        retention.saturating_sub(1),
        &staged_preserve_paths,
    ) {
        Ok(maintenance) => {
            archive_maintenance.record_pass(
                maintenance.archive_set_count_before.unwrap_or_default(),
                maintenance.archive_set_count_after.unwrap_or_default(),
                maintenance.cleanup_removed_paths,
                maintenance.pruned_snapshot_paths,
            );
        }
        Err(error) => {
            return Ok(render_output(
                SnapshotState::HardFailure,
                latest_surface_status,
                LatestSurfaceAction::UnchangedDueToHardFailure,
                snapshot_context,
                config_path,
                source_db_path,
                latest_snapshot_path,
                latest_metadata_path,
                Some(&archive_path),
                Some(cadence_minutes),
                Some(retention),
                Some(&staged_attempt.manifest),
                None,
                None,
                None,
                Some(error.to_string()),
                SnapshotOutputContext {
                    archive_promoted: false,
                    archive_maintenance,
                    staged_progress: staged_attempt.progress.clone(),
                    attempt_duration_ms_override: Some(staged_attempt.attempt_duration_ms),
                    terminal_reason_override: None,
                },
            ));
        }
    }

    if let Err(error) = link_or_copy_atomic(&staged_archive_path, latest_snapshot_path)
        .with_context(|| format!("failed updating {}", latest_snapshot_path.display()))
    {
        return Ok(render_output(
            SnapshotState::HardFailure,
            latest_surface_status,
            LatestSurfaceAction::UnchangedDueToHardFailure,
            snapshot_context,
            config_path,
            source_db_path,
            latest_snapshot_path,
            latest_metadata_path,
            Some(&archive_path),
            Some(cadence_minutes),
            Some(retention),
            Some(&staged_attempt.manifest),
            None,
            None,
            None,
            Some(error.to_string()),
            SnapshotOutputContext {
                archive_promoted: false,
                archive_maintenance,
                staged_progress: staged_attempt.progress.clone(),
                attempt_duration_ms_override: Some(staged_attempt.attempt_duration_ms),
                terminal_reason_override: None,
            },
        ));
    }
    if let Err(error) = invoke_post_snapshot_publish_hook(latest_snapshot_path) {
        return Ok(render_output(
            SnapshotState::HardFailure,
            latest_surface_status,
            LatestSurfaceAction::UnchangedDueToHardFailure,
            snapshot_context,
            config_path,
            source_db_path,
            latest_snapshot_path,
            latest_metadata_path,
            Some(&archive_path),
            Some(cadence_minutes),
            Some(retention),
            Some(&staged_attempt.manifest),
            None,
            None,
            None,
            Some(format!(
                "failed running post-snapshot publish hook for {}: {error}",
                latest_snapshot_path.display()
            )),
            SnapshotOutputContext {
                archive_promoted: false,
                archive_maintenance,
                staged_progress: staged_attempt.progress.clone(),
                attempt_duration_ms_override: Some(staged_attempt.attempt_duration_ms),
                terminal_reason_override: None,
            },
        ));
    }

    let latest_manifest = match manifest_for_snapshot(source_db_path, latest_snapshot_path, now) {
        Ok(manifest) => manifest,
        Err(error) => {
            return Ok(render_output(
                SnapshotState::HardFailure,
                latest_surface_status,
                LatestSurfaceAction::UnchangedDueToHardFailure,
                snapshot_context,
                config_path,
                source_db_path,
                latest_snapshot_path,
                latest_metadata_path,
                Some(&archive_path),
                Some(cadence_minutes),
                Some(retention),
                Some(&staged_attempt.manifest),
                None,
                None,
                None,
                Some(format!(
                    "failed building latest recent_raw snapshot manifest from {}: {error}",
                    latest_snapshot_path.display()
                )),
                SnapshotOutputContext {
                    archive_promoted: false,
                    archive_maintenance,
                    staged_progress: staged_attempt.progress.clone(),
                    attempt_duration_ms_override: Some(staged_attempt.attempt_duration_ms),
                    terminal_reason_override: None,
                },
            ));
        }
    };

    if let Err(error) = write_json_atomic(latest_metadata_path, &latest_manifest)
        .with_context(|| format!("failed writing {}", latest_metadata_path.display()))
    {
        return Ok(render_output(
            SnapshotState::HardFailure,
            latest_surface_status,
            LatestSurfaceAction::UnchangedDueToHardFailure,
            snapshot_context,
            config_path,
            source_db_path,
            latest_snapshot_path,
            latest_metadata_path,
            Some(&archive_path),
            Some(cadence_minutes),
            Some(retention),
            Some(&latest_manifest),
            None,
            None,
            None,
            Some(error.to_string()),
            SnapshotOutputContext {
                archive_promoted: false,
                archive_maintenance,
                staged_progress: staged_attempt.progress.clone(),
                attempt_duration_ms_override: Some(staged_attempt.attempt_duration_ms),
                terminal_reason_override: None,
            },
        ));
    }

    if let Err(error) = invoke_pre_archive_promotion_hook(&staged_archive_path) {
        return Ok(render_output(
            SnapshotState::HardFailure,
            latest_surface_status,
            LatestSurfaceAction::UnchangedDueToHardFailure,
            snapshot_context,
            config_path,
            source_db_path,
            latest_snapshot_path,
            latest_metadata_path,
            Some(&archive_path),
            Some(cadence_minutes),
            Some(retention),
            Some(&latest_manifest),
            None,
            None,
            None,
            Some(format!(
                "failed running pre-archive promotion hook for {}: {error}",
                staged_archive_path.display()
            )),
            SnapshotOutputContext {
                archive_promoted: false,
                archive_maintenance,
                staged_progress: staged_attempt.progress.clone(),
                attempt_duration_ms_override: Some(staged_attempt.attempt_duration_ms),
                terminal_reason_override: None,
            },
        ));
    }

    if let Err(error) = link_or_copy_atomic(&staged_archive_path, &archive_path)
        .with_context(|| format!("failed promoting {}", archive_path.display()))
    {
        return Ok(render_output(
            SnapshotState::HardFailure,
            latest_surface_status,
            LatestSurfaceAction::UnchangedDueToHardFailure,
            snapshot_context,
            config_path,
            source_db_path,
            latest_snapshot_path,
            latest_metadata_path,
            Some(&archive_path),
            Some(cadence_minutes),
            Some(retention),
            Some(&latest_manifest),
            None,
            None,
            None,
            Some(error.to_string()),
            SnapshotOutputContext {
                archive_promoted: false,
                archive_maintenance,
                staged_progress: staged_attempt.progress.clone(),
                attempt_duration_ms_override: Some(staged_attempt.attempt_duration_ms),
                terminal_reason_override: None,
            },
        ));
    }

    let archive_manifest = match manifest_for_snapshot(source_db_path, &archive_path, now) {
        Ok(manifest) => manifest,
        Err(error) => {
            return Ok(render_output(
                SnapshotState::HardFailure,
                latest_surface_status,
                LatestSurfaceAction::UnchangedDueToHardFailure,
                snapshot_context,
                config_path,
                source_db_path,
                latest_snapshot_path,
                latest_metadata_path,
                Some(&archive_path),
                Some(cadence_minutes),
                Some(retention),
                Some(&latest_manifest),
                None,
                None,
                None,
                Some(format!(
                    "failed building archive recent_raw snapshot manifest from {}: {error}",
                    archive_path.display()
                )),
                SnapshotOutputContext {
                    archive_promoted: false,
                    archive_maintenance,
                    staged_progress: staged_attempt.progress.clone(),
                    attempt_duration_ms_override: Some(staged_attempt.attempt_duration_ms),
                    terminal_reason_override: None,
                },
            ));
        }
    };
    if let Err(error) = write_json_atomic(&archive_metadata_path, &archive_manifest)
        .with_context(|| format!("failed writing {}", archive_metadata_path.display()))
    {
        return Ok(render_output(
            SnapshotState::HardFailure,
            latest_surface_status,
            LatestSurfaceAction::UnchangedDueToHardFailure,
            snapshot_context,
            config_path,
            source_db_path,
            latest_snapshot_path,
            latest_metadata_path,
            Some(&archive_path),
            Some(cadence_minutes),
            Some(retention),
            Some(&archive_manifest),
            None,
            None,
            None,
            Some(error.to_string()),
            SnapshotOutputContext {
                archive_promoted: false,
                archive_maintenance,
                staged_progress: staged_attempt.progress.clone(),
                attempt_duration_ms_override: Some(staged_attempt.attempt_duration_ms),
                terminal_reason_override: None,
            },
        ));
    }

    archive_maintenance
        .cleanup_removed_paths
        .extend(remove_staged_snapshot_artifacts(
            &staged_archive_path,
            &staged_archive_metadata_path,
        )?);
    archive_maintenance.archive_set_count_after =
        Some(list_archive_snapshot_paths(snapshot_dir)?.len());

    Ok(render_output(
        SnapshotState::Written,
        latest_surface_status,
        action,
        snapshot_context,
        config_path,
        source_db_path,
        latest_snapshot_path,
        latest_metadata_path,
        Some(&archive_path),
        Some(cadence_minutes),
        Some(retention),
        Some(&archive_manifest),
        None,
        None,
        None,
        None,
        SnapshotOutputContext {
            archive_promoted: true,
            archive_maintenance,
            staged_progress: staged_attempt.progress,
            attempt_duration_ms_override: Some(staged_attempt.attempt_duration_ms),
            terminal_reason_override: Some("written".to_string()),
        },
    ))
}

fn archive_candidate(
    snapshot_dir: &Path,
    manifest: Option<&RecentRawJournalSnapshotManifest>,
) -> Option<PathBuf> {
    if let Some(manifest) = manifest {
        let candidate = PathBuf::from(&manifest.snapshot_path);
        if candidate.exists() && candidate.is_file() {
            return Some(candidate);
        }
    }
    newest_snapshot_archive(snapshot_dir)
}

fn newest_snapshot_archive(snapshot_dir: &Path) -> Option<PathBuf> {
    let mut archives = fs::read_dir(snapshot_dir)
        .ok()?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            path.is_file()
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name.starts_with(JOURNAL_SNAPSHOT_ARCHIVE_PREFIX)
                            && name.ends_with(JOURNAL_SNAPSHOT_ARCHIVE_SUFFIX)
                    })
        })
        .collect::<Vec<_>>();
    archives.sort_by(|left, right| right.file_name().cmp(&left.file_name()));
    archives.into_iter().next()
}

fn staged_snapshot_archive_path(snapshot_dir: &Path) -> PathBuf {
    snapshot_dir.join(format!(
        ".{JOURNAL_SNAPSHOT_ARCHIVE_PREFIX}staged{JOURNAL_SNAPSHOT_ARCHIVE_SUFFIX}{STAGED_ARCHIVE_SNAPSHOT_SUFFIX}"
    ))
}

fn staged_snapshot_metadata_path(snapshot_dir: &Path) -> PathBuf {
    snapshot_dir.join(format!(
        ".{JOURNAL_SNAPSHOT_ARCHIVE_PREFIX}staged{JOURNAL_SNAPSHOT_ARCHIVE_SUFFIX}{STAGED_ARCHIVE_METADATA_SUFFIX}"
    ))
}

fn sqlite_snapshot_wal_path(snapshot_path: &Path) -> PathBuf {
    let mut wal_path = snapshot_path.as_os_str().to_os_string();
    wal_path.push("-wal");
    PathBuf::from(wal_path)
}

fn sqlite_snapshot_shm_path(snapshot_path: &Path) -> PathBuf {
    let mut shm_path = snapshot_path.as_os_str().to_os_string();
    shm_path.push("-shm");
    PathBuf::from(shm_path)
}

fn remove_file_if_exists(path: &Path) -> Result<bool> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).with_context(|| format!("failed removing {}", path.display())),
    }
}

fn remove_snapshot_set(snapshot_path: &Path) -> Result<Vec<PathBuf>> {
    let mut removed_paths = Vec::new();
    for path in [
        snapshot_path.to_path_buf(),
        journal_snapshot_metadata_path(snapshot_path),
        sqlite_snapshot_wal_path(snapshot_path),
        sqlite_snapshot_shm_path(snapshot_path),
    ] {
        if remove_file_if_exists(&path)? {
            removed_paths.push(path);
        }
    }
    Ok(removed_paths)
}

fn remove_staged_snapshot_artifacts(
    staged_snapshot_path: &Path,
    staged_metadata_path: &Path,
) -> Result<Vec<PathBuf>> {
    let mut removed_paths = Vec::new();
    for path in [
        staged_snapshot_path.to_path_buf(),
        staged_metadata_path.to_path_buf(),
        sqlite_snapshot_wal_path(staged_snapshot_path),
        sqlite_snapshot_shm_path(staged_snapshot_path),
    ] {
        if remove_file_if_exists(&path)? {
            removed_paths.push(path);
        }
    }
    Ok(removed_paths)
}

fn load_existing_staged_manifest(
    source_db_path: &Path,
    staged_snapshot_path: &Path,
    staged_metadata_path: &Path,
) -> Result<Option<RecentRawJournalSnapshotManifest>> {
    if !staged_snapshot_path.exists() {
        let _ = remove_file_if_exists(staged_metadata_path)?;
        return Ok(None);
    }
    let created_at = match load_json::<RecentRawJournalSnapshotManifest>(staged_metadata_path) {
        Ok(manifest) => manifest.created_at,
        Err(_) => infer_created_at(staged_snapshot_path)?,
    };
    manifest_for_snapshot(source_db_path, staged_snapshot_path, created_at).map(Some)
}

fn source_window_outran_staged_progress(
    source_state: &RecentRawJournalStateRow,
    staged_manifest: &RecentRawJournalSnapshotManifest,
) -> bool {
    if source_state.row_count == 0 {
        return staged_manifest.row_count > 0;
    }
    if let (Some(source_since), Some(staged_since)) =
        (source_state.covered_since, staged_manifest.covered_since)
    {
        if source_since > staged_since {
            return true;
        }
    }
    if let (Some(source_cursor), Some(staged_cursor)) = (
        source_state.covered_through_cursor.as_ref(),
        staged_manifest.covered_through_cursor.as_ref(),
    ) {
        return discovery_runtime_cursor_cmp(source_cursor, staged_cursor)
            == std::cmp::Ordering::Greater;
    }
    false
}

fn reference_surface_outran_staged_progress(
    reference_manifest: &RecentRawJournalSnapshotManifest,
    staged_manifest: &RecentRawJournalSnapshotManifest,
) -> bool {
    if reference_manifest.row_count == 0 {
        return staged_manifest.row_count > 0;
    }
    if reference_manifest.row_count > staged_manifest.row_count {
        return true;
    }
    if let (Some(reference_since), Some(staged_since)) = (
        reference_manifest.covered_since,
        staged_manifest.covered_since,
    ) {
        if reference_since > staged_since {
            return true;
        }
    }
    if let (Some(reference_cursor), Some(staged_cursor)) = (
        reference_manifest.covered_through_cursor.as_ref(),
        staged_manifest.covered_through_cursor.as_ref(),
    ) {
        return discovery_runtime_cursor_cmp(reference_cursor, staged_cursor)
            == std::cmp::Ordering::Greater;
    }
    false
}

fn staged_manifest_for_state(
    source_db_path: &Path,
    staged_snapshot_path: &Path,
    created_at: DateTime<Utc>,
    state: &RecentRawJournalStateRow,
) -> Result<RecentRawJournalSnapshotManifest> {
    let snapshot_bytes = fs::metadata(staged_snapshot_path)
        .with_context(|| format!("failed stat {}", staged_snapshot_path.display()))?
        .len();
    Ok(snapshot_manifest(
        created_at,
        source_db_path,
        staged_snapshot_path,
        state,
        snapshot_bytes,
    ))
}

fn list_archive_snapshot_paths(snapshot_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut archives = Vec::new();
    if !snapshot_dir.exists() {
        return Ok(archives);
    }
    for entry in fs::read_dir(snapshot_dir)
        .with_context(|| format!("failed reading {}", snapshot_dir.display()))?
    {
        let entry = entry.with_context(|| format!("failed reading {}", snapshot_dir.display()))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.starts_with(JOURNAL_SNAPSHOT_ARCHIVE_PREFIX)
            && name.ends_with(JOURNAL_SNAPSHOT_ARCHIVE_SUFFIX)
        {
            archives.push(path);
        }
    }
    archives.sort_by(|left, right| right.file_name().cmp(&left.file_name()));
    Ok(archives)
}

fn remove_orphan_archive_sidecars(snapshot_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut removed_paths = Vec::new();
    if !snapshot_dir.exists() {
        return Ok(removed_paths);
    }
    for entry in fs::read_dir(snapshot_dir)
        .with_context(|| format!("failed reading {}", snapshot_dir.display()))?
    {
        let entry = entry.with_context(|| format!("failed reading {}", snapshot_dir.display()))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.starts_with(JOURNAL_SNAPSHOT_ARCHIVE_PREFIX) && name.ends_with(".json") {
            let sqlite_path = path.with_extension("sqlite");
            if !sqlite_path.exists() {
                if remove_file_if_exists(&path)? {
                    removed_paths.push(path);
                }
            }
            continue;
        }
        if name.starts_with(JOURNAL_SNAPSHOT_ARCHIVE_PREFIX)
            && (name.ends_with(".sqlite-wal") || name.ends_with(".sqlite-shm"))
        {
            let sqlite_name = name
                .trim_end_matches("-wal")
                .trim_end_matches("-shm")
                .to_string();
            let sqlite_path = snapshot_dir.join(sqlite_name);
            if !sqlite_path.exists() && remove_file_if_exists(&path)? {
                removed_paths.push(path);
            }
        }
    }
    Ok(removed_paths)
}

fn cleanup_stale_staged_snapshot_artifacts(
    snapshot_dir: &Path,
    preserve_paths: &[PathBuf],
) -> Result<Vec<PathBuf>> {
    let mut removed_paths = Vec::new();
    if !snapshot_dir.exists() {
        return Ok(removed_paths);
    }
    for entry in fs::read_dir(snapshot_dir)
        .with_context(|| format!("failed reading {}", snapshot_dir.display()))?
    {
        let entry = entry.with_context(|| format!("failed reading {}", snapshot_dir.display()))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let is_hidden_recent_raw_temp = name.starts_with('.')
            && name.contains(JOURNAL_SNAPSHOT_ARCHIVE_PREFIX)
            && (name.contains(".snapshot-tmp-")
                || name.contains(".tmp-")
                || name.ends_with(STAGED_ARCHIVE_SNAPSHOT_SUFFIX)
                || name.ends_with(STAGED_ARCHIVE_METADATA_SUFFIX));
        if is_hidden_recent_raw_temp
            && !preserve_paths
                .iter()
                .any(|preserve_path| preserve_path == &path)
            && remove_file_if_exists(&path)?
        {
            removed_paths.push(path);
        }
    }
    Ok(removed_paths)
}

fn enforce_snapshot_archive_retention(
    snapshot_dir: &Path,
    keep: usize,
    preserve_paths: &[PathBuf],
) -> Result<SnapshotArchiveMaintenance> {
    let cleanup_removed_paths =
        cleanup_stale_staged_snapshot_artifacts(snapshot_dir, preserve_paths)?;
    let mut orphan_cleanup_removed_paths = remove_orphan_archive_sidecars(snapshot_dir)?;
    let archive_paths = list_archive_snapshot_paths(snapshot_dir)?;
    let archive_set_count_before = archive_paths.len();
    let mut pruned_snapshot_paths = Vec::new();
    for snapshot_path in archive_paths.into_iter().skip(keep) {
        let removed_paths = remove_snapshot_set(&snapshot_path)?;
        if removed_paths.iter().any(|path| path == &snapshot_path) {
            pruned_snapshot_paths.push(snapshot_path);
        }
    }
    let archive_set_count_after = list_archive_snapshot_paths(snapshot_dir)?.len();
    let mut cleanup_removed_paths_all = cleanup_removed_paths;
    cleanup_removed_paths_all.append(&mut orphan_cleanup_removed_paths);
    let mut maintenance = SnapshotArchiveMaintenance::default();
    maintenance.record_pass(
        archive_set_count_before,
        archive_set_count_after,
        cleanup_removed_paths_all,
        pruned_snapshot_paths,
    );
    Ok(maintenance)
}

fn manifest_for_existing_snapshot(
    source_db_path: &Path,
    snapshot_path: &Path,
) -> Result<RecentRawJournalSnapshotManifest> {
    let created_at = infer_created_at(snapshot_path)?;
    manifest_for_snapshot(source_db_path, snapshot_path, created_at)
}

fn infer_created_at(path: &Path) -> Result<DateTime<Utc>> {
    let modified = fs::metadata(path)
        .with_context(|| format!("failed stat {}", path.display()))?
        .modified()
        .with_context(|| format!("failed reading modified time for {}", path.display()))?;
    Ok(DateTime::<Utc>::from(modified))
}

fn manifest_for_snapshot(
    source_db_path: &Path,
    snapshot_path: &Path,
    created_at: DateTime<Utc>,
) -> Result<RecentRawJournalSnapshotManifest> {
    let snapshot_store = SqliteStore::open_read_only(snapshot_path)
        .with_context(|| format!("failed opening {}", snapshot_path.display()))?;
    let state = snapshot_store.recent_raw_journal_state_read_only()?;
    let snapshot_bytes = fs::metadata(snapshot_path)
        .with_context(|| format!("failed stat {}", snapshot_path.display()))?
        .len();
    Ok(snapshot_manifest(
        created_at,
        source_db_path,
        snapshot_path,
        &state,
        snapshot_bytes,
    ))
}

#[cfg(test)]
thread_local! {
    static POST_SNAPSHOT_PUBLISH_HOOK: std::cell::RefCell<Option<Box<dyn FnMut(&Path) -> Result<()>>>> =
        std::cell::RefCell::new(None);
    static PRE_ARCHIVE_PROMOTION_HOOK: std::cell::RefCell<Option<Box<dyn FnMut(&Path) -> Result<()>>>> =
        std::cell::RefCell::new(None);
    static RESUMABLE_SNAPSHOT_PROGRESS_HOOK: std::cell::RefCell<Option<Box<dyn FnMut(usize, usize) -> bool>>> =
        std::cell::RefCell::new(None);
}

#[cfg(test)]
struct SnapshotPublishHookGuard;

#[cfg(test)]
impl Drop for SnapshotPublishHookGuard {
    fn drop(&mut self) {
        POST_SNAPSHOT_PUBLISH_HOOK.with(|slot| {
            slot.borrow_mut().take();
        });
        PRE_ARCHIVE_PROMOTION_HOOK.with(|slot| {
            slot.borrow_mut().take();
        });
        RESUMABLE_SNAPSHOT_PROGRESS_HOOK.with(|slot| {
            slot.borrow_mut().take();
        });
    }
}

#[cfg(test)]
fn install_post_snapshot_publish_hook<F>(hook: F) -> SnapshotPublishHookGuard
where
    F: FnMut(&Path) -> Result<()> + 'static,
{
    POST_SNAPSHOT_PUBLISH_HOOK.with(|slot| {
        let mut slot = slot.borrow_mut();
        assert!(slot.is_none(), "snapshot publish hook already installed");
        *slot = Some(Box::new(hook));
    });
    SnapshotPublishHookGuard
}

#[cfg(test)]
fn install_pre_archive_promotion_hook<F>(hook: F) -> SnapshotPublishHookGuard
where
    F: FnMut(&Path) -> Result<()> + 'static,
{
    PRE_ARCHIVE_PROMOTION_HOOK.with(|slot| {
        let mut slot = slot.borrow_mut();
        assert!(
            slot.is_none(),
            "pre-archive promotion hook already installed"
        );
        *slot = Some(Box::new(hook));
    });
    SnapshotPublishHookGuard
}

#[cfg(test)]
fn install_resumable_snapshot_progress_hook<F>(hook: F) -> SnapshotPublishHookGuard
where
    F: FnMut(usize, usize) -> bool + 'static,
{
    RESUMABLE_SNAPSHOT_PROGRESS_HOOK.with(|slot| {
        let mut slot = slot.borrow_mut();
        assert!(
            slot.is_none(),
            "resumable snapshot progress hook already installed"
        );
        *slot = Some(Box::new(hook));
    });
    SnapshotPublishHookGuard
}

#[cfg(test)]
fn invoke_post_snapshot_publish_hook(snapshot_path: &Path) -> Result<()> {
    let hook = POST_SNAPSHOT_PUBLISH_HOOK.with(|slot| slot.borrow_mut().take());
    if let Some(mut hook) = hook {
        hook(snapshot_path)?;
    }
    Ok(())
}

#[cfg(not(test))]
fn invoke_post_snapshot_publish_hook(_snapshot_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
fn invoke_pre_archive_promotion_hook(snapshot_path: &Path) -> Result<()> {
    let hook = PRE_ARCHIVE_PROMOTION_HOOK.with(|slot| slot.borrow_mut().take());
    if let Some(mut hook) = hook {
        hook(snapshot_path)?;
    }
    Ok(())
}

#[cfg(not(test))]
fn invoke_pre_archive_promotion_hook(_snapshot_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
fn resumable_snapshot_progress_hook_requests_budget_exhaustion(
    completed_batches: usize,
    staged_row_count: usize,
) -> bool {
    RESUMABLE_SNAPSHOT_PROGRESS_HOOK.with(|slot| {
        let mut slot = slot.borrow_mut();
        slot.as_mut()
            .is_some_and(|hook| hook(completed_batches, staged_row_count))
    })
}

#[cfg(not(test))]
fn resumable_snapshot_progress_hook_requests_budget_exhaustion(
    _completed_batches: usize,
    _staged_row_count: usize,
) -> bool {
    false
}

fn snapshot_attempt_deadline(max_attempt_duration: Option<StdDuration>) -> Option<Instant> {
    max_attempt_duration.map(|budget| Instant::now() + budget)
}

// Preserve the staged snapshot across deferred runs so the bounded scheduled path keeps advancing
// on live-size journals instead of restarting from zero on every timer tick.
fn resume_staged_snapshot_with_policy(
    source_db_path: &Path,
    source_store: &SqliteStore,
    snapshot_dir: &Path,
    now: DateTime<Utc>,
    snapshot_policy: &SqliteSnapshotPolicy,
) -> StagedSnapshotAttemptResult {
    let staged_snapshot_path = staged_snapshot_archive_path(snapshot_dir);
    let staged_metadata_path = staged_snapshot_metadata_path(snapshot_dir);
    let mut progress = StagedSnapshotProgress {
        staged_snapshot_path: Some(staged_snapshot_path.clone()),
        staged_metadata_path: Some(staged_metadata_path.clone()),
        ..StagedSnapshotProgress::default()
    };

    if let Err(error) = fs::create_dir_all(snapshot_dir)
        .with_context(|| format!("failed creating {}", snapshot_dir.display()))
    {
        return StagedSnapshotAttemptResult::HardFailure {
            manifest: None,
            progress,
            attempt_duration_ms: 0,
            reason: error.to_string(),
        };
    }

    let source_state = match source_store.recent_raw_journal_state_read_only() {
        Ok(state) => state,
        Err(error) => {
            return StagedSnapshotAttemptResult::HardFailure {
                manifest: None,
                progress,
                attempt_duration_ms: 0,
                reason: format!("failed reading source recent_raw journal state: {error}"),
            };
        }
    };

    let mut existing_manifest = match load_existing_staged_manifest(
        source_db_path,
        &staged_snapshot_path,
        &staged_metadata_path,
    ) {
        Ok(existing_manifest) => existing_manifest,
        Err(error) => {
            return StagedSnapshotAttemptResult::HardFailure {
                manifest: None,
                progress,
                attempt_duration_ms: 0,
                reason: format!(
                    "failed reading staged recent_raw snapshot progress from {}: {error}",
                    staged_snapshot_path.display()
                ),
            };
        }
    };

    if existing_manifest
        .as_ref()
        .is_some_and(|manifest| manifest.source_db_path != source_db_path.display().to_string())
    {
        match remove_staged_snapshot_artifacts(&staged_snapshot_path, &staged_metadata_path) {
            Ok(_) => {
                existing_manifest = None;
            }
            Err(error) => {
                return StagedSnapshotAttemptResult::HardFailure {
                    manifest: None,
                    progress,
                    attempt_duration_ms: 0,
                    reason: format!(
                        "failed resetting staged recent_raw snapshot progress in {} after source path changed: {error}",
                        snapshot_dir.display()
                    ),
                };
            }
        }
    }

    let latest_surface = assess_latest_surface(
        &journal_snapshot_latest_path(snapshot_dir),
        &journal_snapshot_latest_metadata_path(snapshot_dir),
    )
    .ok();

    if existing_manifest.as_ref().is_some_and(|manifest| {
        source_window_outran_staged_progress(&source_state, manifest)
            && latest_surface.as_ref().is_some_and(|surface| {
                surface.manifest.as_ref().is_some_and(|latest_manifest| {
                    reference_surface_outran_staged_progress(latest_manifest, manifest)
                })
            })
    }) {
        match remove_staged_snapshot_artifacts(&staged_snapshot_path, &staged_metadata_path) {
            Ok(_) => {
                existing_manifest = None;
            }
            Err(error) => {
                return StagedSnapshotAttemptResult::HardFailure {
                    manifest: None,
                    progress,
                    attempt_duration_ms: 0,
                    reason: format!(
                        "failed resetting stale staged recent_raw snapshot progress in {}: {error}",
                        snapshot_dir.display()
                    ),
                };
            }
        }
    }

    let staged_created_at = existing_manifest
        .as_ref()
        .map(|manifest| manifest.created_at)
        .unwrap_or(now);
    let staged_store = match SqliteStore::open(&staged_snapshot_path) {
        Ok(store) => store,
        Err(error) => {
            return StagedSnapshotAttemptResult::HardFailure {
                manifest: existing_manifest,
                progress,
                attempt_duration_ms: 0,
                reason: format!(
                    "failed opening staged recent_raw snapshot {}: {error}",
                    staged_snapshot_path.display()
                ),
            };
        }
    };
    if let Err(error) = staged_store.ensure_recent_raw_journal_tables() {
        return StagedSnapshotAttemptResult::HardFailure {
            manifest: existing_manifest,
            progress,
            attempt_duration_ms: 0,
            reason: format!(
                "failed ensuring staged recent_raw snapshot tables in {}: {error}",
                staged_snapshot_path.display()
            ),
        };
    }

    let before_state = match staged_store.recent_raw_journal_state_cached() {
        Ok(state) => state,
        Err(error) => {
            return StagedSnapshotAttemptResult::HardFailure {
                manifest: existing_manifest,
                progress,
                attempt_duration_ms: 0,
                reason: format!(
                    "failed reading staged recent_raw snapshot state from {}: {error}",
                    staged_snapshot_path.display()
                ),
            };
        }
    };
    progress.row_count_before_attempt = Some(before_state.row_count);
    progress.covered_through_cursor_before_attempt = before_state.covered_through_cursor.clone();
    progress.resumed_from_existing_stage = before_state.row_count > 0;

    let deadline = snapshot_attempt_deadline(snapshot_policy.max_attempt_duration);
    let started = Instant::now();
    let batch_limit = snapshot_resume_batch_size(snapshot_policy);
    let mut completed_batches = 0usize;
    let mut current_state = before_state.clone();
    let mut budget_exhausted = false;

    while source_state.row_count > current_state.row_count {
        let deadline =
            deadline.unwrap_or_else(|| Instant::now() + StdDuration::from_secs(24 * 60 * 60));
        progress.terminal_phase = Some(StagedSnapshotTerminalPhase::SourceRead);
        if Instant::now() >= deadline {
            budget_exhausted = true;
            break;
        }

        let mut batch = Vec::with_capacity(batch_limit);
        let page = if let Some(cursor) = current_state.covered_through_cursor.as_ref() {
            source_store.for_each_observed_swap_after_cursor_with_budget(
                cursor.ts_utc,
                cursor.slot,
                &cursor.signature,
                batch_limit,
                deadline,
                |swap| {
                    batch.push(swap);
                    Ok(())
                },
            )
        } else if let Some(covered_since) = source_state.covered_since {
            source_store.for_each_observed_swap_since_with_budget(
                covered_since,
                batch_limit,
                deadline,
                |swap| {
                    batch.push(swap);
                    Ok(())
                },
            )
        } else {
            break;
        };
        let page = match page {
            Ok(page) => page,
            Err(error) => {
                let attempt_duration_ms =
                    started.elapsed().as_millis().min(u64::MAX as u128) as u64;
                let manifest = staged_manifest_for_state(
                    source_db_path,
                    &staged_snapshot_path,
                    staged_created_at,
                    &current_state,
                )
                .ok();
                return StagedSnapshotAttemptResult::HardFailure {
                    manifest,
                    progress,
                    attempt_duration_ms,
                    reason: format!(
                        "failed loading bounded recent_raw source rows into staged snapshot {}: {error}",
                        staged_snapshot_path.display()
                    ),
                };
            }
        };

        if batch.is_empty() {
            if page.time_budget_exhausted {
                budget_exhausted = true;
            }
            break;
        }

        progress.terminal_phase = Some(StagedSnapshotTerminalPhase::StagedWrite);
        let (_write_summary, write_budget_exhausted) = match staged_store
            .insert_recent_raw_journal_batch_with_deadline(&batch, now, deadline)
        {
            Ok(result) => result,
            Err(error) => {
                let attempt_duration_ms =
                    started.elapsed().as_millis().min(u64::MAX as u128) as u64;
                let manifest = staged_manifest_for_state(
                    source_db_path,
                    &staged_snapshot_path,
                    staged_created_at,
                    &current_state,
                )
                .ok();
                return StagedSnapshotAttemptResult::HardFailure {
                        manifest,
                        progress,
                        attempt_duration_ms,
                        reason: format!(
                            "failed persisting bounded recent_raw batch into staged snapshot {}: {error}",
                            staged_snapshot_path.display()
                        ),
                    };
            }
        };

        current_state = match staged_store.recent_raw_journal_state_cached() {
            Ok(state) => state,
            Err(error) => {
                let attempt_duration_ms =
                    started.elapsed().as_millis().min(u64::MAX as u128) as u64;
                let manifest = staged_manifest_for_state(
                    source_db_path,
                    &staged_snapshot_path,
                    staged_created_at,
                    &before_state,
                )
                .ok();
                return StagedSnapshotAttemptResult::HardFailure {
                    manifest,
                    progress,
                    attempt_duration_ms,
                    reason: format!(
                        "failed refreshing staged recent_raw snapshot state from {}: {error}",
                        staged_snapshot_path.display()
                    ),
                };
            }
        };
        completed_batches = completed_batches.saturating_add(1);
        progress.advanced_during_attempt |= current_state.row_count > before_state.row_count
            || current_state.covered_through_cursor != before_state.covered_through_cursor;

        if write_budget_exhausted {
            budget_exhausted = true;
            break;
        }
        if resumable_snapshot_progress_hook_requests_budget_exhaustion(
            completed_batches,
            current_state.row_count,
        ) {
            budget_exhausted = true;
            break;
        }
        if page.time_budget_exhausted || Instant::now() >= deadline {
            budget_exhausted = true;
            break;
        }
        if page.rows_seen < batch_limit {
            break;
        }
    }

    progress.row_count_after_attempt = Some(current_state.row_count);
    progress.covered_through_cursor_after_attempt = current_state.covered_through_cursor.clone();

    let manifest = match staged_manifest_for_state(
        source_db_path,
        &staged_snapshot_path,
        staged_created_at,
        &current_state,
    ) {
        Ok(manifest) => manifest,
        Err(error) => {
            return StagedSnapshotAttemptResult::HardFailure {
                manifest: existing_manifest,
                progress,
                attempt_duration_ms: started.elapsed().as_millis().min(u64::MAX as u128) as u64,
                reason: format!(
                    "failed building staged recent_raw snapshot manifest from {}: {error}",
                    staged_snapshot_path.display()
                ),
            };
        }
    };

    if current_state.row_count > 0 || progress.resumed_from_existing_stage {
        if let Err(error) = write_json_atomic(&staged_metadata_path, &manifest)
            .with_context(|| format!("failed writing {}", staged_metadata_path.display()))
        {
            return StagedSnapshotAttemptResult::HardFailure {
                manifest: Some(manifest),
                progress,
                attempt_duration_ms: started.elapsed().as_millis().min(u64::MAX as u128) as u64,
                reason: error.to_string(),
            };
        }
    }

    if budget_exhausted {
        progress.preserved_for_retry =
            progress.resumed_from_existing_stage || current_state.row_count > 0;
        if !progress.preserved_for_retry {
            let _ = remove_staged_snapshot_artifacts(&staged_snapshot_path, &staged_metadata_path);
        }
        return StagedSnapshotAttemptResult::Deferred(StagedSnapshotAttempt {
            manifest,
            progress,
            attempt_duration_ms: started.elapsed().as_millis().min(u64::MAX as u128) as u64,
        });
    }

    StagedSnapshotAttemptResult::Completed(StagedSnapshotAttempt {
        manifest,
        progress,
        attempt_duration_ms: started.elapsed().as_millis().min(u64::MAX as u128) as u64,
    })
}

fn write_snapshot_with_policy(
    source_db_path: &Path,
    source_store: &SqliteStore,
    snapshot_path: &Path,
    now: DateTime<Utc>,
    snapshot_policy: &SqliteSnapshotPolicy,
) -> Result<(RecentRawJournalSnapshotManifest, SqliteSnapshotSummary), SnapshotWriteError> {
    if let Some(parent) = snapshot_path.parent() {
        fs::create_dir_all(parent).map_err(|error| SnapshotWriteError::HardFailure {
            summary: None,
            reason: format!(
                "failed creating snapshot parent dir {}: {error}",
                parent.display()
            ),
        })?;
    }
    let temp_snapshot_path = snapshot_temp_path(snapshot_path);
    cleanup_snapshot_temp(&temp_snapshot_path);
    let snapshot_outcome = source_store
        .snapshot_into_path_with_policy(&temp_snapshot_path, snapshot_policy)
        .map_err(|error| {
            cleanup_snapshot_temp(&temp_snapshot_path);
            SnapshotWriteError::HardFailure {
                summary: None,
                reason: format!("failed writing {}: {error}", snapshot_path.display()),
            }
        })?;
    let summary = match snapshot_outcome {
        SqliteSnapshotOutcome::Written(summary) => summary,
        SqliteSnapshotOutcome::RetryableBusy(summary) => {
            cleanup_snapshot_temp(&temp_snapshot_path);
            return Err(SnapshotWriteError::RetryableBusy { summary });
        }
        SqliteSnapshotOutcome::Deferred(summary) => {
            cleanup_snapshot_temp(&temp_snapshot_path);
            return Err(SnapshotWriteError::Deferred { summary });
        }
    };
    fs::rename(&temp_snapshot_path, snapshot_path).map_err(|error| {
        cleanup_snapshot_temp(&temp_snapshot_path);
        SnapshotWriteError::HardFailure {
            summary: Some(summary.clone()),
            reason: format!(
                "failed renaming {} to {}: {error}",
                temp_snapshot_path.display(),
                snapshot_path.display()
            ),
        }
    })?;
    invoke_post_snapshot_publish_hook(snapshot_path).map_err(|error| {
        SnapshotWriteError::HardFailure {
            summary: Some(summary.clone()),
            reason: format!(
                "failed running post-snapshot publish hook for {}: {error}",
                snapshot_path.display()
            ),
        }
    })?;
    let manifest = manifest_for_snapshot(source_db_path, snapshot_path, now).map_err(|error| {
        SnapshotWriteError::HardFailure {
            summary: Some(summary.clone()),
            reason: format!(
                "failed building manifest from snapshot {}: {error}",
                snapshot_path.display()
            ),
        }
    })?;
    Ok((manifest, summary))
}

fn snapshot_temp_path(snapshot_path: &Path) -> PathBuf {
    let file_name = snapshot_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("snapshot.sqlite");
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    snapshot_path.with_file_name(format!(
        ".{file_name}.snapshot-tmp-{}-{nonce}",
        process::id()
    ))
}

fn cleanup_snapshot_temp(path: &Path) {
    let _ = fs::remove_file(path);
}

fn link_or_copy_atomic(source_path: &Path, destination_path: &Path) -> Result<()> {
    if let Some(parent) = destination_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed creating {}", parent.display()))?;
    }
    let temp_path = snapshot_temp_path(destination_path);
    cleanup_snapshot_temp(&temp_path);
    match fs::hard_link(source_path, &temp_path) {
        Ok(()) => fs::rename(&temp_path, destination_path).with_context(|| {
            format!(
                "failed renaming {} to {}",
                temp_path.display(),
                destination_path.display()
            )
        }),
        Err(_) => copy_atomic(source_path, destination_path),
    }
}

fn snapshot_manifest(
    created_at: DateTime<Utc>,
    source_db_path: &Path,
    snapshot_path: &Path,
    state: &RecentRawJournalStateRow,
    snapshot_bytes: u64,
) -> RecentRawJournalSnapshotManifest {
    RecentRawJournalSnapshotManifest {
        created_at,
        source_db_path: source_db_path.display().to_string(),
        snapshot_path: snapshot_path.display().to_string(),
        row_count: state.row_count,
        covered_since: state.covered_since,
        covered_through_cursor: state.covered_through_cursor.clone(),
        last_batch_completed_at: state.last_batch_completed_at,
        updated_at: state.updated_at,
        snapshot_bytes,
    }
}

fn render_output(
    state: SnapshotState,
    latest_surface_status: LatestSurfaceStatus,
    latest_surface_action: LatestSurfaceAction,
    snapshot_context: &SnapshotContext,
    config_path: &Path,
    source_db_path: &Path,
    snapshot_path: &Path,
    metadata_path: &Path,
    archive_path: Option<&Path>,
    cadence_minutes: Option<u64>,
    retention: Option<usize>,
    manifest: Option<&RecentRawJournalSnapshotManifest>,
    snapshot_summary: Option<&SqliteSnapshotSummary>,
    retryable_reason: Option<String>,
    deferred_reason: Option<String>,
    hard_failure_reason: Option<String>,
    output_context: SnapshotOutputContext,
) -> SnapshotOutput {
    SnapshotOutput {
        event: "discovery_recent_raw_snapshot".to_string(),
        state: state.as_str().to_string(),
        latest_surface_status: latest_surface_status.as_str().to_string(),
        latest_surface_action: latest_surface_action.as_str().to_string(),
        config_path: config_path.display().to_string(),
        source_db_path: source_db_path.display().to_string(),
        snapshot_path: snapshot_path.display().to_string(),
        metadata_path: metadata_path.display().to_string(),
        archive_path: archive_path.map(|path| path.display().to_string()),
        staged_snapshot_path: output_context
            .staged_progress
            .staged_snapshot_path
            .as_ref()
            .map(|path| path.display().to_string()),
        staged_metadata_path: output_context
            .staged_progress
            .staged_metadata_path
            .as_ref()
            .map(|path| path.display().to_string()),
        cadence_minutes,
        retention,
        pruned_snapshot_paths: output_context
            .archive_maintenance
            .pruned_snapshot_paths
            .iter()
            .map(|path| path.display().to_string())
            .collect(),
        cleanup_removed_paths: output_context
            .archive_maintenance
            .cleanup_removed_paths
            .iter()
            .map(|path| path.display().to_string())
            .collect(),
        archive_promoted: output_context.archive_promoted,
        archive_set_count_before: output_context.archive_maintenance.archive_set_count_before,
        archive_set_count_after: output_context.archive_maintenance.archive_set_count_after,
        staged_progress_resumed: output_context.staged_progress.resumed_from_existing_stage,
        staged_progress_preserved_for_retry: output_context.staged_progress.preserved_for_retry,
        staged_progress_advanced: output_context.staged_progress.advanced_during_attempt,
        staged_row_count_before_attempt: output_context.staged_progress.row_count_before_attempt,
        staged_row_count_after_attempt: output_context.staged_progress.row_count_after_attempt,
        staged_covered_through_cursor_before_attempt: output_context
            .staged_progress
            .covered_through_cursor_before_attempt
            .clone(),
        staged_covered_through_cursor_after_attempt: output_context
            .staged_progress
            .covered_through_cursor_after_attempt
            .clone(),
        source_db_bytes: snapshot_context.source_stats.source_db_bytes,
        source_wal_bytes: snapshot_context.source_stats.source_wal_bytes,
        source_total_bytes: snapshot_context.source_stats.source_total_bytes(),
        source_page_size_bytes: snapshot_context.source_stats.source_page_size_bytes,
        source_page_count: snapshot_context.source_stats.source_page_count,
        snapshot_pages_per_step: snapshot_context.policy.pages_per_step,
        snapshot_pause_between_steps_ms: snapshot_context
            .policy
            .pause_between_steps
            .as_millis()
            .min(u64::MAX as u128) as u64,
        snapshot_max_attempt_duration_ms: snapshot_policy_max_attempt_duration_ms(
            &snapshot_context.policy,
        ),
        attempt_duration_ms: output_context
            .attempt_duration_ms_override
            .or_else(|| snapshot_summary.map(|summary| summary.duration_ms))
            .unwrap_or(0),
        backup_step_count: snapshot_summary
            .map(|summary| summary.backup_step_count)
            .unwrap_or(0),
        backup_retry_count: snapshot_summary
            .map(|summary| summary.backup_retry_count)
            .unwrap_or(0),
        busy_retry_count: snapshot_summary
            .map(|summary| summary.busy_retry_count)
            .unwrap_or(0),
        locked_retry_count: snapshot_summary
            .map(|summary| summary.locked_retry_count)
            .unwrap_or(0),
        backup_total_page_count: snapshot_summary.map(|summary| summary.total_page_count),
        backup_remaining_page_count: snapshot_summary.map(|summary| summary.remaining_page_count),
        backup_copied_page_count: snapshot_summary.map(|summary| summary.copied_page_count),
        terminal_reason: output_context
            .terminal_reason_override
            .or_else(|| snapshot_terminal_reason(state, snapshot_summary)),
        retryable_reason,
        deferred_reason,
        hard_failure_reason,
        created_at: manifest.map(|manifest| manifest.created_at),
        row_count: manifest.map(|manifest| manifest.row_count),
        covered_since: manifest.and_then(|manifest| manifest.covered_since),
        covered_through_cursor: manifest
            .and_then(|manifest| manifest.covered_through_cursor.clone()),
        last_batch_completed_at: manifest.and_then(|manifest| manifest.last_batch_completed_at),
        snapshot_bytes: manifest.map(|manifest| manifest.snapshot_bytes),
    }
}

fn render_human(output: &SnapshotOutput) -> String {
    [
        format!("event={}", output.event),
        format!("state={}", output.state),
        format!("latest_surface_status={}", output.latest_surface_status),
        format!("latest_surface_action={}", output.latest_surface_action),
        format!("config_path={}", output.config_path),
        format!("source_db_path={}", output.source_db_path),
        format!("snapshot_path={}", output.snapshot_path),
        format!("metadata_path={}", output.metadata_path),
        format!(
            "archive_path={}",
            output.archive_path.as_deref().unwrap_or("null")
        ),
        format!(
            "staged_snapshot_path={}",
            output.staged_snapshot_path.as_deref().unwrap_or("null")
        ),
        format!(
            "staged_metadata_path={}",
            output.staged_metadata_path.as_deref().unwrap_or("null")
        ),
        format!(
            "cadence_minutes={}",
            output
                .cadence_minutes
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "retention={}",
            output
                .retention
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!("pruned_snapshots={}", output.pruned_snapshot_paths.len()),
        format!(
            "cleanup_removed_paths={}",
            output.cleanup_removed_paths.len()
        ),
        format!("archive_promoted={}", output.archive_promoted),
        format!(
            "archive_set_count_before={}",
            output
                .archive_set_count_before
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "archive_set_count_after={}",
            output
                .archive_set_count_after
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!("staged_progress_resumed={}", output.staged_progress_resumed),
        format!(
            "staged_progress_preserved_for_retry={}",
            output.staged_progress_preserved_for_retry
        ),
        format!(
            "staged_progress_advanced={}",
            output.staged_progress_advanced
        ),
        format!(
            "staged_row_count_before_attempt={}",
            output
                .staged_row_count_before_attempt
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_row_count_after_attempt={}",
            output
                .staged_row_count_after_attempt
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_covered_through_cursor_before_attempt={}",
            output
                .staged_covered_through_cursor_before_attempt
                .as_ref()
                .map(|cursor| format!(
                    "{}/{}/{}",
                    cursor.ts_utc.to_rfc3339(),
                    cursor.slot,
                    cursor.signature
                ))
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "staged_covered_through_cursor_after_attempt={}",
            output
                .staged_covered_through_cursor_after_attempt
                .as_ref()
                .map(|cursor| format!(
                    "{}/{}/{}",
                    cursor.ts_utc.to_rfc3339(),
                    cursor.slot,
                    cursor.signature
                ))
                .unwrap_or_else(|| "null".to_string())
        ),
        format!("source_db_bytes={}", output.source_db_bytes),
        format!("source_wal_bytes={}", output.source_wal_bytes),
        format!("source_total_bytes={}", output.source_total_bytes),
        format!("source_page_size_bytes={}", output.source_page_size_bytes),
        format!("source_page_count={}", output.source_page_count),
        format!("snapshot_pages_per_step={}", output.snapshot_pages_per_step),
        format!(
            "snapshot_pause_between_steps_ms={}",
            output.snapshot_pause_between_steps_ms
        ),
        format!(
            "snapshot_max_attempt_duration_ms={}",
            output
                .snapshot_max_attempt_duration_ms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!("attempt_duration_ms={}", output.attempt_duration_ms),
        format!("backup_step_count={}", output.backup_step_count),
        format!("backup_retry_count={}", output.backup_retry_count),
        format!("busy_retry_count={}", output.busy_retry_count),
        format!("locked_retry_count={}", output.locked_retry_count),
        format!(
            "backup_total_page_count={}",
            output
                .backup_total_page_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "backup_remaining_page_count={}",
            output
                .backup_remaining_page_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "backup_copied_page_count={}",
            output
                .backup_copied_page_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "terminal_reason={}",
            output.terminal_reason.as_deref().unwrap_or("null")
        ),
        format!(
            "retryable_reason={}",
            output.retryable_reason.as_deref().unwrap_or("null")
        ),
        format!(
            "deferred_reason={}",
            output.deferred_reason.as_deref().unwrap_or("null")
        ),
        format!(
            "hard_failure_reason={}",
            output.hard_failure_reason.as_deref().unwrap_or("null")
        ),
        format!(
            "created_at={}",
            output
                .created_at
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "row_count={}",
            output
                .row_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "covered_since={}",
            output
                .covered_since
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "covered_through_cursor={}",
            output
                .covered_through_cursor
                .as_ref()
                .map(|cursor| format!(
                    "{}/{}/{}",
                    cursor.ts_utc.to_rfc3339(),
                    cursor.slot,
                    cursor.signature
                ))
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "last_batch_completed_at={}",
            output
                .last_batch_completed_at
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "snapshot_bytes={}",
            output
                .snapshot_bytes
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use super::{
        adaptive_snapshot_policy, install_pre_archive_promotion_hook,
        install_resumable_snapshot_progress_hook, parse_args_from, run,
        run_with_snapshot_policy_override, source_window_outran_staged_progress, Config,
        RecentRawJournalSnapshotManifest, SnapshotSourceStats, SqliteStore,
    };
    use anyhow::{Context, Result};
    use chrono::{DateTime, Duration, Utc};
    use copybot_core_types::SwapEvent;
    use copybot_discovery::runtime_restore_ops::{load_json, write_json_atomic};
    use copybot_storage::RecentRawJournalStateRow;
    use serde_json::Value;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Barrier};
    use std::time::Duration as StdDuration;
    use tempfile::tempdir;

    #[test]
    fn parse_args_from_accepts_scheduled_json_and_now() {
        let parsed = parse_args_from(vec![
            "--config".to_string(),
            "configs/live.toml".to_string(),
            "--journal-db-path".to_string(),
            "state/discovery_recent_raw.db".to_string(),
            "--scheduled".to_string(),
            "--json".to_string(),
            "--now".to_string(),
            "2026-03-23T12:00:00Z".to_string(),
        ])
        .expect("parse should succeed")
        .expect("config should be present");
        assert_eq!(parsed.config_path, PathBuf::from("configs/live.toml"));
        assert_eq!(
            parsed.journal_db_path,
            Some(PathBuf::from("state/discovery_recent_raw.db"))
        );
        assert!(parsed.scheduled);
        assert!(parsed.json);
    }

    #[test]
    fn snapshot_state_exit_codes_keep_transient_contention_distinct_from_hard_failure() {
        assert_eq!(super::SnapshotState::Deferred.exit_code(), 75);
        assert_eq!(super::SnapshotState::RetryableBusy.exit_code(), 75);
        assert_eq!(super::SnapshotState::HardFailure.exit_code(), 1);
    }

    #[test]
    fn adaptive_snapshot_policy_scales_for_large_sources() {
        let policy = adaptive_snapshot_policy(&SnapshotSourceStats {
            source_db_bytes: 1_200 * 1024 * 1024,
            source_wal_bytes: 459 * 1024 * 1024,
            source_page_size_bytes: 4096,
            source_page_count: 310_000,
        });
        assert!(
            policy.pages_per_step > 16,
            "large sources must use larger backup steps than the tiny default"
        );
        assert_eq!(
            policy.pause_between_steps,
            StdDuration::from_millis(super::SNAPSHOT_LARGE_PAUSE_BETWEEN_STEPS_MS)
        );
        assert_eq!(
            policy.max_attempt_duration,
            Some(StdDuration::from_millis(
                super::SNAPSHOT_HUGE_MAX_ATTEMPT_DURATION_MS,
            ))
        );
    }

    #[test]
    fn source_window_outran_staged_progress_returns_true_when_source_cursor_is_newer() -> Result<()>
    {
        let source_state = RecentRawJournalStateRow {
            covered_since: Some(parse_ts("2026-03-27T11:43:56Z")?),
            covered_through_cursor: Some(super::DiscoveryRuntimeCursor {
                ts_utc: parse_ts("2026-03-29T12:44:48Z")?,
                slot: 33_206_523,
                signature: "sig-source-newer".to_string(),
            }),
            row_count: 33_206_523,
            ..RecentRawJournalStateRow::default()
        };
        let staged_manifest = RecentRawJournalSnapshotManifest {
            created_at: parse_ts("2026-03-27T12:00:00Z")?,
            source_db_path: "/tmp/source.db".to_string(),
            snapshot_path: "/tmp/staged.sqlite".to_string(),
            row_count: 22_938_251,
            covered_since: Some(parse_ts("2026-03-27T11:43:56Z")?),
            covered_through_cursor: Some(super::DiscoveryRuntimeCursor {
                ts_utc: parse_ts("2026-03-27T11:43:56Z")?,
                slot: 22_938_251,
                signature: "sig-staged-older".to_string(),
            }),
            last_batch_completed_at: Some(parse_ts("2026-03-27T11:45:00Z")?),
            updated_at: Some(parse_ts("2026-03-27T11:45:00Z")?),
            snapshot_bytes: 1,
        };

        assert!(source_window_outran_staged_progress(
            &source_state,
            &staged_manifest
        ));
        Ok(())
    }

    #[test]
    fn scheduled_run_returns_bounded_deferred_outcome_when_attempt_budget_is_exhausted(
    ) -> Result<()> {
        let fixture = make_fixture("recent-raw-snapshot-budget")?;
        let now = parse_ts("2026-03-23T12:00:00Z")?;
        seed_recent_raw_journal_range(
            &fixture.journal_store,
            now - Duration::minutes(30),
            10,
            "sig-budget",
            512,
            now,
        )?;
        let _guard =
            install_resumable_snapshot_progress_hook(|completed_batches, _staged_row_count| {
                completed_batches >= 1
            });

        let deferred = run_with_snapshot_policy_override(
            Config {
                config_path: fixture.config_path.clone(),
                journal_db_path: Some(fixture.journal_db_path.clone()),
                output_path: None,
                scheduled: true,
                force: true,
                json: true,
                now,
            },
            Some(copybot_storage::SqliteSnapshotPolicy {
                busy_timeout: StdDuration::from_millis(1),
                pages_per_step: 8,
                pause_between_steps: StdDuration::from_millis(0),
                retry_backoff_ms: vec![1, 1],
                // Keep a real bounded deadline, but make exhaustion deterministic via the
                // resumable-progress hook after one committed staged batch. A 1ms wall-clock
                // budget can expire before the first staged insert under full-bin runs, which
                // turns this into a flaky zero-progress timeout instead of the intended
                // preserved-progress deferred contract.
                max_attempt_duration: Some(StdDuration::from_secs(5)),
                pin_source_snapshot: true,
            }),
        )?;
        assert_eq!(deferred.exit_code, 75);
        let output: Value = serde_json::from_str(&deferred.rendered_output)?;
        assert_eq!(output["state"], "deferred");
        assert_eq!(
            output["terminal_reason"],
            "staged_write_attempt_duration_budget_exhausted"
        );
        assert_eq!(
            output["latest_surface_action"],
            "unchanged_due_to_attempt_budget"
        );
        assert_eq!(output["snapshot_pages_per_step"], 8);
        assert_eq!(output["staged_progress_preserved_for_retry"], true);
        assert_eq!(output["staged_progress_advanced"], true);
        assert!(
            output["staged_row_count_after_attempt"]
                .as_u64()
                .unwrap_or_default()
                > output["staged_row_count_before_attempt"]
                    .as_u64()
                    .unwrap_or_default(),
            "bounded deferred outcome must expose preserved staged forward progress"
        );
        assert_eq!(staged_artifact_count(&fixture.snapshot_dir())?, 2);
        Ok(())
    }

    #[test]
    fn scheduled_run_resets_outrun_staged_snapshot_instead_of_resuming_it() -> Result<()> {
        let fixture = make_fixture("recent-raw-snapshot-outrun-stage")?;
        let initial_now = parse_ts("2026-03-23T12:00:00Z")?;
        seed_recent_raw_journal_range(
            &fixture.journal_store,
            initial_now - Duration::minutes(5),
            10,
            "sig-outrun-stage",
            16,
            initial_now,
        )?;
        run(Config {
            config_path: fixture.config_path.clone(),
            journal_db_path: Some(fixture.journal_db_path.clone()),
            output_path: None,
            scheduled: true,
            force: true,
            json: false,
            now: initial_now,
        })?;

        let snapshot_dir = fixture.snapshot_dir();
        let stale_stage_seed_path = snapshot_dir.join("stale-stage-seed.sqlite");
        let stale_stage_seed_metadata_path = snapshot_dir.join("stale-stage-seed.json");
        let initial_latest_snapshot_path = snapshot_dir.join("latest.sqlite");
        let initial_latest_manifest_path = snapshot_dir.join("latest.json");
        let initial_latest_manifest: RecentRawJournalSnapshotManifest =
            load_json(&initial_latest_manifest_path)?;
        std::fs::copy(&initial_latest_snapshot_path, &stale_stage_seed_path).with_context(
            || {
                format!(
                    "failed copying {} to {}",
                    initial_latest_snapshot_path.display(),
                    stale_stage_seed_path.display()
                )
            },
        )?;
        write_json_atomic(&stale_stage_seed_metadata_path, &initial_latest_manifest)?;

        seed_recent_raw_journal_range(
            &fixture.journal_store,
            initial_now + Duration::minutes(1),
            10_000,
            "sig-outrun-source",
            128,
            initial_now + Duration::minutes(1),
        )?;
        run(Config {
            config_path: fixture.config_path.clone(),
            journal_db_path: Some(fixture.journal_db_path.clone()),
            output_path: None,
            scheduled: true,
            force: true,
            json: false,
            now: initial_now + Duration::minutes(15),
        })?;

        let staged_snapshot_path = super::staged_snapshot_archive_path(&snapshot_dir);
        let staged_metadata_path = super::staged_snapshot_metadata_path(&snapshot_dir);
        std::fs::copy(&stale_stage_seed_path, &staged_snapshot_path).with_context(|| {
            format!(
                "failed copying {} to {}",
                stale_stage_seed_path.display(),
                staged_snapshot_path.display()
            )
        })?;
        let staged_manifest = super::manifest_for_snapshot(
            &fixture.journal_db_path,
            &staged_snapshot_path,
            initial_latest_manifest.created_at,
        )?;
        write_json_atomic(&staged_metadata_path, &staged_manifest)?;
        let current_source_state = fixture.journal_store.recent_raw_journal_state()?;
        let current_latest_manifest: RecentRawJournalSnapshotManifest =
            load_json(&snapshot_dir.join("latest.json"))?;
        assert!(source_window_outran_staged_progress(
            &current_source_state,
            &staged_manifest
        ));
        assert!(super::reference_surface_outran_staged_progress(
            &current_latest_manifest,
            &staged_manifest
        ));

        let written = run(Config {
            config_path: fixture.config_path.clone(),
            journal_db_path: Some(fixture.journal_db_path.clone()),
            output_path: None,
            scheduled: true,
            force: false,
            json: true,
            now: initial_now + Duration::minutes(30),
        })?;
        assert_eq!(written.exit_code, 0);
        let output: Value = serde_json::from_str(&written.rendered_output)?;
        assert_eq!(output["state"], "written");
        assert_eq!(
            output["staged_progress_resumed"],
            false,
            "unexpected outrun-stage output: {}",
            serde_json::to_string_pretty(&output)?
        );
        assert_eq!(output["staged_row_count_before_attempt"].as_u64(), Some(0));

        let promoted_latest_manifest: RecentRawJournalSnapshotManifest =
            load_json(&snapshot_dir.join("latest.json"))?;
        assert!(
            promoted_latest_manifest.row_count > staged_manifest.row_count,
            "outrun staged progress must be reset and replaced with a newer published latest"
        );
        Ok(())
    }

    #[test]
    fn scheduled_run_resumes_preserved_staged_progress_and_eventually_replaces_stale_latest(
    ) -> Result<()> {
        let fixture = make_fixture("recent-raw-snapshot-resume-incident")?;
        let initial_now = parse_ts("2026-03-23T12:00:00Z")?;
        seed_recent_raw_journal_range(
            &fixture.journal_store,
            initial_now - Duration::minutes(5),
            10,
            "sig-initial",
            32,
            initial_now,
        )?;
        run(Config {
            config_path: fixture.config_path.clone(),
            journal_db_path: Some(fixture.journal_db_path.clone()),
            output_path: None,
            scheduled: true,
            force: true,
            json: false,
            now: initial_now,
        })?;

        let latest_manifest_path = fixture.snapshot_dir().join("latest.json");
        let stale_latest_manifest: RecentRawJournalSnapshotManifest =
            load_json(&latest_manifest_path)?;
        seed_recent_raw_journal_range(
            &fixture.journal_store,
            initial_now + Duration::minutes(1),
            1_000,
            "sig-incident",
            4096,
            initial_now + Duration::minutes(1),
        )?;

        let mut forced_budget_exhaustions = 0usize;
        let _guard = install_resumable_snapshot_progress_hook(move |completed_batches, _rows| {
            if forced_budget_exhaustions < 2 && completed_batches >= 2 {
                forced_budget_exhaustions += 1;
                true
            } else {
                false
            }
        });
        let policy = Some(copybot_storage::SqliteSnapshotPolicy {
            busy_timeout: StdDuration::from_millis(1),
            pages_per_step: 64,
            pause_between_steps: StdDuration::from_millis(0),
            retry_backoff_ms: vec![1, 1],
            max_attempt_duration: Some(StdDuration::from_secs(5)),
            pin_source_snapshot: true,
        });

        let first_deferred = run_with_snapshot_policy_override(
            Config {
                config_path: fixture.config_path.clone(),
                journal_db_path: Some(fixture.journal_db_path.clone()),
                output_path: None,
                scheduled: true,
                force: false,
                json: true,
                now: initial_now + Duration::minutes(15),
            },
            policy.clone(),
        )?;
        let first_output: Value = serde_json::from_str(&first_deferred.rendered_output)?;
        assert_eq!(first_output["state"], "deferred");
        assert_eq!(first_output["latest_surface_status"], "healthy");
        assert_eq!(
            first_output["latest_surface_action"],
            "deferred_due_to_attempt_budget"
        );
        assert_eq!(first_output["staged_progress_resumed"], false);
        assert_eq!(first_output["staged_progress_preserved_for_retry"], true);
        assert_eq!(first_output["staged_progress_advanced"], true);
        let first_staged_rows = first_output["staged_row_count_after_attempt"]
            .as_u64()
            .context("first deferred staged row count must be present")?;
        assert!(first_staged_rows > stale_latest_manifest.row_count as u64);
        assert_eq!(staged_artifact_count(&fixture.snapshot_dir())?, 2);

        let second_deferred = run_with_snapshot_policy_override(
            Config {
                config_path: fixture.config_path.clone(),
                journal_db_path: Some(fixture.journal_db_path.clone()),
                output_path: None,
                scheduled: true,
                force: false,
                json: true,
                now: initial_now + Duration::minutes(20),
            },
            policy.clone(),
        )?;
        let second_output: Value = serde_json::from_str(&second_deferred.rendered_output)?;
        assert_eq!(second_output["state"], "deferred");
        assert_eq!(second_output["staged_progress_resumed"], true);
        assert_eq!(second_output["staged_progress_preserved_for_retry"], true);
        assert_eq!(second_output["staged_progress_advanced"], true);
        assert_eq!(
            second_output["staged_row_count_before_attempt"].as_u64(),
            Some(first_staged_rows)
        );
        let second_staged_rows = second_output["staged_row_count_after_attempt"]
            .as_u64()
            .context("second deferred staged row count must be present")?;
        assert!(second_staged_rows > first_staged_rows);
        assert_eq!(staged_artifact_count(&fixture.snapshot_dir())?, 2);

        let completed = run_with_snapshot_policy_override(
            Config {
                config_path: fixture.config_path.clone(),
                journal_db_path: Some(fixture.journal_db_path.clone()),
                output_path: None,
                scheduled: true,
                force: false,
                json: true,
                now: initial_now + Duration::minutes(25),
            },
            policy,
        )?;
        assert_eq!(completed.exit_code, 0);
        let completed_output: Value = serde_json::from_str(&completed.rendered_output)?;
        assert_eq!(completed_output["state"], "written");
        assert_eq!(completed_output["archive_promoted"], true);
        assert_eq!(completed_output["staged_progress_resumed"], true);
        assert_eq!(staged_artifact_count(&fixture.snapshot_dir())?, 0);

        let promoted_latest_manifest: RecentRawJournalSnapshotManifest =
            load_json(&latest_manifest_path)?;
        assert!(
            promoted_latest_manifest.row_count > stale_latest_manifest.row_count,
            "resumed completion must replace the stale latest surface with newer bounded coverage"
        );
        assert_ne!(
            promoted_latest_manifest.covered_through_cursor,
            stale_latest_manifest.covered_through_cursor
        );
        assert_eq!(
            promoted_latest_manifest.row_count,
            fixture.journal_store.recent_raw_journal_state()?.row_count
        );
        assert!(
            archive_count(&fixture.snapshot_dir())? <= 2,
            "archive retention must remain bounded after resumed completion"
        );
        Ok(())
    }

    #[test]
    fn scheduled_run_completes_under_live_source_writes() -> Result<()> {
        let fixture = make_fixture("recent-raw-snapshot-live-writes")?;
        let now = parse_ts("2026-03-23T12:00:00Z")?;
        seed_recent_raw_journal_many(&fixture.journal_store, now, 1024)?;

        let start_barrier = Arc::new(Barrier::new(2));
        let stop_writes = Arc::new(AtomicBool::new(false));
        let writer_path = fixture.journal_db_path.clone();
        let writer_barrier = start_barrier.clone();
        let writer_stop = stop_writes.clone();
        let writer_now = now + Duration::minutes(1);
        let writer = std::thread::spawn(move || -> Result<()> {
            let writer_store = SqliteStore::open(&writer_path)?;
            writer_barrier.wait();
            let mut counter = 0usize;
            while !writer_stop.load(Ordering::Relaxed) {
                writer_store.insert_recent_raw_journal_batch(
                    &[make_swap(
                        &format!("sig-live-{counter:04}"),
                        writer_now + Duration::seconds(counter as i64),
                        20_000 + counter as u64,
                    )],
                    writer_now + Duration::seconds(counter as i64),
                )?;
                counter += 1;
            }
            Ok(())
        });

        start_barrier.wait();
        let written = run(Config {
            config_path: fixture.config_path.clone(),
            journal_db_path: Some(fixture.journal_db_path.clone()),
            output_path: None,
            scheduled: true,
            force: true,
            json: true,
            now,
        })?;
        stop_writes.store(true, Ordering::Relaxed);
        writer
            .join()
            .expect("writer thread panicked")
            .context("live writer thread failed")?;

        assert_eq!(written.exit_code, 0);
        let output: Value = serde_json::from_str(&written.rendered_output)?;
        assert_eq!(output["state"], "written");
        assert_eq!(output["terminal_reason"], "written");
        assert!(
            output["backup_copied_page_count"]
                .as_u64()
                .unwrap_or_default()
                >= output["backup_total_page_count"]
                    .as_u64()
                    .unwrap_or_default(),
            "completed snapshot must report full backup page coverage"
        );

        let latest_snapshot_path = fixture.snapshot_dir().join("latest.sqlite");
        let latest_manifest: RecentRawJournalSnapshotManifest =
            load_json(&fixture.snapshot_dir().join("latest.json"))?;
        let latest_state = load_snapshot_state(&latest_snapshot_path)?;
        assert_snapshot_manifest_matches_state(
            &latest_manifest,
            &latest_state,
            &latest_snapshot_path,
        )?;
        Ok(())
    }

    #[test]
    fn snapshot_service_and_timer_templates_match_bounded_attempt_contract() -> Result<()> {
        let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let service = std::fs::read_to_string(
            repo_root.join("ops/server_templates/copybot-discovery-recent-raw-snapshot.service"),
        )
        .context("failed reading snapshot service template")?;
        let timer = std::fs::read_to_string(
            repo_root.join("ops/server_templates/copybot-discovery-recent-raw-snapshot.timer"),
        )
        .context("failed reading snapshot timer template")?;
        assert!(
            service.contains("SuccessExitStatus=75"),
            "snapshot service must keep transient deferred outcomes non-fatal for systemd"
        );
        assert!(
            service.contains("TimeoutStartSec=10min"),
            "snapshot service must leave enough outer runtime budget for bounded finalize and retention cleanup"
        );
        assert!(
            timer.contains("OnUnitActiveSec=5m"),
            "snapshot timer should keep periodic retries enabled for bounded deferred runs"
        );
        Ok(())
    }

    #[test]
    fn scheduled_run_snapshots_latest_and_prunes_archives() -> Result<()> {
        let fixture = make_fixture("recent-raw-snapshot")?;
        let first_now = parse_ts("2026-03-23T12:00:00Z")?;
        let second_now = parse_ts("2026-03-23T12:11:00Z")?;
        let third_now = parse_ts("2026-03-23T12:22:00Z")?;
        seed_recent_raw_journal(&fixture.journal_store, third_now)?;

        for now in [first_now, second_now, third_now] {
            run(Config {
                config_path: fixture.config_path.clone(),
                journal_db_path: Some(fixture.journal_db_path.clone()),
                output_path: None,
                scheduled: true,
                force: true,
                json: false,
                now,
            })?;
        }

        let snapshot_dir = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("state/discovery_restore/recent_raw");
        let archives = std::fs::read_dir(&snapshot_dir)?
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name.starts_with("discovery_recent_raw_") && name.ends_with(".sqlite")
                    })
            })
            .collect::<Vec<_>>();
        assert_eq!(archives.len(), 2, "retention should prune oldest snapshot");
        assert!(snapshot_dir.join("latest.sqlite").exists());
        assert!(snapshot_dir.join("latest.json").exists());
        Ok(())
    }

    #[test]
    fn scheduled_run_skips_when_cadence_not_elapsed() -> Result<()> {
        let fixture = make_fixture("recent-raw-snapshot-skip")?;
        let now = parse_ts("2026-03-23T12:00:00Z")?;
        seed_recent_raw_journal(&fixture.journal_store, now)?;
        let snapshot_dir = fixture.snapshot_dir();

        run(Config {
            config_path: fixture.config_path.clone(),
            journal_db_path: Some(fixture.journal_db_path.clone()),
            output_path: None,
            scheduled: true,
            force: false,
            json: false,
            now,
        })?;
        assert!(snapshot_dir.join("latest.sqlite").exists());
        assert!(snapshot_dir.join("latest.json").exists());

        let skipped = run(Config {
            config_path: fixture.config_path.clone(),
            journal_db_path: Some(fixture.journal_db_path.clone()),
            output_path: None,
            scheduled: true,
            force: false,
            json: true,
            now: now + Duration::minutes(2),
        })?;
        assert_eq!(skipped.exit_code, 0);
        let output: Value = serde_json::from_str(&skipped.rendered_output)?;
        assert_eq!(output["state"], "skipped_not_due");
        assert_eq!(output["latest_surface_status"], "healthy");
        assert_eq!(output["latest_surface_action"], "healthy_skip");
        Ok(())
    }

    #[test]
    fn scheduled_run_recreates_latest_sqlite_when_metadata_exists_but_latest_sqlite_missing(
    ) -> Result<()> {
        let fixture = make_fixture("recent-raw-snapshot-self-heal-sqlite")?;
        let now = parse_ts("2026-03-23T12:00:00Z")?;
        seed_recent_raw_journal(&fixture.journal_store, now)?;
        let snapshot_dir = fixture.snapshot_dir();

        run(Config {
            config_path: fixture.config_path.clone(),
            journal_db_path: Some(fixture.journal_db_path.clone()),
            output_path: None,
            scheduled: true,
            force: false,
            json: false,
            now,
        })?;
        let archive_count_before = archive_count(&snapshot_dir)?;
        std::fs::remove_file(snapshot_dir.join("latest.sqlite"))?;

        let healed = run(Config {
            config_path: fixture.config_path.clone(),
            journal_db_path: Some(fixture.journal_db_path.clone()),
            output_path: None,
            scheduled: true,
            force: false,
            json: true,
            now: now + Duration::minutes(2),
        })?;
        assert_eq!(healed.exit_code, 0);
        let output: Value = serde_json::from_str(&healed.rendered_output)?;
        assert_eq!(output["state"], "self_healed_latest_surface");
        assert_eq!(output["latest_surface_status"], "missing_latest_snapshot");
        assert_eq!(
            output["latest_surface_action"],
            "recreated_latest_snapshot_from_archive"
        );
        assert!(snapshot_dir.join("latest.sqlite").exists());
        assert_eq!(archive_count(&snapshot_dir)?, archive_count_before);
        Ok(())
    }

    #[test]
    fn scheduled_run_rewrites_latest_metadata_when_metadata_missing_and_latest_sqlite_exists(
    ) -> Result<()> {
        let fixture = make_fixture("recent-raw-snapshot-self-heal-metadata")?;
        let now = parse_ts("2026-03-23T12:00:00Z")?;
        seed_recent_raw_journal(&fixture.journal_store, now)?;
        let snapshot_dir = fixture.snapshot_dir();

        run(Config {
            config_path: fixture.config_path.clone(),
            journal_db_path: Some(fixture.journal_db_path.clone()),
            output_path: None,
            scheduled: true,
            force: false,
            json: false,
            now,
        })?;
        let archive_count_before = archive_count(&snapshot_dir)?;
        std::fs::remove_file(snapshot_dir.join("latest.json"))?;

        let healed = run(Config {
            config_path: fixture.config_path.clone(),
            journal_db_path: Some(fixture.journal_db_path.clone()),
            output_path: None,
            scheduled: true,
            force: false,
            json: true,
            now: now + Duration::minutes(2),
        })?;
        assert_eq!(healed.exit_code, 0);
        let output: Value = serde_json::from_str(&healed.rendered_output)?;
        assert_eq!(output["state"], "self_healed_latest_surface");
        assert_eq!(output["latest_surface_status"], "missing_latest_metadata");
        assert_eq!(
            output["latest_surface_action"],
            "rewrote_latest_metadata_from_archive"
        );
        assert!(snapshot_dir.join("latest.sqlite").exists());
        assert!(snapshot_dir.join("latest.json").exists());
        assert_eq!(archive_count(&snapshot_dir)?, archive_count_before);
        Ok(())
    }

    #[test]
    fn scheduled_run_retention_still_prunes_after_self_heal_path() -> Result<()> {
        let fixture = make_fixture("recent-raw-snapshot-self-heal-retention")?;
        let first_now = parse_ts("2026-03-23T12:00:00Z")?;
        let second_now = parse_ts("2026-03-23T12:11:00Z")?;
        let third_now = parse_ts("2026-03-23T12:22:00Z")?;
        seed_recent_raw_journal(&fixture.journal_store, third_now)?;
        let snapshot_dir = fixture.snapshot_dir();

        run(Config {
            config_path: fixture.config_path.clone(),
            journal_db_path: Some(fixture.journal_db_path.clone()),
            output_path: None,
            scheduled: true,
            force: true,
            json: false,
            now: first_now,
        })?;
        run(Config {
            config_path: fixture.config_path.clone(),
            journal_db_path: Some(fixture.journal_db_path.clone()),
            output_path: None,
            scheduled: true,
            force: true,
            json: false,
            now: second_now,
        })?;
        std::fs::remove_file(snapshot_dir.join("latest.sqlite"))?;
        let healed = run(Config {
            config_path: fixture.config_path.clone(),
            journal_db_path: Some(fixture.journal_db_path.clone()),
            output_path: None,
            scheduled: true,
            force: false,
            json: true,
            now: second_now + Duration::minutes(2),
        })?;
        assert_eq!(healed.exit_code, 0);
        let healed_output: Value = serde_json::from_str(&healed.rendered_output)?;
        assert_eq!(healed_output["state"], "self_healed_latest_surface");

        run(Config {
            config_path: fixture.config_path.clone(),
            journal_db_path: Some(fixture.journal_db_path.clone()),
            output_path: None,
            scheduled: true,
            force: true,
            json: false,
            now: third_now,
        })?;
        assert_eq!(archive_count(&snapshot_dir)?, 2);
        Ok(())
    }

    #[test]
    fn scheduled_skip_prunes_full_snapshot_sets_and_sidecars() -> Result<()> {
        let fixture = make_fixture("recent-raw-snapshot-full-set-prune")?;
        let now = parse_ts("2026-03-23T12:00:00Z")?;
        seed_recent_raw_journal(&fixture.journal_store, now)?;
        let snapshot_dir = fixture.snapshot_dir();

        run(Config {
            config_path: fixture.config_path.clone(),
            journal_db_path: Some(fixture.journal_db_path.clone()),
            output_path: None,
            scheduled: true,
            force: false,
            json: false,
            now,
        })?;

        seed_fake_archive_set(&snapshot_dir, "20260323T115500Z")?;
        seed_fake_archive_set(&snapshot_dir, "20260323T115000Z")?;

        let skipped = run(Config {
            config_path: fixture.config_path.clone(),
            journal_db_path: Some(fixture.journal_db_path.clone()),
            output_path: None,
            scheduled: true,
            force: false,
            json: true,
            now: now + Duration::minutes(2),
        })?;
        assert_eq!(skipped.exit_code, 0);
        let output: Value = serde_json::from_str(&skipped.rendered_output)?;
        assert_eq!(output["state"], "skipped_not_due");
        assert_eq!(output["archive_set_count_before"], 3);
        assert_eq!(output["archive_set_count_after"], 2);
        assert_eq!(archive_count(&snapshot_dir)?, 2);
        assert_fake_archive_set_absent(&snapshot_dir, "20260323T115000Z");
        assert_fake_archive_set_present(&snapshot_dir, "20260323T115500Z");
        Ok(())
    }

    #[test]
    fn scheduled_failure_before_archive_promotion_keeps_latest_surface_without_archive_growth(
    ) -> Result<()> {
        let fixture = make_fixture("recent-raw-snapshot-pre-promotion-failure")?;
        let now = parse_ts("2026-03-23T12:00:00Z")?;
        seed_recent_raw_journal(&fixture.journal_store, now)?;
        let snapshot_dir = fixture.snapshot_dir();
        let _guard = install_pre_archive_promotion_hook(|_| {
            Err(anyhow::anyhow!("synthetic_pre_archive_promotion_failure"))
        });

        let failed = run(Config {
            config_path: fixture.config_path.clone(),
            journal_db_path: Some(fixture.journal_db_path.clone()),
            output_path: None,
            scheduled: true,
            force: true,
            json: true,
            now,
        })?;
        assert_eq!(failed.exit_code, 1);
        let output: Value = serde_json::from_str(&failed.rendered_output)?;
        assert_eq!(output["state"], "hard_failure");
        assert_eq!(output["archive_promoted"], false);
        assert_eq!(output["archive_set_count_after"], 0);
        let archive_path = PathBuf::from(
            output["archive_path"]
                .as_str()
                .context("archive path must be present for failed scheduled snapshot")?,
        );
        assert!(
            !archive_path.exists(),
            "failed run must not promote archive sqlite"
        );
        assert!(!archive_path.with_extension("json").exists());
        assert!(snapshot_dir.join("latest.sqlite").exists());
        assert!(snapshot_dir.join("latest.json").exists());
        assert_eq!(archive_count(&snapshot_dir)?, 0);
        Ok(())
    }

    #[test]
    fn explicit_run_writes_snapshot_and_manifest() -> Result<()> {
        let fixture = make_fixture("recent-raw-snapshot-explicit")?;
        let now = parse_ts("2026-03-23T12:00:00Z")?;
        seed_recent_raw_journal(&fixture.journal_store, now)?;

        run(Config {
            config_path: fixture.config_path.clone(),
            journal_db_path: Some(fixture.journal_db_path.clone()),
            output_path: Some(PathBuf::from("snapshots/manual.sqlite")),
            scheduled: false,
            force: false,
            json: false,
            now,
        })?;

        let snapshot_path = fixture
            .config_path
            .parent()
            .expect("config parent")
            .join("snapshots/manual.sqlite");
        let manifest_path = snapshot_path.with_extension("json");
        assert!(snapshot_path.exists());
        let manifest: RecentRawJournalSnapshotManifest = load_json(&manifest_path)?;
        assert_eq!(manifest.row_count, 2);

        let snapshot_store = SqliteStore::open_read_only(&snapshot_path)?;
        let state = snapshot_store.recent_raw_journal_state_read_only()?;
        assert_eq!(state.row_count, 2);
        assert_eq!(manifest.covered_since, state.covered_since);
        assert_eq!(
            manifest.covered_through_cursor,
            state.covered_through_cursor
        );
        assert_eq!(
            manifest.last_batch_completed_at,
            state.last_batch_completed_at
        );
        assert_eq!(
            manifest.snapshot_bytes,
            std::fs::metadata(&snapshot_path)?.len()
        );
        Ok(())
    }

    #[test]
    fn scheduled_run_manifest_metadata_matches_snapshot_files_even_if_source_advances_after_publish(
    ) -> Result<()> {
        let fixture = make_fixture("recent-raw-snapshot-drift")?;
        let now = parse_ts("2026-03-23T12:00:00Z")?;
        seed_recent_raw_journal(&fixture.journal_store, now)?;
        let live_journal_db_path = fixture.journal_db_path.clone();
        let live_advance_at = now + Duration::minutes(1);
        let _guard = super::install_post_snapshot_publish_hook(move |_| {
            let live_writer = SqliteStore::open(&live_journal_db_path)?;
            live_writer.insert_recent_raw_journal_batch(
                &[make_swap("sig-c", live_advance_at, 12)],
                live_advance_at,
            )?;
            Ok(())
        });

        let written = run(Config {
            config_path: fixture.config_path.clone(),
            journal_db_path: Some(fixture.journal_db_path.clone()),
            output_path: None,
            scheduled: true,
            force: true,
            json: true,
            now,
        })?;
        assert_eq!(written.exit_code, 0);
        let output: Value = serde_json::from_str(&written.rendered_output)?;
        assert_eq!(output["state"], "written");

        let latest_snapshot_path = fixture.snapshot_dir().join("latest.sqlite");
        let latest_metadata_path = fixture.snapshot_dir().join("latest.json");
        let latest_manifest: RecentRawJournalSnapshotManifest = load_json(&latest_metadata_path)?;
        let latest_state = load_snapshot_state(&latest_snapshot_path)?;
        assert_snapshot_manifest_matches_state(
            &latest_manifest,
            &latest_state,
            &latest_snapshot_path,
        )?;

        let archive_path = PathBuf::from(
            output["archive_path"]
                .as_str()
                .context("archive path must be present for written scheduled snapshot")?,
        );
        let archive_manifest_path = archive_path.with_extension("json");
        let archive_manifest: RecentRawJournalSnapshotManifest = load_json(&archive_manifest_path)?;
        let archive_state = load_snapshot_state(&archive_path)?;
        assert_snapshot_manifest_matches_state(&archive_manifest, &archive_state, &archive_path)?;

        let source_state = SqliteStore::open_read_only(&fixture.journal_db_path)?
            .recent_raw_journal_state_read_only()?;
        assert_eq!(latest_manifest.row_count, 2);
        assert_eq!(archive_manifest.row_count, 2);
        assert_eq!(
            latest_manifest.covered_through_cursor,
            latest_state.covered_through_cursor
        );
        assert_eq!(
            archive_manifest.covered_through_cursor,
            archive_state.covered_through_cursor
        );
        assert!(source_state.row_count > latest_manifest.row_count);
        assert_ne!(
            source_state.covered_through_cursor,
            latest_manifest.covered_through_cursor
        );
        assert_ne!(
            source_state.last_batch_completed_at,
            latest_manifest.last_batch_completed_at
        );
        Ok(())
    }

    struct Fixture {
        journal_store: SqliteStore,
        journal_db_path: PathBuf,
        config_path: PathBuf,
        _temp: tempfile::TempDir,
    }

    fn make_fixture(name: &str) -> Result<Fixture> {
        let temp = tempdir().context("failed to create tempdir")?;
        let journal_db_path = temp.path().join(format!("{name}.db"));
        let config_path = temp.path().join(format!("{name}.toml"));
        let journal_store = SqliteStore::open(&journal_db_path)?;
        std::fs::write(
            &config_path,
            format!(
                "[recent_raw_journal]\npath = \"{}\"\n\n[program_history_validation]\nsource = \"quicknode_blocks_rpc\"\nhttp_url = \"https://example.invalid/blocks\"\nraydium_program_ids = [\"675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8\"]\npumpswap_program_ids = [\"pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA\"]\n\n[runtime_restore_ops]\njournal_snapshot_retention = 2\njournal_snapshot_cadence_minutes = 10\n",
                journal_db_path.display(),
            ),
        )
        .context("failed writing config")?;
        Ok(Fixture {
            journal_store,
            journal_db_path,
            config_path,
            _temp: temp,
        })
    }

    impl Fixture {
        fn snapshot_dir(&self) -> PathBuf {
            self.config_path
                .parent()
                .expect("config parent")
                .join("state/discovery_restore/recent_raw")
        }
    }

    fn seed_recent_raw_journal(store: &SqliteStore, now: DateTime<Utc>) -> Result<()> {
        store.insert_recent_raw_journal_batch(
            &[
                make_swap("sig-a", now - Duration::minutes(2), 10),
                make_swap("sig-b", now - Duration::minutes(1), 11),
            ],
            now,
        )?;
        Ok(())
    }

    fn seed_recent_raw_journal_many(
        store: &SqliteStore,
        now: DateTime<Utc>,
        count: usize,
    ) -> Result<()> {
        let mut swaps = Vec::with_capacity(count);
        for idx in 0..count {
            let ts = now - Duration::seconds((count.saturating_sub(idx)) as i64);
            swaps.push(make_swap(
                &format!("sig-many-{idx:04}"),
                ts,
                10 + idx as u64,
            ));
        }
        store.insert_recent_raw_journal_batch(&swaps, now)?;
        Ok(())
    }

    fn seed_recent_raw_journal_range(
        store: &SqliteStore,
        start: DateTime<Utc>,
        slot_start: u64,
        signature_prefix: &str,
        count: usize,
        completed_at: DateTime<Utc>,
    ) -> Result<()> {
        let mut swaps = Vec::with_capacity(count);
        for idx in 0..count {
            swaps.push(make_swap(
                &format!("{signature_prefix}-{idx:05}"),
                start + Duration::seconds(idx as i64),
                slot_start + idx as u64,
            ));
        }
        store.insert_recent_raw_journal_batch(&swaps, completed_at)?;
        Ok(())
    }

    fn load_snapshot_state(snapshot_path: &std::path::Path) -> Result<RecentRawJournalStateRow> {
        let snapshot_store = SqliteStore::open_read_only(snapshot_path)?;
        snapshot_store.recent_raw_journal_state_read_only()
    }

    fn assert_snapshot_manifest_matches_state(
        manifest: &RecentRawJournalSnapshotManifest,
        state: &RecentRawJournalStateRow,
        snapshot_path: &std::path::Path,
    ) -> Result<()> {
        assert_eq!(manifest.row_count, state.row_count);
        assert_eq!(manifest.covered_since, state.covered_since);
        assert_eq!(
            manifest.covered_through_cursor,
            state.covered_through_cursor
        );
        assert_eq!(
            manifest.last_batch_completed_at,
            state.last_batch_completed_at
        );
        assert_eq!(manifest.updated_at, state.updated_at);
        assert_eq!(
            manifest.snapshot_bytes,
            std::fs::metadata(snapshot_path)?.len()
        );
        Ok(())
    }

    fn make_swap(signature: &str, ts_utc: DateTime<Utc>, slot: u64) -> SwapEvent {
        SwapEvent {
            wallet: "wallet-restore".to_string(),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: format!("token-{signature}"),
            amount_in: 1.0,
            amount_out: 10.0,
            signature: signature.to_string(),
            slot,
            ts_utc,
            exact_amounts: None,
        }
    }

    fn parse_ts(raw: &str) -> Result<DateTime<Utc>> {
        Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
    }

    fn archive_count(snapshot_dir: &PathBuf) -> Result<usize> {
        Ok(std::fs::read_dir(snapshot_dir)?
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name.starts_with("discovery_recent_raw_") && name.ends_with(".sqlite")
                    })
            })
            .count())
    }

    fn staged_artifact_count(snapshot_dir: &Path) -> Result<usize> {
        Ok(std::fs::read_dir(snapshot_dir)?
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name.starts_with(".discovery_recent_raw_")
                            && (name.ends_with(super::STAGED_ARCHIVE_SNAPSHOT_SUFFIX)
                                || name.ends_with(super::STAGED_ARCHIVE_METADATA_SUFFIX))
                    })
            })
            .count())
    }

    fn seed_fake_archive_set(snapshot_dir: &Path, slug: &str) -> Result<()> {
        std::fs::create_dir_all(snapshot_dir)?;
        let sqlite_path = snapshot_dir.join(format!("discovery_recent_raw_{slug}.sqlite"));
        std::fs::write(&sqlite_path, format!("archive-{slug}"))?;
        std::fs::write(
            sqlite_path.with_extension("json"),
            format!("manifest-{slug}"),
        )?;
        std::fs::write(
            super::sqlite_snapshot_wal_path(&sqlite_path),
            format!("wal-{slug}"),
        )?;
        std::fs::write(
            super::sqlite_snapshot_shm_path(&sqlite_path),
            format!("shm-{slug}"),
        )?;
        Ok(())
    }

    fn assert_fake_archive_set_absent(snapshot_dir: &Path, slug: &str) {
        let sqlite_path = snapshot_dir.join(format!("discovery_recent_raw_{slug}.sqlite"));
        assert!(!sqlite_path.exists());
        assert!(!sqlite_path.with_extension("json").exists());
        assert!(!super::sqlite_snapshot_wal_path(&sqlite_path).exists());
        assert!(!super::sqlite_snapshot_shm_path(&sqlite_path).exists());
    }

    fn assert_fake_archive_set_present(snapshot_dir: &Path, slug: &str) {
        let sqlite_path = snapshot_dir.join(format!("discovery_recent_raw_{slug}.sqlite"));
        assert!(sqlite_path.exists());
        assert!(sqlite_path.with_extension("json").exists());
        assert!(super::sqlite_snapshot_wal_path(&sqlite_path).exists());
        assert!(super::sqlite_snapshot_shm_path(&sqlite_path).exists());
    }
}
