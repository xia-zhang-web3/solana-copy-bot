use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::load_from_path;
use copybot_discovery::runtime_restore_ops::{
    copy_atomic, journal_snapshot_archive_path, journal_snapshot_latest_metadata_path,
    journal_snapshot_latest_path, journal_snapshot_metadata_path, load_json,
    prune_rotated_archives, resolve_db_path, resolve_relative_to_config, write_json_atomic,
    JOURNAL_SNAPSHOT_ARCHIVE_PREFIX, JOURNAL_SNAPSHOT_ARCHIVE_SUFFIX,
};
use copybot_storage::{
    DiscoveryRuntimeCursor, RecentRawJournalStateRow, SqliteSnapshotOutcome, SqliteSnapshotPolicy,
    SqliteSnapshotSummary, SqliteStore,
};
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

const USAGE: &str = "usage: discovery_recent_raw_snapshot --config <path> [--journal-db-path <path>] (--output <path> | --scheduled) [--force] [--json] [--now <rfc3339>]";

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
    cadence_minutes: Option<u64>,
    retention: Option<usize>,
    pruned_snapshot_paths: Vec<String>,
    attempt_duration_ms: u64,
    backup_step_count: usize,
    backup_retry_count: usize,
    busy_retry_count: usize,
    locked_retry_count: usize,
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
    HealthySkip,
    RefreshedFromSource,
    RecreatedLatestSnapshotFromArchive,
    RewroteLatestMetadataFromArchive,
    RewroteLatestMetadataFromLatestSqlite,
    RecreatedLatestSurfaceFromSource,
    DeferredDueToRetryableBusy,
    UnchangedDueToRetryableBusy,
    UnchangedDueToHardFailure,
}

impl LatestSurfaceAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::ExplicitOutput => "explicit_output",
            Self::HealthySkip => "healthy_skip",
            Self::RefreshedFromSource => "refreshed_from_source",
            Self::RecreatedLatestSnapshotFromArchive => "recreated_latest_snapshot_from_archive",
            Self::RewroteLatestMetadataFromArchive => "rewrote_latest_metadata_from_archive",
            Self::RewroteLatestMetadataFromLatestSqlite => {
                "rewrote_latest_metadata_from_latest_sqlite"
            }
            Self::RecreatedLatestSurfaceFromSource => "recreated_latest_surface_from_source",
            Self::DeferredDueToRetryableBusy => "deferred_due_to_retryable_busy",
            Self::UnchangedDueToRetryableBusy => "unchanged_due_to_retryable_busy",
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
struct SnapshotExecution {
    rendered_output: String,
    exit_code: i32,
}

#[derive(Debug, Clone)]
enum SnapshotWriteError {
    RetryableBusy {
        summary: SqliteSnapshotSummary,
    },
    HardFailure {
        summary: Option<SqliteSnapshotSummary>,
        reason: String,
    },
}

fn summary_reason(summary: &SqliteSnapshotSummary) -> String {
    summary
        .retry_exhausted_reason
        .map(|reason| reason.as_str().to_string())
        .unwrap_or_else(|| "busy".to_string())
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

fn scheduled_contention_contract(
    latest_surface_status: LatestSurfaceStatus,
    force: bool,
    reason: &str,
) -> (SnapshotState, LatestSurfaceAction, Option<String>) {
    if !force && latest_surface_status == LatestSurfaceStatus::Healthy {
        (
            SnapshotState::Deferred,
            LatestSurfaceAction::DeferredDueToRetryableBusy,
            Some(format!(
                "healthy_latest_surface_retained_after_retryable_{}",
                reason
            )),
        )
    } else {
        (
            SnapshotState::RetryableBusy,
            LatestSurfaceAction::UnchangedDueToRetryableBusy,
            None,
        )
    }
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
    let snapshot_policy = SqliteSnapshotPolicy::default();

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
            &snapshot_policy,
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
            &snapshot_policy,
        ) {
            Ok((manifest, summary)) => {
                match write_json_atomic(&metadata_path, &manifest)
                    .with_context(|| format!("failed writing {}", metadata_path.display()))
                {
                    Ok(()) => render_output(
                        SnapshotState::Written,
                        LatestSurfaceStatus::NotApplicable,
                        LatestSurfaceAction::ExplicitOutput,
                        &config.config_path,
                        &source_db_path,
                        &snapshot_path,
                        &metadata_path,
                        None,
                        None,
                        None,
                        &[],
                        Some(&manifest),
                        Some(&summary),
                        None,
                        None,
                        None,
                    ),
                    Err(error) => render_output(
                        SnapshotState::HardFailure,
                        LatestSurfaceStatus::NotApplicable,
                        LatestSurfaceAction::UnchangedDueToHardFailure,
                        &config.config_path,
                        &source_db_path,
                        &snapshot_path,
                        &metadata_path,
                        None,
                        None,
                        None,
                        &[],
                        Some(&manifest),
                        Some(&summary),
                        None,
                        None,
                        Some(error.to_string()),
                    ),
                }
            }
            Err(SnapshotWriteError::RetryableBusy { summary }) => render_output(
                SnapshotState::RetryableBusy,
                LatestSurfaceStatus::NotApplicable,
                LatestSurfaceAction::UnchangedDueToRetryableBusy,
                &config.config_path,
                &source_db_path,
                &snapshot_path,
                &metadata_path,
                None,
                None,
                None,
                &[],
                None,
                Some(&summary),
                Some(summary_reason(&summary)),
                None,
                None,
            ),
            Err(SnapshotWriteError::HardFailure { summary, reason }) => render_output(
                SnapshotState::HardFailure,
                LatestSurfaceStatus::NotApplicable,
                LatestSurfaceAction::UnchangedDueToHardFailure,
                &config.config_path,
                &source_db_path,
                &snapshot_path,
                &metadata_path,
                None,
                None,
                None,
                &[],
                None,
                summary.as_ref(),
                None,
                None,
                Some(reason),
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
    snapshot_policy: &SqliteSnapshotPolicy,
) -> Result<SnapshotOutput> {
    let snapshot_dir =
        resolve_relative_to_config(&config.config_path, Path::new(configured_snapshot_dir));
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
        let manifest = latest_surface
            .manifest
            .as_ref()
            .expect("healthy latest surface includes manifest");
        return Ok(render_output(
            SnapshotState::SkippedNotDue,
            latest_surface.status,
            LatestSurfaceAction::HealthySkip,
            &config.config_path,
            source_db_path,
            &latest_snapshot_path,
            &latest_metadata_path,
            None,
            Some(cadence_minutes),
            Some(retention),
            &[],
            Some(manifest),
            None,
            None,
            None,
            None,
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
            return Ok(render_output(
                SnapshotState::SelfHealedLatestSurface,
                latest_surface.status,
                action,
                &config.config_path,
                source_db_path,
                &latest_snapshot_path,
                &latest_metadata_path,
                None,
                Some(cadence_minutes),
                Some(retention),
                &[],
                Some(&manifest),
                None,
                None,
                None,
                None,
            ));
        }
    }

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
        snapshot_policy,
        latest_surface.manifest.as_ref(),
        config.force,
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
    snapshot_policy: &SqliteSnapshotPolicy,
    latest_manifest: Option<&RecentRawJournalSnapshotManifest>,
    force: bool,
) -> Result<SnapshotOutput> {
    let archive_path = journal_snapshot_archive_path(snapshot_dir, now);
    let archive_metadata_path = journal_snapshot_metadata_path(&archive_path);
    let (manifest, summary) = match write_snapshot_with_policy(
        source_db_path,
        source_store,
        &archive_path,
        now,
        snapshot_policy,
    ) {
        Ok(result) => result,
        Err(SnapshotWriteError::RetryableBusy { summary }) => {
            let reason = summary_reason(&summary);
            let (state, latest_action, deferred_reason) =
                scheduled_contention_contract(latest_surface_status, force, &reason);
            return Ok(render_output(
                state,
                latest_surface_status,
                latest_action,
                config_path,
                source_db_path,
                latest_snapshot_path,
                latest_metadata_path,
                None,
                Some(cadence_minutes),
                Some(retention),
                &[],
                latest_manifest,
                Some(&summary),
                Some(reason),
                deferred_reason,
                None,
            ));
        }
        Err(SnapshotWriteError::HardFailure { summary, reason }) => {
            return Ok(render_output(
                SnapshotState::HardFailure,
                latest_surface_status,
                LatestSurfaceAction::UnchangedDueToHardFailure,
                config_path,
                source_db_path,
                latest_snapshot_path,
                latest_metadata_path,
                None,
                Some(cadence_minutes),
                Some(retention),
                &[],
                latest_manifest,
                summary.as_ref(),
                None,
                None,
                Some(reason),
            ));
        }
    };
    if let Err(error) = write_json_atomic(&archive_metadata_path, &manifest)
        .with_context(|| format!("failed writing {}", archive_metadata_path.display()))
    {
        return Ok(render_output(
            SnapshotState::HardFailure,
            latest_surface_status,
            LatestSurfaceAction::UnchangedDueToHardFailure,
            config_path,
            source_db_path,
            latest_snapshot_path,
            latest_metadata_path,
            Some(&archive_path),
            Some(cadence_minutes),
            Some(retention),
            &[],
            Some(&manifest),
            Some(&summary),
            None,
            None,
            Some(error.to_string()),
        ));
    }
    if let Err(error) = copy_atomic(&archive_path, latest_snapshot_path)
        .with_context(|| format!("failed updating {}", latest_snapshot_path.display()))
    {
        return Ok(render_output(
            SnapshotState::HardFailure,
            latest_surface_status,
            LatestSurfaceAction::UnchangedDueToHardFailure,
            config_path,
            source_db_path,
            latest_snapshot_path,
            latest_metadata_path,
            Some(&archive_path),
            Some(cadence_minutes),
            Some(retention),
            &[],
            Some(&manifest),
            Some(&summary),
            None,
            None,
            Some(error.to_string()),
        ));
    }
    if let Err(error) = write_json_atomic(latest_metadata_path, &manifest)
        .with_context(|| format!("failed writing {}", latest_metadata_path.display()))
    {
        return Ok(render_output(
            SnapshotState::HardFailure,
            latest_surface_status,
            LatestSurfaceAction::UnchangedDueToHardFailure,
            config_path,
            source_db_path,
            latest_snapshot_path,
            latest_metadata_path,
            Some(&archive_path),
            Some(cadence_minutes),
            Some(retention),
            &[],
            Some(&manifest),
            Some(&summary),
            None,
            None,
            Some(error.to_string()),
        ));
    }

    let pruned = match prune_rotated_archives(
        snapshot_dir,
        JOURNAL_SNAPSHOT_ARCHIVE_PREFIX,
        JOURNAL_SNAPSHOT_ARCHIVE_SUFFIX,
        retention,
    ) {
        Ok(pruned) => pruned,
        Err(error) => {
            return Ok(render_output(
                SnapshotState::HardFailure,
                latest_surface_status,
                LatestSurfaceAction::UnchangedDueToHardFailure,
                config_path,
                source_db_path,
                latest_snapshot_path,
                latest_metadata_path,
                Some(&archive_path),
                Some(cadence_minutes),
                Some(retention),
                &[],
                Some(&manifest),
                Some(&summary),
                None,
                None,
                Some(error.to_string()),
            ));
        }
    };
    for snapshot_path in &pruned {
        let metadata_path = journal_snapshot_metadata_path(snapshot_path);
        if metadata_path.exists() {
            if let Err(error) = fs::remove_file(&metadata_path)
                .with_context(|| format!("failed removing {}", metadata_path.display()))
            {
                return Ok(render_output(
                    SnapshotState::HardFailure,
                    latest_surface_status,
                    LatestSurfaceAction::UnchangedDueToHardFailure,
                    config_path,
                    source_db_path,
                    latest_snapshot_path,
                    latest_metadata_path,
                    Some(&archive_path),
                    Some(cadence_minutes),
                    Some(retention),
                    &[],
                    Some(&manifest),
                    Some(&summary),
                    None,
                    None,
                    Some(error.to_string()),
                ));
            }
        }
    }

    Ok(render_output(
        SnapshotState::Written,
        latest_surface_status,
        action,
        config_path,
        source_db_path,
        latest_snapshot_path,
        latest_metadata_path,
        Some(&archive_path),
        Some(cadence_minutes),
        Some(retention),
        &pruned,
        Some(&manifest),
        Some(&summary),
        None,
        None,
        None,
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
}

#[cfg(test)]
struct SnapshotPublishHookGuard;

#[cfg(test)]
impl Drop for SnapshotPublishHookGuard {
    fn drop(&mut self) {
        POST_SNAPSHOT_PUBLISH_HOOK.with(|slot| {
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
    config_path: &Path,
    source_db_path: &Path,
    snapshot_path: &Path,
    metadata_path: &Path,
    archive_path: Option<&Path>,
    cadence_minutes: Option<u64>,
    retention: Option<usize>,
    pruned_snapshot_paths: &[PathBuf],
    manifest: Option<&RecentRawJournalSnapshotManifest>,
    snapshot_summary: Option<&SqliteSnapshotSummary>,
    retryable_reason: Option<String>,
    deferred_reason: Option<String>,
    hard_failure_reason: Option<String>,
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
        cadence_minutes,
        retention,
        pruned_snapshot_paths: pruned_snapshot_paths
            .iter()
            .map(|path| path.display().to_string())
            .collect(),
        attempt_duration_ms: snapshot_summary
            .map(|summary| summary.duration_ms)
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
        format!("attempt_duration_ms={}", output.attempt_duration_ms),
        format!("backup_step_count={}", output.backup_step_count),
        format!("backup_retry_count={}", output.backup_retry_count),
        format!("busy_retry_count={}", output.busy_retry_count),
        format!("locked_retry_count={}", output.locked_retry_count),
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
    use super::{parse_args_from, run, Config, RecentRawJournalSnapshotManifest, SqliteStore};
    use anyhow::{Context, Result};
    use chrono::{DateTime, Duration, Utc};
    use copybot_core_types::SwapEvent;
    use copybot_discovery::runtime_restore_ops::load_json;
    use copybot_storage::RecentRawJournalStateRow;
    use serde_json::Value;
    use std::path::PathBuf;
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
    fn scheduled_contention_contract_defers_when_latest_surface_is_healthy() {
        let (state, action, deferred_reason) = super::scheduled_contention_contract(
            super::LatestSurfaceStatus::Healthy,
            false,
            "busy",
        );
        assert_eq!(state, super::SnapshotState::Deferred);
        assert_eq!(
            action,
            super::LatestSurfaceAction::DeferredDueToRetryableBusy
        );
        assert_eq!(
            deferred_reason.as_deref(),
            Some("healthy_latest_surface_retained_after_retryable_busy")
        );
    }

    #[test]
    fn scheduled_contention_contract_returns_retryable_busy_without_healthy_latest_surface() {
        let (state, action, deferred_reason) = super::scheduled_contention_contract(
            super::LatestSurfaceStatus::MissingBoth,
            false,
            "locked",
        );
        assert_eq!(state, super::SnapshotState::RetryableBusy);
        assert_eq!(
            action,
            super::LatestSurfaceAction::UnchangedDueToRetryableBusy
        );
        assert!(deferred_reason.is_none());
    }

    #[test]
    fn snapshot_state_exit_codes_keep_transient_contention_distinct_from_hard_failure() {
        assert_eq!(super::SnapshotState::Deferred.exit_code(), 75);
        assert_eq!(super::SnapshotState::RetryableBusy.exit_code(), 75);
        assert_eq!(super::SnapshotState::HardFailure.exit_code(), 1);
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
}
