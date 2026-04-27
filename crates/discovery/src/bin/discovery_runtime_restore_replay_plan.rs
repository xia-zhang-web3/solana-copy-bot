use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use copybot_config::load_from_path;
use copybot_discovery::{runtime_restore_ops::resolve_db_path, DiscoveryService};
use copybot_storage::{
    DiscoveryRuntimeArtifact, DiscoveryRuntimeCursor, RecentRawJournalStateRow, SqliteStore,
};
use serde::Serialize;
use serde_json::Value;
use std::env;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::time::Instant;

const USAGE: &str = "usage: discovery_runtime_restore_replay_plan --config <path> --artifact <path> --journal-db-path <path> --gap-fill-db-path <path> --gap-fill-progress-path <path> --gap-fill-window-start-utc <rfc3339> --gap-fill-window-end-utc <rfc3339> --db-path <non-production target db path> --json [--allow-existing-target-db] [--min-free-gb <n>] [--estimate-only]";

const BYTES_PER_GB: u64 = 1024 * 1024 * 1024;
const ACCEPTED_RESIDUE_POLICY: &str = "accepted_irreducible_boundary_residue";
const FULL_REPLAYABLE_POLICY: &str = "full_replayable_coverage";
const IRREDUCIBLE_VERDICT: &str = "not_proven_due_to_irreducible_boundary_evidence";
const IRREDUCIBLE_REASON: &str =
    "program_history_gap_fill_repair_explicit_missing_segments_irreducible_boundary_evidence_remains";
const IRREDUCIBLE_PHASE: &str = "completed_with_explicit_missing_segments";
const ZERO_PROGRESS_ROOT_REASON: &str =
    "program_history_gap_fill_skipped_persistently_provider_blocked_slot_after_bounded_retries";
const BOUNDARY_PREFIX_REASON: &str =
    "requested_window_prefix_uncovered_after_start_slot_adjustment";
const BOUNDARY_SUFFIX_REASON: &str = "requested_window_suffix_uncovered_after_end_slot_adjustment";
const MAX_ACCEPTED_RESIDUE_TOTAL_MS: i64 = 10_000;
const MAX_ACCEPTED_RESIDUE_SEGMENT_MS: i64 = 1_000;

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let report = build_report(&config)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report)
            .context("failed serializing restore replay plan json")?
    );
    if !report.restore_replay_plan_ready {
        std::process::exit(1);
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    config_path: PathBuf,
    artifact_path: PathBuf,
    journal_db_path: PathBuf,
    gap_fill_db_path: PathBuf,
    gap_fill_progress_path: PathBuf,
    gap_fill_window_start_utc: DateTime<Utc>,
    gap_fill_window_end_utc: DateTime<Utc>,
    db_path: PathBuf,
    allow_existing_target_db: bool,
    min_free_gb: u64,
    estimate_only: bool,
    json: bool,
    now: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct RestoreReplayPlanReport {
    event: &'static str,
    config_path: String,
    configured_runtime_db_path: String,
    target_runtime_db_path: String,
    target_db_explicit: bool,
    target_db_matches_configured_runtime_db: bool,
    target_db_exists: Option<bool>,
    allow_existing_target_db: bool,
    target_db_guard_passed: bool,
    target_db_guard_reason: String,
    target_runtime_artifact_path: String,
    target_runtime_artifact_exists: bool,
    artifact_freshness_assessed: bool,
    fresh_under_export_gate: Option<bool>,
    fresh_under_current_gate: Option<bool>,
    normal_restore_requires_bootstrap_degraded: Option<bool>,
    artifact_runtime_cursor: Option<ReportCursor>,
    gap_fill_acceptance_policy: Option<String>,
    accepted_residue_policy: Option<String>,
    accepted_irreducible_boundary_residue: bool,
    gap_fill_gate_valid_for_restore_review: bool,
    gap_fill_gate_reason: String,
    gap_fill_staged_rows: Option<usize>,
    gap_fill_fetched_rows: Option<usize>,
    rows_withheld_due_to_incomplete_outcome: Option<usize>,
    gap_fill_covered_since: Option<String>,
    gap_fill_covered_through_ts_utc: Option<String>,
    missing_segments_count: Option<usize>,
    journal_db_stats: DbStatsReport,
    gap_fill_db_stats: DbStatsReport,
    scoring_window_days: i64,
    required_window_start: String,
    disk_guard: DiskGuardReport,
    estimate_only: bool,
    restore_replay_plan_ready: bool,
    restore_replay_plan_reason: String,
    recommended_restore_command: String,
    recommended_next_operator_action: String,
    production_green: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct ReportCursor {
    ts_utc: String,
    slot: u64,
    signature: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct DbStatsReport {
    path: String,
    exists: bool,
    file_size_bytes: Option<u64>,
    row_count: Option<usize>,
    covered_since: Option<String>,
    covered_through: Option<ReportCursor>,
    estimate_query_ms: Option<u64>,
    stats_readable: bool,
    stats_source: &'static str,
    query_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct DiskGuardReport {
    guard_path: String,
    free_bytes: Option<u64>,
    min_free_bytes: u64,
    estimated_required_bytes: Option<u64>,
    disk_guard_passed: bool,
    disk_guard_reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TargetDbGuard {
    matches_configured_runtime_db: bool,
    exists: Option<bool>,
    passed: bool,
    reason: String,
}

#[derive(Debug, Clone)]
struct GapFillReview {
    valid_for_restore_review: bool,
    reason: String,
    accepted_residue_policy: Option<&'static str>,
    accepted_irreducible_boundary_residue: bool,
    snapshot: GapFillSnapshot,
}

#[derive(Debug, Clone)]
struct GapFillSnapshot {
    verdict: String,
    reason: String,
    current_phase: String,
    replayable_output: bool,
    requested_window_start: DateTime<Utc>,
    requested_window_end: DateTime<Utc>,
    covered_since: Option<DateTime<Utc>>,
    covered_through: Option<DateTime<Utc>>,
    missing_segments: Vec<MissingSegment>,
    fetched_rows: Option<usize>,
    staged_rows: Option<usize>,
    inserted_rows: usize,
    rows_withheld_due_to_incomplete_outcome: usize,
}

#[derive(Debug, Clone)]
struct MissingSegment {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    reason: String,
}

#[derive(Debug, Clone, Default)]
struct ResidueStats {
    target_boundary_segments_count: usize,
    zero_progress_root_segments_count: usize,
    unknown_non_target_segments_count: usize,
    total_boundary_missing_ms: i64,
    max_boundary_segment_ms: i64,
    start_coverage_deficit_ms: Option<i64>,
    end_coverage_deficit_ms: Option<i64>,
    total_accepted_residue_ms: Option<i64>,
    max_accepted_residue_segment_or_deficit_ms: Option<i64>,
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
    let mut artifact_path: Option<PathBuf> = None;
    let mut journal_db_path: Option<PathBuf> = None;
    let mut gap_fill_db_path: Option<PathBuf> = None;
    let mut gap_fill_progress_path: Option<PathBuf> = None;
    let mut gap_fill_window_start_utc: Option<DateTime<Utc>> = None;
    let mut gap_fill_window_end_utc: Option<DateTime<Utc>> = None;
    let mut db_path: Option<PathBuf> = None;
    let mut allow_existing_target_db = false;
    let mut min_free_gb: u64 = 100;
    let mut estimate_only = true;
    let mut json = false;
    let mut now: Option<DateTime<Utc>> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--artifact" => {
                artifact_path = Some(PathBuf::from(parse_string_arg("--artifact", args.next())?))
            }
            "--journal-db-path" => {
                journal_db_path = Some(PathBuf::from(parse_string_arg(
                    "--journal-db-path",
                    args.next(),
                )?))
            }
            "--gap-fill-db-path" => {
                gap_fill_db_path = Some(PathBuf::from(parse_string_arg(
                    "--gap-fill-db-path",
                    args.next(),
                )?))
            }
            "--gap-fill-progress-path" => {
                gap_fill_progress_path = Some(PathBuf::from(parse_string_arg(
                    "--gap-fill-progress-path",
                    args.next(),
                )?))
            }
            "--gap-fill-window-start-utc" => {
                gap_fill_window_start_utc =
                    Some(parse_ts_arg("--gap-fill-window-start-utc", args.next())?)
            }
            "--gap-fill-window-end-utc" => {
                gap_fill_window_end_utc =
                    Some(parse_ts_arg("--gap-fill-window-end-utc", args.next())?)
            }
            "--db-path" => {
                db_path = Some(PathBuf::from(parse_string_arg("--db-path", args.next())?))
            }
            "--allow-existing-target-db" => allow_existing_target_db = true,
            "--min-free-gb" => {
                min_free_gb = parse_u64_arg("--min-free-gb", args.next())?;
            }
            "--estimate-only" => estimate_only = true,
            "--now" => now = Some(parse_ts_arg("--now", args.next())?),
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}\n{USAGE}"),
        }
    }

    if !json {
        bail!("missing required --json\n{USAGE}");
    }
    let gap_fill_window_start_utc = gap_fill_window_start_utc
        .ok_or_else(|| anyhow!("missing --gap-fill-window-start-utc\n{USAGE}"))?;
    let gap_fill_window_end_utc = gap_fill_window_end_utc
        .ok_or_else(|| anyhow!("missing --gap-fill-window-end-utc\n{USAGE}"))?;
    if gap_fill_window_end_utc <= gap_fill_window_start_utc {
        bail!("--gap-fill-window-end-utc must be after --gap-fill-window-start-utc");
    }

    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config\n{USAGE}"))?,
        artifact_path: artifact_path
            .ok_or_else(|| anyhow!("missing required --artifact\n{USAGE}"))?,
        journal_db_path: journal_db_path
            .ok_or_else(|| anyhow!("missing required --journal-db-path\n{USAGE}"))?,
        gap_fill_db_path: gap_fill_db_path
            .ok_or_else(|| anyhow!("missing required --gap-fill-db-path\n{USAGE}"))?,
        gap_fill_progress_path: gap_fill_progress_path
            .ok_or_else(|| anyhow!("missing required --gap-fill-progress-path\n{USAGE}"))?,
        gap_fill_window_start_utc,
        gap_fill_window_end_utc,
        db_path: db_path.ok_or_else(|| anyhow!("missing required --db-path\n{USAGE}"))?,
        allow_existing_target_db,
        min_free_gb,
        estimate_only,
        json,
        now: now.unwrap_or_else(Utc::now),
    }))
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    let raw = value.ok_or_else(|| anyhow!("missing value for {flag}"))?;
    let trimmed = raw.trim().to_string();
    if trimmed.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(trimmed)
}

fn parse_u64_arg(flag: &str, value: Option<String>) -> Result<u64> {
    parse_string_arg(flag, value)?
        .parse::<u64>()
        .with_context(|| format!("invalid integer for {flag}"))
}

fn parse_ts_arg(flag: &str, value: Option<String>) -> Result<DateTime<Utc>> {
    let raw = parse_string_arg(flag, value)?;
    parse_ts(&raw).with_context(|| format!("invalid timestamp for {flag}: {raw}"))
}

fn parse_ts(raw: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(raw)?.with_timezone(&Utc))
}

fn build_report(config: &Config) -> Result<RestoreReplayPlanReport> {
    let loaded_config = load_from_path(&config.config_path)
        .with_context(|| format!("failed loading config {}", config.config_path.display()))?;
    let discovery = DiscoveryService::new(
        loaded_config.discovery.clone(),
        loaded_config.shadow.clone(),
    );
    let configured_runtime_db_path = absolute_lexical_path(&resolve_db_path(
        &config.config_path,
        None,
        &loaded_config.sqlite.path,
    ))?;
    let target_runtime_db_path = absolute_lexical_path(&resolve_db_path(
        &config.config_path,
        Some(config.db_path.as_path()),
        &loaded_config.sqlite.path,
    ))?;
    let target_db_guard = target_db_guard_report(
        &configured_runtime_db_path,
        &target_runtime_db_path,
        config.allow_existing_target_db,
    );
    let scoring_window_days = loaded_config.discovery.scoring_window_days.max(1) as i64;
    let required_window_start = config.now - Duration::days(scoring_window_days);

    let artifact_file_size = file_size(&config.artifact_path);
    let artifact_result = load_artifact(&config.artifact_path);
    let (artifact_freshness_assessed, freshness, artifact_runtime_cursor, artifact_error) =
        match artifact_result {
            Ok(artifact) => {
                let freshness = discovery.assess_runtime_artifact_freshness(&artifact, config.now);
                (
                    true,
                    Some(freshness),
                    Some(artifact.runtime_cursor.clone()),
                    None,
                )
            }
            Err(error) => (false, None, None, Some(error.to_string())),
        };
    let normal_restore_requires_bootstrap_degraded = freshness.as_ref().map(|freshness| {
        !(freshness.fresh_under_export_gate && freshness.fresh_under_current_gate)
    });

    let gap_fill_review = load_gap_fill_review(config)?;
    let journal_stats = read_db_stats(&config.journal_db_path);
    let gap_fill_stats = read_db_stats(&config.gap_fill_db_path);
    let estimated_required_bytes = estimated_required_bytes(
        artifact_file_size,
        journal_stats.file_size_bytes,
        gap_fill_stats.file_size_bytes,
    );
    let disk_guard = disk_guard_report(
        &target_runtime_db_path,
        config.min_free_gb,
        estimated_required_bytes,
    );
    let (ready, reason) = plan_readiness(
        config,
        artifact_error.as_deref(),
        &gap_fill_review,
        &journal_stats,
        &gap_fill_stats,
        &disk_guard,
        &target_db_guard,
    );
    let recommended_restore_command = if target_db_guard.passed {
        recommended_restore_command(
            config,
            &target_runtime_db_path,
            normal_restore_requires_bootstrap_degraded == Some(true),
        )
    } else {
        format!("not_available:{}", target_db_guard.reason)
    };
    let recommended_next_operator_action = if ready {
        "restore replay plan is ready; run the recommended command only after human approval"
            .to_string()
    } else {
        format!("do not run restore yet; resolve {reason} and rerun this read-only plan")
    };

    Ok(RestoreReplayPlanReport {
        event: "discovery_runtime_restore_replay_plan",
        config_path: config.config_path.display().to_string(),
        configured_runtime_db_path: configured_runtime_db_path.display().to_string(),
        target_runtime_db_path: target_runtime_db_path.display().to_string(),
        target_db_explicit: true,
        target_db_matches_configured_runtime_db: target_db_guard.matches_configured_runtime_db,
        target_db_exists: target_db_guard.exists,
        allow_existing_target_db: config.allow_existing_target_db,
        target_db_guard_passed: target_db_guard.passed,
        target_db_guard_reason: target_db_guard.reason,
        target_runtime_artifact_path: config.artifact_path.display().to_string(),
        target_runtime_artifact_exists: config.artifact_path.exists(),
        artifact_freshness_assessed,
        fresh_under_export_gate: freshness
            .as_ref()
            .map(|freshness| freshness.fresh_under_export_gate),
        fresh_under_current_gate: freshness
            .as_ref()
            .map(|freshness| freshness.fresh_under_current_gate),
        normal_restore_requires_bootstrap_degraded,
        artifact_runtime_cursor: artifact_runtime_cursor.as_ref().map(report_cursor),
        gap_fill_acceptance_policy: gap_fill_review
            .accepted_residue_policy
            .map(ToString::to_string),
        accepted_residue_policy: gap_fill_review
            .accepted_irreducible_boundary_residue
            .then(|| ACCEPTED_RESIDUE_POLICY.to_string()),
        accepted_irreducible_boundary_residue: gap_fill_review
            .accepted_irreducible_boundary_residue,
        gap_fill_gate_valid_for_restore_review: gap_fill_review.valid_for_restore_review,
        gap_fill_gate_reason: gap_fill_review.reason.clone(),
        gap_fill_staged_rows: gap_fill_review.snapshot.staged_rows,
        gap_fill_fetched_rows: gap_fill_review.snapshot.fetched_rows,
        rows_withheld_due_to_incomplete_outcome: Some(
            gap_fill_review
                .snapshot
                .rows_withheld_due_to_incomplete_outcome,
        ),
        gap_fill_covered_since: gap_fill_review.snapshot.covered_since.map(format_ts),
        gap_fill_covered_through_ts_utc: gap_fill_review.snapshot.covered_through.map(format_ts),
        missing_segments_count: Some(gap_fill_review.snapshot.missing_segments.len()),
        journal_db_stats: journal_stats,
        gap_fill_db_stats: gap_fill_stats,
        scoring_window_days,
        required_window_start: format_ts(required_window_start),
        disk_guard,
        estimate_only: config.estimate_only,
        restore_replay_plan_ready: ready,
        restore_replay_plan_reason: reason,
        recommended_restore_command,
        recommended_next_operator_action,
        production_green: false,
    })
}

fn absolute_lexical_path(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()
            .context("failed resolving current directory for restore replay plan path")?
            .join(path)
    };
    Ok(normalize_lexical_path(&absolute))
}

fn normalize_lexical_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() && !path.is_absolute() {
                    normalized.push("..");
                }
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    if normalized.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        normalized
    }
}

fn target_db_guard_report(
    configured_runtime_db_path: &Path,
    target_runtime_db_path: &Path,
    allow_existing_target_db: bool,
) -> TargetDbGuard {
    let matches_configured_runtime_db = target_runtime_db_path == configured_runtime_db_path;
    if matches_configured_runtime_db {
        return TargetDbGuard {
            matches_configured_runtime_db,
            exists: target_runtime_db_path.try_exists().ok(),
            passed: false,
            reason: "restore_replay_plan_target_db_matches_configured_runtime_db".to_string(),
        };
    }

    match target_runtime_db_path.try_exists() {
        Ok(true) if !allow_existing_target_db => TargetDbGuard {
            matches_configured_runtime_db,
            exists: Some(true),
            passed: false,
            reason: "restore_replay_plan_target_db_already_exists".to_string(),
        },
        Ok(exists) => TargetDbGuard {
            matches_configured_runtime_db,
            exists: Some(exists),
            passed: true,
            reason: if exists {
                "restore_replay_plan_existing_target_db_explicitly_allowed".to_string()
            } else {
                "restore_replay_plan_target_db_fresh_non_production_path_verified".to_string()
            },
        },
        Err(error) => TargetDbGuard {
            matches_configured_runtime_db,
            exists: None,
            passed: false,
            reason: format!("restore_replay_plan_target_db_existence_unproven:{error:#}"),
        },
    }
}

fn load_artifact(path: &Path) -> Result<DiscoveryRuntimeArtifact> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed reading runtime artifact {}", path.display()))?;
    serde_json::from_str(&raw)
        .with_context(|| format!("failed parsing runtime artifact {}", path.display()))
}

fn read_db_stats(path: &Path) -> DbStatsReport {
    let exists = path.exists();
    let file_size_bytes = file_size(path);
    if !exists {
        return DbStatsReport {
            path: path.display().to_string(),
            exists,
            file_size_bytes,
            row_count: None,
            covered_since: None,
            covered_through: None,
            estimate_query_ms: None,
            stats_readable: false,
            stats_source: "recent_raw_journal_state_cached_read_only_required",
            query_error: Some("database file is missing".to_string()),
        };
    }

    let started = Instant::now();
    let state = SqliteStore::open_read_only(path)
        .and_then(|store| store.recent_raw_journal_state_cached_read_only_required());
    let estimate_query_ms = elapsed_ms(started);
    match state {
        Ok(state) => db_stats_from_state(path, exists, file_size_bytes, estimate_query_ms, state),
        Err(error) => DbStatsReport {
            path: path.display().to_string(),
            exists,
            file_size_bytes,
            row_count: None,
            covered_since: None,
            covered_through: None,
            estimate_query_ms: Some(estimate_query_ms),
            stats_readable: false,
            stats_source: "recent_raw_journal_state_cached_read_only_required",
            query_error: Some(format!("{error:#}")),
        },
    }
}

fn db_stats_from_state(
    path: &Path,
    exists: bool,
    file_size_bytes: Option<u64>,
    estimate_query_ms: u64,
    state: RecentRawJournalStateRow,
) -> DbStatsReport {
    DbStatsReport {
        path: path.display().to_string(),
        exists,
        file_size_bytes,
        row_count: Some(state.row_count),
        covered_since: state.covered_since.map(format_ts),
        covered_through: state.covered_through_cursor.as_ref().map(report_cursor),
        estimate_query_ms: Some(estimate_query_ms),
        stats_readable: true,
        stats_source: "recent_raw_journal_state_cached_read_only_required",
        query_error: None,
    }
}

fn file_size(path: &Path) -> Option<u64> {
    fs::metadata(path).ok().map(|metadata| metadata.len())
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u64::MAX as u128) as u64
}

fn estimated_required_bytes(
    artifact_file_size: Option<u64>,
    journal_file_size: Option<u64>,
    gap_fill_file_size: Option<u64>,
) -> Option<u64> {
    Some(
        artifact_file_size?
            .saturating_add(journal_file_size?)
            .saturating_add(gap_fill_file_size?),
    )
}

fn disk_guard_report(
    target_runtime_db_path: &Path,
    min_free_gb: u64,
    estimated_required_bytes: Option<u64>,
) -> DiskGuardReport {
    let guard_path = existing_ancestor(
        target_runtime_db_path
            .parent()
            .unwrap_or_else(|| Path::new(".")),
    );
    let min_free_bytes = min_free_gb.saturating_mul(BYTES_PER_GB);
    match free_disk_bytes(&guard_path) {
        Ok(free_bytes) => {
            let disk_guard_passed = free_bytes >= min_free_bytes;
            DiskGuardReport {
                guard_path: guard_path.display().to_string(),
                free_bytes: Some(free_bytes),
                min_free_bytes,
                estimated_required_bytes,
                disk_guard_passed,
                disk_guard_reason: if disk_guard_passed {
                    "restore_replay_plan_disk_guard_passed".to_string()
                } else {
                    "restore_replay_plan_disk_guard_free_space_below_threshold".to_string()
                },
            }
        }
        Err(error) => DiskGuardReport {
            guard_path: guard_path.display().to_string(),
            free_bytes: None,
            min_free_bytes,
            estimated_required_bytes,
            disk_guard_passed: false,
            disk_guard_reason: format!("restore_replay_plan_disk_guard_unproven:{error:#}"),
        },
    }
}

fn existing_ancestor(path: &Path) -> PathBuf {
    let mut candidate = path.to_path_buf();
    loop {
        if candidate.exists() {
            return candidate;
        }
        if !candidate.pop() {
            return PathBuf::from(".");
        }
    }
}

fn free_disk_bytes(path: &Path) -> Result<u64> {
    let output = Command::new("df")
        .arg("-Pk")
        .arg(path)
        .output()
        .with_context(|| format!("failed running df for {}", path.display()))?;
    if !output.status.success() {
        bail!(
            "df failed for {}: {}",
            path.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let stdout = String::from_utf8(output.stdout).context("df output was not utf-8")?;
    let line = stdout
        .lines()
        .nth(1)
        .ok_or_else(|| anyhow!("df output did not include a data row"))?;
    let available_kb = line
        .split_whitespace()
        .nth(3)
        .ok_or_else(|| anyhow!("df output missing Available column"))?
        .parse::<u64>()
        .context("df Available column was not an integer")?;
    Ok(available_kb.saturating_mul(1024))
}

fn plan_readiness(
    config: &Config,
    artifact_error: Option<&str>,
    gap_fill_review: &GapFillReview,
    journal_stats: &DbStatsReport,
    gap_fill_stats: &DbStatsReport,
    disk_guard: &DiskGuardReport,
    target_db_guard: &TargetDbGuard,
) -> (bool, String) {
    if !target_db_guard.passed {
        return (false, target_db_guard.reason.clone());
    }
    if !config.artifact_path.exists() {
        return (false, "restore_replay_plan_missing_artifact".to_string());
    }
    if let Some(error) = artifact_error {
        return (
            false,
            format!("restore_replay_plan_artifact_unreadable_or_malformed:{error}"),
        );
    }
    if !config.journal_db_path.exists() {
        return (false, "restore_replay_plan_missing_journal_db".to_string());
    }
    if !config.gap_fill_db_path.exists() {
        return (false, "restore_replay_plan_missing_gap_fill_db".to_string());
    }
    if !gap_fill_review.valid_for_restore_review {
        return (
            false,
            format!(
                "restore_replay_plan_gap_fill_gate_failed:{}",
                gap_fill_review.reason
            ),
        );
    }
    if !disk_guard.disk_guard_passed {
        return (false, disk_guard.disk_guard_reason.clone());
    }
    if !journal_stats.stats_readable {
        return (
            false,
            "restore_replay_plan_journal_stats_unreadable".to_string(),
        );
    }
    if !gap_fill_stats.stats_readable {
        return (
            false,
            "restore_replay_plan_gap_fill_stats_unreadable".to_string(),
        );
    }
    (
        true,
        "restore_replay_plan_ready_read_only_inputs_verified".to_string(),
    )
}

fn recommended_restore_command(
    config: &Config,
    target_runtime_db_path: &Path,
    include_bootstrap_degraded: bool,
) -> String {
    let mut parts = vec![
        "discovery_runtime_restore".to_string(),
        "--config".to_string(),
        shell_arg(&config.config_path),
        "--artifact".to_string(),
        shell_arg(&config.artifact_path),
        "--db-path".to_string(),
        shell_arg(target_runtime_db_path),
        "--journal-db-path".to_string(),
        shell_arg(&config.journal_db_path),
        "--gap-fill-db-path".to_string(),
        shell_arg(&config.gap_fill_db_path),
        "--gap-fill-progress-path".to_string(),
        shell_arg(&config.gap_fill_progress_path),
        "--gap-fill-window-start-utc".to_string(),
        shell_quote(&format_ts(config.gap_fill_window_start_utc)),
        "--gap-fill-window-end-utc".to_string(),
        shell_quote(&format_ts(config.gap_fill_window_end_utc)),
    ];
    if include_bootstrap_degraded {
        parts.push("--bootstrap-degraded".to_string());
    }
    if config.json {
        parts.push("--json".to_string());
    }
    parts.join(" ")
}

fn shell_arg(path: &Path) -> String {
    shell_quote(&path.display().to_string())
}

fn shell_quote(raw: &str) -> String {
    format!("'{}'", raw.replace('\'', "'\\''"))
}

fn load_gap_fill_review(config: &Config) -> Result<GapFillReview> {
    let raw = fs::read_to_string(&config.gap_fill_progress_path).with_context(|| {
        format!(
            "failed reading gap-fill progress {}",
            config.gap_fill_progress_path.display()
        )
    })?;
    let value = serde_json::from_str::<Value>(&raw).with_context(|| {
        format!(
            "failed parsing gap-fill progress {}",
            config.gap_fill_progress_path.display()
        )
    })?;
    let snapshot = gap_fill_snapshot_from_value(&value)?;
    let (
        valid_for_restore_review,
        reason,
        accepted_residue_policy,
        accepted_irreducible_boundary_residue,
    ) = gap_fill_review_validity(
        &snapshot,
        config.gap_fill_window_start_utc,
        config.gap_fill_window_end_utc,
    );
    Ok(GapFillReview {
        valid_for_restore_review,
        reason,
        accepted_residue_policy,
        accepted_irreducible_boundary_residue,
        snapshot,
    })
}

fn gap_fill_snapshot_from_value(value: &Value) -> Result<GapFillSnapshot> {
    let verdict = required_string(value, "verdict")?;
    let reason = required_string(value, "reason")?;
    let current_phase = required_string(value, "current_phase")?;
    let replayable_output = required_bool(value, "replayable_output")?;
    let requested_window_start = required_ts(value, "requested_window_start")?;
    let requested_window_end = required_ts(value, "requested_window_end")?;
    let missing_segments = required_missing_segments(value)?;
    let inserted_rows = required_usize(value, "inserted_rows")?;
    let rows_withheld_due_to_incomplete_outcome =
        required_usize(value, "rows_withheld_due_to_incomplete_outcome")?;
    Ok(GapFillSnapshot {
        verdict,
        reason,
        current_phase,
        replayable_output,
        requested_window_start,
        requested_window_end,
        covered_since: optional_ts(value, "gap_fill_covered_since")?,
        covered_through: optional_cursor_ts(value, "gap_fill_covered_through_cursor")?,
        missing_segments,
        fetched_rows: optional_usize(value, "fetched_rows")?,
        staged_rows: optional_usize(value, "staged_rows")?,
        inserted_rows,
        rows_withheld_due_to_incomplete_outcome,
    })
}

fn gap_fill_review_validity(
    snapshot: &GapFillSnapshot,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
) -> (bool, String, Option<&'static str>, bool) {
    if snapshot.requested_window_start != window_start {
        return invalid_gap_fill("program_history_restore_replay_plan_window_start_mismatch");
    }
    if snapshot.requested_window_end != window_end {
        return invalid_gap_fill("program_history_restore_replay_plan_window_end_mismatch");
    }
    if snapshot.replayable_output {
        return full_replayable_validity(snapshot, window_end);
    }
    accepted_residue_validity(snapshot)
}

fn full_replayable_validity(
    snapshot: &GapFillSnapshot,
    window_end: DateTime<Utc>,
) -> (bool, String, Option<&'static str>, bool) {
    if snapshot
        .covered_through
        .is_none_or(|covered| covered < window_end)
    {
        return invalid_gap_fill("program_history_restore_replay_plan_replayable_covered_short");
    }
    if !snapshot.missing_segments.is_empty() {
        return invalid_gap_fill("program_history_restore_replay_plan_replayable_missing_segments");
    }
    if snapshot.inserted_rows == 0 {
        return invalid_gap_fill("program_history_restore_replay_plan_replayable_no_inserted_rows");
    }
    if snapshot.rows_withheld_due_to_incomplete_outcome != 0 {
        return invalid_gap_fill("program_history_restore_replay_plan_replayable_withheld_rows");
    }
    (
        true,
        "program_history_restore_replay_plan_gap_fill_full_replayable_valid".to_string(),
        Some(FULL_REPLAYABLE_POLICY),
        false,
    )
}

fn accepted_residue_validity(
    snapshot: &GapFillSnapshot,
) -> (bool, String, Option<&'static str>, bool) {
    if snapshot.verdict != IRREDUCIBLE_VERDICT {
        return invalid_gap_fill("program_history_restore_replay_plan_residue_wrong_verdict");
    }
    if snapshot.reason != IRREDUCIBLE_REASON {
        return invalid_gap_fill("program_history_restore_replay_plan_residue_wrong_reason");
    }
    if snapshot.current_phase != IRREDUCIBLE_PHASE {
        return invalid_gap_fill("program_history_restore_replay_plan_residue_wrong_phase");
    }
    if snapshot.inserted_rows != 0 {
        return invalid_gap_fill(
            "program_history_restore_replay_plan_residue_inserted_rows_present",
        );
    }
    let Some(staged_rows) = snapshot.staged_rows else {
        return invalid_gap_fill("program_history_restore_replay_plan_residue_missing_staged_rows");
    };
    let Some(fetched_rows) = snapshot.fetched_rows else {
        return invalid_gap_fill(
            "program_history_restore_replay_plan_residue_missing_fetched_rows",
        );
    };
    if staged_rows == 0 {
        return invalid_gap_fill("program_history_restore_replay_plan_residue_staged_rows_zero");
    }
    if fetched_rows != staged_rows {
        return invalid_gap_fill(
            "program_history_restore_replay_plan_residue_fetched_rows_mismatch",
        );
    }
    if snapshot.rows_withheld_due_to_incomplete_outcome != staged_rows {
        return invalid_gap_fill(
            "program_history_restore_replay_plan_residue_rows_withheld_mismatch",
        );
    }
    if snapshot.covered_since.is_none() {
        return invalid_gap_fill(
            "program_history_restore_replay_plan_residue_missing_covered_since",
        );
    }
    if snapshot.covered_through.is_none() {
        return invalid_gap_fill(
            "program_history_restore_replay_plan_residue_missing_covered_through",
        );
    }
    let stats = residue_stats(
        &snapshot.missing_segments,
        snapshot.covered_since,
        snapshot.covered_through,
        snapshot.requested_window_start,
        snapshot.requested_window_end,
    );
    if stats.zero_progress_root_segments_count > 0 {
        return invalid_gap_fill("program_history_restore_replay_plan_residue_zero_root_present");
    }
    if stats.unknown_non_target_segments_count > 0 {
        return invalid_gap_fill(
            "program_history_restore_replay_plan_residue_unknown_non_target_present",
        );
    }
    if stats.target_boundary_segments_count == 0 {
        return invalid_gap_fill(
            "program_history_restore_replay_plan_residue_no_boundary_segments",
        );
    }
    if stats.start_coverage_deficit_ms.unwrap_or(i64::MAX) > MAX_ACCEPTED_RESIDUE_SEGMENT_MS {
        return invalid_gap_fill(
            "program_history_restore_replay_plan_residue_start_deficit_too_large",
        );
    }
    if stats.end_coverage_deficit_ms.unwrap_or(i64::MAX) > MAX_ACCEPTED_RESIDUE_SEGMENT_MS {
        return invalid_gap_fill(
            "program_history_restore_replay_plan_residue_end_deficit_too_large",
        );
    }
    if stats.total_accepted_residue_ms.unwrap_or(i64::MAX) > MAX_ACCEPTED_RESIDUE_TOTAL_MS {
        return invalid_gap_fill("program_history_restore_replay_plan_residue_total_too_large");
    }
    if stats
        .max_accepted_residue_segment_or_deficit_ms
        .unwrap_or(i64::MAX)
        > MAX_ACCEPTED_RESIDUE_SEGMENT_MS
    {
        return invalid_gap_fill("program_history_restore_replay_plan_residue_segment_too_large");
    }
    (
        true,
        "program_history_restore_replay_plan_gap_fill_accepted_irreducible_boundary_residue_valid"
            .to_string(),
        Some(ACCEPTED_RESIDUE_POLICY),
        true,
    )
}

fn invalid_gap_fill(reason: &str) -> (bool, String, Option<&'static str>, bool) {
    (false, reason.to_string(), None, false)
}

fn residue_stats(
    segments: &[MissingSegment],
    covered_since: Option<DateTime<Utc>>,
    covered_through: Option<DateTime<Utc>>,
    requested_window_start: DateTime<Utc>,
    requested_window_end: DateTime<Utc>,
) -> ResidueStats {
    let mut stats = ResidueStats::default();
    for segment in segments {
        if segment.reason == ZERO_PROGRESS_ROOT_REASON {
            stats.zero_progress_root_segments_count += 1;
        } else if is_boundary_reason(&segment.reason) {
            stats.target_boundary_segments_count += 1;
            let duration_ms = segment
                .end
                .signed_duration_since(segment.start)
                .num_milliseconds()
                .max(0);
            stats.total_boundary_missing_ms += duration_ms;
            stats.max_boundary_segment_ms = stats.max_boundary_segment_ms.max(duration_ms);
        } else if segment.reason != IRREDUCIBLE_REASON {
            stats.unknown_non_target_segments_count += 1;
        }
    }
    let start_deficit = covered_since.map(|covered| {
        covered
            .signed_duration_since(requested_window_start)
            .num_milliseconds()
            .max(0)
    });
    let end_deficit = covered_through.map(|covered| {
        requested_window_end
            .signed_duration_since(covered)
            .num_milliseconds()
            .max(0)
    });
    stats.start_coverage_deficit_ms = start_deficit;
    stats.end_coverage_deficit_ms = end_deficit;
    stats.total_accepted_residue_ms = start_deficit
        .zip(end_deficit)
        .map(|(start, end)| stats.total_boundary_missing_ms + start + end);
    stats.max_accepted_residue_segment_or_deficit_ms = start_deficit
        .zip(end_deficit)
        .map(|(start, end)| stats.max_boundary_segment_ms.max(start).max(end));
    stats
}

fn is_boundary_reason(reason: &str) -> bool {
    reason == BOUNDARY_PREFIX_REASON || reason == BOUNDARY_SUFFIX_REASON
}

fn required_missing_segments(value: &Value) -> Result<Vec<MissingSegment>> {
    let segments = value
        .get("missing_segments")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("missing_segments missing or non-array"))?;
    segments
        .iter()
        .enumerate()
        .map(|(index, segment)| missing_segment_from_value(index, segment))
        .collect()
}

fn missing_segment_from_value(index: usize, value: &Value) -> Result<MissingSegment> {
    let start = required_segment_ts(value, index, "start")?;
    let end = required_segment_ts(value, index, "end")?;
    if end < start {
        bail!("missing_segments[{index}] end is before start");
    }
    let reason = value
        .get("reason")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| anyhow!("missing_segments[{index}].reason missing or non-string"))?;
    Ok(MissingSegment { start, end, reason })
}

fn required_segment_ts(value: &Value, index: usize, field: &'static str) -> Result<DateTime<Utc>> {
    let raw = value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing_segments[{index}].{field} missing or non-string"))?;
    parse_ts(raw).with_context(|| format!("invalid missing_segments[{index}].{field}"))
}

fn required_string(value: &Value, field: &'static str) -> Result<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| anyhow!("{field} missing or non-string"))
}

fn required_bool(value: &Value, field: &'static str) -> Result<bool> {
    value
        .get(field)
        .and_then(Value::as_bool)
        .ok_or_else(|| anyhow!("{field} missing or non-bool"))
}

fn required_usize(value: &Value, field: &'static str) -> Result<usize> {
    let raw = value
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("{field} missing or non-u64"))?;
    usize::try_from(raw).with_context(|| format!("{field} does not fit usize"))
}

fn optional_usize(value: &Value, field: &'static str) -> Result<Option<usize>> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => {
            let raw = value.as_u64().ok_or_else(|| anyhow!("{field} non-u64"))?;
            Ok(Some(
                usize::try_from(raw).with_context(|| format!("{field} does not fit usize"))?,
            ))
        }
    }
}

fn required_ts(value: &Value, field: &'static str) -> Result<DateTime<Utc>> {
    let raw = required_string(value, field)?;
    parse_ts(&raw).with_context(|| format!("invalid timestamp for {field}"))
}

fn optional_ts(value: &Value, field: &'static str) -> Result<Option<DateTime<Utc>>> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(parse_ts)
        .transpose()
        .with_context(|| format!("invalid timestamp for {field}"))
}

fn optional_cursor_ts(value: &Value, field: &'static str) -> Result<Option<DateTime<Utc>>> {
    value
        .get(field)
        .and_then(|cursor| cursor.get("ts_utc"))
        .and_then(Value::as_str)
        .map(parse_ts)
        .transpose()
        .with_context(|| format!("invalid timestamp for {field}.ts_utc"))
}

fn report_cursor(cursor: &DiscoveryRuntimeCursor) -> ReportCursor {
    ReportCursor {
        ts_utc: format_ts(cursor.ts_utc),
        slot: cursor.slot,
        signature: cursor.signature.clone(),
    }
}

fn format_ts(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339()
}

#[cfg(test)]
mod tests {
    use super::*;
    use copybot_core_types::SwapEvent;
    use copybot_storage::{
        DiscoveryPublicationFreshnessGate, DiscoveryPublicationStateRow, DiscoveryRuntimeMode,
    };
    use tempfile::tempdir;

    const WINDOW_START: &str = "2026-04-18T16:56:04Z";
    const WINDOW_END: &str = "2026-04-23T15:59:39.857Z";
    const NOW: &str = "2026-04-23T16:05:00Z";

    struct Fixture {
        config: Config,
        configured_runtime_db_path: PathBuf,
        _temp: tempfile::TempDir,
    }

    fn make_fixture(name: &str) -> Result<Fixture> {
        let temp = tempdir()?;
        let config_path = temp.path().join(format!("{name}.toml"));
        let artifact_path = temp.path().join(format!("{name}.artifact.json"));
        let journal_db_path = temp.path().join(format!("{name}.journal.sqlite"));
        let gap_fill_db_path = temp.path().join(format!("{name}.gap-fill.sqlite"));
        let progress_path = temp.path().join(format!("{name}.progress.json"));
        let configured_runtime_db_path = temp.path().join(format!("{name}.production.sqlite"));
        let target_db_path = temp.path().join(format!("{name}.target.sqlite"));
        fs::write(
            &config_path,
            format!(
                "[sqlite]\npath = \"{}\"\n\n[discovery]\nscoring_window_days = 7\nrefresh_seconds = 600\nmetric_snapshot_interval_seconds = 1800\nmax_window_swaps_in_memory = 8\nmax_fetch_swaps_per_cycle = 8\nmax_fetch_pages_per_cycle = 5\nfetch_time_budget_ms = 1000\nobserved_swaps_retention_days = 14\n",
                configured_runtime_db_path.display()
            ),
        )?;
        write_runtime_artifact(
            &artifact_path,
            parse_ts(NOW)? - Duration::minutes(10),
            parse_ts("2026-04-16T16:00:00Z")?,
        )?;
        write_accepted_residue_progress(&progress_path, 45_771_784)?;
        write_journal_db(&journal_db_path, "journal", parse_ts(WINDOW_START)?)?;
        write_journal_db(&gap_fill_db_path, "gap-fill", parse_ts(WINDOW_START)?)?;
        Ok(Fixture {
            config: Config {
                config_path,
                artifact_path,
                journal_db_path,
                gap_fill_db_path,
                gap_fill_progress_path: progress_path,
                gap_fill_window_start_utc: parse_ts(WINDOW_START)?,
                gap_fill_window_end_utc: parse_ts(WINDOW_END)?,
                db_path: target_db_path,
                allow_existing_target_db: false,
                min_free_gb: 0,
                estimate_only: true,
                json: true,
                now: parse_ts(NOW)?,
            },
            configured_runtime_db_path,
            _temp: temp,
        })
    }

    fn write_runtime_artifact(
        path: &Path,
        last_published_at: DateTime<Utc>,
        last_published_window_start: DateTime<Utc>,
    ) -> Result<()> {
        let artifact = DiscoveryRuntimeArtifact {
            format_version: copybot_storage::DISCOVERY_RUNTIME_ARTIFACT_FORMAT_VERSION,
            exported_at: last_published_at,
            export_gate: DiscoveryPublicationFreshnessGate {
                scoring_window_days: 7,
                metric_snapshot_interval_seconds: 1_800,
                refresh_seconds: 600,
            },
            publication_state: DiscoveryPublicationStateRow {
                runtime_mode: DiscoveryRuntimeMode::FailClosed,
                reason: "raw_window".to_string(),
                last_published_at: Some(last_published_at),
                last_published_window_start: Some(last_published_window_start),
                published_scoring_source: Some("raw_window".to_string()),
                published_wallet_ids: Some(vec!["wallet-alpha".to_string()]),
                publication_policy_fingerprint: None,
                updated_at: last_published_at,
            },
            runtime_cursor: DiscoveryRuntimeCursor {
                ts_utc: parse_ts(WINDOW_END)? - Duration::milliseconds(857),
                slot: 415_159_799,
                signature: "artifact-runtime-cursor".to_string(),
            },
            published_wallet_metrics_snapshot: Vec::new(),
        };
        fs::write(path, serde_json::to_vec_pretty(&artifact)?)?;
        Ok(())
    }

    fn write_accepted_residue_progress(path: &Path, staged_rows: usize) -> Result<()> {
        let window_start = parse_ts(WINDOW_START)?;
        let window_end = parse_ts(WINDOW_END)?;
        let value = serde_json::json!({
            "verdict": IRREDUCIBLE_VERDICT,
            "reason": IRREDUCIBLE_REASON,
            "current_phase": IRREDUCIBLE_PHASE,
            "replayable_output": false,
            "requested_window_start": WINDOW_START,
            "requested_window_end": WINDOW_END,
            "gap_fill_covered_since": WINDOW_START,
            "gap_fill_covered_through_cursor": {
                "ts_utc": format_ts(window_end - Duration::milliseconds(857)),
                "slot": 415159799u64,
                "signature": "gap-fill-covered-through"
            },
            "missing_segments": [
                {
                    "start": "2026-04-19T20:06:00Z",
                    "end": "2026-04-19T20:06:01Z",
                    "reason": BOUNDARY_PREFIX_REASON
                },
                {
                    "start": "2026-04-19T20:08:00Z",
                    "end": "2026-04-19T20:08:00.600Z",
                    "reason": BOUNDARY_SUFFIX_REASON
                },
                {
                    "start": format_ts(window_start),
                    "end": format_ts(window_end),
                    "reason": IRREDUCIBLE_REASON
                }
            ],
            "inserted_rows": 0,
            "staged_rows": staged_rows,
            "fetched_rows": staged_rows,
            "rows_withheld_due_to_incomplete_outcome": staged_rows
        });
        fs::write(path, serde_json::to_vec_pretty(&value)?)?;
        Ok(())
    }

    fn write_invalid_residue_progress(path: &Path) -> Result<()> {
        let mut value: Value = serde_json::from_slice(&fs::read(path)?)?;
        value["staged_rows"] = Value::from(0);
        value["fetched_rows"] = Value::from(0);
        value["rows_withheld_due_to_incomplete_outcome"] = Value::from(0);
        fs::write(path, serde_json::to_vec_pretty(&value)?)?;
        Ok(())
    }

    fn write_journal_db(path: &Path, prefix: &str, start: DateTime<Utc>) -> Result<()> {
        let store = SqliteStore::open(path)?;
        let swaps = [
            swap(prefix, "a", 1, start),
            swap(prefix, "b", 2, start + Duration::seconds(30)),
        ];
        store.insert_recent_raw_journal_batch(&swaps, start + Duration::minutes(1))?;
        Ok(())
    }

    fn write_stats_broken_db(path: &Path) -> Result<()> {
        let conn = rusqlite::Connection::open(path)?;
        conn.execute_batch("CREATE TABLE unrelated(id INTEGER PRIMARY KEY);")?;
        Ok(())
    }

    fn swap(prefix: &str, suffix: &str, slot: u64, ts: DateTime<Utc>) -> SwapEvent {
        SwapEvent {
            wallet: format!("wallet-{suffix}"),
            dex: "raydium".to_string(),
            token_in: "So11111111111111111111111111111111111111112".to_string(),
            token_out: format!("token-{suffix}"),
            amount_in: 1.0,
            amount_out: 2.0,
            signature: format!("{prefix}-sig-{suffix}"),
            slot,
            ts_utc: ts,
            exact_amounts: None,
        }
    }

    #[test]
    fn missing_db_path_fails_parse() -> Result<()> {
        let fixture = make_fixture("missing-db-path-parse")?;
        let err = parse_args_from(
            vec![
                "--config".to_string(),
                fixture.config.config_path.display().to_string(),
                "--artifact".to_string(),
                fixture.config.artifact_path.display().to_string(),
                "--journal-db-path".to_string(),
                fixture.config.journal_db_path.display().to_string(),
                "--gap-fill-db-path".to_string(),
                fixture.config.gap_fill_db_path.display().to_string(),
                "--gap-fill-progress-path".to_string(),
                fixture.config.gap_fill_progress_path.display().to_string(),
                "--gap-fill-window-start-utc".to_string(),
                WINDOW_START.to_string(),
                "--gap-fill-window-end-utc".to_string(),
                WINDOW_END.to_string(),
                "--json".to_string(),
            ]
            .into_iter(),
        )
        .expect_err("missing --db-path must fail parse");
        assert!(err.to_string().contains("missing required --db-path"));
        Ok(())
    }

    #[test]
    fn valid_accepted_residue_and_readable_stats_with_enough_disk_is_ready() -> Result<()> {
        let fixture = make_fixture("ready")?;
        let report = build_report(&fixture.config)?;
        assert!(report.restore_replay_plan_ready, "{report:#?}");
        assert_eq!(
            report.restore_replay_plan_reason,
            "restore_replay_plan_ready_read_only_inputs_verified"
        );
        assert_eq!(
            report.accepted_residue_policy.as_deref(),
            Some(ACCEPTED_RESIDUE_POLICY)
        );
        assert_eq!(report.gap_fill_staged_rows, Some(45_771_784));
        assert!(report.journal_db_stats.stats_readable);
        assert!(report.gap_fill_db_stats.stats_readable);
        assert!(report.disk_guard.disk_guard_passed);
        assert!(report.target_db_guard_passed);
        assert_eq!(report.target_db_exists, Some(false));
        assert!(!report.target_db_matches_configured_runtime_db);
        assert_eq!(
            report.configured_runtime_db_path,
            fixture.configured_runtime_db_path.display().to_string()
        );
        assert!(report
            .recommended_restore_command
            .contains(&fixture.config.db_path.display().to_string()));
        assert!(!report
            .recommended_restore_command
            .contains(&fixture.configured_runtime_db_path.display().to_string()));
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn target_db_equal_to_configured_runtime_db_fails_closed() -> Result<()> {
        let mut fixture = make_fixture("target-equals-configured")?;
        fixture.config.db_path = fixture.configured_runtime_db_path.clone();
        let report = build_report(&fixture.config)?;
        assert!(!report.restore_replay_plan_ready);
        assert_eq!(
            report.restore_replay_plan_reason,
            "restore_replay_plan_target_db_matches_configured_runtime_db"
        );
        assert!(!report.target_db_guard_passed);
        assert!(report.target_db_matches_configured_runtime_db);
        assert!(!report
            .recommended_restore_command
            .contains(&fixture.configured_runtime_db_path.display().to_string()));
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn target_db_equal_to_configured_runtime_db_fails_even_when_existing_db_allowed() -> Result<()>
    {
        let mut fixture = make_fixture("target-equals-configured-allow-existing")?;
        fixture.config.db_path = fixture.configured_runtime_db_path.clone();
        fixture.config.allow_existing_target_db = true;
        let report = build_report(&fixture.config)?;
        assert!(!report.restore_replay_plan_ready);
        assert_eq!(
            report.restore_replay_plan_reason,
            "restore_replay_plan_target_db_matches_configured_runtime_db"
        );
        assert!(!report.target_db_guard_passed);
        assert!(report.target_db_matches_configured_runtime_db);
        Ok(())
    }

    #[test]
    fn existing_target_db_fails_closed_by_default() -> Result<()> {
        let fixture = make_fixture("existing-target-default")?;
        fs::write(&fixture.config.db_path, b"existing target")?;
        let report = build_report(&fixture.config)?;
        assert!(!report.restore_replay_plan_ready);
        assert_eq!(
            report.restore_replay_plan_reason,
            "restore_replay_plan_target_db_already_exists"
        );
        assert_eq!(report.target_db_exists, Some(true));
        assert!(!report.allow_existing_target_db);
        assert!(!report.target_db_guard_passed);
        assert!(!report
            .recommended_restore_command
            .contains(&fixture.config.db_path.display().to_string()));
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn existing_target_db_is_allowed_only_with_explicit_flag() -> Result<()> {
        let mut fixture = make_fixture("existing-target-allowed")?;
        fs::write(&fixture.config.db_path, b"existing target")?;
        fixture.config.allow_existing_target_db = true;
        let report = build_report(&fixture.config)?;
        assert!(report.restore_replay_plan_ready, "{report:#?}");
        assert_eq!(report.target_db_exists, Some(true));
        assert!(report.allow_existing_target_db);
        assert!(report.target_db_guard_passed);
        assert_eq!(
            report.target_db_guard_reason,
            "restore_replay_plan_existing_target_db_explicitly_allowed"
        );
        assert!(report
            .recommended_restore_command
            .contains(&fixture.config.db_path.display().to_string()));
        assert!(!report
            .recommended_restore_command
            .contains(&fixture.configured_runtime_db_path.display().to_string()));
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn missing_gap_fill_db_fails_closed() -> Result<()> {
        let fixture = make_fixture("missing-gap-fill")?;
        fs::remove_file(&fixture.config.gap_fill_db_path)?;
        let report = build_report(&fixture.config)?;
        assert!(!report.restore_replay_plan_ready);
        assert_eq!(
            report.restore_replay_plan_reason,
            "restore_replay_plan_missing_gap_fill_db"
        );
        Ok(())
    }

    #[test]
    fn missing_journal_db_fails_closed() -> Result<()> {
        let fixture = make_fixture("missing-journal")?;
        fs::remove_file(&fixture.config.journal_db_path)?;
        let report = build_report(&fixture.config)?;
        assert!(!report.restore_replay_plan_ready);
        assert_eq!(
            report.restore_replay_plan_reason,
            "restore_replay_plan_missing_journal_db"
        );
        Ok(())
    }

    #[test]
    fn accepted_residue_invalid_fails_closed() -> Result<()> {
        let fixture = make_fixture("invalid-residue")?;
        write_invalid_residue_progress(&fixture.config.gap_fill_progress_path)?;
        let report = build_report(&fixture.config)?;
        assert!(!report.restore_replay_plan_ready);
        assert!(report
            .restore_replay_plan_reason
            .contains("restore_replay_plan_gap_fill_gate_failed"));
        assert!(!report.gap_fill_gate_valid_for_restore_review);
        Ok(())
    }

    #[test]
    fn disk_free_below_min_fails_closed() -> Result<()> {
        let mut fixture = make_fixture("disk-low")?;
        fixture.config.min_free_gb = u64::MAX;
        let report = build_report(&fixture.config)?;
        assert!(!report.restore_replay_plan_ready);
        assert_eq!(
            report.restore_replay_plan_reason,
            "restore_replay_plan_disk_guard_free_space_below_threshold"
        );
        assert!(!report.disk_guard.disk_guard_passed);
        Ok(())
    }

    #[test]
    fn stale_artifact_requires_bootstrap_degraded_but_does_not_fail_plan_by_itself() -> Result<()> {
        let fixture = make_fixture("stale-artifact")?;
        write_runtime_artifact(
            &fixture.config.artifact_path,
            parse_ts("2026-04-06T17:55:23Z")?,
            parse_ts("2026-04-16T16:00:00Z")?,
        )?;
        let report = build_report(&fixture.config)?;
        assert!(report.restore_replay_plan_ready, "{report:#?}");
        assert_eq!(report.fresh_under_export_gate, Some(false));
        assert_eq!(
            report.normal_restore_requires_bootstrap_degraded,
            Some(true)
        );
        assert!(report
            .recommended_restore_command
            .contains("--bootstrap-degraded"));
        Ok(())
    }

    #[test]
    fn row_stats_query_failure_is_explicit_and_not_ready() -> Result<()> {
        let fixture = make_fixture("stats-failure")?;
        fs::remove_file(&fixture.config.gap_fill_db_path)?;
        write_stats_broken_db(&fixture.config.gap_fill_db_path)?;
        let report = build_report(&fixture.config)?;
        assert!(!report.restore_replay_plan_ready);
        assert_eq!(
            report.restore_replay_plan_reason,
            "restore_replay_plan_gap_fill_stats_unreadable"
        );
        assert!(report.gap_fill_db_stats.query_error.is_some());
        Ok(())
    }

    #[test]
    fn production_green_is_always_false() -> Result<()> {
        let fixture = make_fixture("production-green-false")?;
        let report = build_report(&fixture.config)?;
        assert!(!report.production_green);
        Ok(())
    }

    #[test]
    fn recommended_command_contains_exact_gap_fill_args_and_bootstrap_only_when_required(
    ) -> Result<()> {
        let fixture = make_fixture("recommended-command")?;
        let report = build_report(&fixture.config)?;
        assert!(report.recommended_restore_command.contains("--db-path"));
        assert!(report
            .recommended_restore_command
            .contains(&fixture.config.db_path.display().to_string()));
        assert!(!report
            .recommended_restore_command
            .contains(&fixture.configured_runtime_db_path.display().to_string()));
        assert!(report
            .recommended_restore_command
            .contains("--gap-fill-db-path"));
        assert!(report
            .recommended_restore_command
            .contains(&fixture.config.gap_fill_db_path.display().to_string()));
        assert!(report
            .recommended_restore_command
            .contains("--gap-fill-progress-path"));
        assert!(report
            .recommended_restore_command
            .contains(&fixture.config.gap_fill_progress_path.display().to_string()));
        assert!(report
            .recommended_restore_command
            .contains(&format_ts(parse_ts(WINDOW_START)?)));
        assert!(report
            .recommended_restore_command
            .contains(&format_ts(parse_ts(WINDOW_END)?)));
        assert!(!report
            .recommended_restore_command
            .contains("--bootstrap-degraded"));

        write_runtime_artifact(
            &fixture.config.artifact_path,
            parse_ts("2026-04-06T17:55:23Z")?,
            parse_ts("2026-04-16T16:00:00Z")?,
        )?;
        let stale_report = build_report(&fixture.config)?;
        assert!(stale_report
            .recommended_restore_command
            .contains("--bootstrap-degraded"));
        Ok(())
    }
}
