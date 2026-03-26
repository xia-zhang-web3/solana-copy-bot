#![recursion_limit = "256"]
#![allow(unused_attributes)]

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[allow(dead_code)]
#[path = "copybot_activation_artifact_state_history.rs"]
mod activation_artifact_state_history;
#[allow(dead_code)]
#[path = "copybot_activation_artifact_state_publish_report.rs"]
mod activation_artifact_state_publish_report;

const USAGE: &str = "usage: copybot_activation_artifact_state_archive --state-archive-dir <path> [--snapshot-latest-pointer-dir <path>] [--pointer-name <name>] [--recent-horizon-seconds <seconds>] [--json] (--report | --retention-plan --keep-latest <count> | --retention-apply --keep-latest <count>)";

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let output = run(config)?;
    println!("{output}");
    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    state_archive_dir: PathBuf,
    snapshot_latest_pointer_dir: Option<PathBuf>,
    pointer_name: String,
    recent_horizon_seconds: u64,
    json: bool,
    mode: Mode,
}

#[derive(Debug, Clone)]
enum Mode {
    Report,
    RetentionPlan { keep_latest: usize },
    RetentionApply { keep_latest: usize },
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ArtifactStateArchiveVerdict {
    ArtifactStateArchiveOk,
    ArtifactStateArchiveIncomplete,
    ArtifactStateArchiveInvalidArtifactsPresent,
    ArtifactStateArchivePointerInconsistent,
    ArtifactStateArchiveRetentionPlanReady,
    ArtifactStateArchiveRetentionPlanInsufficientArtifacts,
    ArtifactStateArchiveCleanupApplied,
    ArtifactStateArchiveCleanupNothingToDo,
    ArtifactStateArchiveCleanupBlockedByPointer,
    ArtifactStateArchiveCleanupBlockedByInvalidArtifacts,
    ArtifactStateArchiveCleanupFailedPartial,
}

#[derive(Debug, Clone, Serialize)]
struct InvalidArtifact {
    path: String,
    error: String,
}

#[derive(Debug, Clone)]
struct LoadedSnapshotRecord {
    loaded: activation_artifact_state_publish_report::LoadedStateSnapshot,
}

#[derive(Debug, Clone, Serialize)]
struct SnapshotSummary {
    path: String,
    file_name: String,
    snapshotted_at: DateTime<Utc>,
    state_verdict: String,
    state_reason: String,
    selected_review_generation_id: Option<String>,
    selected_latest_release_generation_id: Option<String>,
    selection_alignment_matches: bool,
    coherent_for_review_operations: bool,
    ambiguous_legacy_count: usize,
    protected_by_latest_pointer: bool,
    protected_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct LatestPointerSummary {
    requested: bool,
    pointer_verdict: Option<String>,
    pointer_reason: String,
    pointer_path: Option<String>,
    pointer_metadata_present: bool,
    selected_snapshot_path: Option<String>,
    selected_snapshot_state_verdict: Option<String>,
    selected_snapshot_state_reason: Option<String>,
    selected_snapshot_ambiguous_legacy_count: Option<usize>,
    selected_snapshot_coherent_for_review_operations: Option<bool>,
    selected_snapshot_loaded: bool,
    selected_snapshot_protected: bool,
    target_exists: bool,
    target_matches_identity: bool,
    blocks_cleanup: bool,
}

#[derive(Debug, Clone, Serialize)]
struct ProtectedSnapshotSummary {
    path: String,
    reason: String,
}

#[derive(Debug, Clone, Serialize)]
struct RetentionSelectionSummary {
    keep_latest: usize,
    would_keep: Vec<SnapshotSummary>,
    would_remove: Vec<SnapshotSummary>,
    protected_snapshots: Vec<ProtectedSnapshotSummary>,
}

#[derive(Debug, Clone, Serialize)]
struct ArtifactStateArchiveReport {
    mode: String,
    verdict: ArtifactStateArchiveVerdict,
    reason: String,
    state_archive_dir: String,
    snapshot_latest_pointer_dir: Option<String>,
    pointer_name: String,
    recent_horizon_seconds: u64,
    state_snapshot_count: usize,
    coherent_count: usize,
    incomplete_count: usize,
    inconsistent_count: usize,
    ambiguous_count: usize,
    non_green_count: usize,
    latest_snapshot: Option<SnapshotSummary>,
    latest_snapshot_age_seconds: Option<u64>,
    latest_snapshot_stale_for_operational_confidence: bool,
    history_sparse_for_operational_confidence: bool,
    latest_pointer: LatestPointerSummary,
    invalid_artifact_count: usize,
    invalid_artifacts: Vec<InvalidArtifact>,
    retention: Option<RetentionSelectionSummary>,
    removed_paths: Vec<String>,
    failed_removals: Vec<String>,
    state_snapshot_archive_management_only: bool,
    execution_untouched: bool,
    activation_authorized: bool,
    not_authorized_summary: String,
}

#[derive(Debug, Clone)]
struct PointerProtection {
    summary: LatestPointerSummary,
    protected_path: Option<PathBuf>,
    protected_reason: Option<String>,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut state_archive_dir: Option<PathBuf> = None;
    let mut snapshot_latest_pointer_dir: Option<PathBuf> = None;
    let mut pointer_name =
        activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME.to_string();
    let mut recent_horizon_seconds =
        activation_artifact_state_history::DEFAULT_RECENT_HORIZON_SECONDS;
    let mut json = false;
    let mut report = false;
    let mut retention_plan = false;
    let mut retention_apply = false;
    let mut keep_latest: Option<usize> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--state-archive-dir" => {
                state_archive_dir = Some(PathBuf::from(parse_string_arg(
                    "--state-archive-dir",
                    args.next(),
                )?))
            }
            "--snapshot-latest-pointer-dir" => {
                snapshot_latest_pointer_dir = Some(PathBuf::from(parse_string_arg(
                    "--snapshot-latest-pointer-dir",
                    args.next(),
                )?))
            }
            "--pointer-name" => {
                pointer_name = parse_name(parse_string_arg("--pointer-name", args.next())?)?
            }
            "--recent-horizon-seconds" => {
                recent_horizon_seconds =
                    parse_usize_arg("--recent-horizon-seconds", args.next())? as u64
            }
            "--report" => report = true,
            "--retention-plan" => retention_plan = true,
            "--retention-apply" => retention_apply = true,
            "--keep-latest" => {
                keep_latest = Some(parse_usize_arg("--keep-latest", args.next())?);
            }
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown arg `{other}`"),
        }
    }

    let mode_count = [report, retention_plan, retention_apply]
        .into_iter()
        .filter(|value| *value)
        .count();
    if mode_count != 1 {
        bail!("exactly one of --report, --retention-plan, or --retention-apply is required");
    }

    let mode = if report {
        if keep_latest.is_some() {
            bail!("--keep-latest cannot be used with --report");
        }
        Mode::Report
    } else if retention_plan {
        let keep_latest =
            keep_latest.ok_or_else(|| anyhow!("--retention-plan requires --keep-latest"))?;
        if keep_latest == 0 {
            bail!("--keep-latest must be greater than zero");
        }
        Mode::RetentionPlan { keep_latest }
    } else {
        let keep_latest =
            keep_latest.ok_or_else(|| anyhow!("--retention-apply requires --keep-latest"))?;
        if keep_latest == 0 {
            bail!("--keep-latest must be greater than zero");
        }
        Mode::RetentionApply { keep_latest }
    };

    Ok(Some(Config {
        state_archive_dir: state_archive_dir
            .ok_or_else(|| anyhow!("--state-archive-dir is required"))?,
        snapshot_latest_pointer_dir,
        pointer_name,
        recent_horizon_seconds,
        json,
        mode,
    }))
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    let raw = value.ok_or_else(|| anyhow!("missing value for {flag}"))?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(trimmed.to_string())
}

fn parse_usize_arg(flag: &str, value: Option<String>) -> Result<usize> {
    let value = parse_string_arg(flag, value)?;
    value
        .parse::<usize>()
        .map_err(|error| anyhow!("failed parsing {flag} as usize: {error}"))
}

fn parse_name(value: String) -> Result<String> {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
    {
        Ok(value)
    } else {
        bail!("names may only contain ascii alphanumeric characters, '-' or '_'");
    }
}

fn run(config: Config) -> Result<String> {
    let report = build_report(&config)?;
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_human(&report))
    }
}

fn build_report(config: &Config) -> Result<ArtifactStateArchiveReport> {
    let archive_paths = collect_archive_snapshot_paths(&config.state_archive_dir)?;
    let history_summary = activation_artifact_state_history::inspect_state_history_summary(
        Some(config.state_archive_dir.clone()),
        Vec::new(),
        false,
        usize::MAX,
        config.recent_horizon_seconds,
        config.snapshot_latest_pointer_dir.clone(),
        config.pointer_name.clone(),
    )?;
    let (mut valid_snapshots, invalid_artifacts) = load_archive_snapshots(&archive_paths);
    valid_snapshots.sort_by(|left, right| {
        right
            .loaded
            .artifact
            .snapshotted_at
            .cmp(&left.loaded.artifact.snapshotted_at)
            .then_with(|| right.loaded.canonical_path.cmp(&left.loaded.canonical_path))
    });
    let pointer_protection = inspect_pointer_protection(config)?;

    let retention = match config.mode {
        Mode::Report => None,
        Mode::RetentionPlan { keep_latest } | Mode::RetentionApply { keep_latest } => Some(
            build_retention_selection(&valid_snapshots, keep_latest, &pointer_protection),
        ),
    };

    let (verdict, reason, removed_paths, failed_removals) = match config.mode {
        Mode::Report => determine_report_verdict(
            &history_summary,
            &invalid_artifacts,
            &pointer_protection.summary,
        ),
        Mode::RetentionPlan { .. } => determine_plan_verdict(
            &history_summary,
            &invalid_artifacts,
            &pointer_protection.summary,
            retention.as_ref().expect("retention plan"),
        ),
        Mode::RetentionApply { .. } => apply_retention(
            &config.state_archive_dir,
            &invalid_artifacts,
            &pointer_protection.summary,
            retention.as_ref().expect("retention plan"),
        )?,
    };

    let latest_snapshot = history_summary.latest_snapshot.as_ref().map(|summary| {
        summarize_snapshot_from_history(summary, retention.as_ref(), &pointer_protection)
    });

    Ok(ArtifactStateArchiveReport {
        mode: mode_name(&config.mode).to_string(),
        verdict,
        reason,
        state_archive_dir: config.state_archive_dir.display().to_string(),
        snapshot_latest_pointer_dir: config
            .snapshot_latest_pointer_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        pointer_name: config.pointer_name.clone(),
        recent_horizon_seconds: config.recent_horizon_seconds,
        state_snapshot_count: history_summary.snapshots_loaded,
        coherent_count: history_summary.coherent_count,
        incomplete_count: history_summary.incomplete_count,
        inconsistent_count: history_summary.inconsistent_count,
        ambiguous_count: history_summary.ambiguous_count,
        non_green_count: history_summary.incomplete_count
            + history_summary.inconsistent_count
            + history_summary.ambiguous_count,
        latest_snapshot,
        latest_snapshot_age_seconds: history_summary.latest_snapshot_age_seconds,
        latest_snapshot_stale_for_operational_confidence: history_summary
            .latest_snapshot_stale_for_operational_confidence,
        history_sparse_for_operational_confidence: history_summary
            .history_sparse_for_operational_confidence,
        latest_pointer: pointer_protection.summary,
        invalid_artifact_count: invalid_artifacts.len(),
        invalid_artifacts,
        retention,
        removed_paths,
        failed_removals,
        state_snapshot_archive_management_only: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary: "State-snapshot archive management only indexes, previews, and boundedly cleans persisted artifact-state snapshots. It does not authorize activation, does not enable execution, and does not override the Stage 3 production gate.".to_string(),
    })
}

fn determine_report_verdict(
    history_summary: &activation_artifact_state_history::ArtifactStateHistoryArchiveSummary,
    invalid_artifacts: &[InvalidArtifact],
    pointer_summary: &LatestPointerSummary,
) -> (
    ArtifactStateArchiveVerdict,
    String,
    Vec<String>,
    Vec<String>,
) {
    if !invalid_artifacts.is_empty() {
        return (
            ArtifactStateArchiveVerdict::ArtifactStateArchiveInvalidArtifactsPresent,
            format!(
                "state snapshot archive contains {} invalid artifact(s); review invalid_artifacts before trusting archive health",
                invalid_artifacts.len()
            ),
            Vec::new(),
            Vec::new(),
        );
    }
    if pointer_summary.blocks_cleanup {
        return (
            ArtifactStateArchiveVerdict::ArtifactStateArchivePointerInconsistent,
            pointer_summary.pointer_reason.clone(),
            Vec::new(),
            Vec::new(),
        );
    }
    if history_summary.snapshots_loaded == 0 {
        return (
            ArtifactStateArchiveVerdict::ArtifactStateArchiveIncomplete,
            "no persisted state snapshots were found in the state snapshot archive".to_string(),
            Vec::new(),
            Vec::new(),
        );
    }
    if pointer_selects_valid_non_green_snapshot(pointer_summary) {
        let state_verdict = pointer_summary
            .selected_snapshot_state_verdict
            .as_deref()
            .unwrap_or("unknown");
        let state_reason = pointer_summary
            .selected_snapshot_state_reason
            .as_deref()
            .unwrap_or("no snapshot state reason recorded");
        let selected_snapshot_path = pointer_summary
            .selected_snapshot_path
            .as_deref()
            .unwrap_or("<unknown>");
        return (
            ArtifactStateArchiveVerdict::ArtifactStateArchiveIncomplete,
            format!(
                "state snapshot latest pointer selects non-green snapshot {} with verdict {}: {}",
                selected_snapshot_path, state_verdict, state_reason
            ),
            Vec::new(),
            Vec::new(),
        );
    }
    if history_summary.latest_snapshot_stale_for_operational_confidence
        || history_summary.history_sparse_for_operational_confidence
        || history_summary
            .latest_snapshot
            .as_ref()
            .is_some_and(|summary| summary.state_verdict != "artifact_state_coherent")
    {
        return (
            ArtifactStateArchiveVerdict::ArtifactStateArchiveIncomplete,
            "state snapshot archive is present, but current archive coverage is stale, sparse, or latest snapshot is non-green".to_string(),
            Vec::new(),
            Vec::new(),
        );
    }
    (
        ArtifactStateArchiveVerdict::ArtifactStateArchiveOk,
        "state snapshot archive is healthy enough for artifact-state review operations".to_string(),
        Vec::new(),
        Vec::new(),
    )
}

fn determine_plan_verdict(
    history_summary: &activation_artifact_state_history::ArtifactStateHistoryArchiveSummary,
    invalid_artifacts: &[InvalidArtifact],
    pointer_summary: &LatestPointerSummary,
    selection: &RetentionSelectionSummary,
) -> (
    ArtifactStateArchiveVerdict,
    String,
    Vec<String>,
    Vec<String>,
) {
    if !invalid_artifacts.is_empty() {
        return (
            ArtifactStateArchiveVerdict::ArtifactStateArchiveInvalidArtifactsPresent,
            format!(
                "state snapshot retention preview found {} invalid artifact(s); preview is computed over valid snapshots only",
                invalid_artifacts.len()
            ),
            Vec::new(),
            Vec::new(),
        );
    }
    if pointer_summary.blocks_cleanup {
        return (
            ArtifactStateArchiveVerdict::ArtifactStateArchivePointerInconsistent,
            format!(
                "state snapshot retention preview is blocked by latest-pointer inconsistency: {}",
                pointer_summary.pointer_reason
            ),
            Vec::new(),
            Vec::new(),
        );
    }
    if history_summary.snapshots_loaded == 0 || selection.would_keep.is_empty() {
        return (
            ArtifactStateArchiveVerdict::ArtifactStateArchiveRetentionPlanInsufficientArtifacts,
            "not enough persisted state snapshots exist to produce a meaningful retention plan"
                .to_string(),
            Vec::new(),
            Vec::new(),
        );
    }
    (
        ArtifactStateArchiveVerdict::ArtifactStateArchiveRetentionPlanReady,
        "state snapshot retention preview is ready; review would_keep, would_remove, and protected snapshots before apply".to_string(),
        Vec::new(),
        Vec::new(),
    )
}

fn apply_retention(
    state_archive_dir: &Path,
    invalid_artifacts: &[InvalidArtifact],
    pointer_summary: &LatestPointerSummary,
    selection: &RetentionSelectionSummary,
) -> Result<(
    ArtifactStateArchiveVerdict,
    String,
    Vec<String>,
    Vec<String>,
)> {
    if !invalid_artifacts.is_empty() {
        return Ok((
            ArtifactStateArchiveVerdict::ArtifactStateArchiveCleanupBlockedByInvalidArtifacts,
            format!(
                "state snapshot cleanup is blocked because {} invalid artifact(s) are present",
                invalid_artifacts.len()
            ),
            Vec::new(),
            Vec::new(),
        ));
    }
    if pointer_summary.blocks_cleanup {
        return Ok((
            ArtifactStateArchiveVerdict::ArtifactStateArchiveCleanupBlockedByPointer,
            format!(
                "state snapshot cleanup is blocked by latest-pointer safety: {}",
                pointer_summary.pointer_reason
            ),
            Vec::new(),
            Vec::new(),
        ));
    }
    if selection.would_remove.is_empty() {
        return Ok((
            ArtifactStateArchiveVerdict::ArtifactStateArchiveCleanupNothingToDo,
            "state snapshot cleanup has nothing to do under the requested keep-latest rule"
                .to_string(),
            Vec::new(),
            Vec::new(),
        ));
    }

    let canonical_archive_root = canonical_archive_root(state_archive_dir)?;
    let mut removed_paths = Vec::new();
    let mut failed_removals = Vec::new();
    for snapshot in &selection.would_remove {
        let path = PathBuf::from(&snapshot.path);
        let canonical = fs::canonicalize(&path).with_context(|| {
            format!(
                "failed canonicalizing state snapshot cleanup candidate {}",
                path.display()
            )
        });
        let canonical = match canonical {
            Ok(path) => path,
            Err(error) => {
                failed_removals.push(format!("{path}: {error:#}", path = path.display()));
                continue;
            }
        };
        if !canonical.starts_with(&canonical_archive_root) {
            failed_removals.push(format!(
                "{}: candidate is outside state snapshot archive {}",
                canonical.display(),
                canonical_archive_root.display()
            ));
            continue;
        }
        match fs::remove_file(&canonical) {
            Ok(_) => removed_paths.push(canonical.display().to_string()),
            Err(error) => failed_removals.push(format!("{}: {}", canonical.display(), error)),
        }
    }

    if failed_removals.is_empty() {
        Ok((
            ArtifactStateArchiveVerdict::ArtifactStateArchiveCleanupApplied,
            format!(
                "state snapshot cleanup removed {} snapshot artifact(s) under the requested keep-latest rule",
                removed_paths.len()
            ),
            removed_paths,
            failed_removals,
        ))
    } else {
        Ok((
            ArtifactStateArchiveVerdict::ArtifactStateArchiveCleanupFailedPartial,
            "state snapshot cleanup removed some artifacts, but at least one deletion failed; review failed_removals before retrying".to_string(),
            removed_paths,
            failed_removals,
        ))
    }
}

fn build_retention_selection(
    valid_snapshots: &[LoadedSnapshotRecord],
    keep_latest: usize,
    pointer_protection: &PointerProtection,
) -> RetentionSelectionSummary {
    let mut protected_paths = BTreeMap::new();
    if let (Some(path), Some(reason)) = (
        pointer_protection.protected_path.as_ref(),
        pointer_protection.protected_reason.as_ref(),
    ) {
        protected_paths.insert(path.clone(), reason.clone());
    }

    let mut keep_paths = valid_snapshots
        .iter()
        .take(keep_latest)
        .map(|record| record.loaded.canonical_path.clone())
        .collect::<BTreeSet<_>>();
    for path in protected_paths.keys() {
        keep_paths.insert(path.clone());
    }

    let would_keep = valid_snapshots
        .iter()
        .filter(|record| keep_paths.contains(&record.loaded.canonical_path))
        .map(|record| summarize_loaded_snapshot(record, &protected_paths))
        .collect::<Vec<_>>();
    let would_remove = valid_snapshots
        .iter()
        .filter(|record| !keep_paths.contains(&record.loaded.canonical_path))
        .map(|record| summarize_loaded_snapshot(record, &protected_paths))
        .collect::<Vec<_>>();

    RetentionSelectionSummary {
        keep_latest,
        would_keep,
        would_remove,
        protected_snapshots: protected_paths
            .into_iter()
            .map(|(path, reason)| ProtectedSnapshotSummary {
                path: path.display().to_string(),
                reason,
            })
            .collect(),
    }
}

fn inspect_pointer_protection(config: &Config) -> Result<PointerProtection> {
    let Some(pointer_dir) = &config.snapshot_latest_pointer_dir else {
        return Ok(PointerProtection {
            summary: LatestPointerSummary {
                requested: false,
                pointer_verdict: None,
                pointer_reason: "state snapshot latest pointer was not requested".to_string(),
                pointer_path: None,
                pointer_metadata_present: false,
                selected_snapshot_path: None,
                selected_snapshot_state_verdict: None,
                selected_snapshot_state_reason: None,
                selected_snapshot_ambiguous_legacy_count: None,
                selected_snapshot_coherent_for_review_operations: None,
                selected_snapshot_loaded: false,
                selected_snapshot_protected: false,
                target_exists: false,
                target_matches_identity: false,
                blocks_cleanup: false,
            },
            protected_path: None,
            protected_reason: None,
        });
    };

    let loaded_metadata =
        activation_artifact_state_publish_report::inspect_latest_pointer_metadata(
            pointer_dir,
            &config.pointer_name,
        )?;
    let report = activation_artifact_state_publish_report::inspect_latest_pointer_report(
        &config.state_archive_dir,
        pointer_dir,
        &config.pointer_name,
        true,
    )?;

    let pointer_verdict = serialize_enum(&report.verdict);
    let selected_snapshot_path = report.persisted_state_snapshot_path.clone().or_else(|| {
        loaded_metadata
            .as_ref()
            .map(|loaded| loaded.metadata.selected_snapshot_path.clone())
    });
    let selected_snapshot_state_verdict = report.state_verdict.clone().or_else(|| {
        loaded_metadata
            .as_ref()
            .map(|loaded| loaded.metadata.snapshot_verdict.clone())
    });
    let selected_snapshot_state_reason = report.state_reason.clone().or_else(|| {
        loaded_metadata
            .as_ref()
            .map(|loaded| loaded.metadata.snapshot_reason.clone())
    });
    let selected_snapshot_ambiguous_legacy_count = loaded_metadata
        .as_ref()
        .map(|loaded| loaded.metadata.ambiguous_legacy_count);
    let selected_snapshot_coherent_for_review_operations = loaded_metadata
        .as_ref()
        .map(|loaded| loaded.metadata.coherent_for_review_operations);
    let protected_path = if report.verdict
        == activation_artifact_state_publish_report::ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotVerifyOk
    {
        selected_snapshot_path
            .as_deref()
            .and_then(|path| fs::canonicalize(Path::new(path)).ok())
    } else {
        None
    };
    let protected_reason = protected_path.as_ref().map(|_| {
        format!(
            "protected by valid latest pointer `{}`",
            config.pointer_name
        )
    });
    let blocks_cleanup = loaded_metadata.is_some()
        && report.verdict
            != activation_artifact_state_publish_report::ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotVerifyOk;

    Ok(PointerProtection {
        summary: LatestPointerSummary {
            requested: true,
            pointer_verdict: Some(pointer_verdict),
            pointer_reason: report.reason.clone(),
            pointer_path: report.latest_pointer_path.clone().or_else(|| {
                Some(
                    pointer_dir
                        .join(format!("{}.json", config.pointer_name))
                        .display()
                        .to_string(),
                )
            }),
            pointer_metadata_present: loaded_metadata.is_some(),
            selected_snapshot_path,
            selected_snapshot_state_verdict,
            selected_snapshot_state_reason,
            selected_snapshot_ambiguous_legacy_count,
            selected_snapshot_coherent_for_review_operations,
            selected_snapshot_loaded: report.persisted_state_snapshot_exists,
            selected_snapshot_protected: protected_path.is_some(),
            target_exists: report.latest_pointer_target_exists,
            target_matches_identity: report.latest_pointer_target_matches_identity,
            blocks_cleanup,
        },
        protected_path,
        protected_reason,
    })
}

fn load_archive_snapshots(paths: &[PathBuf]) -> (Vec<LoadedSnapshotRecord>, Vec<InvalidArtifact>) {
    let mut valid = Vec::new();
    let mut invalid = Vec::new();
    for path in paths {
        match activation_artifact_state_publish_report::inspect_state_snapshot_artifact(path) {
            Ok(loaded) => valid.push(LoadedSnapshotRecord { loaded }),
            Err(error) => invalid.push(InvalidArtifact {
                path: path.display().to_string(),
                error: format!("{error:#}"),
            }),
        }
    }
    (valid, invalid)
}

fn collect_archive_snapshot_paths(archive_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    if !archive_dir.exists() {
        return Ok(paths);
    }
    for entry in fs::read_dir(archive_dir)? {
        let entry = entry?;
        if entry.file_type()?.is_file()
            && entry
                .path()
                .extension()
                .and_then(|value| value.to_str())
                .is_some_and(|value| value.eq_ignore_ascii_case("json"))
        {
            paths.push(entry.path());
        }
    }
    paths.sort();
    Ok(paths)
}

fn summarize_loaded_snapshot(
    record: &LoadedSnapshotRecord,
    protected_paths: &BTreeMap<PathBuf, String>,
) -> SnapshotSummary {
    let protected_reason = protected_paths.get(&record.loaded.canonical_path).cloned();
    SnapshotSummary {
        path: record.loaded.canonical_path.display().to_string(),
        file_name: record
            .loaded
            .canonical_path
            .file_name()
            .map(|value| value.to_string_lossy().into_owned())
            .unwrap_or_else(|| "<unknown>".to_string()),
        snapshotted_at: record.loaded.artifact.snapshotted_at,
        state_verdict: record.loaded.artifact.state_verdict.clone(),
        state_reason: record.loaded.artifact.state_reason.clone(),
        selected_review_generation_id: record.loaded.artifact.selected_review_generation_id.clone(),
        selected_latest_release_generation_id: record
            .loaded
            .artifact
            .selected_latest_release_generation_id
            .clone(),
        selection_alignment_matches: record.loaded.artifact.selection_alignment_matches,
        coherent_for_review_operations: record.loaded.artifact.coherent_for_review_operations,
        ambiguous_legacy_count: record.loaded.artifact.ambiguous_legacy_count,
        protected_by_latest_pointer: protected_reason.is_some(),
        protected_reason,
    }
}

fn summarize_snapshot_from_history(
    summary: &activation_artifact_state_history::StateHistorySnapshotSummary,
    retention: Option<&RetentionSelectionSummary>,
    pointer_protection: &PointerProtection,
) -> SnapshotSummary {
    let canonical =
        fs::canonicalize(&summary.path).unwrap_or_else(|_| PathBuf::from(&summary.path));
    let protected_reason = retention
        .and_then(|selection| {
            selection
                .protected_snapshots
                .iter()
                .find(|entry| entry.path == canonical.display().to_string())
                .map(|entry| entry.reason.clone())
        })
        .or_else(|| {
            if pointer_protection.protected_path.as_ref() == Some(&canonical) {
                pointer_protection.protected_reason.clone()
            } else {
                None
            }
        });
    SnapshotSummary {
        path: canonical.display().to_string(),
        file_name: canonical
            .file_name()
            .map(|value| value.to_string_lossy().into_owned())
            .unwrap_or_else(|| "<unknown>".to_string()),
        snapshotted_at: summary.snapshotted_at,
        state_verdict: summary.state_verdict.clone(),
        state_reason: summary.state_reason.clone(),
        selected_review_generation_id: summary.selected_review_generation_id.clone(),
        selected_latest_release_generation_id: summary
            .selected_latest_release_generation_id
            .clone(),
        selection_alignment_matches: summary.selection_alignment_matches,
        coherent_for_review_operations: summary.state_verdict == "artifact_state_coherent",
        ambiguous_legacy_count: summary.ambiguous_legacy_count,
        protected_by_latest_pointer: protected_reason.is_some(),
        protected_reason,
    }
}

fn canonical_archive_root(archive_dir: &Path) -> Result<PathBuf> {
    fs::canonicalize(archive_dir).with_context(|| {
        format!(
            "failed canonicalizing state snapshot archive {}",
            archive_dir.display()
        )
    })
}

fn mode_name(mode: &Mode) -> &'static str {
    match mode {
        Mode::Report => "report",
        Mode::RetentionPlan { .. } => "retention_plan",
        Mode::RetentionApply { .. } => "retention_apply",
    }
}

fn pointer_selects_valid_non_green_snapshot(pointer_summary: &LatestPointerSummary) -> bool {
    pointer_summary.pointer_verdict.as_deref() == Some("artifact_state_snapshot_verify_ok")
        && pointer_summary.selected_snapshot_loaded
        && pointer_summary.target_exists
        && pointer_summary.target_matches_identity
        && pointer_summary
            .selected_snapshot_state_verdict
            .as_deref()
            .is_some_and(|value| value != "artifact_state_coherent")
}

fn render_human(report: &ArtifactStateArchiveReport) -> String {
    let pointer_verdict = report
        .latest_pointer
        .pointer_verdict
        .clone()
        .unwrap_or_else(|| "not_requested".to_string());
    [
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("state_archive_dir={}", report.state_archive_dir),
        format!("state_snapshot_count={}", report.state_snapshot_count),
        format!("coherent_count={}", report.coherent_count),
        format!("incomplete_count={}", report.incomplete_count),
        format!("inconsistent_count={}", report.inconsistent_count),
        format!("ambiguous_count={}", report.ambiguous_count),
        format!("non_green_count={}", report.non_green_count),
        format!(
            "latest_snapshot_path={}",
            report
                .latest_snapshot
                .as_ref()
                .map(|summary| summary.path.clone())
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!(
            "latest_snapshot_stale_for_operational_confidence={}",
            report.latest_snapshot_stale_for_operational_confidence
        ),
        format!(
            "history_sparse_for_operational_confidence={}",
            report.history_sparse_for_operational_confidence
        ),
        format!("latest_pointer_verdict={pointer_verdict}"),
        format!(
            "latest_pointer_selected_snapshot_path={}",
            report
                .latest_pointer
                .selected_snapshot_path
                .clone()
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!(
            "latest_pointer_selected_snapshot_state_verdict={}",
            report
                .latest_pointer
                .selected_snapshot_state_verdict
                .clone()
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!(
            "latest_pointer_selected_snapshot_state_reason={}",
            report
                .latest_pointer
                .selected_snapshot_state_reason
                .clone()
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!(
            "latest_pointer_selected_snapshot_ambiguous_legacy_count={}",
            report
                .latest_pointer
                .selected_snapshot_ambiguous_legacy_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!(
            "latest_pointer_selected_snapshot_coherent_for_review_operations={}",
            report
                .latest_pointer
                .selected_snapshot_coherent_for_review_operations
                .map(|value| value.to_string())
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!("invalid_artifact_count={}", report.invalid_artifact_count),
        format!(
            "protected_snapshot_count={}",
            report
                .retention
                .as_ref()
                .map(|selection| selection.protected_snapshots.len())
                .unwrap_or_else(|| usize::from(report.latest_pointer.selected_snapshot_protected))
        ),
        format!(
            "would_keep_count={}",
            report
                .retention
                .as_ref()
                .map(|selection| selection.would_keep.len())
                .unwrap_or(0)
        ),
        format!(
            "would_remove_count={}",
            report
                .retention
                .as_ref()
                .map(|selection| selection.would_remove.len())
                .unwrap_or(0)
        ),
        format!("removed_count={}", report.removed_paths.len()),
        format!("failed_removal_count={}", report.failed_removals.len()),
        format!(
            "state_snapshot_archive_management_only={}",
            report.state_snapshot_archive_management_only
        ),
        format!("execution_untouched={}", report.execution_untouched),
        format!("activation_authorized={}", report.activation_authorized),
        format!("not_authorized_summary={}", report.not_authorized_summary),
    ]
    .join("\n")
}

fn serialize_enum<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value)
        .unwrap_or_default()
        .trim_matches('"')
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn empty_state_snapshot_archive_is_reported_honestly() {
        let archive_dir = temp_dir("state_archive_empty");
        let report = build_report(&Config {
            state_archive_dir: archive_dir,
            snapshot_latest_pointer_dir: None,
            pointer_name: activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            recent_horizon_seconds:
                activation_artifact_state_history::DEFAULT_RECENT_HORIZON_SECONDS,
            json: false,
            mode: Mode::Report,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateArchiveVerdict::ArtifactStateArchiveIncomplete
        );
        assert_eq!(report.state_snapshot_count, 0);
        assert!(report.reason.contains("no persisted state snapshots"));
    }

    #[test]
    fn valid_archive_with_coherent_and_non_green_snapshots_summarizes_correctly() {
        let archive_dir = temp_dir("state_archive_summary");
        write_snapshot(
            &archive_dir.join("coherent.json"),
            "artifact_state_coherent",
            "review_a",
            "release_a",
            true,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        write_snapshot(
            &archive_dir.join("ambiguous.json"),
            "artifact_state_ambiguous_legacy_state",
            "review_b",
            "release_b",
            true,
            2,
            ts("2026-03-26T19:00:00Z"),
        );

        let report = build_report(&Config {
            state_archive_dir: archive_dir,
            snapshot_latest_pointer_dir: None,
            pointer_name: activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            recent_horizon_seconds: 365 * 24 * 60 * 60,
            json: false,
            mode: Mode::Report,
        })
        .expect("report");

        assert_eq!(report.state_snapshot_count, 2);
        assert_eq!(report.coherent_count, 1);
        assert_eq!(report.ambiguous_count, 1);
        assert_eq!(report.non_green_count, 1);
        assert_eq!(
            report.verdict,
            ArtifactStateArchiveVerdict::ArtifactStateArchiveIncomplete
        );
    }

    #[test]
    fn retention_plan_keeps_latest_n_snapshots_correctly() {
        let archive_dir = temp_dir("state_archive_plan");
        let older = archive_dir.join("older.json");
        let middle = archive_dir.join("middle.json");
        let newer = archive_dir.join("newer.json");
        write_snapshot(
            &older,
            "artifact_state_coherent",
            "review_a",
            "release_a",
            true,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        write_snapshot(
            &middle,
            "artifact_state_coherent",
            "review_b",
            "release_b",
            true,
            0,
            ts("2026-03-26T19:00:00Z"),
        );
        write_snapshot(
            &newer,
            "artifact_state_coherent",
            "review_c",
            "release_c",
            true,
            0,
            ts("2026-03-26T20:00:00Z"),
        );

        let report = build_report(&Config {
            state_archive_dir: archive_dir,
            snapshot_latest_pointer_dir: None,
            pointer_name: activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            recent_horizon_seconds: 365 * 24 * 60 * 60,
            json: false,
            mode: Mode::RetentionPlan { keep_latest: 2 },
        })
        .expect("report");

        let retention = report.retention.expect("retention");
        assert_eq!(
            report.verdict,
            ArtifactStateArchiveVerdict::ArtifactStateArchiveRetentionPlanReady
        );
        assert_eq!(retention.would_keep.len(), 2);
        assert_eq!(retention.would_remove.len(), 1);
        assert_eq!(retention.would_remove[0].file_name, "older.json");
    }

    #[test]
    fn valid_latest_pointer_target_is_protected_from_cleanup() {
        let archive_dir = temp_dir("state_archive_pointer_protected");
        let pointer_dir = temp_dir("state_archive_pointer_dir");
        let older = archive_dir.join("older.json");
        let middle = archive_dir.join("middle.json");
        let newer = archive_dir.join("newer.json");
        write_snapshot(
            &older,
            "artifact_state_coherent",
            "review_a",
            "release_a",
            true,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        write_snapshot(
            &middle,
            "artifact_state_coherent",
            "review_b",
            "release_b",
            true,
            0,
            ts("2026-03-26T19:00:00Z"),
        );
        write_snapshot(
            &newer,
            "artifact_state_coherent",
            "review_c",
            "release_c",
            true,
            0,
            ts("2026-03-26T20:00:00Z"),
        );
        write_pointer_metadata(
            &pointer_dir,
            activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME,
            &archive_dir,
            &older,
        );

        let report = build_report(&Config {
            state_archive_dir: archive_dir,
            snapshot_latest_pointer_dir: Some(pointer_dir),
            pointer_name: activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            recent_horizon_seconds: 365 * 24 * 60 * 60,
            json: false,
            mode: Mode::RetentionPlan { keep_latest: 1 },
        })
        .expect("report");

        let retention = report.retention.expect("retention");
        assert_eq!(retention.would_keep.len(), 2);
        assert_eq!(retention.would_remove.len(), 1);
        assert_eq!(retention.protected_snapshots.len(), 1);
        assert_eq!(
            retention.protected_snapshots[0].path,
            fs::canonicalize(&older)
                .expect("canonical older")
                .display()
                .to_string()
        );
        assert_eq!(retention.would_remove[0].file_name, "middle.json");
    }

    #[test]
    fn report_mode_honors_valid_pointer_to_older_ambiguous_snapshot() {
        let archive_dir = temp_dir("state_archive_pointer_ambiguous_report");
        let pointer_dir = temp_dir("state_archive_pointer_ambiguous_report_dir");
        let older = archive_dir.join("older_ambiguous.json");
        let newer = archive_dir.join("newer_coherent.json");
        write_snapshot(
            &older,
            "artifact_state_ambiguous_legacy_state",
            "review_a",
            "release_a",
            true,
            2,
            ts("2026-03-26T18:00:00Z"),
        );
        write_snapshot(
            &newer,
            "artifact_state_coherent",
            "review_b",
            "release_b",
            true,
            0,
            ts("2026-03-26T20:00:00Z"),
        );
        write_pointer_metadata(
            &pointer_dir,
            activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME,
            &archive_dir,
            &older,
        );

        let report = build_report(&Config {
            state_archive_dir: archive_dir,
            snapshot_latest_pointer_dir: Some(pointer_dir),
            pointer_name: activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            recent_horizon_seconds: 365 * 24 * 60 * 60,
            json: false,
            mode: Mode::Report,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateArchiveVerdict::ArtifactStateArchiveIncomplete
        );
        assert!(report
            .reason
            .contains("latest pointer selects non-green snapshot"));
        assert_eq!(
            report
                .latest_pointer
                .selected_snapshot_state_verdict
                .as_deref(),
            Some("artifact_state_ambiguous_legacy_state")
        );
        assert_eq!(
            report
                .latest_pointer
                .selected_snapshot_ambiguous_legacy_count,
            Some(2)
        );
        assert_eq!(
            report
                .latest_pointer
                .selected_snapshot_coherent_for_review_operations,
            Some(false)
        );
    }

    #[test]
    fn report_mode_honors_valid_pointer_to_older_incomplete_snapshot() {
        let archive_dir = temp_dir("state_archive_pointer_incomplete_report");
        let pointer_dir = temp_dir("state_archive_pointer_incomplete_report_dir");
        let older = archive_dir.join("older_incomplete.json");
        let newer = archive_dir.join("newer_coherent.json");
        write_snapshot(
            &older,
            "artifact_state_incomplete",
            "review_a",
            "release_a",
            true,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        write_snapshot(
            &newer,
            "artifact_state_coherent",
            "review_b",
            "release_b",
            true,
            0,
            ts("2026-03-26T20:00:00Z"),
        );
        write_pointer_metadata(
            &pointer_dir,
            activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME,
            &archive_dir,
            &older,
        );

        let report = build_report(&Config {
            state_archive_dir: archive_dir,
            snapshot_latest_pointer_dir: Some(pointer_dir),
            pointer_name: activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            recent_horizon_seconds: 365 * 24 * 60 * 60,
            json: false,
            mode: Mode::Report,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateArchiveVerdict::ArtifactStateArchiveIncomplete
        );
        assert_eq!(
            report
                .latest_pointer
                .selected_snapshot_state_verdict
                .as_deref(),
            Some("artifact_state_incomplete")
        );
        assert_eq!(
            report
                .latest_pointer
                .selected_snapshot_coherent_for_review_operations,
            Some(false)
        );
    }

    #[test]
    fn report_mode_can_stay_green_with_valid_pointer_to_older_coherent_snapshot() {
        let archive_dir = temp_dir("state_archive_pointer_coherent_report");
        let pointer_dir = temp_dir("state_archive_pointer_coherent_report_dir");
        let older = archive_dir.join("older_coherent.json");
        let newer = archive_dir.join("newer_coherent.json");
        write_snapshot(
            &older,
            "artifact_state_coherent",
            "review_a",
            "release_a",
            true,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        write_snapshot(
            &newer,
            "artifact_state_coherent",
            "review_b",
            "release_b",
            true,
            0,
            ts("2026-03-26T20:00:00Z"),
        );
        write_pointer_metadata(
            &pointer_dir,
            activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME,
            &archive_dir,
            &older,
        );

        let report = build_report(&Config {
            state_archive_dir: archive_dir,
            snapshot_latest_pointer_dir: Some(pointer_dir),
            pointer_name: activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            recent_horizon_seconds: 365 * 24 * 60 * 60,
            json: false,
            mode: Mode::Report,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateArchiveVerdict::ArtifactStateArchiveOk
        );
        assert_eq!(
            report
                .latest_pointer
                .selected_snapshot_state_verdict
                .as_deref(),
            Some("artifact_state_coherent")
        );
        assert_eq!(
            report
                .latest_pointer
                .selected_snapshot_coherent_for_review_operations,
            Some(true)
        );
    }

    #[test]
    fn dangling_latest_pointer_is_surfaced_honestly() {
        let archive_dir = temp_dir("state_archive_pointer_dangling");
        let pointer_dir = temp_dir("state_archive_pointer_dangling_dir");
        let snapshot = archive_dir.join("snapshot.json");
        write_snapshot(
            &snapshot,
            "artifact_state_coherent",
            "review_a",
            "release_a",
            true,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        write_missing_target_pointer_metadata(
            &pointer_dir,
            activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME,
            &archive_dir,
            &archive_dir.join("missing.json"),
        );

        let report = build_report(&Config {
            state_archive_dir: archive_dir,
            snapshot_latest_pointer_dir: Some(pointer_dir),
            pointer_name: activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            recent_horizon_seconds: 365 * 24 * 60 * 60,
            json: false,
            mode: Mode::RetentionPlan { keep_latest: 1 },
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateArchiveVerdict::ArtifactStateArchivePointerInconsistent
        );
        assert!(report.latest_pointer.blocks_cleanup);
        assert!(report
            .latest_pointer
            .pointer_reason
            .contains("target snapshot artifact is missing"));
    }

    #[test]
    fn retention_apply_removes_exact_preview_selected_snapshots() {
        let archive_dir = temp_dir("state_archive_apply");
        let older = archive_dir.join("older.json");
        let middle = archive_dir.join("middle.json");
        let newer = archive_dir.join("newer.json");
        write_snapshot(
            &older,
            "artifact_state_coherent",
            "review_a",
            "release_a",
            true,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        write_snapshot(
            &middle,
            "artifact_state_coherent",
            "review_b",
            "release_b",
            true,
            0,
            ts("2026-03-26T19:00:00Z"),
        );
        write_snapshot(
            &newer,
            "artifact_state_coherent",
            "review_c",
            "release_c",
            true,
            0,
            ts("2026-03-26T20:00:00Z"),
        );

        let preview = build_report(&Config {
            state_archive_dir: archive_dir.clone(),
            snapshot_latest_pointer_dir: None,
            pointer_name: activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            recent_horizon_seconds: 365 * 24 * 60 * 60,
            json: false,
            mode: Mode::RetentionPlan { keep_latest: 1 },
        })
        .expect("preview");
        let preview_remove = preview
            .retention
            .as_ref()
            .expect("retention")
            .would_remove
            .iter()
            .map(|summary| summary.path.clone())
            .collect::<BTreeSet<_>>();

        let apply = build_report(&Config {
            state_archive_dir: archive_dir.clone(),
            snapshot_latest_pointer_dir: None,
            pointer_name: activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            recent_horizon_seconds: 365 * 24 * 60 * 60,
            json: false,
            mode: Mode::RetentionApply { keep_latest: 1 },
        })
        .expect("apply");

        assert_eq!(
            apply.verdict,
            ArtifactStateArchiveVerdict::ArtifactStateArchiveCleanupApplied
        );
        assert_eq!(
            apply.removed_paths.iter().cloned().collect::<BTreeSet<_>>(),
            preview_remove
        );
        assert!(!older.exists());
        assert!(!middle.exists());
        assert!(newer.exists());
    }

    #[test]
    fn invalid_snapshot_artifact_blocks_cleanup_by_default() {
        let archive_dir = temp_dir("state_archive_invalid_cleanup");
        let valid_snapshot = archive_dir.join("valid.json");
        let invalid_snapshot = archive_dir.join("invalid.json");
        write_snapshot(
            &valid_snapshot,
            "artifact_state_coherent",
            "review_a",
            "release_a",
            true,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        fs::write(&invalid_snapshot, "{not valid json").expect("write invalid snapshot");

        let report = build_report(&Config {
            state_archive_dir: archive_dir,
            snapshot_latest_pointer_dir: None,
            pointer_name: activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            recent_horizon_seconds: 365 * 24 * 60 * 60,
            json: false,
            mode: Mode::RetentionApply { keep_latest: 1 },
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateArchiveVerdict::ArtifactStateArchiveCleanupBlockedByInvalidArtifacts
        );
        assert_eq!(report.invalid_artifact_count, 1);
        assert!(report.removed_paths.is_empty());
    }

    #[test]
    fn nothing_to_do_case_is_explicit() {
        let archive_dir = temp_dir("state_archive_nothing_to_do");
        let snapshot = archive_dir.join("snapshot.json");
        write_snapshot(
            &snapshot,
            "artifact_state_coherent",
            "review_a",
            "release_a",
            true,
            0,
            ts("2026-03-26T18:00:00Z"),
        );

        let report = build_report(&Config {
            state_archive_dir: archive_dir,
            snapshot_latest_pointer_dir: None,
            pointer_name: activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            recent_horizon_seconds: 365 * 24 * 60 * 60,
            json: false,
            mode: Mode::RetentionApply { keep_latest: 1 },
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateArchiveVerdict::ArtifactStateArchiveCleanupNothingToDo
        );
        assert!(report.removed_paths.is_empty());
        assert!(report.execution_untouched);
        assert!(!report.activation_authorized);
    }

    fn write_snapshot(
        path: &Path,
        state_verdict: &str,
        selected_review_generation_id: &str,
        selected_latest_release_generation_id: &str,
        selection_alignment_matches: bool,
        ambiguous_legacy_count: usize,
        snapshotted_at: DateTime<Utc>,
    ) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("snapshot dir");
        }
        let artifact = activation_artifact_state_publish_report::ArtifactStateSnapshotArtifact {
            mode: "artifact_state_snapshot".to_string(),
            snapshot_version: "1".to_string(),
            snapshotted_at,
            build_version: "0.1.0".to_string(),
            git_commit: Some("deadbeef".to_string()),
            state_verdict: state_verdict.to_string(),
            state_reason: format!("sample {state_verdict}"),
            review_archive_dir: "/tmp/review_archive".to_string(),
            review_manifest_dir: "/tmp/review_manifest".to_string(),
            review_bundle_dir: "/tmp/review_bundle".to_string(),
            review_channel_dir: "/tmp/review_channel".to_string(),
            review_channel_name: "current_review".to_string(),
            release_archive_dir: "/tmp/release_archive".to_string(),
            release_history_dir: "/tmp/release_history".to_string(),
            latest_release_pointer_dir: "/tmp/release_pointer".to_string(),
            latest_release_pointer_name: "latest_release".to_string(),
            selected_review_generation_id: Some(selected_review_generation_id.to_string()),
            selected_latest_release_generation_id: Some(
                selected_latest_release_generation_id.to_string(),
            ),
            selection_alignment_matches,
            selection_alignment_summary: if selection_alignment_matches {
                "review and release selections match".to_string()
            } else {
                "review and release selections diverge".to_string()
            },
            review_provenance_verdict: "artifact_provenance_complete".to_string(),
            review_provenance_reason: "sample".to_string(),
            release_provenance_verdict: "artifact_release_provenance_complete".to_string(),
            release_provenance_reason: "sample".to_string(),
            linkage_verdict: "artifact_linkage_complete".to_string(),
            linkage_reason: "sample".to_string(),
            ambiguous_legacy_count,
            coherent_for_review_operations: state_verdict == "artifact_state_coherent",
            artifact_state_only: true,
            execution_untouched: true,
            activation_authorized: false,
            not_authorized_summary: "artifact-state only".to_string(),
            state_report: json!({
                "mode": "artifact_state_report",
                "verdict": state_verdict,
            }),
        };
        fs::write(
            path,
            serde_json::to_string_pretty(&artifact).expect("snapshot json"),
        )
        .expect("write snapshot");
    }

    fn write_pointer_metadata(
        pointer_dir: &Path,
        pointer_name: &str,
        archive_dir: &Path,
        snapshot_path: &Path,
    ) {
        fs::create_dir_all(pointer_dir).expect("pointer dir");
        let loaded = activation_artifact_state_publish_report::inspect_state_snapshot_artifact(
            snapshot_path,
        )
        .expect("load snapshot");
        let metadata = activation_artifact_state_publish_report::StateLatestPointerMetadata {
            pointer_version: "1".to_string(),
            pointer_name: pointer_name.to_string(),
            source_state_archive_dir: fs::canonicalize(archive_dir)
                .expect("canonical archive")
                .display()
                .to_string(),
            selected_snapshot_path: loaded.canonical_path.display().to_string(),
            selected_snapshot_file_name: loaded
                .canonical_path
                .file_name()
                .expect("file")
                .to_string_lossy()
                .into_owned(),
            snapshot_mode: loaded.artifact.mode.clone(),
            snapshotted_at: loaded.artifact.snapshotted_at,
            snapshot_verdict: loaded.artifact.state_verdict.clone(),
            snapshot_reason: loaded.artifact.state_reason.clone(),
            selected_review_generation_id: loaded.artifact.selected_review_generation_id.clone(),
            selected_latest_release_generation_id: loaded
                .artifact
                .selected_latest_release_generation_id
                .clone(),
            selection_alignment_matches: loaded.artifact.selection_alignment_matches,
            coherent_for_review_operations: loaded.artifact.coherent_for_review_operations,
            ambiguous_legacy_count: loaded.artifact.ambiguous_legacy_count,
            pointed_at: ts("2026-03-26T20:05:00Z"),
            build_version: "0.1.0".to_string(),
            git_commit: Some("deadbeef".to_string()),
        };
        fs::write(
            pointer_dir.join(format!("{pointer_name}.json")),
            serde_json::to_string_pretty(&metadata).expect("pointer json"),
        )
        .expect("write pointer");
    }

    fn write_missing_target_pointer_metadata(
        pointer_dir: &Path,
        pointer_name: &str,
        archive_dir: &Path,
        missing_snapshot_path: &Path,
    ) {
        fs::create_dir_all(pointer_dir).expect("pointer dir");
        let metadata = activation_artifact_state_publish_report::StateLatestPointerMetadata {
            pointer_version: "1".to_string(),
            pointer_name: pointer_name.to_string(),
            source_state_archive_dir: fs::canonicalize(archive_dir)
                .expect("canonical archive")
                .display()
                .to_string(),
            selected_snapshot_path: missing_snapshot_path.display().to_string(),
            selected_snapshot_file_name: missing_snapshot_path
                .file_name()
                .expect("file")
                .to_string_lossy()
                .into_owned(),
            snapshot_mode: "artifact_state_snapshot".to_string(),
            snapshotted_at: ts("2026-03-26T20:00:00Z"),
            snapshot_verdict: "artifact_state_coherent".to_string(),
            snapshot_reason: "sample artifact_state_coherent".to_string(),
            selected_review_generation_id: Some("review_missing".to_string()),
            selected_latest_release_generation_id: Some("release_missing".to_string()),
            selection_alignment_matches: true,
            coherent_for_review_operations: true,
            ambiguous_legacy_count: 0,
            pointed_at: ts("2026-03-26T20:05:00Z"),
            build_version: "0.1.0".to_string(),
            git_commit: Some("deadbeef".to_string()),
        };
        fs::write(
            pointer_dir.join(format!("{pointer_name}.json")),
            serde_json::to_string_pretty(&metadata).expect("pointer json"),
        )
        .expect("write pointer");
    }

    fn temp_dir(prefix: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "{}_{}_{}",
            prefix,
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn ts(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .expect("timestamp")
            .with_timezone(&Utc)
    }
}
