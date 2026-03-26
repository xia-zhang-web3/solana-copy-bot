#![recursion_limit = "256"]
#![allow(unused_attributes)]

use anyhow::{anyhow, bail, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[allow(dead_code)]
#[path = "copybot_activation_artifact_state_publish_report.rs"]
mod activation_artifact_state_publish_report;

const USAGE: &str = "usage: copybot_activation_artifact_state_history (--history-dir <path> [--snapshot <path>]... [--latest] [--limit <count>] [--recent-horizon-seconds <seconds>] [--snapshot-latest-pointer-dir <path>] [--pointer-name <name>] | --compare <older> <newer>) [--json]";
const DEFAULT_HISTORY_LIMIT: usize = 10;
const DEFAULT_RECENT_HORIZON_SECONDS: u64 = 7 * 24 * 60 * 60;
const DEFAULT_MIN_SNAPSHOTS_FOR_CONFIDENCE: usize = 2;

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
    history_dir: Option<PathBuf>,
    snapshot_paths: Vec<PathBuf>,
    compare: Option<(PathBuf, PathBuf)>,
    latest: bool,
    limit: usize,
    recent_horizon_seconds: u64,
    latest_pointer_dir: Option<PathBuf>,
    pointer_name: String,
    json: bool,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ArtifactStateHistoryVerdict {
    ArtifactStateHistoryLatestCoherent,
    ArtifactStateHistoryLatestNonGreen,
    ArtifactStateHistoryInsufficientArtifacts,
    ArtifactStateHistoryCompareReady,
    ArtifactStateHistoryInvalidArtifact,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum StateIssueTrend {
    Narrower,
    Wider,
    Unchanged,
    Unknown,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum AlignmentProgression {
    Stable,
    Drifting,
    Unknown,
}

#[derive(Debug, Clone, Serialize)]
struct InvalidArtifact {
    path: String,
    error: String,
}

#[derive(Debug, Clone, Serialize)]
struct SnapshotSummary {
    path: String,
    snapshotted_at: DateTime<Utc>,
    state_verdict: String,
    state_reason: String,
    selected_review_generation_id: Option<String>,
    selected_latest_release_generation_id: Option<String>,
    selection_alignment_matches: bool,
    selection_alignment_summary: String,
    review_provenance_verdict: String,
    release_provenance_verdict: String,
    linkage_verdict: String,
    ambiguous_legacy_count: usize,
}

#[derive(Debug, Clone, Serialize)]
struct StateLatestPointerSummary {
    pointer_present: bool,
    pointer_verdict: String,
    pointer_reason: String,
    pointer_path: Option<String>,
    selected_snapshot_path: Option<String>,
    matches_latest_snapshot: bool,
    selected_review_generation_id: Option<String>,
    selected_latest_release_generation_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ArtifactStateHistorySummaryReport {
    mode: String,
    verdict: ArtifactStateHistoryVerdict,
    reason: String,
    history_dir: Option<String>,
    snapshot_paths_examined: Vec<String>,
    latest_only_requested: bool,
    limit: usize,
    recent_horizon_seconds: u64,
    minimum_snapshots_for_confidence: usize,
    snapshots_loaded: usize,
    invalid_artifact_count: usize,
    invalid_artifacts: Vec<InvalidArtifact>,
    coherent_count: usize,
    incomplete_count: usize,
    inconsistent_count: usize,
    ambiguous_count: usize,
    latest_snapshot: Option<SnapshotSummary>,
    latest_snapshot_age_seconds: Option<u64>,
    latest_snapshot_stale_for_operational_confidence: bool,
    history_sparse_for_operational_confidence: bool,
    latest_selected_review_generation_id: Option<String>,
    latest_selected_latest_release_generation_id: Option<String>,
    alignment_progression: AlignmentProgression,
    review_selection_changed_over_history: bool,
    release_selection_changed_over_history: bool,
    latest_pointer_summary: Option<StateLatestPointerSummary>,
    recent_snapshot_progression: Vec<SnapshotSummary>,
    artifact_analysis_only: bool,
    activation_authorized: bool,
    not_authorized_summary: String,
}

#[derive(Debug, Clone, Serialize)]
struct ArtifactStateHistoryDiffReport {
    mode: String,
    verdict: ArtifactStateHistoryVerdict,
    reason: String,
    older_path: String,
    newer_path: String,
    invalid_artifacts: Vec<InvalidArtifact>,
    older_snapshotted_at: Option<DateTime<Utc>>,
    newer_snapshotted_at: Option<DateTime<Utc>>,
    older_state_verdict: Option<String>,
    newer_state_verdict: Option<String>,
    older_state_reason: Option<String>,
    newer_state_reason: Option<String>,
    issue_trend: StateIssueTrend,
    review_channel_selection_changed: bool,
    older_selected_review_generation_id: Option<String>,
    newer_selected_review_generation_id: Option<String>,
    latest_release_selection_changed: bool,
    older_selected_latest_release_generation_id: Option<String>,
    newer_selected_latest_release_generation_id: Option<String>,
    alignment_changed: bool,
    older_alignment_matches: Option<bool>,
    newer_alignment_matches: Option<bool>,
    older_alignment_summary: Option<String>,
    newer_alignment_summary: Option<String>,
    review_provenance_changed: bool,
    older_review_provenance_verdict: Option<String>,
    newer_review_provenance_verdict: Option<String>,
    release_provenance_changed: bool,
    older_release_provenance_verdict: Option<String>,
    newer_release_provenance_verdict: Option<String>,
    linkage_changed: bool,
    older_linkage_verdict: Option<String>,
    newer_linkage_verdict: Option<String>,
    older_ambiguous_legacy_count: Option<usize>,
    newer_ambiguous_legacy_count: Option<usize>,
    artifact_analysis_only: bool,
    activation_authorized: bool,
    not_authorized_summary: String,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut history_dir: Option<PathBuf> = None;
    let mut snapshot_paths = Vec::new();
    let mut compare: Option<(PathBuf, PathBuf)> = None;
    let mut latest = false;
    let mut limit = DEFAULT_HISTORY_LIMIT;
    let mut recent_horizon_seconds = DEFAULT_RECENT_HORIZON_SECONDS;
    let mut latest_pointer_dir: Option<PathBuf> = None;
    let mut pointer_name =
        activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME.to_string();
    let mut json = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--history-dir" => {
                history_dir = Some(PathBuf::from(parse_string_arg(
                    "--history-dir",
                    args.next(),
                )?))
            }
            "--snapshot" => {
                snapshot_paths.push(PathBuf::from(parse_string_arg("--snapshot", args.next())?))
            }
            "--compare" => {
                let older = PathBuf::from(parse_string_arg("--compare", args.next())?);
                let newer = PathBuf::from(parse_string_arg("--compare", args.next())?);
                compare = Some((older, newer));
            }
            "--latest" => latest = true,
            "--limit" => {
                limit = parse_usize_arg("--limit", args.next())?;
            }
            "--recent-horizon-seconds" => {
                recent_horizon_seconds = parse_u64_arg("--recent-horizon-seconds", args.next())?;
            }
            "--snapshot-latest-pointer-dir" => {
                latest_pointer_dir = Some(PathBuf::from(parse_string_arg(
                    "--snapshot-latest-pointer-dir",
                    args.next(),
                )?))
            }
            "--pointer-name" => {
                pointer_name = parse_string_arg("--pointer-name", args.next())?;
            }
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    if compare.is_some() && (history_dir.is_some() || !snapshot_paths.is_empty() || latest) {
        bail!("--compare cannot be combined with --history-dir, --snapshot, or --latest");
    }
    if compare.is_none() && history_dir.is_none() && snapshot_paths.is_empty() {
        bail!("either --history-dir/--snapshot or --compare is required");
    }

    Ok(Some(Config {
        history_dir,
        snapshot_paths,
        compare,
        latest,
        limit,
        recent_horizon_seconds,
        latest_pointer_dir,
        pointer_name,
        json,
    }))
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    let value = value.ok_or_else(|| anyhow!("missing value for {flag}"))?;
    let trimmed = value.trim();
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

fn parse_u64_arg(flag: &str, value: Option<String>) -> Result<u64> {
    let value = parse_string_arg(flag, value)?;
    value
        .parse::<u64>()
        .map_err(|error| anyhow!("failed parsing {flag} as u64: {error}"))
}

fn run(config: Config) -> Result<String> {
    let output = if let Some((older, newer)) = &config.compare {
        let report = build_diff_report(older, newer)?;
        if config.json {
            serde_json::to_string_pretty(&report)?
        } else {
            render_diff_human(&report)
        }
    } else {
        let report = build_summary_report(&config)?;
        if config.json {
            serde_json::to_string_pretty(&report)?
        } else {
            render_summary_human(&report)
        }
    };
    Ok(output)
}

fn build_summary_report(config: &Config) -> Result<ArtifactStateHistorySummaryReport> {
    let snapshot_paths =
        collect_snapshot_paths(config.history_dir.as_deref(), &config.snapshot_paths)?;
    let (mut valid_snapshots, invalid_artifacts) = load_artifacts(&snapshot_paths);
    valid_snapshots.sort_by(|left, right| {
        right
            .artifact
            .snapshotted_at
            .cmp(&left.artifact.snapshotted_at)
            .then_with(|| right.path.cmp(&left.path))
    });

    if config.latest {
        valid_snapshots.truncate(1);
    } else if valid_snapshots.len() > config.limit {
        valid_snapshots.truncate(config.limit);
    }

    let latest_snapshot = valid_snapshots.first();
    let now = Utc::now();
    let latest_snapshot_age_seconds = latest_snapshot.map(|snapshot| {
        now.signed_duration_since(snapshot.artifact.snapshotted_at)
            .num_seconds()
            .max(0) as u64
    });
    let latest_snapshot_stale_for_operational_confidence =
        latest_snapshot_age_seconds.is_some_and(|age| age > config.recent_horizon_seconds);
    let history_sparse_for_operational_confidence =
        valid_snapshots.len() < DEFAULT_MIN_SNAPSHOTS_FOR_CONFIDENCE;

    let coherent_count = valid_snapshots
        .iter()
        .filter(|snapshot| snapshot.artifact.state_verdict == "artifact_state_coherent")
        .count();
    let incomplete_count = valid_snapshots
        .iter()
        .filter(|snapshot| snapshot.artifact.state_verdict == "artifact_state_incomplete")
        .count();
    let inconsistent_count = valid_snapshots
        .iter()
        .filter(|snapshot| snapshot.artifact.state_verdict == "artifact_state_inconsistent")
        .count();
    let ambiguous_count = valid_snapshots
        .iter()
        .filter(|snapshot| {
            snapshot.artifact.state_verdict == "artifact_state_ambiguous_legacy_state"
        })
        .count();

    let latest_pointer_summary = if let Some(pointer_dir) = config.latest_pointer_dir.as_ref() {
        Some(match resolve_pointer_archive_root(config, &valid_snapshots) {
            Ok(pointer_archive_root) => {
                let pointer_report =
                    activation_artifact_state_publish_report::inspect_latest_pointer_report(
                        &pointer_archive_root,
                        pointer_dir,
                        &config.pointer_name,
                        true,
                    )?;
                StateLatestPointerSummary {
                    pointer_present: pointer_report.latest_pointer_exists,
                    pointer_verdict: serialize_enum(&pointer_report.verdict),
                    pointer_reason: pointer_report.reason.clone(),
                    pointer_path: pointer_report.latest_pointer_path.clone(),
                    selected_snapshot_path: pointer_report.persisted_state_snapshot_path.clone(),
                    matches_latest_snapshot: matches_latest_snapshot_path(
                        pointer_report.persisted_state_snapshot_path.as_deref(),
                        latest_snapshot,
                    ),
                    selected_review_generation_id: pointer_report
                        .selected_review_generation_id
                        .clone(),
                    selected_latest_release_generation_id: pointer_report
                        .selected_latest_release_generation_id
                        .clone(),
                }
            }
            Err(error) => StateLatestPointerSummary {
                pointer_present: false,
                pointer_verdict: serialize_enum(
                    &activation_artifact_state_publish_report::ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotPointerBlocked,
                ),
                pointer_reason: format!("{error:#}"),
                pointer_path: Some(
                    pointer_dir
                        .join(format!("{}.json", config.pointer_name))
                        .display()
                        .to_string(),
                ),
                selected_snapshot_path: None,
                matches_latest_snapshot: false,
                selected_review_generation_id: None,
                selected_latest_release_generation_id: None,
            },
        })
    } else {
        None
    };

    let unique_review_generations = valid_snapshots
        .iter()
        .filter_map(|snapshot| snapshot.artifact.selected_review_generation_id.clone())
        .collect::<BTreeSet<_>>();
    let unique_release_generations = valid_snapshots
        .iter()
        .filter_map(|snapshot| {
            snapshot
                .artifact
                .selected_latest_release_generation_id
                .clone()
        })
        .collect::<BTreeSet<_>>();
    let unique_alignment_states = valid_snapshots
        .iter()
        .map(|snapshot| {
            (
                snapshot.artifact.selected_review_generation_id.clone(),
                snapshot
                    .artifact
                    .selected_latest_release_generation_id
                    .clone(),
                snapshot.artifact.selection_alignment_matches,
            )
        })
        .collect::<BTreeSet<_>>();
    let alignment_progression = if unique_alignment_states.is_empty() {
        AlignmentProgression::Unknown
    } else if unique_alignment_states.len() == 1 {
        AlignmentProgression::Stable
    } else {
        AlignmentProgression::Drifting
    };

    let recent_snapshot_progression = valid_snapshots
        .iter()
        .map(snapshot_summary)
        .collect::<Vec<_>>();

    let (verdict, reason) = if !invalid_artifacts.is_empty() {
        (
            ArtifactStateHistoryVerdict::ArtifactStateHistoryInvalidArtifact,
            format!(
                "state snapshot history contains {} invalid artifact(s); review invalid_artifacts before trusting state progression",
                invalid_artifacts.len()
            ),
        )
    } else if latest_snapshot.is_none()
        || (latest_snapshot
            .is_some_and(|snapshot| snapshot.artifact.state_verdict == "artifact_state_coherent")
            && (latest_snapshot_stale_for_operational_confidence
                || history_sparse_for_operational_confidence))
    {
        (
            ArtifactStateHistoryVerdict::ArtifactStateHistoryInsufficientArtifacts,
            "state snapshot history is too sparse or stale for confident current-state progression review"
                .to_string(),
        )
    } else if latest_snapshot
        .is_some_and(|snapshot| snapshot.artifact.state_verdict == "artifact_state_coherent")
    {
        (
            ArtifactStateHistoryVerdict::ArtifactStateHistoryLatestCoherent,
            "latest persisted artifact state snapshot remains coherent".to_string(),
        )
    } else {
        (
            ArtifactStateHistoryVerdict::ArtifactStateHistoryLatestNonGreen,
            "latest persisted artifact state snapshot is non-green; review ambiguity, incompleteness, or inconsistency progression".to_string(),
        )
    };

    Ok(ArtifactStateHistorySummaryReport {
        mode: "history_summary".to_string(),
        verdict,
        reason,
        history_dir: config.history_dir.as_ref().map(|path| path.display().to_string()),
        snapshot_paths_examined: snapshot_paths
            .iter()
            .map(|path| path.display().to_string())
            .collect(),
        latest_only_requested: config.latest,
        limit: config.limit,
        recent_horizon_seconds: config.recent_horizon_seconds,
        minimum_snapshots_for_confidence: DEFAULT_MIN_SNAPSHOTS_FOR_CONFIDENCE,
        snapshots_loaded: valid_snapshots.len(),
        invalid_artifact_count: invalid_artifacts.len(),
        invalid_artifacts,
        coherent_count,
        incomplete_count,
        inconsistent_count,
        ambiguous_count,
        latest_snapshot: latest_snapshot.map(snapshot_summary),
        latest_snapshot_age_seconds,
        latest_snapshot_stale_for_operational_confidence,
        history_sparse_for_operational_confidence,
        latest_selected_review_generation_id: latest_snapshot
            .and_then(|snapshot| snapshot.artifact.selected_review_generation_id.clone()),
        latest_selected_latest_release_generation_id: latest_snapshot
            .and_then(|snapshot| snapshot.artifact.selected_latest_release_generation_id.clone()),
        alignment_progression,
        review_selection_changed_over_history: unique_review_generations.len() > 1,
        release_selection_changed_over_history: unique_release_generations.len() > 1,
        latest_pointer_summary,
        recent_snapshot_progression,
        artifact_analysis_only: true,
        activation_authorized: false,
        not_authorized_summary:
            "Artifact-state snapshot history only records persisted artifact-state analysis. It does not authorize activation, does not enable execution, and does not override the Stage 3 production gate.".to_string(),
    })
}

fn build_diff_report(older: &Path, newer: &Path) -> Result<ArtifactStateHistoryDiffReport> {
    let older_result =
        activation_artifact_state_publish_report::inspect_state_snapshot_artifact(older);
    let newer_result =
        activation_artifact_state_publish_report::inspect_state_snapshot_artifact(newer);

    let mut invalid_artifacts = Vec::new();
    let older_loaded = match older_result {
        Ok(value) => Some(value),
        Err(error) => {
            invalid_artifacts.push(InvalidArtifact {
                path: older.display().to_string(),
                error: format!("{error:#}"),
            });
            None
        }
    };
    let newer_loaded = match newer_result {
        Ok(value) => Some(value),
        Err(error) => {
            invalid_artifacts.push(InvalidArtifact {
                path: newer.display().to_string(),
                error: format!("{error:#}"),
            });
            None
        }
    };

    if !invalid_artifacts.is_empty() {
        return Ok(ArtifactStateHistoryDiffReport {
            mode: "compare".to_string(),
            verdict: ArtifactStateHistoryVerdict::ArtifactStateHistoryInvalidArtifact,
            reason: "one or both compared state snapshot artifacts are invalid".to_string(),
            older_path: older.display().to_string(),
            newer_path: newer.display().to_string(),
            invalid_artifacts,
            older_snapshotted_at: None,
            newer_snapshotted_at: None,
            older_state_verdict: None,
            newer_state_verdict: None,
            older_state_reason: None,
            newer_state_reason: None,
            issue_trend: StateIssueTrend::Unknown,
            review_channel_selection_changed: false,
            older_selected_review_generation_id: None,
            newer_selected_review_generation_id: None,
            latest_release_selection_changed: false,
            older_selected_latest_release_generation_id: None,
            newer_selected_latest_release_generation_id: None,
            alignment_changed: false,
            older_alignment_matches: None,
            newer_alignment_matches: None,
            older_alignment_summary: None,
            newer_alignment_summary: None,
            review_provenance_changed: false,
            older_review_provenance_verdict: None,
            newer_review_provenance_verdict: None,
            release_provenance_changed: false,
            older_release_provenance_verdict: None,
            newer_release_provenance_verdict: None,
            linkage_changed: false,
            older_linkage_verdict: None,
            newer_linkage_verdict: None,
            older_ambiguous_legacy_count: None,
            newer_ambiguous_legacy_count: None,
            artifact_analysis_only: true,
            activation_authorized: false,
            not_authorized_summary:
                "Artifact-state snapshot diff only compares persisted artifact-state analysis. It does not authorize activation, does not enable execution, and does not override the Stage 3 production gate.".to_string(),
        });
    }

    let older_loaded = older_loaded.expect("older loaded");
    let newer_loaded = newer_loaded.expect("newer loaded");
    let older_summary = snapshot_summary(&older_loaded);
    let newer_summary = snapshot_summary(&newer_loaded);

    Ok(ArtifactStateHistoryDiffReport {
        mode: "compare".to_string(),
        verdict: ArtifactStateHistoryVerdict::ArtifactStateHistoryCompareReady,
        reason: "state snapshot comparison is ready".to_string(),
        older_path: older.display().to_string(),
        newer_path: newer.display().to_string(),
        invalid_artifacts: Vec::new(),
        older_snapshotted_at: Some(older_loaded.artifact.snapshotted_at),
        newer_snapshotted_at: Some(newer_loaded.artifact.snapshotted_at),
        older_state_verdict: Some(older_loaded.artifact.state_verdict.clone()),
        newer_state_verdict: Some(newer_loaded.artifact.state_verdict.clone()),
        older_state_reason: Some(older_loaded.artifact.state_reason.clone()),
        newer_state_reason: Some(newer_loaded.artifact.state_reason.clone()),
        issue_trend: issue_trend(
            &older_loaded.artifact.state_verdict,
            &newer_loaded.artifact.state_verdict,
        ),
        review_channel_selection_changed: older_loaded.artifact.selected_review_generation_id
            != newer_loaded.artifact.selected_review_generation_id,
        older_selected_review_generation_id: older_loaded.artifact.selected_review_generation_id,
        newer_selected_review_generation_id: newer_loaded.artifact.selected_review_generation_id,
        latest_release_selection_changed: older_loaded
            .artifact
            .selected_latest_release_generation_id
            != newer_loaded.artifact.selected_latest_release_generation_id,
        older_selected_latest_release_generation_id: older_loaded
            .artifact
            .selected_latest_release_generation_id,
        newer_selected_latest_release_generation_id: newer_loaded
            .artifact
            .selected_latest_release_generation_id,
        alignment_changed: older_loaded.artifact.selection_alignment_matches
            != newer_loaded.artifact.selection_alignment_matches
            || older_loaded.artifact.selection_alignment_summary
                != newer_loaded.artifact.selection_alignment_summary,
        older_alignment_matches: Some(older_loaded.artifact.selection_alignment_matches),
        newer_alignment_matches: Some(newer_loaded.artifact.selection_alignment_matches),
        older_alignment_summary: Some(older_loaded.artifact.selection_alignment_summary),
        newer_alignment_summary: Some(newer_loaded.artifact.selection_alignment_summary),
        review_provenance_changed: older_loaded.artifact.review_provenance_verdict
            != newer_loaded.artifact.review_provenance_verdict,
        older_review_provenance_verdict: Some(older_summary.review_provenance_verdict),
        newer_review_provenance_verdict: Some(newer_summary.review_provenance_verdict),
        release_provenance_changed: older_loaded.artifact.release_provenance_verdict
            != newer_loaded.artifact.release_provenance_verdict,
        older_release_provenance_verdict: Some(older_summary.release_provenance_verdict),
        newer_release_provenance_verdict: Some(newer_summary.release_provenance_verdict),
        linkage_changed: older_loaded.artifact.linkage_verdict != newer_loaded.artifact.linkage_verdict,
        older_linkage_verdict: Some(older_summary.linkage_verdict),
        newer_linkage_verdict: Some(newer_summary.linkage_verdict),
        older_ambiguous_legacy_count: Some(older_loaded.artifact.ambiguous_legacy_count),
        newer_ambiguous_legacy_count: Some(newer_loaded.artifact.ambiguous_legacy_count),
        artifact_analysis_only: true,
        activation_authorized: false,
        not_authorized_summary:
            "Artifact-state snapshot diff only compares persisted artifact-state analysis. It does not authorize activation, does not enable execution, and does not override the Stage 3 production gate.".to_string(),
    })
}

fn collect_snapshot_paths(
    history_dir: Option<&Path>,
    explicit_paths: &[PathBuf],
) -> Result<Vec<PathBuf>> {
    let mut paths = BTreeSet::new();
    if let Some(history_dir) = history_dir {
        if history_dir.exists() {
            for entry in fs::read_dir(history_dir)? {
                let entry = entry?;
                if entry.file_type()?.is_file()
                    && entry
                        .path()
                        .extension()
                        .and_then(|value| value.to_str())
                        .is_some_and(|value| value.eq_ignore_ascii_case("json"))
                {
                    paths.insert(entry.path());
                }
            }
        }
    }
    for path in explicit_paths {
        paths.insert(path.clone());
    }
    Ok(paths.into_iter().collect())
}

fn load_artifacts(
    paths: &[PathBuf],
) -> (
    Vec<activation_artifact_state_publish_report::LoadedStateSnapshot>,
    Vec<InvalidArtifact>,
) {
    let mut valid = Vec::new();
    let mut invalid = Vec::new();
    for path in paths {
        match activation_artifact_state_publish_report::inspect_state_snapshot_artifact(path) {
            Ok(artifact) => valid.push(artifact),
            Err(error) => invalid.push(InvalidArtifact {
                path: path.display().to_string(),
                error: format!("{error:#}"),
            }),
        }
    }
    (valid, invalid)
}

fn resolve_pointer_archive_root(
    config: &Config,
    snapshots: &[activation_artifact_state_publish_report::LoadedStateSnapshot],
) -> Result<PathBuf> {
    if let Some(history_dir) = config.history_dir.as_ref() {
        return Ok(history_dir.clone());
    }

    let roots = snapshots
        .iter()
        .filter_map(|snapshot| snapshot.canonical_path.parent().map(Path::to_path_buf))
        .collect::<BTreeSet<_>>();

    match roots.len() {
        0 => bail!(
            "snapshot latest pointer verification in snapshot-only mode requires at least one valid snapshot artifact or an explicit --history-dir"
        ),
        1 => Ok(roots.into_iter().next().expect("single archive root")),
        _ => bail!(
            "snapshot latest pointer verification in snapshot-only mode requires snapshots from exactly one deterministic archive root; rerun with --history-dir to disambiguate"
        ),
    }
}

fn canonicalize_if_exists(path: &Path) -> Option<PathBuf> {
    fs::canonicalize(path).ok()
}

fn matches_latest_snapshot_path(
    selected_path: Option<&str>,
    latest_snapshot: Option<&activation_artifact_state_publish_report::LoadedStateSnapshot>,
) -> bool {
    match (selected_path, latest_snapshot) {
        (Some(selected_path), Some(snapshot)) => canonicalize_if_exists(Path::new(selected_path))
            .is_some_and(|selected| selected == snapshot.canonical_path),
        _ => false,
    }
}

fn snapshot_summary(
    snapshot: &activation_artifact_state_publish_report::LoadedStateSnapshot,
) -> SnapshotSummary {
    SnapshotSummary {
        path: snapshot.path.display().to_string(),
        snapshotted_at: snapshot.artifact.snapshotted_at,
        state_verdict: snapshot.artifact.state_verdict.clone(),
        state_reason: snapshot.artifact.state_reason.clone(),
        selected_review_generation_id: snapshot.artifact.selected_review_generation_id.clone(),
        selected_latest_release_generation_id: snapshot
            .artifact
            .selected_latest_release_generation_id
            .clone(),
        selection_alignment_matches: snapshot.artifact.selection_alignment_matches,
        selection_alignment_summary: snapshot.artifact.selection_alignment_summary.clone(),
        review_provenance_verdict: snapshot.artifact.review_provenance_verdict.clone(),
        release_provenance_verdict: snapshot.artifact.release_provenance_verdict.clone(),
        linkage_verdict: snapshot.artifact.linkage_verdict.clone(),
        ambiguous_legacy_count: snapshot.artifact.ambiguous_legacy_count,
    }
}

fn issue_trend(older_verdict: &str, newer_verdict: &str) -> StateIssueTrend {
    let older_score = verdict_severity(older_verdict);
    let newer_score = verdict_severity(newer_verdict);
    match newer_score.cmp(&older_score) {
        std::cmp::Ordering::Less => StateIssueTrend::Narrower,
        std::cmp::Ordering::Greater => StateIssueTrend::Wider,
        std::cmp::Ordering::Equal => StateIssueTrend::Unchanged,
    }
}

fn verdict_severity(verdict: &str) -> usize {
    match verdict {
        "artifact_state_invalid_artifacts_present" => 4,
        "artifact_state_inconsistent" => 3,
        "artifact_state_incomplete" => 2,
        "artifact_state_ambiguous_legacy_state" => 1,
        "artifact_state_coherent" => 0,
        _ => 5,
    }
}

fn serialize_enum<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value)
        .unwrap_or_default()
        .trim_matches('"')
        .to_string()
}

fn render_summary_human(report: &ArtifactStateHistorySummaryReport) -> String {
    [
        "event=copybot_activation_artifact_state_history".to_string(),
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("snapshots_loaded={}", report.snapshots_loaded),
        format!("coherent_count={}", report.coherent_count),
        format!("incomplete_count={}", report.incomplete_count),
        format!("inconsistent_count={}", report.inconsistent_count),
        format!("ambiguous_count={}", report.ambiguous_count),
        format!("invalid_artifact_count={}", report.invalid_artifact_count),
        format!(
            "latest_state_verdict={}",
            report
                .latest_snapshot
                .as_ref()
                .map(|value| value.state_verdict.clone())
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!(
            "latest_selected_review_generation_id={}",
            report
                .latest_selected_review_generation_id
                .clone()
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!(
            "latest_selected_latest_release_generation_id={}",
            report
                .latest_selected_latest_release_generation_id
                .clone()
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!(
            "alignment_progression={}",
            serialize_enum(&report.alignment_progression)
        ),
        format!(
            "latest_pointer_verdict={}",
            report
                .latest_pointer_summary
                .as_ref()
                .map(|value| value.pointer_verdict.clone())
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
        format!("activation_authorized={}", report.activation_authorized),
        format!("not_authorized_summary={}", report.not_authorized_summary),
    ]
    .join("\n")
}

fn render_diff_human(report: &ArtifactStateHistoryDiffReport) -> String {
    [
        "event=copybot_activation_artifact_state_history".to_string(),
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!(
            "older_state_verdict={}",
            report
                .older_state_verdict
                .clone()
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!(
            "newer_state_verdict={}",
            report
                .newer_state_verdict
                .clone()
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!("issue_trend={}", serialize_enum(&report.issue_trend)),
        format!(
            "review_channel_selection_changed={}",
            report.review_channel_selection_changed
        ),
        format!(
            "latest_release_selection_changed={}",
            report.latest_release_selection_changed
        ),
        format!("alignment_changed={}", report.alignment_changed),
        format!(
            "review_provenance_changed={}",
            report.review_provenance_changed
        ),
        format!(
            "release_provenance_changed={}",
            report.release_provenance_changed
        ),
        format!("linkage_changed={}", report.linkage_changed),
        format!(
            "older_ambiguous_legacy_count={}",
            report
                .older_ambiguous_legacy_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!(
            "newer_ambiguous_legacy_count={}",
            report
                .newer_ambiguous_legacy_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!("activation_authorized={}", report.activation_authorized),
        format!("not_authorized_summary={}", report.not_authorized_summary),
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs;

    #[test]
    fn empty_history_yields_insufficient_artifacts() {
        let root = temp_dir("state_history_empty");
        let report = build_summary_report(&Config {
            history_dir: Some(root),
            snapshot_paths: Vec::new(),
            compare: None,
            latest: false,
            limit: DEFAULT_HISTORY_LIMIT,
            recent_horizon_seconds: DEFAULT_RECENT_HORIZON_SECONDS,
            latest_pointer_dir: None,
            pointer_name: activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            json: false,
        })
        .expect("history");

        assert_eq!(
            report.verdict,
            ArtifactStateHistoryVerdict::ArtifactStateHistoryInsufficientArtifacts
        );
    }

    #[test]
    fn history_mode_summarizes_coherent_and_non_green_snapshots_correctly() {
        let root = temp_dir("state_history_summary");
        write_snapshot(
            &root.join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json"),
            "artifact_state_coherent",
            "review_gen_1",
            "review_gen_1",
            true,
            0,
            "artifact_provenance_complete",
            "artifact_release_provenance_complete",
            "artifact_linkage_complete",
            ts("2026-03-26T18:00:00Z"),
        );
        write_snapshot(
            &root.join("state_snapshot__2026-03-27T18-00-00Z__artifact_state_inconsistent.json"),
            "artifact_state_inconsistent",
            "review_gen_2",
            "review_gen_1",
            false,
            0,
            "artifact_provenance_complete",
            "artifact_release_provenance_inconsistent_lineage",
            "artifact_linkage_inconsistent",
            ts("2026-03-27T18:00:00Z"),
        );

        let report = build_summary_report(&Config {
            history_dir: Some(root),
            snapshot_paths: Vec::new(),
            compare: None,
            latest: false,
            limit: DEFAULT_HISTORY_LIMIT,
            recent_horizon_seconds: 365 * 24 * 60 * 60,
            latest_pointer_dir: None,
            pointer_name: activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            json: false,
        })
        .expect("history");

        assert_eq!(
            report.verdict,
            ArtifactStateHistoryVerdict::ArtifactStateHistoryLatestNonGreen
        );
        assert_eq!(report.coherent_count, 1);
        assert_eq!(report.inconsistent_count, 1);
        assert_eq!(
            report
                .latest_snapshot
                .as_ref()
                .expect("latest")
                .state_verdict,
            "artifact_state_inconsistent"
        );
    }

    #[test]
    fn compare_mode_shows_state_verdict_and_alignment_drift_correctly() {
        let root = temp_dir("state_history_compare");
        let older = root.join("older.json");
        let newer = root.join("newer.json");
        write_snapshot(
            &older,
            "artifact_state_ambiguous_legacy_state",
            "review_gen_1",
            "review_gen_1",
            true,
            1,
            "artifact_provenance_complete",
            "artifact_release_provenance_ambiguous_timestamp",
            "artifact_linkage_complete",
            ts("2026-03-26T18:00:00Z"),
        );
        write_snapshot(
            &newer,
            "artifact_state_inconsistent",
            "review_gen_2",
            "review_gen_1",
            false,
            1,
            "artifact_provenance_complete",
            "artifact_release_provenance_inconsistent_lineage",
            "artifact_linkage_inconsistent",
            ts("2026-03-27T18:00:00Z"),
        );

        let report = build_diff_report(&older, &newer).expect("diff");

        assert_eq!(
            report.verdict,
            ArtifactStateHistoryVerdict::ArtifactStateHistoryCompareReady
        );
        assert_eq!(report.issue_trend, StateIssueTrend::Wider);
        assert!(report.review_channel_selection_changed);
        assert!(report.alignment_changed);
        assert!(report.release_provenance_changed);
        assert!(report.linkage_changed);
    }

    #[test]
    fn ambiguous_state_remains_non_green_in_history() {
        let root = temp_dir("state_history_ambiguous");
        write_snapshot(
            &root.join("ambiguous.json"),
            "artifact_state_ambiguous_legacy_state",
            "review_gen_1",
            "review_gen_1",
            true,
            2,
            "artifact_provenance_complete",
            "artifact_release_provenance_ambiguous_timestamp",
            "artifact_linkage_ambiguous_legacy_reference",
            ts("2026-03-26T18:00:00Z"),
        );
        write_snapshot(
            &root.join("older_coherent.json"),
            "artifact_state_coherent",
            "review_gen_1",
            "review_gen_1",
            true,
            0,
            "artifact_provenance_complete",
            "artifact_release_provenance_complete",
            "artifact_linkage_complete",
            ts("2026-03-25T18:00:00Z"),
        );

        let report = build_summary_report(&Config {
            history_dir: Some(root),
            snapshot_paths: Vec::new(),
            compare: None,
            latest: false,
            limit: DEFAULT_HISTORY_LIMIT,
            recent_horizon_seconds: 365 * 24 * 60 * 60,
            latest_pointer_dir: None,
            pointer_name: activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            json: false,
        })
        .expect("history");

        assert_eq!(
            report.verdict,
            ArtifactStateHistoryVerdict::ArtifactStateHistoryLatestNonGreen
        );
        assert_eq!(report.ambiguous_count, 1);
        assert_eq!(
            report
                .latest_snapshot
                .as_ref()
                .expect("latest")
                .state_verdict,
            "artifact_state_ambiguous_legacy_state"
        );
    }

    #[test]
    fn snapshot_only_mode_with_valid_pointer_does_not_false_invalidate_pointer_metadata() {
        let root = temp_dir("state_history_snapshot_only_pointer");
        let archive_dir = root.join("archive");
        let pointer_dir = root.join("pointer");
        let snapshot_path =
            archive_dir.join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        write_snapshot(
            &snapshot_path,
            "artifact_state_coherent",
            "review_gen_1",
            "review_gen_1",
            true,
            0,
            "artifact_provenance_complete",
            "artifact_release_provenance_complete",
            "artifact_linkage_complete",
            ts("2026-03-26T18:00:00Z"),
        );
        let loaded = activation_artifact_state_publish_report::inspect_state_snapshot_artifact(
            &snapshot_path,
        )
        .expect("loaded snapshot");
        write_pointer_metadata(
            &pointer_dir,
            activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME,
            &archive_dir,
            &loaded,
        );

        let report = build_summary_report(&Config {
            history_dir: None,
            snapshot_paths: vec![snapshot_path],
            compare: None,
            latest: false,
            limit: DEFAULT_HISTORY_LIMIT,
            recent_horizon_seconds: 365 * 24 * 60 * 60,
            latest_pointer_dir: Some(pointer_dir),
            pointer_name: activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            json: false,
        })
        .expect("history");

        let pointer_summary = report
            .latest_pointer_summary
            .as_ref()
            .expect("pointer summary");
        assert_eq!(
            pointer_summary.pointer_verdict,
            "artifact_state_snapshot_verify_ok"
        );
    }

    #[test]
    fn canonical_path_identity_ignores_relative_vs_absolute_snapshot_spelling() {
        let root = temp_dir("state_history_relative_match");
        let archive_dir = root.join("archive");
        let snapshot_path =
            archive_dir.join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        write_snapshot(
            &snapshot_path,
            "artifact_state_coherent",
            "review_gen_1",
            "review_gen_1",
            true,
            0,
            "artifact_provenance_complete",
            "artifact_release_provenance_complete",
            "artifact_linkage_complete",
            ts("2026-03-26T18:00:00Z"),
        );
        let loaded = activation_artifact_state_publish_report::inspect_state_snapshot_artifact(
            &snapshot_path,
        )
        .expect("loaded snapshot");
        let relative_loaded = activation_artifact_state_publish_report::LoadedStateSnapshot {
            path: PathBuf::from(format!(
                "archive/{}",
                snapshot_path
                    .file_name()
                    .expect("file name")
                    .to_string_lossy()
            )),
            canonical_path: loaded.canonical_path.clone(),
            artifact: loaded.artifact.clone(),
        };

        assert!(matches_latest_snapshot_path(
            Some(&loaded.canonical_path.display().to_string()),
            Some(&relative_loaded),
        ));
    }

    #[test]
    fn mismatched_pointer_target_stays_non_green() {
        let root = temp_dir("state_history_pointer_mismatch");
        let archive_dir = root.join("archive");
        let other_archive_dir = root.join("other_archive");
        let pointer_dir = root.join("pointer");
        let history_snapshot_path =
            archive_dir.join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        let other_snapshot_path = other_archive_dir
            .join("state_snapshot__2026-03-27T18-00-00Z__artifact_state_coherent.json");
        write_snapshot(
            &history_snapshot_path,
            "artifact_state_coherent",
            "review_gen_1",
            "review_gen_1",
            true,
            0,
            "artifact_provenance_complete",
            "artifact_release_provenance_complete",
            "artifact_linkage_complete",
            ts("2026-03-26T18:00:00Z"),
        );
        write_snapshot(
            &other_snapshot_path,
            "artifact_state_coherent",
            "review_gen_2",
            "review_gen_2",
            true,
            0,
            "artifact_provenance_complete",
            "artifact_release_provenance_complete",
            "artifact_linkage_complete",
            ts("2026-03-27T18:00:00Z"),
        );
        let other_loaded =
            activation_artifact_state_publish_report::inspect_state_snapshot_artifact(
                &other_snapshot_path,
            )
            .expect("loaded snapshot");
        write_pointer_metadata(
            &pointer_dir,
            activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME,
            &other_archive_dir,
            &other_loaded,
        );

        let report = build_summary_report(&Config {
            history_dir: Some(archive_dir),
            snapshot_paths: Vec::new(),
            compare: None,
            latest: false,
            limit: DEFAULT_HISTORY_LIMIT,
            recent_horizon_seconds: 365 * 24 * 60 * 60,
            latest_pointer_dir: Some(pointer_dir),
            pointer_name: activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            json: false,
        })
        .expect("history");

        let pointer_summary = report
            .latest_pointer_summary
            .as_ref()
            .expect("pointer summary");
        assert_ne!(
            pointer_summary.pointer_verdict,
            "artifact_state_snapshot_verify_ok"
        );
        assert!(!pointer_summary.matches_latest_snapshot);
    }

    fn temp_dir(prefix: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "{}_{}_{}",
            prefix,
            std::process::id(),
            unique_nanos()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn unique_nanos() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    }

    fn ts(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .expect("timestamp")
            .with_timezone(&Utc)
    }

    fn write_snapshot(
        path: &Path,
        state_verdict: &str,
        selected_review_generation_id: &str,
        selected_latest_release_generation_id: &str,
        selection_alignment_matches: bool,
        ambiguous_legacy_count: usize,
        review_provenance_verdict: &str,
        release_provenance_verdict: &str,
        linkage_verdict: &str,
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
            review_provenance_verdict: review_provenance_verdict.to_string(),
            review_provenance_reason: "sample".to_string(),
            release_provenance_verdict: release_provenance_verdict.to_string(),
            release_provenance_reason: "sample".to_string(),
            linkage_verdict: linkage_verdict.to_string(),
            linkage_reason: "sample".to_string(),
            ambiguous_legacy_count,
            coherent_for_review_operations: state_verdict == "artifact_state_coherent",
            artifact_state_only: true,
            execution_untouched: true,
            activation_authorized: false,
            not_authorized_summary: "artifact-state only".to_string(),
            state_report: serde_json::json!({
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
        snapshot: &activation_artifact_state_publish_report::LoadedStateSnapshot,
    ) {
        fs::create_dir_all(pointer_dir).expect("pointer dir");
        let canonical_archive_dir = fs::canonicalize(archive_dir).expect("canonical archive");
        fs::write(
            pointer_dir.join(format!("{pointer_name}.json")),
            serde_json::to_string_pretty(&json!({
                "pointer_version": "1",
                "pointer_name": pointer_name,
                "source_state_archive_dir": canonical_archive_dir.display().to_string(),
                "selected_snapshot_path": snapshot.canonical_path.display().to_string(),
                "selected_snapshot_file_name": snapshot
                    .canonical_path
                    .file_name()
                    .expect("snapshot file")
                    .to_string_lossy()
                    .into_owned(),
                "snapshot_mode": snapshot.artifact.mode.clone(),
                "snapshotted_at": snapshot.artifact.snapshotted_at,
                "snapshot_verdict": snapshot.artifact.state_verdict.clone(),
                "snapshot_reason": snapshot.artifact.state_reason.clone(),
                "selected_review_generation_id": snapshot.artifact.selected_review_generation_id.clone(),
                "selected_latest_release_generation_id": snapshot.artifact.selected_latest_release_generation_id.clone(),
                "selection_alignment_matches": snapshot.artifact.selection_alignment_matches,
                "coherent_for_review_operations": snapshot.artifact.coherent_for_review_operations,
                "ambiguous_legacy_count": snapshot.artifact.ambiguous_legacy_count,
                "pointed_at": "2026-03-26T18:10:00Z",
                "build_version": "0.1.0",
                "git_commit": "deadbeef"
            }))
            .expect("serialize pointer metadata"),
        )
        .expect("write pointer metadata");
    }
}
