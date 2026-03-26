#![recursion_limit = "256"]
#![allow(unused_attributes)]

use anyhow::{anyhow, bail, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[allow(dead_code)]
#[path = "copybot_activation_artifact_archive.rs"]
mod activation_artifact_archive;
#[allow(dead_code)]
#[path = "copybot_activation_artifact_channel.rs"]
mod activation_artifact_channel;
#[allow(dead_code)]
#[path = "copybot_activation_artifact_release_history.rs"]
mod activation_artifact_release_history;
#[allow(dead_code)]
#[path = "copybot_activation_artifact_release_publish_report.rs"]
mod activation_artifact_release_publish_report;
#[allow(dead_code)]
#[path = "copybot_activation_artifact_state_publish_report.rs"]
mod activation_artifact_state_publish_report;
#[allow(dead_code)]
#[path = "copybot_activation_artifact_state_report.rs"]
mod activation_artifact_state_report;

const USAGE: &str = "usage: copybot_activation_artifact_state_linkage_report (--state-archive-dir <path> | --state-snapshot <path>...) [--snapshot-latest-pointer-dir <path>] --review-archive-dir <path> --review-manifest-dir <path> --review-bundle-dir <path> --review-channel-dir <path> --release-archive-dir <path> --release-history-dir <path> --latest-pointer-dir <path> [--snapshot-pointer-name <name>] [--review-channel-name <name>] [--latest-pointer-name <name>] [--json]";

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
    state_archive_dir: Option<PathBuf>,
    state_snapshot_paths: Vec<PathBuf>,
    snapshot_latest_pointer_dir: Option<PathBuf>,
    snapshot_pointer_name: String,
    review_archive_dir: PathBuf,
    review_manifest_dir: PathBuf,
    review_bundle_dir: PathBuf,
    review_channel_dir: PathBuf,
    review_channel_name: String,
    release_archive_dir: PathBuf,
    release_history_dir: PathBuf,
    latest_pointer_dir: PathBuf,
    latest_pointer_name: String,
    json: bool,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ArtifactStateLinkageVerdict {
    ArtifactStateLinkageComplete,
    ArtifactStateLinkageIncomplete,
    ArtifactStateLinkageInvalidArtifactsPresent,
    ArtifactStateLinkageInconsistent,
    ArtifactStateLinkageAmbiguousLegacyState,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum PersistedStateSnapshotLinkageVerdict {
    Complete,
    Incomplete,
    Inconsistent,
    AmbiguousLegacyState,
}

#[derive(Debug, Clone, Serialize)]
struct LinkageIssue {
    surface: String,
    path: String,
    error: String,
}

#[derive(Debug, Clone)]
struct StateSnapshotRecord {
    loaded: activation_artifact_state_publish_report::LoadedStateSnapshot,
    identity_key: String,
}

#[derive(Debug, Clone, Serialize)]
struct PersistedStateSnapshotLinkageSummary {
    snapshot_path: String,
    snapshot_file_name: String,
    identity_key: String,
    snapshotted_at: DateTime<Utc>,
    state_verdict: String,
    state_reason: String,
    linkage_verdict: PersistedStateSnapshotLinkageVerdict,
    linkage_reason: String,
    selected_review_generation_id: Option<String>,
    selected_latest_release_generation_id: Option<String>,
    selection_alignment_matches: bool,
    selection_alignment_summary: String,
    coherent_for_review_operations: bool,
    ambiguous_legacy_count: usize,
    selected_review_generation_present_now: bool,
    selected_latest_release_generation_present_now: bool,
    resolves_live_review_release_generations: bool,
    matches_current_review_channel_selection: bool,
    matches_current_latest_release_selection: bool,
    incomplete_reasons: Vec<String>,
    inconsistencies: Vec<String>,
    ambiguities: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct StateSnapshotLatestPointerLinkageSummary {
    requested: bool,
    pointer_name: String,
    pointer_verdict: Option<String>,
    pointer_reason: String,
    pointer_path: Option<String>,
    selected_snapshot_path: Option<String>,
    selected_snapshot_linkage_verdict: Option<PersistedStateSnapshotLinkageVerdict>,
    selected_snapshot_linkage_reason: Option<String>,
    target_exists: bool,
    target_matches_identity: bool,
    selected_snapshot_loaded: bool,
    selected_review_generation_id: Option<String>,
    selected_latest_release_generation_id: Option<String>,
    selected_snapshot_resolves_live_generations: bool,
    selected_snapshot_matches_current_review_channel: bool,
    selected_snapshot_matches_current_latest_release: bool,
}

#[derive(Debug, Clone, Serialize)]
struct ArtifactStateLinkageReport {
    mode: String,
    verdict: ArtifactStateLinkageVerdict,
    reason: String,
    state_archive_dir: Option<String>,
    explicit_state_snapshot_paths: Vec<String>,
    snapshot_latest_pointer_dir: Option<String>,
    snapshot_pointer_name: String,
    review_archive_dir: String,
    review_manifest_dir: String,
    review_bundle_dir: String,
    review_channel_dir: String,
    review_channel_name: String,
    release_archive_dir: String,
    release_history_dir: String,
    latest_pointer_dir: String,
    latest_pointer_name: String,
    current_artifact_state_verdict: String,
    current_artifact_state_reason: String,
    current_review_generation_id: Option<String>,
    current_latest_release_generation_id: Option<String>,
    current_selection_alignment_matches: bool,
    current_selection_alignment_summary: String,
    state_snapshot_count_examined: usize,
    snapshots_resolving_live_generations_count: usize,
    missing_selected_review_generation_count: usize,
    missing_selected_release_generation_count: usize,
    diverged_from_current_review_channel_count: usize,
    diverged_from_current_latest_release_count: usize,
    ambiguous_non_green_snapshot_count: usize,
    invalid_artifact_count: usize,
    invalid_artifacts: Vec<LinkageIssue>,
    representative_snapshot_source: Option<String>,
    representative_snapshot_path: Option<String>,
    representative_snapshot_linkage_verdict: Option<PersistedStateSnapshotLinkageVerdict>,
    representative_snapshot_linkage_reason: Option<String>,
    current_state_snapshot_latest_pointer: StateSnapshotLatestPointerLinkageSummary,
    persisted_state_snapshots: Vec<PersistedStateSnapshotLinkageSummary>,
    artifact_analysis_only: bool,
    execution_untouched: bool,
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
    let mut state_archive_dir: Option<PathBuf> = None;
    let mut state_snapshot_paths = Vec::new();
    let mut snapshot_latest_pointer_dir: Option<PathBuf> = None;
    let mut snapshot_pointer_name =
        activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME.to_string();
    let mut review_archive_dir: Option<PathBuf> = None;
    let mut review_manifest_dir: Option<PathBuf> = None;
    let mut review_bundle_dir: Option<PathBuf> = None;
    let mut review_channel_dir: Option<PathBuf> = None;
    let mut review_channel_name = activation_artifact_channel::DEFAULT_CHANNEL_NAME.to_string();
    let mut release_archive_dir: Option<PathBuf> = None;
    let mut release_history_dir: Option<PathBuf> = None;
    let mut latest_pointer_dir: Option<PathBuf> = None;
    let mut latest_pointer_name =
        activation_artifact_release_publish_report::DEFAULT_LATEST_POINTER_NAME.to_string();
    let mut json = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--state-archive-dir" => {
                state_archive_dir = Some(PathBuf::from(parse_string_arg(
                    "--state-archive-dir",
                    args.next(),
                )?))
            }
            "--state-snapshot" => state_snapshot_paths.push(PathBuf::from(parse_string_arg(
                "--state-snapshot",
                args.next(),
            )?)),
            "--snapshot-latest-pointer-dir" => {
                snapshot_latest_pointer_dir = Some(PathBuf::from(parse_string_arg(
                    "--snapshot-latest-pointer-dir",
                    args.next(),
                )?))
            }
            "--snapshot-pointer-name" => {
                snapshot_pointer_name =
                    parse_name(parse_string_arg("--snapshot-pointer-name", args.next())?)?
            }
            "--review-archive-dir" => {
                review_archive_dir = Some(PathBuf::from(parse_string_arg(
                    "--review-archive-dir",
                    args.next(),
                )?))
            }
            "--review-manifest-dir" => {
                review_manifest_dir = Some(PathBuf::from(parse_string_arg(
                    "--review-manifest-dir",
                    args.next(),
                )?))
            }
            "--review-bundle-dir" => {
                review_bundle_dir = Some(PathBuf::from(parse_string_arg(
                    "--review-bundle-dir",
                    args.next(),
                )?))
            }
            "--review-channel-dir" => {
                review_channel_dir = Some(PathBuf::from(parse_string_arg(
                    "--review-channel-dir",
                    args.next(),
                )?))
            }
            "--review-channel-name" => {
                review_channel_name =
                    parse_name(parse_string_arg("--review-channel-name", args.next())?)?
            }
            "--release-archive-dir" => {
                release_archive_dir = Some(PathBuf::from(parse_string_arg(
                    "--release-archive-dir",
                    args.next(),
                )?))
            }
            "--release-history-dir" => {
                release_history_dir = Some(PathBuf::from(parse_string_arg(
                    "--release-history-dir",
                    args.next(),
                )?))
            }
            "--latest-pointer-dir" => {
                latest_pointer_dir = Some(PathBuf::from(parse_string_arg(
                    "--latest-pointer-dir",
                    args.next(),
                )?))
            }
            "--latest-pointer-name" => {
                latest_pointer_name =
                    parse_name(parse_string_arg("--latest-pointer-name", args.next())?)?
            }
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown arg `{other}`"),
        }
    }

    if state_archive_dir.is_none() && state_snapshot_paths.is_empty() {
        bail!("either --state-archive-dir or --state-snapshot is required");
    }
    if snapshot_latest_pointer_dir.is_some() && state_archive_dir.is_none() {
        bail!("--snapshot-latest-pointer-dir requires --state-archive-dir");
    }

    Ok(Some(Config {
        state_archive_dir,
        state_snapshot_paths,
        snapshot_latest_pointer_dir,
        snapshot_pointer_name,
        review_archive_dir: review_archive_dir
            .ok_or_else(|| anyhow!("--review-archive-dir is required"))?,
        review_manifest_dir: review_manifest_dir
            .ok_or_else(|| anyhow!("--review-manifest-dir is required"))?,
        review_bundle_dir: review_bundle_dir
            .ok_or_else(|| anyhow!("--review-bundle-dir is required"))?,
        review_channel_dir: review_channel_dir
            .ok_or_else(|| anyhow!("--review-channel-dir is required"))?,
        review_channel_name,
        release_archive_dir: release_archive_dir
            .ok_or_else(|| anyhow!("--release-archive-dir is required"))?,
        release_history_dir: release_history_dir
            .ok_or_else(|| anyhow!("--release-history-dir is required"))?,
        latest_pointer_dir: latest_pointer_dir
            .ok_or_else(|| anyhow!("--latest-pointer-dir is required"))?,
        latest_pointer_name,
        json,
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

fn build_report(config: &Config) -> Result<ArtifactStateLinkageReport> {
    let state_report =
        activation_artifact_state_report::inspect_state_report(&activation_artifact_state_report::Config {
            review_archive_dir: config.review_archive_dir.clone(),
            review_manifest_dir: config.review_manifest_dir.clone(),
            review_bundle_dir: config.review_bundle_dir.clone(),
            review_channel_dir: config.review_channel_dir.clone(),
            review_channel_name: config.review_channel_name.clone(),
            release_archive_dir: config.release_archive_dir.clone(),
            release_history_dir: config.release_history_dir.clone(),
            latest_pointer_dir: config.latest_pointer_dir.clone(),
            latest_pointer_name: config.latest_pointer_name.clone(),
            json: false,
        })?;

    let review_generation_ids = load_review_generation_ids(&config.review_archive_dir)?;
    let release_generation_ids =
        load_release_generation_ids(&config.release_archive_dir, &config.release_history_dir)?;
    let (snapshot_records, invalid_artifacts) = load_state_snapshot_records(config)?;
    let mut snapshot_summaries = snapshot_records
        .iter()
        .map(|record| {
            summarize_snapshot_linkage(
                record,
                &review_generation_ids,
                &release_generation_ids,
                &state_report,
            )
        })
        .collect::<Vec<_>>();
    snapshot_summaries.sort_by(|left, right| {
        left.snapshotted_at
            .cmp(&right.snapshotted_at)
            .then_with(|| left.snapshot_path.cmp(&right.snapshot_path))
    });

    let summary_by_path = snapshot_summaries
        .iter()
        .map(|summary| (summary.snapshot_path.clone(), summary.clone()))
        .collect::<BTreeMap<_, _>>();

    let current_snapshot_latest_pointer = inspect_snapshot_latest_pointer(
        config,
        &summary_by_path,
        &review_generation_ids,
        &release_generation_ids,
        &state_report,
    )?;

    let representative_snapshot = if current_snapshot_latest_pointer.requested
        && current_snapshot_latest_pointer.selected_snapshot_loaded
    {
        current_snapshot_latest_pointer
            .selected_snapshot_path
            .as_ref()
            .and_then(|path| summary_by_path.get(path))
            .cloned()
            .map(|summary| ("snapshot_latest_pointer".to_string(), summary))
    } else {
        snapshot_summaries
            .iter()
            .max_by(|left, right| {
                left.snapshotted_at
                    .cmp(&right.snapshotted_at)
                    .then_with(|| left.snapshot_path.cmp(&right.snapshot_path))
            })
            .cloned()
            .map(|summary| ("latest_examined_snapshot".to_string(), summary))
    };

    let snapshots_resolving_live_generations_count = snapshot_summaries
        .iter()
        .filter(|summary| summary.resolves_live_review_release_generations)
        .count();
    let missing_selected_review_generation_count = snapshot_summaries
        .iter()
        .filter(|summary| !summary.selected_review_generation_present_now)
        .count();
    let missing_selected_release_generation_count = snapshot_summaries
        .iter()
        .filter(|summary| !summary.selected_latest_release_generation_present_now)
        .count();
    let diverged_from_current_review_channel_count = snapshot_summaries
        .iter()
        .filter(|summary| !summary.matches_current_review_channel_selection)
        .count();
    let diverged_from_current_latest_release_count = snapshot_summaries
        .iter()
        .filter(|summary| !summary.matches_current_latest_release_selection)
        .count();
    let ambiguous_non_green_snapshot_count = snapshot_summaries
        .iter()
        .filter(|summary| {
            summary.state_verdict != "artifact_state_coherent" || summary.ambiguous_legacy_count > 0
        })
        .count();

    let (verdict, reason) = determine_report_verdict(
        &state_report,
        &invalid_artifacts,
        representative_snapshot.as_ref().map(|(_, summary)| summary),
        &current_snapshot_latest_pointer,
    );

    Ok(ArtifactStateLinkageReport {
        mode: "artifact_state_linkage_report".to_string(),
        verdict,
        reason,
        state_archive_dir: config
            .state_archive_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        explicit_state_snapshot_paths: config
            .state_snapshot_paths
            .iter()
            .map(|path| path.display().to_string())
            .collect(),
        snapshot_latest_pointer_dir: config
            .snapshot_latest_pointer_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        snapshot_pointer_name: config.snapshot_pointer_name.clone(),
        review_archive_dir: config.review_archive_dir.display().to_string(),
        review_manifest_dir: config.review_manifest_dir.display().to_string(),
        review_bundle_dir: config.review_bundle_dir.display().to_string(),
        review_channel_dir: config.review_channel_dir.display().to_string(),
        review_channel_name: config.review_channel_name.clone(),
        release_archive_dir: config.release_archive_dir.display().to_string(),
        release_history_dir: config.release_history_dir.display().to_string(),
        latest_pointer_dir: config.latest_pointer_dir.display().to_string(),
        latest_pointer_name: config.latest_pointer_name.clone(),
        current_artifact_state_verdict: serialize_enum(&state_report.verdict),
        current_artifact_state_reason: state_report.reason.clone(),
        current_review_generation_id: state_report.current_review_generation.selected_generation_id,
        current_latest_release_generation_id: state_report.current_latest_release.generation_id,
        current_selection_alignment_matches: state_report.selection_alignment.selections_match,
        current_selection_alignment_summary: state_report.selection_alignment.summary,
        state_snapshot_count_examined: snapshot_summaries.len(),
        snapshots_resolving_live_generations_count,
        missing_selected_review_generation_count,
        missing_selected_release_generation_count,
        diverged_from_current_review_channel_count,
        diverged_from_current_latest_release_count,
        ambiguous_non_green_snapshot_count,
        invalid_artifact_count: invalid_artifacts.len(),
        invalid_artifacts,
        representative_snapshot_source: representative_snapshot
            .as_ref()
            .map(|(source, _)| source.clone()),
        representative_snapshot_path: representative_snapshot
            .as_ref()
            .map(|(_, summary)| summary.snapshot_path.clone()),
        representative_snapshot_linkage_verdict: representative_snapshot
            .as_ref()
            .map(|(_, summary)| summary.linkage_verdict),
        representative_snapshot_linkage_reason: representative_snapshot
            .as_ref()
            .map(|(_, summary)| summary.linkage_reason.clone()),
        current_state_snapshot_latest_pointer: current_snapshot_latest_pointer,
        persisted_state_snapshots: snapshot_summaries,
        artifact_analysis_only: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary: "Artifact-state linkage analysis only correlates persisted state snapshots with the current review/release artifact chain. It does not authorize activation, does not enable execution, and does not override the Stage 3 production gate.".to_string(),
    })
}

fn determine_report_verdict(
    state_report: &activation_artifact_state_report::ArtifactStateReport,
    invalid_artifacts: &[LinkageIssue],
    representative_snapshot: Option<&PersistedStateSnapshotLinkageSummary>,
    latest_pointer: &StateSnapshotLatestPointerLinkageSummary,
) -> (ArtifactStateLinkageVerdict, String) {
    if !invalid_artifacts.is_empty()
        || state_report.verdict
            == activation_artifact_state_report::ArtifactStateVerdict::ArtifactStateInvalidArtifactsPresent
        || latest_pointer.pointer_verdict.as_deref()
            == Some("artifact_state_snapshot_invalid_metadata")
    {
        return (
            ArtifactStateLinkageVerdict::ArtifactStateLinkageInvalidArtifactsPresent,
            "state snapshot linkage inputs contain invalid artifacts or invalid pointer metadata"
                .to_string(),
        );
    }

    let pointer_missing_target = latest_pointer.requested
        && latest_pointer.pointer_verdict.as_deref()
            == Some("artifact_state_snapshot_verify_missing_target")
        && latest_pointer.pointer_path.is_some()
        && !latest_pointer.target_exists;
    let pointer_missing_metadata = latest_pointer.requested
        && latest_pointer.pointer_verdict.as_deref()
            == Some("artifact_state_snapshot_verify_missing_target")
        && latest_pointer.pointer_path.is_none();

    if pointer_missing_target {
        return (
            ArtifactStateLinkageVerdict::ArtifactStateLinkageInconsistent,
            "state snapshot latest pointer metadata exists, but its target snapshot artifact is missing".to_string(),
        );
    }

    if state_report.verdict
        == activation_artifact_state_report::ArtifactStateVerdict::ArtifactStateInconsistent
        || representative_snapshot.is_some_and(|summary| {
            summary.linkage_verdict == PersistedStateSnapshotLinkageVerdict::Inconsistent
        })
    {
        return (
            ArtifactStateLinkageVerdict::ArtifactStateLinkageInconsistent,
            representative_snapshot
                .map(|summary| summary.linkage_reason.clone())
                .filter(|reason| !reason.is_empty())
                .unwrap_or_else(|| {
                    "the current or representative persisted state snapshot no longer links cleanly to the current artifact chain".to_string()
                }),
        );
    }

    if state_report.verdict
        == activation_artifact_state_report::ArtifactStateVerdict::ArtifactStateIncomplete
        || representative_snapshot.is_none()
        || pointer_missing_metadata
        || representative_snapshot.is_some_and(|summary| {
            summary.linkage_verdict == PersistedStateSnapshotLinkageVerdict::Incomplete
        })
    {
        return (
            ArtifactStateLinkageVerdict::ArtifactStateLinkageIncomplete,
            representative_snapshot
                .map(|summary| summary.linkage_reason.clone())
                .filter(|reason| !reason.is_empty())
                .unwrap_or_else(|| {
                    if pointer_missing_metadata {
                        "no state snapshot latest pointer metadata was found, so current snapshot linkage cannot be confirmed".to_string()
                    } else {
                        "the representative persisted state snapshot does not fully resolve against the current artifact chain".to_string()
                    }
                }),
        );
    }

    if state_report.verdict
        == activation_artifact_state_report::ArtifactStateVerdict::ArtifactStateAmbiguousLegacyState
        || representative_snapshot.is_some_and(|summary| {
            summary.linkage_verdict == PersistedStateSnapshotLinkageVerdict::AmbiguousLegacyState
        })
    {
        return (
            ArtifactStateLinkageVerdict::ArtifactStateLinkageAmbiguousLegacyState,
            representative_snapshot
                .map(|summary| summary.linkage_reason.clone())
                .filter(|reason| !reason.is_empty())
                .unwrap_or_else(|| {
                    "the representative persisted state snapshot depends on ambiguous legacy state and cannot be treated as clean linkage".to_string()
                }),
        );
    }

    (
        ArtifactStateLinkageVerdict::ArtifactStateLinkageComplete,
        "the representative persisted state snapshot still resolves cleanly against the current review and release artifact chain".to_string(),
    )
}

fn load_review_generation_ids(review_archive_dir: &Path) -> Result<BTreeSet<String>> {
    let inventory = activation_artifact_archive::archive_inventory(review_archive_dir)?;
    Ok(inventory
        .generation_summaries
        .iter()
        .map(activation_artifact_archive::generation_id_from_summary)
        .collect())
}

fn load_release_generation_ids(
    release_archive_dir: &Path,
    release_history_dir: &Path,
) -> Result<BTreeSet<String>> {
    let mut generation_ids = BTreeSet::new();
    let mut seen = BTreeSet::new();
    for dir in [release_archive_dir, release_history_dir] {
        for path in collect_json_paths(dir)? {
            let canonical = fs::canonicalize(&path).unwrap_or_else(|_| path.clone());
            if !seen.insert(canonical) {
                continue;
            }
            let loaded = activation_artifact_release_history::inspect_release_artifact(&path)?;
            if let Some(generation_id) = loaded.artifact.generation_id {
                generation_ids.insert(generation_id);
            }
        }
    }
    Ok(generation_ids)
}

fn load_state_snapshot_records(config: &Config) -> Result<(Vec<StateSnapshotRecord>, Vec<LinkageIssue>)> {
    let mut records = Vec::new();
    let mut invalid_artifacts = Vec::new();
    let mut seen = BTreeSet::new();
    let mut candidate_paths = Vec::new();
    if let Some(state_archive_dir) = &config.state_archive_dir {
        candidate_paths.extend(collect_json_paths(state_archive_dir)?);
    }
    candidate_paths.extend(config.state_snapshot_paths.iter().cloned());

    for path in candidate_paths {
        let key = fs::canonicalize(&path).unwrap_or_else(|_| path.clone());
        if !seen.insert(key) {
            continue;
        }
        match activation_artifact_state_publish_report::inspect_state_snapshot_artifact(&path) {
            Ok(loaded) => records.push(StateSnapshotRecord {
                identity_key: snapshot_identity_key(&loaded.artifact, &loaded.canonical_path),
                loaded,
            }),
            Err(error) => invalid_artifacts.push(LinkageIssue {
                surface: "state_snapshot".to_string(),
                path: path.display().to_string(),
                error: format!("{error:#}"),
            }),
        }
    }

    records.sort_by(|left, right| {
        left.loaded
            .artifact
            .snapshotted_at
            .cmp(&right.loaded.artifact.snapshotted_at)
            .then_with(|| left.loaded.canonical_path.cmp(&right.loaded.canonical_path))
    });

    Ok((records, invalid_artifacts))
}

fn inspect_snapshot_latest_pointer(
    config: &Config,
    summaries_by_path: &BTreeMap<String, PersistedStateSnapshotLinkageSummary>,
    review_generation_ids: &BTreeSet<String>,
    release_generation_ids: &BTreeSet<String>,
    state_report: &activation_artifact_state_report::ArtifactStateReport,
) -> Result<StateSnapshotLatestPointerLinkageSummary> {
    let Some(pointer_dir) = &config.snapshot_latest_pointer_dir else {
        return Ok(StateSnapshotLatestPointerLinkageSummary {
            requested: false,
            pointer_name: config.snapshot_pointer_name.clone(),
            pointer_verdict: None,
            pointer_reason: "state snapshot latest pointer was not requested".to_string(),
            pointer_path: None,
            selected_snapshot_path: None,
            selected_snapshot_linkage_verdict: None,
            selected_snapshot_linkage_reason: None,
            target_exists: false,
            target_matches_identity: false,
            selected_snapshot_loaded: false,
            selected_review_generation_id: None,
            selected_latest_release_generation_id: None,
            selected_snapshot_resolves_live_generations: false,
            selected_snapshot_matches_current_review_channel: false,
            selected_snapshot_matches_current_latest_release: false,
        });
    };

    let Some(state_archive_dir) = &config.state_archive_dir else {
        bail!("--snapshot-latest-pointer-dir requires --state-archive-dir");
    };

    let loaded_metadata = activation_artifact_state_publish_report::inspect_latest_pointer_metadata(
        pointer_dir,
        &config.snapshot_pointer_name,
    )
    .ok()
    .flatten();
    let inspected = activation_artifact_state_publish_report::inspect_latest_pointer_report(
        state_archive_dir,
        pointer_dir,
        &config.snapshot_pointer_name,
        true,
    )?;
    let pointer_path = inspected.latest_pointer_path.clone();
    let selected_snapshot_path = inspected
        .persisted_state_snapshot_path
        .clone()
        .or_else(|| {
            loaded_metadata
                .as_ref()
                .map(|metadata| metadata.metadata.selected_snapshot_path.clone())
        });
    let selected_summary = selected_snapshot_path
        .as_ref()
        .and_then(|path| canonicalize_existing_path(Path::new(path)))
        .and_then(|path| summaries_by_path.get(path.to_string_lossy().as_ref()).cloned())
        .or_else(|| {
            selected_snapshot_path
                .as_ref()
                .and_then(|path| summaries_by_path.get(path).cloned())
        })
        .or_else(|| {
            selected_snapshot_path.as_ref().and_then(|path| {
                let loaded = activation_artifact_state_publish_report::inspect_state_snapshot_artifact(
                    Path::new(path),
                )
                .ok()?;
                Some(summarize_snapshot_linkage(
                    &StateSnapshotRecord {
                        identity_key: snapshot_identity_key(&loaded.artifact, &loaded.canonical_path),
                        loaded,
                    },
                    review_generation_ids,
                    release_generation_ids,
                    state_report,
                ))
            })
        });

    Ok(StateSnapshotLatestPointerLinkageSummary {
        requested: true,
        pointer_name: config.snapshot_pointer_name.clone(),
        pointer_verdict: Some(serialize_enum(&inspected.verdict)),
        pointer_reason: inspected.reason.clone(),
        pointer_path,
        selected_snapshot_path,
        selected_snapshot_linkage_verdict: selected_summary
            .as_ref()
            .map(|summary| summary.linkage_verdict),
        selected_snapshot_linkage_reason: selected_summary
            .as_ref()
            .map(|summary| summary.linkage_reason.clone()),
        target_exists: inspected.latest_pointer_target_exists,
        target_matches_identity: inspected.latest_pointer_target_matches_identity,
        selected_snapshot_loaded: selected_summary.is_some(),
        selected_review_generation_id: selected_summary
            .as_ref()
            .and_then(|summary| summary.selected_review_generation_id.clone())
            .or_else(|| {
                loaded_metadata
                    .as_ref()
                    .and_then(|metadata| metadata.metadata.selected_review_generation_id.clone())
            }),
        selected_latest_release_generation_id: selected_summary
            .as_ref()
            .and_then(|summary| summary.selected_latest_release_generation_id.clone())
            .or_else(|| {
                loaded_metadata.as_ref().and_then(|metadata| {
                    metadata
                        .metadata
                        .selected_latest_release_generation_id
                        .clone()
                })
            }),
        selected_snapshot_resolves_live_generations: selected_summary
            .as_ref()
            .is_some_and(|summary| summary.resolves_live_review_release_generations),
        selected_snapshot_matches_current_review_channel: selected_summary
            .as_ref()
            .is_some_and(|summary| summary.matches_current_review_channel_selection),
        selected_snapshot_matches_current_latest_release: selected_summary
            .as_ref()
            .is_some_and(|summary| summary.matches_current_latest_release_selection),
    })
}

fn summarize_snapshot_linkage(
    record: &StateSnapshotRecord,
    review_generation_ids: &BTreeSet<String>,
    release_generation_ids: &BTreeSet<String>,
    state_report: &activation_artifact_state_report::ArtifactStateReport,
) -> PersistedStateSnapshotLinkageSummary {
    let artifact = &record.loaded.artifact;
    let selected_review_generation_present_now = artifact
        .selected_review_generation_id
        .as_ref()
        .is_some_and(|generation_id| review_generation_ids.contains(generation_id));
    let selected_latest_release_generation_present_now = artifact
        .selected_latest_release_generation_id
        .as_ref()
        .is_some_and(|generation_id| release_generation_ids.contains(generation_id));
    let resolves_live_review_release_generations =
        selected_review_generation_present_now && selected_latest_release_generation_present_now;
    let matches_current_review_channel_selection =
        artifact.selected_review_generation_id
            == state_report.current_review_generation.selected_generation_id;
    let matches_current_latest_release_selection =
        artifact.selected_latest_release_generation_id
            == state_report.current_latest_release.generation_id;

    let mut incomplete_reasons = Vec::new();
    let mut inconsistencies = Vec::new();
    let mut ambiguities = Vec::new();

    match artifact.state_verdict.as_str() {
        "artifact_state_inconsistent" => inconsistencies.push(
            "persisted state snapshot summarizes an inconsistent current-state artifact chain"
                .to_string(),
        ),
        "artifact_state_incomplete" => incomplete_reasons.push(
            "persisted state snapshot summarizes an incomplete current-state artifact chain"
                .to_string(),
        ),
        "artifact_state_ambiguous_legacy_state" => ambiguities.push(
            "persisted state snapshot summarizes ambiguous legacy artifact state".to_string(),
        ),
        "artifact_state_invalid_artifacts_present" => inconsistencies.push(
            "persisted state snapshot summarizes invalid current-state artifacts".to_string(),
        ),
        _ => {}
    }

    if !artifact.coherent_for_review_operations && artifact.state_verdict == "artifact_state_coherent"
    {
        inconsistencies.push(
            "persisted state snapshot claims coherent state_verdict but is not coherent for review operations"
                .to_string(),
        );
    }
    if artifact.ambiguous_legacy_count > 0 && artifact.state_verdict != "artifact_state_ambiguous_legacy_state" {
        ambiguities.push(format!(
            "persisted state snapshot carries {} ambiguous legacy conditions",
            artifact.ambiguous_legacy_count
        ));
    }
    if !selected_review_generation_present_now {
        incomplete_reasons.push(
            artifact
                .selected_review_generation_id
                .as_ref()
                .map(|generation_id| {
                    format!(
                        "selected review generation `{generation_id}` no longer resolves in the current review archive"
                    )
                })
                .unwrap_or_else(|| {
                    "persisted state snapshot does not record a selected review generation"
                        .to_string()
                }),
        );
    }
    if !selected_latest_release_generation_present_now {
        incomplete_reasons.push(
            artifact
                .selected_latest_release_generation_id
                .as_ref()
                .map(|generation_id| {
                    format!(
                        "selected latest release generation `{generation_id}` no longer resolves in the current release archive/history"
                    )
                })
                .unwrap_or_else(|| {
                    "persisted state snapshot does not record a selected latest release generation"
                        .to_string()
                }),
        );
    }
    if resolves_live_review_release_generations && !matches_current_review_channel_selection {
        incomplete_reasons.push(
            "persisted state snapshot selected review generation no longer matches the current review channel selection"
                .to_string(),
        );
    }
    if resolves_live_review_release_generations && !matches_current_latest_release_selection {
        incomplete_reasons.push(
            "persisted state snapshot selected latest release generation no longer matches the current latest release selection"
                .to_string(),
        );
    }

    let (linkage_verdict, linkage_reason) = if !inconsistencies.is_empty() {
        (
            PersistedStateSnapshotLinkageVerdict::Inconsistent,
            inconsistencies
                .first()
                .cloned()
                .unwrap_or_else(|| "persisted state snapshot linkage is inconsistent".to_string()),
        )
    } else if !incomplete_reasons.is_empty() {
        (
            PersistedStateSnapshotLinkageVerdict::Incomplete,
            incomplete_reasons
                .first()
                .cloned()
                .unwrap_or_else(|| "persisted state snapshot linkage is incomplete".to_string()),
        )
    } else if !ambiguities.is_empty() {
        (
            PersistedStateSnapshotLinkageVerdict::AmbiguousLegacyState,
            ambiguities
                .first()
                .cloned()
                .unwrap_or_else(|| "persisted state snapshot linkage is ambiguous".to_string()),
        )
    } else {
        (
            PersistedStateSnapshotLinkageVerdict::Complete,
            "persisted state snapshot selections still resolve cleanly against the current review and release artifact chain"
                .to_string(),
        )
    };

    PersistedStateSnapshotLinkageSummary {
        snapshot_path: record.loaded.canonical_path.display().to_string(),
        snapshot_file_name: record
            .loaded
            .canonical_path
            .file_name()
            .map(|value| value.to_string_lossy().into_owned())
            .unwrap_or_else(|| "<unknown>".to_string()),
        identity_key: record.identity_key.clone(),
        snapshotted_at: artifact.snapshotted_at,
        state_verdict: artifact.state_verdict.clone(),
        state_reason: artifact.state_reason.clone(),
        linkage_verdict,
        linkage_reason,
        selected_review_generation_id: artifact.selected_review_generation_id.clone(),
        selected_latest_release_generation_id: artifact.selected_latest_release_generation_id.clone(),
        selection_alignment_matches: artifact.selection_alignment_matches,
        selection_alignment_summary: artifact.selection_alignment_summary.clone(),
        coherent_for_review_operations: artifact.coherent_for_review_operations,
        ambiguous_legacy_count: artifact.ambiguous_legacy_count,
        selected_review_generation_present_now,
        selected_latest_release_generation_present_now,
        resolves_live_review_release_generations,
        matches_current_review_channel_selection,
        matches_current_latest_release_selection,
        incomplete_reasons,
        inconsistencies,
        ambiguities,
    }
}

fn collect_json_paths(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    if !dir.exists() {
        return Ok(paths);
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) == Some("json") {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

fn snapshot_identity_key(
    artifact: &activation_artifact_state_publish_report::ArtifactStateSnapshotArtifact,
    canonical_path: &Path,
) -> String {
    format!(
        "{}|{}|{}|{}|{}",
        artifact.snapshotted_at.to_rfc3339(),
        artifact.state_verdict,
        artifact
            .selected_review_generation_id
            .clone()
            .unwrap_or_else(|| "<none>".to_string()),
        artifact
            .selected_latest_release_generation_id
            .clone()
            .unwrap_or_else(|| "<none>".to_string()),
        canonical_path.display()
    )
}

fn canonicalize_existing_path(path: &Path) -> Option<PathBuf> {
    if !path.exists() {
        return None;
    }
    fs::canonicalize(path).ok()
}

fn render_human(report: &ArtifactStateLinkageReport) -> String {
    let pointer_verdict = report
        .current_state_snapshot_latest_pointer
        .pointer_verdict
        .clone()
        .unwrap_or_else(|| "not_requested".to_string());
    [
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!(
            "state_archive_dir={}",
            report.state_archive_dir.as_deref().unwrap_or("<none>")
        ),
        format!(
            "state_snapshot_count_examined={}",
            report.state_snapshot_count_examined
        ),
        format!(
            "snapshots_resolving_live_generations_count={}",
            report.snapshots_resolving_live_generations_count
        ),
        format!(
            "missing_selected_review_generation_count={}",
            report.missing_selected_review_generation_count
        ),
        format!(
            "missing_selected_release_generation_count={}",
            report.missing_selected_release_generation_count
        ),
        format!(
            "diverged_from_current_review_channel_count={}",
            report.diverged_from_current_review_channel_count
        ),
        format!(
            "diverged_from_current_latest_release_count={}",
            report.diverged_from_current_latest_release_count
        ),
        format!(
            "ambiguous_non_green_snapshot_count={}",
            report.ambiguous_non_green_snapshot_count
        ),
        format!("invalid_artifact_count={}", report.invalid_artifact_count),
        format!(
            "current_artifact_state_verdict={}",
            report.current_artifact_state_verdict
        ),
        format!(
            "current_review_generation_id={}",
            report
                .current_review_generation_id
                .as_deref()
                .unwrap_or("<none>")
        ),
        format!(
            "current_latest_release_generation_id={}",
            report
                .current_latest_release_generation_id
                .as_deref()
                .unwrap_or("<none>")
        ),
        format!(
            "representative_snapshot_path={}",
            report
                .representative_snapshot_path
                .as_deref()
                .unwrap_or("<none>")
        ),
        format!(
            "representative_snapshot_linkage_verdict={}",
            report
                .representative_snapshot_linkage_verdict
                .map(|value| serialize_enum(&value))
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!("snapshot_latest_pointer_verdict={pointer_verdict}"),
        format!("artifact_analysis_only={}", report.artifact_analysis_only),
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
    fn coherent_snapshot_whose_selected_generations_still_exist_yields_green_linkage() {
        let fixture = write_complete_fixture("artifact_state_linkage_complete");
        let state_archive_dir = fixture.root.join("state_snapshots");
        let snapshot_path = state_archive_dir.join("snapshot.json");
        write_snapshot(
            &snapshot_path,
            "artifact_state_coherent",
            &fixture.generation.generation_id,
            &fixture.generation.generation_id,
            true,
            0,
            ts("2026-03-26T18:00:00Z"),
        );

        let report = build_report(&fixture.config(Some(state_archive_dir), None)).expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateLinkageVerdict::ArtifactStateLinkageComplete
        );
        assert_eq!(report.snapshots_resolving_live_generations_count, 1);
        assert_eq!(
            report.representative_snapshot_linkage_verdict,
            Some(PersistedStateSnapshotLinkageVerdict::Complete)
        );
    }

    #[test]
    fn snapshot_referencing_missing_review_generation_is_non_green() {
        let fixture = write_complete_fixture("artifact_state_linkage_missing_review");
        let state_archive_dir = fixture.root.join("state_snapshots");
        let snapshot_path = state_archive_dir.join("snapshot.json");
        write_snapshot(
            &snapshot_path,
            "artifact_state_coherent",
            "missing_review_generation",
            &fixture.generation.generation_id,
            false,
            0,
            ts("2026-03-26T18:00:00Z"),
        );

        let report = build_report(&fixture.config(Some(state_archive_dir), None)).expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateLinkageVerdict::ArtifactStateLinkageIncomplete
        );
        assert_eq!(report.missing_selected_review_generation_count, 1);
    }

    #[test]
    fn snapshot_referencing_missing_release_generation_is_non_green() {
        let fixture = write_complete_fixture("artifact_state_linkage_missing_release");
        let state_archive_dir = fixture.root.join("state_snapshots");
        let snapshot_path = state_archive_dir.join("snapshot.json");
        write_snapshot(
            &snapshot_path,
            "artifact_state_coherent",
            &fixture.generation.generation_id,
            "missing_release_generation",
            false,
            0,
            ts("2026-03-26T18:00:00Z"),
        );

        let report = build_report(&fixture.config(Some(state_archive_dir), None)).expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateLinkageVerdict::ArtifactStateLinkageIncomplete
        );
        assert_eq!(report.missing_selected_release_generation_count, 1);
    }

    #[test]
    fn latest_state_snapshot_pointer_resolving_to_broken_snapshot_linkage_is_surfaced() {
        let fixture = write_complete_fixture("artifact_state_linkage_pointer_broken");
        let state_archive_dir = fixture.root.join("state_snapshots");
        let pointer_dir = fixture.root.join("state_latest");
        let snapshot_path = state_archive_dir.join("snapshot.json");
        write_snapshot(
            &snapshot_path,
            "artifact_state_coherent",
            &fixture.generation.generation_id,
            "missing_release_generation",
            false,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        write_state_pointer_metadata(
            &pointer_dir,
            activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME,
            &state_archive_dir,
            &snapshot_path,
        );

        let report = build_report(&fixture.config(Some(state_archive_dir), Some(pointer_dir)))
            .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateLinkageVerdict::ArtifactStateLinkageIncomplete
        );
        assert_eq!(
            report
                .current_state_snapshot_latest_pointer
                .selected_snapshot_linkage_verdict,
            Some(PersistedStateSnapshotLinkageVerdict::Incomplete)
        );
        assert!(
            report
                .current_state_snapshot_latest_pointer
                .selected_snapshot_loaded
        );
    }

    #[test]
    fn missing_state_snapshot_latest_pointer_target_is_explicitly_broken() {
        let fixture = write_complete_fixture("artifact_state_linkage_pointer_missing_target");
        let state_archive_dir = fixture.root.join("state_snapshots");
        let pointer_dir = fixture.root.join("state_latest");
        let snapshot_path = state_archive_dir.join("snapshot.json");
        write_snapshot(
            &snapshot_path,
            "artifact_state_coherent",
            &fixture.generation.generation_id,
            &fixture.generation.generation_id,
            true,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        let missing_target_path = state_archive_dir.join("missing_snapshot.json");
        write_missing_target_state_pointer_metadata(
            &pointer_dir,
            activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME,
            &state_archive_dir,
            &missing_target_path,
            &fixture.generation.generation_id,
        );

        let report = build_report(&fixture.config(Some(state_archive_dir), Some(pointer_dir)))
            .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateLinkageVerdict::ArtifactStateLinkageInconsistent
        );
        assert!(report.reason.contains("target snapshot artifact is missing"));
        assert_eq!(
            report.current_state_snapshot_latest_pointer.pointer_verdict.as_deref(),
            Some("artifact_state_snapshot_verify_missing_target")
        );
        assert_eq!(
            report
                .current_state_snapshot_latest_pointer
                .selected_snapshot_path
                .as_deref(),
            Some(missing_target_path.to_string_lossy().as_ref())
        );
    }

    #[test]
    fn missing_state_snapshot_latest_pointer_target_beats_clean_representative_reason() {
        let fixture =
            write_complete_fixture("artifact_state_linkage_pointer_missing_target_clean_repr");
        let state_archive_dir = fixture.root.join("state_snapshots");
        let pointer_dir = fixture.root.join("state_latest");
        let older_snapshot_path = state_archive_dir.join("older_snapshot.json");
        let newer_snapshot_path = state_archive_dir.join("newer_snapshot.json");
        write_snapshot(
            &older_snapshot_path,
            "artifact_state_coherent",
            &fixture.generation.generation_id,
            &fixture.generation.generation_id,
            true,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        write_snapshot(
            &newer_snapshot_path,
            "artifact_state_coherent",
            &fixture.generation.generation_id,
            &fixture.generation.generation_id,
            true,
            0,
            ts("2026-03-26T19:00:00Z"),
        );
        let missing_target_path = state_archive_dir.join("missing_snapshot.json");
        write_missing_target_state_pointer_metadata(
            &pointer_dir,
            activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME,
            &state_archive_dir,
            &missing_target_path,
            &fixture.generation.generation_id,
        );

        let report = build_report(&fixture.config(Some(state_archive_dir), Some(pointer_dir)))
            .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateLinkageVerdict::ArtifactStateLinkageInconsistent
        );
        assert!(report.reason.contains("target snapshot artifact is missing"));
        assert_eq!(
            report.representative_snapshot_linkage_verdict,
            Some(PersistedStateSnapshotLinkageVerdict::Complete)
        );
        assert_eq!(
            report
                .representative_snapshot_path
                .as_deref()
                .map(|path| Path::new(path).file_name().expect("file").to_string_lossy().into_owned()),
            Some("newer_snapshot.json".to_string())
        );
    }

    #[test]
    fn ambiguous_non_green_snapshot_is_not_treated_as_clean_green() {
        let fixture = write_complete_fixture("artifact_state_linkage_ambiguous");
        let state_archive_dir = fixture.root.join("state_snapshots");
        let snapshot_path = state_archive_dir.join("snapshot.json");
        write_snapshot(
            &snapshot_path,
            "artifact_state_ambiguous_legacy_state",
            &fixture.generation.generation_id,
            &fixture.generation.generation_id,
            true,
            2,
            ts("2026-03-26T18:00:00Z"),
        );

        let report = build_report(&fixture.config(Some(state_archive_dir), None)).expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateLinkageVerdict::ArtifactStateLinkageAmbiguousLegacyState
        );
        assert_eq!(report.ambiguous_non_green_snapshot_count, 1);
    }

    #[derive(Debug)]
    struct Fixture {
        root: PathBuf,
        generation: ReviewGenerationFixture,
        review_archive_dir: PathBuf,
        review_manifest_dir: PathBuf,
        review_bundle_dir: PathBuf,
        review_channel_dir: PathBuf,
        release_archive_dir: PathBuf,
        release_history_dir: PathBuf,
        latest_pointer_dir: PathBuf,
    }

    impl Fixture {
        fn config(
            &self,
            state_archive_dir: Option<PathBuf>,
            snapshot_latest_pointer_dir: Option<PathBuf>,
        ) -> Config {
            Config {
                state_archive_dir,
                state_snapshot_paths: Vec::new(),
                snapshot_latest_pointer_dir,
                snapshot_pointer_name:
                    activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                        .to_string(),
                review_archive_dir: self.review_archive_dir.clone(),
                review_manifest_dir: self.review_manifest_dir.clone(),
                review_bundle_dir: self.review_bundle_dir.clone(),
                review_channel_dir: self.review_channel_dir.clone(),
                review_channel_name: activation_artifact_channel::DEFAULT_CHANNEL_NAME.to_string(),
                release_archive_dir: self.release_archive_dir.clone(),
                release_history_dir: self.release_history_dir.clone(),
                latest_pointer_dir: self.latest_pointer_dir.clone(),
                latest_pointer_name:
                    activation_artifact_release_publish_report::DEFAULT_LATEST_POINTER_NAME
                        .to_string(),
                json: false,
            }
        }
    }

    #[derive(Debug, Clone)]
    struct GenerationIdentity {
        generated_at: DateTime<Utc>,
        prod_fp: String,
        non_prod_fp: String,
    }

    #[derive(Debug, Clone)]
    struct ReviewGenerationFixture {
        identity: GenerationIdentity,
        generation_id: String,
        generation_dir: String,
        packet_path: String,
        runbook_json_path: String,
        runbook_markdown_path: String,
    }

    fn write_complete_fixture(prefix: &str) -> Fixture {
        let root = temp_dir(prefix);
        let review_archive_dir = root.join("review_archive");
        let review_manifest_dir = root.join("review_manifest");
        let review_bundle_dir = root.join("review_bundle");
        let review_channel_dir = root.join("review_channel");
        let release_archive_dir = root.join("release_archive");
        let release_history_dir = root.join("release_history");
        let latest_pointer_dir = root.join("release_latest");

        let generation = write_review_generation(
            &review_archive_dir,
            "2026-03-26T12:00:00Z",
            "prod_fp",
            "non_prod_fp",
        );
        fs::create_dir_all(&review_manifest_dir).expect("manifest dir");
        fs::write(
            review_manifest_dir.join("manifest.json"),
            serde_json::to_string_pretty(&build_manifest_document(std::slice::from_ref(
                &generation.identity,
            )))
            .expect("manifest json"),
        )
        .expect("write manifest");
        write_bundle_manifest(&review_bundle_dir.join("bundle"), &generation.identity);
        write_review_channel(
            &review_channel_dir,
            &review_archive_dir,
            &generation,
            activation_artifact_channel::DEFAULT_CHANNEL_NAME,
        );

        let release_artifact_path = release_archive_dir
            .join("release__2026-03-26T12-00-00Z__artifact_release_published_and_promoted.json");
        write_release_artifact(
            &release_artifact_path,
            Some("2026-03-26T12:00:00Z"),
            Some(&generation.generation_id),
            Some(&generation.generation_dir),
            Some(&generation.packet_path),
            Some(&generation.runbook_json_path),
            Some(&generation.runbook_markdown_path),
            "artifact_release_published_and_promoted",
        );
        fs::create_dir_all(&release_history_dir).expect("history dir");
        fs::copy(
            &release_artifact_path,
            release_history_dir.join(
                release_artifact_path
                    .file_name()
                    .expect("release artifact file name"),
            ),
        )
        .expect("copy release history");
        write_latest_pointer(
            &latest_pointer_dir,
            &release_archive_dir,
            &release_artifact_path,
            Some("2026-03-26T12:00:00Z"),
            "released_at",
            Some(&generation.generation_id),
            "artifact_release_published_and_promoted",
        );

        Fixture {
            root,
            generation,
            review_archive_dir,
            review_manifest_dir,
            review_bundle_dir,
            review_channel_dir,
            release_archive_dir,
            release_history_dir,
            latest_pointer_dir,
        }
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
            review_channel_name: activation_artifact_channel::DEFAULT_CHANNEL_NAME.to_string(),
            release_archive_dir: "/tmp/release_archive".to_string(),
            release_history_dir: "/tmp/release_history".to_string(),
            latest_release_pointer_dir: "/tmp/release_pointer".to_string(),
            latest_release_pointer_name:
                activation_artifact_release_publish_report::DEFAULT_LATEST_POINTER_NAME
                    .to_string(),
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

    fn write_state_pointer_metadata(
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
            pointed_at: ts("2026-03-26T18:05:00Z"),
            build_version: "0.1.0".to_string(),
            git_commit: Some("deadbeef".to_string()),
        };
        fs::write(
            pointer_dir.join(format!("{pointer_name}.json")),
            serde_json::to_string_pretty(&metadata).expect("pointer json"),
        )
        .expect("write pointer");
    }

    fn write_missing_target_state_pointer_metadata(
        pointer_dir: &Path,
        pointer_name: &str,
        archive_dir: &Path,
        missing_snapshot_path: &Path,
        generation_id: &str,
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
            selected_review_generation_id: Some(generation_id.to_string()),
            selected_latest_release_generation_id: Some(generation_id.to_string()),
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

    fn write_review_generation(
        review_archive_dir: &Path,
        generated_at: &str,
        prod_fp: &str,
        non_prod_fp: &str,
    ) -> ReviewGenerationFixture {
        let stamp = generated_at.replace(':', "-");
        let generation_dir =
            review_archive_dir.join(format!("{}__{}__{}", stamp, prod_fp, non_prod_fp));
        fs::create_dir_all(&generation_dir).expect("create generation dir");
        let packet_path = generation_dir.join("decision_packet.json");
        let runbook_json_path = generation_dir.join("activation_runbook.json");
        let runbook_markdown_path = generation_dir.join("activation_runbook.md");
        fs::write(
            &packet_path,
            serde_json::to_string_pretty(&sample_packet_json(generated_at, prod_fp, non_prod_fp))
                .expect("packet json"),
        )
        .expect("write packet");
        fs::write(
            &runbook_json_path,
            serde_json::to_string_pretty(&sample_runbook_json(
                "2026-03-26T12:05:00Z",
                generated_at,
                prod_fp,
                non_prod_fp,
            ))
            .expect("runbook json"),
        )
        .expect("write runbook json");
        fs::write(
            &runbook_markdown_path,
            "# Tiny-Live Activation Runbook\n\nsample\n",
        )
        .expect("write runbook markdown");

        let identity = GenerationIdentity {
            generated_at: ts(generated_at),
            prod_fp: prod_fp.to_string(),
            non_prod_fp: non_prod_fp.to_string(),
        };

        ReviewGenerationFixture {
            generation_id: format!(
                "{}|{}|{}",
                identity.generated_at.to_rfc3339(),
                prod_fp,
                non_prod_fp
            ),
            generation_dir: generation_dir.display().to_string(),
            packet_path: packet_path.display().to_string(),
            runbook_json_path: runbook_json_path.display().to_string(),
            runbook_markdown_path: runbook_markdown_path.display().to_string(),
            identity,
        }
    }

    fn write_review_channel(
        review_channel_dir: &Path,
        review_archive_dir: &Path,
        generation: &ReviewGenerationFixture,
        channel_name: &str,
    ) {
        fs::create_dir_all(review_channel_dir).expect("review channel dir");
        let payload = json!({
            "channel_version": "1",
            "channel_name": channel_name,
            "source_archive_dir": fs::canonicalize(review_archive_dir).expect("canonical review archive").display().to_string(),
            "selected_generation_id": generation.generation_id,
            "generation_identity": {
                "decision_packet_generated_at": generation.identity.generated_at.to_rfc3339(),
                "prod_config_fingerprint_sha256": generation.identity.prod_fp,
                "non_prod_config_fingerprint_sha256": generation.identity.non_prod_fp
            },
            "decision_packet_paths": [generation.packet_path],
            "runbook_json_paths": [generation.runbook_json_path],
            "runbook_markdown_paths": [generation.runbook_markdown_path],
            "latest_packet_verdict": "decision_packet_discussion_ready_but_not_authorized",
            "latest_runbook_verdict": "runbook_discussion_ready_but_not_authorized",
            "manifest_path": null,
            "bundle_path": null,
            "promoted_at": "2026-03-26T12:06:00Z",
            "build_version": "0.1.0",
            "git_commit": "deadbeef"
        });
        fs::write(
            review_channel_dir.join(format!("{channel_name}.json")),
            serde_json::to_string_pretty(&payload).expect("channel json"),
        )
        .expect("write review channel");
    }

    fn write_release_artifact(
        path: &Path,
        released_at: Option<&str>,
        generation_id: Option<&str>,
        generation_directory: Option<&str>,
        packet_path: Option<&str>,
        runbook_json_path: Option<&str>,
        runbook_markdown_path: Option<&str>,
        verdict: &str,
    ) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("release parent");
        }
        let payload = json!({
            "mode": "artifact_release",
            "released_at": released_at,
            "verdict": verdict,
            "reason": format!("release {verdict}"),
            "config_path": "/etc/solana-copy-bot/live.server.toml",
            "non_prod_config_path": "/etc/solana-copy-bot/devnet.server.toml",
            "archive_dir": "/var/www/solana-copy-bot/state/activation_artifacts/archive",
            "generation_id": generation_id,
            "generation_directory": generation_directory,
            "packet_path": packet_path,
            "runbook_json_path": runbook_json_path,
            "runbook_markdown_path": runbook_markdown_path,
            "manifest_path": null,
            "bundle_path": null,
            "decision_packet_verdict": "decision_packet_discussion_ready_but_not_authorized",
            "checklist_verdict": "activation_checklist_blocked_by_prod_stage3",
            "publish_verdict": "artifact_publish_succeeded",
            "publish_reason": "publish ok",
            "generation_published": true,
            "manifest_generated": false,
            "bundle_exported": false,
            "channel_promotion_attempted": false,
            "channel_promotion_happened": false,
            "channel_verify_attempted": false,
            "channel_name": null,
            "channel_dir": null,
            "channel_metadata_path": null,
            "channel_promote_verdict": null,
            "channel_promote_reason": null,
            "channel_verify_verdict": null,
            "channel_verify_reason": null,
            "execution_untouched": true,
            "activation_authorized": false,
            "not_authorized_summary": "artifact-only"
        });
        fs::write(
            path,
            serde_json::to_string_pretty(&payload).expect("release json"),
        )
        .expect("write release");
    }

    fn write_latest_pointer(
        latest_pointer_dir: &Path,
        release_archive_dir: &Path,
        selected_release_artifact_path: &Path,
        released_at: Option<&str>,
        released_at_source: &str,
        selected_generation_id: Option<&str>,
        release_verdict: &str,
    ) {
        fs::create_dir_all(latest_pointer_dir).expect("pointer dir");
        let canonical_archive = fs::canonicalize(release_archive_dir).unwrap_or_else(|_| {
            fs::create_dir_all(release_archive_dir).expect("create release archive dir");
            fs::canonicalize(release_archive_dir).expect("canonical release archive dir")
        });
        let canonical_selected_release_artifact_path = if selected_release_artifact_path.exists() {
            fs::canonicalize(selected_release_artifact_path).expect("canonical selected release")
        } else {
            canonical_archive.join(
                selected_release_artifact_path
                    .file_name()
                    .unwrap_or_else(|| std::ffi::OsStr::new("missing_release.json")),
            )
        };
        let payload = json!({
            "pointer_version": "1",
            "pointer_name": activation_artifact_release_publish_report::DEFAULT_LATEST_POINTER_NAME,
            "source_release_archive_dir": canonical_archive.display().to_string(),
            "selected_release_artifact_path": canonical_selected_release_artifact_path.display().to_string(),
            "selected_release_artifact_file_name": canonical_selected_release_artifact_path.file_name().map(|value| value.to_string_lossy().into_owned()).unwrap_or_else(|| "release.json".to_string()),
            "release_artifact_mode": "artifact_release",
            "released_at": released_at,
            "released_at_source": released_at_source,
            "compat_loaded_without_released_at": released_at_source != "released_at",
            "deterministic_timestamp_available": released_at.is_some(),
            "ordered_history_confident": released_at.is_some(),
            "release_verdict": release_verdict,
            "release_reason": format!("release {release_verdict}"),
            "selected_generation_id": selected_generation_id,
            "channel_promotion_happened": false,
            "channel_metadata_path": null,
            "pointed_at": "2026-03-26T12:05:00Z",
            "build_version": "0.1.0",
            "git_commit": "deadbeef"
        });
        fs::write(
            latest_pointer_dir.join(format!(
                "{}.json",
                activation_artifact_release_publish_report::DEFAULT_LATEST_POINTER_NAME
            )),
            serde_json::to_string_pretty(&payload).expect("pointer json"),
        )
        .expect("write latest pointer");
    }

    fn build_manifest_document(identities: &[GenerationIdentity]) -> serde_json::Value {
        let generations = identities
            .iter()
            .map(|identity| {
                let prefix = generation_dir_name(identity);
                json!({
                    "identity": {
                        "decision_packet_generated_at": identity.generated_at.to_rfc3339(),
                        "prod_config_fingerprint_sha256": identity.prod_fp,
                        "non_prod_config_fingerprint_sha256": identity.non_prod_fp
                    },
                    "decision_packet_paths": [format!("{prefix}/decision_packet.json")],
                    "runbook_json_paths": [format!("{prefix}/activation_runbook.json")],
                    "runbook_markdown_paths": [format!("{prefix}/activation_runbook.md")],
                    "latest_packet_verdict": "decision_packet_discussion_ready_but_not_authorized",
                    "latest_runbook_verdict": "runbook_discussion_ready_but_not_authorized"
                })
            })
            .collect::<Vec<_>>();
        let files = identities
            .iter()
            .flat_map(|identity| {
                let prefix = generation_dir_name(identity);
                vec![
                    json!({
                        "relative_path": format!("{prefix}/decision_packet.json"),
                        "artifact_kind": "decision_packet_json",
                        "sha256": "dummy",
                        "generation_identity": {
                            "decision_packet_generated_at": identity.generated_at.to_rfc3339(),
                            "prod_config_fingerprint_sha256": identity.prod_fp,
                            "non_prod_config_fingerprint_sha256": identity.non_prod_fp
                        }
                    }),
                    json!({
                        "relative_path": format!("{prefix}/activation_runbook.json"),
                        "artifact_kind": "runbook_json",
                        "sha256": "dummy",
                        "generation_identity": {
                            "decision_packet_generated_at": identity.generated_at.to_rfc3339(),
                            "prod_config_fingerprint_sha256": identity.prod_fp,
                            "non_prod_config_fingerprint_sha256": identity.non_prod_fp
                        }
                    }),
                    json!({
                        "relative_path": format!("{prefix}/activation_runbook.md"),
                        "artifact_kind": "runbook_markdown",
                        "sha256": "dummy",
                        "generation_identity": {
                            "decision_packet_generated_at": identity.generated_at.to_rfc3339(),
                            "prod_config_fingerprint_sha256": identity.prod_fp,
                            "non_prod_config_fingerprint_sha256": identity.non_prod_fp
                        }
                    }),
                ]
            })
            .collect::<Vec<_>>();
        json!({
            "generated_at": "2026-03-26T15:00:00Z",
            "manifest_version": "1",
            "build_version": "0.1.0",
            "git_commit": "deadbeef",
            "archive_dir": "/tmp/archive",
            "hash_algorithm": "sha256",
            "generation_count": generations.len(),
            "file_count": files.len(),
            "orphan_markdown_paths": [],
            "generations": generations,
            "files": files
        })
    }

    fn write_bundle_manifest(bundle_dir: &Path, identity: &GenerationIdentity) {
        fs::create_dir_all(bundle_dir).expect("bundle dir");
        fs::write(
            bundle_dir.join("bundle_manifest.json"),
            serde_json::to_string_pretty(&json!({
                "generated_at": "2026-03-26T15:10:00Z",
                "bundle_version": "1",
                "build_version": "0.1.0",
                "git_commit": "deadbeef",
                "source_archive_dir": "/tmp/archive",
                "generation_id": format!(
                    "{}|{}|{}",
                    identity.generated_at.to_rfc3339(),
                    identity.prod_fp,
                    identity.non_prod_fp
                ),
                "generation_identity": {
                    "decision_packet_generated_at": identity.generated_at.to_rfc3339(),
                    "prod_config_fingerprint_sha256": identity.prod_fp,
                    "non_prod_config_fingerprint_sha256": identity.non_prod_fp
                },
                "latest_packet_verdict": "decision_packet_discussion_ready_but_not_authorized",
                "latest_runbook_verdict": "runbook_discussion_ready_but_not_authorized",
                "hash_algorithm": "sha256",
                "file_count": 3,
                "files": [
                    {
                        "artifact_kind": "decision_packet_json",
                        "source_relative_path": "decision_packet.json",
                        "bundle_relative_path": "artifacts/decision_packet.json",
                        "sha256": "dummy"
                    },
                    {
                        "artifact_kind": "runbook_json",
                        "source_relative_path": "activation_runbook.json",
                        "bundle_relative_path": "artifacts/activation_runbook.json",
                        "sha256": "dummy"
                    },
                    {
                        "artifact_kind": "runbook_markdown",
                        "source_relative_path": "activation_runbook.md",
                        "bundle_relative_path": "artifacts/activation_runbook.md",
                        "sha256": "dummy"
                    }
                ]
            }))
            .expect("bundle json"),
        )
        .expect("write bundle manifest");
    }

    fn generation_dir_name(identity: &GenerationIdentity) -> String {
        format!(
            "{}__{}__{}",
            identity.generated_at.to_rfc3339().replace(':', "-"),
            identity.prod_fp,
            identity.non_prod_fp
        )
    }

    fn sample_packet_json(
        generated_at: &str,
        prod_fingerprint: &str,
        non_prod_fingerprint: &str,
    ) -> serde_json::Value {
        json!({
            "generated_at": generated_at,
            "packet_version": "1",
            "build_version": "0.1.0",
            "git_commit": "deadbeef",
            "prod_config_path": "/etc/solana-copy-bot/live.server.toml",
            "non_prod_config_path": "/etc/solana-copy-bot/devnet.server.toml",
            "operator_note": null,
            "execution_enabled": false,
            "read_only_packet": true,
            "activation_authorized": false,
            "discussion_ready_only": true,
            "prod_stage3_remains_hard_gate": true,
            "non_prod_evidence_is_secondary": true,
            "verdict": "decision_packet_discussion_ready_but_not_authorized",
            "reason": "sample packet",
            "blockers": [],
            "warnings": ["planning-only"],
            "checklist_verdict": "activation_checklist_blocked_by_prod_stage3",
            "checklist_reason": "sample",
            "prod_config_fingerprint": {
                "scope": "prod_execution_policy_guardrails",
                "sha256": prod_fingerprint,
                "secrets_excluded": true,
                "sensitive_urls_redacted_before_hashing": true
            },
            "non_prod_config_fingerprint": {
                "scope": "non_prod_execution_policy_guardrails",
                "sha256": non_prod_fingerprint,
                "secrets_excluded": true,
                "sensitive_urls_redacted_before_hashing": true
            },
            "prod_pre_activation_gate": {
                "verdict": "pre_activation_gates_green",
                "reason": "green",
                "planning_green": true,
                "blocked_by_stage3": false,
                "stage3_verdict": "fresh_current",
                "stage3_reason": "fresh",
                "stage3_captures_within_recent_horizon": 3,
                "stage3_latest_capture_age_seconds": 300,
                "stage4_readiness_verdict": "ready_for_execution_dry_run",
                "stage4_rehearsal_history_verdict": "sufficient_recent_rehearsal_evidence",
                "tiny_live_policy_verdict": "tiny_live_policy_bounded",
                "blockers": []
            },
            "launch_dossier": {
                "verdict": "activation_plan_ready_when_stage_gate_allows",
                "reason": "launch ready",
                "ready_when_stage_gate_allows": true,
                "activation_overlay_complete": true,
                "rollback_plan_complete": true,
                "service_restart_contract_complete": true,
                "activation_overlay_change_count": 3,
                "rollback_overlay_change_count": 3,
                "drift_finding_count": 0,
                "blocker_count": 0,
                "first_blocker": null
            },
            "tiny_live_guardrails": {
                "verdict": "tiny_live_guardrails_bounded",
                "reason": "bounded",
                "bounded": true,
                "enabled": true,
                "blocker_count": 0,
                "first_blocker": null,
                "rollback_trigger_count": 1,
                "rollback_triggers": [{
                    "trigger": "execution_failures",
                    "threshold_kind": "count",
                    "threshold_rate_pct": null,
                    "threshold_seconds": null,
                    "threshold_sol": null,
                    "threshold_count": 3,
                    "evaluation_window_seconds": 300,
                    "action": "rollback"
                }]
            },
            "non_prod_readiness": {
                "verdict": "devnet_readiness_green",
                "reason": "green",
                "green": true,
                "prod_profile_refused": false,
                "config_env": "devnet",
                "blockers": [],
                "warnings": [],
                "dress_latest_record_age_seconds": 300,
                "dress_recent_green_count": 2,
                "activation_latest_record_age_seconds": 300,
                "activation_recent_green_count": 2,
                "activation_recent_rollback_success_count": 2,
                "activation_recent_internal_consistency_count": 2,
                "stale_evidence_excluded": true
            }
        })
    }

    fn sample_runbook_json(
        generated_at: &str,
        decision_packet_generated_at: &str,
        prod_fingerprint: &str,
        non_prod_fingerprint: &str,
    ) -> serde_json::Value {
        json!({
            "generated_at": generated_at,
            "runbook_version": "1",
            "prod_config_path": "/etc/solana-copy-bot/live.server.toml",
            "non_prod_config_path": "/etc/solana-copy-bot/devnet.server.toml",
            "json_output_path": null,
            "markdown_output_path": null,
            "execution_enabled": false,
            "read_only_runbook": true,
            "activation_authorized": false,
            "discussion_ready_only": true,
            "prod_stage3_remains_hard_gate": true,
            "non_prod_evidence_is_secondary": true,
            "verdict": "runbook_discussion_ready_but_not_authorized",
            "reason": "sample runbook",
            "blockers": [],
            "warnings": [],
            "not_authorized_disclaimer": "planning only",
            "decision_packet_version": "1",
            "decision_packet_generated_at": decision_packet_generated_at,
            "decision_packet_verdict": "decision_packet_discussion_ready_but_not_authorized",
            "decision_packet_reason": "sample packet",
            "build_version": "0.1.0",
            "git_commit": "deadbeef",
            "prod_config_fingerprint": {
                "scope": "prod_execution_policy_guardrails",
                "sha256": prod_fingerprint,
                "secrets_excluded": true,
                "sensitive_urls_redacted_before_hashing": true
            },
            "non_prod_config_fingerprint": {
                "scope": "non_prod_execution_policy_guardrails",
                "sha256": non_prod_fingerprint,
                "secrets_excluded": true,
                "sensitive_urls_redacted_before_hashing": true
            },
            "prod_pre_activation_gate": {
                "verdict": "pre_activation_gates_green",
                "reason": "green",
                "planning_green": true,
                "blocked_by_stage3": false,
                "stage3_verdict": "fresh_current",
                "stage3_reason": "fresh",
                "stage3_captures_within_recent_horizon": 3,
                "stage3_latest_capture_age_seconds": 300,
                "stage4_readiness_verdict": "ready_for_execution_dry_run",
                "stage4_rehearsal_history_verdict": "sufficient_recent_rehearsal_evidence",
                "tiny_live_policy_verdict": "tiny_live_policy_bounded",
                "blockers": []
            },
            "launch_dossier": {
                "verdict": "activation_plan_ready_when_stage_gate_allows",
                "reason": "launch ready",
                "ready_when_stage_gate_allows": true,
                "activation_overlay_complete": true,
                "rollback_plan_complete": true,
                "service_restart_contract_complete": true,
                "activation_overlay_change_count": 3,
                "rollback_overlay_change_count": 3,
                "drift_finding_count": 0,
                "blocker_count": 0,
                "first_blocker": null
            },
            "tiny_live_guardrails": {
                "verdict": "tiny_live_guardrails_bounded",
                "reason": "bounded",
                "bounded": true,
                "enabled": true,
                "blocker_count": 0,
                "first_blocker": null,
                "rollback_trigger_count": 1,
                "rollback_triggers": []
            },
            "non_prod_readiness": {
                "verdict": "devnet_readiness_green",
                "reason": "green",
                "green": true,
                "prod_profile_refused": false,
                "config_env": "devnet",
                "blockers": [],
                "warnings": [],
                "dress_latest_record_age_seconds": 300,
                "dress_recent_green_count": 2,
                "activation_latest_record_age_seconds": 300,
                "activation_recent_green_count": 2,
                "activation_recent_rollback_success_count": 2,
                "activation_recent_internal_consistency_count": 2,
                "stale_evidence_excluded": true
            },
            "section_order": [
                "current_state",
                "preflight_checks",
                "bounded_activation_candidate",
                "post_change_checks",
                "rollback_triggers",
                "rollback_procedure",
                "not_authorized_disclaimer"
            ]
        })
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
