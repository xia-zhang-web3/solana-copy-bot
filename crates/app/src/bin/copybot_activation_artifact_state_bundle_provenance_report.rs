#![recursion_limit = "256"]

use anyhow::{anyhow, bail, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[allow(dead_code)]
#[path = "copybot_activation_artifact_state_bundle.rs"]
mod activation_artifact_state_bundle;
#[allow(dead_code)]
#[path = "copybot_activation_artifact_state_publish_report.rs"]
mod activation_artifact_state_publish_report;

const USAGE: &str = "usage: copybot_activation_artifact_state_bundle_provenance_report --state-archive-dir <path> [--snapshot-latest-pointer-dir <path>] [--history-dir <path>] [--history-snapshot <path>]... [--bundle-dir <path>] [--bundle <path>]... [--pointer-name <name>] [--json]";

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
    history_dir: Option<PathBuf>,
    history_snapshot_paths: Vec<PathBuf>,
    bundle_dir: Option<PathBuf>,
    bundle_paths: Vec<PathBuf>,
    pointer_name: String,
    json: bool,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ArtifactStateBundleProvenanceVerdict {
    ArtifactStateBundleProvenanceComplete,
    ArtifactStateBundleProvenanceIncomplete,
    ArtifactStateBundleProvenanceInvalidArtifactsPresent,
    ArtifactStateBundleProvenanceInconsistentLineage,
    ArtifactStateBundleProvenanceAmbiguousLegacyState,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LatestStateSnapshotBundleRelation {
    NoPointerConfigured,
    NoPointerMetadata,
    InvalidMetadata,
    MissingTarget,
    MissingFromArchive,
    MissingFromHistory,
    MissingBundleCoverage,
    BehindLatestArchive,
    BehindLatestHistory,
    NonGreenStateSnapshot,
    AmbiguousStateSnapshot,
    CoveredByArchiveHistoryAndBundle,
}

#[derive(Debug, Clone, Serialize)]
struct StateSnapshotBundleLineageIssue {
    surface: String,
    path: String,
    error: String,
}

#[derive(Debug, Clone, Serialize)]
struct StateSnapshotBundleCoverageSummary {
    identity_key: String,
    snapshotted_at: DateTime<Utc>,
    state_verdict: String,
    selected_review_generation_id: Option<String>,
    selected_latest_release_generation_id: Option<String>,
    archive_paths: Vec<String>,
    history_paths: Vec<String>,
    bundle_paths: Vec<String>,
    latest_pointer_selected: bool,
    latest_by_archive_timestamp: bool,
    latest_by_history_timestamp: bool,
    missing_history_coverage: bool,
    missing_bundle_coverage: bool,
    ambiguous_state_snapshot: bool,
    coherent_for_review_operations: bool,
}

#[derive(Debug, Clone, Serialize)]
struct HistoryOnlyStateSnapshotSummary {
    identity_key: String,
    snapshotted_at: DateTime<Utc>,
    state_verdict: String,
    selected_review_generation_id: Option<String>,
    selected_latest_release_generation_id: Option<String>,
    history_paths: Vec<String>,
    ambiguous_state_snapshot: bool,
    coherent_for_review_operations: bool,
}

#[derive(Debug, Clone, Serialize)]
struct BundleOnlyStateSnapshotSummary {
    identity_key: String,
    snapshotted_at: DateTime<Utc>,
    state_verdict: String,
    state_reason: String,
    selected_review_generation_id: Option<String>,
    selected_latest_release_generation_id: Option<String>,
    bundle_paths: Vec<String>,
    ambiguous_state_snapshot: bool,
    coherent_for_review_operations: bool,
}

#[derive(Debug, Clone, Serialize)]
struct LatestStateSnapshotBundleSummary {
    pointer_name: String,
    pointer_verdict: String,
    pointer_reason: String,
    pointer_path: Option<String>,
    selected_snapshot_path: Option<String>,
    selected_snapshot_file_name: Option<String>,
    selected_snapshotted_at: Option<DateTime<Utc>>,
    selected_state_verdict: Option<String>,
    selected_state_reason: Option<String>,
    selected_review_generation_id: Option<String>,
    selected_latest_release_generation_id: Option<String>,
    selected_ambiguous_legacy_count: Option<usize>,
    selected_coherent_for_review_operations: Option<bool>,
    pointed_at: Option<DateTime<Utc>>,
    target_exists: bool,
    target_matches_identity: bool,
    present_in_archive: bool,
    present_in_history: bool,
    present_in_bundles: bool,
    bundle_coverage_count: usize,
    bundle_paths: Vec<String>,
    matches_latest_archive: bool,
    matches_latest_history: bool,
    relation: LatestStateSnapshotBundleRelation,
    missing_paths: Vec<String>,
    inconsistencies: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct StateSnapshotBundleProvenanceReport {
    mode: String,
    verdict: ArtifactStateBundleProvenanceVerdict,
    reason: String,
    state_archive_dir: String,
    snapshot_latest_pointer_dir: Option<String>,
    history_dir: Option<String>,
    history_snapshot_paths: Vec<String>,
    bundle_dir: Option<String>,
    bundle_paths_examined: Vec<String>,
    pointer_name: String,
    archive_snapshot_count: usize,
    history_snapshot_count: usize,
    bundle_count: usize,
    latest_pointer_present: bool,
    latest_pointer_selected_snapshot_path: Option<String>,
    latest_pointer_relation: LatestStateSnapshotBundleRelation,
    latest_pointer_selected_snapshot_has_bundle_coverage: Option<bool>,
    latest_pointer_selected_snapshot_bundle_paths: Vec<String>,
    latest_archive_snapshot_path: Option<String>,
    latest_history_snapshot_path: Option<String>,
    archive_snapshots_missing_from_history_count: usize,
    archive_snapshots_missing_from_bundle_count: usize,
    history_snapshots_missing_from_archive_count: usize,
    bundle_snapshots_missing_from_archive_count: usize,
    bundle_snapshots_missing_from_history_count: usize,
    invalid_artifact_count: usize,
    invalid_artifacts: Vec<StateSnapshotBundleLineageIssue>,
    ambiguous_snapshot_count: usize,
    non_green_bundled_snapshot_count: usize,
    archive_snapshots: Vec<StateSnapshotBundleCoverageSummary>,
    history_snapshots_missing_from_archive: Vec<HistoryOnlyStateSnapshotSummary>,
    bundle_snapshots_missing_from_archive: Vec<BundleOnlyStateSnapshotSummary>,
    bundle_snapshots_missing_from_history: Vec<BundleOnlyStateSnapshotSummary>,
    latest_pointer: LatestStateSnapshotBundleSummary,
    state_snapshot_bundle_lineage_only: bool,
    execution_untouched: bool,
    activation_authorized: bool,
    not_authorized_summary: String,
}

#[derive(Debug, Clone)]
struct StateSnapshotSurfaceRecord {
    loaded: activation_artifact_state_publish_report::LoadedStateSnapshot,
    identity_key: String,
    canonical_path: PathBuf,
}

#[derive(Debug, Clone)]
struct StateSnapshotBundleRecord {
    summary: activation_artifact_state_bundle::StateSnapshotBundleSummary,
    identity_key: String,
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
    let mut history_dir: Option<PathBuf> = None;
    let mut history_snapshot_paths = Vec::new();
    let mut bundle_dir: Option<PathBuf> = None;
    let mut bundle_paths = Vec::new();
    let mut pointer_name =
        activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME.to_string();
    let mut json = false;

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
            "--history-dir" => {
                history_dir = Some(PathBuf::from(parse_string_arg(
                    "--history-dir",
                    args.next(),
                )?))
            }
            "--history-snapshot" => history_snapshot_paths.push(PathBuf::from(parse_string_arg(
                "--history-snapshot",
                args.next(),
            )?)),
            "--bundle-dir" => {
                bundle_dir = Some(PathBuf::from(parse_string_arg(
                    "--bundle-dir",
                    args.next(),
                )?))
            }
            "--bundle" => {
                bundle_paths.push(PathBuf::from(parse_string_arg("--bundle", args.next())?))
            }
            "--pointer-name" => {
                pointer_name = parse_pointer_name(parse_string_arg("--pointer-name", args.next())?)?
            }
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown arg `{other}`"),
        }
    }

    if history_dir.is_none() && history_snapshot_paths.is_empty() {
        bail!("either --history-dir or --history-snapshot is required");
    }
    if bundle_dir.is_none() && bundle_paths.is_empty() {
        bail!("either --bundle-dir or --bundle is required");
    }

    Ok(Some(Config {
        state_archive_dir: state_archive_dir
            .ok_or_else(|| anyhow!("--state-archive-dir is required"))?,
        snapshot_latest_pointer_dir,
        history_dir,
        history_snapshot_paths,
        bundle_dir,
        bundle_paths,
        pointer_name,
        json,
    }))
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    let raw = value.ok_or_else(|| anyhow!("{flag} requires a value"))?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(trimmed.to_string())
}

fn parse_pointer_name(value: String) -> Result<String> {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
    {
        Ok(value)
    } else {
        bail!("pointer name may only contain ascii alphanumeric characters, '-' or '_'");
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

fn build_report(config: &Config) -> Result<StateSnapshotBundleProvenanceReport> {
    let archive_paths = collect_snapshot_paths(Some(&config.state_archive_dir), &[])?;
    let history_paths = collect_snapshot_paths(
        config.history_dir.as_deref(),
        &config.history_snapshot_paths,
    )?;
    let bundle_paths = activation_artifact_state_bundle::collect_state_snapshot_bundle_paths(
        config.bundle_dir.as_deref(),
        &config.bundle_paths,
    )?;

    let (archive_records, mut invalid_artifacts) =
        load_state_surface("state_archive", &archive_paths);
    let (history_records, invalid_history) = load_state_surface("history", &history_paths);
    invalid_artifacts.extend(invalid_history);
    let (bundle_records, invalid_bundles) = load_bundle_surface(&bundle_paths);
    invalid_artifacts.extend(invalid_bundles);
    let canonical_state_archive_dir = fs::canonicalize(&config.state_archive_dir).ok();

    let pointer_report = if let Some(pointer_dir) = config.snapshot_latest_pointer_dir.as_ref() {
        match activation_artifact_state_publish_report::inspect_latest_pointer_report(
            &config.state_archive_dir,
            pointer_dir,
            &config.pointer_name,
            true,
        ) {
            Ok(report) => report,
            Err(error) => {
                invalid_artifacts.push(StateSnapshotBundleLineageIssue {
                    surface: "state_latest_pointer".to_string(),
                    path: pointer_dir
                        .join(format!("{}.json", config.pointer_name))
                        .display()
                        .to_string(),
                    error: format!("{error:#}"),
                });
                activation_artifact_state_publish_report::ArtifactStateSnapshotPublishReport {
                    mode: "verify_latest".to_string(),
                    verdict: activation_artifact_state_publish_report::ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotInvalidMetadata,
                    reason: "snapshot latest pointer inspection failed operationally".to_string(),
                    state_archive_dir: config.state_archive_dir.display().to_string(),
                    persisted_state_snapshot_path: None,
                    persisted_state_snapshot_exists: false,
                    persisted_state_snapshot_file_name: None,
                    snapshotted_at: None,
                    state_verdict: None,
                    state_reason: None,
                    selected_review_generation_id: None,
                    selected_latest_release_generation_id: None,
                    selection_alignment_matches: None,
                    selection_alignment_summary: None,
                    latest_pointer_attempted: false,
                    latest_pointer_updated: false,
                    latest_pointer_dir: Some(pointer_dir.display().to_string()),
                    latest_pointer_name: config.pointer_name.clone(),
                    latest_pointer_path: Some(
                        pointer_dir
                            .join(format!("{}.json", config.pointer_name))
                            .display()
                            .to_string(),
                    ),
                    latest_pointer_source_state_archive_dir: None,
                    latest_pointer_pointed_at: None,
                    latest_pointer_exists: false,
                    latest_pointer_overwrite_used: false,
                    latest_pointer_target_exists: false,
                    latest_pointer_target_matches_identity: false,
                    verification_attempted: true,
                    missing_paths: Vec::new(),
                    inconsistencies: Vec::new(),
                    artifact_state_only: true,
                    execution_untouched: true,
                    activation_authorized: false,
                    not_authorized_summary:
                        "State-snapshot bundle provenance only audits persisted snapshot/bundle lineage. It does not authorize activation and does not override the Stage 3 production gate.".to_string(),
                }
            }
        }
    } else {
        activation_artifact_state_publish_report::ArtifactStateSnapshotPublishReport {
            mode: "verify_latest".to_string(),
            verdict: activation_artifact_state_publish_report::ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotVerifyMissingTarget,
            reason: "no state snapshot latest pointer directory was provided".to_string(),
            state_archive_dir: config.state_archive_dir.display().to_string(),
            persisted_state_snapshot_path: None,
            persisted_state_snapshot_exists: false,
            persisted_state_snapshot_file_name: None,
            snapshotted_at: None,
            state_verdict: None,
            state_reason: None,
            selected_review_generation_id: None,
            selected_latest_release_generation_id: None,
            selection_alignment_matches: None,
            selection_alignment_summary: None,
            latest_pointer_attempted: false,
            latest_pointer_updated: false,
            latest_pointer_dir: None,
            latest_pointer_name: config.pointer_name.clone(),
            latest_pointer_path: None,
            latest_pointer_source_state_archive_dir: None,
            latest_pointer_pointed_at: None,
            latest_pointer_exists: false,
            latest_pointer_overwrite_used: false,
            latest_pointer_target_exists: false,
            latest_pointer_target_matches_identity: false,
            verification_attempted: false,
            missing_paths: Vec::new(),
            inconsistencies: Vec::new(),
            artifact_state_only: true,
            execution_untouched: true,
            activation_authorized: false,
            not_authorized_summary:
                "State-snapshot bundle provenance only audits persisted snapshot/bundle lineage. It does not authorize activation and does not override the Stage 3 production gate.".to_string(),
        }
    };

    let loaded_pointer_metadata =
        if let Some(pointer_dir) = config.snapshot_latest_pointer_dir.as_ref() {
            match activation_artifact_state_publish_report::inspect_latest_pointer_metadata(
                pointer_dir,
                &config.pointer_name,
            ) {
                Ok(metadata) => metadata,
                Err(error) => {
                    invalid_artifacts.push(StateSnapshotBundleLineageIssue {
                        surface: "state_latest_pointer".to_string(),
                        path: pointer_dir
                            .join(format!("{}.json", config.pointer_name))
                            .display()
                            .to_string(),
                        error: format!("{error:#}"),
                    });
                    None
                }
            }
        } else {
            None
        };

    let archive_map = archive_records.iter().fold(
        BTreeMap::<String, Vec<&StateSnapshotSurfaceRecord>>::new(),
        |mut acc, record| {
            acc.entry(record.identity_key.clone())
                .or_default()
                .push(record);
            acc
        },
    );
    let history_map = history_records.iter().fold(
        BTreeMap::<String, Vec<&StateSnapshotSurfaceRecord>>::new(),
        |mut acc, record| {
            acc.entry(record.identity_key.clone())
                .or_default()
                .push(record);
            acc
        },
    );
    let current_archive_bundle_records = bundle_records
        .iter()
        .filter(|record| {
            bundle_matches_current_archive(
                record,
                canonical_state_archive_dir.as_deref(),
                &archive_records,
            )
        })
        .collect::<Vec<_>>();
    let bundle_map = current_archive_bundle_records.iter().fold(
        BTreeMap::<String, Vec<&StateSnapshotBundleRecord>>::new(),
        |mut acc, record| {
            acc.entry(record.identity_key.clone())
                .or_default()
                .push(*record);
            acc
        },
    );

    let latest_archive = latest_record(&archive_records);
    let latest_history = latest_record(&history_records);
    let selected_pointer_canonical_path = loaded_pointer_metadata.as_ref().and_then(|loaded| {
        fs::canonicalize(Path::new(&loaded.metadata.selected_snapshot_path)).ok()
    });
    let selected_pointer_identity_key = loaded_pointer_metadata
        .as_ref()
        .map(|loaded| identity_key_from_pointer_metadata(&loaded.metadata));
    let present_in_archive = selected_pointer_canonical_path
        .as_ref()
        .is_some_and(|selected| {
            archive_records
                .iter()
                .any(|record| &record.canonical_path == selected)
        });
    let present_in_history = selected_pointer_identity_key
        .as_ref()
        .is_some_and(|identity| history_map.contains_key(identity));
    let bundle_coverage_for_pointer = bundle_paths_for_selected_snapshot(
        &current_archive_bundle_records,
        selected_pointer_identity_key.as_deref(),
        selected_pointer_canonical_path.as_deref(),
    );
    let present_in_bundles = !bundle_coverage_for_pointer.is_empty();
    let matches_latest_archive = selected_pointer_canonical_path
        .as_ref()
        .zip(latest_archive)
        .is_some_and(|(selected, latest)| selected == &latest.canonical_path);
    let matches_latest_history = selected_pointer_identity_key
        .as_ref()
        .zip(latest_history)
        .is_some_and(|(selected, latest)| selected == &latest.identity_key);

    let archive_snapshots = archive_map
        .iter()
        .map(|(identity_key, records)| {
            let sample = records.first().expect("archive sample");
            let history_matches = history_map.get(identity_key);
            let bundle_matches = bundle_map.get(identity_key);
            StateSnapshotBundleCoverageSummary {
                identity_key: identity_key.clone(),
                snapshotted_at: sample.loaded.artifact.snapshotted_at,
                state_verdict: sample.loaded.artifact.state_verdict.clone(),
                selected_review_generation_id: sample
                    .loaded
                    .artifact
                    .selected_review_generation_id
                    .clone(),
                selected_latest_release_generation_id: sample
                    .loaded
                    .artifact
                    .selected_latest_release_generation_id
                    .clone(),
                archive_paths: records
                    .iter()
                    .map(|record| record.loaded.path.display().to_string())
                    .collect(),
                history_paths: history_matches
                    .into_iter()
                    .flat_map(|entries| entries.iter())
                    .map(|record| record.loaded.path.display().to_string())
                    .collect(),
                bundle_paths: bundle_matches
                    .into_iter()
                    .flat_map(|entries| entries.iter())
                    .map(|record| record.summary.bundle_path.clone())
                    .collect(),
                latest_pointer_selected: selected_pointer_identity_key
                    .as_ref()
                    .is_some_and(|selected| selected == identity_key),
                latest_by_archive_timestamp: latest_archive
                    .is_some_and(|latest| latest.identity_key == *identity_key),
                latest_by_history_timestamp: latest_history
                    .is_some_and(|latest| latest.identity_key == *identity_key),
                missing_history_coverage: history_matches.is_none(),
                missing_bundle_coverage: bundle_matches.is_none(),
                ambiguous_state_snapshot: sample.loaded.artifact.state_verdict
                    == "artifact_state_ambiguous_legacy_state",
                coherent_for_review_operations: sample
                    .loaded
                    .artifact
                    .coherent_for_review_operations,
            }
        })
        .collect::<Vec<_>>();

    let history_snapshots_missing_from_archive = history_map
        .iter()
        .filter(|(identity_key, _)| !archive_map.contains_key(*identity_key))
        .map(|(identity_key, records)| {
            let sample = records.first().expect("history sample");
            HistoryOnlyStateSnapshotSummary {
                identity_key: identity_key.clone(),
                snapshotted_at: sample.loaded.artifact.snapshotted_at,
                state_verdict: sample.loaded.artifact.state_verdict.clone(),
                selected_review_generation_id: sample
                    .loaded
                    .artifact
                    .selected_review_generation_id
                    .clone(),
                selected_latest_release_generation_id: sample
                    .loaded
                    .artifact
                    .selected_latest_release_generation_id
                    .clone(),
                history_paths: records
                    .iter()
                    .map(|record| record.loaded.path.display().to_string())
                    .collect(),
                ambiguous_state_snapshot: sample.loaded.artifact.state_verdict
                    == "artifact_state_ambiguous_legacy_state",
                coherent_for_review_operations: sample
                    .loaded
                    .artifact
                    .coherent_for_review_operations,
            }
        })
        .collect::<Vec<_>>();

    let bundle_snapshots_missing_from_archive = build_bundles_not_matching_current_archive(
        &bundle_records,
        canonical_state_archive_dir.as_deref(),
        &archive_records,
    );
    let bundle_snapshots_missing_from_history =
        build_bundle_only_summaries(&bundle_map, &history_map, false);

    let pointer_relation = determine_pointer_relation(
        config.snapshot_latest_pointer_dir.is_some(),
        &pointer_report,
        loaded_pointer_metadata.as_ref(),
        present_in_archive,
        present_in_history,
        present_in_bundles,
        matches_latest_archive,
        matches_latest_history,
    );
    let latest_pointer_summary = LatestStateSnapshotBundleSummary {
        pointer_name: config.pointer_name.clone(),
        pointer_verdict: serialize_enum(&pointer_report.verdict),
        pointer_reason: pointer_report.reason.clone(),
        pointer_path: loaded_pointer_metadata
            .as_ref()
            .map(|loaded| loaded.path.display().to_string())
            .or(pointer_report.latest_pointer_path.clone()),
        selected_snapshot_path: loaded_pointer_metadata
            .as_ref()
            .map(|loaded| loaded.metadata.selected_snapshot_path.clone())
            .or(pointer_report.persisted_state_snapshot_path.clone()),
        selected_snapshot_file_name: loaded_pointer_metadata
            .as_ref()
            .map(|loaded| loaded.metadata.selected_snapshot_file_name.clone())
            .or(pointer_report.persisted_state_snapshot_file_name.clone()),
        selected_snapshotted_at: loaded_pointer_metadata
            .as_ref()
            .map(|loaded| loaded.metadata.snapshotted_at)
            .or(pointer_report.snapshotted_at),
        selected_state_verdict: loaded_pointer_metadata
            .as_ref()
            .map(|loaded| loaded.metadata.snapshot_verdict.clone())
            .or(pointer_report.state_verdict.clone()),
        selected_state_reason: loaded_pointer_metadata
            .as_ref()
            .map(|loaded| loaded.metadata.snapshot_reason.clone())
            .or(pointer_report.state_reason.clone()),
        selected_review_generation_id: loaded_pointer_metadata
            .as_ref()
            .and_then(|loaded| loaded.metadata.selected_review_generation_id.clone())
            .or(pointer_report.selected_review_generation_id.clone()),
        selected_latest_release_generation_id: loaded_pointer_metadata
            .as_ref()
            .and_then(|loaded| {
                loaded
                    .metadata
                    .selected_latest_release_generation_id
                    .clone()
            })
            .or(pointer_report.selected_latest_release_generation_id.clone()),
        selected_ambiguous_legacy_count: loaded_pointer_metadata
            .as_ref()
            .map(|loaded| loaded.metadata.ambiguous_legacy_count),
        selected_coherent_for_review_operations: loaded_pointer_metadata
            .as_ref()
            .map(|loaded| loaded.metadata.coherent_for_review_operations),
        pointed_at: loaded_pointer_metadata
            .as_ref()
            .map(|loaded| loaded.metadata.pointed_at)
            .or(pointer_report.latest_pointer_pointed_at),
        target_exists: pointer_report.latest_pointer_target_exists,
        target_matches_identity: pointer_report.latest_pointer_target_matches_identity,
        present_in_archive,
        present_in_history,
        present_in_bundles,
        bundle_coverage_count: bundle_coverage_for_pointer.len(),
        bundle_paths: bundle_coverage_for_pointer.clone(),
        matches_latest_archive,
        matches_latest_history,
        relation: pointer_relation,
        missing_paths: pointer_report.missing_paths.clone(),
        inconsistencies: pointer_report.inconsistencies.clone(),
    };

    let archive_missing_from_history_count = archive_snapshots
        .iter()
        .filter(|summary| summary.missing_history_coverage)
        .count();
    let archive_missing_from_bundle_count = archive_snapshots
        .iter()
        .filter(|summary| summary.missing_bundle_coverage)
        .count();
    let history_missing_from_archive_count = history_snapshots_missing_from_archive.len();
    let bundle_missing_from_archive_count = bundle_snapshots_missing_from_archive.len();
    let bundle_missing_from_history_count = bundle_snapshots_missing_from_history.len();
    let ambiguous_snapshot_count = unique_ambiguous_snapshot_count(
        &archive_snapshots,
        &history_snapshots_missing_from_archive,
        &bundle_snapshots_missing_from_archive,
        &bundle_snapshots_missing_from_history,
    );
    let non_green_bundled_snapshot_count = unique_non_green_bundle_count(&bundle_map);
    let hard_non_green_pointer_selected = latest_pointer_summary
        .selected_state_verdict
        .as_deref()
        .is_some_and(is_hard_non_green_state_verdict);
    let pointer_selected_missing_bundle_coverage = config.snapshot_latest_pointer_dir.is_some()
        && latest_pointer_summary.selected_snapshot_path.is_some()
        && matches!(
            latest_pointer_summary.relation,
            LatestStateSnapshotBundleRelation::MissingBundleCoverage
        );

    let (verdict, reason) = if !invalid_artifacts.is_empty() {
        (
            ArtifactStateBundleProvenanceVerdict::ArtifactStateBundleProvenanceInvalidArtifactsPresent,
            format!(
                "state snapshot bundle provenance contains {} invalid artifact(s); review invalid_artifacts before trusting bundle lineage",
                invalid_artifacts.len()
            ),
        )
    } else if matches!(
        latest_pointer_summary.relation,
        LatestStateSnapshotBundleRelation::InvalidMetadata
            | LatestStateSnapshotBundleRelation::MissingTarget
            | LatestStateSnapshotBundleRelation::MissingFromArchive
    ) {
        (
            ArtifactStateBundleProvenanceVerdict::ArtifactStateBundleProvenanceInconsistentLineage,
            "state snapshot bundle provenance is inconsistent: the current latest-pointer target does not resolve cleanly against the persisted archive".to_string(),
        )
    } else if bundle_missing_from_archive_count > 0 {
        (
            ArtifactStateBundleProvenanceVerdict::ArtifactStateBundleProvenanceInconsistentLineage,
            "state snapshot bundle provenance is inconsistent: one or more bundles reference snapshots missing from the persisted archive".to_string(),
        )
    } else if archive_snapshots.is_empty() {
        (
            ArtifactStateBundleProvenanceVerdict::ArtifactStateBundleProvenanceIncomplete,
            "no persisted state snapshot artifacts were found in the state snapshot archive"
                .to_string(),
        )
    } else if pointer_selected_missing_bundle_coverage {
        (
            ArtifactStateBundleProvenanceVerdict::ArtifactStateBundleProvenanceIncomplete,
            "the state latest pointer selects a snapshot that has no exported bundle coverage"
                .to_string(),
        )
    } else if archive_missing_from_history_count > 0
        || archive_missing_from_bundle_count > 0
        || history_missing_from_archive_count > 0
        || bundle_missing_from_history_count > 0
        || matches!(
            latest_pointer_summary.relation,
            LatestStateSnapshotBundleRelation::MissingFromHistory
                | LatestStateSnapshotBundleRelation::BehindLatestArchive
                | LatestStateSnapshotBundleRelation::BehindLatestHistory
                | LatestStateSnapshotBundleRelation::NonGreenStateSnapshot
        )
        || hard_non_green_pointer_selected
        || non_green_bundled_snapshot_count > 0
    {
        (
            ArtifactStateBundleProvenanceVerdict::ArtifactStateBundleProvenanceIncomplete,
            "state snapshot bundle provenance is incomplete: bundle coverage, history coverage, or current pointer-selected snapshot state is non-green or stale".to_string(),
        )
    } else if ambiguous_snapshot_count > 0
        || latest_pointer_summary.relation
            == LatestStateSnapshotBundleRelation::AmbiguousStateSnapshot
    {
        (
            ArtifactStateBundleProvenanceVerdict::ArtifactStateBundleProvenanceAmbiguousLegacyState,
            "state snapshot bundle provenance is structurally present, but it depends on ambiguous legacy snapshot state and cannot be treated as clean green provenance".to_string(),
        )
    } else {
        (
            ArtifactStateBundleProvenanceVerdict::ArtifactStateBundleProvenanceComplete,
            "state snapshot archive, history inputs, latest pointer, and exported bundles are aligned".to_string(),
        )
    };

    Ok(StateSnapshotBundleProvenanceReport {
        mode: "artifact_state_snapshot_bundle_provenance_report".to_string(),
        verdict,
        reason,
        state_archive_dir: config.state_archive_dir.display().to_string(),
        snapshot_latest_pointer_dir: config
            .snapshot_latest_pointer_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        history_dir: config
            .history_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        history_snapshot_paths: config
            .history_snapshot_paths
            .iter()
            .map(|path| path.display().to_string())
            .collect(),
        bundle_dir: config.bundle_dir.as_ref().map(|path| path.display().to_string()),
        bundle_paths_examined: bundle_paths
            .iter()
            .map(|path| path.display().to_string())
            .collect(),
        pointer_name: config.pointer_name.clone(),
        archive_snapshot_count: archive_records.len(),
        history_snapshot_count: history_records.len(),
        bundle_count: bundle_records.len(),
        latest_pointer_present: latest_pointer_summary.pointer_path.is_some(),
        latest_pointer_selected_snapshot_path: latest_pointer_summary.selected_snapshot_path.clone(),
        latest_pointer_relation: latest_pointer_summary.relation,
        latest_pointer_selected_snapshot_has_bundle_coverage: config
            .snapshot_latest_pointer_dir
            .as_ref()
            .map(|_| latest_pointer_summary.present_in_bundles),
        latest_pointer_selected_snapshot_bundle_paths: latest_pointer_summary.bundle_paths.clone(),
        latest_archive_snapshot_path: latest_archive
            .map(|record| record.loaded.path.display().to_string()),
        latest_history_snapshot_path: latest_history
            .map(|record| record.loaded.path.display().to_string()),
        archive_snapshots_missing_from_history_count: archive_missing_from_history_count,
        archive_snapshots_missing_from_bundle_count: archive_missing_from_bundle_count,
        history_snapshots_missing_from_archive_count: history_missing_from_archive_count,
        bundle_snapshots_missing_from_archive_count: bundle_missing_from_archive_count,
        bundle_snapshots_missing_from_history_count: bundle_missing_from_history_count,
        invalid_artifact_count: invalid_artifacts.len(),
        invalid_artifacts,
        ambiguous_snapshot_count,
        non_green_bundled_snapshot_count,
        archive_snapshots,
        history_snapshots_missing_from_archive,
        bundle_snapshots_missing_from_archive,
        bundle_snapshots_missing_from_history,
        latest_pointer: latest_pointer_summary,
        state_snapshot_bundle_lineage_only: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary:
            "State snapshot bundle provenance only audits persisted snapshot, history, pointer, and bundle lineage. It does not authorize activation, does not enable execution, and does not override the Stage 3 production gate.".to_string(),
    })
}

fn collect_snapshot_paths(dir: Option<&Path>, explicit_paths: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut paths = BTreeSet::new();
    if let Some(dir) = dir {
        if dir.exists() {
            for entry in fs::read_dir(dir)? {
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

fn load_state_surface(
    surface: &str,
    paths: &[PathBuf],
) -> (
    Vec<StateSnapshotSurfaceRecord>,
    Vec<StateSnapshotBundleLineageIssue>,
) {
    let mut valid = Vec::new();
    let mut invalid = Vec::new();
    for path in paths {
        match activation_artifact_state_publish_report::inspect_state_snapshot_artifact(path) {
            Ok(loaded) => valid.push(StateSnapshotSurfaceRecord {
                identity_key: identity_key_from_snapshot(&loaded.artifact),
                canonical_path: loaded.canonical_path.clone(),
                loaded,
            }),
            Err(error) => invalid.push(StateSnapshotBundleLineageIssue {
                surface: surface.to_string(),
                path: path.display().to_string(),
                error: format!("{error:#}"),
            }),
        }
    }
    (valid, invalid)
}

fn load_bundle_surface(
    paths: &[PathBuf],
) -> (
    Vec<StateSnapshotBundleRecord>,
    Vec<StateSnapshotBundleLineageIssue>,
) {
    let mut valid = Vec::new();
    let mut invalid = Vec::new();
    for path in paths {
        match activation_artifact_state_bundle::inspect_state_snapshot_bundle(path) {
            Ok(summary) if summary.verdict == "artifact_state_bundle_verified" => {
                let Some(_) = summary.source_state_archive_dir.as_deref() else {
                    invalid.push(StateSnapshotBundleLineageIssue {
                        surface: "state_snapshot_bundle".to_string(),
                        path: summary.bundle_path,
                        error: "verified bundle summary is missing source_state_archive_dir"
                            .to_string(),
                    });
                    continue;
                };
                let Some(_) = summary.selected_snapshot_path.as_deref() else {
                    invalid.push(StateSnapshotBundleLineageIssue {
                        surface: "state_snapshot_bundle".to_string(),
                        path: summary.bundle_path,
                        error: "verified bundle summary is missing selected_snapshot_source_path"
                            .to_string(),
                    });
                    continue;
                };
                let Some(snapshotted_at) = summary.snapshotted_at else {
                    invalid.push(StateSnapshotBundleLineageIssue {
                        surface: "state_snapshot_bundle".to_string(),
                        path: summary.bundle_path,
                        error: "verified bundle summary is missing snapshotted_at".to_string(),
                    });
                    continue;
                };
                let Some(state_verdict) = summary.selected_snapshot_state_verdict.as_deref() else {
                    invalid.push(StateSnapshotBundleLineageIssue {
                        surface: "state_snapshot_bundle".to_string(),
                        path: summary.bundle_path,
                        error: "verified bundle summary is missing selected_snapshot_state_verdict"
                            .to_string(),
                    });
                    continue;
                };
                let identity_key = identity_key_from_values(
                    snapshotted_at,
                    state_verdict,
                    summary.selected_review_generation_id.as_deref(),
                    summary.selected_latest_release_generation_id.as_deref(),
                );
                valid.push(StateSnapshotBundleRecord {
                    summary,
                    identity_key,
                });
            }
            Ok(summary) => invalid.push(StateSnapshotBundleLineageIssue {
                surface: "state_snapshot_bundle".to_string(),
                path: summary.bundle_path,
                error: format!(
                    "bundle verification returned `{}`: {}",
                    summary.verdict, summary.reason
                ),
            }),
            Err(error) => invalid.push(StateSnapshotBundleLineageIssue {
                surface: "state_snapshot_bundle".to_string(),
                path: path.display().to_string(),
                error: format!("{error:#}"),
            }),
        }
    }
    (valid, invalid)
}

fn bundle_matches_current_archive(
    bundle: &StateSnapshotBundleRecord,
    canonical_state_archive_dir: Option<&Path>,
    archive_records: &[StateSnapshotSurfaceRecord],
) -> bool {
    let Some(canonical_state_archive_dir) = canonical_state_archive_dir else {
        return false;
    };
    let Some(source_state_archive_dir) = bundle.summary.source_state_archive_dir.as_deref() else {
        return false;
    };
    if Path::new(source_state_archive_dir) != canonical_state_archive_dir {
        return false;
    }
    let Some(selected_snapshot_path) = bundle.summary.selected_snapshot_path.as_deref() else {
        return false;
    };
    let selected_snapshot_path = Path::new(selected_snapshot_path);
    archive_records.iter().any(|record| {
        record.identity_key == bundle.identity_key
            && record.canonical_path == selected_snapshot_path
    })
}

fn bundle_paths_for_selected_snapshot(
    bundle_records: &[&StateSnapshotBundleRecord],
    selected_identity_key: Option<&str>,
    selected_canonical_path: Option<&Path>,
) -> Vec<String> {
    match (selected_identity_key, selected_canonical_path) {
        (Some(selected_identity_key), Some(selected_canonical_path)) => bundle_records
            .iter()
            .filter(|record| {
                record.identity_key == selected_identity_key
                    && record
                        .summary
                        .selected_snapshot_path
                        .as_deref()
                        .is_some_and(|path| Path::new(path) == selected_canonical_path)
            })
            .map(|record| record.summary.bundle_path.clone())
            .collect(),
        _ => Vec::new(),
    }
}

fn latest_record(records: &[StateSnapshotSurfaceRecord]) -> Option<&StateSnapshotSurfaceRecord> {
    records.iter().max_by(|left, right| {
        left.loaded
            .artifact
            .snapshotted_at
            .cmp(&right.loaded.artifact.snapshotted_at)
            .then_with(|| left.canonical_path.cmp(&right.canonical_path))
    })
}

fn build_bundle_only_summaries(
    bundle_map: &BTreeMap<String, Vec<&StateSnapshotBundleRecord>>,
    comparison_map: &BTreeMap<String, Vec<&StateSnapshotSurfaceRecord>>,
    missing_from_archive: bool,
) -> Vec<BundleOnlyStateSnapshotSummary> {
    bundle_map
        .iter()
        .filter(|(identity_key, _)| !comparison_map.contains_key(*identity_key))
        .map(|(identity_key, records)| {
            let sample = records.first().expect("bundle sample");
            BundleOnlyStateSnapshotSummary {
                identity_key: identity_key.clone(),
                snapshotted_at: sample
                    .summary
                    .snapshotted_at
                    .expect("bundle snapshotted_at"),
                state_verdict: sample
                    .summary
                    .selected_snapshot_state_verdict
                    .clone()
                    .unwrap_or_else(|| "<unknown>".to_string()),
                state_reason: sample
                    .summary
                    .selected_snapshot_state_reason
                    .clone()
                    .unwrap_or_else(|| "<unknown>".to_string()),
                selected_review_generation_id: sample.summary.selected_review_generation_id.clone(),
                selected_latest_release_generation_id: sample
                    .summary
                    .selected_latest_release_generation_id
                    .clone(),
                bundle_paths: records
                    .iter()
                    .map(|record| record.summary.bundle_path.clone())
                    .collect(),
                ambiguous_state_snapshot: sample.summary.selected_snapshot_state_verdict.as_deref()
                    == Some("artifact_state_ambiguous_legacy_state"),
                coherent_for_review_operations: sample
                    .summary
                    .coherent_for_review_operations
                    .unwrap_or(false),
            }
        })
        .filter(|summary| missing_from_archive || !summary.bundle_paths.is_empty())
        .collect()
}

fn build_bundles_not_matching_current_archive(
    bundle_records: &[StateSnapshotBundleRecord],
    canonical_state_archive_dir: Option<&Path>,
    archive_records: &[StateSnapshotSurfaceRecord],
) -> Vec<BundleOnlyStateSnapshotSummary> {
    let mut grouped = BTreeMap::<String, Vec<&StateSnapshotBundleRecord>>::new();
    for record in bundle_records.iter().filter(|record| {
        !bundle_matches_current_archive(record, canonical_state_archive_dir, archive_records)
    }) {
        grouped
            .entry(record.identity_key.clone())
            .or_default()
            .push(record);
    }
    grouped
        .into_iter()
        .map(|(identity_key, records)| {
            let sample = records.first().expect("bundle sample");
            BundleOnlyStateSnapshotSummary {
                identity_key,
                snapshotted_at: sample
                    .summary
                    .snapshotted_at
                    .expect("bundle snapshotted_at"),
                state_verdict: sample
                    .summary
                    .selected_snapshot_state_verdict
                    .clone()
                    .unwrap_or_else(|| "<unknown>".to_string()),
                state_reason: sample
                    .summary
                    .selected_snapshot_state_reason
                    .clone()
                    .unwrap_or_else(|| "<unknown>".to_string()),
                selected_review_generation_id: sample.summary.selected_review_generation_id.clone(),
                selected_latest_release_generation_id: sample
                    .summary
                    .selected_latest_release_generation_id
                    .clone(),
                bundle_paths: records
                    .iter()
                    .map(|record| record.summary.bundle_path.clone())
                    .collect(),
                ambiguous_state_snapshot: sample.summary.selected_snapshot_state_verdict.as_deref()
                    == Some("artifact_state_ambiguous_legacy_state"),
                coherent_for_review_operations: sample
                    .summary
                    .coherent_for_review_operations
                    .unwrap_or(false),
            }
        })
        .collect()
}

fn determine_pointer_relation(
    pointer_configured: bool,
    pointer_report: &activation_artifact_state_publish_report::ArtifactStateSnapshotPublishReport,
    loaded_pointer_metadata: Option<
        &activation_artifact_state_publish_report::LoadedStateLatestPointerMetadata,
    >,
    present_in_archive: bool,
    present_in_history: bool,
    present_in_bundles: bool,
    matches_latest_archive: bool,
    matches_latest_history: bool,
) -> LatestStateSnapshotBundleRelation {
    use activation_artifact_state_publish_report::ArtifactStateSnapshotPublisherVerdict as PointerVerdict;

    if !pointer_configured {
        return LatestStateSnapshotBundleRelation::NoPointerConfigured;
    }
    if loaded_pointer_metadata.is_none()
        && pointer_report.verdict == PointerVerdict::ArtifactStateSnapshotVerifyMissingTarget
    {
        return LatestStateSnapshotBundleRelation::NoPointerMetadata;
    }
    if pointer_report.verdict == PointerVerdict::ArtifactStateSnapshotInvalidMetadata {
        return LatestStateSnapshotBundleRelation::InvalidMetadata;
    }
    if pointer_report.verdict == PointerVerdict::ArtifactStateSnapshotVerifyMissingTarget {
        return LatestStateSnapshotBundleRelation::MissingTarget;
    }
    if !present_in_archive {
        return LatestStateSnapshotBundleRelation::MissingFromArchive;
    }
    if !present_in_history {
        return LatestStateSnapshotBundleRelation::MissingFromHistory;
    }
    if !present_in_bundles {
        return LatestStateSnapshotBundleRelation::MissingBundleCoverage;
    }
    if loaded_pointer_metadata
        .is_some_and(|loaded| is_hard_non_green_state_verdict(&loaded.metadata.snapshot_verdict))
    {
        return LatestStateSnapshotBundleRelation::NonGreenStateSnapshot;
    }
    if loaded_pointer_metadata.is_some_and(|loaded| {
        loaded.metadata.snapshot_verdict == "artifact_state_ambiguous_legacy_state"
    }) {
        return LatestStateSnapshotBundleRelation::AmbiguousStateSnapshot;
    }
    if !matches_latest_archive {
        return LatestStateSnapshotBundleRelation::BehindLatestArchive;
    }
    if !matches_latest_history {
        return LatestStateSnapshotBundleRelation::BehindLatestHistory;
    }
    LatestStateSnapshotBundleRelation::CoveredByArchiveHistoryAndBundle
}

fn identity_key_from_snapshot(
    artifact: &activation_artifact_state_publish_report::ArtifactStateSnapshotArtifact,
) -> String {
    identity_key_from_values(
        artifact.snapshotted_at,
        &artifact.state_verdict,
        artifact.selected_review_generation_id.as_deref(),
        artifact.selected_latest_release_generation_id.as_deref(),
    )
}

fn identity_key_from_pointer_metadata(
    metadata: &activation_artifact_state_publish_report::StateLatestPointerMetadata,
) -> String {
    identity_key_from_values(
        metadata.snapshotted_at,
        &metadata.snapshot_verdict,
        metadata.selected_review_generation_id.as_deref(),
        metadata.selected_latest_release_generation_id.as_deref(),
    )
}

fn identity_key_from_values(
    snapshotted_at: DateTime<Utc>,
    state_verdict: &str,
    selected_review_generation_id: Option<&str>,
    selected_latest_release_generation_id: Option<&str>,
) -> String {
    format!(
        "{}|{}|{}|{}",
        snapshotted_at.to_rfc3339(),
        state_verdict,
        selected_review_generation_id.unwrap_or("<none>"),
        selected_latest_release_generation_id.unwrap_or("<none>"),
    )
}

fn unique_ambiguous_snapshot_count(
    archive_snapshots: &[StateSnapshotBundleCoverageSummary],
    history_only: &[HistoryOnlyStateSnapshotSummary],
    bundle_missing_from_archive: &[BundleOnlyStateSnapshotSummary],
    bundle_missing_from_history: &[BundleOnlyStateSnapshotSummary],
) -> usize {
    archive_snapshots
        .iter()
        .filter(|summary| summary.ambiguous_state_snapshot)
        .map(|summary| summary.identity_key.clone())
        .chain(
            history_only
                .iter()
                .filter(|summary| summary.ambiguous_state_snapshot)
                .map(|summary| summary.identity_key.clone()),
        )
        .chain(
            bundle_missing_from_archive
                .iter()
                .filter(|summary| summary.ambiguous_state_snapshot)
                .map(|summary| summary.identity_key.clone()),
        )
        .chain(
            bundle_missing_from_history
                .iter()
                .filter(|summary| summary.ambiguous_state_snapshot)
                .map(|summary| summary.identity_key.clone()),
        )
        .collect::<BTreeSet<_>>()
        .len()
}

fn unique_non_green_bundle_count(
    bundle_map: &BTreeMap<String, Vec<&StateSnapshotBundleRecord>>,
) -> usize {
    bundle_map
        .iter()
        .filter(|(_, records)| {
            records.first().is_some_and(|record| {
                record
                    .summary
                    .selected_snapshot_state_verdict
                    .as_deref()
                    .is_some_and(is_hard_non_green_state_verdict)
            })
        })
        .count()
}

fn is_hard_non_green_state_verdict(verdict: &str) -> bool {
    matches!(
        verdict,
        "artifact_state_incomplete"
            | "artifact_state_inconsistent"
            | "artifact_state_invalid_artifacts_present"
    )
}

fn serialize_enum<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value)
        .unwrap_or_default()
        .trim_matches('"')
        .to_string()
}

fn render_human(report: &StateSnapshotBundleProvenanceReport) -> String {
    [
        "event=copybot_activation_artifact_state_bundle_provenance_report".to_string(),
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("archive_snapshot_count={}", report.archive_snapshot_count),
        format!("history_snapshot_count={}", report.history_snapshot_count),
        format!("bundle_count={}", report.bundle_count),
        format!(
            "latest_pointer_relation={}",
            serialize_enum(&report.latest_pointer_relation)
        ),
        format!(
            "latest_pointer_selected_snapshot_path={}",
            report
                .latest_pointer_selected_snapshot_path
                .clone()
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!(
            "latest_pointer_selected_snapshot_has_bundle_coverage={}",
            report
                .latest_pointer_selected_snapshot_has_bundle_coverage
                .map(|value| value.to_string())
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!(
            "archive_snapshots_missing_from_history_count={}",
            report.archive_snapshots_missing_from_history_count
        ),
        format!(
            "archive_snapshots_missing_from_bundle_count={}",
            report.archive_snapshots_missing_from_bundle_count
        ),
        format!(
            "bundle_snapshots_missing_from_archive_count={}",
            report.bundle_snapshots_missing_from_archive_count
        ),
        format!(
            "bundle_snapshots_missing_from_history_count={}",
            report.bundle_snapshots_missing_from_history_count
        ),
        format!("invalid_artifact_count={}", report.invalid_artifact_count),
        format!(
            "ambiguous_snapshot_count={}",
            report.ambiguous_snapshot_count
        ),
        format!(
            "non_green_bundled_snapshot_count={}",
            report.non_green_bundled_snapshot_count
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
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn complete_lineage_across_archive_pointer_history_and_bundle_yields_green() {
        let root = temp_dir("state_bundle_provenance_complete");
        let archive_dir = root.join("archive");
        let history_dir = root.join("history");
        let pointer_dir = root.join("pointer");
        let bundle_dir = root.join("bundles");
        let snapshot_path =
            archive_dir.join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        let history_snapshot_path =
            history_dir.join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        write_snapshot(
            &snapshot_path,
            "artifact_state_coherent",
            "review_gen_1",
            "release_gen_1",
            true,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        fs::create_dir_all(&history_dir).expect("history dir");
        fs::copy(&snapshot_path, &history_snapshot_path).expect("copy history");
        write_pointer_metadata(
            &pointer_dir,
            activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME,
            &archive_dir,
            &snapshot_path,
            "artifact_state_coherent",
            "review_gen_1",
            "release_gen_1",
            0,
            true,
            ts("2026-03-26T18:00:00Z"),
        );
        activation_artifact_state_bundle::export_state_snapshot_bundle(
            &archive_dir,
            "state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json",
            &bundle_dir.join("bundle_a"),
        )
        .expect("export bundle");

        let report = build_report(&Config {
            state_archive_dir: archive_dir,
            snapshot_latest_pointer_dir: Some(pointer_dir),
            history_dir: Some(history_dir),
            history_snapshot_paths: Vec::new(),
            bundle_dir: Some(bundle_dir),
            bundle_paths: Vec::new(),
            pointer_name: activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleProvenanceVerdict::ArtifactStateBundleProvenanceComplete
        );
        assert_eq!(
            report.latest_pointer_relation,
            LatestStateSnapshotBundleRelation::CoveredByArchiveHistoryAndBundle
        );
        assert_eq!(report.bundle_count, 1);
        assert_eq!(
            report.latest_pointer_selected_snapshot_has_bundle_coverage,
            Some(true)
        );
    }

    #[test]
    fn foreign_bundle_with_same_identity_does_not_count_as_current_archive_coverage() {
        let root = temp_dir("state_bundle_provenance_foreign_bundle");
        let current_archive_dir = root.join("archive_current");
        let foreign_archive_dir = root.join("archive_foreign");
        let history_dir = root.join("history");
        let bundle_dir = root.join("bundles");
        let current_snapshot = current_archive_dir
            .join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        let foreign_snapshot = foreign_archive_dir
            .join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        let history_snapshot =
            history_dir.join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        write_snapshot(
            &current_snapshot,
            "artifact_state_coherent",
            "review_gen_1",
            "release_gen_1",
            true,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        write_snapshot(
            &foreign_snapshot,
            "artifact_state_coherent",
            "review_gen_1",
            "release_gen_1",
            true,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        fs::create_dir_all(&history_dir).expect("history dir");
        fs::copy(&current_snapshot, &history_snapshot).expect("copy history");
        activation_artifact_state_bundle::export_state_snapshot_bundle(
            &foreign_archive_dir,
            "state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json",
            &bundle_dir.join("bundle_foreign"),
        )
        .expect("export foreign bundle");

        let report = build_report(&Config {
            state_archive_dir: current_archive_dir,
            snapshot_latest_pointer_dir: None,
            history_dir: Some(history_dir),
            history_snapshot_paths: Vec::new(),
            bundle_dir: Some(bundle_dir),
            bundle_paths: Vec::new(),
            pointer_name: activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            json: false,
        })
        .expect("report");

        assert_ne!(
            report.verdict,
            ArtifactStateBundleProvenanceVerdict::ArtifactStateBundleProvenanceComplete
        );
        assert_eq!(report.bundle_snapshots_missing_from_archive_count, 1);
        assert_eq!(report.archive_snapshots_missing_from_bundle_count, 1);
    }

    #[test]
    fn latest_pointer_selected_snapshot_requires_same_archive_bundle_coverage() {
        let root = temp_dir("state_bundle_provenance_foreign_pointer_bundle");
        let current_archive_dir = root.join("archive_current");
        let foreign_archive_dir = root.join("archive_foreign");
        let history_dir = root.join("history");
        let pointer_dir = root.join("pointer");
        let bundle_dir = root.join("bundles");
        let current_snapshot = current_archive_dir
            .join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        let foreign_snapshot = foreign_archive_dir
            .join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        let history_snapshot =
            history_dir.join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        write_snapshot(
            &current_snapshot,
            "artifact_state_coherent",
            "review_gen_1",
            "release_gen_1",
            true,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        write_snapshot(
            &foreign_snapshot,
            "artifact_state_coherent",
            "review_gen_1",
            "release_gen_1",
            true,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        fs::create_dir_all(&history_dir).expect("history dir");
        fs::copy(&current_snapshot, &history_snapshot).expect("copy history");
        write_pointer_metadata(
            &pointer_dir,
            activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME,
            &current_archive_dir,
            &current_snapshot,
            "artifact_state_coherent",
            "review_gen_1",
            "release_gen_1",
            0,
            true,
            ts("2026-03-26T18:00:00Z"),
        );
        activation_artifact_state_bundle::export_state_snapshot_bundle(
            &foreign_archive_dir,
            "state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json",
            &bundle_dir.join("bundle_foreign"),
        )
        .expect("export foreign bundle");

        let report = build_report(&Config {
            state_archive_dir: current_archive_dir,
            snapshot_latest_pointer_dir: Some(pointer_dir),
            history_dir: Some(history_dir),
            history_snapshot_paths: Vec::new(),
            bundle_dir: Some(bundle_dir),
            bundle_paths: Vec::new(),
            pointer_name: activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.latest_pointer_relation,
            LatestStateSnapshotBundleRelation::MissingBundleCoverage
        );
        assert_eq!(
            report.latest_pointer_selected_snapshot_has_bundle_coverage,
            Some(false)
        );
    }

    #[test]
    fn latest_pointer_selected_snapshot_missing_bundle_coverage_yields_incomplete() {
        let root = temp_dir("state_bundle_provenance_missing_pointer_bundle");
        let archive_dir = root.join("archive");
        let history_dir = root.join("history");
        let pointer_dir = root.join("pointer");
        let bundle_dir = root.join("bundles");
        let older_snapshot =
            archive_dir.join("state_snapshot__2026-03-26T17-00-00Z__artifact_state_coherent.json");
        let latest_snapshot =
            archive_dir.join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        write_snapshot(
            &older_snapshot,
            "artifact_state_coherent",
            "review_gen_older",
            "release_gen_older",
            true,
            0,
            ts("2026-03-26T17:00:00Z"),
        );
        write_snapshot(
            &latest_snapshot,
            "artifact_state_coherent",
            "review_gen_latest",
            "release_gen_latest",
            true,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        fs::create_dir_all(&history_dir).expect("history dir");
        fs::copy(
            &older_snapshot,
            history_dir.join(older_snapshot.file_name().expect("older name")),
        )
        .expect("copy older history");
        fs::copy(
            &latest_snapshot,
            history_dir.join(latest_snapshot.file_name().expect("latest name")),
        )
        .expect("copy latest history");
        write_pointer_metadata(
            &pointer_dir,
            activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME,
            &archive_dir,
            &latest_snapshot,
            "artifact_state_coherent",
            "review_gen_latest",
            "release_gen_latest",
            0,
            true,
            ts("2026-03-26T18:00:00Z"),
        );
        activation_artifact_state_bundle::export_state_snapshot_bundle(
            &archive_dir,
            "state_snapshot__2026-03-26T17-00-00Z__artifact_state_coherent.json",
            &bundle_dir.join("bundle_older"),
        )
        .expect("export older bundle");

        let report = build_report(&Config {
            state_archive_dir: archive_dir,
            snapshot_latest_pointer_dir: Some(pointer_dir),
            history_dir: Some(history_dir),
            history_snapshot_paths: Vec::new(),
            bundle_dir: Some(bundle_dir),
            bundle_paths: Vec::new(),
            pointer_name: activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleProvenanceVerdict::ArtifactStateBundleProvenanceIncomplete
        );
        assert_eq!(
            report.latest_pointer_relation,
            LatestStateSnapshotBundleRelation::MissingBundleCoverage
        );
        assert_eq!(
            report.latest_pointer_selected_snapshot_has_bundle_coverage,
            Some(false)
        );
    }

    #[test]
    fn bundle_referencing_snapshot_missing_from_archive_yields_inconsistent_lineage() {
        let root = temp_dir("state_bundle_provenance_missing_archive");
        let archive_dir = root.join("archive");
        let history_dir = root.join("history");
        let bundle_dir = root.join("bundles");
        let snapshot_path =
            archive_dir.join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        let history_snapshot_path =
            history_dir.join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        write_snapshot(
            &snapshot_path,
            "artifact_state_coherent",
            "review_gen_1",
            "release_gen_1",
            true,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        fs::create_dir_all(&history_dir).expect("history dir");
        fs::copy(&snapshot_path, &history_snapshot_path).expect("copy history");
        activation_artifact_state_bundle::export_state_snapshot_bundle(
            &archive_dir,
            "state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json",
            &bundle_dir.join("bundle_a"),
        )
        .expect("export bundle");
        fs::remove_file(&snapshot_path).expect("remove archive snapshot");

        let report = build_report(&Config {
            state_archive_dir: archive_dir,
            snapshot_latest_pointer_dir: None,
            history_dir: Some(history_dir),
            history_snapshot_paths: Vec::new(),
            bundle_dir: Some(bundle_dir),
            bundle_paths: Vec::new(),
            pointer_name: activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleProvenanceVerdict::ArtifactStateBundleProvenanceInconsistentLineage
        );
        assert_eq!(report.bundle_snapshots_missing_from_archive_count, 1);
    }

    #[test]
    fn malformed_bundle_metadata_is_reported_as_invalid() {
        let root = temp_dir("state_bundle_provenance_invalid_bundle");
        let archive_dir = root.join("archive");
        let history_dir = root.join("history");
        let bundle_dir = root.join("bundles");
        fs::create_dir_all(&archive_dir).expect("archive dir");
        fs::create_dir_all(&history_dir).expect("history dir");
        let malformed_bundle = bundle_dir.join("bundle_bad");
        fs::create_dir_all(&malformed_bundle).expect("bundle dir");
        fs::write(
            malformed_bundle.join(activation_artifact_state_bundle::BUNDLE_MANIFEST_FILENAME),
            "{broken",
        )
        .expect("write bad manifest");

        let report = build_report(&Config {
            state_archive_dir: archive_dir,
            snapshot_latest_pointer_dir: None,
            history_dir: Some(history_dir),
            history_snapshot_paths: Vec::new(),
            bundle_dir: Some(bundle_dir),
            bundle_paths: Vec::new(),
            pointer_name: activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleProvenanceVerdict::ArtifactStateBundleProvenanceInvalidArtifactsPresent
        );
        assert_eq!(report.invalid_artifact_count, 1);
    }

    #[test]
    fn ambiguous_bundled_snapshot_is_not_treated_as_clean_green() {
        let root = temp_dir("state_bundle_provenance_ambiguous");
        let archive_dir = root.join("archive");
        let history_dir = root.join("history");
        let pointer_dir = root.join("pointer");
        let bundle_dir = root.join("bundles");
        let snapshot_path = archive_dir.join(
            "state_snapshot__2026-03-26T18-00-00Z__artifact_state_ambiguous_legacy_state.json",
        );
        let history_snapshot_path = history_dir.join(
            "state_snapshot__2026-03-26T18-00-00Z__artifact_state_ambiguous_legacy_state.json",
        );
        write_snapshot(
            &snapshot_path,
            "artifact_state_ambiguous_legacy_state",
            "review_gen_1",
            "release_gen_1",
            true,
            2,
            ts("2026-03-26T18:00:00Z"),
        );
        fs::create_dir_all(&history_dir).expect("history dir");
        fs::copy(&snapshot_path, &history_snapshot_path).expect("copy history");
        write_pointer_metadata(
            &pointer_dir,
            activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME,
            &archive_dir,
            &snapshot_path,
            "artifact_state_ambiguous_legacy_state",
            "review_gen_1",
            "release_gen_1",
            2,
            false,
            ts("2026-03-26T18:00:00Z"),
        );
        activation_artifact_state_bundle::export_state_snapshot_bundle(
            &archive_dir,
            "state_snapshot__2026-03-26T18-00-00Z__artifact_state_ambiguous_legacy_state.json",
            &bundle_dir.join("bundle_a"),
        )
        .expect("export bundle");

        let report = build_report(&Config {
            state_archive_dir: archive_dir,
            snapshot_latest_pointer_dir: Some(pointer_dir),
            history_dir: Some(history_dir),
            history_snapshot_paths: Vec::new(),
            bundle_dir: Some(bundle_dir),
            bundle_paths: Vec::new(),
            pointer_name: activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleProvenanceVerdict::ArtifactStateBundleProvenanceAmbiguousLegacyState
        );
        assert_eq!(report.ambiguous_snapshot_count, 1);
        assert_eq!(
            report.latest_pointer_relation,
            LatestStateSnapshotBundleRelation::AmbiguousStateSnapshot
        );
    }

    #[test]
    fn report_is_read_only_for_archive_snapshots() {
        let root = temp_dir("state_bundle_provenance_read_only");
        let archive_dir = root.join("archive");
        let history_dir = root.join("history");
        let bundle_dir = root.join("bundles");
        let snapshot_path =
            archive_dir.join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        write_snapshot(
            &snapshot_path,
            "artifact_state_coherent",
            "review_gen_1",
            "release_gen_1",
            true,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        fs::create_dir_all(&history_dir).expect("history dir");
        fs::copy(
            &snapshot_path,
            history_dir.join(snapshot_path.file_name().expect("snapshot name")),
        )
        .expect("copy history");
        activation_artifact_state_bundle::export_state_snapshot_bundle(
            &archive_dir,
            "state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json",
            &bundle_dir.join("bundle_a"),
        )
        .expect("export bundle");
        let before = fs::read_to_string(&snapshot_path).expect("before");

        let _ = build_report(&Config {
            state_archive_dir: archive_dir,
            snapshot_latest_pointer_dir: None,
            history_dir: Some(history_dir),
            history_snapshot_paths: Vec::new(),
            bundle_dir: Some(bundle_dir),
            bundle_paths: Vec::new(),
            pointer_name: activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            json: false,
        })
        .expect("report");

        let after = fs::read_to_string(&snapshot_path).expect("after");
        assert_eq!(before, after);
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
        snapshot_verdict: &str,
        selected_review_generation_id: &str,
        selected_latest_release_generation_id: &str,
        ambiguous_legacy_count: usize,
        coherent_for_review_operations: bool,
        snapshotted_at: DateTime<Utc>,
    ) {
        fs::create_dir_all(pointer_dir).expect("pointer dir");
        let metadata = activation_artifact_state_publish_report::StateLatestPointerMetadata {
            pointer_version: "1".to_string(),
            pointer_name: pointer_name.to_string(),
            source_state_archive_dir: fs::canonicalize(archive_dir)
                .expect("canonical archive dir")
                .display()
                .to_string(),
            selected_snapshot_path: fs::canonicalize(snapshot_path)
                .expect("canonical snapshot path")
                .display()
                .to_string(),
            selected_snapshot_file_name: snapshot_path
                .file_name()
                .expect("snapshot file name")
                .to_string_lossy()
                .into_owned(),
            snapshot_mode: "artifact_state_snapshot".to_string(),
            snapshotted_at,
            snapshot_verdict: snapshot_verdict.to_string(),
            snapshot_reason: format!("sample {snapshot_verdict}"),
            selected_review_generation_id: Some(selected_review_generation_id.to_string()),
            selected_latest_release_generation_id: Some(
                selected_latest_release_generation_id.to_string(),
            ),
            selection_alignment_matches: true,
            coherent_for_review_operations,
            ambiguous_legacy_count,
            pointed_at: snapshotted_at + chrono::Duration::seconds(30),
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
