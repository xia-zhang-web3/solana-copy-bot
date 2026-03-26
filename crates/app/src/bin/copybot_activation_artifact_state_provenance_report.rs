#![recursion_limit = "256"]

use anyhow::{anyhow, bail, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[allow(dead_code)]
#[path = "copybot_activation_artifact_state_publish_report.rs"]
mod activation_artifact_state_publish_report;

const USAGE: &str = "usage: copybot_activation_artifact_state_provenance_report --state-archive-dir <path> --snapshot-latest-pointer-dir <path> [--history-dir <path>] [--history-snapshot <path>]... [--pointer-name <name>] [--json]";

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
    snapshot_latest_pointer_dir: PathBuf,
    history_dir: Option<PathBuf>,
    history_snapshot_paths: Vec<PathBuf>,
    pointer_name: String,
    json: bool,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ArtifactStateProvenanceVerdict {
    ArtifactStateProvenanceComplete,
    ArtifactStateProvenanceIncomplete,
    ArtifactStateProvenanceInvalidArtifactsPresent,
    ArtifactStateProvenanceInconsistentLineage,
    ArtifactStateProvenanceAmbiguousLegacyState,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LatestStateSnapshotPointerRelation {
    NoPointerMetadata,
    InvalidMetadata,
    MissingTarget,
    MissingFromArchive,
    MissingFromHistory,
    BehindLatestArchive,
    BehindLatestHistory,
    MatchesLatestArchiveAndHistory,
    MatchesLatestArchiveOnly,
    AmbiguousStateSnapshot,
}

#[derive(Debug, Clone, Serialize)]
struct StateSnapshotLineageIssue {
    surface: String,
    path: String,
    error: String,
}

#[derive(Debug, Clone, Serialize)]
struct StateSnapshotCoverageSummary {
    identity_key: String,
    snapshotted_at: DateTime<Utc>,
    state_verdict: String,
    selected_review_generation_id: Option<String>,
    selected_latest_release_generation_id: Option<String>,
    archive_paths: Vec<String>,
    history_paths: Vec<String>,
    latest_pointer_selected: bool,
    latest_by_archive_timestamp: bool,
    latest_by_history_timestamp: bool,
    missing_history_coverage: bool,
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
struct LatestStateSnapshotPointerSummary {
    pointer_name: String,
    pointer_verdict: String,
    pointer_reason: String,
    pointer_path: Option<String>,
    source_state_archive_dir: Option<String>,
    selected_snapshot_path: Option<String>,
    selected_snapshot_file_name: Option<String>,
    selected_snapshotted_at: Option<DateTime<Utc>>,
    selected_state_verdict: Option<String>,
    selected_review_generation_id: Option<String>,
    selected_latest_release_generation_id: Option<String>,
    pointed_at: Option<DateTime<Utc>>,
    target_exists: bool,
    target_matches_identity: bool,
    present_in_archive: bool,
    present_in_history: bool,
    matches_latest_archive: bool,
    matches_latest_history: bool,
    relation: LatestStateSnapshotPointerRelation,
    missing_paths: Vec<String>,
    inconsistencies: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct StateSnapshotProvenanceReport {
    mode: String,
    verdict: ArtifactStateProvenanceVerdict,
    reason: String,
    state_archive_dir: String,
    snapshot_latest_pointer_dir: String,
    history_dir: Option<String>,
    history_snapshot_paths: Vec<String>,
    pointer_name: String,
    state_snapshot_archive_count: usize,
    history_snapshot_count: usize,
    latest_pointer_present: bool,
    latest_pointer_selected_snapshot_path: Option<String>,
    latest_pointer_relation: LatestStateSnapshotPointerRelation,
    latest_archive_snapshot_path: Option<String>,
    latest_history_snapshot_path: Option<String>,
    archive_snapshots_missing_from_history_count: usize,
    history_snapshots_missing_from_archive_count: usize,
    invalid_artifact_count: usize,
    invalid_artifacts: Vec<StateSnapshotLineageIssue>,
    ambiguous_snapshot_count: usize,
    archive_snapshots: Vec<StateSnapshotCoverageSummary>,
    history_snapshots_missing_from_archive: Vec<HistoryOnlyStateSnapshotSummary>,
    latest_pointer: LatestStateSnapshotPointerSummary,
    state_snapshot_lineage_only: bool,
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

    Ok(Some(Config {
        state_archive_dir: state_archive_dir
            .ok_or_else(|| anyhow!("--state-archive-dir is required"))?,
        snapshot_latest_pointer_dir: snapshot_latest_pointer_dir
            .ok_or_else(|| anyhow!("--snapshot-latest-pointer-dir is required"))?,
        history_dir,
        history_snapshot_paths,
        pointer_name,
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

fn build_report(config: &Config) -> Result<StateSnapshotProvenanceReport> {
    let archive_paths = collect_snapshot_paths(Some(&config.state_archive_dir), &[])?;
    let history_paths = collect_snapshot_paths(
        config.history_dir.as_deref(),
        &config.history_snapshot_paths,
    )?;

    let (archive_records, mut invalid_artifacts) =
        load_state_surface("state_archive", &archive_paths);
    let (history_records, invalid_history) = load_state_surface("history", &history_paths);
    invalid_artifacts.extend(invalid_history);

    let pointer_report =
        match activation_artifact_state_publish_report::inspect_latest_pointer_report(
            &config.state_archive_dir,
            &config.snapshot_latest_pointer_dir,
            &config.pointer_name,
            true,
        ) {
            Ok(report) => report,
            Err(error) => {
                invalid_artifacts.push(StateSnapshotLineageIssue {
                    surface: "state_latest_pointer".to_string(),
                    path: config
                        .snapshot_latest_pointer_dir
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
                latest_pointer_dir: Some(config.snapshot_latest_pointer_dir.display().to_string()),
                latest_pointer_name: config.pointer_name.clone(),
                latest_pointer_path: Some(
                    config
                        .snapshot_latest_pointer_dir
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
                    "State-snapshot provenance only audits persisted state snapshot lineage. It does not authorize activation, does not enable execution, and does not override the Stage 3 production gate.".to_string(),
            }
            }
        };
    let loaded_pointer_metadata =
        match activation_artifact_state_publish_report::inspect_latest_pointer_metadata(
            &config.snapshot_latest_pointer_dir,
            &config.pointer_name,
        ) {
            Ok(metadata) => metadata,
            Err(error) => {
                invalid_artifacts.push(StateSnapshotLineageIssue {
                    surface: "state_latest_pointer".to_string(),
                    path: config
                        .snapshot_latest_pointer_dir
                        .join(format!("{}.json", config.pointer_name))
                        .display()
                        .to_string(),
                    error: format!("{error:#}"),
                });
                None
            }
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

    let latest_archive = latest_record(&archive_records);
    let latest_history = latest_record(&history_records);

    let selected_pointer_canonical_path = loaded_pointer_metadata.as_ref().and_then(|loaded| {
        fs::canonicalize(Path::new(&loaded.metadata.selected_snapshot_path)).ok()
    });
    let selected_pointer_identity_key = loaded_pointer_metadata
        .as_ref()
        .map(|loaded| identity_key_from_metadata(&loaded.metadata));

    let present_in_archive = selected_pointer_canonical_path
        .as_ref()
        .is_some_and(|selected| {
            archive_records
                .iter()
                .any(|record| &record.canonical_path == selected)
        });
    let present_in_history = selected_pointer_identity_key
        .as_ref()
        .is_some_and(|identity_key| history_map.contains_key(identity_key));
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
            let sample = records.first().expect("archive record");
            let history_matches = history_map.get(identity_key);
            StateSnapshotCoverageSummary {
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
                latest_pointer_selected: selected_pointer_identity_key
                    .as_ref()
                    .is_some_and(|selected| selected == identity_key),
                latest_by_archive_timestamp: latest_archive
                    .is_some_and(|latest| latest.identity_key == *identity_key),
                latest_by_history_timestamp: latest_history
                    .is_some_and(|latest| latest.identity_key == *identity_key),
                missing_history_coverage: history_matches.is_none(),
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
            let sample = records.first().expect("history record");
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

    let pointer_relation = determine_pointer_relation(
        &pointer_report,
        loaded_pointer_metadata.as_ref(),
        present_in_archive,
        present_in_history,
        matches_latest_archive,
        matches_latest_history,
        latest_archive,
        latest_history,
    );

    let latest_pointer_summary = LatestStateSnapshotPointerSummary {
        pointer_name: config.pointer_name.clone(),
        pointer_verdict: serialize_enum(&pointer_report.verdict),
        pointer_reason: pointer_report.reason.clone(),
        pointer_path: loaded_pointer_metadata
            .as_ref()
            .map(|loaded| loaded.path.display().to_string())
            .or(pointer_report.latest_pointer_path.clone()),
        source_state_archive_dir: loaded_pointer_metadata
            .as_ref()
            .map(|loaded| loaded.metadata.source_state_archive_dir.clone())
            .or(pointer_report
                .latest_pointer_source_state_archive_dir
                .clone()),
        selected_snapshot_path: loaded_pointer_metadata
            .as_ref()
            .map(|loaded| loaded.metadata.selected_snapshot_path.clone()),
        selected_snapshot_file_name: loaded_pointer_metadata
            .as_ref()
            .map(|loaded| loaded.metadata.selected_snapshot_file_name.clone()),
        selected_snapshotted_at: loaded_pointer_metadata
            .as_ref()
            .map(|loaded| loaded.metadata.snapshotted_at),
        selected_state_verdict: loaded_pointer_metadata
            .as_ref()
            .map(|loaded| loaded.metadata.snapshot_verdict.clone()),
        selected_review_generation_id: loaded_pointer_metadata
            .as_ref()
            .and_then(|loaded| loaded.metadata.selected_review_generation_id.clone()),
        selected_latest_release_generation_id: loaded_pointer_metadata.as_ref().and_then(
            |loaded| {
                loaded
                    .metadata
                    .selected_latest_release_generation_id
                    .clone()
            },
        ),
        pointed_at: loaded_pointer_metadata
            .as_ref()
            .map(|loaded| loaded.metadata.pointed_at),
        target_exists: pointer_report.latest_pointer_target_exists,
        target_matches_identity: pointer_report.latest_pointer_target_matches_identity,
        present_in_archive,
        present_in_history,
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
    let history_missing_from_archive_count = history_snapshots_missing_from_archive.len();
    let ambiguous_snapshot_count = unique_ambiguous_snapshot_count(
        &archive_snapshots,
        &history_snapshots_missing_from_archive,
    );
    let latest_current_state_hard_non_green =
        latest_archive.is_some_and(|record| {
            matches!(
                record.loaded.artifact.state_verdict.as_str(),
                "artifact_state_incomplete"
                    | "artifact_state_inconsistent"
                    | "artifact_state_invalid_artifacts_present"
            )
        }) || latest_history.is_some_and(|record| {
            matches!(
                record.loaded.artifact.state_verdict.as_str(),
                "artifact_state_incomplete"
                    | "artifact_state_inconsistent"
                    | "artifact_state_invalid_artifacts_present"
            )
        }) || loaded_pointer_metadata.as_ref().is_some_and(|loaded| {
            matches!(
                loaded.metadata.snapshot_verdict.as_str(),
                "artifact_state_incomplete"
                    | "artifact_state_inconsistent"
                    | "artifact_state_invalid_artifacts_present"
            )
        });

    let (verdict, reason) = if !invalid_artifacts.is_empty() {
        (
            ArtifactStateProvenanceVerdict::ArtifactStateProvenanceInvalidArtifactsPresent,
            format!(
                "state snapshot provenance contains {} invalid artifact(s); review invalid_artifacts before trusting snapshot lineage",
                invalid_artifacts.len()
            ),
        )
    } else if matches!(
        latest_pointer_summary.relation,
        LatestStateSnapshotPointerRelation::MissingTarget
            | LatestStateSnapshotPointerRelation::MissingFromArchive
            | LatestStateSnapshotPointerRelation::InvalidMetadata
    ) || history_missing_from_archive_count > 0
    {
        (
            ArtifactStateProvenanceVerdict::ArtifactStateProvenanceInconsistentLineage,
            "state snapshot lineage is inconsistent: latest pointer or history references do not resolve cleanly against the persisted archive".to_string(),
        )
    } else if archive_snapshots.is_empty() {
        (
            ArtifactStateProvenanceVerdict::ArtifactStateProvenanceIncomplete,
            "no persisted state snapshot artifacts were found in the state snapshot archive"
                .to_string(),
        )
    } else if archive_missing_from_history_count > 0
        || matches!(
            latest_pointer_summary.relation,
            LatestStateSnapshotPointerRelation::NoPointerMetadata
                | LatestStateSnapshotPointerRelation::MissingFromHistory
                | LatestStateSnapshotPointerRelation::BehindLatestArchive
                | LatestStateSnapshotPointerRelation::BehindLatestHistory
        )
        || latest_current_state_hard_non_green
    {
        (
            ArtifactStateProvenanceVerdict::ArtifactStateProvenanceIncomplete,
            "state snapshot lineage is incomplete: history or latest-pointer coverage is missing, stale, or depends on a non-green current-state snapshot".to_string(),
        )
    } else if ambiguous_snapshot_count > 0
        || latest_pointer_summary.relation
            == LatestStateSnapshotPointerRelation::AmbiguousStateSnapshot
    {
        (
            ArtifactStateProvenanceVerdict::ArtifactStateProvenanceAmbiguousLegacyState,
            "state snapshot lineage is structurally present, but it depends on ambiguous current-state snapshots and cannot be treated as clean green provenance".to_string(),
        )
    } else {
        (
            ArtifactStateProvenanceVerdict::ArtifactStateProvenanceComplete,
            "state snapshot archive, latest pointer, and history inputs are aligned".to_string(),
        )
    };

    Ok(StateSnapshotProvenanceReport {
        mode: "artifact_state_snapshot_provenance_report".to_string(),
        verdict,
        reason,
        state_archive_dir: config.state_archive_dir.display().to_string(),
        snapshot_latest_pointer_dir: config.snapshot_latest_pointer_dir.display().to_string(),
        history_dir: config
            .history_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        history_snapshot_paths: config
            .history_snapshot_paths
            .iter()
            .map(|path| path.display().to_string())
            .collect(),
        pointer_name: config.pointer_name.clone(),
        state_snapshot_archive_count: archive_records.len(),
        history_snapshot_count: history_records.len(),
        latest_pointer_present: pointer_report.latest_pointer_exists,
        latest_pointer_selected_snapshot_path: latest_pointer_summary.selected_snapshot_path.clone(),
        latest_pointer_relation: latest_pointer_summary.relation,
        latest_archive_snapshot_path: latest_archive
            .map(|record| record.loaded.path.display().to_string()),
        latest_history_snapshot_path: latest_history
            .map(|record| record.loaded.path.display().to_string()),
        archive_snapshots_missing_from_history_count: archive_missing_from_history_count,
        history_snapshots_missing_from_archive_count: history_missing_from_archive_count,
        invalid_artifact_count: invalid_artifacts.len(),
        invalid_artifacts,
        ambiguous_snapshot_count,
        archive_snapshots,
        history_snapshots_missing_from_archive,
        latest_pointer: latest_pointer_summary,
        state_snapshot_lineage_only: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary:
            "State-snapshot provenance only audits persisted state snapshot lineage. It does not authorize activation, does not enable execution, and does not override the Stage 3 production gate.".to_string(),
    })
}

fn determine_pointer_relation(
    pointer_report: &activation_artifact_state_publish_report::ArtifactStateSnapshotPublishReport,
    loaded_pointer_metadata: Option<
        &activation_artifact_state_publish_report::LoadedStateLatestPointerMetadata,
    >,
    present_in_archive: bool,
    present_in_history: bool,
    matches_latest_archive: bool,
    matches_latest_history: bool,
    latest_archive: Option<&StateSnapshotSurfaceRecord>,
    latest_history: Option<&StateSnapshotSurfaceRecord>,
) -> LatestStateSnapshotPointerRelation {
    use activation_artifact_state_publish_report::ArtifactStateSnapshotPublisherVerdict as PointerVerdict;

    if loaded_pointer_metadata.is_none()
        && pointer_report.verdict == PointerVerdict::ArtifactStateSnapshotVerifyMissingTarget
    {
        return LatestStateSnapshotPointerRelation::NoPointerMetadata;
    }
    if matches!(
        pointer_report.verdict,
        PointerVerdict::ArtifactStateSnapshotInvalidMetadata
    ) {
        return LatestStateSnapshotPointerRelation::InvalidMetadata;
    }
    if matches!(
        pointer_report.verdict,
        PointerVerdict::ArtifactStateSnapshotVerifyMissingTarget
    ) {
        return LatestStateSnapshotPointerRelation::MissingTarget;
    }
    if !present_in_archive {
        return LatestStateSnapshotPointerRelation::MissingFromArchive;
    }
    if !present_in_history {
        return LatestStateSnapshotPointerRelation::MissingFromHistory;
    }
    if !matches_latest_archive {
        return LatestStateSnapshotPointerRelation::BehindLatestArchive;
    }
    if !matches_latest_history {
        return LatestStateSnapshotPointerRelation::BehindLatestHistory;
    }
    if loaded_pointer_metadata.is_some_and(|loaded| {
        loaded.metadata.snapshot_verdict == "artifact_state_ambiguous_legacy_state"
    }) || latest_archive.is_some_and(|record| {
        record.loaded.artifact.state_verdict == "artifact_state_ambiguous_legacy_state"
    }) || latest_history.is_some_and(|record| {
        record.loaded.artifact.state_verdict == "artifact_state_ambiguous_legacy_state"
    }) {
        return LatestStateSnapshotPointerRelation::AmbiguousStateSnapshot;
    }
    if matches_latest_archive && matches_latest_history {
        LatestStateSnapshotPointerRelation::MatchesLatestArchiveAndHistory
    } else {
        LatestStateSnapshotPointerRelation::MatchesLatestArchiveOnly
    }
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
    Vec<StateSnapshotLineageIssue>,
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
            Err(error) => invalid.push(StateSnapshotLineageIssue {
                surface: surface.to_string(),
                path: path.display().to_string(),
                error: format!("{error:#}"),
            }),
        }
    }
    (valid, invalid)
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

fn identity_key_from_snapshot(
    artifact: &activation_artifact_state_publish_report::ArtifactStateSnapshotArtifact,
) -> String {
    format!(
        "{}|{}|{}|{}",
        artifact.snapshotted_at.to_rfc3339(),
        artifact.state_verdict,
        artifact
            .selected_review_generation_id
            .as_deref()
            .unwrap_or("<none>"),
        artifact
            .selected_latest_release_generation_id
            .as_deref()
            .unwrap_or("<none>")
    )
}

fn identity_key_from_metadata(
    metadata: &activation_artifact_state_publish_report::StateLatestPointerMetadata,
) -> String {
    format!(
        "{}|{}|{}|{}",
        metadata.snapshotted_at.to_rfc3339(),
        metadata.snapshot_verdict,
        metadata
            .selected_review_generation_id
            .as_deref()
            .unwrap_or("<none>"),
        metadata
            .selected_latest_release_generation_id
            .as_deref()
            .unwrap_or("<none>")
    )
}

fn unique_ambiguous_snapshot_count(
    archive_snapshots: &[StateSnapshotCoverageSummary],
    history_only: &[HistoryOnlyStateSnapshotSummary],
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
        .collect::<BTreeSet<_>>()
        .len()
}

fn serialize_enum<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value)
        .unwrap_or_default()
        .trim_matches('"')
        .to_string()
}

fn render_human(report: &StateSnapshotProvenanceReport) -> String {
    [
        "event=copybot_activation_artifact_state_provenance_report".to_string(),
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!(
            "state_snapshot_archive_count={}",
            report.state_snapshot_archive_count
        ),
        format!("history_snapshot_count={}", report.history_snapshot_count),
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
            "archive_snapshots_missing_from_history_count={}",
            report.archive_snapshots_missing_from_history_count
        ),
        format!(
            "history_snapshots_missing_from_archive_count={}",
            report.history_snapshots_missing_from_archive_count
        ),
        format!("invalid_artifact_count={}", report.invalid_artifact_count),
        format!(
            "ambiguous_snapshot_count={}",
            report.ambiguous_snapshot_count
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
    fn complete_lineage_across_archive_pointer_and_history_yields_green() {
        let root = temp_dir("state_snapshot_provenance_complete");
        let archive_dir = root.join("archive");
        let pointer_dir = root.join("pointer");
        let history_dir = root.join("history");
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
        fs::copy(&snapshot_path, &history_snapshot_path).expect("copy history snapshot");
        write_pointer_metadata(
            &pointer_dir,
            activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME,
            &archive_dir,
            &snapshot_path,
        );

        let report = build_report(&Config {
            state_archive_dir: archive_dir,
            snapshot_latest_pointer_dir: pointer_dir,
            history_dir: Some(history_dir),
            history_snapshot_paths: Vec::new(),
            pointer_name: activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateProvenanceVerdict::ArtifactStateProvenanceComplete
        );
        assert_eq!(
            report.latest_pointer_relation,
            LatestStateSnapshotPointerRelation::MatchesLatestArchiveAndHistory
        );
        assert_eq!(report.state_snapshot_archive_count, 1);
        assert_eq!(report.history_snapshot_count, 1);
        assert!(!report.activation_authorized);
    }

    #[test]
    fn latest_pointer_targeting_missing_snapshot_yields_inconsistent_lineage() {
        let root = temp_dir("state_snapshot_provenance_missing_target");
        let archive_dir = root.join("archive");
        let pointer_dir = root.join("pointer");
        let history_dir = root.join("history");
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
        fs::copy(&snapshot_path, &history_snapshot_path).expect("copy history snapshot");
        write_pointer_metadata(
            &pointer_dir,
            activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME,
            &archive_dir,
            &snapshot_path,
        );
        fs::remove_file(&snapshot_path).expect("remove archive snapshot");

        let report = build_report(&Config {
            state_archive_dir: archive_dir,
            snapshot_latest_pointer_dir: pointer_dir,
            history_dir: Some(history_dir),
            history_snapshot_paths: Vec::new(),
            pointer_name: activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateProvenanceVerdict::ArtifactStateProvenanceInconsistentLineage
        );
        assert_eq!(
            report.latest_pointer_relation,
            LatestStateSnapshotPointerRelation::MissingTarget
        );
    }

    #[test]
    fn state_snapshots_missing_from_history_yield_incomplete_provenance() {
        let root = temp_dir("state_snapshot_provenance_missing_history");
        let archive_dir = root.join("archive");
        let pointer_dir = root.join("pointer");
        let history_dir = root.join("history");
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
        write_pointer_metadata(
            &pointer_dir,
            activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME,
            &archive_dir,
            &snapshot_path,
        );
        fs::create_dir_all(&history_dir).expect("history dir");

        let report = build_report(&Config {
            state_archive_dir: archive_dir,
            snapshot_latest_pointer_dir: pointer_dir,
            history_dir: Some(history_dir),
            history_snapshot_paths: Vec::new(),
            pointer_name: activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateProvenanceVerdict::ArtifactStateProvenanceIncomplete
        );
        assert_eq!(report.archive_snapshots_missing_from_history_count, 1);
        assert_eq!(
            report.latest_pointer_relation,
            LatestStateSnapshotPointerRelation::MissingFromHistory
        );
    }

    #[test]
    fn malformed_pointer_metadata_or_snapshot_artifact_is_reported_as_invalid() {
        let root = temp_dir("state_snapshot_provenance_invalid");
        let archive_dir = root.join("archive");
        let pointer_dir = root.join("pointer");
        let history_dir = root.join("history");
        fs::create_dir_all(&archive_dir).expect("archive dir");
        fs::create_dir_all(&history_dir).expect("history dir");
        fs::create_dir_all(&pointer_dir).expect("pointer dir");
        fs::write(archive_dir.join("broken.json"), "{broken").expect("broken snapshot");
        fs::write(
            pointer_dir.join(format!(
                "{}.json",
                activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
            )),
            "{broken",
        )
        .expect("broken pointer");

        let report = build_report(&Config {
            state_archive_dir: archive_dir,
            snapshot_latest_pointer_dir: pointer_dir,
            history_dir: Some(history_dir),
            history_snapshot_paths: Vec::new(),
            pointer_name: activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateProvenanceVerdict::ArtifactStateProvenanceInvalidArtifactsPresent
        );
        assert!(report.invalid_artifact_count >= 2);
    }

    #[test]
    fn ambiguous_state_snapshot_lineage_is_not_treated_as_clean_green() {
        let root = temp_dir("state_snapshot_provenance_ambiguous");
        let archive_dir = root.join("archive");
        let pointer_dir = root.join("pointer");
        let history_dir = root.join("history");
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
        fs::copy(&snapshot_path, &history_snapshot_path).expect("copy history snapshot");
        write_pointer_metadata(
            &pointer_dir,
            activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME,
            &archive_dir,
            &snapshot_path,
        );

        let report = build_report(&Config {
            state_archive_dir: archive_dir,
            snapshot_latest_pointer_dir: pointer_dir,
            history_dir: Some(history_dir),
            history_snapshot_paths: Vec::new(),
            pointer_name: activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateProvenanceVerdict::ArtifactStateProvenanceAmbiguousLegacyState
        );
        assert_eq!(report.ambiguous_snapshot_count, 1);
        assert_eq!(
            report.latest_pointer_relation,
            LatestStateSnapshotPointerRelation::AmbiguousStateSnapshot
        );
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
