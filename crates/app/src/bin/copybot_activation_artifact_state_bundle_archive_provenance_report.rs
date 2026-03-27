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
#[path = "copybot_activation_artifact_state_bundle_publish_report.rs"]
mod activation_artifact_state_bundle_publish_report;
#[allow(dead_code)]
#[path = "copybot_activation_artifact_state_publish_report.rs"]
mod activation_artifact_state_publish_report;

const USAGE: &str = "usage: copybot_activation_artifact_state_bundle_archive_provenance_report --bundle-archive-dir <path> --state-archive-dir <path> [--bundle-latest-pointer-dir <path>] [--bundle-pointer-name <name>] [--snapshot-latest-pointer-dir <path>] [--snapshot-pointer-name <name>] [--history-dir <path>] [--history-snapshot <path>]... [--json]";

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
    bundle_archive_dir: PathBuf,
    bundle_latest_pointer_dir: Option<PathBuf>,
    bundle_pointer_name: String,
    state_archive_dir: PathBuf,
    snapshot_latest_pointer_dir: Option<PathBuf>,
    snapshot_pointer_name: String,
    history_dir: Option<PathBuf>,
    history_snapshot_paths: Vec<PathBuf>,
    json: bool,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ArtifactStateBundleArchiveProvenanceVerdict {
    ArtifactStateBundleArchiveProvenanceComplete,
    ArtifactStateBundleArchiveProvenanceIncomplete,
    ArtifactStateBundleArchiveProvenanceInvalidArtifactsPresent,
    ArtifactStateBundleArchiveProvenanceInconsistentLineage,
    ArtifactStateBundleArchiveProvenanceAmbiguousLegacyState,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LatestArchivedBundleRelation {
    NoPointerConfigured,
    NoPointerMetadata,
    InvalidMetadata,
    MissingTarget,
    MissingFromBundleArchive,
    MissingFromStateArchive,
    MissingFromHistory,
    BehindCurrentSnapshotPointer,
    BehindLatestStateArchive,
    BehindLatestHistory,
    NonGreenSnapshotBundle,
    AmbiguousSnapshotBundle,
    CoveredByArchiveAndCurrentSnapshotTruth,
}

#[derive(Debug, Clone, Serialize)]
struct ArtifactLineageIssue {
    surface: String,
    path: String,
    error: String,
}

#[derive(Debug, Clone)]
struct StateSnapshotSurfaceRecord {
    loaded: activation_artifact_state_publish_report::LoadedStateSnapshot,
    identity_key: String,
    canonical_path: PathBuf,
}

#[derive(Debug, Clone)]
struct ArchivedBundleRecord {
    summary: activation_artifact_state_bundle::StateSnapshotBundleSummary,
    identity_key: String,
    bundle_root: PathBuf,
    bundle_dir_name: String,
}

#[derive(Debug, Clone)]
struct CurrentSnapshotTruthRecord {
    identity_key: String,
    display_path: String,
    canonical_state_archive_path: Option<PathBuf>,
    snapshotted_at: DateTime<Utc>,
    state_verdict: String,
    state_reason: String,
    selected_review_generation_id: Option<String>,
    selected_latest_release_generation_id: Option<String>,
    ambiguous_legacy_count: usize,
    coherent_for_review_operations: bool,
    sources: BTreeSet<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ArchivedBundleCoverageSummary {
    bundle_path: String,
    bundle_dir_name: String,
    snapshotted_at: Option<DateTime<Utc>>,
    selected_snapshot_path: Option<String>,
    selected_snapshot_state_verdict: Option<String>,
    selected_snapshot_state_reason: Option<String>,
    selected_review_generation_id: Option<String>,
    selected_latest_release_generation_id: Option<String>,
    ambiguous_legacy_count: Option<usize>,
    coherent_for_review_operations: Option<bool>,
    matches_current_state_archive: bool,
    present_in_history: Option<bool>,
    selected_by_latest_bundle_pointer: bool,
}

#[derive(Debug, Clone, Serialize)]
struct CurrentSnapshotCoverageSummary {
    identity_key: String,
    snapshot_path: String,
    sources: Vec<String>,
    snapshotted_at: DateTime<Utc>,
    state_verdict: String,
    state_reason: String,
    selected_review_generation_id: Option<String>,
    selected_latest_release_generation_id: Option<String>,
    ambiguous_legacy_count: usize,
    coherent_for_review_operations: bool,
    archived_bundle_paths: Vec<String>,
    archived_bundle_count: usize,
    has_archived_bundle_coverage: bool,
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
struct SnapshotLatestPointerSummary {
    pointer_configured: bool,
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
    target_exists: bool,
    target_matches_identity: bool,
    present_in_state_archive: bool,
    present_in_history: Option<bool>,
    present_in_archived_bundles: bool,
    archived_bundle_paths: Vec<String>,
    matches_latest_state_archive: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
struct LatestArchivedBundlePointerSummary {
    pointer_name: String,
    pointer_verdict: String,
    pointer_reason: String,
    pointer_path: Option<String>,
    selected_bundle_path: Option<String>,
    selected_snapshot_path: Option<String>,
    selected_snapshot_file_name: Option<String>,
    selected_snapshotted_at: Option<DateTime<Utc>>,
    selected_snapshot_state_verdict: Option<String>,
    selected_snapshot_state_reason: Option<String>,
    selected_review_generation_id: Option<String>,
    selected_latest_release_generation_id: Option<String>,
    selected_ambiguous_legacy_count: Option<usize>,
    selected_coherent_for_review_operations: Option<bool>,
    target_exists: bool,
    target_matches_identity: bool,
    present_in_bundle_archive: bool,
    present_in_state_archive: bool,
    present_in_history: Option<bool>,
    matches_current_snapshot_pointer: Option<bool>,
    matches_latest_state_archive: Option<bool>,
    matches_latest_history: Option<bool>,
    relation: LatestArchivedBundleRelation,
    missing_paths: Vec<String>,
    inconsistencies: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ArtifactStateBundleArchiveProvenanceReport {
    mode: String,
    verdict: ArtifactStateBundleArchiveProvenanceVerdict,
    reason: String,
    bundle_archive_dir: String,
    bundle_latest_pointer_dir: Option<String>,
    bundle_pointer_name: String,
    state_archive_dir: String,
    snapshot_latest_pointer_dir: Option<String>,
    snapshot_pointer_name: String,
    history_dir: Option<String>,
    history_snapshot_paths: Vec<String>,
    archived_bundle_count: usize,
    valid_archived_bundle_count: usize,
    invalid_or_drifted_bundle_count: usize,
    state_archive_snapshot_count: usize,
    history_snapshot_count: usize,
    latest_archived_bundle_path: Option<String>,
    latest_state_snapshot_path: Option<String>,
    latest_history_snapshot_path: Option<String>,
    latest_bundle_pointer_present: bool,
    latest_bundle_pointer_selected_snapshot_has_current_lineage: Option<bool>,
    latest_bundle_pointer_selected_snapshot_has_archived_bundle_coverage: Option<bool>,
    latest_bundle_pointer_relation: LatestArchivedBundleRelation,
    latest_bundle_pointer_selected_snapshot_path: Option<String>,
    snapshot_latest_pointer_present: bool,
    snapshot_latest_pointer_selected_snapshot_has_archived_bundle_coverage: Option<bool>,
    current_snapshots_missing_archived_bundle_count: usize,
    archived_bundles_missing_state_archive_count: usize,
    archived_bundles_missing_history_count: usize,
    invalid_artifact_count: usize,
    invalid_artifacts: Vec<ArtifactLineageIssue>,
    ambiguous_archived_bundle_count: usize,
    non_green_archived_bundle_count: usize,
    archived_bundles: Vec<ArchivedBundleCoverageSummary>,
    current_snapshot_truths: Vec<CurrentSnapshotCoverageSummary>,
    current_snapshots_missing_archived_bundle: Vec<CurrentSnapshotCoverageSummary>,
    archived_bundles_missing_state_archive: Vec<BundleOnlyStateSnapshotSummary>,
    archived_bundles_missing_history: Vec<BundleOnlyStateSnapshotSummary>,
    latest_bundle_pointer: LatestArchivedBundlePointerSummary,
    snapshot_latest_pointer: SnapshotLatestPointerSummary,
    archived_state_bundle_provenance_only: bool,
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
    let mut bundle_archive_dir: Option<PathBuf> = None;
    let mut bundle_latest_pointer_dir: Option<PathBuf> = None;
    let mut bundle_pointer_name =
        activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME.to_string();
    let mut state_archive_dir: Option<PathBuf> = None;
    let mut snapshot_latest_pointer_dir: Option<PathBuf> = None;
    let mut snapshot_pointer_name =
        activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME.to_string();
    let mut history_dir: Option<PathBuf> = None;
    let mut history_snapshot_paths = Vec::new();
    let mut json = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--bundle-archive-dir" => {
                bundle_archive_dir = Some(PathBuf::from(parse_string_arg(
                    "--bundle-archive-dir",
                    args.next(),
                )?))
            }
            "--bundle-latest-pointer-dir" => {
                bundle_latest_pointer_dir = Some(PathBuf::from(parse_string_arg(
                    "--bundle-latest-pointer-dir",
                    args.next(),
                )?))
            }
            "--bundle-pointer-name" => {
                bundle_pointer_name =
                    parse_name(parse_string_arg("--bundle-pointer-name", args.next())?)?
            }
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
            "--snapshot-pointer-name" => {
                snapshot_pointer_name =
                    parse_name(parse_string_arg("--snapshot-pointer-name", args.next())?)?
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
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown arg `{other}`"),
        }
    }

    Ok(Some(Config {
        bundle_archive_dir: bundle_archive_dir
            .ok_or_else(|| anyhow!("--bundle-archive-dir is required"))?,
        bundle_latest_pointer_dir,
        bundle_pointer_name,
        state_archive_dir: state_archive_dir
            .ok_or_else(|| anyhow!("--state-archive-dir is required"))?,
        snapshot_latest_pointer_dir,
        snapshot_pointer_name,
        history_dir,
        history_snapshot_paths,
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

fn build_report(config: &Config) -> Result<ArtifactStateBundleArchiveProvenanceReport> {
    let archive_paths = collect_snapshot_paths(Some(&config.state_archive_dir), &[])?;
    let history_paths = collect_snapshot_paths(
        config.history_dir.as_deref(),
        &config.history_snapshot_paths,
    )?;
    let bundle_paths = activation_artifact_state_bundle::collect_state_snapshot_bundle_paths(
        Some(&config.bundle_archive_dir),
        &[],
    )?;

    let (archive_records, mut invalid_artifacts) =
        load_state_surface("state_archive", &archive_paths);
    let (history_records, invalid_history) = load_state_surface("history", &history_paths);
    invalid_artifacts.extend(invalid_history);
    let (bundle_records, invalid_bundles) = load_bundle_surface(&bundle_paths);
    invalid_artifacts.extend(invalid_bundles);

    let canonical_state_archive_dir = fs::canonicalize(&config.state_archive_dir).ok();
    let bundle_pointer_report = inspect_bundle_pointer(config, &mut invalid_artifacts);
    let bundle_pointer_metadata = inspect_bundle_pointer_metadata(config, &mut invalid_artifacts);
    let snapshot_pointer_report = inspect_snapshot_pointer(config, &mut invalid_artifacts);
    let snapshot_pointer_metadata =
        inspect_snapshot_pointer_metadata(config, &mut invalid_artifacts);

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
    let current_bundle_records = bundle_records
        .iter()
        .filter(|record| {
            bundle_matches_current_archive(
                record,
                canonical_state_archive_dir.as_deref(),
                &archive_records,
            )
        })
        .collect::<Vec<_>>();
    let current_bundle_map = current_bundle_records.iter().fold(
        BTreeMap::<String, Vec<&ArchivedBundleRecord>>::new(),
        |mut acc, record| {
            acc.entry(record.identity_key.clone())
                .or_default()
                .push(*record);
            acc
        },
    );

    let latest_state_archive = latest_state_record(&archive_records);
    let latest_history = latest_state_record(&history_records);
    let latest_archived_bundle = latest_bundle_record(&bundle_records);

    let snapshot_pointer_identity_key = snapshot_pointer_metadata
        .as_ref()
        .map(|loaded| identity_key_from_pointer_metadata(&loaded.metadata));
    let snapshot_pointer_canonical_path = snapshot_pointer_metadata.as_ref().and_then(|loaded| {
        fs::canonicalize(Path::new(&loaded.metadata.selected_snapshot_path)).ok()
    });
    let snapshot_pointer_present_in_archive =
        snapshot_pointer_canonical_path
            .as_ref()
            .is_some_and(|selected| {
                archive_records
                    .iter()
                    .any(|record| &record.canonical_path == selected)
            });
    let snapshot_pointer_present_in_history = if history_paths.is_empty() {
        None
    } else {
        Some(
            snapshot_pointer_identity_key
                .as_ref()
                .is_some_and(|identity| history_map.contains_key(identity)),
        )
    };
    let snapshot_pointer_bundle_paths = bundle_paths_for_selected_snapshot(
        &current_bundle_records,
        snapshot_pointer_identity_key.as_deref(),
        snapshot_pointer_canonical_path.as_deref(),
    );
    let snapshot_pointer_present_in_bundles = !snapshot_pointer_bundle_paths.is_empty();
    let snapshot_pointer_matches_latest_archive = latest_state_archive.map(|latest| {
        snapshot_pointer_canonical_path
            .as_ref()
            .is_some_and(|selected| selected == &latest.canonical_path)
    });

    let current_snapshot_truths = build_current_snapshot_truths(
        &archive_map,
        latest_state_archive,
        latest_history,
        snapshot_pointer_metadata.as_ref(),
        &current_bundle_records,
    );
    let current_snapshots_missing_archived_bundle = current_snapshot_truths
        .iter()
        .filter(|summary| !summary.has_archived_bundle_coverage)
        .cloned()
        .collect::<Vec<_>>();

    let archived_bundles = bundle_records
        .iter()
        .map(|record| {
            let present_in_history = if history_paths.is_empty() {
                None
            } else {
                Some(history_map.contains_key(&record.identity_key))
            };
            ArchivedBundleCoverageSummary {
                bundle_path: record.bundle_root.display().to_string(),
                bundle_dir_name: record.bundle_dir_name.clone(),
                snapshotted_at: record.summary.snapshotted_at,
                selected_snapshot_path: record.summary.selected_snapshot_path.clone(),
                selected_snapshot_state_verdict: record
                    .summary
                    .selected_snapshot_state_verdict
                    .clone(),
                selected_snapshot_state_reason: record
                    .summary
                    .selected_snapshot_state_reason
                    .clone(),
                selected_review_generation_id: record.summary.selected_review_generation_id.clone(),
                selected_latest_release_generation_id: record
                    .summary
                    .selected_latest_release_generation_id
                    .clone(),
                ambiguous_legacy_count: record.summary.ambiguous_legacy_count,
                coherent_for_review_operations: record.summary.coherent_for_review_operations,
                matches_current_state_archive: bundle_matches_current_archive(
                    record,
                    canonical_state_archive_dir.as_deref(),
                    &archive_records,
                ),
                present_in_history,
                selected_by_latest_bundle_pointer: bundle_pointer_metadata.as_ref().is_some_and(
                    |loaded| {
                        Path::new(&loaded.metadata.selected_bundle_path)
                            == record.bundle_root.as_path()
                    },
                ),
            }
        })
        .collect::<Vec<_>>();

    let archived_bundles_missing_state_archive = build_bundles_not_matching_current_archive(
        &bundle_records,
        canonical_state_archive_dir.as_deref(),
        &archive_records,
    );
    let archived_bundles_missing_history = if history_paths.is_empty() {
        Vec::new()
    } else {
        build_bundle_only_summaries(&current_bundle_map, &history_map)
    };

    let bundle_pointer_identity_key = bundle_pointer_metadata
        .as_ref()
        .map(|loaded| identity_key_from_bundle_pointer_metadata(&loaded.metadata));
    let bundle_pointer_bundle_canonical_path = bundle_pointer_metadata
        .as_ref()
        .and_then(|loaded| fs::canonicalize(Path::new(&loaded.metadata.selected_bundle_path)).ok());
    let bundle_pointer_snapshot_canonical_path =
        bundle_pointer_metadata.as_ref().and_then(|loaded| {
            fs::canonicalize(Path::new(&loaded.metadata.selected_snapshot_source_path)).ok()
        });
    let bundle_pointer_present_in_bundle_archive = bundle_pointer_bundle_canonical_path
        .as_ref()
        .is_some_and(|selected| {
            bundle_records
                .iter()
                .any(|record| &record.bundle_root == selected)
        });
    let bundle_pointer_present_in_state_archive =
        bundle_pointer_identity_key
            .as_ref()
            .is_some_and(|identity| {
                archive_map.contains_key(identity)
                    && bundle_pointer_snapshot_canonical_path.as_ref().is_some_and(
                        |selected_path| {
                            archive_records.iter().any(|record| {
                                record.identity_key == *identity
                                    && &record.canonical_path == selected_path
                            })
                        },
                    )
            })
            && bundle_pointer_metadata.as_ref().is_some_and(|loaded| {
                canonical_state_archive_dir
                    .as_deref()
                    .is_some_and(|archive_root| {
                        Path::new(&loaded.metadata.source_state_archive_dir) == archive_root
                    })
            });
    let bundle_pointer_present_in_history = if history_paths.is_empty() {
        None
    } else {
        Some(
            bundle_pointer_identity_key
                .as_ref()
                .is_some_and(|identity| history_map.contains_key(identity)),
        )
    };
    let bundle_pointer_matches_current_snapshot_pointer =
        if config.snapshot_latest_pointer_dir.is_some() {
            Some(
                bundle_pointer_identity_key
                    .as_ref()
                    .zip(snapshot_pointer_identity_key.as_ref())
                    .is_some_and(|(left, right)| left == right)
                    && bundle_pointer_snapshot_canonical_path
                        .as_ref()
                        .zip(snapshot_pointer_canonical_path.as_ref())
                        .is_some_and(|(left, right)| left == right),
            )
        } else {
            None
        };
    let bundle_pointer_matches_latest_state_archive = latest_state_archive.map(|latest| {
        bundle_pointer_identity_key
            .as_ref()
            .is_some_and(|identity| identity == &latest.identity_key)
            && bundle_pointer_snapshot_canonical_path
                .as_ref()
                .is_some_and(|selected| selected == &latest.canonical_path)
    });
    let bundle_pointer_matches_latest_history = if history_paths.is_empty() {
        None
    } else {
        latest_history.map(|latest| {
            bundle_pointer_identity_key
                .as_ref()
                .is_some_and(|identity| identity == &latest.identity_key)
        })
    };
    let latest_bundle_pointer_relation = determine_latest_bundle_pointer_relation(
        config.bundle_latest_pointer_dir.is_some(),
        &bundle_pointer_report,
        bundle_pointer_metadata.as_ref(),
        bundle_pointer_present_in_bundle_archive,
        bundle_pointer_present_in_state_archive,
        bundle_pointer_present_in_history,
        bundle_pointer_matches_current_snapshot_pointer,
        bundle_pointer_matches_latest_state_archive,
        bundle_pointer_matches_latest_history,
    );

    let latest_bundle_pointer = LatestArchivedBundlePointerSummary {
        pointer_name: config.bundle_pointer_name.clone(),
        pointer_verdict: serialize_enum(&bundle_pointer_report.verdict),
        pointer_reason: bundle_pointer_report.reason.clone(),
        pointer_path: bundle_pointer_report
            .latest_pointer_path
            .clone()
            .or_else(|| {
                bundle_pointer_metadata
                    .as_ref()
                    .map(|loaded| loaded.path.display().to_string())
            }),
        selected_bundle_path: bundle_pointer_report.persisted_bundle_path.clone(),
        selected_snapshot_path: bundle_pointer_report.selected_snapshot_path.clone(),
        selected_snapshot_file_name: bundle_pointer_report.selected_snapshot_file_name.clone(),
        selected_snapshotted_at: bundle_pointer_report.snapshotted_at,
        selected_snapshot_state_verdict: bundle_pointer_report
            .selected_snapshot_state_verdict
            .clone(),
        selected_snapshot_state_reason: bundle_pointer_report
            .selected_snapshot_state_reason
            .clone(),
        selected_review_generation_id: bundle_pointer_report.selected_review_generation_id.clone(),
        selected_latest_release_generation_id: bundle_pointer_report
            .selected_latest_release_generation_id
            .clone(),
        selected_ambiguous_legacy_count: bundle_pointer_report.ambiguous_legacy_count,
        selected_coherent_for_review_operations: bundle_pointer_report
            .coherent_for_review_operations,
        target_exists: bundle_pointer_report.latest_pointer_target_exists,
        target_matches_identity: bundle_pointer_report.latest_pointer_target_matches_identity,
        present_in_bundle_archive: bundle_pointer_present_in_bundle_archive,
        present_in_state_archive: bundle_pointer_present_in_state_archive,
        present_in_history: bundle_pointer_present_in_history,
        matches_current_snapshot_pointer: bundle_pointer_matches_current_snapshot_pointer,
        matches_latest_state_archive: bundle_pointer_matches_latest_state_archive,
        matches_latest_history: bundle_pointer_matches_latest_history,
        relation: latest_bundle_pointer_relation,
        missing_paths: bundle_pointer_report.missing_paths.clone(),
        inconsistencies: bundle_pointer_report.inconsistencies.clone(),
    };

    let snapshot_latest_pointer = SnapshotLatestPointerSummary {
        pointer_configured: config.snapshot_latest_pointer_dir.is_some(),
        pointer_verdict: serialize_enum(&snapshot_pointer_report.verdict),
        pointer_reason: snapshot_pointer_report.reason.clone(),
        pointer_path: snapshot_pointer_report
            .latest_pointer_path
            .clone()
            .or_else(|| {
                snapshot_pointer_metadata
                    .as_ref()
                    .map(|loaded| loaded.path.display().to_string())
            }),
        selected_snapshot_path: snapshot_pointer_report
            .persisted_state_snapshot_path
            .clone(),
        selected_snapshot_file_name: snapshot_pointer_report
            .persisted_state_snapshot_file_name
            .clone(),
        selected_snapshotted_at: snapshot_pointer_report.snapshotted_at,
        selected_state_verdict: snapshot_pointer_report.state_verdict.clone(),
        selected_state_reason: snapshot_pointer_report.state_reason.clone(),
        selected_review_generation_id: snapshot_pointer_report
            .selected_review_generation_id
            .clone(),
        selected_latest_release_generation_id: snapshot_pointer_report
            .selected_latest_release_generation_id
            .clone(),
        selected_ambiguous_legacy_count: snapshot_pointer_metadata
            .as_ref()
            .map(|loaded| loaded.metadata.ambiguous_legacy_count),
        selected_coherent_for_review_operations: snapshot_pointer_metadata
            .as_ref()
            .map(|loaded| loaded.metadata.coherent_for_review_operations),
        target_exists: snapshot_pointer_report.latest_pointer_target_exists,
        target_matches_identity: snapshot_pointer_report.latest_pointer_target_matches_identity,
        present_in_state_archive: snapshot_pointer_present_in_archive,
        present_in_history: snapshot_pointer_present_in_history,
        present_in_archived_bundles: snapshot_pointer_present_in_bundles,
        archived_bundle_paths: snapshot_pointer_bundle_paths.clone(),
        matches_latest_state_archive: snapshot_pointer_matches_latest_archive,
    };

    let ambiguous_archived_bundle_count = archived_bundles
        .iter()
        .filter(|bundle| {
            bundle.selected_snapshot_state_verdict.as_deref()
                == Some("artifact_state_ambiguous_legacy_state")
        })
        .count();
    let non_green_archived_bundle_count = archived_bundles
        .iter()
        .filter(|bundle| {
            bundle
                .selected_snapshot_state_verdict
                .as_deref()
                .is_some_and(is_hard_non_green_state_verdict)
        })
        .count();

    let latest_bundle_pointer_has_current_lineage =
        config.bundle_latest_pointer_dir.as_ref().map(|_| {
            latest_bundle_pointer.present_in_bundle_archive
                && latest_bundle_pointer.present_in_state_archive
                && latest_bundle_pointer.present_in_history.unwrap_or(true)
                && latest_bundle_pointer.target_exists
                && latest_bundle_pointer.target_matches_identity
        });
    let latest_bundle_pointer_has_archived_bundle_coverage = config
        .bundle_latest_pointer_dir
        .as_ref()
        .map(|_| latest_bundle_pointer.present_in_bundle_archive);
    let snapshot_latest_pointer_has_archived_bundle_coverage = config
        .snapshot_latest_pointer_dir
        .as_ref()
        .map(|_| snapshot_latest_pointer.present_in_archived_bundles);

    let bundle_pointer_broken = matches!(
        latest_bundle_pointer.relation,
        LatestArchivedBundleRelation::InvalidMetadata
            | LatestArchivedBundleRelation::MissingTarget
            | LatestArchivedBundleRelation::MissingFromBundleArchive
            | LatestArchivedBundleRelation::MissingFromStateArchive
    );
    let snapshot_pointer_broken = config.snapshot_latest_pointer_dir.is_some()
        && matches!(
            snapshot_pointer_report.verdict,
            activation_artifact_state_publish_report::ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotInvalidMetadata
                | activation_artifact_state_publish_report::ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotVerifyMissingTarget
        );
    let snapshot_pointer_missing_coverage = config.snapshot_latest_pointer_dir.is_some()
        && snapshot_latest_pointer.pointer_verdict == "artifact_state_snapshot_verify_ok"
        && !snapshot_latest_pointer.present_in_archived_bundles;
    let latest_archive_missing_coverage = latest_state_archive.is_some()
        && current_snapshot_truths.iter().any(|truth| {
            truth
                .sources
                .iter()
                .any(|source| source == "latest_state_archive")
                && !truth.has_archived_bundle_coverage
        });
    let latest_history_missing_coverage = !history_paths.is_empty()
        && latest_history.is_some()
        && current_snapshot_truths.iter().any(|truth| {
            truth
                .sources
                .iter()
                .any(|source| source == "latest_history")
                && !truth.has_archived_bundle_coverage
        });
    let stale_bundle_pointer = matches!(
        latest_bundle_pointer.relation,
        LatestArchivedBundleRelation::BehindCurrentSnapshotPointer
            | LatestArchivedBundleRelation::BehindLatestStateArchive
            | LatestArchivedBundleRelation::BehindLatestHistory
            | LatestArchivedBundleRelation::MissingFromHistory
    );
    let hard_non_green_current_truth = current_snapshot_truths
        .iter()
        .any(|truth| is_hard_non_green_state_verdict(&truth.state_verdict));
    let ambiguous_current_truth = current_snapshot_truths
        .iter()
        .any(|truth| truth.state_verdict == "artifact_state_ambiguous_legacy_state");

    let (verdict, reason) = if !invalid_artifacts.is_empty() {
        (
            ArtifactStateBundleArchiveProvenanceVerdict::ArtifactStateBundleArchiveProvenanceInvalidArtifactsPresent,
            format!(
                "archived state-bundle provenance contains {} invalid artifact(s); review invalid_artifacts before trusting archived-bundle lineage",
                invalid_artifacts.len()
            ),
        )
    } else if bundle_pointer_broken
        || snapshot_pointer_broken
        || !archived_bundles_missing_state_archive.is_empty()
    {
        let reason = if bundle_pointer_broken {
            "archived state-bundle provenance is inconsistent: the latest bundle pointer does not resolve cleanly against the archived bundle and current snapshot archive surfaces".to_string()
        } else if snapshot_pointer_broken {
            "archived state-bundle provenance is inconsistent: the current state snapshot latest pointer does not resolve cleanly against the persisted snapshot archive".to_string()
        } else {
            "archived state-bundle provenance is inconsistent: one or more archived bundles reference snapshots missing from the current state snapshot archive".to_string()
        };
        (
            ArtifactStateBundleArchiveProvenanceVerdict::ArtifactStateBundleArchiveProvenanceInconsistentLineage,
            reason,
        )
    } else if archive_records.is_empty() {
        (
            ArtifactStateBundleArchiveProvenanceVerdict::ArtifactStateBundleArchiveProvenanceIncomplete,
            "no persisted state snapshots were found in the current state snapshot archive"
                .to_string(),
        )
    } else if snapshot_pointer_missing_coverage
        || latest_archive_missing_coverage
        || latest_history_missing_coverage
        || !current_snapshots_missing_archived_bundle.is_empty()
        || stale_bundle_pointer
        || !archived_bundles_missing_history.is_empty()
        || non_green_archived_bundle_count > 0
        || hard_non_green_current_truth
        || latest_bundle_pointer.relation == LatestArchivedBundleRelation::NonGreenSnapshotBundle
    {
        let reason = if snapshot_pointer_missing_coverage {
            "archived state-bundle provenance is incomplete: the current state snapshot latest pointer selects a snapshot without archived-bundle coverage".to_string()
        } else if latest_archive_missing_coverage {
            "archived state-bundle provenance is incomplete: the latest persisted state snapshot has no archived-bundle coverage".to_string()
        } else if latest_history_missing_coverage {
            "archived state-bundle provenance is incomplete: the latest history snapshot has no archived-bundle coverage".to_string()
        } else if stale_bundle_pointer {
            "archived state-bundle provenance is incomplete: the latest bundle pointer is behind current snapshot truth or missing history coverage".to_string()
        } else if non_green_archived_bundle_count > 0 || hard_non_green_current_truth {
            "archived state-bundle provenance is incomplete: archived bundles or current snapshot truth remain non-green".to_string()
        } else {
            "archived state-bundle provenance is incomplete: current snapshot truth is only partially covered by deterministic archived bundles".to_string()
        };
        (
            ArtifactStateBundleArchiveProvenanceVerdict::ArtifactStateBundleArchiveProvenanceIncomplete,
            reason,
        )
    } else if ambiguous_archived_bundle_count > 0
        || ambiguous_current_truth
        || latest_bundle_pointer.relation == LatestArchivedBundleRelation::AmbiguousSnapshotBundle
    {
        (
            ArtifactStateBundleArchiveProvenanceVerdict::ArtifactStateBundleArchiveProvenanceAmbiguousLegacyState,
            "archived state-bundle provenance is structurally present, but it depends on ambiguous legacy snapshot state and cannot be treated as clean green provenance".to_string(),
        )
    } else {
        (
            ArtifactStateBundleArchiveProvenanceVerdict::ArtifactStateBundleArchiveProvenanceComplete,
            "deterministic archived bundles, latest bundle pointer, and current state snapshot truth are aligned".to_string(),
        )
    };

    Ok(ArtifactStateBundleArchiveProvenanceReport {
        mode: "artifact_state_bundle_archive_provenance_report".to_string(),
        verdict,
        reason,
        bundle_archive_dir: config.bundle_archive_dir.display().to_string(),
        bundle_latest_pointer_dir: config
            .bundle_latest_pointer_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        bundle_pointer_name: config.bundle_pointer_name.clone(),
        state_archive_dir: config.state_archive_dir.display().to_string(),
        snapshot_latest_pointer_dir: config
            .snapshot_latest_pointer_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        snapshot_pointer_name: config.snapshot_pointer_name.clone(),
        history_dir: config.history_dir.as_ref().map(|path| path.display().to_string()),
        history_snapshot_paths: config
            .history_snapshot_paths
            .iter()
            .map(|path| path.display().to_string())
            .collect(),
        archived_bundle_count: bundle_paths.len(),
        valid_archived_bundle_count: bundle_records.len(),
        invalid_or_drifted_bundle_count: invalid_artifacts
            .iter()
            .filter(|issue| issue.surface == "archived_bundle")
            .count(),
        state_archive_snapshot_count: archive_records.len(),
        history_snapshot_count: history_records.len(),
        latest_archived_bundle_path: latest_archived_bundle
            .map(|record| record.bundle_root.display().to_string()),
        latest_state_snapshot_path: latest_state_archive
            .map(|record| record.loaded.path.display().to_string()),
        latest_history_snapshot_path: latest_history
            .map(|record| record.loaded.path.display().to_string()),
        latest_bundle_pointer_present: latest_bundle_pointer.pointer_path.is_some(),
        latest_bundle_pointer_selected_snapshot_has_current_lineage:
            latest_bundle_pointer_has_current_lineage,
        latest_bundle_pointer_selected_snapshot_has_archived_bundle_coverage:
            latest_bundle_pointer_has_archived_bundle_coverage,
        latest_bundle_pointer_relation: latest_bundle_pointer.relation,
        latest_bundle_pointer_selected_snapshot_path: latest_bundle_pointer
            .selected_snapshot_path
            .clone(),
        snapshot_latest_pointer_present: snapshot_latest_pointer.pointer_path.is_some(),
        snapshot_latest_pointer_selected_snapshot_has_archived_bundle_coverage:
            snapshot_latest_pointer_has_archived_bundle_coverage,
        current_snapshots_missing_archived_bundle_count: current_snapshots_missing_archived_bundle
            .len(),
        archived_bundles_missing_state_archive_count: archived_bundles_missing_state_archive.len(),
        archived_bundles_missing_history_count: archived_bundles_missing_history.len(),
        invalid_artifact_count: invalid_artifacts.len(),
        invalid_artifacts,
        ambiguous_archived_bundle_count,
        non_green_archived_bundle_count,
        archived_bundles,
        current_snapshot_truths: current_snapshot_truths.clone(),
        current_snapshots_missing_archived_bundle,
        archived_bundles_missing_state_archive,
        archived_bundles_missing_history,
        latest_bundle_pointer,
        snapshot_latest_pointer,
        archived_state_bundle_provenance_only: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary:
            "Archived state-bundle provenance only audits deterministic archived bundles, latest bundle pointer metadata, and current persisted state-snapshot lineage. It does not authorize activation, does not enable execution, and does not override the Stage 3 production gate."
                .to_string(),
    })
}

fn inspect_bundle_pointer(
    config: &Config,
    invalid_artifacts: &mut Vec<ArtifactLineageIssue>,
) -> activation_artifact_state_bundle_publish_report::ArtifactStateBundlePublishReport {
    if let Some(pointer_dir) = config.bundle_latest_pointer_dir.as_ref() {
        match activation_artifact_state_bundle_publish_report::inspect_latest_bundle_pointer_report(
            &config.bundle_archive_dir,
            pointer_dir,
            &config.bundle_pointer_name,
            true,
        ) {
            Ok(report) => report,
            Err(error) => {
                invalid_artifacts.push(ArtifactLineageIssue {
                    surface: "bundle_latest_pointer".to_string(),
                    path: pointer_dir
                        .join(format!("{}.json", config.bundle_pointer_name))
                        .display()
                        .to_string(),
                    error: format!("{error:#}"),
                });
                synthetic_bundle_pointer_report(config, pointer_dir)
            }
        }
    } else {
        synthetic_bundle_pointer_report_without_pointer(config)
    }
}

fn inspect_bundle_pointer_metadata(
    config: &Config,
    invalid_artifacts: &mut Vec<ArtifactLineageIssue>,
) -> Option<activation_artifact_state_bundle_publish_report::LoadedStateBundleLatestPointerMetadata>
{
    config.bundle_latest_pointer_dir.as_ref().and_then(|pointer_dir| {
        match activation_artifact_state_bundle_publish_report::inspect_latest_bundle_pointer_metadata(
            pointer_dir,
            &config.bundle_pointer_name,
        ) {
            Ok(metadata) => metadata,
            Err(error) => {
                invalid_artifacts.push(ArtifactLineageIssue {
                    surface: "bundle_latest_pointer".to_string(),
                    path: pointer_dir
                        .join(format!("{}.json", config.bundle_pointer_name))
                        .display()
                        .to_string(),
                    error: format!("{error:#}"),
                });
                None
            }
        }
    })
}

fn inspect_snapshot_pointer(
    config: &Config,
    invalid_artifacts: &mut Vec<ArtifactLineageIssue>,
) -> activation_artifact_state_publish_report::ArtifactStateSnapshotPublishReport {
    if let Some(pointer_dir) = config.snapshot_latest_pointer_dir.as_ref() {
        match activation_artifact_state_publish_report::inspect_latest_pointer_report(
            &config.state_archive_dir,
            pointer_dir,
            &config.snapshot_pointer_name,
            true,
        ) {
            Ok(report) => report,
            Err(error) => {
                invalid_artifacts.push(ArtifactLineageIssue {
                    surface: "snapshot_latest_pointer".to_string(),
                    path: pointer_dir
                        .join(format!("{}.json", config.snapshot_pointer_name))
                        .display()
                        .to_string(),
                    error: format!("{error:#}"),
                });
                synthetic_snapshot_pointer_report(config, pointer_dir)
            }
        }
    } else {
        synthetic_snapshot_pointer_report_without_pointer(config)
    }
}

fn inspect_snapshot_pointer_metadata(
    config: &Config,
    invalid_artifacts: &mut Vec<ArtifactLineageIssue>,
) -> Option<activation_artifact_state_publish_report::LoadedStateLatestPointerMetadata> {
    config
        .snapshot_latest_pointer_dir
        .as_ref()
        .and_then(|pointer_dir| {
            match activation_artifact_state_publish_report::inspect_latest_pointer_metadata(
                pointer_dir,
                &config.snapshot_pointer_name,
            ) {
                Ok(metadata) => metadata,
                Err(error) => {
                    invalid_artifacts.push(ArtifactLineageIssue {
                        surface: "snapshot_latest_pointer".to_string(),
                        path: pointer_dir
                            .join(format!("{}.json", config.snapshot_pointer_name))
                            .display()
                            .to_string(),
                        error: format!("{error:#}"),
                    });
                    None
                }
            }
        })
}

fn synthetic_bundle_pointer_report(
    config: &Config,
    pointer_dir: &Path,
) -> activation_artifact_state_bundle_publish_report::ArtifactStateBundlePublishReport {
    activation_artifact_state_bundle_publish_report::ArtifactStateBundlePublishReport {
        mode: "verify_latest".to_string(),
        verdict:
            activation_artifact_state_bundle_publish_report::ArtifactStateBundleReportVerdict::ArtifactStateBundleReportInvalidMetadata,
        reason: "latest archived bundle pointer inspection failed operationally".to_string(),
        state_archive_dir: None,
        bundle_archive_dir: config.bundle_archive_dir.display().to_string(),
        persisted_bundle_path: None,
        persisted_bundle_exists: false,
        persisted_bundle_dir_name: None,
        bundle_integrity_verdict: None,
        bundle_integrity_reason: None,
        source_state_archive_dir: None,
        selected_snapshot_path: None,
        selected_snapshot_file_name: None,
        snapshotted_at: None,
        selected_snapshot_state_verdict: None,
        selected_snapshot_state_reason: None,
        selected_review_generation_id: None,
        selected_latest_release_generation_id: None,
        ambiguous_legacy_count: None,
        coherent_for_review_operations: None,
        latest_pointer_attempted: false,
        latest_pointer_updated: false,
        latest_pointer_dir: Some(pointer_dir.display().to_string()),
        latest_pointer_name: config.bundle_pointer_name.clone(),
        latest_pointer_path: Some(
            pointer_dir
                .join(format!("{}.json", config.bundle_pointer_name))
                .display()
                .to_string(),
        ),
        latest_pointer_source_bundle_archive_dir: None,
        latest_pointer_pointed_at: None,
        latest_pointer_exists: false,
        latest_pointer_overwrite_used: false,
        latest_pointer_target_exists: false,
        latest_pointer_target_matches_identity: false,
        verification_attempted: true,
        missing_paths: Vec::new(),
        inconsistencies: Vec::new(),
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary:
            "Archived state-bundle provenance only audits archived bundle lineage. It does not authorize activation and does not override the Stage 3 production gate."
                .to_string(),
    }
}

fn synthetic_bundle_pointer_report_without_pointer(
    config: &Config,
) -> activation_artifact_state_bundle_publish_report::ArtifactStateBundlePublishReport {
    activation_artifact_state_bundle_publish_report::ArtifactStateBundlePublishReport {
        mode: "verify_latest".to_string(),
        verdict:
            activation_artifact_state_bundle_publish_report::ArtifactStateBundleReportVerdict::ArtifactStateBundleReportVerifyMissingTarget,
        reason: "no archived bundle latest pointer directory was provided".to_string(),
        state_archive_dir: None,
        bundle_archive_dir: config.bundle_archive_dir.display().to_string(),
        persisted_bundle_path: None,
        persisted_bundle_exists: false,
        persisted_bundle_dir_name: None,
        bundle_integrity_verdict: None,
        bundle_integrity_reason: None,
        source_state_archive_dir: None,
        selected_snapshot_path: None,
        selected_snapshot_file_name: None,
        snapshotted_at: None,
        selected_snapshot_state_verdict: None,
        selected_snapshot_state_reason: None,
        selected_review_generation_id: None,
        selected_latest_release_generation_id: None,
        ambiguous_legacy_count: None,
        coherent_for_review_operations: None,
        latest_pointer_attempted: false,
        latest_pointer_updated: false,
        latest_pointer_dir: None,
        latest_pointer_name: config.bundle_pointer_name.clone(),
        latest_pointer_path: None,
        latest_pointer_source_bundle_archive_dir: None,
        latest_pointer_pointed_at: None,
        latest_pointer_exists: false,
        latest_pointer_overwrite_used: false,
        latest_pointer_target_exists: false,
        latest_pointer_target_matches_identity: false,
        verification_attempted: false,
        missing_paths: Vec::new(),
        inconsistencies: Vec::new(),
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary:
            "Archived state-bundle provenance only audits archived bundle lineage. It does not authorize activation and does not override the Stage 3 production gate."
                .to_string(),
    }
}

fn synthetic_snapshot_pointer_report(
    config: &Config,
    pointer_dir: &Path,
) -> activation_artifact_state_publish_report::ArtifactStateSnapshotPublishReport {
    activation_artifact_state_publish_report::ArtifactStateSnapshotPublishReport {
        mode: "verify_latest".to_string(),
        verdict:
            activation_artifact_state_publish_report::ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotInvalidMetadata,
        reason: "current state snapshot latest pointer inspection failed operationally".to_string(),
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
        latest_pointer_name: config.snapshot_pointer_name.clone(),
        latest_pointer_path: Some(
            pointer_dir
                .join(format!("{}.json", config.snapshot_pointer_name))
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
            "Archived state-bundle provenance only audits persisted snapshot lineage. It does not authorize activation and does not override the Stage 3 production gate."
                .to_string(),
    }
}

fn synthetic_snapshot_pointer_report_without_pointer(
    config: &Config,
) -> activation_artifact_state_publish_report::ArtifactStateSnapshotPublishReport {
    activation_artifact_state_publish_report::ArtifactStateSnapshotPublishReport {
        mode: "verify_latest".to_string(),
        verdict:
            activation_artifact_state_publish_report::ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotVerifyMissingTarget,
        reason: "no current state snapshot latest pointer directory was provided".to_string(),
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
        latest_pointer_name: config.snapshot_pointer_name.clone(),
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
            "Archived state-bundle provenance only audits persisted snapshot lineage. It does not authorize activation and does not override the Stage 3 production gate."
                .to_string(),
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
) -> (Vec<StateSnapshotSurfaceRecord>, Vec<ArtifactLineageIssue>) {
    let mut valid = Vec::new();
    let mut invalid = Vec::new();
    for path in paths {
        match activation_artifact_state_publish_report::inspect_state_snapshot_artifact(path) {
            Ok(loaded) => valid.push(StateSnapshotSurfaceRecord {
                identity_key: identity_key_from_snapshot(&loaded.artifact),
                canonical_path: loaded.canonical_path.clone(),
                loaded,
            }),
            Err(error) => invalid.push(ArtifactLineageIssue {
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
) -> (Vec<ArchivedBundleRecord>, Vec<ArtifactLineageIssue>) {
    let mut valid = Vec::new();
    let mut invalid = Vec::new();
    for path in paths {
        match activation_artifact_state_bundle::inspect_state_snapshot_bundle(path) {
            Ok(summary) if summary.verdict == "artifact_state_bundle_verified" => {
                let Some(snapshotted_at) = summary.snapshotted_at else {
                    invalid.push(ArtifactLineageIssue {
                        surface: "archived_bundle".to_string(),
                        path: summary.bundle_path,
                        error: "verified archived bundle summary is missing snapshotted_at"
                            .to_string(),
                    });
                    continue;
                };
                let Some(state_verdict) = summary.selected_snapshot_state_verdict.as_deref() else {
                    invalid.push(ArtifactLineageIssue {
                        surface: "archived_bundle".to_string(),
                        path: summary.bundle_path,
                        error:
                            "verified archived bundle summary is missing selected_snapshot_state_verdict"
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
                let bundle_root = fs::canonicalize(Path::new(&summary.bundle_path))
                    .unwrap_or_else(|_| PathBuf::from(&summary.bundle_path));
                let bundle_dir_name = bundle_root
                    .file_name()
                    .map(|value| value.to_string_lossy().into_owned())
                    .unwrap_or_else(|| summary.bundle_path.clone());
                valid.push(ArchivedBundleRecord {
                    summary,
                    identity_key,
                    bundle_root,
                    bundle_dir_name,
                });
            }
            Ok(summary) => invalid.push(ArtifactLineageIssue {
                surface: "archived_bundle".to_string(),
                path: summary.bundle_path,
                error: format!(
                    "bundle verification returned `{}`: {}",
                    summary.verdict, summary.reason
                ),
            }),
            Err(error) => invalid.push(ArtifactLineageIssue {
                surface: "archived_bundle".to_string(),
                path: path.display().to_string(),
                error: format!("{error:#}"),
            }),
        }
    }
    (valid, invalid)
}

fn build_current_snapshot_truths(
    archive_map: &BTreeMap<String, Vec<&StateSnapshotSurfaceRecord>>,
    latest_state_archive: Option<&StateSnapshotSurfaceRecord>,
    latest_history: Option<&StateSnapshotSurfaceRecord>,
    snapshot_pointer_metadata: Option<
        &activation_artifact_state_publish_report::LoadedStateLatestPointerMetadata,
    >,
    bundle_records: &[&ArchivedBundleRecord],
) -> Vec<CurrentSnapshotCoverageSummary> {
    let mut truths = BTreeMap::<String, CurrentSnapshotTruthRecord>::new();

    if let Some(latest) = latest_state_archive {
        insert_current_truth(
            &mut truths,
            CurrentSnapshotTruthRecord {
                identity_key: latest.identity_key.clone(),
                display_path: latest.canonical_path.display().to_string(),
                canonical_state_archive_path: Some(latest.canonical_path.clone()),
                snapshotted_at: latest.loaded.artifact.snapshotted_at,
                state_verdict: latest.loaded.artifact.state_verdict.clone(),
                state_reason: latest.loaded.artifact.state_reason.clone(),
                selected_review_generation_id: latest
                    .loaded
                    .artifact
                    .selected_review_generation_id
                    .clone(),
                selected_latest_release_generation_id: latest
                    .loaded
                    .artifact
                    .selected_latest_release_generation_id
                    .clone(),
                ambiguous_legacy_count: latest.loaded.artifact.ambiguous_legacy_count,
                coherent_for_review_operations: latest
                    .loaded
                    .artifact
                    .coherent_for_review_operations,
                sources: BTreeSet::from(["latest_state_archive".to_string()]),
            },
        );
    }

    if let Some(latest) = latest_history {
        let archive_path = archive_map
            .get(&latest.identity_key)
            .and_then(|records| records.first().map(|record| record.canonical_path.clone()));
        insert_current_truth(
            &mut truths,
            CurrentSnapshotTruthRecord {
                identity_key: latest.identity_key.clone(),
                display_path: archive_path
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| latest.loaded.path.display().to_string()),
                canonical_state_archive_path: archive_path,
                snapshotted_at: latest.loaded.artifact.snapshotted_at,
                state_verdict: latest.loaded.artifact.state_verdict.clone(),
                state_reason: latest.loaded.artifact.state_reason.clone(),
                selected_review_generation_id: latest
                    .loaded
                    .artifact
                    .selected_review_generation_id
                    .clone(),
                selected_latest_release_generation_id: latest
                    .loaded
                    .artifact
                    .selected_latest_release_generation_id
                    .clone(),
                ambiguous_legacy_count: latest.loaded.artifact.ambiguous_legacy_count,
                coherent_for_review_operations: latest
                    .loaded
                    .artifact
                    .coherent_for_review_operations,
                sources: BTreeSet::from(["latest_history".to_string()]),
            },
        );
    }

    if let Some(pointer) = snapshot_pointer_metadata {
        let canonical_path =
            fs::canonicalize(Path::new(&pointer.metadata.selected_snapshot_path)).ok();
        let archive_path = canonical_path.as_ref().and_then(|path| {
            archive_map.iter().find_map(|(_, records)| {
                records
                    .iter()
                    .find(|record| &record.canonical_path == path)
                    .map(|record| record.canonical_path.clone())
            })
        });
        insert_current_truth(
            &mut truths,
            CurrentSnapshotTruthRecord {
                identity_key: identity_key_from_pointer_metadata(&pointer.metadata),
                display_path: pointer.metadata.selected_snapshot_path.clone(),
                canonical_state_archive_path: archive_path.or(canonical_path),
                snapshotted_at: pointer.metadata.snapshotted_at,
                state_verdict: pointer.metadata.snapshot_verdict.clone(),
                state_reason: pointer.metadata.snapshot_reason.clone(),
                selected_review_generation_id: pointer
                    .metadata
                    .selected_review_generation_id
                    .clone(),
                selected_latest_release_generation_id: pointer
                    .metadata
                    .selected_latest_release_generation_id
                    .clone(),
                ambiguous_legacy_count: pointer.metadata.ambiguous_legacy_count,
                coherent_for_review_operations: pointer.metadata.coherent_for_review_operations,
                sources: BTreeSet::from(["snapshot_latest_pointer".to_string()]),
            },
        );
    }

    let mut summaries = truths
        .into_values()
        .map(|truth| {
            let bundle_paths = bundle_records
                .iter()
                .filter(|record| bundle_matches_truth(record, &truth))
                .map(|record| record.bundle_root.display().to_string())
                .collect::<Vec<_>>();
            CurrentSnapshotCoverageSummary {
                identity_key: truth.identity_key,
                snapshot_path: truth.display_path,
                sources: truth.sources.into_iter().collect(),
                snapshotted_at: truth.snapshotted_at,
                state_verdict: truth.state_verdict,
                state_reason: truth.state_reason,
                selected_review_generation_id: truth.selected_review_generation_id,
                selected_latest_release_generation_id: truth.selected_latest_release_generation_id,
                ambiguous_legacy_count: truth.ambiguous_legacy_count,
                coherent_for_review_operations: truth.coherent_for_review_operations,
                archived_bundle_count: bundle_paths.len(),
                has_archived_bundle_coverage: !bundle_paths.is_empty(),
                archived_bundle_paths: bundle_paths,
            }
        })
        .collect::<Vec<_>>();
    summaries.sort_by(|left, right| {
        right
            .snapshotted_at
            .cmp(&left.snapshotted_at)
            .then_with(|| left.snapshot_path.cmp(&right.snapshot_path))
    });
    summaries
}

fn insert_current_truth(
    truths: &mut BTreeMap<String, CurrentSnapshotTruthRecord>,
    candidate: CurrentSnapshotTruthRecord,
) {
    let key = candidate
        .canonical_state_archive_path
        .as_ref()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| format!("{}|{}", candidate.identity_key, candidate.display_path));
    if let Some(existing) = truths.get_mut(&key) {
        existing.sources.extend(candidate.sources);
        if existing.canonical_state_archive_path.is_none() {
            existing.canonical_state_archive_path = candidate.canonical_state_archive_path;
        }
    } else {
        truths.insert(key, candidate);
    }
}

fn bundle_matches_truth(
    record: &&ArchivedBundleRecord,
    truth: &CurrentSnapshotTruthRecord,
) -> bool {
    if record.identity_key != truth.identity_key {
        return false;
    }
    match truth.canonical_state_archive_path.as_ref() {
        Some(canonical_path) => record
            .summary
            .selected_snapshot_path
            .as_deref()
            .is_some_and(|path| Path::new(path) == canonical_path),
        None => true,
    }
}

fn bundle_matches_current_archive(
    bundle: &ArchivedBundleRecord,
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
    bundle_records: &[&ArchivedBundleRecord],
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
            .map(|record| record.bundle_root.display().to_string())
            .collect(),
        _ => Vec::new(),
    }
}

fn latest_state_record(
    records: &[StateSnapshotSurfaceRecord],
) -> Option<&StateSnapshotSurfaceRecord> {
    records.iter().max_by(|left, right| {
        left.loaded
            .artifact
            .snapshotted_at
            .cmp(&right.loaded.artifact.snapshotted_at)
            .then_with(|| left.canonical_path.cmp(&right.canonical_path))
    })
}

fn latest_bundle_record(records: &[ArchivedBundleRecord]) -> Option<&ArchivedBundleRecord> {
    records.iter().max_by(|left, right| {
        left.bundle_dir_name
            .cmp(&right.bundle_dir_name)
            .then_with(|| left.bundle_root.cmp(&right.bundle_root))
    })
}

fn build_bundle_only_summaries(
    bundle_map: &BTreeMap<String, Vec<&ArchivedBundleRecord>>,
    comparison_map: &BTreeMap<String, Vec<&StateSnapshotSurfaceRecord>>,
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
                    .map(|record| record.bundle_root.display().to_string())
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

fn build_bundles_not_matching_current_archive(
    bundle_records: &[ArchivedBundleRecord],
    canonical_state_archive_dir: Option<&Path>,
    archive_records: &[StateSnapshotSurfaceRecord],
) -> Vec<BundleOnlyStateSnapshotSummary> {
    let mut grouped = BTreeMap::<String, Vec<&ArchivedBundleRecord>>::new();
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
                    .map(|record| record.bundle_root.display().to_string())
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

fn determine_latest_bundle_pointer_relation(
    pointer_configured: bool,
    pointer_report: &activation_artifact_state_bundle_publish_report::ArtifactStateBundlePublishReport,
    loaded_pointer_metadata: Option<
        &activation_artifact_state_bundle_publish_report::LoadedStateBundleLatestPointerMetadata,
    >,
    present_in_bundle_archive: bool,
    present_in_state_archive: bool,
    present_in_history: Option<bool>,
    matches_current_snapshot_pointer: Option<bool>,
    matches_latest_state_archive: Option<bool>,
    matches_latest_history: Option<bool>,
) -> LatestArchivedBundleRelation {
    use activation_artifact_state_bundle_publish_report::ArtifactStateBundleReportVerdict as PointerVerdict;

    if !pointer_configured {
        return LatestArchivedBundleRelation::NoPointerConfigured;
    }
    if loaded_pointer_metadata.is_none()
        && pointer_report.verdict == PointerVerdict::ArtifactStateBundleReportVerifyMissingTarget
    {
        return LatestArchivedBundleRelation::NoPointerMetadata;
    }
    if pointer_report.verdict == PointerVerdict::ArtifactStateBundleReportInvalidMetadata {
        return LatestArchivedBundleRelation::InvalidMetadata;
    }
    if pointer_report.verdict == PointerVerdict::ArtifactStateBundleReportVerifyMissingTarget {
        return LatestArchivedBundleRelation::MissingTarget;
    }
    if !present_in_bundle_archive {
        return LatestArchivedBundleRelation::MissingFromBundleArchive;
    }
    if !present_in_state_archive {
        return LatestArchivedBundleRelation::MissingFromStateArchive;
    }
    if present_in_history == Some(false) {
        return LatestArchivedBundleRelation::MissingFromHistory;
    }
    if loaded_pointer_metadata.is_some_and(|loaded| {
        is_hard_non_green_state_verdict(&loaded.metadata.snapshot_state_verdict)
    }) {
        return LatestArchivedBundleRelation::NonGreenSnapshotBundle;
    }
    if loaded_pointer_metadata.is_some_and(|loaded| {
        loaded.metadata.snapshot_state_verdict == "artifact_state_ambiguous_legacy_state"
    }) {
        return LatestArchivedBundleRelation::AmbiguousSnapshotBundle;
    }
    if matches_current_snapshot_pointer == Some(false) {
        return LatestArchivedBundleRelation::BehindCurrentSnapshotPointer;
    }
    if matches_latest_state_archive == Some(false) {
        return LatestArchivedBundleRelation::BehindLatestStateArchive;
    }
    if matches_latest_history == Some(false) {
        return LatestArchivedBundleRelation::BehindLatestHistory;
    }
    LatestArchivedBundleRelation::CoveredByArchiveAndCurrentSnapshotTruth
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

fn identity_key_from_bundle_pointer_metadata(
    metadata: &activation_artifact_state_bundle_publish_report::StateBundleLatestPointerMetadata,
) -> String {
    identity_key_from_values(
        metadata.snapshotted_at,
        &metadata.snapshot_state_verdict,
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

fn render_human(report: &ArtifactStateBundleArchiveProvenanceReport) -> String {
    [
        "event=copybot_activation_artifact_state_bundle_archive_provenance_report".to_string(),
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("archived_bundle_count={}", report.archived_bundle_count),
        format!(
            "valid_archived_bundle_count={}",
            report.valid_archived_bundle_count
        ),
        format!(
            "invalid_or_drifted_bundle_count={}",
            report.invalid_or_drifted_bundle_count
        ),
        format!(
            "state_archive_snapshot_count={}",
            report.state_archive_snapshot_count
        ),
        format!("history_snapshot_count={}", report.history_snapshot_count),
        format!(
            "latest_bundle_pointer_relation={}",
            serialize_enum(&report.latest_bundle_pointer_relation)
        ),
        format!(
            "latest_bundle_pointer_selected_snapshot_path={}",
            report
                .latest_bundle_pointer_selected_snapshot_path
                .clone()
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!(
            "latest_bundle_pointer_selected_snapshot_has_current_lineage={}",
            report
                .latest_bundle_pointer_selected_snapshot_has_current_lineage
                .map(|value| value.to_string())
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!(
            "latest_bundle_pointer_selected_snapshot_has_archived_bundle_coverage={}",
            report
                .latest_bundle_pointer_selected_snapshot_has_archived_bundle_coverage
                .map(|value| value.to_string())
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!(
            "snapshot_latest_pointer_selected_snapshot_has_archived_bundle_coverage={}",
            report
                .snapshot_latest_pointer_selected_snapshot_has_archived_bundle_coverage
                .map(|value| value.to_string())
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!(
            "current_snapshots_missing_archived_bundle_count={}",
            report.current_snapshots_missing_archived_bundle_count
        ),
        format!(
            "archived_bundles_missing_state_archive_count={}",
            report.archived_bundles_missing_state_archive_count
        ),
        format!(
            "archived_bundles_missing_history_count={}",
            report.archived_bundles_missing_history_count
        ),
        format!(
            "ambiguous_archived_bundle_count={}",
            report.ambiguous_archived_bundle_count
        ),
        format!(
            "non_green_archived_bundle_count={}",
            report.non_green_archived_bundle_count
        ),
        format!("invalid_artifact_count={}", report.invalid_artifact_count),
        format!(
            "archived_state_bundle_provenance_only={}",
            report.archived_state_bundle_provenance_only
        ),
        format!("execution_untouched={}", report.execution_untouched),
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
    fn complete_lineage_across_bundle_archive_pointer_and_current_snapshots_yields_green() {
        let root = temp_dir("state_bundle_archive_provenance_complete");
        let state_archive_dir = root.join("state_archive");
        let history_dir = root.join("history");
        let bundle_archive_dir = root.join("bundle_archive");
        let bundle_pointer_dir = root.join("bundle_pointer");
        let snapshot_pointer_dir = root.join("snapshot_pointer");
        let snapshot_path = state_archive_dir
            .join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        let history_snapshot_path = history_dir
            .join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent__history.json");
        write_snapshot(
            &snapshot_path,
            "artifact_state_coherent",
            "review_a",
            "release_a",
            true,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        fs::create_dir_all(&history_dir).expect("history dir");
        fs::copy(&snapshot_path, &history_snapshot_path).expect("copy history snapshot");
        let bundle_path =
            export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &snapshot_path);
        write_bundle_pointer_metadata(
            &bundle_pointer_dir,
            activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME,
            &bundle_archive_dir,
            &bundle_path,
        );
        write_snapshot_pointer_metadata(
            &snapshot_pointer_dir,
            activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME,
            &state_archive_dir,
            &snapshot_path,
        );

        let report = build_report(&Config {
            bundle_archive_dir,
            bundle_latest_pointer_dir: Some(bundle_pointer_dir),
            bundle_pointer_name:
                activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME
                    .to_string(),
            state_archive_dir,
            snapshot_latest_pointer_dir: Some(snapshot_pointer_dir),
            snapshot_pointer_name:
                activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME.to_string(),
            history_dir: Some(history_dir),
            history_snapshot_paths: Vec::new(),
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleArchiveProvenanceVerdict::ArtifactStateBundleArchiveProvenanceComplete
        );
        assert_eq!(
            report.latest_bundle_pointer_relation,
            LatestArchivedBundleRelation::CoveredByArchiveAndCurrentSnapshotTruth
        );
        assert_eq!(report.current_snapshots_missing_archived_bundle_count, 0);
        assert_eq!(report.archived_bundles_missing_state_archive_count, 0);
    }

    #[test]
    fn latest_bundle_pointer_target_missing_from_archive_yields_inconsistent_lineage() {
        let root = temp_dir("state_bundle_archive_provenance_missing_pointer_target");
        let state_archive_dir = root.join("state_archive");
        let bundle_archive_dir = root.join("bundle_archive");
        let bundle_pointer_dir = root.join("bundle_pointer");
        let snapshot_path = state_archive_dir
            .join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        write_snapshot(
            &snapshot_path,
            "artifact_state_coherent",
            "review_a",
            "release_a",
            true,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &snapshot_path);
        write_missing_bundle_pointer_metadata(
            &bundle_pointer_dir,
            activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME,
            &bundle_archive_dir,
            &bundle_archive_dir.join("state_snapshot_bundle__missing"),
        );

        let report = build_report(&Config {
            bundle_archive_dir,
            bundle_latest_pointer_dir: Some(bundle_pointer_dir),
            bundle_pointer_name:
                activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME
                    .to_string(),
            state_archive_dir,
            snapshot_latest_pointer_dir: None,
            snapshot_pointer_name:
                activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME.to_string(),
            history_dir: None,
            history_snapshot_paths: Vec::new(),
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleArchiveProvenanceVerdict::ArtifactStateBundleArchiveProvenanceInconsistentLineage
        );
        assert_eq!(
            report.latest_bundle_pointer_relation,
            LatestArchivedBundleRelation::MissingTarget
        );
        assert!(report.reason.contains("latest bundle pointer"));
    }

    #[test]
    fn archived_bundle_referencing_snapshot_missing_from_current_state_archive_yields_inconsistent_lineage(
    ) {
        let root = temp_dir("state_bundle_archive_provenance_foreign_bundle");
        let current_state_archive_dir = root.join("current_state_archive");
        let foreign_state_archive_dir = root.join("foreign_state_archive");
        let bundle_archive_dir = root.join("bundle_archive");
        let current_snapshot = current_state_archive_dir
            .join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        let foreign_snapshot = foreign_state_archive_dir
            .join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        write_snapshot(
            &current_snapshot,
            "artifact_state_coherent",
            "review_a",
            "release_a",
            true,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        write_snapshot(
            &foreign_snapshot,
            "artifact_state_coherent",
            "review_a",
            "release_a",
            true,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        export_archived_bundle(
            &bundle_archive_dir,
            &foreign_state_archive_dir,
            &foreign_snapshot,
        );

        let report = build_report(&Config {
            bundle_archive_dir,
            bundle_latest_pointer_dir: None,
            bundle_pointer_name:
                activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME
                    .to_string(),
            state_archive_dir: current_state_archive_dir,
            snapshot_latest_pointer_dir: None,
            snapshot_pointer_name:
                activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME.to_string(),
            history_dir: None,
            history_snapshot_paths: Vec::new(),
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleArchiveProvenanceVerdict::ArtifactStateBundleArchiveProvenanceInconsistentLineage
        );
        assert_eq!(report.archived_bundles_missing_state_archive_count, 1);
    }

    #[test]
    fn current_latest_snapshot_missing_archived_bundle_coverage_yields_incomplete_provenance() {
        let root = temp_dir("state_bundle_archive_provenance_missing_current_coverage");
        let state_archive_dir = root.join("state_archive");
        let snapshot_pointer_dir = root.join("snapshot_pointer");
        let bundle_archive_dir = root.join("bundle_archive");
        let older_snapshot = state_archive_dir
            .join("state_snapshot__2026-03-26T17-00-00Z__artifact_state_coherent.json");
        let latest_snapshot = state_archive_dir
            .join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        write_snapshot(
            &older_snapshot,
            "artifact_state_coherent",
            "review_a",
            "release_a",
            true,
            0,
            ts("2026-03-26T17:00:00Z"),
        );
        write_snapshot(
            &latest_snapshot,
            "artifact_state_coherent",
            "review_b",
            "release_b",
            true,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &older_snapshot);
        write_snapshot_pointer_metadata(
            &snapshot_pointer_dir,
            activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME,
            &state_archive_dir,
            &latest_snapshot,
        );

        let report = build_report(&Config {
            bundle_archive_dir,
            bundle_latest_pointer_dir: None,
            bundle_pointer_name:
                activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME
                    .to_string(),
            state_archive_dir,
            snapshot_latest_pointer_dir: Some(snapshot_pointer_dir),
            snapshot_pointer_name:
                activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME.to_string(),
            history_dir: None,
            history_snapshot_paths: Vec::new(),
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleArchiveProvenanceVerdict::ArtifactStateBundleArchiveProvenanceIncomplete
        );
        assert!(report.current_snapshots_missing_archived_bundle_count >= 1);
        assert_eq!(
            report.snapshot_latest_pointer_selected_snapshot_has_archived_bundle_coverage,
            Some(false)
        );
    }

    #[test]
    fn ambiguous_archived_bundle_is_surfaced_honestly_and_not_treated_as_clean_green() {
        let root = temp_dir("state_bundle_archive_provenance_ambiguous");
        let state_archive_dir = root.join("state_archive");
        let bundle_archive_dir = root.join("bundle_archive");
        let snapshot = state_archive_dir.join(
            "state_snapshot__2026-03-26T18-00-00Z__artifact_state_ambiguous_legacy_state.json",
        );
        write_snapshot(
            &snapshot,
            "artifact_state_ambiguous_legacy_state",
            "review_a",
            "release_a",
            true,
            2,
            ts("2026-03-26T18:00:00Z"),
        );
        let bundle_path =
            export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &snapshot);
        let bundle_pointer_dir = root.join("bundle_pointer");
        write_bundle_pointer_metadata(
            &bundle_pointer_dir,
            activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME,
            &bundle_archive_dir,
            &bundle_path,
        );

        let report = build_report(&Config {
            bundle_archive_dir,
            bundle_latest_pointer_dir: Some(bundle_pointer_dir),
            bundle_pointer_name:
                activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME
                    .to_string(),
            state_archive_dir,
            snapshot_latest_pointer_dir: None,
            snapshot_pointer_name:
                activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME.to_string(),
            history_dir: None,
            history_snapshot_paths: Vec::new(),
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleArchiveProvenanceVerdict::ArtifactStateBundleArchiveProvenanceAmbiguousLegacyState
        );
        assert_eq!(report.ambiguous_archived_bundle_count, 1);
        assert_eq!(
            report.latest_bundle_pointer_relation,
            LatestArchivedBundleRelation::AmbiguousSnapshotBundle
        );
    }

    #[test]
    fn malformed_archived_bundle_metadata_is_reported_as_invalid() {
        let root = temp_dir("state_bundle_archive_provenance_invalid_bundle");
        let state_archive_dir = root.join("state_archive");
        let bundle_archive_dir = root.join("bundle_archive");
        let snapshot = state_archive_dir
            .join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        write_snapshot(
            &snapshot,
            "artifact_state_coherent",
            "review_a",
            "release_a",
            true,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &snapshot);
        let invalid_bundle_dir = bundle_archive_dir.join("state_snapshot_bundle__broken");
        fs::create_dir_all(&invalid_bundle_dir).expect("invalid bundle dir");
        fs::write(
            invalid_bundle_dir.join(activation_artifact_state_bundle::BUNDLE_MANIFEST_FILENAME),
            "{broken",
        )
        .expect("write invalid manifest");

        let report = build_report(&Config {
            bundle_archive_dir,
            bundle_latest_pointer_dir: None,
            bundle_pointer_name:
                activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME
                    .to_string(),
            state_archive_dir,
            snapshot_latest_pointer_dir: None,
            snapshot_pointer_name:
                activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME.to_string(),
            history_dir: None,
            history_snapshot_paths: Vec::new(),
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleArchiveProvenanceVerdict::ArtifactStateBundleArchiveProvenanceInvalidArtifactsPresent
        );
        assert_eq!(report.invalid_artifact_count, 1);
    }

    fn export_archived_bundle(
        bundle_archive_dir: &Path,
        state_archive_dir: &Path,
        snapshot_path: &Path,
    ) -> PathBuf {
        let snapshot_file_name = snapshot_path
            .file_name()
            .expect("snapshot file name")
            .to_string_lossy()
            .into_owned();
        let bundle_dir = bundle_archive_dir.join(bundle_directory_name(&snapshot_file_name));
        let summary = activation_artifact_state_bundle::export_state_snapshot_bundle(
            state_archive_dir,
            &snapshot_file_name,
            &bundle_dir,
        )
        .expect("export bundle");
        assert_eq!(summary.verdict, "artifact_state_bundle_exported");
        bundle_dir
    }

    fn bundle_directory_name(selected_snapshot_file_name: &str) -> String {
        let stem = Path::new(selected_snapshot_file_name)
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or(selected_snapshot_file_name);
        format!("state_snapshot_bundle__{stem}")
    }

    fn write_bundle_pointer_metadata(
        pointer_dir: &Path,
        pointer_name: &str,
        bundle_archive_dir: &Path,
        bundle_path: &Path,
    ) {
        fs::create_dir_all(pointer_dir).expect("pointer dir");
        let summary = activation_artifact_state_bundle::inspect_state_snapshot_bundle(bundle_path)
            .expect("inspect bundle");
        let canonical_bundle_path = fs::canonicalize(bundle_path).expect("canonical bundle");
        let metadata =
            activation_artifact_state_bundle_publish_report::StateBundleLatestPointerMetadata {
                pointer_version: "1".to_string(),
                pointer_name: pointer_name.to_string(),
                source_bundle_archive_dir: fs::canonicalize(bundle_archive_dir)
                    .expect("canonical bundle archive")
                    .display()
                    .to_string(),
                selected_bundle_path: canonical_bundle_path.display().to_string(),
                selected_bundle_dir_name: canonical_bundle_path
                    .file_name()
                    .expect("bundle dir name")
                    .to_string_lossy()
                    .into_owned(),
                source_state_archive_dir: summary
                    .source_state_archive_dir
                    .expect("source state archive"),
                selected_snapshot_source_path: summary
                    .selected_snapshot_path
                    .expect("selected snapshot path"),
                selected_snapshot_file_name: summary
                    .selected_snapshot_file_name
                    .expect("selected snapshot file name"),
                snapshotted_at: summary.snapshotted_at.expect("snapshotted_at"),
                snapshot_state_verdict: summary
                    .selected_snapshot_state_verdict
                    .expect("snapshot verdict"),
                snapshot_state_reason: summary
                    .selected_snapshot_state_reason
                    .expect("snapshot reason"),
                selected_review_generation_id: summary.selected_review_generation_id.clone(),
                selected_latest_release_generation_id: summary
                    .selected_latest_release_generation_id
                    .clone(),
                ambiguous_legacy_count: summary.ambiguous_legacy_count.unwrap_or_default(),
                coherent_for_review_operations: summary
                    .coherent_for_review_operations
                    .unwrap_or(false),
                pointed_at: ts("2026-03-26T18:05:00Z"),
                build_version: "0.1.0".to_string(),
                git_commit: Some("deadbeef".to_string()),
            };
        let pointer_path =
            activation_artifact_state_bundle_publish_report::latest_state_bundle_pointer_path(
                pointer_dir,
                pointer_name,
            )
            .expect("pointer path");
        fs::write(
            pointer_path,
            serde_json::to_string_pretty(&metadata).expect("pointer json"),
        )
        .expect("write pointer");
    }

    fn write_missing_bundle_pointer_metadata(
        pointer_dir: &Path,
        pointer_name: &str,
        bundle_archive_dir: &Path,
        missing_bundle_path: &Path,
    ) {
        fs::create_dir_all(pointer_dir).expect("pointer dir");
        let metadata =
            activation_artifact_state_bundle_publish_report::StateBundleLatestPointerMetadata {
                pointer_version: "1".to_string(),
                pointer_name: pointer_name.to_string(),
                source_bundle_archive_dir: fs::canonicalize(bundle_archive_dir)
                    .expect("canonical bundle archive")
                    .display()
                    .to_string(),
                selected_bundle_path: missing_bundle_path.display().to_string(),
                selected_bundle_dir_name: missing_bundle_path
                    .file_name()
                    .expect("bundle dir name")
                    .to_string_lossy()
                    .into_owned(),
                source_state_archive_dir: "/tmp/state_archive".to_string(),
                selected_snapshot_source_path:
                    "/tmp/state_archive/state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json"
                        .to_string(),
                selected_snapshot_file_name:
                    "state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json"
                        .to_string(),
                snapshotted_at: ts("2026-03-26T18:00:00Z"),
                snapshot_state_verdict: "artifact_state_coherent".to_string(),
                snapshot_state_reason: "sample artifact_state_coherent".to_string(),
                selected_review_generation_id: Some("review_missing".to_string()),
                selected_latest_release_generation_id: Some("release_missing".to_string()),
                ambiguous_legacy_count: 0,
                coherent_for_review_operations: true,
                pointed_at: ts("2026-03-26T18:05:00Z"),
                build_version: "0.1.0".to_string(),
                git_commit: Some("deadbeef".to_string()),
            };
        let pointer_path =
            activation_artifact_state_bundle_publish_report::latest_state_bundle_pointer_path(
                pointer_dir,
                pointer_name,
            )
            .expect("pointer path");
        fs::write(
            pointer_path,
            serde_json::to_string_pretty(&metadata).expect("pointer json"),
        )
        .expect("write pointer");
    }

    fn write_snapshot_pointer_metadata(
        pointer_dir: &Path,
        pointer_name: &str,
        state_archive_dir: &Path,
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
            source_state_archive_dir: fs::canonicalize(state_archive_dir)
                .expect("canonical state archive")
                .display()
                .to_string(),
            selected_snapshot_path: loaded.canonical_path.display().to_string(),
            selected_snapshot_file_name: loaded
                .canonical_path
                .file_name()
                .expect("snapshot file name")
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
        let pointer_path = pointer_dir.join(format!("{pointer_name}.json"));
        fs::write(
            pointer_path,
            serde_json::to_string_pretty(&metadata).expect("pointer json"),
        )
        .expect("write snapshot pointer");
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
        let artifact = json!({
            "mode": "artifact_state_snapshot",
            "snapshot_version": "1",
            "snapshotted_at": snapshotted_at,
            "build_version": "0.1.0",
            "git_commit": "deadbeef",
            "state_verdict": state_verdict,
            "state_reason": format!("sample {state_verdict}"),
            "review_archive_dir": "/tmp/review_archive",
            "review_manifest_dir": "/tmp/review_manifest",
            "review_bundle_dir": "/tmp/review_bundle",
            "review_channel_dir": "/tmp/review_channel",
            "review_channel_name": "current_review",
            "release_archive_dir": "/tmp/release_archive",
            "release_history_dir": "/tmp/release_history",
            "latest_release_pointer_dir": "/tmp/release_pointer",
            "latest_release_pointer_name": "latest_release",
            "selected_review_generation_id": selected_review_generation_id,
            "selected_latest_release_generation_id": selected_latest_release_generation_id,
            "selection_alignment_matches": selection_alignment_matches,
            "selection_alignment_summary": if selection_alignment_matches {
                "review and release selections match"
            } else {
                "review and release selections diverge"
            },
            "review_provenance_verdict": "artifact_provenance_complete",
            "review_provenance_reason": "sample",
            "release_provenance_verdict": "artifact_release_provenance_complete",
            "release_provenance_reason": "sample",
            "linkage_verdict": "artifact_linkage_complete",
            "linkage_reason": "sample",
            "ambiguous_legacy_count": ambiguous_legacy_count,
            "coherent_for_review_operations": state_verdict == "artifact_state_coherent",
            "artifact_state_only": true,
            "execution_untouched": true,
            "activation_authorized": false,
            "not_authorized_summary": "artifact-state only",
            "state_report": {
                "mode": "artifact_state_report",
                "verdict": state_verdict
            }
        });
        fs::write(
            path,
            serde_json::to_string_pretty(&artifact).expect("snapshot json"),
        )
        .expect("write snapshot");
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
