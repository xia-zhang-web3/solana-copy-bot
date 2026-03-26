#![recursion_limit = "256"]

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[allow(dead_code)]
#[path = "copybot_activation_artifact_release_history.rs"]
mod activation_artifact_release_history;
#[allow(dead_code)]
#[path = "copybot_activation_artifact_release_publish_report.rs"]
mod activation_artifact_release_publish_report;

const USAGE: &str = "usage: copybot_activation_artifact_release_provenance_report --release-archive-dir <path> --latest-pointer-dir <path> [--history-dir <path>] [--history-release <path>]... [--pointer-name <name>] [--json]";

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
    release_archive_dir: PathBuf,
    latest_pointer_dir: PathBuf,
    history_dir: Option<PathBuf>,
    history_release_paths: Vec<PathBuf>,
    pointer_name: String,
    json: bool,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ArtifactReleaseProvenanceVerdict {
    ArtifactReleaseProvenanceComplete,
    ArtifactReleaseProvenanceIncomplete,
    ArtifactReleaseProvenanceInvalidArtifactsPresent,
    ArtifactReleaseProvenanceInconsistentLineage,
    ArtifactReleaseProvenanceAmbiguousTimestamp,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LatestReleasePointerRelation {
    NoPointerMetadata,
    InvalidMetadata,
    MissingTarget,
    MissingFromArchive,
    MissingFromHistory,
    BehindLatestArchive,
    AheadOfHistory,
    MatchesLatestArchiveAndHistory,
    MatchesLatestArchiveOnly,
    AmbiguousTimestamp,
}

#[derive(Debug, Clone, Serialize)]
struct ReleaseLineageIssue {
    surface: String,
    path: String,
    error: String,
}

#[derive(Debug, Clone, Serialize)]
struct ReleaseArtifactCoverageSummary {
    identity_key: String,
    released_at: Option<DateTime<Utc>>,
    released_at_source: activation_artifact_release_history::ReleaseTimestampSource,
    generation_id: Option<String>,
    archive_paths: Vec<String>,
    history_paths: Vec<String>,
    latest_pointer_selected: bool,
    latest_by_archive_timestamp: bool,
    latest_by_history_timestamp: bool,
    missing_history_coverage: bool,
    compat_loaded_without_released_at: bool,
    deterministic_timestamp_available: bool,
}

#[derive(Debug, Clone, Serialize)]
struct HistoryOnlyReleaseSummary {
    identity_key: String,
    released_at: Option<DateTime<Utc>>,
    released_at_source: activation_artifact_release_history::ReleaseTimestampSource,
    generation_id: Option<String>,
    history_paths: Vec<String>,
    compat_loaded_without_released_at: bool,
    deterministic_timestamp_available: bool,
}

#[derive(Debug, Clone, Serialize)]
struct LatestReleasePointerSummary {
    pointer_name: String,
    pointer_verdict: String,
    pointer_reason: String,
    pointer_path: Option<String>,
    source_release_archive_dir: Option<String>,
    selected_release_artifact_path: Option<String>,
    selected_release_artifact_file_name: Option<String>,
    selected_generation_id: Option<String>,
    released_at: Option<DateTime<Utc>>,
    released_at_source: Option<String>,
    pointed_at: Option<DateTime<Utc>>,
    target_exists: bool,
    target_matches_identity: bool,
    present_in_archive: bool,
    present_in_history: bool,
    matches_latest_archive: bool,
    matches_latest_history: bool,
    relation: LatestReleasePointerRelation,
    missing_paths: Vec<String>,
    inconsistencies: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ReleaseProvenanceReport {
    mode: String,
    verdict: ArtifactReleaseProvenanceVerdict,
    reason: String,
    release_archive_dir: String,
    latest_pointer_dir: String,
    history_dir: Option<String>,
    history_release_paths: Vec<String>,
    pointer_name: String,
    archive_release_count: usize,
    history_release_count: usize,
    latest_pointer_present: bool,
    latest_pointer_selected_generation_id: Option<String>,
    latest_pointer_relation: LatestReleasePointerRelation,
    latest_archive_generation_id: Option<String>,
    latest_history_generation_id: Option<String>,
    archive_releases_missing_from_history_count: usize,
    history_releases_missing_from_archive_count: usize,
    invalid_artifact_count: usize,
    invalid_artifacts: Vec<ReleaseLineageIssue>,
    ambiguous_timestamp_count: usize,
    archive_releases: Vec<ReleaseArtifactCoverageSummary>,
    history_releases_missing_from_archive: Vec<HistoryOnlyReleaseSummary>,
    latest_pointer: LatestReleasePointerSummary,
    release_lineage_only: bool,
    execution_untouched: bool,
    activation_authorized: bool,
    not_authorized_summary: String,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct ReleaseProvenanceSummary {
    pub(crate) verdict: String,
    pub(crate) reason: String,
    pub(crate) archive_release_count: usize,
    pub(crate) history_release_count: usize,
    pub(crate) latest_pointer_present: bool,
    pub(crate) latest_pointer_selected_generation_id: Option<String>,
    pub(crate) latest_pointer_relation: String,
    pub(crate) latest_archive_generation_id: Option<String>,
    pub(crate) latest_history_generation_id: Option<String>,
    pub(crate) archive_releases_missing_from_history_count: usize,
    pub(crate) history_releases_missing_from_archive_count: usize,
    pub(crate) invalid_artifact_count: usize,
    pub(crate) ambiguous_timestamp_count: usize,
}

#[derive(Debug, Clone)]
struct ReleaseSurfaceRecord {
    loaded: activation_artifact_release_history::LoadedRelease,
    identity_key: String,
    canonical_path: Option<PathBuf>,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut release_archive_dir: Option<PathBuf> = None;
    let mut latest_pointer_dir: Option<PathBuf> = None;
    let mut history_dir: Option<PathBuf> = None;
    let mut history_release_paths = Vec::new();
    let mut pointer_name =
        activation_artifact_release_publish_report::DEFAULT_LATEST_POINTER_NAME.to_string();
    let mut json = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--release-archive-dir" => {
                release_archive_dir = Some(PathBuf::from(parse_string_arg(
                    "--release-archive-dir",
                    args.next(),
                )?))
            }
            "--latest-pointer-dir" => {
                latest_pointer_dir = Some(PathBuf::from(parse_string_arg(
                    "--latest-pointer-dir",
                    args.next(),
                )?))
            }
            "--history-dir" => {
                history_dir = Some(PathBuf::from(parse_string_arg(
                    "--history-dir",
                    args.next(),
                )?))
            }
            "--history-release" => history_release_paths.push(PathBuf::from(parse_string_arg(
                "--history-release",
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

    Ok(Some(Config {
        release_archive_dir: release_archive_dir
            .ok_or_else(|| anyhow!("--release-archive-dir is required"))?,
        latest_pointer_dir: latest_pointer_dir
            .ok_or_else(|| anyhow!("--latest-pointer-dir is required"))?,
        history_dir,
        history_release_paths,
        pointer_name,
        json,
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

#[allow(dead_code)]
pub(crate) fn inspect_release_provenance_summary(
    release_archive_dir: &Path,
    latest_pointer_dir: &Path,
    history_dir: Option<&Path>,
    history_release_paths: &[PathBuf],
    pointer_name: &str,
) -> Result<ReleaseProvenanceSummary> {
    let report = build_report(&Config {
        release_archive_dir: release_archive_dir.to_path_buf(),
        latest_pointer_dir: latest_pointer_dir.to_path_buf(),
        history_dir: history_dir.map(|path| path.to_path_buf()),
        history_release_paths: history_release_paths.to_vec(),
        pointer_name: pointer_name.to_string(),
        json: false,
    })?;
    Ok(ReleaseProvenanceSummary {
        verdict: serialize_enum(&report.verdict),
        reason: report.reason,
        archive_release_count: report.archive_release_count,
        history_release_count: report.history_release_count,
        latest_pointer_present: report.latest_pointer_present,
        latest_pointer_selected_generation_id: report.latest_pointer_selected_generation_id,
        latest_pointer_relation: serialize_enum(&report.latest_pointer_relation),
        latest_archive_generation_id: report.latest_archive_generation_id,
        latest_history_generation_id: report.latest_history_generation_id,
        archive_releases_missing_from_history_count: report
            .archive_releases_missing_from_history_count,
        history_releases_missing_from_archive_count: report
            .history_releases_missing_from_archive_count,
        invalid_artifact_count: report.invalid_artifact_count,
        ambiguous_timestamp_count: report.ambiguous_timestamp_count,
    })
}

fn build_report(config: &Config) -> Result<ReleaseProvenanceReport> {
    let archive_paths = collect_release_paths(&config.release_archive_dir)?;
    let history_paths = collect_history_paths(config)?;

    let (archive_records, mut invalid_artifacts) =
        load_release_surface("release_archive", &archive_paths);
    let (history_records, invalid_history) = load_release_surface("history", &history_paths);
    invalid_artifacts.extend(invalid_history);

    let pointer_report_result =
        activation_artifact_release_publish_report::inspect_latest_pointer_report(
            &config.release_archive_dir,
            &config.latest_pointer_dir,
            &config.pointer_name,
            true,
        );

    let pointer_report = match pointer_report_result {
        Ok(report) => report,
        Err(error) => {
            invalid_artifacts.push(ReleaseLineageIssue {
                surface: "latest_pointer".to_string(),
                path: latest_pointer_metadata_path(
                    &config.latest_pointer_dir,
                    &config.pointer_name,
                )
                .display()
                .to_string(),
                error: format!("{error:#}"),
            });
            activation_artifact_release_publish_report::ArtifactReleasePublishReport {
                mode: "verify_latest".to_string(),
                verdict: activation_artifact_release_publish_report::ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportInvalidMetadata,
                reason: "latest pointer inspection failed operationally".to_string(),
                release_archive_dir: config.release_archive_dir.display().to_string(),
                persisted_release_artifact_path: None,
                persisted_release_artifact_exists: false,
                persisted_release_artifact_file_name: None,
                release_verdict: None,
                release_reason: None,
                generation_id: None,
                released_at: None,
                released_at_source: None,
                compat_loaded_without_released_at: false,
                deterministic_timestamp_available: false,
                ordered_history_confident: false,
                latest_pointer_attempted: false,
                latest_pointer_updated: false,
                latest_pointer_dir: Some(config.latest_pointer_dir.display().to_string()),
                latest_pointer_name: config.pointer_name.clone(),
                latest_pointer_path: Some(
                    latest_pointer_metadata_path(&config.latest_pointer_dir, &config.pointer_name)
                        .display()
                        .to_string(),
                ),
                latest_pointer_source_release_archive_dir: None,
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
                    "Artifact/release management only persists and verifies release artifacts. It does not authorize activation, does not enable execution, and does not override the Stage 3 production gate.".to_string(),
            }
        }
    };

    if pointer_report.verdict
        == activation_artifact_release_publish_report::ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportInvalidMetadata
    {
        invalid_artifacts.push(ReleaseLineageIssue {
            surface: "latest_pointer".to_string(),
            path: pointer_report
                .latest_pointer_path
                .clone()
                .unwrap_or_else(|| {
                    latest_pointer_metadata_path(&config.latest_pointer_dir, &config.pointer_name)
                        .display()
                        .to_string()
                }),
            error: pointer_report.reason.clone(),
        });
    }

    let archive_identity_map = identity_map(&archive_records);
    let history_identity_map = identity_map(&history_records);
    let archive_canonical_paths = canonical_path_map(&archive_records);
    let history_canonical_paths = canonical_path_map(&history_records);

    let latest_archive_record = latest_deterministic_release(&archive_records);
    let latest_history_record = latest_deterministic_release(&history_records);
    let latest_archive_identity_key =
        latest_archive_record.map(|record| record.identity_key.clone());
    let latest_history_identity_key =
        latest_history_record.map(|record| record.identity_key.clone());

    let pointer_identity_key = pointer_identity_key(&pointer_report);
    let pointer_canonical_path = pointer_report
        .persisted_release_artifact_path
        .as_deref()
        .map(Path::new)
        .filter(|path| path.is_absolute())
        .and_then(|path| fs::canonicalize(path).ok());

    let pointer_present_in_archive = pointer_canonical_path
        .as_ref()
        .is_some_and(|path| archive_canonical_paths.contains_key(path))
        || pointer_identity_key
            .as_ref()
            .is_some_and(|key| archive_identity_map.contains_key(key));
    let pointer_present_in_history = pointer_canonical_path
        .as_ref()
        .is_some_and(|path| history_canonical_paths.contains_key(path))
        || pointer_identity_key
            .as_ref()
            .is_some_and(|key| history_identity_map.contains_key(key));
    let pointer_matches_latest_archive = pointer_identity_key
        .as_ref()
        .zip(latest_archive_identity_key.as_ref())
        .is_some_and(|(left, right)| left == right);
    let pointer_matches_latest_history = pointer_identity_key
        .as_ref()
        .zip(latest_history_identity_key.as_ref())
        .is_some_and(|(left, right)| left == right);

    let pointer_relation = classify_pointer_relation(
        &pointer_report,
        pointer_present_in_archive,
        pointer_present_in_history,
        pointer_matches_latest_archive,
        pointer_matches_latest_history,
        latest_history_record.is_none(),
    );

    let archive_releases = archive_records
        .iter()
        .map(|record| ReleaseArtifactCoverageSummary {
            identity_key: record.identity_key.clone(),
            released_at: record.loaded.effective_released_at,
            released_at_source: record.loaded.released_at_source,
            generation_id: record.loaded.artifact.generation_id.clone(),
            archive_paths: archive_identity_map
                .get(&record.identity_key)
                .map(|value| {
                    value
                        .iter()
                        .map(|entry| entry.loaded.path.display().to_string())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default(),
            history_paths: history_identity_map
                .get(&record.identity_key)
                .map(|value| {
                    value
                        .iter()
                        .map(|entry| entry.loaded.path.display().to_string())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default(),
            latest_pointer_selected: pointer_identity_key
                .as_ref()
                .is_some_and(|value| value == &record.identity_key),
            latest_by_archive_timestamp: latest_archive_identity_key
                .as_ref()
                .is_some_and(|value| value == &record.identity_key),
            latest_by_history_timestamp: latest_history_identity_key
                .as_ref()
                .is_some_and(|value| value == &record.identity_key),
            missing_history_coverage: !history_identity_map.contains_key(&record.identity_key),
            compat_loaded_without_released_at: record.loaded.released_at_source
                != activation_artifact_release_history::ReleaseTimestampSource::ReleasedAt,
            deterministic_timestamp_available: record.loaded.effective_released_at.is_some(),
        })
        .collect::<Vec<_>>();

    let history_releases_missing_from_archive = history_records
        .iter()
        .filter(|record| !archive_identity_map.contains_key(&record.identity_key))
        .map(|record| HistoryOnlyReleaseSummary {
            identity_key: record.identity_key.clone(),
            released_at: record.loaded.effective_released_at,
            released_at_source: record.loaded.released_at_source,
            generation_id: record.loaded.artifact.generation_id.clone(),
            history_paths: history_identity_map
                .get(&record.identity_key)
                .map(|value| {
                    value
                        .iter()
                        .map(|entry| entry.loaded.path.display().to_string())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default(),
            compat_loaded_without_released_at: record.loaded.released_at_source
                != activation_artifact_release_history::ReleaseTimestampSource::ReleasedAt,
            deterministic_timestamp_available: record.loaded.effective_released_at.is_some(),
        })
        .collect::<Vec<_>>();

    let archive_releases_missing_from_history_count = archive_releases
        .iter()
        .filter(|summary| summary.missing_history_coverage)
        .count();
    let history_releases_missing_from_archive_count = history_releases_missing_from_archive.len();
    let ambiguous_timestamp_count = archive_records
        .iter()
        .chain(history_records.iter())
        .filter(|record| {
            record.loaded.released_at_source
                == activation_artifact_release_history::ReleaseTimestampSource::MissingDeterministicTimestamp
        })
        .map(|record| record.identity_key.clone())
        .collect::<BTreeSet<_>>()
        .len();

    let latest_pointer = LatestReleasePointerSummary {
        pointer_name: config.pointer_name.clone(),
        pointer_verdict: serialize_enum(&pointer_report.verdict),
        pointer_reason: pointer_report.reason.clone(),
        pointer_path: pointer_report.latest_pointer_path.clone(),
        source_release_archive_dir: pointer_report
            .latest_pointer_source_release_archive_dir
            .clone(),
        selected_release_artifact_path: pointer_report.persisted_release_artifact_path.clone(),
        selected_release_artifact_file_name: pointer_report
            .persisted_release_artifact_file_name
            .clone(),
        selected_generation_id: pointer_report.generation_id.clone(),
        released_at: pointer_report.released_at,
        released_at_source: pointer_report
            .released_at_source
            .as_ref()
            .map(serialize_enum),
        pointed_at: pointer_report.latest_pointer_pointed_at,
        target_exists: pointer_report.latest_pointer_target_exists,
        target_matches_identity: pointer_report.latest_pointer_target_matches_identity,
        present_in_archive: pointer_present_in_archive,
        present_in_history: pointer_present_in_history,
        matches_latest_archive: pointer_matches_latest_archive,
        matches_latest_history: pointer_matches_latest_history,
        relation: pointer_relation,
        missing_paths: pointer_report.missing_paths.clone(),
        inconsistencies: pointer_report.inconsistencies.clone(),
    };

    let no_archive_releases = archive_records.is_empty();
    let history_missing = history_records.is_empty() && history_paths.is_empty();
    let pointer_missing = pointer_relation == LatestReleasePointerRelation::NoPointerMetadata;
    let pointer_inconsistent = matches!(
        pointer_relation,
        LatestReleasePointerRelation::InvalidMetadata
            | LatestReleasePointerRelation::MissingTarget
            | LatestReleasePointerRelation::MissingFromArchive
    );
    let pointer_incomplete = matches!(
        pointer_relation,
        LatestReleasePointerRelation::NoPointerMetadata
            | LatestReleasePointerRelation::MissingFromHistory
            | LatestReleasePointerRelation::BehindLatestArchive
            | LatestReleasePointerRelation::AheadOfHistory
            | LatestReleasePointerRelation::MatchesLatestArchiveOnly
    );

    let verdict = if !invalid_artifacts.is_empty() {
        ArtifactReleaseProvenanceVerdict::ArtifactReleaseProvenanceInvalidArtifactsPresent
    } else if history_releases_missing_from_archive_count > 0 || pointer_inconsistent {
        ArtifactReleaseProvenanceVerdict::ArtifactReleaseProvenanceInconsistentLineage
    } else if no_archive_releases
        || archive_releases_missing_from_history_count > 0
        || history_missing
        || pointer_missing
        || pointer_incomplete
    {
        ArtifactReleaseProvenanceVerdict::ArtifactReleaseProvenanceIncomplete
    } else if ambiguous_timestamp_count > 0
        || pointer_relation == LatestReleasePointerRelation::AmbiguousTimestamp
    {
        ArtifactReleaseProvenanceVerdict::ArtifactReleaseProvenanceAmbiguousTimestamp
    } else {
        ArtifactReleaseProvenanceVerdict::ArtifactReleaseProvenanceComplete
    };

    let reason = match verdict {
        ArtifactReleaseProvenanceVerdict::ArtifactReleaseProvenanceComplete => format!(
            "release provenance is complete across {} archive release artifact(s), {} history artifact(s), and latest pointer `{}`",
            archive_records.len(),
            history_records.len(),
            config.pointer_name
        ),
        ArtifactReleaseProvenanceVerdict::ArtifactReleaseProvenanceInvalidArtifactsPresent => {
            format!(
                "release provenance is blocked by {} invalid archive/history/latest-pointer artifact(s)",
                invalid_artifacts.len()
            )
        }
        ArtifactReleaseProvenanceVerdict::ArtifactReleaseProvenanceInconsistentLineage
            if pointer_inconsistent =>
        {
            format!(
                "release provenance is inconsistent because latest pointer `{}` does not resolve cleanly: {}",
                config.pointer_name, latest_pointer.pointer_reason
            )
        }
        ArtifactReleaseProvenanceVerdict::ArtifactReleaseProvenanceInconsistentLineage => format!(
            "release provenance is inconsistent: {} history artifact identity/identities are missing from the release archive",
            history_releases_missing_from_archive_count
        ),
        ArtifactReleaseProvenanceVerdict::ArtifactReleaseProvenanceIncomplete if no_archive_releases => {
            "release provenance is incomplete: no persisted release artifacts found in the release archive".to_string()
        }
        ArtifactReleaseProvenanceVerdict::ArtifactReleaseProvenanceIncomplete if history_missing => {
            "release provenance is incomplete: no history release artifacts were provided".to_string()
        }
        ArtifactReleaseProvenanceVerdict::ArtifactReleaseProvenanceIncomplete if pointer_missing => {
            format!(
                "release provenance is incomplete: latest pointer `{}` is missing",
                config.pointer_name
            )
        }
        ArtifactReleaseProvenanceVerdict::ArtifactReleaseProvenanceIncomplete
            if pointer_relation == LatestReleasePointerRelation::BehindLatestArchive =>
        {
            format!(
                "release provenance is incomplete: latest pointer `{}` is behind the latest release artifact in the archive",
                config.pointer_name
            )
        }
        ArtifactReleaseProvenanceVerdict::ArtifactReleaseProvenanceIncomplete
            if pointer_relation == LatestReleasePointerRelation::AheadOfHistory =>
        {
            format!(
                "release provenance is incomplete: latest pointer `{}` is ahead of the history surface",
                config.pointer_name
            )
        }
        ArtifactReleaseProvenanceVerdict::ArtifactReleaseProvenanceIncomplete
            if pointer_relation == LatestReleasePointerRelation::MissingFromHistory =>
        {
            format!(
                "release provenance is incomplete: latest pointer `{}` target is not covered by the history surface",
                config.pointer_name
            )
        }
        ArtifactReleaseProvenanceVerdict::ArtifactReleaseProvenanceIncomplete => format!(
            "release provenance is incomplete for {} release artifact(s): archive/history coverage or latest-pointer progression is missing",
            archive_releases_missing_from_history_count
        ),
        ArtifactReleaseProvenanceVerdict::ArtifactReleaseProvenanceAmbiguousTimestamp => format!(
            "release provenance is not cleanly ordered because {} release artifact(s) still rely on ambiguous legacy timestamp lineage",
            ambiguous_timestamp_count.max(1)
        ),
    };

    Ok(ReleaseProvenanceReport {
        mode: "activation_artifact_release_provenance".to_string(),
        verdict,
        reason,
        release_archive_dir: config.release_archive_dir.display().to_string(),
        latest_pointer_dir: config.latest_pointer_dir.display().to_string(),
        history_dir: config
            .history_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        history_release_paths: config
            .history_release_paths
            .iter()
            .map(|path| path.display().to_string())
            .collect(),
        pointer_name: config.pointer_name.clone(),
        archive_release_count: archive_records.len(),
        history_release_count: history_records.len(),
        latest_pointer_present: latest_pointer.pointer_path.is_some() && latest_pointer.target_exists,
        latest_pointer_selected_generation_id: latest_pointer.selected_generation_id.clone(),
        latest_pointer_relation: latest_pointer.relation,
        latest_archive_generation_id: latest_archive_record
            .and_then(|record| record.loaded.artifact.generation_id.clone()),
        latest_history_generation_id: latest_history_record
            .and_then(|record| record.loaded.artifact.generation_id.clone()),
        archive_releases_missing_from_history_count,
        history_releases_missing_from_archive_count,
        invalid_artifact_count: invalid_artifacts.len(),
        invalid_artifacts,
        ambiguous_timestamp_count,
        archive_releases,
        history_releases_missing_from_archive,
        latest_pointer,
        release_lineage_only: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary:
            "This release provenance report only audits persisted release artifacts, latest-pointer metadata, and release-history coverage. It does not authorize activation and does not override the Stage 3 production gate.".to_string(),
    })
}

fn collect_release_paths(root: &Path) -> Result<Vec<PathBuf>> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut paths = fs::read_dir(root)
        .with_context(|| format!("failed reading release dir {}", root.display()))?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.is_file() && path.extension().is_some_and(|ext| ext == "json"))
        .collect::<Vec<_>>();
    paths.sort();
    Ok(paths)
}

fn collect_history_paths(config: &Config) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    if let Some(dir) = &config.history_dir {
        paths.extend(collect_release_paths(dir)?);
    }
    paths.extend(config.history_release_paths.iter().cloned());
    paths.sort();
    paths.dedup();
    Ok(paths)
}

fn load_release_surface(
    surface: &str,
    paths: &[PathBuf],
) -> (Vec<ReleaseSurfaceRecord>, Vec<ReleaseLineageIssue>) {
    let mut valid = Vec::new();
    let mut invalid = Vec::new();
    for path in paths {
        match activation_artifact_release_history::inspect_release_artifact(path) {
            Ok(loaded) => {
                let file_name = path
                    .file_name()
                    .map(|value| value.to_string_lossy().into_owned())
                    .unwrap_or_else(|| path.display().to_string());
                let identity_key = release_identity_key(&loaded, &file_name);
                let canonical_path = fs::canonicalize(path).ok();
                valid.push(ReleaseSurfaceRecord {
                    loaded,
                    identity_key,
                    canonical_path,
                });
            }
            Err(error) => invalid.push(ReleaseLineageIssue {
                surface: surface.to_string(),
                path: path.display().to_string(),
                error: format!("{error:#}"),
            }),
        }
    }
    valid.sort_by(|left, right| release_sort_key(left).cmp(&release_sort_key(right)));
    (valid, invalid)
}

fn release_sort_key(
    record: &ReleaseSurfaceRecord,
) -> (Option<DateTime<Utc>>, Option<String>, String) {
    (
        record.loaded.effective_released_at,
        record.loaded.artifact.generation_id.clone(),
        record.loaded.path.display().to_string(),
    )
}

fn identity_map<'a>(
    records: &'a [ReleaseSurfaceRecord],
) -> BTreeMap<String, Vec<&'a ReleaseSurfaceRecord>> {
    let mut map = BTreeMap::<String, Vec<&ReleaseSurfaceRecord>>::new();
    for record in records {
        map.entry(record.identity_key.clone())
            .or_default()
            .push(record);
    }
    map
}

fn canonical_path_map<'a>(
    records: &'a [ReleaseSurfaceRecord],
) -> BTreeMap<PathBuf, &'a ReleaseSurfaceRecord> {
    let mut map = BTreeMap::new();
    for record in records {
        if let Some(path) = &record.canonical_path {
            map.insert(path.clone(), record);
        }
    }
    map
}

fn latest_deterministic_release(records: &[ReleaseSurfaceRecord]) -> Option<&ReleaseSurfaceRecord> {
    records
        .iter()
        .filter(|record| record.loaded.effective_released_at.is_some())
        .max_by_key(|record| release_sort_key(record))
}

fn release_identity_key(
    loaded: &activation_artifact_release_history::LoadedRelease,
    file_name: &str,
) -> String {
    if loaded.effective_released_at.is_some() || loaded.artifact.generation_id.is_some() {
        format!(
            "released_at={} | released_at_source={} | generation_id={}",
            loaded
                .effective_released_at
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "null".to_string()),
            serialize_enum(&loaded.released_at_source),
            loaded
                .artifact
                .generation_id
                .clone()
                .unwrap_or_else(|| "null".to_string())
        )
    } else {
        format!("ambiguous_file_name={file_name}")
    }
}

fn pointer_identity_key(
    report: &activation_artifact_release_publish_report::ArtifactReleasePublishReport,
) -> Option<String> {
    if report.released_at.is_none()
        && report.generation_id.is_none()
        && report.persisted_release_artifact_file_name.is_none()
    {
        return None;
    }

    if report.released_at.is_some() || report.generation_id.is_some() {
        Some(format!(
            "released_at={} | released_at_source={} | generation_id={}",
            report
                .released_at
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "null".to_string()),
            report
                .released_at_source
                .as_ref()
                .map(serialize_enum)
                .unwrap_or_else(|| "null".to_string()),
            report
                .generation_id
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ))
    } else {
        report
            .persisted_release_artifact_file_name
            .as_ref()
            .map(|file_name| format!("ambiguous_file_name={file_name}"))
    }
}

fn classify_pointer_relation(
    pointer_report: &activation_artifact_release_publish_report::ArtifactReleasePublishReport,
    present_in_archive: bool,
    present_in_history: bool,
    matches_latest_archive: bool,
    matches_latest_history: bool,
    no_history_records: bool,
) -> LatestReleasePointerRelation {
    use activation_artifact_release_publish_report::ArtifactReleaseReportPublisherVerdict::{
        ArtifactReleaseReportInvalidMetadata, ArtifactReleaseReportVerifyAmbiguousTimestamp,
        ArtifactReleaseReportVerifyMissingTarget, ArtifactReleaseReportVerifyOk,
    };

    match pointer_report.verdict {
        ArtifactReleaseReportInvalidMetadata => LatestReleasePointerRelation::InvalidMetadata,
        ArtifactReleaseReportVerifyAmbiguousTimestamp => {
            LatestReleasePointerRelation::AmbiguousTimestamp
        }
        ArtifactReleaseReportVerifyMissingTarget if !pointer_report.latest_pointer_exists => {
            LatestReleasePointerRelation::NoPointerMetadata
        }
        ArtifactReleaseReportVerifyMissingTarget => LatestReleasePointerRelation::MissingTarget,
        ArtifactReleaseReportVerifyOk if !present_in_archive => {
            LatestReleasePointerRelation::MissingFromArchive
        }
        ArtifactReleaseReportVerifyOk if !present_in_history && no_history_records => {
            LatestReleasePointerRelation::MatchesLatestArchiveOnly
        }
        ArtifactReleaseReportVerifyOk if !present_in_history && matches_latest_archive => {
            LatestReleasePointerRelation::AheadOfHistory
        }
        ArtifactReleaseReportVerifyOk if !present_in_history => {
            LatestReleasePointerRelation::MissingFromHistory
        }
        ArtifactReleaseReportVerifyOk if !matches_latest_archive => {
            LatestReleasePointerRelation::BehindLatestArchive
        }
        ArtifactReleaseReportVerifyOk if matches_latest_archive && matches_latest_history => {
            LatestReleasePointerRelation::MatchesLatestArchiveAndHistory
        }
        ArtifactReleaseReportVerifyOk => LatestReleasePointerRelation::MatchesLatestArchiveOnly,
        _ => LatestReleasePointerRelation::InvalidMetadata,
    }
}

fn latest_pointer_metadata_path(latest_pointer_dir: &Path, pointer_name: &str) -> PathBuf {
    latest_pointer_dir.join(format!("{pointer_name}.json"))
}

fn render_human(report: &ReleaseProvenanceReport) -> String {
    [
        "event=copybot_activation_artifact_release_provenance_report".to_string(),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("release_archive_dir={}", report.release_archive_dir),
        format!("latest_pointer_dir={}", report.latest_pointer_dir),
        format!(
            "history_dir={}",
            report
                .history_dir
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!("pointer_name={}", report.pointer_name),
        format!("archive_release_count={}", report.archive_release_count),
        format!("history_release_count={}", report.history_release_count),
        format!(
            "latest_pointer_relation={}",
            serialize_enum(&report.latest_pointer_relation)
        ),
        format!(
            "latest_pointer_selected_generation_id={}",
            report
                .latest_pointer_selected_generation_id
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "latest_archive_generation_id={}",
            report
                .latest_archive_generation_id
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "latest_history_generation_id={}",
            report
                .latest_history_generation_id
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "archive_releases_missing_from_history_count={}",
            report.archive_releases_missing_from_history_count
        ),
        format!(
            "history_releases_missing_from_archive_count={}",
            report.history_releases_missing_from_archive_count
        ),
        format!(
            "ambiguous_timestamp_count={}",
            report.ambiguous_timestamp_count
        ),
        format!("invalid_artifact_count={}", report.invalid_artifact_count),
        format!("release_lineage_only={}", report.release_lineage_only),
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
    fn complete_lineage_across_archive_pointer_and_history_yields_green() {
        let archive_dir = temp_dir("release_provenance_complete_archive");
        let history_dir = temp_dir("release_provenance_complete_history");
        let latest_pointer_dir = temp_dir("release_provenance_complete_pointer");
        let archive_artifact = archive_dir
            .join("release__2026-03-26T12-00-00Z__artifact_release_published_and_promoted.json");
        write_release_artifact(
            &archive_artifact,
            Some("2026-03-26T12:00:00Z"),
            Some("2026-03-26T12:00:00Z|prod_fp|non_prod_fp"),
            None,
            "artifact_release_published_and_promoted",
        );
        fs::copy(
            &archive_artifact,
            history_dir.join(
                "release__2026-03-26T12-00-00Z__artifact_release_published_and_promoted.json",
            ),
        )
        .expect("copy to history");
        write_pointer_metadata(
            &latest_pointer_dir.join(format!(
                "{}.json",
                activation_artifact_release_publish_report::DEFAULT_LATEST_POINTER_NAME
            )),
            &archive_dir,
            &archive_artifact,
            Some("2026-03-26T12:00:00Z"),
            "released_at",
            Some("2026-03-26T12:00:00Z|prod_fp|non_prod_fp"),
            "artifact_release_published_and_promoted",
        );

        let report = build_report(&Config {
            release_archive_dir: archive_dir,
            latest_pointer_dir,
            history_dir: Some(history_dir),
            history_release_paths: Vec::new(),
            pointer_name: activation_artifact_release_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactReleaseProvenanceVerdict::ArtifactReleaseProvenanceComplete
        );
        assert_eq!(
            report.latest_pointer_relation,
            LatestReleasePointerRelation::MatchesLatestArchiveAndHistory
        );
    }

    #[test]
    fn latest_pointer_targeting_missing_release_artifact_yields_inconsistent_lineage() {
        let archive_dir = temp_dir("release_provenance_missing_target_archive");
        let history_dir = temp_dir("release_provenance_missing_target_history");
        let latest_pointer_dir = temp_dir("release_provenance_missing_target_pointer");
        let missing_target = archive_dir.join("missing_release.json");
        write_pointer_metadata(
            &latest_pointer_dir.join(format!(
                "{}.json",
                activation_artifact_release_publish_report::DEFAULT_LATEST_POINTER_NAME
            )),
            &archive_dir,
            &missing_target,
            Some("2026-03-26T12:00:00Z"),
            "released_at",
            Some("2026-03-26T12:00:00Z|prod_fp|non_prod_fp"),
            "artifact_release_published",
        );

        let report = build_report(&Config {
            release_archive_dir: archive_dir,
            latest_pointer_dir,
            history_dir: Some(history_dir),
            history_release_paths: Vec::new(),
            pointer_name: activation_artifact_release_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactReleaseProvenanceVerdict::ArtifactReleaseProvenanceInconsistentLineage
        );
        assert_eq!(
            report.latest_pointer_relation,
            LatestReleasePointerRelation::MissingTarget
        );
    }

    #[test]
    fn archive_releases_missing_from_history_yield_incomplete_provenance() {
        let archive_dir = temp_dir("release_provenance_incomplete_archive");
        let history_dir = temp_dir("release_provenance_incomplete_history");
        let latest_pointer_dir = temp_dir("release_provenance_incomplete_pointer");
        let archive_artifact =
            archive_dir.join("release__2026-03-26T12-00-00Z__artifact_release_published.json");
        write_release_artifact(
            &archive_artifact,
            Some("2026-03-26T12:00:00Z"),
            Some("2026-03-26T12:00:00Z|prod_fp|non_prod_fp"),
            None,
            "artifact_release_published",
        );
        write_pointer_metadata(
            &latest_pointer_dir.join(format!(
                "{}.json",
                activation_artifact_release_publish_report::DEFAULT_LATEST_POINTER_NAME
            )),
            &archive_dir,
            &archive_artifact,
            Some("2026-03-26T12:00:00Z"),
            "released_at",
            Some("2026-03-26T12:00:00Z|prod_fp|non_prod_fp"),
            "artifact_release_published",
        );

        let report = build_report(&Config {
            release_archive_dir: archive_dir,
            latest_pointer_dir,
            history_dir: Some(history_dir),
            history_release_paths: Vec::new(),
            pointer_name: activation_artifact_release_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactReleaseProvenanceVerdict::ArtifactReleaseProvenanceIncomplete
        );
        assert_eq!(report.archive_releases_missing_from_history_count, 1);
    }

    #[test]
    fn malformed_pointer_metadata_or_release_artifact_is_reported_as_invalid() {
        let archive_dir = temp_dir("release_provenance_invalid_archive");
        let history_dir = temp_dir("release_provenance_invalid_history");
        let latest_pointer_dir = temp_dir("release_provenance_invalid_pointer");
        fs::write(archive_dir.join("bad.json"), "{not json").expect("write invalid archive json");
        fs::write(
            latest_pointer_dir.join(format!(
                "{}.json",
                activation_artifact_release_publish_report::DEFAULT_LATEST_POINTER_NAME
            )),
            "{not json",
        )
        .expect("write invalid pointer json");

        let report = build_report(&Config {
            release_archive_dir: archive_dir,
            latest_pointer_dir,
            history_dir: Some(history_dir),
            history_release_paths: Vec::new(),
            pointer_name: activation_artifact_release_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactReleaseProvenanceVerdict::ArtifactReleaseProvenanceInvalidArtifactsPresent
        );
        assert!(report.invalid_artifact_count >= 2);
    }

    #[test]
    fn ambiguous_timestamp_release_lineage_is_not_treated_as_clean_green() {
        let archive_dir = temp_dir("release_provenance_ambiguous_archive");
        let history_dir = temp_dir("release_provenance_ambiguous_history");
        let latest_pointer_dir = temp_dir("release_provenance_ambiguous_pointer");
        let archive_artifact = archive_dir.join("legacy_release.json");
        write_release_artifact(
            &archive_artifact,
            None,
            None,
            None,
            "artifact_release_published",
        );
        fs::copy(&archive_artifact, history_dir.join("legacy_release.json")).expect("copy history");
        write_pointer_metadata(
            &latest_pointer_dir.join(format!(
                "{}.json",
                activation_artifact_release_publish_report::DEFAULT_LATEST_POINTER_NAME
            )),
            &archive_dir,
            &archive_artifact,
            None,
            "missing_deterministic_timestamp",
            None,
            "artifact_release_published",
        );

        let report = build_report(&Config {
            release_archive_dir: archive_dir,
            latest_pointer_dir,
            history_dir: Some(history_dir),
            history_release_paths: Vec::new(),
            pointer_name: activation_artifact_release_publish_report::DEFAULT_LATEST_POINTER_NAME
                .to_string(),
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactReleaseProvenanceVerdict::ArtifactReleaseProvenanceAmbiguousTimestamp
        );
        assert_eq!(report.ambiguous_timestamp_count, 1);
    }

    fn write_release_artifact(
        path: &Path,
        released_at: Option<&str>,
        generation_id: Option<&str>,
        generation_directory: Option<&str>,
        verdict: &str,
    ) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create parent");
        }
        let released_at_value = released_at.map(|value| json!(value)).unwrap_or(json!(null));
        let generation_directory =
            generation_directory
                .map(|value| value.to_string())
                .or_else(|| {
                    generation_id.map(|value| {
                        let stamp = value.split('|').next().unwrap_or("2026-03-26T12:00:00Z");
                        format!("/tmp/{}__generation", stamp.replace(':', "-"))
                    })
                });
        let payload = json!({
            "mode": "artifact_release",
            "released_at": released_at_value,
            "verdict": verdict,
            "reason": format!("release {verdict}"),
            "config_path": "/etc/solana-copy-bot/live.server.toml",
            "non_prod_config_path": "/etc/solana-copy-bot/devnet.server.toml",
            "archive_dir": "/var/www/solana-copy-bot/state/activation_artifacts/archive",
            "generation_id": generation_id,
            "generation_directory": generation_directory,
            "packet_path": null,
            "runbook_json_path": null,
            "runbook_markdown_path": null,
            "manifest_path": null,
            "bundle_path": null,
            "decision_packet_verdict": "decision_packet_blocked",
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
            serde_json::to_string_pretty(&payload).expect("serialize release"),
        )
        .expect("write release");
    }

    fn write_pointer_metadata(
        path: &Path,
        release_archive_dir: &Path,
        selected_release_artifact_path: &Path,
        released_at: Option<&str>,
        released_at_source: &str,
        generation_id: Option<&str>,
        release_verdict: &str,
    ) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create pointer dir");
        }
        let canonical_archive = fs::canonicalize(release_archive_dir).unwrap_or_else(|_| {
            fs::create_dir_all(release_archive_dir).expect("create release archive dir");
            fs::canonicalize(release_archive_dir).expect("canonical archive dir")
        });
        let canonical_selected_path = if selected_release_artifact_path.exists() {
            fs::canonicalize(selected_release_artifact_path).expect("canonical selected path")
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
            "selected_release_artifact_path": canonical_selected_path.display().to_string(),
            "selected_release_artifact_file_name": canonical_selected_path.file_name().map(|value| value.to_string_lossy().into_owned()).unwrap_or_else(|| "release.json".to_string()),
            "release_artifact_mode": "artifact_release",
            "released_at": released_at.map(|value| json!(value)).unwrap_or(json!(null)),
            "released_at_source": released_at_source,
            "compat_loaded_without_released_at": released_at_source != "released_at",
            "deterministic_timestamp_available": released_at.is_some(),
            "ordered_history_confident": released_at.is_some(),
            "release_verdict": release_verdict,
            "release_reason": format!("release {release_verdict}"),
            "selected_generation_id": generation_id,
            "channel_promotion_happened": false,
            "channel_metadata_path": null,
            "pointed_at": "2026-03-26T12:10:00Z",
            "build_version": env!("CARGO_PKG_VERSION"),
            "git_commit": "deadbeef"
        });
        fs::write(
            path,
            serde_json::to_string_pretty(&payload).expect("serialize pointer"),
        )
        .expect("write pointer");
    }

    fn temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("{prefix}_{nanos}"));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }
}
