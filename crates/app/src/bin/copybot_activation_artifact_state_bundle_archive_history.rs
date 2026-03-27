#![recursion_limit = "256"]

use anyhow::{anyhow, bail, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[allow(dead_code)]
#[path = "copybot_activation_artifact_state_bundle.rs"]
mod activation_artifact_state_bundle;
#[allow(dead_code)]
#[path = "copybot_activation_artifact_state_bundle_publish_report.rs"]
mod activation_artifact_state_bundle_publish_report;

const USAGE: &str = "usage: copybot_activation_artifact_state_bundle_archive_history [--bundle-archive-dir <path>] [--bundle-latest-pointer-dir <path>] [--pointer-name <name>] [--json] (--history | --compare <older-bundle> <newer-bundle>)";

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
    bundle_archive_dir: Option<PathBuf>,
    bundle_latest_pointer_dir: Option<PathBuf>,
    pointer_name: String,
    json: bool,
    mode: Mode,
}

#[derive(Debug, Clone)]
enum Mode {
    History,
    Compare { older: PathBuf, newer: PathBuf },
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ArtifactStateBundleArchiveHistoryVerdict {
    ArtifactStateBundleArchiveHistoryLatestClean,
    ArtifactStateBundleArchiveHistoryLatestNonGreen,
    ArtifactStateBundleArchiveHistoryInvalidArtifactsPresent,
    ArtifactStateBundleArchiveHistoryInsufficientArtifacts,
    ArtifactStateBundleArchiveCompareReady,
    ArtifactStateBundleArchiveCompareInvalidArtifact,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum IssueDirection {
    NarrowedIssues,
    WidenedIssues,
    UnchangedIssues,
}

#[derive(Debug, Clone)]
struct LoadedArchivedBundleRecord {
    bundle_root: PathBuf,
    bundle_dir_name: String,
    summary: activation_artifact_state_bundle::StateSnapshotBundleSummary,
}

#[derive(Debug, Clone, Serialize)]
struct InvalidArchivedBundleSummary {
    path: String,
    bundle_verdict: String,
    reason: String,
}

#[derive(Debug, Clone, Serialize)]
struct ArchivedBundleSummary {
    path: String,
    dir_name: String,
    bundle_integrity_verdict: String,
    bundle_integrity_reason: String,
    snapshotted_at: Option<DateTime<Utc>>,
    selected_snapshot_state_verdict: Option<String>,
    selected_snapshot_state_reason: Option<String>,
    selected_review_generation_id: Option<String>,
    selected_latest_release_generation_id: Option<String>,
    ambiguous_legacy_count: Option<usize>,
    coherent_for_review_operations: Option<bool>,
    latest_by_archive_order: bool,
    selected_by_latest_pointer: bool,
}

#[derive(Debug, Clone, Serialize)]
struct LatestBundlePointerSummary {
    configured: bool,
    pointer_verdict: String,
    pointer_reason: String,
    pointer_path: Option<String>,
    selected_bundle_path: Option<String>,
    selected_snapshot_path: Option<String>,
    selected_snapshot_state_verdict: Option<String>,
    selected_snapshot_state_reason: Option<String>,
    selected_review_generation_id: Option<String>,
    selected_latest_release_generation_id: Option<String>,
    selected_ambiguous_legacy_count: Option<usize>,
    selected_coherent_for_review_operations: Option<bool>,
    target_exists: bool,
    target_matches_identity: bool,
    selected_bundle_loaded: bool,
    selected_bundle_matches_latest_by_archive: Option<bool>,
    selected_bundle_stale: Option<bool>,
    selected_bundle_non_green: Option<bool>,
    selected_bundle_ambiguous: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
struct ArchivedBundleCompareSummary {
    path: String,
    dir_name: String,
    bundle_integrity_verdict: String,
    bundle_integrity_reason: String,
    snapshotted_at: Option<DateTime<Utc>>,
    selected_snapshot_state_verdict: Option<String>,
    selected_snapshot_state_reason: Option<String>,
    selected_review_generation_id: Option<String>,
    selected_latest_release_generation_id: Option<String>,
    ambiguous_legacy_count: Option<usize>,
    coherent_for_review_operations: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
struct ArchivedBundleHistoryReport {
    mode: String,
    verdict: ArtifactStateBundleArchiveHistoryVerdict,
    reason: String,
    bundle_archive_dir: String,
    bundle_latest_pointer_dir: Option<String>,
    pointer_name: String,
    archived_bundle_count: usize,
    valid_archived_bundle_count: usize,
    invalid_or_drifted_bundle_count: usize,
    coherent_snapshot_state_count: usize,
    incomplete_snapshot_state_count: usize,
    inconsistent_snapshot_state_count: usize,
    ambiguous_snapshot_state_count: usize,
    archive_evidence_sparse: bool,
    latest_archived_bundle: Option<ArchivedBundleSummary>,
    latest_pointer: LatestBundlePointerSummary,
    invalid_or_drifted_bundles: Vec<InvalidArchivedBundleSummary>,
    archived_bundles: Vec<ArchivedBundleSummary>,
    archived_bundle_history_only: bool,
    execution_untouched: bool,
    activation_authorized: bool,
    not_authorized_summary: String,
}

#[derive(Debug, Clone, Serialize)]
struct ArchivedBundleCompareReport {
    mode: String,
    verdict: ArtifactStateBundleArchiveHistoryVerdict,
    reason: String,
    bundle_archive_dir: Option<String>,
    bundle_latest_pointer_dir: Option<String>,
    pointer_name: String,
    older_bundle: Option<ArchivedBundleCompareSummary>,
    newer_bundle: Option<ArchivedBundleCompareSummary>,
    bundle_integrity_changed: bool,
    snapshot_state_verdict_changed: bool,
    snapshot_state_reason_changed: bool,
    selected_review_generation_changed: bool,
    selected_latest_release_generation_changed: bool,
    ambiguous_legacy_count_delta: Option<i64>,
    coherent_for_review_operations_changed: bool,
    issue_direction: Option<IssueDirection>,
    latest_pointer: LatestBundlePointerSummary,
    older_matches_latest_pointer: Option<bool>,
    newer_matches_latest_pointer: Option<bool>,
    invalid_artifacts: Vec<InvalidArchivedBundleSummary>,
    archived_bundle_history_only: bool,
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
    let mut pointer_name =
        activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME.to_string();
    let mut json = false;
    let mut history = false;
    let mut compare: Option<(PathBuf, PathBuf)> = None;

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
            "--pointer-name" => {
                pointer_name = parse_name(parse_string_arg("--pointer-name", args.next())?)?
            }
            "--history" => history = true,
            "--compare" => {
                let older = PathBuf::from(parse_string_arg("--compare", args.next())?);
                let newer = PathBuf::from(parse_string_arg("--compare", args.next())?);
                compare = Some((older, newer));
            }
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown arg `{other}`"),
        }
    }

    let mode_count = history as usize + compare.is_some() as usize;
    if mode_count != 1 {
        bail!("choose exactly one of --history or --compare <older-bundle> <newer-bundle>");
    }

    let mode = if history {
        Mode::History
    } else {
        let (older, newer) = compare.expect("compare mode");
        Mode::Compare { older, newer }
    };

    if matches!(mode, Mode::History) && bundle_archive_dir.is_none() {
        bail!("--history requires --bundle-archive-dir <path>");
    }
    if bundle_latest_pointer_dir.is_some() && bundle_archive_dir.is_none() {
        bail!("--bundle-latest-pointer-dir requires --bundle-archive-dir <path>");
    }

    Ok(Some(Config {
        bundle_archive_dir,
        bundle_latest_pointer_dir,
        pointer_name,
        json,
        mode,
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
        bail!("pointer name may only contain ascii alphanumeric characters, '-' or '_'");
    }
}

fn run(config: Config) -> Result<String> {
    match &config.mode {
        Mode::History => {
            let report = build_history_report(&config)?;
            if config.json {
                Ok(serde_json::to_string_pretty(&report)?)
            } else {
                Ok(render_history_human(&report))
            }
        }
        Mode::Compare { older, newer } => {
            let report = build_compare_report(&config, older, newer)?;
            if config.json {
                Ok(serde_json::to_string_pretty(&report)?)
            } else {
                Ok(render_compare_human(&report))
            }
        }
    }
}

fn build_history_report(config: &Config) -> Result<ArchivedBundleHistoryReport> {
    let bundle_archive_dir = config
        .bundle_archive_dir
        .as_deref()
        .ok_or_else(|| anyhow!("--history requires --bundle-archive-dir <path>"))?;
    let bundle_paths = activation_artifact_state_bundle::collect_state_snapshot_bundle_paths(
        Some(bundle_archive_dir),
        &[],
    )?;
    let (records, invalid_bundles) = load_bundle_records(&bundle_paths);
    let latest_archived_bundle = latest_bundle_record(&records);
    let pointer_context = inspect_pointer_context(
        bundle_archive_dir,
        config.bundle_latest_pointer_dir.as_deref(),
        &config.pointer_name,
        latest_archived_bundle,
        &records,
    );

    let archived_bundles = records
        .iter()
        .map(|record| {
            let latest_by_archive_order = latest_archived_bundle
                .is_some_and(|latest| latest.bundle_root == record.bundle_root);
            let selected_by_latest_pointer = pointer_context
                .selected_bundle_canonical_path
                .as_ref()
                .is_some_and(|selected| selected == &record.bundle_root);
            summarize_record(record, latest_by_archive_order, selected_by_latest_pointer)
        })
        .collect::<Vec<_>>();

    let coherent_snapshot_state_count = records
        .iter()
        .filter(|record| {
            record.summary.selected_snapshot_state_verdict.as_deref()
                == Some("artifact_state_coherent")
        })
        .count();
    let incomplete_snapshot_state_count = records
        .iter()
        .filter(|record| {
            record.summary.selected_snapshot_state_verdict.as_deref()
                == Some("artifact_state_incomplete")
        })
        .count();
    let inconsistent_snapshot_state_count = records
        .iter()
        .filter(|record| {
            record.summary.selected_snapshot_state_verdict.as_deref()
                == Some("artifact_state_inconsistent")
        })
        .count();
    let ambiguous_snapshot_state_count = records
        .iter()
        .filter(|record| {
            record.summary.selected_snapshot_state_verdict.as_deref()
                == Some("artifact_state_ambiguous_legacy_state")
        })
        .count();
    let archive_evidence_sparse = records.len() < 2;

    let latest_state_verdict = latest_archived_bundle
        .and_then(|record| record.summary.selected_snapshot_state_verdict.as_deref());
    let pointer_selected_non_green = pointer_context
        .summary
        .selected_bundle_non_green
        .unwrap_or(false);
    let pointer_selected_ambiguous = pointer_context
        .summary
        .selected_bundle_ambiguous
        .unwrap_or(false);
    let pointer_broken = pointer_context.summary.configured
        && pointer_context.summary.pointer_verdict != "artifact_state_bundle_report_verify_ok";

    let (verdict, reason) = if !invalid_bundles.is_empty() {
        (
            ArtifactStateBundleArchiveHistoryVerdict::ArtifactStateBundleArchiveHistoryInvalidArtifactsPresent,
            format!(
                "archived bundle history contains {} invalid or drifted bundle(s)",
                invalid_bundles.len()
            ),
        )
    } else if records.is_empty() {
        (
            ArtifactStateBundleArchiveHistoryVerdict::ArtifactStateBundleArchiveHistoryInsufficientArtifacts,
            "no valid deterministic archived bundles were found in the bundle archive"
                .to_string(),
        )
    } else if pointer_broken {
        (
            ArtifactStateBundleArchiveHistoryVerdict::ArtifactStateBundleArchiveHistoryLatestNonGreen,
            "the latest bundle pointer does not resolve cleanly against the archived bundle surface"
                .to_string(),
        )
    } else if pointer_selected_non_green || pointer_selected_ambiguous {
        (
            ArtifactStateBundleArchiveHistoryVerdict::ArtifactStateBundleArchiveHistoryLatestNonGreen,
            "the latest bundle pointer selects an archived bundle whose embedded snapshot truth remains non-green or ambiguous".to_string(),
        )
    } else if latest_state_verdict.is_some_and(is_non_green_or_ambiguous_verdict) {
        (
            ArtifactStateBundleArchiveHistoryVerdict::ArtifactStateBundleArchiveHistoryLatestNonGreen,
            "the latest archived bundle preserves non-green or ambiguous snapshot truth"
                .to_string(),
        )
    } else {
        (
            ArtifactStateBundleArchiveHistoryVerdict::ArtifactStateBundleArchiveHistoryLatestClean,
            if archive_evidence_sparse {
                "the latest archived bundle is clean, but archived-bundle history remains sparse"
                    .to_string()
            } else {
                "the latest archived bundle verifies cleanly and preserves coherent snapshot truth"
                    .to_string()
            },
        )
    };

    Ok(ArchivedBundleHistoryReport {
        mode: "artifact_state_bundle_archive_history".to_string(),
        verdict,
        reason,
        bundle_archive_dir: bundle_archive_dir.display().to_string(),
        bundle_latest_pointer_dir: config
            .bundle_latest_pointer_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        pointer_name: config.pointer_name.clone(),
        archived_bundle_count: bundle_paths.len(),
        valid_archived_bundle_count: records.len(),
        invalid_or_drifted_bundle_count: invalid_bundles.len(),
        coherent_snapshot_state_count,
        incomplete_snapshot_state_count,
        inconsistent_snapshot_state_count,
        ambiguous_snapshot_state_count,
        archive_evidence_sparse,
        latest_archived_bundle: latest_archived_bundle
            .map(|record| summarize_record(record, true, false)),
        latest_pointer: pointer_context.summary,
        invalid_or_drifted_bundles: invalid_bundles,
        archived_bundles,
        archived_bundle_history_only: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary:
            "Archived state-bundle history only summarizes deterministic archived bundles and optional latest pointer metadata. It does not authorize activation, does not enable execution, and does not override the Stage 3 production gate.".to_string(),
    })
}

fn build_compare_report(
    config: &Config,
    older: &Path,
    newer: &Path,
) -> Result<ArchivedBundleCompareReport> {
    let older_record = load_single_bundle_record(older);
    let newer_record = load_single_bundle_record(newer);
    let latest_record = if let Some(bundle_archive_dir) = config.bundle_archive_dir.as_deref() {
        let bundle_paths = activation_artifact_state_bundle::collect_state_snapshot_bundle_paths(
            Some(bundle_archive_dir),
            &[],
        )?;
        let (records, _) = load_bundle_records(&bundle_paths);
        latest_bundle_record(&records).cloned()
    } else {
        None
    };
    let pointer_context = if let Some(bundle_archive_dir) = config.bundle_archive_dir.as_deref() {
        inspect_pointer_context(
            bundle_archive_dir,
            config.bundle_latest_pointer_dir.as_deref(),
            &config.pointer_name,
            latest_record.as_ref(),
            &[],
        )
    } else {
        default_pointer_context(false)
    };

    let mut invalid_artifacts = Vec::new();
    let older_valid = match older_record {
        Ok(record) => Some(record),
        Err(issue) => {
            invalid_artifacts.push(issue);
            None
        }
    };
    let newer_valid = match newer_record {
        Ok(record) => Some(record),
        Err(issue) => {
            invalid_artifacts.push(issue);
            None
        }
    };

    if !invalid_artifacts.is_empty() {
        return Ok(ArchivedBundleCompareReport {
            mode: "artifact_state_bundle_archive_compare".to_string(),
            verdict:
                ArtifactStateBundleArchiveHistoryVerdict::ArtifactStateBundleArchiveCompareInvalidArtifact,
            reason:
                "archived bundle compare could not load one or more bundle inputs cleanly"
                    .to_string(),
            bundle_archive_dir: config
                .bundle_archive_dir
                .as_ref()
                .map(|path| path.display().to_string()),
            bundle_latest_pointer_dir: config
                .bundle_latest_pointer_dir
                .as_ref()
                .map(|path| path.display().to_string()),
            pointer_name: config.pointer_name.clone(),
            older_bundle: None,
            newer_bundle: None,
            bundle_integrity_changed: false,
            snapshot_state_verdict_changed: false,
            snapshot_state_reason_changed: false,
            selected_review_generation_changed: false,
            selected_latest_release_generation_changed: false,
            ambiguous_legacy_count_delta: None,
            coherent_for_review_operations_changed: false,
            issue_direction: None,
            latest_pointer: pointer_context.summary,
            older_matches_latest_pointer: None,
            newer_matches_latest_pointer: None,
            invalid_artifacts,
            archived_bundle_history_only: true,
            execution_untouched: true,
            activation_authorized: false,
            not_authorized_summary:
                "Archived state-bundle compare only inspects deterministic archived bundle artifacts and optional latest pointer metadata. It does not authorize activation, does not enable execution, and does not override the Stage 3 production gate.".to_string(),
        });
    }

    let older_valid = older_valid.expect("older valid");
    let newer_valid = newer_valid.expect("newer valid");
    let older_summary = summarize_compare_record(&older_valid);
    let newer_summary = summarize_compare_record(&newer_valid);
    let bundle_integrity_changed = older_summary.bundle_integrity_verdict
        != newer_summary.bundle_integrity_verdict
        || older_summary.bundle_integrity_reason != newer_summary.bundle_integrity_reason;
    let snapshot_state_verdict_changed = older_summary.selected_snapshot_state_verdict
        != newer_summary.selected_snapshot_state_verdict;
    let snapshot_state_reason_changed = older_summary.selected_snapshot_state_reason
        != newer_summary.selected_snapshot_state_reason;
    let selected_review_generation_changed =
        older_summary.selected_review_generation_id != newer_summary.selected_review_generation_id;
    let selected_latest_release_generation_changed = older_summary
        .selected_latest_release_generation_id
        != newer_summary.selected_latest_release_generation_id;
    let ambiguous_legacy_count_delta = newer_summary
        .ambiguous_legacy_count
        .zip(older_summary.ambiguous_legacy_count)
        .map(|(newer, older)| newer as i64 - older as i64);
    let coherent_for_review_operations_changed = older_summary.coherent_for_review_operations
        != newer_summary.coherent_for_review_operations;
    let issue_direction = Some(compare_issue_direction(&older_valid, &newer_valid));
    let older_matches_latest_pointer = pointer_context
        .selected_bundle_canonical_path
        .as_ref()
        .map(|selected| *selected == older_valid.bundle_root);
    let newer_matches_latest_pointer = pointer_context
        .selected_bundle_canonical_path
        .as_ref()
        .map(|selected| *selected == newer_valid.bundle_root);

    Ok(ArchivedBundleCompareReport {
        mode: "artifact_state_bundle_archive_compare".to_string(),
        verdict: ArtifactStateBundleArchiveHistoryVerdict::ArtifactStateBundleArchiveCompareReady,
        reason:
            "archived bundle compare is ready; bundle integrity and embedded snapshot truth drift are explicit".to_string(),
        bundle_archive_dir: config
            .bundle_archive_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        bundle_latest_pointer_dir: config
            .bundle_latest_pointer_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        pointer_name: config.pointer_name.clone(),
        older_bundle: Some(older_summary),
        newer_bundle: Some(newer_summary),
        bundle_integrity_changed,
        snapshot_state_verdict_changed,
        snapshot_state_reason_changed,
        selected_review_generation_changed,
        selected_latest_release_generation_changed,
        ambiguous_legacy_count_delta,
        coherent_for_review_operations_changed,
        issue_direction,
        latest_pointer: pointer_context.summary,
        older_matches_latest_pointer,
        newer_matches_latest_pointer,
        invalid_artifacts,
        archived_bundle_history_only: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary:
            "Archived state-bundle compare only inspects deterministic archived bundle artifacts and optional latest pointer metadata. It does not authorize activation, does not enable execution, and does not override the Stage 3 production gate.".to_string(),
    })
}

fn load_bundle_records(
    paths: &[PathBuf],
) -> (
    Vec<LoadedArchivedBundleRecord>,
    Vec<InvalidArchivedBundleSummary>,
) {
    let mut valid = Vec::new();
    let mut invalid = Vec::new();
    for path in paths {
        match load_single_bundle_record(path) {
            Ok(record) => valid.push(record),
            Err(issue) => invalid.push(issue),
        }
    }
    valid.sort_by(|left, right| {
        left.bundle_dir_name
            .cmp(&right.bundle_dir_name)
            .then_with(|| left.bundle_root.cmp(&right.bundle_root))
    });
    (valid, invalid)
}

fn load_single_bundle_record(
    path: &Path,
) -> std::result::Result<LoadedArchivedBundleRecord, InvalidArchivedBundleSummary> {
    match activation_artifact_state_bundle::inspect_state_snapshot_bundle(path) {
        Ok(summary) if summary.verdict == "artifact_state_bundle_verified" => {
            let bundle_root = fs::canonicalize(Path::new(&summary.bundle_path))
                .unwrap_or_else(|_| PathBuf::from(&summary.bundle_path));
            let bundle_dir_name = bundle_root
                .file_name()
                .map(|value| value.to_string_lossy().into_owned())
                .unwrap_or_else(|| summary.bundle_path.clone());
            Ok(LoadedArchivedBundleRecord {
                bundle_root,
                bundle_dir_name,
                summary,
            })
        }
        Ok(summary) => Err(InvalidArchivedBundleSummary {
            path: summary.bundle_path,
            bundle_verdict: summary.verdict,
            reason: summary.reason,
        }),
        Err(error) => Err(InvalidArchivedBundleSummary {
            path: path.display().to_string(),
            bundle_verdict: "artifact_state_bundle_invalid".to_string(),
            reason: format!("{error:#}"),
        }),
    }
}

fn latest_bundle_record(
    records: &[LoadedArchivedBundleRecord],
) -> Option<&LoadedArchivedBundleRecord> {
    records.iter().max_by(|left, right| {
        left.bundle_dir_name
            .cmp(&right.bundle_dir_name)
            .then_with(|| left.bundle_root.cmp(&right.bundle_root))
    })
}

struct PointerContext {
    summary: LatestBundlePointerSummary,
    selected_bundle_canonical_path: Option<PathBuf>,
}

fn inspect_pointer_context(
    bundle_archive_dir: &Path,
    latest_pointer_dir: Option<&Path>,
    pointer_name: &str,
    latest_archived_bundle: Option<&LoadedArchivedBundleRecord>,
    loaded_records: &[LoadedArchivedBundleRecord],
) -> PointerContext {
    let Some(latest_pointer_dir) = latest_pointer_dir else {
        return default_pointer_context(false);
    };

    let report =
        activation_artifact_state_bundle_publish_report::inspect_latest_bundle_pointer_report(
            bundle_archive_dir,
            latest_pointer_dir,
            pointer_name,
            true,
        );
    let metadata =
        activation_artifact_state_bundle_publish_report::inspect_latest_bundle_pointer_metadata(
            latest_pointer_dir,
            pointer_name,
        );

    match (report, metadata) {
        (Ok(report), Ok(metadata)) => {
            let selected_bundle_canonical_path = metadata.as_ref().and_then(|loaded| {
                fs::canonicalize(Path::new(&loaded.metadata.selected_bundle_path)).ok()
            });
            let selected_bundle_loaded = if let Some(selected) =
                selected_bundle_canonical_path.as_ref()
            {
                loaded_records
                    .iter()
                    .any(|record| &record.bundle_root == selected)
                    || latest_archived_bundle.is_some_and(|record| &record.bundle_root == selected)
            } else {
                false
            };
            let selected_bundle_matches_latest_by_archive = selected_bundle_canonical_path
                .as_ref()
                .zip(latest_archived_bundle)
                .map(|(selected, latest)| selected == &latest.bundle_root);
            let selected_bundle_stale =
                selected_bundle_matches_latest_by_archive.map(|matches| !matches);
            let selected_snapshot_state_verdict = metadata
                .as_ref()
                .map(|loaded| loaded.metadata.snapshot_state_verdict.clone())
                .or(report.selected_snapshot_state_verdict.clone());
            let selected_bundle_non_green = selected_snapshot_state_verdict
                .as_deref()
                .map(is_hard_non_green_state_verdict);
            let selected_bundle_ambiguous = selected_snapshot_state_verdict
                .as_deref()
                .map(|verdict| verdict == "artifact_state_ambiguous_legacy_state");
            PointerContext {
                summary: LatestBundlePointerSummary {
                    configured: true,
                    pointer_verdict: serialize_enum(&report.verdict),
                    pointer_reason: report.reason.clone(),
                    pointer_path: report.latest_pointer_path.clone().or_else(|| {
                        metadata
                            .as_ref()
                            .map(|loaded| loaded.path.display().to_string())
                    }),
                    selected_bundle_path: report.persisted_bundle_path.clone().or_else(|| {
                        metadata
                            .as_ref()
                            .map(|loaded| loaded.metadata.selected_bundle_path.clone())
                    }),
                    selected_snapshot_path: report.selected_snapshot_path.clone().or_else(|| {
                        metadata
                            .as_ref()
                            .map(|loaded| loaded.metadata.selected_snapshot_source_path.clone())
                    }),
                    selected_snapshot_state_verdict: selected_snapshot_state_verdict.clone(),
                    selected_snapshot_state_reason: metadata
                        .as_ref()
                        .map(|loaded| loaded.metadata.snapshot_state_reason.clone())
                        .or(report.selected_snapshot_state_reason.clone()),
                    selected_review_generation_id: metadata
                        .as_ref()
                        .and_then(|loaded| loaded.metadata.selected_review_generation_id.clone())
                        .or(report.selected_review_generation_id.clone()),
                    selected_latest_release_generation_id: metadata
                        .as_ref()
                        .and_then(|loaded| {
                            loaded
                                .metadata
                                .selected_latest_release_generation_id
                                .clone()
                        })
                        .or(report.selected_latest_release_generation_id.clone()),
                    selected_ambiguous_legacy_count: metadata
                        .as_ref()
                        .map(|loaded| loaded.metadata.ambiguous_legacy_count)
                        .or(report.ambiguous_legacy_count),
                    selected_coherent_for_review_operations: metadata
                        .as_ref()
                        .map(|loaded| loaded.metadata.coherent_for_review_operations)
                        .or(report.coherent_for_review_operations),
                    target_exists: report.latest_pointer_target_exists,
                    target_matches_identity: report.latest_pointer_target_matches_identity,
                    selected_bundle_loaded,
                    selected_bundle_matches_latest_by_archive,
                    selected_bundle_stale,
                    selected_bundle_non_green,
                    selected_bundle_ambiguous,
                },
                selected_bundle_canonical_path,
            }
        }
        (Err(error), _) | (_, Err(error)) => PointerContext {
            summary: LatestBundlePointerSummary {
                configured: true,
                pointer_verdict: "artifact_state_bundle_report_invalid_metadata".to_string(),
                pointer_reason: format!(
                    "latest bundle pointer inspection failed operationally: {error:#}"
                ),
                pointer_path: Some(
                    latest_pointer_dir
                        .join(format!("{pointer_name}.json"))
                        .display()
                        .to_string(),
                ),
                selected_bundle_path: None,
                selected_snapshot_path: None,
                selected_snapshot_state_verdict: None,
                selected_snapshot_state_reason: None,
                selected_review_generation_id: None,
                selected_latest_release_generation_id: None,
                selected_ambiguous_legacy_count: None,
                selected_coherent_for_review_operations: None,
                target_exists: false,
                target_matches_identity: false,
                selected_bundle_loaded: false,
                selected_bundle_matches_latest_by_archive: None,
                selected_bundle_stale: None,
                selected_bundle_non_green: None,
                selected_bundle_ambiguous: None,
            },
            selected_bundle_canonical_path: None,
        },
    }
}

fn default_pointer_context(configured: bool) -> PointerContext {
    PointerContext {
        summary: LatestBundlePointerSummary {
            configured,
            pointer_verdict: if configured {
                "artifact_state_bundle_report_verify_missing_target".to_string()
            } else {
                "no_pointer_configured".to_string()
            },
            pointer_reason: if configured {
                "bundle latest pointer directory was configured, but the pointer could not be inspected"
                    .to_string()
            } else {
                "no bundle latest pointer directory was provided".to_string()
            },
            pointer_path: None,
            selected_bundle_path: None,
            selected_snapshot_path: None,
            selected_snapshot_state_verdict: None,
            selected_snapshot_state_reason: None,
            selected_review_generation_id: None,
            selected_latest_release_generation_id: None,
            selected_ambiguous_legacy_count: None,
            selected_coherent_for_review_operations: None,
            target_exists: false,
            target_matches_identity: false,
            selected_bundle_loaded: false,
            selected_bundle_matches_latest_by_archive: None,
            selected_bundle_stale: None,
            selected_bundle_non_green: None,
            selected_bundle_ambiguous: None,
        },
        selected_bundle_canonical_path: None,
    }
}

fn summarize_record(
    record: &LoadedArchivedBundleRecord,
    latest_by_archive_order: bool,
    selected_by_latest_pointer: bool,
) -> ArchivedBundleSummary {
    ArchivedBundleSummary {
        path: record.bundle_root.display().to_string(),
        dir_name: record.bundle_dir_name.clone(),
        bundle_integrity_verdict: record.summary.verdict.clone(),
        bundle_integrity_reason: record.summary.reason.clone(),
        snapshotted_at: record.summary.snapshotted_at,
        selected_snapshot_state_verdict: record.summary.selected_snapshot_state_verdict.clone(),
        selected_snapshot_state_reason: record.summary.selected_snapshot_state_reason.clone(),
        selected_review_generation_id: record.summary.selected_review_generation_id.clone(),
        selected_latest_release_generation_id: record
            .summary
            .selected_latest_release_generation_id
            .clone(),
        ambiguous_legacy_count: record.summary.ambiguous_legacy_count,
        coherent_for_review_operations: record.summary.coherent_for_review_operations,
        latest_by_archive_order,
        selected_by_latest_pointer,
    }
}

fn summarize_compare_record(record: &LoadedArchivedBundleRecord) -> ArchivedBundleCompareSummary {
    ArchivedBundleCompareSummary {
        path: record.bundle_root.display().to_string(),
        dir_name: record.bundle_dir_name.clone(),
        bundle_integrity_verdict: record.summary.verdict.clone(),
        bundle_integrity_reason: record.summary.reason.clone(),
        snapshotted_at: record.summary.snapshotted_at,
        selected_snapshot_state_verdict: record.summary.selected_snapshot_state_verdict.clone(),
        selected_snapshot_state_reason: record.summary.selected_snapshot_state_reason.clone(),
        selected_review_generation_id: record.summary.selected_review_generation_id.clone(),
        selected_latest_release_generation_id: record
            .summary
            .selected_latest_release_generation_id
            .clone(),
        ambiguous_legacy_count: record.summary.ambiguous_legacy_count,
        coherent_for_review_operations: record.summary.coherent_for_review_operations,
    }
}

fn compare_issue_direction(
    older: &LoadedArchivedBundleRecord,
    newer: &LoadedArchivedBundleRecord,
) -> IssueDirection {
    let older_score = bundle_issue_score(older);
    let newer_score = bundle_issue_score(newer);
    if newer_score < older_score {
        IssueDirection::NarrowedIssues
    } else if newer_score > older_score {
        IssueDirection::WidenedIssues
    } else {
        IssueDirection::UnchangedIssues
    }
}

fn bundle_issue_score(record: &LoadedArchivedBundleRecord) -> u8 {
    let verdict = record
        .summary
        .selected_snapshot_state_verdict
        .as_deref()
        .unwrap_or("");
    match verdict {
        "artifact_state_coherent" => 0,
        "artifact_state_ambiguous_legacy_state" => 1,
        "artifact_state_incomplete" => 2,
        "artifact_state_inconsistent" => 3,
        "artifact_state_invalid_artifacts_present" => 4,
        _ => 5,
    }
}

fn is_hard_non_green_state_verdict(verdict: &str) -> bool {
    matches!(
        verdict,
        "artifact_state_incomplete"
            | "artifact_state_inconsistent"
            | "artifact_state_invalid_artifacts_present"
    )
}

fn is_non_green_or_ambiguous_verdict(verdict: &str) -> bool {
    is_hard_non_green_state_verdict(verdict) || verdict == "artifact_state_ambiguous_legacy_state"
}

fn serialize_enum<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value)
        .unwrap_or_default()
        .trim_matches('"')
        .to_string()
}

fn render_history_human(report: &ArchivedBundleHistoryReport) -> String {
    [
        "event=copybot_activation_artifact_state_bundle_archive_history".to_string(),
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
            "coherent_snapshot_state_count={}",
            report.coherent_snapshot_state_count
        ),
        format!(
            "incomplete_snapshot_state_count={}",
            report.incomplete_snapshot_state_count
        ),
        format!(
            "inconsistent_snapshot_state_count={}",
            report.inconsistent_snapshot_state_count
        ),
        format!(
            "ambiguous_snapshot_state_count={}",
            report.ambiguous_snapshot_state_count
        ),
        format!("archive_evidence_sparse={}", report.archive_evidence_sparse),
        format!(
            "latest_archived_bundle_path={}",
            report
                .latest_archived_bundle
                .as_ref()
                .map(|bundle| bundle.path.clone())
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!(
            "latest_bundle_pointer_verdict={}",
            report.latest_pointer.pointer_verdict
        ),
        format!(
            "latest_bundle_pointer_selected_bundle_path={}",
            report
                .latest_pointer
                .selected_bundle_path
                .clone()
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!(
            "latest_bundle_pointer_selected_bundle_matches_latest_by_archive={}",
            report
                .latest_pointer
                .selected_bundle_matches_latest_by_archive
                .map(|value| value.to_string())
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!(
            "latest_bundle_pointer_selected_bundle_non_green={}",
            report
                .latest_pointer
                .selected_bundle_non_green
                .map(|value| value.to_string())
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!(
            "latest_bundle_pointer_selected_bundle_ambiguous={}",
            report
                .latest_pointer
                .selected_bundle_ambiguous
                .map(|value| value.to_string())
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!(
            "archived_bundle_history_only={}",
            report.archived_bundle_history_only
        ),
        format!("execution_untouched={}", report.execution_untouched),
        format!("activation_authorized={}", report.activation_authorized),
        format!("not_authorized_summary={}", report.not_authorized_summary),
    ]
    .join("\n")
}

fn render_compare_human(report: &ArchivedBundleCompareReport) -> String {
    [
        "event=copybot_activation_artifact_state_bundle_archive_compare".to_string(),
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!(
            "older_bundle_path={}",
            report
                .older_bundle
                .as_ref()
                .map(|bundle| bundle.path.clone())
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!(
            "newer_bundle_path={}",
            report
                .newer_bundle
                .as_ref()
                .map(|bundle| bundle.path.clone())
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!(
            "snapshot_state_verdict_changed={}",
            report.snapshot_state_verdict_changed
        ),
        format!(
            "selected_review_generation_changed={}",
            report.selected_review_generation_changed
        ),
        format!(
            "selected_latest_release_generation_changed={}",
            report.selected_latest_release_generation_changed
        ),
        format!(
            "ambiguous_legacy_count_delta={}",
            report
                .ambiguous_legacy_count_delta
                .map(|value| value.to_string())
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!(
            "coherent_for_review_operations_changed={}",
            report.coherent_for_review_operations_changed
        ),
        format!(
            "issue_direction={}",
            report
                .issue_direction
                .map(|value| serialize_enum(&value))
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!(
            "latest_bundle_pointer_selected_bundle_path={}",
            report
                .latest_pointer
                .selected_bundle_path
                .clone()
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!(
            "archived_bundle_history_only={}",
            report.archived_bundle_history_only
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
    fn empty_archived_bundle_archive_yields_insufficient_artifacts_summary() {
        let root = temp_dir("state_bundle_archive_history_empty");
        let bundle_archive_dir = root.join("bundle_archive");
        fs::create_dir_all(&bundle_archive_dir).expect("bundle archive dir");

        let report = build_history_report(&Config {
            bundle_archive_dir: Some(bundle_archive_dir),
            bundle_latest_pointer_dir: None,
            pointer_name:
                activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME
                    .to_string(),
            json: false,
            mode: Mode::History,
        })
        .expect("history report");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleArchiveHistoryVerdict::ArtifactStateBundleArchiveHistoryInsufficientArtifacts
        );
        assert_eq!(report.archived_bundle_count, 0);
        assert_eq!(report.valid_archived_bundle_count, 0);
    }

    #[test]
    fn history_summary_over_coherent_and_ambiguous_bundles_is_classified_correctly() {
        let root = temp_dir("state_bundle_archive_history_mix");
        let state_archive_dir = root.join("state_archive");
        let bundle_archive_dir = root.join("bundle_archive");
        let coherent_snapshot = state_archive_dir
            .join("state_snapshot__2026-03-26T17-00-00Z__artifact_state_coherent.json");
        let ambiguous_snapshot = state_archive_dir.join(
            "state_snapshot__2026-03-26T18-00-00Z__artifact_state_ambiguous_legacy_state.json",
        );
        write_snapshot(
            &coherent_snapshot,
            "artifact_state_coherent",
            "review_a",
            "release_a",
            0,
            ts("2026-03-26T17:00:00Z"),
        );
        write_snapshot(
            &ambiguous_snapshot,
            "artifact_state_ambiguous_legacy_state",
            "review_b",
            "release_b",
            2,
            ts("2026-03-26T18:00:00Z"),
        );
        export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &coherent_snapshot);
        export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &ambiguous_snapshot);

        let report = build_history_report(&Config {
            bundle_archive_dir: Some(bundle_archive_dir),
            bundle_latest_pointer_dir: None,
            pointer_name:
                activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME
                    .to_string(),
            json: false,
            mode: Mode::History,
        })
        .expect("history report");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleArchiveHistoryVerdict::ArtifactStateBundleArchiveHistoryLatestNonGreen
        );
        assert_eq!(report.coherent_snapshot_state_count, 1);
        assert_eq!(report.ambiguous_snapshot_state_count, 1);
        assert_eq!(
            report
                .latest_archived_bundle
                .as_ref()
                .and_then(|bundle| bundle.selected_snapshot_state_verdict.as_deref()),
            Some("artifact_state_ambiguous_legacy_state")
        );
    }

    #[test]
    fn valid_latest_pointer_selecting_older_ambiguous_bundle_is_surfaced_honestly() {
        let root = temp_dir("state_bundle_archive_history_pointer_older_ambiguous");
        let state_archive_dir = root.join("state_archive");
        let bundle_archive_dir = root.join("bundle_archive");
        let pointer_dir = root.join("pointer");
        let ambiguous_snapshot = state_archive_dir.join(
            "state_snapshot__2026-03-26T17-00-00Z__artifact_state_ambiguous_legacy_state.json",
        );
        let coherent_snapshot = state_archive_dir
            .join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        write_snapshot(
            &ambiguous_snapshot,
            "artifact_state_ambiguous_legacy_state",
            "review_a",
            "release_a",
            1,
            ts("2026-03-26T17:00:00Z"),
        );
        write_snapshot(
            &coherent_snapshot,
            "artifact_state_coherent",
            "review_b",
            "release_b",
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        let ambiguous_bundle =
            export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &ambiguous_snapshot);
        export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &coherent_snapshot);
        write_bundle_pointer_metadata(
            &pointer_dir,
            activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME,
            &bundle_archive_dir,
            &ambiguous_bundle,
        );

        let report = build_history_report(&Config {
            bundle_archive_dir: Some(bundle_archive_dir),
            bundle_latest_pointer_dir: Some(pointer_dir),
            pointer_name:
                activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME
                    .to_string(),
            json: false,
            mode: Mode::History,
        })
        .expect("history report");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleArchiveHistoryVerdict::ArtifactStateBundleArchiveHistoryLatestNonGreen
        );
        assert_eq!(
            report
                .latest_pointer
                .selected_bundle_matches_latest_by_archive,
            Some(false)
        );
        assert_eq!(report.latest_pointer.selected_bundle_ambiguous, Some(true));
    }

    #[test]
    fn compare_mode_shows_snapshot_verdict_and_selection_drift_correctly() {
        let root = temp_dir("state_bundle_archive_compare_drift");
        let state_archive_dir = root.join("state_archive");
        let bundle_archive_dir = root.join("bundle_archive");
        let older_snapshot = state_archive_dir
            .join("state_snapshot__2026-03-26T17-00-00Z__artifact_state_incomplete.json");
        let newer_snapshot = state_archive_dir
            .join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        write_snapshot(
            &older_snapshot,
            "artifact_state_incomplete",
            "review_a",
            "release_a",
            0,
            ts("2026-03-26T17:00:00Z"),
        );
        write_snapshot(
            &newer_snapshot,
            "artifact_state_coherent",
            "review_b",
            "release_b",
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        let older_bundle =
            export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &older_snapshot);
        let newer_bundle =
            export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &newer_snapshot);

        let report = build_compare_report(
            &Config {
                bundle_archive_dir: Some(bundle_archive_dir),
                bundle_latest_pointer_dir: None,
                pointer_name:
                    activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME
                        .to_string(),
                json: false,
                mode: Mode::Compare {
                    older: older_bundle.clone(),
                    newer: newer_bundle.clone(),
                },
            },
            &older_bundle,
            &newer_bundle,
        )
        .expect("compare report");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleArchiveHistoryVerdict::ArtifactStateBundleArchiveCompareReady
        );
        assert!(report.snapshot_state_verdict_changed);
        assert!(report.selected_review_generation_changed);
        assert!(report.selected_latest_release_generation_changed);
        assert_eq!(report.issue_direction, Some(IssueDirection::NarrowedIssues));
    }

    #[test]
    fn compare_mode_rejects_invalid_archived_bundle_cleanly() {
        let root = temp_dir("state_bundle_archive_compare_invalid");
        let valid_bundle_dir = root.join("valid_bundle");
        let invalid_bundle_dir = root.join("invalid_bundle");
        let state_archive_dir = root.join("state_archive");
        let snapshot = state_archive_dir
            .join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        write_snapshot(
            &snapshot,
            "artifact_state_coherent",
            "review_a",
            "release_a",
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        let valid_bundle = activation_artifact_state_bundle::export_state_snapshot_bundle(
            &state_archive_dir,
            snapshot
                .file_name()
                .and_then(|v| v.to_str())
                .expect("snapshot file name"),
            &valid_bundle_dir,
        )
        .expect("export valid bundle");
        assert_eq!(valid_bundle.verdict, "artifact_state_bundle_exported");
        fs::create_dir_all(&invalid_bundle_dir).expect("invalid bundle dir");
        fs::write(
            invalid_bundle_dir.join(activation_artifact_state_bundle::BUNDLE_MANIFEST_FILENAME),
            "{broken",
        )
        .expect("write invalid manifest");

        let report = build_compare_report(
            &Config {
                bundle_archive_dir: None,
                bundle_latest_pointer_dir: None,
                pointer_name:
                    activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME
                        .to_string(),
                json: false,
                mode: Mode::Compare {
                    older: valid_bundle_dir.clone(),
                    newer: invalid_bundle_dir.clone(),
                },
            },
            &valid_bundle_dir,
            &invalid_bundle_dir,
        )
        .expect("compare report");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleArchiveHistoryVerdict::ArtifactStateBundleArchiveCompareInvalidArtifact
        );
        assert_eq!(report.invalid_artifacts.len(), 1);
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

    fn write_snapshot(
        path: &Path,
        state_verdict: &str,
        selected_review_generation_id: &str,
        selected_latest_release_generation_id: &str,
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
            "selection_alignment_matches": state_verdict == "artifact_state_coherent",
            "selection_alignment_summary": if state_verdict == "artifact_state_coherent" {
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
