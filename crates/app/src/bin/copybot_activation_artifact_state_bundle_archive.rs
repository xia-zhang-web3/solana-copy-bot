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
#[path = "copybot_activation_artifact_state_bundle.rs"]
mod activation_artifact_state_bundle;
#[allow(dead_code)]
#[path = "copybot_activation_artifact_state_bundle_publish_report.rs"]
mod activation_artifact_state_bundle_publish_report;

const USAGE: &str = "usage: copybot_activation_artifact_state_bundle_archive --bundle-archive-dir <path> [--bundle-latest-pointer-dir <path>] [--pointer-name <name>] [--json] (--report | --retention-plan --keep-latest <count> | --retention-apply --keep-latest <count>)";

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
    pointer_name: String,
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
enum ArtifactStateBundleArchiveVerdict {
    ArtifactStateBundleArchiveOk,
    ArtifactStateBundleArchiveIncomplete,
    ArtifactStateBundleArchiveInvalidArtifactsPresent,
    ArtifactStateBundleArchivePointerInconsistent,
    ArtifactStateBundleArchiveRetentionPlanReady,
    ArtifactStateBundleArchiveRetentionPlanInsufficientArtifacts,
    ArtifactStateBundleArchiveCleanupApplied,
    ArtifactStateBundleArchiveCleanupNothingToDo,
    ArtifactStateBundleArchiveCleanupBlockedByPointer,
    ArtifactStateBundleArchiveCleanupBlockedByInvalidBundle,
    ArtifactStateBundleArchiveCleanupFailedPartial,
}

#[derive(Debug, Clone, Serialize)]
struct InvalidBundleSummary {
    path: String,
    bundle_verdict: String,
    reason: String,
}

#[derive(Debug, Clone)]
struct LoadedBundleRecord {
    bundle_root: PathBuf,
    bundle_dir_name: String,
    summary: activation_artifact_state_bundle::StateSnapshotBundleSummary,
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
    protected_by_latest_pointer: bool,
    protected_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct LatestBundlePointerSummary {
    requested: bool,
    pointer_verdict: Option<String>,
    pointer_reason: String,
    pointer_path: Option<String>,
    pointer_metadata_present: bool,
    selected_bundle_path: Option<String>,
    selected_bundle_integrity_verdict: Option<String>,
    selected_bundle_integrity_reason: Option<String>,
    selected_snapshot_state_verdict: Option<String>,
    selected_snapshot_state_reason: Option<String>,
    selected_snapshot_ambiguous_legacy_count: Option<usize>,
    selected_snapshot_coherent_for_review_operations: Option<bool>,
    selected_bundle_loaded: bool,
    selected_bundle_protected: bool,
    target_exists: bool,
    target_matches_identity: bool,
    blocks_cleanup: bool,
}

#[derive(Debug, Clone, Serialize)]
struct ProtectedBundleSummary {
    path: String,
    reason: String,
}

#[derive(Debug, Clone, Serialize)]
struct RetentionSelectionSummary {
    keep_latest: usize,
    would_keep: Vec<ArchivedBundleSummary>,
    would_remove: Vec<ArchivedBundleSummary>,
    protected_bundles: Vec<ProtectedBundleSummary>,
}

#[derive(Debug, Clone, Serialize)]
struct ArtifactStateBundleArchiveReport {
    mode: String,
    verdict: ArtifactStateBundleArchiveVerdict,
    reason: String,
    bundle_archive_dir: String,
    bundle_latest_pointer_dir: Option<String>,
    pointer_name: String,
    archived_bundle_count: usize,
    verified_clean_bundle_count: usize,
    invalid_or_drifted_bundle_count: usize,
    coherent_snapshot_state_count: usize,
    incomplete_snapshot_state_count: usize,
    inconsistent_snapshot_state_count: usize,
    ambiguous_snapshot_state_count: usize,
    non_green_snapshot_state_count: usize,
    latest_archived_bundle: Option<ArchivedBundleSummary>,
    latest_pointer: LatestBundlePointerSummary,
    invalid_or_drifted_bundles: Vec<InvalidBundleSummary>,
    retention: Option<RetentionSelectionSummary>,
    removed_paths: Vec<String>,
    failed_removals: Vec<String>,
    state_bundle_archive_management_only: bool,
    execution_untouched: bool,
    activation_authorized: bool,
    not_authorized_summary: String,
}

#[derive(Debug, Clone)]
struct PointerProtection {
    summary: LatestBundlePointerSummary,
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
    let mut bundle_archive_dir: Option<PathBuf> = None;
    let mut bundle_latest_pointer_dir: Option<PathBuf> = None;
    let mut pointer_name =
        activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME.to_string();
    let mut json = false;
    let mut report = false;
    let mut retention_plan = false;
    let mut retention_apply = false;
    let mut keep_latest: Option<usize> = None;

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
        bundle_archive_dir: bundle_archive_dir
            .ok_or_else(|| anyhow!("--bundle-archive-dir is required"))?,
        bundle_latest_pointer_dir,
        pointer_name,
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

fn build_report(config: &Config) -> Result<ArtifactStateBundleArchiveReport> {
    let archive_paths = activation_artifact_state_bundle::collect_state_snapshot_bundle_paths(
        Some(&config.bundle_archive_dir),
        &[],
    )?;
    let (mut bundles, inspect_errors) = load_archive_bundles(&archive_paths);
    bundles.sort_by(|left, right| {
        right
            .bundle_dir_name
            .cmp(&left.bundle_dir_name)
            .then_with(|| right.bundle_root.cmp(&left.bundle_root))
    });

    let pointer_protection = inspect_pointer_protection(config)?;
    let retention = match config.mode {
        Mode::Report => None,
        Mode::RetentionPlan { keep_latest } | Mode::RetentionApply { keep_latest } => Some(
            build_retention_selection(&bundles, keep_latest, &pointer_protection),
        ),
    };
    let invalid_or_drifted_bundles = collect_invalid_or_drifted_bundles(&bundles, inspect_errors);
    let verified_clean_bundle_count = bundles
        .iter()
        .filter(|record| bundle_verifies_clean(record))
        .count();

    let (verdict, reason, removed_paths, failed_removals) = match config.mode {
        Mode::Report => determine_report_verdict(
            &bundles,
            &invalid_or_drifted_bundles,
            &pointer_protection.summary,
        ),
        Mode::RetentionPlan { .. } => determine_plan_verdict(
            &bundles,
            &invalid_or_drifted_bundles,
            &pointer_protection.summary,
            retention.as_ref().expect("retention plan"),
        ),
        Mode::RetentionApply { .. } => apply_retention(
            &config.bundle_archive_dir,
            &invalid_or_drifted_bundles,
            &pointer_protection.summary,
            retention.as_ref().expect("retention plan"),
        )?,
    };

    Ok(ArtifactStateBundleArchiveReport {
        mode: mode_name(&config.mode).to_string(),
        verdict,
        reason,
        bundle_archive_dir: config.bundle_archive_dir.display().to_string(),
        bundle_latest_pointer_dir: config
            .bundle_latest_pointer_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        pointer_name: config.pointer_name.clone(),
        archived_bundle_count: archive_paths.len(),
        verified_clean_bundle_count,
        invalid_or_drifted_bundle_count: invalid_or_drifted_bundles.len(),
        coherent_snapshot_state_count: count_snapshot_state(&bundles, "artifact_state_coherent"),
        incomplete_snapshot_state_count: count_snapshot_state(&bundles, "artifact_state_incomplete"),
        inconsistent_snapshot_state_count: count_snapshot_state(
            &bundles,
            "artifact_state_inconsistent",
        ),
        ambiguous_snapshot_state_count: count_snapshot_state(
            &bundles,
            "artifact_state_ambiguous_legacy_state",
        ),
        non_green_snapshot_state_count: count_non_green_snapshot_states(&bundles),
        latest_archived_bundle: bundles.first().map(|record| {
            summarize_loaded_bundle(
                record,
                retention.as_ref(),
                &pointer_protection,
            )
        }),
        latest_pointer: pointer_protection.summary,
        invalid_or_drifted_bundles,
        retention,
        removed_paths,
        failed_removals,
        state_bundle_archive_management_only: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary: "State-snapshot bundle archive management only indexes, previews, and boundedly cleans archived bundle artifacts. It preserves underlying snapshot state truth but does not authorize activation, does not enable execution, and does not override the Stage 3 production gate.".to_string(),
    })
}

fn determine_report_verdict(
    bundles: &[LoadedBundleRecord],
    invalid_or_drifted_bundles: &[InvalidBundleSummary],
    pointer_summary: &LatestBundlePointerSummary,
) -> (
    ArtifactStateBundleArchiveVerdict,
    String,
    Vec<String>,
    Vec<String>,
) {
    if !invalid_or_drifted_bundles.is_empty() {
        return (
            ArtifactStateBundleArchiveVerdict::ArtifactStateBundleArchiveInvalidArtifactsPresent,
            format!(
                "state snapshot bundle archive contains {} invalid or drifted bundle(s); review invalid_or_drifted_bundles before trusting archive health",
                invalid_or_drifted_bundles.len()
            ),
            Vec::new(),
            Vec::new(),
        );
    }
    if pointer_summary.blocks_cleanup {
        return (
            ArtifactStateBundleArchiveVerdict::ArtifactStateBundleArchivePointerInconsistent,
            pointer_summary.pointer_reason.clone(),
            Vec::new(),
            Vec::new(),
        );
    }
    if bundles.is_empty() {
        return (
            ArtifactStateBundleArchiveVerdict::ArtifactStateBundleArchiveIncomplete,
            "no archived state snapshot bundles were found in the bundle archive".to_string(),
            Vec::new(),
            Vec::new(),
        );
    }
    if pointer_selects_valid_non_green_bundle(pointer_summary) {
        let snapshot_state_verdict = pointer_summary
            .selected_snapshot_state_verdict
            .as_deref()
            .unwrap_or("unknown");
        let snapshot_state_reason = pointer_summary
            .selected_snapshot_state_reason
            .as_deref()
            .unwrap_or("no snapshot state reason recorded");
        let selected_bundle_path = pointer_summary
            .selected_bundle_path
            .as_deref()
            .unwrap_or("<unknown>");
        return (
            ArtifactStateBundleArchiveVerdict::ArtifactStateBundleArchiveIncomplete,
            format!(
                "state snapshot bundle latest pointer selects non-green archived bundle {} with snapshot verdict {}: {}",
                selected_bundle_path, snapshot_state_verdict, snapshot_state_reason
            ),
            Vec::new(),
            Vec::new(),
        );
    }
    if bundles
        .first()
        .is_some_and(|record| bundle_encapsulates_non_green_snapshot(record))
    {
        let latest = bundles.first().expect("latest bundle");
        return (
            ArtifactStateBundleArchiveVerdict::ArtifactStateBundleArchiveIncomplete,
            format!(
                "latest archived state snapshot bundle {} encapsulates non-green snapshot state {}",
                latest.bundle_root.display(),
                latest
                    .summary
                    .selected_snapshot_state_verdict
                    .as_deref()
                    .unwrap_or("unknown")
            ),
            Vec::new(),
            Vec::new(),
        );
    }
    (
        ArtifactStateBundleArchiveVerdict::ArtifactStateBundleArchiveOk,
        "state snapshot bundle archive is healthy enough for bundle review operations".to_string(),
        Vec::new(),
        Vec::new(),
    )
}

fn determine_plan_verdict(
    bundles: &[LoadedBundleRecord],
    invalid_or_drifted_bundles: &[InvalidBundleSummary],
    pointer_summary: &LatestBundlePointerSummary,
    selection: &RetentionSelectionSummary,
) -> (
    ArtifactStateBundleArchiveVerdict,
    String,
    Vec<String>,
    Vec<String>,
) {
    if !invalid_or_drifted_bundles.is_empty() {
        return (
            ArtifactStateBundleArchiveVerdict::ArtifactStateBundleArchiveInvalidArtifactsPresent,
            format!(
                "state snapshot bundle retention preview found {} invalid or drifted bundle(s); preview is computed over clean bundles only",
                invalid_or_drifted_bundles.len()
            ),
            Vec::new(),
            Vec::new(),
        );
    }
    if pointer_summary.blocks_cleanup {
        return (
            ArtifactStateBundleArchiveVerdict::ArtifactStateBundleArchivePointerInconsistent,
            format!(
                "state snapshot bundle retention preview is blocked by latest-bundle-pointer inconsistency: {}",
                pointer_summary.pointer_reason
            ),
            Vec::new(),
            Vec::new(),
        );
    }
    if bundles.is_empty() || selection.would_keep.is_empty() {
        return (
            ArtifactStateBundleArchiveVerdict::ArtifactStateBundleArchiveRetentionPlanInsufficientArtifacts,
            "not enough archived state snapshot bundles exist to produce a meaningful retention plan"
                .to_string(),
            Vec::new(),
            Vec::new(),
        );
    }
    (
        ArtifactStateBundleArchiveVerdict::ArtifactStateBundleArchiveRetentionPlanReady,
        "state snapshot bundle retention preview is ready; review would_keep, would_remove, and protected bundles before apply".to_string(),
        Vec::new(),
        Vec::new(),
    )
}

fn apply_retention(
    bundle_archive_dir: &Path,
    invalid_or_drifted_bundles: &[InvalidBundleSummary],
    pointer_summary: &LatestBundlePointerSummary,
    selection: &RetentionSelectionSummary,
) -> Result<(
    ArtifactStateBundleArchiveVerdict,
    String,
    Vec<String>,
    Vec<String>,
)> {
    if !invalid_or_drifted_bundles.is_empty() {
        return Ok((
            ArtifactStateBundleArchiveVerdict::ArtifactStateBundleArchiveCleanupBlockedByInvalidBundle,
            format!(
                "state snapshot bundle cleanup is blocked because {} invalid or drifted bundle(s) are present",
                invalid_or_drifted_bundles.len()
            ),
            Vec::new(),
            Vec::new(),
        ));
    }
    if pointer_summary.blocks_cleanup {
        return Ok((
            ArtifactStateBundleArchiveVerdict::ArtifactStateBundleArchiveCleanupBlockedByPointer,
            format!(
                "state snapshot bundle cleanup is blocked by latest-bundle-pointer safety: {}",
                pointer_summary.pointer_reason
            ),
            Vec::new(),
            Vec::new(),
        ));
    }
    if selection.would_remove.is_empty() {
        return Ok((
            ArtifactStateBundleArchiveVerdict::ArtifactStateBundleArchiveCleanupNothingToDo,
            "state snapshot bundle cleanup has nothing to do under the requested keep-latest rule"
                .to_string(),
            Vec::new(),
            Vec::new(),
        ));
    }

    let canonical_archive_root = canonical_bundle_archive_root(bundle_archive_dir)?;
    let mut removed_paths = Vec::new();
    let mut failed_removals = Vec::new();
    for bundle in &selection.would_remove {
        let path = PathBuf::from(&bundle.path);
        let canonical = fs::canonicalize(&path).with_context(|| {
            format!(
                "failed canonicalizing state snapshot bundle cleanup candidate {}",
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
                "{}: candidate is outside state snapshot bundle archive {}",
                canonical.display(),
                canonical_archive_root.display()
            ));
            continue;
        }
        match fs::remove_dir_all(&canonical) {
            Ok(_) => removed_paths.push(canonical.display().to_string()),
            Err(error) => failed_removals.push(format!("{}: {}", canonical.display(), error)),
        }
    }

    if failed_removals.is_empty() {
        Ok((
            ArtifactStateBundleArchiveVerdict::ArtifactStateBundleArchiveCleanupApplied,
            format!(
                "state snapshot bundle cleanup removed {} archived bundle(s) under the requested keep-latest rule",
                removed_paths.len()
            ),
            removed_paths,
            failed_removals,
        ))
    } else {
        Ok((
            ArtifactStateBundleArchiveVerdict::ArtifactStateBundleArchiveCleanupFailedPartial,
            "state snapshot bundle cleanup removed some archived bundles, but at least one deletion failed; review failed_removals before retrying".to_string(),
            removed_paths,
            failed_removals,
        ))
    }
}

fn build_retention_selection(
    bundles: &[LoadedBundleRecord],
    keep_latest: usize,
    pointer_protection: &PointerProtection,
) -> RetentionSelectionSummary {
    let clean_bundles = bundles
        .iter()
        .filter(|record| bundle_verifies_clean(record))
        .collect::<Vec<_>>();
    let mut protected_paths = BTreeMap::new();
    if let (Some(path), Some(reason)) = (
        pointer_protection.protected_path.as_ref(),
        pointer_protection.protected_reason.as_ref(),
    ) {
        protected_paths.insert(path.clone(), reason.clone());
    }

    let mut keep_paths = clean_bundles
        .iter()
        .take(keep_latest)
        .map(|record| record.bundle_root.clone())
        .collect::<BTreeSet<_>>();
    for path in protected_paths.keys() {
        keep_paths.insert(path.clone());
    }

    let would_keep = clean_bundles
        .iter()
        .filter(|record| keep_paths.contains(&record.bundle_root))
        .map(|record| summarize_loaded_bundle(record, None, pointer_protection))
        .collect::<Vec<_>>();
    let would_remove = clean_bundles
        .iter()
        .filter(|record| !keep_paths.contains(&record.bundle_root))
        .map(|record| summarize_loaded_bundle(record, None, pointer_protection))
        .collect::<Vec<_>>();

    RetentionSelectionSummary {
        keep_latest,
        would_keep,
        would_remove,
        protected_bundles: protected_paths
            .into_iter()
            .map(|(path, reason)| ProtectedBundleSummary {
                path: path.display().to_string(),
                reason,
            })
            .collect(),
    }
}

fn inspect_pointer_protection(config: &Config) -> Result<PointerProtection> {
    let Some(pointer_dir) = &config.bundle_latest_pointer_dir else {
        return Ok(PointerProtection {
            summary: LatestBundlePointerSummary {
                requested: false,
                pointer_verdict: None,
                pointer_reason: "state snapshot bundle latest pointer was not requested"
                    .to_string(),
                pointer_path: None,
                pointer_metadata_present: false,
                selected_bundle_path: None,
                selected_bundle_integrity_verdict: None,
                selected_bundle_integrity_reason: None,
                selected_snapshot_state_verdict: None,
                selected_snapshot_state_reason: None,
                selected_snapshot_ambiguous_legacy_count: None,
                selected_snapshot_coherent_for_review_operations: None,
                selected_bundle_loaded: false,
                selected_bundle_protected: false,
                target_exists: false,
                target_matches_identity: false,
                blocks_cleanup: false,
            },
            protected_path: None,
            protected_reason: None,
        });
    };

    let loaded_metadata =
        activation_artifact_state_bundle_publish_report::inspect_latest_bundle_pointer_metadata(
            pointer_dir,
            &config.pointer_name,
        )?;
    let report =
        activation_artifact_state_bundle_publish_report::inspect_latest_bundle_pointer_report(
            &config.bundle_archive_dir,
            pointer_dir,
            &config.pointer_name,
            true,
        )?;

    let selected_bundle_path = report.persisted_bundle_path.clone().or_else(|| {
        loaded_metadata
            .as_ref()
            .map(|loaded| loaded.metadata.selected_bundle_path.clone())
    });
    let protected_path = if report.verdict
        == activation_artifact_state_bundle_publish_report::ArtifactStateBundleReportVerdict::ArtifactStateBundleReportVerifyOk
    {
        selected_bundle_path
            .as_deref()
            .and_then(|path| fs::canonicalize(Path::new(path)).ok())
    } else {
        None
    };
    let protected_reason = protected_path.as_ref().map(|_| {
        format!(
            "protected by valid latest bundle pointer `{}`",
            config.pointer_name
        )
    });
    let blocks_cleanup = loaded_metadata.is_some()
        && report.verdict
            != activation_artifact_state_bundle_publish_report::ArtifactStateBundleReportVerdict::ArtifactStateBundleReportVerifyOk;

    Ok(PointerProtection {
        summary: LatestBundlePointerSummary {
            requested: true,
            pointer_verdict: Some(serialize_enum(&report.verdict)),
            pointer_reason: report.reason.clone(),
            pointer_path: report.latest_pointer_path.clone().or_else(|| {
                activation_artifact_state_bundle_publish_report::latest_state_bundle_pointer_path(
                    pointer_dir,
                    &config.pointer_name,
                )
                .ok()
                .map(|path| path.display().to_string())
            }),
            pointer_metadata_present: loaded_metadata.is_some(),
            selected_bundle_path,
            selected_bundle_integrity_verdict: report.bundle_integrity_verdict.clone(),
            selected_bundle_integrity_reason: report.bundle_integrity_reason.clone(),
            selected_snapshot_state_verdict: report.selected_snapshot_state_verdict.clone(),
            selected_snapshot_state_reason: report.selected_snapshot_state_reason.clone(),
            selected_snapshot_ambiguous_legacy_count: report.ambiguous_legacy_count,
            selected_snapshot_coherent_for_review_operations: report.coherent_for_review_operations,
            selected_bundle_loaded: report.persisted_bundle_exists,
            selected_bundle_protected: protected_path.is_some(),
            target_exists: report.latest_pointer_target_exists,
            target_matches_identity: report.latest_pointer_target_matches_identity,
            blocks_cleanup,
        },
        protected_path,
        protected_reason,
    })
}

fn load_archive_bundles(paths: &[PathBuf]) -> (Vec<LoadedBundleRecord>, Vec<InvalidBundleSummary>) {
    let mut valid = Vec::new();
    let mut invalid = Vec::new();
    for path in paths {
        match activation_artifact_state_bundle::inspect_state_snapshot_bundle(path) {
            Ok(summary) => {
                let bundle_root = fs::canonicalize(Path::new(&summary.bundle_path))
                    .unwrap_or_else(|_| PathBuf::from(&summary.bundle_path));
                let bundle_dir_name = bundle_root
                    .file_name()
                    .map(|value| value.to_string_lossy().into_owned())
                    .unwrap_or_else(|| summary.bundle_path.clone());
                valid.push(LoadedBundleRecord {
                    bundle_root,
                    bundle_dir_name,
                    summary,
                });
            }
            Err(error) => invalid.push(InvalidBundleSummary {
                path: path.display().to_string(),
                bundle_verdict: "bundle_inspection_failed".to_string(),
                reason: format!("{error:#}"),
            }),
        }
    }
    (valid, invalid)
}

fn collect_invalid_or_drifted_bundles(
    bundles: &[LoadedBundleRecord],
    inspect_errors: Vec<InvalidBundleSummary>,
) -> Vec<InvalidBundleSummary> {
    let mut invalid = inspect_errors;
    invalid.extend(bundles.iter().filter_map(|record| {
        if bundle_verifies_clean(record) {
            None
        } else {
            Some(InvalidBundleSummary {
                path: record.bundle_root.display().to_string(),
                bundle_verdict: record.summary.verdict.clone(),
                reason: record.summary.reason.clone(),
            })
        }
    }));
    invalid
}

fn summarize_loaded_bundle(
    record: &LoadedBundleRecord,
    retention: Option<&RetentionSelectionSummary>,
    pointer_protection: &PointerProtection,
) -> ArchivedBundleSummary {
    let protected_reason = retention
        .and_then(|selection| {
            selection
                .protected_bundles
                .iter()
                .find(|entry| entry.path == record.bundle_root.display().to_string())
                .map(|entry| entry.reason.clone())
        })
        .or_else(|| {
            if pointer_protection.protected_path.as_ref() == Some(&record.bundle_root) {
                pointer_protection.protected_reason.clone()
            } else {
                None
            }
        });
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
        protected_by_latest_pointer: protected_reason.is_some(),
        protected_reason,
    }
}

fn count_snapshot_state(bundles: &[LoadedBundleRecord], state_verdict: &str) -> usize {
    bundles
        .iter()
        .filter(|record| {
            record.summary.selected_snapshot_state_verdict.as_deref() == Some(state_verdict)
        })
        .count()
}

fn count_non_green_snapshot_states(bundles: &[LoadedBundleRecord]) -> usize {
    bundles
        .iter()
        .filter(|record| bundle_encapsulates_non_green_snapshot(record))
        .count()
}

fn bundle_verifies_clean(record: &LoadedBundleRecord) -> bool {
    record.summary.verdict == "artifact_state_bundle_verified"
}

fn bundle_encapsulates_non_green_snapshot(record: &LoadedBundleRecord) -> bool {
    record
        .summary
        .selected_snapshot_state_verdict
        .as_deref()
        .is_some_and(|value| value != "artifact_state_coherent")
}

fn pointer_selects_valid_non_green_bundle(pointer_summary: &LatestBundlePointerSummary) -> bool {
    pointer_summary.pointer_verdict.as_deref() == Some("artifact_state_bundle_report_verify_ok")
        && pointer_summary.selected_bundle_loaded
        && pointer_summary.target_exists
        && pointer_summary.target_matches_identity
        && pointer_summary
            .selected_snapshot_state_verdict
            .as_deref()
            .is_some_and(|value| value != "artifact_state_coherent")
}

fn canonical_bundle_archive_root(bundle_archive_dir: &Path) -> Result<PathBuf> {
    fs::canonicalize(bundle_archive_dir).with_context(|| {
        format!(
            "failed canonicalizing state snapshot bundle archive {}",
            bundle_archive_dir.display()
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

fn render_human(report: &ArtifactStateBundleArchiveReport) -> String {
    let pointer_verdict = report
        .latest_pointer
        .pointer_verdict
        .clone()
        .unwrap_or_else(|| "not_requested".to_string());
    [
        "event=copybot_activation_artifact_state_bundle_archive".to_string(),
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("bundle_archive_dir={}", report.bundle_archive_dir),
        format!("archived_bundle_count={}", report.archived_bundle_count),
        format!(
            "verified_clean_bundle_count={}",
            report.verified_clean_bundle_count
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
        format!(
            "non_green_snapshot_state_count={}",
            report.non_green_snapshot_state_count
        ),
        format!(
            "latest_archived_bundle_path={}",
            report
                .latest_archived_bundle
                .as_ref()
                .map(|summary| summary.path.clone())
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!("latest_pointer_verdict={pointer_verdict}"),
        format!(
            "latest_pointer_selected_bundle_path={}",
            report
                .latest_pointer
                .selected_bundle_path
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
        format!(
            "protected_bundle_count={}",
            report
                .retention
                .as_ref()
                .map(|selection| selection.protected_bundles.len())
                .unwrap_or(usize::from(report.latest_pointer.selected_bundle_protected))
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
            "state_bundle_archive_management_only={}",
            report.state_bundle_archive_management_only
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
    fn empty_archived_bundle_dir_is_reported_honestly() {
        let bundle_archive_dir = temp_dir("state_bundle_archive_manager_empty");
        let report = build_report(&Config {
            bundle_archive_dir,
            bundle_latest_pointer_dir: None,
            pointer_name:
                activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME
                    .to_string(),
            json: false,
            mode: Mode::Report,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleArchiveVerdict::ArtifactStateBundleArchiveIncomplete
        );
        assert_eq!(report.archived_bundle_count, 0);
        assert!(report.reason.contains("no archived state snapshot bundles"));
    }

    #[test]
    fn valid_archived_bundle_dir_with_coherent_and_non_green_bundles_summarizes_correctly() {
        let state_archive_dir = temp_dir("state_bundle_archive_manager_summary_state");
        let coherent_snapshot = state_archive_dir
            .join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        let ambiguous_snapshot = state_archive_dir.join(
            "state_snapshot__2026-03-26T19-00-00Z__artifact_state_ambiguous_legacy_state.json",
        );
        write_snapshot(
            &coherent_snapshot,
            "artifact_state_coherent",
            "review_a",
            "release_a",
            true,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        write_snapshot(
            &ambiguous_snapshot,
            "artifact_state_ambiguous_legacy_state",
            "review_b",
            "release_b",
            true,
            2,
            ts("2026-03-26T19:00:00Z"),
        );
        let bundle_archive_dir = temp_dir("state_bundle_archive_manager_summary_archive");
        export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &coherent_snapshot);
        export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &ambiguous_snapshot);

        let report = build_report(&Config {
            bundle_archive_dir,
            bundle_latest_pointer_dir: None,
            pointer_name:
                activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME
                    .to_string(),
            json: false,
            mode: Mode::Report,
        })
        .expect("report");

        assert_eq!(report.archived_bundle_count, 2);
        assert_eq!(report.verified_clean_bundle_count, 2);
        assert_eq!(report.coherent_snapshot_state_count, 1);
        assert_eq!(report.ambiguous_snapshot_state_count, 1);
        assert_eq!(report.non_green_snapshot_state_count, 1);
        assert_eq!(
            report.verdict,
            ArtifactStateBundleArchiveVerdict::ArtifactStateBundleArchiveIncomplete
        );
    }

    #[test]
    fn retention_plan_keeps_latest_n_archived_bundles_correctly() {
        let state_archive_dir = temp_dir("state_bundle_archive_manager_plan_state");
        let older = state_archive_dir
            .join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        let middle = state_archive_dir
            .join("state_snapshot__2026-03-26T19-00-00Z__artifact_state_coherent.json");
        let newer = state_archive_dir
            .join("state_snapshot__2026-03-26T20-00-00Z__artifact_state_coherent.json");
        for (path, review, release, ts_value) in [
            (&older, "review_a", "release_a", "2026-03-26T18:00:00Z"),
            (&middle, "review_b", "release_b", "2026-03-26T19:00:00Z"),
            (&newer, "review_c", "release_c", "2026-03-26T20:00:00Z"),
        ] {
            write_snapshot(
                path,
                "artifact_state_coherent",
                review,
                release,
                true,
                0,
                ts(ts_value),
            );
        }
        let bundle_archive_dir = temp_dir("state_bundle_archive_manager_plan_archive");
        export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &older);
        export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &middle);
        export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &newer);

        let report = build_report(&Config {
            bundle_archive_dir,
            bundle_latest_pointer_dir: None,
            pointer_name:
                activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME
                    .to_string(),
            json: false,
            mode: Mode::RetentionPlan { keep_latest: 2 },
        })
        .expect("report");

        let retention = report.retention.expect("retention");
        assert_eq!(
            report.verdict,
            ArtifactStateBundleArchiveVerdict::ArtifactStateBundleArchiveRetentionPlanReady
        );
        assert_eq!(retention.would_keep.len(), 2);
        assert_eq!(retention.would_remove.len(), 1);
        assert!(retention.would_remove[0]
            .dir_name
            .contains("2026-03-26T18-00-00Z"));
    }

    #[test]
    fn valid_latest_bundle_pointer_target_is_protected_from_cleanup() {
        let state_archive_dir = temp_dir("state_bundle_archive_manager_pointer_state");
        let older = state_archive_dir
            .join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        let middle = state_archive_dir
            .join("state_snapshot__2026-03-26T19-00-00Z__artifact_state_coherent.json");
        let newer = state_archive_dir
            .join("state_snapshot__2026-03-26T20-00-00Z__artifact_state_coherent.json");
        for (path, review, release, ts_value) in [
            (&older, "review_a", "release_a", "2026-03-26T18:00:00Z"),
            (&middle, "review_b", "release_b", "2026-03-26T19:00:00Z"),
            (&newer, "review_c", "release_c", "2026-03-26T20:00:00Z"),
        ] {
            write_snapshot(
                path,
                "artifact_state_coherent",
                review,
                release,
                true,
                0,
                ts(ts_value),
            );
        }
        let bundle_archive_dir = temp_dir("state_bundle_archive_manager_pointer_archive");
        let older_bundle = export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &older);
        export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &middle);
        export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &newer);
        let pointer_dir = temp_dir("state_bundle_archive_manager_pointer_dir");
        write_pointer_metadata(
            &pointer_dir,
            activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME,
            &bundle_archive_dir,
            &older_bundle,
        );

        let report = build_report(&Config {
            bundle_archive_dir,
            bundle_latest_pointer_dir: Some(pointer_dir),
            pointer_name:
                activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME
                    .to_string(),
            json: false,
            mode: Mode::RetentionPlan { keep_latest: 1 },
        })
        .expect("report");

        let retention = report.retention.expect("retention");
        assert_eq!(retention.would_keep.len(), 2);
        assert_eq!(retention.would_remove.len(), 1);
        assert_eq!(retention.protected_bundles.len(), 1);
        assert_eq!(
            retention.protected_bundles[0].path,
            fs::canonicalize(&older_bundle)
                .expect("canonical older bundle")
                .display()
                .to_string()
        );
    }

    #[test]
    fn dangling_latest_bundle_pointer_is_surfaced_honestly_and_blocks_cleanup() {
        let state_archive_dir = temp_dir("state_bundle_archive_manager_dangling_state");
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
        let bundle_archive_dir = temp_dir("state_bundle_archive_manager_dangling_archive");
        export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &snapshot);
        let pointer_dir = temp_dir("state_bundle_archive_manager_dangling_pointer");
        write_missing_target_pointer_metadata(
            &pointer_dir,
            activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME,
            &bundle_archive_dir,
            &bundle_archive_dir.join("state_snapshot_bundle__missing"),
        );

        let report = build_report(&Config {
            bundle_archive_dir,
            bundle_latest_pointer_dir: Some(pointer_dir),
            pointer_name:
                activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME
                    .to_string(),
            json: false,
            mode: Mode::RetentionApply { keep_latest: 1 },
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleArchiveVerdict::ArtifactStateBundleArchiveCleanupBlockedByPointer
        );
        assert!(report.latest_pointer.blocks_cleanup);
        assert!(report
            .latest_pointer
            .pointer_reason
            .contains("target archived bundle is missing"));
    }

    #[test]
    fn retention_apply_removes_exact_preview_selected_archived_bundle_dirs() {
        let state_archive_dir = temp_dir("state_bundle_archive_manager_apply_state");
        let older = state_archive_dir
            .join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        let middle = state_archive_dir
            .join("state_snapshot__2026-03-26T19-00-00Z__artifact_state_coherent.json");
        let newer = state_archive_dir
            .join("state_snapshot__2026-03-26T20-00-00Z__artifact_state_coherent.json");
        for (path, review, release, ts_value) in [
            (&older, "review_a", "release_a", "2026-03-26T18:00:00Z"),
            (&middle, "review_b", "release_b", "2026-03-26T19:00:00Z"),
            (&newer, "review_c", "release_c", "2026-03-26T20:00:00Z"),
        ] {
            write_snapshot(
                path,
                "artifact_state_coherent",
                review,
                release,
                true,
                0,
                ts(ts_value),
            );
        }
        let bundle_archive_dir = temp_dir("state_bundle_archive_manager_apply_archive");
        let older_bundle = export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &older);
        let middle_bundle =
            export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &middle);
        let newer_bundle = export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &newer);

        let preview = build_report(&Config {
            bundle_archive_dir: bundle_archive_dir.clone(),
            bundle_latest_pointer_dir: None,
            pointer_name:
                activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME
                    .to_string(),
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
            bundle_archive_dir: bundle_archive_dir.clone(),
            bundle_latest_pointer_dir: None,
            pointer_name:
                activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME
                    .to_string(),
            json: false,
            mode: Mode::RetentionApply { keep_latest: 1 },
        })
        .expect("apply");

        assert_eq!(
            apply.verdict,
            ArtifactStateBundleArchiveVerdict::ArtifactStateBundleArchiveCleanupApplied
        );
        assert_eq!(
            apply.removed_paths.iter().cloned().collect::<BTreeSet<_>>(),
            preview_remove
        );
        assert!(!older_bundle.exists());
        assert!(!middle_bundle.exists());
        assert!(newer_bundle.exists());
    }

    #[test]
    fn nothing_to_do_case_is_explicit() {
        let state_archive_dir = temp_dir("state_bundle_archive_manager_nothing_state");
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
        let bundle_archive_dir = temp_dir("state_bundle_archive_manager_nothing_archive");
        export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &snapshot);

        let report = build_report(&Config {
            bundle_archive_dir,
            bundle_latest_pointer_dir: None,
            pointer_name:
                activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME
                    .to_string(),
            json: false,
            mode: Mode::RetentionApply { keep_latest: 1 },
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleArchiveVerdict::ArtifactStateBundleArchiveCleanupNothingToDo
        );
        assert!(report.removed_paths.is_empty());
    }

    #[test]
    fn ambiguous_non_green_bundled_snapshot_remains_explicit_in_report() {
        let state_archive_dir = temp_dir("state_bundle_archive_manager_ambiguous_state");
        let older_ambiguous = state_archive_dir.join(
            "state_snapshot__2026-03-26T18-00-00Z__artifact_state_ambiguous_legacy_state.json",
        );
        let newer_coherent = state_archive_dir
            .join("state_snapshot__2026-03-26T20-00-00Z__artifact_state_coherent.json");
        write_snapshot(
            &older_ambiguous,
            "artifact_state_ambiguous_legacy_state",
            "review_a",
            "release_a",
            true,
            3,
            ts("2026-03-26T18:00:00Z"),
        );
        write_snapshot(
            &newer_coherent,
            "artifact_state_coherent",
            "review_b",
            "release_b",
            true,
            0,
            ts("2026-03-26T20:00:00Z"),
        );
        let bundle_archive_dir = temp_dir("state_bundle_archive_manager_ambiguous_archive");
        let ambiguous_bundle =
            export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &older_ambiguous);
        export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &newer_coherent);
        let pointer_dir = temp_dir("state_bundle_archive_manager_ambiguous_pointer");
        write_pointer_metadata(
            &pointer_dir,
            activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME,
            &bundle_archive_dir,
            &ambiguous_bundle,
        );

        let report = build_report(&Config {
            bundle_archive_dir,
            bundle_latest_pointer_dir: Some(pointer_dir),
            pointer_name:
                activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME
                    .to_string(),
            json: false,
            mode: Mode::Report,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleArchiveVerdict::ArtifactStateBundleArchiveIncomplete
        );
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
            Some(3)
        );
        assert_eq!(
            report
                .latest_pointer
                .selected_snapshot_coherent_for_review_operations,
            Some(false)
        );
        assert!(report
            .reason
            .contains("latest pointer selects non-green archived bundle"));
    }

    #[test]
    fn invalid_bundle_blocks_cleanup_by_default() {
        let state_archive_dir = temp_dir("state_bundle_archive_manager_invalid_state");
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
        let bundle_archive_dir = temp_dir("state_bundle_archive_manager_invalid_archive");
        export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &snapshot);
        let invalid_bundle = bundle_archive_dir.join("state_snapshot_bundle__broken");
        fs::create_dir_all(&invalid_bundle).expect("invalid bundle dir");
        fs::write(
            invalid_bundle.join(activation_artifact_state_bundle::BUNDLE_MANIFEST_FILENAME),
            "{broken",
        )
        .expect("write invalid manifest");

        let report = build_report(&Config {
            bundle_archive_dir,
            bundle_latest_pointer_dir: None,
            pointer_name:
                activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME
                    .to_string(),
            json: false,
            mode: Mode::RetentionApply { keep_latest: 1 },
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleArchiveVerdict::ArtifactStateBundleArchiveCleanupBlockedByInvalidBundle
        );
        assert_eq!(report.invalid_or_drifted_bundle_count, 1);
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

    fn write_pointer_metadata(
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
                    .expect("canonical archive")
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
                    .expect("source archive dir"),
                selected_snapshot_source_path: summary
                    .selected_snapshot_path
                    .expect("selected snapshot path"),
                selected_snapshot_file_name: summary
                    .selected_snapshot_file_name
                    .expect("selected snapshot file name"),
                snapshotted_at: summary.snapshotted_at.expect("snapshotted_at"),
                snapshot_state_verdict: summary
                    .selected_snapshot_state_verdict
                    .expect("snapshot state verdict"),
                snapshot_state_reason: summary
                    .selected_snapshot_state_reason
                    .expect("snapshot state reason"),
                selected_review_generation_id: summary.selected_review_generation_id.clone(),
                selected_latest_release_generation_id: summary
                    .selected_latest_release_generation_id
                    .clone(),
                ambiguous_legacy_count: summary.ambiguous_legacy_count.unwrap_or_default(),
                coherent_for_review_operations: summary
                    .coherent_for_review_operations
                    .unwrap_or(false),
                pointed_at: ts("2026-03-26T20:05:00Z"),
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

    fn write_missing_target_pointer_metadata(
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
                    .expect("canonical archive")
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
                pointed_at: ts("2026-03-26T20:05:00Z"),
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
