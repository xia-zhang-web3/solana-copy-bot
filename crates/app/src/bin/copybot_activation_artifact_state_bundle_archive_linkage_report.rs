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
#[path = "copybot_activation_artifact_state_bundle.rs"]
mod activation_artifact_state_bundle;
#[allow(dead_code)]
#[path = "copybot_activation_artifact_state_bundle_publish_report.rs"]
mod activation_artifact_state_bundle_publish_report;
#[allow(dead_code)]
#[path = "copybot_activation_artifact_state_publish_report.rs"]
mod activation_artifact_state_publish_report;
#[allow(dead_code)]
#[path = "copybot_activation_artifact_state_report.rs"]
mod activation_artifact_state_report;

const USAGE: &str = "usage: copybot_activation_artifact_state_bundle_archive_linkage_report --bundle-archive-dir <path> --state-archive-dir <path> --review-archive-dir <path> --review-manifest-dir <path> --review-bundle-dir <path> --review-channel-dir <path> --release-archive-dir <path> --release-history-dir <path> --latest-pointer-dir <path> [--bundle-latest-pointer-dir <path>] [--snapshot-latest-pointer-dir <path>] [--bundle-pointer-name <name>] [--snapshot-pointer-name <name>] [--review-channel-name <name>] [--latest-pointer-name <name>] [--json]";

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
enum ArtifactStateBundleArchiveLinkageVerdict {
    ArtifactStateBundleArchiveLinkageComplete,
    ArtifactStateBundleArchiveLinkageIncomplete,
    ArtifactStateBundleArchiveLinkageInvalidArtifactsPresent,
    ArtifactStateBundleArchiveLinkageInconsistent,
    ArtifactStateBundleArchiveLinkageAmbiguousLegacyState,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ArchivedBundleLinkageVerdict {
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
struct SnapshotPointerContext {
    requested: bool,
    pointer_verdict: Option<String>,
    pointer_reason: String,
    pointer_path: Option<String>,
    selected_snapshot_path: Option<String>,
    identity_key: Option<String>,
    canonical_selected_snapshot_path: Option<PathBuf>,
    target_exists: bool,
    target_matches_identity: bool,
    present_in_state_archive: bool,
}

#[derive(Debug, Clone, Serialize)]
struct SnapshotLatestPointerSummary {
    requested: bool,
    pointer_name: String,
    pointer_verdict: Option<String>,
    pointer_reason: String,
    pointer_path: Option<String>,
    selected_snapshot_path: Option<String>,
    target_exists: bool,
    target_matches_identity: bool,
    present_in_state_archive: bool,
}

#[derive(Debug, Clone, Serialize)]
struct ArchivedBundleLinkageSummary {
    bundle_path: String,
    bundle_dir_name: String,
    identity_key: String,
    snapshotted_at: Option<DateTime<Utc>>,
    selected_snapshot_path: Option<String>,
    selected_snapshot_state_verdict: Option<String>,
    selected_snapshot_state_reason: Option<String>,
    linkage_verdict: ArchivedBundleLinkageVerdict,
    linkage_reason: String,
    selected_review_generation_id: Option<String>,
    selected_latest_release_generation_id: Option<String>,
    coherent_for_review_operations: Option<bool>,
    ambiguous_legacy_count: Option<usize>,
    present_in_bundle_archive: bool,
    present_in_current_state_archive: bool,
    matches_current_snapshot_latest_pointer: Option<bool>,
    matches_latest_state_archive: Option<bool>,
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
struct LatestBundlePointerLinkageSummary {
    requested: bool,
    pointer_name: String,
    pointer_verdict: Option<String>,
    pointer_reason: String,
    pointer_path: Option<String>,
    selected_bundle_path: Option<String>,
    selected_snapshot_path: Option<String>,
    selected_bundle_linkage_verdict: Option<ArchivedBundleLinkageVerdict>,
    selected_bundle_linkage_reason: Option<String>,
    target_exists: bool,
    target_matches_identity: bool,
    selected_bundle_loaded: bool,
    present_in_bundle_archive: bool,
    present_in_current_state_archive: bool,
    selected_review_generation_id: Option<String>,
    selected_latest_release_generation_id: Option<String>,
    selected_bundle_resolves_live_generations: bool,
    selected_bundle_matches_current_review_channel: bool,
    selected_bundle_matches_current_latest_release: bool,
    selected_bundle_matches_current_snapshot_latest_pointer: Option<bool>,
    selected_bundle_matches_latest_state_archive: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
struct ArtifactStateBundleArchiveLinkageReport {
    mode: String,
    verdict: ArtifactStateBundleArchiveLinkageVerdict,
    reason: String,
    bundle_archive_dir: String,
    bundle_latest_pointer_dir: Option<String>,
    bundle_pointer_name: String,
    state_archive_dir: String,
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
    current_state_archive_latest_snapshot_path: Option<String>,
    current_state_snapshot_latest_pointer: SnapshotLatestPointerSummary,
    archived_bundle_count_examined: usize,
    valid_archived_bundle_count: usize,
    bundles_resolving_live_generations_count: usize,
    missing_selected_review_generation_count: usize,
    missing_selected_release_generation_count: usize,
    diverged_from_current_review_channel_count: usize,
    diverged_from_current_latest_release_count: usize,
    diverged_from_current_snapshot_latest_pointer_count: usize,
    diverged_from_latest_state_archive_count: usize,
    ambiguous_non_green_archived_bundle_count: usize,
    invalid_artifact_count: usize,
    invalid_artifacts: Vec<LinkageIssue>,
    representative_bundle_source: Option<String>,
    representative_bundle_path: Option<String>,
    representative_bundle_linkage_verdict: Option<ArchivedBundleLinkageVerdict>,
    representative_bundle_linkage_reason: Option<String>,
    latest_bundle_pointer: LatestBundlePointerLinkageSummary,
    archived_bundles: Vec<ArchivedBundleLinkageSummary>,
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
    let mut bundle_archive_dir: Option<PathBuf> = None;
    let mut bundle_latest_pointer_dir: Option<PathBuf> = None;
    let mut bundle_pointer_name =
        activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME.to_string();
    let mut state_archive_dir: Option<PathBuf> = None;
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

    Ok(Some(Config {
        bundle_archive_dir: bundle_archive_dir
            .ok_or_else(|| anyhow!("--bundle-archive-dir is required"))?,
        bundle_latest_pointer_dir,
        bundle_pointer_name,
        state_archive_dir: state_archive_dir
            .ok_or_else(|| anyhow!("--state-archive-dir is required"))?,
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

fn build_report(config: &Config) -> Result<ArtifactStateBundleArchiveLinkageReport> {
    let state_report = activation_artifact_state_report::inspect_state_report(
        &activation_artifact_state_report::Config {
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
        },
    )?;

    let review_generation_ids = load_review_generation_ids(&config.review_archive_dir)?;
    let release_generation_ids =
        load_release_generation_ids(&config.release_archive_dir, &config.release_history_dir)?;

    let (state_archive_records, mut invalid_artifacts) =
        load_state_archive_records(&config.state_archive_dir);
    let latest_state_archive = latest_state_record(&state_archive_records);
    let snapshot_pointer_context =
        inspect_snapshot_pointer_context(config, &state_archive_records, &mut invalid_artifacts);

    let bundle_paths = activation_artifact_state_bundle::collect_state_snapshot_bundle_paths(
        Some(&config.bundle_archive_dir),
        &[],
    )?;
    let (bundle_records, invalid_bundles) = load_bundle_records(&bundle_paths);
    invalid_artifacts.extend(invalid_bundles);

    let canonical_state_archive_dir = fs::canonicalize(&config.state_archive_dir).ok();

    let mut bundle_summaries = bundle_records
        .iter()
        .map(|record| {
            summarize_bundle_linkage(
                record,
                true,
                canonical_state_archive_dir.as_deref(),
                &state_archive_records,
                latest_state_archive,
                &snapshot_pointer_context,
                &review_generation_ids,
                &release_generation_ids,
                &state_report,
            )
        })
        .collect::<Vec<_>>();
    bundle_summaries.sort_by(|left, right| {
        left.bundle_dir_name
            .cmp(&right.bundle_dir_name)
            .then_with(|| left.bundle_path.cmp(&right.bundle_path))
    });

    let summary_by_path = bundle_summaries
        .iter()
        .map(|summary| (summary.bundle_path.clone(), summary.clone()))
        .collect::<BTreeMap<_, _>>();

    let (latest_bundle_pointer, pointer_selected_summary) = inspect_bundle_latest_pointer(
        config,
        &summary_by_path,
        canonical_state_archive_dir.as_deref(),
        &state_archive_records,
        latest_state_archive,
        &snapshot_pointer_context,
        &review_generation_ids,
        &release_generation_ids,
        &state_report,
    )?;

    let representative_bundle =
        if latest_bundle_pointer.requested && latest_bundle_pointer.selected_bundle_loaded {
            pointer_selected_summary
                .as_ref()
                .cloned()
                .map(|summary| ("bundle_latest_pointer".to_string(), summary))
        } else {
            bundle_summaries
                .iter()
                .max_by(|left, right| {
                    left.bundle_dir_name
                        .cmp(&right.bundle_dir_name)
                        .then_with(|| left.bundle_path.cmp(&right.bundle_path))
                })
                .cloned()
                .map(|summary| ("latest_archived_bundle".to_string(), summary))
        };

    let bundles_resolving_live_generations_count = bundle_summaries
        .iter()
        .filter(|summary| summary.resolves_live_review_release_generations)
        .count();
    let missing_selected_review_generation_count = bundle_summaries
        .iter()
        .filter(|summary| !summary.selected_review_generation_present_now)
        .count();
    let missing_selected_release_generation_count = bundle_summaries
        .iter()
        .filter(|summary| !summary.selected_latest_release_generation_present_now)
        .count();
    let diverged_from_current_review_channel_count = bundle_summaries
        .iter()
        .filter(|summary| !summary.matches_current_review_channel_selection)
        .count();
    let diverged_from_current_latest_release_count = bundle_summaries
        .iter()
        .filter(|summary| !summary.matches_current_latest_release_selection)
        .count();
    let diverged_from_current_snapshot_latest_pointer_count = bundle_summaries
        .iter()
        .filter(|summary| summary.matches_current_snapshot_latest_pointer == Some(false))
        .count();
    let diverged_from_latest_state_archive_count = bundle_summaries
        .iter()
        .filter(|summary| summary.matches_latest_state_archive == Some(false))
        .count();
    let ambiguous_non_green_archived_bundle_count = bundle_summaries
        .iter()
        .filter(|summary| {
            summary
                .selected_snapshot_state_verdict
                .as_deref()
                .is_some_and(|verdict| verdict != "artifact_state_coherent")
                || summary.ambiguous_legacy_count.unwrap_or_default() > 0
        })
        .count();

    let (verdict, reason) = determine_report_verdict(
        &state_report,
        &invalid_artifacts,
        representative_bundle.as_ref().map(|(_, summary)| summary),
        &latest_bundle_pointer,
        &snapshot_pointer_context,
    );

    Ok(ArtifactStateBundleArchiveLinkageReport {
        mode: "artifact_state_bundle_archive_linkage_report".to_string(),
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
        current_state_archive_latest_snapshot_path: latest_state_archive
            .map(|record| record.canonical_path.display().to_string()),
        current_state_snapshot_latest_pointer: SnapshotLatestPointerSummary {
            requested: snapshot_pointer_context.requested,
            pointer_name: config.snapshot_pointer_name.clone(),
            pointer_verdict: snapshot_pointer_context.pointer_verdict.clone(),
            pointer_reason: snapshot_pointer_context.pointer_reason.clone(),
            pointer_path: snapshot_pointer_context.pointer_path.clone(),
            selected_snapshot_path: snapshot_pointer_context.selected_snapshot_path.clone(),
            target_exists: snapshot_pointer_context.target_exists,
            target_matches_identity: snapshot_pointer_context.target_matches_identity,
            present_in_state_archive: snapshot_pointer_context.present_in_state_archive,
        },
        archived_bundle_count_examined: bundle_paths.len(),
        valid_archived_bundle_count: bundle_summaries.len(),
        bundles_resolving_live_generations_count,
        missing_selected_review_generation_count,
        missing_selected_release_generation_count,
        diverged_from_current_review_channel_count,
        diverged_from_current_latest_release_count,
        diverged_from_current_snapshot_latest_pointer_count,
        diverged_from_latest_state_archive_count,
        ambiguous_non_green_archived_bundle_count,
        invalid_artifact_count: invalid_artifacts.len(),
        invalid_artifacts,
        representative_bundle_source: representative_bundle
            .as_ref()
            .map(|(source, _)| source.clone()),
        representative_bundle_path: representative_bundle
            .as_ref()
            .map(|(_, summary)| summary.bundle_path.clone()),
        representative_bundle_linkage_verdict: representative_bundle
            .as_ref()
            .map(|(_, summary)| summary.linkage_verdict),
        representative_bundle_linkage_reason: representative_bundle
            .as_ref()
            .map(|(_, summary)| summary.linkage_reason.clone()),
        latest_bundle_pointer,
        archived_bundles: bundle_summaries,
        artifact_analysis_only: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary: "Archived state-bundle linkage analysis only correlates deterministic archived bundles with the current persisted state-snapshot surface and the live review/release artifact chain. It does not authorize activation, does not enable execution, and does not override the Stage 3 production gate.".to_string(),
    })
}

fn determine_report_verdict(
    state_report: &activation_artifact_state_report::ArtifactStateReport,
    invalid_artifacts: &[LinkageIssue],
    representative_bundle: Option<&ArchivedBundleLinkageSummary>,
    latest_bundle_pointer: &LatestBundlePointerLinkageSummary,
    snapshot_pointer: &SnapshotPointerContext,
) -> (ArtifactStateBundleArchiveLinkageVerdict, String) {
    if !invalid_artifacts.is_empty()
        || state_report.verdict
            == activation_artifact_state_report::ArtifactStateVerdict::ArtifactStateInvalidArtifactsPresent
        || latest_bundle_pointer.pointer_verdict.as_deref()
            == Some("artifact_state_bundle_report_invalid_metadata")
        || snapshot_pointer.pointer_verdict.as_deref()
            == Some("artifact_state_snapshot_invalid_metadata")
    {
        return (
            ArtifactStateBundleArchiveLinkageVerdict::ArtifactStateBundleArchiveLinkageInvalidArtifactsPresent,
            "archived state-bundle linkage inputs contain invalid artifacts or invalid pointer metadata".to_string(),
        );
    }

    let bundle_pointer_missing_target = latest_bundle_pointer.requested
        && latest_bundle_pointer.pointer_verdict.as_deref()
            == Some("artifact_state_bundle_report_verify_missing_target")
        && latest_bundle_pointer.pointer_path.is_some()
        && !latest_bundle_pointer.target_exists;
    let bundle_pointer_missing_metadata = latest_bundle_pointer.requested
        && latest_bundle_pointer.pointer_verdict.as_deref()
            == Some("artifact_state_bundle_report_verify_missing_target")
        && latest_bundle_pointer.pointer_path.is_none();
    let snapshot_pointer_missing_target = snapshot_pointer.requested
        && snapshot_pointer.pointer_verdict.as_deref()
            == Some("artifact_state_snapshot_verify_missing_target")
        && snapshot_pointer.pointer_path.is_some()
        && !snapshot_pointer.target_exists;
    let snapshot_pointer_missing_metadata = snapshot_pointer.requested
        && snapshot_pointer.pointer_verdict.as_deref()
            == Some("artifact_state_snapshot_verify_missing_target")
        && snapshot_pointer.pointer_path.is_none();

    if bundle_pointer_missing_target {
        return (
            ArtifactStateBundleArchiveLinkageVerdict::ArtifactStateBundleArchiveLinkageInconsistent,
            "state snapshot bundle latest pointer metadata exists, but its target archived bundle is missing".to_string(),
        );
    }
    if snapshot_pointer_missing_target {
        return (
            ArtifactStateBundleArchiveLinkageVerdict::ArtifactStateBundleArchiveLinkageInconsistent,
            "current state snapshot latest pointer metadata exists, but its target state snapshot artifact is missing".to_string(),
        );
    }

    if state_report.verdict
        == activation_artifact_state_report::ArtifactStateVerdict::ArtifactStateInconsistent
        || representative_bundle.is_some_and(|summary| {
            summary.linkage_verdict == ArchivedBundleLinkageVerdict::Inconsistent
        })
    {
        return (
            ArtifactStateBundleArchiveLinkageVerdict::ArtifactStateBundleArchiveLinkageInconsistent,
            representative_bundle
                .map(|summary| summary.linkage_reason.clone())
                .filter(|reason| !reason.is_empty())
                .unwrap_or_else(|| {
                    "the current or representative archived bundle no longer links cleanly to the current artifact chain".to_string()
                }),
        );
    }

    if state_report.verdict
        == activation_artifact_state_report::ArtifactStateVerdict::ArtifactStateIncomplete
        || representative_bundle.is_none()
        || bundle_pointer_missing_metadata
        || snapshot_pointer_missing_metadata
        || representative_bundle.is_some_and(|summary| {
            summary.linkage_verdict == ArchivedBundleLinkageVerdict::Incomplete
        })
    {
        return (
            ArtifactStateBundleArchiveLinkageVerdict::ArtifactStateBundleArchiveLinkageIncomplete,
            representative_bundle
                .map(|summary| summary.linkage_reason.clone())
                .filter(|reason| !reason.is_empty())
                .unwrap_or_else(|| {
                    if bundle_pointer_missing_metadata {
                        "no latest archived-bundle pointer metadata was found, so current archived-bundle linkage cannot be confirmed".to_string()
                    } else if snapshot_pointer_missing_metadata {
                        "no current state snapshot latest pointer metadata was found, so current snapshot linkage context cannot be confirmed".to_string()
                    } else {
                        "the representative archived bundle does not fully resolve against the current artifact chain".to_string()
                    }
                }),
        );
    }

    if state_report.verdict
        == activation_artifact_state_report::ArtifactStateVerdict::ArtifactStateAmbiguousLegacyState
        || representative_bundle.is_some_and(|summary| {
            summary.linkage_verdict == ArchivedBundleLinkageVerdict::AmbiguousLegacyState
        })
    {
        return (
            ArtifactStateBundleArchiveLinkageVerdict::ArtifactStateBundleArchiveLinkageAmbiguousLegacyState,
            representative_bundle
                .map(|summary| summary.linkage_reason.clone())
                .filter(|reason| !reason.is_empty())
                .unwrap_or_else(|| {
                    "the representative archived bundle depends on ambiguous legacy snapshot truth and cannot be treated as clean linkage".to_string()
                }),
        );
    }

    (
        ArtifactStateBundleArchiveLinkageVerdict::ArtifactStateBundleArchiveLinkageComplete,
        "the representative deterministic archived bundle still resolves cleanly against the current review and release artifact chain".to_string(),
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

fn load_state_archive_records(
    state_archive_dir: &Path,
) -> (Vec<StateSnapshotRecord>, Vec<LinkageIssue>) {
    let mut valid = Vec::new();
    let mut invalid = Vec::new();
    for path in collect_json_paths(state_archive_dir).unwrap_or_default() {
        match activation_artifact_state_publish_report::inspect_state_snapshot_artifact(&path) {
            Ok(loaded) => valid.push(StateSnapshotRecord {
                identity_key: identity_key_from_snapshot(&loaded.artifact),
                canonical_path: loaded.canonical_path.clone(),
                loaded,
            }),
            Err(error) => invalid.push(LinkageIssue {
                surface: "state_snapshot_archive".to_string(),
                path: path.display().to_string(),
                error: format!("{error:#}"),
            }),
        }
    }
    valid.sort_by(|left, right| {
        left.loaded
            .artifact
            .snapshotted_at
            .cmp(&right.loaded.artifact.snapshotted_at)
            .then_with(|| left.canonical_path.cmp(&right.canonical_path))
    });
    (valid, invalid)
}

fn load_bundle_records(paths: &[PathBuf]) -> (Vec<ArchivedBundleRecord>, Vec<LinkageIssue>) {
    let mut valid = Vec::new();
    let mut invalid = Vec::new();
    for path in paths {
        match activation_artifact_state_bundle::inspect_state_snapshot_bundle(path) {
            Ok(summary) if summary.verdict == "artifact_state_bundle_verified" => {
                let Some(snapshotted_at) = summary.snapshotted_at else {
                    invalid.push(LinkageIssue {
                        surface: "archived_bundle".to_string(),
                        path: summary.bundle_path,
                        error: "verified archived bundle summary is missing snapshotted_at"
                            .to_string(),
                    });
                    continue;
                };
                let Some(state_verdict) = summary.selected_snapshot_state_verdict.as_deref() else {
                    invalid.push(LinkageIssue {
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
            Ok(summary) => invalid.push(LinkageIssue {
                surface: "archived_bundle".to_string(),
                path: summary.bundle_path,
                error: format!(
                    "bundle verification returned `{}`: {}",
                    summary.verdict, summary.reason
                ),
            }),
            Err(error) => invalid.push(LinkageIssue {
                surface: "archived_bundle".to_string(),
                path: path.display().to_string(),
                error: format!("{error:#}"),
            }),
        }
    }
    valid.sort_by(|left, right| {
        left.bundle_dir_name
            .cmp(&right.bundle_dir_name)
            .then_with(|| left.bundle_root.cmp(&right.bundle_root))
    });
    (valid, invalid)
}

fn inspect_snapshot_pointer_context(
    config: &Config,
    state_archive_records: &[StateSnapshotRecord],
    invalid_artifacts: &mut Vec<LinkageIssue>,
) -> SnapshotPointerContext {
    let Some(pointer_dir) = &config.snapshot_latest_pointer_dir else {
        return SnapshotPointerContext {
            requested: false,
            pointer_verdict: None,
            pointer_reason: "current state snapshot latest pointer was not requested".to_string(),
            pointer_path: None,
            selected_snapshot_path: None,
            identity_key: None,
            canonical_selected_snapshot_path: None,
            target_exists: false,
            target_matches_identity: false,
            present_in_state_archive: false,
        };
    };

    let loaded_metadata =
        match activation_artifact_state_publish_report::inspect_latest_pointer_metadata(
            pointer_dir,
            &config.snapshot_pointer_name,
        ) {
            Ok(metadata) => metadata,
            Err(error) => {
                invalid_artifacts.push(LinkageIssue {
                    surface: "snapshot_latest_pointer".to_string(),
                    path: pointer_dir
                        .join(format!("{}.json", config.snapshot_pointer_name))
                        .display()
                        .to_string(),
                    error: format!("{error:#}"),
                });
                None
            }
        };

    let inspected = match activation_artifact_state_publish_report::inspect_latest_pointer_report(
        &config.state_archive_dir,
        pointer_dir,
        &config.snapshot_pointer_name,
        true,
    ) {
        Ok(report) => report,
        Err(error) => {
            invalid_artifacts.push(LinkageIssue {
                surface: "snapshot_latest_pointer".to_string(),
                path: pointer_dir
                    .join(format!("{}.json", config.snapshot_pointer_name))
                    .display()
                    .to_string(),
                error: format!("{error:#}"),
            });
            activation_artifact_state_publish_report::ArtifactStateSnapshotPublishReport {
                mode: "verify_latest".to_string(),
                verdict:
                    activation_artifact_state_publish_report::ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotInvalidMetadata,
                reason: "failed inspecting current state snapshot latest pointer".to_string(),
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
                latest_pointer_path: None,
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
                not_authorized_summary: "artifact-state pointer inspection only".to_string(),
            }
        }
    };

    let selected_snapshot_path = inspected.persisted_state_snapshot_path.clone().or_else(|| {
        loaded_metadata
            .as_ref()
            .map(|loaded| loaded.metadata.selected_snapshot_path.clone())
    });
    let canonical_selected_snapshot_path = selected_snapshot_path
        .as_ref()
        .and_then(|path| canonicalize_existing_path(Path::new(path)));
    let identity_key = loaded_metadata
        .as_ref()
        .map(|loaded| identity_key_from_state_pointer(&loaded.metadata));
    let present_in_state_archive = canonical_selected_snapshot_path
        .as_ref()
        .is_some_and(|path| {
            state_archive_records
                .iter()
                .any(|record| &record.canonical_path == path)
        });

    SnapshotPointerContext {
        requested: true,
        pointer_verdict: Some(serialize_enum(&inspected.verdict)),
        pointer_reason: inspected.reason,
        pointer_path: inspected.latest_pointer_path.or_else(|| {
            loaded_metadata
                .as_ref()
                .map(|loaded| loaded.path.display().to_string())
        }),
        selected_snapshot_path,
        identity_key,
        canonical_selected_snapshot_path,
        target_exists: inspected.latest_pointer_target_exists,
        target_matches_identity: inspected.latest_pointer_target_matches_identity,
        present_in_state_archive,
    }
}

fn inspect_bundle_latest_pointer(
    config: &Config,
    summaries_by_path: &BTreeMap<String, ArchivedBundleLinkageSummary>,
    canonical_state_archive_dir: Option<&Path>,
    state_archive_records: &[StateSnapshotRecord],
    latest_state_archive: Option<&StateSnapshotRecord>,
    snapshot_pointer_context: &SnapshotPointerContext,
    review_generation_ids: &BTreeSet<String>,
    release_generation_ids: &BTreeSet<String>,
    state_report: &activation_artifact_state_report::ArtifactStateReport,
) -> Result<(
    LatestBundlePointerLinkageSummary,
    Option<ArchivedBundleLinkageSummary>,
)> {
    let Some(pointer_dir) = &config.bundle_latest_pointer_dir else {
        return Ok((
            LatestBundlePointerLinkageSummary {
                requested: false,
                pointer_name: config.bundle_pointer_name.clone(),
                pointer_verdict: None,
                pointer_reason: "archived bundle latest pointer was not requested".to_string(),
                pointer_path: None,
                selected_bundle_path: None,
                selected_snapshot_path: None,
                selected_bundle_linkage_verdict: None,
                selected_bundle_linkage_reason: None,
                target_exists: false,
                target_matches_identity: false,
                selected_bundle_loaded: false,
                present_in_bundle_archive: false,
                present_in_current_state_archive: false,
                selected_review_generation_id: None,
                selected_latest_release_generation_id: None,
                selected_bundle_resolves_live_generations: false,
                selected_bundle_matches_current_review_channel: false,
                selected_bundle_matches_current_latest_release: false,
                selected_bundle_matches_current_snapshot_latest_pointer: None,
                selected_bundle_matches_latest_state_archive: None,
            },
            None,
        ));
    };

    let loaded_metadata =
        activation_artifact_state_bundle_publish_report::inspect_latest_bundle_pointer_metadata(
            pointer_dir,
            &config.bundle_pointer_name,
        )
        .ok()
        .flatten();
    let inspected =
        activation_artifact_state_bundle_publish_report::inspect_latest_bundle_pointer_report(
            &config.bundle_archive_dir,
            pointer_dir,
            &config.bundle_pointer_name,
            true,
        )?;

    let selected_bundle_path = inspected.persisted_bundle_path.clone().or_else(|| {
        loaded_metadata
            .as_ref()
            .map(|loaded| loaded.metadata.selected_bundle_path.clone())
    });

    let selected_summary = selected_bundle_path
        .as_ref()
        .and_then(|path| canonicalize_existing_path(Path::new(path)))
        .and_then(|path| {
            summaries_by_path
                .get(path.to_string_lossy().as_ref())
                .cloned()
        })
        .or_else(|| {
            selected_bundle_path.as_ref().and_then(|path| {
                let canonical = canonicalize_existing_path(Path::new(path))?;
                let summary =
                    activation_artifact_state_bundle::inspect_state_snapshot_bundle(&canonical)
                        .ok()?;
                if summary.verdict != "artifact_state_bundle_verified" {
                    return None;
                }
                let record = ArchivedBundleRecord {
                    identity_key: identity_key_from_values(
                        summary.snapshotted_at?,
                        summary.selected_snapshot_state_verdict.as_deref()?,
                        summary.selected_review_generation_id.as_deref(),
                        summary.selected_latest_release_generation_id.as_deref(),
                    ),
                    bundle_dir_name: canonical
                        .file_name()
                        .map(|value| value.to_string_lossy().into_owned())
                        .unwrap_or_else(|| canonical.display().to_string()),
                    bundle_root: canonical,
                    summary,
                };
                Some(summarize_bundle_linkage(
                    &record,
                    false,
                    canonical_state_archive_dir,
                    state_archive_records,
                    latest_state_archive,
                    snapshot_pointer_context,
                    review_generation_ids,
                    release_generation_ids,
                    state_report,
                ))
            })
        });

    let selected_review_generation_id = selected_summary
        .as_ref()
        .and_then(|summary| summary.selected_review_generation_id.clone())
        .or_else(|| {
            loaded_metadata
                .as_ref()
                .and_then(|loaded| loaded.metadata.selected_review_generation_id.clone())
        });
    let selected_latest_release_generation_id = selected_summary
        .as_ref()
        .and_then(|summary| summary.selected_latest_release_generation_id.clone())
        .or_else(|| {
            loaded_metadata.as_ref().and_then(|loaded| {
                loaded
                    .metadata
                    .selected_latest_release_generation_id
                    .clone()
            })
        });

    Ok((
        LatestBundlePointerLinkageSummary {
            requested: true,
            pointer_name: config.bundle_pointer_name.clone(),
            pointer_verdict: Some(serialize_enum(&inspected.verdict)),
            pointer_reason: inspected.reason.clone(),
            pointer_path: inspected.latest_pointer_path.clone().or_else(|| {
                loaded_metadata
                    .as_ref()
                    .map(|loaded| loaded.path.display().to_string())
            }),
            selected_bundle_path,
            selected_snapshot_path: selected_summary
                .as_ref()
                .and_then(|summary| summary.selected_snapshot_path.clone())
                .or_else(|| {
                    loaded_metadata
                        .as_ref()
                        .map(|loaded| loaded.metadata.selected_snapshot_source_path.clone())
                }),
            selected_bundle_linkage_verdict: selected_summary
                .as_ref()
                .map(|summary| summary.linkage_verdict),
            selected_bundle_linkage_reason: selected_summary
                .as_ref()
                .map(|summary| summary.linkage_reason.clone()),
            target_exists: inspected.latest_pointer_target_exists,
            target_matches_identity: inspected.latest_pointer_target_matches_identity,
            selected_bundle_loaded: selected_summary.is_some(),
            present_in_bundle_archive: selected_summary
                .as_ref()
                .is_some_and(|summary| summary.present_in_bundle_archive),
            present_in_current_state_archive: selected_summary
                .as_ref()
                .is_some_and(|summary| summary.present_in_current_state_archive),
            selected_review_generation_id,
            selected_latest_release_generation_id,
            selected_bundle_resolves_live_generations: selected_summary
                .as_ref()
                .is_some_and(|summary| summary.resolves_live_review_release_generations),
            selected_bundle_matches_current_review_channel: selected_summary
                .as_ref()
                .is_some_and(|summary| summary.matches_current_review_channel_selection),
            selected_bundle_matches_current_latest_release: selected_summary
                .as_ref()
                .is_some_and(|summary| summary.matches_current_latest_release_selection),
            selected_bundle_matches_current_snapshot_latest_pointer: selected_summary
                .as_ref()
                .and_then(|summary| summary.matches_current_snapshot_latest_pointer),
            selected_bundle_matches_latest_state_archive: selected_summary
                .as_ref()
                .and_then(|summary| summary.matches_latest_state_archive),
        },
        selected_summary,
    ))
}

fn summarize_bundle_linkage(
    record: &ArchivedBundleRecord,
    present_in_bundle_archive: bool,
    canonical_state_archive_dir: Option<&Path>,
    state_archive_records: &[StateSnapshotRecord],
    latest_state_archive: Option<&StateSnapshotRecord>,
    snapshot_pointer_context: &SnapshotPointerContext,
    review_generation_ids: &BTreeSet<String>,
    release_generation_ids: &BTreeSet<String>,
    state_report: &activation_artifact_state_report::ArtifactStateReport,
) -> ArchivedBundleLinkageSummary {
    let state_verdict = record
        .summary
        .selected_snapshot_state_verdict
        .clone()
        .unwrap_or_else(|| "<unknown>".to_string());
    let state_reason = record
        .summary
        .selected_snapshot_state_reason
        .clone()
        .unwrap_or_else(|| "<unknown>".to_string());
    let coherent_for_review_operations = record.summary.coherent_for_review_operations;
    let ambiguous_legacy_count = record.summary.ambiguous_legacy_count;
    let selected_review_generation_id = record.summary.selected_review_generation_id.clone();
    let selected_latest_release_generation_id =
        record.summary.selected_latest_release_generation_id.clone();

    let selected_review_generation_present_now = selected_review_generation_id
        .as_ref()
        .is_some_and(|generation_id| review_generation_ids.contains(generation_id));
    let selected_latest_release_generation_present_now = selected_latest_release_generation_id
        .as_ref()
        .is_some_and(|generation_id| release_generation_ids.contains(generation_id));
    let resolves_live_review_release_generations =
        selected_review_generation_present_now && selected_latest_release_generation_present_now;
    let matches_current_review_channel_selection = selected_review_generation_id
        == state_report
            .current_review_generation
            .selected_generation_id;
    let matches_current_latest_release_selection =
        selected_latest_release_generation_id == state_report.current_latest_release.generation_id;
    let present_in_current_state_archive = bundle_matches_current_state_archive(
        record,
        canonical_state_archive_dir,
        state_archive_records,
    );
    let matches_current_snapshot_latest_pointer =
        if snapshot_pointer_context.requested && snapshot_pointer_context.identity_key.is_some() {
            Some(bundle_matches_snapshot_pointer(
                record,
                snapshot_pointer_context,
            ))
        } else {
            None
        };
    let matches_latest_state_archive =
        latest_state_archive.map(|latest| bundle_matches_latest_state_archive(record, latest));

    let mut incomplete_reasons = Vec::new();
    let mut inconsistencies = Vec::new();
    let mut ambiguities = Vec::new();

    match state_verdict.as_str() {
        "artifact_state_inconsistent" => inconsistencies.push(
            "archived bundle encapsulates an inconsistent artifact-state snapshot".to_string(),
        ),
        "artifact_state_incomplete" => incomplete_reasons
            .push("archived bundle encapsulates an incomplete artifact-state snapshot".to_string()),
        "artifact_state_ambiguous_legacy_state" => ambiguities
            .push("archived bundle encapsulates ambiguous legacy artifact-state truth".to_string()),
        "artifact_state_invalid_artifacts_present" => inconsistencies
            .push("archived bundle encapsulates invalid current-state artifacts".to_string()),
        _ => {}
    }

    if state_verdict == "artifact_state_coherent" && coherent_for_review_operations == Some(false) {
        inconsistencies.push(
            "archived bundle claims coherent snapshot state_verdict but is not coherent for review operations"
                .to_string(),
        );
    }
    if ambiguous_legacy_count.unwrap_or_default() > 0
        && state_verdict != "artifact_state_ambiguous_legacy_state"
    {
        ambiguities.push(format!(
            "archived bundle carries {} ambiguous legacy conditions",
            ambiguous_legacy_count.unwrap_or_default()
        ));
    }

    if !selected_review_generation_present_now {
        incomplete_reasons.push(
            selected_review_generation_id
                .as_ref()
                .map(|generation_id| {
                    format!(
                        "selected review generation `{generation_id}` no longer resolves in the current review archive"
                    )
                })
                .unwrap_or_else(|| {
                    "archived bundle does not record a selected review generation".to_string()
                }),
        );
    }
    if !selected_latest_release_generation_present_now {
        incomplete_reasons.push(
            selected_latest_release_generation_id
                .as_ref()
                .map(|generation_id| {
                    format!(
                        "selected latest release generation `{generation_id}` no longer resolves in the current release archive/history"
                    )
                })
                .unwrap_or_else(|| {
                    "archived bundle does not record a selected latest release generation"
                        .to_string()
                }),
        );
    }
    if resolves_live_review_release_generations && !matches_current_review_channel_selection {
        incomplete_reasons.push(
            "archived bundle selected review generation no longer matches the current review channel selection"
                .to_string(),
        );
    }
    if resolves_live_review_release_generations && !matches_current_latest_release_selection {
        incomplete_reasons.push(
            "archived bundle selected latest release generation no longer matches the current latest release selection"
                .to_string(),
        );
    }
    if !present_in_current_state_archive {
        incomplete_reasons.push(
            "archived bundle selected snapshot no longer resolves in the current persisted state snapshot archive"
                .to_string(),
        );
    }
    if matches_current_snapshot_latest_pointer == Some(false) {
        incomplete_reasons.push(
            "archived bundle selected snapshot no longer matches the current state snapshot latest pointer"
                .to_string(),
        );
    }
    if matches_latest_state_archive == Some(false) {
        incomplete_reasons.push(
            "archived bundle selected snapshot is behind the latest persisted state snapshot in the current archive"
                .to_string(),
        );
    }

    let (linkage_verdict, linkage_reason) = if !inconsistencies.is_empty() {
        (
            ArchivedBundleLinkageVerdict::Inconsistent,
            inconsistencies
                .first()
                .cloned()
                .unwrap_or_else(|| "archived bundle linkage is inconsistent".to_string()),
        )
    } else if !incomplete_reasons.is_empty() {
        (
            ArchivedBundleLinkageVerdict::Incomplete,
            incomplete_reasons
                .first()
                .cloned()
                .unwrap_or_else(|| "archived bundle linkage is incomplete".to_string()),
        )
    } else if !ambiguities.is_empty() {
        (
            ArchivedBundleLinkageVerdict::AmbiguousLegacyState,
            ambiguities
                .first()
                .cloned()
                .unwrap_or_else(|| "archived bundle linkage is ambiguous".to_string()),
        )
    } else {
        (
            ArchivedBundleLinkageVerdict::Complete,
            "archived bundle selections still resolve cleanly against the current review and release artifact chain"
                .to_string(),
        )
    };

    ArchivedBundleLinkageSummary {
        bundle_path: record.bundle_root.display().to_string(),
        bundle_dir_name: record.bundle_dir_name.clone(),
        identity_key: record.identity_key.clone(),
        snapshotted_at: record.summary.snapshotted_at,
        selected_snapshot_path: record.summary.selected_snapshot_path.clone(),
        selected_snapshot_state_verdict: Some(state_verdict),
        selected_snapshot_state_reason: Some(state_reason),
        linkage_verdict,
        linkage_reason,
        selected_review_generation_id,
        selected_latest_release_generation_id,
        coherent_for_review_operations,
        ambiguous_legacy_count,
        present_in_bundle_archive,
        present_in_current_state_archive,
        matches_current_snapshot_latest_pointer,
        matches_latest_state_archive,
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

fn bundle_matches_current_state_archive(
    record: &ArchivedBundleRecord,
    canonical_state_archive_dir: Option<&Path>,
    state_archive_records: &[StateSnapshotRecord],
) -> bool {
    let Some(canonical_state_archive_dir) = canonical_state_archive_dir else {
        return false;
    };
    let Some(source_state_archive_dir) = record.summary.source_state_archive_dir.as_deref() else {
        return false;
    };
    if Path::new(source_state_archive_dir) != canonical_state_archive_dir {
        return false;
    }
    let Some(selected_snapshot_path) = record.summary.selected_snapshot_path.as_deref() else {
        return false;
    };
    state_archive_records.iter().any(|state| {
        state.identity_key == record.identity_key
            && state.canonical_path == Path::new(selected_snapshot_path)
    })
}

fn bundle_matches_snapshot_pointer(
    record: &ArchivedBundleRecord,
    snapshot_pointer_context: &SnapshotPointerContext,
) -> bool {
    snapshot_pointer_context
        .identity_key
        .as_ref()
        .is_some_and(|identity| identity == &record.identity_key)
        && snapshot_pointer_context
            .canonical_selected_snapshot_path
            .as_ref()
            .zip(record.summary.selected_snapshot_path.as_deref())
            .is_some_and(|(selected_path, bundle_path)| selected_path == Path::new(bundle_path))
}

fn bundle_matches_latest_state_archive(
    record: &ArchivedBundleRecord,
    latest_state_archive: &StateSnapshotRecord,
) -> bool {
    record.identity_key == latest_state_archive.identity_key
        && record
            .summary
            .selected_snapshot_path
            .as_deref()
            .is_some_and(|path| Path::new(path) == latest_state_archive.canonical_path)
}

fn latest_state_record(records: &[StateSnapshotRecord]) -> Option<&StateSnapshotRecord> {
    records.iter().max_by(|left, right| {
        left.loaded
            .artifact
            .snapshotted_at
            .cmp(&right.loaded.artifact.snapshotted_at)
            .then_with(|| left.canonical_path.cmp(&right.canonical_path))
    })
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

fn identity_key_from_state_pointer(
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

fn canonicalize_existing_path(path: &Path) -> Option<PathBuf> {
    if !path.exists() {
        return None;
    }
    fs::canonicalize(path).ok()
}

fn serialize_enum<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value)
        .unwrap_or_default()
        .trim_matches('"')
        .to_string()
}

fn render_human(report: &ArtifactStateBundleArchiveLinkageReport) -> String {
    let bundle_pointer_verdict = report
        .latest_bundle_pointer
        .pointer_verdict
        .clone()
        .unwrap_or_else(|| "not_requested".to_string());
    let snapshot_pointer_verdict = report
        .current_state_snapshot_latest_pointer
        .pointer_verdict
        .clone()
        .unwrap_or_else(|| "not_requested".to_string());
    [
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!(
            "archived_bundle_count_examined={}",
            report.archived_bundle_count_examined
        ),
        format!(
            "valid_archived_bundle_count={}",
            report.valid_archived_bundle_count
        ),
        format!(
            "bundles_resolving_live_generations_count={}",
            report.bundles_resolving_live_generations_count
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
            "diverged_from_current_snapshot_latest_pointer_count={}",
            report.diverged_from_current_snapshot_latest_pointer_count
        ),
        format!(
            "diverged_from_latest_state_archive_count={}",
            report.diverged_from_latest_state_archive_count
        ),
        format!(
            "ambiguous_non_green_archived_bundle_count={}",
            report.ambiguous_non_green_archived_bundle_count
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
            "current_state_archive_latest_snapshot_path={}",
            report
                .current_state_archive_latest_snapshot_path
                .as_deref()
                .unwrap_or("<none>")
        ),
        format!("snapshot_latest_pointer_verdict={snapshot_pointer_verdict}"),
        format!("latest_bundle_pointer_verdict={bundle_pointer_verdict}"),
        format!(
            "latest_bundle_pointer_selected_bundle_path={}",
            report
                .latest_bundle_pointer
                .selected_bundle_path
                .as_deref()
                .unwrap_or("<none>")
        ),
        format!(
            "latest_bundle_pointer_selected_bundle_linkage_verdict={}",
            report
                .latest_bundle_pointer
                .selected_bundle_linkage_verdict
                .map(|value| serialize_enum(&value))
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!(
            "representative_bundle_path={}",
            report
                .representative_bundle_path
                .as_deref()
                .unwrap_or("<none>")
        ),
        format!(
            "representative_bundle_linkage_verdict={}",
            report
                .representative_bundle_linkage_verdict
                .map(|value| serialize_enum(&value))
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!("artifact_analysis_only={}", report.artifact_analysis_only),
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
    fn archived_bundle_whose_summarized_generations_still_exist_yields_green_linkage() {
        let fixture = write_complete_fixture("artifact_state_bundle_archive_linkage_complete");
        let state_archive_dir = fixture.root.join("state_archive");
        let bundle_archive_dir = fixture.root.join("bundle_archive");
        let bundle_pointer_dir = fixture.root.join("bundle_pointer");
        let snapshot_pointer_dir = fixture.root.join("snapshot_pointer");
        let snapshot_path = state_archive_dir
            .join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        write_snapshot(
            &snapshot_path,
            "artifact_state_coherent",
            &fixture.generation.generation_id,
            &fixture.generation.generation_id,
            true,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        let bundle_path =
            export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &snapshot_path);
        write_bundle_pointer_metadata(
            &bundle_pointer_dir,
            activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME,
            &bundle_archive_dir,
            &bundle_path,
        );
        write_state_pointer_metadata(
            &snapshot_pointer_dir,
            activation_artifact_state_publish_report::DEFAULT_LATEST_POINTER_NAME,
            &state_archive_dir,
            &snapshot_path,
        );

        let report = build_report(&fixture.config(
            bundle_archive_dir,
            Some(bundle_pointer_dir),
            state_archive_dir,
            Some(snapshot_pointer_dir),
        ))
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleArchiveLinkageVerdict::ArtifactStateBundleArchiveLinkageComplete
        );
        assert_eq!(report.archived_bundle_count_examined, 1);
        assert_eq!(report.bundles_resolving_live_generations_count, 1);
        assert_eq!(
            report.representative_bundle_linkage_verdict,
            Some(ArchivedBundleLinkageVerdict::Complete)
        );
    }

    #[test]
    fn archived_bundle_referencing_missing_review_generation_yields_non_green_linkage() {
        let fixture =
            write_complete_fixture("artifact_state_bundle_archive_linkage_missing_review");
        let state_archive_dir = fixture.root.join("state_archive");
        let bundle_archive_dir = fixture.root.join("bundle_archive");
        let snapshot_path = state_archive_dir
            .join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        write_snapshot(
            &snapshot_path,
            "artifact_state_coherent",
            "missing_review_generation",
            &fixture.generation.generation_id,
            false,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &snapshot_path);

        let report =
            build_report(&fixture.config(bundle_archive_dir, None, state_archive_dir, None))
                .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleArchiveLinkageVerdict::ArtifactStateBundleArchiveLinkageIncomplete
        );
        assert_eq!(report.missing_selected_review_generation_count, 1);
    }

    #[test]
    fn archived_bundle_referencing_missing_release_generation_yields_non_green_linkage() {
        let fixture =
            write_complete_fixture("artifact_state_bundle_archive_linkage_missing_release");
        let state_archive_dir = fixture.root.join("state_archive");
        let bundle_archive_dir = fixture.root.join("bundle_archive");
        let snapshot_path = state_archive_dir
            .join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        write_snapshot(
            &snapshot_path,
            "artifact_state_coherent",
            &fixture.generation.generation_id,
            "missing_release_generation",
            false,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &snapshot_path);

        let report =
            build_report(&fixture.config(bundle_archive_dir, None, state_archive_dir, None))
                .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleArchiveLinkageVerdict::ArtifactStateBundleArchiveLinkageIncomplete
        );
        assert_eq!(report.missing_selected_release_generation_count, 1);
    }

    #[test]
    fn latest_bundle_pointer_resolving_to_stale_broken_bundle_linkage_is_surfaced_explicitly() {
        let fixture =
            write_complete_fixture("artifact_state_bundle_archive_linkage_pointer_broken");
        let state_archive_dir = fixture.root.join("state_archive");
        let bundle_archive_dir = fixture.root.join("bundle_archive");
        let bundle_pointer_dir = fixture.root.join("bundle_pointer");
        let broken_snapshot = state_archive_dir
            .join("state_snapshot__2026-03-26T17-00-00Z__artifact_state_coherent.json");
        let latest_snapshot = state_archive_dir
            .join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        write_snapshot(
            &broken_snapshot,
            "artifact_state_coherent",
            &fixture.generation.generation_id,
            "missing_release_generation",
            false,
            0,
            ts("2026-03-26T17:00:00Z"),
        );
        write_snapshot(
            &latest_snapshot,
            "artifact_state_coherent",
            &fixture.generation.generation_id,
            &fixture.generation.generation_id,
            true,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        let broken_bundle =
            export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &broken_snapshot);
        export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &latest_snapshot);
        write_bundle_pointer_metadata(
            &bundle_pointer_dir,
            activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME,
            &bundle_archive_dir,
            &broken_bundle,
        );

        let report = build_report(&fixture.config(
            bundle_archive_dir,
            Some(bundle_pointer_dir),
            state_archive_dir,
            None,
        ))
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleArchiveLinkageVerdict::ArtifactStateBundleArchiveLinkageIncomplete
        );
        assert_eq!(
            report.latest_bundle_pointer.selected_bundle_linkage_verdict,
            Some(ArchivedBundleLinkageVerdict::Incomplete)
        );
        assert!(report.reason.contains("selected latest release generation"));
    }

    #[test]
    fn ambiguous_archived_bundle_is_not_treated_as_clean_green_linkage() {
        let fixture = write_complete_fixture("artifact_state_bundle_archive_linkage_ambiguous");
        let state_archive_dir = fixture.root.join("state_archive");
        let bundle_archive_dir = fixture.root.join("bundle_archive");
        let bundle_pointer_dir = fixture.root.join("bundle_pointer");
        let snapshot_path = state_archive_dir.join(
            "state_snapshot__2026-03-26T18-00-00Z__artifact_state_ambiguous_legacy_state.json",
        );
        write_snapshot(
            &snapshot_path,
            "artifact_state_ambiguous_legacy_state",
            &fixture.generation.generation_id,
            &fixture.generation.generation_id,
            true,
            2,
            ts("2026-03-26T18:00:00Z"),
        );
        let bundle_path =
            export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &snapshot_path);
        write_bundle_pointer_metadata(
            &bundle_pointer_dir,
            activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME,
            &bundle_archive_dir,
            &bundle_path,
        );

        let report = build_report(&fixture.config(
            bundle_archive_dir,
            Some(bundle_pointer_dir),
            state_archive_dir,
            None,
        ))
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleArchiveLinkageVerdict::ArtifactStateBundleArchiveLinkageAmbiguousLegacyState
        );
        assert_eq!(report.ambiguous_non_green_archived_bundle_count, 1);
    }

    #[test]
    fn malformed_archived_bundle_metadata_is_reported_as_invalid() {
        let fixture = write_complete_fixture("artifact_state_bundle_archive_linkage_invalid");
        let state_archive_dir = fixture.root.join("state_archive");
        let bundle_archive_dir = fixture.root.join("bundle_archive");
        let snapshot_path = state_archive_dir
            .join("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json");
        write_snapshot(
            &snapshot_path,
            "artifact_state_coherent",
            &fixture.generation.generation_id,
            &fixture.generation.generation_id,
            true,
            0,
            ts("2026-03-26T18:00:00Z"),
        );
        export_archived_bundle(&bundle_archive_dir, &state_archive_dir, &snapshot_path);
        let invalid_bundle_dir = bundle_archive_dir.join("state_snapshot_bundle__broken");
        fs::create_dir_all(&invalid_bundle_dir).expect("invalid bundle dir");
        fs::write(
            invalid_bundle_dir.join(activation_artifact_state_bundle::BUNDLE_MANIFEST_FILENAME),
            "{broken",
        )
        .expect("write invalid manifest");

        let report =
            build_report(&fixture.config(bundle_archive_dir, None, state_archive_dir, None))
                .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleArchiveLinkageVerdict::ArtifactStateBundleArchiveLinkageInvalidArtifactsPresent
        );
        assert_eq!(report.invalid_artifact_count, 1);
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
            bundle_archive_dir: PathBuf,
            bundle_latest_pointer_dir: Option<PathBuf>,
            state_archive_dir: PathBuf,
            snapshot_latest_pointer_dir: Option<PathBuf>,
        ) -> Config {
            Config {
                bundle_archive_dir,
                bundle_latest_pointer_dir,
                bundle_pointer_name:
                    activation_artifact_state_bundle_publish_report::DEFAULT_BUNDLE_POINTER_NAME
                        .to_string(),
                state_archive_dir,
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
                activation_artifact_release_publish_report::DEFAULT_LATEST_POINTER_NAME.to_string(),
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
            .expect("bundle manifest json"),
        )
        .expect("write bundle manifest");
    }

    fn sample_packet_json(
        generated_at: &str,
        prod_fp: &str,
        non_prod_fp: &str,
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
                "sha256": prod_fp,
                "secrets_excluded": true,
                "sensitive_urls_redacted_before_hashing": true
            },
            "non_prod_config_fingerprint": {
                "scope": "non_prod_execution_policy_guardrails",
                "sha256": non_prod_fp,
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
        packet_generated_at: &str,
        prod_fp: &str,
        non_prod_fp: &str,
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
            "decision_packet_generated_at": packet_generated_at,
            "decision_packet_verdict": "decision_packet_discussion_ready_but_not_authorized",
            "decision_packet_reason": "sample packet",
            "build_version": "0.1.0",
            "git_commit": "deadbeef",
            "prod_config_fingerprint": {
                "scope": "prod_execution_policy_guardrails",
                "sha256": prod_fp,
                "secrets_excluded": true,
                "sensitive_urls_redacted_before_hashing": true
            },
            "non_prod_config_fingerprint": {
                "scope": "non_prod_execution_policy_guardrails",
                "sha256": non_prod_fp,
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

    fn generation_dir_name(identity: &GenerationIdentity) -> String {
        format!(
            "{}__{}__{}",
            identity.generated_at.to_rfc3339().replace(':', "-"),
            identity.prod_fp,
            identity.non_prod_fp
        )
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
