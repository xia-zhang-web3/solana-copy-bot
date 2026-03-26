#![recursion_limit = "256"]
#![allow(unused_attributes)]

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: copybot_activation_artifact_release_history (--history-dir <path> [--release <path>]... [--latest] [--limit <count>] [--recent-horizon-seconds <seconds>] | --compare <older> <newer>) [--json]";
const DEFAULT_HISTORY_LIMIT: usize = 10;
const DEFAULT_RECENT_HORIZON_SECONDS: u64 = 7 * 24 * 60 * 60;
const DEFAULT_MIN_RELEASES_FOR_CONFIDENCE: usize = 2;

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
    release_paths: Vec<PathBuf>,
    compare: Option<(PathBuf, PathBuf)>,
    latest: bool,
    limit: usize,
    recent_horizon_seconds: u64,
    json: bool,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ArtifactReleaseHistoryVerdict {
    ArtifactReleaseHistoryLatestPublished,
    ArtifactReleaseHistoryLatestPublishedAndPromoted,
    ArtifactReleaseHistoryLatestBlocked,
    ArtifactReleaseHistoryInsufficientArtifacts,
    ArtifactReleaseHistoryCompareReady,
    ArtifactReleaseHistoryInvalidArtifact,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ReleaseChangeTrend {
    Narrower,
    Wider,
    Unchanged,
    Unknown,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ChannelPromotionProgression {
    NoPromotionAttempts,
    PromotedRecently,
    RepeatedlyBlocked,
    MixedHistory,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReleaseTimestampSource {
    ReleasedAt,
    GenerationIdentity,
    GenerationDirectory,
    MissingDeterministicTimestamp,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ArtifactReleaseVerdict {
    ArtifactReleasePublished,
    ArtifactReleasePublishedAndPromoted,
    ArtifactReleasePublishFailed,
    ArtifactReleaseChannelPromoteBlocked,
    ArtifactReleaseFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ArtifactReleaseArtifact {
    pub(crate) mode: String,
    #[serde(default)]
    pub(crate) released_at: Option<DateTime<Utc>>,
    pub(crate) verdict: ArtifactReleaseVerdict,
    pub(crate) reason: String,
    pub(crate) config_path: String,
    pub(crate) non_prod_config_path: String,
    pub(crate) archive_dir: String,
    pub(crate) generation_id: Option<String>,
    pub(crate) generation_directory: Option<String>,
    pub(crate) packet_path: Option<String>,
    pub(crate) runbook_json_path: Option<String>,
    pub(crate) runbook_markdown_path: Option<String>,
    pub(crate) manifest_path: Option<String>,
    pub(crate) bundle_path: Option<String>,
    pub(crate) decision_packet_verdict: Option<String>,
    pub(crate) checklist_verdict: Option<String>,
    pub(crate) publish_verdict: Option<String>,
    pub(crate) publish_reason: Option<String>,
    pub(crate) generation_published: bool,
    pub(crate) manifest_generated: bool,
    pub(crate) bundle_exported: bool,
    pub(crate) channel_promotion_attempted: bool,
    pub(crate) channel_promotion_happened: bool,
    pub(crate) channel_verify_attempted: bool,
    pub(crate) channel_name: Option<String>,
    pub(crate) channel_dir: Option<String>,
    pub(crate) channel_metadata_path: Option<String>,
    pub(crate) channel_promote_verdict: Option<String>,
    pub(crate) channel_promote_reason: Option<String>,
    pub(crate) channel_verify_verdict: Option<String>,
    pub(crate) channel_verify_reason: Option<String>,
    pub(crate) execution_untouched: bool,
    pub(crate) activation_authorized: bool,
    pub(crate) not_authorized_summary: String,
}

#[derive(Debug, Clone, Serialize)]
struct InvalidArtifact {
    path: String,
    error: String,
}

#[derive(Debug, Clone)]
pub(crate) struct LoadedRelease {
    pub(crate) path: PathBuf,
    pub(crate) artifact: ArtifactReleaseArtifact,
    pub(crate) effective_released_at: Option<DateTime<Utc>>,
    pub(crate) released_at_source: ReleaseTimestampSource,
}

#[derive(Debug, Clone, Serialize)]
struct ReleaseSnapshotSummary {
    path: String,
    released_at: Option<DateTime<Utc>>,
    released_at_source: ReleaseTimestampSource,
    verdict: String,
    reason: String,
    generation_id: Option<String>,
    generation_published: bool,
    channel_promotion_attempted: bool,
    channel_promotion_happened: bool,
    channel_name: Option<String>,
    channel_promote_verdict: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ArtifactReleaseHistorySummaryReport {
    mode: String,
    verdict: ArtifactReleaseHistoryVerdict,
    reason: String,
    history_dir: Option<String>,
    release_paths_examined: Vec<String>,
    latest_only_requested: bool,
    limit: usize,
    recent_horizon_seconds: u64,
    minimum_releases_for_confidence: usize,
    releases_loaded: usize,
    invalid_artifact_count: usize,
    invalid_artifacts: Vec<InvalidArtifact>,
    compat_loaded_without_released_at_count: usize,
    deterministic_timestamp_missing_count: usize,
    published_only_count: usize,
    published_and_promoted_count: usize,
    blocked_count: usize,
    channel_promote_blocked_count: usize,
    latest_release: Option<ReleaseSnapshotSummary>,
    latest_release_age_seconds: Option<u64>,
    latest_release_stale_for_operational_confidence: bool,
    history_sparse_for_operational_confidence: bool,
    latest_promoted_generation_id: Option<String>,
    latest_promoted_channel_name: Option<String>,
    latest_promoted_channel_metadata_path: Option<String>,
    promoted_generation_changed_over_history: bool,
    channel_promotion_progression: ChannelPromotionProgression,
    release_change_trend_since_oldest: ReleaseChangeTrend,
    recent_release_progression: Vec<ReleaseSnapshotSummary>,
    artifact_analysis_only: bool,
    activation_authorized: bool,
    not_authorized_summary: String,
}

#[derive(Debug, Clone, Serialize)]
struct ArtifactReleaseHistoryDiffReport {
    mode: String,
    verdict: ArtifactReleaseHistoryVerdict,
    reason: String,
    older_path: String,
    newer_path: String,
    invalid_artifacts: Vec<InvalidArtifact>,
    older_released_at: Option<DateTime<Utc>>,
    newer_released_at: Option<DateTime<Utc>>,
    older_released_at_source: Option<ReleaseTimestampSource>,
    newer_released_at_source: Option<ReleaseTimestampSource>,
    older_verdict: Option<String>,
    newer_verdict: Option<String>,
    older_reason: Option<String>,
    newer_reason: Option<String>,
    blocker_trend: ReleaseChangeTrend,
    generation_id_changed: bool,
    older_generation_id: Option<String>,
    newer_generation_id: Option<String>,
    packet_path_changed: bool,
    older_packet_path: Option<String>,
    newer_packet_path: Option<String>,
    runbook_json_path_changed: bool,
    older_runbook_json_path: Option<String>,
    newer_runbook_json_path: Option<String>,
    runbook_markdown_path_changed: bool,
    older_runbook_markdown_path: Option<String>,
    newer_runbook_markdown_path: Option<String>,
    manifest_path_changed: bool,
    older_manifest_path: Option<String>,
    newer_manifest_path: Option<String>,
    bundle_path_changed: bool,
    older_bundle_path: Option<String>,
    newer_bundle_path: Option<String>,
    channel_promotion_attempted_changed: bool,
    older_channel_promotion_attempted: Option<bool>,
    newer_channel_promotion_attempted: Option<bool>,
    channel_promotion_happened_changed: bool,
    older_channel_promotion_happened: Option<bool>,
    newer_channel_promotion_happened: Option<bool>,
    channel_target_changed: bool,
    older_channel_name: Option<String>,
    newer_channel_name: Option<String>,
    older_channel_metadata_path: Option<String>,
    newer_channel_metadata_path: Option<String>,
    publish_reason_changed: bool,
    older_publish_reason: Option<String>,
    newer_publish_reason: Option<String>,
    channel_promote_reason_changed: bool,
    older_channel_promote_reason: Option<String>,
    newer_channel_promote_reason: Option<String>,
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
    let mut release_paths = Vec::new();
    let mut compare: Option<(PathBuf, PathBuf)> = None;
    let mut latest = false;
    let mut limit = DEFAULT_HISTORY_LIMIT;
    let mut recent_horizon_seconds = DEFAULT_RECENT_HORIZON_SECONDS;
    let mut json = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--history-dir" => {
                history_dir = Some(PathBuf::from(parse_string_arg(
                    "--history-dir",
                    args.next(),
                )?))
            }
            "--release" => {
                release_paths.push(PathBuf::from(parse_string_arg("--release", args.next())?))
            }
            "--compare" => {
                let older = PathBuf::from(parse_string_arg("--compare", args.next())?);
                let newer = PathBuf::from(parse_string_arg("--compare", args.next())?);
                compare = Some((older, newer));
            }
            "--latest" => latest = true,
            "--limit" => limit = parse_usize_arg("--limit", args.next())?,
            "--recent-horizon-seconds" => {
                recent_horizon_seconds = parse_u64_arg("--recent-horizon-seconds", args.next())?
            }
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    if compare.is_some() && (history_dir.is_some() || !release_paths.is_empty() || latest) {
        bail!("--compare cannot be combined with --history-dir, --release, or --latest");
    }
    if compare.is_none() && history_dir.is_none() && release_paths.is_empty() {
        bail!("either --history-dir/--release or --compare is required");
    }

    Ok(Some(Config {
        history_dir,
        release_paths,
        compare,
        latest,
        limit: limit.max(1),
        recent_horizon_seconds: recent_horizon_seconds.max(1),
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

fn parse_usize_arg(flag: &str, value: Option<String>) -> Result<usize> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<usize>()
        .with_context(|| format!("invalid {flag} usize value: {raw}"))
}

fn parse_u64_arg(flag: &str, value: Option<String>) -> Result<u64> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<u64>()
        .with_context(|| format!("invalid {flag} u64 value: {raw}"))
}

fn run(config: Config) -> Result<String> {
    if let Some((older, newer)) = &config.compare {
        let report = build_compare_report(older, newer)?;
        if config.json {
            serde_json::to_string_pretty(&report)
                .context("failed serializing artifact release history compare report json")
        } else {
            Ok(render_compare_human(&report))
        }
    } else {
        let report = build_history_report(&config)?;
        if config.json {
            serde_json::to_string_pretty(&report)
                .context("failed serializing artifact release history report json")
        } else {
            Ok(render_history_human(&report))
        }
    }
}

fn build_history_report(config: &Config) -> Result<ArtifactReleaseHistorySummaryReport> {
    let release_paths = collect_history_paths(config)?;
    let (mut valid_releases, invalid_artifacts) = load_artifacts(&release_paths);
    valid_releases.sort_by(|left, right| {
        left.effective_released_at
            .cmp(&right.effective_released_at)
            .then_with(|| {
                left.artifact
                    .generation_id
                    .cmp(&right.artifact.generation_id)
            })
            .then_with(|| left.path.cmp(&right.path))
    });

    let latest_release = valid_releases.last().cloned();
    let compat_loaded_without_released_at_count = valid_releases
        .iter()
        .filter(|record| {
            record.released_at_source != ReleaseTimestampSource::ReleasedAt
                && record.effective_released_at.is_some()
        })
        .count();
    let deterministic_timestamp_missing_count = valid_releases
        .iter()
        .filter(|record| record.effective_released_at.is_none())
        .count();
    let published_only_count = valid_releases
        .iter()
        .filter(|record| {
            record.artifact.verdict == ArtifactReleaseVerdict::ArtifactReleasePublished
        })
        .count();
    let published_and_promoted_count = valid_releases
        .iter()
        .filter(|record| {
            record.artifact.verdict == ArtifactReleaseVerdict::ArtifactReleasePublishedAndPromoted
        })
        .count();
    let channel_promote_blocked_count = valid_releases
        .iter()
        .filter(|record| {
            record.artifact.verdict == ArtifactReleaseVerdict::ArtifactReleaseChannelPromoteBlocked
        })
        .count();
    let blocked_count = valid_releases
        .iter()
        .filter(|record| {
            matches!(
                record.artifact.verdict,
                ArtifactReleaseVerdict::ArtifactReleasePublishFailed
                    | ArtifactReleaseVerdict::ArtifactReleaseChannelPromoteBlocked
                    | ArtifactReleaseVerdict::ArtifactReleaseFailed
            )
        })
        .count();

    let latest_release_age_seconds = latest_release.as_ref().and_then(|record| {
        record
            .effective_released_at
            .map(|ts| age_seconds(ts, Utc::now()))
    });
    let latest_release_stale_for_operational_confidence =
        latest_release_age_seconds.is_some_and(|age| age > config.recent_horizon_seconds);
    let history_sparse_for_operational_confidence =
        valid_releases.len() < DEFAULT_MIN_RELEASES_FOR_CONFIDENCE;

    let latest_promoted = valid_releases
        .iter()
        .rev()
        .find(|record| record.artifact.channel_promotion_happened);
    let promoted_generation_changed_over_history =
        unique_count(valid_releases.iter().filter_map(|record| {
            if record.artifact.channel_promotion_happened {
                record.artifact.generation_id.as_deref()
            } else {
                None
            }
        })) > 1;
    let channel_promotion_progression = summarize_channel_promotion_progression(&valid_releases);
    let release_change_trend_since_oldest = release_change_trend(
        valid_releases
            .first()
            .map(|record| &record.artifact.verdict),
        valid_releases.last().map(|record| &record.artifact.verdict),
    );

    let recent_release_progression = if valid_releases.is_empty() {
        Vec::new()
    } else if config.latest {
        vec![release_snapshot_summary(
            valid_releases.last().expect("latest release"),
        )]
    } else {
        let start = valid_releases.len().saturating_sub(config.limit);
        valid_releases[start..]
            .iter()
            .map(release_snapshot_summary)
            .collect()
    };

    let (verdict, reason) = if !invalid_artifacts.is_empty() {
        (
            ArtifactReleaseHistoryVerdict::ArtifactReleaseHistoryInvalidArtifact,
            format!(
                "history contains {} invalid artifact(s); review invalid_artifacts before trusting release progression",
                invalid_artifacts.len()
            ),
        )
    } else if latest_release.is_none() {
        (
            ArtifactReleaseHistoryVerdict::ArtifactReleaseHistoryInsufficientArtifacts,
            "no valid activation artifact release reports were found".to_string(),
        )
    } else {
        let latest = latest_release.as_ref().expect("latest release");
        match latest.artifact.verdict {
            ArtifactReleaseVerdict::ArtifactReleasePublished => (
                ArtifactReleaseHistoryVerdict::ArtifactReleaseHistoryLatestPublished,
                "latest artifact release published a review generation without channel promotion"
                    .to_string(),
            ),
            ArtifactReleaseVerdict::ArtifactReleasePublishedAndPromoted => (
                ArtifactReleaseHistoryVerdict::ArtifactReleaseHistoryLatestPublishedAndPromoted,
                "latest artifact release published and promoted the current review generation"
                    .to_string(),
            ),
            ArtifactReleaseVerdict::ArtifactReleasePublishFailed
            | ArtifactReleaseVerdict::ArtifactReleaseChannelPromoteBlocked
            | ArtifactReleaseVerdict::ArtifactReleaseFailed => (
                ArtifactReleaseHistoryVerdict::ArtifactReleaseHistoryLatestBlocked,
                format!(
                    "latest artifact release remains blocked or incomplete: {}",
                    latest.artifact.reason
                ),
            ),
        }
    };

    Ok(ArtifactReleaseHistorySummaryReport {
        mode: "history".to_string(),
        verdict,
        reason,
        history_dir: config
            .history_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        release_paths_examined: release_paths
            .iter()
            .map(|path| path.display().to_string())
            .collect(),
        latest_only_requested: config.latest,
        limit: config.limit,
        recent_horizon_seconds: config.recent_horizon_seconds,
        minimum_releases_for_confidence: DEFAULT_MIN_RELEASES_FOR_CONFIDENCE,
        releases_loaded: valid_releases.len(),
        invalid_artifact_count: invalid_artifacts.len(),
        invalid_artifacts,
        compat_loaded_without_released_at_count,
        deterministic_timestamp_missing_count,
        published_only_count,
        published_and_promoted_count,
        blocked_count,
        channel_promote_blocked_count,
        latest_release: latest_release.as_ref().map(release_snapshot_summary),
        latest_release_age_seconds,
        latest_release_stale_for_operational_confidence,
        history_sparse_for_operational_confidence,
        latest_promoted_generation_id: latest_promoted
            .and_then(|record| record.artifact.generation_id.clone()),
        latest_promoted_channel_name: latest_promoted
            .and_then(|record| record.artifact.channel_name.clone()),
        latest_promoted_channel_metadata_path: latest_promoted
            .and_then(|record| record.artifact.channel_metadata_path.clone()),
        promoted_generation_changed_over_history,
        channel_promotion_progression,
        release_change_trend_since_oldest,
        recent_release_progression,
        artifact_analysis_only: true,
        activation_authorized: false,
        not_authorized_summary:
            "Artifact/release analysis only. This history report does not authorize activation and does not override the Stage 3 production gate.".to_string(),
    })
}

fn build_compare_report(older: &Path, newer: &Path) -> Result<ArtifactReleaseHistoryDiffReport> {
    let older_result = inspect_release_artifact(older);
    let newer_result = inspect_release_artifact(newer);
    let mut invalid_artifacts = Vec::new();
    let older_loaded = match older_result {
        Ok(artifact) => Some(artifact),
        Err(error) => {
            invalid_artifacts.push(InvalidArtifact {
                path: older.display().to_string(),
                error: format!("{error:#}"),
            });
            None
        }
    };
    let newer_loaded = match newer_result {
        Ok(artifact) => Some(artifact),
        Err(error) => {
            invalid_artifacts.push(InvalidArtifact {
                path: newer.display().to_string(),
                error: format!("{error:#}"),
            });
            None
        }
    };

    if !invalid_artifacts.is_empty() {
        return Ok(ArtifactReleaseHistoryDiffReport {
            mode: "compare".to_string(),
            verdict: ArtifactReleaseHistoryVerdict::ArtifactReleaseHistoryInvalidArtifact,
            reason: "one or more compared artifacts are invalid activation artifact release reports"
                .to_string(),
            older_path: older.display().to_string(),
            newer_path: newer.display().to_string(),
            invalid_artifacts,
            older_released_at: None,
            newer_released_at: None,
            older_released_at_source: None,
            newer_released_at_source: None,
            older_verdict: None,
            newer_verdict: None,
            older_reason: None,
            newer_reason: None,
            blocker_trend: ReleaseChangeTrend::Unknown,
            generation_id_changed: false,
            older_generation_id: None,
            newer_generation_id: None,
            packet_path_changed: false,
            older_packet_path: None,
            newer_packet_path: None,
            runbook_json_path_changed: false,
            older_runbook_json_path: None,
            newer_runbook_json_path: None,
            runbook_markdown_path_changed: false,
            older_runbook_markdown_path: None,
            newer_runbook_markdown_path: None,
            manifest_path_changed: false,
            older_manifest_path: None,
            newer_manifest_path: None,
            bundle_path_changed: false,
            older_bundle_path: None,
            newer_bundle_path: None,
            channel_promotion_attempted_changed: false,
            older_channel_promotion_attempted: None,
            newer_channel_promotion_attempted: None,
            channel_promotion_happened_changed: false,
            older_channel_promotion_happened: None,
            newer_channel_promotion_happened: None,
            channel_target_changed: false,
            older_channel_name: None,
            newer_channel_name: None,
            older_channel_metadata_path: None,
            newer_channel_metadata_path: None,
            publish_reason_changed: false,
            older_publish_reason: None,
            newer_publish_reason: None,
            channel_promote_reason_changed: false,
            older_channel_promote_reason: None,
            newer_channel_promote_reason: None,
            artifact_analysis_only: true,
            activation_authorized: false,
            not_authorized_summary:
                "Artifact/release analysis only. This diff report does not authorize activation and does not override the Stage 3 production gate.".to_string(),
        });
    }

    let older_loaded = older_loaded.expect("older loaded");
    let newer_loaded = newer_loaded.expect("newer loaded");

    Ok(ArtifactReleaseHistoryDiffReport {
        mode: "compare".to_string(),
        verdict: ArtifactReleaseHistoryVerdict::ArtifactReleaseHistoryCompareReady,
        reason: "artifact release diff is ready for operator review".to_string(),
        older_path: older_loaded.path.display().to_string(),
        newer_path: newer_loaded.path.display().to_string(),
        invalid_artifacts,
        older_released_at: older_loaded.effective_released_at,
        newer_released_at: newer_loaded.effective_released_at,
        older_released_at_source: Some(older_loaded.released_at_source),
        newer_released_at_source: Some(newer_loaded.released_at_source),
        older_verdict: Some(serialize_enum(&older_loaded.artifact.verdict)),
        newer_verdict: Some(serialize_enum(&newer_loaded.artifact.verdict)),
        older_reason: Some(older_loaded.artifact.reason.clone()),
        newer_reason: Some(newer_loaded.artifact.reason.clone()),
        blocker_trend: release_change_trend(
            Some(&older_loaded.artifact.verdict),
            Some(&newer_loaded.artifact.verdict),
        ),
        generation_id_changed: older_loaded.artifact.generation_id != newer_loaded.artifact.generation_id,
        older_generation_id: older_loaded.artifact.generation_id.clone(),
        newer_generation_id: newer_loaded.artifact.generation_id.clone(),
        packet_path_changed: older_loaded.artifact.packet_path != newer_loaded.artifact.packet_path,
        older_packet_path: older_loaded.artifact.packet_path.clone(),
        newer_packet_path: newer_loaded.artifact.packet_path.clone(),
        runbook_json_path_changed: older_loaded.artifact.runbook_json_path
            != newer_loaded.artifact.runbook_json_path,
        older_runbook_json_path: older_loaded.artifact.runbook_json_path.clone(),
        newer_runbook_json_path: newer_loaded.artifact.runbook_json_path.clone(),
        runbook_markdown_path_changed: older_loaded.artifact.runbook_markdown_path
            != newer_loaded.artifact.runbook_markdown_path,
        older_runbook_markdown_path: older_loaded.artifact.runbook_markdown_path.clone(),
        newer_runbook_markdown_path: newer_loaded.artifact.runbook_markdown_path.clone(),
        manifest_path_changed: older_loaded.artifact.manifest_path != newer_loaded.artifact.manifest_path,
        older_manifest_path: older_loaded.artifact.manifest_path.clone(),
        newer_manifest_path: newer_loaded.artifact.manifest_path.clone(),
        bundle_path_changed: older_loaded.artifact.bundle_path != newer_loaded.artifact.bundle_path,
        older_bundle_path: older_loaded.artifact.bundle_path.clone(),
        newer_bundle_path: newer_loaded.artifact.bundle_path.clone(),
        channel_promotion_attempted_changed: older_loaded.artifact.channel_promotion_attempted
            != newer_loaded.artifact.channel_promotion_attempted,
        older_channel_promotion_attempted: Some(older_loaded.artifact.channel_promotion_attempted),
        newer_channel_promotion_attempted: Some(newer_loaded.artifact.channel_promotion_attempted),
        channel_promotion_happened_changed: older_loaded.artifact.channel_promotion_happened
            != newer_loaded.artifact.channel_promotion_happened,
        older_channel_promotion_happened: Some(older_loaded.artifact.channel_promotion_happened),
        newer_channel_promotion_happened: Some(newer_loaded.artifact.channel_promotion_happened),
        channel_target_changed: older_loaded.artifact.channel_promotion_happened
            != newer_loaded.artifact.channel_promotion_happened
            || older_loaded.artifact.channel_name != newer_loaded.artifact.channel_name
            || older_loaded.artifact.channel_metadata_path
                != newer_loaded.artifact.channel_metadata_path
            || older_loaded.artifact.generation_id != newer_loaded.artifact.generation_id,
        older_channel_name: older_loaded.artifact.channel_name.clone(),
        newer_channel_name: newer_loaded.artifact.channel_name.clone(),
        older_channel_metadata_path: older_loaded.artifact.channel_metadata_path.clone(),
        newer_channel_metadata_path: newer_loaded.artifact.channel_metadata_path.clone(),
        publish_reason_changed: older_loaded.artifact.publish_reason != newer_loaded.artifact.publish_reason,
        older_publish_reason: older_loaded.artifact.publish_reason.clone(),
        newer_publish_reason: newer_loaded.artifact.publish_reason.clone(),
        channel_promote_reason_changed: older_loaded.artifact.channel_promote_reason
            != newer_loaded.artifact.channel_promote_reason,
        older_channel_promote_reason: older_loaded.artifact.channel_promote_reason.clone(),
        newer_channel_promote_reason: newer_loaded.artifact.channel_promote_reason.clone(),
        artifact_analysis_only: true,
        activation_authorized: false,
        not_authorized_summary:
            "Artifact/release analysis only. This diff report does not authorize activation and does not override the Stage 3 production gate.".to_string(),
    })
}

fn collect_history_paths(config: &Config) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    if let Some(dir) = &config.history_dir {
        let entries = fs::read_dir(dir)
            .with_context(|| format!("failed reading history dir {}", dir.display()))?;
        for entry in entries {
            let entry = entry.with_context(|| {
                format!("failed reading entry in history dir {}", dir.display())
            })?;
            let path = entry.path();
            if path.is_file() && path.extension().is_some_and(|ext| ext == "json") {
                paths.push(path);
            }
        }
    }
    paths.extend(config.release_paths.iter().cloned());
    paths.sort();
    paths.dedup();
    Ok(paths)
}

fn load_artifacts(paths: &[PathBuf]) -> (Vec<LoadedRelease>, Vec<InvalidArtifact>) {
    let mut valid = Vec::new();
    let mut invalid = Vec::new();
    for path in paths {
        match inspect_release_artifact(path) {
            Ok(artifact) => valid.push(artifact),
            Err(error) => invalid.push(InvalidArtifact {
                path: path.display().to_string(),
                error: format!("{error:#}"),
            }),
        }
    }
    (valid, invalid)
}

pub(crate) fn inspect_release_artifact(path: &Path) -> Result<LoadedRelease> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed reading release artifact {}", path.display()))?;
    let artifact: ArtifactReleaseArtifact = serde_json::from_str(&raw)
        .with_context(|| format!("failed parsing release artifact json {}", path.display()))?;
    if artifact.mode != "artifact_release" {
        bail!(
            "artifact {} has mode `{}` instead of `artifact_release`",
            path.display(),
            artifact.mode
        );
    }
    let (effective_released_at, released_at_source) = resolve_effective_released_at(&artifact);
    Ok(LoadedRelease {
        path: path.to_path_buf(),
        artifact,
        effective_released_at,
        released_at_source,
    })
}

fn release_snapshot_summary(record: &LoadedRelease) -> ReleaseSnapshotSummary {
    ReleaseSnapshotSummary {
        path: record.path.display().to_string(),
        released_at: record.effective_released_at,
        released_at_source: record.released_at_source,
        verdict: serialize_enum(&record.artifact.verdict),
        reason: record.artifact.reason.clone(),
        generation_id: record.artifact.generation_id.clone(),
        generation_published: record.artifact.generation_published,
        channel_promotion_attempted: record.artifact.channel_promotion_attempted,
        channel_promotion_happened: record.artifact.channel_promotion_happened,
        channel_name: record.artifact.channel_name.clone(),
        channel_promote_verdict: record.artifact.channel_promote_verdict.clone(),
    }
}

fn resolve_effective_released_at(
    artifact: &ArtifactReleaseArtifact,
) -> (Option<DateTime<Utc>>, ReleaseTimestampSource) {
    if let Some(released_at) = artifact.released_at {
        return (Some(released_at), ReleaseTimestampSource::ReleasedAt);
    }
    if let Some(released_at) = artifact
        .generation_id
        .as_deref()
        .and_then(parse_released_at_from_generation_id)
    {
        return (
            Some(released_at),
            ReleaseTimestampSource::GenerationIdentity,
        );
    }
    if let Some(released_at) = artifact
        .generation_directory
        .as_deref()
        .and_then(parse_released_at_from_generation_directory)
    {
        return (
            Some(released_at),
            ReleaseTimestampSource::GenerationDirectory,
        );
    }
    (None, ReleaseTimestampSource::MissingDeterministicTimestamp)
}

fn parse_released_at_from_generation_id(generation_id: &str) -> Option<DateTime<Utc>> {
    let candidate = generation_id.split('|').next()?.trim();
    DateTime::parse_from_rfc3339(candidate)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn parse_released_at_from_generation_directory(
    generation_directory: &str,
) -> Option<DateTime<Utc>> {
    let basename = Path::new(generation_directory)
        .file_name()?
        .to_string_lossy()
        .into_owned();
    let prefix = basename.split("__").next()?.trim();
    if prefix.len() < 20 || !prefix.contains('T') {
        return None;
    }
    let date = prefix.get(0..10)?;
    let time_with_zone = prefix.get(11..)?;
    let normalized = format!("{}T{}", date, time_with_zone.replace('-', ":"));
    DateTime::parse_from_rfc3339(&normalized)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn unique_count<'a>(values: impl IntoIterator<Item = &'a str>) -> usize {
    values.into_iter().collect::<BTreeSet<_>>().len()
}

fn age_seconds(from: DateTime<Utc>, to: DateTime<Utc>) -> u64 {
    if to < from {
        0
    } else {
        (to - from).num_seconds() as u64
    }
}

fn summarize_channel_promotion_progression(
    releases: &[LoadedRelease],
) -> ChannelPromotionProgression {
    let attempts = releases
        .iter()
        .filter(|record| record.artifact.channel_promotion_attempted)
        .collect::<Vec<_>>();
    if attempts.is_empty() {
        return ChannelPromotionProgression::NoPromotionAttempts;
    }
    if attempts
        .last()
        .is_some_and(|record| record.artifact.channel_promotion_happened)
    {
        return ChannelPromotionProgression::PromotedRecently;
    }
    let trailing_blocked = attempts
        .iter()
        .rev()
        .take(2)
        .all(|record| !record.artifact.channel_promotion_happened);
    if attempts.len() >= 2 && trailing_blocked {
        ChannelPromotionProgression::RepeatedlyBlocked
    } else {
        ChannelPromotionProgression::MixedHistory
    }
}

fn release_change_trend(
    older: Option<&ArtifactReleaseVerdict>,
    newer: Option<&ArtifactReleaseVerdict>,
) -> ReleaseChangeTrend {
    match (older, newer) {
        (Some(older), Some(newer)) => match (release_rank(*older), release_rank(*newer)) {
            (left, right) if right > left => ReleaseChangeTrend::Narrower,
            (left, right) if right < left => ReleaseChangeTrend::Wider,
            _ => ReleaseChangeTrend::Unchanged,
        },
        _ => ReleaseChangeTrend::Unknown,
    }
}

fn release_rank(verdict: ArtifactReleaseVerdict) -> i32 {
    match verdict {
        ArtifactReleaseVerdict::ArtifactReleasePublishedAndPromoted => 3,
        ArtifactReleaseVerdict::ArtifactReleasePublished => 2,
        ArtifactReleaseVerdict::ArtifactReleaseChannelPromoteBlocked => 1,
        ArtifactReleaseVerdict::ArtifactReleasePublishFailed
        | ArtifactReleaseVerdict::ArtifactReleaseFailed => 0,
    }
}

fn render_history_human(report: &ArtifactReleaseHistorySummaryReport) -> String {
    [
        "event=copybot_activation_artifact_release_history".to_string(),
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!(
            "history_dir={}",
            report
                .history_dir
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!("releases_loaded={}", report.releases_loaded),
        format!("invalid_artifact_count={}", report.invalid_artifact_count),
        format!(
            "compat_loaded_without_released_at_count={}",
            report.compat_loaded_without_released_at_count
        ),
        format!(
            "deterministic_timestamp_missing_count={}",
            report.deterministic_timestamp_missing_count
        ),
        format!("published_only_count={}", report.published_only_count),
        format!(
            "published_and_promoted_count={}",
            report.published_and_promoted_count
        ),
        format!("blocked_count={}", report.blocked_count),
        format!(
            "channel_promote_blocked_count={}",
            report.channel_promote_blocked_count
        ),
        format!(
            "latest_release_age_seconds={}",
            report
                .latest_release_age_seconds
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "latest_release_stale_for_operational_confidence={}",
            report.latest_release_stale_for_operational_confidence
        ),
        format!(
            "history_sparse_for_operational_confidence={}",
            report.history_sparse_for_operational_confidence
        ),
        format!(
            "latest_promoted_generation_id={}",
            report
                .latest_promoted_generation_id
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "latest_promoted_channel_name={}",
            report
                .latest_promoted_channel_name
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "latest_promoted_channel_metadata_path={}",
            report
                .latest_promoted_channel_metadata_path
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "promoted_generation_changed_over_history={}",
            report.promoted_generation_changed_over_history
        ),
        format!(
            "channel_promotion_progression={}",
            serialize_enum(&report.channel_promotion_progression)
        ),
        format!(
            "release_change_trend_since_oldest={}",
            serialize_enum(&report.release_change_trend_since_oldest)
        ),
        format!("artifact_analysis_only={}", report.artifact_analysis_only),
        format!("activation_authorized={}", report.activation_authorized),
        format!("not_authorized_summary={}", report.not_authorized_summary),
    ]
    .join("\n")
}

fn render_compare_human(report: &ArtifactReleaseHistoryDiffReport) -> String {
    [
        "event=copybot_activation_artifact_release_history".to_string(),
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("older_path={}", report.older_path),
        format!("newer_path={}", report.newer_path),
        format!(
            "older_released_at_source={}",
            report
                .older_released_at_source
                .map(|value| serialize_enum(&value))
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "newer_released_at_source={}",
            report
                .newer_released_at_source
                .map(|value| serialize_enum(&value))
                .unwrap_or_else(|| "null".to_string())
        ),
        format!("blocker_trend={}", serialize_enum(&report.blocker_trend)),
        format!("generation_id_changed={}", report.generation_id_changed),
        format!(
            "older_generation_id={}",
            report
                .older_generation_id
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "newer_generation_id={}",
            report
                .newer_generation_id
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "channel_promotion_attempted_changed={}",
            report.channel_promotion_attempted_changed
        ),
        format!(
            "channel_promotion_happened_changed={}",
            report.channel_promotion_happened_changed
        ),
        format!("channel_target_changed={}", report.channel_target_changed),
        format!("publish_reason_changed={}", report.publish_reason_changed),
        format!(
            "channel_promote_reason_changed={}",
            report.channel_promote_reason_changed
        ),
        format!("artifact_analysis_only={}", report.artifact_analysis_only),
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
    fn empty_history_yields_insufficient_artifacts() {
        let history_dir = temp_dir("release_history_empty");
        let report = build_history_report(&sample_config(history_dir.clone(), vec![], None))
            .expect("history report");

        assert_eq!(
            report.verdict,
            ArtifactReleaseHistoryVerdict::ArtifactReleaseHistoryInsufficientArtifacts
        );
        assert_eq!(report.releases_loaded, 0);
    }

    #[test]
    fn valid_release_series_yields_correct_latest_summary() {
        let history_dir = temp_dir("release_history_valid");
        write_json(
            &history_dir.join("release_older.json"),
            &sample_release_json(
                "2026-03-26T12:00:00Z",
                "artifact_release_published",
                "older-gen",
                true,
                false,
                "publish only",
                "channel blocked",
            ),
        );
        write_json(
            &history_dir.join("release_newer.json"),
            &sample_release_json(
                "2026-03-26T13:00:00Z",
                "artifact_release_published_and_promoted",
                "newer-gen",
                true,
                true,
                "published",
                "promoted",
            ),
        );

        let report = build_history_report(&sample_config(history_dir.clone(), vec![], None))
            .expect("history report");

        assert_eq!(
            report.verdict,
            ArtifactReleaseHistoryVerdict::ArtifactReleaseHistoryLatestPublishedAndPromoted
        );
        assert_eq!(report.published_only_count, 1);
        assert_eq!(report.published_and_promoted_count, 1);
        assert_eq!(
            report.latest_promoted_generation_id.as_deref(),
            Some("newer-gen")
        );
        assert_eq!(
            report.channel_promotion_progression,
            ChannelPromotionProgression::PromotedRecently
        );
    }

    #[test]
    fn compare_mode_shows_generation_verdict_and_channel_drift() {
        let older = temp_dir("release_history_compare").join("older.json");
        let newer = temp_dir("release_history_compare").join("newer.json");
        write_json(
            &older,
            &sample_release_json(
                "2026-03-26T12:00:00Z",
                "artifact_release_published",
                "older-gen",
                false,
                false,
                "publish only",
                "",
            ),
        );
        write_json(
            &newer,
            &sample_release_json(
                "2026-03-26T13:00:00Z",
                "artifact_release_published_and_promoted",
                "newer-gen",
                true,
                true,
                "publish + promote",
                "promoted",
            ),
        );

        let report = build_compare_report(&older, &newer).expect("compare report");

        assert_eq!(
            report.verdict,
            ArtifactReleaseHistoryVerdict::ArtifactReleaseHistoryCompareReady
        );
        assert!(report.generation_id_changed);
        assert!(report.channel_promotion_attempted_changed);
        assert!(report.channel_promotion_happened_changed);
        assert!(report.channel_target_changed);
        assert_eq!(report.blocker_trend, ReleaseChangeTrend::Narrower);
    }

    #[test]
    fn malformed_release_artifact_is_reported_as_invalid() {
        let history_dir = temp_dir("release_history_invalid");
        fs::write(history_dir.join("broken.json"), "{broken").expect("write invalid json");

        let report = build_history_report(&sample_config(history_dir.clone(), vec![], None))
            .expect("history report");

        assert_eq!(
            report.verdict,
            ArtifactReleaseHistoryVerdict::ArtifactReleaseHistoryInvalidArtifact
        );
        assert_eq!(report.invalid_artifact_count, 1);
    }

    #[test]
    fn blocked_channel_promote_release_is_reflected_honestly() {
        let history_dir = temp_dir("release_history_channel_block");
        write_json(
            &history_dir.join("release.json"),
            &sample_release_json(
                "2026-03-26T12:00:00Z",
                "artifact_release_channel_promote_blocked",
                "blocked-gen",
                true,
                false,
                "published",
                "channel metadata already exists",
            ),
        );

        let report = build_history_report(&sample_config(history_dir.clone(), vec![], None))
            .expect("history report");

        assert_eq!(
            report.verdict,
            ArtifactReleaseHistoryVerdict::ArtifactReleaseHistoryLatestBlocked
        );
        assert_eq!(report.channel_promote_blocked_count, 1);
        assert_eq!(
            report
                .latest_release
                .as_ref()
                .and_then(|value| value.generation_id.as_deref()),
            Some("blocked-gen")
        );
    }

    #[test]
    fn old_format_release_without_released_at_loads_successfully() {
        let history_dir = temp_dir("release_history_legacy");
        write_json(
            &history_dir.join("legacy_release.json"),
            &legacy_release_json(
                "artifact_release_published",
                "2026-03-26T12:00:00Z|prodhash|nonprodhash",
                false,
                false,
                "legacy publish",
                "",
            ),
        );

        let report = build_history_report(&sample_config(history_dir.clone(), vec![], None))
            .expect("history report");

        assert_eq!(
            report.verdict,
            ArtifactReleaseHistoryVerdict::ArtifactReleaseHistoryLatestPublished
        );
        assert_eq!(report.invalid_artifact_count, 0);
        assert_eq!(report.compat_loaded_without_released_at_count, 1);
        assert_eq!(
            report
                .latest_release
                .as_ref()
                .map(|value| value.released_at_source)
                .unwrap_or(ReleaseTimestampSource::MissingDeterministicTimestamp),
            ReleaseTimestampSource::GenerationIdentity
        );
    }

    #[test]
    fn mixed_old_and_new_release_artifacts_sort_deterministically() {
        let history_dir = temp_dir("release_history_mixed");
        write_json(
            &history_dir.join("legacy_release.json"),
            &legacy_release_json(
                "artifact_release_published",
                "2026-03-26T12:00:00Z|prodhash|nonprodhash",
                false,
                false,
                "legacy publish",
                "",
            ),
        );
        write_json(
            &history_dir.join("new_release.json"),
            &sample_release_json(
                "2026-03-26T13:00:00Z",
                "artifact_release_published_and_promoted",
                "2026-03-26T13:00:00Z|prodhash|nonprodhash",
                true,
                true,
                "published",
                "promoted",
            ),
        );

        let report = build_history_report(&sample_config(history_dir.clone(), vec![], None))
            .expect("history report");

        assert_eq!(
            report
                .latest_release
                .as_ref()
                .and_then(|value| value.generation_id.as_deref()),
            Some("2026-03-26T13:00:00Z|prodhash|nonprodhash")
        );
        assert_eq!(
            report.verdict,
            ArtifactReleaseHistoryVerdict::ArtifactReleaseHistoryLatestPublishedAndPromoted
        );
    }

    #[test]
    fn compare_mode_works_for_old_vs_new_pair() {
        let compare_dir = temp_dir("release_history_old_vs_new_compare");
        let older = compare_dir.join("older.json");
        let newer = compare_dir.join("newer.json");
        write_json(
            &older,
            &legacy_release_json(
                "artifact_release_published",
                "2026-03-26T12:00:00Z|prodhash|nonprodhash",
                false,
                false,
                "legacy publish",
                "",
            ),
        );
        write_json(
            &newer,
            &sample_release_json(
                "2026-03-26T13:00:00Z",
                "artifact_release_published_and_promoted",
                "2026-03-26T13:00:00Z|prodhash|nonprodhash",
                true,
                true,
                "publish + promote",
                "promoted",
            ),
        );

        let report = build_compare_report(&older, &newer).expect("compare report");

        assert_eq!(
            report.verdict,
            ArtifactReleaseHistoryVerdict::ArtifactReleaseHistoryCompareReady
        );
        assert_eq!(
            report.older_released_at_source,
            Some(ReleaseTimestampSource::GenerationIdentity)
        );
        assert_eq!(
            report.newer_released_at_source,
            Some(ReleaseTimestampSource::ReleasedAt)
        );
    }

    fn sample_config(
        history_dir: PathBuf,
        release_paths: Vec<PathBuf>,
        compare: Option<(PathBuf, PathBuf)>,
    ) -> Config {
        Config {
            history_dir: Some(history_dir),
            release_paths,
            compare,
            latest: false,
            limit: DEFAULT_HISTORY_LIMIT,
            recent_horizon_seconds: DEFAULT_RECENT_HORIZON_SECONDS,
            json: false,
        }
    }

    fn sample_release_json(
        released_at: &str,
        verdict: &str,
        generation_id: &str,
        channel_promotion_attempted: bool,
        channel_promotion_happened: bool,
        publish_reason: &str,
        channel_promote_reason: &str,
    ) -> serde_json::Value {
        let channel_name = if channel_promotion_attempted {
            Some("current_review")
        } else {
            None
        };
        let channel_dir = if channel_promotion_attempted {
            Some("/var/www/solana-copy-bot/state/activation_artifacts/channel")
        } else {
            None
        };
        let channel_metadata_path = if channel_promotion_attempted {
            Some("/var/www/solana-copy-bot/state/activation_artifacts/channel/current_review.json")
        } else {
            None
        };
        let channel_promote_verdict = if channel_promotion_attempted {
            Some(if channel_promotion_happened {
                "artifact_channel_promoted"
            } else {
                "artifact_channel_refused_without_overwrite"
            })
        } else {
            None
        };
        let channel_promote_reason = if channel_promotion_attempted {
            Some(channel_promote_reason)
        } else {
            None
        };
        let channel_verify_verdict = if channel_promotion_happened {
            Some("artifact_channel_ok")
        } else {
            None
        };
        let channel_verify_reason = if channel_promotion_happened {
            Some("verify ok")
        } else {
            None
        };

        json!({
            "mode": "artifact_release",
            "released_at": released_at,
            "verdict": verdict,
            "reason": format!("release {verdict}"),
            "config_path": "/etc/solana-copy-bot/live.server.toml",
            "non_prod_config_path": "/etc/solana-copy-bot/devnet.server.toml",
            "archive_dir": "/var/www/solana-copy-bot/state/activation_artifacts/archive",
            "generation_id": generation_id,
            "generation_directory": format!("/var/www/solana-copy-bot/state/activation_artifacts/archive/{generation_id}"),
            "packet_path": format!("/var/www/solana-copy-bot/state/activation_artifacts/archive/{generation_id}/decision_packet.json"),
            "runbook_json_path": format!("/var/www/solana-copy-bot/state/activation_artifacts/archive/{generation_id}/activation_runbook.json"),
            "runbook_markdown_path": format!("/var/www/solana-copy-bot/state/activation_artifacts/archive/{generation_id}/activation_runbook.md"),
            "manifest_path": "/var/www/solana-copy-bot/state/activation_artifacts/archive_manifest/latest.json",
            "bundle_path": format!("/var/www/solana-copy-bot/state/activation_artifacts/bundles/{generation_id}"),
            "decision_packet_verdict": "decision_packet_discussion_ready_but_not_authorized",
            "checklist_verdict": "activation_checklist_discussion_ready_but_not_authorized",
            "publish_verdict": if verdict == "artifact_release_publish_failed" { "artifact_publish_failed" } else { "artifact_publish_succeeded" },
            "publish_reason": publish_reason,
            "generation_published": verdict != "artifact_release_publish_failed",
            "manifest_generated": true,
            "bundle_exported": true,
            "channel_promotion_attempted": channel_promotion_attempted,
            "channel_promotion_happened": channel_promotion_happened,
            "channel_verify_attempted": channel_promotion_happened,
            "channel_name": channel_name,
            "channel_dir": channel_dir,
            "channel_metadata_path": channel_metadata_path,
            "channel_promote_verdict": channel_promote_verdict,
            "channel_promote_reason": channel_promote_reason,
            "channel_verify_verdict": channel_verify_verdict,
            "channel_verify_reason": channel_verify_reason,
            "execution_untouched": true,
            "activation_authorized": false,
            "not_authorized_summary": "Artifact/release analysis only."
        })
    }

    fn legacy_release_json(
        verdict: &str,
        generation_id: &str,
        channel_promotion_attempted: bool,
        channel_promotion_happened: bool,
        publish_reason: &str,
        channel_promote_reason: &str,
    ) -> serde_json::Value {
        let mut value = sample_release_json(
            generation_id
                .split('|')
                .next()
                .expect("legacy generation timestamp"),
            verdict,
            generation_id,
            channel_promotion_attempted,
            channel_promotion_happened,
            publish_reason,
            channel_promote_reason,
        );
        value
            .as_object_mut()
            .expect("legacy object")
            .remove("released_at");
        value
    }

    fn write_json(path: &Path, value: &serde_json::Value) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create parent dir");
        }
        fs::write(
            path,
            serde_json::to_string_pretty(value).expect("serialize"),
        )
        .expect("write json");
    }

    fn temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("{}_{}", prefix, nanos));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }
}
