#![recursion_limit = "256"]
#![allow(unused_attributes)]

use anyhow::{anyhow, bail, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::env;
use std::path::PathBuf;

#[allow(dead_code)]
#[path = "copybot_activation_artifact_channel.rs"]
mod activation_artifact_channel;
#[allow(dead_code)]
#[path = "copybot_activation_artifact_linkage_report.rs"]
mod activation_artifact_linkage_report;
#[allow(dead_code)]
#[path = "copybot_activation_artifact_provenance_report.rs"]
mod activation_artifact_provenance_report;
#[allow(dead_code)]
#[path = "copybot_activation_artifact_release_provenance_report.rs"]
mod activation_artifact_release_provenance_report;
#[allow(dead_code)]
#[path = "copybot_activation_artifact_release_publish_report.rs"]
mod activation_artifact_release_publish_report;

const USAGE: &str = "usage: copybot_activation_artifact_state_report --review-archive-dir <path> --review-manifest-dir <path> --review-bundle-dir <path> --review-channel-dir <path> --release-archive-dir <path> --release-history-dir <path> --latest-pointer-dir <path> [--review-channel-name <name>] [--latest-pointer-name <name>] [--json]";

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
pub(crate) struct Config {
    pub(crate) review_archive_dir: PathBuf,
    pub(crate) review_manifest_dir: PathBuf,
    pub(crate) review_bundle_dir: PathBuf,
    pub(crate) review_channel_dir: PathBuf,
    pub(crate) review_channel_name: String,
    pub(crate) release_archive_dir: PathBuf,
    pub(crate) release_history_dir: PathBuf,
    pub(crate) latest_pointer_dir: PathBuf,
    pub(crate) latest_pointer_name: String,
    pub(crate) json: bool,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ArtifactStateVerdict {
    ArtifactStateCoherent,
    ArtifactStateIncomplete,
    ArtifactStateInvalidArtifactsPresent,
    ArtifactStateInconsistent,
    ArtifactStateAmbiguousLegacyState,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct CurrentReviewGenerationSummary {
    pub(crate) channel_verdict: String,
    pub(crate) channel_reason: String,
    pub(crate) channel_metadata_path: String,
    pub(crate) selected_generation_id: Option<String>,
    pub(crate) decision_packet_generated_at: Option<DateTime<Utc>>,
    pub(crate) prod_config_fingerprint_sha256: Option<String>,
    pub(crate) non_prod_config_fingerprint_sha256: Option<String>,
    pub(crate) manifest_verification_verdict: Option<String>,
    pub(crate) bundle_verification_verdict: Option<String>,
    pub(crate) missing_paths: Vec<String>,
    pub(crate) inconsistencies: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct CurrentLatestReleaseSummary {
    pub(crate) pointer_verdict: String,
    pub(crate) pointer_reason: String,
    pub(crate) latest_pointer_path: Option<String>,
    pub(crate) persisted_release_artifact_path: Option<String>,
    pub(crate) release_verdict: Option<String>,
    pub(crate) generation_id: Option<String>,
    pub(crate) released_at: Option<DateTime<Utc>>,
    pub(crate) released_at_source: Option<String>,
    pub(crate) compat_loaded_without_released_at: bool,
    pub(crate) deterministic_timestamp_available: bool,
    pub(crate) ordered_history_confident: bool,
    pub(crate) target_exists: bool,
    pub(crate) target_matches_identity: bool,
    pub(crate) missing_paths: Vec<String>,
    pub(crate) inconsistencies: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SelectionAlignmentSummary {
    pub(crate) selections_match: bool,
    pub(crate) summary: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ReviewProvenanceStateSummary {
    pub(crate) verdict: String,
    pub(crate) reason: String,
    pub(crate) archive_generation_count: usize,
    pub(crate) manifest_file_count: usize,
    pub(crate) bundle_manifest_count: usize,
    pub(crate) complete_generation_count: usize,
    pub(crate) incomplete_generation_count: usize,
    pub(crate) invalid_artifact_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ReleaseProvenanceStateSummary {
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

#[derive(Debug, Clone, Serialize)]
pub(crate) struct LinkageStateSummary {
    pub(crate) verdict: String,
    pub(crate) reason: String,
    pub(crate) release_artifact_count_examined: usize,
    pub(crate) linked_generation_count: usize,
    pub(crate) missing_generation_ref_count: usize,
    pub(crate) missing_packet_ref_count: usize,
    pub(crate) missing_runbook_ref_count: usize,
    pub(crate) invalid_artifact_count: usize,
    pub(crate) ambiguous_legacy_reference_count: usize,
    pub(crate) latest_selected_release_linkage_verdict: Option<String>,
    pub(crate) latest_selected_release_linkage_reason: Option<String>,
    pub(crate) latest_selected_generation_id: Option<String>,
    pub(crate) latest_target_exists: bool,
    pub(crate) latest_target_matches_identity: bool,
    pub(crate) latest_linked_generation_present: bool,
    pub(crate) review_channel_selected_generation_id: Option<String>,
    pub(crate) review_channel_matches_latest_selection: bool,
    pub(crate) review_channel_divergence_summary: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ArtifactStateReport {
    pub(crate) mode: String,
    pub(crate) verdict: ArtifactStateVerdict,
    pub(crate) reason: String,
    pub(crate) review_archive_dir: String,
    pub(crate) review_manifest_dir: String,
    pub(crate) review_bundle_dir: String,
    pub(crate) review_channel_dir: String,
    pub(crate) review_channel_name: String,
    pub(crate) release_archive_dir: String,
    pub(crate) release_history_dir: String,
    pub(crate) latest_pointer_dir: String,
    pub(crate) latest_pointer_name: String,
    pub(crate) current_review_generation: CurrentReviewGenerationSummary,
    pub(crate) current_latest_release: CurrentLatestReleaseSummary,
    pub(crate) selection_alignment: SelectionAlignmentSummary,
    pub(crate) review_provenance: ReviewProvenanceStateSummary,
    pub(crate) release_provenance: ReleaseProvenanceStateSummary,
    pub(crate) linkage: LinkageStateSummary,
    pub(crate) ambiguous_legacy_count: usize,
    pub(crate) coherent_for_review_operations: bool,
    pub(crate) artifact_state_only: bool,
    pub(crate) execution_untouched: bool,
    pub(crate) activation_authorized: bool,
    pub(crate) not_authorized_summary: String,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

pub(crate) fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
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
    let trimmed = raw.trim().to_string();
    if trimmed.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(trimmed)
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
    let report = inspect_state_report(&config)?;
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_human(&report))
    }
}

pub(crate) fn inspect_state_report(config: &Config) -> Result<ArtifactStateReport> {
    let channel_report = activation_artifact_channel::inspect_channel(
        &activation_artifact_channel::Config {
            archive_dir: config.review_archive_dir.clone(),
            channel_dir: config.review_channel_dir.clone(),
            channel_name: config.review_channel_name.clone(),
            json: false,
            mode: activation_artifact_channel::Mode::Verify,
        },
        true,
    )?;
    let latest_pointer_report =
        activation_artifact_release_publish_report::inspect_latest_pointer_report(
            &config.release_archive_dir,
            &config.latest_pointer_dir,
            &config.latest_pointer_name,
            true,
        )?;
    let review_provenance = activation_artifact_provenance_report::inspect_provenance_summary(
        &config.review_archive_dir,
        &config.review_manifest_dir,
        &config.review_bundle_dir,
    )?;
    let release_provenance =
        activation_artifact_release_provenance_report::inspect_release_provenance_summary(
            &config.release_archive_dir,
            &config.latest_pointer_dir,
            Some(&config.release_history_dir),
            &[],
            &config.latest_pointer_name,
        )?;
    let linkage = activation_artifact_linkage_report::inspect_linkage_summary(
        Some(&config.release_archive_dir),
        &[],
        &config.review_archive_dir,
        Some(&config.latest_pointer_dir),
        &config.latest_pointer_name,
        Some(&config.review_channel_dir),
        &config.review_channel_name,
    )?;

    let current_review_generation = CurrentReviewGenerationSummary {
        channel_verdict: serialize_enum(&channel_report.verdict),
        channel_reason: channel_report.reason.clone(),
        channel_metadata_path: channel_report.channel_metadata_path.clone(),
        selected_generation_id: channel_report.selected_generation_id.clone(),
        decision_packet_generated_at: channel_report.decision_packet_generated_at,
        prod_config_fingerprint_sha256: channel_report.prod_config_fingerprint_sha256.clone(),
        non_prod_config_fingerprint_sha256: channel_report
            .non_prod_config_fingerprint_sha256
            .clone(),
        manifest_verification_verdict: channel_report.manifest_verification_verdict.clone(),
        bundle_verification_verdict: channel_report.bundle_verification_verdict.clone(),
        missing_paths: channel_report.missing_paths.clone(),
        inconsistencies: channel_report.inconsistencies.clone(),
    };
    let current_release_generation_id = latest_pointer_report
        .generation_id
        .clone()
        .or_else(|| {
            release_provenance
                .latest_pointer_selected_generation_id
                .clone()
        })
        .or_else(|| linkage.latest_selected_generation_id.clone());
    let current_latest_release = CurrentLatestReleaseSummary {
        pointer_verdict: serialize_enum(&latest_pointer_report.verdict),
        pointer_reason: latest_pointer_report.reason.clone(),
        latest_pointer_path: latest_pointer_report.latest_pointer_path.clone(),
        persisted_release_artifact_path: latest_pointer_report
            .persisted_release_artifact_path
            .clone(),
        release_verdict: latest_pointer_report.release_verdict.clone(),
        generation_id: current_release_generation_id.clone(),
        released_at: latest_pointer_report.released_at,
        released_at_source: latest_pointer_report
            .released_at_source
            .as_ref()
            .map(serialize_enum),
        compat_loaded_without_released_at: latest_pointer_report.compat_loaded_without_released_at,
        deterministic_timestamp_available: latest_pointer_report.deterministic_timestamp_available,
        ordered_history_confident: latest_pointer_report.ordered_history_confident,
        target_exists: latest_pointer_report.latest_pointer_target_exists,
        target_matches_identity: latest_pointer_report.latest_pointer_target_matches_identity,
        missing_paths: latest_pointer_report.missing_paths.clone(),
        inconsistencies: latest_pointer_report.inconsistencies.clone(),
    };
    let review_provenance_summary = ReviewProvenanceStateSummary {
        verdict: review_provenance.verdict.clone(),
        reason: review_provenance.reason.clone(),
        archive_generation_count: review_provenance.archive_generation_count,
        manifest_file_count: review_provenance.manifest_file_count,
        bundle_manifest_count: review_provenance.bundle_manifest_count,
        complete_generation_count: review_provenance.complete_generation_count,
        incomplete_generation_count: review_provenance.incomplete_generation_count,
        invalid_artifact_count: review_provenance.invalid_artifact_count,
    };
    let release_provenance_summary = ReleaseProvenanceStateSummary {
        verdict: release_provenance.verdict.clone(),
        reason: release_provenance.reason.clone(),
        archive_release_count: release_provenance.archive_release_count,
        history_release_count: release_provenance.history_release_count,
        latest_pointer_present: release_provenance.latest_pointer_present,
        latest_pointer_selected_generation_id: release_provenance
            .latest_pointer_selected_generation_id
            .clone(),
        latest_pointer_relation: release_provenance.latest_pointer_relation.clone(),
        latest_archive_generation_id: release_provenance.latest_archive_generation_id.clone(),
        latest_history_generation_id: release_provenance.latest_history_generation_id.clone(),
        archive_releases_missing_from_history_count: release_provenance
            .archive_releases_missing_from_history_count,
        history_releases_missing_from_archive_count: release_provenance
            .history_releases_missing_from_archive_count,
        invalid_artifact_count: release_provenance.invalid_artifact_count,
        ambiguous_timestamp_count: release_provenance.ambiguous_timestamp_count,
    };
    let linkage_summary = LinkageStateSummary {
        verdict: linkage.verdict.clone(),
        reason: linkage.reason.clone(),
        release_artifact_count_examined: linkage.release_artifact_count_examined,
        linked_generation_count: linkage.linked_generation_count,
        missing_generation_ref_count: linkage.missing_generation_ref_count,
        missing_packet_ref_count: linkage.missing_packet_ref_count,
        missing_runbook_ref_count: linkage.missing_runbook_ref_count,
        invalid_artifact_count: linkage.invalid_artifact_count,
        ambiguous_legacy_reference_count: linkage.ambiguous_legacy_reference_count,
        latest_selected_release_linkage_verdict: linkage
            .latest_selected_release_linkage_verdict
            .clone(),
        latest_selected_release_linkage_reason: linkage
            .latest_selected_release_linkage_reason
            .clone(),
        latest_selected_generation_id: linkage.latest_selected_generation_id.clone(),
        latest_target_exists: linkage.latest_target_exists,
        latest_target_matches_identity: linkage.latest_target_matches_identity,
        latest_linked_generation_present: linkage.latest_linked_generation_present,
        review_channel_selected_generation_id: linkage
            .review_channel_selected_generation_id
            .clone(),
        review_channel_matches_latest_selection: linkage.review_channel_matches_latest_selection,
        review_channel_divergence_summary: linkage.review_channel_divergence_summary.clone(),
    };
    let selection_alignment = build_selection_alignment(
        current_review_generation.selected_generation_id.as_deref(),
        current_latest_release.generation_id.as_deref(),
        linkage.review_channel_matches_latest_selection,
        linkage.review_channel_divergence_summary.as_deref(),
    );

    let review_channel_missing = matches!(
        channel_report.verdict,
        activation_artifact_channel::ArtifactChannelVerdict::ArtifactChannelMissingTarget
    ) && !channel_report.channel_metadata_exists;
    let release_pointer_missing = matches!(
        latest_pointer_report.verdict,
        activation_artifact_release_publish_report::ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportVerifyMissingTarget
    ) && !latest_pointer_report.latest_pointer_exists;
    let latest_pointer_target_broken = matches!(
        latest_pointer_report.verdict,
        activation_artifact_release_publish_report::ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportVerifyMissingTarget
    ) && latest_pointer_report.latest_pointer_exists;

    let invalid = review_provenance.verdict == "artifact_provenance_invalid_artifacts_present"
        || release_provenance.verdict == "artifact_release_provenance_invalid_artifacts_present"
        || linkage.verdict == "artifact_linkage_invalid_artifacts_present"
        || matches!(
            channel_report.verdict,
            activation_artifact_channel::ArtifactChannelVerdict::ArtifactChannelInvalidMetadata
        )
        || matches!(
            latest_pointer_report.verdict,
            activation_artifact_release_publish_report::ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportInvalidMetadata
        );
    let inconsistent = !invalid
        && (review_provenance.verdict == "artifact_provenance_inconsistent_lineage"
            || release_provenance.verdict == "artifact_release_provenance_inconsistent_lineage"
            || linkage.verdict == "artifact_linkage_inconsistent"
            || matches!(
                channel_report.verdict,
                activation_artifact_channel::ArtifactChannelVerdict::ArtifactChannelInconsistent
            )
            || latest_pointer_target_broken
            || !selection_alignment.selections_match
                && selection_alignment.summary.contains("diverge"));
    let incomplete = !invalid
        && !inconsistent
        && (review_provenance.verdict == "artifact_provenance_incomplete"
            || release_provenance.verdict == "artifact_release_provenance_incomplete"
            || linkage.verdict == "artifact_linkage_incomplete"
            || review_channel_missing
            || release_pointer_missing
            || current_review_generation.selected_generation_id.is_none()
            || current_latest_release.generation_id.is_none());
    let ambiguous = !invalid
        && (release_provenance.verdict == "artifact_release_provenance_ambiguous_timestamp"
            || linkage.verdict == "artifact_linkage_ambiguous_legacy_reference"
            || matches!(
                latest_pointer_report.verdict,
                activation_artifact_release_publish_report::ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportVerifyAmbiguousTimestamp
            )
            || latest_pointer_report.compat_loaded_without_released_at
            || !latest_pointer_report.ordered_history_confident);

    let ambiguous_legacy_count = release_provenance.ambiguous_timestamp_count
        + linkage.ambiguous_legacy_reference_count
        + usize::from(latest_pointer_report.compat_loaded_without_released_at);

    let (verdict, reason) = if invalid {
        (
            ArtifactStateVerdict::ArtifactStateInvalidArtifactsPresent,
            "review-side, release-side, or cross-surface artifact metadata contains invalid persisted state; do not trust current artifact state until those issues are fixed".to_string(),
        )
    } else if inconsistent {
        (
            ArtifactStateVerdict::ArtifactStateInconsistent,
            "current review generation, latest release artifact, provenance, or linkage state diverge from each other".to_string(),
        )
    } else if incomplete {
        (
            ArtifactStateVerdict::ArtifactStateIncomplete,
            "current artifact state is only partially covered; review-side or release-side provenance/current selections are missing".to_string(),
        )
    } else if ambiguous {
        (
            ArtifactStateVerdict::ArtifactStateAmbiguousLegacyState,
            "legacy or compat-loaded release state leaves current artifact ordering/linkage ambiguous".to_string(),
        )
    } else {
        (
            ArtifactStateVerdict::ArtifactStateCoherent,
            "current review generation, latest release artifact, provenance, and linkage state are mutually coherent for artifact review operations".to_string(),
        )
    };

    Ok(ArtifactStateReport {
        mode: "artifact_state_report".to_string(),
        verdict,
        reason,
        review_archive_dir: config.review_archive_dir.display().to_string(),
        review_manifest_dir: config.review_manifest_dir.display().to_string(),
        review_bundle_dir: config.review_bundle_dir.display().to_string(),
        review_channel_dir: config.review_channel_dir.display().to_string(),
        review_channel_name: config.review_channel_name.clone(),
        release_archive_dir: config.release_archive_dir.display().to_string(),
        release_history_dir: config.release_history_dir.display().to_string(),
        latest_pointer_dir: config.latest_pointer_dir.display().to_string(),
        latest_pointer_name: config.latest_pointer_name.clone(),
        current_review_generation,
        current_latest_release,
        selection_alignment: selection_alignment.clone(),
        review_provenance: review_provenance_summary,
        release_provenance: release_provenance_summary,
        linkage: linkage_summary,
        ambiguous_legacy_count,
        coherent_for_review_operations: verdict == ArtifactStateVerdict::ArtifactStateCoherent,
        artifact_state_only: true,
        execution_untouched: channel_report.execution_untouched
            && latest_pointer_report.execution_untouched,
        activation_authorized: false,
        not_authorized_summary:
            "Artifact state analysis only summarizes persisted review-side and release-side artifacts. It does not authorize activation and does not override the Stage 3 production gate.".to_string(),
    })
}

impl ArtifactStateReport {
    pub(crate) fn selected_review_generation_id(&self) -> Option<&str> {
        self.current_review_generation
            .selected_generation_id
            .as_deref()
    }

    pub(crate) fn selected_latest_release_generation_id(&self) -> Option<&str> {
        self.current_latest_release.generation_id.as_deref()
    }
}

fn build_selection_alignment(
    review_generation_id: Option<&str>,
    release_generation_id: Option<&str>,
    linkage_match: bool,
    divergence_summary: Option<&str>,
) -> SelectionAlignmentSummary {
    if let Some(summary) = divergence_summary {
        return SelectionAlignmentSummary {
            selections_match: false,
            summary: summary.to_string(),
        };
    }
    match (review_generation_id, release_generation_id) {
        (Some(review), Some(release)) if review == release && linkage_match => {
            SelectionAlignmentSummary {
                selections_match: true,
                summary: format!(
                    "review channel and latest release pointer both select generation {review}"
                ),
            }
        }
        (Some(review), Some(release)) => SelectionAlignmentSummary {
            selections_match: false,
            summary: format!(
                "review channel selects generation {review}, but latest release points at {release}"
            ),
        },
        (Some(review), None) => SelectionAlignmentSummary {
            selections_match: false,
            summary: format!(
                "review channel selects generation {review}, but no current latest release generation is available"
            ),
        },
        (None, Some(release)) => SelectionAlignmentSummary {
            selections_match: false,
            summary: format!(
                "latest release points at generation {release}, but no current review channel selection is available"
            ),
        },
        (None, None) => SelectionAlignmentSummary {
            selections_match: false,
            summary: "no current review generation or latest release generation could be established".to_string(),
        },
    }
}

fn render_human(report: &ArtifactStateReport) -> String {
    [
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!(
            "current_review_generation={}",
            report
                .current_review_generation
                .selected_generation_id
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "current_latest_release_generation={}",
            report
                .current_latest_release
                .generation_id
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!("selection_alignment={}", report.selection_alignment.summary),
        format!(
            "review_provenance_verdict={}",
            report.review_provenance.verdict
        ),
        format!(
            "release_provenance_verdict={}",
            report.release_provenance.verdict
        ),
        format!("linkage_verdict={}", report.linkage.verdict),
        format!("ambiguous_legacy_count={}", report.ambiguous_legacy_count),
        format!(
            "coherent_for_review_operations={}",
            report.coherent_for_review_operations
        ),
        format!("artifact_state_only={}", report.artifact_state_only),
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
    use std::fs;
    use std::path::Path;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn fully_aligned_healthy_state_yields_coherent_verdict() {
        let fixture = write_complete_fixture("artifact_state_coherent");
        let report = inspect_state_report(&fixture.config).expect("report");

        assert_eq!(report.verdict, ArtifactStateVerdict::ArtifactStateCoherent);
        assert!(report.selection_alignment.selections_match);
        assert!(report.coherent_for_review_operations);
        assert!(report.execution_untouched);
        assert!(!report.activation_authorized);
    }

    #[test]
    fn divergence_between_review_channel_and_release_latest_pointer_is_inconsistent() {
        let fixture = write_complete_fixture("artifact_state_divergent");
        let generation_b = write_review_generation(
            &fixture.review_archive_dir,
            "2026-03-26T13:00:00Z",
            "prod_fp_b",
            "non_prod_fp_b",
        );
        fs::create_dir_all(&fixture.review_manifest_dir).expect("manifest dir");
        fs::write(
            fixture.review_manifest_dir.join("manifest_b.json"),
            serde_json::to_string_pretty(&build_manifest_document(&[
                fixture.generation.identity.clone(),
                generation_b.identity.clone(),
            ]))
            .expect("manifest json"),
        )
        .expect("write manifest b");
        write_bundle_manifest(
            &fixture.review_bundle_dir.join("bundle_b"),
            &generation_b.identity,
        );
        write_review_channel(
            &fixture.review_channel_dir,
            &fixture.review_archive_dir,
            &generation_b,
            &fixture.config.review_channel_name,
        );

        let report = inspect_state_report(&fixture.config).expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateVerdict::ArtifactStateInconsistent
        );
        assert!(!report.selection_alignment.selections_match);
    }

    #[test]
    fn broken_latest_release_pointer_is_surfaced_explicitly() {
        let fixture = write_complete_fixture("artifact_state_broken_pointer");
        fs::remove_file(&fixture.release_artifact_path).expect("remove release artifact");

        let report = inspect_state_report(&fixture.config).expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateVerdict::ArtifactStateInconsistent
        );
        assert_eq!(
            report.current_latest_release.pointer_verdict,
            "artifact_release_report_verify_missing_target"
        );
        assert_eq!(report.ambiguous_legacy_count, 0);
    }

    #[test]
    fn compat_loaded_legacy_pointer_with_missing_target_stays_broken_not_ambiguous() {
        let temp_root = temp_dir("artifact_state_broken_legacy_pointer");
        let review_archive_dir = temp_root.join("review_archive");
        let review_manifest_dir = temp_root.join("review_manifest");
        let review_bundle_dir = temp_root.join("review_bundle");
        let review_channel_dir = temp_root.join("review_channel");
        let release_archive_dir = temp_root.join("release_archive");
        let release_history_dir = temp_root.join("release_history");
        let latest_pointer_dir = temp_root.join("release_pointer");
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
        let release_artifact_path = release_archive_dir.join("legacy_release.json");
        write_release_artifact(
            &release_artifact_path,
            None,
            Some(&generation.generation_id),
            Some(&generation.generation_dir),
            Some(&generation.packet_path),
            Some(&generation.runbook_json_path),
            Some(&generation.runbook_markdown_path),
            "artifact_release_published",
        );
        fs::create_dir_all(&release_history_dir).expect("history dir");
        fs::copy(
            &release_artifact_path,
            release_history_dir.join("legacy_release.json"),
        )
        .expect("copy legacy release");
        write_latest_pointer(
            &latest_pointer_dir,
            &release_archive_dir,
            &release_artifact_path,
            None,
            "missing_deterministic_timestamp",
            Some(&generation.generation_id),
            "artifact_release_published",
        );
        fs::remove_file(&release_artifact_path).expect("remove archive target");

        let report = inspect_state_report(&Config {
            review_archive_dir,
            review_manifest_dir,
            review_bundle_dir,
            review_channel_dir,
            review_channel_name: activation_artifact_channel::DEFAULT_CHANNEL_NAME.to_string(),
            release_archive_dir,
            release_history_dir,
            latest_pointer_dir,
            latest_pointer_name:
                activation_artifact_release_publish_report::DEFAULT_LATEST_POINTER_NAME.to_string(),
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateVerdict::ArtifactStateInconsistent
        );
        assert!(report.ambiguous_legacy_count > 0);
        assert_eq!(
            report.current_latest_release.pointer_verdict,
            "artifact_release_report_verify_missing_target"
        );
    }

    #[test]
    fn incomplete_release_provenance_yields_non_green_state() {
        let fixture = write_complete_fixture("artifact_state_incomplete_release");
        fs::remove_file(
            fixture.release_history_dir.join(
                fixture
                    .release_artifact_path
                    .file_name()
                    .expect("history name"),
            ),
        )
        .expect("remove history release");

        let report = inspect_state_report(&fixture.config).expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateVerdict::ArtifactStateIncomplete
        );
        assert_eq!(
            report.release_provenance.verdict,
            "artifact_release_provenance_incomplete"
        );
    }

    #[test]
    fn ambiguous_legacy_release_state_is_not_treated_as_clean_green() {
        let fixture = write_complete_fixture("artifact_state_ambiguous");
        write_release_artifact(
            &fixture.release_artifact_path,
            None,
            Some(&fixture.generation.generation_id),
            Some(&fixture.generation.generation_dir),
            Some(&fixture.generation.packet_path),
            Some(&fixture.generation.runbook_json_path),
            Some(&fixture.generation.runbook_markdown_path),
            "artifact_release_published",
        );
        fs::copy(
            &fixture.release_artifact_path,
            fixture.release_history_dir.join(
                fixture
                    .release_artifact_path
                    .file_name()
                    .expect("release artifact file name"),
            ),
        )
        .expect("refresh legacy release history");
        write_latest_pointer(
            &fixture.config.latest_pointer_dir,
            &fixture.config.release_archive_dir,
            &fixture.release_artifact_path,
            Some("2026-03-26T12:00:00Z"),
            "generation_identity",
            Some(&fixture.generation.generation_id),
            "artifact_release_published",
        );

        let report = inspect_state_report(&fixture.config).expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateVerdict::ArtifactStateAmbiguousLegacyState
        );
        assert!(report.ambiguous_legacy_count > 0);
    }

    #[test]
    fn compat_loaded_legacy_pointer_with_inconsistent_release_provenance_stays_broken_not_ambiguous(
    ) {
        let fixture = write_complete_fixture("artifact_state_legacy_inconsistent_release");
        write_release_artifact(
            &fixture.release_artifact_path,
            None,
            Some(&fixture.generation.generation_id),
            Some(&fixture.generation.generation_dir),
            Some(&fixture.generation.packet_path),
            Some(&fixture.generation.runbook_json_path),
            Some(&fixture.generation.runbook_markdown_path),
            "artifact_release_published_and_promoted",
        );
        fs::copy(
            &fixture.release_artifact_path,
            fixture.release_history_dir.join(
                fixture
                    .release_artifact_path
                    .file_name()
                    .expect("release artifact file name"),
            ),
        )
        .expect("refresh history artifact");
        write_latest_pointer(
            &fixture.config.latest_pointer_dir,
            &fixture.config.release_archive_dir,
            &fixture.release_artifact_path,
            None,
            "missing_deterministic_timestamp",
            Some(&fixture.generation.generation_id),
            "artifact_release_published_and_promoted",
        );
        fs::remove_file(&fixture.release_artifact_path).expect("remove archive release target");

        let report = inspect_state_report(&fixture.config).expect("report");

        assert_eq!(
            report.verdict,
            ArtifactStateVerdict::ArtifactStateInconsistent
        );
        assert_eq!(
            report.release_provenance.verdict,
            "artifact_release_provenance_inconsistent_lineage"
        );
        assert!(report.ambiguous_legacy_count > 0);
    }

    #[derive(Debug)]
    struct Fixture {
        config: Config,
        review_archive_dir: PathBuf,
        review_manifest_dir: PathBuf,
        review_bundle_dir: PathBuf,
        review_channel_dir: PathBuf,
        release_history_dir: PathBuf,
        generation: ReviewGenerationFixture,
        release_artifact_path: PathBuf,
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
        let temp_root = temp_dir(prefix);
        let review_archive_dir = temp_root.join("review_archive");
        let review_manifest_dir = temp_root.join("review_manifest");
        let review_bundle_dir = temp_root.join("review_bundle");
        let review_channel_dir = temp_root.join("review_channel");
        let release_archive_dir = temp_root.join("release_archive");
        let release_history_dir = temp_root.join("release_history");
        let latest_pointer_dir = temp_root.join("release_pointer");
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
            config: Config {
                review_archive_dir: review_archive_dir.clone(),
                review_manifest_dir: review_manifest_dir.clone(),
                review_bundle_dir: review_bundle_dir.clone(),
                review_channel_dir: review_channel_dir.clone(),
                review_channel_name: activation_artifact_channel::DEFAULT_CHANNEL_NAME.to_string(),
                release_archive_dir,
                release_history_dir: release_history_dir.clone(),
                latest_pointer_dir,
                latest_pointer_name:
                    activation_artifact_release_publish_report::DEFAULT_LATEST_POINTER_NAME
                        .to_string(),
                json: false,
            },
            review_archive_dir,
            review_manifest_dir,
            review_bundle_dir,
            review_channel_dir,
            release_history_dir,
            generation,
            release_artifact_path,
        }
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
            generated_at: DateTime::parse_from_rfc3339(generated_at)
                .expect("generated_at")
                .with_timezone(&Utc),
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
                    "trigger": "consecutive_hard_failures",
                    "threshold_kind": "count",
                    "threshold_rate_pct": null,
                    "threshold_seconds": null,
                    "threshold_sol": null,
                    "threshold_count": 3,
                    "evaluation_window_seconds": 600,
                    "action": "rollback_now"
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
                "dress_latest_record_age_seconds": 120,
                "dress_recent_green_count": 2,
                "activation_latest_record_age_seconds": 120,
                "activation_recent_green_count": 2,
                "activation_recent_rollback_success_count": 2,
                "activation_recent_internal_consistency_count": 2,
                "stale_evidence_excluded": false
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
            "reason": "runbook sample",
            "blockers": [],
            "warnings": ["planning-only"],
            "not_authorized_disclaimer": "planning-only",
            "decision_packet_version": "1",
            "decision_packet_generated_at": decision_packet_generated_at,
            "prod_config_fingerprint_sha256": prod_fingerprint,
            "non_prod_config_fingerprint_sha256": non_prod_fingerprint,
            "decision_packet_verdict": "decision_packet_discussion_ready_but_not_authorized",
            "decision_packet_reason": "sample",
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
                "rollback_triggers": [{
                    "trigger": "consecutive_hard_failures",
                    "threshold_kind": "count",
                    "threshold_rate_pct": null,
                    "threshold_seconds": null,
                    "threshold_sol": null,
                    "threshold_count": 3,
                    "evaluation_window_seconds": 600,
                    "action": "rollback_now"
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
                "dress_latest_record_age_seconds": 120,
                "dress_recent_green_count": 2,
                "activation_latest_record_age_seconds": 120,
                "activation_recent_green_count": 2,
                "activation_recent_rollback_success_count": 2,
                "activation_recent_internal_consistency_count": 2,
                "stale_evidence_excluded": false
            },
            "activation_candidate": {
                "verdict": "activation_plan_ready_when_stage_gate_allows",
                "reason": "ready",
                "activation_overlay_complete": true,
                "activation_overlay_change_count": 2,
                "activation_overlay_changes": [],
                "effective_tiny_live_contract": null
            },
            "rollback_summary": {
                "rollback_plan_complete": true,
                "service_restart_contract_complete": true,
                "rollback_overlay_change_count": 2,
                "rollback_overlay_changes": [],
                "service_restart_contract": {
                    "service_name": "solana-copy-bot.service",
                    "reload_supported": false,
                    "restart_required": true,
                    "restart_reason": "execution overlay changes require service restart"
                },
                "rollback_trigger_count": 1,
                "rollback_triggers": [{
                    "trigger": "consecutive_hard_failures",
                    "threshold_kind": "count",
                    "threshold_rate_pct": null,
                    "threshold_seconds": null,
                    "threshold_sol": null,
                    "threshold_count": 3,
                    "evaluation_window_seconds": 600,
                    "action": "rollback_now"
                }]
            },
            "section_order": [
                "current_state",
                "preflight_checks",
                "bounded_activation_candidate",
                "post_change_checks",
                "rollback_triggers",
                "rollback_procedure",
                "not_authorized_disclaimer"
            ],
            "sections": [{
                "key": "current_state",
                "title": "Current State",
                "status": "informational",
                "summary": "sample",
                "steps": ["step"],
                "blockers": [],
                "warnings": [],
                "suggested_commands": []
            }]
        })
    }
}
