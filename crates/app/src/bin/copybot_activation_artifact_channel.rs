#![recursion_limit = "256"]
#![allow(unused_attributes)]

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[allow(dead_code)]
#[path = "copybot_activation_artifact_archive.rs"]
mod activation_artifact_archive;
#[allow(dead_code)]
#[path = "copybot_activation_artifact_bundle.rs"]
mod activation_artifact_bundle;
#[allow(dead_code)]
#[path = "copybot_activation_artifact_manifest.rs"]
mod activation_artifact_manifest;

const USAGE: &str = "usage: copybot_activation_artifact_channel --archive-dir <path> --channel-dir <path> [--channel-name <name>] [--json] (--report | --verify | --promote --generation <id-or-rfc3339> [--manifest-path <path>] [--bundle-path <path>] [--allow-overwrite])";
pub(crate) const DEFAULT_CHANNEL_NAME: &str = "current_review";
const CHANNEL_VERSION: &str = "1";

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
    pub(crate) archive_dir: PathBuf,
    pub(crate) channel_dir: PathBuf,
    pub(crate) channel_name: String,
    pub(crate) json: bool,
    pub(crate) mode: Mode,
}

#[derive(Debug, Clone)]
pub(crate) enum Mode {
    Report,
    Verify,
    Promote {
        generation_selector: String,
        manifest_path: Option<PathBuf>,
        bundle_path: Option<PathBuf>,
        allow_overwrite: bool,
    },
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ArtifactChannelVerdict {
    ArtifactChannelOk,
    ArtifactChannelMissingTarget,
    ArtifactChannelInconsistent,
    ArtifactChannelPromoted,
    ArtifactChannelRefusedWithoutOverwrite,
    ArtifactChannelInvalidMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChannelGenerationIdentity {
    decision_packet_generated_at: DateTime<Utc>,
    prod_config_fingerprint_sha256: String,
    non_prod_config_fingerprint_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActivationArtifactChannelMetadata {
    channel_version: String,
    channel_name: String,
    source_archive_dir: String,
    selected_generation_id: String,
    generation_identity: ChannelGenerationIdentity,
    decision_packet_paths: Vec<String>,
    runbook_json_paths: Vec<String>,
    runbook_markdown_paths: Vec<String>,
    latest_packet_verdict: Option<String>,
    latest_runbook_verdict: Option<String>,
    manifest_path: Option<String>,
    bundle_path: Option<String>,
    promoted_at: DateTime<Utc>,
    build_version: String,
    git_commit: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ArtifactChannelReport {
    pub(crate) mode: String,
    pub(crate) verdict: ArtifactChannelVerdict,
    pub(crate) reason: String,
    pub(crate) archive_dir: String,
    pub(crate) channel_dir: String,
    pub(crate) channel_name: String,
    pub(crate) channel_metadata_path: String,
    pub(crate) channel_metadata_exists: bool,
    pub(crate) promotion_happened: bool,
    pub(crate) overwrite_used: bool,
    pub(crate) selected_generation_id: Option<String>,
    pub(crate) decision_packet_generated_at: Option<DateTime<Utc>>,
    pub(crate) prod_config_fingerprint_sha256: Option<String>,
    pub(crate) non_prod_config_fingerprint_sha256: Option<String>,
    pub(crate) decision_packet_paths: Vec<String>,
    pub(crate) runbook_json_paths: Vec<String>,
    pub(crate) runbook_markdown_paths: Vec<String>,
    pub(crate) manifest_path: Option<String>,
    pub(crate) bundle_path: Option<String>,
    pub(crate) latest_packet_verdict: Option<String>,
    pub(crate) latest_runbook_verdict: Option<String>,
    pub(crate) promoted_at: Option<DateTime<Utc>>,
    pub(crate) manifest_verification_verdict: Option<String>,
    pub(crate) bundle_verification_verdict: Option<String>,
    pub(crate) archive_invalid_artifact_count: usize,
    pub(crate) invalid_artifacts: Vec<activation_artifact_archive::InvalidArtifact>,
    pub(crate) missing_paths: Vec<String>,
    pub(crate) inconsistencies: Vec<String>,
    pub(crate) channel_metadata_only: bool,
    pub(crate) execution_untouched: bool,
    pub(crate) activation_authorized: bool,
    pub(crate) not_authorized_summary: String,
}

#[derive(Debug, Clone)]
struct SelectedGeneration {
    generation_id: String,
    summary: activation_artifact_archive::ArchiveArtifactGenerationSummary,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut archive_dir: Option<PathBuf> = None;
    let mut channel_dir: Option<PathBuf> = None;
    let mut channel_name = DEFAULT_CHANNEL_NAME.to_string();
    let mut json = false;
    let mut report = false;
    let mut verify = false;
    let mut promote = false;
    let mut generation_selector: Option<String> = None;
    let mut manifest_path: Option<PathBuf> = None;
    let mut bundle_path: Option<PathBuf> = None;
    let mut allow_overwrite = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--archive-dir" => {
                archive_dir = Some(PathBuf::from(parse_string_arg(
                    "--archive-dir",
                    args.next(),
                )?))
            }
            "--channel-dir" => {
                channel_dir = Some(PathBuf::from(parse_string_arg(
                    "--channel-dir",
                    args.next(),
                )?))
            }
            "--channel-name" => {
                channel_name = parse_channel_name(parse_string_arg("--channel-name", args.next())?)?
            }
            "--json" => json = true,
            "--report" => report = true,
            "--verify" => verify = true,
            "--promote" => promote = true,
            "--generation" => {
                generation_selector = Some(parse_string_arg("--generation", args.next())?)
            }
            "--manifest-path" => {
                manifest_path = Some(PathBuf::from(parse_string_arg(
                    "--manifest-path",
                    args.next(),
                )?))
            }
            "--bundle-path" => {
                bundle_path = Some(PathBuf::from(parse_string_arg(
                    "--bundle-path",
                    args.next(),
                )?))
            }
            "--allow-overwrite" => allow_overwrite = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    let mode_count = [report, verify, promote]
        .into_iter()
        .filter(|value| *value)
        .count();
    if mode_count != 1 {
        bail!("exactly one of --report, --verify, or --promote is required");
    }

    let mode = if report {
        Mode::Report
    } else if verify {
        Mode::Verify
    } else {
        Mode::Promote {
            generation_selector: generation_selector
                .ok_or_else(|| anyhow!("missing required --generation for --promote"))?,
            manifest_path,
            bundle_path,
            allow_overwrite,
        }
    };

    Ok(Some(Config {
        archive_dir: archive_dir.ok_or_else(|| anyhow!("missing required --archive-dir"))?,
        channel_dir: channel_dir.ok_or_else(|| anyhow!("missing required --channel-dir"))?,
        channel_name,
        json,
        mode,
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

fn parse_channel_name(value: String) -> Result<String> {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
    {
        Ok(value)
    } else {
        bail!("channel name may only contain ascii alphanumeric characters, '-' or '_'");
    }
}

fn run(config: Config) -> Result<String> {
    let report = match &config.mode {
        Mode::Report => inspect_channel(&config, false),
        Mode::Verify => inspect_channel(&config, true),
        Mode::Promote {
            generation_selector,
            manifest_path,
            bundle_path,
            allow_overwrite,
        } => promote_channel(
            &config,
            generation_selector,
            manifest_path.as_deref(),
            bundle_path.as_deref(),
            *allow_overwrite,
        ),
    }?;

    if config.json {
        serde_json::to_string_pretty(&report)
            .context("failed serializing activation artifact channel json")
    } else {
        Ok(render_human(&report))
    }
}

pub(crate) fn inspect_channel(
    config: &Config,
    verify_references: bool,
) -> Result<ArtifactChannelReport> {
    let metadata_path = channel_metadata_path(&config.channel_dir, &config.channel_name);
    if !metadata_path.exists() {
        return Ok(ArtifactChannelReport {
            mode: if verify_references { "verify" } else { "report" }.to_string(),
            verdict: ArtifactChannelVerdict::ArtifactChannelMissingTarget,
            reason: format!(
                "no channel metadata found at {}; no current review generation is selected",
                metadata_path.display()
            ),
            archive_dir: config.archive_dir.display().to_string(),
            channel_dir: config.channel_dir.display().to_string(),
            channel_name: config.channel_name.clone(),
            channel_metadata_path: metadata_path.display().to_string(),
            channel_metadata_exists: false,
            promotion_happened: false,
            overwrite_used: false,
            selected_generation_id: None,
            decision_packet_generated_at: None,
            prod_config_fingerprint_sha256: None,
            non_prod_config_fingerprint_sha256: None,
            decision_packet_paths: Vec::new(),
            runbook_json_paths: Vec::new(),
            runbook_markdown_paths: Vec::new(),
            manifest_path: None,
            bundle_path: None,
            latest_packet_verdict: None,
            latest_runbook_verdict: None,
            promoted_at: None,
            manifest_verification_verdict: None,
            bundle_verification_verdict: None,
            archive_invalid_artifact_count: 0,
            invalid_artifacts: Vec::new(),
            missing_paths: Vec::new(),
            inconsistencies: Vec::new(),
            channel_metadata_only: true,
            execution_untouched: true,
            activation_authorized: false,
            not_authorized_summary:
                "Artifact channel management only tracks published review artifacts. It does not authorize activation and does not override the Stage 3 production gate.".to_string(),
        });
    }

    let raw = match fs::read_to_string(&metadata_path) {
        Ok(raw) => raw,
        Err(error) => {
            return Ok(invalid_metadata_report(
                config,
                &metadata_path,
                format!("failed reading channel metadata: {error}"),
            ))
        }
    };
    let metadata: ActivationArtifactChannelMetadata = match serde_json::from_str(&raw) {
        Ok(metadata) => metadata,
        Err(error) => {
            return Ok(invalid_metadata_report(
                config,
                &metadata_path,
                format!("failed parsing channel metadata json: {error}"),
            ))
        }
    };
    if metadata.channel_version != CHANNEL_VERSION {
        return Ok(invalid_metadata_report(
            config,
            &metadata_path,
            format!(
                "unsupported channel metadata version `{}`; expected `{CHANNEL_VERSION}`",
                metadata.channel_version
            ),
        ));
    }
    if metadata.channel_name != config.channel_name {
        return Ok(invalid_metadata_report(
            config,
            &metadata_path,
            format!(
                "channel metadata name `{}` does not match requested channel `{}`",
                metadata.channel_name, config.channel_name
            ),
        ));
    }

    let inventory = activation_artifact_archive::archive_inventory(&config.archive_dir)?;
    let archive_invalid_artifact_count = inventory.invalid_artifacts.len();
    let mut missing_paths = Vec::new();
    let mut inconsistencies = Vec::new();
    let mut manifest_verification_verdict = None;
    let mut bundle_verification_verdict = None;

    let selected = inventory
        .generation_summaries
        .iter()
        .find(|summary| generation_id_from_summary(summary) == metadata.selected_generation_id)
        .cloned();

    if archive_invalid_artifact_count > 0 {
        inconsistencies.push(format!(
            "archive inventory contains {} invalid artifact(s); fix archive state before trusting channel metadata",
            archive_invalid_artifact_count
        ));
    }

    let selected = match selected {
        Some(selected) => selected,
        None => {
            return Ok(build_channel_report(
                if verify_references { "verify" } else { "report" },
                ArtifactChannelVerdict::ArtifactChannelMissingTarget,
                format!(
                    "channel points to generation `{}` but that generation is not present in archive {}",
                    metadata.selected_generation_id,
                    config.archive_dir.display()
                ),
                config,
                &metadata_path,
                Some(&metadata),
                archive_invalid_artifact_count,
                inventory.invalid_artifacts,
                missing_paths,
                inconsistencies,
                manifest_verification_verdict,
                bundle_verification_verdict,
                false,
                false,
            ));
        }
    };

    let expected_generation_id = generation_id_from_summary(&selected);
    if expected_generation_id != metadata.selected_generation_id {
        inconsistencies.push(format!(
            "channel metadata selected_generation_id `{}` does not match archive generation id `{expected_generation_id}`",
            metadata.selected_generation_id
        ));
    }
    if metadata.generation_identity.decision_packet_generated_at
        != selected.decision_packet_generated_at
    {
        inconsistencies.push(
            "channel metadata decision_packet_generated_at does not match archive generation"
                .to_string(),
        );
    }
    if metadata.generation_identity.prod_config_fingerprint_sha256
        != selected.prod_config_fingerprint_sha256
    {
        inconsistencies.push(
            "channel metadata prod_config_fingerprint_sha256 does not match archive generation"
                .to_string(),
        );
    }
    if metadata
        .generation_identity
        .non_prod_config_fingerprint_sha256
        != selected.non_prod_config_fingerprint_sha256
    {
        inconsistencies.push(
            "channel metadata non_prod_config_fingerprint_sha256 does not match archive generation"
                .to_string(),
        );
    }

    compare_path_sets(
        "decision_packet_paths",
        &metadata.decision_packet_paths,
        &selected.decision_packet_paths,
        &mut inconsistencies,
    );
    compare_path_sets(
        "runbook_json_paths",
        &metadata.runbook_json_paths,
        &selected.runbook_json_paths,
        &mut inconsistencies,
    );
    compare_path_sets(
        "runbook_markdown_paths",
        &metadata.runbook_markdown_paths,
        &selected.runbook_markdown_paths,
        &mut inconsistencies,
    );

    collect_missing_paths(
        &config.archive_dir,
        &metadata.decision_packet_paths,
        &mut missing_paths,
    );
    collect_missing_paths(
        &config.archive_dir,
        &metadata.runbook_json_paths,
        &mut missing_paths,
    );
    collect_missing_paths(
        &config.archive_dir,
        &metadata.runbook_markdown_paths,
        &mut missing_paths,
    );

    if let Some(path) = &metadata.manifest_path {
        if let Some(resolved_path) =
            resolve_stored_reference_path(path, "manifest_path", &mut inconsistencies)
        {
            if !resolved_path.exists() {
                missing_paths.push(resolved_path.display().to_string());
            } else if verify_references {
                let report = activation_artifact_manifest::verify_manifest(
                    &config.archive_dir,
                    &resolved_path,
                )?;
                manifest_verification_verdict = Some(serialize_enum(&report.verdict));
                if report.verdict
                    != activation_artifact_manifest::ArtifactManifestVerdict::ArtifactManifestVerified
                {
                    inconsistencies.push(format!(
                        "channel manifest reference failed verification: {}",
                        report.reason
                    ));
                }
            }
        }
    }

    if let Some(path) = &metadata.bundle_path {
        if let Some(resolved_path) =
            resolve_stored_reference_path(path, "bundle_path", &mut inconsistencies)
        {
            if !resolved_path.exists() {
                missing_paths.push(resolved_path.display().to_string());
            } else if verify_references {
                let report = activation_artifact_bundle::verify_bundle(&resolved_path)?;
                bundle_verification_verdict = Some(serialize_enum(&report.verdict));
                if report.verdict
                    != activation_artifact_bundle::ArtifactBundleVerdict::ArtifactBundleVerified
                {
                    inconsistencies.push(format!(
                        "channel bundle reference failed verification: {}",
                        report.reason
                    ));
                } else if report.generation_id.as_deref()
                    != Some(metadata.selected_generation_id.as_str())
                {
                    inconsistencies.push(format!(
                        "channel bundle reference points to generation {:?}, expected {}",
                        report.generation_id, metadata.selected_generation_id
                    ));
                }
            }
        }
    }

    let verdict = if !missing_paths.is_empty() {
        ArtifactChannelVerdict::ArtifactChannelMissingTarget
    } else if !inconsistencies.is_empty() {
        ArtifactChannelVerdict::ArtifactChannelInconsistent
    } else {
        ArtifactChannelVerdict::ArtifactChannelOk
    };
    let reason = match verdict {
        ArtifactChannelVerdict::ArtifactChannelOk => format!(
            "channel `{}` points to a consistent archive generation and all referenced artifacts resolve",
            config.channel_name
        ),
        ArtifactChannelVerdict::ArtifactChannelMissingTarget => {
            "channel metadata exists, but one or more referenced generation artifacts are missing"
                .to_string()
        }
        ArtifactChannelVerdict::ArtifactChannelInconsistent => {
            "channel metadata exists, but its lineage or optional references are inconsistent"
                .to_string()
        }
        ArtifactChannelVerdict::ArtifactChannelPromoted
        | ArtifactChannelVerdict::ArtifactChannelRefusedWithoutOverwrite
        | ArtifactChannelVerdict::ArtifactChannelInvalidMetadata => unreachable!(),
    };

    Ok(build_channel_report(
        if verify_references {
            "verify"
        } else {
            "report"
        },
        verdict,
        reason,
        config,
        &metadata_path,
        Some(&metadata),
        archive_invalid_artifact_count,
        inventory.invalid_artifacts,
        missing_paths,
        inconsistencies,
        manifest_verification_verdict,
        bundle_verification_verdict,
        false,
        false,
    ))
}

pub(crate) fn promote_channel(
    config: &Config,
    generation_selector: &str,
    manifest_path: Option<&Path>,
    bundle_path: Option<&Path>,
    allow_overwrite: bool,
) -> Result<ArtifactChannelReport> {
    let metadata_path = channel_metadata_path(&config.channel_dir, &config.channel_name);
    if metadata_path.exists() && !allow_overwrite {
        return Ok(ArtifactChannelReport {
            mode: "promote".to_string(),
            verdict: ArtifactChannelVerdict::ArtifactChannelRefusedWithoutOverwrite,
            reason: format!(
                "channel metadata {} already exists; rerun with --allow-overwrite to replace the current pointer",
                metadata_path.display()
            ),
            archive_dir: config.archive_dir.display().to_string(),
            channel_dir: config.channel_dir.display().to_string(),
            channel_name: config.channel_name.clone(),
            channel_metadata_path: metadata_path.display().to_string(),
            channel_metadata_exists: true,
            promotion_happened: false,
            overwrite_used: false,
            selected_generation_id: None,
            decision_packet_generated_at: None,
            prod_config_fingerprint_sha256: None,
            non_prod_config_fingerprint_sha256: None,
            decision_packet_paths: Vec::new(),
            runbook_json_paths: Vec::new(),
            runbook_markdown_paths: Vec::new(),
            manifest_path: manifest_path.map(|path| path.display().to_string()),
            bundle_path: bundle_path.map(|path| path.display().to_string()),
            latest_packet_verdict: None,
            latest_runbook_verdict: None,
            promoted_at: None,
            manifest_verification_verdict: None,
            bundle_verification_verdict: None,
            archive_invalid_artifact_count: 0,
            invalid_artifacts: Vec::new(),
            missing_paths: Vec::new(),
            inconsistencies: Vec::new(),
            channel_metadata_only: false,
            execution_untouched: true,
            activation_authorized: false,
            not_authorized_summary:
                "Artifact channel management only tracks published review artifacts. It does not authorize activation and does not override the Stage 3 production gate.".to_string(),
        });
    }

    let inventory = activation_artifact_archive::archive_inventory(&config.archive_dir)?;
    if !inventory.invalid_artifacts.is_empty() {
        return Ok(ArtifactChannelReport {
            mode: "promote".to_string(),
            verdict: ArtifactChannelVerdict::ArtifactChannelInconsistent,
            reason: format!(
                "archive contains {} invalid artifact(s); fix archive state before promoting a channel pointer",
                inventory.invalid_artifacts.len()
            ),
            archive_dir: config.archive_dir.display().to_string(),
            channel_dir: config.channel_dir.display().to_string(),
            channel_name: config.channel_name.clone(),
            channel_metadata_path: metadata_path.display().to_string(),
            channel_metadata_exists: metadata_path.exists(),
            promotion_happened: false,
            overwrite_used: false,
            selected_generation_id: None,
            decision_packet_generated_at: None,
            prod_config_fingerprint_sha256: None,
            non_prod_config_fingerprint_sha256: None,
            decision_packet_paths: Vec::new(),
            runbook_json_paths: Vec::new(),
            runbook_markdown_paths: Vec::new(),
            manifest_path: manifest_path.map(|path| path.display().to_string()),
            bundle_path: bundle_path.map(|path| path.display().to_string()),
            latest_packet_verdict: None,
            latest_runbook_verdict: None,
            promoted_at: None,
            manifest_verification_verdict: None,
            bundle_verification_verdict: None,
            archive_invalid_artifact_count: inventory.invalid_artifacts.len(),
            invalid_artifacts: inventory.invalid_artifacts,
            missing_paths: Vec::new(),
            inconsistencies: Vec::new(),
            channel_metadata_only: false,
            execution_untouched: true,
            activation_authorized: false,
            not_authorized_summary:
                "Artifact channel management only tracks published review artifacts. It does not authorize activation and does not override the Stage 3 production gate.".to_string(),
        });
    }

    let selected = match resolve_generation(&inventory, generation_selector)? {
        Some(selected) => selected,
        None => {
            return Ok(ArtifactChannelReport {
                mode: "promote".to_string(),
                verdict: ArtifactChannelVerdict::ArtifactChannelMissingTarget,
                reason: format!(
                    "no packet-backed archive generation matched selector `{generation_selector}`"
                ),
                archive_dir: config.archive_dir.display().to_string(),
                channel_dir: config.channel_dir.display().to_string(),
                channel_name: config.channel_name.clone(),
                channel_metadata_path: metadata_path.display().to_string(),
                channel_metadata_exists: metadata_path.exists(),
                promotion_happened: false,
                overwrite_used: false,
                selected_generation_id: None,
                decision_packet_generated_at: None,
                prod_config_fingerprint_sha256: None,
                non_prod_config_fingerprint_sha256: None,
                decision_packet_paths: Vec::new(),
                runbook_json_paths: Vec::new(),
                runbook_markdown_paths: Vec::new(),
                manifest_path: manifest_path.map(|path| path.display().to_string()),
                bundle_path: bundle_path.map(|path| path.display().to_string()),
                latest_packet_verdict: None,
                latest_runbook_verdict: None,
                promoted_at: None,
                manifest_verification_verdict: None,
                bundle_verification_verdict: None,
                archive_invalid_artifact_count: 0,
                invalid_artifacts: Vec::new(),
                missing_paths: Vec::new(),
                inconsistencies: Vec::new(),
                channel_metadata_only: false,
                execution_untouched: true,
                activation_authorized: false,
                not_authorized_summary:
                    "Artifact channel management only tracks published review artifacts. It does not authorize activation and does not override the Stage 3 production gate.".to_string(),
            });
        }
    };

    let mut inconsistencies = Vec::new();
    let mut manifest_verification_verdict = None;
    let mut bundle_verification_verdict = None;
    let resolved_manifest_path = if let Some(path) = manifest_path {
        match resolve_reference_path_for_promote(path, "manifest_path") {
            Ok(path) => Some(path),
            Err(error) => {
                inconsistencies.push(format!(
                    "manifest reference cannot be canonicalized deterministically: {error:#}"
                ));
                None
            }
        }
    } else {
        None
    };
    let resolved_bundle_path = if let Some(path) = bundle_path {
        match resolve_reference_path_for_promote(path, "bundle_path") {
            Ok(path) => Some(path),
            Err(error) => {
                inconsistencies.push(format!(
                    "bundle reference cannot be canonicalized deterministically: {error:#}"
                ));
                None
            }
        }
    } else {
        None
    };

    if selected.summary.decision_packet_paths.is_empty()
        || selected.summary.runbook_json_paths.is_empty()
    {
        inconsistencies.push(
            "selected generation is not a complete packet-backed review generation".to_string(),
        );
    }

    if let Some(path) = resolved_manifest_path.as_deref() {
        let report = activation_artifact_manifest::verify_manifest(&config.archive_dir, path)?;
        manifest_verification_verdict = Some(serialize_enum(&report.verdict));
        if report.verdict
            != activation_artifact_manifest::ArtifactManifestVerdict::ArtifactManifestVerified
        {
            inconsistencies.push(format!(
                "manifest reference failed verification before promote: {}",
                report.reason
            ));
        }
    }

    if let Some(path) = resolved_bundle_path.as_deref() {
        let report = activation_artifact_bundle::verify_bundle(path)?;
        bundle_verification_verdict = Some(serialize_enum(&report.verdict));
        if report.verdict
            != activation_artifact_bundle::ArtifactBundleVerdict::ArtifactBundleVerified
        {
            inconsistencies.push(format!(
                "bundle reference failed verification before promote: {}",
                report.reason
            ));
        } else if report.generation_id.as_deref() != Some(selected.generation_id.as_str()) {
            inconsistencies.push(format!(
                "bundle reference points to generation {:?}, expected {}",
                report.generation_id, selected.generation_id
            ));
        }
    }

    if !inconsistencies.is_empty() {
        return Ok(ArtifactChannelReport {
            mode: "promote".to_string(),
            verdict: ArtifactChannelVerdict::ArtifactChannelInconsistent,
            reason:
                "channel promote is blocked because the selected generation references are inconsistent"
                    .to_string(),
            archive_dir: config.archive_dir.display().to_string(),
            channel_dir: config.channel_dir.display().to_string(),
            channel_name: config.channel_name.clone(),
            channel_metadata_path: metadata_path.display().to_string(),
            channel_metadata_exists: metadata_path.exists(),
            promotion_happened: false,
            overwrite_used: false,
            selected_generation_id: Some(selected.generation_id.clone()),
            decision_packet_generated_at: Some(selected.summary.decision_packet_generated_at),
            prod_config_fingerprint_sha256: Some(
                selected.summary.prod_config_fingerprint_sha256.clone(),
            ),
            non_prod_config_fingerprint_sha256: Some(
                selected.summary.non_prod_config_fingerprint_sha256.clone(),
            ),
            decision_packet_paths: selected.summary.decision_packet_paths.clone(),
            runbook_json_paths: selected.summary.runbook_json_paths.clone(),
            runbook_markdown_paths: selected.summary.runbook_markdown_paths.clone(),
            manifest_path: resolved_manifest_path
                .as_ref()
                .map(|path| path.display().to_string()),
            bundle_path: resolved_bundle_path
                .as_ref()
                .map(|path| path.display().to_string()),
            latest_packet_verdict: selected.summary.latest_packet_verdict.clone(),
            latest_runbook_verdict: selected.summary.latest_runbook_verdict.clone(),
            promoted_at: None,
            manifest_verification_verdict,
            bundle_verification_verdict,
            archive_invalid_artifact_count: 0,
            invalid_artifacts: Vec::new(),
            missing_paths: Vec::new(),
            inconsistencies,
            channel_metadata_only: false,
            execution_untouched: true,
            activation_authorized: false,
            not_authorized_summary:
                "Artifact channel management only tracks published review artifacts. It does not authorize activation and does not override the Stage 3 production gate.".to_string(),
        });
    }

    let metadata = ActivationArtifactChannelMetadata {
        channel_version: CHANNEL_VERSION.to_string(),
        channel_name: config.channel_name.clone(),
        source_archive_dir: config.archive_dir.display().to_string(),
        selected_generation_id: selected.generation_id.clone(),
        generation_identity: ChannelGenerationIdentity {
            decision_packet_generated_at: selected.summary.decision_packet_generated_at,
            prod_config_fingerprint_sha256: selected.summary.prod_config_fingerprint_sha256.clone(),
            non_prod_config_fingerprint_sha256: selected
                .summary
                .non_prod_config_fingerprint_sha256
                .clone(),
        },
        decision_packet_paths: selected.summary.decision_packet_paths.clone(),
        runbook_json_paths: selected.summary.runbook_json_paths.clone(),
        runbook_markdown_paths: selected.summary.runbook_markdown_paths.clone(),
        latest_packet_verdict: selected.summary.latest_packet_verdict.clone(),
        latest_runbook_verdict: selected.summary.latest_runbook_verdict.clone(),
        manifest_path: resolved_manifest_path
            .as_ref()
            .map(|path| path.display().to_string()),
        bundle_path: resolved_bundle_path
            .as_ref()
            .map(|path| path.display().to_string()),
        promoted_at: Utc::now(),
        build_version: env!("CARGO_PKG_VERSION").to_string(),
        git_commit: resolve_git_commit(),
    };

    fs::create_dir_all(&config.channel_dir).with_context(|| {
        format!(
            "failed creating channel dir {}",
            config.channel_dir.display()
        )
    })?;
    let stage_path = config.channel_dir.join(format!(
        ".{}.staging_{}",
        config.channel_name,
        unique_suffix()
    ));
    fs::write(&stage_path, serde_json::to_string_pretty(&metadata)?).with_context(|| {
        format!(
            "failed writing staged channel metadata {}",
            stage_path.display()
        )
    })?;
    if metadata_path.exists() && allow_overwrite {
        fs::remove_file(&metadata_path).with_context(|| {
            format!(
                "failed removing existing channel metadata {}",
                metadata_path.display()
            )
        })?;
    }
    fs::rename(&stage_path, &metadata_path).with_context(|| {
        format!(
            "failed promoting staged channel metadata {} to {}",
            stage_path.display(),
            metadata_path.display()
        )
    })?;

    Ok(ArtifactChannelReport {
        mode: "promote".to_string(),
        verdict: ArtifactChannelVerdict::ArtifactChannelPromoted,
        reason: format!(
            "channel `{}` now points to generation {}",
            config.channel_name, selected.generation_id
        ),
        archive_dir: config.archive_dir.display().to_string(),
        channel_dir: config.channel_dir.display().to_string(),
        channel_name: config.channel_name.clone(),
        channel_metadata_path: metadata_path.display().to_string(),
        channel_metadata_exists: true,
        promotion_happened: true,
        overwrite_used: allow_overwrite,
        selected_generation_id: Some(selected.generation_id),
        decision_packet_generated_at: Some(selected.summary.decision_packet_generated_at),
        prod_config_fingerprint_sha256: Some(
            selected.summary.prod_config_fingerprint_sha256.clone(),
        ),
        non_prod_config_fingerprint_sha256: Some(
            selected.summary.non_prod_config_fingerprint_sha256.clone(),
        ),
        decision_packet_paths: selected.summary.decision_packet_paths.clone(),
        runbook_json_paths: selected.summary.runbook_json_paths.clone(),
        runbook_markdown_paths: selected.summary.runbook_markdown_paths.clone(),
        manifest_path: metadata.manifest_path.clone(),
        bundle_path: metadata.bundle_path.clone(),
        latest_packet_verdict: selected.summary.latest_packet_verdict.clone(),
        latest_runbook_verdict: selected.summary.latest_runbook_verdict.clone(),
        promoted_at: Some(metadata.promoted_at),
        manifest_verification_verdict,
        bundle_verification_verdict,
        archive_invalid_artifact_count: 0,
        invalid_artifacts: Vec::new(),
        missing_paths: Vec::new(),
        inconsistencies: Vec::new(),
        channel_metadata_only: false,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary:
            "Artifact channel management only tracks published review artifacts. It does not authorize activation and does not override the Stage 3 production gate.".to_string(),
    })
}

fn invalid_metadata_report(
    config: &Config,
    metadata_path: &Path,
    reason: String,
) -> ArtifactChannelReport {
    ArtifactChannelReport {
        mode: match config.mode {
            Mode::Report => "report",
            Mode::Verify => "verify",
            Mode::Promote { .. } => "promote",
        }
        .to_string(),
        verdict: ArtifactChannelVerdict::ArtifactChannelInvalidMetadata,
        reason,
        archive_dir: config.archive_dir.display().to_string(),
        channel_dir: config.channel_dir.display().to_string(),
        channel_name: config.channel_name.clone(),
        channel_metadata_path: metadata_path.display().to_string(),
        channel_metadata_exists: metadata_path.exists(),
        promotion_happened: false,
        overwrite_used: false,
        selected_generation_id: None,
        decision_packet_generated_at: None,
        prod_config_fingerprint_sha256: None,
        non_prod_config_fingerprint_sha256: None,
        decision_packet_paths: Vec::new(),
        runbook_json_paths: Vec::new(),
        runbook_markdown_paths: Vec::new(),
        manifest_path: None,
        bundle_path: None,
        latest_packet_verdict: None,
        latest_runbook_verdict: None,
        promoted_at: None,
        manifest_verification_verdict: None,
        bundle_verification_verdict: None,
        archive_invalid_artifact_count: 0,
        invalid_artifacts: Vec::new(),
        missing_paths: Vec::new(),
        inconsistencies: Vec::new(),
        channel_metadata_only: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary:
            "Artifact channel management only tracks published review artifacts. It does not authorize activation and does not override the Stage 3 production gate.".to_string(),
    }
}

fn build_channel_report(
    mode: &str,
    verdict: ArtifactChannelVerdict,
    reason: String,
    config: &Config,
    metadata_path: &Path,
    metadata: Option<&ActivationArtifactChannelMetadata>,
    archive_invalid_artifact_count: usize,
    invalid_artifacts: Vec<activation_artifact_archive::InvalidArtifact>,
    missing_paths: Vec<String>,
    inconsistencies: Vec<String>,
    manifest_verification_verdict: Option<String>,
    bundle_verification_verdict: Option<String>,
    promotion_happened: bool,
    overwrite_used: bool,
) -> ArtifactChannelReport {
    ArtifactChannelReport {
        mode: mode.to_string(),
        verdict,
        reason,
        archive_dir: config.archive_dir.display().to_string(),
        channel_dir: config.channel_dir.display().to_string(),
        channel_name: config.channel_name.clone(),
        channel_metadata_path: metadata_path.display().to_string(),
        channel_metadata_exists: metadata_path.exists(),
        promotion_happened,
        overwrite_used,
        selected_generation_id: metadata.map(|value| value.selected_generation_id.clone()),
        decision_packet_generated_at: metadata
            .map(|value| value.generation_identity.decision_packet_generated_at),
        prod_config_fingerprint_sha256: metadata
            .map(|value| value.generation_identity.prod_config_fingerprint_sha256.clone()),
        non_prod_config_fingerprint_sha256: metadata
            .map(|value| value.generation_identity.non_prod_config_fingerprint_sha256.clone()),
        decision_packet_paths: metadata
            .map(|value| value.decision_packet_paths.clone())
            .unwrap_or_default(),
        runbook_json_paths: metadata
            .map(|value| value.runbook_json_paths.clone())
            .unwrap_or_default(),
        runbook_markdown_paths: metadata
            .map(|value| value.runbook_markdown_paths.clone())
            .unwrap_or_default(),
        manifest_path: metadata.and_then(|value| value.manifest_path.clone()),
        bundle_path: metadata.and_then(|value| value.bundle_path.clone()),
        latest_packet_verdict: metadata.and_then(|value| value.latest_packet_verdict.clone()),
        latest_runbook_verdict: metadata.and_then(|value| value.latest_runbook_verdict.clone()),
        promoted_at: metadata.map(|value| value.promoted_at),
        manifest_verification_verdict,
        bundle_verification_verdict,
        archive_invalid_artifact_count,
        invalid_artifacts,
        missing_paths,
        inconsistencies,
        channel_metadata_only: !promotion_happened,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary:
            "Artifact channel management only tracks published review artifacts. It does not authorize activation and does not override the Stage 3 production gate.".to_string(),
    }
}

fn compare_path_sets(
    label: &str,
    stored: &[String],
    actual: &[String],
    inconsistencies: &mut Vec<String>,
) {
    let mut left = stored.to_vec();
    let mut right = actual.to_vec();
    left.sort();
    right.sort();
    if left != right {
        inconsistencies.push(format!(
            "channel metadata {label} does not match archive generation paths"
        ));
    }
}

fn collect_missing_paths(root: &Path, relative_paths: &[String], missing_paths: &mut Vec<String>) {
    for relative in relative_paths {
        if !root.join(relative).exists() {
            missing_paths.push(root.join(relative).display().to_string());
        }
    }
}

fn resolve_reference_path_for_promote(path: &Path, label: &str) -> Result<PathBuf> {
    fs::canonicalize(path)
        .with_context(|| format!("failed canonicalizing {label} {}", path.display()))
}

fn resolve_stored_reference_path(
    path: &str,
    label: &str,
    inconsistencies: &mut Vec<String>,
) -> Option<PathBuf> {
    let resolved = PathBuf::from(path);
    if !resolved.is_absolute() {
        inconsistencies.push(format!(
            "channel metadata {label} `{path}` is not absolute; relative references are unsupported"
        ));
        return None;
    }
    Some(resolved)
}

fn resolve_generation(
    inventory: &activation_artifact_archive::ArchiveInventory,
    selector: &str,
) -> Result<Option<SelectedGeneration>> {
    let candidates = inventory
        .generation_summaries
        .iter()
        .filter(|summary| !summary.decision_packet_paths.is_empty())
        .cloned()
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Ok(None);
    }

    if let Some(exact) = candidates
        .iter()
        .find(|summary| generation_id_from_summary(summary) == selector)
    {
        return Ok(Some(SelectedGeneration {
            summary: exact.clone(),
            generation_id: generation_id_from_summary(exact),
        }));
    }

    let selector_ts = match DateTime::parse_from_rfc3339(selector) {
        Ok(ts) => ts.with_timezone(&Utc),
        Err(_) => return Ok(None),
    };
    let matching = candidates
        .into_iter()
        .filter(|summary| summary.decision_packet_generated_at == selector_ts)
        .collect::<Vec<_>>();
    if matching.len() > 1 {
        bail!(
            "generation selector `{selector}` is ambiguous; use full generation id <rfc3339>|<prod_fp>|<non_prod_fp>"
        );
    }
    Ok(matching
        .into_iter()
        .next()
        .map(|summary| SelectedGeneration {
            generation_id: generation_id_from_summary(&summary),
            summary,
        }))
}

fn generation_id_from_summary(
    summary: &activation_artifact_archive::ArchiveArtifactGenerationSummary,
) -> String {
    format!(
        "{}|{}|{}",
        summary.decision_packet_generated_at.to_rfc3339(),
        summary.prod_config_fingerprint_sha256,
        summary.non_prod_config_fingerprint_sha256
    )
}

fn channel_metadata_path(channel_dir: &Path, channel_name: &str) -> PathBuf {
    channel_dir.join(format!("{channel_name}.json"))
}

fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos()
}

fn resolve_git_commit() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn serialize_enum<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value)
        .unwrap_or_default()
        .trim_matches('"')
        .to_string()
}

fn render_human(report: &ArtifactChannelReport) -> String {
    [
        "event=copybot_activation_artifact_channel".to_string(),
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("archive_dir={}", report.archive_dir),
        format!("channel_dir={}", report.channel_dir),
        format!("channel_name={}", report.channel_name),
        format!("channel_metadata_path={}", report.channel_metadata_path),
        format!("channel_metadata_exists={}", report.channel_metadata_exists),
        format!("promotion_happened={}", report.promotion_happened),
        format!("overwrite_used={}", report.overwrite_used),
        format!(
            "selected_generation_id={}",
            report
                .selected_generation_id
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "decision_packet_generated_at={}",
            report
                .decision_packet_generated_at
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "prod_config_fingerprint_sha256={}",
            report
                .prod_config_fingerprint_sha256
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "non_prod_config_fingerprint_sha256={}",
            report
                .non_prod_config_fingerprint_sha256
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "decision_packet_paths={}",
            report.decision_packet_paths.join(" | ")
        ),
        format!(
            "runbook_json_paths={}",
            report.runbook_json_paths.join(" | ")
        ),
        format!(
            "runbook_markdown_paths={}",
            report.runbook_markdown_paths.join(" | ")
        ),
        format!(
            "manifest_path={}",
            report
                .manifest_path
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "bundle_path={}",
            report
                .bundle_path
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "latest_packet_verdict={}",
            report
                .latest_packet_verdict
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "latest_runbook_verdict={}",
            report
                .latest_runbook_verdict
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "promoted_at={}",
            report
                .promoted_at
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "manifest_verification_verdict={}",
            report
                .manifest_verification_verdict
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "bundle_verification_verdict={}",
            report
                .bundle_verification_verdict
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "archive_invalid_artifact_count={}",
            report.archive_invalid_artifact_count
        ),
        format!("missing_paths={}", report.missing_paths.join(" | ")),
        format!("inconsistencies={}", report.inconsistencies.join(" | ")),
        format!("channel_metadata_only={}", report.channel_metadata_only),
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
    use std::sync::{Mutex, MutexGuard, OnceLock};

    #[test]
    fn report_mode_over_empty_channel_dir() {
        let archive_dir = temp_dir("activation_channel_empty_archive");
        let channel_dir = temp_dir("activation_channel_empty_channel");
        let report = inspect_channel(
            &sample_config(archive_dir.clone(), channel_dir.clone(), Mode::Report),
            false,
        )
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactChannelVerdict::ArtifactChannelMissingTarget
        );
        assert!(!report.channel_metadata_exists);
        assert_eq!(report.archive_dir, archive_dir.display().to_string());
        assert_eq!(report.channel_dir, channel_dir.display().to_string());
    }

    #[test]
    fn promote_mode_writes_valid_channel_metadata_for_existing_generation() {
        let archive_dir = temp_dir("activation_channel_promote_archive");
        let channel_dir = temp_dir("activation_channel_promote_channel");
        let generation_id = write_sample_generation(
            &archive_dir,
            "2026-03-26T12:00:00Z",
            "prod_fp_a",
            "non_prod_fp_a",
        );
        let generation_selector =
            exact_generation_id("2026-03-26T12:00:00Z", "prod_fp_a", "non_prod_fp_a");

        let report = promote_channel(
            &sample_config(
                archive_dir.clone(),
                channel_dir.clone(),
                Mode::Promote {
                    generation_selector: generation_selector.clone(),
                    manifest_path: None,
                    bundle_path: None,
                    allow_overwrite: false,
                },
            ),
            &generation_selector,
            None,
            None,
            false,
        )
        .expect("promote");

        assert_eq!(
            report.verdict,
            ArtifactChannelVerdict::ArtifactChannelPromoted
        );
        assert!(channel_metadata_path(&channel_dir, DEFAULT_CHANNEL_NAME).exists());
        assert_eq!(
            report.selected_generation_id.as_deref(),
            Some(generation_id.as_str())
        );
        assert!(report.execution_untouched);
        assert!(!report.activation_authorized);
    }

    #[test]
    fn verify_mode_passes_for_consistent_channel() {
        let archive_dir = temp_dir("activation_channel_verify_archive");
        let channel_dir = temp_dir("activation_channel_verify_channel");
        write_sample_generation(
            &archive_dir,
            "2026-03-26T12:00:00Z",
            "prod_fp_a",
            "non_prod_fp_a",
        );
        let generation_selector =
            exact_generation_id("2026-03-26T12:00:00Z", "prod_fp_a", "non_prod_fp_a");
        promote_channel(
            &sample_config(
                archive_dir.clone(),
                channel_dir.clone(),
                Mode::Promote {
                    generation_selector: generation_selector.clone(),
                    manifest_path: None,
                    bundle_path: None,
                    allow_overwrite: false,
                },
            ),
            &generation_selector,
            None,
            None,
            false,
        )
        .expect("promote");

        let report = inspect_channel(&sample_config(archive_dir, channel_dir, Mode::Verify), true)
            .expect("verify");

        assert_eq!(report.verdict, ArtifactChannelVerdict::ArtifactChannelOk);
        assert!(report.missing_paths.is_empty());
        assert!(report.inconsistencies.is_empty());
    }

    #[test]
    fn verify_mode_fails_for_missing_target_generation() {
        let archive_dir = temp_dir("activation_channel_missing_target_archive");
        let channel_dir = temp_dir("activation_channel_missing_target_channel");
        write_sample_generation(
            &archive_dir,
            "2026-03-26T12:00:00Z",
            "prod_fp_a",
            "non_prod_fp_a",
        );
        let generation_selector =
            exact_generation_id("2026-03-26T12:00:00Z", "prod_fp_a", "non_prod_fp_a");
        promote_channel(
            &sample_config(
                archive_dir.clone(),
                channel_dir.clone(),
                Mode::Promote {
                    generation_selector: generation_selector.clone(),
                    manifest_path: None,
                    bundle_path: None,
                    allow_overwrite: false,
                },
            ),
            &generation_selector,
            None,
            None,
            false,
        )
        .expect("promote");
        fs::remove_dir_all(generation_dir_path(
            &archive_dir,
            "2026-03-26T12:00:00Z",
            "prod_fp_a",
            "non_prod_fp_a",
        ))
        .expect("remove generation dir");

        let report = inspect_channel(&sample_config(archive_dir, channel_dir, Mode::Verify), true)
            .expect("verify");

        assert_eq!(
            report.verdict,
            ArtifactChannelVerdict::ArtifactChannelMissingTarget
        );
    }

    #[test]
    fn invalid_channel_metadata_is_reported_explicitly() {
        let archive_dir = temp_dir("activation_channel_invalid_metadata_archive");
        let channel_dir = temp_dir("activation_channel_invalid_metadata_channel");
        fs::create_dir_all(&channel_dir).expect("create channel dir");
        fs::write(
            channel_metadata_path(&channel_dir, DEFAULT_CHANNEL_NAME),
            "{broken",
        )
        .expect("write invalid metadata");

        let report = inspect_channel(&sample_config(archive_dir, channel_dir, Mode::Verify), true)
            .expect("verify");

        assert_eq!(
            report.verdict,
            ArtifactChannelVerdict::ArtifactChannelInvalidMetadata
        );
    }

    #[test]
    fn promote_mode_does_not_overwrite_existing_channel_metadata_silently() {
        let archive_dir = temp_dir("activation_channel_overwrite_archive");
        let channel_dir = temp_dir("activation_channel_overwrite_channel");
        write_sample_generation(
            &archive_dir,
            "2026-03-26T12:00:00Z",
            "prod_fp_a",
            "non_prod_fp_a",
        );
        let generation_selector =
            exact_generation_id("2026-03-26T12:00:00Z", "prod_fp_a", "non_prod_fp_a");
        promote_channel(
            &sample_config(
                archive_dir.clone(),
                channel_dir.clone(),
                Mode::Promote {
                    generation_selector: generation_selector.clone(),
                    manifest_path: None,
                    bundle_path: None,
                    allow_overwrite: false,
                },
            ),
            &generation_selector,
            None,
            None,
            false,
        )
        .expect("first promote");

        let report = promote_channel(
            &sample_config(
                archive_dir,
                channel_dir,
                Mode::Promote {
                    generation_selector: generation_selector.clone(),
                    manifest_path: None,
                    bundle_path: None,
                    allow_overwrite: false,
                },
            ),
            &generation_selector,
            None,
            None,
            false,
        )
        .expect("second promote");

        assert_eq!(
            report.verdict,
            ArtifactChannelVerdict::ArtifactChannelRefusedWithoutOverwrite
        );
        assert!(!report.promotion_happened);
    }

    #[test]
    fn promote_with_relative_manifest_and_bundle_paths_stores_absolute_metadata_and_survives_cwd_change(
    ) {
        let archive_dir = temp_dir("activation_channel_relative_archive");
        let channel_dir = temp_dir("activation_channel_relative_channel");
        let generation_id = write_sample_generation(
            &archive_dir,
            "2026-03-26T12:00:00Z",
            "prod_fp_a",
            "non_prod_fp_a",
        );
        let generation_selector =
            exact_generation_id("2026-03-26T12:00:00Z", "prod_fp_a", "non_prod_fp_a");
        let promote_cwd = temp_dir("activation_channel_relative_promote_cwd");
        let verify_cwd = temp_dir("activation_channel_relative_verify_cwd");
        let manifest_path = promote_cwd.join("refs").join("archive_manifest.json");
        let bundle_path = promote_cwd.join("refs").join("portable_bundle");

        activation_artifact_manifest::generate_manifest(&archive_dir, &manifest_path)
            .expect("generate manifest");
        activation_artifact_bundle::export_bundle(&archive_dir, &generation_selector, &bundle_path)
            .expect("export bundle");

        {
            let _cwd = WorkingDirectoryGuard::change_to(&promote_cwd);
            let report = promote_channel(
                &sample_config(
                    archive_dir.clone(),
                    channel_dir.clone(),
                    Mode::Promote {
                        generation_selector: generation_selector.clone(),
                        manifest_path: Some(PathBuf::from("refs/archive_manifest.json")),
                        bundle_path: Some(PathBuf::from("refs/portable_bundle")),
                        allow_overwrite: false,
                    },
                ),
                &generation_selector,
                Some(Path::new("refs/archive_manifest.json")),
                Some(Path::new("refs/portable_bundle")),
                false,
            )
            .expect("promote");

            assert_eq!(
                report.verdict,
                ArtifactChannelVerdict::ArtifactChannelPromoted
            );
            assert_eq!(
                report.selected_generation_id.as_deref(),
                Some(generation_id.as_str())
            );
        }

        let metadata = read_channel_metadata(&channel_dir);
        let expected_manifest_path =
            fs::canonicalize(&manifest_path).expect("canonical manifest path");
        let expected_bundle_path = fs::canonicalize(&bundle_path).expect("canonical bundle path");
        assert_eq!(
            metadata.manifest_path.as_deref(),
            Some(expected_manifest_path.to_string_lossy().as_ref())
        );
        assert_eq!(
            metadata.bundle_path.as_deref(),
            Some(expected_bundle_path.to_string_lossy().as_ref())
        );
        assert!(Path::new(metadata.manifest_path.as_deref().expect("manifest path")).is_absolute());
        assert!(Path::new(metadata.bundle_path.as_deref().expect("bundle path")).is_absolute());

        {
            let _cwd = WorkingDirectoryGuard::change_to(&verify_cwd);
            let report = inspect_channel(
                &sample_config(archive_dir.clone(), channel_dir.clone(), Mode::Verify),
                true,
            )
            .expect("verify");
            assert_eq!(report.verdict, ArtifactChannelVerdict::ArtifactChannelOk);
            assert_eq!(
                report.manifest_verification_verdict.as_deref(),
                Some("artifact_manifest_verified")
            );
            assert_eq!(
                report.bundle_verification_verdict.as_deref(),
                Some("artifact_bundle_verified")
            );
        }

        let generation_dir = generation_dir_path(
            &archive_dir,
            "2026-03-26T12:00:00Z",
            "prod_fp_a",
            "non_prod_fp_a",
        );
        assert!(generation_dir.exists());
        assert!(generation_dir.join("decision_packet.json").exists());
        assert!(generation_dir.join("activation_runbook.json").exists());
        assert!(generation_dir.join("activation_runbook.md").exists());
    }

    #[test]
    fn promote_with_absolute_manifest_and_bundle_paths_still_works() {
        let archive_dir = temp_dir("activation_channel_absolute_archive");
        let channel_dir = temp_dir("activation_channel_absolute_channel");
        write_sample_generation(
            &archive_dir,
            "2026-03-26T12:00:00Z",
            "prod_fp_a",
            "non_prod_fp_a",
        );
        let generation_selector =
            exact_generation_id("2026-03-26T12:00:00Z", "prod_fp_a", "non_prod_fp_a");
        let refs_dir = temp_dir("activation_channel_absolute_refs");
        let manifest_path = refs_dir.join("archive_manifest.json");
        let bundle_path = refs_dir.join("portable_bundle");

        activation_artifact_manifest::generate_manifest(&archive_dir, &manifest_path)
            .expect("generate manifest");
        activation_artifact_bundle::export_bundle(&archive_dir, &generation_selector, &bundle_path)
            .expect("export bundle");

        let report = promote_channel(
            &sample_config(
                archive_dir.clone(),
                channel_dir.clone(),
                Mode::Promote {
                    generation_selector: generation_selector.clone(),
                    manifest_path: Some(manifest_path.clone()),
                    bundle_path: Some(bundle_path.clone()),
                    allow_overwrite: false,
                },
            ),
            &generation_selector,
            Some(&manifest_path),
            Some(&bundle_path),
            false,
        )
        .expect("promote");

        assert_eq!(
            report.verdict,
            ArtifactChannelVerdict::ArtifactChannelPromoted
        );

        let verify_report =
            inspect_channel(&sample_config(archive_dir, channel_dir, Mode::Verify), true)
                .expect("verify");
        assert_eq!(
            verify_report.verdict,
            ArtifactChannelVerdict::ArtifactChannelOk
        );
    }

    fn sample_config(archive_dir: PathBuf, channel_dir: PathBuf, mode: Mode) -> Config {
        Config {
            archive_dir,
            channel_dir,
            channel_name: DEFAULT_CHANNEL_NAME.to_string(),
            json: false,
            mode,
        }
    }

    fn write_sample_generation(
        archive_dir: &Path,
        generated_at: &str,
        prod_fingerprint: &str,
        non_prod_fingerprint: &str,
    ) -> String {
        let generation_dir = generation_dir_path(
            archive_dir,
            generated_at,
            prod_fingerprint,
            non_prod_fingerprint,
        );
        fs::create_dir_all(&generation_dir).expect("create generation dir");
        write_json(
            &generation_dir.join("decision_packet.json"),
            &sample_packet(
                generated_at,
                prod_fingerprint,
                non_prod_fingerprint,
                "decision_packet_discussion_ready_but_not_authorized",
            ),
        );
        write_json(
            &generation_dir.join("activation_runbook.json"),
            &sample_runbook(
                generated_at,
                generated_at,
                prod_fingerprint,
                non_prod_fingerprint,
                "runbook_discussion_ready_but_not_authorized",
            ),
        );
        fs::write(
            generation_dir.join("activation_runbook.md"),
            "# Tiny-Live Activation Runbook\n\nsample",
        )
        .expect("write runbook markdown");
        exact_generation_id(generated_at, prod_fingerprint, non_prod_fingerprint)
    }

    fn generation_dir_path(
        archive_dir: &Path,
        generated_at: &str,
        prod_fingerprint: &str,
        non_prod_fingerprint: &str,
    ) -> PathBuf {
        archive_dir.join(format!(
            "{}__{}__{}",
            generated_at.replace(':', "-"),
            prod_fingerprint,
            non_prod_fingerprint
        ))
    }

    fn exact_generation_id(
        generated_at: &str,
        prod_fingerprint: &str,
        non_prod_fingerprint: &str,
    ) -> String {
        format!(
            "{}|{}|{}",
            DateTime::parse_from_rfc3339(generated_at)
                .expect("ts")
                .with_timezone(&Utc)
                .to_rfc3339(),
            prod_fingerprint,
            non_prod_fingerprint
        )
    }

    fn sample_packet(
        generated_at: &str,
        prod_fingerprint: &str,
        non_prod_fingerprint: &str,
        verdict: &str,
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
            "verdict": verdict,
            "reason": "sample packet",
            "blockers": [],
            "warnings": ["planning-only"],
            "checklist_verdict": "activation_checklist_discussion_ready_but_not_authorized",
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

    fn sample_runbook(
        generated_at: &str,
        decision_packet_generated_at: &str,
        prod_fingerprint: &str,
        non_prod_fingerprint: &str,
        verdict: &str,
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
            "verdict": verdict,
            "reason": "runbook sample",
            "blockers": [],
            "warnings": ["planning-only"],
            "not_authorized_disclaimer": "planning-only",
            "decision_packet_version": "1",
            "decision_packet_generated_at": decision_packet_generated_at,
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
            "section_order": [
                "current_state",
                "preflight_checks",
                "stop_here_conditions",
                "bounded_activation_candidate",
                "post_change_checks",
                "rollback_triggers",
                "rollback_procedure",
                "not_authorized_disclaimer"
            ]
        })
    }

    fn write_json(path: &Path, value: &serde_json::Value) {
        fs::write(
            path,
            serde_json::to_string_pretty(value).expect("serialize"),
        )
        .expect("write");
    }

    fn read_channel_metadata(channel_dir: &Path) -> ActivationArtifactChannelMetadata {
        let raw = fs::read_to_string(channel_metadata_path(channel_dir, DEFAULT_CHANNEL_NAME))
            .expect("read channel metadata");
        serde_json::from_str(&raw).expect("parse channel metadata")
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

    fn cwd_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    struct WorkingDirectoryGuard {
        previous: PathBuf,
        _lock: MutexGuard<'static, ()>,
    }

    impl WorkingDirectoryGuard {
        fn change_to(path: &Path) -> Self {
            let lock = cwd_lock().lock().expect("lock cwd");
            let previous = env::current_dir().expect("current dir");
            env::set_current_dir(path).expect("set current dir");
            Self {
                previous,
                _lock: lock,
            }
        }
    }

    impl Drop for WorkingDirectoryGuard {
        fn drop(&mut self) {
            env::set_current_dir(&self.previous).expect("restore current dir");
        }
    }
}
