#![recursion_limit = "256"]
#![allow(unused_attributes)]

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
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
#[allow(dead_code)]
#[path = "copybot_activation_decision_packet.rs"]
mod activation_decision_packet;
#[allow(dead_code)]
#[path = "copybot_activation_runbook.rs"]
mod activation_runbook;

const USAGE: &str = "usage: copybot_activation_artifact_publish --config <prod-path> --non-prod-config <path> --archive-dir <path> [--json] [--manifest-output <path>] [--bundle-output-dir <path>] [--note <text>] [--now <rfc3339>] [--stage3-limit <count>] [--stage3-recent-horizon-seconds <seconds>] [--rehearsal-limit <count>] [--rehearsal-recent-horizon-seconds <seconds>] [--min-recent-acceptable-rehearsals <count>] [--non-prod-limit <count>] [--non-prod-dress-recent-horizon-seconds <seconds>] [--non-prod-activation-recent-horizon-seconds <seconds>] [--non-prod-min-recent-green-dress <count>] [--non-prod-min-recent-green-activation <count>]";
#[allow(dead_code)]
pub(crate) const DEFAULT_REHEARSAL_HISTORY_LIMIT: usize =
    activation_decision_packet::activation_checklist_report::DEFAULT_REHEARSAL_HISTORY_LIMIT;
#[allow(dead_code)]
pub(crate) const DEFAULT_REHEARSAL_RECENT_HORIZON_SECONDS: u64 =
    activation_decision_packet::activation_checklist_report::DEFAULT_REHEARSAL_RECENT_HORIZON_SECONDS;
#[allow(dead_code)]
pub(crate) const DEFAULT_MIN_RECENT_ACCEPTABLE_REHEARSALS: usize =
    activation_decision_packet::activation_checklist_report::DEFAULT_MIN_RECENT_ACCEPTABLE_REHEARSALS;
#[allow(dead_code)]
pub(crate) const DEFAULT_NON_PROD_HISTORY_LIMIT: usize =
    activation_decision_packet::activation_checklist_report::DEFAULT_NON_PROD_HISTORY_LIMIT;
#[allow(dead_code)]
pub(crate) const DEFAULT_NON_PROD_DRESS_RECENT_HORIZON_SECONDS: u64 =
    activation_decision_packet::activation_checklist_report::DEFAULT_NON_PROD_DRESS_RECENT_HORIZON_SECONDS;
#[allow(dead_code)]
pub(crate) const DEFAULT_NON_PROD_ACTIVATION_RECENT_HORIZON_SECONDS: u64 =
    activation_decision_packet::activation_checklist_report::DEFAULT_NON_PROD_ACTIVATION_RECENT_HORIZON_SECONDS;
#[allow(dead_code)]
pub(crate) const DEFAULT_NON_PROD_MIN_RECENT_GREEN_DRESS: usize =
    activation_decision_packet::activation_checklist_report::DEFAULT_NON_PROD_MIN_RECENT_GREEN_DRESS;
#[allow(dead_code)]
pub(crate) const DEFAULT_NON_PROD_MIN_RECENT_GREEN_ACTIVATION: usize =
    activation_decision_packet::activation_checklist_report::DEFAULT_NON_PROD_MIN_RECENT_GREEN_ACTIVATION;

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed building tokio runtime for activation artifact publish")?;
    let output = runtime.block_on(run(config))?;
    println!("{output}");
    Ok(())
}

#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub(crate) config_path: PathBuf,
    pub(crate) non_prod_config_path: PathBuf,
    pub(crate) archive_dir: PathBuf,
    pub(crate) json: bool,
    pub(crate) manifest_output_path: Option<PathBuf>,
    pub(crate) bundle_output_dir: Option<PathBuf>,
    pub(crate) note: Option<String>,
    pub(crate) now: DateTime<Utc>,
    pub(crate) stage3_limit: usize,
    pub(crate) stage3_recent_horizon_seconds: Option<u64>,
    pub(crate) rehearsal_limit: usize,
    pub(crate) rehearsal_recent_horizon_seconds: u64,
    pub(crate) min_recent_acceptable_rehearsals: usize,
    pub(crate) non_prod_limit: usize,
    pub(crate) non_prod_dress_recent_horizon_seconds: u64,
    pub(crate) non_prod_activation_recent_horizon_seconds: u64,
    pub(crate) non_prod_min_recent_green_dress: usize,
    pub(crate) non_prod_min_recent_green_activation: usize,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ArtifactPublishVerdict {
    ArtifactPublishSucceeded,
    ArtifactPublishBlockedByChecklist,
    ArtifactPublishBlockedByInvalidArchiveState,
    ArtifactPublishPartialManifestSkipped,
    ArtifactPublishFailed,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ArtifactPublishReport {
    pub(crate) mode: String,
    pub(crate) verdict: ArtifactPublishVerdict,
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
    pub(crate) archive_invalid_artifact_count: usize,
    pub(crate) manifest_generated: bool,
    pub(crate) bundle_exported: bool,
    pub(crate) manifest_output_cleaned: bool,
    pub(crate) bundle_output_cleaned: bool,
    pub(crate) read_only_source_config: bool,
    pub(crate) activation_authorized: bool,
    pub(crate) not_authorized_summary: String,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedGeneration {
    pub(crate) generated_at: DateTime<Utc>,
    pub(crate) prod_config_fingerprint_sha256: String,
    pub(crate) non_prod_config_fingerprint_sha256: String,
    pub(crate) decision_packet_verdict: String,
    pub(crate) checklist_verdict: String,
    pub(crate) packet_json: String,
    pub(crate) runbook_json: String,
    pub(crate) runbook_markdown: String,
}

#[derive(Debug, Clone)]
struct GenerationPaths {
    generation_id: String,
    generation_directory_name: String,
    generation_directory: PathBuf,
    packet_path: PathBuf,
    runbook_json_path: PathBuf,
    runbook_markdown_path: PathBuf,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut config_path: Option<PathBuf> = None;
    let mut non_prod_config_path: Option<PathBuf> = None;
    let mut archive_dir: Option<PathBuf> = None;
    let mut json = false;
    let mut manifest_output_path: Option<PathBuf> = None;
    let mut bundle_output_dir: Option<PathBuf> = None;
    let mut note: Option<String> = None;
    let mut now: Option<DateTime<Utc>> = None;
    let mut stage3_limit = copybot_discovery::wallet_freshness_audit::DEFAULT_HISTORY_CAPTURE_LIMIT;
    let mut stage3_recent_horizon_seconds: Option<u64> = None;
    let mut rehearsal_limit =
        activation_decision_packet::activation_checklist_report::DEFAULT_REHEARSAL_HISTORY_LIMIT;
    let mut rehearsal_recent_horizon_seconds = activation_decision_packet::activation_checklist_report::DEFAULT_REHEARSAL_RECENT_HORIZON_SECONDS;
    let mut min_recent_acceptable_rehearsals =
        activation_decision_packet::activation_checklist_report::DEFAULT_MIN_RECENT_ACCEPTABLE_REHEARSALS;
    let mut non_prod_limit =
        activation_decision_packet::activation_checklist_report::DEFAULT_NON_PROD_HISTORY_LIMIT;
    let mut non_prod_dress_recent_horizon_seconds = activation_decision_packet::activation_checklist_report::DEFAULT_NON_PROD_DRESS_RECENT_HORIZON_SECONDS;
    let mut non_prod_activation_recent_horizon_seconds =
        activation_decision_packet::activation_checklist_report::DEFAULT_NON_PROD_ACTIVATION_RECENT_HORIZON_SECONDS;
    let mut non_prod_min_recent_green_dress =
        activation_decision_packet::activation_checklist_report::DEFAULT_NON_PROD_MIN_RECENT_GREEN_DRESS;
    let mut non_prod_min_recent_green_activation =
        activation_decision_packet::activation_checklist_report::DEFAULT_NON_PROD_MIN_RECENT_GREEN_ACTIVATION;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(PathBuf::from(parse_string_arg("--config", args.next())?))
            }
            "--non-prod-config" => {
                non_prod_config_path = Some(PathBuf::from(parse_string_arg(
                    "--non-prod-config",
                    args.next(),
                )?))
            }
            "--archive-dir" => {
                archive_dir = Some(PathBuf::from(parse_string_arg(
                    "--archive-dir",
                    args.next(),
                )?))
            }
            "--json" => json = true,
            "--manifest-output" => {
                manifest_output_path = Some(PathBuf::from(parse_string_arg(
                    "--manifest-output",
                    args.next(),
                )?))
            }
            "--bundle-output-dir" => {
                bundle_output_dir = Some(PathBuf::from(parse_string_arg(
                    "--bundle-output-dir",
                    args.next(),
                )?))
            }
            "--note" => note = Some(parse_string_arg("--note", args.next())?),
            "--now" => now = Some(parse_ts_arg("--now", args.next())?),
            "--stage3-limit" => stage3_limit = parse_usize_arg("--stage3-limit", args.next())?,
            "--stage3-recent-horizon-seconds" => {
                stage3_recent_horizon_seconds = Some(parse_u64_arg(
                    "--stage3-recent-horizon-seconds",
                    args.next(),
                )?)
            }
            "--rehearsal-limit" => {
                rehearsal_limit = parse_usize_arg("--rehearsal-limit", args.next())?
            }
            "--rehearsal-recent-horizon-seconds" => {
                rehearsal_recent_horizon_seconds =
                    parse_u64_arg("--rehearsal-recent-horizon-seconds", args.next())?
            }
            "--min-recent-acceptable-rehearsals" => {
                min_recent_acceptable_rehearsals =
                    parse_usize_arg("--min-recent-acceptable-rehearsals", args.next())?
            }
            "--non-prod-limit" => {
                non_prod_limit = parse_usize_arg("--non-prod-limit", args.next())?
            }
            "--non-prod-dress-recent-horizon-seconds" => {
                non_prod_dress_recent_horizon_seconds =
                    parse_u64_arg("--non-prod-dress-recent-horizon-seconds", args.next())?
            }
            "--non-prod-activation-recent-horizon-seconds" => {
                non_prod_activation_recent_horizon_seconds =
                    parse_u64_arg("--non-prod-activation-recent-horizon-seconds", args.next())?
            }
            "--non-prod-min-recent-green-dress" => {
                non_prod_min_recent_green_dress =
                    parse_usize_arg("--non-prod-min-recent-green-dress", args.next())?
            }
            "--non-prod-min-recent-green-activation" => {
                non_prod_min_recent_green_activation =
                    parse_usize_arg("--non-prod-min-recent-green-activation", args.next())?
            }
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Some(Config {
        config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
        non_prod_config_path: non_prod_config_path
            .ok_or_else(|| anyhow!("missing required --non-prod-config"))?,
        archive_dir: archive_dir.ok_or_else(|| anyhow!("missing required --archive-dir"))?,
        json,
        manifest_output_path,
        bundle_output_dir,
        note,
        now: now.unwrap_or_else(Utc::now),
        stage3_limit: stage3_limit.max(1),
        stage3_recent_horizon_seconds,
        rehearsal_limit: rehearsal_limit.max(1),
        rehearsal_recent_horizon_seconds: rehearsal_recent_horizon_seconds.max(1),
        min_recent_acceptable_rehearsals: min_recent_acceptable_rehearsals.max(1),
        non_prod_limit: non_prod_limit.max(1),
        non_prod_dress_recent_horizon_seconds: non_prod_dress_recent_horizon_seconds.max(1),
        non_prod_activation_recent_horizon_seconds: non_prod_activation_recent_horizon_seconds
            .max(1),
        non_prod_min_recent_green_dress: non_prod_min_recent_green_dress.max(1),
        non_prod_min_recent_green_activation: non_prod_min_recent_green_activation.max(1),
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

fn parse_ts_arg(flag: &str, value: Option<String>) -> Result<DateTime<Utc>> {
    let raw = parse_string_arg(flag, value)?;
    DateTime::parse_from_rfc3339(&raw)
        .map(|ts| ts.with_timezone(&Utc))
        .with_context(|| format!("invalid {flag} rfc3339 timestamp: {raw}"))
}

fn parse_u64_arg(flag: &str, value: Option<String>) -> Result<u64> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<u64>()
        .with_context(|| format!("invalid {flag} u64 value: {raw}"))
}

fn parse_usize_arg(flag: &str, value: Option<String>) -> Result<usize> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<usize>()
        .with_context(|| format!("invalid {flag} usize value: {raw}"))
}

async fn run(config: Config) -> Result<String> {
    let report = evaluate_publish(&config).await?;
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_human(&report))
    }
}

pub(crate) async fn evaluate_publish(config: &Config) -> Result<ArtifactPublishReport> {
    let packet = activation_decision_packet::evaluate_activation_decision_packet(
        &activation_decision_packet::Config {
            config_path: config.config_path.clone(),
            non_prod_config_path: config.non_prod_config_path.clone(),
            json: false,
            output_path: None,
            note: config.note.clone(),
            now: config.now,
            stage3_limit: config.stage3_limit,
            stage3_recent_horizon_seconds: config.stage3_recent_horizon_seconds,
            rehearsal_limit: config.rehearsal_limit,
            rehearsal_recent_horizon_seconds: config.rehearsal_recent_horizon_seconds,
            min_recent_acceptable_rehearsals: config.min_recent_acceptable_rehearsals,
            non_prod_limit: config.non_prod_limit,
            non_prod_dress_recent_horizon_seconds: config.non_prod_dress_recent_horizon_seconds,
            non_prod_activation_recent_horizon_seconds: config
                .non_prod_activation_recent_horizon_seconds,
            non_prod_min_recent_green_dress: config.non_prod_min_recent_green_dress,
            non_prod_min_recent_green_activation: config.non_prod_min_recent_green_activation,
        },
    )
    .await?;

    let decision_packet_verdict = serialize_enum(&packet.verdict);
    let checklist_verdict = packet.checklist_verdict.clone();
    if packet.verdict
        != activation_decision_packet::ActivationDecisionPacketVerdict::DecisionPacketDiscussionReadyButNotAuthorized
    {
        return Ok(ArtifactPublishReport {
            mode: "artifact_publish".to_string(),
            verdict: ArtifactPublishVerdict::ArtifactPublishBlockedByChecklist,
            reason: format!(
                "artifact publish is blocked because activation decision packet is not discussion-ready: {}",
                packet.reason
            ),
            config_path: config.config_path.display().to_string(),
            non_prod_config_path: config.non_prod_config_path.display().to_string(),
            archive_dir: config.archive_dir.display().to_string(),
            generation_id: None,
            generation_directory: None,
            packet_path: None,
            runbook_json_path: None,
            runbook_markdown_path: None,
            manifest_path: None,
            bundle_path: None,
            decision_packet_verdict: Some(decision_packet_verdict),
            checklist_verdict: Some(checklist_verdict),
            archive_invalid_artifact_count: 0,
            manifest_generated: false,
            bundle_exported: false,
            manifest_output_cleaned: false,
            bundle_output_cleaned: false,
            read_only_source_config: true,
            activation_authorized: false,
            not_authorized_summary:
                "Artifact publish only prepares review artifacts and does not authorize activation or override the Stage 3 production gate.".to_string(),
        });
    }

    let archive_invalid_artifact_count = if config.archive_dir.exists() {
        activation_artifact_archive::archive_inventory(&config.archive_dir)?
            .invalid_artifacts
            .len()
    } else {
        0
    };
    if archive_invalid_artifact_count > 0 {
        return Ok(ArtifactPublishReport {
            mode: "artifact_publish".to_string(),
            verdict: ArtifactPublishVerdict::ArtifactPublishBlockedByInvalidArchiveState,
            reason: format!(
                "artifact publish is blocked because archive dir contains {} invalid artifact(s); fix archive state before writing a new generation",
                archive_invalid_artifact_count
            ),
            config_path: config.config_path.display().to_string(),
            non_prod_config_path: config.non_prod_config_path.display().to_string(),
            archive_dir: config.archive_dir.display().to_string(),
            generation_id: None,
            generation_directory: None,
            packet_path: None,
            runbook_json_path: None,
            runbook_markdown_path: None,
            manifest_path: None,
            bundle_path: None,
            decision_packet_verdict: Some(decision_packet_verdict),
            checklist_verdict: Some(checklist_verdict),
            archive_invalid_artifact_count,
            manifest_generated: false,
            bundle_exported: false,
            manifest_output_cleaned: false,
            bundle_output_cleaned: false,
            read_only_source_config: true,
            activation_authorized: false,
            not_authorized_summary:
                "Artifact publish only prepares review artifacts and does not authorize activation or override the Stage 3 production gate.".to_string(),
        });
    }

    let runbook = activation_runbook::evaluate_activation_runbook(&activation_runbook::Config {
        config_path: config.config_path.clone(),
        non_prod_config_path: config.non_prod_config_path.clone(),
        json: false,
        output_path: None,
        markdown_output_path: None,
        now: config.now,
        stage3_limit: config.stage3_limit,
        stage3_recent_horizon_seconds: config.stage3_recent_horizon_seconds,
        rehearsal_limit: config.rehearsal_limit,
        rehearsal_recent_horizon_seconds: config.rehearsal_recent_horizon_seconds,
        min_recent_acceptable_rehearsals: config.min_recent_acceptable_rehearsals,
        non_prod_limit: config.non_prod_limit,
        non_prod_dress_recent_horizon_seconds: config.non_prod_dress_recent_horizon_seconds,
        non_prod_activation_recent_horizon_seconds: config
            .non_prod_activation_recent_horizon_seconds,
        non_prod_min_recent_green_dress: config.non_prod_min_recent_green_dress,
        non_prod_min_recent_green_activation: config.non_prod_min_recent_green_activation,
    })
    .await?;

    let prepared = PreparedGeneration {
        generated_at: packet.generated_at,
        prod_config_fingerprint_sha256: packet.prod_config_fingerprint.sha256.clone(),
        non_prod_config_fingerprint_sha256: packet.non_prod_config_fingerprint.sha256.clone(),
        decision_packet_verdict: serialize_enum(&packet.verdict),
        checklist_verdict: packet.checklist_verdict.clone(),
        packet_json: serde_json::to_string_pretty(&packet)
            .context("failed serializing decision packet for artifact publish")?,
        runbook_json: serde_json::to_string_pretty(&runbook)
            .context("failed serializing activation runbook for artifact publish")?,
        runbook_markdown: activation_runbook::render_markdown(&runbook),
    };

    publish_prepared_generation(config, prepared)
}

pub(crate) fn publish_prepared_generation(
    config: &Config,
    prepared: PreparedGeneration,
) -> Result<ArtifactPublishReport> {
    fs::create_dir_all(&config.archive_dir).with_context(|| {
        format!(
            "failed creating archive dir {}",
            config.archive_dir.display()
        )
    })?;

    let generation_paths = generation_paths(&config.archive_dir, &prepared);
    if generation_paths.generation_directory.exists() {
        return Ok(build_publish_report(
            ArtifactPublishVerdict::ArtifactPublishFailed,
            format!(
                "refusing to overwrite existing generation directory {}",
                generation_paths.generation_directory.display()
            ),
            config,
            Some(&generation_paths),
            &prepared,
            false,
            false,
            false,
            false,
            None,
            None,
            0,
        ));
    }

    let stage_dir = config.archive_dir.join(format!(
        ".{}__staging_{}",
        generation_paths.generation_directory_name,
        unique_suffix()
    ));
    let write_result = (|| -> Result<()> {
        fs::create_dir_all(&stage_dir)
            .with_context(|| format!("failed creating staging dir {}", stage_dir.display()))?;
        fs::write(
            stage_dir.join("decision_packet.json"),
            &prepared.packet_json,
        )
        .with_context(|| {
            format!(
                "failed writing {}",
                stage_dir.join("decision_packet.json").display()
            )
        })?;
        fs::write(
            stage_dir.join("activation_runbook.json"),
            &prepared.runbook_json,
        )
        .with_context(|| {
            format!(
                "failed writing {}",
                stage_dir.join("activation_runbook.json").display()
            )
        })?;
        fs::write(
            stage_dir.join("activation_runbook.md"),
            &prepared.runbook_markdown,
        )
        .with_context(|| {
            format!(
                "failed writing {}",
                stage_dir.join("activation_runbook.md").display()
            )
        })?;
        fs::rename(&stage_dir, &generation_paths.generation_directory).with_context(|| {
            format!(
                "failed promoting staged generation {} to {}",
                stage_dir.display(),
                generation_paths.generation_directory.display()
            )
        })?;
        Ok(())
    })();
    if let Err(error) = write_result {
        let _ = fs::remove_dir_all(&stage_dir);
        return Ok(build_publish_report(
            ArtifactPublishVerdict::ArtifactPublishFailed,
            format!("{error:#}"),
            config,
            Some(&generation_paths),
            &prepared,
            false,
            false,
            false,
            false,
            None,
            None,
            0,
        ));
    }

    let mut manifest_generated = false;
    let mut manifest_path = None;
    let mut bundle_exported = false;
    let mut bundle_path = None;
    let mut manifest_output_cleaned = false;
    let mut bundle_output_cleaned = false;
    let mut verdict = ArtifactPublishVerdict::ArtifactPublishSucceeded;
    let mut reason = format!(
        "artifact publish created generation {} with packet and runbook exports",
        generation_paths.generation_id
    );

    if let Some(output_path) = &config.manifest_output_path {
        manifest_path = Some(output_path.display().to_string());
        let manifest_existed_before = output_path.exists();
        match activation_artifact_manifest::generate_manifest(&config.archive_dir, output_path) {
            Ok(report) => match report.verdict {
                activation_artifact_manifest::ArtifactManifestVerdict::ArtifactManifestGenerated => {
                    manifest_generated = true;
                }
                _ => {
                    verdict = ArtifactPublishVerdict::ArtifactPublishPartialManifestSkipped;
                    reason = format!(
                        "artifact generation was published, but manifest generation was skipped: {}",
                        report.reason
                    );
                }
            },
            Err(error) => {
                manifest_output_cleaned =
                    cleanup_failed_manifest_output(output_path, manifest_existed_before);
                verdict = ArtifactPublishVerdict::ArtifactPublishPartialManifestSkipped;
                reason = format!(
                    "artifact generation was published, but manifest generation failed operationally: {error:#}; manifest_output_cleaned={manifest_output_cleaned}"
                );
            }
        }
    }

    if let Some(bundle_output_dir) = &config.bundle_output_dir {
        bundle_path = Some(bundle_output_dir.display().to_string());
        let bundle_existed_before = bundle_output_dir.exists();
        match activation_artifact_bundle::export_bundle(
            &config.archive_dir,
            &generation_paths.generation_id,
            bundle_output_dir,
        ) {
            Ok(report) => match report.verdict {
                activation_artifact_bundle::ArtifactBundleVerdict::ArtifactBundleExported => {
                    bundle_exported = true;
                }
                _ => {
                    verdict = ArtifactPublishVerdict::ArtifactPublishFailed;
                    reason = format!(
                        "artifact generation was published, but bundle export failed: {}",
                        report.reason
                    );
                }
            },
            Err(error) => {
                bundle_output_cleaned =
                    cleanup_failed_bundle_output(bundle_output_dir, bundle_existed_before);
                verdict = ArtifactPublishVerdict::ArtifactPublishFailed;
                reason = format!(
                    "artifact generation was published, but bundle export failed operationally: {error:#}; bundle_output_cleaned={bundle_output_cleaned}"
                );
            }
        }
    }

    Ok(build_publish_report(
        verdict,
        reason,
        config,
        Some(&generation_paths),
        &prepared,
        manifest_generated,
        bundle_exported,
        manifest_output_cleaned,
        bundle_output_cleaned,
        manifest_path,
        bundle_path,
        0,
    ))
}

fn cleanup_failed_manifest_output(path: &Path, existed_before: bool) -> bool {
    if existed_before || !path.exists() {
        return false;
    }
    if path.is_file() {
        fs::remove_file(path).is_ok()
    } else {
        false
    }
}

fn cleanup_failed_bundle_output(path: &Path, existed_before: bool) -> bool {
    if existed_before || !path.exists() {
        return false;
    }
    if path.is_dir() {
        fs::remove_dir_all(path).is_ok()
    } else if path.is_file() {
        fs::remove_file(path).is_ok()
    } else {
        false
    }
}

fn generation_paths(archive_dir: &Path, prepared: &PreparedGeneration) -> GenerationPaths {
    let generation_id = generation_id(prepared);
    let generation_directory_name = generation_directory_name(prepared);
    let generation_directory = archive_dir.join(&generation_directory_name);
    GenerationPaths {
        generation_id,
        generation_directory_name,
        packet_path: generation_directory.join("decision_packet.json"),
        runbook_json_path: generation_directory.join("activation_runbook.json"),
        runbook_markdown_path: generation_directory.join("activation_runbook.md"),
        generation_directory,
    }
}

fn build_publish_report(
    verdict: ArtifactPublishVerdict,
    reason: String,
    config: &Config,
    generation_paths: Option<&GenerationPaths>,
    prepared: &PreparedGeneration,
    manifest_generated: bool,
    bundle_exported: bool,
    manifest_output_cleaned: bool,
    bundle_output_cleaned: bool,
    manifest_path: Option<String>,
    bundle_path: Option<String>,
    archive_invalid_artifact_count: usize,
) -> ArtifactPublishReport {
    ArtifactPublishReport {
        mode: "artifact_publish".to_string(),
        verdict,
        reason,
        config_path: config.config_path.display().to_string(),
        non_prod_config_path: config.non_prod_config_path.display().to_string(),
        archive_dir: config.archive_dir.display().to_string(),
        generation_id: generation_paths.map(|paths| paths.generation_id.clone()),
        generation_directory: generation_paths
            .map(|paths| paths.generation_directory.display().to_string()),
        packet_path: generation_paths.map(|paths| paths.packet_path.display().to_string()),
        runbook_json_path: generation_paths
            .map(|paths| paths.runbook_json_path.display().to_string()),
        runbook_markdown_path: generation_paths
            .map(|paths| paths.runbook_markdown_path.display().to_string()),
        manifest_path,
        bundle_path,
        decision_packet_verdict: Some(prepared.decision_packet_verdict.clone()),
        checklist_verdict: Some(prepared.checklist_verdict.clone()),
        archive_invalid_artifact_count,
        manifest_generated,
        bundle_exported,
        manifest_output_cleaned,
        bundle_output_cleaned,
        read_only_source_config: true,
        activation_authorized: false,
        not_authorized_summary:
            "Artifact publish only writes review artifacts. It does not authorize activation, does not enable execution, and does not override the Stage 3 production gate.".to_string(),
    }
}

fn generation_id(prepared: &PreparedGeneration) -> String {
    format!(
        "{}|{}|{}",
        prepared.generated_at.to_rfc3339(),
        prepared.prod_config_fingerprint_sha256,
        prepared.non_prod_config_fingerprint_sha256
    )
}

fn generation_directory_name(prepared: &PreparedGeneration) -> String {
    format!(
        "{}__{}__{}",
        prepared.generated_at.to_rfc3339().replace(':', "-"),
        prepared.prod_config_fingerprint_sha256,
        prepared.non_prod_config_fingerprint_sha256
    )
}

fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos()
}

fn render_human(report: &ArtifactPublishReport) -> String {
    [
        "event=copybot_activation_artifact_publish".to_string(),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("config_path={}", report.config_path),
        format!("non_prod_config_path={}", report.non_prod_config_path),
        format!("archive_dir={}", report.archive_dir),
        format!(
            "generation_id={}",
            report
                .generation_id
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "generation_directory={}",
            report
                .generation_directory
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "packet_path={}",
            report
                .packet_path
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "runbook_json_path={}",
            report
                .runbook_json_path
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "runbook_markdown_path={}",
            report
                .runbook_markdown_path
                .clone()
                .unwrap_or_else(|| "null".to_string())
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
            "decision_packet_verdict={}",
            report
                .decision_packet_verdict
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "checklist_verdict={}",
            report
                .checklist_verdict
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "archive_invalid_artifact_count={}",
            report.archive_invalid_artifact_count
        ),
        format!("manifest_generated={}", report.manifest_generated),
        format!("bundle_exported={}", report.bundle_exported),
        format!("manifest_output_cleaned={}", report.manifest_output_cleaned),
        format!("bundle_output_cleaned={}", report.bundle_output_cleaned),
        format!("read_only_source_config={}", report.read_only_source_config),
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

    #[test]
    fn successful_publish_creates_expected_generation_layout() {
        let archive_dir = temp_dir("activation_publish_success");
        let prepared = sample_prepared_generation();

        let report =
            publish_prepared_generation(&sample_config(&archive_dir, None, None), prepared.clone())
                .expect("publish");

        assert_eq!(
            report.verdict,
            ArtifactPublishVerdict::ArtifactPublishSucceeded
        );
        assert!(Path::new(report.packet_path.as_ref().expect("packet path")).exists());
        assert!(Path::new(report.runbook_json_path.as_ref().expect("runbook json")).exists());
        assert!(Path::new(report.runbook_markdown_path.as_ref().expect("runbook md")).exists());
        assert_eq!(report.generation_id, Some(generation_id(&prepared)));
    }

    #[test]
    fn existing_generation_target_is_not_silently_overwritten() {
        let archive_dir = temp_dir("activation_publish_existing");
        let config = sample_config(&archive_dir, None, None);
        let prepared = sample_prepared_generation();

        let first = publish_prepared_generation(&config, prepared.clone()).expect("first");
        let second = publish_prepared_generation(&config, prepared).expect("second");

        assert_eq!(
            first.verdict,
            ArtifactPublishVerdict::ArtifactPublishSucceeded
        );
        assert_eq!(
            second.verdict,
            ArtifactPublishVerdict::ArtifactPublishFailed
        );
        assert!(second
            .reason
            .contains("refusing to overwrite existing generation directory"));
    }

    #[test]
    fn optional_manifest_generation_works_and_is_verifiable() {
        let archive_dir = temp_dir("activation_publish_manifest");
        let manifest_path =
            temp_dir("activation_publish_manifest_out").join("archive_manifest.json");
        let config = sample_config(&archive_dir, Some(manifest_path.clone()), None);

        let report =
            publish_prepared_generation(&config, sample_prepared_generation()).expect("publish");
        let verify = activation_artifact_manifest::verify_manifest(&archive_dir, &manifest_path)
            .expect("verify manifest");

        assert!(report.manifest_generated);
        assert_eq!(
            verify.verdict,
            activation_artifact_manifest::ArtifactManifestVerdict::ArtifactManifestVerified
        );
    }

    #[test]
    fn optional_bundle_generation_works_and_is_verifiable() {
        let archive_dir = temp_dir("activation_publish_bundle");
        let bundle_dir = temp_dir("activation_publish_bundle_out").join("bundle");
        let config = sample_config(&archive_dir, None, Some(bundle_dir.clone()));

        let report =
            publish_prepared_generation(&config, sample_prepared_generation()).expect("publish");
        let verify = activation_artifact_bundle::verify_bundle(&bundle_dir).expect("verify bundle");

        assert!(report.bundle_exported);
        assert_eq!(
            verify.verdict,
            activation_artifact_bundle::ArtifactBundleVerdict::ArtifactBundleVerified
        );
    }

    #[test]
    fn invalid_archive_state_blocks_publish() {
        let archive_dir = temp_dir("activation_publish_invalid_archive");
        fs::write(archive_dir.join("broken.json"), "{broken").expect("write broken archive");
        let prepared = sample_prepared_generation();

        let invalid_count = activation_artifact_archive::archive_inventory(&archive_dir)
            .expect("inventory")
            .invalid_artifacts
            .len();

        let report = if invalid_count > 0 {
            ArtifactPublishReport {
                mode: "artifact_publish".to_string(),
                verdict: ArtifactPublishVerdict::ArtifactPublishBlockedByInvalidArchiveState,
                reason: format!(
                    "artifact publish is blocked because archive dir contains {} invalid artifact(s); fix archive state before writing a new generation",
                    invalid_count
                ),
                config_path: "prod".to_string(),
                non_prod_config_path: "non-prod".to_string(),
                archive_dir: archive_dir.display().to_string(),
                generation_id: None,
                generation_directory: None,
                packet_path: None,
                runbook_json_path: None,
                runbook_markdown_path: None,
                manifest_path: None,
                bundle_path: None,
                decision_packet_verdict: Some(prepared.decision_packet_verdict),
                checklist_verdict: Some(prepared.checklist_verdict),
                archive_invalid_artifact_count: invalid_count,
                manifest_generated: false,
                bundle_exported: false,
                manifest_output_cleaned: false,
                bundle_output_cleaned: false,
                read_only_source_config: true,
                activation_authorized: false,
                not_authorized_summary: "planning-only".to_string(),
            }
        } else {
            unreachable!()
        };

        assert_eq!(
            report.verdict,
            ArtifactPublishVerdict::ArtifactPublishBlockedByInvalidArchiveState
        );
    }

    #[test]
    fn manifest_output_failure_after_generation_publish_yields_structured_verdict() {
        let archive_dir = temp_dir("activation_publish_manifest_failure");
        let bad_parent = temp_dir("activation_publish_manifest_failure_parent").join("as_file");
        fs::write(&bad_parent, "not a dir").expect("write blocking parent file");
        let manifest_path = bad_parent.join("archive_manifest.json");
        let config = sample_config(&archive_dir, Some(manifest_path.clone()), None);

        let report =
            publish_prepared_generation(&config, sample_prepared_generation()).expect("publish");

        assert_eq!(
            report.verdict,
            ArtifactPublishVerdict::ArtifactPublishPartialManifestSkipped
        );
        assert!(report
            .reason
            .contains("manifest generation failed operationally"));
        assert_eq!(
            report.manifest_path.as_deref(),
            Some(manifest_path.to_string_lossy().as_ref())
        );
        assert!(Path::new(report.packet_path.as_ref().expect("packet path")).exists());
        assert!(Path::new(
            report
                .generation_directory
                .as_ref()
                .expect("generation directory")
        )
        .exists());
        assert!(!report.manifest_generated);
    }

    #[test]
    fn bundle_output_failure_after_generation_publish_yields_structured_verdict() {
        let archive_dir = temp_dir("activation_publish_bundle_failure");
        let bundle_path = temp_dir("activation_publish_bundle_failure_out").join("bundle_as_file");
        fs::write(&bundle_path, "not a dir").expect("write blocking bundle file");
        let config = sample_config(&archive_dir, None, Some(bundle_path.clone()));

        let report =
            publish_prepared_generation(&config, sample_prepared_generation()).expect("publish");

        assert_eq!(
            report.verdict,
            ArtifactPublishVerdict::ArtifactPublishFailed
        );
        assert!(report.reason.contains("bundle export failed operationally"));
        assert_eq!(
            report.bundle_path.as_deref(),
            Some(bundle_path.to_string_lossy().as_ref())
        );
        assert!(Path::new(report.packet_path.as_ref().expect("packet path")).exists());
        assert!(Path::new(
            report
                .generation_directory
                .as_ref()
                .expect("generation directory")
        )
        .exists());
        assert!(!report.bundle_exported);
    }

    fn sample_config(
        archive_dir: &Path,
        manifest_output_path: Option<PathBuf>,
        bundle_output_dir: Option<PathBuf>,
    ) -> Config {
        Config {
            config_path: PathBuf::from("/etc/solana-copy-bot/live.server.toml"),
            non_prod_config_path: PathBuf::from("/etc/solana-copy-bot/devnet.server.toml"),
            archive_dir: archive_dir.to_path_buf(),
            json: false,
            manifest_output_path,
            bundle_output_dir,
            note: None,
            now: DateTime::parse_from_rfc3339("2026-03-26T12:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            stage3_limit: 5,
            stage3_recent_horizon_seconds: Some(3600),
            rehearsal_limit: 5,
            rehearsal_recent_horizon_seconds: 3600,
            min_recent_acceptable_rehearsals: 2,
            non_prod_limit: 5,
            non_prod_dress_recent_horizon_seconds: 3600,
            non_prod_activation_recent_horizon_seconds: 3600,
            non_prod_min_recent_green_dress: 2,
            non_prod_min_recent_green_activation: 2,
        }
    }

    fn sample_prepared_generation() -> PreparedGeneration {
        PreparedGeneration {
            generated_at: DateTime::parse_from_rfc3339("2026-03-26T12:00:00Z")
                .expect("ts")
                .with_timezone(&Utc),
            prod_config_fingerprint_sha256: "prod_fp_a".to_string(),
            non_prod_config_fingerprint_sha256: "non_prod_fp_a".to_string(),
            decision_packet_verdict: "decision_packet_discussion_ready_but_not_authorized"
                .to_string(),
            checklist_verdict: "activation_checklist_discussion_ready_but_not_authorized"
                .to_string(),
            packet_json: serde_json::to_string_pretty(&sample_packet_json()).expect("packet json"),
            runbook_json: serde_json::to_string_pretty(&sample_runbook_json())
                .expect("runbook json"),
            runbook_markdown: "# Tiny-Live Activation Runbook\n\nsample".to_string(),
        }
    }

    fn sample_packet_json() -> serde_json::Value {
        json!({
            "generated_at": "2026-03-26T12:00:00Z",
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
            "checklist_verdict": "activation_checklist_discussion_ready_but_not_authorized",
            "checklist_reason": "sample",
            "prod_config_fingerprint": {
                "scope": "prod_execution_policy_guardrails",
                "sha256": "prod_fp_a",
                "secrets_excluded": true,
                "sensitive_urls_redacted_before_hashing": true
            },
            "non_prod_config_fingerprint": {
                "scope": "non_prod_execution_policy_guardrails",
                "sha256": "non_prod_fp_a",
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

    fn sample_runbook_json() -> serde_json::Value {
        json!({
            "generated_at": "2026-03-26T12:00:00Z",
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
            "decision_packet_generated_at": "2026-03-26T12:00:00Z",
            "decision_packet_verdict": "decision_packet_discussion_ready_but_not_authorized",
            "decision_packet_reason": "sample",
            "build_version": "0.1.0",
            "git_commit": "deadbeef",
            "prod_config_fingerprint": {
                "scope": "prod_execution_policy_guardrails",
                "sha256": "prod_fp_a",
                "secrets_excluded": true,
                "sensitive_urls_redacted_before_hashing": true
            },
            "non_prod_config_fingerprint": {
                "scope": "non_prod_execution_policy_guardrails",
                "sha256": "non_prod_fp_a",
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
                    "complete": true,
                    "activation_restart_required": true,
                    "rollback_restart_required": true,
                    "activation_services": ["solana-copy-bot.service"],
                    "rollback_services": ["solana-copy-bot.service"],
                    "activation_steps": ["restart"],
                    "rollback_steps": ["restart"]
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
