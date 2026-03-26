#![recursion_limit = "256"]
#![allow(unused_attributes)]

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::json;
use std::env;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

#[allow(dead_code)]
#[path = "copybot_activation_artifact_channel.rs"]
mod activation_artifact_channel;
#[allow(dead_code)]
#[path = "copybot_activation_artifact_publish.rs"]
mod activation_artifact_publish;

const USAGE: &str = "usage: copybot_activation_artifact_release --config <prod-path> --non-prod-config <path> --archive-dir <path> [--json] [--manifest-output <path>] [--bundle-output-dir <path>] [--note <text>] [--now <rfc3339>] [--stage3-limit <count>] [--stage3-recent-horizon-seconds <seconds>] [--rehearsal-limit <count>] [--rehearsal-recent-horizon-seconds <seconds>] [--min-recent-acceptable-rehearsals <count>] [--non-prod-limit <count>] [--non-prod-dress-recent-horizon-seconds <seconds>] [--non-prod-activation-recent-horizon-seconds <seconds>] [--non-prod-min-recent-green-dress <count>] [--non-prod-min-recent-green-activation <count>] [--promote-channel --channel-dir <path> [--channel-name <name>] [--allow-channel-overwrite]]";

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed building tokio runtime for activation artifact release")?;
    let output = runtime.block_on(run(config))?;
    println!("{output}");
    Ok(())
}

#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub(crate) publish: activation_artifact_publish::Config,
    pub(crate) json: bool,
    pub(crate) promote_channel: bool,
    pub(crate) channel_dir: Option<PathBuf>,
    pub(crate) channel_name: String,
    pub(crate) allow_channel_overwrite: bool,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ArtifactReleaseVerdict {
    ArtifactReleasePublished,
    ArtifactReleasePublishedAndPromoted,
    ArtifactReleasePublishFailed,
    ArtifactReleaseChannelPromoteBlocked,
    ArtifactReleaseFailed,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ArtifactReleaseReport {
    pub(crate) mode: String,
    pub(crate) released_at: DateTime<Utc>,
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

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

pub(crate) fn parse_args_from<I>(args: I) -> Result<Option<Config>>
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
    let mut rehearsal_limit = activation_artifact_publish::DEFAULT_REHEARSAL_HISTORY_LIMIT;
    let mut rehearsal_recent_horizon_seconds =
        activation_artifact_publish::DEFAULT_REHEARSAL_RECENT_HORIZON_SECONDS;
    let mut min_recent_acceptable_rehearsals =
        activation_artifact_publish::DEFAULT_MIN_RECENT_ACCEPTABLE_REHEARSALS;
    let mut non_prod_limit = activation_artifact_publish::DEFAULT_NON_PROD_HISTORY_LIMIT;
    let mut non_prod_dress_recent_horizon_seconds =
        activation_artifact_publish::DEFAULT_NON_PROD_DRESS_RECENT_HORIZON_SECONDS;
    let mut non_prod_activation_recent_horizon_seconds =
        activation_artifact_publish::DEFAULT_NON_PROD_ACTIVATION_RECENT_HORIZON_SECONDS;
    let mut non_prod_min_recent_green_dress =
        activation_artifact_publish::DEFAULT_NON_PROD_MIN_RECENT_GREEN_DRESS;
    let mut non_prod_min_recent_green_activation =
        activation_artifact_publish::DEFAULT_NON_PROD_MIN_RECENT_GREEN_ACTIVATION;
    let mut promote_channel = false;
    let mut channel_dir: Option<PathBuf> = None;
    let mut channel_name = activation_artifact_channel::DEFAULT_CHANNEL_NAME.to_string();
    let mut allow_channel_overwrite = false;

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
            "--promote-channel" => promote_channel = true,
            "--channel-dir" => {
                channel_dir = Some(PathBuf::from(parse_string_arg(
                    "--channel-dir",
                    args.next(),
                )?))
            }
            "--channel-name" => {
                channel_name = parse_channel_name(parse_string_arg("--channel-name", args.next())?)?
            }
            "--allow-channel-overwrite" => allow_channel_overwrite = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    if !promote_channel {
        if channel_dir.is_some() {
            bail!("--channel-dir requires --promote-channel");
        }
        if channel_name != activation_artifact_channel::DEFAULT_CHANNEL_NAME {
            bail!("--channel-name requires --promote-channel");
        }
        if allow_channel_overwrite {
            bail!("--allow-channel-overwrite requires --promote-channel");
        }
    }

    if promote_channel && channel_dir.is_none() {
        bail!("--promote-channel requires --channel-dir");
    }

    Ok(Some(Config {
        publish: activation_artifact_publish::Config {
            config_path: config_path.ok_or_else(|| anyhow!("missing required --config"))?,
            non_prod_config_path: non_prod_config_path
                .ok_or_else(|| anyhow!("missing required --non-prod-config"))?,
            archive_dir: archive_dir.ok_or_else(|| anyhow!("missing required --archive-dir"))?,
            json: false,
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
        },
        json,
        promote_channel,
        channel_dir,
        channel_name,
        allow_channel_overwrite,
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

async fn run(config: Config) -> Result<String> {
    let report = evaluate_release(&config).await;
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_human(&report))
    }
}

pub(crate) async fn evaluate_release(config: &Config) -> ArtifactReleaseReport {
    let publish_report = match activation_artifact_publish::evaluate_publish(&config.publish).await
    {
        Ok(report) => report,
        Err(error) => {
            return build_release_report(
                ArtifactReleaseVerdict::ArtifactReleasePublishFailed,
                format!("artifact release failed during publish evaluation: {error:#}"),
                config,
                None,
                None,
                None,
                false,
                false,
            )
        }
    };

    finalize_release(config, publish_report)
}

fn finalize_release(
    config: &Config,
    publish_report: activation_artifact_publish::ArtifactPublishReport,
) -> ArtifactReleaseReport {
    let generation_published = publish_report.generation_id.is_some();

    if publish_report.verdict
        != activation_artifact_publish::ArtifactPublishVerdict::ArtifactPublishSucceeded
    {
        return build_release_report(
            ArtifactReleaseVerdict::ArtifactReleasePublishFailed,
            format!(
                "artifact release stopped because publish did not complete cleanly: {}",
                publish_report.reason
            ),
            config,
            Some(publish_report),
            None,
            None,
            false,
            generation_published,
        );
    }

    if !config.promote_channel {
        return build_release_report(
            ArtifactReleaseVerdict::ArtifactReleasePublished,
            format!(
                "artifact release published generation {} without channel promotion",
                publish_report
                    .generation_id
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string())
            ),
            config,
            Some(publish_report),
            None,
            None,
            false,
            true,
        );
    }

    let channel_dir = config
        .channel_dir
        .clone()
        .expect("promote_channel validated channel_dir");
    let channel_config = activation_artifact_channel::Config {
        archive_dir: config.publish.archive_dir.clone(),
        channel_dir,
        channel_name: config.channel_name.clone(),
        json: false,
        mode: activation_artifact_channel::Mode::Promote {
            generation_selector: publish_report
                .generation_id
                .clone()
                .expect("successful publish generation id"),
            manifest_path: publish_report.manifest_path.clone().map(PathBuf::from),
            bundle_path: publish_report.bundle_path.clone().map(PathBuf::from),
            allow_overwrite: config.allow_channel_overwrite,
        },
    };

    let promote_report = match activation_artifact_channel::promote_channel(
        &channel_config,
        publish_report
            .generation_id
            .as_deref()
            .expect("successful publish generation id"),
        publish_report.manifest_path.as_deref().map(std::path::Path::new),
        publish_report.bundle_path.as_deref().map(std::path::Path::new),
        config.allow_channel_overwrite,
    ) {
        Ok(report) => report,
        Err(error) => {
            return build_release_report(
                ArtifactReleaseVerdict::ArtifactReleaseFailed,
                format!(
                    "artifact generation was published, but channel promote failed operationally: {error:#}"
                ),
                config,
                Some(publish_report),
                None,
                None,
                true,
                true,
            )
        }
    };

    if promote_report.verdict
        != activation_artifact_channel::ArtifactChannelVerdict::ArtifactChannelPromoted
    {
        return build_release_report(
            ArtifactReleaseVerdict::ArtifactReleaseChannelPromoteBlocked,
            format!(
                "artifact generation was published, but channel promote did not complete cleanly: {}",
                promote_report.reason
            ),
            config,
            Some(publish_report),
            Some(promote_report),
            None,
            true,
            true,
        );
    }

    let verify_config = activation_artifact_channel::Config {
        archive_dir: config.publish.archive_dir.clone(),
        channel_dir: config
            .channel_dir
            .clone()
            .expect("promote_channel validated channel_dir"),
        channel_name: config.channel_name.clone(),
        json: false,
        mode: activation_artifact_channel::Mode::Verify,
    };
    let verify_report = match activation_artifact_channel::inspect_channel(&verify_config, true) {
        Ok(report) => report,
        Err(error) => {
            return build_release_report(
                ArtifactReleaseVerdict::ArtifactReleaseFailed,
                format!(
                    "artifact generation was published and channel promotion succeeded, but channel verify failed operationally: {error:#}"
                ),
                config,
                Some(publish_report),
                Some(promote_report),
                None,
                true,
                true,
            )
        }
    };

    if verify_report.verdict
        != activation_artifact_channel::ArtifactChannelVerdict::ArtifactChannelOk
    {
        return build_release_report(
            ArtifactReleaseVerdict::ArtifactReleaseFailed,
            format!(
                "artifact generation was published and channel metadata was written, but channel verify returned {}: {}",
                serialize_enum(&verify_report.verdict),
                verify_report.reason
            ),
            config,
            Some(publish_report),
            Some(promote_report),
            Some(verify_report),
            true,
            true,
        );
    }

    build_release_report(
        ArtifactReleaseVerdict::ArtifactReleasePublishedAndPromoted,
        format!(
            "artifact release published generation {} and promoted channel `{}`",
            publish_report
                .generation_id
                .clone()
                .unwrap_or_else(|| "unknown".to_string()),
            config.channel_name
        ),
        config,
        Some(publish_report),
        Some(promote_report),
        Some(verify_report),
        true,
        true,
    )
}

fn build_release_report(
    verdict: ArtifactReleaseVerdict,
    reason: String,
    config: &Config,
    publish_report: Option<activation_artifact_publish::ArtifactPublishReport>,
    channel_promote_report: Option<activation_artifact_channel::ArtifactChannelReport>,
    channel_verify_report: Option<activation_artifact_channel::ArtifactChannelReport>,
    channel_promotion_attempted: bool,
    generation_published: bool,
) -> ArtifactReleaseReport {
    let publish_verdict = publish_report
        .as_ref()
        .map(|report| serialize_enum(&report.verdict));
    let publish_reason = publish_report.as_ref().map(|report| report.reason.clone());
    let channel_name = if channel_promotion_attempted {
        Some(config.channel_name.clone())
    } else {
        None
    };
    let channel_dir = if channel_promotion_attempted {
        config
            .channel_dir
            .as_ref()
            .map(|path| path.display().to_string())
    } else {
        None
    };

    ArtifactReleaseReport {
        mode: "artifact_release".to_string(),
        released_at: config.publish.now,
        verdict,
        reason,
        config_path: config.publish.config_path.display().to_string(),
        non_prod_config_path: config.publish.non_prod_config_path.display().to_string(),
        archive_dir: config.publish.archive_dir.display().to_string(),
        generation_id: publish_report.as_ref().and_then(|report| report.generation_id.clone()),
        generation_directory: publish_report
            .as_ref()
            .and_then(|report| report.generation_directory.clone()),
        packet_path: publish_report.as_ref().and_then(|report| report.packet_path.clone()),
        runbook_json_path: publish_report
            .as_ref()
            .and_then(|report| report.runbook_json_path.clone()),
        runbook_markdown_path: publish_report
            .as_ref()
            .and_then(|report| report.runbook_markdown_path.clone()),
        manifest_path: publish_report
            .as_ref()
            .and_then(|report| report.manifest_path.clone()),
        bundle_path: publish_report.as_ref().and_then(|report| report.bundle_path.clone()),
        decision_packet_verdict: publish_report
            .as_ref()
            .and_then(|report| report.decision_packet_verdict.clone()),
        checklist_verdict: publish_report
            .as_ref()
            .and_then(|report| report.checklist_verdict.clone()),
        publish_verdict,
        publish_reason,
        generation_published,
        manifest_generated: publish_report
            .as_ref()
            .map(|report| report.manifest_generated)
            .unwrap_or(false),
        bundle_exported: publish_report
            .as_ref()
            .map(|report| report.bundle_exported)
            .unwrap_or(false),
        channel_promotion_attempted,
        channel_promotion_happened: channel_promote_report
            .as_ref()
            .map(|report| report.promotion_happened)
            .unwrap_or(false),
        channel_verify_attempted: channel_verify_report.is_some(),
        channel_name,
        channel_dir,
        channel_metadata_path: channel_promote_report
            .as_ref()
            .map(|report| report.channel_metadata_path.clone())
            .or_else(|| {
                channel_verify_report
                    .as_ref()
                    .map(|report| report.channel_metadata_path.clone())
            }),
        channel_promote_verdict: channel_promote_report
            .as_ref()
            .map(|report| serialize_enum(&report.verdict)),
        channel_promote_reason: channel_promote_report
            .as_ref()
            .map(|report| report.reason.clone()),
        channel_verify_verdict: channel_verify_report
            .as_ref()
            .map(|report| serialize_enum(&report.verdict)),
        channel_verify_reason: channel_verify_report
            .as_ref()
            .map(|report| report.reason.clone()),
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary:
            "Artifact release only publishes review artifacts and optionally updates review-channel metadata. It does not authorize activation, does not enable execution, and does not override the Stage 3 production gate.".to_string(),
    }
}

fn render_human(report: &ArtifactReleaseReport) -> String {
    [
        "event=copybot_activation_artifact_release".to_string(),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("released_at={}", report.released_at.to_rfc3339()),
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
            "publish_verdict={}",
            report
                .publish_verdict
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "publish_reason={}",
            report
                .publish_reason
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!("generation_published={}", report.generation_published),
        format!("manifest_generated={}", report.manifest_generated),
        format!("bundle_exported={}", report.bundle_exported),
        format!(
            "channel_promotion_attempted={}",
            report.channel_promotion_attempted
        ),
        format!(
            "channel_promotion_happened={}",
            report.channel_promotion_happened
        ),
        format!(
            "channel_verify_attempted={}",
            report.channel_verify_attempted
        ),
        format!(
            "channel_name={}",
            report
                .channel_name
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "channel_dir={}",
            report
                .channel_dir
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "channel_metadata_path={}",
            report
                .channel_metadata_path
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "channel_promote_verdict={}",
            report
                .channel_promote_verdict
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "channel_promote_reason={}",
            report
                .channel_promote_reason
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "channel_verify_verdict={}",
            report
                .channel_verify_verdict
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "channel_verify_reason={}",
            report
                .channel_verify_reason
                .clone()
                .unwrap_or_else(|| "null".to_string())
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
    use std::fs;
    use std::path::Path;

    #[test]
    fn publish_only_release_succeeds() {
        let archive_dir = temp_dir("activation_release_publish_only_archive");
        let config = sample_release_config(&archive_dir, None, None, false, None, false);
        let publish_report = activation_artifact_publish::publish_prepared_generation(
            &config.publish,
            sample_prepared_generation("2026-03-26T12:00:00Z", "prod_fp_a", "non_prod_fp_a"),
        )
        .expect("publish");

        let report = finalize_release(&config, publish_report);

        assert_eq!(
            report.verdict,
            ArtifactReleaseVerdict::ArtifactReleasePublished
        );
        assert!(report.generation_published);
        assert!(!report.channel_promotion_attempted);
        assert!(Path::new(report.packet_path.as_ref().expect("packet path")).exists());
        assert!(!report.activation_authorized);
    }

    #[test]
    fn publish_and_channel_promote_release_succeeds() {
        let archive_dir = temp_dir("activation_release_promote_archive");
        let channel_dir = temp_dir("activation_release_promote_channel");
        let config = sample_release_config(
            &archive_dir,
            None,
            None,
            true,
            Some(channel_dir.clone()),
            false,
        );
        let publish_report = activation_artifact_publish::publish_prepared_generation(
            &config.publish,
            sample_prepared_generation("2026-03-26T12:00:00Z", "prod_fp_a", "non_prod_fp_a"),
        )
        .expect("publish");

        let report = finalize_release(&config, publish_report);

        assert_eq!(
            report.verdict,
            ArtifactReleaseVerdict::ArtifactReleasePublishedAndPromoted
        );
        assert!(report.channel_promotion_attempted);
        assert!(report.channel_promotion_happened);
        assert!(report.channel_verify_attempted);
        assert_eq!(
            report.channel_promote_verdict.as_deref(),
            Some("artifact_channel_promoted")
        );
        assert_eq!(
            report.channel_verify_verdict.as_deref(),
            Some("artifact_channel_ok")
        );
        assert!(channel_dir.join("current_review.json").exists());
    }

    #[test]
    fn publish_succeeds_but_channel_promote_blocked_without_overwrite_flag_yields_partial_verdict()
    {
        let archive_dir = temp_dir("activation_release_channel_block_archive");
        let channel_dir = temp_dir("activation_release_channel_block_channel");
        let base_config = sample_release_config(
            &archive_dir,
            None,
            None,
            true,
            Some(channel_dir.clone()),
            false,
        );

        let first_publish = activation_artifact_publish::publish_prepared_generation(
            &base_config.publish,
            sample_prepared_generation("2026-03-26T12:00:00Z", "prod_fp_a", "non_prod_fp_a"),
        )
        .expect("first publish");
        let first_release = finalize_release(&base_config, first_publish);
        assert_eq!(
            first_release.verdict,
            ArtifactReleaseVerdict::ArtifactReleasePublishedAndPromoted
        );

        let second_publish = activation_artifact_publish::publish_prepared_generation(
            &base_config.publish,
            sample_prepared_generation("2026-03-26T12:05:00Z", "prod_fp_b", "non_prod_fp_b"),
        )
        .expect("second publish");
        let second_release = finalize_release(&base_config, second_publish);

        assert_eq!(
            second_release.verdict,
            ArtifactReleaseVerdict::ArtifactReleaseChannelPromoteBlocked
        );
        assert!(second_release.generation_published);
        assert!(second_release.channel_promotion_attempted);
        assert!(!second_release.channel_promotion_happened);
        assert_eq!(
            second_release.channel_promote_verdict.as_deref(),
            Some("artifact_channel_refused_without_overwrite")
        );

        let channel_metadata = fs::read_to_string(channel_dir.join("current_review.json"))
            .expect("read channel metadata");
        assert!(channel_metadata.contains("2026-03-26T12:00:00+00:00"));
        assert!(!channel_metadata.contains("2026-03-26T12:05:00+00:00"));
    }

    #[test]
    fn release_does_not_silently_overwrite_existing_archive_generation() {
        let archive_dir = temp_dir("activation_release_publish_block_archive");
        let config = sample_release_config(&archive_dir, None, None, false, None, false);
        let prepared =
            sample_prepared_generation("2026-03-26T12:00:00Z", "prod_fp_a", "non_prod_fp_a");

        let first_publish = activation_artifact_publish::publish_prepared_generation(
            &config.publish,
            prepared.clone(),
        )
        .expect("first publish");
        let second_publish =
            activation_artifact_publish::publish_prepared_generation(&config.publish, prepared)
                .expect("second publish");
        let report = finalize_release(&config, second_publish);

        assert_eq!(
            first_publish.verdict,
            activation_artifact_publish::ArtifactPublishVerdict::ArtifactPublishSucceeded
        );
        assert_eq!(
            report.verdict,
            ArtifactReleaseVerdict::ArtifactReleasePublishFailed
        );
        assert!(report
            .publish_reason
            .as_deref()
            .unwrap_or_default()
            .contains("refusing to overwrite existing generation directory"));
    }

    #[test]
    fn release_does_not_silently_overwrite_channel_metadata() {
        let archive_dir = temp_dir("activation_release_channel_overwrite_archive");
        let channel_dir = temp_dir("activation_release_channel_overwrite_channel");
        let config = sample_release_config(
            &archive_dir,
            None,
            None,
            true,
            Some(channel_dir.clone()),
            false,
        );
        let first_release = finalize_release(
            &config,
            activation_artifact_publish::publish_prepared_generation(
                &config.publish,
                sample_prepared_generation("2026-03-26T12:00:00Z", "prod_fp_a", "non_prod_fp_a"),
            )
            .expect("first publish"),
        );
        assert_eq!(
            first_release.verdict,
            ArtifactReleaseVerdict::ArtifactReleasePublishedAndPromoted
        );

        let second_release = finalize_release(
            &config,
            activation_artifact_publish::publish_prepared_generation(
                &config.publish,
                sample_prepared_generation("2026-03-26T12:05:00Z", "prod_fp_b", "non_prod_fp_b"),
            )
            .expect("second publish"),
        );

        assert_eq!(
            second_release.verdict,
            ArtifactReleaseVerdict::ArtifactReleaseChannelPromoteBlocked
        );
        assert!(!second_release.activation_authorized);
    }

    fn sample_release_config(
        archive_dir: &Path,
        manifest_output_path: Option<PathBuf>,
        bundle_output_dir: Option<PathBuf>,
        promote_channel: bool,
        channel_dir: Option<PathBuf>,
        allow_channel_overwrite: bool,
    ) -> Config {
        Config {
            publish: activation_artifact_publish::Config {
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
            },
            json: false,
            promote_channel,
            channel_dir,
            channel_name: activation_artifact_channel::DEFAULT_CHANNEL_NAME.to_string(),
            allow_channel_overwrite,
        }
    }

    fn sample_prepared_generation(
        generated_at: &str,
        prod_fingerprint: &str,
        non_prod_fingerprint: &str,
    ) -> activation_artifact_publish::PreparedGeneration {
        activation_artifact_publish::PreparedGeneration {
            generated_at: DateTime::parse_from_rfc3339(generated_at)
                .expect("ts")
                .with_timezone(&Utc),
            prod_config_fingerprint_sha256: prod_fingerprint.to_string(),
            non_prod_config_fingerprint_sha256: non_prod_fingerprint.to_string(),
            decision_packet_verdict: "decision_packet_discussion_ready_but_not_authorized"
                .to_string(),
            checklist_verdict: "activation_checklist_discussion_ready_but_not_authorized"
                .to_string(),
            packet_json: serde_json::to_string_pretty(&sample_packet_json(
                generated_at,
                prod_fingerprint,
                non_prod_fingerprint,
            ))
            .expect("packet json"),
            runbook_json: serde_json::to_string_pretty(&sample_runbook_json(
                generated_at,
                prod_fingerprint,
                non_prod_fingerprint,
            ))
            .expect("runbook json"),
            runbook_markdown: "# Tiny-Live Activation Runbook\n\nsample".to_string(),
        }
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

    fn sample_runbook_json(
        generated_at: &str,
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
            "decision_packet_generated_at": generated_at,
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
