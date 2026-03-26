#![recursion_limit = "256"]
#![allow(unused_attributes)]

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[allow(dead_code)]
#[path = "copybot_activation_artifact_release.rs"]
mod activation_artifact_release;
#[allow(dead_code)]
#[path = "copybot_activation_artifact_release_history.rs"]
mod activation_artifact_release_history;

const USAGE: &str = "usage: copybot_activation_artifact_release_publish_report --release-archive-dir <path> [--json] [--pointer-name <name>] (--publish [release args...] [--persist-latest-pointer --latest-pointer-dir <path> [--allow-latest-pointer-overwrite]] | --report-latest --latest-pointer-dir <path> | --verify-latest --latest-pointer-dir <path>)";
const RELEASE_POINTER_VERSION: &str = "1";
pub(crate) const DEFAULT_LATEST_POINTER_NAME: &str = "latest_release";

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed building tokio runtime for activation artifact release publisher")?;
    let output = runtime.block_on(run(config))?;
    println!("{output}");
    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    release_archive_dir: PathBuf,
    latest_pointer_dir: Option<PathBuf>,
    pointer_name: String,
    json: bool,
    mode: Mode,
}

#[derive(Debug, Clone)]
enum Mode {
    Publish {
        release_config: activation_artifact_release::Config,
        persist_latest_pointer: bool,
        allow_latest_pointer_overwrite: bool,
    },
    ReportLatest,
    VerifyLatest,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ArtifactReleaseReportPublisherVerdict {
    ArtifactReleaseReportPublished,
    ArtifactReleaseReportPublishedAndPointedLatest,
    ArtifactReleaseReportPointerBlocked,
    ArtifactReleaseReportFailed,
    ArtifactReleaseReportVerifyOk,
    ArtifactReleaseReportVerifyMissingTarget,
    ArtifactReleaseReportVerifyAmbiguousTimestamp,
    ArtifactReleaseReportInvalidMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReleaseLatestPointerMetadata {
    pointer_version: String,
    pointer_name: String,
    source_release_archive_dir: String,
    selected_release_artifact_path: String,
    selected_release_artifact_file_name: String,
    release_artifact_mode: String,
    released_at: Option<DateTime<Utc>>,
    released_at_source: activation_artifact_release_history::ReleaseTimestampSource,
    compat_loaded_without_released_at: bool,
    deterministic_timestamp_available: bool,
    ordered_history_confident: bool,
    release_verdict: String,
    release_reason: String,
    selected_generation_id: Option<String>,
    channel_promotion_happened: bool,
    channel_metadata_path: Option<String>,
    pointed_at: DateTime<Utc>,
    build_version: String,
    git_commit: Option<String>,
}

#[derive(Debug, Clone)]
struct PersistedReleaseArtifact {
    path: PathBuf,
    canonical_path: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ArtifactReleasePublishReport {
    pub(crate) mode: String,
    pub(crate) verdict: ArtifactReleaseReportPublisherVerdict,
    pub(crate) reason: String,
    pub(crate) release_archive_dir: String,
    pub(crate) persisted_release_artifact_path: Option<String>,
    pub(crate) persisted_release_artifact_exists: bool,
    pub(crate) persisted_release_artifact_file_name: Option<String>,
    pub(crate) release_verdict: Option<String>,
    pub(crate) release_reason: Option<String>,
    pub(crate) generation_id: Option<String>,
    pub(crate) released_at: Option<DateTime<Utc>>,
    pub(crate) released_at_source:
        Option<activation_artifact_release_history::ReleaseTimestampSource>,
    pub(crate) compat_loaded_without_released_at: bool,
    pub(crate) deterministic_timestamp_available: bool,
    pub(crate) ordered_history_confident: bool,
    pub(crate) latest_pointer_attempted: bool,
    pub(crate) latest_pointer_updated: bool,
    pub(crate) latest_pointer_dir: Option<String>,
    pub(crate) latest_pointer_name: String,
    pub(crate) latest_pointer_path: Option<String>,
    pub(crate) latest_pointer_source_release_archive_dir: Option<String>,
    pub(crate) latest_pointer_pointed_at: Option<DateTime<Utc>>,
    pub(crate) latest_pointer_exists: bool,
    pub(crate) latest_pointer_overwrite_used: bool,
    pub(crate) latest_pointer_target_exists: bool,
    pub(crate) latest_pointer_target_matches_identity: bool,
    pub(crate) verification_attempted: bool,
    pub(crate) missing_paths: Vec<String>,
    pub(crate) inconsistencies: Vec<String>,
    pub(crate) execution_untouched: bool,
    pub(crate) activation_authorized: bool,
    pub(crate) not_authorized_summary: String,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let args = args.into_iter().collect::<Vec<_>>();
    let mut release_archive_dir: Option<PathBuf> = None;
    let mut latest_pointer_dir: Option<PathBuf> = None;
    let mut pointer_name = DEFAULT_LATEST_POINTER_NAME.to_string();
    let mut json = false;
    let mut publish = false;
    let mut report_latest = false;
    let mut verify_latest = false;
    let mut persist_latest_pointer = false;
    let mut allow_latest_pointer_overwrite = false;
    let mut forwarded_release_args = Vec::new();

    let mut index = 0usize;
    while index < args.len() {
        let arg = args[index].as_str();
        match arg {
            "--release-archive-dir" => {
                index += 1;
                release_archive_dir = Some(PathBuf::from(parse_required_value(
                    "--release-archive-dir",
                    args.get(index),
                )?));
            }
            "--latest-pointer-dir" => {
                index += 1;
                latest_pointer_dir = Some(PathBuf::from(parse_required_value(
                    "--latest-pointer-dir",
                    args.get(index),
                )?));
            }
            "--pointer-name" => {
                index += 1;
                pointer_name =
                    parse_pointer_name(parse_required_value("--pointer-name", args.get(index))?)?;
            }
            "--publish" => publish = true,
            "--report-latest" => report_latest = true,
            "--verify-latest" => verify_latest = true,
            "--persist-latest-pointer" => persist_latest_pointer = true,
            "--allow-latest-pointer-overwrite" => allow_latest_pointer_overwrite = true,
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => {
                forwarded_release_args.push(other.to_string());
                if release_arg_takes_value(other) {
                    index += 1;
                    forwarded_release_args.push(parse_required_value(other, args.get(index))?);
                }
            }
        }
        index += 1;
    }

    let mode_count = [publish, report_latest, verify_latest]
        .into_iter()
        .filter(|value| *value)
        .count();
    if mode_count != 1 {
        bail!("exactly one of --publish, --report-latest, or --verify-latest is required");
    }

    let release_archive_dir =
        release_archive_dir.ok_or_else(|| anyhow!("missing required --release-archive-dir"))?;

    let mode = if publish {
        if report_latest || verify_latest {
            bail!("--publish cannot be combined with --report-latest or --verify-latest");
        }
        if persist_latest_pointer && latest_pointer_dir.is_none() {
            bail!("--persist-latest-pointer requires --latest-pointer-dir");
        }
        if allow_latest_pointer_overwrite && !persist_latest_pointer {
            bail!("--allow-latest-pointer-overwrite requires --persist-latest-pointer");
        }
        let Some(release_config) =
            activation_artifact_release::parse_args_from(forwarded_release_args.into_iter())?
        else {
            return Ok(None);
        };
        Mode::Publish {
            release_config,
            persist_latest_pointer,
            allow_latest_pointer_overwrite,
        }
    } else {
        if !forwarded_release_args.is_empty() {
            bail!(
                "release flow arguments are only allowed with --publish; unexpected extra args: {}",
                forwarded_release_args.join(" ")
            );
        }
        latest_pointer_dir
            .as_ref()
            .ok_or_else(|| anyhow!("missing required --latest-pointer-dir"))?
            .components()
            .next()
            .ok_or_else(|| anyhow!("--latest-pointer-dir cannot be empty"))?;
        if allow_latest_pointer_overwrite {
            bail!("--allow-latest-pointer-overwrite only applies to --publish");
        }
        if persist_latest_pointer {
            bail!("--persist-latest-pointer only applies to --publish");
        }
        if report_latest {
            Mode::ReportLatest
        } else {
            Mode::VerifyLatest
        }
    };

    Ok(Some(Config {
        release_archive_dir,
        latest_pointer_dir,
        pointer_name,
        json,
        mode,
    }))
}

fn parse_required_value(flag: &str, value: Option<&String>) -> Result<String> {
    let raw = value.ok_or_else(|| anyhow!("missing value for {flag}"))?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(trimmed.to_string())
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

fn release_arg_takes_value(flag: &str) -> bool {
    matches!(
        flag,
        "--config"
            | "--non-prod-config"
            | "--archive-dir"
            | "--manifest-output"
            | "--bundle-output-dir"
            | "--note"
            | "--now"
            | "--stage3-limit"
            | "--stage3-recent-horizon-seconds"
            | "--rehearsal-limit"
            | "--rehearsal-recent-horizon-seconds"
            | "--min-recent-acceptable-rehearsals"
            | "--non-prod-limit"
            | "--non-prod-dress-recent-horizon-seconds"
            | "--non-prod-activation-recent-horizon-seconds"
            | "--non-prod-min-recent-green-dress"
            | "--non-prod-min-recent-green-activation"
            | "--channel-dir"
            | "--channel-name"
    )
}

async fn run(config: Config) -> Result<String> {
    let report = match &config.mode {
        Mode::Publish {
            release_config,
            persist_latest_pointer,
            allow_latest_pointer_overwrite,
        } => {
            let release_report =
                activation_artifact_release::evaluate_release(release_config).await;
            publish_release_report(
                &config,
                &release_report,
                *persist_latest_pointer,
                *allow_latest_pointer_overwrite,
            )
        }
        Mode::ReportLatest => inspect_latest_pointer(&config, false),
        Mode::VerifyLatest => inspect_latest_pointer(&config, true),
    }?;

    if config.json {
        serde_json::to_string_pretty(&report)
            .context("failed serializing activation artifact release publisher json")
    } else {
        Ok(render_human(&report))
    }
}

fn publish_release_report(
    config: &Config,
    release_report: &activation_artifact_release::ArtifactReleaseReport,
    persist_latest_pointer: bool,
    allow_latest_pointer_overwrite: bool,
) -> Result<ArtifactReleasePublishReport> {
    let persisted = match persist_release_artifact(&config.release_archive_dir, release_report) {
        Ok(persisted) => persisted,
        Err(error) => {
            return Ok(build_report(
                "publish",
                ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportFailed,
                format!("failed to persist activation artifact release report: {error:#}"),
                config,
                Some(release_report),
                None,
                false,
                false,
                false,
                Vec::new(),
                Vec::new(),
            ))
        }
    };

    if !persist_latest_pointer {
        return Ok(build_report(
            "publish",
            ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportPublished,
            format!(
                "persisted activation artifact release report at {}",
                persisted.path.display()
            ),
            config,
            Some(release_report),
            Some(&persisted),
            false,
            false,
            false,
            Vec::new(),
            Vec::new(),
        ));
    }

    let pointer_result = write_latest_pointer(config, &persisted, allow_latest_pointer_overwrite);
    match pointer_result {
        Ok(pointer_path) => {
            let verify_report = inspect_latest_pointer(config, true)?;
            let (verdict, reason) = match verify_report.verdict {
                ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportVerifyOk => (
                    ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportPublishedAndPointedLatest,
                    format!(
                        "persisted activation artifact release report and updated latest pointer {}",
                        pointer_path.display()
                    ),
                ),
                ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportVerifyMissingTarget
                | ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportVerifyAmbiguousTimestamp
                | ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportInvalidMetadata
                | ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportFailed
                | ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportPointerBlocked
                | ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportPublished
                | ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportPublishedAndPointedLatest => (
                    ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportPointerBlocked,
                    format!(
                        "release report was persisted, but latest pointer verification did not complete cleanly: {}",
                        verify_report.reason
                    ),
                ),
            };
            Ok(finalize_publish_pointer_report(
                verify_report,
                verdict,
                reason,
                true,
                allow_latest_pointer_overwrite,
            ))
        }
        Err(error) => Ok(build_report(
            "publish",
            ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportPointerBlocked,
            format!(
                "release report was persisted, but latest pointer update did not complete cleanly: {error:#}"
            ),
            config,
            Some(release_report),
            Some(&persisted),
            true,
            false,
            allow_latest_pointer_overwrite,
            Vec::new(),
            Vec::new(),
        )),
    }
}

fn persist_release_artifact(
    release_archive_dir: &Path,
    release_report: &activation_artifact_release::ArtifactReleaseReport,
) -> Result<PersistedReleaseArtifact> {
    fs::create_dir_all(release_archive_dir).with_context(|| {
        format!(
            "failed creating release archive dir {}",
            release_archive_dir.display()
        )
    })?;

    let final_path = release_archive_dir.join(release_artifact_file_name(release_report));
    if final_path.exists() {
        bail!(
            "refusing to overwrite existing release artifact {}",
            final_path.display()
        );
    }

    let stage_path =
        release_archive_dir.join(format!(".release_report.staging_{}", unique_suffix()));
    fs::write(
        &stage_path,
        serde_json::to_string_pretty(release_report)
            .context("failed serializing activation artifact release report json")?,
    )
    .with_context(|| {
        format!(
            "failed writing staged release artifact {}",
            stage_path.display()
        )
    })?;
    fs::rename(&stage_path, &final_path).with_context(|| {
        format!(
            "failed promoting staged release artifact {} to {}",
            stage_path.display(),
            final_path.display()
        )
    })?;

    let canonical_path = fs::canonicalize(&final_path).with_context(|| {
        format!(
            "failed canonicalizing persisted release artifact {}",
            final_path.display()
        )
    })?;

    Ok(PersistedReleaseArtifact {
        path: final_path,
        canonical_path,
    })
}

fn write_latest_pointer(
    config: &Config,
    persisted: &PersistedReleaseArtifact,
    allow_overwrite: bool,
) -> Result<PathBuf> {
    let latest_pointer_dir = config
        .latest_pointer_dir
        .as_ref()
        .ok_or_else(|| anyhow!("latest pointer dir is required for pointer updates"))?;
    let metadata_path = latest_pointer_path(latest_pointer_dir, &config.pointer_name);
    if metadata_path.exists() && !allow_overwrite {
        bail!(
            "latest pointer {} already exists; rerun with --allow-latest-pointer-overwrite to replace it",
            metadata_path.display()
        );
    }

    let inspected = activation_artifact_release_history::inspect_release_artifact(&persisted.path)
        .with_context(|| {
            format!(
                "failed reloading persisted release artifact {} for latest pointer metadata",
                persisted.path.display()
            )
        })?;
    let canonical_release_archive_dir =
        canonicalize_release_archive_dir(&config.release_archive_dir).with_context(|| {
            format!(
                "failed canonicalizing release archive dir {} for latest pointer metadata",
                config.release_archive_dir.display()
            )
        })?;
    let ordered_history_confident = inspected.effective_released_at.is_some();
    let metadata = ReleaseLatestPointerMetadata {
        pointer_version: RELEASE_POINTER_VERSION.to_string(),
        pointer_name: config.pointer_name.clone(),
        source_release_archive_dir: canonical_release_archive_dir.display().to_string(),
        selected_release_artifact_path: persisted.canonical_path.display().to_string(),
        selected_release_artifact_file_name: persisted
            .path
            .file_name()
            .map(|value| value.to_string_lossy().into_owned())
            .unwrap_or_else(|| persisted.path.display().to_string()),
        release_artifact_mode: inspected.artifact.mode.clone(),
        released_at: inspected.effective_released_at,
        released_at_source: inspected.released_at_source,
        compat_loaded_without_released_at: inspected.released_at_source
            != activation_artifact_release_history::ReleaseTimestampSource::ReleasedAt,
        deterministic_timestamp_available: inspected.effective_released_at.is_some(),
        ordered_history_confident,
        release_verdict: serialize_enum(&inspected.artifact.verdict),
        release_reason: inspected.artifact.reason.clone(),
        selected_generation_id: inspected.artifact.generation_id.clone(),
        channel_promotion_happened: inspected.artifact.channel_promotion_happened,
        channel_metadata_path: inspected.artifact.channel_metadata_path.clone(),
        pointed_at: Utc::now(),
        build_version: env!("CARGO_PKG_VERSION").to_string(),
        git_commit: resolve_git_commit(),
    };

    fs::create_dir_all(latest_pointer_dir).with_context(|| {
        format!(
            "failed creating latest pointer dir {}",
            latest_pointer_dir.display()
        )
    })?;
    let stage_path = latest_pointer_dir.join(format!(
        ".{}.staging_{}",
        config.pointer_name,
        unique_suffix()
    ));
    fs::write(
        &stage_path,
        serde_json::to_string_pretty(&metadata)
            .context("failed serializing latest pointer metadata json")?,
    )
    .with_context(|| {
        format!(
            "failed writing staged latest pointer metadata {}",
            stage_path.display()
        )
    })?;
    if metadata_path.exists() && allow_overwrite {
        fs::remove_file(&metadata_path).with_context(|| {
            format!(
                "failed removing existing latest pointer metadata {}",
                metadata_path.display()
            )
        })?;
    }
    fs::rename(&stage_path, &metadata_path).with_context(|| {
        format!(
            "failed promoting staged latest pointer metadata {} to {}",
            stage_path.display(),
            metadata_path.display()
        )
    })?;
    Ok(metadata_path)
}

fn inspect_latest_pointer(
    config: &Config,
    verification_attempted: bool,
) -> Result<ArtifactReleasePublishReport> {
    let latest_pointer_dir = config
        .latest_pointer_dir
        .as_ref()
        .ok_or_else(|| anyhow!("latest pointer dir is required for report/verify"))?;
    let metadata_path = latest_pointer_path(latest_pointer_dir, &config.pointer_name);
    if !metadata_path.exists() {
        return Ok(build_report(
            if verification_attempted {
                "verify_latest"
            } else {
                "report_latest"
            },
            ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportVerifyMissingTarget,
            format!(
                "no latest release pointer metadata found at {}",
                metadata_path.display()
            ),
            config,
            None,
            None,
            false,
            false,
            false,
            vec![metadata_path.display().to_string()],
            Vec::new(),
        ));
    }

    let raw = match fs::read_to_string(&metadata_path) {
        Ok(raw) => raw,
        Err(error) => {
            return Ok(build_report(
                if verification_attempted {
                    "verify_latest"
                } else {
                    "report_latest"
                },
                ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportInvalidMetadata,
                format!("failed reading latest pointer metadata: {error}"),
                config,
                None,
                None,
                false,
                false,
                false,
                Vec::new(),
                vec![metadata_path.display().to_string()],
            ))
        }
    };

    let metadata: ReleaseLatestPointerMetadata = match serde_json::from_str(&raw) {
        Ok(metadata) => metadata,
        Err(error) => {
            return Ok(build_report(
                if verification_attempted {
                    "verify_latest"
                } else {
                    "report_latest"
                },
                ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportInvalidMetadata,
                format!("failed parsing latest pointer metadata json: {error}"),
                config,
                None,
                None,
                false,
                false,
                false,
                Vec::new(),
                vec![metadata_path.display().to_string()],
            ))
        }
    };

    let mut missing_paths = Vec::new();
    let mut inconsistencies = Vec::new();
    let canonical_requested_archive_root =
        match canonicalize_release_archive_dir(&config.release_archive_dir) {
            Ok(path) => Some(path),
            Err(error) => {
                inconsistencies.push(format!(
                    "failed canonicalizing requested release archive {}: {error:#}",
                    config.release_archive_dir.display()
                ));
                None
            }
        };
    let canonical_stored_archive_root = match canonicalize_stored_release_archive_dir(
        &metadata.source_release_archive_dir,
    ) {
        Ok(path) => Some(path),
        Err(error) => {
            inconsistencies.push(format!(
                    "latest pointer source_release_archive_dir `{}` cannot be canonicalized deterministically: {error:#}",
                    metadata.source_release_archive_dir
                ));
            None
        }
    };
    if metadata.pointer_version != RELEASE_POINTER_VERSION {
        inconsistencies.push(format!(
            "unsupported latest pointer metadata version `{}`; expected `{RELEASE_POINTER_VERSION}`",
            metadata.pointer_version
        ));
    }
    if metadata.pointer_name != config.pointer_name {
        inconsistencies.push(format!(
            "latest pointer metadata name `{}` does not match requested pointer `{}`",
            metadata.pointer_name, config.pointer_name
        ));
    }
    if let (Some(canonical_stored_archive_root), Some(canonical_requested_archive_root)) = (
        canonical_stored_archive_root.as_ref(),
        canonical_requested_archive_root.as_ref(),
    ) {
        if canonical_stored_archive_root != canonical_requested_archive_root {
            inconsistencies.push(format!(
                "latest pointer metadata source archive `{}` does not match requested release archive `{}`",
                canonical_stored_archive_root.display(),
                canonical_requested_archive_root.display()
            ));
        }
    }

    let target_path = PathBuf::from(&metadata.selected_release_artifact_path);
    if !target_path.is_absolute() {
        inconsistencies.push(format!(
            "latest pointer target `{}` is not absolute; relative targets are unsupported",
            metadata.selected_release_artifact_path
        ));
    }
    if !target_path.exists() {
        missing_paths.push(target_path.display().to_string());
    }

    let inspected_target = if missing_paths.is_empty() {
        match fs::canonicalize(&target_path) {
            Ok(canonical_target) => match canonical_requested_archive_root.as_ref() {
                Some(canonical_archive_root) => {
                    if !canonical_target.starts_with(canonical_archive_root) {
                        inconsistencies.push(format!(
                            "latest pointer target {} is outside release archive {}",
                            canonical_target.display(),
                            canonical_archive_root.display()
                        ));
                        None
                    } else {
                        match activation_artifact_release_history::inspect_release_artifact(
                            &canonical_target,
                        ) {
                            Ok(artifact) => Some(artifact),
                            Err(error) => {
                                inconsistencies.push(format!(
                                        "latest pointer target is not a valid release artifact: {error:#}"
                                    ));
                                None
                            }
                        }
                    }
                }
                None => None,
            },
            Err(error) => {
                inconsistencies.push(format!(
                    "failed canonicalizing latest pointer target {}: {error}",
                    target_path.display()
                ));
                None
            }
        }
    } else {
        None
    };

    if let Some(inspected) = inspected_target.as_ref() {
        if inspected.artifact.mode != metadata.release_artifact_mode {
            inconsistencies.push(format!(
                "latest pointer recorded mode `{}` but target artifact mode is `{}`",
                metadata.release_artifact_mode, inspected.artifact.mode
            ));
        }
        if inspected.effective_released_at != metadata.released_at {
            inconsistencies.push(
                "latest pointer released_at does not match the target release artifact".to_string(),
            );
        }
        if inspected.released_at_source != metadata.released_at_source {
            inconsistencies.push(
                "latest pointer released_at_source does not match the target release artifact"
                    .to_string(),
            );
        }
        let compat_loaded = inspected.released_at_source
            != activation_artifact_release_history::ReleaseTimestampSource::ReleasedAt;
        if compat_loaded != metadata.compat_loaded_without_released_at {
            inconsistencies.push(
                "latest pointer compat_loaded_without_released_at does not match the target release artifact".to_string(),
            );
        }
        let deterministic_timestamp_available = inspected.effective_released_at.is_some();
        if deterministic_timestamp_available != metadata.deterministic_timestamp_available {
            inconsistencies.push(
                "latest pointer deterministic_timestamp_available does not match the target release artifact".to_string(),
            );
        }
        if deterministic_timestamp_available != metadata.ordered_history_confident {
            inconsistencies.push(
                "latest pointer ordered_history_confident does not match the target release artifact".to_string(),
            );
        }
        if serialize_enum(&inspected.artifact.verdict) != metadata.release_verdict {
            inconsistencies.push(
                "latest pointer release_verdict does not match the target release artifact"
                    .to_string(),
            );
        }
        if inspected.artifact.reason != metadata.release_reason {
            inconsistencies.push(
                "latest pointer release_reason does not match the target release artifact"
                    .to_string(),
            );
        }
        if inspected.artifact.generation_id != metadata.selected_generation_id {
            inconsistencies.push(
                "latest pointer selected_generation_id does not match the target release artifact"
                    .to_string(),
            );
        }
        if inspected.artifact.channel_promotion_happened != metadata.channel_promotion_happened {
            inconsistencies.push(
                "latest pointer channel_promotion_happened does not match the target release artifact".to_string(),
            );
        }
        if inspected.artifact.channel_metadata_path != metadata.channel_metadata_path {
            inconsistencies.push(
                "latest pointer channel_metadata_path does not match the target release artifact"
                    .to_string(),
            );
        }
    }

    let (verdict, reason) = if !missing_paths.is_empty() {
        (
            ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportVerifyMissingTarget,
            "latest release pointer exists, but its target release artifact is missing"
                .to_string(),
        )
    } else if !inconsistencies.is_empty() {
        (
            ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportInvalidMetadata,
            "latest release pointer metadata exists, but it is inconsistent with the target release artifact".to_string(),
        )
    } else if metadata.released_at_source
        == activation_artifact_release_history::ReleaseTimestampSource::MissingDeterministicTimestamp
    {
        (
            ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportVerifyAmbiguousTimestamp,
            "latest release pointer resolves, but the target artifact has no deterministic timestamp and is not suitable for ordered history confidence".to_string(),
        )
    } else {
        (
            ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportVerifyOk,
            format!(
                "latest release pointer `{}` resolves to a consistent persisted release artifact",
                config.pointer_name
            ),
        )
    };

    Ok(build_report_from_pointer_metadata(
        if verification_attempted {
            "verify_latest"
        } else {
            "report_latest"
        },
        verdict,
        reason,
        config,
        &metadata_path,
        &metadata,
        missing_paths,
        inconsistencies,
        verification_attempted,
    ))
}

#[allow(dead_code)]
pub(crate) fn inspect_latest_pointer_report(
    release_archive_dir: &Path,
    latest_pointer_dir: &Path,
    pointer_name: &str,
    verification_attempted: bool,
) -> Result<ArtifactReleasePublishReport> {
    let config = Config {
        release_archive_dir: release_archive_dir.to_path_buf(),
        latest_pointer_dir: Some(latest_pointer_dir.to_path_buf()),
        pointer_name: parse_pointer_name(pointer_name.to_string())?,
        json: false,
        mode: if verification_attempted {
            Mode::VerifyLatest
        } else {
            Mode::ReportLatest
        },
    };
    inspect_latest_pointer(&config, verification_attempted)
}

fn build_report(
    mode: &str,
    verdict: ArtifactReleaseReportPublisherVerdict,
    reason: String,
    config: &Config,
    release_report: Option<&activation_artifact_release::ArtifactReleaseReport>,
    persisted: Option<&PersistedReleaseArtifact>,
    latest_pointer_attempted: bool,
    latest_pointer_updated: bool,
    latest_pointer_overwrite_used: bool,
    missing_paths: Vec<String>,
    inconsistencies: Vec<String>,
) -> ArtifactReleasePublishReport {
    let release_report_verdict = release_report.map(|report| serialize_enum(&report.verdict));
    let released_at = release_report.map(|report| report.released_at);
    ArtifactReleasePublishReport {
        mode: mode.to_string(),
        verdict,
        reason,
        release_archive_dir: config.release_archive_dir.display().to_string(),
        persisted_release_artifact_path: persisted.map(|value| value.path.display().to_string()),
        persisted_release_artifact_exists: persisted.is_some_and(|value| value.path.exists()),
        persisted_release_artifact_file_name: persisted.and_then(|value| {
            value
                .path
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
        }),
        release_verdict: release_report_verdict,
        release_reason: release_report.map(|report| report.reason.clone()),
        generation_id: release_report.and_then(|report| report.generation_id.clone()),
        released_at,
        released_at_source: released_at.map(|_| {
            activation_artifact_release_history::ReleaseTimestampSource::ReleasedAt
        }),
        compat_loaded_without_released_at: false,
        deterministic_timestamp_available: released_at.is_some(),
        ordered_history_confident: released_at.is_some(),
        latest_pointer_attempted,
        latest_pointer_updated,
        latest_pointer_dir: config
            .latest_pointer_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        latest_pointer_name: config.pointer_name.clone(),
        latest_pointer_path: config
            .latest_pointer_dir
            .as_ref()
            .map(|dir| latest_pointer_path(dir, &config.pointer_name).display().to_string()),
        latest_pointer_source_release_archive_dir: None,
        latest_pointer_pointed_at: None,
        latest_pointer_exists: config
            .latest_pointer_dir
            .as_ref()
            .is_some_and(|dir| latest_pointer_path(dir, &config.pointer_name).exists()),
        latest_pointer_overwrite_used,
        latest_pointer_target_exists: false,
        latest_pointer_target_matches_identity: false,
        verification_attempted: false,
        missing_paths,
        inconsistencies,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary:
            "Artifact/release management only persists and verifies release artifacts. It does not authorize activation, does not enable execution, and does not override the Stage 3 production gate.".to_string(),
    }
}

fn finalize_publish_pointer_report(
    mut report: ArtifactReleasePublishReport,
    verdict: ArtifactReleaseReportPublisherVerdict,
    reason: String,
    latest_pointer_updated: bool,
    latest_pointer_overwrite_used: bool,
) -> ArtifactReleasePublishReport {
    report.mode = "publish".to_string();
    report.verdict = verdict;
    report.reason = reason;
    report.latest_pointer_attempted = true;
    report.latest_pointer_updated = latest_pointer_updated;
    report.latest_pointer_overwrite_used = latest_pointer_overwrite_used;
    report.verification_attempted = true;
    report
}

fn build_report_from_pointer_metadata(
    mode: &str,
    verdict: ArtifactReleaseReportPublisherVerdict,
    reason: String,
    config: &Config,
    metadata_path: &Path,
    metadata: &ReleaseLatestPointerMetadata,
    missing_paths: Vec<String>,
    inconsistencies: Vec<String>,
    verification_attempted: bool,
) -> ArtifactReleasePublishReport {
    ArtifactReleasePublishReport {
        mode: mode.to_string(),
        verdict,
        reason,
        release_archive_dir: config.release_archive_dir.display().to_string(),
        persisted_release_artifact_path: Some(metadata.selected_release_artifact_path.clone()),
        persisted_release_artifact_exists: Path::new(&metadata.selected_release_artifact_path).exists(),
        persisted_release_artifact_file_name: Some(metadata.selected_release_artifact_file_name.clone()),
        release_verdict: Some(metadata.release_verdict.clone()),
        release_reason: Some(metadata.release_reason.clone()),
        generation_id: metadata.selected_generation_id.clone(),
        released_at: metadata.released_at,
        released_at_source: Some(metadata.released_at_source),
        compat_loaded_without_released_at: metadata.compat_loaded_without_released_at,
        deterministic_timestamp_available: metadata.deterministic_timestamp_available,
        ordered_history_confident: metadata.ordered_history_confident,
        latest_pointer_attempted: false,
        latest_pointer_updated: false,
        latest_pointer_dir: config
            .latest_pointer_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        latest_pointer_name: config.pointer_name.clone(),
        latest_pointer_path: Some(metadata_path.display().to_string()),
        latest_pointer_source_release_archive_dir: Some(metadata.source_release_archive_dir.clone()),
        latest_pointer_pointed_at: Some(metadata.pointed_at),
        latest_pointer_exists: metadata_path.exists(),
        latest_pointer_overwrite_used: false,
        latest_pointer_target_exists: Path::new(&metadata.selected_release_artifact_path).exists(),
        latest_pointer_target_matches_identity: missing_paths.is_empty() && inconsistencies.is_empty(),
        verification_attempted,
        missing_paths,
        inconsistencies,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary:
            "Artifact/release management only persists and verifies release artifacts. It does not authorize activation, does not enable execution, and does not override the Stage 3 production gate.".to_string(),
    }
}

fn release_artifact_file_name(
    report: &activation_artifact_release::ArtifactReleaseReport,
) -> String {
    let released_at = if report.released_at.timestamp_subsec_nanos() == 0 {
        report
            .released_at
            .to_rfc3339_opts(SecondsFormat::Secs, true)
    } else {
        report
            .released_at
            .to_rfc3339_opts(SecondsFormat::Nanos, true)
    };
    format!(
        "release__{}__{}.json",
        released_at.replace(':', "-"),
        serialize_enum(&report.verdict)
    )
}

fn latest_pointer_path(latest_pointer_dir: &Path, pointer_name: &str) -> PathBuf {
    latest_pointer_dir.join(format!("{pointer_name}.json"))
}

fn canonicalize_release_archive_dir(release_archive_dir: &Path) -> Result<PathBuf> {
    fs::canonicalize(release_archive_dir).with_context(|| {
        format!(
            "failed canonicalizing release archive dir {}",
            release_archive_dir.display()
        )
    })
}

fn canonicalize_stored_release_archive_dir(stored_release_archive_dir: &str) -> Result<PathBuf> {
    fs::canonicalize(Path::new(stored_release_archive_dir)).with_context(|| {
        format!(
            "failed canonicalizing stored release archive dir {}",
            stored_release_archive_dir
        )
    })
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

fn render_human(report: &ArtifactReleasePublishReport) -> String {
    [
        "event=copybot_activation_artifact_release_publish_report".to_string(),
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("release_archive_dir={}", report.release_archive_dir),
        format!(
            "persisted_release_artifact_path={}",
            report
                .persisted_release_artifact_path
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "release_verdict={}",
            report
                .release_verdict
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "generation_id={}",
            report
                .generation_id
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "released_at={}",
            report
                .released_at
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "released_at_source={}",
            report
                .released_at_source
                .map(|value| serialize_enum(&value))
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "compat_loaded_without_released_at={}",
            report.compat_loaded_without_released_at
        ),
        format!(
            "ordered_history_confident={}",
            report.ordered_history_confident
        ),
        format!(
            "latest_pointer_attempted={}",
            report.latest_pointer_attempted
        ),
        format!("latest_pointer_updated={}", report.latest_pointer_updated),
        format!(
            "latest_pointer_path={}",
            report
                .latest_pointer_path
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "latest_pointer_source_release_archive_dir={}",
            report
                .latest_pointer_source_release_archive_dir
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "latest_pointer_pointed_at={}",
            report
                .latest_pointer_pointed_at
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "latest_pointer_target_matches_identity={}",
            report.latest_pointer_target_matches_identity
        ),
        format!("verification_attempted={}", report.verification_attempted),
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

    #[test]
    fn publish_one_release_artifact_into_deterministic_archive_layout() {
        let release_archive_dir = temp_dir("release_report_archive_publish");
        let report = sample_release_report("2026-03-26T12:00:00Z", "artifact_release_published");

        let persisted =
            persist_release_artifact(&release_archive_dir, &report).expect("persisted release");

        assert!(persisted.path.exists());
        assert_eq!(
            persisted
                .path
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or_default(),
            "release__2026-03-26T12-00-00Z__artifact_release_published.json"
        );
    }

    #[test]
    fn collision_does_not_overwrite_existing_release_artifact_silently() {
        let release_archive_dir = temp_dir("release_report_archive_collision");
        let report = sample_release_report("2026-03-26T12:00:00Z", "artifact_release_published");

        persist_release_artifact(&release_archive_dir, &report).expect("first persist");
        let error = persist_release_artifact(&release_archive_dir, &report).expect_err("collision");

        assert!(format!("{error:#}").contains("refusing to overwrite existing release artifact"));
    }

    #[test]
    fn latest_pointer_update_works_when_requested() {
        let release_archive_dir = temp_dir("release_report_archive_pointer");
        let latest_pointer_dir = temp_dir("release_report_pointer_dir");
        let report = sample_release_report(
            "2026-03-26T12:00:00Z",
            "artifact_release_published_and_promoted",
        );
        let persisted =
            persist_release_artifact(&release_archive_dir, &report).expect("persisted release");
        let config = sample_pointer_config(
            release_archive_dir.clone(),
            Some(latest_pointer_dir.clone()),
            Mode::ReportLatest,
        );

        let pointer_path = write_latest_pointer(&config, &persisted, false).expect("pointer path");

        assert!(pointer_path.exists());
        assert_eq!(
            pointer_path,
            latest_pointer_path(&latest_pointer_dir, DEFAULT_LATEST_POINTER_NAME)
        );
    }

    #[test]
    fn verify_mode_passes_for_consistent_latest_pointer() {
        let release_archive_dir = temp_dir("release_report_verify_ok_archive");
        let latest_pointer_dir = temp_dir("release_report_verify_ok_pointer");
        let report = sample_release_report("2026-03-26T12:00:00Z", "artifact_release_published");
        let persisted =
            persist_release_artifact(&release_archive_dir, &report).expect("persisted release");
        let config = sample_pointer_config(
            release_archive_dir.clone(),
            Some(latest_pointer_dir.clone()),
            Mode::VerifyLatest,
        );
        write_latest_pointer(&config, &persisted, false).expect("write pointer");

        let verify = inspect_latest_pointer(&config, true).expect("verify");

        assert_eq!(
            verify.verdict,
            ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportVerifyOk
        );
        assert!(verify.latest_pointer_target_matches_identity);
    }

    #[test]
    fn relative_archive_path_pointer_verifies_when_checked_via_absolute_path() {
        let cwd = std::env::current_dir().expect("cwd");
        let release_archive_abs = cwd.join(format!(
            "target/release_report_relative_archive_{}",
            unique_suffix()
        ));
        let release_archive_rel = release_archive_abs
            .strip_prefix(&cwd)
            .expect("relative archive path")
            .to_path_buf();
        let latest_pointer_dir = temp_dir("release_report_relative_pointer");
        fs::create_dir_all(&release_archive_abs).expect("create absolute archive dir");
        let report = sample_release_report("2026-03-26T12:00:00Z", "artifact_release_published");
        let persisted =
            persist_release_artifact(&release_archive_abs, &report).expect("persisted release");

        let relative_config = sample_pointer_config(
            release_archive_rel,
            Some(latest_pointer_dir.clone()),
            Mode::ReportLatest,
        );
        write_latest_pointer(&relative_config, &persisted, false).expect("write pointer");

        let absolute_verify_config = sample_pointer_config(
            release_archive_abs.clone(),
            Some(latest_pointer_dir.clone()),
            Mode::VerifyLatest,
        );
        let verify = inspect_latest_pointer(&absolute_verify_config, true).expect("verify");

        assert_eq!(
            verify.verdict,
            ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportVerifyOk
        );
        assert!(verify.latest_pointer_target_matches_identity);
        fs::remove_dir_all(&release_archive_abs).expect("cleanup relative archive dir");
    }

    #[test]
    fn verify_mode_fails_for_missing_target() {
        let release_archive_dir = temp_dir("release_report_verify_missing_archive");
        let latest_pointer_dir = temp_dir("release_report_verify_missing_pointer");
        let pointer_path = latest_pointer_path(&latest_pointer_dir, DEFAULT_LATEST_POINTER_NAME);
        fs::create_dir_all(&latest_pointer_dir).expect("pointer dir");
        fs::write(
            &pointer_path,
            serde_json::to_string_pretty(&sample_pointer_metadata(
                &release_archive_dir,
                "/tmp/missing_release.json",
                None,
                activation_artifact_release_history::ReleaseTimestampSource::MissingDeterministicTimestamp,
            ))
            .expect("serialize pointer metadata"),
        )
        .expect("write pointer metadata");
        let config = sample_pointer_config(
            release_archive_dir.clone(),
            Some(latest_pointer_dir.clone()),
            Mode::VerifyLatest,
        );

        let verify = inspect_latest_pointer(&config, true).expect("verify");

        assert_eq!(
            verify.verdict,
            ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportVerifyMissingTarget
        );
    }

    #[test]
    fn mismatched_truly_different_archive_dirs_still_fail() {
        let release_archive_a = temp_dir("release_report_archive_a");
        let release_archive_b = temp_dir("release_report_archive_b");
        let latest_pointer_dir = temp_dir("release_report_pointer_mismatch");
        let report = sample_release_report("2026-03-26T12:00:00Z", "artifact_release_published");
        let persisted =
            persist_release_artifact(&release_archive_a, &report).expect("persisted release");
        let write_config = sample_pointer_config(
            release_archive_a.clone(),
            Some(latest_pointer_dir.clone()),
            Mode::ReportLatest,
        );
        write_latest_pointer(&write_config, &persisted, false).expect("write pointer");

        let verify_config = sample_pointer_config(
            release_archive_b.clone(),
            Some(latest_pointer_dir.clone()),
            Mode::VerifyLatest,
        );
        let verify = inspect_latest_pointer(&verify_config, true).expect("verify");

        assert_eq!(
            verify.verdict,
            ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportInvalidMetadata
        );
        assert!(!verify.latest_pointer_target_matches_identity);
    }

    #[test]
    fn compat_ambiguous_timestamp_case_is_surfaced_honestly() {
        let release_archive_dir = temp_dir("release_report_ambiguous_archive");
        let latest_pointer_dir = temp_dir("release_report_ambiguous_pointer");
        let release_path = release_archive_dir.join("legacy_release.json");
        write_json(
            &release_path,
            &legacy_release_json_without_deterministic_timestamp(),
        );
        let pointer_path = latest_pointer_path(&latest_pointer_dir, DEFAULT_LATEST_POINTER_NAME);
        fs::create_dir_all(&latest_pointer_dir).expect("pointer dir");
        fs::write(
            &pointer_path,
            serde_json::to_string_pretty(&sample_pointer_metadata(
                &release_archive_dir,
                &fs::canonicalize(&release_path)
                    .expect("canonical release path")
                    .display()
                    .to_string(),
                None,
                activation_artifact_release_history::ReleaseTimestampSource::MissingDeterministicTimestamp,
            ))
            .expect("serialize pointer metadata"),
        )
        .expect("write pointer metadata");
        let config = sample_pointer_config(
            release_archive_dir.clone(),
            Some(latest_pointer_dir.clone()),
            Mode::VerifyLatest,
        );

        let verify = inspect_latest_pointer(&config, true).expect("verify");

        assert_eq!(
            verify.verdict,
            ArtifactReleaseReportPublisherVerdict::ArtifactReleaseReportVerifyAmbiguousTimestamp
        );
        assert!(!verify.ordered_history_confident);
        assert!(verify.compat_loaded_without_released_at);
    }

    fn sample_pointer_config(
        release_archive_dir: PathBuf,
        latest_pointer_dir: Option<PathBuf>,
        mode: Mode,
    ) -> Config {
        Config {
            release_archive_dir,
            latest_pointer_dir,
            pointer_name: DEFAULT_LATEST_POINTER_NAME.to_string(),
            json: false,
            mode,
        }
    }

    fn sample_release_report(
        released_at: &str,
        verdict: &str,
    ) -> activation_artifact_release::ArtifactReleaseReport {
        activation_artifact_release::ArtifactReleaseReport {
            mode: "artifact_release".to_string(),
            released_at: DateTime::parse_from_rfc3339(released_at)
                .expect("released_at")
                .with_timezone(&Utc),
            verdict: sample_release_verdict(verdict),
            reason: format!("release {verdict}"),
            config_path: "/etc/solana-copy-bot/live.server.toml".to_string(),
            non_prod_config_path: "/etc/solana-copy-bot/devnet.server.toml".to_string(),
            archive_dir: "/var/www/solana-copy-bot/state/activation_artifacts/archive".to_string(),
            generation_id: Some("2026-03-26T12:00:00Z|prodhash|nonprodhash".to_string()),
            generation_directory: Some(
                "/var/www/solana-copy-bot/state/activation_artifacts/archive/2026-03-26T12-00-00Z__prodhash__nonprodhash"
                    .to_string(),
            ),
            packet_path: Some(
                "/var/www/solana-copy-bot/state/activation_artifacts/archive/generation/decision_packet.json"
                    .to_string(),
            ),
            runbook_json_path: Some(
                "/var/www/solana-copy-bot/state/activation_artifacts/archive/generation/activation_runbook.json"
                    .to_string(),
            ),
            runbook_markdown_path: Some(
                "/var/www/solana-copy-bot/state/activation_artifacts/archive/generation/activation_runbook.md"
                    .to_string(),
            ),
            manifest_path: Some(
                "/var/www/solana-copy-bot/state/activation_artifacts/archive_manifest/latest.json"
                    .to_string(),
            ),
            bundle_path: Some(
                "/var/www/solana-copy-bot/state/activation_artifacts/bundles/review-2026-03-26T12-00-00Z"
                    .to_string(),
            ),
            decision_packet_verdict: Some(
                "decision_packet_discussion_ready_but_not_authorized".to_string(),
            ),
            checklist_verdict: Some(
                "activation_checklist_discussion_ready_but_not_authorized".to_string(),
            ),
            publish_verdict: Some("artifact_publish_succeeded".to_string()),
            publish_reason: Some("publish ok".to_string()),
            generation_published: true,
            manifest_generated: true,
            bundle_exported: true,
            channel_promotion_attempted: true,
            channel_promotion_happened: verdict == "artifact_release_published_and_promoted",
            channel_verify_attempted: verdict == "artifact_release_published_and_promoted",
            channel_name: Some("current_review".to_string()),
            channel_dir: Some(
                "/var/www/solana-copy-bot/state/activation_artifacts/channel".to_string(),
            ),
            channel_metadata_path: Some(
                "/var/www/solana-copy-bot/state/activation_artifacts/channel/current_review.json"
                    .to_string(),
            ),
            channel_promote_verdict: Some("artifact_channel_promoted".to_string()),
            channel_promote_reason: Some("promoted".to_string()),
            channel_verify_verdict: Some("artifact_channel_ok".to_string()),
            channel_verify_reason: Some("verify ok".to_string()),
            execution_untouched: true,
            activation_authorized: false,
            not_authorized_summary: "artifact-only".to_string(),
        }
    }

    fn sample_release_verdict(
        verdict: &str,
    ) -> activation_artifact_release::ArtifactReleaseVerdict {
        match verdict {
            "artifact_release_published" => {
                activation_artifact_release::ArtifactReleaseVerdict::ArtifactReleasePublished
            }
            "artifact_release_published_and_promoted" => activation_artifact_release::ArtifactReleaseVerdict::ArtifactReleasePublishedAndPromoted,
            "artifact_release_publish_failed" => activation_artifact_release::ArtifactReleaseVerdict::ArtifactReleasePublishFailed,
            "artifact_release_channel_promote_blocked" => activation_artifact_release::ArtifactReleaseVerdict::ArtifactReleaseChannelPromoteBlocked,
            "artifact_release_failed" => {
                activation_artifact_release::ArtifactReleaseVerdict::ArtifactReleaseFailed
            }
            other => panic!("unexpected sample verdict {other}"),
        }
    }

    fn sample_pointer_metadata(
        release_archive_dir: &Path,
        release_artifact_path: &str,
        released_at: Option<DateTime<Utc>>,
        released_at_source: activation_artifact_release_history::ReleaseTimestampSource,
    ) -> ReleaseLatestPointerMetadata {
        ReleaseLatestPointerMetadata {
            pointer_version: RELEASE_POINTER_VERSION.to_string(),
            pointer_name: DEFAULT_LATEST_POINTER_NAME.to_string(),
            source_release_archive_dir: release_archive_dir.display().to_string(),
            selected_release_artifact_path: release_artifact_path.to_string(),
            selected_release_artifact_file_name: Path::new(release_artifact_path)
                .file_name()
                .map(|value| value.to_string_lossy().into_owned())
                .unwrap_or_else(|| "legacy_release.json".to_string()),
            release_artifact_mode: "artifact_release".to_string(),
            released_at,
            released_at_source,
            compat_loaded_without_released_at: released_at_source
                != activation_artifact_release_history::ReleaseTimestampSource::ReleasedAt,
            deterministic_timestamp_available: released_at.is_some(),
            ordered_history_confident: released_at.is_some(),
            release_verdict: "artifact_release_published".to_string(),
            release_reason: "legacy release".to_string(),
            selected_generation_id: None,
            channel_promotion_happened: false,
            channel_metadata_path: None,
            pointed_at: DateTime::parse_from_rfc3339("2026-03-26T12:10:00Z")
                .expect("pointed_at")
                .with_timezone(&Utc),
            build_version: env!("CARGO_PKG_VERSION").to_string(),
            git_commit: Some("deadbeef".to_string()),
        }
    }

    fn legacy_release_json_without_deterministic_timestamp() -> serde_json::Value {
        json!({
            "mode": "artifact_release",
            "verdict": "artifact_release_published",
            "reason": "legacy release",
            "config_path": "/etc/solana-copy-bot/live.server.toml",
            "non_prod_config_path": "/etc/solana-copy-bot/devnet.server.toml",
            "archive_dir": "/var/www/solana-copy-bot/state/activation_artifacts/archive",
            "generation_id": null,
            "generation_directory": null,
            "packet_path": null,
            "runbook_json_path": null,
            "runbook_markdown_path": null,
            "manifest_path": null,
            "bundle_path": null,
            "decision_packet_verdict": null,
            "checklist_verdict": null,
            "publish_verdict": "artifact_publish_succeeded",
            "publish_reason": "publish ok",
            "generation_published": false,
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
        })
    }

    fn write_json(path: &Path, value: &serde_json::Value) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create parent dir");
        }
        fs::write(
            path,
            serde_json::to_string_pretty(value).expect("serialize json"),
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
