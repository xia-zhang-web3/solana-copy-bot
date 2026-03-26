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
#[path = "copybot_activation_artifact_state_report.rs"]
mod activation_artifact_state_report;

const USAGE: &str = "usage: copybot_activation_artifact_state_publish_report --state-archive-dir <path> [--json] [--pointer-name <name>] (--publish [state-report args...] [--persist-latest-pointer --snapshot-latest-pointer-dir <path> [--allow-latest-pointer-overwrite]] | --report-latest --snapshot-latest-pointer-dir <path> | --verify-latest --snapshot-latest-pointer-dir <path>)";
const SNAPSHOT_VERSION: &str = "1";
const SNAPSHOT_POINTER_VERSION: &str = "1";
pub(crate) const DEFAULT_LATEST_POINTER_NAME: &str = "latest_artifact_state";

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
    state_archive_dir: PathBuf,
    latest_pointer_dir: Option<PathBuf>,
    pointer_name: String,
    json: bool,
    mode: Mode,
}

#[derive(Debug, Clone)]
enum Mode {
    Publish {
        state_config: activation_artifact_state_report::Config,
        persist_latest_pointer: bool,
        allow_latest_pointer_overwrite: bool,
    },
    ReportLatest,
    VerifyLatest,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ArtifactStateSnapshotPublisherVerdict {
    ArtifactStateSnapshotPublished,
    ArtifactStateSnapshotPublishedAndPointedLatest,
    ArtifactStateSnapshotPointerBlocked,
    ArtifactStateSnapshotFailed,
    ArtifactStateSnapshotVerifyOk,
    ArtifactStateSnapshotVerifyMissingTarget,
    ArtifactStateSnapshotInvalidMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ArtifactStateSnapshotArtifact {
    pub(crate) mode: String,
    pub(crate) snapshot_version: String,
    pub(crate) snapshotted_at: DateTime<Utc>,
    pub(crate) build_version: String,
    pub(crate) git_commit: Option<String>,
    pub(crate) state_verdict: String,
    pub(crate) state_reason: String,
    pub(crate) review_archive_dir: String,
    pub(crate) review_manifest_dir: String,
    pub(crate) review_bundle_dir: String,
    pub(crate) review_channel_dir: String,
    pub(crate) review_channel_name: String,
    pub(crate) release_archive_dir: String,
    pub(crate) release_history_dir: String,
    pub(crate) latest_release_pointer_dir: String,
    pub(crate) latest_release_pointer_name: String,
    pub(crate) selected_review_generation_id: Option<String>,
    pub(crate) selected_latest_release_generation_id: Option<String>,
    pub(crate) selection_alignment_matches: bool,
    pub(crate) selection_alignment_summary: String,
    pub(crate) review_provenance_verdict: String,
    pub(crate) review_provenance_reason: String,
    pub(crate) release_provenance_verdict: String,
    pub(crate) release_provenance_reason: String,
    pub(crate) linkage_verdict: String,
    pub(crate) linkage_reason: String,
    pub(crate) ambiguous_legacy_count: usize,
    pub(crate) coherent_for_review_operations: bool,
    pub(crate) artifact_state_only: bool,
    pub(crate) execution_untouched: bool,
    pub(crate) activation_authorized: bool,
    pub(crate) not_authorized_summary: String,
    pub(crate) state_report: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StateLatestPointerMetadata {
    pointer_version: String,
    pointer_name: String,
    source_state_archive_dir: String,
    selected_snapshot_path: String,
    selected_snapshot_file_name: String,
    snapshot_mode: String,
    snapshotted_at: DateTime<Utc>,
    snapshot_verdict: String,
    snapshot_reason: String,
    selected_review_generation_id: Option<String>,
    selected_latest_release_generation_id: Option<String>,
    selection_alignment_matches: bool,
    coherent_for_review_operations: bool,
    ambiguous_legacy_count: usize,
    pointed_at: DateTime<Utc>,
    build_version: String,
    git_commit: Option<String>,
}

#[derive(Debug, Clone)]
struct PersistedStateSnapshot {
    path: PathBuf,
    canonical_path: PathBuf,
}

#[derive(Debug, Clone)]
pub(crate) struct LoadedStateSnapshot {
    #[allow(dead_code)]
    pub(crate) path: PathBuf,
    #[allow(dead_code)]
    pub(crate) canonical_path: PathBuf,
    pub(crate) artifact: ArtifactStateSnapshotArtifact,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ArtifactStateSnapshotPublishReport {
    pub(crate) mode: String,
    pub(crate) verdict: ArtifactStateSnapshotPublisherVerdict,
    pub(crate) reason: String,
    pub(crate) state_archive_dir: String,
    pub(crate) persisted_state_snapshot_path: Option<String>,
    pub(crate) persisted_state_snapshot_exists: bool,
    pub(crate) persisted_state_snapshot_file_name: Option<String>,
    pub(crate) snapshotted_at: Option<DateTime<Utc>>,
    pub(crate) state_verdict: Option<String>,
    pub(crate) state_reason: Option<String>,
    pub(crate) selected_review_generation_id: Option<String>,
    pub(crate) selected_latest_release_generation_id: Option<String>,
    pub(crate) selection_alignment_matches: Option<bool>,
    pub(crate) selection_alignment_summary: Option<String>,
    pub(crate) latest_pointer_attempted: bool,
    pub(crate) latest_pointer_updated: bool,
    pub(crate) latest_pointer_dir: Option<String>,
    pub(crate) latest_pointer_name: String,
    pub(crate) latest_pointer_path: Option<String>,
    pub(crate) latest_pointer_source_state_archive_dir: Option<String>,
    pub(crate) latest_pointer_pointed_at: Option<DateTime<Utc>>,
    pub(crate) latest_pointer_exists: bool,
    pub(crate) latest_pointer_overwrite_used: bool,
    pub(crate) latest_pointer_target_exists: bool,
    pub(crate) latest_pointer_target_matches_identity: bool,
    pub(crate) verification_attempted: bool,
    pub(crate) missing_paths: Vec<String>,
    pub(crate) inconsistencies: Vec<String>,
    pub(crate) artifact_state_only: bool,
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
    let mut state_archive_dir: Option<PathBuf> = None;
    let mut latest_pointer_dir: Option<PathBuf> = None;
    let mut pointer_name = DEFAULT_LATEST_POINTER_NAME.to_string();
    let mut json = false;
    let mut publish = false;
    let mut report_latest = false;
    let mut verify_latest = false;
    let mut persist_latest_pointer = false;
    let mut allow_latest_pointer_overwrite = false;
    let mut forwarded_state_args = Vec::new();

    let mut index = 0usize;
    while index < args.len() {
        let arg = args[index].as_str();
        match arg {
            "--state-archive-dir" => {
                index += 1;
                state_archive_dir = Some(PathBuf::from(parse_required_value(
                    "--state-archive-dir",
                    args.get(index),
                )?));
            }
            "--snapshot-latest-pointer-dir" => {
                index += 1;
                latest_pointer_dir = Some(PathBuf::from(parse_required_value(
                    "--snapshot-latest-pointer-dir",
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
                forwarded_state_args.push(other.to_string());
                if state_arg_takes_value(other) {
                    index += 1;
                    forwarded_state_args.push(parse_required_value(other, args.get(index))?);
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

    let state_archive_dir =
        state_archive_dir.ok_or_else(|| anyhow!("missing required --state-archive-dir"))?;

    let mode = if publish {
        if report_latest || verify_latest {
            bail!("--publish cannot be combined with --report-latest or --verify-latest");
        }
        if persist_latest_pointer && latest_pointer_dir.is_none() {
            bail!("--persist-latest-pointer requires --snapshot-latest-pointer-dir");
        }
        if allow_latest_pointer_overwrite && !persist_latest_pointer {
            bail!("--allow-latest-pointer-overwrite requires --persist-latest-pointer");
        }
        let Some(state_config) =
            activation_artifact_state_report::parse_args_from(forwarded_state_args.into_iter())?
        else {
            return Ok(None);
        };
        Mode::Publish {
            state_config,
            persist_latest_pointer,
            allow_latest_pointer_overwrite,
        }
    } else {
        if !forwarded_state_args.is_empty() {
            bail!(
                "state report arguments are only allowed with --publish; unexpected extra args: {}",
                forwarded_state_args.join(" ")
            );
        }
        latest_pointer_dir
            .as_ref()
            .ok_or_else(|| anyhow!("missing required --snapshot-latest-pointer-dir"))?
            .components()
            .next()
            .ok_or_else(|| anyhow!("--snapshot-latest-pointer-dir cannot be empty"))?;
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
        state_archive_dir,
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

fn state_arg_takes_value(flag: &str) -> bool {
    matches!(
        flag,
        "--review-archive-dir"
            | "--review-manifest-dir"
            | "--review-bundle-dir"
            | "--review-channel-dir"
            | "--review-channel-name"
            | "--release-archive-dir"
            | "--release-history-dir"
            | "--latest-pointer-dir"
            | "--latest-pointer-name"
    )
}

fn run(config: Config) -> Result<String> {
    let report = match &config.mode {
        Mode::Publish {
            state_config,
            persist_latest_pointer,
            allow_latest_pointer_overwrite,
        } => {
            let state_report =
                activation_artifact_state_report::inspect_state_report(state_config)?;
            publish_state_snapshot_report(
                &config,
                &state_report,
                Utc::now(),
                resolve_git_commit(),
                *persist_latest_pointer,
                *allow_latest_pointer_overwrite,
            )
        }
        Mode::ReportLatest => inspect_latest_pointer(&config, false),
        Mode::VerifyLatest => inspect_latest_pointer(&config, true),
    }?;

    if config.json {
        serde_json::to_string_pretty(&report)
            .context("failed serializing activation artifact state publisher json")
    } else {
        Ok(render_human(&report))
    }
}

fn publish_state_snapshot_report(
    config: &Config,
    state_report: &activation_artifact_state_report::ArtifactStateReport,
    snapshotted_at: DateTime<Utc>,
    git_commit: Option<String>,
    persist_latest_pointer: bool,
    allow_latest_pointer_overwrite: bool,
) -> Result<ArtifactStateSnapshotPublishReport> {
    let snapshot_artifact = build_snapshot_artifact(state_report, snapshotted_at, git_commit)?;
    let persisted =
        match persist_state_snapshot_artifact(&config.state_archive_dir, &snapshot_artifact) {
            Ok(persisted) => persisted,
            Err(error) => {
                return Ok(build_report(
                    "publish",
                    ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotFailed,
                    format!("failed to persist activation artifact state snapshot: {error:#}"),
                    config,
                    Some(&snapshot_artifact),
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
            ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotPublished,
            format!(
                "persisted activation artifact state snapshot at {}",
                persisted.path.display()
            ),
            config,
            Some(&snapshot_artifact),
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
                ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotVerifyOk => (
                    ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotPublishedAndPointedLatest,
                    format!(
                        "persisted activation artifact state snapshot and updated latest pointer {}",
                        pointer_path.display()
                    ),
                ),
                ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotVerifyMissingTarget
                | ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotInvalidMetadata
                | ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotFailed
                | ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotPointerBlocked
                | ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotPublished
                | ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotPublishedAndPointedLatest => (
                    ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotPointerBlocked,
                    format!(
                        "state snapshot was persisted, but latest pointer verification did not complete cleanly: {}",
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
            ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotPointerBlocked,
            format!(
                "state snapshot was persisted, but latest pointer update did not complete cleanly: {error:#}"
            ),
            config,
            Some(&snapshot_artifact),
            Some(&persisted),
            true,
            false,
            allow_latest_pointer_overwrite,
            Vec::new(),
            Vec::new(),
        )),
    }
}

fn build_snapshot_artifact(
    report: &activation_artifact_state_report::ArtifactStateReport,
    snapshotted_at: DateTime<Utc>,
    git_commit: Option<String>,
) -> Result<ArtifactStateSnapshotArtifact> {
    Ok(ArtifactStateSnapshotArtifact {
        mode: "artifact_state_snapshot".to_string(),
        snapshot_version: SNAPSHOT_VERSION.to_string(),
        snapshotted_at,
        build_version: env!("CARGO_PKG_VERSION").to_string(),
        git_commit,
        state_verdict: serialize_enum(&report.verdict),
        state_reason: report.reason.clone(),
        review_archive_dir: report.review_archive_dir.clone(),
        review_manifest_dir: report.review_manifest_dir.clone(),
        review_bundle_dir: report.review_bundle_dir.clone(),
        review_channel_dir: report.review_channel_dir.clone(),
        review_channel_name: report.review_channel_name.clone(),
        release_archive_dir: report.release_archive_dir.clone(),
        release_history_dir: report.release_history_dir.clone(),
        latest_release_pointer_dir: report.latest_pointer_dir.clone(),
        latest_release_pointer_name: report.latest_pointer_name.clone(),
        selected_review_generation_id: report.selected_review_generation_id().map(str::to_string),
        selected_latest_release_generation_id: report
            .selected_latest_release_generation_id()
            .map(str::to_string),
        selection_alignment_matches: report.selection_alignment.selections_match,
        selection_alignment_summary: report.selection_alignment.summary.clone(),
        review_provenance_verdict: report.review_provenance.verdict.clone(),
        review_provenance_reason: report.review_provenance.reason.clone(),
        release_provenance_verdict: report.release_provenance.verdict.clone(),
        release_provenance_reason: report.release_provenance.reason.clone(),
        linkage_verdict: report.linkage.verdict.clone(),
        linkage_reason: report.linkage.reason.clone(),
        ambiguous_legacy_count: report.ambiguous_legacy_count,
        coherent_for_review_operations: report.coherent_for_review_operations,
        artifact_state_only: report.artifact_state_only,
        execution_untouched: report.execution_untouched,
        activation_authorized: report.activation_authorized,
        not_authorized_summary: report.not_authorized_summary.clone(),
        state_report: serde_json::to_value(report)
            .context("failed serializing nested artifact state report json")?,
    })
}

fn persist_state_snapshot_artifact(
    state_archive_dir: &Path,
    snapshot: &ArtifactStateSnapshotArtifact,
) -> Result<PersistedStateSnapshot> {
    fs::create_dir_all(state_archive_dir).with_context(|| {
        format!(
            "failed creating state snapshot archive dir {}",
            state_archive_dir.display()
        )
    })?;

    let final_path = state_archive_dir.join(state_snapshot_file_name(snapshot));
    if final_path.exists() {
        bail!(
            "refusing to overwrite existing state snapshot {}",
            final_path.display()
        );
    }

    let stage_path = state_archive_dir.join(format!(".state_snapshot.staging_{}", unique_suffix()));
    fs::write(
        &stage_path,
        serde_json::to_string_pretty(snapshot)
            .context("failed serializing activation artifact state snapshot json")?,
    )
    .with_context(|| {
        format!(
            "failed writing staged state snapshot artifact {}",
            stage_path.display()
        )
    })?;
    fs::rename(&stage_path, &final_path).with_context(|| {
        format!(
            "failed promoting staged state snapshot artifact {} to {}",
            stage_path.display(),
            final_path.display()
        )
    })?;

    let canonical_path = fs::canonicalize(&final_path).with_context(|| {
        format!(
            "failed canonicalizing persisted state snapshot {}",
            final_path.display()
        )
    })?;

    Ok(PersistedStateSnapshot {
        path: final_path,
        canonical_path,
    })
}

fn write_latest_pointer(
    config: &Config,
    persisted: &PersistedStateSnapshot,
    allow_overwrite: bool,
) -> Result<PathBuf> {
    let latest_pointer_dir = config
        .latest_pointer_dir
        .as_ref()
        .ok_or_else(|| anyhow!("snapshot latest pointer dir is required for pointer updates"))?;
    let metadata_path = latest_pointer_path(latest_pointer_dir, &config.pointer_name);
    if metadata_path.exists() && !allow_overwrite {
        bail!(
            "snapshot latest pointer {} already exists; rerun with --allow-latest-pointer-overwrite to replace it",
            metadata_path.display()
        );
    }

    let inspected = inspect_state_snapshot_artifact(&persisted.path).with_context(|| {
        format!(
            "failed reloading persisted state snapshot {} for latest pointer metadata",
            persisted.path.display()
        )
    })?;
    let canonical_state_archive_dir = canonicalize_state_archive_dir(&config.state_archive_dir)
        .with_context(|| {
            format!(
                "failed canonicalizing state snapshot archive dir {} for latest pointer metadata",
                config.state_archive_dir.display()
            )
        })?;
    let metadata = StateLatestPointerMetadata {
        pointer_version: SNAPSHOT_POINTER_VERSION.to_string(),
        pointer_name: config.pointer_name.clone(),
        source_state_archive_dir: canonical_state_archive_dir.display().to_string(),
        selected_snapshot_path: persisted.canonical_path.display().to_string(),
        selected_snapshot_file_name: persisted
            .path
            .file_name()
            .map(|value| value.to_string_lossy().into_owned())
            .unwrap_or_else(|| "artifact_state_snapshot.json".to_string()),
        snapshot_mode: inspected.artifact.mode.clone(),
        snapshotted_at: inspected.artifact.snapshotted_at,
        snapshot_verdict: inspected.artifact.state_verdict.clone(),
        snapshot_reason: inspected.artifact.state_reason.clone(),
        selected_review_generation_id: inspected.artifact.selected_review_generation_id.clone(),
        selected_latest_release_generation_id: inspected
            .artifact
            .selected_latest_release_generation_id
            .clone(),
        selection_alignment_matches: inspected.artifact.selection_alignment_matches,
        coherent_for_review_operations: inspected.artifact.coherent_for_review_operations,
        ambiguous_legacy_count: inspected.artifact.ambiguous_legacy_count,
        pointed_at: Utc::now(),
        build_version: env!("CARGO_PKG_VERSION").to_string(),
        git_commit: resolve_git_commit(),
    };

    fs::create_dir_all(latest_pointer_dir).with_context(|| {
        format!(
            "failed creating snapshot latest pointer dir {}",
            latest_pointer_dir.display()
        )
    })?;
    let stage_path = latest_pointer_dir.join(format!(
        ".state_snapshot_pointer.staging_{}",
        unique_suffix()
    ));
    fs::write(
        &stage_path,
        serde_json::to_string_pretty(&metadata)
            .context("failed serializing snapshot latest pointer metadata json")?,
    )
    .with_context(|| {
        format!(
            "failed writing staged snapshot latest pointer metadata {}",
            stage_path.display()
        )
    })?;
    if metadata_path.exists() && allow_overwrite {
        fs::remove_file(&metadata_path).with_context(|| {
            format!(
                "failed removing existing snapshot latest pointer metadata {}",
                metadata_path.display()
            )
        })?;
    }
    fs::rename(&stage_path, &metadata_path).with_context(|| {
        format!(
            "failed promoting staged snapshot latest pointer metadata {} to {}",
            stage_path.display(),
            metadata_path.display()
        )
    })?;
    Ok(metadata_path)
}

fn inspect_latest_pointer(
    config: &Config,
    verification_attempted: bool,
) -> Result<ArtifactStateSnapshotPublishReport> {
    let latest_pointer_dir = config
        .latest_pointer_dir
        .as_ref()
        .ok_or_else(|| anyhow!("snapshot latest pointer dir is required for report/verify"))?;
    let metadata_path = latest_pointer_path(latest_pointer_dir, &config.pointer_name);
    if !metadata_path.exists() {
        return Ok(build_report(
            if verification_attempted {
                "verify_latest"
            } else {
                "report_latest"
            },
            ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotVerifyMissingTarget,
            format!(
                "no latest state snapshot pointer metadata found at {}",
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
                ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotInvalidMetadata,
                format!("failed reading snapshot latest pointer metadata: {error}"),
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

    let metadata: StateLatestPointerMetadata = match serde_json::from_str(&raw) {
        Ok(metadata) => metadata,
        Err(error) => {
            return Ok(build_report(
                if verification_attempted {
                    "verify_latest"
                } else {
                    "report_latest"
                },
                ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotInvalidMetadata,
                format!("failed parsing snapshot latest pointer metadata json: {error}"),
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
        match canonicalize_state_archive_dir(&config.state_archive_dir) {
            Ok(path) => Some(path),
            Err(error) => {
                inconsistencies.push(format!(
                    "failed canonicalizing requested state snapshot archive {}: {error:#}",
                    config.state_archive_dir.display()
                ));
                None
            }
        };
    let canonical_stored_archive_root = match canonicalize_stored_state_archive_dir(
        &metadata.source_state_archive_dir,
    ) {
        Ok(path) => Some(path),
        Err(error) => {
            inconsistencies.push(format!(
                    "snapshot latest pointer source_state_archive_dir `{}` cannot be canonicalized deterministically: {error:#}",
                    metadata.source_state_archive_dir
                ));
            None
        }
    };

    if metadata.pointer_version != SNAPSHOT_POINTER_VERSION {
        inconsistencies.push(format!(
            "unsupported snapshot latest pointer metadata version `{}`; expected `{SNAPSHOT_POINTER_VERSION}`",
            metadata.pointer_version
        ));
    }
    if metadata.pointer_name != config.pointer_name {
        inconsistencies.push(format!(
            "snapshot latest pointer metadata name `{}` does not match requested pointer `{}`",
            metadata.pointer_name, config.pointer_name
        ));
    }
    if let (Some(stored), Some(requested)) = (
        canonical_stored_archive_root.as_ref(),
        canonical_requested_archive_root.as_ref(),
    ) {
        if stored != requested {
            inconsistencies.push(format!(
                "snapshot latest pointer metadata source archive `{}` does not match requested state snapshot archive `{}`",
                stored.display(),
                requested.display()
            ));
        }
    }

    let target_path = PathBuf::from(&metadata.selected_snapshot_path);
    if !target_path.is_absolute() {
        inconsistencies.push(format!(
            "snapshot latest pointer target `{}` is not absolute; relative targets are unsupported",
            metadata.selected_snapshot_path
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
                            "snapshot latest pointer target {} is outside state snapshot archive {}",
                            canonical_target.display(),
                            canonical_archive_root.display()
                        ));
                        None
                    } else {
                        match inspect_state_snapshot_artifact(&canonical_target) {
                            Ok(snapshot) => Some(snapshot),
                            Err(error) => {
                                inconsistencies.push(format!(
                                    "snapshot latest pointer target is not a valid state snapshot artifact: {error:#}"
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
                    "failed canonicalizing snapshot latest pointer target {}: {error}",
                    target_path.display()
                ));
                None
            }
        }
    } else {
        None
    };

    if let Some(inspected) = inspected_target.as_ref() {
        if inspected.artifact.mode != metadata.snapshot_mode {
            inconsistencies.push(format!(
                "snapshot latest pointer recorded mode `{}` but target artifact mode is `{}`",
                metadata.snapshot_mode, inspected.artifact.mode
            ));
        }
        if inspected.artifact.snapshotted_at != metadata.snapshotted_at {
            inconsistencies.push(
                "snapshot latest pointer snapshotted_at does not match the target snapshot"
                    .to_string(),
            );
        }
        if inspected.artifact.state_verdict != metadata.snapshot_verdict {
            inconsistencies.push(
                "snapshot latest pointer snapshot_verdict does not match the target snapshot"
                    .to_string(),
            );
        }
        if inspected.artifact.state_reason != metadata.snapshot_reason {
            inconsistencies.push(
                "snapshot latest pointer snapshot_reason does not match the target snapshot"
                    .to_string(),
            );
        }
        if inspected.artifact.selected_review_generation_id
            != metadata.selected_review_generation_id
        {
            inconsistencies.push(
                "snapshot latest pointer selected_review_generation_id does not match the target snapshot"
                    .to_string(),
            );
        }
        if inspected.artifact.selected_latest_release_generation_id
            != metadata.selected_latest_release_generation_id
        {
            inconsistencies.push(
                "snapshot latest pointer selected_latest_release_generation_id does not match the target snapshot"
                    .to_string(),
            );
        }
        if inspected.artifact.selection_alignment_matches != metadata.selection_alignment_matches {
            inconsistencies.push(
                "snapshot latest pointer selection_alignment_matches does not match the target snapshot"
                    .to_string(),
            );
        }
        if inspected.artifact.coherent_for_review_operations
            != metadata.coherent_for_review_operations
        {
            inconsistencies.push(
                "snapshot latest pointer coherent_for_review_operations does not match the target snapshot"
                    .to_string(),
            );
        }
        if inspected.artifact.ambiguous_legacy_count != metadata.ambiguous_legacy_count {
            inconsistencies.push(
                "snapshot latest pointer ambiguous_legacy_count does not match the target snapshot"
                    .to_string(),
            );
        }
    }

    let (verdict, reason) = if !missing_paths.is_empty() {
        (
            ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotVerifyMissingTarget,
            "snapshot latest pointer exists, but its target snapshot artifact is missing"
                .to_string(),
        )
    } else if !inconsistencies.is_empty() {
        (
            ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotInvalidMetadata,
            "snapshot latest pointer metadata exists, but it is inconsistent with the target snapshot artifact".to_string(),
        )
    } else {
        (
            ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotVerifyOk,
            format!(
                "snapshot latest pointer `{}` resolves to a consistent persisted state snapshot artifact",
                config.pointer_name
            ),
        )
    };

    let persisted_target = inspected_target.as_ref().map(|_| PersistedStateSnapshot {
        path: target_path.clone(),
        canonical_path: fs::canonicalize(&target_path).unwrap_or_else(|_| target_path.clone()),
    });

    Ok(build_report(
        if verification_attempted {
            "verify_latest"
        } else {
            "report_latest"
        },
        verdict,
        reason,
        config,
        inspected_target.as_ref().map(|value| &value.artifact),
        persisted_target.as_ref(),
        true,
        false,
        false,
        missing_paths,
        inconsistencies,
    )
    .with_pointer_metadata(Some(&metadata), true))
}

pub(crate) fn inspect_latest_pointer_report(
    state_archive_dir: &Path,
    latest_pointer_dir: &Path,
    pointer_name: &str,
    verification_attempted: bool,
) -> Result<ArtifactStateSnapshotPublishReport> {
    inspect_latest_pointer(
        &Config {
            state_archive_dir: state_archive_dir.to_path_buf(),
            latest_pointer_dir: Some(latest_pointer_dir.to_path_buf()),
            pointer_name: pointer_name.to_string(),
            json: false,
            mode: if verification_attempted {
                Mode::VerifyLatest
            } else {
                Mode::ReportLatest
            },
        },
        verification_attempted,
    )
}

pub(crate) fn inspect_state_snapshot_artifact(path: &Path) -> Result<LoadedStateSnapshot> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed reading state snapshot artifact {}", path.display()))?;
    let artifact: ArtifactStateSnapshotArtifact =
        serde_json::from_str(&raw).with_context(|| {
            format!(
                "failed parsing state snapshot artifact json {}",
                path.display()
            )
        })?;
    if artifact.mode != "artifact_state_snapshot" {
        bail!(
            "artifact {} has mode `{}` instead of `artifact_state_snapshot`",
            path.display(),
            artifact.mode
        );
    }
    if artifact.snapshot_version != SNAPSHOT_VERSION {
        bail!(
            "artifact {} has snapshot_version `{}` instead of `{SNAPSHOT_VERSION}`",
            path.display(),
            artifact.snapshot_version
        );
    }
    let canonical_path = fs::canonicalize(path).with_context(|| {
        format!(
            "failed canonicalizing state snapshot artifact path {}",
            path.display()
        )
    })?;
    Ok(LoadedStateSnapshot {
        path: path.to_path_buf(),
        canonical_path,
        artifact,
    })
}

fn latest_pointer_path(latest_pointer_dir: &Path, pointer_name: &str) -> PathBuf {
    latest_pointer_dir.join(format!("{pointer_name}.json"))
}

fn state_snapshot_file_name(snapshot: &ArtifactStateSnapshotArtifact) -> String {
    let snapshotted_at = if snapshot.snapshotted_at.timestamp_subsec_nanos() == 0 {
        snapshot
            .snapshotted_at
            .to_rfc3339_opts(SecondsFormat::Secs, true)
    } else {
        snapshot
            .snapshotted_at
            .to_rfc3339_opts(SecondsFormat::Nanos, true)
    };
    format!(
        "state_snapshot__{}__{}.json",
        snapshotted_at.replace(':', "-"),
        snapshot.state_verdict
    )
}

fn canonicalize_state_archive_dir(state_archive_dir: &Path) -> Result<PathBuf> {
    fs::canonicalize(state_archive_dir).with_context(|| {
        format!(
            "failed canonicalizing state snapshot archive dir {}",
            state_archive_dir.display()
        )
    })
}

fn canonicalize_stored_state_archive_dir(stored_state_archive_dir: &str) -> Result<PathBuf> {
    fs::canonicalize(Path::new(stored_state_archive_dir)).with_context(|| {
        format!(
            "failed canonicalizing stored state snapshot archive dir {}",
            stored_state_archive_dir
        )
    })
}

fn build_report(
    mode: &str,
    verdict: ArtifactStateSnapshotPublisherVerdict,
    reason: String,
    config: &Config,
    snapshot: Option<&ArtifactStateSnapshotArtifact>,
    persisted: Option<&PersistedStateSnapshot>,
    latest_pointer_attempted: bool,
    latest_pointer_updated: bool,
    latest_pointer_overwrite_used: bool,
    missing_paths: Vec<String>,
    inconsistencies: Vec<String>,
) -> ArtifactStateSnapshotPublishReport {
    ArtifactStateSnapshotPublishReport {
        mode: mode.to_string(),
        verdict,
        reason,
        state_archive_dir: config.state_archive_dir.display().to_string(),
        persisted_state_snapshot_path: persisted.map(|value| value.path.display().to_string()),
        persisted_state_snapshot_exists: persisted.is_some(),
        persisted_state_snapshot_file_name: persisted.and_then(|value| {
            value.path
                .file_name()
                .map(|entry| entry.to_string_lossy().into_owned())
        }),
        snapshotted_at: snapshot.map(|value| value.snapshotted_at),
        state_verdict: snapshot.map(|value| value.state_verdict.clone()),
        state_reason: snapshot.map(|value| value.state_reason.clone()),
        selected_review_generation_id: snapshot
            .and_then(|value| value.selected_review_generation_id.clone()),
        selected_latest_release_generation_id: snapshot
            .and_then(|value| value.selected_latest_release_generation_id.clone()),
        selection_alignment_matches: snapshot.map(|value| value.selection_alignment_matches),
        selection_alignment_summary: snapshot.map(|value| value.selection_alignment_summary.clone()),
        latest_pointer_attempted,
        latest_pointer_updated,
        latest_pointer_dir: config
            .latest_pointer_dir
            .as_ref()
            .map(|value| value.display().to_string()),
        latest_pointer_name: config.pointer_name.clone(),
        latest_pointer_path: None,
        latest_pointer_source_state_archive_dir: None,
        latest_pointer_pointed_at: None,
        latest_pointer_exists: false,
        latest_pointer_overwrite_used,
        latest_pointer_target_exists: false,
        latest_pointer_target_matches_identity: false,
        verification_attempted: false,
        missing_paths,
        inconsistencies,
        artifact_state_only: snapshot.is_none_or(|value| value.artifact_state_only),
        execution_untouched: snapshot.is_none_or(|value| value.execution_untouched),
        activation_authorized: snapshot.is_some_and(|value| value.activation_authorized),
        not_authorized_summary: snapshot
            .map(|value| value.not_authorized_summary.clone())
            .unwrap_or_else(|| {
                "Artifact-state management only persists and verifies state snapshots. It does not authorize activation, does not enable execution, and does not override the Stage 3 production gate.".to_string()
            }),
    }
}

impl ArtifactStateSnapshotPublishReport {
    fn with_pointer_metadata(
        mut self,
        metadata: Option<&StateLatestPointerMetadata>,
        verification_attempted: bool,
    ) -> Self {
        if let Some(metadata) = metadata {
            self.latest_pointer_path = self.latest_pointer_dir.as_ref().map(|dir| {
                latest_pointer_path(Path::new(dir), &self.latest_pointer_name)
                    .display()
                    .to_string()
            });
            self.latest_pointer_source_state_archive_dir =
                Some(metadata.source_state_archive_dir.clone());
            self.latest_pointer_pointed_at = Some(metadata.pointed_at);
            self.latest_pointer_exists = true;
            self.latest_pointer_target_exists = self
                .persisted_state_snapshot_path
                .as_ref()
                .is_some_and(|path| Path::new(path).exists());
            self.latest_pointer_target_matches_identity = self.snapshotted_at
                == Some(metadata.snapshotted_at)
                && self.state_verdict.as_deref() == Some(metadata.snapshot_verdict.as_str())
                && self.selected_review_generation_id.as_deref()
                    == metadata.selected_review_generation_id.as_deref()
                && self.selected_latest_release_generation_id.as_deref()
                    == metadata.selected_latest_release_generation_id.as_deref();
        }
        self.verification_attempted = verification_attempted;
        self
    }
}

fn finalize_publish_pointer_report(
    mut report: ArtifactStateSnapshotPublishReport,
    verdict: ArtifactStateSnapshotPublisherVerdict,
    reason: String,
    latest_pointer_updated: bool,
    latest_pointer_overwrite_used: bool,
) -> ArtifactStateSnapshotPublishReport {
    report.mode = "publish".to_string();
    report.verdict = verdict;
    report.reason = reason;
    report.latest_pointer_attempted = true;
    report.latest_pointer_updated = latest_pointer_updated;
    report.latest_pointer_overwrite_used = latest_pointer_overwrite_used;
    report
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

fn render_human(report: &ArtifactStateSnapshotPublishReport) -> String {
    [
        "event=copybot_activation_artifact_state_publish_report".to_string(),
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("state_archive_dir={}", report.state_archive_dir),
        format!(
            "persisted_state_snapshot_path={}",
            report
                .persisted_state_snapshot_path
                .clone()
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!(
            "state_verdict={}",
            report
                .state_verdict
                .clone()
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!(
            "selected_review_generation_id={}",
            report
                .selected_review_generation_id
                .clone()
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!(
            "selected_latest_release_generation_id={}",
            report
                .selected_latest_release_generation_id
                .clone()
                .unwrap_or_else(|| "<none>".to_string())
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
                .unwrap_or_else(|| "<none>".to_string())
        ),
        format!("verification_attempted={}", report.verification_attempted),
        format!("missing_paths={}", report.missing_paths.join(",")),
        format!("inconsistencies={}", report.inconsistencies.join(",")),
        format!("artifact_state_only={}", report.artifact_state_only),
        format!("execution_untouched={}", report.execution_untouched),
        format!("activation_authorized={}", report.activation_authorized),
        format!("not_authorized_summary={}", report.not_authorized_summary),
    ]
    .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn snapshot_publish_succeeds_into_deterministic_archive_layout() {
        let root = temp_dir("state_snapshot_publish");
        let archive_dir = root.join("archive");
        let report = sample_state_report(
            activation_artifact_state_report::ArtifactStateVerdict::ArtifactStateCoherent,
            "artifact_state_coherent",
        );

        let publish = publish_state_snapshot_report(
            &Config {
                state_archive_dir: archive_dir.clone(),
                latest_pointer_dir: None,
                pointer_name: DEFAULT_LATEST_POINTER_NAME.to_string(),
                json: false,
                mode: Mode::ReportLatest,
            },
            &report,
            ts("2026-03-26T18:00:00Z"),
            Some("deadbeef".to_string()),
            false,
            false,
        )
        .expect("publish");

        assert_eq!(
            publish.verdict,
            ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotPublished
        );
        let persisted_path = publish
            .persisted_state_snapshot_path
            .as_ref()
            .expect("persisted path");
        assert!(Path::new(persisted_path).exists());
        assert!(persisted_path
            .contains("state_snapshot__2026-03-26T18-00-00Z__artifact_state_coherent.json"));
    }

    #[test]
    fn collision_does_not_overwrite_existing_snapshot_silently() {
        let root = temp_dir("state_snapshot_collision");
        let archive_dir = root.join("archive");
        let report = sample_state_report(
            activation_artifact_state_report::ArtifactStateVerdict::ArtifactStateCoherent,
            "artifact_state_coherent",
        );
        let snapshot = build_snapshot_artifact(
            &report,
            ts("2026-03-26T18:00:00Z"),
            Some("deadbeef".to_string()),
        )
        .expect("snapshot");

        persist_state_snapshot_artifact(&archive_dir, &snapshot).expect("first persist");
        let error =
            persist_state_snapshot_artifact(&archive_dir, &snapshot).expect_err("collision");
        assert!(format!("{error:#}").contains("refusing to overwrite existing state snapshot"));
    }

    #[test]
    fn latest_pointer_verify_passes_for_consistent_snapshot_target() {
        let root = temp_dir("state_snapshot_pointer_ok");
        let archive_dir = root.join("archive");
        let pointer_dir = root.join("pointer");
        let report = sample_state_report(
            activation_artifact_state_report::ArtifactStateVerdict::ArtifactStateCoherent,
            "artifact_state_coherent",
        );
        let publish = publish_state_snapshot_report(
            &Config {
                state_archive_dir: archive_dir.clone(),
                latest_pointer_dir: Some(pointer_dir.clone()),
                pointer_name: DEFAULT_LATEST_POINTER_NAME.to_string(),
                json: false,
                mode: Mode::ReportLatest,
            },
            &report,
            ts("2026-03-26T18:00:00Z"),
            Some("deadbeef".to_string()),
            true,
            false,
        )
        .expect("publish");

        assert_eq!(
            publish.verdict,
            ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotPublishedAndPointedLatest
        );

        let verify = inspect_latest_pointer_report(
            &archive_dir,
            &pointer_dir,
            DEFAULT_LATEST_POINTER_NAME,
            true,
        )
        .expect("verify");
        assert_eq!(
            verify.verdict,
            ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotVerifyOk
        );
    }

    #[test]
    fn latest_pointer_verify_fails_for_missing_or_invalid_snapshot_target() {
        let root = temp_dir("state_snapshot_pointer_missing");
        let archive_dir = root.join("archive");
        let pointer_dir = root.join("pointer");
        let report = sample_state_report(
            activation_artifact_state_report::ArtifactStateVerdict::ArtifactStateCoherent,
            "artifact_state_coherent",
        );
        let publish = publish_state_snapshot_report(
            &Config {
                state_archive_dir: archive_dir.clone(),
                latest_pointer_dir: Some(pointer_dir.clone()),
                pointer_name: DEFAULT_LATEST_POINTER_NAME.to_string(),
                json: false,
                mode: Mode::ReportLatest,
            },
            &report,
            ts("2026-03-26T18:00:00Z"),
            Some("deadbeef".to_string()),
            true,
            false,
        )
        .expect("publish");
        let persisted_path = PathBuf::from(
            publish
                .persisted_state_snapshot_path
                .as_ref()
                .expect("persisted path"),
        );
        fs::remove_file(&persisted_path).expect("remove snapshot");

        let verify = inspect_latest_pointer_report(
            &archive_dir,
            &pointer_dir,
            DEFAULT_LATEST_POINTER_NAME,
            true,
        )
        .expect("verify");
        assert_eq!(
            verify.verdict,
            ArtifactStateSnapshotPublisherVerdict::ArtifactStateSnapshotVerifyMissingTarget
        );
    }

    fn temp_dir(prefix: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "{}_{}_{}",
            prefix,
            std::process::id(),
            unique_suffix()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn ts(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .expect("timestamp")
            .with_timezone(&Utc)
    }

    fn sample_state_report(
        verdict: activation_artifact_state_report::ArtifactStateVerdict,
        verdict_name: &str,
    ) -> activation_artifact_state_report::ArtifactStateReport {
        activation_artifact_state_report::ArtifactStateReport {
            mode: "artifact_state_report".to_string(),
            verdict,
            reason: format!("sample {verdict_name}"),
            review_archive_dir: "/tmp/review_archive".to_string(),
            review_manifest_dir: "/tmp/review_manifest".to_string(),
            review_bundle_dir: "/tmp/review_bundle".to_string(),
            review_channel_dir: "/tmp/review_channel".to_string(),
            review_channel_name: "current_review".to_string(),
            release_archive_dir: "/tmp/release_archive".to_string(),
            release_history_dir: "/tmp/release_history".to_string(),
            latest_pointer_dir: "/tmp/release_latest".to_string(),
            latest_pointer_name: "latest_release".to_string(),
            current_review_generation:
                activation_artifact_state_report::CurrentReviewGenerationSummary {
                    channel_verdict: "artifact_channel_ok".to_string(),
                    channel_reason: "ok".to_string(),
                    channel_metadata_path: "/tmp/review_channel/current_review.json".to_string(),
                    selected_generation_id: Some("review_gen_1".to_string()),
                    decision_packet_generated_at: Some(ts("2026-03-26T17:00:00Z")),
                    prod_config_fingerprint_sha256: Some("prod_fp".to_string()),
                    non_prod_config_fingerprint_sha256: Some("non_prod_fp".to_string()),
                    manifest_verification_verdict: Some("artifact_manifest_verified".to_string()),
                    bundle_verification_verdict: Some("artifact_bundle_verified".to_string()),
                    missing_paths: Vec::new(),
                    inconsistencies: Vec::new(),
                },
            current_latest_release: activation_artifact_state_report::CurrentLatestReleaseSummary {
                pointer_verdict: "artifact_release_report_verify_ok".to_string(),
                pointer_reason: "ok".to_string(),
                latest_pointer_path: Some("/tmp/release_latest/latest_release.json".to_string()),
                persisted_release_artifact_path: Some(
                    "/tmp/release_archive/release.json".to_string(),
                ),
                release_verdict: Some("artifact_release_published_and_promoted".to_string()),
                generation_id: Some("review_gen_1".to_string()),
                released_at: Some(ts("2026-03-26T17:10:00Z")),
                released_at_source: Some("released_at".to_string()),
                compat_loaded_without_released_at: false,
                deterministic_timestamp_available: true,
                ordered_history_confident: true,
                target_exists: true,
                target_matches_identity: true,
                missing_paths: Vec::new(),
                inconsistencies: Vec::new(),
            },
            selection_alignment: activation_artifact_state_report::SelectionAlignmentSummary {
                selections_match: true,
                summary: "review and release selections match".to_string(),
            },
            review_provenance: activation_artifact_state_report::ReviewProvenanceStateSummary {
                verdict: "artifact_provenance_complete".to_string(),
                reason: "complete".to_string(),
                archive_generation_count: 1,
                manifest_file_count: 1,
                bundle_manifest_count: 1,
                complete_generation_count: 1,
                incomplete_generation_count: 0,
                invalid_artifact_count: 0,
            },
            release_provenance: activation_artifact_state_report::ReleaseProvenanceStateSummary {
                verdict: "artifact_release_provenance_complete".to_string(),
                reason: "complete".to_string(),
                archive_release_count: 1,
                history_release_count: 1,
                latest_pointer_present: true,
                latest_pointer_selected_generation_id: Some("review_gen_1".to_string()),
                latest_pointer_relation: "matches_latest_archive_and_history".to_string(),
                latest_archive_generation_id: Some("review_gen_1".to_string()),
                latest_history_generation_id: Some("review_gen_1".to_string()),
                archive_releases_missing_from_history_count: 0,
                history_releases_missing_from_archive_count: 0,
                invalid_artifact_count: 0,
                ambiguous_timestamp_count: 0,
            },
            linkage: activation_artifact_state_report::LinkageStateSummary {
                verdict: "artifact_linkage_complete".to_string(),
                reason: "complete".to_string(),
                release_artifact_count_examined: 1,
                linked_generation_count: 1,
                missing_generation_ref_count: 0,
                missing_packet_ref_count: 0,
                missing_runbook_ref_count: 0,
                invalid_artifact_count: 0,
                ambiguous_legacy_reference_count: 0,
                latest_selected_release_linkage_verdict: Some("complete".to_string()),
                latest_selected_release_linkage_reason: Some("ok".to_string()),
                latest_selected_generation_id: Some("review_gen_1".to_string()),
                latest_target_exists: true,
                latest_target_matches_identity: true,
                latest_linked_generation_present: true,
                review_channel_selected_generation_id: Some("review_gen_1".to_string()),
                review_channel_matches_latest_selection: true,
                review_channel_divergence_summary: None,
            },
            ambiguous_legacy_count: if verdict_name.contains("ambiguous") {
                1
            } else {
                0
            },
            coherent_for_review_operations: verdict
                == activation_artifact_state_report::ArtifactStateVerdict::ArtifactStateCoherent,
            artifact_state_only: true,
            execution_untouched: true,
            activation_authorized: false,
            not_authorized_summary: "Artifact-state analysis only; not activation authorization."
                .to_string(),
        }
    }
}
