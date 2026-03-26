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
#[path = "copybot_activation_artifact_state_bundle.rs"]
mod activation_artifact_state_bundle;

const USAGE: &str = "usage: copybot_activation_artifact_state_bundle_publish_report --bundle-archive-dir <path> [--json] [--pointer-name <name>] (--publish --state-archive-dir <path> --snapshot <path|file-name|snapshotted-at> [--persist-latest-pointer --bundle-latest-pointer-dir <path> [--allow-latest-pointer-overwrite]] | --report-latest --bundle-latest-pointer-dir <path> | --verify-latest --bundle-latest-pointer-dir <path>)";
const BUNDLE_POINTER_VERSION: &str = "1";
const DEFAULT_BUNDLE_POINTER_NAME: &str = "latest_state_snapshot_bundle";

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
    latest_pointer_dir: Option<PathBuf>,
    pointer_name: String,
    json: bool,
    mode: Mode,
}

#[derive(Debug, Clone)]
enum Mode {
    Publish {
        state_archive_dir: PathBuf,
        snapshot_selector: String,
        persist_latest_pointer: bool,
        allow_latest_pointer_overwrite: bool,
    },
    ReportLatest,
    VerifyLatest,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ArtifactStateBundleReportVerdict {
    ArtifactStateBundleReportPublished,
    ArtifactStateBundleReportPublishedAndPointedLatest,
    ArtifactStateBundleReportPointerBlocked,
    ArtifactStateBundleReportFailed,
    ArtifactStateBundleReportVerifyOk,
    ArtifactStateBundleReportVerifyMissingTarget,
    ArtifactStateBundleReportInvalidMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StateBundleLatestPointerMetadata {
    pointer_version: String,
    pointer_name: String,
    source_bundle_archive_dir: String,
    selected_bundle_path: String,
    selected_bundle_dir_name: String,
    source_state_archive_dir: String,
    selected_snapshot_source_path: String,
    selected_snapshot_file_name: String,
    snapshotted_at: DateTime<Utc>,
    snapshot_state_verdict: String,
    snapshot_state_reason: String,
    selected_review_generation_id: Option<String>,
    selected_latest_release_generation_id: Option<String>,
    ambiguous_legacy_count: usize,
    coherent_for_review_operations: bool,
    pointed_at: DateTime<Utc>,
    build_version: String,
    git_commit: Option<String>,
}

#[derive(Debug, Clone)]
struct PersistedStateSnapshotBundle {
    path: PathBuf,
    canonical_path: PathBuf,
    inspection: activation_artifact_state_bundle::StateSnapshotBundleSummary,
}

#[derive(Debug, Clone, Serialize)]
struct ArtifactStateBundlePublishReport {
    mode: String,
    verdict: ArtifactStateBundleReportVerdict,
    reason: String,
    state_archive_dir: Option<String>,
    bundle_archive_dir: String,
    persisted_bundle_path: Option<String>,
    persisted_bundle_exists: bool,
    persisted_bundle_dir_name: Option<String>,
    bundle_integrity_verdict: Option<String>,
    bundle_integrity_reason: Option<String>,
    source_state_archive_dir: Option<String>,
    selected_snapshot_path: Option<String>,
    selected_snapshot_file_name: Option<String>,
    snapshotted_at: Option<DateTime<Utc>>,
    selected_snapshot_state_verdict: Option<String>,
    selected_snapshot_state_reason: Option<String>,
    selected_review_generation_id: Option<String>,
    selected_latest_release_generation_id: Option<String>,
    ambiguous_legacy_count: Option<usize>,
    coherent_for_review_operations: Option<bool>,
    latest_pointer_attempted: bool,
    latest_pointer_updated: bool,
    latest_pointer_dir: Option<String>,
    latest_pointer_name: String,
    latest_pointer_path: Option<String>,
    latest_pointer_source_bundle_archive_dir: Option<String>,
    latest_pointer_pointed_at: Option<DateTime<Utc>>,
    latest_pointer_exists: bool,
    latest_pointer_overwrite_used: bool,
    latest_pointer_target_exists: bool,
    latest_pointer_target_matches_identity: bool,
    verification_attempted: bool,
    missing_paths: Vec<String>,
    inconsistencies: Vec<String>,
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
    let args = args.into_iter().collect::<Vec<_>>();
    let mut bundle_archive_dir: Option<PathBuf> = None;
    let mut latest_pointer_dir: Option<PathBuf> = None;
    let mut pointer_name = DEFAULT_BUNDLE_POINTER_NAME.to_string();
    let mut state_archive_dir: Option<PathBuf> = None;
    let mut snapshot_selector: Option<String> = None;
    let mut json = false;
    let mut publish = false;
    let mut report_latest = false;
    let mut verify_latest = false;
    let mut persist_latest_pointer = false;
    let mut allow_latest_pointer_overwrite = false;

    let mut index = 0usize;
    while index < args.len() {
        match args[index].as_str() {
            "--bundle-archive-dir" => {
                index += 1;
                bundle_archive_dir = Some(PathBuf::from(parse_required_value(
                    "--bundle-archive-dir",
                    args.get(index),
                )?));
            }
            "--bundle-latest-pointer-dir" => {
                index += 1;
                latest_pointer_dir = Some(PathBuf::from(parse_required_value(
                    "--bundle-latest-pointer-dir",
                    args.get(index),
                )?));
            }
            "--pointer-name" => {
                index += 1;
                pointer_name =
                    parse_pointer_name(parse_required_value("--pointer-name", args.get(index))?)?;
            }
            "--state-archive-dir" => {
                index += 1;
                state_archive_dir = Some(PathBuf::from(parse_required_value(
                    "--state-archive-dir",
                    args.get(index),
                )?));
            }
            "--snapshot" => {
                index += 1;
                snapshot_selector = Some(parse_required_value("--snapshot", args.get(index))?);
            }
            "--json" => json = true,
            "--publish" => publish = true,
            "--report-latest" => report_latest = true,
            "--verify-latest" => verify_latest = true,
            "--persist-latest-pointer" => persist_latest_pointer = true,
            "--allow-latest-pointer-overwrite" => allow_latest_pointer_overwrite = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unrecognized arg `{other}`"),
        }
        index += 1;
    }

    let bundle_archive_dir =
        bundle_archive_dir.ok_or_else(|| anyhow!("--bundle-archive-dir <path> is required"))?;

    let mode_count = publish as usize + report_latest as usize + verify_latest as usize;
    if mode_count != 1 {
        bail!("choose exactly one of --publish, --report-latest, or --verify-latest");
    }

    if persist_latest_pointer && !publish {
        bail!("--persist-latest-pointer only applies to --publish");
    }
    if allow_latest_pointer_overwrite && !persist_latest_pointer {
        bail!("--allow-latest-pointer-overwrite requires --persist-latest-pointer");
    }
    if persist_latest_pointer && latest_pointer_dir.is_none() {
        bail!("--persist-latest-pointer requires --bundle-latest-pointer-dir <path>");
    }
    if (report_latest || verify_latest) && latest_pointer_dir.is_none() {
        bail!("--report-latest/--verify-latest require --bundle-latest-pointer-dir <path>");
    }

    let mode = if publish {
        let state_archive_dir = state_archive_dir
            .ok_or_else(|| anyhow!("--publish requires --state-archive-dir <path>"))?;
        let snapshot_selector =
            snapshot_selector.ok_or_else(|| anyhow!("--publish requires --snapshot <selector>"))?;
        Mode::Publish {
            state_archive_dir,
            snapshot_selector,
            persist_latest_pointer,
            allow_latest_pointer_overwrite,
        }
    } else if report_latest {
        if state_archive_dir.is_some() {
            bail!("--state-archive-dir only applies to --publish");
        }
        if snapshot_selector.is_some() {
            bail!("--snapshot only applies to --publish");
        }
        Mode::ReportLatest
    } else {
        if state_archive_dir.is_some() {
            bail!("--state-archive-dir only applies to --publish");
        }
        if snapshot_selector.is_some() {
            bail!("--snapshot only applies to --publish");
        }
        Mode::VerifyLatest
    };

    Ok(Some(Config {
        bundle_archive_dir,
        latest_pointer_dir,
        pointer_name,
        json,
        mode,
    }))
}

fn parse_required_value(flag: &str, value: Option<&String>) -> Result<String> {
    let value = value.ok_or_else(|| anyhow!("{flag} requires a value"))?;
    let trimmed = value.trim();
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

fn run(config: Config) -> Result<String> {
    let report = match &config.mode {
        Mode::Publish {
            state_archive_dir,
            snapshot_selector,
            persist_latest_pointer,
            allow_latest_pointer_overwrite,
        } => publish_bundle_report(
            &config,
            state_archive_dir,
            snapshot_selector,
            *persist_latest_pointer,
            *allow_latest_pointer_overwrite,
        ),
        Mode::ReportLatest => inspect_latest_pointer(&config, false),
        Mode::VerifyLatest => inspect_latest_pointer(&config, true),
    }?;

    if config.json {
        serde_json::to_string_pretty(&report)
            .context("failed serializing activation artifact state bundle publisher json")
    } else {
        Ok(render_human(&report))
    }
}

fn publish_bundle_report(
    config: &Config,
    state_archive_dir: &Path,
    snapshot_selector: &str,
    persist_latest_pointer: bool,
    allow_latest_pointer_overwrite: bool,
) -> Result<ArtifactStateBundlePublishReport> {
    let persisted =
        match persist_state_snapshot_bundle(config, state_archive_dir, snapshot_selector) {
            Ok(persisted) => persisted,
            Err(error) => {
                return Ok(build_report(
                    "publish",
                    ArtifactStateBundleReportVerdict::ArtifactStateBundleReportFailed,
                    format!("failed to publish archived state snapshot bundle: {error:#}"),
                    config,
                    Some(state_archive_dir),
                    None,
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
            ArtifactStateBundleReportVerdict::ArtifactStateBundleReportPublished,
            format!(
                "published archived state snapshot bundle at {}",
                persisted.path.display()
            ),
            config,
            Some(state_archive_dir),
            Some(&persisted),
            None,
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
                ArtifactStateBundleReportVerdict::ArtifactStateBundleReportVerifyOk => (
                    ArtifactStateBundleReportVerdict::ArtifactStateBundleReportPublishedAndPointedLatest,
                    format!(
                        "published archived state snapshot bundle and updated latest pointer {}",
                        pointer_path.display()
                    ),
                ),
                ArtifactStateBundleReportVerdict::ArtifactStateBundleReportVerifyMissingTarget
                | ArtifactStateBundleReportVerdict::ArtifactStateBundleReportInvalidMetadata
                | ArtifactStateBundleReportVerdict::ArtifactStateBundleReportPointerBlocked
                | ArtifactStateBundleReportVerdict::ArtifactStateBundleReportFailed
                | ArtifactStateBundleReportVerdict::ArtifactStateBundleReportPublished
                | ArtifactStateBundleReportVerdict::ArtifactStateBundleReportPublishedAndPointedLatest => (
                    ArtifactStateBundleReportVerdict::ArtifactStateBundleReportPointerBlocked,
                    format!(
                        "archived bundle was published, but latest bundle pointer verification did not complete cleanly: {}",
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
            ArtifactStateBundleReportVerdict::ArtifactStateBundleReportPointerBlocked,
            format!(
                "archived bundle was published, but latest bundle pointer update did not complete cleanly: {error:#}"
            ),
            config,
            Some(state_archive_dir),
            Some(&persisted),
            None,
            true,
            false,
            allow_latest_pointer_overwrite,
            Vec::new(),
            Vec::new(),
        )),
    }
}

fn persist_state_snapshot_bundle(
    config: &Config,
    state_archive_dir: &Path,
    snapshot_selector: &str,
) -> Result<PersistedStateSnapshotBundle> {
    fs::create_dir_all(&config.bundle_archive_dir).with_context(|| {
        format!(
            "failed creating state snapshot bundle archive dir {}",
            config.bundle_archive_dir.display()
        )
    })?;

    let stage_path = config.bundle_archive_dir.join(format!(
        ".state_snapshot_bundle_archive.staging_{}",
        unique_suffix()
    ));
    let export_summary = activation_artifact_state_bundle::export_state_snapshot_bundle(
        state_archive_dir,
        snapshot_selector,
        &stage_path,
    )
    .with_context(|| {
        format!(
            "failed exporting state snapshot bundle for selector `{snapshot_selector}` from {}",
            state_archive_dir.display()
        )
    })?;

    if export_summary.verdict != "artifact_state_bundle_exported" {
        cleanup_dir_if_exists(&stage_path);
        bail!(
            "state snapshot bundle export did not complete cleanly: {} ({})",
            export_summary.verdict,
            export_summary.reason
        );
    }

    let selected_snapshot_file_name = export_summary
        .selected_snapshot_file_name
        .as_deref()
        .ok_or_else(|| anyhow!("exported bundle did not include selected_snapshot_file_name"))?;
    let final_path = config
        .bundle_archive_dir
        .join(bundle_directory_name(selected_snapshot_file_name));
    if final_path.exists() {
        cleanup_dir_if_exists(&stage_path);
        bail!(
            "refusing to overwrite existing archived state snapshot bundle {}",
            final_path.display()
        );
    }

    fs::rename(&stage_path, &final_path).with_context(|| {
        format!(
            "failed promoting staged state snapshot bundle {} to {}",
            stage_path.display(),
            final_path.display()
        )
    })?;
    let canonical_path = fs::canonicalize(&final_path).with_context(|| {
        format!(
            "failed canonicalizing archived state snapshot bundle {}",
            final_path.display()
        )
    })?;
    let inspection = activation_artifact_state_bundle::inspect_state_snapshot_bundle(&final_path)
        .with_context(|| {
        format!(
            "failed verifying archived state snapshot bundle {} after publish",
            final_path.display()
        )
    })?;
    if inspection.verdict != "artifact_state_bundle_verified" {
        bail!(
            "archived state snapshot bundle did not verify cleanly after publish: {} ({})",
            inspection.verdict,
            inspection.reason
        );
    }

    Ok(PersistedStateSnapshotBundle {
        path: final_path,
        canonical_path,
        inspection,
    })
}

fn write_latest_pointer(
    config: &Config,
    persisted: &PersistedStateSnapshotBundle,
    allow_overwrite: bool,
) -> Result<PathBuf> {
    let latest_pointer_dir = config
        .latest_pointer_dir
        .as_ref()
        .ok_or_else(|| anyhow!("bundle latest pointer dir is required for pointer updates"))?;
    let metadata_path = latest_pointer_path(latest_pointer_dir, &config.pointer_name);
    if metadata_path.exists() && !allow_overwrite {
        bail!(
            "bundle latest pointer {} already exists; rerun with --allow-latest-pointer-overwrite to replace it",
            metadata_path.display()
        );
    }

    let source_state_archive_dir = persisted
        .inspection
        .source_state_archive_dir
        .clone()
        .ok_or_else(|| anyhow!("verified bundle summary is missing source_state_archive_dir"))?;
    let selected_snapshot_path = persisted
        .inspection
        .selected_snapshot_path
        .clone()
        .ok_or_else(|| anyhow!("verified bundle summary is missing selected_snapshot_path"))?;
    let selected_snapshot_file_name = persisted
        .inspection
        .selected_snapshot_file_name
        .clone()
        .ok_or_else(|| anyhow!("verified bundle summary is missing selected_snapshot_file_name"))?;
    let snapshotted_at = persisted
        .inspection
        .snapshotted_at
        .ok_or_else(|| anyhow!("verified bundle summary is missing snapshotted_at"))?;
    let snapshot_state_verdict = persisted
        .inspection
        .selected_snapshot_state_verdict
        .clone()
        .ok_or_else(|| {
            anyhow!("verified bundle summary is missing selected_snapshot_state_verdict")
        })?;
    let snapshot_state_reason = persisted
        .inspection
        .selected_snapshot_state_reason
        .clone()
        .ok_or_else(|| {
            anyhow!("verified bundle summary is missing selected_snapshot_state_reason")
        })?;
    let ambiguous_legacy_count = persisted
        .inspection
        .ambiguous_legacy_count
        .ok_or_else(|| anyhow!("verified bundle summary is missing ambiguous_legacy_count"))?;
    let coherent_for_review_operations = persisted
        .inspection
        .coherent_for_review_operations
        .ok_or_else(|| {
            anyhow!("verified bundle summary is missing coherent_for_review_operations")
        })?;
    let canonical_bundle_archive_dir =
        canonicalize_bundle_archive_dir(&config.bundle_archive_dir).with_context(|| {
            format!(
                "failed canonicalizing state snapshot bundle archive dir {} for latest pointer metadata",
                config.bundle_archive_dir.display()
            )
        })?;
    let metadata = StateBundleLatestPointerMetadata {
        pointer_version: BUNDLE_POINTER_VERSION.to_string(),
        pointer_name: config.pointer_name.clone(),
        source_bundle_archive_dir: canonical_bundle_archive_dir.display().to_string(),
        selected_bundle_path: persisted.canonical_path.display().to_string(),
        selected_bundle_dir_name: persisted
            .path
            .file_name()
            .map(|value| value.to_string_lossy().into_owned())
            .unwrap_or_else(|| "state_snapshot_bundle".to_string()),
        source_state_archive_dir,
        selected_snapshot_source_path: selected_snapshot_path,
        selected_snapshot_file_name,
        snapshotted_at,
        snapshot_state_verdict,
        snapshot_state_reason,
        selected_review_generation_id: persisted.inspection.selected_review_generation_id.clone(),
        selected_latest_release_generation_id: persisted
            .inspection
            .selected_latest_release_generation_id
            .clone(),
        ambiguous_legacy_count,
        coherent_for_review_operations,
        pointed_at: Utc::now(),
        build_version: env!("CARGO_PKG_VERSION").to_string(),
        git_commit: resolve_git_commit(),
    };

    fs::create_dir_all(latest_pointer_dir).with_context(|| {
        format!(
            "failed creating bundle latest pointer dir {}",
            latest_pointer_dir.display()
        )
    })?;
    let stage_path = latest_pointer_dir.join(format!(
        ".state_snapshot_bundle_pointer.staging_{}",
        unique_suffix()
    ));
    fs::write(
        &stage_path,
        serde_json::to_string_pretty(&metadata)
            .context("failed serializing state snapshot bundle latest pointer metadata json")?,
    )
    .with_context(|| {
        format!(
            "failed writing staged state snapshot bundle latest pointer metadata {}",
            stage_path.display()
        )
    })?;
    if metadata_path.exists() && allow_overwrite {
        fs::remove_file(&metadata_path).with_context(|| {
            format!(
                "failed removing existing state snapshot bundle latest pointer metadata {}",
                metadata_path.display()
            )
        })?;
    }
    fs::rename(&stage_path, &metadata_path).with_context(|| {
        format!(
            "failed promoting staged state snapshot bundle latest pointer metadata {} to {}",
            stage_path.display(),
            metadata_path.display()
        )
    })?;
    Ok(metadata_path)
}

fn inspect_latest_pointer(
    config: &Config,
    verification_attempted: bool,
) -> Result<ArtifactStateBundlePublishReport> {
    let latest_pointer_dir = config
        .latest_pointer_dir
        .as_ref()
        .ok_or_else(|| anyhow!("bundle latest pointer dir is required for report/verify"))?;
    let metadata_path = latest_pointer_path(latest_pointer_dir, &config.pointer_name);
    if !metadata_path.exists() {
        return Ok(build_report(
            if verification_attempted {
                "verify_latest"
            } else {
                "report_latest"
            },
            ArtifactStateBundleReportVerdict::ArtifactStateBundleReportVerifyMissingTarget,
            format!(
                "no latest state snapshot bundle pointer metadata found at {}",
                metadata_path.display()
            ),
            config,
            None,
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
                ArtifactStateBundleReportVerdict::ArtifactStateBundleReportInvalidMetadata,
                format!("failed reading state snapshot bundle latest pointer metadata: {error}"),
                config,
                None,
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

    let metadata: StateBundleLatestPointerMetadata = match serde_json::from_str(&raw) {
        Ok(metadata) => metadata,
        Err(error) => {
            return Ok(build_report(
                if verification_attempted {
                    "verify_latest"
                } else {
                    "report_latest"
                },
                ArtifactStateBundleReportVerdict::ArtifactStateBundleReportInvalidMetadata,
                format!(
                    "failed parsing state snapshot bundle latest pointer metadata json: {error}"
                ),
                config,
                None,
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
        match canonicalize_bundle_archive_dir(&config.bundle_archive_dir) {
            Ok(path) => Some(path),
            Err(error) => {
                inconsistencies.push(format!(
                    "failed canonicalizing requested state snapshot bundle archive {}: {error:#}",
                    config.bundle_archive_dir.display()
                ));
                None
            }
        };
    let canonical_stored_archive_root = match canonicalize_stored_bundle_archive_dir(
        &metadata.source_bundle_archive_dir,
    ) {
        Ok(path) => Some(path),
        Err(error) => {
            inconsistencies.push(format!(
                    "state snapshot bundle latest pointer source_bundle_archive_dir `{}` cannot be canonicalized deterministically: {error:#}",
                    metadata.source_bundle_archive_dir
                ));
            None
        }
    };

    if metadata.pointer_version != BUNDLE_POINTER_VERSION {
        inconsistencies.push(format!(
            "unsupported state snapshot bundle latest pointer metadata version `{}`; expected `{BUNDLE_POINTER_VERSION}`",
            metadata.pointer_version
        ));
    }
    if metadata.pointer_name != config.pointer_name {
        inconsistencies.push(format!(
            "state snapshot bundle latest pointer metadata name `{}` does not match requested pointer `{}`",
            metadata.pointer_name, config.pointer_name
        ));
    }
    if let (Some(stored), Some(requested)) = (
        canonical_stored_archive_root.as_ref(),
        canonical_requested_archive_root.as_ref(),
    ) {
        if stored != requested {
            inconsistencies.push(format!(
                "state snapshot bundle latest pointer metadata source archive `{}` does not match requested bundle archive `{}`",
                stored.display(),
                requested.display()
            ));
        }
    }

    let target_path = PathBuf::from(&metadata.selected_bundle_path);
    if !target_path.is_absolute() {
        inconsistencies.push(format!(
            "state snapshot bundle latest pointer target `{}` is not absolute; relative targets are unsupported",
            metadata.selected_bundle_path
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
                            "state snapshot bundle latest pointer target {} is outside bundle archive {}",
                            canonical_target.display(),
                            canonical_archive_root.display()
                        ));
                        None
                    } else {
                        match activation_artifact_state_bundle::inspect_state_snapshot_bundle(
                            &canonical_target,
                        ) {
                            Ok(bundle) => Some(bundle),
                            Err(error) => {
                                inconsistencies.push(format!(
                                    "state snapshot bundle latest pointer target is not a valid bundle: {error:#}"
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
                    "failed canonicalizing state snapshot bundle latest pointer target {}: {error}",
                    target_path.display()
                ));
                None
            }
        }
    } else {
        None
    };

    if let Some(inspected) = inspected_target.as_ref() {
        if inspected.verdict != "artifact_state_bundle_verified" {
            inconsistencies.push(format!(
                "state snapshot bundle latest pointer target bundle integrity verdict `{}` is not clean",
                inspected.verdict
            ));
        }
        if inspected.bundle_path != target_path.display().to_string()
            && inspected.bundle_path
                != fs::canonicalize(&target_path)
                    .unwrap_or_else(|_| target_path.clone())
                    .display()
                    .to_string()
        {
            inconsistencies.push(
                "state snapshot bundle latest pointer bundle_path does not match the target bundle"
                    .to_string(),
            );
        }
        if Path::new(&metadata.selected_bundle_path)
            .file_name()
            .map(|value| value.to_string_lossy().into_owned())
            != Some(metadata.selected_bundle_dir_name.clone())
        {
            inconsistencies.push(
                "state snapshot bundle latest pointer selected_bundle_dir_name does not match the target bundle path"
                    .to_string(),
            );
        }
        if inspected.source_state_archive_dir.as_deref()
            != Some(metadata.source_state_archive_dir.as_str())
        {
            inconsistencies.push(
                "state snapshot bundle latest pointer source_state_archive_dir does not match the target bundle"
                    .to_string(),
            );
        }
        if inspected.selected_snapshot_path.as_deref()
            != Some(metadata.selected_snapshot_source_path.as_str())
        {
            inconsistencies.push(
                "state snapshot bundle latest pointer selected_snapshot_source_path does not match the target bundle"
                    .to_string(),
            );
        }
        if inspected.selected_snapshot_file_name.as_deref()
            != Some(metadata.selected_snapshot_file_name.as_str())
        {
            inconsistencies.push(
                "state snapshot bundle latest pointer selected_snapshot_file_name does not match the target bundle"
                    .to_string(),
            );
        }
        if inspected.snapshotted_at != Some(metadata.snapshotted_at) {
            inconsistencies.push(
                "state snapshot bundle latest pointer snapshotted_at does not match the target bundle"
                    .to_string(),
            );
        }
        if inspected.selected_snapshot_state_verdict.as_deref()
            != Some(metadata.snapshot_state_verdict.as_str())
        {
            inconsistencies.push(
                "state snapshot bundle latest pointer snapshot_state_verdict does not match the target bundle"
                    .to_string(),
            );
        }
        if inspected.selected_snapshot_state_reason.as_deref()
            != Some(metadata.snapshot_state_reason.as_str())
        {
            inconsistencies.push(
                "state snapshot bundle latest pointer snapshot_state_reason does not match the target bundle"
                    .to_string(),
            );
        }
        if inspected.selected_review_generation_id != metadata.selected_review_generation_id {
            inconsistencies.push(
                "state snapshot bundle latest pointer selected_review_generation_id does not match the target bundle"
                    .to_string(),
            );
        }
        if inspected.selected_latest_release_generation_id
            != metadata.selected_latest_release_generation_id
        {
            inconsistencies.push(
                "state snapshot bundle latest pointer selected_latest_release_generation_id does not match the target bundle"
                    .to_string(),
            );
        }
        if inspected.ambiguous_legacy_count != Some(metadata.ambiguous_legacy_count) {
            inconsistencies.push(
                "state snapshot bundle latest pointer ambiguous_legacy_count does not match the target bundle"
                    .to_string(),
            );
        }
        if inspected.coherent_for_review_operations != Some(metadata.coherent_for_review_operations)
        {
            inconsistencies.push(
                "state snapshot bundle latest pointer coherent_for_review_operations does not match the target bundle"
                    .to_string(),
            );
        }
    }

    let (verdict, reason) = if !missing_paths.is_empty() {
        (
            ArtifactStateBundleReportVerdict::ArtifactStateBundleReportVerifyMissingTarget,
            "state snapshot bundle latest pointer exists, but its target archived bundle is missing"
                .to_string(),
        )
    } else if !inconsistencies.is_empty() {
        (
            ArtifactStateBundleReportVerdict::ArtifactStateBundleReportInvalidMetadata,
            "state snapshot bundle latest pointer metadata exists, but it is inconsistent with the target archived bundle".to_string(),
        )
    } else {
        (
            ArtifactStateBundleReportVerdict::ArtifactStateBundleReportVerifyOk,
            format!(
                "state snapshot bundle latest pointer `{}` resolves to a consistent archived bundle",
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
        inspected_target.as_ref(),
        missing_paths,
        inconsistencies,
        verification_attempted,
    ))
}

fn latest_pointer_path(latest_pointer_dir: &Path, pointer_name: &str) -> PathBuf {
    latest_pointer_dir.join(format!("{pointer_name}.json"))
}

fn build_report(
    mode: &str,
    verdict: ArtifactStateBundleReportVerdict,
    reason: String,
    config: &Config,
    state_archive_dir: Option<&Path>,
    persisted: Option<&PersistedStateSnapshotBundle>,
    metadata: Option<&StateBundleLatestPointerMetadata>,
    latest_pointer_attempted: bool,
    latest_pointer_updated: bool,
    latest_pointer_overwrite_used: bool,
    missing_paths: Vec<String>,
    inconsistencies: Vec<String>,
) -> ArtifactStateBundlePublishReport {
    let (persisted_bundle_path, persisted_bundle_exists, persisted_bundle_dir_name) =
        if let Some(persisted) = persisted {
            (
                Some(persisted.path.display().to_string()),
                true,
                persisted
                    .path
                    .file_name()
                    .map(|value| value.to_string_lossy().into_owned()),
            )
        } else if let Some(metadata) = metadata {
            (
                Some(metadata.selected_bundle_path.clone()),
                Path::new(&metadata.selected_bundle_path).exists(),
                Some(metadata.selected_bundle_dir_name.clone()),
            )
        } else {
            (None, false, None)
        };

    let (
        bundle_integrity_verdict,
        bundle_integrity_reason,
        source_state_archive_dir,
        selected_snapshot_path,
        selected_snapshot_file_name,
        snapshotted_at,
        selected_snapshot_state_verdict,
        selected_snapshot_state_reason,
        selected_review_generation_id,
        selected_latest_release_generation_id,
        ambiguous_legacy_count,
        coherent_for_review_operations,
    ) = if let Some(persisted) = persisted {
        (
            Some(persisted.inspection.verdict.clone()),
            Some(persisted.inspection.reason.clone()),
            persisted.inspection.source_state_archive_dir.clone(),
            persisted.inspection.selected_snapshot_path.clone(),
            persisted.inspection.selected_snapshot_file_name.clone(),
            persisted.inspection.snapshotted_at,
            persisted.inspection.selected_snapshot_state_verdict.clone(),
            persisted.inspection.selected_snapshot_state_reason.clone(),
            persisted.inspection.selected_review_generation_id.clone(),
            persisted
                .inspection
                .selected_latest_release_generation_id
                .clone(),
            persisted.inspection.ambiguous_legacy_count,
            persisted.inspection.coherent_for_review_operations,
        )
    } else if let Some(metadata) = metadata {
        (
            None,
            None,
            Some(metadata.source_state_archive_dir.clone()),
            Some(metadata.selected_snapshot_source_path.clone()),
            Some(metadata.selected_snapshot_file_name.clone()),
            Some(metadata.snapshotted_at),
            Some(metadata.snapshot_state_verdict.clone()),
            Some(metadata.snapshot_state_reason.clone()),
            metadata.selected_review_generation_id.clone(),
            metadata.selected_latest_release_generation_id.clone(),
            Some(metadata.ambiguous_legacy_count),
            Some(metadata.coherent_for_review_operations),
        )
    } else {
        (
            None, None, None, None, None, None, None, None, None, None, None, None,
        )
    };

    ArtifactStateBundlePublishReport {
        mode: mode.to_string(),
        verdict,
        reason,
        state_archive_dir: state_archive_dir.map(|path| path.display().to_string()),
        bundle_archive_dir: config.bundle_archive_dir.display().to_string(),
        persisted_bundle_path,
        persisted_bundle_exists,
        persisted_bundle_dir_name,
        bundle_integrity_verdict,
        bundle_integrity_reason,
        source_state_archive_dir,
        selected_snapshot_path,
        selected_snapshot_file_name,
        snapshotted_at,
        selected_snapshot_state_verdict,
        selected_snapshot_state_reason,
        selected_review_generation_id,
        selected_latest_release_generation_id,
        ambiguous_legacy_count,
        coherent_for_review_operations,
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
        latest_pointer_source_bundle_archive_dir: metadata
            .map(|value| value.source_bundle_archive_dir.clone()),
        latest_pointer_pointed_at: metadata.map(|value| value.pointed_at),
        latest_pointer_exists: config
            .latest_pointer_dir
            .as_ref()
            .is_some_and(|dir| latest_pointer_path(dir, &config.pointer_name).exists()),
        latest_pointer_overwrite_used,
        latest_pointer_target_exists: metadata
            .map(|value| Path::new(&value.selected_bundle_path).exists())
            .unwrap_or(false),
        latest_pointer_target_matches_identity: metadata.is_some()
            && missing_paths.is_empty()
            && inconsistencies.is_empty(),
        verification_attempted: false,
        missing_paths,
        inconsistencies,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary:
            "State snapshot bundle archive management only persists and verifies archived bundle artifacts. It preserves underlying snapshot state truth but does not authorize activation, does not enable execution, and does not override the Stage 3 production gate.".to_string(),
    }
}

fn finalize_publish_pointer_report(
    mut report: ArtifactStateBundlePublishReport,
    verdict: ArtifactStateBundleReportVerdict,
    reason: String,
    latest_pointer_updated: bool,
    latest_pointer_overwrite_used: bool,
) -> ArtifactStateBundlePublishReport {
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
    verdict: ArtifactStateBundleReportVerdict,
    reason: String,
    config: &Config,
    metadata_path: &Path,
    metadata: &StateBundleLatestPointerMetadata,
    inspected_bundle: Option<&activation_artifact_state_bundle::StateSnapshotBundleSummary>,
    missing_paths: Vec<String>,
    inconsistencies: Vec<String>,
    verification_attempted: bool,
) -> ArtifactStateBundlePublishReport {
    ArtifactStateBundlePublishReport {
        mode: mode.to_string(),
        verdict,
        reason,
        state_archive_dir: None,
        bundle_archive_dir: config.bundle_archive_dir.display().to_string(),
        persisted_bundle_path: Some(metadata.selected_bundle_path.clone()),
        persisted_bundle_exists: Path::new(&metadata.selected_bundle_path).exists(),
        persisted_bundle_dir_name: Some(metadata.selected_bundle_dir_name.clone()),
        bundle_integrity_verdict: inspected_bundle.map(|value| value.verdict.clone()),
        bundle_integrity_reason: inspected_bundle.map(|value| value.reason.clone()),
        source_state_archive_dir: Some(metadata.source_state_archive_dir.clone()),
        selected_snapshot_path: Some(metadata.selected_snapshot_source_path.clone()),
        selected_snapshot_file_name: Some(metadata.selected_snapshot_file_name.clone()),
        snapshotted_at: Some(metadata.snapshotted_at),
        selected_snapshot_state_verdict: Some(metadata.snapshot_state_verdict.clone()),
        selected_snapshot_state_reason: Some(metadata.snapshot_state_reason.clone()),
        selected_review_generation_id: metadata.selected_review_generation_id.clone(),
        selected_latest_release_generation_id: metadata
            .selected_latest_release_generation_id
            .clone(),
        ambiguous_legacy_count: Some(metadata.ambiguous_legacy_count),
        coherent_for_review_operations: Some(metadata.coherent_for_review_operations),
        latest_pointer_attempted: false,
        latest_pointer_updated: false,
        latest_pointer_dir: config
            .latest_pointer_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        latest_pointer_name: config.pointer_name.clone(),
        latest_pointer_path: Some(metadata_path.display().to_string()),
        latest_pointer_source_bundle_archive_dir: Some(metadata.source_bundle_archive_dir.clone()),
        latest_pointer_pointed_at: Some(metadata.pointed_at),
        latest_pointer_exists: metadata_path.exists(),
        latest_pointer_overwrite_used: false,
        latest_pointer_target_exists: Path::new(&metadata.selected_bundle_path).exists(),
        latest_pointer_target_matches_identity: missing_paths.is_empty() && inconsistencies.is_empty(),
        verification_attempted,
        missing_paths,
        inconsistencies,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary:
            "State snapshot bundle archive management only persists and verifies archived bundle artifacts. It preserves underlying snapshot state truth but does not authorize activation, does not enable execution, and does not override the Stage 3 production gate.".to_string(),
    }
}

fn bundle_directory_name(selected_snapshot_file_name: &str) -> String {
    let stem = Path::new(selected_snapshot_file_name)
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or(selected_snapshot_file_name);
    format!("state_snapshot_bundle__{stem}")
}

fn canonicalize_bundle_archive_dir(bundle_archive_dir: &Path) -> Result<PathBuf> {
    fs::canonicalize(bundle_archive_dir).with_context(|| {
        format!(
            "failed canonicalizing state snapshot bundle archive dir {}",
            bundle_archive_dir.display()
        )
    })
}

fn canonicalize_stored_bundle_archive_dir(stored_bundle_archive_dir: &str) -> Result<PathBuf> {
    fs::canonicalize(Path::new(stored_bundle_archive_dir)).with_context(|| {
        format!(
            "failed canonicalizing stored state snapshot bundle archive dir {}",
            stored_bundle_archive_dir
        )
    })
}

fn render_human(report: &ArtifactStateBundlePublishReport) -> String {
    [
        "event=copybot_activation_artifact_state_bundle_publish_report".to_string(),
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!(
            "state_archive_dir={}",
            report
                .state_archive_dir
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!("bundle_archive_dir={}", report.bundle_archive_dir),
        format!(
            "persisted_bundle_path={}",
            report
                .persisted_bundle_path
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "bundle_integrity_verdict={}",
            report
                .bundle_integrity_verdict
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "selected_snapshot_path={}",
            report
                .selected_snapshot_path
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "selected_snapshot_file_name={}",
            report
                .selected_snapshot_file_name
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "snapshotted_at={}",
            report
                .snapshotted_at
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "selected_snapshot_state_verdict={}",
            report
                .selected_snapshot_state_verdict
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "selected_snapshot_state_reason={}",
            report
                .selected_snapshot_state_reason
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "selected_review_generation_id={}",
            report
                .selected_review_generation_id
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "selected_latest_release_generation_id={}",
            report
                .selected_latest_release_generation_id
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "ambiguous_legacy_count={}",
            report
                .ambiguous_legacy_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!(
            "coherent_for_review_operations={}",
            report
                .coherent_for_review_operations
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string())
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
            "latest_pointer_target_exists={}",
            report.latest_pointer_target_exists
        ),
        format!(
            "latest_pointer_target_matches_identity={}",
            report.latest_pointer_target_matches_identity
        ),
        format!(
            "missing_paths={}",
            if report.missing_paths.is_empty() {
                "null".to_string()
            } else {
                report.missing_paths.join(" | ")
            }
        ),
        format!(
            "inconsistencies={}",
            if report.inconsistencies.is_empty() {
                "null".to_string()
            } else {
                report.inconsistencies.join(" | ")
            }
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

fn cleanup_dir_if_exists(path: &Path) {
    if path.exists() {
        let _ = fs::remove_dir_all(path);
    }
}

fn unique_suffix() -> String {
    format!(
        "{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn publish_succeeds_into_deterministic_bundle_archive_layout() {
        let state_archive_dir = temp_dir("state_bundle_archive_publish_state");
        let snapshot = state_archive_dir
            .join("state_snapshot__2026-03-26T12-00-00Z__artifact_state_coherent.json");
        write_snapshot(
            &snapshot,
            "artifact_state_coherent",
            "review_a",
            "release_a",
            true,
            0,
            ts("2026-03-26T12:00:00Z"),
        );
        let bundle_archive_dir = temp_dir("state_bundle_archive_publish_archive");
        let config = sample_config(
            bundle_archive_dir.clone(),
            None,
            Mode::Publish {
                state_archive_dir: state_archive_dir.clone(),
                snapshot_selector: snapshot
                    .file_name()
                    .expect("file name")
                    .to_string_lossy()
                    .into_owned(),
                persist_latest_pointer: false,
                allow_latest_pointer_overwrite: false,
            },
        );

        let report = publish_bundle_report(
            &config,
            &state_archive_dir,
            snapshot
                .file_name()
                .and_then(|v| v.to_str())
                .unwrap_or_default(),
            false,
            false,
        )
        .expect("publish");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleReportVerdict::ArtifactStateBundleReportPublished
        );
        let persisted_path = PathBuf::from(
            report
                .persisted_bundle_path
                .clone()
                .expect("persisted bundle path"),
        );
        assert!(persisted_path.exists());
        assert_eq!(
            persisted_path
                .file_name()
                .and_then(|v| v.to_str())
                .unwrap_or_default(),
            "state_snapshot_bundle__state_snapshot__2026-03-26T12-00-00Z__artifact_state_coherent"
        );
        assert!(persisted_path
            .join(activation_artifact_state_bundle::BUNDLE_MANIFEST_FILENAME)
            .exists());
        assert!(report.execution_untouched);
        assert!(!report.activation_authorized);
    }

    #[test]
    fn collision_does_not_overwrite_existing_archived_bundle_silently() {
        let state_archive_dir = temp_dir("state_bundle_archive_collision_state");
        let snapshot = state_archive_dir
            .join("state_snapshot__2026-03-26T12-00-00Z__artifact_state_coherent.json");
        write_snapshot(
            &snapshot,
            "artifact_state_coherent",
            "review_a",
            "release_a",
            true,
            0,
            ts("2026-03-26T12:00:00Z"),
        );
        let bundle_archive_dir = temp_dir("state_bundle_archive_collision_archive");
        let config = sample_config(
            bundle_archive_dir.clone(),
            None,
            Mode::Publish {
                state_archive_dir: state_archive_dir.clone(),
                snapshot_selector: snapshot
                    .file_name()
                    .expect("file name")
                    .to_string_lossy()
                    .into_owned(),
                persist_latest_pointer: false,
                allow_latest_pointer_overwrite: false,
            },
        );

        persist_state_snapshot_bundle(
            &config,
            &state_archive_dir,
            snapshot
                .file_name()
                .and_then(|v| v.to_str())
                .unwrap_or_default(),
        )
        .expect("first persist");
        let error = persist_state_snapshot_bundle(
            &config,
            &state_archive_dir,
            snapshot
                .file_name()
                .and_then(|v| v.to_str())
                .unwrap_or_default(),
        )
        .expect_err("collision");

        assert!(format!("{error:#}")
            .contains("refusing to overwrite existing archived state snapshot bundle"));
    }

    #[test]
    fn latest_pointer_update_works_when_requested() {
        let state_archive_dir = temp_dir("state_bundle_archive_pointer_state");
        let snapshot = state_archive_dir
            .join("state_snapshot__2026-03-26T12-00-00Z__artifact_state_coherent.json");
        write_snapshot(
            &snapshot,
            "artifact_state_coherent",
            "review_a",
            "release_a",
            true,
            0,
            ts("2026-03-26T12:00:00Z"),
        );
        let bundle_archive_dir = temp_dir("state_bundle_archive_pointer_archive");
        let latest_pointer_dir = temp_dir("state_bundle_archive_pointer_dir");
        let config = sample_config(
            bundle_archive_dir.clone(),
            Some(latest_pointer_dir.clone()),
            Mode::ReportLatest,
        );
        let persisted = persist_state_snapshot_bundle(
            &sample_config(
                bundle_archive_dir.clone(),
                Some(latest_pointer_dir.clone()),
                Mode::Publish {
                    state_archive_dir: state_archive_dir.clone(),
                    snapshot_selector: snapshot
                        .file_name()
                        .expect("file name")
                        .to_string_lossy()
                        .into_owned(),
                    persist_latest_pointer: false,
                    allow_latest_pointer_overwrite: false,
                },
            ),
            &state_archive_dir,
            snapshot
                .file_name()
                .and_then(|v| v.to_str())
                .unwrap_or_default(),
        )
        .expect("persist");

        let pointer_path = write_latest_pointer(&config, &persisted, false).expect("pointer");

        assert!(pointer_path.exists());
        assert_eq!(
            pointer_path,
            latest_pointer_path(&latest_pointer_dir, DEFAULT_BUNDLE_POINTER_NAME)
        );
    }

    #[test]
    fn latest_pointer_verify_passes_for_consistent_archived_bundle_target() {
        let state_archive_dir = temp_dir("state_bundle_archive_verify_ok_state");
        let snapshot = state_archive_dir
            .join("state_snapshot__2026-03-26T12-00-00Z__artifact_state_coherent.json");
        write_snapshot(
            &snapshot,
            "artifact_state_coherent",
            "review_a",
            "release_a",
            true,
            0,
            ts("2026-03-26T12:00:00Z"),
        );
        let bundle_archive_dir = temp_dir("state_bundle_archive_verify_ok_archive");
        let latest_pointer_dir = temp_dir("state_bundle_archive_verify_ok_pointer");
        let config = sample_config(
            bundle_archive_dir.clone(),
            Some(latest_pointer_dir.clone()),
            Mode::VerifyLatest,
        );
        let persisted = persist_state_snapshot_bundle(
            &sample_config(
                bundle_archive_dir.clone(),
                Some(latest_pointer_dir.clone()),
                Mode::Publish {
                    state_archive_dir: state_archive_dir.clone(),
                    snapshot_selector: snapshot
                        .file_name()
                        .expect("file name")
                        .to_string_lossy()
                        .into_owned(),
                    persist_latest_pointer: false,
                    allow_latest_pointer_overwrite: false,
                },
            ),
            &state_archive_dir,
            snapshot
                .file_name()
                .and_then(|v| v.to_str())
                .unwrap_or_default(),
        )
        .expect("persist");
        write_latest_pointer(&config, &persisted, false).expect("write pointer");

        let verify = inspect_latest_pointer(&config, true).expect("verify");

        assert_eq!(
            verify.verdict,
            ArtifactStateBundleReportVerdict::ArtifactStateBundleReportVerifyOk
        );
        assert!(verify.latest_pointer_target_matches_identity);
        assert_eq!(
            verify.bundle_integrity_verdict.as_deref(),
            Some("artifact_state_bundle_verified")
        );
    }

    #[test]
    fn latest_pointer_verify_fails_for_missing_bundle_target() {
        let bundle_archive_dir = temp_dir("state_bundle_archive_verify_missing_archive");
        let latest_pointer_dir = temp_dir("state_bundle_archive_verify_missing_pointer");
        fs::create_dir_all(&latest_pointer_dir).expect("pointer dir");
        let metadata_path = latest_pointer_path(&latest_pointer_dir, DEFAULT_BUNDLE_POINTER_NAME);
        fs::write(
            &metadata_path,
            serde_json::to_string_pretty(&sample_pointer_metadata(
                &bundle_archive_dir,
                "/tmp/missing_bundle",
                "/tmp/state_archive",
                "/tmp/state_archive/state_snapshot__2026-03-26T12-00-00Z__artifact_state_coherent.json",
                "artifact_state_coherent",
                "sample coherent",
                0,
                true,
            ))
            .expect("serialize pointer"),
        )
        .expect("write pointer");
        let config = sample_config(
            bundle_archive_dir.clone(),
            Some(latest_pointer_dir.clone()),
            Mode::VerifyLatest,
        );

        let verify = inspect_latest_pointer(&config, true).expect("verify");

        assert_eq!(
            verify.verdict,
            ArtifactStateBundleReportVerdict::ArtifactStateBundleReportVerifyMissingTarget
        );
        assert!(verify.reason.contains("target archived bundle is missing"));
    }

    #[test]
    fn invalid_pointer_metadata_is_surfaced_explicitly() {
        let bundle_archive_dir = temp_dir("state_bundle_archive_invalid_metadata_archive");
        let latest_pointer_dir = temp_dir("state_bundle_archive_invalid_metadata_pointer");
        fs::create_dir_all(&latest_pointer_dir).expect("pointer dir");
        fs::write(
            latest_pointer_path(&latest_pointer_dir, DEFAULT_BUNDLE_POINTER_NAME),
            "{broken",
        )
        .expect("write invalid metadata");
        let config = sample_config(
            bundle_archive_dir.clone(),
            Some(latest_pointer_dir.clone()),
            Mode::VerifyLatest,
        );

        let verify = inspect_latest_pointer(&config, true).expect("verify");

        assert_eq!(
            verify.verdict,
            ArtifactStateBundleReportVerdict::ArtifactStateBundleReportInvalidMetadata
        );
    }

    #[test]
    fn ambiguous_non_green_bundled_snapshot_remains_explicit_in_publish_and_report_output() {
        let state_archive_dir = temp_dir("state_bundle_archive_ambiguous_state");
        let snapshot = state_archive_dir.join(
            "state_snapshot__2026-03-26T12-00-00Z__artifact_state_ambiguous_legacy_state.json",
        );
        write_snapshot(
            &snapshot,
            "artifact_state_ambiguous_legacy_state",
            "review_a",
            "release_a",
            true,
            3,
            ts("2026-03-26T12:00:00Z"),
        );
        let bundle_archive_dir = temp_dir("state_bundle_archive_ambiguous_archive");
        let latest_pointer_dir = temp_dir("state_bundle_archive_ambiguous_pointer");
        let publish_config = sample_config(
            bundle_archive_dir.clone(),
            Some(latest_pointer_dir.clone()),
            Mode::Publish {
                state_archive_dir: state_archive_dir.clone(),
                snapshot_selector: snapshot
                    .file_name()
                    .expect("file name")
                    .to_string_lossy()
                    .into_owned(),
                persist_latest_pointer: true,
                allow_latest_pointer_overwrite: false,
            },
        );

        let publish = publish_bundle_report(
            &publish_config,
            &state_archive_dir,
            snapshot
                .file_name()
                .and_then(|v| v.to_str())
                .unwrap_or_default(),
            true,
            false,
        )
        .expect("publish");

        assert_eq!(
            publish.verdict,
            ArtifactStateBundleReportVerdict::ArtifactStateBundleReportPublishedAndPointedLatest
        );
        assert_eq!(
            publish.selected_snapshot_state_verdict.as_deref(),
            Some("artifact_state_ambiguous_legacy_state")
        );
        assert_eq!(publish.ambiguous_legacy_count, Some(3));
        assert_eq!(publish.coherent_for_review_operations, Some(false));

        let report_latest = inspect_latest_pointer(
            &sample_config(
                bundle_archive_dir.clone(),
                Some(latest_pointer_dir.clone()),
                Mode::ReportLatest,
            ),
            false,
        )
        .expect("report latest");
        assert_eq!(
            report_latest.verdict,
            ArtifactStateBundleReportVerdict::ArtifactStateBundleReportVerifyOk
        );
        assert_eq!(
            report_latest.selected_snapshot_state_verdict.as_deref(),
            Some("artifact_state_ambiguous_legacy_state")
        );
        assert_eq!(report_latest.ambiguous_legacy_count, Some(3));
        assert_eq!(report_latest.coherent_for_review_operations, Some(false));
    }

    #[test]
    fn pointer_name_escape_segments_are_rejected_during_arg_parsing() {
        let bundle_archive_dir = temp_dir("state_bundle_archive_pointer_name_escape_archive");
        let latest_pointer_dir = temp_dir("state_bundle_archive_pointer_name_escape_pointer");

        let error = parse_args_from(vec![
            "--bundle-archive-dir".to_string(),
            bundle_archive_dir.display().to_string(),
            "--publish".to_string(),
            "--state-archive-dir".to_string(),
            temp_dir("state_bundle_archive_pointer_name_escape_state")
                .display()
                .to_string(),
            "--snapshot".to_string(),
            "state_snapshot__2026-03-26T12-00-00Z__artifact_state_coherent.json".to_string(),
            "--persist-latest-pointer".to_string(),
            "--bundle-latest-pointer-dir".to_string(),
            latest_pointer_dir.display().to_string(),
            "--pointer-name".to_string(),
            "../escape".to_string(),
        ])
        .expect_err("invalid pointer name");

        assert!(format!("{error:#}")
            .contains("pointer name may only contain ascii alphanumeric characters"));
        assert!(!latest_pointer_dir.join("../escape.json").exists());
    }

    #[test]
    fn pointer_name_nested_path_is_rejected_during_arg_parsing() {
        let bundle_archive_dir = temp_dir("state_bundle_archive_pointer_name_nested_archive");
        let latest_pointer_dir = temp_dir("state_bundle_archive_pointer_name_nested_pointer");

        let error = parse_args_from(vec![
            "--bundle-archive-dir".to_string(),
            bundle_archive_dir.display().to_string(),
            "--report-latest".to_string(),
            "--bundle-latest-pointer-dir".to_string(),
            latest_pointer_dir.display().to_string(),
            "--pointer-name".to_string(),
            "nested/name".to_string(),
        ])
        .expect_err("invalid nested pointer name");

        assert!(format!("{error:#}")
            .contains("pointer name may only contain ascii alphanumeric characters"));
    }

    #[test]
    fn valid_custom_pointer_name_still_works_and_stays_bounded_to_pointer_dir() {
        let state_archive_dir = temp_dir("state_bundle_archive_pointer_name_valid_state");
        let snapshot = state_archive_dir
            .join("state_snapshot__2026-03-26T12-00-00Z__artifact_state_coherent.json");
        write_snapshot(
            &snapshot,
            "artifact_state_coherent",
            "review_a",
            "release_a",
            true,
            0,
            ts("2026-03-26T12:00:00Z"),
        );
        let bundle_archive_dir = temp_dir("state_bundle_archive_pointer_name_valid_archive");
        let latest_pointer_dir = temp_dir("state_bundle_archive_pointer_name_valid_pointer");
        let pointer_name = "review_bundle_latest_1".to_string();
        let config = sample_config(
            bundle_archive_dir.clone(),
            Some(latest_pointer_dir.clone()),
            Mode::Publish {
                state_archive_dir: state_archive_dir.clone(),
                snapshot_selector: snapshot
                    .file_name()
                    .expect("file name")
                    .to_string_lossy()
                    .into_owned(),
                persist_latest_pointer: true,
                allow_latest_pointer_overwrite: false,
            },
        );
        let mut config = config;
        config.pointer_name = parse_pointer_name(pointer_name.clone()).expect("valid pointer name");

        let report = publish_bundle_report(
            &config,
            &state_archive_dir,
            snapshot
                .file_name()
                .and_then(|v| v.to_str())
                .unwrap_or_default(),
            true,
            false,
        )
        .expect("publish");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleReportVerdict::ArtifactStateBundleReportPublishedAndPointedLatest
        );
        let expected_pointer_path = latest_pointer_dir.join(format!("{pointer_name}.json"));
        assert_eq!(
            report.latest_pointer_path.as_deref(),
            Some(expected_pointer_path.to_string_lossy().as_ref())
        );
        assert!(expected_pointer_path.exists());
        assert_eq!(
            expected_pointer_path.parent(),
            Some(latest_pointer_dir.as_path())
        );
    }

    fn sample_config(
        bundle_archive_dir: PathBuf,
        latest_pointer_dir: Option<PathBuf>,
        mode: Mode,
    ) -> Config {
        Config {
            bundle_archive_dir,
            latest_pointer_dir,
            pointer_name: DEFAULT_BUNDLE_POINTER_NAME.to_string(),
            json: false,
            mode,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn sample_pointer_metadata(
        bundle_archive_dir: &Path,
        selected_bundle_path: &str,
        source_state_archive_dir: &str,
        selected_snapshot_source_path: &str,
        snapshot_state_verdict: &str,
        snapshot_state_reason: &str,
        ambiguous_legacy_count: usize,
        coherent_for_review_operations: bool,
    ) -> StateBundleLatestPointerMetadata {
        StateBundleLatestPointerMetadata {
            pointer_version: BUNDLE_POINTER_VERSION.to_string(),
            pointer_name: DEFAULT_BUNDLE_POINTER_NAME.to_string(),
            source_bundle_archive_dir: bundle_archive_dir.display().to_string(),
            selected_bundle_path: selected_bundle_path.to_string(),
            selected_bundle_dir_name: Path::new(selected_bundle_path)
                .file_name()
                .map(|value| value.to_string_lossy().into_owned())
                .unwrap_or_else(|| "missing_bundle".to_string()),
            source_state_archive_dir: source_state_archive_dir.to_string(),
            selected_snapshot_source_path: selected_snapshot_source_path.to_string(),
            selected_snapshot_file_name: Path::new(selected_snapshot_source_path)
                .file_name()
                .map(|value| value.to_string_lossy().into_owned())
                .unwrap_or_else(|| "state_snapshot.json".to_string()),
            snapshotted_at: ts("2026-03-26T12:00:00Z"),
            snapshot_state_verdict: snapshot_state_verdict.to_string(),
            snapshot_state_reason: snapshot_state_reason.to_string(),
            selected_review_generation_id: Some("review_a".to_string()),
            selected_latest_release_generation_id: Some("release_a".to_string()),
            ambiguous_legacy_count,
            coherent_for_review_operations,
            pointed_at: ts("2026-03-26T12:10:00Z"),
            build_version: env!("CARGO_PKG_VERSION").to_string(),
            git_commit: Some("deadbeef".to_string()),
        }
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
                "verdict": state_verdict,
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
