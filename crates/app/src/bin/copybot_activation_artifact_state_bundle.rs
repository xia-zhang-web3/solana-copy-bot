#![recursion_limit = "256"]
#![allow(unused_attributes)]

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::Command;

#[allow(dead_code)]
#[path = "copybot_activation_artifact_state_publish_report.rs"]
mod activation_artifact_state_publish_report;

const USAGE: &str = "usage: copybot_activation_artifact_state_bundle [--json] (--state-archive-dir <path> --export-bundle --snapshot <path|file-name|snapshotted-at> --output <dir> | --verify-bundle <path>)";
const BUNDLE_VERSION: &str = "1";
const HASH_ALGORITHM: &str = "sha256";
pub(crate) const BUNDLE_MANIFEST_FILENAME: &str = "state_snapshot_bundle_manifest.json";
const BUNDLE_ARTIFACTS_DIR: &str = "artifacts";

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
    json: bool,
    mode: Mode,
}

#[derive(Debug, Clone)]
enum Mode {
    Export {
        state_archive_dir: PathBuf,
        snapshot_selector: String,
        output_dir: PathBuf,
    },
    Verify {
        bundle_path: PathBuf,
    },
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ArtifactStateBundleVerdict {
    ArtifactStateBundleExported,
    ArtifactStateBundleVerified,
    ArtifactStateBundleInvalid,
    ArtifactStateBundleDriftDetected,
    ArtifactStateBundleSnapshotNotFound,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum StateBundleArtifactKind {
    StateSnapshotJson,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StateSnapshotBundleFileEntry {
    artifact_kind: StateBundleArtifactKind,
    source_relative_path: String,
    bundle_relative_path: String,
    sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StateSnapshotBundleManifest {
    generated_at: DateTime<Utc>,
    bundle_version: String,
    build_version: String,
    git_commit: Option<String>,
    source_state_archive_dir: String,
    selected_snapshot_selector: String,
    selected_snapshot_source_path: String,
    selected_snapshot_file_name: String,
    snapshotted_at: DateTime<Utc>,
    state_verdict: String,
    state_reason: String,
    selected_review_generation_id: Option<String>,
    selected_latest_release_generation_id: Option<String>,
    selection_alignment_matches: bool,
    selection_alignment_summary: String,
    ambiguous_legacy_count: usize,
    coherent_for_review_operations: bool,
    artifact_state_only: bool,
    execution_untouched: bool,
    activation_authorized: bool,
    not_authorized_summary: String,
    hash_algorithm: String,
    file_count: usize,
    files: Vec<StateSnapshotBundleFileEntry>,
}

#[derive(Debug, Clone, Serialize)]
struct ExportReport {
    mode: String,
    verdict: ArtifactStateBundleVerdict,
    reason: String,
    state_archive_dir: String,
    snapshot_selector: String,
    selected_snapshot_path: Option<String>,
    selected_snapshot_file_name: Option<String>,
    snapshotted_at: Option<DateTime<Utc>>,
    state_verdict: Option<String>,
    state_reason: Option<String>,
    selected_review_generation_id: Option<String>,
    selected_latest_release_generation_id: Option<String>,
    ambiguous_legacy_count: Option<usize>,
    coherent_for_review_operations: Option<bool>,
    output_path: String,
    bundled_file_count: usize,
    bundled_file_paths: Vec<String>,
    hash_coverage_count: usize,
    read_only_state_snapshot_artifacts: bool,
    execution_untouched: bool,
    activation_authorized: bool,
    not_authorized_summary: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct StateSnapshotBundleExportSummary {
    pub(crate) verdict: String,
    pub(crate) reason: String,
    pub(crate) state_archive_dir: String,
    pub(crate) snapshot_selector: String,
    pub(crate) selected_snapshot_path: Option<String>,
    pub(crate) selected_snapshot_file_name: Option<String>,
    pub(crate) snapshotted_at: Option<DateTime<Utc>>,
    pub(crate) state_verdict: Option<String>,
    pub(crate) state_reason: Option<String>,
    pub(crate) selected_review_generation_id: Option<String>,
    pub(crate) selected_latest_release_generation_id: Option<String>,
    pub(crate) ambiguous_legacy_count: Option<usize>,
    pub(crate) coherent_for_review_operations: Option<bool>,
    pub(crate) output_path: String,
}

#[derive(Debug, Clone, Serialize)]
struct VerifyReport {
    mode: String,
    verdict: ArtifactStateBundleVerdict,
    reason: String,
    bundle_path: String,
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
    file_count: usize,
    missing_files: Vec<String>,
    changed_files: Vec<String>,
    unexpected_files: Vec<String>,
    metadata_mismatches: Vec<String>,
    read_only_bundle_analysis: bool,
    execution_untouched: bool,
    activation_authorized: bool,
    not_authorized_summary: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct StateSnapshotBundleSummary {
    pub(crate) verdict: String,
    pub(crate) reason: String,
    pub(crate) bundle_path: String,
    pub(crate) source_state_archive_dir: Option<String>,
    pub(crate) selected_snapshot_path: Option<String>,
    pub(crate) selected_snapshot_file_name: Option<String>,
    pub(crate) snapshotted_at: Option<DateTime<Utc>>,
    pub(crate) selected_snapshot_state_verdict: Option<String>,
    pub(crate) selected_snapshot_state_reason: Option<String>,
    pub(crate) selected_review_generation_id: Option<String>,
    pub(crate) selected_latest_release_generation_id: Option<String>,
    pub(crate) ambiguous_legacy_count: Option<usize>,
    pub(crate) coherent_for_review_operations: Option<bool>,
    pub(crate) missing_files: Vec<String>,
    pub(crate) changed_files: Vec<String>,
    pub(crate) unexpected_files: Vec<String>,
    pub(crate) metadata_mismatches: Vec<String>,
}

#[derive(Debug, Clone)]
struct SelectedSnapshot {
    loaded: activation_artifact_state_publish_report::LoadedStateSnapshot,
    source_relative_path: String,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut json = false;
    let mut state_archive_dir: Option<PathBuf> = None;
    let mut export_bundle = false;
    let mut snapshot_selector: Option<String> = None;
    let mut output_dir: Option<PathBuf> = None;
    let mut verify_bundle: Option<PathBuf> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--json" => json = true,
            "--state-archive-dir" => {
                state_archive_dir = Some(PathBuf::from(parse_string_arg(
                    "--state-archive-dir",
                    args.next(),
                )?))
            }
            "--export-bundle" => export_bundle = true,
            "--snapshot" => snapshot_selector = Some(parse_string_arg("--snapshot", args.next())?),
            "--output" => {
                output_dir = Some(PathBuf::from(parse_string_arg("--output", args.next())?))
            }
            "--verify-bundle" => {
                verify_bundle = Some(PathBuf::from(parse_string_arg(
                    "--verify-bundle",
                    args.next(),
                )?))
            }
            "--help" | "-h" => return Ok(None),
            other => bail!("unrecognized arg `{other}`"),
        }
    }

    let mode = match (
        state_archive_dir,
        export_bundle,
        snapshot_selector,
        output_dir,
        verify_bundle,
    ) {
        (Some(state_archive_dir), true, Some(snapshot_selector), Some(output_dir), None) => {
            Mode::Export {
                state_archive_dir,
                snapshot_selector,
                output_dir,
            }
        }
        (None, false, None, None, Some(bundle_path)) => Mode::Verify { bundle_path },
        (Some(_), true, None, _, None) => bail!("--export-bundle requires --snapshot <selector>"),
        (Some(_), true, Some(_), None, None) => bail!("--export-bundle requires --output <dir>"),
        (Some(_), false, _, _, None) => bail!("--state-archive-dir requires --export-bundle"),
        (None, true, _, _, None) => bail!("--export-bundle requires --state-archive-dir <path>"),
        (_, true, _, _, Some(_)) => bail!("--export-bundle and --verify-bundle cannot be combined"),
        (Some(_), _, _, _, Some(_)) => {
            bail!("--state-archive-dir cannot be combined with --verify-bundle")
        }
        (None, false, Some(_), _, None) => bail!("--snapshot requires --export-bundle"),
        (None, false, None, Some(_), None) => bail!("--output requires --export-bundle"),
        (None, false, None, None, None) => {
            bail!("either --export-bundle or --verify-bundle <path> is required")
        }
        _ => bail!("invalid activation artifact state bundle arguments"),
    };

    Ok(Some(Config { json, mode }))
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    let raw = value.ok_or_else(|| anyhow!("{flag} requires a value"))?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("{flag} cannot be empty");
    }
    Ok(trimmed.to_string())
}

fn run(config: Config) -> Result<String> {
    match config.mode {
        Mode::Export {
            state_archive_dir,
            snapshot_selector,
            output_dir,
        } => {
            let report = export_bundle(&state_archive_dir, &snapshot_selector, &output_dir)?;
            if config.json {
                Ok(serde_json::to_string_pretty(&report)?)
            } else {
                Ok(render_export_human(&report))
            }
        }
        Mode::Verify { bundle_path } => {
            let report = verify_bundle(&bundle_path)?;
            if config.json {
                Ok(serde_json::to_string_pretty(&report)?)
            } else {
                Ok(render_verify_human(&report))
            }
        }
    }
}

#[allow(dead_code)]
pub(crate) fn inspect_state_snapshot_bundle(
    bundle_path: &Path,
) -> Result<StateSnapshotBundleSummary> {
    let report = verify_bundle(bundle_path)?;
    Ok(StateSnapshotBundleSummary {
        verdict: serialize_enum(&report.verdict),
        reason: report.reason,
        bundle_path: report.bundle_path,
        source_state_archive_dir: report.source_state_archive_dir,
        selected_snapshot_path: report.selected_snapshot_path,
        selected_snapshot_file_name: report.selected_snapshot_file_name,
        snapshotted_at: report.snapshotted_at,
        selected_snapshot_state_verdict: report.selected_snapshot_state_verdict,
        selected_snapshot_state_reason: report.selected_snapshot_state_reason,
        selected_review_generation_id: report.selected_review_generation_id,
        selected_latest_release_generation_id: report.selected_latest_release_generation_id,
        ambiguous_legacy_count: report.ambiguous_legacy_count,
        coherent_for_review_operations: report.coherent_for_review_operations,
        missing_files: report.missing_files,
        changed_files: report.changed_files,
        unexpected_files: report.unexpected_files,
        metadata_mismatches: report.metadata_mismatches,
    })
}

#[allow(dead_code)]
pub(crate) fn export_state_snapshot_bundle(
    state_archive_dir: &Path,
    snapshot_selector: &str,
    output_dir: &Path,
) -> Result<StateSnapshotBundleExportSummary> {
    let report = export_bundle(state_archive_dir, snapshot_selector, output_dir)?;
    Ok(StateSnapshotBundleExportSummary {
        verdict: serialize_enum(&report.verdict),
        reason: report.reason,
        state_archive_dir: report.state_archive_dir,
        snapshot_selector: report.snapshot_selector,
        selected_snapshot_path: report.selected_snapshot_path,
        selected_snapshot_file_name: report.selected_snapshot_file_name,
        snapshotted_at: report.snapshotted_at,
        state_verdict: report.state_verdict,
        state_reason: report.state_reason,
        selected_review_generation_id: report.selected_review_generation_id,
        selected_latest_release_generation_id: report.selected_latest_release_generation_id,
        ambiguous_legacy_count: report.ambiguous_legacy_count,
        coherent_for_review_operations: report.coherent_for_review_operations,
        output_path: report.output_path,
    })
}

#[allow(dead_code)]
pub(crate) fn collect_state_snapshot_bundle_paths(
    bundle_dir: Option<&Path>,
    explicit_paths: &[PathBuf],
) -> Result<Vec<PathBuf>> {
    let mut paths = BTreeSet::new();
    if let Some(bundle_dir) = bundle_dir {
        if bundle_dir.exists() {
            collect_bundle_manifest_paths(bundle_dir, &mut paths)?;
        }
    }
    for path in explicit_paths {
        paths.insert(path.clone());
    }
    Ok(paths.into_iter().collect())
}

fn export_bundle(
    state_archive_dir: &Path,
    snapshot_selector: &str,
    output_dir: &Path,
) -> Result<ExportReport> {
    let canonical_archive_dir = canonical_archive_root(state_archive_dir)?;
    let selected = match resolve_selected_snapshot(state_archive_dir, snapshot_selector)? {
        SnapshotSelection::Selected(selected) => selected,
        SnapshotSelection::NotFound => {
            return Ok(ExportReport {
                mode: "export_bundle".to_string(),
                verdict: ArtifactStateBundleVerdict::ArtifactStateBundleSnapshotNotFound,
                reason: format!(
                    "no persisted state snapshot matched selector `{snapshot_selector}`"
                ),
                state_archive_dir: state_archive_dir.display().to_string(),
                snapshot_selector: snapshot_selector.to_string(),
                selected_snapshot_path: None,
                selected_snapshot_file_name: None,
                snapshotted_at: None,
                state_verdict: None,
                state_reason: None,
                selected_review_generation_id: None,
                selected_latest_release_generation_id: None,
                ambiguous_legacy_count: None,
                coherent_for_review_operations: None,
                output_path: output_dir.display().to_string(),
                bundled_file_count: 0,
                bundled_file_paths: Vec::new(),
                hash_coverage_count: 0,
                read_only_state_snapshot_artifacts: true,
                execution_untouched: true,
                activation_authorized: false,
                not_authorized_summary:
                    "State snapshot bundling only packages persisted artifact-state snapshots for review or transfer. It does not authorize activation and does not override the Stage 3 production gate.".to_string(),
            });
        }
        SnapshotSelection::Invalid(reason) => {
            return Ok(ExportReport {
                mode: "export_bundle".to_string(),
                verdict: ArtifactStateBundleVerdict::ArtifactStateBundleInvalid,
                reason,
                state_archive_dir: state_archive_dir.display().to_string(),
                snapshot_selector: snapshot_selector.to_string(),
                selected_snapshot_path: None,
                selected_snapshot_file_name: None,
                snapshotted_at: None,
                state_verdict: None,
                state_reason: None,
                selected_review_generation_id: None,
                selected_latest_release_generation_id: None,
                ambiguous_legacy_count: None,
                coherent_for_review_operations: None,
                output_path: output_dir.display().to_string(),
                bundled_file_count: 0,
                bundled_file_paths: Vec::new(),
                hash_coverage_count: 0,
                read_only_state_snapshot_artifacts: true,
                execution_untouched: true,
                activation_authorized: false,
                not_authorized_summary:
                    "State snapshot bundling only packages persisted artifact-state snapshots for review or transfer. It does not authorize activation and does not override the Stage 3 production gate.".to_string(),
            });
        }
    };

    ensure_bundle_output_dir(output_dir)?;
    let bundle_root = output_dir.to_path_buf();
    let artifacts_root = bundle_root.join(BUNDLE_ARTIFACTS_DIR);
    fs::create_dir_all(&artifacts_root).with_context(|| {
        format!(
            "failed creating state snapshot bundle artifacts dir {}",
            artifacts_root.display()
        )
    })?;

    let file_entry = StateSnapshotBundleFileEntry {
        artifact_kind: StateBundleArtifactKind::StateSnapshotJson,
        source_relative_path: selected.source_relative_path.clone(),
        bundle_relative_path: format!("{BUNDLE_ARTIFACTS_DIR}/{}", selected.source_relative_path),
        sha256: hash_file(&selected.loaded.canonical_path)?,
    };
    let target_path = bundle_root.join(&file_entry.bundle_relative_path);
    if let Some(parent) = target_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed creating bundle subdir {}", parent.display()))?;
    }
    fs::copy(
        state_archive_dir.join(&file_entry.source_relative_path),
        &target_path,
    )
    .with_context(|| {
        format!(
            "failed copying {} into bundle {}",
            state_archive_dir
                .join(&file_entry.source_relative_path)
                .display(),
            target_path.display()
        )
    })?;

    let artifact = &selected.loaded.artifact;
    let manifest = StateSnapshotBundleManifest {
        generated_at: Utc::now(),
        bundle_version: BUNDLE_VERSION.to_string(),
        build_version: env!("CARGO_PKG_VERSION").to_string(),
        git_commit: resolve_git_commit(),
        source_state_archive_dir: canonical_archive_dir.display().to_string(),
        selected_snapshot_selector: snapshot_selector.to_string(),
        selected_snapshot_source_path: selected.loaded.canonical_path.display().to_string(),
        selected_snapshot_file_name: selected
            .loaded
            .canonical_path
            .file_name()
            .map(|value| value.to_string_lossy().into_owned())
            .unwrap_or_else(|| "<unknown>".to_string()),
        snapshotted_at: artifact.snapshotted_at,
        state_verdict: artifact.state_verdict.clone(),
        state_reason: artifact.state_reason.clone(),
        selected_review_generation_id: artifact.selected_review_generation_id.clone(),
        selected_latest_release_generation_id: artifact
            .selected_latest_release_generation_id
            .clone(),
        selection_alignment_matches: artifact.selection_alignment_matches,
        selection_alignment_summary: artifact.selection_alignment_summary.clone(),
        ambiguous_legacy_count: artifact.ambiguous_legacy_count,
        coherent_for_review_operations: artifact.coherent_for_review_operations,
        artifact_state_only: artifact.artifact_state_only,
        execution_untouched: artifact.execution_untouched,
        activation_authorized: artifact.activation_authorized,
        not_authorized_summary: artifact.not_authorized_summary.clone(),
        hash_algorithm: HASH_ALGORITHM.to_string(),
        file_count: 1,
        files: vec![file_entry.clone()],
    };
    write_output(
        &bundle_root.join(BUNDLE_MANIFEST_FILENAME),
        &serde_json::to_string_pretty(&manifest)?,
    )?;

    Ok(ExportReport {
        mode: "export_bundle".to_string(),
        verdict: ArtifactStateBundleVerdict::ArtifactStateBundleExported,
        reason: format!(
            "state snapshot bundle exported for {}",
            manifest.selected_snapshot_file_name
        ),
        state_archive_dir: state_archive_dir.display().to_string(),
        snapshot_selector: snapshot_selector.to_string(),
        selected_snapshot_path: Some(selected.loaded.canonical_path.display().to_string()),
        selected_snapshot_file_name: Some(manifest.selected_snapshot_file_name),
        snapshotted_at: Some(artifact.snapshotted_at),
        state_verdict: Some(artifact.state_verdict.clone()),
        state_reason: Some(artifact.state_reason.clone()),
        selected_review_generation_id: artifact.selected_review_generation_id.clone(),
        selected_latest_release_generation_id: artifact
            .selected_latest_release_generation_id
            .clone(),
        ambiguous_legacy_count: Some(artifact.ambiguous_legacy_count),
        coherent_for_review_operations: Some(artifact.coherent_for_review_operations),
        output_path: bundle_root.display().to_string(),
        bundled_file_count: 1,
        bundled_file_paths: vec![file_entry.bundle_relative_path],
        hash_coverage_count: 1,
        read_only_state_snapshot_artifacts: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary:
            "State snapshot bundling only packages persisted artifact-state snapshots for review or transfer. It does not authorize activation and does not override the Stage 3 production gate.".to_string(),
    })
}

fn verify_bundle(bundle_path: &Path) -> Result<VerifyReport> {
    let bundle_root = resolve_bundle_root(bundle_path)?;
    let manifest_path = bundle_root.join(BUNDLE_MANIFEST_FILENAME);
    let raw_manifest = match fs::read_to_string(&manifest_path) {
        Ok(raw) => raw,
        Err(error) => {
            return Ok(invalid_verify_report(
                &bundle_root,
                format!("failed reading state snapshot bundle manifest: {error}"),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                0,
            ))
        }
    };
    let manifest: StateSnapshotBundleManifest = match serde_json::from_str(&raw_manifest) {
        Ok(manifest) => manifest,
        Err(error) => {
            return Ok(invalid_verify_report(
                &bundle_root,
                format!("failed parsing state snapshot bundle manifest: {error}"),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                0,
            ))
        }
    };

    if manifest.bundle_version.trim().is_empty() {
        return Ok(invalid_verify_report(
            &bundle_root,
            "state snapshot bundle manifest has empty bundle_version".to_string(),
            Some(manifest.source_state_archive_dir),
            Some(manifest.selected_snapshot_source_path),
            Some(manifest.selected_snapshot_file_name),
            Some(manifest.snapshotted_at),
            Some(manifest.state_verdict),
            Some(manifest.state_reason),
            manifest.selected_review_generation_id,
            manifest.selected_latest_release_generation_id,
            Some(manifest.ambiguous_legacy_count),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            0,
        ));
    }
    if manifest.hash_algorithm != HASH_ALGORITHM {
        return Ok(invalid_verify_report(
            &bundle_root,
            format!(
                "state snapshot bundle manifest hash_algorithm `{}` is unsupported; expected `{HASH_ALGORITHM}`",
                manifest.hash_algorithm
            ),
            Some(manifest.source_state_archive_dir),
            Some(manifest.selected_snapshot_source_path),
            Some(manifest.selected_snapshot_file_name),
            Some(manifest.snapshotted_at),
            Some(manifest.state_verdict),
            Some(manifest.state_reason),
            manifest.selected_review_generation_id,
            manifest.selected_latest_release_generation_id,
            Some(manifest.ambiguous_legacy_count),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            0,
        ));
    }
    if manifest.file_count != manifest.files.len() {
        return Ok(invalid_verify_report(
            &bundle_root,
            format!(
                "state snapshot bundle manifest file_count {} does not match {} file entry(ies)",
                manifest.file_count,
                manifest.files.len()
            ),
            Some(manifest.source_state_archive_dir),
            Some(manifest.selected_snapshot_source_path),
            Some(manifest.selected_snapshot_file_name),
            Some(manifest.snapshotted_at),
            Some(manifest.state_verdict),
            Some(manifest.state_reason),
            manifest.selected_review_generation_id,
            manifest.selected_latest_release_generation_id,
            Some(manifest.ambiguous_legacy_count),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            0,
        ));
    }
    if manifest.files.len() != 1
        || manifest.files[0].artifact_kind != StateBundleArtifactKind::StateSnapshotJson
    {
        return Ok(invalid_verify_report(
            &bundle_root,
            "state snapshot bundle manifest must describe exactly one bundled state snapshot artifact"
                .to_string(),
            Some(manifest.source_state_archive_dir),
            Some(manifest.selected_snapshot_source_path),
            Some(manifest.selected_snapshot_file_name),
            Some(manifest.snapshotted_at),
            Some(manifest.state_verdict),
            Some(manifest.state_reason),
            manifest.selected_review_generation_id,
            manifest.selected_latest_release_generation_id,
            Some(manifest.ambiguous_legacy_count),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            0,
        ));
    }

    let artifacts_root = bundle_root.join(BUNDLE_ARTIFACTS_DIR);
    if !artifacts_root.exists() {
        return Ok(VerifyReport {
            mode: "verify_bundle".to_string(),
            verdict: ArtifactStateBundleVerdict::ArtifactStateBundleDriftDetected,
            reason: "state snapshot bundle artifacts directory is missing".to_string(),
            bundle_path: bundle_root.display().to_string(),
            source_state_archive_dir: Some(manifest.source_state_archive_dir),
            selected_snapshot_path: Some(manifest.selected_snapshot_source_path),
            selected_snapshot_file_name: Some(manifest.selected_snapshot_file_name),
            snapshotted_at: Some(manifest.snapshotted_at),
            selected_snapshot_state_verdict: Some(manifest.state_verdict),
            selected_snapshot_state_reason: Some(manifest.state_reason),
            selected_review_generation_id: manifest.selected_review_generation_id,
            selected_latest_release_generation_id: manifest.selected_latest_release_generation_id,
            ambiguous_legacy_count: Some(manifest.ambiguous_legacy_count),
            coherent_for_review_operations: Some(manifest.coherent_for_review_operations),
            file_count: manifest.file_count,
            missing_files: manifest
                .files
                .iter()
                .map(|entry| entry.bundle_relative_path.clone())
                .collect(),
            changed_files: Vec::new(),
            unexpected_files: Vec::new(),
            metadata_mismatches: Vec::new(),
            read_only_bundle_analysis: true,
            execution_untouched: true,
            activation_authorized: false,
            not_authorized_summary:
                "State snapshot bundle verification only validates bundle integrity. It preserves snapshot state truth but does not authorize activation and does not override the Stage 3 production gate.".to_string(),
        });
    }

    let mut missing_files = Vec::new();
    let mut changed_files = Vec::new();
    for entry in &manifest.files {
        validate_safe_bundle_relative_path(&entry.bundle_relative_path)?;
        let bundle_file_path = bundle_root.join(&entry.bundle_relative_path);
        if !bundle_file_path.exists() {
            missing_files.push(entry.bundle_relative_path.clone());
            continue;
        }
        let current_hash = hash_file(&bundle_file_path)?;
        if current_hash != entry.sha256 {
            changed_files.push(entry.bundle_relative_path.clone());
        }
    }

    let expected_paths = manifest
        .files
        .iter()
        .map(|entry| entry.bundle_relative_path.clone())
        .collect::<BTreeSet<_>>();
    let actual_paths = collect_bundle_artifact_paths(&artifacts_root)?
        .into_iter()
        .map(|path| relative_display_path(&bundle_root, &path))
        .collect::<Result<BTreeSet<_>>>()?;
    let unexpected_files = actual_paths
        .difference(&expected_paths)
        .cloned()
        .collect::<Vec<_>>();

    let snapshot_entry = &manifest.files[0];
    let bundled_snapshot_path = bundle_root.join(&snapshot_entry.bundle_relative_path);
    let mut metadata_mismatches = Vec::new();
    let bundled_file_name = bundled_snapshot_path
        .file_name()
        .and_then(|value| value.to_str())
        .map(str::to_owned);
    if bundled_file_name.as_deref() != Some(manifest.selected_snapshot_file_name.as_str()) {
        metadata_mismatches
            .push("selected_snapshot_file_name mismatch with bundled artifact".to_string());
    }
    let source_relative_file_name = Path::new(&snapshot_entry.source_relative_path)
        .file_name()
        .and_then(|value| value.to_str())
        .map(str::to_owned);
    if source_relative_file_name.as_deref() != Some(manifest.selected_snapshot_file_name.as_str()) {
        metadata_mismatches
            .push("selected_snapshot_file_name mismatch with source_relative_path".to_string());
    }
    let selected_source_path = Path::new(&manifest.selected_snapshot_source_path);
    if !selected_source_path.is_absolute() {
        metadata_mismatches
            .push("selected_snapshot_source_path must be an absolute path".to_string());
    } else {
        let selected_source_file_name = selected_source_path
            .file_name()
            .and_then(|value| value.to_str())
            .map(str::to_owned);
        if selected_source_file_name.as_deref()
            != Some(manifest.selected_snapshot_file_name.as_str())
        {
            metadata_mismatches.push(
                "selected_snapshot_source_path mismatch with selected_snapshot_file_name"
                    .to_string(),
            );
        }
        let source_archive_root = Path::new(&manifest.source_state_archive_dir);
        if !source_archive_root.is_absolute() {
            metadata_mismatches
                .push("source_state_archive_dir must be an absolute path".to_string());
        } else {
            let expected_selected_source_path =
                source_archive_root.join(&snapshot_entry.source_relative_path);
            if expected_selected_source_path != selected_source_path {
                metadata_mismatches.push(
                    "selected_snapshot_source_path mismatch with source_relative_path".to_string(),
                );
            }
        }
    }
    if bundled_snapshot_path.exists() {
        match activation_artifact_state_publish_report::inspect_state_snapshot_artifact(
            &bundled_snapshot_path,
        ) {
            Ok(loaded) => {
                if loaded.artifact.snapshotted_at != manifest.snapshotted_at {
                    metadata_mismatches.push("snapshotted_at mismatch".to_string());
                }
                if loaded.artifact.state_verdict != manifest.state_verdict {
                    metadata_mismatches.push("state_verdict mismatch".to_string());
                }
                if loaded.artifact.state_reason != manifest.state_reason {
                    metadata_mismatches.push("state_reason mismatch".to_string());
                }
                if loaded.artifact.selected_review_generation_id
                    != manifest.selected_review_generation_id
                {
                    metadata_mismatches.push("selected_review_generation_id mismatch".to_string());
                }
                if loaded.artifact.selected_latest_release_generation_id
                    != manifest.selected_latest_release_generation_id
                {
                    metadata_mismatches
                        .push("selected_latest_release_generation_id mismatch".to_string());
                }
                if loaded.artifact.selection_alignment_matches
                    != manifest.selection_alignment_matches
                {
                    metadata_mismatches.push("selection_alignment_matches mismatch".to_string());
                }
                if loaded.artifact.selection_alignment_summary
                    != manifest.selection_alignment_summary
                {
                    metadata_mismatches.push("selection_alignment_summary mismatch".to_string());
                }
                if loaded.artifact.ambiguous_legacy_count != manifest.ambiguous_legacy_count {
                    metadata_mismatches.push("ambiguous_legacy_count mismatch".to_string());
                }
                if loaded.artifact.coherent_for_review_operations
                    != manifest.coherent_for_review_operations
                {
                    metadata_mismatches.push("coherent_for_review_operations mismatch".to_string());
                }
                if loaded.artifact.artifact_state_only != manifest.artifact_state_only {
                    metadata_mismatches.push("artifact_state_only mismatch".to_string());
                }
                if loaded.artifact.execution_untouched != manifest.execution_untouched {
                    metadata_mismatches.push("execution_untouched mismatch".to_string());
                }
                if loaded.artifact.activation_authorized != manifest.activation_authorized {
                    metadata_mismatches.push("activation_authorized mismatch".to_string());
                }
            }
            Err(error) => metadata_mismatches.push(format!(
                "bundled state snapshot artifact failed to parse: {error:#}"
            )),
        }
    }

    let verdict = if missing_files.is_empty()
        && changed_files.is_empty()
        && unexpected_files.is_empty()
        && metadata_mismatches.is_empty()
    {
        ArtifactStateBundleVerdict::ArtifactStateBundleVerified
    } else {
        ArtifactStateBundleVerdict::ArtifactStateBundleDriftDetected
    };
    let reason = match verdict {
        ArtifactStateBundleVerdict::ArtifactStateBundleVerified => format!(
            "state snapshot bundle verification passed for {}",
            manifest.selected_snapshot_file_name
        ),
        ArtifactStateBundleVerdict::ArtifactStateBundleDriftDetected => format!(
            "state snapshot bundle verification detected drift: {} missing, {} changed, {} unexpected, {} metadata mismatch(es)",
            missing_files.len(),
            changed_files.len(),
            unexpected_files.len(),
            metadata_mismatches.len()
        ),
        ArtifactStateBundleVerdict::ArtifactStateBundleExported
        | ArtifactStateBundleVerdict::ArtifactStateBundleInvalid
        | ArtifactStateBundleVerdict::ArtifactStateBundleSnapshotNotFound => unreachable!(),
    };

    Ok(VerifyReport {
        mode: "verify_bundle".to_string(),
        verdict,
        reason,
        bundle_path: bundle_root.display().to_string(),
        source_state_archive_dir: Some(manifest.source_state_archive_dir),
        selected_snapshot_path: Some(manifest.selected_snapshot_source_path),
        selected_snapshot_file_name: Some(manifest.selected_snapshot_file_name),
        snapshotted_at: Some(manifest.snapshotted_at),
        selected_snapshot_state_verdict: Some(manifest.state_verdict),
        selected_snapshot_state_reason: Some(manifest.state_reason),
        selected_review_generation_id: manifest.selected_review_generation_id,
        selected_latest_release_generation_id: manifest.selected_latest_release_generation_id,
        ambiguous_legacy_count: Some(manifest.ambiguous_legacy_count),
        coherent_for_review_operations: Some(manifest.coherent_for_review_operations),
        file_count: manifest.file_count,
        missing_files,
        changed_files,
        unexpected_files,
        metadata_mismatches,
        read_only_bundle_analysis: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary:
            "State snapshot bundle verification only validates bundle integrity. It preserves snapshot state truth but does not authorize activation and does not override the Stage 3 production gate.".to_string(),
    })
}

#[allow(clippy::too_many_arguments)]
fn invalid_verify_report(
    bundle_root: &Path,
    reason: String,
    source_state_archive_dir: Option<String>,
    selected_snapshot_path: Option<String>,
    selected_snapshot_file_name: Option<String>,
    snapshotted_at: Option<DateTime<Utc>>,
    selected_snapshot_state_verdict: Option<String>,
    selected_snapshot_state_reason: Option<String>,
    selected_review_generation_id: Option<String>,
    selected_latest_release_generation_id: Option<String>,
    ambiguous_legacy_count: Option<usize>,
    missing_files: Vec<String>,
    changed_files: Vec<String>,
    unexpected_files: Vec<String>,
    metadata_mismatches: Vec<String>,
    file_count: usize,
) -> VerifyReport {
    VerifyReport {
        mode: "verify_bundle".to_string(),
        verdict: ArtifactStateBundleVerdict::ArtifactStateBundleInvalid,
        reason,
        bundle_path: bundle_root.display().to_string(),
        source_state_archive_dir,
        selected_snapshot_path,
        selected_snapshot_file_name,
        snapshotted_at,
        selected_snapshot_state_verdict,
        selected_snapshot_state_reason,
        selected_review_generation_id,
        selected_latest_release_generation_id,
        ambiguous_legacy_count,
        coherent_for_review_operations: None,
        file_count,
        missing_files,
        changed_files,
        unexpected_files,
        metadata_mismatches,
        read_only_bundle_analysis: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary:
            "State snapshot bundle verification only validates bundle integrity. It preserves snapshot state truth but does not authorize activation and does not override the Stage 3 production gate.".to_string(),
    }
}

fn resolve_selected_snapshot(
    state_archive_dir: &Path,
    selector: &str,
) -> Result<SnapshotSelection> {
    let canonical_archive_root = canonical_archive_root(state_archive_dir)?;
    let mut valid = Vec::new();
    let mut invalid = Vec::new();
    for path in collect_snapshot_paths(state_archive_dir)? {
        match activation_artifact_state_publish_report::inspect_state_snapshot_artifact(&path) {
            Ok(loaded) => valid.push(loaded),
            Err(error) => invalid.push((path, format!("{error:#}"))),
        }
    }

    if let Some(path_like_selection) =
        resolve_path_like_selection(&canonical_archive_root, selector, &valid, &invalid)?
    {
        return Ok(path_like_selection);
    }

    let file_name_matches = valid
        .iter()
        .filter(|loaded| {
            loaded
                .canonical_path
                .file_name()
                .and_then(|value| value.to_str())
                == Some(selector)
        })
        .cloned()
        .collect::<Vec<_>>();
    if file_name_matches.len() > 1 {
        return Ok(SnapshotSelection::Invalid(format!(
            "snapshot selector `{selector}` is ambiguous; multiple persisted state snapshots share that file name"
        )));
    }
    if let Some(loaded) = file_name_matches.into_iter().next() {
        return Ok(SnapshotSelection::Selected(SelectedSnapshot {
            source_relative_path: relative_display_path(
                &canonical_archive_root,
                &loaded.canonical_path,
            )?,
            loaded,
        }));
    }

    let selector_ts = match DateTime::parse_from_rfc3339(selector) {
        Ok(ts) => ts.with_timezone(&Utc),
        Err(_) => return Ok(SnapshotSelection::NotFound),
    };
    let timestamp_matches = valid
        .into_iter()
        .filter(|loaded| loaded.artifact.snapshotted_at == selector_ts)
        .collect::<Vec<_>>();
    if timestamp_matches.len() > 1 {
        return Ok(SnapshotSelection::Invalid(format!(
            "snapshot selector `{selector}` is ambiguous; multiple persisted state snapshots share that snapshotted_at"
        )));
    }
    if let Some(loaded) = timestamp_matches.into_iter().next() {
        return Ok(SnapshotSelection::Selected(SelectedSnapshot {
            source_relative_path: relative_display_path(
                &canonical_archive_root,
                &loaded.canonical_path,
            )?,
            loaded,
        }));
    }

    Ok(SnapshotSelection::NotFound)
}

fn resolve_path_like_selection(
    canonical_archive_root: &Path,
    selector: &str,
    valid: &[activation_artifact_state_publish_report::LoadedStateSnapshot],
    invalid: &[(PathBuf, String)],
) -> Result<Option<SnapshotSelection>> {
    let path_like = selector.contains(std::path::MAIN_SEPARATOR) || selector.ends_with(".json");
    if !path_like && Path::new(selector).is_absolute() {
        return Ok(Some(SnapshotSelection::Invalid(format!(
            "snapshot selector `{selector}` points outside the state snapshot archive"
        ))));
    }
    if !path_like {
        return Ok(None);
    }

    let candidate_path = if Path::new(selector).is_absolute() {
        PathBuf::from(selector)
    } else {
        canonical_archive_root.join(selector)
    };
    let canonical_candidate = match fs::canonicalize(&candidate_path) {
        Ok(path) => path,
        Err(_) => return Ok(Some(SnapshotSelection::NotFound)),
    };
    if !canonical_candidate.starts_with(canonical_archive_root) {
        return Ok(Some(SnapshotSelection::Invalid(format!(
            "snapshot selector `{selector}` resolves outside the state snapshot archive"
        ))));
    }
    if let Some(loaded) = valid
        .iter()
        .find(|loaded| loaded.canonical_path == canonical_candidate)
        .cloned()
    {
        return Ok(Some(SnapshotSelection::Selected(SelectedSnapshot {
            source_relative_path: relative_display_path(
                canonical_archive_root,
                &loaded.canonical_path,
            )?,
            loaded,
        })));
    }
    if invalid
        .iter()
        .any(|(path, _)| fs::canonicalize(path).ok().as_ref() == Some(&canonical_candidate))
    {
        return Ok(Some(SnapshotSelection::Invalid(format!(
            "snapshot selector `{selector}` matched a malformed persisted state snapshot"
        ))));
    }
    Ok(Some(SnapshotSelection::NotFound))
}

fn collect_snapshot_paths(archive_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    if !archive_dir.exists() {
        return Ok(paths);
    }
    for entry in fs::read_dir(archive_dir)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_file()
            && path
                .extension()
                .and_then(|value| value.to_str())
                .is_some_and(|value| value.eq_ignore_ascii_case("json"))
        {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

fn ensure_bundle_output_dir(output_dir: &Path) -> Result<()> {
    if output_dir.exists() {
        let metadata = fs::metadata(output_dir)
            .with_context(|| format!("failed reading output path {}", output_dir.display()))?;
        if !metadata.is_dir() {
            bail!(
                "state snapshot bundle output path {} exists and is not a directory",
                output_dir.display()
            );
        }
        let mut entries = fs::read_dir(output_dir)
            .with_context(|| format!("failed reading output dir {}", output_dir.display()))?;
        if entries.next().is_some() {
            bail!(
                "state snapshot bundle output dir {} must be empty to avoid overwriting existing bundle contents",
                output_dir.display()
            );
        }
    } else {
        fs::create_dir_all(output_dir).with_context(|| {
            format!(
                "failed creating state snapshot bundle output dir {}",
                output_dir.display()
            )
        })?;
    }
    Ok(())
}

fn resolve_bundle_root(path: &Path) -> Result<PathBuf> {
    if path.is_dir() {
        return Ok(path.to_path_buf());
    }
    if path.file_name().and_then(|name| name.to_str()) == Some(BUNDLE_MANIFEST_FILENAME) {
        return path.parent().map(Path::to_path_buf).ok_or_else(|| {
            anyhow!(
                "state snapshot bundle manifest path {} has no parent directory",
                path.display()
            )
        });
    }
    bail!(
        "verify-bundle expects a bundle directory or {} path, got {}",
        BUNDLE_MANIFEST_FILENAME,
        path.display()
    )
}

fn validate_safe_bundle_relative_path(relative_path: &str) -> Result<()> {
    let path = Path::new(relative_path);
    if path.is_absolute() {
        bail!("bundle relative path {relative_path} must not be absolute");
    }
    if !relative_path.starts_with(&format!("{BUNDLE_ARTIFACTS_DIR}/")) {
        bail!("bundle relative path {relative_path} must stay under {BUNDLE_ARTIFACTS_DIR}/");
    }
    for component in path.components() {
        if matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        ) {
            bail!("bundle relative path {relative_path} contains forbidden traversal");
        }
    }
    Ok(())
}

fn collect_bundle_artifact_paths(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = fs::read_dir(&dir)
            .with_context(|| format!("failed reading bundle dir {}", dir.display()))?;
        for entry in entries {
            let entry =
                entry.with_context(|| format!("failed reading entry in {}", dir.display()))?;
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.is_file() {
                files.push(path);
            }
        }
    }
    files.sort();
    Ok(files)
}

fn collect_bundle_manifest_paths(root: &Path, paths: &mut BTreeSet<PathBuf>) -> Result<()> {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = fs::read_dir(&dir)
            .with_context(|| format!("failed reading bundle dir {}", dir.display()))?;
        for entry in entries {
            let entry =
                entry.with_context(|| format!("failed reading entry in {}", dir.display()))?;
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.is_file()
                && path.file_name().and_then(|value| value.to_str())
                    == Some(BUNDLE_MANIFEST_FILENAME)
            {
                paths.insert(path);
            }
        }
    }
    Ok(())
}

fn relative_display_path(root: &Path, path: &Path) -> Result<String> {
    let relative = path.strip_prefix(root).with_context(|| {
        format!(
            "path {} is outside expected root {}",
            path.display(),
            root.display()
        )
    })?;
    Ok(relative.to_string_lossy().to_string())
}

fn hash_file(path: &Path) -> Result<String> {
    let contents = fs::read(path)
        .with_context(|| format!("failed reading artifact file {}", path.display()))?;
    let mut hasher = Sha256::new();
    hasher.update(contents);
    Ok(format!("{:x}", hasher.finalize()))
}

fn canonical_archive_root(path: &Path) -> Result<PathBuf> {
    fs::canonicalize(path).with_context(|| {
        format!(
            "failed canonicalizing state snapshot archive {}",
            path.display()
        )
    })
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

fn write_output(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed creating output dir {}", parent.display()))?;
        }
    }
    fs::write(path, contents).with_context(|| format!("failed writing {}", path.display()))
}

fn render_export_human(report: &ExportReport) -> String {
    [
        "event=copybot_activation_artifact_state_bundle".to_string(),
        "mode=export_bundle".to_string(),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("state_archive_dir={}", report.state_archive_dir),
        format!("snapshot_selector={}", report.snapshot_selector),
        format!(
            "selected_snapshot_path={}",
            report
                .selected_snapshot_path
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
            "state_verdict={}",
            report
                .state_verdict
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
        format!("output_path={}", report.output_path),
        format!("bundled_file_count={}", report.bundled_file_count),
        format!(
            "bundled_file_paths={}",
            report.bundled_file_paths.join(" | ")
        ),
        format!("hash_coverage_count={}", report.hash_coverage_count),
        format!(
            "read_only_state_snapshot_artifacts={}",
            report.read_only_state_snapshot_artifacts
        ),
        format!("execution_untouched={}", report.execution_untouched),
        format!("activation_authorized={}", report.activation_authorized),
        format!("not_authorized_summary={}", report.not_authorized_summary),
    ]
    .join("\n")
}

fn render_verify_human(report: &VerifyReport) -> String {
    [
        "event=copybot_activation_artifact_state_bundle".to_string(),
        "mode=verify_bundle".to_string(),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("bundle_path={}", report.bundle_path),
        format!(
            "selected_snapshot_path={}",
            report
                .selected_snapshot_path
                .clone()
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
        format!("file_count={}", report.file_count),
        format!("missing_files={}", report.missing_files.join(" | ")),
        format!("changed_files={}", report.changed_files.join(" | ")),
        format!("unexpected_files={}", report.unexpected_files.join(" | ")),
        format!(
            "metadata_mismatches={}",
            report.metadata_mismatches.join(" | ")
        ),
        format!(
            "read_only_bundle_analysis={}",
            report.read_only_bundle_analysis
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

enum SnapshotSelection {
    Selected(SelectedSnapshot),
    NotFound,
    Invalid(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn export_succeeds_for_valid_selected_state_snapshot() {
        let archive_dir = temp_dir("state_snapshot_bundle_export");
        let older = archive_dir.join("older.json");
        let newer = archive_dir.join("newer.json");
        write_snapshot(
            &older,
            "artifact_state_coherent",
            "review_a",
            "release_a",
            true,
            0,
            ts("2026-03-26T12:00:00Z"),
        );
        write_snapshot(
            &newer,
            "artifact_state_coherent",
            "review_b",
            "release_b",
            true,
            0,
            ts("2026-03-26T13:00:00Z"),
        );
        let output_dir = temp_dir("state_snapshot_bundle_export_out").join("bundle");

        let report = export_bundle(&archive_dir, "2026-03-26T13:00:00Z", &output_dir)
            .expect("export bundle");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleVerdict::ArtifactStateBundleExported
        );
        assert_eq!(report.bundled_file_count, 1);
        assert!(output_dir.join(BUNDLE_MANIFEST_FILENAME).exists());
        assert!(output_dir.join("artifacts/newer.json").exists());
        assert!(!output_dir.join("artifacts/older.json").exists());
    }

    #[test]
    fn verify_passes_on_unchanged_exported_bundle() {
        let archive_dir = temp_dir("state_snapshot_bundle_verify");
        let snapshot = archive_dir.join("snapshot.json");
        write_snapshot(
            &snapshot,
            "artifact_state_coherent",
            "review_a",
            "release_a",
            true,
            0,
            ts("2026-03-26T12:00:00Z"),
        );
        let output_dir = temp_dir("state_snapshot_bundle_verify_out").join("bundle");
        export_bundle(&archive_dir, "snapshot.json", &output_dir).expect("export");

        let report = verify_bundle(&output_dir).expect("verify");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleVerdict::ArtifactStateBundleVerified
        );
        assert_eq!(
            report.selected_snapshot_state_verdict.as_deref(),
            Some("artifact_state_coherent")
        );
        assert!(report.missing_files.is_empty());
        assert!(report.changed_files.is_empty());
    }

    #[test]
    fn snapshot_not_found_is_reported_cleanly() {
        let archive_dir = temp_dir("state_snapshot_bundle_not_found");
        let snapshot = archive_dir.join("snapshot.json");
        write_snapshot(
            &snapshot,
            "artifact_state_coherent",
            "review_a",
            "release_a",
            true,
            0,
            ts("2026-03-26T12:00:00Z"),
        );
        let output_dir = temp_dir("state_snapshot_bundle_not_found_out").join("bundle");

        let report = export_bundle(&archive_dir, "missing.json", &output_dir).expect("export");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleVerdict::ArtifactStateBundleSnapshotNotFound
        );
        assert!(!output_dir.exists());
    }

    #[test]
    fn tampered_bundled_snapshot_file_is_detected() {
        let archive_dir = temp_dir("state_snapshot_bundle_tampered");
        let snapshot = archive_dir.join("snapshot.json");
        write_snapshot(
            &snapshot,
            "artifact_state_coherent",
            "review_a",
            "release_a",
            true,
            0,
            ts("2026-03-26T12:00:00Z"),
        );
        let output_dir = temp_dir("state_snapshot_bundle_tampered_out").join("bundle");
        export_bundle(&archive_dir, "snapshot.json", &output_dir).expect("export");
        fs::write(
            output_dir.join("artifacts/snapshot.json"),
            "{\"tampered\":true}",
        )
        .expect("tamper snapshot");

        let report = verify_bundle(&output_dir).expect("verify");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleVerdict::ArtifactStateBundleDriftDetected
        );
        assert_eq!(
            report.changed_files,
            vec!["artifacts/snapshot.json".to_string()]
        );
    }

    #[test]
    fn tampered_selected_snapshot_file_name_is_detected() {
        let archive_dir = temp_dir("state_snapshot_bundle_tampered_file_name");
        let snapshot = archive_dir.join("snapshot.json");
        write_snapshot(
            &snapshot,
            "artifact_state_coherent",
            "review_a",
            "release_a",
            true,
            0,
            ts("2026-03-26T12:00:00Z"),
        );
        let output_dir = temp_dir("state_snapshot_bundle_tampered_file_name_out").join("bundle");
        export_bundle(&archive_dir, "snapshot.json", &output_dir).expect("export");
        let manifest_path = output_dir.join(BUNDLE_MANIFEST_FILENAME);
        let mut manifest: StateSnapshotBundleManifest =
            serde_json::from_str(&fs::read_to_string(&manifest_path).expect("read manifest"))
                .expect("parse manifest");
        manifest.selected_snapshot_file_name = "tampered.json".to_string();
        fs::write(
            &manifest_path,
            serde_json::to_string_pretty(&manifest).expect("serialize manifest"),
        )
        .expect("write manifest");

        let report = verify_bundle(&output_dir).expect("verify");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleVerdict::ArtifactStateBundleDriftDetected
        );
        assert!(report
            .metadata_mismatches
            .iter()
            .any(|value| value.contains("selected_snapshot_file_name mismatch")));
    }

    #[test]
    fn tampered_selected_snapshot_source_path_is_detected() {
        let archive_dir = temp_dir("state_snapshot_bundle_tampered_source_path");
        let snapshot = archive_dir.join("snapshot.json");
        write_snapshot(
            &snapshot,
            "artifact_state_coherent",
            "review_a",
            "release_a",
            true,
            0,
            ts("2026-03-26T12:00:00Z"),
        );
        let output_dir = temp_dir("state_snapshot_bundle_tampered_source_path_out").join("bundle");
        export_bundle(&archive_dir, "snapshot.json", &output_dir).expect("export");
        let manifest_path = output_dir.join(BUNDLE_MANIFEST_FILENAME);
        let mut manifest: StateSnapshotBundleManifest =
            serde_json::from_str(&fs::read_to_string(&manifest_path).expect("read manifest"))
                .expect("parse manifest");
        manifest.selected_snapshot_source_path =
            "/tmp/elsewhere/state_snapshots/snapshot.json".to_string();
        fs::write(
            &manifest_path,
            serde_json::to_string_pretty(&manifest).expect("serialize manifest"),
        )
        .expect("write manifest");

        let report = verify_bundle(&output_dir).expect("verify");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleVerdict::ArtifactStateBundleDriftDetected
        );
        assert!(report
            .metadata_mismatches
            .iter()
            .any(|value| value.contains("selected_snapshot_source_path")));
    }

    #[test]
    fn malformed_bundle_metadata_is_rejected() {
        let bundle_dir = temp_dir("state_snapshot_bundle_invalid");
        fs::write(bundle_dir.join(BUNDLE_MANIFEST_FILENAME), "{broken").expect("write manifest");

        let report = verify_bundle(&bundle_dir).expect("verify");

        assert_eq!(
            report.verdict,
            ArtifactStateBundleVerdict::ArtifactStateBundleInvalid
        );
    }

    #[test]
    fn ambiguous_snapshot_state_remains_explicit_in_bundle_metadata_and_verify_output() {
        let archive_dir = temp_dir("state_snapshot_bundle_ambiguous");
        let snapshot = archive_dir.join("snapshot.json");
        write_snapshot(
            &snapshot,
            "artifact_state_ambiguous_legacy_state",
            "review_a",
            "release_a",
            true,
            3,
            ts("2026-03-26T12:00:00Z"),
        );
        let output_dir = temp_dir("state_snapshot_bundle_ambiguous_out").join("bundle");
        export_bundle(&archive_dir, "snapshot.json", &output_dir).expect("export");

        let manifest_raw =
            fs::read_to_string(output_dir.join(BUNDLE_MANIFEST_FILENAME)).expect("read manifest");
        let manifest: StateSnapshotBundleManifest =
            serde_json::from_str(&manifest_raw).expect("parse manifest");
        assert_eq!(
            manifest.state_verdict,
            "artifact_state_ambiguous_legacy_state"
        );
        assert_eq!(manifest.ambiguous_legacy_count, 3);
        assert!(!manifest.coherent_for_review_operations);

        let verify = verify_bundle(&output_dir).expect("verify");
        assert_eq!(
            verify.verdict,
            ArtifactStateBundleVerdict::ArtifactStateBundleVerified
        );
        assert_eq!(
            verify.selected_snapshot_state_verdict.as_deref(),
            Some("artifact_state_ambiguous_legacy_state")
        );
        assert_eq!(verify.ambiguous_legacy_count, Some(3));
        assert_eq!(verify.coherent_for_review_operations, Some(false));
        assert!(!verify.activation_authorized);
    }

    #[test]
    fn archive_files_remain_unchanged_by_state_snapshot_bundle_export() {
        let archive_dir = temp_dir("state_snapshot_bundle_no_mutation");
        let snapshot = archive_dir.join("snapshot.json");
        write_snapshot(
            &snapshot,
            "artifact_state_coherent",
            "review_a",
            "release_a",
            true,
            0,
            ts("2026-03-26T12:00:00Z"),
        );
        let before = fs::read_to_string(&snapshot).expect("before");
        let output_dir = temp_dir("state_snapshot_bundle_no_mutation_out").join("bundle");

        let _ = export_bundle(&archive_dir, "snapshot.json", &output_dir).expect("export");

        let after = fs::read_to_string(&snapshot).expect("after");
        assert_eq!(before, after);
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
            review_channel_name: "current_review".to_string(),
            release_archive_dir: "/tmp/release_archive".to_string(),
            release_history_dir: "/tmp/release_history".to_string(),
            latest_release_pointer_dir: "/tmp/release_pointer".to_string(),
            latest_release_pointer_name: "latest_release".to_string(),
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
