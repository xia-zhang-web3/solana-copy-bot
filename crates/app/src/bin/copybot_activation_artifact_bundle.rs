#![recursion_limit = "256"]

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
#[path = "copybot_activation_artifact_archive.rs"]
mod activation_artifact_archive;

const USAGE: &str = "usage: copybot_activation_artifact_bundle [--json] (--archive-dir <path> --export-bundle --generation <id-or-rfc3339> --output <dir> | --verify-bundle <path>)";
const BUNDLE_VERSION: &str = "1";
const HASH_ALGORITHM: &str = "sha256";
const BUNDLE_MANIFEST_FILENAME: &str = "bundle_manifest.json";
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
        archive_dir: PathBuf,
        generation_selector: String,
        output_dir: PathBuf,
    },
    Verify {
        bundle_path: PathBuf,
    },
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ArtifactBundleVerdict {
    ArtifactBundleExported,
    ArtifactBundleVerified,
    ArtifactBundleInvalid,
    ArtifactBundleDriftDetected,
    ArtifactBundleGenerationNotFound,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
enum BundleArtifactKind {
    DecisionPacketJson,
    RunbookJson,
    RunbookMarkdown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct BundleGenerationIdentity {
    decision_packet_generated_at: DateTime<Utc>,
    prod_config_fingerprint_sha256: String,
    non_prod_config_fingerprint_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BundleFileEntry {
    artifact_kind: BundleArtifactKind,
    source_relative_path: String,
    bundle_relative_path: String,
    sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActivationArtifactBundleManifest {
    generated_at: DateTime<Utc>,
    bundle_version: String,
    build_version: String,
    git_commit: Option<String>,
    source_archive_dir: String,
    generation_id: String,
    generation_identity: BundleGenerationIdentity,
    latest_packet_verdict: Option<String>,
    latest_runbook_verdict: Option<String>,
    hash_algorithm: String,
    file_count: usize,
    files: Vec<BundleFileEntry>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ExportReport {
    pub(crate) mode: String,
    pub(crate) verdict: ArtifactBundleVerdict,
    pub(crate) reason: String,
    pub(crate) archive_dir: String,
    pub(crate) output_path: String,
    pub(crate) generation_selector: String,
    pub(crate) selected_generation_id: Option<String>,
    pub(crate) bundled_file_count: usize,
    pub(crate) bundled_file_paths: Vec<String>,
    pub(crate) hash_coverage_count: usize,
    pub(crate) read_only_archive_artifacts: bool,
    pub(crate) execution_untouched: bool,
    pub(crate) activation_authorized: bool,
    pub(crate) not_authorized_summary: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct VerifyReport {
    pub(crate) mode: String,
    pub(crate) verdict: ArtifactBundleVerdict,
    pub(crate) reason: String,
    pub(crate) bundle_path: String,
    pub(crate) generation_id: Option<String>,
    pub(crate) file_count: usize,
    pub(crate) missing_files: Vec<String>,
    pub(crate) changed_files: Vec<String>,
    pub(crate) unexpected_files: Vec<String>,
    pub(crate) metadata_mismatches: Vec<String>,
    pub(crate) invalid_artifact_count: usize,
    pub(crate) invalid_artifacts: Vec<activation_artifact_archive::InvalidArtifact>,
    pub(crate) read_only_bundle_analysis: bool,
    pub(crate) execution_untouched: bool,
    pub(crate) activation_authorized: bool,
    pub(crate) not_authorized_summary: String,
}

#[derive(Debug, Clone)]
struct SelectedGeneration {
    summary: activation_artifact_archive::ArchiveArtifactGenerationSummary,
    generation_id: String,
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
    let mut archive_dir: Option<PathBuf> = None;
    let mut export_bundle = false;
    let mut generation_selector: Option<String> = None;
    let mut output_dir: Option<PathBuf> = None;
    let mut verify_bundle: Option<PathBuf> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--json" => json = true,
            "--archive-dir" => {
                archive_dir = Some(PathBuf::from(parse_string_arg(
                    "--archive-dir",
                    args.next(),
                )?))
            }
            "--export-bundle" => export_bundle = true,
            "--generation" => {
                generation_selector = Some(parse_string_arg("--generation", args.next())?)
            }
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
        archive_dir,
        export_bundle,
        generation_selector,
        output_dir,
        verify_bundle,
    ) {
        (Some(archive_dir), true, Some(generation_selector), Some(output_dir), None) => {
            Mode::Export {
                archive_dir,
                generation_selector,
                output_dir,
            }
        }
        (None, false, None, None, Some(bundle_path)) => Mode::Verify { bundle_path },
        (Some(_), true, None, _, None) => {
            bail!("--export-bundle requires --generation <id-or-rfc3339>")
        }
        (Some(_), true, Some(_), None, None) => bail!("--export-bundle requires --output <dir>"),
        (Some(_), false, _, _, None) => bail!("--archive-dir requires --export-bundle"),
        (None, true, _, _, None) => bail!("--export-bundle requires --archive-dir <path>"),
        (_, true, _, _, Some(_)) => bail!("--export-bundle and --verify-bundle cannot be combined"),
        (Some(_), _, _, _, Some(_)) => {
            bail!("--archive-dir cannot be combined with --verify-bundle")
        }
        (None, false, Some(_), _, None) => bail!("--generation requires --export-bundle"),
        (None, false, None, Some(_), None) => bail!("--output requires --export-bundle"),
        (None, false, None, None, None) => {
            bail!("either --export-bundle or --verify-bundle <path> is required")
        }
        _ => bail!("invalid activation artifact bundle arguments"),
    };

    Ok(Some(Config { json, mode }))
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    value.ok_or_else(|| anyhow!("{flag} requires a value"))
}

fn run(config: Config) -> Result<String> {
    match config.mode {
        Mode::Export {
            archive_dir,
            generation_selector,
            output_dir,
        } => {
            let report = export_bundle(&archive_dir, &generation_selector, &output_dir)?;
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

pub(crate) fn export_bundle(
    archive_dir: &Path,
    generation_selector: &str,
    output_dir: &Path,
) -> Result<ExportReport> {
    let inventory = activation_artifact_archive::archive_inventory(archive_dir)?;
    if !inventory.invalid_artifacts.is_empty() {
        return Ok(ExportReport {
            mode: "export_bundle".to_string(),
            verdict: ArtifactBundleVerdict::ArtifactBundleInvalid,
            reason: format!(
                "bundle export is blocked because the archive contains {} invalid artifact(s)",
                inventory.invalid_artifacts.len()
            ),
            archive_dir: archive_dir.display().to_string(),
            output_path: output_dir.display().to_string(),
            generation_selector: generation_selector.to_string(),
            selected_generation_id: None,
            bundled_file_count: 0,
            bundled_file_paths: Vec::new(),
            hash_coverage_count: 0,
            read_only_archive_artifacts: true,
            execution_untouched: true,
            activation_authorized: false,
            not_authorized_summary:
                "Bundle export only handles exported activation artifacts. It does not authorize activation and does not override the Stage 3 production gate.".to_string(),
        });
    }

    let Some(selected) = resolve_generation(&inventory, generation_selector)? else {
        return Ok(ExportReport {
            mode: "export_bundle".to_string(),
            verdict: ArtifactBundleVerdict::ArtifactBundleGenerationNotFound,
            reason: format!(
                "no packet-backed generation matched selector `{generation_selector}`"
            ),
            archive_dir: archive_dir.display().to_string(),
            output_path: output_dir.display().to_string(),
            generation_selector: generation_selector.to_string(),
            selected_generation_id: None,
            bundled_file_count: 0,
            bundled_file_paths: Vec::new(),
            hash_coverage_count: 0,
            read_only_archive_artifacts: true,
            execution_untouched: true,
            activation_authorized: false,
            not_authorized_summary:
                "Bundle export only handles exported activation artifacts. It does not authorize activation and does not override the Stage 3 production gate.".to_string(),
        });
    };

    ensure_bundle_output_dir(output_dir)?;
    let bundle_root = output_dir.to_path_buf();
    let artifacts_root = bundle_root.join(BUNDLE_ARTIFACTS_DIR);
    fs::create_dir_all(&artifacts_root).with_context(|| {
        format!(
            "failed creating bundle artifacts dir {}",
            artifacts_root.display()
        )
    })?;

    let mut files = Vec::new();
    for path in &selected.summary.decision_packet_paths {
        files.push(build_bundle_file_entry(
            archive_dir,
            Path::new(path),
            BundleArtifactKind::DecisionPacketJson,
        )?);
    }
    for path in &selected.summary.runbook_json_paths {
        files.push(build_bundle_file_entry(
            archive_dir,
            Path::new(path),
            BundleArtifactKind::RunbookJson,
        )?);
    }
    for path in &selected.summary.runbook_markdown_paths {
        files.push(build_bundle_file_entry(
            archive_dir,
            Path::new(path),
            BundleArtifactKind::RunbookMarkdown,
        )?);
    }
    files.sort_by(|left, right| left.bundle_relative_path.cmp(&right.bundle_relative_path));

    for file in &files {
        let target_path = bundle_root.join(&file.bundle_relative_path);
        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed creating bundle subdir {}", parent.display()))?;
        }
        fs::copy(archive_dir.join(&file.source_relative_path), &target_path).with_context(
            || {
                format!(
                    "failed copying {} into bundle {}",
                    archive_dir.join(&file.source_relative_path).display(),
                    target_path.display()
                )
            },
        )?;
    }

    let manifest = ActivationArtifactBundleManifest {
        generated_at: Utc::now(),
        bundle_version: BUNDLE_VERSION.to_string(),
        build_version: env!("CARGO_PKG_VERSION").to_string(),
        git_commit: resolve_git_commit(),
        source_archive_dir: archive_dir.display().to_string(),
        generation_id: selected.generation_id.clone(),
        generation_identity: BundleGenerationIdentity {
            decision_packet_generated_at: selected.summary.decision_packet_generated_at,
            prod_config_fingerprint_sha256: selected.summary.prod_config_fingerprint_sha256.clone(),
            non_prod_config_fingerprint_sha256: selected
                .summary
                .non_prod_config_fingerprint_sha256
                .clone(),
        },
        latest_packet_verdict: selected.summary.latest_packet_verdict.clone(),
        latest_runbook_verdict: selected.summary.latest_runbook_verdict.clone(),
        hash_algorithm: HASH_ALGORITHM.to_string(),
        file_count: files.len(),
        files: files.clone(),
    };
    write_output(
        &bundle_root.join(BUNDLE_MANIFEST_FILENAME),
        &serde_json::to_string_pretty(&manifest)?,
    )?;

    Ok(ExportReport {
        mode: "export_bundle".to_string(),
        verdict: ArtifactBundleVerdict::ArtifactBundleExported,
        reason: format!(
            "bundle exported for generation {} with {} file(s)",
            selected.generation_id,
            files.len()
        ),
        archive_dir: archive_dir.display().to_string(),
        output_path: bundle_root.display().to_string(),
        generation_selector: generation_selector.to_string(),
        selected_generation_id: Some(selected.generation_id),
        bundled_file_count: files.len(),
        bundled_file_paths: files
            .iter()
            .map(|entry| entry.bundle_relative_path.clone())
            .collect(),
        hash_coverage_count: files.len(),
        read_only_archive_artifacts: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary:
            "Bundle export only packages already exported activation artifacts for transfer/review. It does not authorize activation and does not override the Stage 3 production gate.".to_string(),
    })
}

pub(crate) fn verify_bundle(bundle_path: &Path) -> Result<VerifyReport> {
    let bundle_root = resolve_bundle_root(bundle_path)?;
    let manifest_path = bundle_root.join(BUNDLE_MANIFEST_FILENAME);
    let raw_manifest = match fs::read_to_string(&manifest_path) {
        Ok(raw) => raw,
        Err(error) => {
            return Ok(invalid_verify_report(
                &bundle_root,
                format!("failed reading bundle manifest: {error}"),
                None,
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                0,
            ))
        }
    };
    let manifest: ActivationArtifactBundleManifest = match serde_json::from_str(&raw_manifest) {
        Ok(manifest) => manifest,
        Err(error) => {
            return Ok(invalid_verify_report(
                &bundle_root,
                format!("failed parsing bundle manifest: {error}"),
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
            "bundle manifest has empty bundle_version".to_string(),
            Some(manifest.generation_id),
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
                "bundle manifest hash_algorithm `{}` is unsupported; expected `{HASH_ALGORITHM}`",
                manifest.hash_algorithm
            ),
            Some(manifest.generation_id),
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
            verdict: ArtifactBundleVerdict::ArtifactBundleDriftDetected,
            reason: "bundle artifacts directory is missing".to_string(),
            bundle_path: bundle_root.display().to_string(),
            generation_id: Some(manifest.generation_id),
            file_count: manifest.file_count,
            missing_files: manifest
                .files
                .iter()
                .map(|entry| entry.bundle_relative_path.clone())
                .collect(),
            changed_files: Vec::new(),
            unexpected_files: Vec::new(),
            metadata_mismatches: Vec::new(),
            invalid_artifact_count: 0,
            invalid_artifacts: Vec::new(),
            read_only_bundle_analysis: true,
            execution_untouched: true,
            activation_authorized: false,
            not_authorized_summary:
                "Bundle verification only validates bundled artifact integrity. It does not authorize activation and does not override the Stage 3 production gate.".to_string(),
        });
    }

    let inventory = activation_artifact_archive::archive_inventory(&artifacts_root)?;
    let mut metadata_mismatches = Vec::new();
    if !inventory.invalid_artifacts.is_empty() {
        metadata_mismatches.push(format!(
            "bundle artifacts subtree contains {} invalid artifact(s)",
            inventory.invalid_artifacts.len()
        ));
    }
    if inventory.generation_summaries.len() != 1 {
        metadata_mismatches.push(format!(
            "bundle artifacts subtree should contain exactly 1 generation but found {}",
            inventory.generation_summaries.len()
        ));
    }
    if !inventory.orphan_markdown_paths.is_empty() {
        metadata_mismatches.push(format!(
            "bundle artifacts subtree has {} orphan markdown artifact(s)",
            inventory.orphan_markdown_paths.len()
        ));
    }
    if let Some(summary) = inventory.generation_summaries.first() {
        let current_generation_id = generation_id_from_summary(summary);
        if current_generation_id != manifest.generation_id {
            metadata_mismatches.push(format!(
                "bundle manifest generation_id {} does not match bundled generation {}",
                manifest.generation_id, current_generation_id
            ));
        }
        if summary.latest_packet_verdict != manifest.latest_packet_verdict {
            metadata_mismatches.push("latest_packet_verdict mismatch".to_string());
        }
        if summary.latest_runbook_verdict != manifest.latest_runbook_verdict {
            metadata_mismatches.push("latest_runbook_verdict mismatch".to_string());
        }
    }

    let mut missing_files = Vec::new();
    let mut changed_files = Vec::new();
    for entry in &manifest.files {
        let bundle_file_path = bundle_root.join(&entry.bundle_relative_path);
        if !bundle_file_path.exists() {
            missing_files.push(entry.bundle_relative_path.clone());
            continue;
        }
        validate_safe_bundle_relative_path(&entry.bundle_relative_path)?;
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

    let verdict = if !missing_files.is_empty()
        || !changed_files.is_empty()
        || !unexpected_files.is_empty()
        || !metadata_mismatches.is_empty()
    {
        ArtifactBundleVerdict::ArtifactBundleDriftDetected
    } else {
        ArtifactBundleVerdict::ArtifactBundleVerified
    };
    let reason = match verdict {
        ArtifactBundleVerdict::ArtifactBundleVerified => format!(
            "bundle verification passed for generation {} with {} file(s)",
            manifest.generation_id, manifest.file_count
        ),
        ArtifactBundleVerdict::ArtifactBundleDriftDetected => format!(
            "bundle verification detected drift: {} missing, {} changed, {} unexpected, {} metadata mismatch(es)",
            missing_files.len(),
            changed_files.len(),
            unexpected_files.len(),
            metadata_mismatches.len()
        ),
        ArtifactBundleVerdict::ArtifactBundleExported
        | ArtifactBundleVerdict::ArtifactBundleInvalid
        | ArtifactBundleVerdict::ArtifactBundleGenerationNotFound => unreachable!(),
    };

    Ok(VerifyReport {
        mode: "verify_bundle".to_string(),
        verdict,
        reason,
        bundle_path: bundle_root.display().to_string(),
        generation_id: Some(manifest.generation_id),
        file_count: manifest.file_count,
        missing_files,
        changed_files,
        unexpected_files,
        metadata_mismatches,
        invalid_artifact_count: inventory.invalid_artifacts.len(),
        invalid_artifacts: inventory.invalid_artifacts,
        read_only_bundle_analysis: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary:
            "Bundle verification only validates bundled artifact integrity. It does not authorize activation and does not override the Stage 3 production gate.".to_string(),
    })
}

fn invalid_verify_report(
    bundle_root: &Path,
    reason: String,
    generation_id: Option<String>,
    missing_files: Vec<String>,
    changed_files: Vec<String>,
    unexpected_files: Vec<String>,
    metadata_mismatches: Vec<String>,
    invalid_artifact_count: usize,
) -> VerifyReport {
    VerifyReport {
        mode: "verify_bundle".to_string(),
        verdict: ArtifactBundleVerdict::ArtifactBundleInvalid,
        reason,
        bundle_path: bundle_root.display().to_string(),
        generation_id,
        file_count: 0,
        missing_files,
        changed_files,
        unexpected_files,
        metadata_mismatches,
        invalid_artifact_count,
        invalid_artifacts: Vec::new(),
        read_only_bundle_analysis: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary:
            "Bundle verification only validates bundled artifact integrity. It does not authorize activation and does not override the Stage 3 production gate.".to_string(),
    }
}

fn ensure_bundle_output_dir(output_dir: &Path) -> Result<()> {
    if output_dir.exists() {
        let metadata = fs::metadata(output_dir)
            .with_context(|| format!("failed reading output path {}", output_dir.display()))?;
        if !metadata.is_dir() {
            bail!(
                "bundle output path {} exists and is not a directory",
                output_dir.display()
            );
        }
        let mut entries = fs::read_dir(output_dir)
            .with_context(|| format!("failed reading output dir {}", output_dir.display()))?;
        if entries.next().is_some() {
            bail!(
                "bundle output dir {} must be empty to avoid overwriting existing bundle contents",
                output_dir.display()
            );
        }
    } else {
        fs::create_dir_all(output_dir).with_context(|| {
            format!("failed creating bundle output dir {}", output_dir.display())
        })?;
    }
    Ok(())
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

fn build_bundle_file_entry(
    archive_dir: &Path,
    source_path: &Path,
    artifact_kind: BundleArtifactKind,
) -> Result<BundleFileEntry> {
    let source_relative_path = relative_display_path(archive_dir, source_path)?;
    let bundle_relative_path = format!("{BUNDLE_ARTIFACTS_DIR}/{source_relative_path}");
    Ok(BundleFileEntry {
        artifact_kind,
        source_relative_path: source_relative_path.clone(),
        bundle_relative_path,
        sha256: hash_file(source_path)?,
    })
}

fn resolve_bundle_root(path: &Path) -> Result<PathBuf> {
    if path.is_dir() {
        return Ok(path.to_path_buf());
    }
    if path.file_name().and_then(|name| name.to_str()) == Some(BUNDLE_MANIFEST_FILENAME) {
        return path.parent().map(Path::to_path_buf).ok_or_else(|| {
            anyhow!(
                "bundle manifest path {} has no parent directory",
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
        "event=copybot_activation_artifact_bundle".to_string(),
        "mode=export_bundle".to_string(),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("archive_dir={}", report.archive_dir),
        format!("output_path={}", report.output_path),
        format!("generation_selector={}", report.generation_selector),
        format!(
            "selected_generation_id={}",
            report
                .selected_generation_id
                .clone()
                .unwrap_or_else(|| "null".to_string())
        ),
        format!("bundled_file_count={}", report.bundled_file_count),
        format!(
            "bundled_file_paths={}",
            report.bundled_file_paths.join(" | ")
        ),
        format!("hash_coverage_count={}", report.hash_coverage_count),
        format!(
            "read_only_archive_artifacts={}",
            report.read_only_archive_artifacts
        ),
        format!("execution_untouched={}", report.execution_untouched),
        format!("activation_authorized={}", report.activation_authorized),
        format!("not_authorized_summary={}", report.not_authorized_summary),
    ]
    .join("\n")
}

fn render_verify_human(report: &VerifyReport) -> String {
    [
        "event=copybot_activation_artifact_bundle".to_string(),
        "mode=verify_bundle".to_string(),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("bundle_path={}", report.bundle_path),
        format!(
            "generation_id={}",
            report
                .generation_id
                .clone()
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
        format!("invalid_artifact_count={}", report.invalid_artifact_count),
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn export_succeeds_for_valid_selected_generation() {
        let archive_dir = temp_dir("activation_bundle_export");
        write_sample_generation(
            &archive_dir.join("older"),
            "2026-03-26T10:00:00Z",
            "prod_fp_old",
            "non_prod_fp_old",
        );
        write_sample_generation(
            &archive_dir.join("latest"),
            "2026-03-26T12:00:00Z",
            "prod_fp_new",
            "non_prod_fp_new",
        );
        let output_dir = temp_dir("activation_bundle_export_out").join("bundle");

        let report =
            export_bundle(&archive_dir, "2026-03-26T12:00:00Z", &output_dir).expect("export");

        assert_eq!(
            report.verdict,
            ArtifactBundleVerdict::ArtifactBundleExported
        );
        assert_eq!(report.bundled_file_count, 3);
        assert!(output_dir.join(BUNDLE_MANIFEST_FILENAME).exists());
        assert!(output_dir.join("artifacts/latest/packet.json").exists());
        assert!(!output_dir.join("artifacts/older/packet.json").exists());
    }

    #[test]
    fn verify_passes_on_unchanged_exported_bundle() {
        let archive_dir = temp_dir("activation_bundle_verify");
        write_sample_generation(
            &archive_dir,
            "2026-03-26T12:00:00Z",
            "prod_fp_a",
            "non_prod_fp_a",
        );
        let output_dir = temp_dir("activation_bundle_verify_out").join("bundle");
        export_bundle(&archive_dir, "2026-03-26T12:00:00Z", &output_dir).expect("export");

        let report = verify_bundle(&output_dir).expect("verify");

        assert_eq!(
            report.verdict,
            ArtifactBundleVerdict::ArtifactBundleVerified
        );
        assert!(report.missing_files.is_empty());
        assert!(report.changed_files.is_empty());
    }

    #[test]
    fn generation_not_found_is_reported_cleanly() {
        let archive_dir = temp_dir("activation_bundle_not_found");
        write_sample_generation(
            &archive_dir,
            "2026-03-26T12:00:00Z",
            "prod_fp_a",
            "non_prod_fp_a",
        );
        let output_dir = temp_dir("activation_bundle_not_found_out").join("bundle");

        let report =
            export_bundle(&archive_dir, "2026-03-26T13:00:00Z", &output_dir).expect("export");

        assert_eq!(
            report.verdict,
            ArtifactBundleVerdict::ArtifactBundleGenerationNotFound
        );
        assert!(!output_dir.exists());
    }

    #[test]
    fn tampered_bundle_file_is_detected() {
        let archive_dir = temp_dir("activation_bundle_tampered");
        write_sample_generation(
            &archive_dir,
            "2026-03-26T12:00:00Z",
            "prod_fp_a",
            "non_prod_fp_a",
        );
        let output_dir = temp_dir("activation_bundle_tampered_out").join("bundle");
        export_bundle(&archive_dir, "2026-03-26T12:00:00Z", &output_dir).expect("export");
        fs::write(
            output_dir.join("artifacts/packet.json"),
            "{\"tampered\":true}",
        )
        .expect("tamper packet");

        let report = verify_bundle(&output_dir).expect("verify");

        assert_eq!(
            report.verdict,
            ArtifactBundleVerdict::ArtifactBundleDriftDetected
        );
        assert_eq!(report.changed_files.len(), 1);
    }

    #[test]
    fn malformed_bundle_metadata_is_rejected() {
        let bundle_dir = temp_dir("activation_bundle_invalid");
        fs::write(bundle_dir.join(BUNDLE_MANIFEST_FILENAME), "{broken").expect("write manifest");

        let report = verify_bundle(&bundle_dir).expect("verify");

        assert_eq!(report.verdict, ArtifactBundleVerdict::ArtifactBundleInvalid);
    }

    #[test]
    fn archive_files_remain_unchanged_by_bundle_export() {
        let archive_dir = temp_dir("activation_bundle_no_mutation");
        let packet_path = write_sample_generation(
            &archive_dir,
            "2026-03-26T12:00:00Z",
            "prod_fp_a",
            "non_prod_fp_a",
        );
        let before = fs::read_to_string(&packet_path).expect("before");
        let output_dir = temp_dir("activation_bundle_no_mutation_out").join("bundle");

        let _ = export_bundle(&archive_dir, "2026-03-26T12:00:00Z", &output_dir).expect("export");

        let after = fs::read_to_string(&packet_path).expect("after");
        assert_eq!(before, after);
    }

    fn write_sample_generation(
        dir: &Path,
        decision_packet_generated_at: &str,
        prod_fingerprint: &str,
        non_prod_fingerprint: &str,
    ) -> PathBuf {
        fs::create_dir_all(dir).expect("create generation dir");
        let packet_path = dir.join("packet.json");
        fs::write(
            &packet_path,
            serde_json::to_string_pretty(&sample_packet_json(
                decision_packet_generated_at,
                prod_fingerprint,
                non_prod_fingerprint,
            ))
            .expect("serialize packet"),
        )
        .expect("write packet");
        fs::write(
            dir.join("runbook.json"),
            serde_json::to_string_pretty(&sample_runbook_json(
                "2026-03-26T12:05:00Z",
                decision_packet_generated_at,
                prod_fingerprint,
                non_prod_fingerprint,
            ))
            .expect("serialize runbook"),
        )
        .expect("write runbook");
        fs::write(
            dir.join("runbook.md"),
            "# Tiny-Live Activation Runbook\n\nsample",
        )
        .expect("write markdown");
        packet_path
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
