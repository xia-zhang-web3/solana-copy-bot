#![recursion_limit = "256"]

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

#[allow(dead_code)]
#[path = "copybot_activation_artifact_archive.rs"]
mod activation_artifact_archive;

const USAGE: &str = "usage: copybot_activation_artifact_manifest --archive-dir <path> [--json] (--generate-manifest --output <path> | --verify-manifest <path>)";
const MANIFEST_VERSION: &str = "1";
const HASH_ALGORITHM: &str = "sha256";

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
    archive_dir: PathBuf,
    json: bool,
    mode: Mode,
}

#[derive(Debug, Clone)]
enum Mode {
    Generate { output_path: PathBuf },
    Verify { manifest_path: PathBuf },
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ArtifactManifestVerdict {
    ArtifactManifestGenerated,
    ArtifactManifestVerified,
    ArtifactManifestDriftDetected,
    ArtifactManifestInvalid,
    ArtifactManifestMissingFiles,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
enum ArtifactKind {
    DecisionPacketJson,
    RunbookJson,
    RunbookMarkdown,
    OrphanRunbookMarkdown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct ManifestGenerationIdentity {
    decision_packet_generated_at: DateTime<Utc>,
    prod_config_fingerprint_sha256: String,
    non_prod_config_fingerprint_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ManifestGenerationEntry {
    identity: ManifestGenerationIdentity,
    decision_packet_paths: Vec<String>,
    runbook_json_paths: Vec<String>,
    runbook_markdown_paths: Vec<String>,
    latest_packet_verdict: Option<String>,
    latest_runbook_verdict: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ManifestFileEntry {
    relative_path: String,
    artifact_kind: ArtifactKind,
    sha256: String,
    generation_identity: Option<ManifestGenerationIdentity>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActivationArtifactManifest {
    generated_at: DateTime<Utc>,
    manifest_version: String,
    build_version: String,
    git_commit: Option<String>,
    archive_dir: String,
    hash_algorithm: String,
    generation_count: usize,
    file_count: usize,
    orphan_markdown_paths: Vec<String>,
    generations: Vec<ManifestGenerationEntry>,
    files: Vec<ManifestFileEntry>,
}

#[derive(Debug, Clone)]
struct ArchiveSnapshot {
    generations: Vec<ManifestGenerationEntry>,
    files: Vec<ManifestFileEntry>,
    orphan_markdown_paths: Vec<String>,
    invalid_artifacts: Vec<activation_artifact_archive::InvalidArtifact>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GenerateReport {
    pub(crate) mode: String,
    pub(crate) verdict: ArtifactManifestVerdict,
    pub(crate) reason: String,
    pub(crate) archive_dir: String,
    pub(crate) manifest_path: String,
    pub(crate) generation_count: usize,
    pub(crate) file_count: usize,
    pub(crate) hash_coverage_count: usize,
    pub(crate) invalid_artifact_count: usize,
    pub(crate) invalid_artifacts: Vec<activation_artifact_archive::InvalidArtifact>,
    pub(crate) read_only_archive_artifacts: bool,
    pub(crate) execution_untouched: bool,
    pub(crate) activation_authorized: bool,
    pub(crate) not_authorized_summary: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct VerifyReport {
    pub(crate) mode: String,
    pub(crate) verdict: ArtifactManifestVerdict,
    pub(crate) reason: String,
    pub(crate) archive_dir: String,
    pub(crate) manifest_path: String,
    pub(crate) manifest_generation_count: usize,
    pub(crate) current_generation_count: usize,
    pub(crate) manifest_file_count: usize,
    pub(crate) current_file_count: usize,
    pub(crate) missing_files: Vec<String>,
    pub(crate) changed_files: Vec<String>,
    pub(crate) unexpected_files: Vec<String>,
    pub(crate) generation_drift: Vec<String>,
    pub(crate) invalid_artifact_count: usize,
    pub(crate) invalid_artifacts: Vec<activation_artifact_archive::InvalidArtifact>,
    pub(crate) read_only_archive_analysis: bool,
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
    let mut args = args.into_iter();
    let mut archive_dir: Option<PathBuf> = None;
    let mut json = false;
    let mut generate_manifest = false;
    let mut output_path: Option<PathBuf> = None;
    let mut verify_manifest_path: Option<PathBuf> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--archive-dir" => {
                archive_dir = Some(PathBuf::from(parse_string_arg(
                    "--archive-dir",
                    args.next(),
                )?))
            }
            "--json" => json = true,
            "--generate-manifest" => generate_manifest = true,
            "--output" => {
                output_path = Some(PathBuf::from(parse_string_arg("--output", args.next())?))
            }
            "--verify-manifest" => {
                verify_manifest_path = Some(PathBuf::from(parse_string_arg(
                    "--verify-manifest",
                    args.next(),
                )?))
            }
            "--help" | "-h" => return Ok(None),
            other => bail!("unrecognized arg `{other}`"),
        }
    }

    let Some(archive_dir) = archive_dir else {
        bail!("--archive-dir is required");
    };

    let mode = match (generate_manifest, output_path, verify_manifest_path) {
        (true, Some(output_path), None) => Mode::Generate { output_path },
        (false, None, Some(manifest_path)) => Mode::Verify { manifest_path },
        (true, None, None) => bail!("--generate-manifest requires --output <path>"),
        (false, Some(_), None) => bail!("--output requires --generate-manifest"),
        (true, _, Some(_)) => bail!("--generate-manifest and --verify-manifest cannot be combined"),
        (false, Some(_), Some(_)) => bail!("--output cannot be combined with --verify-manifest"),
        (false, None, None) => {
            bail!("either --generate-manifest or --verify-manifest <path> is required")
        }
    };

    Ok(Some(Config {
        archive_dir,
        json,
        mode,
    }))
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    value.ok_or_else(|| anyhow!("{flag} requires a value"))
}

fn run(config: Config) -> Result<String> {
    match &config.mode {
        Mode::Generate { output_path } => {
            let report = generate_manifest(&config.archive_dir, output_path)?;
            if config.json {
                Ok(serde_json::to_string_pretty(&report)?)
            } else {
                Ok(render_generate_human(&report))
            }
        }
        Mode::Verify { manifest_path } => {
            let report = verify_manifest(&config.archive_dir, manifest_path)?;
            if config.json {
                Ok(serde_json::to_string_pretty(&report)?)
            } else {
                Ok(render_verify_human(&report))
            }
        }
    }
}

pub(crate) fn generate_manifest(archive_dir: &Path, output_path: &Path) -> Result<GenerateReport> {
    let snapshot = build_archive_snapshot(archive_dir)?;
    if !snapshot.invalid_artifacts.is_empty() {
        return Ok(GenerateReport {
            mode: "generate_manifest".to_string(),
            verdict: ArtifactManifestVerdict::ArtifactManifestInvalid,
            reason: format!(
                "manifest generation is blocked because {} invalid artifact(s) are present in the archive",
                snapshot.invalid_artifacts.len()
            ),
            archive_dir: archive_dir.display().to_string(),
            manifest_path: output_path.display().to_string(),
            generation_count: snapshot.generations.len(),
            file_count: snapshot.files.len(),
            hash_coverage_count: snapshot.files.len(),
            invalid_artifact_count: snapshot.invalid_artifacts.len(),
            invalid_artifacts: snapshot.invalid_artifacts,
            read_only_archive_artifacts: true,
            execution_untouched: true,
            activation_authorized: false,
            not_authorized_summary:
                "Manifest generation only inspects exported activation artifacts. It does not authorize activation and does not override the Stage 3 production gate.".to_string(),
        });
    }

    let manifest = ActivationArtifactManifest {
        generated_at: Utc::now(),
        manifest_version: MANIFEST_VERSION.to_string(),
        build_version: env!("CARGO_PKG_VERSION").to_string(),
        git_commit: resolve_git_commit(),
        archive_dir: archive_dir.display().to_string(),
        hash_algorithm: HASH_ALGORITHM.to_string(),
        generation_count: snapshot.generations.len(),
        file_count: snapshot.files.len(),
        orphan_markdown_paths: snapshot.orphan_markdown_paths,
        generations: snapshot.generations,
        files: snapshot.files,
    };
    write_output(output_path, &serde_json::to_string_pretty(&manifest)?)?;

    Ok(GenerateReport {
        mode: "generate_manifest".to_string(),
        verdict: ArtifactManifestVerdict::ArtifactManifestGenerated,
        reason: format!(
            "manifest generated for {} archive generation(s) across {} artifact file(s)",
            manifest.generation_count, manifest.file_count
        ),
        archive_dir: archive_dir.display().to_string(),
        manifest_path: output_path.display().to_string(),
        generation_count: manifest.generation_count,
        file_count: manifest.file_count,
        hash_coverage_count: manifest.files.len(),
        invalid_artifact_count: 0,
        invalid_artifacts: Vec::new(),
        read_only_archive_artifacts: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary:
            "Manifest generation only snapshots archive integrity for later verification. It does not authorize activation and does not override the Stage 3 production gate.".to_string(),
    })
}

pub(crate) fn verify_manifest(archive_dir: &Path, manifest_path: &Path) -> Result<VerifyReport> {
    let raw_manifest = match fs::read_to_string(manifest_path) {
        Ok(raw) => raw,
        Err(error) => {
            return Ok(invalid_verify_report(
                archive_dir,
                manifest_path,
                format!("failed reading manifest: {error}"),
                Vec::new(),
                0,
                0,
                0,
                0,
            ))
        }
    };
    let manifest: ActivationArtifactManifest = match serde_json::from_str(&raw_manifest) {
        Ok(manifest) => manifest,
        Err(error) => {
            return Ok(invalid_verify_report(
                archive_dir,
                manifest_path,
                format!("failed parsing manifest json: {error}"),
                Vec::new(),
                0,
                0,
                0,
                0,
            ))
        }
    };
    if manifest.manifest_version.trim().is_empty() {
        return Ok(invalid_verify_report(
            archive_dir,
            manifest_path,
            "manifest has empty manifest_version".to_string(),
            Vec::new(),
            manifest.generation_count,
            0,
            manifest.file_count,
            0,
        ));
    }
    if manifest.hash_algorithm != HASH_ALGORITHM {
        return Ok(invalid_verify_report(
            archive_dir,
            manifest_path,
            format!(
                "manifest hash_algorithm `{}` is unsupported; expected `{HASH_ALGORITHM}`",
                manifest.hash_algorithm
            ),
            Vec::new(),
            manifest.generation_count,
            0,
            manifest.file_count,
            0,
        ));
    }

    let snapshot = build_archive_snapshot(archive_dir)?;
    let current_generation_count = snapshot.generations.len();
    let current_file_count = snapshot.files.len();
    if !snapshot.invalid_artifacts.is_empty() {
        return Ok(invalid_verify_report(
            archive_dir,
            manifest_path,
            format!(
                "current archive contains {} invalid artifact(s); verify archive health before trusting manifest verification",
                snapshot.invalid_artifacts.len()
            ),
            snapshot.invalid_artifacts,
            manifest.generation_count,
            current_generation_count,
            manifest.file_count,
            current_file_count,
        ));
    }

    let manifest_files = manifest
        .files
        .iter()
        .map(|entry| (entry.relative_path.clone(), entry))
        .collect::<BTreeMap<_, _>>();
    let current_files = snapshot
        .files
        .iter()
        .map(|entry| (entry.relative_path.clone(), entry))
        .collect::<BTreeMap<_, _>>();

    let missing_files = manifest_files
        .keys()
        .filter(|path| !current_files.contains_key(*path))
        .cloned()
        .collect::<Vec<_>>();
    let unexpected_files = current_files
        .keys()
        .filter(|path| !manifest_files.contains_key(*path))
        .cloned()
        .collect::<Vec<_>>();
    let changed_files = manifest_files
        .iter()
        .filter_map(|(path, manifest_entry)| {
            let current_entry = current_files.get(path)?;
            if current_entry.sha256 != manifest_entry.sha256
                || current_entry.artifact_kind != manifest_entry.artifact_kind
                || current_entry.generation_identity != manifest_entry.generation_identity
            {
                Some(path.clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    let generation_drift = diff_generations(&manifest.generations, &snapshot.generations);

    let verdict = if !missing_files.is_empty() {
        ArtifactManifestVerdict::ArtifactManifestMissingFiles
    } else if !changed_files.is_empty()
        || !unexpected_files.is_empty()
        || !generation_drift.is_empty()
    {
        ArtifactManifestVerdict::ArtifactManifestDriftDetected
    } else {
        ArtifactManifestVerdict::ArtifactManifestVerified
    };

    let reason = match verdict {
        ArtifactManifestVerdict::ArtifactManifestVerified => format!(
            "manifest verification passed for {} generation(s) and {} artifact file(s)",
            current_generation_count, current_file_count
        ),
        ArtifactManifestVerdict::ArtifactManifestMissingFiles => format!(
            "manifest verification found {} missing file(s)",
            missing_files.len()
        ),
        ArtifactManifestVerdict::ArtifactManifestDriftDetected => format!(
            "manifest verification detected drift: {} changed file(s), {} unexpected file(s), {} generation drift record(s)",
            changed_files.len(),
            unexpected_files.len(),
            generation_drift.len()
        ),
        ArtifactManifestVerdict::ArtifactManifestGenerated
        | ArtifactManifestVerdict::ArtifactManifestInvalid => unreachable!(),
    };

    Ok(VerifyReport {
        mode: "verify_manifest".to_string(),
        verdict,
        reason,
        archive_dir: archive_dir.display().to_string(),
        manifest_path: manifest_path.display().to_string(),
        manifest_generation_count: manifest.generation_count,
        current_generation_count,
        manifest_file_count: manifest.file_count,
        current_file_count,
        missing_files,
        changed_files,
        unexpected_files,
        generation_drift,
        invalid_artifact_count: 0,
        invalid_artifacts: Vec::new(),
        read_only_archive_analysis: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary:
            "Manifest verification only checks archive integrity. It does not authorize activation and does not override the Stage 3 production gate.".to_string(),
    })
}

fn invalid_verify_report(
    archive_dir: &Path,
    manifest_path: &Path,
    reason: String,
    invalid_artifacts: Vec<activation_artifact_archive::InvalidArtifact>,
    manifest_generation_count: usize,
    current_generation_count: usize,
    manifest_file_count: usize,
    current_file_count: usize,
) -> VerifyReport {
    VerifyReport {
        mode: "verify_manifest".to_string(),
        verdict: ArtifactManifestVerdict::ArtifactManifestInvalid,
        reason,
        archive_dir: archive_dir.display().to_string(),
        manifest_path: manifest_path.display().to_string(),
        manifest_generation_count,
        current_generation_count,
        manifest_file_count,
        current_file_count,
        missing_files: Vec::new(),
        changed_files: Vec::new(),
        unexpected_files: Vec::new(),
        generation_drift: Vec::new(),
        invalid_artifact_count: invalid_artifacts.len(),
        invalid_artifacts,
        read_only_archive_analysis: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary:
            "Manifest verification only checks archive integrity. It does not authorize activation and does not override the Stage 3 production gate.".to_string(),
    }
}

fn build_archive_snapshot(archive_dir: &Path) -> Result<ArchiveSnapshot> {
    let inventory = activation_artifact_archive::archive_inventory(archive_dir)?;
    let generations = inventory
        .generation_summaries
        .iter()
        .map(|summary| -> Result<ManifestGenerationEntry> {
            Ok(ManifestGenerationEntry {
                identity: ManifestGenerationIdentity {
                    decision_packet_generated_at: summary.decision_packet_generated_at,
                    prod_config_fingerprint_sha256: summary.prod_config_fingerprint_sha256.clone(),
                    non_prod_config_fingerprint_sha256: summary
                        .non_prod_config_fingerprint_sha256
                        .clone(),
                },
                decision_packet_paths: make_relative_paths(
                    archive_dir,
                    &summary.decision_packet_paths,
                )?,
                runbook_json_paths: make_relative_paths(archive_dir, &summary.runbook_json_paths)?,
                runbook_markdown_paths: make_relative_paths(
                    archive_dir,
                    &summary.runbook_markdown_paths,
                )?,
                latest_packet_verdict: summary.latest_packet_verdict.clone(),
                latest_runbook_verdict: summary.latest_runbook_verdict.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let orphan_markdown_paths = make_relative_paths(archive_dir, &inventory.orphan_markdown_paths)?;
    let mut files = Vec::new();
    for generation in &generations {
        for path in &generation.decision_packet_paths {
            files.push(ManifestFileEntry {
                relative_path: path.clone(),
                artifact_kind: ArtifactKind::DecisionPacketJson,
                sha256: hash_file(&archive_dir.join(path))?,
                generation_identity: Some(generation.identity.clone()),
            });
        }
        for path in &generation.runbook_json_paths {
            files.push(ManifestFileEntry {
                relative_path: path.clone(),
                artifact_kind: ArtifactKind::RunbookJson,
                sha256: hash_file(&archive_dir.join(path))?,
                generation_identity: Some(generation.identity.clone()),
            });
        }
        for path in &generation.runbook_markdown_paths {
            files.push(ManifestFileEntry {
                relative_path: path.clone(),
                artifact_kind: ArtifactKind::RunbookMarkdown,
                sha256: hash_file(&archive_dir.join(path))?,
                generation_identity: Some(generation.identity.clone()),
            });
        }
    }
    for path in &orphan_markdown_paths {
        if files.iter().any(|entry| entry.relative_path == *path) {
            continue;
        }
        files.push(ManifestFileEntry {
            relative_path: path.clone(),
            artifact_kind: ArtifactKind::OrphanRunbookMarkdown,
            sha256: hash_file(&archive_dir.join(path))?,
            generation_identity: None,
        });
    }
    files.sort_by(|left, right| {
        left.relative_path
            .cmp(&right.relative_path)
            .then_with(|| left.artifact_kind.cmp(&right.artifact_kind))
    });

    Ok(ArchiveSnapshot {
        generations,
        files,
        orphan_markdown_paths,
        invalid_artifacts: inventory.invalid_artifacts,
    })
}

fn make_relative_paths(archive_dir: &Path, paths: &[String]) -> Result<Vec<String>> {
    let mut values = paths
        .iter()
        .map(|path| relative_display_path(archive_dir, Path::new(path)))
        .collect::<Result<Vec<_>>>()?;
    values.sort();
    Ok(values)
}

fn relative_display_path(archive_dir: &Path, path: &Path) -> Result<String> {
    let relative = path.strip_prefix(archive_dir).with_context(|| {
        format!(
            "artifact path {} is outside archive dir {}",
            path.display(),
            archive_dir.display()
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

fn diff_generations(
    manifest_generations: &[ManifestGenerationEntry],
    current_generations: &[ManifestGenerationEntry],
) -> Vec<String> {
    let manifest_map = manifest_generations
        .iter()
        .map(|generation| {
            (
                generation_key_label(&generation.identity),
                generation_membership_set(generation),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let current_map = current_generations
        .iter()
        .map(|generation| {
            (
                generation_key_label(&generation.identity),
                generation_membership_set(generation),
            )
        })
        .collect::<BTreeMap<_, _>>();

    let mut drift = Vec::new();
    for key in manifest_map
        .keys()
        .chain(current_map.keys())
        .collect::<BTreeSet<_>>()
    {
        match (manifest_map.get(key), current_map.get(key)) {
            (Some(manifest), Some(current)) if manifest != current => {
                drift.push(format!("generation {key} membership changed"))
            }
            (Some(_), None) => drift.push(format!("generation {key} missing from current archive")),
            (None, Some(_)) => drift.push(format!(
                "generation {key} unexpectedly present in current archive"
            )),
            (Some(_), Some(_)) | (None, None) => {}
        }
    }
    drift
}

fn generation_membership_set(generation: &ManifestGenerationEntry) -> BTreeSet<String> {
    generation
        .decision_packet_paths
        .iter()
        .map(|path| format!("decision_packet:{path}"))
        .chain(
            generation
                .runbook_json_paths
                .iter()
                .map(|path| format!("runbook_json:{path}")),
        )
        .chain(
            generation
                .runbook_markdown_paths
                .iter()
                .map(|path| format!("runbook_markdown:{path}")),
        )
        .collect()
}

fn generation_key_label(identity: &ManifestGenerationIdentity) -> String {
    format!(
        "{}:{}:{}",
        identity.decision_packet_generated_at.to_rfc3339(),
        identity.prod_config_fingerprint_sha256,
        identity.non_prod_config_fingerprint_sha256
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

fn write_output(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed creating output dir {}", parent.display()))?;
        }
    }
    fs::write(path, contents).with_context(|| format!("failed writing {}", path.display()))
}

fn render_generate_human(report: &GenerateReport) -> String {
    [
        "event=copybot_activation_artifact_manifest".to_string(),
        "mode=generate_manifest".to_string(),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("archive_dir={}", report.archive_dir),
        format!("manifest_path={}", report.manifest_path),
        format!("generation_count={}", report.generation_count),
        format!("file_count={}", report.file_count),
        format!("hash_coverage_count={}", report.hash_coverage_count),
        format!("invalid_artifact_count={}", report.invalid_artifact_count),
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
        "event=copybot_activation_artifact_manifest".to_string(),
        "mode=verify_manifest".to_string(),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("archive_dir={}", report.archive_dir),
        format!("manifest_path={}", report.manifest_path),
        format!(
            "manifest_generation_count={}",
            report.manifest_generation_count
        ),
        format!(
            "current_generation_count={}",
            report.current_generation_count
        ),
        format!("manifest_file_count={}", report.manifest_file_count),
        format!("current_file_count={}", report.current_file_count),
        format!("missing_files={}", report.missing_files.join(" | ")),
        format!("changed_files={}", report.changed_files.join(" | ")),
        format!("unexpected_files={}", report.unexpected_files.join(" | ")),
        format!("generation_drift={}", report.generation_drift.join(" | ")),
        format!("invalid_artifact_count={}", report.invalid_artifact_count),
        format!(
            "read_only_archive_analysis={}",
            report.read_only_archive_analysis
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
    fn manifest_generation_succeeds_for_valid_archive() {
        let archive_dir = temp_dir("activation_manifest_generate");
        write_sample_generation(
            &archive_dir,
            "2026-03-26T12:00:00Z",
            "prod_fp_a",
            "non_prod_fp_a",
            true,
        );
        let manifest_path = archive_dir.join("../manifest.json");

        let report = generate_manifest(&archive_dir, &manifest_path).expect("generate");

        assert_eq!(
            report.verdict,
            ArtifactManifestVerdict::ArtifactManifestGenerated
        );
        assert!(manifest_path.exists());
        let manifest: ActivationArtifactManifest =
            serde_json::from_str(&fs::read_to_string(&manifest_path).expect("manifest"))
                .expect("parse manifest");
        assert_eq!(manifest.generation_count, 1);
        assert_eq!(manifest.file_count, 3);
    }

    #[test]
    fn manifest_verification_passes_on_unchanged_archive() {
        let archive_dir = temp_dir("activation_manifest_verify_ok");
        write_sample_generation(
            &archive_dir,
            "2026-03-26T12:00:00Z",
            "prod_fp_a",
            "non_prod_fp_a",
            true,
        );
        let manifest_path = temp_dir("activation_manifest_verify_ok_out").join("manifest.json");
        generate_manifest(&archive_dir, &manifest_path).expect("generate");

        let report = verify_manifest(&archive_dir, &manifest_path).expect("verify");

        assert_eq!(
            report.verdict,
            ArtifactManifestVerdict::ArtifactManifestVerified
        );
        assert!(report.missing_files.is_empty());
        assert!(report.changed_files.is_empty());
    }

    #[test]
    fn deleted_file_is_detected() {
        let archive_dir = temp_dir("activation_manifest_missing");
        let paths = write_sample_generation(
            &archive_dir,
            "2026-03-26T12:00:00Z",
            "prod_fp_a",
            "non_prod_fp_a",
            true,
        );
        let manifest_path = temp_dir("activation_manifest_missing_out").join("manifest.json");
        generate_manifest(&archive_dir, &manifest_path).expect("generate");
        fs::remove_file(paths.runbook_json_path).expect("remove runbook");

        let report = verify_manifest(&archive_dir, &manifest_path).expect("verify");

        assert_eq!(
            report.verdict,
            ArtifactManifestVerdict::ArtifactManifestMissingFiles
        );
        assert_eq!(report.missing_files.len(), 1);
    }

    #[test]
    fn modified_file_hash_is_detected() {
        let archive_dir = temp_dir("activation_manifest_changed");
        let paths = write_sample_generation(
            &archive_dir,
            "2026-03-26T12:00:00Z",
            "prod_fp_a",
            "non_prod_fp_a",
            true,
        );
        let manifest_path = temp_dir("activation_manifest_changed_out").join("manifest.json");
        generate_manifest(&archive_dir, &manifest_path).expect("generate");
        fs::write(
            paths.runbook_markdown_path,
            "# Tiny-Live Activation Runbook\n\nchanged",
        )
        .expect("write changed markdown");

        let report = verify_manifest(&archive_dir, &manifest_path).expect("verify");

        assert_eq!(
            report.verdict,
            ArtifactManifestVerdict::ArtifactManifestDriftDetected
        );
        assert_eq!(report.changed_files.len(), 1);
    }

    #[test]
    fn malformed_manifest_is_rejected() {
        let archive_dir = temp_dir("activation_manifest_invalid");
        write_sample_generation(
            &archive_dir,
            "2026-03-26T12:00:00Z",
            "prod_fp_a",
            "non_prod_fp_a",
            true,
        );
        let manifest_path = temp_dir("activation_manifest_invalid_out").join("manifest.json");
        fs::write(&manifest_path, "{broken").expect("write broken manifest");

        let report = verify_manifest(&archive_dir, &manifest_path).expect("verify");

        assert_eq!(
            report.verdict,
            ArtifactManifestVerdict::ArtifactManifestInvalid
        );
    }

    #[test]
    fn archive_files_remain_unchanged_by_manifest_flows() {
        let archive_dir = temp_dir("activation_manifest_no_mutation");
        let paths = write_sample_generation(
            &archive_dir,
            "2026-03-26T12:00:00Z",
            "prod_fp_a",
            "non_prod_fp_a",
            true,
        );
        let before = fs::read_to_string(&paths.packet_path).expect("before");
        let manifest_path = temp_dir("activation_manifest_no_mutation_out").join("manifest.json");

        generate_manifest(&archive_dir, &manifest_path).expect("generate");
        let _ = verify_manifest(&archive_dir, &manifest_path).expect("verify");

        let after = fs::read_to_string(&paths.packet_path).expect("after");
        assert_eq!(before, after);
    }

    struct SampleGenerationPaths {
        packet_path: PathBuf,
        runbook_json_path: PathBuf,
        runbook_markdown_path: PathBuf,
    }

    fn write_sample_generation(
        archive_dir: &Path,
        decision_packet_generated_at: &str,
        prod_fingerprint: &str,
        non_prod_fingerprint: &str,
        discussion_ready: bool,
    ) -> SampleGenerationPaths {
        let packet_path = archive_dir.join("packet.json");
        let runbook_json_path = archive_dir.join("runbook.json");
        let runbook_markdown_path = archive_dir.join("runbook.md");
        fs::create_dir_all(archive_dir).expect("create archive dir");
        fs::write(
            &packet_path,
            serde_json::to_string_pretty(&sample_packet_json(
                decision_packet_generated_at,
                prod_fingerprint,
                non_prod_fingerprint,
                discussion_ready,
            ))
            .expect("serialize packet"),
        )
        .expect("write packet");
        fs::write(
            &runbook_json_path,
            serde_json::to_string_pretty(&sample_runbook_json(
                "2026-03-26T12:05:00Z",
                decision_packet_generated_at,
                prod_fingerprint,
                non_prod_fingerprint,
                discussion_ready,
            ))
            .expect("serialize runbook"),
        )
        .expect("write runbook");
        fs::write(
            &runbook_markdown_path,
            "# Tiny-Live Activation Runbook\n\nsample",
        )
        .expect("write markdown");

        SampleGenerationPaths {
            packet_path,
            runbook_json_path,
            runbook_markdown_path,
        }
    }

    fn sample_packet_json(
        generated_at: &str,
        prod_fingerprint: &str,
        non_prod_fingerprint: &str,
        discussion_ready: bool,
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
            "discussion_ready_only": discussion_ready,
            "prod_stage3_remains_hard_gate": true,
            "non_prod_evidence_is_secondary": true,
            "verdict": if discussion_ready { "decision_packet_discussion_ready_but_not_authorized" } else { "decision_packet_blocked" },
            "reason": "sample packet",
            "blockers": if discussion_ready { json!([]) } else { json!(["stage3 blocked"]) },
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
        discussion_ready: bool,
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
            "discussion_ready_only": discussion_ready,
            "prod_stage3_remains_hard_gate": true,
            "non_prod_evidence_is_secondary": true,
            "verdict": if discussion_ready { "runbook_discussion_ready_but_not_authorized" } else { "runbook_blocked" },
            "reason": "runbook sample",
            "blockers": [],
            "warnings": ["planning-only"],
            "not_authorized_disclaimer": "planning-only",
            "decision_packet_version": "1",
            "decision_packet_generated_at": decision_packet_generated_at,
            "decision_packet_verdict": if discussion_ready { "decision_packet_discussion_ready_but_not_authorized" } else { "decision_packet_blocked" },
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
