#![recursion_limit = "256"]

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[allow(dead_code)]
#[path = "copybot_activation_artifact_archive.rs"]
mod activation_artifact_archive;

const USAGE: &str = "usage: copybot_activation_artifact_provenance_report --archive-dir <path> --manifest-dir <path> --bundle-dir <path> [--json]";
const BUNDLE_MANIFEST_FILENAME: &str = "bundle_manifest.json";
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
    manifest_dir: PathBuf,
    bundle_dir: PathBuf,
    json: bool,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ArtifactProvenanceVerdict {
    ArtifactProvenanceComplete,
    ArtifactProvenanceIncomplete,
    ArtifactProvenanceInvalidArtifactsPresent,
    ArtifactProvenanceInconsistentLineage,
}

#[derive(Debug, Clone, Serialize)]
struct LineageArtifactIssue {
    surface: String,
    path: String,
    error: String,
}

#[derive(Debug, Clone, Serialize)]
struct GenerationCoverageSummary {
    generation_id: String,
    decision_packet_generated_at: DateTime<Utc>,
    prod_config_fingerprint_sha256: String,
    non_prod_config_fingerprint_sha256: String,
    decision_packet_count: usize,
    runbook_json_count: usize,
    runbook_markdown_count: usize,
    manifest_coverage_count: usize,
    bundle_coverage_count: usize,
    manifest_paths: Vec<String>,
    bundle_manifest_paths: Vec<String>,
    missing_manifest_coverage: bool,
    missing_bundle_coverage: bool,
    lineage_complete: bool,
}

#[derive(Debug, Clone, Serialize)]
struct ReferencedGenerationSummary {
    generation_id: String,
    reference_paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ProvenanceReport {
    mode: String,
    verdict: ArtifactProvenanceVerdict,
    reason: String,
    archive_dir: String,
    manifest_dir: String,
    bundle_dir: String,
    archive_generation_count: usize,
    manifest_file_count: usize,
    bundle_manifest_count: usize,
    manifest_generation_coverage_count: usize,
    bundle_generation_coverage_count: usize,
    complete_generation_count: usize,
    incomplete_generation_count: usize,
    generations: Vec<GenerationCoverageSummary>,
    manifest_generations_missing_from_archive: Vec<ReferencedGenerationSummary>,
    bundle_generations_missing_from_archive: Vec<ReferencedGenerationSummary>,
    invalid_artifact_count: usize,
    invalid_artifacts: Vec<LineageArtifactIssue>,
    read_only_lineage_analysis: bool,
    execution_untouched: bool,
    activation_authorized: bool,
    not_authorized_summary: String,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct ArtifactProvenanceSummary {
    pub(crate) verdict: String,
    pub(crate) reason: String,
    pub(crate) archive_generation_count: usize,
    pub(crate) manifest_file_count: usize,
    pub(crate) bundle_manifest_count: usize,
    pub(crate) complete_generation_count: usize,
    pub(crate) incomplete_generation_count: usize,
    pub(crate) invalid_artifact_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct GenerationIdentity {
    decision_packet_generated_at: DateTime<Utc>,
    prod_config_fingerprint_sha256: String,
    non_prod_config_fingerprint_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    artifact_kind: String,
    sha256: String,
    generation_identity: Option<ManifestGenerationIdentity>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActivationArtifactManifestDocument {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BundleGenerationIdentity {
    decision_packet_generated_at: DateTime<Utc>,
    prod_config_fingerprint_sha256: String,
    non_prod_config_fingerprint_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BundleFileEntry {
    artifact_kind: String,
    source_relative_path: String,
    bundle_relative_path: String,
    sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActivationArtifactBundleManifestDocument {
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

#[derive(Debug, Clone)]
struct LoadedManifest {
    path: String,
    document: ActivationArtifactManifestDocument,
}

#[derive(Debug, Clone)]
struct LoadedBundleManifest {
    path: String,
    document: ActivationArtifactBundleManifestDocument,
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
    let mut manifest_dir: Option<PathBuf> = None;
    let mut bundle_dir: Option<PathBuf> = None;
    let mut json = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--archive-dir" => {
                archive_dir = Some(PathBuf::from(parse_string_arg(
                    "--archive-dir",
                    args.next(),
                )?))
            }
            "--manifest-dir" => {
                manifest_dir = Some(PathBuf::from(parse_string_arg(
                    "--manifest-dir",
                    args.next(),
                )?))
            }
            "--bundle-dir" => {
                bundle_dir = Some(PathBuf::from(parse_string_arg(
                    "--bundle-dir",
                    args.next(),
                )?))
            }
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unrecognized arg `{other}`"),
        }
    }

    Ok(Some(Config {
        archive_dir: archive_dir.ok_or_else(|| anyhow!("--archive-dir is required"))?,
        manifest_dir: manifest_dir.ok_or_else(|| anyhow!("--manifest-dir is required"))?,
        bundle_dir: bundle_dir.ok_or_else(|| anyhow!("--bundle-dir is required"))?,
        json,
    }))
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    value.ok_or_else(|| anyhow!("{flag} requires a value"))
}

fn run(config: Config) -> Result<String> {
    let report = build_report(&config)?;
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_human(&report))
    }
}

#[allow(dead_code)]
pub(crate) fn inspect_provenance_summary(
    archive_dir: &Path,
    manifest_dir: &Path,
    bundle_dir: &Path,
) -> Result<ArtifactProvenanceSummary> {
    let report = build_report(&Config {
        archive_dir: archive_dir.to_path_buf(),
        manifest_dir: manifest_dir.to_path_buf(),
        bundle_dir: bundle_dir.to_path_buf(),
        json: false,
    })?;
    Ok(ArtifactProvenanceSummary {
        verdict: serialize_enum(&report.verdict),
        reason: report.reason,
        archive_generation_count: report.archive_generation_count,
        manifest_file_count: report.manifest_file_count,
        bundle_manifest_count: report.bundle_manifest_count,
        complete_generation_count: report.complete_generation_count,
        incomplete_generation_count: report.incomplete_generation_count,
        invalid_artifact_count: report.invalid_artifact_count,
    })
}

fn build_report(config: &Config) -> Result<ProvenanceReport> {
    let archive_inventory = activation_artifact_archive::archive_inventory(&config.archive_dir)?;
    let mut invalid_artifacts = archive_inventory
        .invalid_artifacts
        .into_iter()
        .map(|artifact| LineageArtifactIssue {
            surface: "archive".to_string(),
            path: artifact.path,
            error: artifact.error,
        })
        .collect::<Vec<_>>();

    let (loaded_manifests, manifest_invalid) = load_manifests(&config.manifest_dir)?;
    invalid_artifacts.extend(manifest_invalid);
    let (loaded_bundles, bundle_invalid) = load_bundle_manifests(&config.bundle_dir)?;
    invalid_artifacts.extend(bundle_invalid);

    let archive_generations = archive_inventory
        .generation_summaries
        .into_iter()
        .filter(|summary| !summary.decision_packet_paths.is_empty())
        .collect::<Vec<_>>();

    let archive_map = archive_generations
        .iter()
        .map(|summary| (generation_id_from_archive_summary(summary), summary))
        .collect::<BTreeMap<_, _>>();

    let mut manifest_coverage = BTreeMap::<String, Vec<String>>::new();
    for manifest in &loaded_manifests {
        for generation in &manifest.document.generations {
            manifest_coverage
                .entry(generation_id_from_manifest_identity(&generation.identity))
                .or_default()
                .push(manifest.path.clone());
        }
    }
    let mut bundle_coverage = BTreeMap::<String, Vec<String>>::new();
    for bundle in &loaded_bundles {
        bundle_coverage
            .entry(bundle.document.generation_id.clone())
            .or_default()
            .push(bundle.path.clone());
    }

    let generations = archive_generations
        .iter()
        .map(|summary| {
            let generation_id = generation_id_from_archive_summary(summary);
            let manifest_paths = manifest_coverage
                .get(&generation_id)
                .cloned()
                .unwrap_or_default();
            let bundle_paths = bundle_coverage
                .get(&generation_id)
                .cloned()
                .unwrap_or_default();
            let missing_manifest_coverage = manifest_paths.is_empty();
            let missing_bundle_coverage = bundle_paths.is_empty();
            GenerationCoverageSummary {
                generation_id,
                decision_packet_generated_at: summary.decision_packet_generated_at,
                prod_config_fingerprint_sha256: summary.prod_config_fingerprint_sha256.clone(),
                non_prod_config_fingerprint_sha256: summary
                    .non_prod_config_fingerprint_sha256
                    .clone(),
                decision_packet_count: summary.decision_packet_paths.len(),
                runbook_json_count: summary.runbook_json_paths.len(),
                runbook_markdown_count: summary.runbook_markdown_paths.len(),
                manifest_coverage_count: manifest_paths.len(),
                bundle_coverage_count: bundle_paths.len(),
                manifest_paths,
                bundle_manifest_paths: bundle_paths,
                missing_manifest_coverage,
                missing_bundle_coverage,
                lineage_complete: !missing_manifest_coverage && !missing_bundle_coverage,
            }
        })
        .collect::<Vec<_>>();

    let manifest_generations_missing_from_archive =
        missing_reference_summaries(&manifest_coverage, &archive_map);
    let bundle_generations_missing_from_archive =
        missing_reference_summaries(&bundle_coverage, &archive_map);

    let complete_generation_count = generations
        .iter()
        .filter(|summary| summary.lineage_complete)
        .count();
    let incomplete_generation_count = generations.len().saturating_sub(complete_generation_count);
    let no_archive_generations = generations.is_empty();

    let verdict = if !invalid_artifacts.is_empty() {
        ArtifactProvenanceVerdict::ArtifactProvenanceInvalidArtifactsPresent
    } else if !manifest_generations_missing_from_archive.is_empty()
        || !bundle_generations_missing_from_archive.is_empty()
    {
        ArtifactProvenanceVerdict::ArtifactProvenanceInconsistentLineage
    } else if no_archive_generations {
        ArtifactProvenanceVerdict::ArtifactProvenanceIncomplete
    } else if incomplete_generation_count > 0 {
        ArtifactProvenanceVerdict::ArtifactProvenanceIncomplete
    } else {
        ArtifactProvenanceVerdict::ArtifactProvenanceComplete
    };

    let reason = match verdict {
        ArtifactProvenanceVerdict::ArtifactProvenanceComplete => format!(
            "provenance is complete across {} archive generation(s), {} manifest file(s), and {} bundle manifest(s)",
            generations.len(),
            loaded_manifests.len(),
            loaded_bundles.len()
        ),
        ArtifactProvenanceVerdict::ArtifactProvenanceIncomplete if no_archive_generations => {
            "provenance is incomplete: no packet-backed archive generations found".to_string()
        }
        ArtifactProvenanceVerdict::ArtifactProvenanceIncomplete => format!(
            "provenance is incomplete for {} archive generation(s): missing manifest or bundle coverage",
            incomplete_generation_count
        ),
        ArtifactProvenanceVerdict::ArtifactProvenanceInvalidArtifactsPresent => format!(
            "provenance is blocked by {} invalid archive/manifest/bundle artifact(s)",
            invalid_artifacts.len()
        ),
        ArtifactProvenanceVerdict::ArtifactProvenanceInconsistentLineage => format!(
            "provenance is inconsistent: {} manifest reference(s) and {} bundle reference(s) point to missing archive generations",
            manifest_generations_missing_from_archive.len(),
            bundle_generations_missing_from_archive.len()
        ),
    };

    Ok(ProvenanceReport {
        mode: "activation_artifact_provenance".to_string(),
        verdict,
        reason,
        archive_dir: config.archive_dir.display().to_string(),
        manifest_dir: config.manifest_dir.display().to_string(),
        bundle_dir: config.bundle_dir.display().to_string(),
        archive_generation_count: generations.len(),
        manifest_file_count: loaded_manifests.len(),
        bundle_manifest_count: loaded_bundles.len(),
        manifest_generation_coverage_count: manifest_coverage.len(),
        bundle_generation_coverage_count: bundle_coverage.len(),
        complete_generation_count,
        incomplete_generation_count,
        generations,
        manifest_generations_missing_from_archive,
        bundle_generations_missing_from_archive,
        invalid_artifact_count: invalid_artifacts.len(),
        invalid_artifacts,
        read_only_lineage_analysis: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary:
            "This provenance report only audits activation artifact lineage across archive, manifests, and bundles. It does not authorize activation and does not override the Stage 3 production gate.".to_string(),
    })
}

fn load_manifests(manifest_dir: &Path) -> Result<(Vec<LoadedManifest>, Vec<LineageArtifactIssue>)> {
    let mut loaded = Vec::new();
    let mut invalid = Vec::new();
    for path in collect_files(manifest_dir)?
        .into_iter()
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("json"))
    {
        let path_string = path.display().to_string();
        match parse_manifest_file(&path) {
            Ok(document) => loaded.push(LoadedManifest {
                path: path_string,
                document,
            }),
            Err(error) => invalid.push(LineageArtifactIssue {
                surface: "manifest".to_string(),
                path: path.display().to_string(),
                error: format!("{error:#}"),
            }),
        }
    }
    Ok((loaded, invalid))
}

fn load_bundle_manifests(
    bundle_dir: &Path,
) -> Result<(Vec<LoadedBundleManifest>, Vec<LineageArtifactIssue>)> {
    let mut loaded = Vec::new();
    let mut invalid = Vec::new();
    for path in collect_files(bundle_dir)?.into_iter().filter(|path| {
        path.file_name().and_then(|name| name.to_str()) == Some(BUNDLE_MANIFEST_FILENAME)
    }) {
        let path_string = path.display().to_string();
        match parse_bundle_manifest_file(&path) {
            Ok(document) => loaded.push(LoadedBundleManifest {
                path: path_string,
                document,
            }),
            Err(error) => invalid.push(LineageArtifactIssue {
                surface: "bundle".to_string(),
                path: path.display().to_string(),
                error: format!("{error:#}"),
            }),
        }
    }
    Ok((loaded, invalid))
}

fn parse_manifest_file(path: &Path) -> Result<ActivationArtifactManifestDocument> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed reading manifest {}", path.display()))?;
    let document: ActivationArtifactManifestDocument = serde_json::from_str(&raw)
        .with_context(|| format!("failed parsing manifest {}", path.display()))?;
    if document.manifest_version.trim().is_empty() {
        bail!("{} has empty manifest_version", path.display());
    }
    if document.hash_algorithm != HASH_ALGORITHM {
        bail!(
            "{} has unsupported hash_algorithm `{}`",
            path.display(),
            document.hash_algorithm
        );
    }
    if document.generation_count != document.generations.len() {
        bail!(
            "{} generation_count={} does not match actual generations={}",
            path.display(),
            document.generation_count,
            document.generations.len()
        );
    }
    if document.file_count != document.files.len() {
        bail!(
            "{} file_count={} does not match actual files={}",
            path.display(),
            document.file_count,
            document.files.len()
        );
    }
    Ok(document)
}

fn parse_bundle_manifest_file(path: &Path) -> Result<ActivationArtifactBundleManifestDocument> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed reading bundle manifest {}", path.display()))?;
    let document: ActivationArtifactBundleManifestDocument = serde_json::from_str(&raw)
        .with_context(|| format!("failed parsing bundle manifest {}", path.display()))?;
    if document.bundle_version.trim().is_empty() {
        bail!("{} has empty bundle_version", path.display());
    }
    if document.hash_algorithm != HASH_ALGORITHM {
        bail!(
            "{} has unsupported hash_algorithm `{}`",
            path.display(),
            document.hash_algorithm
        );
    }
    if document.file_count != document.files.len() {
        bail!(
            "{} file_count={} does not match actual files={}",
            path.display(),
            document.file_count,
            document.files.len()
        );
    }
    let expected_generation_id = generation_id_from_identity(&GenerationIdentity {
        decision_packet_generated_at: document.generation_identity.decision_packet_generated_at,
        prod_config_fingerprint_sha256: document
            .generation_identity
            .prod_config_fingerprint_sha256
            .clone(),
        non_prod_config_fingerprint_sha256: document
            .generation_identity
            .non_prod_config_fingerprint_sha256
            .clone(),
    });
    if document.generation_id != expected_generation_id {
        bail!(
            "{} generation_id {} does not match generation_identity {}",
            path.display(),
            document.generation_id,
            expected_generation_id
        );
    }
    Ok(document)
}

fn collect_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if !root.exists() {
        return Ok(files);
    }
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries =
            fs::read_dir(&dir).with_context(|| format!("failed reading dir {}", dir.display()))?;
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

fn missing_reference_summaries(
    coverage: &BTreeMap<String, Vec<String>>,
    archive_map: &BTreeMap<String, &activation_artifact_archive::ArchiveArtifactGenerationSummary>,
) -> Vec<ReferencedGenerationSummary> {
    coverage
        .iter()
        .filter(|(generation_id, _)| !archive_map.contains_key(*generation_id))
        .map(|(generation_id, paths)| ReferencedGenerationSummary {
            generation_id: generation_id.clone(),
            reference_paths: paths.clone(),
        })
        .collect()
}

fn generation_id_from_archive_summary(
    summary: &activation_artifact_archive::ArchiveArtifactGenerationSummary,
) -> String {
    generation_id_from_identity(&GenerationIdentity {
        decision_packet_generated_at: summary.decision_packet_generated_at,
        prod_config_fingerprint_sha256: summary.prod_config_fingerprint_sha256.clone(),
        non_prod_config_fingerprint_sha256: summary.non_prod_config_fingerprint_sha256.clone(),
    })
}

fn generation_id_from_manifest_identity(identity: &ManifestGenerationIdentity) -> String {
    generation_id_from_identity(&GenerationIdentity {
        decision_packet_generated_at: identity.decision_packet_generated_at,
        prod_config_fingerprint_sha256: identity.prod_config_fingerprint_sha256.clone(),
        non_prod_config_fingerprint_sha256: identity.non_prod_config_fingerprint_sha256.clone(),
    })
}

fn generation_id_from_identity(identity: &GenerationIdentity) -> String {
    format!(
        "{}|{}|{}",
        identity.decision_packet_generated_at.to_rfc3339(),
        identity.prod_config_fingerprint_sha256,
        identity.non_prod_config_fingerprint_sha256
    )
}

fn render_human(report: &ProvenanceReport) -> String {
    [
        "event=copybot_activation_artifact_provenance_report".to_string(),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("archive_dir={}", report.archive_dir),
        format!("manifest_dir={}", report.manifest_dir),
        format!("bundle_dir={}", report.bundle_dir),
        format!(
            "archive_generation_count={}",
            report.archive_generation_count
        ),
        format!("manifest_file_count={}", report.manifest_file_count),
        format!("bundle_manifest_count={}", report.bundle_manifest_count),
        format!(
            "manifest_generation_coverage_count={}",
            report.manifest_generation_coverage_count
        ),
        format!(
            "bundle_generation_coverage_count={}",
            report.bundle_generation_coverage_count
        ),
        format!(
            "complete_generation_count={}",
            report.complete_generation_count
        ),
        format!(
            "incomplete_generation_count={}",
            report.incomplete_generation_count
        ),
        format!(
            "manifest_generations_missing_from_archive={}",
            report
                .manifest_generations_missing_from_archive
                .iter()
                .map(|summary| summary.generation_id.clone())
                .collect::<Vec<_>>()
                .join(" | ")
        ),
        format!(
            "bundle_generations_missing_from_archive={}",
            report
                .bundle_generations_missing_from_archive
                .iter()
                .map(|summary| summary.generation_id.clone())
                .collect::<Vec<_>>()
                .join(" | ")
        ),
        format!("invalid_artifact_count={}", report.invalid_artifact_count),
        format!(
            "read_only_lineage_analysis={}",
            report.read_only_lineage_analysis
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

    #[test]
    fn complete_lineage_yields_green_provenance() {
        let archive_dir = temp_dir("activation_provenance_complete_archive");
        let manifest_dir = temp_dir("activation_provenance_complete_manifest");
        let bundle_dir = temp_dir("activation_provenance_complete_bundle");
        let identity = write_archive_generation(
            &archive_dir.join("g1"),
            "2026-03-26T12:00:00Z",
            "prod_fp_a",
            "non_prod_fp_a",
        );
        write_manifest_file(
            &manifest_dir.join("m1.json"),
            std::slice::from_ref(&identity),
        );
        write_bundle_manifest(
            &bundle_dir.join("b1").join(BUNDLE_MANIFEST_FILENAME),
            &identity,
        );

        let report = build_report(&Config {
            archive_dir,
            manifest_dir,
            bundle_dir,
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactProvenanceVerdict::ArtifactProvenanceComplete
        );
        assert_eq!(report.complete_generation_count, 1);
    }

    #[test]
    fn empty_archive_and_empty_lineage_dirs_do_not_yield_complete() {
        let archive_dir = temp_dir("activation_provenance_empty_archive");
        let manifest_dir = temp_dir("activation_provenance_empty_manifest");
        let bundle_dir = temp_dir("activation_provenance_empty_bundle");

        let report = build_report(&Config {
            archive_dir,
            manifest_dir,
            bundle_dir,
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactProvenanceVerdict::ArtifactProvenanceIncomplete
        );
        assert_eq!(report.archive_generation_count, 0);
        assert_eq!(report.complete_generation_count, 0);
        assert!(report
            .reason
            .contains("no packet-backed archive generations found"));
    }

    #[test]
    fn missing_manifest_or_bundle_yields_incomplete_provenance() {
        let archive_dir = temp_dir("activation_provenance_incomplete_archive");
        let manifest_dir = temp_dir("activation_provenance_incomplete_manifest");
        let bundle_dir = temp_dir("activation_provenance_incomplete_bundle");
        let identity = write_archive_generation(
            &archive_dir.join("g1"),
            "2026-03-26T12:00:00Z",
            "prod_fp_a",
            "non_prod_fp_a",
        );
        write_manifest_file(
            &manifest_dir.join("m1.json"),
            std::slice::from_ref(&identity),
        );
        let _ = bundle_dir; // no bundle coverage

        let report = build_report(&Config {
            archive_dir,
            manifest_dir,
            bundle_dir,
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactProvenanceVerdict::ArtifactProvenanceIncomplete
        );
        assert_eq!(report.incomplete_generation_count, 1);
        assert!(report.generations[0].missing_bundle_coverage);
    }

    #[test]
    fn manifest_or_bundle_referencing_missing_archive_generation_yields_inconsistent_lineage() {
        let archive_dir = temp_dir("activation_provenance_inconsistent_archive");
        let manifest_dir = temp_dir("activation_provenance_inconsistent_manifest");
        let bundle_dir = temp_dir("activation_provenance_inconsistent_bundle");
        let archive_identity = write_archive_generation(
            &archive_dir.join("g1"),
            "2026-03-26T12:00:00Z",
            "prod_fp_a",
            "non_prod_fp_a",
        );
        write_manifest_file(
            &manifest_dir.join("m1.json"),
            &[
                archive_identity.clone(),
                identity("2026-03-26T13:00:00Z", "prod_fp_b", "non_prod_fp_b"),
            ],
        );
        write_bundle_manifest(
            &bundle_dir.join("b1").join(BUNDLE_MANIFEST_FILENAME),
            &archive_identity,
        );
        write_bundle_manifest(
            &bundle_dir.join("b2").join(BUNDLE_MANIFEST_FILENAME),
            &identity("2026-03-26T14:00:00Z", "prod_fp_c", "non_prod_fp_c"),
        );

        let report = build_report(&Config {
            archive_dir,
            manifest_dir,
            bundle_dir,
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactProvenanceVerdict::ArtifactProvenanceInconsistentLineage
        );
        assert_eq!(report.manifest_generations_missing_from_archive.len(), 1);
        assert_eq!(report.bundle_generations_missing_from_archive.len(), 1);
    }

    #[test]
    fn malformed_manifest_or_bundle_is_reported_as_invalid() {
        let archive_dir = temp_dir("activation_provenance_invalid_archive");
        let manifest_dir = temp_dir("activation_provenance_invalid_manifest");
        let bundle_dir = temp_dir("activation_provenance_invalid_bundle");
        write_archive_generation(
            &archive_dir.join("g1"),
            "2026-03-26T12:00:00Z",
            "prod_fp_a",
            "non_prod_fp_a",
        );
        fs::write(manifest_dir.join("m1.json"), "{broken").expect("write manifest");
        fs::create_dir_all(bundle_dir.join("b1")).expect("create bundle");
        fs::write(
            bundle_dir.join("b1").join(BUNDLE_MANIFEST_FILENAME),
            "{broken",
        )
        .expect("write bundle");

        let report = build_report(&Config {
            archive_dir,
            manifest_dir,
            bundle_dir,
            json: false,
        })
        .expect("report");

        assert_eq!(
            report.verdict,
            ArtifactProvenanceVerdict::ArtifactProvenanceInvalidArtifactsPresent
        );
        assert_eq!(report.invalid_artifact_count, 2);
    }

    #[test]
    fn provenance_report_does_not_mutate_archive_artifacts() {
        let archive_dir = temp_dir("activation_provenance_no_mutation_archive");
        let manifest_dir = temp_dir("activation_provenance_no_mutation_manifest");
        let bundle_dir = temp_dir("activation_provenance_no_mutation_bundle");
        let packet_path = archive_dir.join("g1").join("packet.json");
        let identity = write_archive_generation(
            &archive_dir.join("g1"),
            "2026-03-26T12:00:00Z",
            "prod_fp_a",
            "non_prod_fp_a",
        );
        write_manifest_file(
            &manifest_dir.join("m1.json"),
            std::slice::from_ref(&identity),
        );
        write_bundle_manifest(
            &bundle_dir.join("b1").join(BUNDLE_MANIFEST_FILENAME),
            &identity,
        );
        let before = fs::read_to_string(&packet_path).expect("before");

        let _ = build_report(&Config {
            archive_dir,
            manifest_dir,
            bundle_dir,
            json: false,
        })
        .expect("report");

        let after = fs::read_to_string(&packet_path).expect("after");
        assert_eq!(before, after);
    }

    fn write_archive_generation(
        dir: &Path,
        ts: &str,
        prod_fp: &str,
        non_prod_fp: &str,
    ) -> GenerationIdentity {
        fs::create_dir_all(dir).expect("create archive generation");
        fs::write(
            dir.join("packet.json"),
            serde_json::to_string_pretty(&sample_packet_json(ts, prod_fp, non_prod_fp))
                .expect("serialize packet"),
        )
        .expect("write packet");
        fs::write(
            dir.join("runbook.json"),
            serde_json::to_string_pretty(&sample_runbook_json(
                "2026-03-26T12:05:00Z",
                ts,
                prod_fp,
                non_prod_fp,
            ))
            .expect("serialize runbook"),
        )
        .expect("write runbook");
        fs::write(
            dir.join("runbook.md"),
            "# Tiny-Live Activation Runbook\n\nsample",
        )
        .expect("write markdown");
        identity(ts, prod_fp, non_prod_fp)
    }

    fn write_manifest_file(path: &Path, identities: &[GenerationIdentity]) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create manifest parent");
        }
        let generations = identities
            .iter()
            .map(|identity| {
                let packet_rel = format!(
                    "{}/packet.json",
                    identity.decision_packet_generated_at.to_rfc3339().replace(':', "-")
                );
                let runbook_rel = format!(
                    "{}/runbook.json",
                    identity.decision_packet_generated_at.to_rfc3339().replace(':', "-")
                );
                let markdown_rel = format!(
                    "{}/runbook.md",
                    identity.decision_packet_generated_at.to_rfc3339().replace(':', "-")
                );
                json!({
                    "identity": {
                        "decision_packet_generated_at": identity.decision_packet_generated_at.to_rfc3339(),
                        "prod_config_fingerprint_sha256": identity.prod_config_fingerprint_sha256,
                        "non_prod_config_fingerprint_sha256": identity.non_prod_config_fingerprint_sha256
                    },
                    "decision_packet_paths": [packet_rel],
                    "runbook_json_paths": [runbook_rel],
                    "runbook_markdown_paths": [markdown_rel],
                    "latest_packet_verdict": "decision_packet_discussion_ready_but_not_authorized",
                    "latest_runbook_verdict": "runbook_discussion_ready_but_not_authorized"
                })
            })
            .collect::<Vec<_>>();
        let files = identities
            .iter()
            .flat_map(|identity| {
                let prefix = identity.decision_packet_generated_at.to_rfc3339().replace(':', "-");
                vec![
                    json!({
                        "relative_path": format!("{prefix}/packet.json"),
                        "artifact_kind": "decision_packet_json",
                        "sha256": "dummy",
                        "generation_identity": {
                            "decision_packet_generated_at": identity.decision_packet_generated_at.to_rfc3339(),
                            "prod_config_fingerprint_sha256": identity.prod_config_fingerprint_sha256,
                            "non_prod_config_fingerprint_sha256": identity.non_prod_config_fingerprint_sha256
                        }
                    }),
                    json!({
                        "relative_path": format!("{prefix}/runbook.json"),
                        "artifact_kind": "runbook_json",
                        "sha256": "dummy",
                        "generation_identity": {
                            "decision_packet_generated_at": identity.decision_packet_generated_at.to_rfc3339(),
                            "prod_config_fingerprint_sha256": identity.prod_config_fingerprint_sha256,
                            "non_prod_config_fingerprint_sha256": identity.non_prod_config_fingerprint_sha256
                        }
                    }),
                    json!({
                        "relative_path": format!("{prefix}/runbook.md"),
                        "artifact_kind": "runbook_markdown",
                        "sha256": "dummy",
                        "generation_identity": {
                            "decision_packet_generated_at": identity.decision_packet_generated_at.to_rfc3339(),
                            "prod_config_fingerprint_sha256": identity.prod_config_fingerprint_sha256,
                            "non_prod_config_fingerprint_sha256": identity.non_prod_config_fingerprint_sha256
                        }
                    }),
                ]
            })
            .collect::<Vec<_>>();
        fs::write(
            path,
            serde_json::to_string_pretty(&json!({
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
            }))
            .expect("serialize manifest"),
        )
        .expect("write manifest");
    }

    fn write_bundle_manifest(path: &Path, identity: &GenerationIdentity) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create bundle parent");
        }
        fs::write(
            path,
            serde_json::to_string_pretty(&json!({
                "generated_at": "2026-03-26T15:10:00Z",
                "bundle_version": "1",
                "build_version": "0.1.0",
                "git_commit": "deadbeef",
                "source_archive_dir": "/tmp/archive",
                "generation_id": generation_id_from_identity(identity),
                "generation_identity": {
                    "decision_packet_generated_at": identity.decision_packet_generated_at.to_rfc3339(),
                    "prod_config_fingerprint_sha256": identity.prod_config_fingerprint_sha256,
                    "non_prod_config_fingerprint_sha256": identity.non_prod_config_fingerprint_sha256
                },
                "latest_packet_verdict": "decision_packet_discussion_ready_but_not_authorized",
                "latest_runbook_verdict": "runbook_discussion_ready_but_not_authorized",
                "hash_algorithm": "sha256",
                "file_count": 3,
                "files": [
                    {
                        "artifact_kind": "decision_packet_json",
                        "source_relative_path": "packet.json",
                        "bundle_relative_path": "artifacts/packet.json",
                        "sha256": "dummy"
                    },
                    {
                        "artifact_kind": "runbook_json",
                        "source_relative_path": "runbook.json",
                        "bundle_relative_path": "artifacts/runbook.json",
                        "sha256": "dummy"
                    },
                    {
                        "artifact_kind": "runbook_markdown",
                        "source_relative_path": "runbook.md",
                        "bundle_relative_path": "artifacts/runbook.md",
                        "sha256": "dummy"
                    }
                ]
            }))
            .expect("serialize bundle"),
        )
        .expect("write bundle");
    }

    fn identity(ts: &str, prod_fp: &str, non_prod_fp: &str) -> GenerationIdentity {
        GenerationIdentity {
            decision_packet_generated_at: DateTime::parse_from_rfc3339(ts)
                .expect("ts")
                .with_timezone(&Utc),
            prod_config_fingerprint_sha256: prod_fp.to_string(),
            non_prod_config_fingerprint_sha256: non_prod_fp.to_string(),
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
