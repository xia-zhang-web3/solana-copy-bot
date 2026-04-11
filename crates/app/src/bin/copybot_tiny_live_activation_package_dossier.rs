use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::env;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: copybot_tiny_live_activation_package_dossier [--root <path> | --session-dir <path>] [--json] (--latest-dossier | --session-dossier | --verify-dossier)";
const TOP_ACCEPTED_PACKAGE_STEP: &str = "clerestory_certificate";
const PACKAGE_FILE_PREFIX: &str = "tiny_live_activation_package_";
const SESSION_FILE_SUFFIX: &str = ".session.json";
const STATUS_FILE_SUFFIX: &str = ".status.json";
const REPORT_FILE_SUFFIX: &str = ".report.json";
const NOT_AUTHORIZED_SUMMARY: &str =
    "This tiny-live package dossier surface only audits persisted package session/status/report artifacts. It stays planning-safe, does not authorize activation, and does not override the Stage 3 production gate.";

const PACKAGE_CHAIN_STEPS: &[&str] = &[
    "turn_green",
    "execute_frozen",
    "decision_packet",
    "handoff_bundle",
    "review_receipt",
    "activation_ticket",
    "release_capsule",
    "attestation_seal",
    "provenance_certificate",
    "notarization_receipt",
    "registry_entry",
    "filing_certificate",
    "archive_receipt",
    "closure_certificate",
    "finality_receipt",
    "consummation_record",
    "completion_certificate",
    "culmination_receipt",
    "summit_certificate",
    "pinnacle_receipt",
    "capstone_certificate",
    "keystone_receipt",
    "cornerstone_certificate",
    "foundation_receipt",
    "bedrock_certificate",
    "basal_receipt",
    "substructure_certificate",
    "plinth_receipt",
    "pedestal_certificate",
    "dais_receipt",
    "rostrum_certificate",
    "podium_receipt",
    "lectern_certificate",
    "pulpit_receipt",
    "chancel_certificate",
    "apse_receipt",
    "sanctuary_certificate",
    "nave_receipt",
    "transept_certificate",
    "choir_receipt",
    "clerestory_certificate",
];

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
    root: Option<PathBuf>,
    session_dir: Option<PathBuf>,
    json: bool,
    mode: Mode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    LatestDossier,
    SessionDossier,
    VerifyDossier,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageDossierVerdict {
    TinyLivePackageDossierAvailable,
    TinyLivePackageDossierEmpty,
    TinyLivePackageDossierInvalid,
    TinyLivePackageDossierVerifyOk,
    TinyLivePackageDossierVerifyInvalid,
}

#[derive(Debug, Clone)]
struct PackageSessionArtifact {
    package_step_name: String,
    step_rank: usize,
    session_dir: PathBuf,
    generated_at: Option<DateTime<Utc>>,
    file_modified_at: Option<DateTime<Utc>>,
    top_identity_summary: Option<String>,
    top_identity_sha256: Option<String>,
    top_identity_algorithm: Option<String>,
    session_json: Option<Value>,
    status_json: Option<Value>,
    invalid_reasons: Vec<String>,
}

impl PackageSessionArtifact {
    fn ordering_time(&self) -> Option<DateTime<Utc>> {
        self.generated_at.or(self.file_modified_at)
    }
}

#[derive(Debug, Clone)]
struct LineageStepSurface {
    artifact: PackageSessionArtifact,
    next_lower_step_name: Option<String>,
    next_lower_session_dir: Option<PathBuf>,
    next_lower_report_path: Option<PathBuf>,
    next_lower_report_exists: bool,
    next_lower_report_parse_ok: bool,
    next_lower_identity_summary_link: Option<String>,
    next_lower_identity_sha256_link: Option<String>,
    next_lower_identity_algorithm_link: Option<String>,
    invalid_reasons: Vec<String>,
}

#[derive(Debug, Clone)]
struct LineageResolution {
    top_step_name: String,
    top_session_dir: PathBuf,
    lineage: Vec<LineageStepSurface>,
    invalid_reasons: Vec<String>,
}

#[derive(Debug, Clone)]
struct ManifestArtifactEntry {
    step_name: String,
    artifact_kind: ArtifactKind,
    nested_step_name: Option<String>,
    absolute_path: String,
    exists: bool,
    parseable: bool,
    size_bytes: Option<u64>,
    sha256_digest: Option<String>,
    invalid_reasons: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArtifactKind {
    Session,
    Status,
    Report,
}

impl ArtifactKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::Status => "status",
            Self::Report => "report",
        }
    }
}

#[derive(Debug, Clone)]
struct ManifestSurface {
    manifest_digest_sha256: String,
    required_artifact_count: usize,
    present_artifact_count: usize,
    parseable_artifact_count: usize,
    invalid_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct DossierLineageEntry {
    step_name: String,
    generated_at: Option<DateTime<Utc>>,
    session_path: String,
    top_identity_summary: Option<String>,
    top_identity_sha256: Option<String>,
    next_lower_step_name: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct PackageDossierReport {
    mode: String,
    verdict: TinyLivePackageDossierVerdict,
    reason: String,
    root: Option<String>,
    session_dir: Option<String>,
    top_accepted_package_step: String,
    latest_top_step_name: Option<String>,
    latest_top_session_path: Option<String>,
    latest_top_identity_summary: Option<String>,
    latest_top_identity_sha256: Option<String>,
    latest_top_identity_algorithm: Option<String>,
    ordered_lineage_summary: Option<String>,
    lineage_steps: Vec<DossierLineageEntry>,
    manifest_digest_sha256: Option<String>,
    required_artifact_count: usize,
    present_artifact_count: usize,
    parseable_artifact_count: usize,
    continuity_valid: bool,
    manifest_valid: bool,
    continuity_invalid_reasons: Vec<String>,
    manifest_invalid_reasons: Vec<String>,
    invalid_reasons: Vec<String>,
    planning_safe_only: bool,
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
    let mut args = args.into_iter();
    let mut root: Option<PathBuf> = None;
    let mut session_dir: Option<PathBuf> = None;
    let mut json = false;
    let mut mode: Option<Mode> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--root" => {
                root = Some(PathBuf::from(parse_required_value("--root", args.next())?));
            }
            "--session-dir" => {
                session_dir = Some(PathBuf::from(parse_required_value(
                    "--session-dir",
                    args.next(),
                )?));
            }
            "--latest-dossier" => set_mode(&mut mode, Mode::LatestDossier, "--latest-dossier")?,
            "--session-dossier" => set_mode(&mut mode, Mode::SessionDossier, "--session-dossier")?,
            "--verify-dossier" => set_mode(&mut mode, Mode::VerifyDossier, "--verify-dossier")?,
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unrecognized argument: {other}"),
        }
    }

    let Some(mode) = mode else {
        bail!(
            "exactly one of --latest-dossier, --session-dossier, or --verify-dossier is required"
        );
    };

    match mode {
        Mode::LatestDossier | Mode::VerifyDossier => {
            if root.is_none() {
                bail!("missing required --root");
            }
            if session_dir.is_some() {
                bail!("--session-dir is only valid with --session-dossier");
            }
        }
        Mode::SessionDossier => {
            if session_dir.is_none() {
                bail!("missing required --session-dir");
            }
            if root.is_some() {
                bail!("--root is only valid with --latest-dossier or --verify-dossier");
            }
        }
    }

    Ok(Some(Config {
        root,
        session_dir,
        json,
        mode,
    }))
}

fn run(config: Config) -> Result<String> {
    let report = match config.mode {
        Mode::LatestDossier => build_latest_dossier_report(&config)?,
        Mode::SessionDossier => build_session_dossier_report(&config)?,
        Mode::VerifyDossier => build_verify_dossier_report(&config)?,
    };
    if config.json {
        serde_json::to_string_pretty(&report)
            .context("failed serializing tiny-live package dossier report json")
    } else {
        Ok(render_dossier_human(&report))
    }
}

fn build_latest_dossier_report(config: &Config) -> Result<PackageDossierReport> {
    let root = config.root.as_ref().expect("validated root");
    let sessions = scan_package_sessions(root)?;
    let Some(candidate) = select_latest_top_chain_candidate(&sessions) else {
        return Ok(empty_dossier_report(
            "latest_dossier",
            Some(root.display().to_string()),
            None,
            format!(
                "no persisted immutable tiny-live package sessions were found under {}",
                root.display()
            ),
        ));
    };

    let resolution =
        resolve_lineage_from_top(&candidate.session_dir, &candidate.package_step_name)?;
    Ok(compose_dossier_report(
        "latest_dossier",
        Some(root.display().to_string()),
        None,
        resolution,
        false,
    ))
}

fn build_session_dossier_report(config: &Config) -> Result<PackageDossierReport> {
    let session_dir = config.session_dir.as_ref().expect("validated session dir");
    let resolution = resolve_lineage_from_session_dir(session_dir)?;
    Ok(compose_dossier_report(
        "session_dossier",
        None,
        Some(session_dir.display().to_string()),
        resolution,
        false,
    ))
}

fn build_verify_dossier_report(config: &Config) -> Result<PackageDossierReport> {
    let root = config.root.as_ref().expect("validated root");
    let sessions = scan_package_sessions(root)?;
    let Some(candidate) = select_latest_top_chain_candidate(&sessions) else {
        return Ok(empty_dossier_report(
            "verify_dossier",
            Some(root.display().to_string()),
            None,
            format!(
                "no persisted immutable tiny-live package sessions were found under {}",
                root.display()
            ),
        ));
    };

    let resolution =
        resolve_lineage_from_top(&candidate.session_dir, &candidate.package_step_name)?;
    Ok(compose_dossier_report(
        "verify_dossier",
        Some(root.display().to_string()),
        None,
        resolution,
        true,
    ))
}

fn compose_dossier_report(
    mode: &str,
    root: Option<String>,
    session_dir: Option<String>,
    resolution: LineageResolution,
    verify_mode: bool,
) -> PackageDossierReport {
    let manifest = build_manifest_surface(&resolution);
    let continuity_invalid_reasons = resolution.invalid_reasons.clone();
    let manifest_invalid_reasons = manifest.invalid_reasons.clone();
    let continuity_valid = continuity_invalid_reasons.is_empty();
    let manifest_valid = manifest_invalid_reasons.is_empty();
    let mut invalid_reasons = continuity_invalid_reasons.clone();
    invalid_reasons.extend(manifest_invalid_reasons.iter().cloned());

    if verify_mode || mode == "latest_dossier" {
        if resolution.top_step_name != TOP_ACCEPTED_PACKAGE_STEP {
            invalid_reasons.push(format!(
                "latest persisted immutable tiny-live package chain stops at {} below the current top accepted layer {}",
                resolution.top_step_name, TOP_ACCEPTED_PACKAGE_STEP
            ));
        }
    }
    invalid_reasons.dedup();

    let top = resolution.lineage.last().expect("non-empty lineage");
    let lineage_steps = resolution
        .lineage
        .iter()
        .map(|node| DossierLineageEntry {
            step_name: node.artifact.package_step_name.clone(),
            generated_at: node.artifact.ordering_time(),
            session_path: node.artifact.session_dir.display().to_string(),
            top_identity_summary: node.artifact.top_identity_summary.clone(),
            top_identity_sha256: node.artifact.top_identity_sha256.clone(),
            next_lower_step_name: node.next_lower_step_name.clone(),
        })
        .collect::<Vec<_>>();
    let ordered_lineage_summary = summarize_lineage_chain(&resolution.lineage);

    let (verdict, reason) = if invalid_reasons.is_empty() {
        if verify_mode {
            (
                TinyLivePackageDossierVerdict::TinyLivePackageDossierVerifyOk,
                format!(
                    "latest immutable tiny-live package dossier verified cleanly through {}",
                    resolution.top_step_name
                ),
            )
        } else {
            (
                TinyLivePackageDossierVerdict::TinyLivePackageDossierAvailable,
                format!(
                    "immutable tiny-live package dossier is complete and internally consistent through {}",
                    resolution.top_step_name
                ),
            )
        }
    } else if verify_mode {
        (
            TinyLivePackageDossierVerdict::TinyLivePackageDossierVerifyInvalid,
            invalid_reasons.join("; "),
        )
    } else {
        (
            TinyLivePackageDossierVerdict::TinyLivePackageDossierInvalid,
            invalid_reasons.join("; "),
        )
    };

    PackageDossierReport {
        mode: mode.to_string(),
        verdict,
        reason,
        root,
        session_dir,
        top_accepted_package_step: TOP_ACCEPTED_PACKAGE_STEP.to_string(),
        latest_top_step_name: Some(resolution.top_step_name.clone()),
        latest_top_session_path: Some(resolution.top_session_dir.display().to_string()),
        latest_top_identity_summary: top.artifact.top_identity_summary.clone(),
        latest_top_identity_sha256: top.artifact.top_identity_sha256.clone(),
        latest_top_identity_algorithm: top.artifact.top_identity_algorithm.clone(),
        ordered_lineage_summary,
        lineage_steps,
        manifest_digest_sha256: Some(manifest.manifest_digest_sha256),
        required_artifact_count: manifest.required_artifact_count,
        present_artifact_count: manifest.present_artifact_count,
        parseable_artifact_count: manifest.parseable_artifact_count,
        continuity_valid,
        manifest_valid,
        continuity_invalid_reasons,
        manifest_invalid_reasons,
        invalid_reasons,
        planning_safe_only: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary: NOT_AUTHORIZED_SUMMARY.to_string(),
    }
}

fn empty_dossier_report(
    mode: &str,
    root: Option<String>,
    session_dir: Option<String>,
    reason: String,
) -> PackageDossierReport {
    PackageDossierReport {
        mode: mode.to_string(),
        verdict: TinyLivePackageDossierVerdict::TinyLivePackageDossierEmpty,
        reason,
        root,
        session_dir,
        top_accepted_package_step: TOP_ACCEPTED_PACKAGE_STEP.to_string(),
        latest_top_step_name: None,
        latest_top_session_path: None,
        latest_top_identity_summary: None,
        latest_top_identity_sha256: None,
        latest_top_identity_algorithm: None,
        ordered_lineage_summary: None,
        lineage_steps: Vec::new(),
        manifest_digest_sha256: None,
        required_artifact_count: 0,
        present_artifact_count: 0,
        parseable_artifact_count: 0,
        continuity_valid: false,
        manifest_valid: false,
        continuity_invalid_reasons: Vec::new(),
        manifest_invalid_reasons: Vec::new(),
        invalid_reasons: Vec::new(),
        planning_safe_only: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary: NOT_AUTHORIZED_SUMMARY.to_string(),
    }
}

fn build_manifest_surface(resolution: &LineageResolution) -> ManifestSurface {
    let mut artifacts = Vec::new();
    for node in &resolution.lineage {
        let step_name = &node.artifact.package_step_name;
        let session_path = node.artifact.session_dir.join(format!(
            "{PACKAGE_FILE_PREFIX}{step_name}{SESSION_FILE_SUFFIX}"
        ));
        artifacts.push(inspect_manifest_artifact(
            step_name,
            ArtifactKind::Session,
            None,
            &session_path,
        ));

        let status_path = node.artifact.session_dir.join(format!(
            "{PACKAGE_FILE_PREFIX}{step_name}{STATUS_FILE_SUFFIX}"
        ));
        artifacts.push(inspect_manifest_artifact(
            step_name,
            ArtifactKind::Status,
            None,
            &status_path,
        ));

        if let Some(lower_step) = node.next_lower_step_name.as_ref() {
            let report_path = node.next_lower_report_path.clone().unwrap_or_else(|| {
                node.artifact.session_dir.join(format!(
                    "{PACKAGE_FILE_PREFIX}{step_name}.{lower_step}{REPORT_FILE_SUFFIX}"
                ))
            });
            artifacts.push(inspect_manifest_artifact(
                step_name,
                ArtifactKind::Report,
                Some(lower_step.as_str()),
                &report_path,
            ));
        }
    }

    let required_artifact_count = artifacts.len();
    let present_artifact_count = artifacts.iter().filter(|artifact| artifact.exists).count();
    let parseable_artifact_count = artifacts
        .iter()
        .filter(|artifact| artifact.parseable)
        .count();
    let mut invalid_reasons = artifacts
        .iter()
        .flat_map(|artifact| artifact.invalid_reasons.iter().cloned())
        .collect::<Vec<_>>();
    if present_artifact_count != required_artifact_count {
        invalid_reasons.push(format!(
            "manifest is incomplete: present {present_artifact_count} of {required_artifact_count} required persisted artifacts"
        ));
    }
    if parseable_artifact_count != required_artifact_count {
        invalid_reasons.push(format!(
            "manifest is not fully parseable: parseable {parseable_artifact_count} of {required_artifact_count} required persisted artifacts"
        ));
    }
    invalid_reasons.dedup();

    ManifestSurface {
        manifest_digest_sha256: compute_manifest_digest(&artifacts),
        required_artifact_count,
        present_artifact_count,
        parseable_artifact_count,
        invalid_reasons,
    }
}

fn inspect_manifest_artifact(
    step_name: &str,
    artifact_kind: ArtifactKind,
    nested_step_name: Option<&str>,
    path: &Path,
) -> ManifestArtifactEntry {
    let absolute_path = absolute_path_string(path);
    let exists = path.exists();
    let size_bytes = fs::metadata(path).ok().map(|metadata| metadata.len());
    let mut parseable = false;
    let mut sha256_digest = None;
    let mut invalid_reasons = Vec::new();

    if !exists {
        invalid_reasons.push(match artifact_kind {
            ArtifactKind::Session => {
                format!("missing required session artifact {absolute_path}")
            }
            ArtifactKind::Status => {
                format!("missing required status artifact {absolute_path}")
            }
            ArtifactKind::Report => format!(
                "missing required report artifact for {} -> {} at {}",
                step_name,
                nested_step_name.unwrap_or("<missing-nested-step>"),
                absolute_path
            ),
        });
    } else {
        match fs::read(path) {
            Ok(bytes) => {
                sha256_digest = Some(sha256_hex(&bytes));
                parseable = serde_json::from_slice::<Value>(&bytes).is_ok();
                if !parseable {
                    invalid_reasons.push(format!(
                        "failed parsing {} artifact {}",
                        artifact_kind.as_str(),
                        absolute_path
                    ));
                }
            }
            Err(error) => {
                invalid_reasons.push(format!(
                    "failed reading {} artifact {}: {}",
                    artifact_kind.as_str(),
                    absolute_path,
                    error
                ));
            }
        }
    }

    ManifestArtifactEntry {
        step_name: step_name.to_string(),
        artifact_kind,
        nested_step_name: nested_step_name.map(ToOwned::to_owned),
        absolute_path,
        exists,
        parseable,
        size_bytes,
        sha256_digest,
        invalid_reasons,
    }
}

fn compute_manifest_digest(artifacts: &[ManifestArtifactEntry]) -> String {
    let mut hasher = Sha256::new();
    for artifact in artifacts {
        hasher.update(artifact.step_name.as_bytes());
        hasher.update(b"\n");
        hasher.update(artifact.artifact_kind.as_str().as_bytes());
        hasher.update(b"\n");
        hasher.update(
            artifact
                .nested_step_name
                .as_deref()
                .unwrap_or("<none>")
                .as_bytes(),
        );
        hasher.update(b"\n");
        hasher.update(artifact.absolute_path.as_bytes());
        hasher.update(b"\n");
        hasher.update(if artifact.exists { "true" } else { "false" }.as_bytes());
        hasher.update(b"\n");
        hasher.update(if artifact.parseable { "true" } else { "false" }.as_bytes());
        hasher.update(b"\n");
        hasher.update(
            artifact
                .size_bytes
                .map(|size| size.to_string())
                .unwrap_or_else(|| "<none>".to_string())
                .as_bytes(),
        );
        hasher.update(b"\n");
        hasher.update(
            artifact
                .sha256_digest
                .as_deref()
                .unwrap_or("<none>")
                .as_bytes(),
        );
        hasher.update(b"\n");
    }
    format!("{:x}", hasher.finalize())
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn absolute_path_string(path: &Path) -> String {
    if path.is_absolute() {
        path.display().to_string()
    } else {
        env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
            .display()
            .to_string()
    }
}

fn scan_package_sessions(root: &Path) -> Result<Vec<PackageSessionArtifact>> {
    if !root.exists() {
        bail!("root {} does not exist", root.display());
    }
    if !root.is_dir() {
        bail!("root {} is not a directory", root.display());
    }

    let mut session_paths = Vec::new();
    collect_session_paths(root, &mut session_paths)?;
    let mut sessions = Vec::new();
    for session_path in session_paths {
        if let Some(surface) = inspect_session_path(&session_path)? {
            sessions.push(surface);
        }
    }
    Ok(sessions)
}

fn collect_session_paths(dir: &Path, session_paths: &mut Vec<PathBuf>) -> Result<()> {
    let entries =
        fs::read_dir(dir).with_context(|| format!("failed reading directory {}", dir.display()))?;
    for entry in entries {
        let entry = entry.with_context(|| format!("failed reading entry in {}", dir.display()))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .with_context(|| format!("failed reading file type for {}", path.display()))?;
        if file_type.is_symlink() {
            continue;
        }
        if file_type.is_dir() {
            collect_session_paths(&path, session_paths)?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(OsStr::to_str) else {
            continue;
        };
        if name.starts_with(PACKAGE_FILE_PREFIX) && name.ends_with(SESSION_FILE_SUFFIX) {
            session_paths.push(path);
        }
    }
    Ok(())
}

fn inspect_session_path(session_path: &Path) -> Result<Option<PackageSessionArtifact>> {
    let Some(file_name) = session_path.file_name().and_then(OsStr::to_str) else {
        return Ok(None);
    };
    let Some(step_name) = session_step_name_from_session_file(file_name) else {
        return Ok(None);
    };
    if step_rank(&step_name) == 0 {
        return Ok(None);
    }
    let session_dir = session_path.parent().ok_or_else(|| {
        anyhow!(
            "session path {} has no parent directory",
            session_path.display()
        )
    })?;
    Ok(Some(inspect_package_artifact(session_dir, &step_name)?))
}

fn select_latest_top_chain_candidate(
    sessions: &[PackageSessionArtifact],
) -> Option<&PackageSessionArtifact> {
    let max_rank = sessions.iter().map(|session| session.step_rank).max()?;
    sessions
        .iter()
        .filter(|session| session.step_rank == max_rank)
        .max_by(|left, right| {
            left.ordering_time()
                .cmp(&right.ordering_time())
                .then_with(|| left.session_dir.cmp(&right.session_dir))
        })
}

fn resolve_lineage_from_session_dir(session_dir: &Path) -> Result<LineageResolution> {
    if !session_dir.exists() {
        bail!("session dir {} does not exist", session_dir.display());
    }
    if !session_dir.is_dir() {
        bail!("session dir {} is not a directory", session_dir.display());
    }
    let step_name = detect_session_step_name(session_dir)?;
    resolve_lineage_from_top(session_dir, &step_name)
}

fn resolve_lineage_from_top(session_dir: &Path, step_name: &str) -> Result<LineageResolution> {
    let mut lineage_top_down = Vec::new();
    let mut visited = HashSet::new();
    let mut current_step = step_name.to_string();
    let mut current_session_dir = session_dir.to_path_buf();

    loop {
        let visit_key = format!("{current_step}::{}", current_session_dir.display());
        if !visited.insert(visit_key) {
            let mut node = inspect_lineage_step(&current_session_dir, &current_step)?;
            node.invalid_reasons.push(format!(
                "cycle detected while following persisted lineage at {}",
                current_session_dir.display()
            ));
            lineage_top_down.push(node);
            break;
        }

        let node = inspect_lineage_step(&current_session_dir, &current_step)?;
        let next_step_name = node.next_lower_step_name.clone();
        let next_session_dir = node.next_lower_session_dir.clone();
        lineage_top_down.push(node);

        match (next_step_name, next_session_dir) {
            (Some(next_step_name), Some(next_session_dir)) => {
                current_step = next_step_name;
                current_session_dir = next_session_dir;
            }
            _ => break,
        }
    }

    lineage_top_down.reverse();
    let invalid_reasons = validate_lineage_nodes(&mut lineage_top_down);
    let top = lineage_top_down.last().ok_or_else(|| {
        anyhow!(
            "failed resolving persisted lineage from {}",
            session_dir.display()
        )
    })?;

    Ok(LineageResolution {
        top_step_name: top.artifact.package_step_name.clone(),
        top_session_dir: top.artifact.session_dir.clone(),
        lineage: lineage_top_down,
        invalid_reasons,
    })
}

fn detect_session_step_name(session_dir: &Path) -> Result<String> {
    let mut step_names = Vec::new();
    let entries = fs::read_dir(session_dir)
        .with_context(|| format!("failed reading session dir {}", session_dir.display()))?;
    for entry in entries {
        let entry =
            entry.with_context(|| format!("failed reading entry in {}", session_dir.display()))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .with_context(|| format!("failed reading file type for {}", path.display()))?;
        if !file_type.is_file() {
            continue;
        }
        let Some(file_name) = path.file_name().and_then(OsStr::to_str) else {
            continue;
        };
        let Some(step_name) = session_step_name_from_session_file(file_name) else {
            continue;
        };
        if step_rank(&step_name) == 0 {
            continue;
        }
        step_names.push(step_name);
    }

    match step_names.len() {
        0 => bail!(
            "session dir {} does not contain a persisted tiny-live package session artifact",
            session_dir.display()
        ),
        1 => Ok(step_names.remove(0)),
        _ => bail!(
            "session dir {} contains multiple persisted tiny-live package session artifacts",
            session_dir.display()
        ),
    }
}

fn inspect_lineage_step(session_dir: &Path, step_name: &str) -> Result<LineageStepSurface> {
    let artifact = inspect_package_artifact(session_dir, step_name)?;
    let next_lower_step_name = previous_step(step_name).map(ToOwned::to_owned);
    let mut invalid_reasons = artifact.invalid_reasons.clone();

    let next_lower_session_dir = next_lower_step_name.as_ref().and_then(|lower_step| {
        let key = format!("{lower_step}_session_dir");
        extract_string(
            artifact.status_json.as_ref(),
            artifact.session_json.as_ref(),
            &key,
        )
        .map(PathBuf::from)
    });
    let next_lower_report_path = next_lower_step_name.as_ref().map(|lower_step| {
        session_dir.join(format!(
            "{PACKAGE_FILE_PREFIX}{step_name}.{lower_step}{REPORT_FILE_SUFFIX}"
        ))
    });
    let next_lower_report_exists = next_lower_report_path
        .as_ref()
        .map(|path| path.exists())
        .unwrap_or(false);
    let next_lower_report_parse_ok = next_lower_report_path
        .as_ref()
        .map(|path| load_json_value(path).is_ok())
        .unwrap_or(false);
    let next_lower_identity_summary_link = next_lower_step_name.as_ref().and_then(|lower_step| {
        extract_string(
            artifact.status_json.as_ref(),
            artifact.session_json.as_ref(),
            &format!("{lower_step}_summary"),
        )
    });
    let next_lower_identity_sha256_link = next_lower_step_name.as_ref().and_then(|lower_step| {
        extract_string(
            artifact.status_json.as_ref(),
            artifact.session_json.as_ref(),
            &format!("{lower_step}_sha256"),
        )
    });
    let next_lower_identity_algorithm_link = next_lower_step_name.as_ref().and_then(|lower_step| {
        extract_string(
            artifact.status_json.as_ref(),
            artifact.session_json.as_ref(),
            &format!("{lower_step}_algorithm"),
        )
    });

    if let Some(lower_step) = next_lower_step_name.as_ref() {
        let lower_session_dir_key = format!("{lower_step}_session_dir");
        if extract_non_empty_string(artifact.status_json.as_ref(), &lower_session_dir_key).is_none()
            || extract_non_empty_string(artifact.session_json.as_ref(), &lower_session_dir_key)
                .is_none()
        {
            invalid_reasons.push(format!(
                "{} is missing required duplicated {} linkage in session/status artifacts",
                step_name, lower_session_dir_key
            ));
        }
        if next_lower_session_dir.is_none() {
            invalid_reasons.push(format!(
                "{} is missing {}_session_dir",
                step_name, lower_step
            ));
        }
        if let Some(report_path) = next_lower_report_path.as_ref() {
            if !next_lower_report_exists {
                invalid_reasons.push(format!(
                    "{} is missing nested lineage report {}",
                    step_name,
                    report_path.display()
                ));
            } else if !next_lower_report_parse_ok {
                invalid_reasons.push(format!(
                    "{} has unparsable nested lineage report {}",
                    step_name,
                    report_path.display()
                ));
            }
        }
        if next_lower_identity_summary_link.is_none()
            && next_lower_identity_sha256_link.is_none()
            && next_lower_identity_algorithm_link.is_none()
        {
            invalid_reasons.push(format!(
                "{} does not store nested {} identity linkage beyond path/report references",
                step_name, lower_step
            ));
        }

        for key in [
            format!("{lower_step}_session_dir"),
            format!("{lower_step}_summary"),
            format!("{lower_step}_sha256"),
            format!("{lower_step}_algorithm"),
        ] {
            if let Some(reason) = mismatched_string_field(
                artifact.status_json.as_ref(),
                artifact.session_json.as_ref(),
                &key,
                step_name,
            ) {
                invalid_reasons.push(reason);
            }
        }
    }

    Ok(LineageStepSurface {
        artifact,
        next_lower_step_name,
        next_lower_session_dir,
        next_lower_report_path,
        next_lower_report_exists,
        next_lower_report_parse_ok,
        next_lower_identity_summary_link,
        next_lower_identity_sha256_link,
        next_lower_identity_algorithm_link,
        invalid_reasons,
    })
}

fn inspect_package_artifact(session_dir: &Path, step_name: &str) -> Result<PackageSessionArtifact> {
    let session_path = session_dir.join(format!(
        "{PACKAGE_FILE_PREFIX}{step_name}{SESSION_FILE_SUFFIX}"
    ));
    let status_path = session_dir.join(format!(
        "{PACKAGE_FILE_PREFIX}{step_name}{STATUS_FILE_SUFFIX}"
    ));
    let session_exists = session_path.exists();
    let status_exists = status_path.exists();
    let session_json = if session_exists {
        load_json_value(&session_path).ok()
    } else {
        None
    };
    let status_json = if status_exists {
        load_json_value(&status_path).ok()
    } else {
        None
    };

    let mut invalid_reasons = Vec::new();
    if !session_exists {
        invalid_reasons.push(format!(
            "missing session artifact {}",
            session_path.display()
        ));
    } else if session_json.is_none() {
        invalid_reasons.push(format!(
            "failed parsing session artifact {}",
            session_path.display()
        ));
    }
    if !status_exists {
        invalid_reasons.push(format!("missing status artifact {}", status_path.display()));
    } else if status_json.is_none() {
        invalid_reasons.push(format!(
            "failed parsing status artifact {}",
            status_path.display()
        ));
    }

    let generated_at = extract_datetime(status_json.as_ref(), "updated_at")
        .or_else(|| extract_datetime(session_json.as_ref(), "planned_at"));
    if generated_at.is_none() {
        invalid_reasons.push(format!(
            "{} is missing generated_at/updated_at persisted timing",
            session_dir.display()
        ));
    }

    let top_identity_summary = extract_string(
        status_json.as_ref(),
        session_json.as_ref(),
        &format!("{step_name}_summary"),
    );
    let top_identity_sha256 = extract_string(
        status_json.as_ref(),
        session_json.as_ref(),
        &format!("{step_name}_sha256"),
    );
    let top_identity_algorithm = extract_string(
        status_json.as_ref(),
        session_json.as_ref(),
        &format!("{step_name}_algorithm"),
    );

    for key in [
        "session_dir".to_string(),
        "result".to_string(),
        format!("{step_name}_summary"),
        format!("{step_name}_sha256"),
        format!("{step_name}_algorithm"),
    ] {
        if let Some(reason) =
            mismatched_string_field(status_json.as_ref(), session_json.as_ref(), &key, step_name)
        {
            invalid_reasons.push(reason);
        }
    }

    validate_expected_path_field(
        session_json.as_ref(),
        "session_dir",
        session_dir,
        step_name,
        "session",
        &mut invalid_reasons,
    );
    validate_expected_path_field(
        status_json.as_ref(),
        "session_dir",
        session_dir,
        step_name,
        "status",
        &mut invalid_reasons,
    );

    let file_modified_at = file_modified_at(&session_path);

    Ok(PackageSessionArtifact {
        package_step_name: step_name.to_string(),
        step_rank: step_rank(step_name),
        session_dir: session_dir.to_path_buf(),
        generated_at,
        file_modified_at,
        top_identity_summary,
        top_identity_sha256,
        top_identity_algorithm,
        session_json,
        status_json,
        invalid_reasons,
    })
}

fn validate_lineage_nodes(nodes: &mut [LineageStepSurface]) -> Vec<String> {
    let mut invalid_reasons = Vec::new();

    if nodes.is_empty() {
        invalid_reasons.push("resolved lineage is empty".to_string());
        return invalid_reasons;
    }

    if nodes[0].artifact.package_step_name != PACKAGE_CHAIN_STEPS[0] {
        let reason = format!(
            "resolved lineage does not begin at {}; root step is {}",
            PACKAGE_CHAIN_STEPS[0], nodes[0].artifact.package_step_name
        );
        nodes[0].invalid_reasons.push(reason.clone());
        invalid_reasons.push(reason);
    }

    let top_rank = nodes
        .last()
        .map(|node| node.artifact.step_rank)
        .unwrap_or(0);
    if nodes.len() != top_rank {
        let reason = format!(
            "resolved lineage length {} does not match top step rank {} for {}",
            nodes.len(),
            top_rank,
            nodes
                .last()
                .map(|node| node.artifact.package_step_name.as_str())
                .unwrap_or("<unknown>")
        );
        if let Some(top) = nodes.last_mut() {
            top.invalid_reasons.push(reason.clone());
        }
        invalid_reasons.push(reason);
    }

    for (index, node) in nodes.iter_mut().enumerate() {
        let expected_step = PACKAGE_CHAIN_STEPS
            .get(index)
            .copied()
            .unwrap_or("<out-of-range>");
        if node.artifact.package_step_name != expected_step {
            let reason = format!(
                "resolved lineage step {} is {}, expected {}",
                index, node.artifact.package_step_name, expected_step
            );
            node.invalid_reasons.push(reason.clone());
            invalid_reasons.push(reason);
        }
    }

    for index in 1..nodes.len() {
        let lower = nodes[index - 1].clone();
        let current = &mut nodes[index];

        if current.next_lower_step_name.as_deref()
            != Some(lower.artifact.package_step_name.as_str())
        {
            let reason = format!(
                "{} does not link to expected lower step {}; stored lower step is {}",
                current.artifact.package_step_name,
                lower.artifact.package_step_name,
                current
                    .next_lower_step_name
                    .as_deref()
                    .unwrap_or("<missing>")
            );
            current.invalid_reasons.push(reason.clone());
            invalid_reasons.push(reason);
        }

        if current.next_lower_session_dir.as_ref() != Some(&lower.artifact.session_dir) {
            let reason = format!(
                "{} lower-step session dir drifted: stored {} expected {}",
                current.artifact.package_step_name,
                current
                    .next_lower_session_dir
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "<missing>".to_string()),
                lower.artifact.session_dir.display()
            );
            current.invalid_reasons.push(reason.clone());
            invalid_reasons.push(reason);
        }

        if !current.next_lower_report_exists {
            let reason = format!(
                "{} is missing required nested report for {}",
                current.artifact.package_step_name, lower.artifact.package_step_name
            );
            current.invalid_reasons.push(reason.clone());
            invalid_reasons.push(reason);
        } else if !current.next_lower_report_parse_ok {
            let reason = format!(
                "{} has unparsable nested report for {}",
                current.artifact.package_step_name, lower.artifact.package_step_name
            );
            current.invalid_reasons.push(reason.clone());
            invalid_reasons.push(reason);
        }

        let stored_summary = current.next_lower_identity_summary_link.clone();
        let actual_summary = lower.artifact.top_identity_summary.clone();
        compare_nested_identity_link(
            &mut invalid_reasons,
            current,
            &lower.artifact,
            "summary",
            stored_summary.as_deref(),
            actual_summary.as_deref(),
        );
        let stored_sha256 = current.next_lower_identity_sha256_link.clone();
        let actual_sha256 = lower.artifact.top_identity_sha256.clone();
        compare_nested_identity_link(
            &mut invalid_reasons,
            current,
            &lower.artifact,
            "sha256",
            stored_sha256.as_deref(),
            actual_sha256.as_deref(),
        );
        let stored_algorithm = current.next_lower_identity_algorithm_link.clone();
        let actual_algorithm = lower.artifact.top_identity_algorithm.clone();
        compare_nested_identity_link(
            &mut invalid_reasons,
            current,
            &lower.artifact,
            "algorithm",
            stored_algorithm.as_deref(),
            actual_algorithm.as_deref(),
        );
    }

    for node in nodes.iter() {
        invalid_reasons.extend(node.invalid_reasons.iter().cloned());
    }
    invalid_reasons.dedup();
    invalid_reasons
}

fn compare_nested_identity_link(
    invalid_reasons: &mut Vec<String>,
    current: &mut LineageStepSurface,
    lower: &PackageSessionArtifact,
    field_name: &str,
    stored: Option<&str>,
    actual: Option<&str>,
) {
    match (stored, actual) {
        (Some(stored), Some(actual)) if stored != actual => {
            let reason = format!(
                "{} nested {} {} drifted: stored {} expected {}",
                current.artifact.package_step_name,
                lower.package_step_name,
                field_name,
                stored,
                actual
            );
            current.invalid_reasons.push(reason.clone());
            invalid_reasons.push(reason);
        }
        (Some(stored), None) => {
            let reason = format!(
                "{} stores nested {} {} {} but the lower persisted step does not expose that identity field",
                current.artifact.package_step_name, lower.package_step_name, field_name, stored
            );
            current.invalid_reasons.push(reason.clone());
            invalid_reasons.push(reason);
        }
        _ => {}
    }
}

fn summarize_lineage_chain(lineage: &[LineageStepSurface]) -> Option<String> {
    if lineage.is_empty() {
        return None;
    }
    Some(
        lineage
            .iter()
            .map(|node| node.artifact.package_step_name.as_str())
            .collect::<Vec<_>>()
            .join(" -> "),
    )
}

fn previous_step(step_name: &str) -> Option<&'static str> {
    let rank = step_rank(step_name);
    if rank <= 1 {
        None
    } else {
        PACKAGE_CHAIN_STEPS.get(rank - 2).copied()
    }
}

fn session_step_name_from_session_file(file_name: &str) -> Option<String> {
    let trimmed = file_name.strip_prefix(PACKAGE_FILE_PREFIX)?;
    let step = trimmed.strip_suffix(SESSION_FILE_SUFFIX)?;
    if step.is_empty() {
        None
    } else {
        Some(step.to_string())
    }
}

fn step_rank(step_name: &str) -> usize {
    PACKAGE_CHAIN_STEPS
        .iter()
        .position(|candidate| *candidate == step_name)
        .map(|index| index + 1)
        .unwrap_or(0)
}

fn load_json_value(path: &Path) -> Result<Value> {
    let bytes = fs::read(path).with_context(|| format!("failed reading {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("failed parsing {}", path.display()))
}

fn extract_datetime(value: Option<&Value>, key: &str) -> Option<DateTime<Utc>> {
    value
        .and_then(|json| json.get(key))
        .and_then(Value::as_str)
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

fn extract_string(status: Option<&Value>, session: Option<&Value>, key: &str) -> Option<String> {
    extract_non_empty_string(status, key).or_else(|| extract_non_empty_string(session, key))
}

fn extract_non_empty_string(value: Option<&Value>, key: &str) -> Option<String> {
    let candidate = value
        .and_then(|json| json.get(key))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|raw| !raw.is_empty())?;
    Some(candidate.to_string())
}

fn mismatched_string_field(
    status: Option<&Value>,
    session: Option<&Value>,
    key: &str,
    step_name: &str,
) -> Option<String> {
    let status_value = extract_non_empty_string(status, key);
    let session_value = extract_non_empty_string(session, key);
    match (status_value, session_value) {
        (Some(status_value), Some(session_value)) if status_value != session_value => {
            Some(format!(
                "{} persisted {} drifted between status ({}) and session ({})",
                step_name, key, status_value, session_value
            ))
        }
        _ => None,
    }
}

fn validate_expected_path_field(
    value: Option<&Value>,
    key: &str,
    expected: &Path,
    step_name: &str,
    source_name: &str,
    invalid_reasons: &mut Vec<String>,
) {
    if let Some(stored) = extract_non_empty_string(value, key) {
        if PathBuf::from(&stored) != expected {
            invalid_reasons.push(format!(
                "{} {} {} drifted: stored {} expected {}",
                step_name,
                source_name,
                key,
                stored,
                expected.display()
            ));
        }
    }
}

fn parse_required_value(flag: &str, value: Option<String>) -> Result<String> {
    value.ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn set_mode(mode: &mut Option<Mode>, value: Mode, flag: &str) -> Result<()> {
    if mode.replace(value).is_some() {
        bail!("duplicate mode flag provided: {flag}");
    }
    Ok(())
}

fn serialize_enum<T: Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| "<serialization-error>".to_string())
}

fn file_modified_at(path: &Path) -> Option<DateTime<Utc>> {
    fs::metadata(path)
        .ok()
        .and_then(|metadata| metadata.modified().ok())
        .map(DateTime::<Utc>::from)
}

fn render_dossier_human(report: &PackageDossierReport) -> String {
    let mut lines = vec![
        "event=copybot_tiny_live_activation_package_dossier".to_string(),
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!(
            "top_accepted_package_step={}",
            report.top_accepted_package_step
        ),
        format!("continuity_valid={}", report.continuity_valid),
        format!("manifest_valid={}", report.manifest_valid),
        format!("required_artifact_count={}", report.required_artifact_count),
        format!("present_artifact_count={}", report.present_artifact_count),
        format!(
            "parseable_artifact_count={}",
            report.parseable_artifact_count
        ),
    ];
    if let Some(root) = &report.root {
        lines.push(format!("root={root}"));
    }
    if let Some(session_dir) = &report.session_dir {
        lines.push(format!("session_dir={session_dir}"));
    }
    if let Some(step_name) = &report.latest_top_step_name {
        lines.push(format!("latest_top_step_name={step_name}"));
    }
    if let Some(path) = &report.latest_top_session_path {
        lines.push(format!("latest_top_session_path={path}"));
    }
    if let Some(summary) = &report.ordered_lineage_summary {
        lines.push(format!("ordered_lineage_summary={summary}"));
    }
    if let Some(digest) = &report.manifest_digest_sha256 {
        lines.push(format!("manifest_digest_sha256={digest}"));
    }
    lines.push("planning_safe_only=true".to_string());
    lines.push("activation_authorized=false".to_string());
    lines.push(format!(
        "not_authorized_summary={}",
        report.not_authorized_summary
    ));
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn latest_dossier_returns_valid_top_level_dossier_for_complete_latest_chain() {
        let fixture = DossierFixture::new(
            "tiny_live_package_dossier_latest_valid",
            TOP_ACCEPTED_PACKAGE_STEP,
        );

        let report = build_latest_dossier_report(&Config {
            root: Some(fixture.root.clone()),
            session_dir: None,
            json: true,
            mode: Mode::LatestDossier,
        })
        .expect("latest-dossier report");

        assert_eq!(
            report.verdict,
            TinyLivePackageDossierVerdict::TinyLivePackageDossierAvailable
        );
        assert!(report.continuity_valid);
        assert!(report.manifest_valid);
        assert_eq!(
            report.latest_top_step_name.as_deref(),
            Some(TOP_ACCEPTED_PACKAGE_STEP)
        );
        assert_eq!(
            report.latest_top_identity_sha256.as_deref(),
            Some("clerestory_certificate-sha256")
        );
        assert_eq!(
            report.lineage_steps.len(),
            step_rank(TOP_ACCEPTED_PACKAGE_STEP)
        );
        assert!(report.manifest_digest_sha256.is_some());
        assert_eq!(
            report.required_artifact_count,
            step_rank(TOP_ACCEPTED_PACKAGE_STEP) * 3 - 1
        );
    }

    #[test]
    fn session_dossier_works_for_one_explicit_session_dir_without_scanning_unrelated_roots() {
        let fixture = DossierFixture::new(
            "tiny_live_package_dossier_session_explicit",
            "review_receipt",
        );
        let _unrelated = DossierFixture::new(
            "tiny_live_package_dossier_session_unrelated",
            TOP_ACCEPTED_PACKAGE_STEP,
        );

        let report = build_session_dossier_report(&Config {
            root: None,
            session_dir: Some(fixture.step_dir("review_receipt").to_path_buf()),
            json: true,
            mode: Mode::SessionDossier,
        })
        .expect("session-dossier report");

        assert_eq!(
            report.verdict,
            TinyLivePackageDossierVerdict::TinyLivePackageDossierAvailable
        );
        assert_eq!(
            report.latest_top_step_name.as_deref(),
            Some("review_receipt")
        );
        assert_eq!(
            report.latest_top_session_path,
            Some(fixture.step_dir("review_receipt").display().to_string())
        );
        assert_eq!(report.lineage_steps.len(), step_rank("review_receipt"));
    }

    #[test]
    fn verify_dossier_fails_when_lineage_continuity_is_broken() {
        let fixture = DossierFixture::new(
            "tiny_live_package_dossier_broken_lineage",
            TOP_ACCEPTED_PACKAGE_STEP,
        );
        remove_json_key(
            fixture
                .step_dir("decision_packet")
                .join("tiny_live_activation_package_decision_packet.status.json"),
            "execute_frozen_session_dir",
        );

        let report = build_verify_dossier_report(&Config {
            root: Some(fixture.root.clone()),
            session_dir: None,
            json: true,
            mode: Mode::VerifyDossier,
        })
        .expect("verify-dossier report");

        assert_eq!(
            report.verdict,
            TinyLivePackageDossierVerdict::TinyLivePackageDossierVerifyInvalid
        );
        assert!(!report.continuity_valid);
        assert!(report
            .reason
            .contains("missing required duplicated execute_frozen_session_dir linkage"));
    }

    #[test]
    fn verify_dossier_fails_when_manifest_completeness_is_broken_by_missing_nested_artifact() {
        let fixture = DossierFixture::new(
            "tiny_live_package_dossier_missing_manifest",
            TOP_ACCEPTED_PACKAGE_STEP,
        );
        fs::remove_file(
            fixture
                .step_dir("decision_packet")
                .join("tiny_live_activation_package_decision_packet.execute_frozen.report.json"),
        )
        .unwrap();

        let report = build_verify_dossier_report(&Config {
            root: Some(fixture.root.clone()),
            session_dir: None,
            json: true,
            mode: Mode::VerifyDossier,
        })
        .expect("verify-dossier report");

        assert_eq!(
            report.verdict,
            TinyLivePackageDossierVerdict::TinyLivePackageDossierVerifyInvalid
        );
        assert!(!report.manifest_valid);
        assert!(report.reason.contains("missing required report artifact"));
    }

    #[test]
    fn verify_dossier_fails_when_nested_identity_linkage_drifts() {
        let fixture = DossierFixture::new(
            "tiny_live_package_dossier_identity_drift",
            TOP_ACCEPTED_PACKAGE_STEP,
        );
        set_json_string(
            fixture
                .step_dir(TOP_ACCEPTED_PACKAGE_STEP)
                .join("tiny_live_activation_package_clerestory_certificate.status.json"),
            "choir_receipt_sha256",
            "drifted-choir-sha256",
        );

        let report = build_verify_dossier_report(&Config {
            root: Some(fixture.root.clone()),
            session_dir: None,
            json: true,
            mode: Mode::VerifyDossier,
        })
        .expect("verify-dossier report");

        assert_eq!(
            report.verdict,
            TinyLivePackageDossierVerdict::TinyLivePackageDossierVerifyInvalid
        );
        assert!(!report.continuity_valid);
        assert!(report
            .reason
            .contains("clerestory_certificate nested choir_receipt sha256 drifted"));
    }

    #[test]
    fn all_modes_stay_planning_safe_and_do_not_imply_activation_authorization() {
        let fixture = DossierFixture::new(
            "tiny_live_package_dossier_planning_safe",
            TOP_ACCEPTED_PACKAGE_STEP,
        );

        let latest = build_latest_dossier_report(&Config {
            root: Some(fixture.root.clone()),
            session_dir: None,
            json: true,
            mode: Mode::LatestDossier,
        })
        .expect("latest-dossier report");
        let verify = build_verify_dossier_report(&Config {
            root: Some(fixture.root.clone()),
            session_dir: None,
            json: true,
            mode: Mode::VerifyDossier,
        })
        .expect("verify-dossier report");
        let session = build_session_dossier_report(&Config {
            root: None,
            session_dir: Some(fixture.step_dir(TOP_ACCEPTED_PACKAGE_STEP).to_path_buf()),
            json: true,
            mode: Mode::SessionDossier,
        })
        .expect("session-dossier report");

        for report in [&latest, &verify, &session] {
            assert!(report.planning_safe_only);
            assert!(report.execution_untouched);
            assert!(!report.activation_authorized);
            assert!(report
                .not_authorized_summary
                .contains("does not authorize activation"));
        }
        assert_eq!(
            verify.verdict,
            TinyLivePackageDossierVerdict::TinyLivePackageDossierVerifyOk
        );
    }

    #[derive(Debug)]
    struct DossierFixture {
        root: PathBuf,
        step_dirs: BTreeMap<String, PathBuf>,
    }

    impl DossierFixture {
        fn new(label: &str, top_step: &str) -> Self {
            let root = temp_dir(label);
            let mut step_dirs = BTreeMap::new();
            let top_rank = step_rank(top_step);
            let mut lower: Option<StepIdentity> = None;

            for index in 0..top_rank {
                let step_name = PACKAGE_CHAIN_STEPS[index];
                let step_dir = root.join(format!("{:02}-{step_name}", index + 1));
                let identity = StepIdentity {
                    step_name: step_name.to_string(),
                    session_dir: step_dir.clone(),
                    summary: format!("{step_name} summary"),
                    sha256: format!("{step_name}-sha256"),
                    algorithm: "sha256".to_string(),
                };
                write_step_session(
                    &step_dir,
                    step_name,
                    &format!("2026-04-11T00:{:02}:00Z", index),
                    &format!("2026-04-11T00:{:02}:30Z", index),
                    &identity,
                    lower.as_ref(),
                );
                lower = Some(identity);
                step_dirs.insert(step_name.to_string(), step_dir);
            }

            Self { root, step_dirs }
        }

        fn step_dir(&self, step_name: &str) -> &Path {
            self.step_dirs
                .get(step_name)
                .map(PathBuf::as_path)
                .unwrap_or_else(|| panic!("missing fixture step dir for {step_name}"))
        }
    }

    #[derive(Debug, Clone)]
    struct StepIdentity {
        step_name: String,
        session_dir: PathBuf,
        summary: String,
        sha256: String,
        algorithm: String,
    }

    fn write_step_session(
        step_dir: &Path,
        step_name: &str,
        planned_at: &str,
        updated_at: &str,
        identity: &StepIdentity,
        lower: Option<&StepIdentity>,
    ) {
        fs::create_dir_all(step_dir).unwrap();

        let mut session = serde_json::Map::new();
        session.insert("planned_at".to_string(), json!(planned_at));
        session.insert(
            "session_dir".to_string(),
            json!(step_dir.display().to_string()),
        );
        session.insert(
            "result".to_string(),
            json!("ready_for_manual_execution_when_gate_turns_green"),
        );
        session.insert(
            format!("{step_name}_summary"),
            json!(identity.summary.clone()),
        );
        session.insert(
            format!("{step_name}_sha256"),
            json!(identity.sha256.clone()),
        );
        session.insert(
            format!("{step_name}_algorithm"),
            json!(identity.algorithm.clone()),
        );

        let mut status = serde_json::Map::new();
        status.insert("updated_at".to_string(), json!(updated_at));
        status.insert(
            "session_dir".to_string(),
            json!(step_dir.display().to_string()),
        );
        status.insert(
            "result".to_string(),
            json!("ready_for_manual_execution_when_gate_turns_green"),
        );
        status.insert(
            format!("{step_name}_summary"),
            json!(identity.summary.clone()),
        );
        status.insert(
            format!("{step_name}_sha256"),
            json!(identity.sha256.clone()),
        );
        status.insert(
            format!("{step_name}_algorithm"),
            json!(identity.algorithm.clone()),
        );

        if let Some(lower) = lower {
            session.insert(
                format!("{}_session_dir", lower.step_name),
                json!(lower.session_dir.display().to_string()),
            );
            session.insert(
                format!("{}_summary", lower.step_name),
                json!(lower.summary.clone()),
            );
            session.insert(
                format!("{}_sha256", lower.step_name),
                json!(lower.sha256.clone()),
            );
            session.insert(
                format!("{}_algorithm", lower.step_name),
                json!(lower.algorithm.clone()),
            );

            status.insert(
                format!("{}_session_dir", lower.step_name),
                json!(lower.session_dir.display().to_string()),
            );
            status.insert(
                format!("{}_summary", lower.step_name),
                json!(lower.summary.clone()),
            );
            status.insert(
                format!("{}_sha256", lower.step_name),
                json!(lower.sha256.clone()),
            );
            status.insert(
                format!("{}_algorithm", lower.step_name),
                json!(lower.algorithm.clone()),
            );

            let report_path = step_dir.join(format!(
                "{PACKAGE_FILE_PREFIX}{step_name}.{}{REPORT_FILE_SUFFIX}",
                lower.step_name
            ));
            fs::write(
                report_path,
                serde_json::to_vec_pretty(&json!({
                    "verdict": "tiny_live_package_nested_verify_ok",
                    "reason": "nested dossier continuity ok"
                }))
                .unwrap(),
            )
            .unwrap();
        }

        fs::write(
            step_dir.join(format!(
                "{PACKAGE_FILE_PREFIX}{step_name}{SESSION_FILE_SUFFIX}"
            )),
            serde_json::to_vec_pretty(&Value::Object(session)).unwrap(),
        )
        .unwrap();
        fs::write(
            step_dir.join(format!(
                "{PACKAGE_FILE_PREFIX}{step_name}{STATUS_FILE_SUFFIX}"
            )),
            serde_json::to_vec_pretty(&Value::Object(status)).unwrap(),
        )
        .unwrap();
    }

    fn remove_json_key(path: PathBuf, key: &str) {
        let mut value: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        value
            .as_object_mut()
            .expect("json object")
            .remove(key)
            .expect("existing key");
        fs::write(path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
    }

    fn set_json_string(path: PathBuf, key: &str, new_value: &str) {
        let mut value: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        value
            .as_object_mut()
            .expect("json object")
            .insert(key.to_string(), json!(new_value));
        fs::write(path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
    }

    fn temp_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("{label}.{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }
}
