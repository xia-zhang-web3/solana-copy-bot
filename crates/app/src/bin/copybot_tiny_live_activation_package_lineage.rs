use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::env;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const USAGE: &str = "usage: copybot_tiny_live_activation_package_lineage [--root <path> | --session-dir <path>] [--json] (--latest-lineage | --verify-lineage | --session-lineage)";
const TOP_ACCEPTED_PACKAGE_STEP: &str = "clerestory_certificate";
const PACKAGE_FILE_PREFIX: &str = "tiny_live_activation_package_";
const SESSION_FILE_SUFFIX: &str = ".session.json";
const STATUS_FILE_SUFFIX: &str = ".status.json";
const REPORT_FILE_SUFFIX: &str = ".report.json";
const CLERESTORY_VERIFY_BIN: &str = "copybot_tiny_live_activation_package_clerestory_certificate";
const NOT_AUTHORIZED_SUMMARY: &str =
    "This tiny-live package lineage surface only audits persisted package session/status/report artifacts. It stays planning-safe, does not authorize activation, and does not override the Stage 3 production gate.";

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
    LatestLineage,
    VerifyLineage,
    SessionLineage,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageLineageVerdict {
    TinyLivePackageLineageAvailable,
    TinyLivePackageLineageEmpty,
    TinyLivePackageLineageInvalid,
    TinyLivePackageLineageVerifyOk,
    TinyLivePackageLineageVerifyInvalid,
}

#[derive(Debug, Clone)]
struct PackageSessionArtifact {
    package_step_name: String,
    step_rank: usize,
    session_dir: PathBuf,
    status_path: PathBuf,
    status_exists: bool,
    generated_at: Option<DateTime<Utc>>,
    file_modified_at: Option<DateTime<Utc>>,
    result: Option<String>,
    top_identity_summary: Option<String>,
    top_identity_sha256: Option<String>,
    top_identity_algorithm: Option<String>,
    session_json: Option<Value>,
    status_json: Option<Value>,
    invalid_reasons: Vec<String>,
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

#[derive(Debug, Clone, Serialize)]
struct LineageStepSummary {
    step_name: String,
    generated_at: Option<DateTime<Utc>>,
    session_path: String,
    status_path: Option<String>,
    result: Option<String>,
    result_classification: String,
    top_identity_summary: Option<String>,
    top_identity_sha256: Option<String>,
    top_identity_algorithm: Option<String>,
    next_lower_step_name: Option<String>,
    next_lower_session_path: Option<String>,
    next_lower_report_path: Option<String>,
    next_lower_identity_summary: Option<String>,
    next_lower_identity_sha256: Option<String>,
    next_lower_identity_algorithm: Option<String>,
    artifacts_complete: bool,
    invalid_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct PackageLineageReport {
    mode: String,
    verdict: TinyLivePackageLineageVerdict,
    reason: String,
    root: Option<String>,
    session_dir: Option<String>,
    top_accepted_package_step: String,
    latest_top_step_name: Option<String>,
    latest_top_session_path: Option<String>,
    lineage_chain_summary: Option<String>,
    internally_continuous: bool,
    invalid_reasons: Vec<String>,
    lineage: Vec<LineageStepSummary>,
    planning_safe_only: bool,
    execution_untouched: bool,
    activation_authorized: bool,
    not_authorized_summary: String,
}

#[derive(Debug, Clone, Serialize)]
struct VerifyLineageReport {
    mode: String,
    verdict: TinyLivePackageLineageVerdict,
    reason: String,
    root: String,
    top_accepted_package_step: String,
    latest_top_step_name: Option<String>,
    latest_top_session_path: Option<String>,
    lineage_chain_summary: Option<String>,
    internally_continuous: bool,
    invalid_reasons: Vec<String>,
    lineage: Vec<LineageStepSummary>,
    verification_attempted: bool,
    verification_command: Option<String>,
    verification_command_verdict: Option<String>,
    verification_command_reason: Option<String>,
    planning_safe_only: bool,
    execution_untouched: bool,
    activation_authorized: bool,
    not_authorized_summary: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ExternalVerifyReport {
    verdict: String,
    reason: String,
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
            "--latest-lineage" => set_mode(&mut mode, Mode::LatestLineage, "--latest-lineage")?,
            "--verify-lineage" => set_mode(&mut mode, Mode::VerifyLineage, "--verify-lineage")?,
            "--session-lineage" => set_mode(&mut mode, Mode::SessionLineage, "--session-lineage")?,
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unrecognized argument: {other}"),
        }
    }

    let Some(mode) = mode else {
        bail!(
            "exactly one of --latest-lineage, --verify-lineage, or --session-lineage is required"
        );
    };

    match mode {
        Mode::LatestLineage | Mode::VerifyLineage => {
            if root.is_none() {
                bail!("missing required --root");
            }
            if session_dir.is_some() {
                bail!("--session-dir is only valid with --session-lineage");
            }
        }
        Mode::SessionLineage => {
            if session_dir.is_none() {
                bail!("missing required --session-dir");
            }
            if root.is_some() {
                bail!("--root is only valid with --latest-lineage or --verify-lineage");
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
    match config.mode {
        Mode::LatestLineage => {
            let report = build_latest_lineage_report(&config)?;
            if config.json {
                serde_json::to_string_pretty(&report)
                    .context("failed serializing tiny-live latest-lineage report json")
            } else {
                Ok(render_lineage_human(&report))
            }
        }
        Mode::VerifyLineage => {
            let report = build_verify_lineage_report(&config)?;
            if config.json {
                serde_json::to_string_pretty(&report)
                    .context("failed serializing tiny-live verify-lineage report json")
            } else {
                Ok(render_verify_lineage_human(&report))
            }
        }
        Mode::SessionLineage => {
            let report = build_session_lineage_report(&config)?;
            if config.json {
                serde_json::to_string_pretty(&report)
                    .context("failed serializing tiny-live session-lineage report json")
            } else {
                Ok(render_lineage_human(&report))
            }
        }
    }
}

fn build_latest_lineage_report(config: &Config) -> Result<PackageLineageReport> {
    let root = config.root.as_ref().expect("validated root");
    let sessions = scan_package_sessions(root)?;
    let Some(candidate) = select_latest_top_chain_candidate(&sessions) else {
        return Ok(PackageLineageReport {
            mode: "latest_lineage".to_string(),
            verdict: TinyLivePackageLineageVerdict::TinyLivePackageLineageEmpty,
            reason: format!(
                "no persisted immutable tiny-live package sessions were found under {}",
                root.display()
            ),
            root: Some(root.display().to_string()),
            session_dir: None,
            top_accepted_package_step: TOP_ACCEPTED_PACKAGE_STEP.to_string(),
            latest_top_step_name: None,
            latest_top_session_path: None,
            lineage_chain_summary: None,
            internally_continuous: false,
            invalid_reasons: Vec::new(),
            lineage: Vec::new(),
            planning_safe_only: true,
            execution_untouched: true,
            activation_authorized: false,
            not_authorized_summary: NOT_AUTHORIZED_SUMMARY.to_string(),
        });
    };

    let resolution =
        resolve_lineage_from_top(&candidate.session_dir, &candidate.package_step_name)?;
    let mut invalid_reasons = resolution.invalid_reasons.clone();
    if resolution.top_step_name != TOP_ACCEPTED_PACKAGE_STEP {
        invalid_reasons.push(format!(
            "latest persisted immutable tiny-live package chain stops at {} below the current top accepted layer {}",
            resolution.top_step_name, TOP_ACCEPTED_PACKAGE_STEP
        ));
    }
    let lineage_chain_summary = summarize_lineage_chain(&resolution.lineage);
    let internally_continuous = invalid_reasons.is_empty();
    let (verdict, reason) = if internally_continuous {
        (
            TinyLivePackageLineageVerdict::TinyLivePackageLineageAvailable,
            format!(
                "latest persisted immutable tiny-live package lineage is internally continuous from turn_green through {}",
                resolution.top_step_name
            ),
        )
    } else {
        (
            TinyLivePackageLineageVerdict::TinyLivePackageLineageInvalid,
            invalid_reasons.join("; "),
        )
    };

    Ok(PackageLineageReport {
        mode: "latest_lineage".to_string(),
        verdict,
        reason,
        root: Some(root.display().to_string()),
        session_dir: None,
        top_accepted_package_step: TOP_ACCEPTED_PACKAGE_STEP.to_string(),
        latest_top_step_name: Some(resolution.top_step_name.clone()),
        latest_top_session_path: Some(resolution.top_session_dir.display().to_string()),
        lineage_chain_summary,
        internally_continuous,
        invalid_reasons,
        lineage: resolution
            .lineage
            .iter()
            .map(summarize_lineage_step)
            .collect(),
        planning_safe_only: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary: NOT_AUTHORIZED_SUMMARY.to_string(),
    })
}

fn build_verify_lineage_report(config: &Config) -> Result<VerifyLineageReport> {
    let root = config.root.as_ref().expect("validated root");
    let sessions = scan_package_sessions(root)?;
    let Some(candidate) = select_latest_top_chain_candidate(&sessions) else {
        return Ok(VerifyLineageReport {
            mode: "verify_lineage".to_string(),
            verdict: TinyLivePackageLineageVerdict::TinyLivePackageLineageEmpty,
            reason: format!(
                "no persisted immutable tiny-live package sessions were found under {}",
                root.display()
            ),
            root: root.display().to_string(),
            top_accepted_package_step: TOP_ACCEPTED_PACKAGE_STEP.to_string(),
            latest_top_step_name: None,
            latest_top_session_path: None,
            lineage_chain_summary: None,
            internally_continuous: false,
            invalid_reasons: Vec::new(),
            lineage: Vec::new(),
            verification_attempted: false,
            verification_command: None,
            verification_command_verdict: None,
            verification_command_reason: None,
            planning_safe_only: true,
            execution_untouched: true,
            activation_authorized: false,
            not_authorized_summary: NOT_AUTHORIZED_SUMMARY.to_string(),
        });
    };

    let resolution =
        resolve_lineage_from_top(&candidate.session_dir, &candidate.package_step_name)?;
    let mut invalid_reasons = resolution.invalid_reasons.clone();
    if resolution.top_step_name != TOP_ACCEPTED_PACKAGE_STEP {
        invalid_reasons.push(format!(
            "latest persisted immutable tiny-live package chain stops at {} below the current top accepted layer {}",
            resolution.top_step_name, TOP_ACCEPTED_PACKAGE_STEP
        ));
    }

    let lineage_chain_summary = summarize_lineage_chain(&resolution.lineage);
    let internally_continuous = invalid_reasons.is_empty();
    let lineage = resolution
        .lineage
        .iter()
        .map(summarize_lineage_step)
        .collect::<Vec<_>>();

    if !internally_continuous {
        return Ok(VerifyLineageReport {
            mode: "verify_lineage".to_string(),
            verdict: TinyLivePackageLineageVerdict::TinyLivePackageLineageVerifyInvalid,
            reason: invalid_reasons.join("; "),
            root: root.display().to_string(),
            top_accepted_package_step: TOP_ACCEPTED_PACKAGE_STEP.to_string(),
            latest_top_step_name: Some(resolution.top_step_name.clone()),
            latest_top_session_path: Some(resolution.top_session_dir.display().to_string()),
            lineage_chain_summary,
            internally_continuous: false,
            invalid_reasons,
            lineage,
            verification_attempted: false,
            verification_command: None,
            verification_command_verdict: None,
            verification_command_reason: None,
            planning_safe_only: true,
            execution_untouched: true,
            activation_authorized: false,
            not_authorized_summary: NOT_AUTHORIZED_SUMMARY.to_string(),
        });
    }

    let choir_receipt_session_dir =
        find_lineage_step_session_dir(&resolution.lineage, "choir_receipt")
            .ok_or_else(|| anyhow!("resolved lineage is missing choir_receipt"))?;
    let decision_packet_session_dir =
        find_lineage_step_session_dir(&resolution.lineage, "decision_packet")
            .ok_or_else(|| anyhow!("resolved lineage is missing decision_packet"))?;
    let args = vec![
        "--choir-receipt-session-dir".to_string(),
        choir_receipt_session_dir.display().to_string(),
        "--confirm-decision-packet-session-dir".to_string(),
        decision_packet_session_dir.display().to_string(),
        "--session-dir".to_string(),
        resolution.top_session_dir.display().to_string(),
        "--verify-live-package-clerestory-certificate".to_string(),
        "--json".to_string(),
    ];
    let command_preview = render_command_preview(CLERESTORY_VERIFY_BIN, &args);

    match run_json_command(CLERESTORY_VERIFY_BIN, &args) {
        Ok(raw) => {
            let verify: ExternalVerifyReport = serde_json::from_value(raw).with_context(|| {
                format!(
                    "failed parsing JSON verify output from {} for lineage top session {}",
                    CLERESTORY_VERIFY_BIN,
                    resolution.top_session_dir.display()
                )
            })?;
            if verify.verdict == "tiny_live_package_clerestory_certificate_verify_ok" {
                Ok(VerifyLineageReport {
                    mode: "verify_lineage".to_string(),
                    verdict: TinyLivePackageLineageVerdict::TinyLivePackageLineageVerifyOk,
                    reason: format!(
                        "latest persisted immutable tiny-live package lineage verified cleanly through {}",
                        resolution.top_step_name
                    ),
                    root: root.display().to_string(),
                    top_accepted_package_step: TOP_ACCEPTED_PACKAGE_STEP.to_string(),
                    latest_top_step_name: Some(resolution.top_step_name.clone()),
                    latest_top_session_path: Some(resolution.top_session_dir.display().to_string()),
                    lineage_chain_summary,
                    internally_continuous: true,
                    invalid_reasons: Vec::new(),
                    lineage,
                    verification_attempted: true,
                    verification_command: Some(command_preview),
                    verification_command_verdict: Some(verify.verdict),
                    verification_command_reason: Some(verify.reason),
                    planning_safe_only: true,
                    execution_untouched: true,
                    activation_authorized: false,
                    not_authorized_summary: NOT_AUTHORIZED_SUMMARY.to_string(),
                })
            } else {
                invalid_reasons.push(format!(
                    "latest top-layer lineage verification returned {}: {}",
                    verify.verdict, verify.reason
                ));
                Ok(VerifyLineageReport {
                    mode: "verify_lineage".to_string(),
                    verdict: TinyLivePackageLineageVerdict::TinyLivePackageLineageVerifyInvalid,
                    reason: invalid_reasons.join("; "),
                    root: root.display().to_string(),
                    top_accepted_package_step: TOP_ACCEPTED_PACKAGE_STEP.to_string(),
                    latest_top_step_name: Some(resolution.top_step_name.clone()),
                    latest_top_session_path: Some(resolution.top_session_dir.display().to_string()),
                    lineage_chain_summary,
                    internally_continuous: false,
                    invalid_reasons,
                    lineage,
                    verification_attempted: true,
                    verification_command: Some(command_preview),
                    verification_command_verdict: Some(verify.verdict),
                    verification_command_reason: Some(verify.reason),
                    planning_safe_only: true,
                    execution_untouched: true,
                    activation_authorized: false,
                    not_authorized_summary: NOT_AUTHORIZED_SUMMARY.to_string(),
                })
            }
        }
        Err(error) => {
            invalid_reasons.push(format!(
                "failed running {} for lineage top session {}: {error}",
                CLERESTORY_VERIFY_BIN,
                resolution.top_session_dir.display()
            ));
            Ok(VerifyLineageReport {
                mode: "verify_lineage".to_string(),
                verdict: TinyLivePackageLineageVerdict::TinyLivePackageLineageVerifyInvalid,
                reason: invalid_reasons.join("; "),
                root: root.display().to_string(),
                top_accepted_package_step: TOP_ACCEPTED_PACKAGE_STEP.to_string(),
                latest_top_step_name: Some(resolution.top_step_name.clone()),
                latest_top_session_path: Some(resolution.top_session_dir.display().to_string()),
                lineage_chain_summary,
                internally_continuous: false,
                invalid_reasons,
                lineage,
                verification_attempted: true,
                verification_command: Some(command_preview),
                verification_command_verdict: None,
                verification_command_reason: None,
                planning_safe_only: true,
                execution_untouched: true,
                activation_authorized: false,
                not_authorized_summary: NOT_AUTHORIZED_SUMMARY.to_string(),
            })
        }
    }
}

fn build_session_lineage_report(config: &Config) -> Result<PackageLineageReport> {
    let session_dir = config.session_dir.as_ref().expect("validated session dir");
    let resolution = resolve_lineage_from_session_dir(session_dir)?;
    let invalid_reasons = resolution.invalid_reasons.clone();
    let internally_continuous = invalid_reasons.is_empty();
    let lineage_chain_summary = summarize_lineage_chain(&resolution.lineage);
    let (verdict, reason) = if internally_continuous {
        (
            TinyLivePackageLineageVerdict::TinyLivePackageLineageAvailable,
            format!(
                "persisted immutable tiny-live session lineage is internally continuous from turn_green through {}",
                resolution.top_step_name
            ),
        )
    } else {
        (
            TinyLivePackageLineageVerdict::TinyLivePackageLineageInvalid,
            invalid_reasons.join("; "),
        )
    };

    Ok(PackageLineageReport {
        mode: "session_lineage".to_string(),
        verdict,
        reason,
        root: None,
        session_dir: Some(session_dir.display().to_string()),
        top_accepted_package_step: TOP_ACCEPTED_PACKAGE_STEP.to_string(),
        latest_top_step_name: Some(resolution.top_step_name.clone()),
        latest_top_session_path: Some(resolution.top_session_dir.display().to_string()),
        lineage_chain_summary,
        internally_continuous,
        invalid_reasons,
        lineage: resolution
            .lineage
            .iter()
            .map(summarize_lineage_step)
            .collect(),
        planning_safe_only: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary: NOT_AUTHORIZED_SUMMARY.to_string(),
    })
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
        if !name.starts_with(PACKAGE_FILE_PREFIX) || !name.ends_with(SESSION_FILE_SUFFIX) {
            continue;
        }
        session_paths.push(path);
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

    let result = extract_string(status_json.as_ref(), session_json.as_ref(), "result");
    if result.is_none() {
        invalid_reasons.push(format!(
            "{} is missing persisted result classification",
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
        status_path,
        status_exists,
        generated_at,
        file_modified_at,
        result,
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

fn summarize_lineage_step(surface: &LineageStepSurface) -> LineageStepSummary {
    LineageStepSummary {
        step_name: surface.artifact.package_step_name.clone(),
        generated_at: surface.artifact.ordering_time(),
        session_path: surface.artifact.session_dir.display().to_string(),
        status_path: if surface.artifact.status_exists {
            Some(surface.artifact.status_path.display().to_string())
        } else {
            None
        },
        result: surface.artifact.result.clone(),
        result_classification: classify_result(
            surface.artifact.result.as_deref(),
            &surface.invalid_reasons,
        ),
        top_identity_summary: surface.artifact.top_identity_summary.clone(),
        top_identity_sha256: surface.artifact.top_identity_sha256.clone(),
        top_identity_algorithm: surface.artifact.top_identity_algorithm.clone(),
        next_lower_step_name: surface.next_lower_step_name.clone(),
        next_lower_session_path: surface
            .next_lower_session_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        next_lower_report_path: surface
            .next_lower_report_path
            .as_ref()
            .map(|path| path.display().to_string()),
        next_lower_identity_summary: surface.next_lower_identity_summary_link.clone(),
        next_lower_identity_sha256: surface.next_lower_identity_sha256_link.clone(),
        next_lower_identity_algorithm: surface.next_lower_identity_algorithm_link.clone(),
        artifacts_complete: surface.invalid_reasons.is_empty(),
        invalid_reasons: surface.invalid_reasons.clone(),
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

fn find_lineage_step_session_dir(
    lineage: &[LineageStepSurface],
    step_name: &str,
) -> Option<PathBuf> {
    lineage
        .iter()
        .find(|node| node.artifact.package_step_name == step_name)
        .map(|node| node.artifact.session_dir.clone())
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

impl PackageSessionArtifact {
    fn ordering_time(&self) -> Option<DateTime<Utc>> {
        self.generated_at.or(self.file_modified_at)
    }
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

fn classify_result(result: Option<&str>, invalid_reasons: &[String]) -> String {
    if !invalid_reasons.is_empty() {
        return "invalid_or_incomplete".to_string();
    }
    match result {
        Some("ready_for_manual_execution_when_gate_turns_green") => {
            "ready_for_manual_execution_when_gate_turns_green".to_string()
        }
        Some(value) if value.starts_with("refused_now_by_") => value.to_string(),
        Some(value) => format!("non_standard:{value}"),
        None => "invalid_or_incomplete".to_string(),
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

fn render_command_preview(binary: &str, args: &[String]) -> String {
    let mut rendered = vec![shell_escape(binary)];
    rendered.extend(args.iter().map(|arg| shell_escape(arg)));
    rendered.join(" ")
}

fn shell_escape(value: &str) -> String {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '.' | '_' | '-' | ':' | '='))
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

fn run_json_command(binary: &str, args: &[String]) -> Result<Value> {
    let output = Command::new(binary)
        .args(args)
        .output()
        .with_context(|| format!("failed executing {binary}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        bail!(
            "{binary} exited with status {}: {}",
            output
                .status
                .code()
                .map(|code| code.to_string())
                .unwrap_or_else(|| "<signal>".to_string()),
            if stderr.is_empty() {
                "<no stderr>".to_string()
            } else {
                stderr
            }
        );
    }
    serde_json::from_slice(&output.stdout)
        .with_context(|| format!("failed parsing JSON stdout from {binary}"))
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

fn render_lineage_human(report: &PackageLineageReport) -> String {
    let mut lines = vec![
        "event=copybot_tiny_live_activation_package_lineage".to_string(),
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!(
            "top_accepted_package_step={}",
            report.top_accepted_package_step
        ),
        format!("internally_continuous={}", report.internally_continuous),
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
    if let Some(summary) = &report.lineage_chain_summary {
        lines.push(format!("lineage_chain_summary={summary}"));
    }
    for (index, step) in report.lineage.iter().enumerate() {
        lines.push(format!("lineage[{index}].step_name={}", step.step_name));
        lines.push(format!(
            "lineage[{index}].session_path={}",
            step.session_path
        ));
        lines.push(format!(
            "lineage[{index}].result_classification={}",
            step.result_classification
        ));
        if let Some(next_lower_step_name) = &step.next_lower_step_name {
            lines.push(format!(
                "lineage[{index}].next_lower_step_name={next_lower_step_name}"
            ));
        }
    }
    lines.push("planning_safe_only=true".to_string());
    lines.push("activation_authorized=false".to_string());
    lines.push(format!(
        "not_authorized_summary={}",
        report.not_authorized_summary
    ));
    lines.join("\n")
}

fn render_verify_lineage_human(report: &VerifyLineageReport) -> String {
    let mut lines = vec![
        "event=copybot_tiny_live_activation_package_lineage".to_string(),
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("root={}", report.root),
        format!(
            "top_accepted_package_step={}",
            report.top_accepted_package_step
        ),
        format!("internally_continuous={}", report.internally_continuous),
        format!("verification_attempted={}", report.verification_attempted),
    ];
    if let Some(step_name) = &report.latest_top_step_name {
        lines.push(format!("latest_top_step_name={step_name}"));
    }
    if let Some(path) = &report.latest_top_session_path {
        lines.push(format!("latest_top_session_path={path}"));
    }
    if let Some(summary) = &report.lineage_chain_summary {
        lines.push(format!("lineage_chain_summary={summary}"));
    }
    if let Some(command) = &report.verification_command {
        lines.push(format!("verification_command={command}"));
    }
    if let Some(verdict) = &report.verification_command_verdict {
        lines.push(format!("verification_command_verdict={verdict}"));
    }
    if let Some(reason) = &report.verification_command_reason {
        lines.push(format!("verification_command_reason={reason}"));
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
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{Mutex, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn latest_lineage_returns_full_ordered_chain_for_valid_persisted_latest_session() {
        let fixture = LineageFixture::new(
            "tiny_live_package_lineage_latest_valid",
            TOP_ACCEPTED_PACKAGE_STEP,
        );

        let report = build_latest_lineage_report(&Config {
            root: Some(fixture.root.clone()),
            session_dir: None,
            json: true,
            mode: Mode::LatestLineage,
        })
        .expect("latest-lineage report");

        assert_eq!(
            report.verdict,
            TinyLivePackageLineageVerdict::TinyLivePackageLineageAvailable
        );
        assert!(report.internally_continuous);
        assert_eq!(
            report.latest_top_step_name.as_deref(),
            Some(TOP_ACCEPTED_PACKAGE_STEP)
        );
        assert_eq!(
            report.lineage.first().map(|step| step.step_name.as_str()),
            Some("turn_green")
        );
        assert_eq!(
            report.lineage.last().map(|step| step.step_name.as_str()),
            Some(TOP_ACCEPTED_PACKAGE_STEP)
        );
        assert_eq!(report.lineage.len(), step_rank(TOP_ACCEPTED_PACKAGE_STEP));
    }

    #[test]
    fn verify_lineage_fails_when_one_nested_session_link_is_missing() {
        let fixture = LineageFixture::new(
            "tiny_live_package_lineage_missing_link",
            TOP_ACCEPTED_PACKAGE_STEP,
        );
        remove_json_key(
            fixture
                .step_dir("decision_packet")
                .join("tiny_live_activation_package_decision_packet.status.json"),
            "execute_frozen_session_dir",
        );

        let bin_dir = temp_dir("tiny_live_package_lineage_stub_missing_link");
        write_clerestory_verify_stub(&bin_dir);
        let _path_guard = PathGuard::install(&bin_dir);

        let report = build_verify_lineage_report(&Config {
            root: Some(fixture.root.clone()),
            session_dir: None,
            json: true,
            mode: Mode::VerifyLineage,
        })
        .expect("verify-lineage report");

        assert_eq!(
            report.verdict,
            TinyLivePackageLineageVerdict::TinyLivePackageLineageVerifyInvalid
        );
        assert!(!report.verification_attempted);
        assert!(report
            .reason
            .contains("missing required duplicated execute_frozen_session_dir linkage"));
    }

    #[test]
    fn verify_lineage_fails_when_top_level_latest_chain_is_below_clerestory_certificate() {
        let fixture = LineageFixture::new("tiny_live_package_lineage_below_top", "choir_receipt");

        let report = build_verify_lineage_report(&Config {
            root: Some(fixture.root.clone()),
            session_dir: None,
            json: true,
            mode: Mode::VerifyLineage,
        })
        .expect("verify-lineage report");

        assert_eq!(
            report.verdict,
            TinyLivePackageLineageVerdict::TinyLivePackageLineageVerifyInvalid
        );
        assert!(!report.verification_attempted);
        assert!(report
            .reason
            .contains("below the current top accepted layer clerestory_certificate"));
    }

    #[test]
    fn verify_lineage_fails_when_nested_identity_summary_or_sha_link_drifts() {
        let fixture =
            LineageFixture::new("tiny_live_package_lineage_drift", TOP_ACCEPTED_PACKAGE_STEP);
        set_json_string(
            fixture
                .step_dir(TOP_ACCEPTED_PACKAGE_STEP)
                .join("tiny_live_activation_package_clerestory_certificate.status.json"),
            "choir_receipt_sha256",
            "drifted-choir-sha256",
        );

        let bin_dir = temp_dir("tiny_live_package_lineage_stub_drift");
        write_clerestory_verify_stub(&bin_dir);
        let _path_guard = PathGuard::install(&bin_dir);

        let report = build_verify_lineage_report(&Config {
            root: Some(fixture.root.clone()),
            session_dir: None,
            json: true,
            mode: Mode::VerifyLineage,
        })
        .expect("verify-lineage report");

        assert_eq!(
            report.verdict,
            TinyLivePackageLineageVerdict::TinyLivePackageLineageVerifyInvalid
        );
        assert!(!report.verification_attempted);
        assert!(report
            .reason
            .contains("clerestory_certificate nested choir_receipt sha256 drifted"));
    }

    #[test]
    fn session_lineage_works_for_explicit_session_dir_without_scanning_unrelated_roots() {
        let fixture = LineageFixture::new(
            "tiny_live_package_lineage_session_explicit",
            "review_receipt",
        );
        let _unrelated = LineageFixture::new(
            "tiny_live_package_lineage_session_unrelated",
            TOP_ACCEPTED_PACKAGE_STEP,
        );

        let report = build_session_lineage_report(&Config {
            root: None,
            session_dir: Some(fixture.step_dir("review_receipt").to_path_buf()),
            json: true,
            mode: Mode::SessionLineage,
        })
        .expect("session-lineage report");

        assert_eq!(
            report.verdict,
            TinyLivePackageLineageVerdict::TinyLivePackageLineageAvailable
        );
        assert_eq!(report.lineage.len(), step_rank("review_receipt"));
        assert_eq!(
            report.latest_top_step_name.as_deref(),
            Some("review_receipt")
        );
        assert_eq!(
            report.latest_top_session_path.as_deref(),
            Some(
                fixture
                    .step_dir("review_receipt")
                    .display()
                    .to_string()
                    .as_str()
            )
        );
    }

    #[test]
    fn all_modes_stay_planning_safe_and_do_not_imply_activation_authorization() {
        let fixture = LineageFixture::new(
            "tiny_live_package_lineage_planning_safe",
            TOP_ACCEPTED_PACKAGE_STEP,
        );
        let bin_dir = temp_dir("tiny_live_package_lineage_stub_planning_safe");
        write_clerestory_verify_stub(&bin_dir);
        let _path_guard = PathGuard::install(&bin_dir);

        let latest = build_latest_lineage_report(&Config {
            root: Some(fixture.root.clone()),
            session_dir: None,
            json: true,
            mode: Mode::LatestLineage,
        })
        .expect("latest-lineage report");
        let verify = build_verify_lineage_report(&Config {
            root: Some(fixture.root.clone()),
            session_dir: None,
            json: true,
            mode: Mode::VerifyLineage,
        })
        .expect("verify-lineage report");
        let session = build_session_lineage_report(&Config {
            root: None,
            session_dir: Some(fixture.step_dir(TOP_ACCEPTED_PACKAGE_STEP).to_path_buf()),
            json: true,
            mode: Mode::SessionLineage,
        })
        .expect("session-lineage report");

        assert!(latest.planning_safe_only);
        assert!(latest.execution_untouched);
        assert!(!latest.activation_authorized);
        assert!(latest
            .not_authorized_summary
            .contains("does not authorize activation"));

        assert_eq!(
            verify.verdict,
            TinyLivePackageLineageVerdict::TinyLivePackageLineageVerifyOk
        );
        assert!(verify.planning_safe_only);
        assert!(verify.execution_untouched);
        assert!(!verify.activation_authorized);
        assert!(verify
            .not_authorized_summary
            .contains("does not authorize activation"));

        assert!(session.planning_safe_only);
        assert!(session.execution_untouched);
        assert!(!session.activation_authorized);
        assert!(session
            .not_authorized_summary
            .contains("does not authorize activation"));
    }

    #[derive(Debug)]
    struct LineageFixture {
        root: PathBuf,
        step_dirs: BTreeMap<String, PathBuf>,
    }

    impl LineageFixture {
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
                    "ready_for_manual_execution_when_gate_turns_green",
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
        result: &str,
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
        session.insert("result".to_string(), json!(result));
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
        status.insert("result".to_string(), json!(result));
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
                    "reason": "nested lineage verified"
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

    fn write_clerestory_verify_stub(bin_dir: &Path) {
        fs::create_dir_all(bin_dir).unwrap();
        let stub_path = bin_dir.join(CLERESTORY_VERIFY_BIN);
        let script = r#"#!/bin/sh
set -eu
printf '%s\n' '{"verdict":"tiny_live_package_clerestory_certificate_verify_ok","reason":"verify ok"}'
"#;
        fs::write(&stub_path, script).unwrap();
        let mut perms = fs::metadata(&stub_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&stub_path, perms).unwrap();
    }

    struct PathGuard {
        original_path: std::ffi::OsString,
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl PathGuard {
        fn install(bin_dir: &Path) -> Self {
            static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
            let lock = LOCK
                .get_or_init(|| Mutex::new(()))
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let original_path = env::var_os("PATH").unwrap_or_default();
            let mut parts = vec![bin_dir.as_os_str().to_os_string()];
            parts.extend(env::split_paths(&original_path).map(|path| path.into_os_string()));
            let joined = env::join_paths(parts.iter()).unwrap();
            env::set_var("PATH", &joined);
            Self {
                original_path,
                _lock: lock,
            }
        }
    }

    impl Drop for PathGuard {
        fn drop(&mut self) {
            env::set_var("PATH", &self.original_path);
        }
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
