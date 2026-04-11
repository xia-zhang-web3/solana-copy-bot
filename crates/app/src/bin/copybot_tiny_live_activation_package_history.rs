use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::env;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const USAGE: &str = "usage: copybot_tiny_live_activation_package_history --root <path> [--limit <count>] [--json] (--history | --latest | --verify-latest)";
const DEFAULT_HISTORY_LIMIT: usize = 10;
const TOP_ACCEPTED_PACKAGE_STEP: &str = "clerestory_certificate";
const PACKAGE_FILE_PREFIX: &str = "tiny_live_activation_package_";
const SESSION_FILE_SUFFIX: &str = ".session.json";
const STATUS_FILE_SUFFIX: &str = ".status.json";
const REPORT_FILE_SUFFIX: &str = ".report.json";
const CLERESTORY_VERIFY_BIN: &str = "copybot_tiny_live_activation_package_clerestory_certificate";
const NOT_AUTHORIZED_SUMMARY: &str =
    "This tiny-live package history surface only audits persisted package session/status/report artifacts. It stays planning-safe, does not authorize activation, and does not override the Stage 3 production gate.";

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
    root: PathBuf,
    limit: usize,
    json: bool,
    mode: Mode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    History,
    Latest,
    VerifyLatest,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageHistoryVerdict {
    TinyLivePackageHistoryAvailable,
    TinyLivePackageHistoryEmpty,
    TinyLivePackageLatestAvailable,
    TinyLivePackageLatestInvalid,
    TinyLivePackageLatestVerifyOk,
    TinyLivePackageLatestVerifyInvalid,
}

#[derive(Debug, Clone)]
struct PackageSessionSurface {
    package_step_name: String,
    step_rank: usize,
    session_dir: PathBuf,
    status_path: PathBuf,
    status_exists: bool,
    generated_at: Option<DateTime<Utc>>,
    file_modified_at: Option<DateTime<Utc>>,
    result: Option<String>,
    refusal_vs_ready_classification: String,
    current_pre_activation_gate_verdict: Option<String>,
    top_package_identity_summary: Option<String>,
    top_package_identity_sha256: Option<String>,
    primary_nested_step_name: Option<String>,
    primary_nested_report_path: Option<PathBuf>,
    primary_nested_session_dir: Option<String>,
    nested_package_identity_summary: Option<String>,
    chain_fingerprint_summary: Option<String>,
    verify_evidence_exists: bool,
    session_json: Option<Value>,
    status_json: Option<Value>,
    invalid_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct PackageSessionSummary {
    package_step_name: String,
    generated_at: Option<DateTime<Utc>>,
    session_path: String,
    status_path: Option<String>,
    result: Option<String>,
    refusal_vs_ready_classification: String,
    current_pre_activation_gate_verdict: Option<String>,
    top_package_identity_summary: Option<String>,
    top_package_identity_sha256: Option<String>,
    nested_package_step_name: Option<String>,
    nested_step_report_path: Option<String>,
    nested_package_session_dir: Option<String>,
    nested_package_identity_summary: Option<String>,
    chain_fingerprint_summary: Option<String>,
    verify_evidence_exists: bool,
    artifacts_complete: bool,
    reaches_current_top_accepted_layer: bool,
    invalid_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct PackageHistoryReport {
    mode: String,
    verdict: TinyLivePackageHistoryVerdict,
    reason: String,
    root: String,
    limit: usize,
    top_accepted_package_step: String,
    sessions_found: usize,
    invalid_session_count: usize,
    latest_session_path: Option<String>,
    sessions: Vec<PackageSessionSummary>,
    planning_safe_only: bool,
    execution_untouched: bool,
    activation_authorized: bool,
    not_authorized_summary: String,
}

#[derive(Debug, Clone, Serialize)]
struct PackageLatestReport {
    mode: String,
    verdict: TinyLivePackageHistoryVerdict,
    reason: String,
    root: String,
    top_accepted_package_step: String,
    latest_session: Option<PackageSessionSummary>,
    planning_safe_only: bool,
    execution_untouched: bool,
    activation_authorized: bool,
    not_authorized_summary: String,
}

#[derive(Debug, Clone, Serialize)]
struct PackageVerifyLatestReport {
    mode: String,
    verdict: TinyLivePackageHistoryVerdict,
    reason: String,
    root: String,
    top_accepted_package_step: String,
    latest_session: Option<PackageSessionSummary>,
    verification_attempted: bool,
    verification_command: Option<String>,
    verification_command_verdict: Option<String>,
    verification_command_reason: Option<String>,
    invalid_reasons: Vec<String>,
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
    let mut limit = DEFAULT_HISTORY_LIMIT;
    let mut json = false;
    let mut mode: Option<Mode> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--root" => {
                root = Some(PathBuf::from(parse_required_value("--root", args.next())?));
            }
            "--limit" => {
                limit = parse_usize_arg("--limit", args.next())?;
            }
            "--history" => set_mode(&mut mode, Mode::History, "--history")?,
            "--latest" => set_mode(&mut mode, Mode::Latest, "--latest")?,
            "--verify-latest" => set_mode(&mut mode, Mode::VerifyLatest, "--verify-latest")?,
            "--json" => json = true,
            "--help" | "-h" => return Ok(None),
            other => bail!("unrecognized argument: {other}"),
        }
    }

    let Some(mode) = mode else {
        bail!("exactly one of --history, --latest, or --verify-latest is required");
    };

    Ok(Some(Config {
        root: root.ok_or_else(|| anyhow!("missing required --root"))?,
        limit: limit.max(1),
        json,
        mode,
    }))
}

fn run(config: Config) -> Result<String> {
    match config.mode {
        Mode::History => {
            let report = build_history_report(&config)?;
            if config.json {
                serde_json::to_string_pretty(&report)
                    .context("failed serializing tiny-live package history report json")
            } else {
                Ok(render_history_human(&report))
            }
        }
        Mode::Latest => {
            let report = build_latest_report(&config)?;
            if config.json {
                serde_json::to_string_pretty(&report)
                    .context("failed serializing tiny-live package latest report json")
            } else {
                Ok(render_latest_human(&report))
            }
        }
        Mode::VerifyLatest => {
            let report = build_verify_latest_report(&config)?;
            if config.json {
                serde_json::to_string_pretty(&report)
                    .context("failed serializing tiny-live package verify-latest report json")
            } else {
                Ok(render_verify_latest_human(&report))
            }
        }
    }
}

fn build_history_report(config: &Config) -> Result<PackageHistoryReport> {
    let sessions = scan_package_sessions(&config.root)?;
    if sessions.is_empty() {
        return Ok(PackageHistoryReport {
            mode: "history".to_string(),
            verdict: TinyLivePackageHistoryVerdict::TinyLivePackageHistoryEmpty,
            reason: format!(
                "no persisted immutable tiny-live package sessions were found under {}",
                config.root.display()
            ),
            root: config.root.display().to_string(),
            limit: config.limit,
            top_accepted_package_step: TOP_ACCEPTED_PACKAGE_STEP.to_string(),
            sessions_found: 0,
            invalid_session_count: 0,
            latest_session_path: None,
            sessions: Vec::new(),
            planning_safe_only: true,
            execution_untouched: true,
            activation_authorized: false,
            not_authorized_summary: NOT_AUTHORIZED_SUMMARY.to_string(),
        });
    }

    let invalid_session_count = sessions
        .iter()
        .filter(|surface| !surface.invalid_reasons.is_empty())
        .count();
    let latest_session_path = sessions
        .first()
        .map(|surface| surface.session_dir.display().to_string());
    let recent = sessions
        .iter()
        .take(config.limit)
        .map(summarize_session)
        .collect::<Vec<_>>();

    Ok(PackageHistoryReport {
        mode: "history".to_string(),
        verdict: TinyLivePackageHistoryVerdict::TinyLivePackageHistoryAvailable,
        reason: format!(
            "found {} persisted immutable tiny-live package session(s) under {}; returning the {} most recent session(s)",
            sessions.len(),
            config.root.display(),
            recent.len()
        ),
        root: config.root.display().to_string(),
        limit: config.limit,
        top_accepted_package_step: TOP_ACCEPTED_PACKAGE_STEP.to_string(),
        sessions_found: sessions.len(),
        invalid_session_count,
        latest_session_path,
        sessions: recent,
        planning_safe_only: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary: NOT_AUTHORIZED_SUMMARY.to_string(),
    })
}

fn build_latest_report(config: &Config) -> Result<PackageLatestReport> {
    let sessions = scan_package_sessions(&config.root)?;
    let Some(latest) = sessions.first() else {
        return Ok(PackageLatestReport {
            mode: "latest".to_string(),
            verdict: TinyLivePackageHistoryVerdict::TinyLivePackageHistoryEmpty,
            reason: format!(
                "no persisted immutable tiny-live package sessions were found under {}",
                config.root.display()
            ),
            root: config.root.display().to_string(),
            top_accepted_package_step: TOP_ACCEPTED_PACKAGE_STEP.to_string(),
            latest_session: None,
            planning_safe_only: true,
            execution_untouched: true,
            activation_authorized: false,
            not_authorized_summary: NOT_AUTHORIZED_SUMMARY.to_string(),
        });
    };

    let summary = summarize_session(latest);
    let mut invalid_reasons = latest.invalid_reasons.clone();
    if latest.package_step_name != TOP_ACCEPTED_PACKAGE_STEP {
        invalid_reasons.push(format!(
            "latest persisted immutable package step {} is below the current top accepted layer {}",
            latest.package_step_name, TOP_ACCEPTED_PACKAGE_STEP
        ));
    }

    let (verdict, reason) = if invalid_reasons.is_empty() {
        (
            TinyLivePackageHistoryVerdict::TinyLivePackageLatestAvailable,
            format!(
                "latest immutable tiny-live package session is {} at {} with classification {}",
                latest.package_step_name,
                latest.session_dir.display(),
                latest.refusal_vs_ready_classification
            ),
        )
    } else {
        (
            TinyLivePackageHistoryVerdict::TinyLivePackageLatestInvalid,
            invalid_reasons.join("; "),
        )
    };

    Ok(PackageLatestReport {
        mode: "latest".to_string(),
        verdict,
        reason,
        root: config.root.display().to_string(),
        top_accepted_package_step: TOP_ACCEPTED_PACKAGE_STEP.to_string(),
        latest_session: Some(summary),
        planning_safe_only: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary: NOT_AUTHORIZED_SUMMARY.to_string(),
    })
}

fn build_verify_latest_report(config: &Config) -> Result<PackageVerifyLatestReport> {
    let sessions = scan_package_sessions(&config.root)?;
    let Some(latest) = sessions.first() else {
        return Ok(PackageVerifyLatestReport {
            mode: "verify_latest".to_string(),
            verdict: TinyLivePackageHistoryVerdict::TinyLivePackageHistoryEmpty,
            reason: format!(
                "no persisted immutable tiny-live package sessions were found under {}",
                config.root.display()
            ),
            root: config.root.display().to_string(),
            top_accepted_package_step: TOP_ACCEPTED_PACKAGE_STEP.to_string(),
            latest_session: None,
            verification_attempted: false,
            verification_command: None,
            verification_command_verdict: None,
            verification_command_reason: None,
            invalid_reasons: Vec::new(),
            planning_safe_only: true,
            execution_untouched: true,
            activation_authorized: false,
            not_authorized_summary: NOT_AUTHORIZED_SUMMARY.to_string(),
        });
    };

    let latest_summary = summarize_session(latest);
    let mut invalid_reasons = latest.invalid_reasons.clone();

    if latest.package_step_name != TOP_ACCEPTED_PACKAGE_STEP {
        invalid_reasons.push(format!(
            "latest persisted immutable package step {} is below the current top accepted layer {}",
            latest.package_step_name, TOP_ACCEPTED_PACKAGE_STEP
        ));
        return Ok(invalid_verify_report(
            config,
            Some(latest_summary),
            false,
            None,
            None,
            None,
            invalid_reasons,
        ));
    }

    let choir_receipt_session_dir = extract_string(
        latest.status_json.as_ref(),
        latest.session_json.as_ref(),
        "choir_receipt_session_dir",
    );
    if choir_receipt_session_dir.is_none() {
        invalid_reasons.push(
            "latest clerestory-certificate session is missing choir_receipt_session_dir"
                .to_string(),
        );
    }
    let decision_packet_session_dir = extract_string(
        latest.status_json.as_ref(),
        latest.session_json.as_ref(),
        "decision_packet_session_dir",
    );
    if decision_packet_session_dir.is_none() {
        invalid_reasons.push(
            "latest clerestory-certificate session is missing decision_packet_session_dir"
                .to_string(),
        );
    }
    if !latest.verify_evidence_exists {
        invalid_reasons.push(
            "latest clerestory-certificate session is missing required archived nested choir-receipt verify evidence".to_string(),
        );
    }

    if !invalid_reasons.is_empty() {
        return Ok(invalid_verify_report(
            config,
            Some(latest_summary),
            false,
            None,
            None,
            None,
            invalid_reasons,
        ));
    }

    let choir_receipt_session_dir = choir_receipt_session_dir.expect("checked choir session dir");
    let decision_packet_session_dir =
        decision_packet_session_dir.expect("checked decision packet session dir");
    let args = vec![
        "--choir-receipt-session-dir".to_string(),
        choir_receipt_session_dir.clone(),
        "--confirm-decision-packet-session-dir".to_string(),
        decision_packet_session_dir.clone(),
        "--session-dir".to_string(),
        latest.session_dir.display().to_string(),
        "--verify-live-package-clerestory-certificate".to_string(),
        "--json".to_string(),
    ];
    let command_preview = render_command_preview(CLERESTORY_VERIFY_BIN, &args);

    match run_json_command(CLERESTORY_VERIFY_BIN, &args) {
        Ok(raw) => {
            let verify: ExternalVerifyReport = serde_json::from_value(raw).with_context(|| {
                format!(
                    "failed parsing JSON verify output from {} for latest session {}",
                    CLERESTORY_VERIFY_BIN,
                    latest.session_dir.display()
                )
            })?;
            if verify.verdict == "tiny_live_package_clerestory_certificate_verify_ok" {
                Ok(PackageVerifyLatestReport {
                    mode: "verify_latest".to_string(),
                    verdict: TinyLivePackageHistoryVerdict::TinyLivePackageLatestVerifyOk,
                    reason: format!(
                        "latest immutable tiny-live package chain verified cleanly at top accepted layer {}",
                        TOP_ACCEPTED_PACKAGE_STEP
                    ),
                    root: config.root.display().to_string(),
                    top_accepted_package_step: TOP_ACCEPTED_PACKAGE_STEP.to_string(),
                    latest_session: Some(latest_summary),
                    verification_attempted: true,
                    verification_command: Some(command_preview),
                    verification_command_verdict: Some(verify.verdict),
                    verification_command_reason: Some(verify.reason),
                    invalid_reasons: Vec::new(),
                    planning_safe_only: true,
                    execution_untouched: true,
                    activation_authorized: false,
                    not_authorized_summary: NOT_AUTHORIZED_SUMMARY.to_string(),
                })
            } else {
                invalid_reasons.push(format!(
                    "latest top-layer verification returned {}: {}",
                    verify.verdict, verify.reason
                ));
                Ok(invalid_verify_report(
                    config,
                    Some(latest_summary),
                    true,
                    Some(command_preview),
                    Some(verify.verdict),
                    Some(verify.reason),
                    invalid_reasons,
                ))
            }
        }
        Err(error) => {
            invalid_reasons.push(format!(
                "failed running {} for latest session {}: {error}",
                CLERESTORY_VERIFY_BIN,
                latest.session_dir.display()
            ));
            Ok(invalid_verify_report(
                config,
                Some(latest_summary),
                true,
                Some(command_preview),
                None,
                None,
                invalid_reasons,
            ))
        }
    }
}

fn invalid_verify_report(
    config: &Config,
    latest_session: Option<PackageSessionSummary>,
    verification_attempted: bool,
    verification_command: Option<String>,
    verification_command_verdict: Option<String>,
    verification_command_reason: Option<String>,
    invalid_reasons: Vec<String>,
) -> PackageVerifyLatestReport {
    PackageVerifyLatestReport {
        mode: "verify_latest".to_string(),
        verdict: TinyLivePackageHistoryVerdict::TinyLivePackageLatestVerifyInvalid,
        reason: if invalid_reasons.is_empty() {
            "latest immutable tiny-live package chain verification failed closed".to_string()
        } else {
            invalid_reasons.join("; ")
        },
        root: config.root.display().to_string(),
        top_accepted_package_step: TOP_ACCEPTED_PACKAGE_STEP.to_string(),
        latest_session,
        verification_attempted,
        verification_command,
        verification_command_verdict,
        verification_command_reason,
        invalid_reasons,
        planning_safe_only: true,
        execution_untouched: true,
        activation_authorized: false,
        not_authorized_summary: NOT_AUTHORIZED_SUMMARY.to_string(),
    }
}

fn scan_package_sessions(root: &Path) -> Result<Vec<PackageSessionSurface>> {
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
    sessions.sort_by(|left, right| {
        right
            .ordering_time()
            .cmp(&left.ordering_time())
            .then_with(|| right.step_rank.cmp(&left.step_rank))
            .then_with(|| left.session_dir.cmp(&right.session_dir))
    });
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

fn inspect_session_path(session_path: &Path) -> Result<Option<PackageSessionSurface>> {
    let Some(file_name) = session_path.file_name().and_then(OsStr::to_str) else {
        return Ok(None);
    };
    let Some(package_step_name) = session_step_name_from_session_file(file_name) else {
        return Ok(None);
    };
    let step_rank = step_rank(&package_step_name);
    if step_rank == 0 {
        return Ok(None);
    }

    let session_dir = session_path
        .parent()
        .ok_or_else(|| {
            anyhow!(
                "session path {} has no parent directory",
                session_path.display()
            )
        })?
        .to_path_buf();
    let status_path = session_dir.join(format!(
        "{PACKAGE_FILE_PREFIX}{package_step_name}{STATUS_FILE_SUFFIX}"
    ));
    let status_exists = status_path.exists();
    let session_json = load_json_value(session_path).ok();
    let status_json = if status_exists {
        load_json_value(&status_path).ok()
    } else {
        None
    };

    let mut invalid_reasons = Vec::new();
    if session_json.is_none() {
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

    let nested_report = primary_nested_report(&session_dir, &package_step_name)?;
    let generated_at = extract_datetime(status_json.as_ref(), "updated_at")
        .or_else(|| extract_datetime(session_json.as_ref(), "planned_at"));
    let result = extract_string(status_json.as_ref(), session_json.as_ref(), "result");
    if result.is_none() {
        invalid_reasons.push(format!(
            "missing result field in persisted status for {}",
            session_dir.display()
        ));
    }

    let top_summary_key = format!("{}_summary", package_step_name);
    let top_sha_key = format!("{}_sha256", package_step_name);
    let top_package_identity_summary = extract_string(
        status_json.as_ref(),
        session_json.as_ref(),
        &top_summary_key,
    );
    if top_package_identity_summary.is_none() {
        invalid_reasons.push(format!(
            "missing {} in persisted session/status for {}",
            top_summary_key,
            session_dir.display()
        ));
    }
    let top_package_identity_sha256 =
        extract_string(status_json.as_ref(), session_json.as_ref(), &top_sha_key);
    let primary_nested_step_name = nested_report
        .as_ref()
        .and_then(|report| report.nested_step_name.clone());
    let primary_nested_session_dir = primary_nested_step_name.as_ref().and_then(|step| {
        let key = format!("{step}_session_dir");
        extract_string(status_json.as_ref(), session_json.as_ref(), &key)
    });
    let nested_package_identity_summary = primary_nested_step_name.as_ref().and_then(|step| {
        let key = format!("{step}_summary");
        extract_string(status_json.as_ref(), session_json.as_ref(), &key)
    });
    let verify_evidence_exists = nested_report.is_some();
    let chain_fingerprint_summary = extract_string(
        status_json.as_ref(),
        session_json.as_ref(),
        "chain_fingerprint_summary",
    );
    let current_pre_activation_gate_verdict = extract_string(
        status_json.as_ref(),
        session_json.as_ref(),
        "current_pre_activation_gate_verdict",
    );
    let refusal_vs_ready_classification =
        classify_refusal_vs_ready(result.as_deref(), &invalid_reasons);

    Ok(Some(PackageSessionSurface {
        package_step_name,
        step_rank,
        session_dir,
        status_path,
        status_exists,
        generated_at,
        file_modified_at: file_modified_at(session_path),
        result,
        refusal_vs_ready_classification,
        current_pre_activation_gate_verdict,
        top_package_identity_summary,
        top_package_identity_sha256,
        primary_nested_step_name,
        primary_nested_report_path: nested_report.as_ref().map(|report| report.path.clone()),
        primary_nested_session_dir,
        nested_package_identity_summary,
        chain_fingerprint_summary,
        verify_evidence_exists,
        session_json,
        status_json,
        invalid_reasons,
    }))
}

fn summarize_session(surface: &PackageSessionSurface) -> PackageSessionSummary {
    PackageSessionSummary {
        package_step_name: surface.package_step_name.clone(),
        generated_at: surface.ordering_time(),
        session_path: surface.session_dir.display().to_string(),
        status_path: if surface.status_exists {
            Some(surface.status_path.display().to_string())
        } else {
            None
        },
        result: surface.result.clone(),
        refusal_vs_ready_classification: surface.refusal_vs_ready_classification.clone(),
        current_pre_activation_gate_verdict: surface.current_pre_activation_gate_verdict.clone(),
        top_package_identity_summary: surface.top_package_identity_summary.clone(),
        top_package_identity_sha256: surface.top_package_identity_sha256.clone(),
        nested_package_step_name: surface.primary_nested_step_name.clone(),
        nested_step_report_path: surface
            .primary_nested_report_path
            .as_ref()
            .map(|path| path.display().to_string()),
        nested_package_session_dir: surface.primary_nested_session_dir.clone(),
        nested_package_identity_summary: surface.nested_package_identity_summary.clone(),
        chain_fingerprint_summary: surface.chain_fingerprint_summary.clone(),
        verify_evidence_exists: surface.verify_evidence_exists,
        artifacts_complete: surface.invalid_reasons.is_empty(),
        reaches_current_top_accepted_layer: surface.package_step_name == TOP_ACCEPTED_PACKAGE_STEP,
        invalid_reasons: surface.invalid_reasons.clone(),
    }
}

#[derive(Debug, Clone)]
struct NestedReportSurface {
    path: PathBuf,
    nested_step_name: Option<String>,
}

fn primary_nested_report(
    session_dir: &Path,
    current_step_name: &str,
) -> Result<Option<NestedReportSurface>> {
    let mut preferred: Option<(usize, NestedReportSurface)> = None;
    let mut fallback: Option<NestedReportSurface> = None;
    let current_rank = step_rank(current_step_name);
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
        let Some(name) = path.file_name().and_then(OsStr::to_str) else {
            continue;
        };
        let Some(nested_step_name) = nested_step_name_from_report_file(current_step_name, name)
        else {
            continue;
        };

        let surface = NestedReportSurface {
            path: path.clone(),
            nested_step_name: Some(nested_step_name.clone()),
        };

        let nested_rank = step_rank(&nested_step_name);
        if nested_rank > 0 && nested_rank < current_rank {
            match preferred {
                Some((best_rank, _)) if best_rank >= nested_rank => {}
                _ => preferred = Some((nested_rank, surface)),
            }
        } else if fallback.is_none() {
            fallback = Some(surface);
        }
    }

    Ok(preferred.map(|(_, surface)| surface).or(fallback))
}

fn nested_step_name_from_report_file(current_step_name: &str, file_name: &str) -> Option<String> {
    let prefix = format!("{PACKAGE_FILE_PREFIX}{current_step_name}.");
    let trimmed = file_name.strip_prefix(&prefix)?;
    let nested = trimmed.strip_suffix(REPORT_FILE_SUFFIX)?;
    if nested.is_empty() {
        None
    } else {
        Some(nested.to_string())
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

impl PackageSessionSurface {
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

fn classify_refusal_vs_ready(result: Option<&str>, invalid_reasons: &[String]) -> String {
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

fn parse_usize_arg(flag: &str, value: Option<String>) -> Result<usize> {
    let raw = parse_required_value(flag, value)?;
    raw.parse::<usize>()
        .with_context(|| format!("invalid integer value for {flag}: {raw}"))
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

fn render_history_human(report: &PackageHistoryReport) -> String {
    let mut lines = vec![
        "event=copybot_tiny_live_activation_package_history".to_string(),
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("root={}", report.root),
        format!(
            "top_accepted_package_step={}",
            report.top_accepted_package_step
        ),
        format!("sessions_found={}", report.sessions_found),
        format!("invalid_session_count={}", report.invalid_session_count),
    ];
    if let Some(path) = &report.latest_session_path {
        lines.push(format!("latest_session_path={path}"));
    }
    for (index, session) in report.sessions.iter().enumerate() {
        lines.push(format!(
            "session[{index}].package_step_name={}",
            session.package_step_name
        ));
        lines.push(format!(
            "session[{index}].generated_at={}",
            session
                .generated_at
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "<unknown>".to_string())
        ));
        lines.push(format!(
            "session[{index}].session_path={}",
            session.session_path
        ));
        lines.push(format!(
            "session[{index}].result={}",
            session.result.as_deref().unwrap_or("<missing>")
        ));
        lines.push(format!(
            "session[{index}].verify_evidence_exists={}",
            session.verify_evidence_exists
        ));
    }
    lines.push("planning_safe_only=true".to_string());
    lines.push("activation_authorized=false".to_string());
    lines.push(format!(
        "not_authorized_summary={}",
        report.not_authorized_summary
    ));
    lines.join("\n")
}

fn render_latest_human(report: &PackageLatestReport) -> String {
    let mut lines = vec![
        "event=copybot_tiny_live_activation_package_history".to_string(),
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("root={}", report.root),
        format!(
            "top_accepted_package_step={}",
            report.top_accepted_package_step
        ),
    ];
    if let Some(session) = &report.latest_session {
        lines.push(format!(
            "latest.package_step_name={}",
            session.package_step_name
        ));
        lines.push(format!("latest.session_path={}", session.session_path));
        lines.push(format!(
            "latest.result={}",
            session.result.as_deref().unwrap_or("<missing>")
        ));
        lines.push(format!(
            "latest.refusal_vs_ready_classification={}",
            session.refusal_vs_ready_classification
        ));
        if let Some(path) = &session.nested_step_report_path {
            lines.push(format!("latest.nested_step_report_path={path}"));
        }
        if let Some(summary) = &session.chain_fingerprint_summary {
            lines.push(format!("latest.chain_fingerprint_summary={summary}"));
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

fn render_verify_latest_human(report: &PackageVerifyLatestReport) -> String {
    let mut lines = vec![
        "event=copybot_tiny_live_activation_package_history".to_string(),
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("root={}", report.root),
        format!(
            "top_accepted_package_step={}",
            report.top_accepted_package_step
        ),
        format!("verification_attempted={}", report.verification_attempted),
    ];
    if let Some(command) = &report.verification_command {
        lines.push(format!("verification_command={command}"));
    }
    if let Some(verdict) = &report.verification_command_verdict {
        lines.push(format!("verification_command_verdict={verdict}"));
    }
    if let Some(reason) = &report.verification_command_reason {
        lines.push(format!("verification_command_reason={reason}"));
    }
    if let Some(session) = &report.latest_session {
        lines.push(format!(
            "latest.package_step_name={}",
            session.package_step_name
        ));
        lines.push(format!("latest.session_path={}", session.session_path));
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
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{Mutex, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn history_mode_returns_bounded_ordered_sessions() {
        let root = temp_dir("tiny_live_package_history_ordered");
        write_package_session(
            &root.join("2026-04-01-choir"),
            "choir_receipt",
            "2026-04-01T10:00:00Z",
            "2026-04-01T10:01:00Z",
            "ready_for_manual_execution_when_gate_turns_green",
            Some("choir identity"),
            Some("choir-sha"),
            Some("transept_certificate"),
            Some("transept identity"),
            Some("chain one"),
            true,
        );
        write_package_session(
            &root.join("2026-04-02-clerestory"),
            "clerestory_certificate",
            "2026-04-02T10:00:00Z",
            "2026-04-02T10:01:00Z",
            "ready_for_manual_execution_when_gate_turns_green",
            Some("clerestory identity"),
            Some("clerestory-sha"),
            Some("choir_receipt"),
            Some("choir identity"),
            Some("chain two"),
            true,
        );
        write_package_session(
            &root.join("2026-04-03-choir"),
            "choir_receipt",
            "2026-04-03T10:00:00Z",
            "2026-04-03T10:02:00Z",
            "refused_now_by_stage3",
            Some("choir latest"),
            Some("choir-latest-sha"),
            Some("transept_certificate"),
            Some("transept latest"),
            Some("chain three"),
            true,
        );

        let report = build_history_report(&Config {
            root: root.clone(),
            limit: 2,
            json: true,
            mode: Mode::History,
        })
        .expect("history report");

        assert_eq!(
            report.verdict,
            TinyLivePackageHistoryVerdict::TinyLivePackageHistoryAvailable
        );
        assert_eq!(report.sessions.len(), 2);
        assert_eq!(report.sessions[0].package_step_name, "choir_receipt");
        assert_eq!(
            report.sessions[0].session_path,
            root.join("2026-04-03-choir").display().to_string()
        );
        assert_eq!(
            report.sessions[1].package_step_name,
            "clerestory_certificate"
        );
        assert!(report.sessions[0].verify_evidence_exists);
    }

    #[test]
    fn latest_mode_returns_newest_valid_package_session() {
        let root = temp_dir("tiny_live_package_history_latest");
        write_package_session(
            &root.join("older-choir"),
            "choir_receipt",
            "2026-04-01T10:00:00Z",
            "2026-04-01T10:01:00Z",
            "ready_for_manual_execution_when_gate_turns_green",
            Some("older choir"),
            Some("older-sha"),
            Some("transept_certificate"),
            Some("older transept"),
            Some("older chain"),
            true,
        );
        write_package_session(
            &root.join("newer-clerestory"),
            "clerestory_certificate",
            "2026-04-02T10:00:00Z",
            "2026-04-02T10:05:00Z",
            "ready_for_manual_execution_when_gate_turns_green",
            Some("newer clerestory"),
            Some("newer-sha"),
            Some("choir_receipt"),
            Some("newer choir"),
            Some("newer chain"),
            true,
        );

        let report = build_latest_report(&Config {
            root,
            limit: DEFAULT_HISTORY_LIMIT,
            json: true,
            mode: Mode::Latest,
        })
        .expect("latest report");

        assert_eq!(
            report.verdict,
            TinyLivePackageHistoryVerdict::TinyLivePackageLatestAvailable
        );
        let latest = report.latest_session.expect("latest session");
        assert_eq!(latest.package_step_name, "clerestory_certificate");
        assert_eq!(
            latest.refusal_vs_ready_classification,
            "ready_for_manual_execution_when_gate_turns_green"
        );
        assert!(latest.reaches_current_top_accepted_layer);
    }

    #[test]
    fn verify_latest_fails_when_nested_chain_continuity_is_broken() {
        let root = temp_dir("tiny_live_package_history_broken_chain");
        let session_dir = root.join("clerestory-session");
        write_package_session(
            &session_dir,
            "clerestory_certificate",
            "2026-04-03T10:00:00Z",
            "2026-04-03T10:01:00Z",
            "ready_for_manual_execution_when_gate_turns_green",
            Some("clerestory identity"),
            Some("clerestory-sha"),
            Some("choir_receipt"),
            Some("choir identity"),
            Some("chain summary"),
            true,
        );
        fs::write(
            session_dir.join(
                "tiny_live_activation_package_clerestory_certificate.choir_receipt.report.json",
            ),
            serde_json::to_vec_pretty(&json!({
                "marker": "broken-chain"
            }))
            .unwrap(),
        )
        .unwrap();

        let bin_dir = temp_dir("tiny_live_package_history_stub_bin_broken");
        write_clerestory_verify_stub(&bin_dir);
        let _path_guard = PathGuard::install(&bin_dir);

        let report = build_verify_latest_report(&Config {
            root,
            limit: DEFAULT_HISTORY_LIMIT,
            json: true,
            mode: Mode::VerifyLatest,
        })
        .expect("verify latest report");

        assert_eq!(
            report.verdict,
            TinyLivePackageHistoryVerdict::TinyLivePackageLatestVerifyInvalid
        );
        assert!(report.verification_attempted);
        assert_eq!(
            report.verification_command_verdict.as_deref(),
            Some("tiny_live_package_clerestory_certificate_verify_invalid")
        );
    }

    #[test]
    fn verify_latest_fails_when_top_level_status_is_tampered() {
        let root = temp_dir("tiny_live_package_history_tampered_status");
        let session_dir = root.join("clerestory-session");
        write_package_session(
            &session_dir,
            "clerestory_certificate",
            "2026-04-03T10:00:00Z",
            "2026-04-03T10:01:00Z",
            "ready_for_manual_execution_when_gate_turns_green",
            Some("tampered clerestory certificate summary"),
            Some("clerestory-sha"),
            Some("choir_receipt"),
            Some("choir identity"),
            Some("chain summary"),
            true,
        );

        let bin_dir = temp_dir("tiny_live_package_history_stub_bin_tampered");
        write_clerestory_verify_stub(&bin_dir);
        let _path_guard = PathGuard::install(&bin_dir);

        let report = build_verify_latest_report(&Config {
            root,
            limit: DEFAULT_HISTORY_LIMIT,
            json: true,
            mode: Mode::VerifyLatest,
        })
        .expect("verify latest report");

        assert_eq!(
            report.verdict,
            TinyLivePackageHistoryVerdict::TinyLivePackageLatestVerifyInvalid
        );
        assert!(report
            .reason
            .contains("tiny_live_package_clerestory_certificate_verify_invalid"));
    }

    #[test]
    fn verify_latest_fails_when_required_nested_verified_truth_is_missing() {
        let root = temp_dir("tiny_live_package_history_missing_nested_truth");
        let session_dir = root.join("clerestory-session");
        write_package_session(
            &session_dir,
            "clerestory_certificate",
            "2026-04-03T10:00:00Z",
            "2026-04-03T10:01:00Z",
            "ready_for_manual_execution_when_gate_turns_green",
            Some("clerestory identity"),
            Some("clerestory-sha"),
            Some("choir_receipt"),
            Some("choir identity"),
            Some("chain summary"),
            false,
        );

        let report = build_verify_latest_report(&Config {
            root,
            limit: DEFAULT_HISTORY_LIMIT,
            json: true,
            mode: Mode::VerifyLatest,
        })
        .expect("verify latest report");

        assert_eq!(
            report.verdict,
            TinyLivePackageHistoryVerdict::TinyLivePackageLatestVerifyInvalid
        );
        assert!(!report.verification_attempted);
        assert!(report
            .reason
            .contains("missing required archived nested choir-receipt verify evidence"));
    }

    #[test]
    fn verify_latest_stays_planning_safe_and_does_not_authorize_activation() {
        let root = temp_dir("tiny_live_package_history_planning_safe");
        let session_dir = root.join("clerestory-session");
        write_package_session(
            &session_dir,
            "clerestory_certificate",
            "2026-04-03T10:00:00Z",
            "2026-04-03T10:01:00Z",
            "ready_for_manual_execution_when_gate_turns_green",
            Some("clerestory identity"),
            Some("clerestory-sha"),
            Some("choir_receipt"),
            Some("choir identity"),
            Some("chain summary"),
            true,
        );

        let bin_dir = temp_dir("tiny_live_package_history_stub_bin_ok");
        write_clerestory_verify_stub(&bin_dir);
        let _path_guard = PathGuard::install(&bin_dir);

        let report = build_verify_latest_report(&Config {
            root,
            limit: DEFAULT_HISTORY_LIMIT,
            json: true,
            mode: Mode::VerifyLatest,
        })
        .expect("verify latest report");

        assert_eq!(
            report.verdict,
            TinyLivePackageHistoryVerdict::TinyLivePackageLatestVerifyOk
        );
        assert!(report.planning_safe_only);
        assert!(report.execution_untouched);
        assert!(!report.activation_authorized);
        assert!(report
            .not_authorized_summary
            .contains("does not authorize activation"));
    }

    fn write_package_session(
        session_dir: &Path,
        step_name: &str,
        planned_at: &str,
        updated_at: &str,
        result: &str,
        top_summary: Option<&str>,
        top_sha: Option<&str>,
        nested_step: Option<&str>,
        nested_summary: Option<&str>,
        chain_summary: Option<&str>,
        write_nested_report: bool,
    ) {
        fs::create_dir_all(session_dir).unwrap();
        let session_file = session_dir.join(format!(
            "{PACKAGE_FILE_PREFIX}{step_name}{SESSION_FILE_SUFFIX}"
        ));
        let status_file = session_dir.join(format!(
            "{PACKAGE_FILE_PREFIX}{step_name}{STATUS_FILE_SUFFIX}"
        ));
        let mut session = serde_json::Map::new();
        session.insert("planned_at".to_string(), json!(planned_at));
        session.insert(
            "session_dir".to_string(),
            json!(session_dir.display().to_string()),
        );
        session.insert(
            "decision_packet_session_dir".to_string(),
            json!(session_dir.join("decision-packet").display().to_string()),
        );
        if let Some(summary) = top_summary {
            session.insert(format!("{step_name}_summary"), json!(summary));
        }
        if let Some(sha) = top_sha {
            session.insert(format!("{step_name}_sha256"), json!(sha));
        }
        if let Some(summary) = chain_summary {
            session.insert("chain_fingerprint_summary".to_string(), json!(summary));
        }
        session.insert(
            "current_pre_activation_gate_verdict".to_string(),
            json!("green"),
        );
        if let Some(nested_step) = nested_step {
            session.insert(
                format!("{nested_step}_session_dir"),
                json!(session_dir
                    .join(format!("{nested_step}-session"))
                    .display()
                    .to_string()),
            );
            if let Some(summary) = nested_summary {
                session.insert(format!("{nested_step}_summary"), json!(summary));
            }
        }
        fs::write(
            session_file,
            serde_json::to_vec_pretty(&Value::Object(session)).unwrap(),
        )
        .unwrap();

        let mut status = serde_json::Map::new();
        status.insert("updated_at".to_string(), json!(updated_at));
        status.insert(
            "session_dir".to_string(),
            json!(session_dir.display().to_string()),
        );
        status.insert("result".to_string(), json!(result));
        status.insert(
            "decision_packet_session_dir".to_string(),
            json!(session_dir.join("decision-packet").display().to_string()),
        );
        if let Some(summary) = top_summary {
            status.insert(format!("{step_name}_summary"), json!(summary));
        }
        if let Some(sha) = top_sha {
            status.insert(format!("{step_name}_sha256"), json!(sha));
        }
        if let Some(summary) = chain_summary {
            status.insert("chain_fingerprint_summary".to_string(), json!(summary));
        }
        status.insert(
            "current_pre_activation_gate_verdict".to_string(),
            json!("green"),
        );
        if let Some(nested_step) = nested_step {
            status.insert(
                format!("{nested_step}_session_dir"),
                json!(session_dir
                    .join(format!("{nested_step}-session"))
                    .display()
                    .to_string()),
            );
            if let Some(summary) = nested_summary {
                status.insert(format!("{nested_step}_summary"), json!(summary));
            }
        }
        fs::write(
            status_file,
            serde_json::to_vec_pretty(&Value::Object(status)).unwrap(),
        )
        .unwrap();

        if write_nested_report {
            let nested_step = nested_step.unwrap_or("nested_step");
            let report_file = session_dir.join(format!(
                "{PACKAGE_FILE_PREFIX}{step_name}.{nested_step}{REPORT_FILE_SUFFIX}"
            ));
            fs::write(
                report_file,
                serde_json::to_vec_pretty(&json!({
                    "verdict": "tiny_live_package_nested_verify_ok",
                    "reason": "nested verify ok"
                }))
                .unwrap(),
            )
            .unwrap();
        }
    }

    fn write_clerestory_verify_stub(bin_dir: &Path) {
        fs::create_dir_all(bin_dir).unwrap();
        let stub_path = bin_dir.join(CLERESTORY_VERIFY_BIN);
        let script = r#"#!/bin/sh
set -eu
session_dir=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --session-dir)
      session_dir="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
status_path="$session_dir/tiny_live_activation_package_clerestory_certificate.status.json"
report_path="$session_dir/tiny_live_activation_package_clerestory_certificate.choir_receipt.report.json"
if [ ! -f "$report_path" ]; then
  printf '%s\n' '{"verdict":"tiny_live_package_clerestory_certificate_verify_invalid","reason":"missing nested verified choir receipt report"}'
  exit 0
fi
if grep -F 'broken-chain' "$report_path" >/dev/null 2>&1; then
  printf '%s\n' '{"verdict":"tiny_live_package_clerestory_certificate_verify_invalid","reason":"nested chain continuity broken"}'
  exit 0
fi
if grep -F 'tampered clerestory certificate summary' "$status_path" >/dev/null 2>&1; then
  printf '%s\n' '{"verdict":"tiny_live_package_clerestory_certificate_verify_invalid","reason":"top-level clerestory status tampered"}'
  exit 0
fi
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
