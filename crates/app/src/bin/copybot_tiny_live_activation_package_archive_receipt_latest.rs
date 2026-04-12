#![recursion_limit = "256"]

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const USAGE: &str = "usage: copybot_tiny_live_activation_package_archive_receipt_latest --root <path> [--session-dir <path>] [--output <path>] [--json] (--plan-latest-archive-receipt | --render-latest-archive-receipt-script | --run-latest-archive-receipt | --verify-latest-archive-receipt)";
const ARCHIVE_RECEIPT_LATEST_SESSION_VERSION: &str = "1";
const ARCHIVE_RECEIPT_LATEST_STATUS_VERSION: &str = "1";
const FILING_CERTIFICATE_LATEST_BIN: &str =
    "copybot_tiny_live_activation_package_filing_certificate_latest";
const ARCHIVE_RECEIPT_BIN: &str = "copybot_tiny_live_activation_package_archive_receipt";
const FILING_CERTIFICATE_LATEST_PLAN_READY: &str =
    "tiny_live_package_filing_certificate_latest_plan_ready";
const FILING_CERTIFICATE_LATEST_VERIFY_OK: &str =
    "tiny_live_package_filing_certificate_latest_verify_ok";
const ARCHIVE_RECEIPT_VERIFY_OK: &str = "tiny_live_package_archive_receipt_verify_ok";
const TOP_ACCEPTED_PACKAGE_STEP: &str = "clerestory_certificate";
const PLANNING_SAFE_STATEMENT: &str =
    "this latest-archive-receipt handoff binds the latest persisted immutable package chain to the accepted archive-receipt contract, but it never overrides Stage 3 or pre-activation refusal truth, it stays archival and read-only, it never runs the frozen live cutover controller by itself, and activation_authorized remains false";

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
    mode: Mode,
    root: Option<PathBuf>,
    session_dir: Option<PathBuf>,
    output_path: Option<PathBuf>,
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    PlanLatestArchiveReceipt,
    RenderLatestArchiveReceiptScript,
    RunLatestArchiveReceipt,
    VerifyLatestArchiveReceipt,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageArchiveReceiptLatestVerdict {
    TinyLivePackageArchiveReceiptLatestPlanReady,
    TinyLivePackageArchiveReceiptLatestScriptRendered,
    TinyLivePackageArchiveReceiptLatestRefusedByMissingLatestChain,
    TinyLivePackageArchiveReceiptLatestRefusedByInvalidLatestChain,
    TinyLivePackageArchiveReceiptLatestRefusedByDriftedArchiveReceiptContract,
    TinyLivePackageArchiveReceiptLatestRefusedNowByStage3,
    TinyLivePackageArchiveReceiptLatestRefusedNowByPreActivationGate,
    TinyLivePackageArchiveReceiptLatestReadyForManualExecutionWhenGateTurnsGreen,
    TinyLivePackageArchiveReceiptLatestVerifyOk,
    TinyLivePackageArchiveReceiptLatestVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LivePackageArchiveReceiptLatestResult {
    RefusedByMissingLatestChain,
    RefusedByInvalidLatestChain,
    RefusedByDriftedArchiveReceiptContract,
    RefusedNowByStage3,
    RefusedNowByPreActivationGate,
    ReadyForManualExecutionWhenGateTurnsGreen,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageArchiveReceiptLatestStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageArchiveReceiptLatestSession {
    session_version: String,
    planned_at: DateTime<Utc>,
    root: String,
    session_dir: String,
    nested_filing_certificate_latest_session_dir: String,
    nested_archive_receipt_session_dir: String,
    downstream_decision_packet_session_dir: String,
    latest_top_step_name: Option<String>,
    latest_top_session_dir: Option<String>,
    historical_execute_frozen_session_dir: Option<String>,
    turn_green_session_dir: Option<String>,
    launch_packet_session_dir: Option<String>,
    package_dir: Option<String>,
    install_root: Option<String>,
    target_service_name: Option<String>,
    backend_command: Option<String>,
    wrapper_timeout_ms: Option<u64>,
    service_status_max_staleness_ms: Option<u64>,
    resolve_latest_filing_certificate_command_summary: String,
    run_latest_archive_receipt_command_summary: String,
    verify_latest_archive_receipt_command_summary: String,
    downstream_archive_receipt_command_summary: String,
    verify_archive_receipt_command_summary: String,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    clerestory_certificate_summary: Option<String>,
    clerestory_certificate_sha256: Option<String>,
    clerestory_certificate_algorithm: Option<String>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageArchiveReceiptLatestStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    root: String,
    session_dir: String,
    latest_top_step_name: Option<String>,
    latest_top_session_dir: Option<String>,
    historical_execute_frozen_session_dir: Option<String>,
    turn_green_session_dir: Option<String>,
    launch_packet_session_dir: Option<String>,
    nested_filing_certificate_latest_session_dir: String,
    nested_archive_receipt_session_dir: String,
    downstream_decision_packet_session_dir: String,
    result: LivePackageArchiveReceiptLatestResult,
    reason: String,
    latest_filing_certificate_plan_verdict: Option<String>,
    latest_archive_receipt_verdict: Option<String>,
    archive_receipt_verdict: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    latest_filing_certificate_plan_step: Option<PackageArchiveReceiptLatestStepArtifact>,
    latest_archive_receipt_step: Option<PackageArchiveReceiptLatestStepArtifact>,
    archive_receipt_step: Option<PackageArchiveReceiptLatestStepArtifact>,
    operator_next_action_summary: String,
    activation_authorized: bool,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackageArchiveReceiptLatestPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    report_path: PathBuf,
    latest_filing_certificate_plan_report_path: PathBuf,
    latest_archive_receipt_report_path: PathBuf,
    archive_receipt_report_path: PathBuf,
    nested_filing_certificate_latest_session_dir: PathBuf,
    nested_archive_receipt_session_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageArchiveReceiptLatestReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageArchiveReceiptLatestVerdict,
    reason: String,
    root: Option<String>,
    latest_top_step_name: Option<String>,
    latest_top_session_dir: Option<String>,
    historical_execute_frozen_session_dir: Option<String>,
    turn_green_session_dir: Option<String>,
    launch_packet_session_dir: Option<String>,
    package_dir: Option<String>,
    install_root: Option<String>,
    target_service_name: Option<String>,
    backend_command: Option<String>,
    wrapper_timeout_ms: Option<u64>,
    service_status_max_staleness_ms: Option<u64>,
    session_dir: Option<String>,
    archive_receipt_latest_session_path: Option<String>,
    archive_receipt_latest_status_path: Option<String>,
    archive_receipt_latest_report_path: Option<String>,
    latest_filing_certificate_plan_report_path: Option<String>,
    latest_archive_receipt_report_path: Option<String>,
    archive_receipt_report_path: Option<String>,
    nested_filing_certificate_latest_session_dir: Option<String>,
    nested_archive_receipt_session_dir: Option<String>,
    downstream_decision_packet_session_dir: Option<String>,
    latest_filing_certificate_plan_verdict: Option<String>,
    latest_archive_receipt_verdict: Option<String>,
    archive_receipt_verdict: Option<String>,
    result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    resolve_latest_filing_certificate_command_summary: Option<String>,
    run_latest_archive_receipt_command_summary: Option<String>,
    verify_latest_archive_receipt_command_summary: Option<String>,
    downstream_archive_receipt_command_summary: Option<String>,
    verify_archive_receipt_command_summary: Option<String>,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    clerestory_certificate_summary: Option<String>,
    clerestory_certificate_sha256: Option<String>,
    clerestory_certificate_algorithm: Option<String>,
    verification_mismatches: Vec<String>,
    activation_authorized: bool,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct LatestFilingCertificateSnapshot {
    latest_top_step_name: String,
    latest_top_session_dir: PathBuf,
    downstream_decision_packet_session_dir: PathBuf,
    historical_execute_frozen_session_dir: PathBuf,
    turn_green_session_dir: PathBuf,
    launch_packet_session_dir: PathBuf,
    package_dir: PathBuf,
    install_root: PathBuf,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    reviewed_frozen_live_cutover_controller_command_summary: String,
    clerestory_certificate_summary: String,
    clerestory_certificate_sha256: String,
    clerestory_certificate_algorithm: String,
}

#[derive(Debug, Clone)]
struct ResolvedLatestFilingCertificate {
    snapshot: LatestFilingCertificateSnapshot,
    latest_filing_certificate_plan_raw: Value,
    latest_filing_certificate_plan: FilingCertificateLatestReportView,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResolutionFailureKind {
    MissingLatestChain,
    InvalidLatestChain,
    DriftedRegistryEntryContract,
}

#[derive(Debug, Clone)]
struct LatestFilingCertificateResolutionFailure {
    kind: ResolutionFailureKind,
    reason: String,
    snapshot: Option<LatestFilingCertificateSnapshot>,
    latest_filing_certificate_plan_raw: Option<Value>,
    latest_filing_certificate_plan_verdict: Option<String>,
    latest_filing_certificate_plan_reason: Option<String>,
    latest_filing_certificate_plan_generated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Deserialize)]
struct FilingCertificateLatestReportView {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: String,
    reason: String,
    latest_top_step_name: Option<String>,
    latest_top_session_dir: Option<String>,
    historical_execute_frozen_session_dir: Option<String>,
    turn_green_session_dir: Option<String>,
    launch_packet_session_dir: Option<String>,
    package_dir: Option<String>,
    install_root: Option<String>,
    target_service_name: Option<String>,
    backend_command: Option<String>,
    wrapper_timeout_ms: Option<u64>,
    service_status_max_staleness_ms: Option<u64>,
    session_dir: Option<String>,
    #[serde(alias = "downstream_decision_packet_session_dir")]
    downstream_decision_packet_session_dir: Option<String>,
    latest_execute_verdict: Option<String>,
    decision_packet_plan_verdict: Option<String>,
    result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    clerestory_certificate_summary: Option<String>,
    clerestory_certificate_sha256: Option<String>,
    clerestory_certificate_algorithm: Option<String>,
    activation_authorized: bool,
    explicit_statement: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ArchiveReceiptReportView {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: String,
    reason: String,
    filing_certificate_session_dir: String,
    provenance_certificate_session_dir: Option<String>,
    attestation_seal_session_dir: Option<String>,
    release_capsule_session_dir: Option<String>,
    activation_ticket_session_dir: Option<String>,
    review_receipt_session_dir: Option<String>,
    handoff_bundle_session_dir: Option<String>,
    decision_packet_session_dir: Option<String>,
    execute_frozen_session_dir: Option<String>,
    turn_green_session_dir: Option<String>,
    launch_packet_session_dir: Option<String>,
    package_dir: Option<String>,
    install_root: Option<String>,
    target_service_name: Option<String>,
    backend_command: Option<String>,
    wrapper_timeout_ms: Option<u64>,
    service_status_max_staleness_ms: Option<u64>,
    session_dir: Option<String>,
    archive_receipt_session_path: Option<String>,
    archive_receipt_status_path: Option<String>,
    archived_filing_certificate_report_path: Option<String>,
    result: Option<String>,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    filing_certificate_step: Option<PackageArchiveReceiptLatestStepArtifact>,
    verify_filing_certificate_command_summary: Option<String>,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    provenance_certificate_summary: Option<String>,
    chain_fingerprint_summary: Option<String>,
    chain_fingerprint_sha256: Option<String>,
    chain_fingerprint_algorithm: Option<String>,
    release_capsule_digest_manifest_sha256: Option<String>,
    release_capsule_digest_manifest_entry_count: Option<usize>,
    release_capsule_digest_algorithm: Option<String>,
    notarization_receipt_summary: Option<String>,
    ledger_seal_summary: Option<String>,
    ledger_seal_sha256: Option<String>,
    ledger_seal_algorithm: Option<String>,
    registry_entry_summary: Option<String>,
    registry_entry_sha256: Option<String>,
    registry_entry_algorithm: Option<String>,
    verification_mismatches: Option<Vec<String>>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ArchiveReceiptSessionView {
    filing_certificate_session_dir: String,
    provenance_certificate_session_dir: String,
    attestation_seal_session_dir: String,
    release_capsule_session_dir: String,
    activation_ticket_session_dir: String,
    review_receipt_session_dir: String,
    handoff_bundle_session_dir: String,
    decision_packet_session_dir: String,
    execute_frozen_session_dir: String,
    turn_green_session_dir: String,
    launch_packet_session_dir: String,
    package_dir: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    session_dir: String,
    verify_filing_certificate_command_summary: String,
    reviewed_frozen_live_cutover_controller_command_summary: String,
    provenance_certificate_summary: String,
    chain_fingerprint_summary: String,
    chain_fingerprint_sha256: String,
    chain_fingerprint_algorithm: String,
    release_capsule_digest_manifest_sha256: String,
    release_capsule_digest_manifest_entry_count: usize,
    release_capsule_digest_algorithm: String,
    notarization_receipt_summary: String,
    ledger_seal_summary: String,
    ledger_seal_sha256: String,
    ledger_seal_algorithm: String,
    registry_entry_summary: String,
    registry_entry_sha256: String,
    registry_entry_algorithm: String,
    explicit_statement: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ArchiveReceiptStatusView {
    session_dir: String,
    filing_certificate_session_dir: String,
    result: String,
    reason: String,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    filing_certificate_step: Option<PackageArchiveReceiptLatestStepArtifact>,
    provenance_certificate_summary: String,
    chain_fingerprint_summary: String,
    chain_fingerprint_sha256: String,
    chain_fingerprint_algorithm: String,
    release_capsule_digest_manifest_sha256: String,
    release_capsule_digest_manifest_entry_count: usize,
    release_capsule_digest_algorithm: String,
    notarization_receipt_summary: String,
    ledger_seal_summary: String,
    ledger_seal_sha256: String,
    ledger_seal_algorithm: String,
    registry_entry_summary: String,
    registry_entry_sha256: String,
    registry_entry_algorithm: String,
    explicit_statement: String,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut root = None;
    let mut session_dir = None;
    let mut output_path = None;
    let mut json = false;
    let mut mode = None;

    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--root" => root = Some(PathBuf::from(parse_string_arg("--root", iter.next())?)),
            "--session-dir" => {
                session_dir = Some(PathBuf::from(parse_string_arg(
                    "--session-dir",
                    iter.next(),
                )?))
            }
            "--output" => {
                output_path = Some(PathBuf::from(parse_string_arg("--output", iter.next())?))
            }
            "--json" => json = true,
            "--plan-latest-archive-receipt" => {
                set_mode(&mut mode, Mode::PlanLatestArchiveReceipt, arg.as_str())?
            }
            "--render-latest-archive-receipt-script" => set_mode(
                &mut mode,
                Mode::RenderLatestArchiveReceiptScript,
                arg.as_str(),
            )?,
            "--run-latest-archive-receipt" => {
                set_mode(&mut mode, Mode::RunLatestArchiveReceipt, arg.as_str())?
            }
            "--verify-latest-archive-receipt" => {
                set_mode(&mut mode, Mode::VerifyLatestArchiveReceipt, arg.as_str())?
            }
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown arg {other}; {USAGE}"),
        }
    }

    let Some(mode) = mode else {
        return Ok(None);
    };
    if matches!(
        mode,
        Mode::PlanLatestArchiveReceipt
            | Mode::RenderLatestArchiveReceiptScript
            | Mode::RunLatestArchiveReceipt
    ) && root.is_none()
    {
        bail!("missing --root");
    }
    if matches!(mode, Mode::RenderLatestArchiveReceiptScript) && output_path.is_none() {
        bail!("missing --output for render mode");
    }
    if matches!(
        mode,
        Mode::RunLatestArchiveReceipt | Mode::VerifyLatestArchiveReceipt
    ) && session_dir.is_none()
    {
        bail!("missing --session-dir");
    }

    Ok(Some(Config {
        mode,
        root,
        session_dir,
        output_path,
        json,
    }))
}

fn run(config: Config) -> Result<String> {
    let report = match config.mode {
        Mode::PlanLatestArchiveReceipt => plan_latest_archive_receipt_report(&config)?,
        Mode::RenderLatestArchiveReceiptScript => {
            render_latest_archive_receipt_script_report(&config)?
        }
        Mode::RunLatestArchiveReceipt => run_latest_archive_receipt_report(&config)?,
        Mode::VerifyLatestArchiveReceipt => verify_latest_archive_receipt_report(&config)?,
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_report_lines(&report))
    }
}

fn plan_latest_archive_receipt_report(
    config: &Config,
) -> Result<PackageArchiveReceiptLatestReport> {
    let root = config
        .root
        .as_ref()
        .ok_or_else(|| anyhow!("missing --root"))?;
    let suggested_session_dir = config.session_dir.clone().unwrap_or_else(|| {
        root.join("tiny_live_activation_package_archive_receipt_latest.session")
    });
    let paths = filing_certificate_latest_paths(&suggested_session_dir);

    match resolve_latest_filing_certificate(root, &paths) {
        Ok(resolved) => Ok(plan_report_from_resolution(
            "plan_latest_archive_receipt",
            TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestPlanReady,
            format!(
                "latest immutable {} chain under {} resolves to exact latest-filing-certificate {} lineage, exact decision-packet confirmation anchor {}, and the accepted archive-receipt contract remains current",
                resolved.snapshot.latest_top_step_name,
                root.display(),
                paths.nested_filing_certificate_latest_session_dir.display(),
                resolved.snapshot.downstream_decision_packet_session_dir.display(),
            ),
            root,
            &suggested_session_dir,
            &paths,
            &resolved,
        )),
        Err(failure) => Ok(report_from_resolution_failure(
            "plan_latest_archive_receipt",
            root,
            &suggested_session_dir,
            &paths,
            &failure,
        )),
    }
}

fn render_latest_archive_receipt_script_report(
    config: &Config,
) -> Result<PackageArchiveReceiptLatestReport> {
    let root = config
        .root
        .as_ref()
        .ok_or_else(|| anyhow!("missing --root"))?;
    let output_path = config
        .output_path
        .as_ref()
        .ok_or_else(|| anyhow!("missing --output for render mode"))?;
    let suggested_session_dir = config
        .session_dir
        .clone()
        .unwrap_or_else(|| output_path.with_extension("session"));
    let paths = filing_certificate_latest_paths(&suggested_session_dir);

    match resolve_latest_filing_certificate(root, &paths) {
        Ok(resolved) => {
            write_script(
                output_path,
                &format!(
                    "#!/usr/bin/env bash\nset -euo pipefail\n\n{run_latest}\n{verify_latest}\n{run_handoff_bundle}\n{verify_handoff_bundle}\n",
                    run_latest = run_latest_archive_receipt_command_summary(
                        root,
                        &paths.nested_filing_certificate_latest_session_dir,
                    ),
                    verify_latest = verify_latest_archive_receipt_command_summary(
                        &paths.nested_filing_certificate_latest_session_dir,
                    ),
                    run_handoff_bundle = downstream_archive_receipt_command_summary(
                        &resolved.snapshot,
                        &expected_downstream_filing_certificate_session_dir(&paths),
                        &paths.nested_archive_receipt_session_dir,
                    ),
                    verify_handoff_bundle = verify_archive_receipt_command_summary(
                        &resolved.snapshot,
                        &expected_downstream_filing_certificate_session_dir(&paths),
                        &paths.nested_archive_receipt_session_dir,
                    ),
                ),
            )?;
            Ok(plan_report_from_resolution(
                "render_latest_archive_receipt_script",
                TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestScriptRendered,
                format!(
                    "rendered latest-archive-receipt handoff script to {} using the accepted latest-filing-certificate and archive-receipt contracts",
                    output_path.display()
                ),
                root,
                &suggested_session_dir,
                &paths,
                &resolved,
            ))
        }
        Err(failure) => Ok(report_from_resolution_failure(
            "render_latest_archive_receipt_script",
            root,
            &suggested_session_dir,
            &paths,
            &failure,
        )),
    }
}

fn run_latest_archive_receipt_report(config: &Config) -> Result<PackageArchiveReceiptLatestReport> {
    let root = config
        .root
        .as_ref()
        .ok_or_else(|| anyhow!("missing --root"))?;
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for run mode"))?;
    if session_dir.exists() {
        bail!(
            "refusing to overwrite existing latest-archive-receipt session dir {}",
            session_dir.display()
        );
    }
    let paths = filing_certificate_latest_paths(session_dir);
    create_clean_session_dir(session_dir, "latest-archive-receipt")?;

    let resolution = resolve_latest_filing_certificate(root, &paths);
    match resolution {
        Ok(resolved) => {
            run_latest_filing_certificate_with_resolution(root, session_dir, &paths, &resolved)
        }
        Err(failure) => run_latest_filing_certificate_failure(root, session_dir, &paths, &failure),
    }
}

fn verify_latest_archive_receipt_report(
    config: &Config,
) -> Result<PackageArchiveReceiptLatestReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for verify mode"))?;
    let paths = filing_certificate_latest_paths(session_dir);

    let session: PackageArchiveReceiptLatestSession = match load_json(&paths.session_path) {
        Ok(value) => value,
        Err(error) => {
            return Ok(
                verify_invalid_filing_certificate_latest_report_without_session(
                    session_dir,
                    vec![format!(
                        "failed reading latest-archive-receipt session artifact {}: {error}",
                        paths.session_path.display()
                    )],
                ),
            )
        }
    };
    let status: PackageArchiveReceiptLatestStatus = match load_json(&paths.status_path) {
        Ok(value) => value,
        Err(error) => {
            return Ok(
                verify_invalid_filing_certificate_latest_report_without_session(
                    session_dir,
                    vec![format!(
                        "failed reading latest-archive-receipt status artifact {}: {error}",
                        paths.status_path.display()
                    )],
                ),
            )
        }
    };
    let stored_report: PackageArchiveReceiptLatestReport = match load_json(&paths.report_path) {
        Ok(value) => value,
        Err(error) => {
            return Ok(
                verify_invalid_filing_certificate_latest_report_without_session(
                    session_dir,
                    vec![format!(
                        "failed reading latest-archive-receipt report artifact {}: {error}",
                        paths.report_path.display()
                    )],
                ),
            )
        }
    };

    let mut mismatches = Vec::new();
    let root = PathBuf::from(&session.root);

    compare_string(
        &session.root,
        &status.root,
        "latest-archive-receipt root consistency",
        &mut mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "latest-archive-receipt session session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.session_dir,
        &session_dir.display().to_string(),
        "latest-archive-receipt status session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "latest-archive-receipt report session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.nested_filing_certificate_latest_session_dir,
        &paths
            .nested_filing_certificate_latest_session_dir
            .display()
            .to_string(),
        "latest-archive-receipt session nested_filing_certificate_latest_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.nested_filing_certificate_latest_session_dir,
        &paths
            .nested_filing_certificate_latest_session_dir
            .display()
            .to_string(),
        "latest-archive-receipt status nested_filing_certificate_latest_session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report
            .nested_filing_certificate_latest_session_dir
            .as_deref(),
        Some(
            paths
                .nested_filing_certificate_latest_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "latest-archive-receipt report nested_filing_certificate_latest_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.nested_archive_receipt_session_dir,
        &paths
            .nested_archive_receipt_session_dir
            .display()
            .to_string(),
        "latest-archive-receipt session nested_archive_receipt_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.nested_archive_receipt_session_dir,
        &paths
            .nested_archive_receipt_session_dir
            .display()
            .to_string(),
        "latest-archive-receipt status nested_archive_receipt_session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.nested_archive_receipt_session_dir.as_deref(),
        Some(
            paths
                .nested_archive_receipt_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "latest-archive-receipt report nested_archive_receipt_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.downstream_decision_packet_session_dir,
        &expected_downstream_decision_packet_session_dir(&paths)
            .display()
            .to_string(),
        "latest-archive-receipt session downstream_decision_packet_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.downstream_decision_packet_session_dir,
        &expected_downstream_decision_packet_session_dir(&paths)
            .display()
            .to_string(),
        "latest-archive-receipt status downstream_decision_packet_session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report
            .downstream_decision_packet_session_dir
            .as_deref(),
        Some(
            expected_downstream_decision_packet_session_dir(&paths)
                .display()
                .to_string()
                .as_str(),
        ),
        "latest-archive-receipt report downstream_decision_packet_session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.archive_receipt_latest_session_path.as_deref(),
        Some(paths.session_path.display().to_string().as_str()),
        "latest-archive-receipt report session path",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.archive_receipt_latest_status_path.as_deref(),
        Some(paths.status_path.display().to_string().as_str()),
        "latest-archive-receipt report status path",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.archive_receipt_latest_report_path.as_deref(),
        Some(paths.report_path.display().to_string().as_str()),
        "latest-archive-receipt report report path",
        &mut mismatches,
    );
    compare_string(
        &session.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "latest-archive-receipt session explicit_statement",
        &mut mismatches,
    );
    compare_string(
        &status.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "latest-archive-receipt status explicit_statement",
        &mut mismatches,
    );
    compare_string(
        &stored_report.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "latest-archive-receipt report explicit_statement",
        &mut mismatches,
    );
    compare_string(
        &stored_report.mode,
        "run_latest_archive_receipt",
        "latest-archive-receipt stored report mode",
        &mut mismatches,
    );
    compare_string(
        &serialize_enum(&stored_report.verdict),
        &serialize_enum(&verdict_for_result(status.result)),
        "latest-archive-receipt stored report verdict",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.result.as_deref(),
        Some(serialize_enum(&status.result).as_str()),
        "latest-archive-receipt stored report result consistency",
        &mut mismatches,
    );
    compare_string(
        &stored_report.reason,
        &status.reason,
        "latest-archive-receipt stored report reason consistency",
        &mut mismatches,
    );
    if stored_report.activation_authorized != status.activation_authorized {
        mismatches.push(format!(
            "latest-archive-receipt stored report activation_authorized {} does not match status {}",
            stored_report.activation_authorized, status.activation_authorized
        ));
    }
    compare_option_string(
        stored_report
            .latest_filing_certificate_plan_verdict
            .as_deref(),
        status.latest_filing_certificate_plan_verdict.as_deref(),
        "stored report latest_filing_certificate_plan_verdict",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.latest_archive_receipt_verdict.as_deref(),
        status.latest_archive_receipt_verdict.as_deref(),
        "stored report latest_archive_receipt_verdict",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.archive_receipt_verdict.as_deref(),
        status.archive_receipt_verdict.as_deref(),
        "stored report archive_receipt_verdict",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.current_pre_activation_gate_verdict.as_deref(),
        status.current_pre_activation_gate_verdict.as_deref(),
        "stored report current_pre_activation_gate_verdict",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.current_pre_activation_gate_reason.as_deref(),
        status.current_pre_activation_gate_reason.as_deref(),
        "stored report current_pre_activation_gate_reason",
        &mut mismatches,
    );

    let resolution = resolve_latest_filing_certificate(&root, &paths);
    match resolution {
        Ok(resolved) => {
            compare_session_against_snapshot(
                &session,
                &root,
                session_dir,
                &paths,
                &resolved.snapshot,
                &mut mismatches,
            );
            compare_report_against_snapshot(
                &stored_report,
                &root,
                session_dir,
                &paths,
                &resolved.snapshot,
                &mut mismatches,
            );

            let stored_latest_plan: FilingCertificateLatestReportView = load_required_step_json(
                &status.latest_filing_certificate_plan_step,
                &paths.latest_filing_certificate_plan_report_path,
                "latest_filing_certificate_plan_step",
                &mut mismatches,
            )?;
            compare_step_artifact_against_report_metadata(
                status.latest_filing_certificate_plan_step.as_ref(),
                &paths.latest_filing_certificate_plan_report_path,
                "plan_latest_filing_certificate",
                &stored_latest_plan.verdict,
                &stored_latest_plan.reason,
                stored_latest_plan.generated_at,
                "stored latest_filing_certificate_plan_step",
                &mut mismatches,
            );
            compare_filing_certificate_latest_plan_against_snapshot(
                &stored_latest_plan,
                &resolved.snapshot,
                &paths.nested_filing_certificate_latest_session_dir,
                &mut mismatches,
            );
            compare_option_string(
                status.latest_filing_certificate_plan_verdict.as_deref(),
                Some(stored_latest_plan.verdict.as_str()),
                "stored status latest_filing_certificate_plan_verdict",
                &mut mismatches,
            );
            compare_option_string(
                stored_report
                    .latest_filing_certificate_plan_verdict
                    .as_deref(),
                Some(stored_latest_plan.verdict.as_str()),
                "stored report latest_filing_certificate_plan_verdict",
                &mut mismatches,
            );

            let stored_latest_run: FilingCertificateLatestReportView = load_required_step_json(
                &status.latest_archive_receipt_step,
                &paths.latest_archive_receipt_report_path,
                "latest_archive_receipt_step",
                &mut mismatches,
            )?;
            compare_step_artifact_against_report_metadata(
                status.latest_archive_receipt_step.as_ref(),
                &paths.latest_archive_receipt_report_path,
                "run_latest_filing_certificate",
                &stored_latest_run.verdict,
                &stored_latest_run.reason,
                stored_latest_run.generated_at,
                "stored latest_archive_receipt_step",
                &mut mismatches,
            );
            compare_filing_certificate_latest_run_against_snapshot(
                &stored_latest_run,
                &resolved.snapshot,
                &paths.nested_filing_certificate_latest_session_dir,
                &mut mismatches,
            );
            compare_option_string(
                status.latest_archive_receipt_verdict.as_deref(),
                Some(stored_latest_run.verdict.as_str()),
                "stored status latest_archive_receipt_verdict",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.latest_archive_receipt_verdict.as_deref(),
                Some(stored_latest_run.verdict.as_str()),
                "stored report latest_archive_receipt_verdict",
                &mut mismatches,
            );

            let actual_latest_run: FilingCertificateLatestReportView = match load_json(
                &nested_filing_certificate_latest_report_path(
                    &paths.nested_filing_certificate_latest_session_dir,
                ),
            ) {
                Ok(value) => value,
                Err(error) => {
                    mismatches.push(format!(
                        "failed loading actual nested latest-filing-certificate report under {}: {error}",
                        paths.nested_filing_certificate_latest_session_dir.display()
                    ));
                    fallback_filing_certificate_latest_copy()
                }
            };
            compare_filing_certificate_latest_copy_matches_actual(
                &stored_latest_run,
                &actual_latest_run,
                &mut mismatches,
            );

            let latest_verify_raw = match run_json_command(
                FILING_CERTIFICATE_LATEST_BIN,
                &latest_archive_receipt_verify_args(
                    &paths.nested_filing_certificate_latest_session_dir,
                ),
            ) {
                Ok(value) => value,
                Err(error) => {
                    mismatches.push(format!(
                        "failed verifying nested latest-filing-certificate session under {}: {error}",
                        paths.nested_filing_certificate_latest_session_dir.display()
                    ));
                    Value::Null
                }
            };
            if !latest_verify_raw.is_null() {
                let latest_verify: FilingCertificateLatestReportView =
                    serde_json::from_value(latest_verify_raw)
                        .context("failed parsing nested latest-filing-certificate verify json")?;
                if latest_verify.verdict != FILING_CERTIFICATE_LATEST_VERIFY_OK {
                    mismatches.push(format!(
                        "nested latest-filing-certificate verification is non-green: {}",
                        latest_verify.reason
                    ));
                }
            }

            let stored_handoff_bundle: ArchiveReceiptReportView = load_required_step_json(
                &status.archive_receipt_step,
                &paths.archive_receipt_report_path,
                "archive_receipt_step",
                &mut mismatches,
            )?;
            compare_step_artifact_against_report_metadata(
                status.archive_receipt_step.as_ref(),
                &paths.archive_receipt_report_path,
                "run_live_package_archive_receipt",
                &stored_handoff_bundle.verdict,
                &stored_handoff_bundle.reason,
                stored_handoff_bundle.generated_at,
                "stored archive_receipt_step",
                &mut mismatches,
            );
            compare_archive_receipt_run_against_snapshot(
                &stored_handoff_bundle,
                &resolved.snapshot,
                &expected_downstream_filing_certificate_session_dir(&paths),
                &paths.nested_archive_receipt_session_dir,
                &mut mismatches,
            );
            compare_option_string(
                status.archive_receipt_verdict.as_deref(),
                Some(stored_handoff_bundle.verdict.as_str()),
                "stored status archive_receipt_verdict",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.archive_receipt_verdict.as_deref(),
                Some(stored_handoff_bundle.verdict.as_str()),
                "stored report archive_receipt_verdict",
                &mut mismatches,
            );

            let nested_handoff_bundle_session: ArchiveReceiptSessionView = match load_json(
                &nested_archive_receipt_session_path(&paths.nested_archive_receipt_session_dir),
            ) {
                Ok(value) => value,
                Err(error) => {
                    mismatches.push(format!(
                        "failed reading nested archive-receipt session under {}: {error}",
                        paths.nested_archive_receipt_session_dir.display()
                    ));
                    fallback_notarization_receipt_session(
                        &resolved.snapshot,
                        &paths.nested_archive_receipt_session_dir,
                    )
                }
            };
            let nested_handoff_bundle_status: ArchiveReceiptStatusView = match load_json(
                &nested_archive_receipt_status_path(&paths.nested_archive_receipt_session_dir),
            ) {
                Ok(value) => value,
                Err(error) => {
                    mismatches.push(format!(
                        "failed reading nested archive-receipt status under {}: {error}",
                        paths.nested_archive_receipt_session_dir.display()
                    ));
                    fallback_notarization_receipt_status(
                        &resolved.snapshot,
                        &paths.nested_archive_receipt_session_dir,
                    )
                }
            };
            compare_archive_receipt_copy_against_nested_truth(
                &stored_handoff_bundle,
                &nested_handoff_bundle_session,
                &nested_handoff_bundle_status,
                &mut mismatches,
            );

            let handoff_bundle_verify_raw = match run_json_command(
                ARCHIVE_RECEIPT_BIN,
                &archive_receipt_verify_args(
                    &resolved.snapshot,
                    &expected_downstream_filing_certificate_session_dir(&paths),
                    &paths.nested_archive_receipt_session_dir,
                ),
            ) {
                Ok(value) => value,
                Err(error) => {
                    mismatches.push(format!(
                        "failed verifying nested archive-receipt session under {}: {error}",
                        paths.nested_archive_receipt_session_dir.display()
                    ));
                    Value::Null
                }
            };
            if !handoff_bundle_verify_raw.is_null() {
                let handoff_bundle_verify: ArchiveReceiptReportView =
                    serde_json::from_value(handoff_bundle_verify_raw)
                        .context("failed parsing nested archive-receipt verify json")?;
                compare_option_string(
                    stored_handoff_bundle
                        .attestation_seal_session_dir
                        .as_deref(),
                    handoff_bundle_verify
                        .attestation_seal_session_dir
                        .as_deref(),
                    "stored filing-certificate copy attestation_seal_session_dir against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.chain_fingerprint_summary.as_deref(),
                    handoff_bundle_verify.chain_fingerprint_summary.as_deref(),
                    "stored filing-certificate copy chain_fingerprint_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.chain_fingerprint_sha256.as_deref(),
                    handoff_bundle_verify.chain_fingerprint_sha256.as_deref(),
                    "stored filing-certificate copy chain_fingerprint_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.chain_fingerprint_algorithm.as_deref(),
                    handoff_bundle_verify.chain_fingerprint_algorithm.as_deref(),
                    "stored filing-certificate copy chain_fingerprint_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .reviewed_frozen_live_cutover_controller_command_summary
                        .as_deref(),
                    handoff_bundle_verify
                        .reviewed_frozen_live_cutover_controller_command_summary
                        .as_deref(),
                    "stored archive-receipt copy reviewed_frozen_live_cutover_controller_command_summary",
                    &mut mismatches,
                );
                if handoff_bundle_verify.verdict != ARCHIVE_RECEIPT_VERIFY_OK {
                    mismatches.push(format!(
                        "nested archive-receipt verification is non-green: {}",
                        handoff_bundle_verify.reason
                    ));
                }
            }

            let expected_result = filing_certificate_latest_result_from_notarization_receipt(
                nested_handoff_bundle_status.result.as_str(),
            )
            .unwrap_or(
                LivePackageArchiveReceiptLatestResult::RefusedByDriftedArchiveReceiptContract,
            );
            if status.result != expected_result {
                mismatches.push(format!(
                    "stored latest-archive-receipt result {} does not match nested archive-receipt result {}",
                    serialize_enum(&status.result),
                    serialize_enum(&expected_result)
                ));
            }
            compare_string(
                &status.reason,
                &nested_handoff_bundle_status.reason,
                "stored status reason",
                &mut mismatches,
            );
            compare_string(
                &status.operator_next_action_summary,
                &operator_next_action_summary(expected_result),
                "stored status operator_next_action_summary",
                &mut mismatches,
            );
            if status.activation_authorized {
                mismatches.push(
                    "stored status activation_authorized must remain false for latest-archive-receipt"
                        .to_string(),
                );
            }
            compare_option_string(
                status.current_pre_activation_gate_verdict.as_deref(),
                nested_handoff_bundle_status
                    .current_pre_activation_gate_verdict
                    .as_deref(),
                "stored status current_pre_activation_gate_verdict",
                &mut mismatches,
            );
            compare_option_string(
                status.current_pre_activation_gate_reason.as_deref(),
                nested_handoff_bundle_status
                    .current_pre_activation_gate_reason
                    .as_deref(),
                "stored status current_pre_activation_gate_reason",
                &mut mismatches,
            );
        }
        Err(failure) => {
            compare_session_against_failure(
                &session,
                &root,
                session_dir,
                &paths,
                &failure,
                &mut mismatches,
            );
            compare_report_against_failure(
                &stored_report,
                &root,
                session_dir,
                &paths,
                &failure,
                &mut mismatches,
            );
            if failure.latest_filing_certificate_plan_raw.is_some() {
                let stored_latest_plan: FilingCertificateLatestReportView =
                    load_required_step_json(
                        &status.latest_filing_certificate_plan_step,
                        &paths.latest_filing_certificate_plan_report_path,
                        "latest_filing_certificate_plan_step",
                        &mut mismatches,
                    )?;
                compare_step_artifact_against_report_metadata(
                    status.latest_filing_certificate_plan_step.as_ref(),
                    &paths.latest_filing_certificate_plan_report_path,
                    "plan_latest_filing_certificate",
                    &stored_latest_plan.verdict,
                    &stored_latest_plan.reason,
                    stored_latest_plan.generated_at,
                    "stored latest_filing_certificate_plan_step",
                    &mut mismatches,
                );
                compare_option_string(
                    status.latest_filing_certificate_plan_verdict.as_deref(),
                    failure.latest_filing_certificate_plan_verdict.as_deref(),
                    "stored status latest_filing_certificate_plan_verdict on failure",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_report
                        .latest_filing_certificate_plan_verdict
                        .as_deref(),
                    failure.latest_filing_certificate_plan_verdict.as_deref(),
                    "stored report latest_filing_certificate_plan_verdict on failure",
                    &mut mismatches,
                );
            }
            compare_option_string(
                status.latest_archive_receipt_verdict.as_deref(),
                None,
                "stored status latest_archive_receipt_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.latest_archive_receipt_verdict.as_deref(),
                None,
                "stored report latest_archive_receipt_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                status.archive_receipt_verdict.as_deref(),
                None,
                "stored status archive_receipt_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.archive_receipt_verdict.as_deref(),
                None,
                "stored report archive_receipt_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                status.current_pre_activation_gate_verdict.as_deref(),
                None,
                "stored status current_pre_activation_gate_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.current_pre_activation_gate_verdict.as_deref(),
                None,
                "stored report current_pre_activation_gate_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                status.current_pre_activation_gate_reason.as_deref(),
                None,
                "stored status current_pre_activation_gate_reason on failure",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.current_pre_activation_gate_reason.as_deref(),
                None,
                "stored report current_pre_activation_gate_reason on failure",
                &mut mismatches,
            );
            compare_string(
                &status.reason,
                &failure.reason,
                "stored status failure reason",
                &mut mismatches,
            );
            compare_string(
                &stored_report.reason,
                &failure.reason,
                "stored report failure reason",
                &mut mismatches,
            );
            compare_string(
                &status.operator_next_action_summary,
                &operator_next_action_summary(result_from_failure_kind(failure.kind)),
                "stored status operator_next_action_summary on failure",
                &mut mismatches,
            );
            if status.activation_authorized {
                mismatches.push(
                    "stored status activation_authorized must be false on failure".to_string(),
                );
            }
            if stored_report.activation_authorized {
                mismatches.push(
                    "stored report activation_authorized must be false on failure".to_string(),
                );
            }
        }
    }

    let verdict = if mismatches.is_empty() {
        TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestVerifyOk
    } else {
        TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestVerifyInvalid
    };
    let reason = if mismatches.is_empty() {
        format!(
            "latest-archive-receipt session under {} is coherent with the current latest immutable chain and the accepted archive-receipt contract",
            session_dir.display()
        )
    } else {
        mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "latest-archive-receipt verification failed".to_string())
    };

    Ok(PackageArchiveReceiptLatestReport {
        generated_at: Utc::now(),
        mode: "verify_latest_archive_receipt".to_string(),
        verdict,
        reason,
        root: Some(session.root.clone()),
        latest_top_step_name: status.latest_top_step_name.clone(),
        latest_top_session_dir: status.latest_top_session_dir.clone(),
        historical_execute_frozen_session_dir: status
            .historical_execute_frozen_session_dir
            .clone(),
        turn_green_session_dir: status.turn_green_session_dir.clone(),
        launch_packet_session_dir: status.launch_packet_session_dir.clone(),
        package_dir: session.package_dir.clone(),
        install_root: session.install_root.clone(),
        target_service_name: session.target_service_name.clone(),
        backend_command: session.backend_command.clone(),
        wrapper_timeout_ms: session.wrapper_timeout_ms,
        service_status_max_staleness_ms: session.service_status_max_staleness_ms,
        session_dir: Some(session_dir.display().to_string()),
        archive_receipt_latest_session_path: Some(paths.session_path.display().to_string()),
        archive_receipt_latest_status_path: Some(paths.status_path.display().to_string()),
        archive_receipt_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_filing_certificate_plan_report_path: Some(
            paths
                .latest_filing_certificate_plan_report_path
                .display()
                .to_string(),
        ),
        latest_archive_receipt_report_path: Some(
            paths.latest_archive_receipt_report_path.display().to_string(),
        ),
        archive_receipt_report_path: Some(paths.archive_receipt_report_path.display().to_string()),
        nested_filing_certificate_latest_session_dir: Some(
            paths
                .nested_filing_certificate_latest_session_dir
                .display()
                .to_string(),
        ),
        nested_archive_receipt_session_dir: Some(
            paths.nested_archive_receipt_session_dir.display().to_string(),
        ),
        downstream_decision_packet_session_dir: Some(
            expected_downstream_decision_packet_session_dir(&paths)
                .display()
                .to_string(),
        ),
        latest_filing_certificate_plan_verdict: status.latest_filing_certificate_plan_verdict.clone(),
        latest_archive_receipt_verdict: status.latest_archive_receipt_verdict.clone(),
        archive_receipt_verdict: status.archive_receipt_verdict.clone(),
        result: Some(serialize_enum(&status.result)),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        resolve_latest_filing_certificate_command_summary: Some(
            session.resolve_latest_filing_certificate_command_summary.clone(),
        ),
        run_latest_archive_receipt_command_summary: Some(
            session.run_latest_archive_receipt_command_summary.clone(),
        ),
        verify_latest_archive_receipt_command_summary: Some(
            session.verify_latest_archive_receipt_command_summary.clone(),
        ),
        downstream_archive_receipt_command_summary: Some(
            session.downstream_archive_receipt_command_summary.clone(),
        ),
        verify_archive_receipt_command_summary: Some(session.verify_archive_receipt_command_summary.clone()),
        reviewed_frozen_live_cutover_controller_command_summary: session
            .reviewed_frozen_live_cutover_controller_command_summary
            .clone(),
        clerestory_certificate_summary: session.clerestory_certificate_summary.clone(),
        clerestory_certificate_sha256: session.clerestory_certificate_sha256.clone(),
        clerestory_certificate_algorithm: session.clerestory_certificate_algorithm.clone(),
        verification_mismatches: mismatches,
        activation_authorized: false,
        explicit_statement:
            "this verification proves only whether the latest immutable chain still resolves cleanly into the accepted archive-receipt contract; it never runs the frozen controller"
                .to_string(),
    })
}

fn run_latest_filing_certificate_with_resolution(
    root: &Path,
    session_dir: &Path,
    paths: &PackageArchiveReceiptLatestPaths,
    resolved: &ResolvedLatestFilingCertificate,
) -> Result<PackageArchiveReceiptLatestReport> {
    let session = planned_session(root, session_dir, paths, Some(resolved), None);
    persist_json(&paths.session_path, &session)?;
    persist_json(
        &paths.latest_filing_certificate_plan_report_path,
        &resolved.latest_filing_certificate_plan_raw,
    )?;

    let latest_filing_certificate_plan_step = Some(step_artifact(
        &paths.latest_filing_certificate_plan_report_path,
        "plan_latest_filing_certificate",
        &resolved.latest_filing_certificate_plan.verdict,
        &resolved.latest_filing_certificate_plan.reason,
        resolved.latest_filing_certificate_plan.generated_at,
    ));

    let latest_filing_certificate_run_raw = match run_json_command(
        FILING_CERTIFICATE_LATEST_BIN,
        &latest_filing_certificate_run_args(
            root,
            &paths.nested_filing_certificate_latest_session_dir,
        ),
    ) {
        Ok(value) => value,
        Err(error) => {
            let status = refusal_status(
                root,
                session_dir,
                paths,
                Some(resolved),
                LivePackageArchiveReceiptLatestResult::RefusedByDriftedArchiveReceiptContract,
                format!(
                    "failed running nested latest-filing-certificate handoff under {}: {error}",
                    paths.nested_filing_certificate_latest_session_dir.display()
                ),
                latest_filing_certificate_plan_step,
                None,
                None,
            );
            persist_json(&paths.status_path, &status)?;
            let report = report_from_status(
                "run_latest_archive_receipt",
                verdict_for_result(status.result),
                session_dir,
                paths,
                &session,
                &status,
                Vec::new(),
            );
            persist_json(&paths.report_path, &report)?;
            return Ok(report);
        }
    };
    persist_json(
        &paths.latest_archive_receipt_report_path,
        &latest_filing_certificate_run_raw,
    )?;
    let latest_filing_certificate_run: FilingCertificateLatestReportView =
        serde_json::from_value(latest_filing_certificate_run_raw)
            .context("failed parsing nested latest-filing-certificate run json")?;
    let latest_archive_receipt_step = Some(step_artifact(
        &paths.latest_archive_receipt_report_path,
        "run_latest_filing_certificate",
        &latest_filing_certificate_run.verdict,
        &latest_filing_certificate_run.reason,
        latest_filing_certificate_run.generated_at,
    ));

    if let Some(terminal_failure) =
        latest_filing_certificate_terminal_failure(&latest_filing_certificate_run)
    {
        let status = refusal_status(
            root,
            session_dir,
            paths,
            Some(resolved),
            terminal_failure,
            latest_filing_certificate_run.reason.clone(),
            latest_filing_certificate_plan_step,
            latest_archive_receipt_step,
            None,
        );
        persist_json(&paths.status_path, &status)?;
        let report = report_from_status(
            "run_latest_archive_receipt",
            verdict_for_result(status.result),
            session_dir,
            paths,
            &session,
            &status,
            Vec::new(),
        );
        persist_json(&paths.report_path, &report)?;
        return Ok(report);
    }

    let mut run_mismatches = Vec::new();
    compare_filing_certificate_latest_run_against_snapshot(
        &latest_filing_certificate_run,
        &resolved.snapshot,
        &paths.nested_filing_certificate_latest_session_dir,
        &mut run_mismatches,
    );
    if !run_mismatches.is_empty() {
        let status = refusal_status(
            root,
            session_dir,
            paths,
            Some(resolved),
            LivePackageArchiveReceiptLatestResult::RefusedByDriftedArchiveReceiptContract,
            run_mismatches[0].clone(),
            latest_filing_certificate_plan_step,
            latest_archive_receipt_step,
            None,
        );
        persist_json(&paths.status_path, &status)?;
        let report = report_from_status(
            "run_latest_archive_receipt",
            verdict_for_result(status.result),
            session_dir,
            paths,
            &session,
            &status,
            Vec::new(),
        );
        persist_json(&paths.report_path, &report)?;
        return Ok(report);
    }

    let latest_filing_certificate_verify: FilingCertificateLatestReportView = match run_json_command(
        FILING_CERTIFICATE_LATEST_BIN,
        &latest_archive_receipt_verify_args(&paths.nested_filing_certificate_latest_session_dir),
    ) {
        Ok(value) => serde_json::from_value(value)
            .context("failed parsing nested latest-filing-certificate verify json")?,
        Err(error) => {
            let status = refusal_status(
                root,
                session_dir,
                paths,
                Some(resolved),
                LivePackageArchiveReceiptLatestResult::RefusedByDriftedArchiveReceiptContract,
                format!(
                    "failed verifying nested latest-filing-certificate handoff under {}: {error}",
                    paths.nested_filing_certificate_latest_session_dir.display()
                ),
                latest_filing_certificate_plan_step,
                latest_archive_receipt_step,
                None,
            );
            persist_json(&paths.status_path, &status)?;
            let report = report_from_status(
                "run_latest_archive_receipt",
                verdict_for_result(status.result),
                session_dir,
                paths,
                &session,
                &status,
                Vec::new(),
            );
            persist_json(&paths.report_path, &report)?;
            return Ok(report);
        }
    };
    if latest_filing_certificate_verify.verdict != FILING_CERTIFICATE_LATEST_VERIFY_OK {
        let status = refusal_status(
            root,
            session_dir,
            paths,
            Some(resolved),
            LivePackageArchiveReceiptLatestResult::RefusedByDriftedArchiveReceiptContract,
            latest_filing_certificate_verify.reason,
            latest_filing_certificate_plan_step,
            latest_archive_receipt_step,
            None,
        );
        persist_json(&paths.status_path, &status)?;
        let report = report_from_status(
            "run_latest_archive_receipt",
            verdict_for_result(status.result),
            session_dir,
            paths,
            &session,
            &status,
            Vec::new(),
        );
        persist_json(&paths.report_path, &report)?;
        return Ok(report);
    }

    let release_capsule_run_raw = match run_json_command(
        ARCHIVE_RECEIPT_BIN,
        &registry_entry_run_args(
            &resolved.snapshot,
            &expected_downstream_filing_certificate_session_dir(paths),
            &paths.nested_archive_receipt_session_dir,
        ),
    ) {
        Ok(value) => value,
        Err(error) => {
            let status = refusal_status(
                root,
                session_dir,
                paths,
                Some(resolved),
                LivePackageArchiveReceiptLatestResult::RefusedByDriftedArchiveReceiptContract,
                format!(
                    "failed running downstream archive-receipt contract under {}: {error}",
                    paths.nested_archive_receipt_session_dir.display()
                ),
                latest_filing_certificate_plan_step,
                latest_archive_receipt_step,
                None,
            );
            persist_json(&paths.status_path, &status)?;
            let report = report_from_status(
                "run_latest_archive_receipt",
                verdict_for_result(status.result),
                session_dir,
                paths,
                &session,
                &status,
                Vec::new(),
            );
            persist_json(&paths.report_path, &report)?;
            return Ok(report);
        }
    };
    persist_json(&paths.archive_receipt_report_path, &release_capsule_run_raw)?;
    let provenance_certificate_run: ArchiveReceiptReportView =
        serde_json::from_value(release_capsule_run_raw)
            .context("failed parsing nested archive-receipt run json")?;
    let archive_receipt_step = Some(step_artifact(
        &paths.archive_receipt_report_path,
        "run_live_package_archive_receipt",
        &provenance_certificate_run.verdict,
        &provenance_certificate_run.reason,
        provenance_certificate_run.generated_at,
    ));

    let mut provenance_certificate_mismatches = Vec::new();
    compare_archive_receipt_run_against_snapshot(
        &provenance_certificate_run,
        &resolved.snapshot,
        &expected_downstream_filing_certificate_session_dir(paths),
        &paths.nested_archive_receipt_session_dir,
        &mut provenance_certificate_mismatches,
    );
    if !provenance_certificate_mismatches.is_empty() {
        let status = refusal_status(
            root,
            session_dir,
            paths,
            Some(resolved),
            LivePackageArchiveReceiptLatestResult::RefusedByDriftedArchiveReceiptContract,
            provenance_certificate_mismatches[0].clone(),
            latest_filing_certificate_plan_step,
            latest_archive_receipt_step,
            archive_receipt_step,
        );
        persist_json(&paths.status_path, &status)?;
        let report = report_from_status(
            "run_latest_archive_receipt",
            verdict_for_result(status.result),
            session_dir,
            paths,
            &session,
            &status,
            Vec::new(),
        );
        persist_json(&paths.report_path, &report)?;
        return Ok(report);
    }

    let provenance_certificate_verify: ArchiveReceiptReportView = match run_json_command(
        ARCHIVE_RECEIPT_BIN,
        &archive_receipt_verify_args(
            &resolved.snapshot,
            &expected_downstream_filing_certificate_session_dir(paths),
            &paths.nested_archive_receipt_session_dir,
        ),
    ) {
        Ok(value) => serde_json::from_value(value)
            .context("failed parsing nested archive-receipt verify json")?,
        Err(error) => {
            let status = refusal_status(
                root,
                session_dir,
                paths,
                Some(resolved),
                LivePackageArchiveReceiptLatestResult::RefusedByDriftedArchiveReceiptContract,
                format!(
                    "failed verifying downstream archive-receipt contract under {}: {error}",
                    paths.nested_archive_receipt_session_dir.display()
                ),
                latest_filing_certificate_plan_step,
                latest_archive_receipt_step,
                archive_receipt_step,
            );
            persist_json(&paths.status_path, &status)?;
            let report = report_from_status(
                "run_latest_archive_receipt",
                verdict_for_result(status.result),
                session_dir,
                paths,
                &session,
                &status,
                Vec::new(),
            );
            persist_json(&paths.report_path, &report)?;
            return Ok(report);
        }
    };
    if provenance_certificate_verify.verdict != ARCHIVE_RECEIPT_VERIFY_OK {
        let status = refusal_status(
            root,
            session_dir,
            paths,
            Some(resolved),
            LivePackageArchiveReceiptLatestResult::RefusedByDriftedArchiveReceiptContract,
            provenance_certificate_verify.reason,
            latest_filing_certificate_plan_step,
            latest_archive_receipt_step,
            archive_receipt_step,
        );
        persist_json(&paths.status_path, &status)?;
        let report = report_from_status(
            "run_latest_archive_receipt",
            verdict_for_result(status.result),
            session_dir,
            paths,
            &session,
            &status,
            Vec::new(),
        );
        persist_json(&paths.report_path, &report)?;
        return Ok(report);
    }

    let result = filing_certificate_latest_result_from_notarization_receipt(
        provenance_certificate_run
            .result
            .as_deref()
            .unwrap_or_default(),
    )
    .unwrap_or(LivePackageArchiveReceiptLatestResult::RefusedByDriftedArchiveReceiptContract);
    let status = PackageArchiveReceiptLatestStatus {
        status_version: ARCHIVE_RECEIPT_LATEST_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        root: root.display().to_string(),
        session_dir: session_dir.display().to_string(),
        latest_top_step_name: Some(resolved.snapshot.latest_top_step_name.clone()),
        latest_top_session_dir: Some(
            resolved
                .snapshot
                .latest_top_session_dir
                .display()
                .to_string(),
        ),
        historical_execute_frozen_session_dir: Some(
            resolved
                .snapshot
                .historical_execute_frozen_session_dir
                .display()
                .to_string(),
        ),
        turn_green_session_dir: Some(
            resolved
                .snapshot
                .turn_green_session_dir
                .display()
                .to_string(),
        ),
        launch_packet_session_dir: Some(
            resolved
                .snapshot
                .launch_packet_session_dir
                .display()
                .to_string(),
        ),
        nested_filing_certificate_latest_session_dir: paths
            .nested_filing_certificate_latest_session_dir
            .display()
            .to_string(),
        nested_archive_receipt_session_dir: paths
            .nested_archive_receipt_session_dir
            .display()
            .to_string(),
        downstream_decision_packet_session_dir: resolved
            .snapshot
            .downstream_decision_packet_session_dir
            .display()
            .to_string(),
        result,
        reason: provenance_certificate_run.reason.clone(),
        latest_filing_certificate_plan_verdict: Some(
            resolved.latest_filing_certificate_plan.verdict.clone(),
        ),
        latest_archive_receipt_verdict: Some(latest_filing_certificate_run.verdict.clone()),
        archive_receipt_verdict: Some(provenance_certificate_run.verdict.clone()),
        current_pre_activation_gate_verdict: provenance_certificate_run
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: provenance_certificate_run
            .current_pre_activation_gate_reason
            .clone(),
        latest_filing_certificate_plan_step,
        latest_archive_receipt_step,
        archive_receipt_step,
        operator_next_action_summary: operator_next_action_summary(result),
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    };
    persist_json(&paths.status_path, &status)?;
    let report = report_from_status(
        "run_latest_archive_receipt",
        verdict_for_result(status.result),
        session_dir,
        paths,
        &session,
        &status,
        Vec::new(),
    );
    persist_json(&paths.report_path, &report)?;
    Ok(report)
}

fn run_latest_filing_certificate_failure(
    root: &Path,
    session_dir: &Path,
    paths: &PackageArchiveReceiptLatestPaths,
    failure: &LatestFilingCertificateResolutionFailure,
) -> Result<PackageArchiveReceiptLatestReport> {
    let session = planned_session(root, session_dir, paths, None, Some(failure));
    persist_json(&paths.session_path, &session)?;
    if let Some(value) = &failure.latest_filing_certificate_plan_raw {
        persist_json(&paths.latest_filing_certificate_plan_report_path, value)?;
    }
    let status = refusal_status_from_resolution_failure(root, session_dir, paths, failure);
    persist_json(&paths.status_path, &status)?;
    let report = report_from_status(
        "run_latest_archive_receipt",
        verdict_from_failure_kind(failure.kind),
        session_dir,
        paths,
        &session,
        &status,
        Vec::new(),
    );
    persist_json(&paths.report_path, &report)?;
    Ok(report)
}

fn resolve_latest_filing_certificate(
    root: &Path,
    paths: &PackageArchiveReceiptLatestPaths,
) -> std::result::Result<ResolvedLatestFilingCertificate, LatestFilingCertificateResolutionFailure>
{
    let latest_filing_certificate_plan_raw = match run_json_command(
        FILING_CERTIFICATE_LATEST_BIN,
        &latest_filing_certificate_plan_args(
            root,
            &paths.nested_filing_certificate_latest_session_dir,
        ),
    ) {
        Ok(value) => value,
        Err(error) => {
            return Err(LatestFilingCertificateResolutionFailure {
                kind: ResolutionFailureKind::MissingLatestChain,
                reason: format!(
                    "failed resolving latest immutable chain under {}: {error}",
                    root.display()
                ),
                snapshot: None,
                latest_filing_certificate_plan_raw: None,
                latest_filing_certificate_plan_verdict: None,
                latest_filing_certificate_plan_reason: None,
                latest_filing_certificate_plan_generated_at: None,
            });
        }
    };
    let latest_filing_certificate_plan: FilingCertificateLatestReportView =
        serde_json::from_value(latest_filing_certificate_plan_raw.clone()).map_err(|error| {
            LatestFilingCertificateResolutionFailure {
                kind: ResolutionFailureKind::InvalidLatestChain,
                reason: format!(
                    "failed parsing latest-filing-certificate plan json for {}: {error}",
                    root.display()
                ),
                snapshot: None,
                latest_filing_certificate_plan_raw: Some(
                    latest_filing_certificate_plan_raw.clone(),
                ),
                latest_filing_certificate_plan_verdict: None,
                latest_filing_certificate_plan_reason: None,
                latest_filing_certificate_plan_generated_at: None,
            }
        })?;

    let snapshot =
        parse_snapshot_from_filing_certificate_latest(&latest_filing_certificate_plan).ok();
    if latest_filing_certificate_plan.verdict != FILING_CERTIFICATE_LATEST_PLAN_READY {
        return Err(LatestFilingCertificateResolutionFailure {
            kind: map_filing_certificate_latest_failure_kind(
                &latest_filing_certificate_plan.verdict,
            ),
            reason: latest_filing_certificate_plan.reason.clone(),
            snapshot,
            latest_filing_certificate_plan_raw: Some(latest_filing_certificate_plan_raw),
            latest_filing_certificate_plan_verdict: Some(latest_filing_certificate_plan.verdict),
            latest_filing_certificate_plan_reason: Some(latest_filing_certificate_plan.reason),
            latest_filing_certificate_plan_generated_at: Some(
                latest_filing_certificate_plan.generated_at,
            ),
        });
    }

    let snapshot = parse_snapshot_from_filing_certificate_latest(&latest_filing_certificate_plan)
        .map_err(|error| LatestFilingCertificateResolutionFailure {
        kind: ResolutionFailureKind::InvalidLatestChain,
        reason: error.to_string(),
        snapshot: None,
        latest_filing_certificate_plan_raw: Some(latest_filing_certificate_plan_raw.clone()),
        latest_filing_certificate_plan_verdict: Some(
            latest_filing_certificate_plan.verdict.clone(),
        ),
        latest_filing_certificate_plan_reason: Some(latest_filing_certificate_plan.reason.clone()),
        latest_filing_certificate_plan_generated_at: Some(
            latest_filing_certificate_plan.generated_at,
        ),
    })?;

    let mut mismatches = Vec::new();
    compare_filing_certificate_latest_plan_against_snapshot(
        &latest_filing_certificate_plan,
        &snapshot,
        &paths.nested_filing_certificate_latest_session_dir,
        &mut mismatches,
    );
    if !mismatches.is_empty() {
        return Err(LatestFilingCertificateResolutionFailure {
            kind: ResolutionFailureKind::DriftedRegistryEntryContract,
            reason: mismatches[0].clone(),
            snapshot: Some(snapshot),
            latest_filing_certificate_plan_raw: Some(latest_filing_certificate_plan_raw),
            latest_filing_certificate_plan_verdict: Some(latest_filing_certificate_plan.verdict),
            latest_filing_certificate_plan_reason: Some(latest_filing_certificate_plan.reason),
            latest_filing_certificate_plan_generated_at: Some(
                latest_filing_certificate_plan.generated_at,
            ),
        });
    }

    Ok(ResolvedLatestFilingCertificate {
        snapshot,
        latest_filing_certificate_plan_raw,
        latest_filing_certificate_plan,
    })
}

fn parse_snapshot_from_filing_certificate_latest(
    view: &FilingCertificateLatestReportView,
) -> Result<LatestFilingCertificateSnapshot> {
    let latest_top_step_name =
        required_option_string(view.latest_top_step_name.as_ref(), "latest_top_step_name")?;
    if latest_top_step_name != TOP_ACCEPTED_PACKAGE_STEP {
        bail!(
            "latest-filing-certificate plan top accepted step is {} instead of {}",
            latest_top_step_name,
            TOP_ACCEPTED_PACKAGE_STEP
        );
    }
    if view.activation_authorized {
        bail!(
            "latest-filing-certificate plan must remain planning-safe with activation_authorized=false"
        );
    }
    Ok(LatestFilingCertificateSnapshot {
        latest_top_step_name,
        latest_top_session_dir: PathBuf::from(required_option_string(
            view.latest_top_session_dir.as_ref(),
            "latest_top_session_dir",
        )?),
        downstream_decision_packet_session_dir: PathBuf::from(required_option_string(
            view.downstream_decision_packet_session_dir.as_ref(),
            "downstream_decision_packet_session_dir",
        )?),
        historical_execute_frozen_session_dir: PathBuf::from(required_option_string(
            view.historical_execute_frozen_session_dir.as_ref(),
            "historical_execute_frozen_session_dir",
        )?),
        turn_green_session_dir: PathBuf::from(required_option_string(
            view.turn_green_session_dir.as_ref(),
            "turn_green_session_dir",
        )?),
        launch_packet_session_dir: PathBuf::from(required_option_string(
            view.launch_packet_session_dir.as_ref(),
            "launch_packet_session_dir",
        )?),
        package_dir: PathBuf::from(required_option_string(
            view.package_dir.as_ref(),
            "package_dir",
        )?),
        install_root: PathBuf::from(required_option_string(
            view.install_root.as_ref(),
            "install_root",
        )?),
        target_service_name: required_option_string(
            view.target_service_name.as_ref(),
            "target_service_name",
        )?,
        backend_command: required_option_string(view.backend_command.as_ref(), "backend_command")?,
        wrapper_timeout_ms: view.wrapper_timeout_ms.ok_or_else(|| {
            anyhow!("latest-filing-certificate plan is missing wrapper_timeout_ms")
        })?,
        service_status_max_staleness_ms: view.service_status_max_staleness_ms.ok_or_else(|| {
            anyhow!("latest-filing-certificate plan is missing service_status_max_staleness_ms")
        })?,
        reviewed_frozen_live_cutover_controller_command_summary: required_option_string(
            view.reviewed_frozen_live_cutover_controller_command_summary
                .as_ref(),
            "reviewed_frozen_live_cutover_controller_command_summary",
        )?,
        clerestory_certificate_summary: required_option_string(
            view.clerestory_certificate_summary.as_ref(),
            "clerestory_certificate_summary",
        )?,
        clerestory_certificate_sha256: required_option_string(
            view.clerestory_certificate_sha256.as_ref(),
            "clerestory_certificate_sha256",
        )?,
        clerestory_certificate_algorithm: required_option_string(
            view.clerestory_certificate_algorithm.as_ref(),
            "clerestory_certificate_algorithm",
        )?,
    })
}

fn plan_report_from_resolution(
    mode: &str,
    verdict: TinyLivePackageArchiveReceiptLatestVerdict,
    reason: String,
    root: &Path,
    session_dir: &Path,
    paths: &PackageArchiveReceiptLatestPaths,
    resolved: &ResolvedLatestFilingCertificate,
) -> PackageArchiveReceiptLatestReport {
    PackageArchiveReceiptLatestReport {
        generated_at: Utc::now(),
        mode: canonical_wrapper_mode(mode).to_string(),
        verdict,
        reason,
        root: Some(root.display().to_string()),
        latest_top_step_name: Some(resolved.snapshot.latest_top_step_name.clone()),
        latest_top_session_dir: Some(
            resolved
                .snapshot
                .latest_top_session_dir
                .display()
                .to_string(),
        ),
        historical_execute_frozen_session_dir: Some(
            resolved
                .snapshot
                .historical_execute_frozen_session_dir
                .display()
                .to_string(),
        ),
        turn_green_session_dir: Some(
            resolved
                .snapshot
                .turn_green_session_dir
                .display()
                .to_string(),
        ),
        launch_packet_session_dir: Some(
            resolved
                .snapshot
                .launch_packet_session_dir
                .display()
                .to_string(),
        ),
        package_dir: Some(resolved.snapshot.package_dir.display().to_string()),
        install_root: Some(resolved.snapshot.install_root.display().to_string()),
        target_service_name: Some(resolved.snapshot.target_service_name.clone()),
        backend_command: Some(resolved.snapshot.backend_command.clone()),
        wrapper_timeout_ms: Some(resolved.snapshot.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(resolved.snapshot.service_status_max_staleness_ms),
        session_dir: Some(session_dir.display().to_string()),
        archive_receipt_latest_session_path: Some(paths.session_path.display().to_string()),
        archive_receipt_latest_status_path: Some(paths.status_path.display().to_string()),
        archive_receipt_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_filing_certificate_plan_report_path: Some(
            paths
                .latest_filing_certificate_plan_report_path
                .display()
                .to_string(),
        ),
        latest_archive_receipt_report_path: Some(
            paths
                .latest_archive_receipt_report_path
                .display()
                .to_string(),
        ),
        archive_receipt_report_path: Some(paths.archive_receipt_report_path.display().to_string()),
        nested_filing_certificate_latest_session_dir: Some(
            paths
                .nested_filing_certificate_latest_session_dir
                .display()
                .to_string(),
        ),
        nested_archive_receipt_session_dir: Some(
            paths
                .nested_archive_receipt_session_dir
                .display()
                .to_string(),
        ),
        downstream_decision_packet_session_dir: Some(
            resolved
                .snapshot
                .downstream_decision_packet_session_dir
                .display()
                .to_string(),
        ),
        latest_filing_certificate_plan_verdict: Some(
            resolved.latest_filing_certificate_plan.verdict.clone(),
        ),
        latest_archive_receipt_verdict: None,
        archive_receipt_verdict: None,
        result: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        resolve_latest_filing_certificate_command_summary: Some(
            resolve_latest_filing_certificate_command_summary(
                root,
                &paths.nested_filing_certificate_latest_session_dir,
            ),
        ),
        run_latest_archive_receipt_command_summary: Some(
            run_latest_archive_receipt_command_summary(
                root,
                &paths.nested_filing_certificate_latest_session_dir,
            ),
        ),
        verify_latest_archive_receipt_command_summary: Some(
            verify_latest_archive_receipt_command_summary(
                &paths.nested_filing_certificate_latest_session_dir,
            ),
        ),
        downstream_archive_receipt_command_summary: Some(
            downstream_archive_receipt_command_summary(
                &resolved.snapshot,
                &expected_downstream_filing_certificate_session_dir(paths),
                &paths.nested_archive_receipt_session_dir,
            ),
        ),
        verify_archive_receipt_command_summary: Some(verify_archive_receipt_command_summary(
            &resolved.snapshot,
            &expected_downstream_filing_certificate_session_dir(paths),
            &paths.nested_archive_receipt_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            resolved
                .snapshot
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        clerestory_certificate_summary: Some(
            resolved.snapshot.clerestory_certificate_summary.clone(),
        ),
        clerestory_certificate_sha256: Some(
            resolved.snapshot.clerestory_certificate_sha256.clone(),
        ),
        clerestory_certificate_algorithm: Some(
            resolved.snapshot.clerestory_certificate_algorithm.clone(),
        ),
        verification_mismatches: Vec::new(),
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn report_from_resolution_failure(
    mode: &str,
    root: &Path,
    session_dir: &Path,
    paths: &PackageArchiveReceiptLatestPaths,
    failure: &LatestFilingCertificateResolutionFailure,
) -> PackageArchiveReceiptLatestReport {
    let snapshot = failure.snapshot.as_ref();
    PackageArchiveReceiptLatestReport {
        generated_at: Utc::now(),
        mode: canonical_wrapper_mode(mode).to_string(),
        verdict: verdict_from_failure_kind(failure.kind),
        reason: failure.reason.clone(),
        root: Some(root.display().to_string()),
        latest_top_step_name: snapshot.map(|value| value.latest_top_step_name.clone()),
        latest_top_session_dir: snapshot
            .map(|value| value.latest_top_session_dir.display().to_string()),
        historical_execute_frozen_session_dir: snapshot.map(|value| {
            value
                .historical_execute_frozen_session_dir
                .display()
                .to_string()
        }),
        turn_green_session_dir: snapshot
            .map(|value| value.turn_green_session_dir.display().to_string()),
        launch_packet_session_dir: snapshot
            .map(|value| value.launch_packet_session_dir.display().to_string()),
        package_dir: snapshot.map(|value| value.package_dir.display().to_string()),
        install_root: snapshot.map(|value| value.install_root.display().to_string()),
        target_service_name: snapshot.map(|value| value.target_service_name.clone()),
        backend_command: snapshot.map(|value| value.backend_command.clone()),
        wrapper_timeout_ms: snapshot.map(|value| value.wrapper_timeout_ms),
        service_status_max_staleness_ms: snapshot
            .map(|value| value.service_status_max_staleness_ms),
        session_dir: Some(session_dir.display().to_string()),
        archive_receipt_latest_session_path: Some(paths.session_path.display().to_string()),
        archive_receipt_latest_status_path: Some(paths.status_path.display().to_string()),
        archive_receipt_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_filing_certificate_plan_report_path: Some(
            paths
                .latest_filing_certificate_plan_report_path
                .display()
                .to_string(),
        ),
        latest_archive_receipt_report_path: Some(
            paths
                .latest_archive_receipt_report_path
                .display()
                .to_string(),
        ),
        archive_receipt_report_path: Some(paths.archive_receipt_report_path.display().to_string()),
        nested_filing_certificate_latest_session_dir: Some(
            paths
                .nested_filing_certificate_latest_session_dir
                .display()
                .to_string(),
        ),
        nested_archive_receipt_session_dir: Some(
            paths
                .nested_archive_receipt_session_dir
                .display()
                .to_string(),
        ),
        downstream_decision_packet_session_dir: snapshot.map(|value| {
            value
                .downstream_decision_packet_session_dir
                .display()
                .to_string()
        }),
        latest_filing_certificate_plan_verdict: failure
            .latest_filing_certificate_plan_verdict
            .clone(),
        latest_archive_receipt_verdict: None,
        archive_receipt_verdict: None,
        result: Some(serialize_enum(&result_from_failure_kind(failure.kind))),
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        resolve_latest_filing_certificate_command_summary: Some(
            resolve_latest_filing_certificate_command_summary(
                root,
                &paths.nested_filing_certificate_latest_session_dir,
            ),
        ),
        run_latest_archive_receipt_command_summary: Some(
            run_latest_archive_receipt_command_summary(
                root,
                &paths.nested_filing_certificate_latest_session_dir,
            ),
        ),
        verify_latest_archive_receipt_command_summary: Some(
            verify_latest_archive_receipt_command_summary(
                &paths.nested_filing_certificate_latest_session_dir,
            ),
        ),
        downstream_archive_receipt_command_summary: snapshot.map(|value| {
            downstream_archive_receipt_command_summary(
                value,
                &expected_downstream_filing_certificate_session_dir(paths),
                &paths.nested_archive_receipt_session_dir,
            )
        }),
        verify_archive_receipt_command_summary: snapshot.map(|value| {
            verify_archive_receipt_command_summary(
                value,
                &expected_downstream_filing_certificate_session_dir(paths),
                &paths.nested_archive_receipt_session_dir,
            )
        }),
        reviewed_frozen_live_cutover_controller_command_summary: snapshot.map(|value| {
            value
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone()
        }),
        clerestory_certificate_summary: snapshot
            .map(|value| value.clerestory_certificate_summary.clone()),
        clerestory_certificate_sha256: snapshot
            .map(|value| value.clerestory_certificate_sha256.clone()),
        clerestory_certificate_algorithm: snapshot
            .map(|value| value.clerestory_certificate_algorithm.clone()),
        verification_mismatches: Vec::new(),
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn planned_session(
    root: &Path,
    session_dir: &Path,
    paths: &PackageArchiveReceiptLatestPaths,
    resolved: Option<&ResolvedLatestFilingCertificate>,
    failure: Option<&LatestFilingCertificateResolutionFailure>,
) -> PackageArchiveReceiptLatestSession {
    let snapshot = resolved
        .map(|value| &value.snapshot)
        .or_else(|| failure.and_then(|value| value.snapshot.as_ref()));
    PackageArchiveReceiptLatestSession {
        session_version: ARCHIVE_RECEIPT_LATEST_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        root: root.display().to_string(),
        session_dir: session_dir.display().to_string(),
        nested_filing_certificate_latest_session_dir: paths
            .nested_filing_certificate_latest_session_dir
            .display()
            .to_string(),
        nested_archive_receipt_session_dir: paths
            .nested_archive_receipt_session_dir
            .display()
            .to_string(),
        downstream_decision_packet_session_dir: snapshot
            .map(|value| {
                value
                    .downstream_decision_packet_session_dir
                    .display()
                    .to_string()
            })
            .unwrap_or_else(|| {
                expected_downstream_decision_packet_session_dir(paths)
                    .display()
                    .to_string()
            }),
        latest_top_step_name: snapshot.map(|value| value.latest_top_step_name.clone()),
        latest_top_session_dir: snapshot
            .map(|value| value.latest_top_session_dir.display().to_string()),
        historical_execute_frozen_session_dir: snapshot.map(|value| {
            value
                .historical_execute_frozen_session_dir
                .display()
                .to_string()
        }),
        turn_green_session_dir: snapshot
            .map(|value| value.turn_green_session_dir.display().to_string()),
        launch_packet_session_dir: snapshot
            .map(|value| value.launch_packet_session_dir.display().to_string()),
        package_dir: snapshot.map(|value| value.package_dir.display().to_string()),
        install_root: snapshot.map(|value| value.install_root.display().to_string()),
        target_service_name: snapshot.map(|value| value.target_service_name.clone()),
        backend_command: snapshot.map(|value| value.backend_command.clone()),
        wrapper_timeout_ms: snapshot.map(|value| value.wrapper_timeout_ms),
        service_status_max_staleness_ms: snapshot
            .map(|value| value.service_status_max_staleness_ms),
        resolve_latest_filing_certificate_command_summary:
            resolve_latest_filing_certificate_command_summary(
                root,
                &paths.nested_filing_certificate_latest_session_dir,
            ),
        run_latest_archive_receipt_command_summary: run_latest_archive_receipt_command_summary(
            root,
            &paths.nested_filing_certificate_latest_session_dir,
        ),
        verify_latest_archive_receipt_command_summary:
            verify_latest_archive_receipt_command_summary(
                &paths.nested_filing_certificate_latest_session_dir,
            ),
        downstream_archive_receipt_command_summary: snapshot
            .map(|value| {
                downstream_archive_receipt_command_summary(
                    value,
                    &expected_downstream_filing_certificate_session_dir(paths),
                    &paths.nested_archive_receipt_session_dir,
                )
            })
            .unwrap_or_else(|| "<attestation-seal-run-unavailable>".to_string()),
        verify_archive_receipt_command_summary: snapshot
            .map(|value| {
                verify_archive_receipt_command_summary(
                    value,
                    &expected_downstream_filing_certificate_session_dir(paths),
                    &paths.nested_archive_receipt_session_dir,
                )
            })
            .unwrap_or_else(|| "<attestation-seal-verify-unavailable>".to_string()),
        reviewed_frozen_live_cutover_controller_command_summary: snapshot.map(|value| {
            value
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone()
        }),
        clerestory_certificate_summary: snapshot
            .map(|value| value.clerestory_certificate_summary.clone()),
        clerestory_certificate_sha256: snapshot
            .map(|value| value.clerestory_certificate_sha256.clone()),
        clerestory_certificate_algorithm: snapshot
            .map(|value| value.clerestory_certificate_algorithm.clone()),
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn report_from_status(
    mode: &str,
    verdict: TinyLivePackageArchiveReceiptLatestVerdict,
    session_dir: &Path,
    paths: &PackageArchiveReceiptLatestPaths,
    session: &PackageArchiveReceiptLatestSession,
    status: &PackageArchiveReceiptLatestStatus,
    verification_mismatches: Vec<String>,
) -> PackageArchiveReceiptLatestReport {
    PackageArchiveReceiptLatestReport {
        generated_at: Utc::now(),
        mode: canonical_wrapper_mode(mode).to_string(),
        verdict,
        reason: status.reason.clone(),
        root: Some(session.root.clone()),
        latest_top_step_name: status.latest_top_step_name.clone(),
        latest_top_session_dir: status.latest_top_session_dir.clone(),
        historical_execute_frozen_session_dir: status.historical_execute_frozen_session_dir.clone(),
        turn_green_session_dir: status.turn_green_session_dir.clone(),
        launch_packet_session_dir: status.launch_packet_session_dir.clone(),
        package_dir: session.package_dir.clone(),
        install_root: session.install_root.clone(),
        target_service_name: session.target_service_name.clone(),
        backend_command: session.backend_command.clone(),
        wrapper_timeout_ms: session.wrapper_timeout_ms,
        service_status_max_staleness_ms: session.service_status_max_staleness_ms,
        session_dir: Some(session_dir.display().to_string()),
        archive_receipt_latest_session_path: Some(paths.session_path.display().to_string()),
        archive_receipt_latest_status_path: Some(paths.status_path.display().to_string()),
        archive_receipt_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_filing_certificate_plan_report_path: Some(
            paths
                .latest_filing_certificate_plan_report_path
                .display()
                .to_string(),
        ),
        latest_archive_receipt_report_path: Some(
            paths
                .latest_archive_receipt_report_path
                .display()
                .to_string(),
        ),
        archive_receipt_report_path: Some(paths.archive_receipt_report_path.display().to_string()),
        nested_filing_certificate_latest_session_dir: Some(
            session.nested_filing_certificate_latest_session_dir.clone(),
        ),
        nested_archive_receipt_session_dir: Some(
            session.nested_archive_receipt_session_dir.clone(),
        ),
        downstream_decision_packet_session_dir: Some(
            session.downstream_decision_packet_session_dir.clone(),
        ),
        latest_filing_certificate_plan_verdict: status
            .latest_filing_certificate_plan_verdict
            .clone(),
        latest_archive_receipt_verdict: status.latest_archive_receipt_verdict.clone(),
        archive_receipt_verdict: status.archive_receipt_verdict.clone(),
        result: Some(serialize_enum(&status.result)),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        resolve_latest_filing_certificate_command_summary: Some(
            session
                .resolve_latest_filing_certificate_command_summary
                .clone(),
        ),
        run_latest_archive_receipt_command_summary: Some(
            session.run_latest_archive_receipt_command_summary.clone(),
        ),
        verify_latest_archive_receipt_command_summary: Some(
            session
                .verify_latest_archive_receipt_command_summary
                .clone(),
        ),
        downstream_archive_receipt_command_summary: Some(
            session.downstream_archive_receipt_command_summary.clone(),
        ),
        verify_archive_receipt_command_summary: Some(
            session.verify_archive_receipt_command_summary.clone(),
        ),
        reviewed_frozen_live_cutover_controller_command_summary: session
            .reviewed_frozen_live_cutover_controller_command_summary
            .clone(),
        clerestory_certificate_summary: session.clerestory_certificate_summary.clone(),
        clerestory_certificate_sha256: session.clerestory_certificate_sha256.clone(),
        clerestory_certificate_algorithm: session.clerestory_certificate_algorithm.clone(),
        verification_mismatches,
        activation_authorized: status.activation_authorized,
        explicit_statement: session.explicit_statement.clone(),
    }
}

fn filing_certificate_latest_paths(session_dir: &Path) -> PackageArchiveReceiptLatestPaths {
    PackageArchiveReceiptLatestPaths {
        session_path: session_dir
            .join("tiny_live_activation_package_archive_receipt_latest.session.json"),
        status_path: session_dir
            .join("tiny_live_activation_package_archive_receipt_latest.status.json"),
        report_path: session_dir
            .join("tiny_live_activation_package_archive_receipt_latest.report.json"),
        latest_filing_certificate_plan_report_path: session_dir.join(
            "tiny_live_activation_package_archive_receipt_latest.filing_certificate_latest_plan.report.json",
        ),
        latest_archive_receipt_report_path: session_dir.join(
            "tiny_live_activation_package_archive_receipt_latest.filing_certificate_latest.report.json",
        ),
        archive_receipt_report_path: session_dir.join(
            "tiny_live_activation_package_archive_receipt_latest.archive_receipt.report.json",
        ),
        nested_filing_certificate_latest_session_dir: session_dir
            .join("tiny_live_activation_package_archive_receipt_latest.filing_certificate_latest_session"),
        nested_archive_receipt_session_dir: session_dir.join(
            "tiny_live_activation_package_archive_receipt_latest.archive_receipt_session",
        ),
    }
}

fn expected_downstream_decision_packet_session_dir(
    paths: &PackageArchiveReceiptLatestPaths,
) -> PathBuf {
    paths
        .nested_filing_certificate_latest_session_dir
        .join("tiny_live_activation_package_filing_certificate_latest.decision_packet_session")
}

fn expected_downstream_filing_certificate_session_dir(
    paths: &PackageArchiveReceiptLatestPaths,
) -> PathBuf {
    paths
        .nested_filing_certificate_latest_session_dir
        .join("tiny_live_activation_package_filing_certificate_latest.filing_certificate_session")
}

fn expected_downstream_review_receipt_session_dir(
    paths: &PackageArchiveReceiptLatestPaths,
) -> PathBuf {
    paths
        .nested_filing_certificate_latest_session_dir
        .join("tiny_live_activation_package_filing_certificate_latest.review_receipt_session")
}

fn latest_filing_certificate_plan_args(root: &Path, session_dir: &Path) -> Vec<String> {
    vec![
        "--root".to_string(),
        root.display().to_string(),
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--plan-latest-filing-certificate".to_string(),
        "--json".to_string(),
    ]
}

fn latest_filing_certificate_run_args(root: &Path, session_dir: &Path) -> Vec<String> {
    vec![
        "--root".to_string(),
        root.display().to_string(),
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--run-latest-filing-certificate".to_string(),
        "--json".to_string(),
    ]
}

fn latest_archive_receipt_verify_args(session_dir: &Path) -> Vec<String> {
    vec![
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--verify-latest-filing-certificate".to_string(),
        "--json".to_string(),
    ]
}

fn registry_entry_run_args(
    snapshot: &LatestFilingCertificateSnapshot,
    filing_certificate_session_dir: &Path,
    session_dir: &Path,
) -> Vec<String> {
    vec![
        "--filing-certificate-session-dir".to_string(),
        filing_certificate_session_dir.display().to_string(),
        "--confirm-decision-packet-session-dir".to_string(),
        snapshot
            .downstream_decision_packet_session_dir
            .display()
            .to_string(),
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--run-live-package-archive-receipt".to_string(),
        "--json".to_string(),
    ]
}

fn archive_receipt_verify_args(
    snapshot: &LatestFilingCertificateSnapshot,
    filing_certificate_session_dir: &Path,
    session_dir: &Path,
) -> Vec<String> {
    vec![
        "--filing-certificate-session-dir".to_string(),
        filing_certificate_session_dir.display().to_string(),
        "--confirm-decision-packet-session-dir".to_string(),
        snapshot
            .downstream_decision_packet_session_dir
            .display()
            .to_string(),
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--verify-live-package-archive-receipt".to_string(),
        "--json".to_string(),
    ]
}

fn resolve_latest_filing_certificate_command_summary(root: &Path, session_dir: &Path) -> String {
    format!(
        "{FILING_CERTIFICATE_LATEST_BIN} --root {} --session-dir {} --plan-latest-filing-certificate --json",
        shell_escape_path(root),
        shell_escape_path(session_dir),
    )
}

fn run_latest_archive_receipt_command_summary(root: &Path, session_dir: &Path) -> String {
    format!(
        "{FILING_CERTIFICATE_LATEST_BIN} --root {} --session-dir {} --run-latest-filing-certificate --json",
        shell_escape_path(root),
        shell_escape_path(session_dir),
    )
}

fn verify_latest_archive_receipt_command_summary(session_dir: &Path) -> String {
    format!(
        "{FILING_CERTIFICATE_LATEST_BIN} --session-dir {} --verify-latest-filing-certificate --json",
        shell_escape_path(session_dir),
    )
}

fn downstream_archive_receipt_command_summary(
    snapshot: &LatestFilingCertificateSnapshot,
    filing_certificate_session_dir: &Path,
    session_dir: &Path,
) -> String {
    format!(
        "{ARCHIVE_RECEIPT_BIN} --filing-certificate-session-dir {} --confirm-decision-packet-session-dir {} --session-dir {} --run-live-package-archive-receipt --json",
        shell_escape_path(filing_certificate_session_dir),
        shell_escape_path(&snapshot.downstream_decision_packet_session_dir),
        shell_escape_path(session_dir),
    )
}

fn verify_archive_receipt_command_summary(
    snapshot: &LatestFilingCertificateSnapshot,
    filing_certificate_session_dir: &Path,
    session_dir: &Path,
) -> String {
    format!(
        "{ARCHIVE_RECEIPT_BIN} --filing-certificate-session-dir {} --confirm-decision-packet-session-dir {} --session-dir {} --verify-live-package-archive-receipt --json",
        shell_escape_path(filing_certificate_session_dir),
        shell_escape_path(&snapshot.downstream_decision_packet_session_dir),
        shell_escape_path(session_dir),
    )
}

fn nested_filing_certificate_latest_report_path(session_dir: &Path) -> PathBuf {
    session_dir.join("tiny_live_activation_package_filing_certificate_latest.report.json")
}

fn nested_archive_receipt_session_path(session_dir: &Path) -> PathBuf {
    session_dir.join("tiny_live_activation_package_archive_receipt.session.json")
}

fn nested_archive_receipt_status_path(session_dir: &Path) -> PathBuf {
    session_dir.join("tiny_live_activation_package_archive_receipt.status.json")
}

fn set_mode(mode: &mut Option<Mode>, value: Mode, flag: &str) -> Result<()> {
    if mode.replace(value).is_some() {
        bail!("multiple modes specified; latest was {flag}");
    }
    Ok(())
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    value.ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn run_json_command(binary: &str, args: &[String]) -> Result<Value> {
    let output = Command::new(binary)
        .args(args)
        .output()
        .with_context(|| format!("failed spawning {binary}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("{binary} exited with {}: {}", output.status, stderr.trim());
    }
    serde_json::from_slice(&output.stdout)
        .with_context(|| format!("failed parsing JSON output from {binary}"))
}

fn shell_escape_path(path: &Path) -> String {
    shell_escape_string(&path.display().to_string())
}

fn shell_escape_string(value: &str) -> String {
    if value
        .chars()
        .all(|raw| raw.is_ascii_alphanumeric() || "/._-".contains(raw))
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\"'\"'"))
    }
}

fn create_clean_session_dir(session_dir: &Path, label: &str) -> Result<()> {
    fs::create_dir_all(session_dir).with_context(|| {
        format!(
            "failed creating {label} session dir {}",
            session_dir.display()
        )
    })
}

fn write_script(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, contents).with_context(|| {
        format!(
            "failed writing latest-archive-receipt script to {}",
            path.display()
        )
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(path)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms)?;
    }
    Ok(())
}

fn persist_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_vec_pretty(value)?)
        .with_context(|| format!("failed writing {}", path.display()))
}

fn load_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let bytes = fs::read(path).with_context(|| format!("failed reading {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("failed parsing {}", path.display()))
}

fn step_artifact(
    path: &Path,
    mode: &str,
    verdict: &str,
    reason: &str,
    generated_at: DateTime<Utc>,
) -> PackageArchiveReceiptLatestStepArtifact {
    PackageArchiveReceiptLatestStepArtifact {
        path: path.display().to_string(),
        mode: mode.to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        generated_at,
    }
}

fn canonical_wrapper_mode(mode: &str) -> &str {
    match mode {
        "plan_latest_archive_receipt" => "plan_latest_archive_receipt",
        "render_latest_archive_receipt_script" => "render_latest_archive_receipt_script",
        "run_latest_archive_receipt" => "run_latest_archive_receipt",
        "verify_latest_archive_receipt" => "verify_latest_archive_receipt",
        other => other,
    }
}

fn load_required_step_json<T: for<'de> Deserialize<'de>>(
    step: &Option<PackageArchiveReceiptLatestStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<T> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("{label} is missing from latest-archive-receipt status"))?;
    compare_string(
        &step.path,
        &expected_path.display().to_string(),
        &format!("{label} path"),
        mismatches,
    );
    load_json(expected_path)
}

fn compare_string(actual: &str, expected: &str, label: &str, mismatches: &mut Vec<String>) {
    if actual != expected {
        mismatches.push(format!(
            "{label} {actual:?} does not match expected {expected:?}"
        ));
    }
}

fn compare_option_string(
    actual: Option<&str>,
    expected: Option<&str>,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    if actual != expected {
        mismatches.push(format!(
            "{label} {:?} does not match expected {:?}",
            actual, expected
        ));
    }
}

fn compare_option_u64(
    actual: Option<u64>,
    expected: Option<u64>,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    if actual != expected {
        mismatches.push(format!(
            "{label} {:?} does not match expected {:?}",
            actual, expected
        ));
    }
}

fn compare_option_usize(
    actual: Option<usize>,
    expected: Option<usize>,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    if actual != expected {
        mismatches.push(format!(
            "{label} {:?} does not match expected {:?}",
            actual, expected
        ));
    }
}

fn compare_option_json(
    actual: Option<&Value>,
    expected: Option<&Value>,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    if actual != expected {
        mismatches.push(format!(
            "{label} {:?} does not match expected {:?}",
            actual, expected
        ));
    }
}

fn compare_step_artifact_against_report_metadata(
    step: Option<&PackageArchiveReceiptLatestStepArtifact>,
    expected_path: &Path,
    expected_mode: &str,
    expected_verdict: &str,
    expected_reason: &str,
    expected_generated_at: DateTime<Utc>,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    let Some(step) = step else {
        mismatches.push(format!(
            "{label} is missing from latest-archive-receipt status"
        ));
        return;
    };
    compare_string(
        &step.path,
        &expected_path.display().to_string(),
        &format!("{label} path"),
        mismatches,
    );
    compare_string(
        &step.mode,
        expected_mode,
        &format!("{label} mode"),
        mismatches,
    );
    compare_string(
        &step.verdict,
        expected_verdict,
        &format!("{label} verdict"),
        mismatches,
    );
    compare_string(
        &step.reason,
        expected_reason,
        &format!("{label} reason"),
        mismatches,
    );
    if step.generated_at != expected_generated_at {
        mismatches.push(format!(
            "{label} generated_at {:?} does not match expected {:?}",
            step.generated_at, expected_generated_at
        ));
    }
}

fn compare_filing_certificate_latest_plan_against_snapshot(
    stored: &FilingCertificateLatestReportView,
    snapshot: &LatestFilingCertificateSnapshot,
    session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        Some(stored.verdict.as_str()),
        Some(FILING_CERTIFICATE_LATEST_PLAN_READY),
        "stored latest-filing-certificate plan verdict",
        mismatches,
    );
    compare_string(
        &stored.mode,
        "plan_latest_filing_certificate",
        "stored latest-filing-certificate plan mode",
        mismatches,
    );
    compare_option_string(
        stored.latest_top_step_name.as_deref(),
        Some(snapshot.latest_top_step_name.as_str()),
        "stored latest-filing-certificate plan latest_top_step_name",
        mismatches,
    );
    compare_option_string(
        stored.latest_top_session_dir.as_deref(),
        Some(
            snapshot
                .latest_top_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored latest-filing-certificate plan latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored latest-filing-certificate plan session_dir",
        mismatches,
    );
    compare_option_string(
        stored.downstream_decision_packet_session_dir.as_deref(),
        Some(
            snapshot
                .downstream_decision_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored latest-filing-certificate plan downstream_decision_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.package_dir.as_deref(),
        Some(snapshot.package_dir.display().to_string().as_str()),
        "stored latest-filing-certificate plan package_dir",
        mismatches,
    );
    compare_option_string(
        stored.install_root.as_deref(),
        Some(snapshot.install_root.display().to_string().as_str()),
        "stored latest-filing-certificate plan install_root",
        mismatches,
    );
    compare_option_string(
        stored.target_service_name.as_deref(),
        Some(snapshot.target_service_name.as_str()),
        "stored latest-filing-certificate plan target_service_name",
        mismatches,
    );
    compare_option_string(
        stored.backend_command.as_deref(),
        Some(snapshot.backend_command.as_str()),
        "stored latest-filing-certificate plan backend_command",
        mismatches,
    );
    compare_option_u64(
        stored.wrapper_timeout_ms,
        Some(snapshot.wrapper_timeout_ms),
        "stored latest-filing-certificate plan wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        stored.service_status_max_staleness_ms,
        Some(snapshot.service_status_max_staleness_ms),
        "stored latest-filing-certificate plan service_status_max_staleness_ms",
        mismatches,
    );
    compare_option_string(
        stored
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        Some(
            snapshot
                .reviewed_frozen_live_cutover_controller_command_summary
                .as_str(),
        ),
        "stored latest-filing-certificate plan reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_summary.as_deref(),
        Some(snapshot.clerestory_certificate_summary.as_str()),
        "stored latest-filing-certificate plan clerestory_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_sha256.as_deref(),
        Some(snapshot.clerestory_certificate_sha256.as_str()),
        "stored latest-filing-certificate plan clerestory_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_algorithm.as_deref(),
        Some(snapshot.clerestory_certificate_algorithm.as_str()),
        "stored latest-filing-certificate plan clerestory_certificate_algorithm",
        mismatches,
    );
}

fn compare_filing_certificate_latest_run_against_snapshot(
    stored: &FilingCertificateLatestReportView,
    snapshot: &LatestFilingCertificateSnapshot,
    session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &stored.mode,
        "run_latest_filing_certificate",
        "stored latest-filing-certificate run mode",
        mismatches,
    );
    compare_option_string(
        stored.latest_top_step_name.as_deref(),
        Some(snapshot.latest_top_step_name.as_str()),
        "stored latest-filing-certificate run latest_top_step_name",
        mismatches,
    );
    compare_option_string(
        stored.latest_top_session_dir.as_deref(),
        Some(
            snapshot
                .latest_top_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored latest-filing-certificate run latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored latest-filing-certificate run session_dir",
        mismatches,
    );
    compare_option_string(
        stored.downstream_decision_packet_session_dir.as_deref(),
        Some(
            snapshot
                .downstream_decision_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored latest-filing-certificate run downstream_decision_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.package_dir.as_deref(),
        Some(snapshot.package_dir.display().to_string().as_str()),
        "stored latest-filing-certificate run package_dir",
        mismatches,
    );
    compare_option_string(
        stored.install_root.as_deref(),
        Some(snapshot.install_root.display().to_string().as_str()),
        "stored latest-filing-certificate run install_root",
        mismatches,
    );
    compare_option_string(
        stored.target_service_name.as_deref(),
        Some(snapshot.target_service_name.as_str()),
        "stored latest-filing-certificate run target_service_name",
        mismatches,
    );
    compare_option_string(
        stored.backend_command.as_deref(),
        Some(snapshot.backend_command.as_str()),
        "stored latest-filing-certificate run backend_command",
        mismatches,
    );
    compare_option_u64(
        stored.wrapper_timeout_ms,
        Some(snapshot.wrapper_timeout_ms),
        "stored latest-filing-certificate run wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        stored.service_status_max_staleness_ms,
        Some(snapshot.service_status_max_staleness_ms),
        "stored latest-filing-certificate run service_status_max_staleness_ms",
        mismatches,
    );
    compare_option_string(
        stored
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        Some(
            snapshot
                .reviewed_frozen_live_cutover_controller_command_summary
                .as_str(),
        ),
        "stored latest-filing-certificate run reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_summary.as_deref(),
        Some(snapshot.clerestory_certificate_summary.as_str()),
        "stored latest-filing-certificate run clerestory_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_sha256.as_deref(),
        Some(snapshot.clerestory_certificate_sha256.as_str()),
        "stored latest-filing-certificate run clerestory_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_algorithm.as_deref(),
        Some(snapshot.clerestory_certificate_algorithm.as_str()),
        "stored latest-filing-certificate run clerestory_certificate_algorithm",
        mismatches,
    );
}

fn compare_filing_certificate_latest_copy_matches_actual(
    stored: &FilingCertificateLatestReportView,
    actual: &FilingCertificateLatestReportView,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        Some(stored.verdict.as_str()),
        Some(actual.verdict.as_str()),
        "stored latest-filing-certificate copy verdict",
        mismatches,
    );
    compare_option_string(
        Some(stored.reason.as_str()),
        Some(actual.reason.as_str()),
        "stored latest-filing-certificate copy reason",
        mismatches,
    );
    if stored.generated_at != actual.generated_at {
        mismatches.push(format!(
            "stored latest-filing-certificate copy generated_at {:?} does not match actual {:?}",
            stored.generated_at, actual.generated_at
        ));
    }
    compare_option_string(
        stored.latest_top_step_name.as_deref(),
        actual.latest_top_step_name.as_deref(),
        "stored latest-filing-certificate copy latest_top_step_name",
        mismatches,
    );
    compare_option_string(
        stored.latest_top_session_dir.as_deref(),
        actual.latest_top_session_dir.as_deref(),
        "stored latest-filing-certificate copy latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        actual.session_dir.as_deref(),
        "stored latest-filing-certificate copy session_dir",
        mismatches,
    );
    compare_option_string(
        stored.downstream_decision_packet_session_dir.as_deref(),
        actual.downstream_decision_packet_session_dir.as_deref(),
        "stored latest-filing-certificate copy downstream_decision_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.result.as_deref(),
        actual.result.as_deref(),
        "stored latest-filing-certificate copy result",
        mismatches,
    );
    compare_option_string(
        stored.explicit_statement.as_deref(),
        actual.explicit_statement.as_deref(),
        "stored latest-filing-certificate copy explicit_statement",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_verdict.as_deref(),
        actual.current_pre_activation_gate_verdict.as_deref(),
        "stored latest-filing-certificate copy current_pre_activation_gate_verdict",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_reason.as_deref(),
        actual.current_pre_activation_gate_reason.as_deref(),
        "stored latest-filing-certificate copy current_pre_activation_gate_reason",
        mismatches,
    );
    compare_option_string(
        stored
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        actual
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        "stored latest-filing-certificate copy reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
}

fn compare_archive_receipt_run_against_snapshot(
    stored: &ArchiveReceiptReportView,
    snapshot: &LatestFilingCertificateSnapshot,
    filing_certificate_session_dir: &Path,
    session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &stored.mode,
        "run_live_package_archive_receipt",
        "stored filing-certificate run mode",
        mismatches,
    );
    compare_string(
        &stored.filing_certificate_session_dir,
        &filing_certificate_session_dir.display().to_string(),
        "stored filing-certificate run filing_certificate_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.decision_packet_session_dir.as_deref(),
        Some(
            snapshot
                .downstream_decision_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored filing-certificate run decision_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.execute_frozen_session_dir.as_deref(),
        Some(
            snapshot
                .historical_execute_frozen_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored filing-certificate run execute_frozen_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.turn_green_session_dir.as_deref(),
        Some(
            snapshot
                .turn_green_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored filing-certificate run turn_green_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.launch_packet_session_dir.as_deref(),
        Some(
            snapshot
                .launch_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored filing-certificate run launch_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.package_dir.as_deref(),
        Some(snapshot.package_dir.display().to_string().as_str()),
        "stored filing-certificate run package_dir",
        mismatches,
    );
    compare_option_string(
        stored.install_root.as_deref(),
        Some(snapshot.install_root.display().to_string().as_str()),
        "stored filing-certificate run install_root",
        mismatches,
    );
    compare_option_string(
        stored.target_service_name.as_deref(),
        Some(snapshot.target_service_name.as_str()),
        "stored filing-certificate run target_service_name",
        mismatches,
    );
    compare_option_string(
        stored.backend_command.as_deref(),
        Some(snapshot.backend_command.as_str()),
        "stored filing-certificate run backend_command",
        mismatches,
    );
    compare_option_u64(
        stored.wrapper_timeout_ms,
        Some(snapshot.wrapper_timeout_ms),
        "stored filing-certificate run wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        stored.service_status_max_staleness_ms,
        Some(snapshot.service_status_max_staleness_ms),
        "stored filing-certificate run service_status_max_staleness_ms",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored filing-certificate run session_dir",
        mismatches,
    );
    compare_option_string(
        stored.archive_receipt_session_path.as_deref(),
        Some(
            nested_archive_receipt_session_path(session_dir)
                .display()
                .to_string()
                .as_str(),
        ),
        "stored filing-certificate run archive_receipt_session_path",
        mismatches,
    );
    compare_option_string(
        stored.archive_receipt_status_path.as_deref(),
        Some(
            nested_archive_receipt_status_path(session_dir)
                .display()
                .to_string()
                .as_str(),
        ),
        "stored filing-certificate run archive_receipt_status_path",
        mismatches,
    );
    compare_option_string(
        stored.archived_filing_certificate_report_path.as_deref(),
        Some(
            session_dir
                .join("tiny_live_activation_package_archive_receipt.filing_certificate.report.json")
                .display()
                .to_string()
                .as_str(),
        ),
        "stored filing-certificate run archived_filing_certificate_report_path",
        mismatches,
    );
    compare_option_string(
        stored.verify_filing_certificate_command_summary.as_deref(),
        Some(
            archive_receipt_verify_filing_certificate_command_summary(
                filing_certificate_session_dir,
            )
            .as_str(),
        ),
        "stored filing-certificate run verify_filing_certificate_command_summary",
        mismatches,
    );
    compare_option_string(
        stored
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        Some(
            snapshot
                .reviewed_frozen_live_cutover_controller_command_summary
                .as_str(),
        ),
        "stored filing-certificate run reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
}

fn compare_archive_receipt_copy_against_nested_truth(
    stored: &ArchiveReceiptReportView,
    nested_session: &ArchiveReceiptSessionView,
    nested_status: &ArchiveReceiptStatusView,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &stored.filing_certificate_session_dir,
        &nested_session.filing_certificate_session_dir,
        "stored filing-certificate copy filing_certificate_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.provenance_certificate_session_dir.as_deref(),
        Some(nested_session.provenance_certificate_session_dir.as_str()),
        "stored filing-certificate copy provenance_certificate_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.attestation_seal_session_dir.as_deref(),
        Some(nested_session.attestation_seal_session_dir.as_str()),
        "stored filing-certificate copy attestation_seal_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.release_capsule_session_dir.as_deref(),
        Some(nested_session.release_capsule_session_dir.as_str()),
        "stored filing-certificate copy release_capsule_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.activation_ticket_session_dir.as_deref(),
        Some(nested_session.activation_ticket_session_dir.as_str()),
        "stored filing-certificate copy activation_ticket_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.review_receipt_session_dir.as_deref(),
        Some(nested_session.review_receipt_session_dir.as_str()),
        "stored filing-certificate copy review_receipt_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.handoff_bundle_session_dir.as_deref(),
        Some(nested_session.handoff_bundle_session_dir.as_str()),
        "stored filing-certificate copy handoff_bundle_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.decision_packet_session_dir.as_deref(),
        Some(nested_session.decision_packet_session_dir.as_str()),
        "stored filing-certificate copy decision_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.execute_frozen_session_dir.as_deref(),
        Some(nested_session.execute_frozen_session_dir.as_str()),
        "stored filing-certificate copy execute_frozen_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.turn_green_session_dir.as_deref(),
        Some(nested_session.turn_green_session_dir.as_str()),
        "stored filing-certificate copy turn_green_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.launch_packet_session_dir.as_deref(),
        Some(nested_session.launch_packet_session_dir.as_str()),
        "stored filing-certificate copy launch_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(nested_session.session_dir.as_str()),
        "stored filing-certificate copy session_dir",
        mismatches,
    );
    compare_option_string(
        stored.result.as_deref(),
        Some(nested_status.result.as_str()),
        "stored filing-certificate copy result",
        mismatches,
    );
    compare_option_string(
        Some(stored.verdict.as_str()),
        archive_receipt_run_verdict_for_result(&nested_status.result),
        "stored filing-certificate copy verdict",
        mismatches,
    );
    compare_option_string(
        stored.handoff_bundle_result.as_deref(),
        nested_status.handoff_bundle_result.as_deref(),
        "stored filing-certificate copy handoff_bundle_result",
        mismatches,
    );
    compare_option_string(
        stored.decision_packet_result.as_deref(),
        nested_status.decision_packet_result.as_deref(),
        "stored filing-certificate copy decision_packet_result",
        mismatches,
    );
    compare_option_string(
        stored.execute_frozen_result.as_deref(),
        nested_status.execute_frozen_result.as_deref(),
        "stored filing-certificate copy execute_frozen_result",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_verdict.as_deref(),
        nested_status.current_pre_activation_gate_verdict.as_deref(),
        "stored filing-certificate copy current_pre_activation_gate_verdict",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_reason.as_deref(),
        nested_status.current_pre_activation_gate_reason.as_deref(),
        "stored filing-certificate copy current_pre_activation_gate_reason",
        mismatches,
    );
    compare_option_string(
        stored.verify_filing_certificate_command_summary.as_deref(),
        Some(
            nested_session
                .verify_filing_certificate_command_summary
                .as_str(),
        ),
        "stored filing-certificate copy verify_filing_certificate_command_summary",
        mismatches,
    );
    compare_option_string(
        stored
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        Some(
            nested_session
                .reviewed_frozen_live_cutover_controller_command_summary
                .as_str(),
        ),
        "stored filing-certificate copy reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_option_string(
        stored.provenance_certificate_summary.as_deref(),
        Some(nested_status.provenance_certificate_summary.as_str()),
        "stored filing-certificate copy provenance_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.chain_fingerprint_summary.as_deref(),
        Some(nested_status.chain_fingerprint_summary.as_str()),
        "stored filing-certificate copy chain_fingerprint_summary",
        mismatches,
    );
    compare_option_string(
        stored.chain_fingerprint_sha256.as_deref(),
        Some(nested_status.chain_fingerprint_sha256.as_str()),
        "stored filing-certificate copy chain_fingerprint_sha256",
        mismatches,
    );
    compare_option_string(
        stored.chain_fingerprint_algorithm.as_deref(),
        Some(nested_status.chain_fingerprint_algorithm.as_str()),
        "stored filing-certificate copy chain_fingerprint_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.notarization_receipt_summary.as_deref(),
        Some(nested_status.notarization_receipt_summary.as_str()),
        "stored filing-certificate copy notarization_receipt_summary",
        mismatches,
    );
    compare_option_string(
        stored.release_capsule_digest_manifest_sha256.as_deref(),
        Some(
            nested_status
                .release_capsule_digest_manifest_sha256
                .as_str(),
        ),
        "stored filing-certificate copy release_capsule_digest_manifest_sha256",
        mismatches,
    );
    compare_option_usize(
        stored.release_capsule_digest_manifest_entry_count,
        Some(nested_status.release_capsule_digest_manifest_entry_count),
        "stored filing-certificate copy release_capsule_digest_manifest_entry_count",
        mismatches,
    );
    compare_option_string(
        stored.release_capsule_digest_algorithm.as_deref(),
        Some(nested_status.release_capsule_digest_algorithm.as_str()),
        "stored filing-certificate copy release_capsule_digest_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.ledger_seal_summary.as_deref(),
        Some(nested_status.ledger_seal_summary.as_str()),
        "stored filing-certificate copy ledger_seal_summary",
        mismatches,
    );
    compare_option_string(
        stored.ledger_seal_sha256.as_deref(),
        Some(nested_status.ledger_seal_sha256.as_str()),
        "stored filing-certificate copy ledger_seal_sha256",
        mismatches,
    );
    compare_option_string(
        stored.ledger_seal_algorithm.as_deref(),
        Some(nested_status.ledger_seal_algorithm.as_str()),
        "stored filing-certificate copy ledger_seal_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.registry_entry_summary.as_deref(),
        Some(nested_status.registry_entry_summary.as_str()),
        "stored filing-certificate copy registry_entry_summary",
        mismatches,
    );
    compare_option_string(
        stored.registry_entry_sha256.as_deref(),
        Some(nested_status.registry_entry_sha256.as_str()),
        "stored filing-certificate copy registry_entry_sha256",
        mismatches,
    );
    compare_option_string(
        stored.registry_entry_algorithm.as_deref(),
        Some(nested_status.registry_entry_algorithm.as_str()),
        "stored filing-certificate copy registry_entry_algorithm",
        mismatches,
    );
    compare_string(
        &stored.reason,
        &nested_status.reason,
        "stored filing-certificate copy reason",
        mismatches,
    );
    compare_string(
        &stored.explicit_statement,
        &nested_status.explicit_statement,
        "stored filing-certificate copy explicit_statement",
        mismatches,
    );
}

fn compare_session_against_snapshot(
    session: &PackageArchiveReceiptLatestSession,
    root: &Path,
    session_dir: &Path,
    paths: &PackageArchiveReceiptLatestPaths,
    snapshot: &LatestFilingCertificateSnapshot,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &session.resolve_latest_filing_certificate_command_summary,
        &resolve_latest_filing_certificate_command_summary(
            root,
            &paths.nested_filing_certificate_latest_session_dir,
        ),
        "stored session resolve_latest_filing_certificate_command_summary",
        mismatches,
    );
    compare_string(
        &session.run_latest_archive_receipt_command_summary,
        &run_latest_archive_receipt_command_summary(
            root,
            &paths.nested_filing_certificate_latest_session_dir,
        ),
        "stored session run_latest_archive_receipt_command_summary",
        mismatches,
    );
    compare_string(
        &session.verify_latest_archive_receipt_command_summary,
        &verify_latest_archive_receipt_command_summary(
            &paths.nested_filing_certificate_latest_session_dir,
        ),
        "stored session verify_latest_archive_receipt_command_summary",
        mismatches,
    );
    compare_string(
        &session.downstream_archive_receipt_command_summary,
        &downstream_archive_receipt_command_summary(
            snapshot,
            &expected_downstream_filing_certificate_session_dir(paths),
            &paths.nested_archive_receipt_session_dir,
        ),
        "stored session downstream_archive_receipt_command_summary",
        mismatches,
    );
    compare_string(
        &session.verify_archive_receipt_command_summary,
        &verify_archive_receipt_command_summary(
            snapshot,
            &expected_downstream_filing_certificate_session_dir(paths),
            &paths.nested_archive_receipt_session_dir,
        ),
        "stored session verify_archive_receipt_command_summary",
        mismatches,
    );
    compare_option_string(
        session.latest_top_step_name.as_deref(),
        Some(snapshot.latest_top_step_name.as_str()),
        "stored session latest_top_step_name",
        mismatches,
    );
    compare_option_string(
        session.latest_top_session_dir.as_deref(),
        Some(
            snapshot
                .latest_top_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored session latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        session.package_dir.as_deref(),
        Some(snapshot.package_dir.display().to_string().as_str()),
        "stored session package_dir",
        mismatches,
    );
    compare_option_string(
        session.install_root.as_deref(),
        Some(snapshot.install_root.display().to_string().as_str()),
        "stored session install_root",
        mismatches,
    );
    compare_option_string(
        session.target_service_name.as_deref(),
        Some(snapshot.target_service_name.as_str()),
        "stored session target_service_name",
        mismatches,
    );
    compare_option_string(
        session.backend_command.as_deref(),
        Some(snapshot.backend_command.as_str()),
        "stored session backend_command",
        mismatches,
    );
    compare_option_u64(
        session.wrapper_timeout_ms,
        Some(snapshot.wrapper_timeout_ms),
        "stored session wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        session.service_status_max_staleness_ms,
        Some(snapshot.service_status_max_staleness_ms),
        "stored session service_status_max_staleness_ms",
        mismatches,
    );
    compare_option_string(
        session
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        Some(
            snapshot
                .reviewed_frozen_live_cutover_controller_command_summary
                .as_str(),
        ),
        "stored session reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_option_string(
        session.clerestory_certificate_summary.as_deref(),
        Some(snapshot.clerestory_certificate_summary.as_str()),
        "stored session clerestory_certificate_summary",
        mismatches,
    );
    compare_option_string(
        session.clerestory_certificate_sha256.as_deref(),
        Some(snapshot.clerestory_certificate_sha256.as_str()),
        "stored session clerestory_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        session.clerestory_certificate_algorithm.as_deref(),
        Some(snapshot.clerestory_certificate_algorithm.as_str()),
        "stored session clerestory_certificate_algorithm",
        mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "stored session session_dir",
        mismatches,
    );
    compare_string(
        &session.nested_filing_certificate_latest_session_dir,
        &paths
            .nested_filing_certificate_latest_session_dir
            .display()
            .to_string(),
        "stored session nested_filing_certificate_latest_session_dir",
        mismatches,
    );
    compare_string(
        &session.nested_archive_receipt_session_dir,
        &paths
            .nested_archive_receipt_session_dir
            .display()
            .to_string(),
        "stored session nested_archive_receipt_session_dir",
        mismatches,
    );
    compare_string(
        &session.downstream_decision_packet_session_dir,
        &snapshot
            .downstream_decision_packet_session_dir
            .display()
            .to_string(),
        "stored session downstream_decision_packet_session_dir",
        mismatches,
    );
    compare_string(
        &session.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "stored session explicit_statement",
        mismatches,
    );
}

fn compare_report_against_snapshot(
    report: &PackageArchiveReceiptLatestReport,
    root: &Path,
    session_dir: &Path,
    paths: &PackageArchiveReceiptLatestPaths,
    snapshot: &LatestFilingCertificateSnapshot,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        report.root.as_deref(),
        Some(root.display().to_string().as_str()),
        "stored report root",
        mismatches,
    );
    compare_option_string(
        report.latest_top_step_name.as_deref(),
        Some(snapshot.latest_top_step_name.as_str()),
        "stored report latest_top_step_name",
        mismatches,
    );
    compare_option_string(
        report.latest_top_session_dir.as_deref(),
        Some(
            snapshot
                .latest_top_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        report.package_dir.as_deref(),
        Some(snapshot.package_dir.display().to_string().as_str()),
        "stored report package_dir",
        mismatches,
    );
    compare_option_string(
        report.install_root.as_deref(),
        Some(snapshot.install_root.display().to_string().as_str()),
        "stored report install_root",
        mismatches,
    );
    compare_option_string(
        report.target_service_name.as_deref(),
        Some(snapshot.target_service_name.as_str()),
        "stored report target_service_name",
        mismatches,
    );
    compare_option_string(
        report.backend_command.as_deref(),
        Some(snapshot.backend_command.as_str()),
        "stored report backend_command",
        mismatches,
    );
    compare_option_u64(
        report.wrapper_timeout_ms,
        Some(snapshot.wrapper_timeout_ms),
        "stored report wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        report.service_status_max_staleness_ms,
        Some(snapshot.service_status_max_staleness_ms),
        "stored report service_status_max_staleness_ms",
        mismatches,
    );
    compare_option_string(
        report
            .resolve_latest_filing_certificate_command_summary
            .as_deref(),
        Some(
            resolve_latest_filing_certificate_command_summary(
                root,
                &paths.nested_filing_certificate_latest_session_dir,
            )
            .as_str(),
        ),
        "stored report resolve_latest_filing_certificate_command_summary",
        mismatches,
    );
    compare_option_string(
        report.run_latest_archive_receipt_command_summary.as_deref(),
        Some(
            run_latest_archive_receipt_command_summary(
                root,
                &paths.nested_filing_certificate_latest_session_dir,
            )
            .as_str(),
        ),
        "stored report run_latest_archive_receipt_command_summary",
        mismatches,
    );
    compare_option_string(
        report
            .verify_latest_archive_receipt_command_summary
            .as_deref(),
        Some(
            verify_latest_archive_receipt_command_summary(
                &paths.nested_filing_certificate_latest_session_dir,
            )
            .as_str(),
        ),
        "stored report verify_latest_archive_receipt_command_summary",
        mismatches,
    );
    compare_option_string(
        report.downstream_archive_receipt_command_summary.as_deref(),
        Some(
            downstream_archive_receipt_command_summary(
                snapshot,
                &expected_downstream_filing_certificate_session_dir(paths),
                &paths.nested_archive_receipt_session_dir,
            )
            .as_str(),
        ),
        "stored report downstream_archive_receipt_command_summary",
        mismatches,
    );
    compare_option_string(
        report.verify_archive_receipt_command_summary.as_deref(),
        Some(
            verify_archive_receipt_command_summary(
                snapshot,
                &expected_downstream_filing_certificate_session_dir(paths),
                &paths.nested_archive_receipt_session_dir,
            )
            .as_str(),
        ),
        "stored report verify_archive_receipt_command_summary",
        mismatches,
    );
    compare_option_string(
        report
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        Some(
            snapshot
                .reviewed_frozen_live_cutover_controller_command_summary
                .as_str(),
        ),
        "stored report reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_option_string(
        report.clerestory_certificate_summary.as_deref(),
        Some(snapshot.clerestory_certificate_summary.as_str()),
        "stored report clerestory_certificate_summary",
        mismatches,
    );
    compare_option_string(
        report.clerestory_certificate_sha256.as_deref(),
        Some(snapshot.clerestory_certificate_sha256.as_str()),
        "stored report clerestory_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        report.clerestory_certificate_algorithm.as_deref(),
        Some(snapshot.clerestory_certificate_algorithm.as_str()),
        "stored report clerestory_certificate_algorithm",
        mismatches,
    );
    compare_option_string(
        report.archive_receipt_latest_session_path.as_deref(),
        Some(paths.session_path.display().to_string().as_str()),
        "stored report archive_receipt_latest_session_path",
        mismatches,
    );
    compare_option_string(
        report.archive_receipt_latest_status_path.as_deref(),
        Some(paths.status_path.display().to_string().as_str()),
        "stored report archive_receipt_latest_status_path",
        mismatches,
    );
    compare_option_string(
        report.archive_receipt_latest_report_path.as_deref(),
        Some(paths.report_path.display().to_string().as_str()),
        "stored report archive_receipt_latest_report_path",
        mismatches,
    );
    compare_option_string(
        report.latest_filing_certificate_plan_report_path.as_deref(),
        Some(
            paths
                .latest_filing_certificate_plan_report_path
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report latest_filing_certificate_plan_report_path",
        mismatches,
    );
    compare_option_string(
        report.latest_archive_receipt_report_path.as_deref(),
        Some(
            paths
                .latest_archive_receipt_report_path
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report latest_archive_receipt_report_path",
        mismatches,
    );
    compare_option_string(
        report.archive_receipt_report_path.as_deref(),
        Some(
            paths
                .archive_receipt_report_path
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report archive_receipt_report_path",
        mismatches,
    );
    compare_option_string(
        report
            .nested_filing_certificate_latest_session_dir
            .as_deref(),
        Some(
            paths
                .nested_filing_certificate_latest_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report nested_filing_certificate_latest_session_dir",
        mismatches,
    );
    compare_option_string(
        report.nested_archive_receipt_session_dir.as_deref(),
        Some(
            paths
                .nested_archive_receipt_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report nested_archive_receipt_session_dir",
        mismatches,
    );
    compare_option_string(
        report.downstream_decision_packet_session_dir.as_deref(),
        Some(
            snapshot
                .downstream_decision_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report downstream_decision_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        report.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored report session_dir",
        mismatches,
    );
    compare_string(
        &report.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "stored report explicit_statement",
        mismatches,
    );
}

fn compare_session_against_failure(
    session: &PackageArchiveReceiptLatestSession,
    root: &Path,
    session_dir: &Path,
    paths: &PackageArchiveReceiptLatestPaths,
    failure: &LatestFilingCertificateResolutionFailure,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &session.root,
        &root.display().to_string(),
        "stored session root on failure",
        mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "stored session session_dir on failure",
        mismatches,
    );
    compare_string(
        &session.nested_filing_certificate_latest_session_dir,
        &paths
            .nested_filing_certificate_latest_session_dir
            .display()
            .to_string(),
        "stored session nested_filing_certificate_latest_session_dir on failure",
        mismatches,
    );
    compare_string(
        &session.nested_archive_receipt_session_dir,
        &paths
            .nested_archive_receipt_session_dir
            .display()
            .to_string(),
        "stored session nested_archive_receipt_session_dir on failure",
        mismatches,
    );
    if let Some(snapshot) = failure.snapshot.as_ref() {
        compare_session_against_snapshot(session, root, session_dir, paths, snapshot, mismatches);
    }
}

fn compare_report_against_failure(
    report: &PackageArchiveReceiptLatestReport,
    root: &Path,
    session_dir: &Path,
    paths: &PackageArchiveReceiptLatestPaths,
    failure: &LatestFilingCertificateResolutionFailure,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        report.root.as_deref(),
        Some(root.display().to_string().as_str()),
        "stored report root on failure",
        mismatches,
    );
    compare_option_string(
        report.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored report session_dir on failure",
        mismatches,
    );
    compare_option_string(
        report.result.as_deref(),
        Some(serialize_enum(&result_from_failure_kind(failure.kind)).as_str()),
        "stored report result on failure",
        mismatches,
    );
    compare_string(
        &report.mode,
        "run_latest_archive_receipt",
        "stored report mode on failure",
        mismatches,
    );
    compare_string(
        &serialize_enum(&report.verdict),
        &serialize_enum(&verdict_from_failure_kind(failure.kind)),
        "stored report verdict on failure",
        mismatches,
    );
    compare_string(
        &report.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "stored report explicit_statement on failure",
        mismatches,
    );
    if let Some(snapshot) = failure.snapshot.as_ref() {
        compare_report_against_snapshot(report, root, session_dir, paths, snapshot, mismatches);
    }
}

fn required_option_string(value: Option<&String>, label: &str) -> Result<String> {
    value
        .filter(|entry| !entry.trim().is_empty())
        .cloned()
        .ok_or_else(|| anyhow!("missing required {label}"))
}

fn map_filing_certificate_latest_failure_kind(verdict: &str) -> ResolutionFailureKind {
    match verdict {
        "tiny_live_package_filing_certificate_latest_refused_by_missing_latest_chain" => {
            ResolutionFailureKind::MissingLatestChain
        }
        "tiny_live_package_filing_certificate_latest_refused_by_drifted_registry_entry_contract" => {
            ResolutionFailureKind::DriftedRegistryEntryContract
        }
        _ => ResolutionFailureKind::InvalidLatestChain,
    }
}

fn latest_filing_certificate_terminal_failure(
    report: &FilingCertificateLatestReportView,
) -> Option<LivePackageArchiveReceiptLatestResult> {
    match report.result.as_deref() {
        Some("refused_by_missing_latest_chain") => {
            Some(LivePackageArchiveReceiptLatestResult::RefusedByMissingLatestChain)
        }
        Some("refused_by_invalid_latest_chain") => {
            Some(LivePackageArchiveReceiptLatestResult::RefusedByInvalidLatestChain)
        }
        Some("refused_by_drifted_registry_entry_contract") => {
            Some(LivePackageArchiveReceiptLatestResult::RefusedByDriftedArchiveReceiptContract)
        }
        _ => None,
    }
}

fn filing_certificate_latest_result_from_notarization_receipt(
    result: &str,
) -> Option<LivePackageArchiveReceiptLatestResult> {
    match result {
        "ready_for_manual_execution_when_gate_turns_green" => {
            Some(LivePackageArchiveReceiptLatestResult::ReadyForManualExecutionWhenGateTurnsGreen)
        }
        "refused_now_by_stage3" => Some(LivePackageArchiveReceiptLatestResult::RefusedNowByStage3),
        "refused_now_by_pre_activation_gate" => {
            Some(LivePackageArchiveReceiptLatestResult::RefusedNowByPreActivationGate)
        }
        "refused_now_by_invalid_or_drifted_contract" => {
            Some(LivePackageArchiveReceiptLatestResult::RefusedByDriftedArchiveReceiptContract)
        }
        _ => None,
    }
}

fn archive_receipt_run_verdict_for_result(result: &str) -> Option<&'static str> {
    match result {
        "ready_for_manual_execution_when_gate_turns_green" => Some(
            "tiny_live_package_archive_receipt_ready_for_manual_execution_when_gate_turns_green",
        ),
        "refused_now_by_stage3" => Some("tiny_live_package_archive_receipt_refused_now_by_stage3"),
        "refused_now_by_pre_activation_gate" => {
            Some("tiny_live_package_archive_receipt_refused_now_by_pre_activation_gate")
        }
        "refused_now_by_invalid_or_drifted_contract" => {
            Some("tiny_live_package_archive_receipt_refused_now_by_invalid_or_drifted_contract")
        }
        _ => None,
    }
}

fn result_from_failure_kind(kind: ResolutionFailureKind) -> LivePackageArchiveReceiptLatestResult {
    match kind {
        ResolutionFailureKind::MissingLatestChain => {
            LivePackageArchiveReceiptLatestResult::RefusedByMissingLatestChain
        }
        ResolutionFailureKind::InvalidLatestChain => {
            LivePackageArchiveReceiptLatestResult::RefusedByInvalidLatestChain
        }
        ResolutionFailureKind::DriftedRegistryEntryContract => {
            LivePackageArchiveReceiptLatestResult::RefusedByDriftedArchiveReceiptContract
        }
    }
}

fn verdict_for_result(
    result: LivePackageArchiveReceiptLatestResult,
) -> TinyLivePackageArchiveReceiptLatestVerdict {
    match result {
        LivePackageArchiveReceiptLatestResult::RefusedByMissingLatestChain => {
            TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestRefusedByMissingLatestChain
        }
        LivePackageArchiveReceiptLatestResult::RefusedByInvalidLatestChain => {
            TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestRefusedByInvalidLatestChain
        }
        LivePackageArchiveReceiptLatestResult::RefusedByDriftedArchiveReceiptContract => {
            TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestRefusedByDriftedArchiveReceiptContract
        }
        LivePackageArchiveReceiptLatestResult::RefusedNowByStage3 => {
            TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestRefusedNowByStage3
        }
        LivePackageArchiveReceiptLatestResult::RefusedNowByPreActivationGate => {
            TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestRefusedNowByPreActivationGate
        }
        LivePackageArchiveReceiptLatestResult::ReadyForManualExecutionWhenGateTurnsGreen => {
            TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestReadyForManualExecutionWhenGateTurnsGreen
        }
    }
}

fn verdict_from_failure_kind(
    kind: ResolutionFailureKind,
) -> TinyLivePackageArchiveReceiptLatestVerdict {
    verdict_for_result(result_from_failure_kind(kind))
}

fn operator_next_action_summary(result: LivePackageArchiveReceiptLatestResult) -> String {
    match result {
        LivePackageArchiveReceiptLatestResult::RefusedByMissingLatestChain => {
            "no immutable clerestory chain is available yet; do not manually hunt filing-certificate or archive-receipt lineage by hand".to_string()
        }
        LivePackageArchiveReceiptLatestResult::RefusedByInvalidLatestChain => {
            "the latest immutable chain is incomplete or below the accepted clerestory layer; mint a fresh coherent clerestory chain instead of bypassing the accepted latest-filing-certificate / archive-receipt path".to_string()
        }
        LivePackageArchiveReceiptLatestResult::RefusedByDriftedArchiveReceiptContract => {
            "the latest chain no longer resolves cleanly into the accepted latest-filing-certificate / archive-receipt contract; repair that drift instead of bypassing the bounded handoff path".to_string()
        }
        LivePackageArchiveReceiptLatestResult::RefusedNowByStage3 => {
            "the frozen controller stays refused now by Stage 3; rerun this latest-archive-receipt refresh after current Stage 3 truth turns green".to_string()
        }
        LivePackageArchiveReceiptLatestResult::RefusedNowByPreActivationGate => {
            "the frozen controller stays refused now by the pre-activation gate; rerun this latest-archive-receipt refresh after the current pre-activation gate turns green".to_string()
        }
        LivePackageArchiveReceiptLatestResult::ReadyForManualExecutionWhenGateTurnsGreen => {
            "the latest immutable chain now resolves into the accepted archive-receipt contract and one bounded latest archive-receipt handoff is ready for manual execution when gate truth turns green; this wrapper still did not run or authorize the frozen controller".to_string()
        }
    }
}

fn serialize_enum<T: Serialize>(value: &T) -> String {
    match serde_json::to_value(value) {
        Ok(Value::String(raw)) => raw,
        Ok(other) => other.to_string(),
        Err(_) => "<serialization-error>".to_string(),
    }
}

fn archive_receipt_verify_filing_certificate_command_summary(
    filing_certificate_session_dir: &Path,
) -> String {
    format!(
        "verify immutable filing-certificate session under {} before freezing the final archive receipt",
        filing_certificate_session_dir.display()
    )
}

fn refusal_status(
    root: &Path,
    session_dir: &Path,
    paths: &PackageArchiveReceiptLatestPaths,
    resolved: Option<&ResolvedLatestFilingCertificate>,
    result: LivePackageArchiveReceiptLatestResult,
    reason: String,
    latest_filing_certificate_plan_step: Option<PackageArchiveReceiptLatestStepArtifact>,
    latest_archive_receipt_step: Option<PackageArchiveReceiptLatestStepArtifact>,
    archive_receipt_step: Option<PackageArchiveReceiptLatestStepArtifact>,
) -> PackageArchiveReceiptLatestStatus {
    let snapshot = resolved.map(|value| &value.snapshot);
    PackageArchiveReceiptLatestStatus {
        status_version: ARCHIVE_RECEIPT_LATEST_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        root: root.display().to_string(),
        session_dir: session_dir.display().to_string(),
        latest_top_step_name: snapshot.map(|value| value.latest_top_step_name.clone()),
        latest_top_session_dir: snapshot
            .map(|value| value.latest_top_session_dir.display().to_string()),
        historical_execute_frozen_session_dir: snapshot.map(|value| {
            value
                .historical_execute_frozen_session_dir
                .display()
                .to_string()
        }),
        turn_green_session_dir: snapshot
            .map(|value| value.turn_green_session_dir.display().to_string()),
        launch_packet_session_dir: snapshot
            .map(|value| value.launch_packet_session_dir.display().to_string()),
        nested_filing_certificate_latest_session_dir: paths
            .nested_filing_certificate_latest_session_dir
            .display()
            .to_string(),
        nested_archive_receipt_session_dir: paths
            .nested_archive_receipt_session_dir
            .display()
            .to_string(),
        downstream_decision_packet_session_dir: snapshot
            .map(|value| {
                value
                    .downstream_decision_packet_session_dir
                    .display()
                    .to_string()
            })
            .unwrap_or_else(|| {
                expected_downstream_decision_packet_session_dir(paths)
                    .display()
                    .to_string()
            }),
        result,
        reason,
        latest_filing_certificate_plan_verdict: resolved
            .map(|value| value.latest_filing_certificate_plan.verdict.clone()),
        latest_archive_receipt_verdict: latest_archive_receipt_step
            .as_ref()
            .map(|value| value.verdict.clone()),
        archive_receipt_verdict: archive_receipt_step
            .as_ref()
            .map(|value| value.verdict.clone()),
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        latest_filing_certificate_plan_step,
        latest_archive_receipt_step,
        archive_receipt_step,
        operator_next_action_summary: operator_next_action_summary(result),
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn refusal_status_from_resolution_failure(
    root: &Path,
    session_dir: &Path,
    paths: &PackageArchiveReceiptLatestPaths,
    failure: &LatestFilingCertificateResolutionFailure,
) -> PackageArchiveReceiptLatestStatus {
    PackageArchiveReceiptLatestStatus {
        status_version: ARCHIVE_RECEIPT_LATEST_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        root: root.display().to_string(),
        session_dir: session_dir.display().to_string(),
        latest_top_step_name: failure
            .snapshot
            .as_ref()
            .map(|value| value.latest_top_step_name.clone()),
        latest_top_session_dir: failure
            .snapshot
            .as_ref()
            .map(|value| value.latest_top_session_dir.display().to_string()),
        historical_execute_frozen_session_dir: failure.snapshot.as_ref().map(|value| {
            value
                .historical_execute_frozen_session_dir
                .display()
                .to_string()
        }),
        turn_green_session_dir: failure
            .snapshot
            .as_ref()
            .map(|value| value.turn_green_session_dir.display().to_string()),
        launch_packet_session_dir: failure
            .snapshot
            .as_ref()
            .map(|value| value.launch_packet_session_dir.display().to_string()),
        nested_filing_certificate_latest_session_dir: paths
            .nested_filing_certificate_latest_session_dir
            .display()
            .to_string(),
        nested_archive_receipt_session_dir: paths
            .nested_archive_receipt_session_dir
            .display()
            .to_string(),
        downstream_decision_packet_session_dir: failure
            .snapshot
            .as_ref()
            .map(|value| {
                value
                    .downstream_decision_packet_session_dir
                    .display()
                    .to_string()
            })
            .unwrap_or_else(|| {
                expected_downstream_decision_packet_session_dir(paths)
                    .display()
                    .to_string()
            }),
        result: result_from_failure_kind(failure.kind),
        reason: failure.reason.clone(),
        latest_filing_certificate_plan_verdict: failure
            .latest_filing_certificate_plan_verdict
            .clone(),
        latest_archive_receipt_verdict: None,
        archive_receipt_verdict: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        latest_filing_certificate_plan_step: failure
            .latest_filing_certificate_plan_raw
            .as_ref()
            .map(|_| {
                step_artifact(
                    &paths.latest_filing_certificate_plan_report_path,
                    "plan_latest_filing_certificate",
                    failure
                        .latest_filing_certificate_plan_verdict
                        .as_deref()
                        .unwrap_or("<missing>"),
                    failure
                        .latest_filing_certificate_plan_reason
                        .as_deref()
                        .unwrap_or("<missing>"),
                    failure
                        .latest_filing_certificate_plan_generated_at
                        .unwrap_or_else(Utc::now),
                )
            }),
        latest_archive_receipt_step: None,
        archive_receipt_step: None,
        operator_next_action_summary: operator_next_action_summary(result_from_failure_kind(
            failure.kind,
        )),
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn verify_invalid_filing_certificate_latest_report_without_session(
    session_dir: &Path,
    verification_mismatches: Vec<String>,
) -> PackageArchiveReceiptLatestReport {
    let reason = verification_mismatches
        .first()
        .cloned()
        .unwrap_or_else(|| "latest-archive-receipt verification failed".to_string());
    PackageArchiveReceiptLatestReport {
        generated_at: Utc::now(),
        mode: "verify_latest_archive_receipt".to_string(),
        verdict: TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestVerifyInvalid,
        reason,
        root: None,
        latest_top_step_name: None,
        latest_top_session_dir: None,
        historical_execute_frozen_session_dir: None,
        turn_green_session_dir: None,
        launch_packet_session_dir: None,
        package_dir: None,
        install_root: None,
        target_service_name: None,
        backend_command: None,
        wrapper_timeout_ms: None,
        service_status_max_staleness_ms: None,
        session_dir: Some(session_dir.display().to_string()),
        archive_receipt_latest_session_path: Some(
            filing_certificate_latest_paths(session_dir)
                .session_path
                .display()
                .to_string(),
        ),
        archive_receipt_latest_status_path: Some(
            filing_certificate_latest_paths(session_dir)
                .status_path
                .display()
                .to_string(),
        ),
        archive_receipt_latest_report_path: Some(
            filing_certificate_latest_paths(session_dir)
                .report_path
                .display()
                .to_string(),
        ),
        latest_filing_certificate_plan_report_path: None,
        latest_archive_receipt_report_path: None,
        archive_receipt_report_path: None,
        nested_filing_certificate_latest_session_dir: None,
        nested_archive_receipt_session_dir: None,
        downstream_decision_packet_session_dir: None,
        latest_filing_certificate_plan_verdict: None,
        latest_archive_receipt_verdict: None,
        archive_receipt_verdict: None,
        result: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        resolve_latest_filing_certificate_command_summary: None,
        run_latest_archive_receipt_command_summary: None,
        verify_latest_archive_receipt_command_summary: None,
        downstream_archive_receipt_command_summary: None,
        verify_archive_receipt_command_summary: None,
        reviewed_frozen_live_cutover_controller_command_summary: None,
        clerestory_certificate_summary: None,
        clerestory_certificate_sha256: None,
        clerestory_certificate_algorithm: None,
        verification_mismatches,
        activation_authorized: false,
        explicit_statement:
            "this verification proves only whether the latest immutable chain still resolves cleanly into the accepted archive-receipt contract; it never runs the frozen controller"
                .to_string(),
    }
}

fn fallback_filing_certificate_latest_copy() -> FilingCertificateLatestReportView {
    FilingCertificateLatestReportView {
        generated_at: Utc::now(),
        mode: "<missing>".to_string(),
        verdict: "<missing>".to_string(),
        reason: "<missing>".to_string(),
        latest_top_step_name: None,
        latest_top_session_dir: None,
        historical_execute_frozen_session_dir: None,
        turn_green_session_dir: None,
        launch_packet_session_dir: None,
        package_dir: None,
        install_root: None,
        target_service_name: None,
        backend_command: None,
        wrapper_timeout_ms: None,
        service_status_max_staleness_ms: None,
        session_dir: None,
        downstream_decision_packet_session_dir: None,
        latest_execute_verdict: None,
        decision_packet_plan_verdict: None,
        result: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        reviewed_frozen_live_cutover_controller_command_summary: None,
        clerestory_certificate_summary: None,
        clerestory_certificate_sha256: None,
        clerestory_certificate_algorithm: None,
        activation_authorized: false,
        explicit_statement: None,
    }
}

fn fallback_notarization_receipt_session(
    snapshot: &LatestFilingCertificateSnapshot,
    session_dir: &Path,
) -> ArchiveReceiptSessionView {
    ArchiveReceiptSessionView {
        filing_certificate_session_dir: "<missing>".to_string(),
        provenance_certificate_session_dir: "<missing>".to_string(),
        attestation_seal_session_dir: "<missing>".to_string(),
        release_capsule_session_dir: "<missing>".to_string(),
        activation_ticket_session_dir: "<missing>".to_string(),
        review_receipt_session_dir: "<missing>".to_string(),
        handoff_bundle_session_dir: "<missing>".to_string(),
        decision_packet_session_dir: snapshot
            .downstream_decision_packet_session_dir
            .display()
            .to_string(),
        execute_frozen_session_dir: snapshot
            .historical_execute_frozen_session_dir
            .display()
            .to_string(),
        turn_green_session_dir: snapshot.turn_green_session_dir.display().to_string(),
        launch_packet_session_dir: snapshot.launch_packet_session_dir.display().to_string(),
        package_dir: snapshot.package_dir.display().to_string(),
        install_root: snapshot.install_root.display().to_string(),
        target_service_name: snapshot.target_service_name.clone(),
        backend_command: snapshot.backend_command.clone(),
        wrapper_timeout_ms: snapshot.wrapper_timeout_ms,
        service_status_max_staleness_ms: snapshot.service_status_max_staleness_ms,
        session_dir: session_dir.display().to_string(),
        verify_filing_certificate_command_summary:
            archive_receipt_verify_filing_certificate_command_summary(Path::new("<missing>")),
        reviewed_frozen_live_cutover_controller_command_summary: snapshot
            .reviewed_frozen_live_cutover_controller_command_summary
            .clone(),
        provenance_certificate_summary:
            "Provenance certificate outcome: fallback session is synthetic and must not verify green."
                .to_string(),
        chain_fingerprint_summary:
            "Chain fingerprint identity: fallback session is synthetic and must not verify green."
                .to_string(),
        chain_fingerprint_sha256: "<missing>".to_string(),
        chain_fingerprint_algorithm: "sha256".to_string(),
        release_capsule_digest_manifest_sha256: "<missing>".to_string(),
        release_capsule_digest_manifest_entry_count: 0,
        release_capsule_digest_algorithm: "sha256".to_string(),
        notarization_receipt_summary:
            "Notarization receipt outcome: fallback session is synthetic and must not verify green."
                .to_string(),
        ledger_seal_summary:
            "Ledger seal identity: fallback session is synthetic and must not verify green."
                .to_string(),
        ledger_seal_sha256: "<missing>".to_string(),
        ledger_seal_algorithm: "sha256".to_string(),
        registry_entry_summary:
            "Registry entry outcome: fallback session is synthetic and must not verify green."
                .to_string(),
        registry_entry_sha256: "<missing>".to_string(),
        registry_entry_algorithm: "sha256".to_string(),
        explicit_statement:
            "the verified provenance-certificate session is the primary input here; this immutable notarization receipt freezes one final operator-facing ledger seal over the canonical chain fingerprint without enabling production execution"
                .to_string(),
    }
}

fn fallback_notarization_receipt_status(
    _snapshot: &LatestFilingCertificateSnapshot,
    session_dir: &Path,
) -> ArchiveReceiptStatusView {
    ArchiveReceiptStatusView {
        session_dir: session_dir.display().to_string(),
        filing_certificate_session_dir: "<missing>".to_string(),
        result: "refused_now_by_invalid_or_drifted_contract".to_string(),
        reason: "<missing>".to_string(),
        handoff_bundle_result: None,
        decision_packet_result: None,
        execute_frozen_result: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        filing_certificate_step: None,
        provenance_certificate_summary:
            "Provenance certificate outcome: fallback status is synthetic and must not verify green."
                .to_string(),
        chain_fingerprint_summary:
            "Chain fingerprint identity: fallback status is synthetic and must not verify green."
                .to_string(),
        chain_fingerprint_sha256: "<missing>".to_string(),
        chain_fingerprint_algorithm: "sha256".to_string(),
        release_capsule_digest_manifest_sha256: "<missing>".to_string(),
        release_capsule_digest_manifest_entry_count: 0,
        release_capsule_digest_algorithm: "sha256".to_string(),
        notarization_receipt_summary:
            "Notarization receipt outcome: fallback status is synthetic and must not verify green."
                .to_string(),
        ledger_seal_summary:
            "Ledger seal identity: fallback status is synthetic and must not verify green."
                .to_string(),
        ledger_seal_sha256: "<missing>".to_string(),
        ledger_seal_algorithm: "sha256".to_string(),
        registry_entry_summary:
            "Registry entry outcome: fallback status is synthetic and must not verify green."
                .to_string(),
        registry_entry_sha256: "<missing>".to_string(),
        registry_entry_algorithm: "sha256".to_string(),
        explicit_statement:
            "this notarization receipt is archival and read-only; it never executes or enables the frozen controller"
                .to_string(),
    }
}

fn render_report_lines(report: &PackageArchiveReceiptLatestReport) -> String {
    let mut lines = vec![
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
    ];
    if let Some(value) = &report.root {
        lines.push(format!("root={value}"));
    }
    if let Some(value) = &report.latest_top_step_name {
        lines.push(format!("latest_top_step_name={value}"));
    }
    if let Some(value) = &report.latest_top_session_dir {
        lines.push(format!("latest_top_session_dir={value}"));
    }
    if let Some(value) = &report.session_dir {
        lines.push(format!("session_dir={value}"));
    }
    if let Some(value) = &report.nested_filing_certificate_latest_session_dir {
        lines.push(format!(
            "nested_filing_certificate_latest_session_dir={value}"
        ));
    }
    if let Some(value) = &report.downstream_decision_packet_session_dir {
        lines.push(format!("downstream_decision_packet_session_dir={value}"));
    }
    if let Some(value) = &report.nested_archive_receipt_session_dir {
        lines.push(format!("nested_archive_receipt_session_dir={value}"));
    }
    if let Some(value) = &report.result {
        lines.push(format!("result={value}"));
    }
    if let Some(value) = &report.downstream_archive_receipt_command_summary {
        lines.push(format!(
            "downstream_archive_receipt_command_summary={value}"
        ));
    }
    if let Some(value) = &report.reviewed_frozen_live_cutover_controller_command_summary {
        lines.push(format!(
            "reviewed_frozen_live_cutover_controller_command_summary={value}"
        ));
    }
    lines.push(format!(
        "activation_authorized={}",
        report.activation_authorized
    ));
    lines.push(format!("explicit_statement={}", report.explicit_statement));
    if !report.verification_mismatches.is_empty() {
        lines.push("verification_mismatches:".to_string());
        lines.extend(
            report
                .verification_mismatches
                .iter()
                .map(|entry| format!("  - {entry}")),
        );
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::ffi::OsString;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    struct FakeNotarizationReceiptLatestFixture {
        temp_dir: PathBuf,
        root: PathBuf,
        wrapper_session_dir: PathBuf,
        output_script_path: PathBuf,
        bin_dir: PathBuf,
        latest_top_session_dir: PathBuf,
        historical_execute_frozen_session_dir: PathBuf,
        turn_green_session_dir: PathBuf,
        launch_packet_session_dir: PathBuf,
        package_dir: PathBuf,
        install_root: PathBuf,
        backend_command: PathBuf,
        filing_certificate_latest_plan_json_path: PathBuf,
        filing_certificate_latest_run_json_path: PathBuf,
        filing_certificate_latest_verify_json_path: PathBuf,
        handoff_bundle_run_json_path: PathBuf,
        handoff_bundle_verify_json_path: PathBuf,
        handoff_bundle_session_json_path: PathBuf,
        handoff_bundle_status_json_path: PathBuf,
        frozen_controller_command: String,
    }

    impl FakeNotarizationReceiptLatestFixture {
        fn plan_config(&self) -> Config {
            Config {
                mode: Mode::PlanLatestArchiveReceipt,
                root: Some(self.root.clone()),
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn render_config(&self) -> Config {
            Config {
                mode: Mode::RenderLatestArchiveReceiptScript,
                root: Some(self.root.clone()),
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: Some(self.output_script_path.clone()),
                json: true,
            }
        }

        fn run_config(&self) -> Config {
            Config {
                mode: Mode::RunLatestArchiveReceipt,
                root: Some(self.root.clone()),
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn verify_config(&self) -> Config {
            Config {
                mode: Mode::VerifyLatestArchiveReceipt,
                root: None,
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }
    }

    #[test]
    fn plan_mode_resolves_latest_valid_clerestory_chain_to_exact_expected_nested_latest_filing_certificate_lineage(
    ) {
        let fixture = fake_filing_certificate_latest_fixture(
            "filing_certificate_latest_plan_ok",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let report = plan_latest_archive_receipt_report(&fixture.plan_config()).unwrap();
        let paths = filing_certificate_latest_paths(&fixture.wrapper_session_dir);
        assert_eq!(
            report.verdict,
            TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestPlanReady
        );
        assert_eq!(
            report.latest_top_step_name.as_deref(),
            Some("clerestory_certificate")
        );
        assert_eq!(
            report
                .nested_filing_certificate_latest_session_dir
                .as_deref(),
            Some(
                paths
                    .nested_filing_certificate_latest_session_dir
                    .display()
                    .to_string()
                    .as_str()
            )
        );
        assert_eq!(
            report.downstream_decision_packet_session_dir.as_deref(),
            Some(
                expected_downstream_decision_packet_session_dir(&paths)
                    .display()
                    .to_string()
                    .as_str()
            )
        );
        assert!(report
            .downstream_archive_receipt_command_summary
            .as_deref()
            .unwrap()
            .contains(ARCHIVE_RECEIPT_BIN));
    }

    #[test]
    fn render_mode_emits_exact_downstream_provenance_certificate_contract_not_second_controller_path(
    ) {
        let fixture = fake_filing_certificate_latest_fixture(
            "filing_certificate_latest_render_ok",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let report = render_latest_archive_receipt_script_report(&fixture.render_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestScriptRendered
        );
        let script = fs::read_to_string(&fixture.output_script_path).unwrap();
        assert!(script.contains(FILING_CERTIFICATE_LATEST_BIN));
        assert!(script.contains(ARCHIVE_RECEIPT_BIN));
        assert!(script.contains("--filing-certificate-session-dir"));
        assert!(!script.contains("--activation-ticket-session-dir"));
        assert!(script.contains("--run-live-package-archive-receipt"));
        assert!(script.contains("--verify-live-package-archive-receipt"));
        assert!(!script.contains("copybot_tiny_live_activation_package_live_cutover"));
    }

    #[test]
    fn run_mode_refuses_when_latest_chain_missing_invalid_or_below_clerestory() {
        for (label, verdict, reason, expected) in [
            (
                "filing_certificate_latest_missing",
                "tiny_live_package_filing_certificate_latest_refused_by_missing_latest_chain",
                "no persisted immutable chain exists",
                TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestRefusedByMissingLatestChain,
            ),
            (
                "filing_certificate_latest_invalid",
                "tiny_live_package_filing_certificate_latest_refused_by_invalid_latest_chain",
                "latest immutable chain is incomplete",
                TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestRefusedByInvalidLatestChain,
            ),
            (
                "filing_certificate_latest_below",
                "tiny_live_package_filing_certificate_latest_refused_by_invalid_latest_chain",
                "latest persisted top chain is choir_receipt, which is below clerestory_certificate",
                TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestRefusedByInvalidLatestChain,
            ),
        ] {
            let fixture = fake_filing_certificate_latest_fixture(
                label,
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_go_live_signoff",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);

            let mut plan: Value = load_json(&fixture.filing_certificate_latest_plan_json_path).unwrap();
            plan["verdict"] = json!(verdict);
            plan["reason"] = json!(reason);
            if label.ends_with("below") {
                plan["latest_top_step_name"] = json!("choir_receipt");
            }
            persist_json(&fixture.filing_certificate_latest_plan_json_path, &plan).unwrap();

            let report = run_latest_archive_receipt_report(&fixture.run_config()).unwrap();
            assert_eq!(report.verdict, expected);
        }
    }

    #[test]
    fn plan_mode_maps_real_latest_filing_certificate_drift_verdict_to_local_drift_refusal() {
        let fixture = fake_filing_certificate_latest_fixture(
            "filing_certificate_latest_plan_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let mut plan: Value = load_json(&fixture.filing_certificate_latest_plan_json_path).unwrap();
        plan["verdict"] = json!(
            "tiny_live_package_filing_certificate_latest_refused_by_drifted_registry_entry_contract"
        );
        plan["reason"] = json!("real accepted latest-filing-certificate drift");
        persist_json(&fixture.filing_certificate_latest_plan_json_path, &plan).unwrap();

        let report = run_latest_archive_receipt_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestRefusedByDriftedArchiveReceiptContract
        );
        assert_eq!(
            report.result.as_deref(),
            Some("refused_by_drifted_archive_receipt_contract")
        );
    }

    #[test]
    fn run_mode_takes_terminal_drift_refusal_when_nested_latest_filing_certificate_reports_real_drift(
    ) {
        let fixture = fake_filing_certificate_latest_fixture(
            "filing_certificate_latest_run_drift",
            "refused_by_drifted_registry_entry_contract",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let report = run_latest_archive_receipt_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestRefusedByDriftedArchiveReceiptContract
        );
        assert_eq!(
            report.result.as_deref(),
            Some("refused_by_drifted_archive_receipt_contract")
        );
        assert_eq!(report.archive_receipt_verdict.as_deref(), None);
    }

    #[test]
    fn run_mode_refuses_when_nested_release_capsule_lineage_drifts_from_latest_immutable_chain() {
        let fixture = fake_filing_certificate_latest_fixture(
            "filing_certificate_latest_handoff_bundle_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let mut run: Value = load_json(&fixture.filing_certificate_latest_run_json_path).unwrap();
        run["package_dir"] = json!("/tampered/package");
        persist_json(&fixture.filing_certificate_latest_run_json_path, &run).unwrap();

        let report = run_latest_archive_receipt_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestRefusedByDriftedArchiveReceiptContract
        );
        assert!(report
            .reason
            .contains("stored latest-filing-certificate run package_dir"));
    }

    #[test]
    fn verify_mode_fails_when_persisted_latest_filing_certificate_artifacts_are_tampered_or_missing_nested_lineage_truth(
    ) {
        {
            let fixture = fake_filing_certificate_latest_fixture(
                "filing_certificate_latest_verify_tampered_copy",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_go_live_signoff",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_latest_archive_receipt_report(&fixture.run_config()).unwrap();

            let paths = filing_certificate_latest_paths(&fixture.wrapper_session_dir);
            let mut report: Value = load_json(&paths.archive_receipt_report_path).unwrap();
            report["reason"] = json!("tampered stored provenance-certificate copy reason");
            persist_json(&paths.archive_receipt_report_path, &report).unwrap();

            let verify = verify_latest_archive_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestVerifyInvalid
            );
            assert!(verify
                .verification_mismatches
                .iter()
                .any(|entry| entry.contains("stored provenance-certificate copy reason")));
        }

        {
            let fixture = fake_filing_certificate_latest_fixture(
                "filing_certificate_latest_verify_missing_nested_truth",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_go_live_signoff",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_latest_archive_receipt_report(&fixture.run_config()).unwrap();

            let paths = filing_certificate_latest_paths(&fixture.wrapper_session_dir);
            fs::remove_file(nested_filing_certificate_latest_report_path(
                &paths.nested_filing_certificate_latest_session_dir,
            ))
            .unwrap();

            let verify = verify_latest_archive_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestVerifyInvalid
            );
            assert!(verify.verification_mismatches.iter().any(|entry| {
                entry.contains("tiny_live_activation_package_filing_certificate_latest.report.json")
            }));
        }
    }

    #[test]
    fn verify_fails_when_copied_nested_latest_filing_certificate_explicit_statement_drifts() {
        let fixture = fake_filing_certificate_latest_fixture(
            "filing_certificate_latest_verify_launch_packet_explicit_statement_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_archive_receipt_report(&fixture.run_config()).unwrap();

        let paths = filing_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.latest_archive_receipt_report_path).unwrap();
        report["explicit_statement"] =
            json!("tampered nested latest release capsule explicit statement");
        persist_json(&paths.latest_archive_receipt_report_path, &report).unwrap();

        let verify = verify_latest_archive_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored latest-filing-certificate copy explicit_statement")
        }));
    }

    #[test]
    fn verify_fails_when_copied_nested_provenance_certificate_decision_packet_result_drifts() {
        let fixture = fake_filing_certificate_latest_fixture(
            "filing_certificate_latest_verify_decision_packet_result_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_archive_receipt_report(&fixture.run_config()).unwrap();

        let paths = filing_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.archive_receipt_report_path).unwrap();
        report["decision_packet_result"] = json!("tampered_decision_packet_result");
        persist_json(&paths.archive_receipt_report_path, &report).unwrap();

        let verify = verify_latest_archive_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored filing-certificate copy decision_packet_result")
        }));
    }

    #[test]
    fn verify_fails_when_copied_nested_notarization_receipt_attestation_seal_session_dir_drifts() {
        let fixture = fake_filing_certificate_latest_fixture(
            "filing_certificate_latest_verify_attestation_seal_session_dir_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_archive_receipt_report(&fixture.run_config()).unwrap();

        let paths = filing_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.archive_receipt_report_path).unwrap();
        report["attestation_seal_session_dir"] = json!("/tampered/attestation-seal-session");
        persist_json(&paths.archive_receipt_report_path, &report).unwrap();

        let verify = verify_latest_archive_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored filing-certificate copy attestation_seal_session_dir")
        }));
    }

    #[test]
    fn verify_fails_when_copied_nested_notarization_receipt_chain_fingerprint_summary_drifts() {
        let fixture = fake_filing_certificate_latest_fixture(
            "filing_certificate_latest_verify_chain_fingerprint_summary_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_archive_receipt_report(&fixture.run_config()).unwrap();

        let paths = filing_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.archive_receipt_report_path).unwrap();
        report["chain_fingerprint_summary"] = json!("tampered chain fingerprint summary");
        persist_json(&paths.archive_receipt_report_path, &report).unwrap();

        let verify = verify_latest_archive_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored filing-certificate copy chain_fingerprint_summary")
        }));
    }

    #[test]
    fn verify_fails_when_copied_nested_notarization_receipt_chain_fingerprint_sha256_drifts() {
        let fixture = fake_filing_certificate_latest_fixture(
            "filing_certificate_latest_verify_chain_fingerprint_sha256_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_archive_receipt_report(&fixture.run_config()).unwrap();

        let paths = filing_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.archive_receipt_report_path).unwrap();
        report["chain_fingerprint_sha256"] = json!("tampered-chain-fingerprint-sha256");
        persist_json(&paths.archive_receipt_report_path, &report).unwrap();

        let verify = verify_latest_archive_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored filing-certificate copy chain_fingerprint_sha256")
        }));
    }

    #[test]
    fn verify_fails_when_copied_nested_notarization_receipt_chain_fingerprint_algorithm_drifts() {
        let fixture = fake_filing_certificate_latest_fixture(
            "filing_certificate_latest_verify_chain_fingerprint_algorithm_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_archive_receipt_report(&fixture.run_config()).unwrap();

        let paths = filing_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.archive_receipt_report_path).unwrap();
        report["chain_fingerprint_algorithm"] = json!("tampered-fingerprint-algorithm");
        persist_json(&paths.archive_receipt_report_path, &report).unwrap();

        let verify = verify_latest_archive_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored filing-certificate copy chain_fingerprint_algorithm")
        }));
    }

    #[test]
    fn verify_fails_when_downstream_decision_packet_confirmation_anchor_drifts() {
        let fixture = fake_filing_certificate_latest_fixture(
            "filing_certificate_latest_verify_decision_packet_anchor_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_archive_receipt_report(&fixture.run_config()).unwrap();

        let paths = filing_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.archive_receipt_report_path).unwrap();
        report["decision_packet_session_dir"] = json!("/tampered/decision-packet-session");
        persist_json(&paths.archive_receipt_report_path, &report).unwrap();

        let verify = verify_latest_archive_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored filing-certificate copy decision_packet_session_dir")
        }));
    }

    #[test]
    fn verify_fails_when_copied_nested_provenance_certificate_execute_frozen_result_drifts() {
        let fixture = fake_filing_certificate_latest_fixture(
            "filing_certificate_latest_verify_current_authorization_result_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_archive_receipt_report(&fixture.run_config()).unwrap();

        let paths = filing_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.archive_receipt_report_path).unwrap();
        report["execute_frozen_result"] = json!("tampered_execute_frozen_result");
        persist_json(&paths.archive_receipt_report_path, &report).unwrap();

        let verify = verify_latest_archive_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored filing-certificate copy execute_frozen_result")
        }));
    }

    #[test]
    fn verify_fails_when_copied_nested_archive_receipt_verdict_drifts() {
        let fixture = fake_filing_certificate_latest_fixture(
            "filing_certificate_latest_verify_release_capsule_verdict_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_archive_receipt_report(&fixture.run_config()).unwrap();

        let paths = filing_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.archive_receipt_report_path).unwrap();
        report["verdict"] = json!("tampered_release_capsule_verdict");
        persist_json(&paths.archive_receipt_report_path, &report).unwrap();

        let verify = verify_latest_archive_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestVerifyInvalid
        );
        assert!(verify
            .verification_mismatches
            .iter()
            .any(|entry| entry.contains("stored filing-certificate copy verdict")));
    }

    #[test]
    fn verify_fails_when_copied_nested_provenance_certificate_current_live_cutover_controller_summary_drifts(
    ) {
        let fixture = fake_filing_certificate_latest_fixture(
            "filing_certificate_latest_verify_current_live_cutover_summary_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_archive_receipt_report(&fixture.run_config()).unwrap();

        let paths = filing_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.archive_receipt_report_path).unwrap();
        report["reviewed_frozen_live_cutover_controller_command_summary"] =
            json!("tampered live cutover controller summary");
        persist_json(&paths.archive_receipt_report_path, &report).unwrap();

        let verify = verify_latest_archive_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains(
                "stored archive-receipt copy reviewed_frozen_live_cutover_controller_command_summary",
            )
        }));
    }

    #[test]
    fn all_modes_preserve_stage3_and_pre_activation_refusal_semantics_and_do_not_imply_activation_authorization_by_themselves(
    ) {
        {
            let fixture = fake_filing_certificate_latest_fixture(
                "filing_certificate_latest_stage3",
                "refused_now_by_stage3",
                "refused_now_by_stage3",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);

            let plan = plan_latest_archive_receipt_report(&fixture.plan_config()).unwrap();
            assert_eq!(
                plan.verdict,
                TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestPlanReady
            );
            assert!(!plan.activation_authorized);

            let run = run_latest_archive_receipt_report(&fixture.run_config()).unwrap();
            assert_eq!(
                run.verdict,
                TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestRefusedNowByStage3
            );
            assert_eq!(run.result.as_deref(), Some("refused_now_by_stage3"));
            assert!(!run.activation_authorized);

            let verify = verify_latest_archive_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestVerifyOk,
                "{:#?}",
                verify.verification_mismatches
            );
            assert!(!verify.activation_authorized);
        }

        {
            let fixture = fake_filing_certificate_latest_fixture(
                "filing_certificate_latest_pre_activation",
                "refused_now_by_pre_activation_gate",
                "refused_now_by_pre_activation_gate",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);

            let run = run_latest_archive_receipt_report(&fixture.run_config()).unwrap();
            assert_eq!(
                run.verdict,
                TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestRefusedNowByPreActivationGate
            );
            assert_eq!(
                run.result.as_deref(),
                Some("refused_now_by_pre_activation_gate")
            );
            assert!(!run.activation_authorized);

            let verify = verify_latest_archive_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestVerifyOk,
                "{:#?}",
                verify.verification_mismatches
            );
            assert!(!verify.activation_authorized);
        }

        {
            let fixture = fake_filing_certificate_latest_fixture(
                "filing_certificate_latest_green",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_go_live_signoff",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);

            let run = run_latest_archive_receipt_report(&fixture.run_config()).unwrap();
            assert_eq!(
                run.verdict,
                TinyLivePackageArchiveReceiptLatestVerdict::TinyLivePackageArchiveReceiptLatestReadyForManualExecutionWhenGateTurnsGreen
            );
            assert_eq!(
                run.result.as_deref(),
                Some("ready_for_manual_execution_when_gate_turns_green")
            );
            assert!(!run.activation_authorized);
            assert!(run
                .explicit_statement
                .contains("never runs the frozen live cutover controller"));
        }
    }

    fn fake_filing_certificate_latest_fixture(
        label: &str,
        filing_certificate_latest_run_result: &str,
        handoff_bundle_result: &str,
    ) -> FakeNotarizationReceiptLatestFixture {
        let temp_dir = temp_dir(label);
        let root = temp_dir.join("root");
        let wrapper_session_dir = temp_dir.join("notarization-receipt-latest-session");
        let output_script_path = temp_dir.join("notarization-receipt-latest.sh");
        let bin_dir = temp_dir.join("bin");
        let latest_top_session_dir = temp_dir.join("latest-clerestory-session");
        let historical_execute_frozen_session_dir =
            temp_dir.join("historical-execute-frozen-session");
        let turn_green_session_dir = temp_dir.join("historical-turn-green-session");
        let launch_packet_session_dir = temp_dir.join("historical-launch-packet-session");
        let package_dir = temp_dir.join("package");
        let install_root = temp_dir.join("install-root");
        let backend_command = temp_dir.join("backend");
        let filing_certificate_latest_plan_json_path =
            temp_dir.join("filing_certificate_latest.plan.json");
        let filing_certificate_latest_run_json_path =
            temp_dir.join("filing_certificate_latest.run.json");
        let filing_certificate_latest_verify_json_path =
            temp_dir.join("filing_certificate_latest.verify.json");
        let handoff_bundle_run_json_path = temp_dir.join("handoff_bundle.run.json");
        let handoff_bundle_verify_json_path = temp_dir.join("handoff_bundle.verify.json");
        let handoff_bundle_session_json_path = temp_dir.join("handoff_bundle.session.json");
        let handoff_bundle_status_json_path = temp_dir.join("handoff_bundle.status.json");
        let frozen_controller_command =
            "copybot_tiny_live_activation_package_live_cutover --package-dir /fake/pkg".to_string();

        for path in [
            &root,
            &bin_dir,
            &latest_top_session_dir,
            &historical_execute_frozen_session_dir,
            &turn_green_session_dir,
            &launch_packet_session_dir,
            &package_dir,
            &install_root,
        ] {
            fs::create_dir_all(path).unwrap();
        }
        fs::write(&backend_command, "#!/bin/sh\n").unwrap();

        let fixture = FakeNotarizationReceiptLatestFixture {
            temp_dir,
            root,
            wrapper_session_dir,
            output_script_path,
            bin_dir,
            latest_top_session_dir,
            historical_execute_frozen_session_dir,
            turn_green_session_dir,
            launch_packet_session_dir,
            package_dir,
            install_root,
            backend_command,
            filing_certificate_latest_plan_json_path,
            filing_certificate_latest_run_json_path,
            filing_certificate_latest_verify_json_path,
            handoff_bundle_run_json_path,
            handoff_bundle_verify_json_path,
            handoff_bundle_session_json_path,
            handoff_bundle_status_json_path,
            frozen_controller_command,
        };

        persist_json(
            &fixture.filing_certificate_latest_plan_json_path,
            &default_filing_certificate_latest_plan_report(&fixture),
        )
        .unwrap();
        persist_json(
            &fixture.filing_certificate_latest_run_json_path,
            &default_filing_certificate_latest_run_report(
                &fixture,
                filing_certificate_latest_run_result,
            ),
        )
        .unwrap();
        persist_json(
            &fixture.filing_certificate_latest_verify_json_path,
            &default_filing_certificate_latest_verify_report(
                &fixture,
                filing_certificate_latest_run_result,
            ),
        )
        .unwrap();
        persist_json(
            &fixture.handoff_bundle_session_json_path,
            &default_provenance_certificate_session(&fixture),
        )
        .unwrap();
        persist_json(
            &fixture.handoff_bundle_status_json_path,
            &default_provenance_certificate_status(&fixture, handoff_bundle_result),
        )
        .unwrap();
        persist_json(
            &fixture.handoff_bundle_run_json_path,
            &default_provenance_certificate_run_report(&fixture, handoff_bundle_result),
        )
        .unwrap();
        persist_json(
            &fixture.handoff_bundle_verify_json_path,
            &default_provenance_certificate_verify_report(&fixture, handoff_bundle_result),
        )
        .unwrap();

        let paths = filing_certificate_latest_paths(&fixture.wrapper_session_dir);
        write_fake_filing_certificate_latest_command(
            &fixture.bin_dir.join(FILING_CERTIFICATE_LATEST_BIN),
            &fixture.filing_certificate_latest_plan_json_path,
            &fixture.filing_certificate_latest_run_json_path,
            &fixture.filing_certificate_latest_verify_json_path,
            &fixture.root,
            &paths.nested_filing_certificate_latest_session_dir,
        );
        write_fake_release_capsule_command(
            &fixture.bin_dir.join(ARCHIVE_RECEIPT_BIN),
            &fixture.handoff_bundle_run_json_path,
            &fixture.handoff_bundle_verify_json_path,
            &fixture.handoff_bundle_session_json_path,
            &fixture.handoff_bundle_status_json_path,
            &expected_downstream_filing_certificate_session_dir(&paths),
            &expected_downstream_decision_packet_session_dir(&paths),
            &paths.nested_archive_receipt_session_dir,
        );

        fixture
    }

    fn default_filing_certificate_latest_plan_report(
        fixture: &FakeNotarizationReceiptLatestFixture,
    ) -> Value {
        let paths = filing_certificate_latest_paths(&fixture.wrapper_session_dir);
        json!({
            "generated_at": Utc::now(),
            "mode": "plan_latest_filing_certificate",
            "verdict": FILING_CERTIFICATE_LATEST_PLAN_READY,
            "reason": "latest immutable chain resolves cleanly into accepted latest-filing-certificate contract",
            "latest_top_step_name": "clerestory_certificate",
            "latest_top_session_dir": fixture.latest_top_session_dir.display().to_string(),
            "historical_execute_frozen_session_dir": fixture.historical_execute_frozen_session_dir.display().to_string(),
            "turn_green_session_dir": fixture.turn_green_session_dir.display().to_string(),
            "launch_packet_session_dir": fixture.launch_packet_session_dir.display().to_string(),
            "package_dir": fixture.package_dir.display().to_string(),
            "install_root": fixture.install_root.display().to_string(),
            "target_service_name": "solana-copy-bot.service",
            "backend_command": fixture.backend_command.display().to_string(),
            "wrapper_timeout_ms": 1200,
            "service_status_max_staleness_ms": 4500,
            "session_dir": paths.nested_filing_certificate_latest_session_dir.display().to_string(),
            "downstream_decision_packet_session_dir": expected_downstream_decision_packet_session_dir(&paths).display().to_string(),
            "latest_execute_verdict": "tiny_live_package_execute_latest_plan_ready",
            "decision_packet_plan_verdict": "tiny_live_package_decision_packet_plan_ready",
            "reviewed_frozen_live_cutover_controller_command_summary": fixture.frozen_controller_command,
            "clerestory_certificate_summary": "latest clerestory summary",
            "clerestory_certificate_sha256": "latest-clerestory-sha",
            "clerestory_certificate_algorithm": "sha256",
            "activation_authorized": false,
            "explicit_statement": "latest filing-certificate wrapper plan"
        })
    }

    fn canonical_filing_certificate_latest_result(result: &str) -> &str {
        match result {
            "runnable_when_gate_truth_turns_green" => {
                "ready_for_manual_execution_when_gate_turns_green"
            }
            other => other,
        }
    }

    fn canonical_release_capsule_result(result: &str) -> &str {
        match result {
            "ready_for_manual_go_live_signoff" => {
                "ready_for_manual_execution_when_gate_turns_green"
            }
            other => other,
        }
    }

    fn default_filing_certificate_latest_run_report(
        fixture: &FakeNotarizationReceiptLatestFixture,
        result: &str,
    ) -> Value {
        let result = canonical_filing_certificate_latest_result(result);
        let paths = filing_certificate_latest_paths(&fixture.wrapper_session_dir);
        let (verdict, gate_verdict, gate_reason) = match result {
            "ready_for_manual_execution_when_gate_turns_green" => (
                "tiny_live_package_filing_certificate_latest_ready_for_manual_execution_when_gate_turns_green",
                Value::Null,
                Value::Null,
            ),
            "refused_now_by_stage3" => (
                "tiny_live_package_filing_certificate_latest_refused_now_by_stage3",
                json!("blocked"),
                json!("stage3 blocked"),
            ),
            "refused_now_by_pre_activation_gate" => (
                "tiny_live_package_filing_certificate_latest_refused_now_by_pre_activation_gate",
                json!("blocked"),
                json!("pre gate blocked"),
            ),
            "refused_by_missing_latest_chain" => (
                "tiny_live_package_filing_certificate_latest_refused_by_missing_latest_chain",
                Value::Null,
                Value::Null,
            ),
            "refused_by_invalid_latest_chain" => (
                "tiny_live_package_filing_certificate_latest_refused_by_invalid_latest_chain",
                Value::Null,
                Value::Null,
            ),
            _ => (
                "tiny_live_package_filing_certificate_latest_refused_by_drifted_registry_entry_contract",
                Value::Null,
                Value::Null,
            ),
        };
        json!({
            "generated_at": Utc::now(),
            "mode": "run_latest_filing_certificate",
            "verdict": verdict,
            "reason": format!("latest filing-certificate {result}"),
            "latest_top_step_name": "clerestory_certificate",
            "latest_top_session_dir": fixture.latest_top_session_dir.display().to_string(),
            "historical_execute_frozen_session_dir": fixture.historical_execute_frozen_session_dir.display().to_string(),
            "turn_green_session_dir": fixture.turn_green_session_dir.display().to_string(),
            "launch_packet_session_dir": fixture.launch_packet_session_dir.display().to_string(),
            "package_dir": fixture.package_dir.display().to_string(),
            "install_root": fixture.install_root.display().to_string(),
            "target_service_name": "solana-copy-bot.service",
            "backend_command": fixture.backend_command.display().to_string(),
            "wrapper_timeout_ms": 1200,
            "service_status_max_staleness_ms": 4500,
            "session_dir": paths.nested_filing_certificate_latest_session_dir.display().to_string(),
            "downstream_decision_packet_session_dir": expected_downstream_decision_packet_session_dir(&paths).display().to_string(),
            "latest_execute_verdict": "tiny_live_package_execute_latest_plan_ready",
            "decision_packet_plan_verdict": "tiny_live_package_decision_packet_plan_ready",
            "result": result,
            "current_pre_activation_gate_verdict": gate_verdict,
            "current_pre_activation_gate_reason": gate_reason,
            "reviewed_frozen_live_cutover_controller_command_summary": fixture.frozen_controller_command,
            "clerestory_certificate_summary": "latest clerestory summary",
            "clerestory_certificate_sha256": "latest-clerestory-sha",
            "clerestory_certificate_algorithm": "sha256",
            "activation_authorized": false,
            "explicit_statement": "latest filing-certificate wrapper run"
        })
    }

    fn default_filing_certificate_latest_verify_report(
        fixture: &FakeNotarizationReceiptLatestFixture,
        result: &str,
    ) -> Value {
        let mut report = default_filing_certificate_latest_run_report(fixture, result);
        report["mode"] = json!("verify_latest_filing_certificate");
        report["verdict"] = json!(FILING_CERTIFICATE_LATEST_VERIFY_OK);
        report["reason"] = json!("verify ok");
        report
    }

    fn fake_release_capsule_audit_manifest_summary() -> &'static str {
        "Hash-locked audit manifest: sha256 digests over 9 archival members; any content drift invalidates this release capsule"
    }

    fn fake_release_capsule_digest_manifest_sha256() -> &'static str {
        "attestation-seal-manifest-sha256"
    }

    fn fake_release_capsule_summary(result: &str, frozen_controller_command: &str) -> String {
        match result {
            "ready_for_manual_execution_when_gate_turns_green" => format!(
                "Release capsule outcome: this immutable release record is ready for manual execution when gate truth turns green; the exact reviewed frozen controller command remains: {frozen_controller_command}"
            ),
            "refused_now_by_stage3" => {
                "Release capsule outcome: the immutable reviewed chain remains refused now by Stage 3; archive this capsule as tamper-evident refusal evidence only and do not run the frozen controller.".to_string()
            }
            "refused_now_by_pre_activation_gate" => {
                "Release capsule outcome: the immutable reviewed chain remains refused now by the pre-activation gate; archive this capsule as tamper-evident refusal evidence only and do not run the frozen controller.".to_string()
            }
            _ => {
                "Release capsule outcome: the immutable reviewed chain is invalid or drifted; archive this capsule for audit only and mint a new coherent warrant chain before any later execution record.".to_string()
            }
        }
    }

    fn fake_attestation_reason(result: &str) -> &'static str {
        match result {
            "ready_for_manual_execution_when_gate_turns_green" => {
                "the immutable release capsule is coherent and this attestation seal is ready for manual execution when gate truth turns green"
            }
            "refused_now_by_stage3" => {
                "the immutable release capsule remains refused now by Stage 3, so this attestation seal remains an explicit refusal record"
            }
            "refused_now_by_pre_activation_gate" => {
                "the immutable release capsule remains refused now by the pre-activation gate, so this attestation seal remains an explicit refusal record"
            }
            _ => {
                "the verified attestation-seal chain is invalid, drifted, or non-runnable, so this attestation seal remains refused"
            }
        }
    }

    fn fake_provenance_certificate_summary(
        result: &str,
        frozen_controller_command: &str,
    ) -> String {
        match result {
            "ready_for_manual_execution_when_gate_turns_green" => format!(
                "Attestation seal outcome: this immutable custody record is ready for manual execution when gate truth turns green; the exact reviewed frozen controller command remains: {frozen_controller_command}"
            ),
            "refused_now_by_stage3" => {
                "Attestation seal outcome: the immutable reviewed chain remains refused now by Stage 3; archive this final seal as custody evidence only and do not run the frozen controller.".to_string()
            }
            "refused_now_by_pre_activation_gate" => {
                "Attestation seal outcome: the immutable reviewed chain remains refused now by the pre-activation gate; archive this final seal as custody evidence only and do not run the frozen controller.".to_string()
            }
            _ => {
                "Attestation seal outcome: the immutable reviewed chain is invalid or drifted; archive this final seal for audit only and mint a new coherent custody record before any later execution record.".to_string()
            }
        }
    }

    fn fake_notarization_receipt_summary() -> String {
        format!(
            "Notarization receipt outcome: nested provenance-certificate truth remains archival and read-only while freezing one final ledger seal over release-capsule digest manifest sha256 {}",
            fake_release_capsule_digest_manifest_sha256()
        )
    }

    fn fake_registry_entry_summary(result: &str, frozen_controller_command: &str) -> String {
        match result {
            "ready_for_manual_execution_when_gate_turns_green" => format!(
                "Registry entry outcome: this immutable registry-style record is ready for manual execution when gate truth turns green; the exact reviewed frozen controller command remains: {frozen_controller_command}"
            ),
            "refused_now_by_stage3" => {
                "Registry entry outcome: the immutable sealed chain remains refused now by Stage 3; archive this final registry record as refusal evidence only and do not run the frozen controller.".to_string()
            }
            "refused_now_by_pre_activation_gate" => {
                "Registry entry outcome: the immutable sealed chain remains refused now by the pre-activation gate; archive this final registry record as refusal evidence only and do not run the frozen controller.".to_string()
            }
            _ => {
                "Registry entry outcome: the immutable sealed chain is invalid or drifted; archive this final registry record for audit only and mint a new coherent entry before any later execution record.".to_string()
            }
        }
    }

    fn default_provenance_certificate_session(
        fixture: &FakeNotarizationReceiptLatestFixture,
    ) -> Value {
        let paths = filing_certificate_latest_paths(&fixture.wrapper_session_dir);
        let filing_certificate_session_dir =
            expected_downstream_filing_certificate_session_dir(&paths);
        let provenance_certificate_session_dir = fixture
            .temp_dir
            .join("downstream-registry-entry-provenance-certificate-session");
        let attestation_seal_session_dir = fixture
            .temp_dir
            .join("downstream-provenance-certificate-attestation-seal-session");
        let activation_ticket_session_dir = fixture
            .temp_dir
            .join("downstream-provenance-certificate-activation-ticket-session");
        let review_receipt_session_dir = expected_downstream_review_receipt_session_dir(&paths);
        let handoff_bundle_session_dir = fixture
            .temp_dir
            .join("downstream-provenance-certificate-handoff-bundle-session");
        let snapshot = LatestFilingCertificateSnapshot {
            latest_top_step_name: "clerestory_certificate".to_string(),
            latest_top_session_dir: fixture.latest_top_session_dir.clone(),
            downstream_decision_packet_session_dir: expected_downstream_decision_packet_session_dir(
                &paths,
            ),
            historical_execute_frozen_session_dir: fixture
                .historical_execute_frozen_session_dir
                .clone(),
            turn_green_session_dir: fixture.turn_green_session_dir.clone(),
            launch_packet_session_dir: fixture.launch_packet_session_dir.clone(),
            package_dir: fixture.package_dir.clone(),
            install_root: fixture.install_root.clone(),
            target_service_name: "solana-copy-bot.service".to_string(),
            backend_command: fixture.backend_command.display().to_string(),
            wrapper_timeout_ms: 1200,
            service_status_max_staleness_ms: 4500,
            reviewed_frozen_live_cutover_controller_command_summary: fixture
                .frozen_controller_command
                .clone(),
            clerestory_certificate_summary: "latest clerestory summary".to_string(),
            clerestory_certificate_sha256: "latest-clerestory-sha".to_string(),
            clerestory_certificate_algorithm: "sha256".to_string(),
        };
        json!({
            "session_version": "1",
            "planned_at": Utc::now(),
            "filing_certificate_session_dir": filing_certificate_session_dir.display().to_string(),
            "provenance_certificate_session_dir": provenance_certificate_session_dir.display().to_string(),
            "attestation_seal_session_dir": attestation_seal_session_dir.display().to_string(),
            "release_capsule_session_dir": filing_certificate_session_dir.display().to_string(),
            "activation_ticket_session_dir": activation_ticket_session_dir.display().to_string(),
            "review_receipt_session_dir": review_receipt_session_dir.display().to_string(),
            "handoff_bundle_session_dir": handoff_bundle_session_dir.display().to_string(),
            "decision_packet_session_dir": snapshot.downstream_decision_packet_session_dir.display().to_string(),
            "execute_frozen_session_dir": snapshot.historical_execute_frozen_session_dir.display().to_string(),
            "turn_green_session_dir": snapshot.turn_green_session_dir.display().to_string(),
            "launch_packet_session_dir": snapshot.launch_packet_session_dir.display().to_string(),
            "package_dir": snapshot.package_dir.display().to_string(),
            "install_root": snapshot.install_root.display().to_string(),
            "target_service_name": snapshot.target_service_name,
            "backend_command": snapshot.backend_command,
            "wrapper_timeout_ms": snapshot.wrapper_timeout_ms,
            "service_status_max_staleness_ms": snapshot.service_status_max_staleness_ms,
            "session_dir": filing_certificate_latest_paths(&fixture.wrapper_session_dir).nested_archive_receipt_session_dir.display().to_string(),
            "verify_filing_certificate_command_summary": archive_receipt_verify_filing_certificate_command_summary(&filing_certificate_session_dir),
            "reviewed_frozen_live_cutover_controller_command_summary": fixture.frozen_controller_command,
            "provenance_certificate_summary": fake_provenance_certificate_summary(
                "ready_for_manual_execution_when_gate_turns_green",
                &fixture.frozen_controller_command
            ),
            "chain_fingerprint_summary": "Chain fingerprint identity: fixture provenance fingerprint over nested release-capsule digest manifest truth.",
            "chain_fingerprint_sha256": "fixture-provenance-fingerprint-sha256",
            "chain_fingerprint_algorithm": "sha256",
            "release_capsule_digest_manifest_sha256": fake_release_capsule_digest_manifest_sha256(),
            "release_capsule_digest_manifest_entry_count": 9,
            "release_capsule_digest_algorithm": "sha256",
            "notarization_receipt_summary": fake_notarization_receipt_summary(),
            "ledger_seal_summary": "Ledger seal identity: fixture notarization seal over nested chain fingerprint truth.",
            "ledger_seal_sha256": "fixture-notarization-ledger-seal-sha256",
            "ledger_seal_algorithm": "sha256",
            "registry_entry_summary": fake_registry_entry_summary(
                "ready_for_manual_execution_when_gate_turns_green",
                &fixture.frozen_controller_command
            ),
            "registry_entry_sha256": "fixture-registry-entry-sha256",
            "registry_entry_algorithm": "sha256",
            "explicit_statement": "the verified provenance-certificate session is the primary input here; this immutable notarization receipt freezes one final operator-facing ledger seal over the canonical chain fingerprint without enabling production execution"
        })
    }

    fn default_provenance_certificate_status(
        fixture: &FakeNotarizationReceiptLatestFixture,
        result: &str,
    ) -> Value {
        let result = canonical_release_capsule_result(result);
        let paths = filing_certificate_latest_paths(&fixture.wrapper_session_dir);
        let (reason, gate_verdict, gate_reason) = match result {
            "ready_for_manual_execution_when_gate_turns_green" => {
                (fake_attestation_reason(result), json!("green"), json!("ok"))
            }
            "refused_now_by_stage3" => (
                fake_attestation_reason(result),
                json!("blocked"),
                json!("stage3 blocked"),
            ),
            "refused_now_by_pre_activation_gate" => (
                fake_attestation_reason(result),
                json!("blocked"),
                json!("pre gate blocked"),
            ),
            _ => (fake_attestation_reason(result), Value::Null, Value::Null),
        };
        json!({
            "status_version": "1",
            "updated_at": Utc::now(),
            "session_dir": paths.nested_archive_receipt_session_dir.display().to_string(),
            "filing_certificate_session_dir": expected_downstream_filing_certificate_session_dir(&paths).display().to_string(),
            "result": result,
            "reason": reason,
            "handoff_bundle_result": if result == "ready_for_manual_execution_when_gate_turns_green" { json!("ready_for_manual_go_live_review") } else if result == "refused_now_by_stage3" { json!("refused_now_by_stage3") } else { json!("refused_now_by_pre_activation_gate") },
            "decision_packet_result": if result == "ready_for_manual_execution_when_gate_turns_green" { json!("runnable_when_gate_truth_turns_green") } else if result == "refused_now_by_stage3" { json!("refused_now_by_stage3") } else { json!("refused_now_by_pre_activation_gate") },
            "execute_frozen_result": "completed_keep_running",
            "current_pre_activation_gate_verdict": gate_verdict,
            "current_pre_activation_gate_reason": gate_reason,
            "filing_certificate_step": Value::Null,
            "provenance_certificate_summary": fake_provenance_certificate_summary(result, &fixture.frozen_controller_command),
            "chain_fingerprint_summary": "Chain fingerprint identity: fixture status provenance fingerprint.",
            "chain_fingerprint_sha256": "fixture-provenance-fingerprint-sha256",
            "chain_fingerprint_algorithm": "sha256",
            "release_capsule_digest_manifest_sha256": fake_release_capsule_digest_manifest_sha256(),
            "release_capsule_digest_manifest_entry_count": 9,
            "release_capsule_digest_algorithm": "sha256",
            "notarization_receipt_summary": fake_notarization_receipt_summary(),
            "ledger_seal_summary": "Ledger seal identity: fixture status notarization seal.",
            "ledger_seal_sha256": "fixture-notarization-ledger-seal-sha256",
            "ledger_seal_algorithm": "sha256",
            "registry_entry_summary": fake_registry_entry_summary(result, &fixture.frozen_controller_command),
            "registry_entry_sha256": "fixture-registry-entry-sha256",
            "registry_entry_algorithm": "sha256",
            "explicit_statement": "this registry entry is archival and read-only; it never executes or enables the frozen controller"
        })
    }

    fn default_provenance_certificate_run_report(
        fixture: &FakeNotarizationReceiptLatestFixture,
        result: &str,
    ) -> Value {
        let result = canonical_release_capsule_result(result);
        let paths = filing_certificate_latest_paths(&fixture.wrapper_session_dir);
        let status = default_provenance_certificate_status(fixture, result);
        let archive_receipt_verdict = match result {
            "ready_for_manual_execution_when_gate_turns_green" => {
                "tiny_live_package_archive_receipt_ready_for_manual_execution_when_gate_turns_green"
            }
            "refused_now_by_stage3" => "tiny_live_package_archive_receipt_refused_now_by_stage3",
            "refused_now_by_pre_activation_gate" => {
                "tiny_live_package_archive_receipt_refused_now_by_pre_activation_gate"
            }
            _ => "tiny_live_package_archive_receipt_refused_now_by_invalid_or_drifted_contract",
        };
        json!({
            "generated_at": Utc::now(),
            "mode": "run_live_package_archive_receipt",
            "verdict": archive_receipt_verdict,
            "reason": status["reason"],
            "filing_certificate_session_dir": expected_downstream_filing_certificate_session_dir(&paths).display().to_string(),
            "provenance_certificate_session_dir": fixture.temp_dir.join("downstream-registry-entry-provenance-certificate-session").display().to_string(),
            "attestation_seal_session_dir": fixture.temp_dir.join("downstream-provenance-certificate-attestation-seal-session").display().to_string(),
            "release_capsule_session_dir": expected_downstream_filing_certificate_session_dir(&paths).display().to_string(),
            "activation_ticket_session_dir": fixture.temp_dir.join("downstream-provenance-certificate-activation-ticket-session").display().to_string(),
            "review_receipt_session_dir": expected_downstream_review_receipt_session_dir(&paths).display().to_string(),
            "handoff_bundle_session_dir": fixture.temp_dir.join("downstream-provenance-certificate-handoff-bundle-session").display().to_string(),
            "decision_packet_session_dir": expected_downstream_decision_packet_session_dir(&paths).display().to_string(),
            "execute_frozen_session_dir": fixture.historical_execute_frozen_session_dir.display().to_string(),
            "turn_green_session_dir": fixture.turn_green_session_dir.display().to_string(),
            "launch_packet_session_dir": fixture.launch_packet_session_dir.display().to_string(),
            "package_dir": fixture.package_dir.display().to_string(),
            "install_root": fixture.install_root.display().to_string(),
            "target_service_name": "solana-copy-bot.service",
            "backend_command": fixture.backend_command.display().to_string(),
            "wrapper_timeout_ms": 1200,
            "service_status_max_staleness_ms": 4500,
            "session_dir": paths.nested_archive_receipt_session_dir.display().to_string(),
            "archive_receipt_session_path": nested_archive_receipt_session_path(&paths.nested_archive_receipt_session_dir).display().to_string(),
            "archive_receipt_status_path": nested_archive_receipt_status_path(&paths.nested_archive_receipt_session_dir).display().to_string(),
            "archived_filing_certificate_report_path": paths.nested_archive_receipt_session_dir.join("tiny_live_activation_package_archive_receipt.filing_certificate.report.json").display().to_string(),
            "result": result,
            "handoff_bundle_result": status["handoff_bundle_result"],
            "decision_packet_result": status["decision_packet_result"],
            "execute_frozen_result": status["execute_frozen_result"],
            "filing_certificate_step": Value::Null,
            "verify_filing_certificate_command_summary": archive_receipt_verify_filing_certificate_command_summary(&expected_downstream_filing_certificate_session_dir(&paths)),
            "reviewed_frozen_live_cutover_controller_command_summary": fixture.frozen_controller_command,
            "provenance_certificate_summary": status["provenance_certificate_summary"],
            "chain_fingerprint_summary": status["chain_fingerprint_summary"],
            "chain_fingerprint_sha256": status["chain_fingerprint_sha256"],
            "chain_fingerprint_algorithm": status["chain_fingerprint_algorithm"],
            "release_capsule_digest_manifest_sha256": status["release_capsule_digest_manifest_sha256"],
            "release_capsule_digest_manifest_entry_count": status["release_capsule_digest_manifest_entry_count"],
            "release_capsule_digest_algorithm": status["release_capsule_digest_algorithm"],
            "notarization_receipt_summary": status["notarization_receipt_summary"],
            "ledger_seal_summary": status["ledger_seal_summary"],
            "ledger_seal_sha256": status["ledger_seal_sha256"],
            "ledger_seal_algorithm": status["ledger_seal_algorithm"],
            "registry_entry_summary": status["registry_entry_summary"],
            "registry_entry_sha256": status["registry_entry_sha256"],
            "registry_entry_algorithm": status["registry_entry_algorithm"],
            "current_pre_activation_gate_verdict": status["current_pre_activation_gate_verdict"],
            "current_pre_activation_gate_reason": status["current_pre_activation_gate_reason"],
            "explicit_statement": status["explicit_statement"]
        })
    }

    fn default_provenance_certificate_verify_report(
        fixture: &FakeNotarizationReceiptLatestFixture,
        result: &str,
    ) -> Value {
        let mut report = default_provenance_certificate_run_report(fixture, result);
        report["mode"] = json!("verify_live_package_archive_receipt");
        report["verdict"] = json!(ARCHIVE_RECEIPT_VERIFY_OK);
        report["reason"] = json!("verify ok");
        report["explicit_statement"] = json!("this verification binds one immutable registry entry to verified notarization-receipt truth, the exact reviewed frozen controller summary, the canonical chain fingerprint, and the final sealed registry identity");
        report
    }

    fn write_fake_filing_certificate_latest_command(
        path: &Path,
        plan_json_path: &Path,
        run_json_path: &Path,
        verify_json_path: &Path,
        root: &Path,
        session_dir: &Path,
    ) {
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --session-dir {} \"*) : ;;\n  *) echo 'unexpected latest-filing-certificate session-dir' >&2; exit 1;;\nesac\nmode=''\ncase \" $* \" in\n  *\" --plan-latest-filing-certificate \"*) mode='plan';;\n  *\" --run-latest-filing-certificate \"*) mode='run';;\n  *\" --verify-latest-filing-certificate \"*) mode='verify';;\n  *) echo 'unexpected latest-filing-certificate mode' >&2; exit 1;;\nesac\ncase \"$mode\" in\n  plan|run)\n    case \" $* \" in\n      *\" --root {} \"*) : ;;\n      *) echo 'unexpected root' >&2; exit 1;;\n    esac\n    ;;\nesac\ncase \"$mode\" in\n  plan) cat '{}';;\n  run) mkdir -p '{}'; cp '{}' '{}/tiny_live_activation_package_filing_certificate_latest.report.json'; cat '{}';;\n  verify) cat '{}';;\n  *) exit 1;;\nesac\n",
            session_dir.display(),
            root.display(),
            plan_json_path.display(),
            session_dir.display(),
            run_json_path.display(),
            session_dir.display(),
            run_json_path.display(),
            verify_json_path.display(),
        );
        fs::write(path, script).unwrap();
        let mut perms = fs::metadata(path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).unwrap();
    }

    fn write_fake_release_capsule_command(
        path: &Path,
        run_json_path: &Path,
        verify_json_path: &Path,
        session_json_path: &Path,
        status_json_path: &Path,
        release_capsule_session_dir: &Path,
        decision_packet_session_dir: &Path,
        session_dir: &Path,
    ) {
        let nested_session_path = nested_archive_receipt_session_path(session_dir);
        let nested_status_path = nested_archive_receipt_status_path(session_dir);
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --filing-certificate-session-dir {} \"*) : ;;\n  *) echo 'unexpected registry-entry notarization-receipt-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --confirm-decision-packet-session-dir {} \"*) : ;;\n  *) echo 'unexpected registry-entry confirm-decision-packet-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --session-dir {} \"*) : ;;\n  *) echo 'unexpected registry-entry session-dir' >&2; exit 1;;\nesac\nmode=''\ncase \" $* \" in\n  *\" --run-live-package-archive-receipt \"*) mode='run';;\n  *\" --verify-live-package-archive-receipt \"*) mode='verify';;\n  *) echo 'unexpected registry-entry mode' >&2; exit 1;;\nesac\ncase \"$mode\" in\n  run) mkdir -p '{}'; cp '{}' '{}'; cp '{}' '{}'; cat '{}';;\n  verify) cat '{}';;\n  *) exit 1;;\nesac\n",
            release_capsule_session_dir.display(),
            decision_packet_session_dir.display(),
            session_dir.display(),
            session_dir.display(),
            session_json_path.display(),
            nested_session_path.display(),
            status_json_path.display(),
            nested_status_path.display(),
            run_json_path.display(),
            verify_json_path.display(),
        );
        fs::write(path, script).unwrap();
        let mut perms = fs::metadata(path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).unwrap();
    }

    struct PathGuard {
        _lock: MutexGuard<'static, ()>,
        original_path: Option<OsString>,
    }

    impl PathGuard {
        fn install(bin_dir: &Path) -> Self {
            let lock = env_lock()
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let original_path = std::env::var_os("PATH");
            let mut new_path = OsString::from(bin_dir.as_os_str());
            if let Some(existing) = &original_path {
                new_path.push(":");
                new_path.push(existing);
            }
            std::env::set_var("PATH", &new_path);
            Self {
                _lock: lock,
                original_path,
            }
        }
    }

    impl Drop for PathGuard {
        fn drop(&mut self) {
            match &self.original_path {
                Some(value) => std::env::set_var("PATH", value),
                None => std::env::remove_var("PATH"),
            }
        }
    }

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn temp_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_nanos(0))
            .as_nanos();
        let path = std::env::temp_dir().join(format!("{label}.{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }
}
