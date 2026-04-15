#![recursion_limit = "512"]

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const USAGE: &str = "usage: copybot_tiny_live_activation_package_choir_receipt_latest --root <path> [--session-dir <path>] [--output <path>] [--json] (--plan-latest-choir-receipt | --render-latest-choir-receipt-script | --run-latest-choir-receipt | --verify-latest-choir-receipt)";
const CHOIR_RECEIPT_LATEST_SESSION_VERSION: &str = "1";
const CHOIR_RECEIPT_LATEST_STATUS_VERSION: &str = "1";
const CHOIR_RECEIPT_LATEST_BIN: &str = "copybot_tiny_live_activation_package_choir_receipt_latest";
const TRANSEPT_CERTIFICATE_LATEST_BIN: &str =
    "copybot_tiny_live_activation_package_transept_certificate_latest";
const CHOIR_RECEIPT_BIN: &str = "copybot_tiny_live_activation_package_choir_receipt";
const TRANSEPT_CERTIFICATE_LATEST_PLAN_READY: &str =
    "tiny_live_package_transept_certificate_latest_plan_ready";
const TRANSEPT_CERTIFICATE_LATEST_VERIFY_OK: &str =
    "tiny_live_package_transept_certificate_latest_verify_ok";
const CHOIR_RECEIPT_VERIFY_OK: &str = "tiny_live_package_choir_receipt_verify_ok";
const TOP_ACCEPTED_PACKAGE_STEP: &str = "clerestory_certificate";
const PLANNING_SAFE_STATEMENT: &str =
    "this latest-choir-receipt handoff binds the latest persisted immutable package chain to the accepted choir-receipt contract, but it never overrides Stage 3 or pre-activation refusal truth, it stays archival and read-only, it never runs the frozen live cutover controller by itself, and activation_authorized remains false";

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
    PlanLatestChoirReceipt,
    RenderLatestChoirReceiptScript,
    RunLatestChoirReceipt,
    VerifyLatestChoirReceipt,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageChoirReceiptLatestVerdict {
    TinyLivePackageChoirReceiptLatestPlanReady,
    TinyLivePackageChoirReceiptLatestScriptRendered,
    TinyLivePackageChoirReceiptLatestRefusedByMissingLatestChain,
    TinyLivePackageChoirReceiptLatestRefusedByInvalidLatestChain,
    TinyLivePackageChoirReceiptLatestRefusedByDriftedChoirReceiptContract,
    TinyLivePackageChoirReceiptLatestRefusedNowByStage3,
    TinyLivePackageChoirReceiptLatestRefusedNowByPreActivationGate,
    TinyLivePackageChoirReceiptLatestReadyForManualExecutionWhenGateTurnsGreen,
    TinyLivePackageChoirReceiptLatestVerifyOk,
    TinyLivePackageChoirReceiptLatestVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LivePackageChoirReceiptLatestResult {
    RefusedByMissingLatestChain,
    RefusedByInvalidLatestChain,
    RefusedByDriftedChoirReceiptContract,
    RefusedNowByStage3,
    RefusedNowByPreActivationGate,
    ReadyForManualExecutionWhenGateTurnsGreen,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageChoirReceiptLatestStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageChoirReceiptLatestSession {
    session_version: String,
    planned_at: DateTime<Utc>,
    root: String,
    session_dir: String,
    nested_transept_certificate_latest_session_dir: String,
    nested_choir_receipt_session_dir: String,
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
    resolve_latest_transept_certificate_command_summary: String,
    run_latest_choir_receipt_command_summary: String,
    verify_latest_choir_receipt_command_summary: String,
    downstream_choir_receipt_command_summary: String,
    verify_choir_receipt_command_summary: String,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    clerestory_certificate_summary: Option<String>,
    clerestory_certificate_sha256: Option<String>,
    clerestory_certificate_algorithm: Option<String>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageChoirReceiptLatestStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    root: String,
    session_dir: String,
    latest_top_step_name: Option<String>,
    latest_top_session_dir: Option<String>,
    historical_execute_frozen_session_dir: Option<String>,
    turn_green_session_dir: Option<String>,
    launch_packet_session_dir: Option<String>,
    nested_transept_certificate_latest_session_dir: String,
    nested_choir_receipt_session_dir: String,
    downstream_decision_packet_session_dir: String,
    result: LivePackageChoirReceiptLatestResult,
    reason: String,
    latest_transept_certificate_plan_verdict: Option<String>,
    latest_transept_certificate_verdict: Option<String>,
    choir_receipt_verdict: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    latest_transept_certificate_plan_step: Option<PackageChoirReceiptLatestStepArtifact>,
    latest_transept_certificate_step: Option<PackageChoirReceiptLatestStepArtifact>,
    choir_receipt_step: Option<PackageChoirReceiptLatestStepArtifact>,
    operator_next_action_summary: String,
    activation_authorized: bool,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackageChoirReceiptLatestPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    report_path: PathBuf,
    latest_transept_certificate_plan_report_path: PathBuf,
    latest_transept_certificate_report_path: PathBuf,
    choir_receipt_report_path: PathBuf,
    nested_transept_certificate_latest_session_dir: PathBuf,
    nested_choir_receipt_session_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageChoirReceiptLatestReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageChoirReceiptLatestVerdict,
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
    choir_receipt_latest_session_path: Option<String>,
    choir_receipt_latest_status_path: Option<String>,
    choir_receipt_latest_report_path: Option<String>,
    latest_transept_certificate_plan_report_path: Option<String>,
    latest_transept_certificate_report_path: Option<String>,
    choir_receipt_report_path: Option<String>,
    nested_transept_certificate_latest_session_dir: Option<String>,
    nested_choir_receipt_session_dir: Option<String>,
    downstream_decision_packet_session_dir: Option<String>,
    latest_transept_certificate_plan_verdict: Option<String>,
    latest_transept_certificate_verdict: Option<String>,
    choir_receipt_verdict: Option<String>,
    result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    resolve_latest_transept_certificate_command_summary: Option<String>,
    run_latest_choir_receipt_command_summary: Option<String>,
    verify_latest_choir_receipt_command_summary: Option<String>,
    downstream_choir_receipt_command_summary: Option<String>,
    verify_choir_receipt_command_summary: Option<String>,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    clerestory_certificate_summary: Option<String>,
    clerestory_certificate_sha256: Option<String>,
    clerestory_certificate_algorithm: Option<String>,
    verification_mismatches: Vec<String>,
    activation_authorized: bool,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct LatestTranseptCertificateSnapshot {
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
struct ResolvedLatestTranseptCertificate {
    snapshot: LatestTranseptCertificateSnapshot,
    latest_transept_certificate_plan_raw: Value,
    latest_transept_certificate_plan: TranseptCertificateLatestReportView,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResolutionFailureKind {
    MissingLatestChain,
    InvalidLatestChain,
    DriftedTranseptCertificateContract,
}

#[derive(Debug, Clone)]
struct LatestTranseptCertificateResolutionFailure {
    kind: ResolutionFailureKind,
    reason: String,
    snapshot: Option<LatestTranseptCertificateSnapshot>,
    latest_transept_certificate_plan_raw: Option<Value>,
    latest_transept_certificate_plan_verdict: Option<String>,
    latest_transept_certificate_plan_reason: Option<String>,
    latest_transept_certificate_plan_generated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Deserialize)]
struct TranseptCertificateLatestReportView {
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
struct ChoirReceiptReportView {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: String,
    reason: String,
    transept_certificate_session_dir: String,
    pedestal_certificate_session_dir: Option<String>,
    plinth_receipt_session_dir: Option<String>,
    substructure_certificate_session_dir: Option<String>,
    basal_receipt_session_dir: Option<String>,
    bedrock_certificate_session_dir: Option<String>,
    foundation_receipt_session_dir: Option<String>,
    registry_entry_session_dir: Option<String>,
    notarization_receipt_session_dir: Option<String>,
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
    choir_receipt_session_path: Option<String>,
    choir_receipt_status_path: Option<String>,
    archived_transept_certificate_report_path: Option<String>,
    result: Option<String>,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    transept_certificate_step: Option<PackageChoirReceiptLatestStepArtifact>,
    verify_transept_certificate_command_summary: Option<String>,
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
    filing_certificate_summary: Option<String>,
    filing_certificate_sha256: Option<String>,
    filing_certificate_algorithm: Option<String>,
    archive_receipt_summary: Option<String>,
    archive_receipt_sha256: Option<String>,
    archive_receipt_algorithm: Option<String>,
    closure_certificate_summary: Option<String>,
    closure_certificate_sha256: Option<String>,
    closure_certificate_algorithm: Option<String>,
    finality_receipt_summary: Option<String>,
    finality_receipt_sha256: Option<String>,
    finality_receipt_algorithm: Option<String>,
    consummation_record_summary: Option<String>,
    consummation_record_sha256: Option<String>,
    consummation_record_algorithm: Option<String>,
    completion_certificate_summary: Option<String>,
    completion_certificate_sha256: Option<String>,
    completion_certificate_algorithm: Option<String>,
    culmination_receipt_summary: Option<String>,
    culmination_receipt_sha256: Option<String>,
    culmination_receipt_algorithm: Option<String>,
    summit_certificate_summary: Option<String>,
    summit_certificate_sha256: Option<String>,
    summit_certificate_algorithm: Option<String>,
    pinnacle_receipt_summary: Option<String>,
    pinnacle_receipt_sha256: Option<String>,
    pinnacle_receipt_algorithm: Option<String>,
    capstone_certificate_summary: Option<String>,
    capstone_certificate_sha256: Option<String>,
    capstone_certificate_algorithm: Option<String>,
    keystone_receipt_summary: Option<String>,
    keystone_receipt_sha256: Option<String>,
    keystone_receipt_algorithm: Option<String>,
    cornerstone_certificate_summary: Option<String>,
    cornerstone_certificate_sha256: Option<String>,
    cornerstone_certificate_algorithm: Option<String>,
    foundation_receipt_summary: Option<String>,
    foundation_receipt_sha256: Option<String>,
    foundation_receipt_algorithm: Option<String>,
    bedrock_certificate_summary: Option<String>,
    bedrock_certificate_sha256: Option<String>,
    bedrock_certificate_algorithm: Option<String>,
    basal_receipt_summary: Option<String>,
    basal_receipt_sha256: Option<String>,
    basal_receipt_algorithm: Option<String>,
    substructure_certificate_summary: Option<String>,
    substructure_certificate_sha256: Option<String>,
    substructure_certificate_algorithm: Option<String>,
    plinth_receipt_summary: Option<String>,
    plinth_receipt_sha256: Option<String>,
    plinth_receipt_algorithm: Option<String>,
    pedestal_certificate_summary: Option<String>,
    pedestal_certificate_sha256: Option<String>,
    pedestal_certificate_algorithm: Option<String>,
    transept_certificate_summary: Option<String>,
    transept_certificate_sha256: Option<String>,
    transept_certificate_algorithm: Option<String>,
    choir_receipt_summary: Option<String>,
    choir_receipt_sha256: Option<String>,
    choir_receipt_algorithm: Option<String>,
    verification_mismatches: Option<Vec<String>>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ChoirReceiptSessionView {
    transept_certificate_session_dir: String,
    pedestal_certificate_session_dir: String,
    plinth_receipt_session_dir: String,
    substructure_certificate_session_dir: String,
    basal_receipt_session_dir: String,
    bedrock_certificate_session_dir: String,
    foundation_receipt_session_dir: String,
    registry_entry_session_dir: String,
    notarization_receipt_session_dir: String,
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
    verify_transept_certificate_command_summary: String,
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
    filing_certificate_summary: String,
    filing_certificate_sha256: String,
    filing_certificate_algorithm: String,
    archive_receipt_summary: String,
    archive_receipt_sha256: String,
    archive_receipt_algorithm: String,
    closure_certificate_summary: String,
    closure_certificate_sha256: String,
    closure_certificate_algorithm: String,
    finality_receipt_summary: String,
    finality_receipt_sha256: String,
    finality_receipt_algorithm: String,
    consummation_record_summary: String,
    consummation_record_sha256: String,
    consummation_record_algorithm: String,
    completion_certificate_summary: String,
    completion_certificate_sha256: String,
    completion_certificate_algorithm: String,
    culmination_receipt_summary: String,
    culmination_receipt_sha256: String,
    culmination_receipt_algorithm: String,
    summit_certificate_summary: String,
    summit_certificate_sha256: String,
    summit_certificate_algorithm: String,
    pinnacle_receipt_summary: String,
    pinnacle_receipt_sha256: String,
    pinnacle_receipt_algorithm: String,
    capstone_certificate_summary: String,
    capstone_certificate_sha256: String,
    capstone_certificate_algorithm: String,
    keystone_receipt_summary: String,
    keystone_receipt_sha256: String,
    keystone_receipt_algorithm: String,
    cornerstone_certificate_summary: String,
    cornerstone_certificate_sha256: String,
    cornerstone_certificate_algorithm: String,
    foundation_receipt_summary: String,
    foundation_receipt_sha256: String,
    foundation_receipt_algorithm: String,
    bedrock_certificate_summary: String,
    bedrock_certificate_sha256: String,
    bedrock_certificate_algorithm: String,
    basal_receipt_summary: String,
    basal_receipt_sha256: String,
    basal_receipt_algorithm: String,
    substructure_certificate_summary: String,
    substructure_certificate_sha256: String,
    substructure_certificate_algorithm: String,
    plinth_receipt_summary: String,
    plinth_receipt_sha256: String,
    plinth_receipt_algorithm: String,
    pedestal_certificate_summary: String,
    pedestal_certificate_sha256: String,
    pedestal_certificate_algorithm: String,
    transept_certificate_summary: String,
    transept_certificate_sha256: String,
    transept_certificate_algorithm: String,
    choir_receipt_summary: String,
    choir_receipt_sha256: String,
    choir_receipt_algorithm: String,
    explicit_statement: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ChoirReceiptStatusView {
    session_dir: String,
    transept_certificate_session_dir: String,
    pedestal_certificate_session_dir: String,
    plinth_receipt_session_dir: String,
    substructure_certificate_session_dir: String,
    basal_receipt_session_dir: String,
    bedrock_certificate_session_dir: String,
    foundation_receipt_session_dir: String,
    result: String,
    reason: String,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    transept_certificate_step: Option<PackageChoirReceiptLatestStepArtifact>,
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
    filing_certificate_summary: String,
    filing_certificate_sha256: String,
    filing_certificate_algorithm: String,
    archive_receipt_summary: String,
    archive_receipt_sha256: String,
    archive_receipt_algorithm: String,
    closure_certificate_summary: String,
    closure_certificate_sha256: String,
    closure_certificate_algorithm: String,
    finality_receipt_summary: String,
    finality_receipt_sha256: String,
    finality_receipt_algorithm: String,
    consummation_record_summary: String,
    consummation_record_sha256: String,
    consummation_record_algorithm: String,
    completion_certificate_summary: String,
    completion_certificate_sha256: String,
    completion_certificate_algorithm: String,
    culmination_receipt_summary: String,
    culmination_receipt_sha256: String,
    culmination_receipt_algorithm: String,
    summit_certificate_summary: String,
    summit_certificate_sha256: String,
    summit_certificate_algorithm: String,
    pinnacle_receipt_summary: String,
    pinnacle_receipt_sha256: String,
    pinnacle_receipt_algorithm: String,
    capstone_certificate_summary: String,
    capstone_certificate_sha256: String,
    capstone_certificate_algorithm: String,
    keystone_receipt_summary: String,
    keystone_receipt_sha256: String,
    keystone_receipt_algorithm: String,
    cornerstone_certificate_summary: String,
    cornerstone_certificate_sha256: String,
    cornerstone_certificate_algorithm: String,
    foundation_receipt_summary: String,
    foundation_receipt_sha256: String,
    foundation_receipt_algorithm: String,
    bedrock_certificate_summary: String,
    bedrock_certificate_sha256: String,
    bedrock_certificate_algorithm: String,
    basal_receipt_summary: String,
    basal_receipt_sha256: String,
    basal_receipt_algorithm: String,
    substructure_certificate_summary: String,
    substructure_certificate_sha256: String,
    substructure_certificate_algorithm: String,
    plinth_receipt_summary: String,
    plinth_receipt_sha256: String,
    plinth_receipt_algorithm: String,
    pedestal_certificate_summary: String,
    pedestal_certificate_sha256: String,
    pedestal_certificate_algorithm: String,
    transept_certificate_summary: String,
    transept_certificate_sha256: String,
    transept_certificate_algorithm: String,
    choir_receipt_summary: String,
    choir_receipt_sha256: String,
    choir_receipt_algorithm: String,
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
            "--plan-latest-choir-receipt" => {
                set_mode(&mut mode, Mode::PlanLatestChoirReceipt, arg.as_str())?
            }
            "--render-latest-choir-receipt-script" => set_mode(
                &mut mode,
                Mode::RenderLatestChoirReceiptScript,
                arg.as_str(),
            )?,
            "--run-latest-choir-receipt" => {
                set_mode(&mut mode, Mode::RunLatestChoirReceipt, arg.as_str())?
            }
            "--verify-latest-choir-receipt" => {
                set_mode(&mut mode, Mode::VerifyLatestChoirReceipt, arg.as_str())?
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
        Mode::PlanLatestChoirReceipt
            | Mode::RenderLatestChoirReceiptScript
            | Mode::RunLatestChoirReceipt
    ) && root.is_none()
    {
        bail!("missing --root");
    }
    if matches!(mode, Mode::RenderLatestChoirReceiptScript) && output_path.is_none() {
        bail!("missing --output for render mode");
    }
    if matches!(
        mode,
        Mode::RunLatestChoirReceipt | Mode::VerifyLatestChoirReceipt
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
        Mode::PlanLatestChoirReceipt => plan_latest_choir_receipt_report(&config)?,
        Mode::RenderLatestChoirReceiptScript => render_latest_choir_receipt_script_report(&config)?,
        Mode::RunLatestChoirReceipt => run_latest_choir_receipt_report(&config)?,
        Mode::VerifyLatestChoirReceipt => verify_latest_choir_receipt_report(&config)?,
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_report_lines(&report))
    }
}

fn plan_latest_choir_receipt_report(config: &Config) -> Result<PackageChoirReceiptLatestReport> {
    let root = config
        .root
        .as_ref()
        .ok_or_else(|| anyhow!("missing --root"))?;
    let suggested_session_dir = config
        .session_dir
        .clone()
        .unwrap_or_else(|| root.join("tiny_live_activation_package_choir_receipt_latest.session"));
    let paths = choir_receipt_latest_paths(&suggested_session_dir);

    match resolve_latest_transept_certificate(root, &paths) {
        Ok(resolved) => Ok(plan_report_from_resolution(
            "plan_latest_choir_receipt",
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestPlanReady,
            format!(
                "latest immutable {} chain under {} resolves to exact latest-transept-certificate lineage via accepted latest-transept-certificate session {}, exact decision-packet confirmation anchor {}, and the accepted choir-receipt contract remains current",
                resolved.snapshot.latest_top_step_name,
                root.display(),
                paths.nested_transept_certificate_latest_session_dir.display(),
                resolved.snapshot.downstream_decision_packet_session_dir.display(),
            ),
            root,
            &suggested_session_dir,
            &paths,
            &resolved,
        )),
        Err(failure) => Ok(report_from_resolution_failure(
            "plan_latest_choir_receipt",
            root,
            &suggested_session_dir,
            &paths,
            &failure,
        )),
    }
}

fn render_latest_choir_receipt_script_report(
    config: &Config,
) -> Result<PackageChoirReceiptLatestReport> {
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
    let paths = choir_receipt_latest_paths(&suggested_session_dir);

    match resolve_latest_transept_certificate(root, &paths) {
        Ok(resolved) => {
            write_script(
                output_path,
                &format!(
                    "#!/usr/bin/env bash\nset -euo pipefail\n\n{run_latest}\n{verify_latest}\n{run_choir_receipt}\n{verify_choir_receipt}\n",
                    run_latest = run_latest_choir_receipt_command_summary(
                        root,
                        &suggested_session_dir,
                    ),
                    verify_latest =
                        verify_latest_choir_receipt_command_summary(&suggested_session_dir),
                    run_choir_receipt = downstream_choir_receipt_command_summary(
                        &resolved.snapshot,
                        &expected_downstream_transept_certificate_session_dir(&paths),
                        &paths.nested_choir_receipt_session_dir,
                    ),
                    verify_choir_receipt = verify_choir_receipt_command_summary(
                        &resolved.snapshot,
                        &expected_downstream_transept_certificate_session_dir(&paths),
                        &paths.nested_choir_receipt_session_dir,
                    ),
                ),
            )?;
            Ok(plan_report_from_resolution(
                "render_latest_choir_receipt_script",
                TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestScriptRendered,
                format!(
                    "rendered latest-choir-receipt handoff script to {} using the accepted latest-transept-certificate lineage and the accepted choir-receipt contract",
                    output_path.display()
                ),
                root,
                &suggested_session_dir,
                &paths,
                &resolved,
            ))
        }
        Err(failure) => Ok(report_from_resolution_failure(
            "render_latest_choir_receipt_script",
            root,
            &suggested_session_dir,
            &paths,
            &failure,
        )),
    }
}

fn run_latest_choir_receipt_report(config: &Config) -> Result<PackageChoirReceiptLatestReport> {
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
            "refusing to overwrite existing latest-choir-receipt session dir {}",
            session_dir.display()
        );
    }
    let paths = choir_receipt_latest_paths(session_dir);
    create_clean_session_dir(session_dir, "latest-choir-receipt")?;

    let resolution = resolve_latest_transept_certificate(root, &paths);
    match resolution {
        Ok(resolved) => {
            run_latest_choir_receipt_with_resolution(root, session_dir, &paths, &resolved)
        }
        Err(failure) => run_latest_choir_receipt_failure(root, session_dir, &paths, &failure),
    }
}

fn verify_latest_choir_receipt_report(config: &Config) -> Result<PackageChoirReceiptLatestReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for verify mode"))?;
    let paths = choir_receipt_latest_paths(session_dir);

    let session: PackageChoirReceiptLatestSession = match load_json(&paths.session_path) {
        Ok(value) => value,
        Err(error) => {
            return Ok(verify_invalid_choir_receipt_latest_report_without_session(
                session_dir,
                vec![format!(
                    "failed reading latest-choir-receipt session artifact {}: {error}",
                    paths.session_path.display()
                )],
            ))
        }
    };
    let status: PackageChoirReceiptLatestStatus = match load_json(&paths.status_path) {
        Ok(value) => value,
        Err(error) => {
            return Ok(verify_invalid_choir_receipt_latest_report_without_session(
                session_dir,
                vec![format!(
                    "failed reading latest-choir-receipt status artifact {}: {error}",
                    paths.status_path.display()
                )],
            ))
        }
    };
    let stored_report: PackageChoirReceiptLatestReport = match load_json(&paths.report_path) {
        Ok(value) => value,
        Err(error) => {
            return Ok(verify_invalid_choir_receipt_latest_report_without_session(
                session_dir,
                vec![format!(
                    "failed reading latest-choir-receipt report artifact {}: {error}",
                    paths.report_path.display()
                )],
            ))
        }
    };

    let mut mismatches = Vec::new();
    let root = PathBuf::from(&session.root);

    compare_string(
        &session.root,
        &status.root,
        "latest-choir-receipt root consistency",
        &mut mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "latest-choir-receipt session session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.session_dir,
        &session_dir.display().to_string(),
        "latest-choir-receipt status session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "latest-choir-receipt report session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.nested_transept_certificate_latest_session_dir,
        &paths
            .nested_transept_certificate_latest_session_dir
            .display()
            .to_string(),
        "latest-choir-receipt session nested_transept_certificate_latest_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.nested_transept_certificate_latest_session_dir,
        &paths
            .nested_transept_certificate_latest_session_dir
            .display()
            .to_string(),
        "latest-choir-receipt status nested_transept_certificate_latest_session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report
            .nested_transept_certificate_latest_session_dir
            .as_deref(),
        Some(
            paths
                .nested_transept_certificate_latest_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "latest-choir-receipt report nested_transept_certificate_latest_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.nested_choir_receipt_session_dir,
        &paths.nested_choir_receipt_session_dir.display().to_string(),
        "latest-choir-receipt session nested_choir_receipt_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.nested_choir_receipt_session_dir,
        &paths.nested_choir_receipt_session_dir.display().to_string(),
        "latest-choir-receipt status nested_choir_receipt_session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.nested_choir_receipt_session_dir.as_deref(),
        Some(
            paths
                .nested_choir_receipt_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "latest-choir-receipt report nested_choir_receipt_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.downstream_decision_packet_session_dir,
        &expected_downstream_decision_packet_session_dir(&paths)
            .display()
            .to_string(),
        "latest-choir-receipt session downstream_decision_packet_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.downstream_decision_packet_session_dir,
        &expected_downstream_decision_packet_session_dir(&paths)
            .display()
            .to_string(),
        "latest-choir-receipt status downstream_decision_packet_session_dir",
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
        "latest-choir-receipt report downstream_decision_packet_session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.choir_receipt_latest_session_path.as_deref(),
        Some(paths.session_path.display().to_string().as_str()),
        "latest-choir-receipt report session path",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.choir_receipt_latest_status_path.as_deref(),
        Some(paths.status_path.display().to_string().as_str()),
        "latest-choir-receipt report status path",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.choir_receipt_latest_report_path.as_deref(),
        Some(paths.report_path.display().to_string().as_str()),
        "latest-choir-receipt report report path",
        &mut mismatches,
    );
    compare_string(
        &session.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "latest-choir-receipt session explicit_statement",
        &mut mismatches,
    );
    compare_string(
        &status.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "latest-choir-receipt status explicit_statement",
        &mut mismatches,
    );
    compare_string(
        &stored_report.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "latest-choir-receipt report explicit_statement",
        &mut mismatches,
    );
    compare_string(
        &stored_report.mode,
        "run_latest_choir_receipt",
        "latest-choir-receipt stored report mode",
        &mut mismatches,
    );
    compare_string(
        &serialize_enum(&stored_report.verdict),
        &serialize_enum(&verdict_for_result(status.result)),
        "latest-choir-receipt stored report verdict",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.result.as_deref(),
        Some(serialize_enum(&status.result).as_str()),
        "latest-choir-receipt stored report result consistency",
        &mut mismatches,
    );
    compare_string(
        &stored_report.reason,
        &status.reason,
        "latest-choir-receipt stored report reason consistency",
        &mut mismatches,
    );
    if stored_report.activation_authorized != status.activation_authorized {
        mismatches.push(format!(
            "latest-choir-receipt stored report activation_authorized {} does not match status {}",
            stored_report.activation_authorized, status.activation_authorized
        ));
    }
    compare_option_string(
        stored_report
            .latest_transept_certificate_plan_verdict
            .as_deref(),
        status.latest_transept_certificate_plan_verdict.as_deref(),
        "stored report latest_transept_certificate_plan_verdict",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.latest_transept_certificate_verdict.as_deref(),
        status.latest_transept_certificate_verdict.as_deref(),
        "stored report latest_transept_certificate_verdict",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.choir_receipt_verdict.as_deref(),
        status.choir_receipt_verdict.as_deref(),
        "stored report choir_receipt_verdict",
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

    let resolution = resolve_latest_transept_certificate(&root, &paths);
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

            let stored_latest_plan: TranseptCertificateLatestReportView = load_required_step_json(
                &status.latest_transept_certificate_plan_step,
                &paths.latest_transept_certificate_plan_report_path,
                "latest_transept_certificate_plan_step",
                &mut mismatches,
            )?;
            compare_step_artifact_against_report_metadata(
                status.latest_transept_certificate_plan_step.as_ref(),
                &paths.latest_transept_certificate_plan_report_path,
                "plan_latest_transept_certificate",
                &stored_latest_plan.verdict,
                &stored_latest_plan.reason,
                stored_latest_plan.generated_at,
                "stored latest_transept_certificate_plan_step",
                &mut mismatches,
            );
            compare_transept_certificate_latest_plan_against_snapshot(
                &stored_latest_plan,
                &resolved.snapshot,
                &paths.nested_transept_certificate_latest_session_dir,
                &mut mismatches,
            );
            compare_option_string(
                status.latest_transept_certificate_plan_verdict.as_deref(),
                Some(stored_latest_plan.verdict.as_str()),
                "stored status latest_transept_certificate_plan_verdict",
                &mut mismatches,
            );
            compare_option_string(
                stored_report
                    .latest_transept_certificate_plan_verdict
                    .as_deref(),
                Some(stored_latest_plan.verdict.as_str()),
                "stored report latest_transept_certificate_plan_verdict",
                &mut mismatches,
            );

            let stored_latest_run: TranseptCertificateLatestReportView = load_required_step_json(
                &status.latest_transept_certificate_step,
                &paths.latest_transept_certificate_report_path,
                "latest_transept_certificate_step",
                &mut mismatches,
            )?;
            compare_step_artifact_against_report_metadata(
                status.latest_transept_certificate_step.as_ref(),
                &paths.latest_transept_certificate_report_path,
                "run_latest_transept_certificate",
                &stored_latest_run.verdict,
                &stored_latest_run.reason,
                stored_latest_run.generated_at,
                "stored latest_transept_certificate_step",
                &mut mismatches,
            );
            compare_transept_certificate_latest_run_against_snapshot(
                &stored_latest_run,
                &resolved.snapshot,
                &paths.nested_transept_certificate_latest_session_dir,
                &mut mismatches,
            );
            compare_option_string(
                status.latest_transept_certificate_verdict.as_deref(),
                Some(stored_latest_run.verdict.as_str()),
                "stored status latest_transept_certificate_verdict",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.latest_transept_certificate_verdict.as_deref(),
                Some(stored_latest_run.verdict.as_str()),
                "stored report latest_transept_certificate_verdict",
                &mut mismatches,
            );

            let actual_latest_run: TranseptCertificateLatestReportView = match load_json(
                &nested_transept_certificate_latest_report_path(
                    &paths.nested_transept_certificate_latest_session_dir,
                ),
            ) {
                Ok(value) => value,
                Err(error) => {
                    mismatches.push(format!(
                        "failed loading actual nested latest-transept-certificate report under {}: {error}",
                        paths.nested_transept_certificate_latest_session_dir.display()
                    ));
                    fallback_transept_certificate_latest_copy()
                }
            };
            compare_transept_certificate_latest_copy_matches_actual(
                &stored_latest_run,
                &actual_latest_run,
                &mut mismatches,
            );

            let latest_verify_raw = match run_json_command(
                TRANSEPT_CERTIFICATE_LATEST_BIN,
                &latest_transept_certificate_verify_args(
                    &paths.nested_transept_certificate_latest_session_dir,
                ),
            ) {
                Ok(value) => value,
                Err(error) => {
                    mismatches.push(format!(
                        "failed verifying nested latest-transept-certificate session under {}: {error}",
                        paths.nested_transept_certificate_latest_session_dir.display()
                    ));
                    Value::Null
                }
            };
            if !latest_verify_raw.is_null() {
                let latest_verify: TranseptCertificateLatestReportView =
                    serde_json::from_value(latest_verify_raw)
                        .context("failed parsing nested latest-transept-certificate verify json")?;
                if latest_verify.verdict != TRANSEPT_CERTIFICATE_LATEST_VERIFY_OK {
                    mismatches.push(format!(
                        "nested latest-transept-certificate verification is non-green: {}",
                        latest_verify.reason
                    ));
                }
            }

            let stored_handoff_bundle: ChoirReceiptReportView = load_required_step_json(
                &status.choir_receipt_step,
                &paths.choir_receipt_report_path,
                "choir_receipt_step",
                &mut mismatches,
            )?;
            compare_step_artifact_against_report_metadata(
                status.choir_receipt_step.as_ref(),
                &paths.choir_receipt_report_path,
                "run_live_package_choir_receipt",
                &stored_handoff_bundle.verdict,
                &stored_handoff_bundle.reason,
                stored_handoff_bundle.generated_at,
                "stored choir_receipt_step",
                &mut mismatches,
            );
            compare_choir_receipt_run_against_snapshot(
                &stored_handoff_bundle,
                &resolved.snapshot,
                &expected_downstream_transept_certificate_session_dir(&paths),
                &paths.nested_choir_receipt_session_dir,
                &mut mismatches,
            );
            compare_option_string(
                status.choir_receipt_verdict.as_deref(),
                Some(stored_handoff_bundle.verdict.as_str()),
                "stored status choir_receipt_verdict",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.choir_receipt_verdict.as_deref(),
                Some(stored_handoff_bundle.verdict.as_str()),
                "stored report choir_receipt_verdict",
                &mut mismatches,
            );

            let nested_handoff_bundle_session: ChoirReceiptSessionView = match load_json(
                &nested_choir_receipt_session_path(&paths.nested_choir_receipt_session_dir),
            ) {
                Ok(value) => value,
                Err(error) => {
                    mismatches.push(format!(
                        "failed reading nested choir-receipt session under {}: {error}",
                        paths.nested_choir_receipt_session_dir.display()
                    ));
                    fallback_choir_receipt_session(
                        &resolved.snapshot,
                        &paths.nested_choir_receipt_session_dir,
                    )
                }
            };
            let nested_handoff_bundle_status: ChoirReceiptStatusView = match load_json(
                &nested_choir_receipt_status_path(&paths.nested_choir_receipt_session_dir),
            ) {
                Ok(value) => value,
                Err(error) => {
                    mismatches.push(format!(
                        "failed reading nested choir-receipt status under {}: {error}",
                        paths.nested_choir_receipt_session_dir.display()
                    ));
                    fallback_choir_receipt_status(
                        &resolved.snapshot,
                        &paths.nested_choir_receipt_session_dir,
                    )
                }
            };
            compare_choir_receipt_copy_against_nested_truth(
                &stored_handoff_bundle,
                &nested_handoff_bundle_session,
                &nested_handoff_bundle_status,
                &mut mismatches,
            );

            let handoff_bundle_verify_raw = match run_json_command(
                CHOIR_RECEIPT_BIN,
                &choir_receipt_verify_args(
                    &resolved.snapshot,
                    &expected_downstream_transept_certificate_session_dir(&paths),
                    &paths.nested_choir_receipt_session_dir,
                ),
            ) {
                Ok(value) => value,
                Err(error) => {
                    mismatches.push(format!(
                        "failed verifying nested choir-receipt session under {}: {error}",
                        paths.nested_choir_receipt_session_dir.display()
                    ));
                    Value::Null
                }
            };
            if !handoff_bundle_verify_raw.is_null() {
                let handoff_bundle_verify: ChoirReceiptReportView =
                    serde_json::from_value(handoff_bundle_verify_raw)
                        .context("failed parsing nested choir-receipt verify json")?;
                compare_option_string(
                    stored_handoff_bundle.registry_entry_session_dir.as_deref(),
                    handoff_bundle_verify.registry_entry_session_dir.as_deref(),
                    "stored choir-receipt copy registry_entry_session_dir against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.notarization_receipt_session_dir.as_deref(),
                    handoff_bundle_verify.notarization_receipt_session_dir.as_deref(),
                    "stored choir-receipt copy notarization_receipt_session_dir against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .attestation_seal_session_dir
                        .as_deref(),
                    handoff_bundle_verify
                        .attestation_seal_session_dir
                        .as_deref(),
                    "stored choir-receipt copy attestation_seal_session_dir against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.chain_fingerprint_summary.as_deref(),
                    handoff_bundle_verify.chain_fingerprint_summary.as_deref(),
                    "stored choir-receipt copy chain_fingerprint_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.chain_fingerprint_sha256.as_deref(),
                    handoff_bundle_verify.chain_fingerprint_sha256.as_deref(),
                    "stored choir-receipt copy chain_fingerprint_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.chain_fingerprint_algorithm.as_deref(),
                    handoff_bundle_verify.chain_fingerprint_algorithm.as_deref(),
                    "stored choir-receipt copy chain_fingerprint_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.filing_certificate_summary.as_deref(),
                    handoff_bundle_verify.filing_certificate_summary.as_deref(),
                    "stored choir-receipt copy filing_certificate_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.filing_certificate_sha256.as_deref(),
                    handoff_bundle_verify.filing_certificate_sha256.as_deref(),
                    "stored choir-receipt copy filing_certificate_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .filing_certificate_algorithm
                        .as_deref(),
                    handoff_bundle_verify
                        .filing_certificate_algorithm
                        .as_deref(),
                    "stored choir-receipt copy filing_certificate_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.archive_receipt_summary.as_deref(),
                    handoff_bundle_verify.archive_receipt_summary.as_deref(),
                    "stored choir-receipt copy archive_receipt_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.archive_receipt_sha256.as_deref(),
                    handoff_bundle_verify.archive_receipt_sha256.as_deref(),
                    "stored choir-receipt copy archive_receipt_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.archive_receipt_algorithm.as_deref(),
                    handoff_bundle_verify.archive_receipt_algorithm.as_deref(),
                    "stored choir-receipt copy archive_receipt_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.closure_certificate_summary.as_deref(),
                    handoff_bundle_verify.closure_certificate_summary.as_deref(),
                    "stored choir-receipt copy closure_certificate_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.closure_certificate_sha256.as_deref(),
                    handoff_bundle_verify.closure_certificate_sha256.as_deref(),
                    "stored choir-receipt copy closure_certificate_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .closure_certificate_algorithm
                        .as_deref(),
                    handoff_bundle_verify
                        .closure_certificate_algorithm
                        .as_deref(),
                    "stored choir-receipt copy closure_certificate_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.finality_receipt_summary.as_deref(),
                    handoff_bundle_verify.finality_receipt_summary.as_deref(),
                    "stored choir-receipt copy finality_receipt_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.finality_receipt_sha256.as_deref(),
                    handoff_bundle_verify.finality_receipt_sha256.as_deref(),
                    "stored choir-receipt copy finality_receipt_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.finality_receipt_algorithm.as_deref(),
                    handoff_bundle_verify.finality_receipt_algorithm.as_deref(),
                    "stored choir-receipt copy finality_receipt_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.consummation_record_summary.as_deref(),
                    handoff_bundle_verify.consummation_record_summary.as_deref(),
                    "stored choir-receipt copy consummation_record_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.consummation_record_sha256.as_deref(),
                    handoff_bundle_verify.consummation_record_sha256.as_deref(),
                    "stored choir-receipt copy consummation_record_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .consummation_record_algorithm
                        .as_deref(),
                    handoff_bundle_verify
                        .consummation_record_algorithm
                        .as_deref(),
                    "stored choir-receipt copy consummation_record_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .completion_certificate_summary
                        .as_deref(),
                    handoff_bundle_verify
                        .completion_certificate_summary
                        .as_deref(),
                    "stored choir-receipt copy completion_certificate_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .completion_certificate_sha256
                        .as_deref(),
                    handoff_bundle_verify
                        .completion_certificate_sha256
                        .as_deref(),
                    "stored choir-receipt copy completion_certificate_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.completion_certificate_algorithm.as_deref(),
                    handoff_bundle_verify.completion_certificate_algorithm.as_deref(),
                    "stored choir-receipt copy completion_certificate_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.culmination_receipt_summary.as_deref(),
                    handoff_bundle_verify.culmination_receipt_summary.as_deref(),
                    "stored choir-receipt copy culmination_receipt_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.culmination_receipt_sha256.as_deref(),
                    handoff_bundle_verify.culmination_receipt_sha256.as_deref(),
                    "stored choir-receipt copy culmination_receipt_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .culmination_receipt_algorithm
                        .as_deref(),
                    handoff_bundle_verify
                        .culmination_receipt_algorithm
                        .as_deref(),
                    "stored choir-receipt copy culmination_receipt_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.summit_certificate_summary.as_deref(),
                    handoff_bundle_verify.summit_certificate_summary.as_deref(),
                    "stored choir-receipt copy summit_certificate_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.summit_certificate_sha256.as_deref(),
                    handoff_bundle_verify.summit_certificate_sha256.as_deref(),
                    "stored choir-receipt copy summit_certificate_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .summit_certificate_algorithm
                        .as_deref(),
                    handoff_bundle_verify
                        .summit_certificate_algorithm
                        .as_deref(),
                    "stored choir-receipt copy summit_certificate_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.pinnacle_receipt_summary.as_deref(),
                    handoff_bundle_verify.pinnacle_receipt_summary.as_deref(),
                    "stored choir-receipt copy pinnacle_receipt_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.pinnacle_receipt_sha256.as_deref(),
                    handoff_bundle_verify.pinnacle_receipt_sha256.as_deref(),
                    "stored choir-receipt copy pinnacle_receipt_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.pinnacle_receipt_algorithm.as_deref(),
                    handoff_bundle_verify.pinnacle_receipt_algorithm.as_deref(),
                    "stored choir-receipt copy pinnacle_receipt_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.keystone_receipt_summary.as_deref(),
                    handoff_bundle_verify.keystone_receipt_summary.as_deref(),
                    "stored choir-receipt copy keystone_receipt_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.keystone_receipt_sha256.as_deref(),
                    handoff_bundle_verify.keystone_receipt_sha256.as_deref(),
                    "stored choir-receipt copy keystone_receipt_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.keystone_receipt_algorithm.as_deref(),
                    handoff_bundle_verify.keystone_receipt_algorithm.as_deref(),
                    "stored choir-receipt copy keystone_receipt_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .cornerstone_certificate_summary
                        .as_deref(),
                    handoff_bundle_verify
                        .cornerstone_certificate_summary
                        .as_deref(),
                    "stored choir-receipt copy cornerstone_certificate_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .cornerstone_certificate_sha256
                        .as_deref(),
                    handoff_bundle_verify
                        .cornerstone_certificate_sha256
                        .as_deref(),
                    "stored choir-receipt copy cornerstone_certificate_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .cornerstone_certificate_algorithm
                        .as_deref(),
                    handoff_bundle_verify
                        .cornerstone_certificate_algorithm
                        .as_deref(),
                    "stored choir-receipt copy cornerstone_certificate_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .capstone_certificate_summary
                        .as_deref(),
                    handoff_bundle_verify
                        .capstone_certificate_summary
                        .as_deref(),
                    "stored choir-receipt copy capstone_certificate_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.capstone_certificate_sha256.as_deref(),
                    handoff_bundle_verify.capstone_certificate_sha256.as_deref(),
                    "stored choir-receipt copy capstone_certificate_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .capstone_certificate_algorithm
                        .as_deref(),
                    handoff_bundle_verify
                        .capstone_certificate_algorithm
                        .as_deref(),
                    "stored choir-receipt copy capstone_certificate_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.foundation_receipt_summary.as_deref(),
                    handoff_bundle_verify.foundation_receipt_summary.as_deref(),
                    "stored choir-receipt copy foundation_receipt_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.foundation_receipt_sha256.as_deref(),
                    handoff_bundle_verify.foundation_receipt_sha256.as_deref(),
                    "stored choir-receipt copy foundation_receipt_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .foundation_receipt_algorithm
                        .as_deref(),
                    handoff_bundle_verify
                        .foundation_receipt_algorithm
                        .as_deref(),
                    "stored choir-receipt copy foundation_receipt_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.bedrock_certificate_summary.as_deref(),
                    handoff_bundle_verify.bedrock_certificate_summary.as_deref(),
                    "stored choir-receipt copy bedrock_certificate_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.bedrock_certificate_sha256.as_deref(),
                    handoff_bundle_verify.bedrock_certificate_sha256.as_deref(),
                    "stored choir-receipt copy bedrock_certificate_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .bedrock_certificate_algorithm
                        .as_deref(),
                    handoff_bundle_verify
                        .bedrock_certificate_algorithm
                        .as_deref(),
                    "stored choir-receipt copy bedrock_certificate_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.basal_receipt_summary.as_deref(),
                    handoff_bundle_verify.basal_receipt_summary.as_deref(),
                    "stored choir-receipt copy basal_receipt_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.basal_receipt_sha256.as_deref(),
                    handoff_bundle_verify.basal_receipt_sha256.as_deref(),
                    "stored choir-receipt copy basal_receipt_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.basal_receipt_algorithm.as_deref(),
                    handoff_bundle_verify.basal_receipt_algorithm.as_deref(),
                    "stored choir-receipt copy basal_receipt_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .substructure_certificate_summary
                        .as_deref(),
                    handoff_bundle_verify
                        .substructure_certificate_summary
                        .as_deref(),
                    "stored choir-receipt copy substructure_certificate_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .substructure_certificate_sha256
                        .as_deref(),
                    handoff_bundle_verify
                        .substructure_certificate_sha256
                        .as_deref(),
                    "stored choir-receipt copy substructure_certificate_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .substructure_certificate_algorithm
                        .as_deref(),
                    handoff_bundle_verify
                        .substructure_certificate_algorithm
                        .as_deref(),
                    "stored choir-receipt copy substructure_certificate_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.plinth_receipt_summary.as_deref(),
                    handoff_bundle_verify.plinth_receipt_summary.as_deref(),
                    "stored choir-receipt copy plinth_receipt_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.plinth_receipt_sha256.as_deref(),
                    handoff_bundle_verify.plinth_receipt_sha256.as_deref(),
                    "stored choir-receipt copy plinth_receipt_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.plinth_receipt_algorithm.as_deref(),
                    handoff_bundle_verify.plinth_receipt_algorithm.as_deref(),
                    "stored choir-receipt copy plinth_receipt_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .pedestal_certificate_summary
                        .as_deref(),
                    handoff_bundle_verify
                        .pedestal_certificate_summary
                        .as_deref(),
                    "stored choir-receipt copy pedestal_certificate_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.pedestal_certificate_sha256.as_deref(),
                    handoff_bundle_verify.pedestal_certificate_sha256.as_deref(),
                    "stored choir-receipt copy pedestal_certificate_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .pedestal_certificate_algorithm
                        .as_deref(),
                    handoff_bundle_verify
                        .pedestal_certificate_algorithm
                        .as_deref(),
                    "stored choir-receipt copy pedestal_certificate_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .transept_certificate_summary
                        .as_deref(),
                    handoff_bundle_verify
                        .transept_certificate_summary
                        .as_deref(),
                    "stored choir-receipt copy transept_certificate_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.transept_certificate_sha256.as_deref(),
                    handoff_bundle_verify.transept_certificate_sha256.as_deref(),
                    "stored choir-receipt copy transept_certificate_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .transept_certificate_algorithm
                        .as_deref(),
                    handoff_bundle_verify
                        .transept_certificate_algorithm
                        .as_deref(),
                    "stored choir-receipt copy transept_certificate_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .reviewed_frozen_live_cutover_controller_command_summary
                        .as_deref(),
                    handoff_bundle_verify
                        .reviewed_frozen_live_cutover_controller_command_summary
                        .as_deref(),
                    "stored choir-receipt copy reviewed_frozen_live_cutover_controller_command_summary",
                    &mut mismatches,
                );
                if handoff_bundle_verify.verdict != CHOIR_RECEIPT_VERIFY_OK {
                    mismatches.push(format!(
                        "nested choir-receipt verification is non-green: {}",
                        handoff_bundle_verify.reason
                    ));
                }
            }

            let expected_result = transept_certificate_latest_result_from_nested_status(
                nested_handoff_bundle_status.result.as_str(),
            )
            .unwrap_or(LivePackageChoirReceiptLatestResult::RefusedByDriftedChoirReceiptContract);
            if status.result != expected_result {
                mismatches.push(format!(
                    "stored latest-choir-receipt result {} does not match nested choir-receipt result {}",
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
                    "stored status activation_authorized must remain false for latest-choir-receipt"
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
            if failure.latest_transept_certificate_plan_raw.is_some() {
                let stored_latest_plan: TranseptCertificateLatestReportView =
                    load_required_step_json(
                        &status.latest_transept_certificate_plan_step,
                        &paths.latest_transept_certificate_plan_report_path,
                        "latest_transept_certificate_plan_step",
                        &mut mismatches,
                    )?;
                compare_step_artifact_against_report_metadata(
                    status.latest_transept_certificate_plan_step.as_ref(),
                    &paths.latest_transept_certificate_plan_report_path,
                    "plan_latest_transept_certificate",
                    &stored_latest_plan.verdict,
                    &stored_latest_plan.reason,
                    stored_latest_plan.generated_at,
                    "stored latest_transept_certificate_plan_step",
                    &mut mismatches,
                );
                compare_option_string(
                    status.latest_transept_certificate_plan_verdict.as_deref(),
                    failure.latest_transept_certificate_plan_verdict.as_deref(),
                    "stored status latest_transept_certificate_plan_verdict on failure",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_report
                        .latest_transept_certificate_plan_verdict
                        .as_deref(),
                    failure.latest_transept_certificate_plan_verdict.as_deref(),
                    "stored report latest_transept_certificate_plan_verdict on failure",
                    &mut mismatches,
                );
            }
            compare_option_string(
                status.latest_transept_certificate_verdict.as_deref(),
                None,
                "stored status latest_transept_certificate_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.latest_transept_certificate_verdict.as_deref(),
                None,
                "stored report latest_transept_certificate_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                status.choir_receipt_verdict.as_deref(),
                None,
                "stored status choir_receipt_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.choir_receipt_verdict.as_deref(),
                None,
                "stored report choir_receipt_verdict on failure",
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
        TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyOk
    } else {
        TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
    };
    let reason = if mismatches.is_empty() {
        format!(
            "latest-choir-receipt session under {} is coherent with the current latest immutable chain and the accepted choir-receipt contract",
            session_dir.display()
        )
    } else {
        mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "latest-choir-receipt verification failed".to_string())
    };

    Ok(PackageChoirReceiptLatestReport {
        generated_at: Utc::now(),
        mode: "verify_latest_choir_receipt".to_string(),
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
        choir_receipt_latest_session_path: Some(paths.session_path.display().to_string()),
        choir_receipt_latest_status_path: Some(paths.status_path.display().to_string()),
        choir_receipt_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_transept_certificate_plan_report_path: Some(
            paths
                .latest_transept_certificate_plan_report_path
                .display()
                .to_string(),
        ),
        latest_transept_certificate_report_path: Some(
            paths.latest_transept_certificate_report_path.display().to_string(),
        ),
        choir_receipt_report_path: Some(paths.choir_receipt_report_path.display().to_string()),
        nested_transept_certificate_latest_session_dir: Some(
            paths
                .nested_transept_certificate_latest_session_dir
                .display()
                .to_string(),
        ),
        nested_choir_receipt_session_dir: Some(
            paths.nested_choir_receipt_session_dir.display().to_string(),
        ),
        downstream_decision_packet_session_dir: Some(
            expected_downstream_decision_packet_session_dir(&paths)
                .display()
                .to_string(),
        ),
        latest_transept_certificate_plan_verdict: status.latest_transept_certificate_plan_verdict.clone(),
        latest_transept_certificate_verdict: status.latest_transept_certificate_verdict.clone(),
        choir_receipt_verdict: status.choir_receipt_verdict.clone(),
        result: Some(serialize_enum(&status.result)),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        resolve_latest_transept_certificate_command_summary: Some(
            session.resolve_latest_transept_certificate_command_summary.clone(),
        ),
        run_latest_choir_receipt_command_summary: Some(
            session.run_latest_choir_receipt_command_summary.clone(),
        ),
        verify_latest_choir_receipt_command_summary: Some(
            session.verify_latest_choir_receipt_command_summary.clone(),
        ),
        downstream_choir_receipt_command_summary: Some(
            session.downstream_choir_receipt_command_summary.clone(),
        ),
        verify_choir_receipt_command_summary: Some(
            session.verify_choir_receipt_command_summary.clone(),
        ),
        reviewed_frozen_live_cutover_controller_command_summary: session
            .reviewed_frozen_live_cutover_controller_command_summary
            .clone(),
        clerestory_certificate_summary: session.clerestory_certificate_summary.clone(),
        clerestory_certificate_sha256: session.clerestory_certificate_sha256.clone(),
        clerestory_certificate_algorithm: session.clerestory_certificate_algorithm.clone(),
        verification_mismatches: mismatches,
        activation_authorized: false,
        explicit_statement:
            "this verification proves only whether the latest immutable chain still resolves cleanly into the accepted choir-receipt contract; it never runs the frozen controller"
                .to_string(),
    })
}

fn run_latest_choir_receipt_with_resolution(
    root: &Path,
    session_dir: &Path,
    paths: &PackageChoirReceiptLatestPaths,
    resolved: &ResolvedLatestTranseptCertificate,
) -> Result<PackageChoirReceiptLatestReport> {
    let session = planned_session(root, session_dir, paths, Some(resolved), None);
    persist_json(&paths.session_path, &session)?;
    persist_json(
        &paths.latest_transept_certificate_plan_report_path,
        &resolved.latest_transept_certificate_plan_raw,
    )?;

    let latest_transept_certificate_plan_step = Some(step_artifact(
        &paths.latest_transept_certificate_plan_report_path,
        "plan_latest_transept_certificate",
        &resolved.latest_transept_certificate_plan.verdict,
        &resolved.latest_transept_certificate_plan.reason,
        resolved.latest_transept_certificate_plan.generated_at,
    ));

    let latest_transept_certificate_run_raw = match run_json_command(
        TRANSEPT_CERTIFICATE_LATEST_BIN,
        &latest_transept_certificate_run_args(
            root,
            &paths.nested_transept_certificate_latest_session_dir,
        ),
    ) {
        Ok(value) => value,
        Err(error) => {
            let status = refusal_status(
                root,
                session_dir,
                paths,
                Some(resolved),
                LivePackageChoirReceiptLatestResult::RefusedByDriftedChoirReceiptContract,
                format!(
                    "failed running nested latest-transept-certificate handoff under {}: {error}",
                    paths
                        .nested_transept_certificate_latest_session_dir
                        .display()
                ),
                latest_transept_certificate_plan_step,
                None,
                None,
            );
            persist_json(&paths.status_path, &status)?;
            let report = report_from_status(
                "run_latest_choir_receipt",
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
        &paths.latest_transept_certificate_report_path,
        &latest_transept_certificate_run_raw,
    )?;
    let latest_transept_certificate_run: TranseptCertificateLatestReportView =
        serde_json::from_value(latest_transept_certificate_run_raw)
            .context("failed parsing nested latest-transept-certificate run json")?;
    let latest_transept_certificate_step = Some(step_artifact(
        &paths.latest_transept_certificate_report_path,
        "run_latest_transept_certificate",
        &latest_transept_certificate_run.verdict,
        &latest_transept_certificate_run.reason,
        latest_transept_certificate_run.generated_at,
    ));

    if let Some(terminal_failure) =
        latest_transept_certificate_terminal_failure(&latest_transept_certificate_run)
    {
        let status = refusal_status(
            root,
            session_dir,
            paths,
            Some(resolved),
            terminal_failure,
            latest_transept_certificate_run.reason.clone(),
            latest_transept_certificate_plan_step,
            latest_transept_certificate_step,
            None,
        );
        persist_json(&paths.status_path, &status)?;
        let report = report_from_status(
            "run_latest_choir_receipt",
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
    compare_transept_certificate_latest_run_against_snapshot(
        &latest_transept_certificate_run,
        &resolved.snapshot,
        &paths.nested_transept_certificate_latest_session_dir,
        &mut run_mismatches,
    );
    if !run_mismatches.is_empty() {
        let status = refusal_status(
            root,
            session_dir,
            paths,
            Some(resolved),
            LivePackageChoirReceiptLatestResult::RefusedByDriftedChoirReceiptContract,
            run_mismatches[0].clone(),
            latest_transept_certificate_plan_step,
            latest_transept_certificate_step,
            None,
        );
        persist_json(&paths.status_path, &status)?;
        let report = report_from_status(
            "run_latest_choir_receipt",
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

    let latest_transept_certificate_verify: TranseptCertificateLatestReportView =
        match run_json_command(
            TRANSEPT_CERTIFICATE_LATEST_BIN,
            &latest_transept_certificate_verify_args(
                &paths.nested_transept_certificate_latest_session_dir,
            ),
        ) {
            Ok(value) => serde_json::from_value(value)
                .context("failed parsing nested latest-transept-certificate verify json")?,
            Err(error) => {
                let status = refusal_status(
                    root,
                    session_dir,
                    paths,
                    Some(resolved),
                    LivePackageChoirReceiptLatestResult::RefusedByDriftedChoirReceiptContract,
                    format!(
                    "failed verifying nested latest-transept-certificate handoff under {}: {error}",
                    paths.nested_transept_certificate_latest_session_dir.display()
                ),
                    latest_transept_certificate_plan_step,
                    latest_transept_certificate_step,
                    None,
                );
                persist_json(&paths.status_path, &status)?;
                let report = report_from_status(
                    "run_latest_choir_receipt",
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
    if latest_transept_certificate_verify.verdict != TRANSEPT_CERTIFICATE_LATEST_VERIFY_OK {
        let status = refusal_status(
            root,
            session_dir,
            paths,
            Some(resolved),
            LivePackageChoirReceiptLatestResult::RefusedByDriftedChoirReceiptContract,
            latest_transept_certificate_verify.reason,
            latest_transept_certificate_plan_step,
            latest_transept_certificate_step,
            None,
        );
        persist_json(&paths.status_path, &status)?;
        let report = report_from_status(
            "run_latest_choir_receipt",
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
        CHOIR_RECEIPT_BIN,
        &choir_receipt_run_args(
            &resolved.snapshot,
            &expected_downstream_transept_certificate_session_dir(paths),
            &paths.nested_choir_receipt_session_dir,
        ),
    ) {
        Ok(value) => value,
        Err(error) => {
            let status = refusal_status(
                root,
                session_dir,
                paths,
                Some(resolved),
                LivePackageChoirReceiptLatestResult::RefusedByDriftedChoirReceiptContract,
                format!(
                    "failed running downstream choir-receipt contract under {}: {error}",
                    paths.nested_choir_receipt_session_dir.display()
                ),
                latest_transept_certificate_plan_step,
                latest_transept_certificate_step,
                None,
            );
            persist_json(&paths.status_path, &status)?;
            let report = report_from_status(
                "run_latest_choir_receipt",
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
    persist_json(&paths.choir_receipt_report_path, &release_capsule_run_raw)?;
    let provenance_certificate_run: ChoirReceiptReportView =
        serde_json::from_value(release_capsule_run_raw)
            .context("failed parsing nested choir-receipt run json")?;
    let choir_receipt_step = Some(step_artifact(
        &paths.choir_receipt_report_path,
        "run_live_package_choir_receipt",
        &provenance_certificate_run.verdict,
        &provenance_certificate_run.reason,
        provenance_certificate_run.generated_at,
    ));

    let mut provenance_certificate_mismatches = Vec::new();
    compare_choir_receipt_run_against_snapshot(
        &provenance_certificate_run,
        &resolved.snapshot,
        &expected_downstream_transept_certificate_session_dir(paths),
        &paths.nested_choir_receipt_session_dir,
        &mut provenance_certificate_mismatches,
    );
    if !provenance_certificate_mismatches.is_empty() {
        let status = refusal_status(
            root,
            session_dir,
            paths,
            Some(resolved),
            LivePackageChoirReceiptLatestResult::RefusedByDriftedChoirReceiptContract,
            provenance_certificate_mismatches[0].clone(),
            latest_transept_certificate_plan_step,
            latest_transept_certificate_step,
            choir_receipt_step,
        );
        persist_json(&paths.status_path, &status)?;
        let report = report_from_status(
            "run_latest_choir_receipt",
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

    let provenance_certificate_verify: ChoirReceiptReportView = match run_json_command(
        CHOIR_RECEIPT_BIN,
        &choir_receipt_verify_args(
            &resolved.snapshot,
            &expected_downstream_transept_certificate_session_dir(paths),
            &paths.nested_choir_receipt_session_dir,
        ),
    ) {
        Ok(value) => serde_json::from_value(value)
            .context("failed parsing nested choir-receipt verify json")?,
        Err(error) => {
            let status = refusal_status(
                root,
                session_dir,
                paths,
                Some(resolved),
                LivePackageChoirReceiptLatestResult::RefusedByDriftedChoirReceiptContract,
                format!(
                    "failed verifying downstream choir-receipt contract under {}: {error}",
                    paths.nested_choir_receipt_session_dir.display()
                ),
                latest_transept_certificate_plan_step,
                latest_transept_certificate_step,
                choir_receipt_step,
            );
            persist_json(&paths.status_path, &status)?;
            let report = report_from_status(
                "run_latest_choir_receipt",
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
    if provenance_certificate_verify.verdict != CHOIR_RECEIPT_VERIFY_OK {
        let status = refusal_status(
            root,
            session_dir,
            paths,
            Some(resolved),
            LivePackageChoirReceiptLatestResult::RefusedByDriftedChoirReceiptContract,
            provenance_certificate_verify.reason,
            latest_transept_certificate_plan_step,
            latest_transept_certificate_step,
            choir_receipt_step,
        );
        persist_json(&paths.status_path, &status)?;
        let report = report_from_status(
            "run_latest_choir_receipt",
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

    let result = transept_certificate_latest_result_from_nested_status(
        provenance_certificate_run
            .result
            .as_deref()
            .unwrap_or_default(),
    )
    .unwrap_or(LivePackageChoirReceiptLatestResult::RefusedByDriftedChoirReceiptContract);
    let status = PackageChoirReceiptLatestStatus {
        status_version: CHOIR_RECEIPT_LATEST_STATUS_VERSION.to_string(),
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
        nested_transept_certificate_latest_session_dir: paths
            .nested_transept_certificate_latest_session_dir
            .display()
            .to_string(),
        nested_choir_receipt_session_dir: paths
            .nested_choir_receipt_session_dir
            .display()
            .to_string(),
        downstream_decision_packet_session_dir: resolved
            .snapshot
            .downstream_decision_packet_session_dir
            .display()
            .to_string(),
        result,
        reason: provenance_certificate_run.reason.clone(),
        latest_transept_certificate_plan_verdict: Some(
            resolved.latest_transept_certificate_plan.verdict.clone(),
        ),
        latest_transept_certificate_verdict: Some(latest_transept_certificate_run.verdict.clone()),
        choir_receipt_verdict: Some(provenance_certificate_run.verdict.clone()),
        current_pre_activation_gate_verdict: provenance_certificate_run
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: provenance_certificate_run
            .current_pre_activation_gate_reason
            .clone(),
        latest_transept_certificate_plan_step,
        latest_transept_certificate_step,
        choir_receipt_step,
        operator_next_action_summary: operator_next_action_summary(result),
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    };
    persist_json(&paths.status_path, &status)?;
    let report = report_from_status(
        "run_latest_choir_receipt",
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

fn run_latest_choir_receipt_failure(
    root: &Path,
    session_dir: &Path,
    paths: &PackageChoirReceiptLatestPaths,
    failure: &LatestTranseptCertificateResolutionFailure,
) -> Result<PackageChoirReceiptLatestReport> {
    let session = planned_session(root, session_dir, paths, None, Some(failure));
    persist_json(&paths.session_path, &session)?;
    if let Some(value) = &failure.latest_transept_certificate_plan_raw {
        persist_json(&paths.latest_transept_certificate_plan_report_path, value)?;
    }
    let status = refusal_status_from_resolution_failure(root, session_dir, paths, failure);
    persist_json(&paths.status_path, &status)?;
    let report = report_from_status(
        "run_latest_choir_receipt",
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

fn resolve_latest_transept_certificate(
    root: &Path,
    paths: &PackageChoirReceiptLatestPaths,
) -> std::result::Result<
    ResolvedLatestTranseptCertificate,
    LatestTranseptCertificateResolutionFailure,
> {
    let latest_transept_certificate_plan_raw = match run_json_command(
        TRANSEPT_CERTIFICATE_LATEST_BIN,
        &latest_transept_certificate_plan_args(
            root,
            &paths.nested_transept_certificate_latest_session_dir,
        ),
    ) {
        Ok(value) => value,
        Err(error) => {
            return Err(LatestTranseptCertificateResolutionFailure {
                kind: ResolutionFailureKind::MissingLatestChain,
                reason: format!(
                    "failed resolving latest immutable chain under {}: {error}",
                    root.display()
                ),
                snapshot: None,
                latest_transept_certificate_plan_raw: None,
                latest_transept_certificate_plan_verdict: None,
                latest_transept_certificate_plan_reason: None,
                latest_transept_certificate_plan_generated_at: None,
            });
        }
    };
    let latest_transept_certificate_plan: TranseptCertificateLatestReportView =
        serde_json::from_value(latest_transept_certificate_plan_raw.clone()).map_err(
            |error| LatestTranseptCertificateResolutionFailure {
                kind: ResolutionFailureKind::InvalidLatestChain,
                reason: format!(
                    "failed parsing latest-transept-certificate upstream latest-transept-certificate plan json for {}: {error}",
                    root.display()
                ),
                snapshot: None,
                latest_transept_certificate_plan_raw: Some(
                    latest_transept_certificate_plan_raw.clone(),
                ),
                latest_transept_certificate_plan_verdict: None,
                latest_transept_certificate_plan_reason: None,
                latest_transept_certificate_plan_generated_at: None,
            },
        )?;

    let snapshot =
        parse_snapshot_from_latest_transept_certificate(&latest_transept_certificate_plan).ok();
    if latest_transept_certificate_plan.verdict != TRANSEPT_CERTIFICATE_LATEST_PLAN_READY {
        return Err(LatestTranseptCertificateResolutionFailure {
            kind: map_transept_certificate_latest_failure_kind(
                &latest_transept_certificate_plan.verdict,
            ),
            reason: format!(
                "latest-transept-certificate resolution refused because nested latest-transept-certificate reported: {}",
                latest_transept_certificate_plan.reason
            ),
            snapshot,
            latest_transept_certificate_plan_raw: Some(latest_transept_certificate_plan_raw),
            latest_transept_certificate_plan_verdict: Some(
                latest_transept_certificate_plan.verdict,
            ),
            latest_transept_certificate_plan_reason: Some(
                latest_transept_certificate_plan.reason,
            ),
            latest_transept_certificate_plan_generated_at: Some(
                latest_transept_certificate_plan.generated_at,
            ),
        });
    }

    let snapshot =
        parse_snapshot_from_latest_transept_certificate(&latest_transept_certificate_plan)
            .map_err(|error| LatestTranseptCertificateResolutionFailure {
                kind: ResolutionFailureKind::InvalidLatestChain,
                reason: format!("latest-transept-certificate resolution failed: {error}"),
                snapshot: None,
                latest_transept_certificate_plan_raw: Some(
                    latest_transept_certificate_plan_raw.clone(),
                ),
                latest_transept_certificate_plan_verdict: Some(
                    latest_transept_certificate_plan.verdict.clone(),
                ),
                latest_transept_certificate_plan_reason: Some(
                    latest_transept_certificate_plan.reason.clone(),
                ),
                latest_transept_certificate_plan_generated_at: Some(
                    latest_transept_certificate_plan.generated_at,
                ),
            })?;

    let mut mismatches = Vec::new();
    compare_transept_certificate_latest_plan_against_snapshot(
        &latest_transept_certificate_plan,
        &snapshot,
        &paths.nested_transept_certificate_latest_session_dir,
        &mut mismatches,
    );
    if !mismatches.is_empty() {
        return Err(LatestTranseptCertificateResolutionFailure {
            kind: ResolutionFailureKind::DriftedTranseptCertificateContract,
            reason: format!(
                "latest-transept-certificate drift detected: {}",
                mismatches[0]
            ),
            snapshot: Some(snapshot),
            latest_transept_certificate_plan_raw: Some(latest_transept_certificate_plan_raw),
            latest_transept_certificate_plan_verdict: Some(
                latest_transept_certificate_plan.verdict,
            ),
            latest_transept_certificate_plan_reason: Some(latest_transept_certificate_plan.reason),
            latest_transept_certificate_plan_generated_at: Some(
                latest_transept_certificate_plan.generated_at,
            ),
        });
    }

    Ok(ResolvedLatestTranseptCertificate {
        snapshot,
        latest_transept_certificate_plan_raw,
        latest_transept_certificate_plan,
    })
}

fn parse_snapshot_from_latest_transept_certificate(
    view: &TranseptCertificateLatestReportView,
) -> Result<LatestTranseptCertificateSnapshot> {
    let latest_top_step_name =
        required_option_string(view.latest_top_step_name.as_ref(), "latest_top_step_name")?;
    if latest_top_step_name != TOP_ACCEPTED_PACKAGE_STEP {
        bail!(
            "latest-transept-certificate nested latest-transept-certificate plan top accepted step is {} instead of {}",
            latest_top_step_name,
            TOP_ACCEPTED_PACKAGE_STEP
        );
    }
    if view.activation_authorized {
        bail!(
            "latest-transept-certificate nested latest-transept-certificate plan must remain planning-safe with activation_authorized=false"
        );
    }
    Ok(LatestTranseptCertificateSnapshot {
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
            anyhow!(
                "latest-transept-certificate nested latest-transept-certificate plan is missing wrapper_timeout_ms"
            )
        })?,
        service_status_max_staleness_ms: view.service_status_max_staleness_ms.ok_or_else(|| {
            anyhow!(
                "latest-transept-certificate nested latest-transept-certificate plan is missing service_status_max_staleness_ms"
            )
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
    verdict: TinyLivePackageChoirReceiptLatestVerdict,
    reason: String,
    root: &Path,
    session_dir: &Path,
    paths: &PackageChoirReceiptLatestPaths,
    resolved: &ResolvedLatestTranseptCertificate,
) -> PackageChoirReceiptLatestReport {
    PackageChoirReceiptLatestReport {
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
        choir_receipt_latest_session_path: Some(paths.session_path.display().to_string()),
        choir_receipt_latest_status_path: Some(paths.status_path.display().to_string()),
        choir_receipt_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_transept_certificate_plan_report_path: Some(
            paths
                .latest_transept_certificate_plan_report_path
                .display()
                .to_string(),
        ),
        latest_transept_certificate_report_path: Some(
            paths
                .latest_transept_certificate_report_path
                .display()
                .to_string(),
        ),
        choir_receipt_report_path: Some(paths.choir_receipt_report_path.display().to_string()),
        nested_transept_certificate_latest_session_dir: Some(
            paths
                .nested_transept_certificate_latest_session_dir
                .display()
                .to_string(),
        ),
        nested_choir_receipt_session_dir: Some(
            paths.nested_choir_receipt_session_dir.display().to_string(),
        ),
        downstream_decision_packet_session_dir: Some(
            resolved
                .snapshot
                .downstream_decision_packet_session_dir
                .display()
                .to_string(),
        ),
        latest_transept_certificate_plan_verdict: Some(
            resolved.latest_transept_certificate_plan.verdict.clone(),
        ),
        latest_transept_certificate_verdict: None,
        choir_receipt_verdict: None,
        result: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        resolve_latest_transept_certificate_command_summary: Some(
            resolve_latest_transept_certificate_command_summary(
                root,
                &paths.nested_transept_certificate_latest_session_dir,
            ),
        ),
        run_latest_choir_receipt_command_summary: Some(run_latest_choir_receipt_command_summary(
            root,
            session_dir,
        )),
        verify_latest_choir_receipt_command_summary: Some(
            verify_latest_choir_receipt_command_summary(session_dir),
        ),
        downstream_choir_receipt_command_summary: Some(downstream_choir_receipt_command_summary(
            &resolved.snapshot,
            &expected_downstream_transept_certificate_session_dir(paths),
            &paths.nested_choir_receipt_session_dir,
        )),
        verify_choir_receipt_command_summary: Some(verify_choir_receipt_command_summary(
            &resolved.snapshot,
            &expected_downstream_transept_certificate_session_dir(paths),
            &paths.nested_choir_receipt_session_dir,
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
    paths: &PackageChoirReceiptLatestPaths,
    failure: &LatestTranseptCertificateResolutionFailure,
) -> PackageChoirReceiptLatestReport {
    let snapshot = failure.snapshot.as_ref();
    PackageChoirReceiptLatestReport {
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
        choir_receipt_latest_session_path: Some(paths.session_path.display().to_string()),
        choir_receipt_latest_status_path: Some(paths.status_path.display().to_string()),
        choir_receipt_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_transept_certificate_plan_report_path: Some(
            paths
                .latest_transept_certificate_plan_report_path
                .display()
                .to_string(),
        ),
        latest_transept_certificate_report_path: Some(
            paths
                .latest_transept_certificate_report_path
                .display()
                .to_string(),
        ),
        choir_receipt_report_path: Some(paths.choir_receipt_report_path.display().to_string()),
        nested_transept_certificate_latest_session_dir: Some(
            paths
                .nested_transept_certificate_latest_session_dir
                .display()
                .to_string(),
        ),
        nested_choir_receipt_session_dir: Some(
            paths.nested_choir_receipt_session_dir.display().to_string(),
        ),
        downstream_decision_packet_session_dir: snapshot.map(|value| {
            value
                .downstream_decision_packet_session_dir
                .display()
                .to_string()
        }),
        latest_transept_certificate_plan_verdict: failure
            .latest_transept_certificate_plan_verdict
            .clone(),
        latest_transept_certificate_verdict: None,
        choir_receipt_verdict: None,
        result: Some(serialize_enum(&result_from_failure_kind(failure.kind))),
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        resolve_latest_transept_certificate_command_summary: Some(
            resolve_latest_transept_certificate_command_summary(
                root,
                &paths.nested_transept_certificate_latest_session_dir,
            ),
        ),
        run_latest_choir_receipt_command_summary: Some(run_latest_choir_receipt_command_summary(
            root,
            session_dir,
        )),
        verify_latest_choir_receipt_command_summary: Some(
            verify_latest_choir_receipt_command_summary(session_dir),
        ),
        downstream_choir_receipt_command_summary: snapshot.map(|value| {
            downstream_choir_receipt_command_summary(
                value,
                &expected_downstream_transept_certificate_session_dir(paths),
                &paths.nested_choir_receipt_session_dir,
            )
        }),
        verify_choir_receipt_command_summary: snapshot.map(|value| {
            verify_choir_receipt_command_summary(
                value,
                &expected_downstream_transept_certificate_session_dir(paths),
                &paths.nested_choir_receipt_session_dir,
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
    paths: &PackageChoirReceiptLatestPaths,
    resolved: Option<&ResolvedLatestTranseptCertificate>,
    failure: Option<&LatestTranseptCertificateResolutionFailure>,
) -> PackageChoirReceiptLatestSession {
    let snapshot = resolved
        .map(|value| &value.snapshot)
        .or_else(|| failure.and_then(|value| value.snapshot.as_ref()));
    PackageChoirReceiptLatestSession {
        session_version: CHOIR_RECEIPT_LATEST_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        root: root.display().to_string(),
        session_dir: session_dir.display().to_string(),
        nested_transept_certificate_latest_session_dir: paths
            .nested_transept_certificate_latest_session_dir
            .display()
            .to_string(),
        nested_choir_receipt_session_dir: paths
            .nested_choir_receipt_session_dir
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
        resolve_latest_transept_certificate_command_summary:
            resolve_latest_transept_certificate_command_summary(
                root,
                &paths.nested_transept_certificate_latest_session_dir,
            ),
        run_latest_choir_receipt_command_summary: run_latest_choir_receipt_command_summary(
            root,
            session_dir,
        ),
        verify_latest_choir_receipt_command_summary: verify_latest_choir_receipt_command_summary(
            session_dir,
        ),
        downstream_choir_receipt_command_summary: snapshot
            .map(|value| {
                downstream_choir_receipt_command_summary(
                    value,
                    &expected_downstream_transept_certificate_session_dir(paths),
                    &paths.nested_choir_receipt_session_dir,
                )
            })
            .unwrap_or_else(|| "<choir-receipt-run-unavailable>".to_string()),
        verify_choir_receipt_command_summary: snapshot
            .map(|value| {
                verify_choir_receipt_command_summary(
                    value,
                    &expected_downstream_transept_certificate_session_dir(paths),
                    &paths.nested_choir_receipt_session_dir,
                )
            })
            .unwrap_or_else(|| "<choir-receipt-verify-unavailable>".to_string()),
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
    verdict: TinyLivePackageChoirReceiptLatestVerdict,
    session_dir: &Path,
    paths: &PackageChoirReceiptLatestPaths,
    session: &PackageChoirReceiptLatestSession,
    status: &PackageChoirReceiptLatestStatus,
    verification_mismatches: Vec<String>,
) -> PackageChoirReceiptLatestReport {
    PackageChoirReceiptLatestReport {
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
        choir_receipt_latest_session_path: Some(paths.session_path.display().to_string()),
        choir_receipt_latest_status_path: Some(paths.status_path.display().to_string()),
        choir_receipt_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_transept_certificate_plan_report_path: Some(
            paths
                .latest_transept_certificate_plan_report_path
                .display()
                .to_string(),
        ),
        latest_transept_certificate_report_path: Some(
            paths
                .latest_transept_certificate_report_path
                .display()
                .to_string(),
        ),
        choir_receipt_report_path: Some(paths.choir_receipt_report_path.display().to_string()),
        nested_transept_certificate_latest_session_dir: Some(
            session
                .nested_transept_certificate_latest_session_dir
                .clone(),
        ),
        nested_choir_receipt_session_dir: Some(session.nested_choir_receipt_session_dir.clone()),
        downstream_decision_packet_session_dir: Some(
            session.downstream_decision_packet_session_dir.clone(),
        ),
        latest_transept_certificate_plan_verdict: status
            .latest_transept_certificate_plan_verdict
            .clone(),
        latest_transept_certificate_verdict: status.latest_transept_certificate_verdict.clone(),
        choir_receipt_verdict: status.choir_receipt_verdict.clone(),
        result: Some(serialize_enum(&status.result)),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        resolve_latest_transept_certificate_command_summary: Some(
            session
                .resolve_latest_transept_certificate_command_summary
                .clone(),
        ),
        run_latest_choir_receipt_command_summary: Some(
            session.run_latest_choir_receipt_command_summary.clone(),
        ),
        verify_latest_choir_receipt_command_summary: Some(
            session.verify_latest_choir_receipt_command_summary.clone(),
        ),
        downstream_choir_receipt_command_summary: Some(
            session.downstream_choir_receipt_command_summary.clone(),
        ),
        verify_choir_receipt_command_summary: Some(
            session.verify_choir_receipt_command_summary.clone(),
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

fn choir_receipt_latest_paths(session_dir: &Path) -> PackageChoirReceiptLatestPaths {
    PackageChoirReceiptLatestPaths {
        session_path: session_dir
            .join("tiny_live_activation_package_choir_receipt_latest.session.json"),
        status_path: session_dir
            .join("tiny_live_activation_package_choir_receipt_latest.status.json"),
        report_path: session_dir
            .join("tiny_live_activation_package_choir_receipt_latest.report.json"),
        latest_transept_certificate_plan_report_path: session_dir.join(
            "tiny_live_activation_package_choir_receipt_latest.transept_certificate_latest_plan.report.json",
        ),
        latest_transept_certificate_report_path: session_dir.join(
            "tiny_live_activation_package_choir_receipt_latest.transept_certificate_latest.report.json",
        ),
        choir_receipt_report_path: session_dir.join(
            "tiny_live_activation_package_choir_receipt_latest.choir_receipt.report.json",
        ),
        nested_transept_certificate_latest_session_dir: session_dir.join(
            "tiny_live_activation_package_choir_receipt_latest.transept_certificate_latest_session",
        ),
        nested_choir_receipt_session_dir: session_dir.join(
            "tiny_live_activation_package_choir_receipt_latest.choir_receipt_session",
        ),
    }
}

fn expected_downstream_decision_packet_session_dir(
    paths: &PackageChoirReceiptLatestPaths,
) -> PathBuf {
    paths
        .nested_transept_certificate_latest_session_dir
        .join("tiny_live_activation_package_transept_certificate_latest.decision_packet_session")
}

fn expected_downstream_transept_certificate_session_dir(
    paths: &PackageChoirReceiptLatestPaths,
) -> PathBuf {
    paths.nested_transept_certificate_latest_session_dir.join(
        "tiny_live_activation_package_transept_certificate_latest.transept_certificate_session",
    )
}

fn expected_downstream_review_receipt_session_dir(
    paths: &PackageChoirReceiptLatestPaths,
) -> PathBuf {
    paths
        .nested_transept_certificate_latest_session_dir
        .join("tiny_live_activation_package_transept_certificate_latest.review_receipt_session")
}

fn latest_transept_certificate_plan_args(root: &Path, session_dir: &Path) -> Vec<String> {
    vec![
        "--root".to_string(),
        root.display().to_string(),
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--plan-latest-transept-certificate".to_string(),
        "--json".to_string(),
    ]
}

fn latest_transept_certificate_run_args(root: &Path, session_dir: &Path) -> Vec<String> {
    vec![
        "--root".to_string(),
        root.display().to_string(),
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--run-latest-transept-certificate".to_string(),
        "--json".to_string(),
    ]
}

fn latest_transept_certificate_verify_args(session_dir: &Path) -> Vec<String> {
    vec![
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--verify-latest-transept-certificate".to_string(),
        "--json".to_string(),
    ]
}

fn choir_receipt_run_args(
    snapshot: &LatestTranseptCertificateSnapshot,
    transept_certificate_session_dir: &Path,
    session_dir: &Path,
) -> Vec<String> {
    vec![
        "--transept-certificate-session-dir".to_string(),
        transept_certificate_session_dir.display().to_string(),
        "--confirm-decision-packet-session-dir".to_string(),
        snapshot
            .downstream_decision_packet_session_dir
            .display()
            .to_string(),
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--run-live-package-choir-receipt".to_string(),
        "--json".to_string(),
    ]
}

fn choir_receipt_verify_args(
    snapshot: &LatestTranseptCertificateSnapshot,
    transept_certificate_session_dir: &Path,
    session_dir: &Path,
) -> Vec<String> {
    vec![
        "--transept-certificate-session-dir".to_string(),
        transept_certificate_session_dir.display().to_string(),
        "--confirm-decision-packet-session-dir".to_string(),
        snapshot
            .downstream_decision_packet_session_dir
            .display()
            .to_string(),
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--verify-live-package-choir-receipt".to_string(),
        "--json".to_string(),
    ]
}

fn resolve_latest_transept_certificate_command_summary(root: &Path, session_dir: &Path) -> String {
    format!(
        "{TRANSEPT_CERTIFICATE_LATEST_BIN} --root {} --session-dir {} --plan-latest-transept-certificate --json",
        shell_escape_path(root),
        shell_escape_path(session_dir),
    )
}

fn run_latest_choir_receipt_command_summary(root: &Path, session_dir: &Path) -> String {
    format!(
        "{CHOIR_RECEIPT_LATEST_BIN} --root {} --session-dir {} --run-latest-choir-receipt --json",
        shell_escape_path(root),
        shell_escape_path(session_dir),
    )
}

fn verify_latest_choir_receipt_command_summary(session_dir: &Path) -> String {
    format!(
        "{CHOIR_RECEIPT_LATEST_BIN} --session-dir {} --verify-latest-choir-receipt --json",
        shell_escape_path(session_dir),
    )
}

fn downstream_choir_receipt_command_summary(
    snapshot: &LatestTranseptCertificateSnapshot,
    transept_certificate_session_dir: &Path,
    session_dir: &Path,
) -> String {
    format!(
        "{CHOIR_RECEIPT_BIN} --transept-certificate-session-dir {} --confirm-decision-packet-session-dir {} --session-dir {} --run-live-package-choir-receipt --json",
        shell_escape_path(transept_certificate_session_dir),
        shell_escape_path(&snapshot.downstream_decision_packet_session_dir),
        shell_escape_path(session_dir),
    )
}

fn verify_choir_receipt_command_summary(
    snapshot: &LatestTranseptCertificateSnapshot,
    transept_certificate_session_dir: &Path,
    session_dir: &Path,
) -> String {
    format!(
        "{CHOIR_RECEIPT_BIN} --transept-certificate-session-dir {} --confirm-decision-packet-session-dir {} --session-dir {} --verify-live-package-choir-receipt --json",
        shell_escape_path(transept_certificate_session_dir),
        shell_escape_path(&snapshot.downstream_decision_packet_session_dir),
        shell_escape_path(session_dir),
    )
}

fn nested_transept_certificate_latest_report_path(session_dir: &Path) -> PathBuf {
    session_dir.join("tiny_live_activation_package_transept_certificate_latest.report.json")
}

fn nested_choir_receipt_session_path(session_dir: &Path) -> PathBuf {
    session_dir.join("tiny_live_activation_package_choir_receipt.session.json")
}

fn nested_choir_receipt_status_path(session_dir: &Path) -> PathBuf {
    session_dir.join("tiny_live_activation_package_choir_receipt.status.json")
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
            "failed writing latest-choir-receipt script to {}",
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
) -> PackageChoirReceiptLatestStepArtifact {
    PackageChoirReceiptLatestStepArtifact {
        path: path.display().to_string(),
        mode: mode.to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        generated_at,
    }
}

fn canonical_wrapper_mode(mode: &str) -> &str {
    match mode {
        "plan_latest_choir_receipt" => "plan_latest_choir_receipt",
        "render_latest_choir_receipt_script" => "render_latest_choir_receipt_script",
        "run_latest_choir_receipt" => "run_latest_choir_receipt",
        "verify_latest_choir_receipt" => "verify_latest_choir_receipt",
        other => other,
    }
}

fn load_required_step_json<T: for<'de> Deserialize<'de>>(
    step: &Option<PackageChoirReceiptLatestStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<T> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("{label} is missing from latest-choir-receipt status"))?;
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
    step: Option<&PackageChoirReceiptLatestStepArtifact>,
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
            "{label} is missing from latest-choir-receipt status"
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

fn compare_transept_certificate_latest_plan_against_snapshot(
    stored: &TranseptCertificateLatestReportView,
    snapshot: &LatestTranseptCertificateSnapshot,
    session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        Some(stored.verdict.as_str()),
        Some(TRANSEPT_CERTIFICATE_LATEST_PLAN_READY),
        "stored latest-transept-certificate plan verdict",
        mismatches,
    );
    compare_string(
        &stored.mode,
        "plan_latest_transept_certificate",
        "stored latest-transept-certificate plan mode",
        mismatches,
    );
    compare_option_string(
        stored.latest_top_step_name.as_deref(),
        Some(snapshot.latest_top_step_name.as_str()),
        "stored latest-transept-certificate plan latest_top_step_name",
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
        "stored latest-transept-certificate plan latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored latest-transept-certificate plan session_dir",
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
        "stored latest-transept-certificate plan downstream_decision_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.package_dir.as_deref(),
        Some(snapshot.package_dir.display().to_string().as_str()),
        "stored latest-transept-certificate plan package_dir",
        mismatches,
    );
    compare_option_string(
        stored.install_root.as_deref(),
        Some(snapshot.install_root.display().to_string().as_str()),
        "stored latest-transept-certificate plan install_root",
        mismatches,
    );
    compare_option_string(
        stored.target_service_name.as_deref(),
        Some(snapshot.target_service_name.as_str()),
        "stored latest-transept-certificate plan target_service_name",
        mismatches,
    );
    compare_option_string(
        stored.backend_command.as_deref(),
        Some(snapshot.backend_command.as_str()),
        "stored latest-transept-certificate plan backend_command",
        mismatches,
    );
    compare_option_u64(
        stored.wrapper_timeout_ms,
        Some(snapshot.wrapper_timeout_ms),
        "stored latest-transept-certificate plan wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        stored.service_status_max_staleness_ms,
        Some(snapshot.service_status_max_staleness_ms),
        "stored latest-transept-certificate plan service_status_max_staleness_ms",
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
        "stored latest-transept-certificate plan reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_summary.as_deref(),
        Some(snapshot.clerestory_certificate_summary.as_str()),
        "stored latest-transept-certificate plan clerestory_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_sha256.as_deref(),
        Some(snapshot.clerestory_certificate_sha256.as_str()),
        "stored latest-transept-certificate plan clerestory_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_algorithm.as_deref(),
        Some(snapshot.clerestory_certificate_algorithm.as_str()),
        "stored latest-transept-certificate plan clerestory_certificate_algorithm",
        mismatches,
    );
}

fn compare_transept_certificate_latest_run_against_snapshot(
    stored: &TranseptCertificateLatestReportView,
    snapshot: &LatestTranseptCertificateSnapshot,
    session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &stored.mode,
        "run_latest_transept_certificate",
        "stored latest-transept-certificate run mode",
        mismatches,
    );
    compare_option_string(
        stored.latest_top_step_name.as_deref(),
        Some(snapshot.latest_top_step_name.as_str()),
        "stored latest-transept-certificate run latest_top_step_name",
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
        "stored latest-transept-certificate run latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored latest-transept-certificate run session_dir",
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
        "stored latest-transept-certificate run downstream_decision_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.package_dir.as_deref(),
        Some(snapshot.package_dir.display().to_string().as_str()),
        "stored latest-transept-certificate run package_dir",
        mismatches,
    );
    compare_option_string(
        stored.install_root.as_deref(),
        Some(snapshot.install_root.display().to_string().as_str()),
        "stored latest-transept-certificate run install_root",
        mismatches,
    );
    compare_option_string(
        stored.target_service_name.as_deref(),
        Some(snapshot.target_service_name.as_str()),
        "stored latest-transept-certificate run target_service_name",
        mismatches,
    );
    compare_option_string(
        stored.backend_command.as_deref(),
        Some(snapshot.backend_command.as_str()),
        "stored latest-transept-certificate run backend_command",
        mismatches,
    );
    compare_option_u64(
        stored.wrapper_timeout_ms,
        Some(snapshot.wrapper_timeout_ms),
        "stored latest-transept-certificate run wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        stored.service_status_max_staleness_ms,
        Some(snapshot.service_status_max_staleness_ms),
        "stored latest-transept-certificate run service_status_max_staleness_ms",
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
        "stored latest-transept-certificate run reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_summary.as_deref(),
        Some(snapshot.clerestory_certificate_summary.as_str()),
        "stored latest-transept-certificate run clerestory_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_sha256.as_deref(),
        Some(snapshot.clerestory_certificate_sha256.as_str()),
        "stored latest-transept-certificate run clerestory_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_algorithm.as_deref(),
        Some(snapshot.clerestory_certificate_algorithm.as_str()),
        "stored latest-transept-certificate run clerestory_certificate_algorithm",
        mismatches,
    );
}

fn compare_transept_certificate_latest_copy_matches_actual(
    stored: &TranseptCertificateLatestReportView,
    actual: &TranseptCertificateLatestReportView,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        Some(stored.verdict.as_str()),
        Some(actual.verdict.as_str()),
        "stored latest-transept-certificate copy verdict",
        mismatches,
    );
    compare_option_string(
        Some(stored.reason.as_str()),
        Some(actual.reason.as_str()),
        "stored latest-transept-certificate copy reason",
        mismatches,
    );
    if stored.generated_at != actual.generated_at {
        mismatches.push(format!(
            "stored latest-transept-certificate copy generated_at {:?} does not match actual {:?}",
            stored.generated_at, actual.generated_at
        ));
    }
    compare_option_string(
        stored.latest_top_step_name.as_deref(),
        actual.latest_top_step_name.as_deref(),
        "stored latest-transept-certificate copy latest_top_step_name",
        mismatches,
    );
    compare_option_string(
        stored.latest_top_session_dir.as_deref(),
        actual.latest_top_session_dir.as_deref(),
        "stored latest-transept-certificate copy latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        actual.session_dir.as_deref(),
        "stored latest-transept-certificate copy session_dir",
        mismatches,
    );
    compare_option_string(
        stored.downstream_decision_packet_session_dir.as_deref(),
        actual.downstream_decision_packet_session_dir.as_deref(),
        "stored latest-transept-certificate copy downstream_decision_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.result.as_deref(),
        actual.result.as_deref(),
        "stored latest-transept-certificate copy result",
        mismatches,
    );
    compare_option_string(
        stored.explicit_statement.as_deref(),
        actual.explicit_statement.as_deref(),
        "stored latest-transept-certificate copy explicit_statement",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_verdict.as_deref(),
        actual.current_pre_activation_gate_verdict.as_deref(),
        "stored latest-transept-certificate copy current_pre_activation_gate_verdict",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_reason.as_deref(),
        actual.current_pre_activation_gate_reason.as_deref(),
        "stored latest-transept-certificate copy current_pre_activation_gate_reason",
        mismatches,
    );
    compare_option_string(
        stored
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        actual
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        "stored latest-transept-certificate copy reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
}

fn compare_choir_receipt_run_against_snapshot(
    stored: &ChoirReceiptReportView,
    snapshot: &LatestTranseptCertificateSnapshot,
    transept_certificate_session_dir: &Path,
    session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &stored.mode,
        "run_live_package_choir_receipt",
        "stored choir-receipt run mode",
        mismatches,
    );
    compare_string(
        &stored.transept_certificate_session_dir,
        &transept_certificate_session_dir.display().to_string(),
        "stored choir-receipt run transept_certificate_session_dir",
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
        "stored choir-receipt run decision_packet_session_dir",
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
        "stored choir-receipt run execute_frozen_session_dir",
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
        "stored choir-receipt run turn_green_session_dir",
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
        "stored choir-receipt run launch_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.package_dir.as_deref(),
        Some(snapshot.package_dir.display().to_string().as_str()),
        "stored choir-receipt run package_dir",
        mismatches,
    );
    compare_option_string(
        stored.install_root.as_deref(),
        Some(snapshot.install_root.display().to_string().as_str()),
        "stored choir-receipt run install_root",
        mismatches,
    );
    compare_option_string(
        stored.target_service_name.as_deref(),
        Some(snapshot.target_service_name.as_str()),
        "stored choir-receipt run target_service_name",
        mismatches,
    );
    compare_option_string(
        stored.backend_command.as_deref(),
        Some(snapshot.backend_command.as_str()),
        "stored choir-receipt run backend_command",
        mismatches,
    );
    compare_option_u64(
        stored.wrapper_timeout_ms,
        Some(snapshot.wrapper_timeout_ms),
        "stored choir-receipt run wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        stored.service_status_max_staleness_ms,
        Some(snapshot.service_status_max_staleness_ms),
        "stored choir-receipt run service_status_max_staleness_ms",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored choir-receipt run session_dir",
        mismatches,
    );
    compare_option_string(
        stored.choir_receipt_session_path.as_deref(),
        Some(
            nested_choir_receipt_session_path(session_dir)
                .display()
                .to_string()
                .as_str(),
        ),
        "stored choir-receipt run choir_receipt_session_path",
        mismatches,
    );
    compare_option_string(
        stored.choir_receipt_status_path.as_deref(),
        Some(
            nested_choir_receipt_status_path(session_dir)
                .display()
                .to_string()
                .as_str(),
        ),
        "stored choir-receipt run choir_receipt_status_path",
        mismatches,
    );
    compare_option_string(
        stored.archived_transept_certificate_report_path.as_deref(),
        Some(
            session_dir
                .join("tiny_live_activation_package_choir_receipt.transept_certificate.report.json")
                .display()
                .to_string()
                .as_str(),
        ),
        "stored choir-receipt run archived_transept_certificate_report_path",
        mismatches,
    );
    compare_option_string(
        stored
            .verify_transept_certificate_command_summary
            .as_deref(),
        Some(verify_choir_receipt_input_command_summary(transept_certificate_session_dir).as_str()),
        "stored choir-receipt run verify_transept_certificate_command_summary",
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
        "stored choir-receipt run reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
}

fn compare_choir_receipt_copy_against_nested_truth(
    stored: &ChoirReceiptReportView,
    nested_session: &ChoirReceiptSessionView,
    nested_status: &ChoirReceiptStatusView,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        stored.pedestal_certificate_session_dir.as_deref(),
        Some(nested_session.pedestal_certificate_session_dir.as_str()),
        "stored choir-receipt copy pedestal_certificate_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.plinth_receipt_session_dir.as_deref(),
        Some(nested_session.plinth_receipt_session_dir.as_str()),
        "stored choir-receipt copy plinth_receipt_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.registry_entry_session_dir.as_deref(),
        Some(nested_session.registry_entry_session_dir.as_str()),
        "stored choir-receipt copy registry_entry_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.notarization_receipt_session_dir.as_deref(),
        Some(nested_session.notarization_receipt_session_dir.as_str()),
        "stored choir-receipt copy notarization_receipt_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.provenance_certificate_session_dir.as_deref(),
        Some(nested_session.provenance_certificate_session_dir.as_str()),
        "stored choir-receipt copy provenance_certificate_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.attestation_seal_session_dir.as_deref(),
        Some(nested_session.attestation_seal_session_dir.as_str()),
        "stored choir-receipt copy attestation_seal_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.release_capsule_session_dir.as_deref(),
        Some(nested_session.release_capsule_session_dir.as_str()),
        "stored choir-receipt copy release_capsule_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.activation_ticket_session_dir.as_deref(),
        Some(nested_session.activation_ticket_session_dir.as_str()),
        "stored choir-receipt copy activation_ticket_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.review_receipt_session_dir.as_deref(),
        Some(nested_session.review_receipt_session_dir.as_str()),
        "stored choir-receipt copy review_receipt_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.handoff_bundle_session_dir.as_deref(),
        Some(nested_session.handoff_bundle_session_dir.as_str()),
        "stored choir-receipt copy handoff_bundle_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.decision_packet_session_dir.as_deref(),
        Some(nested_session.decision_packet_session_dir.as_str()),
        "stored choir-receipt copy decision_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.execute_frozen_session_dir.as_deref(),
        Some(nested_session.execute_frozen_session_dir.as_str()),
        "stored choir-receipt copy execute_frozen_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.turn_green_session_dir.as_deref(),
        Some(nested_session.turn_green_session_dir.as_str()),
        "stored choir-receipt copy turn_green_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.launch_packet_session_dir.as_deref(),
        Some(nested_session.launch_packet_session_dir.as_str()),
        "stored choir-receipt copy launch_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(nested_session.session_dir.as_str()),
        "stored choir-receipt copy session_dir",
        mismatches,
    );
    compare_option_string(
        stored.result.as_deref(),
        Some(nested_status.result.as_str()),
        "stored choir-receipt copy result",
        mismatches,
    );
    compare_option_string(
        Some(stored.verdict.as_str()),
        choir_receipt_run_verdict_for_result(&nested_status.result),
        "stored choir-receipt copy verdict",
        mismatches,
    );
    compare_option_string(
        stored.handoff_bundle_result.as_deref(),
        nested_status.handoff_bundle_result.as_deref(),
        "stored choir-receipt copy handoff_bundle_result",
        mismatches,
    );
    compare_option_string(
        stored.decision_packet_result.as_deref(),
        nested_status.decision_packet_result.as_deref(),
        "stored choir-receipt copy decision_packet_result",
        mismatches,
    );
    compare_option_string(
        stored.execute_frozen_result.as_deref(),
        nested_status.execute_frozen_result.as_deref(),
        "stored choir-receipt copy execute_frozen_result",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_verdict.as_deref(),
        nested_status.current_pre_activation_gate_verdict.as_deref(),
        "stored choir-receipt copy current_pre_activation_gate_verdict",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_reason.as_deref(),
        nested_status.current_pre_activation_gate_reason.as_deref(),
        "stored choir-receipt copy current_pre_activation_gate_reason",
        mismatches,
    );
    compare_option_string(
        stored
            .verify_transept_certificate_command_summary
            .as_deref(),
        Some(
            nested_session
                .verify_transept_certificate_command_summary
                .as_str(),
        ),
        "stored choir-receipt copy verify_transept_certificate_command_summary",
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
        "stored choir-receipt copy reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_option_string(
        stored.provenance_certificate_summary.as_deref(),
        Some(nested_status.provenance_certificate_summary.as_str()),
        "stored choir-receipt copy provenance_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.chain_fingerprint_summary.as_deref(),
        Some(nested_status.chain_fingerprint_summary.as_str()),
        "stored choir-receipt copy chain_fingerprint_summary",
        mismatches,
    );
    compare_option_string(
        stored.chain_fingerprint_sha256.as_deref(),
        Some(nested_status.chain_fingerprint_sha256.as_str()),
        "stored choir-receipt copy chain_fingerprint_sha256",
        mismatches,
    );
    compare_option_string(
        stored.chain_fingerprint_algorithm.as_deref(),
        Some(nested_status.chain_fingerprint_algorithm.as_str()),
        "stored choir-receipt copy chain_fingerprint_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.notarization_receipt_summary.as_deref(),
        Some(nested_status.notarization_receipt_summary.as_str()),
        "stored choir-receipt copy notarization_receipt_summary",
        mismatches,
    );
    compare_option_string(
        stored.release_capsule_digest_manifest_sha256.as_deref(),
        Some(
            nested_status
                .release_capsule_digest_manifest_sha256
                .as_str(),
        ),
        "stored choir-receipt copy release_capsule_digest_manifest_sha256",
        mismatches,
    );
    compare_option_usize(
        stored.release_capsule_digest_manifest_entry_count,
        Some(nested_status.release_capsule_digest_manifest_entry_count),
        "stored choir-receipt copy release_capsule_digest_manifest_entry_count",
        mismatches,
    );
    compare_option_string(
        stored.release_capsule_digest_algorithm.as_deref(),
        Some(nested_status.release_capsule_digest_algorithm.as_str()),
        "stored choir-receipt copy release_capsule_digest_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.ledger_seal_summary.as_deref(),
        Some(nested_status.ledger_seal_summary.as_str()),
        "stored choir-receipt copy ledger_seal_summary",
        mismatches,
    );
    compare_option_string(
        stored.ledger_seal_sha256.as_deref(),
        Some(nested_status.ledger_seal_sha256.as_str()),
        "stored choir-receipt copy ledger_seal_sha256",
        mismatches,
    );
    compare_option_string(
        stored.ledger_seal_algorithm.as_deref(),
        Some(nested_status.ledger_seal_algorithm.as_str()),
        "stored choir-receipt copy ledger_seal_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.registry_entry_summary.as_deref(),
        Some(nested_status.registry_entry_summary.as_str()),
        "stored choir-receipt copy registry_entry_summary",
        mismatches,
    );
    compare_option_string(
        stored.registry_entry_sha256.as_deref(),
        Some(nested_status.registry_entry_sha256.as_str()),
        "stored choir-receipt copy registry_entry_sha256",
        mismatches,
    );
    compare_option_string(
        stored.registry_entry_algorithm.as_deref(),
        Some(nested_status.registry_entry_algorithm.as_str()),
        "stored choir-receipt copy registry_entry_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.filing_certificate_summary.as_deref(),
        Some(nested_status.filing_certificate_summary.as_str()),
        "stored choir-receipt copy filing_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.filing_certificate_sha256.as_deref(),
        Some(nested_status.filing_certificate_sha256.as_str()),
        "stored choir-receipt copy filing_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.filing_certificate_algorithm.as_deref(),
        Some(nested_status.filing_certificate_algorithm.as_str()),
        "stored choir-receipt copy filing_certificate_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.archive_receipt_summary.as_deref(),
        Some(nested_status.archive_receipt_summary.as_str()),
        "stored choir-receipt copy archive_receipt_summary",
        mismatches,
    );
    compare_option_string(
        stored.archive_receipt_sha256.as_deref(),
        Some(nested_status.archive_receipt_sha256.as_str()),
        "stored choir-receipt copy archive_receipt_sha256",
        mismatches,
    );
    compare_option_string(
        stored.archive_receipt_algorithm.as_deref(),
        Some(nested_status.archive_receipt_algorithm.as_str()),
        "stored choir-receipt copy archive_receipt_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.closure_certificate_summary.as_deref(),
        Some(nested_status.closure_certificate_summary.as_str()),
        "stored choir-receipt copy closure_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.closure_certificate_sha256.as_deref(),
        Some(nested_status.closure_certificate_sha256.as_str()),
        "stored choir-receipt copy closure_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.closure_certificate_algorithm.as_deref(),
        Some(nested_status.closure_certificate_algorithm.as_str()),
        "stored choir-receipt copy closure_certificate_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.finality_receipt_summary.as_deref(),
        Some(nested_status.finality_receipt_summary.as_str()),
        "stored choir-receipt copy finality_receipt_summary",
        mismatches,
    );
    compare_option_string(
        stored.finality_receipt_sha256.as_deref(),
        Some(nested_status.finality_receipt_sha256.as_str()),
        "stored choir-receipt copy finality_receipt_sha256",
        mismatches,
    );
    compare_option_string(
        stored.finality_receipt_algorithm.as_deref(),
        Some(nested_status.finality_receipt_algorithm.as_str()),
        "stored choir-receipt copy finality_receipt_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.consummation_record_summary.as_deref(),
        Some(nested_status.consummation_record_summary.as_str()),
        "stored choir-receipt copy consummation_record_summary",
        mismatches,
    );
    compare_option_string(
        stored.consummation_record_sha256.as_deref(),
        Some(nested_status.consummation_record_sha256.as_str()),
        "stored choir-receipt copy consummation_record_sha256",
        mismatches,
    );
    compare_option_string(
        stored.consummation_record_algorithm.as_deref(),
        Some(nested_status.consummation_record_algorithm.as_str()),
        "stored choir-receipt copy consummation_record_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.completion_certificate_summary.as_deref(),
        Some(nested_status.completion_certificate_summary.as_str()),
        "stored choir-receipt copy completion_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.completion_certificate_sha256.as_deref(),
        Some(nested_status.completion_certificate_sha256.as_str()),
        "stored choir-receipt copy completion_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.completion_certificate_algorithm.as_deref(),
        Some(nested_status.completion_certificate_algorithm.as_str()),
        "stored choir-receipt copy completion_certificate_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.culmination_receipt_summary.as_deref(),
        Some(nested_status.culmination_receipt_summary.as_str()),
        "stored choir-receipt copy culmination_receipt_summary",
        mismatches,
    );
    compare_option_string(
        stored.culmination_receipt_sha256.as_deref(),
        Some(nested_status.culmination_receipt_sha256.as_str()),
        "stored choir-receipt copy culmination_receipt_sha256",
        mismatches,
    );
    compare_option_string(
        stored.culmination_receipt_algorithm.as_deref(),
        Some(nested_status.culmination_receipt_algorithm.as_str()),
        "stored choir-receipt copy culmination_receipt_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.summit_certificate_summary.as_deref(),
        Some(nested_status.summit_certificate_summary.as_str()),
        "stored choir-receipt copy summit_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.summit_certificate_sha256.as_deref(),
        Some(nested_status.summit_certificate_sha256.as_str()),
        "stored choir-receipt copy summit_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.summit_certificate_algorithm.as_deref(),
        Some(nested_status.summit_certificate_algorithm.as_str()),
        "stored choir-receipt copy summit_certificate_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.pinnacle_receipt_summary.as_deref(),
        Some(nested_status.pinnacle_receipt_summary.as_str()),
        "stored choir-receipt copy pinnacle_receipt_summary",
        mismatches,
    );
    compare_option_string(
        stored.pinnacle_receipt_sha256.as_deref(),
        Some(nested_status.pinnacle_receipt_sha256.as_str()),
        "stored choir-receipt copy pinnacle_receipt_sha256",
        mismatches,
    );
    compare_option_string(
        stored.pinnacle_receipt_algorithm.as_deref(),
        Some(nested_status.pinnacle_receipt_algorithm.as_str()),
        "stored choir-receipt copy pinnacle_receipt_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.keystone_receipt_summary.as_deref(),
        Some(nested_status.keystone_receipt_summary.as_str()),
        "stored choir-receipt copy keystone_receipt_summary",
        mismatches,
    );
    compare_option_string(
        stored.keystone_receipt_sha256.as_deref(),
        Some(nested_status.keystone_receipt_sha256.as_str()),
        "stored choir-receipt copy keystone_receipt_sha256",
        mismatches,
    );
    compare_option_string(
        stored.keystone_receipt_algorithm.as_deref(),
        Some(nested_status.keystone_receipt_algorithm.as_str()),
        "stored choir-receipt copy keystone_receipt_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.cornerstone_certificate_summary.as_deref(),
        Some(nested_status.cornerstone_certificate_summary.as_str()),
        "stored choir-receipt copy cornerstone_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.cornerstone_certificate_sha256.as_deref(),
        Some(nested_status.cornerstone_certificate_sha256.as_str()),
        "stored choir-receipt copy cornerstone_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.cornerstone_certificate_algorithm.as_deref(),
        Some(nested_status.cornerstone_certificate_algorithm.as_str()),
        "stored choir-receipt copy cornerstone_certificate_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.capstone_certificate_summary.as_deref(),
        Some(nested_status.capstone_certificate_summary.as_str()),
        "stored choir-receipt copy capstone_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.capstone_certificate_sha256.as_deref(),
        Some(nested_status.capstone_certificate_sha256.as_str()),
        "stored choir-receipt copy capstone_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.capstone_certificate_algorithm.as_deref(),
        Some(nested_status.capstone_certificate_algorithm.as_str()),
        "stored choir-receipt copy capstone_certificate_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.foundation_receipt_summary.as_deref(),
        Some(nested_status.foundation_receipt_summary.as_str()),
        "stored choir-receipt copy foundation_receipt_summary",
        mismatches,
    );
    compare_option_string(
        stored.foundation_receipt_sha256.as_deref(),
        Some(nested_status.foundation_receipt_sha256.as_str()),
        "stored choir-receipt copy foundation_receipt_sha256",
        mismatches,
    );
    compare_option_string(
        stored.foundation_receipt_algorithm.as_deref(),
        Some(nested_status.foundation_receipt_algorithm.as_str()),
        "stored choir-receipt copy foundation_receipt_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.bedrock_certificate_summary.as_deref(),
        Some(nested_status.bedrock_certificate_summary.as_str()),
        "stored choir-receipt copy bedrock_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.bedrock_certificate_sha256.as_deref(),
        Some(nested_status.bedrock_certificate_sha256.as_str()),
        "stored choir-receipt copy bedrock_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.bedrock_certificate_algorithm.as_deref(),
        Some(nested_status.bedrock_certificate_algorithm.as_str()),
        "stored choir-receipt copy bedrock_certificate_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.basal_receipt_summary.as_deref(),
        Some(nested_status.basal_receipt_summary.as_str()),
        "stored choir-receipt copy basal_receipt_summary",
        mismatches,
    );
    compare_option_string(
        stored.basal_receipt_sha256.as_deref(),
        Some(nested_status.basal_receipt_sha256.as_str()),
        "stored choir-receipt copy basal_receipt_sha256",
        mismatches,
    );
    compare_option_string(
        stored.basal_receipt_algorithm.as_deref(),
        Some(nested_status.basal_receipt_algorithm.as_str()),
        "stored choir-receipt copy basal_receipt_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.substructure_certificate_summary.as_deref(),
        Some(nested_status.substructure_certificate_summary.as_str()),
        "stored choir-receipt copy substructure_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.substructure_certificate_sha256.as_deref(),
        Some(nested_status.substructure_certificate_sha256.as_str()),
        "stored choir-receipt copy substructure_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.substructure_certificate_algorithm.as_deref(),
        Some(nested_status.substructure_certificate_algorithm.as_str()),
        "stored choir-receipt copy substructure_certificate_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.plinth_receipt_summary.as_deref(),
        Some(nested_status.plinth_receipt_summary.as_str()),
        "stored choir-receipt copy plinth_receipt_summary",
        mismatches,
    );
    compare_option_string(
        stored.plinth_receipt_sha256.as_deref(),
        Some(nested_status.plinth_receipt_sha256.as_str()),
        "stored choir-receipt copy plinth_receipt_sha256",
        mismatches,
    );
    compare_option_string(
        stored.plinth_receipt_algorithm.as_deref(),
        Some(nested_status.plinth_receipt_algorithm.as_str()),
        "stored choir-receipt copy plinth_receipt_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.pedestal_certificate_summary.as_deref(),
        Some(nested_status.pedestal_certificate_summary.as_str()),
        "stored choir-receipt copy pedestal_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.pedestal_certificate_sha256.as_deref(),
        Some(nested_status.pedestal_certificate_sha256.as_str()),
        "stored choir-receipt copy pedestal_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.pedestal_certificate_algorithm.as_deref(),
        Some(nested_status.pedestal_certificate_algorithm.as_str()),
        "stored choir-receipt copy pedestal_certificate_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.transept_certificate_summary.as_deref(),
        Some(nested_status.transept_certificate_summary.as_str()),
        "stored choir-receipt copy transept_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.transept_certificate_sha256.as_deref(),
        Some(nested_status.transept_certificate_sha256.as_str()),
        "stored choir-receipt copy transept_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.transept_certificate_algorithm.as_deref(),
        Some(nested_status.transept_certificate_algorithm.as_str()),
        "stored choir-receipt copy transept_certificate_algorithm",
        mismatches,
    );
    compare_string(
        &stored.reason,
        &nested_status.reason,
        "stored choir-receipt copy reason",
        mismatches,
    );
    compare_string(
        &stored.explicit_statement,
        &nested_status.explicit_statement,
        "stored choir-receipt copy explicit_statement",
        mismatches,
    );
}

fn compare_session_against_snapshot(
    session: &PackageChoirReceiptLatestSession,
    root: &Path,
    session_dir: &Path,
    paths: &PackageChoirReceiptLatestPaths,
    snapshot: &LatestTranseptCertificateSnapshot,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &session.resolve_latest_transept_certificate_command_summary,
        &resolve_latest_transept_certificate_command_summary(
            root,
            &paths.nested_transept_certificate_latest_session_dir,
        ),
        "stored session resolve_latest_transept_certificate_command_summary",
        mismatches,
    );
    compare_string(
        &session.run_latest_choir_receipt_command_summary,
        &run_latest_choir_receipt_command_summary(root, session_dir),
        "stored session run_latest_choir_receipt_command_summary",
        mismatches,
    );
    compare_string(
        &session.verify_latest_choir_receipt_command_summary,
        &verify_latest_choir_receipt_command_summary(session_dir),
        "stored session verify_latest_choir_receipt_command_summary",
        mismatches,
    );
    compare_string(
        &session.downstream_choir_receipt_command_summary,
        &downstream_choir_receipt_command_summary(
            snapshot,
            &expected_downstream_transept_certificate_session_dir(paths),
            &paths.nested_choir_receipt_session_dir,
        ),
        "stored session downstream_choir_receipt_command_summary",
        mismatches,
    );
    compare_string(
        &session.verify_choir_receipt_command_summary,
        &verify_choir_receipt_command_summary(
            snapshot,
            &expected_downstream_transept_certificate_session_dir(paths),
            &paths.nested_choir_receipt_session_dir,
        ),
        "stored session verify_choir_receipt_command_summary",
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
        &session.nested_transept_certificate_latest_session_dir,
        &paths
            .nested_transept_certificate_latest_session_dir
            .display()
            .to_string(),
        "stored session nested_transept_certificate_latest_session_dir",
        mismatches,
    );
    compare_string(
        &session.nested_choir_receipt_session_dir,
        &paths.nested_choir_receipt_session_dir.display().to_string(),
        "stored session nested_choir_receipt_session_dir",
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
    report: &PackageChoirReceiptLatestReport,
    root: &Path,
    session_dir: &Path,
    paths: &PackageChoirReceiptLatestPaths,
    snapshot: &LatestTranseptCertificateSnapshot,
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
            .resolve_latest_transept_certificate_command_summary
            .as_deref(),
        Some(
            resolve_latest_transept_certificate_command_summary(
                root,
                &paths.nested_transept_certificate_latest_session_dir,
            )
            .as_str(),
        ),
        "stored report resolve_latest_transept_certificate_command_summary",
        mismatches,
    );
    compare_option_string(
        report.run_latest_choir_receipt_command_summary.as_deref(),
        Some(run_latest_choir_receipt_command_summary(root, session_dir).as_str()),
        "stored report run_latest_choir_receipt_command_summary",
        mismatches,
    );
    compare_option_string(
        report
            .verify_latest_choir_receipt_command_summary
            .as_deref(),
        Some(verify_latest_choir_receipt_command_summary(session_dir).as_str()),
        "stored report verify_latest_choir_receipt_command_summary",
        mismatches,
    );
    compare_option_string(
        report.downstream_choir_receipt_command_summary.as_deref(),
        Some(
            downstream_choir_receipt_command_summary(
                snapshot,
                &expected_downstream_transept_certificate_session_dir(paths),
                &paths.nested_choir_receipt_session_dir,
            )
            .as_str(),
        ),
        "stored report downstream_choir_receipt_command_summary",
        mismatches,
    );
    compare_option_string(
        report.verify_choir_receipt_command_summary.as_deref(),
        Some(
            verify_choir_receipt_command_summary(
                snapshot,
                &expected_downstream_transept_certificate_session_dir(paths),
                &paths.nested_choir_receipt_session_dir,
            )
            .as_str(),
        ),
        "stored report verify_choir_receipt_command_summary",
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
        report.choir_receipt_latest_session_path.as_deref(),
        Some(paths.session_path.display().to_string().as_str()),
        "stored report choir_receipt_latest_session_path",
        mismatches,
    );
    compare_option_string(
        report.choir_receipt_latest_status_path.as_deref(),
        Some(paths.status_path.display().to_string().as_str()),
        "stored report choir_receipt_latest_status_path",
        mismatches,
    );
    compare_option_string(
        report.choir_receipt_latest_report_path.as_deref(),
        Some(paths.report_path.display().to_string().as_str()),
        "stored report choir_receipt_latest_report_path",
        mismatches,
    );
    compare_option_string(
        report
            .latest_transept_certificate_plan_report_path
            .as_deref(),
        Some(
            paths
                .latest_transept_certificate_plan_report_path
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report latest_transept_certificate_plan_report_path",
        mismatches,
    );
    compare_option_string(
        report.latest_transept_certificate_report_path.as_deref(),
        Some(
            paths
                .latest_transept_certificate_report_path
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report latest_transept_certificate_report_path",
        mismatches,
    );
    compare_option_string(
        report.choir_receipt_report_path.as_deref(),
        Some(
            paths
                .choir_receipt_report_path
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report choir_receipt_report_path",
        mismatches,
    );
    compare_option_string(
        report
            .nested_transept_certificate_latest_session_dir
            .as_deref(),
        Some(
            paths
                .nested_transept_certificate_latest_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report nested_transept_certificate_latest_session_dir",
        mismatches,
    );
    compare_option_string(
        report.nested_choir_receipt_session_dir.as_deref(),
        Some(
            paths
                .nested_choir_receipt_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report nested_choir_receipt_session_dir",
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
    session: &PackageChoirReceiptLatestSession,
    root: &Path,
    session_dir: &Path,
    paths: &PackageChoirReceiptLatestPaths,
    failure: &LatestTranseptCertificateResolutionFailure,
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
        &session.nested_transept_certificate_latest_session_dir,
        &paths
            .nested_transept_certificate_latest_session_dir
            .display()
            .to_string(),
        "stored session nested_transept_certificate_latest_session_dir on failure",
        mismatches,
    );
    compare_string(
        &session.nested_choir_receipt_session_dir,
        &paths.nested_choir_receipt_session_dir.display().to_string(),
        "stored session nested_choir_receipt_session_dir on failure",
        mismatches,
    );
    if let Some(snapshot) = failure.snapshot.as_ref() {
        compare_session_against_snapshot(session, root, session_dir, paths, snapshot, mismatches);
    }
}

fn compare_report_against_failure(
    report: &PackageChoirReceiptLatestReport,
    root: &Path,
    session_dir: &Path,
    paths: &PackageChoirReceiptLatestPaths,
    failure: &LatestTranseptCertificateResolutionFailure,
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
        "run_latest_choir_receipt",
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

fn map_transept_certificate_latest_failure_kind(verdict: &str) -> ResolutionFailureKind {
    match verdict {
        "tiny_live_package_transept_certificate_latest_refused_by_missing_latest_chain" => {
            ResolutionFailureKind::MissingLatestChain
        }
        "tiny_live_package_transept_certificate_latest_refused_by_drifted_transept_certificate_contract" => {
            ResolutionFailureKind::DriftedTranseptCertificateContract
        }
        _ => ResolutionFailureKind::InvalidLatestChain,
    }
}

fn latest_transept_certificate_terminal_failure(
    report: &TranseptCertificateLatestReportView,
) -> Option<LivePackageChoirReceiptLatestResult> {
    match report.result.as_deref() {
        Some("refused_by_missing_latest_chain") => {
            Some(LivePackageChoirReceiptLatestResult::RefusedByMissingLatestChain)
        }
        Some("refused_by_invalid_latest_chain") => {
            Some(LivePackageChoirReceiptLatestResult::RefusedByInvalidLatestChain)
        }
        Some("refused_by_drifted_transept_certificate_contract") => {
            Some(LivePackageChoirReceiptLatestResult::RefusedByDriftedChoirReceiptContract)
        }
        _ => None,
    }
}

fn transept_certificate_latest_result_from_nested_status(
    result: &str,
) -> Option<LivePackageChoirReceiptLatestResult> {
    match result {
        "ready_for_manual_execution_when_gate_turns_green" => {
            Some(LivePackageChoirReceiptLatestResult::ReadyForManualExecutionWhenGateTurnsGreen)
        }
        "refused_now_by_stage3" => Some(LivePackageChoirReceiptLatestResult::RefusedNowByStage3),
        "refused_now_by_pre_activation_gate" => {
            Some(LivePackageChoirReceiptLatestResult::RefusedNowByPreActivationGate)
        }
        "refused_now_by_invalid_or_drifted_contract" => {
            Some(LivePackageChoirReceiptLatestResult::RefusedByDriftedChoirReceiptContract)
        }
        _ => None,
    }
}

fn choir_receipt_run_verdict_for_result(result: &str) -> Option<&'static str> {
    match result {
        "ready_for_manual_execution_when_gate_turns_green" => {
            Some("tiny_live_package_choir_receipt_ready_for_manual_execution_when_gate_turns_green")
        }
        "refused_now_by_stage3" => Some("tiny_live_package_choir_receipt_refused_now_by_stage3"),
        "refused_now_by_pre_activation_gate" => {
            Some("tiny_live_package_choir_receipt_refused_now_by_pre_activation_gate")
        }
        "refused_now_by_invalid_or_drifted_contract" => {
            Some("tiny_live_package_choir_receipt_refused_now_by_invalid_or_drifted_contract")
        }
        _ => None,
    }
}

fn result_from_failure_kind(kind: ResolutionFailureKind) -> LivePackageChoirReceiptLatestResult {
    match kind {
        ResolutionFailureKind::MissingLatestChain => {
            LivePackageChoirReceiptLatestResult::RefusedByMissingLatestChain
        }
        ResolutionFailureKind::InvalidLatestChain => {
            LivePackageChoirReceiptLatestResult::RefusedByInvalidLatestChain
        }
        ResolutionFailureKind::DriftedTranseptCertificateContract => {
            LivePackageChoirReceiptLatestResult::RefusedByDriftedChoirReceiptContract
        }
    }
}

fn verdict_for_result(
    result: LivePackageChoirReceiptLatestResult,
) -> TinyLivePackageChoirReceiptLatestVerdict {
    match result {
        LivePackageChoirReceiptLatestResult::RefusedByMissingLatestChain => {
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestRefusedByMissingLatestChain
        }
        LivePackageChoirReceiptLatestResult::RefusedByInvalidLatestChain => {
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestRefusedByInvalidLatestChain
        }
        LivePackageChoirReceiptLatestResult::RefusedByDriftedChoirReceiptContract => {
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestRefusedByDriftedChoirReceiptContract
        }
        LivePackageChoirReceiptLatestResult::RefusedNowByStage3 => {
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestRefusedNowByStage3
        }
        LivePackageChoirReceiptLatestResult::RefusedNowByPreActivationGate => {
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestRefusedNowByPreActivationGate
        }
        LivePackageChoirReceiptLatestResult::ReadyForManualExecutionWhenGateTurnsGreen => {
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestReadyForManualExecutionWhenGateTurnsGreen
        }
    }
}

fn verdict_from_failure_kind(
    kind: ResolutionFailureKind,
) -> TinyLivePackageChoirReceiptLatestVerdict {
    verdict_for_result(result_from_failure_kind(kind))
}

fn operator_next_action_summary(result: LivePackageChoirReceiptLatestResult) -> String {
    match result {
        LivePackageChoirReceiptLatestResult::RefusedByMissingLatestChain => {
            "no immutable clerestory chain is available yet; do not manually hunt latest-transept-certificate or choir-receipt lineage by hand".to_string()
        }
        LivePackageChoirReceiptLatestResult::RefusedByInvalidLatestChain => {
            "the latest immutable chain is incomplete or below the accepted clerestory layer; mint a fresh coherent clerestory chain instead of bypassing the accepted latest-transept-certificate / choir-receipt path".to_string()
        }
        LivePackageChoirReceiptLatestResult::RefusedByDriftedChoirReceiptContract => {
            "the latest chain no longer resolves cleanly into the accepted latest-transept-certificate / choir-receipt contract; repair that drift instead of bypassing the bounded handoff path".to_string()
        }
        LivePackageChoirReceiptLatestResult::RefusedNowByStage3 => {
            "the frozen controller stays refused now by Stage 3; rerun this latest-choir-receipt refresh after current Stage 3 truth turns green".to_string()
        }
        LivePackageChoirReceiptLatestResult::RefusedNowByPreActivationGate => {
            "the frozen controller stays refused now by the pre-activation gate; rerun this latest-choir-receipt refresh after the current pre-activation gate turns green".to_string()
        }
        LivePackageChoirReceiptLatestResult::ReadyForManualExecutionWhenGateTurnsGreen => {
            "the latest immutable chain now resolves into the accepted choir-receipt contract and one bounded latest-choir-receipt handoff is ready for manual execution when gate truth turns green; this wrapper still did not run or authorize the frozen controller".to_string()
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

fn verify_choir_receipt_input_command_summary(transept_certificate_session_dir: &Path) -> String {
    format!(
        "verify immutable transept-certificate session under {} before freezing the final choir receipt",
        transept_certificate_session_dir.display()
    )
}

fn refusal_status(
    root: &Path,
    session_dir: &Path,
    paths: &PackageChoirReceiptLatestPaths,
    resolved: Option<&ResolvedLatestTranseptCertificate>,
    result: LivePackageChoirReceiptLatestResult,
    reason: String,
    latest_transept_certificate_plan_step: Option<PackageChoirReceiptLatestStepArtifact>,
    latest_transept_certificate_step: Option<PackageChoirReceiptLatestStepArtifact>,
    choir_receipt_step: Option<PackageChoirReceiptLatestStepArtifact>,
) -> PackageChoirReceiptLatestStatus {
    let snapshot = resolved.map(|value| &value.snapshot);
    PackageChoirReceiptLatestStatus {
        status_version: CHOIR_RECEIPT_LATEST_STATUS_VERSION.to_string(),
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
        nested_transept_certificate_latest_session_dir: paths
            .nested_transept_certificate_latest_session_dir
            .display()
            .to_string(),
        nested_choir_receipt_session_dir: paths
            .nested_choir_receipt_session_dir
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
        latest_transept_certificate_plan_verdict: resolved
            .map(|value| value.latest_transept_certificate_plan.verdict.clone()),
        latest_transept_certificate_verdict: latest_transept_certificate_step
            .as_ref()
            .map(|value| value.verdict.clone()),
        choir_receipt_verdict: choir_receipt_step
            .as_ref()
            .map(|value| value.verdict.clone()),
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        latest_transept_certificate_plan_step,
        latest_transept_certificate_step,
        choir_receipt_step,
        operator_next_action_summary: operator_next_action_summary(result),
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn refusal_status_from_resolution_failure(
    root: &Path,
    session_dir: &Path,
    paths: &PackageChoirReceiptLatestPaths,
    failure: &LatestTranseptCertificateResolutionFailure,
) -> PackageChoirReceiptLatestStatus {
    PackageChoirReceiptLatestStatus {
        status_version: CHOIR_RECEIPT_LATEST_STATUS_VERSION.to_string(),
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
        nested_transept_certificate_latest_session_dir: paths
            .nested_transept_certificate_latest_session_dir
            .display()
            .to_string(),
        nested_choir_receipt_session_dir: paths
            .nested_choir_receipt_session_dir
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
        latest_transept_certificate_plan_verdict: failure
            .latest_transept_certificate_plan_verdict
            .clone(),
        latest_transept_certificate_verdict: None,
        choir_receipt_verdict: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        latest_transept_certificate_plan_step: failure
            .latest_transept_certificate_plan_raw
            .as_ref()
            .map(|_| {
                step_artifact(
                    &paths.latest_transept_certificate_plan_report_path,
                    "plan_latest_transept_certificate",
                    failure
                        .latest_transept_certificate_plan_verdict
                        .as_deref()
                        .unwrap_or("<missing>"),
                    failure
                        .latest_transept_certificate_plan_reason
                        .as_deref()
                        .unwrap_or("<missing>"),
                    failure
                        .latest_transept_certificate_plan_generated_at
                        .unwrap_or_else(Utc::now),
                )
            }),
        latest_transept_certificate_step: None,
        choir_receipt_step: None,
        operator_next_action_summary: operator_next_action_summary(result_from_failure_kind(
            failure.kind,
        )),
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn verify_invalid_choir_receipt_latest_report_without_session(
    session_dir: &Path,
    verification_mismatches: Vec<String>,
) -> PackageChoirReceiptLatestReport {
    let reason = verification_mismatches
        .first()
        .cloned()
        .unwrap_or_else(|| "latest-choir-receipt verification failed".to_string());
    PackageChoirReceiptLatestReport {
        generated_at: Utc::now(),
        mode: "verify_latest_choir_receipt".to_string(),
        verdict: TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid,
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
        choir_receipt_latest_session_path: Some(
            choir_receipt_latest_paths(session_dir)
                .session_path
                .display()
                .to_string(),
        ),
        choir_receipt_latest_status_path: Some(
            choir_receipt_latest_paths(session_dir)
                .status_path
                .display()
                .to_string(),
        ),
        choir_receipt_latest_report_path: Some(
            choir_receipt_latest_paths(session_dir)
                .report_path
                .display()
                .to_string(),
        ),
        latest_transept_certificate_plan_report_path: None,
        latest_transept_certificate_report_path: None,
        choir_receipt_report_path: None,
        nested_transept_certificate_latest_session_dir: None,
        nested_choir_receipt_session_dir: None,
        downstream_decision_packet_session_dir: None,
        latest_transept_certificate_plan_verdict: None,
        latest_transept_certificate_verdict: None,
        choir_receipt_verdict: None,
        result: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        resolve_latest_transept_certificate_command_summary: None,
        run_latest_choir_receipt_command_summary: None,
        verify_latest_choir_receipt_command_summary: None,
        downstream_choir_receipt_command_summary: None,
        verify_choir_receipt_command_summary: None,
        reviewed_frozen_live_cutover_controller_command_summary: None,
        clerestory_certificate_summary: None,
        clerestory_certificate_sha256: None,
        clerestory_certificate_algorithm: None,
        verification_mismatches,
        activation_authorized: false,
        explicit_statement:
            "this verification proves only whether the latest immutable chain still resolves cleanly into the accepted choir-receipt contract; it never runs the frozen controller"
                .to_string(),
    }
}

fn fallback_transept_certificate_latest_copy() -> TranseptCertificateLatestReportView {
    TranseptCertificateLatestReportView {
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

fn fallback_choir_receipt_session(
    snapshot: &LatestTranseptCertificateSnapshot,
    session_dir: &Path,
) -> ChoirReceiptSessionView {
    ChoirReceiptSessionView {
        transept_certificate_session_dir: "<missing>".to_string(),
        pedestal_certificate_session_dir: "<missing>".to_string(),
        plinth_receipt_session_dir: "<missing>".to_string(),
        substructure_certificate_session_dir: "<missing>".to_string(),
        basal_receipt_session_dir: "<missing>".to_string(),
        bedrock_certificate_session_dir: "<missing>".to_string(),
        foundation_receipt_session_dir: "<missing>".to_string(),
        registry_entry_session_dir: "<missing>".to_string(),
        notarization_receipt_session_dir: "<missing>".to_string(),
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
        verify_transept_certificate_command_summary:
            verify_choir_receipt_input_command_summary(Path::new("<missing>")),
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
        filing_certificate_summary:
            "Filing certificate outcome: fallback session is synthetic and must not verify green."
                .to_string(),
        filing_certificate_sha256: "<missing>".to_string(),
        filing_certificate_algorithm: "sha256".to_string(),
        archive_receipt_summary:
            "Archive receipt outcome: fallback session is synthetic and must not verify green."
                .to_string(),
        archive_receipt_sha256: "<missing>".to_string(),
        archive_receipt_algorithm: "sha256".to_string(),
        closure_certificate_summary:
            "Closure certificate outcome: fallback session is synthetic and must not verify green."
                .to_string(),
        closure_certificate_sha256: "<missing>".to_string(),
        closure_certificate_algorithm: "sha256".to_string(),
        finality_receipt_summary:
            "Finality receipt outcome: fallback session is synthetic and must not verify green."
                .to_string(),
        finality_receipt_sha256: "<missing>".to_string(),
        finality_receipt_algorithm: "sha256".to_string(),
        consummation_record_summary:
            "Consummation record outcome: fallback session is synthetic and must not verify green."
                .to_string(),
        consummation_record_sha256: "<missing>".to_string(),
        consummation_record_algorithm: "sha256".to_string(),
        completion_certificate_summary:
            "Completion certificate outcome: fallback session is synthetic and must not verify green."
                .to_string(),
        completion_certificate_sha256: "<missing>".to_string(),
        completion_certificate_algorithm: "sha256".to_string(),
        culmination_receipt_summary:
            "Culmination receipt outcome: fallback session is synthetic and must not verify green."
                .to_string(),
        culmination_receipt_sha256: "<missing>".to_string(),
        culmination_receipt_algorithm: "sha256".to_string(),
        summit_certificate_summary:
            "Summit certificate outcome: fallback session is synthetic and must not verify green."
                .to_string(),
        summit_certificate_sha256: "<missing>".to_string(),
        summit_certificate_algorithm: "sha256".to_string(),
        pinnacle_receipt_summary:
            "Pinnacle receipt outcome: fallback session is synthetic and must not verify green."
                .to_string(),
        pinnacle_receipt_sha256: "<missing>".to_string(),
        pinnacle_receipt_algorithm: "sha256".to_string(),
        capstone_certificate_summary:
            "Capstone certificate outcome: fallback session is synthetic and must not verify green."
                .to_string(),
        capstone_certificate_sha256: "<missing>".to_string(),
        capstone_certificate_algorithm: "sha256".to_string(),
        keystone_receipt_summary:
            "Keystone receipt outcome: fallback session is synthetic and must not verify green."
                .to_string(),
        keystone_receipt_sha256: "<missing>".to_string(),
        keystone_receipt_algorithm: "sha256".to_string(),
        cornerstone_certificate_summary:
            "Cornerstone certificate identity: fallback session is synthetic and must not verify green."
                .to_string(),
        cornerstone_certificate_sha256: "<missing>".to_string(),
        cornerstone_certificate_algorithm: "sha256".to_string(),
        foundation_receipt_summary:
            "Foundation receipt identity: fallback session is synthetic and must not verify green."
                .to_string(),
        foundation_receipt_sha256: "<missing>".to_string(),
        foundation_receipt_algorithm: "sha256".to_string(),
        bedrock_certificate_summary:
            "Bedrock certificate identity: fallback session is synthetic and must not verify green."
                .to_string(),
        bedrock_certificate_sha256: "<missing>".to_string(),
        bedrock_certificate_algorithm: "sha256".to_string(),
        basal_receipt_summary:
            "Basal receipt identity: fallback session is synthetic and must not verify green."
                .to_string(),
        basal_receipt_sha256: "<missing>".to_string(),
        basal_receipt_algorithm: "sha256".to_string(),
        substructure_certificate_summary:
            "Substructure certificate identity: fallback session is synthetic and must not verify green."
                .to_string(),
        substructure_certificate_sha256: "<missing>".to_string(),
        substructure_certificate_algorithm: "sha256".to_string(),
        plinth_receipt_summary:
            "Plinth receipt identity: fallback session is synthetic and must not verify green."
                .to_string(),
        plinth_receipt_sha256: "<missing>".to_string(),
        plinth_receipt_algorithm: "sha256".to_string(),
        pedestal_certificate_summary:
            "Pedestal certificate identity: fallback session is synthetic and must not verify green."
                .to_string(),
        pedestal_certificate_sha256: "<missing>".to_string(),
        pedestal_certificate_algorithm: "sha256".to_string(),
        transept_certificate_summary:
            "Transept certificate identity: fallback session is synthetic and must not verify green."
                .to_string(),
        transept_certificate_sha256: "<missing>".to_string(),
        transept_certificate_algorithm: "sha256".to_string(),
        choir_receipt_summary:
            "Choir receipt identity: fallback session is synthetic and must not verify green."
                .to_string(),
        choir_receipt_sha256: "<missing>".to_string(),
        choir_receipt_algorithm: "sha256".to_string(),
        explicit_statement:
            "the verified transept-certificate session is the primary input here; this immutable choir receipt freezes one final ensign-seal identity over the fully culminated chain without enabling production execution"
                .to_string(),
    }
}

fn fallback_choir_receipt_status(
    _snapshot: &LatestTranseptCertificateSnapshot,
    session_dir: &Path,
) -> ChoirReceiptStatusView {
    ChoirReceiptStatusView {
        session_dir: session_dir.display().to_string(),
        transept_certificate_session_dir: "<missing>".to_string(),
        pedestal_certificate_session_dir: "<missing>".to_string(),
        plinth_receipt_session_dir: "<missing>".to_string(),
        substructure_certificate_session_dir: "<missing>".to_string(),
        basal_receipt_session_dir: "<missing>".to_string(),
        bedrock_certificate_session_dir: "<missing>".to_string(),
        foundation_receipt_session_dir: "<missing>".to_string(),
        result: "refused_now_by_invalid_or_drifted_contract".to_string(),
        reason: "<missing>".to_string(),
        handoff_bundle_result: None,
        decision_packet_result: None,
        execute_frozen_result: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        transept_certificate_step: None,
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
        filing_certificate_summary:
            "Filing certificate outcome: fallback status is synthetic and must not verify green."
                .to_string(),
        filing_certificate_sha256: "<missing>".to_string(),
        filing_certificate_algorithm: "sha256".to_string(),
        archive_receipt_summary:
            "Archive receipt outcome: fallback status is synthetic and must not verify green."
                .to_string(),
        archive_receipt_sha256: "<missing>".to_string(),
        archive_receipt_algorithm: "sha256".to_string(),
        closure_certificate_summary:
            "Closure certificate outcome: fallback status is synthetic and must not verify green."
                .to_string(),
        closure_certificate_sha256: "<missing>".to_string(),
        closure_certificate_algorithm: "sha256".to_string(),
        finality_receipt_summary:
            "Finality receipt outcome: fallback status is synthetic and must not verify green."
                .to_string(),
        finality_receipt_sha256: "<missing>".to_string(),
        finality_receipt_algorithm: "sha256".to_string(),
        consummation_record_summary:
            "Consummation record outcome: fallback status is synthetic and must not verify green."
                .to_string(),
        consummation_record_sha256: "<missing>".to_string(),
        consummation_record_algorithm: "sha256".to_string(),
        completion_certificate_summary:
            "Completion certificate outcome: fallback status is synthetic and must not verify green."
                .to_string(),
        completion_certificate_sha256: "<missing>".to_string(),
        completion_certificate_algorithm: "sha256".to_string(),
        culmination_receipt_summary:
            "Culmination receipt outcome: fallback status is synthetic and must not verify green."
                .to_string(),
        culmination_receipt_sha256: "<missing>".to_string(),
        culmination_receipt_algorithm: "sha256".to_string(),
        summit_certificate_summary:
            "Summit certificate outcome: fallback status is synthetic and must not verify green."
                .to_string(),
        summit_certificate_sha256: "<missing>".to_string(),
        summit_certificate_algorithm: "sha256".to_string(),
        pinnacle_receipt_summary:
            "Pinnacle receipt outcome: fallback status is synthetic and must not verify green."
                .to_string(),
        pinnacle_receipt_sha256: "<missing>".to_string(),
        pinnacle_receipt_algorithm: "sha256".to_string(),
        capstone_certificate_summary:
            "Capstone certificate outcome: fallback status is synthetic and must not verify green."
                .to_string(),
        capstone_certificate_sha256: "<missing>".to_string(),
        capstone_certificate_algorithm: "sha256".to_string(),
        keystone_receipt_summary:
            "Keystone receipt outcome: fallback status is synthetic and must not verify green."
                .to_string(),
        keystone_receipt_sha256: "<missing>".to_string(),
        keystone_receipt_algorithm: "sha256".to_string(),
        cornerstone_certificate_summary:
            "Cornerstone certificate identity: fallback status is synthetic and must not verify green."
                .to_string(),
        cornerstone_certificate_sha256: "<missing>".to_string(),
        cornerstone_certificate_algorithm: "sha256".to_string(),
        foundation_receipt_summary:
            "Foundation receipt identity: fallback status is synthetic and must not verify green."
                .to_string(),
        foundation_receipt_sha256: "<missing>".to_string(),
        foundation_receipt_algorithm: "sha256".to_string(),
        bedrock_certificate_summary:
            "Bedrock certificate identity: fallback status is synthetic and must not verify green."
                .to_string(),
        bedrock_certificate_sha256: "<missing>".to_string(),
        bedrock_certificate_algorithm: "sha256".to_string(),
        basal_receipt_summary:
            "Basal receipt identity: fallback status is synthetic and must not verify green."
                .to_string(),
        basal_receipt_sha256: "<missing>".to_string(),
        basal_receipt_algorithm: "sha256".to_string(),
        substructure_certificate_summary:
            "Substructure certificate identity: fallback status is synthetic and must not verify green."
                .to_string(),
        substructure_certificate_sha256: "<missing>".to_string(),
        substructure_certificate_algorithm: "sha256".to_string(),
        plinth_receipt_summary:
            "Plinth receipt identity: fallback status is synthetic and must not verify green."
                .to_string(),
        plinth_receipt_sha256: "<missing>".to_string(),
        plinth_receipt_algorithm: "sha256".to_string(),
        pedestal_certificate_summary:
            "Pedestal certificate identity: fallback status is synthetic and must not verify green."
                .to_string(),
        pedestal_certificate_sha256: "<missing>".to_string(),
        pedestal_certificate_algorithm: "sha256".to_string(),
        transept_certificate_summary:
            "Transept certificate identity: fallback status is synthetic and must not verify green."
                .to_string(),
        transept_certificate_sha256: "<missing>".to_string(),
        transept_certificate_algorithm: "sha256".to_string(),
        choir_receipt_summary:
            "Choir receipt identity: fallback status is synthetic and must not verify green."
                .to_string(),
        choir_receipt_sha256: "<missing>".to_string(),
        choir_receipt_algorithm: "sha256".to_string(),
        explicit_statement:
            "this choir receipt is archival and read-only; it never executes or enables the frozen controller"
                .to_string(),
    }
}

fn render_report_lines(report: &PackageChoirReceiptLatestReport) -> String {
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
    if let Some(value) = &report.nested_transept_certificate_latest_session_dir {
        lines.push(format!(
            "nested_transept_certificate_latest_session_dir={value}"
        ));
    }
    if let Some(value) = &report.downstream_decision_packet_session_dir {
        lines.push(format!("downstream_decision_packet_session_dir={value}"));
    }
    if let Some(value) = &report.nested_choir_receipt_session_dir {
        lines.push(format!("nested_choir_receipt_session_dir={value}"));
    }
    if let Some(value) = &report.result {
        lines.push(format!("result={value}"));
    }
    if let Some(value) = &report.downstream_choir_receipt_command_summary {
        lines.push(format!("downstream_choir_receipt_command_summary={value}"));
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

    struct FakeTranseptCertificateLatestFixture {
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
        transept_certificate_latest_plan_json_path: PathBuf,
        transept_certificate_latest_run_json_path: PathBuf,
        transept_certificate_latest_verify_json_path: PathBuf,
        handoff_bundle_run_json_path: PathBuf,
        handoff_bundle_verify_json_path: PathBuf,
        handoff_bundle_session_json_path: PathBuf,
        handoff_bundle_status_json_path: PathBuf,
        frozen_controller_command: String,
    }

    impl FakeTranseptCertificateLatestFixture {
        fn plan_config(&self) -> Config {
            Config {
                mode: Mode::PlanLatestChoirReceipt,
                root: Some(self.root.clone()),
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn render_config(&self) -> Config {
            Config {
                mode: Mode::RenderLatestChoirReceiptScript,
                root: Some(self.root.clone()),
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: Some(self.output_script_path.clone()),
                json: true,
            }
        }

        fn run_config(&self) -> Config {
            Config {
                mode: Mode::RunLatestChoirReceipt,
                root: Some(self.root.clone()),
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn verify_config(&self) -> Config {
            Config {
                mode: Mode::VerifyLatestChoirReceipt,
                root: None,
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }
    }

    #[test]
    fn plan_mode_resolves_latest_valid_clerestory_chain_to_exact_expected_nested_latest_transept_certificate_lineage(
    ) {
        let fixture = fake_transept_certificate_latest_fixture(
            "transept_certificate_latest_plan_ok",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let report = plan_latest_choir_receipt_report(&fixture.plan_config()).unwrap();
        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        assert_eq!(
            report.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestPlanReady
        );
        assert_eq!(report.mode, "plan_latest_choir_receipt");
        assert_eq!(
            report.latest_top_step_name.as_deref(),
            Some("clerestory_certificate")
        );
        assert_eq!(
            report
                .nested_transept_certificate_latest_session_dir
                .as_deref(),
            Some(
                paths
                    .nested_transept_certificate_latest_session_dir
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
            .downstream_choir_receipt_command_summary
            .as_deref()
            .unwrap()
            .contains(CHOIR_RECEIPT_BIN));
    }

    #[test]
    fn plan_mode_without_session_dir_uses_choir_latest_default_wrapper_session_dir() {
        let fixture = fake_transept_certificate_latest_fixture(
            "choir_receipt_latest_plan_default_session_dir",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let default_wrapper_session_dir = fixture
            .root
            .join("tiny_live_activation_package_choir_receipt_latest.session");
        let stale_transept_wrapper_session_dir = fixture
            .root
            .join("tiny_live_activation_package_transept_certificate_latest.session");
        let default_paths = choir_receipt_latest_paths(&default_wrapper_session_dir);
        let mut plan_json = default_transept_certificate_latest_plan_report(&fixture);
        plan_json["session_dir"] = json!(default_paths
            .nested_transept_certificate_latest_session_dir
            .display()
            .to_string());
        plan_json["downstream_decision_packet_session_dir"] = json!(
            expected_downstream_decision_packet_session_dir(&default_paths)
                .display()
                .to_string()
        );
        persist_json(
            &fixture.transept_certificate_latest_plan_json_path,
            &plan_json,
        )
        .unwrap();
        write_fake_transept_certificate_latest_command(
            &fixture.bin_dir.join(TRANSEPT_CERTIFICATE_LATEST_BIN),
            &fixture.transept_certificate_latest_plan_json_path,
            &fixture.transept_certificate_latest_run_json_path,
            &fixture.transept_certificate_latest_verify_json_path,
            &fixture.root,
            &default_paths.nested_transept_certificate_latest_session_dir,
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let config = Config {
            mode: Mode::PlanLatestChoirReceipt,
            root: Some(fixture.root.clone()),
            session_dir: None,
            output_path: None,
            json: true,
        };
        let report = plan_latest_choir_receipt_report(&config).unwrap();
        let default_wrapper_session_dir = default_wrapper_session_dir.display().to_string();
        let stale_transept_wrapper_session_dir =
            stale_transept_wrapper_session_dir.display().to_string();

        assert_eq!(
            report.session_dir.as_deref(),
            Some(default_wrapper_session_dir.as_str())
        );
        assert_eq!(
            report.choir_receipt_latest_session_path.as_deref().unwrap(),
            default_paths.session_path.display().to_string()
        );
        for summary in [
            report
                .run_latest_choir_receipt_command_summary
                .as_deref()
                .expect("missing run_latest_choir_receipt_command_summary"),
            report
                .verify_latest_choir_receipt_command_summary
                .as_deref()
                .expect("missing verify_latest_choir_receipt_command_summary"),
        ] {
            assert!(
                summary.contains(default_wrapper_session_dir.as_str()),
                "self command summary must use choir latest default session dir: {summary}"
            );
            assert!(
                !summary.contains(stale_transept_wrapper_session_dir.as_str()),
                "self command summary must not use transept latest default wrapper session dir: {summary}"
            );
        }
        assert_eq!(
            report
                .nested_transept_certificate_latest_session_dir
                .as_deref(),
            Some(
                default_paths
                    .nested_transept_certificate_latest_session_dir
                    .display()
                    .to_string()
                    .as_str()
            )
        );
    }

    #[test]
    fn render_mode_emits_exact_downstream_choir_receipt_contract_not_second_controller_path() {
        let fixture = fake_transept_certificate_latest_fixture(
            "transept_certificate_latest_render_ok",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let report = render_latest_choir_receipt_script_report(&fixture.render_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestScriptRendered
        );
        assert_eq!(report.mode, "render_latest_choir_receipt_script");
        let script = fs::read_to_string(&fixture.output_script_path).unwrap();
        assert!(script.contains(CHOIR_RECEIPT_LATEST_BIN));
        assert!(!script.contains(TRANSEPT_CERTIFICATE_LATEST_BIN));
        assert!(script.contains(CHOIR_RECEIPT_BIN));
        assert!(script.contains("--run-latest-choir-receipt"));
        assert!(script.contains("--verify-latest-choir-receipt"));
        assert!(!script.contains("--run-latest-transept-certificate"));
        assert!(!script.contains("--verify-latest-transept-certificate"));
        assert!(script.contains("--transept-certificate-session-dir"));
        assert!(!script.contains("--activation-ticket-session-dir"));
        assert!(script.contains("--run-live-package-choir-receipt"));
        assert!(script.contains("--verify-live-package-choir-receipt"));
        assert!(!script.contains("copybot_tiny_live_activation_package_live_cutover"));
    }

    #[test]
    fn run_mode_refuses_when_latest_chain_missing_invalid_or_below_clerestory() {
        for (label, verdict, reason, expected) in [
            (
                "transept_certificate_latest_missing",
                "tiny_live_package_transept_certificate_latest_refused_by_missing_latest_chain",
                "no persisted immutable chain exists",
                TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestRefusedByMissingLatestChain,
            ),
            (
                "transept_certificate_latest_invalid",
                "tiny_live_package_transept_certificate_latest_refused_by_invalid_latest_chain",
                "latest immutable chain is incomplete",
                TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestRefusedByInvalidLatestChain,
            ),
            (
                "transept_certificate_latest_below",
                "tiny_live_package_transept_certificate_latest_refused_by_invalid_latest_chain",
                "latest persisted top chain is choir_receipt, which is below clerestory_certificate",
                TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestRefusedByInvalidLatestChain,
            ),
        ] {
            let fixture = fake_transept_certificate_latest_fixture(
                label,
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_go_live_signoff",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);

            let mut plan: Value = load_json(&fixture.transept_certificate_latest_plan_json_path).unwrap();
            plan["verdict"] = json!(verdict);
            plan["reason"] = json!(reason);
            if label.ends_with("below") {
                plan["latest_top_step_name"] = json!("choir_receipt");
            }
            persist_json(&fixture.transept_certificate_latest_plan_json_path, &plan).unwrap();

            let report = run_latest_choir_receipt_report(&fixture.run_config()).unwrap();
            assert_eq!(report.verdict, expected);
        }
    }

    #[test]
    fn plan_mode_maps_real_latest_transept_certificate_drift_verdict_to_local_drift_refusal() {
        let fixture = fake_transept_certificate_latest_fixture(
            "transept_certificate_latest_plan_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let mut plan: Value =
            load_json(&fixture.transept_certificate_latest_plan_json_path).unwrap();
        plan["verdict"] =
            json!("tiny_live_package_transept_certificate_latest_refused_by_drifted_transept_certificate_contract");
        plan["reason"] = json!("real accepted latest-transept-certificate drift");
        persist_json(&fixture.transept_certificate_latest_plan_json_path, &plan).unwrap();

        let report = run_latest_choir_receipt_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestRefusedByDriftedChoirReceiptContract
        );
        assert_eq!(
            report.result.as_deref(),
            Some("refused_by_drifted_choir_receipt_contract")
        );
    }

    #[test]
    fn run_mode_takes_terminal_drift_refusal_when_nested_latest_transept_certificate_reports_real_drift(
    ) {
        let fixture = fake_transept_certificate_latest_fixture(
            "transept_certificate_latest_run_drift",
            "refused_by_drifted_transept_certificate_contract",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let report = run_latest_choir_receipt_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestRefusedByDriftedChoirReceiptContract
        );
        assert_eq!(
            report.result.as_deref(),
            Some("refused_by_drifted_choir_receipt_contract")
        );
        assert_eq!(report.choir_receipt_verdict.as_deref(), None);
    }

    #[test]
    fn run_mode_refuses_when_nested_latest_transept_certificate_lineage_drifts_from_latest_immutable_chain(
    ) {
        let fixture = fake_transept_certificate_latest_fixture(
            "transept_certificate_latest_lineage_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let mut run: Value = load_json(&fixture.transept_certificate_latest_run_json_path).unwrap();
        run["package_dir"] = json!("/tampered/package");
        persist_json(&fixture.transept_certificate_latest_run_json_path, &run).unwrap();

        let report = run_latest_choir_receipt_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestRefusedByDriftedChoirReceiptContract
        );
        assert!(report
            .reason
            .contains("stored latest-transept-certificate run package_dir"));
    }

    #[test]
    fn valid_nested_latest_transept_modes_are_accepted_by_latest_choir_plan_run_verify() {
        let fixture = fake_transept_certificate_latest_fixture(
            "choir_receipt_latest_accepts_real_nested_transept_modes",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let plan = plan_latest_choir_receipt_report(&fixture.plan_config()).unwrap();
        assert_eq!(
            plan.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestPlanReady
        );

        let run = run_latest_choir_receipt_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestReadyForManualExecutionWhenGateTurnsGreen
        );

        let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyOk,
            "{:#?}",
            verify.verification_mismatches
        );

        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let stored_plan: Value =
            load_json(&paths.latest_transept_certificate_plan_report_path).unwrap();
        let stored_run: Value = load_json(&paths.latest_transept_certificate_report_path).unwrap();
        let status: Value = load_json(&paths.status_path).unwrap();

        assert_eq!(
            stored_plan["mode"],
            json!("plan_latest_transept_certificate")
        );
        assert_eq!(stored_run["mode"], json!("run_latest_transept_certificate"));
        assert_eq!(
            status["latest_transept_certificate_plan_step"]["mode"],
            json!("plan_latest_transept_certificate")
        );
        assert_eq!(
            status["latest_transept_certificate_step"]["mode"],
            json!("run_latest_transept_certificate")
        );
    }

    #[test]
    fn plan_rejects_stale_parent_mode_in_nested_latest_transept_plan_payload() {
        let fixture = fake_transept_certificate_latest_fixture(
            "choir_receipt_latest_rejects_parent_mode_in_nested_transept_plan",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let mut plan: Value =
            load_json(&fixture.transept_certificate_latest_plan_json_path).unwrap();
        plan["mode"] = json!("plan_latest_choir_receipt");
        persist_json(&fixture.transept_certificate_latest_plan_json_path, &plan).unwrap();

        let report = plan_latest_choir_receipt_report(&fixture.plan_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestRefusedByDriftedChoirReceiptContract
        );
        assert!(report.reason.contains(
            "stored latest-transept-certificate plan mode \"plan_latest_choir_receipt\" does not match expected \"plan_latest_transept_certificate\""
        ));
    }

    #[test]
    fn run_rejects_stale_parent_mode_in_nested_latest_transept_run_payload() {
        let fixture = fake_transept_certificate_latest_fixture(
            "choir_receipt_latest_rejects_parent_mode_in_nested_transept_run",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let mut run: Value = load_json(&fixture.transept_certificate_latest_run_json_path).unwrap();
        run["mode"] = json!("run_latest_choir_receipt");
        persist_json(&fixture.transept_certificate_latest_run_json_path, &run).unwrap();

        let report = run_latest_choir_receipt_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestRefusedByDriftedChoirReceiptContract
        );
        assert!(report.reason.contains(
            "stored latest-transept-certificate run mode \"run_latest_choir_receipt\" does not match expected \"run_latest_transept_certificate\""
        ));
    }

    #[test]
    fn verify_rejects_stale_parent_modes_in_copied_nested_latest_transept_reports() {
        {
            let fixture = fake_transept_certificate_latest_fixture(
                "choir_receipt_latest_rejects_parent_mode_in_copied_transept_plan",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_go_live_signoff",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

            let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
            let mut plan: Value =
                load_json(&paths.latest_transept_certificate_plan_report_path).unwrap();
            plan["mode"] = json!("plan_latest_choir_receipt");
            persist_json(&paths.latest_transept_certificate_plan_report_path, &plan).unwrap();

            let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
            );
            assert!(verify.verification_mismatches.iter().any(|entry| {
                entry.contains(
                    "stored latest-transept-certificate plan mode \"plan_latest_choir_receipt\" does not match expected \"plan_latest_transept_certificate\"",
                )
            }));
        }

        {
            let fixture = fake_transept_certificate_latest_fixture(
                "choir_receipt_latest_rejects_parent_mode_in_copied_transept_run",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_go_live_signoff",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

            let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
            let mut run: Value = load_json(&paths.latest_transept_certificate_report_path).unwrap();
            run["mode"] = json!("run_latest_choir_receipt");
            persist_json(&paths.latest_transept_certificate_report_path, &run).unwrap();

            let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
            );
            assert!(verify.verification_mismatches.iter().any(|entry| {
                entry.contains(
                    "stored latest-transept-certificate run mode \"run_latest_choir_receipt\" does not match expected \"run_latest_transept_certificate\"",
                )
            }));
        }
    }

    #[test]
    fn verify_mode_fails_when_persisted_latest_transept_certificate_artifacts_are_tampered_or_missing_nested_lineage_truth(
    ) {
        {
            let fixture = fake_transept_certificate_latest_fixture(
                "transept_certificate_latest_verify_tampered_copy",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_go_live_signoff",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

            let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
            let mut report: Value = load_json(&paths.choir_receipt_report_path).unwrap();
            report["decision_packet_session_dir"] = json!("/tampered/run-decision-packet-session");
            persist_json(&paths.choir_receipt_report_path, &report).unwrap();

            let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
            );
            assert!(verify.verification_mismatches.iter().any(|entry| {
                entry.contains("stored choir-receipt run decision_packet_session_dir")
            }));
        }

        {
            let fixture = fake_transept_certificate_latest_fixture(
                "transept_certificate_latest_verify_missing_nested_truth",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_go_live_signoff",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

            let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
            fs::remove_file(nested_transept_certificate_latest_report_path(
                &paths.nested_transept_certificate_latest_session_dir,
            ))
            .unwrap();

            let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
            );
            assert!(verify.verification_mismatches.iter().any(|entry| {
                entry.contains(
                    "tiny_live_activation_package_transept_certificate_latest.report.json",
                )
            }));
        }
    }

    #[test]
    fn verify_accepts_real_shaped_native_choir_and_transept_identity_layers_on_happy_path() {
        let fixture = fake_transept_certificate_latest_fixture(
            "choir_receipt_latest_verify_real_shaped_choir_paths_green",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

        let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyOk,
            "{:#?}",
            verify.verification_mismatches
        );
    }

    #[test]
    fn fake_native_choir_payload_uses_ensign_seal_and_transept_guidon_seal_input_wording() {
        let fixture = fake_transept_certificate_latest_fixture(
            "choir_receipt_latest_fixture_identity_wording",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let snapshot = LatestTranseptCertificateSnapshot {
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
        let fallback_session =
            fallback_choir_receipt_session(&snapshot, &paths.nested_choir_receipt_session_dir);
        assert!(fallback_session.explicit_statement.contains("ensign-seal"));
        assert!(!fallback_session.explicit_statement.contains("guidon-seal"));
        assert!(!fallback_session.explicit_statement.contains("pennant-seal"));
        assert!(!fallback_session.explicit_statement.contains("banner-seal"));
        assert!(!fallback_session
            .explicit_statement
            .contains("standard-seal"));
        assert!(!fallback_session.explicit_statement.contains("herald"));
        assert!(!fallback_session.explicit_statement.contains("crest"));
        assert!(!fallback_session.explicit_statement.contains("hallmark"));
        assert!(!fallback_session.explicit_statement.contains("escutcheon"));
        assert!(!fallback_session.explicit_statement.contains("blazon"));
        assert!(!fallback_session.explicit_statement.contains("armorial"));

        let payloads = [
            ("session", default_choir_receipt_session(&fixture)),
            (
                "status",
                default_choir_receipt_status(
                    &fixture,
                    "ready_for_manual_execution_when_gate_turns_green",
                ),
            ),
            (
                "run",
                default_choir_receipt_run_report(
                    &fixture,
                    "ready_for_manual_execution_when_gate_turns_green",
                ),
            ),
            (
                "verify",
                default_choir_receipt_verify_report(
                    &fixture,
                    "ready_for_manual_execution_when_gate_turns_green",
                ),
            ),
        ];

        for (label, payload) in payloads {
            let choir_summary = payload["choir_receipt_summary"]
                .as_str()
                .unwrap_or_else(|| panic!("{label} choir_receipt_summary missing"));
            assert!(
                choir_summary.contains("ensign-seal"),
                "{label} choir summary must use accepted ensign-seal wording: {choir_summary}"
            );
            assert!(
                !choir_summary.contains("guidon-seal"),
                "{label} choir summary must not use upstream transept guidon-seal wording: {choir_summary}"
            );
            assert!(
                !choir_summary.contains("pennant-seal"),
                "{label} choir summary must not use upstream nave pennant-seal wording: {choir_summary}"
            );
            assert!(
                !choir_summary.contains("banner-seal"),
                "{label} choir summary must not use upstream sanctuary banner-seal wording: {choir_summary}"
            );
            assert!(
                !choir_summary.contains("standard-seal"),
                "{label} choir summary must not use upstream apse standard-seal wording: {choir_summary}"
            );
            assert!(
                !choir_summary.contains("herald"),
                "{label} choir summary must not use upstream chancel herald wording: {choir_summary}"
            );
            assert!(
                !choir_summary.contains("crest"),
                "{label} choir summary must not use upstream pulpit crest wording: {choir_summary}"
            );
            assert!(
                !choir_summary.contains("hallmark"),
                "{label} choir summary must not use stale hallmark wording: {choir_summary}"
            );
            assert!(
                !choir_summary.contains("escutcheon"),
                "{label} choir summary must not use upstream rostrum escutcheon wording: {choir_summary}"
            );
            assert!(
                !choir_summary.contains("blazon"),
                "{label} choir summary must not use upstream podium blazon wording: {choir_summary}"
            );
            assert!(
                !choir_summary.contains("armorial"),
                "{label} choir summary must not use upstream lectern armorial wording: {choir_summary}"
            );

            let explicit_statement = payload["explicit_statement"]
                .as_str()
                .unwrap_or_else(|| panic!("{label} explicit_statement missing"));
            assert!(
                !explicit_statement.contains("hallmark"),
                "{label} explicit statement must not use stale hallmark wording: {explicit_statement}"
            );
            if explicit_statement.contains("freezes one final") {
                assert!(
                    explicit_statement.contains("ensign-seal"),
                    "{label} choir session/run explicit statement must use ensign-seal wording: {explicit_statement}"
                );
                assert!(
                    !explicit_statement.contains("guidon-seal"),
                    "{label} choir session/run explicit statement must not use upstream transept guidon-seal wording: {explicit_statement}"
                );
                assert!(
                    !explicit_statement.contains("pennant-seal"),
                    "{label} choir session/run explicit statement must not use upstream nave pennant-seal wording: {explicit_statement}"
                );
                assert!(
                    !explicit_statement.contains("banner-seal"),
                    "{label} choir session/run explicit statement must not use upstream sanctuary banner-seal wording: {explicit_statement}"
                );
                assert!(
                    !explicit_statement.contains("standard-seal"),
                    "{label} choir session/run explicit statement must not use upstream apse standard-seal wording: {explicit_statement}"
                );
                assert!(
                    !explicit_statement.contains("herald"),
                    "{label} choir session/run explicit statement must not use upstream chancel herald wording: {explicit_statement}"
                );
                assert!(
                    !explicit_statement.contains("crest"),
                    "{label} choir session/run explicit statement must not use upstream pulpit crest wording: {explicit_statement}"
                );
                assert!(
                    !explicit_statement.contains("escutcheon"),
                    "{label} choir session/run explicit statement must not use upstream rostrum escutcheon wording: {explicit_statement}"
                );
                assert!(
                    !explicit_statement.contains("blazon"),
                    "{label} choir session/run explicit statement must not use upstream podium blazon wording: {explicit_statement}"
                );
                assert!(
                    !explicit_statement.contains("armorial"),
                    "{label} choir session/run explicit statement must not use upstream lectern armorial wording: {explicit_statement}"
                );
            }

            let transept_input_summary = payload["transept_certificate_summary"]
                .as_str()
                .unwrap_or_else(|| panic!("{label} transept_certificate_summary missing"));
            assert!(
                transept_input_summary.contains("guidon-seal"),
                "{label} transept input summary must use accepted guidon-seal wording: {transept_input_summary}"
            );
            assert!(
                transept_input_summary.contains("nave-receipt"),
                "{label} transept input summary must bind to nave-receipt truth: {transept_input_summary}"
            );
            assert!(
                !transept_input_summary.contains("pennant-seal"),
                "{label} transept input summary must not use upstream nave pennant-seal wording: {transept_input_summary}"
            );
            assert!(
                !transept_input_summary.contains("banner-seal"),
                "{label} transept input summary must not use upstream sanctuary banner-seal wording: {transept_input_summary}"
            );
            assert!(
                !transept_input_summary.contains("standard-seal"),
                "{label} transept input summary must not use upstream apse standard-seal wording: {transept_input_summary}"
            );
            assert!(
                !transept_input_summary.contains("herald"),
                "{label} transept input summary must not use upstream chancel herald wording: {transept_input_summary}"
            );
            assert!(
                !transept_input_summary.contains("crest"),
                "{label} transept input summary must not use upstream pulpit crest wording: {transept_input_summary}"
            );
            assert!(
                !transept_input_summary.contains("hallmark"),
                "{label} transept input summary must not use stale hallmark wording: {transept_input_summary}"
            );
            assert!(
                !transept_input_summary.contains("escutcheon"),
                "{label} transept input summary must not use upstream rostrum escutcheon wording: {transept_input_summary}"
            );
            assert!(
                !transept_input_summary.contains("blazon"),
                "{label} transept input summary must not use upstream podium blazon wording: {transept_input_summary}"
            );
            assert!(
                !transept_input_summary.contains("armorial"),
                "{label} transept input summary must not use upstream lectern armorial wording: {transept_input_summary}"
            );
            assert!(
                !transept_input_summary.contains("rostrum-certificate"),
                "{label} transept input summary must not bind directly to rostrum-certificate truth: {transept_input_summary}"
            );
            assert!(
                !transept_input_summary.contains("dais-receipt"),
                "{label} transept input summary must not bind directly to dais-receipt truth: {transept_input_summary}"
            );
            assert!(
                !transept_input_summary.contains("plinth-receipt"),
                "{label} transept input summary must not bind directly to plinth-receipt truth: {transept_input_summary}"
            );
        }
    }

    #[test]
    fn run_and_verify_reject_stale_foreign_wrapper_schema_for_downstream_choir_copy() {
        let fixture = fake_transept_certificate_latest_fixture(
            "choir_receipt_latest_verify_rejects_stale_keystone_wrapper_schema",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        assert!(paths.choir_receipt_report_path.ends_with(
            "tiny_live_activation_package_choir_receipt_latest.choir_receipt.report.json"
        ));
        assert!(paths.choir_receipt_report_path.exists());
        assert!(!fixture
            .wrapper_session_dir
            .join(
                "tiny_live_activation_package_choir_receipt_latest.transept_certificate.report.json"
            )
            .exists());
        assert!(!fixture
            .wrapper_session_dir
            .join(
                "tiny_live_activation_package_choir_receipt_latest.pedestal_certificate.report.json"
            )
            .exists());
        assert!(!fixture
            .wrapper_session_dir
            .join("tiny_live_activation_package_choir_receipt_latest.plinth_receipt.report.json")
            .exists());
        assert!(!fixture
            .wrapper_session_dir
            .join(
                "tiny_live_activation_package_choir_receipt_latest.substructure_certificate.report.json"
            )
            .exists());
        assert!(paths
            .nested_choir_receipt_session_dir
            .ends_with("tiny_live_activation_package_choir_receipt_latest.choir_receipt_session"));
        assert!(!fixture
            .wrapper_session_dir
            .join("tiny_live_activation_package_choir_receipt_latest.transept_certificate_session")
            .exists());
        assert!(!fixture
            .wrapper_session_dir
            .join("tiny_live_activation_package_choir_receipt_latest.pedestal_certificate_session")
            .exists());
        assert!(!fixture
            .wrapper_session_dir
            .join("tiny_live_activation_package_choir_receipt_latest.plinth_receipt_session")
            .exists());
        assert!(!fixture
            .wrapper_session_dir
            .join(
                "tiny_live_activation_package_choir_receipt_latest.substructure_certificate_session"
            )
            .exists());

        let mut status: Value = load_json(&paths.status_path).unwrap();
        let stored_step = status
            .get("choir_receipt_step")
            .cloned()
            .expect("choir_receipt_step should be present");
        assert!(status.get("keystone_receipt_step").is_none());
        status["keystone_receipt_step"] = stored_step;
        if let Some(object) = status.as_object_mut() {
            object.remove("choir_receipt_step");
        }
        persist_json(&paths.status_path, &status).unwrap();

        let error = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap_err();
        assert!(error.to_string().contains("choir_receipt_step"));
    }

    #[test]
    fn verify_names_choir_receipt_when_nested_native_session_load_breaks() {
        let fixture = fake_transept_certificate_latest_fixture(
            "transept_certificate_latest_verify_nested_dais_session_load_break",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        fs::remove_file(nested_choir_receipt_session_path(
            &paths.nested_choir_receipt_session_dir,
        ))
        .unwrap();

        let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
        );
        assert!(verify
            .verification_mismatches
            .iter()
            .any(|entry| { entry.contains("failed reading nested choir-receipt session under") }));
    }

    #[test]
    fn verify_fails_when_copied_native_choir_receipt_session_path_drifts() {
        let fixture = fake_transept_certificate_latest_fixture(
            "transept_certificate_latest_verify_dais_session_path_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.choir_receipt_report_path).unwrap();
        report["choir_receipt_session_path"] = json!("/tampered/finality-receipt-session.json");
        persist_json(&paths.choir_receipt_report_path, &report).unwrap();

        let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored choir-receipt run choir_receipt_session_path")
        }));
    }

    #[test]
    fn verify_fails_when_copied_native_choir_receipt_status_path_drifts() {
        let fixture = fake_transept_certificate_latest_fixture(
            "transept_certificate_latest_verify_dais_status_path_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.choir_receipt_report_path).unwrap();
        report["choir_receipt_status_path"] = json!("/tampered/finality-receipt-status.json");
        persist_json(&paths.choir_receipt_report_path, &report).unwrap();

        let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
        );
        assert!(verify
            .verification_mismatches
            .iter()
            .any(|entry| { entry.contains("stored choir-receipt run choir_receipt_status_path") }));
    }

    #[test]
    fn verify_fails_when_copied_native_choir_pedestal_certificate_identity_drifts() {
        let fixture = fake_transept_certificate_latest_fixture(
            "choir_receipt_verify_pedestal_identity_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.choir_receipt_report_path).unwrap();
        report["pedestal_certificate_sha256"] = json!("tampered-pedestal-certificate-sha256");
        persist_json(&paths.choir_receipt_report_path, &report).unwrap();

        let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored choir-receipt copy pedestal_certificate_sha256")
        }));
    }

    #[test]
    fn run_names_latest_transept_certificate_when_nested_latest_wrapper_run_fails() {
        let fixture = fake_transept_certificate_latest_fixture(
            "transept_certificate_latest_nested_run_failure_wording",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        overwrite_fake_transept_certificate_latest_command(
            &fixture.bin_dir.join(TRANSEPT_CERTIFICATE_LATEST_BIN),
            &fixture.transept_certificate_latest_plan_json_path,
            &fixture.transept_certificate_latest_run_json_path,
            &fixture.transept_certificate_latest_verify_json_path,
            None,
            Some("forced nested latest-transept-certificate run failure"),
            None,
            &fixture.root,
            &paths.nested_transept_certificate_latest_session_dir,
        );

        let report = run_latest_choir_receipt_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestRefusedByDriftedChoirReceiptContract
        );
        assert!(report
            .reason
            .contains("failed running nested latest-transept-certificate handoff under"));
    }

    #[test]
    fn run_names_latest_transept_certificate_when_nested_latest_wrapper_verify_fails() {
        let fixture = fake_transept_certificate_latest_fixture(
            "transept_certificate_latest_nested_verify_failure_wording",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        overwrite_fake_transept_certificate_latest_command(
            &fixture.bin_dir.join(TRANSEPT_CERTIFICATE_LATEST_BIN),
            &fixture.transept_certificate_latest_plan_json_path,
            &fixture.transept_certificate_latest_run_json_path,
            &fixture.transept_certificate_latest_verify_json_path,
            None,
            None,
            Some("forced nested latest-transept-certificate verify failure"),
            &fixture.root,
            &paths.nested_transept_certificate_latest_session_dir,
        );

        let report = run_latest_choir_receipt_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestRefusedByDriftedChoirReceiptContract
        );
        assert!(report
            .reason
            .contains("failed verifying nested latest-transept-certificate handoff under"));
    }

    #[test]
    fn run_names_choir_receipt_when_downstream_native_run_fails() {
        let fixture = fake_transept_certificate_latest_fixture(
            "choir_receipt_downstream_run_failure_wording",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        overwrite_fake_release_capsule_command(
            &fixture.bin_dir.join(CHOIR_RECEIPT_BIN),
            &fixture.handoff_bundle_run_json_path,
            &fixture.handoff_bundle_verify_json_path,
            &fixture.handoff_bundle_session_json_path,
            &fixture.handoff_bundle_status_json_path,
            Some("forced downstream native choir run failure"),
            None,
            &expected_downstream_transept_certificate_session_dir(&paths),
            &expected_downstream_decision_packet_session_dir(&paths),
            &paths.nested_choir_receipt_session_dir,
        );

        let report = run_latest_choir_receipt_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestRefusedByDriftedChoirReceiptContract
        );
        assert!(report
            .reason
            .contains("failed running downstream choir-receipt contract under"));
        assert!(!report
            .reason
            .contains("failed running downstream plinth-receipt contract under"));
        assert!(!report
            .reason
            .contains("failed running downstream transept-certificate contract under"));
        assert!(!report.reason.contains("native chancel"));
        assert!(!report.reason.contains("native pulpit"));
        assert!(!report.reason.contains("native lectern"));
    }

    #[test]
    fn run_names_choir_receipt_when_downstream_native_verify_fails() {
        let fixture = fake_transept_certificate_latest_fixture(
            "choir_receipt_downstream_verify_failure_wording",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        overwrite_fake_release_capsule_command(
            &fixture.bin_dir.join(CHOIR_RECEIPT_BIN),
            &fixture.handoff_bundle_run_json_path,
            &fixture.handoff_bundle_verify_json_path,
            &fixture.handoff_bundle_session_json_path,
            &fixture.handoff_bundle_status_json_path,
            None,
            Some("forced downstream native choir verify failure"),
            &expected_downstream_transept_certificate_session_dir(&paths),
            &expected_downstream_decision_packet_session_dir(&paths),
            &paths.nested_choir_receipt_session_dir,
        );

        let report = run_latest_choir_receipt_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestRefusedByDriftedChoirReceiptContract
        );
        assert!(report
            .reason
            .contains("failed verifying downstream choir-receipt contract under"));
        assert!(!report
            .reason
            .contains("failed verifying downstream plinth-receipt contract under"));
        assert!(!report
            .reason
            .contains("failed verifying downstream transept-certificate contract under"));
        assert!(!report.reason.contains("native chancel"));
        assert!(!report.reason.contains("native pulpit"));
        assert!(!report.reason.contains("native lectern"));
    }

    #[test]
    fn run_uses_choir_placeholders_when_missing_snapshot_blocks_nested_handoff_summaries() {
        let fixture = fake_transept_certificate_latest_fixture(
            "choir_receipt_latest_missing_snapshot_placeholders",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        overwrite_fake_transept_certificate_latest_command(
            &fixture.bin_dir.join(TRANSEPT_CERTIFICATE_LATEST_BIN),
            &fixture.transept_certificate_latest_plan_json_path,
            &fixture.transept_certificate_latest_run_json_path,
            &fixture.transept_certificate_latest_verify_json_path,
            Some("forced latest-transept-certificate plan failure without snapshot"),
            None,
            None,
            &fixture.root,
            &paths.nested_transept_certificate_latest_session_dir,
        );

        let report = run_latest_choir_receipt_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestRefusedByMissingLatestChain
        );
        assert_eq!(
            report.downstream_choir_receipt_command_summary.as_deref(),
            Some("<choir-receipt-run-unavailable>")
        );
        assert_eq!(
            report.verify_choir_receipt_command_summary.as_deref(),
            Some("<choir-receipt-verify-unavailable>")
        );

        let session: PackageChoirReceiptLatestSession = load_json(&paths.session_path).unwrap();
        assert_eq!(
            session.downstream_choir_receipt_command_summary,
            "<choir-receipt-run-unavailable>"
        );
        assert_eq!(
            session.verify_choir_receipt_command_summary,
            "<choir-receipt-verify-unavailable>"
        );
    }

    #[test]
    fn verify_names_choir_receipt_when_nested_native_verify_is_non_green() {
        let fixture = fake_transept_certificate_latest_fixture(
            "choir_receipt_verify_non_green_wording",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

        let mut verify_json: Value = load_json(&fixture.handoff_bundle_verify_json_path).unwrap();
        verify_json["verdict"] = json!("tiny_live_package_choir_receipt_verify_invalid");
        verify_json["reason"] = json!("forced nested choir-receipt verify non-green");
        persist_json(&fixture.handoff_bundle_verify_json_path, &verify_json).unwrap();

        let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
        );
        assert!(verify
            .verification_mismatches
            .iter()
            .any(|entry| { entry.contains("nested choir-receipt verification is non-green") }));
        assert!(!verify.verification_mismatches.iter().any(|entry| {
            entry.contains("nested substructure-certificate verification is non-green")
        }));
    }

    #[test]
    fn verify_names_latest_choir_receipt_when_wrapper_result_drifts() {
        let fixture = fake_transept_certificate_latest_fixture(
            "choir_receipt_wrapper_result_mismatch_wording",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let mut status: Value = load_json(&paths.status_path).unwrap();
        status["result"] = json!("refused_by_missing_latest_chain");
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
        );
        assert!(verify
            .verification_mismatches
            .iter()
            .any(|entry| { entry.contains("stored latest-choir-receipt result") }));
        assert!(!verify
            .verification_mismatches
            .iter()
            .any(|entry| { entry.contains("stored latest-transept-certificate result") }));
    }

    #[test]
    fn verify_names_latest_choir_receipt_when_activation_authorized_drifts() {
        let fixture = fake_transept_certificate_latest_fixture(
            "choir_receipt_activation_authorized_wording",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let mut status: Value = load_json(&paths.status_path).unwrap();
        status["activation_authorized"] = json!(true);
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains(
                "stored status activation_authorized must remain false for latest-choir-receipt",
            )
        }));
        assert!(!verify.verification_mismatches.iter().any(|entry| {
            entry.contains(
                "stored status activation_authorized must remain false for latest-transept-certificate",
            )
        }));
    }

    #[test]
    fn verify_fails_when_copied_native_pinnacle_receipt_sha256_drifts() {
        let fixture = fake_transept_certificate_latest_fixture(
            "transept_certificate_latest_verify_pinnacle_receipt_sha256_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.choir_receipt_report_path).unwrap();
        report["pinnacle_receipt_sha256"] = json!("tampered-summit-certificate-sha256");
        persist_json(&paths.choir_receipt_report_path, &report).unwrap();

        let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
        );
        assert!(verify
            .verification_mismatches
            .iter()
            .any(|entry| { entry.contains("stored choir-receipt copy pinnacle_receipt_sha256") }));
    }

    #[test]
    fn verify_fails_when_copied_nested_latest_transept_certificate_explicit_statement_drifts() {
        let fixture = fake_transept_certificate_latest_fixture(
            "transept_certificate_latest_verify_launch_packet_explicit_statement_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.latest_transept_certificate_report_path).unwrap();
        report["explicit_statement"] =
            json!("tampered nested latest release capsule explicit statement");
        persist_json(&paths.latest_transept_certificate_report_path, &report).unwrap();

        let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored latest-transept-certificate copy explicit_statement")
        }));
    }

    #[test]
    fn verify_rejects_tampered_stale_parent_wrapper_mode_string() {
        let fixture = fake_transept_certificate_latest_fixture(
            "choir_receipt_latest_verify_wrapper_mode_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.report_path).unwrap();
        report["mode"] = json!("run_latest_substructure_certificate");
        persist_json(&paths.report_path, &report).unwrap();

        let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
        );
        assert!(verify
            .verification_mismatches
            .iter()
            .any(|entry| { entry.contains("latest-choir-receipt stored report mode") }));
    }

    #[test]
    fn wrapper_self_modes_and_self_command_summaries_use_choir_latest_contract() {
        let fixture = fake_transept_certificate_latest_fixture(
            "choir_receipt_latest_self_mode_and_summary_shape",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let plan = plan_latest_choir_receipt_report(&fixture.plan_config()).unwrap();
        assert_eq!(plan.mode, "plan_latest_choir_receipt");
        assert!(plan
            .run_latest_choir_receipt_command_summary
            .as_deref()
            .unwrap()
            .contains(CHOIR_RECEIPT_LATEST_BIN));
        assert!(plan
            .run_latest_choir_receipt_command_summary
            .as_deref()
            .unwrap()
            .contains("--run-latest-choir-receipt"));
        assert!(!plan
            .run_latest_choir_receipt_command_summary
            .as_deref()
            .unwrap()
            .contains("--run-latest-transept-certificate"));
        assert!(plan
            .verify_latest_choir_receipt_command_summary
            .as_deref()
            .unwrap()
            .contains(CHOIR_RECEIPT_LATEST_BIN));
        assert!(plan
            .verify_latest_choir_receipt_command_summary
            .as_deref()
            .unwrap()
            .contains("--verify-latest-choir-receipt"));
        assert!(!plan
            .verify_latest_choir_receipt_command_summary
            .as_deref()
            .unwrap()
            .contains("--verify-latest-transept-certificate"));

        let run = run_latest_choir_receipt_report(&fixture.run_config()).unwrap();
        assert_eq!(run.mode, "run_latest_choir_receipt");

        let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(verify.mode, "verify_latest_choir_receipt");

        let session: PackageChoirReceiptLatestSession =
            load_json(&choir_receipt_latest_paths(&fixture.wrapper_session_dir).session_path)
                .unwrap();
        assert!(session
            .run_latest_choir_receipt_command_summary
            .contains(CHOIR_RECEIPT_LATEST_BIN));
        assert!(session
            .run_latest_choir_receipt_command_summary
            .contains("--run-latest-choir-receipt"));
        assert!(!session
            .run_latest_choir_receipt_command_summary
            .contains("--run-latest-transept-certificate"));
        assert!(session
            .verify_latest_choir_receipt_command_summary
            .contains(CHOIR_RECEIPT_LATEST_BIN));
        assert!(session
            .verify_latest_choir_receipt_command_summary
            .contains("--verify-latest-choir-receipt"));
        assert!(!session
            .verify_latest_choir_receipt_command_summary
            .contains("--verify-latest-transept-certificate"));

        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let session_json: Value = load_json(&paths.session_path).unwrap();
        assert!(session_json
            .get("verify_choir_receipt_command_summary")
            .is_some());
        assert!(session_json
            .get("verify_transept_certificate_command_summary")
            .is_none());

        let report_json: Value = load_json(&paths.report_path).unwrap();
        assert!(report_json
            .get("verify_choir_receipt_command_summary")
            .is_some());
        assert!(report_json
            .get("verify_transept_certificate_command_summary")
            .is_none());
    }

    #[test]
    fn native_choir_input_uses_real_latest_transept_certificate_session_path() {
        let fixture = fake_transept_certificate_latest_fixture(
            "choir_receipt_native_input_session_dir_shape",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);

        let downstream = expected_downstream_transept_certificate_session_dir(&paths);
        let expected = paths.nested_transept_certificate_latest_session_dir.join(
            "tiny_live_activation_package_transept_certificate_latest.transept_certificate_session",
        );

        assert_eq!(downstream, expected);
        assert!(downstream
            .file_name()
            .unwrap()
            .to_string_lossy()
            .contains("transept_certificate_session"));
        assert!(!downstream
            .file_name()
            .unwrap()
            .to_string_lossy()
            .contains("plinth_receipt_session"));

        assert!(paths
            .nested_choir_receipt_session_dir
            .file_name()
            .unwrap()
            .to_string_lossy()
            .contains("choir_receipt_session"));
        assert!(!paths
            .nested_choir_receipt_session_dir
            .file_name()
            .unwrap()
            .to_string_lossy()
            .contains("transept_certificate_session"));
        assert!(!paths
            .nested_choir_receipt_session_dir
            .file_name()
            .unwrap()
            .to_string_lossy()
            .contains("substructure_certificate_session"));
    }

    #[test]
    fn run_persists_copied_native_choir_session_and_status_under_real_choir_filenames() {
        let fixture = fake_transept_certificate_latest_fixture(
            "choir_receipt_latest_native_choir_filenames",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let real_session =
            nested_choir_receipt_session_path(&paths.nested_choir_receipt_session_dir);
        let real_status = nested_choir_receipt_status_path(&paths.nested_choir_receipt_session_dir);
        let stale_session = paths
            .nested_choir_receipt_session_dir
            .join("tiny_live_activation_package_substructure_certificate.session.json");
        let stale_status = paths
            .nested_choir_receipt_session_dir
            .join("tiny_live_activation_package_substructure_certificate.status.json");
        let stale_transept_session = paths
            .nested_choir_receipt_session_dir
            .join("tiny_live_activation_package_transept_certificate.session.json");
        let stale_transept_status = paths
            .nested_choir_receipt_session_dir
            .join("tiny_live_activation_package_transept_certificate.status.json");

        assert!(
            real_session.exists(),
            "missing real native choir session path"
        );
        assert!(
            real_status.exists(),
            "missing real native choir status path"
        );
        assert!(
            !stale_session.exists(),
            "stale native substructure session filename should not be used"
        );
        assert!(
            !stale_status.exists(),
            "stale native substructure status filename should not be used"
        );
        assert!(
            !stale_transept_session.exists(),
            "stale native transept session filename should not be used"
        );
        assert!(
            !stale_transept_status.exists(),
            "stale native transept status filename should not be used"
        );
    }

    #[test]
    fn verify_fails_when_copied_native_choir_decision_packet_result_drifts() {
        let fixture = fake_transept_certificate_latest_fixture(
            "transept_certificate_latest_verify_decision_packet_result_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.choir_receipt_report_path).unwrap();
        report["decision_packet_result"] = json!("tampered_decision_packet_result");
        persist_json(&paths.choir_receipt_report_path, &report).unwrap();

        let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
        );
        assert!(verify
            .verification_mismatches
            .iter()
            .any(|entry| { entry.contains("stored choir-receipt copy decision_packet_result") }));
    }

    #[test]
    fn verify_fails_when_copied_nested_notarization_receipt_attestation_seal_session_dir_drifts() {
        let fixture = fake_transept_certificate_latest_fixture(
            "transept_certificate_latest_verify_attestation_seal_session_dir_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.choir_receipt_report_path).unwrap();
        report["attestation_seal_session_dir"] = json!("/tampered/attestation-seal-session");
        persist_json(&paths.choir_receipt_report_path, &report).unwrap();

        let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored choir-receipt copy attestation_seal_session_dir")
        }));
    }

    #[test]
    fn verify_fails_when_copied_native_finality_receipt_registry_entry_session_dir_drifts() {
        let fixture = fake_transept_certificate_latest_fixture(
            "transept_certificate_latest_verify_registry_entry_session_dir_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.choir_receipt_report_path).unwrap();
        report["registry_entry_session_dir"] = json!("/tampered/registry-entry-session");
        persist_json(&paths.choir_receipt_report_path, &report).unwrap();

        let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored choir-receipt copy registry_entry_session_dir")
        }));
    }

    #[test]
    fn verify_fails_when_copied_native_finality_receipt_notarization_receipt_session_dir_drifts() {
        let fixture = fake_transept_certificate_latest_fixture(
            "transept_certificate_latest_verify_notarization_receipt_session_dir_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.choir_receipt_report_path).unwrap();
        report["notarization_receipt_session_dir"] =
            json!("/tampered/notarization-receipt-session");
        persist_json(&paths.choir_receipt_report_path, &report).unwrap();

        let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored choir-receipt copy notarization_receipt_session_dir")
        }));
    }

    #[test]
    fn verify_fails_when_copied_nested_notarization_receipt_chain_fingerprint_summary_drifts() {
        let fixture = fake_transept_certificate_latest_fixture(
            "transept_certificate_latest_verify_chain_fingerprint_summary_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.choir_receipt_report_path).unwrap();
        report["chain_fingerprint_summary"] = json!("tampered chain fingerprint summary");
        persist_json(&paths.choir_receipt_report_path, &report).unwrap();

        let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored choir-receipt copy chain_fingerprint_summary")
        }));
    }

    #[test]
    fn verify_fails_when_copied_nested_notarization_receipt_chain_fingerprint_sha256_drifts() {
        let fixture = fake_transept_certificate_latest_fixture(
            "transept_certificate_latest_verify_chain_fingerprint_sha256_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.choir_receipt_report_path).unwrap();
        report["chain_fingerprint_sha256"] = json!("tampered-chain-fingerprint-sha256");
        persist_json(&paths.choir_receipt_report_path, &report).unwrap();

        let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
        );
        assert!(verify
            .verification_mismatches
            .iter()
            .any(|entry| { entry.contains("stored choir-receipt copy chain_fingerprint_sha256") }));
    }

    #[test]
    fn verify_fails_when_copied_nested_notarization_receipt_chain_fingerprint_algorithm_drifts() {
        let fixture = fake_transept_certificate_latest_fixture(
            "transept_certificate_latest_verify_chain_fingerprint_algorithm_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.choir_receipt_report_path).unwrap();
        report["chain_fingerprint_algorithm"] = json!("tampered-fingerprint-algorithm");
        persist_json(&paths.choir_receipt_report_path, &report).unwrap();

        let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored choir-receipt copy chain_fingerprint_algorithm")
        }));
    }

    #[test]
    fn verify_fails_when_copied_native_completion_certificate_closure_certificate_sha256_drifts() {
        let fixture = fake_transept_certificate_latest_fixture(
            "transept_certificate_latest_verify_closure_certificate_sha256_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.choir_receipt_report_path).unwrap();
        report["closure_certificate_sha256"] = json!("tampered-closure-certificate-sha256");
        persist_json(&paths.choir_receipt_report_path, &report).unwrap();

        let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored choir-receipt copy closure_certificate_sha256")
        }));
    }

    #[test]
    fn verify_fails_when_copied_native_finality_receipt_archive_receipt_sha256_drifts() {
        let fixture = fake_transept_certificate_latest_fixture(
            "transept_certificate_latest_verify_archive_receipt_sha256_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.choir_receipt_report_path).unwrap();
        report["archive_receipt_sha256"] = json!("tampered-archive-receipt-sha256");
        persist_json(&paths.choir_receipt_report_path, &report).unwrap();

        let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
        );
        assert!(verify
            .verification_mismatches
            .iter()
            .any(|entry| { entry.contains("stored choir-receipt copy archive_receipt_sha256") }));
    }

    #[test]
    fn verify_fails_when_copied_native_finality_receipt_finality_receipt_sha256_drifts() {
        let fixture = fake_transept_certificate_latest_fixture(
            "transept_certificate_latest_verify_finality_receipt_sha256_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.choir_receipt_report_path).unwrap();
        report["finality_receipt_sha256"] = json!("tampered-finality-receipt-sha256");
        persist_json(&paths.choir_receipt_report_path, &report).unwrap();

        let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
        );
        assert!(verify
            .verification_mismatches
            .iter()
            .any(|entry| { entry.contains("stored choir-receipt copy finality_receipt_sha256") }));
    }

    #[test]
    fn verify_fails_when_copied_native_consummation_record_sha256_drifts() {
        let fixture = fake_transept_certificate_latest_fixture(
            "transept_certificate_latest_verify_consummation_record_sha256_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.choir_receipt_report_path).unwrap();
        report["consummation_record_sha256"] = json!("tampered-consummation-record-sha256");
        persist_json(&paths.choir_receipt_report_path, &report).unwrap();

        let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored choir-receipt copy consummation_record_sha256")
        }));
    }

    #[test]
    fn verify_fails_when_copied_native_completion_certificate_completion_certificate_sha256_drifts()
    {
        let fixture = fake_transept_certificate_latest_fixture(
            "transept_certificate_latest_verify_completion_certificate_sha256_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.choir_receipt_report_path).unwrap();
        report["completion_certificate_sha256"] = json!("tampered-completion-certificate-sha256");
        persist_json(&paths.choir_receipt_report_path, &report).unwrap();

        let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored choir-receipt copy completion_certificate_sha256")
        }));
    }

    #[test]
    fn verify_fails_when_downstream_decision_packet_confirmation_anchor_drifts() {
        let fixture = fake_transept_certificate_latest_fixture(
            "transept_certificate_latest_verify_decision_packet_anchor_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.choir_receipt_report_path).unwrap();
        report["decision_packet_session_dir"] = json!("/tampered/decision-packet-session");
        persist_json(&paths.choir_receipt_report_path, &report).unwrap();

        let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored choir-receipt copy decision_packet_session_dir")
        }));
    }

    #[test]
    fn verify_fails_when_copied_nested_provenance_certificate_execute_frozen_result_drifts() {
        let fixture = fake_transept_certificate_latest_fixture(
            "transept_certificate_latest_verify_current_authorization_result_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.choir_receipt_report_path).unwrap();
        report["execute_frozen_result"] = json!("tampered_execute_frozen_result");
        persist_json(&paths.choir_receipt_report_path, &report).unwrap();

        let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
        );
        assert!(verify
            .verification_mismatches
            .iter()
            .any(|entry| { entry.contains("stored choir-receipt copy execute_frozen_result") }));
    }

    #[test]
    fn verify_fails_when_copied_nested_choir_receipt_verdict_drifts() {
        let fixture = fake_transept_certificate_latest_fixture(
            "transept_certificate_latest_verify_release_capsule_verdict_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.choir_receipt_report_path).unwrap();
        report["verdict"] = json!("tampered_release_capsule_verdict");
        persist_json(&paths.choir_receipt_report_path, &report).unwrap();

        let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
        );
        assert!(verify
            .verification_mismatches
            .iter()
            .any(|entry| entry.contains("stored choir-receipt copy verdict")));
    }

    #[test]
    fn verify_fails_when_copied_nested_provenance_certificate_current_live_cutover_controller_summary_drifts(
    ) {
        let fixture = fake_transept_certificate_latest_fixture(
            "transept_certificate_latest_verify_current_live_cutover_summary_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_choir_receipt_report(&fixture.run_config()).unwrap();

        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.choir_receipt_report_path).unwrap();
        report["reviewed_frozen_live_cutover_controller_command_summary"] =
            json!("tampered live cutover controller summary");
        persist_json(&paths.choir_receipt_report_path, &report).unwrap();

        let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains(
                "stored choir-receipt copy reviewed_frozen_live_cutover_controller_command_summary",
            )
        }));
    }

    #[test]
    fn all_modes_preserve_stage3_and_pre_activation_refusal_semantics_and_do_not_imply_activation_authorization_by_themselves(
    ) {
        {
            let fixture = fake_transept_certificate_latest_fixture(
                "transept_certificate_latest_stage3",
                "refused_now_by_stage3",
                "refused_now_by_stage3",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);

            let plan = plan_latest_choir_receipt_report(&fixture.plan_config()).unwrap();
            assert_eq!(
                plan.verdict,
                TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestPlanReady
            );
            assert_eq!(plan.mode, "plan_latest_choir_receipt");
            assert!(!plan.activation_authorized);

            let run = run_latest_choir_receipt_report(&fixture.run_config()).unwrap();
            assert_eq!(
                run.verdict,
                TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestRefusedNowByStage3
            );
            assert_eq!(run.mode, "run_latest_choir_receipt");
            assert_eq!(run.result.as_deref(), Some("refused_now_by_stage3"));
            assert!(!run.activation_authorized);

            let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyOk,
                "{:#?}",
                verify.verification_mismatches
            );
            assert_eq!(verify.mode, "verify_latest_choir_receipt");
            assert!(!verify.activation_authorized);
        }

        {
            let fixture = fake_transept_certificate_latest_fixture(
                "transept_certificate_latest_pre_activation",
                "refused_now_by_pre_activation_gate",
                "refused_now_by_pre_activation_gate",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);

            let run = run_latest_choir_receipt_report(&fixture.run_config()).unwrap();
            assert_eq!(
                run.verdict,
                TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestRefusedNowByPreActivationGate
            );
            assert_eq!(
                run.result.as_deref(),
                Some("refused_now_by_pre_activation_gate")
            );
            assert!(!run.activation_authorized);

            let verify = verify_latest_choir_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestVerifyOk,
                "{:#?}",
                verify.verification_mismatches
            );
            assert!(!verify.activation_authorized);
        }

        {
            let fixture = fake_transept_certificate_latest_fixture(
                "transept_certificate_latest_green",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_go_live_signoff",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);

            let run = run_latest_choir_receipt_report(&fixture.run_config()).unwrap();
            assert_eq!(
                run.verdict,
                TinyLivePackageChoirReceiptLatestVerdict::TinyLivePackageChoirReceiptLatestReadyForManualExecutionWhenGateTurnsGreen
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

    fn fake_transept_certificate_latest_fixture(
        label: &str,
        transept_certificate_latest_run_result: &str,
        handoff_bundle_result: &str,
    ) -> FakeTranseptCertificateLatestFixture {
        let temp_dir = temp_dir(label);
        let root = temp_dir.join("root");
        let wrapper_session_dir = temp_dir.join("culmination-receipt-latest-session");
        let output_script_path = temp_dir.join("culmination-receipt-latest.sh");
        let bin_dir = temp_dir.join("bin");
        let latest_top_session_dir = temp_dir.join("latest-clerestory-session");
        let historical_execute_frozen_session_dir =
            temp_dir.join("historical-execute-frozen-session");
        let turn_green_session_dir = temp_dir.join("historical-turn-green-session");
        let launch_packet_session_dir = temp_dir.join("historical-launch-packet-session");
        let package_dir = temp_dir.join("package");
        let install_root = temp_dir.join("install-root");
        let backend_command = temp_dir.join("backend");
        let transept_certificate_latest_plan_json_path =
            temp_dir.join("transept_certificate_latest.plan.json");
        let transept_certificate_latest_run_json_path =
            temp_dir.join("transept_certificate_latest.run.json");
        let transept_certificate_latest_verify_json_path =
            temp_dir.join("transept_certificate_latest.verify.json");
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

        let fixture = FakeTranseptCertificateLatestFixture {
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
            transept_certificate_latest_plan_json_path,
            transept_certificate_latest_run_json_path,
            transept_certificate_latest_verify_json_path,
            handoff_bundle_run_json_path,
            handoff_bundle_verify_json_path,
            handoff_bundle_session_json_path,
            handoff_bundle_status_json_path,
            frozen_controller_command,
        };

        persist_json(
            &fixture.transept_certificate_latest_plan_json_path,
            &default_transept_certificate_latest_plan_report(&fixture),
        )
        .unwrap();
        persist_json(
            &fixture.transept_certificate_latest_run_json_path,
            &default_transept_certificate_latest_run_report(
                &fixture,
                transept_certificate_latest_run_result,
            ),
        )
        .unwrap();
        persist_json(
            &fixture.transept_certificate_latest_verify_json_path,
            &default_transept_certificate_latest_verify_report(
                &fixture,
                transept_certificate_latest_run_result,
            ),
        )
        .unwrap();
        persist_json(
            &fixture.handoff_bundle_session_json_path,
            &default_choir_receipt_session(&fixture),
        )
        .unwrap();
        persist_json(
            &fixture.handoff_bundle_status_json_path,
            &default_choir_receipt_status(&fixture, handoff_bundle_result),
        )
        .unwrap();
        persist_json(
            &fixture.handoff_bundle_run_json_path,
            &default_choir_receipt_run_report(&fixture, handoff_bundle_result),
        )
        .unwrap();
        persist_json(
            &fixture.handoff_bundle_verify_json_path,
            &default_choir_receipt_verify_report(&fixture, handoff_bundle_result),
        )
        .unwrap();

        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        write_fake_transept_certificate_latest_command(
            &fixture.bin_dir.join(TRANSEPT_CERTIFICATE_LATEST_BIN),
            &fixture.transept_certificate_latest_plan_json_path,
            &fixture.transept_certificate_latest_run_json_path,
            &fixture.transept_certificate_latest_verify_json_path,
            &fixture.root,
            &paths.nested_transept_certificate_latest_session_dir,
        );
        write_fake_release_capsule_command(
            &fixture.bin_dir.join(CHOIR_RECEIPT_BIN),
            &fixture.handoff_bundle_run_json_path,
            &fixture.handoff_bundle_verify_json_path,
            &fixture.handoff_bundle_session_json_path,
            &fixture.handoff_bundle_status_json_path,
            &expected_downstream_transept_certificate_session_dir(&paths),
            &expected_downstream_decision_packet_session_dir(&paths),
            &paths.nested_choir_receipt_session_dir,
        );

        fixture
    }

    fn default_transept_certificate_latest_plan_report(
        fixture: &FakeTranseptCertificateLatestFixture,
    ) -> Value {
        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        json!({
            "generated_at": Utc::now(),
            "mode": "plan_latest_transept_certificate",
            "verdict": TRANSEPT_CERTIFICATE_LATEST_PLAN_READY,
            "reason": "latest immutable chain resolves cleanly into accepted latest-transept-certificate contract",
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
            "session_dir": paths.nested_transept_certificate_latest_session_dir.display().to_string(),
            "downstream_decision_packet_session_dir": expected_downstream_decision_packet_session_dir(&paths).display().to_string(),
            "latest_execute_verdict": "tiny_live_package_execute_latest_plan_ready",
            "decision_packet_plan_verdict": "tiny_live_package_decision_packet_plan_ready",
            "reviewed_frozen_live_cutover_controller_command_summary": fixture.frozen_controller_command,
            "clerestory_certificate_summary": "latest clerestory summary",
            "clerestory_certificate_sha256": "latest-clerestory-sha",
            "clerestory_certificate_algorithm": "sha256",
            "activation_authorized": false,
            "explicit_statement": "latest transept-certificate wrapper plan"
        })
    }

    fn canonical_transept_certificate_latest_result(result: &str) -> &str {
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

    fn default_transept_certificate_latest_run_report(
        fixture: &FakeTranseptCertificateLatestFixture,
        result: &str,
    ) -> Value {
        let result = canonical_transept_certificate_latest_result(result);
        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let (verdict, gate_verdict, gate_reason) = match result {
            "ready_for_manual_execution_when_gate_turns_green" => (
                "tiny_live_package_transept_certificate_latest_ready_for_manual_execution_when_gate_turns_green",
                Value::Null,
                Value::Null,
            ),
            "refused_now_by_stage3" => (
                "tiny_live_package_transept_certificate_latest_refused_now_by_stage3",
                json!("blocked"),
                json!("stage3 blocked"),
            ),
            "refused_now_by_pre_activation_gate" => (
                "tiny_live_package_transept_certificate_latest_refused_now_by_pre_activation_gate",
                json!("blocked"),
                json!("pre gate blocked"),
            ),
            "refused_by_missing_latest_chain" => (
                "tiny_live_package_transept_certificate_latest_refused_by_missing_latest_chain",
                Value::Null,
                Value::Null,
            ),
            "refused_by_invalid_latest_chain" => (
                "tiny_live_package_transept_certificate_latest_refused_by_invalid_latest_chain",
                Value::Null,
                Value::Null,
            ),
            _ => (
                "tiny_live_package_transept_certificate_latest_refused_by_drifted_transept_certificate_contract",
                Value::Null,
                Value::Null,
            ),
        };
        json!({
            "generated_at": Utc::now(),
            "mode": "run_latest_transept_certificate",
            "verdict": verdict,
            "reason": format!("latest-transept-certificate {result}"),
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
            "session_dir": paths.nested_transept_certificate_latest_session_dir.display().to_string(),
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
            "explicit_statement": "latest transept-certificate wrapper run"
        })
    }

    fn default_transept_certificate_latest_verify_report(
        fixture: &FakeTranseptCertificateLatestFixture,
        result: &str,
    ) -> Value {
        let mut report = default_transept_certificate_latest_run_report(fixture, result);
        report["mode"] = json!("verify_latest_transept_certificate");
        report["verdict"] = json!(TRANSEPT_CERTIFICATE_LATEST_VERIFY_OK);
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

    fn default_choir_receipt_session(fixture: &FakeTranseptCertificateLatestFixture) -> Value {
        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let transept_certificate_session_dir =
            expected_downstream_transept_certificate_session_dir(&paths);
        let substructure_certificate_session_dir = fixture
            .temp_dir
            .join("downstream-basal-receipt-substructure-certificate-session");
        let pedestal_certificate_session_dir = fixture
            .temp_dir
            .join("downstream-transept-certificate-pedestal-certificate-session");
        let plinth_receipt_session_dir = fixture
            .temp_dir
            .join("downstream-pedestal-certificate-plinth-receipt-session");
        let basal_receipt_session_dir = fixture
            .temp_dir
            .join("downstream-bedrock-certificate-basal-receipt-session");
        let registry_entry_session_dir = fixture
            .temp_dir
            .join("downstream-culmination-receipt-registry-entry-session");
        let notarization_receipt_session_dir = fixture
            .temp_dir
            .join("downstream-culmination-receipt-notarization-receipt-session");
        let provenance_certificate_session_dir = fixture
            .temp_dir
            .join("downstream-registry-entry-provenance-certificate-session");
        let attestation_seal_session_dir = fixture
            .temp_dir
            .join("downstream-provenance-certificate-attestation-seal-session");
        let foundation_receipt_session_dir = fixture
            .temp_dir
            .join("downstream-cornerstone-certificate-foundation-receipt-session");
        let bedrock_certificate_session_dir = fixture
            .temp_dir
            .join("downstream-bedrock-certificate-substructure-certificate-session");
        let activation_ticket_session_dir = fixture
            .temp_dir
            .join("downstream-provenance-certificate-activation-ticket-session");
        let review_receipt_session_dir = expected_downstream_review_receipt_session_dir(&paths);
        let handoff_bundle_session_dir = fixture
            .temp_dir
            .join("downstream-provenance-certificate-handoff-bundle-session");
        let snapshot = LatestTranseptCertificateSnapshot {
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
            "transept_certificate_session_dir": transept_certificate_session_dir.display().to_string(),
            "pedestal_certificate_session_dir": pedestal_certificate_session_dir.display().to_string(),
            "plinth_receipt_session_dir": plinth_receipt_session_dir.display().to_string(),
            "substructure_certificate_session_dir": substructure_certificate_session_dir.display().to_string(),
            "basal_receipt_session_dir": basal_receipt_session_dir.display().to_string(),
            "bedrock_certificate_session_dir": bedrock_certificate_session_dir.display().to_string(),
            "foundation_receipt_session_dir": foundation_receipt_session_dir.display().to_string(),
            "registry_entry_session_dir": registry_entry_session_dir.display().to_string(),
            "notarization_receipt_session_dir": notarization_receipt_session_dir.display().to_string(),
            "provenance_certificate_session_dir": provenance_certificate_session_dir.display().to_string(),
            "attestation_seal_session_dir": attestation_seal_session_dir.display().to_string(),
            "release_capsule_session_dir": plinth_receipt_session_dir.display().to_string(),
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
            "session_dir": choir_receipt_latest_paths(&fixture.wrapper_session_dir).nested_choir_receipt_session_dir.display().to_string(),
            "verify_transept_certificate_command_summary": verify_choir_receipt_input_command_summary(&transept_certificate_session_dir),
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
            "filing_certificate_summary": "Filing certificate identity: fixture filed certificate over final sealed registry truth.",
            "filing_certificate_sha256": "fixture-filing-certificate-sha256",
            "filing_certificate_algorithm": "sha256",
            "archive_receipt_summary": "Archive receipt identity: fixture archived closure input chain.",
            "archive_receipt_sha256": "fixture-archive-receipt-sha256",
            "archive_receipt_algorithm": "sha256",
            "closure_certificate_summary": "Closure certificate identity: fixture final closure over archived chain and reviewed controller summary.",
            "closure_certificate_sha256": "fixture-closure-certificate-sha256",
            "closure_certificate_algorithm": "sha256",
            "finality_receipt_summary": "Finality receipt identity: fixture immutable final end-state record over verified closure-certificate truth.",
            "finality_receipt_sha256": "fixture-finality-receipt-sha256",
            "finality_receipt_algorithm": "sha256",
            "consummation_record_summary": "Consummation record identity: fixture immutable final terminus record over verified finality-receipt truth.",
            "consummation_record_sha256": "fixture-consummation-record-sha256",
            "consummation_record_algorithm": "sha256",
            "completion_certificate_summary": "Completion certificate identity: fixture immutable omega seal over verified consummation-record truth.",
            "completion_certificate_sha256": "fixture-completion-certificate-sha256",
            "completion_certificate_algorithm": "sha256",
            "culmination_receipt_summary": "Culmination receipt identity: fixture immutable apex seal over verified completion-certificate truth.",
            "culmination_receipt_sha256": "fixture-culmination-receipt-sha256",
            "culmination_receipt_algorithm": "sha256",
            "summit_certificate_summary": "Summit certificate identity: fixture immutable zenith seal over verified culmination-receipt truth.",
            "summit_certificate_sha256": "fixture-summit-certificate-sha256",
            "summit_certificate_algorithm": "sha256",
            "pinnacle_receipt_summary": "Pinnacle receipt identity: fixture immutable crown seal over verified summit-certificate truth.",
            "pinnacle_receipt_sha256": "fixture-pinnacle-receipt-sha256",
            "pinnacle_receipt_algorithm": "sha256",
            "keystone_receipt_summary": "Keystone receipt identity: fixture immutable sovereign seal over verified pinnacle-receipt truth.",
            "keystone_receipt_sha256": "fixture-keystone-receipt-sha256",
            "keystone_receipt_algorithm": "sha256",
            "cornerstone_certificate_summary": "Cornerstone certificate identity: fixture immutable foundation seal over verified keystone-receipt truth.",
            "cornerstone_certificate_sha256": "fixture-cornerstone-certificate-sha256",
            "cornerstone_certificate_algorithm": "sha256",
            "foundation_receipt_summary": "Foundation receipt identity: fixture immutable diadem seal over verified cornerstone-certificate truth.",
            "foundation_receipt_sha256": "fixture-foundation-receipt-sha256",
            "foundation_receipt_algorithm": "sha256",
            "capstone_certificate_summary": "Capstone certificate identity: fixture immutable sovereign seal over verified pinnacle-receipt truth.",
            "capstone_certificate_sha256": "fixture-capstone-certificate-sha256",
            "capstone_certificate_algorithm": "sha256",
            "bedrock_certificate_summary": "Bedrock certificate identity: fixture immutable coronet seal over verified foundation-receipt truth.",
            "bedrock_certificate_sha256": "fixture-bedrock-certificate-sha256",
            "bedrock_certificate_algorithm": "sha256",
            "basal_receipt_summary": "Basal receipt identity: fixture immutable circlet seal over verified bedrock-certificate truth.",
            "basal_receipt_sha256": "fixture-basal-receipt-sha256",
            "basal_receipt_algorithm": "sha256",
            "substructure_certificate_summary": "Substructure certificate identity: fixture immutable signet seal over verified bedrock-certificate truth.",
            "substructure_certificate_sha256": "fixture-substructure-certificate-sha256",
            "substructure_certificate_algorithm": "sha256",
            "plinth_receipt_summary": "Plinth receipt identity: fixture immutable cachet seal over verified substructure-certificate truth.",
            "plinth_receipt_sha256": "fixture-plinth-receipt-sha256",
            "plinth_receipt_algorithm": "sha256",
            "pedestal_certificate_summary": "Pedestal certificate identity: fixture immutable mark seal over verified plinth-receipt truth.",
            "pedestal_certificate_sha256": "fixture-pedestal-certificate-sha256",
            "pedestal_certificate_algorithm": "sha256",
            "transept_certificate_summary": "Transept certificate identity: fixture immutable guidon-seal over verified nave-receipt truth.",
            "transept_certificate_sha256": "fixture-transept-certificate-sha256",
            "transept_certificate_algorithm": "sha256",
            "choir_receipt_summary": "Choir receipt identity: fixture immutable ensign-seal over verified transept-certificate truth.",
            "choir_receipt_sha256": "fixture-choir-receipt-sha256",
            "choir_receipt_algorithm": "sha256",
            "explicit_statement": "the verified transept-certificate session is the primary input here; this immutable choir receipt freezes one final ensign-seal identity over the fully culminated chain without enabling production execution"
        })
    }

    fn default_choir_receipt_status(
        fixture: &FakeTranseptCertificateLatestFixture,
        result: &str,
    ) -> Value {
        let result = canonical_release_capsule_result(result);
        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let substructure_certificate_session_dir = fixture
            .temp_dir
            .join("downstream-basal-receipt-substructure-certificate-session");
        let pedestal_certificate_session_dir = fixture
            .temp_dir
            .join("downstream-transept-certificate-pedestal-certificate-session");
        let plinth_receipt_session_dir = fixture
            .temp_dir
            .join("downstream-pedestal-certificate-plinth-receipt-session");
        let basal_receipt_session_dir = fixture
            .temp_dir
            .join("downstream-bedrock-certificate-basal-receipt-session");
        let foundation_receipt_session_dir = fixture
            .temp_dir
            .join("downstream-cornerstone-certificate-foundation-receipt-session");
        let bedrock_certificate_session_dir = fixture
            .temp_dir
            .join("downstream-bedrock-certificate-substructure-certificate-session");
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
            "session_dir": paths.nested_choir_receipt_session_dir.display().to_string(),
            "transept_certificate_session_dir": expected_downstream_transept_certificate_session_dir(&paths).display().to_string(),
            "pedestal_certificate_session_dir": pedestal_certificate_session_dir.display().to_string(),
            "plinth_receipt_session_dir": plinth_receipt_session_dir.display().to_string(),
            "substructure_certificate_session_dir": substructure_certificate_session_dir.display().to_string(),
            "basal_receipt_session_dir": basal_receipt_session_dir.display().to_string(),
            "bedrock_certificate_session_dir": bedrock_certificate_session_dir.display().to_string(),
            "foundation_receipt_session_dir": foundation_receipt_session_dir.display().to_string(),
            "result": result,
            "reason": reason,
            "handoff_bundle_result": if result == "ready_for_manual_execution_when_gate_turns_green" { json!("ready_for_manual_go_live_review") } else if result == "refused_now_by_stage3" { json!("refused_now_by_stage3") } else { json!("refused_now_by_pre_activation_gate") },
            "decision_packet_result": if result == "ready_for_manual_execution_when_gate_turns_green" { json!("runnable_when_gate_truth_turns_green") } else if result == "refused_now_by_stage3" { json!("refused_now_by_stage3") } else { json!("refused_now_by_pre_activation_gate") },
            "execute_frozen_result": "completed_keep_running",
            "current_pre_activation_gate_verdict": gate_verdict,
            "current_pre_activation_gate_reason": gate_reason,
            "transept_certificate_step": Value::Null,
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
            "filing_certificate_summary": "Filing certificate identity: fixture status filed certificate over final sealed registry truth.",
            "filing_certificate_sha256": "fixture-filing-certificate-sha256",
            "filing_certificate_algorithm": "sha256",
            "archive_receipt_summary": "Archive receipt identity: fixture status archived closure input chain.",
            "archive_receipt_sha256": "fixture-archive-receipt-sha256",
            "archive_receipt_algorithm": "sha256",
            "closure_certificate_summary": "Closure certificate identity: fixture status final closure over archived chain and reviewed controller summary.",
            "closure_certificate_sha256": "fixture-closure-certificate-sha256",
            "closure_certificate_algorithm": "sha256",
            "finality_receipt_summary": "Finality receipt identity: fixture status immutable final end-state record over verified closure-certificate truth.",
            "finality_receipt_sha256": "fixture-finality-receipt-sha256",
            "finality_receipt_algorithm": "sha256",
            "consummation_record_summary": "Consummation record identity: fixture status immutable final terminus record over verified finality-receipt truth.",
            "consummation_record_sha256": "fixture-consummation-record-sha256",
            "consummation_record_algorithm": "sha256",
            "completion_certificate_summary": "Completion certificate identity: fixture status immutable omega seal over verified consummation-record truth.",
            "completion_certificate_sha256": "fixture-completion-certificate-sha256",
            "completion_certificate_algorithm": "sha256",
            "culmination_receipt_summary": "Culmination receipt identity: fixture status immutable apex seal over verified completion-certificate truth.",
            "culmination_receipt_sha256": "fixture-culmination-receipt-sha256",
            "culmination_receipt_algorithm": "sha256",
            "summit_certificate_summary": "Summit certificate identity: fixture status immutable zenith seal over verified culmination-receipt truth.",
            "summit_certificate_sha256": "fixture-summit-certificate-sha256",
            "summit_certificate_algorithm": "sha256",
            "pinnacle_receipt_summary": "Pinnacle receipt identity: fixture status immutable crown seal over verified summit-certificate truth.",
            "pinnacle_receipt_sha256": "fixture-pinnacle-receipt-sha256",
            "pinnacle_receipt_algorithm": "sha256",
            "keystone_receipt_summary": "Keystone receipt identity: fixture status immutable sovereign seal over verified pinnacle-receipt truth.",
            "keystone_receipt_sha256": "fixture-keystone-receipt-sha256",
            "keystone_receipt_algorithm": "sha256",
            "cornerstone_certificate_summary": "Cornerstone certificate identity: fixture status immutable foundation seal over verified keystone-receipt truth.",
            "cornerstone_certificate_sha256": "fixture-cornerstone-certificate-sha256",
            "cornerstone_certificate_algorithm": "sha256",
            "foundation_receipt_summary": "Foundation receipt identity: fixture status immutable diadem seal over verified cornerstone-certificate truth.",
            "foundation_receipt_sha256": "fixture-foundation-receipt-sha256",
            "foundation_receipt_algorithm": "sha256",
            "capstone_certificate_summary": "Capstone certificate identity: fixture status immutable sovereign seal over verified pinnacle-receipt truth.",
            "capstone_certificate_sha256": "fixture-capstone-certificate-sha256",
            "capstone_certificate_algorithm": "sha256",
            "bedrock_certificate_summary": "Bedrock certificate identity: fixture status immutable coronet seal over verified foundation-receipt truth.",
            "bedrock_certificate_sha256": "fixture-bedrock-certificate-sha256",
            "bedrock_certificate_algorithm": "sha256",
            "basal_receipt_summary": "Basal receipt identity: fixture status immutable circlet seal over verified bedrock-certificate truth.",
            "basal_receipt_sha256": "fixture-basal-receipt-sha256",
            "basal_receipt_algorithm": "sha256",
            "substructure_certificate_summary": "Substructure certificate identity: fixture status immutable signet seal over verified bedrock-certificate truth.",
            "substructure_certificate_sha256": "fixture-substructure-certificate-sha256",
            "substructure_certificate_algorithm": "sha256",
            "plinth_receipt_summary": "Plinth receipt identity: fixture status immutable cachet seal over verified substructure-certificate truth.",
            "plinth_receipt_sha256": "fixture-plinth-receipt-sha256",
            "plinth_receipt_algorithm": "sha256",
            "pedestal_certificate_summary": "Pedestal certificate identity: fixture status immutable mark seal over verified plinth-receipt truth.",
            "pedestal_certificate_sha256": "fixture-pedestal-certificate-sha256",
            "pedestal_certificate_algorithm": "sha256",
            "transept_certificate_summary": "Transept certificate identity: fixture status immutable guidon-seal over verified nave-receipt truth.",
            "transept_certificate_sha256": "fixture-transept-certificate-sha256",
            "transept_certificate_algorithm": "sha256",
            "choir_receipt_summary": "Choir receipt identity: fixture status immutable ensign-seal over verified transept-certificate truth.",
            "choir_receipt_sha256": "fixture-choir-receipt-sha256",
            "choir_receipt_algorithm": "sha256",
            "explicit_statement": "this choir receipt is archival and read-only; it never executes or enables the frozen controller"
        })
    }

    fn default_choir_receipt_run_report(
        fixture: &FakeTranseptCertificateLatestFixture,
        result: &str,
    ) -> Value {
        let result = canonical_release_capsule_result(result);
        let paths = choir_receipt_latest_paths(&fixture.wrapper_session_dir);
        let status = default_choir_receipt_status(fixture, result);
        let choir_receipt_verdict = match result {
            "ready_for_manual_execution_when_gate_turns_green" => {
                "tiny_live_package_choir_receipt_ready_for_manual_execution_when_gate_turns_green"
            }
            "refused_now_by_stage3" => "tiny_live_package_choir_receipt_refused_now_by_stage3",
            "refused_now_by_pre_activation_gate" => {
                "tiny_live_package_choir_receipt_refused_now_by_pre_activation_gate"
            }
            _ => "tiny_live_package_choir_receipt_refused_now_by_invalid_or_drifted_contract",
        };
        let substructure_certificate_session_dir = fixture
            .temp_dir
            .join("downstream-basal-receipt-substructure-certificate-session");
        let pedestal_certificate_session_dir = fixture
            .temp_dir
            .join("downstream-transept-certificate-pedestal-certificate-session");
        let plinth_receipt_session_dir = fixture
            .temp_dir
            .join("downstream-pedestal-certificate-plinth-receipt-session");
        let basal_receipt_session_dir = fixture
            .temp_dir
            .join("downstream-bedrock-certificate-basal-receipt-session");
        let foundation_receipt_session_dir = fixture
            .temp_dir
            .join("downstream-cornerstone-certificate-foundation-receipt-session");
        let bedrock_certificate_session_dir = fixture
            .temp_dir
            .join("downstream-bedrock-certificate-substructure-certificate-session");
        json!({
            "generated_at": Utc::now(),
            "mode": "run_live_package_choir_receipt",
            "verdict": choir_receipt_verdict,
            "reason": status["reason"],
            "transept_certificate_session_dir": expected_downstream_transept_certificate_session_dir(&paths).display().to_string(),
            "pedestal_certificate_session_dir": pedestal_certificate_session_dir.display().to_string(),
            "plinth_receipt_session_dir": plinth_receipt_session_dir.display().to_string(),
            "substructure_certificate_session_dir": substructure_certificate_session_dir.display().to_string(),
            "basal_receipt_session_dir": basal_receipt_session_dir.display().to_string(),
            "bedrock_certificate_session_dir": bedrock_certificate_session_dir.display().to_string(),
            "foundation_receipt_session_dir": foundation_receipt_session_dir.display().to_string(),
            "registry_entry_session_dir": fixture.temp_dir.join("downstream-culmination-receipt-registry-entry-session").display().to_string(),
            "notarization_receipt_session_dir": fixture.temp_dir.join("downstream-culmination-receipt-notarization-receipt-session").display().to_string(),
            "provenance_certificate_session_dir": fixture.temp_dir.join("downstream-registry-entry-provenance-certificate-session").display().to_string(),
            "attestation_seal_session_dir": fixture.temp_dir.join("downstream-provenance-certificate-attestation-seal-session").display().to_string(),
            "release_capsule_session_dir": plinth_receipt_session_dir.display().to_string(),
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
            "session_dir": paths.nested_choir_receipt_session_dir.display().to_string(),
            "choir_receipt_session_path": nested_choir_receipt_session_path(&paths.nested_choir_receipt_session_dir).display().to_string(),
            "choir_receipt_status_path": nested_choir_receipt_status_path(&paths.nested_choir_receipt_session_dir).display().to_string(),
            "archived_transept_certificate_report_path": paths.nested_choir_receipt_session_dir.join("tiny_live_activation_package_choir_receipt.transept_certificate.report.json").display().to_string(),
            "result": result,
            "handoff_bundle_result": status["handoff_bundle_result"],
            "decision_packet_result": status["decision_packet_result"],
            "execute_frozen_result": status["execute_frozen_result"],
            "transept_certificate_step": status["transept_certificate_step"],
            "verify_transept_certificate_command_summary": verify_choir_receipt_input_command_summary(&expected_downstream_transept_certificate_session_dir(&paths)),
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
            "filing_certificate_summary": status["filing_certificate_summary"],
            "filing_certificate_sha256": status["filing_certificate_sha256"],
            "filing_certificate_algorithm": status["filing_certificate_algorithm"],
            "archive_receipt_summary": status["archive_receipt_summary"],
            "archive_receipt_sha256": status["archive_receipt_sha256"],
            "archive_receipt_algorithm": status["archive_receipt_algorithm"],
            "closure_certificate_summary": status["closure_certificate_summary"],
            "closure_certificate_sha256": status["closure_certificate_sha256"],
            "closure_certificate_algorithm": status["closure_certificate_algorithm"],
            "finality_receipt_summary": status["finality_receipt_summary"],
            "finality_receipt_sha256": status["finality_receipt_sha256"],
            "finality_receipt_algorithm": status["finality_receipt_algorithm"],
            "consummation_record_summary": status["consummation_record_summary"],
            "consummation_record_sha256": status["consummation_record_sha256"],
            "consummation_record_algorithm": status["consummation_record_algorithm"],
            "completion_certificate_summary": status["completion_certificate_summary"],
            "completion_certificate_sha256": status["completion_certificate_sha256"],
            "completion_certificate_algorithm": status["completion_certificate_algorithm"],
            "culmination_receipt_summary": status["culmination_receipt_summary"],
            "culmination_receipt_sha256": status["culmination_receipt_sha256"],
            "culmination_receipt_algorithm": status["culmination_receipt_algorithm"],
            "summit_certificate_summary": status["summit_certificate_summary"],
            "summit_certificate_sha256": status["summit_certificate_sha256"],
            "summit_certificate_algorithm": status["summit_certificate_algorithm"],
            "pinnacle_receipt_summary": status["pinnacle_receipt_summary"],
            "pinnacle_receipt_sha256": status["pinnacle_receipt_sha256"],
            "pinnacle_receipt_algorithm": status["pinnacle_receipt_algorithm"],
            "capstone_certificate_summary": status["capstone_certificate_summary"],
            "capstone_certificate_sha256": status["capstone_certificate_sha256"],
            "capstone_certificate_algorithm": status["capstone_certificate_algorithm"],
            "keystone_receipt_summary": status["keystone_receipt_summary"],
            "keystone_receipt_sha256": status["keystone_receipt_sha256"],
            "keystone_receipt_algorithm": status["keystone_receipt_algorithm"],
            "cornerstone_certificate_summary": status["cornerstone_certificate_summary"],
            "cornerstone_certificate_sha256": status["cornerstone_certificate_sha256"],
            "cornerstone_certificate_algorithm": status["cornerstone_certificate_algorithm"],
            "foundation_receipt_summary": status["foundation_receipt_summary"],
            "foundation_receipt_sha256": status["foundation_receipt_sha256"],
            "foundation_receipt_algorithm": status["foundation_receipt_algorithm"],
            "bedrock_certificate_summary": status["bedrock_certificate_summary"],
            "bedrock_certificate_sha256": status["bedrock_certificate_sha256"],
            "bedrock_certificate_algorithm": status["bedrock_certificate_algorithm"],
            "basal_receipt_summary": status["basal_receipt_summary"],
            "basal_receipt_sha256": status["basal_receipt_sha256"],
            "basal_receipt_algorithm": status["basal_receipt_algorithm"],
            "substructure_certificate_summary": status["substructure_certificate_summary"],
            "substructure_certificate_sha256": status["substructure_certificate_sha256"],
            "substructure_certificate_algorithm": status["substructure_certificate_algorithm"],
            "plinth_receipt_summary": status["plinth_receipt_summary"],
            "plinth_receipt_sha256": status["plinth_receipt_sha256"],
            "plinth_receipt_algorithm": status["plinth_receipt_algorithm"],
            "pedestal_certificate_summary": status["pedestal_certificate_summary"],
            "pedestal_certificate_sha256": status["pedestal_certificate_sha256"],
            "pedestal_certificate_algorithm": status["pedestal_certificate_algorithm"],
            "transept_certificate_summary": status["transept_certificate_summary"],
            "transept_certificate_sha256": status["transept_certificate_sha256"],
            "transept_certificate_algorithm": status["transept_certificate_algorithm"],
            "choir_receipt_summary": status["choir_receipt_summary"],
            "choir_receipt_sha256": status["choir_receipt_sha256"],
            "choir_receipt_algorithm": status["choir_receipt_algorithm"],
            "current_pre_activation_gate_verdict": status["current_pre_activation_gate_verdict"],
            "current_pre_activation_gate_reason": status["current_pre_activation_gate_reason"],
            "explicit_statement": status["explicit_statement"]
        })
    }

    fn default_choir_receipt_verify_report(
        fixture: &FakeTranseptCertificateLatestFixture,
        result: &str,
    ) -> Value {
        let mut report = default_choir_receipt_run_report(fixture, result);
        report["mode"] = json!("verify_live_package_choir_receipt");
        report["verdict"] = json!(CHOIR_RECEIPT_VERIFY_OK);
        report["reason"] = json!("verify ok");
        report["explicit_statement"] = json!("this verification binds one immutable choir receipt to verified transept-certificate truth, the exact reviewed frozen controller summary, the canonical chain fingerprint, the ledger seal, the registry entry, the filing-certificate identity, the archive-receipt identity, the closure-certificate identity, the finality-receipt identity, the consummation-record identity, the completion-certificate identity, the culmination-receipt identity, the summit-certificate identity, the pinnacle-receipt identity, the capstone-certificate identity, the keystone-receipt identity, the cornerstone-certificate identity, the foundation-receipt identity, the bedrock-certificate identity, the basal-receipt identity, the substructure-certificate identity, the plinth-receipt identity, the pedestal-certificate identity, the dais-receipt identity, the rostrum-certificate identity, the podium-receipt identity, the lectern-certificate identity, the pulpit-receipt identity, the chancel-certificate identity, the apse-receipt identity, the sanctuary-certificate identity, the nave-receipt identity, and the transept-certificate identity");
        report
    }

    fn write_fake_transept_certificate_latest_command(
        path: &Path,
        plan_json_path: &Path,
        run_json_path: &Path,
        verify_json_path: &Path,
        root: &Path,
        session_dir: &Path,
    ) {
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --session-dir {} \"*) : ;;\n  *) echo 'unexpected latest-transept-certificate session-dir' >&2; exit 1;;\nesac\nmode=''\ncase \" $* \" in\n  *\" --plan-latest-transept-certificate \"*) mode='plan';;\n  *\" --run-latest-transept-certificate \"*) mode='run';;\n  *\" --verify-latest-transept-certificate \"*) mode='verify';;\n  *) echo 'unexpected latest-transept-certificate mode' >&2; exit 1;;\nesac\ncase \"$mode\" in\n  plan|run)\n    case \" $* \" in\n      *\" --root {} \"*) : ;;\n      *) echo 'unexpected root' >&2; exit 1;;\n    esac\n    ;;\nesac\ncase \"$mode\" in\n  plan) cat '{}';;\n  run) mkdir -p '{}'; cp '{}' '{}/tiny_live_activation_package_transept_certificate_latest.report.json'; cat '{}';;\n  verify) cat '{}';;\n  *) exit 1;;\nesac\n",
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

    fn overwrite_fake_transept_certificate_latest_command(
        path: &Path,
        plan_json_path: &Path,
        run_json_path: &Path,
        verify_json_path: &Path,
        plan_failure: Option<&str>,
        run_failure: Option<&str>,
        verify_failure: Option<&str>,
        root: &Path,
        session_dir: &Path,
    ) {
        let plan_branch = plan_failure.map_or_else(
            || format!("cat '{}'", plan_json_path.display()),
            |message| format!("echo '{}' >&2; exit 1", message.replace('\'', "'\"'\"'")),
        );
        let run_branch = run_failure.map_or_else(
            || {
                format!(
                    "mkdir -p '{}'; cat '{}'",
                    session_dir.display(),
                    run_json_path.display()
                )
            },
            |message| format!("echo '{}' >&2; exit 1", message.replace('\'', "'\"'\"'")),
        );
        let verify_branch = verify_failure.map_or_else(
            || format!("cat '{}'", verify_json_path.display()),
            |message| format!("echo '{}' >&2; exit 1", message.replace('\'', "'\"'\"'")),
        );
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --session-dir {} \"*) : ;;\n  *) echo 'unexpected latest-transept-certificate session-dir' >&2; exit 1;;\nesac\nmode=''\ncase \" $* \" in\n  *\" --plan-latest-transept-certificate \"*) mode='plan';;\n  *\" --run-latest-transept-certificate \"*) mode='run';;\n  *\" --verify-latest-transept-certificate \"*) mode='verify';;\n  *) echo 'unexpected latest-transept-certificate mode' >&2; exit 1;;\nesac\ncase \"$mode\" in\n  plan|run)\n    case \" $* \" in\n      *\" --root {} \"*) : ;;\n      *) echo 'unexpected root' >&2; exit 1;;\n    esac\n    ;;\nesac\ncase \"$mode\" in\n  plan) {};;\n  run) {};;\n  verify) {};;\n  *) exit 1;;\nesac\n",
            session_dir.display(),
            root.display(),
            plan_branch,
            run_branch,
            verify_branch,
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
        let nested_session_path = nested_choir_receipt_session_path(session_dir);
        let nested_status_path = nested_choir_receipt_status_path(session_dir);
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --transept-certificate-session-dir {} \"*) : ;;\n  *) echo 'unexpected transept-certificate-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --confirm-decision-packet-session-dir {} \"*) : ;;\n  *) echo 'unexpected confirm-decision-packet-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --session-dir {} \"*) : ;;\n  *) echo 'unexpected choir-receipt session-dir' >&2; exit 1;;\nesac\nmode=''\ncase \" $* \" in\n  *\" --run-live-package-choir-receipt \"*) mode='run';;\n  *\" --verify-live-package-choir-receipt \"*) mode='verify';;\n  *) echo 'unexpected choir-receipt mode' >&2; exit 1;;\nesac\ncase \"$mode\" in\n  run) mkdir -p '{}'; cp '{}' '{}'; cp '{}' '{}'; cat '{}';;\n  verify) cat '{}';;\n  *) exit 1;;\nesac\n",
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

    fn overwrite_fake_release_capsule_command(
        path: &Path,
        run_json_path: &Path,
        verify_json_path: &Path,
        session_json_path: &Path,
        status_json_path: &Path,
        run_failure: Option<&str>,
        verify_failure: Option<&str>,
        release_capsule_session_dir: &Path,
        decision_packet_session_dir: &Path,
        session_dir: &Path,
    ) {
        let nested_session_path = nested_choir_receipt_session_path(session_dir);
        let nested_status_path = nested_choir_receipt_status_path(session_dir);
        let run_branch = run_failure.map_or_else(
            || {
                format!(
                    "mkdir -p '{}'; cp '{}' '{}'; cp '{}' '{}'; cat '{}'",
                    session_dir.display(),
                    session_json_path.display(),
                    nested_session_path.display(),
                    status_json_path.display(),
                    nested_status_path.display(),
                    run_json_path.display(),
                )
            },
            |message| format!("echo '{}' >&2; exit 1", message.replace('\'', "'\"'\"'")),
        );
        let verify_branch = verify_failure.map_or_else(
            || format!("cat '{}'", verify_json_path.display()),
            |message| format!("echo '{}' >&2; exit 1", message.replace('\'', "'\"'\"'")),
        );
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --transept-certificate-session-dir {} \"*) : ;;\n  *) echo 'unexpected transept-certificate-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --confirm-decision-packet-session-dir {} \"*) : ;;\n  *) echo 'unexpected confirm-decision-packet-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --session-dir {} \"*) : ;;\n  *) echo 'unexpected choir-receipt session-dir' >&2; exit 1;;\nesac\nmode=''\ncase \" $* \" in\n  *\" --run-live-package-choir-receipt \"*) mode='run';;\n  *\" --verify-live-package-choir-receipt \"*) mode='verify';;\n  *) echo 'unexpected choir-receipt mode' >&2; exit 1;;\nesac\ncase \"$mode\" in\n  run) {};;\n  verify) {};;\n  *) exit 1;;\nesac\n",
            release_capsule_session_dir.display(),
            decision_packet_session_dir.display(),
            session_dir.display(),
            run_branch,
            verify_branch,
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
