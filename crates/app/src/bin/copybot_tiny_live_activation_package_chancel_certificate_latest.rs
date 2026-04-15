#![recursion_limit = "512"]

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const USAGE: &str = "usage: copybot_tiny_live_activation_package_chancel_certificate_latest --root <path> [--session-dir <path>] [--output <path>] [--json] (--plan-latest-chancel-certificate | --render-latest-chancel-certificate-script | --run-latest-chancel-certificate | --verify-latest-chancel-certificate)";
const CHANCEL_CERTIFICATE_LATEST_SESSION_VERSION: &str = "1";
const CHANCEL_CERTIFICATE_LATEST_STATUS_VERSION: &str = "1";
const CHANCEL_CERTIFICATE_LATEST_BIN: &str =
    "copybot_tiny_live_activation_package_chancel_certificate_latest";
const PULPIT_RECEIPT_LATEST_BIN: &str =
    "copybot_tiny_live_activation_package_pulpit_receipt_latest";
const CHANCEL_CERTIFICATE_BIN: &str = "copybot_tiny_live_activation_package_chancel_certificate";
const PULPIT_RECEIPT_LATEST_PLAN_READY: &str = "tiny_live_package_pulpit_receipt_latest_plan_ready";
const PULPIT_RECEIPT_LATEST_VERIFY_OK: &str = "tiny_live_package_pulpit_receipt_latest_verify_ok";
const CHANCEL_CERTIFICATE_VERIFY_OK: &str = "tiny_live_package_chancel_certificate_verify_ok";
const TOP_ACCEPTED_PACKAGE_STEP: &str = "clerestory_certificate";
const PLANNING_SAFE_STATEMENT: &str =
    "this latest-chancel-certificate handoff binds the latest persisted immutable package chain to the accepted chancel-certificate contract, but it never overrides Stage 3 or pre-activation refusal truth, it stays archival and read-only, it never runs the frozen live cutover controller by itself, and activation_authorized remains false";

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
    PlanLatestChancelCertificate,
    RenderLatestChancelCertificateScript,
    RunLatestChancelCertificate,
    VerifyLatestChancelCertificate,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageChancelCertificateLatestVerdict {
    TinyLivePackageChancelCertificateLatestPlanReady,
    TinyLivePackageChancelCertificateLatestScriptRendered,
    TinyLivePackageChancelCertificateLatestRefusedByMissingLatestChain,
    TinyLivePackageChancelCertificateLatestRefusedByInvalidLatestChain,
    TinyLivePackageChancelCertificateLatestRefusedByDriftedChancelCertificateContract,
    TinyLivePackageChancelCertificateLatestRefusedNowByStage3,
    TinyLivePackageChancelCertificateLatestRefusedNowByPreActivationGate,
    TinyLivePackageChancelCertificateLatestReadyForManualExecutionWhenGateTurnsGreen,
    TinyLivePackageChancelCertificateLatestVerifyOk,
    TinyLivePackageChancelCertificateLatestVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LivePackageChancelCertificateLatestResult {
    RefusedByMissingLatestChain,
    RefusedByInvalidLatestChain,
    RefusedByDriftedChancelCertificateContract,
    RefusedNowByStage3,
    RefusedNowByPreActivationGate,
    ReadyForManualExecutionWhenGateTurnsGreen,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageChancelCertificateLatestStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageChancelCertificateLatestSession {
    session_version: String,
    planned_at: DateTime<Utc>,
    root: String,
    session_dir: String,
    nested_pulpit_receipt_latest_session_dir: String,
    nested_chancel_certificate_session_dir: String,
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
    resolve_latest_pulpit_receipt_command_summary: String,
    run_latest_chancel_certificate_command_summary: String,
    verify_latest_chancel_certificate_command_summary: String,
    downstream_chancel_certificate_command_summary: String,
    verify_chancel_certificate_command_summary: String,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    clerestory_certificate_summary: Option<String>,
    clerestory_certificate_sha256: Option<String>,
    clerestory_certificate_algorithm: Option<String>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageChancelCertificateLatestStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    root: String,
    session_dir: String,
    latest_top_step_name: Option<String>,
    latest_top_session_dir: Option<String>,
    historical_execute_frozen_session_dir: Option<String>,
    turn_green_session_dir: Option<String>,
    launch_packet_session_dir: Option<String>,
    nested_pulpit_receipt_latest_session_dir: String,
    nested_chancel_certificate_session_dir: String,
    downstream_decision_packet_session_dir: String,
    result: LivePackageChancelCertificateLatestResult,
    reason: String,
    latest_pulpit_receipt_plan_verdict: Option<String>,
    latest_pulpit_receipt_verdict: Option<String>,
    chancel_certificate_verdict: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    latest_pulpit_receipt_plan_step: Option<PackageChancelCertificateLatestStepArtifact>,
    latest_pulpit_receipt_step: Option<PackageChancelCertificateLatestStepArtifact>,
    chancel_certificate_step: Option<PackageChancelCertificateLatestStepArtifact>,
    operator_next_action_summary: String,
    activation_authorized: bool,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackageChancelCertificateLatestPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    report_path: PathBuf,
    latest_pulpit_receipt_plan_report_path: PathBuf,
    latest_pulpit_receipt_report_path: PathBuf,
    chancel_certificate_report_path: PathBuf,
    nested_pulpit_receipt_latest_session_dir: PathBuf,
    nested_chancel_certificate_session_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageChancelCertificateLatestReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageChancelCertificateLatestVerdict,
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
    chancel_certificate_latest_session_path: Option<String>,
    chancel_certificate_latest_status_path: Option<String>,
    chancel_certificate_latest_report_path: Option<String>,
    latest_pulpit_receipt_plan_report_path: Option<String>,
    latest_pulpit_receipt_report_path: Option<String>,
    chancel_certificate_report_path: Option<String>,
    nested_pulpit_receipt_latest_session_dir: Option<String>,
    nested_chancel_certificate_session_dir: Option<String>,
    downstream_decision_packet_session_dir: Option<String>,
    latest_pulpit_receipt_plan_verdict: Option<String>,
    latest_pulpit_receipt_verdict: Option<String>,
    chancel_certificate_verdict: Option<String>,
    result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    resolve_latest_pulpit_receipt_command_summary: Option<String>,
    run_latest_chancel_certificate_command_summary: Option<String>,
    verify_latest_chancel_certificate_command_summary: Option<String>,
    downstream_chancel_certificate_command_summary: Option<String>,
    verify_chancel_certificate_command_summary: Option<String>,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    clerestory_certificate_summary: Option<String>,
    clerestory_certificate_sha256: Option<String>,
    clerestory_certificate_algorithm: Option<String>,
    verification_mismatches: Vec<String>,
    activation_authorized: bool,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct LatestPulpitReceiptSnapshot {
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
struct ResolvedLatestPulpitReceipt {
    snapshot: LatestPulpitReceiptSnapshot,
    latest_pulpit_receipt_plan_raw: Value,
    latest_pulpit_receipt_plan: PulpitReceiptLatestReportView,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResolutionFailureKind {
    MissingLatestChain,
    InvalidLatestChain,
    DriftedPulpitReceiptContract,
}

#[derive(Debug, Clone)]
struct LatestPulpitReceiptResolutionFailure {
    kind: ResolutionFailureKind,
    reason: String,
    snapshot: Option<LatestPulpitReceiptSnapshot>,
    latest_pulpit_receipt_plan_raw: Option<Value>,
    latest_pulpit_receipt_plan_verdict: Option<String>,
    latest_pulpit_receipt_plan_reason: Option<String>,
    latest_pulpit_receipt_plan_generated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Deserialize)]
struct PulpitReceiptLatestReportView {
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
struct ChancelCertificateReportView {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: String,
    reason: String,
    pulpit_receipt_session_dir: String,
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
    chancel_certificate_session_path: Option<String>,
    chancel_certificate_status_path: Option<String>,
    archived_pulpit_receipt_report_path: Option<String>,
    result: Option<String>,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    pulpit_receipt_step: Option<PackageChancelCertificateLatestStepArtifact>,
    verify_pulpit_receipt_command_summary: Option<String>,
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
    pulpit_receipt_summary: Option<String>,
    pulpit_receipt_sha256: Option<String>,
    pulpit_receipt_algorithm: Option<String>,
    chancel_certificate_summary: Option<String>,
    chancel_certificate_sha256: Option<String>,
    chancel_certificate_algorithm: Option<String>,
    verification_mismatches: Option<Vec<String>>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ChancelCertificateSessionView {
    pulpit_receipt_session_dir: String,
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
    verify_pulpit_receipt_command_summary: String,
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
    pulpit_receipt_summary: String,
    pulpit_receipt_sha256: String,
    pulpit_receipt_algorithm: String,
    chancel_certificate_summary: String,
    chancel_certificate_sha256: String,
    chancel_certificate_algorithm: String,
    explicit_statement: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ChancelCertificateStatusView {
    session_dir: String,
    pulpit_receipt_session_dir: String,
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
    pulpit_receipt_step: Option<PackageChancelCertificateLatestStepArtifact>,
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
    pulpit_receipt_summary: String,
    pulpit_receipt_sha256: String,
    pulpit_receipt_algorithm: String,
    chancel_certificate_summary: String,
    chancel_certificate_sha256: String,
    chancel_certificate_algorithm: String,
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
            "--plan-latest-chancel-certificate" => {
                set_mode(&mut mode, Mode::PlanLatestChancelCertificate, arg.as_str())?
            }
            "--render-latest-chancel-certificate-script" => set_mode(
                &mut mode,
                Mode::RenderLatestChancelCertificateScript,
                arg.as_str(),
            )?,
            "--run-latest-chancel-certificate" => {
                set_mode(&mut mode, Mode::RunLatestChancelCertificate, arg.as_str())?
            }
            "--verify-latest-chancel-certificate" => set_mode(
                &mut mode,
                Mode::VerifyLatestChancelCertificate,
                arg.as_str(),
            )?,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown arg {other}; {USAGE}"),
        }
    }

    let Some(mode) = mode else {
        return Ok(None);
    };
    if matches!(
        mode,
        Mode::PlanLatestChancelCertificate
            | Mode::RenderLatestChancelCertificateScript
            | Mode::RunLatestChancelCertificate
    ) && root.is_none()
    {
        bail!("missing --root");
    }
    if matches!(mode, Mode::RenderLatestChancelCertificateScript) && output_path.is_none() {
        bail!("missing --output for render mode");
    }
    if matches!(
        mode,
        Mode::RunLatestChancelCertificate | Mode::VerifyLatestChancelCertificate
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
        Mode::PlanLatestChancelCertificate => plan_latest_chancel_certificate_report(&config)?,
        Mode::RenderLatestChancelCertificateScript => {
            render_latest_chancel_certificate_script_report(&config)?
        }
        Mode::RunLatestChancelCertificate => run_latest_chancel_certificate_report(&config)?,
        Mode::VerifyLatestChancelCertificate => verify_latest_chancel_certificate_report(&config)?,
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_report_lines(&report))
    }
}

fn plan_latest_chancel_certificate_report(
    config: &Config,
) -> Result<PackageChancelCertificateLatestReport> {
    let root = config
        .root
        .as_ref()
        .ok_or_else(|| anyhow!("missing --root"))?;
    let suggested_session_dir = config.session_dir.clone().unwrap_or_else(|| {
        root.join("tiny_live_activation_package_chancel_certificate_latest.session")
    });
    let paths = chancel_certificate_latest_paths(&suggested_session_dir);

    match resolve_latest_pulpit_receipt(root, &paths) {
        Ok(resolved) => Ok(plan_report_from_resolution(
            "plan_latest_chancel_certificate",
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestPlanReady,
            format!(
                "latest immutable {} chain under {} resolves to exact latest-pulpit-receipt lineage via accepted latest-pulpit-receipt session {}, exact decision-packet confirmation anchor {}, and the accepted chancel-certificate contract remains current",
                resolved.snapshot.latest_top_step_name,
                root.display(),
                paths.nested_pulpit_receipt_latest_session_dir.display(),
                resolved.snapshot.downstream_decision_packet_session_dir.display(),
            ),
            root,
            &suggested_session_dir,
            &paths,
            &resolved,
        )),
        Err(failure) => Ok(report_from_resolution_failure(
            "plan_latest_chancel_certificate",
            root,
            &suggested_session_dir,
            &paths,
            &failure,
        )),
    }
}

fn render_latest_chancel_certificate_script_report(
    config: &Config,
) -> Result<PackageChancelCertificateLatestReport> {
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
    let paths = chancel_certificate_latest_paths(&suggested_session_dir);

    match resolve_latest_pulpit_receipt(root, &paths) {
        Ok(resolved) => {
            write_script(
                output_path,
                &format!(
                    "#!/usr/bin/env bash\nset -euo pipefail\n\n{run_latest}\n{verify_latest}\n{run_chancel_certificate}\n{verify_chancel_certificate}\n",
                    run_latest = run_latest_chancel_certificate_command_summary(
                        root,
                        &suggested_session_dir,
                    ),
                    verify_latest =
                        verify_latest_chancel_certificate_command_summary(&suggested_session_dir),
                    run_chancel_certificate = downstream_chancel_certificate_command_summary(
                        &resolved.snapshot,
                        &expected_downstream_pulpit_receipt_session_dir(&paths),
                        &paths.nested_chancel_certificate_session_dir,
                    ),
                    verify_chancel_certificate = verify_chancel_certificate_command_summary(
                        &resolved.snapshot,
                        &expected_downstream_pulpit_receipt_session_dir(&paths),
                        &paths.nested_chancel_certificate_session_dir,
                    ),
                ),
            )?;
            Ok(plan_report_from_resolution(
                "render_latest_chancel_certificate_script",
                TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestScriptRendered,
                format!(
                    "rendered latest-chancel-certificate handoff script to {} using the accepted latest-pulpit-receipt lineage and the accepted chancel-certificate contract",
                    output_path.display()
                ),
                root,
                &suggested_session_dir,
                &paths,
                &resolved,
            ))
        }
        Err(failure) => Ok(report_from_resolution_failure(
            "render_latest_chancel_certificate_script",
            root,
            &suggested_session_dir,
            &paths,
            &failure,
        )),
    }
}

fn run_latest_chancel_certificate_report(
    config: &Config,
) -> Result<PackageChancelCertificateLatestReport> {
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
            "refusing to overwrite existing latest-chancel-certificate session dir {}",
            session_dir.display()
        );
    }
    let paths = chancel_certificate_latest_paths(session_dir);
    create_clean_session_dir(session_dir, "latest-chancel-certificate")?;

    let resolution = resolve_latest_pulpit_receipt(root, &paths);
    match resolution {
        Ok(resolved) => {
            run_latest_chancel_certificate_with_resolution(root, session_dir, &paths, &resolved)
        }
        Err(failure) => run_latest_chancel_certificate_failure(root, session_dir, &paths, &failure),
    }
}

fn verify_latest_chancel_certificate_report(
    config: &Config,
) -> Result<PackageChancelCertificateLatestReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for verify mode"))?;
    let paths = chancel_certificate_latest_paths(session_dir);

    let session: PackageChancelCertificateLatestSession = match load_json(&paths.session_path) {
        Ok(value) => value,
        Err(error) => {
            return Ok(
                verify_invalid_chancel_certificate_latest_report_without_session(
                    session_dir,
                    vec![format!(
                        "failed reading latest-chancel-certificate session artifact {}: {error}",
                        paths.session_path.display()
                    )],
                ),
            )
        }
    };
    let status: PackageChancelCertificateLatestStatus = match load_json(&paths.status_path) {
        Ok(value) => value,
        Err(error) => {
            return Ok(
                verify_invalid_chancel_certificate_latest_report_without_session(
                    session_dir,
                    vec![format!(
                        "failed reading latest-chancel-certificate status artifact {}: {error}",
                        paths.status_path.display()
                    )],
                ),
            )
        }
    };
    let stored_report: PackageChancelCertificateLatestReport = match load_json(&paths.report_path) {
        Ok(value) => value,
        Err(error) => {
            return Ok(
                verify_invalid_chancel_certificate_latest_report_without_session(
                    session_dir,
                    vec![format!(
                        "failed reading latest-chancel-certificate report artifact {}: {error}",
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
        "latest-chancel-certificate root consistency",
        &mut mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "latest-chancel-certificate session session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.session_dir,
        &session_dir.display().to_string(),
        "latest-chancel-certificate status session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "latest-chancel-certificate report session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.nested_pulpit_receipt_latest_session_dir,
        &paths
            .nested_pulpit_receipt_latest_session_dir
            .display()
            .to_string(),
        "latest-chancel-certificate session nested_pulpit_receipt_latest_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.nested_pulpit_receipt_latest_session_dir,
        &paths
            .nested_pulpit_receipt_latest_session_dir
            .display()
            .to_string(),
        "latest-chancel-certificate status nested_pulpit_receipt_latest_session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report
            .nested_pulpit_receipt_latest_session_dir
            .as_deref(),
        Some(
            paths
                .nested_pulpit_receipt_latest_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "latest-chancel-certificate report nested_pulpit_receipt_latest_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.nested_chancel_certificate_session_dir,
        &paths
            .nested_chancel_certificate_session_dir
            .display()
            .to_string(),
        "latest-chancel-certificate session nested_chancel_certificate_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.nested_chancel_certificate_session_dir,
        &paths
            .nested_chancel_certificate_session_dir
            .display()
            .to_string(),
        "latest-chancel-certificate status nested_chancel_certificate_session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report
            .nested_chancel_certificate_session_dir
            .as_deref(),
        Some(
            paths
                .nested_chancel_certificate_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "latest-chancel-certificate report nested_chancel_certificate_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.downstream_decision_packet_session_dir,
        &expected_downstream_decision_packet_session_dir(&paths)
            .display()
            .to_string(),
        "latest-chancel-certificate session downstream_decision_packet_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.downstream_decision_packet_session_dir,
        &expected_downstream_decision_packet_session_dir(&paths)
            .display()
            .to_string(),
        "latest-chancel-certificate status downstream_decision_packet_session_dir",
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
        "latest-chancel-certificate report downstream_decision_packet_session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report
            .chancel_certificate_latest_session_path
            .as_deref(),
        Some(paths.session_path.display().to_string().as_str()),
        "latest-chancel-certificate report session path",
        &mut mismatches,
    );
    compare_option_string(
        stored_report
            .chancel_certificate_latest_status_path
            .as_deref(),
        Some(paths.status_path.display().to_string().as_str()),
        "latest-chancel-certificate report status path",
        &mut mismatches,
    );
    compare_option_string(
        stored_report
            .chancel_certificate_latest_report_path
            .as_deref(),
        Some(paths.report_path.display().to_string().as_str()),
        "latest-chancel-certificate report report path",
        &mut mismatches,
    );
    compare_string(
        &session.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "latest-chancel-certificate session explicit_statement",
        &mut mismatches,
    );
    compare_string(
        &status.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "latest-chancel-certificate status explicit_statement",
        &mut mismatches,
    );
    compare_string(
        &stored_report.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "latest-chancel-certificate report explicit_statement",
        &mut mismatches,
    );
    compare_string(
        &stored_report.mode,
        "run_latest_chancel_certificate",
        "latest-chancel-certificate stored report mode",
        &mut mismatches,
    );
    compare_string(
        &serialize_enum(&stored_report.verdict),
        &serialize_enum(&verdict_for_result(status.result)),
        "latest-chancel-certificate stored report verdict",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.result.as_deref(),
        Some(serialize_enum(&status.result).as_str()),
        "latest-chancel-certificate stored report result consistency",
        &mut mismatches,
    );
    compare_string(
        &stored_report.reason,
        &status.reason,
        "latest-chancel-certificate stored report reason consistency",
        &mut mismatches,
    );
    if stored_report.activation_authorized != status.activation_authorized {
        mismatches.push(format!(
            "latest-chancel-certificate stored report activation_authorized {} does not match status {}",
            stored_report.activation_authorized, status.activation_authorized
        ));
    }
    compare_option_string(
        stored_report.latest_pulpit_receipt_plan_verdict.as_deref(),
        status.latest_pulpit_receipt_plan_verdict.as_deref(),
        "stored report latest_pulpit_receipt_plan_verdict",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.latest_pulpit_receipt_verdict.as_deref(),
        status.latest_pulpit_receipt_verdict.as_deref(),
        "stored report latest_pulpit_receipt_verdict",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.chancel_certificate_verdict.as_deref(),
        status.chancel_certificate_verdict.as_deref(),
        "stored report chancel_certificate_verdict",
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

    let resolution = resolve_latest_pulpit_receipt(&root, &paths);
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

            let stored_latest_plan: PulpitReceiptLatestReportView = load_required_step_json(
                &status.latest_pulpit_receipt_plan_step,
                &paths.latest_pulpit_receipt_plan_report_path,
                "latest_pulpit_receipt_plan_step",
                &mut mismatches,
            )?;
            compare_step_artifact_against_report_metadata(
                status.latest_pulpit_receipt_plan_step.as_ref(),
                &paths.latest_pulpit_receipt_plan_report_path,
                "plan_latest_pulpit_receipt",
                &stored_latest_plan.verdict,
                &stored_latest_plan.reason,
                stored_latest_plan.generated_at,
                "stored latest_pulpit_receipt_plan_step",
                &mut mismatches,
            );
            compare_pulpit_receipt_latest_plan_against_snapshot(
                &stored_latest_plan,
                &resolved.snapshot,
                &paths.nested_pulpit_receipt_latest_session_dir,
                &mut mismatches,
            );
            compare_option_string(
                status.latest_pulpit_receipt_plan_verdict.as_deref(),
                Some(stored_latest_plan.verdict.as_str()),
                "stored status latest_pulpit_receipt_plan_verdict",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.latest_pulpit_receipt_plan_verdict.as_deref(),
                Some(stored_latest_plan.verdict.as_str()),
                "stored report latest_pulpit_receipt_plan_verdict",
                &mut mismatches,
            );

            let stored_latest_run: PulpitReceiptLatestReportView = load_required_step_json(
                &status.latest_pulpit_receipt_step,
                &paths.latest_pulpit_receipt_report_path,
                "latest_pulpit_receipt_step",
                &mut mismatches,
            )?;
            compare_step_artifact_against_report_metadata(
                status.latest_pulpit_receipt_step.as_ref(),
                &paths.latest_pulpit_receipt_report_path,
                "run_latest_pulpit_receipt",
                &stored_latest_run.verdict,
                &stored_latest_run.reason,
                stored_latest_run.generated_at,
                "stored latest_pulpit_receipt_step",
                &mut mismatches,
            );
            compare_pulpit_receipt_latest_run_against_snapshot(
                &stored_latest_run,
                &resolved.snapshot,
                &paths.nested_pulpit_receipt_latest_session_dir,
                &mut mismatches,
            );
            compare_option_string(
                status.latest_pulpit_receipt_verdict.as_deref(),
                Some(stored_latest_run.verdict.as_str()),
                "stored status latest_pulpit_receipt_verdict",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.latest_pulpit_receipt_verdict.as_deref(),
                Some(stored_latest_run.verdict.as_str()),
                "stored report latest_pulpit_receipt_verdict",
                &mut mismatches,
            );

            let actual_latest_run: PulpitReceiptLatestReportView = match load_json(
                &nested_pulpit_receipt_latest_report_path(
                    &paths.nested_pulpit_receipt_latest_session_dir,
                ),
            ) {
                Ok(value) => value,
                Err(error) => {
                    mismatches.push(format!(
                        "failed loading actual nested latest-pulpit-receipt report under {}: {error}",
                        paths.nested_pulpit_receipt_latest_session_dir.display()
                    ));
                    fallback_pulpit_receipt_latest_copy()
                }
            };
            compare_pulpit_receipt_latest_copy_matches_actual(
                &stored_latest_run,
                &actual_latest_run,
                &mut mismatches,
            );

            let latest_verify_raw = match run_json_command(
                PULPIT_RECEIPT_LATEST_BIN,
                &latest_pulpit_receipt_verify_args(&paths.nested_pulpit_receipt_latest_session_dir),
            ) {
                Ok(value) => value,
                Err(error) => {
                    mismatches.push(format!(
                        "failed verifying nested latest-pulpit-receipt session under {}: {error}",
                        paths.nested_pulpit_receipt_latest_session_dir.display()
                    ));
                    Value::Null
                }
            };
            if !latest_verify_raw.is_null() {
                let latest_verify: PulpitReceiptLatestReportView =
                    serde_json::from_value(latest_verify_raw)
                        .context("failed parsing nested latest-pulpit-receipt verify json")?;
                if latest_verify.verdict != PULPIT_RECEIPT_LATEST_VERIFY_OK {
                    mismatches.push(format!(
                        "nested latest-pulpit-receipt verification is non-green: {}",
                        latest_verify.reason
                    ));
                }
            }

            let stored_handoff_bundle: ChancelCertificateReportView = load_required_step_json(
                &status.chancel_certificate_step,
                &paths.chancel_certificate_report_path,
                "chancel_certificate_step",
                &mut mismatches,
            )?;
            compare_step_artifact_against_report_metadata(
                status.chancel_certificate_step.as_ref(),
                &paths.chancel_certificate_report_path,
                "run_live_package_chancel_certificate",
                &stored_handoff_bundle.verdict,
                &stored_handoff_bundle.reason,
                stored_handoff_bundle.generated_at,
                "stored chancel_certificate_step",
                &mut mismatches,
            );
            compare_chancel_certificate_run_against_snapshot(
                &stored_handoff_bundle,
                &resolved.snapshot,
                &expected_downstream_pulpit_receipt_session_dir(&paths),
                &paths.nested_chancel_certificate_session_dir,
                &mut mismatches,
            );
            compare_option_string(
                status.chancel_certificate_verdict.as_deref(),
                Some(stored_handoff_bundle.verdict.as_str()),
                "stored status chancel_certificate_verdict",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.chancel_certificate_verdict.as_deref(),
                Some(stored_handoff_bundle.verdict.as_str()),
                "stored report chancel_certificate_verdict",
                &mut mismatches,
            );

            let nested_handoff_bundle_session: ChancelCertificateSessionView =
                match load_json(&nested_chancel_certificate_session_path(
                    &paths.nested_chancel_certificate_session_dir,
                )) {
                    Ok(value) => value,
                    Err(error) => {
                        mismatches.push(format!(
                            "failed reading nested chancel-certificate session under {}: {error}",
                            paths.nested_chancel_certificate_session_dir.display()
                        ));
                        fallback_notarization_receipt_session(
                            &resolved.snapshot,
                            &paths.nested_chancel_certificate_session_dir,
                        )
                    }
                };
            let nested_handoff_bundle_status: ChancelCertificateStatusView =
                match load_json(&nested_chancel_certificate_status_path(
                    &paths.nested_chancel_certificate_session_dir,
                )) {
                    Ok(value) => value,
                    Err(error) => {
                        mismatches.push(format!(
                            "failed reading nested chancel-certificate status under {}: {error}",
                            paths.nested_chancel_certificate_session_dir.display()
                        ));
                        fallback_notarization_receipt_status(
                            &resolved.snapshot,
                            &paths.nested_chancel_certificate_session_dir,
                        )
                    }
                };
            compare_chancel_certificate_copy_against_nested_truth(
                &stored_handoff_bundle,
                &nested_handoff_bundle_session,
                &nested_handoff_bundle_status,
                &mut mismatches,
            );

            let handoff_bundle_verify_raw = match run_json_command(
                CHANCEL_CERTIFICATE_BIN,
                &chancel_certificate_verify_args(
                    &resolved.snapshot,
                    &expected_downstream_pulpit_receipt_session_dir(&paths),
                    &paths.nested_chancel_certificate_session_dir,
                ),
            ) {
                Ok(value) => value,
                Err(error) => {
                    mismatches.push(format!(
                        "failed verifying nested chancel-certificate session under {}: {error}",
                        paths.nested_chancel_certificate_session_dir.display()
                    ));
                    Value::Null
                }
            };
            if !handoff_bundle_verify_raw.is_null() {
                let handoff_bundle_verify: ChancelCertificateReportView =
                    serde_json::from_value(handoff_bundle_verify_raw)
                        .context("failed parsing nested chancel-certificate verify json")?;
                compare_option_string(
                    stored_handoff_bundle.registry_entry_session_dir.as_deref(),
                    handoff_bundle_verify.registry_entry_session_dir.as_deref(),
                    "stored chancel-certificate copy registry_entry_session_dir against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.notarization_receipt_session_dir.as_deref(),
                    handoff_bundle_verify.notarization_receipt_session_dir.as_deref(),
                    "stored chancel-certificate copy notarization_receipt_session_dir against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .attestation_seal_session_dir
                        .as_deref(),
                    handoff_bundle_verify
                        .attestation_seal_session_dir
                        .as_deref(),
                    "stored chancel-certificate copy attestation_seal_session_dir against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.chain_fingerprint_summary.as_deref(),
                    handoff_bundle_verify.chain_fingerprint_summary.as_deref(),
                    "stored chancel-certificate copy chain_fingerprint_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.chain_fingerprint_sha256.as_deref(),
                    handoff_bundle_verify.chain_fingerprint_sha256.as_deref(),
                    "stored chancel-certificate copy chain_fingerprint_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.chain_fingerprint_algorithm.as_deref(),
                    handoff_bundle_verify.chain_fingerprint_algorithm.as_deref(),
                    "stored chancel-certificate copy chain_fingerprint_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.filing_certificate_summary.as_deref(),
                    handoff_bundle_verify.filing_certificate_summary.as_deref(),
                    "stored chancel-certificate copy filing_certificate_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.filing_certificate_sha256.as_deref(),
                    handoff_bundle_verify.filing_certificate_sha256.as_deref(),
                    "stored chancel-certificate copy filing_certificate_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .filing_certificate_algorithm
                        .as_deref(),
                    handoff_bundle_verify
                        .filing_certificate_algorithm
                        .as_deref(),
                    "stored chancel-certificate copy filing_certificate_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.archive_receipt_summary.as_deref(),
                    handoff_bundle_verify.archive_receipt_summary.as_deref(),
                    "stored chancel-certificate copy archive_receipt_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.archive_receipt_sha256.as_deref(),
                    handoff_bundle_verify.archive_receipt_sha256.as_deref(),
                    "stored chancel-certificate copy archive_receipt_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.archive_receipt_algorithm.as_deref(),
                    handoff_bundle_verify.archive_receipt_algorithm.as_deref(),
                    "stored chancel-certificate copy archive_receipt_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.closure_certificate_summary.as_deref(),
                    handoff_bundle_verify.closure_certificate_summary.as_deref(),
                    "stored chancel-certificate copy closure_certificate_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.closure_certificate_sha256.as_deref(),
                    handoff_bundle_verify.closure_certificate_sha256.as_deref(),
                    "stored chancel-certificate copy closure_certificate_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .closure_certificate_algorithm
                        .as_deref(),
                    handoff_bundle_verify
                        .closure_certificate_algorithm
                        .as_deref(),
                    "stored chancel-certificate copy closure_certificate_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.finality_receipt_summary.as_deref(),
                    handoff_bundle_verify.finality_receipt_summary.as_deref(),
                    "stored chancel-certificate copy finality_receipt_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.finality_receipt_sha256.as_deref(),
                    handoff_bundle_verify.finality_receipt_sha256.as_deref(),
                    "stored chancel-certificate copy finality_receipt_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.finality_receipt_algorithm.as_deref(),
                    handoff_bundle_verify.finality_receipt_algorithm.as_deref(),
                    "stored chancel-certificate copy finality_receipt_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.consummation_record_summary.as_deref(),
                    handoff_bundle_verify.consummation_record_summary.as_deref(),
                    "stored chancel-certificate copy consummation_record_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.consummation_record_sha256.as_deref(),
                    handoff_bundle_verify.consummation_record_sha256.as_deref(),
                    "stored chancel-certificate copy consummation_record_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .consummation_record_algorithm
                        .as_deref(),
                    handoff_bundle_verify
                        .consummation_record_algorithm
                        .as_deref(),
                    "stored chancel-certificate copy consummation_record_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .completion_certificate_summary
                        .as_deref(),
                    handoff_bundle_verify
                        .completion_certificate_summary
                        .as_deref(),
                    "stored chancel-certificate copy completion_certificate_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .completion_certificate_sha256
                        .as_deref(),
                    handoff_bundle_verify
                        .completion_certificate_sha256
                        .as_deref(),
                    "stored chancel-certificate copy completion_certificate_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.completion_certificate_algorithm.as_deref(),
                    handoff_bundle_verify.completion_certificate_algorithm.as_deref(),
                    "stored chancel-certificate copy completion_certificate_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.culmination_receipt_summary.as_deref(),
                    handoff_bundle_verify.culmination_receipt_summary.as_deref(),
                    "stored chancel-certificate copy culmination_receipt_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.culmination_receipt_sha256.as_deref(),
                    handoff_bundle_verify.culmination_receipt_sha256.as_deref(),
                    "stored chancel-certificate copy culmination_receipt_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .culmination_receipt_algorithm
                        .as_deref(),
                    handoff_bundle_verify
                        .culmination_receipt_algorithm
                        .as_deref(),
                    "stored chancel-certificate copy culmination_receipt_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.summit_certificate_summary.as_deref(),
                    handoff_bundle_verify.summit_certificate_summary.as_deref(),
                    "stored chancel-certificate copy summit_certificate_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.summit_certificate_sha256.as_deref(),
                    handoff_bundle_verify.summit_certificate_sha256.as_deref(),
                    "stored chancel-certificate copy summit_certificate_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .summit_certificate_algorithm
                        .as_deref(),
                    handoff_bundle_verify
                        .summit_certificate_algorithm
                        .as_deref(),
                    "stored chancel-certificate copy summit_certificate_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.pinnacle_receipt_summary.as_deref(),
                    handoff_bundle_verify.pinnacle_receipt_summary.as_deref(),
                    "stored chancel-certificate copy pinnacle_receipt_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.pinnacle_receipt_sha256.as_deref(),
                    handoff_bundle_verify.pinnacle_receipt_sha256.as_deref(),
                    "stored chancel-certificate copy pinnacle_receipt_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.pinnacle_receipt_algorithm.as_deref(),
                    handoff_bundle_verify.pinnacle_receipt_algorithm.as_deref(),
                    "stored chancel-certificate copy pinnacle_receipt_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.keystone_receipt_summary.as_deref(),
                    handoff_bundle_verify.keystone_receipt_summary.as_deref(),
                    "stored chancel-certificate copy keystone_receipt_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.keystone_receipt_sha256.as_deref(),
                    handoff_bundle_verify.keystone_receipt_sha256.as_deref(),
                    "stored chancel-certificate copy keystone_receipt_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.keystone_receipt_algorithm.as_deref(),
                    handoff_bundle_verify.keystone_receipt_algorithm.as_deref(),
                    "stored chancel-certificate copy keystone_receipt_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .cornerstone_certificate_summary
                        .as_deref(),
                    handoff_bundle_verify
                        .cornerstone_certificate_summary
                        .as_deref(),
                    "stored chancel-certificate copy cornerstone_certificate_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .cornerstone_certificate_sha256
                        .as_deref(),
                    handoff_bundle_verify
                        .cornerstone_certificate_sha256
                        .as_deref(),
                    "stored chancel-certificate copy cornerstone_certificate_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .cornerstone_certificate_algorithm
                        .as_deref(),
                    handoff_bundle_verify
                        .cornerstone_certificate_algorithm
                        .as_deref(),
                    "stored chancel-certificate copy cornerstone_certificate_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .capstone_certificate_summary
                        .as_deref(),
                    handoff_bundle_verify
                        .capstone_certificate_summary
                        .as_deref(),
                    "stored chancel-certificate copy capstone_certificate_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.capstone_certificate_sha256.as_deref(),
                    handoff_bundle_verify.capstone_certificate_sha256.as_deref(),
                    "stored chancel-certificate copy capstone_certificate_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .capstone_certificate_algorithm
                        .as_deref(),
                    handoff_bundle_verify
                        .capstone_certificate_algorithm
                        .as_deref(),
                    "stored chancel-certificate copy capstone_certificate_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.foundation_receipt_summary.as_deref(),
                    handoff_bundle_verify.foundation_receipt_summary.as_deref(),
                    "stored chancel-certificate copy foundation_receipt_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.foundation_receipt_sha256.as_deref(),
                    handoff_bundle_verify.foundation_receipt_sha256.as_deref(),
                    "stored chancel-certificate copy foundation_receipt_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .foundation_receipt_algorithm
                        .as_deref(),
                    handoff_bundle_verify
                        .foundation_receipt_algorithm
                        .as_deref(),
                    "stored chancel-certificate copy foundation_receipt_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.bedrock_certificate_summary.as_deref(),
                    handoff_bundle_verify.bedrock_certificate_summary.as_deref(),
                    "stored chancel-certificate copy bedrock_certificate_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.bedrock_certificate_sha256.as_deref(),
                    handoff_bundle_verify.bedrock_certificate_sha256.as_deref(),
                    "stored chancel-certificate copy bedrock_certificate_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .bedrock_certificate_algorithm
                        .as_deref(),
                    handoff_bundle_verify
                        .bedrock_certificate_algorithm
                        .as_deref(),
                    "stored chancel-certificate copy bedrock_certificate_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.basal_receipt_summary.as_deref(),
                    handoff_bundle_verify.basal_receipt_summary.as_deref(),
                    "stored chancel-certificate copy basal_receipt_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.basal_receipt_sha256.as_deref(),
                    handoff_bundle_verify.basal_receipt_sha256.as_deref(),
                    "stored chancel-certificate copy basal_receipt_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.basal_receipt_algorithm.as_deref(),
                    handoff_bundle_verify.basal_receipt_algorithm.as_deref(),
                    "stored chancel-certificate copy basal_receipt_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .substructure_certificate_summary
                        .as_deref(),
                    handoff_bundle_verify
                        .substructure_certificate_summary
                        .as_deref(),
                    "stored chancel-certificate copy substructure_certificate_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .substructure_certificate_sha256
                        .as_deref(),
                    handoff_bundle_verify
                        .substructure_certificate_sha256
                        .as_deref(),
                    "stored chancel-certificate copy substructure_certificate_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .substructure_certificate_algorithm
                        .as_deref(),
                    handoff_bundle_verify
                        .substructure_certificate_algorithm
                        .as_deref(),
                    "stored chancel-certificate copy substructure_certificate_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.plinth_receipt_summary.as_deref(),
                    handoff_bundle_verify.plinth_receipt_summary.as_deref(),
                    "stored chancel-certificate copy plinth_receipt_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.plinth_receipt_sha256.as_deref(),
                    handoff_bundle_verify.plinth_receipt_sha256.as_deref(),
                    "stored chancel-certificate copy plinth_receipt_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.plinth_receipt_algorithm.as_deref(),
                    handoff_bundle_verify.plinth_receipt_algorithm.as_deref(),
                    "stored chancel-certificate copy plinth_receipt_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .pedestal_certificate_summary
                        .as_deref(),
                    handoff_bundle_verify
                        .pedestal_certificate_summary
                        .as_deref(),
                    "stored chancel-certificate copy pedestal_certificate_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.pedestal_certificate_sha256.as_deref(),
                    handoff_bundle_verify.pedestal_certificate_sha256.as_deref(),
                    "stored chancel-certificate copy pedestal_certificate_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .pedestal_certificate_algorithm
                        .as_deref(),
                    handoff_bundle_verify
                        .pedestal_certificate_algorithm
                        .as_deref(),
                    "stored chancel-certificate copy pedestal_certificate_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.pulpit_receipt_summary.as_deref(),
                    handoff_bundle_verify.pulpit_receipt_summary.as_deref(),
                    "stored chancel-certificate copy pulpit_receipt_summary against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.pulpit_receipt_sha256.as_deref(),
                    handoff_bundle_verify.pulpit_receipt_sha256.as_deref(),
                    "stored chancel-certificate copy pulpit_receipt_sha256 against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle.pulpit_receipt_algorithm.as_deref(),
                    handoff_bundle_verify.pulpit_receipt_algorithm.as_deref(),
                    "stored chancel-certificate copy pulpit_receipt_algorithm against verify truth",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_handoff_bundle
                        .reviewed_frozen_live_cutover_controller_command_summary
                        .as_deref(),
                    handoff_bundle_verify
                        .reviewed_frozen_live_cutover_controller_command_summary
                        .as_deref(),
                    "stored chancel-certificate copy reviewed_frozen_live_cutover_controller_command_summary",
                    &mut mismatches,
                );
                if handoff_bundle_verify.verdict != CHANCEL_CERTIFICATE_VERIFY_OK {
                    mismatches.push(format!(
                        "nested chancel-certificate verification is non-green: {}",
                        handoff_bundle_verify.reason
                    ));
                }
            }

            let expected_result = pulpit_receipt_latest_result_from_nested_status(
                nested_handoff_bundle_status.result.as_str(),
            )
            .unwrap_or(LivePackageChancelCertificateLatestResult::RefusedByDriftedChancelCertificateContract);
            if status.result != expected_result {
                mismatches.push(format!(
                    "stored latest-chancel-certificate result {} does not match nested chancel-certificate result {}",
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
                    "stored status activation_authorized must remain false for latest-chancel-certificate"
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
            if failure.latest_pulpit_receipt_plan_raw.is_some() {
                let stored_latest_plan: PulpitReceiptLatestReportView = load_required_step_json(
                    &status.latest_pulpit_receipt_plan_step,
                    &paths.latest_pulpit_receipt_plan_report_path,
                    "latest_pulpit_receipt_plan_step",
                    &mut mismatches,
                )?;
                compare_step_artifact_against_report_metadata(
                    status.latest_pulpit_receipt_plan_step.as_ref(),
                    &paths.latest_pulpit_receipt_plan_report_path,
                    "plan_latest_pulpit_receipt",
                    &stored_latest_plan.verdict,
                    &stored_latest_plan.reason,
                    stored_latest_plan.generated_at,
                    "stored latest_pulpit_receipt_plan_step",
                    &mut mismatches,
                );
                compare_option_string(
                    status.latest_pulpit_receipt_plan_verdict.as_deref(),
                    failure.latest_pulpit_receipt_plan_verdict.as_deref(),
                    "stored status latest_pulpit_receipt_plan_verdict on failure",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_report.latest_pulpit_receipt_plan_verdict.as_deref(),
                    failure.latest_pulpit_receipt_plan_verdict.as_deref(),
                    "stored report latest_pulpit_receipt_plan_verdict on failure",
                    &mut mismatches,
                );
            }
            compare_option_string(
                status.latest_pulpit_receipt_verdict.as_deref(),
                None,
                "stored status latest_pulpit_receipt_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.latest_pulpit_receipt_verdict.as_deref(),
                None,
                "stored report latest_pulpit_receipt_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                status.chancel_certificate_verdict.as_deref(),
                None,
                "stored status chancel_certificate_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.chancel_certificate_verdict.as_deref(),
                None,
                "stored report chancel_certificate_verdict on failure",
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
        TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyOk
    } else {
        TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
    };
    let reason = if mismatches.is_empty() {
        format!(
            "latest-chancel-certificate session under {} is coherent with the current latest immutable chain and the accepted chancel-certificate contract",
            session_dir.display()
        )
    } else {
        mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "latest-chancel-certificate verification failed".to_string())
    };

    Ok(PackageChancelCertificateLatestReport {
        generated_at: Utc::now(),
        mode: "verify_latest_chancel_certificate".to_string(),
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
        chancel_certificate_latest_session_path: Some(paths.session_path.display().to_string()),
        chancel_certificate_latest_status_path: Some(paths.status_path.display().to_string()),
        chancel_certificate_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_pulpit_receipt_plan_report_path: Some(
            paths
                .latest_pulpit_receipt_plan_report_path
                .display()
                .to_string(),
        ),
        latest_pulpit_receipt_report_path: Some(
            paths.latest_pulpit_receipt_report_path.display().to_string(),
        ),
        chancel_certificate_report_path: Some(paths.chancel_certificate_report_path.display().to_string()),
        nested_pulpit_receipt_latest_session_dir: Some(
            paths
                .nested_pulpit_receipt_latest_session_dir
                .display()
                .to_string(),
        ),
        nested_chancel_certificate_session_dir: Some(
            paths.nested_chancel_certificate_session_dir.display().to_string(),
        ),
        downstream_decision_packet_session_dir: Some(
            expected_downstream_decision_packet_session_dir(&paths)
                .display()
                .to_string(),
        ),
        latest_pulpit_receipt_plan_verdict: status.latest_pulpit_receipt_plan_verdict.clone(),
        latest_pulpit_receipt_verdict: status.latest_pulpit_receipt_verdict.clone(),
        chancel_certificate_verdict: status.chancel_certificate_verdict.clone(),
        result: Some(serialize_enum(&status.result)),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        resolve_latest_pulpit_receipt_command_summary: Some(
            session.resolve_latest_pulpit_receipt_command_summary.clone(),
        ),
        run_latest_chancel_certificate_command_summary: Some(
            session.run_latest_chancel_certificate_command_summary.clone(),
        ),
        verify_latest_chancel_certificate_command_summary: Some(
            session.verify_latest_chancel_certificate_command_summary.clone(),
        ),
        downstream_chancel_certificate_command_summary: Some(
            session.downstream_chancel_certificate_command_summary.clone(),
        ),
        verify_chancel_certificate_command_summary: Some(
            session.verify_chancel_certificate_command_summary.clone(),
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
            "this verification proves only whether the latest immutable chain still resolves cleanly into the accepted chancel-certificate contract; it never runs the frozen controller"
                .to_string(),
    })
}

fn run_latest_chancel_certificate_with_resolution(
    root: &Path,
    session_dir: &Path,
    paths: &PackageChancelCertificateLatestPaths,
    resolved: &ResolvedLatestPulpitReceipt,
) -> Result<PackageChancelCertificateLatestReport> {
    let session = planned_session(root, session_dir, paths, Some(resolved), None);
    persist_json(&paths.session_path, &session)?;
    persist_json(
        &paths.latest_pulpit_receipt_plan_report_path,
        &resolved.latest_pulpit_receipt_plan_raw,
    )?;

    let latest_pulpit_receipt_plan_step = Some(step_artifact(
        &paths.latest_pulpit_receipt_plan_report_path,
        "plan_latest_pulpit_receipt",
        &resolved.latest_pulpit_receipt_plan.verdict,
        &resolved.latest_pulpit_receipt_plan.reason,
        resolved.latest_pulpit_receipt_plan.generated_at,
    ));

    let latest_pulpit_receipt_run_raw = match run_json_command(
        PULPIT_RECEIPT_LATEST_BIN,
        &latest_pulpit_receipt_run_args(root, &paths.nested_pulpit_receipt_latest_session_dir),
    ) {
        Ok(value) => value,
        Err(error) => {
            let status = refusal_status(
                root,
                session_dir,
                paths,
                Some(resolved),
                LivePackageChancelCertificateLatestResult::RefusedByDriftedChancelCertificateContract,
                format!(
                    "failed running nested latest-pulpit-receipt handoff under {}: {error}",
                    paths
                        .nested_pulpit_receipt_latest_session_dir
                        .display()
                ),
                latest_pulpit_receipt_plan_step,
                None,
                None,
            );
            persist_json(&paths.status_path, &status)?;
            let report = report_from_status(
                "run_latest_chancel_certificate",
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
        &paths.latest_pulpit_receipt_report_path,
        &latest_pulpit_receipt_run_raw,
    )?;
    let latest_pulpit_receipt_run: PulpitReceiptLatestReportView =
        serde_json::from_value(latest_pulpit_receipt_run_raw)
            .context("failed parsing nested latest-pulpit-receipt run json")?;
    let latest_pulpit_receipt_step = Some(step_artifact(
        &paths.latest_pulpit_receipt_report_path,
        "run_latest_pulpit_receipt",
        &latest_pulpit_receipt_run.verdict,
        &latest_pulpit_receipt_run.reason,
        latest_pulpit_receipt_run.generated_at,
    ));

    if let Some(terminal_failure) =
        latest_pulpit_receipt_terminal_failure(&latest_pulpit_receipt_run)
    {
        let status = refusal_status(
            root,
            session_dir,
            paths,
            Some(resolved),
            terminal_failure,
            latest_pulpit_receipt_run.reason.clone(),
            latest_pulpit_receipt_plan_step,
            latest_pulpit_receipt_step,
            None,
        );
        persist_json(&paths.status_path, &status)?;
        let report = report_from_status(
            "run_latest_chancel_certificate",
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
    compare_pulpit_receipt_latest_run_against_snapshot(
        &latest_pulpit_receipt_run,
        &resolved.snapshot,
        &paths.nested_pulpit_receipt_latest_session_dir,
        &mut run_mismatches,
    );
    if !run_mismatches.is_empty() {
        let status = refusal_status(
            root,
            session_dir,
            paths,
            Some(resolved),
            LivePackageChancelCertificateLatestResult::RefusedByDriftedChancelCertificateContract,
            run_mismatches[0].clone(),
            latest_pulpit_receipt_plan_step,
            latest_pulpit_receipt_step,
            None,
        );
        persist_json(&paths.status_path, &status)?;
        let report = report_from_status(
            "run_latest_chancel_certificate",
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

    let latest_pulpit_receipt_verify: PulpitReceiptLatestReportView = match run_json_command(
        PULPIT_RECEIPT_LATEST_BIN,
        &latest_pulpit_receipt_verify_args(&paths.nested_pulpit_receipt_latest_session_dir),
    ) {
        Ok(value) => serde_json::from_value(value)
            .context("failed parsing nested latest-pulpit-receipt verify json")?,
        Err(error) => {
            let status = refusal_status(
                    root,
                    session_dir,
                    paths,
                    Some(resolved),
                    LivePackageChancelCertificateLatestResult::RefusedByDriftedChancelCertificateContract,
                    format!(
                    "failed verifying nested latest-pulpit-receipt handoff under {}: {error}",
                    paths.nested_pulpit_receipt_latest_session_dir.display()
                ),
                    latest_pulpit_receipt_plan_step,
                    latest_pulpit_receipt_step,
                    None,
                );
            persist_json(&paths.status_path, &status)?;
            let report = report_from_status(
                "run_latest_chancel_certificate",
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
    if latest_pulpit_receipt_verify.verdict != PULPIT_RECEIPT_LATEST_VERIFY_OK {
        let status = refusal_status(
            root,
            session_dir,
            paths,
            Some(resolved),
            LivePackageChancelCertificateLatestResult::RefusedByDriftedChancelCertificateContract,
            latest_pulpit_receipt_verify.reason,
            latest_pulpit_receipt_plan_step,
            latest_pulpit_receipt_step,
            None,
        );
        persist_json(&paths.status_path, &status)?;
        let report = report_from_status(
            "run_latest_chancel_certificate",
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
        CHANCEL_CERTIFICATE_BIN,
        &chancel_certificate_run_args(
            &resolved.snapshot,
            &expected_downstream_pulpit_receipt_session_dir(paths),
            &paths.nested_chancel_certificate_session_dir,
        ),
    ) {
        Ok(value) => value,
        Err(error) => {
            let status = refusal_status(
                root,
                session_dir,
                paths,
                Some(resolved),
                LivePackageChancelCertificateLatestResult::RefusedByDriftedChancelCertificateContract,
                format!(
                    "failed running downstream chancel-certificate contract under {}: {error}",
                    paths.nested_chancel_certificate_session_dir.display()
                ),
                latest_pulpit_receipt_plan_step,
                latest_pulpit_receipt_step,
                None,
            );
            persist_json(&paths.status_path, &status)?;
            let report = report_from_status(
                "run_latest_chancel_certificate",
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
        &paths.chancel_certificate_report_path,
        &release_capsule_run_raw,
    )?;
    let provenance_certificate_run: ChancelCertificateReportView =
        serde_json::from_value(release_capsule_run_raw)
            .context("failed parsing nested chancel-certificate run json")?;
    let chancel_certificate_step = Some(step_artifact(
        &paths.chancel_certificate_report_path,
        "run_live_package_chancel_certificate",
        &provenance_certificate_run.verdict,
        &provenance_certificate_run.reason,
        provenance_certificate_run.generated_at,
    ));

    let mut provenance_certificate_mismatches = Vec::new();
    compare_chancel_certificate_run_against_snapshot(
        &provenance_certificate_run,
        &resolved.snapshot,
        &expected_downstream_pulpit_receipt_session_dir(paths),
        &paths.nested_chancel_certificate_session_dir,
        &mut provenance_certificate_mismatches,
    );
    if !provenance_certificate_mismatches.is_empty() {
        let status = refusal_status(
            root,
            session_dir,
            paths,
            Some(resolved),
            LivePackageChancelCertificateLatestResult::RefusedByDriftedChancelCertificateContract,
            provenance_certificate_mismatches[0].clone(),
            latest_pulpit_receipt_plan_step,
            latest_pulpit_receipt_step,
            chancel_certificate_step,
        );
        persist_json(&paths.status_path, &status)?;
        let report = report_from_status(
            "run_latest_chancel_certificate",
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

    let provenance_certificate_verify: ChancelCertificateReportView = match run_json_command(
        CHANCEL_CERTIFICATE_BIN,
        &chancel_certificate_verify_args(
            &resolved.snapshot,
            &expected_downstream_pulpit_receipt_session_dir(paths),
            &paths.nested_chancel_certificate_session_dir,
        ),
    ) {
        Ok(value) => serde_json::from_value(value)
            .context("failed parsing nested chancel-certificate verify json")?,
        Err(error) => {
            let status = refusal_status(
                root,
                session_dir,
                paths,
                Some(resolved),
                LivePackageChancelCertificateLatestResult::RefusedByDriftedChancelCertificateContract,
                format!(
                    "failed verifying downstream chancel-certificate contract under {}: {error}",
                    paths.nested_chancel_certificate_session_dir.display()
                ),
                latest_pulpit_receipt_plan_step,
                latest_pulpit_receipt_step,
                chancel_certificate_step,
            );
            persist_json(&paths.status_path, &status)?;
            let report = report_from_status(
                "run_latest_chancel_certificate",
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
    if provenance_certificate_verify.verdict != CHANCEL_CERTIFICATE_VERIFY_OK {
        let status = refusal_status(
            root,
            session_dir,
            paths,
            Some(resolved),
            LivePackageChancelCertificateLatestResult::RefusedByDriftedChancelCertificateContract,
            provenance_certificate_verify.reason,
            latest_pulpit_receipt_plan_step,
            latest_pulpit_receipt_step,
            chancel_certificate_step,
        );
        persist_json(&paths.status_path, &status)?;
        let report = report_from_status(
            "run_latest_chancel_certificate",
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

    let result = pulpit_receipt_latest_result_from_nested_status(
        provenance_certificate_run
            .result
            .as_deref()
            .unwrap_or_default(),
    )
    .unwrap_or(
        LivePackageChancelCertificateLatestResult::RefusedByDriftedChancelCertificateContract,
    );
    let status = PackageChancelCertificateLatestStatus {
        status_version: CHANCEL_CERTIFICATE_LATEST_STATUS_VERSION.to_string(),
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
        nested_pulpit_receipt_latest_session_dir: paths
            .nested_pulpit_receipt_latest_session_dir
            .display()
            .to_string(),
        nested_chancel_certificate_session_dir: paths
            .nested_chancel_certificate_session_dir
            .display()
            .to_string(),
        downstream_decision_packet_session_dir: resolved
            .snapshot
            .downstream_decision_packet_session_dir
            .display()
            .to_string(),
        result,
        reason: provenance_certificate_run.reason.clone(),
        latest_pulpit_receipt_plan_verdict: Some(
            resolved.latest_pulpit_receipt_plan.verdict.clone(),
        ),
        latest_pulpit_receipt_verdict: Some(latest_pulpit_receipt_run.verdict.clone()),
        chancel_certificate_verdict: Some(provenance_certificate_run.verdict.clone()),
        current_pre_activation_gate_verdict: provenance_certificate_run
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: provenance_certificate_run
            .current_pre_activation_gate_reason
            .clone(),
        latest_pulpit_receipt_plan_step,
        latest_pulpit_receipt_step,
        chancel_certificate_step,
        operator_next_action_summary: operator_next_action_summary(result),
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    };
    persist_json(&paths.status_path, &status)?;
    let report = report_from_status(
        "run_latest_chancel_certificate",
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

fn run_latest_chancel_certificate_failure(
    root: &Path,
    session_dir: &Path,
    paths: &PackageChancelCertificateLatestPaths,
    failure: &LatestPulpitReceiptResolutionFailure,
) -> Result<PackageChancelCertificateLatestReport> {
    let session = planned_session(root, session_dir, paths, None, Some(failure));
    persist_json(&paths.session_path, &session)?;
    if let Some(value) = &failure.latest_pulpit_receipt_plan_raw {
        persist_json(&paths.latest_pulpit_receipt_plan_report_path, value)?;
    }
    let status = refusal_status_from_resolution_failure(root, session_dir, paths, failure);
    persist_json(&paths.status_path, &status)?;
    let report = report_from_status(
        "run_latest_chancel_certificate",
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

fn resolve_latest_pulpit_receipt(
    root: &Path,
    paths: &PackageChancelCertificateLatestPaths,
) -> std::result::Result<ResolvedLatestPulpitReceipt, LatestPulpitReceiptResolutionFailure> {
    let latest_pulpit_receipt_plan_raw = match run_json_command(
        PULPIT_RECEIPT_LATEST_BIN,
        &latest_pulpit_receipt_plan_args(root, &paths.nested_pulpit_receipt_latest_session_dir),
    ) {
        Ok(value) => value,
        Err(error) => {
            return Err(LatestPulpitReceiptResolutionFailure {
                kind: ResolutionFailureKind::MissingLatestChain,
                reason: format!(
                    "failed resolving latest immutable chain under {}: {error}",
                    root.display()
                ),
                snapshot: None,
                latest_pulpit_receipt_plan_raw: None,
                latest_pulpit_receipt_plan_verdict: None,
                latest_pulpit_receipt_plan_reason: None,
                latest_pulpit_receipt_plan_generated_at: None,
            });
        }
    };
    let latest_pulpit_receipt_plan: PulpitReceiptLatestReportView =
        serde_json::from_value(latest_pulpit_receipt_plan_raw.clone()).map_err(
            |error| LatestPulpitReceiptResolutionFailure {
                kind: ResolutionFailureKind::InvalidLatestChain,
                reason: format!(
                    "failed parsing latest-pulpit-receipt upstream latest-pulpit-receipt plan json for {}: {error}",
                    root.display()
                ),
                snapshot: None,
                latest_pulpit_receipt_plan_raw: Some(
                    latest_pulpit_receipt_plan_raw.clone(),
                ),
                latest_pulpit_receipt_plan_verdict: None,
                latest_pulpit_receipt_plan_reason: None,
                latest_pulpit_receipt_plan_generated_at: None,
            },
        )?;

    let snapshot = parse_snapshot_from_latest_pulpit_receipt(&latest_pulpit_receipt_plan).ok();
    if latest_pulpit_receipt_plan.verdict != PULPIT_RECEIPT_LATEST_PLAN_READY {
        return Err(LatestPulpitReceiptResolutionFailure {
            kind: map_pulpit_receipt_latest_failure_kind(
                &latest_pulpit_receipt_plan.verdict,
            ),
            reason: format!(
                "latest-pulpit-receipt resolution refused because nested latest-pulpit-receipt reported: {}",
                latest_pulpit_receipt_plan.reason
            ),
            snapshot,
            latest_pulpit_receipt_plan_raw: Some(latest_pulpit_receipt_plan_raw),
            latest_pulpit_receipt_plan_verdict: Some(
                latest_pulpit_receipt_plan.verdict,
            ),
            latest_pulpit_receipt_plan_reason: Some(
                latest_pulpit_receipt_plan.reason,
            ),
            latest_pulpit_receipt_plan_generated_at: Some(
                latest_pulpit_receipt_plan.generated_at,
            ),
        });
    }

    let snapshot = parse_snapshot_from_latest_pulpit_receipt(&latest_pulpit_receipt_plan).map_err(
        |error| LatestPulpitReceiptResolutionFailure {
            kind: ResolutionFailureKind::InvalidLatestChain,
            reason: format!("latest-pulpit-receipt resolution failed: {error}"),
            snapshot: None,
            latest_pulpit_receipt_plan_raw: Some(latest_pulpit_receipt_plan_raw.clone()),
            latest_pulpit_receipt_plan_verdict: Some(latest_pulpit_receipt_plan.verdict.clone()),
            latest_pulpit_receipt_plan_reason: Some(latest_pulpit_receipt_plan.reason.clone()),
            latest_pulpit_receipt_plan_generated_at: Some(latest_pulpit_receipt_plan.generated_at),
        },
    )?;

    let mut mismatches = Vec::new();
    compare_pulpit_receipt_latest_plan_against_snapshot(
        &latest_pulpit_receipt_plan,
        &snapshot,
        &paths.nested_pulpit_receipt_latest_session_dir,
        &mut mismatches,
    );
    if !mismatches.is_empty() {
        return Err(LatestPulpitReceiptResolutionFailure {
            kind: ResolutionFailureKind::DriftedPulpitReceiptContract,
            reason: format!("latest-pulpit-receipt drift detected: {}", mismatches[0]),
            snapshot: Some(snapshot),
            latest_pulpit_receipt_plan_raw: Some(latest_pulpit_receipt_plan_raw),
            latest_pulpit_receipt_plan_verdict: Some(latest_pulpit_receipt_plan.verdict),
            latest_pulpit_receipt_plan_reason: Some(latest_pulpit_receipt_plan.reason),
            latest_pulpit_receipt_plan_generated_at: Some(latest_pulpit_receipt_plan.generated_at),
        });
    }

    Ok(ResolvedLatestPulpitReceipt {
        snapshot,
        latest_pulpit_receipt_plan_raw,
        latest_pulpit_receipt_plan,
    })
}

fn parse_snapshot_from_latest_pulpit_receipt(
    view: &PulpitReceiptLatestReportView,
) -> Result<LatestPulpitReceiptSnapshot> {
    let latest_top_step_name =
        required_option_string(view.latest_top_step_name.as_ref(), "latest_top_step_name")?;
    if latest_top_step_name != TOP_ACCEPTED_PACKAGE_STEP {
        bail!(
            "latest-pulpit-receipt nested latest-pulpit-receipt plan top accepted step is {} instead of {}",
            latest_top_step_name,
            TOP_ACCEPTED_PACKAGE_STEP
        );
    }
    if view.activation_authorized {
        bail!(
            "latest-pulpit-receipt nested latest-pulpit-receipt plan must remain planning-safe with activation_authorized=false"
        );
    }
    Ok(LatestPulpitReceiptSnapshot {
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
                "latest-pulpit-receipt nested latest-pulpit-receipt plan is missing wrapper_timeout_ms"
            )
        })?,
        service_status_max_staleness_ms: view.service_status_max_staleness_ms.ok_or_else(|| {
            anyhow!(
                "latest-pulpit-receipt nested latest-pulpit-receipt plan is missing service_status_max_staleness_ms"
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
    verdict: TinyLivePackageChancelCertificateLatestVerdict,
    reason: String,
    root: &Path,
    session_dir: &Path,
    paths: &PackageChancelCertificateLatestPaths,
    resolved: &ResolvedLatestPulpitReceipt,
) -> PackageChancelCertificateLatestReport {
    PackageChancelCertificateLatestReport {
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
        chancel_certificate_latest_session_path: Some(paths.session_path.display().to_string()),
        chancel_certificate_latest_status_path: Some(paths.status_path.display().to_string()),
        chancel_certificate_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_pulpit_receipt_plan_report_path: Some(
            paths
                .latest_pulpit_receipt_plan_report_path
                .display()
                .to_string(),
        ),
        latest_pulpit_receipt_report_path: Some(
            paths
                .latest_pulpit_receipt_report_path
                .display()
                .to_string(),
        ),
        chancel_certificate_report_path: Some(
            paths.chancel_certificate_report_path.display().to_string(),
        ),
        nested_pulpit_receipt_latest_session_dir: Some(
            paths
                .nested_pulpit_receipt_latest_session_dir
                .display()
                .to_string(),
        ),
        nested_chancel_certificate_session_dir: Some(
            paths
                .nested_chancel_certificate_session_dir
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
        latest_pulpit_receipt_plan_verdict: Some(
            resolved.latest_pulpit_receipt_plan.verdict.clone(),
        ),
        latest_pulpit_receipt_verdict: None,
        chancel_certificate_verdict: None,
        result: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        resolve_latest_pulpit_receipt_command_summary: Some(
            resolve_latest_pulpit_receipt_command_summary(
                root,
                &paths.nested_pulpit_receipt_latest_session_dir,
            ),
        ),
        run_latest_chancel_certificate_command_summary: Some(
            run_latest_chancel_certificate_command_summary(root, session_dir),
        ),
        verify_latest_chancel_certificate_command_summary: Some(
            verify_latest_chancel_certificate_command_summary(session_dir),
        ),
        downstream_chancel_certificate_command_summary: Some(
            downstream_chancel_certificate_command_summary(
                &resolved.snapshot,
                &expected_downstream_pulpit_receipt_session_dir(paths),
                &paths.nested_chancel_certificate_session_dir,
            ),
        ),
        verify_chancel_certificate_command_summary: Some(
            verify_chancel_certificate_command_summary(
                &resolved.snapshot,
                &expected_downstream_pulpit_receipt_session_dir(paths),
                &paths.nested_chancel_certificate_session_dir,
            ),
        ),
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
    paths: &PackageChancelCertificateLatestPaths,
    failure: &LatestPulpitReceiptResolutionFailure,
) -> PackageChancelCertificateLatestReport {
    let snapshot = failure.snapshot.as_ref();
    PackageChancelCertificateLatestReport {
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
        chancel_certificate_latest_session_path: Some(paths.session_path.display().to_string()),
        chancel_certificate_latest_status_path: Some(paths.status_path.display().to_string()),
        chancel_certificate_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_pulpit_receipt_plan_report_path: Some(
            paths
                .latest_pulpit_receipt_plan_report_path
                .display()
                .to_string(),
        ),
        latest_pulpit_receipt_report_path: Some(
            paths
                .latest_pulpit_receipt_report_path
                .display()
                .to_string(),
        ),
        chancel_certificate_report_path: Some(
            paths.chancel_certificate_report_path.display().to_string(),
        ),
        nested_pulpit_receipt_latest_session_dir: Some(
            paths
                .nested_pulpit_receipt_latest_session_dir
                .display()
                .to_string(),
        ),
        nested_chancel_certificate_session_dir: Some(
            paths
                .nested_chancel_certificate_session_dir
                .display()
                .to_string(),
        ),
        downstream_decision_packet_session_dir: snapshot.map(|value| {
            value
                .downstream_decision_packet_session_dir
                .display()
                .to_string()
        }),
        latest_pulpit_receipt_plan_verdict: failure.latest_pulpit_receipt_plan_verdict.clone(),
        latest_pulpit_receipt_verdict: None,
        chancel_certificate_verdict: None,
        result: Some(serialize_enum(&result_from_failure_kind(failure.kind))),
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        resolve_latest_pulpit_receipt_command_summary: Some(
            resolve_latest_pulpit_receipt_command_summary(
                root,
                &paths.nested_pulpit_receipt_latest_session_dir,
            ),
        ),
        run_latest_chancel_certificate_command_summary: Some(
            run_latest_chancel_certificate_command_summary(root, session_dir),
        ),
        verify_latest_chancel_certificate_command_summary: Some(
            verify_latest_chancel_certificate_command_summary(session_dir),
        ),
        downstream_chancel_certificate_command_summary: snapshot.map(|value| {
            downstream_chancel_certificate_command_summary(
                value,
                &expected_downstream_pulpit_receipt_session_dir(paths),
                &paths.nested_chancel_certificate_session_dir,
            )
        }),
        verify_chancel_certificate_command_summary: snapshot.map(|value| {
            verify_chancel_certificate_command_summary(
                value,
                &expected_downstream_pulpit_receipt_session_dir(paths),
                &paths.nested_chancel_certificate_session_dir,
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
    paths: &PackageChancelCertificateLatestPaths,
    resolved: Option<&ResolvedLatestPulpitReceipt>,
    failure: Option<&LatestPulpitReceiptResolutionFailure>,
) -> PackageChancelCertificateLatestSession {
    let snapshot = resolved
        .map(|value| &value.snapshot)
        .or_else(|| failure.and_then(|value| value.snapshot.as_ref()));
    PackageChancelCertificateLatestSession {
        session_version: CHANCEL_CERTIFICATE_LATEST_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        root: root.display().to_string(),
        session_dir: session_dir.display().to_string(),
        nested_pulpit_receipt_latest_session_dir: paths
            .nested_pulpit_receipt_latest_session_dir
            .display()
            .to_string(),
        nested_chancel_certificate_session_dir: paths
            .nested_chancel_certificate_session_dir
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
        resolve_latest_pulpit_receipt_command_summary:
            resolve_latest_pulpit_receipt_command_summary(
                root,
                &paths.nested_pulpit_receipt_latest_session_dir,
            ),
        run_latest_chancel_certificate_command_summary:
            run_latest_chancel_certificate_command_summary(root, session_dir),
        verify_latest_chancel_certificate_command_summary:
            verify_latest_chancel_certificate_command_summary(session_dir),
        downstream_chancel_certificate_command_summary: snapshot
            .map(|value| {
                downstream_chancel_certificate_command_summary(
                    value,
                    &expected_downstream_pulpit_receipt_session_dir(paths),
                    &paths.nested_chancel_certificate_session_dir,
                )
            })
            .unwrap_or_else(|| "<chancel-certificate-run-unavailable>".to_string()),
        verify_chancel_certificate_command_summary: snapshot
            .map(|value| {
                verify_chancel_certificate_command_summary(
                    value,
                    &expected_downstream_pulpit_receipt_session_dir(paths),
                    &paths.nested_chancel_certificate_session_dir,
                )
            })
            .unwrap_or_else(|| "<chancel-certificate-verify-unavailable>".to_string()),
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
    verdict: TinyLivePackageChancelCertificateLatestVerdict,
    session_dir: &Path,
    paths: &PackageChancelCertificateLatestPaths,
    session: &PackageChancelCertificateLatestSession,
    status: &PackageChancelCertificateLatestStatus,
    verification_mismatches: Vec<String>,
) -> PackageChancelCertificateLatestReport {
    PackageChancelCertificateLatestReport {
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
        chancel_certificate_latest_session_path: Some(paths.session_path.display().to_string()),
        chancel_certificate_latest_status_path: Some(paths.status_path.display().to_string()),
        chancel_certificate_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_pulpit_receipt_plan_report_path: Some(
            paths
                .latest_pulpit_receipt_plan_report_path
                .display()
                .to_string(),
        ),
        latest_pulpit_receipt_report_path: Some(
            paths
                .latest_pulpit_receipt_report_path
                .display()
                .to_string(),
        ),
        chancel_certificate_report_path: Some(
            paths.chancel_certificate_report_path.display().to_string(),
        ),
        nested_pulpit_receipt_latest_session_dir: Some(
            session.nested_pulpit_receipt_latest_session_dir.clone(),
        ),
        nested_chancel_certificate_session_dir: Some(
            session.nested_chancel_certificate_session_dir.clone(),
        ),
        downstream_decision_packet_session_dir: Some(
            session.downstream_decision_packet_session_dir.clone(),
        ),
        latest_pulpit_receipt_plan_verdict: status.latest_pulpit_receipt_plan_verdict.clone(),
        latest_pulpit_receipt_verdict: status.latest_pulpit_receipt_verdict.clone(),
        chancel_certificate_verdict: status.chancel_certificate_verdict.clone(),
        result: Some(serialize_enum(&status.result)),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        resolve_latest_pulpit_receipt_command_summary: Some(
            session
                .resolve_latest_pulpit_receipt_command_summary
                .clone(),
        ),
        run_latest_chancel_certificate_command_summary: Some(
            session
                .run_latest_chancel_certificate_command_summary
                .clone(),
        ),
        verify_latest_chancel_certificate_command_summary: Some(
            session
                .verify_latest_chancel_certificate_command_summary
                .clone(),
        ),
        downstream_chancel_certificate_command_summary: Some(
            session
                .downstream_chancel_certificate_command_summary
                .clone(),
        ),
        verify_chancel_certificate_command_summary: Some(
            session.verify_chancel_certificate_command_summary.clone(),
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

fn chancel_certificate_latest_paths(session_dir: &Path) -> PackageChancelCertificateLatestPaths {
    PackageChancelCertificateLatestPaths {
        session_path: session_dir
            .join("tiny_live_activation_package_chancel_certificate_latest.session.json"),
        status_path: session_dir
            .join("tiny_live_activation_package_chancel_certificate_latest.status.json"),
        report_path: session_dir
            .join("tiny_live_activation_package_chancel_certificate_latest.report.json"),
        latest_pulpit_receipt_plan_report_path: session_dir.join(
            "tiny_live_activation_package_chancel_certificate_latest.pulpit_receipt_latest_plan.report.json",
        ),
        latest_pulpit_receipt_report_path: session_dir.join(
            "tiny_live_activation_package_chancel_certificate_latest.pulpit_receipt_latest.report.json",
        ),
        chancel_certificate_report_path: session_dir.join(
            "tiny_live_activation_package_chancel_certificate_latest.chancel_certificate.report.json",
        ),
        nested_pulpit_receipt_latest_session_dir: session_dir.join(
            "tiny_live_activation_package_chancel_certificate_latest.pulpit_receipt_latest_session",
        ),
        nested_chancel_certificate_session_dir: session_dir.join(
            "tiny_live_activation_package_chancel_certificate_latest.chancel_certificate_session",
        ),
    }
}

fn expected_downstream_decision_packet_session_dir(
    paths: &PackageChancelCertificateLatestPaths,
) -> PathBuf {
    paths
        .nested_pulpit_receipt_latest_session_dir
        .join("tiny_live_activation_package_pulpit_receipt_latest.decision_packet_session")
}

fn expected_downstream_pulpit_receipt_session_dir(
    paths: &PackageChancelCertificateLatestPaths,
) -> PathBuf {
    paths
        .nested_pulpit_receipt_latest_session_dir
        .join("tiny_live_activation_package_pulpit_receipt_latest.pulpit_receipt_session")
}

fn expected_downstream_review_receipt_session_dir(
    paths: &PackageChancelCertificateLatestPaths,
) -> PathBuf {
    paths
        .nested_pulpit_receipt_latest_session_dir
        .join("tiny_live_activation_package_pulpit_receipt_latest.review_receipt_session")
}

fn latest_pulpit_receipt_plan_args(root: &Path, session_dir: &Path) -> Vec<String> {
    vec![
        "--root".to_string(),
        root.display().to_string(),
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--plan-latest-pulpit-receipt".to_string(),
        "--json".to_string(),
    ]
}

fn latest_pulpit_receipt_run_args(root: &Path, session_dir: &Path) -> Vec<String> {
    vec![
        "--root".to_string(),
        root.display().to_string(),
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--run-latest-pulpit-receipt".to_string(),
        "--json".to_string(),
    ]
}

fn latest_pulpit_receipt_verify_args(session_dir: &Path) -> Vec<String> {
    vec![
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--verify-latest-pulpit-receipt".to_string(),
        "--json".to_string(),
    ]
}

fn chancel_certificate_run_args(
    snapshot: &LatestPulpitReceiptSnapshot,
    pulpit_receipt_session_dir: &Path,
    session_dir: &Path,
) -> Vec<String> {
    vec![
        "--pulpit-receipt-session-dir".to_string(),
        pulpit_receipt_session_dir.display().to_string(),
        "--confirm-decision-packet-session-dir".to_string(),
        snapshot
            .downstream_decision_packet_session_dir
            .display()
            .to_string(),
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--run-live-package-chancel-certificate".to_string(),
        "--json".to_string(),
    ]
}

fn chancel_certificate_verify_args(
    snapshot: &LatestPulpitReceiptSnapshot,
    pulpit_receipt_session_dir: &Path,
    session_dir: &Path,
) -> Vec<String> {
    vec![
        "--pulpit-receipt-session-dir".to_string(),
        pulpit_receipt_session_dir.display().to_string(),
        "--confirm-decision-packet-session-dir".to_string(),
        snapshot
            .downstream_decision_packet_session_dir
            .display()
            .to_string(),
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--verify-live-package-chancel-certificate".to_string(),
        "--json".to_string(),
    ]
}

fn resolve_latest_pulpit_receipt_command_summary(root: &Path, session_dir: &Path) -> String {
    format!(
        "{PULPIT_RECEIPT_LATEST_BIN} --root {} --session-dir {} --plan-latest-pulpit-receipt --json",
        shell_escape_path(root),
        shell_escape_path(session_dir),
    )
}

fn run_latest_chancel_certificate_command_summary(root: &Path, session_dir: &Path) -> String {
    format!(
        "{CHANCEL_CERTIFICATE_LATEST_BIN} --root {} --session-dir {} --run-latest-chancel-certificate --json",
        shell_escape_path(root),
        shell_escape_path(session_dir),
    )
}

fn verify_latest_chancel_certificate_command_summary(session_dir: &Path) -> String {
    format!(
        "{CHANCEL_CERTIFICATE_LATEST_BIN} --session-dir {} --verify-latest-chancel-certificate --json",
        shell_escape_path(session_dir),
    )
}

fn downstream_chancel_certificate_command_summary(
    snapshot: &LatestPulpitReceiptSnapshot,
    pulpit_receipt_session_dir: &Path,
    session_dir: &Path,
) -> String {
    format!(
        "{CHANCEL_CERTIFICATE_BIN} --pulpit-receipt-session-dir {} --confirm-decision-packet-session-dir {} --session-dir {} --run-live-package-chancel-certificate --json",
        shell_escape_path(pulpit_receipt_session_dir),
        shell_escape_path(&snapshot.downstream_decision_packet_session_dir),
        shell_escape_path(session_dir),
    )
}

fn verify_chancel_certificate_command_summary(
    snapshot: &LatestPulpitReceiptSnapshot,
    pulpit_receipt_session_dir: &Path,
    session_dir: &Path,
) -> String {
    format!(
        "{CHANCEL_CERTIFICATE_BIN} --pulpit-receipt-session-dir {} --confirm-decision-packet-session-dir {} --session-dir {} --verify-live-package-chancel-certificate --json",
        shell_escape_path(pulpit_receipt_session_dir),
        shell_escape_path(&snapshot.downstream_decision_packet_session_dir),
        shell_escape_path(session_dir),
    )
}

fn nested_pulpit_receipt_latest_report_path(session_dir: &Path) -> PathBuf {
    session_dir.join("tiny_live_activation_package_pulpit_receipt_latest.report.json")
}

fn nested_chancel_certificate_session_path(session_dir: &Path) -> PathBuf {
    session_dir.join("tiny_live_activation_package_chancel_certificate.session.json")
}

fn nested_chancel_certificate_status_path(session_dir: &Path) -> PathBuf {
    session_dir.join("tiny_live_activation_package_chancel_certificate.status.json")
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
            "failed writing latest-chancel-certificate script to {}",
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
) -> PackageChancelCertificateLatestStepArtifact {
    PackageChancelCertificateLatestStepArtifact {
        path: path.display().to_string(),
        mode: mode.to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        generated_at,
    }
}

fn canonical_wrapper_mode(mode: &str) -> &str {
    match mode {
        "plan_latest_chancel_certificate" => "plan_latest_chancel_certificate",
        "render_latest_chancel_certificate_script" => "render_latest_chancel_certificate_script",
        "run_latest_chancel_certificate" => "run_latest_chancel_certificate",
        "verify_latest_chancel_certificate" => "verify_latest_chancel_certificate",
        other => other,
    }
}

fn load_required_step_json<T: for<'de> Deserialize<'de>>(
    step: &Option<PackageChancelCertificateLatestStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<T> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("{label} is missing from latest-chancel-certificate status"))?;
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
    step: Option<&PackageChancelCertificateLatestStepArtifact>,
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
            "{label} is missing from latest-chancel-certificate status"
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

fn compare_pulpit_receipt_latest_plan_against_snapshot(
    stored: &PulpitReceiptLatestReportView,
    snapshot: &LatestPulpitReceiptSnapshot,
    session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        Some(stored.verdict.as_str()),
        Some(PULPIT_RECEIPT_LATEST_PLAN_READY),
        "stored latest-pulpit-receipt plan verdict",
        mismatches,
    );
    compare_string(
        &stored.mode,
        "plan_latest_pulpit_receipt",
        "stored latest-pulpit-receipt plan mode",
        mismatches,
    );
    compare_option_string(
        stored.latest_top_step_name.as_deref(),
        Some(snapshot.latest_top_step_name.as_str()),
        "stored latest-pulpit-receipt plan latest_top_step_name",
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
        "stored latest-pulpit-receipt plan latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored latest-pulpit-receipt plan session_dir",
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
        "stored latest-pulpit-receipt plan downstream_decision_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.package_dir.as_deref(),
        Some(snapshot.package_dir.display().to_string().as_str()),
        "stored latest-pulpit-receipt plan package_dir",
        mismatches,
    );
    compare_option_string(
        stored.install_root.as_deref(),
        Some(snapshot.install_root.display().to_string().as_str()),
        "stored latest-pulpit-receipt plan install_root",
        mismatches,
    );
    compare_option_string(
        stored.target_service_name.as_deref(),
        Some(snapshot.target_service_name.as_str()),
        "stored latest-pulpit-receipt plan target_service_name",
        mismatches,
    );
    compare_option_string(
        stored.backend_command.as_deref(),
        Some(snapshot.backend_command.as_str()),
        "stored latest-pulpit-receipt plan backend_command",
        mismatches,
    );
    compare_option_u64(
        stored.wrapper_timeout_ms,
        Some(snapshot.wrapper_timeout_ms),
        "stored latest-pulpit-receipt plan wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        stored.service_status_max_staleness_ms,
        Some(snapshot.service_status_max_staleness_ms),
        "stored latest-pulpit-receipt plan service_status_max_staleness_ms",
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
        "stored latest-pulpit-receipt plan reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_summary.as_deref(),
        Some(snapshot.clerestory_certificate_summary.as_str()),
        "stored latest-pulpit-receipt plan clerestory_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_sha256.as_deref(),
        Some(snapshot.clerestory_certificate_sha256.as_str()),
        "stored latest-pulpit-receipt plan clerestory_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_algorithm.as_deref(),
        Some(snapshot.clerestory_certificate_algorithm.as_str()),
        "stored latest-pulpit-receipt plan clerestory_certificate_algorithm",
        mismatches,
    );
}

fn compare_pulpit_receipt_latest_run_against_snapshot(
    stored: &PulpitReceiptLatestReportView,
    snapshot: &LatestPulpitReceiptSnapshot,
    session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &stored.mode,
        "run_latest_pulpit_receipt",
        "stored latest-pulpit-receipt run mode",
        mismatches,
    );
    compare_option_string(
        stored.latest_top_step_name.as_deref(),
        Some(snapshot.latest_top_step_name.as_str()),
        "stored latest-pulpit-receipt run latest_top_step_name",
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
        "stored latest-pulpit-receipt run latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored latest-pulpit-receipt run session_dir",
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
        "stored latest-pulpit-receipt run downstream_decision_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.package_dir.as_deref(),
        Some(snapshot.package_dir.display().to_string().as_str()),
        "stored latest-pulpit-receipt run package_dir",
        mismatches,
    );
    compare_option_string(
        stored.install_root.as_deref(),
        Some(snapshot.install_root.display().to_string().as_str()),
        "stored latest-pulpit-receipt run install_root",
        mismatches,
    );
    compare_option_string(
        stored.target_service_name.as_deref(),
        Some(snapshot.target_service_name.as_str()),
        "stored latest-pulpit-receipt run target_service_name",
        mismatches,
    );
    compare_option_string(
        stored.backend_command.as_deref(),
        Some(snapshot.backend_command.as_str()),
        "stored latest-pulpit-receipt run backend_command",
        mismatches,
    );
    compare_option_u64(
        stored.wrapper_timeout_ms,
        Some(snapshot.wrapper_timeout_ms),
        "stored latest-pulpit-receipt run wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        stored.service_status_max_staleness_ms,
        Some(snapshot.service_status_max_staleness_ms),
        "stored latest-pulpit-receipt run service_status_max_staleness_ms",
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
        "stored latest-pulpit-receipt run reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_summary.as_deref(),
        Some(snapshot.clerestory_certificate_summary.as_str()),
        "stored latest-pulpit-receipt run clerestory_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_sha256.as_deref(),
        Some(snapshot.clerestory_certificate_sha256.as_str()),
        "stored latest-pulpit-receipt run clerestory_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_algorithm.as_deref(),
        Some(snapshot.clerestory_certificate_algorithm.as_str()),
        "stored latest-pulpit-receipt run clerestory_certificate_algorithm",
        mismatches,
    );
}

fn compare_pulpit_receipt_latest_copy_matches_actual(
    stored: &PulpitReceiptLatestReportView,
    actual: &PulpitReceiptLatestReportView,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        Some(stored.verdict.as_str()),
        Some(actual.verdict.as_str()),
        "stored latest-pulpit-receipt copy verdict",
        mismatches,
    );
    compare_option_string(
        Some(stored.reason.as_str()),
        Some(actual.reason.as_str()),
        "stored latest-pulpit-receipt copy reason",
        mismatches,
    );
    if stored.generated_at != actual.generated_at {
        mismatches.push(format!(
            "stored latest-pulpit-receipt copy generated_at {:?} does not match actual {:?}",
            stored.generated_at, actual.generated_at
        ));
    }
    compare_option_string(
        stored.latest_top_step_name.as_deref(),
        actual.latest_top_step_name.as_deref(),
        "stored latest-pulpit-receipt copy latest_top_step_name",
        mismatches,
    );
    compare_option_string(
        stored.latest_top_session_dir.as_deref(),
        actual.latest_top_session_dir.as_deref(),
        "stored latest-pulpit-receipt copy latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        actual.session_dir.as_deref(),
        "stored latest-pulpit-receipt copy session_dir",
        mismatches,
    );
    compare_option_string(
        stored.downstream_decision_packet_session_dir.as_deref(),
        actual.downstream_decision_packet_session_dir.as_deref(),
        "stored latest-pulpit-receipt copy downstream_decision_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.result.as_deref(),
        actual.result.as_deref(),
        "stored latest-pulpit-receipt copy result",
        mismatches,
    );
    compare_option_string(
        stored.explicit_statement.as_deref(),
        actual.explicit_statement.as_deref(),
        "stored latest-pulpit-receipt copy explicit_statement",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_verdict.as_deref(),
        actual.current_pre_activation_gate_verdict.as_deref(),
        "stored latest-pulpit-receipt copy current_pre_activation_gate_verdict",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_reason.as_deref(),
        actual.current_pre_activation_gate_reason.as_deref(),
        "stored latest-pulpit-receipt copy current_pre_activation_gate_reason",
        mismatches,
    );
    compare_option_string(
        stored
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        actual
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        "stored latest-pulpit-receipt copy reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
}

fn compare_chancel_certificate_run_against_snapshot(
    stored: &ChancelCertificateReportView,
    snapshot: &LatestPulpitReceiptSnapshot,
    pulpit_receipt_session_dir: &Path,
    session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &stored.mode,
        "run_live_package_chancel_certificate",
        "stored chancel-certificate run mode",
        mismatches,
    );
    compare_string(
        &stored.pulpit_receipt_session_dir,
        &pulpit_receipt_session_dir.display().to_string(),
        "stored chancel-certificate run pulpit_receipt_session_dir",
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
        "stored chancel-certificate run decision_packet_session_dir",
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
        "stored chancel-certificate run execute_frozen_session_dir",
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
        "stored chancel-certificate run turn_green_session_dir",
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
        "stored chancel-certificate run launch_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.package_dir.as_deref(),
        Some(snapshot.package_dir.display().to_string().as_str()),
        "stored chancel-certificate run package_dir",
        mismatches,
    );
    compare_option_string(
        stored.install_root.as_deref(),
        Some(snapshot.install_root.display().to_string().as_str()),
        "stored chancel-certificate run install_root",
        mismatches,
    );
    compare_option_string(
        stored.target_service_name.as_deref(),
        Some(snapshot.target_service_name.as_str()),
        "stored chancel-certificate run target_service_name",
        mismatches,
    );
    compare_option_string(
        stored.backend_command.as_deref(),
        Some(snapshot.backend_command.as_str()),
        "stored chancel-certificate run backend_command",
        mismatches,
    );
    compare_option_u64(
        stored.wrapper_timeout_ms,
        Some(snapshot.wrapper_timeout_ms),
        "stored chancel-certificate run wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        stored.service_status_max_staleness_ms,
        Some(snapshot.service_status_max_staleness_ms),
        "stored chancel-certificate run service_status_max_staleness_ms",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored chancel-certificate run session_dir",
        mismatches,
    );
    compare_option_string(
        stored.chancel_certificate_session_path.as_deref(),
        Some(
            nested_chancel_certificate_session_path(session_dir)
                .display()
                .to_string()
                .as_str(),
        ),
        "stored chancel-certificate run chancel_certificate_session_path",
        mismatches,
    );
    compare_option_string(
        stored.chancel_certificate_status_path.as_deref(),
        Some(
            nested_chancel_certificate_status_path(session_dir)
                .display()
                .to_string()
                .as_str(),
        ),
        "stored chancel-certificate run chancel_certificate_status_path",
        mismatches,
    );
    compare_option_string(
        stored.archived_pulpit_receipt_report_path.as_deref(),
        Some(
            session_dir
                .join("tiny_live_activation_package_chancel_certificate.pulpit_receipt.report.json")
                .display()
                .to_string()
                .as_str(),
        ),
        "stored chancel-certificate run archived_pulpit_receipt_report_path",
        mismatches,
    );
    compare_option_string(
        stored.verify_pulpit_receipt_command_summary.as_deref(),
        Some(verify_chancel_certificate_input_command_summary(pulpit_receipt_session_dir).as_str()),
        "stored chancel-certificate run verify_pulpit_receipt_command_summary",
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
        "stored chancel-certificate run reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
}

fn compare_chancel_certificate_copy_against_nested_truth(
    stored: &ChancelCertificateReportView,
    nested_session: &ChancelCertificateSessionView,
    nested_status: &ChancelCertificateStatusView,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        stored.pedestal_certificate_session_dir.as_deref(),
        Some(nested_session.pedestal_certificate_session_dir.as_str()),
        "stored chancel-certificate copy pedestal_certificate_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.plinth_receipt_session_dir.as_deref(),
        Some(nested_session.plinth_receipt_session_dir.as_str()),
        "stored chancel-certificate copy plinth_receipt_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.registry_entry_session_dir.as_deref(),
        Some(nested_session.registry_entry_session_dir.as_str()),
        "stored chancel-certificate copy registry_entry_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.notarization_receipt_session_dir.as_deref(),
        Some(nested_session.notarization_receipt_session_dir.as_str()),
        "stored chancel-certificate copy notarization_receipt_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.provenance_certificate_session_dir.as_deref(),
        Some(nested_session.provenance_certificate_session_dir.as_str()),
        "stored chancel-certificate copy provenance_certificate_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.attestation_seal_session_dir.as_deref(),
        Some(nested_session.attestation_seal_session_dir.as_str()),
        "stored chancel-certificate copy attestation_seal_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.release_capsule_session_dir.as_deref(),
        Some(nested_session.release_capsule_session_dir.as_str()),
        "stored chancel-certificate copy release_capsule_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.activation_ticket_session_dir.as_deref(),
        Some(nested_session.activation_ticket_session_dir.as_str()),
        "stored chancel-certificate copy activation_ticket_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.review_receipt_session_dir.as_deref(),
        Some(nested_session.review_receipt_session_dir.as_str()),
        "stored chancel-certificate copy review_receipt_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.handoff_bundle_session_dir.as_deref(),
        Some(nested_session.handoff_bundle_session_dir.as_str()),
        "stored chancel-certificate copy handoff_bundle_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.decision_packet_session_dir.as_deref(),
        Some(nested_session.decision_packet_session_dir.as_str()),
        "stored chancel-certificate copy decision_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.execute_frozen_session_dir.as_deref(),
        Some(nested_session.execute_frozen_session_dir.as_str()),
        "stored chancel-certificate copy execute_frozen_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.turn_green_session_dir.as_deref(),
        Some(nested_session.turn_green_session_dir.as_str()),
        "stored chancel-certificate copy turn_green_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.launch_packet_session_dir.as_deref(),
        Some(nested_session.launch_packet_session_dir.as_str()),
        "stored chancel-certificate copy launch_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(nested_session.session_dir.as_str()),
        "stored chancel-certificate copy session_dir",
        mismatches,
    );
    compare_option_string(
        stored.result.as_deref(),
        Some(nested_status.result.as_str()),
        "stored chancel-certificate copy result",
        mismatches,
    );
    compare_option_string(
        Some(stored.verdict.as_str()),
        chancel_certificate_run_verdict_for_result(&nested_status.result),
        "stored chancel-certificate copy verdict",
        mismatches,
    );
    compare_option_string(
        stored.handoff_bundle_result.as_deref(),
        nested_status.handoff_bundle_result.as_deref(),
        "stored chancel-certificate copy handoff_bundle_result",
        mismatches,
    );
    compare_option_string(
        stored.decision_packet_result.as_deref(),
        nested_status.decision_packet_result.as_deref(),
        "stored chancel-certificate copy decision_packet_result",
        mismatches,
    );
    compare_option_string(
        stored.execute_frozen_result.as_deref(),
        nested_status.execute_frozen_result.as_deref(),
        "stored chancel-certificate copy execute_frozen_result",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_verdict.as_deref(),
        nested_status.current_pre_activation_gate_verdict.as_deref(),
        "stored chancel-certificate copy current_pre_activation_gate_verdict",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_reason.as_deref(),
        nested_status.current_pre_activation_gate_reason.as_deref(),
        "stored chancel-certificate copy current_pre_activation_gate_reason",
        mismatches,
    );
    compare_option_string(
        stored.verify_pulpit_receipt_command_summary.as_deref(),
        Some(
            nested_session
                .verify_pulpit_receipt_command_summary
                .as_str(),
        ),
        "stored chancel-certificate copy verify_pulpit_receipt_command_summary",
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
        "stored chancel-certificate copy reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_option_string(
        stored.provenance_certificate_summary.as_deref(),
        Some(nested_status.provenance_certificate_summary.as_str()),
        "stored chancel-certificate copy provenance_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.chain_fingerprint_summary.as_deref(),
        Some(nested_status.chain_fingerprint_summary.as_str()),
        "stored chancel-certificate copy chain_fingerprint_summary",
        mismatches,
    );
    compare_option_string(
        stored.chain_fingerprint_sha256.as_deref(),
        Some(nested_status.chain_fingerprint_sha256.as_str()),
        "stored chancel-certificate copy chain_fingerprint_sha256",
        mismatches,
    );
    compare_option_string(
        stored.chain_fingerprint_algorithm.as_deref(),
        Some(nested_status.chain_fingerprint_algorithm.as_str()),
        "stored chancel-certificate copy chain_fingerprint_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.notarization_receipt_summary.as_deref(),
        Some(nested_status.notarization_receipt_summary.as_str()),
        "stored chancel-certificate copy notarization_receipt_summary",
        mismatches,
    );
    compare_option_string(
        stored.release_capsule_digest_manifest_sha256.as_deref(),
        Some(
            nested_status
                .release_capsule_digest_manifest_sha256
                .as_str(),
        ),
        "stored chancel-certificate copy release_capsule_digest_manifest_sha256",
        mismatches,
    );
    compare_option_usize(
        stored.release_capsule_digest_manifest_entry_count,
        Some(nested_status.release_capsule_digest_manifest_entry_count),
        "stored chancel-certificate copy release_capsule_digest_manifest_entry_count",
        mismatches,
    );
    compare_option_string(
        stored.release_capsule_digest_algorithm.as_deref(),
        Some(nested_status.release_capsule_digest_algorithm.as_str()),
        "stored chancel-certificate copy release_capsule_digest_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.ledger_seal_summary.as_deref(),
        Some(nested_status.ledger_seal_summary.as_str()),
        "stored chancel-certificate copy ledger_seal_summary",
        mismatches,
    );
    compare_option_string(
        stored.ledger_seal_sha256.as_deref(),
        Some(nested_status.ledger_seal_sha256.as_str()),
        "stored chancel-certificate copy ledger_seal_sha256",
        mismatches,
    );
    compare_option_string(
        stored.ledger_seal_algorithm.as_deref(),
        Some(nested_status.ledger_seal_algorithm.as_str()),
        "stored chancel-certificate copy ledger_seal_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.registry_entry_summary.as_deref(),
        Some(nested_status.registry_entry_summary.as_str()),
        "stored chancel-certificate copy registry_entry_summary",
        mismatches,
    );
    compare_option_string(
        stored.registry_entry_sha256.as_deref(),
        Some(nested_status.registry_entry_sha256.as_str()),
        "stored chancel-certificate copy registry_entry_sha256",
        mismatches,
    );
    compare_option_string(
        stored.registry_entry_algorithm.as_deref(),
        Some(nested_status.registry_entry_algorithm.as_str()),
        "stored chancel-certificate copy registry_entry_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.filing_certificate_summary.as_deref(),
        Some(nested_status.filing_certificate_summary.as_str()),
        "stored chancel-certificate copy filing_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.filing_certificate_sha256.as_deref(),
        Some(nested_status.filing_certificate_sha256.as_str()),
        "stored chancel-certificate copy filing_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.filing_certificate_algorithm.as_deref(),
        Some(nested_status.filing_certificate_algorithm.as_str()),
        "stored chancel-certificate copy filing_certificate_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.archive_receipt_summary.as_deref(),
        Some(nested_status.archive_receipt_summary.as_str()),
        "stored chancel-certificate copy archive_receipt_summary",
        mismatches,
    );
    compare_option_string(
        stored.archive_receipt_sha256.as_deref(),
        Some(nested_status.archive_receipt_sha256.as_str()),
        "stored chancel-certificate copy archive_receipt_sha256",
        mismatches,
    );
    compare_option_string(
        stored.archive_receipt_algorithm.as_deref(),
        Some(nested_status.archive_receipt_algorithm.as_str()),
        "stored chancel-certificate copy archive_receipt_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.closure_certificate_summary.as_deref(),
        Some(nested_status.closure_certificate_summary.as_str()),
        "stored chancel-certificate copy closure_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.closure_certificate_sha256.as_deref(),
        Some(nested_status.closure_certificate_sha256.as_str()),
        "stored chancel-certificate copy closure_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.closure_certificate_algorithm.as_deref(),
        Some(nested_status.closure_certificate_algorithm.as_str()),
        "stored chancel-certificate copy closure_certificate_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.finality_receipt_summary.as_deref(),
        Some(nested_status.finality_receipt_summary.as_str()),
        "stored chancel-certificate copy finality_receipt_summary",
        mismatches,
    );
    compare_option_string(
        stored.finality_receipt_sha256.as_deref(),
        Some(nested_status.finality_receipt_sha256.as_str()),
        "stored chancel-certificate copy finality_receipt_sha256",
        mismatches,
    );
    compare_option_string(
        stored.finality_receipt_algorithm.as_deref(),
        Some(nested_status.finality_receipt_algorithm.as_str()),
        "stored chancel-certificate copy finality_receipt_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.consummation_record_summary.as_deref(),
        Some(nested_status.consummation_record_summary.as_str()),
        "stored chancel-certificate copy consummation_record_summary",
        mismatches,
    );
    compare_option_string(
        stored.consummation_record_sha256.as_deref(),
        Some(nested_status.consummation_record_sha256.as_str()),
        "stored chancel-certificate copy consummation_record_sha256",
        mismatches,
    );
    compare_option_string(
        stored.consummation_record_algorithm.as_deref(),
        Some(nested_status.consummation_record_algorithm.as_str()),
        "stored chancel-certificate copy consummation_record_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.completion_certificate_summary.as_deref(),
        Some(nested_status.completion_certificate_summary.as_str()),
        "stored chancel-certificate copy completion_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.completion_certificate_sha256.as_deref(),
        Some(nested_status.completion_certificate_sha256.as_str()),
        "stored chancel-certificate copy completion_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.completion_certificate_algorithm.as_deref(),
        Some(nested_status.completion_certificate_algorithm.as_str()),
        "stored chancel-certificate copy completion_certificate_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.culmination_receipt_summary.as_deref(),
        Some(nested_status.culmination_receipt_summary.as_str()),
        "stored chancel-certificate copy culmination_receipt_summary",
        mismatches,
    );
    compare_option_string(
        stored.culmination_receipt_sha256.as_deref(),
        Some(nested_status.culmination_receipt_sha256.as_str()),
        "stored chancel-certificate copy culmination_receipt_sha256",
        mismatches,
    );
    compare_option_string(
        stored.culmination_receipt_algorithm.as_deref(),
        Some(nested_status.culmination_receipt_algorithm.as_str()),
        "stored chancel-certificate copy culmination_receipt_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.summit_certificate_summary.as_deref(),
        Some(nested_status.summit_certificate_summary.as_str()),
        "stored chancel-certificate copy summit_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.summit_certificate_sha256.as_deref(),
        Some(nested_status.summit_certificate_sha256.as_str()),
        "stored chancel-certificate copy summit_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.summit_certificate_algorithm.as_deref(),
        Some(nested_status.summit_certificate_algorithm.as_str()),
        "stored chancel-certificate copy summit_certificate_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.pinnacle_receipt_summary.as_deref(),
        Some(nested_status.pinnacle_receipt_summary.as_str()),
        "stored chancel-certificate copy pinnacle_receipt_summary",
        mismatches,
    );
    compare_option_string(
        stored.pinnacle_receipt_sha256.as_deref(),
        Some(nested_status.pinnacle_receipt_sha256.as_str()),
        "stored chancel-certificate copy pinnacle_receipt_sha256",
        mismatches,
    );
    compare_option_string(
        stored.pinnacle_receipt_algorithm.as_deref(),
        Some(nested_status.pinnacle_receipt_algorithm.as_str()),
        "stored chancel-certificate copy pinnacle_receipt_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.keystone_receipt_summary.as_deref(),
        Some(nested_status.keystone_receipt_summary.as_str()),
        "stored chancel-certificate copy keystone_receipt_summary",
        mismatches,
    );
    compare_option_string(
        stored.keystone_receipt_sha256.as_deref(),
        Some(nested_status.keystone_receipt_sha256.as_str()),
        "stored chancel-certificate copy keystone_receipt_sha256",
        mismatches,
    );
    compare_option_string(
        stored.keystone_receipt_algorithm.as_deref(),
        Some(nested_status.keystone_receipt_algorithm.as_str()),
        "stored chancel-certificate copy keystone_receipt_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.cornerstone_certificate_summary.as_deref(),
        Some(nested_status.cornerstone_certificate_summary.as_str()),
        "stored chancel-certificate copy cornerstone_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.cornerstone_certificate_sha256.as_deref(),
        Some(nested_status.cornerstone_certificate_sha256.as_str()),
        "stored chancel-certificate copy cornerstone_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.cornerstone_certificate_algorithm.as_deref(),
        Some(nested_status.cornerstone_certificate_algorithm.as_str()),
        "stored chancel-certificate copy cornerstone_certificate_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.capstone_certificate_summary.as_deref(),
        Some(nested_status.capstone_certificate_summary.as_str()),
        "stored chancel-certificate copy capstone_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.capstone_certificate_sha256.as_deref(),
        Some(nested_status.capstone_certificate_sha256.as_str()),
        "stored chancel-certificate copy capstone_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.capstone_certificate_algorithm.as_deref(),
        Some(nested_status.capstone_certificate_algorithm.as_str()),
        "stored chancel-certificate copy capstone_certificate_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.foundation_receipt_summary.as_deref(),
        Some(nested_status.foundation_receipt_summary.as_str()),
        "stored chancel-certificate copy foundation_receipt_summary",
        mismatches,
    );
    compare_option_string(
        stored.foundation_receipt_sha256.as_deref(),
        Some(nested_status.foundation_receipt_sha256.as_str()),
        "stored chancel-certificate copy foundation_receipt_sha256",
        mismatches,
    );
    compare_option_string(
        stored.foundation_receipt_algorithm.as_deref(),
        Some(nested_status.foundation_receipt_algorithm.as_str()),
        "stored chancel-certificate copy foundation_receipt_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.bedrock_certificate_summary.as_deref(),
        Some(nested_status.bedrock_certificate_summary.as_str()),
        "stored chancel-certificate copy bedrock_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.bedrock_certificate_sha256.as_deref(),
        Some(nested_status.bedrock_certificate_sha256.as_str()),
        "stored chancel-certificate copy bedrock_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.bedrock_certificate_algorithm.as_deref(),
        Some(nested_status.bedrock_certificate_algorithm.as_str()),
        "stored chancel-certificate copy bedrock_certificate_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.basal_receipt_summary.as_deref(),
        Some(nested_status.basal_receipt_summary.as_str()),
        "stored chancel-certificate copy basal_receipt_summary",
        mismatches,
    );
    compare_option_string(
        stored.basal_receipt_sha256.as_deref(),
        Some(nested_status.basal_receipt_sha256.as_str()),
        "stored chancel-certificate copy basal_receipt_sha256",
        mismatches,
    );
    compare_option_string(
        stored.basal_receipt_algorithm.as_deref(),
        Some(nested_status.basal_receipt_algorithm.as_str()),
        "stored chancel-certificate copy basal_receipt_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.substructure_certificate_summary.as_deref(),
        Some(nested_status.substructure_certificate_summary.as_str()),
        "stored chancel-certificate copy substructure_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.substructure_certificate_sha256.as_deref(),
        Some(nested_status.substructure_certificate_sha256.as_str()),
        "stored chancel-certificate copy substructure_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.substructure_certificate_algorithm.as_deref(),
        Some(nested_status.substructure_certificate_algorithm.as_str()),
        "stored chancel-certificate copy substructure_certificate_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.plinth_receipt_summary.as_deref(),
        Some(nested_status.plinth_receipt_summary.as_str()),
        "stored chancel-certificate copy plinth_receipt_summary",
        mismatches,
    );
    compare_option_string(
        stored.plinth_receipt_sha256.as_deref(),
        Some(nested_status.plinth_receipt_sha256.as_str()),
        "stored chancel-certificate copy plinth_receipt_sha256",
        mismatches,
    );
    compare_option_string(
        stored.plinth_receipt_algorithm.as_deref(),
        Some(nested_status.plinth_receipt_algorithm.as_str()),
        "stored chancel-certificate copy plinth_receipt_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.pedestal_certificate_summary.as_deref(),
        Some(nested_status.pedestal_certificate_summary.as_str()),
        "stored chancel-certificate copy pedestal_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.pedestal_certificate_sha256.as_deref(),
        Some(nested_status.pedestal_certificate_sha256.as_str()),
        "stored chancel-certificate copy pedestal_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.pedestal_certificate_algorithm.as_deref(),
        Some(nested_status.pedestal_certificate_algorithm.as_str()),
        "stored chancel-certificate copy pedestal_certificate_algorithm",
        mismatches,
    );
    compare_option_string(
        stored.pulpit_receipt_summary.as_deref(),
        Some(nested_status.pulpit_receipt_summary.as_str()),
        "stored chancel-certificate copy pulpit_receipt_summary",
        mismatches,
    );
    compare_option_string(
        stored.pulpit_receipt_sha256.as_deref(),
        Some(nested_status.pulpit_receipt_sha256.as_str()),
        "stored chancel-certificate copy pulpit_receipt_sha256",
        mismatches,
    );
    compare_option_string(
        stored.pulpit_receipt_algorithm.as_deref(),
        Some(nested_status.pulpit_receipt_algorithm.as_str()),
        "stored chancel-certificate copy pulpit_receipt_algorithm",
        mismatches,
    );
    compare_string(
        &stored.reason,
        &nested_status.reason,
        "stored chancel-certificate copy reason",
        mismatches,
    );
    compare_string(
        &stored.explicit_statement,
        &nested_status.explicit_statement,
        "stored chancel-certificate copy explicit_statement",
        mismatches,
    );
}

fn compare_session_against_snapshot(
    session: &PackageChancelCertificateLatestSession,
    root: &Path,
    session_dir: &Path,
    paths: &PackageChancelCertificateLatestPaths,
    snapshot: &LatestPulpitReceiptSnapshot,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &session.resolve_latest_pulpit_receipt_command_summary,
        &resolve_latest_pulpit_receipt_command_summary(
            root,
            &paths.nested_pulpit_receipt_latest_session_dir,
        ),
        "stored session resolve_latest_pulpit_receipt_command_summary",
        mismatches,
    );
    compare_string(
        &session.run_latest_chancel_certificate_command_summary,
        &run_latest_chancel_certificate_command_summary(root, session_dir),
        "stored session run_latest_chancel_certificate_command_summary",
        mismatches,
    );
    compare_string(
        &session.verify_latest_chancel_certificate_command_summary,
        &verify_latest_chancel_certificate_command_summary(session_dir),
        "stored session verify_latest_chancel_certificate_command_summary",
        mismatches,
    );
    compare_string(
        &session.downstream_chancel_certificate_command_summary,
        &downstream_chancel_certificate_command_summary(
            snapshot,
            &expected_downstream_pulpit_receipt_session_dir(paths),
            &paths.nested_chancel_certificate_session_dir,
        ),
        "stored session downstream_chancel_certificate_command_summary",
        mismatches,
    );
    compare_string(
        &session.verify_chancel_certificate_command_summary,
        &verify_chancel_certificate_command_summary(
            snapshot,
            &expected_downstream_pulpit_receipt_session_dir(paths),
            &paths.nested_chancel_certificate_session_dir,
        ),
        "stored session verify_chancel_certificate_command_summary",
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
        &session.nested_pulpit_receipt_latest_session_dir,
        &paths
            .nested_pulpit_receipt_latest_session_dir
            .display()
            .to_string(),
        "stored session nested_pulpit_receipt_latest_session_dir",
        mismatches,
    );
    compare_string(
        &session.nested_chancel_certificate_session_dir,
        &paths
            .nested_chancel_certificate_session_dir
            .display()
            .to_string(),
        "stored session nested_chancel_certificate_session_dir",
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
    report: &PackageChancelCertificateLatestReport,
    root: &Path,
    session_dir: &Path,
    paths: &PackageChancelCertificateLatestPaths,
    snapshot: &LatestPulpitReceiptSnapshot,
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
            .resolve_latest_pulpit_receipt_command_summary
            .as_deref(),
        Some(
            resolve_latest_pulpit_receipt_command_summary(
                root,
                &paths.nested_pulpit_receipt_latest_session_dir,
            )
            .as_str(),
        ),
        "stored report resolve_latest_pulpit_receipt_command_summary",
        mismatches,
    );
    compare_option_string(
        report
            .run_latest_chancel_certificate_command_summary
            .as_deref(),
        Some(run_latest_chancel_certificate_command_summary(root, session_dir).as_str()),
        "stored report run_latest_chancel_certificate_command_summary",
        mismatches,
    );
    compare_option_string(
        report
            .verify_latest_chancel_certificate_command_summary
            .as_deref(),
        Some(verify_latest_chancel_certificate_command_summary(session_dir).as_str()),
        "stored report verify_latest_chancel_certificate_command_summary",
        mismatches,
    );
    compare_option_string(
        report
            .downstream_chancel_certificate_command_summary
            .as_deref(),
        Some(
            downstream_chancel_certificate_command_summary(
                snapshot,
                &expected_downstream_pulpit_receipt_session_dir(paths),
                &paths.nested_chancel_certificate_session_dir,
            )
            .as_str(),
        ),
        "stored report downstream_chancel_certificate_command_summary",
        mismatches,
    );
    compare_option_string(
        report.verify_chancel_certificate_command_summary.as_deref(),
        Some(
            verify_chancel_certificate_command_summary(
                snapshot,
                &expected_downstream_pulpit_receipt_session_dir(paths),
                &paths.nested_chancel_certificate_session_dir,
            )
            .as_str(),
        ),
        "stored report verify_chancel_certificate_command_summary",
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
        report.chancel_certificate_latest_session_path.as_deref(),
        Some(paths.session_path.display().to_string().as_str()),
        "stored report chancel_certificate_latest_session_path",
        mismatches,
    );
    compare_option_string(
        report.chancel_certificate_latest_status_path.as_deref(),
        Some(paths.status_path.display().to_string().as_str()),
        "stored report chancel_certificate_latest_status_path",
        mismatches,
    );
    compare_option_string(
        report.chancel_certificate_latest_report_path.as_deref(),
        Some(paths.report_path.display().to_string().as_str()),
        "stored report chancel_certificate_latest_report_path",
        mismatches,
    );
    compare_option_string(
        report.latest_pulpit_receipt_plan_report_path.as_deref(),
        Some(
            paths
                .latest_pulpit_receipt_plan_report_path
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report latest_pulpit_receipt_plan_report_path",
        mismatches,
    );
    compare_option_string(
        report.latest_pulpit_receipt_report_path.as_deref(),
        Some(
            paths
                .latest_pulpit_receipt_report_path
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report latest_pulpit_receipt_report_path",
        mismatches,
    );
    compare_option_string(
        report.chancel_certificate_report_path.as_deref(),
        Some(
            paths
                .chancel_certificate_report_path
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report chancel_certificate_report_path",
        mismatches,
    );
    compare_option_string(
        report.nested_pulpit_receipt_latest_session_dir.as_deref(),
        Some(
            paths
                .nested_pulpit_receipt_latest_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report nested_pulpit_receipt_latest_session_dir",
        mismatches,
    );
    compare_option_string(
        report.nested_chancel_certificate_session_dir.as_deref(),
        Some(
            paths
                .nested_chancel_certificate_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report nested_chancel_certificate_session_dir",
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
    session: &PackageChancelCertificateLatestSession,
    root: &Path,
    session_dir: &Path,
    paths: &PackageChancelCertificateLatestPaths,
    failure: &LatestPulpitReceiptResolutionFailure,
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
        &session.nested_pulpit_receipt_latest_session_dir,
        &paths
            .nested_pulpit_receipt_latest_session_dir
            .display()
            .to_string(),
        "stored session nested_pulpit_receipt_latest_session_dir on failure",
        mismatches,
    );
    compare_string(
        &session.nested_chancel_certificate_session_dir,
        &paths
            .nested_chancel_certificate_session_dir
            .display()
            .to_string(),
        "stored session nested_chancel_certificate_session_dir on failure",
        mismatches,
    );
    if let Some(snapshot) = failure.snapshot.as_ref() {
        compare_session_against_snapshot(session, root, session_dir, paths, snapshot, mismatches);
    }
}

fn compare_report_against_failure(
    report: &PackageChancelCertificateLatestReport,
    root: &Path,
    session_dir: &Path,
    paths: &PackageChancelCertificateLatestPaths,
    failure: &LatestPulpitReceiptResolutionFailure,
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
        "run_latest_chancel_certificate",
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

fn map_pulpit_receipt_latest_failure_kind(verdict: &str) -> ResolutionFailureKind {
    match verdict {
        "tiny_live_package_pulpit_receipt_latest_refused_by_missing_latest_chain" => {
            ResolutionFailureKind::MissingLatestChain
        }
        "tiny_live_package_pulpit_receipt_latest_refused_by_drifted_pulpit_receipt_contract" => {
            ResolutionFailureKind::DriftedPulpitReceiptContract
        }
        _ => ResolutionFailureKind::InvalidLatestChain,
    }
}

fn latest_pulpit_receipt_terminal_failure(
    report: &PulpitReceiptLatestReportView,
) -> Option<LivePackageChancelCertificateLatestResult> {
    match report.result.as_deref() {
        Some("refused_by_missing_latest_chain") => {
            Some(LivePackageChancelCertificateLatestResult::RefusedByMissingLatestChain)
        }
        Some("refused_by_invalid_latest_chain") => {
            Some(LivePackageChancelCertificateLatestResult::RefusedByInvalidLatestChain)
        }
        Some("refused_by_drifted_pulpit_receipt_contract") => Some(
            LivePackageChancelCertificateLatestResult::RefusedByDriftedChancelCertificateContract,
        ),
        _ => None,
    }
}

fn pulpit_receipt_latest_result_from_nested_status(
    result: &str,
) -> Option<LivePackageChancelCertificateLatestResult> {
    match result {
        "ready_for_manual_execution_when_gate_turns_green" => Some(
            LivePackageChancelCertificateLatestResult::ReadyForManualExecutionWhenGateTurnsGreen,
        ),
        "refused_now_by_stage3" => {
            Some(LivePackageChancelCertificateLatestResult::RefusedNowByStage3)
        }
        "refused_now_by_pre_activation_gate" => {
            Some(LivePackageChancelCertificateLatestResult::RefusedNowByPreActivationGate)
        }
        "refused_now_by_invalid_or_drifted_contract" => Some(
            LivePackageChancelCertificateLatestResult::RefusedByDriftedChancelCertificateContract,
        ),
        _ => None,
    }
}

fn chancel_certificate_run_verdict_for_result(result: &str) -> Option<&'static str> {
    match result {
        "ready_for_manual_execution_when_gate_turns_green" => Some(
            "tiny_live_package_chancel_certificate_ready_for_manual_execution_when_gate_turns_green",
        ),
        "refused_now_by_stage3" => Some("tiny_live_package_chancel_certificate_refused_now_by_stage3"),
        "refused_now_by_pre_activation_gate" => {
            Some("tiny_live_package_chancel_certificate_refused_now_by_pre_activation_gate")
        }
        "refused_now_by_invalid_or_drifted_contract" => {
            Some("tiny_live_package_chancel_certificate_refused_now_by_invalid_or_drifted_contract")
        }
        _ => None,
    }
}

fn result_from_failure_kind(
    kind: ResolutionFailureKind,
) -> LivePackageChancelCertificateLatestResult {
    match kind {
        ResolutionFailureKind::MissingLatestChain => {
            LivePackageChancelCertificateLatestResult::RefusedByMissingLatestChain
        }
        ResolutionFailureKind::InvalidLatestChain => {
            LivePackageChancelCertificateLatestResult::RefusedByInvalidLatestChain
        }
        ResolutionFailureKind::DriftedPulpitReceiptContract => {
            LivePackageChancelCertificateLatestResult::RefusedByDriftedChancelCertificateContract
        }
    }
}

fn verdict_for_result(
    result: LivePackageChancelCertificateLatestResult,
) -> TinyLivePackageChancelCertificateLatestVerdict {
    match result {
        LivePackageChancelCertificateLatestResult::RefusedByMissingLatestChain => {
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestRefusedByMissingLatestChain
        }
        LivePackageChancelCertificateLatestResult::RefusedByInvalidLatestChain => {
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestRefusedByInvalidLatestChain
        }
        LivePackageChancelCertificateLatestResult::RefusedByDriftedChancelCertificateContract => {
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestRefusedByDriftedChancelCertificateContract
        }
        LivePackageChancelCertificateLatestResult::RefusedNowByStage3 => {
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestRefusedNowByStage3
        }
        LivePackageChancelCertificateLatestResult::RefusedNowByPreActivationGate => {
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestRefusedNowByPreActivationGate
        }
        LivePackageChancelCertificateLatestResult::ReadyForManualExecutionWhenGateTurnsGreen => {
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestReadyForManualExecutionWhenGateTurnsGreen
        }
    }
}

fn verdict_from_failure_kind(
    kind: ResolutionFailureKind,
) -> TinyLivePackageChancelCertificateLatestVerdict {
    verdict_for_result(result_from_failure_kind(kind))
}

fn operator_next_action_summary(result: LivePackageChancelCertificateLatestResult) -> String {
    match result {
        LivePackageChancelCertificateLatestResult::RefusedByMissingLatestChain => {
            "no immutable clerestory chain is available yet; do not manually hunt latest-pulpit-receipt or chancel-certificate lineage by hand".to_string()
        }
        LivePackageChancelCertificateLatestResult::RefusedByInvalidLatestChain => {
            "the latest immutable chain is incomplete or below the accepted clerestory layer; mint a fresh coherent clerestory chain instead of bypassing the accepted latest-pulpit-receipt / chancel-certificate path".to_string()
        }
        LivePackageChancelCertificateLatestResult::RefusedByDriftedChancelCertificateContract => {
            "the latest chain no longer resolves cleanly into the accepted latest-pulpit-receipt / chancel-certificate contract; repair that drift instead of bypassing the bounded handoff path".to_string()
        }
        LivePackageChancelCertificateLatestResult::RefusedNowByStage3 => {
            "the frozen controller stays refused now by Stage 3; rerun this latest-chancel-certificate refresh after current Stage 3 truth turns green".to_string()
        }
        LivePackageChancelCertificateLatestResult::RefusedNowByPreActivationGate => {
            "the frozen controller stays refused now by the pre-activation gate; rerun this latest-chancel-certificate refresh after the current pre-activation gate turns green".to_string()
        }
        LivePackageChancelCertificateLatestResult::ReadyForManualExecutionWhenGateTurnsGreen => {
            "the latest immutable chain now resolves into the accepted chancel-certificate contract and one bounded latest-chancel-certificate handoff is ready for manual execution when gate truth turns green; this wrapper still did not run or authorize the frozen controller".to_string()
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

fn verify_chancel_certificate_input_command_summary(pulpit_receipt_session_dir: &Path) -> String {
    format!(
        "verify immutable pulpit-receipt session under {} before freezing the final chancel certificate",
        pulpit_receipt_session_dir.display()
    )
}

fn refusal_status(
    root: &Path,
    session_dir: &Path,
    paths: &PackageChancelCertificateLatestPaths,
    resolved: Option<&ResolvedLatestPulpitReceipt>,
    result: LivePackageChancelCertificateLatestResult,
    reason: String,
    latest_pulpit_receipt_plan_step: Option<PackageChancelCertificateLatestStepArtifact>,
    latest_pulpit_receipt_step: Option<PackageChancelCertificateLatestStepArtifact>,
    chancel_certificate_step: Option<PackageChancelCertificateLatestStepArtifact>,
) -> PackageChancelCertificateLatestStatus {
    let snapshot = resolved.map(|value| &value.snapshot);
    PackageChancelCertificateLatestStatus {
        status_version: CHANCEL_CERTIFICATE_LATEST_STATUS_VERSION.to_string(),
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
        nested_pulpit_receipt_latest_session_dir: paths
            .nested_pulpit_receipt_latest_session_dir
            .display()
            .to_string(),
        nested_chancel_certificate_session_dir: paths
            .nested_chancel_certificate_session_dir
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
        latest_pulpit_receipt_plan_verdict: resolved
            .map(|value| value.latest_pulpit_receipt_plan.verdict.clone()),
        latest_pulpit_receipt_verdict: latest_pulpit_receipt_step
            .as_ref()
            .map(|value| value.verdict.clone()),
        chancel_certificate_verdict: chancel_certificate_step
            .as_ref()
            .map(|value| value.verdict.clone()),
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        latest_pulpit_receipt_plan_step,
        latest_pulpit_receipt_step,
        chancel_certificate_step,
        operator_next_action_summary: operator_next_action_summary(result),
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn refusal_status_from_resolution_failure(
    root: &Path,
    session_dir: &Path,
    paths: &PackageChancelCertificateLatestPaths,
    failure: &LatestPulpitReceiptResolutionFailure,
) -> PackageChancelCertificateLatestStatus {
    PackageChancelCertificateLatestStatus {
        status_version: CHANCEL_CERTIFICATE_LATEST_STATUS_VERSION.to_string(),
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
        nested_pulpit_receipt_latest_session_dir: paths
            .nested_pulpit_receipt_latest_session_dir
            .display()
            .to_string(),
        nested_chancel_certificate_session_dir: paths
            .nested_chancel_certificate_session_dir
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
        latest_pulpit_receipt_plan_verdict: failure.latest_pulpit_receipt_plan_verdict.clone(),
        latest_pulpit_receipt_verdict: None,
        chancel_certificate_verdict: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        latest_pulpit_receipt_plan_step: failure.latest_pulpit_receipt_plan_raw.as_ref().map(
            |_| {
                step_artifact(
                    &paths.latest_pulpit_receipt_plan_report_path,
                    "plan_latest_pulpit_receipt",
                    failure
                        .latest_pulpit_receipt_plan_verdict
                        .as_deref()
                        .unwrap_or("<missing>"),
                    failure
                        .latest_pulpit_receipt_plan_reason
                        .as_deref()
                        .unwrap_or("<missing>"),
                    failure
                        .latest_pulpit_receipt_plan_generated_at
                        .unwrap_or_else(Utc::now),
                )
            },
        ),
        latest_pulpit_receipt_step: None,
        chancel_certificate_step: None,
        operator_next_action_summary: operator_next_action_summary(result_from_failure_kind(
            failure.kind,
        )),
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn verify_invalid_chancel_certificate_latest_report_without_session(
    session_dir: &Path,
    verification_mismatches: Vec<String>,
) -> PackageChancelCertificateLatestReport {
    let reason = verification_mismatches
        .first()
        .cloned()
        .unwrap_or_else(|| "latest-chancel-certificate verification failed".to_string());
    PackageChancelCertificateLatestReport {
        generated_at: Utc::now(),
        mode: "verify_latest_chancel_certificate".to_string(),
        verdict: TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid,
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
        chancel_certificate_latest_session_path: Some(
            chancel_certificate_latest_paths(session_dir)
                .session_path
                .display()
                .to_string(),
        ),
        chancel_certificate_latest_status_path: Some(
            chancel_certificate_latest_paths(session_dir)
                .status_path
                .display()
                .to_string(),
        ),
        chancel_certificate_latest_report_path: Some(
            chancel_certificate_latest_paths(session_dir)
                .report_path
                .display()
                .to_string(),
        ),
        latest_pulpit_receipt_plan_report_path: None,
        latest_pulpit_receipt_report_path: None,
        chancel_certificate_report_path: None,
        nested_pulpit_receipt_latest_session_dir: None,
        nested_chancel_certificate_session_dir: None,
        downstream_decision_packet_session_dir: None,
        latest_pulpit_receipt_plan_verdict: None,
        latest_pulpit_receipt_verdict: None,
        chancel_certificate_verdict: None,
        result: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        resolve_latest_pulpit_receipt_command_summary: None,
        run_latest_chancel_certificate_command_summary: None,
        verify_latest_chancel_certificate_command_summary: None,
        downstream_chancel_certificate_command_summary: None,
        verify_chancel_certificate_command_summary: None,
        reviewed_frozen_live_cutover_controller_command_summary: None,
        clerestory_certificate_summary: None,
        clerestory_certificate_sha256: None,
        clerestory_certificate_algorithm: None,
        verification_mismatches,
        activation_authorized: false,
        explicit_statement:
            "this verification proves only whether the latest immutable chain still resolves cleanly into the accepted chancel-certificate contract; it never runs the frozen controller"
                .to_string(),
    }
}

fn fallback_pulpit_receipt_latest_copy() -> PulpitReceiptLatestReportView {
    PulpitReceiptLatestReportView {
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
    snapshot: &LatestPulpitReceiptSnapshot,
    session_dir: &Path,
) -> ChancelCertificateSessionView {
    ChancelCertificateSessionView {
        pulpit_receipt_session_dir: "<missing>".to_string(),
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
        verify_pulpit_receipt_command_summary:
            verify_chancel_certificate_input_command_summary(Path::new("<missing>")),
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
        pulpit_receipt_summary:
            "Pulpit receipt identity: fallback session is synthetic and must not verify green."
                .to_string(),
        pulpit_receipt_sha256: "<missing>".to_string(),
        pulpit_receipt_algorithm: "sha256".to_string(),
        chancel_certificate_summary:
            "Chancel certificate identity: fallback session is synthetic and must not verify green."
                .to_string(),
        chancel_certificate_sha256: "<missing>".to_string(),
        chancel_certificate_algorithm: "sha256".to_string(),
        explicit_statement:
            "the verified pulpit-receipt session is the primary input here; this immutable chancel certificate freezes one final herald identity over the fully culminated chain without enabling production execution"
                .to_string(),
    }
}

fn fallback_notarization_receipt_status(
    _snapshot: &LatestPulpitReceiptSnapshot,
    session_dir: &Path,
) -> ChancelCertificateStatusView {
    ChancelCertificateStatusView {
        session_dir: session_dir.display().to_string(),
        pulpit_receipt_session_dir: "<missing>".to_string(),
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
        pulpit_receipt_step: None,
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
        pulpit_receipt_summary:
            "Pulpit receipt identity: fallback status is synthetic and must not verify green."
                .to_string(),
        pulpit_receipt_sha256: "<missing>".to_string(),
        pulpit_receipt_algorithm: "sha256".to_string(),
        chancel_certificate_summary:
            "Chancel certificate identity: fallback status is synthetic and must not verify green."
                .to_string(),
        chancel_certificate_sha256: "<missing>".to_string(),
        chancel_certificate_algorithm: "sha256".to_string(),
        explicit_statement:
            "this chancel certificate is archival and read-only; it never executes or enables the frozen controller"
                .to_string(),
    }
}

fn render_report_lines(report: &PackageChancelCertificateLatestReport) -> String {
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
    if let Some(value) = &report.nested_pulpit_receipt_latest_session_dir {
        lines.push(format!("nested_pulpit_receipt_latest_session_dir={value}"));
    }
    if let Some(value) = &report.downstream_decision_packet_session_dir {
        lines.push(format!("downstream_decision_packet_session_dir={value}"));
    }
    if let Some(value) = &report.nested_chancel_certificate_session_dir {
        lines.push(format!("nested_chancel_certificate_session_dir={value}"));
    }
    if let Some(value) = &report.result {
        lines.push(format!("result={value}"));
    }
    if let Some(value) = &report.downstream_chancel_certificate_command_summary {
        lines.push(format!(
            "downstream_chancel_certificate_command_summary={value}"
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

    struct FakePulpitReceiptLatestFixture {
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
        pulpit_receipt_latest_plan_json_path: PathBuf,
        pulpit_receipt_latest_run_json_path: PathBuf,
        pulpit_receipt_latest_verify_json_path: PathBuf,
        handoff_bundle_run_json_path: PathBuf,
        handoff_bundle_verify_json_path: PathBuf,
        handoff_bundle_session_json_path: PathBuf,
        handoff_bundle_status_json_path: PathBuf,
        frozen_controller_command: String,
    }

    impl FakePulpitReceiptLatestFixture {
        fn plan_config(&self) -> Config {
            Config {
                mode: Mode::PlanLatestChancelCertificate,
                root: Some(self.root.clone()),
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn render_config(&self) -> Config {
            Config {
                mode: Mode::RenderLatestChancelCertificateScript,
                root: Some(self.root.clone()),
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: Some(self.output_script_path.clone()),
                json: true,
            }
        }

        fn run_config(&self) -> Config {
            Config {
                mode: Mode::RunLatestChancelCertificate,
                root: Some(self.root.clone()),
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn verify_config(&self) -> Config {
            Config {
                mode: Mode::VerifyLatestChancelCertificate,
                root: None,
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }
    }

    #[test]
    fn plan_mode_resolves_latest_valid_clerestory_chain_to_exact_expected_nested_latest_pulpit_receipt_lineage(
    ) {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "pulpit_receipt_latest_plan_ok",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let report = plan_latest_chancel_certificate_report(&fixture.plan_config()).unwrap();
        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        assert_eq!(
            report.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestPlanReady
        );
        assert_eq!(report.mode, "plan_latest_chancel_certificate");
        assert_eq!(
            report.latest_top_step_name.as_deref(),
            Some("clerestory_certificate")
        );
        assert_eq!(
            report.nested_pulpit_receipt_latest_session_dir.as_deref(),
            Some(
                paths
                    .nested_pulpit_receipt_latest_session_dir
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
            .downstream_chancel_certificate_command_summary
            .as_deref()
            .unwrap()
            .contains(CHANCEL_CERTIFICATE_BIN));
    }

    #[test]
    fn plan_mode_without_session_dir_uses_chancel_latest_default_wrapper_session_dir() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "chancel_certificate_latest_plan_default_session_dir",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let default_wrapper_session_dir = fixture
            .root
            .join("tiny_live_activation_package_chancel_certificate_latest.session");
        let stale_pulpit_wrapper_session_dir = fixture
            .root
            .join("tiny_live_activation_package_pulpit_receipt_latest.session");
        let default_paths = chancel_certificate_latest_paths(&default_wrapper_session_dir);
        let mut plan_json = default_pulpit_receipt_latest_plan_report(&fixture);
        plan_json["session_dir"] = json!(default_paths
            .nested_pulpit_receipt_latest_session_dir
            .display()
            .to_string());
        plan_json["downstream_decision_packet_session_dir"] = json!(
            expected_downstream_decision_packet_session_dir(&default_paths)
                .display()
                .to_string()
        );
        persist_json(&fixture.pulpit_receipt_latest_plan_json_path, &plan_json).unwrap();
        write_fake_pulpit_receipt_latest_command(
            &fixture.bin_dir.join(PULPIT_RECEIPT_LATEST_BIN),
            &fixture.pulpit_receipt_latest_plan_json_path,
            &fixture.pulpit_receipt_latest_run_json_path,
            &fixture.pulpit_receipt_latest_verify_json_path,
            &fixture.root,
            &default_paths.nested_pulpit_receipt_latest_session_dir,
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let config = Config {
            mode: Mode::PlanLatestChancelCertificate,
            root: Some(fixture.root.clone()),
            session_dir: None,
            output_path: None,
            json: true,
        };
        let report = plan_latest_chancel_certificate_report(&config).unwrap();
        let default_wrapper_session_dir = default_wrapper_session_dir.display().to_string();
        let stale_pulpit_wrapper_session_dir =
            stale_pulpit_wrapper_session_dir.display().to_string();

        assert_eq!(
            report.session_dir.as_deref(),
            Some(default_wrapper_session_dir.as_str())
        );
        assert_eq!(
            report
                .chancel_certificate_latest_session_path
                .as_deref()
                .unwrap(),
            default_paths.session_path.display().to_string()
        );
        for summary in [
            report
                .run_latest_chancel_certificate_command_summary
                .as_deref()
                .expect("missing run_latest_chancel_certificate_command_summary"),
            report
                .verify_latest_chancel_certificate_command_summary
                .as_deref()
                .expect("missing verify_latest_chancel_certificate_command_summary"),
        ] {
            assert!(
                summary.contains(default_wrapper_session_dir.as_str()),
                "self command summary must use chancel latest default session dir: {summary}"
            );
            assert!(
                !summary.contains(stale_pulpit_wrapper_session_dir.as_str()),
                "self command summary must not use pulpit latest default wrapper session dir: {summary}"
            );
        }
        assert_eq!(
            report.nested_pulpit_receipt_latest_session_dir.as_deref(),
            Some(
                default_paths
                    .nested_pulpit_receipt_latest_session_dir
                    .display()
                    .to_string()
                    .as_str()
            )
        );
    }

    #[test]
    fn render_mode_emits_exact_downstream_chancel_certificate_contract_not_second_controller_path()
    {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "pulpit_receipt_latest_render_ok",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let report =
            render_latest_chancel_certificate_script_report(&fixture.render_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestScriptRendered
        );
        assert_eq!(report.mode, "render_latest_chancel_certificate_script");
        let script = fs::read_to_string(&fixture.output_script_path).unwrap();
        assert!(script.contains(CHANCEL_CERTIFICATE_LATEST_BIN));
        assert!(!script.contains(PULPIT_RECEIPT_LATEST_BIN));
        assert!(script.contains(CHANCEL_CERTIFICATE_BIN));
        assert!(script.contains("--run-latest-chancel-certificate"));
        assert!(script.contains("--verify-latest-chancel-certificate"));
        assert!(!script.contains("--run-latest-pulpit-receipt"));
        assert!(!script.contains("--verify-latest-pulpit-receipt"));
        assert!(script.contains("--pulpit-receipt-session-dir"));
        assert!(!script.contains("--activation-ticket-session-dir"));
        assert!(script.contains("--run-live-package-chancel-certificate"));
        assert!(script.contains("--verify-live-package-chancel-certificate"));
        assert!(!script.contains("copybot_tiny_live_activation_package_live_cutover"));
    }

    #[test]
    fn run_mode_refuses_when_latest_chain_missing_invalid_or_below_clerestory() {
        for (label, verdict, reason, expected) in [
            (
                "pulpit_receipt_latest_missing",
                "tiny_live_package_pulpit_receipt_latest_refused_by_missing_latest_chain",
                "no persisted immutable chain exists",
                TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestRefusedByMissingLatestChain,
            ),
            (
                "pulpit_receipt_latest_invalid",
                "tiny_live_package_pulpit_receipt_latest_refused_by_invalid_latest_chain",
                "latest immutable chain is incomplete",
                TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestRefusedByInvalidLatestChain,
            ),
            (
                "pulpit_receipt_latest_below",
                "tiny_live_package_pulpit_receipt_latest_refused_by_invalid_latest_chain",
                "latest persisted top chain is choir_receipt, which is below clerestory_certificate",
                TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestRefusedByInvalidLatestChain,
            ),
        ] {
            let fixture = fake_pulpit_receipt_latest_fixture(
                label,
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_go_live_signoff",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);

            let mut plan: Value = load_json(&fixture.pulpit_receipt_latest_plan_json_path).unwrap();
            plan["verdict"] = json!(verdict);
            plan["reason"] = json!(reason);
            if label.ends_with("below") {
                plan["latest_top_step_name"] = json!("choir_receipt");
            }
            persist_json(&fixture.pulpit_receipt_latest_plan_json_path, &plan).unwrap();

            let report = run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();
            assert_eq!(report.verdict, expected);
        }
    }

    #[test]
    fn plan_mode_maps_real_latest_pulpit_receipt_drift_verdict_to_local_drift_refusal() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "pulpit_receipt_latest_plan_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let mut plan: Value = load_json(&fixture.pulpit_receipt_latest_plan_json_path).unwrap();
        plan["verdict"] = json!(
            "tiny_live_package_pulpit_receipt_latest_refused_by_drifted_pulpit_receipt_contract"
        );
        plan["reason"] = json!("real accepted latest-pulpit-receipt drift");
        persist_json(&fixture.pulpit_receipt_latest_plan_json_path, &plan).unwrap();

        let report = run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestRefusedByDriftedChancelCertificateContract
        );
        assert_eq!(
            report.result.as_deref(),
            Some("refused_by_drifted_chancel_certificate_contract")
        );
    }

    #[test]
    fn run_mode_takes_terminal_drift_refusal_when_nested_latest_pulpit_receipt_reports_real_drift()
    {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "pulpit_receipt_latest_run_drift",
            "refused_by_drifted_pulpit_receipt_contract",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let report = run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestRefusedByDriftedChancelCertificateContract
        );
        assert_eq!(
            report.result.as_deref(),
            Some("refused_by_drifted_chancel_certificate_contract")
        );
        assert_eq!(report.chancel_certificate_verdict.as_deref(), None);
    }

    #[test]
    fn run_mode_refuses_when_nested_latest_pulpit_receipt_lineage_drifts_from_latest_immutable_chain(
    ) {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "pulpit_receipt_latest_handoff_bundle_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let mut run: Value = load_json(&fixture.pulpit_receipt_latest_run_json_path).unwrap();
        run["package_dir"] = json!("/tampered/package");
        persist_json(&fixture.pulpit_receipt_latest_run_json_path, &run).unwrap();

        let report = run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestRefusedByDriftedChancelCertificateContract
        );
        assert!(report
            .reason
            .contains("stored latest-pulpit-receipt run package_dir"));
    }

    #[test]
    fn valid_nested_latest_pulpit_modes_are_accepted_by_latest_chancel_plan_run_verify() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "chancel_certificate_latest_accepts_real_nested_lectern_modes",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let plan = plan_latest_chancel_certificate_report(&fixture.plan_config()).unwrap();
        assert_eq!(
            plan.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestPlanReady
        );

        let run = run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestReadyForManualExecutionWhenGateTurnsGreen
        );

        let verify = verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyOk,
            "{:#?}",
            verify.verification_mismatches
        );

        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let stored_plan: Value = load_json(&paths.latest_pulpit_receipt_plan_report_path).unwrap();
        let stored_run: Value = load_json(&paths.latest_pulpit_receipt_report_path).unwrap();
        let status: Value = load_json(&paths.status_path).unwrap();

        assert_eq!(stored_plan["mode"], json!("plan_latest_pulpit_receipt"));
        assert_eq!(stored_run["mode"], json!("run_latest_pulpit_receipt"));
        assert_eq!(
            status["latest_pulpit_receipt_plan_step"]["mode"],
            json!("plan_latest_pulpit_receipt")
        );
        assert_eq!(
            status["latest_pulpit_receipt_step"]["mode"],
            json!("run_latest_pulpit_receipt")
        );
    }

    #[test]
    fn plan_rejects_stale_parent_mode_in_nested_latest_pulpit_plan_payload() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "chancel_certificate_latest_rejects_parent_mode_in_nested_lectern_plan",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let mut plan: Value = load_json(&fixture.pulpit_receipt_latest_plan_json_path).unwrap();
        plan["mode"] = json!("plan_latest_chancel_certificate");
        persist_json(&fixture.pulpit_receipt_latest_plan_json_path, &plan).unwrap();

        let report = plan_latest_chancel_certificate_report(&fixture.plan_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestRefusedByDriftedChancelCertificateContract
        );
        assert!(report.reason.contains(
            "stored latest-pulpit-receipt plan mode \"plan_latest_chancel_certificate\" does not match expected \"plan_latest_pulpit_receipt\""
        ));
    }

    #[test]
    fn run_rejects_stale_parent_mode_in_nested_latest_pulpit_run_payload() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "chancel_certificate_latest_rejects_parent_mode_in_nested_lectern_run",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let mut run: Value = load_json(&fixture.pulpit_receipt_latest_run_json_path).unwrap();
        run["mode"] = json!("run_latest_chancel_certificate");
        persist_json(&fixture.pulpit_receipt_latest_run_json_path, &run).unwrap();

        let report = run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestRefusedByDriftedChancelCertificateContract
        );
        assert!(report.reason.contains(
            "stored latest-pulpit-receipt run mode \"run_latest_chancel_certificate\" does not match expected \"run_latest_pulpit_receipt\""
        ));
    }

    #[test]
    fn verify_rejects_stale_parent_modes_in_copied_nested_latest_pulpit_reports() {
        {
            let fixture = fake_pulpit_receipt_latest_fixture(
                "chancel_certificate_latest_rejects_parent_mode_in_copied_lectern_plan",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_go_live_signoff",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

            let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
            let mut plan: Value = load_json(&paths.latest_pulpit_receipt_plan_report_path).unwrap();
            plan["mode"] = json!("plan_latest_chancel_certificate");
            persist_json(&paths.latest_pulpit_receipt_plan_report_path, &plan).unwrap();

            let verify =
                verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
            );
            assert!(verify.verification_mismatches.iter().any(|entry| {
                entry.contains(
                    "stored latest-pulpit-receipt plan mode \"plan_latest_chancel_certificate\" does not match expected \"plan_latest_pulpit_receipt\"",
                )
            }));
        }

        {
            let fixture = fake_pulpit_receipt_latest_fixture(
                "chancel_certificate_latest_rejects_parent_mode_in_copied_lectern_run",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_go_live_signoff",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

            let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
            let mut run: Value = load_json(&paths.latest_pulpit_receipt_report_path).unwrap();
            run["mode"] = json!("run_latest_chancel_certificate");
            persist_json(&paths.latest_pulpit_receipt_report_path, &run).unwrap();

            let verify =
                verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
            );
            assert!(verify.verification_mismatches.iter().any(|entry| {
                entry.contains(
                    "stored latest-pulpit-receipt run mode \"run_latest_chancel_certificate\" does not match expected \"run_latest_pulpit_receipt\"",
                )
            }));
        }
    }

    #[test]
    fn verify_mode_fails_when_persisted_latest_pulpit_receipt_artifacts_are_tampered_or_missing_nested_lineage_truth(
    ) {
        {
            let fixture = fake_pulpit_receipt_latest_fixture(
                "pulpit_receipt_latest_verify_tampered_copy",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_go_live_signoff",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

            let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
            let mut report: Value = load_json(&paths.chancel_certificate_report_path).unwrap();
            report["decision_packet_session_dir"] = json!("/tampered/run-decision-packet-session");
            persist_json(&paths.chancel_certificate_report_path, &report).unwrap();

            let verify =
                verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
            );
            assert!(verify.verification_mismatches.iter().any(|entry| {
                entry.contains("stored chancel-certificate run decision_packet_session_dir")
            }));
        }

        {
            let fixture = fake_pulpit_receipt_latest_fixture(
                "pulpit_receipt_latest_verify_missing_nested_truth",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_go_live_signoff",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

            let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
            fs::remove_file(nested_pulpit_receipt_latest_report_path(
                &paths.nested_pulpit_receipt_latest_session_dir,
            ))
            .unwrap();

            let verify =
                verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
            );
            assert!(verify.verification_mismatches.iter().any(|entry| {
                entry.contains("tiny_live_activation_package_pulpit_receipt_latest.report.json")
            }));
        }
    }

    #[test]
    fn verify_accepts_real_shaped_native_pulpit_and_chancel_identity_layers_on_happy_path() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "chancel_certificate_latest_verify_real_shaped_rostrum_paths_green",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

        let verify = verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyOk,
            "{:#?}",
            verify.verification_mismatches
        );
    }

    #[test]
    fn fake_native_chancel_payload_uses_herald_and_pulpit_crest_input_wording() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "chancel_certificate_latest_fixture_identity_wording",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let snapshot = LatestPulpitReceiptSnapshot {
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
        let fallback_session = fallback_notarization_receipt_session(
            &snapshot,
            &paths.nested_chancel_certificate_session_dir,
        );
        assert!(fallback_session.explicit_statement.contains("herald"));
        assert!(!fallback_session.explicit_statement.contains("crest"));
        assert!(!fallback_session.explicit_statement.contains("hallmark"));
        assert!(!fallback_session.explicit_statement.contains("escutcheon"));
        assert!(!fallback_session.explicit_statement.contains("blazon"));
        assert!(!fallback_session.explicit_statement.contains("armorial"));

        let payloads = [
            ("session", default_provenance_certificate_session(&fixture)),
            (
                "status",
                default_provenance_certificate_status(
                    &fixture,
                    "ready_for_manual_execution_when_gate_turns_green",
                ),
            ),
            (
                "run",
                default_provenance_certificate_run_report(
                    &fixture,
                    "ready_for_manual_execution_when_gate_turns_green",
                ),
            ),
            (
                "verify",
                default_provenance_certificate_verify_report(
                    &fixture,
                    "ready_for_manual_execution_when_gate_turns_green",
                ),
            ),
        ];

        for (label, payload) in payloads {
            let chancel_summary = payload["chancel_certificate_summary"]
                .as_str()
                .unwrap_or_else(|| panic!("{label} chancel_certificate_summary missing"));
            assert!(
                chancel_summary.contains("herald"),
                "{label} chancel summary must use accepted herald wording: {chancel_summary}"
            );
            assert!(
                !chancel_summary.contains("crest"),
                "{label} chancel summary must not use upstream pulpit crest wording: {chancel_summary}"
            );
            assert!(
                !chancel_summary.contains("hallmark"),
                "{label} chancel summary must not use stale hallmark wording: {chancel_summary}"
            );
            assert!(
                !chancel_summary.contains("escutcheon"),
                "{label} chancel summary must not use upstream rostrum escutcheon wording: {chancel_summary}"
            );
            assert!(
                !chancel_summary.contains("blazon"),
                "{label} chancel summary must not use upstream podium blazon wording: {chancel_summary}"
            );
            assert!(
                !chancel_summary.contains("armorial"),
                "{label} chancel summary must not use upstream lectern armorial wording: {chancel_summary}"
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
                    explicit_statement.contains("herald"),
                    "{label} chancel session/run explicit statement must use herald wording: {explicit_statement}"
                );
                assert!(
                    !explicit_statement.contains("crest"),
                    "{label} chancel session/run explicit statement must not use upstream pulpit crest wording: {explicit_statement}"
                );
                assert!(
                    !explicit_statement.contains("escutcheon"),
                    "{label} chancel session/run explicit statement must not use upstream rostrum escutcheon wording: {explicit_statement}"
                );
                assert!(
                    !explicit_statement.contains("blazon"),
                    "{label} chancel session/run explicit statement must not use upstream podium blazon wording: {explicit_statement}"
                );
                assert!(
                    !explicit_statement.contains("armorial"),
                    "{label} chancel session/run explicit statement must not use upstream lectern armorial wording: {explicit_statement}"
                );
            }

            let pulpit_summary = payload["pulpit_receipt_summary"]
                .as_str()
                .unwrap_or_else(|| panic!("{label} pulpit_receipt_summary missing"));
            assert!(
                pulpit_summary.contains("crest"),
                "{label} pulpit input summary must use accepted crest wording: {pulpit_summary}"
            );
            assert!(
                pulpit_summary.contains("lectern-certificate"),
                "{label} pulpit input summary must bind to lectern-certificate truth: {pulpit_summary}"
            );
            assert!(
                !pulpit_summary.contains("hallmark"),
                "{label} pulpit input summary must not use stale hallmark wording: {pulpit_summary}"
            );
            assert!(
                !pulpit_summary.contains("escutcheon"),
                "{label} pulpit input summary must not use upstream rostrum escutcheon wording: {pulpit_summary}"
            );
            assert!(
                !pulpit_summary.contains("blazon"),
                "{label} pulpit input summary must not use upstream podium blazon wording: {pulpit_summary}"
            );
            assert!(
                !pulpit_summary.contains("armorial"),
                "{label} pulpit input summary must not use upstream lectern armorial wording: {pulpit_summary}"
            );
            assert!(
                !pulpit_summary.contains("rostrum-certificate"),
                "{label} pulpit input summary must not bind directly to rostrum-certificate truth: {pulpit_summary}"
            );
            assert!(
                !pulpit_summary.contains("dais-receipt"),
                "{label} pulpit input summary must not bind directly to dais-receipt truth: {pulpit_summary}"
            );
            assert!(
                !pulpit_summary.contains("plinth-receipt"),
                "{label} pulpit input summary must not bind directly to plinth-receipt truth: {pulpit_summary}"
            );
        }
    }

    #[test]
    fn run_and_verify_reject_stale_foreign_wrapper_schema_for_downstream_chancel_copy() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "chancel_certificate_latest_verify_rejects_stale_keystone_wrapper_schema",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        assert!(paths.chancel_certificate_report_path.ends_with(
            "tiny_live_activation_package_chancel_certificate_latest.chancel_certificate.report.json"
        ));
        assert!(paths.chancel_certificate_report_path.exists());
        assert!(!fixture
            .wrapper_session_dir
            .join(
                "tiny_live_activation_package_chancel_certificate_latest.pulpit_receipt.report.json"
            )
            .exists());
        assert!(!fixture
            .wrapper_session_dir
            .join(
                "tiny_live_activation_package_chancel_certificate_latest.pedestal_certificate.report.json"
            )
            .exists());
        assert!(!fixture
            .wrapper_session_dir
            .join("tiny_live_activation_package_chancel_certificate_latest.plinth_receipt.report.json")
            .exists());
        assert!(!fixture
            .wrapper_session_dir
            .join(
                "tiny_live_activation_package_chancel_certificate_latest.substructure_certificate.report.json"
            )
            .exists());
        assert!(paths.nested_chancel_certificate_session_dir.ends_with(
            "tiny_live_activation_package_chancel_certificate_latest.chancel_certificate_session"
        ));
        assert!(!fixture
            .wrapper_session_dir
            .join("tiny_live_activation_package_chancel_certificate_latest.pulpit_receipt_session")
            .exists());
        assert!(!fixture
            .wrapper_session_dir
            .join("tiny_live_activation_package_chancel_certificate_latest.pedestal_certificate_session")
            .exists());
        assert!(!fixture
            .wrapper_session_dir
            .join("tiny_live_activation_package_chancel_certificate_latest.plinth_receipt_session")
            .exists());
        assert!(!fixture
            .wrapper_session_dir
            .join("tiny_live_activation_package_chancel_certificate_latest.substructure_certificate_session")
            .exists());

        let mut status: Value = load_json(&paths.status_path).unwrap();
        let stored_step = status
            .get("chancel_certificate_step")
            .cloned()
            .expect("chancel_certificate_step should be present");
        assert!(status.get("keystone_receipt_step").is_none());
        status["keystone_receipt_step"] = stored_step;
        if let Some(object) = status.as_object_mut() {
            object.remove("chancel_certificate_step");
        }
        persist_json(&paths.status_path, &status).unwrap();

        let error = verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap_err();
        assert!(error.to_string().contains("chancel_certificate_step"));
    }

    #[test]
    fn verify_names_chancel_certificate_when_nested_native_session_load_breaks() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "pulpit_receipt_latest_verify_nested_dais_session_load_break",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        fs::remove_file(nested_chancel_certificate_session_path(
            &paths.nested_chancel_certificate_session_dir,
        ))
        .unwrap();

        let verify = verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("failed reading nested chancel-certificate session under")
        }));
    }

    #[test]
    fn verify_fails_when_copied_native_chancel_certificate_session_path_drifts() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "pulpit_receipt_latest_verify_dais_session_path_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.chancel_certificate_report_path).unwrap();
        report["chancel_certificate_session_path"] =
            json!("/tampered/finality-receipt-session.json");
        persist_json(&paths.chancel_certificate_report_path, &report).unwrap();

        let verify = verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored chancel-certificate run chancel_certificate_session_path")
        }));
    }

    #[test]
    fn verify_fails_when_copied_native_chancel_certificate_status_path_drifts() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "pulpit_receipt_latest_verify_dais_status_path_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.chancel_certificate_report_path).unwrap();
        report["chancel_certificate_status_path"] = json!("/tampered/finality-receipt-status.json");
        persist_json(&paths.chancel_certificate_report_path, &report).unwrap();

        let verify = verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored chancel-certificate run chancel_certificate_status_path")
        }));
    }

    #[test]
    fn verify_fails_when_copied_native_chancel_pedestal_certificate_identity_drifts() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "chancel_certificate_verify_pedestal_identity_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.chancel_certificate_report_path).unwrap();
        report["pedestal_certificate_sha256"] = json!("tampered-pedestal-certificate-sha256");
        persist_json(&paths.chancel_certificate_report_path, &report).unwrap();

        let verify = verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored chancel-certificate copy pedestal_certificate_sha256")
        }));
    }

    #[test]
    fn run_names_latest_pulpit_receipt_when_nested_latest_wrapper_run_fails() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "pulpit_receipt_latest_nested_run_failure_wording",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        overwrite_fake_pulpit_receipt_latest_command(
            &fixture.bin_dir.join(PULPIT_RECEIPT_LATEST_BIN),
            &fixture.pulpit_receipt_latest_plan_json_path,
            &fixture.pulpit_receipt_latest_run_json_path,
            &fixture.pulpit_receipt_latest_verify_json_path,
            None,
            Some("forced nested latest-pulpit-receipt run failure"),
            None,
            &fixture.root,
            &paths.nested_pulpit_receipt_latest_session_dir,
        );

        let report = run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestRefusedByDriftedChancelCertificateContract
        );
        assert!(report
            .reason
            .contains("failed running nested latest-pulpit-receipt handoff under"));
    }

    #[test]
    fn run_names_latest_pulpit_receipt_when_nested_latest_wrapper_verify_fails() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "pulpit_receipt_latest_nested_verify_failure_wording",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        overwrite_fake_pulpit_receipt_latest_command(
            &fixture.bin_dir.join(PULPIT_RECEIPT_LATEST_BIN),
            &fixture.pulpit_receipt_latest_plan_json_path,
            &fixture.pulpit_receipt_latest_run_json_path,
            &fixture.pulpit_receipt_latest_verify_json_path,
            None,
            None,
            Some("forced nested latest-pulpit-receipt verify failure"),
            &fixture.root,
            &paths.nested_pulpit_receipt_latest_session_dir,
        );

        let report = run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestRefusedByDriftedChancelCertificateContract
        );
        assert!(report
            .reason
            .contains("failed verifying nested latest-pulpit-receipt handoff under"));
    }

    #[test]
    fn run_names_chancel_certificate_when_downstream_native_run_fails() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "chancel_certificate_downstream_run_failure_wording",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        overwrite_fake_release_capsule_command(
            &fixture.bin_dir.join(CHANCEL_CERTIFICATE_BIN),
            &fixture.handoff_bundle_run_json_path,
            &fixture.handoff_bundle_verify_json_path,
            &fixture.handoff_bundle_session_json_path,
            &fixture.handoff_bundle_status_json_path,
            Some("forced downstream native chancel run failure"),
            None,
            &expected_downstream_pulpit_receipt_session_dir(&paths),
            &expected_downstream_decision_packet_session_dir(&paths),
            &paths.nested_chancel_certificate_session_dir,
        );

        let report = run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestRefusedByDriftedChancelCertificateContract
        );
        assert!(report
            .reason
            .contains("failed running downstream chancel-certificate contract under"));
        assert!(!report
            .reason
            .contains("failed running downstream plinth-receipt contract under"));
        assert!(!report
            .reason
            .contains("failed running downstream pulpit-receipt contract under"));
        assert!(!report.reason.contains("native pulpit"));
        assert!(!report.reason.contains("native lectern"));
    }

    #[test]
    fn run_names_chancel_certificate_when_downstream_native_verify_fails() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "chancel_certificate_downstream_verify_failure_wording",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        overwrite_fake_release_capsule_command(
            &fixture.bin_dir.join(CHANCEL_CERTIFICATE_BIN),
            &fixture.handoff_bundle_run_json_path,
            &fixture.handoff_bundle_verify_json_path,
            &fixture.handoff_bundle_session_json_path,
            &fixture.handoff_bundle_status_json_path,
            None,
            Some("forced downstream native chancel verify failure"),
            &expected_downstream_pulpit_receipt_session_dir(&paths),
            &expected_downstream_decision_packet_session_dir(&paths),
            &paths.nested_chancel_certificate_session_dir,
        );

        let report = run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestRefusedByDriftedChancelCertificateContract
        );
        assert!(report
            .reason
            .contains("failed verifying downstream chancel-certificate contract under"));
        assert!(!report
            .reason
            .contains("failed verifying downstream plinth-receipt contract under"));
        assert!(!report
            .reason
            .contains("failed verifying downstream pulpit-receipt contract under"));
        assert!(!report.reason.contains("native pulpit"));
        assert!(!report.reason.contains("native lectern"));
    }

    #[test]
    fn run_uses_chancel_placeholders_when_missing_snapshot_blocks_nested_handoff_summaries() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "chancel_certificate_latest_missing_snapshot_placeholders",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        overwrite_fake_pulpit_receipt_latest_command(
            &fixture.bin_dir.join(PULPIT_RECEIPT_LATEST_BIN),
            &fixture.pulpit_receipt_latest_plan_json_path,
            &fixture.pulpit_receipt_latest_run_json_path,
            &fixture.pulpit_receipt_latest_verify_json_path,
            Some("forced latest-pulpit-receipt plan failure without snapshot"),
            None,
            None,
            &fixture.root,
            &paths.nested_pulpit_receipt_latest_session_dir,
        );

        let report = run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestRefusedByMissingLatestChain
        );
        assert_eq!(
            report
                .downstream_chancel_certificate_command_summary
                .as_deref(),
            Some("<chancel-certificate-run-unavailable>")
        );
        assert_eq!(
            report.verify_chancel_certificate_command_summary.as_deref(),
            Some("<chancel-certificate-verify-unavailable>")
        );

        let session: PackageChancelCertificateLatestSession =
            load_json(&paths.session_path).unwrap();
        assert_eq!(
            session.downstream_chancel_certificate_command_summary,
            "<chancel-certificate-run-unavailable>"
        );
        assert_eq!(
            session.verify_chancel_certificate_command_summary,
            "<chancel-certificate-verify-unavailable>"
        );
    }

    #[test]
    fn verify_names_chancel_certificate_when_nested_native_verify_is_non_green() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "chancel_certificate_verify_non_green_wording",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

        let mut verify_json: Value = load_json(&fixture.handoff_bundle_verify_json_path).unwrap();
        verify_json["verdict"] = json!("tiny_live_package_chancel_certificate_verify_invalid");
        verify_json["reason"] = json!("forced nested chancel-certificate verify non-green");
        persist_json(&fixture.handoff_bundle_verify_json_path, &verify_json).unwrap();

        let verify = verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("nested chancel-certificate verification is non-green")
        }));
        assert!(!verify.verification_mismatches.iter().any(|entry| {
            entry.contains("nested substructure-certificate verification is non-green")
        }));
    }

    #[test]
    fn verify_names_latest_chancel_certificate_when_wrapper_result_drifts() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "chancel_certificate_wrapper_result_mismatch_wording",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut status: Value = load_json(&paths.status_path).unwrap();
        status["result"] = json!("refused_by_missing_latest_chain");
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
        );
        assert!(verify
            .verification_mismatches
            .iter()
            .any(|entry| { entry.contains("stored latest-chancel-certificate result") }));
        assert!(!verify
            .verification_mismatches
            .iter()
            .any(|entry| { entry.contains("stored latest-pulpit-receipt result") }));
    }

    #[test]
    fn verify_names_latest_chancel_certificate_when_activation_authorized_drifts() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "chancel_certificate_activation_authorized_wording",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut status: Value = load_json(&paths.status_path).unwrap();
        status["activation_authorized"] = json!(true);
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains(
                "stored status activation_authorized must remain false for latest-chancel-certificate",
            )
        }));
        assert!(!verify.verification_mismatches.iter().any(|entry| {
            entry.contains(
                "stored status activation_authorized must remain false for latest-pulpit-receipt",
            )
        }));
    }

    #[test]
    fn verify_fails_when_copied_native_pinnacle_receipt_sha256_drifts() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "pulpit_receipt_latest_verify_pinnacle_receipt_sha256_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.chancel_certificate_report_path).unwrap();
        report["pinnacle_receipt_sha256"] = json!("tampered-summit-certificate-sha256");
        persist_json(&paths.chancel_certificate_report_path, &report).unwrap();

        let verify = verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored chancel-certificate copy pinnacle_receipt_sha256")
        }));
    }

    #[test]
    fn verify_fails_when_copied_nested_latest_pulpit_receipt_explicit_statement_drifts() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "pulpit_receipt_latest_verify_launch_packet_explicit_statement_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.latest_pulpit_receipt_report_path).unwrap();
        report["explicit_statement"] =
            json!("tampered nested latest release capsule explicit statement");
        persist_json(&paths.latest_pulpit_receipt_report_path, &report).unwrap();

        let verify = verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored latest-pulpit-receipt copy explicit_statement")
        }));
    }

    #[test]
    fn verify_rejects_tampered_stale_substructure_wrapper_mode_string() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "pulpit_receipt_latest_verify_wrapper_mode_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.report_path).unwrap();
        report["mode"] = json!("run_latest_substructure_certificate");
        persist_json(&paths.report_path, &report).unwrap();

        let verify = verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
        );
        assert!(verify
            .verification_mismatches
            .iter()
            .any(|entry| { entry.contains("latest-chancel-certificate stored report mode") }));
    }

    #[test]
    fn wrapper_self_modes_and_self_command_summaries_use_chancel_latest_contract() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "chancel_certificate_latest_self_mode_and_summary_shape",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let plan = plan_latest_chancel_certificate_report(&fixture.plan_config()).unwrap();
        assert_eq!(plan.mode, "plan_latest_chancel_certificate");
        assert!(plan
            .run_latest_chancel_certificate_command_summary
            .as_deref()
            .unwrap()
            .contains(CHANCEL_CERTIFICATE_LATEST_BIN));
        assert!(plan
            .run_latest_chancel_certificate_command_summary
            .as_deref()
            .unwrap()
            .contains("--run-latest-chancel-certificate"));
        assert!(!plan
            .run_latest_chancel_certificate_command_summary
            .as_deref()
            .unwrap()
            .contains("--run-latest-pulpit-receipt"));
        assert!(plan
            .verify_latest_chancel_certificate_command_summary
            .as_deref()
            .unwrap()
            .contains(CHANCEL_CERTIFICATE_LATEST_BIN));
        assert!(plan
            .verify_latest_chancel_certificate_command_summary
            .as_deref()
            .unwrap()
            .contains("--verify-latest-chancel-certificate"));
        assert!(!plan
            .verify_latest_chancel_certificate_command_summary
            .as_deref()
            .unwrap()
            .contains("--verify-latest-pulpit-receipt"));

        let run = run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();
        assert_eq!(run.mode, "run_latest_chancel_certificate");

        let verify = verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(verify.mode, "verify_latest_chancel_certificate");

        let session: PackageChancelCertificateLatestSession =
            load_json(&chancel_certificate_latest_paths(&fixture.wrapper_session_dir).session_path)
                .unwrap();
        assert!(session
            .run_latest_chancel_certificate_command_summary
            .contains(CHANCEL_CERTIFICATE_LATEST_BIN));
        assert!(session
            .run_latest_chancel_certificate_command_summary
            .contains("--run-latest-chancel-certificate"));
        assert!(!session
            .run_latest_chancel_certificate_command_summary
            .contains("--run-latest-pulpit-receipt"));
        assert!(session
            .verify_latest_chancel_certificate_command_summary
            .contains(CHANCEL_CERTIFICATE_LATEST_BIN));
        assert!(session
            .verify_latest_chancel_certificate_command_summary
            .contains("--verify-latest-chancel-certificate"));
        assert!(!session
            .verify_latest_chancel_certificate_command_summary
            .contains("--verify-latest-pulpit-receipt"));

        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let session_json: Value = load_json(&paths.session_path).unwrap();
        assert!(session_json
            .get("verify_chancel_certificate_command_summary")
            .is_some());
        assert!(session_json
            .get("verify_pulpit_receipt_command_summary")
            .is_none());

        let report_json: Value = load_json(&paths.report_path).unwrap();
        assert!(report_json
            .get("verify_chancel_certificate_command_summary")
            .is_some());
        assert!(report_json
            .get("verify_pulpit_receipt_command_summary")
            .is_none());
    }

    #[test]
    fn native_pulpit_input_uses_real_latest_pulpit_receipt_session_path() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "chancel_certificate_native_input_session_dir_shape",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);

        let downstream = expected_downstream_pulpit_receipt_session_dir(&paths);
        let expected = paths
            .nested_pulpit_receipt_latest_session_dir
            .join("tiny_live_activation_package_pulpit_receipt_latest.pulpit_receipt_session");

        assert_eq!(downstream, expected);
        assert!(downstream
            .file_name()
            .unwrap()
            .to_string_lossy()
            .contains("pulpit_receipt_session"));
        assert!(!downstream
            .file_name()
            .unwrap()
            .to_string_lossy()
            .contains("plinth_receipt_session"));

        assert!(paths
            .nested_chancel_certificate_session_dir
            .file_name()
            .unwrap()
            .to_string_lossy()
            .contains("chancel_certificate_session"));
        assert!(!paths
            .nested_chancel_certificate_session_dir
            .file_name()
            .unwrap()
            .to_string_lossy()
            .contains("pulpit_receipt_session"));
        assert!(!paths
            .nested_chancel_certificate_session_dir
            .file_name()
            .unwrap()
            .to_string_lossy()
            .contains("substructure_certificate_session"));
    }

    #[test]
    fn run_persists_copied_native_chancel_session_and_status_under_real_chancel_filenames() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "chancel_certificate_latest_native_chancel_filenames",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let real_session =
            nested_chancel_certificate_session_path(&paths.nested_chancel_certificate_session_dir);
        let real_status =
            nested_chancel_certificate_status_path(&paths.nested_chancel_certificate_session_dir);
        let stale_session = paths
            .nested_chancel_certificate_session_dir
            .join("tiny_live_activation_package_substructure_certificate.session.json");
        let stale_status = paths
            .nested_chancel_certificate_session_dir
            .join("tiny_live_activation_package_substructure_certificate.status.json");
        let stale_pulpit_session = paths
            .nested_chancel_certificate_session_dir
            .join("tiny_live_activation_package_pulpit_receipt.session.json");
        let stale_pulpit_status = paths
            .nested_chancel_certificate_session_dir
            .join("tiny_live_activation_package_pulpit_receipt.status.json");

        assert!(
            real_session.exists(),
            "missing real native chancel session path"
        );
        assert!(
            real_status.exists(),
            "missing real native chancel status path"
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
            !stale_pulpit_session.exists(),
            "stale native pulpit session filename should not be used"
        );
        assert!(
            !stale_pulpit_status.exists(),
            "stale native pulpit status filename should not be used"
        );
    }

    #[test]
    fn verify_fails_when_copied_nested_provenance_certificate_decision_packet_result_drifts() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "pulpit_receipt_latest_verify_decision_packet_result_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.chancel_certificate_report_path).unwrap();
        report["decision_packet_result"] = json!("tampered_decision_packet_result");
        persist_json(&paths.chancel_certificate_report_path, &report).unwrap();

        let verify = verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored chancel-certificate copy decision_packet_result")
        }));
    }

    #[test]
    fn verify_fails_when_copied_nested_notarization_receipt_attestation_seal_session_dir_drifts() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "pulpit_receipt_latest_verify_attestation_seal_session_dir_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.chancel_certificate_report_path).unwrap();
        report["attestation_seal_session_dir"] = json!("/tampered/attestation-seal-session");
        persist_json(&paths.chancel_certificate_report_path, &report).unwrap();

        let verify = verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored chancel-certificate copy attestation_seal_session_dir")
        }));
    }

    #[test]
    fn verify_fails_when_copied_native_finality_receipt_registry_entry_session_dir_drifts() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "pulpit_receipt_latest_verify_registry_entry_session_dir_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.chancel_certificate_report_path).unwrap();
        report["registry_entry_session_dir"] = json!("/tampered/registry-entry-session");
        persist_json(&paths.chancel_certificate_report_path, &report).unwrap();

        let verify = verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored chancel-certificate copy registry_entry_session_dir")
        }));
    }

    #[test]
    fn verify_fails_when_copied_native_finality_receipt_notarization_receipt_session_dir_drifts() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "pulpit_receipt_latest_verify_notarization_receipt_session_dir_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.chancel_certificate_report_path).unwrap();
        report["notarization_receipt_session_dir"] =
            json!("/tampered/notarization-receipt-session");
        persist_json(&paths.chancel_certificate_report_path, &report).unwrap();

        let verify = verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored chancel-certificate copy notarization_receipt_session_dir")
        }));
    }

    #[test]
    fn verify_fails_when_copied_nested_notarization_receipt_chain_fingerprint_summary_drifts() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "pulpit_receipt_latest_verify_chain_fingerprint_summary_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.chancel_certificate_report_path).unwrap();
        report["chain_fingerprint_summary"] = json!("tampered chain fingerprint summary");
        persist_json(&paths.chancel_certificate_report_path, &report).unwrap();

        let verify = verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored chancel-certificate copy chain_fingerprint_summary")
        }));
    }

    #[test]
    fn verify_fails_when_copied_nested_notarization_receipt_chain_fingerprint_sha256_drifts() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "pulpit_receipt_latest_verify_chain_fingerprint_sha256_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.chancel_certificate_report_path).unwrap();
        report["chain_fingerprint_sha256"] = json!("tampered-chain-fingerprint-sha256");
        persist_json(&paths.chancel_certificate_report_path, &report).unwrap();

        let verify = verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored chancel-certificate copy chain_fingerprint_sha256")
        }));
    }

    #[test]
    fn verify_fails_when_copied_nested_notarization_receipt_chain_fingerprint_algorithm_drifts() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "pulpit_receipt_latest_verify_chain_fingerprint_algorithm_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.chancel_certificate_report_path).unwrap();
        report["chain_fingerprint_algorithm"] = json!("tampered-fingerprint-algorithm");
        persist_json(&paths.chancel_certificate_report_path, &report).unwrap();

        let verify = verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored chancel-certificate copy chain_fingerprint_algorithm")
        }));
    }

    #[test]
    fn verify_fails_when_copied_native_completion_certificate_closure_certificate_sha256_drifts() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "pulpit_receipt_latest_verify_closure_certificate_sha256_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.chancel_certificate_report_path).unwrap();
        report["closure_certificate_sha256"] = json!("tampered-closure-certificate-sha256");
        persist_json(&paths.chancel_certificate_report_path, &report).unwrap();

        let verify = verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored chancel-certificate copy closure_certificate_sha256")
        }));
    }

    #[test]
    fn verify_fails_when_copied_native_finality_receipt_archive_receipt_sha256_drifts() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "pulpit_receipt_latest_verify_archive_receipt_sha256_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.chancel_certificate_report_path).unwrap();
        report["archive_receipt_sha256"] = json!("tampered-archive-receipt-sha256");
        persist_json(&paths.chancel_certificate_report_path, &report).unwrap();

        let verify = verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored chancel-certificate copy archive_receipt_sha256")
        }));
    }

    #[test]
    fn verify_fails_when_copied_native_finality_receipt_finality_receipt_sha256_drifts() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "pulpit_receipt_latest_verify_finality_receipt_sha256_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.chancel_certificate_report_path).unwrap();
        report["finality_receipt_sha256"] = json!("tampered-finality-receipt-sha256");
        persist_json(&paths.chancel_certificate_report_path, &report).unwrap();

        let verify = verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored chancel-certificate copy finality_receipt_sha256")
        }));
    }

    #[test]
    fn verify_fails_when_copied_native_consummation_record_sha256_drifts() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "pulpit_receipt_latest_verify_consummation_record_sha256_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.chancel_certificate_report_path).unwrap();
        report["consummation_record_sha256"] = json!("tampered-consummation-record-sha256");
        persist_json(&paths.chancel_certificate_report_path, &report).unwrap();

        let verify = verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored chancel-certificate copy consummation_record_sha256")
        }));
    }

    #[test]
    fn verify_fails_when_copied_native_completion_certificate_completion_certificate_sha256_drifts()
    {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "pulpit_receipt_latest_verify_completion_certificate_sha256_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.chancel_certificate_report_path).unwrap();
        report["completion_certificate_sha256"] = json!("tampered-completion-certificate-sha256");
        persist_json(&paths.chancel_certificate_report_path, &report).unwrap();

        let verify = verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored chancel-certificate copy completion_certificate_sha256")
        }));
    }

    #[test]
    fn verify_fails_when_downstream_decision_packet_confirmation_anchor_drifts() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "pulpit_receipt_latest_verify_decision_packet_anchor_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.chancel_certificate_report_path).unwrap();
        report["decision_packet_session_dir"] = json!("/tampered/decision-packet-session");
        persist_json(&paths.chancel_certificate_report_path, &report).unwrap();

        let verify = verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored chancel-certificate copy decision_packet_session_dir")
        }));
    }

    #[test]
    fn verify_fails_when_copied_nested_provenance_certificate_execute_frozen_result_drifts() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "pulpit_receipt_latest_verify_current_authorization_result_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.chancel_certificate_report_path).unwrap();
        report["execute_frozen_result"] = json!("tampered_execute_frozen_result");
        persist_json(&paths.chancel_certificate_report_path, &report).unwrap();

        let verify = verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored chancel-certificate copy execute_frozen_result")
        }));
    }

    #[test]
    fn verify_fails_when_copied_nested_chancel_certificate_verdict_drifts() {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "pulpit_receipt_latest_verify_release_capsule_verdict_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.chancel_certificate_report_path).unwrap();
        report["verdict"] = json!("tampered_release_capsule_verdict");
        persist_json(&paths.chancel_certificate_report_path, &report).unwrap();

        let verify = verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
        );
        assert!(verify
            .verification_mismatches
            .iter()
            .any(|entry| entry.contains("stored chancel-certificate copy verdict")));
    }

    #[test]
    fn verify_fails_when_copied_nested_provenance_certificate_current_live_cutover_controller_summary_drifts(
    ) {
        let fixture = fake_pulpit_receipt_latest_fixture(
            "pulpit_receipt_latest_verify_current_live_cutover_summary_drift",
            "runnable_when_gate_truth_turns_green",
            "ready_for_manual_go_live_signoff",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();

        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.chancel_certificate_report_path).unwrap();
        report["reviewed_frozen_live_cutover_controller_command_summary"] =
            json!("tampered live cutover controller summary");
        persist_json(&paths.chancel_certificate_report_path, &report).unwrap();

        let verify = verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains(
                "stored chancel-certificate copy reviewed_frozen_live_cutover_controller_command_summary",
            )
        }));
    }

    #[test]
    fn all_modes_preserve_stage3_and_pre_activation_refusal_semantics_and_do_not_imply_activation_authorization_by_themselves(
    ) {
        {
            let fixture = fake_pulpit_receipt_latest_fixture(
                "pulpit_receipt_latest_stage3",
                "refused_now_by_stage3",
                "refused_now_by_stage3",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);

            let plan = plan_latest_chancel_certificate_report(&fixture.plan_config()).unwrap();
            assert_eq!(
                plan.verdict,
                TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestPlanReady
            );
            assert_eq!(plan.mode, "plan_latest_chancel_certificate");
            assert!(!plan.activation_authorized);

            let run = run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();
            assert_eq!(
                run.verdict,
                TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestRefusedNowByStage3
            );
            assert_eq!(run.mode, "run_latest_chancel_certificate");
            assert_eq!(run.result.as_deref(), Some("refused_now_by_stage3"));
            assert!(!run.activation_authorized);

            let verify =
                verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyOk,
                "{:#?}",
                verify.verification_mismatches
            );
            assert_eq!(verify.mode, "verify_latest_chancel_certificate");
            assert!(!verify.activation_authorized);
        }

        {
            let fixture = fake_pulpit_receipt_latest_fixture(
                "pulpit_receipt_latest_pre_activation",
                "refused_now_by_pre_activation_gate",
                "refused_now_by_pre_activation_gate",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);

            let run = run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();
            assert_eq!(
                run.verdict,
                TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestRefusedNowByPreActivationGate
            );
            assert_eq!(
                run.result.as_deref(),
                Some("refused_now_by_pre_activation_gate")
            );
            assert!(!run.activation_authorized);

            let verify =
                verify_latest_chancel_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestVerifyOk,
                "{:#?}",
                verify.verification_mismatches
            );
            assert!(!verify.activation_authorized);
        }

        {
            let fixture = fake_pulpit_receipt_latest_fixture(
                "pulpit_receipt_latest_green",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_go_live_signoff",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);

            let run = run_latest_chancel_certificate_report(&fixture.run_config()).unwrap();
            assert_eq!(
                run.verdict,
                TinyLivePackageChancelCertificateLatestVerdict::TinyLivePackageChancelCertificateLatestReadyForManualExecutionWhenGateTurnsGreen
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

    fn fake_pulpit_receipt_latest_fixture(
        label: &str,
        pulpit_receipt_latest_run_result: &str,
        handoff_bundle_result: &str,
    ) -> FakePulpitReceiptLatestFixture {
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
        let pulpit_receipt_latest_plan_json_path = temp_dir.join("pulpit_receipt_latest.plan.json");
        let pulpit_receipt_latest_run_json_path = temp_dir.join("pulpit_receipt_latest.run.json");
        let pulpit_receipt_latest_verify_json_path =
            temp_dir.join("pulpit_receipt_latest.verify.json");
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

        let fixture = FakePulpitReceiptLatestFixture {
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
            pulpit_receipt_latest_plan_json_path,
            pulpit_receipt_latest_run_json_path,
            pulpit_receipt_latest_verify_json_path,
            handoff_bundle_run_json_path,
            handoff_bundle_verify_json_path,
            handoff_bundle_session_json_path,
            handoff_bundle_status_json_path,
            frozen_controller_command,
        };

        persist_json(
            &fixture.pulpit_receipt_latest_plan_json_path,
            &default_pulpit_receipt_latest_plan_report(&fixture),
        )
        .unwrap();
        persist_json(
            &fixture.pulpit_receipt_latest_run_json_path,
            &default_pulpit_receipt_latest_run_report(&fixture, pulpit_receipt_latest_run_result),
        )
        .unwrap();
        persist_json(
            &fixture.pulpit_receipt_latest_verify_json_path,
            &default_pulpit_receipt_latest_verify_report(
                &fixture,
                pulpit_receipt_latest_run_result,
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

        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        write_fake_pulpit_receipt_latest_command(
            &fixture.bin_dir.join(PULPIT_RECEIPT_LATEST_BIN),
            &fixture.pulpit_receipt_latest_plan_json_path,
            &fixture.pulpit_receipt_latest_run_json_path,
            &fixture.pulpit_receipt_latest_verify_json_path,
            &fixture.root,
            &paths.nested_pulpit_receipt_latest_session_dir,
        );
        write_fake_release_capsule_command(
            &fixture.bin_dir.join(CHANCEL_CERTIFICATE_BIN),
            &fixture.handoff_bundle_run_json_path,
            &fixture.handoff_bundle_verify_json_path,
            &fixture.handoff_bundle_session_json_path,
            &fixture.handoff_bundle_status_json_path,
            &expected_downstream_pulpit_receipt_session_dir(&paths),
            &expected_downstream_decision_packet_session_dir(&paths),
            &paths.nested_chancel_certificate_session_dir,
        );

        fixture
    }

    fn default_pulpit_receipt_latest_plan_report(
        fixture: &FakePulpitReceiptLatestFixture,
    ) -> Value {
        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        json!({
            "generated_at": Utc::now(),
            "mode": "plan_latest_pulpit_receipt",
            "verdict": PULPIT_RECEIPT_LATEST_PLAN_READY,
            "reason": "latest immutable chain resolves cleanly into accepted latest-pulpit-receipt contract",
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
            "session_dir": paths.nested_pulpit_receipt_latest_session_dir.display().to_string(),
            "downstream_decision_packet_session_dir": expected_downstream_decision_packet_session_dir(&paths).display().to_string(),
            "latest_execute_verdict": "tiny_live_package_execute_latest_plan_ready",
            "decision_packet_plan_verdict": "tiny_live_package_decision_packet_plan_ready",
            "reviewed_frozen_live_cutover_controller_command_summary": fixture.frozen_controller_command,
            "clerestory_certificate_summary": "latest clerestory summary",
            "clerestory_certificate_sha256": "latest-clerestory-sha",
            "clerestory_certificate_algorithm": "sha256",
            "activation_authorized": false,
            "explicit_statement": "latest pulpit-receipt wrapper plan"
        })
    }

    fn canonical_pulpit_receipt_latest_result(result: &str) -> &str {
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

    fn default_pulpit_receipt_latest_run_report(
        fixture: &FakePulpitReceiptLatestFixture,
        result: &str,
    ) -> Value {
        let result = canonical_pulpit_receipt_latest_result(result);
        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let (verdict, gate_verdict, gate_reason) = match result {
            "ready_for_manual_execution_when_gate_turns_green" => (
                "tiny_live_package_pulpit_receipt_latest_ready_for_manual_execution_when_gate_turns_green",
                Value::Null,
                Value::Null,
            ),
            "refused_now_by_stage3" => (
                "tiny_live_package_pulpit_receipt_latest_refused_now_by_stage3",
                json!("blocked"),
                json!("stage3 blocked"),
            ),
            "refused_now_by_pre_activation_gate" => (
                "tiny_live_package_pulpit_receipt_latest_refused_now_by_pre_activation_gate",
                json!("blocked"),
                json!("pre gate blocked"),
            ),
            "refused_by_missing_latest_chain" => (
                "tiny_live_package_pulpit_receipt_latest_refused_by_missing_latest_chain",
                Value::Null,
                Value::Null,
            ),
            "refused_by_invalid_latest_chain" => (
                "tiny_live_package_pulpit_receipt_latest_refused_by_invalid_latest_chain",
                Value::Null,
                Value::Null,
            ),
            _ => (
                "tiny_live_package_pulpit_receipt_latest_refused_by_drifted_pulpit_receipt_contract",
                Value::Null,
                Value::Null,
            ),
        };
        json!({
            "generated_at": Utc::now(),
            "mode": "run_latest_pulpit_receipt",
            "verdict": verdict,
            "reason": format!("latest-pulpit-receipt {result}"),
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
            "session_dir": paths.nested_pulpit_receipt_latest_session_dir.display().to_string(),
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
            "explicit_statement": "latest pulpit-receipt wrapper run"
        })
    }

    fn default_pulpit_receipt_latest_verify_report(
        fixture: &FakePulpitReceiptLatestFixture,
        result: &str,
    ) -> Value {
        let mut report = default_pulpit_receipt_latest_run_report(fixture, result);
        report["mode"] = json!("verify_latest_pulpit_receipt");
        report["verdict"] = json!(PULPIT_RECEIPT_LATEST_VERIFY_OK);
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

    fn default_provenance_certificate_session(fixture: &FakePulpitReceiptLatestFixture) -> Value {
        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let pulpit_receipt_session_dir = expected_downstream_pulpit_receipt_session_dir(&paths);
        let substructure_certificate_session_dir = fixture
            .temp_dir
            .join("downstream-basal-receipt-substructure-certificate-session");
        let pedestal_certificate_session_dir = fixture
            .temp_dir
            .join("downstream-pulpit-receipt-pedestal-certificate-session");
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
        let snapshot = LatestPulpitReceiptSnapshot {
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
            "pulpit_receipt_session_dir": pulpit_receipt_session_dir.display().to_string(),
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
            "session_dir": chancel_certificate_latest_paths(&fixture.wrapper_session_dir).nested_chancel_certificate_session_dir.display().to_string(),
            "verify_pulpit_receipt_command_summary": verify_chancel_certificate_input_command_summary(&pulpit_receipt_session_dir),
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
            "pulpit_receipt_summary": "Pulpit receipt identity: fixture immutable crest seal over verified lectern-certificate truth.",
            "pulpit_receipt_sha256": "fixture-pulpit-receipt-sha256",
            "pulpit_receipt_algorithm": "sha256",
            "chancel_certificate_summary": "Chancel certificate identity: fixture immutable herald seal over verified pulpit-receipt truth.",
            "chancel_certificate_sha256": "fixture-chancel-certificate-sha256",
            "chancel_certificate_algorithm": "sha256",
            "explicit_statement": "the verified pulpit-receipt session is the primary input here; this immutable chancel certificate freezes one final herald identity over the fully culminated chain without enabling production execution"
        })
    }

    fn default_provenance_certificate_status(
        fixture: &FakePulpitReceiptLatestFixture,
        result: &str,
    ) -> Value {
        let result = canonical_release_capsule_result(result);
        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let substructure_certificate_session_dir = fixture
            .temp_dir
            .join("downstream-basal-receipt-substructure-certificate-session");
        let pedestal_certificate_session_dir = fixture
            .temp_dir
            .join("downstream-pulpit-receipt-pedestal-certificate-session");
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
            "session_dir": paths.nested_chancel_certificate_session_dir.display().to_string(),
            "pulpit_receipt_session_dir": expected_downstream_pulpit_receipt_session_dir(&paths).display().to_string(),
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
            "pulpit_receipt_step": Value::Null,
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
            "pulpit_receipt_summary": "Pulpit receipt identity: fixture status immutable crest seal over verified lectern-certificate truth.",
            "pulpit_receipt_sha256": "fixture-pulpit-receipt-sha256",
            "pulpit_receipt_algorithm": "sha256",
            "chancel_certificate_summary": "Chancel certificate identity: fixture status immutable herald seal over verified pulpit-receipt truth.",
            "chancel_certificate_sha256": "fixture-chancel-certificate-sha256",
            "chancel_certificate_algorithm": "sha256",
            "explicit_statement": "this chancel certificate is archival and read-only; it never executes or enables the frozen controller"
        })
    }

    fn default_provenance_certificate_run_report(
        fixture: &FakePulpitReceiptLatestFixture,
        result: &str,
    ) -> Value {
        let result = canonical_release_capsule_result(result);
        let paths = chancel_certificate_latest_paths(&fixture.wrapper_session_dir);
        let status = default_provenance_certificate_status(fixture, result);
        let chancel_certificate_verdict = match result {
            "ready_for_manual_execution_when_gate_turns_green" => {
                "tiny_live_package_chancel_certificate_ready_for_manual_execution_when_gate_turns_green"
            }
            "refused_now_by_stage3" => "tiny_live_package_chancel_certificate_refused_now_by_stage3",
            "refused_now_by_pre_activation_gate" => {
                "tiny_live_package_chancel_certificate_refused_now_by_pre_activation_gate"
            }
            _ => "tiny_live_package_chancel_certificate_refused_now_by_invalid_or_drifted_contract",
        };
        let substructure_certificate_session_dir = fixture
            .temp_dir
            .join("downstream-basal-receipt-substructure-certificate-session");
        let pedestal_certificate_session_dir = fixture
            .temp_dir
            .join("downstream-pulpit-receipt-pedestal-certificate-session");
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
            "mode": "run_live_package_chancel_certificate",
            "verdict": chancel_certificate_verdict,
            "reason": status["reason"],
            "pulpit_receipt_session_dir": expected_downstream_pulpit_receipt_session_dir(&paths).display().to_string(),
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
            "session_dir": paths.nested_chancel_certificate_session_dir.display().to_string(),
            "chancel_certificate_session_path": nested_chancel_certificate_session_path(&paths.nested_chancel_certificate_session_dir).display().to_string(),
            "chancel_certificate_status_path": nested_chancel_certificate_status_path(&paths.nested_chancel_certificate_session_dir).display().to_string(),
            "archived_pulpit_receipt_report_path": paths.nested_chancel_certificate_session_dir.join("tiny_live_activation_package_chancel_certificate.pulpit_receipt.report.json").display().to_string(),
            "result": result,
            "handoff_bundle_result": status["handoff_bundle_result"],
            "decision_packet_result": status["decision_packet_result"],
            "execute_frozen_result": status["execute_frozen_result"],
            "pulpit_receipt_step": status["pulpit_receipt_step"],
            "verify_pulpit_receipt_command_summary": verify_chancel_certificate_input_command_summary(&expected_downstream_pulpit_receipt_session_dir(&paths)),
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
            "pulpit_receipt_summary": status["pulpit_receipt_summary"],
            "pulpit_receipt_sha256": status["pulpit_receipt_sha256"],
            "pulpit_receipt_algorithm": status["pulpit_receipt_algorithm"],
            "chancel_certificate_summary": status["chancel_certificate_summary"],
            "chancel_certificate_sha256": status["chancel_certificate_sha256"],
            "chancel_certificate_algorithm": status["chancel_certificate_algorithm"],
            "current_pre_activation_gate_verdict": status["current_pre_activation_gate_verdict"],
            "current_pre_activation_gate_reason": status["current_pre_activation_gate_reason"],
            "explicit_statement": status["explicit_statement"]
        })
    }

    fn default_provenance_certificate_verify_report(
        fixture: &FakePulpitReceiptLatestFixture,
        result: &str,
    ) -> Value {
        let mut report = default_provenance_certificate_run_report(fixture, result);
        report["mode"] = json!("verify_live_package_chancel_certificate");
        report["verdict"] = json!(CHANCEL_CERTIFICATE_VERIFY_OK);
        report["reason"] = json!("verify ok");
        report["explicit_statement"] = json!("this verification binds one immutable chancel certificate to verified pulpit-receipt truth, the exact reviewed frozen controller summary, the canonical chain fingerprint, the ledger seal, the registry entry, the filing-certificate identity, the archive-receipt identity, the closure-certificate identity, the finality-receipt identity, the consummation-record identity, the completion-certificate identity, the culmination-receipt identity, the summit-certificate identity, the pinnacle-receipt identity, the capstone-certificate identity, the keystone-receipt identity, the cornerstone-certificate identity, the foundation-receipt identity, the bedrock-certificate identity, the basal-receipt identity, the substructure-certificate identity, the plinth-receipt identity, the pedestal-certificate identity, the dais-receipt identity, the rostrum-certificate identity, and the pulpit-receipt identity");
        report
    }

    fn write_fake_pulpit_receipt_latest_command(
        path: &Path,
        plan_json_path: &Path,
        run_json_path: &Path,
        verify_json_path: &Path,
        root: &Path,
        session_dir: &Path,
    ) {
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --session-dir {} \"*) : ;;\n  *) echo 'unexpected latest-pulpit-receipt session-dir' >&2; exit 1;;\nesac\nmode=''\ncase \" $* \" in\n  *\" --plan-latest-pulpit-receipt \"*) mode='plan';;\n  *\" --run-latest-pulpit-receipt \"*) mode='run';;\n  *\" --verify-latest-pulpit-receipt \"*) mode='verify';;\n  *) echo 'unexpected latest-pulpit-receipt mode' >&2; exit 1;;\nesac\ncase \"$mode\" in\n  plan|run)\n    case \" $* \" in\n      *\" --root {} \"*) : ;;\n      *) echo 'unexpected root' >&2; exit 1;;\n    esac\n    ;;\nesac\ncase \"$mode\" in\n  plan) cat '{}';;\n  run) mkdir -p '{}'; cp '{}' '{}/tiny_live_activation_package_pulpit_receipt_latest.report.json'; cat '{}';;\n  verify) cat '{}';;\n  *) exit 1;;\nesac\n",
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

    fn overwrite_fake_pulpit_receipt_latest_command(
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
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --session-dir {} \"*) : ;;\n  *) echo 'unexpected latest-pulpit-receipt session-dir' >&2; exit 1;;\nesac\nmode=''\ncase \" $* \" in\n  *\" --plan-latest-pulpit-receipt \"*) mode='plan';;\n  *\" --run-latest-pulpit-receipt \"*) mode='run';;\n  *\" --verify-latest-pulpit-receipt \"*) mode='verify';;\n  *) echo 'unexpected latest-pulpit-receipt mode' >&2; exit 1;;\nesac\ncase \"$mode\" in\n  plan|run)\n    case \" $* \" in\n      *\" --root {} \"*) : ;;\n      *) echo 'unexpected root' >&2; exit 1;;\n    esac\n    ;;\nesac\ncase \"$mode\" in\n  plan) {};;\n  run) {};;\n  verify) {};;\n  *) exit 1;;\nesac\n",
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
        let nested_session_path = nested_chancel_certificate_session_path(session_dir);
        let nested_status_path = nested_chancel_certificate_status_path(session_dir);
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --pulpit-receipt-session-dir {} \"*) : ;;\n  *) echo 'unexpected pulpit-receipt-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --confirm-decision-packet-session-dir {} \"*) : ;;\n  *) echo 'unexpected confirm-decision-packet-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --session-dir {} \"*) : ;;\n  *) echo 'unexpected chancel-certificate session-dir' >&2; exit 1;;\nesac\nmode=''\ncase \" $* \" in\n  *\" --run-live-package-chancel-certificate \"*) mode='run';;\n  *\" --verify-live-package-chancel-certificate \"*) mode='verify';;\n  *) echo 'unexpected chancel-certificate mode' >&2; exit 1;;\nesac\ncase \"$mode\" in\n  run) mkdir -p '{}'; cp '{}' '{}'; cp '{}' '{}'; cat '{}';;\n  verify) cat '{}';;\n  *) exit 1;;\nesac\n",
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
        let nested_session_path = nested_chancel_certificate_session_path(session_dir);
        let nested_status_path = nested_chancel_certificate_status_path(session_dir);
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
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --pulpit-receipt-session-dir {} \"*) : ;;\n  *) echo 'unexpected pulpit-receipt-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --confirm-decision-packet-session-dir {} \"*) : ;;\n  *) echo 'unexpected confirm-decision-packet-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --session-dir {} \"*) : ;;\n  *) echo 'unexpected chancel-certificate session-dir' >&2; exit 1;;\nesac\nmode=''\ncase \" $* \" in\n  *\" --run-live-package-chancel-certificate \"*) mode='run';;\n  *\" --verify-live-package-chancel-certificate \"*) mode='verify';;\n  *) echo 'unexpected chancel-certificate mode' >&2; exit 1;;\nesac\ncase \"$mode\" in\n  run) {};;\n  verify) {};;\n  *) exit 1;;\nesac\n",
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
