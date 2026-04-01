use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_app::tiny_live_activation::install_target_managed_surface::validate_turn_green_session_dir as validate_managed_surface_session_dir;
use copybot_app::tiny_live_activation::package_apse_receipt_sanctuary_certificate;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: copybot_tiny_live_activation_package_sanctuary_certificate --apse-receipt-session-dir <path> [--confirm-decision-packet-session-dir <path>] [--session-dir <path>] [--output <path>] [--json] (--plan-live-package-sanctuary-certificate | --render-live-package-sanctuary-certificate | --run-live-package-sanctuary-certificate | --verify-live-package-sanctuary-certificate)";
const SANCTUARY_CERTIFICATE_SESSION_VERSION: &str = "1";
const SANCTUARY_CERTIFICATE_STATUS_VERSION: &str = "1";
const SANCTUARY_CERTIFICATE_ALGORITHM: &str = "sha256";
const SANCTUARY_CERTIFICATE_SESSION_EXPLICIT_STATEMENT: &str =
    "the verified apse-receipt session is the primary input here; this immutable sanctuary certificate freezes one final banner-seal identity over the fully culminated chain without enabling production execution";
const SANCTUARY_CERTIFICATE_STATUS_EXPLICIT_STATEMENT: &str =
    "this sanctuary certificate is archival and read-only; it never executes or enables the frozen controller";
const SANCTUARY_CERTIFICATE_VERIFY_EXPLICIT_STATEMENT: &str =
    "this verification binds one immutable sanctuary certificate to verified apse-receipt truth, the exact reviewed frozen controller summary, the canonical chain fingerprint, the ledger seal, the registry entry, the filing-certificate identity, the archive-receipt identity, the closure-certificate identity, the finality-receipt identity, the consummation-record identity, the completion-certificate identity, the culmination-receipt identity, the summit-certificate identity, the pinnacle-receipt identity, the capstone-certificate identity, the keystone-receipt identity, the cornerstone-certificate identity, the foundation-receipt identity, the bedrock-certificate identity, the basal-receipt identity, the substructure-certificate identity, the plinth-receipt identity, the pedestal-certificate identity, the dais-receipt identity, the rostrum-certificate identity, the podium-receipt identity, the lectern-certificate identity, the pulpit-receipt identity, the chancel-certificate identity, and the apse-receipt identity";

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
    apse_receipt_session_dir: PathBuf,
    confirmed_decision_packet_session_dir: Option<PathBuf>,
    session_dir: Option<PathBuf>,
    output_path: Option<PathBuf>,
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    PlanLivePackageSanctuaryCertificate,
    RenderLivePackageSanctuaryCertificate,
    RunLivePackageSanctuaryCertificate,
    VerifyLivePackageSanctuaryCertificate,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageSanctuaryCertificateVerdict {
    TinyLivePackageSanctuaryCertificatePlanReady,
    TinyLivePackageSanctuaryCertificateRendered,
    TinyLivePackageSanctuaryCertificateRefusedNowByStage3,
    TinyLivePackageSanctuaryCertificateRefusedNowByPreActivationGate,
    TinyLivePackageSanctuaryCertificateRefusedNowByInvalidOrDriftedContract,
    TinyLivePackageSanctuaryCertificateReadyForManualExecutionWhenGateTurnsGreen,
    TinyLivePackageSanctuaryCertificateVerifyOk,
    TinyLivePackageSanctuaryCertificateVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LivePackageSanctuaryCertificateResult {
    RefusedNowByStage3,
    RefusedNowByPreActivationGate,
    RefusedNowByInvalidOrDriftedContract,
    ReadyForManualExecutionWhenGateTurnsGreen,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PackageSanctuaryCertificateStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PackageSanctuaryCertificateSession {
    session_version: String,
    planned_at: DateTime<Utc>,
    apse_receipt_session_dir: String,
    lectern_certificate_session_dir: String,
    dais_receipt_session_dir: String,
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
    verify_apse_receipt_command_summary: String,
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
    summit_certificate_summary: String,
    summit_certificate_sha256: String,
    summit_certificate_algorithm: String,
    culmination_receipt_summary: String,
    culmination_receipt_sha256: String,
    culmination_receipt_algorithm: String,
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
    dais_receipt_summary: String,
    dais_receipt_sha256: String,
    dais_receipt_algorithm: String,
    rostrum_certificate_summary: String,
    rostrum_certificate_sha256: String,
    rostrum_certificate_algorithm: String,
    podium_receipt_summary: String,
    podium_receipt_sha256: String,
    podium_receipt_algorithm: String,
    lectern_certificate_summary: String,
    lectern_certificate_sha256: String,
    lectern_certificate_algorithm: String,
    pulpit_receipt_summary: String,
    pulpit_receipt_sha256: String,
    pulpit_receipt_algorithm: String,
    chancel_certificate_summary: String,
    chancel_certificate_sha256: String,
    chancel_certificate_algorithm: String,
    apse_receipt_summary: String,
    apse_receipt_sha256: String,
    apse_receipt_algorithm: String,
    sanctuary_certificate_summary: String,
    sanctuary_certificate_sha256: String,
    sanctuary_certificate_algorithm: String,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PackageSanctuaryCertificateStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    session_dir: String,
    apse_receipt_session_dir: String,
    lectern_certificate_session_dir: String,
    dais_receipt_session_dir: String,
    pedestal_certificate_session_dir: String,
    plinth_receipt_session_dir: String,
    substructure_certificate_session_dir: String,
    basal_receipt_session_dir: String,
    bedrock_certificate_session_dir: String,
    foundation_receipt_session_dir: String,
    result: LivePackageSanctuaryCertificateResult,
    reason: String,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    apse_receipt_step: Option<PackageSanctuaryCertificateStepArtifact>,
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
    summit_certificate_summary: String,
    summit_certificate_sha256: String,
    summit_certificate_algorithm: String,
    culmination_receipt_summary: String,
    culmination_receipt_sha256: String,
    culmination_receipt_algorithm: String,
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
    dais_receipt_summary: String,
    dais_receipt_sha256: String,
    dais_receipt_algorithm: String,
    rostrum_certificate_summary: String,
    rostrum_certificate_sha256: String,
    rostrum_certificate_algorithm: String,
    podium_receipt_summary: String,
    podium_receipt_sha256: String,
    podium_receipt_algorithm: String,
    lectern_certificate_summary: String,
    lectern_certificate_sha256: String,
    lectern_certificate_algorithm: String,
    pulpit_receipt_summary: String,
    pulpit_receipt_sha256: String,
    pulpit_receipt_algorithm: String,
    chancel_certificate_summary: String,
    chancel_certificate_sha256: String,
    chancel_certificate_algorithm: String,
    apse_receipt_summary: String,
    apse_receipt_sha256: String,
    apse_receipt_algorithm: String,
    sanctuary_certificate_summary: String,
    sanctuary_certificate_sha256: String,
    sanctuary_certificate_algorithm: String,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackageSanctuaryCertificatePaths {
    session_path: PathBuf,
    status_path: PathBuf,
    apse_receipt_report_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageSanctuaryCertificateReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageSanctuaryCertificateVerdict,
    reason: String,
    apse_receipt_session_dir: String,
    lectern_certificate_session_dir: Option<String>,
    dais_receipt_session_dir: Option<String>,
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
    apse_receipt_session_path: Option<String>,
    apse_receipt_status_path: Option<String>,
    archived_apse_receipt_report_path: Option<String>,
    result: Option<String>,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    apse_receipt_step: Option<PackageSanctuaryCertificateStepArtifact>,
    verify_apse_receipt_command_summary: Option<String>,
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
    summit_certificate_summary: Option<String>,
    summit_certificate_sha256: Option<String>,
    summit_certificate_algorithm: Option<String>,
    culmination_receipt_summary: Option<String>,
    culmination_receipt_sha256: Option<String>,
    culmination_receipt_algorithm: Option<String>,
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
    dais_receipt_summary: Option<String>,
    dais_receipt_sha256: Option<String>,
    dais_receipt_algorithm: Option<String>,
    rostrum_certificate_summary: Option<String>,
    rostrum_certificate_sha256: Option<String>,
    rostrum_certificate_algorithm: Option<String>,
    podium_receipt_summary: Option<String>,
    podium_receipt_sha256: Option<String>,
    podium_receipt_algorithm: Option<String>,
    lectern_certificate_summary: Option<String>,
    lectern_certificate_sha256: Option<String>,
    lectern_certificate_algorithm: Option<String>,
    pulpit_receipt_summary: Option<String>,
    pulpit_receipt_sha256: Option<String>,
    pulpit_receipt_algorithm: Option<String>,
    chancel_certificate_summary: Option<String>,
    chancel_certificate_sha256: Option<String>,
    chancel_certificate_algorithm: Option<String>,
    apse_receipt_summary: Option<String>,
    apse_receipt_sha256: Option<String>,
    apse_receipt_algorithm: Option<String>,
    sanctuary_certificate_summary: Option<String>,
    sanctuary_certificate_sha256: Option<String>,
    sanctuary_certificate_algorithm: Option<String>,
    verification_mismatches: Vec<String>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Deserialize)]
struct NestedPulpitReceiptReportView {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: String,
    reason: String,
}

#[derive(Debug, Serialize)]
struct SanctuaryCertificatePayload<'a> {
    result: &'a str,
    handoff_bundle_result: Option<&'a str>,
    decision_packet_result: Option<&'a str>,
    execute_frozen_result: Option<&'a str>,
    current_pre_activation_gate_verdict: Option<&'a str>,
    current_pre_activation_gate_reason: Option<&'a str>,
    reviewed_frozen_live_cutover_controller_command_summary: &'a str,
    chain_fingerprint_sha256: &'a str,
    chain_fingerprint_algorithm: &'a str,
    release_capsule_digest_manifest_sha256: &'a str,
    release_capsule_digest_manifest_entry_count: usize,
    release_capsule_digest_algorithm: &'a str,
    ledger_seal_sha256: &'a str,
    ledger_seal_algorithm: &'a str,
    registry_entry_sha256: &'a str,
    registry_entry_algorithm: &'a str,
    filing_certificate_sha256: &'a str,
    filing_certificate_algorithm: &'a str,
    archive_receipt_sha256: &'a str,
    archive_receipt_algorithm: &'a str,
    closure_certificate_summary: &'a str,
    closure_certificate_sha256: &'a str,
    closure_certificate_algorithm: &'a str,
    finality_receipt_sha256: &'a str,
    finality_receipt_algorithm: &'a str,
    consummation_record_sha256: &'a str,
    consummation_record_algorithm: &'a str,
    completion_certificate_summary: &'a str,
    completion_certificate_sha256: &'a str,
    completion_certificate_algorithm: &'a str,
    summit_certificate_summary: &'a str,
    summit_certificate_sha256: &'a str,
    summit_certificate_algorithm: &'a str,
    culmination_receipt_summary: &'a str,
    culmination_receipt_sha256: &'a str,
    culmination_receipt_algorithm: &'a str,
    pinnacle_receipt_summary: &'a str,
    pinnacle_receipt_sha256: &'a str,
    pinnacle_receipt_algorithm: &'a str,
    capstone_certificate_summary: &'a str,
    capstone_certificate_sha256: &'a str,
    capstone_certificate_algorithm: &'a str,
    keystone_receipt_summary: &'a str,
    keystone_receipt_sha256: &'a str,
    keystone_receipt_algorithm: &'a str,
    cornerstone_certificate_summary: &'a str,
    cornerstone_certificate_sha256: &'a str,
    cornerstone_certificate_algorithm: &'a str,
    foundation_receipt_summary: &'a str,
    foundation_receipt_sha256: &'a str,
    foundation_receipt_algorithm: &'a str,
    bedrock_certificate_summary: &'a str,
    bedrock_certificate_sha256: &'a str,
    bedrock_certificate_algorithm: &'a str,
    basal_receipt_summary: &'a str,
    basal_receipt_sha256: &'a str,
    basal_receipt_algorithm: &'a str,
    substructure_certificate_summary: &'a str,
    substructure_certificate_sha256: &'a str,
    substructure_certificate_algorithm: &'a str,
    plinth_receipt_summary: &'a str,
    plinth_receipt_sha256: &'a str,
    plinth_receipt_algorithm: &'a str,
    pedestal_certificate_summary: &'a str,
    pedestal_certificate_sha256: &'a str,
    pedestal_certificate_algorithm: &'a str,
    dais_receipt_summary: &'a str,
    dais_receipt_sha256: &'a str,
    dais_receipt_algorithm: &'a str,
    rostrum_certificate_summary: &'a str,
    rostrum_certificate_sha256: &'a str,
    rostrum_certificate_algorithm: &'a str,
    podium_receipt_summary: &'a str,
    podium_receipt_sha256: &'a str,
    podium_receipt_algorithm: &'a str,
    lectern_certificate_summary: &'a str,
    lectern_certificate_sha256: &'a str,
    lectern_certificate_algorithm: &'a str,
    pulpit_receipt_summary: &'a str,
    pulpit_receipt_sha256: &'a str,
    pulpit_receipt_algorithm: &'a str,
    chancel_certificate_summary: &'a str,
    chancel_certificate_sha256: &'a str,
    chancel_certificate_algorithm: &'a str,
    apse_receipt_summary: &'a str,
    apse_receipt_sha256: &'a str,
    apse_receipt_algorithm: &'a str,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut apse_receipt_session_dir: Option<PathBuf> = None;
    let mut confirmed_decision_packet_session_dir: Option<PathBuf> = None;
    let mut session_dir: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut json = false;
    let mut mode: Option<Mode> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--apse-receipt-session-dir" => {
                apse_receipt_session_dir = Some(PathBuf::from(parse_string_arg(
                    "--apse-receipt-session-dir",
                    args.next(),
                )?))
            }
            "--confirm-decision-packet-session-dir" => {
                confirmed_decision_packet_session_dir = Some(PathBuf::from(parse_string_arg(
                    "--confirm-decision-packet-session-dir",
                    args.next(),
                )?))
            }
            "--session-dir" => {
                session_dir = Some(PathBuf::from(parse_string_arg(
                    "--session-dir",
                    args.next(),
                )?))
            }
            "--output" => {
                output_path = Some(PathBuf::from(parse_string_arg("--output", args.next())?))
            }
            "--json" => json = true,
            "--plan-live-package-sanctuary-certificate" => set_mode(
                &mut mode,
                Mode::PlanLivePackageSanctuaryCertificate,
                arg.as_str(),
            )?,
            "--render-live-package-sanctuary-certificate" => set_mode(
                &mut mode,
                Mode::RenderLivePackageSanctuaryCertificate,
                arg.as_str(),
            )?,
            "--run-live-package-sanctuary-certificate" => set_mode(
                &mut mode,
                Mode::RunLivePackageSanctuaryCertificate,
                arg.as_str(),
            )?,
            "--verify-live-package-sanctuary-certificate" => set_mode(
                &mut mode,
                Mode::VerifyLivePackageSanctuaryCertificate,
                arg.as_str(),
            )?,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    let Some(mode) = mode else {
        return Ok(None);
    };
    if matches!(mode, Mode::RenderLivePackageSanctuaryCertificate) && output_path.is_none() {
        bail!("missing --output for --render-live-package-sanctuary-certificate");
    }
    if matches!(
        mode,
        Mode::RunLivePackageSanctuaryCertificate | Mode::VerifyLivePackageSanctuaryCertificate
    ) && session_dir.is_none()
    {
        bail!("missing --session-dir for run/verify mode");
    }
    if matches!(
        mode,
        Mode::RunLivePackageSanctuaryCertificate | Mode::VerifyLivePackageSanctuaryCertificate
    ) && confirmed_decision_packet_session_dir.is_none()
    {
        bail!("missing --confirm-decision-packet-session-dir for run/verify mode");
    }
    let apse_receipt_session_dir =
        apse_receipt_session_dir.ok_or_else(|| anyhow!("missing --apse-receipt-session-dir"))?;
    Ok(Some(Config {
        mode,
        apse_receipt_session_dir,
        confirmed_decision_packet_session_dir,
        session_dir,
        output_path,
        json,
    }))
}

fn run(config: Config) -> Result<String> {
    let report = match config.mode {
        Mode::PlanLivePackageSanctuaryCertificate => {
            plan_live_package_sanctuary_certificate_report(&config)?
        }
        Mode::RenderLivePackageSanctuaryCertificate => {
            render_live_package_sanctuary_certificate_report(&config)?
        }
        Mode::RunLivePackageSanctuaryCertificate => {
            run_live_package_sanctuary_certificate_report(&config)?
        }
        Mode::VerifyLivePackageSanctuaryCertificate => {
            verify_live_package_sanctuary_certificate_report(&config)?
        }
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_report_lines(&report))
    }
}

fn plan_live_package_sanctuary_certificate_report(
    config: &Config,
) -> Result<PackageSanctuaryCertificateReport> {
    let (apse_receipt_contract, classification) =
        match config.confirmed_decision_packet_session_dir.as_deref() {
            Some(confirmed_decision_packet_session_dir) => {
                match package_apse_receipt_sanctuary_certificate::verify_live_package_apse_receipt_for_sanctuary_certificate(
                    &config.apse_receipt_session_dir,
                    confirmed_decision_packet_session_dir,
                ) {
                    Ok(verified) => {
                        let classification = classify_verified_apse_receipt(&verified);
                        (verified.contract, classification)
                    }
                    Err(_) => {
                        let raw_contract = package_apse_receipt_sanctuary_certificate::load_live_package_apse_receipt_contract_for_sanctuary_certificate(
                            &config.apse_receipt_session_dir,
                        )?;
                        let classification =
                            classify_apse_receipt_contract(&raw_contract, false);
                        (raw_contract, classification)
                    }
                }
            }
            None => {
                let raw_contract = package_apse_receipt_sanctuary_certificate::load_live_package_apse_receipt_contract_for_sanctuary_certificate(
                    &config.apse_receipt_session_dir,
                )?;
                let classification =
                    classify_apse_receipt_contract(&raw_contract, false);
                (raw_contract, classification)
            }
        };
    let session_dir = config.session_dir.clone().unwrap_or_else(|| {
        config
            .apse_receipt_session_dir
            .join("sanctuary-certificate")
    });
    let result = serialize_enum(&classification.0);
    let sanctuary_certificate_sha256 = compute_sanctuary_certificate_sha256(
        &result,
        apse_receipt_contract.handoff_bundle_result.as_deref(),
        apse_receipt_contract.decision_packet_result.as_deref(),
        apse_receipt_contract.execute_frozen_result.as_deref(),
        apse_receipt_contract
            .current_pre_activation_gate_verdict
            .as_deref(),
        apse_receipt_contract
            .current_pre_activation_gate_reason
            .as_deref(),
        &apse_receipt_contract.reviewed_frozen_live_cutover_controller_command_summary,
        &apse_receipt_contract.chain_fingerprint_sha256,
        &apse_receipt_contract.chain_fingerprint_algorithm,
        &apse_receipt_contract.release_capsule_digest_manifest_sha256,
        apse_receipt_contract.release_capsule_digest_manifest_entry_count,
        &apse_receipt_contract.release_capsule_digest_algorithm,
        &apse_receipt_contract.ledger_seal_sha256,
        &apse_receipt_contract.ledger_seal_algorithm,
        &apse_receipt_contract.registry_entry_sha256,
        &apse_receipt_contract.registry_entry_algorithm,
        &apse_receipt_contract.filing_certificate_sha256,
        &apse_receipt_contract.filing_certificate_algorithm,
        &apse_receipt_contract.archive_receipt_sha256,
        &apse_receipt_contract.archive_receipt_algorithm,
        &apse_receipt_contract.closure_certificate_summary,
        &apse_receipt_contract.closure_certificate_sha256,
        &apse_receipt_contract.closure_certificate_algorithm,
        &apse_receipt_contract.finality_receipt_sha256,
        &apse_receipt_contract.finality_receipt_algorithm,
        &apse_receipt_contract.consummation_record_sha256,
        &apse_receipt_contract.consummation_record_algorithm,
        &apse_receipt_contract.completion_certificate_summary,
        &apse_receipt_contract.completion_certificate_sha256,
        &apse_receipt_contract.completion_certificate_algorithm,
        &apse_receipt_contract.summit_certificate_summary,
        &apse_receipt_contract.summit_certificate_sha256,
        &apse_receipt_contract.summit_certificate_algorithm,
        &apse_receipt_contract.culmination_receipt_summary,
        &apse_receipt_contract.culmination_receipt_sha256,
        &apse_receipt_contract.culmination_receipt_algorithm,
        &apse_receipt_contract.pinnacle_receipt_summary,
        &apse_receipt_contract.pinnacle_receipt_sha256,
        &apse_receipt_contract.pinnacle_receipt_algorithm,
        &apse_receipt_contract.capstone_certificate_summary,
        &apse_receipt_contract.capstone_certificate_sha256,
        &apse_receipt_contract.capstone_certificate_algorithm,
        &apse_receipt_contract.keystone_receipt_summary,
        &apse_receipt_contract.keystone_receipt_sha256,
        &apse_receipt_contract.keystone_receipt_algorithm,
        &apse_receipt_contract.cornerstone_certificate_summary,
        &apse_receipt_contract.cornerstone_certificate_sha256,
        &apse_receipt_contract.cornerstone_certificate_algorithm,
        &apse_receipt_contract.foundation_receipt_summary,
        &apse_receipt_contract.foundation_receipt_sha256,
        &apse_receipt_contract.foundation_receipt_algorithm,
        &apse_receipt_contract.bedrock_certificate_summary,
        &apse_receipt_contract.bedrock_certificate_sha256,
        &apse_receipt_contract.bedrock_certificate_algorithm,
        &apse_receipt_contract.basal_receipt_summary,
        &apse_receipt_contract.basal_receipt_sha256,
        &apse_receipt_contract.basal_receipt_algorithm,
        &apse_receipt_contract.substructure_certificate_summary,
        &apse_receipt_contract.substructure_certificate_sha256,
        &apse_receipt_contract.substructure_certificate_algorithm,
        &apse_receipt_contract.plinth_receipt_summary,
        &apse_receipt_contract.plinth_receipt_sha256,
        &apse_receipt_contract.plinth_receipt_algorithm,
        &apse_receipt_contract.pedestal_certificate_summary,
        &apse_receipt_contract.pedestal_certificate_sha256,
        &apse_receipt_contract.pedestal_certificate_algorithm,
        &apse_receipt_contract.dais_receipt_summary,
        &apse_receipt_contract.dais_receipt_sha256,
        &apse_receipt_contract.dais_receipt_algorithm,
        &apse_receipt_contract.rostrum_certificate_summary,
        &apse_receipt_contract.rostrum_certificate_sha256,
        &apse_receipt_contract.rostrum_certificate_algorithm,
        &apse_receipt_contract.podium_receipt_summary,
        &apse_receipt_contract.podium_receipt_sha256,
        &apse_receipt_contract.podium_receipt_algorithm,
        &apse_receipt_contract.lectern_certificate_summary,
        &apse_receipt_contract.lectern_certificate_sha256,
        &apse_receipt_contract.lectern_certificate_algorithm,
        &apse_receipt_contract.pulpit_receipt_summary,
        &apse_receipt_contract.pulpit_receipt_sha256,
        &apse_receipt_contract.pulpit_receipt_algorithm,
        &apse_receipt_contract.chancel_certificate_summary,
        &apse_receipt_contract.chancel_certificate_sha256,
        &apse_receipt_contract.chancel_certificate_algorithm,
        &apse_receipt_contract.apse_receipt_summary,
        &apse_receipt_contract.apse_receipt_sha256,
        &apse_receipt_contract.apse_receipt_algorithm,
    )?;

    let lectern_certificate_contract = apse_receipt_contract.clone();
    Ok(PackageSanctuaryCertificateReport {
        generated_at: Utc::now(),
        mode: "plan_live_package_sanctuary_certificate".to_string(),
        verdict:
            TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificatePlanReady,
        reason: format!(
            "apse-receipt-native sanctuary certificate is explicit for verified apse-receipt session {}; this command stays archival and read-only while freezing one final banner-seal identity over the fully culminated chain",
            config.apse_receipt_session_dir.display()
        ),
        apse_receipt_session_dir: config.apse_receipt_session_dir.display().to_string(),
        lectern_certificate_session_dir: Some(
            lectern_certificate_contract.lectern_certificate_session_dir.display().to_string(),
        ),
        dais_receipt_session_dir: Some(
            lectern_certificate_contract.dais_receipt_session_dir.display().to_string(),
        ),
        pedestal_certificate_session_dir: Some(
            lectern_certificate_contract.pedestal_certificate_session_dir.display().to_string(),
        ),
        plinth_receipt_session_dir: Some(
            lectern_certificate_contract.plinth_receipt_session_dir.display().to_string(),
        ),
        substructure_certificate_session_dir: Some(
            lectern_certificate_contract
                .substructure_certificate_session_dir
                .display()
                .to_string(),
        ),
        basal_receipt_session_dir: Some(
            lectern_certificate_contract
                .basal_receipt_session_dir
                .display()
                .to_string(),
        ),
        bedrock_certificate_session_dir: Some(
            lectern_certificate_contract
                .bedrock_certificate_session_dir
                .display()
                .to_string(),
        ),
        foundation_receipt_session_dir: Some(
            lectern_certificate_contract
                .foundation_receipt_session_dir
                .display()
                .to_string(),
        ),
        registry_entry_session_dir: Some(
            lectern_certificate_contract.registry_entry_session_dir.display().to_string(),
        ),
        notarization_receipt_session_dir: Some(
            lectern_certificate_contract
                .notarization_receipt_session_dir
                .display()
                .to_string(),
        ),
        provenance_certificate_session_dir: Some(
            lectern_certificate_contract
                .provenance_certificate_session_dir
                .display()
                .to_string(),
        ),
        attestation_seal_session_dir: Some(
            lectern_certificate_contract
                .attestation_seal_session_dir
                .display()
                .to_string(),
        ),
        release_capsule_session_dir: Some(
            lectern_certificate_contract
                .release_capsule_session_dir
                .display()
                .to_string(),
        ),
        activation_ticket_session_dir: Some(
            lectern_certificate_contract
                .activation_ticket_session_dir
                .display()
                .to_string(),
        ),
        review_receipt_session_dir: Some(
            lectern_certificate_contract.review_receipt_session_dir.display().to_string(),
        ),
        handoff_bundle_session_dir: Some(
            lectern_certificate_contract.handoff_bundle_session_dir.display().to_string(),
        ),
        decision_packet_session_dir: Some(
            lectern_certificate_contract
                .decision_packet_session_dir
                .display()
                .to_string(),
        ),
        execute_frozen_session_dir: Some(
            lectern_certificate_contract
                .execute_frozen_session_dir
                .display()
                .to_string(),
        ),
        turn_green_session_dir: Some(
            lectern_certificate_contract.turn_green_session_dir.display().to_string(),
        ),
        launch_packet_session_dir: Some(
            lectern_certificate_contract.launch_packet_session_dir.display().to_string(),
        ),
        package_dir: Some(lectern_certificate_contract.package_dir.display().to_string()),
        install_root: Some(lectern_certificate_contract.install_root.display().to_string()),
        target_service_name: Some(lectern_certificate_contract.target_service_name.clone()),
        backend_command: Some(lectern_certificate_contract.backend_command.clone()),
        wrapper_timeout_ms: Some(lectern_certificate_contract.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(lectern_certificate_contract.service_status_max_staleness_ms),
        session_dir: Some(session_dir.display().to_string()),
        apse_receipt_session_path: None,
        apse_receipt_status_path: None,
        archived_apse_receipt_report_path: None,
        result: Some(result),
        handoff_bundle_result: lectern_certificate_contract.handoff_bundle_result.clone(),
        decision_packet_result: lectern_certificate_contract.decision_packet_result.clone(),
        execute_frozen_result: lectern_certificate_contract.execute_frozen_result.clone(),
        current_pre_activation_gate_verdict: lectern_certificate_contract
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: lectern_certificate_contract
            .current_pre_activation_gate_reason
            .clone(),
        apse_receipt_step: None,
        verify_apse_receipt_command_summary: Some(verify_apse_receipt_command_summary(
            &config.apse_receipt_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            lectern_certificate_contract
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        provenance_certificate_summary: Some(lectern_certificate_contract.provenance_certificate_summary.clone()),
        chain_fingerprint_summary: Some(lectern_certificate_contract.chain_fingerprint_summary.clone()),
        chain_fingerprint_sha256: Some(lectern_certificate_contract.chain_fingerprint_sha256.clone()),
        chain_fingerprint_algorithm: Some(lectern_certificate_contract.chain_fingerprint_algorithm.clone()),
        release_capsule_digest_manifest_sha256: Some(
            lectern_certificate_contract.release_capsule_digest_manifest_sha256.clone(),
        ),
        release_capsule_digest_manifest_entry_count: Some(
            lectern_certificate_contract.release_capsule_digest_manifest_entry_count,
        ),
        release_capsule_digest_algorithm: Some(
            lectern_certificate_contract.release_capsule_digest_algorithm.clone(),
        ),
        notarization_receipt_summary: Some(lectern_certificate_contract.notarization_receipt_summary.clone()),
        ledger_seal_summary: Some(lectern_certificate_contract.ledger_seal_summary.clone()),
        ledger_seal_sha256: Some(lectern_certificate_contract.ledger_seal_sha256.clone()),
        ledger_seal_algorithm: Some(lectern_certificate_contract.ledger_seal_algorithm.clone()),
        registry_entry_summary: Some(lectern_certificate_contract.registry_entry_summary.clone()),
        registry_entry_sha256: Some(lectern_certificate_contract.registry_entry_sha256.clone()),
        registry_entry_algorithm: Some(lectern_certificate_contract.registry_entry_algorithm.clone()),
        filing_certificate_summary: Some(lectern_certificate_contract.filing_certificate_summary.clone()),
        filing_certificate_sha256: Some(lectern_certificate_contract.filing_certificate_sha256.clone()),
        filing_certificate_algorithm: Some(lectern_certificate_contract.filing_certificate_algorithm.clone()),
        archive_receipt_summary: Some(lectern_certificate_contract.archive_receipt_summary.clone()),
        archive_receipt_sha256: Some(lectern_certificate_contract.archive_receipt_sha256.clone()),
        archive_receipt_algorithm: Some(lectern_certificate_contract.archive_receipt_algorithm.clone()),
        closure_certificate_summary: Some(lectern_certificate_contract.closure_certificate_summary.clone()),
        closure_certificate_sha256: Some(lectern_certificate_contract.closure_certificate_sha256.clone()),
        closure_certificate_algorithm: Some(
            lectern_certificate_contract.closure_certificate_algorithm.clone(),
        ),
        finality_receipt_summary: Some(lectern_certificate_contract.finality_receipt_summary.clone()),
        finality_receipt_sha256: Some(lectern_certificate_contract.finality_receipt_sha256.clone()),
        finality_receipt_algorithm: Some(lectern_certificate_contract.finality_receipt_algorithm.clone()),
        consummation_record_summary: Some(lectern_certificate_contract.consummation_record_summary.clone()),
        consummation_record_sha256: Some(lectern_certificate_contract.consummation_record_sha256.clone()),
        consummation_record_algorithm: Some(lectern_certificate_contract.consummation_record_algorithm.clone()),
        completion_certificate_summary: Some(
            lectern_certificate_contract.completion_certificate_summary.clone(),
        ),
        completion_certificate_sha256: Some(
            lectern_certificate_contract.completion_certificate_sha256.clone(),
        ),
        completion_certificate_algorithm: Some(
            lectern_certificate_contract.completion_certificate_algorithm.clone(),
        ),
        summit_certificate_summary: Some(lectern_certificate_contract.summit_certificate_summary.clone()),
        summit_certificate_sha256: Some(lectern_certificate_contract.summit_certificate_sha256.clone()),
        summit_certificate_algorithm: Some(
            lectern_certificate_contract.summit_certificate_algorithm.clone(),
        ),
        culmination_receipt_summary: Some(
            lectern_certificate_contract.culmination_receipt_summary.clone(),
        ),
        culmination_receipt_sha256: Some(
            lectern_certificate_contract.culmination_receipt_sha256.clone(),
        ),
        culmination_receipt_algorithm: Some(
            lectern_certificate_contract.culmination_receipt_algorithm.clone(),
        ),
        pinnacle_receipt_summary: Some(lectern_certificate_contract.pinnacle_receipt_summary.clone()),
        pinnacle_receipt_sha256: Some(lectern_certificate_contract.pinnacle_receipt_sha256.clone()),
        pinnacle_receipt_algorithm: Some(
            lectern_certificate_contract.pinnacle_receipt_algorithm.clone(),
        ),
        capstone_certificate_summary: Some(
            lectern_certificate_contract.capstone_certificate_summary.clone(),
        ),
        capstone_certificate_sha256: Some(
            lectern_certificate_contract.capstone_certificate_sha256.clone(),
        ),
        capstone_certificate_algorithm: Some(
            lectern_certificate_contract.capstone_certificate_algorithm.clone(),
        ),
        keystone_receipt_summary: Some(lectern_certificate_contract.keystone_receipt_summary.clone()),
        keystone_receipt_sha256: Some(lectern_certificate_contract.keystone_receipt_sha256.clone()),
        keystone_receipt_algorithm: Some(
            lectern_certificate_contract.keystone_receipt_algorithm.clone(),
        ),
        cornerstone_certificate_summary: Some(lectern_certificate_contract.cornerstone_certificate_summary.clone()),
        cornerstone_certificate_sha256: Some(lectern_certificate_contract.cornerstone_certificate_sha256.clone()),
        cornerstone_certificate_algorithm: Some(lectern_certificate_contract.cornerstone_certificate_algorithm.clone()),
        foundation_receipt_summary: Some(lectern_certificate_contract.foundation_receipt_summary.clone()),
        foundation_receipt_sha256: Some(lectern_certificate_contract.foundation_receipt_sha256.clone()),
        foundation_receipt_algorithm: Some(lectern_certificate_contract.foundation_receipt_algorithm.clone()),
        bedrock_certificate_summary: Some(lectern_certificate_contract.bedrock_certificate_summary.clone()),
        bedrock_certificate_sha256: Some(lectern_certificate_contract.bedrock_certificate_sha256.clone()),
        bedrock_certificate_algorithm: Some(
            lectern_certificate_contract.bedrock_certificate_algorithm.clone(),
        ),
        basal_receipt_summary: Some(lectern_certificate_contract.basal_receipt_summary.clone()),
        basal_receipt_sha256: Some(lectern_certificate_contract.basal_receipt_sha256.clone()),
        basal_receipt_algorithm: Some(
            lectern_certificate_contract.basal_receipt_algorithm.clone(),
        ),
        substructure_certificate_summary: Some(
            lectern_certificate_contract.substructure_certificate_summary.clone(),
        ),
        substructure_certificate_sha256: Some(
            lectern_certificate_contract.substructure_certificate_sha256.clone(),
        ),
        substructure_certificate_algorithm: Some(
            lectern_certificate_contract.substructure_certificate_algorithm.clone(),
        ),
        plinth_receipt_summary: Some(lectern_certificate_contract.plinth_receipt_summary.clone()),
        plinth_receipt_sha256: Some(lectern_certificate_contract.plinth_receipt_sha256.clone()),
        plinth_receipt_algorithm: Some(lectern_certificate_contract.plinth_receipt_algorithm.clone()),
        pedestal_certificate_summary: Some(lectern_certificate_contract.pedestal_certificate_summary.clone()),
        pedestal_certificate_sha256: Some(lectern_certificate_contract.pedestal_certificate_sha256.clone()),
        pedestal_certificate_algorithm: Some(lectern_certificate_contract.pedestal_certificate_algorithm.clone()),
        dais_receipt_summary: Some(lectern_certificate_contract.dais_receipt_summary.clone()),
        dais_receipt_sha256: Some(lectern_certificate_contract.dais_receipt_sha256.clone()),
        dais_receipt_algorithm: Some(lectern_certificate_contract.dais_receipt_algorithm.clone()),
        rostrum_certificate_summary: Some(lectern_certificate_contract.rostrum_certificate_summary.clone()),
        rostrum_certificate_sha256: Some(lectern_certificate_contract.rostrum_certificate_sha256.clone()),
        rostrum_certificate_algorithm: Some(lectern_certificate_contract.rostrum_certificate_algorithm.clone()),
        podium_receipt_summary: Some(lectern_certificate_contract.podium_receipt_summary.clone()),
        podium_receipt_sha256: Some(lectern_certificate_contract.podium_receipt_sha256.clone()),
        podium_receipt_algorithm: Some(lectern_certificate_contract.podium_receipt_algorithm.clone()),
        lectern_certificate_summary: Some(lectern_certificate_contract.lectern_certificate_summary.clone()),
        lectern_certificate_sha256: Some(lectern_certificate_contract.lectern_certificate_sha256.clone()),
        lectern_certificate_algorithm: Some(lectern_certificate_contract.lectern_certificate_algorithm.clone()),
        pulpit_receipt_summary: Some(
            lectern_certificate_contract.pulpit_receipt_summary.clone(),
        ),
        pulpit_receipt_sha256: Some(
            lectern_certificate_contract.pulpit_receipt_sha256.clone(),
        ),
        pulpit_receipt_algorithm: Some(
            lectern_certificate_contract.pulpit_receipt_algorithm.clone(),
        ),
        chancel_certificate_summary: Some(
            apse_receipt_contract.chancel_certificate_summary.clone(),
        ),
        chancel_certificate_sha256: Some(
            apse_receipt_contract.chancel_certificate_sha256.clone(),
        ),
        chancel_certificate_algorithm: Some(
            apse_receipt_contract.chancel_certificate_algorithm.clone(),
        ),
        apse_receipt_summary: Some(
            apse_receipt_contract.apse_receipt_summary.clone(),
        ),
        apse_receipt_sha256: Some(
            apse_receipt_contract.apse_receipt_sha256.clone(),
        ),
        apse_receipt_algorithm: Some(
            apse_receipt_contract.apse_receipt_algorithm.clone(),
        ),
        sanctuary_certificate_summary: Some(sanctuary_certificate_summary(
            classification.0,
            &apse_receipt_contract.reviewed_frozen_live_cutover_controller_command_summary,
        )),
        sanctuary_certificate_sha256: Some(sanctuary_certificate_sha256),
        sanctuary_certificate_algorithm: Some(SANCTUARY_CERTIFICATE_ALGORITHM.to_string()),
        verification_mismatches: Vec::new(),
        explicit_statement: SANCTUARY_CERTIFICATE_SESSION_EXPLICIT_STATEMENT.to_string(),
    })
}

fn render_live_package_sanctuary_certificate_report(
    config: &Config,
) -> Result<PackageSanctuaryCertificateReport> {
    let plan = plan_live_package_sanctuary_certificate_report(config)?;
    let output_path = config
        .output_path
        .as_ref()
        .ok_or_else(|| anyhow!("missing --output for render mode"))?;
    let session_dir = PathBuf::from(
        plan.session_dir
            .clone()
            .ok_or_else(|| anyhow!("plan is missing session_dir for render"))?,
    );
    let confirmed_decision_packet_session_dir = PathBuf::from(
        plan.decision_packet_session_dir
            .clone()
            .ok_or_else(|| anyhow!("plan is missing decision_packet_session_dir for render"))?,
    );
    let script = format!(
        "#!/bin/sh\nset -eu\ncopybot_tiny_live_activation_package_sanctuary_certificate --apse-receipt-session-dir {} --confirm-decision-packet-session-dir {} --session-dir {} --run-live-package-sanctuary-certificate --json\ncopybot_tiny_live_activation_package_sanctuary_certificate --apse-receipt-session-dir {} --confirm-decision-packet-session-dir {} --session-dir {} --verify-live-package-sanctuary-certificate --json\n",
        shell_escape_path(&config.apse_receipt_session_dir),
        shell_escape_path(&confirmed_decision_packet_session_dir),
        shell_escape_path(&session_dir),
        shell_escape_path(&config.apse_receipt_session_dir),
        shell_escape_path(&confirmed_decision_packet_session_dir),
        shell_escape_path(&session_dir),
    );
    write_script(output_path, &script)?;
    Ok(PackageSanctuaryCertificateReport {
        generated_at: Utc::now(),
        mode: "render_live_package_sanctuary_certificate".to_string(),
        verdict:
            TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateRendered,
        reason: format!(
            "rendered sanctuary-certificate run/verify script to {}; the primary input remains the immutable apse-receipt session and run/verify still require the decision-packet confirmation anchor",
            output_path.display()
        ),
        explicit_statement: SANCTUARY_CERTIFICATE_SESSION_EXPLICIT_STATEMENT.to_string(),
        ..plan
    })
}

fn run_live_package_sanctuary_certificate_report(
    config: &Config,
) -> Result<PackageSanctuaryCertificateReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for run mode"))?;
    let verified_lectern_certificate = match package_apse_receipt_sanctuary_certificate::verify_live_package_apse_receipt_for_sanctuary_certificate(
        &config.apse_receipt_session_dir,
        config
            .confirmed_decision_packet_session_dir
            .as_deref()
            .ok_or_else(|| anyhow!("missing --confirm-decision-packet-session-dir for run mode"))?,
    ) {
        Ok(value) => value,
        Err(error) => {
            return Ok(failure_report_without_contract(
                config,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateRefusedNowByInvalidOrDriftedContract,
                format!(
                    "failed to verify apse-receipt session under {}: {error}",
                    config.apse_receipt_session_dir.display()
                ),
            ))
        }
    };

    validate_managed_surface_session_dir(
        &verified_lectern_certificate.contract.install_root,
        session_dir,
    )
    .with_context(|| {
        format!(
            "sanctuary-certificate session dir {} overlaps the managed live target surface derived from verified apse-receipt install_root {}",
            session_dir.display(),
            verified_lectern_certificate.contract.install_root.display()
        )
    })?;
    ensure_clean_sanctuary_certificate_session_dir(session_dir)?;
    let paths = sanctuary_certificate_paths(session_dir);
    let classification = classify_verified_apse_receipt(&verified_lectern_certificate);
    let result = classification.0;
    let sanctuary_certificate_sha256 = compute_sanctuary_certificate_sha256(
        &serialize_enum(&result),
        verified_lectern_certificate
            .contract
            .handoff_bundle_result
            .as_deref(),
        verified_lectern_certificate
            .contract
            .decision_packet_result
            .as_deref(),
        verified_lectern_certificate
            .contract
            .execute_frozen_result
            .as_deref(),
        verified_lectern_certificate
            .contract
            .current_pre_activation_gate_verdict
            .as_deref(),
        verified_lectern_certificate
            .contract
            .current_pre_activation_gate_reason
            .as_deref(),
        &verified_lectern_certificate
            .contract
            .reviewed_frozen_live_cutover_controller_command_summary,
        &verified_lectern_certificate
            .contract
            .chain_fingerprint_sha256,
        &verified_lectern_certificate
            .contract
            .chain_fingerprint_algorithm,
        &verified_lectern_certificate
            .contract
            .release_capsule_digest_manifest_sha256,
        verified_lectern_certificate
            .contract
            .release_capsule_digest_manifest_entry_count,
        &verified_lectern_certificate
            .contract
            .release_capsule_digest_algorithm,
        &verified_lectern_certificate.contract.ledger_seal_sha256,
        &verified_lectern_certificate.contract.ledger_seal_algorithm,
        &verified_lectern_certificate.contract.registry_entry_sha256,
        &verified_lectern_certificate
            .contract
            .registry_entry_algorithm,
        &verified_lectern_certificate
            .contract
            .filing_certificate_sha256,
        &verified_lectern_certificate
            .contract
            .filing_certificate_algorithm,
        &verified_lectern_certificate.contract.archive_receipt_sha256,
        &verified_lectern_certificate
            .contract
            .archive_receipt_algorithm,
        &verified_lectern_certificate
            .contract
            .closure_certificate_summary,
        &verified_lectern_certificate
            .contract
            .closure_certificate_sha256,
        &verified_lectern_certificate
            .contract
            .closure_certificate_algorithm,
        &verified_lectern_certificate
            .contract
            .finality_receipt_sha256,
        &verified_lectern_certificate
            .contract
            .finality_receipt_algorithm,
        &verified_lectern_certificate
            .contract
            .consummation_record_sha256,
        &verified_lectern_certificate
            .contract
            .consummation_record_algorithm,
        &verified_lectern_certificate
            .contract
            .completion_certificate_summary,
        &verified_lectern_certificate
            .contract
            .completion_certificate_sha256,
        &verified_lectern_certificate
            .contract
            .completion_certificate_algorithm,
        &verified_lectern_certificate
            .contract
            .summit_certificate_summary,
        &verified_lectern_certificate
            .contract
            .summit_certificate_sha256,
        &verified_lectern_certificate
            .contract
            .summit_certificate_algorithm,
        &verified_lectern_certificate
            .contract
            .culmination_receipt_summary,
        &verified_lectern_certificate
            .contract
            .culmination_receipt_sha256,
        &verified_lectern_certificate
            .contract
            .culmination_receipt_algorithm,
        &verified_lectern_certificate
            .contract
            .pinnacle_receipt_summary,
        &verified_lectern_certificate
            .contract
            .pinnacle_receipt_sha256,
        &verified_lectern_certificate
            .contract
            .pinnacle_receipt_algorithm,
        &verified_lectern_certificate
            .contract
            .capstone_certificate_summary,
        &verified_lectern_certificate
            .contract
            .capstone_certificate_sha256,
        &verified_lectern_certificate
            .contract
            .capstone_certificate_algorithm,
        &verified_lectern_certificate
            .contract
            .keystone_receipt_summary,
        &verified_lectern_certificate
            .contract
            .keystone_receipt_sha256,
        &verified_lectern_certificate
            .contract
            .keystone_receipt_algorithm,
        &verified_lectern_certificate
            .contract
            .cornerstone_certificate_summary,
        &verified_lectern_certificate
            .contract
            .cornerstone_certificate_sha256,
        &verified_lectern_certificate
            .contract
            .cornerstone_certificate_algorithm,
        &verified_lectern_certificate
            .contract
            .foundation_receipt_summary,
        &verified_lectern_certificate
            .contract
            .foundation_receipt_sha256,
        &verified_lectern_certificate
            .contract
            .foundation_receipt_algorithm,
        &verified_lectern_certificate
            .contract
            .bedrock_certificate_summary,
        &verified_lectern_certificate
            .contract
            .bedrock_certificate_sha256,
        &verified_lectern_certificate
            .contract
            .bedrock_certificate_algorithm,
        &verified_lectern_certificate.contract.basal_receipt_summary,
        &verified_lectern_certificate.contract.basal_receipt_sha256,
        &verified_lectern_certificate
            .contract
            .basal_receipt_algorithm,
        &verified_lectern_certificate
            .contract
            .substructure_certificate_summary,
        &verified_lectern_certificate
            .contract
            .substructure_certificate_sha256,
        &verified_lectern_certificate
            .contract
            .substructure_certificate_algorithm,
        &verified_lectern_certificate.contract.plinth_receipt_summary,
        &verified_lectern_certificate.contract.plinth_receipt_sha256,
        &verified_lectern_certificate
            .contract
            .plinth_receipt_algorithm,
        &verified_lectern_certificate
            .contract
            .pedestal_certificate_summary,
        &verified_lectern_certificate
            .contract
            .pedestal_certificate_sha256,
        &verified_lectern_certificate
            .contract
            .pedestal_certificate_algorithm,
        &verified_lectern_certificate.contract.dais_receipt_summary,
        &verified_lectern_certificate.contract.dais_receipt_sha256,
        &verified_lectern_certificate.contract.dais_receipt_algorithm,
        &verified_lectern_certificate
            .contract
            .rostrum_certificate_summary,
        &verified_lectern_certificate
            .contract
            .rostrum_certificate_sha256,
        &verified_lectern_certificate
            .contract
            .rostrum_certificate_algorithm,
        &verified_lectern_certificate.contract.podium_receipt_summary,
        &verified_lectern_certificate.contract.podium_receipt_sha256,
        &verified_lectern_certificate
            .contract
            .podium_receipt_algorithm,
        &verified_lectern_certificate
            .contract
            .lectern_certificate_summary,
        &verified_lectern_certificate
            .contract
            .lectern_certificate_sha256,
        &verified_lectern_certificate
            .contract
            .lectern_certificate_algorithm,
        &verified_lectern_certificate.contract.pulpit_receipt_summary,
        &verified_lectern_certificate.contract.pulpit_receipt_sha256,
        &verified_lectern_certificate
            .contract
            .pulpit_receipt_algorithm,
        &verified_lectern_certificate
            .contract
            .chancel_certificate_summary,
        &verified_lectern_certificate
            .contract
            .chancel_certificate_sha256,
        &verified_lectern_certificate
            .contract
            .chancel_certificate_algorithm,
        &verified_lectern_certificate.contract.apse_receipt_summary,
        &verified_lectern_certificate.contract.apse_receipt_sha256,
        &verified_lectern_certificate.contract.apse_receipt_algorithm,
    )?;
    let session = planned_session(
        config,
        session_dir,
        &verified_lectern_certificate.contract,
        result,
        &sanctuary_certificate_sha256,
    );
    persist_json(&paths.session_path, &session)?;
    persist_json(
        &paths.apse_receipt_report_path,
        &verified_lectern_certificate.report_json,
    )?;
    let apse_receipt_step = Some(step_artifact(
        &paths.apse_receipt_report_path,
        "verify_live_package_apse_receipt",
        &verified_lectern_certificate.verdict,
        &verified_lectern_certificate.reason,
        verified_lectern_certificate.generated_at,
    ));
    let status = PackageSanctuaryCertificateStatus {
        status_version: SANCTUARY_CERTIFICATE_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        session_dir: session_dir.display().to_string(),
        apse_receipt_session_dir: config.apse_receipt_session_dir.display().to_string(),
        lectern_certificate_session_dir: verified_lectern_certificate
            .contract
            .lectern_certificate_session_dir
            .display()
            .to_string(),
        dais_receipt_session_dir: verified_lectern_certificate
            .contract
            .dais_receipt_session_dir
            .display()
            .to_string(),
        pedestal_certificate_session_dir: verified_lectern_certificate
            .contract
            .pedestal_certificate_session_dir
            .display()
            .to_string(),
        plinth_receipt_session_dir: verified_lectern_certificate
            .contract
            .plinth_receipt_session_dir
            .display()
            .to_string(),
        substructure_certificate_session_dir: verified_lectern_certificate
            .contract
            .substructure_certificate_session_dir
            .display()
            .to_string(),
        basal_receipt_session_dir: verified_lectern_certificate
            .contract
            .basal_receipt_session_dir
            .display()
            .to_string(),
        bedrock_certificate_session_dir: verified_lectern_certificate
            .contract
            .bedrock_certificate_session_dir
            .display()
            .to_string(),
        foundation_receipt_session_dir: verified_lectern_certificate
            .contract
            .foundation_receipt_session_dir
            .display()
            .to_string(),
        result,
        reason: classification.2.clone(),
        handoff_bundle_result: verified_lectern_certificate
            .contract
            .handoff_bundle_result
            .clone(),
        decision_packet_result: verified_lectern_certificate
            .contract
            .decision_packet_result
            .clone(),
        execute_frozen_result: verified_lectern_certificate
            .contract
            .execute_frozen_result
            .clone(),
        current_pre_activation_gate_verdict: verified_lectern_certificate
            .contract
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: verified_lectern_certificate
            .contract
            .current_pre_activation_gate_reason
            .clone(),
        apse_receipt_step,
        provenance_certificate_summary: verified_lectern_certificate
            .contract
            .provenance_certificate_summary
            .clone(),
        chain_fingerprint_summary: verified_lectern_certificate
            .contract
            .chain_fingerprint_summary
            .clone(),
        chain_fingerprint_sha256: verified_lectern_certificate
            .contract
            .chain_fingerprint_sha256
            .clone(),
        chain_fingerprint_algorithm: verified_lectern_certificate
            .contract
            .chain_fingerprint_algorithm
            .clone(),
        release_capsule_digest_manifest_sha256: verified_lectern_certificate
            .contract
            .release_capsule_digest_manifest_sha256
            .clone(),
        release_capsule_digest_manifest_entry_count: verified_lectern_certificate
            .contract
            .release_capsule_digest_manifest_entry_count,
        release_capsule_digest_algorithm: verified_lectern_certificate
            .contract
            .release_capsule_digest_algorithm
            .clone(),
        notarization_receipt_summary: verified_lectern_certificate
            .contract
            .notarization_receipt_summary
            .clone(),
        ledger_seal_summary: verified_lectern_certificate
            .contract
            .ledger_seal_summary
            .clone(),
        ledger_seal_sha256: verified_lectern_certificate
            .contract
            .ledger_seal_sha256
            .clone(),
        ledger_seal_algorithm: verified_lectern_certificate
            .contract
            .ledger_seal_algorithm
            .clone(),
        registry_entry_summary: verified_lectern_certificate
            .contract
            .registry_entry_summary
            .clone(),
        registry_entry_sha256: verified_lectern_certificate
            .contract
            .registry_entry_sha256
            .clone(),
        registry_entry_algorithm: verified_lectern_certificate
            .contract
            .registry_entry_algorithm
            .clone(),
        filing_certificate_summary: verified_lectern_certificate
            .contract
            .filing_certificate_summary
            .clone(),
        filing_certificate_sha256: verified_lectern_certificate
            .contract
            .filing_certificate_sha256
            .clone(),
        filing_certificate_algorithm: verified_lectern_certificate
            .contract
            .filing_certificate_algorithm
            .clone(),
        archive_receipt_summary: verified_lectern_certificate
            .contract
            .archive_receipt_summary
            .clone(),
        archive_receipt_sha256: verified_lectern_certificate
            .contract
            .archive_receipt_sha256
            .clone(),
        archive_receipt_algorithm: verified_lectern_certificate
            .contract
            .archive_receipt_algorithm
            .clone(),
        closure_certificate_summary: verified_lectern_certificate
            .contract
            .closure_certificate_summary
            .clone(),
        closure_certificate_sha256: verified_lectern_certificate
            .contract
            .closure_certificate_sha256
            .clone(),
        closure_certificate_algorithm: verified_lectern_certificate
            .contract
            .closure_certificate_algorithm
            .clone(),
        finality_receipt_summary: verified_lectern_certificate
            .contract
            .finality_receipt_summary
            .clone(),
        finality_receipt_sha256: verified_lectern_certificate
            .contract
            .finality_receipt_sha256
            .clone(),
        finality_receipt_algorithm: verified_lectern_certificate
            .contract
            .finality_receipt_algorithm
            .clone(),
        consummation_record_summary: verified_lectern_certificate
            .contract
            .consummation_record_summary
            .clone(),
        consummation_record_sha256: verified_lectern_certificate
            .contract
            .consummation_record_sha256
            .clone(),
        consummation_record_algorithm: verified_lectern_certificate
            .contract
            .consummation_record_algorithm
            .clone(),
        completion_certificate_summary: verified_lectern_certificate
            .contract
            .completion_certificate_summary
            .clone(),
        completion_certificate_sha256: verified_lectern_certificate
            .contract
            .completion_certificate_sha256
            .clone(),
        completion_certificate_algorithm: verified_lectern_certificate
            .contract
            .completion_certificate_algorithm
            .clone(),
        summit_certificate_summary: verified_lectern_certificate
            .contract
            .summit_certificate_summary
            .clone(),
        summit_certificate_sha256: verified_lectern_certificate
            .contract
            .summit_certificate_sha256
            .clone(),
        summit_certificate_algorithm: verified_lectern_certificate
            .contract
            .summit_certificate_algorithm
            .clone(),
        culmination_receipt_summary: verified_lectern_certificate
            .contract
            .culmination_receipt_summary
            .clone(),
        culmination_receipt_sha256: verified_lectern_certificate
            .contract
            .culmination_receipt_sha256
            .clone(),
        culmination_receipt_algorithm: verified_lectern_certificate
            .contract
            .culmination_receipt_algorithm
            .clone(),
        pinnacle_receipt_summary: verified_lectern_certificate
            .contract
            .pinnacle_receipt_summary
            .clone(),
        pinnacle_receipt_sha256: verified_lectern_certificate
            .contract
            .pinnacle_receipt_sha256
            .clone(),
        pinnacle_receipt_algorithm: verified_lectern_certificate
            .contract
            .pinnacle_receipt_algorithm
            .clone(),
        capstone_certificate_summary: verified_lectern_certificate
            .contract
            .capstone_certificate_summary
            .clone(),
        capstone_certificate_sha256: verified_lectern_certificate
            .contract
            .capstone_certificate_sha256
            .clone(),
        capstone_certificate_algorithm: verified_lectern_certificate
            .contract
            .capstone_certificate_algorithm
            .clone(),
        keystone_receipt_summary: verified_lectern_certificate
            .contract
            .keystone_receipt_summary
            .clone(),
        keystone_receipt_sha256: verified_lectern_certificate
            .contract
            .keystone_receipt_sha256
            .clone(),
        keystone_receipt_algorithm: verified_lectern_certificate
            .contract
            .keystone_receipt_algorithm
            .clone(),
        cornerstone_certificate_summary: verified_lectern_certificate
            .contract
            .cornerstone_certificate_summary
            .clone(),
        cornerstone_certificate_sha256: verified_lectern_certificate
            .contract
            .cornerstone_certificate_sha256
            .clone(),
        cornerstone_certificate_algorithm: verified_lectern_certificate
            .contract
            .cornerstone_certificate_algorithm
            .clone(),
        foundation_receipt_summary: verified_lectern_certificate
            .contract
            .foundation_receipt_summary
            .clone(),
        foundation_receipt_sha256: verified_lectern_certificate
            .contract
            .foundation_receipt_sha256
            .clone(),
        foundation_receipt_algorithm: verified_lectern_certificate
            .contract
            .foundation_receipt_algorithm
            .clone(),
        bedrock_certificate_summary: verified_lectern_certificate
            .contract
            .bedrock_certificate_summary
            .clone(),
        bedrock_certificate_sha256: verified_lectern_certificate
            .contract
            .bedrock_certificate_sha256
            .clone(),
        bedrock_certificate_algorithm: verified_lectern_certificate
            .contract
            .bedrock_certificate_algorithm
            .clone(),
        basal_receipt_summary: verified_lectern_certificate
            .contract
            .basal_receipt_summary
            .clone(),
        basal_receipt_sha256: verified_lectern_certificate
            .contract
            .basal_receipt_sha256
            .clone(),
        basal_receipt_algorithm: verified_lectern_certificate
            .contract
            .basal_receipt_algorithm
            .clone(),
        substructure_certificate_summary: verified_lectern_certificate
            .contract
            .substructure_certificate_summary
            .clone(),
        substructure_certificate_sha256: verified_lectern_certificate
            .contract
            .substructure_certificate_sha256
            .clone(),
        substructure_certificate_algorithm: verified_lectern_certificate
            .contract
            .substructure_certificate_algorithm
            .clone(),
        plinth_receipt_summary: verified_lectern_certificate
            .contract
            .plinth_receipt_summary
            .clone(),
        plinth_receipt_sha256: verified_lectern_certificate
            .contract
            .plinth_receipt_sha256
            .clone(),
        plinth_receipt_algorithm: verified_lectern_certificate
            .contract
            .plinth_receipt_algorithm
            .clone(),
        pedestal_certificate_summary: verified_lectern_certificate
            .contract
            .pedestal_certificate_summary
            .clone(),
        pedestal_certificate_sha256: verified_lectern_certificate
            .contract
            .pedestal_certificate_sha256
            .clone(),
        pedestal_certificate_algorithm: verified_lectern_certificate
            .contract
            .pedestal_certificate_algorithm
            .clone(),
        dais_receipt_summary: verified_lectern_certificate
            .contract
            .dais_receipt_summary
            .clone(),
        dais_receipt_sha256: verified_lectern_certificate
            .contract
            .dais_receipt_sha256
            .clone(),
        dais_receipt_algorithm: verified_lectern_certificate
            .contract
            .dais_receipt_algorithm
            .clone(),
        rostrum_certificate_summary: verified_lectern_certificate
            .contract
            .rostrum_certificate_summary
            .clone(),
        rostrum_certificate_sha256: verified_lectern_certificate
            .contract
            .rostrum_certificate_sha256
            .clone(),
        rostrum_certificate_algorithm: verified_lectern_certificate
            .contract
            .rostrum_certificate_algorithm
            .clone(),
        podium_receipt_summary: verified_lectern_certificate
            .contract
            .podium_receipt_summary
            .clone(),
        podium_receipt_sha256: verified_lectern_certificate
            .contract
            .podium_receipt_sha256
            .clone(),
        podium_receipt_algorithm: verified_lectern_certificate
            .contract
            .podium_receipt_algorithm
            .clone(),
        lectern_certificate_summary: verified_lectern_certificate
            .contract
            .lectern_certificate_summary
            .clone(),
        lectern_certificate_sha256: verified_lectern_certificate
            .contract
            .lectern_certificate_sha256
            .clone(),
        lectern_certificate_algorithm: verified_lectern_certificate
            .contract
            .lectern_certificate_algorithm
            .clone(),
        pulpit_receipt_summary: verified_lectern_certificate
            .contract
            .pulpit_receipt_summary
            .clone(),
        pulpit_receipt_sha256: verified_lectern_certificate
            .contract
            .pulpit_receipt_sha256
            .clone(),
        pulpit_receipt_algorithm: verified_lectern_certificate
            .contract
            .pulpit_receipt_algorithm
            .clone(),
        chancel_certificate_summary: verified_lectern_certificate
            .contract
            .chancel_certificate_summary
            .clone(),
        chancel_certificate_sha256: verified_lectern_certificate
            .contract
            .chancel_certificate_sha256
            .clone(),
        chancel_certificate_algorithm: verified_lectern_certificate
            .contract
            .chancel_certificate_algorithm
            .clone(),
        apse_receipt_summary: verified_lectern_certificate
            .contract
            .apse_receipt_summary
            .clone(),
        apse_receipt_sha256: verified_lectern_certificate
            .contract
            .apse_receipt_sha256
            .clone(),
        apse_receipt_algorithm: verified_lectern_certificate
            .contract
            .apse_receipt_algorithm
            .clone(),
        sanctuary_certificate_summary: sanctuary_certificate_summary(
            result,
            &verified_lectern_certificate
                .contract
                .reviewed_frozen_live_cutover_controller_command_summary,
        ),
        sanctuary_certificate_sha256: sanctuary_certificate_sha256.clone(),
        sanctuary_certificate_algorithm: SANCTUARY_CERTIFICATE_ALGORITHM.to_string(),
        explicit_statement: SANCTUARY_CERTIFICATE_STATUS_EXPLICIT_STATEMENT.to_string(),
    };
    persist_json(&paths.status_path, &status)?;
    Ok(report_from_status(
        config,
        session_dir,
        &paths,
        &verified_lectern_certificate.contract,
        classification.1,
        &status,
    ))
}

fn verify_live_package_sanctuary_certificate_report(
    config: &Config,
) -> Result<PackageSanctuaryCertificateReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for verify mode"))?;
    let paths = sanctuary_certificate_paths(session_dir);
    let session: PackageSanctuaryCertificateSession = load_json(&paths.session_path)?;
    let status: PackageSanctuaryCertificateStatus = load_json(&paths.status_path)?;
    let mut mismatches = Vec::new();

    compare_string(
        &session.apse_receipt_session_dir,
        &config.apse_receipt_session_dir.display().to_string(),
        "apse-receipt apse_receipt_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "apse-receipt session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.session_dir,
        &session_dir.display().to_string(),
        "apse-receipt status session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.apse_receipt_session_dir,
        &config.apse_receipt_session_dir.display().to_string(),
        "apse-receipt status apse_receipt_session_dir",
        &mut mismatches,
    );

    let verified_lectern_certificate = match package_apse_receipt_sanctuary_certificate::verify_live_package_apse_receipt_for_sanctuary_certificate(
        &config.apse_receipt_session_dir,
        config
            .confirmed_decision_packet_session_dir
            .as_deref()
            .ok_or_else(|| anyhow!("missing --confirm-decision-packet-session-dir for verify mode"))?,
    ) {
        Ok(value) => value,
        Err(error) => {
            let raw_contract = package_apse_receipt_sanctuary_certificate::load_live_package_apse_receipt_contract_for_sanctuary_certificate(
                &config.apse_receipt_session_dir,
            )?;
            return Ok(failure_report_for_verify(
                config,
                session_dir,
                &raw_contract,
                vec![format!(
                    "failed to verify apse-receipt session under {}: {error}",
                    config.apse_receipt_session_dir.display()
                )],
            ));
        }
    };

    compare_string(
        &session.substructure_certificate_session_dir,
        &verified_lectern_certificate
            .contract
            .substructure_certificate_session_dir
            .display()
            .to_string(),
        "sanctuary-certificate substructure_certificate_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.substructure_certificate_session_dir,
        &verified_lectern_certificate
            .contract
            .substructure_certificate_session_dir
            .display()
            .to_string(),
        "sanctuary-certificate status substructure_certificate_session_dir",
        &mut mismatches,
    );

    compare_string(
        &session.bedrock_certificate_session_dir,
        &verified_lectern_certificate
            .contract
            .bedrock_certificate_session_dir
            .display()
            .to_string(),
        "sanctuary-certificate bedrock_certificate_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.bedrock_certificate_session_dir,
        &verified_lectern_certificate
            .contract
            .bedrock_certificate_session_dir
            .display()
            .to_string(),
        "sanctuary-certificate status bedrock_certificate_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.foundation_receipt_session_dir,
        &verified_lectern_certificate
            .contract
            .foundation_receipt_session_dir
            .display()
            .to_string(),
        "sanctuary-certificate foundation_receipt_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.foundation_receipt_session_dir,
        &verified_lectern_certificate
            .contract
            .foundation_receipt_session_dir
            .display()
            .to_string(),
        "sanctuary-certificate status foundation_receipt_session_dir",
        &mut mismatches,
    );

    let stored_lectern_certificate_report: serde_json::Value = load_required_step_json(
        &status.apse_receipt_step,
        &paths.apse_receipt_report_path,
        "apse_receipt_step",
        &mut mismatches,
    )?;
    let stored_lectern_certificate_report_view: NestedPulpitReceiptReportView =
        serde_json::from_value(stored_lectern_certificate_report.clone()).with_context(|| {
            format!(
                "failed parsing archived nested apse-receipt report {}",
                paths.apse_receipt_report_path.display()
            )
        })?;
    let verified_lectern_certificate_report_view: NestedPulpitReceiptReportView =
        serde_json::from_value(verified_lectern_certificate.report_json.clone()).with_context(
            || {
                format!(
                    "failed parsing freshly verified nested apse-receipt report for {}",
                    config.apse_receipt_session_dir.display()
                )
            },
        )?;
    compare_json_ignoring_generated_at(
        &stored_lectern_certificate_report,
        &verified_lectern_certificate.report_json,
        "sanctuary-certificate archived apse-receipt report json",
        &mut mismatches,
    );
    compare_string(
        &stored_lectern_certificate_report_view.mode,
        &verified_lectern_certificate_report_view.mode,
        "sanctuary-certificate archived apse-receipt report mode",
        &mut mismatches,
    );
    compare_string(
        &stored_lectern_certificate_report_view.verdict,
        &verified_lectern_certificate_report_view.verdict,
        "sanctuary-certificate archived apse-receipt report verdict",
        &mut mismatches,
    );
    compare_string(
        &stored_lectern_certificate_report_view.reason,
        &verified_lectern_certificate_report_view.reason,
        "sanctuary-certificate archived apse-receipt report reason",
        &mut mismatches,
    );
    if stored_lectern_certificate_report_view.generated_at
        != verified_lectern_certificate_report_view.generated_at
    {
        mismatches.push(format!(
            "sanctuary-certificate archived apse-receipt report generated_at {:?} does not match freshly verified nested truth {:?}",
            stored_lectern_certificate_report_view.generated_at,
            verified_lectern_certificate_report_view.generated_at
        ));
    }
    let expected_apse_receipt_step = step_artifact(
        &paths.apse_receipt_report_path,
        &verified_lectern_certificate_report_view.mode,
        &verified_lectern_certificate_report_view.verdict,
        &verified_lectern_certificate_report_view.reason,
        verified_lectern_certificate_report_view.generated_at,
    );
    compare_step_artifact(
        status.apse_receipt_step.as_ref(),
        &expected_apse_receipt_step,
        "sanctuary-certificate apse_receipt_step",
        &mut mismatches,
    );
    if verified_lectern_certificate.verdict != "tiny_live_package_apse_receipt_verify_ok" {
        mismatches.push(format!(
            "sanctuary-certificate nested apse-receipt verification is non-green: {}",
            verified_lectern_certificate.reason
        ));
    }

    let classification = classify_verified_apse_receipt(&verified_lectern_certificate);
    let expected_sanctuary_certificate_sha256 = compute_sanctuary_certificate_sha256(
        &serialize_enum(&classification.0),
        verified_lectern_certificate
            .contract
            .handoff_bundle_result
            .as_deref(),
        verified_lectern_certificate
            .contract
            .decision_packet_result
            .as_deref(),
        verified_lectern_certificate
            .contract
            .execute_frozen_result
            .as_deref(),
        verified_lectern_certificate
            .contract
            .current_pre_activation_gate_verdict
            .as_deref(),
        verified_lectern_certificate
            .contract
            .current_pre_activation_gate_reason
            .as_deref(),
        &verified_lectern_certificate
            .contract
            .reviewed_frozen_live_cutover_controller_command_summary,
        &verified_lectern_certificate
            .contract
            .chain_fingerprint_sha256,
        &verified_lectern_certificate
            .contract
            .chain_fingerprint_algorithm,
        &verified_lectern_certificate
            .contract
            .release_capsule_digest_manifest_sha256,
        verified_lectern_certificate
            .contract
            .release_capsule_digest_manifest_entry_count,
        &verified_lectern_certificate
            .contract
            .release_capsule_digest_algorithm,
        &verified_lectern_certificate.contract.ledger_seal_sha256,
        &verified_lectern_certificate.contract.ledger_seal_algorithm,
        &verified_lectern_certificate.contract.registry_entry_sha256,
        &verified_lectern_certificate
            .contract
            .registry_entry_algorithm,
        &verified_lectern_certificate
            .contract
            .filing_certificate_sha256,
        &verified_lectern_certificate
            .contract
            .filing_certificate_algorithm,
        &verified_lectern_certificate.contract.archive_receipt_sha256,
        &verified_lectern_certificate
            .contract
            .archive_receipt_algorithm,
        &verified_lectern_certificate
            .contract
            .closure_certificate_summary,
        &verified_lectern_certificate
            .contract
            .closure_certificate_sha256,
        &verified_lectern_certificate
            .contract
            .closure_certificate_algorithm,
        &verified_lectern_certificate
            .contract
            .finality_receipt_sha256,
        &verified_lectern_certificate
            .contract
            .finality_receipt_algorithm,
        &verified_lectern_certificate
            .contract
            .consummation_record_sha256,
        &verified_lectern_certificate
            .contract
            .consummation_record_algorithm,
        &verified_lectern_certificate
            .contract
            .completion_certificate_summary,
        &verified_lectern_certificate
            .contract
            .completion_certificate_sha256,
        &verified_lectern_certificate
            .contract
            .completion_certificate_algorithm,
        &verified_lectern_certificate
            .contract
            .summit_certificate_summary,
        &verified_lectern_certificate
            .contract
            .summit_certificate_sha256,
        &verified_lectern_certificate
            .contract
            .summit_certificate_algorithm,
        &verified_lectern_certificate
            .contract
            .culmination_receipt_summary,
        &verified_lectern_certificate
            .contract
            .culmination_receipt_sha256,
        &verified_lectern_certificate
            .contract
            .culmination_receipt_algorithm,
        &verified_lectern_certificate
            .contract
            .pinnacle_receipt_summary,
        &verified_lectern_certificate
            .contract
            .pinnacle_receipt_sha256,
        &verified_lectern_certificate
            .contract
            .pinnacle_receipt_algorithm,
        &verified_lectern_certificate
            .contract
            .capstone_certificate_summary,
        &verified_lectern_certificate
            .contract
            .capstone_certificate_sha256,
        &verified_lectern_certificate
            .contract
            .capstone_certificate_algorithm,
        &verified_lectern_certificate
            .contract
            .keystone_receipt_summary,
        &verified_lectern_certificate
            .contract
            .keystone_receipt_sha256,
        &verified_lectern_certificate
            .contract
            .keystone_receipt_algorithm,
        &verified_lectern_certificate
            .contract
            .cornerstone_certificate_summary,
        &verified_lectern_certificate
            .contract
            .cornerstone_certificate_sha256,
        &verified_lectern_certificate
            .contract
            .cornerstone_certificate_algorithm,
        &verified_lectern_certificate
            .contract
            .foundation_receipt_summary,
        &verified_lectern_certificate
            .contract
            .foundation_receipt_sha256,
        &verified_lectern_certificate
            .contract
            .foundation_receipt_algorithm,
        &verified_lectern_certificate
            .contract
            .bedrock_certificate_summary,
        &verified_lectern_certificate
            .contract
            .bedrock_certificate_sha256,
        &verified_lectern_certificate
            .contract
            .bedrock_certificate_algorithm,
        &verified_lectern_certificate.contract.basal_receipt_summary,
        &verified_lectern_certificate.contract.basal_receipt_sha256,
        &verified_lectern_certificate
            .contract
            .basal_receipt_algorithm,
        &verified_lectern_certificate
            .contract
            .substructure_certificate_summary,
        &verified_lectern_certificate
            .contract
            .substructure_certificate_sha256,
        &verified_lectern_certificate
            .contract
            .substructure_certificate_algorithm,
        &verified_lectern_certificate.contract.plinth_receipt_summary,
        &verified_lectern_certificate.contract.plinth_receipt_sha256,
        &verified_lectern_certificate
            .contract
            .plinth_receipt_algorithm,
        &verified_lectern_certificate
            .contract
            .pedestal_certificate_summary,
        &verified_lectern_certificate
            .contract
            .pedestal_certificate_sha256,
        &verified_lectern_certificate
            .contract
            .pedestal_certificate_algorithm,
        &verified_lectern_certificate.contract.dais_receipt_summary,
        &verified_lectern_certificate.contract.dais_receipt_sha256,
        &verified_lectern_certificate.contract.dais_receipt_algorithm,
        &verified_lectern_certificate
            .contract
            .rostrum_certificate_summary,
        &verified_lectern_certificate
            .contract
            .rostrum_certificate_sha256,
        &verified_lectern_certificate
            .contract
            .rostrum_certificate_algorithm,
        &verified_lectern_certificate.contract.podium_receipt_summary,
        &verified_lectern_certificate.contract.podium_receipt_sha256,
        &verified_lectern_certificate
            .contract
            .podium_receipt_algorithm,
        &verified_lectern_certificate
            .contract
            .lectern_certificate_summary,
        &verified_lectern_certificate
            .contract
            .lectern_certificate_sha256,
        &verified_lectern_certificate
            .contract
            .lectern_certificate_algorithm,
        &verified_lectern_certificate.contract.pulpit_receipt_summary,
        &verified_lectern_certificate.contract.pulpit_receipt_sha256,
        &verified_lectern_certificate
            .contract
            .pulpit_receipt_algorithm,
        &verified_lectern_certificate
            .contract
            .chancel_certificate_summary,
        &verified_lectern_certificate
            .contract
            .chancel_certificate_sha256,
        &verified_lectern_certificate
            .contract
            .chancel_certificate_algorithm,
        &verified_lectern_certificate.contract.apse_receipt_summary,
        &verified_lectern_certificate.contract.apse_receipt_sha256,
        &verified_lectern_certificate.contract.apse_receipt_algorithm,
    )?;

    let expected_session = expected_session_from_verified(
        config,
        session_dir,
        session.planned_at,
        &verified_lectern_certificate.contract,
        classification.0,
        &expected_sanctuary_certificate_sha256,
    );
    if session != expected_session {
        mismatches.push(
            "sanctuary-certificate session contract does not match freshly verified apse-receipt truth"
                .to_string(),
        );
    }
    let expected_status = expected_status_from_verified(
        config,
        session_dir,
        status.updated_at,
        &verified_lectern_certificate.contract,
        classification,
        expected_apse_receipt_step.clone(),
        &expected_sanctuary_certificate_sha256,
    );
    if status != expected_status {
        mismatches.push(
            "sanctuary-certificate status contract does not match freshly verified apse-receipt truth"
                .to_string(),
        );
    }

    if !mismatches.is_empty() {
        return Ok(failure_report_for_verify(
            config,
            session_dir,
            &verified_lectern_certificate.contract,
            mismatches,
        ));
    }

    Ok(report_from_status(
        config,
        session_dir,
        &paths,
        &verified_lectern_certificate.contract,
        TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyOk,
        &expected_status,
    ))
}

fn verify_apse_receipt_command_summary(apse_receipt_session_dir: &Path) -> String {
    format!(
        "verify immutable apse-receipt session under {} before freezing the final sanctuary certificate",
        apse_receipt_session_dir.display()
    )
}

fn classify_apse_receipt_contract(
    contract: &package_apse_receipt_sanctuary_certificate::LivePackageApseReceiptContractView,
    assume_verified: bool,
) -> (
    LivePackageSanctuaryCertificateResult,
    TinyLivePackageSanctuaryCertificateVerdict,
    String,
) {
    if !assume_verified {
        return (
            LivePackageSanctuaryCertificateResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateRefusedNowByInvalidOrDriftedContract,
            "the apse-receipt chain is not verified, so this sanctuary certificate remains refused"
                .to_string(),
        );
    }
    match contract.result.as_deref() {
        Some("refused_now_by_stage3") => (
            LivePackageSanctuaryCertificateResult::RefusedNowByStage3,
            TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateRefusedNowByStage3,
            "the immutable apse receipt remains refused now by Stage 3, so this sanctuary certificate remains an explicit refusal record"
                .to_string(),
        ),
        Some("refused_now_by_pre_activation_gate") => (
            LivePackageSanctuaryCertificateResult::RefusedNowByPreActivationGate,
            TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateRefusedNowByPreActivationGate,
            "the immutable apse receipt remains refused now by the pre-activation gate, so this sanctuary certificate remains an explicit refusal record"
                .to_string(),
        ),
        Some("ready_for_manual_execution_when_gate_turns_green") => (
            LivePackageSanctuaryCertificateResult::ReadyForManualExecutionWhenGateTurnsGreen,
            TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateReadyForManualExecutionWhenGateTurnsGreen,
            "the immutable apse receipt is coherent and this sanctuary certificate is ready for manual execution when gate truth turns green"
                .to_string(),
        ),
        _ => (
            LivePackageSanctuaryCertificateResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateRefusedNowByInvalidOrDriftedContract,
            "the verified apse-receipt chain is invalid, drifted, or non-runnable, so this sanctuary certificate remains refused"
                .to_string(),
        ),
    }
}

fn classify_verified_apse_receipt(
    verified: &package_apse_receipt_sanctuary_certificate::VerifiedLivePackageApseReceiptSanctuaryCertificateStep,
) -> (
    LivePackageSanctuaryCertificateResult,
    TinyLivePackageSanctuaryCertificateVerdict,
    String,
) {
    if verified.verdict != "tiny_live_package_apse_receipt_verify_ok" {
        return (
            LivePackageSanctuaryCertificateResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateRefusedNowByInvalidOrDriftedContract,
            "the immutable apse receipt did not verify cleanly, so this sanctuary certificate remains refused"
                .to_string(),
        );
    }
    classify_apse_receipt_contract(&verified.contract, true)
}

fn sanctuary_certificate_summary(
    result: LivePackageSanctuaryCertificateResult,
    reviewed_frozen_live_cutover_controller_command_summary: &str,
) -> String {
    match result {
        LivePackageSanctuaryCertificateResult::RefusedNowByStage3 => {
            "Apse receipt outcome: the immutable culminated chain remains refused now by Stage 3; archive this final standard seal as refusal evidence only and do not run the frozen controller.".to_string()
        }
        LivePackageSanctuaryCertificateResult::RefusedNowByPreActivationGate => {
            "Apse receipt outcome: the immutable culminated chain remains refused now by the pre-activation gate; archive this final standard seal as refusal evidence only and do not run the frozen controller.".to_string()
        }
        LivePackageSanctuaryCertificateResult::RefusedNowByInvalidOrDriftedContract => {
            "Apse receipt outcome: the immutable culminated chain is invalid or drifted; archive this final standard seal for audit only and mint a new coherent receipt before any later execution handoff.".to_string()
        }
        LivePackageSanctuaryCertificateResult::ReadyForManualExecutionWhenGateTurnsGreen => format!(
            "Apse receipt outcome: this immutable standard seal is ready for manual execution when gate truth turns green; the exact reviewed frozen controller command remains: {reviewed_frozen_live_cutover_controller_command_summary}"
        ),
    }
}

fn sanctuary_certificate_paths(session_dir: &Path) -> PackageSanctuaryCertificatePaths {
    PackageSanctuaryCertificatePaths {
        session_path: session_dir
            .join("tiny_live_activation_package_sanctuary_certificate.session.json"),
        status_path: session_dir
            .join("tiny_live_activation_package_sanctuary_certificate.status.json"),
        apse_receipt_report_path: session_dir
            .join("tiny_live_activation_package_sanctuary_certificate.apse_receipt.report.json"),
    }
}

fn ensure_clean_sanctuary_certificate_session_dir(session_dir: &Path) -> Result<()> {
    if session_dir.exists() {
        bail!(
            "refusing to overwrite existing sanctuary-certificate session dir {}",
            session_dir.display()
        );
    }
    fs::create_dir_all(session_dir).with_context(|| {
        format!(
            "failed creating sanctuary-certificate session dir {}",
            session_dir.display()
        )
    })
}

fn planned_session(
    config: &Config,
    session_dir: &Path,
    contract: &package_apse_receipt_sanctuary_certificate::LivePackageApseReceiptContractView,
    result: LivePackageSanctuaryCertificateResult,
    sanctuary_certificate_sha256: &str,
) -> PackageSanctuaryCertificateSession {
    PackageSanctuaryCertificateSession {
        session_version: SANCTUARY_CERTIFICATE_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        apse_receipt_session_dir: config.apse_receipt_session_dir.display().to_string(),
        lectern_certificate_session_dir: contract
            .lectern_certificate_session_dir
            .display()
            .to_string(),
        dais_receipt_session_dir: contract.dais_receipt_session_dir.display().to_string(),
        pedestal_certificate_session_dir: contract
            .pedestal_certificate_session_dir
            .display()
            .to_string(),
        plinth_receipt_session_dir: contract.plinth_receipt_session_dir.display().to_string(),
        substructure_certificate_session_dir: contract
            .substructure_certificate_session_dir
            .display()
            .to_string(),
        basal_receipt_session_dir: contract.basal_receipt_session_dir.display().to_string(),
        bedrock_certificate_session_dir: contract
            .bedrock_certificate_session_dir
            .display()
            .to_string(),
        foundation_receipt_session_dir: contract
            .foundation_receipt_session_dir
            .display()
            .to_string(),
        registry_entry_session_dir: contract.registry_entry_session_dir.display().to_string(),
        notarization_receipt_session_dir: contract
            .notarization_receipt_session_dir
            .display()
            .to_string(),
        provenance_certificate_session_dir: contract
            .provenance_certificate_session_dir
            .display()
            .to_string(),
        attestation_seal_session_dir: contract.attestation_seal_session_dir.display().to_string(),
        release_capsule_session_dir: contract.release_capsule_session_dir.display().to_string(),
        activation_ticket_session_dir: contract.activation_ticket_session_dir.display().to_string(),
        review_receipt_session_dir: contract.review_receipt_session_dir.display().to_string(),
        handoff_bundle_session_dir: contract.handoff_bundle_session_dir.display().to_string(),
        decision_packet_session_dir: contract.decision_packet_session_dir.display().to_string(),
        execute_frozen_session_dir: contract.execute_frozen_session_dir.display().to_string(),
        turn_green_session_dir: contract.turn_green_session_dir.display().to_string(),
        launch_packet_session_dir: contract.launch_packet_session_dir.display().to_string(),
        package_dir: contract.package_dir.display().to_string(),
        install_root: contract.install_root.display().to_string(),
        target_service_name: contract.target_service_name.clone(),
        backend_command: contract.backend_command.clone(),
        wrapper_timeout_ms: contract.wrapper_timeout_ms,
        service_status_max_staleness_ms: contract.service_status_max_staleness_ms,
        session_dir: session_dir.display().to_string(),
        verify_apse_receipt_command_summary: verify_apse_receipt_command_summary(
            &config.apse_receipt_session_dir,
        ),
        reviewed_frozen_live_cutover_controller_command_summary: contract
            .reviewed_frozen_live_cutover_controller_command_summary
            .clone(),
        provenance_certificate_summary: contract.provenance_certificate_summary.clone(),
        chain_fingerprint_summary: contract.chain_fingerprint_summary.clone(),
        chain_fingerprint_sha256: contract.chain_fingerprint_sha256.clone(),
        chain_fingerprint_algorithm: contract.chain_fingerprint_algorithm.clone(),
        release_capsule_digest_manifest_sha256: contract
            .release_capsule_digest_manifest_sha256
            .clone(),
        release_capsule_digest_manifest_entry_count: contract
            .release_capsule_digest_manifest_entry_count,
        release_capsule_digest_algorithm: contract.release_capsule_digest_algorithm.clone(),
        notarization_receipt_summary: contract.notarization_receipt_summary.clone(),
        ledger_seal_summary: contract.ledger_seal_summary.clone(),
        ledger_seal_sha256: contract.ledger_seal_sha256.clone(),
        ledger_seal_algorithm: contract.ledger_seal_algorithm.clone(),
        registry_entry_summary: contract.registry_entry_summary.clone(),
        registry_entry_sha256: contract.registry_entry_sha256.clone(),
        registry_entry_algorithm: contract.registry_entry_algorithm.clone(),
        filing_certificate_summary: contract.filing_certificate_summary.clone(),
        filing_certificate_sha256: contract.filing_certificate_sha256.clone(),
        filing_certificate_algorithm: contract.filing_certificate_algorithm.clone(),
        archive_receipt_summary: contract.archive_receipt_summary.clone(),
        archive_receipt_sha256: contract.archive_receipt_sha256.clone(),
        archive_receipt_algorithm: contract.archive_receipt_algorithm.clone(),
        closure_certificate_summary: contract.closure_certificate_summary.clone(),
        closure_certificate_sha256: contract.closure_certificate_sha256.clone(),
        closure_certificate_algorithm: contract.closure_certificate_algorithm.clone(),
        finality_receipt_summary: contract.finality_receipt_summary.clone(),
        finality_receipt_sha256: contract.finality_receipt_sha256.clone(),
        finality_receipt_algorithm: contract.finality_receipt_algorithm.clone(),
        consummation_record_summary: contract.consummation_record_summary.clone(),
        consummation_record_sha256: contract.consummation_record_sha256.clone(),
        consummation_record_algorithm: contract.consummation_record_algorithm.clone(),
        completion_certificate_summary: contract.completion_certificate_summary.clone(),
        completion_certificate_sha256: contract.completion_certificate_sha256.clone(),
        completion_certificate_algorithm: contract.completion_certificate_algorithm.clone(),
        summit_certificate_summary: contract.summit_certificate_summary.clone(),
        summit_certificate_sha256: contract.summit_certificate_sha256.clone(),
        summit_certificate_algorithm: contract.summit_certificate_algorithm.clone(),
        culmination_receipt_summary: contract.culmination_receipt_summary.clone(),
        culmination_receipt_sha256: contract.culmination_receipt_sha256.clone(),
        culmination_receipt_algorithm: contract.culmination_receipt_algorithm.clone(),
        pinnacle_receipt_summary: contract.pinnacle_receipt_summary.clone(),
        pinnacle_receipt_sha256: contract.pinnacle_receipt_sha256.clone(),
        pinnacle_receipt_algorithm: contract.pinnacle_receipt_algorithm.clone(),
        capstone_certificate_summary: contract.capstone_certificate_summary.clone(),
        capstone_certificate_sha256: contract.capstone_certificate_sha256.clone(),
        capstone_certificate_algorithm: contract.capstone_certificate_algorithm.clone(),
        keystone_receipt_summary: contract.keystone_receipt_summary.clone(),
        keystone_receipt_sha256: contract.keystone_receipt_sha256.clone(),
        keystone_receipt_algorithm: contract.keystone_receipt_algorithm.clone(),
        cornerstone_certificate_summary: contract.cornerstone_certificate_summary.clone(),
        cornerstone_certificate_sha256: contract.cornerstone_certificate_sha256.clone(),
        cornerstone_certificate_algorithm: contract.cornerstone_certificate_algorithm.clone(),
        foundation_receipt_summary: contract.foundation_receipt_summary.clone(),
        foundation_receipt_sha256: contract.foundation_receipt_sha256.clone(),
        foundation_receipt_algorithm: contract.foundation_receipt_algorithm.clone(),
        bedrock_certificate_summary: contract.bedrock_certificate_summary.clone(),
        bedrock_certificate_sha256: contract.bedrock_certificate_sha256.clone(),
        bedrock_certificate_algorithm: contract.bedrock_certificate_algorithm.clone(),
        basal_receipt_summary: contract.basal_receipt_summary.clone(),
        basal_receipt_sha256: contract.basal_receipt_sha256.clone(),
        basal_receipt_algorithm: contract.basal_receipt_algorithm.clone(),
        substructure_certificate_summary: contract.substructure_certificate_summary.clone(),
        substructure_certificate_sha256: contract.substructure_certificate_sha256.clone(),
        substructure_certificate_algorithm: contract.substructure_certificate_algorithm.clone(),
        plinth_receipt_summary: contract.plinth_receipt_summary.clone(),
        plinth_receipt_sha256: contract.plinth_receipt_sha256.clone(),
        plinth_receipt_algorithm: contract.plinth_receipt_algorithm.clone(),
        pedestal_certificate_summary: contract.pedestal_certificate_summary.clone(),
        pedestal_certificate_sha256: contract.pedestal_certificate_sha256.clone(),
        pedestal_certificate_algorithm: contract.pedestal_certificate_algorithm.clone(),
        dais_receipt_summary: contract.dais_receipt_summary.clone(),
        dais_receipt_sha256: contract.dais_receipt_sha256.clone(),
        dais_receipt_algorithm: contract.dais_receipt_algorithm.clone(),
        rostrum_certificate_summary: contract.rostrum_certificate_summary.clone(),
        rostrum_certificate_sha256: contract.rostrum_certificate_sha256.clone(),
        rostrum_certificate_algorithm: contract.rostrum_certificate_algorithm.clone(),
        podium_receipt_summary: contract.podium_receipt_summary.clone(),
        podium_receipt_sha256: contract.podium_receipt_sha256.clone(),
        podium_receipt_algorithm: contract.podium_receipt_algorithm.clone(),
        lectern_certificate_summary: contract.lectern_certificate_summary.clone(),
        lectern_certificate_sha256: contract.lectern_certificate_sha256.clone(),
        lectern_certificate_algorithm: contract.lectern_certificate_algorithm.clone(),
        pulpit_receipt_summary: contract.pulpit_receipt_summary.clone(),
        pulpit_receipt_sha256: contract.pulpit_receipt_sha256.clone(),
        pulpit_receipt_algorithm: contract.pulpit_receipt_algorithm.clone(),
        chancel_certificate_summary: contract.chancel_certificate_summary.clone(),
        chancel_certificate_sha256: contract.chancel_certificate_sha256.clone(),
        chancel_certificate_algorithm: contract.chancel_certificate_algorithm.clone(),
        apse_receipt_summary: contract.apse_receipt_summary.clone(),
        apse_receipt_sha256: contract.apse_receipt_sha256.clone(),
        apse_receipt_algorithm: contract.apse_receipt_algorithm.clone(),
        sanctuary_certificate_summary: sanctuary_certificate_summary(
            result,
            &contract.reviewed_frozen_live_cutover_controller_command_summary,
        ),
        sanctuary_certificate_sha256: sanctuary_certificate_sha256.to_string(),
        sanctuary_certificate_algorithm: SANCTUARY_CERTIFICATE_ALGORITHM.to_string(),
        explicit_statement: SANCTUARY_CERTIFICATE_SESSION_EXPLICIT_STATEMENT.to_string(),
    }
}

fn expected_session_from_verified(
    config: &Config,
    session_dir: &Path,
    planned_at: DateTime<Utc>,
    contract: &package_apse_receipt_sanctuary_certificate::LivePackageApseReceiptContractView,
    result: LivePackageSanctuaryCertificateResult,
    sanctuary_certificate_sha256: &str,
) -> PackageSanctuaryCertificateSession {
    PackageSanctuaryCertificateSession {
        planned_at,
        ..planned_session(
            config,
            session_dir,
            contract,
            result,
            sanctuary_certificate_sha256,
        )
    }
}

fn expected_status_from_verified(
    config: &Config,
    session_dir: &Path,
    updated_at: DateTime<Utc>,
    contract: &package_apse_receipt_sanctuary_certificate::LivePackageApseReceiptContractView,
    classification: (
        LivePackageSanctuaryCertificateResult,
        TinyLivePackageSanctuaryCertificateVerdict,
        String,
    ),
    apse_receipt_step: PackageSanctuaryCertificateStepArtifact,
    sanctuary_certificate_sha256: &str,
) -> PackageSanctuaryCertificateStatus {
    PackageSanctuaryCertificateStatus {
        status_version: SANCTUARY_CERTIFICATE_STATUS_VERSION.to_string(),
        updated_at,
        session_dir: session_dir.display().to_string(),
        apse_receipt_session_dir: config.apse_receipt_session_dir.display().to_string(),
        lectern_certificate_session_dir: contract
            .lectern_certificate_session_dir
            .display()
            .to_string(),
        dais_receipt_session_dir: contract.dais_receipt_session_dir.display().to_string(),
        pedestal_certificate_session_dir: contract
            .pedestal_certificate_session_dir
            .display()
            .to_string(),
        plinth_receipt_session_dir: contract.plinth_receipt_session_dir.display().to_string(),
        substructure_certificate_session_dir: contract
            .substructure_certificate_session_dir
            .display()
            .to_string(),
        basal_receipt_session_dir: contract.basal_receipt_session_dir.display().to_string(),
        bedrock_certificate_session_dir: contract
            .bedrock_certificate_session_dir
            .display()
            .to_string(),
        foundation_receipt_session_dir: contract
            .foundation_receipt_session_dir
            .display()
            .to_string(),
        result: classification.0,
        reason: classification.2,
        handoff_bundle_result: contract.handoff_bundle_result.clone(),
        decision_packet_result: contract.decision_packet_result.clone(),
        execute_frozen_result: contract.execute_frozen_result.clone(),
        current_pre_activation_gate_verdict: contract.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: contract.current_pre_activation_gate_reason.clone(),
        apse_receipt_step: Some(apse_receipt_step),
        provenance_certificate_summary: contract.provenance_certificate_summary.clone(),
        chain_fingerprint_summary: contract.chain_fingerprint_summary.clone(),
        chain_fingerprint_sha256: contract.chain_fingerprint_sha256.clone(),
        chain_fingerprint_algorithm: contract.chain_fingerprint_algorithm.clone(),
        release_capsule_digest_manifest_sha256: contract
            .release_capsule_digest_manifest_sha256
            .clone(),
        release_capsule_digest_manifest_entry_count: contract
            .release_capsule_digest_manifest_entry_count,
        release_capsule_digest_algorithm: contract.release_capsule_digest_algorithm.clone(),
        notarization_receipt_summary: contract.notarization_receipt_summary.clone(),
        ledger_seal_summary: contract.ledger_seal_summary.clone(),
        ledger_seal_sha256: contract.ledger_seal_sha256.clone(),
        ledger_seal_algorithm: contract.ledger_seal_algorithm.clone(),
        registry_entry_summary: contract.registry_entry_summary.clone(),
        registry_entry_sha256: contract.registry_entry_sha256.clone(),
        registry_entry_algorithm: contract.registry_entry_algorithm.clone(),
        filing_certificate_summary: contract.filing_certificate_summary.clone(),
        filing_certificate_sha256: contract.filing_certificate_sha256.clone(),
        filing_certificate_algorithm: contract.filing_certificate_algorithm.clone(),
        archive_receipt_summary: contract.archive_receipt_summary.clone(),
        archive_receipt_sha256: contract.archive_receipt_sha256.clone(),
        archive_receipt_algorithm: contract.archive_receipt_algorithm.clone(),
        closure_certificate_summary: contract.closure_certificate_summary.clone(),
        closure_certificate_sha256: contract.closure_certificate_sha256.clone(),
        closure_certificate_algorithm: contract.closure_certificate_algorithm.clone(),
        finality_receipt_summary: contract.finality_receipt_summary.clone(),
        finality_receipt_sha256: contract.finality_receipt_sha256.clone(),
        finality_receipt_algorithm: contract.finality_receipt_algorithm.clone(),
        consummation_record_summary: contract.consummation_record_summary.clone(),
        consummation_record_sha256: contract.consummation_record_sha256.clone(),
        consummation_record_algorithm: contract.consummation_record_algorithm.clone(),
        completion_certificate_summary: contract.completion_certificate_summary.clone(),
        completion_certificate_sha256: contract.completion_certificate_sha256.clone(),
        completion_certificate_algorithm: contract.completion_certificate_algorithm.clone(),
        summit_certificate_summary: contract.summit_certificate_summary.clone(),
        summit_certificate_sha256: contract.summit_certificate_sha256.clone(),
        summit_certificate_algorithm: contract.summit_certificate_algorithm.clone(),
        culmination_receipt_summary: contract.culmination_receipt_summary.clone(),
        culmination_receipt_sha256: contract.culmination_receipt_sha256.clone(),
        culmination_receipt_algorithm: contract.culmination_receipt_algorithm.clone(),
        pinnacle_receipt_summary: contract.pinnacle_receipt_summary.clone(),
        pinnacle_receipt_sha256: contract.pinnacle_receipt_sha256.clone(),
        pinnacle_receipt_algorithm: contract.pinnacle_receipt_algorithm.clone(),
        capstone_certificate_summary: contract.capstone_certificate_summary.clone(),
        capstone_certificate_sha256: contract.capstone_certificate_sha256.clone(),
        capstone_certificate_algorithm: contract.capstone_certificate_algorithm.clone(),
        keystone_receipt_summary: contract.keystone_receipt_summary.clone(),
        keystone_receipt_sha256: contract.keystone_receipt_sha256.clone(),
        keystone_receipt_algorithm: contract.keystone_receipt_algorithm.clone(),
        cornerstone_certificate_summary: contract.cornerstone_certificate_summary.clone(),
        cornerstone_certificate_sha256: contract.cornerstone_certificate_sha256.clone(),
        cornerstone_certificate_algorithm: contract.cornerstone_certificate_algorithm.clone(),
        foundation_receipt_summary: contract.foundation_receipt_summary.clone(),
        foundation_receipt_sha256: contract.foundation_receipt_sha256.clone(),
        foundation_receipt_algorithm: contract.foundation_receipt_algorithm.clone(),
        bedrock_certificate_summary: contract.bedrock_certificate_summary.clone(),
        bedrock_certificate_sha256: contract.bedrock_certificate_sha256.clone(),
        bedrock_certificate_algorithm: contract.bedrock_certificate_algorithm.clone(),
        basal_receipt_summary: contract.basal_receipt_summary.clone(),
        basal_receipt_sha256: contract.basal_receipt_sha256.clone(),
        basal_receipt_algorithm: contract.basal_receipt_algorithm.clone(),
        substructure_certificate_summary: contract.substructure_certificate_summary.clone(),
        substructure_certificate_sha256: contract.substructure_certificate_sha256.clone(),
        substructure_certificate_algorithm: contract.substructure_certificate_algorithm.clone(),
        plinth_receipt_summary: contract.plinth_receipt_summary.clone(),
        plinth_receipt_sha256: contract.plinth_receipt_sha256.clone(),
        plinth_receipt_algorithm: contract.plinth_receipt_algorithm.clone(),
        pedestal_certificate_summary: contract.pedestal_certificate_summary.clone(),
        pedestal_certificate_sha256: contract.pedestal_certificate_sha256.clone(),
        pedestal_certificate_algorithm: contract.pedestal_certificate_algorithm.clone(),
        dais_receipt_summary: contract.dais_receipt_summary.clone(),
        dais_receipt_sha256: contract.dais_receipt_sha256.clone(),
        dais_receipt_algorithm: contract.dais_receipt_algorithm.clone(),
        rostrum_certificate_summary: contract.rostrum_certificate_summary.clone(),
        rostrum_certificate_sha256: contract.rostrum_certificate_sha256.clone(),
        rostrum_certificate_algorithm: contract.rostrum_certificate_algorithm.clone(),
        podium_receipt_summary: contract.podium_receipt_summary.clone(),
        podium_receipt_sha256: contract.podium_receipt_sha256.clone(),
        podium_receipt_algorithm: contract.podium_receipt_algorithm.clone(),
        lectern_certificate_summary: contract.lectern_certificate_summary.clone(),
        lectern_certificate_sha256: contract.lectern_certificate_sha256.clone(),
        lectern_certificate_algorithm: contract.lectern_certificate_algorithm.clone(),
        pulpit_receipt_summary: contract.pulpit_receipt_summary.clone(),
        pulpit_receipt_sha256: contract.pulpit_receipt_sha256.clone(),
        pulpit_receipt_algorithm: contract.pulpit_receipt_algorithm.clone(),
        chancel_certificate_summary: contract.chancel_certificate_summary.clone(),
        chancel_certificate_sha256: contract.chancel_certificate_sha256.clone(),
        chancel_certificate_algorithm: contract.chancel_certificate_algorithm.clone(),
        apse_receipt_summary: contract.apse_receipt_summary.clone(),
        apse_receipt_sha256: contract.apse_receipt_sha256.clone(),
        apse_receipt_algorithm: contract.apse_receipt_algorithm.clone(),
        sanctuary_certificate_summary: sanctuary_certificate_summary(
            classification.0,
            &contract.reviewed_frozen_live_cutover_controller_command_summary,
        ),
        sanctuary_certificate_sha256: sanctuary_certificate_sha256.to_string(),
        sanctuary_certificate_algorithm: SANCTUARY_CERTIFICATE_ALGORITHM.to_string(),
        explicit_statement: SANCTUARY_CERTIFICATE_STATUS_EXPLICIT_STATEMENT.to_string(),
    }
}

fn report_from_status(
    config: &Config,
    session_dir: &Path,
    paths: &PackageSanctuaryCertificatePaths,
    contract: &package_apse_receipt_sanctuary_certificate::LivePackageApseReceiptContractView,
    verdict: TinyLivePackageSanctuaryCertificateVerdict,
    status: &PackageSanctuaryCertificateStatus,
) -> PackageSanctuaryCertificateReport {
    PackageSanctuaryCertificateReport {
        generated_at: Utc::now(),
        mode: if verdict == TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyOk {
            "verify_live_package_sanctuary_certificate".to_string()
        } else {
            "run_live_package_sanctuary_certificate".to_string()
        },
        verdict,
        reason: status.reason.clone(),
        apse_receipt_session_dir: config
            .apse_receipt_session_dir
            .display()
            .to_string(),
        lectern_certificate_session_dir: Some(
            contract
                .lectern_certificate_session_dir
                .display()
                .to_string(),
        ),
        dais_receipt_session_dir: Some(contract.dais_receipt_session_dir.display().to_string()),
        pedestal_certificate_session_dir: Some(
            contract
                .pedestal_certificate_session_dir
                .display()
                .to_string(),
        ),
        plinth_receipt_session_dir: Some(contract.plinth_receipt_session_dir.display().to_string()),
        substructure_certificate_session_dir: Some(
            contract
                .substructure_certificate_session_dir
                .display()
                .to_string(),
        ),
        basal_receipt_session_dir: Some(contract.basal_receipt_session_dir.display().to_string()),
        bedrock_certificate_session_dir: Some(
            contract
                .bedrock_certificate_session_dir
                .display()
                .to_string(),
        ),
        foundation_receipt_session_dir: Some(
            contract
                .foundation_receipt_session_dir
                .display()
                .to_string(),
        ),
        registry_entry_session_dir: Some(contract.registry_entry_session_dir.display().to_string()),
        notarization_receipt_session_dir: Some(
            contract
                .notarization_receipt_session_dir
                .display()
                .to_string(),
        ),
        provenance_certificate_session_dir: Some(
            contract
                .provenance_certificate_session_dir
                .display()
                .to_string(),
        ),
        attestation_seal_session_dir: Some(
            contract.attestation_seal_session_dir.display().to_string(),
        ),
        release_capsule_session_dir: Some(
            contract.release_capsule_session_dir.display().to_string(),
        ),
        activation_ticket_session_dir: Some(
            contract.activation_ticket_session_dir.display().to_string(),
        ),
        review_receipt_session_dir: Some(contract.review_receipt_session_dir.display().to_string()),
        handoff_bundle_session_dir: Some(contract.handoff_bundle_session_dir.display().to_string()),
        decision_packet_session_dir: Some(
            contract.decision_packet_session_dir.display().to_string(),
        ),
        execute_frozen_session_dir: Some(contract.execute_frozen_session_dir.display().to_string()),
        turn_green_session_dir: Some(contract.turn_green_session_dir.display().to_string()),
        launch_packet_session_dir: Some(contract.launch_packet_session_dir.display().to_string()),
        package_dir: Some(contract.package_dir.display().to_string()),
        install_root: Some(contract.install_root.display().to_string()),
        target_service_name: Some(contract.target_service_name.clone()),
        backend_command: Some(contract.backend_command.clone()),
        wrapper_timeout_ms: Some(contract.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(contract.service_status_max_staleness_ms),
        session_dir: Some(session_dir.display().to_string()),
        apse_receipt_session_path: Some(paths.session_path.display().to_string()),
        apse_receipt_status_path: Some(paths.status_path.display().to_string()),
        archived_apse_receipt_report_path: Some(
            paths.apse_receipt_report_path.display().to_string(),
        ),
        result: Some(serialize_enum(&status.result)),
        handoff_bundle_result: status.handoff_bundle_result.clone(),
        decision_packet_result: status.decision_packet_result.clone(),
        execute_frozen_result: status.execute_frozen_result.clone(),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        apse_receipt_step: status.apse_receipt_step.clone(),
        verify_apse_receipt_command_summary: Some(
            verify_apse_receipt_command_summary(&config.apse_receipt_session_dir),
        ),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            contract
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        provenance_certificate_summary: Some(status.provenance_certificate_summary.clone()),
        chain_fingerprint_summary: Some(status.chain_fingerprint_summary.clone()),
        chain_fingerprint_sha256: Some(status.chain_fingerprint_sha256.clone()),
        chain_fingerprint_algorithm: Some(status.chain_fingerprint_algorithm.clone()),
        release_capsule_digest_manifest_sha256: Some(
            status.release_capsule_digest_manifest_sha256.clone(),
        ),
        release_capsule_digest_manifest_entry_count: Some(
            status.release_capsule_digest_manifest_entry_count,
        ),
        release_capsule_digest_algorithm: Some(status.release_capsule_digest_algorithm.clone()),
        notarization_receipt_summary: Some(status.notarization_receipt_summary.clone()),
        ledger_seal_summary: Some(status.ledger_seal_summary.clone()),
        ledger_seal_sha256: Some(status.ledger_seal_sha256.clone()),
        ledger_seal_algorithm: Some(status.ledger_seal_algorithm.clone()),
        registry_entry_summary: Some(status.registry_entry_summary.clone()),
        registry_entry_sha256: Some(status.registry_entry_sha256.clone()),
        registry_entry_algorithm: Some(status.registry_entry_algorithm.clone()),
        filing_certificate_summary: Some(status.filing_certificate_summary.clone()),
        filing_certificate_sha256: Some(status.filing_certificate_sha256.clone()),
        filing_certificate_algorithm: Some(status.filing_certificate_algorithm.clone()),
        archive_receipt_summary: Some(status.archive_receipt_summary.clone()),
        archive_receipt_sha256: Some(status.archive_receipt_sha256.clone()),
        archive_receipt_algorithm: Some(status.archive_receipt_algorithm.clone()),
        closure_certificate_summary: Some(status.closure_certificate_summary.clone()),
        closure_certificate_sha256: Some(status.closure_certificate_sha256.clone()),
        closure_certificate_algorithm: Some(status.closure_certificate_algorithm.clone()),
        finality_receipt_summary: Some(status.finality_receipt_summary.clone()),
        finality_receipt_sha256: Some(status.finality_receipt_sha256.clone()),
        finality_receipt_algorithm: Some(status.finality_receipt_algorithm.clone()),
        consummation_record_summary: Some(status.consummation_record_summary.clone()),
        consummation_record_sha256: Some(status.consummation_record_sha256.clone()),
        consummation_record_algorithm: Some(status.consummation_record_algorithm.clone()),
        completion_certificate_summary: Some(status.completion_certificate_summary.clone()),
        completion_certificate_sha256: Some(status.completion_certificate_sha256.clone()),
        completion_certificate_algorithm: Some(status.completion_certificate_algorithm.clone()),
        summit_certificate_summary: Some(status.summit_certificate_summary.clone()),
        summit_certificate_sha256: Some(status.summit_certificate_sha256.clone()),
        summit_certificate_algorithm: Some(status.summit_certificate_algorithm.clone()),
        culmination_receipt_summary: Some(status.culmination_receipt_summary.clone()),
        culmination_receipt_sha256: Some(status.culmination_receipt_sha256.clone()),
        culmination_receipt_algorithm: Some(status.culmination_receipt_algorithm.clone()),
        pinnacle_receipt_summary: Some(status.pinnacle_receipt_summary.clone()),
        pinnacle_receipt_sha256: Some(status.pinnacle_receipt_sha256.clone()),
        pinnacle_receipt_algorithm: Some(status.pinnacle_receipt_algorithm.clone()),
        capstone_certificate_summary: Some(status.capstone_certificate_summary.clone()),
        capstone_certificate_sha256: Some(status.capstone_certificate_sha256.clone()),
        capstone_certificate_algorithm: Some(status.capstone_certificate_algorithm.clone()),
        keystone_receipt_summary: Some(status.keystone_receipt_summary.clone()),
        keystone_receipt_sha256: Some(status.keystone_receipt_sha256.clone()),
        keystone_receipt_algorithm: Some(status.keystone_receipt_algorithm.clone()),
        cornerstone_certificate_summary: Some(status.cornerstone_certificate_summary.clone()),
        cornerstone_certificate_sha256: Some(status.cornerstone_certificate_sha256.clone()),
        cornerstone_certificate_algorithm: Some(status.cornerstone_certificate_algorithm.clone()),
        foundation_receipt_summary: Some(status.foundation_receipt_summary.clone()),
        foundation_receipt_sha256: Some(status.foundation_receipt_sha256.clone()),
        foundation_receipt_algorithm: Some(status.foundation_receipt_algorithm.clone()),
        bedrock_certificate_summary: Some(status.bedrock_certificate_summary.clone()),
        bedrock_certificate_sha256: Some(status.bedrock_certificate_sha256.clone()),
        bedrock_certificate_algorithm: Some(status.bedrock_certificate_algorithm.clone()),
        basal_receipt_summary: Some(status.basal_receipt_summary.clone()),
        basal_receipt_sha256: Some(status.basal_receipt_sha256.clone()),
        basal_receipt_algorithm: Some(status.basal_receipt_algorithm.clone()),
        substructure_certificate_summary: Some(status.substructure_certificate_summary.clone()),
        substructure_certificate_sha256: Some(status.substructure_certificate_sha256.clone()),
        substructure_certificate_algorithm: Some(status.substructure_certificate_algorithm.clone()),
        plinth_receipt_summary: Some(status.plinth_receipt_summary.clone()),
        plinth_receipt_sha256: Some(status.plinth_receipt_sha256.clone()),
        plinth_receipt_algorithm: Some(status.plinth_receipt_algorithm.clone()),
        pedestal_certificate_summary: Some(status.pedestal_certificate_summary.clone()),
        pedestal_certificate_sha256: Some(status.pedestal_certificate_sha256.clone()),
        pedestal_certificate_algorithm: Some(status.pedestal_certificate_algorithm.clone()),
        dais_receipt_summary: Some(status.dais_receipt_summary.clone()),
        dais_receipt_sha256: Some(status.dais_receipt_sha256.clone()),
        dais_receipt_algorithm: Some(status.dais_receipt_algorithm.clone()),
        rostrum_certificate_summary: Some(status.rostrum_certificate_summary.clone()),
        rostrum_certificate_sha256: Some(status.rostrum_certificate_sha256.clone()),
        rostrum_certificate_algorithm: Some(status.rostrum_certificate_algorithm.clone()),
        podium_receipt_summary: Some(status.podium_receipt_summary.clone()),
        podium_receipt_sha256: Some(status.podium_receipt_sha256.clone()),
        podium_receipt_algorithm: Some(status.podium_receipt_algorithm.clone()),
        lectern_certificate_summary: Some(status.lectern_certificate_summary.clone()),
        lectern_certificate_sha256: Some(status.lectern_certificate_sha256.clone()),
        lectern_certificate_algorithm: Some(status.lectern_certificate_algorithm.clone()),
        pulpit_receipt_summary: Some(status.pulpit_receipt_summary.clone()),
        pulpit_receipt_sha256: Some(status.pulpit_receipt_sha256.clone()),
        pulpit_receipt_algorithm: Some(status.pulpit_receipt_algorithm.clone()),
        chancel_certificate_summary: Some(status.chancel_certificate_summary.clone()),
        chancel_certificate_sha256: Some(status.chancel_certificate_sha256.clone()),
        chancel_certificate_algorithm: Some(status.chancel_certificate_algorithm.clone()),
        apse_receipt_summary: Some(status.apse_receipt_summary.clone()),
        apse_receipt_sha256: Some(status.apse_receipt_sha256.clone()),
        apse_receipt_algorithm: Some(status.apse_receipt_algorithm.clone()),
        sanctuary_certificate_summary: Some(status.sanctuary_certificate_summary.clone()),
        sanctuary_certificate_sha256: Some(status.sanctuary_certificate_sha256.clone()),
        sanctuary_certificate_algorithm: Some(status.sanctuary_certificate_algorithm.clone()),
        verification_mismatches: Vec::new(),
        explicit_statement: if verdict
            == TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyOk
        {
            SANCTUARY_CERTIFICATE_VERIFY_EXPLICIT_STATEMENT.to_string()
        } else {
            status.explicit_statement.clone()
        },
    }
}

fn failure_report_without_contract(
    config: &Config,
    verdict: TinyLivePackageSanctuaryCertificateVerdict,
    reason: String,
) -> PackageSanctuaryCertificateReport {
    PackageSanctuaryCertificateReport {
        generated_at: Utc::now(),
        mode: match config.mode {
            Mode::PlanLivePackageSanctuaryCertificate => {
                "plan_live_package_sanctuary_certificate".to_string()
            }
            Mode::RenderLivePackageSanctuaryCertificate => {
                "render_live_package_sanctuary_certificate".to_string()
            }
            Mode::RunLivePackageSanctuaryCertificate => {
                "run_live_package_sanctuary_certificate".to_string()
            }
            Mode::VerifyLivePackageSanctuaryCertificate => {
                "verify_live_package_sanctuary_certificate".to_string()
            }
        },
        verdict,
        reason,
        apse_receipt_session_dir: config
            .apse_receipt_session_dir
            .display()
            .to_string(),
        lectern_certificate_session_dir: None,
        dais_receipt_session_dir: None,
        pedestal_certificate_session_dir: None,
        plinth_receipt_session_dir: None,
        substructure_certificate_session_dir: None,
        basal_receipt_session_dir: None,
        bedrock_certificate_session_dir: None,
        foundation_receipt_session_dir: None,
        registry_entry_session_dir: None,
        notarization_receipt_session_dir: None,
        provenance_certificate_session_dir: None,
        attestation_seal_session_dir: None,
        release_capsule_session_dir: None,
        activation_ticket_session_dir: None,
        review_receipt_session_dir: None,
        handoff_bundle_session_dir: None,
        decision_packet_session_dir: None,
        execute_frozen_session_dir: None,
        turn_green_session_dir: None,
        launch_packet_session_dir: None,
        package_dir: None,
        install_root: None,
        target_service_name: None,
        backend_command: None,
        wrapper_timeout_ms: None,
        service_status_max_staleness_ms: None,
        session_dir: config
            .session_dir
            .as_ref()
            .map(|value| value.display().to_string()),
        apse_receipt_session_path: None,
        apse_receipt_status_path: None,
        archived_apse_receipt_report_path: None,
        result: Some("refused_now_by_invalid_or_drifted_contract".to_string()),
        handoff_bundle_result: None,
        decision_packet_result: None,
        execute_frozen_result: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        apse_receipt_step: None,
        verify_apse_receipt_command_summary: Some(verify_apse_receipt_command_summary(
            &config.apse_receipt_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: None,
        provenance_certificate_summary: None,
        chain_fingerprint_summary: None,
        chain_fingerprint_sha256: None,
        chain_fingerprint_algorithm: None,
        release_capsule_digest_manifest_sha256: None,
        release_capsule_digest_manifest_entry_count: None,
        release_capsule_digest_algorithm: None,
        notarization_receipt_summary: None,
        ledger_seal_summary: None,
        ledger_seal_sha256: None,
        ledger_seal_algorithm: None,
        registry_entry_summary: None,
        registry_entry_sha256: None,
        registry_entry_algorithm: None,
        filing_certificate_summary: None,
        filing_certificate_sha256: None,
        filing_certificate_algorithm: None,
        archive_receipt_summary: None,
        archive_receipt_sha256: None,
        archive_receipt_algorithm: None,
        closure_certificate_summary: None,
        closure_certificate_sha256: None,
        closure_certificate_algorithm: None,
        finality_receipt_summary: None,
        finality_receipt_sha256: None,
        finality_receipt_algorithm: None,
        consummation_record_summary: None,
        consummation_record_sha256: None,
        consummation_record_algorithm: None,
        completion_certificate_summary: None,
        completion_certificate_sha256: None,
        completion_certificate_algorithm: None,
        summit_certificate_summary: None,
        summit_certificate_sha256: None,
        summit_certificate_algorithm: None,
        culmination_receipt_summary: None,
        culmination_receipt_sha256: None,
        culmination_receipt_algorithm: None,
        pinnacle_receipt_summary: None,
        pinnacle_receipt_sha256: None,
        pinnacle_receipt_algorithm: None,
        capstone_certificate_summary: None,
        capstone_certificate_sha256: None,
        capstone_certificate_algorithm: None,
        keystone_receipt_summary: None,
        keystone_receipt_sha256: None,
        keystone_receipt_algorithm: None,
        cornerstone_certificate_summary: None,
        cornerstone_certificate_sha256: None,
        cornerstone_certificate_algorithm: None,
        foundation_receipt_summary: None,
        foundation_receipt_sha256: None,
        foundation_receipt_algorithm: None,
        bedrock_certificate_summary: None,
        bedrock_certificate_sha256: None,
        bedrock_certificate_algorithm: None,
        basal_receipt_summary: None,
        basal_receipt_sha256: None,
        basal_receipt_algorithm: None,
        substructure_certificate_summary: None,
        substructure_certificate_sha256: None,
        substructure_certificate_algorithm: None,
        plinth_receipt_summary: None,
        plinth_receipt_sha256: None,
        plinth_receipt_algorithm: None,
        pedestal_certificate_summary: None,
        pedestal_certificate_sha256: None,
        pedestal_certificate_algorithm: None,
        dais_receipt_summary: None,
        dais_receipt_sha256: None,
        dais_receipt_algorithm: None,
        rostrum_certificate_summary: None,
        rostrum_certificate_sha256: None,
        rostrum_certificate_algorithm: None,
        podium_receipt_summary: None,
        podium_receipt_sha256: None,
        podium_receipt_algorithm: None,
        lectern_certificate_summary: None,
        lectern_certificate_sha256: None,
        lectern_certificate_algorithm: None,
        pulpit_receipt_summary: None,
        pulpit_receipt_sha256: None,
        pulpit_receipt_algorithm: None,
        chancel_certificate_summary: None,
        chancel_certificate_sha256: None,
        chancel_certificate_algorithm: None,
        apse_receipt_summary: None,
        apse_receipt_sha256: None,
        apse_receipt_algorithm: None,
        sanctuary_certificate_summary: None,
        sanctuary_certificate_sha256: None,
        sanctuary_certificate_algorithm: Some(SANCTUARY_CERTIFICATE_ALGORITHM.to_string()),
        verification_mismatches: Vec::new(),
        explicit_statement:
            "this immutable sanctuary certificate refused before persisting because the apse-receipt artifact could not be verified coherently"
                .to_string(),
    }
}

fn failure_report_for_verify(
    config: &Config,
    session_dir: &Path,
    contract: &package_apse_receipt_sanctuary_certificate::LivePackageApseReceiptContractView,
    mismatches: Vec<String>,
) -> PackageSanctuaryCertificateReport {
    let paths = sanctuary_certificate_paths(session_dir);
    let reason = mismatches.first().cloned().unwrap_or_else(|| {
        "apse-receipt-native sanctuary-certificate verification failed".to_string()
    });
    PackageSanctuaryCertificateReport {
        generated_at: Utc::now(),
        mode: "verify_live_package_sanctuary_certificate".to_string(),
        verdict: TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid,
        reason,
        apse_receipt_session_dir: config
            .apse_receipt_session_dir
            .display()
            .to_string(),
        lectern_certificate_session_dir: Some(
            contract
                .lectern_certificate_session_dir
                .display()
                .to_string(),
        ),
        dais_receipt_session_dir: Some(contract.dais_receipt_session_dir.display().to_string()),
        pedestal_certificate_session_dir: Some(
            contract
                .pedestal_certificate_session_dir
                .display()
                .to_string(),
        ),
        plinth_receipt_session_dir: Some(contract.plinth_receipt_session_dir.display().to_string()),
        substructure_certificate_session_dir: Some(
            contract
                .substructure_certificate_session_dir
                .display()
                .to_string(),
        ),
        basal_receipt_session_dir: Some(contract.basal_receipt_session_dir.display().to_string()),
        bedrock_certificate_session_dir: Some(
            contract
                .bedrock_certificate_session_dir
                .display()
                .to_string(),
        ),
        foundation_receipt_session_dir: Some(
            contract
                .foundation_receipt_session_dir
                .display()
                .to_string(),
        ),
        registry_entry_session_dir: Some(contract.registry_entry_session_dir.display().to_string()),
        notarization_receipt_session_dir: Some(
            contract
                .notarization_receipt_session_dir
                .display()
                .to_string(),
        ),
        provenance_certificate_session_dir: Some(
            contract
                .provenance_certificate_session_dir
                .display()
                .to_string(),
        ),
        attestation_seal_session_dir: Some(
            contract.attestation_seal_session_dir.display().to_string(),
        ),
        release_capsule_session_dir: Some(
            contract.release_capsule_session_dir.display().to_string(),
        ),
        activation_ticket_session_dir: Some(
            contract.activation_ticket_session_dir.display().to_string(),
        ),
        review_receipt_session_dir: Some(contract.review_receipt_session_dir.display().to_string()),
        handoff_bundle_session_dir: Some(contract.handoff_bundle_session_dir.display().to_string()),
        decision_packet_session_dir: Some(
            contract.decision_packet_session_dir.display().to_string(),
        ),
        execute_frozen_session_dir: Some(contract.execute_frozen_session_dir.display().to_string()),
        turn_green_session_dir: Some(contract.turn_green_session_dir.display().to_string()),
        launch_packet_session_dir: Some(contract.launch_packet_session_dir.display().to_string()),
        package_dir: Some(contract.package_dir.display().to_string()),
        install_root: Some(contract.install_root.display().to_string()),
        target_service_name: Some(contract.target_service_name.clone()),
        backend_command: Some(contract.backend_command.clone()),
        wrapper_timeout_ms: Some(contract.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(contract.service_status_max_staleness_ms),
        session_dir: Some(session_dir.display().to_string()),
        apse_receipt_session_path: Some(paths.session_path.display().to_string()),
        apse_receipt_status_path: Some(paths.status_path.display().to_string()),
        archived_apse_receipt_report_path: Some(
            paths.apse_receipt_report_path.display().to_string(),
        ),
        result: None,
        handoff_bundle_result: contract.handoff_bundle_result.clone(),
        decision_packet_result: contract.decision_packet_result.clone(),
        execute_frozen_result: contract.execute_frozen_result.clone(),
        current_pre_activation_gate_verdict: contract.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: contract.current_pre_activation_gate_reason.clone(),
        apse_receipt_step: None,
        verify_apse_receipt_command_summary: Some(
            verify_apse_receipt_command_summary(&config.apse_receipt_session_dir),
        ),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            contract
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        provenance_certificate_summary: Some(contract.provenance_certificate_summary.clone()),
        chain_fingerprint_summary: Some(contract.chain_fingerprint_summary.clone()),
        chain_fingerprint_sha256: Some(contract.chain_fingerprint_sha256.clone()),
        chain_fingerprint_algorithm: Some(contract.chain_fingerprint_algorithm.clone()),
        release_capsule_digest_manifest_sha256: Some(
            contract.release_capsule_digest_manifest_sha256.clone(),
        ),
        release_capsule_digest_manifest_entry_count: Some(
            contract.release_capsule_digest_manifest_entry_count,
        ),
        release_capsule_digest_algorithm: Some(contract.release_capsule_digest_algorithm.clone()),
        notarization_receipt_summary: Some(contract.notarization_receipt_summary.clone()),
        ledger_seal_summary: Some(contract.ledger_seal_summary.clone()),
        ledger_seal_sha256: Some(contract.ledger_seal_sha256.clone()),
        ledger_seal_algorithm: Some(contract.ledger_seal_algorithm.clone()),
        registry_entry_summary: Some(contract.registry_entry_summary.clone()),
        registry_entry_sha256: Some(contract.registry_entry_sha256.clone()),
        registry_entry_algorithm: Some(contract.registry_entry_algorithm.clone()),
        filing_certificate_summary: Some(contract.filing_certificate_summary.clone()),
        filing_certificate_sha256: Some(contract.filing_certificate_sha256.clone()),
        filing_certificate_algorithm: Some(contract.filing_certificate_algorithm.clone()),
        archive_receipt_summary: Some(contract.archive_receipt_summary.clone()),
        archive_receipt_sha256: Some(contract.archive_receipt_sha256.clone()),
        archive_receipt_algorithm: Some(contract.archive_receipt_algorithm.clone()),
        closure_certificate_summary: Some(contract.closure_certificate_summary.clone()),
        closure_certificate_sha256: Some(contract.closure_certificate_sha256.clone()),
        closure_certificate_algorithm: Some(contract.closure_certificate_algorithm.clone()),
        finality_receipt_summary: Some(contract.finality_receipt_summary.clone()),
        finality_receipt_sha256: Some(contract.finality_receipt_sha256.clone()),
        finality_receipt_algorithm: Some(contract.finality_receipt_algorithm.clone()),
        consummation_record_summary: Some(contract.consummation_record_summary.clone()),
        consummation_record_sha256: Some(contract.consummation_record_sha256.clone()),
        consummation_record_algorithm: Some(contract.consummation_record_algorithm.clone()),
        completion_certificate_summary: Some(contract.completion_certificate_summary.clone()),
        completion_certificate_sha256: Some(contract.completion_certificate_sha256.clone()),
        completion_certificate_algorithm: Some(contract.completion_certificate_algorithm.clone()),
        summit_certificate_summary: Some(contract.summit_certificate_summary.clone()),
        summit_certificate_sha256: Some(contract.summit_certificate_sha256.clone()),
        summit_certificate_algorithm: Some(contract.summit_certificate_algorithm.clone()),
        culmination_receipt_summary: Some(contract.culmination_receipt_summary.clone()),
        culmination_receipt_sha256: Some(contract.culmination_receipt_sha256.clone()),
        culmination_receipt_algorithm: Some(contract.culmination_receipt_algorithm.clone()),
        pinnacle_receipt_summary: Some(contract.pinnacle_receipt_summary.clone()),
        pinnacle_receipt_sha256: Some(contract.pinnacle_receipt_sha256.clone()),
        pinnacle_receipt_algorithm: Some(contract.pinnacle_receipt_algorithm.clone()),
        capstone_certificate_summary: Some(contract.capstone_certificate_summary.clone()),
        capstone_certificate_sha256: Some(contract.capstone_certificate_sha256.clone()),
        capstone_certificate_algorithm: Some(contract.capstone_certificate_algorithm.clone()),
        keystone_receipt_summary: Some(contract.keystone_receipt_summary.clone()),
        keystone_receipt_sha256: Some(contract.keystone_receipt_sha256.clone()),
        keystone_receipt_algorithm: Some(contract.keystone_receipt_algorithm.clone()),
        cornerstone_certificate_summary: Some(contract.cornerstone_certificate_summary.clone()),
        cornerstone_certificate_sha256: Some(contract.cornerstone_certificate_sha256.clone()),
        cornerstone_certificate_algorithm: Some(contract.cornerstone_certificate_algorithm.clone()),
        foundation_receipt_summary: Some(contract.foundation_receipt_summary.clone()),
        foundation_receipt_sha256: Some(contract.foundation_receipt_sha256.clone()),
        foundation_receipt_algorithm: Some(contract.foundation_receipt_algorithm.clone()),
        bedrock_certificate_summary: Some(contract.bedrock_certificate_summary.clone()),
        bedrock_certificate_sha256: Some(contract.bedrock_certificate_sha256.clone()),
        bedrock_certificate_algorithm: Some(contract.bedrock_certificate_algorithm.clone()),
        basal_receipt_summary: Some(contract.basal_receipt_summary.clone()),
        basal_receipt_sha256: Some(contract.basal_receipt_sha256.clone()),
        basal_receipt_algorithm: Some(contract.basal_receipt_algorithm.clone()),
        substructure_certificate_summary: Some(contract.substructure_certificate_summary.clone()),
        substructure_certificate_sha256: Some(contract.substructure_certificate_sha256.clone()),
        substructure_certificate_algorithm: Some(
            contract.substructure_certificate_algorithm.clone(),
        ),
        plinth_receipt_summary: Some(contract.plinth_receipt_summary.clone()),
        plinth_receipt_sha256: Some(contract.plinth_receipt_sha256.clone()),
        plinth_receipt_algorithm: Some(contract.plinth_receipt_algorithm.clone()),
        pedestal_certificate_summary: Some(contract.pedestal_certificate_summary.clone()),
        pedestal_certificate_sha256: Some(contract.pedestal_certificate_sha256.clone()),
        pedestal_certificate_algorithm: Some(contract.pedestal_certificate_algorithm.clone()),
        dais_receipt_summary: Some(contract.dais_receipt_summary.clone()),
        dais_receipt_sha256: Some(contract.dais_receipt_sha256.clone()),
        dais_receipt_algorithm: Some(contract.dais_receipt_algorithm.clone()),
        rostrum_certificate_summary: Some(contract.rostrum_certificate_summary.clone()),
        rostrum_certificate_sha256: Some(contract.rostrum_certificate_sha256.clone()),
        rostrum_certificate_algorithm: Some(contract.rostrum_certificate_algorithm.clone()),
        podium_receipt_summary: Some(contract.podium_receipt_summary.clone()),
        podium_receipt_sha256: Some(contract.podium_receipt_sha256.clone()),
        podium_receipt_algorithm: Some(contract.podium_receipt_algorithm.clone()),
        lectern_certificate_summary: Some(contract.lectern_certificate_summary.clone()),
        lectern_certificate_sha256: Some(contract.lectern_certificate_sha256.clone()),
        lectern_certificate_algorithm: Some(contract.lectern_certificate_algorithm.clone()),
        pulpit_receipt_summary: Some(contract.pulpit_receipt_summary.clone()),
        pulpit_receipt_sha256: Some(contract.pulpit_receipt_sha256.clone()),
        pulpit_receipt_algorithm: Some(contract.pulpit_receipt_algorithm.clone()),
        chancel_certificate_summary: Some(contract.chancel_certificate_summary.clone()),
        chancel_certificate_sha256: Some(contract.chancel_certificate_sha256.clone()),
        chancel_certificate_algorithm: Some(contract.chancel_certificate_algorithm.clone()),
        apse_receipt_summary: Some(contract.apse_receipt_summary.clone()),
        apse_receipt_sha256: Some(contract.apse_receipt_sha256.clone()),
        apse_receipt_algorithm: Some(contract.apse_receipt_algorithm.clone()),
        sanctuary_certificate_summary: None,
        sanctuary_certificate_sha256: None,
        sanctuary_certificate_algorithm: Some(SANCTUARY_CERTIFICATE_ALGORITHM.to_string()),
        verification_mismatches: mismatches,
        explicit_statement: SANCTUARY_CERTIFICATE_VERIFY_EXPLICIT_STATEMENT.to_string(),
    }
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
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

fn persist_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed creating {}", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(value)?;
    fs::write(path, bytes).with_context(|| format!("failed writing {}", path.display()))
}

fn load_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let bytes = fs::read(path).with_context(|| format!("failed reading {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("failed parsing {}", path.display()))
}

fn load_required_step_json(
    step: &Option<PackageSanctuaryCertificateStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<serde_json::Value> {
    let Some(step) = step else {
        mismatches.push(format!("{label} is missing from apse-receipt status"));
        return load_json(expected_path);
    };
    compare_string(
        &step.path,
        &expected_path.display().to_string(),
        &format!("{label} path"),
        mismatches,
    );
    load_json(expected_path)
}

fn step_artifact(
    path: &Path,
    mode: &str,
    verdict: &str,
    reason: &str,
    generated_at: DateTime<Utc>,
) -> PackageSanctuaryCertificateStepArtifact {
    PackageSanctuaryCertificateStepArtifact {
        path: path.display().to_string(),
        mode: mode.to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        generated_at,
    }
}

fn compare_string(actual: &str, expected: &str, label: &str, mismatches: &mut Vec<String>) {
    if actual != expected {
        mismatches.push(format!(
            "{label} {actual:?} does not match expected {expected:?}"
        ));
    }
}

fn compare_json_ignoring_generated_at(
    actual: &serde_json::Value,
    expected: &serde_json::Value,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    let normalized_actual = normalized_report_json_without_generated_at(actual.clone());
    let normalized_expected = normalized_report_json_without_generated_at(expected.clone());
    if normalized_actual != normalized_expected {
        mismatches.push(format!(
            "{label} does not match freshly verified nested truth"
        ));
    }
}

fn normalized_report_json_without_generated_at(mut value: serde_json::Value) -> serde_json::Value {
    if let Some(object) = value.as_object_mut() {
        object.remove("generated_at");
    }
    value
}

fn compare_step_artifact(
    actual: Option<&PackageSanctuaryCertificateStepArtifact>,
    expected: &PackageSanctuaryCertificateStepArtifact,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    let Some(actual) = actual else {
        mismatches.push(format!("{label} is missing from apse-receipt status"));
        return;
    };
    compare_string(
        &actual.path,
        &expected.path,
        &format!("{label} path"),
        mismatches,
    );
    compare_string(
        &actual.mode,
        &expected.mode,
        &format!("{label} mode"),
        mismatches,
    );
    compare_string(
        &actual.verdict,
        &expected.verdict,
        &format!("{label} verdict"),
        mismatches,
    );
    compare_string(
        &actual.reason,
        &expected.reason,
        &format!("{label} reason"),
        mismatches,
    );
    if actual.generated_at != expected.generated_at {
        mismatches.push(format!(
            "{label} generated_at {:?} does not match expected {:?}",
            actual.generated_at, expected.generated_at
        ));
    }
}

fn compute_sanctuary_certificate_sha256(
    result: &str,
    handoff_bundle_result: Option<&str>,
    decision_packet_result: Option<&str>,
    execute_frozen_result: Option<&str>,
    current_pre_activation_gate_verdict: Option<&str>,
    current_pre_activation_gate_reason: Option<&str>,
    reviewed_frozen_live_cutover_controller_command_summary: &str,
    chain_fingerprint_sha256: &str,
    chain_fingerprint_algorithm: &str,
    release_capsule_digest_manifest_sha256: &str,
    release_capsule_digest_manifest_entry_count: usize,
    release_capsule_digest_algorithm: &str,
    ledger_seal_sha256: &str,
    ledger_seal_algorithm: &str,
    registry_entry_sha256: &str,
    registry_entry_algorithm: &str,
    filing_certificate_sha256: &str,
    filing_certificate_algorithm: &str,
    archive_receipt_sha256: &str,
    archive_receipt_algorithm: &str,
    closure_certificate_summary: &str,
    closure_certificate_sha256: &str,
    closure_certificate_algorithm: &str,
    finality_receipt_sha256: &str,
    finality_receipt_algorithm: &str,
    consummation_record_sha256: &str,
    consummation_record_algorithm: &str,
    completion_certificate_summary: &str,
    completion_certificate_sha256: &str,
    completion_certificate_algorithm: &str,
    summit_certificate_summary: &str,
    summit_certificate_sha256: &str,
    summit_certificate_algorithm: &str,
    culmination_receipt_summary: &str,
    culmination_receipt_sha256: &str,
    culmination_receipt_algorithm: &str,
    pinnacle_receipt_summary: &str,
    pinnacle_receipt_sha256: &str,
    pinnacle_receipt_algorithm: &str,
    capstone_certificate_summary: &str,
    capstone_certificate_sha256: &str,
    capstone_certificate_algorithm: &str,
    keystone_receipt_summary: &str,
    keystone_receipt_sha256: &str,
    keystone_receipt_algorithm: &str,
    cornerstone_certificate_summary: &str,
    cornerstone_certificate_sha256: &str,
    cornerstone_certificate_algorithm: &str,
    foundation_receipt_summary: &str,
    foundation_receipt_sha256: &str,
    foundation_receipt_algorithm: &str,
    bedrock_certificate_summary: &str,
    bedrock_certificate_sha256: &str,
    bedrock_certificate_algorithm: &str,
    basal_receipt_summary: &str,
    basal_receipt_sha256: &str,
    basal_receipt_algorithm: &str,
    substructure_certificate_summary: &str,
    substructure_certificate_sha256: &str,
    substructure_certificate_algorithm: &str,
    plinth_receipt_summary: &str,
    plinth_receipt_sha256: &str,
    plinth_receipt_algorithm: &str,
    pedestal_certificate_summary: &str,
    pedestal_certificate_sha256: &str,
    pedestal_certificate_algorithm: &str,
    dais_receipt_summary: &str,
    dais_receipt_sha256: &str,
    dais_receipt_algorithm: &str,
    rostrum_certificate_summary: &str,
    rostrum_certificate_sha256: &str,
    rostrum_certificate_algorithm: &str,
    podium_receipt_summary: &str,
    podium_receipt_sha256: &str,
    podium_receipt_algorithm: &str,
    lectern_certificate_summary: &str,
    lectern_certificate_sha256: &str,
    lectern_certificate_algorithm: &str,
    pulpit_receipt_summary: &str,
    pulpit_receipt_sha256: &str,
    pulpit_receipt_algorithm: &str,
    chancel_certificate_summary: &str,
    chancel_certificate_sha256: &str,
    chancel_certificate_algorithm: &str,
    apse_receipt_summary: &str,
    apse_receipt_sha256: &str,
    apse_receipt_algorithm: &str,
) -> Result<String> {
    canonical_sha256_hex(&SanctuaryCertificatePayload {
        result,
        handoff_bundle_result,
        decision_packet_result,
        execute_frozen_result,
        current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason,
        reviewed_frozen_live_cutover_controller_command_summary,
        chain_fingerprint_sha256,
        chain_fingerprint_algorithm,
        release_capsule_digest_manifest_sha256,
        release_capsule_digest_manifest_entry_count,
        release_capsule_digest_algorithm,
        ledger_seal_sha256,
        ledger_seal_algorithm,
        registry_entry_sha256,
        registry_entry_algorithm,
        filing_certificate_sha256,
        filing_certificate_algorithm,
        archive_receipt_sha256,
        archive_receipt_algorithm,
        closure_certificate_summary,
        closure_certificate_sha256,
        closure_certificate_algorithm,
        finality_receipt_sha256,
        finality_receipt_algorithm,
        consummation_record_sha256,
        consummation_record_algorithm,
        completion_certificate_summary,
        completion_certificate_sha256,
        completion_certificate_algorithm,
        summit_certificate_summary,
        summit_certificate_sha256,
        summit_certificate_algorithm,
        culmination_receipt_summary,
        culmination_receipt_sha256,
        culmination_receipt_algorithm,
        pinnacle_receipt_summary,
        pinnacle_receipt_sha256,
        pinnacle_receipt_algorithm,
        capstone_certificate_summary,
        capstone_certificate_sha256,
        capstone_certificate_algorithm,
        keystone_receipt_summary,
        keystone_receipt_sha256,
        keystone_receipt_algorithm,
        cornerstone_certificate_summary,
        cornerstone_certificate_sha256,
        cornerstone_certificate_algorithm,
        foundation_receipt_summary,
        foundation_receipt_sha256,
        foundation_receipt_algorithm,
        bedrock_certificate_summary,
        bedrock_certificate_sha256,
        bedrock_certificate_algorithm,
        basal_receipt_summary,
        basal_receipt_sha256,
        basal_receipt_algorithm,
        substructure_certificate_summary,
        substructure_certificate_sha256,
        substructure_certificate_algorithm,
        plinth_receipt_summary,
        plinth_receipt_sha256,
        plinth_receipt_algorithm,
        pedestal_certificate_summary,
        pedestal_certificate_sha256,
        pedestal_certificate_algorithm,
        dais_receipt_summary,
        dais_receipt_sha256,
        dais_receipt_algorithm,
        rostrum_certificate_summary,
        rostrum_certificate_sha256,
        rostrum_certificate_algorithm,
        podium_receipt_summary,
        podium_receipt_sha256,
        podium_receipt_algorithm,
        lectern_certificate_summary,
        lectern_certificate_sha256,
        lectern_certificate_algorithm,
        pulpit_receipt_summary,
        pulpit_receipt_sha256,
        pulpit_receipt_algorithm,
        chancel_certificate_summary,
        chancel_certificate_sha256,
        chancel_certificate_algorithm,
        apse_receipt_summary,
        apse_receipt_sha256,
        apse_receipt_algorithm,
    })
}

fn canonical_sha256_hex<T: Serialize>(value: &T) -> Result<String> {
    Ok(sha256_hex_bytes(&serde_json::to_vec_pretty(value)?))
}

fn sha256_hex_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!("{digest:x}")
}

fn write_script(path: &Path, contents: &str) -> Result<()> {
    fs::write(path, contents).with_context(|| {
        format!(
            "failed writing sanctuary-certificate script to {}",
            path.display()
        )
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(path)?.permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions)?;
    }
    Ok(())
}

fn shell_escape_path(path: &Path) -> String {
    let value = path.display().to_string();
    if value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || b"/._-".contains(&byte))
    {
        value
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

fn render_report_lines(report: &PackageSanctuaryCertificateReport) -> String {
    let mut lines = vec![
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!(
            "apse_receipt_session_dir={}",
            report.apse_receipt_session_dir
        ),
    ];
    if let Some(value) = &report.substructure_certificate_session_dir {
        lines.push(format!("substructure_certificate_session_dir={value}"));
    }
    if let Some(value) = &report.result {
        lines.push(format!("result={value}"));
    }
    if let Some(value) = &report.consummation_record_sha256 {
        lines.push(format!("consummation_record_sha256={value}"));
    }
    if let Some(value) = &report.summit_certificate_sha256 {
        lines.push(format!("summit_certificate_sha256={value}"));
    }
    if let Some(value) = &report.bedrock_certificate_sha256 {
        lines.push(format!("bedrock_certificate_sha256={value}"));
    }
    if let Some(value) = &report.basal_receipt_sha256 {
        lines.push(format!("basal_receipt_sha256={value}"));
    }
    if let Some(value) = &report.cornerstone_certificate_sha256 {
        lines.push(format!("cornerstone_certificate_sha256={value}"));
    }
    if let Some(value) = &report.pulpit_receipt_sha256 {
        lines.push(format!("pulpit_receipt_sha256={value}"));
    }
    if let Some(value) = &report.filing_certificate_sha256 {
        lines.push(format!("filing_certificate_sha256={value}"));
    }
    if let Some(value) = &report.archive_receipt_sha256 {
        lines.push(format!("archive_receipt_sha256={value}"));
    }
    if let Some(value) = &report.closure_certificate_sha256 {
        lines.push(format!("closure_certificate_sha256={value}"));
    }
    if let Some(value) = &report.finality_receipt_sha256 {
        lines.push(format!("finality_receipt_sha256={value}"));
    }
    if let Some(value) = &report.registry_entry_sha256 {
        lines.push(format!("registry_entry_sha256={value}"));
    }
    if let Some(value) = &report.ledger_seal_sha256 {
        lines.push(format!("ledger_seal_sha256={value}"));
    }
    if let Some(value) = &report.chain_fingerprint_sha256 {
        lines.push(format!("chain_fingerprint_sha256={value}"));
    }
    if let Some(value) = &report.session_dir {
        lines.push(format!("session_dir={value}"));
    }
    if !report.verification_mismatches.is_empty() {
        lines.push(format!(
            "verification_mismatches={}",
            report.verification_mismatches.join(" | ")
        ));
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Map, Value};
    use std::ffi::OsString;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn run_ready_for_manual_execution_when_gate_turns_green_then_verify_stays_green() {
        let fixture = fake_command_fixture(
            "consummation_record_ready",
            consummation_record_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        let run = run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateReadyForManualExecutionWhenGateTurnsGreen
        );
        let verify =
            verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyOk
        );
    }

    #[test]
    fn stage3_refusal_stays_explicit() {
        let fixture = fake_command_fixture(
            "consummation_record_stage3_refusal",
            consummation_record_verify_report(
                "refused_now_by_stage3",
                "refused_now_by_stage3",
                "refused_now_by_stage3",
                "refused_now_by_stage3",
                "red",
                "stage3 blocked",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        let run = run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateRefusedNowByStage3
        );
    }

    #[test]
    fn pre_activation_refusal_stays_explicit() {
        let fixture = fake_command_fixture(
            "consummation_record_gate_refusal",
            consummation_record_verify_report(
                "refused_now_by_pre_activation_gate",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "refused_now_by_pre_activation_gate",
                "red",
                "pre-activation blocked",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        let run = run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateRefusedNowByPreActivationGate
        );
    }

    #[test]
    fn drifted_apse_receipt_contract_is_refused() {
        let fixture = fake_command_fixture(
            "apse_receipt_drifted_contract",
            json!({
                "generated_at": Utc::now(),
                "mode": "verify_live_package_apse_receipt",
                "verdict": "tiny_live_package_apse_receipt_verify_invalid",
                "reason": "drifted",
                "result": "refused_now_by_invalid_or_drifted_contract",
                "handoff_bundle_result": "ready_for_manual_go_live_review",
                "decision_packet_result": "runnable_when_gate_truth_turns_green",
                "execute_frozen_result": "ready_for_manual_execution_when_gate_turns_green",
                "current_pre_activation_gate_verdict": "red",
                "current_pre_activation_gate_reason": "drifted"
            }),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        let run = run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateRefusedNowByInvalidOrDriftedContract
        );
    }

    #[test]
    fn run_refuses_managed_surface_overlap_before_writing_any_artifacts() {
        let fixture = fake_command_fixture(
            "sanctuary_certificate_managed_overlap",
            bedrock_certificate_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        let managed_paths = copybot_app::tiny_live_activation::install_target_managed_surface::derive_install_target_managed_surface_paths(
            &fixture.install_root,
        )
        .unwrap();
        let overlap_session_dir = managed_paths
            .runtime_dir
            .join("sanctuary-certificate-session");
        fs::create_dir_all(overlap_session_dir.parent().unwrap()).unwrap();

        let config = Config {
            session_dir: Some(overlap_session_dir.clone()),
            ..fixture.run_config()
        };
        let error = run_live_package_sanctuary_certificate_report(&config).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("overlaps the managed live target surface"),
            "{error:#}"
        );
        let paths = sanctuary_certificate_paths(&overlap_session_dir);
        assert!(!paths.session_path.exists());
        assert!(!paths.status_path.exists());
    }

    #[test]
    fn verify_rejects_tampered_apse_receipt_step_path() {
        let fixture = fake_command_fixture(
            "apse_receipt_tampered_step_path",
            bedrock_certificate_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
        let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
        let mut status: PackageSanctuaryCertificateStatus = load_json(&paths.status_path).unwrap();
        status.apse_receipt_step.as_mut().unwrap().path =
            "/tmp/foreign.apse-receipt.report.json".to_string();
        persist_json(&paths.status_path, &status).unwrap();

        let verify =
            verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_rostrum_certificate_sha256_summary_or_algorithm() {
        {
            let fixture = fake_command_fixture(
                "sanctuary_certificate_tampered_rostrum_certificate_sha256",
                bedrock_certificate_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut session: PackageSanctuaryCertificateSession =
                load_json(&paths.session_path).unwrap();
            session.rostrum_certificate_sha256 = "tampered-rostrum-certificate-sha256".to_string();
            persist_json(&paths.session_path, &session).unwrap();

            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "sanctuary_certificate_tampered_rostrum_certificate_summary",
                bedrock_certificate_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut session: PackageSanctuaryCertificateSession =
                load_json(&paths.session_path).unwrap();
            session.rostrum_certificate_summary =
                "tampered rostrum certificate summary".to_string();
            persist_json(&paths.session_path, &session).unwrap();

            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "sanctuary_certificate_tampered_rostrum_certificate_algorithm",
                bedrock_certificate_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut session: PackageSanctuaryCertificateSession =
                load_json(&paths.session_path).unwrap();
            session.rostrum_certificate_algorithm =
                "tampered-rostrum-certificate-algorithm".to_string();
            persist_json(&paths.session_path, &session).unwrap();

            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }
    }

    #[test]
    fn verify_rejects_tampered_status_result() {
        let fixture = fake_command_fixture(
            "consummation_record_tampered_result",
            consummation_record_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
        let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
        let mut status: PackageSanctuaryCertificateStatus = load_json(&paths.status_path).unwrap();
        status.result = LivePackageSanctuaryCertificateResult::RefusedNowByStage3;
        persist_json(&paths.status_path, &status).unwrap();

        let verify =
            verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_status_gate_fields() {
        let fixture = fake_command_fixture(
            "consummation_record_tampered_gate_fields",
            consummation_record_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
        let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
        let mut status: PackageSanctuaryCertificateStatus = load_json(&paths.status_path).unwrap();
        status.current_pre_activation_gate_verdict = Some("red".to_string());
        status.current_pre_activation_gate_reason = Some("tampered".to_string());
        persist_json(&paths.status_path, &status).unwrap();

        let verify =
            verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_nested_apse_receipt_report_content() {
        let fixture = fake_command_fixture(
            "apse_receipt_tampered_nested_report",
            bedrock_certificate_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
        let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
        let mut report: serde_json::Value = load_json(&paths.apse_receipt_report_path).unwrap();
        report.as_object_mut().unwrap().insert(
            "reason".to_string(),
            json!("tampered nested apse-receipt reason"),
        );
        persist_json(&paths.apse_receipt_report_path, &report).unwrap();

        let verify =
            verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_retimed_nested_apse_receipt_report_and_step_generated_at() {
        let fixture = fake_command_fixture(
            "apse_receipt_retimed_nested_report",
            bedrock_certificate_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
        let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
        let mut report: serde_json::Value = load_json(&paths.apse_receipt_report_path).unwrap();
        let archived_generated_at =
            DateTime::parse_from_rfc3339(report["generated_at"].as_str().unwrap())
                .unwrap()
                .with_timezone(&Utc);
        let forged_generated_at = archived_generated_at + chrono::Duration::seconds(61);
        report.as_object_mut().unwrap().insert(
            "generated_at".to_string(),
            json!(forged_generated_at.to_rfc3339()),
        );
        persist_json(&paths.apse_receipt_report_path, &report).unwrap();

        let mut status: PackageSanctuaryCertificateStatus = load_json(&paths.status_path).unwrap();
        status.apse_receipt_step.as_mut().unwrap().generated_at = forged_generated_at;
        persist_json(&paths.status_path, &status).unwrap();

        let verify =
            verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_chain_ledger_registry_filing_archive_summit_closure_finality_or_consummation_identity_fields(
    ) {
        {
            let fixture = fake_command_fixture(
                "consummation_record_tampered_chain",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.chain_fingerprint_sha256 = "tampered-chain".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "consummation_record_tampered_ledger",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.ledger_seal_sha256 = "tampered-ledger".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "consummation_record_tampered_registry",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.registry_entry_sha256 = "tampered-registry".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "consummation_record_tampered_filing",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.filing_certificate_sha256 = "tampered-filing".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "consummation_record_tampered_archive",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.archive_receipt_sha256 = "tampered-archive".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "consummation_record_tampered_summit_identity",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.summit_certificate_sha256 = "tampered-summit".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "consummation_record_tampered_closure_identity",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.closure_certificate_sha256 = "tampered-closure".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "consummation_record_tampered_finality_identity",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.finality_receipt_sha256 = "tampered-finality".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "consummation_record_tampered_consummation_identity",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.consummation_record_sha256 = "tampered-consummation".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }
    }

    #[test]
    fn verify_rejects_tampered_bedrock_capstone_keystone_cornerstone_or_foundation_identity_fields()
    {
        {
            let fixture = fake_command_fixture(
                "sanctuary_certificate_tampered_bedrock_identity",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.bedrock_certificate_sha256 = "tampered-bedrock".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "sanctuary_certificate_tampered_capstone_identity",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.capstone_certificate_sha256 = "tampered-capstone".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "sanctuary_certificate_tampered_keystone_identity",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.keystone_receipt_sha256 = "tampered-keystone".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "sanctuary_certificate_tampered_cornerstone_identity",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.cornerstone_certificate_sha256 = "tampered-cornerstone".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "sanctuary_certificate_tampered_foundation_identity",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.foundation_receipt_sha256 = "tampered-foundation".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }
    }

    #[test]
    fn verify_rejects_tampered_basal_receipt_sha256() {
        let fixture = fake_command_fixture(
            "sanctuary_certificate_tampered_basal_receipt_sha256",
            consummation_record_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
        let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
        let mut status: PackageSanctuaryCertificateStatus = load_json(&paths.status_path).unwrap();
        status.basal_receipt_sha256 = "tampered-basal".to_string();
        persist_json(&paths.status_path, &status).unwrap();
        let verify =
            verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_substructure_certificate_sha256() {
        let fixture = fake_command_fixture(
            "sanctuary_certificate_tampered_substructure_certificate_sha256",
            consummation_record_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
        let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
        let mut status: PackageSanctuaryCertificateStatus = load_json(&paths.status_path).unwrap();
        status.substructure_certificate_sha256 = "tampered-substructure".to_string();
        persist_json(&paths.status_path, &status).unwrap();
        let verify =
            verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_substructure_certificate_summary_or_algorithm() {
        {
            let fixture = fake_command_fixture(
                "sanctuary_certificate_tampered_substructure_certificate_summary",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.substructure_certificate_summary = "tampered substructure summary".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "sanctuary_certificate_tampered_substructure_certificate_algorithm",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.substructure_certificate_algorithm =
                "tampered-substructure-algorithm".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }
    }

    #[test]
    fn verify_rejects_tampered_plinth_receipt_sha256_summary_or_algorithm() {
        {
            let fixture = fake_command_fixture(
                "sanctuary_certificate_tampered_plinth_receipt_sha256",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.plinth_receipt_sha256 = "tampered-plinth-receipt".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "sanctuary_certificate_tampered_plinth_receipt_summary",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.plinth_receipt_summary = "tampered plinth receipt summary".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "sanctuary_certificate_tampered_plinth_receipt_algorithm",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.plinth_receipt_algorithm = "tampered-plinth-receipt-algorithm".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }
    }

    #[test]
    fn verify_rejects_tampered_pedestal_certificate_sha256_summary_or_algorithm() {
        {
            let fixture = fake_command_fixture(
                "apse_receipt_tampered_pedestal_sha256",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.pedestal_certificate_sha256 = "tampered-pedestal".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "apse_receipt_tampered_pedestal_summary",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.pedestal_certificate_summary = "tampered pedestal summary".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "apse_receipt_tampered_pedestal_algorithm",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.pedestal_certificate_algorithm = "tampered-pedestal-algorithm".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }
    }

    #[test]
    fn verify_rejects_tampered_podium_receipt_sha256_summary_or_algorithm() {
        {
            let fixture = fake_command_fixture(
                "sanctuary_certificate_tampered_podium_receipt_sha256",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.podium_receipt_sha256 = "tampered-podium-receipt".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "sanctuary_certificate_tampered_podium_receipt_summary",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.podium_receipt_summary = "tampered podium receipt summary".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "sanctuary_certificate_tampered_podium_receipt_algorithm",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.podium_receipt_algorithm = "tampered-podium-receipt-algorithm".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }
    }

    #[test]
    fn verify_rejects_tampered_lectern_certificate_sha256() {
        let fixture = fake_command_fixture(
            "sanctuary_certificate_tampered_lectern_certificate_sha256",
            consummation_record_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
        let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
        let mut status: PackageSanctuaryCertificateStatus = load_json(&paths.status_path).unwrap();
        status.lectern_certificate_sha256 = "tampered-lectern-certificate".to_string();
        persist_json(&paths.status_path, &status).unwrap();
        let verify =
            verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_lectern_certificate_summary_or_algorithm() {
        {
            let fixture = fake_command_fixture(
                "sanctuary_certificate_tampered_lectern_certificate_summary",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.lectern_certificate_summary = "tampered lectern certificate summary".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "sanctuary_certificate_tampered_lectern_certificate_algorithm",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.lectern_certificate_algorithm =
                "tampered-lectern-certificate-algorithm".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }
    }

    #[test]
    fn verify_rejects_tampered_bedrock_certificate_summary_or_algorithm() {
        {
            let fixture = fake_command_fixture(
                "sanctuary_certificate_tampered_bedrock_certificate_summary",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.bedrock_certificate_summary = "tampered bedrock summary".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "sanctuary_certificate_tampered_bedrock_certificate_algorithm",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.bedrock_certificate_algorithm = "tampered-bedrock-algorithm".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }
    }

    #[test]
    fn verify_rejects_tampered_pulpit_receipt_sha256_summary_or_algorithm() {
        {
            let fixture = fake_command_fixture(
                "sanctuary_certificate_tampered_pulpit_receipt_sha256",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.pulpit_receipt_sha256 = "tampered-pulpit".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "sanctuary_certificate_tampered_pulpit_receipt_summary",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.pulpit_receipt_summary = "tampered pulpit summary".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "sanctuary_certificate_tampered_pulpit_receipt_algorithm",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.pulpit_receipt_algorithm = "tampered-pulpit-algorithm".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }
    }

    #[test]
    fn verify_rejects_tampered_chancel_certificate_sha256() {
        let fixture = fake_command_fixture(
            "sanctuary_certificate_tampered_chancel_certificate_sha256",
            consummation_record_verify_report(
                "ready_for_manual_execution_when_gate_turns_green",
                "ready_for_manual_go_live_review",
                "runnable_when_gate_truth_turns_green",
                "ready_for_manual_execution_when_gate_turns_green",
                "green",
                "ok",
            ),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
        let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
        let mut status: PackageSanctuaryCertificateStatus = load_json(&paths.status_path).unwrap();
        status.chancel_certificate_sha256 = "tampered-chancel-certificate-sha256".to_string();
        persist_json(&paths.status_path, &status).unwrap();
        let verify =
            verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_chancel_certificate_summary_or_algorithm() {
        {
            let fixture = fake_command_fixture(
                "sanctuary_certificate_tampered_chancel_certificate_summary",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.chancel_certificate_summary = "tampered chancel certificate summary".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "sanctuary_certificate_tampered_chancel_certificate_algorithm",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.chancel_certificate_algorithm =
                "tampered-chancel-certificate-algorithm".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }
    }

    #[test]
    fn verify_rejects_tampered_apse_receipt_sha256_summary_or_algorithm() {
        {
            let fixture = fake_command_fixture(
                "apse_receipt_tampered_apse_sha256",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.apse_receipt_sha256 = "tampered-apse-sha256".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "apse_receipt_tampered_apse_summary",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.apse_receipt_summary = "tampered apse summary".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "apse_receipt_tampered_apse_algorithm",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.apse_receipt_algorithm = "tampered-apse-algorithm".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }
    }

    #[test]
    fn verify_rejects_tampered_sanctuary_certificate_sha256_summary_or_algorithm() {
        {
            let fixture = fake_command_fixture(
                "sanctuary_certificate_tampered_sanctuary_sha256",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.sanctuary_certificate_sha256 = "tampered-sanctuary-sha256".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "sanctuary_certificate_tampered_sanctuary_summary",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.sanctuary_certificate_summary = "tampered sanctuary summary".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "sanctuary_certificate_tampered_sanctuary_algorithm",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.sanctuary_certificate_algorithm = "tampered-sanctuary-algorithm".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }
    }

    #[test]
    fn verify_rejects_tampered_closure_certificate_summary_or_algorithm() {
        {
            let fixture = fake_command_fixture(
                "consummation_record_tampered_closure_summary",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.closure_certificate_summary = "tampered closure summary".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "consummation_record_tampered_closure_algorithm",
                consummation_record_verify_report(
                    "ready_for_manual_execution_when_gate_turns_green",
                    "ready_for_manual_go_live_review",
                    "runnable_when_gate_truth_turns_green",
                    "ready_for_manual_execution_when_gate_turns_green",
                    "green",
                    "ok",
                ),
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_live_package_sanctuary_certificate_report(&fixture.run_config()).unwrap();
            let paths = sanctuary_certificate_paths(&fixture.apse_receipt_session_dir);
            let mut status: PackageSanctuaryCertificateStatus =
                load_json(&paths.status_path).unwrap();
            status.closure_certificate_algorithm = "sha512".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_sanctuary_certificate_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageSanctuaryCertificateVerdict::TinyLivePackageSanctuaryCertificateVerifyInvalid
            );
        }
    }

    struct FakeCommandFixture {
        bin_dir: PathBuf,
        apse_receipt_session_dir: PathBuf,
        input_apse_receipt_session_dir: PathBuf,
        decision_packet_session_dir: PathBuf,
        install_root: PathBuf,
    }

    impl FakeCommandFixture {
        fn run_config(&self) -> Config {
            Config {
                mode: Mode::RunLivePackageSanctuaryCertificate,
                apse_receipt_session_dir: self.input_apse_receipt_session_dir.clone(),
                confirmed_decision_packet_session_dir: Some(
                    self.decision_packet_session_dir.clone(),
                ),
                session_dir: Some(self.apse_receipt_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn verify_config(&self) -> Config {
            Config {
                mode: Mode::VerifyLivePackageSanctuaryCertificate,
                apse_receipt_session_dir: self.input_apse_receipt_session_dir.clone(),
                confirmed_decision_packet_session_dir: Some(
                    self.decision_packet_session_dir.clone(),
                ),
                session_dir: Some(self.apse_receipt_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }
    }

    fn fake_command_fixture(label: &str, verify_report: serde_json::Value) -> FakeCommandFixture {
        let fixture_dir = temp_dir(label);
        let bin_dir = fixture_dir.join("bin");
        fs::create_dir_all(&bin_dir).unwrap();
        let apse_receipt_session_dir = fixture_dir.join("sanctuary-certificate-session");
        let input_apse_receipt_session_dir = fixture_dir.join("apse-receipt-session");
        let lectern_certificate_session_dir = fixture_dir.join("lectern-certificate-session");
        let chancel_certificate_session_dir = fixture_dir.join("chancel-certificate-session");
        let rostrum_certificate_session_dir = fixture_dir.join("rostrum-certificate-session");
        let dais_receipt_session_dir = fixture_dir.join("dais-receipt-session");
        let pedestal_certificate_session_dir = fixture_dir.join("pedestal-certificate-session");
        let plinth_receipt_session_dir = fixture_dir.join("plinth-receipt-session");
        let substructure_certificate_session_dir =
            fixture_dir.join("substructure-certificate-session");
        let basal_receipt_session_dir = fixture_dir.join("basal-receipt-session");
        let bedrock_certificate_session_dir = fixture_dir.join("bedrock-certificate-session");
        let foundation_receipt_session_dir = fixture_dir.join("foundation-receipt-session");
        let cornerstone_certificate_session_dir =
            fixture_dir.join("cornerstone-certificate-session");
        let keystone_receipt_session_dir = fixture_dir.join("keystone-receipt-session");
        let summit_certificate_session_dir = fixture_dir.join("summit-certificate-session");
        let culmination_receipt_session_dir = fixture_dir.join("culmination-receipt-session");
        let finality_receipt_session_dir = fixture_dir.join("finality-receipt-session");
        let archive_receipt_session_dir = fixture_dir.join("archive-receipt-session");
        let registry_entry_session_dir = fixture_dir.join("registry-entry-session");
        let notarization_receipt_session_dir = fixture_dir.join("notarization-receipt-session");
        let provenance_certificate_session_dir = fixture_dir.join("provenance-certificate-session");
        let attestation_seal_session_dir = fixture_dir.join("attestation-seal-session");
        let release_capsule_session_dir = fixture_dir.join("release-capsule-session");
        let activation_ticket_session_dir = fixture_dir.join("activation-ticket-session");
        let review_receipt_session_dir = fixture_dir.join("review-receipt-session");
        let handoff_bundle_session_dir = fixture_dir.join("handoff-bundle-session");
        let decision_packet_session_dir = fixture_dir.join("decision-packet-session");
        let execute_frozen_session_dir = fixture_dir.join("execute-frozen-session");
        let turn_green_session_dir = fixture_dir.join("turn-green-session");
        let launch_packet_session_dir = fixture_dir.join("launch-packet-session");
        let package_dir = fixture_dir.join("package");
        let install_root = fixture_dir.join("live-root");
        let backend_command = fixture_dir.join("fake-backend");
        for path in [
            &input_apse_receipt_session_dir,
            &chancel_certificate_session_dir,
            &lectern_certificate_session_dir,
            &rostrum_certificate_session_dir,
            &dais_receipt_session_dir,
            &pedestal_certificate_session_dir,
            &plinth_receipt_session_dir,
            &substructure_certificate_session_dir,
            &basal_receipt_session_dir,
            &bedrock_certificate_session_dir,
            &foundation_receipt_session_dir,
            &cornerstone_certificate_session_dir,
            &keystone_receipt_session_dir,
            &summit_certificate_session_dir,
            &culmination_receipt_session_dir,
            &finality_receipt_session_dir,
            &archive_receipt_session_dir,
            &registry_entry_session_dir,
            &notarization_receipt_session_dir,
            &provenance_certificate_session_dir,
            &attestation_seal_session_dir,
            &release_capsule_session_dir,
            &activation_ticket_session_dir,
            &review_receipt_session_dir,
            &handoff_bundle_session_dir,
            &decision_packet_session_dir,
            &execute_frozen_session_dir,
            &turn_green_session_dir,
            &launch_packet_session_dir,
            &package_dir,
            &install_root,
        ] {
            fs::create_dir_all(path).unwrap();
        }

        persist_json(
            &input_apse_receipt_session_dir
                .join("tiny_live_activation_package_apse_receipt.session.json"),
            &stored_apse_receipt_session_fixture(
                &lectern_certificate_session_dir,
                &chancel_certificate_session_dir,
                &rostrum_certificate_session_dir,
                &pedestal_certificate_session_dir,
                &plinth_receipt_session_dir,
                &substructure_certificate_session_dir,
                &basal_receipt_session_dir,
                &bedrock_certificate_session_dir,
                &foundation_receipt_session_dir,
                &cornerstone_certificate_session_dir,
                &keystone_receipt_session_dir,
                &finality_receipt_session_dir,
                &archive_receipt_session_dir,
                &registry_entry_session_dir,
                &notarization_receipt_session_dir,
                &provenance_certificate_session_dir,
                &attestation_seal_session_dir,
                &release_capsule_session_dir,
                &activation_ticket_session_dir,
                &review_receipt_session_dir,
                &handoff_bundle_session_dir,
                &decision_packet_session_dir,
                &execute_frozen_session_dir,
                &turn_green_session_dir,
                &launch_packet_session_dir,
                &package_dir,
                &install_root,
                &backend_command,
                &fixture_dir.join("frozen-live-cutover-session"),
            ),
        )
        .unwrap();
        persist_json(
            &input_apse_receipt_session_dir
                .join("tiny_live_activation_package_apse_receipt.status.json"),
            &stored_apse_receipt_status_fixture(
                &lectern_certificate_session_dir,
                &chancel_certificate_session_dir,
                &rostrum_certificate_session_dir,
                &pedestal_certificate_session_dir,
                &plinth_receipt_session_dir,
                &substructure_certificate_session_dir,
                &verify_report,
            ),
        )
        .unwrap();
        persist_json(
            &input_apse_receipt_session_dir
                .join("tiny_live_activation_package_apse_receipt.chancel_certificate.report.json"),
            &json!({
                "decision_packet_session_dir": decision_packet_session_dir.display().to_string()
            }),
        )
        .unwrap();

        let mut verify_report = verify_report;
        bind_apse_receipt_report_to_fixture(
            &mut verify_report,
            &lectern_certificate_session_dir,
            &chancel_certificate_session_dir,
            &rostrum_certificate_session_dir,
            &dais_receipt_session_dir,
            &pedestal_certificate_session_dir,
            &plinth_receipt_session_dir,
            &substructure_certificate_session_dir,
            &basal_receipt_session_dir,
            &bedrock_certificate_session_dir,
            &foundation_receipt_session_dir,
            &cornerstone_certificate_session_dir,
            &keystone_receipt_session_dir,
            &finality_receipt_session_dir,
            &archive_receipt_session_dir,
            &registry_entry_session_dir,
            &notarization_receipt_session_dir,
            &provenance_certificate_session_dir,
            &attestation_seal_session_dir,
            &release_capsule_session_dir,
            &activation_ticket_session_dir,
            &review_receipt_session_dir,
            &handoff_bundle_session_dir,
            &decision_packet_session_dir,
            &execute_frozen_session_dir,
            &turn_green_session_dir,
            &launch_packet_session_dir,
            &package_dir,
            &install_root,
            &backend_command,
        );
        let verify_path = fixture_dir.join("apse-receipt.verify.json");
        persist_json(&verify_path, &verify_report).unwrap();
        write_fake_apse_receipt_verify_command(
            &bin_dir.join("copybot_tiny_live_activation_package_apse_receipt"),
            &verify_path,
            &chancel_certificate_session_dir,
            &decision_packet_session_dir,
            &input_apse_receipt_session_dir,
        );

        FakeCommandFixture {
            bin_dir,
            apse_receipt_session_dir,
            input_apse_receipt_session_dir,
            decision_packet_session_dir,
            install_root,
        }
    }

    fn bind_apse_receipt_report_to_fixture(
        verify_report: &mut serde_json::Value,
        lectern_certificate_session_dir: &Path,
        chancel_certificate_session_dir: &Path,
        rostrum_certificate_session_dir: &Path,
        dais_receipt_session_dir: &Path,
        pedestal_certificate_session_dir: &Path,
        plinth_receipt_session_dir: &Path,
        substructure_certificate_session_dir: &Path,
        basal_receipt_session_dir: &Path,
        bedrock_certificate_session_dir: &Path,
        foundation_receipt_session_dir: &Path,
        cornerstone_certificate_session_dir: &Path,
        keystone_receipt_session_dir: &Path,
        finality_receipt_session_dir: &Path,
        archive_receipt_session_dir: &Path,
        registry_entry_session_dir: &Path,
        notarization_receipt_session_dir: &Path,
        provenance_certificate_session_dir: &Path,
        attestation_seal_session_dir: &Path,
        release_capsule_session_dir: &Path,
        activation_ticket_session_dir: &Path,
        review_receipt_session_dir: &Path,
        handoff_bundle_session_dir: &Path,
        decision_packet_session_dir: &Path,
        execute_frozen_session_dir: &Path,
        turn_green_session_dir: &Path,
        launch_packet_session_dir: &Path,
        package_dir: &Path,
        install_root: &Path,
        backend_command: &Path,
    ) {
        let object = verify_report.as_object_mut().unwrap();
        object.insert(
            "lectern_certificate_session_dir".to_string(),
            json!(lectern_certificate_session_dir.display().to_string()),
        );
        object.insert(
            "chancel_certificate_session_dir".to_string(),
            json!(chancel_certificate_session_dir.display().to_string()),
        );
        object.insert(
            "rostrum_certificate_session_dir".to_string(),
            json!(rostrum_certificate_session_dir.display().to_string()),
        );
        object.insert(
            "dais_receipt_session_dir".to_string(),
            json!(dais_receipt_session_dir.display().to_string()),
        );
        object.insert(
            "pedestal_certificate_session_dir".to_string(),
            json!(pedestal_certificate_session_dir.display().to_string()),
        );
        object.insert(
            "plinth_receipt_session_dir".to_string(),
            json!(plinth_receipt_session_dir.display().to_string()),
        );
        object.insert(
            "substructure_certificate_session_dir".to_string(),
            json!(substructure_certificate_session_dir.display().to_string()),
        );
        object.insert(
            "basal_receipt_session_dir".to_string(),
            json!(basal_receipt_session_dir.display().to_string()),
        );
        object.insert(
            "bedrock_certificate_session_dir".to_string(),
            json!(bedrock_certificate_session_dir.display().to_string()),
        );
        object.insert(
            "foundation_receipt_session_dir".to_string(),
            json!(foundation_receipt_session_dir.display().to_string()),
        );
        object.insert(
            "cornerstone_certificate_session_dir".to_string(),
            json!(cornerstone_certificate_session_dir.display().to_string()),
        );
        object.insert(
            "keystone_receipt_session_dir".to_string(),
            json!(keystone_receipt_session_dir.display().to_string()),
        );
        object.insert(
            "finality_receipt_session_dir".to_string(),
            json!(finality_receipt_session_dir.display().to_string()),
        );
        object.insert(
            "archive_receipt_session_dir".to_string(),
            json!(archive_receipt_session_dir.display().to_string()),
        );
        object.insert(
            "registry_entry_session_dir".to_string(),
            json!(registry_entry_session_dir.display().to_string()),
        );
        object.insert(
            "notarization_receipt_session_dir".to_string(),
            json!(notarization_receipt_session_dir.display().to_string()),
        );
        object.insert(
            "provenance_certificate_session_dir".to_string(),
            json!(provenance_certificate_session_dir.display().to_string()),
        );
        object.insert(
            "attestation_seal_session_dir".to_string(),
            json!(attestation_seal_session_dir.display().to_string()),
        );
        object.insert(
            "release_capsule_session_dir".to_string(),
            json!(release_capsule_session_dir.display().to_string()),
        );
        object.insert(
            "activation_ticket_session_dir".to_string(),
            json!(activation_ticket_session_dir.display().to_string()),
        );
        object.insert(
            "review_receipt_session_dir".to_string(),
            json!(review_receipt_session_dir.display().to_string()),
        );
        object.insert(
            "handoff_bundle_session_dir".to_string(),
            json!(handoff_bundle_session_dir.display().to_string()),
        );
        object.insert(
            "decision_packet_session_dir".to_string(),
            json!(decision_packet_session_dir.display().to_string()),
        );
        object.insert(
            "execute_frozen_session_dir".to_string(),
            json!(execute_frozen_session_dir.display().to_string()),
        );
        object.insert(
            "turn_green_session_dir".to_string(),
            json!(turn_green_session_dir.display().to_string()),
        );
        object.insert(
            "launch_packet_session_dir".to_string(),
            json!(launch_packet_session_dir.display().to_string()),
        );
        object.insert(
            "package_dir".to_string(),
            json!(package_dir.display().to_string()),
        );
        object.insert(
            "install_root".to_string(),
            json!(install_root.display().to_string()),
        );
        object.insert(
            "target_service_name".to_string(),
            json!("solana-copy-bot.service"),
        );
        object.insert(
            "backend_command".to_string(),
            json!(backend_command.display().to_string()),
        );
        object.insert("wrapper_timeout_ms".to_string(), json!(1200_u64));
        object.insert(
            "service_status_max_staleness_ms".to_string(),
            json!(4500_u64),
        );
        object.insert(
            "verify_chancel_certificate_command_summary".to_string(),
            json!(format!(
                "verify immutable chancel-certificate session under {} before freezing the final apse receipt",
                chancel_certificate_session_dir.display()
            )),
        );
        object.insert(
            "reviewed_frozen_live_cutover_controller_command_summary".to_string(),
            json!(format!(
                "copybot_tiny_live_activation_package_live_cutover --package-dir {} --install-root {} --target-service solana-copy-bot.service --backend-command {} --wrapper-timeout-ms 1200 --service-status-max-staleness-ms 4500 --session-dir {} --run-live-package-cutover",
                shell_escape_path(package_dir),
                shell_escape_path(install_root),
                shell_escape_path(backend_command),
                shell_escape_path(&attestation_seal_session_dir.join("frozen-live-cutover-session")),
            )),
        );
        object.insert(
            "provenance_certificate_summary".to_string(),
            json!("Provenance certificate summary."),
        );
        object.insert(
            "chain_fingerprint_summary".to_string(),
            json!("Canonical chain fingerprint summary."),
        );
        object.insert(
            "chain_fingerprint_sha256".to_string(),
            json!("canonical-chain-sha256"),
        );
        object.insert("chain_fingerprint_algorithm".to_string(), json!("sha256"));
        object.insert(
            "release_capsule_digest_manifest_sha256".to_string(),
            json!("release-capsule-manifest-sha256"),
        );
        object.insert(
            "release_capsule_digest_manifest_entry_count".to_string(),
            json!(3),
        );
        object.insert(
            "release_capsule_digest_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "notarization_receipt_summary".to_string(),
            json!("Notarization receipt summary."),
        );
        object.insert(
            "ledger_seal_summary".to_string(),
            json!("Ledger seal summary."),
        );
        object.insert(
            "ledger_seal_sha256".to_string(),
            json!("ledger-seal-sha256"),
        );
        object.insert("ledger_seal_algorithm".to_string(), json!("sha256"));
        object.insert(
            "registry_entry_summary".to_string(),
            json!("Registry entry summary."),
        );
        object.insert(
            "registry_entry_sha256".to_string(),
            json!("registry-entry-sha256"),
        );
        object.insert("registry_entry_algorithm".to_string(), json!("sha256"));
        object.insert(
            "filing_certificate_summary".to_string(),
            json!("Filing certificate summary."),
        );
        object.insert(
            "filing_certificate_sha256".to_string(),
            json!("filing-certificate-sha256"),
        );
        object.insert("filing_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "archive_receipt_summary".to_string(),
            json!("Archive receipt summary."),
        );
        object.insert(
            "archive_receipt_sha256".to_string(),
            json!("archive-receipt-sha256"),
        );
        object.insert("archive_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "closure_certificate_summary".to_string(),
            json!("Closure certificate summary."),
        );
        object.insert(
            "closure_certificate_sha256".to_string(),
            json!("closure-certificate-sha256"),
        );
        object.insert("closure_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "finality_receipt_summary".to_string(),
            json!("Finality receipt summary."),
        );
        object.insert(
            "finality_receipt_sha256".to_string(),
            json!("finality-receipt-sha256"),
        );
        object.insert("finality_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "consummation_record_summary".to_string(),
            json!("Consummation record summary."),
        );
        object.insert(
            "consummation_record_sha256".to_string(),
            json!("consummation-record-sha256"),
        );
        object.insert("consummation_record_algorithm".to_string(), json!("sha256"));
        object.insert(
            "completion_certificate_summary".to_string(),
            json!("Completion certificate summary."),
        );
        object.insert(
            "completion_certificate_sha256".to_string(),
            json!("completion-certificate-sha256"),
        );
        object.insert(
            "completion_certificate_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "summit_certificate_summary".to_string(),
            json!("Summit certificate summary."),
        );
        object.insert(
            "summit_certificate_sha256".to_string(),
            json!("summit-certificate-sha256"),
        );
        object.insert("summit_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "culmination_receipt_summary".to_string(),
            json!("Culmination receipt summary."),
        );
        object.insert(
            "culmination_receipt_sha256".to_string(),
            json!("culmination-receipt-sha256"),
        );
        object.insert("culmination_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "pinnacle_receipt_summary".to_string(),
            json!("Pinnacle receipt summary."),
        );
        object.insert(
            "pinnacle_receipt_sha256".to_string(),
            json!("pinnacle-receipt-sha256"),
        );
        object.insert("pinnacle_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "capstone_certificate_summary".to_string(),
            json!("Capstone certificate summary."),
        );
        object.insert(
            "capstone_certificate_sha256".to_string(),
            json!("capstone-certificate-sha256"),
        );
        object.insert(
            "capstone_certificate_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "keystone_receipt_summary".to_string(),
            json!("Keystone receipt summary."),
        );
        object.insert(
            "keystone_receipt_sha256".to_string(),
            json!("keystone-receipt-sha256"),
        );
        object.insert("keystone_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "cornerstone_certificate_summary".to_string(),
            json!("Cornerstone certificate summary."),
        );
        object.insert(
            "cornerstone_certificate_sha256".to_string(),
            json!("cornerstone-certificate-sha256"),
        );
        object.insert(
            "cornerstone_certificate_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "foundation_receipt_summary".to_string(),
            json!("Foundation receipt summary."),
        );
        object.insert(
            "foundation_receipt_sha256".to_string(),
            json!("foundation-receipt-sha256"),
        );
        object.insert("foundation_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "bedrock_certificate_summary".to_string(),
            json!("Bedrock certificate summary."),
        );
        object.insert(
            "bedrock_certificate_sha256".to_string(),
            json!("bedrock-certificate-sha256"),
        );
        object.insert("bedrock_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "basal_receipt_summary".to_string(),
            json!("Basal receipt summary."),
        );
        object.insert(
            "basal_receipt_sha256".to_string(),
            json!("basal-receipt-sha256"),
        );
        object.insert("basal_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "substructure_certificate_summary".to_string(),
            json!("Substructure certificate summary."),
        );
        object.insert(
            "substructure_certificate_sha256".to_string(),
            json!("substructure-certificate-sha256"),
        );
        object.insert(
            "substructure_certificate_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "plinth_receipt_summary".to_string(),
            json!("Plinth receipt summary."),
        );
        object.insert(
            "plinth_receipt_sha256".to_string(),
            json!("plinth-receipt-sha256"),
        );
        object.insert("plinth_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "pedestal_certificate_summary".to_string(),
            json!("Pedestal certificate summary."),
        );
        object.insert(
            "pedestal_certificate_sha256".to_string(),
            json!("pedestal-certificate-sha256"),
        );
        object.insert(
            "pedestal_certificate_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "dais_receipt_summary".to_string(),
            json!("Dais receipt summary."),
        );
        object.insert(
            "dais_receipt_sha256".to_string(),
            json!("dais-receipt-sha256"),
        );
        object.insert("dais_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "rostrum_certificate_summary".to_string(),
            json!("Rostrum certificate summary."),
        );
        object.insert(
            "rostrum_certificate_sha256".to_string(),
            json!("rostrum-certificate-sha256"),
        );
        object.insert("rostrum_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "podium_receipt_summary".to_string(),
            json!("Podium receipt summary."),
        );
        object.insert(
            "podium_receipt_sha256".to_string(),
            json!("podium-receipt-sha256"),
        );
        object.insert("podium_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "lectern_certificate_summary".to_string(),
            json!("Lectern certificate summary."),
        );
        object.insert(
            "lectern_certificate_sha256".to_string(),
            json!("lectern-certificate-sha256"),
        );
        object.insert("lectern_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "pulpit_receipt_summary".to_string(),
            json!("Pulpit receipt summary."),
        );
        object.insert("pulpit_receipt_sha256".to_string(), json!("crest-sha256"));
        object.insert("pulpit_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "chancel_certificate_summary".to_string(),
            json!("Chancel certificate summary."),
        );
        object.insert(
            "chancel_certificate_sha256".to_string(),
            json!("herald-certificate-sha256"),
        );
        object.insert("chancel_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "apse_receipt_summary".to_string(),
            json!("Apse receipt summary."),
        );
        object.insert("apse_receipt_sha256".to_string(), json!("herald-sha256"));
        object.insert("apse_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "explicit_statement".to_string(),
            json!("apse-receipt verify statement"),
        );
    }

    fn apse_receipt_verify_report(
        result: &str,
        handoff_bundle_result: &str,
        decision_packet_result: &str,
        execute_frozen_result: &str,
        gate_verdict: &str,
        gate_reason: &str,
    ) -> serde_json::Value {
        json!({
            "generated_at": Utc::now(),
            "mode": "verify_live_package_apse_receipt",
            "verdict": "tiny_live_package_apse_receipt_verify_ok",
            "reason": "apse-receipt chain is coherent",
            "result": result,
            "handoff_bundle_result": handoff_bundle_result,
            "decision_packet_result": decision_packet_result,
            "execute_frozen_result": execute_frozen_result,
            "current_pre_activation_gate_verdict": gate_verdict,
            "current_pre_activation_gate_reason": gate_reason
        })
    }

    fn bedrock_certificate_verify_report(
        result: &str,
        handoff_bundle_result: &str,
        decision_packet_result: &str,
        execute_frozen_result: &str,
        gate_verdict: &str,
        gate_reason: &str,
    ) -> serde_json::Value {
        apse_receipt_verify_report(
            result,
            handoff_bundle_result,
            decision_packet_result,
            execute_frozen_result,
            gate_verdict,
            gate_reason,
        )
    }

    fn capstone_fixture_verify_report(
        result: &str,
        handoff_bundle_result: &str,
        decision_packet_result: &str,
        execute_frozen_result: &str,
        gate_verdict: &str,
        gate_reason: &str,
    ) -> serde_json::Value {
        apse_receipt_verify_report(
            result,
            handoff_bundle_result,
            decision_packet_result,
            execute_frozen_result,
            gate_verdict,
            gate_reason,
        )
    }

    fn consummation_record_verify_report(
        result: &str,
        handoff_bundle_result: &str,
        decision_packet_result: &str,
        execute_frozen_result: &str,
        gate_verdict: &str,
        gate_reason: &str,
    ) -> serde_json::Value {
        capstone_fixture_verify_report(
            result,
            handoff_bundle_result,
            decision_packet_result,
            execute_frozen_result,
            gate_verdict,
            gate_reason,
        )
    }

    fn write_fake_apse_receipt_verify_command(
        path: &Path,
        verify_path: &Path,
        _expected_chancel_certificate_session_dir: &Path,
        expected_decision_packet_session_dir: &Path,
        expected_apse_receipt_session_dir: &Path,
    ) {
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --chancel-certificate-session-dir \"*) : ;;\n  *) echo 'unexpected chancel-certificate-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --confirm-decision-packet-session-dir {} \"*) : ;;\n  *) echo 'unexpected decision-packet-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --session-dir {} \"*) : ;;\n  *) echo 'unexpected apse-receipt session dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --verify-live-package-apse-receipt \"*) : ;;\n  *) echo 'missing apse-receipt verify mode' >&2; exit 1;;\nesac\ncat '{}'\n",
            shell_escape_path(expected_decision_packet_session_dir),
            shell_escape_path(expected_apse_receipt_session_dir),
            verify_path.display(),
        );
        fs::write(path, script).unwrap();
        let mut perms = fs::metadata(path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).unwrap();
    }

    struct PathGuard {
        original_path: OsString,
        _lock: MutexGuard<'static, ()>,
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

    fn stored_apse_receipt_session_fixture(
        lectern_certificate_session_dir: &Path,
        chancel_certificate_session_dir: &Path,
        _rostrum_certificate_session_dir: &Path,
        pedestal_certificate_session_dir: &Path,
        plinth_receipt_session_dir: &Path,
        substructure_certificate_session_dir: &Path,
        basal_receipt_session_dir: &Path,
        bedrock_certificate_session_dir: &Path,
        foundation_receipt_session_dir: &Path,
        cornerstone_certificate_session_dir: &Path,
        keystone_receipt_session_dir: &Path,
        finality_receipt_session_dir: &Path,
        archive_receipt_session_dir: &Path,
        registry_entry_session_dir: &Path,
        notarization_receipt_session_dir: &Path,
        provenance_certificate_session_dir: &Path,
        attestation_seal_session_dir: &Path,
        release_capsule_session_dir: &Path,
        activation_ticket_session_dir: &Path,
        review_receipt_session_dir: &Path,
        handoff_bundle_session_dir: &Path,
        decision_packet_session_dir: &Path,
        execute_frozen_session_dir: &Path,
        turn_green_session_dir: &Path,
        launch_packet_session_dir: &Path,
        package_dir: &Path,
        install_root: &Path,
        backend_command: &Path,
        frozen_live_cutover_session_dir: &Path,
    ) -> Value {
        let mut object = Map::new();
        object.insert(
            "lectern_certificate_session_dir".to_string(),
            json!(lectern_certificate_session_dir.display().to_string()),
        );
        object.insert(
            "chancel_certificate_session_dir".to_string(),
            json!(chancel_certificate_session_dir.display().to_string()),
        );
        object.insert(
            "dais_receipt_session_dir".to_string(),
            json!(lectern_certificate_session_dir
                .parent()
                .unwrap()
                .join("dais-receipt-session")
                .display()
                .to_string()),
        );
        object.insert(
            "pedestal_certificate_session_dir".to_string(),
            json!(pedestal_certificate_session_dir.display().to_string()),
        );
        object.insert(
            "plinth_receipt_session_dir".to_string(),
            json!(plinth_receipt_session_dir.display().to_string()),
        );
        object.insert(
            "substructure_certificate_session_dir".to_string(),
            json!(substructure_certificate_session_dir.display().to_string()),
        );
        object.insert(
            "basal_receipt_session_dir".to_string(),
            json!(basal_receipt_session_dir.display().to_string()),
        );
        object.insert(
            "bedrock_certificate_session_dir".to_string(),
            json!(bedrock_certificate_session_dir.display().to_string()),
        );
        object.insert(
            "foundation_receipt_session_dir".to_string(),
            json!(foundation_receipt_session_dir.display().to_string()),
        );
        object.insert(
            "cornerstone_certificate_session_dir".to_string(),
            json!(cornerstone_certificate_session_dir.display().to_string()),
        );
        object.insert(
            "keystone_receipt_session_dir".to_string(),
            json!(keystone_receipt_session_dir.display().to_string()),
        );
        object.insert(
            "finality_receipt_session_dir".to_string(),
            json!(finality_receipt_session_dir.display().to_string()),
        );
        object.insert(
            "archive_receipt_session_dir".to_string(),
            json!(archive_receipt_session_dir.display().to_string()),
        );
        object.insert(
            "registry_entry_session_dir".to_string(),
            json!(registry_entry_session_dir.display().to_string()),
        );
        object.insert(
            "notarization_receipt_session_dir".to_string(),
            json!(notarization_receipt_session_dir.display().to_string()),
        );
        object.insert(
            "provenance_certificate_session_dir".to_string(),
            json!(provenance_certificate_session_dir.display().to_string()),
        );
        object.insert(
            "attestation_seal_session_dir".to_string(),
            json!(attestation_seal_session_dir.display().to_string()),
        );
        object.insert(
            "release_capsule_session_dir".to_string(),
            json!(release_capsule_session_dir.display().to_string()),
        );
        object.insert(
            "activation_ticket_session_dir".to_string(),
            json!(activation_ticket_session_dir.display().to_string()),
        );
        object.insert(
            "review_receipt_session_dir".to_string(),
            json!(review_receipt_session_dir.display().to_string()),
        );
        object.insert(
            "handoff_bundle_session_dir".to_string(),
            json!(handoff_bundle_session_dir.display().to_string()),
        );
        object.insert(
            "decision_packet_session_dir".to_string(),
            json!(decision_packet_session_dir.display().to_string()),
        );
        object.insert(
            "execute_frozen_session_dir".to_string(),
            json!(execute_frozen_session_dir.display().to_string()),
        );
        object.insert(
            "turn_green_session_dir".to_string(),
            json!(turn_green_session_dir.display().to_string()),
        );
        object.insert(
            "launch_packet_session_dir".to_string(),
            json!(launch_packet_session_dir.display().to_string()),
        );
        object.insert(
            "package_dir".to_string(),
            json!(package_dir.display().to_string()),
        );
        object.insert(
            "install_root".to_string(),
            json!(install_root.display().to_string()),
        );
        object.insert(
            "target_service_name".to_string(),
            json!("solana-copy-bot.service"),
        );
        object.insert(
            "backend_command".to_string(),
            json!(backend_command.display().to_string()),
        );
        object.insert("wrapper_timeout_ms".to_string(), json!(1200));
        object.insert("service_status_max_staleness_ms".to_string(), json!(4500));
        object.insert(
            "verify_chancel_certificate_command_summary".to_string(),
            json!(format!(
                "verify immutable chancel-certificate session under {} before freezing the final apse receipt",
                chancel_certificate_session_dir.display()
            )),
        );
        object.insert(
            "reviewed_frozen_live_cutover_controller_command_summary".to_string(),
            json!(format!(
                "copybot_tiny_live_activation_package_live_cutover --package-dir {} --install-root {} --target-service solana-copy-bot.service --backend-command {} --wrapper-timeout-ms 1200 --service-status-max-staleness-ms 4500 --session-dir {} --run-live-package-cutover",
                shell_escape_path(package_dir),
                shell_escape_path(install_root),
                shell_escape_path(backend_command),
                shell_escape_path(frozen_live_cutover_session_dir),
            )),
        );
        object.insert(
            "provenance_certificate_summary".to_string(),
            json!("Provenance certificate summary."),
        );
        object.insert(
            "chain_fingerprint_summary".to_string(),
            json!("Canonical chain fingerprint summary."),
        );
        object.insert(
            "chain_fingerprint_sha256".to_string(),
            json!("canonical-chain-sha256"),
        );
        object.insert("chain_fingerprint_algorithm".to_string(), json!("sha256"));
        object.insert(
            "release_capsule_digest_manifest_sha256".to_string(),
            json!("release-capsule-manifest-sha256"),
        );
        object.insert(
            "release_capsule_digest_manifest_entry_count".to_string(),
            json!(3),
        );
        object.insert(
            "release_capsule_digest_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "notarization_receipt_summary".to_string(),
            json!("Notarization receipt summary."),
        );
        object.insert(
            "ledger_seal_summary".to_string(),
            json!("Ledger seal summary."),
        );
        object.insert(
            "ledger_seal_sha256".to_string(),
            json!("ledger-seal-sha256"),
        );
        object.insert("ledger_seal_algorithm".to_string(), json!("sha256"));
        object.insert(
            "registry_entry_summary".to_string(),
            json!("Registry entry summary."),
        );
        object.insert(
            "registry_entry_sha256".to_string(),
            json!("registry-entry-sha256"),
        );
        object.insert("registry_entry_algorithm".to_string(), json!("sha256"));
        object.insert(
            "filing_certificate_summary".to_string(),
            json!("Filing certificate summary."),
        );
        object.insert(
            "filing_certificate_sha256".to_string(),
            json!("filing-certificate-sha256"),
        );
        object.insert("filing_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "archive_receipt_summary".to_string(),
            json!("Archive receipt summary."),
        );
        object.insert(
            "archive_receipt_sha256".to_string(),
            json!("archive-receipt-sha256"),
        );
        object.insert("archive_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "closure_certificate_summary".to_string(),
            json!("Closure certificate summary."),
        );
        object.insert(
            "closure_certificate_sha256".to_string(),
            json!("closure-certificate-sha256"),
        );
        object.insert("closure_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "finality_receipt_summary".to_string(),
            json!("Finality receipt summary."),
        );
        object.insert(
            "finality_receipt_sha256".to_string(),
            json!("finality-receipt-sha256"),
        );
        object.insert("finality_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "consummation_record_summary".to_string(),
            json!("Consummation record summary."),
        );
        object.insert(
            "consummation_record_sha256".to_string(),
            json!("consummation-record-sha256"),
        );
        object.insert("consummation_record_algorithm".to_string(), json!("sha256"));
        object.insert(
            "completion_certificate_summary".to_string(),
            json!("Completion certificate summary."),
        );
        object.insert(
            "completion_certificate_sha256".to_string(),
            json!("completion-certificate-sha256"),
        );
        object.insert(
            "completion_certificate_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "summit_certificate_summary".to_string(),
            json!("Summit certificate summary."),
        );
        object.insert(
            "summit_certificate_sha256".to_string(),
            json!("summit-certificate-sha256"),
        );
        object.insert("summit_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "culmination_receipt_summary".to_string(),
            json!("Culmination receipt summary."),
        );
        object.insert(
            "culmination_receipt_sha256".to_string(),
            json!("culmination-receipt-sha256"),
        );
        object.insert("culmination_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "pinnacle_receipt_summary".to_string(),
            json!("Pinnacle receipt summary."),
        );
        object.insert(
            "pinnacle_receipt_sha256".to_string(),
            json!("pinnacle-receipt-sha256"),
        );
        object.insert("pinnacle_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "capstone_certificate_summary".to_string(),
            json!("Capstone certificate summary."),
        );
        object.insert(
            "capstone_certificate_sha256".to_string(),
            json!("capstone-certificate-sha256"),
        );
        object.insert(
            "capstone_certificate_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "keystone_receipt_summary".to_string(),
            json!("Keystone receipt summary."),
        );
        object.insert(
            "keystone_receipt_sha256".to_string(),
            json!("keystone-receipt-sha256"),
        );
        object.insert("keystone_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "cornerstone_certificate_summary".to_string(),
            json!("Cornerstone certificate summary."),
        );
        object.insert(
            "cornerstone_certificate_sha256".to_string(),
            json!("cornerstone-certificate-sha256"),
        );
        object.insert(
            "cornerstone_certificate_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "foundation_receipt_summary".to_string(),
            json!("Foundation receipt summary."),
        );
        object.insert(
            "foundation_receipt_sha256".to_string(),
            json!("foundation-receipt-sha256"),
        );
        object.insert("foundation_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "bedrock_certificate_summary".to_string(),
            json!("Bedrock certificate summary."),
        );
        object.insert(
            "bedrock_certificate_sha256".to_string(),
            json!("bedrock-certificate-sha256"),
        );
        object.insert("bedrock_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "basal_receipt_summary".to_string(),
            json!("Basal receipt summary."),
        );
        object.insert(
            "basal_receipt_sha256".to_string(),
            json!("basal-receipt-sha256"),
        );
        object.insert("basal_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "substructure_certificate_summary".to_string(),
            json!("Substructure certificate summary."),
        );
        object.insert(
            "substructure_certificate_sha256".to_string(),
            json!("substructure-certificate-sha256"),
        );
        object.insert(
            "substructure_certificate_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "plinth_receipt_summary".to_string(),
            json!("Plinth receipt summary."),
        );
        object.insert(
            "plinth_receipt_sha256".to_string(),
            json!("plinth-receipt-sha256"),
        );
        object.insert("plinth_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "pedestal_certificate_summary".to_string(),
            json!("Pedestal certificate summary."),
        );
        object.insert(
            "pedestal_certificate_sha256".to_string(),
            json!("pedestal-certificate-sha256"),
        );
        object.insert(
            "pedestal_certificate_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "dais_receipt_summary".to_string(),
            json!("Dais receipt summary."),
        );
        object.insert(
            "dais_receipt_sha256".to_string(),
            json!("dais-receipt-sha256"),
        );
        object.insert("dais_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "rostrum_certificate_summary".to_string(),
            json!("Rostrum certificate summary."),
        );
        object.insert(
            "rostrum_certificate_sha256".to_string(),
            json!("rostrum-certificate-sha256"),
        );
        object.insert("rostrum_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "podium_receipt_summary".to_string(),
            json!("Podium receipt summary."),
        );
        object.insert(
            "podium_receipt_sha256".to_string(),
            json!("podium-receipt-sha256"),
        );
        object.insert("podium_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "lectern_certificate_summary".to_string(),
            json!("Lectern certificate summary."),
        );
        object.insert(
            "lectern_certificate_sha256".to_string(),
            json!("lectern-certificate-sha256"),
        );
        object.insert("lectern_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "pulpit_receipt_summary".to_string(),
            json!("Pulpit receipt summary."),
        );
        object.insert("pulpit_receipt_sha256".to_string(), json!("crest-sha256"));
        object.insert("pulpit_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "chancel_certificate_summary".to_string(),
            json!("Chancel certificate summary."),
        );
        object.insert(
            "chancel_certificate_sha256".to_string(),
            json!("herald-certificate-sha256"),
        );
        object.insert("chancel_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "apse_receipt_summary".to_string(),
            json!("Apse receipt summary."),
        );
        object.insert("apse_receipt_sha256".to_string(), json!("herald-sha256"));
        object.insert("apse_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "explicit_statement".to_string(),
            json!("apse-receipt statement"),
        );
        Value::Object(object)
    }

    fn stored_apse_receipt_status_fixture(
        lectern_certificate_session_dir: &Path,
        chancel_certificate_session_dir: &Path,
        _rostrum_certificate_session_dir: &Path,
        pedestal_certificate_session_dir: &Path,
        plinth_receipt_session_dir: &Path,
        substructure_certificate_session_dir: &Path,
        verify_report: &Value,
    ) -> Value {
        let mut object = Map::new();
        object.insert(
            "lectern_certificate_session_dir".to_string(),
            json!(lectern_certificate_session_dir.display().to_string()),
        );
        object.insert(
            "chancel_certificate_session_dir".to_string(),
            json!(chancel_certificate_session_dir.display().to_string()),
        );
        object.insert(
            "dais_receipt_session_dir".to_string(),
            json!(lectern_certificate_session_dir
                .parent()
                .unwrap()
                .join("dais-receipt-session")
                .display()
                .to_string()),
        );
        object.insert(
            "pedestal_certificate_session_dir".to_string(),
            json!(pedestal_certificate_session_dir.display().to_string()),
        );
        object.insert(
            "plinth_receipt_session_dir".to_string(),
            json!(plinth_receipt_session_dir.display().to_string()),
        );
        object.insert(
            "substructure_certificate_session_dir".to_string(),
            json!(substructure_certificate_session_dir.display().to_string()),
        );
        object.insert(
            "result".to_string(),
            json!(verify_report
                .get("result")
                .and_then(|value| value.as_str())
                .unwrap()),
        );
        object.insert(
            "handoff_bundle_result".to_string(),
            json!(verify_report
                .get("handoff_bundle_result")
                .and_then(|value| value.as_str())),
        );
        object.insert(
            "decision_packet_result".to_string(),
            json!(verify_report
                .get("decision_packet_result")
                .and_then(|value| value.as_str())),
        );
        object.insert(
            "execute_frozen_result".to_string(),
            json!(verify_report
                .get("execute_frozen_result")
                .and_then(|value| value.as_str())),
        );
        object.insert(
            "current_pre_activation_gate_verdict".to_string(),
            json!(verify_report
                .get("current_pre_activation_gate_verdict")
                .and_then(|value| value.as_str())),
        );
        object.insert(
            "current_pre_activation_gate_reason".to_string(),
            json!(verify_report
                .get("current_pre_activation_gate_reason")
                .and_then(|value| value.as_str())),
        );
        object.insert(
            "provenance_certificate_summary".to_string(),
            json!("Provenance certificate summary."),
        );
        object.insert(
            "chain_fingerprint_summary".to_string(),
            json!("Canonical chain fingerprint summary."),
        );
        object.insert(
            "chain_fingerprint_sha256".to_string(),
            json!("canonical-chain-sha256"),
        );
        object.insert("chain_fingerprint_algorithm".to_string(), json!("sha256"));
        object.insert(
            "release_capsule_digest_manifest_sha256".to_string(),
            json!("release-capsule-manifest-sha256"),
        );
        object.insert(
            "release_capsule_digest_manifest_entry_count".to_string(),
            json!(3),
        );
        object.insert(
            "release_capsule_digest_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "notarization_receipt_summary".to_string(),
            json!("Notarization receipt summary."),
        );
        object.insert(
            "ledger_seal_summary".to_string(),
            json!("Ledger seal summary."),
        );
        object.insert(
            "ledger_seal_sha256".to_string(),
            json!("ledger-seal-sha256"),
        );
        object.insert("ledger_seal_algorithm".to_string(), json!("sha256"));
        object.insert(
            "registry_entry_summary".to_string(),
            json!("Registry entry summary."),
        );
        object.insert(
            "registry_entry_sha256".to_string(),
            json!("registry-entry-sha256"),
        );
        object.insert("registry_entry_algorithm".to_string(), json!("sha256"));
        object.insert(
            "filing_certificate_summary".to_string(),
            json!("Filing certificate summary."),
        );
        object.insert(
            "filing_certificate_sha256".to_string(),
            json!("filing-certificate-sha256"),
        );
        object.insert("filing_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "archive_receipt_summary".to_string(),
            json!("Archive receipt summary."),
        );
        object.insert(
            "archive_receipt_sha256".to_string(),
            json!("archive-receipt-sha256"),
        );
        object.insert("archive_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "closure_certificate_summary".to_string(),
            json!("Closure certificate summary."),
        );
        object.insert(
            "closure_certificate_sha256".to_string(),
            json!("closure-certificate-sha256"),
        );
        object.insert("closure_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "finality_receipt_summary".to_string(),
            json!("Finality receipt summary."),
        );
        object.insert(
            "finality_receipt_sha256".to_string(),
            json!("finality-receipt-sha256"),
        );
        object.insert("finality_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "consummation_record_summary".to_string(),
            json!("Consummation record summary."),
        );
        object.insert(
            "consummation_record_sha256".to_string(),
            json!("consummation-record-sha256"),
        );
        object.insert("consummation_record_algorithm".to_string(), json!("sha256"));
        object.insert(
            "completion_certificate_summary".to_string(),
            json!("Completion certificate summary."),
        );
        object.insert(
            "completion_certificate_sha256".to_string(),
            json!("completion-certificate-sha256"),
        );
        object.insert(
            "completion_certificate_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "summit_certificate_summary".to_string(),
            json!("Summit certificate summary."),
        );
        object.insert(
            "summit_certificate_sha256".to_string(),
            json!("summit-certificate-sha256"),
        );
        object.insert("summit_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "culmination_receipt_summary".to_string(),
            json!("Culmination receipt summary."),
        );
        object.insert(
            "culmination_receipt_sha256".to_string(),
            json!("culmination-receipt-sha256"),
        );
        object.insert("culmination_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "pinnacle_receipt_summary".to_string(),
            json!("Pinnacle receipt summary."),
        );
        object.insert(
            "pinnacle_receipt_sha256".to_string(),
            json!("pinnacle-receipt-sha256"),
        );
        object.insert("pinnacle_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "capstone_certificate_summary".to_string(),
            json!("Capstone certificate summary."),
        );
        object.insert(
            "capstone_certificate_sha256".to_string(),
            json!("capstone-certificate-sha256"),
        );
        object.insert(
            "capstone_certificate_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "keystone_receipt_summary".to_string(),
            json!("Keystone receipt summary."),
        );
        object.insert(
            "keystone_receipt_sha256".to_string(),
            json!("keystone-receipt-sha256"),
        );
        object.insert("keystone_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "cornerstone_certificate_summary".to_string(),
            json!("Cornerstone certificate summary."),
        );
        object.insert(
            "cornerstone_certificate_sha256".to_string(),
            json!("cornerstone-certificate-sha256"),
        );
        object.insert(
            "cornerstone_certificate_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "foundation_receipt_summary".to_string(),
            json!("Foundation receipt summary."),
        );
        object.insert(
            "foundation_receipt_sha256".to_string(),
            json!("foundation-receipt-sha256"),
        );
        object.insert("foundation_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "bedrock_certificate_summary".to_string(),
            json!("Bedrock certificate summary."),
        );
        object.insert(
            "bedrock_certificate_sha256".to_string(),
            json!("bedrock-certificate-sha256"),
        );
        object.insert("bedrock_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "basal_receipt_summary".to_string(),
            json!("Basal receipt summary."),
        );
        object.insert(
            "basal_receipt_sha256".to_string(),
            json!("basal-receipt-sha256"),
        );
        object.insert("basal_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "substructure_certificate_summary".to_string(),
            json!("Substructure certificate summary."),
        );
        object.insert(
            "substructure_certificate_sha256".to_string(),
            json!("substructure-certificate-sha256"),
        );
        object.insert(
            "substructure_certificate_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "plinth_receipt_summary".to_string(),
            json!("Plinth receipt summary."),
        );
        object.insert(
            "plinth_receipt_sha256".to_string(),
            json!("plinth-receipt-sha256"),
        );
        object.insert("plinth_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "pedestal_certificate_summary".to_string(),
            json!("Pedestal certificate summary."),
        );
        object.insert(
            "pedestal_certificate_sha256".to_string(),
            json!("pedestal-certificate-sha256"),
        );
        object.insert(
            "pedestal_certificate_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "dais_receipt_summary".to_string(),
            json!("Dais receipt summary."),
        );
        object.insert(
            "dais_receipt_sha256".to_string(),
            json!("dais-receipt-sha256"),
        );
        object.insert("dais_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "rostrum_certificate_summary".to_string(),
            json!("Rostrum certificate summary."),
        );
        object.insert(
            "rostrum_certificate_sha256".to_string(),
            json!("rostrum-certificate-sha256"),
        );
        object.insert("rostrum_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "podium_receipt_summary".to_string(),
            json!("Podium receipt summary."),
        );
        object.insert(
            "podium_receipt_sha256".to_string(),
            json!("podium-receipt-sha256"),
        );
        object.insert("podium_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "lectern_certificate_summary".to_string(),
            json!("Lectern certificate summary."),
        );
        object.insert(
            "lectern_certificate_sha256".to_string(),
            json!("lectern-certificate-sha256"),
        );
        object.insert("lectern_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "pulpit_receipt_summary".to_string(),
            json!("Pulpit receipt summary."),
        );
        object.insert("pulpit_receipt_sha256".to_string(), json!("crest-sha256"));
        object.insert("pulpit_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "chancel_certificate_summary".to_string(),
            json!("Chancel certificate summary."),
        );
        object.insert(
            "chancel_certificate_sha256".to_string(),
            json!("herald-certificate-sha256"),
        );
        object.insert("chancel_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "apse_receipt_summary".to_string(),
            json!("Apse receipt summary."),
        );
        object.insert("apse_receipt_sha256".to_string(), json!("herald-sha256"));
        object.insert("apse_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "explicit_statement".to_string(),
            json!("apse-receipt status statement"),
        );
        Value::Object(object)
    }
}
