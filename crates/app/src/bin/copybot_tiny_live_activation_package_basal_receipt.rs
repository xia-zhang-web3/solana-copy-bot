use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_app::tiny_live_activation::install_target_managed_surface::validate_turn_green_session_dir as validate_managed_surface_session_dir;
use copybot_app::tiny_live_activation::package_bedrock_certificate_basal_receipt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: copybot_tiny_live_activation_package_basal_receipt --bedrock-certificate-session-dir <path> [--confirm-decision-packet-session-dir <path>] [--session-dir <path>] [--output <path>] [--json] (--plan-live-package-basal-receipt | --render-live-package-basal-receipt | --run-live-package-basal-receipt | --verify-live-package-basal-receipt)";
const BASAL_RECEIPT_SESSION_VERSION: &str = "1";
const BASAL_RECEIPT_STATUS_VERSION: &str = "1";
const BASAL_RECEIPT_ALGORITHM: &str = "sha256";
const BASAL_RECEIPT_SESSION_EXPLICIT_STATEMENT: &str =
    "the verified bedrock-certificate session is the primary input here; this immutable basal receipt freezes one final circlet identity over the fully culminated chain without enabling production execution";
const BASAL_RECEIPT_STATUS_EXPLICIT_STATEMENT: &str =
    "this basal receipt is archival and read-only; it never executes or enables the frozen controller";
const BASAL_RECEIPT_VERIFY_EXPLICIT_STATEMENT: &str =
    "this verification binds one immutable basal receipt to verified bedrock-certificate truth, the exact reviewed frozen controller summary, the canonical chain fingerprint, the ledger seal, the registry entry, the filing-certificate identity, the archive-receipt identity, the closure-certificate identity, the finality-receipt identity, the consummation-record identity, the completion-certificate identity, the culmination-receipt identity, the summit-certificate identity, the pinnacle-receipt identity, the capstone-certificate identity, the keystone-receipt identity, the cornerstone-certificate identity, the foundation-receipt identity, and the bedrock-certificate identity";

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
    bedrock_certificate_session_dir: PathBuf,
    confirmed_decision_packet_session_dir: Option<PathBuf>,
    session_dir: Option<PathBuf>,
    output_path: Option<PathBuf>,
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    PlanLivePackageBasalReceipt,
    RenderLivePackageBasalReceipt,
    RunLivePackageBasalReceipt,
    VerifyLivePackageBasalReceipt,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageBasalReceiptVerdict {
    TinyLivePackageBasalReceiptPlanReady,
    TinyLivePackageBasalReceiptRendered,
    TinyLivePackageBasalReceiptRefusedNowByStage3,
    TinyLivePackageBasalReceiptRefusedNowByPreActivationGate,
    TinyLivePackageBasalReceiptRefusedNowByInvalidOrDriftedContract,
    TinyLivePackageBasalReceiptReadyForManualExecutionWhenGateTurnsGreen,
    TinyLivePackageBasalReceiptVerifyOk,
    TinyLivePackageBasalReceiptVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LivePackageBasalReceiptResult {
    RefusedNowByStage3,
    RefusedNowByPreActivationGate,
    RefusedNowByInvalidOrDriftedContract,
    ReadyForManualExecutionWhenGateTurnsGreen,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PackageBasalReceiptStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PackageBasalReceiptSession {
    session_version: String,
    planned_at: DateTime<Utc>,
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
    verify_foundation_receipt_command_summary: String,
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
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PackageBasalReceiptStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    session_dir: String,
    bedrock_certificate_session_dir: String,
    foundation_receipt_session_dir: String,
    result: LivePackageBasalReceiptResult,
    reason: String,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    bedrock_certificate_step: Option<PackageBasalReceiptStepArtifact>,
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
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackageBasalReceiptPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    bedrock_certificate_report_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageBasalReceiptReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageBasalReceiptVerdict,
    reason: String,
    bedrock_certificate_session_dir: String,
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
    basal_receipt_session_path: Option<String>,
    basal_receipt_status_path: Option<String>,
    archived_bedrock_certificate_report_path: Option<String>,
    result: Option<String>,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    bedrock_certificate_step: Option<PackageBasalReceiptStepArtifact>,
    verify_foundation_receipt_command_summary: Option<String>,
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
    verification_mismatches: Vec<String>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Deserialize)]
struct NestedBedrockCertificateReportView {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: String,
    reason: String,
}

#[derive(Debug, Serialize)]
struct BasalReceiptPayload<'a> {
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
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut bedrock_certificate_session_dir: Option<PathBuf> = None;
    let mut confirmed_decision_packet_session_dir: Option<PathBuf> = None;
    let mut session_dir: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut json = false;
    let mut mode: Option<Mode> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--bedrock-certificate-session-dir" => {
                bedrock_certificate_session_dir = Some(PathBuf::from(parse_string_arg(
                    "--bedrock-certificate-session-dir",
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
            "--plan-live-package-basal-receipt" => {
                set_mode(&mut mode, Mode::PlanLivePackageBasalReceipt, arg.as_str())?
            }
            "--render-live-package-basal-receipt" => {
                set_mode(&mut mode, Mode::RenderLivePackageBasalReceipt, arg.as_str())?
            }
            "--run-live-package-basal-receipt" => {
                set_mode(&mut mode, Mode::RunLivePackageBasalReceipt, arg.as_str())?
            }
            "--verify-live-package-basal-receipt" => {
                set_mode(&mut mode, Mode::VerifyLivePackageBasalReceipt, arg.as_str())?
            }
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    let Some(mode) = mode else {
        return Ok(None);
    };
    if matches!(mode, Mode::RenderLivePackageBasalReceipt) && output_path.is_none() {
        bail!("missing --output for --render-live-package-basal-receipt");
    }
    if matches!(
        mode,
        Mode::RunLivePackageBasalReceipt | Mode::VerifyLivePackageBasalReceipt
    ) && session_dir.is_none()
    {
        bail!("missing --session-dir for run/verify mode");
    }
    if matches!(
        mode,
        Mode::RunLivePackageBasalReceipt | Mode::VerifyLivePackageBasalReceipt
    ) && confirmed_decision_packet_session_dir.is_none()
    {
        bail!("missing --confirm-decision-packet-session-dir for run/verify mode");
    }
    let bedrock_certificate_session_dir = bedrock_certificate_session_dir
        .ok_or_else(|| anyhow!("missing --bedrock-certificate-session-dir"))?;
    Ok(Some(Config {
        mode,
        bedrock_certificate_session_dir,
        confirmed_decision_packet_session_dir,
        session_dir,
        output_path,
        json,
    }))
}

fn run(config: Config) -> Result<String> {
    let report = match config.mode {
        Mode::PlanLivePackageBasalReceipt => plan_live_package_basal_receipt_report(&config)?,
        Mode::RenderLivePackageBasalReceipt => render_live_package_basal_receipt_report(&config)?,
        Mode::RunLivePackageBasalReceipt => run_live_package_basal_receipt_report(&config)?,
        Mode::VerifyLivePackageBasalReceipt => verify_live_package_basal_receipt_report(&config)?,
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_report_lines(&report))
    }
}

fn plan_live_package_basal_receipt_report(config: &Config) -> Result<PackageBasalReceiptReport> {
    let (cornerstone_certificate, classification) =
        match config.confirmed_decision_packet_session_dir.as_deref() {
            Some(confirmed_decision_packet_session_dir) => {
                match package_bedrock_certificate_basal_receipt::verify_live_package_bedrock_certificate_for_basal_receipt(
                    &config.bedrock_certificate_session_dir,
                    confirmed_decision_packet_session_dir,
                ) {
                    Ok(verified) => {
                        let classification = classify_verified_bedrock_certificate(&verified);
                        (verified.contract, classification)
                    }
                    Err(_) => {
                        let raw_contract = package_bedrock_certificate_basal_receipt::load_live_package_bedrock_certificate_contract_for_basal_receipt(
                            &config.bedrock_certificate_session_dir,
                        )?;
                        let classification = classify_bedrock_certificate_contract(&raw_contract, false);
                        (raw_contract, classification)
                    }
                }
            }
            None => {
                let raw_contract = package_bedrock_certificate_basal_receipt::load_live_package_bedrock_certificate_contract_for_basal_receipt(
                    &config.bedrock_certificate_session_dir,
                )?;
                let classification = classify_bedrock_certificate_contract(&raw_contract, false);
                (raw_contract, classification)
            }
        };
    let session_dir = config
        .session_dir
        .clone()
        .unwrap_or_else(|| config.bedrock_certificate_session_dir.join("basal-receipt"));
    let result = serialize_enum(&classification.0);
    let basal_receipt_sha256 = compute_basal_receipt_sha256(
        &result,
        cornerstone_certificate.handoff_bundle_result.as_deref(),
        cornerstone_certificate.decision_packet_result.as_deref(),
        cornerstone_certificate.execute_frozen_result.as_deref(),
        cornerstone_certificate
            .current_pre_activation_gate_verdict
            .as_deref(),
        cornerstone_certificate
            .current_pre_activation_gate_reason
            .as_deref(),
        &cornerstone_certificate.reviewed_frozen_live_cutover_controller_command_summary,
        &cornerstone_certificate.chain_fingerprint_sha256,
        &cornerstone_certificate.chain_fingerprint_algorithm,
        &cornerstone_certificate.release_capsule_digest_manifest_sha256,
        cornerstone_certificate.release_capsule_digest_manifest_entry_count,
        &cornerstone_certificate.release_capsule_digest_algorithm,
        &cornerstone_certificate.ledger_seal_sha256,
        &cornerstone_certificate.ledger_seal_algorithm,
        &cornerstone_certificate.registry_entry_sha256,
        &cornerstone_certificate.registry_entry_algorithm,
        &cornerstone_certificate.filing_certificate_sha256,
        &cornerstone_certificate.filing_certificate_algorithm,
        &cornerstone_certificate.archive_receipt_sha256,
        &cornerstone_certificate.archive_receipt_algorithm,
        &cornerstone_certificate.closure_certificate_summary,
        &cornerstone_certificate.closure_certificate_sha256,
        &cornerstone_certificate.closure_certificate_algorithm,
        &cornerstone_certificate.finality_receipt_sha256,
        &cornerstone_certificate.finality_receipt_algorithm,
        &cornerstone_certificate.consummation_record_sha256,
        &cornerstone_certificate.consummation_record_algorithm,
        &cornerstone_certificate.completion_certificate_summary,
        &cornerstone_certificate.completion_certificate_sha256,
        &cornerstone_certificate.completion_certificate_algorithm,
        &cornerstone_certificate.summit_certificate_summary,
        &cornerstone_certificate.summit_certificate_sha256,
        &cornerstone_certificate.summit_certificate_algorithm,
        &cornerstone_certificate.culmination_receipt_summary,
        &cornerstone_certificate.culmination_receipt_sha256,
        &cornerstone_certificate.culmination_receipt_algorithm,
        &cornerstone_certificate.pinnacle_receipt_summary,
        &cornerstone_certificate.pinnacle_receipt_sha256,
        &cornerstone_certificate.pinnacle_receipt_algorithm,
        &cornerstone_certificate.capstone_certificate_summary,
        &cornerstone_certificate.capstone_certificate_sha256,
        &cornerstone_certificate.capstone_certificate_algorithm,
        &cornerstone_certificate.keystone_receipt_summary,
        &cornerstone_certificate.keystone_receipt_sha256,
        &cornerstone_certificate.keystone_receipt_algorithm,
        &cornerstone_certificate.cornerstone_certificate_summary,
        &cornerstone_certificate.cornerstone_certificate_sha256,
        &cornerstone_certificate.cornerstone_certificate_algorithm,
        &cornerstone_certificate.foundation_receipt_summary,
        &cornerstone_certificate.foundation_receipt_sha256,
        &cornerstone_certificate.foundation_receipt_algorithm,
        &cornerstone_certificate.bedrock_certificate_summary,
        &cornerstone_certificate.bedrock_certificate_sha256,
        &cornerstone_certificate.bedrock_certificate_algorithm,
    )?;

    Ok(PackageBasalReceiptReport {
        generated_at: Utc::now(),
        mode: "plan_live_package_basal_receipt".to_string(),
        verdict:
            TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptPlanReady,
        reason: format!(
            "bedrock-certificate-native basal receipt is explicit for verified bedrock-certificate session {}; this command stays archival and read-only while freezing one final circlet identity over the fully culminated chain",
            config.bedrock_certificate_session_dir.display()
        ),
        bedrock_certificate_session_dir: config.bedrock_certificate_session_dir.display().to_string(),
        foundation_receipt_session_dir: Some(
            cornerstone_certificate
                .foundation_receipt_session_dir
                .display()
                .to_string(),
        ),
        registry_entry_session_dir: Some(
            cornerstone_certificate.registry_entry_session_dir.display().to_string(),
        ),
        notarization_receipt_session_dir: Some(
            cornerstone_certificate
                .notarization_receipt_session_dir
                .display()
                .to_string(),
        ),
        provenance_certificate_session_dir: Some(
            cornerstone_certificate
                .provenance_certificate_session_dir
                .display()
                .to_string(),
        ),
        attestation_seal_session_dir: Some(
            cornerstone_certificate
                .attestation_seal_session_dir
                .display()
                .to_string(),
        ),
        release_capsule_session_dir: Some(
            cornerstone_certificate
                .release_capsule_session_dir
                .display()
                .to_string(),
        ),
        activation_ticket_session_dir: Some(
            cornerstone_certificate
                .activation_ticket_session_dir
                .display()
                .to_string(),
        ),
        review_receipt_session_dir: Some(
            cornerstone_certificate.review_receipt_session_dir.display().to_string(),
        ),
        handoff_bundle_session_dir: Some(
            cornerstone_certificate.handoff_bundle_session_dir.display().to_string(),
        ),
        decision_packet_session_dir: Some(
            cornerstone_certificate
                .decision_packet_session_dir
                .display()
                .to_string(),
        ),
        execute_frozen_session_dir: Some(
            cornerstone_certificate
                .execute_frozen_session_dir
                .display()
                .to_string(),
        ),
        turn_green_session_dir: Some(
            cornerstone_certificate.turn_green_session_dir.display().to_string(),
        ),
        launch_packet_session_dir: Some(
            cornerstone_certificate.launch_packet_session_dir.display().to_string(),
        ),
        package_dir: Some(cornerstone_certificate.package_dir.display().to_string()),
        install_root: Some(cornerstone_certificate.install_root.display().to_string()),
        target_service_name: Some(cornerstone_certificate.target_service_name.clone()),
        backend_command: Some(cornerstone_certificate.backend_command.clone()),
        wrapper_timeout_ms: Some(cornerstone_certificate.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(cornerstone_certificate.service_status_max_staleness_ms),
        session_dir: Some(session_dir.display().to_string()),
        basal_receipt_session_path: None,
        basal_receipt_status_path: None,
        archived_bedrock_certificate_report_path: None,
        result: Some(result),
        handoff_bundle_result: cornerstone_certificate.handoff_bundle_result.clone(),
        decision_packet_result: cornerstone_certificate.decision_packet_result.clone(),
        execute_frozen_result: cornerstone_certificate.execute_frozen_result.clone(),
        current_pre_activation_gate_verdict: cornerstone_certificate
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: cornerstone_certificate
            .current_pre_activation_gate_reason
            .clone(),
        bedrock_certificate_step: None,
        verify_foundation_receipt_command_summary: Some(verify_foundation_receipt_command_summary(
            &config.bedrock_certificate_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            cornerstone_certificate
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        provenance_certificate_summary: Some(cornerstone_certificate.provenance_certificate_summary.clone()),
        chain_fingerprint_summary: Some(cornerstone_certificate.chain_fingerprint_summary.clone()),
        chain_fingerprint_sha256: Some(cornerstone_certificate.chain_fingerprint_sha256.clone()),
        chain_fingerprint_algorithm: Some(cornerstone_certificate.chain_fingerprint_algorithm.clone()),
        release_capsule_digest_manifest_sha256: Some(
            cornerstone_certificate.release_capsule_digest_manifest_sha256.clone(),
        ),
        release_capsule_digest_manifest_entry_count: Some(
            cornerstone_certificate.release_capsule_digest_manifest_entry_count,
        ),
        release_capsule_digest_algorithm: Some(
            cornerstone_certificate.release_capsule_digest_algorithm.clone(),
        ),
        notarization_receipt_summary: Some(cornerstone_certificate.notarization_receipt_summary.clone()),
        ledger_seal_summary: Some(cornerstone_certificate.ledger_seal_summary.clone()),
        ledger_seal_sha256: Some(cornerstone_certificate.ledger_seal_sha256.clone()),
        ledger_seal_algorithm: Some(cornerstone_certificate.ledger_seal_algorithm.clone()),
        registry_entry_summary: Some(cornerstone_certificate.registry_entry_summary.clone()),
        registry_entry_sha256: Some(cornerstone_certificate.registry_entry_sha256.clone()),
        registry_entry_algorithm: Some(cornerstone_certificate.registry_entry_algorithm.clone()),
        filing_certificate_summary: Some(cornerstone_certificate.filing_certificate_summary.clone()),
        filing_certificate_sha256: Some(cornerstone_certificate.filing_certificate_sha256.clone()),
        filing_certificate_algorithm: Some(cornerstone_certificate.filing_certificate_algorithm.clone()),
        archive_receipt_summary: Some(cornerstone_certificate.archive_receipt_summary.clone()),
        archive_receipt_sha256: Some(cornerstone_certificate.archive_receipt_sha256.clone()),
        archive_receipt_algorithm: Some(cornerstone_certificate.archive_receipt_algorithm.clone()),
        closure_certificate_summary: Some(cornerstone_certificate.closure_certificate_summary.clone()),
        closure_certificate_sha256: Some(cornerstone_certificate.closure_certificate_sha256.clone()),
        closure_certificate_algorithm: Some(
            cornerstone_certificate.closure_certificate_algorithm.clone(),
        ),
        finality_receipt_summary: Some(cornerstone_certificate.finality_receipt_summary.clone()),
        finality_receipt_sha256: Some(cornerstone_certificate.finality_receipt_sha256.clone()),
        finality_receipt_algorithm: Some(cornerstone_certificate.finality_receipt_algorithm.clone()),
        consummation_record_summary: Some(cornerstone_certificate.consummation_record_summary.clone()),
        consummation_record_sha256: Some(cornerstone_certificate.consummation_record_sha256.clone()),
        consummation_record_algorithm: Some(cornerstone_certificate.consummation_record_algorithm.clone()),
        completion_certificate_summary: Some(
            cornerstone_certificate.completion_certificate_summary.clone(),
        ),
        completion_certificate_sha256: Some(
            cornerstone_certificate.completion_certificate_sha256.clone(),
        ),
        completion_certificate_algorithm: Some(
            cornerstone_certificate.completion_certificate_algorithm.clone(),
        ),
        summit_certificate_summary: Some(cornerstone_certificate.summit_certificate_summary.clone()),
        summit_certificate_sha256: Some(cornerstone_certificate.summit_certificate_sha256.clone()),
        summit_certificate_algorithm: Some(
            cornerstone_certificate.summit_certificate_algorithm.clone(),
        ),
        culmination_receipt_summary: Some(
            cornerstone_certificate.culmination_receipt_summary.clone(),
        ),
        culmination_receipt_sha256: Some(
            cornerstone_certificate.culmination_receipt_sha256.clone(),
        ),
        culmination_receipt_algorithm: Some(
            cornerstone_certificate.culmination_receipt_algorithm.clone(),
        ),
        pinnacle_receipt_summary: Some(cornerstone_certificate.pinnacle_receipt_summary.clone()),
        pinnacle_receipt_sha256: Some(cornerstone_certificate.pinnacle_receipt_sha256.clone()),
        pinnacle_receipt_algorithm: Some(
            cornerstone_certificate.pinnacle_receipt_algorithm.clone(),
        ),
        capstone_certificate_summary: Some(
            cornerstone_certificate.capstone_certificate_summary.clone(),
        ),
        capstone_certificate_sha256: Some(
            cornerstone_certificate.capstone_certificate_sha256.clone(),
        ),
        capstone_certificate_algorithm: Some(
            cornerstone_certificate.capstone_certificate_algorithm.clone(),
        ),
        keystone_receipt_summary: Some(
            cornerstone_certificate.keystone_receipt_summary.clone(),
        ),
        keystone_receipt_sha256: Some(
            cornerstone_certificate.keystone_receipt_sha256.clone(),
        ),
        keystone_receipt_algorithm: Some(
            cornerstone_certificate.keystone_receipt_algorithm.clone(),
        ),
        cornerstone_certificate_summary: Some(cornerstone_certificate.cornerstone_certificate_summary.clone()),
        cornerstone_certificate_sha256: Some(cornerstone_certificate.cornerstone_certificate_sha256.clone()),
        cornerstone_certificate_algorithm: Some(cornerstone_certificate.cornerstone_certificate_algorithm.clone()),
        foundation_receipt_summary: Some(cornerstone_certificate.foundation_receipt_summary.clone()),
        foundation_receipt_sha256: Some(cornerstone_certificate.foundation_receipt_sha256.clone()),
        foundation_receipt_algorithm: Some(cornerstone_certificate.foundation_receipt_algorithm.clone()),
        bedrock_certificate_summary: Some(cornerstone_certificate.bedrock_certificate_summary.clone()),
        bedrock_certificate_sha256: Some(cornerstone_certificate.bedrock_certificate_sha256.clone()),
        bedrock_certificate_algorithm: Some(
            cornerstone_certificate.bedrock_certificate_algorithm.clone(),
        ),
        basal_receipt_summary: Some(basal_receipt_summary(
            classification.0,
            &cornerstone_certificate.reviewed_frozen_live_cutover_controller_command_summary,
        )),
        basal_receipt_sha256: Some(basal_receipt_sha256),
        basal_receipt_algorithm: Some(BASAL_RECEIPT_ALGORITHM.to_string()),
        verification_mismatches: Vec::new(),
        explicit_statement: BASAL_RECEIPT_SESSION_EXPLICIT_STATEMENT.to_string(),
    })
}

fn render_live_package_basal_receipt_report(config: &Config) -> Result<PackageBasalReceiptReport> {
    let plan = plan_live_package_basal_receipt_report(config)?;
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
        "#!/bin/sh\nset -eu\ncopybot_tiny_live_activation_package_basal_receipt --bedrock-certificate-session-dir {} --confirm-decision-packet-session-dir {} --session-dir {} --run-live-package-basal-receipt --json\ncopybot_tiny_live_activation_package_basal_receipt --bedrock-certificate-session-dir {} --confirm-decision-packet-session-dir {} --session-dir {} --verify-live-package-basal-receipt --json\n",
        shell_escape_path(&config.bedrock_certificate_session_dir),
        shell_escape_path(&confirmed_decision_packet_session_dir),
        shell_escape_path(&session_dir),
        shell_escape_path(&config.bedrock_certificate_session_dir),
        shell_escape_path(&confirmed_decision_packet_session_dir),
        shell_escape_path(&session_dir),
    );
    write_script(output_path, &script)?;
    Ok(PackageBasalReceiptReport {
        generated_at: Utc::now(),
        mode: "render_live_package_basal_receipt".to_string(),
        verdict:
            TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptRendered,
        reason: format!(
            "rendered basal-receipt run/verify script to {}; the primary input remains the immutable bedrock-certificate session and run/verify still require the decision-packet confirmation anchor",
            output_path.display()
        ),
        explicit_statement: BASAL_RECEIPT_SESSION_EXPLICIT_STATEMENT.to_string(),
        ..plan
    })
}

fn run_live_package_basal_receipt_report(config: &Config) -> Result<PackageBasalReceiptReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for run mode"))?;
    let verified_bedrock_certificate = match package_bedrock_certificate_basal_receipt::verify_live_package_bedrock_certificate_for_basal_receipt(
        &config.bedrock_certificate_session_dir,
        config
            .confirmed_decision_packet_session_dir
            .as_deref()
            .ok_or_else(|| anyhow!("missing --confirm-decision-packet-session-dir for run mode"))?,
    ) {
        Ok(value) => value,
        Err(error) => {
            return Ok(failure_report_without_contract(
                config,
                TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptRefusedNowByInvalidOrDriftedContract,
                format!(
                    "failed to verify bedrock-certificate session under {}: {error}",
                    config.bedrock_certificate_session_dir.display()
                ),
            ))
        }
    };

    validate_managed_surface_session_dir(
        &verified_bedrock_certificate.contract.install_root,
        session_dir,
    )
    .with_context(|| {
        format!(
            "basal-receipt session dir {} overlaps the managed live target surface derived from verified bedrock-certificate install_root {}",
            session_dir.display(),
            verified_bedrock_certificate.contract.install_root.display()
        )
    })?;
    ensure_clean_basal_receipt_session_dir(session_dir)?;
    let paths = basal_receipt_paths(session_dir);
    let classification = classify_verified_bedrock_certificate(&verified_bedrock_certificate);
    let result = classification.0;
    let basal_receipt_sha256 = compute_basal_receipt_sha256(
        &serialize_enum(&result),
        verified_bedrock_certificate
            .contract
            .handoff_bundle_result
            .as_deref(),
        verified_bedrock_certificate
            .contract
            .decision_packet_result
            .as_deref(),
        verified_bedrock_certificate
            .contract
            .execute_frozen_result
            .as_deref(),
        verified_bedrock_certificate
            .contract
            .current_pre_activation_gate_verdict
            .as_deref(),
        verified_bedrock_certificate
            .contract
            .current_pre_activation_gate_reason
            .as_deref(),
        &verified_bedrock_certificate
            .contract
            .reviewed_frozen_live_cutover_controller_command_summary,
        &verified_bedrock_certificate
            .contract
            .chain_fingerprint_sha256,
        &verified_bedrock_certificate
            .contract
            .chain_fingerprint_algorithm,
        &verified_bedrock_certificate
            .contract
            .release_capsule_digest_manifest_sha256,
        verified_bedrock_certificate
            .contract
            .release_capsule_digest_manifest_entry_count,
        &verified_bedrock_certificate
            .contract
            .release_capsule_digest_algorithm,
        &verified_bedrock_certificate.contract.ledger_seal_sha256,
        &verified_bedrock_certificate.contract.ledger_seal_algorithm,
        &verified_bedrock_certificate.contract.registry_entry_sha256,
        &verified_bedrock_certificate
            .contract
            .registry_entry_algorithm,
        &verified_bedrock_certificate
            .contract
            .filing_certificate_sha256,
        &verified_bedrock_certificate
            .contract
            .filing_certificate_algorithm,
        &verified_bedrock_certificate.contract.archive_receipt_sha256,
        &verified_bedrock_certificate
            .contract
            .archive_receipt_algorithm,
        &verified_bedrock_certificate
            .contract
            .closure_certificate_summary,
        &verified_bedrock_certificate
            .contract
            .closure_certificate_sha256,
        &verified_bedrock_certificate
            .contract
            .closure_certificate_algorithm,
        &verified_bedrock_certificate
            .contract
            .finality_receipt_sha256,
        &verified_bedrock_certificate
            .contract
            .finality_receipt_algorithm,
        &verified_bedrock_certificate
            .contract
            .consummation_record_sha256,
        &verified_bedrock_certificate
            .contract
            .consummation_record_algorithm,
        &verified_bedrock_certificate
            .contract
            .completion_certificate_summary,
        &verified_bedrock_certificate
            .contract
            .completion_certificate_sha256,
        &verified_bedrock_certificate
            .contract
            .completion_certificate_algorithm,
        &verified_bedrock_certificate
            .contract
            .summit_certificate_summary,
        &verified_bedrock_certificate
            .contract
            .summit_certificate_sha256,
        &verified_bedrock_certificate
            .contract
            .summit_certificate_algorithm,
        &verified_bedrock_certificate
            .contract
            .culmination_receipt_summary,
        &verified_bedrock_certificate
            .contract
            .culmination_receipt_sha256,
        &verified_bedrock_certificate
            .contract
            .culmination_receipt_algorithm,
        &verified_bedrock_certificate
            .contract
            .pinnacle_receipt_summary,
        &verified_bedrock_certificate
            .contract
            .pinnacle_receipt_sha256,
        &verified_bedrock_certificate
            .contract
            .pinnacle_receipt_algorithm,
        &verified_bedrock_certificate
            .contract
            .capstone_certificate_summary,
        &verified_bedrock_certificate
            .contract
            .capstone_certificate_sha256,
        &verified_bedrock_certificate
            .contract
            .capstone_certificate_algorithm,
        &verified_bedrock_certificate
            .contract
            .keystone_receipt_summary,
        &verified_bedrock_certificate
            .contract
            .keystone_receipt_sha256,
        &verified_bedrock_certificate
            .contract
            .keystone_receipt_algorithm,
        &verified_bedrock_certificate
            .contract
            .cornerstone_certificate_summary,
        &verified_bedrock_certificate
            .contract
            .cornerstone_certificate_sha256,
        &verified_bedrock_certificate
            .contract
            .cornerstone_certificate_algorithm,
        &verified_bedrock_certificate
            .contract
            .foundation_receipt_summary,
        &verified_bedrock_certificate
            .contract
            .foundation_receipt_sha256,
        &verified_bedrock_certificate
            .contract
            .foundation_receipt_algorithm,
        &verified_bedrock_certificate
            .contract
            .bedrock_certificate_summary,
        &verified_bedrock_certificate
            .contract
            .bedrock_certificate_sha256,
        &verified_bedrock_certificate
            .contract
            .bedrock_certificate_algorithm,
    )?;
    let session = planned_session(
        config,
        session_dir,
        &verified_bedrock_certificate.contract,
        result,
        &basal_receipt_sha256,
    );
    persist_json(&paths.session_path, &session)?;
    persist_json(
        &paths.bedrock_certificate_report_path,
        &verified_bedrock_certificate.report_json,
    )?;
    let bedrock_certificate_step = Some(step_artifact(
        &paths.bedrock_certificate_report_path,
        "verify_live_package_bedrock_certificate",
        &verified_bedrock_certificate.verdict,
        &verified_bedrock_certificate.reason,
        verified_bedrock_certificate.generated_at,
    ));
    let status = PackageBasalReceiptStatus {
        status_version: BASAL_RECEIPT_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        session_dir: session_dir.display().to_string(),
        bedrock_certificate_session_dir: config
            .bedrock_certificate_session_dir
            .display()
            .to_string(),
        foundation_receipt_session_dir: verified_bedrock_certificate
            .contract
            .foundation_receipt_session_dir
            .display()
            .to_string(),
        result,
        reason: classification.2.clone(),
        handoff_bundle_result: verified_bedrock_certificate
            .contract
            .handoff_bundle_result
            .clone(),
        decision_packet_result: verified_bedrock_certificate
            .contract
            .decision_packet_result
            .clone(),
        execute_frozen_result: verified_bedrock_certificate
            .contract
            .execute_frozen_result
            .clone(),
        current_pre_activation_gate_verdict: verified_bedrock_certificate
            .contract
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: verified_bedrock_certificate
            .contract
            .current_pre_activation_gate_reason
            .clone(),
        bedrock_certificate_step: bedrock_certificate_step.clone(),
        provenance_certificate_summary: verified_bedrock_certificate
            .contract
            .provenance_certificate_summary
            .clone(),
        chain_fingerprint_summary: verified_bedrock_certificate
            .contract
            .chain_fingerprint_summary
            .clone(),
        chain_fingerprint_sha256: verified_bedrock_certificate
            .contract
            .chain_fingerprint_sha256
            .clone(),
        chain_fingerprint_algorithm: verified_bedrock_certificate
            .contract
            .chain_fingerprint_algorithm
            .clone(),
        release_capsule_digest_manifest_sha256: verified_bedrock_certificate
            .contract
            .release_capsule_digest_manifest_sha256
            .clone(),
        release_capsule_digest_manifest_entry_count: verified_bedrock_certificate
            .contract
            .release_capsule_digest_manifest_entry_count,
        release_capsule_digest_algorithm: verified_bedrock_certificate
            .contract
            .release_capsule_digest_algorithm
            .clone(),
        notarization_receipt_summary: verified_bedrock_certificate
            .contract
            .notarization_receipt_summary
            .clone(),
        ledger_seal_summary: verified_bedrock_certificate
            .contract
            .ledger_seal_summary
            .clone(),
        ledger_seal_sha256: verified_bedrock_certificate
            .contract
            .ledger_seal_sha256
            .clone(),
        ledger_seal_algorithm: verified_bedrock_certificate
            .contract
            .ledger_seal_algorithm
            .clone(),
        registry_entry_summary: verified_bedrock_certificate
            .contract
            .registry_entry_summary
            .clone(),
        registry_entry_sha256: verified_bedrock_certificate
            .contract
            .registry_entry_sha256
            .clone(),
        registry_entry_algorithm: verified_bedrock_certificate
            .contract
            .registry_entry_algorithm
            .clone(),
        filing_certificate_summary: verified_bedrock_certificate
            .contract
            .filing_certificate_summary
            .clone(),
        filing_certificate_sha256: verified_bedrock_certificate
            .contract
            .filing_certificate_sha256
            .clone(),
        filing_certificate_algorithm: verified_bedrock_certificate
            .contract
            .filing_certificate_algorithm
            .clone(),
        archive_receipt_summary: verified_bedrock_certificate
            .contract
            .archive_receipt_summary
            .clone(),
        archive_receipt_sha256: verified_bedrock_certificate
            .contract
            .archive_receipt_sha256
            .clone(),
        archive_receipt_algorithm: verified_bedrock_certificate
            .contract
            .archive_receipt_algorithm
            .clone(),
        closure_certificate_summary: verified_bedrock_certificate
            .contract
            .closure_certificate_summary
            .clone(),
        closure_certificate_sha256: verified_bedrock_certificate
            .contract
            .closure_certificate_sha256
            .clone(),
        closure_certificate_algorithm: verified_bedrock_certificate
            .contract
            .closure_certificate_algorithm
            .clone(),
        finality_receipt_summary: verified_bedrock_certificate
            .contract
            .finality_receipt_summary
            .clone(),
        finality_receipt_sha256: verified_bedrock_certificate
            .contract
            .finality_receipt_sha256
            .clone(),
        finality_receipt_algorithm: verified_bedrock_certificate
            .contract
            .finality_receipt_algorithm
            .clone(),
        consummation_record_summary: verified_bedrock_certificate
            .contract
            .consummation_record_summary
            .clone(),
        consummation_record_sha256: verified_bedrock_certificate
            .contract
            .consummation_record_sha256
            .clone(),
        consummation_record_algorithm: verified_bedrock_certificate
            .contract
            .consummation_record_algorithm
            .clone(),
        completion_certificate_summary: verified_bedrock_certificate
            .contract
            .completion_certificate_summary
            .clone(),
        completion_certificate_sha256: verified_bedrock_certificate
            .contract
            .completion_certificate_sha256
            .clone(),
        completion_certificate_algorithm: verified_bedrock_certificate
            .contract
            .completion_certificate_algorithm
            .clone(),
        summit_certificate_summary: verified_bedrock_certificate
            .contract
            .summit_certificate_summary
            .clone(),
        summit_certificate_sha256: verified_bedrock_certificate
            .contract
            .summit_certificate_sha256
            .clone(),
        summit_certificate_algorithm: verified_bedrock_certificate
            .contract
            .summit_certificate_algorithm
            .clone(),
        culmination_receipt_summary: verified_bedrock_certificate
            .contract
            .culmination_receipt_summary
            .clone(),
        culmination_receipt_sha256: verified_bedrock_certificate
            .contract
            .culmination_receipt_sha256
            .clone(),
        culmination_receipt_algorithm: verified_bedrock_certificate
            .contract
            .culmination_receipt_algorithm
            .clone(),
        pinnacle_receipt_summary: verified_bedrock_certificate
            .contract
            .pinnacle_receipt_summary
            .clone(),
        pinnacle_receipt_sha256: verified_bedrock_certificate
            .contract
            .pinnacle_receipt_sha256
            .clone(),
        pinnacle_receipt_algorithm: verified_bedrock_certificate
            .contract
            .pinnacle_receipt_algorithm
            .clone(),
        capstone_certificate_summary: verified_bedrock_certificate
            .contract
            .capstone_certificate_summary
            .clone(),
        capstone_certificate_sha256: verified_bedrock_certificate
            .contract
            .capstone_certificate_sha256
            .clone(),
        capstone_certificate_algorithm: verified_bedrock_certificate
            .contract
            .capstone_certificate_algorithm
            .clone(),
        keystone_receipt_summary: verified_bedrock_certificate
            .contract
            .keystone_receipt_summary
            .clone(),
        keystone_receipt_sha256: verified_bedrock_certificate
            .contract
            .keystone_receipt_sha256
            .clone(),
        keystone_receipt_algorithm: verified_bedrock_certificate
            .contract
            .keystone_receipt_algorithm
            .clone(),
        cornerstone_certificate_summary: verified_bedrock_certificate
            .contract
            .cornerstone_certificate_summary
            .clone(),
        cornerstone_certificate_sha256: verified_bedrock_certificate
            .contract
            .cornerstone_certificate_sha256
            .clone(),
        cornerstone_certificate_algorithm: verified_bedrock_certificate
            .contract
            .cornerstone_certificate_algorithm
            .clone(),
        foundation_receipt_summary: verified_bedrock_certificate
            .contract
            .foundation_receipt_summary
            .clone(),
        foundation_receipt_sha256: verified_bedrock_certificate
            .contract
            .foundation_receipt_sha256
            .clone(),
        foundation_receipt_algorithm: verified_bedrock_certificate
            .contract
            .foundation_receipt_algorithm
            .clone(),
        bedrock_certificate_summary: verified_bedrock_certificate
            .contract
            .bedrock_certificate_summary
            .clone(),
        bedrock_certificate_sha256: verified_bedrock_certificate
            .contract
            .bedrock_certificate_sha256
            .clone(),
        bedrock_certificate_algorithm: verified_bedrock_certificate
            .contract
            .bedrock_certificate_algorithm
            .clone(),
        basal_receipt_summary: basal_receipt_summary(
            result,
            &verified_bedrock_certificate
                .contract
                .reviewed_frozen_live_cutover_controller_command_summary,
        ),
        basal_receipt_sha256: basal_receipt_sha256.clone(),
        basal_receipt_algorithm: BASAL_RECEIPT_ALGORITHM.to_string(),
        explicit_statement: BASAL_RECEIPT_STATUS_EXPLICIT_STATEMENT.to_string(),
    };
    persist_json(&paths.status_path, &status)?;
    Ok(report_from_status(
        config,
        session_dir,
        &paths,
        &verified_bedrock_certificate.contract,
        classification.1,
        &status,
    ))
}

fn verify_live_package_basal_receipt_report(config: &Config) -> Result<PackageBasalReceiptReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for verify mode"))?;
    let paths = basal_receipt_paths(session_dir);
    let session: PackageBasalReceiptSession = load_json(&paths.session_path)?;
    let status: PackageBasalReceiptStatus = load_json(&paths.status_path)?;
    let mut mismatches = Vec::new();

    compare_string(
        &session.bedrock_certificate_session_dir,
        &config.bedrock_certificate_session_dir.display().to_string(),
        "basal-receipt bedrock_certificate_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "basal-receipt session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.session_dir,
        &session_dir.display().to_string(),
        "basal-receipt status session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.bedrock_certificate_session_dir,
        &config.bedrock_certificate_session_dir.display().to_string(),
        "basal-receipt status bedrock_certificate_session_dir",
        &mut mismatches,
    );

    let verified_bedrock_certificate = match package_bedrock_certificate_basal_receipt::verify_live_package_bedrock_certificate_for_basal_receipt(
        &config.bedrock_certificate_session_dir,
        config
            .confirmed_decision_packet_session_dir
            .as_deref()
            .ok_or_else(|| anyhow!("missing --confirm-decision-packet-session-dir for verify mode"))?,
    ) {
        Ok(value) => value,
        Err(error) => {
            let raw_contract = package_bedrock_certificate_basal_receipt::load_live_package_bedrock_certificate_contract_for_basal_receipt(
                &config.bedrock_certificate_session_dir,
            )?;
            return Ok(failure_report_for_verify(
                config,
                session_dir,
                &raw_contract,
                vec![format!(
                    "failed to verify bedrock-certificate session under {}: {error}",
                    config.bedrock_certificate_session_dir.display()
                )],
            ));
        }
    };

    compare_string(
        &session.foundation_receipt_session_dir,
        &verified_bedrock_certificate
            .contract
            .foundation_receipt_session_dir
            .display()
            .to_string(),
        "basal-receipt foundation_receipt_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.foundation_receipt_session_dir,
        &verified_bedrock_certificate
            .contract
            .foundation_receipt_session_dir
            .display()
            .to_string(),
        "basal-receipt status foundation_receipt_session_dir",
        &mut mismatches,
    );

    let stored_foundation_receipt_report: serde_json::Value = load_required_step_json(
        &status.bedrock_certificate_step,
        &paths.bedrock_certificate_report_path,
        "bedrock_certificate_step",
        &mut mismatches,
    )?;
    let stored_foundation_receipt_report_view: NestedBedrockCertificateReportView =
        serde_json::from_value(stored_foundation_receipt_report.clone()).with_context(|| {
            format!(
                "failed parsing archived nested bedrock-certificate report {}",
                paths.bedrock_certificate_report_path.display()
            )
        })?;
    let verified_bedrock_certificate_report_view: NestedBedrockCertificateReportView =
        serde_json::from_value(verified_bedrock_certificate.report_json.clone()).with_context(
            || {
                format!(
                    "failed parsing freshly verified nested bedrock-certificate report for {}",
                    config.bedrock_certificate_session_dir.display()
                )
            },
        )?;
    compare_json_ignoring_generated_at(
        &stored_foundation_receipt_report,
        &verified_bedrock_certificate.report_json,
        "basal-receipt archived bedrock-certificate report json",
        &mut mismatches,
    );
    compare_string(
        &stored_foundation_receipt_report_view.mode,
        &verified_bedrock_certificate_report_view.mode,
        "basal-receipt archived bedrock-certificate report mode",
        &mut mismatches,
    );
    compare_string(
        &stored_foundation_receipt_report_view.verdict,
        &verified_bedrock_certificate_report_view.verdict,
        "basal-receipt archived bedrock-certificate report verdict",
        &mut mismatches,
    );
    compare_string(
        &stored_foundation_receipt_report_view.reason,
        &verified_bedrock_certificate_report_view.reason,
        "basal-receipt archived bedrock-certificate report reason",
        &mut mismatches,
    );
    if stored_foundation_receipt_report_view.generated_at
        != verified_bedrock_certificate_report_view.generated_at
    {
        mismatches.push(format!(
            "basal-receipt archived bedrock-certificate report generated_at {:?} does not match freshly verified nested truth {:?}",
            stored_foundation_receipt_report_view.generated_at,
            verified_bedrock_certificate_report_view.generated_at
        ));
    }
    let expected_bedrock_certificate_step = step_artifact(
        &paths.bedrock_certificate_report_path,
        &verified_bedrock_certificate_report_view.mode,
        &verified_bedrock_certificate_report_view.verdict,
        &verified_bedrock_certificate_report_view.reason,
        verified_bedrock_certificate_report_view.generated_at,
    );
    compare_step_artifact(
        status.bedrock_certificate_step.as_ref(),
        &expected_bedrock_certificate_step,
        "basal-receipt bedrock_certificate_step",
        &mut mismatches,
    );
    if verified_bedrock_certificate.verdict != "tiny_live_package_bedrock_certificate_verify_ok" {
        mismatches.push(format!(
            "basal-receipt nested bedrock-certificate verification is non-green: {}",
            verified_bedrock_certificate.reason
        ));
    }

    let classification = classify_verified_bedrock_certificate(&verified_bedrock_certificate);
    let expected_basal_receipt_sha256 = compute_basal_receipt_sha256(
        &serialize_enum(&classification.0),
        verified_bedrock_certificate
            .contract
            .handoff_bundle_result
            .as_deref(),
        verified_bedrock_certificate
            .contract
            .decision_packet_result
            .as_deref(),
        verified_bedrock_certificate
            .contract
            .execute_frozen_result
            .as_deref(),
        verified_bedrock_certificate
            .contract
            .current_pre_activation_gate_verdict
            .as_deref(),
        verified_bedrock_certificate
            .contract
            .current_pre_activation_gate_reason
            .as_deref(),
        &verified_bedrock_certificate
            .contract
            .reviewed_frozen_live_cutover_controller_command_summary,
        &verified_bedrock_certificate
            .contract
            .chain_fingerprint_sha256,
        &verified_bedrock_certificate
            .contract
            .chain_fingerprint_algorithm,
        &verified_bedrock_certificate
            .contract
            .release_capsule_digest_manifest_sha256,
        verified_bedrock_certificate
            .contract
            .release_capsule_digest_manifest_entry_count,
        &verified_bedrock_certificate
            .contract
            .release_capsule_digest_algorithm,
        &verified_bedrock_certificate.contract.ledger_seal_sha256,
        &verified_bedrock_certificate.contract.ledger_seal_algorithm,
        &verified_bedrock_certificate.contract.registry_entry_sha256,
        &verified_bedrock_certificate
            .contract
            .registry_entry_algorithm,
        &verified_bedrock_certificate
            .contract
            .filing_certificate_sha256,
        &verified_bedrock_certificate
            .contract
            .filing_certificate_algorithm,
        &verified_bedrock_certificate.contract.archive_receipt_sha256,
        &verified_bedrock_certificate
            .contract
            .archive_receipt_algorithm,
        &verified_bedrock_certificate
            .contract
            .closure_certificate_summary,
        &verified_bedrock_certificate
            .contract
            .closure_certificate_sha256,
        &verified_bedrock_certificate
            .contract
            .closure_certificate_algorithm,
        &verified_bedrock_certificate
            .contract
            .finality_receipt_sha256,
        &verified_bedrock_certificate
            .contract
            .finality_receipt_algorithm,
        &verified_bedrock_certificate
            .contract
            .consummation_record_sha256,
        &verified_bedrock_certificate
            .contract
            .consummation_record_algorithm,
        &verified_bedrock_certificate
            .contract
            .completion_certificate_summary,
        &verified_bedrock_certificate
            .contract
            .completion_certificate_sha256,
        &verified_bedrock_certificate
            .contract
            .completion_certificate_algorithm,
        &verified_bedrock_certificate
            .contract
            .summit_certificate_summary,
        &verified_bedrock_certificate
            .contract
            .summit_certificate_sha256,
        &verified_bedrock_certificate
            .contract
            .summit_certificate_algorithm,
        &verified_bedrock_certificate
            .contract
            .culmination_receipt_summary,
        &verified_bedrock_certificate
            .contract
            .culmination_receipt_sha256,
        &verified_bedrock_certificate
            .contract
            .culmination_receipt_algorithm,
        &verified_bedrock_certificate
            .contract
            .pinnacle_receipt_summary,
        &verified_bedrock_certificate
            .contract
            .pinnacle_receipt_sha256,
        &verified_bedrock_certificate
            .contract
            .pinnacle_receipt_algorithm,
        &verified_bedrock_certificate
            .contract
            .capstone_certificate_summary,
        &verified_bedrock_certificate
            .contract
            .capstone_certificate_sha256,
        &verified_bedrock_certificate
            .contract
            .capstone_certificate_algorithm,
        &verified_bedrock_certificate
            .contract
            .keystone_receipt_summary,
        &verified_bedrock_certificate
            .contract
            .keystone_receipt_sha256,
        &verified_bedrock_certificate
            .contract
            .keystone_receipt_algorithm,
        &verified_bedrock_certificate
            .contract
            .cornerstone_certificate_summary,
        &verified_bedrock_certificate
            .contract
            .cornerstone_certificate_sha256,
        &verified_bedrock_certificate
            .contract
            .cornerstone_certificate_algorithm,
        &verified_bedrock_certificate
            .contract
            .foundation_receipt_summary,
        &verified_bedrock_certificate
            .contract
            .foundation_receipt_sha256,
        &verified_bedrock_certificate
            .contract
            .foundation_receipt_algorithm,
        &verified_bedrock_certificate
            .contract
            .bedrock_certificate_summary,
        &verified_bedrock_certificate
            .contract
            .bedrock_certificate_sha256,
        &verified_bedrock_certificate
            .contract
            .bedrock_certificate_algorithm,
    )?;

    let expected_session = expected_session_from_verified(
        config,
        session_dir,
        session.planned_at,
        &verified_bedrock_certificate.contract,
        classification.0,
        &expected_basal_receipt_sha256,
    );
    if session != expected_session {
        mismatches.push(
            "basal-receipt session contract does not match freshly verified bedrock-certificate truth"
                .to_string(),
        );
    }
    let expected_status = expected_status_from_verified(
        config,
        session_dir,
        status.updated_at,
        &verified_bedrock_certificate.contract,
        classification,
        expected_bedrock_certificate_step.clone(),
        &expected_basal_receipt_sha256,
    );
    if status != expected_status {
        mismatches.push(
            "basal-receipt status contract does not match freshly verified bedrock-certificate truth"
                .to_string(),
        );
    }

    if !mismatches.is_empty() {
        return Ok(failure_report_for_verify(
            config,
            session_dir,
            &verified_bedrock_certificate.contract,
            mismatches,
        ));
    }

    Ok(report_from_status(
        config,
        session_dir,
        &paths,
        &verified_bedrock_certificate.contract,
        TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyOk,
        &expected_status,
    ))
}

fn verify_foundation_receipt_command_summary(bedrock_certificate_session_dir: &Path) -> String {
    format!(
        "verify immutable bedrock-certificate session under {} before freezing the final basal receipt",
        bedrock_certificate_session_dir.display()
    )
}

fn classify_bedrock_certificate_contract(
    contract: &package_bedrock_certificate_basal_receipt::LivePackageBedrockCertificateContractView,
    assume_verified: bool,
) -> (
    LivePackageBasalReceiptResult,
    TinyLivePackageBasalReceiptVerdict,
    String,
) {
    if !assume_verified {
        return (
            LivePackageBasalReceiptResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptRefusedNowByInvalidOrDriftedContract,
            "the bedrock-certificate chain is not verified, so this basal receipt remains refused"
                .to_string(),
        );
    }
    match contract.result.as_deref() {
        Some("refused_now_by_stage3") => (
            LivePackageBasalReceiptResult::RefusedNowByStage3,
            TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptRefusedNowByStage3,
            "the immutable bedrock certificate remains refused now by Stage 3, so this basal receipt remains an explicit refusal record"
                .to_string(),
        ),
        Some("refused_now_by_pre_activation_gate") => (
            LivePackageBasalReceiptResult::RefusedNowByPreActivationGate,
            TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptRefusedNowByPreActivationGate,
            "the immutable bedrock certificate remains refused now by the pre-activation gate, so this basal receipt remains an explicit refusal record"
                .to_string(),
        ),
        Some("ready_for_manual_execution_when_gate_turns_green") => (
            LivePackageBasalReceiptResult::ReadyForManualExecutionWhenGateTurnsGreen,
            TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptReadyForManualExecutionWhenGateTurnsGreen,
            "the immutable bedrock certificate is coherent and this basal receipt is ready for manual execution when gate truth turns green"
                .to_string(),
        ),
        _ => (
            LivePackageBasalReceiptResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptRefusedNowByInvalidOrDriftedContract,
            "the verified bedrock-certificate chain is invalid, drifted, or non-runnable, so this basal receipt remains refused"
                .to_string(),
        ),
    }
}

fn classify_verified_bedrock_certificate(
    verified: &package_bedrock_certificate_basal_receipt::VerifiedLivePackageBedrockCertificateBasalReceiptStep,
) -> (
    LivePackageBasalReceiptResult,
    TinyLivePackageBasalReceiptVerdict,
    String,
) {
    if verified.verdict != "tiny_live_package_bedrock_certificate_verify_ok" {
        return (
            LivePackageBasalReceiptResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptRefusedNowByInvalidOrDriftedContract,
            "the immutable bedrock certificate did not verify cleanly, so this basal receipt remains refused"
                .to_string(),
        );
    }
    classify_bedrock_certificate_contract(&verified.contract, true)
}

fn basal_receipt_summary(
    result: LivePackageBasalReceiptResult,
    reviewed_frozen_live_cutover_controller_command_summary: &str,
) -> String {
    match result {
        LivePackageBasalReceiptResult::RefusedNowByStage3 => {
            "Basal receipt outcome: the immutable culminated chain remains refused now by Stage 3; archive this final circlet seal as refusal evidence only and do not run the frozen controller.".to_string()
        }
        LivePackageBasalReceiptResult::RefusedNowByPreActivationGate => {
            "Basal receipt outcome: the immutable culminated chain remains refused now by the pre-activation gate; archive this final circlet seal as refusal evidence only and do not run the frozen controller.".to_string()
        }
        LivePackageBasalReceiptResult::RefusedNowByInvalidOrDriftedContract => {
            "Basal receipt outcome: the immutable culminated chain is invalid or drifted; archive this final circlet seal for audit only and mint a new coherent certificate before any later execution handoff.".to_string()
        }
        LivePackageBasalReceiptResult::ReadyForManualExecutionWhenGateTurnsGreen => format!(
            "Basal receipt outcome: this immutable circlet seal is ready for manual execution when gate truth turns green; the exact reviewed frozen controller command remains: {reviewed_frozen_live_cutover_controller_command_summary}"
        ),
    }
}

fn basal_receipt_paths(session_dir: &Path) -> PackageBasalReceiptPaths {
    PackageBasalReceiptPaths {
        session_path: session_dir.join("tiny_live_activation_package_basal_receipt.session.json"),
        status_path: session_dir.join("tiny_live_activation_package_basal_receipt.status.json"),
        bedrock_certificate_report_path: session_dir.join(
            "tiny_live_activation_package_bedrock_certificate.foundation_receipt.report.json",
        ),
    }
}

fn ensure_clean_basal_receipt_session_dir(session_dir: &Path) -> Result<()> {
    if session_dir.exists() {
        bail!(
            "refusing to overwrite existing basal-receipt session dir {}",
            session_dir.display()
        );
    }
    fs::create_dir_all(session_dir).with_context(|| {
        format!(
            "failed creating basal-receipt session dir {}",
            session_dir.display()
        )
    })
}

fn planned_session(
    config: &Config,
    session_dir: &Path,
    contract: &package_bedrock_certificate_basal_receipt::LivePackageBedrockCertificateContractView,
    result: LivePackageBasalReceiptResult,
    basal_receipt_sha256: &str,
) -> PackageBasalReceiptSession {
    PackageBasalReceiptSession {
        session_version: BASAL_RECEIPT_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        bedrock_certificate_session_dir: config
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
        verify_foundation_receipt_command_summary: verify_foundation_receipt_command_summary(
            &config.bedrock_certificate_session_dir,
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
        basal_receipt_summary: basal_receipt_summary(
            result,
            &contract.reviewed_frozen_live_cutover_controller_command_summary,
        ),
        basal_receipt_sha256: basal_receipt_sha256.to_string(),
        basal_receipt_algorithm: BASAL_RECEIPT_ALGORITHM.to_string(),
        explicit_statement: BASAL_RECEIPT_SESSION_EXPLICIT_STATEMENT.to_string(),
    }
}

fn expected_session_from_verified(
    config: &Config,
    session_dir: &Path,
    planned_at: DateTime<Utc>,
    contract: &package_bedrock_certificate_basal_receipt::LivePackageBedrockCertificateContractView,
    result: LivePackageBasalReceiptResult,
    basal_receipt_sha256: &str,
) -> PackageBasalReceiptSession {
    PackageBasalReceiptSession {
        planned_at,
        ..planned_session(config, session_dir, contract, result, basal_receipt_sha256)
    }
}

fn expected_status_from_verified(
    config: &Config,
    session_dir: &Path,
    updated_at: DateTime<Utc>,
    contract: &package_bedrock_certificate_basal_receipt::LivePackageBedrockCertificateContractView,
    classification: (
        LivePackageBasalReceiptResult,
        TinyLivePackageBasalReceiptVerdict,
        String,
    ),
    bedrock_certificate_step: PackageBasalReceiptStepArtifact,
    basal_receipt_sha256: &str,
) -> PackageBasalReceiptStatus {
    PackageBasalReceiptStatus {
        status_version: BASAL_RECEIPT_STATUS_VERSION.to_string(),
        updated_at,
        session_dir: session_dir.display().to_string(),
        bedrock_certificate_session_dir: config
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
        bedrock_certificate_step: Some(bedrock_certificate_step),
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
        basal_receipt_summary: basal_receipt_summary(
            classification.0,
            &contract.reviewed_frozen_live_cutover_controller_command_summary,
        ),
        basal_receipt_sha256: basal_receipt_sha256.to_string(),
        basal_receipt_algorithm: BASAL_RECEIPT_ALGORITHM.to_string(),
        explicit_statement: BASAL_RECEIPT_STATUS_EXPLICIT_STATEMENT.to_string(),
    }
}

fn report_from_status(
    config: &Config,
    session_dir: &Path,
    paths: &PackageBasalReceiptPaths,
    contract: &package_bedrock_certificate_basal_receipt::LivePackageBedrockCertificateContractView,
    verdict: TinyLivePackageBasalReceiptVerdict,
    status: &PackageBasalReceiptStatus,
) -> PackageBasalReceiptReport {
    PackageBasalReceiptReport {
        generated_at: Utc::now(),
        mode: if verdict == TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyOk
        {
            "verify_live_package_basal_receipt".to_string()
        } else {
            "run_live_package_basal_receipt".to_string()
        },
        verdict,
        reason: status.reason.clone(),
        bedrock_certificate_session_dir: config
            .bedrock_certificate_session_dir
            .display()
            .to_string(),
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
        basal_receipt_session_path: Some(paths.session_path.display().to_string()),
        basal_receipt_status_path: Some(paths.status_path.display().to_string()),
        archived_bedrock_certificate_report_path: Some(
            paths.bedrock_certificate_report_path.display().to_string(),
        ),
        result: Some(serialize_enum(&status.result)),
        handoff_bundle_result: status.handoff_bundle_result.clone(),
        decision_packet_result: status.decision_packet_result.clone(),
        execute_frozen_result: status.execute_frozen_result.clone(),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        bedrock_certificate_step: status.bedrock_certificate_step.clone(),
        verify_foundation_receipt_command_summary: Some(verify_foundation_receipt_command_summary(
            &config.bedrock_certificate_session_dir,
        )),
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
        verification_mismatches: Vec::new(),
        explicit_statement: if verdict
            == TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyOk
        {
            BASAL_RECEIPT_VERIFY_EXPLICIT_STATEMENT.to_string()
        } else {
            status.explicit_statement.clone()
        },
    }
}

fn failure_report_without_contract(
    config: &Config,
    verdict: TinyLivePackageBasalReceiptVerdict,
    reason: String,
) -> PackageBasalReceiptReport {
    PackageBasalReceiptReport {
        generated_at: Utc::now(),
        mode: match config.mode {
            Mode::PlanLivePackageBasalReceipt => {
                "plan_live_package_basal_receipt".to_string()
            }
            Mode::RenderLivePackageBasalReceipt => {
                "render_live_package_basal_receipt".to_string()
            }
            Mode::RunLivePackageBasalReceipt => {
                "run_live_package_basal_receipt".to_string()
            }
            Mode::VerifyLivePackageBasalReceipt => {
                "verify_live_package_basal_receipt".to_string()
            }
        },
        verdict,
        reason,
        bedrock_certificate_session_dir: config
            .bedrock_certificate_session_dir
            .display()
            .to_string(),
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
        basal_receipt_session_path: None,
        basal_receipt_status_path: None,
        archived_bedrock_certificate_report_path: None,
        result: Some("refused_now_by_invalid_or_drifted_contract".to_string()),
        handoff_bundle_result: None,
        decision_packet_result: None,
        execute_frozen_result: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        bedrock_certificate_step: None,
        verify_foundation_receipt_command_summary: Some(verify_foundation_receipt_command_summary(
            &config.bedrock_certificate_session_dir,
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
        basal_receipt_algorithm: Some(BASAL_RECEIPT_ALGORITHM.to_string()),
        verification_mismatches: Vec::new(),
        explicit_statement:
            "this immutable basal receipt refused before persisting because the bedrock-certificate artifact could not be verified coherently"
                .to_string(),
    }
}

fn failure_report_for_verify(
    config: &Config,
    session_dir: &Path,
    contract: &package_bedrock_certificate_basal_receipt::LivePackageBedrockCertificateContractView,
    mismatches: Vec<String>,
) -> PackageBasalReceiptReport {
    let paths = basal_receipt_paths(session_dir);
    let reason = mismatches.first().cloned().unwrap_or_else(|| {
        "bedrock-certificate-native basal-receipt verification failed".to_string()
    });
    PackageBasalReceiptReport {
        generated_at: Utc::now(),
        mode: "verify_live_package_basal_receipt".to_string(),
        verdict: TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyInvalid,
        reason,
        bedrock_certificate_session_dir: config
            .bedrock_certificate_session_dir
            .display()
            .to_string(),
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
        basal_receipt_session_path: Some(paths.session_path.display().to_string()),
        basal_receipt_status_path: Some(paths.status_path.display().to_string()),
        archived_bedrock_certificate_report_path: Some(
            paths.bedrock_certificate_report_path.display().to_string(),
        ),
        result: None,
        handoff_bundle_result: contract.handoff_bundle_result.clone(),
        decision_packet_result: contract.decision_packet_result.clone(),
        execute_frozen_result: contract.execute_frozen_result.clone(),
        current_pre_activation_gate_verdict: contract.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: contract.current_pre_activation_gate_reason.clone(),
        bedrock_certificate_step: None,
        verify_foundation_receipt_command_summary: Some(verify_foundation_receipt_command_summary(
            &config.bedrock_certificate_session_dir,
        )),
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
        basal_receipt_summary: None,
        basal_receipt_sha256: None,
        basal_receipt_algorithm: Some(BASAL_RECEIPT_ALGORITHM.to_string()),
        verification_mismatches: mismatches,
        explicit_statement: BASAL_RECEIPT_VERIFY_EXPLICIT_STATEMENT.to_string(),
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
    step: &Option<PackageBasalReceiptStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<serde_json::Value> {
    let Some(step) = step else {
        mismatches.push(format!("{label} is missing from basal-receipt status"));
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
) -> PackageBasalReceiptStepArtifact {
    PackageBasalReceiptStepArtifact {
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
    actual: Option<&PackageBasalReceiptStepArtifact>,
    expected: &PackageBasalReceiptStepArtifact,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    let Some(actual) = actual else {
        mismatches.push(format!("{label} is missing from basal-receipt status"));
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

fn compute_basal_receipt_sha256(
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
) -> Result<String> {
    canonical_sha256_hex(&BasalReceiptPayload {
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
    fs::write(path, contents)
        .with_context(|| format!("failed writing basal-receipt script to {}", path.display()))?;
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

fn render_report_lines(report: &PackageBasalReceiptReport) -> String {
    let mut lines = vec![
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!(
            "bedrock_certificate_session_dir={}",
            report.bedrock_certificate_session_dir
        ),
    ];
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
    if let Some(value) = &report.cornerstone_certificate_sha256 {
        lines.push(format!("cornerstone_certificate_sha256={value}"));
    }
    if let Some(value) = &report.basal_receipt_sha256 {
        lines.push(format!("basal_receipt_sha256={value}"));
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
        let run = run_live_package_basal_receipt_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptReadyForManualExecutionWhenGateTurnsGreen
        );
        let verify = verify_live_package_basal_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyOk
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
        let run = run_live_package_basal_receipt_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptRefusedNowByStage3
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
        let run = run_live_package_basal_receipt_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptRefusedNowByPreActivationGate
        );
    }

    #[test]
    fn drifted_foundation_receipt_contract_is_refused() {
        let fixture = fake_command_fixture(
            "foundation_receipt_drifted_contract",
            json!({
                "generated_at": Utc::now(),
                "mode": "verify_live_package_foundation_receipt",
                "verdict": "tiny_live_package_foundation_receipt_verify_invalid",
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
        let run = run_live_package_basal_receipt_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptRefusedNowByInvalidOrDriftedContract
        );
    }

    #[test]
    fn run_refuses_managed_surface_overlap_before_writing_any_artifacts() {
        let fixture = fake_command_fixture(
            "basal_receipt_managed_overlap",
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
            .join("bedrock-certificate-session");
        fs::create_dir_all(overlap_session_dir.parent().unwrap()).unwrap();

        let config = Config {
            session_dir: Some(overlap_session_dir.clone()),
            ..fixture.run_config()
        };
        let error = run_live_package_basal_receipt_report(&config).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("overlaps the managed live target surface"),
            "{error:#}"
        );
        let paths = basal_receipt_paths(&overlap_session_dir);
        assert!(!paths.session_path.exists());
        assert!(!paths.status_path.exists());
    }

    #[test]
    fn verify_rejects_tampered_bedrock_certificate_step_path() {
        let fixture = fake_command_fixture(
            "foundation_receipt_tampered_step_path",
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
        run_live_package_basal_receipt_report(&fixture.run_config()).unwrap();
        let paths = basal_receipt_paths(&fixture.basal_receipt_session_dir);
        let mut status: PackageBasalReceiptStatus = load_json(&paths.status_path).unwrap();
        status.bedrock_certificate_step.as_mut().unwrap().path =
            "/tmp/foreign.foundation-receipt.report.json".to_string();
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_live_package_basal_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_foundation_receipt_text() {
        let fixture = fake_command_fixture(
            "foundation_receipt_tampered_text",
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
        run_live_package_basal_receipt_report(&fixture.run_config()).unwrap();
        let paths = basal_receipt_paths(&fixture.basal_receipt_session_dir);
        let mut session: PackageBasalReceiptSession = load_json(&paths.session_path).unwrap();
        session.foundation_receipt_summary = "tampered foundation receipt summary".to_string();
        persist_json(&paths.session_path, &session).unwrap();

        let verify = verify_live_package_basal_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyInvalid
        );
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
        run_live_package_basal_receipt_report(&fixture.run_config()).unwrap();
        let paths = basal_receipt_paths(&fixture.basal_receipt_session_dir);
        let mut status: PackageBasalReceiptStatus = load_json(&paths.status_path).unwrap();
        status.result = LivePackageBasalReceiptResult::RefusedNowByStage3;
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_live_package_basal_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyInvalid
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
        run_live_package_basal_receipt_report(&fixture.run_config()).unwrap();
        let paths = basal_receipt_paths(&fixture.basal_receipt_session_dir);
        let mut status: PackageBasalReceiptStatus = load_json(&paths.status_path).unwrap();
        status.current_pre_activation_gate_verdict = Some("red".to_string());
        status.current_pre_activation_gate_reason = Some("tampered".to_string());
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_live_package_basal_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_nested_foundation_receipt_report_content() {
        let fixture = fake_command_fixture(
            "foundation_receipt_tampered_nested_report",
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
        run_live_package_basal_receipt_report(&fixture.run_config()).unwrap();
        let paths = basal_receipt_paths(&fixture.basal_receipt_session_dir);
        let mut report: serde_json::Value =
            load_json(&paths.bedrock_certificate_report_path).unwrap();
        report.as_object_mut().unwrap().insert(
            "reason".to_string(),
            json!("tampered nested foundation-receipt reason"),
        );
        persist_json(&paths.bedrock_certificate_report_path, &report).unwrap();

        let verify = verify_live_package_basal_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_retimed_nested_foundation_receipt_report_and_step_generated_at() {
        let fixture = fake_command_fixture(
            "foundation_receipt_retimed_nested_report",
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
        run_live_package_basal_receipt_report(&fixture.run_config()).unwrap();
        let paths = basal_receipt_paths(&fixture.basal_receipt_session_dir);
        let mut report: serde_json::Value =
            load_json(&paths.bedrock_certificate_report_path).unwrap();
        let archived_generated_at =
            DateTime::parse_from_rfc3339(report["generated_at"].as_str().unwrap())
                .unwrap()
                .with_timezone(&Utc);
        let forged_generated_at = archived_generated_at + chrono::Duration::seconds(61);
        report.as_object_mut().unwrap().insert(
            "generated_at".to_string(),
            json!(forged_generated_at.to_rfc3339()),
        );
        persist_json(&paths.bedrock_certificate_report_path, &report).unwrap();

        let mut status: PackageBasalReceiptStatus = load_json(&paths.status_path).unwrap();
        status
            .bedrock_certificate_step
            .as_mut()
            .unwrap()
            .generated_at = forged_generated_at;
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_live_package_basal_receipt_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyInvalid
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
            run_live_package_basal_receipt_report(&fixture.run_config()).unwrap();
            let paths = basal_receipt_paths(&fixture.basal_receipt_session_dir);
            let mut status: PackageBasalReceiptStatus = load_json(&paths.status_path).unwrap();
            status.chain_fingerprint_sha256 = "tampered-chain".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_basal_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyInvalid
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
            run_live_package_basal_receipt_report(&fixture.run_config()).unwrap();
            let paths = basal_receipt_paths(&fixture.basal_receipt_session_dir);
            let mut status: PackageBasalReceiptStatus = load_json(&paths.status_path).unwrap();
            status.ledger_seal_sha256 = "tampered-ledger".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_basal_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyInvalid
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
            run_live_package_basal_receipt_report(&fixture.run_config()).unwrap();
            let paths = basal_receipt_paths(&fixture.basal_receipt_session_dir);
            let mut status: PackageBasalReceiptStatus = load_json(&paths.status_path).unwrap();
            status.registry_entry_sha256 = "tampered-registry".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_basal_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyInvalid
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
            run_live_package_basal_receipt_report(&fixture.run_config()).unwrap();
            let paths = basal_receipt_paths(&fixture.basal_receipt_session_dir);
            let mut status: PackageBasalReceiptStatus = load_json(&paths.status_path).unwrap();
            status.filing_certificate_sha256 = "tampered-filing".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_basal_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyInvalid
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
            run_live_package_basal_receipt_report(&fixture.run_config()).unwrap();
            let paths = basal_receipt_paths(&fixture.basal_receipt_session_dir);
            let mut status: PackageBasalReceiptStatus = load_json(&paths.status_path).unwrap();
            status.archive_receipt_sha256 = "tampered-archive".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_basal_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyInvalid
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
            run_live_package_basal_receipt_report(&fixture.run_config()).unwrap();
            let paths = basal_receipt_paths(&fixture.basal_receipt_session_dir);
            let mut status: PackageBasalReceiptStatus = load_json(&paths.status_path).unwrap();
            status.summit_certificate_sha256 = "tampered-summit".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_basal_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyInvalid
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
            run_live_package_basal_receipt_report(&fixture.run_config()).unwrap();
            let paths = basal_receipt_paths(&fixture.basal_receipt_session_dir);
            let mut status: PackageBasalReceiptStatus = load_json(&paths.status_path).unwrap();
            status.closure_certificate_sha256 = "tampered-closure".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_basal_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyInvalid
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
            run_live_package_basal_receipt_report(&fixture.run_config()).unwrap();
            let paths = basal_receipt_paths(&fixture.basal_receipt_session_dir);
            let mut status: PackageBasalReceiptStatus = load_json(&paths.status_path).unwrap();
            status.finality_receipt_sha256 = "tampered-finality".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_basal_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyInvalid
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
            run_live_package_basal_receipt_report(&fixture.run_config()).unwrap();
            let paths = basal_receipt_paths(&fixture.basal_receipt_session_dir);
            let mut status: PackageBasalReceiptStatus = load_json(&paths.status_path).unwrap();
            status.consummation_record_sha256 = "tampered-consummation".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_basal_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyInvalid
            );
        }
    }

    #[test]
    fn verify_rejects_tampered_bedrock_capstone_keystone_cornerstone_or_foundation_identity_fields()
    {
        {
            let fixture = fake_command_fixture(
                "basal_receipt_tampered_bedrock_identity",
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
            run_live_package_basal_receipt_report(&fixture.run_config()).unwrap();
            let paths = basal_receipt_paths(&fixture.basal_receipt_session_dir);
            let mut status: PackageBasalReceiptStatus = load_json(&paths.status_path).unwrap();
            status.bedrock_certificate_sha256 = "tampered-bedrock".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_basal_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "basal_receipt_tampered_capstone_identity",
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
            run_live_package_basal_receipt_report(&fixture.run_config()).unwrap();
            let paths = basal_receipt_paths(&fixture.basal_receipt_session_dir);
            let mut status: PackageBasalReceiptStatus = load_json(&paths.status_path).unwrap();
            status.capstone_certificate_sha256 = "tampered-capstone".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_basal_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "basal_receipt_tampered_keystone_identity",
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
            run_live_package_basal_receipt_report(&fixture.run_config()).unwrap();
            let paths = basal_receipt_paths(&fixture.basal_receipt_session_dir);
            let mut status: PackageBasalReceiptStatus = load_json(&paths.status_path).unwrap();
            status.keystone_receipt_sha256 = "tampered-keystone".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_basal_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "basal_receipt_tampered_cornerstone_identity",
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
            run_live_package_basal_receipt_report(&fixture.run_config()).unwrap();
            let paths = basal_receipt_paths(&fixture.basal_receipt_session_dir);
            let mut status: PackageBasalReceiptStatus = load_json(&paths.status_path).unwrap();
            status.cornerstone_certificate_sha256 = "tampered-cornerstone".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_basal_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "basal_receipt_tampered_foundation_identity",
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
            run_live_package_basal_receipt_report(&fixture.run_config()).unwrap();
            let paths = basal_receipt_paths(&fixture.basal_receipt_session_dir);
            let mut status: PackageBasalReceiptStatus = load_json(&paths.status_path).unwrap();
            status.foundation_receipt_sha256 = "tampered-foundation".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_basal_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyInvalid
            );
        }
    }

    #[test]
    fn verify_rejects_tampered_bedrock_certificate_summary_or_algorithm() {
        {
            let fixture = fake_command_fixture(
                "basal_receipt_tampered_bedrock_summary",
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
            run_live_package_basal_receipt_report(&fixture.run_config()).unwrap();
            let paths = basal_receipt_paths(&fixture.basal_receipt_session_dir);
            let mut status: PackageBasalReceiptStatus = load_json(&paths.status_path).unwrap();
            status.bedrock_certificate_summary = "tampered bedrock summary".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_basal_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "basal_receipt_tampered_bedrock_algorithm",
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
            run_live_package_basal_receipt_report(&fixture.run_config()).unwrap();
            let paths = basal_receipt_paths(&fixture.basal_receipt_session_dir);
            let mut status: PackageBasalReceiptStatus = load_json(&paths.status_path).unwrap();
            status.bedrock_certificate_algorithm = "tampered-bedrock-algorithm".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_basal_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyInvalid
            );
        }
    }

    #[test]
    fn verify_rejects_tampered_basal_receipt_summary_or_algorithm() {
        {
            let fixture = fake_command_fixture(
                "basal_receipt_tampered_basal_summary",
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
            run_live_package_basal_receipt_report(&fixture.run_config()).unwrap();
            let paths = basal_receipt_paths(&fixture.basal_receipt_session_dir);
            let mut status: PackageBasalReceiptStatus = load_json(&paths.status_path).unwrap();
            status.basal_receipt_summary = "tampered basal summary".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_basal_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyInvalid
            );
        }

        {
            let fixture = fake_command_fixture(
                "basal_receipt_tampered_basal_algorithm",
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
            run_live_package_basal_receipt_report(&fixture.run_config()).unwrap();
            let paths = basal_receipt_paths(&fixture.basal_receipt_session_dir);
            let mut status: PackageBasalReceiptStatus = load_json(&paths.status_path).unwrap();
            status.basal_receipt_algorithm = "tampered-basal-algorithm".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_basal_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyInvalid
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
            run_live_package_basal_receipt_report(&fixture.run_config()).unwrap();
            let paths = basal_receipt_paths(&fixture.basal_receipt_session_dir);
            let mut status: PackageBasalReceiptStatus = load_json(&paths.status_path).unwrap();
            status.closure_certificate_summary = "tampered closure summary".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_basal_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyInvalid
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
            run_live_package_basal_receipt_report(&fixture.run_config()).unwrap();
            let paths = basal_receipt_paths(&fixture.basal_receipt_session_dir);
            let mut status: PackageBasalReceiptStatus = load_json(&paths.status_path).unwrap();
            status.closure_certificate_algorithm = "sha512".to_string();
            persist_json(&paths.status_path, &status).unwrap();
            let verify =
                verify_live_package_basal_receipt_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageBasalReceiptVerdict::TinyLivePackageBasalReceiptVerifyInvalid
            );
        }
    }

    struct FakeCommandFixture {
        bin_dir: PathBuf,
        basal_receipt_session_dir: PathBuf,
        bedrock_certificate_session_dir: PathBuf,
        decision_packet_session_dir: PathBuf,
        install_root: PathBuf,
    }

    impl FakeCommandFixture {
        fn run_config(&self) -> Config {
            Config {
                mode: Mode::RunLivePackageBasalReceipt,
                bedrock_certificate_session_dir: self.bedrock_certificate_session_dir.clone(),
                confirmed_decision_packet_session_dir: Some(
                    self.decision_packet_session_dir.clone(),
                ),
                session_dir: Some(self.basal_receipt_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn verify_config(&self) -> Config {
            Config {
                mode: Mode::VerifyLivePackageBasalReceipt,
                bedrock_certificate_session_dir: self.bedrock_certificate_session_dir.clone(),
                confirmed_decision_packet_session_dir: Some(
                    self.decision_packet_session_dir.clone(),
                ),
                session_dir: Some(self.basal_receipt_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }
    }

    fn fake_command_fixture(label: &str, verify_report: serde_json::Value) -> FakeCommandFixture {
        let fixture_dir = temp_dir(label);
        let bin_dir = fixture_dir.join("bin");
        fs::create_dir_all(&bin_dir).unwrap();
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
            &bedrock_certificate_session_dir
                .join("tiny_live_activation_package_bedrock_certificate.session.json"),
            &stored_bedrock_certificate_session_fixture(
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
            &bedrock_certificate_session_dir
                .join("tiny_live_activation_package_bedrock_certificate.status.json"),
            &stored_bedrock_certificate_status_fixture(&verify_report),
        )
        .unwrap();
        persist_json(
            &bedrock_certificate_session_dir.join(
                "tiny_live_activation_package_bedrock_certificate.foundation_receipt.report.json",
            ),
            &json!({
                "decision_packet_session_dir": decision_packet_session_dir.display().to_string()
            }),
        )
        .unwrap();

        let mut verify_report = verify_report;
        bind_bedrock_certificate_report_to_fixture(
            &mut verify_report,
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
        let verify_path = fixture_dir.join("bedrock-certificate.verify.json");
        persist_json(&verify_path, &verify_report).unwrap();
        write_fake_bedrock_certificate_verify_command(
            &bin_dir.join("copybot_tiny_live_activation_package_bedrock_certificate"),
            &verify_path,
            &foundation_receipt_session_dir,
            &decision_packet_session_dir,
            &bedrock_certificate_session_dir,
        );

        FakeCommandFixture {
            bin_dir,
            basal_receipt_session_dir,
            bedrock_certificate_session_dir,
            decision_packet_session_dir,
            install_root,
        }
    }

    fn bind_bedrock_certificate_report_to_fixture(
        verify_report: &mut serde_json::Value,
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
            "verify_foundation_receipt_command_summary".to_string(),
            json!(format!(
                "verify immutable foundation-receipt session under {} before freezing the final bedrock certificate",
                foundation_receipt_session_dir.display()
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
            "explicit_statement".to_string(),
            json!("bedrock-certificate verify statement"),
        );
    }

    fn bedrock_certificate_verify_report(
        result: &str,
        handoff_bundle_result: &str,
        decision_packet_result: &str,
        execute_frozen_result: &str,
        gate_verdict: &str,
        gate_reason: &str,
    ) -> serde_json::Value {
        json!({
            "generated_at": Utc::now(),
            "mode": "verify_live_package_bedrock_certificate",
            "verdict": "tiny_live_package_bedrock_certificate_verify_ok",
            "reason": "bedrock-certificate chain is coherent",
            "result": result,
            "handoff_bundle_result": handoff_bundle_result,
            "decision_packet_result": decision_packet_result,
            "execute_frozen_result": execute_frozen_result,
            "current_pre_activation_gate_verdict": gate_verdict,
            "current_pre_activation_gate_reason": gate_reason
        })
    }

    fn capstone_fixture_verify_report(
        result: &str,
        handoff_bundle_result: &str,
        decision_packet_result: &str,
        execute_frozen_result: &str,
        gate_verdict: &str,
        gate_reason: &str,
    ) -> serde_json::Value {
        bedrock_certificate_verify_report(
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

    fn write_fake_bedrock_certificate_verify_command(
        path: &Path,
        verify_path: &Path,
        expected_foundation_receipt_session_dir: &Path,
        expected_decision_packet_session_dir: &Path,
        bedrock_certificate_session_dir: &Path,
    ) {
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --foundation-receipt-session-dir {} \"*) : ;;\n  *) echo 'unexpected foundation-receipt-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --confirm-decision-packet-session-dir {} \"*) : ;;\n  *) echo 'unexpected decision-packet-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --session-dir {} \"*) : ;;\n  *) echo 'unexpected bedrock-certificate session dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --verify-live-package-bedrock-certificate \"*) : ;;\n  *) echo 'missing bedrock verify mode' >&2; exit 1;;\nesac\ncat '{}'\n",
            shell_escape_path(expected_foundation_receipt_session_dir),
            shell_escape_path(expected_decision_packet_session_dir),
            shell_escape_path(bedrock_certificate_session_dir),
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

    fn stored_bedrock_certificate_session_fixture(
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
            "verify_foundation_receipt_command_summary".to_string(),
            json!(format!(
                "verify immutable foundation-receipt session under {} before freezing the final bedrock certificate",
                foundation_receipt_session_dir.display()
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
            "explicit_statement".to_string(),
            json!("bedrock-certificate statement"),
        );
        Value::Object(object)
    }

    fn stored_bedrock_certificate_status_fixture(verify_report: &Value) -> Value {
        let mut object = Map::new();
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
            "explicit_statement".to_string(),
            json!("bedrock-certificate status statement"),
        );
        Value::Object(object)
    }
}
