use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const PINNACLE_RECEIPT_BIN: &str = "copybot_tiny_live_activation_package_pinnacle_receipt";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LivePackagePinnacleReceiptContractView {
    pub pinnacle_receipt_session_dir: PathBuf,
    pub summit_certificate_session_dir: PathBuf,
    pub culmination_receipt_session_dir: PathBuf,
    pub registry_entry_session_dir: PathBuf,
    pub notarization_receipt_session_dir: PathBuf,
    pub provenance_certificate_session_dir: PathBuf,
    pub attestation_seal_session_dir: PathBuf,
    pub release_capsule_session_dir: PathBuf,
    pub activation_ticket_session_dir: PathBuf,
    pub review_receipt_session_dir: PathBuf,
    pub handoff_bundle_session_dir: PathBuf,
    pub decision_packet_session_dir: PathBuf,
    pub execute_frozen_session_dir: PathBuf,
    pub turn_green_session_dir: PathBuf,
    pub launch_packet_session_dir: PathBuf,
    pub package_dir: PathBuf,
    pub install_root: PathBuf,
    pub target_service_name: String,
    pub backend_command: String,
    pub wrapper_timeout_ms: u64,
    pub service_status_max_staleness_ms: u64,
    pub result: Option<String>,
    pub handoff_bundle_result: Option<String>,
    pub decision_packet_result: Option<String>,
    pub execute_frozen_result: Option<String>,
    pub current_pre_activation_gate_verdict: Option<String>,
    pub current_pre_activation_gate_reason: Option<String>,
    pub verify_pinnacle_receipt_command_summary: String,
    pub reviewed_frozen_live_cutover_controller_command_summary: String,
    pub provenance_certificate_summary: String,
    pub chain_fingerprint_summary: String,
    pub chain_fingerprint_sha256: String,
    pub chain_fingerprint_algorithm: String,
    pub release_capsule_digest_manifest_sha256: String,
    pub release_capsule_digest_manifest_entry_count: usize,
    pub release_capsule_digest_algorithm: String,
    pub notarization_receipt_summary: String,
    pub ledger_seal_summary: String,
    pub ledger_seal_sha256: String,
    pub ledger_seal_algorithm: String,
    pub registry_entry_summary: String,
    pub registry_entry_sha256: String,
    pub registry_entry_algorithm: String,
    pub filing_certificate_summary: String,
    pub filing_certificate_sha256: String,
    pub filing_certificate_algorithm: String,
    pub archive_receipt_summary: String,
    pub archive_receipt_sha256: String,
    pub archive_receipt_algorithm: String,
    pub closure_certificate_summary: String,
    pub closure_certificate_sha256: String,
    pub closure_certificate_algorithm: String,
    pub finality_receipt_summary: String,
    pub finality_receipt_sha256: String,
    pub finality_receipt_algorithm: String,
    pub consummation_record_summary: String,
    pub consummation_record_sha256: String,
    pub consummation_record_algorithm: String,
    pub completion_certificate_summary: String,
    pub completion_certificate_sha256: String,
    pub completion_certificate_algorithm: String,
    pub summit_certificate_summary: String,
    pub summit_certificate_sha256: String,
    pub summit_certificate_algorithm: String,
    pub culmination_receipt_summary: String,
    pub culmination_receipt_sha256: String,
    pub culmination_receipt_algorithm: String,
    pub pinnacle_receipt_summary: String,
    pub pinnacle_receipt_sha256: String,
    pub pinnacle_receipt_algorithm: String,
    pub explicit_statement: String,
}

#[derive(Debug, Clone)]
pub struct VerifiedLivePackagePinnacleReceiptCapstoneCertificateStep {
    pub report_json: serde_json::Value,
    pub verdict: String,
    pub reason: String,
    pub generated_at: DateTime<Utc>,
    pub contract: LivePackagePinnacleReceiptContractView,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackagePinnacleReceiptArtifactPaths {
    pub session_path: PathBuf,
    pub status_path: PathBuf,
    pub summit_certificate_report_path: PathBuf,
}

#[derive(Debug, Deserialize)]
struct StoredPinnacleReceiptSession {
    summit_certificate_session_dir: String,
    culmination_receipt_session_dir: String,
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
    #[serde(alias = "verify_summit_certificate_command_summary")]
    #[serde(alias = "verify_culmination_receipt_command_summary")]
    #[serde(alias = "verify_completion_certificate_command_summary")]
    verify_pinnacle_receipt_command_summary: String,
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
    explicit_statement: String,
}

#[derive(Debug, Deserialize)]
struct StoredPinnacleReceiptStatus {
    result: String,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
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
    explicit_statement: String,
}

#[derive(Debug, Deserialize)]
struct StoredSummitCertificateReportView {
    decision_packet_session_dir: String,
}

#[derive(Debug, Deserialize)]
struct PinnacleReceiptVerifyReportView {
    generated_at: DateTime<Utc>,
    verdict: String,
    reason: String,
    summit_certificate_session_dir: String,
    culmination_receipt_session_dir: String,
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
    result: Option<String>,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    #[serde(alias = "verify_summit_certificate_command_summary")]
    #[serde(alias = "verify_culmination_receipt_command_summary")]
    #[serde(alias = "verify_completion_certificate_command_summary")]
    verify_pinnacle_receipt_command_summary: Option<String>,
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
    explicit_statement: String,
}

pub fn load_live_package_pinnacle_receipt_contract_for_capstone_certificate(
    pinnacle_receipt_session_dir: &Path,
) -> Result<LivePackagePinnacleReceiptContractView> {
    let paths = pinnacle_receipt_artifact_paths(pinnacle_receipt_session_dir);
    let session: StoredPinnacleReceiptSession = load_json(&paths.session_path)?;
    let status: StoredPinnacleReceiptStatus = load_json(&paths.status_path)?;
    Ok(LivePackagePinnacleReceiptContractView {
        pinnacle_receipt_session_dir: pinnacle_receipt_session_dir.to_path_buf(),
        summit_certificate_session_dir: PathBuf::from(session.summit_certificate_session_dir),
        culmination_receipt_session_dir: PathBuf::from(session.culmination_receipt_session_dir),
        registry_entry_session_dir: PathBuf::from(session.registry_entry_session_dir),
        notarization_receipt_session_dir: PathBuf::from(session.notarization_receipt_session_dir),
        provenance_certificate_session_dir: PathBuf::from(
            session.provenance_certificate_session_dir,
        ),
        attestation_seal_session_dir: PathBuf::from(session.attestation_seal_session_dir),
        release_capsule_session_dir: PathBuf::from(session.release_capsule_session_dir),
        activation_ticket_session_dir: PathBuf::from(session.activation_ticket_session_dir),
        review_receipt_session_dir: PathBuf::from(session.review_receipt_session_dir),
        handoff_bundle_session_dir: PathBuf::from(session.handoff_bundle_session_dir),
        decision_packet_session_dir: PathBuf::from(session.decision_packet_session_dir),
        execute_frozen_session_dir: PathBuf::from(session.execute_frozen_session_dir),
        turn_green_session_dir: PathBuf::from(session.turn_green_session_dir),
        launch_packet_session_dir: PathBuf::from(session.launch_packet_session_dir),
        package_dir: PathBuf::from(session.package_dir),
        install_root: PathBuf::from(session.install_root),
        target_service_name: session.target_service_name,
        backend_command: session.backend_command,
        wrapper_timeout_ms: session.wrapper_timeout_ms,
        service_status_max_staleness_ms: session.service_status_max_staleness_ms,
        result: Some(status.result),
        handoff_bundle_result: status.handoff_bundle_result,
        decision_packet_result: status.decision_packet_result,
        execute_frozen_result: status.execute_frozen_result,
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason,
        verify_pinnacle_receipt_command_summary: session
            .verify_pinnacle_receipt_command_summary,
        reviewed_frozen_live_cutover_controller_command_summary: session
            .reviewed_frozen_live_cutover_controller_command_summary,
        provenance_certificate_summary: if status.provenance_certificate_summary.is_empty() {
            session.provenance_certificate_summary
        } else {
            status.provenance_certificate_summary
        },
        chain_fingerprint_summary: if status.chain_fingerprint_summary.is_empty() {
            session.chain_fingerprint_summary
        } else {
            status.chain_fingerprint_summary
        },
        chain_fingerprint_sha256: if status.chain_fingerprint_sha256.is_empty() {
            session.chain_fingerprint_sha256
        } else {
            status.chain_fingerprint_sha256
        },
        chain_fingerprint_algorithm: if status.chain_fingerprint_algorithm.is_empty() {
            session.chain_fingerprint_algorithm
        } else {
            status.chain_fingerprint_algorithm
        },
        release_capsule_digest_manifest_sha256: if status
            .release_capsule_digest_manifest_sha256
            .is_empty()
        {
            session.release_capsule_digest_manifest_sha256
        } else {
            status.release_capsule_digest_manifest_sha256
        },
        release_capsule_digest_manifest_entry_count: if status
            .release_capsule_digest_manifest_entry_count
            == 0
        {
            session.release_capsule_digest_manifest_entry_count
        } else {
            status.release_capsule_digest_manifest_entry_count
        },
        release_capsule_digest_algorithm: if status.release_capsule_digest_algorithm.is_empty() {
            session.release_capsule_digest_algorithm
        } else {
            status.release_capsule_digest_algorithm
        },
        notarization_receipt_summary: if status.notarization_receipt_summary.is_empty() {
            session.notarization_receipt_summary
        } else {
            status.notarization_receipt_summary
        },
        ledger_seal_summary: if status.ledger_seal_summary.is_empty() {
            session.ledger_seal_summary
        } else {
            status.ledger_seal_summary
        },
        ledger_seal_sha256: if status.ledger_seal_sha256.is_empty() {
            session.ledger_seal_sha256
        } else {
            status.ledger_seal_sha256
        },
        ledger_seal_algorithm: if status.ledger_seal_algorithm.is_empty() {
            session.ledger_seal_algorithm
        } else {
            status.ledger_seal_algorithm
        },
        registry_entry_summary: if status.registry_entry_summary.is_empty() {
            session.registry_entry_summary
        } else {
            status.registry_entry_summary
        },
        registry_entry_sha256: if status.registry_entry_sha256.is_empty() {
            session.registry_entry_sha256
        } else {
            status.registry_entry_sha256
        },
        registry_entry_algorithm: if status.registry_entry_algorithm.is_empty() {
            session.registry_entry_algorithm
        } else {
            status.registry_entry_algorithm
        },
        filing_certificate_summary: if status.filing_certificate_summary.is_empty() {
            session.filing_certificate_summary
        } else {
            status.filing_certificate_summary
        },
        filing_certificate_sha256: if status.filing_certificate_sha256.is_empty() {
            session.filing_certificate_sha256
        } else {
            status.filing_certificate_sha256
        },
        filing_certificate_algorithm: if status.filing_certificate_algorithm.is_empty() {
            session.filing_certificate_algorithm
        } else {
            status.filing_certificate_algorithm
        },
        archive_receipt_summary: if status.archive_receipt_summary.is_empty() {
            session.archive_receipt_summary
        } else {
            status.archive_receipt_summary
        },
        archive_receipt_sha256: if status.archive_receipt_sha256.is_empty() {
            session.archive_receipt_sha256
        } else {
            status.archive_receipt_sha256
        },
        archive_receipt_algorithm: if status.archive_receipt_algorithm.is_empty() {
            session.archive_receipt_algorithm
        } else {
            status.archive_receipt_algorithm
        },
        closure_certificate_summary: if status.closure_certificate_summary.is_empty() {
            session.closure_certificate_summary
        } else {
            status.closure_certificate_summary
        },
        closure_certificate_sha256: if status.closure_certificate_sha256.is_empty() {
            session.closure_certificate_sha256
        } else {
            status.closure_certificate_sha256
        },
        closure_certificate_algorithm: if status.closure_certificate_algorithm.is_empty() {
            session.closure_certificate_algorithm
        } else {
            status.closure_certificate_algorithm
        },
        finality_receipt_summary: if status.finality_receipt_summary.is_empty() {
            session.finality_receipt_summary
        } else {
            status.finality_receipt_summary
        },
        finality_receipt_sha256: if status.finality_receipt_sha256.is_empty() {
            session.finality_receipt_sha256
        } else {
            status.finality_receipt_sha256
        },
        finality_receipt_algorithm: if status.finality_receipt_algorithm.is_empty() {
            session.finality_receipt_algorithm
        } else {
            status.finality_receipt_algorithm
        },
        consummation_record_summary: if status.consummation_record_summary.is_empty() {
            session.consummation_record_summary
        } else {
            status.consummation_record_summary
        },
        consummation_record_sha256: if status.consummation_record_sha256.is_empty() {
            session.consummation_record_sha256
        } else {
            status.consummation_record_sha256
        },
        consummation_record_algorithm: if status.consummation_record_algorithm.is_empty() {
            session.consummation_record_algorithm
        } else {
            status.consummation_record_algorithm
        },
        completion_certificate_summary: if status.completion_certificate_summary.is_empty() {
            session.completion_certificate_summary
        } else {
            status.completion_certificate_summary
        },
        completion_certificate_sha256: if status.completion_certificate_sha256.is_empty() {
            session.completion_certificate_sha256
        } else {
            status.completion_certificate_sha256
        },
        completion_certificate_algorithm: if status.completion_certificate_algorithm.is_empty() {
            session.completion_certificate_algorithm
        } else {
            status.completion_certificate_algorithm
        },
        summit_certificate_summary: if status.summit_certificate_summary.is_empty() {
            session.summit_certificate_summary
        } else {
            status.summit_certificate_summary
        },
        summit_certificate_sha256: if status.summit_certificate_sha256.is_empty() {
            session.summit_certificate_sha256
        } else {
            status.summit_certificate_sha256
        },
        summit_certificate_algorithm: if status.summit_certificate_algorithm.is_empty() {
            session.summit_certificate_algorithm
        } else {
            status.summit_certificate_algorithm
        },
        culmination_receipt_summary: if status.culmination_receipt_summary.is_empty() {
            session.culmination_receipt_summary
        } else {
            status.culmination_receipt_summary
        },
        culmination_receipt_sha256: if status.culmination_receipt_sha256.is_empty() {
            session.culmination_receipt_sha256
        } else {
            status.culmination_receipt_sha256
        },
        culmination_receipt_algorithm: if status.culmination_receipt_algorithm.is_empty() {
            session.culmination_receipt_algorithm
        } else {
            status.culmination_receipt_algorithm
        },
        pinnacle_receipt_summary: if status.pinnacle_receipt_summary.is_empty() {
            session.pinnacle_receipt_summary
        } else {
            status.pinnacle_receipt_summary
        },
        pinnacle_receipt_sha256: if status.pinnacle_receipt_sha256.is_empty() {
            session.pinnacle_receipt_sha256
        } else {
            status.pinnacle_receipt_sha256
        },
        pinnacle_receipt_algorithm: if status.pinnacle_receipt_algorithm.is_empty() {
            session.pinnacle_receipt_algorithm
        } else {
            status.pinnacle_receipt_algorithm
        },
        explicit_statement: if status.explicit_statement.is_empty() {
            session.explicit_statement
        } else {
            status.explicit_statement
        },
    })
}

pub fn verify_live_package_pinnacle_receipt_for_capstone_certificate(
    pinnacle_receipt_session_dir: &Path,
    confirmed_decision_packet_session_dir: &Path,
) -> Result<VerifiedLivePackagePinnacleReceiptCapstoneCertificateStep> {
    let contract = load_live_package_pinnacle_receipt_contract_for_capstone_certificate(
        pinnacle_receipt_session_dir,
    )?;
    let trusted_decision_packet_session_dir =
        load_confirmed_decision_packet_session_dir_for_verify(
            pinnacle_receipt_session_dir,
            confirmed_decision_packet_session_dir,
            &contract,
        )?;
    let raw_report = run_json_command(
        PINNACLE_RECEIPT_BIN,
        &pinnacle_receipt_verify_args(
            &contract.summit_certificate_session_dir,
            &trusted_decision_packet_session_dir,
            pinnacle_receipt_session_dir,
        ),
    )?;
    let report: PinnacleReceiptVerifyReportView = serde_json::from_value(raw_report.clone())
        .context("failed parsing pinnacle-receipt verify report")?;
    let reported_summit_certificate_session_dir =
        PathBuf::from(&report.summit_certificate_session_dir);
    if reported_summit_certificate_session_dir != contract.summit_certificate_session_dir {
        bail!(
            "pinnacle-receipt verify report summit_certificate_session_dir {:?} does not match trusted {:?}",
            reported_summit_certificate_session_dir.display(),
            contract.summit_certificate_session_dir.display()
        );
    }
    let reported_culmination_receipt_session_dir =
        PathBuf::from(&report.culmination_receipt_session_dir);
    if reported_culmination_receipt_session_dir != contract.culmination_receipt_session_dir {
        bail!(
            "pinnacle-receipt verify report culmination_receipt_session_dir {:?} does not match trusted {:?}",
            reported_culmination_receipt_session_dir.display(),
            contract.culmination_receipt_session_dir.display()
        );
    }
    let reported_decision_packet_session_dir = PathBuf::from(required_string_field(
        report.decision_packet_session_dir,
        "decision_packet_session_dir",
    )?);
    if reported_decision_packet_session_dir != trusted_decision_packet_session_dir {
        bail!(
            "pinnacle-receipt verify report decision_packet_session_dir {:?} does not match trusted {:?}",
            reported_decision_packet_session_dir.display(),
            trusted_decision_packet_session_dir.display()
        );
    }
    let reported_registry_entry_session_dir = PathBuf::from(required_string_field(
        report.registry_entry_session_dir,
        "registry_entry_session_dir",
    )?);
    if reported_registry_entry_session_dir != contract.registry_entry_session_dir {
        bail!(
            "pinnacle-receipt verify report registry_entry_session_dir {:?} does not match trusted {:?}",
            reported_registry_entry_session_dir.display(),
            contract.registry_entry_session_dir.display()
        );
    }
    Ok(VerifiedLivePackagePinnacleReceiptCapstoneCertificateStep {
        report_json: raw_report,
        verdict: report.verdict.clone(),
        reason: report.reason.clone(),
        generated_at: report.generated_at,
        contract: LivePackagePinnacleReceiptContractView {
            pinnacle_receipt_session_dir: pinnacle_receipt_session_dir.to_path_buf(),
            summit_certificate_session_dir: reported_summit_certificate_session_dir,
            culmination_receipt_session_dir: reported_culmination_receipt_session_dir,
            registry_entry_session_dir: reported_registry_entry_session_dir,
            notarization_receipt_session_dir: PathBuf::from(required_string_field(
                report.notarization_receipt_session_dir,
                "notarization_receipt_session_dir",
            )?),
            provenance_certificate_session_dir: PathBuf::from(required_string_field(
                report.provenance_certificate_session_dir,
                "provenance_certificate_session_dir",
            )?),
            attestation_seal_session_dir: PathBuf::from(required_string_field(
                report.attestation_seal_session_dir,
                "attestation_seal_session_dir",
            )?),
            release_capsule_session_dir: PathBuf::from(required_string_field(
                report.release_capsule_session_dir,
                "release_capsule_session_dir",
            )?),
            activation_ticket_session_dir: PathBuf::from(required_string_field(
                report.activation_ticket_session_dir,
                "activation_ticket_session_dir",
            )?),
            review_receipt_session_dir: PathBuf::from(required_string_field(
                report.review_receipt_session_dir,
                "review_receipt_session_dir",
            )?),
            handoff_bundle_session_dir: PathBuf::from(required_string_field(
                report.handoff_bundle_session_dir,
                "handoff_bundle_session_dir",
            )?),
            decision_packet_session_dir: trusted_decision_packet_session_dir,
            execute_frozen_session_dir: PathBuf::from(required_string_field(
                report.execute_frozen_session_dir,
                "execute_frozen_session_dir",
            )?),
            turn_green_session_dir: PathBuf::from(required_string_field(
                report.turn_green_session_dir,
                "turn_green_session_dir",
            )?),
            launch_packet_session_dir: PathBuf::from(required_string_field(
                report.launch_packet_session_dir,
                "launch_packet_session_dir",
            )?),
            package_dir: PathBuf::from(required_string_field(report.package_dir, "package_dir")?),
            install_root: PathBuf::from(required_string_field(
                report.install_root,
                "install_root",
            )?),
            target_service_name: required_string_field(
                report.target_service_name,
                "target_service_name",
            )?,
            backend_command: required_string_field(report.backend_command, "backend_command")?,
            wrapper_timeout_ms: required_u64_field(
                report.wrapper_timeout_ms,
                "wrapper_timeout_ms",
            )?,
            service_status_max_staleness_ms: required_u64_field(
                report.service_status_max_staleness_ms,
                "service_status_max_staleness_ms",
            )?,
            result: report.result.or(contract.result),
            handoff_bundle_result: report
                .handoff_bundle_result
                .or(contract.handoff_bundle_result),
            decision_packet_result: report
                .decision_packet_result
                .or(contract.decision_packet_result),
            execute_frozen_result: report
                .execute_frozen_result
                .or(contract.execute_frozen_result),
            current_pre_activation_gate_verdict: report
                .current_pre_activation_gate_verdict
                .or(contract.current_pre_activation_gate_verdict),
            current_pre_activation_gate_reason: report
                .current_pre_activation_gate_reason
                .or(contract.current_pre_activation_gate_reason),
            verify_pinnacle_receipt_command_summary: required_string_field(
                report.verify_pinnacle_receipt_command_summary,
                "verify_pinnacle_receipt_command_summary",
            )?,
            reviewed_frozen_live_cutover_controller_command_summary: required_string_field(
                report.reviewed_frozen_live_cutover_controller_command_summary,
                "reviewed_frozen_live_cutover_controller_command_summary",
            )?,
            provenance_certificate_summary: required_string_field(
                report.provenance_certificate_summary,
                "provenance_certificate_summary",
            )?,
            chain_fingerprint_summary: required_string_field(
                report.chain_fingerprint_summary,
                "chain_fingerprint_summary",
            )?,
            chain_fingerprint_sha256: required_string_field(
                report.chain_fingerprint_sha256,
                "chain_fingerprint_sha256",
            )?,
            chain_fingerprint_algorithm: required_string_field(
                report.chain_fingerprint_algorithm,
                "chain_fingerprint_algorithm",
            )?,
            release_capsule_digest_manifest_sha256: required_string_field(
                report.release_capsule_digest_manifest_sha256,
                "release_capsule_digest_manifest_sha256",
            )?,
            release_capsule_digest_manifest_entry_count: required_usize_field(
                report.release_capsule_digest_manifest_entry_count,
                "release_capsule_digest_manifest_entry_count",
            )?,
            release_capsule_digest_algorithm: required_string_field(
                report.release_capsule_digest_algorithm,
                "release_capsule_digest_algorithm",
            )?,
            notarization_receipt_summary: required_string_field(
                report.notarization_receipt_summary,
                "notarization_receipt_summary",
            )?,
            ledger_seal_summary: required_string_field(
                report.ledger_seal_summary,
                "ledger_seal_summary",
            )?,
            ledger_seal_sha256: required_string_field(
                report.ledger_seal_sha256,
                "ledger_seal_sha256",
            )?,
            ledger_seal_algorithm: required_string_field(
                report.ledger_seal_algorithm,
                "ledger_seal_algorithm",
            )?,
            registry_entry_summary: required_string_field(
                report.registry_entry_summary,
                "registry_entry_summary",
            )?,
            registry_entry_sha256: required_string_field(
                report.registry_entry_sha256,
                "registry_entry_sha256",
            )?,
            registry_entry_algorithm: required_string_field(
                report.registry_entry_algorithm,
                "registry_entry_algorithm",
            )?,
            filing_certificate_summary: required_string_field(
                report.filing_certificate_summary,
                "filing_certificate_summary",
            )?,
            filing_certificate_sha256: required_string_field(
                report.filing_certificate_sha256,
                "filing_certificate_sha256",
            )?,
            filing_certificate_algorithm: required_string_field(
                report.filing_certificate_algorithm,
                "filing_certificate_algorithm",
            )?,
            archive_receipt_summary: required_string_field(
                report.archive_receipt_summary,
                "archive_receipt_summary",
            )?,
            archive_receipt_sha256: required_string_field(
                report.archive_receipt_sha256,
                "archive_receipt_sha256",
            )?,
            archive_receipt_algorithm: required_string_field(
                report.archive_receipt_algorithm,
                "archive_receipt_algorithm",
            )?,
            closure_certificate_summary: required_string_field(
                report.closure_certificate_summary,
                "closure_certificate_summary",
            )?,
            closure_certificate_sha256: required_string_field(
                report.closure_certificate_sha256,
                "closure_certificate_sha256",
            )?,
            closure_certificate_algorithm: required_string_field(
                report.closure_certificate_algorithm,
                "closure_certificate_algorithm",
            )?,
            finality_receipt_summary: required_string_field(
                report.finality_receipt_summary,
                "finality_receipt_summary",
            )?,
            finality_receipt_sha256: required_string_field(
                report.finality_receipt_sha256,
                "finality_receipt_sha256",
            )?,
            finality_receipt_algorithm: required_string_field(
                report.finality_receipt_algorithm,
                "finality_receipt_algorithm",
            )?,
            consummation_record_summary: required_string_field(
                report.consummation_record_summary,
                "consummation_record_summary",
            )?,
            consummation_record_sha256: required_string_field(
                report.consummation_record_sha256,
                "consummation_record_sha256",
            )?,
            consummation_record_algorithm: required_string_field(
                report.consummation_record_algorithm,
                "consummation_record_algorithm",
            )?,
            completion_certificate_summary: required_string_field(
                report.completion_certificate_summary,
                "completion_certificate_summary",
            )?,
            completion_certificate_sha256: required_string_field(
                report.completion_certificate_sha256,
                "completion_certificate_sha256",
            )?,
            completion_certificate_algorithm: required_string_field(
                report.completion_certificate_algorithm,
                "completion_certificate_algorithm",
            )?,
            summit_certificate_summary: required_string_field(
                report.summit_certificate_summary,
                "summit_certificate_summary",
            )?,
            summit_certificate_sha256: required_string_field(
                report.summit_certificate_sha256,
                "summit_certificate_sha256",
            )?,
            summit_certificate_algorithm: required_string_field(
                report.summit_certificate_algorithm,
                "summit_certificate_algorithm",
            )?,
            culmination_receipt_summary: required_string_field(
                report.culmination_receipt_summary,
                "culmination_receipt_summary",
            )?,
            culmination_receipt_sha256: required_string_field(
                report.culmination_receipt_sha256,
                "culmination_receipt_sha256",
            )?,
            culmination_receipt_algorithm: required_string_field(
                report.culmination_receipt_algorithm,
                "culmination_receipt_algorithm",
            )?,
            pinnacle_receipt_summary: required_string_field(
                report.pinnacle_receipt_summary,
                "pinnacle_receipt_summary",
            )?,
            pinnacle_receipt_sha256: required_string_field(
                report.pinnacle_receipt_sha256,
                "pinnacle_receipt_sha256",
            )?,
            pinnacle_receipt_algorithm: required_string_field(
                report.pinnacle_receipt_algorithm,
                "pinnacle_receipt_algorithm",
            )?,
            explicit_statement: report.explicit_statement,
        },
    })
}

pub fn pinnacle_receipt_verify_args(
    summit_certificate_session_dir: &Path,
    decision_packet_session_dir: &Path,
    pinnacle_receipt_session_dir: &Path,
) -> Vec<String> {
    vec![
        "--summit-certificate-session-dir".to_string(),
        summit_certificate_session_dir.display().to_string(),
        "--confirm-decision-packet-session-dir".to_string(),
        decision_packet_session_dir.display().to_string(),
        "--session-dir".to_string(),
        pinnacle_receipt_session_dir.display().to_string(),
        "--verify-live-package-pinnacle-receipt".to_string(),
        "--json".to_string(),
    ]
}

pub fn pinnacle_receipt_artifact_paths(
    session_dir: &Path,
) -> PackagePinnacleReceiptArtifactPaths {
    PackagePinnacleReceiptArtifactPaths {
        session_path: session_dir
            .join("tiny_live_activation_package_pinnacle_receipt.session.json"),
        status_path: session_dir
            .join("tiny_live_activation_package_pinnacle_receipt.status.json"),
        summit_certificate_report_path: session_dir.join(
            "tiny_live_activation_package_pinnacle_receipt.summit_certificate.report.json",
        ),
    }
}

fn load_confirmed_decision_packet_session_dir_for_verify(
    pinnacle_receipt_session_dir: &Path,
    confirmed_decision_packet_session_dir: &Path,
    contract: &LivePackagePinnacleReceiptContractView,
) -> Result<PathBuf> {
    let paths = pinnacle_receipt_artifact_paths(pinnacle_receipt_session_dir);
    let archived_report: StoredSummitCertificateReportView =
        load_json(&paths.summit_certificate_report_path)?;
    let confirmed_decision_packet_session_dir = confirmed_decision_packet_session_dir.to_path_buf();
    if confirmed_decision_packet_session_dir != contract.decision_packet_session_dir {
        bail!(
            "confirmed decision-packet session dir {:?} does not match stored pinnacle-receipt contract {:?}",
            confirmed_decision_packet_session_dir.display(),
            contract.decision_packet_session_dir.display()
        );
    }
    let archived_decision_packet_session_dir =
        PathBuf::from(archived_report.decision_packet_session_dir);
    if confirmed_decision_packet_session_dir != archived_decision_packet_session_dir {
        bail!(
            "confirmed decision-packet session dir {:?} does not match archived nested summit-certificate report {:?}",
            confirmed_decision_packet_session_dir.display(),
            archived_decision_packet_session_dir.display()
        );
    }
    Ok(confirmed_decision_packet_session_dir)
}

fn required_string_field(value: Option<String>, field: &str) -> Result<String> {
    value.ok_or_else(|| anyhow!("pinnacle-receipt verify report is missing {field}"))
}

fn required_u64_field(value: Option<u64>, field: &str) -> Result<u64> {
    value.ok_or_else(|| anyhow!("pinnacle-receipt verify report is missing {field}"))
}

fn required_usize_field(value: Option<usize>, field: &str) -> Result<usize> {
    value.ok_or_else(|| anyhow!("pinnacle-receipt verify report is missing {field}"))
}

fn load_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let bytes = fs::read(path).with_context(|| format!("failed reading {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("failed parsing {}", path.display()))
}

fn run_json_command(binary: &str, args: &[String]) -> Result<serde_json::Value> {
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
                .map(|value| value.to_string())
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Map, Value};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn load_contract_reads_stored_pinnacle_receipt_files() {
        let dir = temp_dir("pinnacle_receipt_contract");
        fs::write(
            dir.join("tiny_live_activation_package_pinnacle_receipt.session.json"),
            serde_json::to_vec_pretty(&stored_pinnacle_receipt_session_value(
                "/tmp/decision-packet-session",
            ))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_pinnacle_receipt.status.json"),
            serde_json::to_vec_pretty(&stored_pinnacle_receipt_status_value(
                "refused_now_by_stage3",
                true,
            ))
            .unwrap(),
        )
        .unwrap();

        let contract =
            load_live_package_pinnacle_receipt_contract_for_capstone_certificate(&dir).unwrap();
        assert_eq!(
            contract.decision_packet_session_dir,
            PathBuf::from("/tmp/decision-packet-session")
        );
        assert_eq!(
            contract.registry_entry_session_dir,
            PathBuf::from("/tmp/registry-entry-session")
        );
        assert_eq!(contract.result.as_deref(), Some("refused_now_by_stage3"));
        assert_eq!(contract.closure_certificate_sha256, "bead1234");
        assert_eq!(contract.finality_receipt_sha256, "facefeed");
        assert_eq!(contract.consummation_record_sha256, "deadbeef");
        assert_eq!(contract.completion_certificate_sha256, "cafebabe");
        assert_eq!(contract.culmination_receipt_sha256, "ab12cd34");
        assert_eq!(contract.summit_certificate_sha256, "summit-sha256");
        assert_eq!(contract.pinnacle_receipt_sha256, "pinnacle-sha256");
    }

    #[test]
    fn pinnacle_receipt_verify_args_are_exact_and_bounded() {
        let args = pinnacle_receipt_verify_args(
            Path::new("/tmp/summit-certificate-session"),
            Path::new("/tmp/decision-packet-session"),
            Path::new("/tmp/pinnacle-receipt-session"),
        );
        assert_eq!(
            args,
            vec![
                "--summit-certificate-session-dir",
                "/tmp/summit-certificate-session",
                "--confirm-decision-packet-session-dir",
                "/tmp/decision-packet-session",
                "--session-dir",
                "/tmp/pinnacle-receipt-session",
                "--verify-live-package-pinnacle-receipt",
                "--json"
            ]
        );
    }

    #[test]
    fn confirmed_decision_packet_session_dir_must_match_stored_contract_and_archive() {
        let dir = temp_dir("pinnacle_receipt_confirmed_decision_packet");
        let trusted_decision_packet_session_dir = dir.join("trusted-decision-packet");
        let foreign_decision_packet_session_dir = dir.join("foreign-decision-packet");
        fs::create_dir_all(&trusted_decision_packet_session_dir).unwrap();
        fs::create_dir_all(&foreign_decision_packet_session_dir).unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_pinnacle_receipt.session.json"),
            serde_json::to_vec_pretty(&stored_pinnacle_receipt_session_value(
                &foreign_decision_packet_session_dir.display().to_string(),
            ))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_pinnacle_receipt.status.json"),
            serde_json::to_vec_pretty(&stored_pinnacle_receipt_status_value(
                "refused_now_by_invalid_or_drifted_contract",
                false,
            ))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_pinnacle_receipt.summit_certificate.report.json"),
            serde_json::to_vec_pretty(&json!({
                "decision_packet_session_dir": foreign_decision_packet_session_dir.display().to_string()
            }))
            .unwrap(),
        )
        .unwrap();

        let contract =
            load_live_package_pinnacle_receipt_contract_for_capstone_certificate(&dir).unwrap();
        let error = load_confirmed_decision_packet_session_dir_for_verify(
            &dir,
            &trusted_decision_packet_session_dir,
            &contract,
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("confirmed decision-packet session dir"),
            "{error:#}"
        );
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

    fn stored_pinnacle_receipt_session_value(decision_packet_session_dir: &str) -> Value {
        let mut object = Map::new();
        object.insert(
            "summit_certificate_session_dir".to_string(),
            json!("/tmp/summit-certificate-session"),
        );
        object.insert(
            "culmination_receipt_session_dir".to_string(),
            json!("/tmp/culmination-receipt-session"),
        );
        object.insert(
            "registry_entry_session_dir".to_string(),
            json!("/tmp/registry-entry-session"),
        );
        object.insert(
            "notarization_receipt_session_dir".to_string(),
            json!("/tmp/notarization-receipt-session"),
        );
        object.insert(
            "provenance_certificate_session_dir".to_string(),
            json!("/tmp/provenance-certificate-session"),
        );
        object.insert(
            "attestation_seal_session_dir".to_string(),
            json!("/tmp/attestation-seal-session"),
        );
        object.insert(
            "release_capsule_session_dir".to_string(),
            json!("/tmp/release-capsule-session"),
        );
        object.insert(
            "activation_ticket_session_dir".to_string(),
            json!("/tmp/activation-ticket-session"),
        );
        object.insert(
            "review_receipt_session_dir".to_string(),
            json!("/tmp/review-receipt-session"),
        );
        object.insert(
            "handoff_bundle_session_dir".to_string(),
            json!("/tmp/handoff-bundle-session"),
        );
        object.insert(
            "decision_packet_session_dir".to_string(),
            json!(decision_packet_session_dir),
        );
        object.insert(
            "execute_frozen_session_dir".to_string(),
            json!("/tmp/execute-frozen-session"),
        );
        object.insert(
            "turn_green_session_dir".to_string(),
            json!("/tmp/turn-green-session"),
        );
        object.insert(
            "launch_packet_session_dir".to_string(),
            json!("/tmp/launch-packet-session"),
        );
        object.insert("package_dir".to_string(), json!("/tmp/frozen-package"));
        object.insert("install_root".to_string(), json!("/"));
        object.insert(
            "target_service_name".to_string(),
            json!("solana-copy-bot.service"),
        );
        object.insert("backend_command".to_string(), json!("/tmp/backend"));
        object.insert("wrapper_timeout_ms".to_string(), json!(1200_u64));
        object.insert(
            "service_status_max_staleness_ms".to_string(),
            json!(4500_u64),
        );
        object.insert(
            "verify_summit_certificate_command_summary".to_string(),
            json!("verify summit certificate"),
        );
        object.insert(
            "reviewed_frozen_live_cutover_controller_command_summary".to_string(),
            json!("run frozen cutover"),
        );
        object.insert(
            "provenance_certificate_summary".to_string(),
            json!("provenance certificate"),
        );
        object.insert(
            "chain_fingerprint_summary".to_string(),
            json!("chain fingerprint"),
        );
        object.insert("chain_fingerprint_sha256".to_string(), json!("abc123"));
        object.insert("chain_fingerprint_algorithm".to_string(), json!("sha256"));
        object.insert(
            "release_capsule_digest_manifest_sha256".to_string(),
            json!("def456"),
        );
        object.insert(
            "release_capsule_digest_manifest_entry_count".to_string(),
            json!(9),
        );
        object.insert(
            "release_capsule_digest_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "notarization_receipt_summary".to_string(),
            json!("notarization receipt"),
        );
        object.insert("ledger_seal_summary".to_string(), json!("ledger seal"));
        object.insert("ledger_seal_sha256".to_string(), json!("feedface"));
        object.insert("ledger_seal_algorithm".to_string(), json!("sha256"));
        object.insert(
            "registry_entry_summary".to_string(),
            json!("registry entry"),
        );
        object.insert("registry_entry_sha256".to_string(), json!("c0ffee"));
        object.insert("registry_entry_algorithm".to_string(), json!("sha256"));
        object.insert(
            "filing_certificate_summary".to_string(),
            json!("filing certificate"),
        );
        object.insert("filing_certificate_sha256".to_string(), json!("f1f1f1f1"));
        object.insert("filing_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "archive_receipt_summary".to_string(),
            json!("archive receipt"),
        );
        object.insert("archive_receipt_sha256".to_string(), json!("c0ffee"));
        object.insert("archive_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "closure_certificate_summary".to_string(),
            json!("closure certificate"),
        );
        object.insert("closure_certificate_sha256".to_string(), json!("bead1234"));
        object.insert("closure_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "finality_receipt_summary".to_string(),
            json!("finality receipt"),
        );
        object.insert("finality_receipt_sha256".to_string(), json!("facefeed"));
        object.insert("finality_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "consummation_record_summary".to_string(),
            json!("consummation record"),
        );
        object.insert("consummation_record_sha256".to_string(), json!("deadbeef"));
        object.insert("consummation_record_algorithm".to_string(), json!("sha256"));
        object.insert(
            "explicit_statement".to_string(),
            json!("summit certificate statement"),
        );
        object.insert(
            "completion_certificate_summary".to_string(),
            json!("completion certificate"),
        );
        object.insert(
            "completion_certificate_sha256".to_string(),
            json!("cafebabe"),
        );
        object.insert(
            "completion_certificate_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "summit_certificate_summary".to_string(),
            json!("summit certificate"),
        );
        object.insert("summit_certificate_sha256".to_string(), json!("summit-sha256"));
        object.insert("summit_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "culmination_receipt_summary".to_string(),
            json!("culmination receipt"),
        );
        object.insert("culmination_receipt_sha256".to_string(), json!("ab12cd34"));
        object.insert("culmination_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "pinnacle_receipt_summary".to_string(),
            json!("pinnacle receipt"),
        );
        object.insert(
            "pinnacle_receipt_sha256".to_string(),
            json!("pinnacle-sha256"),
        );
        object.insert("pinnacle_receipt_algorithm".to_string(), json!("sha256"));
        Value::Object(object)
    }

    fn stored_pinnacle_receipt_status_value(result: &str, include_nested_results: bool) -> Value {
        let mut object = Map::new();
        object.insert("result".to_string(), json!(result));
        if include_nested_results {
            object.insert(
                "handoff_bundle_result".to_string(),
                json!("refused_now_by_stage3"),
            );
            object.insert(
                "decision_packet_result".to_string(),
                json!("refused_now_by_stage3"),
            );
            object.insert(
                "execute_frozen_result".to_string(),
                json!("refused_now_by_stage3"),
            );
        }
        object.insert(
            "provenance_certificate_summary".to_string(),
            json!("provenance certificate"),
        );
        object.insert(
            "chain_fingerprint_summary".to_string(),
            json!("chain fingerprint"),
        );
        object.insert("chain_fingerprint_sha256".to_string(), json!("abc123"));
        object.insert("chain_fingerprint_algorithm".to_string(), json!("sha256"));
        object.insert(
            "release_capsule_digest_manifest_sha256".to_string(),
            json!("def456"),
        );
        object.insert(
            "release_capsule_digest_manifest_entry_count".to_string(),
            json!(9),
        );
        object.insert(
            "release_capsule_digest_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "notarization_receipt_summary".to_string(),
            json!("notarization receipt"),
        );
        object.insert("ledger_seal_summary".to_string(), json!("ledger seal"));
        object.insert("ledger_seal_sha256".to_string(), json!("feedface"));
        object.insert("ledger_seal_algorithm".to_string(), json!("sha256"));
        object.insert(
            "registry_entry_summary".to_string(),
            json!("registry entry"),
        );
        object.insert("registry_entry_sha256".to_string(), json!("c0ffee"));
        object.insert("registry_entry_algorithm".to_string(), json!("sha256"));
        object.insert(
            "filing_certificate_summary".to_string(),
            json!("filing certificate"),
        );
        object.insert("filing_certificate_sha256".to_string(), json!("f1f1f1f1"));
        object.insert("filing_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "archive_receipt_summary".to_string(),
            json!("archive receipt"),
        );
        object.insert("archive_receipt_sha256".to_string(), json!("c0ffee"));
        object.insert("archive_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "closure_certificate_summary".to_string(),
            json!("closure certificate"),
        );
        object.insert("closure_certificate_sha256".to_string(), json!("bead1234"));
        object.insert("closure_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "finality_receipt_summary".to_string(),
            json!("finality receipt"),
        );
        object.insert("finality_receipt_sha256".to_string(), json!("facefeed"));
        object.insert("finality_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "consummation_record_summary".to_string(),
            json!("consummation record"),
        );
        object.insert("consummation_record_sha256".to_string(), json!("deadbeef"));
        object.insert("consummation_record_algorithm".to_string(), json!("sha256"));
        object.insert(
            "explicit_statement".to_string(),
            json!("summit certificate status statement"),
        );
        object.insert(
            "completion_certificate_summary".to_string(),
            json!("completion certificate"),
        );
        object.insert(
            "completion_certificate_sha256".to_string(),
            json!("cafebabe"),
        );
        object.insert(
            "completion_certificate_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "summit_certificate_summary".to_string(),
            json!("summit certificate"),
        );
        object.insert("summit_certificate_sha256".to_string(), json!("summit-sha256"));
        object.insert("summit_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "culmination_receipt_summary".to_string(),
            json!("culmination receipt"),
        );
        object.insert("culmination_receipt_sha256".to_string(), json!("ab12cd34"));
        object.insert("culmination_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "pinnacle_receipt_summary".to_string(),
            json!("pinnacle receipt"),
        );
        object.insert(
            "pinnacle_receipt_sha256".to_string(),
            json!("pinnacle-sha256"),
        );
        object.insert("pinnacle_receipt_algorithm".to_string(), json!("sha256"));
        Value::Object(object)
    }
}
