use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const CHANCEL_CERTIFICATE_BIN: &str = "copybot_tiny_live_activation_package_chancel_certificate";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LivePackageChancelCertificateContractView {
    pub chancel_certificate_session_dir: PathBuf,
    pub lectern_certificate_session_dir: PathBuf,
    pub pulpit_receipt_session_dir: PathBuf,
    pub dais_receipt_session_dir: PathBuf,
    pub pedestal_certificate_session_dir: PathBuf,
    pub plinth_receipt_session_dir: PathBuf,
    pub substructure_certificate_session_dir: PathBuf,
    pub basal_receipt_session_dir: PathBuf,
    pub bedrock_certificate_session_dir: PathBuf,
    pub foundation_receipt_session_dir: PathBuf,
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
    pub verify_pulpit_receipt_command_summary: String,
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
    pub capstone_certificate_summary: String,
    pub capstone_certificate_sha256: String,
    pub capstone_certificate_algorithm: String,
    pub keystone_receipt_summary: String,
    pub keystone_receipt_sha256: String,
    pub keystone_receipt_algorithm: String,
    pub cornerstone_certificate_summary: String,
    pub cornerstone_certificate_sha256: String,
    pub cornerstone_certificate_algorithm: String,
    pub foundation_receipt_summary: String,
    pub foundation_receipt_sha256: String,
    pub foundation_receipt_algorithm: String,
    pub bedrock_certificate_summary: String,
    pub bedrock_certificate_sha256: String,
    pub bedrock_certificate_algorithm: String,
    pub basal_receipt_summary: String,
    pub basal_receipt_sha256: String,
    pub basal_receipt_algorithm: String,
    pub substructure_certificate_summary: String,
    pub substructure_certificate_sha256: String,
    pub substructure_certificate_algorithm: String,
    pub plinth_receipt_summary: String,
    pub plinth_receipt_sha256: String,
    pub plinth_receipt_algorithm: String,
    pub pedestal_certificate_summary: String,
    pub pedestal_certificate_sha256: String,
    pub pedestal_certificate_algorithm: String,
    pub dais_receipt_summary: String,
    pub dais_receipt_sha256: String,
    pub dais_receipt_algorithm: String,
    pub rostrum_certificate_summary: String,
    pub rostrum_certificate_sha256: String,
    pub rostrum_certificate_algorithm: String,
    pub podium_receipt_summary: String,
    pub podium_receipt_sha256: String,
    pub podium_receipt_algorithm: String,
    pub lectern_certificate_summary: String,
    pub lectern_certificate_sha256: String,
    pub lectern_certificate_algorithm: String,
    pub pulpit_receipt_summary: String,
    pub pulpit_receipt_sha256: String,
    pub pulpit_receipt_algorithm: String,
    pub chancel_certificate_summary: String,
    pub chancel_certificate_sha256: String,
    pub chancel_certificate_algorithm: String,
    pub explicit_statement: String,
}

#[derive(Debug, Clone)]
pub struct VerifiedLivePackageChancelCertificateApseReceiptStep {
    pub report_json: serde_json::Value,
    pub verdict: String,
    pub reason: String,
    pub generated_at: DateTime<Utc>,
    pub contract: LivePackageChancelCertificateContractView,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageChancelCertificateArtifactPaths {
    pub session_path: PathBuf,
    pub status_path: PathBuf,
    pub pulpit_receipt_report_path: PathBuf,
}

#[derive(Debug, Deserialize)]
struct StoredChancelCertificateSession {
    lectern_certificate_session_dir: String,
    pulpit_receipt_session_dir: String,
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
    #[serde(
        alias = "verify_dais_receipt_command_summary",
        alias = "verify_pedestal_certificate_command_summary",
        alias = "verify_basal_receipt_command_summary",
        alias = "verify_substructure_certificate_command_summary"
    )]
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
    explicit_statement: String,
}

#[derive(Debug, Deserialize)]
struct StoredChancelCertificateStatus {
    lectern_certificate_session_dir: Option<String>,
    pulpit_receipt_session_dir: Option<String>,
    dais_receipt_session_dir: Option<String>,
    pedestal_certificate_session_dir: Option<String>,
    plinth_receipt_session_dir: Option<String>,
    substructure_certificate_session_dir: Option<String>,
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
    explicit_statement: String,
}

#[derive(Debug, Deserialize)]
struct StoredPulpitReceiptReportView {
    decision_packet_session_dir: String,
}

#[derive(Debug, Deserialize)]
struct ChancelCertificateVerifyReportView {
    generated_at: DateTime<Utc>,
    verdict: String,
    reason: String,
    lectern_certificate_session_dir: Option<String>,
    pulpit_receipt_session_dir: Option<String>,
    dais_receipt_session_dir: Option<String>,
    pedestal_certificate_session_dir: Option<String>,
    plinth_receipt_session_dir: Option<String>,
    substructure_certificate_session_dir: Option<String>,
    basal_receipt_session_dir: Option<String>,
    foundation_receipt_session_dir: Option<String>,
    bedrock_certificate_session_dir: String,
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
    #[serde(
        alias = "verify_dais_receipt_command_summary",
        alias = "verify_pedestal_certificate_command_summary",
        alias = "verify_substructure_certificate_command_summary"
    )]
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
    explicit_statement: String,
}

pub fn load_live_package_chancel_certificate_contract_for_apse_receipt(
    chancel_certificate_session_dir: &Path,
) -> Result<LivePackageChancelCertificateContractView> {
    let paths = chancel_certificate_artifact_paths(chancel_certificate_session_dir);
    let session: StoredChancelCertificateSession = load_json(&paths.session_path)?;
    let status: StoredChancelCertificateStatus = load_json(&paths.status_path)?;
    Ok(LivePackageChancelCertificateContractView {
        chancel_certificate_session_dir: chancel_certificate_session_dir.to_path_buf(),
        lectern_certificate_session_dir: PathBuf::from(
            status
                .lectern_certificate_session_dir
                .unwrap_or(session.lectern_certificate_session_dir),
        ),
        pulpit_receipt_session_dir: PathBuf::from(
            status
                .pulpit_receipt_session_dir
                .unwrap_or(session.pulpit_receipt_session_dir),
        ),
        dais_receipt_session_dir: PathBuf::from(
            status
                .dais_receipt_session_dir
                .unwrap_or(session.dais_receipt_session_dir),
        ),
        pedestal_certificate_session_dir: PathBuf::from(
            status
                .pedestal_certificate_session_dir
                .unwrap_or(session.pedestal_certificate_session_dir),
        ),
        plinth_receipt_session_dir: PathBuf::from(
            status
                .plinth_receipt_session_dir
                .unwrap_or(session.plinth_receipt_session_dir),
        ),
        substructure_certificate_session_dir: PathBuf::from(
            status
                .substructure_certificate_session_dir
                .unwrap_or(session.substructure_certificate_session_dir),
        ),
        basal_receipt_session_dir: PathBuf::from(session.basal_receipt_session_dir),
        bedrock_certificate_session_dir: PathBuf::from(session.bedrock_certificate_session_dir),
        foundation_receipt_session_dir: PathBuf::from(session.foundation_receipt_session_dir),
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
        verify_pulpit_receipt_command_summary: session.verify_pulpit_receipt_command_summary,
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
        capstone_certificate_summary: if status.capstone_certificate_summary.is_empty() {
            session.capstone_certificate_summary
        } else {
            status.capstone_certificate_summary
        },
        capstone_certificate_sha256: if status.capstone_certificate_sha256.is_empty() {
            session.capstone_certificate_sha256
        } else {
            status.capstone_certificate_sha256
        },
        capstone_certificate_algorithm: if status.capstone_certificate_algorithm.is_empty() {
            session.capstone_certificate_algorithm
        } else {
            status.capstone_certificate_algorithm
        },
        keystone_receipt_summary: if status.keystone_receipt_summary.is_empty() {
            session.keystone_receipt_summary
        } else {
            status.keystone_receipt_summary
        },
        keystone_receipt_sha256: if status.keystone_receipt_sha256.is_empty() {
            session.keystone_receipt_sha256
        } else {
            status.keystone_receipt_sha256
        },
        keystone_receipt_algorithm: if status.keystone_receipt_algorithm.is_empty() {
            session.keystone_receipt_algorithm
        } else {
            status.keystone_receipt_algorithm
        },
        cornerstone_certificate_summary: if status.cornerstone_certificate_summary.is_empty() {
            session.cornerstone_certificate_summary
        } else {
            status.cornerstone_certificate_summary
        },
        cornerstone_certificate_sha256: if status.cornerstone_certificate_sha256.is_empty() {
            session.cornerstone_certificate_sha256
        } else {
            status.cornerstone_certificate_sha256
        },
        cornerstone_certificate_algorithm: if status.cornerstone_certificate_algorithm.is_empty() {
            session.cornerstone_certificate_algorithm
        } else {
            status.cornerstone_certificate_algorithm
        },
        foundation_receipt_summary: if status.foundation_receipt_summary.is_empty() {
            session.foundation_receipt_summary
        } else {
            status.foundation_receipt_summary
        },
        foundation_receipt_sha256: if status.foundation_receipt_sha256.is_empty() {
            session.foundation_receipt_sha256
        } else {
            status.foundation_receipt_sha256
        },
        foundation_receipt_algorithm: if status.foundation_receipt_algorithm.is_empty() {
            session.foundation_receipt_algorithm
        } else {
            status.foundation_receipt_algorithm
        },
        bedrock_certificate_summary: if status.bedrock_certificate_summary.is_empty() {
            session.bedrock_certificate_summary
        } else {
            status.bedrock_certificate_summary
        },
        bedrock_certificate_sha256: if status.bedrock_certificate_sha256.is_empty() {
            session.bedrock_certificate_sha256
        } else {
            status.bedrock_certificate_sha256
        },
        bedrock_certificate_algorithm: if status.bedrock_certificate_algorithm.is_empty() {
            session.bedrock_certificate_algorithm
        } else {
            status.bedrock_certificate_algorithm
        },
        basal_receipt_summary: if status.basal_receipt_summary.is_empty() {
            session.basal_receipt_summary
        } else {
            status.basal_receipt_summary
        },
        basal_receipt_sha256: if status.basal_receipt_sha256.is_empty() {
            session.basal_receipt_sha256
        } else {
            status.basal_receipt_sha256
        },
        basal_receipt_algorithm: if status.basal_receipt_algorithm.is_empty() {
            session.basal_receipt_algorithm
        } else {
            status.basal_receipt_algorithm
        },
        substructure_certificate_summary: if status.substructure_certificate_summary.is_empty() {
            session.substructure_certificate_summary
        } else {
            status.substructure_certificate_summary
        },
        substructure_certificate_sha256: if status.substructure_certificate_sha256.is_empty() {
            session.substructure_certificate_sha256
        } else {
            status.substructure_certificate_sha256
        },
        substructure_certificate_algorithm: if status.substructure_certificate_algorithm.is_empty()
        {
            session.substructure_certificate_algorithm
        } else {
            status.substructure_certificate_algorithm
        },
        plinth_receipt_summary: if status.plinth_receipt_summary.is_empty() {
            session.plinth_receipt_summary
        } else {
            status.plinth_receipt_summary
        },
        plinth_receipt_sha256: if status.plinth_receipt_sha256.is_empty() {
            session.plinth_receipt_sha256
        } else {
            status.plinth_receipt_sha256
        },
        plinth_receipt_algorithm: if status.plinth_receipt_algorithm.is_empty() {
            session.plinth_receipt_algorithm
        } else {
            status.plinth_receipt_algorithm
        },
        pedestal_certificate_summary: if status.pedestal_certificate_summary.is_empty() {
            session.pedestal_certificate_summary
        } else {
            status.pedestal_certificate_summary
        },
        pedestal_certificate_sha256: if status.pedestal_certificate_sha256.is_empty() {
            session.pedestal_certificate_sha256
        } else {
            status.pedestal_certificate_sha256
        },
        pedestal_certificate_algorithm: if status.pedestal_certificate_algorithm.is_empty() {
            session.pedestal_certificate_algorithm
        } else {
            status.pedestal_certificate_algorithm
        },
        dais_receipt_summary: if status.dais_receipt_summary.is_empty() {
            session.dais_receipt_summary
        } else {
            status.dais_receipt_summary
        },
        dais_receipt_sha256: if status.dais_receipt_sha256.is_empty() {
            session.dais_receipt_sha256
        } else {
            status.dais_receipt_sha256
        },
        dais_receipt_algorithm: if status.dais_receipt_algorithm.is_empty() {
            session.dais_receipt_algorithm
        } else {
            status.dais_receipt_algorithm
        },
        rostrum_certificate_summary: if status.rostrum_certificate_summary.is_empty() {
            session.rostrum_certificate_summary
        } else {
            status.rostrum_certificate_summary
        },
        rostrum_certificate_sha256: if status.rostrum_certificate_sha256.is_empty() {
            session.rostrum_certificate_sha256
        } else {
            status.rostrum_certificate_sha256
        },
        rostrum_certificate_algorithm: if status.rostrum_certificate_algorithm.is_empty() {
            session.rostrum_certificate_algorithm
        } else {
            status.rostrum_certificate_algorithm
        },
        podium_receipt_summary: if status.podium_receipt_summary.is_empty() {
            session.podium_receipt_summary
        } else {
            status.podium_receipt_summary
        },
        podium_receipt_sha256: if status.podium_receipt_sha256.is_empty() {
            session.podium_receipt_sha256
        } else {
            status.podium_receipt_sha256
        },
        podium_receipt_algorithm: if status.podium_receipt_algorithm.is_empty() {
            session.podium_receipt_algorithm
        } else {
            status.podium_receipt_algorithm
        },
        lectern_certificate_summary: if status.lectern_certificate_summary.is_empty() {
            session.lectern_certificate_summary
        } else {
            status.lectern_certificate_summary
        },
        lectern_certificate_sha256: if status.lectern_certificate_sha256.is_empty() {
            session.lectern_certificate_sha256
        } else {
            status.lectern_certificate_sha256
        },
        lectern_certificate_algorithm: if status.lectern_certificate_algorithm.is_empty() {
            session.lectern_certificate_algorithm
        } else {
            status.lectern_certificate_algorithm
        },
        pulpit_receipt_summary: if status.pulpit_receipt_summary.is_empty() {
            session.pulpit_receipt_summary
        } else {
            status.pulpit_receipt_summary
        },
        pulpit_receipt_sha256: if status.pulpit_receipt_sha256.is_empty() {
            session.pulpit_receipt_sha256
        } else {
            status.pulpit_receipt_sha256
        },
        pulpit_receipt_algorithm: if status.pulpit_receipt_algorithm.is_empty() {
            session.pulpit_receipt_algorithm
        } else {
            status.pulpit_receipt_algorithm
        },
        chancel_certificate_summary: if status.chancel_certificate_summary.is_empty() {
            session.chancel_certificate_summary
        } else {
            status.chancel_certificate_summary
        },
        chancel_certificate_sha256: if status.chancel_certificate_sha256.is_empty() {
            session.chancel_certificate_sha256
        } else {
            status.chancel_certificate_sha256
        },
        chancel_certificate_algorithm: if status.chancel_certificate_algorithm.is_empty() {
            session.chancel_certificate_algorithm
        } else {
            status.chancel_certificate_algorithm
        },
        explicit_statement: if status.explicit_statement.is_empty() {
            session.explicit_statement
        } else {
            status.explicit_statement
        },
    })
}

pub fn verify_live_package_chancel_certificate_for_apse_receipt(
    chancel_certificate_session_dir: &Path,
    confirmed_decision_packet_session_dir: &Path,
) -> Result<VerifiedLivePackageChancelCertificateApseReceiptStep> {
    let contract = load_live_package_chancel_certificate_contract_for_apse_receipt(
        chancel_certificate_session_dir,
    )?;
    let trusted_decision_packet_session_dir =
        load_confirmed_decision_packet_session_dir_for_chancel_verify(
            chancel_certificate_session_dir,
            confirmed_decision_packet_session_dir,
            &contract,
        )?;
    let raw_report = run_json_command(
        CHANCEL_CERTIFICATE_BIN,
        &chancel_certificate_verify_args(
            chancel_certificate_session_dir,
            &contract.lectern_certificate_session_dir,
            &trusted_decision_packet_session_dir,
        ),
    )?;
    let report: ChancelCertificateVerifyReportView = serde_json::from_value(raw_report.clone())
        .context("failed parsing chancel-certificate verify report")?;
    let reported_lectern_certificate_session_dir = PathBuf::from(required_string_field(
        report.lectern_certificate_session_dir,
        "lectern_certificate_session_dir",
    )?);
    let reported_pulpit_receipt_session_dir = PathBuf::from(required_string_field(
        report.pulpit_receipt_session_dir,
        "pulpit_receipt_session_dir",
    )?);
    let reported_pedestal_certificate_session_dir = PathBuf::from(required_string_field(
        report.pedestal_certificate_session_dir,
        "pedestal_certificate_session_dir",
    )?);
    let reported_plinth_receipt_session_dir = PathBuf::from(required_string_field(
        report.plinth_receipt_session_dir,
        "plinth_receipt_session_dir",
    )?);
    let reported_substructure_certificate_session_dir = PathBuf::from(required_string_field(
        report.substructure_certificate_session_dir,
        "substructure_certificate_session_dir",
    )?);
    let reported_basal_receipt_session_dir = PathBuf::from(required_string_field(
        report.basal_receipt_session_dir,
        "basal_receipt_session_dir",
    )?);
    let reported_foundation_receipt_session_dir = PathBuf::from(required_string_field(
        report.foundation_receipt_session_dir,
        "foundation_receipt_session_dir",
    )?);
    let reported_bedrock_certificate_session_dir =
        PathBuf::from(&report.bedrock_certificate_session_dir);
    if reported_lectern_certificate_session_dir != contract.lectern_certificate_session_dir {
        bail!(
            "chancel-certificate verify report lectern_certificate_session_dir {:?} does not match trusted {:?}",
            reported_lectern_certificate_session_dir.display(),
            contract.lectern_certificate_session_dir.display()
        );
    }
    if reported_pulpit_receipt_session_dir != contract.pulpit_receipt_session_dir {
        bail!(
            "chancel-certificate verify report pulpit_receipt_session_dir {:?} does not match trusted {:?}",
            reported_pulpit_receipt_session_dir.display(),
            contract.pulpit_receipt_session_dir.display()
        );
    }
    if reported_pedestal_certificate_session_dir != contract.pedestal_certificate_session_dir {
        bail!(
            "chancel-certificate verify report pedestal_certificate_session_dir {:?} does not match trusted {:?}",
            reported_pedestal_certificate_session_dir.display(),
            contract.pedestal_certificate_session_dir.display()
        );
    }
    if reported_plinth_receipt_session_dir != contract.plinth_receipt_session_dir {
        bail!(
            "chancel-certificate verify report plinth_receipt_session_dir {:?} does not match trusted {:?}",
            reported_plinth_receipt_session_dir.display(),
            contract.plinth_receipt_session_dir.display()
        );
    }
    if reported_substructure_certificate_session_dir
        != contract.substructure_certificate_session_dir
    {
        bail!(
            "chancel-certificate verify report substructure_certificate_session_dir {:?} does not match trusted {:?}",
            reported_substructure_certificate_session_dir.display(),
            contract.substructure_certificate_session_dir.display()
        );
    }
    if reported_bedrock_certificate_session_dir != contract.bedrock_certificate_session_dir {
        bail!(
            "chancel-certificate verify report bedrock_certificate_session_dir {:?} does not match trusted {:?}",
            reported_bedrock_certificate_session_dir.display(),
            contract.bedrock_certificate_session_dir.display()
        );
    }
    if reported_basal_receipt_session_dir != contract.basal_receipt_session_dir {
        bail!(
            "chancel-certificate verify report basal_receipt_session_dir {:?} does not match trusted {:?}",
            reported_basal_receipt_session_dir.display(),
            contract.basal_receipt_session_dir.display()
        );
    }
    if reported_foundation_receipt_session_dir != contract.foundation_receipt_session_dir {
        bail!(
            "chancel-certificate verify report foundation_receipt_session_dir {:?} does not match trusted {:?}",
            reported_foundation_receipt_session_dir.display(),
            contract.foundation_receipt_session_dir.display()
        );
    }
    let reported_decision_packet_session_dir = PathBuf::from(required_string_field(
        report.decision_packet_session_dir,
        "decision_packet_session_dir",
    )?);
    if reported_decision_packet_session_dir != trusted_decision_packet_session_dir {
        bail!(
            "chancel-certificate verify report decision_packet_session_dir {:?} does not match trusted {:?}",
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
            "chancel-certificate verify report registry_entry_session_dir {:?} does not match trusted {:?}",
            reported_registry_entry_session_dir.display(),
            contract.registry_entry_session_dir.display()
        );
    }
    Ok(VerifiedLivePackageChancelCertificateApseReceiptStep {
        report_json: raw_report,
        verdict: report.verdict.clone(),
        reason: report.reason.clone(),
        generated_at: report.generated_at,
        contract: LivePackageChancelCertificateContractView {
            chancel_certificate_session_dir: chancel_certificate_session_dir.to_path_buf(),
            lectern_certificate_session_dir: reported_lectern_certificate_session_dir,
            pulpit_receipt_session_dir: reported_pulpit_receipt_session_dir,
            dais_receipt_session_dir: PathBuf::from(required_string_field(
                report.dais_receipt_session_dir,
                "dais_receipt_session_dir",
            )?),
            pedestal_certificate_session_dir: reported_pedestal_certificate_session_dir,
            plinth_receipt_session_dir: reported_plinth_receipt_session_dir,
            substructure_certificate_session_dir: reported_substructure_certificate_session_dir,
            basal_receipt_session_dir: reported_basal_receipt_session_dir,
            bedrock_certificate_session_dir: reported_bedrock_certificate_session_dir,
            foundation_receipt_session_dir: reported_foundation_receipt_session_dir,
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
            verify_pulpit_receipt_command_summary: required_string_field(
                report.verify_pulpit_receipt_command_summary,
                "verify_pulpit_receipt_command_summary",
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
            capstone_certificate_summary: required_string_field(
                report.capstone_certificate_summary,
                "capstone_certificate_summary",
            )?,
            capstone_certificate_sha256: required_string_field(
                report.capstone_certificate_sha256,
                "capstone_certificate_sha256",
            )?,
            capstone_certificate_algorithm: required_string_field(
                report.capstone_certificate_algorithm,
                "capstone_certificate_algorithm",
            )?,
            keystone_receipt_summary: required_string_field(
                report.keystone_receipt_summary,
                "keystone_receipt_summary",
            )?,
            keystone_receipt_sha256: required_string_field(
                report.keystone_receipt_sha256,
                "keystone_receipt_sha256",
            )?,
            keystone_receipt_algorithm: required_string_field(
                report.keystone_receipt_algorithm,
                "keystone_receipt_algorithm",
            )?,
            cornerstone_certificate_summary: required_string_field(
                report.cornerstone_certificate_summary,
                "cornerstone_certificate_summary",
            )?,
            cornerstone_certificate_sha256: required_string_field(
                report.cornerstone_certificate_sha256,
                "cornerstone_certificate_sha256",
            )?,
            cornerstone_certificate_algorithm: required_string_field(
                report.cornerstone_certificate_algorithm,
                "cornerstone_certificate_algorithm",
            )?,
            foundation_receipt_summary: required_string_field(
                report.foundation_receipt_summary,
                "foundation_receipt_summary",
            )?,
            foundation_receipt_sha256: required_string_field(
                report.foundation_receipt_sha256,
                "foundation_receipt_sha256",
            )?,
            foundation_receipt_algorithm: required_string_field(
                report.foundation_receipt_algorithm,
                "foundation_receipt_algorithm",
            )?,
            bedrock_certificate_summary: required_string_field(
                report.bedrock_certificate_summary,
                "bedrock_certificate_summary",
            )?,
            bedrock_certificate_sha256: required_string_field(
                report.bedrock_certificate_sha256,
                "bedrock_certificate_sha256",
            )?,
            bedrock_certificate_algorithm: required_string_field(
                report.bedrock_certificate_algorithm,
                "bedrock_certificate_algorithm",
            )?,
            basal_receipt_summary: required_string_field(
                report.basal_receipt_summary,
                "basal_receipt_summary",
            )?,
            basal_receipt_sha256: required_string_field(
                report.basal_receipt_sha256,
                "basal_receipt_sha256",
            )?,
            basal_receipt_algorithm: required_string_field(
                report.basal_receipt_algorithm,
                "basal_receipt_algorithm",
            )?,
            substructure_certificate_summary: required_string_field(
                report.substructure_certificate_summary,
                "substructure_certificate_summary",
            )?,
            substructure_certificate_sha256: required_string_field(
                report.substructure_certificate_sha256,
                "substructure_certificate_sha256",
            )?,
            substructure_certificate_algorithm: required_string_field(
                report.substructure_certificate_algorithm,
                "substructure_certificate_algorithm",
            )?,
            plinth_receipt_summary: required_string_field(
                report.plinth_receipt_summary,
                "plinth_receipt_summary",
            )?,
            plinth_receipt_sha256: required_string_field(
                report.plinth_receipt_sha256,
                "plinth_receipt_sha256",
            )?,
            plinth_receipt_algorithm: required_string_field(
                report.plinth_receipt_algorithm,
                "plinth_receipt_algorithm",
            )?,
            pedestal_certificate_summary: required_string_field(
                report.pedestal_certificate_summary,
                "pedestal_certificate_summary",
            )?,
            pedestal_certificate_sha256: required_string_field(
                report.pedestal_certificate_sha256,
                "pedestal_certificate_sha256",
            )?,
            pedestal_certificate_algorithm: required_string_field(
                report.pedestal_certificate_algorithm,
                "pedestal_certificate_algorithm",
            )?,
            dais_receipt_summary: required_string_field(
                report.dais_receipt_summary,
                "dais_receipt_summary",
            )?,
            dais_receipt_sha256: required_string_field(
                report.dais_receipt_sha256,
                "dais_receipt_sha256",
            )?,
            dais_receipt_algorithm: required_string_field(
                report.dais_receipt_algorithm,
                "dais_receipt_algorithm",
            )?,
            rostrum_certificate_summary: required_string_field(
                report.rostrum_certificate_summary,
                "rostrum_certificate_summary",
            )?,
            rostrum_certificate_sha256: required_string_field(
                report.rostrum_certificate_sha256,
                "rostrum_certificate_sha256",
            )?,
            rostrum_certificate_algorithm: required_string_field(
                report.rostrum_certificate_algorithm,
                "rostrum_certificate_algorithm",
            )?,
            podium_receipt_summary: required_string_field(
                report.podium_receipt_summary,
                "podium_receipt_summary",
            )?,
            podium_receipt_sha256: required_string_field(
                report.podium_receipt_sha256,
                "podium_receipt_sha256",
            )?,
            podium_receipt_algorithm: required_string_field(
                report.podium_receipt_algorithm,
                "podium_receipt_algorithm",
            )?,
            lectern_certificate_summary: required_string_field(
                report.lectern_certificate_summary,
                "lectern_certificate_summary",
            )?,
            lectern_certificate_sha256: required_string_field(
                report.lectern_certificate_sha256,
                "lectern_certificate_sha256",
            )?,
            lectern_certificate_algorithm: required_string_field(
                report.lectern_certificate_algorithm,
                "lectern_certificate_algorithm",
            )?,
            pulpit_receipt_summary: required_string_field(
                report.pulpit_receipt_summary,
                "pulpit_receipt_summary",
            )?,
            pulpit_receipt_sha256: required_string_field(
                report.pulpit_receipt_sha256,
                "pulpit_receipt_sha256",
            )?,
            pulpit_receipt_algorithm: required_string_field(
                report.pulpit_receipt_algorithm,
                "pulpit_receipt_algorithm",
            )?,
            chancel_certificate_summary: required_string_field(
                report.chancel_certificate_summary,
                "chancel_certificate_summary",
            )?,
            chancel_certificate_sha256: required_string_field(
                report.chancel_certificate_sha256,
                "chancel_certificate_sha256",
            )?,
            chancel_certificate_algorithm: required_string_field(
                report.chancel_certificate_algorithm,
                "chancel_certificate_algorithm",
            )?,
            explicit_statement: report.explicit_statement,
        },
    })
}

pub fn chancel_certificate_verify_args(
    chancel_certificate_session_dir: &Path,
    pulpit_receipt_session_dir: &Path,
    decision_packet_session_dir: &Path,
) -> Vec<String> {
    vec![
        "--pulpit-receipt-session-dir".to_string(),
        pulpit_receipt_session_dir.display().to_string(),
        "--confirm-decision-packet-session-dir".to_string(),
        decision_packet_session_dir.display().to_string(),
        "--session-dir".to_string(),
        chancel_certificate_session_dir.display().to_string(),
        "--verify-live-package-chancel-certificate".to_string(),
        "--json".to_string(),
    ]
}

pub fn chancel_certificate_artifact_paths(
    session_dir: &Path,
) -> PackageChancelCertificateArtifactPaths {
    PackageChancelCertificateArtifactPaths {
        session_path: session_dir
            .join("tiny_live_activation_package_chancel_certificate.session.json"),
        status_path: session_dir
            .join("tiny_live_activation_package_chancel_certificate.status.json"),
        pulpit_receipt_report_path: session_dir
            .join("tiny_live_activation_package_chancel_certificate.pulpit_receipt.report.json"),
    }
}

fn load_confirmed_decision_packet_session_dir_for_chancel_verify(
    chancel_certificate_session_dir: &Path,
    confirmed_decision_packet_session_dir: &Path,
    contract: &LivePackageChancelCertificateContractView,
) -> Result<PathBuf> {
    let paths = chancel_certificate_artifact_paths(chancel_certificate_session_dir);
    let archived_report: StoredPulpitReceiptReportView =
        load_json(&paths.pulpit_receipt_report_path)?;
    let confirmed_decision_packet_session_dir = confirmed_decision_packet_session_dir.to_path_buf();
    if confirmed_decision_packet_session_dir != contract.decision_packet_session_dir {
        bail!(
            "confirmed decision-packet session dir {:?} does not match stored chancel-certificate contract {:?}",
            confirmed_decision_packet_session_dir.display(),
            contract.decision_packet_session_dir.display()
        );
    }
    let archived_decision_packet_session_dir =
        PathBuf::from(archived_report.decision_packet_session_dir);
    if confirmed_decision_packet_session_dir != archived_decision_packet_session_dir {
        bail!(
            "confirmed decision-packet session dir {:?} does not match archived nested lectern-certificate report {:?}",
            confirmed_decision_packet_session_dir.display(),
            archived_decision_packet_session_dir.display()
        );
    }
    Ok(confirmed_decision_packet_session_dir)
}

fn required_string_field(value: Option<String>, field: &str) -> Result<String> {
    value.ok_or_else(|| anyhow!("chancel-certificate verify report is missing {field}"))
}

fn required_u64_field(value: Option<u64>, field: &str) -> Result<u64> {
    value.ok_or_else(|| anyhow!("chancel-certificate verify report is missing {field}"))
}

fn required_usize_field(value: Option<usize>, field: &str) -> Result<usize> {
    value.ok_or_else(|| anyhow!("chancel-certificate verify report is missing {field}"))
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
    fn load_contract_reads_stored_pulpit_receipt_files() {
        let dir = temp_dir("pulpit_receipt_contract");
        fs::write(
            dir.join("tiny_live_activation_package_chancel_certificate.session.json"),
            serde_json::to_vec_pretty(&stored_pulpit_receipt_session_value(
                "/tmp/decision-packet-session",
            ))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_chancel_certificate.status.json"),
            serde_json::to_vec_pretty(&stored_pulpit_receipt_status_value(
                "refused_now_by_stage3",
                true,
            ))
            .unwrap(),
        )
        .unwrap();

        let contract =
            load_live_package_chancel_certificate_contract_for_apse_receipt(&dir).unwrap();
        assert_eq!(contract.chancel_certificate_session_dir, dir);
        assert_eq!(
            contract.lectern_certificate_session_dir,
            PathBuf::from("/tmp/lectern-certificate-session")
        );
        assert_eq!(
            contract.pulpit_receipt_session_dir,
            PathBuf::from("/tmp/rostrum-certificate-session")
        );
        assert_eq!(
            contract.dais_receipt_session_dir,
            PathBuf::from("/tmp/dais-receipt-session")
        );
        assert_eq!(
            contract.pedestal_certificate_session_dir,
            PathBuf::from("/tmp/pedestal-certificate-session")
        );
        assert_eq!(
            contract.plinth_receipt_session_dir,
            PathBuf::from("/tmp/plinth-receipt-session")
        );
        assert_eq!(
            contract.substructure_certificate_session_dir,
            PathBuf::from("/tmp/substructure-certificate-session")
        );
        assert_eq!(
            contract.basal_receipt_session_dir,
            PathBuf::from("/tmp/basal-receipt-session")
        );
        assert_eq!(
            contract.decision_packet_session_dir,
            PathBuf::from("/tmp/decision-packet-session")
        );
        assert_eq!(
            contract.bedrock_certificate_session_dir,
            PathBuf::from("/tmp/bedrock-certificate-session")
        );
        assert_eq!(
            contract.foundation_receipt_session_dir,
            PathBuf::from("/tmp/foundation-receipt-session")
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
        assert_eq!(contract.summit_certificate_sha256, "summit-sha256");
        assert_eq!(contract.culmination_receipt_sha256, "ab12cd34");
        assert_eq!(contract.pinnacle_receipt_sha256, "crown-sha256");
        assert_eq!(contract.capstone_certificate_sha256, "sovereign-sha256");
        assert_eq!(contract.keystone_receipt_sha256, "imperial-sha256");
        assert_eq!(contract.cornerstone_certificate_sha256, "regalia-sha256");
        assert_eq!(contract.foundation_receipt_sha256, "diadem-sha256");
        assert_eq!(contract.bedrock_certificate_sha256, "coronet-sha256");
        assert_eq!(contract.basal_receipt_sha256, "circlet-sha256");
        assert_eq!(contract.substructure_certificate_sha256, "signet-sha256");
        assert_eq!(contract.plinth_receipt_sha256, "cachet-sha256");
        assert_eq!(contract.pedestal_certificate_sha256, "imprint-sha256");
        assert_eq!(contract.dais_receipt_sha256, "hallmark-sha256");
        assert_eq!(contract.rostrum_certificate_sha256, "escutcheon-sha256");
        assert_eq!(contract.podium_receipt_sha256, "blazon-sha256");
        assert_eq!(contract.lectern_certificate_sha256, "armorial-sha256");
        assert_eq!(contract.pulpit_receipt_sha256, "crest-sha256");
        assert_eq!(contract.chancel_certificate_sha256, "herald-sha256");
    }

    #[test]
    fn chancel_certificate_verify_args_are_exact_and_bounded() {
        let args = chancel_certificate_verify_args(
            Path::new("/tmp/chancel-certificate-session"),
            Path::new("/tmp/pulpit-receipt-session"),
            Path::new("/tmp/decision-packet-session"),
        );
        assert_eq!(
            args,
            vec![
                "--pulpit-receipt-session-dir",
                "/tmp/pulpit-receipt-session",
                "--confirm-decision-packet-session-dir",
                "/tmp/decision-packet-session",
                "--session-dir",
                "/tmp/chancel-certificate-session",
                "--verify-live-package-chancel-certificate",
                "--json"
            ]
        );
    }

    #[test]
    fn confirmed_decision_packet_session_dir_must_match_stored_contract_and_archive() {
        let dir = temp_dir("pulpit_receipt_confirmed_decision_packet");
        let trusted_decision_packet_session_dir = dir.join("trusted-decision-packet");
        let foreign_decision_packet_session_dir = dir.join("foreign-decision-packet");
        fs::create_dir_all(&trusted_decision_packet_session_dir).unwrap();
        fs::create_dir_all(&foreign_decision_packet_session_dir).unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_chancel_certificate.session.json"),
            serde_json::to_vec_pretty(&stored_pulpit_receipt_session_value(
                &foreign_decision_packet_session_dir.display().to_string(),
            ))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_chancel_certificate.status.json"),
            serde_json::to_vec_pretty(&stored_pulpit_receipt_status_value(
                "refused_now_by_invalid_or_drifted_contract",
                false,
            ))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_chancel_certificate.pulpit_receipt.report.json"),
            serde_json::to_vec_pretty(&json!({
                "decision_packet_session_dir": foreign_decision_packet_session_dir.display().to_string()
            }))
            .unwrap(),
        )
        .unwrap();

        let contract =
            load_live_package_chancel_certificate_contract_for_apse_receipt(&dir).unwrap();
        let error = load_confirmed_decision_packet_session_dir_for_chancel_verify(
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

    fn stored_pulpit_receipt_session_value(decision_packet_session_dir: &str) -> Value {
        let mut object = Map::new();
        object.insert(
            "lectern_certificate_session_dir".to_string(),
            json!("/tmp/lectern-certificate-session"),
        );
        object.insert(
            "pulpit_receipt_session_dir".to_string(),
            json!("/tmp/rostrum-certificate-session"),
        );
        object.insert(
            "dais_receipt_session_dir".to_string(),
            json!("/tmp/dais-receipt-session"),
        );
        object.insert(
            "pedestal_certificate_session_dir".to_string(),
            json!("/tmp/pedestal-certificate-session"),
        );
        object.insert(
            "plinth_receipt_session_dir".to_string(),
            json!("/tmp/plinth-receipt-session"),
        );
        object.insert(
            "substructure_certificate_session_dir".to_string(),
            json!("/tmp/substructure-certificate-session"),
        );
        object.insert(
            "basal_receipt_session_dir".to_string(),
            json!("/tmp/basal-receipt-session"),
        );
        object.insert(
            "bedrock_certificate_session_dir".to_string(),
            json!("/tmp/bedrock-certificate-session"),
        );
        object.insert(
            "foundation_receipt_session_dir".to_string(),
            json!("/tmp/foundation-receipt-session"),
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
            "verify_dais_receipt_command_summary".to_string(),
            json!("verify dais receipt"),
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
            json!("basal receipt statement"),
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
        object.insert(
            "summit_certificate_sha256".to_string(),
            json!("summit-sha256"),
        );
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
        object.insert("pinnacle_receipt_sha256".to_string(), json!("crown-sha256"));
        object.insert("pinnacle_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "capstone_certificate_summary".to_string(),
            json!("capstone certificate"),
        );
        object.insert(
            "capstone_certificate_sha256".to_string(),
            json!("sovereign-sha256"),
        );
        object.insert(
            "capstone_certificate_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "keystone_receipt_summary".to_string(),
            json!("keystone receipt"),
        );
        object.insert(
            "keystone_receipt_sha256".to_string(),
            json!("imperial-sha256"),
        );
        object.insert("keystone_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "cornerstone_certificate_summary".to_string(),
            json!("cornerstone certificate"),
        );
        object.insert(
            "cornerstone_certificate_sha256".to_string(),
            json!("regalia-sha256"),
        );
        object.insert(
            "cornerstone_certificate_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "foundation_receipt_summary".to_string(),
            json!("foundation receipt"),
        );
        object.insert(
            "foundation_receipt_sha256".to_string(),
            json!("diadem-sha256"),
        );
        object.insert("foundation_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "bedrock_certificate_summary".to_string(),
            json!("bedrock certificate"),
        );
        object.insert(
            "bedrock_certificate_sha256".to_string(),
            json!("coronet-sha256"),
        );
        object.insert("bedrock_certificate_algorithm".to_string(), json!("sha256"));
        object.insert("basal_receipt_summary".to_string(), json!("basal receipt"));
        object.insert("basal_receipt_sha256".to_string(), json!("circlet-sha256"));
        object.insert("basal_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "substructure_certificate_summary".to_string(),
            json!("substructure certificate"),
        );
        object.insert(
            "substructure_certificate_sha256".to_string(),
            json!("signet-sha256"),
        );
        object.insert(
            "substructure_certificate_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "plinth_receipt_summary".to_string(),
            json!("plinth receipt"),
        );
        object.insert("plinth_receipt_sha256".to_string(), json!("cachet-sha256"));
        object.insert("plinth_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "pedestal_certificate_summary".to_string(),
            json!("pedestal certificate"),
        );
        object.insert(
            "pedestal_certificate_sha256".to_string(),
            json!("imprint-sha256"),
        );
        object.insert(
            "pedestal_certificate_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert("dais_receipt_summary".to_string(), json!("dais receipt"));
        object.insert("dais_receipt_sha256".to_string(), json!("hallmark-sha256"));
        object.insert("dais_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "rostrum_certificate_summary".to_string(),
            json!("rostrum certificate"),
        );
        object.insert(
            "rostrum_certificate_sha256".to_string(),
            json!("escutcheon-sha256"),
        );
        object.insert("rostrum_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "podium_receipt_summary".to_string(),
            json!("podium receipt"),
        );
        object.insert("podium_receipt_sha256".to_string(), json!("blazon-sha256"));
        object.insert("podium_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "lectern_certificate_summary".to_string(),
            json!("lectern certificate"),
        );
        object.insert(
            "lectern_certificate_sha256".to_string(),
            json!("armorial-sha256"),
        );
        object.insert("lectern_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "pulpit_receipt_summary".to_string(),
            json!("pulpit receipt"),
        );
        object.insert("pulpit_receipt_sha256".to_string(), json!("crest-sha256"));
        object.insert("pulpit_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "chancel_certificate_summary".to_string(),
            json!("chancel certificate"),
        );
        object.insert(
            "chancel_certificate_sha256".to_string(),
            json!("herald-sha256"),
        );
        object.insert("chancel_certificate_algorithm".to_string(), json!("sha256"));
        Value::Object(object)
    }

    fn stored_pulpit_receipt_status_value(result: &str, include_nested_results: bool) -> Value {
        let mut object = Map::new();
        object.insert(
            "lectern_certificate_session_dir".to_string(),
            json!("/tmp/lectern-certificate-session"),
        );
        object.insert(
            "pulpit_receipt_session_dir".to_string(),
            json!("/tmp/rostrum-certificate-session"),
        );
        object.insert(
            "dais_receipt_session_dir".to_string(),
            json!("/tmp/dais-receipt-session"),
        );
        object.insert(
            "pedestal_certificate_session_dir".to_string(),
            json!("/tmp/pedestal-certificate-session"),
        );
        object.insert(
            "plinth_receipt_session_dir".to_string(),
            json!("/tmp/plinth-receipt-session"),
        );
        object.insert(
            "substructure_certificate_session_dir".to_string(),
            json!("/tmp/substructure-certificate-session"),
        );
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
            json!("basal receipt status statement"),
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
        object.insert(
            "summit_certificate_sha256".to_string(),
            json!("summit-sha256"),
        );
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
        object.insert("pinnacle_receipt_sha256".to_string(), json!("crown-sha256"));
        object.insert("pinnacle_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "capstone_certificate_summary".to_string(),
            json!("capstone certificate"),
        );
        object.insert(
            "capstone_certificate_sha256".to_string(),
            json!("sovereign-sha256"),
        );
        object.insert(
            "capstone_certificate_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "keystone_receipt_summary".to_string(),
            json!("keystone receipt"),
        );
        object.insert(
            "keystone_receipt_sha256".to_string(),
            json!("imperial-sha256"),
        );
        object.insert("keystone_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "cornerstone_certificate_summary".to_string(),
            json!("cornerstone certificate"),
        );
        object.insert(
            "cornerstone_certificate_sha256".to_string(),
            json!("regalia-sha256"),
        );
        object.insert(
            "cornerstone_certificate_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "foundation_receipt_summary".to_string(),
            json!("foundation receipt"),
        );
        object.insert(
            "foundation_receipt_sha256".to_string(),
            json!("diadem-sha256"),
        );
        object.insert("foundation_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "bedrock_certificate_summary".to_string(),
            json!("bedrock certificate"),
        );
        object.insert(
            "bedrock_certificate_sha256".to_string(),
            json!("coronet-sha256"),
        );
        object.insert("bedrock_certificate_algorithm".to_string(), json!("sha256"));
        object.insert("basal_receipt_summary".to_string(), json!("basal receipt"));
        object.insert("basal_receipt_sha256".to_string(), json!("circlet-sha256"));
        object.insert("basal_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "substructure_certificate_summary".to_string(),
            json!("substructure certificate"),
        );
        object.insert(
            "substructure_certificate_sha256".to_string(),
            json!("signet-sha256"),
        );
        object.insert(
            "substructure_certificate_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert(
            "plinth_receipt_summary".to_string(),
            json!("plinth receipt"),
        );
        object.insert("plinth_receipt_sha256".to_string(), json!("cachet-sha256"));
        object.insert("plinth_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "pedestal_certificate_summary".to_string(),
            json!("pedestal certificate"),
        );
        object.insert(
            "pedestal_certificate_sha256".to_string(),
            json!("imprint-sha256"),
        );
        object.insert(
            "pedestal_certificate_algorithm".to_string(),
            json!("sha256"),
        );
        object.insert("dais_receipt_summary".to_string(), json!("dais receipt"));
        object.insert("dais_receipt_sha256".to_string(), json!("hallmark-sha256"));
        object.insert("dais_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "rostrum_certificate_summary".to_string(),
            json!("rostrum certificate"),
        );
        object.insert(
            "rostrum_certificate_sha256".to_string(),
            json!("escutcheon-sha256"),
        );
        object.insert("rostrum_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "podium_receipt_summary".to_string(),
            json!("podium receipt"),
        );
        object.insert("podium_receipt_sha256".to_string(), json!("blazon-sha256"));
        object.insert("podium_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "lectern_certificate_summary".to_string(),
            json!("lectern certificate"),
        );
        object.insert(
            "lectern_certificate_sha256".to_string(),
            json!("armorial-sha256"),
        );
        object.insert("lectern_certificate_algorithm".to_string(), json!("sha256"));
        object.insert(
            "pulpit_receipt_summary".to_string(),
            json!("pulpit receipt"),
        );
        object.insert("pulpit_receipt_sha256".to_string(), json!("crest-sha256"));
        object.insert("pulpit_receipt_algorithm".to_string(), json!("sha256"));
        object.insert(
            "chancel_certificate_summary".to_string(),
            json!("chancel certificate"),
        );
        object.insert(
            "chancel_certificate_sha256".to_string(),
            json!("herald-sha256"),
        );
        object.insert("chancel_certificate_algorithm".to_string(), json!("sha256"));
        Value::Object(object)
    }
}
