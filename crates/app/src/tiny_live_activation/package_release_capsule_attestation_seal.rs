use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const RELEASE_CAPSULE_BIN: &str = "copybot_tiny_live_activation_package_release_capsule";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReleaseCapsuleDigestEntry {
    pub label: String,
    pub path: String,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReleaseCapsuleDigestManifest {
    pub digest_algorithm: String,
    pub entries: Vec<ReleaseCapsuleDigestEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LivePackageReleaseCapsuleContractView {
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
    pub verify_activation_ticket_command_summary: String,
    pub reviewed_frozen_live_cutover_controller_command_summary: String,
    pub release_capsule_summary: String,
    pub audit_manifest_summary: String,
    pub digest_algorithm: String,
    pub explicit_statement: String,
}

#[derive(Debug, Clone)]
pub struct VerifiedLivePackageReleaseCapsuleAttestationSealStep {
    pub report_json: serde_json::Value,
    pub verdict: String,
    pub reason: String,
    pub generated_at: DateTime<Utc>,
    pub contract: LivePackageReleaseCapsuleContractView,
    pub digest_manifest: ReleaseCapsuleDigestManifest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageReleaseCapsuleArtifactPaths {
    pub session_path: PathBuf,
    pub status_path: PathBuf,
    pub activation_ticket_report_path: PathBuf,
    pub digest_manifest_path: PathBuf,
}

#[derive(Debug, Deserialize)]
struct StoredReleaseCapsuleSession {
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
    verify_activation_ticket_command_summary: String,
    reviewed_frozen_live_cutover_controller_command_summary: String,
    release_capsule_summary: String,
    audit_manifest_summary: String,
    digest_algorithm: String,
    explicit_statement: String,
}

#[derive(Debug, Deserialize)]
struct StoredReleaseCapsuleStatus {
    result: String,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    release_capsule_summary: String,
    audit_manifest_summary: String,
    digest_algorithm: String,
    explicit_statement: String,
}

#[derive(Debug, Deserialize)]
struct StoredActivationTicketReportView {
    decision_packet_session_dir: String,
}

#[derive(Debug, Deserialize)]
struct ReleaseCapsuleVerifyReportView {
    generated_at: DateTime<Utc>,
    verdict: String,
    reason: String,
    activation_ticket_session_dir: String,
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
    verify_activation_ticket_command_summary: Option<String>,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    release_capsule_summary: Option<String>,
    audit_manifest_summary: Option<String>,
    digest_algorithm: Option<String>,
    digest_manifest: Option<ReleaseCapsuleDigestManifest>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    explicit_statement: String,
}

pub fn load_live_package_release_capsule_contract_for_attestation_seal(
    release_capsule_session_dir: &Path,
) -> Result<LivePackageReleaseCapsuleContractView> {
    let paths = release_capsule_artifact_paths(release_capsule_session_dir);
    let session: StoredReleaseCapsuleSession = load_json(&paths.session_path)?;
    let status: StoredReleaseCapsuleStatus = load_json(&paths.status_path)?;
    Ok(LivePackageReleaseCapsuleContractView {
        release_capsule_session_dir: release_capsule_session_dir.to_path_buf(),
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
        verify_activation_ticket_command_summary: session.verify_activation_ticket_command_summary,
        reviewed_frozen_live_cutover_controller_command_summary: session
            .reviewed_frozen_live_cutover_controller_command_summary,
        release_capsule_summary: if status.release_capsule_summary.is_empty() {
            session.release_capsule_summary
        } else {
            status.release_capsule_summary
        },
        audit_manifest_summary: if status.audit_manifest_summary.is_empty() {
            session.audit_manifest_summary
        } else {
            status.audit_manifest_summary
        },
        digest_algorithm: if status.digest_algorithm.is_empty() {
            session.digest_algorithm
        } else {
            status.digest_algorithm
        },
        explicit_statement: if status.explicit_statement.is_empty() {
            session.explicit_statement
        } else {
            status.explicit_statement
        },
    })
}

pub fn verify_live_package_release_capsule_for_attestation_seal(
    release_capsule_session_dir: &Path,
    confirmed_decision_packet_session_dir: &Path,
) -> Result<VerifiedLivePackageReleaseCapsuleAttestationSealStep> {
    let contract = load_live_package_release_capsule_contract_for_attestation_seal(
        release_capsule_session_dir,
    )?;
    let trusted_decision_packet_session_dir =
        load_confirmed_decision_packet_session_dir_for_verify(
            release_capsule_session_dir,
            confirmed_decision_packet_session_dir,
            &contract,
        )?;
    let raw_report = run_json_command(
        RELEASE_CAPSULE_BIN,
        &release_capsule_verify_args(
            &contract.activation_ticket_session_dir,
            &trusted_decision_packet_session_dir,
            release_capsule_session_dir,
        ),
    )?;
    let report: ReleaseCapsuleVerifyReportView = serde_json::from_value(raw_report.clone())
        .context("failed parsing release-capsule verify report")?;
    if PathBuf::from(&report.activation_ticket_session_dir)
        != contract.activation_ticket_session_dir
    {
        bail!(
            "release-capsule verify report activation_ticket_session_dir {:?} does not match trusted {:?}",
            report.activation_ticket_session_dir,
            contract.activation_ticket_session_dir.display()
        );
    }
    let reported_decision_packet_session_dir = PathBuf::from(required_string_field(
        report.decision_packet_session_dir,
        "decision_packet_session_dir",
    )?);
    if reported_decision_packet_session_dir != trusted_decision_packet_session_dir {
        bail!(
            "release-capsule verify report decision_packet_session_dir {:?} does not match trusted {:?}",
            reported_decision_packet_session_dir.display(),
            trusted_decision_packet_session_dir.display()
        );
    }
    let digest_manifest = report
        .digest_manifest
        .ok_or_else(|| anyhow!("release-capsule verify report is missing digest_manifest"))?;
    Ok(VerifiedLivePackageReleaseCapsuleAttestationSealStep {
        report_json: raw_report,
        verdict: report.verdict.clone(),
        reason: report.reason.clone(),
        generated_at: report.generated_at,
        contract: LivePackageReleaseCapsuleContractView {
            release_capsule_session_dir: release_capsule_session_dir.to_path_buf(),
            activation_ticket_session_dir: contract.activation_ticket_session_dir,
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
            verify_activation_ticket_command_summary: required_string_field(
                report.verify_activation_ticket_command_summary,
                "verify_activation_ticket_command_summary",
            )?,
            reviewed_frozen_live_cutover_controller_command_summary: required_string_field(
                report.reviewed_frozen_live_cutover_controller_command_summary,
                "reviewed_frozen_live_cutover_controller_command_summary",
            )?,
            release_capsule_summary: required_string_field(
                report.release_capsule_summary,
                "release_capsule_summary",
            )?,
            audit_manifest_summary: required_string_field(
                report.audit_manifest_summary,
                "audit_manifest_summary",
            )?,
            digest_algorithm: required_string_field(report.digest_algorithm, "digest_algorithm")?,
            explicit_statement: report.explicit_statement,
        },
        digest_manifest,
    })
}

pub fn release_capsule_verify_args(
    activation_ticket_session_dir: &Path,
    decision_packet_session_dir: &Path,
    release_capsule_session_dir: &Path,
) -> Vec<String> {
    vec![
        "--activation-ticket-session-dir".to_string(),
        activation_ticket_session_dir.display().to_string(),
        "--confirm-decision-packet-session-dir".to_string(),
        decision_packet_session_dir.display().to_string(),
        "--session-dir".to_string(),
        release_capsule_session_dir.display().to_string(),
        "--verify-live-package-release-capsule".to_string(),
        "--json".to_string(),
    ]
}

pub fn release_capsule_artifact_paths(session_dir: &Path) -> PackageReleaseCapsuleArtifactPaths {
    PackageReleaseCapsuleArtifactPaths {
        session_path: session_dir.join("tiny_live_activation_package_release_capsule.session.json"),
        status_path: session_dir.join("tiny_live_activation_package_release_capsule.status.json"),
        activation_ticket_report_path: session_dir
            .join("tiny_live_activation_package_release_capsule.activation_ticket.report.json"),
        digest_manifest_path: session_dir
            .join("tiny_live_activation_package_release_capsule.digest_manifest.json"),
    }
}

fn load_confirmed_decision_packet_session_dir_for_verify(
    release_capsule_session_dir: &Path,
    confirmed_decision_packet_session_dir: &Path,
    contract: &LivePackageReleaseCapsuleContractView,
) -> Result<PathBuf> {
    let paths = release_capsule_artifact_paths(release_capsule_session_dir);
    let archived_report: StoredActivationTicketReportView =
        load_json(&paths.activation_ticket_report_path)?;
    let confirmed_decision_packet_session_dir = confirmed_decision_packet_session_dir.to_path_buf();
    if confirmed_decision_packet_session_dir != contract.decision_packet_session_dir {
        bail!(
            "confirmed decision-packet session dir {:?} does not match stored release-capsule contract {:?}",
            confirmed_decision_packet_session_dir.display(),
            contract.decision_packet_session_dir.display()
        );
    }
    let archived_decision_packet_session_dir =
        PathBuf::from(archived_report.decision_packet_session_dir);
    if confirmed_decision_packet_session_dir != archived_decision_packet_session_dir {
        bail!(
            "confirmed decision-packet session dir {:?} does not match archived nested activation-ticket report {:?}",
            confirmed_decision_packet_session_dir.display(),
            archived_decision_packet_session_dir.display()
        );
    }
    Ok(confirmed_decision_packet_session_dir)
}

fn required_string_field(value: Option<String>, field: &str) -> Result<String> {
    value.ok_or_else(|| anyhow!("release-capsule verify report is missing {field}"))
}

fn required_u64_field(value: Option<u64>, field: &str) -> Result<u64> {
    value.ok_or_else(|| anyhow!("release-capsule verify report is missing {field}"))
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
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn load_contract_reads_stored_release_capsule_files() {
        let dir = temp_dir("release_capsule_contract");
        fs::write(
            dir.join("tiny_live_activation_package_release_capsule.session.json"),
            serde_json::to_vec_pretty(&json!({
                "activation_ticket_session_dir": "/tmp/activation-ticket-session",
                "review_receipt_session_dir": "/tmp/review-receipt-session",
                "handoff_bundle_session_dir": "/tmp/handoff-bundle-session",
                "decision_packet_session_dir": "/tmp/decision-packet-session",
                "execute_frozen_session_dir": "/tmp/execute-frozen-session",
                "turn_green_session_dir": "/tmp/turn-green-session",
                "launch_packet_session_dir": "/tmp/launch-packet-session",
                "package_dir": "/tmp/frozen-package",
                "install_root": "/",
                "target_service_name": "solana-copy-bot.service",
                "backend_command": "/tmp/backend",
                "wrapper_timeout_ms": 1200,
                "service_status_max_staleness_ms": 4500,
                "verify_activation_ticket_command_summary": "verify activation ticket",
                "reviewed_frozen_live_cutover_controller_command_summary": "run frozen cutover",
                "release_capsule_summary": "release capsule",
                "audit_manifest_summary": "audit manifest",
                "digest_algorithm": "sha256",
                "explicit_statement": "release capsule statement"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_release_capsule.status.json"),
            serde_json::to_vec_pretty(&json!({
                "result": "refused_now_by_stage3",
                "handoff_bundle_result": "refused_now_by_stage3",
                "decision_packet_result": "refused_now_by_stage3",
                "execute_frozen_result": "refused_now_by_stage3",
                "release_capsule_summary": "release capsule",
                "audit_manifest_summary": "audit manifest",
                "digest_algorithm": "sha256",
                "explicit_statement": "release capsule status statement"
            }))
            .unwrap(),
        )
        .unwrap();

        let contract =
            load_live_package_release_capsule_contract_for_attestation_seal(&dir).unwrap();
        assert_eq!(
            contract.decision_packet_session_dir,
            PathBuf::from("/tmp/decision-packet-session")
        );
        assert_eq!(contract.result.as_deref(), Some("refused_now_by_stage3"));
        assert_eq!(contract.audit_manifest_summary, "audit manifest");
    }

    #[test]
    fn release_capsule_verify_args_are_exact_and_bounded() {
        let args = release_capsule_verify_args(
            Path::new("/tmp/activation-ticket-session"),
            Path::new("/tmp/decision-packet-session"),
            Path::new("/tmp/release-capsule-session"),
        );
        assert_eq!(
            args,
            vec![
                "--activation-ticket-session-dir",
                "/tmp/activation-ticket-session",
                "--confirm-decision-packet-session-dir",
                "/tmp/decision-packet-session",
                "--session-dir",
                "/tmp/release-capsule-session",
                "--verify-live-package-release-capsule",
                "--json"
            ]
        );
    }

    #[test]
    fn confirmed_decision_packet_session_dir_must_match_stored_contract_and_archive() {
        let dir = temp_dir("release_capsule_confirmed_decision_packet");
        let trusted_decision_packet_session_dir = dir.join("trusted-decision-packet");
        let foreign_decision_packet_session_dir = dir.join("foreign-decision-packet");
        fs::create_dir_all(&trusted_decision_packet_session_dir).unwrap();
        fs::create_dir_all(&foreign_decision_packet_session_dir).unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_release_capsule.session.json"),
            serde_json::to_vec_pretty(&json!({
                "activation_ticket_session_dir": "/tmp/activation-ticket-session",
                "review_receipt_session_dir": "/tmp/review-receipt-session",
                "handoff_bundle_session_dir": "/tmp/handoff-bundle-session",
                "decision_packet_session_dir": foreign_decision_packet_session_dir.display().to_string(),
                "execute_frozen_session_dir": "/tmp/execute-frozen-session",
                "turn_green_session_dir": "/tmp/turn-green-session",
                "launch_packet_session_dir": "/tmp/launch-packet-session",
                "package_dir": "/tmp/frozen-package",
                "install_root": "/",
                "target_service_name": "solana-copy-bot.service",
                "backend_command": "/tmp/backend",
                "wrapper_timeout_ms": 1200,
                "service_status_max_staleness_ms": 4500,
                "verify_activation_ticket_command_summary": "verify activation ticket",
                "reviewed_frozen_live_cutover_controller_command_summary": "run frozen cutover",
                "release_capsule_summary": "release capsule",
                "audit_manifest_summary": "audit manifest",
                "digest_algorithm": "sha256",
                "explicit_statement": "release capsule statement"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_release_capsule.status.json"),
            serde_json::to_vec_pretty(&json!({
                "result": "refused_now_by_invalid_or_drifted_contract",
                "release_capsule_summary": "release capsule",
                "audit_manifest_summary": "audit manifest",
                "digest_algorithm": "sha256",
                "explicit_statement": "release capsule status statement"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_release_capsule.activation_ticket.report.json"),
            serde_json::to_vec_pretty(&json!({
                "decision_packet_session_dir": foreign_decision_packet_session_dir.display().to_string()
            }))
            .unwrap(),
        )
        .unwrap();

        let contract =
            load_live_package_release_capsule_contract_for_attestation_seal(&dir).unwrap();
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
}
