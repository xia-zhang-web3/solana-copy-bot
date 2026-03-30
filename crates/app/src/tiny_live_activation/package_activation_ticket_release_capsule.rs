use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const ACTIVATION_TICKET_BIN: &str = "copybot_tiny_live_activation_package_activation_ticket";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LivePackageActivationTicketContractView {
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
    pub verify_review_receipt_command_summary: String,
    pub reviewed_frozen_live_cutover_controller_command_summary: String,
    pub activation_ticket_summary: String,
    pub execution_warrant_summary: String,
    pub explicit_statement: String,
}

#[derive(Debug, Clone)]
pub struct VerifiedLivePackageActivationTicketReleaseCapsuleStep {
    pub report_json: serde_json::Value,
    pub verdict: String,
    pub reason: String,
    pub generated_at: DateTime<Utc>,
    pub contract: LivePackageActivationTicketContractView,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageActivationTicketArtifactPaths {
    pub session_path: PathBuf,
    pub status_path: PathBuf,
    pub review_receipt_report_path: PathBuf,
}

#[derive(Debug, Deserialize)]
struct StoredActivationTicketSession {
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
    verify_review_receipt_command_summary: String,
    reviewed_frozen_live_cutover_controller_command_summary: String,
    activation_ticket_summary: String,
    execution_warrant_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Deserialize)]
struct StoredActivationTicketStatus {
    result: String,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    activation_ticket_summary: String,
    execution_warrant_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Deserialize)]
struct StoredReviewReceiptReportView {
    decision_packet_session_dir: String,
}

#[derive(Debug, Deserialize)]
struct ActivationTicketVerifyReportView {
    generated_at: DateTime<Utc>,
    verdict: String,
    reason: String,
    review_receipt_session_dir: String,
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
    verify_review_receipt_command_summary: Option<String>,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    activation_ticket_summary: Option<String>,
    execution_warrant_summary: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    explicit_statement: String,
}

pub fn load_live_package_activation_ticket_contract_for_release_capsule(
    activation_ticket_session_dir: &Path,
) -> Result<LivePackageActivationTicketContractView> {
    let paths = activation_ticket_artifact_paths(activation_ticket_session_dir);
    let session: StoredActivationTicketSession = load_json(&paths.session_path)?;
    let status: StoredActivationTicketStatus = load_json(&paths.status_path)?;
    Ok(LivePackageActivationTicketContractView {
        activation_ticket_session_dir: activation_ticket_session_dir.to_path_buf(),
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
        verify_review_receipt_command_summary: session.verify_review_receipt_command_summary,
        reviewed_frozen_live_cutover_controller_command_summary: session
            .reviewed_frozen_live_cutover_controller_command_summary,
        activation_ticket_summary: if status.activation_ticket_summary.is_empty() {
            session.activation_ticket_summary
        } else {
            status.activation_ticket_summary
        },
        execution_warrant_summary: if status.execution_warrant_summary.is_empty() {
            session.execution_warrant_summary
        } else {
            status.execution_warrant_summary
        },
        explicit_statement: if status.explicit_statement.is_empty() {
            session.explicit_statement
        } else {
            status.explicit_statement
        },
    })
}

pub fn verify_live_package_activation_ticket_for_release_capsule(
    activation_ticket_session_dir: &Path,
    confirmed_decision_packet_session_dir: &Path,
) -> Result<VerifiedLivePackageActivationTicketReleaseCapsuleStep> {
    let contract = load_live_package_activation_ticket_contract_for_release_capsule(
        activation_ticket_session_dir,
    )?;
    let trusted_decision_packet_session_dir =
        load_confirmed_decision_packet_session_dir_for_verify(
            activation_ticket_session_dir,
            confirmed_decision_packet_session_dir,
            &contract,
        )?;
    let raw_report = run_json_command(
        ACTIVATION_TICKET_BIN,
        &activation_ticket_verify_args(
            &contract.review_receipt_session_dir,
            &trusted_decision_packet_session_dir,
            activation_ticket_session_dir,
        ),
    )?;
    let report: ActivationTicketVerifyReportView = serde_json::from_value(raw_report.clone())
        .context("failed parsing activation-ticket verify report")?;
    if PathBuf::from(&report.review_receipt_session_dir) != contract.review_receipt_session_dir {
        bail!(
            "activation-ticket verify report review_receipt_session_dir {:?} does not match trusted {:?}",
            report.review_receipt_session_dir,
            contract.review_receipt_session_dir.display()
        );
    }
    let reported_decision_packet_session_dir = PathBuf::from(required_string_field(
        report.decision_packet_session_dir,
        "decision_packet_session_dir",
    )?);
    if reported_decision_packet_session_dir != trusted_decision_packet_session_dir {
        bail!(
            "activation-ticket verify report decision_packet_session_dir {:?} does not match trusted {:?}",
            reported_decision_packet_session_dir.display(),
            trusted_decision_packet_session_dir.display()
        );
    }
    Ok(VerifiedLivePackageActivationTicketReleaseCapsuleStep {
        report_json: raw_report,
        verdict: report.verdict.clone(),
        reason: report.reason.clone(),
        generated_at: report.generated_at,
        contract: LivePackageActivationTicketContractView {
            activation_ticket_session_dir: activation_ticket_session_dir.to_path_buf(),
            review_receipt_session_dir: contract.review_receipt_session_dir,
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
            verify_review_receipt_command_summary: required_string_field(
                report.verify_review_receipt_command_summary,
                "verify_review_receipt_command_summary",
            )?,
            reviewed_frozen_live_cutover_controller_command_summary: required_string_field(
                report.reviewed_frozen_live_cutover_controller_command_summary,
                "reviewed_frozen_live_cutover_controller_command_summary",
            )?,
            activation_ticket_summary: required_string_field(
                report.activation_ticket_summary,
                "activation_ticket_summary",
            )?,
            execution_warrant_summary: required_string_field(
                report.execution_warrant_summary,
                "execution_warrant_summary",
            )?,
            explicit_statement: report.explicit_statement,
        },
    })
}

pub fn activation_ticket_verify_args(
    review_receipt_session_dir: &Path,
    decision_packet_session_dir: &Path,
    activation_ticket_session_dir: &Path,
) -> Vec<String> {
    vec![
        "--review-receipt-session-dir".to_string(),
        review_receipt_session_dir.display().to_string(),
        "--confirm-decision-packet-session-dir".to_string(),
        decision_packet_session_dir.display().to_string(),
        "--session-dir".to_string(),
        activation_ticket_session_dir.display().to_string(),
        "--verify-live-package-activation-ticket".to_string(),
        "--json".to_string(),
    ]
}

pub fn activation_ticket_artifact_paths(
    session_dir: &Path,
) -> PackageActivationTicketArtifactPaths {
    PackageActivationTicketArtifactPaths {
        session_path: session_dir
            .join("tiny_live_activation_package_activation_ticket.session.json"),
        status_path: session_dir.join("tiny_live_activation_package_activation_ticket.status.json"),
        review_receipt_report_path: session_dir
            .join("tiny_live_activation_package_activation_ticket.review_receipt.report.json"),
    }
}

fn load_confirmed_decision_packet_session_dir_for_verify(
    activation_ticket_session_dir: &Path,
    confirmed_decision_packet_session_dir: &Path,
    contract: &LivePackageActivationTicketContractView,
) -> Result<PathBuf> {
    let paths = activation_ticket_artifact_paths(activation_ticket_session_dir);
    let archived_report: StoredReviewReceiptReportView =
        load_json(&paths.review_receipt_report_path)?;
    let confirmed_decision_packet_session_dir = confirmed_decision_packet_session_dir.to_path_buf();
    if confirmed_decision_packet_session_dir != contract.decision_packet_session_dir {
        bail!(
            "confirmed decision-packet session dir {:?} does not match stored activation-ticket contract {:?}",
            confirmed_decision_packet_session_dir.display(),
            contract.decision_packet_session_dir.display()
        );
    }
    let archived_decision_packet_session_dir =
        PathBuf::from(archived_report.decision_packet_session_dir);
    if confirmed_decision_packet_session_dir != archived_decision_packet_session_dir {
        bail!(
            "confirmed decision-packet session dir {:?} does not match archived nested review-receipt report {:?}",
            confirmed_decision_packet_session_dir.display(),
            archived_decision_packet_session_dir.display()
        );
    }
    Ok(confirmed_decision_packet_session_dir)
}

fn required_string_field(value: Option<String>, field: &str) -> Result<String> {
    value.ok_or_else(|| anyhow!("activation-ticket verify report is missing {field}"))
}

fn required_u64_field(value: Option<u64>, field: &str) -> Result<u64> {
    value.ok_or_else(|| anyhow!("activation-ticket verify report is missing {field}"))
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
    fn load_contract_reads_stored_activation_ticket_files() {
        let dir = temp_dir("activation_ticket_contract");
        fs::write(
            dir.join("tiny_live_activation_package_activation_ticket.session.json"),
            serde_json::to_vec_pretty(&json!({
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
                "verify_review_receipt_command_summary": "verify review receipt",
                "reviewed_frozen_live_cutover_controller_command_summary": "run frozen cutover",
                "activation_ticket_summary": "activation ticket",
                "execution_warrant_summary": "execution warrant",
                "explicit_statement": "activation ticket statement"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_activation_ticket.status.json"),
            serde_json::to_vec_pretty(&json!({
                "result": "refused_now_by_stage3",
                "handoff_bundle_result": "refused_now_by_stage3",
                "decision_packet_result": "refused_now_by_stage3",
                "execute_frozen_result": "refused_now_by_stage3",
                "activation_ticket_summary": "activation ticket",
                "execution_warrant_summary": "execution warrant",
                "explicit_statement": "activation ticket status statement"
            }))
            .unwrap(),
        )
        .unwrap();

        let contract =
            load_live_package_activation_ticket_contract_for_release_capsule(&dir).unwrap();
        assert_eq!(
            contract.decision_packet_session_dir,
            PathBuf::from("/tmp/decision-packet-session")
        );
        assert_eq!(contract.result.as_deref(), Some("refused_now_by_stage3"));
        assert_eq!(contract.activation_ticket_summary, "activation ticket");
    }

    #[test]
    fn activation_ticket_verify_args_are_exact_and_bounded() {
        let args = activation_ticket_verify_args(
            Path::new("/tmp/review-receipt-session"),
            Path::new("/tmp/decision-packet-session"),
            Path::new("/tmp/activation-ticket-session"),
        );
        assert_eq!(
            args,
            vec![
                "--review-receipt-session-dir",
                "/tmp/review-receipt-session",
                "--confirm-decision-packet-session-dir",
                "/tmp/decision-packet-session",
                "--session-dir",
                "/tmp/activation-ticket-session",
                "--verify-live-package-activation-ticket",
                "--json"
            ]
        );
    }

    #[test]
    fn confirmed_decision_packet_session_dir_must_match_stored_contract_and_archive() {
        let dir = temp_dir("activation_ticket_confirmed_decision_packet");
        let trusted_decision_packet_session_dir = dir.join("trusted-decision-packet");
        let foreign_decision_packet_session_dir = dir.join("foreign-decision-packet");
        fs::create_dir_all(&trusted_decision_packet_session_dir).unwrap();
        fs::create_dir_all(&foreign_decision_packet_session_dir).unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_activation_ticket.session.json"),
            serde_json::to_vec_pretty(&json!({
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
                "verify_review_receipt_command_summary": "verify review receipt",
                "reviewed_frozen_live_cutover_controller_command_summary": "run frozen cutover",
                "activation_ticket_summary": "activation ticket",
                "execution_warrant_summary": "execution warrant",
                "explicit_statement": "activation ticket statement"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_activation_ticket.status.json"),
            serde_json::to_vec_pretty(&json!({
                "result": "refused_now_by_invalid_or_drifted_contract",
                "activation_ticket_summary": "activation ticket",
                "execution_warrant_summary": "execution warrant",
                "explicit_statement": "activation ticket status statement"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_activation_ticket.review_receipt.report.json"),
            serde_json::to_vec_pretty(&json!({
                "decision_packet_session_dir": foreign_decision_packet_session_dir.display().to_string()
            }))
            .unwrap(),
        )
        .unwrap();

        let contract =
            load_live_package_activation_ticket_contract_for_release_capsule(&dir).unwrap();
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
