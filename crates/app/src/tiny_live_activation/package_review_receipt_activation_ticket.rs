use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const REVIEW_RECEIPT_BIN: &str = "copybot_tiny_live_activation_package_review_receipt";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LivePackageReviewReceiptContractView {
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
    pub verify_handoff_bundle_command_summary: String,
    pub reviewed_frozen_live_cutover_controller_command_summary: String,
    pub review_receipt_summary: String,
    pub checklist_acknowledgement_summary: String,
    pub runbook_acknowledgement_summary: String,
    pub explicit_statement: String,
}

#[derive(Debug, Clone)]
pub struct VerifiedLivePackageReviewReceiptActivationTicketStep {
    pub report_json: serde_json::Value,
    pub verdict: String,
    pub reason: String,
    pub generated_at: DateTime<Utc>,
    pub contract: LivePackageReviewReceiptContractView,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageReviewReceiptArtifactPaths {
    pub session_path: PathBuf,
    pub status_path: PathBuf,
    pub handoff_bundle_report_path: PathBuf,
}

#[derive(Debug, Deserialize)]
struct StoredReviewReceiptSession {
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
    verify_handoff_bundle_command_summary: String,
    reviewed_frozen_live_cutover_controller_command_summary: String,
    review_receipt_summary: String,
    checklist_acknowledgement_summary: String,
    runbook_acknowledgement_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Deserialize)]
struct StoredReviewReceiptStatus {
    result: String,
    handoff_bundle_result: Option<String>,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    review_receipt_summary: String,
    checklist_acknowledgement_summary: String,
    runbook_acknowledgement_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Deserialize)]
struct StoredHandoffBundleReportView {
    decision_packet_session_dir: String,
}

#[derive(Debug, Deserialize)]
struct ReviewReceiptVerifyReportView {
    generated_at: DateTime<Utc>,
    verdict: String,
    reason: String,
    handoff_bundle_session_dir: String,
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
    verify_handoff_bundle_command_summary: Option<String>,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    review_receipt_summary: Option<String>,
    checklist_acknowledgement_summary: Option<String>,
    runbook_acknowledgement_summary: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    explicit_statement: String,
}

pub fn load_live_package_review_receipt_contract_for_activation_ticket(
    review_receipt_session_dir: &Path,
) -> Result<LivePackageReviewReceiptContractView> {
    let paths = review_receipt_artifact_paths(review_receipt_session_dir);
    let session: StoredReviewReceiptSession = load_json(&paths.session_path)?;
    let status: StoredReviewReceiptStatus = load_json(&paths.status_path)?;
    Ok(LivePackageReviewReceiptContractView {
        review_receipt_session_dir: review_receipt_session_dir.to_path_buf(),
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
        verify_handoff_bundle_command_summary: session.verify_handoff_bundle_command_summary,
        reviewed_frozen_live_cutover_controller_command_summary: session
            .reviewed_frozen_live_cutover_controller_command_summary,
        review_receipt_summary: if status.review_receipt_summary.is_empty() {
            session.review_receipt_summary
        } else {
            status.review_receipt_summary
        },
        checklist_acknowledgement_summary: if status.checklist_acknowledgement_summary.is_empty() {
            session.checklist_acknowledgement_summary
        } else {
            status.checklist_acknowledgement_summary
        },
        runbook_acknowledgement_summary: if status.runbook_acknowledgement_summary.is_empty() {
            session.runbook_acknowledgement_summary
        } else {
            status.runbook_acknowledgement_summary
        },
        explicit_statement: if status.explicit_statement.is_empty() {
            session.explicit_statement
        } else {
            status.explicit_statement
        },
    })
}

pub fn verify_live_package_review_receipt_for_activation_ticket(
    review_receipt_session_dir: &Path,
    confirmed_decision_packet_session_dir: &Path,
) -> Result<VerifiedLivePackageReviewReceiptActivationTicketStep> {
    let contract = load_live_package_review_receipt_contract_for_activation_ticket(
        review_receipt_session_dir,
    )?;
    let trusted_decision_packet_session_dir =
        load_confirmed_decision_packet_session_dir_for_verify(
            review_receipt_session_dir,
            confirmed_decision_packet_session_dir,
            &contract,
        )?;
    let raw_report = run_json_command(
        REVIEW_RECEIPT_BIN,
        &review_receipt_verify_args(
            &contract.handoff_bundle_session_dir,
            &trusted_decision_packet_session_dir,
            review_receipt_session_dir,
        ),
    )?;
    let report: ReviewReceiptVerifyReportView = serde_json::from_value(raw_report.clone())
        .context("failed parsing review-receipt verify report")?;
    if PathBuf::from(&report.handoff_bundle_session_dir) != contract.handoff_bundle_session_dir {
        bail!(
            "review-receipt verify report handoff_bundle_session_dir {:?} does not match trusted {:?}",
            report.handoff_bundle_session_dir,
            contract.handoff_bundle_session_dir.display()
        );
    }
    let reported_decision_packet_session_dir = PathBuf::from(required_string_field(
        report.decision_packet_session_dir,
        "decision_packet_session_dir",
    )?);
    if reported_decision_packet_session_dir != trusted_decision_packet_session_dir {
        bail!(
            "review-receipt verify report decision_packet_session_dir {:?} does not match trusted {:?}",
            reported_decision_packet_session_dir.display(),
            trusted_decision_packet_session_dir.display()
        );
    }
    Ok(VerifiedLivePackageReviewReceiptActivationTicketStep {
        report_json: raw_report,
        verdict: report.verdict.clone(),
        reason: report.reason.clone(),
        generated_at: report.generated_at,
        contract: LivePackageReviewReceiptContractView {
            review_receipt_session_dir: review_receipt_session_dir.to_path_buf(),
            handoff_bundle_session_dir: contract.handoff_bundle_session_dir,
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
            verify_handoff_bundle_command_summary: required_string_field(
                report.verify_handoff_bundle_command_summary,
                "verify_handoff_bundle_command_summary",
            )?,
            reviewed_frozen_live_cutover_controller_command_summary: required_string_field(
                report.reviewed_frozen_live_cutover_controller_command_summary,
                "reviewed_frozen_live_cutover_controller_command_summary",
            )?,
            review_receipt_summary: required_string_field(
                report.review_receipt_summary,
                "review_receipt_summary",
            )?,
            checklist_acknowledgement_summary: required_string_field(
                report.checklist_acknowledgement_summary,
                "checklist_acknowledgement_summary",
            )?,
            runbook_acknowledgement_summary: required_string_field(
                report.runbook_acknowledgement_summary,
                "runbook_acknowledgement_summary",
            )?,
            explicit_statement: report.explicit_statement,
        },
    })
}

pub fn review_receipt_verify_args(
    handoff_bundle_session_dir: &Path,
    decision_packet_session_dir: &Path,
    review_receipt_session_dir: &Path,
) -> Vec<String> {
    vec![
        "--handoff-bundle-session-dir".to_string(),
        handoff_bundle_session_dir.display().to_string(),
        "--confirm-decision-packet-session-dir".to_string(),
        decision_packet_session_dir.display().to_string(),
        "--session-dir".to_string(),
        review_receipt_session_dir.display().to_string(),
        "--verify-live-package-review-receipt".to_string(),
        "--json".to_string(),
    ]
}

pub fn review_receipt_artifact_paths(session_dir: &Path) -> PackageReviewReceiptArtifactPaths {
    PackageReviewReceiptArtifactPaths {
        session_path: session_dir.join("tiny_live_activation_package_review_receipt.session.json"),
        status_path: session_dir.join("tiny_live_activation_package_review_receipt.status.json"),
        handoff_bundle_report_path: session_dir
            .join("tiny_live_activation_package_review_receipt.handoff_bundle.report.json"),
    }
}

fn load_confirmed_decision_packet_session_dir_for_verify(
    review_receipt_session_dir: &Path,
    confirmed_decision_packet_session_dir: &Path,
    contract: &LivePackageReviewReceiptContractView,
) -> Result<PathBuf> {
    let paths = review_receipt_artifact_paths(review_receipt_session_dir);
    let archived_report: StoredHandoffBundleReportView =
        load_json(&paths.handoff_bundle_report_path)?;
    let confirmed_decision_packet_session_dir = confirmed_decision_packet_session_dir.to_path_buf();
    if confirmed_decision_packet_session_dir != contract.decision_packet_session_dir {
        bail!(
            "confirmed decision-packet session dir {:?} does not match stored review-receipt contract {:?}",
            confirmed_decision_packet_session_dir.display(),
            contract.decision_packet_session_dir.display()
        );
    }
    let archived_decision_packet_session_dir =
        PathBuf::from(archived_report.decision_packet_session_dir);
    if confirmed_decision_packet_session_dir != archived_decision_packet_session_dir {
        bail!(
            "confirmed decision-packet session dir {:?} does not match archived nested handoff-bundle report {:?}",
            confirmed_decision_packet_session_dir.display(),
            archived_decision_packet_session_dir.display()
        );
    }
    Ok(confirmed_decision_packet_session_dir)
}

fn required_string_field(value: Option<String>, field: &str) -> Result<String> {
    value.ok_or_else(|| anyhow!("review-receipt verify report is missing {field}"))
}

fn required_u64_field(value: Option<u64>, field: &str) -> Result<u64> {
    value.ok_or_else(|| anyhow!("review-receipt verify report is missing {field}"))
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
    fn load_contract_reads_stored_review_receipt_files() {
        let dir = temp_dir("review_receipt_contract");
        fs::write(
            dir.join("tiny_live_activation_package_review_receipt.session.json"),
            serde_json::to_vec_pretty(&json!({
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
                "verify_handoff_bundle_command_summary": "verify handoff bundle",
                "reviewed_frozen_live_cutover_controller_command_summary": "run frozen cutover",
                "review_receipt_summary": "review receipt",
                "checklist_acknowledgement_summary": "checklist ack",
                "runbook_acknowledgement_summary": "runbook ack",
                "explicit_statement": "review receipt statement"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_review_receipt.status.json"),
            serde_json::to_vec_pretty(&json!({
                "result": "refused_now_by_stage3",
                "handoff_bundle_result": "refused_now_by_stage3",
                "decision_packet_result": "refused_now_by_stage3",
                "execute_frozen_result": "refused_now_by_stage3",
                "review_receipt_summary": "review receipt",
                "checklist_acknowledgement_summary": "checklist ack",
                "runbook_acknowledgement_summary": "runbook ack",
                "explicit_statement": "review receipt status statement"
            }))
            .unwrap(),
        )
        .unwrap();

        let contract =
            load_live_package_review_receipt_contract_for_activation_ticket(&dir).unwrap();
        assert_eq!(
            contract.decision_packet_session_dir,
            PathBuf::from("/tmp/decision-packet-session")
        );
        assert_eq!(contract.result.as_deref(), Some("refused_now_by_stage3"));
        assert_eq!(contract.review_receipt_summary, "review receipt");
    }

    #[test]
    fn review_receipt_verify_args_are_exact_and_bounded() {
        let args = review_receipt_verify_args(
            Path::new("/tmp/handoff-bundle-session"),
            Path::new("/tmp/decision-packet-session"),
            Path::new("/tmp/review-receipt-session"),
        );
        assert_eq!(
            args,
            vec![
                "--handoff-bundle-session-dir",
                "/tmp/handoff-bundle-session",
                "--confirm-decision-packet-session-dir",
                "/tmp/decision-packet-session",
                "--session-dir",
                "/tmp/review-receipt-session",
                "--verify-live-package-review-receipt",
                "--json"
            ]
        );
    }

    #[test]
    fn confirmed_decision_packet_session_dir_must_match_stored_contract_and_archive() {
        let dir = temp_dir("review_receipt_confirmed_decision_packet");
        let trusted_decision_packet_session_dir = dir.join("trusted-decision-packet");
        let foreign_decision_packet_session_dir = dir.join("foreign-decision-packet");
        fs::create_dir_all(&trusted_decision_packet_session_dir).unwrap();
        fs::create_dir_all(&foreign_decision_packet_session_dir).unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_review_receipt.session.json"),
            serde_json::to_vec_pretty(&json!({
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
                "verify_handoff_bundle_command_summary": "verify handoff bundle",
                "reviewed_frozen_live_cutover_controller_command_summary": "run frozen cutover",
                "review_receipt_summary": "review receipt",
                "checklist_acknowledgement_summary": "checklist ack",
                "runbook_acknowledgement_summary": "runbook ack",
                "explicit_statement": "review receipt statement"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_review_receipt.status.json"),
            serde_json::to_vec_pretty(&json!({
                "result": "refused_now_by_invalid_or_drifted_contract",
                "review_receipt_summary": "review receipt",
                "checklist_acknowledgement_summary": "checklist ack",
                "runbook_acknowledgement_summary": "runbook ack",
                "explicit_statement": "review receipt status statement"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_review_receipt.handoff_bundle.report.json"),
            serde_json::to_vec_pretty(&json!({
                "decision_packet_session_dir": foreign_decision_packet_session_dir.display().to_string()
            }))
            .unwrap(),
        )
        .unwrap();

        let contract =
            load_live_package_review_receipt_contract_for_activation_ticket(&dir).unwrap();
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
