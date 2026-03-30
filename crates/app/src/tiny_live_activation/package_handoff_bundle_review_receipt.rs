use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const HANDOFF_BUNDLE_BIN: &str = "copybot_tiny_live_activation_package_handoff_bundle";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LivePackageHandoffBundleContractView {
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
    pub decision_packet_result: Option<String>,
    pub execute_frozen_result: Option<String>,
    pub current_pre_activation_gate_verdict: Option<String>,
    pub current_pre_activation_gate_reason: Option<String>,
    pub verify_decision_packet_command_summary: String,
    pub frozen_live_cutover_controller_command_summary: String,
    pub operator_checklist_summary: String,
    pub operator_runbook_summary: String,
    pub handoff_bundle_summary: String,
    pub explicit_statement: String,
}

#[derive(Debug, Clone)]
pub struct VerifiedLivePackageHandoffBundleReviewStep {
    pub report_json: serde_json::Value,
    pub verdict: String,
    pub reason: String,
    pub generated_at: DateTime<Utc>,
    pub contract: LivePackageHandoffBundleContractView,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageHandoffBundleArtifactPaths {
    pub session_path: PathBuf,
    pub status_path: PathBuf,
    pub decision_packet_report_path: PathBuf,
}

#[derive(Debug, Deserialize)]
struct StoredHandoffBundleSession {
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
    verify_decision_packet_command_summary: String,
    frozen_live_cutover_controller_command_summary: String,
    operator_checklist_summary: String,
    operator_runbook_summary: String,
    handoff_bundle_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Deserialize)]
struct StoredHandoffBundleStatus {
    decision_packet_session_dir: String,
    result: String,
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    operator_checklist_summary: String,
    operator_runbook_summary: String,
    handoff_bundle_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Deserialize)]
struct StoredDecisionPacketReportView {
    decision_packet_session_dir: String,
}

#[derive(Debug, Deserialize)]
struct HandoffBundleVerifyReportView {
    generated_at: DateTime<Utc>,
    verdict: String,
    reason: String,
    decision_packet_session_dir: String,
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
    decision_packet_result: Option<String>,
    execute_frozen_result: Option<String>,
    verify_decision_packet_command_summary: Option<String>,
    frozen_live_cutover_controller_command_summary: Option<String>,
    operator_checklist_summary: Option<String>,
    operator_runbook_summary: Option<String>,
    handoff_bundle_summary: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    explicit_statement: String,
}

pub fn load_live_package_handoff_bundle_contract_for_review_receipt(
    handoff_bundle_session_dir: &Path,
) -> Result<LivePackageHandoffBundleContractView> {
    let paths = handoff_bundle_artifact_paths(handoff_bundle_session_dir);
    let session: StoredHandoffBundleSession = load_json(&paths.session_path)?;
    let status: StoredHandoffBundleStatus = load_json(&paths.status_path)?;
    Ok(LivePackageHandoffBundleContractView {
        handoff_bundle_session_dir: handoff_bundle_session_dir.to_path_buf(),
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
        decision_packet_result: status.decision_packet_result,
        execute_frozen_result: status.execute_frozen_result,
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason,
        verify_decision_packet_command_summary: session.verify_decision_packet_command_summary,
        frozen_live_cutover_controller_command_summary: session
            .frozen_live_cutover_controller_command_summary,
        operator_checklist_summary: if status.operator_checklist_summary.is_empty() {
            session.operator_checklist_summary
        } else {
            status.operator_checklist_summary
        },
        operator_runbook_summary: if status.operator_runbook_summary.is_empty() {
            session.operator_runbook_summary
        } else {
            status.operator_runbook_summary
        },
        handoff_bundle_summary: if status.handoff_bundle_summary.is_empty() {
            session.handoff_bundle_summary
        } else {
            status.handoff_bundle_summary
        },
        explicit_statement: if status.explicit_statement.is_empty() {
            session.explicit_statement
        } else {
            status.explicit_statement
        },
    })
}

pub fn verify_live_package_handoff_bundle_for_review_receipt(
    handoff_bundle_session_dir: &Path,
    confirmed_decision_packet_session_dir: &Path,
) -> Result<VerifiedLivePackageHandoffBundleReviewStep> {
    let contract =
        load_live_package_handoff_bundle_contract_for_review_receipt(handoff_bundle_session_dir)?;
    let trusted_decision_packet_session_dir =
        load_confirmed_decision_packet_session_dir_for_verify(
            handoff_bundle_session_dir,
            confirmed_decision_packet_session_dir,
            &contract,
        )?;
    let raw_report = run_json_command(
        HANDOFF_BUNDLE_BIN,
        &handoff_bundle_verify_args(
            &trusted_decision_packet_session_dir,
            handoff_bundle_session_dir,
        ),
    )?;
    let report: HandoffBundleVerifyReportView = serde_json::from_value(raw_report.clone())
        .context("failed parsing handoff-bundle verify report")?;
    let reported_decision_packet_session_dir = PathBuf::from(&report.decision_packet_session_dir);
    if reported_decision_packet_session_dir != trusted_decision_packet_session_dir {
        bail!(
            "handoff-bundle verify report decision_packet_session_dir {:?} does not match trusted {:?}",
            report.decision_packet_session_dir,
            trusted_decision_packet_session_dir.display()
        );
    }
    Ok(VerifiedLivePackageHandoffBundleReviewStep {
        report_json: raw_report,
        verdict: report.verdict.clone(),
        reason: report.reason.clone(),
        generated_at: report.generated_at,
        contract: LivePackageHandoffBundleContractView {
            handoff_bundle_session_dir: handoff_bundle_session_dir.to_path_buf(),
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
            verify_decision_packet_command_summary: required_string_field(
                report.verify_decision_packet_command_summary,
                "verify_decision_packet_command_summary",
            )?,
            frozen_live_cutover_controller_command_summary: required_string_field(
                report.frozen_live_cutover_controller_command_summary,
                "frozen_live_cutover_controller_command_summary",
            )?,
            operator_checklist_summary: required_string_field(
                report.operator_checklist_summary,
                "operator_checklist_summary",
            )?,
            operator_runbook_summary: required_string_field(
                report.operator_runbook_summary,
                "operator_runbook_summary",
            )?,
            handoff_bundle_summary: required_string_field(
                report.handoff_bundle_summary,
                "handoff_bundle_summary",
            )?,
            explicit_statement: report.explicit_statement,
        },
    })
}

pub fn handoff_bundle_verify_args(
    decision_packet_session_dir: &Path,
    handoff_bundle_session_dir: &Path,
) -> Vec<String> {
    vec![
        "--decision-packet-session-dir".to_string(),
        decision_packet_session_dir.display().to_string(),
        "--session-dir".to_string(),
        handoff_bundle_session_dir.display().to_string(),
        "--verify-live-package-handoff-bundle".to_string(),
        "--json".to_string(),
    ]
}

pub fn handoff_bundle_artifact_paths(session_dir: &Path) -> PackageHandoffBundleArtifactPaths {
    PackageHandoffBundleArtifactPaths {
        session_path: session_dir.join("tiny_live_activation_package_handoff_bundle.session.json"),
        status_path: session_dir.join("tiny_live_activation_package_handoff_bundle.status.json"),
        decision_packet_report_path: session_dir
            .join("tiny_live_activation_package_handoff_bundle.decision_packet.report.json"),
    }
}

fn load_trusted_decision_packet_session_dir_for_verify(
    handoff_bundle_session_dir: &Path,
) -> Result<PathBuf> {
    let paths = handoff_bundle_artifact_paths(handoff_bundle_session_dir);
    let session: StoredHandoffBundleSession = load_json(&paths.session_path)?;
    let status: StoredHandoffBundleStatus = load_json(&paths.status_path)?;
    if status.decision_packet_session_dir != session.decision_packet_session_dir {
        bail!(
            "stored handoff-bundle status decision_packet_session_dir {:?} does not match session {:?}",
            status.decision_packet_session_dir,
            session.decision_packet_session_dir
        );
    }
    let report: StoredDecisionPacketReportView = load_json(&paths.decision_packet_report_path)?;
    let trusted_decision_packet_session_dir = PathBuf::from(&session.decision_packet_session_dir);
    let reported_decision_packet_session_dir = PathBuf::from(&report.decision_packet_session_dir);
    if reported_decision_packet_session_dir != trusted_decision_packet_session_dir {
        bail!(
            "stored handoff-bundle decision-packet report decision_packet_session_dir {:?} does not match trusted handoff-bundle session {:?}",
            reported_decision_packet_session_dir.display(),
            trusted_decision_packet_session_dir.display()
        );
    }
    Ok(trusted_decision_packet_session_dir)
}

fn load_confirmed_decision_packet_session_dir_for_verify(
    handoff_bundle_session_dir: &Path,
    confirmed_decision_packet_session_dir: &Path,
    contract: &LivePackageHandoffBundleContractView,
) -> Result<PathBuf> {
    let stored_trusted_decision_packet_session_dir =
        load_trusted_decision_packet_session_dir_for_verify(handoff_bundle_session_dir)?;
    let confirmed_decision_packet_session_dir = confirmed_decision_packet_session_dir.to_path_buf();
    if confirmed_decision_packet_session_dir != contract.decision_packet_session_dir {
        bail!(
            "confirmed decision-packet session dir {:?} does not match stored handoff-bundle contract {:?}",
            confirmed_decision_packet_session_dir.display(),
            contract.decision_packet_session_dir.display()
        );
    }
    if confirmed_decision_packet_session_dir != stored_trusted_decision_packet_session_dir {
        bail!(
            "confirmed decision-packet session dir {:?} does not match stored handoff-bundle archived report {:?}",
            confirmed_decision_packet_session_dir.display(),
            stored_trusted_decision_packet_session_dir.display()
        );
    }
    Ok(confirmed_decision_packet_session_dir)
}

fn required_string_field(value: Option<String>, field: &str) -> Result<String> {
    value.ok_or_else(|| anyhow!("handoff-bundle verify report is missing {field}"))
}

fn required_u64_field(value: Option<u64>, field: &str) -> Result<u64> {
    value.ok_or_else(|| anyhow!("handoff-bundle verify report is missing {field}"))
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
    fn load_contract_reads_stored_handoff_bundle_files() {
        let dir = temp_dir("handoff_bundle_contract");
        fs::write(
            dir.join("tiny_live_activation_package_handoff_bundle.session.json"),
            serde_json::to_vec_pretty(&json!({
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
                "verify_decision_packet_command_summary": "verify decision packet",
                "frozen_live_cutover_controller_command_summary": "run frozen cutover",
                "operator_checklist_summary": "checklist",
                "operator_runbook_summary": "runbook",
                "handoff_bundle_summary": "dossier",
                "explicit_statement": "handoff-bundle statement"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_handoff_bundle.status.json"),
            serde_json::to_vec_pretty(&json!({
                "decision_packet_session_dir": "/tmp/decision-packet-session",
                "result": "refused_now_by_stage3",
                "decision_packet_result": "refused_now_by_stage3",
                "execute_frozen_result": "refused_now_by_stage3",
                "operator_checklist_summary": "checklist",
                "operator_runbook_summary": "runbook",
                "handoff_bundle_summary": "dossier",
                "explicit_statement": "handoff-bundle status statement"
            }))
            .unwrap(),
        )
        .unwrap();

        let contract = load_live_package_handoff_bundle_contract_for_review_receipt(&dir).unwrap();
        assert_eq!(
            contract.decision_packet_session_dir,
            PathBuf::from("/tmp/decision-packet-session")
        );
        assert_eq!(contract.result.as_deref(), Some("refused_now_by_stage3"));
        assert_eq!(contract.handoff_bundle_summary, "dossier");
    }

    #[test]
    fn trusted_decision_packet_session_dir_is_loaded_from_archived_report() {
        let dir = temp_dir("handoff_bundle_trusted_decision_packet");
        let trusted_decision_packet_session_dir = dir.join("trusted-decision-packet");
        fs::create_dir_all(&trusted_decision_packet_session_dir).unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_handoff_bundle.session.json"),
            serde_json::to_vec_pretty(&json!({
                "decision_packet_session_dir": trusted_decision_packet_session_dir.display().to_string(),
                "execute_frozen_session_dir": "/tmp/execute-frozen-session",
                "turn_green_session_dir": "/tmp/turn-green-session",
                "launch_packet_session_dir": "/tmp/launch-packet-session",
                "package_dir": "/tmp/frozen-package",
                "install_root": "/",
                "target_service_name": "solana-copy-bot.service",
                "backend_command": "/tmp/backend",
                "wrapper_timeout_ms": 1200,
                "service_status_max_staleness_ms": 4500,
                "verify_decision_packet_command_summary": "verify decision packet",
                "frozen_live_cutover_controller_command_summary": "run frozen cutover",
                "operator_checklist_summary": "checklist",
                "operator_runbook_summary": "runbook",
                "handoff_bundle_summary": "dossier",
                "explicit_statement": "handoff-bundle statement"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_handoff_bundle.status.json"),
            serde_json::to_vec_pretty(&json!({
                "decision_packet_session_dir": trusted_decision_packet_session_dir.display().to_string(),
                "result": "refused_now_by_stage3",
                "operator_checklist_summary": "checklist",
                "operator_runbook_summary": "runbook",
                "handoff_bundle_summary": "dossier",
                "explicit_statement": "handoff-bundle status statement"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_handoff_bundle.decision_packet.report.json"),
            serde_json::to_vec_pretty(&json!({
                "decision_packet_session_dir": trusted_decision_packet_session_dir.display().to_string()
            }))
            .unwrap(),
        )
        .unwrap();

        let trusted = load_trusted_decision_packet_session_dir_for_verify(&dir).unwrap();
        assert_eq!(trusted, trusted_decision_packet_session_dir);
    }

    #[test]
    fn handoff_bundle_verify_args_are_exact_and_bounded() {
        let args = handoff_bundle_verify_args(
            Path::new("/tmp/decision-packet-session"),
            Path::new("/tmp/handoff-bundle-session"),
        );
        assert_eq!(
            args,
            vec![
                "--decision-packet-session-dir",
                "/tmp/decision-packet-session",
                "--session-dir",
                "/tmp/handoff-bundle-session",
                "--verify-live-package-handoff-bundle",
                "--json"
            ]
        );
    }

    #[test]
    fn confirmed_decision_packet_session_dir_must_match_stored_contract_and_archive() {
        let dir = temp_dir("handoff_bundle_confirmed_decision_packet");
        let trusted_decision_packet_session_dir = dir.join("trusted-decision-packet");
        let foreign_decision_packet_session_dir = dir.join("foreign-decision-packet");
        fs::create_dir_all(&trusted_decision_packet_session_dir).unwrap();
        fs::create_dir_all(&foreign_decision_packet_session_dir).unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_handoff_bundle.session.json"),
            serde_json::to_vec_pretty(&json!({
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
                "verify_decision_packet_command_summary": "verify decision packet",
                "frozen_live_cutover_controller_command_summary": "run frozen cutover",
                "operator_checklist_summary": "checklist",
                "operator_runbook_summary": "runbook",
                "handoff_bundle_summary": "dossier",
                "explicit_statement": "handoff-bundle statement"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_handoff_bundle.status.json"),
            serde_json::to_vec_pretty(&json!({
                "decision_packet_session_dir": foreign_decision_packet_session_dir.display().to_string(),
                "result": "refused_now_by_invalid_or_drifted_contract",
                "operator_checklist_summary": "checklist",
                "operator_runbook_summary": "runbook",
                "handoff_bundle_summary": "dossier",
                "explicit_statement": "handoff-bundle status statement"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_handoff_bundle.decision_packet.report.json"),
            serde_json::to_vec_pretty(&json!({
                "decision_packet_session_dir": foreign_decision_packet_session_dir.display().to_string()
            }))
            .unwrap(),
        )
        .unwrap();

        let contract = load_live_package_handoff_bundle_contract_for_review_receipt(&dir).unwrap();
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
