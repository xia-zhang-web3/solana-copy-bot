use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const DECISION_PACKET_BIN: &str = "copybot_tiny_live_activation_package_decision_packet";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LivePackageDecisionPacketContractView {
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
    pub execute_frozen_result: Option<String>,
    pub current_pre_activation_gate_verdict: Option<String>,
    pub current_pre_activation_gate_reason: Option<String>,
    pub verify_execute_frozen_command_summary: String,
    pub frozen_live_cutover_controller_command_summary: String,
    pub operator_checklist_summary: String,
    pub operator_runbook_summary: String,
    pub explicit_statement: String,
}

#[derive(Debug, Clone)]
pub struct VerifiedLivePackageDecisionPacketHandoffStep {
    pub report_json: serde_json::Value,
    pub verdict: String,
    pub reason: String,
    pub generated_at: DateTime<Utc>,
    pub contract: LivePackageDecisionPacketContractView,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageDecisionPacketArtifactPaths {
    pub session_path: PathBuf,
    pub status_path: PathBuf,
    pub execute_frozen_report_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageExecuteFrozenArtifactPaths {
    pub session_path: PathBuf,
    pub status_path: PathBuf,
    pub turn_green_report_path: PathBuf,
    pub live_cutover_report_path: PathBuf,
}

#[derive(Debug, Deserialize)]
struct StoredDecisionPacketSession {
    execute_frozen_session_dir: String,
    turn_green_session_dir: String,
    launch_packet_session_dir: String,
    package_dir: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    verify_execute_frozen_command_summary: String,
    frozen_live_cutover_controller_command_summary: String,
    operator_checklist_summary: String,
    operator_runbook_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Deserialize)]
struct StoredDecisionPacketStatus {
    execute_frozen_session_dir: String,
    result: String,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    execute_frozen_step: Option<StoredStepArtifact>,
    operator_checklist_summary: String,
    operator_runbook_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Deserialize)]
struct StoredStepArtifact {
    path: String,
}

#[derive(Debug, Deserialize)]
struct StoredExecuteFrozenReportView {
    session_dir: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DecisionPacketVerifyReportView {
    generated_at: DateTime<Utc>,
    verdict: String,
    reason: String,
    execute_frozen_session_dir: String,
    turn_green_session_dir: Option<String>,
    launch_packet_session_dir: Option<String>,
    package_dir: Option<String>,
    install_root: Option<String>,
    target_service_name: Option<String>,
    backend_command: Option<String>,
    wrapper_timeout_ms: Option<u64>,
    service_status_max_staleness_ms: Option<u64>,
    result: Option<String>,
    execute_frozen_result: Option<String>,
    verify_execute_frozen_command_summary: Option<String>,
    frozen_live_cutover_controller_command_summary: Option<String>,
    operator_checklist_summary: Option<String>,
    operator_runbook_summary: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    explicit_statement: String,
}

pub fn load_live_package_decision_packet_contract_for_handoff_bundle(
    decision_packet_session_dir: &Path,
) -> Result<LivePackageDecisionPacketContractView> {
    let paths = decision_packet_artifact_paths(decision_packet_session_dir);
    let session: StoredDecisionPacketSession = load_json(&paths.session_path)?;
    let status: StoredDecisionPacketStatus = load_json(&paths.status_path)?;
    Ok(LivePackageDecisionPacketContractView {
        decision_packet_session_dir: decision_packet_session_dir.to_path_buf(),
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
        execute_frozen_result: status.execute_frozen_result,
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason,
        verify_execute_frozen_command_summary: session.verify_execute_frozen_command_summary,
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
        explicit_statement: if status.explicit_statement.is_empty() {
            session.explicit_statement
        } else {
            status.explicit_statement
        },
    })
}

pub fn verify_live_package_decision_packet_for_handoff_bundle(
    decision_packet_session_dir: &Path,
) -> Result<VerifiedLivePackageDecisionPacketHandoffStep> {
    let trusted_execute_frozen_session_dir =
        load_trusted_execute_frozen_session_dir_for_verify(decision_packet_session_dir)?;
    let contract =
        load_live_package_decision_packet_contract_for_handoff_bundle(decision_packet_session_dir)?;
    let raw_report = run_json_command(
        DECISION_PACKET_BIN,
        &decision_packet_verify_args(
            &trusted_execute_frozen_session_dir,
            decision_packet_session_dir,
        ),
    )?;
    let report: DecisionPacketVerifyReportView = serde_json::from_value(raw_report.clone())
        .context("failed parsing decision-packet verify report")?;
    let reported_execute_frozen_session_dir = PathBuf::from(&report.execute_frozen_session_dir);
    if reported_execute_frozen_session_dir != trusted_execute_frozen_session_dir {
        bail!(
            "decision-packet verify report execute_frozen_session_dir {:?} does not match trusted {:?}",
            report.execute_frozen_session_dir,
            trusted_execute_frozen_session_dir.display()
        );
    }
    Ok(VerifiedLivePackageDecisionPacketHandoffStep {
        report_json: raw_report,
        verdict: report.verdict.clone(),
        reason: report.reason.clone(),
        generated_at: report.generated_at,
        contract: LivePackageDecisionPacketContractView {
            decision_packet_session_dir: decision_packet_session_dir.to_path_buf(),
            execute_frozen_session_dir: trusted_execute_frozen_session_dir,
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
            execute_frozen_result: report
                .execute_frozen_result
                .or(contract.execute_frozen_result),
            current_pre_activation_gate_verdict: report
                .current_pre_activation_gate_verdict
                .or(contract.current_pre_activation_gate_verdict),
            current_pre_activation_gate_reason: report
                .current_pre_activation_gate_reason
                .or(contract.current_pre_activation_gate_reason),
            verify_execute_frozen_command_summary: required_string_field(
                report.verify_execute_frozen_command_summary,
                "verify_execute_frozen_command_summary",
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
            explicit_statement: report.explicit_statement,
        },
    })
}

pub fn decision_packet_verify_args(
    execute_frozen_session_dir: &Path,
    decision_packet_session_dir: &Path,
) -> Vec<String> {
    vec![
        "--execute-frozen-session-dir".to_string(),
        execute_frozen_session_dir.display().to_string(),
        "--session-dir".to_string(),
        decision_packet_session_dir.display().to_string(),
        "--verify-live-package-decision-packet".to_string(),
        "--json".to_string(),
    ]
}

pub fn decision_packet_artifact_paths(session_dir: &Path) -> PackageDecisionPacketArtifactPaths {
    PackageDecisionPacketArtifactPaths {
        session_path: session_dir.join("tiny_live_activation_package_decision_packet.session.json"),
        status_path: session_dir.join("tiny_live_activation_package_decision_packet.status.json"),
        execute_frozen_report_path: session_dir
            .join("tiny_live_activation_package_decision_packet.execute_frozen.report.json"),
    }
}

pub fn execute_frozen_artifact_paths(session_dir: &Path) -> PackageExecuteFrozenArtifactPaths {
    PackageExecuteFrozenArtifactPaths {
        session_path: session_dir.join("tiny_live_activation_package_execute_frozen.session.json"),
        status_path: session_dir.join("tiny_live_activation_package_execute_frozen.status.json"),
        turn_green_report_path: session_dir
            .join("tiny_live_activation_package_execute_frozen.turn_green.report.json"),
        live_cutover_report_path: session_dir
            .join("tiny_live_activation_package_execute_frozen.live_cutover.report.json"),
    }
}

fn load_trusted_execute_frozen_session_dir_for_verify(
    decision_packet_session_dir: &Path,
) -> Result<PathBuf> {
    let paths = decision_packet_artifact_paths(decision_packet_session_dir);
    let session: StoredDecisionPacketSession = load_json(&paths.session_path)?;
    let status: StoredDecisionPacketStatus = load_json(&paths.status_path)?;
    if status.execute_frozen_session_dir != session.execute_frozen_session_dir {
        bail!(
            "stored decision-packet status execute_frozen_session_dir {:?} does not match session {:?}",
            status.execute_frozen_session_dir,
            session.execute_frozen_session_dir
        );
    }
    let execute_frozen_step = status
        .execute_frozen_step
        .ok_or_else(|| anyhow!("decision-packet status is missing execute_frozen_step"))?;
    let expected_report_path = paths.execute_frozen_report_path.display().to_string();
    if execute_frozen_step.path != expected_report_path {
        bail!(
            "decision-packet execute_frozen_step path {:?} does not match expected {:?}",
            execute_frozen_step.path,
            expected_report_path
        );
    }
    let report: StoredExecuteFrozenReportView = load_json(&paths.execute_frozen_report_path)?;
    let trusted_execute_frozen_session_dir = PathBuf::from(&session.execute_frozen_session_dir);
    let reported_execute_frozen_session_dir =
        PathBuf::from(report.session_dir.ok_or_else(|| {
            anyhow!(
                "decision-packet execute-frozen report {} is missing session_dir",
                paths.execute_frozen_report_path.display()
            )
        })?);
    if reported_execute_frozen_session_dir != trusted_execute_frozen_session_dir {
        bail!(
            "stored decision-packet execute-frozen report session_dir {:?} does not match trusted decision-packet session {:?}",
            reported_execute_frozen_session_dir.display(),
            trusted_execute_frozen_session_dir.display()
        );
    }
    Ok(trusted_execute_frozen_session_dir)
}

fn required_string_field(value: Option<String>, field: &str) -> Result<String> {
    value.ok_or_else(|| anyhow!("decision-packet verify report is missing {field}"))
}

fn required_u64_field(value: Option<u64>, field: &str) -> Result<u64> {
    value.ok_or_else(|| anyhow!("decision-packet verify report is missing {field}"))
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
    fn load_contract_reads_stored_decision_packet_files() {
        let dir = temp_dir("decision_packet_contract");
        fs::write(
            dir.join("tiny_live_activation_package_decision_packet.session.json"),
            serde_json::to_vec_pretty(&json!({
                "execute_frozen_session_dir": "/tmp/execute-frozen-session",
                "turn_green_session_dir": "/tmp/turn-green-session",
                "launch_packet_session_dir": "/tmp/launch-packet-session",
                "package_dir": "/tmp/frozen-package",
                "install_root": "/",
                "target_service_name": "solana-copy-bot.service",
                "backend_command": "/tmp/backend",
                "wrapper_timeout_ms": 1200,
                "service_status_max_staleness_ms": 4500,
                "verify_execute_frozen_command_summary": "verify execute frozen",
                "frozen_live_cutover_controller_command_summary": "run frozen cutover",
                "operator_checklist_summary": "checklist",
                "operator_runbook_summary": "runbook",
                "explicit_statement": "decision-packet statement"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_decision_packet.status.json"),
            serde_json::to_vec_pretty(&json!({
                "execute_frozen_session_dir": "/tmp/execute-frozen-session",
                "result": "runnable_when_gate_truth_turns_green",
                "execute_frozen_result": "completed_keep_running",
                "execute_frozen_step": {
                    "path": dir.join("tiny_live_activation_package_decision_packet.execute_frozen.report.json")
                },
                "operator_checklist_summary": "checklist",
                "operator_runbook_summary": "runbook",
                "explicit_statement": "decision-packet status statement"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_decision_packet.execute_frozen.report.json"),
            serde_json::to_vec_pretty(&json!({
                "session_dir": "/tmp/execute-frozen-session"
            }))
            .unwrap(),
        )
        .unwrap();

        let contract = load_live_package_decision_packet_contract_for_handoff_bundle(&dir).unwrap();
        assert_eq!(
            contract.execute_frozen_session_dir,
            PathBuf::from("/tmp/execute-frozen-session")
        );
        assert_eq!(
            contract.result.as_deref(),
            Some("runnable_when_gate_truth_turns_green")
        );
    }

    #[test]
    fn trusted_execute_frozen_session_dir_is_loaded_from_persisted_step_report() {
        let dir = temp_dir("decision_packet_trusted_execute_frozen");
        let trusted_execute_frozen_session_dir = dir.join("trusted-execute-frozen");
        fs::create_dir_all(&trusted_execute_frozen_session_dir).unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_decision_packet.session.json"),
            serde_json::to_vec_pretty(&json!({
                "execute_frozen_session_dir": trusted_execute_frozen_session_dir.display().to_string(),
                "turn_green_session_dir": "/tmp/turn-green-session",
                "launch_packet_session_dir": "/tmp/launch-packet-session",
                "package_dir": "/tmp/frozen-package",
                "install_root": "/",
                "target_service_name": "solana-copy-bot.service",
                "backend_command": "/tmp/backend",
                "wrapper_timeout_ms": 1200,
                "service_status_max_staleness_ms": 4500,
                "verify_execute_frozen_command_summary": "verify execute frozen",
                "frozen_live_cutover_controller_command_summary": "run frozen cutover",
                "operator_checklist_summary": "checklist",
                "operator_runbook_summary": "runbook",
                "explicit_statement": "decision-packet statement"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_decision_packet.status.json"),
            serde_json::to_vec_pretty(&json!({
                "execute_frozen_session_dir": trusted_execute_frozen_session_dir.display().to_string(),
                "result": "refused_now_by_stage3",
                "execute_frozen_step": {
                    "path": dir.join("tiny_live_activation_package_decision_packet.execute_frozen.report.json")
                },
                "operator_checklist_summary": "checklist",
                "operator_runbook_summary": "runbook",
                "explicit_statement": "decision-packet status statement"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_decision_packet.execute_frozen.report.json"),
            serde_json::to_vec_pretty(&json!({
                "session_dir": trusted_execute_frozen_session_dir.display().to_string()
            }))
            .unwrap(),
        )
        .unwrap();

        let trusted = load_trusted_execute_frozen_session_dir_for_verify(&dir).unwrap();
        assert_eq!(trusted, trusted_execute_frozen_session_dir);
    }

    #[test]
    fn decision_packet_verify_args_are_exact_and_bounded() {
        let args = decision_packet_verify_args(
            Path::new("/tmp/execute-frozen-session"),
            Path::new("/tmp/decision-packet-session"),
        );
        assert_eq!(
            args,
            vec![
                "--execute-frozen-session-dir",
                "/tmp/execute-frozen-session",
                "--session-dir",
                "/tmp/decision-packet-session",
                "--verify-live-package-decision-packet",
                "--json"
            ]
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
