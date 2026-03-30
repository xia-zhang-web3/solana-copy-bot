use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const EXECUTE_FROZEN_BIN: &str = "copybot_tiny_live_activation_package_execute_frozen";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LivePackageExecuteFrozenContractView {
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
    pub turn_green_result_now: Option<String>,
    pub current_pre_activation_gate_verdict: Option<String>,
    pub current_pre_activation_gate_reason: Option<String>,
    pub verify_turn_green_command_summary: String,
    pub execute_frozen_command_summary: String,
    pub frozen_live_cutover_controller_command_summary: String,
    pub execution_happened: bool,
    pub explicit_statement: String,
}

#[derive(Debug, Clone)]
pub struct VerifiedLivePackageExecuteFrozenDecisionStep {
    pub report_json: serde_json::Value,
    pub verdict: String,
    pub reason: String,
    pub generated_at: DateTime<Utc>,
    pub contract: LivePackageExecuteFrozenContractView,
}

#[derive(Debug, Deserialize)]
struct StoredExecuteFrozenSession {
    turn_green_session_dir: String,
    launch_packet_session_dir: String,
    package_dir: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    verify_turn_green_command_summary: String,
    execute_frozen_command_summary: String,
    frozen_live_cutover_controller_command_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Deserialize)]
struct StoredExecuteFrozenStatus {
    result: String,
    turn_green_result_now: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    turn_green_step: Option<StoredStepArtifact>,
    live_cutover_step: Option<StoredStepArtifact>,
    explicit_statement: String,
}

#[derive(Debug, Deserialize)]
struct StoredStepArtifact {
    path: String,
}

#[derive(Debug, Deserialize)]
struct StoredTurnGreenReportView {
    turn_green_session_dir: String,
}

#[derive(Debug, Deserialize)]
struct ExecuteFrozenVerifyReportView {
    generated_at: DateTime<Utc>,
    verdict: String,
    reason: String,
    turn_green_session_dir: String,
    launch_packet_session_dir: Option<String>,
    package_dir: Option<String>,
    install_root: Option<String>,
    target_service_name: Option<String>,
    backend_command: Option<String>,
    wrapper_timeout_ms: Option<u64>,
    service_status_max_staleness_ms: Option<u64>,
    result: Option<String>,
    turn_green_result_now: Option<String>,
    verify_turn_green_command_summary: Option<String>,
    execute_frozen_command_summary: Option<String>,
    frozen_live_cutover_controller_command_summary: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    execution_happened: bool,
    explicit_statement: String,
}

pub fn load_live_package_execute_frozen_contract_for_decision_packet(
    execute_frozen_session_dir: &Path,
) -> Result<LivePackageExecuteFrozenContractView> {
    let paths = execute_frozen_paths(execute_frozen_session_dir);
    let session: StoredExecuteFrozenSession = load_json(&paths.session_path)?;
    let status: StoredExecuteFrozenStatus = load_json(&paths.status_path)?;
    Ok(LivePackageExecuteFrozenContractView {
        execute_frozen_session_dir: execute_frozen_session_dir.to_path_buf(),
        turn_green_session_dir: PathBuf::from(session.turn_green_session_dir),
        launch_packet_session_dir: PathBuf::from(session.launch_packet_session_dir),
        package_dir: PathBuf::from(session.package_dir),
        install_root: PathBuf::from(session.install_root),
        target_service_name: session.target_service_name,
        backend_command: session.backend_command,
        wrapper_timeout_ms: session.wrapper_timeout_ms,
        service_status_max_staleness_ms: session.service_status_max_staleness_ms,
        result: Some(status.result),
        turn_green_result_now: status.turn_green_result_now,
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason,
        verify_turn_green_command_summary: session.verify_turn_green_command_summary,
        execute_frozen_command_summary: session.execute_frozen_command_summary,
        frozen_live_cutover_controller_command_summary: session
            .frozen_live_cutover_controller_command_summary,
        execution_happened: status.live_cutover_step.is_some(),
        explicit_statement: if status.explicit_statement.is_empty() {
            session.explicit_statement
        } else {
            status.explicit_statement
        },
    })
}

pub fn verify_live_package_execute_frozen_for_decision_packet(
    execute_frozen_session_dir: &Path,
) -> Result<VerifiedLivePackageExecuteFrozenDecisionStep> {
    let trusted_turn_green_session_dir =
        load_trusted_turn_green_session_dir_for_verify(execute_frozen_session_dir)?;
    let contract =
        load_live_package_execute_frozen_contract_for_decision_packet(execute_frozen_session_dir)?;
    let raw_report = run_json_command(
        EXECUTE_FROZEN_BIN,
        &execute_frozen_verify_args(&trusted_turn_green_session_dir, execute_frozen_session_dir),
    )?;
    let report: ExecuteFrozenVerifyReportView = serde_json::from_value(raw_report.clone())
        .context("failed parsing execute-frozen verify report")?;
    let reported_turn_green_session_dir = PathBuf::from(&report.turn_green_session_dir);
    if reported_turn_green_session_dir != trusted_turn_green_session_dir {
        bail!(
            "execute-frozen verify report turn_green_session_dir {:?} does not match trusted {:?}",
            report.turn_green_session_dir,
            trusted_turn_green_session_dir.display()
        );
    }
    Ok(VerifiedLivePackageExecuteFrozenDecisionStep {
        report_json: raw_report,
        verdict: report.verdict.clone(),
        reason: report.reason.clone(),
        generated_at: report.generated_at,
        contract: LivePackageExecuteFrozenContractView {
            execute_frozen_session_dir: execute_frozen_session_dir.to_path_buf(),
            turn_green_session_dir: trusted_turn_green_session_dir,
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
            turn_green_result_now: report
                .turn_green_result_now
                .or(contract.turn_green_result_now),
            current_pre_activation_gate_verdict: report
                .current_pre_activation_gate_verdict
                .or(contract.current_pre_activation_gate_verdict),
            current_pre_activation_gate_reason: report
                .current_pre_activation_gate_reason
                .or(contract.current_pre_activation_gate_reason),
            verify_turn_green_command_summary: required_string_field(
                report.verify_turn_green_command_summary,
                "verify_turn_green_command_summary",
            )?,
            execute_frozen_command_summary: required_string_field(
                report.execute_frozen_command_summary,
                "execute_frozen_command_summary",
            )?,
            frozen_live_cutover_controller_command_summary: required_string_field(
                report.frozen_live_cutover_controller_command_summary,
                "frozen_live_cutover_controller_command_summary",
            )?,
            execution_happened: report.execution_happened,
            explicit_statement: report.explicit_statement,
        },
    })
}

pub fn execute_frozen_verify_args(
    turn_green_session_dir: &Path,
    execute_frozen_session_dir: &Path,
) -> Vec<String> {
    vec![
        "--turn-green-session-dir".to_string(),
        turn_green_session_dir.display().to_string(),
        "--session-dir".to_string(),
        execute_frozen_session_dir.display().to_string(),
        "--verify-live-package-execute-frozen".to_string(),
        "--json".to_string(),
    ]
}

#[derive(Debug, Clone)]
struct ExecuteFrozenPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    turn_green_report_path: PathBuf,
}

fn execute_frozen_paths(session_dir: &Path) -> ExecuteFrozenPaths {
    ExecuteFrozenPaths {
        session_path: session_dir.join("tiny_live_activation_package_execute_frozen.session.json"),
        status_path: session_dir.join("tiny_live_activation_package_execute_frozen.status.json"),
        turn_green_report_path: session_dir
            .join("tiny_live_activation_package_execute_frozen.turn_green.report.json"),
    }
}

fn load_trusted_turn_green_session_dir_for_verify(
    execute_frozen_session_dir: &Path,
) -> Result<PathBuf> {
    let paths = execute_frozen_paths(execute_frozen_session_dir);
    let session: StoredExecuteFrozenSession = load_json(&paths.session_path)?;
    let status: StoredExecuteFrozenStatus = load_json(&paths.status_path)?;
    let turn_green_step = status
        .turn_green_step
        .ok_or_else(|| anyhow!("execute-frozen status is missing turn_green_step"))?;
    let expected_report_path = paths.turn_green_report_path.display().to_string();
    if turn_green_step.path != expected_report_path {
        bail!(
            "execute-frozen turn_green_step path {:?} does not match expected {:?}",
            turn_green_step.path,
            expected_report_path
        );
    }
    let report: StoredTurnGreenReportView = load_json(&paths.turn_green_report_path)?;
    let trusted_turn_green_session_dir = PathBuf::from(&session.turn_green_session_dir);
    if PathBuf::from(&report.turn_green_session_dir) != trusted_turn_green_session_dir {
        bail!(
            "stored execute-frozen turn-green report turn_green_session_dir {:?} does not match trusted execute-frozen session {:?}",
            report.turn_green_session_dir,
            trusted_turn_green_session_dir.display()
        );
    }
    Ok(trusted_turn_green_session_dir)
}

fn required_string_field(value: Option<String>, field: &str) -> Result<String> {
    value.ok_or_else(|| anyhow!("execute-frozen verify report is missing {field}"))
}

fn required_u64_field(value: Option<u64>, field: &str) -> Result<u64> {
    value.ok_or_else(|| anyhow!("execute-frozen verify report is missing {field}"))
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
    fn load_contract_reads_stored_execute_frozen_files() {
        let dir = temp_dir("execute_frozen_contract");
        fs::write(
            dir.join("tiny_live_activation_package_execute_frozen.session.json"),
            serde_json::to_vec_pretty(&json!({
                "turn_green_session_dir": "/tmp/turn-green-session",
                "launch_packet_session_dir": "/tmp/launch-packet-session",
                "package_dir": "/tmp/frozen-package",
                "install_root": "/",
                "target_service_name": "solana-copy-bot.service",
                "backend_command": "/tmp/backend",
                "wrapper_timeout_ms": 1200,
                "service_status_max_staleness_ms": 4500,
                "verify_turn_green_command_summary": "verify turn green",
                "execute_frozen_command_summary": "run execute frozen",
                "frozen_live_cutover_controller_command_summary": "run frozen cutover",
                "explicit_statement": "execute-frozen statement"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_execute_frozen.status.json"),
            serde_json::to_vec_pretty(&json!({
                "result": "refused_now_by_stage3",
                "turn_green_result_now": "refused_now_by_stage3",
                "turn_green_step": {
                    "path": dir.join("tiny_live_activation_package_execute_frozen.turn_green.report.json")
                },
                "operator_next_action_summary": "wait",
                "explicit_statement": "execute-frozen status statement"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_execute_frozen.turn_green.report.json"),
            serde_json::to_vec_pretty(&json!({
                "turn_green_session_dir": "/tmp/turn-green-session"
            }))
            .unwrap(),
        )
        .unwrap();

        let contract = load_live_package_execute_frozen_contract_for_decision_packet(&dir).unwrap();
        assert_eq!(
            contract.turn_green_session_dir,
            PathBuf::from("/tmp/turn-green-session")
        );
        assert_eq!(contract.result.as_deref(), Some("refused_now_by_stage3"));
        assert!(!contract.execution_happened);
    }

    #[test]
    fn trusted_turn_green_session_dir_is_loaded_from_persisted_step_report() {
        let dir = temp_dir("execute_frozen_trusted_turn_green");
        let trusted_turn_green_session_dir = dir.join("trusted-turn-green");
        fs::create_dir_all(&trusted_turn_green_session_dir).unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_execute_frozen.session.json"),
            serde_json::to_vec_pretty(&json!({
                "turn_green_session_dir": trusted_turn_green_session_dir.display().to_string(),
                "launch_packet_session_dir": "/tmp/launch-packet-session",
                "package_dir": "/tmp/frozen-package",
                "install_root": "/",
                "target_service_name": "solana-copy-bot.service",
                "backend_command": "/tmp/backend",
                "wrapper_timeout_ms": 1200,
                "service_status_max_staleness_ms": 4500,
                "verify_turn_green_command_summary": "verify turn green",
                "execute_frozen_command_summary": "run execute frozen",
                "frozen_live_cutover_controller_command_summary": "run frozen cutover",
                "explicit_statement": "execute-frozen statement"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_execute_frozen.status.json"),
            serde_json::to_vec_pretty(&json!({
                "result": "refused_now_by_stage3",
                "turn_green_step": {
                    "path": dir.join("tiny_live_activation_package_execute_frozen.turn_green.report.json")
                },
                "operator_next_action_summary": "wait",
                "explicit_statement": "execute-frozen status statement"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_execute_frozen.turn_green.report.json"),
            serde_json::to_vec_pretty(&json!({
                "turn_green_session_dir": trusted_turn_green_session_dir.display().to_string()
            }))
            .unwrap(),
        )
        .unwrap();

        let trusted = load_trusted_turn_green_session_dir_for_verify(&dir).unwrap();
        assert_eq!(trusted, trusted_turn_green_session_dir);
    }

    #[test]
    fn verify_args_are_exact_and_bounded() {
        let args = execute_frozen_verify_args(
            Path::new("/tmp/turn-green-session"),
            Path::new("/tmp/execute-frozen-session"),
        );
        assert_eq!(
            args,
            vec![
                "--turn-green-session-dir",
                "/tmp/turn-green-session",
                "--session-dir",
                "/tmp/execute-frozen-session",
                "--verify-live-package-execute-frozen",
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
