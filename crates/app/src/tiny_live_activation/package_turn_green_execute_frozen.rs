use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

pub use crate::tiny_live_activation::install_target_managed_surface::validate_turn_green_session_dir;

const TURN_GREEN_BIN: &str = "copybot_tiny_live_activation_package_turn_green";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LivePackageTurnGreenContractView {
    pub turn_green_session_dir: PathBuf,
    pub launch_packet_session_dir: PathBuf,
    pub package_dir: PathBuf,
    pub install_root: PathBuf,
    pub target_service_name: String,
    pub backend_command: String,
    pub wrapper_timeout_ms: u64,
    pub service_status_max_staleness_ms: u64,
    pub result: Option<String>,
    pub executable_now: bool,
    pub frozen_controller_still_current: bool,
    pub current_pre_activation_gate_verdict: Option<String>,
    pub current_pre_activation_gate_reason: Option<String>,
    pub verify_frozen_launch_packet_summary: String,
    pub refresh_current_authorization_command_summary: String,
    pub frozen_live_cutover_controller_command_summary: String,
    pub current_live_cutover_controller_command_summary: String,
    pub operator_next_action_summary: String,
    pub explicit_statement: String,
}

#[derive(Debug, Clone)]
pub struct VerifiedLivePackageTurnGreenExecuteFrozenStep {
    pub report_json: serde_json::Value,
    pub verdict: String,
    pub reason: String,
    pub generated_at: DateTime<Utc>,
    pub contract: LivePackageTurnGreenContractView,
}

#[derive(Debug, Deserialize)]
struct StoredTurnGreenSession {
    launch_packet_session_dir: String,
    package_dir: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    verify_frozen_launch_packet_summary: String,
    refresh_current_authorization_command_summary: String,
    frozen_live_cutover_controller_command_summary: String,
}

#[derive(Debug, Deserialize)]
struct StoredTurnGreenStatus {
    result: String,
    executable_now: bool,
    frozen_controller_still_current: bool,
    frozen_launch_packet_step: Option<StoredStepArtifact>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    operator_next_action_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Deserialize)]
struct StoredStepArtifact {
    path: String,
}

#[derive(Debug, Deserialize)]
struct StoredCurrentControllerSummary {
    run_live_cutover_command_summary: String,
}

#[derive(Debug, Deserialize)]
struct StoredFrozenLaunchPacketReportView {
    session_dir: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TurnGreenVerifyReportView {
    generated_at: DateTime<Utc>,
    verdict: String,
    reason: String,
    package_dir: Option<String>,
    install_root: Option<String>,
    target_service_name: Option<String>,
    backend_command: Option<String>,
    wrapper_timeout_ms: Option<u64>,
    service_status_max_staleness_ms: Option<u64>,
    result: Option<String>,
    frozen_controller_still_current: bool,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    verify_frozen_launch_packet_summary: Option<String>,
    refresh_current_authorization_command_summary: Option<String>,
    frozen_live_cutover_controller_command_summary: Option<String>,
    current_live_cutover_controller_command_summary: Option<String>,
    operator_next_action_summary: Option<String>,
    executable_now: bool,
    explicit_statement: String,
}

pub fn load_live_package_turn_green_contract_for_execute_frozen(
    turn_green_session_dir: &Path,
) -> Result<LivePackageTurnGreenContractView> {
    let paths = turn_green_paths(turn_green_session_dir);
    let session: StoredTurnGreenSession = load_json(&paths.session_path)?;
    let status: StoredTurnGreenStatus = load_json(&paths.status_path)?;
    let current_controller: StoredCurrentControllerSummary =
        load_json(&paths.current_controller_report_path)?;
    Ok(LivePackageTurnGreenContractView {
        turn_green_session_dir: turn_green_session_dir.to_path_buf(),
        launch_packet_session_dir: PathBuf::from(session.launch_packet_session_dir),
        package_dir: PathBuf::from(session.package_dir),
        install_root: PathBuf::from(session.install_root),
        target_service_name: session.target_service_name,
        backend_command: session.backend_command,
        wrapper_timeout_ms: session.wrapper_timeout_ms,
        service_status_max_staleness_ms: session.service_status_max_staleness_ms,
        result: Some(status.result),
        executable_now: status.executable_now,
        frozen_controller_still_current: status.frozen_controller_still_current,
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason,
        verify_frozen_launch_packet_summary: session.verify_frozen_launch_packet_summary,
        refresh_current_authorization_command_summary: session
            .refresh_current_authorization_command_summary,
        frozen_live_cutover_controller_command_summary: session
            .frozen_live_cutover_controller_command_summary,
        current_live_cutover_controller_command_summary: current_controller
            .run_live_cutover_command_summary,
        operator_next_action_summary: status.operator_next_action_summary,
        explicit_statement: status.explicit_statement,
    })
}

pub fn verify_live_package_turn_green_for_execute_frozen(
    turn_green_session_dir: &Path,
) -> Result<VerifiedLivePackageTurnGreenExecuteFrozenStep> {
    let trusted_launch_packet_session_dir =
        load_trusted_launch_packet_session_dir_for_verify(turn_green_session_dir)?;
    let contract =
        load_live_package_turn_green_contract_for_execute_frozen(turn_green_session_dir)?;
    let raw_report = run_json_command(
        TURN_GREEN_BIN,
        &turn_green_verify_args(&trusted_launch_packet_session_dir, turn_green_session_dir),
    )?;
    let report: TurnGreenVerifyReportView = serde_json::from_value(raw_report.clone())
        .context("failed parsing turn-green verify report")?;
    Ok(VerifiedLivePackageTurnGreenExecuteFrozenStep {
        report_json: raw_report,
        verdict: report.verdict.clone(),
        reason: report.reason.clone(),
        generated_at: report.generated_at,
        contract: LivePackageTurnGreenContractView {
            launch_packet_session_dir: trusted_launch_packet_session_dir,
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
            executable_now: report.executable_now,
            frozen_controller_still_current: report.frozen_controller_still_current,
            current_pre_activation_gate_verdict: report.current_pre_activation_gate_verdict,
            current_pre_activation_gate_reason: report.current_pre_activation_gate_reason,
            verify_frozen_launch_packet_summary: required_string_field(
                report.verify_frozen_launch_packet_summary,
                "verify_frozen_launch_packet_summary",
            )?,
            refresh_current_authorization_command_summary: required_string_field(
                report.refresh_current_authorization_command_summary,
                "refresh_current_authorization_command_summary",
            )?,
            frozen_live_cutover_controller_command_summary: required_string_field(
                report.frozen_live_cutover_controller_command_summary,
                "frozen_live_cutover_controller_command_summary",
            )?,
            current_live_cutover_controller_command_summary: required_string_field(
                report.current_live_cutover_controller_command_summary,
                "current_live_cutover_controller_command_summary",
            )?,
            operator_next_action_summary: required_string_field(
                report.operator_next_action_summary,
                "operator_next_action_summary",
            )?,
            explicit_statement: report.explicit_statement,
            ..contract
        },
    })
}

pub fn turn_green_verify_args(
    launch_packet_session_dir: &Path,
    turn_green_session_dir: &Path,
) -> Vec<String> {
    vec![
        "--launch-packet-session-dir".to_string(),
        launch_packet_session_dir.display().to_string(),
        "--session-dir".to_string(),
        turn_green_session_dir.display().to_string(),
        "--verify-live-package-turn-green".to_string(),
        "--json".to_string(),
    ]
}

#[derive(Debug, Clone)]
struct TurnGreenPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    frozen_launch_packet_report_path: PathBuf,
    current_controller_report_path: PathBuf,
}

fn turn_green_paths(session_dir: &Path) -> TurnGreenPaths {
    TurnGreenPaths {
        session_path: session_dir.join("tiny_live_activation_package_turn_green.session.json"),
        status_path: session_dir.join("tiny_live_activation_package_turn_green.status.json"),
        frozen_launch_packet_report_path: session_dir
            .join("tiny_live_activation_package_turn_green.frozen_launch_packet.report.json"),
        current_controller_report_path: session_dir
            .join("tiny_live_activation_package_turn_green.current_controller.report.json"),
    }
}

fn load_trusted_launch_packet_session_dir_for_verify(
    turn_green_session_dir: &Path,
) -> Result<PathBuf> {
    let paths = turn_green_paths(turn_green_session_dir);
    let status: StoredTurnGreenStatus = load_json(&paths.status_path)?;
    let frozen_launch_packet_step = status
        .frozen_launch_packet_step
        .ok_or_else(|| anyhow!("turn-green status is missing frozen_launch_packet_step"))?;
    let expected_report_path = paths.frozen_launch_packet_report_path.display().to_string();
    if frozen_launch_packet_step.path != expected_report_path {
        bail!(
            "turn-green frozen_launch_packet_step path {:?} does not match expected {:?}",
            frozen_launch_packet_step.path,
            expected_report_path
        );
    }
    let frozen_report: StoredFrozenLaunchPacketReportView =
        load_json(&paths.frozen_launch_packet_report_path)?;
    let session_dir = frozen_report.session_dir.ok_or_else(|| {
        anyhow!(
            "turn-green frozen launch packet report {} is missing session_dir",
            paths.frozen_launch_packet_report_path.display()
        )
    })?;
    Ok(PathBuf::from(session_dir))
}

fn required_string_field(value: Option<String>, field: &str) -> Result<String> {
    value.ok_or_else(|| anyhow!("turn-green verify report is missing {field}"))
}

fn required_u64_field(value: Option<u64>, field: &str) -> Result<u64> {
    value.ok_or_else(|| anyhow!("turn-green verify report is missing {field}"))
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
    fn load_contract_reads_stored_turn_green_files() {
        let dir = temp_dir("turn_green_contract");
        fs::write(
            dir.join("tiny_live_activation_package_turn_green.session.json"),
            serde_json::to_vec_pretty(&json!({
                "launch_packet_session_dir": "/tmp/frozen-packet",
                "package_dir": "/tmp/frozen-package",
                "install_root": "/",
                "target_service_name": "solana-copy-bot.service",
                "backend_command": "/tmp/backend",
                "wrapper_timeout_ms": 1200,
                "service_status_max_staleness_ms": 4500,
                "verify_frozen_launch_packet_summary": "verify packet",
                "refresh_current_authorization_command_summary": "refresh auth",
                "frozen_live_cutover_controller_command_summary": "run cutover"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_turn_green.status.json"),
            serde_json::to_vec_pretty(&json!({
                "result": "executable_now",
                "executable_now": true,
                "frozen_controller_still_current": true,
                "current_pre_activation_gate_verdict": "green",
                "current_pre_activation_gate_reason": "ok",
                "operator_next_action_summary": "run exact controller",
                "explicit_statement": "turn green statement"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_turn_green.current_controller.report.json"),
            serde_json::to_vec_pretty(&json!({
                "run_live_cutover_command_summary": "copybot_tiny_live_activation_package_live_cutover ..."
            }))
            .unwrap(),
        )
        .unwrap();

        let contract = load_live_package_turn_green_contract_for_execute_frozen(&dir).unwrap();
        assert_eq!(contract.install_root, PathBuf::from("/"));
        assert_eq!(contract.result.as_deref(), Some("executable_now"));
        assert!(contract.executable_now);
        assert_eq!(
            contract.current_live_cutover_controller_command_summary,
            "copybot_tiny_live_activation_package_live_cutover ..."
        );
    }

    #[test]
    fn verify_args_are_exact_and_bounded() {
        let args = turn_green_verify_args(
            Path::new("/tmp/frozen-launch-packet"),
            Path::new("/tmp/turn-green-session"),
        );
        assert_eq!(
            args,
            vec![
                "--launch-packet-session-dir",
                "/tmp/frozen-launch-packet",
                "--session-dir",
                "/tmp/turn-green-session",
                "--verify-live-package-turn-green",
                "--json"
            ]
        );
    }

    #[test]
    fn trusted_launch_packet_session_dir_is_loaded_from_persisted_step_report() {
        let dir = temp_dir("turn_green_trusted_launch_packet");
        let trusted_launch_packet_session_dir = dir.join("trusted-launch-packet");
        fs::create_dir_all(&trusted_launch_packet_session_dir).unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_turn_green.status.json"),
            serde_json::to_vec_pretty(&json!({
                "result": "executable_now",
                "executable_now": true,
                "frozen_controller_still_current": true,
                "frozen_launch_packet_step": {
                    "path": dir.join("tiny_live_activation_package_turn_green.frozen_launch_packet.report.json")
                },
                "operator_next_action_summary": "run exact controller",
                "explicit_statement": "turn green statement"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join("tiny_live_activation_package_turn_green.frozen_launch_packet.report.json"),
            serde_json::to_vec_pretty(&json!({
                "session_dir": trusted_launch_packet_session_dir.display().to_string()
            }))
            .unwrap(),
        )
        .unwrap();

        let trusted = load_trusted_launch_packet_session_dir_for_verify(&dir).unwrap();
        assert_eq!(trusted, trusted_launch_packet_session_dir);
    }

    #[test]
    fn session_dir_guard_is_reexported_from_shared_surface() {
        let dir = temp_dir("turn_green_shared_guard");
        let session_dir = dir.join("turn-green-session");
        validate_turn_green_session_dir(Path::new("/"), &session_dir).unwrap();
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
