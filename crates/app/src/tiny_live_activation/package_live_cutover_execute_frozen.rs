use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::path::Path;
use std::process::Command;

const LIVE_CUTOVER_BIN: &str = "copybot_tiny_live_activation_package_live_cutover";

#[derive(Debug, Clone)]
pub struct PackageLiveCutoverExecuteFrozenStep {
    pub report_json: serde_json::Value,
    pub verdict: String,
    pub reason: String,
    pub generated_at: DateTime<Utc>,
    pub result: Option<String>,
    pub run_live_cutover_command_summary: String,
    pub current_pre_activation_gate_verdict: Option<String>,
    pub current_pre_activation_gate_reason: Option<String>,
    pub activation_authorized: bool,
}

#[derive(Debug, Deserialize)]
struct LiveCutoverReportView {
    generated_at: DateTime<Utc>,
    verdict: String,
    reason: String,
    result: Option<String>,
    run_live_cutover_command_summary: String,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    activation_authorized: bool,
}

pub fn run_live_package_cutover_for_execute_frozen(
    package_dir: &Path,
    install_root: &Path,
    target_service_name: &str,
    backend_command: &str,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    session_dir: &Path,
) -> Result<PackageLiveCutoverExecuteFrozenStep> {
    let args = live_cutover_args(
        package_dir,
        install_root,
        target_service_name,
        backend_command,
        wrapper_timeout_ms,
        service_status_max_staleness_ms,
        session_dir,
        "--run-live-package-cutover",
    );
    let raw_report = run_json_command(LIVE_CUTOVER_BIN, &args)?;
    step_from_raw_report(raw_report)
}

pub fn verify_live_package_cutover_for_execute_frozen(
    package_dir: &Path,
    install_root: &Path,
    target_service_name: &str,
    backend_command: &str,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    session_dir: &Path,
) -> Result<PackageLiveCutoverExecuteFrozenStep> {
    let args = live_cutover_args(
        package_dir,
        install_root,
        target_service_name,
        backend_command,
        wrapper_timeout_ms,
        service_status_max_staleness_ms,
        session_dir,
        "--verify-live-package-cutover",
    );
    let raw_report = run_json_command(LIVE_CUTOVER_BIN, &args)?;
    step_from_raw_report(raw_report)
}

pub fn live_cutover_args(
    package_dir: &Path,
    install_root: &Path,
    target_service_name: &str,
    backend_command: &str,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    session_dir: &Path,
    mode_flag: &str,
) -> Vec<String> {
    vec![
        "--package-dir".to_string(),
        package_dir.display().to_string(),
        "--install-root".to_string(),
        install_root.display().to_string(),
        "--target-service".to_string(),
        target_service_name.to_string(),
        "--backend-command".to_string(),
        backend_command.to_string(),
        "--wrapper-timeout-ms".to_string(),
        wrapper_timeout_ms.to_string(),
        "--service-status-max-staleness-ms".to_string(),
        service_status_max_staleness_ms.to_string(),
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        mode_flag.to_string(),
        "--json".to_string(),
    ]
}

fn step_from_raw_report(
    raw_report: serde_json::Value,
) -> Result<PackageLiveCutoverExecuteFrozenStep> {
    let report: LiveCutoverReportView = serde_json::from_value(raw_report.clone())
        .context("failed parsing live-cutover JSON report")?;
    Ok(PackageLiveCutoverExecuteFrozenStep {
        report_json: raw_report,
        verdict: report.verdict,
        reason: report.reason,
        generated_at: report.generated_at,
        result: report.result,
        run_live_cutover_command_summary: report.run_live_cutover_command_summary,
        current_pre_activation_gate_verdict: report.current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason: report.current_pre_activation_gate_reason,
        activation_authorized: report.activation_authorized,
    })
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

    #[test]
    fn run_args_match_frozen_controller_contract() {
        let args = live_cutover_args(
            Path::new("/tmp/package"),
            Path::new("/"),
            "solana-copy-bot.service",
            "/tmp/backend",
            1200,
            4500,
            Path::new("/tmp/live-cutover-session"),
            "--run-live-package-cutover",
        );
        assert_eq!(
            args,
            vec![
                "--package-dir",
                "/tmp/package",
                "--install-root",
                "/",
                "--target-service",
                "solana-copy-bot.service",
                "--backend-command",
                "/tmp/backend",
                "--wrapper-timeout-ms",
                "1200",
                "--service-status-max-staleness-ms",
                "4500",
                "--session-dir",
                "/tmp/live-cutover-session",
                "--run-live-package-cutover",
                "--json"
            ]
        );
    }
}
