use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_install_target.rs"]
mod tiny_live_activation_install_target;
#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_live_execute.rs"]
mod tiny_live_activation_live_execute;
#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_package.rs"]
mod tiny_live_activation_package;
#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_package_deploy.rs"]
mod tiny_live_activation_package_deploy;

const USAGE: &str = "usage: copybot_tiny_live_activation_package_preflight --package-dir <path> --install-root <path> --target-service <name> --backend-command <path-or-name> [--wrapper-timeout-ms <ms>] [--service-status-max-staleness-ms <ms>] [--session-dir <path>] [--output <path>] [--json] (--plan-live-package-preflight | --render-live-package-preflight-script | --run-live-package-preflight | --verify-live-package-preflight)";
const PREFLIGHT_SESSION_VERSION: &str = "1";
const PREFLIGHT_STATUS_VERSION: &str = "1";
const SERVICE_STATUS_REPORT_VERSION: &str = "1";
pub(crate) const DEFAULT_SERVICE_STATUS_MAX_STALENESS_MS: u64 = 60_000;
const DEFAULT_STATUS_COMMAND_TIMEOUT_MS: u64 = 15_000;

fn main() -> Result<()> {
    let Some(config) = parse_args()? else {
        println!("{USAGE}");
        return Ok(());
    };
    let output = run(config)?;
    println!("{output}");
    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    mode: Mode,
    package_dir: PathBuf,
    install_root: PathBuf,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    session_dir: Option<PathBuf>,
    output_path: Option<PathBuf>,
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    PlanLivePackagePreflight,
    RenderLivePackagePreflightScript,
    RunLivePackagePreflight,
    VerifyLivePackagePreflight,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackagePreflightVerdict {
    TinyLivePackagePreflightPlanReady,
    TinyLivePackagePreflightScriptRendered,
    TinyLivePackagePreflightCompletedReadyForCutoverPlanning,
    TinyLivePackagePreflightCompletedInstallTargetMissing,
    TinyLivePackagePreflightCompletedInstallTargetDrifted,
    TinyLivePackagePreflightCompletedCutoverBlockedByGate,
    TinyLivePackagePreflightFailedDuringPackageVerify,
    TinyLivePackagePreflightFailedDuringLiveTargetVerify,
    TinyLivePackagePreflightFailedDuringServiceStatusVerify,
    TinyLivePackagePreflightVerifyOk,
    TinyLivePackagePreflightVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LivePackagePreflightResult {
    ReadyForCutoverPlanning,
    InstallTargetMissing,
    InstallTargetDrifted,
    CutoverBlockedByGate,
    FailedDuringPackageVerify,
    FailedDuringLiveTargetVerify,
    FailedDuringServiceStatusVerify,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackagePreflightStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackagePreflightSession {
    session_version: String,
    planned_at: DateTime<Utc>,
    package_dir: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    session_dir: String,
    package_verify_command_summary: String,
    verify_install_target_command_summary: String,
    verify_wrapper_binding_command_summary: String,
    verify_service_status_command_summary: String,
    package_cutover_plan_command_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackagePreflightStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    session_dir: String,
    result: LivePackagePreflightResult,
    reason: String,
    package_verify_step: Option<PackagePreflightStepArtifact>,
    live_target_verify_step: Option<PackagePreflightStepArtifact>,
    wrapper_verify_step: Option<PackagePreflightStepArtifact>,
    service_status_step: Option<PackagePreflightStepArtifact>,
    cutover_plan_step: Option<PackagePreflightStepArtifact>,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackagePreflightPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    package_verify_report_path: PathBuf,
    live_target_verify_report_path: PathBuf,
    wrapper_verify_report_path: PathBuf,
    service_status_report_path: PathBuf,
    service_status_artifact_path: PathBuf,
    service_status_runtime_dir: PathBuf,
    service_status_log_path: PathBuf,
    cutover_plan_report_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackagePreflightReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackagePreflightVerdict,
    reason: String,
    package_dir: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    session_dir: Option<String>,
    preflight_session_path: Option<String>,
    preflight_status_path: Option<String>,
    result: Option<String>,
    package_verify_step: Option<PackagePreflightStepArtifact>,
    live_target_verify_step: Option<PackagePreflightStepArtifact>,
    wrapper_verify_step: Option<PackagePreflightStepArtifact>,
    service_status_step: Option<PackagePreflightStepArtifact>,
    cutover_plan_step: Option<PackagePreflightStepArtifact>,
    package_verify_command_summary: String,
    verify_install_target_command_summary: String,
    verify_wrapper_binding_command_summary: String,
    verify_service_status_command_summary: String,
    package_cutover_plan_command_summary: String,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    service_status_observed_at: Option<DateTime<Utc>>,
    service_status_age_ms: Option<i64>,
    verification_mismatches: Vec<String>,
    activation_authorized: bool,
    explicit_statement: String,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct PackageDeployReportView {
    verdict: String,
    reason: String,
    package_dir: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    packaged_activation_config_path: Option<String>,
    packaged_activation_metadata_path: Option<String>,
    packaged_rollback_config_path: Option<String>,
    packaged_wrapper_path: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    activation_authorized: bool,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct InstallTargetVerificationView {
    mismatches: Vec<String>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct InstallTargetReportView {
    verdict: String,
    reason: String,
    install_root: String,
    wrapper_path: String,
    target_config_path: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    installed_activation_config_path: String,
    installed_rollback_config_path: String,
    runtime_dir: String,
    backup_dir: String,
    session_dir: String,
    verification: Option<InstallTargetVerificationView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WrapperBindingReport {
    report_version: String,
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: String,
    reason: String,
    package_dir: String,
    install_root: String,
    packaged_wrapper_path: String,
    installed_wrapper_path: String,
    packaged_activation_config_path: String,
    installed_activation_config_path: String,
    packaged_rollback_config_path: String,
    installed_rollback_config_path: String,
    wrapper_bytes_match_package: bool,
    activation_bytes_match_package: bool,
    rollback_bytes_match_package: bool,
    service_control_wrapper_verification:
        tiny_live_activation_live_execute::live_service_control_wrapper_contract::WrapperVerificationSummary,
    verification_mismatches: Vec<String>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ServiceStatusReport {
    report_version: String,
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: String,
    reason: String,
    package_dir: String,
    install_root: String,
    installed_wrapper_path: String,
    target_config_path: String,
    target_service_name: String,
    backend_command: String,
    scratch_runtime_dir: String,
    service_status_path: String,
    service_status_observed_at: Option<DateTime<Utc>>,
    service_status_age_ms: Option<i64>,
    max_service_status_staleness_ms: u64,
    expected_config_fingerprint_sha256: String,
    current_target_config_fingerprint_sha256: String,
    expected_execution_enabled: bool,
    current_target_execution_enabled: bool,
    command_exit_success: bool,
    service_control_wrapper_verification:
        tiny_live_activation_live_execute::live_service_control_wrapper_contract::WrapperVerificationSummary,
    verification_mismatches: Vec<String>,
    explicit_statement: String,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut package_dir: Option<PathBuf> = None;
    let mut install_root: Option<PathBuf> = None;
    let mut target_service_name: Option<String> = None;
    let mut backend_command: Option<String> = None;
    let mut wrapper_timeout_ms =
        tiny_live_activation_live_execute::live_service_control_wrapper_contract::DEFAULT_TIMEOUT_MS;
    let mut service_status_max_staleness_ms = DEFAULT_SERVICE_STATUS_MAX_STALENESS_MS;
    let mut session_dir: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut json = false;
    let mut mode: Option<Mode> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--package-dir" => {
                package_dir = Some(PathBuf::from(parse_string_arg(
                    "--package-dir",
                    args.next(),
                )?))
            }
            "--install-root" => {
                install_root = Some(PathBuf::from(parse_string_arg(
                    "--install-root",
                    args.next(),
                )?))
            }
            "--target-service" => {
                target_service_name = Some(parse_string_arg("--target-service", args.next())?)
            }
            "--backend-command" => {
                backend_command = Some(parse_string_arg("--backend-command", args.next())?)
            }
            "--wrapper-timeout-ms" => {
                wrapper_timeout_ms = parse_u64_arg("--wrapper-timeout-ms", args.next())?
            }
            "--service-status-max-staleness-ms" => {
                service_status_max_staleness_ms =
                    parse_u64_arg("--service-status-max-staleness-ms", args.next())?
            }
            "--session-dir" => {
                session_dir = Some(PathBuf::from(parse_string_arg(
                    "--session-dir",
                    args.next(),
                )?))
            }
            "--output" => {
                output_path = Some(PathBuf::from(parse_string_arg("--output", args.next())?))
            }
            "--json" => json = true,
            "--plan-live-package-preflight" => set_mode(
                &mut mode,
                Mode::PlanLivePackagePreflight,
                "--plan-live-package-preflight",
            )?,
            "--render-live-package-preflight-script" => set_mode(
                &mut mode,
                Mode::RenderLivePackagePreflightScript,
                "--render-live-package-preflight-script",
            )?,
            "--run-live-package-preflight" => set_mode(
                &mut mode,
                Mode::RunLivePackagePreflight,
                "--run-live-package-preflight",
            )?,
            "--verify-live-package-preflight" => set_mode(
                &mut mode,
                Mode::VerifyLivePackagePreflight,
                "--verify-live-package-preflight",
            )?,
            "--help" | "-h" => return Ok(None),
            other => bail!("unrecognized argument: {other}"),
        }
    }

    let mode = mode.ok_or_else(|| anyhow!("missing required mode"))?;
    let package_dir = package_dir.ok_or_else(|| anyhow!("missing required --package-dir"))?;
    let install_root = install_root.ok_or_else(|| anyhow!("missing required --install-root"))?;
    let target_service_name =
        target_service_name.ok_or_else(|| anyhow!("missing required --target-service"))?;
    let backend_command =
        backend_command.ok_or_else(|| anyhow!("missing required --backend-command"))?;
    if matches!(
        mode,
        Mode::RunLivePackagePreflight | Mode::VerifyLivePackagePreflight
    ) && session_dir.is_none()
    {
        bail!("missing required --session-dir for run/verify live package preflight");
    }
    if matches!(mode, Mode::RenderLivePackagePreflightScript) && output_path.is_none() {
        bail!("missing required --output for --render-live-package-preflight-script");
    }

    Ok(Some(Config {
        mode,
        package_dir,
        install_root,
        target_service_name,
        backend_command,
        wrapper_timeout_ms,
        service_status_max_staleness_ms,
        session_dir,
        output_path,
        json,
    }))
}

fn set_mode(mode: &mut Option<Mode>, next: Mode, flag: &str) -> Result<()> {
    if mode.replace(next).is_some() {
        bail!("multiple modes provided; {flag} conflicts with earlier mode");
    }
    Ok(())
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    value.ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn parse_u64_arg(flag: &str, value: Option<String>) -> Result<u64> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<u64>()
        .with_context(|| format!("failed parsing {flag} as u64"))
}

fn run(config: Config) -> Result<String> {
    let report = match config.mode {
        Mode::PlanLivePackagePreflight => plan_live_package_preflight_report(&config)?,
        Mode::RenderLivePackagePreflightScript => {
            render_live_package_preflight_script_report(&config)?
        }
        Mode::RunLivePackagePreflight => run_live_package_preflight_report(&config)?,
        Mode::VerifyLivePackagePreflight => verify_live_package_preflight_report(&config)?,
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_human_report(&report))
    }
}

#[allow(dead_code)]
pub(crate) struct PackagePreflightCapabilityStep {
    pub(crate) report_json: serde_json::Value,
    pub(crate) verdict: String,
    pub(crate) reason: String,
    pub(crate) generated_at: DateTime<Utc>,
    pub(crate) result: Option<String>,
    pub(crate) current_pre_activation_gate_verdict: Option<String>,
    pub(crate) current_pre_activation_gate_reason: Option<String>,
    pub(crate) service_status_observed_at: Option<DateTime<Utc>>,
    pub(crate) activation_authorized: bool,
}

#[allow(dead_code)]
pub(crate) fn run_live_package_preflight_for_capability(
    package_dir: &Path,
    install_root: &Path,
    target_service_name: &str,
    backend_command: &str,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    session_dir: &Path,
    after_service_status_hook: Option<&dyn Fn(&Path) -> Result<()>>,
) -> Result<PackagePreflightCapabilityStep> {
    let config = Config {
        mode: Mode::RunLivePackagePreflight,
        package_dir: package_dir.to_path_buf(),
        install_root: install_root.to_path_buf(),
        target_service_name: target_service_name.to_string(),
        backend_command: backend_command.to_string(),
        wrapper_timeout_ms,
        service_status_max_staleness_ms,
        session_dir: Some(session_dir.to_path_buf()),
        output_path: None,
        json: true,
    };
    let report = run_live_package_preflight_report_with_hook(&config, after_service_status_hook)?;
    package_preflight_capability_step(report)
}

#[allow(dead_code)]
pub(crate) fn verify_live_package_preflight_for_capability(
    package_dir: &Path,
    install_root: &Path,
    target_service_name: &str,
    backend_command: &str,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    session_dir: &Path,
) -> Result<PackagePreflightCapabilityStep> {
    let config = Config {
        mode: Mode::VerifyLivePackagePreflight,
        package_dir: package_dir.to_path_buf(),
        install_root: install_root.to_path_buf(),
        target_service_name: target_service_name.to_string(),
        backend_command: backend_command.to_string(),
        wrapper_timeout_ms,
        service_status_max_staleness_ms,
        session_dir: Some(session_dir.to_path_buf()),
        output_path: None,
        json: true,
    };
    let report = verify_live_package_preflight_report(&config)?;
    package_preflight_capability_step(report)
}

fn package_preflight_capability_step(
    report: PackagePreflightReport,
) -> Result<PackagePreflightCapabilityStep> {
    let activation_authorized = matches!(
        report.verdict,
        TinyLivePackagePreflightVerdict::TinyLivePackagePreflightCompletedReadyForCutoverPlanning
            | TinyLivePackagePreflightVerdict::TinyLivePackagePreflightVerifyOk
    ) && report.result.as_deref() == Some("ready_for_cutover_planning");
    Ok(PackagePreflightCapabilityStep {
        report_json: serde_json::to_value(&report)?,
        verdict: serialize_enum(&report.verdict),
        reason: report.reason.clone(),
        generated_at: report.generated_at,
        result: report.result.clone(),
        current_pre_activation_gate_verdict: report.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: report.current_pre_activation_gate_reason.clone(),
        service_status_observed_at: report.service_status_observed_at,
        activation_authorized,
    })
}

fn plan_live_package_preflight_report(config: &Config) -> Result<PackagePreflightReport> {
    let step = tiny_live_activation_package_deploy::run_mode_for_rehearsal(
        &config.package_dir,
        &config.install_root,
        &config.target_service_name,
        &config.backend_command,
        config.wrapper_timeout_ms,
        tiny_live_activation_package_deploy::RehearsalMode::VerifyPackageDeployTarget,
    )?;
    if step.verdict != "tiny_live_package_deploy_verify_ok" {
        return Ok(base_report(
            config,
            TinyLivePackagePreflightVerdict::TinyLivePackagePreflightFailedDuringPackageVerify,
            step.reason,
        ));
    }
    Ok(base_report(
        config,
        TinyLivePackagePreflightVerdict::TinyLivePackagePreflightPlanReady,
        format!(
            "package {} is ready for a bounded read-only live-host preflight over install root {}",
            config.package_dir.display(),
            config.install_root.display()
        ),
    ))
}

fn render_live_package_preflight_script_report(config: &Config) -> Result<PackagePreflightReport> {
    let plan = plan_live_package_preflight_report(config)?;
    if plan.verdict != TinyLivePackagePreflightVerdict::TinyLivePackagePreflightPlanReady {
        return Ok(plan);
    }
    let output_path = config
        .output_path
        .as_ref()
        .ok_or_else(|| anyhow!("missing --output for --render-live-package-preflight-script"))?;
    ensure_new_output_path(output_path)?;
    let script = format!(
        "#!/usr/bin/env bash\nset -euo pipefail\nif [ \"$#\" -ne 1 ]; then\n  echo \"usage: $(basename \"$0\") <session-dir>\" >&2\n  exit 64\nfi\ncopybot_tiny_live_activation_package_preflight --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --service-status-max-staleness-ms {} --session-dir \"$1\" --run-live-package-preflight\n",
        shell_single_quote(&config.package_dir.display().to_string()),
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&config.target_service_name),
        shell_single_quote(&config.backend_command),
        config.wrapper_timeout_ms,
        config.service_status_max_staleness_ms,
    );
    write_text_file(output_path, &script)?;
    #[cfg(unix)]
    set_executable(output_path)?;
    Ok(PackagePreflightReport {
        generated_at: Utc::now(),
        mode: "render_live_package_preflight_script".to_string(),
        verdict: TinyLivePackagePreflightVerdict::TinyLivePackagePreflightScriptRendered,
        reason: format!(
            "live-host package preflight script rendered to {} without mutating the target",
            output_path.display()
        ),
        explicit_statement:
            "rendering the live-host package preflight script does not authorize or perform production activation by itself"
                .to_string(),
        ..plan
    })
}

fn run_live_package_preflight_report(config: &Config) -> Result<PackagePreflightReport> {
    run_live_package_preflight_report_with_hook(config, None)
}

fn run_live_package_preflight_report_with_hook(
    config: &Config,
    after_service_status_hook: Option<&dyn Fn(&Path) -> Result<()>>,
) -> Result<PackagePreflightReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for --run-live-package-preflight"))?;
    let package_contract = tiny_live_activation_package::verify_package_contract_for_deploy(
        &config.package_dir,
        &config.install_root,
        &config.target_service_name,
        &config.backend_command,
        config.wrapper_timeout_ms,
    )?;
    validate_live_session_dir(
        config,
        session_dir,
        &package_contract.install_target_summary.runtime_dir,
        &package_contract.install_target_summary.backup_dir,
        &package_contract.install_target_summary.session_dir,
        &package_contract.install_target_summary.wrapper_path,
        &package_contract.install_target_summary.target_config_path,
    )?;
    ensure_clean_preflight_session_dir(session_dir)?;
    let paths = package_preflight_paths(session_dir);
    let session = planned_session(config, session_dir);
    persist_json(&paths.session_path, &session)?;

    let package_verify = tiny_live_activation_package_deploy::run_mode_for_rehearsal(
        &config.package_dir,
        &config.install_root,
        &config.target_service_name,
        &config.backend_command,
        config.wrapper_timeout_ms,
        tiny_live_activation_package_deploy::RehearsalMode::VerifyPackageDeployTarget,
    )?;
    persist_json_value(
        &paths.package_verify_report_path,
        &package_verify.report_json,
    )?;
    let package_verify_step = Some(step_artifact(
        &paths.package_verify_report_path,
        "verify_package_deploy_target",
        &package_verify.verdict,
        &package_verify.reason,
        package_verify.generated_at,
    ));
    if package_verify.verdict != "tiny_live_package_deploy_verify_ok" {
        let status = PackagePreflightStatus {
            status_version: PREFLIGHT_STATUS_VERSION.to_string(),
            updated_at: Utc::now(),
            session_dir: session_dir.display().to_string(),
            result: LivePackagePreflightResult::FailedDuringPackageVerify,
            reason: package_verify.reason.clone(),
            package_verify_step: package_verify_step.clone(),
            live_target_verify_step: None,
            wrapper_verify_step: None,
            service_status_step: None,
            cutover_plan_step: None,
            explicit_statement:
                "this live-host package preflight session is read-only evidence only; it does not authorize production activation"
                    .to_string(),
        };
        persist_json(&paths.status_path, &status)?;
        return Ok(report_from_status(
            config,
            session_dir,
            TinyLivePackagePreflightVerdict::TinyLivePackagePreflightFailedDuringPackageVerify,
            &status,
            None,
            None,
            None,
            false,
        ));
    }

    let live_target_verify =
        tiny_live_activation_install_target::verify_install_target_for_rehearsal(
            &config.install_root,
            &config.target_service_name,
            &config.backend_command,
            config.wrapper_timeout_ms,
        )?;
    persist_json_value(
        &paths.live_target_verify_report_path,
        &live_target_verify.report_json,
    )?;
    let live_target_verify_step = Some(step_artifact(
        &paths.live_target_verify_report_path,
        "verify_install_target",
        &live_target_verify.verdict,
        &live_target_verify.reason,
        live_target_verify.generated_at,
    ));

    let wrapper_verify = verify_installed_wrapper_binding(
        &config.package_dir,
        &config.install_root,
        &package_contract,
        &live_target_verify,
    )?;
    persist_json(&paths.wrapper_verify_report_path, &wrapper_verify)?;
    let wrapper_verify_step = Some(step_artifact(
        &paths.wrapper_verify_report_path,
        "verify_installed_wrapper_binding",
        &wrapper_verify.verdict,
        &wrapper_verify.reason,
        wrapper_verify.generated_at,
    ));

    if live_target_verify.verdict != "tiny_live_install_target_verify_ok"
        || wrapper_verify.verdict != "tiny_live_package_preflight_wrapper_verify_ok"
    {
        let mut install_mismatches = live_target_verify.verification_mismatches.clone();
        install_mismatches.extend(wrapper_verify.verification_mismatches.clone());
        let result = if live_target_missing_on_disk(&live_target_verify) {
            LivePackagePreflightResult::InstallTargetMissing
        } else {
            LivePackagePreflightResult::InstallTargetDrifted
        };
        let verdict = match result {
            LivePackagePreflightResult::InstallTargetMissing => {
                TinyLivePackagePreflightVerdict::TinyLivePackagePreflightCompletedInstallTargetMissing
            }
            _ => {
                TinyLivePackagePreflightVerdict::TinyLivePackagePreflightCompletedInstallTargetDrifted
            }
        };
        let reason = install_mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| live_target_verify.reason.clone());
        let status = PackagePreflightStatus {
            status_version: PREFLIGHT_STATUS_VERSION.to_string(),
            updated_at: Utc::now(),
            session_dir: session_dir.display().to_string(),
            result,
            reason,
            package_verify_step: package_verify_step.clone(),
            live_target_verify_step: live_target_verify_step.clone(),
            wrapper_verify_step: wrapper_verify_step.clone(),
            service_status_step: None,
            cutover_plan_step: None,
            explicit_statement:
                "this live-host package preflight session is read-only evidence only; it does not authorize production activation"
                    .to_string(),
        };
        persist_json(&paths.status_path, &status)?;
        return Ok(report_from_status(
            config,
            session_dir,
            verdict,
            &status,
            None,
            None,
            None,
            false,
        ));
    }

    let service_status = verify_live_service_status_report(
        config,
        &package_contract.install_target_summary.wrapper_path,
        &package_contract.install_target_summary.target_config_path,
        &paths,
        after_service_status_hook,
    )?;
    persist_json(&paths.service_status_report_path, &service_status)?;
    let service_status_step = Some(step_artifact(
        &paths.service_status_report_path,
        "verify_live_service_status",
        &service_status.verdict,
        &service_status.reason,
        service_status.generated_at,
    ));
    if service_status.verdict != "tiny_live_package_preflight_service_status_verify_ok" {
        let status = PackagePreflightStatus {
            status_version: PREFLIGHT_STATUS_VERSION.to_string(),
            updated_at: Utc::now(),
            session_dir: session_dir.display().to_string(),
            result: LivePackagePreflightResult::FailedDuringServiceStatusVerify,
            reason: service_status.reason.clone(),
            package_verify_step: package_verify_step.clone(),
            live_target_verify_step: live_target_verify_step.clone(),
            wrapper_verify_step: wrapper_verify_step.clone(),
            service_status_step: service_status_step.clone(),
            cutover_plan_step: None,
            explicit_statement:
                "this live-host package preflight session is read-only evidence only; it does not authorize production activation"
                    .to_string(),
        };
        persist_json(&paths.status_path, &status)?;
        return Ok(report_from_status(
            config,
            session_dir,
            TinyLivePackagePreflightVerdict::TinyLivePackagePreflightFailedDuringServiceStatusVerify,
            &status,
            None,
            None,
            service_status.service_status_observed_at,
            false,
        ));
    }

    let cutover_plan = tiny_live_activation_package_deploy::run_mode_for_rehearsal(
        &config.package_dir,
        &config.install_root,
        &config.target_service_name,
        &config.backend_command,
        config.wrapper_timeout_ms,
        tiny_live_activation_package_deploy::RehearsalMode::PlanPackageCutover,
    )?;
    persist_json_value(&paths.cutover_plan_report_path, &cutover_plan.report_json)?;
    let cutover_plan_step = Some(step_artifact(
        &paths.cutover_plan_report_path,
        "plan_package_cutover",
        &cutover_plan.verdict,
        &cutover_plan.reason,
        cutover_plan.generated_at,
    ));
    let (result, verdict) = match cutover_plan.verdict.as_str() {
        "tiny_live_package_cutover_plan_ready" => (
            LivePackagePreflightResult::ReadyForCutoverPlanning,
            TinyLivePackagePreflightVerdict::TinyLivePackagePreflightCompletedReadyForCutoverPlanning,
        ),
        "tiny_live_package_cutover_refused_by_stage3"
        | "tiny_live_package_cutover_refused_by_pre_activation_gate" => (
            LivePackagePreflightResult::CutoverBlockedByGate,
            TinyLivePackagePreflightVerdict::TinyLivePackagePreflightCompletedCutoverBlockedByGate,
        ),
        _ => (
            LivePackagePreflightResult::FailedDuringLiveTargetVerify,
            TinyLivePackagePreflightVerdict::TinyLivePackagePreflightFailedDuringLiveTargetVerify,
        ),
    };
    let status = PackagePreflightStatus {
        status_version: PREFLIGHT_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        session_dir: session_dir.display().to_string(),
        result,
        reason: cutover_plan.reason.clone(),
        package_verify_step,
        live_target_verify_step,
        wrapper_verify_step,
        service_status_step,
        cutover_plan_step,
        explicit_statement:
            "this live-host package preflight session is read-only evidence only; it does not authorize production activation"
                .to_string(),
    };
    persist_json(&paths.status_path, &status)?;
    Ok(report_from_status(
        config,
        session_dir,
        verdict,
        &status,
        cutover_plan.current_pre_activation_gate_verdict.clone(),
        cutover_plan.current_pre_activation_gate_reason.clone(),
        service_status.service_status_observed_at,
        cutover_plan.activation_authorized,
    ))
}

fn verify_live_package_preflight_report(config: &Config) -> Result<PackagePreflightReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for --verify-live-package-preflight"))?;
    let package_contract = match tiny_live_activation_package::verify_package_contract_for_deploy(
        &config.package_dir,
        &config.install_root,
        &config.target_service_name,
        &config.backend_command,
        config.wrapper_timeout_ms,
    ) {
        Ok(value) => value,
        Err(error) => {
            let paths = package_preflight_paths(session_dir);
            return Ok(PackagePreflightReport {
                generated_at: Utc::now(),
                mode: "verify_live_package_preflight".to_string(),
                verdict: TinyLivePackagePreflightVerdict::TinyLivePackagePreflightVerifyInvalid,
                reason: format!(
                    "package does not match the requested live target contract: {error}"
                ),
                package_dir: config.package_dir.display().to_string(),
                install_root: config.install_root.display().to_string(),
                target_service_name: config.target_service_name.clone(),
                backend_command: config.backend_command.clone(),
                wrapper_timeout_ms: config.wrapper_timeout_ms,
                service_status_max_staleness_ms: config.service_status_max_staleness_ms,
                session_dir: Some(session_dir.display().to_string()),
                preflight_session_path: Some(paths.session_path.display().to_string()),
                preflight_status_path: Some(paths.status_path.display().to_string()),
                result: None,
                package_verify_step: None,
                live_target_verify_step: None,
                wrapper_verify_step: None,
                service_status_step: None,
                cutover_plan_step: None,
                package_verify_command_summary: package_verify_command_summary(config),
                verify_install_target_command_summary: verify_install_target_command_summary(config),
                verify_wrapper_binding_command_summary: verify_wrapper_binding_command_summary(config),
                verify_service_status_command_summary: verify_service_status_command_summary(config),
                package_cutover_plan_command_summary: package_cutover_plan_command_summary(config),
                current_pre_activation_gate_verdict: None,
                current_pre_activation_gate_reason: None,
                service_status_observed_at: None,
                service_status_age_ms: None,
                verification_mismatches: vec![error.to_string()],
                activation_authorized: false,
                explicit_statement:
                    "this live-host package preflight verification checks bounded read-only host evidence only; it does not authorize production activation"
                        .to_string(),
            });
        }
    };
    validate_live_session_dir(
        config,
        session_dir,
        &package_contract.install_target_summary.runtime_dir,
        &package_contract.install_target_summary.backup_dir,
        &package_contract.install_target_summary.session_dir,
        &package_contract.install_target_summary.wrapper_path,
        &package_contract.install_target_summary.target_config_path,
    )?;
    let paths = package_preflight_paths(session_dir);
    let session: PackagePreflightSession = load_json(&paths.session_path)?;
    let status: PackagePreflightStatus = load_json(&paths.status_path)?;
    let mut mismatches = Vec::new();

    compare_string(
        &session.package_dir,
        &config.package_dir.display().to_string(),
        "package preflight package_dir",
        &mut mismatches,
    );
    compare_string(
        &session.install_root,
        &config.install_root.display().to_string(),
        "package preflight install_root",
        &mut mismatches,
    );
    compare_string(
        &session.target_service_name,
        &config.target_service_name,
        "package preflight target_service_name",
        &mut mismatches,
    );
    compare_string(
        &session.backend_command,
        &config.backend_command,
        "package preflight backend_command",
        &mut mismatches,
    );
    if session.wrapper_timeout_ms != config.wrapper_timeout_ms {
        mismatches.push(format!(
            "package preflight wrapper_timeout_ms {} does not match requested {}",
            session.wrapper_timeout_ms, config.wrapper_timeout_ms
        ));
    }
    if session.service_status_max_staleness_ms != config.service_status_max_staleness_ms {
        mismatches.push(format!(
            "package preflight service_status_max_staleness_ms {} does not match requested {}",
            session.service_status_max_staleness_ms, config.service_status_max_staleness_ms
        ));
    }
    if session.session_dir != session_dir.display().to_string() {
        mismatches.push(
            "package preflight session_dir does not match the explicit session dir".to_string(),
        );
    }
    if status.session_dir != session_dir.display().to_string() {
        mismatches.push(
            "package preflight status session_dir does not match the explicit session dir"
                .to_string(),
        );
    }

    let package_verify_report = load_required_package_deploy_report(
        &status.package_verify_step,
        &paths.package_verify_report_path,
        "package_verify_step",
        &mut mismatches,
    )?;
    validate_package_deploy_report_contract(
        &package_verify_report,
        config,
        "package verify step",
        &mut mismatches,
    );

    let mut current_pre_activation_gate_verdict = None;
    let mut current_pre_activation_gate_reason = None;
    let mut service_status_observed_at = None;
    let mut activation_authorized = false;

    match status.result {
        LivePackagePreflightResult::FailedDuringPackageVerify => {
            if package_verify_report.verdict == "tiny_live_package_deploy_verify_ok" {
                mismatches.push(
                    "package verify step is green but preflight result says failed_during_package_verify"
                        .to_string(),
                );
            }
            if status.live_target_verify_step.is_some()
                || status.wrapper_verify_step.is_some()
                || status.service_status_step.is_some()
                || status.cutover_plan_step.is_some()
            {
                mismatches.push(
                    "failed_during_package_verify preflight must not contain later steps"
                        .to_string(),
                );
            }
        }
        LivePackagePreflightResult::InstallTargetMissing
        | LivePackagePreflightResult::InstallTargetDrifted => {
            let live_target_report = load_required_install_target_report(
                &status.live_target_verify_step,
                &paths.live_target_verify_report_path,
                "live_target_verify_step",
                &mut mismatches,
            )?;
            validate_install_target_report_contract(
                &live_target_report,
                config,
                "live target verify step",
                &mut mismatches,
            );
            let wrapper_report = load_required_wrapper_binding_report(
                &status.wrapper_verify_step,
                &paths.wrapper_verify_report_path,
                "wrapper_verify_step",
                &mut mismatches,
            )?;
            validate_wrapper_binding_report_contract(
                &wrapper_report,
                config,
                &package_contract,
                &live_target_report,
                &mut mismatches,
            );
            if status.service_status_step.is_some() || status.cutover_plan_step.is_some() {
                mismatches.push(
                    "install-target missing/drifted preflight must not contain service-status or cutover-plan steps"
                        .to_string(),
                );
            }
        }
        LivePackagePreflightResult::FailedDuringServiceStatusVerify => {
            let live_target_report = load_required_install_target_report(
                &status.live_target_verify_step,
                &paths.live_target_verify_report_path,
                "live_target_verify_step",
                &mut mismatches,
            )?;
            validate_install_target_report_contract(
                &live_target_report,
                config,
                "live target verify step",
                &mut mismatches,
            );
            if live_target_report.verdict != "tiny_live_install_target_verify_ok" {
                mismatches.push(format!(
                    "live target verify step verdict {} is not verify ok for a later-stage preflight result",
                    live_target_report.verdict
                ));
            }
            let wrapper_report = load_required_wrapper_binding_report(
                &status.wrapper_verify_step,
                &paths.wrapper_verify_report_path,
                "wrapper_verify_step",
                &mut mismatches,
            )?;
            validate_wrapper_binding_report_contract(
                &wrapper_report,
                config,
                &package_contract,
                &live_target_report,
                &mut mismatches,
            );
            if wrapper_report.verdict != "tiny_live_package_preflight_wrapper_verify_ok" {
                mismatches.push(format!(
                    "wrapper verify step verdict {} is not verify ok for a later-stage preflight result",
                    wrapper_report.verdict
                ));
            }
            let service_status_report = load_required_service_status_report(
                &status.service_status_step,
                &paths.service_status_report_path,
                "service_status_step",
                &mut mismatches,
            )?;
            validate_service_status_report_contract(
                &service_status_report,
                config,
                &package_contract,
                &live_target_report,
                &paths,
                &mut mismatches,
            );
            service_status_observed_at = service_status_report.service_status_observed_at;
            if service_status_report.verdict
                == "tiny_live_package_preflight_service_status_verify_ok"
            {
                mismatches.push(
                    "service status step is green but preflight result says failed_during_service_status_verify"
                        .to_string(),
                );
            }
            if status.cutover_plan_step.is_some() {
                mismatches.push(
                    "failed_during_service_status_verify preflight must not contain a cutover plan step"
                        .to_string(),
                );
            }
        }
        LivePackagePreflightResult::ReadyForCutoverPlanning
        | LivePackagePreflightResult::CutoverBlockedByGate
        | LivePackagePreflightResult::FailedDuringLiveTargetVerify => {
            let live_target_report = load_required_install_target_report(
                &status.live_target_verify_step,
                &paths.live_target_verify_report_path,
                "live_target_verify_step",
                &mut mismatches,
            )?;
            validate_install_target_report_contract(
                &live_target_report,
                config,
                "live target verify step",
                &mut mismatches,
            );
            if live_target_report.verdict != "tiny_live_install_target_verify_ok" {
                mismatches.push(format!(
                    "live target verify step verdict {} is not verify ok for a later-stage preflight result",
                    live_target_report.verdict
                ));
            }
            let wrapper_report = load_required_wrapper_binding_report(
                &status.wrapper_verify_step,
                &paths.wrapper_verify_report_path,
                "wrapper_verify_step",
                &mut mismatches,
            )?;
            validate_wrapper_binding_report_contract(
                &wrapper_report,
                config,
                &package_contract,
                &live_target_report,
                &mut mismatches,
            );
            if wrapper_report.verdict != "tiny_live_package_preflight_wrapper_verify_ok" {
                mismatches.push(format!(
                    "wrapper verify step verdict {} is not verify ok for a later-stage preflight result",
                    wrapper_report.verdict
                ));
            }
            let service_status_report = load_required_service_status_report(
                &status.service_status_step,
                &paths.service_status_report_path,
                "service_status_step",
                &mut mismatches,
            )?;
            validate_service_status_report_contract(
                &service_status_report,
                config,
                &package_contract,
                &live_target_report,
                &paths,
                &mut mismatches,
            );
            service_status_observed_at = service_status_report.service_status_observed_at;
            if service_status_report.verdict
                != "tiny_live_package_preflight_service_status_verify_ok"
            {
                mismatches.push(format!(
                    "service status step verdict {} is not verify ok for a later-stage preflight result",
                    service_status_report.verdict
                ));
            }
            let cutover_report = load_required_package_deploy_report(
                &status.cutover_plan_step,
                &paths.cutover_plan_report_path,
                "cutover_plan_step",
                &mut mismatches,
            )?;
            validate_package_deploy_report_contract(
                &cutover_report,
                config,
                "cutover plan step",
                &mut mismatches,
            );
            current_pre_activation_gate_verdict =
                cutover_report.current_pre_activation_gate_verdict.clone();
            current_pre_activation_gate_reason =
                cutover_report.current_pre_activation_gate_reason.clone();
            activation_authorized = cutover_report.activation_authorized;
            match status.result {
                LivePackagePreflightResult::ReadyForCutoverPlanning => {
                    if cutover_report.verdict != "tiny_live_package_cutover_plan_ready" {
                        mismatches.push(format!(
                            "cutover plan step verdict {} is not package cutover plan ready for a ready preflight",
                            cutover_report.verdict
                        ));
                    }
                }
                LivePackagePreflightResult::CutoverBlockedByGate => {
                    if cutover_report.verdict != "tiny_live_package_cutover_refused_by_stage3"
                        && cutover_report.verdict
                            != "tiny_live_package_cutover_refused_by_pre_activation_gate"
                    {
                        mismatches.push(format!(
                            "cutover plan step verdict {} does not reflect a gate-blocked cutover preflight",
                            cutover_report.verdict
                        ));
                    }
                }
                LivePackagePreflightResult::FailedDuringLiveTargetVerify => {
                    if cutover_report.verdict == "tiny_live_package_cutover_plan_ready" {
                        mismatches.push(
                            "cutover plan step is green but preflight result says failed_during_live_target_verify"
                                .to_string(),
                        );
                    }
                }
                _ => {}
            }
        }
    }

    let verdict = if mismatches.is_empty() {
        TinyLivePackagePreflightVerdict::TinyLivePackagePreflightVerifyOk
    } else {
        TinyLivePackagePreflightVerdict::TinyLivePackagePreflightVerifyInvalid
    };
    let reason = if mismatches.is_empty() {
        format!(
            "live-host package preflight session under {} is coherent and bound to the requested package/host contract",
            session_dir.display()
        )
    } else {
        mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "live-host package preflight verification failed".to_string())
    };

    Ok(PackagePreflightReport {
        generated_at: Utc::now(),
        mode: "verify_live_package_preflight".to_string(),
        verdict,
        reason,
        package_dir: config.package_dir.display().to_string(),
        install_root: config.install_root.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        wrapper_timeout_ms: config.wrapper_timeout_ms,
        service_status_max_staleness_ms: config.service_status_max_staleness_ms,
        session_dir: Some(session_dir.display().to_string()),
        preflight_session_path: Some(paths.session_path.display().to_string()),
        preflight_status_path: Some(paths.status_path.display().to_string()),
        result: Some(serialize_enum(&status.result)),
        package_verify_step: status.package_verify_step,
        live_target_verify_step: status.live_target_verify_step,
        wrapper_verify_step: status.wrapper_verify_step,
        service_status_step: status.service_status_step,
        cutover_plan_step: status.cutover_plan_step,
        package_verify_command_summary: package_verify_command_summary(config),
        verify_install_target_command_summary: verify_install_target_command_summary(config),
        verify_wrapper_binding_command_summary: verify_wrapper_binding_command_summary(config),
        verify_service_status_command_summary: verify_service_status_command_summary(config),
        package_cutover_plan_command_summary: package_cutover_plan_command_summary(config),
        current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason,
        service_status_observed_at,
        service_status_age_ms: service_status_observed_at.map(|ts| age_ms(ts)),
        verification_mismatches: mismatches,
        activation_authorized,
        explicit_statement:
            "this live-host package preflight verification checks bounded read-only host evidence only; it does not authorize production activation"
                .to_string(),
    })
}

fn verify_installed_wrapper_binding(
    package_dir: &Path,
    install_root: &Path,
    package_contract: &tiny_live_activation_package::VerifiedActivationPackageContract,
    install_target: &tiny_live_activation_install_target::InstallTargetVerifyStep,
) -> Result<WrapperBindingReport> {
    let wrapper_verification = match tiny_live_activation_live_execute::inspect_service_control_wrapper_contract_for_live_target(
        Path::new(&install_target.wrapper_path),
    ) {
        Ok(value) => value,
        Err(error) => tiny_live_activation_live_execute::live_service_control_wrapper_contract::WrapperVerificationSummary {
            wrapper_path: install_target.wrapper_path.clone(),
            wrapper_version: None,
            backend_command: None,
            timeout_ms: None,
            supported_actions: Vec::new(),
            status_schema_version: None,
            executable: None,
            exact_content_matches_expected: false,
            mismatches: vec![error.to_string()],
        },
    };
    let mut mismatches = wrapper_verification.mismatches.clone();
    let wrapper_bytes_match_package = compare_file_bytes(
        &package_contract.wrapper_path,
        Path::new(&install_target.wrapper_path),
    )
    .unwrap_or(false);
    if !wrapper_bytes_match_package {
        mismatches.push(format!(
            "installed wrapper {} does not match packaged wrapper {}",
            install_target.wrapper_path,
            package_contract.wrapper_path.display()
        ));
    }
    let activation_bytes_match_package = compare_file_bytes(
        &package_contract.activation_artifact_path,
        Path::new(&install_target.installed_activation_config_path),
    )
    .unwrap_or(false);
    if !activation_bytes_match_package {
        mismatches.push(format!(
            "installed activation artifact {} does not match packaged activation artifact {}",
            install_target.installed_activation_config_path,
            package_contract.activation_artifact_path.display()
        ));
    }
    let rollback_bytes_match_package = compare_file_bytes(
        &package_contract.rollback_artifact_path,
        Path::new(&install_target.installed_rollback_config_path),
    )
    .unwrap_or(false);
    if !rollback_bytes_match_package {
        mismatches.push(format!(
            "installed rollback artifact {} does not match packaged rollback artifact {}",
            install_target.installed_rollback_config_path,
            package_contract.rollback_artifact_path.display()
        ));
    }
    let verdict = if mismatches.is_empty() {
        "tiny_live_package_preflight_wrapper_verify_ok".to_string()
    } else {
        "tiny_live_package_preflight_wrapper_verify_invalid".to_string()
    };
    let reason = if mismatches.is_empty() {
        format!(
            "installed wrapper and rendered artifacts under {} still match the immutable package",
            install_root.display()
        )
    } else {
        mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "installed wrapper/package binding verification failed".to_string())
    };
    Ok(WrapperBindingReport {
        report_version: PREFLIGHT_STATUS_VERSION.to_string(),
        generated_at: Utc::now(),
        mode: "verify_installed_wrapper_binding".to_string(),
        verdict,
        reason,
        package_dir: package_dir.display().to_string(),
        install_root: install_root.display().to_string(),
        packaged_wrapper_path: package_contract.wrapper_path.display().to_string(),
        installed_wrapper_path: install_target.wrapper_path.clone(),
        packaged_activation_config_path: package_contract.activation_artifact_path.display().to_string(),
        installed_activation_config_path: install_target.installed_activation_config_path.clone(),
        packaged_rollback_config_path: package_contract.rollback_artifact_path.display().to_string(),
        installed_rollback_config_path: install_target.installed_rollback_config_path.clone(),
        wrapper_bytes_match_package,
        activation_bytes_match_package,
        rollback_bytes_match_package,
        service_control_wrapper_verification: wrapper_verification,
        verification_mismatches: mismatches,
        explicit_statement:
            "this installed-wrapper binding check is read-only and proves the live host still matches the immutable package; it does not authorize activation"
                .to_string(),
    })
}

fn verify_live_service_status_report(
    config: &Config,
    installed_wrapper_path: &str,
    target_config_path: &str,
    paths: &PackagePreflightPaths,
    after_service_status_hook: Option<&dyn Fn(&Path) -> Result<()>>,
) -> Result<ServiceStatusReport> {
    let wrapper_verification =
        tiny_live_activation_live_execute::inspect_service_control_wrapper_contract_for_live_target(
            Path::new(installed_wrapper_path),
        )
        .map_err(|error| anyhow!(error))?;
    if paths.service_status_artifact_path.exists() {
        fs::remove_file(&paths.service_status_artifact_path).ok();
    }
    fs::create_dir_all(&paths.service_status_runtime_dir).with_context(|| {
        format!(
            "failed creating service-status scratch runtime dir {}",
            paths.service_status_runtime_dir.display()
        )
    })?;
    let current_target = copybot_config::load_from_path(Path::new(target_config_path))
        .with_context(|| {
            format!(
                "failed loading target config {} for live-host package preflight",
                target_config_path
            )
        })?;
    let current_fingerprint =
        tiny_live_activation_live_execute::activation_decision_packet::build_config_fingerprint(
            tiny_live_activation_live_execute::tiny_live_activation_execute::FINGERPRINT_SCOPE,
            &current_target,
        )?;
    let command_exit_success = run_service_status_command(
        Path::new(installed_wrapper_path),
        &config.target_service_name,
        Path::new(target_config_path),
        &paths.service_status_runtime_dir,
        &paths.service_status_artifact_path,
        &current_fingerprint.sha256,
        current_target.execution.enabled,
    )?;
    if let Some(hook) = after_service_status_hook {
        hook(&paths.service_status_artifact_path)?;
    }
    let status = tiny_live_activation_live_execute::load_live_service_status(
        &paths.service_status_artifact_path,
    )?;
    let mut mismatches = tiny_live_activation_live_execute::verify_live_service_status_for_watch(
        &status,
        "status",
        Path::new(target_config_path),
        &config.target_service_name,
        &current_fingerprint.sha256,
        current_target.execution.enabled,
    );
    let observed_age_ms = age_ms(status.observed_at);
    if observed_age_ms < 0 {
        mismatches.push(format!(
            "service status artifact observed_at {} is in the future",
            status.observed_at
        ));
    }
    if observed_age_ms as u64 > config.service_status_max_staleness_ms {
        mismatches.push(format!(
            "service status artifact is stale: age {}ms exceeds max {}ms",
            observed_age_ms, config.service_status_max_staleness_ms
        ));
    }
    if status.target_config_path != target_config_path {
        mismatches.push(
            "service status target_config_path does not match the deterministic install contract"
                .to_string(),
        );
    }
    if status.service_name != config.target_service_name {
        mismatches.push(
            "service status service_name does not match the explicit target service".to_string(),
        );
    }
    if status.observed_config_fingerprint_sha256 != current_fingerprint.sha256 {
        mismatches.push(
            "service status observed fingerprint does not match the current target fingerprint"
                .to_string(),
        );
    }
    if status.observed_execution_enabled != current_target.execution.enabled {
        mismatches.push(
            "service status observed execution posture does not match the current target config"
                .to_string(),
        );
    }
    if !command_exit_success {
        mismatches.push("service control wrapper status command exited non-zero".to_string());
    }
    let verdict = if mismatches.is_empty() {
        "tiny_live_package_preflight_service_status_verify_ok".to_string()
    } else {
        "tiny_live_package_preflight_service_status_verify_invalid".to_string()
    };
    let reason = if mismatches.is_empty() {
        format!(
            "installed wrapper {} reported fresh bounded service status for {}",
            installed_wrapper_path, config.target_service_name
        )
    } else {
        mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "service status verification failed".to_string())
    };
    Ok(ServiceStatusReport {
        report_version: SERVICE_STATUS_REPORT_VERSION.to_string(),
        generated_at: Utc::now(),
        mode: "verify_live_service_status".to_string(),
        verdict,
        reason,
        package_dir: config.package_dir.display().to_string(),
        install_root: config.install_root.display().to_string(),
        installed_wrapper_path: installed_wrapper_path.to_string(),
        target_config_path: target_config_path.to_string(),
        target_service_name: config.target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        scratch_runtime_dir: paths.service_status_runtime_dir.display().to_string(),
        service_status_path: paths.service_status_artifact_path.display().to_string(),
        service_status_observed_at: Some(status.observed_at),
        service_status_age_ms: Some(observed_age_ms),
        max_service_status_staleness_ms: config.service_status_max_staleness_ms,
        expected_config_fingerprint_sha256: current_fingerprint.sha256.clone(),
        current_target_config_fingerprint_sha256: current_fingerprint.sha256.clone(),
        expected_execution_enabled: current_target.execution.enabled,
        current_target_execution_enabled: current_target.execution.enabled,
        command_exit_success,
        service_control_wrapper_verification: wrapper_verification,
        verification_mismatches: mismatches,
        explicit_statement:
            "this live service-status preflight step is a bounded read-only probe; it does not restart the service or authorize activation"
                .to_string(),
    })
}

fn run_service_status_command(
    wrapper_path: &Path,
    target_service_name: &str,
    target_config_path: &Path,
    runtime_dir: &Path,
    status_path: &Path,
    expected_fingerprint_sha256: &str,
    expected_execution_enabled: bool,
) -> Result<bool> {
    let log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(
            status_path
                .parent()
                .unwrap()
                .join("tiny_live_activation_package_preflight.service_status.log"),
        )
        .with_context(|| {
            format!(
                "failed opening live package preflight service-status log under {}",
                status_path.parent().unwrap().display()
            )
        })?;
    let stderr = log
        .try_clone()
        .with_context(|| "failed cloning service-status log handle")?;
    let mut child = Command::new(wrapper_path)
        .arg("--action")
        .arg("status")
        .arg("--service-name")
        .arg(target_service_name)
        .arg("--target-config")
        .arg(target_config_path)
        .arg("--runtime-dir")
        .arg(runtime_dir)
        .arg("--status-path")
        .arg(status_path)
        .arg("--expected-config-fingerprint")
        .arg(expected_fingerprint_sha256)
        .arg("--expected-execution-enabled")
        .arg(expected_execution_enabled.to_string())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(stderr))
        .spawn()
        .with_context(|| {
            format!(
                "failed spawning installed wrapper {} for bounded status action",
                wrapper_path.display()
            )
        })?;
    wait_for_child(&mut child, DEFAULT_STATUS_COMMAND_TIMEOUT_MS)
}

fn wait_for_child(child: &mut Child, timeout_ms: u64) -> Result<bool> {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status.success());
        }
        if Instant::now() >= deadline {
            child.kill().ok();
            child.wait().ok();
            return Ok(false);
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn base_report(
    config: &Config,
    verdict: TinyLivePackagePreflightVerdict,
    reason: String,
) -> PackagePreflightReport {
    PackagePreflightReport {
        generated_at: Utc::now(),
        mode: mode_name(config.mode).to_string(),
        verdict,
        reason,
        package_dir: config.package_dir.display().to_string(),
        install_root: config.install_root.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        wrapper_timeout_ms: config.wrapper_timeout_ms,
        service_status_max_staleness_ms: config.service_status_max_staleness_ms,
        session_dir: config
            .session_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        preflight_session_path: config
            .session_dir
            .as_ref()
            .map(|path| package_preflight_paths(path).session_path.display().to_string()),
        preflight_status_path: config
            .session_dir
            .as_ref()
            .map(|path| package_preflight_paths(path).status_path.display().to_string()),
        result: None,
        package_verify_step: None,
        live_target_verify_step: None,
        wrapper_verify_step: None,
        service_status_step: None,
        cutover_plan_step: None,
        package_verify_command_summary: package_verify_command_summary(config),
        verify_install_target_command_summary: verify_install_target_command_summary(config),
        verify_wrapper_binding_command_summary: verify_wrapper_binding_command_summary(config),
        verify_service_status_command_summary: verify_service_status_command_summary(config),
        package_cutover_plan_command_summary: package_cutover_plan_command_summary(config),
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        service_status_observed_at: None,
        service_status_age_ms: None,
        verification_mismatches: Vec::new(),
        activation_authorized: false,
        explicit_statement:
            "this live-host package preflight layer is read-only host evidence only; it does not authorize or perform production activation by itself"
                .to_string(),
    }
}

fn report_from_status(
    config: &Config,
    session_dir: &Path,
    verdict: TinyLivePackagePreflightVerdict,
    status: &PackagePreflightStatus,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    service_status_observed_at: Option<DateTime<Utc>>,
    activation_authorized: bool,
) -> PackagePreflightReport {
    let paths = package_preflight_paths(session_dir);
    PackagePreflightReport {
        generated_at: Utc::now(),
        mode: "run_live_package_preflight".to_string(),
        verdict,
        reason: status.reason.clone(),
        package_dir: config.package_dir.display().to_string(),
        install_root: config.install_root.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        wrapper_timeout_ms: config.wrapper_timeout_ms,
        service_status_max_staleness_ms: config.service_status_max_staleness_ms,
        session_dir: Some(session_dir.display().to_string()),
        preflight_session_path: Some(paths.session_path.display().to_string()),
        preflight_status_path: Some(paths.status_path.display().to_string()),
        result: Some(serialize_enum(&status.result)),
        package_verify_step: status.package_verify_step.clone(),
        live_target_verify_step: status.live_target_verify_step.clone(),
        wrapper_verify_step: status.wrapper_verify_step.clone(),
        service_status_step: status.service_status_step.clone(),
        cutover_plan_step: status.cutover_plan_step.clone(),
        package_verify_command_summary: package_verify_command_summary(config),
        verify_install_target_command_summary: verify_install_target_command_summary(config),
        verify_wrapper_binding_command_summary: verify_wrapper_binding_command_summary(config),
        verify_service_status_command_summary: verify_service_status_command_summary(config),
        package_cutover_plan_command_summary: package_cutover_plan_command_summary(config),
        current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason,
        service_status_observed_at,
        service_status_age_ms: service_status_observed_at.map(|ts| age_ms(ts)),
        verification_mismatches: Vec::new(),
        activation_authorized,
        explicit_statement:
            "this live-host package preflight session is read-only evidence only; it does not authorize production activation"
                .to_string(),
    }
}

fn mode_name(mode: Mode) -> &'static str {
    match mode {
        Mode::PlanLivePackagePreflight => "plan_live_package_preflight",
        Mode::RenderLivePackagePreflightScript => "render_live_package_preflight_script",
        Mode::RunLivePackagePreflight => "run_live_package_preflight",
        Mode::VerifyLivePackagePreflight => "verify_live_package_preflight",
    }
}

fn package_verify_command_summary(config: &Config) -> String {
    format!(
        "copybot_tiny_live_activation_package_deploy --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --verify-package-deploy-target",
        shell_single_quote(&config.package_dir.display().to_string()),
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&config.target_service_name),
        shell_single_quote(&config.backend_command),
        config.wrapper_timeout_ms,
    )
}

fn verify_install_target_command_summary(config: &Config) -> String {
    format!(
        "copybot_tiny_live_activation_install_target --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --verify-install-target",
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&config.target_service_name),
        shell_single_quote(&config.backend_command),
        config.wrapper_timeout_ms,
    )
}

fn verify_wrapper_binding_command_summary(config: &Config) -> String {
    format!(
        "copybot_live_service_control_wrapper --path <installed-wrapper-under-{}> --verify-wrapper && compare packaged wrapper/artifact bytes against installed paths under {}",
        config.install_root.display(),
        config.install_root.display()
    )
}

fn verify_service_status_command_summary(config: &Config) -> String {
    format!(
        "<installed-wrapper-under-{}> --action status --service-name {} --target-config <installed-target-config> --runtime-dir <session-scratch-runtime> --status-path <session-status-artifact> --expected-config-fingerprint <current-target-fingerprint> --expected-execution-enabled false",
        config.install_root.display(),
        shell_single_quote(&config.target_service_name),
    )
}

fn package_cutover_plan_command_summary(config: &Config) -> String {
    format!(
        "copybot_tiny_live_activation_package_deploy --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --plan-package-cutover",
        shell_single_quote(&config.package_dir.display().to_string()),
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&config.target_service_name),
        shell_single_quote(&config.backend_command),
        config.wrapper_timeout_ms,
    )
}

fn planned_session(config: &Config, session_dir: &Path) -> PackagePreflightSession {
    PackagePreflightSession {
        session_version: PREFLIGHT_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        package_dir: config.package_dir.display().to_string(),
        install_root: config.install_root.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        wrapper_timeout_ms: config.wrapper_timeout_ms,
        service_status_max_staleness_ms: config.service_status_max_staleness_ms,
        session_dir: session_dir.display().to_string(),
        package_verify_command_summary: package_verify_command_summary(config),
        verify_install_target_command_summary: verify_install_target_command_summary(config),
        verify_wrapper_binding_command_summary: verify_wrapper_binding_command_summary(config),
        verify_service_status_command_summary: verify_service_status_command_summary(config),
        package_cutover_plan_command_summary: package_cutover_plan_command_summary(config),
        explicit_statement:
            "this live-host package preflight session binds one immutable package plus one explicit live host contract; it does not authorize production activation"
                .to_string(),
    }
}

fn step_artifact(
    path: &Path,
    mode: &str,
    verdict: &str,
    reason: &str,
    generated_at: DateTime<Utc>,
) -> PackagePreflightStepArtifact {
    PackagePreflightStepArtifact {
        path: path.display().to_string(),
        mode: mode.to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        generated_at,
    }
}

fn package_preflight_paths(session_dir: &Path) -> PackagePreflightPaths {
    PackagePreflightPaths {
        session_path: session_dir.join("tiny_live_activation_package_preflight.session.json"),
        status_path: session_dir.join("tiny_live_activation_package_preflight.status.json"),
        package_verify_report_path: session_dir
            .join("tiny_live_activation_package_preflight.package_verify.report.json"),
        live_target_verify_report_path: session_dir
            .join("tiny_live_activation_package_preflight.live_target_verify.report.json"),
        wrapper_verify_report_path: session_dir
            .join("tiny_live_activation_package_preflight.wrapper_verify.report.json"),
        service_status_report_path: session_dir
            .join("tiny_live_activation_package_preflight.service_status.report.json"),
        service_status_artifact_path: session_dir
            .join("tiny_live_activation_package_preflight.service_status.artifact.json"),
        service_status_runtime_dir: session_dir
            .join("tiny_live_activation_package_preflight.service_status.runtime"),
        service_status_log_path: session_dir
            .join("tiny_live_activation_package_preflight.service_status.log"),
        cutover_plan_report_path: session_dir
            .join("tiny_live_activation_package_preflight.cutover_plan.report.json"),
    }
}

fn ensure_clean_preflight_session_dir(session_dir: &Path) -> Result<()> {
    let paths = package_preflight_paths(session_dir);
    for artifact in [
        &paths.session_path,
        &paths.status_path,
        &paths.package_verify_report_path,
        &paths.live_target_verify_report_path,
        &paths.wrapper_verify_report_path,
        &paths.service_status_report_path,
        &paths.service_status_artifact_path,
        &paths.service_status_log_path,
        &paths.cutover_plan_report_path,
    ] {
        if artifact.exists() {
            bail!(
                "refusing to reuse live package preflight session dir {}; artifact {} already exists",
                session_dir.display(),
                artifact.display()
            );
        }
    }
    if paths.service_status_runtime_dir.exists() {
        bail!(
            "refusing to reuse live package preflight session dir {}; scratch runtime {} already exists",
            session_dir.display(),
            paths.service_status_runtime_dir.display()
        );
    }
    fs::create_dir_all(session_dir)
        .with_context(|| format!("failed creating session dir {}", session_dir.display()))
}

fn validate_live_session_dir(
    config: &Config,
    session_dir: &Path,
    runtime_dir: &str,
    backup_dir: &str,
    managed_session_dir: &str,
    wrapper_path: &str,
    target_config_path: &str,
) -> Result<()> {
    if !config.install_root.is_absolute() {
        bail!("install root must be absolute");
    }
    if !session_dir.is_absolute() {
        bail!("session dir must be absolute");
    }
    let install_root_anchor = find_existing_anchor(&config.install_root, "install root")?;
    reject_anchor_symlink(&install_root_anchor, "install root")?;
    let resolved_install_root = if config.install_root.exists() {
        fs::canonicalize(&config.install_root).with_context(|| {
            format!(
                "failed canonicalizing install root {}",
                config.install_root.display()
            )
        })?
    } else {
        fs::canonicalize(&install_root_anchor).with_context(|| {
            format!(
                "failed canonicalizing install root anchor {}",
                install_root_anchor.display()
            )
        })?
    };
    let session_anchor = find_existing_anchor(session_dir, "session dir")?;
    reject_anchor_symlink(&session_anchor, "session dir")?;
    reject_descendant_symlinks(session_dir, &session_anchor, "session dir")?;
    let resolved_session_anchor = fs::canonicalize(&session_anchor).with_context(|| {
        format!(
            "failed canonicalizing session dir anchor {}",
            session_anchor.display()
        )
    })?;
    if session_dir.exists() {
        let metadata = fs::symlink_metadata(session_dir).with_context(|| {
            format!(
                "failed reading session dir metadata {}",
                session_dir.display()
            )
        })?;
        if metadata.file_type().is_symlink() {
            bail!(
                "session dir {} must not be a symlink",
                session_dir.display()
            );
        }
    }
    if session_dir.starts_with(&config.install_root)
        || resolved_session_anchor.starts_with(&resolved_install_root)
    {
        bail!(
            "session dir {} must stay outside install root {} because live-host package preflight is read-only with respect to the live install root",
            session_dir.display(),
            config.install_root.display()
        );
    }
    for forbidden_root in [
        Path::new(runtime_dir),
        Path::new(backup_dir),
        Path::new(managed_session_dir),
        Path::new(wrapper_path)
            .parent()
            .ok_or_else(|| anyhow!("installed wrapper path has no parent"))?,
        Path::new(target_config_path)
            .parent()
            .ok_or_else(|| anyhow!("installed target config path has no parent"))?,
    ] {
        if session_dir.starts_with(forbidden_root) {
            bail!(
                "session dir {} must stay outside managed live-target path {}",
                session_dir.display(),
                forbidden_root.display()
            );
        }
    }
    Ok(())
}

fn reject_anchor_symlink(anchor: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(anchor).with_context(|| {
        format!(
            "failed reading metadata for {label} anchor {}",
            anchor.display()
        )
    })?;
    if metadata.file_type().is_symlink() {
        bail!("{label} anchor {} must not be a symlink", anchor.display());
    }
    Ok(())
}

fn find_existing_anchor(path: &Path, label: &str) -> Result<PathBuf> {
    let mut current = path.to_path_buf();
    loop {
        if current.exists() {
            return Ok(current);
        }
        if !current.pop() {
            bail!("{label} {} has no existing parent anchor", path.display());
        }
    }
}

fn reject_descendant_symlinks(path: &Path, anchor: &Path, label: &str) -> Result<()> {
    let relative = path
        .strip_prefix(anchor)
        .with_context(|| format!("failed stripping {label} anchor {}", anchor.display()))?;
    let mut current = anchor.to_path_buf();
    for component in relative.components() {
        current.push(component.as_os_str());
        if current.exists() {
            let metadata = fs::symlink_metadata(&current)
                .with_context(|| format!("failed reading metadata for {}", current.display()))?;
            if metadata.file_type().is_symlink() {
                bail!(
                    "{label} {} must not traverse symlinked path component {}",
                    path.display(),
                    current.display()
                );
            }
        }
    }
    Ok(())
}

fn load_required_package_deploy_report(
    step: &Option<PackagePreflightStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<PackageDeployReportView> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("missing required preflight step {label}"))?;
    load_bound_step_json(step, expected_path, label, mismatches)
}

fn load_required_install_target_report(
    step: &Option<PackagePreflightStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<InstallTargetReportView> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("missing required preflight step {label}"))?;
    load_bound_step_json(step, expected_path, label, mismatches)
}

fn load_required_wrapper_binding_report(
    step: &Option<PackagePreflightStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<WrapperBindingReport> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("missing required preflight step {label}"))?;
    load_bound_step_json(step, expected_path, label, mismatches)
}

fn load_required_service_status_report(
    step: &Option<PackagePreflightStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<ServiceStatusReport> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("missing required preflight step {label}"))?;
    load_bound_step_json(step, expected_path, label, mismatches)
}

fn load_bound_step_json<T: for<'de> Deserialize<'de>>(
    step: &PackagePreflightStepArtifact,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<T> {
    let expected = expected_path.display().to_string();
    if step.path != expected {
        mismatches.push(format!(
            "live package preflight {label} path {} does not match deterministic session artifact {}",
            step.path, expected
        ));
    }
    load_json(expected_path).with_context(|| {
        format!(
            "failed reading deterministic live package preflight {label} report {}",
            expected_path.display()
        )
    })
}

fn validate_package_deploy_report_contract(
    report: &PackageDeployReportView,
    config: &Config,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &report.package_dir,
        &config.package_dir.display().to_string(),
        &format!("{label} package_dir"),
        mismatches,
    );
    compare_string(
        &report.install_root,
        &config.install_root.display().to_string(),
        &format!("{label} install_root"),
        mismatches,
    );
    compare_string(
        &report.target_service_name,
        &config.target_service_name,
        &format!("{label} target_service_name"),
        mismatches,
    );
    compare_string(
        &report.backend_command,
        &config.backend_command,
        &format!("{label} backend_command"),
        mismatches,
    );
    if report.wrapper_timeout_ms != config.wrapper_timeout_ms {
        mismatches.push(format!(
            "{label} wrapper_timeout_ms {} does not match requested {}",
            report.wrapper_timeout_ms, config.wrapper_timeout_ms
        ));
    }
}

fn validate_install_target_report_contract(
    report: &InstallTargetReportView,
    config: &Config,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &report.install_root,
        &config.install_root.display().to_string(),
        &format!("{label} install_root"),
        mismatches,
    );
    compare_string(
        &report.target_service_name,
        &config.target_service_name,
        &format!("{label} target_service_name"),
        mismatches,
    );
    compare_string(
        &report.backend_command,
        &config.backend_command,
        &format!("{label} backend_command"),
        mismatches,
    );
    if report.wrapper_timeout_ms != config.wrapper_timeout_ms {
        mismatches.push(format!(
            "{label} wrapper_timeout_ms {} does not match requested {}",
            report.wrapper_timeout_ms, config.wrapper_timeout_ms
        ));
    }
}

fn validate_wrapper_binding_report_contract(
    report: &WrapperBindingReport,
    config: &Config,
    package_contract: &tiny_live_activation_package::VerifiedActivationPackageContract,
    live_target_report: &InstallTargetReportView,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &report.package_dir,
        &config.package_dir.display().to_string(),
        "wrapper verify package_dir",
        mismatches,
    );
    compare_string(
        &report.install_root,
        &config.install_root.display().to_string(),
        "wrapper verify install_root",
        mismatches,
    );
    compare_string(
        &report.packaged_wrapper_path,
        &package_contract.wrapper_path.display().to_string(),
        "wrapper verify packaged_wrapper_path",
        mismatches,
    );
    compare_string(
        &report.installed_wrapper_path,
        &live_target_report.wrapper_path,
        "wrapper verify installed_wrapper_path",
        mismatches,
    );
    compare_string(
        &report.packaged_activation_config_path,
        &package_contract
            .activation_artifact_path
            .display()
            .to_string(),
        "wrapper verify packaged_activation_config_path",
        mismatches,
    );
    compare_string(
        &report.installed_activation_config_path,
        &live_target_report.installed_activation_config_path,
        "wrapper verify installed_activation_config_path",
        mismatches,
    );
    compare_string(
        &report.packaged_rollback_config_path,
        &package_contract
            .rollback_artifact_path
            .display()
            .to_string(),
        "wrapper verify packaged_rollback_config_path",
        mismatches,
    );
    compare_string(
        &report.installed_rollback_config_path,
        &live_target_report.installed_rollback_config_path,
        "wrapper verify installed_rollback_config_path",
        mismatches,
    );
    if !report.wrapper_bytes_match_package {
        mismatches
            .push("wrapper verify report says wrapper bytes do not match package".to_string());
    }
    if !report.activation_bytes_match_package {
        mismatches.push(
            "wrapper verify report says activation artifact bytes do not match package".to_string(),
        );
    }
    if !report.rollback_bytes_match_package {
        mismatches.push(
            "wrapper verify report says rollback artifact bytes do not match package".to_string(),
        );
    }
    if report.verdict == "tiny_live_package_preflight_wrapper_verify_ok"
        && !report.verification_mismatches.is_empty()
    {
        mismatches.push(
            "wrapper verify report claims ok but still carries verification mismatches".to_string(),
        );
    }
}

fn validate_service_status_report_contract(
    report: &ServiceStatusReport,
    config: &Config,
    package_contract: &tiny_live_activation_package::VerifiedActivationPackageContract,
    live_target_report: &InstallTargetReportView,
    paths: &PackagePreflightPaths,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &report.package_dir,
        &config.package_dir.display().to_string(),
        "service status package_dir",
        mismatches,
    );
    compare_string(
        &report.install_root,
        &config.install_root.display().to_string(),
        "service status install_root",
        mismatches,
    );
    compare_string(
        &report.installed_wrapper_path,
        &live_target_report.wrapper_path,
        "service status installed_wrapper_path",
        mismatches,
    );
    compare_string(
        &report.target_config_path,
        &live_target_report.target_config_path,
        "service status target_config_path",
        mismatches,
    );
    compare_string(
        &report.target_service_name,
        &config.target_service_name,
        "service status target_service_name",
        mismatches,
    );
    compare_string(
        &report.backend_command,
        &config.backend_command,
        "service status backend_command",
        mismatches,
    );
    compare_string(
        &report.service_status_path,
        &paths.service_status_artifact_path.display().to_string(),
        "service status artifact path",
        mismatches,
    );
    compare_string(
        &report.scratch_runtime_dir,
        &paths.service_status_runtime_dir.display().to_string(),
        "service status scratch runtime dir",
        mismatches,
    );
    if Path::new(&report.scratch_runtime_dir).starts_with(&config.install_root) {
        mismatches.push(
            "service status scratch runtime dir must stay outside the managed live install root"
                .to_string(),
        );
    }
    let raw_status: tiny_live_activation_live_execute::LiveServiceStatus =
        match load_json(&paths.service_status_artifact_path) {
            Ok(value) => value,
            Err(error) => {
                mismatches.push(format!(
                    "failed reading deterministic raw service status artifact {}: {error}",
                    paths.service_status_artifact_path.display()
                ));
                return;
            }
        };
    if report.service_status_observed_at != Some(raw_status.observed_at) {
        mismatches.push(
            "service status report observed_at does not match the deterministic raw service status artifact"
                .to_string(),
        );
    }
    let current_age_ms = age_ms(raw_status.observed_at);
    if current_age_ms > config.service_status_max_staleness_ms as i64 {
        mismatches.push(format!(
            "service status report is now stale: age_ms={} exceeds max_service_status_staleness_ms={}",
            current_age_ms, config.service_status_max_staleness_ms
        ));
    }
    if report.max_service_status_staleness_ms != config.service_status_max_staleness_ms {
        mismatches.push(format!(
            "service status report max_service_status_staleness_ms {} does not match requested {}",
            report.max_service_status_staleness_ms, config.service_status_max_staleness_ms
        ));
    }
    if report.expected_config_fingerprint_sha256 != report.current_target_config_fingerprint_sha256
    {
        mismatches.push(
            "service status report expected fingerprint does not match current target fingerprint"
                .to_string(),
        );
    }
    let installed_activation =
        tiny_live_activation_live_execute::tiny_live_activation_execute::inspect_rendered_config_artifact(
            Path::new(&live_target_report.installed_activation_config_path),
        )
        .map_err(|error| anyhow!(error))
        .unwrap();
    if report.expected_config_fingerprint_sha256
        != installed_activation
            .metadata
            .source_config_fingerprint_sha256
    {
        mismatches.push(
            "service status report expected fingerprint does not match the installed activation artifact source fingerprint"
                .to_string(),
        );
    }
    if report
        .service_control_wrapper_verification
        .mismatches
        .is_empty()
        && report.verdict != "tiny_live_package_preflight_service_status_verify_ok"
        && report.verification_mismatches.is_empty()
    {
        mismatches.push(
            "service status report is non-green without explicit verification mismatches"
                .to_string(),
        );
    }
    let _ = package_contract;
}

fn live_target_missing_on_disk(
    live_target_verify: &tiny_live_activation_install_target::InstallTargetVerifyStep,
) -> bool {
    [
        live_target_verify.wrapper_path.as_str(),
        live_target_verify.target_config_path.as_str(),
        live_target_verify.installed_activation_config_path.as_str(),
        live_target_verify.installed_rollback_config_path.as_str(),
        live_target_verify.runtime_dir.as_str(),
        live_target_verify.backup_dir.as_str(),
        live_target_verify.session_dir.as_str(),
    ]
    .iter()
    .any(|path| !Path::new(path).exists())
}

fn compare_file_bytes(left: &Path, right: &Path) -> Result<bool> {
    let left_bytes =
        fs::read(left).with_context(|| format!("failed reading left file {}", left.display()))?;
    let right_bytes = fs::read(right)
        .with_context(|| format!("failed reading right file {}", right.display()))?;
    Ok(left_bytes == right_bytes)
}

fn compare_string(actual: &str, expected: &str, label: &str, mismatches: &mut Vec<String>) {
    if actual != expected {
        mismatches.push(format!(
            "{label} {actual} does not match expected {expected}"
        ));
    }
}

fn age_ms(observed_at: DateTime<Utc>) -> i64 {
    Utc::now()
        .signed_duration_since(observed_at)
        .num_milliseconds()
}

fn load_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed reading json {}", path.display()))?;
    serde_json::from_str(&raw).with_context(|| format!("failed parsing json {}", path.display()))
}

fn persist_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let raw = serde_json::to_string_pretty(value)?;
    write_text_file(path, &raw)
}

fn persist_json_value(path: &Path, value: &serde_json::Value) -> Result<()> {
    let raw = serde_json::to_string_pretty(value)?;
    write_text_file(path, &raw)
}

fn ensure_new_output_path(path: &Path) -> Result<()> {
    if !path.is_absolute() {
        bail!("output path must be absolute");
    }
    if path.exists() {
        bail!("refusing to overwrite existing output {}", path.display());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed creating output parent {}", parent.display()))?;
    }
    Ok(())
}

fn write_text_file(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed creating parent dir {}", parent.display()))?;
    }
    fs::write(path, contents).with_context(|| format!("failed writing {}", path.display()))
}

#[cfg(unix)]
fn set_executable(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = fs::metadata(path)
        .with_context(|| format!("failed reading metadata for {}", path.display()))?
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions)
        .with_context(|| format!("failed setting executable bit for {}", path.display()))
}

fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn render_human_report(report: &PackagePreflightReport) -> String {
    let mut lines = vec![
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("package_dir={}", report.package_dir),
        format!("install_root={}", report.install_root),
        format!("target_service_name={}", report.target_service_name),
        format!("backend_command={}", report.backend_command),
        format!("wrapper_timeout_ms={}", report.wrapper_timeout_ms),
        format!(
            "service_status_max_staleness_ms={}",
            report.service_status_max_staleness_ms
        ),
    ];
    if let Some(value) = &report.session_dir {
        lines.push(format!("session_dir={value}"));
    }
    if let Some(value) = &report.result {
        lines.push(format!("result={value}"));
    }
    if let Some(value) = &report.current_pre_activation_gate_verdict {
        lines.push(format!("current_pre_activation_gate_verdict={value}"));
    }
    if let Some(value) = &report.current_pre_activation_gate_reason {
        lines.push(format!("current_pre_activation_gate_reason={value}"));
    }
    if let Some(value) = &report.service_status_observed_at {
        lines.push(format!("service_status_observed_at={value}"));
    }
    if let Some(value) = report.service_status_age_ms {
        lines.push(format!("service_status_age_ms={value}"));
    }
    if !report.verification_mismatches.is_empty() {
        lines.push("verification_mismatches:".to_string());
        lines.extend(
            report
                .verification_mismatches
                .iter()
                .map(|item| format!("  {item}")),
        );
    }
    lines.push(format!(
        "activation_authorized={}",
        report.activation_authorized
    ));
    lines.push(format!("explicit_statement={}", report.explicit_statement));
    lines.join("\n")
}

fn serialize_enum<T: Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_else(|| "<unknown>".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use copybot_config::load_from_path;
    use copybot_storage::{
        DiscoveryWalletFreshnessCaptureWrite, ExecutionDryRunRehearsalWrite, SqliteStore,
    };
    use sha2::Digest;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::thread;

    #[test]
    fn coherent_package_and_matching_fake_live_target_yield_green_preflight() {
        let fixture = preflight_fixture(
            "tiny_live_package_preflight_green",
            GateState::Green,
            true,
            true,
        );
        let report = run_live_package_preflight_report(&fixture.config).expect("run preflight");
        assert_eq!(
            report.verdict,
            TinyLivePackagePreflightVerdict::TinyLivePackagePreflightCompletedReadyForCutoverPlanning
        );
        assert_eq!(report.result.as_deref(), Some("ready_for_cutover_planning"));
    }

    #[test]
    fn session_dir_under_install_root_is_rejected_without_writing_preflight_artifacts() {
        let fixture = preflight_fixture(
            "tiny_live_package_preflight_session_inside_root",
            GateState::Green,
            true,
            true,
        );
        let session_dir = fixture
            .install_root
            .join("var/lib/solana-copy-bot/tiny-live/preflight-session");
        let config = Config {
            session_dir: Some(session_dir.clone()),
            ..fixture.config.clone()
        };
        let error = run_live_package_preflight_report(&config).expect_err("session dir rejected");
        assert!(
            error.to_string().contains("must stay outside install root"),
            "unexpected error: {error:#}"
        );
        let paths = package_preflight_paths(&session_dir);
        assert!(!paths.session_path.exists());
        assert!(!paths.status_path.exists());
        assert!(!paths.package_verify_report_path.exists());
        assert!(!paths.live_target_verify_report_path.exists());
        assert!(!paths.wrapper_verify_report_path.exists());
        assert!(!paths.service_status_report_path.exists());
        assert!(!paths.service_status_artifact_path.exists());
        assert!(!paths.service_status_runtime_dir.exists());
        assert!(!paths.service_status_log_path.exists());
        assert!(!paths.cutover_plan_report_path.exists());
    }

    #[test]
    fn missing_installed_wrapper_yields_explicit_non_green_preflight() {
        let fixture = preflight_fixture(
            "tiny_live_package_preflight_missing_wrapper",
            GateState::Green,
            true,
            true,
        );
        fs::remove_file(
            fixture
                .install_root
                .join("usr/local/bin/copybot-live-service-control"),
        )
        .unwrap();
        let report = run_live_package_preflight_report(&fixture.config).expect("run preflight");
        assert_eq!(
            report.verdict,
            TinyLivePackagePreflightVerdict::TinyLivePackagePreflightCompletedInstallTargetMissing
        );
    }

    #[test]
    fn installed_wrapper_tampered_vs_package_contract_yields_explicit_non_green_preflight() {
        let fixture = preflight_fixture(
            "tiny_live_package_preflight_tampered_wrapper",
            GateState::Green,
            true,
            true,
        );
        fs::write(
            fixture
                .install_root
                .join("usr/local/bin/copybot-live-service-control"),
            "#!/usr/bin/env bash\necho tampered\n",
        )
        .unwrap();
        let report = run_live_package_preflight_report(&fixture.config).expect("run preflight");
        assert_eq!(
            report.verdict,
            TinyLivePackagePreflightVerdict::TinyLivePackagePreflightCompletedInstallTargetDrifted
        );
    }

    #[test]
    fn target_config_fingerprint_drift_yields_explicit_non_green_preflight() {
        let fixture = preflight_fixture(
            "tiny_live_package_preflight_target_drift",
            GateState::Green,
            true,
            true,
        );
        let drifted = fs::read_to_string(
            fixture
                .install_root
                .join("etc/solana-copy-bot/live.server.toml"),
        )
        .unwrap()
        .replace("copy_notional_sol = 0.05", "copy_notional_sol = 0.04");
        fs::write(
            fixture
                .install_root
                .join("etc/solana-copy-bot/live.server.toml"),
            drifted,
        )
        .unwrap();
        let report = run_live_package_preflight_report(&fixture.config).expect("run preflight");
        assert_eq!(
            report.verdict,
            TinyLivePackagePreflightVerdict::TinyLivePackagePreflightCompletedInstallTargetDrifted
        );
    }

    #[test]
    fn stale_service_status_evidence_yields_explicit_non_green_preflight() {
        let fixture = preflight_fixture(
            "tiny_live_package_preflight_stale_status",
            GateState::Green,
            true,
            true,
        );
        let report = run_live_package_preflight_report_with_hook(
            &fixture.config,
            Some(&|status_path| {
                let mut status: tiny_live_activation_live_execute::LiveServiceStatus =
                    load_json(status_path)?;
                status.observed_at = ts("2026-03-20T12:00:00Z");
                persist_json(status_path, &status)?;
                Ok(())
            }),
        )
        .expect("run preflight");
        assert_eq!(
            report.verdict,
            TinyLivePackagePreflightVerdict::TinyLivePackagePreflightFailedDuringServiceStatusVerify
        );
    }

    #[test]
    fn cutover_readiness_blocked_by_current_non_green_gate_is_explicit() {
        let fixture = preflight_fixture(
            "tiny_live_package_preflight_cutover_blocked",
            GateState::Stage3Blocked,
            true,
            true,
        );
        let report = run_live_package_preflight_report(&fixture.config).expect("run preflight");
        assert_eq!(
            report.verdict,
            TinyLivePackagePreflightVerdict::TinyLivePackagePreflightCompletedCutoverBlockedByGate
        );
        assert!(matches!(
            report.current_pre_activation_gate_verdict.as_deref(),
            Some("blocked_by_stage3") | Some("blocked_by_pre_activation_gate")
        ));
    }

    #[test]
    fn verify_live_package_preflight_rejects_tampered_step_path() {
        let fixture = preflight_fixture(
            "tiny_live_package_preflight_tampered_path",
            GateState::Green,
            true,
            true,
        );
        run_live_package_preflight_report(&fixture.config).expect("run preflight");
        let session_dir = fixture.config.session_dir.as_ref().unwrap();
        let paths = package_preflight_paths(session_dir);
        let mut status: PackagePreflightStatus = load_json(&paths.status_path).unwrap();
        status.service_status_step.as_mut().unwrap().path =
            fixture.fixture_dir.join("other.json").display().to_string();
        persist_json(&paths.status_path, &status).unwrap();
        let verify =
            verify_live_package_preflight_report(&fixture.config).expect("verify preflight");
        assert_eq!(
            verify.verdict,
            TinyLivePackagePreflightVerdict::TinyLivePackagePreflightVerifyInvalid
        );
    }

    #[test]
    fn verify_live_package_preflight_rejects_mismatched_target_contract() {
        let fixture = preflight_fixture(
            "tiny_live_package_preflight_wrong_target",
            GateState::Green,
            true,
            true,
        );
        run_live_package_preflight_report(&fixture.config).expect("run preflight");
        let verify = verify_live_package_preflight_report(&Config {
            target_service_name: "other.service".to_string(),
            ..fixture.config.clone()
        })
        .expect("verify preflight");
        assert_eq!(
            verify.verdict,
            TinyLivePackagePreflightVerdict::TinyLivePackagePreflightVerifyInvalid
        );
    }

    struct PreflightFixture {
        fixture_dir: PathBuf,
        install_root: PathBuf,
        config: Config,
        _rpc_server: MockHttpServer,
        _adapter_server: MockHttpServer,
    }

    #[derive(Debug, Clone, Copy)]
    enum GateState {
        Green,
        Stage3Blocked,
    }

    fn preflight_fixture(
        label: &str,
        gate_state: GateState,
        install_target: bool,
        seed_backup: bool,
    ) -> PreflightFixture {
        let fixture_dir = temp_dir(label);
        let install_root = fixture_dir.join("root");
        let target_config_path = install_root.join("etc/solana-copy-bot/live.server.toml");
        fs::create_dir_all(target_config_path.parent().unwrap()).unwrap();
        let db_path = fixture_dir.join("live.db");
        let rpc_server = MockHttpServer::spawn(
            8,
            serde_json::json!({"jsonrpc":"2.0","id":1,"result":123456u64}).to_string(),
            None,
        );
        let adapter_server = MockHttpServer::spawn(
            8,
            serde_json::json!({
                "status": "ok",
                "route": "jito",
                "contract_version": "v1",
                "detail": "simulated"
            })
            .to_string(),
            Some("authorization"),
        );
        let backend_path = fixture_dir.join("fake-backend.sh");
        write_fake_backend(&backend_path);
        fs::write(
            &target_config_path,
            dynamic_live_config_toml(
                &db_path,
                &rpc_server.url(""),
                &adapter_server.url("/submit"),
                false,
            ),
        )
        .unwrap();
        seed_current_gate_state(&db_path, gate_state);

        let source_dir = fixture_dir.join("source");
        fs::create_dir_all(&source_dir).unwrap();
        let activation_config_source_path = source_dir.join("rendered.activation.toml");
        let rollback_config_source_path = source_dir.join("rendered.rollback.toml");
        write_rendered_artifact_pair(
            &target_config_path,
            &activation_config_source_path,
            &rollback_config_source_path,
            &db_path,
            &rpc_server.url(""),
            &adapter_server.url("/submit"),
        );

        let package_dir = fixture_dir.join("package");
        tiny_live_activation_package::export_activation_package_for_deploy(
            &package_dir,
            &install_root,
            "solana-copy-bot.service",
            &backend_path.display().to_string(),
            tiny_live_activation_live_execute::live_service_control_wrapper_contract::DEFAULT_TIMEOUT_MS,
            &activation_config_source_path,
            &rollback_config_source_path,
        )
        .expect("export package");

        if install_target {
            let install = tiny_live_activation_package_deploy::run_mode_for_rehearsal(
                &package_dir,
                &install_root,
                "solana-copy-bot.service",
                &backend_path.display().to_string(),
                tiny_live_activation_live_execute::live_service_control_wrapper_contract::DEFAULT_TIMEOUT_MS,
                tiny_live_activation_package_deploy::RehearsalMode::InstallFromPackage,
            )
            .expect("install package");
            assert_eq!(
                install.verdict,
                "tiny_live_package_deploy_install_completed"
            );
            if seed_backup {
                tiny_live_activation_live_execute::backup_current_config_report_for_cutover(
                    Path::new(install.installed_activation_config_path.as_ref().unwrap()),
                    Path::new(install.installed_rollback_config_path.as_ref().unwrap()),
                    Path::new(install.installed_target_config_path.as_ref().unwrap()),
                    "solana-copy-bot.service",
                    Path::new(install.installed_wrapper_path.as_ref().unwrap()),
                    Path::new(install.runtime_dir.as_ref().unwrap()),
                    Path::new(install.backup_dir.as_ref().unwrap()),
                    10_000,
                    10_000,
                )
                .expect("seed backup proof");
            }
        }

        PreflightFixture {
            fixture_dir: fixture_dir.clone(),
            install_root: install_root.clone(),
            config: Config {
                mode: Mode::RunLivePackagePreflight,
                package_dir,
                install_root,
                target_service_name: "solana-copy-bot.service".to_string(),
                backend_command: backend_path.display().to_string(),
                wrapper_timeout_ms: tiny_live_activation_live_execute::live_service_control_wrapper_contract::DEFAULT_TIMEOUT_MS,
                service_status_max_staleness_ms: DEFAULT_SERVICE_STATUS_MAX_STALENESS_MS,
                session_dir: Some(fixture_dir.join("preflight-session")),
                output_path: None,
                json: false,
            },
            _rpc_server: rpc_server,
            _adapter_server: adapter_server,
        }
    }

    fn write_rendered_artifact_pair(
        target_config_path: &Path,
        activation_path: &Path,
        rollback_path: &Path,
        db_path: &Path,
        rpc_url: &str,
        adapter_url: &str,
    ) {
        let source = load_from_path(target_config_path).unwrap();
        let fingerprint = tiny_live_activation_live_execute::activation_decision_packet::build_config_fingerprint(
            tiny_live_activation_live_execute::tiny_live_activation_execute::FINGERPRINT_SCOPE,
            &source,
        )
        .unwrap();
        write_rendered_artifact(
            activation_path,
            tiny_live_activation_live_execute::tiny_live_activation_execute::RenderKind::Activation,
            dynamic_live_config_toml(db_path, rpc_url, adapter_url, true),
            target_config_path,
            &fingerprint.sha256,
            false,
            true,
        );
        write_rendered_artifact(
            rollback_path,
            tiny_live_activation_live_execute::tiny_live_activation_execute::RenderKind::Rollback,
            dynamic_live_config_toml(db_path, rpc_url, adapter_url, false),
            target_config_path,
            &fingerprint.sha256,
            false,
            false,
        );
    }

    fn write_rendered_artifact(
        path: &Path,
        render_kind: tiny_live_activation_live_execute::tiny_live_activation_execute::RenderKind,
        contents: String,
        source_config_path: &Path,
        source_fingerprint_sha256: &str,
        source_execution_enabled: bool,
        target_execution_enabled: bool,
    ) {
        fs::write(path, &contents).unwrap();
        let hash = format!("{:x}", sha2::Sha256::digest(contents.as_bytes()));
        let metadata = tiny_live_activation_live_execute::tiny_live_activation_execute::RenderedConfigMetadata {
            metadata_version: "1".to_string(),
            render_kind,
            generated_at: ts("2026-03-28T12:00:00Z"),
            input_config_path: source_config_path.display().to_string(),
            output_config_path: path.display().to_string(),
            source_config_fingerprint_scope: tiny_live_activation_live_execute::tiny_live_activation_execute::FINGERPRINT_SCOPE
                .to_string(),
            source_config_fingerprint_sha256: source_fingerprint_sha256.to_string(),
            expected_source_fingerprint_sha256: Some(source_fingerprint_sha256.to_string()),
            rendered_config_sha256: hash,
            pre_activation_gate_verdict: "pre_activation_gates_green".to_string(),
            pre_activation_gate_reason: "green".to_string(),
            activation_plan_verdict: "activation_plan_ready_when_stage_gate_allows".to_string(),
            activation_plan_reason: "ready".to_string(),
            activation_overlay_complete: true,
            rollback_plan_complete: true,
            service_restart_contract_complete: true,
            field_expectations: vec![tiny_live_activation_live_execute::tiny_live_activation_execute::FieldExpectation {
                field: "execution.enabled".to_string(),
                source_value: serde_json::json!(source_execution_enabled),
                target_value: serde_json::json!(target_execution_enabled),
                reason: "test".to_string(),
                source: "test".to_string(),
            }],
            execution_untouched_by_batch: true,
            activation_authorized: false,
            not_authorized_summary: "test".to_string(),
        };
        fs::write(
            tiny_live_activation_live_execute::tiny_live_activation_execute::metadata_path_for_rendered_config(path),
            serde_json::to_string_pretty(&metadata).unwrap(),
        )
        .unwrap();
        tiny_live_activation_live_execute::tiny_live_activation_execute::inspect_rendered_config_artifact(path).unwrap();
    }

    fn seed_current_gate_state(db_path: &Path, gate_state: GateState) {
        let store = SqliteStore::open(db_path).unwrap();
        let now = Utc::now();
        match gate_state {
            GateState::Green => {
                append_wallet_freshness_capture(
                    &store,
                    now - chrono::Duration::minutes(5),
                    "validated_current",
                    "fresh_current",
                    true,
                );
                append_wallet_freshness_capture(
                    &store,
                    now - chrono::Duration::minutes(15),
                    "validated_current",
                    "fresh_current",
                    true,
                );
                append_wallet_freshness_capture(
                    &store,
                    now - chrono::Duration::minutes(25),
                    "validated_current",
                    "fresh_current",
                    true,
                );
                store
                    .append_execution_dry_run_rehearsal(&rehearsal_write(
                        now - chrono::Duration::minutes(3),
                        "rehearsal_green",
                    ))
                    .unwrap();
                store
                    .append_execution_dry_run_rehearsal(&rehearsal_write(
                        now - chrono::Duration::minutes(12),
                        "rehearsal_green_with_business_reject",
                    ))
                    .unwrap();
            }
            GateState::Stage3Blocked => {
                append_wallet_freshness_capture(
                    &store,
                    now - chrono::Duration::minutes(5),
                    "publication_drifting",
                    "drifting_but_acceptable",
                    false,
                );
                append_wallet_freshness_capture(
                    &store,
                    now - chrono::Duration::minutes(15),
                    "publication_drifting",
                    "drifting_but_acceptable",
                    false,
                );
                append_wallet_freshness_capture(
                    &store,
                    now - chrono::Duration::minutes(25),
                    "publication_drifting",
                    "drifting_but_acceptable",
                    false,
                );
            }
        }
    }

    fn append_wallet_freshness_capture(
        store: &SqliteStore,
        captured_at: DateTime<Utc>,
        history_verdict: &str,
        audit_verdict: &str,
        exact_match: bool,
    ) {
        store
            .append_discovery_wallet_freshness_capture(&DiscoveryWalletFreshnessCaptureWrite {
                captured_at,
                recent_cycles: 3,
                verdict: history_verdict.to_string(),
                reason: "seed".to_string(),
                publication_age_seconds: Some(60),
                raw_truth_sufficient: true,
                raw_truth_reason: "full_scoring_window_raw_truth_available".to_string(),
                shadow_signal_verdict: "shadow_signals_present_but_concentrated".to_string(),
                shadow_signal_reason: "seed-shadow".to_string(),
                published_wallet_ids: vec!["wallet-alpha".to_string(), "wallet-beta".to_string()],
                active_follow_wallet_ids: vec![
                    "wallet-alpha".to_string(),
                    "wallet-beta".to_string(),
                ],
                current_raw_top_wallet_ids: vec![
                    "wallet-alpha".to_string(),
                    "wallet-beta".to_string(),
                ],
                audit_json: serde_json::json!({
                    "now": captured_at,
                    "window_start": captured_at - chrono::Duration::days(5),
                    "verdict": audit_verdict,
                    "reason": "seed",
                    "follow_top_n": 2,
                    "publication_truth_available": true,
                    "publication_runtime_mode": "healthy",
                    "publication_recent_under_gate": true,
                    "latest_publication_ts": captured_at,
                    "publication_age_seconds": 60,
                    "latest_publication_window_start": captured_at - chrono::Duration::days(5),
                    "published_scoring_source": "raw_window_persisted_stream",
                    "published_wallet_ids": ["wallet-alpha", "wallet-beta"],
                    "active_follow_wallet_ids": ["wallet-alpha", "wallet-beta"],
                    "current_raw_top_wallet_ids": ["wallet-alpha", "wallet-beta"],
                    "published_vs_current_raw": {
                        "left_count": 2,
                        "right_count": 2,
                        "overlap_count": if exact_match { 2 } else { 1 },
                        "exact_match": exact_match,
                        "only_left": if exact_match { serde_json::json!([]) } else { serde_json::json!(["wallet-beta"]) },
                        "only_right": if exact_match { serde_json::json!([]) } else { serde_json::json!(["wallet-gamma"]) }
                    },
                    "active_follow_vs_current_raw": {
                        "left_count": 2,
                        "right_count": 2,
                        "overlap_count": if exact_match { 2 } else { 1 },
                        "exact_match": exact_match,
                        "only_left": if exact_match { serde_json::json!([]) } else { serde_json::json!(["wallet-beta"]) },
                        "only_right": if exact_match { serde_json::json!([]) } else { serde_json::json!(["wallet-gamma"]) }
                    },
                    "active_follow_vs_published": {
                        "left_count": 2,
                        "right_count": 2,
                        "overlap_count": 2,
                        "exact_match": true,
                        "only_left": [],
                        "only_right": []
                    },
                    "raw_truth": {
                        "sufficient": true,
                        "reason": "full_scoring_window_raw_truth_available",
                        "observed_swaps_loaded": 10,
                        "eligible_wallet_count": 2,
                        "top_wallet_count": 2,
                        "short_retention_configured": false,
                        "covered_since": captured_at - chrono::Duration::days(5),
                        "covered_through_cursor": {
                            "ts_utc": captured_at,
                            "slot": 1,
                            "signature": "sig"
                        },
                        "covered_through_lag_seconds": 30,
                        "tail_fresh_within_runtime_lag": true,
                        "runtime_freshness_lag_seconds": 600,
                        "total_observed_swaps_rows": 10
                    },
                    "rotation": {
                        "signal_available": true,
                        "reason": null,
                        "cycles_requested": 3,
                        "cycles_completed": 3,
                        "sample_interval_seconds": 600,
                        "overlap_with_previous_cycle": 2,
                        "entered_since_previous_cycle": if exact_match {
                            serde_json::json!(["wallet-gamma"])
                        } else {
                            serde_json::json!([])
                        },
                        "left_since_previous_cycle": [],
                        "stable_wallets_across_cycles": ["wallet-alpha", "wallet-beta"],
                        "unique_wallet_count_across_cycles": if exact_match { 3 } else { 2 },
                        "samples": []
                    }
                })
                .to_string(),
                shadow_signal_json: serde_json::json!({
                    "recent_window_start": captured_at - chrono::Duration::minutes(30),
                    "recent_window_end": captured_at,
                    "selected_wallet_ids": ["wallet-alpha", "wallet-beta"],
                    "selected_wallet_count": 2,
                    "selected_wallets_with_recent_raw_activity": 1,
                    "selected_wallets_with_recent_shadow_signal": 1,
                    "recent_raw_swap_count": 3,
                    "recent_shadow_signal_count": 1,
                    "recent_raw_activity_wallet_ids": ["wallet-alpha"],
                    "recent_shadow_signal_wallet_ids": ["wallet-alpha"],
                    "recent_raw_activity_by_wallet": [{
                        "wallet_id": "wallet-alpha",
                        "row_count": 3,
                        "latest_ts": captured_at
                    }],
                    "recent_shadow_signal_by_wallet": [{
                        "wallet_id": "wallet-alpha",
                        "row_count": 1,
                        "latest_ts": captured_at
                    }],
                    "raw_activity_top_wallet_share": 1.0,
                    "shadow_signal_top_wallet_share": 1.0,
                    "raw_activity_broadly_distributed": false,
                    "shadow_signal_broadly_distributed": false,
                    "verdict": "shadow_signals_present_but_concentrated",
                    "reason": "seed-shadow"
                })
                .to_string(),
            })
            .unwrap();
    }

    fn rehearsal_write(
        rehearsed_at: DateTime<Utc>,
        verdict: &str,
    ) -> ExecutionDryRunRehearsalWrite {
        ExecutionDryRunRehearsalWrite {
            rehearsed_at,
            execution_mode: "adapter_submit_confirm".to_string(),
            execution_enabled: false,
            route: "jito".to_string(),
            token: "So11111111111111111111111111111111111111112".to_string(),
            notional_sol: 0.01,
            signer_pubkey_configured: true,
            config_valid: true,
            connectivity_valid: true,
            adapter_contract_valid: true,
            policy_contract_valid: true,
            route_contract_valid: true,
            ready_for_dry_run: true,
            would_be_admissible_for_later_tiny_live: true,
            rpc_preconditions_valid: true,
            rpc_slot: Some(123),
            rpc_blockhash: Some("abc".to_string()),
            rpc_signer_balance_lamports: Some(1),
            adapter_result_classification: verdict.to_string(),
            adapter_accepted: Some(true),
            adapter_detail: verdict.to_string(),
            policy_echo_present: true,
            route_echo_present: true,
            contract_version_echo_present: true,
            response_slippage_bps: None,
            response_tip_lamports: None,
            response_compute_unit_limit: None,
            response_compute_unit_price_micro_lamports: None,
            verdict: verdict.to_string(),
            reason: verdict.to_string(),
            blockers: Vec::new(),
            warnings: Vec::new(),
            rehearsal_json: "{}".to_string(),
        }
    }

    fn dynamic_live_config_toml(
        db_path: &Path,
        rpc_url: &str,
        adapter_url: &str,
        execution_enabled: bool,
    ) -> String {
        let mut contents = sample_activation_config_toml_from_template()
            .replace("/tmp/copybot-live.db", &db_path.display().to_string())
            .replace("https://rpc.example", rpc_url)
            .replace("http://127.0.0.1:8080/submit", adapter_url);
        if !execution_enabled {
            contents = contents.replacen("enabled = true", "enabled = false", 1);
        }
        contents
    }

    fn write_fake_backend(path: &Path) {
        let script = r#"#!/usr/bin/env bash
set -euo pipefail
cmd="${1:-}"
case "$cmd" in
  show)
    printf 'active\nrunning\n'
    ;;
  restart)
    exit 0
    ;;
  *)
    echo "unexpected command $cmd" >&2
    exit 2
    ;;
esac
"#;
        fs::write(path, script).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(path).unwrap().permissions();
            perms.set_mode(0o755);
            fs::set_permissions(path, perms).unwrap();
        }
    }

    fn sample_activation_config_toml_from_template() -> String {
        sample_safe_config_toml(Path::new("/tmp/copybot-live.db")).replacen(
            "enabled = false",
            "enabled = true",
            1,
        )
    }

    fn sample_safe_config_toml(db_path: &Path) -> String {
        format!(
            r#"
[system]
env = "prod-live"

[sqlite]
path = "{}"

[execution]
enabled = false
mode = "adapter_submit_confirm"
batch_size = 1
default_route = "jito"
rpc_http_url = "https://rpc.example"
submit_adapter_http_url = "http://127.0.0.1:8080/submit"
submit_adapter_contract_version = "v1"
submit_adapter_require_policy_echo = true
submit_adapter_auth_token = "adapter-token"
submit_allowed_routes = ["jito"]
submit_route_order = ["jito"]
pretrade_max_fee_overhead_bps = 800
pretrade_max_priority_fee_lamports = 1500
slippage_bps = 40.0
execution_signer_pubkey = "11111111111111111111111111111111"

[execution.submit_route_max_slippage_bps]
jito = 40.0

[execution.submit_route_tip_lamports]
jito = 10000

[execution.submit_route_compute_unit_limit]
jito = 300000

[execution.submit_route_compute_unit_price_micro_lamports]
jito = 1500

[shadow]
copy_notional_sol = 0.05

[risk]
max_position_sol = 0.05
max_concurrent_positions = 1
daily_loss_limit_pct = 0.75

[tiny_live_policy]
enabled = true
max_trade_notional_sol = 0.05
max_batch_size = 1
max_concurrent_positions = 1
max_daily_loss_limit_pct = 1.0
allowed_routes = ["jito"]
require_policy_echo = true
max_pretrade_fee_overhead_bps = 1000
max_pretrade_priority_fee_lamports = 2000

[tiny_live_policy.max_route_slippage_bps]
jito = 50.0

[tiny_live_policy.max_route_tip_lamports]
jito = 10000

[tiny_live_policy.max_route_compute_unit_price_micro_lamports]
jito = 1500

[tiny_live_guardrails]
enabled = true
evaluation_window_seconds = 900
max_execution_error_rate_pct = 5.0
max_adapter_contract_failure_rate_pct = 1.0
max_policy_echo_mismatch_rate_pct = 1.0
max_fee_or_slippage_breach_rate_pct = 5.0
max_connectivity_degraded_window_seconds = 120
max_daily_realized_loss_sol = 0.05
max_consecutive_hard_failures = 3
"#,
            db_path.display()
        )
        .trim_start()
        .to_string()
    }

    fn temp_dir(label: &str) -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        loop {
            let unique = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "{label}_{}_{}_{}",
                std::process::id(),
                unique,
                counter
            ));
            match fs::create_dir(&path) {
                Ok(()) => return path,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("failed creating temp dir {}: {error}", path.display()),
            }
        }
    }

    fn ts(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .unwrap()
            .with_timezone(&Utc)
    }

    struct MockHttpServer {
        addr: SocketAddr,
        handle: Option<thread::JoinHandle<()>>,
    }

    impl MockHttpServer {
        fn spawn(
            expected_requests: usize,
            response_body: String,
            required_header: Option<&str>,
        ) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
            listener
                .set_nonblocking(false)
                .expect("mock server blocking listener");
            let addr = listener.local_addr().expect("mock server addr");
            let required_header = required_header.map(str::to_string);
            let handle = thread::spawn(move || {
                for _ in 0..expected_requests {
                    let (mut stream, _) = listener.accept().expect("accept mock request");
                    let Some(request) = read_http_request(&mut stream) else {
                        break;
                    };
                    if let Some(required_header) = required_header.as_deref() {
                        assert!(
                            request.headers.iter().any(|header| header
                                .to_ascii_lowercase()
                                .starts_with(required_header)),
                            "missing required header {required_header} in {:?}",
                            request.headers
                        );
                    }
                    write_http_response(&mut stream, &response_body);
                }
            });
            Self {
                addr,
                handle: Some(handle),
            }
        }

        fn url(&self, path: &str) -> String {
            format!("http://{}{}", self.addr, path)
        }
    }

    impl Drop for MockHttpServer {
        fn drop(&mut self) {
            let _ = TcpStream::connect(self.addr);
            if let Some(handle) = self.handle.take() {
                let _ = handle.join();
            }
        }
    }

    struct ParsedHttpRequest {
        headers: Vec<String>,
    }

    fn read_http_request(stream: &mut TcpStream) -> Option<ParsedHttpRequest> {
        let mut header_bytes = Vec::new();
        let mut buf = [0_u8; 1024];
        let header_end;
        loop {
            let bytes = stream.read(&mut buf).expect("read request");
            if bytes == 0 {
                return None;
            }
            header_bytes.extend_from_slice(&buf[..bytes]);
            if let Some(pos) = header_bytes
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
            {
                header_end = pos + 4;
                break;
            }
        }

        let header_text = String::from_utf8_lossy(&header_bytes[..header_end]).to_string();
        let headers: Vec<String> = header_text.lines().map(|line| line.to_string()).collect();
        let content_length = headers
            .iter()
            .find_map(|line| {
                let lower = line.to_ascii_lowercase();
                lower
                    .strip_prefix("content-length:")
                    .and_then(|value| value.trim().parse::<usize>().ok())
            })
            .unwrap_or(0);
        let mut body_bytes = header_bytes[header_end..].to_vec();
        while body_bytes.len() < content_length {
            let bytes = stream.read(&mut buf).expect("read request body");
            assert!(bytes > 0, "unexpected eof reading body");
            body_bytes.extend_from_slice(&buf[..bytes]);
        }
        Some(ParsedHttpRequest { headers })
    }

    fn write_http_response(stream: &mut TcpStream, body: &str) {
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream
            .write_all(response.as_bytes())
            .expect("write mock response");
        stream.flush().expect("flush mock response");
    }
}
