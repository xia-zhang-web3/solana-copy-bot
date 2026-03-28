use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

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
#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_package_preflight.rs"]
mod tiny_live_activation_package_preflight;

const USAGE: &str = "usage: copybot_tiny_live_activation_package_capability --package-dir <path> --install-root <path> --target-service <name> --backend-command <path-or-name> [--wrapper-timeout-ms <ms>] [--service-status-max-staleness-ms <ms>] [--session-dir <path>] [--output <path>] [--json] (--plan-live-package-capability | --render-live-package-capability-script | --run-live-package-capability | --verify-live-package-capability)";
const CAPABILITY_SESSION_VERSION: &str = "1";
const CAPABILITY_STATUS_VERSION: &str = "1";
const FILESYSTEM_PROBE_REPORT_VERSION: &str = "1";

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
    PlanLivePackageCapability,
    RenderLivePackageCapabilityScript,
    RunLivePackageCapability,
    VerifyLivePackageCapability,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageCapabilityVerdict {
    TinyLivePackageCapabilityPlanReady,
    TinyLivePackageCapabilityScriptRendered,
    TinyLivePackageCapabilityCompletedReadyForCutoverWhenGateAllows,
    TinyLivePackageCapabilityCompletedInstallTargetMissing,
    TinyLivePackageCapabilityCompletedInstallTargetDrifted,
    TinyLivePackageCapabilityCompletedFilesystemProbeFailed,
    TinyLivePackageCapabilityCompletedServiceProbeFailed,
    TinyLivePackageCapabilityCompletedCutoverBlockedByGate,
    TinyLivePackageCapabilityFailedDuringPackageVerify,
    TinyLivePackageCapabilityFailedDuringLiveTargetVerify,
    TinyLivePackageCapabilityVerifyOk,
    TinyLivePackageCapabilityVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LivePackageCapabilityResult {
    ReadyForCutoverWhenGateAllows,
    InstallTargetMissing,
    InstallTargetDrifted,
    FilesystemProbeFailed,
    ServiceProbeFailed,
    CutoverBlockedByGate,
    FailedDuringPackageVerify,
    FailedDuringLiveTargetVerify,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageCapabilityStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageCapabilitySession {
    session_version: String,
    planned_at: DateTime<Utc>,
    package_dir: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    session_dir: String,
    nested_preflight_session_dir: String,
    run_preflight_command_summary: String,
    filesystem_probe_command_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageCapabilityStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    session_dir: String,
    result: LivePackageCapabilityResult,
    reason: String,
    preflight_step: Option<PackageCapabilityStepArtifact>,
    filesystem_probe_step: Option<PackageCapabilityStepArtifact>,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackageCapabilityPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    preflight_session_dir: PathBuf,
    preflight_report_path: PathBuf,
    filesystem_probe_report_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageCapabilityReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageCapabilityVerdict,
    reason: String,
    package_dir: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    session_dir: Option<String>,
    capability_session_path: Option<String>,
    capability_status_path: Option<String>,
    nested_preflight_session_dir: Option<String>,
    result: Option<String>,
    preflight_step: Option<PackageCapabilityStepArtifact>,
    filesystem_probe_step: Option<PackageCapabilityStepArtifact>,
    run_preflight_command_summary: String,
    filesystem_probe_command_summary: String,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    service_status_observed_at: Option<DateTime<Utc>>,
    service_status_age_ms: Option<i64>,
    verification_mismatches: Vec<String>,
    activation_authorized: bool,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FilesystemCapabilityReport {
    report_version: String,
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: String,
    reason: String,
    package_dir: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    backup_dir: String,
    session_dir: String,
    target_config_path: String,
    target_config_parent: String,
    backup_probe_path: String,
    session_probe_path: String,
    target_config_probe_source_path: String,
    target_config_probe_renamed_path: String,
    backup_probe_write_ok: bool,
    session_probe_write_ok: bool,
    target_config_probe_write_ok: bool,
    target_config_probe_rename_ok: bool,
    backup_probe_cleanup_ok: bool,
    session_probe_cleanup_ok: bool,
    target_config_probe_cleanup_ok: bool,
    verification_mismatches: Vec<String>,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct FilesystemProbeTargets {
    backup_dir: PathBuf,
    session_dir: PathBuf,
    target_config_path: PathBuf,
}

#[derive(Debug, Clone)]
struct FilesystemProbeArtifacts {
    backup_probe_path: PathBuf,
    session_probe_path: PathBuf,
    target_config_probe_source_path: PathBuf,
    target_config_probe_renamed_path: PathBuf,
}

#[derive(Default)]
struct CapabilityRunHooks<'a> {
    after_service_status_hook: Option<&'a dyn Fn(&Path) -> Result<()>>,
    before_filesystem_probe_hook: Option<&'a dyn Fn(&FilesystemProbeTargets) -> Result<()>>,
    before_filesystem_cleanup_hook: Option<&'a dyn Fn(&FilesystemProbeArtifacts) -> Result<()>>,
}

#[derive(Debug, Deserialize)]
struct StoredPreflightReportView {
    verdict: String,
    reason: String,
    package_dir: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    session_dir: Option<String>,
    result: Option<String>,
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
    let mut service_status_max_staleness_ms =
        tiny_live_activation_package_preflight::DEFAULT_SERVICE_STATUS_MAX_STALENESS_MS;
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
            "--plan-live-package-capability" => set_mode(
                &mut mode,
                Mode::PlanLivePackageCapability,
                "--plan-live-package-capability",
            )?,
            "--render-live-package-capability-script" => set_mode(
                &mut mode,
                Mode::RenderLivePackageCapabilityScript,
                "--render-live-package-capability-script",
            )?,
            "--run-live-package-capability" => set_mode(
                &mut mode,
                Mode::RunLivePackageCapability,
                "--run-live-package-capability",
            )?,
            "--verify-live-package-capability" => set_mode(
                &mut mode,
                Mode::VerifyLivePackageCapability,
                "--verify-live-package-capability",
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
        Mode::RunLivePackageCapability | Mode::VerifyLivePackageCapability
    ) && session_dir.is_none()
    {
        bail!("missing required --session-dir for run/verify live package capability");
    }
    if matches!(mode, Mode::RenderLivePackageCapabilityScript) && output_path.is_none() {
        bail!("missing required --output for --render-live-package-capability-script");
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
        Mode::PlanLivePackageCapability => plan_live_package_capability_report(&config)?,
        Mode::RenderLivePackageCapabilityScript => {
            render_live_package_capability_script_report(&config)?
        }
        Mode::RunLivePackageCapability => run_live_package_capability_report(&config)?,
        Mode::VerifyLivePackageCapability => verify_live_package_capability_report(&config)?,
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_human_report(&report))
    }
}

fn plan_live_package_capability_report(config: &Config) -> Result<PackageCapabilityReport> {
    tiny_live_activation_package::verify_package_contract_for_deploy(
        &config.package_dir,
        &config.install_root,
        &config.target_service_name,
        &config.backend_command,
        config.wrapper_timeout_ms,
    )?;
    Ok(base_report(
        config,
        TinyLivePackageCapabilityVerdict::TinyLivePackageCapabilityPlanReady,
        format!(
            "package {} is ready for a bounded live-host capability session over install root {}",
            config.package_dir.display(),
            config.install_root.display()
        ),
    ))
}

fn render_live_package_capability_script_report(
    config: &Config,
) -> Result<PackageCapabilityReport> {
    let plan = plan_live_package_capability_report(config)?;
    let output_path = config
        .output_path
        .as_ref()
        .ok_or_else(|| anyhow!("missing --output for --render-live-package-capability-script"))?;
    ensure_new_output_path(output_path)?;
    let script = format!(
        "#!/usr/bin/env bash\nset -euo pipefail\nif [ \"$#\" -ne 1 ]; then\n  echo \"usage: $(basename \"$0\") <session-dir>\" >&2\n  exit 64\nfi\ncopybot_tiny_live_activation_package_capability --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --service-status-max-staleness-ms {} --session-dir \"$1\" --run-live-package-capability\n",
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
    Ok(PackageCapabilityReport {
        generated_at: Utc::now(),
        mode: "render_live_package_capability_script".to_string(),
        verdict: TinyLivePackageCapabilityVerdict::TinyLivePackageCapabilityScriptRendered,
        reason: format!(
            "live-host package capability script rendered to {} without mutating the target",
            output_path.display()
        ),
        explicit_statement:
            "rendering the live-host package capability script does not authorize or perform production activation by itself"
                .to_string(),
        ..plan
    })
}

fn run_live_package_capability_report(config: &Config) -> Result<PackageCapabilityReport> {
    run_live_package_capability_report_with_hooks(config, CapabilityRunHooks::default())
}

fn run_live_package_capability_report_with_hooks(
    config: &Config,
    hooks: CapabilityRunHooks<'_>,
) -> Result<PackageCapabilityReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for --run-live-package-capability"))?;
    let package_contract = tiny_live_activation_package::verify_package_contract_for_deploy(
        &config.package_dir,
        &config.install_root,
        &config.target_service_name,
        &config.backend_command,
        config.wrapper_timeout_ms,
    )?;
    validate_capability_session_dir(&config.install_root, session_dir)?;
    ensure_clean_capability_session_dir(session_dir)?;
    let paths = package_capability_paths(session_dir);
    let session = planned_session(config, session_dir, &paths.preflight_session_dir);
    persist_json(&paths.session_path, &session)?;

    let preflight =
        tiny_live_activation_package_preflight::run_live_package_preflight_for_capability(
            &config.package_dir,
            &config.install_root,
            &config.target_service_name,
            &config.backend_command,
            config.wrapper_timeout_ms,
            config.service_status_max_staleness_ms,
            &paths.preflight_session_dir,
            hooks.after_service_status_hook,
        )?;
    persist_json_value(&paths.preflight_report_path, &preflight.report_json)?;
    let preflight_step = Some(step_artifact(
        &paths.preflight_report_path,
        "run_live_package_preflight",
        &preflight.verdict,
        &preflight.reason,
        preflight.generated_at,
    ));

    match preflight.result.as_deref() {
        Some("failed_during_package_verify") => {
            let status = PackageCapabilityStatus {
                status_version: CAPABILITY_STATUS_VERSION.to_string(),
                updated_at: Utc::now(),
                session_dir: session_dir.display().to_string(),
                result: LivePackageCapabilityResult::FailedDuringPackageVerify,
                reason: preflight.reason.clone(),
                preflight_step: preflight_step.clone(),
                filesystem_probe_step: None,
                explicit_statement:
                    "this live-host package capability session is bounded host evidence only; it does not authorize production activation"
                        .to_string(),
            };
            persist_json(&paths.status_path, &status)?;
            return Ok(report_from_status(
                config,
                session_dir,
                &paths,
                TinyLivePackageCapabilityVerdict::TinyLivePackageCapabilityFailedDuringPackageVerify,
                &status,
                preflight.current_pre_activation_gate_verdict,
                preflight.current_pre_activation_gate_reason,
                preflight.service_status_observed_at,
                preflight.activation_authorized,
            ));
        }
        Some("install_target_missing") => {
            let status = PackageCapabilityStatus {
                status_version: CAPABILITY_STATUS_VERSION.to_string(),
                updated_at: Utc::now(),
                session_dir: session_dir.display().to_string(),
                result: LivePackageCapabilityResult::InstallTargetMissing,
                reason: preflight.reason.clone(),
                preflight_step: preflight_step.clone(),
                filesystem_probe_step: None,
                explicit_statement:
                    "this live-host package capability session is bounded host evidence only; it does not authorize production activation"
                        .to_string(),
            };
            persist_json(&paths.status_path, &status)?;
            return Ok(report_from_status(
                config,
                session_dir,
                &paths,
                TinyLivePackageCapabilityVerdict::TinyLivePackageCapabilityCompletedInstallTargetMissing,
                &status,
                preflight.current_pre_activation_gate_verdict,
                preflight.current_pre_activation_gate_reason,
                preflight.service_status_observed_at,
                preflight.activation_authorized,
            ));
        }
        Some("install_target_drifted") => {
            let status = PackageCapabilityStatus {
                status_version: CAPABILITY_STATUS_VERSION.to_string(),
                updated_at: Utc::now(),
                session_dir: session_dir.display().to_string(),
                result: LivePackageCapabilityResult::InstallTargetDrifted,
                reason: preflight.reason.clone(),
                preflight_step: preflight_step.clone(),
                filesystem_probe_step: None,
                explicit_statement:
                    "this live-host package capability session is bounded host evidence only; it does not authorize production activation"
                        .to_string(),
            };
            persist_json(&paths.status_path, &status)?;
            return Ok(report_from_status(
                config,
                session_dir,
                &paths,
                TinyLivePackageCapabilityVerdict::TinyLivePackageCapabilityCompletedInstallTargetDrifted,
                &status,
                preflight.current_pre_activation_gate_verdict,
                preflight.current_pre_activation_gate_reason,
                preflight.service_status_observed_at,
                preflight.activation_authorized,
            ));
        }
        Some("failed_during_live_target_verify") => {
            let status = PackageCapabilityStatus {
                status_version: CAPABILITY_STATUS_VERSION.to_string(),
                updated_at: Utc::now(),
                session_dir: session_dir.display().to_string(),
                result: LivePackageCapabilityResult::FailedDuringLiveTargetVerify,
                reason: preflight.reason.clone(),
                preflight_step: preflight_step.clone(),
                filesystem_probe_step: None,
                explicit_statement:
                    "this live-host package capability session is bounded host evidence only; it does not authorize production activation"
                        .to_string(),
            };
            persist_json(&paths.status_path, &status)?;
            return Ok(report_from_status(
                config,
                session_dir,
                &paths,
                TinyLivePackageCapabilityVerdict::TinyLivePackageCapabilityFailedDuringLiveTargetVerify,
                &status,
                preflight.current_pre_activation_gate_verdict,
                preflight.current_pre_activation_gate_reason,
                preflight.service_status_observed_at,
                preflight.activation_authorized,
            ));
        }
        Some("failed_during_service_status_verify") => {
            let status = PackageCapabilityStatus {
                status_version: CAPABILITY_STATUS_VERSION.to_string(),
                updated_at: Utc::now(),
                session_dir: session_dir.display().to_string(),
                result: LivePackageCapabilityResult::ServiceProbeFailed,
                reason: preflight.reason.clone(),
                preflight_step: preflight_step.clone(),
                filesystem_probe_step: None,
                explicit_statement:
                    "this live-host package capability session is bounded host evidence only; it does not authorize production activation"
                        .to_string(),
            };
            persist_json(&paths.status_path, &status)?;
            return Ok(report_from_status(
                config,
                session_dir,
                &paths,
                TinyLivePackageCapabilityVerdict::TinyLivePackageCapabilityCompletedServiceProbeFailed,
                &status,
                preflight.current_pre_activation_gate_verdict,
                preflight.current_pre_activation_gate_reason,
                preflight.service_status_observed_at,
                preflight.activation_authorized,
            ));
        }
        Some("ready_for_cutover_planning") | Some("cutover_blocked_by_gate") => {}
        Some(other) => bail!("unexpected preflight result {other}"),
        None => bail!("preflight report is missing result"),
    }

    let probe_targets = FilesystemProbeTargets {
        backup_dir: PathBuf::from(&package_contract.install_target_summary.backup_dir),
        session_dir: session_dir.to_path_buf(),
        target_config_path: PathBuf::from(
            &package_contract.install_target_summary.target_config_path,
        ),
    };
    if let Some(hook) = hooks.before_filesystem_probe_hook {
        hook(&probe_targets)?;
    }
    let filesystem_probe =
        run_filesystem_probe(config, &probe_targets, hooks.before_filesystem_cleanup_hook)?;
    persist_json(&paths.filesystem_probe_report_path, &filesystem_probe)?;
    let filesystem_probe_step = Some(step_artifact(
        &paths.filesystem_probe_report_path,
        "verify_live_filesystem_capability",
        &filesystem_probe.verdict,
        &filesystem_probe.reason,
        filesystem_probe.generated_at,
    ));
    if filesystem_probe.verdict != "tiny_live_package_capability_filesystem_probe_ok" {
        let status = PackageCapabilityStatus {
            status_version: CAPABILITY_STATUS_VERSION.to_string(),
            updated_at: Utc::now(),
            session_dir: session_dir.display().to_string(),
            result: LivePackageCapabilityResult::FilesystemProbeFailed,
            reason: filesystem_probe.reason.clone(),
            preflight_step,
            filesystem_probe_step,
            explicit_statement:
                "this live-host package capability session is bounded host evidence only; it does not authorize production activation"
                    .to_string(),
        };
        persist_json(&paths.status_path, &status)?;
        return Ok(report_from_status(
            config,
            session_dir,
            &paths,
            TinyLivePackageCapabilityVerdict::TinyLivePackageCapabilityCompletedFilesystemProbeFailed,
            &status,
            preflight.current_pre_activation_gate_verdict,
            preflight.current_pre_activation_gate_reason,
            preflight.service_status_observed_at,
            preflight.activation_authorized,
        ));
    }

    let (result, verdict) = match preflight.result.as_deref() {
        Some("ready_for_cutover_planning") => (
            LivePackageCapabilityResult::ReadyForCutoverWhenGateAllows,
            TinyLivePackageCapabilityVerdict::TinyLivePackageCapabilityCompletedReadyForCutoverWhenGateAllows,
        ),
        Some("cutover_blocked_by_gate") => (
            LivePackageCapabilityResult::CutoverBlockedByGate,
            TinyLivePackageCapabilityVerdict::TinyLivePackageCapabilityCompletedCutoverBlockedByGate,
        ),
        other => bail!("unexpected preflight result after filesystem probe: {:?}", other),
    };
    let status = PackageCapabilityStatus {
        status_version: CAPABILITY_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        session_dir: session_dir.display().to_string(),
        result,
        reason: preflight.reason.clone(),
        preflight_step,
        filesystem_probe_step,
        explicit_statement:
            "this live-host package capability session is bounded host evidence only; it does not authorize production activation"
                .to_string(),
    };
    persist_json(&paths.status_path, &status)?;
    Ok(report_from_status(
        config,
        session_dir,
        &paths,
        verdict,
        &status,
        preflight.current_pre_activation_gate_verdict,
        preflight.current_pre_activation_gate_reason,
        preflight.service_status_observed_at,
        preflight.activation_authorized,
    ))
}

fn verify_live_package_capability_report(config: &Config) -> Result<PackageCapabilityReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for --verify-live-package-capability"))?;
    let package_contract = match tiny_live_activation_package::verify_package_contract_for_deploy(
        &config.package_dir,
        &config.install_root,
        &config.target_service_name,
        &config.backend_command,
        config.wrapper_timeout_ms,
    ) {
        Ok(value) => value,
        Err(error) => {
            let paths = package_capability_paths(session_dir);
            return Ok(PackageCapabilityReport {
                generated_at: Utc::now(),
                mode: "verify_live_package_capability".to_string(),
                verdict: TinyLivePackageCapabilityVerdict::TinyLivePackageCapabilityVerifyInvalid,
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
                capability_session_path: Some(paths.session_path.display().to_string()),
                capability_status_path: Some(paths.status_path.display().to_string()),
                nested_preflight_session_dir: Some(paths.preflight_session_dir.display().to_string()),
                result: None,
                preflight_step: None,
                filesystem_probe_step: None,
                run_preflight_command_summary: run_preflight_command_summary(config, &paths.preflight_session_dir),
                filesystem_probe_command_summary: filesystem_probe_command_summary(config),
                current_pre_activation_gate_verdict: None,
                current_pre_activation_gate_reason: None,
                service_status_observed_at: None,
                service_status_age_ms: None,
                verification_mismatches: vec![error.to_string()],
                activation_authorized: false,
                explicit_statement:
                    "this live-host package capability verification checks bounded host evidence only; it does not authorize production activation"
                        .to_string(),
            });
        }
    };
    validate_capability_session_dir(&config.install_root, session_dir)?;
    let paths = package_capability_paths(session_dir);
    let session: PackageCapabilitySession = load_json(&paths.session_path)?;
    let status: PackageCapabilityStatus = load_json(&paths.status_path)?;
    let mut mismatches = Vec::new();

    compare_string(
        &session.package_dir,
        &config.package_dir.display().to_string(),
        "package capability package_dir",
        &mut mismatches,
    );
    compare_string(
        &session.install_root,
        &config.install_root.display().to_string(),
        "package capability install_root",
        &mut mismatches,
    );
    compare_string(
        &session.target_service_name,
        &config.target_service_name,
        "package capability target_service_name",
        &mut mismatches,
    );
    compare_string(
        &session.backend_command,
        &config.backend_command,
        "package capability backend_command",
        &mut mismatches,
    );
    if session.wrapper_timeout_ms != config.wrapper_timeout_ms {
        mismatches.push(format!(
            "package capability wrapper_timeout_ms {} does not match requested {}",
            session.wrapper_timeout_ms, config.wrapper_timeout_ms
        ));
    }
    if session.service_status_max_staleness_ms != config.service_status_max_staleness_ms {
        mismatches.push(format!(
            "package capability service_status_max_staleness_ms {} does not match requested {}",
            session.service_status_max_staleness_ms, config.service_status_max_staleness_ms
        ));
    }
    if session.session_dir != session_dir.display().to_string() {
        mismatches.push(
            "package capability session_dir does not match the explicit session dir".to_string(),
        );
    }
    compare_string(
        &session.nested_preflight_session_dir,
        &paths.preflight_session_dir.display().to_string(),
        "package capability nested_preflight_session_dir",
        &mut mismatches,
    );
    if status.session_dir != session_dir.display().to_string() {
        mismatches.push(
            "package capability status session_dir does not match the explicit session dir"
                .to_string(),
        );
    }

    let stored_preflight: StoredPreflightReportView = load_required_step_json(
        &status.preflight_step,
        &paths.preflight_report_path,
        "preflight_step",
        &mut mismatches,
    )?;
    compare_string(
        &stored_preflight.package_dir,
        &config.package_dir.display().to_string(),
        "stored preflight package_dir",
        &mut mismatches,
    );
    compare_string(
        &stored_preflight.install_root,
        &config.install_root.display().to_string(),
        "stored preflight install_root",
        &mut mismatches,
    );
    compare_string(
        &stored_preflight.target_service_name,
        &config.target_service_name,
        "stored preflight target_service_name",
        &mut mismatches,
    );
    compare_string(
        &stored_preflight.backend_command,
        &config.backend_command,
        "stored preflight backend_command",
        &mut mismatches,
    );
    if stored_preflight.wrapper_timeout_ms != config.wrapper_timeout_ms {
        mismatches.push(format!(
            "stored preflight wrapper_timeout_ms {} does not match requested {}",
            stored_preflight.wrapper_timeout_ms, config.wrapper_timeout_ms
        ));
    }
    if stored_preflight.service_status_max_staleness_ms != config.service_status_max_staleness_ms {
        mismatches.push(format!(
            "stored preflight service_status_max_staleness_ms {} does not match requested {}",
            stored_preflight.service_status_max_staleness_ms,
            config.service_status_max_staleness_ms
        ));
    }
    if stored_preflight.session_dir.as_deref()
        != Some(paths.preflight_session_dir.display().to_string().as_str())
    {
        mismatches.push(
            "stored preflight session_dir does not match the deterministic nested preflight session dir"
                .to_string(),
        );
    }
    if let Some(step) = &status.preflight_step {
        if stored_preflight.verdict != step.verdict {
            mismatches.push(format!(
                "stored preflight verdict {} does not match recorded preflight step verdict {}",
                stored_preflight.verdict, step.verdict
            ));
        }
        if stored_preflight.reason != step.reason {
            mismatches.push(
                "stored preflight reason does not match the recorded preflight step reason"
                    .to_string(),
            );
        }
    }

    let verified_preflight =
        tiny_live_activation_package_preflight::verify_live_package_preflight_for_capability(
            &config.package_dir,
            &config.install_root,
            &config.target_service_name,
            &config.backend_command,
            config.wrapper_timeout_ms,
            config.service_status_max_staleness_ms,
            &paths.preflight_session_dir,
        )?;
    if verified_preflight.verdict != "tiny_live_package_preflight_verify_ok" {
        mismatches.push(format!(
            "nested live package preflight verification is non-green: {}",
            verified_preflight.reason
        ));
    }
    if stored_preflight.result != verified_preflight.result {
        mismatches.push(format!(
            "stored preflight result {:?} does not match verified nested preflight result {:?}",
            stored_preflight.result, verified_preflight.result
        ));
    }

    let current_pre_activation_gate_verdict = verified_preflight
        .current_pre_activation_gate_verdict
        .clone();
    let current_pre_activation_gate_reason = verified_preflight
        .current_pre_activation_gate_reason
        .clone();
    let service_status_observed_at = verified_preflight.service_status_observed_at;
    let activation_authorized = verified_preflight.activation_authorized;

    match status.result {
        LivePackageCapabilityResult::FailedDuringPackageVerify => {
            if verified_preflight.result.as_deref() != Some("failed_during_package_verify") {
                mismatches.push(format!(
                    "nested preflight result {:?} does not match failed_during_package_verify capability result",
                    verified_preflight.result
                ));
            }
            if status.filesystem_probe_step.is_some() {
                mismatches.push(
                    "failed_during_package_verify capability session must not contain a filesystem probe step"
                        .to_string(),
                );
            }
        }
        LivePackageCapabilityResult::InstallTargetMissing => {
            if verified_preflight.result.as_deref() != Some("install_target_missing") {
                mismatches.push(format!(
                    "nested preflight result {:?} does not match install_target_missing capability result",
                    verified_preflight.result
                ));
            }
            if status.filesystem_probe_step.is_some() {
                mismatches.push(
                    "install_target_missing capability session must not contain a filesystem probe step"
                        .to_string(),
                );
            }
        }
        LivePackageCapabilityResult::InstallTargetDrifted => {
            if verified_preflight.result.as_deref() != Some("install_target_drifted") {
                mismatches.push(format!(
                    "nested preflight result {:?} does not match install_target_drifted capability result",
                    verified_preflight.result
                ));
            }
            if status.filesystem_probe_step.is_some() {
                mismatches.push(
                    "install_target_drifted capability session must not contain a filesystem probe step"
                        .to_string(),
                );
            }
        }
        LivePackageCapabilityResult::ServiceProbeFailed => {
            if verified_preflight.result.as_deref() != Some("failed_during_service_status_verify") {
                mismatches.push(format!(
                    "nested preflight result {:?} does not match service_probe_failed capability result",
                    verified_preflight.result
                ));
            }
            if status.filesystem_probe_step.is_some() {
                mismatches.push(
                    "service_probe_failed capability session must not contain a filesystem probe step"
                        .to_string(),
                );
            }
        }
        LivePackageCapabilityResult::FailedDuringLiveTargetVerify => {
            if verified_preflight.result.as_deref() != Some("failed_during_live_target_verify") {
                mismatches.push(format!(
                    "nested preflight result {:?} does not match failed_during_live_target_verify capability result",
                    verified_preflight.result
                ));
            }
            if status.filesystem_probe_step.is_some() {
                mismatches.push(
                    "failed_during_live_target_verify capability session must not contain a filesystem probe step"
                        .to_string(),
                );
            }
        }
        LivePackageCapabilityResult::FilesystemProbeFailed => {
            if !matches!(
                verified_preflight.result.as_deref(),
                Some("ready_for_cutover_planning") | Some("cutover_blocked_by_gate")
            ) {
                mismatches.push(format!(
                    "nested preflight result {:?} is not compatible with a filesystem-probe capability result",
                    verified_preflight.result
                ));
            }
            let probe: FilesystemCapabilityReport = load_required_step_json(
                &status.filesystem_probe_step,
                &paths.filesystem_probe_report_path,
                "filesystem_probe_step",
                &mut mismatches,
            )?;
            validate_filesystem_probe_report(
                &probe,
                config,
                &package_contract,
                session_dir,
                &mut mismatches,
            );
            if probe.verdict == "tiny_live_package_capability_filesystem_probe_ok" {
                mismatches.push(
                    "filesystem probe step is green but capability result says filesystem_probe_failed"
                        .to_string(),
                );
            }
        }
        LivePackageCapabilityResult::ReadyForCutoverWhenGateAllows
        | LivePackageCapabilityResult::CutoverBlockedByGate => {
            let probe: FilesystemCapabilityReport = load_required_step_json(
                &status.filesystem_probe_step,
                &paths.filesystem_probe_report_path,
                "filesystem_probe_step",
                &mut mismatches,
            )?;
            validate_filesystem_probe_report(
                &probe,
                config,
                &package_contract,
                session_dir,
                &mut mismatches,
            );
            if probe.verdict != "tiny_live_package_capability_filesystem_probe_ok" {
                mismatches.push(format!(
                    "filesystem probe verdict {} is not ok for a ready/blocked capability result",
                    probe.verdict
                ));
            }
            match status.result {
                LivePackageCapabilityResult::ReadyForCutoverWhenGateAllows => {
                    if verified_preflight.result.as_deref() != Some("ready_for_cutover_planning") {
                        mismatches.push(format!(
                            "nested preflight result {:?} does not match ready_for_cutover_when_gate_allows",
                            verified_preflight.result
                        ));
                    }
                }
                LivePackageCapabilityResult::CutoverBlockedByGate => {
                    if verified_preflight.result.as_deref() != Some("cutover_blocked_by_gate") {
                        mismatches.push(format!(
                            "nested preflight result {:?} does not match cutover_blocked_by_gate capability result",
                            verified_preflight.result
                        ));
                    }
                }
                _ => {}
            }
        }
    }

    let verdict = if mismatches.is_empty() {
        TinyLivePackageCapabilityVerdict::TinyLivePackageCapabilityVerifyOk
    } else {
        TinyLivePackageCapabilityVerdict::TinyLivePackageCapabilityVerifyInvalid
    };
    let reason = if mismatches.is_empty() {
        format!(
            "live-host package capability session under {} is coherent and bound to the requested package/host contract",
            session_dir.display()
        )
    } else {
        mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "live-host package capability verification failed".to_string())
    };

    Ok(PackageCapabilityReport {
        generated_at: Utc::now(),
        mode: "verify_live_package_capability".to_string(),
        verdict,
        reason,
        package_dir: config.package_dir.display().to_string(),
        install_root: config.install_root.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        wrapper_timeout_ms: config.wrapper_timeout_ms,
        service_status_max_staleness_ms: config.service_status_max_staleness_ms,
        session_dir: Some(session_dir.display().to_string()),
        capability_session_path: Some(paths.session_path.display().to_string()),
        capability_status_path: Some(paths.status_path.display().to_string()),
        nested_preflight_session_dir: Some(paths.preflight_session_dir.display().to_string()),
        result: Some(serialize_enum(&status.result)),
        preflight_step: status.preflight_step,
        filesystem_probe_step: status.filesystem_probe_step,
        run_preflight_command_summary: run_preflight_command_summary(config, &paths.preflight_session_dir),
        filesystem_probe_command_summary: filesystem_probe_command_summary(config),
        current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason,
        service_status_observed_at,
        service_status_age_ms: service_status_observed_at.map(age_ms),
        verification_mismatches: mismatches,
        activation_authorized,
        explicit_statement:
            "this live-host package capability verification checks bounded host evidence only; it does not authorize production activation"
                .to_string(),
    })
}

fn run_filesystem_probe(
    config: &Config,
    targets: &FilesystemProbeTargets,
    before_cleanup_hook: Option<&dyn Fn(&FilesystemProbeArtifacts) -> Result<()>>,
) -> Result<FilesystemCapabilityReport> {
    let probe_id = format!(
        "tiny_live_activation_package_capability.{}",
        Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_else(|| Utc::now().timestamp_micros() * 1_000)
    );
    let target_parent = targets
        .target_config_path
        .parent()
        .ok_or_else(|| {
            anyhow!(
                "target config path {} has no parent",
                targets.target_config_path.display()
            )
        })?
        .to_path_buf();
    let artifacts = FilesystemProbeArtifacts {
        backup_probe_path: targets
            .backup_dir
            .join(format!("{probe_id}.backup.probe.tmp")),
        session_probe_path: targets
            .session_dir
            .join(format!("{probe_id}.session.probe.tmp")),
        target_config_probe_source_path: target_parent
            .join(format!(".{probe_id}.atomic.source.tmp")),
        target_config_probe_renamed_path: target_parent
            .join(format!(".{probe_id}.atomic.renamed.tmp")),
    };

    let mut mismatches = Vec::new();
    let mut backup_probe_write_ok = false;
    let mut session_probe_write_ok = false;
    let mut target_config_probe_write_ok = false;
    let mut target_config_probe_rename_ok = false;
    let mut backup_probe_cleanup_ok = true;
    let mut session_probe_cleanup_ok = true;
    let mut target_config_probe_cleanup_ok = true;

    if let Err(error) = write_synced_probe(&artifacts.backup_probe_path, b"backup-dir probe\n") {
        mismatches.push(format!(
            "backup dir {} is not usable for bounded temp probe writes: {error}",
            targets.backup_dir.display()
        ));
        backup_probe_cleanup_ok = !artifacts.backup_probe_path.exists();
    } else {
        backup_probe_write_ok = true;
    }

    if let Err(error) = write_synced_probe(&artifacts.session_probe_path, b"session-dir probe\n") {
        mismatches.push(format!(
            "session dir {} is not usable for bounded temp probe writes: {error}",
            targets.session_dir.display()
        ));
        session_probe_cleanup_ok = !artifacts.session_probe_path.exists();
    } else {
        session_probe_write_ok = true;
    }

    if let Err(error) = write_synced_probe(
        &artifacts.target_config_probe_source_path,
        b"target-config-parent atomic probe\n",
    ) {
        mismatches.push(format!(
            "target-config parent {} is not usable for bounded atomic temp writes: {error}",
            target_parent.display()
        ));
        target_config_probe_cleanup_ok = !artifacts.target_config_probe_source_path.exists()
            && !artifacts.target_config_probe_renamed_path.exists();
    } else {
        target_config_probe_write_ok = true;
        if let Err(error) = rename_synced_probe(
            &artifacts.target_config_probe_source_path,
            &artifacts.target_config_probe_renamed_path,
        ) {
            mismatches.push(format!(
                "target-config parent {} failed bounded atomic rename rehearsal: {error}",
                target_parent.display()
            ));
            target_config_probe_cleanup_ok = !artifacts.target_config_probe_source_path.exists()
                && !artifacts.target_config_probe_renamed_path.exists();
        } else {
            target_config_probe_rename_ok = true;
        }
    }

    if let Some(hook) = before_cleanup_hook {
        hook(&artifacts)?;
    }

    if artifacts.backup_probe_path.exists() {
        if let Err(error) = fs::remove_file(&artifacts.backup_probe_path) {
            backup_probe_cleanup_ok = false;
            mismatches.push(format!(
                "backup-dir probe cleanup failed for {}: {error}",
                artifacts.backup_probe_path.display()
            ));
        } else if let Some(parent) = artifacts.backup_probe_path.parent() {
            if let Err(error) = sync_dir(parent) {
                backup_probe_cleanup_ok = false;
                mismatches.push(format!(
                    "backup-dir probe cleanup sync failed for {}: {error}",
                    parent.display()
                ));
            }
        }
    }
    if artifacts.session_probe_path.exists() {
        if let Err(error) = fs::remove_file(&artifacts.session_probe_path) {
            session_probe_cleanup_ok = false;
            mismatches.push(format!(
                "session-dir probe cleanup failed for {}: {error}",
                artifacts.session_probe_path.display()
            ));
        } else if let Some(parent) = artifacts.session_probe_path.parent() {
            if let Err(error) = sync_dir(parent) {
                session_probe_cleanup_ok = false;
                mismatches.push(format!(
                    "session-dir probe cleanup sync failed for {}: {error}",
                    parent.display()
                ));
            }
        }
    }
    if artifacts.target_config_probe_source_path.exists() {
        if let Err(error) = fs::remove_file(&artifacts.target_config_probe_source_path) {
            target_config_probe_cleanup_ok = false;
            mismatches.push(format!(
                "target-config probe cleanup failed for {}: {error}",
                artifacts.target_config_probe_source_path.display()
            ));
        }
    }
    if artifacts.target_config_probe_renamed_path.exists() {
        if let Err(error) = fs::remove_file(&artifacts.target_config_probe_renamed_path) {
            target_config_probe_cleanup_ok = false;
            mismatches.push(format!(
                "target-config probe cleanup failed for {}: {error}",
                artifacts.target_config_probe_renamed_path.display()
            ));
        }
    }
    if target_config_probe_cleanup_ok {
        if let Err(error) = sync_dir(&target_parent) {
            target_config_probe_cleanup_ok = false;
            mismatches.push(format!(
                "target-config parent cleanup sync failed for {}: {error}",
                target_parent.display()
            ));
        }
    }

    let verdict = if mismatches.is_empty() {
        "tiny_live_package_capability_filesystem_probe_ok".to_string()
    } else {
        "tiny_live_package_capability_filesystem_probe_invalid".to_string()
    };
    let reason = if mismatches.is_empty() {
        format!(
            "backup dir {}, session dir {}, and target-config parent {} all support bounded cutover filesystem mechanics",
            targets.backup_dir.display(),
            targets.session_dir.display(),
            target_parent.display()
        )
    } else {
        mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "filesystem capability probe failed".to_string())
    };

    Ok(FilesystemCapabilityReport {
        report_version: FILESYSTEM_PROBE_REPORT_VERSION.to_string(),
        generated_at: Utc::now(),
        mode: "run_live_package_capability_filesystem_probe".to_string(),
        verdict,
        reason,
        package_dir: config.package_dir.display().to_string(),
        install_root: config.install_root.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        backup_dir: targets.backup_dir.display().to_string(),
        session_dir: targets.session_dir.display().to_string(),
        target_config_path: targets.target_config_path.display().to_string(),
        target_config_parent: target_parent.display().to_string(),
        backup_probe_path: artifacts.backup_probe_path.display().to_string(),
        session_probe_path: artifacts.session_probe_path.display().to_string(),
        target_config_probe_source_path: artifacts
            .target_config_probe_source_path
            .display()
            .to_string(),
        target_config_probe_renamed_path: artifacts
            .target_config_probe_renamed_path
            .display()
            .to_string(),
        backup_probe_write_ok,
        session_probe_write_ok,
        target_config_probe_write_ok,
        target_config_probe_rename_ok,
        backup_probe_cleanup_ok,
        session_probe_cleanup_ok,
        target_config_probe_cleanup_ok,
        verification_mismatches: mismatches,
        explicit_statement:
            "this filesystem capability probe uses bounded ephemeral temp artifacts only; it does not modify the installed target config contents or authorize activation"
                .to_string(),
    })
}

fn write_synced_probe(path: &Path, contents: &[u8]) -> Result<()> {
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .with_context(|| format!("failed opening bounded probe file {}", path.display()))?;
    file.write_all(contents)
        .with_context(|| format!("failed writing bounded probe file {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("failed syncing bounded probe file {}", path.display()))?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)
            .with_context(|| format!("failed syncing parent dir {}", parent.display()))?;
    }
    Ok(())
}

fn rename_synced_probe(source: &Path, destination: &Path) -> Result<()> {
    fs::rename(source, destination).with_context(|| {
        format!(
            "failed renaming bounded probe file {} -> {}",
            source.display(),
            destination.display()
        )
    })?;
    if let Some(parent) = destination.parent() {
        sync_dir(parent)
            .with_context(|| format!("failed syncing parent dir {}", parent.display()))?;
    }
    Ok(())
}

fn sync_dir(path: &Path) -> Result<()> {
    let dir = fs::File::open(path)
        .with_context(|| format!("failed opening dir {} for sync", path.display()))?;
    dir.sync_all()
        .with_context(|| format!("failed syncing dir {}", path.display()))
}

fn base_report(
    config: &Config,
    verdict: TinyLivePackageCapabilityVerdict,
    reason: String,
) -> PackageCapabilityReport {
    let paths = config
        .session_dir
        .as_ref()
        .map(|path| package_capability_paths(path));
    PackageCapabilityReport {
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
        capability_session_path: paths
            .as_ref()
            .map(|value| value.session_path.display().to_string()),
        capability_status_path: paths
            .as_ref()
            .map(|value| value.status_path.display().to_string()),
        nested_preflight_session_dir: paths
            .as_ref()
            .map(|value| value.preflight_session_dir.display().to_string()),
        result: None,
        preflight_step: None,
        filesystem_probe_step: None,
        run_preflight_command_summary: run_preflight_command_summary(
            config,
            &config
                .session_dir
                .as_ref()
                .map(|path| package_capability_paths(path).preflight_session_dir)
                .unwrap_or_else(|| PathBuf::from("<session-dir>/tiny_live_activation_package_capability.preflight")),
        ),
        filesystem_probe_command_summary: filesystem_probe_command_summary(config),
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        service_status_observed_at: None,
        service_status_age_ms: None,
        verification_mismatches: Vec::new(),
        activation_authorized: false,
        explicit_statement:
            "this live-host package capability layer is bounded host evidence only; it does not authorize or perform production activation by itself"
                .to_string(),
    }
}

fn report_from_status(
    config: &Config,
    session_dir: &Path,
    paths: &PackageCapabilityPaths,
    verdict: TinyLivePackageCapabilityVerdict,
    status: &PackageCapabilityStatus,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    service_status_observed_at: Option<DateTime<Utc>>,
    activation_authorized: bool,
) -> PackageCapabilityReport {
    PackageCapabilityReport {
        generated_at: Utc::now(),
        mode: "run_live_package_capability".to_string(),
        verdict,
        reason: status.reason.clone(),
        package_dir: config.package_dir.display().to_string(),
        install_root: config.install_root.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        wrapper_timeout_ms: config.wrapper_timeout_ms,
        service_status_max_staleness_ms: config.service_status_max_staleness_ms,
        session_dir: Some(session_dir.display().to_string()),
        capability_session_path: Some(paths.session_path.display().to_string()),
        capability_status_path: Some(paths.status_path.display().to_string()),
        nested_preflight_session_dir: Some(paths.preflight_session_dir.display().to_string()),
        result: Some(serialize_enum(&status.result)),
        preflight_step: status.preflight_step.clone(),
        filesystem_probe_step: status.filesystem_probe_step.clone(),
        run_preflight_command_summary: run_preflight_command_summary(config, &paths.preflight_session_dir),
        filesystem_probe_command_summary: filesystem_probe_command_summary(config),
        current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason,
        service_status_observed_at,
        service_status_age_ms: service_status_observed_at.map(age_ms),
        verification_mismatches: Vec::new(),
        activation_authorized,
        explicit_statement:
            "this live-host package capability session is bounded host evidence only; it does not authorize production activation"
                .to_string(),
    }
}

fn mode_name(mode: Mode) -> &'static str {
    match mode {
        Mode::PlanLivePackageCapability => "plan_live_package_capability",
        Mode::RenderLivePackageCapabilityScript => "render_live_package_capability_script",
        Mode::RunLivePackageCapability => "run_live_package_capability",
        Mode::VerifyLivePackageCapability => "verify_live_package_capability",
    }
}

fn run_preflight_command_summary(config: &Config, preflight_session_dir: &Path) -> String {
    format!(
        "copybot_tiny_live_activation_package_preflight --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --service-status-max-staleness-ms {} --session-dir {} --run-live-package-preflight",
        shell_single_quote(&config.package_dir.display().to_string()),
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&config.target_service_name),
        shell_single_quote(&config.backend_command),
        config.wrapper_timeout_ms,
        config.service_status_max_staleness_ms,
        shell_single_quote(&preflight_session_dir.display().to_string()),
    )
}

fn filesystem_probe_command_summary(config: &Config) -> String {
    format!(
        "bounded filesystem probes: create+sync+cleanup temp artifact under managed backup dir; create+sync+cleanup temp artifact under session dir; create+sync+rename+cleanup sibling temp artifact under target-config parent for service {} under install root {}",
        shell_single_quote(&config.target_service_name),
        shell_single_quote(&config.install_root.display().to_string()),
    )
}

fn planned_session(
    config: &Config,
    session_dir: &Path,
    preflight_session_dir: &Path,
) -> PackageCapabilitySession {
    PackageCapabilitySession {
        session_version: CAPABILITY_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        package_dir: config.package_dir.display().to_string(),
        install_root: config.install_root.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        wrapper_timeout_ms: config.wrapper_timeout_ms,
        service_status_max_staleness_ms: config.service_status_max_staleness_ms,
        session_dir: session_dir.display().to_string(),
        nested_preflight_session_dir: preflight_session_dir.display().to_string(),
        run_preflight_command_summary: run_preflight_command_summary(config, preflight_session_dir),
        filesystem_probe_command_summary: filesystem_probe_command_summary(config),
        explicit_statement:
            "this live-host package capability session binds one immutable package plus one explicit live host contract; it does not authorize production activation"
                .to_string(),
    }
}

fn step_artifact(
    path: &Path,
    mode: &str,
    verdict: &str,
    reason: &str,
    generated_at: DateTime<Utc>,
) -> PackageCapabilityStepArtifact {
    PackageCapabilityStepArtifact {
        path: path.display().to_string(),
        mode: mode.to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        generated_at,
    }
}

fn package_capability_paths(session_dir: &Path) -> PackageCapabilityPaths {
    PackageCapabilityPaths {
        session_path: session_dir.join("tiny_live_activation_package_capability.session.json"),
        status_path: session_dir.join("tiny_live_activation_package_capability.status.json"),
        preflight_session_dir: session_dir
            .join("tiny_live_activation_package_capability.preflight"),
        preflight_report_path: session_dir
            .join("tiny_live_activation_package_capability.preflight.report.json"),
        filesystem_probe_report_path: session_dir
            .join("tiny_live_activation_package_capability.filesystem_probe.report.json"),
    }
}

fn ensure_clean_capability_session_dir(session_dir: &Path) -> Result<()> {
    let paths = package_capability_paths(session_dir);
    for artifact in [
        &paths.session_path,
        &paths.status_path,
        &paths.preflight_report_path,
        &paths.filesystem_probe_report_path,
    ] {
        if artifact.exists() {
            bail!(
                "refusing to reuse live package capability session dir {}; artifact {} already exists",
                session_dir.display(),
                artifact.display()
            );
        }
    }
    if paths.preflight_session_dir.exists() {
        bail!(
            "refusing to reuse live package capability session dir {}; nested preflight session {} already exists",
            session_dir.display(),
            paths.preflight_session_dir.display()
        );
    }
    fs::create_dir_all(session_dir)
        .with_context(|| format!("failed creating session dir {}", session_dir.display()))
}

fn validate_capability_session_dir(install_root: &Path, session_dir: &Path) -> Result<()> {
    if !install_root.is_absolute() {
        bail!("install root must be absolute");
    }
    if !session_dir.is_absolute() {
        bail!("session dir must be absolute");
    }
    let install_root_anchor = find_existing_anchor(install_root, "install root")?;
    reject_anchor_symlink(&install_root_anchor, "install root")?;
    let resolved_install_root = if install_root.exists() {
        fs::canonicalize(install_root).with_context(|| {
            format!(
                "failed canonicalizing install root {}",
                install_root.display()
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
    if session_dir.starts_with(install_root)
        || resolved_session_anchor.starts_with(&resolved_install_root)
    {
        bail!(
            "session dir {} must stay outside install root {} because live-host package capability is read-only with respect to the live install root",
            session_dir.display(),
            install_root.display()
        );
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

fn load_required_step_json<T: for<'de> Deserialize<'de>>(
    step: &Option<PackageCapabilityStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<T> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("missing required package capability step {label}"))?;
    let expected = expected_path.display().to_string();
    if step.path != expected {
        mismatches.push(format!(
            "live package capability {label} path {} does not match deterministic session artifact {}",
            step.path, expected
        ));
    }
    load_json(expected_path).with_context(|| {
        format!(
            "failed reading deterministic live package capability {label} report {}",
            expected_path.display()
        )
    })
}

fn validate_filesystem_probe_report(
    report: &FilesystemCapabilityReport,
    config: &Config,
    package_contract: &tiny_live_activation_package::VerifiedActivationPackageContract,
    session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &report.package_dir,
        &config.package_dir.display().to_string(),
        "filesystem probe package_dir",
        mismatches,
    );
    compare_string(
        &report.install_root,
        &config.install_root.display().to_string(),
        "filesystem probe install_root",
        mismatches,
    );
    compare_string(
        &report.target_service_name,
        &config.target_service_name,
        "filesystem probe target_service_name",
        mismatches,
    );
    compare_string(
        &report.backend_command,
        &config.backend_command,
        "filesystem probe backend_command",
        mismatches,
    );
    compare_string(
        &report.backup_dir,
        &package_contract.install_target_summary.backup_dir,
        "filesystem probe backup_dir",
        mismatches,
    );
    compare_string(
        &report.session_dir,
        &session_dir.display().to_string(),
        "filesystem probe session_dir",
        mismatches,
    );
    compare_string(
        &report.target_config_path,
        &package_contract.install_target_summary.target_config_path,
        "filesystem probe target_config_path",
        mismatches,
    );
    let target_parent = Path::new(&package_contract.install_target_summary.target_config_path)
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_default();
    compare_string(
        &report.target_config_parent,
        &target_parent.display().to_string(),
        "filesystem probe target_config_parent",
        mismatches,
    );
    for (label, path, expected_root) in [
        (
            "backup_probe_path",
            Path::new(&report.backup_probe_path),
            Path::new(&report.backup_dir),
        ),
        (
            "session_probe_path",
            Path::new(&report.session_probe_path),
            Path::new(&report.session_dir),
        ),
        (
            "target_config_probe_source_path",
            Path::new(&report.target_config_probe_source_path),
            Path::new(&report.target_config_parent),
        ),
        (
            "target_config_probe_renamed_path",
            Path::new(&report.target_config_probe_renamed_path),
            Path::new(&report.target_config_parent),
        ),
    ] {
        if !path.starts_with(expected_root) {
            mismatches.push(format!(
                "filesystem probe {label} {} is not bounded under {}",
                path.display(),
                expected_root.display()
            ));
        }
    }
    if Path::new(&report.session_dir).starts_with(&config.install_root) {
        mismatches.push(
            "filesystem probe session_dir must stay outside the managed live install root"
                .to_string(),
        );
    }
    if report.backup_probe_write_ok && Path::new(&report.backup_probe_path).exists() {
        mismatches.push(
            "filesystem probe backup temp artifact still exists even though cleanup was expected"
                .to_string(),
        );
    }
    if report.session_probe_write_ok && Path::new(&report.session_probe_path).exists() {
        mismatches.push(
            "filesystem probe session temp artifact still exists even though cleanup was expected"
                .to_string(),
        );
    }
    if (report.target_config_probe_write_ok || report.target_config_probe_rename_ok)
        && (Path::new(&report.target_config_probe_source_path).exists()
            || Path::new(&report.target_config_probe_renamed_path).exists())
    {
        mismatches.push(
            "filesystem probe target-config temp artifacts still exist even though cleanup was expected"
                .to_string(),
        );
    }
    if report.verdict == "tiny_live_package_capability_filesystem_probe_ok" {
        if !report.backup_probe_write_ok
            || !report.session_probe_write_ok
            || !report.target_config_probe_write_ok
            || !report.target_config_probe_rename_ok
            || !report.backup_probe_cleanup_ok
            || !report.session_probe_cleanup_ok
            || !report.target_config_probe_cleanup_ok
            || !report.verification_mismatches.is_empty()
        {
            mismatches.push(
                "filesystem probe report claims ok but does not satisfy the bounded success contract"
                    .to_string(),
            );
        }
    } else if report.verification_mismatches.is_empty() {
        mismatches.push(
            "filesystem probe report is non-green without explicit verification mismatches"
                .to_string(),
        );
    }
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

fn render_human_report(report: &PackageCapabilityReport) -> String {
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
    use std::io::{ErrorKind, Read, Write};
    use std::net::{SocketAddr, TcpListener};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn coherent_package_and_matching_fake_live_target_yield_green_capability_session() {
        let fixture = capability_fixture(
            "tiny_live_package_capability_green",
            GateState::Green,
            true,
            true,
        );
        let report = run_live_package_capability_report(&fixture.config).expect("run capability");
        assert_eq!(
            report.verdict,
            TinyLivePackageCapabilityVerdict::TinyLivePackageCapabilityCompletedReadyForCutoverWhenGateAllows
        );
    }

    #[test]
    fn backup_dir_probe_failure_is_explicitly_non_green() {
        let fixture = capability_fixture(
            "tiny_live_package_capability_backup_fail",
            GateState::Green,
            true,
            true,
        );
        let report = run_live_package_capability_report_with_hooks(
            &fixture.config,
            CapabilityRunHooks {
                before_filesystem_probe_hook: Some(&|targets| {
                    fs::remove_dir_all(&targets.backup_dir)
                        .context("remove fake backup dir before probe")?;
                    Ok(())
                }),
                ..CapabilityRunHooks::default()
            },
        )
        .expect("run capability");
        assert_eq!(
            report.verdict,
            TinyLivePackageCapabilityVerdict::TinyLivePackageCapabilityCompletedFilesystemProbeFailed
        );
    }

    #[test]
    fn target_config_parent_atomic_rehearsal_failure_is_explicitly_non_green() {
        let fixture = capability_fixture(
            "tiny_live_package_capability_atomic_fail",
            GateState::Green,
            true,
            true,
        );
        let report = run_live_package_capability_report_with_hooks(
            &fixture.config,
            CapabilityRunHooks {
                before_filesystem_probe_hook: Some(&|targets| {
                    let parent = targets.target_config_path.parent().unwrap().to_path_buf();
                    fs::remove_dir_all(&parent).context("remove fake target-config parent")?;
                    Ok(())
                }),
                ..CapabilityRunHooks::default()
            },
        )
        .expect("run capability");
        assert_eq!(
            report.verdict,
            TinyLivePackageCapabilityVerdict::TinyLivePackageCapabilityCompletedFilesystemProbeFailed
        );
    }

    #[test]
    fn probe_cleanup_failure_is_surfaced_explicitly() {
        let fixture = capability_fixture(
            "tiny_live_package_capability_cleanup_fail",
            GateState::Green,
            true,
            true,
        );
        let report = run_live_package_capability_report_with_hooks(
            &fixture.config,
            CapabilityRunHooks {
                before_filesystem_cleanup_hook: Some(&|artifacts| {
                    if artifacts.target_config_probe_renamed_path.exists() {
                        fs::remove_file(&artifacts.target_config_probe_renamed_path).ok();
                    }
                    fs::create_dir_all(&artifacts.target_config_probe_renamed_path)
                        .context("replace renamed probe file with a directory")?;
                    Ok(())
                }),
                ..CapabilityRunHooks::default()
            },
        )
        .expect("run capability");
        assert_eq!(
            report.verdict,
            TinyLivePackageCapabilityVerdict::TinyLivePackageCapabilityCompletedFilesystemProbeFailed
        );
    }

    #[test]
    fn installed_wrapper_tampered_vs_package_contract_yields_non_green_capability() {
        let fixture = capability_fixture(
            "tiny_live_package_capability_tampered_wrapper",
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
        let report = run_live_package_capability_report(&fixture.config).expect("run capability");
        assert_eq!(
            report.verdict,
            TinyLivePackageCapabilityVerdict::TinyLivePackageCapabilityCompletedInstallTargetDrifted
        );
    }

    #[test]
    fn stale_service_status_evidence_yields_explicit_non_green_capability() {
        let fixture = capability_fixture(
            "tiny_live_package_capability_stale_status",
            GateState::Green,
            true,
            true,
        );
        let report = run_live_package_capability_report_with_hooks(
            &fixture.config,
            CapabilityRunHooks {
                after_service_status_hook: Some(&|status_path| {
                    let mut status: tiny_live_activation_live_execute::LiveServiceStatus =
                        load_json(status_path)?;
                    status.observed_at = ts("2026-03-20T00:00:00Z");
                    persist_json(status_path, &status)
                }),
                ..CapabilityRunHooks::default()
            },
        )
        .expect("run capability");
        assert_eq!(
            report.verdict,
            TinyLivePackageCapabilityVerdict::TinyLivePackageCapabilityCompletedServiceProbeFailed
        );
    }

    #[test]
    fn cutover_readiness_blocked_by_current_non_green_gate_is_explicit() {
        let fixture = capability_fixture(
            "tiny_live_package_capability_cutover_blocked",
            GateState::Stage3Blocked,
            true,
            true,
        );
        let report = run_live_package_capability_report(&fixture.config).expect("run capability");
        assert_eq!(
            report.verdict,
            TinyLivePackageCapabilityVerdict::TinyLivePackageCapabilityCompletedCutoverBlockedByGate
        );
        assert!(matches!(
            report.current_pre_activation_gate_verdict.as_deref(),
            Some("blocked_by_stage3") | Some("blocked_by_pre_activation_gate")
        ));
    }

    #[test]
    fn verify_live_package_capability_rejects_tampered_step_path() {
        let fixture = capability_fixture(
            "tiny_live_package_capability_tampered_path",
            GateState::Green,
            true,
            true,
        );
        run_live_package_capability_report(&fixture.config).expect("run capability");
        let session_dir = fixture.config.session_dir.as_ref().unwrap();
        let paths = package_capability_paths(session_dir);
        let mut status: PackageCapabilityStatus = load_json(&paths.status_path).unwrap();
        status.preflight_step.as_mut().unwrap().path =
            fixture.fixture_dir.join("other.json").display().to_string();
        persist_json(&paths.status_path, &status).unwrap();
        let verify =
            verify_live_package_capability_report(&fixture.config).expect("verify capability");
        assert_eq!(
            verify.verdict,
            TinyLivePackageCapabilityVerdict::TinyLivePackageCapabilityVerifyInvalid
        );
    }

    #[test]
    fn verify_live_package_capability_rejects_mismatched_target_contract() {
        let fixture = capability_fixture(
            "tiny_live_package_capability_wrong_target",
            GateState::Green,
            true,
            true,
        );
        run_live_package_capability_report(&fixture.config).expect("run capability");
        let verify = verify_live_package_capability_report(&Config {
            target_service_name: "other.service".to_string(),
            ..fixture.config.clone()
        })
        .expect("verify capability");
        assert_eq!(
            verify.verdict,
            TinyLivePackageCapabilityVerdict::TinyLivePackageCapabilityVerifyInvalid
        );
    }

    struct CapabilityFixture {
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

    fn capability_fixture(
        label: &str,
        gate_state: GateState,
        install_target: bool,
        seed_backup: bool,
    ) -> CapabilityFixture {
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

        CapabilityFixture {
            fixture_dir: fixture_dir.clone(),
            install_root: install_root.clone(),
            config: Config {
                mode: Mode::RunLivePackageCapability,
                package_dir,
                install_root,
                target_service_name: "solana-copy-bot.service".to_string(),
                backend_command: backend_path.display().to_string(),
                wrapper_timeout_ms: tiny_live_activation_live_execute::live_service_control_wrapper_contract::DEFAULT_TIMEOUT_MS,
                service_status_max_staleness_ms: tiny_live_activation_package_preflight::DEFAULT_SERVICE_STATUS_MAX_STALENESS_MS,
                session_dir: Some(fixture_dir.join("capability-session")),
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
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "{}_{}_{}_{}",
                label,
                std::process::id(),
                now,
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
        shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>,
        handle: Option<thread::JoinHandle<()>>,
    }

    impl MockHttpServer {
        fn spawn(max_requests: usize, response_body: String, auth_header: Option<&str>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let addr = listener.local_addr().unwrap();
            let auth_header = auth_header.map(str::to_string);
            let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let shutdown_flag = std::sync::Arc::clone(&shutdown);
            let handle = thread::spawn(move || {
                let mut served = 0usize;
                let mut idle_loops = 0usize;
                while served < max_requests
                    && idle_loops < 6_000
                    && !shutdown_flag.load(std::sync::atomic::Ordering::Relaxed)
                {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            served += 1;
                            idle_loops = 0;
                            stream.set_nonblocking(false).unwrap();
                            let mut request = String::new();
                            let mut buf = [0u8; 4096];
                            let n = stream.read(&mut buf).unwrap();
                            request.push_str(&String::from_utf8_lossy(&buf[..n]));
                            if let Some(expected) = &auth_header {
                                assert!(
                                    request.lines().any(|line| line
                                        .to_ascii_lowercase()
                                        .starts_with(&format!(
                                            "{}:",
                                            expected.to_ascii_lowercase()
                                        ))),
                                    "missing expected header {expected} in request {request}"
                                );
                            }
                            let response = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                response_body.len(),
                                response_body
                            );
                            stream.write_all(response.as_bytes()).unwrap();
                        }
                        Err(error) if error.kind() == ErrorKind::WouldBlock => {
                            idle_loops += 1;
                            thread::sleep(Duration::from_millis(10));
                        }
                        Err(error) => panic!("mock http server accept failed: {error}"),
                    }
                }
            });
            Self {
                addr,
                shutdown,
                handle: Some(handle),
            }
        }

        fn url(&self, path: &str) -> String {
            format!("http://{}{}", self.addr, path)
        }
    }

    impl Drop for MockHttpServer {
        fn drop(&mut self) {
            self.shutdown
                .store(true, std::sync::atomic::Ordering::Relaxed);
            if let Some(handle) = self.handle.take() {
                let _ = handle.join();
            }
        }
    }
}
