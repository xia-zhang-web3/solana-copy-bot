use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
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

const USAGE: &str = "usage: copybot_tiny_live_activation_package_rehearsal --package-dir <path> --install-root <path> --target-service <name> --backend-command <path-or-name> [--wrapper-timeout-ms <ms>] [--session-dir <path>] [--output <path>] [--json] (--plan-package-rehearsal | --render-package-rehearsal-script | --run-package-rehearsal | --verify-package-rehearsal)";
const REHEARSAL_SESSION_VERSION: &str = "1";
const REHEARSAL_STATUS_VERSION: &str = "1";
const DEFAULT_STARTUP_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_ROLLBACK_TIMEOUT_MS: u64 = 10_000;

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
    session_dir: Option<PathBuf>,
    output_path: Option<PathBuf>,
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    PlanPackageRehearsal,
    RenderPackageRehearsalScript,
    RunPackageRehearsal,
    VerifyPackageRehearsal,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageRehearsalVerdict {
    TinyLivePackageRehearsalPlanReady,
    TinyLivePackageRehearsalScriptRendered,
    TinyLivePackageRehearsalCompleted,
    TinyLivePackageRehearsalFailedBeforeInstall,
    TinyLivePackageRehearsalFailedDuringInstall,
    TinyLivePackageRehearsalFailedDuringVerify,
    TinyLivePackageRehearsalFailedDuringCutoverPlan,
    TinyLivePackageRehearsalVerifyOk,
    TinyLivePackageRehearsalVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum PackageRehearsalResult {
    Completed,
    FailedBeforeInstall,
    FailedDuringInstall,
    FailedDuringVerify,
    FailedDuringCutoverPlan,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageRehearsalStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageRehearsalSession {
    session_version: String,
    planned_at: DateTime<Utc>,
    package_dir: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    session_dir: String,
    package_verify_command_summary: String,
    install_from_package_command_summary: String,
    verify_install_target_command_summary: String,
    package_cutover_plan_command_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageRehearsalStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    session_dir: String,
    result: PackageRehearsalResult,
    reason: String,
    package_verify_step: Option<PackageRehearsalStepArtifact>,
    install_step: Option<PackageRehearsalStepArtifact>,
    verify_target_step: Option<PackageRehearsalStepArtifact>,
    cutover_plan_step: Option<PackageRehearsalStepArtifact>,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackageRehearsalPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    package_verify_report_path: PathBuf,
    install_report_path: PathBuf,
    verify_target_report_path: PathBuf,
    cutover_plan_report_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageRehearsalReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageRehearsalVerdict,
    reason: String,
    package_dir: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    session_dir: Option<String>,
    rehearsal_session_path: Option<String>,
    rehearsal_status_path: Option<String>,
    result: Option<String>,
    package_verify_step: Option<PackageRehearsalStepArtifact>,
    install_step: Option<PackageRehearsalStepArtifact>,
    verify_target_step: Option<PackageRehearsalStepArtifact>,
    cutover_plan_step: Option<PackageRehearsalStepArtifact>,
    package_verify_command_summary: String,
    install_from_package_command_summary: String,
    verify_install_target_command_summary: String,
    package_cutover_plan_command_summary: String,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    verification_mismatches: Vec<String>,
    activation_authorized: bool,
    explicit_statement: String,
}

#[derive(Debug, Deserialize)]
struct PackageDeployReportView {
    verdict: String,
    package_dir: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    packaged_activation_config_path: Option<String>,
    packaged_rollback_config_path: Option<String>,
    packaged_wrapper_path: Option<String>,
    installed_wrapper_path: Option<String>,
    installed_activation_config_path: Option<String>,
    installed_rollback_config_path: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    activation_authorized: bool,
}

#[derive(Debug, Deserialize)]
struct InstallTargetVerificationView {
    mismatches: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct InstallTargetReportView {
    verdict: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    verification: Option<InstallTargetVerificationView>,
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
            "--plan-package-rehearsal" => set_mode(
                &mut mode,
                Mode::PlanPackageRehearsal,
                "--plan-package-rehearsal",
            )?,
            "--render-package-rehearsal-script" => set_mode(
                &mut mode,
                Mode::RenderPackageRehearsalScript,
                "--render-package-rehearsal-script",
            )?,
            "--run-package-rehearsal" => set_mode(
                &mut mode,
                Mode::RunPackageRehearsal,
                "--run-package-rehearsal",
            )?,
            "--verify-package-rehearsal" => set_mode(
                &mut mode,
                Mode::VerifyPackageRehearsal,
                "--verify-package-rehearsal",
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
        Mode::RunPackageRehearsal | Mode::VerifyPackageRehearsal
    ) && session_dir.is_none()
    {
        bail!("missing required --session-dir for run/verify package rehearsal");
    }
    if matches!(mode, Mode::RenderPackageRehearsalScript) && output_path.is_none() {
        bail!("missing required --output for --render-package-rehearsal-script");
    }

    Ok(Some(Config {
        mode,
        package_dir,
        install_root,
        target_service_name,
        backend_command,
        wrapper_timeout_ms,
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
        Mode::PlanPackageRehearsal => plan_package_rehearsal_report(&config)?,
        Mode::RenderPackageRehearsalScript => render_package_rehearsal_script_report(&config)?,
        Mode::RunPackageRehearsal => run_package_rehearsal_report(&config)?,
        Mode::VerifyPackageRehearsal => verify_package_rehearsal_report(&config)?,
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_human_report(&report))
    }
}

fn plan_package_rehearsal_report(config: &Config) -> Result<PackageRehearsalReport> {
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
            TinyLivePackageRehearsalVerdict::TinyLivePackageRehearsalVerifyInvalid,
            step.reason,
        ));
    }
    Ok(base_report(
        config,
        TinyLivePackageRehearsalVerdict::TinyLivePackageRehearsalPlanReady,
        format!(
            "package {} is ready for a bounded fake-root rehearsal using {} as the single immutable handoff input",
            config.package_dir.display(),
            config.install_root.display()
        ),
    ))
}

fn render_package_rehearsal_script_report(config: &Config) -> Result<PackageRehearsalReport> {
    let plan = plan_package_rehearsal_report(config)?;
    if plan.verdict != TinyLivePackageRehearsalVerdict::TinyLivePackageRehearsalPlanReady {
        return Ok(plan);
    }
    let output_path = config
        .output_path
        .as_ref()
        .ok_or_else(|| anyhow!("missing --output for --render-package-rehearsal-script"))?;
    ensure_new_output_path(output_path)?;
    let script = format!(
        "#!/usr/bin/env bash\nset -euo pipefail\nif [ \"$#\" -ne 1 ]; then\n  echo \"usage: $(basename \"$0\") <session-dir>\" >&2\n  exit 64\nfi\ncopybot_tiny_live_activation_package_rehearsal --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --session-dir \"$1\" --run-package-rehearsal\n",
        shell_single_quote(&config.package_dir.display().to_string()),
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&config.target_service_name),
        shell_single_quote(&config.backend_command),
        config.wrapper_timeout_ms,
    );
    write_text_file(output_path, &script)?;
    #[cfg(unix)]
    set_executable(output_path)?;
    Ok(PackageRehearsalReport {
        generated_at: Utc::now(),
        mode: "render_package_rehearsal_script".to_string(),
        verdict: TinyLivePackageRehearsalVerdict::TinyLivePackageRehearsalScriptRendered,
        reason: format!(
            "package-native rehearsal script rendered to {} without mutating the target",
            output_path.display()
        ),
        explicit_statement:
            "rendering the package-native rehearsal script does not authorize or perform production activation by itself"
                .to_string(),
        ..plan
    })
}

fn run_package_rehearsal_report(config: &Config) -> Result<PackageRehearsalReport> {
    run_package_rehearsal_report_with_hooks(config, None, None)
}

fn run_package_rehearsal_report_with_hooks(
    config: &Config,
    before_install_hook: Option<&dyn Fn() -> Result<()>>,
    after_install_hook: Option<
        &dyn Fn(&tiny_live_activation_package_deploy::PackageDeployRehearsalStep) -> Result<()>,
    >,
) -> Result<PackageRehearsalReport> {
    validate_fake_rehearsal_contract(config)?;
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for --run-package-rehearsal"))?;
    ensure_clean_package_rehearsal_session_dir(session_dir)?;
    let paths = package_rehearsal_paths(session_dir);
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
        let status = PackageRehearsalStatus {
            status_version: REHEARSAL_STATUS_VERSION.to_string(),
            updated_at: Utc::now(),
            session_dir: session_dir.display().to_string(),
            result: PackageRehearsalResult::FailedBeforeInstall,
            reason: package_verify.reason.clone(),
            package_verify_step: package_verify_step.clone(),
            install_step: None,
            verify_target_step: None,
            cutover_plan_step: None,
            explicit_statement:
                "this package-native rehearsal session is artifact evidence only; it does not authorize production activation"
                    .to_string(),
        };
        persist_json(&paths.status_path, &status)?;
        return Ok(report_from_status(
            config,
            session_dir,
            TinyLivePackageRehearsalVerdict::TinyLivePackageRehearsalFailedBeforeInstall,
            &status,
            None,
            None,
        ));
    }
    if let Some(hook) = before_install_hook {
        hook()?;
    }

    let install = tiny_live_activation_package_deploy::run_mode_for_rehearsal(
        &config.package_dir,
        &config.install_root,
        &config.target_service_name,
        &config.backend_command,
        config.wrapper_timeout_ms,
        tiny_live_activation_package_deploy::RehearsalMode::InstallFromPackage,
    )?;
    persist_json_value(&paths.install_report_path, &install.report_json)?;
    let install_step = Some(step_artifact(
        &paths.install_report_path,
        "install_from_package",
        &install.verdict,
        &install.reason,
        install.generated_at,
    ));
    if install.verdict != "tiny_live_package_deploy_install_completed" {
        let status = PackageRehearsalStatus {
            status_version: REHEARSAL_STATUS_VERSION.to_string(),
            updated_at: Utc::now(),
            session_dir: session_dir.display().to_string(),
            result: PackageRehearsalResult::FailedDuringInstall,
            reason: install.reason.clone(),
            package_verify_step: package_verify_step.clone(),
            install_step: install_step.clone(),
            verify_target_step: None,
            cutover_plan_step: None,
            explicit_statement:
                "this package-native rehearsal session is artifact evidence only; it does not authorize production activation"
                    .to_string(),
        };
        persist_json(&paths.status_path, &status)?;
        return Ok(report_from_status(
            config,
            session_dir,
            TinyLivePackageRehearsalVerdict::TinyLivePackageRehearsalFailedDuringInstall,
            &status,
            None,
            None,
        ));
    }
    if let Some(hook) = after_install_hook {
        hook(&install)?;
    }

    let verify_target = tiny_live_activation_install_target::verify_install_target_for_rehearsal(
        &config.install_root,
        &config.target_service_name,
        &config.backend_command,
        config.wrapper_timeout_ms,
    )?;
    persist_json_value(&paths.verify_target_report_path, &verify_target.report_json)?;
    let verify_target_step = Some(step_artifact(
        &paths.verify_target_report_path,
        "verify_install_target",
        &verify_target.verdict,
        &verify_target.reason,
        verify_target.generated_at,
    ));
    if verify_target.verdict != "tiny_live_install_target_verify_ok" {
        let status = PackageRehearsalStatus {
            status_version: REHEARSAL_STATUS_VERSION.to_string(),
            updated_at: Utc::now(),
            session_dir: session_dir.display().to_string(),
            result: PackageRehearsalResult::FailedDuringVerify,
            reason: verify_target.reason.clone(),
            package_verify_step: package_verify_step.clone(),
            install_step: install_step.clone(),
            verify_target_step: verify_target_step.clone(),
            cutover_plan_step: None,
            explicit_statement:
                "this package-native rehearsal session is artifact evidence only; it does not authorize production activation"
                    .to_string(),
        };
        persist_json(&paths.status_path, &status)?;
        return Ok(report_from_status(
            config,
            session_dir,
            TinyLivePackageRehearsalVerdict::TinyLivePackageRehearsalFailedDuringVerify,
            &status,
            None,
            None,
        ));
    }

    if let (
        Some(activation_config_path),
        Some(rollback_config_path),
        Some(target_config_path),
        Some(runtime_dir),
        Some(backup_dir),
    ) = (
        install.installed_activation_config_path.as_ref(),
        install.installed_rollback_config_path.as_ref(),
        install.installed_target_config_path.as_ref(),
        install.runtime_dir.as_ref(),
        install.backup_dir.as_ref(),
    ) {
        let _ = tiny_live_activation_live_execute::backup_current_config_report_for_cutover(
            Path::new(activation_config_path),
            Path::new(rollback_config_path),
            Path::new(target_config_path),
            &config.target_service_name,
            Path::new(
                install
                    .installed_wrapper_path
                    .as_deref()
                    .unwrap_or_default(),
            ),
            Path::new(runtime_dir),
            Path::new(backup_dir),
            DEFAULT_STARTUP_TIMEOUT_MS,
            DEFAULT_ROLLBACK_TIMEOUT_MS,
        );
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
    let (result, verdict) = if cutover_plan.verdict == "tiny_live_package_cutover_plan_ready" {
        (
            PackageRehearsalResult::Completed,
            TinyLivePackageRehearsalVerdict::TinyLivePackageRehearsalCompleted,
        )
    } else {
        (
            PackageRehearsalResult::FailedDuringCutoverPlan,
            TinyLivePackageRehearsalVerdict::TinyLivePackageRehearsalFailedDuringCutoverPlan,
        )
    };
    let status = PackageRehearsalStatus {
        status_version: REHEARSAL_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        session_dir: session_dir.display().to_string(),
        result,
        reason: cutover_plan.reason.clone(),
        package_verify_step,
        install_step,
        verify_target_step,
        cutover_plan_step,
        explicit_statement:
            "this package-native rehearsal session is artifact evidence only; it does not authorize production activation"
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
    ))
}

fn verify_package_rehearsal_report(config: &Config) -> Result<PackageRehearsalReport> {
    validate_fake_rehearsal_contract(config)?;
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for --verify-package-rehearsal"))?;
    let paths = package_rehearsal_paths(session_dir);
    let session: PackageRehearsalSession = load_json(&paths.session_path)?;
    let status: PackageRehearsalStatus = load_json(&paths.status_path)?;
    let mut mismatches = Vec::new();

    if session.session_dir != session_dir.display().to_string() {
        mismatches.push(
            "package rehearsal session_dir does not match the explicit session dir".to_string(),
        );
    }
    if status.session_dir != session_dir.display().to_string() {
        mismatches.push(
            "package rehearsal status session_dir does not match the explicit session dir"
                .to_string(),
        );
    }
    compare_string(
        &session.package_dir,
        &config.package_dir.display().to_string(),
        "package rehearsal package_dir",
        &mut mismatches,
    );
    compare_string(
        &session.install_root,
        &config.install_root.display().to_string(),
        "package rehearsal install_root",
        &mut mismatches,
    );
    compare_string(
        &session.target_service_name,
        &config.target_service_name,
        "package rehearsal target_service_name",
        &mut mismatches,
    );
    compare_string(
        &session.backend_command,
        &config.backend_command,
        "package rehearsal backend_command",
        &mut mismatches,
    );
    if session.wrapper_timeout_ms != config.wrapper_timeout_ms {
        mismatches.push(format!(
            "package rehearsal wrapper_timeout_ms {} does not match requested {}",
            session.wrapper_timeout_ms, config.wrapper_timeout_ms
        ));
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
    let mut activation_authorized = false;

    match status.result {
        PackageRehearsalResult::FailedBeforeInstall => {
            if package_verify_report.verdict == "tiny_live_package_deploy_verify_ok" {
                mismatches.push(
                    "package verify step is green but rehearsal result says failed_before_install"
                        .to_string(),
                );
            }
            if status.install_step.is_some()
                || status.verify_target_step.is_some()
                || status.cutover_plan_step.is_some()
            {
                mismatches.push(
                    "failed_before_install rehearsal must not contain install/verify/cutover steps"
                        .to_string(),
                );
            }
        }
        PackageRehearsalResult::FailedDuringInstall => {
            let install = load_required_package_deploy_report(
                &status.install_step,
                &paths.install_report_path,
                "install_step",
                &mut mismatches,
            )?;
            validate_package_deploy_report_contract(
                &install,
                config,
                "install step",
                &mut mismatches,
            );
            validate_installed_assets_match_package(
                &install,
                &config.package_dir,
                &config.install_root,
                &mut mismatches,
            );
            if install.verdict == "tiny_live_package_deploy_install_completed" {
                mismatches.push(
                    "install step is green but rehearsal result says failed_during_install"
                        .to_string(),
                );
            }
            if status.verify_target_step.is_some() || status.cutover_plan_step.is_some() {
                mismatches.push(
                    "failed_during_install rehearsal must not contain verify/cutover steps"
                        .to_string(),
                );
            }
        }
        PackageRehearsalResult::FailedDuringVerify => {
            let install = load_required_package_deploy_report(
                &status.install_step,
                &paths.install_report_path,
                "install_step",
                &mut mismatches,
            )?;
            validate_package_deploy_report_contract(
                &install,
                config,
                "install step",
                &mut mismatches,
            );
            validate_installed_assets_match_package(
                &install,
                &config.package_dir,
                &config.install_root,
                &mut mismatches,
            );
            if install.verdict != "tiny_live_package_deploy_install_completed" {
                mismatches.push(format!(
                    "install step verdict {} is not install completed for a failed_during_verify session",
                    install.verdict
                ));
            }
            let verify_report = load_required_install_target_report(
                &status.verify_target_step,
                &paths.verify_target_report_path,
                "verify_target_step",
                &mut mismatches,
            )?;
            validate_install_target_report_contract(
                &verify_report,
                config,
                "verify target step",
                &mut mismatches,
            );
            if verify_report.verdict == "tiny_live_install_target_verify_ok" {
                mismatches.push(
                    "verify target step is green but rehearsal result says failed_during_verify"
                        .to_string(),
                );
            }
            if status.cutover_plan_step.is_some() {
                mismatches.push(
                    "failed_during_verify rehearsal must not contain a cutover plan step"
                        .to_string(),
                );
            }
        }
        PackageRehearsalResult::FailedDuringCutoverPlan | PackageRehearsalResult::Completed => {
            let install = load_required_package_deploy_report(
                &status.install_step,
                &paths.install_report_path,
                "install_step",
                &mut mismatches,
            )?;
            validate_package_deploy_report_contract(
                &install,
                config,
                "install step",
                &mut mismatches,
            );
            validate_installed_assets_match_package(
                &install,
                &config.package_dir,
                &config.install_root,
                &mut mismatches,
            );
            if install.verdict != "tiny_live_package_deploy_install_completed" {
                mismatches.push(format!(
                    "install step verdict {} is not install completed for a later-stage rehearsal result",
                    install.verdict
                ));
            }
            let verify_report = load_required_install_target_report(
                &status.verify_target_step,
                &paths.verify_target_report_path,
                "verify_target_step",
                &mut mismatches,
            )?;
            validate_install_target_report_contract(
                &verify_report,
                config,
                "verify target step",
                &mut mismatches,
            );
            if verify_report.verdict != "tiny_live_install_target_verify_ok" {
                mismatches.push(format!(
                    "verify target step verdict {} is not verify ok for a later-stage rehearsal result",
                    verify_report.verdict
                ));
            }
            let cutover = load_required_package_deploy_report(
                &status.cutover_plan_step,
                &paths.cutover_plan_report_path,
                "cutover_plan_step",
                &mut mismatches,
            )?;
            validate_package_deploy_report_contract(
                &cutover,
                config,
                "cutover plan step",
                &mut mismatches,
            );
            current_pre_activation_gate_verdict =
                cutover.current_pre_activation_gate_verdict.clone();
            current_pre_activation_gate_reason = cutover.current_pre_activation_gate_reason.clone();
            activation_authorized = cutover.activation_authorized;
            match status.result {
                PackageRehearsalResult::Completed => {
                    if cutover.verdict != "tiny_live_package_cutover_plan_ready" {
                        mismatches.push(format!(
                            "cutover plan step verdict {} is not package cutover plan ready for a completed rehearsal",
                            cutover.verdict
                        ));
                    }
                }
                PackageRehearsalResult::FailedDuringCutoverPlan => {
                    if cutover.verdict == "tiny_live_package_cutover_plan_ready" {
                        mismatches.push(
                            "cutover plan step is green but rehearsal result says failed_during_cutover_plan"
                                .to_string(),
                        );
                    }
                }
                _ => {}
            }
        }
    }

    let verdict = if mismatches.is_empty() {
        TinyLivePackageRehearsalVerdict::TinyLivePackageRehearsalVerifyOk
    } else {
        TinyLivePackageRehearsalVerdict::TinyLivePackageRehearsalVerifyInvalid
    };
    let reason = if mismatches.is_empty() {
        format!(
            "package rehearsal session under {} is coherent and bound to the requested package/target contract",
            session_dir.display()
        )
    } else {
        mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "package rehearsal verification failed".to_string())
    };

    Ok(PackageRehearsalReport {
        generated_at: Utc::now(),
        mode: "verify_package_rehearsal".to_string(),
        verdict,
        reason,
        package_dir: config.package_dir.display().to_string(),
        install_root: config.install_root.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        wrapper_timeout_ms: config.wrapper_timeout_ms,
        session_dir: Some(session_dir.display().to_string()),
        rehearsal_session_path: Some(paths.session_path.display().to_string()),
        rehearsal_status_path: Some(paths.status_path.display().to_string()),
        result: Some(serialize_enum(&status.result)),
        package_verify_step: status.package_verify_step,
        install_step: status.install_step,
        verify_target_step: status.verify_target_step,
        cutover_plan_step: status.cutover_plan_step,
        package_verify_command_summary: package_verify_command_summary(config),
        install_from_package_command_summary: install_from_package_command_summary(config),
        verify_install_target_command_summary: verify_install_target_command_summary(config),
        package_cutover_plan_command_summary: package_cutover_plan_command_summary(config),
        current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason,
        verification_mismatches: mismatches,
        activation_authorized,
        explicit_statement:
            "this package-native rehearsal verification checks immutable package handoff evidence only; it does not authorize production activation"
                .to_string(),
    })
}

fn base_report(
    config: &Config,
    verdict: TinyLivePackageRehearsalVerdict,
    reason: String,
) -> PackageRehearsalReport {
    PackageRehearsalReport {
        generated_at: Utc::now(),
        mode: mode_name(config.mode).to_string(),
        verdict,
        reason,
        package_dir: config.package_dir.display().to_string(),
        install_root: config.install_root.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        wrapper_timeout_ms: config.wrapper_timeout_ms,
        session_dir: config
            .session_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        rehearsal_session_path: config
            .session_dir
            .as_ref()
            .map(|path| package_rehearsal_paths(path).session_path.display().to_string()),
        rehearsal_status_path: config
            .session_dir
            .as_ref()
            .map(|path| package_rehearsal_paths(path).status_path.display().to_string()),
        result: None,
        package_verify_step: None,
        install_step: None,
        verify_target_step: None,
        cutover_plan_step: None,
        package_verify_command_summary: package_verify_command_summary(config),
        install_from_package_command_summary: install_from_package_command_summary(config),
        verify_install_target_command_summary: verify_install_target_command_summary(config),
        package_cutover_plan_command_summary: package_cutover_plan_command_summary(config),
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        verification_mismatches: Vec::new(),
        activation_authorized: false,
        explicit_statement:
            "this package-native rehearsal layer proves bounded package handoff mechanics only; it does not authorize or perform real production activation by itself"
                .to_string(),
    }
}

fn report_from_status(
    config: &Config,
    session_dir: &Path,
    verdict: TinyLivePackageRehearsalVerdict,
    status: &PackageRehearsalStatus,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
) -> PackageRehearsalReport {
    let paths = package_rehearsal_paths(session_dir);
    PackageRehearsalReport {
        generated_at: Utc::now(),
        mode: "run_package_rehearsal".to_string(),
        verdict,
        reason: status.reason.clone(),
        package_dir: config.package_dir.display().to_string(),
        install_root: config.install_root.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        wrapper_timeout_ms: config.wrapper_timeout_ms,
        session_dir: Some(session_dir.display().to_string()),
        rehearsal_session_path: Some(paths.session_path.display().to_string()),
        rehearsal_status_path: Some(paths.status_path.display().to_string()),
        result: Some(serialize_enum(&status.result)),
        package_verify_step: status.package_verify_step.clone(),
        install_step: status.install_step.clone(),
        verify_target_step: status.verify_target_step.clone(),
        cutover_plan_step: status.cutover_plan_step.clone(),
        package_verify_command_summary: package_verify_command_summary(config),
        install_from_package_command_summary: install_from_package_command_summary(config),
        verify_install_target_command_summary: verify_install_target_command_summary(config),
        package_cutover_plan_command_summary: package_cutover_plan_command_summary(config),
        current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason,
        verification_mismatches: Vec::new(),
        activation_authorized: false,
        explicit_statement:
            "this package-native rehearsal session is fake-root evidence only; it does not authorize production activation"
                .to_string(),
    }
}

fn mode_name(mode: Mode) -> &'static str {
    match mode {
        Mode::PlanPackageRehearsal => "plan_package_rehearsal",
        Mode::RenderPackageRehearsalScript => "render_package_rehearsal_script",
        Mode::RunPackageRehearsal => "run_package_rehearsal",
        Mode::VerifyPackageRehearsal => "verify_package_rehearsal",
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

fn install_from_package_command_summary(config: &Config) -> String {
    format!(
        "copybot_tiny_live_activation_package_deploy --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --install-from-package",
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

fn planned_session(config: &Config, session_dir: &Path) -> PackageRehearsalSession {
    PackageRehearsalSession {
        session_version: REHEARSAL_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        package_dir: config.package_dir.display().to_string(),
        install_root: config.install_root.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        wrapper_timeout_ms: config.wrapper_timeout_ms,
        session_dir: session_dir.display().to_string(),
        package_verify_command_summary: package_verify_command_summary(config),
        install_from_package_command_summary: install_from_package_command_summary(config),
        verify_install_target_command_summary: verify_install_target_command_summary(config),
        package_cutover_plan_command_summary: package_cutover_plan_command_summary(config),
        explicit_statement:
            "this package-native rehearsal session binds one immutable package plus an explicit fake target contract; it does not authorize production activation"
                .to_string(),
    }
}

fn step_artifact(
    path: &Path,
    mode: &str,
    verdict: &str,
    reason: &str,
    generated_at: DateTime<Utc>,
) -> PackageRehearsalStepArtifact {
    PackageRehearsalStepArtifact {
        path: path.display().to_string(),
        mode: mode.to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        generated_at,
    }
}

fn package_rehearsal_paths(session_dir: &Path) -> PackageRehearsalPaths {
    PackageRehearsalPaths {
        session_path: session_dir.join("tiny_live_activation_package_rehearsal.session.json"),
        status_path: session_dir.join("tiny_live_activation_package_rehearsal.status.json"),
        package_verify_report_path: session_dir
            .join("tiny_live_activation_package_rehearsal.package_verify.report.json"),
        install_report_path: session_dir
            .join("tiny_live_activation_package_rehearsal.install.report.json"),
        verify_target_report_path: session_dir
            .join("tiny_live_activation_package_rehearsal.verify_target.report.json"),
        cutover_plan_report_path: session_dir
            .join("tiny_live_activation_package_rehearsal.cutover_plan.report.json"),
    }
}

fn ensure_clean_package_rehearsal_session_dir(session_dir: &Path) -> Result<()> {
    let paths = package_rehearsal_paths(session_dir);
    for artifact in [
        &paths.session_path,
        &paths.status_path,
        &paths.package_verify_report_path,
        &paths.install_report_path,
        &paths.verify_target_report_path,
        &paths.cutover_plan_report_path,
    ] {
        if artifact.exists() {
            bail!(
                "refusing to reuse package rehearsal session dir {}; artifact {} already exists",
                session_dir.display(),
                artifact.display()
            );
        }
    }
    fs::create_dir_all(session_dir)
        .with_context(|| format!("failed creating session dir {}", session_dir.display()))
}

fn validate_fake_rehearsal_contract(config: &Config) -> Result<()> {
    validate_path_within_temp_root(&config.install_root, "install root", true)?;
    if let Some(session_dir) = &config.session_dir {
        validate_path_within_temp_root(session_dir, "session dir", false)?;
    }
    Ok(())
}

fn validate_path_within_temp_root(path: &Path, label: &str, require_exists: bool) -> Result<()> {
    if !path.is_absolute() {
        bail!("{label} must be an absolute path");
    }
    let temp_root = fs::canonicalize(env::temp_dir()).with_context(|| {
        format!(
            "failed canonicalizing temp dir {}",
            env::temp_dir().display()
        )
    })?;
    let anchor = find_existing_anchor(path, label)?;
    let anchor_real = fs::canonicalize(&anchor)
        .with_context(|| format!("failed canonicalizing {label} anchor {}", anchor.display()))?;
    if !anchor_real.starts_with(&temp_root) {
        bail!(
            "{label} {} resolves outside temp root {}",
            path.display(),
            temp_root.display()
        );
    }
    if require_exists && anchor != path {
        bail!("{label} {} must already exist", path.display());
    }
    reject_descendant_symlinks(path, &anchor, label)?;
    if path.exists() {
        let metadata = fs::symlink_metadata(path)
            .with_context(|| format!("failed reading {label} metadata {}", path.display()))?;
        if metadata.file_type().is_symlink() {
            bail!("{label} {} must not be a symlink", path.display());
        }
        let real = fs::canonicalize(path)
            .with_context(|| format!("failed canonicalizing {label} {}", path.display()))?;
        if !real.starts_with(&temp_root) {
            bail!(
                "{label} {} resolves outside temp root {}",
                path.display(),
                temp_root.display()
            );
        }
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
    step: &Option<PackageRehearsalStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<PackageDeployReportView> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("missing required rehearsal step {label}"))?;
    load_bound_step_json(step, expected_path, label, mismatches)
}

fn load_required_install_target_report(
    step: &Option<PackageRehearsalStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<InstallTargetReportView> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("missing required rehearsal step {label}"))?;
    load_bound_step_json(step, expected_path, label, mismatches)
}

fn load_bound_step_json<T: for<'de> Deserialize<'de>>(
    step: &PackageRehearsalStepArtifact,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<T> {
    let expected = expected_path.display().to_string();
    if step.path != expected {
        mismatches.push(format!(
            "package rehearsal {label} path {} does not match deterministic session artifact {}",
            step.path, expected
        ));
    }
    load_json(expected_path).with_context(|| {
        format!(
            "failed reading deterministic package rehearsal {label} report {}",
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
    if let Some(verification) = &report.verification {
        if !verification.mismatches.is_empty()
            && report.verdict == "tiny_live_install_target_verify_ok"
        {
            mismatches.push(format!(
                "{label} claims verify ok but still carries verification mismatches"
            ));
        }
    }
}

fn validate_installed_assets_match_package(
    install_report: &PackageDeployReportView,
    package_dir: &Path,
    install_root: &Path,
    mismatches: &mut Vec<String>,
) {
    if let Some(path) = &install_report.packaged_wrapper_path {
        ensure_path_is_under_root(Path::new(path), package_dir, "packaged wrapper", mismatches);
    } else {
        mismatches.push("install step missing packaged_wrapper_path".to_string());
    }
    if let Some(path) = &install_report.packaged_activation_config_path {
        ensure_path_is_under_root(
            Path::new(path),
            package_dir,
            "packaged activation artifact",
            mismatches,
        );
    } else {
        mismatches.push("install step missing packaged_activation_config_path".to_string());
    }
    if let Some(path) = &install_report.packaged_rollback_config_path {
        ensure_path_is_under_root(
            Path::new(path),
            package_dir,
            "packaged rollback artifact",
            mismatches,
        );
    } else {
        mismatches.push("install step missing packaged_rollback_config_path".to_string());
    }
    if let Some(path) = &install_report.installed_wrapper_path {
        ensure_path_is_under_root(
            Path::new(path),
            install_root,
            "installed wrapper",
            mismatches,
        );
    } else {
        mismatches.push("install step missing installed_wrapper_path".to_string());
    }
    if let Some(path) = &install_report.installed_activation_config_path {
        ensure_path_is_under_root(
            Path::new(path),
            install_root,
            "installed activation artifact",
            mismatches,
        );
    } else {
        mismatches.push("install step missing installed_activation_config_path".to_string());
    }
    if let Some(path) = &install_report.installed_rollback_config_path {
        ensure_path_is_under_root(
            Path::new(path),
            install_root,
            "installed rollback artifact",
            mismatches,
        );
    } else {
        mismatches.push("install step missing installed_rollback_config_path".to_string());
    }
    if let (Some(packaged_wrapper), Some(installed_wrapper)) = (
        install_report.packaged_wrapper_path.as_deref(),
        install_report.installed_wrapper_path.as_deref(),
    ) {
        compare_file_bytes(
            Path::new(packaged_wrapper),
            Path::new(installed_wrapper),
            "packaged wrapper",
            "installed wrapper",
            mismatches,
        );
    }
    if let (Some(packaged_activation), Some(installed_activation)) = (
        install_report.packaged_activation_config_path.as_deref(),
        install_report.installed_activation_config_path.as_deref(),
    ) {
        compare_file_bytes(
            Path::new(packaged_activation),
            Path::new(installed_activation),
            "packaged activation artifact",
            "installed activation artifact",
            mismatches,
        );
    }
    if let (Some(packaged_rollback), Some(installed_rollback)) = (
        install_report.packaged_rollback_config_path.as_deref(),
        install_report.installed_rollback_config_path.as_deref(),
    ) {
        compare_file_bytes(
            Path::new(packaged_rollback),
            Path::new(installed_rollback),
            "packaged rollback artifact",
            "installed rollback artifact",
            mismatches,
        );
    }
}

fn compare_file_bytes(
    left: &Path,
    right: &Path,
    left_label: &str,
    right_label: &str,
    mismatches: &mut Vec<String>,
) {
    match (fs::read(left), fs::read(right)) {
        (Ok(left_bytes), Ok(right_bytes)) => {
            if left_bytes != right_bytes {
                mismatches.push(format!(
                    "{left_label} {} does not match {right_label} {}",
                    left.display(),
                    right.display()
                ));
            }
        }
        (Err(error), _) => mismatches.push(format!(
            "failed reading {left_label} {}: {error:#}",
            left.display()
        )),
        (_, Err(error)) => mismatches.push(format!(
            "failed reading {right_label} {}: {error:#}",
            right.display()
        )),
    }
}

fn ensure_path_is_under_root(path: &Path, root: &Path, label: &str, mismatches: &mut Vec<String>) {
    if !path.starts_with(root) {
        mismatches.push(format!(
            "{label} {} does not stay under expected root {}",
            path.display(),
            root.display()
        ));
    }
}

fn compare_string(actual: &str, expected: &str, label: &str, mismatches: &mut Vec<String>) {
    if actual != expected {
        mismatches.push(format!(
            "{label} {actual} does not match expected {expected}"
        ));
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

fn render_human_report(report: &PackageRehearsalReport) -> String {
    let mut lines = vec![
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("package_dir={}", report.package_dir),
        format!("install_root={}", report.install_root),
        format!("target_service_name={}", report.target_service_name),
        format!("backend_command={}", report.backend_command),
        format!("wrapper_timeout_ms={}", report.wrapper_timeout_ms),
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
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn coherent_package_and_matching_target_contract_complete_rehearsal() {
        let fixture = rehearsal_fixture("tiny_live_package_rehearsal_completed", GateState::Green);
        let report = run_package_rehearsal_report(&fixture.config).expect("run rehearsal");
        assert_eq!(
            report.verdict,
            TinyLivePackageRehearsalVerdict::TinyLivePackageRehearsalCompleted
        );
        assert_eq!(report.result.as_deref(), Some("completed"));
    }

    #[test]
    fn tampered_package_hash_fails_before_install() {
        let fixture = rehearsal_fixture("tiny_live_package_rehearsal_bad_hash", GateState::Green);
        fs::write(
            fixture
                .package_dir
                .join("artifacts/rendered.activation.toml"),
            sample_safe_config_toml(&fixture.db_path),
        )
        .unwrap();
        let report = run_package_rehearsal_report(&fixture.config).expect("run rehearsal");
        assert_eq!(
            report.verdict,
            TinyLivePackageRehearsalVerdict::TinyLivePackageRehearsalFailedBeforeInstall
        );
    }

    #[test]
    fn tampered_packaged_wrapper_fails_during_install() {
        let fixture =
            rehearsal_fixture("tiny_live_package_rehearsal_bad_wrapper", GateState::Green);
        let packaged_wrapper_path = fixture
            .package_dir
            .join("wrapper/copybot-live-service-control");
        let report = run_package_rehearsal_report_with_hooks(
            &fixture.config,
            Some(&|| {
                fs::write(
                    &packaged_wrapper_path,
                    "#!/usr/bin/env bash\necho tampered\n",
                )?;
                Ok(())
            }),
            None,
        )
        .expect("run rehearsal");
        assert_eq!(
            report.verdict,
            TinyLivePackageRehearsalVerdict::TinyLivePackageRehearsalFailedDuringInstall
        );
    }

    #[test]
    fn verify_install_target_mismatch_fails_during_verify() {
        let fixture = rehearsal_fixture("tiny_live_package_rehearsal_bad_verify", GateState::Green);
        let report = run_package_rehearsal_report_with_hooks(
            &fixture.config,
            None,
            Some(&|install| {
                let runtime_dir = install
                    .runtime_dir
                    .as_ref()
                    .expect("install runtime dir")
                    .clone();
                fs::remove_dir_all(&runtime_dir)?;
                Ok(())
            }),
        )
        .expect("run rehearsal");
        assert_eq!(
            report.verdict,
            TinyLivePackageRehearsalVerdict::TinyLivePackageRehearsalFailedDuringVerify
        );
    }

    #[test]
    fn package_cutover_plan_blocked_by_current_non_green_gate_is_explicit() {
        let fixture = rehearsal_fixture(
            "tiny_live_package_rehearsal_cutover_blocked",
            GateState::Stage3Blocked,
        );
        let report = run_package_rehearsal_report(&fixture.config).expect("run rehearsal");
        assert_eq!(
            report.verdict,
            TinyLivePackageRehearsalVerdict::TinyLivePackageRehearsalFailedDuringCutoverPlan
        );
        assert!(matches!(
            report.current_pre_activation_gate_verdict.as_deref(),
            Some("blocked_by_stage3") | Some("blocked_by_pre_activation_gate")
        ));
    }

    #[test]
    fn verify_package_rehearsal_rejects_tampered_step_path() {
        let fixture = rehearsal_fixture(
            "tiny_live_package_rehearsal_tampered_path",
            GateState::Green,
        );
        run_package_rehearsal_report(&fixture.config).expect("run rehearsal");
        let session_dir = fixture.config.session_dir.as_ref().unwrap();
        let paths = package_rehearsal_paths(session_dir);
        let mut status: PackageRehearsalStatus = load_json(&paths.status_path).unwrap();
        status.cutover_plan_step.as_mut().unwrap().path =
            fixture.fixture_dir.join("other.json").display().to_string();
        persist_json(&paths.status_path, &status).unwrap();
        let verify = verify_package_rehearsal_report(&fixture.config).expect("verify rehearsal");
        assert_eq!(
            verify.verdict,
            TinyLivePackageRehearsalVerdict::TinyLivePackageRehearsalVerifyInvalid
        );
    }

    #[test]
    fn verify_package_rehearsal_rejects_mismatched_target_contract() {
        let fixture =
            rehearsal_fixture("tiny_live_package_rehearsal_wrong_target", GateState::Green);
        run_package_rehearsal_report(&fixture.config).expect("run rehearsal");
        let verify = verify_package_rehearsal_report(&Config {
            target_service_name: "other.service".to_string(),
            ..fixture.config.clone()
        })
        .expect("verify rehearsal");
        assert_eq!(
            verify.verdict,
            TinyLivePackageRehearsalVerdict::TinyLivePackageRehearsalVerifyInvalid
        );
    }

    struct RehearsalFixture {
        fixture_dir: PathBuf,
        package_dir: PathBuf,
        db_path: PathBuf,
        config: Config,
        _rpc_server: MockHttpServer,
        _adapter_server: MockHttpServer,
    }

    #[derive(Debug, Clone, Copy)]
    enum GateState {
        Green,
        Stage3Blocked,
    }

    fn rehearsal_fixture(label: &str, gate_state: GateState) -> RehearsalFixture {
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
            "systemctl",
            tiny_live_activation_live_execute::live_service_control_wrapper_contract::DEFAULT_TIMEOUT_MS,
            &activation_config_source_path,
            &rollback_config_source_path,
        )
        .expect("export package");

        RehearsalFixture {
            fixture_dir: fixture_dir.clone(),
            package_dir: package_dir.clone(),
            db_path,
            config: Config {
                mode: Mode::RunPackageRehearsal,
                package_dir,
                install_root,
                target_service_name: "solana-copy-bot.service".to_string(),
                backend_command: "systemctl".to_string(),
                wrapper_timeout_ms: tiny_live_activation_live_execute::live_service_control_wrapper_contract::DEFAULT_TIMEOUT_MS,
                session_dir: Some(fixture_dir.join("rehearsal-session")),
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
        if matches!(gate_state, GateState::Green) {
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
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("{label}_{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
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
