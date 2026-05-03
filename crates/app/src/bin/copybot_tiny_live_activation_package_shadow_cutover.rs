use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_cutover.rs"]
mod tiny_live_activation_cutover;
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

const USAGE: &str = "usage: copybot_tiny_live_activation_package_shadow_cutover --package-dir <path> --shadow-install-root <path> --live-install-root <path> --shadow-target-service <name> --backend-command <path-or-name> [--wrapper-timeout-ms <ms>] [--session-dir <path>] [--output <path>] [--json] (--plan-package-shadow-cutover | --render-package-shadow-cutover-script | --run-package-shadow-cutover | --verify-package-shadow-cutover)";
const SHADOW_SESSION_VERSION: &str = "1";
const SHADOW_STATUS_VERSION: &str = "1";
const DEFAULT_STARTUP_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_ROLLBACK_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_SAMPLE_CADENCE_MS: u64 = 1_000;
const DEFAULT_MAX_OBSERVATION_STALENESS_MS: u64 = 60_000;

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
    shadow_install_root: PathBuf,
    live_install_root: PathBuf,
    shadow_target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    session_dir: Option<PathBuf>,
    output_path: Option<PathBuf>,
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    PlanPackageShadowCutover,
    RenderPackageShadowCutoverScript,
    RunPackageShadowCutover,
    VerifyPackageShadowCutover,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageShadowCutoverVerdict {
    TinyLivePackageShadowCutoverPlanReady,
    TinyLivePackageShadowCutoverScriptRendered,
    TinyLivePackageShadowCutoverCompletedKeepRunning,
    TinyLivePackageShadowCutoverCompletedWithRollback,
    TinyLivePackageShadowCutoverFailedBeforeInstall,
    TinyLivePackageShadowCutoverFailedBeforeApply,
    TinyLivePackageShadowCutoverFailedDuringWatch,
    TinyLivePackageShadowCutoverVerifyOk,
    TinyLivePackageShadowCutoverVerifyInvalid,
    TinyLivePackageShadowCutoverRefusedUnsafeShadowTarget,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum PackageShadowCutoverResult {
    CompletedKeepRunning,
    CompletedWithRollback,
    FailedBeforeInstall,
    FailedBeforeApply,
    FailedDuringWatch,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageShadowCutoverStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageShadowCutoverSession {
    session_version: String,
    planned_at: DateTime<Utc>,
    package_dir: String,
    shadow_install_root: String,
    live_install_root: String,
    shadow_target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    session_dir: String,
    nested_cutover_session_dir: String,
    package_verify_command_summary: String,
    install_from_package_command_summary: String,
    verify_install_target_command_summary: String,
    backup_command_summary: String,
    run_shadow_cutover_command_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageShadowCutoverStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    session_dir: String,
    nested_cutover_session_dir: String,
    result: PackageShadowCutoverResult,
    reason: String,
    package_verify_step: Option<PackageShadowCutoverStepArtifact>,
    install_step: Option<PackageShadowCutoverStepArtifact>,
    verify_target_step: Option<PackageShadowCutoverStepArtifact>,
    backup_step: Option<PackageShadowCutoverStepArtifact>,
    shadow_cutover_step: Option<PackageShadowCutoverStepArtifact>,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackageShadowCutoverPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    package_verify_report_path: PathBuf,
    install_report_path: PathBuf,
    verify_target_report_path: PathBuf,
    backup_report_path: PathBuf,
    shadow_cutover_report_path: PathBuf,
    nested_cutover_session_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageShadowCutoverReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageShadowCutoverVerdict,
    reason: String,
    package_dir: String,
    shadow_install_root: String,
    live_install_root: String,
    shadow_target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    session_dir: Option<String>,
    shadow_cutover_session_path: Option<String>,
    shadow_cutover_status_path: Option<String>,
    nested_cutover_session_dir: Option<String>,
    result: Option<String>,
    package_verify_step: Option<PackageShadowCutoverStepArtifact>,
    install_step: Option<PackageShadowCutoverStepArtifact>,
    verify_target_step: Option<PackageShadowCutoverStepArtifact>,
    backup_step: Option<PackageShadowCutoverStepArtifact>,
    shadow_cutover_step: Option<PackageShadowCutoverStepArtifact>,
    package_verify_command_summary: String,
    install_from_package_command_summary: String,
    verify_install_target_command_summary: String,
    backup_command_summary: String,
    run_shadow_cutover_command_summary: String,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
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
    packaged_rollback_config_path: Option<String>,
    packaged_wrapper_path: Option<String>,
    installed_wrapper_path: Option<String>,
    installed_target_config_path: Option<String>,
    installed_activation_config_path: Option<String>,
    installed_rollback_config_path: Option<String>,
    runtime_dir: Option<String>,
    backup_dir: Option<String>,
    session_dir: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    activation_authorized: bool,
}

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

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct CutoverReportView {
    verdict: String,
    reason: String,
    activation_config_path: String,
    rollback_config_path: String,
    runtime_dir: String,
    target_config_path: String,
    target_service_name: String,
    service_control_command_path: String,
    backup_dir: String,
    session_dir: Option<String>,
    cutover_session_path: Option<String>,
    cutover_status_path: Option<String>,
    live_result: Option<String>,
    decision: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    activation_authorized: bool,
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
    let mut shadow_install_root: Option<PathBuf> = None;
    let mut live_install_root: Option<PathBuf> = None;
    let mut shadow_target_service_name: Option<String> = None;
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
            "--shadow-install-root" => {
                shadow_install_root = Some(PathBuf::from(parse_string_arg(
                    "--shadow-install-root",
                    args.next(),
                )?))
            }
            "--live-install-root" => {
                live_install_root = Some(PathBuf::from(parse_string_arg(
                    "--live-install-root",
                    args.next(),
                )?))
            }
            "--shadow-target-service" => {
                shadow_target_service_name =
                    Some(parse_string_arg("--shadow-target-service", args.next())?)
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
            "--plan-package-shadow-cutover" => set_mode(
                &mut mode,
                Mode::PlanPackageShadowCutover,
                "--plan-package-shadow-cutover",
            )?,
            "--render-package-shadow-cutover-script" => set_mode(
                &mut mode,
                Mode::RenderPackageShadowCutoverScript,
                "--render-package-shadow-cutover-script",
            )?,
            "--run-package-shadow-cutover" => set_mode(
                &mut mode,
                Mode::RunPackageShadowCutover,
                "--run-package-shadow-cutover",
            )?,
            "--verify-package-shadow-cutover" => set_mode(
                &mut mode,
                Mode::VerifyPackageShadowCutover,
                "--verify-package-shadow-cutover",
            )?,
            "--help" | "-h" => return Ok(None),
            other => bail!("unrecognized argument: {other}"),
        }
    }

    let mode = mode.ok_or_else(|| anyhow!("missing required mode"))?;
    let package_dir = package_dir.ok_or_else(|| anyhow!("missing required --package-dir"))?;
    let shadow_install_root =
        shadow_install_root.ok_or_else(|| anyhow!("missing required --shadow-install-root"))?;
    let live_install_root =
        live_install_root.ok_or_else(|| anyhow!("missing required --live-install-root"))?;
    let shadow_target_service_name = shadow_target_service_name
        .ok_or_else(|| anyhow!("missing required --shadow-target-service"))?;
    let backend_command =
        backend_command.ok_or_else(|| anyhow!("missing required --backend-command"))?;
    if matches!(
        mode,
        Mode::RunPackageShadowCutover | Mode::VerifyPackageShadowCutover
    ) && session_dir.is_none()
    {
        bail!("missing required --session-dir for run/verify package shadow cutover");
    }
    if matches!(mode, Mode::RenderPackageShadowCutoverScript) && output_path.is_none() {
        bail!("missing required --output for --render-package-shadow-cutover-script");
    }

    Ok(Some(Config {
        mode,
        package_dir,
        shadow_install_root,
        live_install_root,
        shadow_target_service_name,
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
        Mode::PlanPackageShadowCutover => plan_package_shadow_cutover_report(&config)?,
        Mode::RenderPackageShadowCutoverScript => {
            render_package_shadow_cutover_script_report(&config)?
        }
        Mode::RunPackageShadowCutover => run_package_shadow_cutover_report(&config)?,
        Mode::VerifyPackageShadowCutover => verify_package_shadow_cutover_report(&config)?,
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_human_report(&report))
    }
}

fn plan_package_shadow_cutover_report(config: &Config) -> Result<PackageShadowCutoverReport> {
    if let Some(report) = shadow_target_validation_refusal_report(config, false)? {
        return Ok(report);
    }
    let package_verify = tiny_live_activation_package_deploy::run_mode_for_rehearsal(
        &config.package_dir,
        &config.shadow_install_root,
        &config.shadow_target_service_name,
        &config.backend_command,
        config.wrapper_timeout_ms,
        tiny_live_activation_package_deploy::RehearsalMode::VerifyPackageDeployTarget,
    )?;
    if package_verify.verdict != "tiny_live_package_deploy_verify_ok" {
        return Ok(base_report(
            config,
            TinyLivePackageShadowCutoverVerdict::TinyLivePackageShadowCutoverVerifyInvalid,
            package_verify.reason,
        ));
    }
    Ok(base_report(
        config,
        TinyLivePackageShadowCutoverVerdict::TinyLivePackageShadowCutoverPlanReady,
        format!(
            "package {} is ready for a bounded shadow cutover rehearsal over {} using {} as the sole immutable handoff input",
            config.package_dir.display(),
            config.shadow_install_root.display(),
            config.package_dir.display()
        ),
    ))
}

fn render_package_shadow_cutover_script_report(
    config: &Config,
) -> Result<PackageShadowCutoverReport> {
    let plan = plan_package_shadow_cutover_report(config)?;
    if plan.verdict != TinyLivePackageShadowCutoverVerdict::TinyLivePackageShadowCutoverPlanReady {
        return Ok(plan);
    }
    let output_path = config
        .output_path
        .as_ref()
        .ok_or_else(|| anyhow!("missing --output for --render-package-shadow-cutover-script"))?;
    ensure_new_output_path(output_path)?;
    let script = format!(
        "#!/usr/bin/env bash\nset -euo pipefail\nif [ \"$#\" -ne 1 ]; then\n  echo \"usage: $(basename \"$0\") <session-dir>\" >&2\n  exit 64\nfi\ncopybot_tiny_live_activation_package_shadow_cutover --package-dir {} --shadow-install-root {} --live-install-root {} --shadow-target-service {} --backend-command {} --wrapper-timeout-ms {} --session-dir \"$1\" --run-package-shadow-cutover\n",
        shell_single_quote(&config.package_dir.display().to_string()),
        shell_single_quote(&config.shadow_install_root.display().to_string()),
        shell_single_quote(&config.live_install_root.display().to_string()),
        shell_single_quote(&config.shadow_target_service_name),
        shell_single_quote(&config.backend_command),
        config.wrapper_timeout_ms,
    );
    write_text_file(output_path, &script)?;
    #[cfg(unix)]
    set_executable(output_path)?;
    Ok(PackageShadowCutoverReport {
        generated_at: Utc::now(),
        mode: "render_package_shadow_cutover_script".to_string(),
        verdict: TinyLivePackageShadowCutoverVerdict::TinyLivePackageShadowCutoverScriptRendered,
        reason: format!(
            "package-native shadow cutover script rendered to {} without mutating the shadow or live targets",
            output_path.display()
        ),
        explicit_statement:
            "rendering the package-native shadow cutover script does not authorize or perform production activation by itself"
                .to_string(),
        ..plan
    })
}

fn run_package_shadow_cutover_report(config: &Config) -> Result<PackageShadowCutoverReport> {
    run_package_shadow_cutover_report_with_hooks(
        config,
        None,
        None,
        tiny_live_activation_cutover::CutoverRehearsalObservationSeed::default(),
    )
}

fn run_package_shadow_cutover_report_with_hooks(
    config: &Config,
    before_install_hook: Option<&dyn Fn() -> Result<()>>,
    after_install_hook: Option<
        &dyn Fn(&tiny_live_activation_package_deploy::PackageDeployRehearsalStep) -> Result<()>,
    >,
    observation_seed: tiny_live_activation_cutover::CutoverRehearsalObservationSeed,
) -> Result<PackageShadowCutoverReport> {
    if let Some(report) = shadow_target_validation_refusal_report(config, true)? {
        return Ok(report);
    }
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for --run-package-shadow-cutover"))?;
    ensure_clean_shadow_cutover_session_dir(session_dir)?;
    let paths = package_shadow_cutover_paths(session_dir);
    let session = planned_session(config, session_dir, &paths.nested_cutover_session_dir);
    persist_json(&paths.session_path, &session)?;

    let package_verify = tiny_live_activation_package_deploy::run_mode_for_rehearsal(
        &config.package_dir,
        &config.shadow_install_root,
        &config.shadow_target_service_name,
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
        let status = PackageShadowCutoverStatus {
            status_version: SHADOW_STATUS_VERSION.to_string(),
            updated_at: Utc::now(),
            session_dir: session_dir.display().to_string(),
            nested_cutover_session_dir: paths.nested_cutover_session_dir.display().to_string(),
            result: PackageShadowCutoverResult::FailedBeforeInstall,
            reason: package_verify.reason.clone(),
            package_verify_step: package_verify_step.clone(),
            install_step: None,
            verify_target_step: None,
            backup_step: None,
            shadow_cutover_step: None,
            explicit_statement:
                "this package-native shadow cutover session is bounded shadow evidence only; it does not authorize production activation"
                    .to_string(),
        };
        persist_json(&paths.status_path, &status)?;
        return Ok(report_from_status(
            config,
            session_dir,
            TinyLivePackageShadowCutoverVerdict::TinyLivePackageShadowCutoverFailedBeforeInstall,
            &status,
            None,
            None,
            false,
        ));
    }
    if let Some(hook) = before_install_hook {
        hook()?;
    }

    let install = tiny_live_activation_package_deploy::run_mode_for_rehearsal(
        &config.package_dir,
        &config.shadow_install_root,
        &config.shadow_target_service_name,
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
        let status = PackageShadowCutoverStatus {
            status_version: SHADOW_STATUS_VERSION.to_string(),
            updated_at: Utc::now(),
            session_dir: session_dir.display().to_string(),
            nested_cutover_session_dir: paths.nested_cutover_session_dir.display().to_string(),
            result: PackageShadowCutoverResult::FailedBeforeInstall,
            reason: install.reason.clone(),
            package_verify_step: package_verify_step.clone(),
            install_step: install_step.clone(),
            verify_target_step: None,
            backup_step: None,
            shadow_cutover_step: None,
            explicit_statement:
                "this package-native shadow cutover session is bounded shadow evidence only; it does not authorize production activation"
                    .to_string(),
        };
        persist_json(&paths.status_path, &status)?;
        return Ok(report_from_status(
            config,
            session_dir,
            TinyLivePackageShadowCutoverVerdict::TinyLivePackageShadowCutoverFailedBeforeInstall,
            &status,
            None,
            None,
            false,
        ));
    }
    if let Some(hook) = after_install_hook {
        hook(&install)?;
    }

    let verify_target = tiny_live_activation_install_target::verify_install_target_for_rehearsal(
        &config.shadow_install_root,
        &config.shadow_target_service_name,
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
        let status = PackageShadowCutoverStatus {
            status_version: SHADOW_STATUS_VERSION.to_string(),
            updated_at: Utc::now(),
            session_dir: session_dir.display().to_string(),
            nested_cutover_session_dir: paths.nested_cutover_session_dir.display().to_string(),
            result: PackageShadowCutoverResult::FailedBeforeApply,
            reason: verify_target.reason.clone(),
            package_verify_step: package_verify_step.clone(),
            install_step: install_step.clone(),
            verify_target_step: verify_target_step.clone(),
            backup_step: None,
            shadow_cutover_step: None,
            explicit_statement:
                "this package-native shadow cutover session is bounded shadow evidence only; it does not authorize production activation"
                    .to_string(),
        };
        persist_json(&paths.status_path, &status)?;
        return Ok(report_from_status(
            config,
            session_dir,
            TinyLivePackageShadowCutoverVerdict::TinyLivePackageShadowCutoverFailedBeforeApply,
            &status,
            None,
            None,
            false,
        ));
    }

    let backup_report =
        tiny_live_activation_live_execute::backup_current_config_report_for_cutover(
            Path::new(required_string(
                install.installed_activation_config_path.as_deref(),
                "install step missing installed activation config path",
            )?),
            Path::new(required_string(
                install.installed_rollback_config_path.as_deref(),
                "install step missing installed rollback config path",
            )?),
            Path::new(required_string(
                install.installed_target_config_path.as_deref(),
                "install step missing installed target config path",
            )?),
            &config.shadow_target_service_name,
            Path::new(required_string(
                install.installed_wrapper_path.as_deref(),
                "install step missing installed wrapper path",
            )?),
            Path::new(required_string(
                install.runtime_dir.as_deref(),
                "install step missing runtime dir",
            )?),
            Path::new(required_string(
                install.backup_dir.as_deref(),
                "install step missing backup dir",
            )?),
            DEFAULT_STARTUP_TIMEOUT_MS,
            DEFAULT_ROLLBACK_TIMEOUT_MS,
        )?;
    persist_json(&paths.backup_report_path, &backup_report)?;
    let backup_step = Some(step_artifact(
        &paths.backup_report_path,
        "backup_current_config",
        &serialize_enum(&backup_report.verdict),
        &backup_report.reason,
        backup_report.generated_at,
    ));
    if backup_report.verdict
        != tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveBackupCreated
    {
        let status = PackageShadowCutoverStatus {
            status_version: SHADOW_STATUS_VERSION.to_string(),
            updated_at: Utc::now(),
            session_dir: session_dir.display().to_string(),
            nested_cutover_session_dir: paths.nested_cutover_session_dir.display().to_string(),
            result: PackageShadowCutoverResult::FailedBeforeApply,
            reason: backup_report.reason.clone(),
            package_verify_step: package_verify_step.clone(),
            install_step: install_step.clone(),
            verify_target_step: verify_target_step.clone(),
            backup_step: backup_step.clone(),
            shadow_cutover_step: None,
            explicit_statement:
                "this package-native shadow cutover session is bounded shadow evidence only; it does not authorize production activation"
                    .to_string(),
        };
        persist_json(&paths.status_path, &status)?;
        return Ok(report_from_status(
            config,
            session_dir,
            TinyLivePackageShadowCutoverVerdict::TinyLivePackageShadowCutoverFailedBeforeApply,
            &status,
            None,
            None,
            false,
        ));
    }

    let cutover = tiny_live_activation_cutover::run_live_cutover_for_rehearsal(
        Path::new(required_string(
            install.installed_activation_config_path.as_deref(),
            "install step missing installed activation config path",
        )?),
        Path::new(required_string(
            install.installed_rollback_config_path.as_deref(),
            "install step missing installed rollback config path",
        )?),
        Path::new(required_string(
            install.runtime_dir.as_deref(),
            "install step missing runtime dir",
        )?),
        Path::new(required_string(
            install.installed_target_config_path.as_deref(),
            "install step missing installed target config path",
        )?),
        &config.shadow_target_service_name,
        Path::new(required_string(
            install.installed_wrapper_path.as_deref(),
            "install step missing installed wrapper path",
        )?),
        Path::new(required_string(
            install.backup_dir.as_deref(),
            "install step missing backup dir",
        )?),
        &paths.nested_cutover_session_dir,
        DEFAULT_STARTUP_TIMEOUT_MS,
        DEFAULT_ROLLBACK_TIMEOUT_MS,
        None,
        DEFAULT_SAMPLE_CADENCE_MS,
        Some(DEFAULT_MAX_OBSERVATION_STALENESS_MS),
        &observation_seed,
    )?;
    persist_json_value(&paths.shadow_cutover_report_path, &cutover.report_json)?;
    let shadow_cutover_step = Some(step_artifact(
        &paths.shadow_cutover_report_path,
        "run_live_cutover",
        &cutover.verdict,
        &cutover.reason,
        cutover.generated_at,
    ));
    let (result, verdict) = match cutover.live_result.as_deref() {
        Some("completed_keep_running") => (
            PackageShadowCutoverResult::CompletedKeepRunning,
            TinyLivePackageShadowCutoverVerdict::TinyLivePackageShadowCutoverCompletedKeepRunning,
        ),
        Some("completed_with_rollback") => (
            PackageShadowCutoverResult::CompletedWithRollback,
            TinyLivePackageShadowCutoverVerdict::TinyLivePackageShadowCutoverCompletedWithRollback,
        ),
        Some("failed_before_apply") => (
            PackageShadowCutoverResult::FailedBeforeApply,
            TinyLivePackageShadowCutoverVerdict::TinyLivePackageShadowCutoverFailedBeforeApply,
        ),
        Some("failed_during_watch") => (
            PackageShadowCutoverResult::FailedDuringWatch,
            TinyLivePackageShadowCutoverVerdict::TinyLivePackageShadowCutoverFailedDuringWatch,
        ),
        _ => (
            PackageShadowCutoverResult::FailedBeforeApply,
            TinyLivePackageShadowCutoverVerdict::TinyLivePackageShadowCutoverFailedBeforeApply,
        ),
    };
    let status = PackageShadowCutoverStatus {
        status_version: SHADOW_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        session_dir: session_dir.display().to_string(),
        nested_cutover_session_dir: paths.nested_cutover_session_dir.display().to_string(),
        result,
        reason: cutover.reason.clone(),
        package_verify_step,
        install_step,
        verify_target_step,
        backup_step,
        shadow_cutover_step,
        explicit_statement:
            "this package-native shadow cutover session is bounded shadow evidence only; it does not authorize production activation"
                .to_string(),
    };
    persist_json(&paths.status_path, &status)?;
    Ok(report_from_status(
        config,
        session_dir,
        verdict,
        &status,
        cutover.current_pre_activation_gate_verdict.clone(),
        cutover.current_pre_activation_gate_reason.clone(),
        cutover.activation_authorized,
    ))
}

fn verify_package_shadow_cutover_report(config: &Config) -> Result<PackageShadowCutoverReport> {
    if let Some(report) = shadow_target_validation_refusal_report(config, true)? {
        return Ok(report);
    }
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for --verify-package-shadow-cutover"))?;
    let paths = package_shadow_cutover_paths(session_dir);
    let session: PackageShadowCutoverSession = load_json(&paths.session_path)?;
    let status: PackageShadowCutoverStatus = load_json(&paths.status_path)?;
    let mut mismatches = Vec::new();

    compare_string(
        &session.package_dir,
        &config.package_dir.display().to_string(),
        "package shadow cutover package_dir",
        &mut mismatches,
    );
    compare_string(
        &session.shadow_install_root,
        &config.shadow_install_root.display().to_string(),
        "package shadow cutover shadow_install_root",
        &mut mismatches,
    );
    compare_string(
        &session.live_install_root,
        &config.live_install_root.display().to_string(),
        "package shadow cutover live_install_root",
        &mut mismatches,
    );
    compare_string(
        &session.shadow_target_service_name,
        &config.shadow_target_service_name,
        "package shadow cutover shadow_target_service_name",
        &mut mismatches,
    );
    compare_string(
        &session.backend_command,
        &config.backend_command,
        "package shadow cutover backend_command",
        &mut mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "package shadow cutover session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.nested_cutover_session_dir,
        &paths.nested_cutover_session_dir.display().to_string(),
        "package shadow cutover nested_cutover_session_dir",
        &mut mismatches,
    );
    if session.wrapper_timeout_ms != config.wrapper_timeout_ms {
        mismatches.push(format!(
            "package shadow cutover wrapper_timeout_ms {} does not match requested {}",
            session.wrapper_timeout_ms, config.wrapper_timeout_ms
        ));
    }
    compare_string(
        &status.session_dir,
        &session_dir.display().to_string(),
        "package shadow cutover status session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.nested_cutover_session_dir,
        &paths.nested_cutover_session_dir.display().to_string(),
        "package shadow cutover status nested_cutover_session_dir",
        &mut mismatches,
    );

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
        PackageShadowCutoverResult::FailedBeforeInstall => {
            if let Some(install_step) = &status.install_step {
                let install_report = load_bound_step_json::<PackageDeployReportView>(
                    install_step,
                    &paths.install_report_path,
                    "install_step",
                    &mut mismatches,
                )?;
                validate_package_deploy_report_contract(
                    &install_report,
                    config,
                    "install step",
                    &mut mismatches,
                );
                if install_report.verdict == "tiny_live_package_deploy_install_completed" {
                    mismatches.push(
                        "install step is green but top-level result says failed_before_install"
                            .to_string(),
                    );
                }
            }
            if status.verify_target_step.is_some()
                || status.backup_step.is_some()
                || status.shadow_cutover_step.is_some()
            {
                mismatches.push(
                    "failed_before_install session must not contain verify/backup/shadow-cutover steps"
                        .to_string(),
                );
            }
        }
        PackageShadowCutoverResult::FailedBeforeApply
        | PackageShadowCutoverResult::CompletedKeepRunning
        | PackageShadowCutoverResult::CompletedWithRollback
        | PackageShadowCutoverResult::FailedDuringWatch => {
            let install_report = load_required_package_deploy_report(
                &status.install_step,
                &paths.install_report_path,
                "install_step",
                &mut mismatches,
            )?;
            validate_package_deploy_report_contract(
                &install_report,
                config,
                "install step",
                &mut mismatches,
            );
            validate_installed_assets_match_package(
                &install_report,
                &config.package_dir,
                &config.shadow_install_root,
                &mut mismatches,
            );
            if install_report.verdict != "tiny_live_package_deploy_install_completed" {
                mismatches.push(format!(
                    "install step verdict {} is not install completed for a later-stage shadow session",
                    install_report.verdict
                ));
            }

            let verify_target_report = load_required_install_target_report(
                &status.verify_target_step,
                &paths.verify_target_report_path,
                "verify_target_step",
                &mut mismatches,
            )?;
            validate_install_target_report_contract(
                &verify_target_report,
                config,
                "verify target step",
                &mut mismatches,
            );

            let backup_report: tiny_live_activation_live_execute::LiveExecuteReport =
                load_required_live_execute_report(
                    &status.backup_step,
                    &paths.backup_report_path,
                    "backup_step",
                    &mut mismatches,
                )?;
            validate_backup_report_contract(
                &backup_report,
                &install_report,
                config,
                &mut mismatches,
            );

            let shadow_cutover_report = load_required_cutover_report(
                &status.shadow_cutover_step,
                &paths.shadow_cutover_report_path,
                "shadow_cutover_step",
                &mut mismatches,
            )?;
            validate_shadow_cutover_report_contract(
                &shadow_cutover_report,
                &install_report,
                &paths.nested_cutover_session_dir,
                config,
                &mut mismatches,
            );

            let nested_cutover_verify =
                tiny_live_activation_cutover::verify_live_cutover_session_for_rehearsal(
                    Path::new(required_string(
                        install_report.installed_activation_config_path.as_deref(),
                        "install step missing installed activation config path",
                    )?),
                    Path::new(required_string(
                        install_report.installed_rollback_config_path.as_deref(),
                        "install step missing installed rollback config path",
                    )?),
                    Path::new(required_string(
                        install_report.runtime_dir.as_deref(),
                        "install step missing runtime dir",
                    )?),
                    Path::new(required_string(
                        install_report.installed_target_config_path.as_deref(),
                        "install step missing installed target config path",
                    )?),
                    &config.shadow_target_service_name,
                    Path::new(required_string(
                        install_report.installed_wrapper_path.as_deref(),
                        "install step missing installed wrapper path",
                    )?),
                    Path::new(required_string(
                        install_report.backup_dir.as_deref(),
                        "install step missing backup dir",
                    )?),
                    &paths.nested_cutover_session_dir,
                    DEFAULT_STARTUP_TIMEOUT_MS,
                    DEFAULT_ROLLBACK_TIMEOUT_MS,
                    None,
                    DEFAULT_SAMPLE_CADENCE_MS,
                    Some(DEFAULT_MAX_OBSERVATION_STALENESS_MS),
                )?;
            if nested_cutover_verify.verdict != "tiny_live_cutover_verify_ok" {
                mismatches.extend(nested_cutover_verify.verification_mismatches.clone());
            }
            current_pre_activation_gate_verdict = nested_cutover_verify
                .current_pre_activation_gate_verdict
                .clone();
            current_pre_activation_gate_reason = nested_cutover_verify
                .current_pre_activation_gate_reason
                .clone();
            activation_authorized = nested_cutover_verify.activation_authorized;

            match status.result {
                PackageShadowCutoverResult::FailedBeforeApply => {
                    if shadow_cutover_report.live_result.as_deref()
                        == Some("completed_keep_running")
                        || shadow_cutover_report.live_result.as_deref()
                            == Some("completed_with_rollback")
                    {
                        mismatches.push(
                            "shadow cutover report completed, but top-level result says failed_before_apply"
                                .to_string(),
                        );
                    }
                }
                PackageShadowCutoverResult::CompletedKeepRunning => {
                    if shadow_cutover_report.live_result.as_deref()
                        != Some("completed_keep_running")
                    {
                        mismatches.push(
                            "shadow cutover report live_result is not completed_keep_running for a keep-running shadow session"
                                .to_string(),
                        );
                    }
                    let target = load_from_path(Path::new(required_string(
                        install_report.installed_target_config_path.as_deref(),
                        "install step missing installed target config path",
                    )?))
                    .with_context(|| {
                        "failed loading shadow target config after keep-running session"
                    })?;
                    if !target.execution.enabled {
                        mismatches.push(
                            "shadow target config must keep execution.enabled=true after completed_keep_running"
                                .to_string(),
                        );
                    }
                }
                PackageShadowCutoverResult::CompletedWithRollback => {
                    if shadow_cutover_report.live_result.as_deref()
                        != Some("completed_with_rollback")
                    {
                        mismatches.push(
                            "shadow cutover report live_result is not completed_with_rollback for a rollback shadow session"
                                .to_string(),
                        );
                    }
                    let target = load_from_path(Path::new(required_string(
                        install_report.installed_target_config_path.as_deref(),
                        "install step missing installed target config path",
                    )?))
                    .with_context(|| {
                        "failed loading shadow target config after rollback session"
                    })?;
                    if target.execution.enabled {
                        mismatches.push(
                            "shadow target config must restore execution.enabled=false after completed_with_rollback"
                                .to_string(),
                        );
                    }
                }
                PackageShadowCutoverResult::FailedDuringWatch => {
                    if shadow_cutover_report.live_result.as_deref() != Some("failed_during_watch") {
                        mismatches.push(
                            "shadow cutover report live_result is not failed_during_watch for a failed-during-watch shadow session"
                                .to_string(),
                        );
                    }
                }
                PackageShadowCutoverResult::FailedBeforeInstall => {}
            }
        }
    }

    let verdict = if mismatches.is_empty() {
        TinyLivePackageShadowCutoverVerdict::TinyLivePackageShadowCutoverVerifyOk
    } else {
        TinyLivePackageShadowCutoverVerdict::TinyLivePackageShadowCutoverVerifyInvalid
    };
    let reason = if mismatches.is_empty() {
        format!(
            "package-native shadow cutover session under {} is coherent and bound to the requested package/shadow contract",
            session_dir.display()
        )
    } else {
        mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "package-native shadow cutover verification failed".to_string())
    };
    Ok(PackageShadowCutoverReport {
        generated_at: Utc::now(),
        mode: "verify_package_shadow_cutover".to_string(),
        verdict,
        reason,
        package_dir: config.package_dir.display().to_string(),
        shadow_install_root: config.shadow_install_root.display().to_string(),
        live_install_root: config.live_install_root.display().to_string(),
        shadow_target_service_name: config.shadow_target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        wrapper_timeout_ms: config.wrapper_timeout_ms,
        session_dir: Some(session_dir.display().to_string()),
        shadow_cutover_session_path: Some(paths.session_path.display().to_string()),
        shadow_cutover_status_path: Some(paths.status_path.display().to_string()),
        nested_cutover_session_dir: Some(paths.nested_cutover_session_dir.display().to_string()),
        result: Some(serialize_enum(&status.result)),
        package_verify_step: status.package_verify_step,
        install_step: status.install_step,
        verify_target_step: status.verify_target_step,
        backup_step: status.backup_step,
        shadow_cutover_step: status.shadow_cutover_step,
        package_verify_command_summary: package_verify_command_summary(config),
        install_from_package_command_summary: install_from_package_command_summary(config),
        verify_install_target_command_summary: verify_install_target_command_summary(config),
        backup_command_summary: backup_command_summary(config),
        run_shadow_cutover_command_summary: run_shadow_cutover_command_summary(
            config,
            &paths.nested_cutover_session_dir,
        ),
        current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason,
        verification_mismatches: mismatches,
        activation_authorized,
        explicit_statement:
            "this package-native shadow cutover verification checks bounded shadow evidence only; it does not authorize production activation"
                .to_string(),
    })
}

fn base_report(
    config: &Config,
    verdict: TinyLivePackageShadowCutoverVerdict,
    reason: String,
) -> PackageShadowCutoverReport {
    PackageShadowCutoverReport {
        generated_at: Utc::now(),
        mode: mode_name(config.mode).to_string(),
        verdict,
        reason,
        package_dir: config.package_dir.display().to_string(),
        shadow_install_root: config.shadow_install_root.display().to_string(),
        live_install_root: config.live_install_root.display().to_string(),
        shadow_target_service_name: config.shadow_target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        wrapper_timeout_ms: config.wrapper_timeout_ms,
        session_dir: config
            .session_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        shadow_cutover_session_path: config
            .session_dir
            .as_ref()
            .map(|path| package_shadow_cutover_paths(path).session_path.display().to_string()),
        shadow_cutover_status_path: config
            .session_dir
            .as_ref()
            .map(|path| package_shadow_cutover_paths(path).status_path.display().to_string()),
        nested_cutover_session_dir: config.session_dir.as_ref().map(|path| {
            package_shadow_cutover_paths(path)
                .nested_cutover_session_dir
                .display()
                .to_string()
        }),
        result: None,
        package_verify_step: None,
        install_step: None,
        verify_target_step: None,
        backup_step: None,
        shadow_cutover_step: None,
        package_verify_command_summary: package_verify_command_summary(config),
        install_from_package_command_summary: install_from_package_command_summary(config),
        verify_install_target_command_summary: verify_install_target_command_summary(config),
        backup_command_summary: backup_command_summary(config),
        run_shadow_cutover_command_summary: run_shadow_cutover_command_summary(
            config,
            &config
                .session_dir
                .as_ref()
                .map(|path| package_shadow_cutover_paths(path).nested_cutover_session_dir)
                .unwrap_or_else(|| {
                    PathBuf::from(
                        "<session-dir>/tiny_live_activation_package_shadow_cutover.cutover_session",
                    )
                }),
        ),
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        verification_mismatches: Vec::new(),
        activation_authorized: false,
        explicit_statement:
            "this package-native shadow cutover layer is bounded shadow evidence only; it does not authorize or perform real production activation by itself"
                .to_string(),
    }
}

fn report_from_status(
    config: &Config,
    session_dir: &Path,
    verdict: TinyLivePackageShadowCutoverVerdict,
    status: &PackageShadowCutoverStatus,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    activation_authorized: bool,
) -> PackageShadowCutoverReport {
    let paths = package_shadow_cutover_paths(session_dir);
    PackageShadowCutoverReport {
        generated_at: Utc::now(),
        mode: "run_package_shadow_cutover".to_string(),
        verdict,
        reason: status.reason.clone(),
        package_dir: config.package_dir.display().to_string(),
        shadow_install_root: config.shadow_install_root.display().to_string(),
        live_install_root: config.live_install_root.display().to_string(),
        shadow_target_service_name: config.shadow_target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        wrapper_timeout_ms: config.wrapper_timeout_ms,
        session_dir: Some(session_dir.display().to_string()),
        shadow_cutover_session_path: Some(paths.session_path.display().to_string()),
        shadow_cutover_status_path: Some(paths.status_path.display().to_string()),
        nested_cutover_session_dir: Some(paths.nested_cutover_session_dir.display().to_string()),
        result: Some(serialize_enum(&status.result)),
        package_verify_step: status.package_verify_step.clone(),
        install_step: status.install_step.clone(),
        verify_target_step: status.verify_target_step.clone(),
        backup_step: status.backup_step.clone(),
        shadow_cutover_step: status.shadow_cutover_step.clone(),
        package_verify_command_summary: package_verify_command_summary(config),
        install_from_package_command_summary: install_from_package_command_summary(config),
        verify_install_target_command_summary: verify_install_target_command_summary(config),
        backup_command_summary: backup_command_summary(config),
        run_shadow_cutover_command_summary: run_shadow_cutover_command_summary(
            config,
            &paths.nested_cutover_session_dir,
        ),
        current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason,
        verification_mismatches: Vec::new(),
        activation_authorized,
        explicit_statement:
            "this package-native shadow cutover session is bounded shadow evidence only; it does not authorize production activation"
                .to_string(),
    }
}

fn mode_name(mode: Mode) -> &'static str {
    match mode {
        Mode::PlanPackageShadowCutover => "plan_package_shadow_cutover",
        Mode::RenderPackageShadowCutoverScript => "render_package_shadow_cutover_script",
        Mode::RunPackageShadowCutover => "run_package_shadow_cutover",
        Mode::VerifyPackageShadowCutover => "verify_package_shadow_cutover",
    }
}

fn package_verify_command_summary(config: &Config) -> String {
    format!(
        "copybot_tiny_live_activation_package_deploy --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --verify-package-deploy-target",
        shell_single_quote(&config.package_dir.display().to_string()),
        shell_single_quote(&config.shadow_install_root.display().to_string()),
        shell_single_quote(&config.shadow_target_service_name),
        shell_single_quote(&config.backend_command),
        config.wrapper_timeout_ms,
    )
}

fn install_from_package_command_summary(config: &Config) -> String {
    format!(
        "copybot_tiny_live_activation_package_deploy --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --install-from-package",
        shell_single_quote(&config.package_dir.display().to_string()),
        shell_single_quote(&config.shadow_install_root.display().to_string()),
        shell_single_quote(&config.shadow_target_service_name),
        shell_single_quote(&config.backend_command),
        config.wrapper_timeout_ms,
    )
}

fn verify_install_target_command_summary(config: &Config) -> String {
    format!(
        "copybot_tiny_live_activation_install_target --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --verify-install-target",
        shell_single_quote(&config.shadow_install_root.display().to_string()),
        shell_single_quote(&config.shadow_target_service_name),
        shell_single_quote(&config.backend_command),
        config.wrapper_timeout_ms,
    )
}

fn backup_command_summary(config: &Config) -> String {
    format!(
        "package-native shadow backup step uses installed activation/rollback artifacts under {} to create bounded backup proof before shadow apply/watch/rollback; this remains non-authorizing and never targets live root {}",
        shell_single_quote(&config.shadow_install_root.display().to_string()),
        shell_single_quote(&config.live_install_root.display().to_string()),
    )
}

fn run_shadow_cutover_command_summary(
    config: &Config,
    nested_cutover_session_dir: &Path,
) -> String {
    format!(
        "copybot_tiny_live_activation_cutover --activation-config <installed-activation-under-{}> --rollback-config <installed-rollback-under-{}> --runtime-dir <installed-runtime-under-{}> --target-config <shadow-target-config-under-{}> --target-service {} --service-control-command <installed-wrapper-under-{}> --backup-dir <installed-backup-under-{}> --session-dir {} --run-live-cutover",
        shell_single_quote(&config.shadow_install_root.display().to_string()),
        shell_single_quote(&config.shadow_install_root.display().to_string()),
        shell_single_quote(&config.shadow_install_root.display().to_string()),
        shell_single_quote(&config.shadow_install_root.display().to_string()),
        shell_single_quote(&config.shadow_target_service_name),
        shell_single_quote(&config.shadow_install_root.display().to_string()),
        shell_single_quote(&config.shadow_install_root.display().to_string()),
        shell_single_quote(&nested_cutover_session_dir.display().to_string()),
    )
}

fn planned_session(
    config: &Config,
    session_dir: &Path,
    nested_cutover_session_dir: &Path,
) -> PackageShadowCutoverSession {
    PackageShadowCutoverSession {
        session_version: SHADOW_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        package_dir: config.package_dir.display().to_string(),
        shadow_install_root: config.shadow_install_root.display().to_string(),
        live_install_root: config.live_install_root.display().to_string(),
        shadow_target_service_name: config.shadow_target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        wrapper_timeout_ms: config.wrapper_timeout_ms,
        session_dir: session_dir.display().to_string(),
        nested_cutover_session_dir: nested_cutover_session_dir.display().to_string(),
        package_verify_command_summary: package_verify_command_summary(config),
        install_from_package_command_summary: install_from_package_command_summary(config),
        verify_install_target_command_summary: verify_install_target_command_summary(config),
        backup_command_summary: backup_command_summary(config),
        run_shadow_cutover_command_summary: run_shadow_cutover_command_summary(
            config,
            nested_cutover_session_dir,
        ),
        explicit_statement:
            "this package-native shadow cutover session binds one immutable package plus one explicit shadow target contract; it does not authorize production activation"
                .to_string(),
    }
}

fn step_artifact(
    path: &Path,
    mode: &str,
    verdict: &str,
    reason: &str,
    generated_at: DateTime<Utc>,
) -> PackageShadowCutoverStepArtifact {
    PackageShadowCutoverStepArtifact {
        path: path.display().to_string(),
        mode: mode.to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        generated_at,
    }
}

fn package_shadow_cutover_paths(session_dir: &Path) -> PackageShadowCutoverPaths {
    PackageShadowCutoverPaths {
        session_path: session_dir.join("tiny_live_activation_package_shadow_cutover.session.json"),
        status_path: session_dir.join("tiny_live_activation_package_shadow_cutover.status.json"),
        package_verify_report_path: session_dir
            .join("tiny_live_activation_package_shadow_cutover.package_verify.report.json"),
        install_report_path: session_dir
            .join("tiny_live_activation_package_shadow_cutover.install.report.json"),
        verify_target_report_path: session_dir
            .join("tiny_live_activation_package_shadow_cutover.verify_target.report.json"),
        backup_report_path: session_dir
            .join("tiny_live_activation_package_shadow_cutover.backup.report.json"),
        shadow_cutover_report_path: session_dir
            .join("tiny_live_activation_package_shadow_cutover.shadow_cutover.report.json"),
        nested_cutover_session_dir: session_dir
            .join("tiny_live_activation_package_shadow_cutover.cutover_session"),
    }
}

fn ensure_clean_shadow_cutover_session_dir(session_dir: &Path) -> Result<()> {
    let paths = package_shadow_cutover_paths(session_dir);
    for artifact in [
        &paths.session_path,
        &paths.status_path,
        &paths.package_verify_report_path,
        &paths.install_report_path,
        &paths.verify_target_report_path,
        &paths.backup_report_path,
        &paths.shadow_cutover_report_path,
        &paths.nested_cutover_session_dir,
    ] {
        if artifact.exists() {
            bail!(
                "refusing to reuse package shadow cutover session dir {}; artifact {} already exists",
                session_dir.display(),
                artifact.display()
            );
        }
    }
    fs::create_dir_all(session_dir)
        .with_context(|| format!("failed creating session dir {}", session_dir.display()))
}

fn validate_shadow_target_contract(config: &Config, require_session_dir: bool) -> Result<()> {
    validate_shadow_install_root_separation(
        &config.live_install_root,
        &config.shadow_install_root,
    )?;
    validate_shadow_target_service_name(&config.shadow_target_service_name)?;
    if require_session_dir {
        let session_dir = config
            .session_dir
            .as_ref()
            .ok_or_else(|| anyhow!("missing session dir"))?;
        validate_shadow_session_dir(
            &config.live_install_root,
            &config.shadow_install_root,
            session_dir,
        )?;
    }
    Ok(())
}

fn shadow_target_validation_refusal_report(
    config: &Config,
    require_session_dir: bool,
) -> Result<Option<PackageShadowCutoverReport>> {
    match validate_shadow_target_contract(config, require_session_dir) {
        Ok(()) => Ok(None),
        Err(error) => Ok(Some(base_report(
            config,
            TinyLivePackageShadowCutoverVerdict::TinyLivePackageShadowCutoverRefusedUnsafeShadowTarget,
            error.to_string(),
        ))),
    }
}

fn validate_shadow_target_service_name(service_name: &str) -> Result<()> {
    tiny_live_activation_live_execute::validate_target_service_name_for_live_target(service_name)?;
    let lower = service_name.to_ascii_lowercase();
    if lower == "solana-copy-bot.service"
        || lower == "solana-copy-bot"
        || lower.starts_with("solana-copy-bot")
        || lower.contains("solana-copy-bot.service")
    {
        bail!(
            "shadow target service {} is unsafe because package-native shadow cutover must not reuse the real prod service contract",
            service_name
        );
    }
    Ok(())
}

fn validate_shadow_install_root_separation(live_root: &Path, shadow_root: &Path) -> Result<()> {
    if !live_root.is_absolute() {
        bail!("live install root must be an absolute path");
    }
    if !shadow_root.is_absolute() {
        bail!("shadow install root must be an absolute path");
    }
    for managed_path in managed_live_target_surface_paths(live_root)? {
        validate_disjoint_from_managed_path(
            shadow_root,
            &managed_path,
            "shadow install root",
            "managed live-target path",
        )?;
    }
    Ok(())
}

fn validate_shadow_session_dir(
    live_root: &Path,
    shadow_root: &Path,
    session_dir: &Path,
) -> Result<()> {
    if !session_dir.is_absolute() {
        bail!("session dir must be an absolute path");
    }
    let session_anchor = find_existing_anchor(session_dir, "session dir")?;
    reject_anchor_symlink(&session_anchor, "session dir")?;
    reject_descendant_symlinks(session_dir, &session_anchor, "session dir")?;
    if session_dir.starts_with(shadow_root) {
        bail!(
            "session dir {} must stay outside shadow install root {}",
            session_dir.display(),
            shadow_root.display()
        );
    }
    let resolved_session_anchor = fs::canonicalize(&session_anchor).with_context(|| {
        format!(
            "failed canonicalizing session dir anchor {}",
            session_anchor.display()
        )
    })?;
    let shadow_anchor = find_existing_anchor(shadow_root, "shadow install root")?;
    let resolved_shadow = if shadow_root.exists() {
        fs::canonicalize(shadow_root).with_context(|| {
            format!(
                "failed canonicalizing shadow root {}",
                shadow_root.display()
            )
        })?
    } else {
        fs::canonicalize(&shadow_anchor).with_context(|| {
            format!(
                "failed canonicalizing shadow install root anchor {}",
                shadow_anchor.display()
            )
        })?
    };
    if resolved_session_anchor.starts_with(&resolved_shadow) {
        bail!(
            "session dir {} must stay outside shadow install root {} after resolving path identity",
            session_dir.display(),
            shadow_root.display()
        );
    }
    for managed_path in managed_live_target_surface_paths(live_root)? {
        if session_dir.starts_with(&managed_path)
            || resolved_session_anchor.starts_with(&resolved_host_path_identity(
                &managed_path,
                "managed live-target path",
            )?)
        {
            bail!(
                "session dir {} must stay outside managed live-target path {}",
                session_dir.display(),
                managed_path.display()
            );
        }
    }
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
    Ok(())
}

fn managed_live_target_surface_paths(live_root: &Path) -> Result<Vec<PathBuf>> {
    let surface = tiny_live_activation_install_target::derive_install_target_managed_surface_paths(
        live_root,
    )?;
    Ok(vec![
        surface.wrapper_path,
        surface.wrapper_parent,
        surface.target_config_path,
        surface.target_config_parent,
        surface.runtime_dir,
        surface.backup_dir,
        surface.session_dir,
    ])
}

fn validate_disjoint_from_managed_path(
    candidate: &Path,
    managed_path: &Path,
    candidate_label: &str,
    managed_label: &str,
) -> Result<()> {
    let candidate_anchor = find_existing_anchor(candidate, candidate_label)?;
    reject_anchor_symlink(&candidate_anchor, candidate_label)?;
    reject_descendant_symlinks(candidate, &candidate_anchor, candidate_label)?;
    if candidate.starts_with(managed_path) || managed_path.starts_with(candidate) {
        bail!(
            "{candidate_label} {} must stay disjoint from {managed_label} {}",
            candidate.display(),
            managed_path.display()
        );
    }
    let resolved_candidate = resolved_non_symlink_path_identity(candidate, candidate_label)?;
    let resolved_managed = resolved_host_path_identity(managed_path, managed_label)?;
    if resolved_candidate.starts_with(&resolved_managed)
        || resolved_managed.starts_with(&resolved_candidate)
    {
        bail!(
            "{candidate_label} {} must stay disjoint from {managed_label} {} after resolving path identity",
            candidate.display(),
            managed_path.display()
        );
    }
    Ok(())
}

fn resolved_non_symlink_path_identity(path: &Path, label: &str) -> Result<PathBuf> {
    let anchor = find_existing_anchor(path, label)?;
    reject_anchor_symlink(&anchor, label)?;
    reject_descendant_symlinks(path, &anchor, label)?;
    build_resolved_path_identity(path, &anchor, label)
}

fn resolved_host_path_identity(path: &Path, label: &str) -> Result<PathBuf> {
    let anchor = find_existing_anchor(path, label)?;
    build_resolved_path_identity(path, &anchor, label)
}

fn build_resolved_path_identity(path: &Path, anchor: &Path, label: &str) -> Result<PathBuf> {
    let resolved_anchor = fs::canonicalize(&anchor)
        .with_context(|| format!("failed canonicalizing {label} anchor {}", anchor.display()))?;
    let relative = path
        .strip_prefix(&anchor)
        .with_context(|| format!("failed stripping {label} anchor {}", anchor.display()))?;
    if relative.as_os_str().is_empty() {
        Ok(resolved_anchor)
    } else {
        Ok(resolved_anchor.join(relative))
    }
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
    step: &Option<PackageShadowCutoverStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<PackageDeployReportView> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("missing required shadow cutover step {label}"))?;
    load_bound_step_json(step, expected_path, label, mismatches)
}

fn load_required_install_target_report(
    step: &Option<PackageShadowCutoverStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<InstallTargetReportView> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("missing required shadow cutover step {label}"))?;
    load_bound_step_json(step, expected_path, label, mismatches)
}

fn load_required_live_execute_report(
    step: &Option<PackageShadowCutoverStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<tiny_live_activation_live_execute::LiveExecuteReport> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("missing required shadow cutover step {label}"))?;
    load_bound_step_json(step, expected_path, label, mismatches)
}

fn load_required_cutover_report(
    step: &Option<PackageShadowCutoverStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<CutoverReportView> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("missing required shadow cutover step {label}"))?;
    load_bound_step_json(step, expected_path, label, mismatches)
}

fn load_bound_step_json<T: for<'de> Deserialize<'de>>(
    step: &PackageShadowCutoverStepArtifact,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<T> {
    let expected = expected_path.display().to_string();
    if step.path != expected {
        mismatches.push(format!(
            "package shadow cutover {label} path {} does not match deterministic session artifact {}",
            step.path, expected
        ));
    }
    load_json(expected_path).with_context(|| {
        format!(
            "failed reading deterministic package shadow cutover {label} report {}",
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
        &config.shadow_install_root.display().to_string(),
        &format!("{label} install_root"),
        mismatches,
    );
    compare_string(
        &report.target_service_name,
        &config.shadow_target_service_name,
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
        &config.shadow_install_root.display().to_string(),
        &format!("{label} install_root"),
        mismatches,
    );
    compare_string(
        &report.target_service_name,
        &config.shadow_target_service_name,
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

fn validate_backup_report_contract(
    report: &tiny_live_activation_live_execute::LiveExecuteReport,
    install_report: &PackageDeployReportView,
    config: &Config,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &report.activation_config_path,
        required_string(
            install_report.installed_activation_config_path.as_deref(),
            "install step missing installed activation config path",
        )
        .unwrap_or_default(),
        "backup step activation_config_path",
        mismatches,
    );
    compare_string(
        &report.rollback_config_path,
        required_string(
            install_report.installed_rollback_config_path.as_deref(),
            "install step missing installed rollback config path",
        )
        .unwrap_or_default(),
        "backup step rollback_config_path",
        mismatches,
    );
    compare_string(
        &report.target_config_path,
        required_string(
            install_report.installed_target_config_path.as_deref(),
            "install step missing installed target config path",
        )
        .unwrap_or_default(),
        "backup step target_config_path",
        mismatches,
    );
    compare_string(
        &report.target_service_name,
        &config.shadow_target_service_name,
        "backup step target_service_name",
        mismatches,
    );
    compare_string(
        &report.service_control_command_path,
        required_string(
            install_report.installed_wrapper_path.as_deref(),
            "install step missing installed wrapper path",
        )
        .unwrap_or_default(),
        "backup step service_control_command_path",
        mismatches,
    );
    compare_string(
        &report.runtime_dir,
        required_string(
            install_report.runtime_dir.as_deref(),
            "install step missing runtime dir",
        )
        .unwrap_or_default(),
        "backup step runtime_dir",
        mismatches,
    );
    compare_string(
        &report.backup_dir,
        required_string(
            install_report.backup_dir.as_deref(),
            "install step missing backup dir",
        )
        .unwrap_or_default(),
        "backup step backup_dir",
        mismatches,
    );
}

fn validate_shadow_cutover_report_contract(
    report: &CutoverReportView,
    install_report: &PackageDeployReportView,
    nested_cutover_session_dir: &Path,
    config: &Config,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &report.activation_config_path,
        required_string(
            install_report.installed_activation_config_path.as_deref(),
            "install step missing installed activation config path",
        )
        .unwrap_or_default(),
        "shadow cutover step activation_config_path",
        mismatches,
    );
    compare_string(
        &report.rollback_config_path,
        required_string(
            install_report.installed_rollback_config_path.as_deref(),
            "install step missing installed rollback config path",
        )
        .unwrap_or_default(),
        "shadow cutover step rollback_config_path",
        mismatches,
    );
    compare_string(
        &report.runtime_dir,
        required_string(
            install_report.runtime_dir.as_deref(),
            "install step missing runtime dir",
        )
        .unwrap_or_default(),
        "shadow cutover step runtime_dir",
        mismatches,
    );
    compare_string(
        &report.target_config_path,
        required_string(
            install_report.installed_target_config_path.as_deref(),
            "install step missing installed target config path",
        )
        .unwrap_or_default(),
        "shadow cutover step target_config_path",
        mismatches,
    );
    compare_string(
        &report.target_service_name,
        &config.shadow_target_service_name,
        "shadow cutover step target_service_name",
        mismatches,
    );
    compare_string(
        &report.service_control_command_path,
        required_string(
            install_report.installed_wrapper_path.as_deref(),
            "install step missing installed wrapper path",
        )
        .unwrap_or_default(),
        "shadow cutover step service_control_command_path",
        mismatches,
    );
    compare_string(
        &report.backup_dir,
        required_string(
            install_report.backup_dir.as_deref(),
            "install step missing backup dir",
        )
        .unwrap_or_default(),
        "shadow cutover step backup_dir",
        mismatches,
    );
    compare_string(
        report.session_dir.as_deref().unwrap_or_default(),
        &nested_cutover_session_dir.display().to_string(),
        "shadow cutover step session_dir",
        mismatches,
    );
}

fn required_string<'a>(value: Option<&'a str>, error: &'static str) -> Result<&'a str> {
    value.ok_or_else(|| anyhow!(error))
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

fn render_human_report(report: &PackageShadowCutoverReport) -> String {
    let mut lines = vec![
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("package_dir={}", report.package_dir),
        format!("shadow_install_root={}", report.shadow_install_root),
        format!("live_install_root={}", report.live_install_root),
        format!(
            "shadow_target_service_name={}",
            report.shadow_target_service_name
        ),
        format!("backend_command={}", report.backend_command),
        format!("wrapper_timeout_ms={}", report.wrapper_timeout_ms),
    ];
    if let Some(value) = &report.session_dir {
        lines.push(format!("session_dir={value}"));
    }
    if let Some(value) = &report.nested_cutover_session_dir {
        lines.push(format!("nested_cutover_session_dir={value}"));
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
    use copybot_storage::{
        DiscoveryWalletFreshnessCaptureWrite, ExecutionDryRunRehearsalWrite, SqliteStore,
    };
    use sha2::Digest;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn coherent_package_and_safe_shadow_target_complete_shadow_cutover_rehearsal() {
        let fixture = shadow_fixture(
            "tiny_live_package_shadow_cutover_complete",
            GateState::Green,
        );
        let report = run_package_shadow_cutover_report(&fixture.config).expect("run shadow");
        assert_eq!(
            report.verdict,
            TinyLivePackageShadowCutoverVerdict::TinyLivePackageShadowCutoverCompletedKeepRunning
        );
        assert_eq!(report.result.as_deref(), Some("completed_keep_running"));
        let live_root_bytes = fs::read(&fixture.live_target_config_path).unwrap();
        let original_live_root_bytes = fs::read(&fixture.original_live_target_config_path).unwrap();
        assert_eq!(live_root_bytes, original_live_root_bytes);
    }

    #[test]
    fn watch_triggered_breach_completes_with_rollback() {
        let fixture = shadow_fixture(
            "tiny_live_package_shadow_cutover_rollback",
            GateState::Green,
        );
        let report = run_package_shadow_cutover_report_with_hooks(
            &fixture.config,
            None,
            None,
            tiny_live_activation_cutover::CutoverRehearsalObservationSeed {
                total_execution_attempts: 12,
                execution_error_count: 12,
                ..Default::default()
            },
        )
        .expect("run shadow");
        assert_eq!(
            report.verdict,
            TinyLivePackageShadowCutoverVerdict::TinyLivePackageShadowCutoverCompletedWithRollback
        );
        let shadow_target =
            load_from_path(&fixture.shadow_target_config_path).expect("load shadow target");
        assert!(!shadow_target.execution.enabled);
    }

    #[test]
    fn backup_failure_is_explicit() {
        let fixture = shadow_fixture(
            "tiny_live_package_shadow_cutover_backup_fail",
            GateState::Green,
        );
        let report = run_package_shadow_cutover_report_with_hooks(
            &fixture.config,
            None,
            Some(&|install| {
                let backup_dir = PathBuf::from(install.backup_dir.as_ref().unwrap());
                fs::remove_dir_all(&backup_dir)?;
                fs::write(&backup_dir, "not-a-dir")?;
                Ok(())
            }),
            Default::default(),
        )
        .expect("run shadow");
        assert_eq!(
            report.verdict,
            TinyLivePackageShadowCutoverVerdict::TinyLivePackageShadowCutoverFailedBeforeApply
        );
    }

    #[cfg(unix)]
    #[test]
    fn apply_failure_is_explicit() {
        let fixture = shadow_fixture(
            "tiny_live_package_shadow_cutover_apply_fail",
            GateState::Green,
        );
        let report = run_package_shadow_cutover_report_with_hooks(
            &fixture.config,
            None,
            Some(&|install| {
                let runtime_dir = PathBuf::from(install.runtime_dir.as_ref().unwrap());
                fs::create_dir_all(&runtime_dir)?;
                fs::write(runtime_dir.join("force_apply_exit_failure"), "1")?;
                Ok(())
            }),
            Default::default(),
        )
        .expect("run shadow");
        assert_eq!(
            report.verdict,
            TinyLivePackageShadowCutoverVerdict::TinyLivePackageShadowCutoverFailedBeforeApply
        );
    }

    #[test]
    fn verify_package_shadow_cutover_rejects_tampered_session_step_path() {
        let fixture = shadow_fixture(
            "tiny_live_package_shadow_cutover_tampered_path",
            GateState::Green,
        );
        run_package_shadow_cutover_report(&fixture.config).expect("run shadow");
        let session_dir = fixture.config.session_dir.as_ref().unwrap();
        let paths = package_shadow_cutover_paths(session_dir);
        let mut status: PackageShadowCutoverStatus = load_json(&paths.status_path).unwrap();
        status.shadow_cutover_step.as_mut().unwrap().path = fixture
            .fixture_dir
            .join("foreign.json")
            .display()
            .to_string();
        persist_json(&paths.status_path, &status).unwrap();
        let verify = verify_package_shadow_cutover_report(&fixture.config).expect("verify");
        assert_eq!(
            verify.verdict,
            TinyLivePackageShadowCutoverVerdict::TinyLivePackageShadowCutoverVerifyInvalid
        );
    }

    #[test]
    fn shadow_target_equal_to_prod_target_is_refused() {
        let fixture = shadow_fixture(
            "tiny_live_package_shadow_cutover_unsafe_root",
            GateState::Green,
        );
        let report = run_json_report(Config {
            mode: Mode::PlanPackageShadowCutover,
            shadow_install_root: fixture.live_install_root.clone(),
            ..fixture.config.clone()
        });
        assert_eq!(
            report.verdict,
            TinyLivePackageShadowCutoverVerdict::TinyLivePackageShadowCutoverRefusedUnsafeShadowTarget
        );
    }

    #[test]
    fn live_install_root_root_and_disjoint_absolute_shadow_root_are_allowed() {
        let fixture = shadow_fixture(
            "tiny_live_package_shadow_cutover_live_root_slash",
            GateState::Green,
        );
        let report = run_json_report(Config {
            mode: Mode::PlanPackageShadowCutover,
            live_install_root: PathBuf::from("/"),
            ..fixture.config.clone()
        });
        assert_eq!(
            report.verdict,
            TinyLivePackageShadowCutoverVerdict::TinyLivePackageShadowCutoverPlanReady
        );
    }

    #[test]
    fn live_install_root_root_and_disjoint_absolute_shadow_root_can_run() {
        let fixture = shadow_fixture(
            "tiny_live_package_shadow_cutover_live_root_slash_run",
            GateState::Green,
        );
        let report = run_package_shadow_cutover_report(&Config {
            live_install_root: PathBuf::from("/"),
            ..fixture.config.clone()
        })
        .expect("run shadow");
        assert_eq!(
            report.verdict,
            TinyLivePackageShadowCutoverVerdict::TinyLivePackageShadowCutoverCompletedKeepRunning,
            "reason: {}",
            report.reason
        );
    }

    #[test]
    fn shadow_root_overlapping_managed_live_target_path_is_refused() {
        let fixture = shadow_fixture(
            "tiny_live_package_shadow_cutover_managed_overlap",
            GateState::Green,
        );
        let report = run_json_report(Config {
            mode: Mode::PlanPackageShadowCutover,
            live_install_root: PathBuf::from("/"),
            shadow_install_root: PathBuf::from("/etc/solana-copy-bot"),
            ..fixture.config.clone()
        });
        assert_eq!(
            report.verdict,
            TinyLivePackageShadowCutoverVerdict::TinyLivePackageShadowCutoverRefusedUnsafeShadowTarget
        );
    }

    #[test]
    fn shadow_target_prod_service_name_is_refused() {
        let fixture = shadow_fixture(
            "tiny_live_package_shadow_cutover_unsafe_service",
            GateState::Green,
        );
        let report = run_json_report(Config {
            mode: Mode::PlanPackageShadowCutover,
            shadow_target_service_name: "solana-copy-bot.service".to_string(),
            ..fixture.config.clone()
        });
        assert_eq!(
            report.verdict,
            TinyLivePackageShadowCutoverVerdict::TinyLivePackageShadowCutoverRefusedUnsafeShadowTarget
        );
    }

    struct ShadowFixture {
        fixture_dir: PathBuf,
        live_install_root: PathBuf,
        live_target_config_path: PathBuf,
        original_live_target_config_path: PathBuf,
        shadow_target_config_path: PathBuf,
        config: Config,
        _rpc_server: MockHttpServer,
        _adapter_server: MockHttpServer,
    }

    #[allow(dead_code)]
    #[derive(Debug, Clone, Copy)]
    enum GateState {
        Green,
        Stage3Blocked,
    }

    fn shadow_fixture(label: &str, gate_state: GateState) -> ShadowFixture {
        let fixture_dir = temp_dir(label);
        let live_install_root = fixture_dir.join("live-root");
        let shadow_install_root = fixture_dir.join("shadow-root");
        let live_target_config_path =
            live_install_root.join("etc/solana-copy-bot/live.server.toml");
        let shadow_target_config_path =
            shadow_install_root.join("etc/solana-copy-bot/live.server.toml");
        fs::create_dir_all(live_target_config_path.parent().unwrap()).unwrap();
        fs::create_dir_all(shadow_target_config_path.parent().unwrap()).unwrap();
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
        let backend_path = fixture_dir.join("fake-shadow-backend.sh");
        write_fake_backend(&backend_path);
        let disabled_contents = dynamic_live_config_toml(
            &db_path,
            &rpc_server.url(""),
            &adapter_server.url("/submit"),
            false,
        );
        fs::write(&live_target_config_path, &disabled_contents).unwrap();
        fs::write(&shadow_target_config_path, &disabled_contents).unwrap();
        let original_live_target_config_path = fixture_dir.join("original-live.server.toml");
        fs::write(&original_live_target_config_path, &disabled_contents).unwrap();
        seed_current_gate_state(&db_path, gate_state);

        let source_dir = fixture_dir.join("source");
        fs::create_dir_all(&source_dir).unwrap();
        let activation_config_source_path = source_dir.join("rendered.activation.toml");
        let rollback_config_source_path = source_dir.join("rendered.rollback.toml");
        write_rendered_artifact_pair(
            &shadow_target_config_path,
            &activation_config_source_path,
            &rollback_config_source_path,
            &db_path,
            &rpc_server.url(""),
            &adapter_server.url("/submit"),
        );

        let package_dir = fixture_dir.join("package");
        tiny_live_activation_package::export_activation_package_for_deploy(
            &package_dir,
            &shadow_install_root,
            "copybot-shadow.service",
            &backend_path.display().to_string(),
            tiny_live_activation_live_execute::live_service_control_wrapper_contract::DEFAULT_TIMEOUT_MS,
            &activation_config_source_path,
            &rollback_config_source_path,
        )
        .expect("export package");

        ShadowFixture {
            fixture_dir: fixture_dir.clone(),
            live_install_root: live_install_root.clone(),
            live_target_config_path,
            original_live_target_config_path,
            shadow_target_config_path,
            config: Config {
                mode: Mode::RunPackageShadowCutover,
                package_dir,
                shadow_install_root,
                live_install_root,
                shadow_target_service_name: "copybot-shadow.service".to_string(),
                backend_command: backend_path.display().to_string(),
                wrapper_timeout_ms: tiny_live_activation_live_execute::live_service_control_wrapper_contract::DEFAULT_TIMEOUT_MS,
                session_dir: Some(fixture_dir.join("shadow-session")),
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
        let fingerprint =
            tiny_live_activation_live_execute::activation_decision_packet::build_config_fingerprint(
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
                    "window_start": captured_at - chrono::Duration::days(2),
                    "verdict": audit_verdict,
                    "reason": "seed",
                    "follow_top_n": 2,
                    "publication_truth_available": true,
                    "publication_runtime_mode": "healthy",
                    "publication_recent_under_gate": true,
                    "latest_publication_ts": captured_at,
                    "publication_age_seconds": 60,
                    "latest_publication_window_start": captured_at - chrono::Duration::days(2),
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
                        "covered_since": captured_at - chrono::Duration::days(2),
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

    #[cfg(unix)]
    fn write_fake_backend(path: &Path) {
        let script = r#"#!/usr/bin/env bash
set -euo pipefail
cmd="${1:-}"
service="${2:-}"
runtime_dir="${COPYBOT_LIVE_SERVICE_CONTROL_RUNTIME_DIR:-}"
action="${COPYBOT_LIVE_SERVICE_CONTROL_ACTION:-}"
mkdir -p "$runtime_dir"
case "$cmd" in
  restart)
    if [[ "$action" == "activation" && -f "$runtime_dir/force_apply_exit_failure" ]]; then
      echo "forced activation failure" >&2
      exit 7
    fi
    if [[ "$action" == "rollback" && -f "$runtime_dir/force_rollback_exit_failure" ]]; then
      echo "forced rollback failure" >&2
      exit 8
    fi
    echo "restart:$service:$action" >> "$runtime_dir/backend.log"
    ;;
  show)
    echo "show:$service:$action" >> "$runtime_dir/backend.log"
    printf 'active\nrunning\n'
    ;;
  *)
    echo "unexpected backend command $cmd" >&2
    exit 2
    ;;
esac
"#;
        fs::write(path, script).unwrap();
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).unwrap();
    }

    #[cfg(not(unix))]
    fn write_fake_backend(path: &Path) {
        fs::write(path, "").unwrap();
    }

    fn run_json_report(config: Config) -> PackageShadowCutoverReport {
        let output = run(Config {
            json: true,
            ..config
        })
        .expect("run json report");
        serde_json::from_str(&output).expect("parse json report")
    }

    fn temp_dir(label: &str) -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        loop {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let dir = env::temp_dir().join(format!(
                "{}_{}_{}_{}",
                label,
                std::process::id(),
                nanos,
                counter
            ));
            match fs::create_dir(&dir) {
                Ok(()) => return dir,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("failed creating temp dir {}: {error}", dir.display()),
            }
        }
    }

    fn ts(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .unwrap()
            .with_timezone(&Utc)
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

[tiny_live_policy.max_route_compute_unit_limit]
jito = 300000

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
        let headers = header_text.lines().map(|line| line.to_string()).collect();
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
            .expect("write response");
        stream.flush().expect("flush response");
    }
}
