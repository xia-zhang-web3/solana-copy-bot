use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
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

type VerifiedActivationPackageContract =
    tiny_live_activation_package::VerifiedActivationPackageContract;
type InstallTargetPackageSummary = tiny_live_activation_install_target::InstallTargetPackageSummary;

const USAGE: &str = "usage: copybot_tiny_live_activation_package_deploy --package-dir <path> --install-root <path> --target-service <name> --backend-command <path-or-name> [--wrapper-timeout-ms <ms>] [--output <path>] [--json] (--plan-package-deploy | --render-package-deploy-script | --verify-package-deploy-target | --plan-package-cutover | --install-from-package)";

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
    output_path: Option<PathBuf>,
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    PlanPackageDeploy,
    RenderPackageDeployScript,
    VerifyPackageDeployTarget,
    PlanPackageCutover,
    InstallFromPackage,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub(crate) enum RehearsalMode {
    VerifyPackageDeployTarget,
    InstallFromPackage,
    PlanPackageCutover,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageDeployVerdict {
    TinyLivePackageDeployPlanReady,
    TinyLivePackageDeployScriptRendered,
    TinyLivePackageDeployVerifyOk,
    TinyLivePackageDeployVerifyInvalid,
    TinyLivePackageDeployInstallCompleted,
    TinyLivePackageDeployInstallRefused,
    TinyLivePackageCutoverPlanReady,
    TinyLivePackageCutoverRefusedByStage3,
    TinyLivePackageCutoverRefusedByPreActivationGate,
    TinyLivePackageCutoverRefusedByInvalidPackage,
    TinyLivePackageCutoverRefusedByInvalidTarget,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageDeployReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageDeployVerdict,
    reason: String,
    package_dir: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    manifest_path: Option<String>,
    packaged_activation_config_path: Option<String>,
    packaged_activation_metadata_path: Option<String>,
    packaged_rollback_config_path: Option<String>,
    packaged_rollback_metadata_path: Option<String>,
    packaged_wrapper_path: Option<String>,
    packaged_wrapper_verification_path: Option<String>,
    install_target_summary_path: Option<String>,
    installed_wrapper_path: Option<String>,
    installed_target_config_path: Option<String>,
    installed_activation_config_path: Option<String>,
    installed_activation_metadata_path: Option<String>,
    installed_rollback_config_path: Option<String>,
    installed_rollback_metadata_path: Option<String>,
    runtime_dir: Option<String>,
    backup_dir: Option<String>,
    session_dir: Option<String>,
    package_verify_command_summary: Option<String>,
    install_from_package_command_summary: Option<String>,
    live_execute_verify_command_summary: Option<String>,
    watch_live_command_summary: Option<String>,
    cutover_plan_command_summary: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    service_control_wrapper_verification: Option<serde_json::Value>,
    activation_authorized: bool,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct DeployContractContext {
    package: VerifiedActivationPackageContract,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct PackageDeployRehearsalStep {
    pub(crate) report_json: serde_json::Value,
    pub(crate) verdict: String,
    pub(crate) reason: String,
    pub(crate) generated_at: DateTime<Utc>,
    pub(crate) installed_wrapper_path: Option<String>,
    pub(crate) installed_target_config_path: Option<String>,
    pub(crate) installed_activation_config_path: Option<String>,
    pub(crate) installed_activation_metadata_path: Option<String>,
    pub(crate) installed_rollback_config_path: Option<String>,
    pub(crate) installed_rollback_metadata_path: Option<String>,
    pub(crate) runtime_dir: Option<String>,
    pub(crate) backup_dir: Option<String>,
    pub(crate) session_dir: Option<String>,
    pub(crate) current_pre_activation_gate_verdict: Option<String>,
    pub(crate) current_pre_activation_gate_reason: Option<String>,
    pub(crate) activation_authorized: bool,
}

#[derive(Debug, Clone)]
struct InstallTargetView {
    wrapper_path: String,
    target_config_path: String,
    installed_activation_config_path: String,
    installed_activation_metadata_path: String,
    installed_rollback_config_path: String,
    installed_rollback_metadata_path: String,
    runtime_dir: String,
    backup_dir: String,
    session_dir: String,
    live_execute_verify_command_summary: String,
    watch_live_command_summary: String,
    cutover_plan_command_summary: String,
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
            "--output" => {
                output_path = Some(PathBuf::from(parse_string_arg("--output", args.next())?))
            }
            "--json" => json = true,
            "--plan-package-deploy" => {
                set_mode(&mut mode, Mode::PlanPackageDeploy, "--plan-package-deploy")?
            }
            "--render-package-deploy-script" => set_mode(
                &mut mode,
                Mode::RenderPackageDeployScript,
                "--render-package-deploy-script",
            )?,
            "--verify-package-deploy-target" => set_mode(
                &mut mode,
                Mode::VerifyPackageDeployTarget,
                "--verify-package-deploy-target",
            )?,
            "--plan-package-cutover" => set_mode(
                &mut mode,
                Mode::PlanPackageCutover,
                "--plan-package-cutover",
            )?,
            "--install-from-package" => set_mode(
                &mut mode,
                Mode::InstallFromPackage,
                "--install-from-package",
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
    if matches!(mode, Mode::RenderPackageDeployScript) && output_path.is_none() {
        bail!("missing required --output for --render-package-deploy-script");
    }

    Ok(Some(Config {
        mode,
        package_dir,
        install_root,
        target_service_name,
        backend_command,
        wrapper_timeout_ms,
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

#[allow(dead_code)]
pub(crate) fn run_mode_for_rehearsal(
    package_dir: &Path,
    install_root: &Path,
    target_service_name: &str,
    backend_command: &str,
    wrapper_timeout_ms: u64,
    mode: RehearsalMode,
) -> Result<PackageDeployRehearsalStep> {
    let config = Config {
        mode: match mode {
            RehearsalMode::VerifyPackageDeployTarget => Mode::VerifyPackageDeployTarget,
            RehearsalMode::InstallFromPackage => Mode::InstallFromPackage,
            RehearsalMode::PlanPackageCutover => Mode::PlanPackageCutover,
        },
        package_dir: package_dir.to_path_buf(),
        install_root: install_root.to_path_buf(),
        target_service_name: target_service_name.to_string(),
        backend_command: backend_command.to_string(),
        wrapper_timeout_ms,
        output_path: None,
        json: true,
    };
    let output = run(config)?;
    let report: PackageDeployReport =
        serde_json::from_str(&output).context("failed parsing package deploy report")?;
    Ok(PackageDeployRehearsalStep {
        report_json: serde_json::to_value(&report)?,
        verdict: serialize_enum(&report.verdict),
        reason: report.reason.clone(),
        generated_at: report.generated_at,
        installed_wrapper_path: report.installed_wrapper_path.clone(),
        installed_target_config_path: report.installed_target_config_path.clone(),
        installed_activation_config_path: report.installed_activation_config_path.clone(),
        installed_activation_metadata_path: report.installed_activation_metadata_path.clone(),
        installed_rollback_config_path: report.installed_rollback_config_path.clone(),
        installed_rollback_metadata_path: report.installed_rollback_metadata_path.clone(),
        runtime_dir: report.runtime_dir.clone(),
        backup_dir: report.backup_dir.clone(),
        session_dir: report.session_dir.clone(),
        current_pre_activation_gate_verdict: report.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: report.current_pre_activation_gate_reason.clone(),
        activation_authorized: report.activation_authorized,
    })
}

fn run(config: Config) -> Result<String> {
    let report = match config.mode {
        Mode::PlanPackageDeploy => match plan_package_deploy_report(&config) {
            Ok(report) => report,
            Err(error) => refusal_report(
                &config,
                TinyLivePackageDeployVerdict::TinyLivePackageDeployVerifyInvalid,
                error.to_string(),
            ),
        },
        Mode::RenderPackageDeployScript => match render_package_deploy_script_report(&config) {
            Ok(report) => report,
            Err(error) => refusal_report(
                &config,
                TinyLivePackageDeployVerdict::TinyLivePackageDeployInstallRefused,
                error.to_string(),
            ),
        },
        Mode::VerifyPackageDeployTarget => match verify_package_deploy_target_report(&config) {
            Ok(report) => report,
            Err(error) => refusal_report(
                &config,
                TinyLivePackageDeployVerdict::TinyLivePackageDeployVerifyInvalid,
                error.to_string(),
            ),
        },
        Mode::PlanPackageCutover => match plan_package_cutover_report(&config) {
            Ok(report) => report,
            Err(error) => refusal_report(
                &config,
                TinyLivePackageDeployVerdict::TinyLivePackageCutoverRefusedByInvalidPackage,
                error.to_string(),
            ),
        },
        Mode::InstallFromPackage => match install_from_package_report(&config) {
            Ok(report) => report,
            Err(error) => refusal_report(
                &config,
                TinyLivePackageDeployVerdict::TinyLivePackageDeployInstallRefused,
                error.to_string(),
            ),
        },
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_human_report(&report))
    }
}

fn plan_package_deploy_report(config: &Config) -> Result<PackageDeployReport> {
    let contract = verify_package_contract(config)?;
    Ok(base_report(
        config,
        Some(&contract),
        TinyLivePackageDeployVerdict::TinyLivePackageDeployPlanReady,
        format!(
            "package {} is ready to deploy as the single immutable handoff input for install root {}",
            config.package_dir.display(),
            config.install_root.display()
        ),
        None,
        None,
    ))
}

fn verify_package_deploy_target_report(config: &Config) -> Result<PackageDeployReport> {
    let contract = verify_package_contract(config)?;
    Ok(base_report(
        config,
        Some(&contract),
        TinyLivePackageDeployVerdict::TinyLivePackageDeployVerifyOk,
        format!(
            "package {} matches the requested install root/service/backend contract",
            config.package_dir.display()
        ),
        None,
        None,
    ))
}

fn render_package_deploy_script_report(config: &Config) -> Result<PackageDeployReport> {
    let plan = plan_package_deploy_report(config)?;
    if plan.verdict != TinyLivePackageDeployVerdict::TinyLivePackageDeployPlanReady {
        return Ok(plan);
    }
    let output_path = config
        .output_path
        .as_ref()
        .ok_or_else(|| anyhow!("missing --output for --render-package-deploy-script"))?;
    ensure_new_output_path(output_path)?;
    let script = format!(
        "#!/usr/bin/env bash\nset -euo pipefail\n{}\n",
        install_from_package_command_summary(config)
    );
    write_text_file(output_path, &script)?;
    #[cfg(unix)]
    set_executable(output_path)?;
    Ok(PackageDeployReport {
        generated_at: Utc::now(),
        mode: "render_package_deploy_script".to_string(),
        verdict: TinyLivePackageDeployVerdict::TinyLivePackageDeployScriptRendered,
        reason: format!(
            "package-native deploy script rendered to {} without mutating the target",
            output_path.display()
        ),
        explicit_statement:
            "rendering the package-native deploy script does not authorize or perform production activation by itself"
                .to_string(),
        ..plan
    })
}

fn install_from_package_report(config: &Config) -> Result<PackageDeployReport> {
    let contract = verify_package_contract(config)?;
    let installed =
        tiny_live_activation_install_target::install_target_from_source_paths_for_package(
            &config.install_root,
            &config.target_service_name,
            &config.backend_command,
            config.wrapper_timeout_ms,
            &contract.package.wrapper_path,
            &contract.package.activation_artifact_path,
            &contract.package.rollback_artifact_path,
        )?;
    Ok(base_report(
        config,
        Some(&contract),
        TinyLivePackageDeployVerdict::TinyLivePackageDeployInstallCompleted,
        format!(
            "package {} installed deterministically under {} using only packaged activation assets and the packaged wrapper artifact",
            config.package_dir.display(),
            config.install_root.display()
        ),
        Some(installed),
        None,
    ))
}

fn plan_package_cutover_report(config: &Config) -> Result<PackageDeployReport> {
    let contract = verify_package_contract(config)?;
    let summary = &contract.package.install_target_summary;
    let cutover = tiny_live_activation_cutover::plan_live_cutover_for_package_inputs(
        Path::new(&summary.installed_activation_config_path),
        Path::new(&summary.installed_rollback_config_path),
        Path::new(&summary.runtime_dir),
        Path::new(&summary.target_config_path),
        &summary.target_service_name,
        Path::new(&summary.wrapper_path),
        Path::new(&summary.backup_dir),
        Path::new(&summary.session_dir),
    )?;
    let verdict = match cutover.verdict.as_str() {
        "tiny_live_live_cutover_plan_ready" => {
            TinyLivePackageDeployVerdict::TinyLivePackageCutoverPlanReady
        }
        "tiny_live_live_cutover_refused_by_stage3" => {
            TinyLivePackageDeployVerdict::TinyLivePackageCutoverRefusedByStage3
        }
        "tiny_live_live_cutover_refused_by_pre_activation_gate" => {
            TinyLivePackageDeployVerdict::TinyLivePackageCutoverRefusedByPreActivationGate
        }
        _ => TinyLivePackageDeployVerdict::TinyLivePackageCutoverRefusedByInvalidTarget,
    };
    Ok(base_report(
        config,
        Some(&contract),
        verdict,
        cutover.reason.clone(),
        None,
        Some(cutover),
    ))
}

fn verify_package_contract(config: &Config) -> Result<DeployContractContext> {
    Ok(DeployContractContext {
        package: tiny_live_activation_package::verify_package_contract_for_deploy(
            &config.package_dir,
            &config.install_root,
            &config.target_service_name,
            &config.backend_command,
            config.wrapper_timeout_ms,
        )?,
    })
}

fn base_report(
    config: &Config,
    contract: Option<&DeployContractContext>,
    verdict: TinyLivePackageDeployVerdict,
    reason: String,
    installed_summary: Option<InstallTargetPackageSummary>,
    cutover_summary: Option<tiny_live_activation_cutover::PackageCutoverPlanSummary>,
) -> PackageDeployReport {
    let summary = installed_summary
        .as_ref()
        .map(install_target_view_from_installed_summary)
        .or_else(|| contract.map(install_target_view_from_contract));
    PackageDeployReport {
        generated_at: Utc::now(),
        mode: mode_name(config.mode).to_string(),
        verdict,
        reason,
        package_dir: config.package_dir.display().to_string(),
        install_root: config.install_root.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        wrapper_timeout_ms: config.wrapper_timeout_ms,
        manifest_path: contract.map(|value| value.package.manifest_path.display().to_string()),
        packaged_activation_config_path: contract.map(|value| {
            value
                .package
                .activation_artifact_path
                .display()
                .to_string()
        }),
        packaged_activation_metadata_path: contract.map(|value| {
            value
                .package
                .activation_metadata_path
                .display()
                .to_string()
        }),
        packaged_rollback_config_path: contract.map(|value| {
            value.package.rollback_artifact_path.display().to_string()
        }),
        packaged_rollback_metadata_path: contract.map(|value| {
            value.package.rollback_metadata_path.display().to_string()
        }),
        packaged_wrapper_path: contract.map(|value| value.package.wrapper_path.display().to_string()),
        packaged_wrapper_verification_path: contract.map(|value| {
            value
                .package
                .wrapper_verification_path
                .display()
                .to_string()
        }),
        install_target_summary_path: contract.map(|value| {
            value
                .package
                .install_target_summary_path
                .display()
                .to_string()
        }),
        installed_wrapper_path: summary.as_ref().map(|value| value.wrapper_path.clone()),
        installed_target_config_path: summary
            .as_ref()
            .map(|value| value.target_config_path.clone()),
        installed_activation_config_path: summary
            .as_ref()
            .map(|value| value.installed_activation_config_path.clone()),
        installed_activation_metadata_path: summary
            .as_ref()
            .map(|value| value.installed_activation_metadata_path.clone()),
        installed_rollback_config_path: summary
            .as_ref()
            .map(|value| value.installed_rollback_config_path.clone()),
        installed_rollback_metadata_path: summary
            .as_ref()
            .map(|value| value.installed_rollback_metadata_path.clone()),
        runtime_dir: summary.as_ref().map(|value| value.runtime_dir.clone()),
        backup_dir: summary.as_ref().map(|value| value.backup_dir.clone()),
        session_dir: summary.as_ref().map(|value| value.session_dir.clone()),
        package_verify_command_summary: Some(package_verify_command_summary(config)),
        install_from_package_command_summary: Some(install_from_package_command_summary(config)),
        live_execute_verify_command_summary: summary
            .as_ref()
            .map(|value| value.live_execute_verify_command_summary.clone()),
        watch_live_command_summary: summary
            .as_ref()
            .map(|value| value.watch_live_command_summary.clone()),
        cutover_plan_command_summary: cutover_summary
            .as_ref()
            .and_then(|value| value.cutover_plan_command_summary.clone())
            .or_else(|| summary.as_ref().map(|value| value.cutover_plan_command_summary.clone())),
        current_pre_activation_gate_verdict: cutover_summary
            .as_ref()
            .and_then(|value| value.current_pre_activation_gate_verdict.clone()),
        current_pre_activation_gate_reason: cutover_summary
            .as_ref()
            .and_then(|value| value.current_pre_activation_gate_reason.clone()),
        service_control_wrapper_verification: contract
            .and_then(|value| serde_json::to_value(&value.package.wrapper_verification).ok()),
        activation_authorized: cutover_summary
            .as_ref()
            .map(|value| value.activation_authorized)
            .unwrap_or(false),
        explicit_statement:
            "this package-native deploy layer plans or installs bounded activation assets from one immutable package; it does not authorize or perform real production activation by itself"
                .to_string(),
    }
}

fn refusal_report(
    config: &Config,
    verdict: TinyLivePackageDeployVerdict,
    reason: String,
) -> PackageDeployReport {
    base_report(config, None, verdict, reason, None, None)
}

fn install_target_view_from_contract(contract: &DeployContractContext) -> InstallTargetView {
    let summary = &contract.package.install_target_summary;
    InstallTargetView {
        wrapper_path: summary.wrapper_path.clone(),
        target_config_path: summary.target_config_path.clone(),
        installed_activation_config_path: summary.installed_activation_config_path.clone(),
        installed_activation_metadata_path: summary.installed_activation_metadata_path.clone(),
        installed_rollback_config_path: summary.installed_rollback_config_path.clone(),
        installed_rollback_metadata_path: summary.installed_rollback_metadata_path.clone(),
        runtime_dir: summary.runtime_dir.clone(),
        backup_dir: summary.backup_dir.clone(),
        session_dir: summary.session_dir.clone(),
        live_execute_verify_command_summary: summary.live_execute_verify_command_summary.clone(),
        watch_live_command_summary: summary.watch_live_command_summary.clone(),
        cutover_plan_command_summary: summary.cutover_plan_command_summary.clone(),
    }
}

fn install_target_view_from_installed_summary(
    summary: &InstallTargetPackageSummary,
) -> InstallTargetView {
    InstallTargetView {
        wrapper_path: summary.wrapper_path.clone(),
        target_config_path: summary.target_config_path.clone(),
        installed_activation_config_path: summary.installed_activation_config_path.clone(),
        installed_activation_metadata_path: summary.installed_activation_metadata_path.clone(),
        installed_rollback_config_path: summary.installed_rollback_config_path.clone(),
        installed_rollback_metadata_path: summary.installed_rollback_metadata_path.clone(),
        runtime_dir: summary.runtime_dir.clone(),
        backup_dir: summary.backup_dir.clone(),
        session_dir: summary.session_dir.clone(),
        live_execute_verify_command_summary: summary.live_execute_verify_command_summary.clone(),
        watch_live_command_summary: summary.watch_live_command_summary.clone(),
        cutover_plan_command_summary: summary.cutover_plan_command_summary.clone(),
    }
}

fn mode_name(mode: Mode) -> &'static str {
    match mode {
        Mode::PlanPackageDeploy => "plan_package_deploy",
        Mode::RenderPackageDeployScript => "render_package_deploy_script",
        Mode::VerifyPackageDeployTarget => "verify_package_deploy_target",
        Mode::PlanPackageCutover => "plan_package_cutover",
        Mode::InstallFromPackage => "install_from_package",
    }
}

fn package_verify_command_summary(config: &Config) -> String {
    format!(
        "copybot_tiny_live_activation_package --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --package-dir {} --verify-package",
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&config.target_service_name),
        shell_single_quote(&config.backend_command),
        config.wrapper_timeout_ms,
        shell_single_quote(&config.package_dir.display().to_string()),
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

fn render_human_report(report: &PackageDeployReport) -> String {
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
    if let Some(value) = &report.manifest_path {
        lines.push(format!("manifest_path={value}"));
    }
    if let Some(value) = &report.install_from_package_command_summary {
        lines.push(format!("install_from_package_command_summary={value}"));
    }
    if let Some(value) = &report.cutover_plan_command_summary {
        lines.push(format!("cutover_plan_command_summary={value}"));
    }
    if let Some(value) = &report.current_pre_activation_gate_verdict {
        lines.push(format!("current_pre_activation_gate_verdict={value}"));
    }
    if let Some(value) = &report.current_pre_activation_gate_reason {
        lines.push(format!("current_pre_activation_gate_reason={value}"));
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

fn ensure_new_output_path(path: &Path) -> Result<()> {
    if !path.is_absolute() {
        bail!("output path must be absolute");
    }
    if path.exists() {
        bail!("refusing to overwrite existing output {}", path.display());
    }
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("output path {} has no parent", path.display()))?;
    fs::create_dir_all(parent)
        .with_context(|| format!("failed creating output parent {}", parent.display()))
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

#[cfg(test)]
fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use copybot_config::load_from_path;
    #[test]
    fn package_deploy_plan_is_green_for_matching_fake_target() {
        let fixture = deploy_fixture("tiny_live_package_deploy_plan");
        let report = plan_package_deploy_report(&fixture.config).expect("plan");
        assert_eq!(
            report.verdict,
            TinyLivePackageDeployVerdict::TinyLivePackageDeployPlanReady
        );
    }

    #[test]
    fn verify_package_deploy_target_is_green_for_matching_fake_target() {
        let fixture = deploy_fixture("tiny_live_package_deploy_verify");
        let report = verify_package_deploy_target_report(&fixture.config).expect("verify");
        assert_eq!(
            report.verdict,
            TinyLivePackageDeployVerdict::TinyLivePackageDeployVerifyOk
        );
    }

    #[test]
    fn wrong_target_contract_is_rejected_through_package_native_flow() {
        let fixture = deploy_fixture("tiny_live_package_deploy_wrong_target");
        let wrong_root = fixture.fixture_dir.join("other-root");
        fs::create_dir_all(&wrong_root).unwrap();
        let wrong_root_report = run_json_report(Config {
            mode: Mode::VerifyPackageDeployTarget,
            install_root: wrong_root,
            output_path: None,
            ..fixture.config.clone()
        });
        assert_eq!(
            wrong_root_report.verdict,
            TinyLivePackageDeployVerdict::TinyLivePackageDeployVerifyInvalid
        );
        assert!(wrong_root_report.reason.contains("--install-root"));

        let wrong_service_report = run_json_report(Config {
            mode: Mode::VerifyPackageDeployTarget,
            target_service_name: "other.service".to_string(),
            output_path: None,
            ..fixture.config.clone()
        });
        assert_eq!(
            wrong_service_report.verdict,
            TinyLivePackageDeployVerdict::TinyLivePackageDeployVerifyInvalid
        );
        assert!(wrong_service_report.reason.contains("--target-service"));

        let wrong_backend_report = run_json_report(Config {
            mode: Mode::VerifyPackageDeployTarget,
            backend_command: "launchctl".to_string(),
            output_path: None,
            ..fixture.config.clone()
        });
        assert_eq!(
            wrong_backend_report.verdict,
            TinyLivePackageDeployVerdict::TinyLivePackageDeployVerifyInvalid
        );
        assert!(wrong_backend_report.reason.contains("--backend-command"));
    }

    #[test]
    fn tampered_package_hash_blocks_package_native_deploy() {
        let fixture = deploy_fixture("tiny_live_package_deploy_bad_hash");
        fs::write(
            fixture
                .package_dir
                .join("artifacts/rendered.activation.toml"),
            sample_safe_config_toml(),
        )
        .unwrap();
        let report = run_json_report(Config {
            mode: Mode::VerifyPackageDeployTarget,
            ..fixture.config.clone()
        });
        assert_eq!(
            report.verdict,
            TinyLivePackageDeployVerdict::TinyLivePackageDeployVerifyInvalid
        );
        assert!(report.reason.contains("hash mismatch"));
    }

    #[test]
    fn render_package_deploy_script_is_deterministic_and_non_overwriting() {
        let fixture = deploy_fixture("tiny_live_package_deploy_script");
        let output_path = fixture.fixture_dir.join("package-deploy.sh");
        let report = render_package_deploy_script_report(&Config {
            mode: Mode::RenderPackageDeployScript,
            output_path: Some(output_path.clone()),
            ..fixture.config.clone()
        })
        .expect("render");
        assert_eq!(
            report.verdict,
            TinyLivePackageDeployVerdict::TinyLivePackageDeployScriptRendered
        );
        assert!(output_path.exists());
        let script = fs::read_to_string(&output_path).unwrap();
        assert!(script.contains("--install-from-package"));
        assert!(!script.contains("copybot_tiny_live_activation_install_target"));
        assert!(!script.contains("copybot_live_service_control_wrapper"));
        let second = run(Config {
            mode: Mode::RenderPackageDeployScript,
            output_path: Some(output_path),
            json: true,
            ..fixture.config.clone()
        })
        .expect("second render");
        assert!(second.contains("tiny_live_package_deploy_install_refused"));
    }

    #[test]
    fn install_from_package_writes_only_under_fake_root() {
        let fixture = deploy_fixture("tiny_live_package_deploy_install");
        let report = install_from_package_report(&fixture.config).expect("install");
        assert_eq!(
            report.verdict,
            TinyLivePackageDeployVerdict::TinyLivePackageDeployInstallCompleted
        );
        let root = &fixture.install_root;
        assert!(root
            .join("usr/local/bin/copybot-live-service-control")
            .exists());
        assert!(root
            .join("var/lib/solana-copy-bot/tiny-live/rendered.activation.toml")
            .exists());
        assert!(root
            .join("var/lib/solana-copy-bot/tiny-live/rendered.rollback.toml")
            .exists());
        assert!(root
            .join("var/lib/solana-copy-bot/tiny-live/runtime")
            .exists());
        assert!(root
            .join("var/lib/solana-copy-bot/tiny-live/backups")
            .exists());
        assert!(root
            .join("var/lib/solana-copy-bot/tiny-live/sessions")
            .exists());
        let installed_wrapper =
            fs::read(root.join("usr/local/bin/copybot-live-service-control")).unwrap();
        let packaged_wrapper = fs::read(
            fixture
                .package_dir
                .join("wrapper/copybot-live-service-control"),
        )
        .unwrap();
        assert_eq!(installed_wrapper, packaged_wrapper);
    }

    #[test]
    fn tampered_packaged_wrapper_is_refused_for_install_from_package() {
        let fixture = deploy_fixture("tiny_live_package_deploy_bad_wrapper");
        fs::write(
            fixture
                .package_dir
                .join("wrapper/copybot-live-service-control"),
            "#!/usr/bin/env bash\necho tampered\n",
        )
        .unwrap();
        let report = run_json_report(Config {
            mode: Mode::InstallFromPackage,
            ..fixture.config.clone()
        });
        assert_eq!(
            report.verdict,
            TinyLivePackageDeployVerdict::TinyLivePackageDeployInstallRefused
        );
        assert!(
            report.reason.contains("wrapper")
                || report.reason.contains("hash mismatch")
                || report.reason.contains("verification")
        );
    }

    #[test]
    fn package_cutover_plan_is_blocked_by_current_non_green_gate() {
        let fixture = deploy_fixture("tiny_live_package_deploy_cutover_blocked");
        install_from_package_report(&fixture.config).expect("install");
        let report = plan_package_cutover_report(&fixture.config).expect("cutover");
        assert!(
            matches!(
                report.verdict,
                TinyLivePackageDeployVerdict::TinyLivePackageCutoverRefusedByStage3
                    | TinyLivePackageDeployVerdict::TinyLivePackageCutoverRefusedByPreActivationGate
            ),
            "unexpected cutover verdict: {:?}",
            report.verdict
        );
        assert!(!report.activation_authorized);
    }

    struct DeployFixture {
        fixture_dir: PathBuf,
        package_dir: PathBuf,
        install_root: PathBuf,
        config: Config,
    }

    fn deploy_fixture(label: &str) -> DeployFixture {
        let fixture_dir = temp_dir(label);
        let install_root = fixture_dir.join("root");
        let target_config_path = install_root.join("etc/solana-copy-bot/live.server.toml");
        fs::create_dir_all(target_config_path.parent().unwrap()).unwrap();
        fs::write(&target_config_path, sample_safe_config_toml()).unwrap();

        let source_dir = fixture_dir.join("source");
        fs::create_dir_all(&source_dir).unwrap();
        let activation_config_source_path = source_dir.join("rendered.activation.toml");
        let rollback_config_source_path = source_dir.join("rendered.rollback.toml");
        write_rendered_artifact_pair(
            &target_config_path,
            &activation_config_source_path,
            &rollback_config_source_path,
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

        DeployFixture {
            fixture_dir,
            package_dir: package_dir.clone(),
            install_root: install_root.clone(),
            config: Config {
                mode: Mode::PlanPackageDeploy,
                package_dir,
                install_root,
                target_service_name: "solana-copy-bot.service".to_string(),
                backend_command: "systemctl".to_string(),
                wrapper_timeout_ms: tiny_live_activation_live_execute::live_service_control_wrapper_contract::DEFAULT_TIMEOUT_MS,
                output_path: None,
                json: false,
            },
        }
    }

    fn write_rendered_artifact_pair(
        target_config_path: &Path,
        activation_path: &Path,
        rollback_path: &Path,
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
            sample_activation_config_toml(),
            target_config_path,
            &fingerprint.sha256,
            false,
            true,
        );
        write_rendered_artifact(
            rollback_path,
            tiny_live_activation_live_execute::tiny_live_activation_execute::RenderKind::Rollback,
            sample_safe_config_toml(),
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
            rendered_config_sha256: sha256_hex(contents.as_bytes()),
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

    fn sample_activation_config_toml() -> String {
        sample_safe_config_toml().replacen("enabled = false", "enabled = true", 1)
    }

    fn sample_safe_config_toml() -> String {
        r#"
[system]
env = "prod-live"

[sqlite]
path = "/tmp/copybot-live.db"

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
"#
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

    fn run_json_report(config: Config) -> PackageDeployReport {
        let output = run(Config {
            json: true,
            ..config
        })
        .expect("run");
        serde_json::from_str(&output).expect("json report")
    }
}
