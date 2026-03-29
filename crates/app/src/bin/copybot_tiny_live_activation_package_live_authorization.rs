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
#[path = "copybot_tiny_live_activation_package_live_cutover.rs"]
mod tiny_live_activation_package_live_cutover;
#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_package_preflight.rs"]
mod tiny_live_activation_package_preflight;

const USAGE: &str = "usage: copybot_tiny_live_activation_package_live_authorization --package-dir <path> --install-root <path> --target-service <name> --backend-command <path-or-name> [--wrapper-timeout-ms <ms>] [--service-status-max-staleness-ms <ms>] [--session-dir <path>] [--output <path>] [--json] (--plan-live-package-authorization | --render-live-package-authorization-script | --run-live-package-authorization | --verify-live-package-authorization)";
const AUTHORIZATION_SESSION_VERSION: &str = "1";
const AUTHORIZATION_STATUS_VERSION: &str = "1";
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
    service_status_max_staleness_ms: u64,
    session_dir: Option<PathBuf>,
    output_path: Option<PathBuf>,
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    PlanLivePackageAuthorization,
    RenderLivePackageAuthorizationScript,
    RunLivePackageAuthorization,
    VerifyLivePackageAuthorization,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageLiveAuthorizationVerdict {
    TinyLivePackageLiveAuthorizationPlanReady,
    TinyLivePackageLiveAuthorizationScriptRendered,
    TinyLivePackageLiveAuthorizationAuthorizedNow,
    TinyLivePackageLiveAuthorizationRefusedByStage3,
    TinyLivePackageLiveAuthorizationRefusedByPreActivationGate,
    TinyLivePackageLiveAuthorizationRefusedByInvalidTarget,
    TinyLivePackageLiveAuthorizationVerifyOk,
    TinyLivePackageLiveAuthorizationVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LivePackageAuthorizationResult {
    AuthorizedNow,
    RefusedByStage3,
    RefusedByPreActivationGate,
    RefusedByInvalidTarget,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageLiveAuthorizationStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageLiveAuthorizationSession {
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
    gate_evaluation_command_summary: String,
    authorized_live_cutover_command_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageLiveAuthorizationStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    session_dir: String,
    nested_preflight_session_dir: String,
    result: LivePackageAuthorizationResult,
    reason: String,
    preflight_step: Option<PackageLiveAuthorizationStepArtifact>,
    gate_step: Option<PackageLiveAuthorizationStepArtifact>,
    controller_plan_step: Option<PackageLiveAuthorizationStepArtifact>,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackageLiveAuthorizationPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    preflight_report_path: PathBuf,
    gate_report_path: PathBuf,
    controller_plan_report_path: PathBuf,
    preflight_session_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageLiveAuthorizationReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageLiveAuthorizationVerdict,
    reason: String,
    package_dir: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    session_dir: Option<String>,
    authorization_session_path: Option<String>,
    authorization_status_path: Option<String>,
    nested_preflight_session_dir: Option<String>,
    result: Option<String>,
    preflight_step: Option<PackageLiveAuthorizationStepArtifact>,
    gate_step: Option<PackageLiveAuthorizationStepArtifact>,
    controller_plan_step: Option<PackageLiveAuthorizationStepArtifact>,
    run_preflight_command_summary: String,
    gate_evaluation_command_summary: String,
    authorized_live_cutover_command_summary: String,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    verification_mismatches: Vec<String>,
    activation_authorized: bool,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PackageLiveAuthorizationLaunchPacketStep {
    pub(crate) report_json: serde_json::Value,
    pub(crate) verdict: String,
    pub(crate) reason: String,
    pub(crate) generated_at: DateTime<Utc>,
    pub(crate) result: Option<String>,
    pub(crate) current_pre_activation_gate_verdict: Option<String>,
    pub(crate) current_pre_activation_gate_reason: Option<String>,
    pub(crate) gate_evaluation_command_summary: String,
    pub(crate) authorized_live_cutover_command_summary: String,
    pub(crate) activation_authorized: bool,
}

#[derive(Debug, Deserialize)]
struct StoredPreflightReportView {
    package_dir: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    session_dir: Option<String>,
    result: Option<String>,
    cutover_plan_step: Option<PackageLiveAuthorizationStepArtifact>,
}

#[derive(Debug, Clone, Deserialize)]
struct ResolvedInstallTargetPaths {
    wrapper_path: String,
    target_config_path: String,
    installed_activation_config_path: String,
    installed_rollback_config_path: String,
    runtime_dir: String,
    backup_dir: String,
}

#[allow(dead_code)]
pub(crate) fn run_live_package_authorization_for_launch_packet(
    package_dir: &Path,
    install_root: &Path,
    target_service_name: &str,
    backend_command: &str,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    session_dir: &Path,
) -> Result<PackageLiveAuthorizationLaunchPacketStep> {
    let config = Config {
        mode: Mode::RunLivePackageAuthorization,
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
    let report = run_live_package_authorization_report(&config)?;
    package_live_authorization_launch_packet_step(report)
}

#[allow(dead_code)]
pub(crate) fn verify_live_package_authorization_for_launch_packet(
    package_dir: &Path,
    install_root: &Path,
    target_service_name: &str,
    backend_command: &str,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    session_dir: &Path,
) -> Result<PackageLiveAuthorizationLaunchPacketStep> {
    let config = Config {
        mode: Mode::VerifyLivePackageAuthorization,
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
    let report = verify_live_package_authorization_report(&config)?;
    package_live_authorization_launch_packet_step(report)
}

fn package_live_authorization_launch_packet_step(
    report: PackageLiveAuthorizationReport,
) -> Result<PackageLiveAuthorizationLaunchPacketStep> {
    Ok(PackageLiveAuthorizationLaunchPacketStep {
        report_json: serde_json::to_value(&report)?,
        verdict: serialize_enum(&report.verdict),
        reason: report.reason.clone(),
        generated_at: report.generated_at,
        result: report.result.clone(),
        current_pre_activation_gate_verdict: report.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: report.current_pre_activation_gate_reason.clone(),
        gate_evaluation_command_summary: report.gate_evaluation_command_summary.clone(),
        authorized_live_cutover_command_summary: report
            .authorized_live_cutover_command_summary
            .clone(),
        activation_authorized: report.activation_authorized,
    })
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
            "--plan-live-package-authorization" => set_mode(
                &mut mode,
                Mode::PlanLivePackageAuthorization,
                "--plan-live-package-authorization",
            )?,
            "--render-live-package-authorization-script" => set_mode(
                &mut mode,
                Mode::RenderLivePackageAuthorizationScript,
                "--render-live-package-authorization-script",
            )?,
            "--run-live-package-authorization" => set_mode(
                &mut mode,
                Mode::RunLivePackageAuthorization,
                "--run-live-package-authorization",
            )?,
            "--verify-live-package-authorization" => set_mode(
                &mut mode,
                Mode::VerifyLivePackageAuthorization,
                "--verify-live-package-authorization",
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

    if matches!(mode, Mode::RenderLivePackageAuthorizationScript) && output_path.is_none() {
        bail!("missing required --output for --render-live-package-authorization-script");
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

fn set_mode(slot: &mut Option<Mode>, mode: Mode, flag: &str) -> Result<()> {
    if slot.replace(mode).is_some() {
        bail!("multiple modes provided; {flag} conflicts with an earlier mode");
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
        Mode::PlanLivePackageAuthorization => match plan_live_package_authorization_report(&config) {
            Ok(report) => report,
            Err(error) => refusal_report(
                &config,
                TinyLivePackageLiveAuthorizationVerdict::TinyLivePackageLiveAuthorizationRefusedByInvalidTarget,
                error.to_string(),
            ),
        },
        Mode::RenderLivePackageAuthorizationScript => {
            match render_live_package_authorization_script_report(&config) {
                Ok(report) => report,
                Err(error) => refusal_report(
                    &config,
                    TinyLivePackageLiveAuthorizationVerdict::TinyLivePackageLiveAuthorizationRefusedByInvalidTarget,
                    error.to_string(),
                ),
            }
        }
        Mode::RunLivePackageAuthorization => match run_live_package_authorization_report(&config) {
            Ok(report) => report,
            Err(error) => refusal_report(
                &config,
                TinyLivePackageLiveAuthorizationVerdict::TinyLivePackageLiveAuthorizationRefusedByInvalidTarget,
                error.to_string(),
            ),
        },
        Mode::VerifyLivePackageAuthorization => {
            match verify_live_package_authorization_report(&config) {
                Ok(report) => report,
                Err(error) => refusal_report(
                    &config,
                    TinyLivePackageLiveAuthorizationVerdict::TinyLivePackageLiveAuthorizationVerifyInvalid,
                    error.to_string(),
                ),
            }
        }
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_report_lines(&report))
    }
}

fn plan_live_package_authorization_report(
    config: &Config,
) -> Result<PackageLiveAuthorizationReport> {
    let _ = tiny_live_activation_package::verify_package_contract_for_deploy(
        &config.package_dir,
        &config.install_root,
        &config.target_service_name,
        &config.backend_command,
        config.wrapper_timeout_ms,
    )?;
    let controller_view = controller_plan_view(config);
    if controller_view.verdict != "tiny_live_package_live_cutover_plan_ready" {
        bail!("{}", controller_view.reason);
    }
    Ok(base_report(
        config,
        TinyLivePackageLiveAuthorizationVerdict::TinyLivePackageLiveAuthorizationPlanReady,
        format!(
            "package-native live authorization session is explicit for install root {}; current Stage 3 truth is still evaluated only at run time and remains the hard go/no-go boundary",
            config.install_root.display()
        ),
        &controller_view,
    ))
}

fn render_live_package_authorization_script_report(
    config: &Config,
) -> Result<PackageLiveAuthorizationReport> {
    let plan = plan_live_package_authorization_report(config)?;
    let output_path = config.output_path.as_ref().ok_or_else(|| {
        anyhow!("missing --output for --render-live-package-authorization-script")
    })?;
    ensure_new_output_path(output_path)?;
    let session_dir = config
        .session_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("<session-dir>"));
    let script = format!(
        "#!/usr/bin/env bash\nset -euo pipefail\n\ncopybot_tiny_live_activation_package_live_authorization --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --service-status-max-staleness-ms {} --plan-live-package-authorization\ncopybot_tiny_live_activation_package_live_authorization --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --service-status-max-staleness-ms {} --session-dir {} --run-live-package-authorization\ncopybot_tiny_live_activation_package_live_authorization --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --service-status-max-staleness-ms {} --session-dir {} --verify-live-package-authorization\n",
        shell_single_quote(&config.package_dir.display().to_string()),
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&config.target_service_name),
        shell_single_quote(&config.backend_command),
        config.wrapper_timeout_ms,
        config.service_status_max_staleness_ms,
        shell_single_quote(&config.package_dir.display().to_string()),
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&config.target_service_name),
        shell_single_quote(&config.backend_command),
        config.wrapper_timeout_ms,
        config.service_status_max_staleness_ms,
        shell_single_quote(&session_dir.display().to_string()),
        shell_single_quote(&config.package_dir.display().to_string()),
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&config.target_service_name),
        shell_single_quote(&config.backend_command),
        config.wrapper_timeout_ms,
        config.service_status_max_staleness_ms,
        shell_single_quote(&session_dir.display().to_string()),
    );
    write_text_file(output_path, &script)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(output_path)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(output_path, perms)?;
    }
    Ok(PackageLiveAuthorizationReport {
        verdict:
            TinyLivePackageLiveAuthorizationVerdict::TinyLivePackageLiveAuthorizationScriptRendered,
        reason: format!(
            "rendered package-native live authorization script to {}",
            output_path.display()
        ),
        ..plan
    })
}

fn run_live_package_authorization_report(
    config: &Config,
) -> Result<PackageLiveAuthorizationReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for --run-live-package-authorization"))?;
    ensure_clean_live_package_authorization_session_dir(session_dir)?;
    let paths = package_live_authorization_paths(session_dir);
    let controller_view = controller_plan_view(config);
    let session = PackageLiveAuthorizationSession {
        session_version: AUTHORIZATION_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        package_dir: config.package_dir.display().to_string(),
        install_root: config.install_root.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        wrapper_timeout_ms: config.wrapper_timeout_ms,
        service_status_max_staleness_ms: config.service_status_max_staleness_ms,
        session_dir: session_dir.display().to_string(),
        nested_preflight_session_dir: paths.preflight_session_dir.display().to_string(),
        run_preflight_command_summary: run_preflight_command_summary(config, &paths.preflight_session_dir),
        gate_evaluation_command_summary: controller_view.gate_evaluation_command_summary.clone(),
        authorized_live_cutover_command_summary: controller_view
            .run_live_cutover_command_summary
            .clone(),
        explicit_statement:
            "this package-native live authorization session binds one immutable package, one explicit live target contract, and one final live cutover controller into one go/no-go artifact; current Stage 3 / pre-activation truth remains the hard authorization boundary"
                .to_string(),
    };
    persist_json(&paths.session_path, &session)?;
    persist_json(&paths.controller_plan_report_path, &controller_view)?;
    let controller_plan_step = Some(step_artifact(
        &paths.controller_plan_report_path,
        "plan_live_cutover_controller",
        &controller_view.verdict,
        &controller_view.reason,
        Utc::now(),
    ));

    let preflight =
        tiny_live_activation_package_preflight::run_live_package_preflight_for_capability(
            &config.package_dir,
            &config.install_root,
            &config.target_service_name,
            &config.backend_command,
            config.wrapper_timeout_ms,
            config.service_status_max_staleness_ms,
            &paths.preflight_session_dir,
            None,
        )?;
    persist_json_value(&paths.preflight_report_path, &preflight.report_json)?;
    let preflight_step = Some(step_artifact(
        &paths.preflight_report_path,
        "run_live_package_preflight",
        &preflight.verdict,
        &preflight.reason,
        preflight.generated_at,
    ));
    let stored_preflight: StoredPreflightReportView =
        serde_json::from_value(preflight.report_json.clone())
            .context("failed parsing nested preflight report json")?;

    let maybe_package_contract = tiny_live_activation_package::verify_package_contract_for_deploy(
        &config.package_dir,
        &config.install_root,
        &config.target_service_name,
        &config.backend_command,
        config.wrapper_timeout_ms,
    )
    .ok();
    let maybe_install_summary = maybe_package_contract
        .as_ref()
        .map(|value| resolve_install_target_paths(&value.install_target_summary));
    let gate_report = maybe_install_summary.as_ref().map(|install_summary| {
        tiny_live_activation_live_execute::build_plan_live_report_for_drill(
            Path::new(&install_summary.installed_activation_config_path),
            Path::new(&install_summary.installed_rollback_config_path),
            Path::new(&install_summary.target_config_path),
            &config.target_service_name,
            Path::new(&install_summary.wrapper_path),
            Path::new(&install_summary.runtime_dir),
            Path::new(&install_summary.backup_dir),
            DEFAULT_STARTUP_TIMEOUT_MS,
            DEFAULT_ROLLBACK_TIMEOUT_MS,
        )
    });
    let gate_report = match gate_report.transpose()? {
        Some(value) => {
            persist_json(&paths.gate_report_path, &value)?;
            Some(value)
        }
        None => None,
    };
    let gate_step = gate_report.as_ref().map(|report| {
        step_artifact(
            &paths.gate_report_path,
            "evaluate_live_gate",
            &serialize_enum(&report.verdict),
            &report.reason,
            report.generated_at,
        )
    });

    let (
        result,
        verdict,
        reason,
        current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason,
        activation_authorized,
    ) = if controller_view.verdict != "tiny_live_package_live_cutover_plan_ready" {
        (
                LivePackageAuthorizationResult::RefusedByInvalidTarget,
                TinyLivePackageLiveAuthorizationVerdict::TinyLivePackageLiveAuthorizationRefusedByInvalidTarget,
                controller_view.reason.clone(),
                None,
                None,
                false,
            )
    } else if !preflight_allows_live_authorization(&stored_preflight) {
        (
                LivePackageAuthorizationResult::RefusedByInvalidTarget,
                TinyLivePackageLiveAuthorizationVerdict::TinyLivePackageLiveAuthorizationRefusedByInvalidTarget,
                preflight.reason.clone(),
                preflight.current_pre_activation_gate_verdict.clone(),
                preflight.current_pre_activation_gate_reason.clone(),
                false,
            )
    } else if let Some(report) = gate_report.as_ref() {
        match report.verdict {
                tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLivePlanReady
                    if preflight_proves_authorized_now(&stored_preflight) => (
                    LivePackageAuthorizationResult::AuthorizedNow,
                    TinyLivePackageLiveAuthorizationVerdict::TinyLivePackageLiveAuthorizationAuthorizedNow,
                    "current Stage 3 / pre-activation truth is green and the exact package-native live cutover controller is authorized now".to_string(),
                    report.current_pre_activation_gate_verdict.clone(),
                    report.current_pre_activation_gate_reason.clone(),
                    true,
                ),
                tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLivePlanReady => (
                    LivePackageAuthorizationResult::RefusedByInvalidTarget,
                    TinyLivePackageLiveAuthorizationVerdict::TinyLivePackageLiveAuthorizationRefusedByInvalidTarget,
                    "gate step is plan-ready but nested live package preflight did not prove genuine green authorization truth".to_string(),
                    report.current_pre_activation_gate_verdict.clone(),
                    report.current_pre_activation_gate_reason.clone(),
                    false,
                ),
                tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyRefusedByStage3 => (
                    LivePackageAuthorizationResult::RefusedByStage3,
                    TinyLivePackageLiveAuthorizationVerdict::TinyLivePackageLiveAuthorizationRefusedByStage3,
                    report.reason.clone(),
                    report.current_pre_activation_gate_verdict.clone(),
                    report.current_pre_activation_gate_reason.clone(),
                    false,
                ),
                tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyRefusedByPreActivationGate => (
                    LivePackageAuthorizationResult::RefusedByPreActivationGate,
                    TinyLivePackageLiveAuthorizationVerdict::TinyLivePackageLiveAuthorizationRefusedByPreActivationGate,
                    report.reason.clone(),
                    report.current_pre_activation_gate_verdict.clone(),
                    report.current_pre_activation_gate_reason.clone(),
                    false,
                ),
                _ => (
                    LivePackageAuthorizationResult::RefusedByInvalidTarget,
                    TinyLivePackageLiveAuthorizationVerdict::TinyLivePackageLiveAuthorizationRefusedByInvalidTarget,
                    report.reason.clone(),
                    report.current_pre_activation_gate_verdict.clone(),
                    report.current_pre_activation_gate_reason.clone(),
                    false,
                ),
            }
    } else {
        (
                LivePackageAuthorizationResult::RefusedByInvalidTarget,
                TinyLivePackageLiveAuthorizationVerdict::TinyLivePackageLiveAuthorizationRefusedByInvalidTarget,
                "live authorization could not derive a bounded gate-evaluation step from the requested package/live target contract".to_string(),
                preflight.current_pre_activation_gate_verdict.clone(),
                preflight.current_pre_activation_gate_reason.clone(),
                false,
            )
    };

    let status = PackageLiveAuthorizationStatus {
        status_version: AUTHORIZATION_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        session_dir: session_dir.display().to_string(),
        nested_preflight_session_dir: paths.preflight_session_dir.display().to_string(),
        result,
        reason,
        preflight_step,
        gate_step,
        controller_plan_step,
        explicit_statement:
            "this package-native live authorization session binds one immutable package, one explicit live target contract, and one final live cutover controller into one go/no-go artifact; current Stage 3 / pre-activation truth remains the hard authorization boundary"
                .to_string(),
    };
    persist_json(&paths.status_path, &status)?;
    Ok(report_from_status(
        config,
        session_dir,
        &paths,
        verdict,
        &status,
        current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason,
        activation_authorized,
    ))
}

fn verify_live_package_authorization_report(
    config: &Config,
) -> Result<PackageLiveAuthorizationReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for --verify-live-package-authorization"))?;
    let package_contract = match tiny_live_activation_package::verify_package_contract_for_deploy(
        &config.package_dir,
        &config.install_root,
        &config.target_service_name,
        &config.backend_command,
        config.wrapper_timeout_ms,
    ) {
        Ok(value) => value,
        Err(error) => {
            let paths = package_live_authorization_paths(session_dir);
            return Ok(PackageLiveAuthorizationReport {
                generated_at: Utc::now(),
                mode: "verify_live_package_authorization".to_string(),
                verdict: TinyLivePackageLiveAuthorizationVerdict::TinyLivePackageLiveAuthorizationVerifyInvalid,
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
                authorization_session_path: Some(paths.session_path.display().to_string()),
                authorization_status_path: Some(paths.status_path.display().to_string()),
                nested_preflight_session_dir: Some(paths.preflight_session_dir.display().to_string()),
                result: None,
                preflight_step: None,
                gate_step: None,
                controller_plan_step: None,
                run_preflight_command_summary: run_preflight_command_summary(config, &paths.preflight_session_dir),
                gate_evaluation_command_summary: gate_evaluation_command_summary_placeholder(config),
                authorized_live_cutover_command_summary: controller_plan_view(config)
                    .run_live_cutover_command_summary,
                current_pre_activation_gate_verdict: None,
                current_pre_activation_gate_reason: None,
                verification_mismatches: vec![error.to_string()],
                activation_authorized: false,
                explicit_statement:
                    "this package-native live authorization verification checks bounded package/gate/controller evidence only; it does not authorize activation by itself"
                        .to_string(),
            });
        }
    };
    let install_summary = resolve_install_target_paths(&package_contract.install_target_summary);
    let paths = package_live_authorization_paths(session_dir);
    let session: PackageLiveAuthorizationSession = load_json(&paths.session_path)?;
    let status: PackageLiveAuthorizationStatus = load_json(&paths.status_path)?;
    let mut mismatches = Vec::new();

    compare_string(
        &session.package_dir,
        &config.package_dir.display().to_string(),
        "package live authorization package_dir",
        &mut mismatches,
    );
    compare_string(
        &session.install_root,
        &config.install_root.display().to_string(),
        "package live authorization install_root",
        &mut mismatches,
    );
    compare_string(
        &session.target_service_name,
        &config.target_service_name,
        "package live authorization target_service_name",
        &mut mismatches,
    );
    compare_string(
        &session.backend_command,
        &config.backend_command,
        "package live authorization backend_command",
        &mut mismatches,
    );
    if session.wrapper_timeout_ms != config.wrapper_timeout_ms {
        mismatches.push(format!(
            "package live authorization wrapper_timeout_ms {} does not match requested {}",
            session.wrapper_timeout_ms, config.wrapper_timeout_ms
        ));
    }
    if session.service_status_max_staleness_ms != config.service_status_max_staleness_ms {
        mismatches.push(format!(
            "package live authorization service_status_max_staleness_ms {} does not match requested {}",
            session.service_status_max_staleness_ms, config.service_status_max_staleness_ms
        ));
    }
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "package live authorization session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.nested_preflight_session_dir,
        &paths.preflight_session_dir.display().to_string(),
        "package live authorization nested_preflight_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.session_dir,
        &session_dir.display().to_string(),
        "package live authorization status session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.nested_preflight_session_dir,
        &paths.preflight_session_dir.display().to_string(),
        "package live authorization status nested_preflight_session_dir",
        &mut mismatches,
    );

    let expected_controller_view = controller_plan_view(config);
    let stored_controller_view: tiny_live_activation_package_live_cutover::LiveCutoverControllerPlanView =
        load_required_step_json(
            &status.controller_plan_step,
            &paths.controller_plan_report_path,
            "controller_plan_step",
            &mut mismatches,
        )?;
    validate_controller_plan_view_contract(
        &stored_controller_view,
        &expected_controller_view,
        &mut mismatches,
    );
    compare_string(
        &session.authorized_live_cutover_command_summary,
        &expected_controller_view.run_live_cutover_command_summary,
        "package live authorization authorized_live_cutover_command_summary",
        &mut mismatches,
    );
    compare_string(
        &session.gate_evaluation_command_summary,
        &expected_controller_view.gate_evaluation_command_summary,
        "package live authorization gate_evaluation_command_summary",
        &mut mismatches,
    );
    compare_string(
        &session.run_preflight_command_summary,
        &run_preflight_command_summary(config, &paths.preflight_session_dir),
        "package live authorization run_preflight_command_summary",
        &mut mismatches,
    );

    let stored_preflight: StoredPreflightReportView = load_required_step_json(
        &status.preflight_step,
        &paths.preflight_report_path,
        "preflight_step",
        &mut mismatches,
    )?;
    validate_preflight_report_contract(&stored_preflight, config, &paths, &mut mismatches);
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
    let gate_report = match &status.gate_step {
        Some(step) => Some(load_bound_step_json::<
            tiny_live_activation_live_execute::LiveExecuteReport,
        >(
            step,
            &paths.gate_report_path,
            "gate_step",
            &mut mismatches,
        )?),
        None => None,
    };
    if let Some(report) = gate_report.as_ref() {
        validate_live_execute_report_contract(
            report,
            &install_summary,
            config,
            "gate step",
            &mut mismatches,
        );
    }

    let mut current_pre_activation_gate_verdict = verified_preflight
        .current_pre_activation_gate_verdict
        .clone();
    let mut current_pre_activation_gate_reason = verified_preflight
        .current_pre_activation_gate_reason
        .clone();

    match status.result {
        LivePackageAuthorizationResult::AuthorizedNow => {
            if !preflight_allows_live_authorization(&stored_preflight) {
                mismatches.push(
                    "authorized_now session must carry a green/eligible nested preflight result"
                        .to_string(),
                );
            }
            if !preflight_proves_authorized_now(&stored_preflight) {
                mismatches.push(
                    "authorized_now session must store nested preflight ready_for_cutover_planning"
                        .to_string(),
                );
            }
            if !verified_preflight_proves_authorized_now(&verified_preflight) {
                mismatches.push(
                    "authorized_now session must verify nested preflight ready_for_cutover_planning with activation_authorized=true"
                        .to_string(),
                );
            }
            if stored_controller_view.verdict != "tiny_live_package_live_cutover_plan_ready" {
                mismatches.push(
                    "authorized_now session must carry a plan-ready live cutover controller contract"
                        .to_string(),
                );
            }
            let report = gate_report.as_ref().ok_or_else(|| {
                anyhow!("missing required package live authorization step gate_step")
            })?;
            if report.verdict
                != tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLivePlanReady
            {
                mismatches.push(
                    "authorized_now session must carry a plan-ready gate evaluation step"
                        .to_string(),
                );
            }
            current_pre_activation_gate_verdict =
                report.current_pre_activation_gate_verdict.clone();
            current_pre_activation_gate_reason = report.current_pre_activation_gate_reason.clone();
        }
        LivePackageAuthorizationResult::RefusedByStage3 => {
            if !preflight_allows_live_authorization(&stored_preflight) {
                mismatches.push(
                    "refused_by_stage3 session must still carry an eligible nested preflight result"
                        .to_string(),
                );
            }
            if stored_controller_view.verdict != "tiny_live_package_live_cutover_plan_ready" {
                mismatches.push(
                    "refused_by_stage3 session must still carry a plan-ready live cutover controller contract"
                        .to_string(),
                );
            }
            let report = gate_report.as_ref().ok_or_else(|| {
                anyhow!("missing required package live authorization step gate_step")
            })?;
            if report.verdict
                != tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyRefusedByStage3
            {
                mismatches.push(
                    "refused_by_stage3 session must carry a stage3-refused gate evaluation step"
                        .to_string(),
                );
            }
            current_pre_activation_gate_verdict =
                report.current_pre_activation_gate_verdict.clone();
            current_pre_activation_gate_reason = report.current_pre_activation_gate_reason.clone();
        }
        LivePackageAuthorizationResult::RefusedByPreActivationGate => {
            if !preflight_allows_live_authorization(&stored_preflight) {
                mismatches.push(
                    "refused_by_pre_activation_gate session must still carry an eligible nested preflight result"
                        .to_string(),
                );
            }
            if stored_controller_view.verdict != "tiny_live_package_live_cutover_plan_ready" {
                mismatches.push(
                    "refused_by_pre_activation_gate session must still carry a plan-ready live cutover controller contract"
                        .to_string(),
                );
            }
            let report = gate_report.as_ref().ok_or_else(|| {
                anyhow!("missing required package live authorization step gate_step")
            })?;
            if report.verdict
                != tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyRefusedByPreActivationGate
            {
                mismatches.push(
                    "refused_by_pre_activation_gate session must carry a pre-activation-refused gate evaluation step"
                        .to_string(),
                );
            }
            current_pre_activation_gate_verdict =
                report.current_pre_activation_gate_verdict.clone();
            current_pre_activation_gate_reason = report.current_pre_activation_gate_reason.clone();
        }
        LivePackageAuthorizationResult::RefusedByInvalidTarget => {
            let controller_invalid =
                stored_controller_view.verdict != "tiny_live_package_live_cutover_plan_ready";
            let preflight_invalid = !preflight_allows_live_authorization(&stored_preflight);
            let gate_invalid = gate_report.as_ref().is_some_and(|report| {
                !matches!(
                    report.verdict,
                    tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLivePlanReady
                        | tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyRefusedByStage3
                        | tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyRefusedByPreActivationGate
                )
            });
            if !(controller_invalid || preflight_invalid || gate_invalid || gate_report.is_none()) {
                mismatches.push(
                    "refused_by_invalid_target session must carry an invalid controller contract, non-eligible preflight result, or invalid gate step"
                        .to_string(),
                );
            }
            if let Some(report) = gate_report.as_ref() {
                current_pre_activation_gate_verdict =
                    report.current_pre_activation_gate_verdict.clone();
                current_pre_activation_gate_reason =
                    report.current_pre_activation_gate_reason.clone();
            }
        }
    }

    let activation_authorized = mismatches.is_empty()
        && status.result == LivePackageAuthorizationResult::AuthorizedNow
        && verified_preflight_proves_authorized_now(&verified_preflight);
    let verdict = if mismatches.is_empty() {
        TinyLivePackageLiveAuthorizationVerdict::TinyLivePackageLiveAuthorizationVerifyOk
    } else {
        TinyLivePackageLiveAuthorizationVerdict::TinyLivePackageLiveAuthorizationVerifyInvalid
    };
    let reason = if mismatches.is_empty() {
        format!(
            "package-native live authorization session under {} is coherent and bound to the requested package/live target/controller contract",
            session_dir.display()
        )
    } else {
        mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "package-native live authorization verification failed".to_string())
    };
    Ok(PackageLiveAuthorizationReport {
        generated_at: Utc::now(),
        mode: "verify_live_package_authorization".to_string(),
        verdict,
        reason,
        package_dir: config.package_dir.display().to_string(),
        install_root: config.install_root.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        wrapper_timeout_ms: config.wrapper_timeout_ms,
        service_status_max_staleness_ms: config.service_status_max_staleness_ms,
        session_dir: Some(session_dir.display().to_string()),
        authorization_session_path: Some(paths.session_path.display().to_string()),
        authorization_status_path: Some(paths.status_path.display().to_string()),
        nested_preflight_session_dir: Some(paths.preflight_session_dir.display().to_string()),
        result: Some(serialize_enum(&status.result)),
        preflight_step: status.preflight_step,
        gate_step: status.gate_step,
        controller_plan_step: status.controller_plan_step,
        run_preflight_command_summary: run_preflight_command_summary(config, &paths.preflight_session_dir),
        gate_evaluation_command_summary: expected_controller_view.gate_evaluation_command_summary,
        authorized_live_cutover_command_summary: expected_controller_view
            .run_live_cutover_command_summary,
        current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason,
        verification_mismatches: mismatches,
        activation_authorized,
        explicit_statement:
            "this package-native live authorization verification checks bounded package/gate/controller evidence only; it does not authorize activation by itself"
                .to_string(),
    })
}

fn controller_plan_view(
    config: &Config,
) -> tiny_live_activation_package_live_cutover::LiveCutoverControllerPlanView {
    tiny_live_activation_package_live_cutover::live_cutover_controller_plan_view_for_authorization(
        &config.package_dir,
        &config.install_root,
        &config.target_service_name,
        &config.backend_command,
        config.wrapper_timeout_ms,
        config.service_status_max_staleness_ms,
    )
}

fn base_report(
    config: &Config,
    verdict: TinyLivePackageLiveAuthorizationVerdict,
    reason: String,
    controller_view: &tiny_live_activation_package_live_cutover::LiveCutoverControllerPlanView,
) -> PackageLiveAuthorizationReport {
    let session_dir = config.session_dir.as_ref();
    let paths = session_dir.map(|path| package_live_authorization_paths(path));
    PackageLiveAuthorizationReport {
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
        session_dir: session_dir.map(|path| path.display().to_string()),
        authorization_session_path: paths
            .as_ref()
            .map(|value| value.session_path.display().to_string()),
        authorization_status_path: paths
            .as_ref()
            .map(|value| value.status_path.display().to_string()),
        nested_preflight_session_dir: paths
            .as_ref()
            .map(|value| value.preflight_session_dir.display().to_string()),
        result: None,
        preflight_step: None,
        gate_step: None,
        controller_plan_step: None,
        run_preflight_command_summary: run_preflight_command_summary(
            config,
            &paths
                .as_ref()
                .map(|value| value.preflight_session_dir.clone())
                .unwrap_or_else(|| {
                    PathBuf::from(
                        "<session-dir>/tiny_live_activation_package_live_authorization.preflight_session",
                    )
                }),
        ),
        gate_evaluation_command_summary: controller_view.gate_evaluation_command_summary.clone(),
        authorized_live_cutover_command_summary: controller_view
            .run_live_cutover_command_summary
            .clone(),
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        verification_mismatches: Vec::new(),
        activation_authorized: false,
        explicit_statement:
            "this package-native live authorization session binds one immutable package, one explicit live target contract, and one final live cutover controller into one go/no-go artifact; current Stage 3 / pre-activation truth still remains the hard authorization boundary"
                .to_string(),
    }
}

fn refusal_report(
    config: &Config,
    verdict: TinyLivePackageLiveAuthorizationVerdict,
    reason: String,
) -> PackageLiveAuthorizationReport {
    let controller_view = controller_plan_view(config);
    base_report(config, verdict, reason, &controller_view)
}

fn report_from_status(
    config: &Config,
    session_dir: &Path,
    paths: &PackageLiveAuthorizationPaths,
    verdict: TinyLivePackageLiveAuthorizationVerdict,
    status: &PackageLiveAuthorizationStatus,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    activation_authorized: bool,
) -> PackageLiveAuthorizationReport {
    let controller_view = controller_plan_view(config);
    PackageLiveAuthorizationReport {
        generated_at: Utc::now(),
        mode: "run_live_package_authorization".to_string(),
        verdict,
        reason: status.reason.clone(),
        package_dir: config.package_dir.display().to_string(),
        install_root: config.install_root.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        wrapper_timeout_ms: config.wrapper_timeout_ms,
        service_status_max_staleness_ms: config.service_status_max_staleness_ms,
        session_dir: Some(session_dir.display().to_string()),
        authorization_session_path: Some(paths.session_path.display().to_string()),
        authorization_status_path: Some(paths.status_path.display().to_string()),
        nested_preflight_session_dir: Some(paths.preflight_session_dir.display().to_string()),
        result: Some(serialize_enum(&status.result)),
        preflight_step: status.preflight_step.clone(),
        gate_step: status.gate_step.clone(),
        controller_plan_step: status.controller_plan_step.clone(),
        run_preflight_command_summary: run_preflight_command_summary(config, &paths.preflight_session_dir),
        gate_evaluation_command_summary: controller_view.gate_evaluation_command_summary,
        authorized_live_cutover_command_summary: controller_view.run_live_cutover_command_summary,
        current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason,
        verification_mismatches: Vec::new(),
        activation_authorized,
        explicit_statement:
            "this package-native live authorization session produces one deterministic go/no-go artifact only; it still does not authorize activation unless the current gate is genuinely green"
                .to_string(),
    }
}

fn mode_name(mode: Mode) -> &'static str {
    match mode {
        Mode::PlanLivePackageAuthorization => "plan_live_package_authorization",
        Mode::RenderLivePackageAuthorizationScript => "render_live_package_authorization_script",
        Mode::RunLivePackageAuthorization => "run_live_package_authorization",
        Mode::VerifyLivePackageAuthorization => "verify_live_package_authorization",
    }
}

fn step_artifact(
    path: &Path,
    mode: &str,
    verdict: &str,
    reason: &str,
    generated_at: DateTime<Utc>,
) -> PackageLiveAuthorizationStepArtifact {
    PackageLiveAuthorizationStepArtifact {
        path: path.display().to_string(),
        mode: mode.to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        generated_at,
    }
}

fn package_live_authorization_paths(session_dir: &Path) -> PackageLiveAuthorizationPaths {
    PackageLiveAuthorizationPaths {
        session_path: session_dir
            .join("tiny_live_activation_package_live_authorization.session.json"),
        status_path: session_dir
            .join("tiny_live_activation_package_live_authorization.status.json"),
        preflight_report_path: session_dir
            .join("tiny_live_activation_package_live_authorization.preflight.report.json"),
        gate_report_path: session_dir
            .join("tiny_live_activation_package_live_authorization.gate.report.json"),
        controller_plan_report_path: session_dir
            .join("tiny_live_activation_package_live_authorization.controller.report.json"),
        preflight_session_dir: session_dir
            .join("tiny_live_activation_package_live_authorization.preflight_session"),
    }
}

fn ensure_clean_live_package_authorization_session_dir(session_dir: &Path) -> Result<()> {
    let paths = package_live_authorization_paths(session_dir);
    for path in [
        &paths.session_path,
        &paths.status_path,
        &paths.preflight_report_path,
        &paths.gate_report_path,
        &paths.controller_plan_report_path,
    ] {
        if path.exists() {
            bail!(
                "refusing to reuse package live authorization session dir {}; artifact {} already exists",
                session_dir.display(),
                path.display()
            );
        }
    }
    if paths.preflight_session_dir.exists() {
        bail!(
            "refusing to reuse package live authorization session dir {}; artifact {} already exists",
            session_dir.display(),
            paths.preflight_session_dir.display()
        );
    }
    fs::create_dir_all(session_dir)
        .with_context(|| format!("failed creating session dir {}", session_dir.display()))
}

fn ensure_new_output_path(path: &Path) -> Result<()> {
    if path.exists() {
        bail!("refusing to overwrite existing output {}", path.display());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed creating parent dir {}", parent.display()))?;
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

fn load_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed reading json {}", path.display()))?;
    serde_json::from_str(&raw).with_context(|| format!("failed parsing json {}", path.display()))
}

fn persist_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let raw = serde_json::to_string_pretty(value)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed creating parent dir {}", parent.display()))?;
    }
    fs::write(path, raw).with_context(|| format!("failed writing {}", path.display()))
}

fn persist_json_value(path: &Path, value: &serde_json::Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed creating parent dir {}", parent.display()))?;
    }
    fs::write(path, serde_json::to_string_pretty(value)?)
        .with_context(|| format!("failed writing {}", path.display()))
}

fn load_required_step_json<T: for<'de> Deserialize<'de>>(
    step: &Option<PackageLiveAuthorizationStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<T> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("missing required package live authorization step {label}"))?;
    load_bound_step_json(step, expected_path, label, mismatches)
}

fn load_bound_step_json<T: for<'de> Deserialize<'de>>(
    step: &PackageLiveAuthorizationStepArtifact,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<T> {
    let expected = expected_path.display().to_string();
    if step.path != expected {
        mismatches.push(format!(
            "package live authorization {label} path {} does not match deterministic session artifact {}",
            step.path, expected
        ));
    }
    load_json(expected_path).with_context(|| {
        format!(
            "failed reading deterministic package live authorization {label} report {}",
            expected_path.display()
        )
    })
}

fn resolve_install_target_paths<S>(summary: &S) -> ResolvedInstallTargetPaths
where
    S: Serialize,
{
    let value = serde_json::to_value(summary)
        .expect("serializing install target summary for path resolution should succeed");
    serde_json::from_value(value)
        .expect("install target summary shape should match resolved install target paths")
}

fn compare_string(actual: &str, expected: &str, label: &str, mismatches: &mut Vec<String>) {
    if actual != expected {
        mismatches.push(format!(
            "{label} {actual} does not match expected {expected}"
        ));
    }
}

fn serialize_enum<T: Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "unknown".to_string())
}

fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
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

fn gate_evaluation_command_summary_placeholder(config: &Config) -> String {
    format!(
        "copybot_tiny_live_activation_live_execute --activation-config <installed-activation-config> --rollback-config <installed-rollback-config> --target-config <installed-target-config> --target-service {} --service-control-command <installed-wrapper> --runtime-dir <installed-runtime-dir> --backup-dir <installed-backup-dir> --plan-live",
        shell_single_quote(&config.target_service_name),
    )
}

fn validate_preflight_report_contract(
    report: &StoredPreflightReportView,
    config: &Config,
    paths: &PackageLiveAuthorizationPaths,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &report.package_dir,
        &config.package_dir.display().to_string(),
        "stored preflight package_dir",
        mismatches,
    );
    compare_string(
        &report.install_root,
        &config.install_root.display().to_string(),
        "stored preflight install_root",
        mismatches,
    );
    compare_string(
        &report.target_service_name,
        &config.target_service_name,
        "stored preflight target_service_name",
        mismatches,
    );
    compare_string(
        &report.backend_command,
        &config.backend_command,
        "stored preflight backend_command",
        mismatches,
    );
    if report.wrapper_timeout_ms != config.wrapper_timeout_ms {
        mismatches.push(format!(
            "stored preflight wrapper_timeout_ms {} does not match requested {}",
            report.wrapper_timeout_ms, config.wrapper_timeout_ms
        ));
    }
    if report.service_status_max_staleness_ms != config.service_status_max_staleness_ms {
        mismatches.push(format!(
            "stored preflight service_status_max_staleness_ms {} does not match requested {}",
            report.service_status_max_staleness_ms, config.service_status_max_staleness_ms
        ));
    }
    compare_string(
        report.session_dir.as_deref().unwrap_or_default(),
        &paths.preflight_session_dir.display().to_string(),
        "stored preflight session_dir",
        mismatches,
    );
}

fn preflight_allows_live_authorization(report: &StoredPreflightReportView) -> bool {
    match report.result.as_deref() {
        Some("ready_for_cutover_planning") | Some("cutover_blocked_by_gate") => true,
        Some("failed_during_live_target_verify") => {
            report.cutover_plan_step.as_ref().is_some_and(|step| {
                step.verdict == "tiny_live_package_cutover_refused_by_missing_backup"
            })
        }
        _ => false,
    }
}

fn preflight_proves_authorized_now(report: &StoredPreflightReportView) -> bool {
    report.result.as_deref() == Some("ready_for_cutover_planning")
}

fn verified_preflight_proves_authorized_now(
    report: &tiny_live_activation_package_preflight::PackagePreflightCapabilityStep,
) -> bool {
    report.result.as_deref() == Some("ready_for_cutover_planning") && report.activation_authorized
}

fn validate_live_execute_report_contract(
    report: &tiny_live_activation_live_execute::LiveExecuteReport,
    install_summary: &ResolvedInstallTargetPaths,
    config: &Config,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &report.activation_config_path,
        &install_summary.installed_activation_config_path,
        &format!("{label} activation_config_path"),
        mismatches,
    );
    compare_string(
        &report.rollback_config_path,
        &install_summary.installed_rollback_config_path,
        &format!("{label} rollback_config_path"),
        mismatches,
    );
    compare_string(
        &report.target_config_path,
        &install_summary.target_config_path,
        &format!("{label} target_config_path"),
        mismatches,
    );
    compare_string(
        &report.target_service_name,
        &config.target_service_name,
        &format!("{label} target_service_name"),
        mismatches,
    );
    compare_string(
        &report.service_control_command_path,
        &install_summary.wrapper_path,
        &format!("{label} service_control_command_path"),
        mismatches,
    );
    compare_string(
        &report.runtime_dir,
        &install_summary.runtime_dir,
        &format!("{label} runtime_dir"),
        mismatches,
    );
    compare_string(
        &report.backup_dir,
        &install_summary.backup_dir,
        &format!("{label} backup_dir"),
        mismatches,
    );
}

fn validate_controller_plan_view_contract(
    stored: &tiny_live_activation_package_live_cutover::LiveCutoverControllerPlanView,
    expected: &tiny_live_activation_package_live_cutover::LiveCutoverControllerPlanView,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &stored.mode,
        &expected.mode,
        "controller plan mode",
        mismatches,
    );
    compare_string(
        &stored.verdict,
        &expected.verdict,
        "controller plan verdict",
        mismatches,
    );
    compare_string(
        &stored.reason,
        &expected.reason,
        "controller plan reason",
        mismatches,
    );
    compare_string(
        &stored.package_dir,
        &expected.package_dir,
        "controller plan package_dir",
        mismatches,
    );
    compare_string(
        &stored.install_root,
        &expected.install_root,
        "controller plan install_root",
        mismatches,
    );
    compare_string(
        &stored.target_service_name,
        &expected.target_service_name,
        "controller plan target_service_name",
        mismatches,
    );
    compare_string(
        &stored.backend_command,
        &expected.backend_command,
        "controller plan backend_command",
        mismatches,
    );
    if stored.wrapper_timeout_ms != expected.wrapper_timeout_ms {
        mismatches.push(format!(
            "controller plan wrapper_timeout_ms {} does not match expected {}",
            stored.wrapper_timeout_ms, expected.wrapper_timeout_ms
        ));
    }
    if stored.service_status_max_staleness_ms != expected.service_status_max_staleness_ms {
        mismatches.push(format!(
            "controller plan service_status_max_staleness_ms {} does not match expected {}",
            stored.service_status_max_staleness_ms, expected.service_status_max_staleness_ms
        ));
    }
    compare_string(
        &stored.run_preflight_command_summary,
        &expected.run_preflight_command_summary,
        "controller plan run_preflight_command_summary",
        mismatches,
    );
    compare_string(
        &stored.gate_evaluation_command_summary,
        &expected.gate_evaluation_command_summary,
        "controller plan gate_evaluation_command_summary",
        mismatches,
    );
    compare_string(
        &stored.backup_proof_command_summary,
        &expected.backup_proof_command_summary,
        "controller plan backup_proof_command_summary",
        mismatches,
    );
    compare_string(
        &stored.run_live_cutover_command_summary,
        &expected.run_live_cutover_command_summary,
        "controller plan run_live_cutover_command_summary",
        mismatches,
    );
    compare_string(
        &stored.explicit_statement,
        &expected.explicit_statement,
        "controller plan explicit_statement",
        mismatches,
    );
}

fn render_report_lines(report: &PackageLiveAuthorizationReport) -> String {
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
    if let Some(value) = &report.authorization_session_path {
        lines.push(format!("authorization_session_path={value}"));
    }
    if let Some(value) = &report.authorization_status_path {
        lines.push(format!("authorization_status_path={value}"));
    }
    if let Some(value) = &report.nested_preflight_session_dir {
        lines.push(format!("nested_preflight_session_dir={value}"));
    }
    if let Some(value) = &report.result {
        lines.push(format!("result={value}"));
    }
    lines.push(format!(
        "run_preflight_command_summary={}",
        report.run_preflight_command_summary
    ));
    lines.push(format!(
        "gate_evaluation_command_summary={}",
        report.gate_evaluation_command_summary
    ));
    lines.push(format!(
        "authorized_live_cutover_command_summary={}",
        report.authorized_live_cutover_command_summary
    ));
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
    if !report.verification_mismatches.is_empty() {
        lines.push("verification_mismatches=".to_string());
        for mismatch in &report.verification_mismatches {
            lines.push(format!("- {mismatch}"));
        }
    }
    lines.push(format!("explicit_statement={}", report.explicit_statement));
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::tiny_live_activation_install_target;
    use super::tiny_live_activation_live_execute;
    use super::tiny_live_activation_package;
    use super::tiny_live_activation_package_preflight;
    use super::*;
    use copybot_config::load_from_path;
    use copybot_storage::{
        DiscoveryWalletFreshnessCaptureWrite, ExecutionDryRunRehearsalWrite, SqliteStore,
    };
    use sha2::{Digest, Sha256};
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn green_gate_authorizes_now() {
        let fixture = authorization_fixture(
            "tiny_live_activation_package_live_authorization_green",
            GateState::Green,
        );
        let report = run_json_report(fixture.config.clone());
        assert_eq!(
            report.verdict,
            TinyLivePackageLiveAuthorizationVerdict::TinyLivePackageLiveAuthorizationAuthorizedNow
        );
        assert_eq!(report.result.as_deref(), Some("authorized_now"));
        assert!(report.activation_authorized);
    }

    #[test]
    fn green_run_then_verify_stays_authorized_now() {
        let fixture = authorization_fixture(
            "tiny_live_activation_package_live_authorization_verify_green",
            GateState::Green,
        );
        let run_report = run_json_report(fixture.config.clone());
        assert_eq!(
            run_report.verdict,
            TinyLivePackageLiveAuthorizationVerdict::TinyLivePackageLiveAuthorizationAuthorizedNow
        );

        let verify = verify_live_package_authorization_report(&fixture.config).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageLiveAuthorizationVerdict::TinyLivePackageLiveAuthorizationVerifyOk,
            "verification mismatches: {:?}",
            verify.verification_mismatches
        );
        assert_eq!(verify.result.as_deref(), Some("authorized_now"));
        assert!(verify.activation_authorized);
    }

    #[test]
    fn stage3_non_green_refuses_authorization() {
        let fixture = authorization_fixture(
            "tiny_live_activation_package_live_authorization_stage3",
            GateState::Stage3Blocked,
        );
        let report = run_json_report(fixture.config.clone());
        assert_eq!(
            report.verdict,
            TinyLivePackageLiveAuthorizationVerdict::TinyLivePackageLiveAuthorizationRefusedByStage3
        );
        assert_eq!(report.result.as_deref(), Some("refused_by_stage3"));
        assert!(!report.activation_authorized);
    }

    #[test]
    fn pre_activation_gate_non_green_refuses_authorization() {
        let fixture = authorization_fixture(
            "tiny_live_activation_package_live_authorization_pre_gate",
            GateState::PreActivationBlocked,
        );
        let report = run_json_report(fixture.config.clone());
        assert_eq!(
            report.verdict,
            TinyLivePackageLiveAuthorizationVerdict::TinyLivePackageLiveAuthorizationRefusedByPreActivationGate
        );
        assert_eq!(
            report.result.as_deref(),
            Some("refused_by_pre_activation_gate")
        );
        assert!(!report.activation_authorized);
    }

    #[test]
    fn tampered_installed_wrapper_is_refused_as_invalid_target() {
        let fixture = authorization_fixture(
            "tiny_live_activation_package_live_authorization_invalid_wrapper",
            GateState::Green,
        );
        let wrapper_path = fixture
            .config
            .install_root
            .join("usr/local/bin/copybot-live-service-control");
        fs::write(&wrapper_path, "#!/usr/bin/env bash\necho tampered\n").unwrap();
        let report = run_json_report(fixture.config.clone());
        assert_eq!(
            report.verdict,
            TinyLivePackageLiveAuthorizationVerdict::TinyLivePackageLiveAuthorizationRefusedByInvalidTarget
        );
        assert_eq!(report.result.as_deref(), Some("refused_by_invalid_target"));
    }

    #[test]
    fn verify_rejects_tampered_controller_plan_step_path() {
        let fixture = authorization_fixture(
            "tiny_live_activation_package_live_authorization_verify_tampered_path",
            GateState::Green,
        );
        run_json_report(fixture.config.clone());
        let paths = package_live_authorization_paths(fixture.config.session_dir.as_ref().unwrap());
        let mut status: PackageLiveAuthorizationStatus = load_json(&paths.status_path).unwrap();
        let other = fixture.fixture_dir.join("other.report.json");
        fs::write(&other, "{}").unwrap();
        status.controller_plan_step.as_mut().unwrap().path = other.display().to_string();
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_live_package_authorization_report(&fixture.config).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageLiveAuthorizationVerdict::TinyLivePackageLiveAuthorizationVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_mismatched_target_contract() {
        let fixture = authorization_fixture(
            "tiny_live_activation_package_live_authorization_verify_target_mismatch",
            GateState::Green,
        );
        run_json_report(fixture.config.clone());
        let verify = verify_live_package_authorization_report(&Config {
            target_service_name: "other.service".to_string(),
            ..fixture.config
        })
        .unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageLiveAuthorizationVerdict::TinyLivePackageLiveAuthorizationVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_authorized_now_when_nested_preflight_still_gate_blocked() {
        let fixture = authorization_fixture(
            "tiny_live_activation_package_live_authorization_verify_gate_tamper",
            GateState::Stage3Blocked,
        );
        let run_report = run_json_report(fixture.config.clone());
        assert_eq!(
            run_report.verdict,
            TinyLivePackageLiveAuthorizationVerdict::TinyLivePackageLiveAuthorizationRefusedByStage3
        );

        let paths = package_live_authorization_paths(fixture.config.session_dir.as_ref().unwrap());
        let mut status: PackageLiveAuthorizationStatus = load_json(&paths.status_path).unwrap();
        status.result = LivePackageAuthorizationResult::AuthorizedNow;
        status.reason = "tampered authorized_now".to_string();
        persist_json(&paths.status_path, &status).unwrap();

        let mut gate_report: tiny_live_activation_live_execute::LiveExecuteReport =
            load_json(&paths.gate_report_path).unwrap();
        gate_report.verdict =
            tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLivePlanReady;
        gate_report.reason = "tampered plan ready gate".to_string();
        persist_json(&paths.gate_report_path, &gate_report).unwrap();

        let verify = verify_live_package_authorization_report(&fixture.config).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageLiveAuthorizationVerdict::TinyLivePackageLiveAuthorizationVerifyInvalid
        );
        assert!(!verify.activation_authorized);
        assert!(verify.verification_mismatches.iter().any(|mismatch| {
            mismatch.contains("nested preflight ready_for_cutover_planning")
                || mismatch.contains("nested preflight activation_authorized")
        }));
    }

    #[derive(Debug, Clone, Copy)]
    enum GateState {
        Green,
        Stage3Blocked,
        PreActivationBlocked,
    }

    struct AuthorizationFixture {
        fixture_dir: PathBuf,
        config: Config,
        _rpc_server: MockHttpServer,
        _adapter_server: MockHttpServer,
    }

    fn authorization_fixture(label: &str, gate_state: GateState) -> AuthorizationFixture {
        let fixture_dir = temp_dir(label);
        let install_root = fixture_dir.join("live-root");
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
            None,
        );
        let backend_path = fixture_dir.join("fake-live-backend.sh");
        write_fake_backend(&backend_path);
        let disabled_contents = dynamic_live_config_toml(
            &db_path,
            &rpc_server.url(""),
            &adapter_server.url("/submit"),
            false,
        );
        fs::write(&target_config_path, &disabled_contents).unwrap();
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
        .unwrap();
        tiny_live_activation_install_target::install_target_from_source_paths_for_package(
            &install_root,
            "solana-copy-bot.service",
            &backend_path.display().to_string(),
            tiny_live_activation_live_execute::live_service_control_wrapper_contract::DEFAULT_TIMEOUT_MS,
            &package_dir.join("wrapper/copybot-live-service-control"),
            &package_dir.join("artifacts/rendered.activation.toml"),
            &package_dir.join("artifacts/rendered.rollback.toml"),
        )
        .unwrap();

        AuthorizationFixture {
            fixture_dir: fixture_dir.clone(),
            config: Config {
                mode: Mode::RunLivePackageAuthorization,
                package_dir,
                install_root,
                target_service_name: "solana-copy-bot.service".to_string(),
                backend_command: backend_path.display().to_string(),
                wrapper_timeout_ms: tiny_live_activation_live_execute::live_service_control_wrapper_contract::DEFAULT_TIMEOUT_MS,
                service_status_max_staleness_ms:
                    tiny_live_activation_package_preflight::DEFAULT_SERVICE_STATUS_MAX_STALENESS_MS,
                session_dir: Some(fixture_dir.join("live-authorization-session")),
                output_path: None,
                json: false,
            },
            _rpc_server: rpc_server,
            _adapter_server: adapter_server,
        }
    }

    fn run_json_report(config: Config) -> PackageLiveAuthorizationReport {
        let output = run(Config {
            json: true,
            ..config
        })
        .unwrap();
        serde_json::from_str(&output).unwrap()
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
        let hash = format!("{:x}", Sha256::digest(contents.as_bytes()));
        let metadata =
            tiny_live_activation_live_execute::tiny_live_activation_execute::RenderedConfigMetadata {
                metadata_version: "1".to_string(),
                render_kind,
                generated_at: ts("2026-03-28T12:00:00Z"),
                input_config_path: source_config_path.display().to_string(),
                output_config_path: path.display().to_string(),
                source_config_fingerprint_scope:
                    tiny_live_activation_live_execute::tiny_live_activation_execute::FINGERPRINT_SCOPE
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
            GateState::PreActivationBlocked => {
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
                        now - chrono::Duration::days(2),
                        "stale_rehearsal",
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

    fn rehearsal_write(rehearsed_at: DateTime<Utc>, label: &str) -> ExecutionDryRunRehearsalWrite {
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
            adapter_result_classification: label.to_string(),
            adapter_accepted: Some(true),
            adapter_detail: label.to_string(),
            policy_echo_present: true,
            route_echo_present: true,
            contract_version_echo_present: true,
            response_slippage_bps: None,
            response_tip_lamports: None,
            response_compute_unit_limit: None,
            response_compute_unit_price_micro_lamports: None,
            verdict: label.to_string(),
            reason: label.to_string(),
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

    fn dynamic_live_config_toml(
        db_path: &Path,
        rpc_url: &str,
        adapter_url: &str,
        execution_enabled: bool,
    ) -> String {
        let mut contents = sample_safe_config_toml(db_path)
            .replace("https://rpc.example", rpc_url)
            .replace("http://127.0.0.1:8080/submit", adapter_url);
        if execution_enabled {
            contents = contents.replacen("enabled = false", "enabled = true", 1);
        }
        contents
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

    fn temp_dir(label: &str) -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        loop {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
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
        _handle: std::thread::JoinHandle<()>,
    }

    impl MockHttpServer {
        fn spawn(max_requests: usize, body: String, required_header: Option<&str>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let required_header = required_header.map(str::to_owned);
            let body = Arc::new(body);
            let handle = std::thread::spawn(move || {
                for _ in 0..max_requests {
                    let (mut stream, _) = listener.accept().unwrap();
                    let mut buffer = [0u8; 8192];
                    let bytes_read = stream.read(&mut buffer).unwrap_or(0);
                    let request = String::from_utf8_lossy(&buffer[..bytes_read]);
                    if let Some(required) = required_header.as_deref() {
                        assert!(
                            request.lines().any(|line| line
                                .eq_ignore_ascii_case(&format!("x-copybot-env: {required}"))),
                            "request missing required x-copybot-env header: {request}",
                        );
                    }
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    stream.write_all(response.as_bytes()).unwrap();
                    stream.flush().unwrap();
                }
            });
            Self {
                addr,
                _handle: handle,
            }
        }

        fn url(&self, path: &str) -> String {
            format!("http://{}{}", self.addr, path)
        }
    }
}
