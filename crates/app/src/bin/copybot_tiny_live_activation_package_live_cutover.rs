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
#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_package_preflight.rs"]
mod tiny_live_activation_package_preflight;

const USAGE: &str = "usage: copybot_tiny_live_activation_package_live_cutover --package-dir <path> --install-root <path> --target-service <name> --backend-command <path-or-name> [--wrapper-timeout-ms <ms>] [--service-status-max-staleness-ms <ms>] [--session-dir <path>] [--output <path>] [--json] (--plan-live-package-cutover | --render-live-package-cutover-script | --run-live-package-cutover | --verify-live-package-cutover)";
const LIVE_CUTOVER_SESSION_VERSION: &str = "1";
const LIVE_CUTOVER_STATUS_VERSION: &str = "1";
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
    PlanLivePackageCutover,
    RenderLivePackageCutoverScript,
    RunLivePackageCutover,
    VerifyLivePackageCutover,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageLiveCutoverVerdict {
    TinyLivePackageLiveCutoverPlanReady,
    TinyLivePackageLiveCutoverScriptRendered,
    TinyLivePackageLiveCutoverRefusedByStage3,
    TinyLivePackageLiveCutoverRefusedByPreActivationGate,
    TinyLivePackageLiveCutoverRefusedByInvalidTarget,
    TinyLivePackageLiveCutoverCompletedKeepRunning,
    TinyLivePackageLiveCutoverCompletedWithRollback,
    TinyLivePackageLiveCutoverCompletedBackupFailed,
    TinyLivePackageLiveCutoverCompletedApplyFailed,
    TinyLivePackageLiveCutoverCompletedWatchFailed,
    TinyLivePackageLiveCutoverVerifyOk,
    TinyLivePackageLiveCutoverVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LivePackageCutoverResult {
    RefusedByStage3,
    RefusedByPreActivationGate,
    RefusedByInvalidTarget,
    CompletedKeepRunning,
    CompletedWithRollback,
    CompletedBackupFailed,
    CompletedApplyFailed,
    CompletedWatchFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageLiveCutoverStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageLiveCutoverSession {
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
    nested_cutover_session_dir: String,
    run_preflight_command_summary: String,
    gate_evaluation_command_summary: String,
    backup_proof_command_summary: String,
    run_live_cutover_command_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageLiveCutoverStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    session_dir: String,
    nested_preflight_session_dir: String,
    nested_cutover_session_dir: String,
    result: LivePackageCutoverResult,
    reason: String,
    preflight_step: Option<PackageLiveCutoverStepArtifact>,
    gate_step: Option<PackageLiveCutoverStepArtifact>,
    backup_step: Option<PackageLiveCutoverStepArtifact>,
    live_cutover_step: Option<PackageLiveCutoverStepArtifact>,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackageLiveCutoverPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    preflight_report_path: PathBuf,
    gate_report_path: PathBuf,
    backup_report_path: PathBuf,
    live_cutover_report_path: PathBuf,
    preflight_session_dir: PathBuf,
    nested_cutover_session_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageLiveCutoverReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageLiveCutoverVerdict,
    reason: String,
    package_dir: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    session_dir: Option<String>,
    live_cutover_session_path: Option<String>,
    live_cutover_status_path: Option<String>,
    nested_preflight_session_dir: Option<String>,
    nested_cutover_session_dir: Option<String>,
    result: Option<String>,
    preflight_step: Option<PackageLiveCutoverStepArtifact>,
    gate_step: Option<PackageLiveCutoverStepArtifact>,
    backup_step: Option<PackageLiveCutoverStepArtifact>,
    live_cutover_step: Option<PackageLiveCutoverStepArtifact>,
    run_preflight_command_summary: String,
    gate_evaluation_command_summary: String,
    backup_proof_command_summary: String,
    run_live_cutover_command_summary: String,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    verification_mismatches: Vec<String>,
    activation_authorized: bool,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LiveCutoverControllerPlanView {
    pub(crate) mode: String,
    pub(crate) verdict: String,
    pub(crate) reason: String,
    pub(crate) package_dir: String,
    pub(crate) install_root: String,
    pub(crate) target_service_name: String,
    pub(crate) backend_command: String,
    pub(crate) wrapper_timeout_ms: u64,
    pub(crate) service_status_max_staleness_ms: u64,
    pub(crate) run_preflight_command_summary: String,
    pub(crate) gate_evaluation_command_summary: String,
    pub(crate) backup_proof_command_summary: String,
    pub(crate) run_live_cutover_command_summary: String,
    pub(crate) explicit_statement: String,
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
    cutover_plan_step: Option<PackageLiveCutoverStepArtifact>,
}

#[derive(Debug, Deserialize)]
struct CutoverReportView {
    activation_config_path: String,
    rollback_config_path: String,
    runtime_dir: String,
    target_config_path: String,
    target_service_name: String,
    service_control_command_path: String,
    backup_dir: String,
    session_dir: Option<String>,
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

#[derive(Default)]
struct LiveCutoverRunHooks<'a> {
    before_backup_hook: Option<&'a dyn Fn(&ResolvedInstallTargetPaths) -> Result<()>>,
    before_cutover_hook: Option<&'a dyn Fn(&ResolvedInstallTargetPaths) -> Result<()>>,
    cutover_observation_seed:
        Option<&'a tiny_live_activation_cutover::CutoverRehearsalObservationSeed>,
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
            "--plan-live-package-cutover" => set_mode(
                &mut mode,
                Mode::PlanLivePackageCutover,
                "--plan-live-package-cutover",
            )?,
            "--render-live-package-cutover-script" => set_mode(
                &mut mode,
                Mode::RenderLivePackageCutoverScript,
                "--render-live-package-cutover-script",
            )?,
            "--run-live-package-cutover" => set_mode(
                &mut mode,
                Mode::RunLivePackageCutover,
                "--run-live-package-cutover",
            )?,
            "--verify-live-package-cutover" => set_mode(
                &mut mode,
                Mode::VerifyLivePackageCutover,
                "--verify-live-package-cutover",
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

    if matches!(mode, Mode::RenderLivePackageCutoverScript) && output_path.is_none() {
        bail!("missing required --output for --render-live-package-cutover-script");
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
        Mode::PlanLivePackageCutover => match plan_live_package_cutover_report(&config) {
            Ok(report) => report,
            Err(error) => refusal_report(
                &config,
                TinyLivePackageLiveCutoverVerdict::TinyLivePackageLiveCutoverRefusedByInvalidTarget,
                error.to_string(),
            ),
        },
        Mode::RenderLivePackageCutoverScript => match render_live_package_cutover_script_report(
            &config,
        ) {
            Ok(report) => report,
            Err(error) => refusal_report(
                &config,
                TinyLivePackageLiveCutoverVerdict::TinyLivePackageLiveCutoverRefusedByInvalidTarget,
                error.to_string(),
            ),
        },
        Mode::RunLivePackageCutover => match run_live_package_cutover_report(&config) {
            Ok(report) => report,
            Err(error) => refusal_report(
                &config,
                TinyLivePackageLiveCutoverVerdict::TinyLivePackageLiveCutoverRefusedByInvalidTarget,
                error.to_string(),
            ),
        },
        Mode::VerifyLivePackageCutover => match verify_live_package_cutover_report(&config) {
            Ok(report) => report,
            Err(error) => refusal_report(
                &config,
                TinyLivePackageLiveCutoverVerdict::TinyLivePackageLiveCutoverVerifyInvalid,
                error.to_string(),
            ),
        },
    };
    if config.json {
        serde_json::to_string_pretty(&report)
            .context("failed to serialize package live cutover report")
    } else {
        Ok(render_report_lines(&report))
    }
}

fn plan_live_package_cutover_report(config: &Config) -> Result<PackageLiveCutoverReport> {
    let package_contract = tiny_live_activation_package::verify_package_contract_for_deploy(
        &config.package_dir,
        &config.install_root,
        &config.target_service_name,
        &config.backend_command,
        config.wrapper_timeout_ms,
    )?;
    let _ = resolve_install_target_paths(&package_contract.install_target_summary);
    Ok(base_report(
        config,
        TinyLivePackageLiveCutoverVerdict::TinyLivePackageLiveCutoverPlanReady,
        format!(
            "package-native real live cutover controller is explicit for install root {}; current Stage 3 truth is still evaluated only at run time and remains a hard refusal boundary",
            config.install_root.display()
        ),
    ))
}

fn render_live_package_cutover_script_report(config: &Config) -> Result<PackageLiveCutoverReport> {
    let _ = tiny_live_activation_package::verify_package_contract_for_deploy(
        &config.package_dir,
        &config.install_root,
        &config.target_service_name,
        &config.backend_command,
        config.wrapper_timeout_ms,
    )?;
    let output_path = config
        .output_path
        .as_ref()
        .ok_or_else(|| anyhow!("missing --output for --render-live-package-cutover-script"))?;
    if output_path.exists() {
        bail!(
            "refusing to overwrite existing live package cutover script {}",
            output_path.display()
        );
    }
    let session_dir = config
        .session_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("<session-dir>"));
    let script = format!(
        "#!/usr/bin/env bash\nset -euo pipefail\n\ncopybot_tiny_live_activation_package_live_cutover --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --service-status-max-staleness-ms {} --plan-live-package-cutover\ncopybot_tiny_live_activation_package_live_cutover --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --service-status-max-staleness-ms {} --session-dir {} --run-live-package-cutover\ncopybot_tiny_live_activation_package_live_cutover --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --service-status-max-staleness-ms {} --session-dir {} --verify-live-package-cutover\n",
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
    fs::write(output_path, script)
        .with_context(|| format!("failed writing {}", output_path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(output_path)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(output_path, perms)?;
    }
    Ok(base_report(
        config,
        TinyLivePackageLiveCutoverVerdict::TinyLivePackageLiveCutoverScriptRendered,
        format!(
            "rendered package-native real live cutover controller script to {}",
            output_path.display()
        ),
    ))
}

fn controller_plan_view_from_report(
    report: PackageLiveCutoverReport,
) -> LiveCutoverControllerPlanView {
    LiveCutoverControllerPlanView {
        mode: report.mode,
        verdict: serialize_enum(&report.verdict),
        reason: report.reason,
        package_dir: report.package_dir,
        install_root: report.install_root,
        target_service_name: report.target_service_name,
        backend_command: report.backend_command,
        wrapper_timeout_ms: report.wrapper_timeout_ms,
        service_status_max_staleness_ms: report.service_status_max_staleness_ms,
        run_preflight_command_summary: report.run_preflight_command_summary,
        gate_evaluation_command_summary: report.gate_evaluation_command_summary,
        backup_proof_command_summary: report.backup_proof_command_summary,
        run_live_cutover_command_summary: report.run_live_cutover_command_summary,
        explicit_statement: report.explicit_statement,
    }
}

#[allow(dead_code)]
pub(crate) fn live_cutover_controller_plan_view_for_authorization(
    package_dir: &Path,
    install_root: &Path,
    target_service_name: &str,
    backend_command: &str,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
) -> LiveCutoverControllerPlanView {
    let config = Config {
        mode: Mode::PlanLivePackageCutover,
        package_dir: package_dir.to_path_buf(),
        install_root: install_root.to_path_buf(),
        target_service_name: target_service_name.to_string(),
        backend_command: backend_command.to_string(),
        wrapper_timeout_ms,
        service_status_max_staleness_ms,
        session_dir: None,
        output_path: None,
        json: false,
    };
    let report = match plan_live_package_cutover_report(&config) {
        Ok(report) => report,
        Err(error) => refusal_report(
            &config,
            TinyLivePackageLiveCutoverVerdict::TinyLivePackageLiveCutoverRefusedByInvalidTarget,
            error.to_string(),
        ),
    };
    controller_plan_view_from_report(report)
}

fn run_live_package_cutover_report(config: &Config) -> Result<PackageLiveCutoverReport> {
    run_live_package_cutover_report_with_hooks(config, LiveCutoverRunHooks::default())
}

fn run_live_package_cutover_report_with_hooks(
    config: &Config,
    hooks: LiveCutoverRunHooks<'_>,
) -> Result<PackageLiveCutoverReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for --run-live-package-cutover"))?;
    ensure_clean_live_package_cutover_session_dir(session_dir)?;
    let paths = package_live_cutover_paths(session_dir);
    let package_contract = tiny_live_activation_package::verify_package_contract_for_deploy(
        &config.package_dir,
        &config.install_root,
        &config.target_service_name,
        &config.backend_command,
        config.wrapper_timeout_ms,
    )?;
    let install_summary = resolve_install_target_paths(&package_contract.install_target_summary);
    let session = planned_session(config, session_dir, &paths);
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
            .context("failed parsing persisted live package preflight report")?;
    if !preflight_allows_live_cutover_controller(&stored_preflight) {
        let status = PackageLiveCutoverStatus {
            status_version: LIVE_CUTOVER_STATUS_VERSION.to_string(),
            updated_at: Utc::now(),
            session_dir: session_dir.display().to_string(),
            nested_preflight_session_dir: paths.preflight_session_dir.display().to_string(),
            nested_cutover_session_dir: paths.nested_cutover_session_dir.display().to_string(),
            result: LivePackageCutoverResult::RefusedByInvalidTarget,
            reason: preflight.reason.clone(),
            preflight_step: preflight_step.clone(),
            gate_step: None,
            backup_step: None,
            live_cutover_step: None,
            explicit_statement:
                "this package-native real live cutover controller remains bounded by the explicit live target contract and stopped before authorization because package, wrapper, target, or service-status evidence was non-green"
                    .to_string(),
        };
        persist_json(&paths.status_path, &status)?;
        return Ok(report_from_status(
            config,
            session_dir,
            &paths,
            TinyLivePackageLiveCutoverVerdict::TinyLivePackageLiveCutoverRefusedByInvalidTarget,
            &status,
            preflight.current_pre_activation_gate_verdict,
            preflight.current_pre_activation_gate_reason,
            preflight.activation_authorized,
        ));
    }

    let gate_report = tiny_live_activation_live_execute::build_plan_live_report_for_drill(
        Path::new(&install_summary.installed_activation_config_path),
        Path::new(&install_summary.installed_rollback_config_path),
        Path::new(&install_summary.target_config_path),
        &config.target_service_name,
        Path::new(&install_summary.wrapper_path),
        Path::new(&install_summary.runtime_dir),
        Path::new(&install_summary.backup_dir),
        DEFAULT_STARTUP_TIMEOUT_MS,
        DEFAULT_ROLLBACK_TIMEOUT_MS,
    )?;
    persist_json(&paths.gate_report_path, &gate_report)?;
    let gate_step = Some(step_artifact(
        &paths.gate_report_path,
        "evaluate_live_gate",
        &serialize_enum(&gate_report.verdict),
        &gate_report.reason,
        gate_report.generated_at,
    ));
    match gate_report.verdict {
        tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLivePlanReady => {}
        tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyRefusedByStage3 => {
            let status = PackageLiveCutoverStatus {
                status_version: LIVE_CUTOVER_STATUS_VERSION.to_string(),
                updated_at: Utc::now(),
                session_dir: session_dir.display().to_string(),
                nested_preflight_session_dir: paths.preflight_session_dir.display().to_string(),
                nested_cutover_session_dir: paths.nested_cutover_session_dir.display().to_string(),
                result: LivePackageCutoverResult::RefusedByStage3,
                reason: gate_report.reason.clone(),
                preflight_step,
                gate_step: gate_step.clone(),
                backup_step: None,
                live_cutover_step: None,
                explicit_statement:
                    "this package-native real live cutover controller remains refused because current Stage 3 is still non-green on the explicit live target"
                        .to_string(),
            };
            persist_json(&paths.status_path, &status)?;
            return Ok(report_from_status(
                config,
                session_dir,
                &paths,
                TinyLivePackageLiveCutoverVerdict::TinyLivePackageLiveCutoverRefusedByStage3,
                &status,
                gate_report.current_pre_activation_gate_verdict.clone(),
                gate_report.current_pre_activation_gate_reason.clone(),
                gate_report.activation_authorized,
            ));
        }
        tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyRefusedByPreActivationGate => {
            let status = PackageLiveCutoverStatus {
                status_version: LIVE_CUTOVER_STATUS_VERSION.to_string(),
                updated_at: Utc::now(),
                session_dir: session_dir.display().to_string(),
                nested_preflight_session_dir: paths.preflight_session_dir.display().to_string(),
                nested_cutover_session_dir: paths.nested_cutover_session_dir.display().to_string(),
                result: LivePackageCutoverResult::RefusedByPreActivationGate,
                reason: gate_report.reason.clone(),
                preflight_step,
                gate_step: gate_step.clone(),
                backup_step: None,
                live_cutover_step: None,
                explicit_statement:
                    "this package-native real live cutover controller remains refused because the current pre-activation gate is still non-green on the explicit live target"
                        .to_string(),
            };
            persist_json(&paths.status_path, &status)?;
            return Ok(report_from_status(
                config,
                session_dir,
                &paths,
                TinyLivePackageLiveCutoverVerdict::TinyLivePackageLiveCutoverRefusedByPreActivationGate,
                &status,
                gate_report.current_pre_activation_gate_verdict.clone(),
                gate_report.current_pre_activation_gate_reason.clone(),
                gate_report.activation_authorized,
            ));
        }
        _ => {
            let status = PackageLiveCutoverStatus {
                status_version: LIVE_CUTOVER_STATUS_VERSION.to_string(),
                updated_at: Utc::now(),
                session_dir: session_dir.display().to_string(),
                nested_preflight_session_dir: paths.preflight_session_dir.display().to_string(),
                nested_cutover_session_dir: paths.nested_cutover_session_dir.display().to_string(),
                result: LivePackageCutoverResult::RefusedByInvalidTarget,
                reason: gate_report.reason.clone(),
                preflight_step,
                gate_step: gate_step.clone(),
                backup_step: None,
                live_cutover_step: None,
                explicit_statement:
                    "this package-native real live cutover controller remains refused because the explicit live target or wrapper contract is non-green"
                        .to_string(),
            };
            persist_json(&paths.status_path, &status)?;
            return Ok(report_from_status(
                config,
                session_dir,
                &paths,
                TinyLivePackageLiveCutoverVerdict::TinyLivePackageLiveCutoverRefusedByInvalidTarget,
                &status,
                gate_report.current_pre_activation_gate_verdict.clone(),
                gate_report.current_pre_activation_gate_reason.clone(),
                gate_report.activation_authorized,
            ));
        }
    }

    if let Some(hook) = hooks.before_backup_hook {
        hook(&install_summary)?;
    }
    let backup_report =
        tiny_live_activation_live_execute::backup_current_config_report_for_cutover(
            Path::new(&install_summary.installed_activation_config_path),
            Path::new(&install_summary.installed_rollback_config_path),
            Path::new(&install_summary.target_config_path),
            &config.target_service_name,
            Path::new(&install_summary.wrapper_path),
            Path::new(&install_summary.runtime_dir),
            Path::new(&install_summary.backup_dir),
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
        let status = PackageLiveCutoverStatus {
            status_version: LIVE_CUTOVER_STATUS_VERSION.to_string(),
            updated_at: Utc::now(),
            session_dir: session_dir.display().to_string(),
            nested_preflight_session_dir: paths.preflight_session_dir.display().to_string(),
            nested_cutover_session_dir: paths.nested_cutover_session_dir.display().to_string(),
            result: LivePackageCutoverResult::CompletedBackupFailed,
            reason: backup_report.reason.clone(),
            preflight_step,
            gate_step,
            backup_step,
            live_cutover_step: None,
            explicit_statement:
                "this package-native real live cutover controller stopped before apply because the bounded live backup proof could not be established"
                    .to_string(),
        };
        persist_json(&paths.status_path, &status)?;
        return Ok(report_from_status(
            config,
            session_dir,
            &paths,
            TinyLivePackageLiveCutoverVerdict::TinyLivePackageLiveCutoverCompletedBackupFailed,
            &status,
            gate_report.current_pre_activation_gate_verdict.clone(),
            gate_report.current_pre_activation_gate_reason.clone(),
            gate_report.activation_authorized,
        ));
    }

    if let Some(hook) = hooks.before_cutover_hook {
        hook(&install_summary)?;
    }
    let live_cutover = if let Some(seed) = hooks.cutover_observation_seed {
        tiny_live_activation_cutover::run_live_cutover_for_rehearsal(
            Path::new(&install_summary.installed_activation_config_path),
            Path::new(&install_summary.installed_rollback_config_path),
            Path::new(&install_summary.runtime_dir),
            Path::new(&install_summary.target_config_path),
            &config.target_service_name,
            Path::new(&install_summary.wrapper_path),
            Path::new(&install_summary.backup_dir),
            &paths.nested_cutover_session_dir,
            DEFAULT_STARTUP_TIMEOUT_MS,
            DEFAULT_ROLLBACK_TIMEOUT_MS,
            None,
            DEFAULT_SAMPLE_CADENCE_MS,
            Some(DEFAULT_MAX_OBSERVATION_STALENESS_MS),
            seed,
        )?
    } else {
        tiny_live_activation_cutover::run_live_cutover_for_package_inputs_actual(
            Path::new(&install_summary.installed_activation_config_path),
            Path::new(&install_summary.installed_rollback_config_path),
            Path::new(&install_summary.runtime_dir),
            Path::new(&install_summary.target_config_path),
            &config.target_service_name,
            Path::new(&install_summary.wrapper_path),
            Path::new(&install_summary.backup_dir),
            &paths.nested_cutover_session_dir,
            DEFAULT_STARTUP_TIMEOUT_MS,
            DEFAULT_ROLLBACK_TIMEOUT_MS,
            None,
            DEFAULT_SAMPLE_CADENCE_MS,
            Some(DEFAULT_MAX_OBSERVATION_STALENESS_MS),
        )?
    };
    persist_json_value(&paths.live_cutover_report_path, &live_cutover.report_json)?;
    let live_cutover_step = Some(step_artifact(
        &paths.live_cutover_report_path,
        "run_live_cutover",
        &live_cutover.verdict,
        &live_cutover.reason,
        live_cutover.generated_at,
    ));
    let (result, verdict) = match live_cutover.live_result.as_deref() {
        Some("completed_keep_running") => (
            LivePackageCutoverResult::CompletedKeepRunning,
            TinyLivePackageLiveCutoverVerdict::TinyLivePackageLiveCutoverCompletedKeepRunning,
        ),
        Some("completed_with_rollback") => (
            LivePackageCutoverResult::CompletedWithRollback,
            TinyLivePackageLiveCutoverVerdict::TinyLivePackageLiveCutoverCompletedWithRollback,
        ),
        Some("failed_during_watch") => (
            LivePackageCutoverResult::CompletedWatchFailed,
            TinyLivePackageLiveCutoverVerdict::TinyLivePackageLiveCutoverCompletedWatchFailed,
        ),
        _ => (
            LivePackageCutoverResult::CompletedApplyFailed,
            TinyLivePackageLiveCutoverVerdict::TinyLivePackageLiveCutoverCompletedApplyFailed,
        ),
    };
    let status = PackageLiveCutoverStatus {
        status_version: LIVE_CUTOVER_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        session_dir: session_dir.display().to_string(),
        nested_preflight_session_dir: paths.preflight_session_dir.display().to_string(),
        nested_cutover_session_dir: paths.nested_cutover_session_dir.display().to_string(),
        result,
        reason: live_cutover.reason.clone(),
        preflight_step,
        gate_step,
        backup_step,
        live_cutover_step,
        explicit_statement:
            "this package-native real live cutover controller remains bounded to one immutable package and one explicit live target contract; any real activation happens only when the current gate allows"
                .to_string(),
    };
    persist_json(&paths.status_path, &status)?;
    Ok(report_from_status(
        config,
        session_dir,
        &paths,
        verdict,
        &status,
        live_cutover.current_pre_activation_gate_verdict,
        live_cutover.current_pre_activation_gate_reason,
        live_cutover.activation_authorized,
    ))
}

fn verify_live_package_cutover_report(config: &Config) -> Result<PackageLiveCutoverReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for --verify-live-package-cutover"))?;
    let package_contract = match tiny_live_activation_package::verify_package_contract_for_deploy(
        &config.package_dir,
        &config.install_root,
        &config.target_service_name,
        &config.backend_command,
        config.wrapper_timeout_ms,
    ) {
        Ok(value) => value,
        Err(error) => {
            let paths = package_live_cutover_paths(session_dir);
            return Ok(PackageLiveCutoverReport {
                generated_at: Utc::now(),
                mode: "verify_live_package_cutover".to_string(),
                verdict: TinyLivePackageLiveCutoverVerdict::TinyLivePackageLiveCutoverVerifyInvalid,
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
                live_cutover_session_path: Some(paths.session_path.display().to_string()),
                live_cutover_status_path: Some(paths.status_path.display().to_string()),
                nested_preflight_session_dir: Some(paths.preflight_session_dir.display().to_string()),
                nested_cutover_session_dir: Some(paths.nested_cutover_session_dir.display().to_string()),
                result: None,
                preflight_step: None,
                gate_step: None,
                backup_step: None,
                live_cutover_step: None,
                run_preflight_command_summary: run_preflight_command_summary(config, &paths.preflight_session_dir),
                gate_evaluation_command_summary: gate_evaluation_command_summary_placeholder(config),
                backup_proof_command_summary: backup_proof_command_summary_placeholder(config),
                run_live_cutover_command_summary: run_live_cutover_command_summary_placeholder(
                    config,
                    &paths.nested_cutover_session_dir,
                ),
                current_pre_activation_gate_verdict: None,
                current_pre_activation_gate_reason: None,
                verification_mismatches: vec![error.to_string()],
                activation_authorized: false,
                explicit_statement:
                    "this package-native real live cutover verification checks bounded controller evidence only; it does not authorize production activation"
                        .to_string(),
            });
        }
    };
    let install_summary = resolve_install_target_paths(&package_contract.install_target_summary);
    let paths = package_live_cutover_paths(session_dir);
    let session: PackageLiveCutoverSession = load_json(&paths.session_path)?;
    let status: PackageLiveCutoverStatus = load_json(&paths.status_path)?;
    let mut mismatches = Vec::new();

    compare_string(
        &session.package_dir,
        &config.package_dir.display().to_string(),
        "package live cutover package_dir",
        &mut mismatches,
    );
    compare_string(
        &session.install_root,
        &config.install_root.display().to_string(),
        "package live cutover install_root",
        &mut mismatches,
    );
    compare_string(
        &session.target_service_name,
        &config.target_service_name,
        "package live cutover target_service_name",
        &mut mismatches,
    );
    compare_string(
        &session.backend_command,
        &config.backend_command,
        "package live cutover backend_command",
        &mut mismatches,
    );
    if session.wrapper_timeout_ms != config.wrapper_timeout_ms {
        mismatches.push(format!(
            "package live cutover wrapper_timeout_ms {} does not match requested {}",
            session.wrapper_timeout_ms, config.wrapper_timeout_ms
        ));
    }
    if session.service_status_max_staleness_ms != config.service_status_max_staleness_ms {
        mismatches.push(format!(
            "package live cutover service_status_max_staleness_ms {} does not match requested {}",
            session.service_status_max_staleness_ms, config.service_status_max_staleness_ms
        ));
    }
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "package live cutover session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.nested_preflight_session_dir,
        &paths.preflight_session_dir.display().to_string(),
        "package live cutover nested_preflight_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.nested_cutover_session_dir,
        &paths.nested_cutover_session_dir.display().to_string(),
        "package live cutover nested_cutover_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.session_dir,
        &session_dir.display().to_string(),
        "package live cutover status session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.nested_preflight_session_dir,
        &paths.preflight_session_dir.display().to_string(),
        "package live cutover status nested_preflight_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.nested_cutover_session_dir,
        &paths.nested_cutover_session_dir.display().to_string(),
        "package live cutover status nested_cutover_session_dir",
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

    let mut current_pre_activation_gate_verdict = verified_preflight
        .current_pre_activation_gate_verdict
        .clone();
    let mut current_pre_activation_gate_reason = verified_preflight
        .current_pre_activation_gate_reason
        .clone();
    let mut activation_authorized = verified_preflight.activation_authorized;

    match status.result {
        LivePackageCutoverResult::RefusedByInvalidTarget => {
            if let Some(step) = &status.gate_step {
                let stored_gate: tiny_live_activation_live_execute::LiveExecuteReport =
                    load_bound_step_json(
                        step,
                        &paths.gate_report_path,
                        "gate_step",
                        &mut mismatches,
                    )?;
                validate_live_execute_report_contract(
                    &stored_gate,
                    &install_summary,
                    config,
                    "gate step",
                    &mut mismatches,
                );
                match stored_gate.verdict {
                    tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyRefusedByStage3
                    | tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyRefusedByPreActivationGate => {
                        mismatches.push(
                            "refused_by_invalid_target session cannot use a stage3/pre-activation gate refusal verdict"
                                .to_string(),
                        );
                    }
                    _ => {}
                }
                current_pre_activation_gate_verdict =
                    stored_gate.current_pre_activation_gate_verdict.clone();
                current_pre_activation_gate_reason =
                    stored_gate.current_pre_activation_gate_reason.clone();
                activation_authorized = stored_gate.activation_authorized;
            } else if !matches!(
                verified_preflight.result.as_deref(),
                Some("failed_during_package_verify")
                    | Some("install_target_missing")
                    | Some("install_target_drifted")
                    | Some("failed_during_live_target_verify")
                    | Some("failed_during_service_status_verify")
            ) {
                mismatches.push(
                    "refused_by_invalid_target session without gate evidence must come from a non-green preflight result"
                        .to_string(),
                );
            }
            if status.backup_step.is_some() || status.live_cutover_step.is_some() {
                mismatches.push(
                    "refused_by_invalid_target session must not contain backup or live cutover evidence"
                        .to_string(),
                );
            }
        }
        LivePackageCutoverResult::RefusedByStage3
        | LivePackageCutoverResult::RefusedByPreActivationGate => {
            let stored_gate: tiny_live_activation_live_execute::LiveExecuteReport =
                load_required_live_execute_report(
                    &status.gate_step,
                    &paths.gate_report_path,
                    "gate_step",
                    &mut mismatches,
                )?;
            validate_live_execute_report_contract(
                &stored_gate,
                &install_summary,
                config,
                "gate step",
                &mut mismatches,
            );
            current_pre_activation_gate_verdict =
                stored_gate.current_pre_activation_gate_verdict.clone();
            current_pre_activation_gate_reason =
                stored_gate.current_pre_activation_gate_reason.clone();
            activation_authorized = stored_gate.activation_authorized;
            let expected = match status.result {
                LivePackageCutoverResult::RefusedByStage3 => {
                    tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyRefusedByStage3
                }
                LivePackageCutoverResult::RefusedByPreActivationGate => {
                    tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveApplyRefusedByPreActivationGate
                }
                _ => unreachable!(),
            };
            if stored_gate.verdict != expected {
                mismatches.push(format!(
                    "gate step verdict {} does not match top-level refused result {}",
                    serialize_enum(&stored_gate.verdict),
                    serialize_enum(&status.result)
                ));
            }
            if status.backup_step.is_some() || status.live_cutover_step.is_some() {
                mismatches.push(
                    "refused-by-gate session must not contain backup or live cutover evidence"
                        .to_string(),
                );
            }
        }
        LivePackageCutoverResult::CompletedBackupFailed => {
            let stored_gate: tiny_live_activation_live_execute::LiveExecuteReport =
                load_required_live_execute_report(
                    &status.gate_step,
                    &paths.gate_report_path,
                    "gate_step",
                    &mut mismatches,
                )?;
            validate_live_execute_report_contract(
                &stored_gate,
                &install_summary,
                config,
                "gate step",
                &mut mismatches,
            );
            if stored_gate.verdict
                != tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLivePlanReady
            {
                mismatches.push(
                    "completed_backup_failed session must have a green gate step before backup"
                        .to_string(),
                );
            }
            let stored_backup: tiny_live_activation_live_execute::LiveExecuteReport =
                load_required_live_execute_report(
                    &status.backup_step,
                    &paths.backup_report_path,
                    "backup_step",
                    &mut mismatches,
                )?;
            validate_live_execute_report_contract(
                &stored_backup,
                &install_summary,
                config,
                "backup step",
                &mut mismatches,
            );
            if stored_backup.verdict
                == tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveBackupCreated
            {
                mismatches.push(
                    "completed_backup_failed session cannot keep a green backup-created verdict"
                        .to_string(),
                );
            }
            if status.live_cutover_step.is_some() {
                mismatches.push(
                    "completed_backup_failed session must not contain live cutover evidence"
                        .to_string(),
                );
            }
            current_pre_activation_gate_verdict =
                stored_gate.current_pre_activation_gate_verdict.clone();
            current_pre_activation_gate_reason =
                stored_gate.current_pre_activation_gate_reason.clone();
            activation_authorized = stored_gate.activation_authorized;
        }
        LivePackageCutoverResult::CompletedKeepRunning
        | LivePackageCutoverResult::CompletedWithRollback
        | LivePackageCutoverResult::CompletedApplyFailed
        | LivePackageCutoverResult::CompletedWatchFailed => {
            let stored_gate: tiny_live_activation_live_execute::LiveExecuteReport =
                load_required_live_execute_report(
                    &status.gate_step,
                    &paths.gate_report_path,
                    "gate_step",
                    &mut mismatches,
                )?;
            validate_live_execute_report_contract(
                &stored_gate,
                &install_summary,
                config,
                "gate step",
                &mut mismatches,
            );
            if stored_gate.verdict
                != tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLivePlanReady
            {
                mismatches.push(
                    "completed live cutover session must have a green gate step before backup/apply"
                        .to_string(),
                );
            }
            let stored_backup: tiny_live_activation_live_execute::LiveExecuteReport =
                load_required_live_execute_report(
                    &status.backup_step,
                    &paths.backup_report_path,
                    "backup_step",
                    &mut mismatches,
                )?;
            validate_live_execute_report_contract(
                &stored_backup,
                &install_summary,
                config,
                "backup step",
                &mut mismatches,
            );
            if stored_backup.verdict
                != tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveBackupCreated
            {
                mismatches.push(
                    "completed live cutover session must keep a backup-created verdict"
                        .to_string(),
                );
            } else {
                validate_green_backup_report_binding(
                    &stored_backup,
                    &install_summary,
                    config,
                    "backup step",
                    &mut mismatches,
                );
            }
            if let Err(error) = tiny_live_activation_live_execute::load_backup_proof_for_watch(
                Path::new(&install_summary.installed_activation_config_path),
                Path::new(&install_summary.installed_rollback_config_path),
                Path::new(&install_summary.target_config_path),
                &config.target_service_name,
                Path::new(&install_summary.wrapper_path),
                Path::new(&install_summary.runtime_dir),
                Path::new(&install_summary.backup_dir),
            ) {
                mismatches.push(format!(
                    "stored live backup proof is no longer valid: {error}"
                ));
            }

            let stored_cutover: CutoverReportView = load_required_step_json(
                &status.live_cutover_step,
                &paths.live_cutover_report_path,
                "live_cutover_step",
                &mut mismatches,
            )?;
            validate_cutover_report_contract(
                &stored_cutover,
                &install_summary,
                &paths.nested_cutover_session_dir,
                config,
                &mut mismatches,
            );
            let nested_verify =
                tiny_live_activation_cutover::verify_live_cutover_session_for_package_inputs_actual(
                    Path::new(&install_summary.installed_activation_config_path),
                    Path::new(&install_summary.installed_rollback_config_path),
                    Path::new(&install_summary.runtime_dir),
                    Path::new(&install_summary.target_config_path),
                    &config.target_service_name,
                    Path::new(&install_summary.wrapper_path),
                    Path::new(&install_summary.backup_dir),
                    &paths.nested_cutover_session_dir,
                    DEFAULT_STARTUP_TIMEOUT_MS,
                    DEFAULT_ROLLBACK_TIMEOUT_MS,
                    None,
                    DEFAULT_SAMPLE_CADENCE_MS,
                    Some(DEFAULT_MAX_OBSERVATION_STALENESS_MS),
                )?;
            if nested_verify.verdict != "tiny_live_cutover_verify_ok" {
                mismatches.extend(nested_verify.verification_mismatches.clone());
            }
            current_pre_activation_gate_verdict =
                nested_verify.current_pre_activation_gate_verdict.clone();
            current_pre_activation_gate_reason =
                nested_verify.current_pre_activation_gate_reason.clone();
            activation_authorized = nested_verify.activation_authorized;
            let expected_nested_result = match status.result {
                LivePackageCutoverResult::CompletedKeepRunning => Some("completed_keep_running"),
                LivePackageCutoverResult::CompletedWithRollback => Some("completed_with_rollback"),
                LivePackageCutoverResult::CompletedApplyFailed => Some("failed_before_apply"),
                LivePackageCutoverResult::CompletedWatchFailed => Some("failed_during_watch"),
                _ => None,
            };
            if nested_verify.live_result.as_deref() != expected_nested_result {
                mismatches.push(format!(
                    "nested cutover result {:?} does not match top-level result {}",
                    nested_verify.live_result,
                    serialize_enum(&status.result)
                ));
            }
        }
    }

    let verdict = if mismatches.is_empty() {
        TinyLivePackageLiveCutoverVerdict::TinyLivePackageLiveCutoverVerifyOk
    } else {
        TinyLivePackageLiveCutoverVerdict::TinyLivePackageLiveCutoverVerifyInvalid
    };
    let reason = if mismatches.is_empty() {
        format!(
            "package-native real live cutover session under {} is coherent and bound to the requested package/live target contract",
            session_dir.display()
        )
    } else {
        mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "package-native real live cutover verification failed".to_string())
    };
    Ok(PackageLiveCutoverReport {
        generated_at: Utc::now(),
        mode: "verify_live_package_cutover".to_string(),
        verdict,
        reason,
        package_dir: config.package_dir.display().to_string(),
        install_root: config.install_root.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        wrapper_timeout_ms: config.wrapper_timeout_ms,
        service_status_max_staleness_ms: config.service_status_max_staleness_ms,
        session_dir: Some(session_dir.display().to_string()),
        live_cutover_session_path: Some(paths.session_path.display().to_string()),
        live_cutover_status_path: Some(paths.status_path.display().to_string()),
        nested_preflight_session_dir: Some(paths.preflight_session_dir.display().to_string()),
        nested_cutover_session_dir: Some(paths.nested_cutover_session_dir.display().to_string()),
        result: Some(serialize_enum(&status.result)),
        preflight_step: status.preflight_step,
        gate_step: status.gate_step,
        backup_step: status.backup_step,
        live_cutover_step: status.live_cutover_step,
        run_preflight_command_summary: run_preflight_command_summary(config, &paths.preflight_session_dir),
        gate_evaluation_command_summary: gate_evaluation_command_summary(config, &install_summary),
        backup_proof_command_summary: backup_proof_command_summary(config, &install_summary),
        run_live_cutover_command_summary: run_live_cutover_command_summary(
            config,
            &install_summary,
            &paths.nested_cutover_session_dir,
        ),
        current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason,
        verification_mismatches: mismatches,
        activation_authorized,
        explicit_statement:
            "this package-native real live cutover verification checks bounded controller evidence only; it does not authorize production activation"
                .to_string(),
    })
}

fn base_report(
    config: &Config,
    verdict: TinyLivePackageLiveCutoverVerdict,
    reason: String,
) -> PackageLiveCutoverReport {
    let session_dir = config.session_dir.as_ref();
    let paths = session_dir.map(|path| package_live_cutover_paths(path));
    PackageLiveCutoverReport {
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
        live_cutover_session_path: paths
            .as_ref()
            .map(|value| value.session_path.display().to_string()),
        live_cutover_status_path: paths
            .as_ref()
            .map(|value| value.status_path.display().to_string()),
        nested_preflight_session_dir: paths
            .as_ref()
            .map(|value| value.preflight_session_dir.display().to_string()),
        nested_cutover_session_dir: paths
            .as_ref()
            .map(|value| value.nested_cutover_session_dir.display().to_string()),
        result: None,
        preflight_step: None,
        gate_step: None,
        backup_step: None,
        live_cutover_step: None,
        run_preflight_command_summary: run_preflight_command_summary(
            config,
            &paths
                .as_ref()
                .map(|value| value.preflight_session_dir.clone())
                .unwrap_or_else(|| {
                    PathBuf::from(
                        "<session-dir>/tiny_live_activation_package_live_cutover.preflight_session",
                    )
                }),
        ),
        gate_evaluation_command_summary: gate_evaluation_command_summary_placeholder(config),
        backup_proof_command_summary: backup_proof_command_summary_placeholder(config),
        run_live_cutover_command_summary: run_live_cutover_command_summary_placeholder(
            config,
            &paths
                .as_ref()
                .map(|value| value.nested_cutover_session_dir.clone())
                .unwrap_or_else(|| {
                    PathBuf::from(
                        "<session-dir>/tiny_live_activation_package_live_cutover.cutover_session",
                    )
                }),
        ),
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        verification_mismatches: Vec::new(),
        activation_authorized: false,
        explicit_statement:
            "this package-native real live cutover controller is the final explicit execution boundary, but current Stage 3 and pre-activation truth still remain the hard authorization gate"
                .to_string(),
    }
}

fn refusal_report(
    config: &Config,
    verdict: TinyLivePackageLiveCutoverVerdict,
    reason: String,
) -> PackageLiveCutoverReport {
    base_report(config, verdict, reason)
}

fn report_from_status(
    config: &Config,
    session_dir: &Path,
    paths: &PackageLiveCutoverPaths,
    verdict: TinyLivePackageLiveCutoverVerdict,
    status: &PackageLiveCutoverStatus,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    activation_authorized: bool,
) -> PackageLiveCutoverReport {
    let package_contract = tiny_live_activation_package::verify_package_contract_for_deploy(
        &config.package_dir,
        &config.install_root,
        &config.target_service_name,
        &config.backend_command,
        config.wrapper_timeout_ms,
    )
    .ok();
    let install_summary = package_contract
        .as_ref()
        .map(|value| resolve_install_target_paths(&value.install_target_summary));
    PackageLiveCutoverReport {
        generated_at: Utc::now(),
        mode: "run_live_package_cutover".to_string(),
        verdict,
        reason: status.reason.clone(),
        package_dir: config.package_dir.display().to_string(),
        install_root: config.install_root.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        wrapper_timeout_ms: config.wrapper_timeout_ms,
        service_status_max_staleness_ms: config.service_status_max_staleness_ms,
        session_dir: Some(session_dir.display().to_string()),
        live_cutover_session_path: Some(paths.session_path.display().to_string()),
        live_cutover_status_path: Some(paths.status_path.display().to_string()),
        nested_preflight_session_dir: Some(paths.preflight_session_dir.display().to_string()),
        nested_cutover_session_dir: Some(paths.nested_cutover_session_dir.display().to_string()),
        result: Some(serialize_enum(&status.result)),
        preflight_step: status.preflight_step.clone(),
        gate_step: status.gate_step.clone(),
        backup_step: status.backup_step.clone(),
        live_cutover_step: status.live_cutover_step.clone(),
        run_preflight_command_summary: run_preflight_command_summary(config, &paths.preflight_session_dir),
        gate_evaluation_command_summary: install_summary
            .as_ref()
            .map(|value| gate_evaluation_command_summary(config, value))
            .unwrap_or_else(|| gate_evaluation_command_summary_placeholder(config)),
        backup_proof_command_summary: install_summary
            .as_ref()
            .map(|value| backup_proof_command_summary(config, value))
            .unwrap_or_else(|| backup_proof_command_summary_placeholder(config)),
        run_live_cutover_command_summary: install_summary
            .as_ref()
            .map(|value| run_live_cutover_command_summary(config, value, &paths.nested_cutover_session_dir))
            .unwrap_or_else(|| {
                run_live_cutover_command_summary_placeholder(config, &paths.nested_cutover_session_dir)
            }),
        current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason,
        verification_mismatches: Vec::new(),
        activation_authorized,
        explicit_statement:
            "this package-native real live cutover controller remains bounded to one immutable package and one explicit live target contract; authorization still depends on current gate truth"
                .to_string(),
    }
}

fn mode_name(mode: Mode) -> &'static str {
    match mode {
        Mode::PlanLivePackageCutover => "plan_live_package_cutover",
        Mode::RenderLivePackageCutoverScript => "render_live_package_cutover_script",
        Mode::RunLivePackageCutover => "run_live_package_cutover",
        Mode::VerifyLivePackageCutover => "verify_live_package_cutover",
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

fn gate_evaluation_command_summary(
    config: &Config,
    install_summary: &ResolvedInstallTargetPaths,
) -> String {
    format!(
        "copybot_tiny_live_activation_live_execute --activation-config {} --rollback-config {} --target-config {} --target-service {} --service-control-command {} --runtime-dir {} --backup-dir {} --plan-live",
        shell_single_quote(&install_summary.installed_activation_config_path),
        shell_single_quote(&install_summary.installed_rollback_config_path),
        shell_single_quote(&install_summary.target_config_path),
        shell_single_quote(&config.target_service_name),
        shell_single_quote(&install_summary.wrapper_path),
        shell_single_quote(&install_summary.runtime_dir),
        shell_single_quote(&install_summary.backup_dir),
    )
}

fn gate_evaluation_command_summary_placeholder(config: &Config) -> String {
    format!(
        "copybot_tiny_live_activation_live_execute --activation-config <installed-activation-under-{}> --rollback-config <installed-rollback-under-{}> --target-config <live-target-config-under-{}> --target-service {} --service-control-command <installed-wrapper-under-{}> --runtime-dir <installed-runtime-under-{}> --backup-dir <installed-backup-under-{}> --plan-live",
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&config.target_service_name),
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&config.install_root.display().to_string()),
    )
}

fn backup_proof_command_summary(
    config: &Config,
    install_summary: &ResolvedInstallTargetPaths,
) -> String {
    format!(
        "copybot_tiny_live_activation_live_execute --activation-config {} --rollback-config {} --target-config {} --target-service {} --service-control-command {} --runtime-dir {} --backup-dir {} --backup-current-config",
        shell_single_quote(&install_summary.installed_activation_config_path),
        shell_single_quote(&install_summary.installed_rollback_config_path),
        shell_single_quote(&install_summary.target_config_path),
        shell_single_quote(&config.target_service_name),
        shell_single_quote(&install_summary.wrapper_path),
        shell_single_quote(&install_summary.runtime_dir),
        shell_single_quote(&install_summary.backup_dir),
    )
}

fn backup_proof_command_summary_placeholder(config: &Config) -> String {
    format!(
        "copybot_tiny_live_activation_live_execute --activation-config <installed-activation-under-{}> --rollback-config <installed-rollback-under-{}> --target-config <live-target-config-under-{}> --target-service {} --service-control-command <installed-wrapper-under-{}> --runtime-dir <installed-runtime-under-{}> --backup-dir <installed-backup-under-{}> --backup-current-config",
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&config.target_service_name),
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&config.install_root.display().to_string()),
    )
}

fn run_live_cutover_command_summary(
    config: &Config,
    install_summary: &ResolvedInstallTargetPaths,
    nested_cutover_session_dir: &Path,
) -> String {
    format!(
        "copybot_tiny_live_activation_cutover --activation-config {} --rollback-config {} --runtime-dir {} --target-config {} --target-service {} --service-control-command {} --backup-dir {} --session-dir {} --run-live-cutover",
        shell_single_quote(&install_summary.installed_activation_config_path),
        shell_single_quote(&install_summary.installed_rollback_config_path),
        shell_single_quote(&install_summary.runtime_dir),
        shell_single_quote(&install_summary.target_config_path),
        shell_single_quote(&config.target_service_name),
        shell_single_quote(&install_summary.wrapper_path),
        shell_single_quote(&install_summary.backup_dir),
        shell_single_quote(&nested_cutover_session_dir.display().to_string()),
    )
}

fn run_live_cutover_command_summary_placeholder(
    config: &Config,
    nested_cutover_session_dir: &Path,
) -> String {
    format!(
        "copybot_tiny_live_activation_cutover --activation-config <installed-activation-under-{}> --rollback-config <installed-rollback-under-{}> --runtime-dir <installed-runtime-under-{}> --target-config <live-target-config-under-{}> --target-service {} --service-control-command <installed-wrapper-under-{}> --backup-dir <installed-backup-under-{}> --session-dir {} --run-live-cutover",
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&config.target_service_name),
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&nested_cutover_session_dir.display().to_string()),
    )
}

fn planned_session(
    config: &Config,
    session_dir: &Path,
    paths: &PackageLiveCutoverPaths,
) -> PackageLiveCutoverSession {
    PackageLiveCutoverSession {
        session_version: LIVE_CUTOVER_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        package_dir: config.package_dir.display().to_string(),
        install_root: config.install_root.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        wrapper_timeout_ms: config.wrapper_timeout_ms,
        service_status_max_staleness_ms: config.service_status_max_staleness_ms,
        session_dir: session_dir.display().to_string(),
        nested_preflight_session_dir: paths.preflight_session_dir.display().to_string(),
        nested_cutover_session_dir: paths.nested_cutover_session_dir.display().to_string(),
        run_preflight_command_summary: run_preflight_command_summary(config, &paths.preflight_session_dir),
        gate_evaluation_command_summary: gate_evaluation_command_summary_placeholder(config),
        backup_proof_command_summary: backup_proof_command_summary_placeholder(config),
        run_live_cutover_command_summary: run_live_cutover_command_summary_placeholder(
            config,
            &paths.nested_cutover_session_dir,
        ),
        explicit_statement:
            "this package-native real live cutover controller binds one immutable package to one explicit live target contract and still requires current Stage 3 / pre-activation truth to authorize any real apply"
                .to_string(),
    }
}

fn step_artifact(
    path: &Path,
    mode: &str,
    verdict: &str,
    reason: &str,
    generated_at: DateTime<Utc>,
) -> PackageLiveCutoverStepArtifact {
    PackageLiveCutoverStepArtifact {
        path: path.display().to_string(),
        mode: mode.to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        generated_at,
    }
}

fn package_live_cutover_paths(session_dir: &Path) -> PackageLiveCutoverPaths {
    PackageLiveCutoverPaths {
        session_path: session_dir.join("tiny_live_activation_package_live_cutover.session.json"),
        status_path: session_dir.join("tiny_live_activation_package_live_cutover.status.json"),
        preflight_report_path: session_dir
            .join("tiny_live_activation_package_live_cutover.preflight.report.json"),
        gate_report_path: session_dir
            .join("tiny_live_activation_package_live_cutover.gate.report.json"),
        backup_report_path: session_dir
            .join("tiny_live_activation_package_live_cutover.backup.report.json"),
        live_cutover_report_path: session_dir
            .join("tiny_live_activation_package_live_cutover.live_cutover.report.json"),
        preflight_session_dir: session_dir
            .join("tiny_live_activation_package_live_cutover.preflight_session"),
        nested_cutover_session_dir: session_dir
            .join("tiny_live_activation_package_live_cutover.cutover_session"),
    }
}

fn ensure_clean_live_package_cutover_session_dir(session_dir: &Path) -> Result<()> {
    let paths = package_live_cutover_paths(session_dir);
    for artifact in [
        &paths.session_path,
        &paths.status_path,
        &paths.preflight_report_path,
        &paths.gate_report_path,
        &paths.backup_report_path,
        &paths.live_cutover_report_path,
        &paths.preflight_session_dir,
        &paths.nested_cutover_session_dir,
    ] {
        if artifact.exists() {
            bail!(
                "refusing to reuse package live cutover session dir {}; artifact {} already exists",
                session_dir.display(),
                artifact.display()
            );
        }
    }
    fs::create_dir_all(session_dir)
        .with_context(|| format!("failed creating session dir {}", session_dir.display()))
}

fn validate_preflight_report_contract(
    report: &StoredPreflightReportView,
    config: &Config,
    paths: &PackageLiveCutoverPaths,
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

fn preflight_allows_live_cutover_controller(report: &StoredPreflightReportView) -> bool {
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

fn validate_green_backup_report_binding(
    report: &tiny_live_activation_live_execute::LiveExecuteReport,
    install_summary: &ResolvedInstallTargetPaths,
    config: &Config,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    match expected_live_backup_proof_paths(install_summary, config) {
        Ok((expected_backup_config_path, expected_backup_metadata_path)) => {
            compare_string(
                report.backup_config_path.as_deref().unwrap_or_default(),
                &expected_backup_config_path,
                &format!("{label} backup_config_path"),
                mismatches,
            );
            compare_string(
                report.backup_metadata_path.as_deref().unwrap_or_default(),
                &expected_backup_metadata_path,
                &format!("{label} backup_metadata_path"),
                mismatches,
            );
        }
        Err(error) => mismatches.push(format!(
            "failed deriving expected live backup proof paths for {label}: {error}"
        )),
    }
}

fn validate_cutover_report_contract(
    report: &CutoverReportView,
    install_summary: &ResolvedInstallTargetPaths,
    nested_cutover_session_dir: &Path,
    config: &Config,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &report.activation_config_path,
        &install_summary.installed_activation_config_path,
        "live cutover step activation_config_path",
        mismatches,
    );
    compare_string(
        &report.rollback_config_path,
        &install_summary.installed_rollback_config_path,
        "live cutover step rollback_config_path",
        mismatches,
    );
    compare_string(
        &report.runtime_dir,
        &install_summary.runtime_dir,
        "live cutover step runtime_dir",
        mismatches,
    );
    compare_string(
        &report.target_config_path,
        &install_summary.target_config_path,
        "live cutover step target_config_path",
        mismatches,
    );
    compare_string(
        &report.target_service_name,
        &config.target_service_name,
        "live cutover step target_service_name",
        mismatches,
    );
    compare_string(
        &report.service_control_command_path,
        &install_summary.wrapper_path,
        "live cutover step service_control_command_path",
        mismatches,
    );
    compare_string(
        &report.backup_dir,
        &install_summary.backup_dir,
        "live cutover step backup_dir",
        mismatches,
    );
    compare_string(
        report.session_dir.as_deref().unwrap_or_default(),
        &nested_cutover_session_dir.display().to_string(),
        "live cutover step session_dir",
        mismatches,
    );
}

fn load_required_live_execute_report(
    step: &Option<PackageLiveCutoverStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<tiny_live_activation_live_execute::LiveExecuteReport> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("missing required package live cutover step {label}"))?;
    load_bound_step_json(step, expected_path, label, mismatches)
}

fn load_required_step_json<T: for<'de> Deserialize<'de>>(
    step: &Option<PackageLiveCutoverStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<T> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("missing required package live cutover step {label}"))?;
    load_bound_step_json(step, expected_path, label, mismatches)
}

fn load_bound_step_json<T: for<'de> Deserialize<'de>>(
    step: &PackageLiveCutoverStepArtifact,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<T> {
    let expected = expected_path.display().to_string();
    if step.path != expected {
        mismatches.push(format!(
            "package live cutover {label} path {} does not match deterministic session artifact {}",
            step.path, expected
        ));
    }
    load_json(expected_path).with_context(|| {
        format!(
            "failed reading deterministic package live cutover {label} report {}",
            expected_path.display()
        )
    })
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

fn resolve_install_target_paths<S>(summary: &S) -> ResolvedInstallTargetPaths
where
    S: Serialize,
{
    let value = serde_json::to_value(summary)
        .expect("serializing install target summary for path resolution should succeed");
    serde_json::from_value(value)
        .expect("install target summary shape should match resolved install target paths")
}

fn expected_live_backup_proof_paths(
    install_summary: &ResolvedInstallTargetPaths,
    config: &Config,
) -> Result<(String, String)> {
    let (activation, _rollback) =
        tiny_live_activation_live_execute::load_validated_artifact_pair_for_live_target(
            Path::new(&install_summary.installed_activation_config_path),
            Path::new(&install_summary.installed_rollback_config_path),
        )?;
    let (backup_config_path, backup_metadata_path) =
        tiny_live_activation_live_execute::backup_artifact_paths_for_watch(
            Path::new(&install_summary.target_config_path),
            &config.target_service_name,
            Path::new(&install_summary.backup_dir),
            &activation.metadata.source_config_fingerprint_sha256,
        );
    Ok((
        backup_config_path.display().to_string(),
        backup_metadata_path.display().to_string(),
    ))
}

fn render_report_lines(report: &PackageLiveCutoverReport) -> String {
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
    if let Some(value) = &report.live_cutover_session_path {
        lines.push(format!("live_cutover_session_path={value}"));
    }
    if let Some(value) = &report.live_cutover_status_path {
        lines.push(format!("live_cutover_status_path={value}"));
    }
    if let Some(value) = &report.nested_preflight_session_dir {
        lines.push(format!("nested_preflight_session_dir={value}"));
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
    lines.push(format!(
        "activation_authorized={}",
        report.activation_authorized
    ));
    if !report.verification_mismatches.is_empty() {
        lines.push(format!(
            "verification_mismatches={}",
            report.verification_mismatches.join(" | ")
        ));
    }
    lines.push(report.explicit_statement.clone());
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::tiny_live_activation_cutover;
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
    use std::io::{Read, Write as IoWrite};
    use std::net::{SocketAddr, TcpListener};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn green_gate_yields_completed_keep_running() {
        let fixture = live_cutover_fixture(
            "tiny_live_activation_package_live_cutover_keep_running",
            GateState::Green,
        );
        let report = run_live_package_cutover_report_with_hooks(
            &fixture.config,
            LiveCutoverRunHooks {
                cutover_observation_seed: Some(
                    &tiny_live_activation_cutover::CutoverRehearsalObservationSeed::default(),
                ),
                ..LiveCutoverRunHooks::default()
            },
        )
        .expect("run live cutover");
        assert_eq!(
            report.verdict,
            TinyLivePackageLiveCutoverVerdict::TinyLivePackageLiveCutoverCompletedKeepRunning
        );
        assert_eq!(report.result.as_deref(), Some("completed_keep_running"));
        let target = load_from_path(&fixture.target_config_path).unwrap();
        assert!(target.execution.enabled);
    }

    #[test]
    fn watch_triggered_failure_completes_with_rollback() {
        let fixture = live_cutover_fixture(
            "tiny_live_activation_package_live_cutover_watch_rollback",
            GateState::Green,
        );
        let observation_seed = tiny_live_activation_cutover::CutoverRehearsalObservationSeed {
            target_running: false,
            note: "watch_failure".to_string(),
            ..tiny_live_activation_cutover::CutoverRehearsalObservationSeed::default()
        };
        let report = run_live_package_cutover_report_with_hooks(
            &fixture.config,
            LiveCutoverRunHooks {
                cutover_observation_seed: Some(&observation_seed),
                ..LiveCutoverRunHooks::default()
            },
        )
        .expect("run live cutover");
        assert_eq!(
            report.verdict,
            TinyLivePackageLiveCutoverVerdict::TinyLivePackageLiveCutoverCompletedWithRollback
        );
        assert_eq!(report.result.as_deref(), Some("completed_with_rollback"));
        let target = load_from_path(&fixture.target_config_path).unwrap();
        assert!(!target.execution.enabled);
        let target_bytes = fs::read(&fixture.target_config_path).unwrap();
        let original_bytes = fs::read(&fixture.original_target_config_path).unwrap();
        assert_eq!(target_bytes, original_bytes);
    }

    #[test]
    fn backup_failure_is_explicit() {
        let fixture = live_cutover_fixture(
            "tiny_live_activation_package_live_cutover_backup_failure",
            GateState::Green,
        );
        let report = run_live_package_cutover_report_with_hooks(
            &fixture.config,
            LiveCutoverRunHooks {
                before_backup_hook: Some(&|install_summary| {
                    let activation = tiny_live_activation_live_execute::tiny_live_activation_execute::inspect_rendered_config_artifact(
                        Path::new(&install_summary.installed_activation_config_path),
                    )?;
                    let (backup_config_path, _) =
                        tiny_live_activation_live_execute::backup_artifact_paths_for_watch(
                            Path::new(&install_summary.target_config_path),
                            "solana-copy-bot.service",
                            Path::new(&install_summary.backup_dir),
                            &activation.metadata.source_config_fingerprint_sha256,
                        );
                    fs::create_dir_all(backup_config_path.parent().unwrap())?;
                    fs::write(&backup_config_path, "preexisting backup")?;
                    Ok(())
                }),
                ..LiveCutoverRunHooks::default()
            },
        )
        .expect("run live cutover");
        assert_eq!(
            report.verdict,
            TinyLivePackageLiveCutoverVerdict::TinyLivePackageLiveCutoverCompletedBackupFailed
        );
        assert_eq!(report.result.as_deref(), Some("completed_backup_failed"));
    }

    #[test]
    fn apply_failure_is_explicit() {
        let fixture = live_cutover_fixture(
            "tiny_live_activation_package_live_cutover_apply_failure",
            GateState::Green,
        );
        let report = run_live_package_cutover_report_with_hooks(
            &fixture.config,
            LiveCutoverRunHooks {
                before_cutover_hook: Some(&|install_summary| {
                    fs::create_dir_all(&install_summary.runtime_dir)?;
                    fs::write(
                        Path::new(&install_summary.runtime_dir).join("force_apply_status_mismatch"),
                        "1",
                    )?;
                    Ok(())
                }),
                cutover_observation_seed: Some(
                    &tiny_live_activation_cutover::CutoverRehearsalObservationSeed::default(),
                ),
                ..LiveCutoverRunHooks::default()
            },
        )
        .expect("run live cutover");
        assert_eq!(
            report.verdict,
            TinyLivePackageLiveCutoverVerdict::TinyLivePackageLiveCutoverCompletedApplyFailed
        );
        assert_eq!(report.result.as_deref(), Some("completed_apply_failed"));
    }

    #[test]
    fn stage3_non_green_refuses_run() {
        let fixture = live_cutover_fixture(
            "tiny_live_activation_package_live_cutover_stage3",
            GateState::Stage3Blocked,
        );
        let report = run_json_report(fixture.config.clone());
        assert_eq!(
            report.verdict,
            TinyLivePackageLiveCutoverVerdict::TinyLivePackageLiveCutoverRefusedByStage3
        );
        assert_eq!(report.result.as_deref(), Some("refused_by_stage3"));
    }

    #[test]
    fn pre_activation_gate_non_green_refuses_run() {
        let fixture = live_cutover_fixture(
            "tiny_live_activation_package_live_cutover_pre_gate",
            GateState::PreActivationBlocked,
        );
        let report = run_json_report(fixture.config.clone());
        assert_eq!(
            report.verdict,
            TinyLivePackageLiveCutoverVerdict::TinyLivePackageLiveCutoverRefusedByPreActivationGate
        );
        assert_eq!(
            report.result.as_deref(),
            Some("refused_by_pre_activation_gate")
        );
    }

    #[test]
    fn verify_rejects_tampered_live_cutover_step_path() {
        let fixture = live_cutover_fixture(
            "tiny_live_activation_package_live_cutover_verify_tampered",
            GateState::Green,
        );
        run_live_package_cutover_report_with_hooks(
            &fixture.config,
            LiveCutoverRunHooks {
                cutover_observation_seed: Some(
                    &tiny_live_activation_cutover::CutoverRehearsalObservationSeed::default(),
                ),
                ..LiveCutoverRunHooks::default()
            },
        )
        .unwrap();
        let paths = package_live_cutover_paths(fixture.config.session_dir.as_ref().unwrap());
        let mut status: PackageLiveCutoverStatus = load_json(&paths.status_path).unwrap();
        let other = fixture.fixture_dir.join("other.report.json");
        fs::write(&other, "{}").unwrap();
        status.live_cutover_step.as_mut().unwrap().path = other.display().to_string();
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_live_package_cutover_report(&fixture.config).expect("verify");
        assert_eq!(
            verify.verdict,
            TinyLivePackageLiveCutoverVerdict::TinyLivePackageLiveCutoverVerifyInvalid
        );
    }

    #[test]
    fn verify_accepts_coherent_backup_step_binding() {
        let fixture = live_cutover_fixture(
            "tiny_live_activation_package_live_cutover_verify_backup_green",
            GateState::Green,
        );
        run_live_package_cutover_report_with_hooks(
            &fixture.config,
            LiveCutoverRunHooks {
                cutover_observation_seed: Some(
                    &tiny_live_activation_cutover::CutoverRehearsalObservationSeed::default(),
                ),
                ..LiveCutoverRunHooks::default()
            },
        )
        .unwrap();

        let verify = verify_live_package_cutover_report(&fixture.config).expect("verify");
        assert_eq!(
            verify.verdict,
            TinyLivePackageLiveCutoverVerdict::TinyLivePackageLiveCutoverVerifyOk,
            "{verify:#?}"
        );
    }

    #[test]
    fn verify_rejects_tampered_backup_step_backup_config_path() {
        let fixture = live_cutover_fixture(
            "tiny_live_activation_package_live_cutover_verify_backup_config_path",
            GateState::Green,
        );
        run_live_package_cutover_report_with_hooks(
            &fixture.config,
            LiveCutoverRunHooks {
                cutover_observation_seed: Some(
                    &tiny_live_activation_cutover::CutoverRehearsalObservationSeed::default(),
                ),
                ..LiveCutoverRunHooks::default()
            },
        )
        .unwrap();
        let paths = package_live_cutover_paths(fixture.config.session_dir.as_ref().unwrap());
        let mut backup_report: tiny_live_activation_live_execute::LiveExecuteReport =
            load_json(&paths.backup_report_path).unwrap();
        backup_report.backup_config_path = Some(
            fixture
                .fixture_dir
                .join("foreign-backup.toml")
                .display()
                .to_string(),
        );
        persist_json(&paths.backup_report_path, &backup_report).unwrap();

        let verify = verify_live_package_cutover_report(&fixture.config).expect("verify");
        assert_eq!(
            verify.verdict,
            TinyLivePackageLiveCutoverVerdict::TinyLivePackageLiveCutoverVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_backup_step_backup_metadata_path() {
        let fixture = live_cutover_fixture(
            "tiny_live_activation_package_live_cutover_verify_backup_metadata_path",
            GateState::Green,
        );
        run_live_package_cutover_report_with_hooks(
            &fixture.config,
            LiveCutoverRunHooks {
                cutover_observation_seed: Some(
                    &tiny_live_activation_cutover::CutoverRehearsalObservationSeed::default(),
                ),
                ..LiveCutoverRunHooks::default()
            },
        )
        .unwrap();
        let paths = package_live_cutover_paths(fixture.config.session_dir.as_ref().unwrap());
        let mut backup_report: tiny_live_activation_live_execute::LiveExecuteReport =
            load_json(&paths.backup_report_path).unwrap();
        backup_report.backup_metadata_path = Some(
            fixture
                .fixture_dir
                .join("foreign-backup.metadata.json")
                .display()
                .to_string(),
        );
        persist_json(&paths.backup_report_path, &backup_report).unwrap();

        let verify = verify_live_package_cutover_report(&fixture.config).expect("verify");
        assert_eq!(
            verify.verdict,
            TinyLivePackageLiveCutoverVerdict::TinyLivePackageLiveCutoverVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_mismatched_target_contract() {
        let fixture = live_cutover_fixture(
            "tiny_live_activation_package_live_cutover_verify_target_mismatch",
            GateState::Green,
        );
        run_live_package_cutover_report_with_hooks(
            &fixture.config,
            LiveCutoverRunHooks {
                cutover_observation_seed: Some(
                    &tiny_live_activation_cutover::CutoverRehearsalObservationSeed::default(),
                ),
                ..LiveCutoverRunHooks::default()
            },
        )
        .unwrap();
        let verify = verify_live_package_cutover_report(&Config {
            target_service_name: "other.service".to_string(),
            ..fixture.config
        })
        .expect("verify");
        assert_eq!(
            verify.verdict,
            TinyLivePackageLiveCutoverVerdict::TinyLivePackageLiveCutoverVerifyInvalid
        );
    }

    #[derive(Debug, Clone, Copy)]
    enum GateState {
        Green,
        Stage3Blocked,
        PreActivationBlocked,
    }

    struct LiveCutoverFixture {
        fixture_dir: PathBuf,
        target_config_path: PathBuf,
        original_target_config_path: PathBuf,
        config: Config,
        _rpc_server: MockHttpServer,
        _adapter_server: MockHttpServer,
    }

    fn live_cutover_fixture(label: &str, gate_state: GateState) -> LiveCutoverFixture {
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
            Some("authorization"),
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
        let original_target_config_path = fixture_dir.join("original-live.server.toml");
        fs::write(&original_target_config_path, &disabled_contents).unwrap();
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
        let install = tiny_live_activation_install_target::install_target_from_source_paths_for_package(
            &install_root,
            "solana-copy-bot.service",
            &backend_path.display().to_string(),
            tiny_live_activation_live_execute::live_service_control_wrapper_contract::DEFAULT_TIMEOUT_MS,
            &package_dir.join("wrapper/copybot-live-service-control"),
            &package_dir.join("artifacts/rendered.activation.toml"),
            &package_dir.join("artifacts/rendered.rollback.toml"),
        )
        .unwrap();
        assert_eq!(install.target_service_name, "solana-copy-bot.service");

        LiveCutoverFixture {
            fixture_dir: fixture_dir.clone(),
            target_config_path,
            original_target_config_path,
            config: Config {
                mode: Mode::RunLivePackageCutover,
                package_dir,
                install_root,
                target_service_name: "solana-copy-bot.service".to_string(),
                backend_command: backend_path.display().to_string(),
                wrapper_timeout_ms: tiny_live_activation_live_execute::live_service_control_wrapper_contract::DEFAULT_TIMEOUT_MS,
                service_status_max_staleness_ms:
                    tiny_live_activation_package_preflight::DEFAULT_SERVICE_STATUS_MAX_STALENESS_MS,
                session_dir: Some(fixture_dir.join("live-cutover-session")),
                output_path: None,
                json: false,
            },
            _rpc_server: rpc_server,
            _adapter_server: adapter_server,
        }
    }

    fn run_json_report(config: Config) -> PackageLiveCutoverReport {
        let output = run(Config {
            json: true,
            ..config
        })
        .expect("run json report");
        serde_json::from_str(&output).expect("parse json report")
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
        let metadata = tiny_live_activation_live_execute::tiny_live_activation_execute::RenderedConfigMetadata {
            metadata_version: "1".to_string(),
            render_kind,
            generated_at: ts("2026-03-28T12:00:00Z"),
            input_config_path: source_config_path.display().to_string(),
            output_config_path: path.display().to_string(),
            source_config_fingerprint_scope: tiny_live_activation_live_execute::tiny_live_activation_execute::FINGERPRINT_SCOPE.to_string(),
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
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("{}_{}", label, nanos));
        fs::create_dir_all(&dir).unwrap();
        dir
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
            let handle = std::thread::spawn(move || {
                for _ in 0..max_requests {
                    let (mut stream, _) = listener.accept().unwrap();
                    let mut buffer = [0u8; 8192];
                    let size = stream.read(&mut buffer).unwrap();
                    let request = String::from_utf8_lossy(&buffer[..size]);
                    if let Some(header) = &required_header {
                        assert!(request.to_ascii_lowercase().contains(header));
                    }
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{}",
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
            format!("http://{}:{}{}", self.addr.ip(), self.addr.port(), path)
        }
    }
}
