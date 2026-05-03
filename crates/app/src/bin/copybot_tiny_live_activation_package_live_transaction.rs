use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_config::load_from_path;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::env;
use std::fs;
use std::io::Write;
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
#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_package_preflight.rs"]
mod tiny_live_activation_package_preflight;

const USAGE: &str = "usage: copybot_tiny_live_activation_package_live_transaction --package-dir <path> --install-root <path> --target-service <name> --backend-command <path-or-name> [--wrapper-timeout-ms <ms>] [--service-status-max-staleness-ms <ms>] [--session-dir <path>] [--output <path>] [--json] (--plan-live-package-transaction | --render-live-package-transaction-script | --run-live-package-transaction | --verify-live-package-transaction)";
const TRANSACTION_SESSION_VERSION: &str = "1";
const TRANSACTION_STATUS_VERSION: &str = "1";
const WRAPPER_BINDING_REPORT_VERSION: &str = "1";
const SERVICE_STATUS_REPORT_VERSION: &str = "1";
const DRY_TRANSACTION_REPORT_VERSION: &str = "1";
const DEFAULT_STATUS_COMMAND_TIMEOUT_MS: u64 = 15_000;
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
    PlanLivePackageTransaction,
    RenderLivePackageTransactionScript,
    RunLivePackageTransaction,
    VerifyLivePackageTransaction,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageLiveTransactionVerdict {
    TinyLivePackageLiveTransactionPlanReady,
    TinyLivePackageLiveTransactionScriptRendered,
    TinyLivePackageLiveTransactionCompletedReadyForCutoverWhenGateAllows,
    TinyLivePackageLiveTransactionCompletedInstallTargetMissing,
    TinyLivePackageLiveTransactionCompletedInstallTargetDrifted,
    TinyLivePackageLiveTransactionCompletedBackupFailed,
    TinyLivePackageLiveTransactionCompletedDryTransactionFailed,
    TinyLivePackageLiveTransactionCompletedServiceProbeFailed,
    TinyLivePackageLiveTransactionCompletedCutoverBlockedByGate,
    TinyLivePackageLiveTransactionFailedDuringPackageVerify,
    TinyLivePackageLiveTransactionFailedDuringLiveTargetVerify,
    TinyLivePackageLiveTransactionVerifyOk,
    TinyLivePackageLiveTransactionVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LivePackageTransactionResult {
    ReadyForCutoverWhenGateAllows,
    InstallTargetMissing,
    InstallTargetDrifted,
    BackupFailed,
    DryTransactionFailed,
    ServiceProbeFailed,
    CutoverBlockedByGate,
    FailedDuringPackageVerify,
    FailedDuringLiveTargetVerify,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageLiveTransactionStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageLiveTransactionSession {
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
    backup_proof_command_summary: String,
    dry_transaction_command_summary: String,
    package_cutover_plan_command_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageLiveTransactionStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    session_dir: String,
    result: LivePackageTransactionResult,
    reason: String,
    package_verify_step: Option<PackageLiveTransactionStepArtifact>,
    live_target_verify_step: Option<PackageLiveTransactionStepArtifact>,
    wrapper_verify_step: Option<PackageLiveTransactionStepArtifact>,
    service_status_step: Option<PackageLiveTransactionStepArtifact>,
    backup_proof_step: Option<PackageLiveTransactionStepArtifact>,
    dry_transaction_step: Option<PackageLiveTransactionStepArtifact>,
    cutover_plan_step: Option<PackageLiveTransactionStepArtifact>,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackageLiveTransactionPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    package_verify_report_path: PathBuf,
    live_target_verify_report_path: PathBuf,
    wrapper_verify_report_path: PathBuf,
    service_status_report_path: PathBuf,
    service_status_artifact_path: PathBuf,
    service_status_runtime_dir: PathBuf,
    service_status_log_path: PathBuf,
    backup_report_path: PathBuf,
    dry_transaction_report_path: PathBuf,
    cutover_plan_report_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageLiveTransactionReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageLiveTransactionVerdict,
    reason: String,
    package_dir: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    session_dir: Option<String>,
    transaction_session_path: Option<String>,
    transaction_status_path: Option<String>,
    result: Option<String>,
    package_verify_step: Option<PackageLiveTransactionStepArtifact>,
    live_target_verify_step: Option<PackageLiveTransactionStepArtifact>,
    wrapper_verify_step: Option<PackageLiveTransactionStepArtifact>,
    service_status_step: Option<PackageLiveTransactionStepArtifact>,
    backup_proof_step: Option<PackageLiveTransactionStepArtifact>,
    dry_transaction_step: Option<PackageLiveTransactionStepArtifact>,
    cutover_plan_step: Option<PackageLiveTransactionStepArtifact>,
    package_verify_command_summary: String,
    verify_install_target_command_summary: String,
    verify_wrapper_binding_command_summary: String,
    verify_service_status_command_summary: String,
    backup_proof_command_summary: String,
    dry_transaction_command_summary: String,
    package_cutover_plan_command_summary: String,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    service_status_observed_at: Option<DateTime<Utc>>,
    service_status_age_ms: Option<i64>,
    verification_mismatches: Vec<String>,
    activation_authorized: bool,
    explicit_statement: String,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DryTransactionReport {
    report_version: String,
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: String,
    reason: String,
    package_dir: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    session_dir: String,
    target_config_path: String,
    target_config_parent: String,
    probe_source_path: String,
    probe_renamed_path: String,
    target_config_sha256_before: String,
    target_config_sha256_after: String,
    source_write_ok: bool,
    rename_ok: bool,
    source_cleanup_ok: bool,
    renamed_cleanup_ok: bool,
    parent_sync_ok: bool,
    target_config_bytes_unchanged: bool,
    verification_mismatches: Vec<String>,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct DryTransactionArtifacts {
    probe_source_path: PathBuf,
    probe_renamed_path: PathBuf,
}

#[derive(Default)]
struct LiveTransactionRunHooks<'a> {
    after_service_status_hook: Option<&'a dyn Fn(&Path) -> Result<()>>,
    before_backup_hook: Option<&'a dyn Fn(&BackupHookTargets) -> Result<()>>,
    before_dry_transaction_cleanup_hook: Option<&'a dyn Fn(&DryTransactionArtifacts) -> Result<()>>,
}

#[derive(Debug, Clone)]
struct BackupHookTargets {
    activation_config_path: PathBuf,
    rollback_config_path: PathBuf,
    target_config_path: PathBuf,
    installed_wrapper_path: PathBuf,
    runtime_dir: PathBuf,
    backup_dir: PathBuf,
}

#[derive(Debug, Deserialize)]
struct PackageDeployStoredView {
    verdict: String,
    reason: String,
    package_dir: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
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
    wrapper_path: String,
    target_config_path: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    installed_activation_config_path: String,
    installed_rollback_config_path: String,
    runtime_dir: String,
    backup_dir: String,
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
            "--plan-live-package-transaction" => set_mode(
                &mut mode,
                Mode::PlanLivePackageTransaction,
                "--plan-live-package-transaction",
            )?,
            "--render-live-package-transaction-script" => set_mode(
                &mut mode,
                Mode::RenderLivePackageTransactionScript,
                "--render-live-package-transaction-script",
            )?,
            "--run-live-package-transaction" => set_mode(
                &mut mode,
                Mode::RunLivePackageTransaction,
                "--run-live-package-transaction",
            )?,
            "--verify-live-package-transaction" => set_mode(
                &mut mode,
                Mode::VerifyLivePackageTransaction,
                "--verify-live-package-transaction",
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
    if matches!(mode, Mode::RenderLivePackageTransactionScript) && output_path.is_none() {
        bail!("missing required --output for --render-live-package-transaction-script");
    }
    if matches!(
        mode,
        Mode::RunLivePackageTransaction | Mode::VerifyLivePackageTransaction
    ) && session_dir.is_none()
    {
        bail!("missing required --session-dir");
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

fn run(config: Config) -> Result<String> {
    let report = match config.mode {
        Mode::PlanLivePackageTransaction => match plan_live_package_transaction_report(&config) {
            Ok(report) => report,
            Err(error) => refusal_report(
                &config,
                TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionVerifyInvalid,
                error.to_string(),
            ),
        },
        Mode::RenderLivePackageTransactionScript => {
            match render_live_package_transaction_script_report(&config) {
                Ok(report) => report,
                Err(error) => refusal_report(
                    &config,
                    TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionVerifyInvalid,
                    error.to_string(),
                ),
            }
        }
        Mode::RunLivePackageTransaction => match run_live_package_transaction_report(&config) {
            Ok(report) => report,
            Err(error) => refusal_report(
                &config,
                TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionVerifyInvalid,
                error.to_string(),
            ),
        },
        Mode::VerifyLivePackageTransaction => {
            match verify_live_package_transaction_report(&config) {
                Ok(report) => report,
                Err(error) => refusal_report(
                    &config,
                    TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionVerifyInvalid,
                    error.to_string(),
                ),
            }
        }
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_human_report(&report))
    }
}

fn plan_live_package_transaction_report(config: &Config) -> Result<PackageLiveTransactionReport> {
    tiny_live_activation_package::verify_package_contract_for_deploy(
        &config.package_dir,
        &config.install_root,
        &config.target_service_name,
        &config.backend_command,
        config.wrapper_timeout_ms,
    )?;
    Ok(base_report(
        config,
        TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionPlanReady,
        format!(
            "package {} is ready for a bounded live-target dry cutover transaction session over install root {}",
            config.package_dir.display(),
            config.install_root.display()
        ),
    ))
}

fn render_live_package_transaction_script_report(
    config: &Config,
) -> Result<PackageLiveTransactionReport> {
    let plan = plan_live_package_transaction_report(config)?;
    if plan.verdict
        != TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionPlanReady
    {
        return Ok(plan);
    }
    let output_path = config
        .output_path
        .as_ref()
        .ok_or_else(|| anyhow!("missing --output for --render-live-package-transaction-script"))?;
    ensure_new_output_path(output_path)?;
    let script = format!(
        "#!/usr/bin/env bash\nset -euo pipefail\nif [ \"$#\" -ne 1 ]; then\n  echo \"usage: $(basename \"$0\") <session-dir>\" >&2\n  exit 64\nfi\ncopybot_tiny_live_activation_package_live_transaction --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --service-status-max-staleness-ms {} --session-dir \"$1\" --run-live-package-transaction\n",
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
    Ok(PackageLiveTransactionReport {
        generated_at: Utc::now(),
        mode: "render_live_package_transaction_script".to_string(),
        verdict:
            TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionScriptRendered,
        reason: format!(
            "live-target dry transaction script rendered to {} without enabling execution or restarting the service",
            output_path.display()
        ),
        explicit_statement:
            "rendering the live-target dry transaction session script does not authorize or perform production activation by itself"
                .to_string(),
        ..plan
    })
}

fn run_live_package_transaction_report(config: &Config) -> Result<PackageLiveTransactionReport> {
    run_live_package_transaction_report_with_hooks(config, LiveTransactionRunHooks::default())
}

fn run_live_package_transaction_report_with_hooks(
    config: &Config,
    hooks: LiveTransactionRunHooks<'_>,
) -> Result<PackageLiveTransactionReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for --run-live-package-transaction"))?;
    validate_live_transaction_session_dir(&config.install_root, session_dir)?;
    ensure_clean_live_transaction_session_dir(session_dir)?;
    let paths = package_live_transaction_paths(session_dir);
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
        let status = PackageLiveTransactionStatus {
            status_version: TRANSACTION_STATUS_VERSION.to_string(),
            updated_at: Utc::now(),
            session_dir: session_dir.display().to_string(),
            result: LivePackageTransactionResult::FailedDuringPackageVerify,
            reason: package_verify.reason.clone(),
            package_verify_step: package_verify_step.clone(),
            live_target_verify_step: None,
            wrapper_verify_step: None,
            service_status_step: None,
            backup_proof_step: None,
            dry_transaction_step: None,
            cutover_plan_step: None,
            explicit_statement:
                "this live-target dry transaction session is bounded evidence only; it does not authorize production activation"
                    .to_string(),
        };
        persist_json(&paths.status_path, &status)?;
        return Ok(report_from_status(
            config,
            session_dir,
            &paths,
            TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionFailedDuringPackageVerify,
            &status,
            None,
            None,
            None,
            false,
        ));
    }

    let package_contract = tiny_live_activation_package::verify_package_contract_for_deploy(
        &config.package_dir,
        &config.install_root,
        &config.target_service_name,
        &config.backend_command,
        config.wrapper_timeout_ms,
    )?;

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
        let mut mismatches = live_target_verify.verification_mismatches.clone();
        mismatches.extend(wrapper_verify.verification_mismatches.clone());
        let result = if live_target_missing_on_disk(&live_target_verify) {
            LivePackageTransactionResult::InstallTargetMissing
        } else {
            LivePackageTransactionResult::InstallTargetDrifted
        };
        let verdict = match result {
            LivePackageTransactionResult::InstallTargetMissing => {
                TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionCompletedInstallTargetMissing
            }
            _ => {
                TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionCompletedInstallTargetDrifted
            }
        };
        let reason = mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| live_target_verify.reason.clone());
        let status = PackageLiveTransactionStatus {
            status_version: TRANSACTION_STATUS_VERSION.to_string(),
            updated_at: Utc::now(),
            session_dir: session_dir.display().to_string(),
            result,
            reason,
            package_verify_step: package_verify_step.clone(),
            live_target_verify_step: live_target_verify_step.clone(),
            wrapper_verify_step: wrapper_verify_step.clone(),
            service_status_step: None,
            backup_proof_step: None,
            dry_transaction_step: None,
            cutover_plan_step: None,
            explicit_statement:
                "this live-target dry transaction session is bounded evidence only; it does not authorize production activation"
                    .to_string(),
        };
        persist_json(&paths.status_path, &status)?;
        return Ok(report_from_status(
            config,
            session_dir,
            &paths,
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
        &live_target_verify.wrapper_path,
        &live_target_verify.target_config_path,
        &paths,
        hooks.after_service_status_hook,
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
        let status = PackageLiveTransactionStatus {
            status_version: TRANSACTION_STATUS_VERSION.to_string(),
            updated_at: Utc::now(),
            session_dir: session_dir.display().to_string(),
            result: LivePackageTransactionResult::ServiceProbeFailed,
            reason: service_status.reason.clone(),
            package_verify_step: package_verify_step.clone(),
            live_target_verify_step: live_target_verify_step.clone(),
            wrapper_verify_step: wrapper_verify_step.clone(),
            service_status_step: service_status_step.clone(),
            backup_proof_step: None,
            dry_transaction_step: None,
            cutover_plan_step: None,
            explicit_statement:
                "this live-target dry transaction session is bounded evidence only; it does not authorize production activation"
                    .to_string(),
        };
        persist_json(&paths.status_path, &status)?;
        return Ok(report_from_status(
            config,
            session_dir,
            &paths,
            TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionCompletedServiceProbeFailed,
            &status,
            None,
            None,
            service_status.service_status_observed_at,
            false,
        ));
    }

    let backup_targets = BackupHookTargets {
        activation_config_path: PathBuf::from(&live_target_verify.installed_activation_config_path),
        rollback_config_path: PathBuf::from(&live_target_verify.installed_rollback_config_path),
        target_config_path: PathBuf::from(&live_target_verify.target_config_path),
        installed_wrapper_path: PathBuf::from(&live_target_verify.wrapper_path),
        runtime_dir: PathBuf::from(&live_target_verify.runtime_dir),
        backup_dir: PathBuf::from(&live_target_verify.backup_dir),
    };
    if let Some(hook) = hooks.before_backup_hook {
        hook(&backup_targets)?;
    }
    let backup_report =
        tiny_live_activation_live_execute::backup_current_config_report_for_cutover(
            &backup_targets.activation_config_path,
            &backup_targets.rollback_config_path,
            &backup_targets.target_config_path,
            &config.target_service_name,
            &backup_targets.installed_wrapper_path,
            &backup_targets.runtime_dir,
            &backup_targets.backup_dir,
            DEFAULT_STARTUP_TIMEOUT_MS,
            DEFAULT_ROLLBACK_TIMEOUT_MS,
        )?;
    persist_json(&paths.backup_report_path, &backup_report)?;
    let backup_proof_step = Some(step_artifact(
        &paths.backup_report_path,
        "backup_current_config",
        &serialize_enum(&backup_report.verdict),
        &backup_report.reason,
        backup_report.generated_at,
    ));
    let backup_verified = if backup_report.verdict
        == tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveBackupCreated
    {
        tiny_live_activation_live_execute::load_backup_proof_for_watch(
            &backup_targets.activation_config_path,
            &backup_targets.rollback_config_path,
            &backup_targets.target_config_path,
            &config.target_service_name,
            &backup_targets.installed_wrapper_path,
            &backup_targets.runtime_dir,
            &backup_targets.backup_dir,
        )
        .map(|_| ())
        .map_err(|error| error.to_string())
    } else {
        Err(backup_report.reason.clone())
    };
    if let Err(reason) = backup_verified {
        let status = PackageLiveTransactionStatus {
            status_version: TRANSACTION_STATUS_VERSION.to_string(),
            updated_at: Utc::now(),
            session_dir: session_dir.display().to_string(),
            result: LivePackageTransactionResult::BackupFailed,
            reason,
            package_verify_step: package_verify_step.clone(),
            live_target_verify_step: live_target_verify_step.clone(),
            wrapper_verify_step: wrapper_verify_step.clone(),
            service_status_step: service_status_step.clone(),
            backup_proof_step: backup_proof_step.clone(),
            dry_transaction_step: None,
            cutover_plan_step: None,
            explicit_statement:
                "this live-target dry transaction session is bounded evidence only; it does not authorize production activation"
                    .to_string(),
        };
        persist_json(&paths.status_path, &status)?;
        return Ok(report_from_status(
            config,
            session_dir,
            &paths,
            TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionCompletedBackupFailed,
            &status,
            None,
            None,
            service_status.service_status_observed_at,
            false,
        ));
    }

    let dry_transaction = run_dry_transaction_report(
        config,
        session_dir,
        &live_target_verify.target_config_path,
        hooks.before_dry_transaction_cleanup_hook,
    )?;
    persist_json(&paths.dry_transaction_report_path, &dry_transaction)?;
    let dry_transaction_step = Some(step_artifact(
        &paths.dry_transaction_report_path,
        "run_dry_target_config_transaction",
        &dry_transaction.verdict,
        &dry_transaction.reason,
        dry_transaction.generated_at,
    ));
    if dry_transaction.verdict != "tiny_live_package_live_transaction_dry_transaction_ok" {
        let status = PackageLiveTransactionStatus {
            status_version: TRANSACTION_STATUS_VERSION.to_string(),
            updated_at: Utc::now(),
            session_dir: session_dir.display().to_string(),
            result: LivePackageTransactionResult::DryTransactionFailed,
            reason: dry_transaction.reason.clone(),
            package_verify_step: package_verify_step.clone(),
            live_target_verify_step: live_target_verify_step.clone(),
            wrapper_verify_step: wrapper_verify_step.clone(),
            service_status_step: service_status_step.clone(),
            backup_proof_step: backup_proof_step.clone(),
            dry_transaction_step: dry_transaction_step.clone(),
            cutover_plan_step: None,
            explicit_statement:
                "this live-target dry transaction session is bounded evidence only; it does not authorize production activation"
                    .to_string(),
        };
        persist_json(&paths.status_path, &status)?;
        return Ok(report_from_status(
            config,
            session_dir,
            &paths,
            TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionCompletedDryTransactionFailed,
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
            LivePackageTransactionResult::ReadyForCutoverWhenGateAllows,
            TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionCompletedReadyForCutoverWhenGateAllows,
        ),
        "tiny_live_package_cutover_refused_by_stage3"
        | "tiny_live_package_cutover_refused_by_pre_activation_gate" => (
            LivePackageTransactionResult::CutoverBlockedByGate,
            TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionCompletedCutoverBlockedByGate,
        ),
        _ => (
            LivePackageTransactionResult::FailedDuringLiveTargetVerify,
            TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionFailedDuringLiveTargetVerify,
        ),
    };
    let status = PackageLiveTransactionStatus {
        status_version: TRANSACTION_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        session_dir: session_dir.display().to_string(),
        result,
        reason: cutover_plan.reason.clone(),
        package_verify_step,
        live_target_verify_step,
        wrapper_verify_step,
        service_status_step,
        backup_proof_step,
        dry_transaction_step,
        cutover_plan_step,
        explicit_statement:
            "this live-target dry transaction session is bounded evidence only; it does not authorize production activation"
                .to_string(),
    };
    persist_json(&paths.status_path, &status)?;
    Ok(report_from_status(
        config,
        session_dir,
        &paths,
        verdict,
        &status,
        cutover_plan.current_pre_activation_gate_verdict.clone(),
        cutover_plan.current_pre_activation_gate_reason.clone(),
        service_status.service_status_observed_at,
        cutover_plan.activation_authorized,
    ))
}

fn verify_live_package_transaction_report(config: &Config) -> Result<PackageLiveTransactionReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for --verify-live-package-transaction"))?;
    validate_live_transaction_session_dir(&config.install_root, session_dir)?;
    let package_contract = match tiny_live_activation_package::verify_package_contract_for_deploy(
        &config.package_dir,
        &config.install_root,
        &config.target_service_name,
        &config.backend_command,
        config.wrapper_timeout_ms,
    ) {
        Ok(value) => value,
        Err(error) => {
            let paths = package_live_transaction_paths(session_dir);
            return Ok(PackageLiveTransactionReport {
                generated_at: Utc::now(),
                mode: "verify_live_package_transaction".to_string(),
                verdict:
                    TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionVerifyInvalid,
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
                transaction_session_path: Some(paths.session_path.display().to_string()),
                transaction_status_path: Some(paths.status_path.display().to_string()),
                result: None,
                package_verify_step: None,
                live_target_verify_step: None,
                wrapper_verify_step: None,
                service_status_step: None,
                backup_proof_step: None,
                dry_transaction_step: None,
                cutover_plan_step: None,
                package_verify_command_summary: package_verify_command_summary(config),
                verify_install_target_command_summary: verify_install_target_command_summary(config),
                verify_wrapper_binding_command_summary: verify_wrapper_binding_command_summary(config),
                verify_service_status_command_summary: verify_service_status_command_summary(config, &paths),
                backup_proof_command_summary: package_backup_command_summary(config),
                dry_transaction_command_summary: dry_transaction_command_summary(
                    config,
                    &config
                        .install_root
                        .join("etc/solana-copy-bot/live.server.toml")
                        .display()
                        .to_string(),
                    session_dir,
                ),
                package_cutover_plan_command_summary: package_cutover_plan_command_summary(config),
                current_pre_activation_gate_verdict: None,
                current_pre_activation_gate_reason: None,
                service_status_observed_at: None,
                service_status_age_ms: None,
                verification_mismatches: vec![error.to_string()],
                activation_authorized: false,
                explicit_statement:
                    "this live-target dry transaction verification checks bounded host evidence only; it does not authorize production activation"
                        .to_string(),
            });
        }
    };
    let paths = package_live_transaction_paths(session_dir);
    let session: PackageLiveTransactionSession = load_json(&paths.session_path)?;
    let status: PackageLiveTransactionStatus = load_json(&paths.status_path)?;
    let mut mismatches = Vec::new();

    compare_string(
        &session.package_dir,
        &config.package_dir.display().to_string(),
        "package live transaction package_dir",
        &mut mismatches,
    );
    compare_string(
        &session.install_root,
        &config.install_root.display().to_string(),
        "package live transaction install_root",
        &mut mismatches,
    );
    compare_string(
        &session.target_service_name,
        &config.target_service_name,
        "package live transaction target_service_name",
        &mut mismatches,
    );
    compare_string(
        &session.backend_command,
        &config.backend_command,
        "package live transaction backend_command",
        &mut mismatches,
    );
    if session.wrapper_timeout_ms != config.wrapper_timeout_ms {
        mismatches.push(format!(
            "package live transaction wrapper_timeout_ms {} does not match requested {}",
            session.wrapper_timeout_ms, config.wrapper_timeout_ms
        ));
    }
    if session.service_status_max_staleness_ms != config.service_status_max_staleness_ms {
        mismatches.push(format!(
            "package live transaction service_status_max_staleness_ms {} does not match requested {}",
            session.service_status_max_staleness_ms, config.service_status_max_staleness_ms
        ));
    }
    if session.session_dir != session_dir.display().to_string() {
        mismatches.push(
            "package live transaction session_dir does not match the explicit session dir"
                .to_string(),
        );
    }
    if status.session_dir != session_dir.display().to_string() {
        mismatches.push(
            "package live transaction status session_dir does not match the explicit session dir"
                .to_string(),
        );
    }

    let stored_package_verify: PackageDeployStoredView = load_required_step_json(
        &status.package_verify_step,
        &paths.package_verify_report_path,
        "package_verify_step",
        &mut mismatches,
    )?;
    compare_package_deploy_view(
        &stored_package_verify,
        config,
        "stored package verify report",
        &mut mismatches,
    );
    let rerun_package_verify = tiny_live_activation_package_deploy::run_mode_for_rehearsal(
        &config.package_dir,
        &config.install_root,
        &config.target_service_name,
        &config.backend_command,
        config.wrapper_timeout_ms,
        tiny_live_activation_package_deploy::RehearsalMode::VerifyPackageDeployTarget,
    )?;
    if stored_package_verify.verdict != rerun_package_verify.verdict {
        mismatches.push(format!(
            "stored package verify verdict {} does not match current package verify verdict {}",
            stored_package_verify.verdict, rerun_package_verify.verdict
        ));
    }
    if stored_package_verify.reason != rerun_package_verify.reason {
        mismatches.push(
            "stored package verify reason does not match current package verify reason".to_string(),
        );
    }

    if status.result == LivePackageTransactionResult::FailedDuringPackageVerify {
        if status.live_target_verify_step.is_some()
            || status.wrapper_verify_step.is_some()
            || status.service_status_step.is_some()
            || status.backup_proof_step.is_some()
            || status.dry_transaction_step.is_some()
            || status.cutover_plan_step.is_some()
        {
            mismatches.push(
                "later live transaction steps must be absent when result is failed_during_package_verify"
                    .to_string(),
            );
        }
        let verdict = if mismatches.is_empty() {
            TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionVerifyOk
        } else {
            TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionVerifyInvalid
        };
        return Ok(verified_report_from_status(
            config,
            session_dir,
            &paths,
            verdict,
            &status,
            None,
            None,
            None,
            false,
            mismatches,
        ));
    }

    let stored_live_target: InstallTargetReportView = load_required_step_json(
        &status.live_target_verify_step,
        &paths.live_target_verify_report_path,
        "live_target_verify_step",
        &mut mismatches,
    )?;
    compare_install_target_report_view(
        &stored_live_target,
        config,
        "stored live target verify report",
        &mut mismatches,
    );
    let live_target_verify =
        tiny_live_activation_install_target::verify_install_target_for_rehearsal(
            &config.install_root,
            &config.target_service_name,
            &config.backend_command,
            config.wrapper_timeout_ms,
        )?;
    if live_target_verify.verdict != stored_live_target.verdict {
        mismatches.push(format!(
            "stored live target verify verdict {} does not match current {}",
            stored_live_target.verdict, live_target_verify.verdict
        ));
    }

    let stored_wrapper: WrapperBindingReport = load_required_step_json(
        &status.wrapper_verify_step,
        &paths.wrapper_verify_report_path,
        "wrapper_verify_step",
        &mut mismatches,
    )?;
    validate_wrapper_binding_report_contract(
        &stored_wrapper,
        config,
        &package_contract,
        &live_target_verify,
        &mut mismatches,
    );
    let rerun_wrapper = verify_installed_wrapper_binding(
        &config.package_dir,
        &config.install_root,
        &package_contract,
        &live_target_verify,
    )?;
    if stored_wrapper.verdict != rerun_wrapper.verdict {
        mismatches.push(format!(
            "stored wrapper verify verdict {} does not match current {}",
            stored_wrapper.verdict, rerun_wrapper.verdict
        ));
    }

    if matches!(
        status.result,
        LivePackageTransactionResult::InstallTargetMissing
            | LivePackageTransactionResult::InstallTargetDrifted
            | LivePackageTransactionResult::FailedDuringLiveTargetVerify
    ) {
        if status.service_status_step.is_some()
            || status.backup_proof_step.is_some()
            || status.dry_transaction_step.is_some()
            || status.cutover_plan_step.is_some()
        {
            mismatches.push(
                "service/backup/dry/cutover steps must be absent when live target verification failed"
                    .to_string(),
            );
        }
        let verdict = if mismatches.is_empty() {
            TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionVerifyOk
        } else {
            TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionVerifyInvalid
        };
        return Ok(verified_report_from_status(
            config,
            session_dir,
            &paths,
            verdict,
            &status,
            None,
            None,
            None,
            false,
            mismatches,
        ));
    }

    let stored_service_status: ServiceStatusReport = load_required_step_json(
        &status.service_status_step,
        &paths.service_status_report_path,
        "service_status_step",
        &mut mismatches,
    )?;
    validate_service_status_report_contract(
        &stored_service_status,
        config,
        &package_contract,
        &stored_live_target,
        &paths,
        &mut mismatches,
    );
    let service_status_observed_at = stored_service_status.service_status_observed_at;

    if status.result == LivePackageTransactionResult::ServiceProbeFailed {
        if status.backup_proof_step.is_some()
            || status.dry_transaction_step.is_some()
            || status.cutover_plan_step.is_some()
        {
            mismatches.push(
                "backup/dry/cutover steps must be absent when service probe failed".to_string(),
            );
        }
        let verdict = if mismatches.is_empty() {
            TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionVerifyOk
        } else {
            TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionVerifyInvalid
        };
        return Ok(verified_report_from_status(
            config,
            session_dir,
            &paths,
            verdict,
            &status,
            None,
            None,
            service_status_observed_at,
            false,
            mismatches,
        ));
    }

    let stored_backup: tiny_live_activation_live_execute::LiveExecuteReport =
        load_required_step_json(
            &status.backup_proof_step,
            &paths.backup_report_path,
            "backup_proof_step",
            &mut mismatches,
        )?;
    validate_backup_report_contract(&stored_backup, config, &stored_live_target, &mut mismatches);
    if let Err(error) = tiny_live_activation_live_execute::load_backup_proof_for_watch(
        Path::new(&stored_live_target.installed_activation_config_path),
        Path::new(&stored_live_target.installed_rollback_config_path),
        Path::new(&stored_live_target.target_config_path),
        &config.target_service_name,
        Path::new(&stored_live_target.wrapper_path),
        Path::new(&stored_live_target.runtime_dir),
        Path::new(&stored_live_target.backup_dir),
    ) {
        mismatches.push(format!("live backup proof is no longer valid: {error}"));
    }

    if status.result == LivePackageTransactionResult::BackupFailed {
        if status.dry_transaction_step.is_some() || status.cutover_plan_step.is_some() {
            mismatches
                .push("dry/cutover steps must be absent when backup proof failed".to_string());
        }
        let verdict = if mismatches.is_empty() {
            TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionVerifyOk
        } else {
            TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionVerifyInvalid
        };
        return Ok(verified_report_from_status(
            config,
            session_dir,
            &paths,
            verdict,
            &status,
            None,
            None,
            service_status_observed_at,
            false,
            mismatches,
        ));
    }

    let stored_dry_transaction: DryTransactionReport = load_required_step_json(
        &status.dry_transaction_step,
        &paths.dry_transaction_report_path,
        "dry_transaction_step",
        &mut mismatches,
    )?;
    validate_dry_transaction_report_contract(
        &stored_dry_transaction,
        config,
        session_dir,
        &stored_live_target.target_config_path,
        &mut mismatches,
    );

    if status.result == LivePackageTransactionResult::DryTransactionFailed {
        if status.cutover_plan_step.is_some() {
            mismatches.push("cutover step must be absent when dry transaction failed".to_string());
        }
        let verdict = if mismatches.is_empty() {
            TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionVerifyOk
        } else {
            TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionVerifyInvalid
        };
        return Ok(verified_report_from_status(
            config,
            session_dir,
            &paths,
            verdict,
            &status,
            None,
            None,
            service_status_observed_at,
            false,
            mismatches,
        ));
    }

    let stored_cutover: PackageDeployStoredView = load_required_step_json(
        &status.cutover_plan_step,
        &paths.cutover_plan_report_path,
        "cutover_plan_step",
        &mut mismatches,
    )?;
    compare_package_deploy_view(
        &stored_cutover,
        config,
        "stored cutover plan report",
        &mut mismatches,
    );
    let rerun_cutover = tiny_live_activation_package_deploy::run_mode_for_rehearsal(
        &config.package_dir,
        &config.install_root,
        &config.target_service_name,
        &config.backend_command,
        config.wrapper_timeout_ms,
        tiny_live_activation_package_deploy::RehearsalMode::PlanPackageCutover,
    )?;
    if stored_cutover.verdict != rerun_cutover.verdict {
        mismatches.push(format!(
            "stored cutover plan verdict {} does not match current {}",
            stored_cutover.verdict, rerun_cutover.verdict
        ));
    }
    if stored_cutover.reason != rerun_cutover.reason {
        mismatches.push(
            "stored cutover plan reason does not match current cutover plan reason".to_string(),
        );
    }

    let verdict = if mismatches.is_empty() {
        TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionVerifyOk
    } else {
        TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionVerifyInvalid
    };
    Ok(verified_report_from_status(
        config,
        session_dir,
        &paths,
        verdict,
        &status,
        stored_cutover.current_pre_activation_gate_verdict,
        stored_cutover.current_pre_activation_gate_reason,
        service_status_observed_at,
        stored_cutover.activation_authorized,
        mismatches,
    ))
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
        report_version: WRAPPER_BINDING_REPORT_VERSION.to_string(),
        generated_at: Utc::now(),
        mode: "verify_installed_wrapper_binding".to_string(),
        verdict,
        reason,
        package_dir: package_dir.display().to_string(),
        install_root: install_root.display().to_string(),
        packaged_wrapper_path: package_contract.wrapper_path.display().to_string(),
        installed_wrapper_path: install_target.wrapper_path.clone(),
        packaged_activation_config_path: package_contract
            .activation_artifact_path
            .display()
            .to_string(),
        installed_activation_config_path: install_target.installed_activation_config_path.clone(),
        packaged_rollback_config_path: package_contract
            .rollback_artifact_path
            .display()
            .to_string(),
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
    paths: &PackageLiveTransactionPaths,
    after_service_status_hook: Option<&dyn Fn(&Path) -> Result<()>>,
) -> Result<ServiceStatusReport> {
    let wrapper_verification =
        tiny_live_activation_live_execute::inspect_service_control_wrapper_contract_for_live_target(
            Path::new(installed_wrapper_path),
        )?;
    if paths.service_status_artifact_path.exists() {
        fs::remove_file(&paths.service_status_artifact_path).ok();
    }
    fs::create_dir_all(&paths.service_status_runtime_dir).with_context(|| {
        format!(
            "failed creating service-status scratch runtime dir {}",
            paths.service_status_runtime_dir.display()
        )
    })?;
    let current_target = load_from_path(Path::new(target_config_path)).with_context(|| {
        format!(
            "failed loading target config {} for live package transaction",
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
        &paths.service_status_log_path,
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
            "this live service-status transaction step is a bounded read-only probe; it does not restart the service or authorize activation"
                .to_string(),
    })
}

fn run_dry_transaction_report(
    config: &Config,
    session_dir: &Path,
    target_config_path: &str,
    before_cleanup_hook: Option<&dyn Fn(&DryTransactionArtifacts) -> Result<()>>,
) -> Result<DryTransactionReport> {
    let target_config_path = Path::new(target_config_path);
    let target_parent = target_config_path.parent().ok_or_else(|| {
        anyhow!(
            "target config path {} has no parent",
            target_config_path.display()
        )
    })?;
    let before_bytes = fs::read(target_config_path).with_context(|| {
        format!(
            "failed reading target config {} before dry transaction rehearsal",
            target_config_path.display()
        )
    })?;
    let probe_id = format!(
        "tiny_live_activation_package_live_transaction.{}",
        Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_else(|| Utc::now().timestamp_micros() * 1_000)
    );
    let artifacts = DryTransactionArtifacts {
        probe_source_path: target_parent.join(format!(".{probe_id}.source.tmp")),
        probe_renamed_path: target_parent.join(format!(".{probe_id}.renamed.tmp")),
    };
    let mut mismatches = Vec::new();
    let mut source_write_ok = false;
    let mut rename_ok = false;
    let mut source_cleanup_ok = true;
    let mut renamed_cleanup_ok = true;
    let mut parent_sync_ok = true;

    if let Err(error) = write_synced_probe(
        &artifacts.probe_source_path,
        b"live-target dry transaction probe\n",
    ) {
        mismatches.push(format!(
            "target-config parent {} is not usable for bounded dry transaction temp writes: {error}",
            target_parent.display()
        ));
        source_cleanup_ok = !artifacts.probe_source_path.exists();
        renamed_cleanup_ok = !artifacts.probe_renamed_path.exists();
    } else {
        source_write_ok = true;
        if let Err(error) =
            rename_synced_probe(&artifacts.probe_source_path, &artifacts.probe_renamed_path)
        {
            mismatches.push(format!(
                "target-config parent {} failed bounded dry transaction rename rehearsal: {error}",
                target_parent.display()
            ));
            source_cleanup_ok = !artifacts.probe_source_path.exists();
            renamed_cleanup_ok = !artifacts.probe_renamed_path.exists();
        } else {
            rename_ok = true;
        }
    }

    if let Some(hook) = before_cleanup_hook {
        if let Err(error) = hook(&artifacts) {
            mismatches.push(format!(
                "dry transaction cleanup preparation failed for {}: {error}",
                target_parent.display()
            ));
        }
    }

    if artifacts.probe_source_path.exists() {
        if let Err(error) = fs::remove_file(&artifacts.probe_source_path) {
            source_cleanup_ok = false;
            mismatches.push(format!(
                "dry transaction probe cleanup failed for {}: {error}",
                artifacts.probe_source_path.display()
            ));
        }
    }
    if artifacts.probe_renamed_path.exists() {
        if let Err(error) = fs::remove_file(&artifacts.probe_renamed_path) {
            renamed_cleanup_ok = false;
            mismatches.push(format!(
                "dry transaction probe cleanup failed for {}: {error}",
                artifacts.probe_renamed_path.display()
            ));
        }
    }
    if let Err(error) = sync_dir(target_parent) {
        parent_sync_ok = false;
        mismatches.push(format!(
            "target-config parent cleanup sync failed for {}: {error}",
            target_parent.display()
        ));
    }

    let after_bytes = fs::read(target_config_path).with_context(|| {
        format!(
            "failed reading target config {} after dry transaction rehearsal",
            target_config_path.display()
        )
    })?;
    let target_config_bytes_unchanged = before_bytes == after_bytes;
    if !target_config_bytes_unchanged {
        mismatches.push(format!(
            "dry transaction rehearsal changed effective contents of target config {}",
            target_config_path.display()
        ));
    }

    let verdict = if mismatches.is_empty() {
        "tiny_live_package_live_transaction_dry_transaction_ok".to_string()
    } else {
        "tiny_live_package_live_transaction_dry_transaction_invalid".to_string()
    };
    let reason = if mismatches.is_empty() {
        format!(
            "target-config parent {} supports bounded sibling temp/write/fsync/rename/cleanup rehearsal without changing {}",
            target_parent.display(),
            target_config_path.display()
        )
    } else {
        mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "dry transaction rehearsal failed".to_string())
    };

    Ok(DryTransactionReport {
        report_version: DRY_TRANSACTION_REPORT_VERSION.to_string(),
        generated_at: Utc::now(),
        mode: "run_live_package_dry_transaction".to_string(),
        verdict,
        reason,
        package_dir: config.package_dir.display().to_string(),
        install_root: config.install_root.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        session_dir: session_dir.display().to_string(),
        target_config_path: target_config_path.display().to_string(),
        target_config_parent: target_parent.display().to_string(),
        probe_source_path: artifacts.probe_source_path.display().to_string(),
        probe_renamed_path: artifacts.probe_renamed_path.display().to_string(),
        target_config_sha256_before: sha256_hex(&before_bytes),
        target_config_sha256_after: sha256_hex(&after_bytes),
        source_write_ok,
        rename_ok,
        source_cleanup_ok,
        renamed_cleanup_ok,
        parent_sync_ok,
        target_config_bytes_unchanged,
        verification_mismatches: mismatches,
        explicit_statement:
            "this dry transaction rehearsal uses bounded sibling temp artifacts only and verifies the live target config remains byte-identical; it does not enable execution or authorize activation"
                .to_string(),
    })
}

fn run_service_status_command(
    wrapper_path: &Path,
    target_service_name: &str,
    target_config_path: &Path,
    runtime_dir: &Path,
    status_path: &Path,
    log_path: &Path,
    expected_fingerprint_sha256: &str,
    expected_execution_enabled: bool,
) -> Result<bool> {
    let log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .with_context(|| {
            format!(
                "failed opening transaction service-status log {}",
                log_path.display()
            )
        })?;
    let stderr = log
        .try_clone()
        .with_context(|| "failed cloning transaction service-status log handle")?;
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
        thread::sleep(Duration::from_millis(50));
    }
}

fn validate_live_transaction_session_dir(install_root: &Path, session_dir: &Path) -> Result<()> {
    if !install_root.is_absolute() {
        bail!("install root must be absolute");
    }
    if !session_dir.is_absolute() {
        bail!("session dir must be absolute");
    }
    let managed_surface =
        tiny_live_activation_install_target::derive_install_target_managed_surface_paths(
            install_root,
        )?;
    for (label, path) in [
        ("live wrapper path", managed_surface.wrapper_path.as_path()),
        (
            "live wrapper parent",
            managed_surface.wrapper_parent.as_path(),
        ),
        (
            "live target config path",
            managed_surface.target_config_path.as_path(),
        ),
        (
            "live target config parent",
            managed_surface.target_config_parent.as_path(),
        ),
        ("live runtime dir", managed_surface.runtime_dir.as_path()),
        ("live backup dir", managed_surface.backup_dir.as_path()),
        ("live session dir", managed_surface.session_dir.as_path()),
    ] {
        validate_disjoint_from_managed_path(session_dir, path, "session dir", label)?;
    }
    Ok(())
}

fn package_live_transaction_paths(session_dir: &Path) -> PackageLiveTransactionPaths {
    PackageLiveTransactionPaths {
        session_path: session_dir
            .join("tiny_live_activation_package_live_transaction.session.json"),
        status_path: session_dir.join("tiny_live_activation_package_live_transaction.status.json"),
        package_verify_report_path: session_dir
            .join("tiny_live_activation_package_live_transaction.package_verify.report.json"),
        live_target_verify_report_path: session_dir
            .join("tiny_live_activation_package_live_transaction.live_target_verify.report.json"),
        wrapper_verify_report_path: session_dir
            .join("tiny_live_activation_package_live_transaction.wrapper_verify.report.json"),
        service_status_report_path: session_dir
            .join("tiny_live_activation_package_live_transaction.service_status.report.json"),
        service_status_artifact_path: session_dir
            .join("tiny_live_activation_package_live_transaction.service_status.raw.json"),
        service_status_runtime_dir: session_dir
            .join("tiny_live_activation_package_live_transaction.service_status.runtime"),
        service_status_log_path: session_dir
            .join("tiny_live_activation_package_live_transaction.service_status.log"),
        backup_report_path: session_dir
            .join("tiny_live_activation_package_live_transaction.backup.report.json"),
        dry_transaction_report_path: session_dir
            .join("tiny_live_activation_package_live_transaction.dry_transaction.report.json"),
        cutover_plan_report_path: session_dir
            .join("tiny_live_activation_package_live_transaction.cutover_plan.report.json"),
    }
}

fn ensure_clean_live_transaction_session_dir(session_dir: &Path) -> Result<()> {
    let paths = package_live_transaction_paths(session_dir);
    for artifact in [
        &paths.session_path,
        &paths.status_path,
        &paths.package_verify_report_path,
        &paths.live_target_verify_report_path,
        &paths.wrapper_verify_report_path,
        &paths.service_status_report_path,
        &paths.service_status_artifact_path,
        &paths.service_status_log_path,
        &paths.backup_report_path,
        &paths.dry_transaction_report_path,
        &paths.cutover_plan_report_path,
    ] {
        if artifact.exists() {
            bail!(
                "refusing to reuse live package transaction session dir {}; artifact {} already exists",
                session_dir.display(),
                artifact.display()
            );
        }
    }
    if paths.service_status_runtime_dir.exists() {
        bail!(
            "refusing to reuse live package transaction session dir {}; service-status scratch runtime {} already exists",
            session_dir.display(),
            paths.service_status_runtime_dir.display()
        );
    }
    fs::create_dir_all(session_dir)
        .with_context(|| format!("failed creating session dir {}", session_dir.display()))
}

fn planned_session(config: &Config, session_dir: &Path) -> PackageLiveTransactionSession {
    let managed_surface =
        tiny_live_activation_install_target::derive_install_target_managed_surface_paths(
            &config.install_root,
        )
        .ok();
    let target_config_path = managed_surface
        .as_ref()
        .map(|value| value.target_config_path.display().to_string())
        .unwrap_or_else(|| {
            config
                .install_root
                .join("etc/solana-copy-bot/live.server.toml")
                .display()
                .to_string()
        });
    PackageLiveTransactionSession {
        session_version: TRANSACTION_SESSION_VERSION.to_string(),
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
        verify_service_status_command_summary: verify_service_status_command_summary(
            config,
            &package_live_transaction_paths(session_dir),
        ),
        backup_proof_command_summary: package_backup_command_summary(config),
        dry_transaction_command_summary: dry_transaction_command_summary(
            config,
            &target_config_path,
            session_dir,
        ),
        package_cutover_plan_command_summary: package_cutover_plan_command_summary(config),
        explicit_statement:
            "this live-target dry transaction session binds one immutable package plus one explicit live host contract; it does not authorize production activation"
                .to_string(),
    }
}

fn base_report(
    config: &Config,
    verdict: TinyLivePackageLiveTransactionVerdict,
    reason: String,
) -> PackageLiveTransactionReport {
    let paths = config
        .session_dir
        .as_ref()
        .map(|path| package_live_transaction_paths(path));
    let managed_surface =
        tiny_live_activation_install_target::derive_install_target_managed_surface_paths(
            &config.install_root,
        )
        .ok();
    let target_config_path = managed_surface
        .as_ref()
        .map(|value| value.target_config_path.display().to_string())
        .unwrap_or_else(|| {
            config
                .install_root
                .join("etc/solana-copy-bot/live.server.toml")
                .display()
                .to_string()
        });
    let fallback_session_dir = PathBuf::from("<session-dir>");
    PackageLiveTransactionReport {
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
        transaction_session_path: paths
            .as_ref()
            .map(|value| value.session_path.display().to_string()),
        transaction_status_path: paths
            .as_ref()
            .map(|value| value.status_path.display().to_string()),
        result: None,
        package_verify_step: None,
        live_target_verify_step: None,
        wrapper_verify_step: None,
        service_status_step: None,
        backup_proof_step: None,
        dry_transaction_step: None,
        cutover_plan_step: None,
        package_verify_command_summary: package_verify_command_summary(config),
        verify_install_target_command_summary: verify_install_target_command_summary(config),
        verify_wrapper_binding_command_summary: verify_wrapper_binding_command_summary(config),
        verify_service_status_command_summary: verify_service_status_command_summary(
            config,
            &paths
                .as_ref()
                .cloned()
                .unwrap_or_else(|| package_live_transaction_paths(Path::new("<session-dir>"))),
        ),
        backup_proof_command_summary: package_backup_command_summary(config),
        dry_transaction_command_summary: dry_transaction_command_summary(
            config,
            &target_config_path,
            config
                .session_dir
                .as_ref()
                .unwrap_or(&fallback_session_dir),
        ),
        package_cutover_plan_command_summary: package_cutover_plan_command_summary(config),
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        service_status_observed_at: None,
        service_status_age_ms: None,
        verification_mismatches: Vec::new(),
        activation_authorized: false,
        explicit_statement:
            "this live-target dry transaction layer is bounded host evidence only; it does not authorize or perform production activation by itself"
                .to_string(),
    }
}

fn refusal_report(
    config: &Config,
    verdict: TinyLivePackageLiveTransactionVerdict,
    reason: String,
) -> PackageLiveTransactionReport {
    base_report(config, verdict, reason)
}

fn report_from_status(
    config: &Config,
    session_dir: &Path,
    paths: &PackageLiveTransactionPaths,
    verdict: TinyLivePackageLiveTransactionVerdict,
    status: &PackageLiveTransactionStatus,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    service_status_observed_at: Option<DateTime<Utc>>,
    activation_authorized: bool,
) -> PackageLiveTransactionReport {
    PackageLiveTransactionReport {
        generated_at: Utc::now(),
        mode: "run_live_package_transaction".to_string(),
        verdict,
        reason: status.reason.clone(),
        package_dir: config.package_dir.display().to_string(),
        install_root: config.install_root.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        wrapper_timeout_ms: config.wrapper_timeout_ms,
        service_status_max_staleness_ms: config.service_status_max_staleness_ms,
        session_dir: Some(session_dir.display().to_string()),
        transaction_session_path: Some(paths.session_path.display().to_string()),
        transaction_status_path: Some(paths.status_path.display().to_string()),
        result: Some(serialize_enum(&status.result)),
        package_verify_step: status.package_verify_step.clone(),
        live_target_verify_step: status.live_target_verify_step.clone(),
        wrapper_verify_step: status.wrapper_verify_step.clone(),
        service_status_step: status.service_status_step.clone(),
        backup_proof_step: status.backup_proof_step.clone(),
        dry_transaction_step: status.dry_transaction_step.clone(),
        cutover_plan_step: status.cutover_plan_step.clone(),
        package_verify_command_summary: package_verify_command_summary(config),
        verify_install_target_command_summary: verify_install_target_command_summary(config),
        verify_wrapper_binding_command_summary: verify_wrapper_binding_command_summary(config),
        verify_service_status_command_summary: verify_service_status_command_summary(config, paths),
        backup_proof_command_summary: package_backup_command_summary(config),
        dry_transaction_command_summary: dry_transaction_command_summary(
            config,
            &tiny_live_activation_install_target::derive_install_target_managed_surface_paths(
                &config.install_root,
            )
            .map(|value| value.target_config_path.display().to_string())
            .unwrap_or_else(|_| {
                config
                    .install_root
                    .join("etc/solana-copy-bot/live.server.toml")
                    .display()
                    .to_string()
            }),
            session_dir,
        ),
        package_cutover_plan_command_summary: package_cutover_plan_command_summary(config),
        current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason,
        service_status_observed_at,
        service_status_age_ms: service_status_observed_at.map(age_ms),
        verification_mismatches: Vec::new(),
        activation_authorized,
        explicit_statement:
            "this live-target dry transaction session is bounded host evidence only; it does not authorize production activation"
                .to_string(),
    }
}

fn verified_report_from_status(
    config: &Config,
    session_dir: &Path,
    paths: &PackageLiveTransactionPaths,
    verdict: TinyLivePackageLiveTransactionVerdict,
    status: &PackageLiveTransactionStatus,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    service_status_observed_at: Option<DateTime<Utc>>,
    activation_authorized: bool,
    verification_mismatches: Vec<String>,
) -> PackageLiveTransactionReport {
    let mut report = report_from_status(
        config,
        session_dir,
        paths,
        verdict,
        status,
        current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason,
        service_status_observed_at,
        activation_authorized,
    );
    report.mode = "verify_live_package_transaction".to_string();
    report.verification_mismatches = verification_mismatches;
    report.reason = if report.verification_mismatches.is_empty() {
        "live-target dry transaction session evidence is coherent and bound to the explicit target contract".to_string()
    } else {
        report
            .verification_mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "live-target dry transaction verification failed".to_string())
    };
    report
}

fn mode_name(mode: Mode) -> &'static str {
    match mode {
        Mode::PlanLivePackageTransaction => "plan_live_package_transaction",
        Mode::RenderLivePackageTransactionScript => "render_live_package_transaction_script",
        Mode::RunLivePackageTransaction => "run_live_package_transaction",
        Mode::VerifyLivePackageTransaction => "verify_live_package_transaction",
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
        "bounded wrapper/package binding verification: compare installed wrapper + installed activation/rollback artifacts under {} against immutable package {}",
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&config.package_dir.display().to_string()),
    )
}

fn verify_service_status_command_summary(
    config: &Config,
    paths: &PackageLiveTransactionPaths,
) -> String {
    format!(
        "installed repo-managed wrapper status probe for service {} via backend {} using scratch runtime {} and status artifact {}",
        shell_single_quote(&config.target_service_name),
        shell_single_quote(&config.backend_command),
        shell_single_quote(&paths.service_status_runtime_dir.display().to_string()),
        shell_single_quote(&paths.service_status_artifact_path.display().to_string()),
    )
}

fn package_backup_command_summary(config: &Config) -> String {
    format!(
        "copybot_tiny_live_activation_live_execute --activation-config <installed-activation> --rollback-config <installed-rollback> --target-config <installed-target> --target-service {} --service-control-command <installed-wrapper> --runtime-dir <installed-runtime> --backup-dir <installed-backup> --backup-current-config",
        shell_single_quote(&config.target_service_name),
    )
}

fn dry_transaction_command_summary(
    config: &Config,
    target_config_path: &str,
    session_dir: &Path,
) -> String {
    format!(
        "bounded dry target-config transaction rehearsal for {} under install root {} with session evidence rooted at {}",
        shell_single_quote(target_config_path),
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&session_dir.display().to_string()),
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

fn step_artifact(
    path: &Path,
    mode: &str,
    verdict: &str,
    reason: &str,
    generated_at: DateTime<Utc>,
) -> PackageLiveTransactionStepArtifact {
    PackageLiveTransactionStepArtifact {
        path: path.display().to_string(),
        mode: mode.to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        generated_at,
    }
}

fn compare_package_deploy_view(
    report: &PackageDeployStoredView,
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

fn compare_install_target_report_view(
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
    if report.verdict == "tiny_live_install_target_verify_ok"
        && report
            .verification
            .as_ref()
            .is_some_and(|value| !value.mismatches.is_empty())
    {
        mismatches.push(format!(
            "{label} claims verify_ok but still carries verification mismatches"
        ));
    }
}

fn validate_wrapper_binding_report_contract(
    report: &WrapperBindingReport,
    config: &Config,
    package_contract: &tiny_live_activation_package::VerifiedActivationPackageContract,
    live_target_verify: &tiny_live_activation_install_target::InstallTargetVerifyStep,
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
        &live_target_verify.wrapper_path,
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
        &live_target_verify.installed_activation_config_path,
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
        &live_target_verify.installed_rollback_config_path,
        "wrapper verify installed_rollback_config_path",
        mismatches,
    );
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
    paths: &PackageLiveTransactionPaths,
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
    if report.max_service_status_staleness_ms != config.service_status_max_staleness_ms {
        mismatches.push(format!(
            "service status report max_service_status_staleness_ms {} does not match requested {}",
            report.max_service_status_staleness_ms, config.service_status_max_staleness_ms
        ));
    }
    let current_service_status_age_ms = age_ms(raw_status.observed_at);
    if current_service_status_age_ms < 0 {
        mismatches.push(format!(
            "service status artifact observed_at {} is in the future during verification",
            raw_status.observed_at
        ));
    } else if current_service_status_age_ms as u64 > config.service_status_max_staleness_ms {
        mismatches.push(format!(
            "service status artifact is stale during verification: observed age {}ms exceeds max {}ms",
            current_service_status_age_ms, config.service_status_max_staleness_ms
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

fn validate_backup_report_contract(
    report: &tiny_live_activation_live_execute::LiveExecuteReport,
    config: &Config,
    live_target_report: &InstallTargetReportView,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &report.activation_config_path,
        &live_target_report.installed_activation_config_path,
        "backup report activation_config_path",
        mismatches,
    );
    compare_string(
        &report.rollback_config_path,
        &live_target_report.installed_rollback_config_path,
        "backup report rollback_config_path",
        mismatches,
    );
    compare_string(
        &report.target_config_path,
        &live_target_report.target_config_path,
        "backup report target_config_path",
        mismatches,
    );
    compare_string(
        &report.target_service_name,
        &config.target_service_name,
        "backup report target_service_name",
        mismatches,
    );
    compare_string(
        &report.service_control_command_path,
        &live_target_report.wrapper_path,
        "backup report service_control_command_path",
        mismatches,
    );
    compare_string(
        &report.runtime_dir,
        &live_target_report.runtime_dir,
        "backup report runtime_dir",
        mismatches,
    );
    compare_string(
        &report.backup_dir,
        &live_target_report.backup_dir,
        "backup report backup_dir",
        mismatches,
    );
    if report.verdict
        == tiny_live_activation_live_execute::TinyLiveLiveExecuteVerdict::TinyLiveLiveBackupCreated
        && report
            .live_verification
            .as_ref()
            .is_some_and(|value| !value.mismatches.is_empty())
    {
        mismatches.push(
            "backup report claims backup_created but still carries live verification mismatches"
                .to_string(),
        );
    }
}

fn validate_dry_transaction_report_contract(
    report: &DryTransactionReport,
    config: &Config,
    session_dir: &Path,
    target_config_path: &str,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &report.package_dir,
        &config.package_dir.display().to_string(),
        "dry transaction package_dir",
        mismatches,
    );
    compare_string(
        &report.install_root,
        &config.install_root.display().to_string(),
        "dry transaction install_root",
        mismatches,
    );
    compare_string(
        &report.target_service_name,
        &config.target_service_name,
        "dry transaction target_service_name",
        mismatches,
    );
    compare_string(
        &report.backend_command,
        &config.backend_command,
        "dry transaction backend_command",
        mismatches,
    );
    compare_string(
        &report.session_dir,
        &session_dir.display().to_string(),
        "dry transaction session_dir",
        mismatches,
    );
    compare_string(
        &report.target_config_path,
        target_config_path,
        "dry transaction target_config_path",
        mismatches,
    );
    let target_parent = Path::new(target_config_path)
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_default();
    compare_string(
        &report.target_config_parent,
        &target_parent.display().to_string(),
        "dry transaction target_config_parent",
        mismatches,
    );
    for (label, path, expected_root) in [
        (
            "probe_source_path",
            Path::new(&report.probe_source_path),
            Path::new(&report.target_config_parent),
        ),
        (
            "probe_renamed_path",
            Path::new(&report.probe_renamed_path),
            Path::new(&report.target_config_parent),
        ),
    ] {
        if !path.starts_with(expected_root) {
            mismatches.push(format!(
                "dry transaction {label} {} is not bounded under {}",
                path.display(),
                expected_root.display()
            ));
        }
    }
    if report.target_config_sha256_before != report.target_config_sha256_after {
        mismatches.push(
            "dry transaction report target config sha256 changed across the rehearsal".to_string(),
        );
    }
    if report.verdict == "tiny_live_package_live_transaction_dry_transaction_ok"
        && (!report.target_config_bytes_unchanged
            || !report.source_write_ok
            || !report.rename_ok
            || !report.source_cleanup_ok
            || !report.renamed_cleanup_ok
            || !report.parent_sync_ok
            || !report.verification_mismatches.is_empty())
    {
        mismatches.push(
            "dry transaction report claims ok but still carries failed probe flags or mismatches"
                .to_string(),
        );
    }
}

fn live_target_missing_on_disk(
    live_target_verify: &tiny_live_activation_install_target::InstallTargetVerifyStep,
) -> bool {
    [
        &live_target_verify.wrapper_path,
        &live_target_verify.target_config_path,
        &live_target_verify.installed_activation_config_path,
        &live_target_verify.installed_rollback_config_path,
        &live_target_verify.runtime_dir,
        &live_target_verify.backup_dir,
        &live_target_verify.session_dir,
    ]
    .iter()
    .any(|path| !Path::new(path).exists())
}

fn load_required_step_json<T: for<'de> Deserialize<'de>>(
    step: &Option<PackageLiveTransactionStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<T> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("missing required live package transaction step {label}"))?;
    let expected = expected_path.display().to_string();
    if step.path != expected {
        mismatches.push(format!(
            "live package transaction {label} path {} does not match deterministic session artifact {}",
            step.path, expected
        ));
    }
    load_json(expected_path).with_context(|| {
        format!(
            "failed reading deterministic live package transaction {label} report {}",
            expected_path.display()
        )
    })
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
    let resolved_anchor = fs::canonicalize(anchor)
        .with_context(|| format!("failed canonicalizing {label} anchor {}", anchor.display()))?;
    let relative = path
        .strip_prefix(anchor)
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

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    value.ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn parse_u64_arg(flag: &str, value: Option<String>) -> Result<u64> {
    let raw = parse_string_arg(flag, value)?;
    raw.parse::<u64>()
        .with_context(|| format!("failed parsing {flag} as u64"))
}

fn set_mode(mode: &mut Option<Mode>, next: Mode, flag: &str) -> Result<()> {
    if mode.replace(next).is_some() {
        bail!("multiple modes provided; {flag} conflicts with earlier mode");
    }
    Ok(())
}

fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn serialize_enum<T: Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_else(|| "<unknown>".to_string())
}

fn render_human_report(report: &PackageLiveTransactionReport) -> String {
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
    if let Some(value) = &report.transaction_session_path {
        lines.push(format!("transaction_session_path={value}"));
    }
    if let Some(value) = &report.transaction_status_path {
        lines.push(format!("transaction_status_path={value}"));
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
    if let Some(value) = report.service_status_observed_at {
        lines.push(format!("service_status_observed_at={value}"));
    }
    if let Some(value) = report.service_status_age_ms {
        lines.push(format!("service_status_age_ms={value}"));
    }
    for (label, step) in [
        ("package_verify_step", &report.package_verify_step),
        ("live_target_verify_step", &report.live_target_verify_step),
        ("wrapper_verify_step", &report.wrapper_verify_step),
        ("service_status_step", &report.service_status_step),
        ("backup_proof_step", &report.backup_proof_step),
        ("dry_transaction_step", &report.dry_transaction_step),
        ("cutover_plan_step", &report.cutover_plan_step),
    ] {
        if let Some(step) = step {
            lines.push(format!("{label}_path={}", step.path));
            lines.push(format!("{label}_mode={}", step.mode));
            lines.push(format!("{label}_verdict={}", step.verdict));
            lines.push(format!("{label}_reason={}", step.reason));
        }
    }
    if !report.verification_mismatches.is_empty() {
        lines.push("verification_mismatches:".to_string());
        lines.extend(
            report
                .verification_mismatches
                .iter()
                .map(|entry| format!("  {entry}")),
        );
    }
    lines.push(format!(
        "activation_authorized={}",
        report.activation_authorized
    ));
    lines.push(format!(
        "package_verify_command_summary={}",
        report.package_verify_command_summary
    ));
    lines.push(format!(
        "verify_install_target_command_summary={}",
        report.verify_install_target_command_summary
    ));
    lines.push(format!(
        "verify_wrapper_binding_command_summary={}",
        report.verify_wrapper_binding_command_summary
    ));
    lines.push(format!(
        "verify_service_status_command_summary={}",
        report.verify_service_status_command_summary
    ));
    lines.push(format!(
        "backup_proof_command_summary={}",
        report.backup_proof_command_summary
    ));
    lines.push(format!(
        "dry_transaction_command_summary={}",
        report.dry_transaction_command_summary
    ));
    lines.push(format!(
        "package_cutover_plan_command_summary={}",
        report.package_cutover_plan_command_summary
    ));
    lines.push(format!("explicit_statement={}", report.explicit_statement));
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use copybot_storage::{
        DiscoveryWalletFreshnessCaptureWrite, ExecutionDryRunRehearsalWrite, SqliteStore,
    };
    use std::io::{Read, Write as IoWrite};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn coherent_live_target_contract_with_backup_and_dry_transaction_is_ready() {
        let fixture = transaction_fixture(
            "tiny_live_activation_package_live_transaction_ready",
            GateState::Green,
        );
        let report = run_json_report(fixture.config.clone());
        assert_eq!(
            report.verdict,
            TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionCompletedReadyForCutoverWhenGateAllows
        );
        assert_eq!(
            report.result.as_deref(),
            Some("ready_for_cutover_when_gate_allows")
        );
        let target_bytes = fs::read(&fixture.target_config_path).unwrap();
        let original_bytes = fs::read(&fixture.original_target_config_path).unwrap();
        assert_eq!(target_bytes, original_bytes);
    }

    #[test]
    fn backup_creation_failure_is_explicit() {
        let fixture = transaction_fixture(
            "tiny_live_activation_package_live_transaction_backup_failure",
            GateState::Green,
        );
        let package_contract = tiny_live_activation_package::verify_package_contract_for_deploy(
            &fixture.config.package_dir,
            &fixture.config.install_root,
            &fixture.config.target_service_name,
            &fixture.config.backend_command,
            fixture.config.wrapper_timeout_ms,
        )
        .unwrap();
        let installed_activation = tiny_live_activation_live_execute::tiny_live_activation_execute::inspect_rendered_config_artifact(
            Path::new(&package_contract.install_target_summary.installed_activation_config_path),
        )
        .unwrap();
        let (backup_config_path, _) =
            tiny_live_activation_live_execute::backup_artifact_paths_for_watch(
                Path::new(&package_contract.install_target_summary.target_config_path),
                &fixture.config.target_service_name,
                Path::new(&package_contract.install_target_summary.backup_dir),
                &installed_activation
                    .metadata
                    .source_config_fingerprint_sha256,
            );
        fs::create_dir_all(backup_config_path.parent().unwrap()).unwrap();
        fs::write(&backup_config_path, "preexisting backup").unwrap();

        let report = run_json_report(fixture.config.clone());
        assert_eq!(
            report.verdict,
            TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionCompletedBackupFailed
        );
        assert_eq!(report.result.as_deref(), Some("backup_failed"));
    }

    #[test]
    fn dry_transaction_cleanup_failure_is_explicit() {
        let fixture = transaction_fixture(
            "tiny_live_activation_package_live_transaction_dry_cleanup_failure",
            GateState::Green,
        );
        let report = run_live_package_transaction_report_with_hooks(
            &fixture.config,
            LiveTransactionRunHooks {
                before_dry_transaction_cleanup_hook: Some(&|artifacts| {
                    fs::create_dir_all(&artifacts.probe_renamed_path)?;
                    Ok(())
                }),
                ..LiveTransactionRunHooks::default()
            },
        )
        .unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionCompletedDryTransactionFailed
        );
    }

    #[cfg(unix)]
    #[test]
    fn tampered_installed_wrapper_is_explicit_non_green() {
        let fixture = transaction_fixture(
            "tiny_live_activation_package_live_transaction_tampered_wrapper",
            GateState::Green,
        );
        let package_contract = tiny_live_activation_package::verify_package_contract_for_deploy(
            &fixture.config.package_dir,
            &fixture.config.install_root,
            &fixture.config.target_service_name,
            &fixture.config.backend_command,
            fixture.config.wrapper_timeout_ms,
        )
        .unwrap();
        write_non_wrapper(Path::new(
            &package_contract.install_target_summary.wrapper_path,
        ));
        let report = run_json_report(fixture.config.clone());
        assert_eq!(
            report.verdict,
            TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionCompletedInstallTargetDrifted
        );
    }

    #[test]
    fn stale_service_status_evidence_is_explicit_non_green() {
        let fixture = transaction_fixture(
            "tiny_live_activation_package_live_transaction_stale_status",
            GateState::Green,
        );
        let report = run_live_package_transaction_report_with_hooks(
            &fixture.config,
            LiveTransactionRunHooks {
                after_service_status_hook: Some(&|status_path| {
                    let mut status: tiny_live_activation_live_execute::LiveServiceStatus =
                        load_json(status_path)?;
                    status.observed_at = Utc::now() - chrono::Duration::minutes(10);
                    persist_json(status_path, &status)?;
                    Ok(())
                }),
                ..LiveTransactionRunHooks::default()
            },
        )
        .unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionCompletedServiceProbeFailed
        );
    }

    #[test]
    fn verify_stays_green_when_service_status_age_snapshot_drifted_but_observed_at_is_fresh() {
        let fixture = transaction_fixture(
            "tiny_live_activation_package_live_transaction_verify_age_drift_green",
            GateState::Green,
        );
        let run_report = run_live_package_transaction_report_with_hooks(
            &fixture.config,
            LiveTransactionRunHooks {
                after_service_status_hook: Some(&|status_path| {
                    let mut status: tiny_live_activation_live_execute::LiveServiceStatus =
                        load_json(status_path)?;
                    status.observed_at = Utc::now() - chrono::Duration::seconds(15);
                    persist_json(status_path, &status)?;
                    Ok(())
                }),
                ..LiveTransactionRunHooks::default()
            },
        )
        .unwrap();
        assert_eq!(
            run_report.verdict,
            TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionCompletedReadyForCutoverWhenGateAllows
        );

        let paths = package_live_transaction_paths(
            fixture.config.session_dir.as_ref().expect("session dir"),
        );
        let mut stored_service_status: ServiceStatusReport =
            load_json(&paths.service_status_report_path).unwrap();
        stored_service_status.service_status_age_ms = Some(0);
        persist_json(&paths.service_status_report_path, &stored_service_status).unwrap();

        let verify = run_json_report(Config {
            mode: Mode::VerifyLivePackageTransaction,
            ..fixture.config.clone()
        });
        assert_eq!(
            verify.verdict,
            TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionVerifyOk,
            "{verify:#?}"
        );
    }

    #[test]
    fn verify_rejects_service_status_once_observed_at_is_truly_stale() {
        let fixture = transaction_fixture(
            "tiny_live_activation_package_live_transaction_verify_status_stale",
            GateState::Green,
        );
        let run_report = run_json_report(fixture.config.clone());
        assert_eq!(
            run_report.verdict,
            TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionCompletedReadyForCutoverWhenGateAllows
        );

        let paths = package_live_transaction_paths(
            fixture.config.session_dir.as_ref().expect("session dir"),
        );
        let stale_observed_at = Utc::now() - chrono::Duration::minutes(10);
        let mut raw_status: tiny_live_activation_live_execute::LiveServiceStatus =
            load_json(&paths.service_status_artifact_path).unwrap();
        raw_status.observed_at = stale_observed_at;
        persist_json(&paths.service_status_artifact_path, &raw_status).unwrap();

        let mut stored_service_status: ServiceStatusReport =
            load_json(&paths.service_status_report_path).unwrap();
        stored_service_status.service_status_observed_at = Some(stale_observed_at);
        stored_service_status.service_status_age_ms = Some(1);
        persist_json(&paths.service_status_report_path, &stored_service_status).unwrap();

        let verify = run_json_report(Config {
            mode: Mode::VerifyLivePackageTransaction,
            ..fixture.config
        });
        assert_eq!(
            verify.verdict,
            TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionVerifyInvalid
        );
        assert!(verify
            .verification_mismatches
            .iter()
            .any(|entry| entry.contains("service status artifact is stale during verification")));
    }

    #[test]
    fn cutover_blocked_by_stage3_is_explicit() {
        let fixture = transaction_fixture(
            "tiny_live_activation_package_live_transaction_gate_blocked",
            GateState::Stage3Blocked,
        );
        let report = run_json_report(fixture.config.clone());
        assert_eq!(
            report.verdict,
            TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionCompletedCutoverBlockedByGate
        );
    }

    #[test]
    fn verify_rejects_tampered_step_path_or_target_contract() {
        let fixture = transaction_fixture(
            "tiny_live_activation_package_live_transaction_verify_tampered",
            GateState::Green,
        );
        run_json_report(fixture.config.clone());
        let paths = package_live_transaction_paths(fixture.config.session_dir.as_ref().unwrap());
        let mut status: PackageLiveTransactionStatus = load_json(&paths.status_path).unwrap();
        let other = fixture.fixture_dir.join("other.report.json");
        persist_json(&other, &serde_json::json!({"fake": true})).unwrap();
        status.backup_proof_step.as_mut().unwrap().path = other.display().to_string();
        persist_json(&paths.status_path, &status).unwrap();
        let verify = run_json_report(Config {
            mode: Mode::VerifyLivePackageTransaction,
            ..fixture.config.clone()
        });
        assert_eq!(
            verify.verdict,
            TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionVerifyInvalid
        );

        let wrong_target_verify = run_json_report(Config {
            mode: Mode::VerifyLivePackageTransaction,
            target_service_name: "other.service".to_string(),
            ..fixture.config
        });
        assert_eq!(
            wrong_target_verify.verdict,
            TinyLivePackageLiveTransactionVerdict::TinyLivePackageLiveTransactionVerifyInvalid
        );
    }

    #[derive(Debug)]
    struct TransactionFixture {
        fixture_dir: PathBuf,
        target_config_path: PathBuf,
        original_target_config_path: PathBuf,
        config: Config,
        _rpc_server: MockHttpServer,
        _adapter_server: MockHttpServer,
    }

    #[derive(Debug, Clone, Copy)]
    enum GateState {
        Green,
        Stage3Blocked,
    }

    fn transaction_fixture(label: &str, gate_state: GateState) -> TransactionFixture {
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
        let install = tiny_live_activation_package_deploy::run_mode_for_rehearsal(
            &package_dir,
            &install_root,
            "solana-copy-bot.service",
            &backend_path.display().to_string(),
            tiny_live_activation_live_execute::live_service_control_wrapper_contract::DEFAULT_TIMEOUT_MS,
            tiny_live_activation_package_deploy::RehearsalMode::InstallFromPackage,
        )
        .unwrap();
        assert_eq!(
            install.verdict,
            "tiny_live_package_deploy_install_completed"
        );

        TransactionFixture {
            fixture_dir: fixture_dir.clone(),
            target_config_path,
            original_target_config_path,
            config: Config {
                mode: Mode::RunLivePackageTransaction,
                package_dir,
                install_root,
                target_service_name: "solana-copy-bot.service".to_string(),
                backend_command: backend_path.display().to_string(),
                wrapper_timeout_ms: tiny_live_activation_live_execute::live_service_control_wrapper_contract::DEFAULT_TIMEOUT_MS,
                service_status_max_staleness_ms:
                    tiny_live_activation_package_preflight::DEFAULT_SERVICE_STATUS_MAX_STALENESS_MS,
                session_dir: Some(fixture_dir.join("transaction-session")),
                output_path: None,
                json: false,
            },
            _rpc_server: rpc_server,
            _adapter_server: adapter_server,
        }
    }

    fn run_json_report(config: Config) -> PackageLiveTransactionReport {
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

    #[cfg(unix)]
    fn write_non_wrapper(path: &Path) {
        use std::os::unix::fs::PermissionsExt;
        fs::write(path, "#!/usr/bin/env bash\necho fake\n").unwrap();
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).unwrap();
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

    #[derive(Debug)]
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
        let header_text = String::from_utf8_lossy(&header_bytes[..header_end]);
        let headers = header_text
            .lines()
            .skip(1)
            .filter(|line| !line.trim().is_empty())
            .map(|line| line.trim().to_string())
            .collect();
        Some(ParsedHttpRequest { headers })
    }

    fn write_http_response(stream: &mut TcpStream, body: &str) {
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream
            .write_all(response.as_bytes())
            .expect("write response");
        stream.flush().expect("flush response");
    }
}
