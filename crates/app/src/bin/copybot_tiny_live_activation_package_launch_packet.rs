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
#[path = "copybot_tiny_live_activation_package_live_authorization.rs"]
mod tiny_live_activation_package_live_authorization;
#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_package_live_cutover.rs"]
mod tiny_live_activation_package_live_cutover;
#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_package_preflight.rs"]
mod tiny_live_activation_package_preflight;

const USAGE: &str = "usage: copybot_tiny_live_activation_package_launch_packet --package-dir <path> --install-root <path> --target-service <name> --backend-command <path-or-name> [--wrapper-timeout-ms <ms>] [--service-status-max-staleness-ms <ms>] [--session-dir <path>] [--output <path>] [--json] (--plan-live-package-launch-packet | --render-live-package-launch-packet | --run-live-package-launch-packet | --verify-live-package-launch-packet)";
const LAUNCH_PACKET_SESSION_VERSION: &str = "1";
const LAUNCH_PACKET_STATUS_VERSION: &str = "1";

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
    PlanLivePackageLaunchPacket,
    RenderLivePackageLaunchPacket,
    RunLivePackageLaunchPacket,
    VerifyLivePackageLaunchPacket,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageLaunchPacketVerdict {
    TinyLivePackageLaunchPacketPlanReady,
    TinyLivePackageLaunchPacketRendered,
    TinyLivePackageLaunchPacketRefusedByStage3,
    TinyLivePackageLaunchPacketRefusedByPreActivationGate,
    TinyLivePackageLaunchPacketRefusedByInvalidTarget,
    TinyLivePackageLaunchPacketEligibleWhenGateTurnsGreen,
    TinyLivePackageLaunchPacketVerifyOk,
    TinyLivePackageLaunchPacketVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LivePackageLaunchPacketResult {
    EligibleWhenGateTurnsGreen,
    RefusedByStage3,
    RefusedByPreActivationGate,
    RefusedByInvalidTarget,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageLaunchPacketStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageLaunchPacketSession {
    session_version: String,
    planned_at: DateTime<Utc>,
    package_dir: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    session_dir: String,
    nested_authorization_session_dir: String,
    run_live_authorization_command_summary: String,
    frozen_live_cutover_controller_command_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageLaunchPacketStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    session_dir: String,
    nested_authorization_session_dir: String,
    result: LivePackageLaunchPacketResult,
    reason: String,
    authorization_result_now: Option<String>,
    authorization_step: Option<PackageLaunchPacketStepArtifact>,
    controller_step: Option<PackageLaunchPacketStepArtifact>,
    operator_next_action_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackageLaunchPacketPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    authorization_report_path: PathBuf,
    controller_report_path: PathBuf,
    authorization_session_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageLaunchPacketReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageLaunchPacketVerdict,
    reason: String,
    package_dir: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    session_dir: Option<String>,
    launch_packet_session_path: Option<String>,
    launch_packet_status_path: Option<String>,
    nested_authorization_session_dir: Option<String>,
    result: Option<String>,
    authorization_result_now: Option<String>,
    authorization_step: Option<PackageLaunchPacketStepArtifact>,
    controller_step: Option<PackageLaunchPacketStepArtifact>,
    run_live_authorization_command_summary: String,
    frozen_live_cutover_controller_command_summary: String,
    operator_next_action_summary: String,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    verification_mismatches: Vec<String>,
    activation_authorized: bool,
    explicit_statement: String,
}

#[derive(Debug, Deserialize)]
struct StoredAuthorizationReportView {
    package_dir: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    session_dir: Option<String>,
    result: Option<String>,
    reason: String,
    gate_evaluation_command_summary: String,
    authorized_live_cutover_command_summary: String,
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
            "--plan-live-package-launch-packet" => set_mode(
                &mut mode,
                Mode::PlanLivePackageLaunchPacket,
                "--plan-live-package-launch-packet",
            )?,
            "--render-live-package-launch-packet" => set_mode(
                &mut mode,
                Mode::RenderLivePackageLaunchPacket,
                "--render-live-package-launch-packet",
            )?,
            "--run-live-package-launch-packet" => set_mode(
                &mut mode,
                Mode::RunLivePackageLaunchPacket,
                "--run-live-package-launch-packet",
            )?,
            "--verify-live-package-launch-packet" => set_mode(
                &mut mode,
                Mode::VerifyLivePackageLaunchPacket,
                "--verify-live-package-launch-packet",
            )?,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    let Some(mode) = mode else {
        return Ok(None);
    };
    let package_dir = package_dir.ok_or_else(|| anyhow!("missing required --package-dir"))?;
    let install_root = install_root.ok_or_else(|| anyhow!("missing required --install-root"))?;
    let target_service_name =
        target_service_name.ok_or_else(|| anyhow!("missing required --target-service"))?;
    let backend_command =
        backend_command.ok_or_else(|| anyhow!("missing required --backend-command"))?;
    if matches!(mode, Mode::RenderLivePackageLaunchPacket) && output_path.is_none() {
        bail!("missing required --output for --render-live-package-launch-packet");
    }
    if matches!(
        mode,
        Mode::RunLivePackageLaunchPacket | Mode::VerifyLivePackageLaunchPacket
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

fn set_mode(mode: &mut Option<Mode>, value: Mode, flag: &str) -> Result<()> {
    if mode.replace(value).is_some() {
        bail!("multiple modes specified including {flag}");
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
        Mode::PlanLivePackageLaunchPacket => match plan_live_package_launch_packet_report(&config)
        {
            Ok(report) => report,
            Err(error) => refusal_report(
                &config,
                TinyLivePackageLaunchPacketVerdict::TinyLivePackageLaunchPacketRefusedByInvalidTarget,
                error.to_string(),
            ),
        },
        Mode::RenderLivePackageLaunchPacket => match render_live_package_launch_packet_report(&config)
        {
            Ok(report) => report,
            Err(error) => refusal_report(
                &config,
                TinyLivePackageLaunchPacketVerdict::TinyLivePackageLaunchPacketRefusedByInvalidTarget,
                error.to_string(),
            ),
        },
        Mode::RunLivePackageLaunchPacket => match run_live_package_launch_packet_report(&config) {
            Ok(report) => report,
            Err(error) => refusal_report(
                &config,
                TinyLivePackageLaunchPacketVerdict::TinyLivePackageLaunchPacketRefusedByInvalidTarget,
                error.to_string(),
            ),
        },
        Mode::VerifyLivePackageLaunchPacket => {
            match verify_live_package_launch_packet_report(&config) {
                Ok(report) => report,
                Err(error) => refusal_report(
                    &config,
                    TinyLivePackageLaunchPacketVerdict::TinyLivePackageLaunchPacketVerifyInvalid,
                    error.to_string(),
                ),
            }
        }
    };
    if config.json {
        serde_json::to_string_pretty(&report)
            .context("failed to serialize live package launch packet report")
    } else {
        Ok(render_report_lines(&report))
    }
}

fn plan_live_package_launch_packet_report(config: &Config) -> Result<PackageLaunchPacketReport> {
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
        TinyLivePackageLaunchPacketVerdict::TinyLivePackageLaunchPacketPlanReady,
        format!(
            "package-native live launch packet is explicit for install root {}; today it still serializes the current authorization truth and remains non-authorizing by itself",
            config.install_root.display()
        ),
        &controller_view,
    ))
}

fn render_live_package_launch_packet_report(config: &Config) -> Result<PackageLaunchPacketReport> {
    let plan = plan_live_package_launch_packet_report(config)?;
    let output_path = config
        .output_path
        .as_ref()
        .ok_or_else(|| anyhow!("missing --output for --render-live-package-launch-packet"))?;
    ensure_new_output_path(output_path)?;
    let session_dir = config
        .session_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("<session-dir>"));
    let script = format!(
        "#!/usr/bin/env bash\nset -euo pipefail\n\ncopybot_tiny_live_activation_package_launch_packet --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --service-status-max-staleness-ms {} --plan-live-package-launch-packet\ncopybot_tiny_live_activation_package_launch_packet --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --service-status-max-staleness-ms {} --session-dir {} --run-live-package-launch-packet\ncopybot_tiny_live_activation_package_launch_packet --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --service-status-max-staleness-ms {} --session-dir {} --verify-live-package-launch-packet\n",
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
    Ok(PackageLaunchPacketReport {
        verdict: TinyLivePackageLaunchPacketVerdict::TinyLivePackageLaunchPacketRendered,
        reason: format!(
            "rendered package-native live launch packet script to {}",
            output_path.display()
        ),
        ..plan
    })
}

fn run_live_package_launch_packet_report(config: &Config) -> Result<PackageLaunchPacketReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for --run-live-package-launch-packet"))?;
    ensure_clean_live_package_launch_packet_session_dir(session_dir)?;
    let paths = package_live_launch_packet_paths(session_dir);
    let controller_view = controller_plan_view(config);
    let session = PackageLaunchPacketSession {
        session_version: LAUNCH_PACKET_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        package_dir: config.package_dir.display().to_string(),
        install_root: config.install_root.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        wrapper_timeout_ms: config.wrapper_timeout_ms,
        service_status_max_staleness_ms: config.service_status_max_staleness_ms,
        session_dir: session_dir.display().to_string(),
        nested_authorization_session_dir: paths.authorization_session_dir.display().to_string(),
        run_live_authorization_command_summary: run_live_authorization_command_summary(
            config,
            &paths.authorization_session_dir,
        ),
        frozen_live_cutover_controller_command_summary: controller_view
            .run_live_cutover_command_summary
            .clone(),
        explicit_statement:
            "this package-native live launch packet freezes one immutable package, one explicit live target contract, one operator-facing authorization truth, and one final live cutover controller into one handoff artifact only; it does not execute live cutover by itself"
                .to_string(),
    };
    persist_json(&paths.session_path, &session)?;
    persist_json(&paths.controller_report_path, &controller_view)?;
    let controller_step = Some(step_artifact(
        &paths.controller_report_path,
        "plan_live_cutover_controller",
        &controller_view.verdict,
        &controller_view.reason,
        Utc::now(),
    ));

    let authorization =
        tiny_live_activation_package_live_authorization::run_live_package_authorization_for_launch_packet(
            &config.package_dir,
            &config.install_root,
            &config.target_service_name,
            &config.backend_command,
            config.wrapper_timeout_ms,
            config.service_status_max_staleness_ms,
            &paths.authorization_session_dir,
        )?;
    persist_json_value(&paths.authorization_report_path, &authorization.report_json)?;
    let authorization_step = Some(step_artifact(
        &paths.authorization_report_path,
        "run_live_authorization",
        &authorization.verdict,
        &authorization.reason,
        authorization.generated_at,
    ));
    let stored_authorization: StoredAuthorizationReportView =
        serde_json::from_value(authorization.report_json.clone())
            .context("failed parsing nested launch-packet authorization report json")?;

    let authorization_matches_controller =
        authorization_report_matches_controller(&stored_authorization, &controller_view);
    let (
        result,
        verdict,
        reason,
        current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason,
        activation_authorized,
    ) = if controller_view.verdict != "tiny_live_package_live_cutover_plan_ready" {
        (
            LivePackageLaunchPacketResult::RefusedByInvalidTarget,
            TinyLivePackageLaunchPacketVerdict::TinyLivePackageLaunchPacketRefusedByInvalidTarget,
            controller_view.reason.clone(),
            None,
            None,
            false,
        )
    } else if !authorization_matches_controller {
        (
            LivePackageLaunchPacketResult::RefusedByInvalidTarget,
            TinyLivePackageLaunchPacketVerdict::TinyLivePackageLaunchPacketRefusedByInvalidTarget,
            "nested live authorization contract did not freeze the same final live cutover controller command summary"
                .to_string(),
            stored_authorization.current_pre_activation_gate_verdict.clone(),
            stored_authorization.current_pre_activation_gate_reason.clone(),
            false,
        )
    } else {
        match stored_authorization.result.as_deref() {
            Some("authorized_now") if authorization_proves_eligible_now(&authorization) => (
                LivePackageLaunchPacketResult::EligibleWhenGateTurnsGreen,
                TinyLivePackageLaunchPacketVerdict::TinyLivePackageLaunchPacketEligibleWhenGateTurnsGreen,
                "launch packet froze exact package/live target/controller truth and the current authorization chain is green".to_string(),
                stored_authorization.current_pre_activation_gate_verdict.clone(),
                stored_authorization.current_pre_activation_gate_reason.clone(),
                true,
            ),
            Some("authorized_now") => (
                LivePackageLaunchPacketResult::RefusedByInvalidTarget,
                TinyLivePackageLaunchPacketVerdict::TinyLivePackageLaunchPacketRefusedByInvalidTarget,
                "nested live authorization reported authorized_now without verified green authorization truth".to_string(),
                stored_authorization.current_pre_activation_gate_verdict.clone(),
                stored_authorization.current_pre_activation_gate_reason.clone(),
                false,
            ),
            Some("refused_by_stage3") => (
                LivePackageLaunchPacketResult::RefusedByStage3,
                TinyLivePackageLaunchPacketVerdict::TinyLivePackageLaunchPacketRefusedByStage3,
                stored_authorization.reason.clone(),
                stored_authorization.current_pre_activation_gate_verdict.clone(),
                stored_authorization.current_pre_activation_gate_reason.clone(),
                false,
            ),
            Some("refused_by_pre_activation_gate") => (
                LivePackageLaunchPacketResult::RefusedByPreActivationGate,
                TinyLivePackageLaunchPacketVerdict::TinyLivePackageLaunchPacketRefusedByPreActivationGate,
                stored_authorization.reason.clone(),
                stored_authorization.current_pre_activation_gate_verdict.clone(),
                stored_authorization.current_pre_activation_gate_reason.clone(),
                false,
            ),
            _ => (
                LivePackageLaunchPacketResult::RefusedByInvalidTarget,
                TinyLivePackageLaunchPacketVerdict::TinyLivePackageLaunchPacketRefusedByInvalidTarget,
                stored_authorization.reason.clone(),
                stored_authorization.current_pre_activation_gate_verdict.clone(),
                stored_authorization.current_pre_activation_gate_reason.clone(),
                false,
            ),
        }
    };
    let operator_next_action_summary =
        operator_next_action_summary(&controller_view, result, activation_authorized);
    let status = PackageLaunchPacketStatus {
        status_version: LAUNCH_PACKET_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        session_dir: session_dir.display().to_string(),
        nested_authorization_session_dir: paths.authorization_session_dir.display().to_string(),
        result,
        reason,
        authorization_result_now: stored_authorization.result.clone(),
        authorization_step,
        controller_step,
        operator_next_action_summary,
        explicit_statement:
            "this package-native live launch packet is an immutable operator handoff artifact only; current Stage 3 / pre-activation truth still remains the hard live authorization boundary"
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

fn verify_live_package_launch_packet_report(config: &Config) -> Result<PackageLaunchPacketReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for --verify-live-package-launch-packet"))?;
    let paths = package_live_launch_packet_paths(session_dir);
    let session: PackageLaunchPacketSession = load_json(&paths.session_path)?;
    let status: PackageLaunchPacketStatus = load_json(&paths.status_path)?;
    let expected_controller_view = controller_plan_view(config);
    let mut mismatches = Vec::new();

    compare_string(
        &session.package_dir,
        &config.package_dir.display().to_string(),
        "package launch packet package_dir",
        &mut mismatches,
    );
    compare_string(
        &session.install_root,
        &config.install_root.display().to_string(),
        "package launch packet install_root",
        &mut mismatches,
    );
    compare_string(
        &session.target_service_name,
        &config.target_service_name,
        "package launch packet target_service_name",
        &mut mismatches,
    );
    compare_string(
        &session.backend_command,
        &config.backend_command,
        "package launch packet backend_command",
        &mut mismatches,
    );
    if session.wrapper_timeout_ms != config.wrapper_timeout_ms {
        mismatches.push(format!(
            "package launch packet wrapper_timeout_ms {} does not match requested {}",
            session.wrapper_timeout_ms, config.wrapper_timeout_ms
        ));
    }
    if session.service_status_max_staleness_ms != config.service_status_max_staleness_ms {
        mismatches.push(format!(
            "package launch packet service_status_max_staleness_ms {} does not match requested {}",
            session.service_status_max_staleness_ms, config.service_status_max_staleness_ms
        ));
    }
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "package launch packet session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.nested_authorization_session_dir,
        &paths.authorization_session_dir.display().to_string(),
        "package launch packet nested_authorization_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.session_dir,
        &session_dir.display().to_string(),
        "package launch packet status session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.nested_authorization_session_dir,
        &paths.authorization_session_dir.display().to_string(),
        "package launch packet status nested_authorization_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.run_live_authorization_command_summary,
        &run_live_authorization_command_summary(config, &paths.authorization_session_dir),
        "package launch packet run_live_authorization_command_summary",
        &mut mismatches,
    );
    compare_string(
        &session.frozen_live_cutover_controller_command_summary,
        &expected_controller_view.run_live_cutover_command_summary,
        "package launch packet frozen_live_cutover_controller_command_summary",
        &mut mismatches,
    );

    let stored_controller_view: tiny_live_activation_package_live_cutover::LiveCutoverControllerPlanView =
        load_required_step_json(
            &status.controller_step,
            &paths.controller_report_path,
            "controller_step",
            &mut mismatches,
        )?;
    validate_controller_plan_view_contract(
        &stored_controller_view,
        &expected_controller_view,
        &mut mismatches,
    );

    let stored_authorization: StoredAuthorizationReportView = load_required_step_json(
        &status.authorization_step,
        &paths.authorization_report_path,
        "authorization_step",
        &mut mismatches,
    )?;
    validate_authorization_report_contract(
        &stored_authorization,
        config,
        &paths,
        &expected_controller_view,
        &mut mismatches,
    );
    let verified_authorization =
        tiny_live_activation_package_live_authorization::verify_live_package_authorization_for_launch_packet(
            &config.package_dir,
            &config.install_root,
            &config.target_service_name,
            &config.backend_command,
            config.wrapper_timeout_ms,
            config.service_status_max_staleness_ms,
            &paths.authorization_session_dir,
        )?;
    if verified_authorization.verdict != "tiny_live_package_live_authorization_verify_ok" {
        mismatches.push(format!(
            "nested live authorization verification is non-green: {}",
            verified_authorization.reason
        ));
    }
    if stored_authorization.result != verified_authorization.result {
        mismatches.push(format!(
            "stored authorization result {:?} does not match verified nested authorization result {:?}",
            stored_authorization.result, verified_authorization.result
        ));
    }
    if stored_authorization.activation_authorized != verified_authorization.activation_authorized {
        mismatches.push(format!(
            "stored authorization activation_authorized {} does not match verified nested authorization activation_authorized {}",
            stored_authorization.activation_authorized,
            verified_authorization.activation_authorized
        ));
    }

    let expected_result = match verified_authorization.result.as_deref() {
        Some("authorized_now")
            if expected_controller_view.verdict == "tiny_live_package_live_cutover_plan_ready"
                && authorization_proves_eligible_now(&verified_authorization) =>
        {
            Some(LivePackageLaunchPacketResult::EligibleWhenGateTurnsGreen)
        }
        Some("refused_by_stage3")
            if expected_controller_view.verdict == "tiny_live_package_live_cutover_plan_ready" =>
        {
            Some(LivePackageLaunchPacketResult::RefusedByStage3)
        }
        Some("refused_by_pre_activation_gate")
            if expected_controller_view.verdict == "tiny_live_package_live_cutover_plan_ready" =>
        {
            Some(LivePackageLaunchPacketResult::RefusedByPreActivationGate)
        }
        Some("refused_by_invalid_target") => {
            Some(LivePackageLaunchPacketResult::RefusedByInvalidTarget)
        }
        _ if expected_controller_view.verdict != "tiny_live_package_live_cutover_plan_ready" => {
            Some(LivePackageLaunchPacketResult::RefusedByInvalidTarget)
        }
        _ => None,
    };
    match (status.result, expected_result) {
        (actual, Some(expected)) if actual != expected => mismatches.push(format!(
            "package launch packet result {} does not match verified nested authorization/controller truth {}",
            serialize_enum(&actual),
            serialize_enum(&expected)
        )),
        (_, None) => mismatches.push(
            "package launch packet could not derive a coherent result from the verified authorization/controller chain"
                .to_string(),
        ),
        _ => {}
    }
    if status.authorization_result_now != verified_authorization.result {
        mismatches.push(format!(
            "package launch packet status authorization_result_now {:?} does not match verified nested authorization result {:?}",
            status.authorization_result_now, verified_authorization.result
        ));
    }
    let expected_operator_next_action_summary = operator_next_action_summary(
        &expected_controller_view,
        status.result,
        status.result == LivePackageLaunchPacketResult::EligibleWhenGateTurnsGreen
            && authorization_proves_eligible_now(&verified_authorization),
    );
    compare_string(
        &status.operator_next_action_summary,
        &expected_operator_next_action_summary,
        "package launch packet operator_next_action_summary",
        &mut mismatches,
    );

    let activation_authorized = mismatches.is_empty()
        && status.result == LivePackageLaunchPacketResult::EligibleWhenGateTurnsGreen
        && authorization_proves_eligible_now(&verified_authorization);
    let verdict = if mismatches.is_empty() {
        TinyLivePackageLaunchPacketVerdict::TinyLivePackageLaunchPacketVerifyOk
    } else {
        TinyLivePackageLaunchPacketVerdict::TinyLivePackageLaunchPacketVerifyInvalid
    };
    let reason = if mismatches.is_empty() {
        format!(
            "package-native live launch packet session under {} is coherent and frozen to the requested package/live target/authorization/controller contract",
            session_dir.display()
        )
    } else {
        mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "package-native live launch packet verification failed".to_string())
    };
    Ok(PackageLaunchPacketReport {
        generated_at: Utc::now(),
        mode: "verify_live_package_launch_packet".to_string(),
        verdict,
        reason,
        package_dir: config.package_dir.display().to_string(),
        install_root: config.install_root.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        wrapper_timeout_ms: config.wrapper_timeout_ms,
        service_status_max_staleness_ms: config.service_status_max_staleness_ms,
        session_dir: Some(session_dir.display().to_string()),
        launch_packet_session_path: Some(paths.session_path.display().to_string()),
        launch_packet_status_path: Some(paths.status_path.display().to_string()),
        nested_authorization_session_dir: Some(paths.authorization_session_dir.display().to_string()),
        result: Some(serialize_enum(&status.result)),
        authorization_result_now: verified_authorization.result.clone(),
        authorization_step: status.authorization_step,
        controller_step: status.controller_step,
        run_live_authorization_command_summary: run_live_authorization_command_summary(
            config,
            &paths.authorization_session_dir,
        ),
        frozen_live_cutover_controller_command_summary: expected_controller_view
            .run_live_cutover_command_summary,
        operator_next_action_summary: status.operator_next_action_summary,
        current_pre_activation_gate_verdict: verified_authorization
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: verified_authorization
            .current_pre_activation_gate_reason
            .clone(),
        verification_mismatches: mismatches,
        activation_authorized,
        explicit_statement:
            "this package-native live launch packet verification checks only bounded package/authorization/controller evidence; it still does not execute live cutover by itself"
                .to_string(),
    })
}

fn base_report(
    config: &Config,
    verdict: TinyLivePackageLaunchPacketVerdict,
    reason: String,
    controller_view: &tiny_live_activation_package_live_cutover::LiveCutoverControllerPlanView,
) -> PackageLaunchPacketReport {
    let session_dir = config.session_dir.as_ref();
    let paths = session_dir.map(|path| package_live_launch_packet_paths(path));
    PackageLaunchPacketReport {
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
        launch_packet_session_path: paths
            .as_ref()
            .map(|value| value.session_path.display().to_string()),
        launch_packet_status_path: paths
            .as_ref()
            .map(|value| value.status_path.display().to_string()),
        nested_authorization_session_dir: paths
            .as_ref()
            .map(|value| value.authorization_session_dir.display().to_string()),
        result: None,
        authorization_result_now: None,
        authorization_step: None,
        controller_step: None,
        run_live_authorization_command_summary: run_live_authorization_command_summary(
            config,
            &paths
                .as_ref()
                .map(|value| value.authorization_session_dir.clone())
                .unwrap_or_else(|| {
                    PathBuf::from(
                        "<session-dir>/tiny_live_activation_package_launch_packet.authorization_session",
                    )
                }),
        ),
        frozen_live_cutover_controller_command_summary: controller_view
            .run_live_cutover_command_summary
            .clone(),
        operator_next_action_summary:
            "inspect the frozen launch packet, then wait for current Stage 3 / pre-activation truth to become genuinely green before considering any real live cutover invocation"
                .to_string(),
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        verification_mismatches: Vec::new(),
        activation_authorized: false,
        explicit_statement:
            "this package-native live launch packet is an immutable operator handoff artifact only; it freezes authorization/controller truth without running live cutover"
                .to_string(),
    }
}

fn refusal_report(
    config: &Config,
    verdict: TinyLivePackageLaunchPacketVerdict,
    reason: String,
) -> PackageLaunchPacketReport {
    let controller_view = controller_plan_view(config);
    base_report(config, verdict, reason, &controller_view)
}

fn report_from_status(
    config: &Config,
    session_dir: &Path,
    paths: &PackageLaunchPacketPaths,
    verdict: TinyLivePackageLaunchPacketVerdict,
    status: &PackageLaunchPacketStatus,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    activation_authorized: bool,
) -> PackageLaunchPacketReport {
    let controller_view = controller_plan_view(config);
    PackageLaunchPacketReport {
        generated_at: Utc::now(),
        mode: "run_live_package_launch_packet".to_string(),
        verdict,
        reason: status.reason.clone(),
        package_dir: config.package_dir.display().to_string(),
        install_root: config.install_root.display().to_string(),
        target_service_name: config.target_service_name.clone(),
        backend_command: config.backend_command.clone(),
        wrapper_timeout_ms: config.wrapper_timeout_ms,
        service_status_max_staleness_ms: config.service_status_max_staleness_ms,
        session_dir: Some(session_dir.display().to_string()),
        launch_packet_session_path: Some(paths.session_path.display().to_string()),
        launch_packet_status_path: Some(paths.status_path.display().to_string()),
        nested_authorization_session_dir: Some(paths.authorization_session_dir.display().to_string()),
        result: Some(serialize_enum(&status.result)),
        authorization_result_now: status.authorization_result_now.clone(),
        authorization_step: status.authorization_step.clone(),
        controller_step: status.controller_step.clone(),
        run_live_authorization_command_summary: run_live_authorization_command_summary(
            config,
            &paths.authorization_session_dir,
        ),
        frozen_live_cutover_controller_command_summary: controller_view
            .run_live_cutover_command_summary,
        operator_next_action_summary: status.operator_next_action_summary.clone(),
        current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason,
        verification_mismatches: Vec::new(),
        activation_authorized,
        explicit_statement:
            "this package-native live launch packet freezes the exact current authorization/refusal truth together with the final live cutover controller contract; it does not launch live cutover by itself"
                .to_string(),
    }
}

fn mode_name(mode: Mode) -> &'static str {
    match mode {
        Mode::PlanLivePackageLaunchPacket => "plan_live_package_launch_packet",
        Mode::RenderLivePackageLaunchPacket => "render_live_package_launch_packet",
        Mode::RunLivePackageLaunchPacket => "run_live_package_launch_packet",
        Mode::VerifyLivePackageLaunchPacket => "verify_live_package_launch_packet",
    }
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

fn run_live_authorization_command_summary(
    config: &Config,
    authorization_session_dir: &Path,
) -> String {
    format!(
        "copybot_tiny_live_activation_package_live_authorization --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --service-status-max-staleness-ms {} --session-dir {} --run-live-package-authorization",
        shell_single_quote(&config.package_dir.display().to_string()),
        shell_single_quote(&config.install_root.display().to_string()),
        shell_single_quote(&config.target_service_name),
        shell_single_quote(&config.backend_command),
        config.wrapper_timeout_ms,
        config.service_status_max_staleness_ms,
        shell_single_quote(&authorization_session_dir.display().to_string()),
    )
}

fn operator_next_action_summary(
    controller_view: &tiny_live_activation_package_live_cutover::LiveCutoverControllerPlanView,
    result: LivePackageLaunchPacketResult,
    activation_authorized: bool,
) -> String {
    match result {
        LivePackageLaunchPacketResult::EligibleWhenGateTurnsGreen if activation_authorized => format!(
            "current authorization truth is green; the exact bounded next step remains {}. This launch packet itself still does not execute live cutover.",
            controller_view.run_live_cutover_command_summary
        ),
        LivePackageLaunchPacketResult::RefusedByStage3 => format!(
            "today refused by current Stage 3 truth; do not run live cutover now. Once the current Stage 3 / pre-activation truth genuinely turns green, the frozen bounded controller command remains {}",
            controller_view.run_live_cutover_command_summary
        ),
        LivePackageLaunchPacketResult::RefusedByPreActivationGate => format!(
            "today refused by the current pre-activation gate; do not run live cutover now. Once the current Stage 3 / pre-activation truth genuinely turns green, the frozen bounded controller command remains {}",
            controller_view.run_live_cutover_command_summary
        ),
        LivePackageLaunchPacketResult::RefusedByInvalidTarget => {
            if controller_view.verdict == "tiny_live_package_live_cutover_plan_ready" {
                format!(
                    "the live target/package/controller contract is non-green today; repair that contract before any launch. The frozen bounded controller command remains {}",
                    controller_view.run_live_cutover_command_summary
                )
            } else {
                "the final live cutover controller contract is not plan-ready today; repair package/live target/controller truth before any launch packet can become eligible".to_string()
            }
        }
        LivePackageLaunchPacketResult::EligibleWhenGateTurnsGreen => format!(
            "current nested authorization did not prove genuine green truth; do not run live cutover now. The frozen bounded controller command remains {}",
            controller_view.run_live_cutover_command_summary
        ),
    }
}

fn package_live_launch_packet_paths(session_dir: &Path) -> PackageLaunchPacketPaths {
    PackageLaunchPacketPaths {
        session_path: session_dir.join("tiny_live_activation_package_launch_packet.session.json"),
        status_path: session_dir.join("tiny_live_activation_package_launch_packet.status.json"),
        authorization_report_path: session_dir
            .join("tiny_live_activation_package_launch_packet.authorization.report.json"),
        controller_report_path: session_dir
            .join("tiny_live_activation_package_launch_packet.controller.report.json"),
        authorization_session_dir: session_dir
            .join("tiny_live_activation_package_launch_packet.authorization_session"),
    }
}

fn ensure_clean_live_package_launch_packet_session_dir(session_dir: &Path) -> Result<()> {
    let paths = package_live_launch_packet_paths(session_dir);
    for path in [
        &paths.session_path,
        &paths.status_path,
        &paths.authorization_report_path,
        &paths.controller_report_path,
    ] {
        if path.exists() {
            bail!(
                "refusing to reuse package live launch packet session dir {}; artifact {} already exists",
                session_dir.display(),
                path.display()
            );
        }
    }
    if paths.authorization_session_dir.exists() {
        bail!(
            "refusing to reuse package live launch packet session dir {}; artifact {} already exists",
            session_dir.display(),
            paths.authorization_session_dir.display()
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
    step: &Option<PackageLaunchPacketStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<T> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("missing required package launch packet step {label}"))?;
    load_bound_step_json(step, expected_path, label, mismatches)
}

fn load_bound_step_json<T: for<'de> Deserialize<'de>>(
    step: &PackageLaunchPacketStepArtifact,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<T> {
    let expected = expected_path.display().to_string();
    if step.path != expected {
        mismatches.push(format!(
            "package launch packet {label} path {} does not match deterministic session artifact {}",
            step.path, expected
        ));
    }
    load_json(expected_path).with_context(|| {
        format!(
            "failed reading deterministic package launch packet {label} report {}",
            expected_path.display()
        )
    })
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

fn step_artifact(
    path: &Path,
    mode: &str,
    verdict: &str,
    reason: &str,
    generated_at: DateTime<Utc>,
) -> PackageLaunchPacketStepArtifact {
    PackageLaunchPacketStepArtifact {
        path: path.display().to_string(),
        mode: mode.to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        generated_at,
    }
}

fn authorization_report_matches_controller(
    authorization: &StoredAuthorizationReportView,
    controller_view: &tiny_live_activation_package_live_cutover::LiveCutoverControllerPlanView,
) -> bool {
    authorization.authorized_live_cutover_command_summary
        == controller_view.run_live_cutover_command_summary
        && authorization.gate_evaluation_command_summary
            == controller_view.gate_evaluation_command_summary
}

fn authorization_proves_eligible_now(
    authorization: &tiny_live_activation_package_live_authorization::PackageLiveAuthorizationLaunchPacketStep,
) -> bool {
    authorization.result.as_deref() == Some("authorized_now") && authorization.activation_authorized
}

fn validate_authorization_report_contract(
    report: &StoredAuthorizationReportView,
    config: &Config,
    paths: &PackageLaunchPacketPaths,
    expected_controller_view: &tiny_live_activation_package_live_cutover::LiveCutoverControllerPlanView,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &report.package_dir,
        &config.package_dir.display().to_string(),
        "stored authorization package_dir",
        mismatches,
    );
    compare_string(
        &report.install_root,
        &config.install_root.display().to_string(),
        "stored authorization install_root",
        mismatches,
    );
    compare_string(
        &report.target_service_name,
        &config.target_service_name,
        "stored authorization target_service_name",
        mismatches,
    );
    compare_string(
        &report.backend_command,
        &config.backend_command,
        "stored authorization backend_command",
        mismatches,
    );
    if report.wrapper_timeout_ms != config.wrapper_timeout_ms {
        mismatches.push(format!(
            "stored authorization wrapper_timeout_ms {} does not match requested {}",
            report.wrapper_timeout_ms, config.wrapper_timeout_ms
        ));
    }
    if report.service_status_max_staleness_ms != config.service_status_max_staleness_ms {
        mismatches.push(format!(
            "stored authorization service_status_max_staleness_ms {} does not match requested {}",
            report.service_status_max_staleness_ms, config.service_status_max_staleness_ms
        ));
    }
    compare_string(
        report.session_dir.as_deref().unwrap_or_default(),
        &paths.authorization_session_dir.display().to_string(),
        "stored authorization session_dir",
        mismatches,
    );
    compare_string(
        &report.gate_evaluation_command_summary,
        &expected_controller_view.gate_evaluation_command_summary,
        "stored authorization gate_evaluation_command_summary",
        mismatches,
    );
    compare_string(
        &report.authorized_live_cutover_command_summary,
        &expected_controller_view.run_live_cutover_command_summary,
        "stored authorization authorized_live_cutover_command_summary",
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

fn render_report_lines(report: &PackageLaunchPacketReport) -> String {
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
    if let Some(value) = &report.launch_packet_session_path {
        lines.push(format!("launch_packet_session_path={value}"));
    }
    if let Some(value) = &report.launch_packet_status_path {
        lines.push(format!("launch_packet_status_path={value}"));
    }
    if let Some(value) = &report.nested_authorization_session_dir {
        lines.push(format!("nested_authorization_session_dir={value}"));
    }
    if let Some(value) = &report.result {
        lines.push(format!("result={value}"));
    }
    if let Some(value) = &report.authorization_result_now {
        lines.push(format!("authorization_result_now={value}"));
    }
    lines.push(format!(
        "run_live_authorization_command_summary={}",
        report.run_live_authorization_command_summary
    ));
    lines.push(format!(
        "frozen_live_cutover_controller_command_summary={}",
        report.frozen_live_cutover_controller_command_summary
    ));
    lines.push(format!(
        "operator_next_action_summary={}",
        report.operator_next_action_summary
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
    lines.push(format!("explicit_statement={}", report.explicit_statement));
    if !report.verification_mismatches.is_empty() {
        lines.push("verification_mismatches:".to_string());
        for mismatch in &report.verification_mismatches {
            lines.push(format!("  - {mismatch}"));
        }
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tiny_live_activation_live_execute::tiny_live_activation_execute::RenderedConfigMetadata;
    use chrono::Duration;
    use copybot_config::load_from_path;
    use copybot_storage::{
        DiscoveryWalletFreshnessCaptureWrite, ExecutionDryRunRehearsalWrite, SqliteStore,
    };
    use sha2::{Digest, Sha256};
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener};
    use std::sync::Arc;

    #[test]
    fn green_gate_yields_eligible_launch_packet() {
        let fixture = launch_packet_fixture(
            "tiny_live_activation_package_launch_packet_green",
            GateState::Green,
        );
        let report = run_json_report(fixture.config.clone());
        assert_eq!(
            report.verdict,
            TinyLivePackageLaunchPacketVerdict::TinyLivePackageLaunchPacketEligibleWhenGateTurnsGreen
        );
        assert_eq!(
            report.result.as_deref(),
            Some("eligible_when_gate_turns_green")
        );
        assert_eq!(
            report.authorization_result_now.as_deref(),
            Some("authorized_now")
        );
        assert!(report.activation_authorized);
    }

    #[test]
    fn green_run_then_verify_stays_eligible() {
        let fixture = launch_packet_fixture(
            "tiny_live_activation_package_launch_packet_verify_green",
            GateState::Green,
        );
        let run_report = run_json_report(fixture.config.clone());
        assert_eq!(
            run_report.verdict,
            TinyLivePackageLaunchPacketVerdict::TinyLivePackageLaunchPacketEligibleWhenGateTurnsGreen
        );

        let verify = verify_live_package_launch_packet_report(&fixture.config).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageLaunchPacketVerdict::TinyLivePackageLaunchPacketVerifyOk,
            "verification mismatches: {:?}",
            verify.verification_mismatches
        );
        assert_eq!(
            verify.result.as_deref(),
            Some("eligible_when_gate_turns_green")
        );
        assert_eq!(
            verify.authorization_result_now.as_deref(),
            Some("authorized_now")
        );
        assert!(verify.activation_authorized);
    }

    #[test]
    fn stage3_non_green_yields_refused_packet() {
        let fixture = launch_packet_fixture(
            "tiny_live_activation_package_launch_packet_stage3",
            GateState::Stage3Blocked,
        );
        let report = run_json_report(fixture.config.clone());
        assert_eq!(
            report.verdict,
            TinyLivePackageLaunchPacketVerdict::TinyLivePackageLaunchPacketRefusedByStage3
        );
        assert_eq!(report.result.as_deref(), Some("refused_by_stage3"));
        assert!(!report.activation_authorized);
    }

    #[test]
    fn pre_activation_non_green_yields_refused_packet() {
        let fixture = launch_packet_fixture(
            "tiny_live_activation_package_launch_packet_pre_gate",
            GateState::PreActivationBlocked,
        );
        let report = run_json_report(fixture.config.clone());
        assert_eq!(
            report.verdict,
            TinyLivePackageLaunchPacketVerdict::TinyLivePackageLaunchPacketRefusedByPreActivationGate
        );
        assert_eq!(
            report.result.as_deref(),
            Some("refused_by_pre_activation_gate")
        );
        assert!(!report.activation_authorized);
    }

    #[test]
    fn tampered_installed_wrapper_refuses_packet_as_invalid_target() {
        let fixture = launch_packet_fixture(
            "tiny_live_activation_package_launch_packet_invalid_wrapper",
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
            TinyLivePackageLaunchPacketVerdict::TinyLivePackageLaunchPacketRefusedByInvalidTarget
        );
        assert_eq!(report.result.as_deref(), Some("refused_by_invalid_target"));
    }

    #[test]
    fn verify_rejects_tampered_authorization_step_path() {
        let fixture = launch_packet_fixture(
            "tiny_live_activation_package_launch_packet_tampered_auth_path",
            GateState::Green,
        );
        run_json_report(fixture.config.clone());
        let paths = package_live_launch_packet_paths(fixture.config.session_dir.as_ref().unwrap());
        let mut status: PackageLaunchPacketStatus = load_json(&paths.status_path).unwrap();
        let other = fixture.fixture_dir.join("other.report.json");
        fs::write(&other, "{}").unwrap();
        status.authorization_step.as_mut().unwrap().path = other.display().to_string();
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_live_package_launch_packet_report(&fixture.config).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageLaunchPacketVerdict::TinyLivePackageLaunchPacketVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_mismatched_target_contract() {
        let fixture = launch_packet_fixture(
            "tiny_live_activation_package_launch_packet_target_mismatch",
            GateState::Green,
        );
        run_json_report(fixture.config.clone());
        let verify = verify_live_package_launch_packet_report(&Config {
            target_service_name: "other.service".to_string(),
            ..fixture.config
        })
        .unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageLaunchPacketVerdict::TinyLivePackageLaunchPacketVerifyInvalid
        );
    }

    #[derive(Debug, Clone, Copy)]
    enum GateState {
        Green,
        Stage3Blocked,
        PreActivationBlocked,
    }

    struct LaunchPacketFixture {
        fixture_dir: PathBuf,
        config: Config,
        _rpc_server: MockHttpServer,
        _adapter_server: MockHttpServer,
    }

    fn launch_packet_fixture(label: &str, gate_state: GateState) -> LaunchPacketFixture {
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

        LaunchPacketFixture {
            fixture_dir: fixture_dir.clone(),
            config: Config {
                mode: Mode::RunLivePackageLaunchPacket,
                package_dir,
                install_root,
                target_service_name: "solana-copy-bot.service".to_string(),
                backend_command: backend_path.display().to_string(),
                wrapper_timeout_ms: tiny_live_activation_live_execute::live_service_control_wrapper_contract::DEFAULT_TIMEOUT_MS,
                service_status_max_staleness_ms:
                    tiny_live_activation_package_preflight::DEFAULT_SERVICE_STATUS_MAX_STALENESS_MS,
                session_dir: Some(fixture_dir.join("launch-packet-session")),
                output_path: None,
                json: false,
            },
            _rpc_server: rpc_server,
            _adapter_server: adapter_server,
        }
    }

    fn run_json_report(config: Config) -> PackageLaunchPacketReport {
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
        let metadata = RenderedConfigMetadata {
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
            field_expectations: vec![
                tiny_live_activation_live_execute::tiny_live_activation_execute::FieldExpectation {
                    field: "execution.enabled".to_string(),
                    source_value: serde_json::json!(source_execution_enabled),
                    target_value: serde_json::json!(target_execution_enabled),
                    reason: "test".to_string(),
                    source: "test".to_string(),
                },
            ],
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
                    now - Duration::minutes(5),
                    "validated_current",
                    "fresh_current",
                    true,
                );
                append_wallet_freshness_capture(
                    &store,
                    now - Duration::minutes(15),
                    "validated_current",
                    "fresh_current",
                    true,
                );
                append_wallet_freshness_capture(
                    &store,
                    now - Duration::minutes(25),
                    "validated_current",
                    "fresh_current",
                    true,
                );
                store
                    .append_execution_dry_run_rehearsal(&rehearsal_write(
                        now - Duration::minutes(3),
                        "rehearsal_green",
                    ))
                    .unwrap();
                store
                    .append_execution_dry_run_rehearsal(&rehearsal_write(
                        now - Duration::minutes(12),
                        "rehearsal_green_with_business_reject",
                    ))
                    .unwrap();
            }
            GateState::PreActivationBlocked => {
                append_wallet_freshness_capture(
                    &store,
                    now - Duration::minutes(5),
                    "validated_current",
                    "fresh_current",
                    true,
                );
                append_wallet_freshness_capture(
                    &store,
                    now - Duration::minutes(15),
                    "validated_current",
                    "fresh_current",
                    true,
                );
                append_wallet_freshness_capture(
                    &store,
                    now - Duration::minutes(25),
                    "validated_current",
                    "fresh_current",
                    true,
                );
                store
                    .append_execution_dry_run_rehearsal(&rehearsal_write(
                        now - Duration::days(2),
                        "stale_rehearsal",
                    ))
                    .unwrap();
            }
            GateState::Stage3Blocked => {
                append_wallet_freshness_capture(
                    &store,
                    now - Duration::minutes(5),
                    "publication_drifting",
                    "drifting_but_acceptable",
                    false,
                );
                append_wallet_freshness_capture(
                    &store,
                    now - Duration::minutes(15),
                    "publication_drifting",
                    "drifting_but_acceptable",
                    false,
                );
                append_wallet_freshness_capture(
                    &store,
                    now - Duration::minutes(25),
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
                    "window_start": captured_at - Duration::days(5),
                    "verdict": audit_verdict,
                    "reason": "seed",
                    "follow_top_n": 2,
                    "publication_truth_available": true,
                    "publication_runtime_mode": "healthy",
                    "publication_recent_under_gate": true,
                    "latest_publication_ts": captured_at,
                    "publication_age_seconds": 60,
                    "latest_publication_window_start": captured_at - Duration::days(5),
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
                        "covered_since": captured_at - Duration::days(5),
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
                    "recent_window_start": captured_at - Duration::minutes(30),
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
                    if let Some(header) = required_header.as_deref() {
                        let header_name = format!("{header}:");
                        assert!(
                            request
                                .lines()
                                .any(|line| line.to_ascii_lowercase().starts_with(&header_name)),
                            "missing required header {header} in request: {request}"
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
