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
#[path = "copybot_tiny_live_activation_package_launch_packet.rs"]
mod tiny_live_activation_package_launch_packet;
#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_package_live_authorization.rs"]
mod tiny_live_activation_package_live_authorization;
#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_package_live_cutover.rs"]
mod tiny_live_activation_package_live_cutover;
#[allow(dead_code)]
#[path = "copybot_tiny_live_activation_package_preflight.rs"]
mod tiny_live_activation_package_preflight;

const USAGE: &str = "usage: copybot_tiny_live_activation_package_turn_green --launch-packet-session-dir <path> [--session-dir <path>] [--output <path>] [--json] (--plan-live-package-turn-green | --render-live-package-turn-green-script | --run-live-package-turn-green | --verify-live-package-turn-green)";
const TURN_GREEN_SESSION_VERSION: &str = "1";
const TURN_GREEN_STATUS_VERSION: &str = "1";

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
    launch_packet_session_dir: PathBuf,
    session_dir: Option<PathBuf>,
    output_path: Option<PathBuf>,
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    PlanLivePackageTurnGreen,
    RenderLivePackageTurnGreenScript,
    RunLivePackageTurnGreen,
    VerifyLivePackageTurnGreen,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageTurnGreenVerdict {
    TinyLivePackageTurnGreenPlanReady,
    TinyLivePackageTurnGreenScriptRendered,
    TinyLivePackageTurnGreenRefusedNowByStage3,
    TinyLivePackageTurnGreenRefusedNowByPreActivationGate,
    TinyLivePackageTurnGreenRefusedNowByInvalidOrDriftedContract,
    TinyLivePackageTurnGreenExecutableNow,
    TinyLivePackageTurnGreenVerifyOk,
    TinyLivePackageTurnGreenVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LivePackageTurnGreenResult {
    RefusedNowByStage3,
    RefusedNowByPreActivationGate,
    RefusedNowByInvalidOrDriftedContract,
    ExecutableNow,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageTurnGreenStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageTurnGreenSession {
    session_version: String,
    planned_at: DateTime<Utc>,
    launch_packet_session_dir: String,
    package_dir: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    session_dir: String,
    nested_current_authorization_session_dir: String,
    verify_frozen_launch_packet_summary: String,
    refresh_current_authorization_command_summary: String,
    frozen_live_cutover_controller_command_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageTurnGreenStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    session_dir: String,
    launch_packet_session_dir: String,
    nested_current_authorization_session_dir: String,
    result: LivePackageTurnGreenResult,
    reason: String,
    frozen_launch_packet_result: Option<String>,
    current_authorization_result_now: Option<String>,
    frozen_launch_packet_step: Option<PackageTurnGreenStepArtifact>,
    current_authorization_step: Option<PackageTurnGreenStepArtifact>,
    current_controller_step: Option<PackageTurnGreenStepArtifact>,
    frozen_controller_still_current: bool,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    operator_next_action_summary: String,
    executable_now: bool,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackageTurnGreenPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    frozen_launch_packet_report_path: PathBuf,
    current_authorization_report_path: PathBuf,
    current_controller_report_path: PathBuf,
    current_authorization_session_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageTurnGreenReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageTurnGreenVerdict,
    reason: String,
    launch_packet_session_dir: String,
    package_dir: Option<String>,
    install_root: Option<String>,
    target_service_name: Option<String>,
    backend_command: Option<String>,
    wrapper_timeout_ms: Option<u64>,
    service_status_max_staleness_ms: Option<u64>,
    session_dir: Option<String>,
    turn_green_session_path: Option<String>,
    turn_green_status_path: Option<String>,
    nested_current_authorization_session_dir: Option<String>,
    result: Option<String>,
    frozen_launch_packet_result: Option<String>,
    current_authorization_result_now: Option<String>,
    frozen_launch_packet_step: Option<PackageTurnGreenStepArtifact>,
    current_authorization_step: Option<PackageTurnGreenStepArtifact>,
    current_controller_step: Option<PackageTurnGreenStepArtifact>,
    frozen_controller_still_current: bool,
    verify_frozen_launch_packet_summary: Option<String>,
    refresh_current_authorization_command_summary: Option<String>,
    frozen_live_cutover_controller_command_summary: Option<String>,
    current_live_cutover_controller_command_summary: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    operator_next_action_summary: Option<String>,
    verification_mismatches: Vec<String>,
    executable_now: bool,
    explicit_statement: String,
}

#[derive(Debug, Deserialize)]
struct StoredLaunchPacketReportView {
    package_dir: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    session_dir: Option<String>,
    result: Option<String>,
    authorization_result_now: Option<String>,
    activation_authorized: bool,
    run_live_authorization_command_summary: String,
    frozen_live_cutover_controller_command_summary: String,
    operator_next_action_summary: String,
}

#[derive(Debug, Deserialize)]
struct StoredAuthorizationRefreshReportView {
    package_dir: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    session_dir: Option<String>,
    result: Option<String>,
    gate_evaluation_command_summary: String,
    authorized_live_cutover_command_summary: String,
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
    let mut launch_packet_session_dir: Option<PathBuf> = None;
    let mut session_dir: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut json = false;
    let mut mode: Option<Mode> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--launch-packet-session-dir" => {
                launch_packet_session_dir = Some(PathBuf::from(parse_string_arg(
                    "--launch-packet-session-dir",
                    args.next(),
                )?))
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
            "--plan-live-package-turn-green" => {
                set_mode(&mut mode, Mode::PlanLivePackageTurnGreen, arg.as_str())?
            }
            "--render-live-package-turn-green-script" => set_mode(
                &mut mode,
                Mode::RenderLivePackageTurnGreenScript,
                arg.as_str(),
            )?,
            "--run-live-package-turn-green" => {
                set_mode(&mut mode, Mode::RunLivePackageTurnGreen, arg.as_str())?
            }
            "--verify-live-package-turn-green" => {
                set_mode(&mut mode, Mode::VerifyLivePackageTurnGreen, arg.as_str())?
            }
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    let Some(mode) = mode else {
        return Ok(None);
    };
    if matches!(mode, Mode::RenderLivePackageTurnGreenScript) && output_path.is_none() {
        bail!("missing --output for --render-live-package-turn-green-script");
    }
    if matches!(
        mode,
        Mode::RunLivePackageTurnGreen | Mode::VerifyLivePackageTurnGreen
    ) && session_dir.is_none()
    {
        bail!("missing --session-dir for run/verify mode");
    }
    let launch_packet_session_dir =
        launch_packet_session_dir.ok_or_else(|| anyhow!("missing --launch-packet-session-dir"))?;
    Ok(Some(Config {
        mode,
        launch_packet_session_dir,
        session_dir,
        output_path,
        json,
    }))
}

fn run(config: Config) -> Result<String> {
    let report = match config.mode {
        Mode::PlanLivePackageTurnGreen => plan_live_package_turn_green_report(&config)?,
        Mode::RenderLivePackageTurnGreenScript => render_live_package_turn_green_report(&config)?,
        Mode::RunLivePackageTurnGreen => run_live_package_turn_green_report(&config)?,
        Mode::VerifyLivePackageTurnGreen => verify_live_package_turn_green_report(&config)?,
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_report_lines(&report))
    }
}

fn plan_live_package_turn_green_report(config: &Config) -> Result<PackageTurnGreenReport> {
    let frozen =
        tiny_live_activation_package_launch_packet::load_live_package_launch_packet_contract_for_turn_green(
            &config.launch_packet_session_dir,
        )?;
    Ok(PackageTurnGreenReport {
        generated_at: Utc::now(),
        mode: "plan_live_package_turn_green".to_string(),
        verdict: TinyLivePackageTurnGreenVerdict::TinyLivePackageTurnGreenPlanReady,
        reason: format!(
            "launch-packet-native turn-green refresh is explicit for frozen launch packet {}; this command only refreshes current gate/controller truth and never runs the frozen live cutover controller itself",
            config.launch_packet_session_dir.display()
        ),
        launch_packet_session_dir: config.launch_packet_session_dir.display().to_string(),
        package_dir: Some(frozen.package_dir.display().to_string()),
        install_root: Some(frozen.install_root.display().to_string()),
        target_service_name: Some(frozen.target_service_name.clone()),
        backend_command: Some(frozen.backend_command.clone()),
        wrapper_timeout_ms: Some(frozen.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(frozen.service_status_max_staleness_ms),
        session_dir: config
            .session_dir
            .as_ref()
            .map(|value| value.display().to_string()),
        turn_green_session_path: None,
        turn_green_status_path: None,
        nested_current_authorization_session_dir: config.session_dir.as_ref().map(|value| {
            turn_green_paths(value)
                .current_authorization_session_dir
                .display()
                .to_string()
        }),
        result: None,
        frozen_launch_packet_result: frozen.frozen_result.clone(),
        current_authorization_result_now: None,
        frozen_launch_packet_step: None,
        current_authorization_step: None,
        current_controller_step: None,
        frozen_controller_still_current: false,
        verify_frozen_launch_packet_summary: Some(verify_frozen_launch_packet_summary(
            &config.launch_packet_session_dir,
        )),
        refresh_current_authorization_command_summary: config.session_dir.as_ref().map(|value| {
            refresh_current_authorization_command_summary(
                &frozen,
                &turn_green_paths(value).current_authorization_session_dir,
            )
        }),
        frozen_live_cutover_controller_command_summary: Some(
            frozen.frozen_live_cutover_controller_command_summary,
        ),
        current_live_cutover_controller_command_summary: Some(
            frozen.controller_plan_view.run_live_cutover_command_summary,
        ),
        current_pre_activation_gate_verdict: frozen.current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason: frozen.current_pre_activation_gate_reason,
        operator_next_action_summary: Some(
            "run the launch-packet-native turn-green refresh session to answer whether the frozen controller is executable right now; this step remains non-authorizing until current gate truth is revalidated"
                .to_string(),
        ),
        verification_mismatches: Vec::new(),
        executable_now: false,
        explicit_statement:
            "this packet-native turn-green refresh binds one immutable launch packet to current live host truth and never runs the frozen controller itself"
                .to_string(),
    })
}

fn render_live_package_turn_green_report(config: &Config) -> Result<PackageTurnGreenReport> {
    let plan = plan_live_package_turn_green_report(config)?;
    let output_path = config
        .output_path
        .as_ref()
        .ok_or_else(|| anyhow!("missing --output for render mode"))?;
    let session_dir = config
        .session_dir
        .clone()
        .unwrap_or_else(|| output_path.with_extension("session"));
    let script = format!(
        "#!/usr/bin/env bash\nset -euo pipefail\n\ncopybot_tiny_live_activation_package_turn_green --launch-packet-session-dir {} --plan-live-package-turn-green\ncopybot_tiny_live_activation_package_turn_green --launch-packet-session-dir {} --session-dir {} --run-live-package-turn-green\ncopybot_tiny_live_activation_package_turn_green --launch-packet-session-dir {} --session-dir {} --verify-live-package-turn-green\n",
        shell_escape_path(&config.launch_packet_session_dir),
        shell_escape_path(&config.launch_packet_session_dir),
        shell_escape_path(&session_dir),
        shell_escape_path(&config.launch_packet_session_dir),
        shell_escape_path(&session_dir),
    );
    write_script(output_path, &script)?;
    Ok(PackageTurnGreenReport {
        generated_at: Utc::now(),
        mode: "render_live_package_turn_green_script".to_string(),
        verdict: TinyLivePackageTurnGreenVerdict::TinyLivePackageTurnGreenScriptRendered,
        reason: format!(
            "rendered launch-packet-native turn-green refresh script to {}",
            output_path.display()
        ),
        launch_packet_session_dir: plan.launch_packet_session_dir,
        package_dir: plan.package_dir,
        install_root: plan.install_root,
        target_service_name: plan.target_service_name,
        backend_command: plan.backend_command,
        wrapper_timeout_ms: plan.wrapper_timeout_ms,
        service_status_max_staleness_ms: plan.service_status_max_staleness_ms,
        session_dir: Some(session_dir.display().to_string()),
        turn_green_session_path: None,
        turn_green_status_path: None,
        nested_current_authorization_session_dir: Some(
            turn_green_paths(&session_dir)
                .current_authorization_session_dir
                .display()
                .to_string(),
        ),
        result: None,
        frozen_launch_packet_result: plan.frozen_launch_packet_result,
        current_authorization_result_now: None,
        frozen_launch_packet_step: None,
        current_authorization_step: None,
        current_controller_step: None,
        frozen_controller_still_current: false,
        verify_frozen_launch_packet_summary: plan.verify_frozen_launch_packet_summary,
        refresh_current_authorization_command_summary: plan
            .refresh_current_authorization_command_summary,
        frozen_live_cutover_controller_command_summary: plan
            .frozen_live_cutover_controller_command_summary,
        current_live_cutover_controller_command_summary: plan
            .current_live_cutover_controller_command_summary,
        current_pre_activation_gate_verdict: plan.current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason: plan.current_pre_activation_gate_reason,
        operator_next_action_summary: Some(format!(
            "the rendered script refreshes the immutable launch packet at {} into an executable-now/refused-now artifact without running the frozen controller",
            config.launch_packet_session_dir.display()
        )),
        verification_mismatches: Vec::new(),
        executable_now: false,
        explicit_statement:
            "this rendered script stays read-only with respect to the real target; it only refreshes launch-packet truth and never executes the frozen controller"
                .to_string(),
    })
}

fn run_live_package_turn_green_report(config: &Config) -> Result<PackageTurnGreenReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for run mode"))?;
    let frozen = match tiny_live_activation_package_launch_packet::verify_live_package_launch_packet_for_turn_green(
        &config.launch_packet_session_dir,
    ) {
        Ok(step) => step,
        Err(error) => {
            return Ok(failure_report_without_frozen_contract(
                config,
                TinyLivePackageTurnGreenVerdict::TinyLivePackageTurnGreenRefusedNowByInvalidOrDriftedContract,
                format!(
                    "failed to verify frozen launch packet under {}: {error}",
                    config.launch_packet_session_dir.display()
                ),
            ))
        }
    };

    let frozen_contract = frozen.contract.clone();
    validate_turn_green_session_dir(&frozen_contract.install_root, session_dir)?;
    ensure_clean_turn_green_session_dir(session_dir)?;
    let paths = turn_green_paths(session_dir);
    let current_controller_view = current_controller_view(&frozen_contract);
    let current_controller_step = build_step_artifact(
        &paths.current_controller_report_path,
        "plan_current_live_cutover_controller",
        &current_controller_view.verdict,
        &current_controller_view.reason,
        Utc::now(),
    );
    persist_json(
        &paths.current_controller_report_path,
        &current_controller_view,
    )?;
    persist_json(&paths.frozen_launch_packet_report_path, &frozen.report_json)?;

    let current_authorization = tiny_live_activation_package_live_authorization::run_live_package_authorization_for_launch_packet(
        &frozen_contract.package_dir,
        &frozen_contract.install_root,
        &frozen_contract.target_service_name,
        &frozen_contract.backend_command,
        frozen_contract.wrapper_timeout_ms,
        frozen_contract.service_status_max_staleness_ms,
        &paths.current_authorization_session_dir,
    )?;
    persist_json(
        &paths.current_authorization_report_path,
        &current_authorization.report_json,
    )?;

    let frozen_launch_packet_step = build_step_artifact(
        &paths.frozen_launch_packet_report_path,
        "verify_live_package_launch_packet",
        &frozen.verdict,
        &frozen.reason,
        frozen.generated_at,
    );
    let current_authorization_step = build_step_artifact(
        &paths.current_authorization_report_path,
        "run_live_package_authorization",
        &current_authorization.verdict,
        &current_authorization.reason,
        current_authorization.generated_at,
    );
    let frozen_controller_still_current = controller_plan_views_match(
        &frozen_contract.controller_plan_view,
        &current_controller_view,
    ) && frozen_contract
        .frozen_live_cutover_controller_command_summary
        == current_controller_view.run_live_cutover_command_summary
        && current_authorization.authorized_live_cutover_command_summary
            == current_controller_view.run_live_cutover_command_summary
        && current_authorization.gate_evaluation_command_summary
            == current_controller_view.gate_evaluation_command_summary;

    let (result, verdict, reason, executable_now) = classify_turn_green_result(
        &frozen,
        &current_authorization,
        &current_controller_view,
        frozen_controller_still_current,
    );
    let operator_next_action_summary = operator_next_action_summary(
        result,
        &frozen_contract.frozen_live_cutover_controller_command_summary,
    );
    let status = PackageTurnGreenStatus {
        status_version: TURN_GREEN_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        session_dir: session_dir.display().to_string(),
        launch_packet_session_dir: config.launch_packet_session_dir.display().to_string(),
        nested_current_authorization_session_dir: paths
            .current_authorization_session_dir
            .display()
            .to_string(),
        result,
        reason: reason.clone(),
        frozen_launch_packet_result: frozen_contract.frozen_result.clone(),
        current_authorization_result_now: current_authorization.result.clone(),
        frozen_launch_packet_step: Some(frozen_launch_packet_step.clone()),
        current_authorization_step: Some(current_authorization_step.clone()),
        current_controller_step: Some(current_controller_step.clone()),
        frozen_controller_still_current,
        current_pre_activation_gate_verdict: current_authorization
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: current_authorization
            .current_pre_activation_gate_reason
            .clone(),
        operator_next_action_summary: operator_next_action_summary.clone(),
        executable_now,
        explicit_statement:
            "this packet-native turn-green refresh never runs the frozen live cutover controller; it only classifies whether that frozen controller is executable now under current truth"
                .to_string(),
    };
    let session = PackageTurnGreenSession {
        session_version: TURN_GREEN_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        launch_packet_session_dir: config.launch_packet_session_dir.display().to_string(),
        package_dir: frozen_contract.package_dir.display().to_string(),
        install_root: frozen_contract.install_root.display().to_string(),
        target_service_name: frozen_contract.target_service_name.clone(),
        backend_command: frozen_contract.backend_command.clone(),
        wrapper_timeout_ms: frozen_contract.wrapper_timeout_ms,
        service_status_max_staleness_ms: frozen_contract.service_status_max_staleness_ms,
        session_dir: session_dir.display().to_string(),
        nested_current_authorization_session_dir: paths
            .current_authorization_session_dir
            .display()
            .to_string(),
        verify_frozen_launch_packet_summary: verify_frozen_launch_packet_summary(
            &config.launch_packet_session_dir,
        ),
        refresh_current_authorization_command_summary: refresh_current_authorization_command_summary(
            &frozen_contract,
            &paths.current_authorization_session_dir,
        ),
        frozen_live_cutover_controller_command_summary: frozen_contract
            .frozen_live_cutover_controller_command_summary
            .clone(),
        explicit_statement:
            "the immutable launch packet is the primary handoff input here; current host truth may only refuse or mark that frozen controller executable now"
                .to_string(),
    };
    persist_json(&paths.session_path, &session)?;
    persist_json(&paths.status_path, &status)?;
    Ok(PackageTurnGreenReport {
        generated_at: Utc::now(),
        mode: "run_live_package_turn_green".to_string(),
        verdict,
        reason,
        launch_packet_session_dir: config.launch_packet_session_dir.display().to_string(),
        package_dir: Some(frozen_contract.package_dir.display().to_string()),
        install_root: Some(frozen_contract.install_root.display().to_string()),
        target_service_name: Some(frozen_contract.target_service_name),
        backend_command: Some(frozen_contract.backend_command),
        wrapper_timeout_ms: Some(frozen_contract.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(frozen_contract.service_status_max_staleness_ms),
        session_dir: Some(session_dir.display().to_string()),
        turn_green_session_path: Some(paths.session_path.display().to_string()),
        turn_green_status_path: Some(paths.status_path.display().to_string()),
        nested_current_authorization_session_dir: Some(
            paths
                .current_authorization_session_dir
                .display()
                .to_string(),
        ),
        result: Some(serialize_enum(&status.result)),
        frozen_launch_packet_result: status.frozen_launch_packet_result,
        current_authorization_result_now: status.current_authorization_result_now,
        frozen_launch_packet_step: Some(frozen_launch_packet_step),
        current_authorization_step: Some(current_authorization_step),
        current_controller_step: Some(current_controller_step),
        frozen_controller_still_current,
        verify_frozen_launch_packet_summary: Some(session.verify_frozen_launch_packet_summary),
        refresh_current_authorization_command_summary: Some(
            session.refresh_current_authorization_command_summary,
        ),
        frozen_live_cutover_controller_command_summary: Some(
            session.frozen_live_cutover_controller_command_summary,
        ),
        current_live_cutover_controller_command_summary: Some(
            current_controller_view.run_live_cutover_command_summary,
        ),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason,
        operator_next_action_summary: Some(operator_next_action_summary),
        verification_mismatches: Vec::new(),
        executable_now,
        explicit_statement: status.explicit_statement,
    })
}

fn verify_live_package_turn_green_report(config: &Config) -> Result<PackageTurnGreenReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for verify mode"))?;
    let paths = turn_green_paths(session_dir);
    let mut mismatches = Vec::new();
    let verified_frozen = match tiny_live_activation_package_launch_packet::verify_live_package_launch_packet_for_turn_green(
        &config.launch_packet_session_dir,
    ) {
        Ok(step) => {
            validate_turn_green_session_dir(&step.contract.install_root, session_dir)?;
            step
        }
        Err(error) => {
            mismatches.push(format!(
                "failed to verify frozen launch packet under {}: {error}",
                config.launch_packet_session_dir.display()
            ));
            fallback_verified_frozen_step(&config.launch_packet_session_dir)
        }
    };
    let session: PackageTurnGreenSession = load_json(&paths.session_path)?;
    let status: PackageTurnGreenStatus = load_json(&paths.status_path)?;

    compare_string(
        &session.launch_packet_session_dir,
        &config.launch_packet_session_dir.display().to_string(),
        "turn-green launch_packet_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "turn-green session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.session_dir,
        &session_dir.display().to_string(),
        "turn-green status session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.launch_packet_session_dir,
        &config.launch_packet_session_dir.display().to_string(),
        "turn-green status launch_packet_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.nested_current_authorization_session_dir,
        &paths
            .current_authorization_session_dir
            .display()
            .to_string(),
        "turn-green nested_current_authorization_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.nested_current_authorization_session_dir,
        &paths
            .current_authorization_session_dir
            .display()
            .to_string(),
        "turn-green status nested_current_authorization_session_dir",
        &mut mismatches,
    );
    if verified_frozen.verdict != "tiny_live_package_launch_packet_verify_ok" {
        mismatches.push(format!(
            "nested frozen launch packet verification is non-green: {}",
            verified_frozen.reason
        ));
    }

    let stored_frozen_report: StoredLaunchPacketReportView = load_required_step_json(
        &status.frozen_launch_packet_step,
        &paths.frozen_launch_packet_report_path,
        "frozen_launch_packet_step",
        &mut mismatches,
    )?;
    validate_stored_launch_packet_report(
        &stored_frozen_report,
        &verified_frozen.contract,
        &mut mismatches,
    );

    let current_controller_view = current_controller_view(&verified_frozen.contract);
    validate_stored_turn_green_session_contract(
        &session,
        &verified_frozen.contract,
        &paths.current_authorization_session_dir,
        &mut mismatches,
    );
    let stored_current_controller_view: tiny_live_activation_package_live_cutover::LiveCutoverControllerPlanView =
        load_required_step_json(
            &status.current_controller_step,
            &paths.current_controller_report_path,
            "current_controller_step",
            &mut mismatches,
        )?;
    if !controller_plan_views_match(&stored_current_controller_view, &current_controller_view) {
        mismatches.push(
            "stored current_controller_step does not match the current live cutover controller contract"
                .to_string(),
        );
    }
    if !controller_plan_views_match(
        &verified_frozen.contract.controller_plan_view,
        &current_controller_view,
    ) {
        mismatches.push(
            "current live cutover controller contract no longer matches the frozen launch packet controller summary"
                .to_string(),
        );
    }

    let verified_current_authorization = match tiny_live_activation_package_live_authorization::verify_live_package_authorization_for_launch_packet(
        &verified_frozen.contract.package_dir,
        &verified_frozen.contract.install_root,
        &verified_frozen.contract.target_service_name,
        &verified_frozen.contract.backend_command,
        verified_frozen.contract.wrapper_timeout_ms,
        verified_frozen.contract.service_status_max_staleness_ms,
        &paths.current_authorization_session_dir,
    ) {
        Ok(step) => step,
        Err(error) => {
            mismatches.push(format!(
                "failed to verify nested current authorization refresh under {}: {error}",
                paths.current_authorization_session_dir.display()
            ));
            fallback_current_authorization_step()
        }
    };
    if verified_current_authorization.verdict != "tiny_live_package_live_authorization_verify_ok" {
        mismatches.push(format!(
            "current authorization refresh verification is non-green: {}",
            verified_current_authorization.reason
        ));
    }
    let stored_current_authorization: StoredAuthorizationRefreshReportView =
        load_required_step_json(
            &status.current_authorization_step,
            &paths.current_authorization_report_path,
            "current_authorization_step",
            &mut mismatches,
        )?;
    validate_stored_current_authorization_report(
        &stored_current_authorization,
        &verified_frozen.contract,
        &current_controller_view,
        &paths.current_authorization_session_dir,
        &mut mismatches,
    );
    if stored_current_authorization.result != verified_current_authorization.result {
        mismatches.push(format!(
            "stored current authorization result {:?} does not match verified current authorization result {:?}",
            stored_current_authorization.result, verified_current_authorization.result
        ));
    }
    if stored_current_authorization.activation_authorized
        != verified_current_authorization.activation_authorized
    {
        mismatches.push(format!(
            "stored current authorization activation_authorized {} does not match verified current authorization activation_authorized {}",
            stored_current_authorization.activation_authorized,
            verified_current_authorization.activation_authorized
        ));
    }

    let frozen_controller_still_current = controller_plan_views_match(
        &verified_frozen.contract.controller_plan_view,
        &current_controller_view,
    ) && verified_frozen
        .contract
        .frozen_live_cutover_controller_command_summary
        == current_controller_view.run_live_cutover_command_summary
        && verified_current_authorization.authorized_live_cutover_command_summary
            == current_controller_view.run_live_cutover_command_summary
        && verified_current_authorization.gate_evaluation_command_summary
            == current_controller_view.gate_evaluation_command_summary;
    if status.frozen_controller_still_current != frozen_controller_still_current {
        mismatches.push(format!(
            "stored frozen_controller_still_current {} does not match verified {}",
            status.frozen_controller_still_current, frozen_controller_still_current
        ));
    }

    let (expected_result, _expected_verdict, _expected_reason, expected_executable_now) =
        classify_turn_green_result(
            &verified_frozen,
            &verified_current_authorization,
            &current_controller_view,
            frozen_controller_still_current,
        );
    if status.result != expected_result {
        mismatches.push(format!(
            "stored turn-green result {} does not match verified refreshed chain {}",
            serialize_enum(&status.result),
            serialize_enum(&expected_result)
        ));
    }
    if status.current_authorization_result_now != verified_current_authorization.result {
        mismatches.push(format!(
            "stored current_authorization_result_now {:?} does not match verified current authorization result {:?}",
            status.current_authorization_result_now,
            verified_current_authorization.result
        ));
    }
    if status.executable_now != expected_executable_now {
        mismatches.push(format!(
            "stored executable_now {} does not match verified {}",
            status.executable_now, expected_executable_now
        ));
    }
    compare_string(
        &status.operator_next_action_summary,
        &operator_next_action_summary(
            expected_result,
            &verified_frozen
                .contract
                .frozen_live_cutover_controller_command_summary,
        ),
        "turn-green operator_next_action_summary",
        &mut mismatches,
    );
    if status.current_pre_activation_gate_verdict
        != verified_current_authorization.current_pre_activation_gate_verdict
    {
        mismatches.push(format!(
            "stored current_pre_activation_gate_verdict {:?} does not match verified {:?}",
            status.current_pre_activation_gate_verdict,
            verified_current_authorization.current_pre_activation_gate_verdict
        ));
    }
    if status.current_pre_activation_gate_reason
        != verified_current_authorization.current_pre_activation_gate_reason
    {
        mismatches.push(format!(
            "stored current_pre_activation_gate_reason {:?} does not match verified {:?}",
            status.current_pre_activation_gate_reason,
            verified_current_authorization.current_pre_activation_gate_reason
        ));
    }

    let verdict = if mismatches.is_empty() {
        TinyLivePackageTurnGreenVerdict::TinyLivePackageTurnGreenVerifyOk
    } else {
        TinyLivePackageTurnGreenVerdict::TinyLivePackageTurnGreenVerifyInvalid
    };
    let reason = if mismatches.is_empty() {
        format!(
            "launch-packet-native turn-green session under {} is coherent and correctly classifies the frozen controller against current live truth",
            session_dir.display()
        )
    } else {
        mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "launch-packet-native turn-green verification failed".to_string())
    };

    Ok(PackageTurnGreenReport {
        generated_at: Utc::now(),
        mode: "verify_live_package_turn_green".to_string(),
        verdict,
        reason,
        launch_packet_session_dir: config.launch_packet_session_dir.display().to_string(),
        package_dir: Some(verified_frozen.contract.package_dir.display().to_string()),
        install_root: Some(verified_frozen.contract.install_root.display().to_string()),
        target_service_name: Some(verified_frozen.contract.target_service_name.clone()),
        backend_command: Some(verified_frozen.contract.backend_command.clone()),
        wrapper_timeout_ms: Some(verified_frozen.contract.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(
            verified_frozen.contract.service_status_max_staleness_ms,
        ),
        session_dir: Some(session_dir.display().to_string()),
        turn_green_session_path: Some(paths.session_path.display().to_string()),
        turn_green_status_path: Some(paths.status_path.display().to_string()),
        nested_current_authorization_session_dir: Some(
            paths.current_authorization_session_dir.display().to_string(),
        ),
        result: Some(serialize_enum(&status.result)),
        frozen_launch_packet_result: status.frozen_launch_packet_result,
        current_authorization_result_now: status.current_authorization_result_now,
        frozen_launch_packet_step: status.frozen_launch_packet_step,
        current_authorization_step: status.current_authorization_step,
        current_controller_step: status.current_controller_step,
        frozen_controller_still_current,
        verify_frozen_launch_packet_summary: Some(verify_frozen_launch_packet_summary(
            &config.launch_packet_session_dir,
        )),
        refresh_current_authorization_command_summary: Some(
            refresh_current_authorization_command_summary(
                &verified_frozen.contract,
                &paths.current_authorization_session_dir,
            ),
        ),
        frozen_live_cutover_controller_command_summary: Some(
            verified_frozen
                .contract
                .frozen_live_cutover_controller_command_summary,
        ),
        current_live_cutover_controller_command_summary: Some(
            current_controller_view.run_live_cutover_command_summary,
        ),
        current_pre_activation_gate_verdict: verified_current_authorization
            .current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason: verified_current_authorization
            .current_pre_activation_gate_reason,
        operator_next_action_summary: Some(status.operator_next_action_summary),
        verification_mismatches: mismatches,
        executable_now: status.executable_now && expected_executable_now,
        explicit_statement:
            "this verification proves only whether the frozen controller is executable now; it never runs that controller"
                .to_string(),
    })
}

fn verify_frozen_launch_packet_summary(launch_packet_session_dir: &Path) -> String {
    format!(
        "verify immutable launch packet session under {} before refreshing current gate/controller truth",
        launch_packet_session_dir.display()
    )
}

fn refresh_current_authorization_command_summary(
    frozen: &tiny_live_activation_package_launch_packet::LivePackageLaunchPacketContractView,
    current_authorization_session_dir: &Path,
) -> String {
    format!(
        "copybot_tiny_live_activation_package_live_authorization --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --service-status-max-staleness-ms {} --session-dir {} --run-live-package-authorization",
        frozen.package_dir.display(),
        frozen.install_root.display(),
        frozen.target_service_name,
        frozen.backend_command,
        frozen.wrapper_timeout_ms,
        frozen.service_status_max_staleness_ms,
        current_authorization_session_dir.display()
    )
}

fn current_controller_view(
    frozen: &tiny_live_activation_package_launch_packet::LivePackageLaunchPacketContractView,
) -> tiny_live_activation_package_live_cutover::LiveCutoverControllerPlanView {
    tiny_live_activation_package_live_cutover::live_cutover_controller_plan_view_for_authorization(
        &frozen.package_dir,
        &frozen.install_root,
        &frozen.target_service_name,
        &frozen.backend_command,
        frozen.wrapper_timeout_ms,
        frozen.service_status_max_staleness_ms,
    )
}

fn controller_plan_views_match<L: Serialize, R: Serialize>(left: &L, right: &R) -> bool {
    serde_json::to_value(left).ok() == serde_json::to_value(right).ok()
}

fn current_authorization_proves_executable_now(
    step: &tiny_live_activation_package_live_authorization::PackageLiveAuthorizationLaunchPacketStep,
) -> bool {
    step.result.as_deref() == Some("authorized_now") && step.activation_authorized
}

fn classify_turn_green_result(
    verified_frozen: &tiny_live_activation_package_launch_packet::VerifiedLivePackageLaunchPacketTurnGreenStep,
    current_authorization: &tiny_live_activation_package_live_authorization::PackageLiveAuthorizationLaunchPacketStep,
    current_controller_view: &tiny_live_activation_package_live_cutover::LiveCutoverControllerPlanView,
    frozen_controller_still_current: bool,
) -> (
    LivePackageTurnGreenResult,
    TinyLivePackageTurnGreenVerdict,
    String,
    bool,
) {
    if verified_frozen.verdict != "tiny_live_package_launch_packet_verify_ok"
        || !frozen_controller_still_current
        || current_controller_view.verdict != "tiny_live_package_live_cutover_plan_ready"
    {
        return (
            LivePackageTurnGreenResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageTurnGreenVerdict::TinyLivePackageTurnGreenRefusedNowByInvalidOrDriftedContract,
            "the frozen launch packet, current wrapper/target contract, or current live cutover controller summary drifted and the frozen controller is not executable now".to_string(),
            false,
        );
    }

    match current_authorization.result.as_deref() {
        Some("authorized_now") if current_authorization_proves_executable_now(current_authorization) => (
            LivePackageTurnGreenResult::ExecutableNow,
            TinyLivePackageTurnGreenVerdict::TinyLivePackageTurnGreenExecutableNow,
            "the frozen launch packet still matches the current live host contract and the refreshed gate truth is green; the frozen controller is executable now if the operator explicitly chooses to run it".to_string(),
            true,
        ),
        Some("refused_by_stage3") => (
            LivePackageTurnGreenResult::RefusedNowByStage3,
            TinyLivePackageTurnGreenVerdict::TinyLivePackageTurnGreenRefusedNowByStage3,
            "the frozen launch packet is still coherent, but the frozen controller remains refused now because current Stage 3 is non-green".to_string(),
            false,
        ),
        Some("refused_by_pre_activation_gate") => (
            LivePackageTurnGreenResult::RefusedNowByPreActivationGate,
            TinyLivePackageTurnGreenVerdict::TinyLivePackageTurnGreenRefusedNowByPreActivationGate,
            "the frozen launch packet is still coherent, but the frozen controller remains refused now because the current pre-activation gate is non-green".to_string(),
            false,
        ),
        _ => (
            LivePackageTurnGreenResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageTurnGreenVerdict::TinyLivePackageTurnGreenRefusedNowByInvalidOrDriftedContract,
            "the refreshed authorization chain is non-green or drifted, so the frozen controller is not executable now".to_string(),
            false,
        ),
    }
}

fn operator_next_action_summary(
    result: LivePackageTurnGreenResult,
    frozen_live_cutover_controller_command_summary: &str,
) -> String {
    match result {
        LivePackageTurnGreenResult::ExecutableNow => format!(
            "the frozen controller is executable now if the operator explicitly chooses to run this exact command: {frozen_live_cutover_controller_command_summary}"
        ),
        LivePackageTurnGreenResult::RefusedNowByStage3 => {
            "the frozen controller stays refused now by Stage 3; rerun this turn-green refresh after current Stage 3 truth turns green".to_string()
        }
        LivePackageTurnGreenResult::RefusedNowByPreActivationGate => {
            "the frozen controller stays refused now by the pre-activation gate; rerun this turn-green refresh after the current pre-activation gate turns green".to_string()
        }
        LivePackageTurnGreenResult::RefusedNowByInvalidOrDriftedContract => {
            "the frozen launch packet or current live contract drifted; repair the target or mint a new launch packet before considering execution".to_string()
        }
    }
}

fn turn_green_paths(session_dir: &Path) -> PackageTurnGreenPaths {
    PackageTurnGreenPaths {
        session_path: session_dir.join("tiny_live_activation_package_turn_green.session.json"),
        status_path: session_dir.join("tiny_live_activation_package_turn_green.status.json"),
        frozen_launch_packet_report_path: session_dir
            .join("tiny_live_activation_package_turn_green.launch_packet.report.json"),
        current_authorization_report_path: session_dir
            .join("tiny_live_activation_package_turn_green.current_authorization.report.json"),
        current_controller_report_path: session_dir
            .join("tiny_live_activation_package_turn_green.current_controller.report.json"),
        current_authorization_session_dir: session_dir
            .join("tiny_live_activation_package_turn_green.current_authorization_session"),
    }
}

fn validate_turn_green_session_dir(install_root: &Path, session_dir: &Path) -> Result<()> {
    if !install_root.is_absolute() {
        bail!("frozen install root must be absolute");
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

fn ensure_clean_turn_green_session_dir(session_dir: &Path) -> Result<()> {
    if session_dir.exists() {
        bail!(
            "refusing to overwrite existing turn-green session dir {}",
            session_dir.display()
        );
    }
    fs::create_dir_all(session_dir).with_context(|| {
        format!(
            "failed creating launch-packet-native turn-green session dir {}",
            session_dir.display()
        )
    })
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    value.ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn set_mode(mode: &mut Option<Mode>, value: Mode, flag: &str) -> Result<()> {
    if mode.replace(value).is_some() {
        bail!("multiple modes specified; latest was {flag}");
    }
    Ok(())
}

fn shell_escape_path(path: &Path) -> String {
    let raw = path.display().to_string();
    if raw
        .chars()
        .all(|value| value.is_ascii_alphanumeric() || "/._-".contains(value))
    {
        raw
    } else {
        format!("'{}'", raw.replace('\'', "'\"'\"'"))
    }
}

fn write_script(path: &Path, contents: &str) -> Result<()> {
    fs::write(path, contents)
        .with_context(|| format!("failed writing turn-green script to {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(path)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms)?;
    }
    Ok(())
}

fn persist_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_vec_pretty(value)?)
        .with_context(|| format!("failed writing {}", path.display()))
}

fn load_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let bytes = fs::read(path).with_context(|| format!("failed reading {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("failed parsing {}", path.display()))
}

fn build_step_artifact(
    path: &Path,
    mode: &str,
    verdict: &str,
    reason: &str,
    generated_at: DateTime<Utc>,
) -> PackageTurnGreenStepArtifact {
    PackageTurnGreenStepArtifact {
        path: path.display().to_string(),
        mode: mode.to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        generated_at,
    }
}

fn load_required_step_json<T: for<'de> Deserialize<'de>>(
    step: &Option<PackageTurnGreenStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<T> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("{label} is missing from turn-green status"))?;
    compare_string(
        &step.path,
        &expected_path.display().to_string(),
        &format!("{label} path"),
        mismatches,
    );
    load_json(expected_path)
}

fn compare_string(actual: &str, expected: &str, label: &str, mismatches: &mut Vec<String>) {
    if actual != expected {
        mismatches.push(format!(
            "{label} {actual:?} does not match expected {expected:?}"
        ));
    }
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

fn validate_stored_turn_green_session_contract(
    stored: &PackageTurnGreenSession,
    verified: &tiny_live_activation_package_launch_packet::LivePackageLaunchPacketContractView,
    current_authorization_session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &stored.package_dir,
        &verified.package_dir.display().to_string(),
        "stored turn-green session package_dir",
        mismatches,
    );
    compare_string(
        &stored.install_root,
        &verified.install_root.display().to_string(),
        "stored turn-green session install_root",
        mismatches,
    );
    compare_string(
        &stored.target_service_name,
        &verified.target_service_name,
        "stored turn-green session target_service_name",
        mismatches,
    );
    compare_string(
        &stored.backend_command,
        &verified.backend_command,
        "stored turn-green session backend_command",
        mismatches,
    );
    if stored.wrapper_timeout_ms != verified.wrapper_timeout_ms {
        mismatches.push(format!(
            "stored turn-green session wrapper_timeout_ms {} does not match verified {}",
            stored.wrapper_timeout_ms, verified.wrapper_timeout_ms
        ));
    }
    if stored.service_status_max_staleness_ms != verified.service_status_max_staleness_ms {
        mismatches.push(format!(
            "stored turn-green session service_status_max_staleness_ms {} does not match verified {}",
            stored.service_status_max_staleness_ms,
            verified.service_status_max_staleness_ms
        ));
    }
    compare_string(
        &stored.verify_frozen_launch_packet_summary,
        &verify_frozen_launch_packet_summary(&verified.launch_packet_session_dir),
        "stored turn-green session verify_frozen_launch_packet_summary",
        mismatches,
    );
    compare_string(
        &stored.refresh_current_authorization_command_summary,
        &refresh_current_authorization_command_summary(verified, current_authorization_session_dir),
        "stored turn-green session refresh_current_authorization_command_summary",
        mismatches,
    );
    compare_string(
        &stored.frozen_live_cutover_controller_command_summary,
        &verified.frozen_live_cutover_controller_command_summary,
        "stored turn-green session frozen_live_cutover_controller_command_summary",
        mismatches,
    );
}

fn validate_stored_launch_packet_report(
    stored: &StoredLaunchPacketReportView,
    verified: &tiny_live_activation_package_launch_packet::LivePackageLaunchPacketContractView,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &stored.package_dir,
        &verified.package_dir.display().to_string(),
        "stored frozen launch packet package_dir",
        mismatches,
    );
    compare_string(
        &stored.install_root,
        &verified.install_root.display().to_string(),
        "stored frozen launch packet install_root",
        mismatches,
    );
    compare_string(
        &stored.target_service_name,
        &verified.target_service_name,
        "stored frozen launch packet target_service_name",
        mismatches,
    );
    compare_string(
        &stored.backend_command,
        &verified.backend_command,
        "stored frozen launch packet backend_command",
        mismatches,
    );
    if stored.wrapper_timeout_ms != verified.wrapper_timeout_ms {
        mismatches.push(format!(
            "stored frozen launch packet wrapper_timeout_ms {} does not match verified {}",
            stored.wrapper_timeout_ms, verified.wrapper_timeout_ms
        ));
    }
    if stored.service_status_max_staleness_ms != verified.service_status_max_staleness_ms {
        mismatches.push(format!(
            "stored frozen launch packet service_status_max_staleness_ms {} does not match verified {}",
            stored.service_status_max_staleness_ms, verified.service_status_max_staleness_ms
        ));
    }
    compare_string(
        stored.session_dir.as_deref().unwrap_or_default(),
        &verified.launch_packet_session_dir.display().to_string(),
        "stored frozen launch packet session_dir",
        mismatches,
    );
    if stored.result != verified.frozen_result {
        mismatches.push(format!(
            "stored frozen launch packet result {:?} does not match verified {:?}",
            stored.result, verified.frozen_result
        ));
    }
    if stored.authorization_result_now != verified.authorization_result_now {
        mismatches.push(format!(
            "stored frozen launch packet authorization_result_now {:?} does not match verified {:?}",
            stored.authorization_result_now, verified.authorization_result_now
        ));
    }
    if stored.activation_authorized != verified.activation_authorized {
        mismatches.push(format!(
            "stored frozen launch packet activation_authorized {} does not match verified {}",
            stored.activation_authorized, verified.activation_authorized
        ));
    }
    compare_string(
        &stored.run_live_authorization_command_summary,
        &verified.run_live_authorization_command_summary,
        "stored frozen launch packet run_live_authorization_command_summary",
        mismatches,
    );
    compare_string(
        &stored.frozen_live_cutover_controller_command_summary,
        &verified.frozen_live_cutover_controller_command_summary,
        "stored frozen launch packet frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_string(
        &stored.operator_next_action_summary,
        &verified.operator_next_action_summary,
        "stored frozen launch packet operator_next_action_summary",
        mismatches,
    );
}

fn validate_stored_current_authorization_report(
    stored: &StoredAuthorizationRefreshReportView,
    frozen: &tiny_live_activation_package_launch_packet::LivePackageLaunchPacketContractView,
    current_controller_view: &tiny_live_activation_package_live_cutover::LiveCutoverControllerPlanView,
    current_authorization_session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &stored.package_dir,
        &frozen.package_dir.display().to_string(),
        "stored current authorization package_dir",
        mismatches,
    );
    compare_string(
        &stored.install_root,
        &frozen.install_root.display().to_string(),
        "stored current authorization install_root",
        mismatches,
    );
    compare_string(
        &stored.target_service_name,
        &frozen.target_service_name,
        "stored current authorization target_service_name",
        mismatches,
    );
    compare_string(
        &stored.backend_command,
        &frozen.backend_command,
        "stored current authorization backend_command",
        mismatches,
    );
    if stored.wrapper_timeout_ms != frozen.wrapper_timeout_ms {
        mismatches.push(format!(
            "stored current authorization wrapper_timeout_ms {} does not match frozen {}",
            stored.wrapper_timeout_ms, frozen.wrapper_timeout_ms
        ));
    }
    if stored.service_status_max_staleness_ms != frozen.service_status_max_staleness_ms {
        mismatches.push(format!(
            "stored current authorization service_status_max_staleness_ms {} does not match frozen {}",
            stored.service_status_max_staleness_ms,
            frozen.service_status_max_staleness_ms
        ));
    }
    compare_string(
        stored.session_dir.as_deref().unwrap_or_default(),
        &current_authorization_session_dir.display().to_string(),
        "stored current authorization session_dir",
        mismatches,
    );
    compare_string(
        &stored.gate_evaluation_command_summary,
        &current_controller_view.gate_evaluation_command_summary,
        "stored current authorization gate_evaluation_command_summary",
        mismatches,
    );
    compare_string(
        &stored.authorized_live_cutover_command_summary,
        &current_controller_view.run_live_cutover_command_summary,
        "stored current authorization authorized_live_cutover_command_summary",
        mismatches,
    );
}

fn failure_report_without_frozen_contract(
    config: &Config,
    verdict: TinyLivePackageTurnGreenVerdict,
    reason: String,
) -> PackageTurnGreenReport {
    PackageTurnGreenReport {
        generated_at: Utc::now(),
        mode: match config.mode {
            Mode::PlanLivePackageTurnGreen => "plan_live_package_turn_green".to_string(),
            Mode::RenderLivePackageTurnGreenScript => {
                "render_live_package_turn_green_script".to_string()
            }
            Mode::RunLivePackageTurnGreen => "run_live_package_turn_green".to_string(),
            Mode::VerifyLivePackageTurnGreen => "verify_live_package_turn_green".to_string(),
        },
        verdict,
        reason,
        launch_packet_session_dir: config.launch_packet_session_dir.display().to_string(),
        package_dir: None,
        install_root: None,
        target_service_name: None,
        backend_command: None,
        wrapper_timeout_ms: None,
        service_status_max_staleness_ms: None,
        session_dir: config
            .session_dir
            .as_ref()
            .map(|value| value.display().to_string()),
        turn_green_session_path: None,
        turn_green_status_path: None,
        nested_current_authorization_session_dir: None,
        result: Some("refused_now_by_invalid_or_drifted_contract".to_string()),
        frozen_launch_packet_result: None,
        current_authorization_result_now: None,
        frozen_launch_packet_step: None,
        current_authorization_step: None,
        current_controller_step: None,
        frozen_controller_still_current: false,
        verify_frozen_launch_packet_summary: Some(verify_frozen_launch_packet_summary(
            &config.launch_packet_session_dir,
        )),
        refresh_current_authorization_command_summary: None,
        frozen_live_cutover_controller_command_summary: None,
        current_live_cutover_controller_command_summary: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        operator_next_action_summary: Some(
            "the immutable launch packet could not be verified, so the frozen controller is not executable now"
                .to_string(),
        ),
        verification_mismatches: Vec::new(),
        executable_now: false,
        explicit_statement:
            "this turn-green refresh stays non-executing; any failure here refuses the frozen controller without mutating the real target"
                .to_string(),
    }
}

fn fallback_verified_frozen_step(
    launch_packet_session_dir: &Path,
) -> tiny_live_activation_package_launch_packet::VerifiedLivePackageLaunchPacketTurnGreenStep {
    tiny_live_activation_package_launch_packet::VerifiedLivePackageLaunchPacketTurnGreenStep {
        report_json: serde_json::json!({}),
        verdict: "tiny_live_package_launch_packet_verify_invalid".to_string(),
        reason: "failed to verify frozen launch packet".to_string(),
        generated_at: Utc::now(),
        contract: tiny_live_activation_package_launch_packet::LivePackageLaunchPacketContractView {
            launch_packet_session_dir: launch_packet_session_dir.to_path_buf(),
            package_dir: PathBuf::new(),
            install_root: PathBuf::new(),
            target_service_name: String::new(),
            backend_command: String::new(),
            wrapper_timeout_ms: 0,
            service_status_max_staleness_ms: 0,
            frozen_result: None,
            authorization_result_now: None,
            activation_authorized: false,
            current_pre_activation_gate_verdict: None,
            current_pre_activation_gate_reason: None,
            run_live_authorization_command_summary: String::new(),
            frozen_live_cutover_controller_command_summary: String::new(),
            operator_next_action_summary: String::new(),
            explicit_statement: String::new(),
            controller_plan_view: serde_json::from_value(serde_json::json!({
                "mode": "",
                "verdict": "",
                "reason": "",
                "package_dir": "",
                "install_root": "",
                "target_service_name": "",
                "backend_command": "",
                "wrapper_timeout_ms": 0,
                "service_status_max_staleness_ms": 0,
                "run_preflight_command_summary": "",
                "gate_evaluation_command_summary": "",
                "backup_proof_command_summary": "",
                "run_live_cutover_command_summary": "",
                "explicit_statement": ""
            }))
            .expect("fallback launch packet controller plan view"),
        },
    }
}

fn fallback_current_authorization_step(
) -> tiny_live_activation_package_live_authorization::PackageLiveAuthorizationLaunchPacketStep {
    tiny_live_activation_package_live_authorization::PackageLiveAuthorizationLaunchPacketStep {
        report_json: serde_json::json!({}),
        verdict: "tiny_live_package_live_authorization_verify_invalid".to_string(),
        reason: "failed to verify nested current authorization refresh".to_string(),
        generated_at: Utc::now(),
        result: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        gate_evaluation_command_summary: String::new(),
        authorized_live_cutover_command_summary: String::new(),
        activation_authorized: false,
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

fn serialize_enum<T: Serialize>(value: &T) -> String {
    match serde_json::to_value(value) {
        Ok(serde_json::Value::String(raw)) => raw,
        Ok(other) => other.to_string(),
        Err(_) => "<serialization-error>".to_string(),
    }
}

fn render_report_lines(report: &PackageTurnGreenReport) -> String {
    let mut lines = vec![
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!(
            "launch_packet_session_dir={}",
            report.launch_packet_session_dir
        ),
    ];
    if let Some(value) = &report.package_dir {
        lines.push(format!("package_dir={value}"));
    }
    if let Some(value) = &report.install_root {
        lines.push(format!("install_root={value}"));
    }
    if let Some(value) = &report.target_service_name {
        lines.push(format!("target_service_name={value}"));
    }
    if let Some(value) = &report.backend_command {
        lines.push(format!("backend_command={value}"));
    }
    if let Some(value) = report.wrapper_timeout_ms {
        lines.push(format!("wrapper_timeout_ms={value}"));
    }
    if let Some(value) = report.service_status_max_staleness_ms {
        lines.push(format!("service_status_max_staleness_ms={value}"));
    }
    if let Some(value) = &report.result {
        lines.push(format!("result={value}"));
    }
    lines.push(format!("executable_now={}", report.executable_now));
    if let Some(value) = &report.current_pre_activation_gate_verdict {
        lines.push(format!("current_pre_activation_gate_verdict={value}"));
    }
    if let Some(value) = &report.current_pre_activation_gate_reason {
        lines.push(format!("current_pre_activation_gate_reason={value}"));
    }
    if let Some(value) = &report.operator_next_action_summary {
        lines.push(format!("operator_next_action_summary={value}"));
    }
    if !report.verification_mismatches.is_empty() {
        lines.push("verification_mismatches:".to_string());
        lines.extend(
            report
                .verification_mismatches
                .iter()
                .map(|entry| format!("  - {entry}")),
        );
    }
    lines.push(format!("explicit_statement={}", report.explicit_statement));
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use copybot_config::load_from_path;
    use rusqlite::{params, Connection};
    use sha2::{Digest, Sha256};
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tiny_live_activation_live_execute::tiny_live_activation_execute::RenderedConfigMetadata;

    #[test]
    fn green_gate_yields_executable_now() {
        let fixture = turn_green_fixture(
            "tiny_live_activation_package_turn_green_green",
            GateState::Green,
        );
        let report = run_json_report(fixture.config.clone());
        assert_eq!(
            report.verdict,
            TinyLivePackageTurnGreenVerdict::TinyLivePackageTurnGreenExecutableNow
        );
        assert_eq!(report.result.as_deref(), Some("executable_now"));
        assert!(report.executable_now);
    }

    #[test]
    fn green_run_then_verify_stays_executable_now() {
        let fixture = turn_green_fixture(
            "tiny_live_activation_package_turn_green_verify_green",
            GateState::Green,
        );
        let report = run_json_report(fixture.config.clone());
        assert_eq!(
            report.verdict,
            TinyLivePackageTurnGreenVerdict::TinyLivePackageTurnGreenExecutableNow
        );

        let verify = verify_live_package_turn_green_report(&fixture.config).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageTurnGreenVerdict::TinyLivePackageTurnGreenVerifyOk,
            "{:#?}",
            verify.verification_mismatches
        );
        assert_eq!(verify.result.as_deref(), Some("executable_now"));
        assert!(verify.executable_now);
    }

    #[test]
    fn session_dir_overlapping_managed_live_target_path_is_rejected_without_writing_turn_green_artifacts(
    ) {
        let fixture = turn_green_fixture(
            "tiny_live_activation_package_turn_green_session_under_root",
            GateState::Green,
        );
        let managed_surface =
            tiny_live_activation_install_target::derive_install_target_managed_surface_paths(
                &fixture.fixture_dir.join("live-root"),
            )
            .unwrap();
        let unsafe_session_dir = managed_surface.session_dir.join("turn-green-session");
        let config = Config {
            session_dir: Some(unsafe_session_dir.clone()),
            ..fixture.config.clone()
        };

        let error =
            run(config).expect_err("session dir overlapping managed live-target path must fail");
        assert!(
            error.to_string().contains("must stay disjoint from"),
            "{error:#}"
        );

        let paths = turn_green_paths(&unsafe_session_dir);
        assert!(!unsafe_session_dir.exists());
        assert!(!paths.session_path.exists());
        assert!(!paths.status_path.exists());
        assert!(!paths.frozen_launch_packet_report_path.exists());
        assert!(!paths.current_authorization_report_path.exists());
        assert!(!paths.current_controller_report_path.exists());
        assert!(!paths.current_authorization_session_dir.exists());
    }

    #[test]
    fn session_dir_disjoint_from_managed_surface_is_allowed_when_install_root_is_root() {
        let temp_root = temp_dir("tiny_live_activation_package_turn_green_root_safe_session");
        let session_dir = temp_root.join("turn-green-session");
        validate_turn_green_session_dir(Path::new("/"), &session_dir).expect(
            "disjoint /tmp-style session dir should stay allowed when frozen install_root=/",
        );
    }

    #[test]
    fn session_dir_overlapping_managed_live_target_path_is_rejected_when_install_root_is_root() {
        let managed_surface =
            tiny_live_activation_install_target::derive_install_target_managed_surface_paths(
                Path::new("/"),
            )
            .unwrap();
        let overlapping_session_dir = managed_surface.session_dir.join("turn-green-session");
        let error = validate_turn_green_session_dir(Path::new("/"), &overlapping_session_dir)
            .expect_err("managed live-target overlap must be rejected");
        assert!(
            error.to_string().contains("must stay disjoint from"),
            "{error:#}"
        );
    }

    #[test]
    fn verify_rejects_session_dir_overlapping_managed_live_target_path() {
        let fixture = turn_green_fixture(
            "tiny_live_activation_package_turn_green_verify_session_under_root",
            GateState::Green,
        );
        let managed_surface =
            tiny_live_activation_install_target::derive_install_target_managed_surface_paths(
                &fixture.fixture_dir.join("live-root"),
            )
            .unwrap();
        let unsafe_session_dir = managed_surface.session_dir.join("turn-green-session");
        let config = Config {
            mode: Mode::VerifyLivePackageTurnGreen,
            session_dir: Some(unsafe_session_dir),
            ..fixture.config
        };

        let error = verify_live_package_turn_green_report(&config)
            .expect_err("verify must reject session dir overlapping managed live-target path");
        assert!(
            error.to_string().contains("must stay disjoint from"),
            "{error:#}"
        );
    }

    #[test]
    fn current_stage3_non_green_yields_refused_now() {
        let fixture = turn_green_fixture(
            "tiny_live_activation_package_turn_green_stage3",
            GateState::Stage3Blocked,
        );
        let report = run_json_report(fixture.config.clone());
        assert_eq!(
            report.verdict,
            TinyLivePackageTurnGreenVerdict::TinyLivePackageTurnGreenRefusedNowByStage3
        );
        assert_eq!(report.result.as_deref(), Some("refused_now_by_stage3"));
        assert!(!report.executable_now);
    }

    #[test]
    fn current_pre_activation_non_green_yields_refused_now() {
        let fixture = turn_green_fixture(
            "tiny_live_activation_package_turn_green_pregate",
            GateState::PreActivationBlocked,
        );
        let report = run_json_report(fixture.config.clone());
        assert_eq!(
            report.verdict,
            TinyLivePackageTurnGreenVerdict::TinyLivePackageTurnGreenRefusedNowByPreActivationGate
        );
        assert_eq!(
            report.result.as_deref(),
            Some("refused_now_by_pre_activation_gate")
        );
        assert!(!report.executable_now);
    }

    #[test]
    fn tampered_launch_packet_controller_summary_yields_invalid_or_drifted_refusal() {
        let fixture = turn_green_fixture(
            "tiny_live_activation_package_turn_green_tampered_packet",
            GateState::Green,
        );
        let launch_packet_paths =
            tiny_live_activation_package_launch_packet_paths(&fixture.launch_packet_session_dir);
        let mut controller_view: tiny_live_activation_package_live_cutover::LiveCutoverControllerPlanView =
            load_json(&launch_packet_paths.controller_report_path).unwrap();
        controller_view.run_live_cutover_command_summary = "tampered cutover command".to_string();
        persist_json(
            &launch_packet_paths.controller_report_path,
            &controller_view,
        )
        .unwrap();

        let report = run_json_report(fixture.config.clone());
        assert_eq!(
            report.verdict,
            TinyLivePackageTurnGreenVerdict::TinyLivePackageTurnGreenRefusedNowByInvalidOrDriftedContract
        );
        assert_eq!(
            report.result.as_deref(),
            Some("refused_now_by_invalid_or_drifted_contract")
        );
    }

    #[test]
    fn verify_rejects_tampered_launch_packet_step_path() {
        let fixture = turn_green_fixture(
            "tiny_live_activation_package_turn_green_tampered_step_path",
            GateState::Green,
        );
        run_json_report(fixture.config.clone());
        let paths = turn_green_paths(fixture.config.session_dir.as_ref().unwrap());
        let mut status: PackageTurnGreenStatus = load_json(&paths.status_path).unwrap();
        let foreign_path = fixture
            .fixture_dir
            .join("foreign.launch-packet.report.json");
        fs::write(&foreign_path, "{}").unwrap();
        status.frozen_launch_packet_step.as_mut().unwrap().path =
            foreign_path.display().to_string();
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_live_package_turn_green_report(&fixture.config).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageTurnGreenVerdict::TinyLivePackageTurnGreenVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_verify_frozen_launch_packet_summary() {
        let fixture = turn_green_fixture(
            "tiny_live_activation_package_turn_green_tampered_verify_summary",
            GateState::Green,
        );
        run_json_report(fixture.config.clone());
        let paths = turn_green_paths(fixture.config.session_dir.as_ref().unwrap());
        let mut session: PackageTurnGreenSession = load_json(&paths.session_path).unwrap();
        session.verify_frozen_launch_packet_summary = "tampered verify summary".to_string();
        persist_json(&paths.session_path, &session).unwrap();

        let verify = verify_live_package_turn_green_report(&fixture.config).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageTurnGreenVerdict::TinyLivePackageTurnGreenVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_refresh_current_authorization_command_summary() {
        let fixture = turn_green_fixture(
            "tiny_live_activation_package_turn_green_tampered_refresh_summary",
            GateState::Green,
        );
        run_json_report(fixture.config.clone());
        let paths = turn_green_paths(fixture.config.session_dir.as_ref().unwrap());
        let mut session: PackageTurnGreenSession = load_json(&paths.session_path).unwrap();
        session.refresh_current_authorization_command_summary =
            "tampered authorization refresh summary".to_string();
        persist_json(&paths.session_path, &session).unwrap();

        let verify = verify_live_package_turn_green_report(&fixture.config).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageTurnGreenVerdict::TinyLivePackageTurnGreenVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_frozen_live_cutover_controller_command_summary() {
        let fixture = turn_green_fixture(
            "tiny_live_activation_package_turn_green_tampered_controller_summary",
            GateState::Green,
        );
        run_json_report(fixture.config.clone());
        let paths = turn_green_paths(fixture.config.session_dir.as_ref().unwrap());
        let mut session: PackageTurnGreenSession = load_json(&paths.session_path).unwrap();
        session.frozen_live_cutover_controller_command_summary =
            "tampered live cutover controller summary".to_string();
        persist_json(&paths.session_path, &session).unwrap();

        let verify = verify_live_package_turn_green_report(&fixture.config).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageTurnGreenVerdict::TinyLivePackageTurnGreenVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_mismatched_launch_packet_session_dir() {
        let fixture = turn_green_fixture(
            "tiny_live_activation_package_turn_green_packet_mismatch",
            GateState::Green,
        );
        run_json_report(fixture.config.clone());
        let verify = verify_live_package_turn_green_report(&Config {
            launch_packet_session_dir: fixture.fixture_dir.join("other-launch-packet"),
            ..fixture.config
        })
        .unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageTurnGreenVerdict::TinyLivePackageTurnGreenVerifyInvalid
        );
    }

    #[derive(Debug, Clone, Copy)]
    enum GateState {
        Green,
        Stage3Blocked,
        PreActivationBlocked,
    }

    struct TurnGreenFixture {
        fixture_dir: PathBuf,
        launch_packet_session_dir: PathBuf,
        config: Config,
        _rpc_server: MockHttpServer,
        _adapter_server: MockHttpServer,
    }

    fn turn_green_fixture(label: &str, gate_state: GateState) -> TurnGreenFixture {
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

        let launch_packet_session_dir = fixture_dir.join("launch-packet-session");
        tiny_live_activation_package_launch_packet::run_live_package_launch_packet_for_turn_green(
            &package_dir,
            &install_root,
            "solana-copy-bot.service",
            &backend_path.display().to_string(),
            tiny_live_activation_live_execute::live_service_control_wrapper_contract::DEFAULT_TIMEOUT_MS,
            tiny_live_activation_package_preflight::DEFAULT_SERVICE_STATUS_MAX_STALENESS_MS,
            &launch_packet_session_dir,
        )
        .unwrap();

        TurnGreenFixture {
            fixture_dir: fixture_dir.clone(),
            launch_packet_session_dir: launch_packet_session_dir.clone(),
            config: Config {
                mode: Mode::RunLivePackageTurnGreen,
                launch_packet_session_dir,
                session_dir: Some(fixture_dir.join("turn-green-session")),
                output_path: None,
                json: false,
            },
            _rpc_server: rpc_server,
            _adapter_server: adapter_server,
        }
    }

    fn run_json_report(config: Config) -> PackageTurnGreenReport {
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
        tiny_live_activation_live_execute::tiny_live_activation_execute::inspect_rendered_config_artifact(path)
            .unwrap();
    }

    fn seed_current_gate_state(db_path: &Path, gate_state: GateState) {
        let conn = Connection::open(db_path).unwrap();
        conn.execute_batch(
            r#"
CREATE TABLE IF NOT EXISTS pre_activation_gate_history (
    captured_at_utc TEXT NOT NULL,
    top_level_gate_verdict TEXT NOT NULL,
    top_level_gate_reason TEXT NOT NULL,
    current_stage3_verdict TEXT NOT NULL,
    current_stage3_reason TEXT NOT NULL,
    stage4_readiness_verdict TEXT NOT NULL,
    stage4_readiness_reason TEXT NOT NULL,
    recent_rehearsal_summary TEXT NOT NULL,
    policy_verdict TEXT NOT NULL,
    policy_reason TEXT NOT NULL,
    readiness_json TEXT NOT NULL,
    stage3_json TEXT NOT NULL,
    rehearsal_json TEXT NOT NULL,
    policy_json TEXT NOT NULL
);
"#,
        )
        .unwrap();
        let now = Utc::now();
        let (top_verdict, top_reason, stage3_verdict, stage3_reason) = match gate_state {
            GateState::Green => (
                "pre_activation_gates_green",
                "green",
                "stage3_turnkey_green",
                "green",
            ),
            GateState::Stage3Blocked => (
                "pre_activation_gate_blocked",
                "stage3_blocked",
                "stage3_turnkey_blocked",
                "stage3_blocked",
            ),
            GateState::PreActivationBlocked => (
                "pre_activation_gate_blocked",
                "policy_blocked",
                "stage3_turnkey_green",
                "green",
            ),
        };
        let stage4_reason = match gate_state {
            GateState::PreActivationBlocked => "policy_blocked",
            _ => "green",
        };
        let policy_verdict = match gate_state {
            GateState::PreActivationBlocked => "tiny_live_policy_non_green",
            _ => "tiny_live_policy_green",
        };
        let policy_reason = match gate_state {
            GateState::PreActivationBlocked => "policy_blocked",
            _ => "green",
        };
        conn.execute(
            r#"
INSERT INTO pre_activation_gate_history (
    captured_at_utc,
    top_level_gate_verdict,
    top_level_gate_reason,
    current_stage3_verdict,
    current_stage3_reason,
    stage4_readiness_verdict,
    stage4_readiness_reason,
    recent_rehearsal_summary,
    policy_verdict,
    policy_reason,
    readiness_json,
    stage3_json,
    rehearsal_json,
    policy_json
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
"#,
            params![
                now.to_rfc3339(),
                top_verdict,
                top_reason,
                stage3_verdict,
                stage3_reason,
                "execution_ready_for_bounded_activation",
                stage4_reason,
                "recent_green_rehearsal_histories_present",
                policy_verdict,
                policy_reason,
                "{}",
                "{}",
                "{}",
                "{}"
            ],
        )
        .unwrap();
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

    fn tiny_live_activation_package_launch_packet_paths(
        session_dir: &Path,
    ) -> LaunchPacketPathsView {
        LaunchPacketPathsView {
            controller_report_path: session_dir
                .join("tiny_live_activation_package_launch_packet.controller.report.json"),
        }
    }

    struct LaunchPacketPathsView {
        controller_report_path: PathBuf,
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
