use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_app::tiny_live_activation::package_live_cutover_execute_frozen as live_cutover_execute_frozen;
use copybot_app::tiny_live_activation::package_turn_green_execute_frozen as turn_green_execute_frozen;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: copybot_tiny_live_activation_package_execute_frozen --turn-green-session-dir <path> [--session-dir <path>] [--output <path>] [--json] (--plan-live-package-execute-frozen | --render-live-package-execute-frozen-script | --run-live-package-execute-frozen | --verify-live-package-execute-frozen)";
const EXECUTE_FROZEN_SESSION_VERSION: &str = "1";
const EXECUTE_FROZEN_STATUS_VERSION: &str = "1";

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
    turn_green_session_dir: PathBuf,
    session_dir: Option<PathBuf>,
    output_path: Option<PathBuf>,
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    PlanLivePackageExecuteFrozen,
    RenderLivePackageExecuteFrozenScript,
    RunLivePackageExecuteFrozen,
    VerifyLivePackageExecuteFrozen,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageExecuteFrozenVerdict {
    TinyLivePackageExecuteFrozenPlanReady,
    TinyLivePackageExecuteFrozenScriptRendered,
    TinyLivePackageExecuteFrozenRefusedNowByStage3,
    TinyLivePackageExecuteFrozenRefusedNowByPreActivationGate,
    TinyLivePackageExecuteFrozenRefusedNowByInvalidOrDriftedContract,
    TinyLivePackageExecuteFrozenRunAllowed,
    TinyLivePackageExecuteFrozenCompletedKeepRunning,
    TinyLivePackageExecuteFrozenCompletedWithRollback,
    TinyLivePackageExecuteFrozenCompletedBackupFailed,
    TinyLivePackageExecuteFrozenCompletedApplyFailed,
    TinyLivePackageExecuteFrozenCompletedWatchFailed,
    TinyLivePackageExecuteFrozenVerifyOk,
    TinyLivePackageExecuteFrozenVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LivePackageExecuteFrozenResult {
    RefusedNowByStage3,
    RefusedNowByPreActivationGate,
    RefusedNowByInvalidOrDriftedContract,
    CompletedKeepRunning,
    CompletedWithRollback,
    CompletedBackupFailed,
    CompletedApplyFailed,
    CompletedWatchFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageExecuteFrozenStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageExecuteFrozenSession {
    session_version: String,
    planned_at: DateTime<Utc>,
    turn_green_session_dir: String,
    launch_packet_session_dir: String,
    package_dir: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    session_dir: String,
    nested_live_cutover_session_dir: String,
    verify_turn_green_command_summary: String,
    execute_frozen_command_summary: String,
    frozen_live_cutover_controller_command_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageExecuteFrozenStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    session_dir: String,
    turn_green_session_dir: String,
    nested_live_cutover_session_dir: String,
    result: LivePackageExecuteFrozenResult,
    reason: String,
    turn_green_result_now: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    turn_green_step: Option<PackageExecuteFrozenStepArtifact>,
    live_cutover_step: Option<PackageExecuteFrozenStepArtifact>,
    operator_next_action_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackageExecuteFrozenPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    turn_green_report_path: PathBuf,
    live_cutover_report_path: PathBuf,
    nested_live_cutover_session_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageExecuteFrozenReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageExecuteFrozenVerdict,
    reason: String,
    turn_green_session_dir: String,
    launch_packet_session_dir: Option<String>,
    package_dir: Option<String>,
    install_root: Option<String>,
    target_service_name: Option<String>,
    backend_command: Option<String>,
    wrapper_timeout_ms: Option<u64>,
    service_status_max_staleness_ms: Option<u64>,
    session_dir: Option<String>,
    execute_frozen_session_path: Option<String>,
    execute_frozen_status_path: Option<String>,
    nested_live_cutover_session_dir: Option<String>,
    result: Option<String>,
    turn_green_result_now: Option<String>,
    turn_green_step: Option<PackageExecuteFrozenStepArtifact>,
    live_cutover_step: Option<PackageExecuteFrozenStepArtifact>,
    verify_turn_green_command_summary: Option<String>,
    execute_frozen_command_summary: Option<String>,
    frozen_live_cutover_controller_command_summary: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    verification_mismatches: Vec<String>,
    execution_happened: bool,
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
    let mut turn_green_session_dir: Option<PathBuf> = None;
    let mut session_dir: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut json = false;
    let mut mode: Option<Mode> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--turn-green-session-dir" => {
                turn_green_session_dir = Some(PathBuf::from(parse_string_arg(
                    "--turn-green-session-dir",
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
            "--plan-live-package-execute-frozen" => {
                set_mode(&mut mode, Mode::PlanLivePackageExecuteFrozen, arg.as_str())?
            }
            "--render-live-package-execute-frozen-script" => set_mode(
                &mut mode,
                Mode::RenderLivePackageExecuteFrozenScript,
                arg.as_str(),
            )?,
            "--run-live-package-execute-frozen" => {
                set_mode(&mut mode, Mode::RunLivePackageExecuteFrozen, arg.as_str())?
            }
            "--verify-live-package-execute-frozen" => set_mode(
                &mut mode,
                Mode::VerifyLivePackageExecuteFrozen,
                arg.as_str(),
            )?,
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    let Some(mode) = mode else {
        return Ok(None);
    };
    if matches!(mode, Mode::RenderLivePackageExecuteFrozenScript) && output_path.is_none() {
        bail!("missing --output for --render-live-package-execute-frozen-script");
    }
    if matches!(
        mode,
        Mode::RunLivePackageExecuteFrozen | Mode::VerifyLivePackageExecuteFrozen
    ) && session_dir.is_none()
    {
        bail!("missing --session-dir for run/verify mode");
    }
    let turn_green_session_dir =
        turn_green_session_dir.ok_or_else(|| anyhow!("missing --turn-green-session-dir"))?;
    Ok(Some(Config {
        mode,
        turn_green_session_dir,
        session_dir,
        output_path,
        json,
    }))
}

fn run(config: Config) -> Result<String> {
    let report = match config.mode {
        Mode::PlanLivePackageExecuteFrozen => plan_live_package_execute_frozen_report(&config)?,
        Mode::RenderLivePackageExecuteFrozenScript => {
            render_live_package_execute_frozen_report(&config)?
        }
        Mode::RunLivePackageExecuteFrozen => run_live_package_execute_frozen_report(&config)?,
        Mode::VerifyLivePackageExecuteFrozen => verify_live_package_execute_frozen_report(&config)?,
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_report_lines(&report))
    }
}

fn plan_live_package_execute_frozen_report(config: &Config) -> Result<PackageExecuteFrozenReport> {
    let frozen =
        turn_green_execute_frozen::load_live_package_turn_green_contract_for_execute_frozen(
            &config.turn_green_session_dir,
        )?;
    Ok(PackageExecuteFrozenReport {
        generated_at: Utc::now(),
        mode: "plan_live_package_execute_frozen".to_string(),
        verdict: TinyLivePackageExecuteFrozenVerdict::TinyLivePackageExecuteFrozenPlanReady,
        reason: format!(
            "turn-green-native frozen execution handoff is explicit for verified turn-green session {}; this command reuses the frozen live cutover controller without manual argument restitching",
            config.turn_green_session_dir.display()
        ),
        turn_green_session_dir: config.turn_green_session_dir.display().to_string(),
        launch_packet_session_dir: Some(frozen.launch_packet_session_dir.display().to_string()),
        package_dir: Some(frozen.package_dir.display().to_string()),
        install_root: Some(frozen.install_root.display().to_string()),
        target_service_name: Some(frozen.target_service_name),
        backend_command: Some(frozen.backend_command),
        wrapper_timeout_ms: Some(frozen.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(frozen.service_status_max_staleness_ms),
        session_dir: config
            .session_dir
            .as_ref()
            .map(|value| value.display().to_string()),
        execute_frozen_session_path: None,
        execute_frozen_status_path: None,
        nested_live_cutover_session_dir: config.session_dir.as_ref().map(|value| {
            execute_frozen_paths(value)
                .nested_live_cutover_session_dir
                .display()
                .to_string()
        }),
        result: frozen.result.clone(),
        turn_green_result_now: frozen.result.clone(),
        turn_green_step: None,
        live_cutover_step: None,
        verify_turn_green_command_summary: Some(verify_turn_green_command_summary(
            &config.turn_green_session_dir,
        )),
        execute_frozen_command_summary: config
            .session_dir
            .as_ref()
            .map(|value| run_execute_frozen_command_summary(&config.turn_green_session_dir, value)),
        frozen_live_cutover_controller_command_summary: Some(
            frozen.frozen_live_cutover_controller_command_summary,
        ),
        current_pre_activation_gate_verdict: frozen.current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason: frozen.current_pre_activation_gate_reason,
        verification_mismatches: Vec::new(),
        execution_happened: false,
        explicit_statement:
            "this handoff command binds one verified turn-green session to one exact frozen live cutover execution path; it never substitutes target, service, backend, or package truth from ad-hoc arguments"
                .to_string(),
    })
}

fn render_live_package_execute_frozen_report(
    config: &Config,
) -> Result<PackageExecuteFrozenReport> {
    let plan = plan_live_package_execute_frozen_report(config)?;
    let output_path = config
        .output_path
        .as_ref()
        .ok_or_else(|| anyhow!("missing --output for render mode"))?;
    let session_dir = config
        .session_dir
        .clone()
        .unwrap_or_else(|| output_path.with_extension("session"));
    let script = format!(
        "#!/usr/bin/env bash\nset -euo pipefail\n\ncopybot_tiny_live_activation_package_execute_frozen --turn-green-session-dir {} --plan-live-package-execute-frozen\ncopybot_tiny_live_activation_package_execute_frozen --turn-green-session-dir {} --session-dir {} --run-live-package-execute-frozen\ncopybot_tiny_live_activation_package_execute_frozen --turn-green-session-dir {} --session-dir {} --verify-live-package-execute-frozen\n",
        shell_escape_path(&config.turn_green_session_dir),
        shell_escape_path(&config.turn_green_session_dir),
        shell_escape_path(&session_dir),
        shell_escape_path(&config.turn_green_session_dir),
        shell_escape_path(&session_dir),
    );
    write_script(output_path, &script)?;
    Ok(PackageExecuteFrozenReport {
        generated_at: Utc::now(),
        mode: "render_live_package_execute_frozen_script".to_string(),
        verdict: TinyLivePackageExecuteFrozenVerdict::TinyLivePackageExecuteFrozenScriptRendered,
        reason: format!(
            "rendered turn-green-native frozen execution handoff script to {}",
            output_path.display()
        ),
        turn_green_session_dir: plan.turn_green_session_dir,
        launch_packet_session_dir: plan.launch_packet_session_dir,
        package_dir: plan.package_dir,
        install_root: plan.install_root,
        target_service_name: plan.target_service_name,
        backend_command: plan.backend_command,
        wrapper_timeout_ms: plan.wrapper_timeout_ms,
        service_status_max_staleness_ms: plan.service_status_max_staleness_ms,
        session_dir: Some(session_dir.display().to_string()),
        execute_frozen_session_path: None,
        execute_frozen_status_path: None,
        nested_live_cutover_session_dir: Some(
            execute_frozen_paths(&session_dir)
                .nested_live_cutover_session_dir
                .display()
                .to_string(),
        ),
        result: plan.result,
        turn_green_result_now: plan.turn_green_result_now,
        turn_green_step: None,
        live_cutover_step: None,
        verify_turn_green_command_summary: plan.verify_turn_green_command_summary,
        execute_frozen_command_summary: Some(run_execute_frozen_command_summary(
            &config.turn_green_session_dir,
            &session_dir,
        )),
        frozen_live_cutover_controller_command_summary: plan
            .frozen_live_cutover_controller_command_summary,
        current_pre_activation_gate_verdict: plan.current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason: plan.current_pre_activation_gate_reason,
        verification_mismatches: Vec::new(),
        execution_happened: false,
        explicit_statement:
            "this rendered script accepts only the verified turn-green session as handoff input; it never asks the operator to restitch package, target, or controller arguments"
                .to_string(),
    })
}

fn run_live_package_execute_frozen_report(config: &Config) -> Result<PackageExecuteFrozenReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for run mode"))?;
    let verified_turn_green =
        match turn_green_execute_frozen::verify_live_package_turn_green_for_execute_frozen(
            &config.turn_green_session_dir,
        ) {
            Ok(value) => value,
            Err(error) => {
                return Ok(failure_report_without_contract(
                    config,
                    TinyLivePackageExecuteFrozenVerdict::TinyLivePackageExecuteFrozenRefusedNowByInvalidOrDriftedContract,
                    format!(
                        "failed to verify frozen turn-green session under {}: {error}",
                        config.turn_green_session_dir.display()
                    ),
                ))
            }
        };
    turn_green_execute_frozen::validate_turn_green_session_dir(
        &verified_turn_green.contract.install_root,
        session_dir,
    )?;
    ensure_clean_execute_frozen_session_dir(session_dir)?;
    let paths = execute_frozen_paths(session_dir);
    let session = planned_session(config, session_dir, &paths, &verified_turn_green.contract);
    persist_json(&paths.session_path, &session)?;
    persist_json(
        &paths.turn_green_report_path,
        &verified_turn_green.report_json,
    )?;
    let turn_green_step = Some(step_artifact(
        &paths.turn_green_report_path,
        "verify_live_package_turn_green",
        &verified_turn_green.verdict,
        &verified_turn_green.reason,
        verified_turn_green.generated_at,
    ));

    if let Some((refusal_verdict, refusal_result, refusal_reason)) =
        classify_turn_green_refusal(&verified_turn_green)
    {
        let status = refusal_status(
            session_dir,
            &config.turn_green_session_dir,
            &paths.nested_live_cutover_session_dir,
            refusal_result,
            refusal_reason,
            verified_turn_green.contract.result.clone(),
            verified_turn_green
                .contract
                .current_pre_activation_gate_verdict
                .clone(),
            verified_turn_green
                .contract
                .current_pre_activation_gate_reason
                .clone(),
            verified_turn_green
                .contract
                .operator_next_action_summary
                .clone(),
        );
        let mut status = status;
        status.turn_green_step = turn_green_step.clone();
        persist_json(&paths.status_path, &status)?;
        return Ok(report_from_status(
            config,
            &verified_turn_green.contract,
            session_dir,
            &paths,
            refusal_verdict,
            &status,
        ));
    }

    let nested_live_cutover =
        live_cutover_execute_frozen::run_live_package_cutover_for_execute_frozen(
            &verified_turn_green.contract.package_dir,
            &verified_turn_green.contract.install_root,
            &verified_turn_green.contract.target_service_name,
            &verified_turn_green.contract.backend_command,
            verified_turn_green.contract.wrapper_timeout_ms,
            verified_turn_green.contract.service_status_max_staleness_ms,
            &paths.nested_live_cutover_session_dir,
        )?;
    persist_json(
        &paths.live_cutover_report_path,
        &nested_live_cutover.report_json,
    )?;
    let live_cutover_step = Some(step_artifact(
        &paths.live_cutover_report_path,
        "run_live_package_live_cutover",
        &nested_live_cutover.verdict,
        &nested_live_cutover.reason,
        nested_live_cutover.generated_at,
    ));
    let (result, verdict, reason) = map_live_cutover_outcome(&nested_live_cutover);
    let status = PackageExecuteFrozenStatus {
        status_version: EXECUTE_FROZEN_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        session_dir: session_dir.display().to_string(),
        turn_green_session_dir: config.turn_green_session_dir.display().to_string(),
        nested_live_cutover_session_dir: paths
            .nested_live_cutover_session_dir
            .display()
            .to_string(),
        result,
        reason: reason.clone(),
        turn_green_result_now: verified_turn_green.contract.result.clone(),
        current_pre_activation_gate_verdict: nested_live_cutover
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: nested_live_cutover
            .current_pre_activation_gate_reason
            .clone(),
        turn_green_step,
        live_cutover_step,
        operator_next_action_summary: operator_next_action_summary(
            result,
            &verified_turn_green
                .contract
                .frozen_live_cutover_controller_command_summary,
        ),
        explicit_statement:
            "this turn-green-native handoff executed only the exact frozen live cutover controller contract verified by the nested turn-green session"
                .to_string(),
    };
    persist_json(&paths.status_path, &status)?;
    Ok(report_from_status(
        config,
        &verified_turn_green.contract,
        session_dir,
        &paths,
        verdict,
        &status,
    ))
}

fn verify_live_package_execute_frozen_report(
    config: &Config,
) -> Result<PackageExecuteFrozenReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for verify mode"))?;
    let paths = execute_frozen_paths(session_dir);
    let session: PackageExecuteFrozenSession = load_json(&paths.session_path)?;
    let status: PackageExecuteFrozenStatus = load_json(&paths.status_path)?;
    let mut mismatches = Vec::new();

    compare_string(
        &session.turn_green_session_dir,
        &config.turn_green_session_dir.display().to_string(),
        "execute-frozen turn_green_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "execute-frozen session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.session_dir,
        &session_dir.display().to_string(),
        "execute-frozen status session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.turn_green_session_dir,
        &config.turn_green_session_dir.display().to_string(),
        "execute-frozen status turn_green_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.nested_live_cutover_session_dir,
        &paths.nested_live_cutover_session_dir.display().to_string(),
        "execute-frozen nested_live_cutover_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.nested_live_cutover_session_dir,
        &paths.nested_live_cutover_session_dir.display().to_string(),
        "execute-frozen status nested_live_cutover_session_dir",
        &mut mismatches,
    );

    let verified_turn_green =
        match turn_green_execute_frozen::verify_live_package_turn_green_for_execute_frozen(
            &config.turn_green_session_dir,
        ) {
            Ok(value) => {
                turn_green_execute_frozen::validate_turn_green_session_dir(
                    &value.contract.install_root,
                    session_dir,
                )?;
                value
            }
            Err(error) => {
                let raw_contract = turn_green_execute_frozen::load_live_package_turn_green_contract_for_execute_frozen(
                    &config.turn_green_session_dir,
                )?;
                return Ok(failure_report_for_verify(
                    config,
                    session_dir,
                    &raw_contract,
                    vec![format!(
                        "failed to verify frozen turn-green session under {}: {error}",
                        config.turn_green_session_dir.display()
                    )],
                ));
            }
        };

    let stored_turn_green_report: serde_json::Value = load_required_step_json(
        &status.turn_green_step,
        &paths.turn_green_report_path,
        "turn_green_step",
        &mut mismatches,
    )?;
    let _ = stored_turn_green_report;
    compare_string(
        &session.launch_packet_session_dir,
        &verified_turn_green
            .contract
            .launch_packet_session_dir
            .display()
            .to_string(),
        "execute-frozen launch_packet_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.package_dir,
        &verified_turn_green
            .contract
            .package_dir
            .display()
            .to_string(),
        "execute-frozen package_dir",
        &mut mismatches,
    );
    compare_string(
        &session.install_root,
        &verified_turn_green
            .contract
            .install_root
            .display()
            .to_string(),
        "execute-frozen install_root",
        &mut mismatches,
    );
    compare_string(
        &session.target_service_name,
        &verified_turn_green.contract.target_service_name,
        "execute-frozen target_service_name",
        &mut mismatches,
    );
    compare_string(
        &session.backend_command,
        &verified_turn_green.contract.backend_command,
        "execute-frozen backend_command",
        &mut mismatches,
    );
    if session.wrapper_timeout_ms != verified_turn_green.contract.wrapper_timeout_ms {
        mismatches.push(format!(
            "execute-frozen wrapper_timeout_ms {} does not match verified {}",
            session.wrapper_timeout_ms, verified_turn_green.contract.wrapper_timeout_ms
        ));
    }
    if session.service_status_max_staleness_ms
        != verified_turn_green.contract.service_status_max_staleness_ms
    {
        mismatches.push(format!(
            "execute-frozen service_status_max_staleness_ms {} does not match verified {}",
            session.service_status_max_staleness_ms,
            verified_turn_green.contract.service_status_max_staleness_ms
        ));
    }
    compare_string(
        &session.verify_turn_green_command_summary,
        &verify_turn_green_command_summary(&config.turn_green_session_dir),
        "execute-frozen verify_turn_green_command_summary",
        &mut mismatches,
    );
    compare_string(
        &session.execute_frozen_command_summary,
        &run_execute_frozen_command_summary(&config.turn_green_session_dir, session_dir),
        "execute-frozen execute_frozen_command_summary",
        &mut mismatches,
    );
    compare_string(
        &session.frozen_live_cutover_controller_command_summary,
        &verified_turn_green
            .contract
            .frozen_live_cutover_controller_command_summary,
        "execute-frozen frozen_live_cutover_controller_command_summary",
        &mut mismatches,
    );
    if verified_turn_green.verdict != "tiny_live_package_turn_green_verify_ok" {
        mismatches.push(format!(
            "nested turn-green verification is non-green: {}",
            verified_turn_green.reason
        ));
    }

    let expected_result = if let Some(live_cutover_step) = &status.live_cutover_step {
        let verified_live_cutover =
            live_cutover_execute_frozen::verify_live_package_cutover_for_execute_frozen(
                &verified_turn_green.contract.package_dir,
                &verified_turn_green.contract.install_root,
                &verified_turn_green.contract.target_service_name,
                &verified_turn_green.contract.backend_command,
                verified_turn_green.contract.wrapper_timeout_ms,
                verified_turn_green.contract.service_status_max_staleness_ms,
                &paths.nested_live_cutover_session_dir,
            )?;
        let _: serde_json::Value = load_required_step_json(
            &Some(live_cutover_step.clone()),
            &paths.live_cutover_report_path,
            "live_cutover_step",
            &mut mismatches,
        )?;
        if verified_live_cutover.verdict != "tiny_live_package_live_cutover_verify_ok" {
            mismatches.push(format!(
                "nested live cutover verification is non-green: {}",
                verified_live_cutover.reason
            ));
        }
        let (mapped_result, _, _) = map_live_cutover_outcome(&verified_live_cutover);
        if status.current_pre_activation_gate_verdict
            != verified_live_cutover.current_pre_activation_gate_verdict
        {
            mismatches.push(format!(
                "stored current_pre_activation_gate_verdict {:?} does not match nested live cutover {:?}",
                status.current_pre_activation_gate_verdict,
                verified_live_cutover.current_pre_activation_gate_verdict
            ));
        }
        if status.current_pre_activation_gate_reason
            != verified_live_cutover.current_pre_activation_gate_reason
        {
            mismatches.push(format!(
                "stored current_pre_activation_gate_reason {:?} does not match nested live cutover {:?}",
                status.current_pre_activation_gate_reason,
                verified_live_cutover.current_pre_activation_gate_reason
            ));
        }
        mapped_result
    } else {
        match verified_turn_green.contract.result.as_deref() {
            Some("refused_now_by_stage3") => LivePackageExecuteFrozenResult::RefusedNowByStage3,
            Some("refused_now_by_pre_activation_gate") => {
                LivePackageExecuteFrozenResult::RefusedNowByPreActivationGate
            }
            Some("refused_now_by_invalid_or_drifted_contract") => {
                LivePackageExecuteFrozenResult::RefusedNowByInvalidOrDriftedContract
            }
            Some("executable_now") => {
                mismatches.push(
                    "execute-frozen session is missing nested live cutover evidence even though verified turn-green truth is executable_now"
                        .to_string(),
                );
                LivePackageExecuteFrozenResult::RefusedNowByInvalidOrDriftedContract
            }
            _ => LivePackageExecuteFrozenResult::RefusedNowByInvalidOrDriftedContract,
        }
    };

    if status.result != expected_result {
        mismatches.push(format!(
            "stored execute-frozen result {} does not match verified {}",
            serialize_enum(&status.result),
            serialize_enum(&expected_result)
        ));
    }
    compare_string(
        &status.operator_next_action_summary,
        &operator_next_action_summary(
            expected_result,
            &verified_turn_green
                .contract
                .frozen_live_cutover_controller_command_summary,
        ),
        "execute-frozen operator_next_action_summary",
        &mut mismatches,
    );

    let verdict = if mismatches.is_empty() {
        TinyLivePackageExecuteFrozenVerdict::TinyLivePackageExecuteFrozenVerifyOk
    } else {
        TinyLivePackageExecuteFrozenVerdict::TinyLivePackageExecuteFrozenVerifyInvalid
    };
    let reason = if mismatches.is_empty() {
        format!(
            "turn-green-native execute-frozen session under {} is coherent and correctly binds the verified frozen controller handoff",
            session_dir.display()
        )
    } else {
        mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "turn-green-native execute-frozen verification failed".to_string())
    };
    Ok(PackageExecuteFrozenReport {
        generated_at: Utc::now(),
        mode: "verify_live_package_execute_frozen".to_string(),
        verdict,
        reason,
        turn_green_session_dir: config.turn_green_session_dir.display().to_string(),
        launch_packet_session_dir: Some(
            verified_turn_green
                .contract
                .launch_packet_session_dir
                .display()
                .to_string(),
        ),
        package_dir: Some(verified_turn_green.contract.package_dir.display().to_string()),
        install_root: Some(verified_turn_green.contract.install_root.display().to_string()),
        target_service_name: Some(verified_turn_green.contract.target_service_name.clone()),
        backend_command: Some(verified_turn_green.contract.backend_command.clone()),
        wrapper_timeout_ms: Some(verified_turn_green.contract.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(
            verified_turn_green.contract.service_status_max_staleness_ms,
        ),
        session_dir: Some(session_dir.display().to_string()),
        execute_frozen_session_path: Some(paths.session_path.display().to_string()),
        execute_frozen_status_path: Some(paths.status_path.display().to_string()),
        nested_live_cutover_session_dir: Some(
            paths.nested_live_cutover_session_dir.display().to_string(),
        ),
        result: Some(serialize_enum(&status.result)),
        turn_green_result_now: status.turn_green_result_now.clone(),
        turn_green_step: status.turn_green_step.clone(),
        live_cutover_step: status.live_cutover_step.clone(),
        verify_turn_green_command_summary: Some(verify_turn_green_command_summary(
            &config.turn_green_session_dir,
        )),
        execute_frozen_command_summary: Some(run_execute_frozen_command_summary(
            &config.turn_green_session_dir,
            session_dir,
        )),
        frozen_live_cutover_controller_command_summary: Some(
            verified_turn_green
                .contract
                .frozen_live_cutover_controller_command_summary,
        ),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        verification_mismatches: mismatches,
        execution_happened: status.live_cutover_step.is_some(),
        explicit_statement:
            "this verification binds the verified turn-green artifact to the frozen live cutover handoff and never substitutes a different controller contract"
                .to_string(),
    })
}

fn verify_turn_green_command_summary(turn_green_session_dir: &Path) -> String {
    format!(
        "verify immutable turn-green session under {} before handing off execution to the frozen live cutover controller",
        turn_green_session_dir.display()
    )
}

fn run_execute_frozen_command_summary(turn_green_session_dir: &Path, session_dir: &Path) -> String {
    format!(
        "copybot_tiny_live_activation_package_execute_frozen --turn-green-session-dir {} --session-dir {} --run-live-package-execute-frozen",
        shell_escape_path(turn_green_session_dir),
        shell_escape_path(session_dir),
    )
}

fn classify_turn_green_refusal(
    verified_turn_green: &turn_green_execute_frozen::VerifiedLivePackageTurnGreenExecuteFrozenStep,
) -> Option<(
    TinyLivePackageExecuteFrozenVerdict,
    LivePackageExecuteFrozenResult,
    String,
)> {
    if verified_turn_green.verdict != "tiny_live_package_turn_green_verify_ok" {
        return Some((
            TinyLivePackageExecuteFrozenVerdict::TinyLivePackageExecuteFrozenRefusedNowByInvalidOrDriftedContract,
            LivePackageExecuteFrozenResult::RefusedNowByInvalidOrDriftedContract,
            format!(
                "verified turn-green chain is non-green: {}",
                verified_turn_green.reason
            ),
        ));
    }
    if !verified_turn_green.contract.frozen_controller_still_current {
        return Some((
            TinyLivePackageExecuteFrozenVerdict::TinyLivePackageExecuteFrozenRefusedNowByInvalidOrDriftedContract,
            LivePackageExecuteFrozenResult::RefusedNowByInvalidOrDriftedContract,
            "the frozen live cutover controller summary drifted and cannot be executed".to_string(),
        ));
    }
    match verified_turn_green.contract.result.as_deref() {
        Some("refused_now_by_stage3") => Some((
            TinyLivePackageExecuteFrozenVerdict::TinyLivePackageExecuteFrozenRefusedNowByStage3,
            LivePackageExecuteFrozenResult::RefusedNowByStage3,
            verified_turn_green.reason.clone(),
        )),
        Some("refused_now_by_pre_activation_gate") => Some((
            TinyLivePackageExecuteFrozenVerdict::TinyLivePackageExecuteFrozenRefusedNowByPreActivationGate,
            LivePackageExecuteFrozenResult::RefusedNowByPreActivationGate,
            verified_turn_green.reason.clone(),
        )),
        Some("refused_now_by_invalid_or_drifted_contract") => Some((
            TinyLivePackageExecuteFrozenVerdict::TinyLivePackageExecuteFrozenRefusedNowByInvalidOrDriftedContract,
            LivePackageExecuteFrozenResult::RefusedNowByInvalidOrDriftedContract,
            verified_turn_green.reason.clone(),
        )),
        Some("executable_now") if verified_turn_green.contract.executable_now => None,
        _ => Some((
            TinyLivePackageExecuteFrozenVerdict::TinyLivePackageExecuteFrozenRefusedNowByInvalidOrDriftedContract,
            LivePackageExecuteFrozenResult::RefusedNowByInvalidOrDriftedContract,
            "the verified turn-green artifact does not prove executable_now for the frozen controller".to_string(),
        )),
    }
}

fn map_live_cutover_outcome(
    step: &live_cutover_execute_frozen::PackageLiveCutoverExecuteFrozenStep,
) -> (
    LivePackageExecuteFrozenResult,
    TinyLivePackageExecuteFrozenVerdict,
    String,
) {
    match step.result.as_deref() {
        Some("completed_keep_running") => (
            LivePackageExecuteFrozenResult::CompletedKeepRunning,
            TinyLivePackageExecuteFrozenVerdict::TinyLivePackageExecuteFrozenCompletedKeepRunning,
            step.reason.clone(),
        ),
        Some("completed_with_rollback") => (
            LivePackageExecuteFrozenResult::CompletedWithRollback,
            TinyLivePackageExecuteFrozenVerdict::TinyLivePackageExecuteFrozenCompletedWithRollback,
            step.reason.clone(),
        ),
        Some("completed_backup_failed") => (
            LivePackageExecuteFrozenResult::CompletedBackupFailed,
            TinyLivePackageExecuteFrozenVerdict::TinyLivePackageExecuteFrozenCompletedBackupFailed,
            step.reason.clone(),
        ),
        Some("completed_apply_failed") => (
            LivePackageExecuteFrozenResult::CompletedApplyFailed,
            TinyLivePackageExecuteFrozenVerdict::TinyLivePackageExecuteFrozenCompletedApplyFailed,
            step.reason.clone(),
        ),
        Some("completed_watch_failed") => (
            LivePackageExecuteFrozenResult::CompletedWatchFailed,
            TinyLivePackageExecuteFrozenVerdict::TinyLivePackageExecuteFrozenCompletedWatchFailed,
            step.reason.clone(),
        ),
        Some("refused_by_stage3") => (
            LivePackageExecuteFrozenResult::RefusedNowByStage3,
            TinyLivePackageExecuteFrozenVerdict::TinyLivePackageExecuteFrozenRefusedNowByStage3,
            step.reason.clone(),
        ),
        Some("refused_by_pre_activation_gate") => (
            LivePackageExecuteFrozenResult::RefusedNowByPreActivationGate,
            TinyLivePackageExecuteFrozenVerdict::TinyLivePackageExecuteFrozenRefusedNowByPreActivationGate,
            step.reason.clone(),
        ),
        _ => (
            LivePackageExecuteFrozenResult::RefusedNowByInvalidOrDriftedContract,
            TinyLivePackageExecuteFrozenVerdict::TinyLivePackageExecuteFrozenRefusedNowByInvalidOrDriftedContract,
            step.reason.clone(),
        ),
    }
}

fn operator_next_action_summary(
    result: LivePackageExecuteFrozenResult,
    frozen_live_cutover_controller_command_summary: &str,
) -> String {
    match result {
        LivePackageExecuteFrozenResult::RefusedNowByStage3 => {
            "the frozen controller remains refused now by Stage 3; rerun turn-green after current Stage 3 truth turns green".to_string()
        }
        LivePackageExecuteFrozenResult::RefusedNowByPreActivationGate => {
            "the frozen controller remains refused now by the pre-activation gate; rerun turn-green after the current pre-activation gate turns green".to_string()
        }
        LivePackageExecuteFrozenResult::RefusedNowByInvalidOrDriftedContract => {
            "the frozen turn-green or live controller contract drifted; mint a new coherent turn-green artifact before attempting execution".to_string()
        }
        LivePackageExecuteFrozenResult::CompletedKeepRunning => format!(
            "the frozen controller executed successfully and the live target stayed running; nested live cutover contract was: {frozen_live_cutover_controller_command_summary}"
        ),
        LivePackageExecuteFrozenResult::CompletedWithRollback => format!(
            "the frozen controller executed and rolled back cleanly; inspect nested live cutover evidence from: {frozen_live_cutover_controller_command_summary}"
        ),
        LivePackageExecuteFrozenResult::CompletedBackupFailed => {
            "the frozen controller stopped before apply because backup proof creation failed".to_string()
        }
        LivePackageExecuteFrozenResult::CompletedApplyFailed => {
            "the frozen controller reached apply and failed; inspect nested live cutover evidence before any retry".to_string()
        }
        LivePackageExecuteFrozenResult::CompletedWatchFailed => {
            "the frozen controller reached watch and degraded; inspect nested rollback evidence before any retry".to_string()
        }
    }
}

fn execute_frozen_paths(session_dir: &Path) -> PackageExecuteFrozenPaths {
    PackageExecuteFrozenPaths {
        session_path: session_dir.join("tiny_live_activation_package_execute_frozen.session.json"),
        status_path: session_dir.join("tiny_live_activation_package_execute_frozen.status.json"),
        turn_green_report_path: session_dir
            .join("tiny_live_activation_package_execute_frozen.turn_green.report.json"),
        live_cutover_report_path: session_dir
            .join("tiny_live_activation_package_execute_frozen.live_cutover.report.json"),
        nested_live_cutover_session_dir: session_dir
            .join("tiny_live_activation_package_execute_frozen.live_cutover_session"),
    }
}

fn ensure_clean_execute_frozen_session_dir(session_dir: &Path) -> Result<()> {
    if session_dir.exists() {
        bail!(
            "refusing to overwrite existing execute-frozen session dir {}",
            session_dir.display()
        );
    }
    fs::create_dir_all(session_dir).with_context(|| {
        format!(
            "failed creating turn-green-native execute-frozen session dir {}",
            session_dir.display()
        )
    })
}

fn planned_session(
    config: &Config,
    session_dir: &Path,
    paths: &PackageExecuteFrozenPaths,
    frozen: &turn_green_execute_frozen::LivePackageTurnGreenContractView,
) -> PackageExecuteFrozenSession {
    PackageExecuteFrozenSession {
        session_version: EXECUTE_FROZEN_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        turn_green_session_dir: config.turn_green_session_dir.display().to_string(),
        launch_packet_session_dir: frozen.launch_packet_session_dir.display().to_string(),
        package_dir: frozen.package_dir.display().to_string(),
        install_root: frozen.install_root.display().to_string(),
        target_service_name: frozen.target_service_name.clone(),
        backend_command: frozen.backend_command.clone(),
        wrapper_timeout_ms: frozen.wrapper_timeout_ms,
        service_status_max_staleness_ms: frozen.service_status_max_staleness_ms,
        session_dir: session_dir.display().to_string(),
        nested_live_cutover_session_dir: paths
            .nested_live_cutover_session_dir
            .display()
            .to_string(),
        verify_turn_green_command_summary: verify_turn_green_command_summary(
            &config.turn_green_session_dir,
        ),
        execute_frozen_command_summary: run_execute_frozen_command_summary(
            &config.turn_green_session_dir,
            session_dir,
        ),
        frozen_live_cutover_controller_command_summary: frozen
            .frozen_live_cutover_controller_command_summary
            .clone(),
        explicit_statement:
            "the verified turn-green session is the only mutable handoff input here; this command never restitches package, target, or controller truth from loose args"
                .to_string(),
    }
}

fn refusal_status(
    session_dir: &Path,
    turn_green_session_dir: &Path,
    nested_live_cutover_session_dir: &Path,
    result: LivePackageExecuteFrozenResult,
    reason: String,
    turn_green_result_now: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    operator_next_action_summary: String,
) -> PackageExecuteFrozenStatus {
    PackageExecuteFrozenStatus {
        status_version: EXECUTE_FROZEN_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        session_dir: session_dir.display().to_string(),
        turn_green_session_dir: turn_green_session_dir.display().to_string(),
        nested_live_cutover_session_dir: nested_live_cutover_session_dir.display().to_string(),
        result,
        reason,
        turn_green_result_now,
        current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason,
        turn_green_step: None,
        live_cutover_step: None,
        operator_next_action_summary,
        explicit_statement:
            "this turn-green-native handoff refused the frozen controller without mutating the real target because the verified turn-green chain is not executable_now"
                .to_string(),
    }
}

fn report_from_status(
    config: &Config,
    frozen: &turn_green_execute_frozen::LivePackageTurnGreenContractView,
    session_dir: &Path,
    paths: &PackageExecuteFrozenPaths,
    verdict: TinyLivePackageExecuteFrozenVerdict,
    status: &PackageExecuteFrozenStatus,
) -> PackageExecuteFrozenReport {
    PackageExecuteFrozenReport {
        generated_at: Utc::now(),
        mode: "run_live_package_execute_frozen".to_string(),
        verdict,
        reason: status.reason.clone(),
        turn_green_session_dir: config.turn_green_session_dir.display().to_string(),
        launch_packet_session_dir: Some(frozen.launch_packet_session_dir.display().to_string()),
        package_dir: Some(frozen.package_dir.display().to_string()),
        install_root: Some(frozen.install_root.display().to_string()),
        target_service_name: Some(frozen.target_service_name.clone()),
        backend_command: Some(frozen.backend_command.clone()),
        wrapper_timeout_ms: Some(frozen.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(frozen.service_status_max_staleness_ms),
        session_dir: Some(session_dir.display().to_string()),
        execute_frozen_session_path: Some(paths.session_path.display().to_string()),
        execute_frozen_status_path: Some(paths.status_path.display().to_string()),
        nested_live_cutover_session_dir: Some(
            paths.nested_live_cutover_session_dir.display().to_string(),
        ),
        result: Some(serialize_enum(&status.result)),
        turn_green_result_now: status.turn_green_result_now.clone(),
        turn_green_step: status.turn_green_step.clone(),
        live_cutover_step: status.live_cutover_step.clone(),
        verify_turn_green_command_summary: Some(verify_turn_green_command_summary(
            &config.turn_green_session_dir,
        )),
        execute_frozen_command_summary: Some(run_execute_frozen_command_summary(
            &config.turn_green_session_dir,
            session_dir,
        )),
        frozen_live_cutover_controller_command_summary: Some(
            frozen
                .frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        verification_mismatches: Vec::new(),
        execution_happened: status.live_cutover_step.is_some(),
        explicit_statement: status.explicit_statement.clone(),
    }
}

fn failure_report_without_contract(
    config: &Config,
    verdict: TinyLivePackageExecuteFrozenVerdict,
    reason: String,
) -> PackageExecuteFrozenReport {
    PackageExecuteFrozenReport {
        generated_at: Utc::now(),
        mode: match config.mode {
            Mode::PlanLivePackageExecuteFrozen => "plan_live_package_execute_frozen".to_string(),
            Mode::RenderLivePackageExecuteFrozenScript => {
                "render_live_package_execute_frozen_script".to_string()
            }
            Mode::RunLivePackageExecuteFrozen => "run_live_package_execute_frozen".to_string(),
            Mode::VerifyLivePackageExecuteFrozen => {
                "verify_live_package_execute_frozen".to_string()
            }
        },
        verdict,
        reason,
        turn_green_session_dir: config.turn_green_session_dir.display().to_string(),
        launch_packet_session_dir: None,
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
        execute_frozen_session_path: None,
        execute_frozen_status_path: None,
        nested_live_cutover_session_dir: None,
        result: Some("refused_now_by_invalid_or_drifted_contract".to_string()),
        turn_green_result_now: None,
        turn_green_step: None,
        live_cutover_step: None,
        verify_turn_green_command_summary: Some(verify_turn_green_command_summary(
            &config.turn_green_session_dir,
        )),
        execute_frozen_command_summary: config
            .session_dir
            .as_ref()
            .map(|value| run_execute_frozen_command_summary(&config.turn_green_session_dir, value)),
        frozen_live_cutover_controller_command_summary: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        verification_mismatches: Vec::new(),
        execution_happened: false,
        explicit_statement:
            "this execute-frozen handoff refused before any nested controller invocation because the turn-green artifact could not be loaded coherently"
                .to_string(),
    }
}

fn failure_report_for_verify(
    config: &Config,
    session_dir: &Path,
    frozen: &turn_green_execute_frozen::LivePackageTurnGreenContractView,
    mismatches: Vec<String>,
) -> PackageExecuteFrozenReport {
    let paths = execute_frozen_paths(session_dir);
    let reason = mismatches
        .first()
        .cloned()
        .unwrap_or_else(|| "turn-green-native execute-frozen verification failed".to_string());
    PackageExecuteFrozenReport {
        generated_at: Utc::now(),
        mode: "verify_live_package_execute_frozen".to_string(),
        verdict: TinyLivePackageExecuteFrozenVerdict::TinyLivePackageExecuteFrozenVerifyInvalid,
        reason,
        turn_green_session_dir: config.turn_green_session_dir.display().to_string(),
        launch_packet_session_dir: Some(frozen.launch_packet_session_dir.display().to_string()),
        package_dir: Some(frozen.package_dir.display().to_string()),
        install_root: Some(frozen.install_root.display().to_string()),
        target_service_name: Some(frozen.target_service_name.clone()),
        backend_command: Some(frozen.backend_command.clone()),
        wrapper_timeout_ms: Some(frozen.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(frozen.service_status_max_staleness_ms),
        session_dir: Some(session_dir.display().to_string()),
        execute_frozen_session_path: Some(paths.session_path.display().to_string()),
        execute_frozen_status_path: Some(paths.status_path.display().to_string()),
        nested_live_cutover_session_dir: Some(
            paths.nested_live_cutover_session_dir.display().to_string(),
        ),
        result: None,
        turn_green_result_now: frozen.result.clone(),
        turn_green_step: None,
        live_cutover_step: None,
        verify_turn_green_command_summary: Some(verify_turn_green_command_summary(
            &config.turn_green_session_dir,
        )),
        execute_frozen_command_summary: Some(run_execute_frozen_command_summary(
            &config.turn_green_session_dir,
            session_dir,
        )),
        frozen_live_cutover_controller_command_summary: Some(
            frozen
                .frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        current_pre_activation_gate_verdict: frozen.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: frozen.current_pre_activation_gate_reason.clone(),
        verification_mismatches: mismatches,
        execution_happened: false,
        explicit_statement:
            "this verification failure means the verified turn-green handoff no longer proves the frozen controller contract"
                .to_string(),
    }
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
        .with_context(|| format!("failed writing execute-frozen script to {}", path.display()))?;
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

fn step_artifact(
    path: &Path,
    mode: &str,
    verdict: &str,
    reason: &str,
    generated_at: DateTime<Utc>,
) -> PackageExecuteFrozenStepArtifact {
    PackageExecuteFrozenStepArtifact {
        path: path.display().to_string(),
        mode: mode.to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        generated_at,
    }
}

fn load_required_step_json<T: for<'de> Deserialize<'de>>(
    step: &Option<PackageExecuteFrozenStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<T> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("{label} is missing from execute-frozen status"))?;
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

fn serialize_enum<T: Serialize>(value: &T) -> String {
    match serde_json::to_value(value) {
        Ok(serde_json::Value::String(raw)) => raw,
        Ok(other) => other.to_string(),
        Err(_) => "<serialization-error>".to_string(),
    }
}

fn render_report_lines(report: &PackageExecuteFrozenReport) -> String {
    let mut lines = vec![
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
        format!("turn_green_session_dir={}", report.turn_green_session_dir),
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
    if let Some(value) = &report.result {
        lines.push(format!("result={value}"));
    }
    lines.push(format!("execution_happened={}", report.execution_happened));
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
                .map(|entry| format!("  - {entry}")),
        );
    }
    lines.push(format!("explicit_statement={}", report.explicit_statement));
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use copybot_app::tiny_live_activation::install_target_managed_surface::derive_install_target_managed_surface_paths;
    use serde_json::json;
    use std::ffi::OsString;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn classify_turn_green_refusal_requires_current_frozen_controller() {
        let step = verified_turn_green_step(
            "tiny_live_package_turn_green_verify_ok",
            Some("executable_now"),
            true,
            false,
        );
        let refusal = classify_turn_green_refusal(&step).expect("drift must refuse");
        assert_eq!(
            refusal.0,
            TinyLivePackageExecuteFrozenVerdict::TinyLivePackageExecuteFrozenRefusedNowByInvalidOrDriftedContract
        );
        assert_eq!(
            refusal.1,
            LivePackageExecuteFrozenResult::RefusedNowByInvalidOrDriftedContract
        );
    }

    #[test]
    fn classify_turn_green_refusal_allows_executable_now_only_for_green_chain() {
        let step = verified_turn_green_step(
            "tiny_live_package_turn_green_verify_ok",
            Some("executable_now"),
            true,
            true,
        );
        assert!(classify_turn_green_refusal(&step).is_none());
    }

    #[test]
    fn map_live_cutover_outcome_keeps_verdict_mapping_bounded() {
        let step = live_cutover_execute_frozen::PackageLiveCutoverExecuteFrozenStep {
            report_json: json!({}),
            verdict: "tiny_live_package_live_cutover_verify_ok".to_string(),
            reason: "completed".to_string(),
            generated_at: Utc::now(),
            result: Some("completed_keep_running".to_string()),
            run_live_cutover_command_summary: "cutover".to_string(),
            current_pre_activation_gate_verdict: Some("green".to_string()),
            current_pre_activation_gate_reason: Some("ok".to_string()),
            activation_authorized: true,
        };
        let mapped = map_live_cutover_outcome(&step);
        assert_eq!(
            mapped.0,
            LivePackageExecuteFrozenResult::CompletedKeepRunning
        );
        assert_eq!(
            mapped.1,
            TinyLivePackageExecuteFrozenVerdict::TinyLivePackageExecuteFrozenCompletedKeepRunning
        );
    }

    #[test]
    fn run_completed_keep_running_then_verify_stays_green() {
        let fixture = fake_command_fixture(
            "execute_frozen_keep_running",
            executable_now_turn_green_report("executable_now"),
            Some(live_cutover_report("completed_keep_running")),
            Some(verified_live_cutover_report("completed_keep_running")),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let run = run_live_package_execute_frozen_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageExecuteFrozenVerdict::TinyLivePackageExecuteFrozenCompletedKeepRunning
        );
        assert_eq!(run.result.as_deref(), Some("completed_keep_running"));
        assert!(run.execution_happened);

        let verify = verify_live_package_execute_frozen_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageExecuteFrozenVerdict::TinyLivePackageExecuteFrozenVerifyOk,
            "{:#?}",
            verify.verification_mismatches
        );
        assert_eq!(verify.result.as_deref(), Some("completed_keep_running"));
    }

    #[test]
    fn stage3_refusal_stays_explicit() {
        let fixture = fake_command_fixture(
            "execute_frozen_stage3_refusal",
            refused_turn_green_report("refused_now_by_stage3", "stage3 blocked"),
            None,
            None,
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let report = run_live_package_execute_frozen_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageExecuteFrozenVerdict::TinyLivePackageExecuteFrozenRefusedNowByStage3
        );
        assert_eq!(report.result.as_deref(), Some("refused_now_by_stage3"));
        assert!(!report.execution_happened);
    }

    #[test]
    fn pre_activation_refusal_stays_explicit() {
        let fixture = fake_command_fixture(
            "execute_frozen_pregate_refusal",
            refused_turn_green_report(
                "refused_now_by_pre_activation_gate",
                "pre-activation blocked",
            ),
            None,
            None,
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let report = run_live_package_execute_frozen_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageExecuteFrozenVerdict::TinyLivePackageExecuteFrozenRefusedNowByPreActivationGate
        );
        assert_eq!(
            report.result.as_deref(),
            Some("refused_now_by_pre_activation_gate")
        );
        assert!(!report.execution_happened);
    }

    #[test]
    fn drifted_frozen_controller_summary_is_refused() {
        let fixture = fake_command_fixture(
            "execute_frozen_drifted_controller",
            drifted_turn_green_report(),
            None,
            None,
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let report = run_live_package_execute_frozen_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageExecuteFrozenVerdict::TinyLivePackageExecuteFrozenRefusedNowByInvalidOrDriftedContract
        );
        assert_eq!(
            report.result.as_deref(),
            Some("refused_now_by_invalid_or_drifted_contract")
        );
        assert!(!report.execution_happened);
    }

    #[test]
    fn verify_rejects_tampered_turn_green_step_path() {
        let fixture = fake_command_fixture(
            "execute_frozen_tampered_turn_green_step",
            executable_now_turn_green_report("executable_now"),
            Some(live_cutover_report("completed_keep_running")),
            Some(verified_live_cutover_report("completed_keep_running")),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_execute_frozen_report(&fixture.run_config()).unwrap();
        let paths = execute_frozen_paths(&fixture.execute_frozen_session_dir);
        let mut status: PackageExecuteFrozenStatus = load_json(&paths.status_path).unwrap();
        let foreign = fixture.fixture_dir.join("foreign.turn-green.report.json");
        fs::write(&foreign, "{}").unwrap();
        status.turn_green_step.as_mut().unwrap().path = foreign.display().to_string();
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_live_package_execute_frozen_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageExecuteFrozenVerdict::TinyLivePackageExecuteFrozenVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_top_level_session_contract_tamper() {
        let fixture = fake_command_fixture(
            "execute_frozen_tampered_session",
            executable_now_turn_green_report("executable_now"),
            Some(live_cutover_report("completed_keep_running")),
            Some(verified_live_cutover_report("completed_keep_running")),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_execute_frozen_report(&fixture.run_config()).unwrap();
        let paths = execute_frozen_paths(&fixture.execute_frozen_session_dir);
        let mut session: PackageExecuteFrozenSession = load_json(&paths.session_path).unwrap();
        session.frozen_live_cutover_controller_command_summary = "tampered command".to_string();
        persist_json(&paths.session_path, &session).unwrap();

        let verify = verify_live_package_execute_frozen_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageExecuteFrozenVerdict::TinyLivePackageExecuteFrozenVerifyInvalid
        );
    }

    #[test]
    fn verify_rejects_tampered_turn_green_launch_packet_session_dir() {
        let fixture = fake_command_fixture(
            "execute_frozen_tampered_turn_green_launch_packet",
            executable_now_turn_green_report("executable_now"),
            Some(live_cutover_report("completed_keep_running")),
            Some(verified_live_cutover_report("completed_keep_running")),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        run_live_package_execute_frozen_report(&fixture.run_config()).unwrap();
        let turn_green_session_path = fixture
            .turn_green_session_dir
            .join("tiny_live_activation_package_turn_green.session.json");
        let mut turn_green_session: serde_json::Value =
            load_json(&turn_green_session_path).unwrap();
        let foreign_launch_packet_session_dir = fixture.fixture_dir.join("foreign-launch-packet");
        fs::create_dir_all(&foreign_launch_packet_session_dir).unwrap();
        turn_green_session["launch_packet_session_dir"] =
            json!(foreign_launch_packet_session_dir.display().to_string());
        persist_json(&turn_green_session_path, &turn_green_session).unwrap();

        let verify = verify_live_package_execute_frozen_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageExecuteFrozenVerdict::TinyLivePackageExecuteFrozenVerifyInvalid
        );
        assert!(
            verify
                .verification_mismatches
                .iter()
                .any(|entry| entry.contains("nested turn-green verification is non-green")),
            "{:#?}",
            verify.verification_mismatches
        );
    }

    #[test]
    fn managed_surface_overlap_is_refused_without_writes() {
        let fixture = fake_command_fixture(
            "execute_frozen_overlap_refusal",
            executable_now_turn_green_report("executable_now"),
            Some(live_cutover_report("completed_keep_running")),
            Some(verified_live_cutover_report("completed_keep_running")),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        let managed_surface =
            derive_install_target_managed_surface_paths(&fixture.install_root).unwrap();
        let unsafe_session_dir = managed_surface.session_dir.join("execute-frozen-session");
        let config = Config {
            session_dir: Some(unsafe_session_dir.clone()),
            ..fixture.run_config()
        };

        let error = run_live_package_execute_frozen_report(&config).expect_err("overlap must fail");
        assert!(
            error.to_string().contains("must stay disjoint from"),
            "{error:#}"
        );
        let paths = execute_frozen_paths(&unsafe_session_dir);
        assert!(!unsafe_session_dir.exists());
        assert!(!paths.session_path.exists());
        assert!(!paths.status_path.exists());
    }

    #[test]
    fn run_refuses_before_writing_when_stored_install_root_is_tampered_and_real_managed_surface_overlaps(
    ) {
        let fixture = fake_command_fixture(
            "execute_frozen_tampered_install_root_guard",
            executable_now_turn_green_report("executable_now"),
            Some(live_cutover_report("completed_keep_running")),
            Some(verified_live_cutover_report("completed_keep_running")),
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let turn_green_session_path = fixture
            .turn_green_session_dir
            .join("tiny_live_activation_package_turn_green.session.json");
        let mut turn_green_session: serde_json::Value =
            load_json(&turn_green_session_path).unwrap();
        let fake_install_root = fixture.fixture_dir.join("tampered-install-root");
        fs::create_dir_all(&fake_install_root).unwrap();
        turn_green_session["install_root"] = json!(fake_install_root.display().to_string());
        persist_json(&turn_green_session_path, &turn_green_session).unwrap();

        let managed_surface =
            derive_install_target_managed_surface_paths(&fixture.install_root).unwrap();
        let unsafe_session_dir = managed_surface.session_dir.join("execute-frozen-session");
        let config = Config {
            session_dir: Some(unsafe_session_dir.clone()),
            ..fixture.run_config()
        };

        let error = run_live_package_execute_frozen_report(&config).expect_err("overlap must fail");
        assert!(
            error.to_string().contains("must stay disjoint from"),
            "{error:#}"
        );
        let paths = execute_frozen_paths(&unsafe_session_dir);
        assert!(!unsafe_session_dir.exists());
        assert!(!paths.session_path.exists());
        assert!(!paths.status_path.exists());
    }

    fn verified_turn_green_step(
        verdict: &str,
        result: Option<&str>,
        executable_now: bool,
        frozen_controller_still_current: bool,
    ) -> turn_green_execute_frozen::VerifiedLivePackageTurnGreenExecuteFrozenStep {
        turn_green_execute_frozen::VerifiedLivePackageTurnGreenExecuteFrozenStep {
            report_json: json!({}),
            verdict: verdict.to_string(),
            reason: "reason".to_string(),
            generated_at: Utc::now(),
            contract: turn_green_execute_frozen::LivePackageTurnGreenContractView {
                turn_green_session_dir: PathBuf::from("/tmp/turn-green"),
                launch_packet_session_dir: PathBuf::from("/tmp/launch-packet"),
                package_dir: PathBuf::from("/tmp/package"),
                install_root: PathBuf::from("/"),
                target_service_name: "solana-copy-bot.service".to_string(),
                backend_command: "/tmp/backend".to_string(),
                wrapper_timeout_ms: 1200,
                service_status_max_staleness_ms: 4500,
                result: result.map(ToOwned::to_owned),
                executable_now,
                frozen_controller_still_current,
                current_pre_activation_gate_verdict: Some("green".to_string()),
                current_pre_activation_gate_reason: Some("ok".to_string()),
                verify_frozen_launch_packet_summary: "verify packet".to_string(),
                refresh_current_authorization_command_summary: "refresh auth".to_string(),
                frozen_live_cutover_controller_command_summary: "run cutover".to_string(),
                current_live_cutover_controller_command_summary: "run cutover".to_string(),
                operator_next_action_summary: "next".to_string(),
                explicit_statement: "statement".to_string(),
            },
        }
    }

    struct FakeCommandFixture {
        fixture_dir: PathBuf,
        bin_dir: PathBuf,
        install_root: PathBuf,
        turn_green_session_dir: PathBuf,
        execute_frozen_session_dir: PathBuf,
    }

    impl FakeCommandFixture {
        fn run_config(&self) -> Config {
            Config {
                mode: Mode::RunLivePackageExecuteFrozen,
                turn_green_session_dir: self.turn_green_session_dir.clone(),
                session_dir: Some(self.execute_frozen_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn verify_config(&self) -> Config {
            Config {
                mode: Mode::VerifyLivePackageExecuteFrozen,
                turn_green_session_dir: self.turn_green_session_dir.clone(),
                session_dir: Some(self.execute_frozen_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }
    }

    fn fake_command_fixture(
        label: &str,
        turn_green_verify_report: serde_json::Value,
        live_cutover_run_report: Option<serde_json::Value>,
        live_cutover_verify_report: Option<serde_json::Value>,
    ) -> FakeCommandFixture {
        let fixture_dir = temp_dir(label);
        let bin_dir = fixture_dir.join("bin");
        fs::create_dir_all(&bin_dir).unwrap();
        let install_root = fixture_dir.join("live-root");
        fs::create_dir_all(&install_root).unwrap();
        let turn_green_session_dir = fixture_dir.join("turn-green-session");
        fs::create_dir_all(&turn_green_session_dir).unwrap();
        let execute_frozen_session_dir = fixture_dir.join("execute-frozen-session");
        let package_dir = fixture_dir.join("package");
        let launch_packet_session_dir = fixture_dir.join("launch-packet-session");
        let backend_command = fixture_dir.join("fake-backend");
        let frozen_live_cutover_session_dir = fixture_dir.join("frozen-live-cutover-session");
        let frozen_launch_packet_report_path = turn_green_session_dir
            .join("tiny_live_activation_package_turn_green.frozen_launch_packet.report.json");
        fs::create_dir_all(&package_dir).unwrap();
        fs::create_dir_all(&launch_packet_session_dir).unwrap();

        let frozen_controller_command = frozen_live_cutover_command_summary(
            &package_dir,
            &install_root,
            &backend_command,
            &frozen_live_cutover_session_dir,
        );
        let mut turn_green_verify_report = turn_green_verify_report;
        bind_turn_green_report_to_fixture(
            &mut turn_green_verify_report,
            &package_dir,
            &install_root,
            &backend_command,
            &frozen_controller_command,
        );
        let current_controller_command = turn_green_verify_report
            .get("current_live_cutover_controller_command_summary")
            .and_then(|value| value.as_str())
            .unwrap_or(frozen_controller_command.as_str())
            .to_string();
        let result = turn_green_verify_report
            .get("result")
            .and_then(|value| value.as_str())
            .unwrap_or("refused_now_by_invalid_or_drifted_contract");
        let executable_now = turn_green_verify_report
            .get("executable_now")
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
        let frozen_controller_still_current = turn_green_verify_report
            .get("frozen_controller_still_current")
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
        let current_pre_activation_gate_verdict = turn_green_verify_report
            .get("current_pre_activation_gate_verdict")
            .and_then(|value| value.as_str());
        let current_pre_activation_gate_reason = turn_green_verify_report
            .get("current_pre_activation_gate_reason")
            .and_then(|value| value.as_str());
        let operator_next_action_summary = turn_green_verify_report
            .get("operator_next_action_summary")
            .and_then(|value| value.as_str())
            .unwrap_or("next action");
        let explicit_statement = turn_green_verify_report
            .get("explicit_statement")
            .and_then(|value| value.as_str())
            .unwrap_or("turn-green statement");

        persist_json(
            &turn_green_session_dir.join("tiny_live_activation_package_turn_green.session.json"),
            &json!({
                "launch_packet_session_dir": launch_packet_session_dir.display().to_string(),
                "package_dir": package_dir.display().to_string(),
                "install_root": install_root.display().to_string(),
                "target_service_name": "solana-copy-bot.service",
                "backend_command": backend_command.display().to_string(),
                "wrapper_timeout_ms": 1200,
                "service_status_max_staleness_ms": 4500,
                "verify_frozen_launch_packet_summary": "verify packet",
                "refresh_current_authorization_command_summary": "refresh auth",
                "frozen_live_cutover_controller_command_summary": frozen_controller_command
            }),
        )
        .unwrap();
        persist_json(
            &turn_green_session_dir.join("tiny_live_activation_package_turn_green.status.json"),
            &json!({
                "result": result,
                "executable_now": executable_now,
                "frozen_controller_still_current": frozen_controller_still_current,
                "frozen_launch_packet_step": {
                    "path": frozen_launch_packet_report_path.display().to_string()
                },
                "current_pre_activation_gate_verdict": current_pre_activation_gate_verdict,
                "current_pre_activation_gate_reason": current_pre_activation_gate_reason,
                "operator_next_action_summary": operator_next_action_summary,
                "explicit_statement": explicit_statement
            }),
        )
        .unwrap();
        persist_json(
            &frozen_launch_packet_report_path,
            &json!({
                "session_dir": launch_packet_session_dir.display().to_string()
            }),
        )
        .unwrap();
        persist_json(
            &turn_green_session_dir
                .join("tiny_live_activation_package_turn_green.current_controller.report.json"),
            &json!({
                "run_live_cutover_command_summary": current_controller_command
            }),
        )
        .unwrap();

        let turn_green_verify_path = fixture_dir.join("turn_green.verify.json");
        let turn_green_verify_invalid_path = fixture_dir.join("turn_green.verify.invalid.json");
        let turn_green_verify_invalid_report =
            invalid_turn_green_verify_report(&turn_green_verify_report);
        persist_json(&turn_green_verify_path, &turn_green_verify_report).unwrap();
        persist_json(
            &turn_green_verify_invalid_path,
            &turn_green_verify_invalid_report,
        )
        .unwrap();
        write_fake_turn_green_verify_command(
            &bin_dir.join("copybot_tiny_live_activation_package_turn_green"),
            &turn_green_verify_path,
            &turn_green_verify_invalid_path,
            &launch_packet_session_dir,
            &turn_green_session_dir,
        );

        let live_cutover_run_path = fixture_dir.join("live_cutover.run.json");
        let live_cutover_verify_path = fixture_dir.join("live_cutover.verify.json");
        if let Some(mut report) = live_cutover_run_report {
            bind_live_cutover_report_to_fixture(&mut report, &frozen_controller_command);
            persist_json(&live_cutover_run_path, &report).unwrap();
        }
        if let Some(mut report) = live_cutover_verify_report {
            bind_live_cutover_report_to_fixture(&mut report, &frozen_controller_command);
            persist_json(&live_cutover_verify_path, &report).unwrap();
        }
        write_fake_mode_switching_json_command(
            &bin_dir.join("copybot_tiny_live_activation_package_live_cutover"),
            &live_cutover_run_path,
            &live_cutover_verify_path,
        );

        FakeCommandFixture {
            fixture_dir,
            bin_dir,
            install_root,
            turn_green_session_dir,
            execute_frozen_session_dir,
        }
    }

    fn bind_turn_green_report_to_fixture(
        report: &mut serde_json::Value,
        package_dir: &Path,
        install_root: &Path,
        backend_command: &Path,
        frozen_controller_command: &str,
    ) {
        let object = report
            .as_object_mut()
            .expect("turn-green report fixture must be an object");
        object.insert(
            "package_dir".to_string(),
            json!(package_dir.display().to_string()),
        );
        object.insert(
            "install_root".to_string(),
            json!(install_root.display().to_string()),
        );
        object.insert(
            "target_service_name".to_string(),
            json!("solana-copy-bot.service"),
        );
        object.insert(
            "backend_command".to_string(),
            json!(backend_command.display().to_string()),
        );
        object.insert("wrapper_timeout_ms".to_string(), json!(1200));
        object.insert("service_status_max_staleness_ms".to_string(), json!(4500));
        object.insert(
            "verify_frozen_launch_packet_summary".to_string(),
            json!("verify packet"),
        );
        object.insert(
            "refresh_current_authorization_command_summary".to_string(),
            json!("refresh auth"),
        );
        object.insert(
            "frozen_live_cutover_controller_command_summary".to_string(),
            json!(frozen_controller_command),
        );
        if !object.contains_key("current_live_cutover_controller_command_summary") {
            object.insert(
                "current_live_cutover_controller_command_summary".to_string(),
                json!(frozen_controller_command),
            );
        }
    }

    fn bind_live_cutover_report_to_fixture(
        report: &mut serde_json::Value,
        frozen_controller_command: &str,
    ) {
        let object = report
            .as_object_mut()
            .expect("live-cutover report fixture must be an object");
        object.insert(
            "run_live_cutover_command_summary".to_string(),
            json!(frozen_controller_command),
        );
    }

    fn frozen_live_cutover_command_summary(
        package_dir: &Path,
        install_root: &Path,
        backend_command: &Path,
        session_dir: &Path,
    ) -> String {
        format!(
            "copybot_tiny_live_activation_package_live_cutover --package-dir {} --install-root {} --target-service solana-copy-bot.service --backend-command {} --wrapper-timeout-ms 1200 --service-status-max-staleness-ms 4500 --session-dir {} --run-live-package-cutover",
            shell_escape_path(package_dir),
            shell_escape_path(install_root),
            shell_escape_path(backend_command),
            shell_escape_path(session_dir),
        )
    }

    fn executable_now_turn_green_report(result: &str) -> serde_json::Value {
        json!({
            "generated_at": Utc::now(),
            "verdict": "tiny_live_package_turn_green_verify_ok",
            "reason": "turn-green chain is green",
            "result": result,
            "frozen_controller_still_current": true,
            "current_pre_activation_gate_verdict": "green",
            "current_pre_activation_gate_reason": "ok",
            "operator_next_action_summary": "run exact controller",
            "executable_now": true,
            "explicit_statement": "turn-green statement"
        })
    }

    fn refused_turn_green_report(result: &str, reason: &str) -> serde_json::Value {
        json!({
            "generated_at": Utc::now(),
            "verdict": "tiny_live_package_turn_green_verify_ok",
            "reason": reason,
            "result": result,
            "frozen_controller_still_current": true,
            "current_pre_activation_gate_verdict": "blocked",
            "current_pre_activation_gate_reason": reason,
            "operator_next_action_summary": "do not run",
            "executable_now": false,
            "explicit_statement": "turn-green statement"
        })
    }

    fn drifted_turn_green_report() -> serde_json::Value {
        json!({
            "generated_at": Utc::now(),
            "verdict": "tiny_live_package_turn_green_verify_ok",
            "reason": "controller drifted",
            "result": "refused_now_by_invalid_or_drifted_contract",
            "frozen_controller_still_current": false,
            "current_pre_activation_gate_verdict": "green",
            "current_pre_activation_gate_reason": "ok",
            "frozen_live_cutover_controller_command_summary": "frozen cutover",
            "current_live_cutover_controller_command_summary": "current cutover drifted",
            "operator_next_action_summary": "mint a new turn-green artifact",
            "executable_now": false,
            "explicit_statement": "turn-green statement"
        })
    }

    fn live_cutover_report(result: &str) -> serde_json::Value {
        json!({
            "generated_at": Utc::now(),
            "verdict": format!("tiny_live_package_live_cutover_{result}"),
            "reason": format!("live cutover {result}"),
            "result": result,
            "run_live_cutover_command_summary": "copybot_tiny_live_activation_package_live_cutover ...",
            "current_pre_activation_gate_verdict": "green",
            "current_pre_activation_gate_reason": "ok",
            "activation_authorized": true
        })
    }

    fn verified_live_cutover_report(result: &str) -> serde_json::Value {
        json!({
            "generated_at": Utc::now(),
            "verdict": "tiny_live_package_live_cutover_verify_ok",
            "reason": format!("live cutover {result}"),
            "result": result,
            "run_live_cutover_command_summary": "copybot_tiny_live_activation_package_live_cutover ...",
            "current_pre_activation_gate_verdict": "green",
            "current_pre_activation_gate_reason": "ok",
            "activation_authorized": true
        })
    }

    fn invalid_turn_green_verify_report(success_report: &serde_json::Value) -> serde_json::Value {
        let mut report = success_report.clone();
        let object = report
            .as_object_mut()
            .expect("turn-green verify report fixture must be an object");
        object.insert(
            "verdict".to_string(),
            json!("tiny_live_package_turn_green_verify_invalid"),
        );
        object.insert(
            "reason".to_string(),
            json!(
                "stored launch_packet_session_dir does not match the verified frozen launch packet"
            ),
        );
        object.insert("executable_now".to_string(), json!(false));
        object.insert(
            "result".to_string(),
            json!("refused_now_by_invalid_or_drifted_contract"),
        );
        report
    }

    fn write_fake_turn_green_verify_command(
        path: &Path,
        json_path: &Path,
        invalid_json_path: &Path,
        expected_launch_packet_session_dir: &Path,
        turn_green_session_dir: &Path,
    ) {
        let expected_launch_packet_session_dir =
            expected_launch_packet_session_dir.display().to_string();
        let expected_turn_green_session_dir = turn_green_session_dir.display().to_string();
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --launch-packet-session-dir {expected_launch_packet_session_dir} \"*) : ;;\n  *) echo 'unexpected launch-packet-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --session-dir {expected_turn_green_session_dir} \"*) : ;;\n  *) echo 'unexpected turn-green session dir' >&2; exit 1;;\nesac\nsession_json='{expected_turn_green_session_dir}/tiny_live_activation_package_turn_green.session.json'\nif grep -F '\"launch_packet_session_dir\": \"{expected_launch_packet_session_dir}\"' \"$session_json\" >/dev/null; then\n  cat '{}'\nelse\n  cat '{}'\nfi\n",
            json_path.display(),
            invalid_json_path.display()
        );
        fs::write(path, script).unwrap();
        let mut perms = fs::metadata(path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).unwrap();
    }

    fn write_fake_mode_switching_json_command(
        path: &Path,
        run_json_path: &Path,
        verify_json_path: &Path,
    ) {
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --run-live-package-cutover \"*) cat '{}';;\n  *\" --verify-live-package-cutover \"*) cat '{}';;\n  *) echo 'unexpected mode' >&2; exit 1;;\nesac\n",
            run_json_path.display(),
            verify_json_path.display()
        );
        fs::write(path, script).unwrap();
        let mut perms = fs::metadata(path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).unwrap();
    }

    struct PathGuard {
        _lock: MutexGuard<'static, ()>,
        original_path: Option<OsString>,
    }

    impl PathGuard {
        fn install(bin_dir: &Path) -> Self {
            let lock = env_lock()
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let original_path = std::env::var_os("PATH");
            let mut new_path = OsString::from(bin_dir.as_os_str());
            if let Some(existing) = &original_path {
                new_path.push(":");
                new_path.push(existing);
            }
            std::env::set_var("PATH", &new_path);
            Self {
                _lock: lock,
                original_path,
            }
        }
    }

    impl Drop for PathGuard {
        fn drop(&mut self) {
            match &self.original_path {
                Some(value) => std::env::set_var("PATH", value),
                None => std::env::remove_var("PATH"),
            }
        }
    }

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn temp_dir(label: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("{label}.{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }
}
