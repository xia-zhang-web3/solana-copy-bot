use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const USAGE: &str = "usage: copybot_tiny_live_activation_package_turn_green_latest --root <path> [--session-dir <path>] [--output <path>] [--json] (--plan-latest-turn-green | --render-latest-turn-green-script | --run-latest-turn-green | --verify-latest-turn-green)";
const TURN_GREEN_LATEST_SESSION_VERSION: &str = "1";
const TURN_GREEN_LATEST_STATUS_VERSION: &str = "1";
const LAUNCH_PACKET_LATEST_BIN: &str = "copybot_tiny_live_activation_package_launch_packet_latest";
const TURN_GREEN_BIN: &str = "copybot_tiny_live_activation_package_turn_green";
const LAUNCH_PACKET_LATEST_PLAN_READY: &str = "tiny_live_package_launch_packet_latest_plan_ready";
const LAUNCH_PACKET_LATEST_VERIFY_OK: &str = "tiny_live_package_launch_packet_latest_verify_ok";
const TURN_GREEN_VERIFY_OK: &str = "tiny_live_package_turn_green_verify_ok";
const PLANNING_SAFE_STATEMENT: &str =
    "this latest-turn-green handoff binds the latest persisted immutable package chain to the accepted turn-green contract, but it never overrides Stage 3 or pre-activation refusal truth and it never runs the frozen live cutover controller by itself";

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
    root: Option<PathBuf>,
    session_dir: Option<PathBuf>,
    output_path: Option<PathBuf>,
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    PlanLatestTurnGreen,
    RenderLatestTurnGreenScript,
    RunLatestTurnGreen,
    VerifyLatestTurnGreen,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageTurnGreenLatestVerdict {
    TinyLivePackageTurnGreenLatestPlanReady,
    TinyLivePackageTurnGreenLatestScriptRendered,
    TinyLivePackageTurnGreenLatestRefusedByMissingLatestChain,
    TinyLivePackageTurnGreenLatestRefusedByInvalidLatestChain,
    TinyLivePackageTurnGreenLatestRefusedByDriftedTurnGreenContract,
    TinyLivePackageTurnGreenLatestRefusedNowByStage3,
    TinyLivePackageTurnGreenLatestRefusedNowByPreActivationGate,
    TinyLivePackageTurnGreenLatestExecutableNow,
    TinyLivePackageTurnGreenLatestVerifyOk,
    TinyLivePackageTurnGreenLatestVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LivePackageTurnGreenLatestResult {
    RefusedByMissingLatestChain,
    RefusedByInvalidLatestChain,
    RefusedByDriftedTurnGreenContract,
    RefusedNowByStage3,
    RefusedNowByPreActivationGate,
    ExecutableNow,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageTurnGreenLatestStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageTurnGreenLatestSession {
    session_version: String,
    planned_at: DateTime<Utc>,
    root: String,
    session_dir: String,
    nested_launch_packet_latest_session_dir: String,
    nested_turn_green_session_dir: String,
    downstream_launch_packet_session_dir: String,
    latest_top_step_name: Option<String>,
    latest_top_session_dir: Option<String>,
    package_dir: Option<String>,
    install_root: Option<String>,
    target_service_name: Option<String>,
    backend_command: Option<String>,
    wrapper_timeout_ms: Option<u64>,
    service_status_max_staleness_ms: Option<u64>,
    resolve_latest_launch_packet_command_summary: String,
    run_latest_launch_packet_command_summary: String,
    verify_latest_launch_packet_command_summary: String,
    downstream_turn_green_command_summary: String,
    verify_turn_green_command_summary: String,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    clerestory_certificate_summary: Option<String>,
    clerestory_certificate_sha256: Option<String>,
    clerestory_certificate_algorithm: Option<String>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageTurnGreenLatestStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    root: String,
    session_dir: String,
    latest_top_step_name: Option<String>,
    latest_top_session_dir: Option<String>,
    nested_launch_packet_latest_session_dir: String,
    nested_turn_green_session_dir: String,
    downstream_launch_packet_session_dir: String,
    result: LivePackageTurnGreenLatestResult,
    reason: String,
    latest_launch_packet_plan_verdict: Option<String>,
    latest_launch_packet_verdict: Option<String>,
    turn_green_verdict: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    latest_launch_packet_plan_step: Option<PackageTurnGreenLatestStepArtifact>,
    latest_launch_packet_step: Option<PackageTurnGreenLatestStepArtifact>,
    turn_green_step: Option<PackageTurnGreenLatestStepArtifact>,
    operator_next_action_summary: String,
    executable_now: bool,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackageTurnGreenLatestPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    report_path: PathBuf,
    latest_launch_packet_plan_report_path: PathBuf,
    latest_launch_packet_report_path: PathBuf,
    turn_green_report_path: PathBuf,
    nested_launch_packet_latest_session_dir: PathBuf,
    nested_turn_green_session_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageTurnGreenLatestReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageTurnGreenLatestVerdict,
    reason: String,
    root: Option<String>,
    latest_top_step_name: Option<String>,
    latest_top_session_dir: Option<String>,
    package_dir: Option<String>,
    install_root: Option<String>,
    target_service_name: Option<String>,
    backend_command: Option<String>,
    wrapper_timeout_ms: Option<u64>,
    service_status_max_staleness_ms: Option<u64>,
    session_dir: Option<String>,
    turn_green_latest_session_path: Option<String>,
    turn_green_latest_status_path: Option<String>,
    turn_green_latest_report_path: Option<String>,
    latest_launch_packet_plan_report_path: Option<String>,
    latest_launch_packet_report_path: Option<String>,
    turn_green_report_path: Option<String>,
    nested_launch_packet_latest_session_dir: Option<String>,
    nested_turn_green_session_dir: Option<String>,
    downstream_launch_packet_session_dir: Option<String>,
    latest_launch_packet_plan_verdict: Option<String>,
    latest_launch_packet_verdict: Option<String>,
    turn_green_verdict: Option<String>,
    result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    resolve_latest_launch_packet_command_summary: Option<String>,
    run_latest_launch_packet_command_summary: Option<String>,
    verify_latest_launch_packet_command_summary: Option<String>,
    downstream_turn_green_command_summary: Option<String>,
    verify_turn_green_command_summary: Option<String>,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    clerestory_certificate_summary: Option<String>,
    clerestory_certificate_sha256: Option<String>,
    clerestory_certificate_algorithm: Option<String>,
    verification_mismatches: Vec<String>,
    executable_now: bool,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct LatestTurnGreenSnapshot {
    latest_top_step_name: String,
    latest_top_session_dir: PathBuf,
    downstream_launch_packet_session_dir: PathBuf,
    package_dir: PathBuf,
    install_root: PathBuf,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    reviewed_frozen_live_cutover_controller_command_summary: String,
    clerestory_certificate_summary: String,
    clerestory_certificate_sha256: String,
    clerestory_certificate_algorithm: String,
}

#[derive(Debug, Clone)]
struct ResolvedLatestTurnGreen {
    snapshot: LatestTurnGreenSnapshot,
    latest_launch_packet_plan_raw: Value,
    latest_launch_packet_plan: LaunchPacketLatestReportView,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResolutionFailureKind {
    MissingLatestChain,
    InvalidLatestChain,
    DriftedTurnGreenContract,
}

#[derive(Debug, Clone)]
struct LatestTurnGreenResolutionFailure {
    kind: ResolutionFailureKind,
    reason: String,
    snapshot: Option<LatestTurnGreenSnapshot>,
    latest_launch_packet_plan_raw: Option<Value>,
    latest_launch_packet_plan_verdict: Option<String>,
    latest_launch_packet_plan_reason: Option<String>,
    latest_launch_packet_plan_generated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Deserialize)]
struct LaunchPacketLatestReportView {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: String,
    reason: String,
    latest_top_step_name: Option<String>,
    latest_top_session_dir: Option<String>,
    package_dir: Option<String>,
    install_root: Option<String>,
    target_service_name: Option<String>,
    backend_command: Option<String>,
    wrapper_timeout_ms: Option<u64>,
    service_status_max_staleness_ms: Option<u64>,
    session_dir: Option<String>,
    nested_launch_packet_session_dir: Option<String>,
    result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    clerestory_certificate_summary: Option<String>,
    clerestory_certificate_sha256: Option<String>,
    clerestory_certificate_algorithm: Option<String>,
    explicit_statement: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct TurnGreenReportView {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: String,
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
    frozen_controller_still_current: bool,
    verify_frozen_launch_packet_summary: Option<String>,
    refresh_current_authorization_command_summary: Option<String>,
    frozen_live_cutover_controller_command_summary: Option<String>,
    current_live_cutover_controller_command_summary: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    operator_next_action_summary: Option<String>,
    executable_now: bool,
    explicit_statement: String,
}

#[derive(Debug, Clone, Deserialize)]
struct TurnGreenSessionView {
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

#[derive(Debug, Clone, Deserialize)]
struct TurnGreenStatusView {
    session_dir: String,
    launch_packet_session_dir: String,
    nested_current_authorization_session_dir: String,
    result: String,
    reason: String,
    frozen_launch_packet_result: Option<String>,
    current_authorization_result_now: Option<String>,
    frozen_controller_still_current: bool,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    operator_next_action_summary: String,
    executable_now: bool,
    explicit_statement: String,
}

fn parse_args() -> Result<Option<Config>> {
    parse_args_from(env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> Result<Option<Config>>
where
    I: IntoIterator<Item = String>,
{
    let mut root = None;
    let mut session_dir = None;
    let mut output_path = None;
    let mut json = false;
    let mut mode = None;

    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--root" => root = Some(PathBuf::from(parse_string_arg("--root", iter.next())?)),
            "--session-dir" => {
                session_dir = Some(PathBuf::from(parse_string_arg(
                    "--session-dir",
                    iter.next(),
                )?))
            }
            "--output" => {
                output_path = Some(PathBuf::from(parse_string_arg("--output", iter.next())?))
            }
            "--json" => json = true,
            "--plan-latest-turn-green" => {
                set_mode(&mut mode, Mode::PlanLatestTurnGreen, arg.as_str())?
            }
            "--render-latest-turn-green-script" => {
                set_mode(&mut mode, Mode::RenderLatestTurnGreenScript, arg.as_str())?
            }
            "--run-latest-turn-green" => {
                set_mode(&mut mode, Mode::RunLatestTurnGreen, arg.as_str())?
            }
            "--verify-latest-turn-green" => {
                set_mode(&mut mode, Mode::VerifyLatestTurnGreen, arg.as_str())?
            }
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown arg {other}; {USAGE}"),
        }
    }

    let Some(mode) = mode else {
        return Ok(None);
    };
    if matches!(
        mode,
        Mode::PlanLatestTurnGreen | Mode::RenderLatestTurnGreenScript | Mode::RunLatestTurnGreen
    ) && root.is_none()
    {
        bail!("missing --root");
    }
    if matches!(mode, Mode::RenderLatestTurnGreenScript) && output_path.is_none() {
        bail!("missing --output for render mode");
    }
    if matches!(mode, Mode::RunLatestTurnGreen | Mode::VerifyLatestTurnGreen)
        && session_dir.is_none()
    {
        bail!("missing --session-dir");
    }

    Ok(Some(Config {
        mode,
        root,
        session_dir,
        output_path,
        json,
    }))
}

fn run(config: Config) -> Result<String> {
    let report = match config.mode {
        Mode::PlanLatestTurnGreen => plan_latest_turn_green_report(&config)?,
        Mode::RenderLatestTurnGreenScript => render_latest_turn_green_script_report(&config)?,
        Mode::RunLatestTurnGreen => run_latest_turn_green_report(&config)?,
        Mode::VerifyLatestTurnGreen => verify_latest_turn_green_report(&config)?,
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_report_lines(&report))
    }
}

fn plan_latest_turn_green_report(config: &Config) -> Result<PackageTurnGreenLatestReport> {
    let root = config
        .root
        .as_ref()
        .ok_or_else(|| anyhow!("missing --root"))?;
    let suggested_session_dir = config
        .session_dir
        .clone()
        .unwrap_or_else(|| root.join("tiny_live_activation_package_turn_green_latest.session"));
    let paths = turn_green_latest_paths(&suggested_session_dir);

    match resolve_latest_turn_green(root, &paths) {
        Ok(resolved) => Ok(plan_report_from_resolution(
            "plan_latest_turn_green",
            TinyLivePackageTurnGreenLatestVerdict::TinyLivePackageTurnGreenLatestPlanReady,
            format!(
                "latest immutable {} chain under {} resolves to exact latest-launch-packet {} lineage and the accepted turn-green contract remains current",
                resolved.snapshot.latest_top_step_name,
                root.display(),
                paths.nested_launch_packet_latest_session_dir.display(),
            ),
            root,
            &suggested_session_dir,
            &paths,
            &resolved,
        )),
        Err(failure) => Ok(report_from_resolution_failure(
            "plan_latest_turn_green",
            root,
            &suggested_session_dir,
            &paths,
            &failure,
        )),
    }
}

fn render_latest_turn_green_script_report(config: &Config) -> Result<PackageTurnGreenLatestReport> {
    let root = config
        .root
        .as_ref()
        .ok_or_else(|| anyhow!("missing --root"))?;
    let output_path = config
        .output_path
        .as_ref()
        .ok_or_else(|| anyhow!("missing --output for render mode"))?;
    let suggested_session_dir = config
        .session_dir
        .clone()
        .unwrap_or_else(|| output_path.with_extension("session"));
    let paths = turn_green_latest_paths(&suggested_session_dir);

    match resolve_latest_turn_green(root, &paths) {
        Ok(resolved) => {
            write_script(
                output_path,
                &format!(
                    "#!/usr/bin/env bash\nset -euo pipefail\n\n{run_latest}\n{verify_latest}\n{run_turn_green}\n{verify_turn_green}\n",
                    run_latest = run_latest_launch_packet_command_summary(
                        root,
                        &paths.nested_launch_packet_latest_session_dir,
                    ),
                    verify_latest = verify_latest_launch_packet_command_summary(
                        &paths.nested_launch_packet_latest_session_dir,
                    ),
                    run_turn_green = downstream_turn_green_command_summary(
                        &resolved.snapshot,
                        &paths.nested_turn_green_session_dir,
                    ),
                    verify_turn_green = verify_turn_green_command_summary(
                        &resolved.snapshot,
                        &paths.nested_turn_green_session_dir,
                    ),
                ),
            )?;
            Ok(plan_report_from_resolution(
                "render_latest_turn_green_script",
                TinyLivePackageTurnGreenLatestVerdict::TinyLivePackageTurnGreenLatestScriptRendered,
                format!(
                    "rendered latest-turn-green handoff script to {} using the accepted latest-launch-packet and turn-green contracts",
                    output_path.display()
                ),
                root,
                &suggested_session_dir,
                &paths,
                &resolved,
            ))
        }
        Err(failure) => Ok(report_from_resolution_failure(
            "render_latest_turn_green_script",
            root,
            &suggested_session_dir,
            &paths,
            &failure,
        )),
    }
}

fn run_latest_turn_green_report(config: &Config) -> Result<PackageTurnGreenLatestReport> {
    let root = config
        .root
        .as_ref()
        .ok_or_else(|| anyhow!("missing --root"))?;
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for run mode"))?;
    if session_dir.exists() {
        bail!(
            "refusing to overwrite existing latest-turn-green session dir {}",
            session_dir.display()
        );
    }
    let paths = turn_green_latest_paths(session_dir);
    create_clean_session_dir(session_dir, "latest-turn-green")?;

    let resolution = resolve_latest_turn_green(root, &paths);
    match resolution {
        Ok(resolved) => run_latest_turn_green_with_resolution(root, session_dir, &paths, &resolved),
        Err(failure) => run_latest_turn_green_failure(root, session_dir, &paths, &failure),
    }
}

fn verify_latest_turn_green_report(config: &Config) -> Result<PackageTurnGreenLatestReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for verify mode"))?;
    let paths = turn_green_latest_paths(session_dir);

    let session: PackageTurnGreenLatestSession = match load_json(&paths.session_path) {
        Ok(value) => value,
        Err(error) => {
            return Ok(verify_invalid_report_without_session(
                session_dir,
                vec![format!(
                    "failed reading latest-turn-green session artifact {}: {error}",
                    paths.session_path.display()
                )],
            ))
        }
    };
    let status: PackageTurnGreenLatestStatus = match load_json(&paths.status_path) {
        Ok(value) => value,
        Err(error) => {
            return Ok(verify_invalid_report_without_session(
                session_dir,
                vec![format!(
                    "failed reading latest-turn-green status artifact {}: {error}",
                    paths.status_path.display()
                )],
            ))
        }
    };
    let stored_report: PackageTurnGreenLatestReport = match load_json(&paths.report_path) {
        Ok(value) => value,
        Err(error) => {
            return Ok(verify_invalid_report_without_session(
                session_dir,
                vec![format!(
                    "failed reading latest-turn-green report artifact {}: {error}",
                    paths.report_path.display()
                )],
            ))
        }
    };

    let mut mismatches = Vec::new();
    let root = PathBuf::from(&session.root);

    compare_string(
        &session.root,
        &status.root,
        "latest-turn-green root consistency",
        &mut mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "latest-turn-green session session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.session_dir,
        &session_dir.display().to_string(),
        "latest-turn-green status session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "latest-turn-green report session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.nested_launch_packet_latest_session_dir,
        &paths
            .nested_launch_packet_latest_session_dir
            .display()
            .to_string(),
        "latest-turn-green session nested_launch_packet_latest_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.nested_launch_packet_latest_session_dir,
        &paths
            .nested_launch_packet_latest_session_dir
            .display()
            .to_string(),
        "latest-turn-green status nested_launch_packet_latest_session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report
            .nested_launch_packet_latest_session_dir
            .as_deref(),
        Some(
            paths
                .nested_launch_packet_latest_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "latest-turn-green report nested_launch_packet_latest_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.nested_turn_green_session_dir,
        &paths.nested_turn_green_session_dir.display().to_string(),
        "latest-turn-green session nested_turn_green_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.nested_turn_green_session_dir,
        &paths.nested_turn_green_session_dir.display().to_string(),
        "latest-turn-green status nested_turn_green_session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.nested_turn_green_session_dir.as_deref(),
        Some(
            paths
                .nested_turn_green_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "latest-turn-green report nested_turn_green_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.downstream_launch_packet_session_dir,
        &expected_downstream_launch_packet_session_dir(&paths)
            .display()
            .to_string(),
        "latest-turn-green session downstream_launch_packet_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.downstream_launch_packet_session_dir,
        &expected_downstream_launch_packet_session_dir(&paths)
            .display()
            .to_string(),
        "latest-turn-green status downstream_launch_packet_session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report
            .downstream_launch_packet_session_dir
            .as_deref(),
        Some(
            expected_downstream_launch_packet_session_dir(&paths)
                .display()
                .to_string()
                .as_str(),
        ),
        "latest-turn-green report downstream_launch_packet_session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.turn_green_latest_session_path.as_deref(),
        Some(paths.session_path.display().to_string().as_str()),
        "latest-turn-green report session path",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.turn_green_latest_status_path.as_deref(),
        Some(paths.status_path.display().to_string().as_str()),
        "latest-turn-green report status path",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.turn_green_latest_report_path.as_deref(),
        Some(paths.report_path.display().to_string().as_str()),
        "latest-turn-green report report path",
        &mut mismatches,
    );
    compare_string(
        &session.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "latest-turn-green session explicit_statement",
        &mut mismatches,
    );
    compare_string(
        &status.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "latest-turn-green status explicit_statement",
        &mut mismatches,
    );
    compare_string(
        &stored_report.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "latest-turn-green report explicit_statement",
        &mut mismatches,
    );
    compare_string(
        &stored_report.mode,
        "run_latest_turn_green",
        "latest-turn-green stored report mode",
        &mut mismatches,
    );
    compare_string(
        &serialize_enum(&stored_report.verdict),
        &serialize_enum(&verdict_for_result(status.result)),
        "latest-turn-green stored report verdict",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.result.as_deref(),
        Some(serialize_enum(&status.result).as_str()),
        "latest-turn-green stored report result consistency",
        &mut mismatches,
    );
    compare_string(
        &stored_report.reason,
        &status.reason,
        "latest-turn-green stored report reason consistency",
        &mut mismatches,
    );
    if stored_report.executable_now != status.executable_now {
        mismatches.push(format!(
            "latest-turn-green stored report executable_now {} does not match status {}",
            stored_report.executable_now, status.executable_now
        ));
    }
    compare_option_string(
        stored_report.latest_launch_packet_plan_verdict.as_deref(),
        status.latest_launch_packet_plan_verdict.as_deref(),
        "stored report latest_launch_packet_plan_verdict",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.latest_launch_packet_verdict.as_deref(),
        status.latest_launch_packet_verdict.as_deref(),
        "stored report latest_launch_packet_verdict",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.turn_green_verdict.as_deref(),
        status.turn_green_verdict.as_deref(),
        "stored report turn_green_verdict",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.current_pre_activation_gate_verdict.as_deref(),
        status.current_pre_activation_gate_verdict.as_deref(),
        "stored report current_pre_activation_gate_verdict",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.current_pre_activation_gate_reason.as_deref(),
        status.current_pre_activation_gate_reason.as_deref(),
        "stored report current_pre_activation_gate_reason",
        &mut mismatches,
    );

    let resolution = resolve_latest_turn_green(&root, &paths);
    match resolution {
        Ok(resolved) => {
            compare_session_against_snapshot(
                &session,
                &root,
                session_dir,
                &paths,
                &resolved.snapshot,
                &mut mismatches,
            );
            compare_report_against_snapshot(
                &stored_report,
                &root,
                session_dir,
                &paths,
                &resolved.snapshot,
                &mut mismatches,
            );

            let stored_latest_plan: LaunchPacketLatestReportView = load_required_step_json(
                &status.latest_launch_packet_plan_step,
                &paths.latest_launch_packet_plan_report_path,
                "latest_launch_packet_plan_step",
                &mut mismatches,
            )?;
            compare_step_artifact_against_report_metadata(
                status.latest_launch_packet_plan_step.as_ref(),
                &paths.latest_launch_packet_plan_report_path,
                "plan_latest_launch_packet",
                &stored_latest_plan.verdict,
                &stored_latest_plan.reason,
                stored_latest_plan.generated_at,
                "stored latest_launch_packet_plan_step",
                &mut mismatches,
            );
            compare_launch_packet_latest_plan_against_snapshot(
                &stored_latest_plan,
                &resolved.snapshot,
                &paths.nested_launch_packet_latest_session_dir,
                &mut mismatches,
            );
            compare_option_string(
                status.latest_launch_packet_plan_verdict.as_deref(),
                Some(stored_latest_plan.verdict.as_str()),
                "stored status latest_launch_packet_plan_verdict",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.latest_launch_packet_plan_verdict.as_deref(),
                Some(stored_latest_plan.verdict.as_str()),
                "stored report latest_launch_packet_plan_verdict",
                &mut mismatches,
            );

            let stored_latest_run: LaunchPacketLatestReportView = load_required_step_json(
                &status.latest_launch_packet_step,
                &paths.latest_launch_packet_report_path,
                "latest_launch_packet_step",
                &mut mismatches,
            )?;
            compare_step_artifact_against_report_metadata(
                status.latest_launch_packet_step.as_ref(),
                &paths.latest_launch_packet_report_path,
                "run_latest_launch_packet",
                &stored_latest_run.verdict,
                &stored_latest_run.reason,
                stored_latest_run.generated_at,
                "stored latest_launch_packet_step",
                &mut mismatches,
            );
            compare_launch_packet_latest_run_against_snapshot(
                &stored_latest_run,
                &resolved.snapshot,
                &paths.nested_launch_packet_latest_session_dir,
                &mut mismatches,
            );
            compare_option_string(
                status.latest_launch_packet_verdict.as_deref(),
                Some(stored_latest_run.verdict.as_str()),
                "stored status latest_launch_packet_verdict",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.latest_launch_packet_verdict.as_deref(),
                Some(stored_latest_run.verdict.as_str()),
                "stored report latest_launch_packet_verdict",
                &mut mismatches,
            );

            let actual_latest_run: LaunchPacketLatestReportView = match load_json(
                &nested_launch_packet_latest_report_path(
                    &paths.nested_launch_packet_latest_session_dir,
                ),
            ) {
                Ok(value) => value,
                Err(error) => {
                    mismatches.push(format!(
                        "failed loading actual nested latest-launch-packet report under {}: {error}",
                        paths.nested_launch_packet_latest_session_dir.display()
                    ));
                    fallback_launch_packet_latest_copy()
                }
            };
            compare_launch_packet_latest_copy_matches_actual(
                &stored_latest_run,
                &actual_latest_run,
                &mut mismatches,
            );

            let latest_verify_raw = match run_json_command(
                LAUNCH_PACKET_LATEST_BIN,
                &latest_launch_packet_verify_args(&paths.nested_launch_packet_latest_session_dir),
            ) {
                Ok(value) => value,
                Err(error) => {
                    mismatches.push(format!(
                        "failed verifying nested latest-launch-packet session under {}: {error}",
                        paths.nested_launch_packet_latest_session_dir.display()
                    ));
                    Value::Null
                }
            };
            if !latest_verify_raw.is_null() {
                let latest_verify: LaunchPacketLatestReportView =
                    serde_json::from_value(latest_verify_raw)
                        .context("failed parsing nested latest-launch-packet verify json")?;
                if latest_verify.verdict != LAUNCH_PACKET_LATEST_VERIFY_OK {
                    mismatches.push(format!(
                        "nested latest-launch-packet verification is non-green: {}",
                        latest_verify.reason
                    ));
                }
            }

            let stored_turn_green: TurnGreenReportView = load_required_step_json(
                &status.turn_green_step,
                &paths.turn_green_report_path,
                "turn_green_step",
                &mut mismatches,
            )?;
            compare_step_artifact_against_report_metadata(
                status.turn_green_step.as_ref(),
                &paths.turn_green_report_path,
                "run_live_package_turn_green",
                &stored_turn_green.verdict,
                &stored_turn_green.reason,
                stored_turn_green.generated_at,
                "stored turn_green_step",
                &mut mismatches,
            );
            compare_turn_green_run_against_snapshot(
                &stored_turn_green,
                &resolved.snapshot,
                &paths.nested_turn_green_session_dir,
                &mut mismatches,
            );
            compare_option_string(
                status.turn_green_verdict.as_deref(),
                Some(stored_turn_green.verdict.as_str()),
                "stored status turn_green_verdict",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.turn_green_verdict.as_deref(),
                Some(stored_turn_green.verdict.as_str()),
                "stored report turn_green_verdict",
                &mut mismatches,
            );

            let nested_turn_green_session: TurnGreenSessionView = match load_json(
                &nested_turn_green_session_path(&paths.nested_turn_green_session_dir),
            ) {
                Ok(value) => value,
                Err(error) => {
                    mismatches.push(format!(
                        "failed reading nested turn-green session under {}: {error}",
                        paths.nested_turn_green_session_dir.display()
                    ));
                    fallback_turn_green_session(
                        &resolved.snapshot,
                        &paths.nested_turn_green_session_dir,
                    )
                }
            };
            let nested_turn_green_status: TurnGreenStatusView = match load_json(
                &nested_turn_green_status_path(&paths.nested_turn_green_session_dir),
            ) {
                Ok(value) => value,
                Err(error) => {
                    mismatches.push(format!(
                        "failed reading nested turn-green status under {}: {error}",
                        paths.nested_turn_green_session_dir.display()
                    ));
                    fallback_turn_green_status(
                        &resolved.snapshot,
                        &paths.nested_turn_green_session_dir,
                    )
                }
            };
            compare_turn_green_copy_against_nested_truth(
                &stored_turn_green,
                &nested_turn_green_session,
                &nested_turn_green_status,
                &mut mismatches,
            );

            let turn_green_verify_raw = match run_json_command(
                TURN_GREEN_BIN,
                &turn_green_verify_args(&resolved.snapshot, &paths.nested_turn_green_session_dir),
            ) {
                Ok(value) => value,
                Err(error) => {
                    mismatches.push(format!(
                        "failed verifying nested turn-green session under {}: {error}",
                        paths.nested_turn_green_session_dir.display()
                    ));
                    Value::Null
                }
            };
            if !turn_green_verify_raw.is_null() {
                let turn_green_verify: TurnGreenReportView =
                    serde_json::from_value(turn_green_verify_raw)
                        .context("failed parsing nested turn-green verify json")?;
                compare_option_string(
                    stored_turn_green
                        .current_live_cutover_controller_command_summary
                        .as_deref(),
                    turn_green_verify
                        .current_live_cutover_controller_command_summary
                        .as_deref(),
                    "stored turn-green copy current_live_cutover_controller_command_summary",
                    &mut mismatches,
                );
                if turn_green_verify.verdict != TURN_GREEN_VERIFY_OK {
                    mismatches.push(format!(
                        "nested turn-green verification is non-green: {}",
                        turn_green_verify.reason
                    ));
                }
            }

            let expected_result =
                turn_green_latest_result_from_turn_green(nested_turn_green_status.result.as_str())
                    .unwrap_or(LivePackageTurnGreenLatestResult::RefusedByDriftedTurnGreenContract);
            if status.result != expected_result {
                mismatches.push(format!(
                    "stored latest-turn-green result {} does not match nested turn-green result {}",
                    serialize_enum(&status.result),
                    serialize_enum(&expected_result)
                ));
            }
            compare_string(
                &status.reason,
                &nested_turn_green_status.reason,
                "stored status reason",
                &mut mismatches,
            );
            compare_string(
                &status.operator_next_action_summary,
                &nested_turn_green_status.operator_next_action_summary,
                "stored status operator_next_action_summary",
                &mut mismatches,
            );
            if status.executable_now != nested_turn_green_status.executable_now {
                mismatches.push(format!(
                    "stored status executable_now {} does not match nested turn-green status {}",
                    status.executable_now, nested_turn_green_status.executable_now
                ));
            }
            compare_option_string(
                status.current_pre_activation_gate_verdict.as_deref(),
                nested_turn_green_status
                    .current_pre_activation_gate_verdict
                    .as_deref(),
                "stored status current_pre_activation_gate_verdict",
                &mut mismatches,
            );
            compare_option_string(
                status.current_pre_activation_gate_reason.as_deref(),
                nested_turn_green_status
                    .current_pre_activation_gate_reason
                    .as_deref(),
                "stored status current_pre_activation_gate_reason",
                &mut mismatches,
            );
        }
        Err(failure) => {
            compare_session_against_failure(
                &session,
                &root,
                session_dir,
                &paths,
                &failure,
                &mut mismatches,
            );
            compare_report_against_failure(
                &stored_report,
                &root,
                session_dir,
                &paths,
                &failure,
                &mut mismatches,
            );
            if failure.latest_launch_packet_plan_raw.is_some() {
                let stored_latest_plan: LaunchPacketLatestReportView = load_required_step_json(
                    &status.latest_launch_packet_plan_step,
                    &paths.latest_launch_packet_plan_report_path,
                    "latest_launch_packet_plan_step",
                    &mut mismatches,
                )?;
                compare_step_artifact_against_report_metadata(
                    status.latest_launch_packet_plan_step.as_ref(),
                    &paths.latest_launch_packet_plan_report_path,
                    "plan_latest_launch_packet",
                    &stored_latest_plan.verdict,
                    &stored_latest_plan.reason,
                    stored_latest_plan.generated_at,
                    "stored latest_launch_packet_plan_step",
                    &mut mismatches,
                );
                compare_option_string(
                    status.latest_launch_packet_plan_verdict.as_deref(),
                    failure.latest_launch_packet_plan_verdict.as_deref(),
                    "stored status latest_launch_packet_plan_verdict on failure",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_report.latest_launch_packet_plan_verdict.as_deref(),
                    failure.latest_launch_packet_plan_verdict.as_deref(),
                    "stored report latest_launch_packet_plan_verdict on failure",
                    &mut mismatches,
                );
            }
            compare_option_string(
                status.latest_launch_packet_verdict.as_deref(),
                None,
                "stored status latest_launch_packet_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.latest_launch_packet_verdict.as_deref(),
                None,
                "stored report latest_launch_packet_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                status.turn_green_verdict.as_deref(),
                None,
                "stored status turn_green_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.turn_green_verdict.as_deref(),
                None,
                "stored report turn_green_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                status.current_pre_activation_gate_verdict.as_deref(),
                None,
                "stored status current_pre_activation_gate_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.current_pre_activation_gate_verdict.as_deref(),
                None,
                "stored report current_pre_activation_gate_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                status.current_pre_activation_gate_reason.as_deref(),
                None,
                "stored status current_pre_activation_gate_reason on failure",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.current_pre_activation_gate_reason.as_deref(),
                None,
                "stored report current_pre_activation_gate_reason on failure",
                &mut mismatches,
            );
            compare_string(
                &status.reason,
                &failure.reason,
                "stored status failure reason",
                &mut mismatches,
            );
            compare_string(
                &stored_report.reason,
                &failure.reason,
                "stored report failure reason",
                &mut mismatches,
            );
            compare_string(
                &status.operator_next_action_summary,
                &operator_next_action_summary(result_from_failure_kind(failure.kind)),
                "stored status operator_next_action_summary on failure",
                &mut mismatches,
            );
            if status.executable_now {
                mismatches
                    .push("stored status executable_now must be false on failure".to_string());
            }
            if stored_report.executable_now {
                mismatches
                    .push("stored report executable_now must be false on failure".to_string());
            }
        }
    }

    let verdict = if mismatches.is_empty() {
        TinyLivePackageTurnGreenLatestVerdict::TinyLivePackageTurnGreenLatestVerifyOk
    } else {
        TinyLivePackageTurnGreenLatestVerdict::TinyLivePackageTurnGreenLatestVerifyInvalid
    };
    let reason = if mismatches.is_empty() {
        format!(
            "latest-turn-green session under {} is coherent with the current latest immutable chain and the accepted turn-green contract",
            session_dir.display()
        )
    } else {
        mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "latest-turn-green verification failed".to_string())
    };

    Ok(PackageTurnGreenLatestReport {
        generated_at: Utc::now(),
        mode: "verify_latest_turn_green".to_string(),
        verdict,
        reason,
        root: Some(session.root.clone()),
        latest_top_step_name: status.latest_top_step_name.clone(),
        latest_top_session_dir: status.latest_top_session_dir.clone(),
        package_dir: session.package_dir.clone(),
        install_root: session.install_root.clone(),
        target_service_name: session.target_service_name.clone(),
        backend_command: session.backend_command.clone(),
        wrapper_timeout_ms: session.wrapper_timeout_ms,
        service_status_max_staleness_ms: session.service_status_max_staleness_ms,
        session_dir: Some(session_dir.display().to_string()),
        turn_green_latest_session_path: Some(paths.session_path.display().to_string()),
        turn_green_latest_status_path: Some(paths.status_path.display().to_string()),
        turn_green_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_launch_packet_plan_report_path: Some(
            paths
                .latest_launch_packet_plan_report_path
                .display()
                .to_string(),
        ),
        latest_launch_packet_report_path: Some(
            paths.latest_launch_packet_report_path.display().to_string(),
        ),
        turn_green_report_path: Some(paths.turn_green_report_path.display().to_string()),
        nested_launch_packet_latest_session_dir: Some(
            paths
                .nested_launch_packet_latest_session_dir
                .display()
                .to_string(),
        ),
        nested_turn_green_session_dir: Some(
            paths.nested_turn_green_session_dir.display().to_string(),
        ),
        downstream_launch_packet_session_dir: Some(
            expected_downstream_launch_packet_session_dir(&paths)
                .display()
                .to_string(),
        ),
        latest_launch_packet_plan_verdict: status.latest_launch_packet_plan_verdict.clone(),
        latest_launch_packet_verdict: status.latest_launch_packet_verdict.clone(),
        turn_green_verdict: status.turn_green_verdict.clone(),
        result: Some(serialize_enum(&status.result)),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        resolve_latest_launch_packet_command_summary: Some(
            session.resolve_latest_launch_packet_command_summary.clone(),
        ),
        run_latest_launch_packet_command_summary: Some(
            session.run_latest_launch_packet_command_summary.clone(),
        ),
        verify_latest_launch_packet_command_summary: Some(
            session.verify_latest_launch_packet_command_summary.clone(),
        ),
        downstream_turn_green_command_summary: Some(
            session.downstream_turn_green_command_summary.clone(),
        ),
        verify_turn_green_command_summary: Some(session.verify_turn_green_command_summary.clone()),
        reviewed_frozen_live_cutover_controller_command_summary: session
            .reviewed_frozen_live_cutover_controller_command_summary
            .clone(),
        clerestory_certificate_summary: session.clerestory_certificate_summary.clone(),
        clerestory_certificate_sha256: session.clerestory_certificate_sha256.clone(),
        clerestory_certificate_algorithm: session.clerestory_certificate_algorithm.clone(),
        verification_mismatches: mismatches,
        executable_now: status.executable_now,
        explicit_statement:
            "this verification proves only whether the latest immutable chain still resolves cleanly into the accepted turn-green contract; it never runs the frozen controller"
                .to_string(),
    })
}

fn run_latest_turn_green_with_resolution(
    root: &Path,
    session_dir: &Path,
    paths: &PackageTurnGreenLatestPaths,
    resolved: &ResolvedLatestTurnGreen,
) -> Result<PackageTurnGreenLatestReport> {
    let session = planned_session(root, session_dir, paths, Some(resolved), None);
    persist_json(&paths.session_path, &session)?;
    persist_json(
        &paths.latest_launch_packet_plan_report_path,
        &resolved.latest_launch_packet_plan_raw,
    )?;

    let latest_launch_packet_plan_step = Some(step_artifact(
        &paths.latest_launch_packet_plan_report_path,
        "plan_latest_launch_packet",
        &resolved.latest_launch_packet_plan.verdict,
        &resolved.latest_launch_packet_plan.reason,
        resolved.latest_launch_packet_plan.generated_at,
    ));

    let latest_launch_packet_run_raw = match run_json_command(
        LAUNCH_PACKET_LATEST_BIN,
        &latest_launch_packet_run_args(root, &paths.nested_launch_packet_latest_session_dir),
    ) {
        Ok(value) => value,
        Err(error) => {
            let status = refusal_status(
                root,
                session_dir,
                paths,
                Some(resolved),
                LivePackageTurnGreenLatestResult::RefusedByDriftedTurnGreenContract,
                format!(
                    "failed running nested latest-launch-packet handoff under {}: {error}",
                    paths.nested_launch_packet_latest_session_dir.display()
                ),
                latest_launch_packet_plan_step,
                None,
                None,
            );
            persist_json(&paths.status_path, &status)?;
            let report = report_from_status(
                "run_latest_turn_green",
                verdict_for_result(status.result),
                session_dir,
                paths,
                &session,
                &status,
                Vec::new(),
            );
            persist_json(&paths.report_path, &report)?;
            return Ok(report);
        }
    };
    persist_json(
        &paths.latest_launch_packet_report_path,
        &latest_launch_packet_run_raw,
    )?;
    let latest_launch_packet_run: LaunchPacketLatestReportView =
        serde_json::from_value(latest_launch_packet_run_raw)
            .context("failed parsing nested latest-launch-packet run json")?;
    let latest_launch_packet_step = Some(step_artifact(
        &paths.latest_launch_packet_report_path,
        "run_latest_launch_packet",
        &latest_launch_packet_run.verdict,
        &latest_launch_packet_run.reason,
        latest_launch_packet_run.generated_at,
    ));

    if let Some(terminal_failure) = latest_launch_packet_terminal_failure(&latest_launch_packet_run)
    {
        let status = refusal_status(
            root,
            session_dir,
            paths,
            Some(resolved),
            terminal_failure,
            latest_launch_packet_run.reason.clone(),
            latest_launch_packet_plan_step,
            latest_launch_packet_step,
            None,
        );
        persist_json(&paths.status_path, &status)?;
        let report = report_from_status(
            "run_latest_turn_green",
            verdict_for_result(status.result),
            session_dir,
            paths,
            &session,
            &status,
            Vec::new(),
        );
        persist_json(&paths.report_path, &report)?;
        return Ok(report);
    }

    let mut run_mismatches = Vec::new();
    compare_launch_packet_latest_run_against_snapshot(
        &latest_launch_packet_run,
        &resolved.snapshot,
        &paths.nested_launch_packet_latest_session_dir,
        &mut run_mismatches,
    );
    if !run_mismatches.is_empty() {
        let status = refusal_status(
            root,
            session_dir,
            paths,
            Some(resolved),
            LivePackageTurnGreenLatestResult::RefusedByDriftedTurnGreenContract,
            run_mismatches[0].clone(),
            latest_launch_packet_plan_step,
            latest_launch_packet_step,
            None,
        );
        persist_json(&paths.status_path, &status)?;
        let report = report_from_status(
            "run_latest_turn_green",
            verdict_for_result(status.result),
            session_dir,
            paths,
            &session,
            &status,
            Vec::new(),
        );
        persist_json(&paths.report_path, &report)?;
        return Ok(report);
    }

    let latest_launch_packet_verify: LaunchPacketLatestReportView = match run_json_command(
        LAUNCH_PACKET_LATEST_BIN,
        &latest_launch_packet_verify_args(&paths.nested_launch_packet_latest_session_dir),
    ) {
        Ok(value) => serde_json::from_value(value)
            .context("failed parsing nested latest-launch-packet verify json")?,
        Err(error) => {
            let status = refusal_status(
                root,
                session_dir,
                paths,
                Some(resolved),
                LivePackageTurnGreenLatestResult::RefusedByDriftedTurnGreenContract,
                format!(
                    "failed verifying nested latest-launch-packet handoff under {}: {error}",
                    paths.nested_launch_packet_latest_session_dir.display()
                ),
                latest_launch_packet_plan_step,
                latest_launch_packet_step,
                None,
            );
            persist_json(&paths.status_path, &status)?;
            let report = report_from_status(
                "run_latest_turn_green",
                verdict_for_result(status.result),
                session_dir,
                paths,
                &session,
                &status,
                Vec::new(),
            );
            persist_json(&paths.report_path, &report)?;
            return Ok(report);
        }
    };
    if latest_launch_packet_verify.verdict != LAUNCH_PACKET_LATEST_VERIFY_OK {
        let status = refusal_status(
            root,
            session_dir,
            paths,
            Some(resolved),
            LivePackageTurnGreenLatestResult::RefusedByDriftedTurnGreenContract,
            latest_launch_packet_verify.reason,
            latest_launch_packet_plan_step,
            latest_launch_packet_step,
            None,
        );
        persist_json(&paths.status_path, &status)?;
        let report = report_from_status(
            "run_latest_turn_green",
            verdict_for_result(status.result),
            session_dir,
            paths,
            &session,
            &status,
            Vec::new(),
        );
        persist_json(&paths.report_path, &report)?;
        return Ok(report);
    }

    let turn_green_run_raw = match run_json_command(
        TURN_GREEN_BIN,
        &turn_green_run_args(&resolved.snapshot, &paths.nested_turn_green_session_dir),
    ) {
        Ok(value) => value,
        Err(error) => {
            let status = refusal_status(
                root,
                session_dir,
                paths,
                Some(resolved),
                LivePackageTurnGreenLatestResult::RefusedByDriftedTurnGreenContract,
                format!(
                    "failed running downstream turn-green contract under {}: {error}",
                    paths.nested_turn_green_session_dir.display()
                ),
                latest_launch_packet_plan_step,
                latest_launch_packet_step,
                None,
            );
            persist_json(&paths.status_path, &status)?;
            let report = report_from_status(
                "run_latest_turn_green",
                verdict_for_result(status.result),
                session_dir,
                paths,
                &session,
                &status,
                Vec::new(),
            );
            persist_json(&paths.report_path, &report)?;
            return Ok(report);
        }
    };
    persist_json(&paths.turn_green_report_path, &turn_green_run_raw)?;
    let turn_green_run: TurnGreenReportView = serde_json::from_value(turn_green_run_raw)
        .context("failed parsing nested turn-green run json")?;
    let turn_green_step = Some(step_artifact(
        &paths.turn_green_report_path,
        "run_live_package_turn_green",
        &turn_green_run.verdict,
        &turn_green_run.reason,
        turn_green_run.generated_at,
    ));

    let mut turn_green_mismatches = Vec::new();
    compare_turn_green_run_against_snapshot(
        &turn_green_run,
        &resolved.snapshot,
        &paths.nested_turn_green_session_dir,
        &mut turn_green_mismatches,
    );
    if !turn_green_mismatches.is_empty() {
        let status = refusal_status(
            root,
            session_dir,
            paths,
            Some(resolved),
            LivePackageTurnGreenLatestResult::RefusedByDriftedTurnGreenContract,
            turn_green_mismatches[0].clone(),
            latest_launch_packet_plan_step,
            latest_launch_packet_step,
            turn_green_step,
        );
        persist_json(&paths.status_path, &status)?;
        let report = report_from_status(
            "run_latest_turn_green",
            verdict_for_result(status.result),
            session_dir,
            paths,
            &session,
            &status,
            Vec::new(),
        );
        persist_json(&paths.report_path, &report)?;
        return Ok(report);
    }

    let turn_green_verify: TurnGreenReportView = match run_json_command(
        TURN_GREEN_BIN,
        &turn_green_verify_args(&resolved.snapshot, &paths.nested_turn_green_session_dir),
    ) {
        Ok(value) => {
            serde_json::from_value(value).context("failed parsing nested turn-green verify json")?
        }
        Err(error) => {
            let status = refusal_status(
                root,
                session_dir,
                paths,
                Some(resolved),
                LivePackageTurnGreenLatestResult::RefusedByDriftedTurnGreenContract,
                format!(
                    "failed verifying downstream turn-green contract under {}: {error}",
                    paths.nested_turn_green_session_dir.display()
                ),
                latest_launch_packet_plan_step,
                latest_launch_packet_step,
                turn_green_step,
            );
            persist_json(&paths.status_path, &status)?;
            let report = report_from_status(
                "run_latest_turn_green",
                verdict_for_result(status.result),
                session_dir,
                paths,
                &session,
                &status,
                Vec::new(),
            );
            persist_json(&paths.report_path, &report)?;
            return Ok(report);
        }
    };
    if turn_green_verify.verdict != TURN_GREEN_VERIFY_OK {
        let status = refusal_status(
            root,
            session_dir,
            paths,
            Some(resolved),
            LivePackageTurnGreenLatestResult::RefusedByDriftedTurnGreenContract,
            turn_green_verify.reason,
            latest_launch_packet_plan_step,
            latest_launch_packet_step,
            turn_green_step,
        );
        persist_json(&paths.status_path, &status)?;
        let report = report_from_status(
            "run_latest_turn_green",
            verdict_for_result(status.result),
            session_dir,
            paths,
            &session,
            &status,
            Vec::new(),
        );
        persist_json(&paths.report_path, &report)?;
        return Ok(report);
    }

    let result = turn_green_latest_result_from_turn_green(
        turn_green_run.result.as_deref().unwrap_or_default(),
    )
    .unwrap_or(LivePackageTurnGreenLatestResult::RefusedByDriftedTurnGreenContract);
    let status = PackageTurnGreenLatestStatus {
        status_version: TURN_GREEN_LATEST_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        root: root.display().to_string(),
        session_dir: session_dir.display().to_string(),
        latest_top_step_name: Some(resolved.snapshot.latest_top_step_name.clone()),
        latest_top_session_dir: Some(
            resolved
                .snapshot
                .latest_top_session_dir
                .display()
                .to_string(),
        ),
        nested_launch_packet_latest_session_dir: paths
            .nested_launch_packet_latest_session_dir
            .display()
            .to_string(),
        nested_turn_green_session_dir: paths.nested_turn_green_session_dir.display().to_string(),
        downstream_launch_packet_session_dir: resolved
            .snapshot
            .downstream_launch_packet_session_dir
            .display()
            .to_string(),
        result,
        reason: turn_green_run.reason.clone(),
        latest_launch_packet_plan_verdict: Some(resolved.latest_launch_packet_plan.verdict.clone()),
        latest_launch_packet_verdict: Some(latest_launch_packet_run.verdict.clone()),
        turn_green_verdict: Some(turn_green_run.verdict.clone()),
        current_pre_activation_gate_verdict: turn_green_run
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: turn_green_run
            .current_pre_activation_gate_reason
            .clone(),
        latest_launch_packet_plan_step,
        latest_launch_packet_step,
        turn_green_step,
        operator_next_action_summary: turn_green_run
            .operator_next_action_summary
            .clone()
            .unwrap_or_else(|| operator_next_action_summary(result)),
        executable_now: turn_green_run.executable_now,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    };
    persist_json(&paths.status_path, &status)?;
    let report = report_from_status(
        "run_latest_turn_green",
        verdict_for_result(status.result),
        session_dir,
        paths,
        &session,
        &status,
        Vec::new(),
    );
    persist_json(&paths.report_path, &report)?;
    Ok(report)
}

fn run_latest_turn_green_failure(
    root: &Path,
    session_dir: &Path,
    paths: &PackageTurnGreenLatestPaths,
    failure: &LatestTurnGreenResolutionFailure,
) -> Result<PackageTurnGreenLatestReport> {
    let session = planned_session(root, session_dir, paths, None, Some(failure));
    persist_json(&paths.session_path, &session)?;
    if let Some(value) = &failure.latest_launch_packet_plan_raw {
        persist_json(&paths.latest_launch_packet_plan_report_path, value)?;
    }
    let status = refusal_status_from_resolution_failure(root, session_dir, paths, failure);
    persist_json(&paths.status_path, &status)?;
    let report = report_from_status(
        "run_latest_turn_green",
        verdict_from_failure_kind(failure.kind),
        session_dir,
        paths,
        &session,
        &status,
        Vec::new(),
    );
    persist_json(&paths.report_path, &report)?;
    Ok(report)
}

fn resolve_latest_turn_green(
    root: &Path,
    paths: &PackageTurnGreenLatestPaths,
) -> std::result::Result<ResolvedLatestTurnGreen, LatestTurnGreenResolutionFailure> {
    let latest_launch_packet_plan_raw = match run_json_command(
        LAUNCH_PACKET_LATEST_BIN,
        &latest_launch_packet_plan_args(root, &paths.nested_launch_packet_latest_session_dir),
    ) {
        Ok(value) => value,
        Err(error) => {
            return Err(LatestTurnGreenResolutionFailure {
                kind: ResolutionFailureKind::MissingLatestChain,
                reason: format!(
                    "failed resolving latest immutable chain under {}: {error}",
                    root.display()
                ),
                snapshot: None,
                latest_launch_packet_plan_raw: None,
                latest_launch_packet_plan_verdict: None,
                latest_launch_packet_plan_reason: None,
                latest_launch_packet_plan_generated_at: None,
            });
        }
    };
    let latest_launch_packet_plan: LaunchPacketLatestReportView =
        serde_json::from_value(latest_launch_packet_plan_raw.clone()).map_err(|error| {
            LatestTurnGreenResolutionFailure {
                kind: ResolutionFailureKind::InvalidLatestChain,
                reason: format!(
                    "failed parsing latest-launch-packet plan json for {}: {error}",
                    root.display()
                ),
                snapshot: None,
                latest_launch_packet_plan_raw: Some(latest_launch_packet_plan_raw.clone()),
                latest_launch_packet_plan_verdict: None,
                latest_launch_packet_plan_reason: None,
                latest_launch_packet_plan_generated_at: None,
            }
        })?;

    let snapshot = parse_snapshot_from_launch_packet_latest(&latest_launch_packet_plan).ok();
    if latest_launch_packet_plan.verdict != LAUNCH_PACKET_LATEST_PLAN_READY {
        return Err(LatestTurnGreenResolutionFailure {
            kind: map_launch_packet_latest_failure_kind(&latest_launch_packet_plan.verdict),
            reason: latest_launch_packet_plan.reason.clone(),
            snapshot,
            latest_launch_packet_plan_raw: Some(latest_launch_packet_plan_raw),
            latest_launch_packet_plan_verdict: Some(latest_launch_packet_plan.verdict),
            latest_launch_packet_plan_reason: Some(latest_launch_packet_plan.reason),
            latest_launch_packet_plan_generated_at: Some(latest_launch_packet_plan.generated_at),
        });
    }

    let snapshot =
        parse_snapshot_from_launch_packet_latest(&latest_launch_packet_plan).map_err(|error| {
            LatestTurnGreenResolutionFailure {
                kind: ResolutionFailureKind::InvalidLatestChain,
                reason: error.to_string(),
                snapshot: None,
                latest_launch_packet_plan_raw: Some(latest_launch_packet_plan_raw.clone()),
                latest_launch_packet_plan_verdict: Some(latest_launch_packet_plan.verdict.clone()),
                latest_launch_packet_plan_reason: Some(latest_launch_packet_plan.reason.clone()),
                latest_launch_packet_plan_generated_at: Some(
                    latest_launch_packet_plan.generated_at,
                ),
            }
        })?;

    let mut mismatches = Vec::new();
    compare_launch_packet_latest_plan_against_snapshot(
        &latest_launch_packet_plan,
        &snapshot,
        &paths.nested_launch_packet_latest_session_dir,
        &mut mismatches,
    );
    if !mismatches.is_empty() {
        return Err(LatestTurnGreenResolutionFailure {
            kind: ResolutionFailureKind::DriftedTurnGreenContract,
            reason: mismatches[0].clone(),
            snapshot: Some(snapshot),
            latest_launch_packet_plan_raw: Some(latest_launch_packet_plan_raw),
            latest_launch_packet_plan_verdict: Some(latest_launch_packet_plan.verdict),
            latest_launch_packet_plan_reason: Some(latest_launch_packet_plan.reason),
            latest_launch_packet_plan_generated_at: Some(latest_launch_packet_plan.generated_at),
        });
    }

    Ok(ResolvedLatestTurnGreen {
        snapshot,
        latest_launch_packet_plan_raw,
        latest_launch_packet_plan,
    })
}

fn parse_snapshot_from_launch_packet_latest(
    view: &LaunchPacketLatestReportView,
) -> Result<LatestTurnGreenSnapshot> {
    Ok(LatestTurnGreenSnapshot {
        latest_top_step_name: required_option_string(
            view.latest_top_step_name.as_ref(),
            "latest_top_step_name",
        )?,
        latest_top_session_dir: PathBuf::from(required_option_string(
            view.latest_top_session_dir.as_ref(),
            "latest_top_session_dir",
        )?),
        downstream_launch_packet_session_dir: PathBuf::from(required_option_string(
            view.nested_launch_packet_session_dir.as_ref(),
            "nested_launch_packet_session_dir",
        )?),
        package_dir: PathBuf::from(required_option_string(
            view.package_dir.as_ref(),
            "package_dir",
        )?),
        install_root: PathBuf::from(required_option_string(
            view.install_root.as_ref(),
            "install_root",
        )?),
        target_service_name: required_option_string(
            view.target_service_name.as_ref(),
            "target_service_name",
        )?,
        backend_command: required_option_string(view.backend_command.as_ref(), "backend_command")?,
        wrapper_timeout_ms: view
            .wrapper_timeout_ms
            .ok_or_else(|| anyhow!("latest-launch-packet plan is missing wrapper_timeout_ms"))?,
        service_status_max_staleness_ms: view.service_status_max_staleness_ms.ok_or_else(|| {
            anyhow!("latest-launch-packet plan is missing service_status_max_staleness_ms")
        })?,
        reviewed_frozen_live_cutover_controller_command_summary: required_option_string(
            view.reviewed_frozen_live_cutover_controller_command_summary
                .as_ref(),
            "reviewed_frozen_live_cutover_controller_command_summary",
        )?,
        clerestory_certificate_summary: required_option_string(
            view.clerestory_certificate_summary.as_ref(),
            "clerestory_certificate_summary",
        )?,
        clerestory_certificate_sha256: required_option_string(
            view.clerestory_certificate_sha256.as_ref(),
            "clerestory_certificate_sha256",
        )?,
        clerestory_certificate_algorithm: required_option_string(
            view.clerestory_certificate_algorithm.as_ref(),
            "clerestory_certificate_algorithm",
        )?,
    })
}

fn plan_report_from_resolution(
    mode: &str,
    verdict: TinyLivePackageTurnGreenLatestVerdict,
    reason: String,
    root: &Path,
    session_dir: &Path,
    paths: &PackageTurnGreenLatestPaths,
    resolved: &ResolvedLatestTurnGreen,
) -> PackageTurnGreenLatestReport {
    PackageTurnGreenLatestReport {
        generated_at: Utc::now(),
        mode: mode.to_string(),
        verdict,
        reason,
        root: Some(root.display().to_string()),
        latest_top_step_name: Some(resolved.snapshot.latest_top_step_name.clone()),
        latest_top_session_dir: Some(
            resolved
                .snapshot
                .latest_top_session_dir
                .display()
                .to_string(),
        ),
        package_dir: Some(resolved.snapshot.package_dir.display().to_string()),
        install_root: Some(resolved.snapshot.install_root.display().to_string()),
        target_service_name: Some(resolved.snapshot.target_service_name.clone()),
        backend_command: Some(resolved.snapshot.backend_command.clone()),
        wrapper_timeout_ms: Some(resolved.snapshot.wrapper_timeout_ms),
        service_status_max_staleness_ms: Some(resolved.snapshot.service_status_max_staleness_ms),
        session_dir: Some(session_dir.display().to_string()),
        turn_green_latest_session_path: Some(paths.session_path.display().to_string()),
        turn_green_latest_status_path: Some(paths.status_path.display().to_string()),
        turn_green_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_launch_packet_plan_report_path: Some(
            paths
                .latest_launch_packet_plan_report_path
                .display()
                .to_string(),
        ),
        latest_launch_packet_report_path: Some(
            paths.latest_launch_packet_report_path.display().to_string(),
        ),
        turn_green_report_path: Some(paths.turn_green_report_path.display().to_string()),
        nested_launch_packet_latest_session_dir: Some(
            paths
                .nested_launch_packet_latest_session_dir
                .display()
                .to_string(),
        ),
        nested_turn_green_session_dir: Some(
            paths.nested_turn_green_session_dir.display().to_string(),
        ),
        downstream_launch_packet_session_dir: Some(
            resolved
                .snapshot
                .downstream_launch_packet_session_dir
                .display()
                .to_string(),
        ),
        latest_launch_packet_plan_verdict: Some(resolved.latest_launch_packet_plan.verdict.clone()),
        latest_launch_packet_verdict: None,
        turn_green_verdict: None,
        result: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        resolve_latest_launch_packet_command_summary: Some(
            resolve_latest_launch_packet_command_summary(
                root,
                &paths.nested_launch_packet_latest_session_dir,
            ),
        ),
        run_latest_launch_packet_command_summary: Some(run_latest_launch_packet_command_summary(
            root,
            &paths.nested_launch_packet_latest_session_dir,
        )),
        verify_latest_launch_packet_command_summary: Some(
            verify_latest_launch_packet_command_summary(
                &paths.nested_launch_packet_latest_session_dir,
            ),
        ),
        downstream_turn_green_command_summary: Some(downstream_turn_green_command_summary(
            &resolved.snapshot,
            &paths.nested_turn_green_session_dir,
        )),
        verify_turn_green_command_summary: Some(verify_turn_green_command_summary(
            &resolved.snapshot,
            &paths.nested_turn_green_session_dir,
        )),
        reviewed_frozen_live_cutover_controller_command_summary: Some(
            resolved
                .snapshot
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone(),
        ),
        clerestory_certificate_summary: Some(
            resolved.snapshot.clerestory_certificate_summary.clone(),
        ),
        clerestory_certificate_sha256: Some(
            resolved.snapshot.clerestory_certificate_sha256.clone(),
        ),
        clerestory_certificate_algorithm: Some(
            resolved.snapshot.clerestory_certificate_algorithm.clone(),
        ),
        verification_mismatches: Vec::new(),
        executable_now: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn report_from_resolution_failure(
    mode: &str,
    root: &Path,
    session_dir: &Path,
    paths: &PackageTurnGreenLatestPaths,
    failure: &LatestTurnGreenResolutionFailure,
) -> PackageTurnGreenLatestReport {
    let snapshot = failure.snapshot.as_ref();
    PackageTurnGreenLatestReport {
        generated_at: Utc::now(),
        mode: mode.to_string(),
        verdict: verdict_from_failure_kind(failure.kind),
        reason: failure.reason.clone(),
        root: Some(root.display().to_string()),
        latest_top_step_name: snapshot.map(|value| value.latest_top_step_name.clone()),
        latest_top_session_dir: snapshot
            .map(|value| value.latest_top_session_dir.display().to_string()),
        package_dir: snapshot.map(|value| value.package_dir.display().to_string()),
        install_root: snapshot.map(|value| value.install_root.display().to_string()),
        target_service_name: snapshot.map(|value| value.target_service_name.clone()),
        backend_command: snapshot.map(|value| value.backend_command.clone()),
        wrapper_timeout_ms: snapshot.map(|value| value.wrapper_timeout_ms),
        service_status_max_staleness_ms: snapshot
            .map(|value| value.service_status_max_staleness_ms),
        session_dir: Some(session_dir.display().to_string()),
        turn_green_latest_session_path: Some(paths.session_path.display().to_string()),
        turn_green_latest_status_path: Some(paths.status_path.display().to_string()),
        turn_green_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_launch_packet_plan_report_path: Some(
            paths
                .latest_launch_packet_plan_report_path
                .display()
                .to_string(),
        ),
        latest_launch_packet_report_path: Some(
            paths.latest_launch_packet_report_path.display().to_string(),
        ),
        turn_green_report_path: Some(paths.turn_green_report_path.display().to_string()),
        nested_launch_packet_latest_session_dir: Some(
            paths
                .nested_launch_packet_latest_session_dir
                .display()
                .to_string(),
        ),
        nested_turn_green_session_dir: Some(
            paths.nested_turn_green_session_dir.display().to_string(),
        ),
        downstream_launch_packet_session_dir: snapshot.map(|value| {
            value
                .downstream_launch_packet_session_dir
                .display()
                .to_string()
        }),
        latest_launch_packet_plan_verdict: failure.latest_launch_packet_plan_verdict.clone(),
        latest_launch_packet_verdict: None,
        turn_green_verdict: None,
        result: Some(serialize_enum(&result_from_failure_kind(failure.kind))),
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        resolve_latest_launch_packet_command_summary: Some(
            resolve_latest_launch_packet_command_summary(
                root,
                &paths.nested_launch_packet_latest_session_dir,
            ),
        ),
        run_latest_launch_packet_command_summary: Some(run_latest_launch_packet_command_summary(
            root,
            &paths.nested_launch_packet_latest_session_dir,
        )),
        verify_latest_launch_packet_command_summary: Some(
            verify_latest_launch_packet_command_summary(
                &paths.nested_launch_packet_latest_session_dir,
            ),
        ),
        downstream_turn_green_command_summary: snapshot.map(|value| {
            downstream_turn_green_command_summary(value, &paths.nested_turn_green_session_dir)
        }),
        verify_turn_green_command_summary: snapshot.map(|value| {
            verify_turn_green_command_summary(value, &paths.nested_turn_green_session_dir)
        }),
        reviewed_frozen_live_cutover_controller_command_summary: snapshot.map(|value| {
            value
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone()
        }),
        clerestory_certificate_summary: snapshot
            .map(|value| value.clerestory_certificate_summary.clone()),
        clerestory_certificate_sha256: snapshot
            .map(|value| value.clerestory_certificate_sha256.clone()),
        clerestory_certificate_algorithm: snapshot
            .map(|value| value.clerestory_certificate_algorithm.clone()),
        verification_mismatches: Vec::new(),
        executable_now: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn planned_session(
    root: &Path,
    session_dir: &Path,
    paths: &PackageTurnGreenLatestPaths,
    resolved: Option<&ResolvedLatestTurnGreen>,
    failure: Option<&LatestTurnGreenResolutionFailure>,
) -> PackageTurnGreenLatestSession {
    let snapshot = resolved
        .map(|value| &value.snapshot)
        .or_else(|| failure.and_then(|value| value.snapshot.as_ref()));
    PackageTurnGreenLatestSession {
        session_version: TURN_GREEN_LATEST_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        root: root.display().to_string(),
        session_dir: session_dir.display().to_string(),
        nested_launch_packet_latest_session_dir: paths
            .nested_launch_packet_latest_session_dir
            .display()
            .to_string(),
        nested_turn_green_session_dir: paths.nested_turn_green_session_dir.display().to_string(),
        downstream_launch_packet_session_dir: snapshot
            .map(|value| {
                value
                    .downstream_launch_packet_session_dir
                    .display()
                    .to_string()
            })
            .unwrap_or_else(|| {
                expected_downstream_launch_packet_session_dir(paths)
                    .display()
                    .to_string()
            }),
        latest_top_step_name: snapshot.map(|value| value.latest_top_step_name.clone()),
        latest_top_session_dir: snapshot
            .map(|value| value.latest_top_session_dir.display().to_string()),
        package_dir: snapshot.map(|value| value.package_dir.display().to_string()),
        install_root: snapshot.map(|value| value.install_root.display().to_string()),
        target_service_name: snapshot.map(|value| value.target_service_name.clone()),
        backend_command: snapshot.map(|value| value.backend_command.clone()),
        wrapper_timeout_ms: snapshot.map(|value| value.wrapper_timeout_ms),
        service_status_max_staleness_ms: snapshot
            .map(|value| value.service_status_max_staleness_ms),
        resolve_latest_launch_packet_command_summary: resolve_latest_launch_packet_command_summary(
            root,
            &paths.nested_launch_packet_latest_session_dir,
        ),
        run_latest_launch_packet_command_summary: run_latest_launch_packet_command_summary(
            root,
            &paths.nested_launch_packet_latest_session_dir,
        ),
        verify_latest_launch_packet_command_summary: verify_latest_launch_packet_command_summary(
            &paths.nested_launch_packet_latest_session_dir,
        ),
        downstream_turn_green_command_summary: snapshot
            .map(|value| {
                downstream_turn_green_command_summary(value, &paths.nested_turn_green_session_dir)
            })
            .unwrap_or_else(|| "<turn-green-run-unavailable>".to_string()),
        verify_turn_green_command_summary: snapshot
            .map(|value| {
                verify_turn_green_command_summary(value, &paths.nested_turn_green_session_dir)
            })
            .unwrap_or_else(|| "<turn-green-verify-unavailable>".to_string()),
        reviewed_frozen_live_cutover_controller_command_summary: snapshot.map(|value| {
            value
                .reviewed_frozen_live_cutover_controller_command_summary
                .clone()
        }),
        clerestory_certificate_summary: snapshot
            .map(|value| value.clerestory_certificate_summary.clone()),
        clerestory_certificate_sha256: snapshot
            .map(|value| value.clerestory_certificate_sha256.clone()),
        clerestory_certificate_algorithm: snapshot
            .map(|value| value.clerestory_certificate_algorithm.clone()),
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn report_from_status(
    mode: &str,
    verdict: TinyLivePackageTurnGreenLatestVerdict,
    session_dir: &Path,
    paths: &PackageTurnGreenLatestPaths,
    session: &PackageTurnGreenLatestSession,
    status: &PackageTurnGreenLatestStatus,
    verification_mismatches: Vec<String>,
) -> PackageTurnGreenLatestReport {
    PackageTurnGreenLatestReport {
        generated_at: Utc::now(),
        mode: mode.to_string(),
        verdict,
        reason: status.reason.clone(),
        root: Some(session.root.clone()),
        latest_top_step_name: status.latest_top_step_name.clone(),
        latest_top_session_dir: status.latest_top_session_dir.clone(),
        package_dir: session.package_dir.clone(),
        install_root: session.install_root.clone(),
        target_service_name: session.target_service_name.clone(),
        backend_command: session.backend_command.clone(),
        wrapper_timeout_ms: session.wrapper_timeout_ms,
        service_status_max_staleness_ms: session.service_status_max_staleness_ms,
        session_dir: Some(session_dir.display().to_string()),
        turn_green_latest_session_path: Some(paths.session_path.display().to_string()),
        turn_green_latest_status_path: Some(paths.status_path.display().to_string()),
        turn_green_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_launch_packet_plan_report_path: Some(
            paths
                .latest_launch_packet_plan_report_path
                .display()
                .to_string(),
        ),
        latest_launch_packet_report_path: Some(
            paths.latest_launch_packet_report_path.display().to_string(),
        ),
        turn_green_report_path: Some(paths.turn_green_report_path.display().to_string()),
        nested_launch_packet_latest_session_dir: Some(
            session.nested_launch_packet_latest_session_dir.clone(),
        ),
        nested_turn_green_session_dir: Some(session.nested_turn_green_session_dir.clone()),
        downstream_launch_packet_session_dir: Some(
            session.downstream_launch_packet_session_dir.clone(),
        ),
        latest_launch_packet_plan_verdict: status.latest_launch_packet_plan_verdict.clone(),
        latest_launch_packet_verdict: status.latest_launch_packet_verdict.clone(),
        turn_green_verdict: status.turn_green_verdict.clone(),
        result: Some(serialize_enum(&status.result)),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        resolve_latest_launch_packet_command_summary: Some(
            session.resolve_latest_launch_packet_command_summary.clone(),
        ),
        run_latest_launch_packet_command_summary: Some(
            session.run_latest_launch_packet_command_summary.clone(),
        ),
        verify_latest_launch_packet_command_summary: Some(
            session.verify_latest_launch_packet_command_summary.clone(),
        ),
        downstream_turn_green_command_summary: Some(
            session.downstream_turn_green_command_summary.clone(),
        ),
        verify_turn_green_command_summary: Some(session.verify_turn_green_command_summary.clone()),
        reviewed_frozen_live_cutover_controller_command_summary: session
            .reviewed_frozen_live_cutover_controller_command_summary
            .clone(),
        clerestory_certificate_summary: session.clerestory_certificate_summary.clone(),
        clerestory_certificate_sha256: session.clerestory_certificate_sha256.clone(),
        clerestory_certificate_algorithm: session.clerestory_certificate_algorithm.clone(),
        verification_mismatches,
        executable_now: status.executable_now,
        explicit_statement: session.explicit_statement.clone(),
    }
}

fn turn_green_latest_paths(session_dir: &Path) -> PackageTurnGreenLatestPaths {
    PackageTurnGreenLatestPaths {
        session_path: session_dir
            .join("tiny_live_activation_package_turn_green_latest.session.json"),
        status_path: session_dir.join("tiny_live_activation_package_turn_green_latest.status.json"),
        report_path: session_dir.join("tiny_live_activation_package_turn_green_latest.report.json"),
        latest_launch_packet_plan_report_path: session_dir.join(
            "tiny_live_activation_package_turn_green_latest.launch_packet_latest_plan.report.json",
        ),
        latest_launch_packet_report_path: session_dir.join(
            "tiny_live_activation_package_turn_green_latest.launch_packet_latest.report.json",
        ),
        turn_green_report_path: session_dir
            .join("tiny_live_activation_package_turn_green_latest.turn_green.report.json"),
        nested_launch_packet_latest_session_dir: session_dir
            .join("tiny_live_activation_package_turn_green_latest.launch_packet_latest_session"),
        nested_turn_green_session_dir: session_dir
            .join("tiny_live_activation_package_turn_green_latest.turn_green_session"),
    }
}

fn expected_downstream_launch_packet_session_dir(paths: &PackageTurnGreenLatestPaths) -> PathBuf {
    paths
        .nested_launch_packet_latest_session_dir
        .join("tiny_live_activation_package_launch_packet_latest.launch_packet_session")
}

fn latest_launch_packet_plan_args(root: &Path, session_dir: &Path) -> Vec<String> {
    vec![
        "--root".to_string(),
        root.display().to_string(),
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--plan-latest-launch-packet".to_string(),
        "--json".to_string(),
    ]
}

fn latest_launch_packet_run_args(root: &Path, session_dir: &Path) -> Vec<String> {
    vec![
        "--root".to_string(),
        root.display().to_string(),
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--run-latest-launch-packet".to_string(),
        "--json".to_string(),
    ]
}

fn latest_launch_packet_verify_args(session_dir: &Path) -> Vec<String> {
    vec![
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--verify-latest-launch-packet".to_string(),
        "--json".to_string(),
    ]
}

fn turn_green_run_args(snapshot: &LatestTurnGreenSnapshot, session_dir: &Path) -> Vec<String> {
    vec![
        "--launch-packet-session-dir".to_string(),
        snapshot
            .downstream_launch_packet_session_dir
            .display()
            .to_string(),
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--run-live-package-turn-green".to_string(),
        "--json".to_string(),
    ]
}

fn turn_green_verify_args(snapshot: &LatestTurnGreenSnapshot, session_dir: &Path) -> Vec<String> {
    vec![
        "--launch-packet-session-dir".to_string(),
        snapshot
            .downstream_launch_packet_session_dir
            .display()
            .to_string(),
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--verify-live-package-turn-green".to_string(),
        "--json".to_string(),
    ]
}

fn resolve_latest_launch_packet_command_summary(root: &Path, session_dir: &Path) -> String {
    format!(
        "{LAUNCH_PACKET_LATEST_BIN} --root {} --session-dir {} --plan-latest-launch-packet",
        shell_escape_path(root),
        shell_escape_path(session_dir),
    )
}

fn run_latest_launch_packet_command_summary(root: &Path, session_dir: &Path) -> String {
    format!(
        "{LAUNCH_PACKET_LATEST_BIN} --root {} --session-dir {} --run-latest-launch-packet",
        shell_escape_path(root),
        shell_escape_path(session_dir),
    )
}

fn verify_latest_launch_packet_command_summary(session_dir: &Path) -> String {
    format!(
        "{LAUNCH_PACKET_LATEST_BIN} --session-dir {} --verify-latest-launch-packet",
        shell_escape_path(session_dir),
    )
}

fn downstream_turn_green_command_summary(
    snapshot: &LatestTurnGreenSnapshot,
    session_dir: &Path,
) -> String {
    format!(
        "{TURN_GREEN_BIN} --launch-packet-session-dir {} --session-dir {} --run-live-package-turn-green",
        shell_escape_path(&snapshot.downstream_launch_packet_session_dir),
        shell_escape_path(session_dir),
    )
}

fn verify_turn_green_command_summary(
    snapshot: &LatestTurnGreenSnapshot,
    session_dir: &Path,
) -> String {
    format!(
        "{TURN_GREEN_BIN} --launch-packet-session-dir {} --session-dir {} --verify-live-package-turn-green",
        shell_escape_path(&snapshot.downstream_launch_packet_session_dir),
        shell_escape_path(session_dir),
    )
}

fn nested_launch_packet_latest_report_path(session_dir: &Path) -> PathBuf {
    session_dir.join("tiny_live_activation_package_launch_packet_latest.report.json")
}

fn nested_turn_green_session_path(session_dir: &Path) -> PathBuf {
    session_dir.join("tiny_live_activation_package_turn_green.session.json")
}

fn nested_turn_green_status_path(session_dir: &Path) -> PathBuf {
    session_dir.join("tiny_live_activation_package_turn_green.status.json")
}

fn nested_turn_green_current_authorization_session_dir(session_dir: &Path) -> PathBuf {
    session_dir.join("tiny_live_activation_package_turn_green.current_authorization_session")
}

fn set_mode(mode: &mut Option<Mode>, value: Mode, flag: &str) -> Result<()> {
    if mode.replace(value).is_some() {
        bail!("multiple modes specified; latest was {flag}");
    }
    Ok(())
}

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    value.ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn run_json_command(binary: &str, args: &[String]) -> Result<Value> {
    let output = Command::new(binary)
        .args(args)
        .output()
        .with_context(|| format!("failed spawning {binary}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("{binary} exited with {}: {}", output.status, stderr.trim());
    }
    serde_json::from_slice(&output.stdout)
        .with_context(|| format!("failed parsing JSON output from {binary}"))
}

fn shell_escape_path(path: &Path) -> String {
    shell_escape_string(&path.display().to_string())
}

fn shell_escape_string(value: &str) -> String {
    if value
        .chars()
        .all(|raw| raw.is_ascii_alphanumeric() || "/._-".contains(raw))
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\"'\"'"))
    }
}

fn create_clean_session_dir(session_dir: &Path, label: &str) -> Result<()> {
    fs::create_dir_all(session_dir).with_context(|| {
        format!(
            "failed creating {label} session dir {}",
            session_dir.display()
        )
    })
}

fn write_script(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, contents).with_context(|| {
        format!(
            "failed writing latest-turn-green script to {}",
            path.display()
        )
    })?;
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
) -> PackageTurnGreenLatestStepArtifact {
    PackageTurnGreenLatestStepArtifact {
        path: path.display().to_string(),
        mode: mode.to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        generated_at,
    }
}

fn load_required_step_json<T: for<'de> Deserialize<'de>>(
    step: &Option<PackageTurnGreenLatestStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<T> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("{label} is missing from latest-turn-green status"))?;
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

fn compare_option_string(
    actual: Option<&str>,
    expected: Option<&str>,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    if actual != expected {
        mismatches.push(format!(
            "{label} {:?} does not match expected {:?}",
            actual, expected
        ));
    }
}

fn compare_option_u64(
    actual: Option<u64>,
    expected: Option<u64>,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    if actual != expected {
        mismatches.push(format!(
            "{label} {:?} does not match expected {:?}",
            actual, expected
        ));
    }
}

fn compare_step_artifact_against_report_metadata(
    step: Option<&PackageTurnGreenLatestStepArtifact>,
    expected_path: &Path,
    expected_mode: &str,
    expected_verdict: &str,
    expected_reason: &str,
    expected_generated_at: DateTime<Utc>,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    let Some(step) = step else {
        mismatches.push(format!("{label} is missing from latest-turn-green status"));
        return;
    };
    compare_string(
        &step.path,
        &expected_path.display().to_string(),
        &format!("{label} path"),
        mismatches,
    );
    compare_string(
        &step.mode,
        expected_mode,
        &format!("{label} mode"),
        mismatches,
    );
    compare_string(
        &step.verdict,
        expected_verdict,
        &format!("{label} verdict"),
        mismatches,
    );
    compare_string(
        &step.reason,
        expected_reason,
        &format!("{label} reason"),
        mismatches,
    );
    if step.generated_at != expected_generated_at {
        mismatches.push(format!(
            "{label} generated_at {:?} does not match expected {:?}",
            step.generated_at, expected_generated_at
        ));
    }
}

fn compare_launch_packet_latest_plan_against_snapshot(
    stored: &LaunchPacketLatestReportView,
    snapshot: &LatestTurnGreenSnapshot,
    session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        Some(stored.verdict.as_str()),
        Some(LAUNCH_PACKET_LATEST_PLAN_READY),
        "stored latest-launch-packet plan verdict",
        mismatches,
    );
    compare_string(
        &stored.mode,
        "plan_latest_launch_packet",
        "stored latest-launch-packet plan mode",
        mismatches,
    );
    compare_option_string(
        stored.latest_top_step_name.as_deref(),
        Some(snapshot.latest_top_step_name.as_str()),
        "stored latest-launch-packet plan latest_top_step_name",
        mismatches,
    );
    compare_option_string(
        stored.latest_top_session_dir.as_deref(),
        Some(
            snapshot
                .latest_top_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored latest-launch-packet plan latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored latest-launch-packet plan session_dir",
        mismatches,
    );
    compare_option_string(
        stored.nested_launch_packet_session_dir.as_deref(),
        Some(
            snapshot
                .downstream_launch_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored latest-launch-packet plan nested_launch_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.package_dir.as_deref(),
        Some(snapshot.package_dir.display().to_string().as_str()),
        "stored latest-launch-packet plan package_dir",
        mismatches,
    );
    compare_option_string(
        stored.install_root.as_deref(),
        Some(snapshot.install_root.display().to_string().as_str()),
        "stored latest-launch-packet plan install_root",
        mismatches,
    );
    compare_option_string(
        stored.target_service_name.as_deref(),
        Some(snapshot.target_service_name.as_str()),
        "stored latest-launch-packet plan target_service_name",
        mismatches,
    );
    compare_option_string(
        stored.backend_command.as_deref(),
        Some(snapshot.backend_command.as_str()),
        "stored latest-launch-packet plan backend_command",
        mismatches,
    );
    compare_option_u64(
        stored.wrapper_timeout_ms,
        Some(snapshot.wrapper_timeout_ms),
        "stored latest-launch-packet plan wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        stored.service_status_max_staleness_ms,
        Some(snapshot.service_status_max_staleness_ms),
        "stored latest-launch-packet plan service_status_max_staleness_ms",
        mismatches,
    );
    compare_option_string(
        stored
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        Some(
            snapshot
                .reviewed_frozen_live_cutover_controller_command_summary
                .as_str(),
        ),
        "stored latest-launch-packet plan reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_summary.as_deref(),
        Some(snapshot.clerestory_certificate_summary.as_str()),
        "stored latest-launch-packet plan clerestory_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_sha256.as_deref(),
        Some(snapshot.clerestory_certificate_sha256.as_str()),
        "stored latest-launch-packet plan clerestory_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_algorithm.as_deref(),
        Some(snapshot.clerestory_certificate_algorithm.as_str()),
        "stored latest-launch-packet plan clerestory_certificate_algorithm",
        mismatches,
    );
}

fn compare_launch_packet_latest_run_against_snapshot(
    stored: &LaunchPacketLatestReportView,
    snapshot: &LatestTurnGreenSnapshot,
    session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &stored.mode,
        "run_latest_launch_packet",
        "stored latest-launch-packet run mode",
        mismatches,
    );
    compare_option_string(
        stored.latest_top_step_name.as_deref(),
        Some(snapshot.latest_top_step_name.as_str()),
        "stored latest-launch-packet run latest_top_step_name",
        mismatches,
    );
    compare_option_string(
        stored.latest_top_session_dir.as_deref(),
        Some(
            snapshot
                .latest_top_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored latest-launch-packet run latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored latest-launch-packet run session_dir",
        mismatches,
    );
    compare_option_string(
        stored.nested_launch_packet_session_dir.as_deref(),
        Some(
            snapshot
                .downstream_launch_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored latest-launch-packet run nested_launch_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.package_dir.as_deref(),
        Some(snapshot.package_dir.display().to_string().as_str()),
        "stored latest-launch-packet run package_dir",
        mismatches,
    );
    compare_option_string(
        stored.install_root.as_deref(),
        Some(snapshot.install_root.display().to_string().as_str()),
        "stored latest-launch-packet run install_root",
        mismatches,
    );
    compare_option_string(
        stored.target_service_name.as_deref(),
        Some(snapshot.target_service_name.as_str()),
        "stored latest-launch-packet run target_service_name",
        mismatches,
    );
    compare_option_string(
        stored.backend_command.as_deref(),
        Some(snapshot.backend_command.as_str()),
        "stored latest-launch-packet run backend_command",
        mismatches,
    );
    compare_option_u64(
        stored.wrapper_timeout_ms,
        Some(snapshot.wrapper_timeout_ms),
        "stored latest-launch-packet run wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        stored.service_status_max_staleness_ms,
        Some(snapshot.service_status_max_staleness_ms),
        "stored latest-launch-packet run service_status_max_staleness_ms",
        mismatches,
    );
    compare_option_string(
        stored
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        Some(
            snapshot
                .reviewed_frozen_live_cutover_controller_command_summary
                .as_str(),
        ),
        "stored latest-launch-packet run reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_summary.as_deref(),
        Some(snapshot.clerestory_certificate_summary.as_str()),
        "stored latest-launch-packet run clerestory_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_sha256.as_deref(),
        Some(snapshot.clerestory_certificate_sha256.as_str()),
        "stored latest-launch-packet run clerestory_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_algorithm.as_deref(),
        Some(snapshot.clerestory_certificate_algorithm.as_str()),
        "stored latest-launch-packet run clerestory_certificate_algorithm",
        mismatches,
    );
}

fn compare_launch_packet_latest_copy_matches_actual(
    stored: &LaunchPacketLatestReportView,
    actual: &LaunchPacketLatestReportView,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        Some(stored.verdict.as_str()),
        Some(actual.verdict.as_str()),
        "stored latest-launch-packet copy verdict",
        mismatches,
    );
    compare_option_string(
        Some(stored.reason.as_str()),
        Some(actual.reason.as_str()),
        "stored latest-launch-packet copy reason",
        mismatches,
    );
    if stored.generated_at != actual.generated_at {
        mismatches.push(format!(
            "stored latest-launch-packet copy generated_at {:?} does not match actual {:?}",
            stored.generated_at, actual.generated_at
        ));
    }
    compare_option_string(
        stored.latest_top_step_name.as_deref(),
        actual.latest_top_step_name.as_deref(),
        "stored latest-launch-packet copy latest_top_step_name",
        mismatches,
    );
    compare_option_string(
        stored.latest_top_session_dir.as_deref(),
        actual.latest_top_session_dir.as_deref(),
        "stored latest-launch-packet copy latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        actual.session_dir.as_deref(),
        "stored latest-launch-packet copy session_dir",
        mismatches,
    );
    compare_option_string(
        stored.nested_launch_packet_session_dir.as_deref(),
        actual.nested_launch_packet_session_dir.as_deref(),
        "stored latest-launch-packet copy nested_launch_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.result.as_deref(),
        actual.result.as_deref(),
        "stored latest-launch-packet copy result",
        mismatches,
    );
    compare_option_string(
        stored.explicit_statement.as_deref(),
        actual.explicit_statement.as_deref(),
        "stored latest-launch-packet copy explicit_statement",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_verdict.as_deref(),
        actual.current_pre_activation_gate_verdict.as_deref(),
        "stored latest-launch-packet copy current_pre_activation_gate_verdict",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_reason.as_deref(),
        actual.current_pre_activation_gate_reason.as_deref(),
        "stored latest-launch-packet copy current_pre_activation_gate_reason",
        mismatches,
    );
    compare_option_string(
        stored
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        actual
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        "stored latest-launch-packet copy reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
}

fn compare_turn_green_run_against_snapshot(
    stored: &TurnGreenReportView,
    snapshot: &LatestTurnGreenSnapshot,
    session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &stored.mode,
        "run_live_package_turn_green",
        "stored turn-green run mode",
        mismatches,
    );
    compare_string(
        &stored.launch_packet_session_dir,
        &snapshot
            .downstream_launch_packet_session_dir
            .display()
            .to_string(),
        "stored turn-green run launch_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.package_dir.as_deref(),
        Some(snapshot.package_dir.display().to_string().as_str()),
        "stored turn-green run package_dir",
        mismatches,
    );
    compare_option_string(
        stored.install_root.as_deref(),
        Some(snapshot.install_root.display().to_string().as_str()),
        "stored turn-green run install_root",
        mismatches,
    );
    compare_option_string(
        stored.target_service_name.as_deref(),
        Some(snapshot.target_service_name.as_str()),
        "stored turn-green run target_service_name",
        mismatches,
    );
    compare_option_string(
        stored.backend_command.as_deref(),
        Some(snapshot.backend_command.as_str()),
        "stored turn-green run backend_command",
        mismatches,
    );
    compare_option_u64(
        stored.wrapper_timeout_ms,
        Some(snapshot.wrapper_timeout_ms),
        "stored turn-green run wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        stored.service_status_max_staleness_ms,
        Some(snapshot.service_status_max_staleness_ms),
        "stored turn-green run service_status_max_staleness_ms",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored turn-green run session_dir",
        mismatches,
    );
    compare_option_string(
        stored.turn_green_session_path.as_deref(),
        Some(
            nested_turn_green_session_path(session_dir)
                .display()
                .to_string()
                .as_str(),
        ),
        "stored turn-green run turn_green_session_path",
        mismatches,
    );
    compare_option_string(
        stored.turn_green_status_path.as_deref(),
        Some(
            nested_turn_green_status_path(session_dir)
                .display()
                .to_string()
                .as_str(),
        ),
        "stored turn-green run turn_green_status_path",
        mismatches,
    );
    compare_option_string(
        stored.nested_current_authorization_session_dir.as_deref(),
        Some(
            nested_turn_green_current_authorization_session_dir(session_dir)
                .display()
                .to_string()
                .as_str(),
        ),
        "stored turn-green run nested_current_authorization_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.verify_frozen_launch_packet_summary.as_deref(),
        Some(
            turn_green_verify_frozen_launch_packet_summary(
                &snapshot.downstream_launch_packet_session_dir,
            )
            .as_str(),
        ),
        "stored turn-green run verify_frozen_launch_packet_summary",
        mismatches,
    );
    compare_option_string(
        stored
            .refresh_current_authorization_command_summary
            .as_deref(),
        Some(
            turn_green_refresh_current_authorization_command_summary(
                snapshot,
                &nested_turn_green_current_authorization_session_dir(session_dir),
            )
            .as_str(),
        ),
        "stored turn-green run refresh_current_authorization_command_summary",
        mismatches,
    );
    compare_option_string(
        stored
            .frozen_live_cutover_controller_command_summary
            .as_deref(),
        Some(
            snapshot
                .reviewed_frozen_live_cutover_controller_command_summary
                .as_str(),
        ),
        "stored turn-green run frozen_live_cutover_controller_command_summary",
        mismatches,
    );
}

fn compare_turn_green_copy_against_nested_truth(
    stored: &TurnGreenReportView,
    nested_session: &TurnGreenSessionView,
    nested_status: &TurnGreenStatusView,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &stored.launch_packet_session_dir,
        &nested_session.launch_packet_session_dir,
        "stored turn-green copy launch_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(nested_session.session_dir.as_str()),
        "stored turn-green copy session_dir",
        mismatches,
    );
    compare_option_string(
        stored.result.as_deref(),
        Some(nested_status.result.as_str()),
        "stored turn-green copy result",
        mismatches,
    );
    compare_option_string(
        Some(stored.verdict.as_str()),
        turn_green_run_verdict_for_result(&nested_status.result),
        "stored turn-green copy verdict",
        mismatches,
    );
    compare_option_string(
        stored.frozen_launch_packet_result.as_deref(),
        nested_status.frozen_launch_packet_result.as_deref(),
        "stored turn-green copy frozen_launch_packet_result",
        mismatches,
    );
    compare_option_string(
        stored.current_authorization_result_now.as_deref(),
        nested_status.current_authorization_result_now.as_deref(),
        "stored turn-green copy current_authorization_result_now",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_verdict.as_deref(),
        nested_status.current_pre_activation_gate_verdict.as_deref(),
        "stored turn-green copy current_pre_activation_gate_verdict",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_reason.as_deref(),
        nested_status.current_pre_activation_gate_reason.as_deref(),
        "stored turn-green copy current_pre_activation_gate_reason",
        mismatches,
    );
    compare_option_string(
        stored.operator_next_action_summary.as_deref(),
        Some(nested_status.operator_next_action_summary.as_str()),
        "stored turn-green copy operator_next_action_summary",
        mismatches,
    );
    compare_option_string(
        stored.verify_frozen_launch_packet_summary.as_deref(),
        Some(nested_session.verify_frozen_launch_packet_summary.as_str()),
        "stored turn-green copy verify_frozen_launch_packet_summary",
        mismatches,
    );
    compare_option_string(
        stored
            .refresh_current_authorization_command_summary
            .as_deref(),
        Some(
            nested_session
                .refresh_current_authorization_command_summary
                .as_str(),
        ),
        "stored turn-green copy refresh_current_authorization_command_summary",
        mismatches,
    );
    compare_option_string(
        stored
            .frozen_live_cutover_controller_command_summary
            .as_deref(),
        Some(
            nested_session
                .frozen_live_cutover_controller_command_summary
                .as_str(),
        ),
        "stored turn-green copy frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_string(
        &stored.reason,
        &nested_status.reason,
        "stored turn-green copy reason",
        mismatches,
    );
    if stored.executable_now != nested_status.executable_now {
        mismatches.push(format!(
            "stored turn-green copy executable_now {} does not match nested status {}",
            stored.executable_now, nested_status.executable_now
        ));
    }
    if stored.frozen_controller_still_current != nested_status.frozen_controller_still_current {
        mismatches.push(format!(
            "stored turn-green copy frozen_controller_still_current {} does not match nested status {}",
            stored.frozen_controller_still_current, nested_status.frozen_controller_still_current
        ));
    }
    compare_string(
        &stored.explicit_statement,
        &nested_status.explicit_statement,
        "stored turn-green copy explicit_statement",
        mismatches,
    );
}

fn compare_session_against_snapshot(
    session: &PackageTurnGreenLatestSession,
    root: &Path,
    session_dir: &Path,
    paths: &PackageTurnGreenLatestPaths,
    snapshot: &LatestTurnGreenSnapshot,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &session.resolve_latest_launch_packet_command_summary,
        &resolve_latest_launch_packet_command_summary(
            root,
            &paths.nested_launch_packet_latest_session_dir,
        ),
        "stored session resolve_latest_launch_packet_command_summary",
        mismatches,
    );
    compare_string(
        &session.run_latest_launch_packet_command_summary,
        &run_latest_launch_packet_command_summary(
            root,
            &paths.nested_launch_packet_latest_session_dir,
        ),
        "stored session run_latest_launch_packet_command_summary",
        mismatches,
    );
    compare_string(
        &session.verify_latest_launch_packet_command_summary,
        &verify_latest_launch_packet_command_summary(
            &paths.nested_launch_packet_latest_session_dir,
        ),
        "stored session verify_latest_launch_packet_command_summary",
        mismatches,
    );
    compare_string(
        &session.downstream_turn_green_command_summary,
        &downstream_turn_green_command_summary(snapshot, &paths.nested_turn_green_session_dir),
        "stored session downstream_turn_green_command_summary",
        mismatches,
    );
    compare_string(
        &session.verify_turn_green_command_summary,
        &verify_turn_green_command_summary(snapshot, &paths.nested_turn_green_session_dir),
        "stored session verify_turn_green_command_summary",
        mismatches,
    );
    compare_option_string(
        session.latest_top_step_name.as_deref(),
        Some(snapshot.latest_top_step_name.as_str()),
        "stored session latest_top_step_name",
        mismatches,
    );
    compare_option_string(
        session.latest_top_session_dir.as_deref(),
        Some(
            snapshot
                .latest_top_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored session latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        session.package_dir.as_deref(),
        Some(snapshot.package_dir.display().to_string().as_str()),
        "stored session package_dir",
        mismatches,
    );
    compare_option_string(
        session.install_root.as_deref(),
        Some(snapshot.install_root.display().to_string().as_str()),
        "stored session install_root",
        mismatches,
    );
    compare_option_string(
        session.target_service_name.as_deref(),
        Some(snapshot.target_service_name.as_str()),
        "stored session target_service_name",
        mismatches,
    );
    compare_option_string(
        session.backend_command.as_deref(),
        Some(snapshot.backend_command.as_str()),
        "stored session backend_command",
        mismatches,
    );
    compare_option_u64(
        session.wrapper_timeout_ms,
        Some(snapshot.wrapper_timeout_ms),
        "stored session wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        session.service_status_max_staleness_ms,
        Some(snapshot.service_status_max_staleness_ms),
        "stored session service_status_max_staleness_ms",
        mismatches,
    );
    compare_option_string(
        session
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        Some(
            snapshot
                .reviewed_frozen_live_cutover_controller_command_summary
                .as_str(),
        ),
        "stored session reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_option_string(
        session.clerestory_certificate_summary.as_deref(),
        Some(snapshot.clerestory_certificate_summary.as_str()),
        "stored session clerestory_certificate_summary",
        mismatches,
    );
    compare_option_string(
        session.clerestory_certificate_sha256.as_deref(),
        Some(snapshot.clerestory_certificate_sha256.as_str()),
        "stored session clerestory_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        session.clerestory_certificate_algorithm.as_deref(),
        Some(snapshot.clerestory_certificate_algorithm.as_str()),
        "stored session clerestory_certificate_algorithm",
        mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "stored session session_dir",
        mismatches,
    );
    compare_string(
        &session.nested_launch_packet_latest_session_dir,
        &paths
            .nested_launch_packet_latest_session_dir
            .display()
            .to_string(),
        "stored session nested_launch_packet_latest_session_dir",
        mismatches,
    );
    compare_string(
        &session.nested_turn_green_session_dir,
        &paths.nested_turn_green_session_dir.display().to_string(),
        "stored session nested_turn_green_session_dir",
        mismatches,
    );
    compare_string(
        &session.downstream_launch_packet_session_dir,
        &snapshot
            .downstream_launch_packet_session_dir
            .display()
            .to_string(),
        "stored session downstream_launch_packet_session_dir",
        mismatches,
    );
    compare_string(
        &session.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "stored session explicit_statement",
        mismatches,
    );
}

fn compare_report_against_snapshot(
    report: &PackageTurnGreenLatestReport,
    root: &Path,
    session_dir: &Path,
    paths: &PackageTurnGreenLatestPaths,
    snapshot: &LatestTurnGreenSnapshot,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        report.root.as_deref(),
        Some(root.display().to_string().as_str()),
        "stored report root",
        mismatches,
    );
    compare_option_string(
        report.latest_top_step_name.as_deref(),
        Some(snapshot.latest_top_step_name.as_str()),
        "stored report latest_top_step_name",
        mismatches,
    );
    compare_option_string(
        report.latest_top_session_dir.as_deref(),
        Some(
            snapshot
                .latest_top_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        report.package_dir.as_deref(),
        Some(snapshot.package_dir.display().to_string().as_str()),
        "stored report package_dir",
        mismatches,
    );
    compare_option_string(
        report.install_root.as_deref(),
        Some(snapshot.install_root.display().to_string().as_str()),
        "stored report install_root",
        mismatches,
    );
    compare_option_string(
        report.target_service_name.as_deref(),
        Some(snapshot.target_service_name.as_str()),
        "stored report target_service_name",
        mismatches,
    );
    compare_option_string(
        report.backend_command.as_deref(),
        Some(snapshot.backend_command.as_str()),
        "stored report backend_command",
        mismatches,
    );
    compare_option_u64(
        report.wrapper_timeout_ms,
        Some(snapshot.wrapper_timeout_ms),
        "stored report wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        report.service_status_max_staleness_ms,
        Some(snapshot.service_status_max_staleness_ms),
        "stored report service_status_max_staleness_ms",
        mismatches,
    );
    compare_option_string(
        report
            .resolve_latest_launch_packet_command_summary
            .as_deref(),
        Some(
            resolve_latest_launch_packet_command_summary(
                root,
                &paths.nested_launch_packet_latest_session_dir,
            )
            .as_str(),
        ),
        "stored report resolve_latest_launch_packet_command_summary",
        mismatches,
    );
    compare_option_string(
        report.run_latest_launch_packet_command_summary.as_deref(),
        Some(
            run_latest_launch_packet_command_summary(
                root,
                &paths.nested_launch_packet_latest_session_dir,
            )
            .as_str(),
        ),
        "stored report run_latest_launch_packet_command_summary",
        mismatches,
    );
    compare_option_string(
        report
            .verify_latest_launch_packet_command_summary
            .as_deref(),
        Some(
            verify_latest_launch_packet_command_summary(
                &paths.nested_launch_packet_latest_session_dir,
            )
            .as_str(),
        ),
        "stored report verify_latest_launch_packet_command_summary",
        mismatches,
    );
    compare_option_string(
        report.downstream_turn_green_command_summary.as_deref(),
        Some(
            downstream_turn_green_command_summary(snapshot, &paths.nested_turn_green_session_dir)
                .as_str(),
        ),
        "stored report downstream_turn_green_command_summary",
        mismatches,
    );
    compare_option_string(
        report.verify_turn_green_command_summary.as_deref(),
        Some(
            verify_turn_green_command_summary(snapshot, &paths.nested_turn_green_session_dir)
                .as_str(),
        ),
        "stored report verify_turn_green_command_summary",
        mismatches,
    );
    compare_option_string(
        report
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        Some(
            snapshot
                .reviewed_frozen_live_cutover_controller_command_summary
                .as_str(),
        ),
        "stored report reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_option_string(
        report.clerestory_certificate_summary.as_deref(),
        Some(snapshot.clerestory_certificate_summary.as_str()),
        "stored report clerestory_certificate_summary",
        mismatches,
    );
    compare_option_string(
        report.clerestory_certificate_sha256.as_deref(),
        Some(snapshot.clerestory_certificate_sha256.as_str()),
        "stored report clerestory_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        report.clerestory_certificate_algorithm.as_deref(),
        Some(snapshot.clerestory_certificate_algorithm.as_str()),
        "stored report clerestory_certificate_algorithm",
        mismatches,
    );
    compare_option_string(
        report.turn_green_latest_session_path.as_deref(),
        Some(paths.session_path.display().to_string().as_str()),
        "stored report turn_green_latest_session_path",
        mismatches,
    );
    compare_option_string(
        report.turn_green_latest_status_path.as_deref(),
        Some(paths.status_path.display().to_string().as_str()),
        "stored report turn_green_latest_status_path",
        mismatches,
    );
    compare_option_string(
        report.turn_green_latest_report_path.as_deref(),
        Some(paths.report_path.display().to_string().as_str()),
        "stored report turn_green_latest_report_path",
        mismatches,
    );
    compare_option_string(
        report.latest_launch_packet_plan_report_path.as_deref(),
        Some(
            paths
                .latest_launch_packet_plan_report_path
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report latest_launch_packet_plan_report_path",
        mismatches,
    );
    compare_option_string(
        report.latest_launch_packet_report_path.as_deref(),
        Some(
            paths
                .latest_launch_packet_report_path
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report latest_launch_packet_report_path",
        mismatches,
    );
    compare_option_string(
        report.turn_green_report_path.as_deref(),
        Some(paths.turn_green_report_path.display().to_string().as_str()),
        "stored report turn_green_report_path",
        mismatches,
    );
    compare_option_string(
        report.nested_launch_packet_latest_session_dir.as_deref(),
        Some(
            paths
                .nested_launch_packet_latest_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report nested_launch_packet_latest_session_dir",
        mismatches,
    );
    compare_option_string(
        report.nested_turn_green_session_dir.as_deref(),
        Some(
            paths
                .nested_turn_green_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report nested_turn_green_session_dir",
        mismatches,
    );
    compare_option_string(
        report.downstream_launch_packet_session_dir.as_deref(),
        Some(
            snapshot
                .downstream_launch_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report downstream_launch_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        report.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored report session_dir",
        mismatches,
    );
    compare_string(
        &report.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "stored report explicit_statement",
        mismatches,
    );
}

fn compare_session_against_failure(
    session: &PackageTurnGreenLatestSession,
    root: &Path,
    session_dir: &Path,
    paths: &PackageTurnGreenLatestPaths,
    failure: &LatestTurnGreenResolutionFailure,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &session.root,
        &root.display().to_string(),
        "stored session root on failure",
        mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "stored session session_dir on failure",
        mismatches,
    );
    compare_string(
        &session.nested_launch_packet_latest_session_dir,
        &paths
            .nested_launch_packet_latest_session_dir
            .display()
            .to_string(),
        "stored session nested_launch_packet_latest_session_dir on failure",
        mismatches,
    );
    compare_string(
        &session.nested_turn_green_session_dir,
        &paths.nested_turn_green_session_dir.display().to_string(),
        "stored session nested_turn_green_session_dir on failure",
        mismatches,
    );
    if let Some(snapshot) = failure.snapshot.as_ref() {
        compare_session_against_snapshot(session, root, session_dir, paths, snapshot, mismatches);
    }
}

fn compare_report_against_failure(
    report: &PackageTurnGreenLatestReport,
    root: &Path,
    session_dir: &Path,
    paths: &PackageTurnGreenLatestPaths,
    failure: &LatestTurnGreenResolutionFailure,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        report.root.as_deref(),
        Some(root.display().to_string().as_str()),
        "stored report root on failure",
        mismatches,
    );
    compare_option_string(
        report.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored report session_dir on failure",
        mismatches,
    );
    compare_option_string(
        report.result.as_deref(),
        Some(serialize_enum(&result_from_failure_kind(failure.kind)).as_str()),
        "stored report result on failure",
        mismatches,
    );
    compare_string(
        &report.mode,
        "run_latest_turn_green",
        "stored report mode on failure",
        mismatches,
    );
    compare_string(
        &serialize_enum(&report.verdict),
        &serialize_enum(&verdict_from_failure_kind(failure.kind)),
        "stored report verdict on failure",
        mismatches,
    );
    compare_string(
        &report.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "stored report explicit_statement on failure",
        mismatches,
    );
    if let Some(snapshot) = failure.snapshot.as_ref() {
        compare_report_against_snapshot(report, root, session_dir, paths, snapshot, mismatches);
    }
}

fn required_option_string(value: Option<&String>, label: &str) -> Result<String> {
    value
        .filter(|entry| !entry.trim().is_empty())
        .cloned()
        .ok_or_else(|| anyhow!("missing required {label}"))
}

fn map_launch_packet_latest_failure_kind(verdict: &str) -> ResolutionFailureKind {
    match verdict {
        "tiny_live_package_launch_packet_latest_refused_by_missing_latest_chain" => {
            ResolutionFailureKind::MissingLatestChain
        }
        "tiny_live_package_launch_packet_latest_refused_by_drifted_launch_contract" => {
            ResolutionFailureKind::DriftedTurnGreenContract
        }
        _ => ResolutionFailureKind::InvalidLatestChain,
    }
}

fn latest_launch_packet_terminal_failure(
    report: &LaunchPacketLatestReportView,
) -> Option<LivePackageTurnGreenLatestResult> {
    match report.result.as_deref() {
        Some("refused_by_missing_latest_chain") => {
            Some(LivePackageTurnGreenLatestResult::RefusedByMissingLatestChain)
        }
        Some("refused_by_invalid_latest_chain") => {
            Some(LivePackageTurnGreenLatestResult::RefusedByInvalidLatestChain)
        }
        Some("refused_by_drifted_launch_contract") => {
            Some(LivePackageTurnGreenLatestResult::RefusedByDriftedTurnGreenContract)
        }
        _ => None,
    }
}

fn turn_green_latest_result_from_turn_green(
    result: &str,
) -> Option<LivePackageTurnGreenLatestResult> {
    match result {
        "executable_now" => Some(LivePackageTurnGreenLatestResult::ExecutableNow),
        "refused_now_by_stage3" => Some(LivePackageTurnGreenLatestResult::RefusedNowByStage3),
        "refused_now_by_pre_activation_gate" => {
            Some(LivePackageTurnGreenLatestResult::RefusedNowByPreActivationGate)
        }
        "refused_now_by_invalid_or_drifted_contract" => {
            Some(LivePackageTurnGreenLatestResult::RefusedByDriftedTurnGreenContract)
        }
        _ => None,
    }
}

fn turn_green_run_verdict_for_result(result: &str) -> Option<&'static str> {
    match result {
        "executable_now" => Some("tiny_live_package_turn_green_executable_now"),
        "refused_now_by_stage3" => Some("tiny_live_package_turn_green_refused_now_by_stage3"),
        "refused_now_by_pre_activation_gate" => {
            Some("tiny_live_package_turn_green_refused_now_by_pre_activation_gate")
        }
        "refused_now_by_invalid_or_drifted_contract" => {
            Some("tiny_live_package_turn_green_refused_now_by_invalid_or_drifted_contract")
        }
        _ => None,
    }
}

fn result_from_failure_kind(kind: ResolutionFailureKind) -> LivePackageTurnGreenLatestResult {
    match kind {
        ResolutionFailureKind::MissingLatestChain => {
            LivePackageTurnGreenLatestResult::RefusedByMissingLatestChain
        }
        ResolutionFailureKind::InvalidLatestChain => {
            LivePackageTurnGreenLatestResult::RefusedByInvalidLatestChain
        }
        ResolutionFailureKind::DriftedTurnGreenContract => {
            LivePackageTurnGreenLatestResult::RefusedByDriftedTurnGreenContract
        }
    }
}

fn verdict_for_result(
    result: LivePackageTurnGreenLatestResult,
) -> TinyLivePackageTurnGreenLatestVerdict {
    match result {
        LivePackageTurnGreenLatestResult::RefusedByMissingLatestChain => {
            TinyLivePackageTurnGreenLatestVerdict::TinyLivePackageTurnGreenLatestRefusedByMissingLatestChain
        }
        LivePackageTurnGreenLatestResult::RefusedByInvalidLatestChain => {
            TinyLivePackageTurnGreenLatestVerdict::TinyLivePackageTurnGreenLatestRefusedByInvalidLatestChain
        }
        LivePackageTurnGreenLatestResult::RefusedByDriftedTurnGreenContract => {
            TinyLivePackageTurnGreenLatestVerdict::TinyLivePackageTurnGreenLatestRefusedByDriftedTurnGreenContract
        }
        LivePackageTurnGreenLatestResult::RefusedNowByStage3 => {
            TinyLivePackageTurnGreenLatestVerdict::TinyLivePackageTurnGreenLatestRefusedNowByStage3
        }
        LivePackageTurnGreenLatestResult::RefusedNowByPreActivationGate => {
            TinyLivePackageTurnGreenLatestVerdict::TinyLivePackageTurnGreenLatestRefusedNowByPreActivationGate
        }
        LivePackageTurnGreenLatestResult::ExecutableNow => {
            TinyLivePackageTurnGreenLatestVerdict::TinyLivePackageTurnGreenLatestExecutableNow
        }
    }
}

fn verdict_from_failure_kind(kind: ResolutionFailureKind) -> TinyLivePackageTurnGreenLatestVerdict {
    verdict_for_result(result_from_failure_kind(kind))
}

fn operator_next_action_summary(result: LivePackageTurnGreenLatestResult) -> String {
    match result {
        LivePackageTurnGreenLatestResult::RefusedByMissingLatestChain => {
            "no immutable clerestory chain is available yet; do not manually hunt launch-packet or turn-green lineage by hand".to_string()
        }
        LivePackageTurnGreenLatestResult::RefusedByInvalidLatestChain => {
            "the latest immutable chain is incomplete or below the accepted clerestory layer; mint a fresh coherent clerestory chain instead of bypassing the accepted latest-launch-packet / turn-green path".to_string()
        }
        LivePackageTurnGreenLatestResult::RefusedByDriftedTurnGreenContract => {
            "the latest chain no longer resolves cleanly into the accepted latest-launch-packet / turn-green contract; repair that drift instead of bypassing the bounded handoff path".to_string()
        }
        LivePackageTurnGreenLatestResult::RefusedNowByStage3 => {
            "the frozen controller stays refused now by Stage 3; rerun this latest-turn-green refresh after current Stage 3 truth turns green".to_string()
        }
        LivePackageTurnGreenLatestResult::RefusedNowByPreActivationGate => {
            "the frozen controller stays refused now by the pre-activation gate; rerun this latest-turn-green refresh after the current pre-activation gate turns green".to_string()
        }
        LivePackageTurnGreenLatestResult::ExecutableNow => {
            "the latest immutable chain now resolves into an executable-now turn-green handoff; the frozen controller is executable now only if the operator explicitly chooses to run the already accepted controller command".to_string()
        }
    }
}

fn serialize_enum<T: Serialize>(value: &T) -> String {
    match serde_json::to_value(value) {
        Ok(Value::String(raw)) => raw,
        Ok(other) => other.to_string(),
        Err(_) => "<serialization-error>".to_string(),
    }
}

fn turn_green_verify_frozen_launch_packet_summary(launch_packet_session_dir: &Path) -> String {
    format!(
        "verify immutable launch packet session under {} before refreshing current gate/controller truth",
        launch_packet_session_dir.display()
    )
}

fn turn_green_refresh_current_authorization_command_summary(
    snapshot: &LatestTurnGreenSnapshot,
    current_authorization_session_dir: &Path,
) -> String {
    format!(
        "copybot_tiny_live_activation_package_live_authorization --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --service-status-max-staleness-ms {} --session-dir {} --run-live-package-authorization",
        snapshot.package_dir.display(),
        snapshot.install_root.display(),
        snapshot.target_service_name,
        snapshot.backend_command,
        snapshot.wrapper_timeout_ms,
        snapshot.service_status_max_staleness_ms,
        current_authorization_session_dir.display()
    )
}

fn refusal_status(
    root: &Path,
    session_dir: &Path,
    paths: &PackageTurnGreenLatestPaths,
    resolved: Option<&ResolvedLatestTurnGreen>,
    result: LivePackageTurnGreenLatestResult,
    reason: String,
    latest_launch_packet_plan_step: Option<PackageTurnGreenLatestStepArtifact>,
    latest_launch_packet_step: Option<PackageTurnGreenLatestStepArtifact>,
    turn_green_step: Option<PackageTurnGreenLatestStepArtifact>,
) -> PackageTurnGreenLatestStatus {
    let snapshot = resolved.map(|value| &value.snapshot);
    PackageTurnGreenLatestStatus {
        status_version: TURN_GREEN_LATEST_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        root: root.display().to_string(),
        session_dir: session_dir.display().to_string(),
        latest_top_step_name: snapshot.map(|value| value.latest_top_step_name.clone()),
        latest_top_session_dir: snapshot
            .map(|value| value.latest_top_session_dir.display().to_string()),
        nested_launch_packet_latest_session_dir: paths
            .nested_launch_packet_latest_session_dir
            .display()
            .to_string(),
        nested_turn_green_session_dir: paths.nested_turn_green_session_dir.display().to_string(),
        downstream_launch_packet_session_dir: snapshot
            .map(|value| {
                value
                    .downstream_launch_packet_session_dir
                    .display()
                    .to_string()
            })
            .unwrap_or_else(|| {
                expected_downstream_launch_packet_session_dir(paths)
                    .display()
                    .to_string()
            }),
        result,
        reason,
        latest_launch_packet_plan_verdict: resolved
            .map(|value| value.latest_launch_packet_plan.verdict.clone()),
        latest_launch_packet_verdict: latest_launch_packet_step
            .as_ref()
            .map(|value| value.verdict.clone()),
        turn_green_verdict: turn_green_step.as_ref().map(|value| value.verdict.clone()),
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        latest_launch_packet_plan_step,
        latest_launch_packet_step,
        turn_green_step,
        operator_next_action_summary: operator_next_action_summary(result),
        executable_now: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn refusal_status_from_resolution_failure(
    root: &Path,
    session_dir: &Path,
    paths: &PackageTurnGreenLatestPaths,
    failure: &LatestTurnGreenResolutionFailure,
) -> PackageTurnGreenLatestStatus {
    PackageTurnGreenLatestStatus {
        status_version: TURN_GREEN_LATEST_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        root: root.display().to_string(),
        session_dir: session_dir.display().to_string(),
        latest_top_step_name: failure
            .snapshot
            .as_ref()
            .map(|value| value.latest_top_step_name.clone()),
        latest_top_session_dir: failure
            .snapshot
            .as_ref()
            .map(|value| value.latest_top_session_dir.display().to_string()),
        nested_launch_packet_latest_session_dir: paths
            .nested_launch_packet_latest_session_dir
            .display()
            .to_string(),
        nested_turn_green_session_dir: paths.nested_turn_green_session_dir.display().to_string(),
        downstream_launch_packet_session_dir: failure
            .snapshot
            .as_ref()
            .map(|value| {
                value
                    .downstream_launch_packet_session_dir
                    .display()
                    .to_string()
            })
            .unwrap_or_else(|| {
                expected_downstream_launch_packet_session_dir(paths)
                    .display()
                    .to_string()
            }),
        result: result_from_failure_kind(failure.kind),
        reason: failure.reason.clone(),
        latest_launch_packet_plan_verdict: failure.latest_launch_packet_plan_verdict.clone(),
        latest_launch_packet_verdict: None,
        turn_green_verdict: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        latest_launch_packet_plan_step: failure.latest_launch_packet_plan_raw.as_ref().map(|_| {
            step_artifact(
                &paths.latest_launch_packet_plan_report_path,
                "plan_latest_launch_packet",
                failure
                    .latest_launch_packet_plan_verdict
                    .as_deref()
                    .unwrap_or("<missing>"),
                failure
                    .latest_launch_packet_plan_reason
                    .as_deref()
                    .unwrap_or("<missing>"),
                failure
                    .latest_launch_packet_plan_generated_at
                    .unwrap_or_else(Utc::now),
            )
        }),
        latest_launch_packet_step: None,
        turn_green_step: None,
        operator_next_action_summary: operator_next_action_summary(result_from_failure_kind(
            failure.kind,
        )),
        executable_now: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn verify_invalid_report_without_session(
    session_dir: &Path,
    verification_mismatches: Vec<String>,
) -> PackageTurnGreenLatestReport {
    let reason = verification_mismatches
        .first()
        .cloned()
        .unwrap_or_else(|| "latest-turn-green verification failed".to_string());
    PackageTurnGreenLatestReport {
        generated_at: Utc::now(),
        mode: "verify_latest_turn_green".to_string(),
        verdict: TinyLivePackageTurnGreenLatestVerdict::TinyLivePackageTurnGreenLatestVerifyInvalid,
        reason,
        root: None,
        latest_top_step_name: None,
        latest_top_session_dir: None,
        package_dir: None,
        install_root: None,
        target_service_name: None,
        backend_command: None,
        wrapper_timeout_ms: None,
        service_status_max_staleness_ms: None,
        session_dir: Some(session_dir.display().to_string()),
        turn_green_latest_session_path: Some(
            turn_green_latest_paths(session_dir)
                .session_path
                .display()
                .to_string(),
        ),
        turn_green_latest_status_path: Some(
            turn_green_latest_paths(session_dir)
                .status_path
                .display()
                .to_string(),
        ),
        turn_green_latest_report_path: Some(
            turn_green_latest_paths(session_dir)
                .report_path
                .display()
                .to_string(),
        ),
        latest_launch_packet_plan_report_path: None,
        latest_launch_packet_report_path: None,
        turn_green_report_path: None,
        nested_launch_packet_latest_session_dir: None,
        nested_turn_green_session_dir: None,
        downstream_launch_packet_session_dir: None,
        latest_launch_packet_plan_verdict: None,
        latest_launch_packet_verdict: None,
        turn_green_verdict: None,
        result: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        resolve_latest_launch_packet_command_summary: None,
        run_latest_launch_packet_command_summary: None,
        verify_latest_launch_packet_command_summary: None,
        downstream_turn_green_command_summary: None,
        verify_turn_green_command_summary: None,
        reviewed_frozen_live_cutover_controller_command_summary: None,
        clerestory_certificate_summary: None,
        clerestory_certificate_sha256: None,
        clerestory_certificate_algorithm: None,
        verification_mismatches,
        executable_now: false,
        explicit_statement:
            "this verification proves only whether the latest immutable chain still resolves cleanly into the accepted turn-green contract; it never runs the frozen controller"
                .to_string(),
    }
}

fn fallback_launch_packet_latest_copy() -> LaunchPacketLatestReportView {
    LaunchPacketLatestReportView {
        generated_at: Utc::now(),
        mode: "<missing>".to_string(),
        verdict: "<missing>".to_string(),
        reason: "<missing>".to_string(),
        latest_top_step_name: None,
        latest_top_session_dir: None,
        package_dir: None,
        install_root: None,
        target_service_name: None,
        backend_command: None,
        wrapper_timeout_ms: None,
        service_status_max_staleness_ms: None,
        session_dir: None,
        nested_launch_packet_session_dir: None,
        result: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        reviewed_frozen_live_cutover_controller_command_summary: None,
        clerestory_certificate_summary: None,
        clerestory_certificate_sha256: None,
        clerestory_certificate_algorithm: None,
        explicit_statement: None,
    }
}

fn fallback_turn_green_session(
    snapshot: &LatestTurnGreenSnapshot,
    session_dir: &Path,
) -> TurnGreenSessionView {
    TurnGreenSessionView {
        launch_packet_session_dir: snapshot.downstream_launch_packet_session_dir.display().to_string(),
        package_dir: snapshot.package_dir.display().to_string(),
        install_root: snapshot.install_root.display().to_string(),
        target_service_name: snapshot.target_service_name.clone(),
        backend_command: snapshot.backend_command.clone(),
        wrapper_timeout_ms: snapshot.wrapper_timeout_ms,
        service_status_max_staleness_ms: snapshot.service_status_max_staleness_ms,
        session_dir: session_dir.display().to_string(),
        nested_current_authorization_session_dir: nested_turn_green_current_authorization_session_dir(session_dir)
            .display()
            .to_string(),
        verify_frozen_launch_packet_summary: turn_green_verify_frozen_launch_packet_summary(
            &snapshot.downstream_launch_packet_session_dir,
        ),
        refresh_current_authorization_command_summary:
            turn_green_refresh_current_authorization_command_summary(
                snapshot,
                &nested_turn_green_current_authorization_session_dir(session_dir),
            ),
        frozen_live_cutover_controller_command_summary: snapshot
            .reviewed_frozen_live_cutover_controller_command_summary
            .clone(),
        explicit_statement:
            "the immutable launch packet is the primary handoff input here; current host truth may only refuse or mark that frozen controller executable now"
                .to_string(),
    }
}

fn fallback_turn_green_status(
    _snapshot: &LatestTurnGreenSnapshot,
    session_dir: &Path,
) -> TurnGreenStatusView {
    TurnGreenStatusView {
        session_dir: session_dir.display().to_string(),
        launch_packet_session_dir: "<missing>".to_string(),
        nested_current_authorization_session_dir: nested_turn_green_current_authorization_session_dir(session_dir)
            .display()
            .to_string(),
        result: "refused_now_by_invalid_or_drifted_contract".to_string(),
        reason: "<missing>".to_string(),
        frozen_launch_packet_result: None,
        current_authorization_result_now: None,
        frozen_controller_still_current: false,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        operator_next_action_summary:
            "the frozen launch packet or current live contract drifted; repair the target or mint a new launch packet before considering execution"
                .to_string(),
        executable_now: false,
        explicit_statement:
            "this packet-native turn-green refresh never runs the frozen live cutover controller; it only classifies whether that frozen controller is executable now under current truth"
                .to_string(),
    }
}

fn render_report_lines(report: &PackageTurnGreenLatestReport) -> String {
    let mut lines = vec![
        format!("mode={}", report.mode),
        format!("verdict={}", serialize_enum(&report.verdict)),
        format!("reason={}", report.reason),
    ];
    if let Some(value) = &report.root {
        lines.push(format!("root={value}"));
    }
    if let Some(value) = &report.latest_top_step_name {
        lines.push(format!("latest_top_step_name={value}"));
    }
    if let Some(value) = &report.latest_top_session_dir {
        lines.push(format!("latest_top_session_dir={value}"));
    }
    if let Some(value) = &report.session_dir {
        lines.push(format!("session_dir={value}"));
    }
    if let Some(value) = &report.nested_launch_packet_latest_session_dir {
        lines.push(format!("nested_launch_packet_latest_session_dir={value}"));
    }
    if let Some(value) = &report.downstream_launch_packet_session_dir {
        lines.push(format!("downstream_launch_packet_session_dir={value}"));
    }
    if let Some(value) = &report.nested_turn_green_session_dir {
        lines.push(format!("nested_turn_green_session_dir={value}"));
    }
    if let Some(value) = &report.result {
        lines.push(format!("result={value}"));
    }
    if let Some(value) = &report.downstream_turn_green_command_summary {
        lines.push(format!("downstream_turn_green_command_summary={value}"));
    }
    if let Some(value) = &report.reviewed_frozen_live_cutover_controller_command_summary {
        lines.push(format!(
            "reviewed_frozen_live_cutover_controller_command_summary={value}"
        ));
    }
    lines.push(format!("executable_now={}", report.executable_now));
    lines.push(format!("explicit_statement={}", report.explicit_statement));
    if !report.verification_mismatches.is_empty() {
        lines.push("verification_mismatches:".to_string());
        lines.extend(
            report
                .verification_mismatches
                .iter()
                .map(|entry| format!("  - {entry}")),
        );
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::ffi::OsString;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    struct FakeTurnGreenLatestFixture {
        temp_dir: PathBuf,
        root: PathBuf,
        wrapper_session_dir: PathBuf,
        output_script_path: PathBuf,
        bin_dir: PathBuf,
        latest_top_session_dir: PathBuf,
        package_dir: PathBuf,
        install_root: PathBuf,
        backend_command: PathBuf,
        launch_packet_latest_plan_json_path: PathBuf,
        launch_packet_latest_run_json_path: PathBuf,
        launch_packet_latest_verify_json_path: PathBuf,
        turn_green_run_json_path: PathBuf,
        turn_green_verify_json_path: PathBuf,
        turn_green_session_json_path: PathBuf,
        turn_green_status_json_path: PathBuf,
        frozen_controller_command: String,
    }

    impl FakeTurnGreenLatestFixture {
        fn plan_config(&self) -> Config {
            Config {
                mode: Mode::PlanLatestTurnGreen,
                root: Some(self.root.clone()),
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn render_config(&self) -> Config {
            Config {
                mode: Mode::RenderLatestTurnGreenScript,
                root: Some(self.root.clone()),
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: Some(self.output_script_path.clone()),
                json: true,
            }
        }

        fn run_config(&self) -> Config {
            Config {
                mode: Mode::RunLatestTurnGreen,
                root: Some(self.root.clone()),
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn verify_config(&self) -> Config {
            Config {
                mode: Mode::VerifyLatestTurnGreen,
                root: None,
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }
    }

    #[test]
    fn plan_mode_resolves_latest_valid_clerestory_chain_to_exact_expected_nested_latest_launch_packet_lineage(
    ) {
        let fixture = fake_turn_green_latest_fixture(
            "turn_green_latest_plan_ok",
            "completed",
            "executable_now",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let report = plan_latest_turn_green_report(&fixture.plan_config()).unwrap();
        let paths = turn_green_latest_paths(&fixture.wrapper_session_dir);
        assert_eq!(
            report.verdict,
            TinyLivePackageTurnGreenLatestVerdict::TinyLivePackageTurnGreenLatestPlanReady
        );
        assert_eq!(
            report.latest_top_step_name.as_deref(),
            Some("clerestory_certificate")
        );
        assert_eq!(
            report.nested_launch_packet_latest_session_dir.as_deref(),
            Some(
                paths
                    .nested_launch_packet_latest_session_dir
                    .display()
                    .to_string()
                    .as_str()
            )
        );
        assert_eq!(
            report.downstream_launch_packet_session_dir.as_deref(),
            Some(
                expected_downstream_launch_packet_session_dir(&paths)
                    .display()
                    .to_string()
                    .as_str()
            )
        );
        assert!(report
            .downstream_turn_green_command_summary
            .as_deref()
            .unwrap()
            .contains(TURN_GREEN_BIN));
    }

    #[test]
    fn render_mode_emits_exact_downstream_turn_green_contract_not_second_controller_path() {
        let fixture = fake_turn_green_latest_fixture(
            "turn_green_latest_render_ok",
            "completed",
            "executable_now",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let report = render_latest_turn_green_script_report(&fixture.render_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageTurnGreenLatestVerdict::TinyLivePackageTurnGreenLatestScriptRendered
        );
        let script = fs::read_to_string(&fixture.output_script_path).unwrap();
        assert!(script.contains(LAUNCH_PACKET_LATEST_BIN));
        assert!(script.contains(TURN_GREEN_BIN));
        assert!(script.contains("--run-live-package-turn-green"));
        assert!(script.contains("--verify-live-package-turn-green"));
        assert!(!script.contains("copybot_tiny_live_activation_package_live_cutover"));
    }

    #[test]
    fn run_mode_refuses_when_latest_chain_missing_invalid_or_below_clerestory() {
        for (label, verdict, reason, expected) in [
            (
                "turn_green_latest_missing",
                "tiny_live_package_launch_packet_latest_refused_by_missing_latest_chain",
                "no persisted immutable chain exists",
                TinyLivePackageTurnGreenLatestVerdict::TinyLivePackageTurnGreenLatestRefusedByMissingLatestChain,
            ),
            (
                "turn_green_latest_invalid",
                "tiny_live_package_launch_packet_latest_refused_by_invalid_latest_chain",
                "latest immutable chain is incomplete",
                TinyLivePackageTurnGreenLatestVerdict::TinyLivePackageTurnGreenLatestRefusedByInvalidLatestChain,
            ),
            (
                "turn_green_latest_below",
                "tiny_live_package_launch_packet_latest_refused_by_invalid_latest_chain",
                "latest persisted top chain is choir_receipt, which is below clerestory_certificate",
                TinyLivePackageTurnGreenLatestVerdict::TinyLivePackageTurnGreenLatestRefusedByInvalidLatestChain,
            ),
        ] {
            let fixture =
                fake_turn_green_latest_fixture(label, "completed", "executable_now");
            let _path_guard = PathGuard::install(&fixture.bin_dir);

            let mut plan: Value = load_json(&fixture.launch_packet_latest_plan_json_path).unwrap();
            plan["verdict"] = json!(verdict);
            plan["reason"] = json!(reason);
            if label.ends_with("below") {
                plan["latest_top_step_name"] = json!("choir_receipt");
            }
            persist_json(&fixture.launch_packet_latest_plan_json_path, &plan).unwrap();

            let report = run_latest_turn_green_report(&fixture.run_config()).unwrap();
            assert_eq!(report.verdict, expected);
        }
    }

    #[test]
    fn run_mode_refuses_when_nested_launch_packet_lineage_drifts_from_latest_immutable_chain() {
        let fixture = fake_turn_green_latest_fixture(
            "turn_green_latest_launch_packet_drift",
            "completed",
            "executable_now",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let mut run: Value = load_json(&fixture.launch_packet_latest_run_json_path).unwrap();
        run["package_dir"] = json!("/tampered/package");
        persist_json(&fixture.launch_packet_latest_run_json_path, &run).unwrap();

        let report = run_latest_turn_green_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageTurnGreenLatestVerdict::TinyLivePackageTurnGreenLatestRefusedByDriftedTurnGreenContract
        );
        assert!(report
            .reason
            .contains("stored latest-launch-packet run package_dir"));
    }

    #[test]
    fn verify_mode_fails_when_persisted_latest_turn_green_artifacts_are_tampered_or_missing_nested_lineage_truth(
    ) {
        {
            let fixture = fake_turn_green_latest_fixture(
                "turn_green_latest_verify_tampered_copy",
                "completed",
                "executable_now",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_latest_turn_green_report(&fixture.run_config()).unwrap();

            let paths = turn_green_latest_paths(&fixture.wrapper_session_dir);
            let mut report: Value = load_json(&paths.turn_green_report_path).unwrap();
            report["reason"] = json!("tampered stored turn-green copy reason");
            persist_json(&paths.turn_green_report_path, &report).unwrap();

            let verify = verify_latest_turn_green_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageTurnGreenLatestVerdict::TinyLivePackageTurnGreenLatestVerifyInvalid
            );
            assert!(verify
                .verification_mismatches
                .iter()
                .any(|entry| entry.contains("stored turn-green copy reason")));
        }

        {
            let fixture = fake_turn_green_latest_fixture(
                "turn_green_latest_verify_missing_nested_truth",
                "completed",
                "executable_now",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_latest_turn_green_report(&fixture.run_config()).unwrap();

            let paths = turn_green_latest_paths(&fixture.wrapper_session_dir);
            fs::remove_file(nested_launch_packet_latest_report_path(
                &paths.nested_launch_packet_latest_session_dir,
            ))
            .unwrap();

            let verify = verify_latest_turn_green_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageTurnGreenLatestVerdict::TinyLivePackageTurnGreenLatestVerifyInvalid
            );
            assert!(verify.verification_mismatches.iter().any(|entry| {
                entry.contains("tiny_live_activation_package_launch_packet_latest.report.json")
            }));
        }
    }

    #[test]
    fn verify_fails_when_copied_nested_latest_launch_packet_explicit_statement_drifts() {
        let fixture = fake_turn_green_latest_fixture(
            "turn_green_latest_verify_launch_packet_explicit_statement_drift",
            "completed",
            "executable_now",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_turn_green_report(&fixture.run_config()).unwrap();

        let paths = turn_green_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.latest_launch_packet_report_path).unwrap();
        report["explicit_statement"] =
            json!("tampered nested latest launch packet explicit statement");
        persist_json(&paths.latest_launch_packet_report_path, &report).unwrap();

        let verify = verify_latest_turn_green_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageTurnGreenLatestVerdict::TinyLivePackageTurnGreenLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored latest-launch-packet copy explicit_statement")
        }));
    }

    #[test]
    fn verify_fails_when_copied_nested_turn_green_frozen_launch_packet_result_drifts() {
        let fixture = fake_turn_green_latest_fixture(
            "turn_green_latest_verify_frozen_launch_packet_result_drift",
            "completed",
            "executable_now",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_turn_green_report(&fixture.run_config()).unwrap();

        let paths = turn_green_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.turn_green_report_path).unwrap();
        report["frozen_launch_packet_result"] = json!("tampered_frozen_launch_packet_result");
        persist_json(&paths.turn_green_report_path, &report).unwrap();

        let verify = verify_latest_turn_green_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageTurnGreenLatestVerdict::TinyLivePackageTurnGreenLatestVerifyInvalid
        );
        assert!(verify
            .verification_mismatches
            .iter()
            .any(|entry| { entry.contains("stored turn-green copy frozen_launch_packet_result") }));
    }

    #[test]
    fn verify_fails_when_copied_nested_turn_green_current_authorization_result_now_drifts() {
        let fixture = fake_turn_green_latest_fixture(
            "turn_green_latest_verify_current_authorization_result_drift",
            "completed",
            "executable_now",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_turn_green_report(&fixture.run_config()).unwrap();

        let paths = turn_green_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.turn_green_report_path).unwrap();
        report["current_authorization_result_now"] =
            json!("tampered_current_authorization_result_now");
        persist_json(&paths.turn_green_report_path, &report).unwrap();

        let verify = verify_latest_turn_green_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageTurnGreenLatestVerdict::TinyLivePackageTurnGreenLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored turn-green copy current_authorization_result_now")
        }));
    }

    #[test]
    fn verify_fails_when_copied_nested_turn_green_verdict_drifts() {
        let fixture = fake_turn_green_latest_fixture(
            "turn_green_latest_verify_turn_green_verdict_drift",
            "completed",
            "executable_now",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_turn_green_report(&fixture.run_config()).unwrap();

        let paths = turn_green_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.turn_green_report_path).unwrap();
        report["verdict"] = json!("tampered_turn_green_verdict");
        persist_json(&paths.turn_green_report_path, &report).unwrap();

        let verify = verify_latest_turn_green_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageTurnGreenLatestVerdict::TinyLivePackageTurnGreenLatestVerifyInvalid
        );
        assert!(verify
            .verification_mismatches
            .iter()
            .any(|entry| entry.contains("stored turn-green copy verdict")));
    }

    #[test]
    fn verify_fails_when_copied_nested_turn_green_current_live_cutover_controller_summary_drifts() {
        let fixture = fake_turn_green_latest_fixture(
            "turn_green_latest_verify_current_live_cutover_summary_drift",
            "completed",
            "executable_now",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_turn_green_report(&fixture.run_config()).unwrap();

        let paths = turn_green_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.turn_green_report_path).unwrap();
        report["current_live_cutover_controller_command_summary"] =
            json!("tampered live cutover controller summary");
        persist_json(&paths.turn_green_report_path, &report).unwrap();

        let verify = verify_latest_turn_green_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageTurnGreenLatestVerdict::TinyLivePackageTurnGreenLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored turn-green copy current_live_cutover_controller_command_summary")
        }));
    }

    #[test]
    fn all_modes_preserve_stage3_and_pre_activation_refusal_semantics_and_do_not_imply_activation_authorization_by_themselves(
    ) {
        {
            let fixture = fake_turn_green_latest_fixture(
                "turn_green_latest_stage3",
                "refused_now_by_stage3",
                "refused_now_by_stage3",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);

            let plan = plan_latest_turn_green_report(&fixture.plan_config()).unwrap();
            assert_eq!(
                plan.verdict,
                TinyLivePackageTurnGreenLatestVerdict::TinyLivePackageTurnGreenLatestPlanReady
            );
            assert!(!plan.executable_now);

            let run = run_latest_turn_green_report(&fixture.run_config()).unwrap();
            assert_eq!(
                run.verdict,
                TinyLivePackageTurnGreenLatestVerdict::TinyLivePackageTurnGreenLatestRefusedNowByStage3
            );
            assert_eq!(run.result.as_deref(), Some("refused_now_by_stage3"));
            assert!(!run.executable_now);

            let verify = verify_latest_turn_green_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageTurnGreenLatestVerdict::TinyLivePackageTurnGreenLatestVerifyOk,
                "{:#?}",
                verify.verification_mismatches
            );
            assert!(!verify.executable_now);
        }

        {
            let fixture = fake_turn_green_latest_fixture(
                "turn_green_latest_pre_activation",
                "refused_now_by_pre_activation_gate",
                "refused_now_by_pre_activation_gate",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);

            let run = run_latest_turn_green_report(&fixture.run_config()).unwrap();
            assert_eq!(
                run.verdict,
                TinyLivePackageTurnGreenLatestVerdict::TinyLivePackageTurnGreenLatestRefusedNowByPreActivationGate
            );
            assert_eq!(
                run.result.as_deref(),
                Some("refused_now_by_pre_activation_gate")
            );
            assert!(!run.executable_now);

            let verify = verify_latest_turn_green_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageTurnGreenLatestVerdict::TinyLivePackageTurnGreenLatestVerifyOk,
                "{:#?}",
                verify.verification_mismatches
            );
            assert!(!verify.executable_now);
        }

        {
            let fixture = fake_turn_green_latest_fixture(
                "turn_green_latest_green",
                "completed",
                "executable_now",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);

            let run = run_latest_turn_green_report(&fixture.run_config()).unwrap();
            assert_eq!(
                run.verdict,
                TinyLivePackageTurnGreenLatestVerdict::TinyLivePackageTurnGreenLatestExecutableNow
            );
            assert_eq!(run.result.as_deref(), Some("executable_now"));
            assert!(run.executable_now);
            assert!(run
                .explicit_statement
                .contains("never runs the frozen live cutover controller"));
        }
    }

    fn fake_turn_green_latest_fixture(
        label: &str,
        launch_packet_latest_run_result: &str,
        turn_green_result: &str,
    ) -> FakeTurnGreenLatestFixture {
        let temp_dir = temp_dir(label);
        let root = temp_dir.join("root");
        let wrapper_session_dir = temp_dir.join("turn-green-latest-session");
        let output_script_path = temp_dir.join("turn-green-latest.sh");
        let bin_dir = temp_dir.join("bin");
        let latest_top_session_dir = temp_dir.join("latest-clerestory-session");
        let package_dir = temp_dir.join("package");
        let install_root = temp_dir.join("install-root");
        let backend_command = temp_dir.join("backend");
        let launch_packet_latest_plan_json_path = temp_dir.join("launch_packet_latest.plan.json");
        let launch_packet_latest_run_json_path = temp_dir.join("launch_packet_latest.run.json");
        let launch_packet_latest_verify_json_path =
            temp_dir.join("launch_packet_latest.verify.json");
        let turn_green_run_json_path = temp_dir.join("turn_green.run.json");
        let turn_green_verify_json_path = temp_dir.join("turn_green.verify.json");
        let turn_green_session_json_path = temp_dir.join("turn_green.session.json");
        let turn_green_status_json_path = temp_dir.join("turn_green.status.json");
        let frozen_controller_command =
            "copybot_tiny_live_activation_package_live_cutover --package-dir /fake/pkg".to_string();

        for path in [
            &root,
            &bin_dir,
            &latest_top_session_dir,
            &package_dir,
            &install_root,
        ] {
            fs::create_dir_all(path).unwrap();
        }
        fs::write(&backend_command, "#!/bin/sh\n").unwrap();

        let fixture = FakeTurnGreenLatestFixture {
            temp_dir,
            root,
            wrapper_session_dir,
            output_script_path,
            bin_dir,
            latest_top_session_dir,
            package_dir,
            install_root,
            backend_command,
            launch_packet_latest_plan_json_path,
            launch_packet_latest_run_json_path,
            launch_packet_latest_verify_json_path,
            turn_green_run_json_path,
            turn_green_verify_json_path,
            turn_green_session_json_path,
            turn_green_status_json_path,
            frozen_controller_command,
        };

        persist_json(
            &fixture.launch_packet_latest_plan_json_path,
            &default_launch_packet_latest_plan_report(&fixture),
        )
        .unwrap();
        persist_json(
            &fixture.launch_packet_latest_run_json_path,
            &default_launch_packet_latest_run_report(&fixture, launch_packet_latest_run_result),
        )
        .unwrap();
        persist_json(
            &fixture.launch_packet_latest_verify_json_path,
            &default_launch_packet_latest_verify_report(&fixture, launch_packet_latest_run_result),
        )
        .unwrap();
        persist_json(
            &fixture.turn_green_session_json_path,
            &default_turn_green_session(&fixture),
        )
        .unwrap();
        persist_json(
            &fixture.turn_green_status_json_path,
            &default_turn_green_status(&fixture, turn_green_result),
        )
        .unwrap();
        persist_json(
            &fixture.turn_green_run_json_path,
            &default_turn_green_run_report(&fixture, turn_green_result),
        )
        .unwrap();
        persist_json(
            &fixture.turn_green_verify_json_path,
            &default_turn_green_verify_report(&fixture, turn_green_result),
        )
        .unwrap();

        let paths = turn_green_latest_paths(&fixture.wrapper_session_dir);
        write_fake_launch_packet_latest_command(
            &fixture.bin_dir.join(LAUNCH_PACKET_LATEST_BIN),
            &fixture.launch_packet_latest_plan_json_path,
            &fixture.launch_packet_latest_run_json_path,
            &fixture.launch_packet_latest_verify_json_path,
            &fixture.root,
            &paths.nested_launch_packet_latest_session_dir,
        );
        write_fake_turn_green_command(
            &fixture.bin_dir.join(TURN_GREEN_BIN),
            &fixture.turn_green_run_json_path,
            &fixture.turn_green_verify_json_path,
            &fixture.turn_green_session_json_path,
            &fixture.turn_green_status_json_path,
            &expected_downstream_launch_packet_session_dir(&paths),
            &paths.nested_turn_green_session_dir,
        );

        fixture
    }

    fn default_launch_packet_latest_plan_report(fixture: &FakeTurnGreenLatestFixture) -> Value {
        let paths = turn_green_latest_paths(&fixture.wrapper_session_dir);
        json!({
            "generated_at": Utc::now(),
            "mode": "plan_latest_launch_packet",
            "verdict": LAUNCH_PACKET_LATEST_PLAN_READY,
            "reason": "latest immutable chain resolves cleanly into accepted latest-launch-packet contract",
            "latest_top_step_name": "clerestory_certificate",
            "latest_top_session_dir": fixture.latest_top_session_dir.display().to_string(),
            "package_dir": fixture.package_dir.display().to_string(),
            "install_root": fixture.install_root.display().to_string(),
            "target_service_name": "solana-copy-bot.service",
            "backend_command": fixture.backend_command.display().to_string(),
            "wrapper_timeout_ms": 1200,
            "service_status_max_staleness_ms": 4500,
            "session_dir": paths.nested_launch_packet_latest_session_dir.display().to_string(),
            "nested_launch_packet_session_dir": expected_downstream_launch_packet_session_dir(&paths).display().to_string(),
            "reviewed_frozen_live_cutover_controller_command_summary": fixture.frozen_controller_command,
            "clerestory_certificate_summary": "latest clerestory summary",
            "clerestory_certificate_sha256": "latest-clerestory-sha",
            "clerestory_certificate_algorithm": "sha256",
            "explicit_statement": "latest launch packet wrapper plan"
        })
    }

    fn default_launch_packet_latest_run_report(
        fixture: &FakeTurnGreenLatestFixture,
        result: &str,
    ) -> Value {
        let paths = turn_green_latest_paths(&fixture.wrapper_session_dir);
        let (verdict, gate_verdict, gate_reason) = match result {
            "completed" => (
                "tiny_live_package_launch_packet_latest_completed",
                Value::Null,
                Value::Null,
            ),
            "refused_now_by_stage3" => (
                "tiny_live_package_launch_packet_latest_refused_now_by_stage3",
                json!("blocked"),
                json!("stage3 blocked"),
            ),
            "refused_now_by_pre_activation_gate" => (
                "tiny_live_package_launch_packet_latest_refused_now_by_pre_activation_gate",
                json!("blocked"),
                json!("pre gate blocked"),
            ),
            "refused_by_missing_latest_chain" => (
                "tiny_live_package_launch_packet_latest_refused_by_missing_latest_chain",
                Value::Null,
                Value::Null,
            ),
            "refused_by_invalid_latest_chain" => (
                "tiny_live_package_launch_packet_latest_refused_by_invalid_latest_chain",
                Value::Null,
                Value::Null,
            ),
            _ => (
                "tiny_live_package_launch_packet_latest_refused_by_drifted_launch_contract",
                Value::Null,
                Value::Null,
            ),
        };
        json!({
            "generated_at": Utc::now(),
            "mode": "run_latest_launch_packet",
            "verdict": verdict,
            "reason": format!("latest launch packet {result}"),
            "latest_top_step_name": "clerestory_certificate",
            "latest_top_session_dir": fixture.latest_top_session_dir.display().to_string(),
            "package_dir": fixture.package_dir.display().to_string(),
            "install_root": fixture.install_root.display().to_string(),
            "target_service_name": "solana-copy-bot.service",
            "backend_command": fixture.backend_command.display().to_string(),
            "wrapper_timeout_ms": 1200,
            "service_status_max_staleness_ms": 4500,
            "session_dir": paths.nested_launch_packet_latest_session_dir.display().to_string(),
            "nested_launch_packet_session_dir": expected_downstream_launch_packet_session_dir(&paths).display().to_string(),
            "result": result,
            "current_pre_activation_gate_verdict": gate_verdict,
            "current_pre_activation_gate_reason": gate_reason,
            "reviewed_frozen_live_cutover_controller_command_summary": fixture.frozen_controller_command,
            "clerestory_certificate_summary": "latest clerestory summary",
            "clerestory_certificate_sha256": "latest-clerestory-sha",
            "clerestory_certificate_algorithm": "sha256",
            "explicit_statement": "latest launch packet wrapper run"
        })
    }

    fn default_launch_packet_latest_verify_report(
        fixture: &FakeTurnGreenLatestFixture,
        result: &str,
    ) -> Value {
        let mut report = default_launch_packet_latest_run_report(fixture, result);
        report["mode"] = json!("verify_latest_launch_packet");
        report["verdict"] = json!(LAUNCH_PACKET_LATEST_VERIFY_OK);
        report["reason"] = json!("verify ok");
        report
    }

    fn default_turn_green_session(fixture: &FakeTurnGreenLatestFixture) -> Value {
        let paths = turn_green_latest_paths(&fixture.wrapper_session_dir);
        let snapshot = LatestTurnGreenSnapshot {
            latest_top_step_name: "clerestory_certificate".to_string(),
            latest_top_session_dir: fixture.latest_top_session_dir.clone(),
            downstream_launch_packet_session_dir: expected_downstream_launch_packet_session_dir(
                &paths,
            ),
            package_dir: fixture.package_dir.clone(),
            install_root: fixture.install_root.clone(),
            target_service_name: "solana-copy-bot.service".to_string(),
            backend_command: fixture.backend_command.display().to_string(),
            wrapper_timeout_ms: 1200,
            service_status_max_staleness_ms: 4500,
            reviewed_frozen_live_cutover_controller_command_summary: fixture
                .frozen_controller_command
                .clone(),
            clerestory_certificate_summary: "latest clerestory summary".to_string(),
            clerestory_certificate_sha256: "latest-clerestory-sha".to_string(),
            clerestory_certificate_algorithm: "sha256".to_string(),
        };
        json!({
            "session_version": "1",
            "planned_at": Utc::now(),
            "launch_packet_session_dir": snapshot.downstream_launch_packet_session_dir.display().to_string(),
            "package_dir": snapshot.package_dir.display().to_string(),
            "install_root": snapshot.install_root.display().to_string(),
            "target_service_name": snapshot.target_service_name,
            "backend_command": snapshot.backend_command,
            "wrapper_timeout_ms": snapshot.wrapper_timeout_ms,
            "service_status_max_staleness_ms": snapshot.service_status_max_staleness_ms,
            "session_dir": turn_green_latest_paths(&fixture.wrapper_session_dir).nested_turn_green_session_dir.display().to_string(),
            "nested_current_authorization_session_dir": nested_turn_green_current_authorization_session_dir(&turn_green_latest_paths(&fixture.wrapper_session_dir).nested_turn_green_session_dir).display().to_string(),
            "verify_frozen_launch_packet_summary": turn_green_verify_frozen_launch_packet_summary(&snapshot.downstream_launch_packet_session_dir),
            "refresh_current_authorization_command_summary": turn_green_refresh_current_authorization_command_summary(&snapshot, &nested_turn_green_current_authorization_session_dir(&turn_green_latest_paths(&fixture.wrapper_session_dir).nested_turn_green_session_dir)),
            "frozen_live_cutover_controller_command_summary": fixture.frozen_controller_command,
            "explicit_statement": "the immutable launch packet is the primary handoff input here; current host truth may only refuse or mark that frozen controller executable now"
        })
    }

    fn default_turn_green_status(fixture: &FakeTurnGreenLatestFixture, result: &str) -> Value {
        let paths = turn_green_latest_paths(&fixture.wrapper_session_dir);
        let (reason, gate_verdict, gate_reason, executable_now, operator_next_action_summary) =
            match result {
                "executable_now" => (
                    "the frozen launch packet still matches the current live host contract and the refreshed gate truth is green; the frozen controller is executable now if the operator explicitly chooses to run it",
                    json!("green"),
                    json!("ok"),
                    true,
                    json!(format!(
                        "the frozen controller is executable now if the operator explicitly chooses to run this exact command: {}",
                        fixture.frozen_controller_command
                    )),
                ),
                "refused_now_by_stage3" => (
                    "the frozen launch packet is still coherent, but the frozen controller remains refused now because current Stage 3 is non-green",
                    json!("blocked"),
                    json!("stage3 blocked"),
                    false,
                    json!("the frozen controller stays refused now by Stage 3; rerun this turn-green refresh after current Stage 3 truth turns green"),
                ),
                _ => (
                    "the frozen launch packet is still coherent, but the frozen controller remains refused now because the current pre-activation gate is non-green",
                    json!("blocked"),
                    json!("pre gate blocked"),
                    false,
                    json!("the frozen controller stays refused now by the pre-activation gate; rerun this turn-green refresh after the current pre-activation gate turns green"),
                ),
            };
        json!({
            "status_version": "1",
            "updated_at": Utc::now(),
            "session_dir": paths.nested_turn_green_session_dir.display().to_string(),
            "launch_packet_session_dir": expected_downstream_launch_packet_session_dir(&paths).display().to_string(),
            "nested_current_authorization_session_dir": nested_turn_green_current_authorization_session_dir(&paths.nested_turn_green_session_dir).display().to_string(),
            "result": result,
            "reason": reason,
            "frozen_launch_packet_result": "eligible_when_gate_turns_green",
            "current_authorization_result_now": if result == "executable_now" { json!("authorized_now") } else if result == "refused_now_by_stage3" { json!("refused_by_stage3") } else { json!("refused_by_pre_activation_gate") },
            "frozen_controller_still_current": true,
            "current_pre_activation_gate_verdict": gate_verdict,
            "current_pre_activation_gate_reason": gate_reason,
            "operator_next_action_summary": operator_next_action_summary,
            "executable_now": executable_now,
            "explicit_statement": "this packet-native turn-green refresh never runs the frozen live cutover controller; it only classifies whether that frozen controller is executable now under current truth"
        })
    }

    fn default_turn_green_run_report(fixture: &FakeTurnGreenLatestFixture, result: &str) -> Value {
        let paths = turn_green_latest_paths(&fixture.wrapper_session_dir);
        let status = default_turn_green_status(fixture, result);
        let verdict = match result {
            "executable_now" => "tiny_live_package_turn_green_executable_now",
            "refused_now_by_stage3" => "tiny_live_package_turn_green_refused_now_by_stage3",
            _ => "tiny_live_package_turn_green_refused_now_by_pre_activation_gate",
        };
        json!({
            "generated_at": Utc::now(),
            "mode": "run_live_package_turn_green",
            "verdict": verdict,
            "reason": status["reason"],
            "launch_packet_session_dir": expected_downstream_launch_packet_session_dir(&paths).display().to_string(),
            "package_dir": fixture.package_dir.display().to_string(),
            "install_root": fixture.install_root.display().to_string(),
            "target_service_name": "solana-copy-bot.service",
            "backend_command": fixture.backend_command.display().to_string(),
            "wrapper_timeout_ms": 1200,
            "service_status_max_staleness_ms": 4500,
            "session_dir": paths.nested_turn_green_session_dir.display().to_string(),
            "turn_green_session_path": nested_turn_green_session_path(&paths.nested_turn_green_session_dir).display().to_string(),
            "turn_green_status_path": nested_turn_green_status_path(&paths.nested_turn_green_session_dir).display().to_string(),
            "nested_current_authorization_session_dir": nested_turn_green_current_authorization_session_dir(&paths.nested_turn_green_session_dir).display().to_string(),
            "result": result,
            "frozen_launch_packet_result": status["frozen_launch_packet_result"],
            "current_authorization_result_now": status["current_authorization_result_now"],
            "frozen_controller_still_current": true,
            "verify_frozen_launch_packet_summary": turn_green_verify_frozen_launch_packet_summary(&expected_downstream_launch_packet_session_dir(&paths)),
            "refresh_current_authorization_command_summary": turn_green_refresh_current_authorization_command_summary(
                &LatestTurnGreenSnapshot {
                    latest_top_step_name: "clerestory_certificate".to_string(),
                    latest_top_session_dir: fixture.latest_top_session_dir.clone(),
                    downstream_launch_packet_session_dir: expected_downstream_launch_packet_session_dir(&paths),
                    package_dir: fixture.package_dir.clone(),
                    install_root: fixture.install_root.clone(),
                    target_service_name: "solana-copy-bot.service".to_string(),
                    backend_command: fixture.backend_command.display().to_string(),
                    wrapper_timeout_ms: 1200,
                    service_status_max_staleness_ms: 4500,
                    reviewed_frozen_live_cutover_controller_command_summary: fixture.frozen_controller_command.clone(),
                    clerestory_certificate_summary: "latest clerestory summary".to_string(),
                    clerestory_certificate_sha256: "latest-clerestory-sha".to_string(),
                    clerestory_certificate_algorithm: "sha256".to_string(),
                },
                &nested_turn_green_current_authorization_session_dir(&paths.nested_turn_green_session_dir),
            ),
            "frozen_live_cutover_controller_command_summary": fixture.frozen_controller_command,
            "current_live_cutover_controller_command_summary": fixture.frozen_controller_command,
            "current_pre_activation_gate_verdict": status["current_pre_activation_gate_verdict"],
            "current_pre_activation_gate_reason": status["current_pre_activation_gate_reason"],
            "operator_next_action_summary": status["operator_next_action_summary"],
            "executable_now": status["executable_now"],
            "explicit_statement": status["explicit_statement"]
        })
    }

    fn default_turn_green_verify_report(
        fixture: &FakeTurnGreenLatestFixture,
        result: &str,
    ) -> Value {
        let mut report = default_turn_green_run_report(fixture, result);
        report["mode"] = json!("verify_live_package_turn_green");
        report["verdict"] = json!(TURN_GREEN_VERIFY_OK);
        report["reason"] = json!("verify ok");
        report["explicit_statement"] = json!("this verification proves only whether the frozen controller is executable now; it never runs that controller");
        report
    }

    fn write_fake_launch_packet_latest_command(
        path: &Path,
        plan_json_path: &Path,
        run_json_path: &Path,
        verify_json_path: &Path,
        root: &Path,
        session_dir: &Path,
    ) {
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --root {} \"*) : ;;\n  *\" --verify-latest-launch-packet \"*) : ;;\n  *) echo 'unexpected root' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --session-dir {} \"*) : ;;\n  *) echo 'unexpected latest-launch-packet session-dir' >&2; exit 1;;\nesac\nmode=''\ncase \" $* \" in\n  *\" --plan-latest-launch-packet \"*) mode='plan';;\n  *\" --run-latest-launch-packet \"*) mode='run';;\n  *\" --verify-latest-launch-packet \"*) mode='verify';;\n  *) echo 'unexpected latest-launch-packet mode' >&2; exit 1;;\nesac\ncase \"$mode\" in\n  plan) cat '{}';;\n  run) mkdir -p '{}'; cp '{}' '{}/tiny_live_activation_package_launch_packet_latest.report.json'; cat '{}';;\n  verify) cat '{}';;\n  *) exit 1;;\nesac\n",
            root.display(),
            session_dir.display(),
            plan_json_path.display(),
            session_dir.display(),
            run_json_path.display(),
            session_dir.display(),
            run_json_path.display(),
            verify_json_path.display(),
        );
        fs::write(path, script).unwrap();
        let mut perms = fs::metadata(path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).unwrap();
    }

    fn write_fake_turn_green_command(
        path: &Path,
        run_json_path: &Path,
        verify_json_path: &Path,
        session_json_path: &Path,
        status_json_path: &Path,
        launch_packet_session_dir: &Path,
        session_dir: &Path,
    ) {
        let nested_session_path = nested_turn_green_session_path(session_dir);
        let nested_status_path = nested_turn_green_status_path(session_dir);
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --launch-packet-session-dir {} \"*) : ;;\n  *) echo 'unexpected turn-green launch-packet-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --session-dir {} \"*) : ;;\n  *) echo 'unexpected turn-green session-dir' >&2; exit 1;;\nesac\nmode=''\ncase \" $* \" in\n  *\" --run-live-package-turn-green \"*) mode='run';;\n  *\" --verify-live-package-turn-green \"*) mode='verify';;\n  *) echo 'unexpected turn-green mode' >&2; exit 1;;\nesac\ncase \"$mode\" in\n  run) mkdir -p '{}'; cp '{}' '{}'; cp '{}' '{}'; cat '{}';;\n  verify) cat '{}';;\n  *) exit 1;;\nesac\n",
            launch_packet_session_dir.display(),
            session_dir.display(),
            session_dir.display(),
            session_json_path.display(),
            nested_session_path.display(),
            status_json_path.display(),
            nested_status_path.display(),
            run_json_path.display(),
            verify_json_path.display(),
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
            .unwrap_or(Duration::from_nanos(0))
            .as_nanos();
        let path = std::env::temp_dir().join(format!("{label}.{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }
}
