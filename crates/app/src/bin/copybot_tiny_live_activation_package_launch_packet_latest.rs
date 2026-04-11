use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const USAGE: &str = "usage: copybot_tiny_live_activation_package_launch_packet_latest --root <path> [--session-dir <path>] [--output <path>] [--json] (--plan-latest-launch-packet | --render-latest-launch-packet-script | --run-latest-launch-packet | --verify-latest-launch-packet)";
const LAUNCH_PACKET_LATEST_SESSION_VERSION: &str = "1";
const LAUNCH_PACKET_LATEST_STATUS_VERSION: &str = "1";
const AUTHORIZE_LATEST_BIN: &str = "copybot_tiny_live_activation_package_authorize_latest";
const LAUNCH_PACKET_BIN: &str = "copybot_tiny_live_activation_package_launch_packet";
const AUTHORIZE_LATEST_PLAN_READY: &str = "tiny_live_package_authorize_latest_plan_ready";
const AUTHORIZE_LATEST_VERIFY_OK: &str = "tiny_live_package_authorize_latest_verify_ok";
const LAUNCH_PACKET_PLAN_READY: &str = "tiny_live_package_launch_packet_plan_ready";
const LAUNCH_PACKET_VERIFY_OK: &str = "tiny_live_package_launch_packet_verify_ok";
const PLANNING_SAFE_STATEMENT: &str =
    "this latest-launch-packet handoff binds the latest persisted immutable package chain to the accepted launch-packet contract, but it never overrides Stage 3 or pre-activation refusal truth and it never executes live cutover by itself";

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
    PlanLatestLaunchPacket,
    RenderLatestLaunchPacketScript,
    RunLatestLaunchPacket,
    VerifyLatestLaunchPacket,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageLaunchPacketLatestVerdict {
    TinyLivePackageLaunchPacketLatestPlanReady,
    TinyLivePackageLaunchPacketLatestScriptRendered,
    TinyLivePackageLaunchPacketLatestRefusedByMissingLatestChain,
    TinyLivePackageLaunchPacketLatestRefusedByInvalidLatestChain,
    TinyLivePackageLaunchPacketLatestRefusedByDriftedLaunchContract,
    TinyLivePackageLaunchPacketLatestRefusedNowByStage3,
    TinyLivePackageLaunchPacketLatestRefusedNowByPreActivationGate,
    TinyLivePackageLaunchPacketLatestCompleted,
    TinyLivePackageLaunchPacketLatestVerifyOk,
    TinyLivePackageLaunchPacketLatestVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LivePackageLaunchPacketLatestResult {
    RefusedByMissingLatestChain,
    RefusedByInvalidLatestChain,
    RefusedByDriftedLaunchContract,
    RefusedNowByStage3,
    RefusedNowByPreActivationGate,
    Completed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageLaunchPacketLatestStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageLaunchPacketLatestSession {
    session_version: String,
    planned_at: DateTime<Utc>,
    root: String,
    session_dir: String,
    nested_authorize_latest_session_dir: String,
    nested_launch_packet_session_dir: String,
    latest_top_step_name: Option<String>,
    latest_top_session_dir: Option<String>,
    historical_launch_packet_session_dir: Option<String>,
    package_dir: Option<String>,
    install_root: Option<String>,
    target_service_name: Option<String>,
    backend_command: Option<String>,
    wrapper_timeout_ms: Option<u64>,
    service_status_max_staleness_ms: Option<u64>,
    resolve_latest_authorization_command_summary: String,
    run_latest_authorization_command_summary: String,
    verify_latest_authorization_command_summary: String,
    plan_launch_packet_command_summary: String,
    downstream_launch_packet_command_summary: String,
    verify_launch_packet_command_summary: String,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    clerestory_certificate_summary: Option<String>,
    clerestory_certificate_sha256: Option<String>,
    clerestory_certificate_algorithm: Option<String>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageLaunchPacketLatestStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    root: String,
    session_dir: String,
    latest_top_step_name: Option<String>,
    latest_top_session_dir: Option<String>,
    historical_launch_packet_session_dir: Option<String>,
    nested_authorize_latest_session_dir: String,
    nested_launch_packet_session_dir: String,
    result: LivePackageLaunchPacketLatestResult,
    reason: String,
    latest_authorization_plan_verdict: Option<String>,
    latest_authorization_verdict: Option<String>,
    launch_packet_plan_verdict: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    latest_authorization_plan_step: Option<PackageLaunchPacketLatestStepArtifact>,
    latest_authorization_step: Option<PackageLaunchPacketLatestStepArtifact>,
    launch_packet_plan_step: Option<PackageLaunchPacketLatestStepArtifact>,
    launch_packet_step: Option<PackageLaunchPacketLatestStepArtifact>,
    operator_next_action_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackageLaunchPacketLatestPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    report_path: PathBuf,
    latest_authorization_plan_report_path: PathBuf,
    latest_authorization_report_path: PathBuf,
    launch_packet_plan_report_path: PathBuf,
    launch_packet_report_path: PathBuf,
    nested_authorize_latest_session_dir: PathBuf,
    nested_launch_packet_session_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageLaunchPacketLatestReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageLaunchPacketLatestVerdict,
    reason: String,
    root: Option<String>,
    latest_top_step_name: Option<String>,
    latest_top_session_dir: Option<String>,
    historical_launch_packet_session_dir: Option<String>,
    package_dir: Option<String>,
    install_root: Option<String>,
    target_service_name: Option<String>,
    backend_command: Option<String>,
    wrapper_timeout_ms: Option<u64>,
    service_status_max_staleness_ms: Option<u64>,
    session_dir: Option<String>,
    launch_packet_latest_session_path: Option<String>,
    launch_packet_latest_status_path: Option<String>,
    launch_packet_latest_report_path: Option<String>,
    latest_authorization_plan_report_path: Option<String>,
    latest_authorization_report_path: Option<String>,
    launch_packet_plan_report_path: Option<String>,
    launch_packet_report_path: Option<String>,
    nested_authorize_latest_session_dir: Option<String>,
    nested_launch_packet_session_dir: Option<String>,
    latest_authorization_plan_verdict: Option<String>,
    latest_authorization_verdict: Option<String>,
    launch_packet_plan_verdict: Option<String>,
    result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    resolve_latest_authorization_command_summary: Option<String>,
    run_latest_authorization_command_summary: Option<String>,
    verify_latest_authorization_command_summary: Option<String>,
    plan_launch_packet_command_summary: Option<String>,
    downstream_launch_packet_command_summary: Option<String>,
    verify_launch_packet_command_summary: Option<String>,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    clerestory_certificate_summary: Option<String>,
    clerestory_certificate_sha256: Option<String>,
    clerestory_certificate_algorithm: Option<String>,
    verification_mismatches: Vec<String>,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct LatestLaunchPacketSnapshot {
    latest_top_step_name: String,
    latest_top_session_dir: PathBuf,
    historical_launch_packet_session_dir: PathBuf,
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
struct ResolvedLatestLaunchPacket {
    snapshot: LatestLaunchPacketSnapshot,
    latest_authorization_plan_raw: Value,
    latest_authorization_plan: AuthorizeLatestReportView,
    launch_packet_plan_raw: Value,
    launch_packet_plan: LaunchPacketReportView,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResolutionFailureKind {
    MissingLatestChain,
    InvalidLatestChain,
    DriftedLaunchContract,
}

#[derive(Debug, Clone)]
struct LatestLaunchPacketResolutionFailure {
    kind: ResolutionFailureKind,
    reason: String,
    snapshot: Option<LatestLaunchPacketSnapshot>,
    latest_authorization_plan_raw: Option<Value>,
    latest_authorization_plan_verdict: Option<String>,
    latest_authorization_plan_reason: Option<String>,
    latest_authorization_plan_generated_at: Option<DateTime<Utc>>,
    launch_packet_plan_raw: Option<Value>,
    launch_packet_plan_verdict: Option<String>,
    launch_packet_plan_reason: Option<String>,
    launch_packet_plan_generated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Deserialize)]
struct AuthorizeLatestReportView {
    generated_at: DateTime<Utc>,
    verdict: String,
    reason: String,
    latest_top_step_name: Option<String>,
    latest_top_session_dir: Option<String>,
    launch_packet_session_dir: Option<String>,
    package_dir: Option<String>,
    install_root: Option<String>,
    target_service_name: Option<String>,
    backend_command: Option<String>,
    wrapper_timeout_ms: Option<u64>,
    service_status_max_staleness_ms: Option<u64>,
    session_dir: Option<String>,
    #[allow(dead_code)]
    nested_live_authorization_session_dir: Option<String>,
    result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    clerestory_certificate_summary: Option<String>,
    clerestory_certificate_sha256: Option<String>,
    clerestory_certificate_algorithm: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct LaunchPacketReportView {
    generated_at: DateTime<Utc>,
    verdict: String,
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
    run_live_authorization_command_summary: String,
    frozen_live_cutover_controller_command_summary: String,
    operator_next_action_summary: String,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
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
            "--plan-latest-launch-packet" => {
                set_mode(
                    &mut mode,
                    Mode::PlanLatestLaunchPacket,
                    "--plan-latest-launch-packet",
                )?;
            }
            "--render-latest-launch-packet-script" => {
                set_mode(
                    &mut mode,
                    Mode::RenderLatestLaunchPacketScript,
                    "--render-latest-launch-packet-script",
                )?;
            }
            "--run-latest-launch-packet" => {
                set_mode(
                    &mut mode,
                    Mode::RunLatestLaunchPacket,
                    "--run-latest-launch-packet",
                )?;
            }
            "--verify-latest-launch-packet" => {
                set_mode(
                    &mut mode,
                    Mode::VerifyLatestLaunchPacket,
                    "--verify-latest-launch-packet",
                )?;
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
        Mode::PlanLatestLaunchPacket
            | Mode::RenderLatestLaunchPacketScript
            | Mode::RunLatestLaunchPacket
    ) && root.is_none()
    {
        bail!("missing --root");
    }
    if matches!(mode, Mode::RenderLatestLaunchPacketScript) && output_path.is_none() {
        bail!("missing --output for render mode");
    }
    if matches!(
        mode,
        Mode::RunLatestLaunchPacket | Mode::VerifyLatestLaunchPacket
    ) && session_dir.is_none()
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
        Mode::PlanLatestLaunchPacket => plan_latest_launch_packet_report(&config)?,
        Mode::RenderLatestLaunchPacketScript => render_latest_launch_packet_script_report(&config)?,
        Mode::RunLatestLaunchPacket => run_latest_launch_packet_report(&config)?,
        Mode::VerifyLatestLaunchPacket => verify_latest_launch_packet_report(&config)?,
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_report_lines(&report))
    }
}

fn plan_latest_launch_packet_report(config: &Config) -> Result<PackageLaunchPacketLatestReport> {
    let root = config
        .root
        .as_ref()
        .ok_or_else(|| anyhow!("missing --root"))?;
    let suggested_session_dir = config
        .session_dir
        .clone()
        .unwrap_or_else(|| root.join("tiny_live_activation_package_launch_packet_latest.session"));
    let paths = launch_packet_latest_paths(&suggested_session_dir);
    match resolve_latest_launch_packet(root, &paths) {
        Ok(resolved) => Ok(plan_report_from_resolution(
            "plan_latest_launch_packet",
            TinyLivePackageLaunchPacketLatestVerdict::TinyLivePackageLaunchPacketLatestPlanReady,
            format!(
                "latest immutable {} chain under {} resolves to exact latest-authorization {} lineage and the accepted launch-packet contract remains current",
                resolved.snapshot.latest_top_step_name,
                root.display(),
                paths.nested_authorize_latest_session_dir.display(),
            ),
            root,
            &suggested_session_dir,
            &paths,
            &resolved,
        )),
        Err(failure) => Ok(report_from_resolution_failure(
            "plan_latest_launch_packet",
            root,
            &suggested_session_dir,
            &paths,
            &failure,
        )),
    }
}

fn render_latest_launch_packet_script_report(
    config: &Config,
) -> Result<PackageLaunchPacketLatestReport> {
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
    let paths = launch_packet_latest_paths(&suggested_session_dir);
    match resolve_latest_launch_packet(root, &paths) {
        Ok(resolved) => {
            write_script(
                output_path,
                &format!(
                    "#!/usr/bin/env bash\nset -euo pipefail\n\n{plan}\n{run}\n{verify}\n",
                    plan = plan_launch_packet_command_summary(
                        &resolved.snapshot,
                        &paths.nested_launch_packet_session_dir,
                    ),
                    run = downstream_launch_packet_command_summary(
                        &resolved.snapshot,
                        &paths.nested_launch_packet_session_dir,
                    ),
                    verify = verify_launch_packet_command_summary(
                        &resolved.snapshot,
                        &paths.nested_launch_packet_session_dir,
                    ),
                ),
            )?;
            Ok(plan_report_from_resolution(
                "render_latest_launch_packet_script",
                TinyLivePackageLaunchPacketLatestVerdict::TinyLivePackageLaunchPacketLatestScriptRendered,
                format!(
                    "rendered latest-launch-packet handoff script to {} using the accepted launch-packet contract",
                    output_path.display()
                ),
                root,
                &suggested_session_dir,
                &paths,
                &resolved,
            ))
        }
        Err(failure) => Ok(report_from_resolution_failure(
            "render_latest_launch_packet_script",
            root,
            &suggested_session_dir,
            &paths,
            &failure,
        )),
    }
}

fn run_latest_launch_packet_report(config: &Config) -> Result<PackageLaunchPacketLatestReport> {
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
            "refusing to overwrite existing latest-launch-packet session dir {}",
            session_dir.display()
        );
    }
    let paths = launch_packet_latest_paths(session_dir);
    create_clean_session_dir(session_dir, "latest-launch-packet")?;
    let resolution = resolve_latest_launch_packet(root, &paths);
    match resolution {
        Ok(resolved) => {
            run_latest_launch_packet_with_resolution(root, session_dir, &paths, &resolved)
        }
        Err(failure) => run_latest_launch_packet_failure(root, session_dir, &paths, &failure),
    }
}

fn verify_latest_launch_packet_report(config: &Config) -> Result<PackageLaunchPacketLatestReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for verify mode"))?;
    let paths = launch_packet_latest_paths(session_dir);
    let session: PackageLaunchPacketLatestSession = load_json(&paths.session_path)?;
    let status: PackageLaunchPacketLatestStatus = load_json(&paths.status_path)?;
    let stored_report: PackageLaunchPacketLatestReport = load_json(&paths.report_path)?;
    let root = PathBuf::from(&session.root);
    let mut mismatches = Vec::new();
    let stored_report_verdict = serialize_enum(&stored_report.verdict);
    let expected_report_verdict = serialize_enum(&verdict_for_result(status.result));

    compare_string(
        &session.root,
        &root.display().to_string(),
        "latest-launch-packet session root",
        &mut mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "latest-launch-packet session session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.nested_authorize_latest_session_dir,
        &paths
            .nested_authorize_latest_session_dir
            .display()
            .to_string(),
        "latest-launch-packet session nested_authorize_latest_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.nested_launch_packet_session_dir,
        &paths.nested_launch_packet_session_dir.display().to_string(),
        "latest-launch-packet session nested_launch_packet_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "stored session explicit_statement",
        &mut mismatches,
    );
    compare_string(
        &status.root,
        &root.display().to_string(),
        "latest-launch-packet status root",
        &mut mismatches,
    );
    compare_string(
        &status.session_dir,
        &session_dir.display().to_string(),
        "latest-launch-packet status session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.nested_authorize_latest_session_dir,
        &paths
            .nested_authorize_latest_session_dir
            .display()
            .to_string(),
        "latest-launch-packet status nested_authorize_latest_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.nested_launch_packet_session_dir,
        &paths.nested_launch_packet_session_dir.display().to_string(),
        "latest-launch-packet status nested_launch_packet_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "stored status explicit_statement",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.root.as_deref(),
        Some(root.display().to_string().as_str()),
        "latest-launch-packet report root",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "latest-launch-packet report session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.nested_authorize_latest_session_dir.as_deref(),
        Some(
            paths
                .nested_authorize_latest_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "latest-launch-packet report nested_authorize_latest_session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.nested_launch_packet_session_dir.as_deref(),
        Some(
            paths
                .nested_launch_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "latest-launch-packet report nested_launch_packet_session_dir",
        &mut mismatches,
    );
    compare_string(
        &stored_report.mode,
        "run_latest_launch_packet",
        "stored report mode",
        &mut mismatches,
    );
    compare_string(
        &stored_report_verdict,
        &expected_report_verdict,
        "stored report verdict",
        &mut mismatches,
    );
    compare_string(
        &stored_report.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "stored report explicit_statement",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.latest_authorization_plan_verdict.as_deref(),
        status.latest_authorization_plan_verdict.as_deref(),
        "stored report latest_authorization_plan_verdict",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.latest_authorization_verdict.as_deref(),
        status.latest_authorization_verdict.as_deref(),
        "stored report latest_authorization_verdict",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.launch_packet_plan_verdict.as_deref(),
        status.launch_packet_plan_verdict.as_deref(),
        "stored report launch_packet_plan_verdict",
        &mut mismatches,
    );

    let resolution = resolve_latest_launch_packet(&root, &paths);
    match resolution.as_ref() {
        Ok(resolved) => {
            compare_session_against_snapshot(
                &session,
                root.as_path(),
                session_dir,
                &paths,
                &resolved.snapshot,
                &mut mismatches,
            );
            compare_report_against_snapshot(
                &stored_report,
                root.as_path(),
                session_dir,
                &paths,
                &resolved.snapshot,
                &mut mismatches,
            );

            let stored_authorization_plan: AuthorizeLatestReportView = load_required_step_json(
                &status.latest_authorization_plan_step,
                &paths.latest_authorization_plan_report_path,
                "latest_authorization_plan_step",
                &mut mismatches,
            )?;
            compare_step_artifact_against_report_metadata(
                status.latest_authorization_plan_step.as_ref(),
                &paths.latest_authorization_plan_report_path,
                "plan_latest_authorization",
                &stored_authorization_plan.verdict,
                &stored_authorization_plan.reason,
                stored_authorization_plan.generated_at,
                "stored latest_authorization_plan_step",
                &mut mismatches,
            );
            compare_authorize_latest_plan_against_snapshot(
                &stored_authorization_plan,
                &resolved.snapshot,
                &paths.nested_authorize_latest_session_dir,
                &mut mismatches,
            );
            compare_option_string(
                status.latest_authorization_plan_verdict.as_deref(),
                Some(stored_authorization_plan.verdict.as_str()),
                "stored status latest_authorization_plan_verdict",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.latest_authorization_plan_verdict.as_deref(),
                Some(stored_authorization_plan.verdict.as_str()),
                "stored report latest_authorization_plan_verdict",
                &mut mismatches,
            );

            let stored_launch_packet_plan: LaunchPacketReportView = load_required_step_json(
                &status.launch_packet_plan_step,
                &paths.launch_packet_plan_report_path,
                "launch_packet_plan_step",
                &mut mismatches,
            )?;
            compare_step_artifact_against_report_metadata(
                status.launch_packet_plan_step.as_ref(),
                &paths.launch_packet_plan_report_path,
                "plan_live_package_launch_packet",
                &stored_launch_packet_plan.verdict,
                &stored_launch_packet_plan.reason,
                stored_launch_packet_plan.generated_at,
                "stored launch_packet_plan_step",
                &mut mismatches,
            );
            compare_launch_packet_plan_against_snapshot(
                &stored_launch_packet_plan,
                &resolved.snapshot,
                &paths.nested_launch_packet_session_dir,
                &mut mismatches,
            );
            compare_option_string(
                status.launch_packet_plan_verdict.as_deref(),
                Some(stored_launch_packet_plan.verdict.as_str()),
                "stored status launch_packet_plan_verdict",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.launch_packet_plan_verdict.as_deref(),
                Some(stored_launch_packet_plan.verdict.as_str()),
                "stored report launch_packet_plan_verdict",
                &mut mismatches,
            );

            let stored_authorization_run: AuthorizeLatestReportView = load_required_step_json(
                &status.latest_authorization_step,
                &paths.latest_authorization_report_path,
                "latest_authorization_step",
                &mut mismatches,
            )?;
            compare_step_artifact_against_report_metadata(
                status.latest_authorization_step.as_ref(),
                &paths.latest_authorization_report_path,
                "run_latest_authorization",
                &stored_authorization_run.verdict,
                &stored_authorization_run.reason,
                stored_authorization_run.generated_at,
                "stored latest_authorization_step",
                &mut mismatches,
            );
            compare_authorize_latest_run_against_snapshot(
                &stored_authorization_run,
                &resolved.snapshot,
                &paths.nested_authorize_latest_session_dir,
                &mut mismatches,
            );
            compare_option_string(
                status.latest_authorization_verdict.as_deref(),
                Some(stored_authorization_run.verdict.as_str()),
                "stored status latest_authorization_verdict",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.latest_authorization_verdict.as_deref(),
                Some(stored_authorization_run.verdict.as_str()),
                "stored report latest_authorization_verdict",
                &mut mismatches,
            );
            let actual_authorization_run: Option<AuthorizeLatestReportView> = match load_json(
                &authorize_latest_nested_report_path(&paths.nested_authorize_latest_session_dir),
            ) {
                Ok(value) => Some(value),
                Err(error) => {
                    mismatches.push(format!(
                        "failed loading actual nested latest-authorization report under {}: {error}",
                        paths.nested_authorize_latest_session_dir.display()
                    ));
                    None
                }
            };
            if let Some(actual_authorization_run) = actual_authorization_run.as_ref() {
                compare_authorize_latest_copy_matches_actual(
                    &stored_authorization_run,
                    actual_authorization_run,
                    &mut mismatches,
                );
            }
            let verified_authorization_raw = run_json_command(
                AUTHORIZE_LATEST_BIN,
                &authorize_latest_verify_args(&paths.nested_authorize_latest_session_dir),
            )?;
            let verified_authorization: AuthorizeLatestReportView =
                serde_json::from_value(verified_authorization_raw)
                    .context("failed parsing latest-authorization verify json")?;
            if verified_authorization.verdict != AUTHORIZE_LATEST_VERIFY_OK {
                mismatches.push(format!(
                    "nested latest-authorization verification is non-green: {}",
                    verified_authorization.reason
                ));
            }

            let stored_launch_packet_run: LaunchPacketReportView = load_required_step_json(
                &status.launch_packet_step,
                &paths.launch_packet_report_path,
                "launch_packet_step",
                &mut mismatches,
            )?;
            compare_step_artifact_against_report_metadata(
                status.launch_packet_step.as_ref(),
                &paths.launch_packet_report_path,
                "run_live_package_launch_packet",
                &stored_launch_packet_run.verdict,
                &stored_launch_packet_run.reason,
                stored_launch_packet_run.generated_at,
                "stored launch_packet_step",
                &mut mismatches,
            );
            compare_launch_packet_run_against_snapshot(
                &stored_launch_packet_run,
                &resolved.snapshot,
                &paths.nested_launch_packet_session_dir,
                &mut mismatches,
            );
            let actual_launch_packet_run: Option<LaunchPacketReportView> = match load_json(
                &nested_launch_packet_report_path(&paths.nested_launch_packet_session_dir),
            ) {
                Ok(value) => Some(value),
                Err(error) => {
                    mismatches.push(format!(
                        "failed loading actual nested launch-packet report under {}: {error}",
                        paths.nested_launch_packet_session_dir.display()
                    ));
                    None
                }
            };
            if let Some(actual_launch_packet_run) = actual_launch_packet_run.as_ref() {
                compare_launch_packet_copy_matches_actual(
                    &stored_launch_packet_run,
                    actual_launch_packet_run,
                    &mut mismatches,
                );
            }
            let verified_launch_packet_raw = run_json_command(
                LAUNCH_PACKET_BIN,
                &launch_packet_args(
                    &resolved.snapshot,
                    &paths.nested_launch_packet_session_dir,
                    "--verify-live-package-launch-packet",
                ),
            )?;
            let verified_launch_packet: LaunchPacketReportView =
                serde_json::from_value(verified_launch_packet_raw)
                    .context("failed parsing launch-packet verify json")?;
            if verified_launch_packet.verdict != LAUNCH_PACKET_VERIFY_OK {
                mismatches.push(format!(
                    "nested launch-packet verification is non-green: {}",
                    verified_launch_packet.reason
                ));
            }

            let expected_result = match launch_packet_latest_result_from_launch_packet(
                verified_launch_packet.result.as_deref(),
            ) {
                Some(value) => value,
                None => {
                    mismatches
                        .push("verified launch-packet result is missing or incoherent".to_string());
                    LivePackageLaunchPacketLatestResult::RefusedByDriftedLaunchContract
                }
            };
            if status.result != expected_result {
                mismatches.push(format!(
                    "stored latest-launch-packet result {} does not match verified nested launch-packet result {}",
                    serialize_enum(&status.result),
                    serialize_enum(&expected_result)
                ));
            }
            let expected_operator_next_action_summary = operator_next_action_summary(
                expected_result,
                Some(
                    &resolved
                        .snapshot
                        .reviewed_frozen_live_cutover_controller_command_summary,
                ),
            );
            compare_string(
                &status.operator_next_action_summary,
                &expected_operator_next_action_summary,
                "stored status operator_next_action_summary",
                &mut mismatches,
            );
            compare_string(
                &stored_report.reason,
                &status.reason,
                "stored report reason",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.result.as_deref(),
                Some(serialize_enum(&status.result).as_str()),
                "stored report result",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.current_pre_activation_gate_verdict.as_deref(),
                verified_launch_packet
                    .current_pre_activation_gate_verdict
                    .as_deref(),
                "stored report current_pre_activation_gate_verdict",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.current_pre_activation_gate_reason.as_deref(),
                verified_launch_packet
                    .current_pre_activation_gate_reason
                    .as_deref(),
                "stored report current_pre_activation_gate_reason",
                &mut mismatches,
            );
            compare_string(
                &status.reason,
                &stored_launch_packet_run.reason,
                "stored status reason",
                &mut mismatches,
            );
        }
        Err(failure) => {
            compare_session_against_failure(
                &session,
                root.as_path(),
                session_dir,
                &paths,
                failure,
                &mut mismatches,
            );
            compare_report_against_failure(
                &stored_report,
                root.as_path(),
                session_dir,
                &paths,
                failure,
                &mut mismatches,
            );
            let expected_result = result_from_failure_kind(failure.kind);
            if status.result != expected_result {
                mismatches.push(format!(
                    "stored latest-launch-packet failure result {} does not match current failure {}",
                    serialize_enum(&status.result),
                    serialize_enum(&expected_result)
                ));
            }
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
            compare_option_string(
                status.latest_authorization_plan_verdict.as_deref(),
                failure.latest_authorization_plan_verdict.as_deref(),
                "stored status latest_authorization_plan_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.latest_authorization_plan_verdict.as_deref(),
                failure.latest_authorization_plan_verdict.as_deref(),
                "stored report latest_authorization_plan_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                status.latest_authorization_verdict.as_deref(),
                None,
                "stored status latest_authorization_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                status.launch_packet_plan_verdict.as_deref(),
                failure.launch_packet_plan_verdict.as_deref(),
                "stored status launch_packet_plan_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.latest_authorization_verdict.as_deref(),
                None,
                "stored report latest_authorization_verdict on failure",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.launch_packet_plan_verdict.as_deref(),
                failure.launch_packet_plan_verdict.as_deref(),
                "stored report launch_packet_plan_verdict on failure",
                &mut mismatches,
            );
            let expected_operator = operator_next_action_summary(expected_result, None);
            compare_string(
                &status.operator_next_action_summary,
                &expected_operator,
                "stored status operator_next_action_summary on failure",
                &mut mismatches,
            );
        }
    }

    let verdict = if mismatches.is_empty() {
        TinyLivePackageLaunchPacketLatestVerdict::TinyLivePackageLaunchPacketLatestVerifyOk
    } else {
        TinyLivePackageLaunchPacketLatestVerdict::TinyLivePackageLaunchPacketLatestVerifyInvalid
    };
    let reason = if mismatches.is_empty() {
        format!(
            "latest-launch-packet session under {} is coherent with the current latest immutable chain and the accepted launch-packet contract",
            session_dir.display()
        )
    } else {
        mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "latest-launch-packet verification failed".to_string())
    };
    Ok(PackageLaunchPacketLatestReport {
        generated_at: Utc::now(),
        mode: "verify_latest_launch_packet".to_string(),
        verdict,
        reason,
        root: Some(root.display().to_string()),
        latest_top_step_name: session.latest_top_step_name.clone(),
        latest_top_session_dir: session.latest_top_session_dir.clone(),
        historical_launch_packet_session_dir: session.historical_launch_packet_session_dir.clone(),
        package_dir: session.package_dir.clone(),
        install_root: session.install_root.clone(),
        target_service_name: session.target_service_name.clone(),
        backend_command: session.backend_command.clone(),
        wrapper_timeout_ms: session.wrapper_timeout_ms,
        service_status_max_staleness_ms: session.service_status_max_staleness_ms,
        session_dir: Some(session_dir.display().to_string()),
        launch_packet_latest_session_path: Some(paths.session_path.display().to_string()),
        launch_packet_latest_status_path: Some(paths.status_path.display().to_string()),
        launch_packet_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_authorization_plan_report_path: Some(
            paths
                .latest_authorization_plan_report_path
                .display()
                .to_string(),
        ),
        latest_authorization_report_path: Some(
            paths.latest_authorization_report_path.display().to_string(),
        ),
        launch_packet_plan_report_path: Some(
            paths.launch_packet_plan_report_path.display().to_string(),
        ),
        launch_packet_report_path: Some(paths.launch_packet_report_path.display().to_string()),
        nested_authorize_latest_session_dir: Some(
            paths
                .nested_authorize_latest_session_dir
                .display()
                .to_string(),
        ),
        nested_launch_packet_session_dir: Some(
            paths.nested_launch_packet_session_dir.display().to_string(),
        ),
        latest_authorization_plan_verdict: status.latest_authorization_plan_verdict.clone(),
        latest_authorization_verdict: status.latest_authorization_verdict.clone(),
        launch_packet_plan_verdict: status.launch_packet_plan_verdict.clone(),
        result: Some(serialize_enum(&status.result)),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        resolve_latest_authorization_command_summary: Some(
            session.resolve_latest_authorization_command_summary.clone(),
        ),
        run_latest_authorization_command_summary: Some(
            session.run_latest_authorization_command_summary.clone(),
        ),
        verify_latest_authorization_command_summary: Some(
            session.verify_latest_authorization_command_summary.clone(),
        ),
        plan_launch_packet_command_summary: Some(
            session.plan_launch_packet_command_summary.clone(),
        ),
        downstream_launch_packet_command_summary: Some(
            session.downstream_launch_packet_command_summary.clone(),
        ),
        verify_launch_packet_command_summary: Some(
            session.verify_launch_packet_command_summary.clone(),
        ),
        reviewed_frozen_live_cutover_controller_command_summary: session
            .reviewed_frozen_live_cutover_controller_command_summary
            .clone(),
        clerestory_certificate_summary: session.clerestory_certificate_summary.clone(),
        clerestory_certificate_sha256: session.clerestory_certificate_sha256.clone(),
        clerestory_certificate_algorithm: session.clerestory_certificate_algorithm.clone(),
        verification_mismatches: mismatches,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    })
}

fn run_latest_launch_packet_with_resolution(
    root: &Path,
    session_dir: &Path,
    paths: &PackageLaunchPacketLatestPaths,
    resolved: &ResolvedLatestLaunchPacket,
) -> Result<PackageLaunchPacketLatestReport> {
    let session = planned_session(root, session_dir, paths, Some(resolved), None);
    persist_json(&paths.session_path, &session)?;
    persist_json(
        &paths.latest_authorization_plan_report_path,
        &resolved.latest_authorization_plan_raw,
    )?;
    persist_json(
        &paths.launch_packet_plan_report_path,
        &resolved.launch_packet_plan_raw,
    )?;

    let latest_authorization_plan_step = Some(step_artifact(
        &paths.latest_authorization_plan_report_path,
        "plan_latest_authorization",
        &resolved.latest_authorization_plan.verdict,
        &resolved.latest_authorization_plan.reason,
        resolved.latest_authorization_plan.generated_at,
    ));
    let launch_packet_plan_step = Some(step_artifact(
        &paths.launch_packet_plan_report_path,
        "plan_live_package_launch_packet",
        &resolved.launch_packet_plan.verdict,
        &resolved.launch_packet_plan.reason,
        resolved.launch_packet_plan.generated_at,
    ));

    let latest_authorization_raw = match run_json_command(
        AUTHORIZE_LATEST_BIN,
        &authorize_latest_run_args(root, &paths.nested_authorize_latest_session_dir),
    ) {
        Ok(value) => value,
        Err(error) => {
            return finalize_run_refusal(
                root,
                session_dir,
                paths,
                Some(resolved),
                LivePackageLaunchPacketLatestResult::RefusedByDriftedLaunchContract,
                format!(
                    "failed running nested latest-authorization handoff under {}: {error}",
                    paths.nested_authorize_latest_session_dir.display()
                ),
                latest_authorization_plan_step,
                None,
                launch_packet_plan_step,
                None,
                None,
                None,
            );
        }
    };
    persist_json(
        &paths.latest_authorization_report_path,
        &latest_authorization_raw,
    )?;
    let latest_authorization_view: AuthorizeLatestReportView =
        serde_json::from_value(latest_authorization_raw)
            .context("failed parsing nested latest-authorization run json")?;
    let latest_authorization_step = Some(step_artifact(
        &paths.latest_authorization_report_path,
        "run_latest_authorization",
        &latest_authorization_view.verdict,
        &latest_authorization_view.reason,
        latest_authorization_view.generated_at,
    ));
    let mut authorization_mismatches = Vec::new();
    compare_authorize_latest_run_against_snapshot(
        &latest_authorization_view,
        &resolved.snapshot,
        &paths.nested_authorize_latest_session_dir,
        &mut authorization_mismatches,
    );
    if !authorization_mismatches.is_empty() {
        return finalize_run_refusal(
            root,
            session_dir,
            paths,
            Some(resolved),
            LivePackageLaunchPacketLatestResult::RefusedByDriftedLaunchContract,
            authorization_mismatches[0].clone(),
            latest_authorization_plan_step,
            latest_authorization_step,
            launch_packet_plan_step,
            None,
            latest_authorization_view
                .current_pre_activation_gate_verdict
                .clone(),
            latest_authorization_view
                .current_pre_activation_gate_reason
                .clone(),
        );
    }

    let launch_packet_raw = match run_json_command(
        LAUNCH_PACKET_BIN,
        &launch_packet_args(
            &resolved.snapshot,
            &paths.nested_launch_packet_session_dir,
            "--run-live-package-launch-packet",
        ),
    ) {
        Ok(value) => value,
        Err(error) => {
            return finalize_run_refusal(
                root,
                session_dir,
                paths,
                Some(resolved),
                LivePackageLaunchPacketLatestResult::RefusedByDriftedLaunchContract,
                format!(
                    "failed running downstream launch-packet contract under {}: {error}",
                    paths.nested_launch_packet_session_dir.display()
                ),
                latest_authorization_plan_step,
                latest_authorization_step,
                launch_packet_plan_step,
                None,
                latest_authorization_view
                    .current_pre_activation_gate_verdict
                    .clone(),
                latest_authorization_view
                    .current_pre_activation_gate_reason
                    .clone(),
            );
        }
    };
    persist_json(&paths.launch_packet_report_path, &launch_packet_raw)?;
    let launch_packet_view: LaunchPacketReportView = serde_json::from_value(launch_packet_raw)
        .context("failed parsing nested launch-packet run json")?;
    let launch_packet_step = Some(step_artifact(
        &paths.launch_packet_report_path,
        "run_live_package_launch_packet",
        &launch_packet_view.verdict,
        &launch_packet_view.reason,
        launch_packet_view.generated_at,
    ));
    let mut launch_packet_mismatches = Vec::new();
    compare_launch_packet_run_against_snapshot(
        &launch_packet_view,
        &resolved.snapshot,
        &paths.nested_launch_packet_session_dir,
        &mut launch_packet_mismatches,
    );
    if !launch_packet_mismatches.is_empty() {
        return finalize_run_refusal(
            root,
            session_dir,
            paths,
            Some(resolved),
            LivePackageLaunchPacketLatestResult::RefusedByDriftedLaunchContract,
            launch_packet_mismatches[0].clone(),
            latest_authorization_plan_step,
            latest_authorization_step,
            launch_packet_plan_step,
            launch_packet_step,
            launch_packet_view
                .current_pre_activation_gate_verdict
                .clone(),
            launch_packet_view
                .current_pre_activation_gate_reason
                .clone(),
        );
    }

    let result =
        launch_packet_latest_result_from_launch_packet(launch_packet_view.result.as_deref())
            .unwrap_or(LivePackageLaunchPacketLatestResult::RefusedByDriftedLaunchContract);
    let reason = launch_packet_view.reason.clone();
    let status = PackageLaunchPacketLatestStatus {
        status_version: LAUNCH_PACKET_LATEST_STATUS_VERSION.to_string(),
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
        historical_launch_packet_session_dir: Some(
            resolved
                .snapshot
                .historical_launch_packet_session_dir
                .display()
                .to_string(),
        ),
        nested_authorize_latest_session_dir: paths
            .nested_authorize_latest_session_dir
            .display()
            .to_string(),
        nested_launch_packet_session_dir: paths
            .nested_launch_packet_session_dir
            .display()
            .to_string(),
        result,
        reason,
        latest_authorization_plan_verdict: Some(resolved.latest_authorization_plan.verdict.clone()),
        latest_authorization_verdict: Some(latest_authorization_view.verdict.clone()),
        launch_packet_plan_verdict: Some(resolved.launch_packet_plan.verdict.clone()),
        current_pre_activation_gate_verdict: launch_packet_view
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: launch_packet_view
            .current_pre_activation_gate_reason
            .clone(),
        latest_authorization_plan_step,
        latest_authorization_step,
        launch_packet_plan_step,
        launch_packet_step,
        operator_next_action_summary: operator_next_action_summary(
            result,
            Some(
                &resolved
                    .snapshot
                    .reviewed_frozen_live_cutover_controller_command_summary,
            ),
        ),
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    };
    persist_json(&paths.status_path, &status)?;
    let report = report_from_status(
        "run_latest_launch_packet",
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

fn run_latest_launch_packet_failure(
    root: &Path,
    session_dir: &Path,
    paths: &PackageLaunchPacketLatestPaths,
    failure: &LatestLaunchPacketResolutionFailure,
) -> Result<PackageLaunchPacketLatestReport> {
    let session = planned_session(root, session_dir, paths, None, Some(failure));
    persist_json(&paths.session_path, &session)?;

    let latest_authorization_plan_step =
        if let (Some(raw), Some(verdict), Some(reason), Some(generated_at)) = (
            failure.latest_authorization_plan_raw.as_ref(),
            failure.latest_authorization_plan_verdict.as_ref(),
            failure.latest_authorization_plan_reason.as_ref(),
            failure.latest_authorization_plan_generated_at,
        ) {
            persist_json(&paths.latest_authorization_plan_report_path, raw)?;
            Some(step_artifact(
                &paths.latest_authorization_plan_report_path,
                "plan_latest_authorization",
                verdict,
                reason,
                generated_at,
            ))
        } else {
            None
        };

    let launch_packet_plan_step =
        if let (Some(raw), Some(verdict), Some(reason), Some(generated_at)) = (
            failure.launch_packet_plan_raw.as_ref(),
            failure.launch_packet_plan_verdict.as_ref(),
            failure.launch_packet_plan_reason.as_ref(),
            failure.launch_packet_plan_generated_at,
        ) {
            persist_json(&paths.launch_packet_plan_report_path, raw)?;
            Some(step_artifact(
                &paths.launch_packet_plan_report_path,
                "plan_live_package_launch_packet",
                verdict,
                reason,
                generated_at,
            ))
        } else {
            None
        };

    let status = PackageLaunchPacketLatestStatus {
        status_version: LAUNCH_PACKET_LATEST_STATUS_VERSION.to_string(),
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
        historical_launch_packet_session_dir: failure.snapshot.as_ref().map(|value| {
            value
                .historical_launch_packet_session_dir
                .display()
                .to_string()
        }),
        nested_authorize_latest_session_dir: paths
            .nested_authorize_latest_session_dir
            .display()
            .to_string(),
        nested_launch_packet_session_dir: paths
            .nested_launch_packet_session_dir
            .display()
            .to_string(),
        result: result_from_failure_kind(failure.kind),
        reason: failure.reason.clone(),
        latest_authorization_plan_verdict: failure.latest_authorization_plan_verdict.clone(),
        latest_authorization_verdict: None,
        launch_packet_plan_verdict: failure.launch_packet_plan_verdict.clone(),
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        latest_authorization_plan_step,
        latest_authorization_step: None,
        launch_packet_plan_step,
        launch_packet_step: None,
        operator_next_action_summary: operator_next_action_summary(
            result_from_failure_kind(failure.kind),
            failure.snapshot.as_ref().map(|value| {
                value
                    .reviewed_frozen_live_cutover_controller_command_summary
                    .as_str()
            }),
        ),
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    };
    persist_json(&paths.status_path, &status)?;
    let report = report_from_status(
        "run_latest_launch_packet",
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

fn finalize_run_refusal(
    root: &Path,
    session_dir: &Path,
    paths: &PackageLaunchPacketLatestPaths,
    resolved: Option<&ResolvedLatestLaunchPacket>,
    result: LivePackageLaunchPacketLatestResult,
    reason: String,
    latest_authorization_plan_step: Option<PackageLaunchPacketLatestStepArtifact>,
    latest_authorization_step: Option<PackageLaunchPacketLatestStepArtifact>,
    launch_packet_plan_step: Option<PackageLaunchPacketLatestStepArtifact>,
    launch_packet_step: Option<PackageLaunchPacketLatestStepArtifact>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
) -> Result<PackageLaunchPacketLatestReport> {
    let session: PackageLaunchPacketLatestSession = load_json(&paths.session_path)?;
    let status = PackageLaunchPacketLatestStatus {
        status_version: LAUNCH_PACKET_LATEST_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        root: root.display().to_string(),
        session_dir: session_dir.display().to_string(),
        latest_top_step_name: resolved.map(|value| value.snapshot.latest_top_step_name.clone()),
        latest_top_session_dir: resolved
            .map(|value| value.snapshot.latest_top_session_dir.display().to_string()),
        historical_launch_packet_session_dir: resolved.map(|value| {
            value
                .snapshot
                .historical_launch_packet_session_dir
                .display()
                .to_string()
        }),
        nested_authorize_latest_session_dir: paths
            .nested_authorize_latest_session_dir
            .display()
            .to_string(),
        nested_launch_packet_session_dir: paths
            .nested_launch_packet_session_dir
            .display()
            .to_string(),
        result,
        reason,
        latest_authorization_plan_verdict: latest_authorization_plan_step
            .as_ref()
            .map(|value| value.verdict.clone()),
        latest_authorization_verdict: latest_authorization_step
            .as_ref()
            .map(|value| value.verdict.clone()),
        launch_packet_plan_verdict: launch_packet_plan_step
            .as_ref()
            .map(|value| value.verdict.clone()),
        current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason,
        latest_authorization_plan_step,
        latest_authorization_step,
        launch_packet_plan_step,
        launch_packet_step,
        operator_next_action_summary: operator_next_action_summary(
            result,
            resolved.as_ref().map(|value| {
                value
                    .snapshot
                    .reviewed_frozen_live_cutover_controller_command_summary
                    .as_str()
            }),
        ),
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    };
    persist_json(&paths.status_path, &status)?;
    let report = report_from_status(
        "run_latest_launch_packet",
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

fn resolve_latest_launch_packet(
    root: &Path,
    paths: &PackageLaunchPacketLatestPaths,
) -> std::result::Result<ResolvedLatestLaunchPacket, LatestLaunchPacketResolutionFailure> {
    let latest_authorization_plan_raw = match run_json_command(
        AUTHORIZE_LATEST_BIN,
        &authorize_latest_plan_args(root, &paths.nested_authorize_latest_session_dir),
    ) {
        Ok(value) => value,
        Err(error) => {
            return Err(LatestLaunchPacketResolutionFailure {
                kind: ResolutionFailureKind::MissingLatestChain,
                reason: format!(
                    "failed resolving latest immutable chain under {}: {error}",
                    root.display()
                ),
                snapshot: None,
                latest_authorization_plan_raw: None,
                latest_authorization_plan_verdict: None,
                latest_authorization_plan_reason: None,
                latest_authorization_plan_generated_at: None,
                launch_packet_plan_raw: None,
                launch_packet_plan_verdict: None,
                launch_packet_plan_reason: None,
                launch_packet_plan_generated_at: None,
            });
        }
    };
    let latest_authorization_plan: AuthorizeLatestReportView =
        serde_json::from_value(latest_authorization_plan_raw.clone()).map_err(|error| {
            LatestLaunchPacketResolutionFailure {
                kind: ResolutionFailureKind::InvalidLatestChain,
                reason: format!(
                    "failed parsing latest-authorization plan json for {}: {error}",
                    root.display()
                ),
                snapshot: None,
                latest_authorization_plan_raw: Some(latest_authorization_plan_raw.clone()),
                latest_authorization_plan_verdict: None,
                latest_authorization_plan_reason: None,
                latest_authorization_plan_generated_at: None,
                launch_packet_plan_raw: None,
                launch_packet_plan_verdict: None,
                launch_packet_plan_reason: None,
                launch_packet_plan_generated_at: None,
            }
        })?;

    let snapshot = parse_snapshot_from_authorize_latest(&latest_authorization_plan).ok();
    if latest_authorization_plan.verdict != AUTHORIZE_LATEST_PLAN_READY {
        return Err(LatestLaunchPacketResolutionFailure {
            kind: map_authorize_latest_failure_kind(&latest_authorization_plan.verdict),
            reason: latest_authorization_plan.reason.clone(),
            snapshot,
            latest_authorization_plan_raw: Some(latest_authorization_plan_raw),
            latest_authorization_plan_verdict: Some(latest_authorization_plan.verdict),
            latest_authorization_plan_reason: Some(latest_authorization_plan.reason),
            latest_authorization_plan_generated_at: Some(latest_authorization_plan.generated_at),
            launch_packet_plan_raw: None,
            launch_packet_plan_verdict: None,
            launch_packet_plan_reason: None,
            launch_packet_plan_generated_at: None,
        });
    }

    let snapshot =
        parse_snapshot_from_authorize_latest(&latest_authorization_plan).map_err(|error| {
            LatestLaunchPacketResolutionFailure {
                kind: ResolutionFailureKind::InvalidLatestChain,
                reason: error.to_string(),
                snapshot: None,
                latest_authorization_plan_raw: Some(latest_authorization_plan_raw.clone()),
                latest_authorization_plan_verdict: Some(latest_authorization_plan.verdict.clone()),
                latest_authorization_plan_reason: Some(latest_authorization_plan.reason.clone()),
                latest_authorization_plan_generated_at: Some(
                    latest_authorization_plan.generated_at,
                ),
                launch_packet_plan_raw: None,
                launch_packet_plan_verdict: None,
                launch_packet_plan_reason: None,
                launch_packet_plan_generated_at: None,
            }
        })?;
    let mut authorize_plan_mismatches = Vec::new();
    compare_authorize_latest_plan_against_snapshot(
        &latest_authorization_plan,
        &snapshot,
        &paths.nested_authorize_latest_session_dir,
        &mut authorize_plan_mismatches,
    );
    if !authorize_plan_mismatches.is_empty() {
        return Err(LatestLaunchPacketResolutionFailure {
            kind: ResolutionFailureKind::DriftedLaunchContract,
            reason: authorize_plan_mismatches[0].clone(),
            snapshot: Some(snapshot),
            latest_authorization_plan_raw: Some(latest_authorization_plan_raw),
            latest_authorization_plan_verdict: Some(latest_authorization_plan.verdict),
            latest_authorization_plan_reason: Some(latest_authorization_plan.reason),
            latest_authorization_plan_generated_at: Some(latest_authorization_plan.generated_at),
            launch_packet_plan_raw: None,
            launch_packet_plan_verdict: None,
            launch_packet_plan_reason: None,
            launch_packet_plan_generated_at: None,
        });
    }

    let launch_packet_plan_raw = run_json_command(
        LAUNCH_PACKET_BIN,
        &launch_packet_args(
            &snapshot,
            &paths.nested_launch_packet_session_dir,
            "--plan-live-package-launch-packet",
        ),
    )
    .map_err(|error| LatestLaunchPacketResolutionFailure {
        kind: ResolutionFailureKind::DriftedLaunchContract,
        reason: format!(
            "failed planning downstream launch-packet contract under {}: {error}",
            paths.nested_launch_packet_session_dir.display()
        ),
        snapshot: Some(snapshot.clone()),
        latest_authorization_plan_raw: Some(latest_authorization_plan_raw.clone()),
        latest_authorization_plan_verdict: Some(latest_authorization_plan.verdict.clone()),
        latest_authorization_plan_reason: Some(latest_authorization_plan.reason.clone()),
        latest_authorization_plan_generated_at: Some(latest_authorization_plan.generated_at),
        launch_packet_plan_raw: None,
        launch_packet_plan_verdict: None,
        launch_packet_plan_reason: None,
        launch_packet_plan_generated_at: None,
    })?;
    let launch_packet_plan: LaunchPacketReportView =
        serde_json::from_value(launch_packet_plan_raw.clone()).map_err(|error| {
            LatestLaunchPacketResolutionFailure {
                kind: ResolutionFailureKind::DriftedLaunchContract,
                reason: format!("failed parsing launch-packet plan json: {error}"),
                snapshot: Some(snapshot.clone()),
                latest_authorization_plan_raw: Some(latest_authorization_plan_raw.clone()),
                latest_authorization_plan_verdict: Some(latest_authorization_plan.verdict.clone()),
                latest_authorization_plan_reason: Some(latest_authorization_plan.reason.clone()),
                latest_authorization_plan_generated_at: Some(
                    latest_authorization_plan.generated_at,
                ),
                launch_packet_plan_raw: Some(launch_packet_plan_raw.clone()),
                launch_packet_plan_verdict: None,
                launch_packet_plan_reason: None,
                launch_packet_plan_generated_at: None,
            }
        })?;
    if launch_packet_plan.verdict != LAUNCH_PACKET_PLAN_READY {
        return Err(LatestLaunchPacketResolutionFailure {
            kind: ResolutionFailureKind::DriftedLaunchContract,
            reason: launch_packet_plan.reason.clone(),
            snapshot: Some(snapshot.clone()),
            latest_authorization_plan_raw: Some(latest_authorization_plan_raw),
            latest_authorization_plan_verdict: Some(latest_authorization_plan.verdict),
            latest_authorization_plan_reason: Some(latest_authorization_plan.reason),
            latest_authorization_plan_generated_at: Some(latest_authorization_plan.generated_at),
            launch_packet_plan_raw: Some(launch_packet_plan_raw),
            launch_packet_plan_verdict: Some(launch_packet_plan.verdict),
            launch_packet_plan_reason: Some(launch_packet_plan.reason),
            launch_packet_plan_generated_at: Some(launch_packet_plan.generated_at),
        });
    }
    let mut launch_packet_plan_mismatches = Vec::new();
    compare_launch_packet_plan_against_snapshot(
        &launch_packet_plan,
        &snapshot,
        &paths.nested_launch_packet_session_dir,
        &mut launch_packet_plan_mismatches,
    );
    if !launch_packet_plan_mismatches.is_empty() {
        return Err(LatestLaunchPacketResolutionFailure {
            kind: ResolutionFailureKind::DriftedLaunchContract,
            reason: launch_packet_plan_mismatches[0].clone(),
            snapshot: Some(snapshot),
            latest_authorization_plan_raw: Some(latest_authorization_plan_raw),
            latest_authorization_plan_verdict: Some(latest_authorization_plan.verdict),
            latest_authorization_plan_reason: Some(latest_authorization_plan.reason),
            latest_authorization_plan_generated_at: Some(latest_authorization_plan.generated_at),
            launch_packet_plan_raw: Some(launch_packet_plan_raw),
            launch_packet_plan_verdict: Some(launch_packet_plan.verdict),
            launch_packet_plan_reason: Some(launch_packet_plan.reason),
            launch_packet_plan_generated_at: Some(launch_packet_plan.generated_at),
        });
    }

    Ok(ResolvedLatestLaunchPacket {
        snapshot,
        latest_authorization_plan_raw,
        latest_authorization_plan,
        launch_packet_plan_raw,
        launch_packet_plan,
    })
}

fn parse_snapshot_from_authorize_latest(
    view: &AuthorizeLatestReportView,
) -> Result<LatestLaunchPacketSnapshot> {
    Ok(LatestLaunchPacketSnapshot {
        latest_top_step_name: required_option_string(
            view.latest_top_step_name.as_ref(),
            "latest_top_step_name",
        )?,
        latest_top_session_dir: PathBuf::from(required_option_string(
            view.latest_top_session_dir.as_ref(),
            "latest_top_session_dir",
        )?),
        historical_launch_packet_session_dir: PathBuf::from(required_option_string(
            view.launch_packet_session_dir.as_ref(),
            "launch_packet_session_dir",
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
            .ok_or_else(|| anyhow!("authorize_latest plan is missing wrapper_timeout_ms"))?,
        service_status_max_staleness_ms: view.service_status_max_staleness_ms.ok_or_else(|| {
            anyhow!("authorize_latest plan is missing service_status_max_staleness_ms")
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
    verdict: TinyLivePackageLaunchPacketLatestVerdict,
    reason: String,
    root: &Path,
    session_dir: &Path,
    paths: &PackageLaunchPacketLatestPaths,
    resolved: &ResolvedLatestLaunchPacket,
) -> PackageLaunchPacketLatestReport {
    PackageLaunchPacketLatestReport {
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
        historical_launch_packet_session_dir: Some(
            resolved
                .snapshot
                .historical_launch_packet_session_dir
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
        launch_packet_latest_session_path: Some(paths.session_path.display().to_string()),
        launch_packet_latest_status_path: Some(paths.status_path.display().to_string()),
        launch_packet_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_authorization_plan_report_path: Some(
            paths
                .latest_authorization_plan_report_path
                .display()
                .to_string(),
        ),
        latest_authorization_report_path: Some(
            paths.latest_authorization_report_path.display().to_string(),
        ),
        launch_packet_plan_report_path: Some(
            paths.launch_packet_plan_report_path.display().to_string(),
        ),
        launch_packet_report_path: Some(paths.launch_packet_report_path.display().to_string()),
        nested_authorize_latest_session_dir: Some(
            paths
                .nested_authorize_latest_session_dir
                .display()
                .to_string(),
        ),
        nested_launch_packet_session_dir: Some(
            paths.nested_launch_packet_session_dir.display().to_string(),
        ),
        latest_authorization_plan_verdict: Some(resolved.latest_authorization_plan.verdict.clone()),
        latest_authorization_verdict: None,
        launch_packet_plan_verdict: Some(resolved.launch_packet_plan.verdict.clone()),
        result: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        resolve_latest_authorization_command_summary: Some(
            resolve_latest_authorization_command_summary(
                root,
                &paths.nested_authorize_latest_session_dir,
            ),
        ),
        run_latest_authorization_command_summary: Some(run_latest_authorization_command_summary(
            root,
            &paths.nested_authorize_latest_session_dir,
        )),
        verify_latest_authorization_command_summary: Some(
            verify_latest_authorization_command_summary(&paths.nested_authorize_latest_session_dir),
        ),
        plan_launch_packet_command_summary: Some(plan_launch_packet_command_summary(
            &resolved.snapshot,
            &paths.nested_launch_packet_session_dir,
        )),
        downstream_launch_packet_command_summary: Some(downstream_launch_packet_command_summary(
            &resolved.snapshot,
            &paths.nested_launch_packet_session_dir,
        )),
        verify_launch_packet_command_summary: Some(verify_launch_packet_command_summary(
            &resolved.snapshot,
            &paths.nested_launch_packet_session_dir,
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
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn report_from_resolution_failure(
    mode: &str,
    root: &Path,
    session_dir: &Path,
    paths: &PackageLaunchPacketLatestPaths,
    failure: &LatestLaunchPacketResolutionFailure,
) -> PackageLaunchPacketLatestReport {
    let snapshot = failure.snapshot.as_ref();
    PackageLaunchPacketLatestReport {
        generated_at: Utc::now(),
        mode: mode.to_string(),
        verdict: verdict_from_failure_kind(failure.kind),
        reason: failure.reason.clone(),
        root: Some(root.display().to_string()),
        latest_top_step_name: snapshot.map(|value| value.latest_top_step_name.clone()),
        latest_top_session_dir: snapshot
            .map(|value| value.latest_top_session_dir.display().to_string()),
        historical_launch_packet_session_dir: snapshot.map(|value| {
            value
                .historical_launch_packet_session_dir
                .display()
                .to_string()
        }),
        package_dir: snapshot.map(|value| value.package_dir.display().to_string()),
        install_root: snapshot.map(|value| value.install_root.display().to_string()),
        target_service_name: snapshot.map(|value| value.target_service_name.clone()),
        backend_command: snapshot.map(|value| value.backend_command.clone()),
        wrapper_timeout_ms: snapshot.map(|value| value.wrapper_timeout_ms),
        service_status_max_staleness_ms: snapshot
            .map(|value| value.service_status_max_staleness_ms),
        session_dir: Some(session_dir.display().to_string()),
        launch_packet_latest_session_path: Some(paths.session_path.display().to_string()),
        launch_packet_latest_status_path: Some(paths.status_path.display().to_string()),
        launch_packet_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_authorization_plan_report_path: Some(
            paths
                .latest_authorization_plan_report_path
                .display()
                .to_string(),
        ),
        latest_authorization_report_path: Some(
            paths.latest_authorization_report_path.display().to_string(),
        ),
        launch_packet_plan_report_path: Some(
            paths.launch_packet_plan_report_path.display().to_string(),
        ),
        launch_packet_report_path: Some(paths.launch_packet_report_path.display().to_string()),
        nested_authorize_latest_session_dir: Some(
            paths
                .nested_authorize_latest_session_dir
                .display()
                .to_string(),
        ),
        nested_launch_packet_session_dir: Some(
            paths.nested_launch_packet_session_dir.display().to_string(),
        ),
        latest_authorization_plan_verdict: failure.latest_authorization_plan_verdict.clone(),
        latest_authorization_verdict: None,
        launch_packet_plan_verdict: failure.launch_packet_plan_verdict.clone(),
        result: Some(serialize_enum(&result_from_failure_kind(failure.kind))),
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        resolve_latest_authorization_command_summary: Some(
            resolve_latest_authorization_command_summary(
                root,
                &paths.nested_authorize_latest_session_dir,
            ),
        ),
        run_latest_authorization_command_summary: Some(run_latest_authorization_command_summary(
            root,
            &paths.nested_authorize_latest_session_dir,
        )),
        verify_latest_authorization_command_summary: Some(
            verify_latest_authorization_command_summary(&paths.nested_authorize_latest_session_dir),
        ),
        plan_launch_packet_command_summary: snapshot.map(|value| {
            plan_launch_packet_command_summary(value, &paths.nested_launch_packet_session_dir)
        }),
        downstream_launch_packet_command_summary: snapshot.map(|value| {
            downstream_launch_packet_command_summary(value, &paths.nested_launch_packet_session_dir)
        }),
        verify_launch_packet_command_summary: snapshot.map(|value| {
            verify_launch_packet_command_summary(value, &paths.nested_launch_packet_session_dir)
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
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn planned_session(
    root: &Path,
    session_dir: &Path,
    paths: &PackageLaunchPacketLatestPaths,
    resolved: Option<&ResolvedLatestLaunchPacket>,
    failure: Option<&LatestLaunchPacketResolutionFailure>,
) -> PackageLaunchPacketLatestSession {
    let snapshot = resolved
        .map(|value| &value.snapshot)
        .or_else(|| failure.and_then(|value| value.snapshot.as_ref()));
    PackageLaunchPacketLatestSession {
        session_version: LAUNCH_PACKET_LATEST_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        root: root.display().to_string(),
        session_dir: session_dir.display().to_string(),
        nested_authorize_latest_session_dir: paths
            .nested_authorize_latest_session_dir
            .display()
            .to_string(),
        nested_launch_packet_session_dir: paths
            .nested_launch_packet_session_dir
            .display()
            .to_string(),
        latest_top_step_name: snapshot.map(|value| value.latest_top_step_name.clone()),
        latest_top_session_dir: snapshot
            .map(|value| value.latest_top_session_dir.display().to_string()),
        historical_launch_packet_session_dir: snapshot.map(|value| {
            value
                .historical_launch_packet_session_dir
                .display()
                .to_string()
        }),
        package_dir: snapshot.map(|value| value.package_dir.display().to_string()),
        install_root: snapshot.map(|value| value.install_root.display().to_string()),
        target_service_name: snapshot.map(|value| value.target_service_name.clone()),
        backend_command: snapshot.map(|value| value.backend_command.clone()),
        wrapper_timeout_ms: snapshot.map(|value| value.wrapper_timeout_ms),
        service_status_max_staleness_ms: snapshot
            .map(|value| value.service_status_max_staleness_ms),
        resolve_latest_authorization_command_summary: resolve_latest_authorization_command_summary(
            root,
            &paths.nested_authorize_latest_session_dir,
        ),
        run_latest_authorization_command_summary: run_latest_authorization_command_summary(
            root,
            &paths.nested_authorize_latest_session_dir,
        ),
        verify_latest_authorization_command_summary: verify_latest_authorization_command_summary(
            &paths.nested_authorize_latest_session_dir,
        ),
        plan_launch_packet_command_summary: snapshot
            .map(|value| {
                plan_launch_packet_command_summary(value, &paths.nested_launch_packet_session_dir)
            })
            .unwrap_or_else(|| "<launch-packet-plan-unavailable>".to_string()),
        downstream_launch_packet_command_summary: snapshot
            .map(|value| {
                downstream_launch_packet_command_summary(
                    value,
                    &paths.nested_launch_packet_session_dir,
                )
            })
            .unwrap_or_else(|| "<launch-packet-run-unavailable>".to_string()),
        verify_launch_packet_command_summary: snapshot
            .map(|value| {
                verify_launch_packet_command_summary(value, &paths.nested_launch_packet_session_dir)
            })
            .unwrap_or_else(|| "<launch-packet-verify-unavailable>".to_string()),
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
    verdict: TinyLivePackageLaunchPacketLatestVerdict,
    session_dir: &Path,
    paths: &PackageLaunchPacketLatestPaths,
    session: &PackageLaunchPacketLatestSession,
    status: &PackageLaunchPacketLatestStatus,
    verification_mismatches: Vec<String>,
) -> PackageLaunchPacketLatestReport {
    PackageLaunchPacketLatestReport {
        generated_at: Utc::now(),
        mode: mode.to_string(),
        verdict,
        reason: status.reason.clone(),
        root: Some(session.root.clone()),
        latest_top_step_name: status.latest_top_step_name.clone(),
        latest_top_session_dir: status.latest_top_session_dir.clone(),
        historical_launch_packet_session_dir: status.historical_launch_packet_session_dir.clone(),
        package_dir: session.package_dir.clone(),
        install_root: session.install_root.clone(),
        target_service_name: session.target_service_name.clone(),
        backend_command: session.backend_command.clone(),
        wrapper_timeout_ms: session.wrapper_timeout_ms,
        service_status_max_staleness_ms: session.service_status_max_staleness_ms,
        session_dir: Some(session_dir.display().to_string()),
        launch_packet_latest_session_path: Some(paths.session_path.display().to_string()),
        launch_packet_latest_status_path: Some(paths.status_path.display().to_string()),
        launch_packet_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_authorization_plan_report_path: Some(
            paths
                .latest_authorization_plan_report_path
                .display()
                .to_string(),
        ),
        latest_authorization_report_path: Some(
            paths.latest_authorization_report_path.display().to_string(),
        ),
        launch_packet_plan_report_path: Some(
            paths.launch_packet_plan_report_path.display().to_string(),
        ),
        launch_packet_report_path: Some(paths.launch_packet_report_path.display().to_string()),
        nested_authorize_latest_session_dir: Some(
            session.nested_authorize_latest_session_dir.clone(),
        ),
        nested_launch_packet_session_dir: Some(session.nested_launch_packet_session_dir.clone()),
        latest_authorization_plan_verdict: status.latest_authorization_plan_verdict.clone(),
        latest_authorization_verdict: status.latest_authorization_verdict.clone(),
        launch_packet_plan_verdict: status.launch_packet_plan_verdict.clone(),
        result: Some(serialize_enum(&status.result)),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        resolve_latest_authorization_command_summary: Some(
            session.resolve_latest_authorization_command_summary.clone(),
        ),
        run_latest_authorization_command_summary: Some(
            session.run_latest_authorization_command_summary.clone(),
        ),
        verify_latest_authorization_command_summary: Some(
            session.verify_latest_authorization_command_summary.clone(),
        ),
        plan_launch_packet_command_summary: Some(
            session.plan_launch_packet_command_summary.clone(),
        ),
        downstream_launch_packet_command_summary: Some(
            session.downstream_launch_packet_command_summary.clone(),
        ),
        verify_launch_packet_command_summary: Some(
            session.verify_launch_packet_command_summary.clone(),
        ),
        reviewed_frozen_live_cutover_controller_command_summary: session
            .reviewed_frozen_live_cutover_controller_command_summary
            .clone(),
        clerestory_certificate_summary: session.clerestory_certificate_summary.clone(),
        clerestory_certificate_sha256: session.clerestory_certificate_sha256.clone(),
        clerestory_certificate_algorithm: session.clerestory_certificate_algorithm.clone(),
        verification_mismatches,
        explicit_statement: session.explicit_statement.clone(),
    }
}

fn launch_packet_latest_paths(session_dir: &Path) -> PackageLaunchPacketLatestPaths {
    PackageLaunchPacketLatestPaths {
        session_path: session_dir
            .join("tiny_live_activation_package_launch_packet_latest.session.json"),
        status_path: session_dir
            .join("tiny_live_activation_package_launch_packet_latest.status.json"),
        report_path: session_dir
            .join("tiny_live_activation_package_launch_packet_latest.report.json"),
        latest_authorization_plan_report_path: session_dir.join(
            "tiny_live_activation_package_launch_packet_latest.latest_authorization_plan.report.json",
        ),
        latest_authorization_report_path: session_dir.join(
            "tiny_live_activation_package_launch_packet_latest.latest_authorization.report.json",
        ),
        launch_packet_plan_report_path: session_dir
            .join("tiny_live_activation_package_launch_packet_latest.launch_packet_plan.report.json"),
        launch_packet_report_path: session_dir
            .join("tiny_live_activation_package_launch_packet_latest.launch_packet.report.json"),
        nested_authorize_latest_session_dir: session_dir.join(
            "tiny_live_activation_package_launch_packet_latest.authorize_latest_session",
        ),
        nested_launch_packet_session_dir: session_dir
            .join("tiny_live_activation_package_launch_packet_latest.launch_packet_session"),
    }
}

fn authorize_latest_nested_report_path(session_dir: &Path) -> PathBuf {
    session_dir.join("tiny_live_activation_package_authorize_latest.report.json")
}

fn nested_launch_packet_report_path(session_dir: &Path) -> PathBuf {
    session_dir.join("tiny_live_activation_package_launch_packet.report.json")
}

fn launch_packet_nested_authorization_session_dir(session_dir: &Path) -> PathBuf {
    session_dir.join("tiny_live_activation_package_launch_packet.authorization_session")
}

fn resolve_latest_authorization_command_summary(root: &Path, session_dir: &Path) -> String {
    format!(
        "{AUTHORIZE_LATEST_BIN} --root {} --session-dir {} --plan-latest-authorization",
        shell_escape_path(root),
        shell_escape_path(session_dir),
    )
}

fn run_latest_authorization_command_summary(root: &Path, session_dir: &Path) -> String {
    format!(
        "{AUTHORIZE_LATEST_BIN} --root {} --session-dir {} --run-latest-authorization",
        shell_escape_path(root),
        shell_escape_path(session_dir),
    )
}

fn verify_latest_authorization_command_summary(session_dir: &Path) -> String {
    format!(
        "{AUTHORIZE_LATEST_BIN} --session-dir {} --verify-latest-authorization",
        shell_escape_path(session_dir),
    )
}

fn plan_launch_packet_command_summary(
    snapshot: &LatestLaunchPacketSnapshot,
    session_dir: &Path,
) -> String {
    format!(
        "{LAUNCH_PACKET_BIN} --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --service-status-max-staleness-ms {} --session-dir {} --plan-live-package-launch-packet",
        shell_escape_path(&snapshot.package_dir),
        shell_escape_path(&snapshot.install_root),
        shell_escape_string(&snapshot.target_service_name),
        shell_escape_string(&snapshot.backend_command),
        snapshot.wrapper_timeout_ms,
        snapshot.service_status_max_staleness_ms,
        shell_escape_path(session_dir),
    )
}

fn downstream_launch_packet_command_summary(
    snapshot: &LatestLaunchPacketSnapshot,
    session_dir: &Path,
) -> String {
    format!(
        "{LAUNCH_PACKET_BIN} --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --service-status-max-staleness-ms {} --session-dir {} --run-live-package-launch-packet",
        shell_escape_path(&snapshot.package_dir),
        shell_escape_path(&snapshot.install_root),
        shell_escape_string(&snapshot.target_service_name),
        shell_escape_string(&snapshot.backend_command),
        snapshot.wrapper_timeout_ms,
        snapshot.service_status_max_staleness_ms,
        shell_escape_path(session_dir),
    )
}

fn verify_launch_packet_command_summary(
    snapshot: &LatestLaunchPacketSnapshot,
    session_dir: &Path,
) -> String {
    format!(
        "{LAUNCH_PACKET_BIN} --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --service-status-max-staleness-ms {} --session-dir {} --verify-live-package-launch-packet",
        shell_escape_path(&snapshot.package_dir),
        shell_escape_path(&snapshot.install_root),
        shell_escape_string(&snapshot.target_service_name),
        shell_escape_string(&snapshot.backend_command),
        snapshot.wrapper_timeout_ms,
        snapshot.service_status_max_staleness_ms,
        shell_escape_path(session_dir),
    )
}

fn authorize_latest_plan_args(root: &Path, session_dir: &Path) -> Vec<String> {
    vec![
        "--root".to_string(),
        root.display().to_string(),
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--plan-latest-authorization".to_string(),
        "--json".to_string(),
    ]
}

fn authorize_latest_run_args(root: &Path, session_dir: &Path) -> Vec<String> {
    vec![
        "--root".to_string(),
        root.display().to_string(),
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--run-latest-authorization".to_string(),
        "--json".to_string(),
    ]
}

fn authorize_latest_verify_args(session_dir: &Path) -> Vec<String> {
    vec![
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        "--verify-latest-authorization".to_string(),
        "--json".to_string(),
    ]
}

fn launch_packet_args(
    snapshot: &LatestLaunchPacketSnapshot,
    session_dir: &Path,
    mode_flag: &str,
) -> Vec<String> {
    vec![
        "--package-dir".to_string(),
        snapshot.package_dir.display().to_string(),
        "--install-root".to_string(),
        snapshot.install_root.display().to_string(),
        "--target-service".to_string(),
        snapshot.target_service_name.clone(),
        "--backend-command".to_string(),
        snapshot.backend_command.clone(),
        "--wrapper-timeout-ms".to_string(),
        snapshot.wrapper_timeout_ms.to_string(),
        "--service-status-max-staleness-ms".to_string(),
        snapshot.service_status_max_staleness_ms.to_string(),
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        mode_flag.to_string(),
        "--json".to_string(),
    ]
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
        .with_context(|| format!("failed spawning {}", binary))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("{binary} exited with {}: {}", output.status, stderr.trim());
    }
    serde_json::from_slice(&output.stdout)
        .with_context(|| format!("failed parsing JSON output from {}", binary))
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
            "failed writing latest-launch-packet script to {}",
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
) -> PackageLaunchPacketLatestStepArtifact {
    PackageLaunchPacketLatestStepArtifact {
        path: path.display().to_string(),
        mode: mode.to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        generated_at,
    }
}

fn load_required_step_json<T: for<'de> Deserialize<'de>>(
    step: &Option<PackageLaunchPacketLatestStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<T> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("{label} is missing from latest-launch-packet status"))?;
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
    step: Option<&PackageLaunchPacketLatestStepArtifact>,
    expected_path: &Path,
    expected_mode: &str,
    expected_verdict: &str,
    expected_reason: &str,
    expected_generated_at: DateTime<Utc>,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    let Some(step) = step else {
        mismatches.push(format!(
            "{label} is missing from latest-launch-packet status"
        ));
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

fn compare_authorize_latest_plan_against_snapshot(
    stored: &AuthorizeLatestReportView,
    snapshot: &LatestLaunchPacketSnapshot,
    session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        Some(stored.verdict.as_str()),
        Some(AUTHORIZE_LATEST_PLAN_READY),
        "stored latest-authorization plan verdict",
        mismatches,
    );
    compare_option_string(
        stored.latest_top_step_name.as_deref(),
        Some(snapshot.latest_top_step_name.as_str()),
        "stored latest-authorization plan latest_top_step_name",
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
        "stored latest-authorization plan latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.launch_packet_session_dir.as_deref(),
        Some(
            snapshot
                .historical_launch_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored latest-authorization plan historical launch_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored latest-authorization plan session_dir",
        mismatches,
    );
    compare_option_string(
        stored.package_dir.as_deref(),
        Some(snapshot.package_dir.display().to_string().as_str()),
        "stored latest-authorization plan package_dir",
        mismatches,
    );
    compare_option_string(
        stored.install_root.as_deref(),
        Some(snapshot.install_root.display().to_string().as_str()),
        "stored latest-authorization plan install_root",
        mismatches,
    );
    compare_option_string(
        stored.target_service_name.as_deref(),
        Some(snapshot.target_service_name.as_str()),
        "stored latest-authorization plan target_service_name",
        mismatches,
    );
    compare_option_string(
        stored.backend_command.as_deref(),
        Some(snapshot.backend_command.as_str()),
        "stored latest-authorization plan backend_command",
        mismatches,
    );
    compare_option_u64(
        stored.wrapper_timeout_ms,
        Some(snapshot.wrapper_timeout_ms),
        "stored latest-authorization plan wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        stored.service_status_max_staleness_ms,
        Some(snapshot.service_status_max_staleness_ms),
        "stored latest-authorization plan service_status_max_staleness_ms",
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
        "stored latest-authorization plan reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_summary.as_deref(),
        Some(snapshot.clerestory_certificate_summary.as_str()),
        "stored latest-authorization plan clerestory_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_sha256.as_deref(),
        Some(snapshot.clerestory_certificate_sha256.as_str()),
        "stored latest-authorization plan clerestory_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_algorithm.as_deref(),
        Some(snapshot.clerestory_certificate_algorithm.as_str()),
        "stored latest-authorization plan clerestory_certificate_algorithm",
        mismatches,
    );
}

fn compare_authorize_latest_run_against_snapshot(
    stored: &AuthorizeLatestReportView,
    snapshot: &LatestLaunchPacketSnapshot,
    session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        stored.latest_top_step_name.as_deref(),
        Some(snapshot.latest_top_step_name.as_str()),
        "stored latest-authorization run latest_top_step_name",
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
        "stored latest-authorization run latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.launch_packet_session_dir.as_deref(),
        Some(
            snapshot
                .historical_launch_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored latest-authorization run historical launch_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored latest-authorization run session_dir",
        mismatches,
    );
    compare_option_string(
        stored.package_dir.as_deref(),
        Some(snapshot.package_dir.display().to_string().as_str()),
        "stored latest-authorization run package_dir",
        mismatches,
    );
    compare_option_string(
        stored.install_root.as_deref(),
        Some(snapshot.install_root.display().to_string().as_str()),
        "stored latest-authorization run install_root",
        mismatches,
    );
    compare_option_string(
        stored.target_service_name.as_deref(),
        Some(snapshot.target_service_name.as_str()),
        "stored latest-authorization run target_service_name",
        mismatches,
    );
    compare_option_string(
        stored.backend_command.as_deref(),
        Some(snapshot.backend_command.as_str()),
        "stored latest-authorization run backend_command",
        mismatches,
    );
    compare_option_u64(
        stored.wrapper_timeout_ms,
        Some(snapshot.wrapper_timeout_ms),
        "stored latest-authorization run wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        stored.service_status_max_staleness_ms,
        Some(snapshot.service_status_max_staleness_ms),
        "stored latest-authorization run service_status_max_staleness_ms",
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
        "stored latest-authorization run reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_summary.as_deref(),
        Some(snapshot.clerestory_certificate_summary.as_str()),
        "stored latest-authorization run clerestory_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_sha256.as_deref(),
        Some(snapshot.clerestory_certificate_sha256.as_str()),
        "stored latest-authorization run clerestory_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_algorithm.as_deref(),
        Some(snapshot.clerestory_certificate_algorithm.as_str()),
        "stored latest-authorization run clerestory_certificate_algorithm",
        mismatches,
    );
}

fn compare_authorize_latest_copy_matches_actual(
    stored: &AuthorizeLatestReportView,
    actual: &AuthorizeLatestReportView,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        Some(stored.verdict.as_str()),
        Some(actual.verdict.as_str()),
        "stored latest-authorization copy verdict",
        mismatches,
    );
    compare_option_string(
        Some(stored.reason.as_str()),
        Some(actual.reason.as_str()),
        "stored latest-authorization copy reason",
        mismatches,
    );
    if stored.generated_at != actual.generated_at {
        mismatches.push(format!(
            "stored latest-authorization copy generated_at {:?} does not match actual {:?}",
            stored.generated_at, actual.generated_at
        ));
    }
    compare_option_string(
        stored.latest_top_step_name.as_deref(),
        actual.latest_top_step_name.as_deref(),
        "stored latest-authorization copy latest_top_step_name",
        mismatches,
    );
    compare_option_string(
        stored.latest_top_session_dir.as_deref(),
        actual.latest_top_session_dir.as_deref(),
        "stored latest-authorization copy latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.launch_packet_session_dir.as_deref(),
        actual.launch_packet_session_dir.as_deref(),
        "stored latest-authorization copy historical launch_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        actual.session_dir.as_deref(),
        "stored latest-authorization copy session_dir",
        mismatches,
    );
    compare_option_string(
        stored.result.as_deref(),
        actual.result.as_deref(),
        "stored latest-authorization copy result",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_verdict.as_deref(),
        actual.current_pre_activation_gate_verdict.as_deref(),
        "stored latest-authorization copy current_pre_activation_gate_verdict",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_reason.as_deref(),
        actual.current_pre_activation_gate_reason.as_deref(),
        "stored latest-authorization copy current_pre_activation_gate_reason",
        mismatches,
    );
}

fn compare_launch_packet_plan_against_snapshot(
    stored: &LaunchPacketReportView,
    snapshot: &LatestLaunchPacketSnapshot,
    session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        Some(stored.verdict.as_str()),
        Some(LAUNCH_PACKET_PLAN_READY),
        "stored launch-packet plan verdict",
        mismatches,
    );
    compare_string(
        &stored.package_dir,
        &snapshot.package_dir.display().to_string(),
        "stored launch-packet plan package_dir",
        mismatches,
    );
    compare_string(
        &stored.install_root,
        &snapshot.install_root.display().to_string(),
        "stored launch-packet plan install_root",
        mismatches,
    );
    compare_string(
        &stored.target_service_name,
        &snapshot.target_service_name,
        "stored launch-packet plan target_service_name",
        mismatches,
    );
    compare_string(
        &stored.backend_command,
        &snapshot.backend_command,
        "stored launch-packet plan backend_command",
        mismatches,
    );
    if stored.wrapper_timeout_ms != snapshot.wrapper_timeout_ms {
        mismatches.push(format!(
            "stored launch-packet plan wrapper_timeout_ms {} does not match expected {}",
            stored.wrapper_timeout_ms, snapshot.wrapper_timeout_ms
        ));
    }
    if stored.service_status_max_staleness_ms != snapshot.service_status_max_staleness_ms {
        mismatches.push(format!(
            "stored launch-packet plan service_status_max_staleness_ms {} does not match expected {}",
            stored.service_status_max_staleness_ms, snapshot.service_status_max_staleness_ms
        ));
    }
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored launch-packet plan session_dir",
        mismatches,
    );
    compare_option_string(
        stored.nested_authorization_session_dir.as_deref(),
        Some(
            launch_packet_nested_authorization_session_dir(session_dir)
                .display()
                .to_string()
                .as_str(),
        ),
        "stored launch-packet plan nested_authorization_session_dir",
        mismatches,
    );
    compare_string(
        &stored.run_live_authorization_command_summary,
        &launch_packet_nested_authorization_command_summary(snapshot, session_dir),
        "stored launch-packet plan run_live_authorization_command_summary",
        mismatches,
    );
    compare_string(
        &stored.frozen_live_cutover_controller_command_summary,
        &snapshot.reviewed_frozen_live_cutover_controller_command_summary,
        "stored launch-packet plan frozen_live_cutover_controller_command_summary",
        mismatches,
    );
}

fn compare_launch_packet_run_against_snapshot(
    stored: &LaunchPacketReportView,
    snapshot: &LatestLaunchPacketSnapshot,
    session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &stored.package_dir,
        &snapshot.package_dir.display().to_string(),
        "stored launch-packet run package_dir",
        mismatches,
    );
    compare_string(
        &stored.install_root,
        &snapshot.install_root.display().to_string(),
        "stored launch-packet run install_root",
        mismatches,
    );
    compare_string(
        &stored.target_service_name,
        &snapshot.target_service_name,
        "stored launch-packet run target_service_name",
        mismatches,
    );
    compare_string(
        &stored.backend_command,
        &snapshot.backend_command,
        "stored launch-packet run backend_command",
        mismatches,
    );
    if stored.wrapper_timeout_ms != snapshot.wrapper_timeout_ms {
        mismatches.push(format!(
            "stored launch-packet run wrapper_timeout_ms {} does not match expected {}",
            stored.wrapper_timeout_ms, snapshot.wrapper_timeout_ms
        ));
    }
    if stored.service_status_max_staleness_ms != snapshot.service_status_max_staleness_ms {
        mismatches.push(format!(
            "stored launch-packet run service_status_max_staleness_ms {} does not match expected {}",
            stored.service_status_max_staleness_ms, snapshot.service_status_max_staleness_ms
        ));
    }
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored launch-packet run session_dir",
        mismatches,
    );
    compare_option_string(
        stored.nested_authorization_session_dir.as_deref(),
        Some(
            launch_packet_nested_authorization_session_dir(session_dir)
                .display()
                .to_string()
                .as_str(),
        ),
        "stored launch-packet run nested_authorization_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.launch_packet_session_path.as_deref(),
        Some(
            session_dir
                .join("tiny_live_activation_package_launch_packet.session.json")
                .display()
                .to_string()
                .as_str(),
        ),
        "stored launch-packet run launch_packet_session_path",
        mismatches,
    );
    compare_option_string(
        stored.launch_packet_status_path.as_deref(),
        Some(
            session_dir
                .join("tiny_live_activation_package_launch_packet.status.json")
                .display()
                .to_string()
                .as_str(),
        ),
        "stored launch-packet run launch_packet_status_path",
        mismatches,
    );
    compare_string(
        &stored.run_live_authorization_command_summary,
        &launch_packet_nested_authorization_command_summary(snapshot, session_dir),
        "stored launch-packet run run_live_authorization_command_summary",
        mismatches,
    );
    compare_string(
        &stored.frozen_live_cutover_controller_command_summary,
        &snapshot.reviewed_frozen_live_cutover_controller_command_summary,
        "stored launch-packet run frozen_live_cutover_controller_command_summary",
        mismatches,
    );
}

fn compare_launch_packet_copy_matches_actual(
    stored: &LaunchPacketReportView,
    actual: &LaunchPacketReportView,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        Some(stored.verdict.as_str()),
        Some(actual.verdict.as_str()),
        "stored launch-packet copy verdict",
        mismatches,
    );
    compare_option_string(
        Some(stored.reason.as_str()),
        Some(actual.reason.as_str()),
        "stored launch-packet copy reason",
        mismatches,
    );
    if stored.generated_at != actual.generated_at {
        mismatches.push(format!(
            "stored launch-packet copy generated_at {:?} does not match actual {:?}",
            stored.generated_at, actual.generated_at
        ));
    }
    compare_option_string(
        stored.session_dir.as_deref(),
        actual.session_dir.as_deref(),
        "stored launch-packet copy session_dir",
        mismatches,
    );
    compare_option_string(
        stored.nested_authorization_session_dir.as_deref(),
        actual.nested_authorization_session_dir.as_deref(),
        "stored launch-packet copy nested_authorization_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.result.as_deref(),
        actual.result.as_deref(),
        "stored launch-packet copy result",
        mismatches,
    );
    compare_option_string(
        stored.authorization_result_now.as_deref(),
        actual.authorization_result_now.as_deref(),
        "stored launch-packet copy authorization_result_now",
        mismatches,
    );
    compare_string(
        &stored.operator_next_action_summary,
        &actual.operator_next_action_summary,
        "stored launch-packet copy operator_next_action_summary",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_verdict.as_deref(),
        actual.current_pre_activation_gate_verdict.as_deref(),
        "stored launch-packet copy current_pre_activation_gate_verdict",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_reason.as_deref(),
        actual.current_pre_activation_gate_reason.as_deref(),
        "stored launch-packet copy current_pre_activation_gate_reason",
        mismatches,
    );
}

fn compare_session_against_snapshot(
    session: &PackageLaunchPacketLatestSession,
    root: &Path,
    session_dir: &Path,
    paths: &PackageLaunchPacketLatestPaths,
    snapshot: &LatestLaunchPacketSnapshot,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &session.resolve_latest_authorization_command_summary,
        &resolve_latest_authorization_command_summary(
            root,
            &paths.nested_authorize_latest_session_dir,
        ),
        "stored session resolve_latest_authorization_command_summary",
        mismatches,
    );
    compare_string(
        &session.run_latest_authorization_command_summary,
        &run_latest_authorization_command_summary(root, &paths.nested_authorize_latest_session_dir),
        "stored session run_latest_authorization_command_summary",
        mismatches,
    );
    compare_string(
        &session.verify_latest_authorization_command_summary,
        &verify_latest_authorization_command_summary(&paths.nested_authorize_latest_session_dir),
        "stored session verify_latest_authorization_command_summary",
        mismatches,
    );
    compare_string(
        &session.plan_launch_packet_command_summary,
        &plan_launch_packet_command_summary(snapshot, &paths.nested_launch_packet_session_dir),
        "stored session plan_launch_packet_command_summary",
        mismatches,
    );
    compare_string(
        &session.downstream_launch_packet_command_summary,
        &downstream_launch_packet_command_summary(
            snapshot,
            &paths.nested_launch_packet_session_dir,
        ),
        "stored session downstream_launch_packet_command_summary",
        mismatches,
    );
    compare_string(
        &session.verify_launch_packet_command_summary,
        &verify_launch_packet_command_summary(snapshot, &paths.nested_launch_packet_session_dir),
        "stored session verify_launch_packet_command_summary",
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
        session.historical_launch_packet_session_dir.as_deref(),
        Some(
            snapshot
                .historical_launch_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored session historical_launch_packet_session_dir",
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
        &session.nested_authorize_latest_session_dir,
        &paths
            .nested_authorize_latest_session_dir
            .display()
            .to_string(),
        "stored session nested_authorize_latest_session_dir",
        mismatches,
    );
    compare_string(
        &session.nested_launch_packet_session_dir,
        &paths.nested_launch_packet_session_dir.display().to_string(),
        "stored session nested_launch_packet_session_dir",
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
    report: &PackageLaunchPacketLatestReport,
    root: &Path,
    session_dir: &Path,
    paths: &PackageLaunchPacketLatestPaths,
    snapshot: &LatestLaunchPacketSnapshot,
    mismatches: &mut Vec<String>,
) {
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
        report.historical_launch_packet_session_dir.as_deref(),
        Some(
            snapshot
                .historical_launch_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report historical_launch_packet_session_dir",
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
            .resolve_latest_authorization_command_summary
            .as_deref(),
        Some(
            resolve_latest_authorization_command_summary(
                root,
                &paths.nested_authorize_latest_session_dir,
            )
            .as_str(),
        ),
        "stored report resolve_latest_authorization_command_summary",
        mismatches,
    );
    compare_option_string(
        report.run_latest_authorization_command_summary.as_deref(),
        Some(
            run_latest_authorization_command_summary(
                root,
                &paths.nested_authorize_latest_session_dir,
            )
            .as_str(),
        ),
        "stored report run_latest_authorization_command_summary",
        mismatches,
    );
    compare_option_string(
        report
            .verify_latest_authorization_command_summary
            .as_deref(),
        Some(
            verify_latest_authorization_command_summary(&paths.nested_authorize_latest_session_dir)
                .as_str(),
        ),
        "stored report verify_latest_authorization_command_summary",
        mismatches,
    );
    compare_option_string(
        report.plan_launch_packet_command_summary.as_deref(),
        Some(
            plan_launch_packet_command_summary(snapshot, &paths.nested_launch_packet_session_dir)
                .as_str(),
        ),
        "stored report plan_launch_packet_command_summary",
        mismatches,
    );
    compare_option_string(
        report.downstream_launch_packet_command_summary.as_deref(),
        Some(
            downstream_launch_packet_command_summary(
                snapshot,
                &paths.nested_launch_packet_session_dir,
            )
            .as_str(),
        ),
        "stored report downstream_launch_packet_command_summary",
        mismatches,
    );
    compare_option_string(
        report.verify_launch_packet_command_summary.as_deref(),
        Some(
            verify_launch_packet_command_summary(snapshot, &paths.nested_launch_packet_session_dir)
                .as_str(),
        ),
        "stored report verify_launch_packet_command_summary",
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
        report.launch_packet_latest_session_path.as_deref(),
        Some(paths.session_path.display().to_string().as_str()),
        "stored report launch_packet_latest_session_path",
        mismatches,
    );
    compare_option_string(
        report.launch_packet_latest_status_path.as_deref(),
        Some(paths.status_path.display().to_string().as_str()),
        "stored report launch_packet_latest_status_path",
        mismatches,
    );
    compare_option_string(
        report.launch_packet_latest_report_path.as_deref(),
        Some(paths.report_path.display().to_string().as_str()),
        "stored report launch_packet_latest_report_path",
        mismatches,
    );
    compare_option_string(
        report.latest_authorization_plan_report_path.as_deref(),
        Some(
            paths
                .latest_authorization_plan_report_path
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report latest_authorization_plan_report_path",
        mismatches,
    );
    compare_option_string(
        report.latest_authorization_report_path.as_deref(),
        Some(
            paths
                .latest_authorization_report_path
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report latest_authorization_report_path",
        mismatches,
    );
    compare_option_string(
        report.launch_packet_plan_report_path.as_deref(),
        Some(
            paths
                .launch_packet_plan_report_path
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report launch_packet_plan_report_path",
        mismatches,
    );
    compare_option_string(
        report.launch_packet_report_path.as_deref(),
        Some(
            paths
                .launch_packet_report_path
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report launch_packet_report_path",
        mismatches,
    );
    compare_option_string(
        report.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored report session_dir",
        mismatches,
    );
    compare_string(
        &report.mode,
        "run_latest_launch_packet",
        "stored report mode",
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
    session: &PackageLaunchPacketLatestSession,
    root: &Path,
    session_dir: &Path,
    paths: &PackageLaunchPacketLatestPaths,
    failure: &LatestLaunchPacketResolutionFailure,
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
        &session.nested_authorize_latest_session_dir,
        &paths
            .nested_authorize_latest_session_dir
            .display()
            .to_string(),
        "stored session nested_authorize_latest_session_dir on failure",
        mismatches,
    );
    compare_string(
        &session.nested_launch_packet_session_dir,
        &paths.nested_launch_packet_session_dir.display().to_string(),
        "stored session nested_launch_packet_session_dir on failure",
        mismatches,
    );
    if let Some(snapshot) = failure.snapshot.as_ref() {
        compare_session_against_snapshot(session, root, session_dir, paths, snapshot, mismatches);
    }
}

fn compare_report_against_failure(
    report: &PackageLaunchPacketLatestReport,
    root: &Path,
    session_dir: &Path,
    paths: &PackageLaunchPacketLatestPaths,
    failure: &LatestLaunchPacketResolutionFailure,
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
        "run_latest_launch_packet",
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
        .ok_or_else(|| anyhow!("missing required {}", label))
}

fn map_authorize_latest_failure_kind(verdict: &str) -> ResolutionFailureKind {
    match verdict {
        "tiny_live_package_authorize_latest_refused_by_missing_latest_chain" => {
            ResolutionFailureKind::MissingLatestChain
        }
        "tiny_live_package_authorize_latest_refused_by_drifted_authorization_contract" => {
            ResolutionFailureKind::DriftedLaunchContract
        }
        _ => ResolutionFailureKind::InvalidLatestChain,
    }
}

fn launch_packet_latest_result_from_launch_packet(
    result: Option<&str>,
) -> Option<LivePackageLaunchPacketLatestResult> {
    match result {
        Some("eligible_when_gate_turns_green") => {
            Some(LivePackageLaunchPacketLatestResult::Completed)
        }
        Some("refused_by_stage3") => Some(LivePackageLaunchPacketLatestResult::RefusedNowByStage3),
        Some("refused_by_pre_activation_gate") => {
            Some(LivePackageLaunchPacketLatestResult::RefusedNowByPreActivationGate)
        }
        Some("refused_by_invalid_target") => {
            Some(LivePackageLaunchPacketLatestResult::RefusedByDriftedLaunchContract)
        }
        _ => None,
    }
}

fn result_from_failure_kind(kind: ResolutionFailureKind) -> LivePackageLaunchPacketLatestResult {
    match kind {
        ResolutionFailureKind::MissingLatestChain => {
            LivePackageLaunchPacketLatestResult::RefusedByMissingLatestChain
        }
        ResolutionFailureKind::InvalidLatestChain => {
            LivePackageLaunchPacketLatestResult::RefusedByInvalidLatestChain
        }
        ResolutionFailureKind::DriftedLaunchContract => {
            LivePackageLaunchPacketLatestResult::RefusedByDriftedLaunchContract
        }
    }
}

fn verdict_for_result(
    result: LivePackageLaunchPacketLatestResult,
) -> TinyLivePackageLaunchPacketLatestVerdict {
    match result {
        LivePackageLaunchPacketLatestResult::RefusedByMissingLatestChain => {
            TinyLivePackageLaunchPacketLatestVerdict::TinyLivePackageLaunchPacketLatestRefusedByMissingLatestChain
        }
        LivePackageLaunchPacketLatestResult::RefusedByInvalidLatestChain => {
            TinyLivePackageLaunchPacketLatestVerdict::TinyLivePackageLaunchPacketLatestRefusedByInvalidLatestChain
        }
        LivePackageLaunchPacketLatestResult::RefusedByDriftedLaunchContract => {
            TinyLivePackageLaunchPacketLatestVerdict::TinyLivePackageLaunchPacketLatestRefusedByDriftedLaunchContract
        }
        LivePackageLaunchPacketLatestResult::RefusedNowByStage3 => {
            TinyLivePackageLaunchPacketLatestVerdict::TinyLivePackageLaunchPacketLatestRefusedNowByStage3
        }
        LivePackageLaunchPacketLatestResult::RefusedNowByPreActivationGate => {
            TinyLivePackageLaunchPacketLatestVerdict::TinyLivePackageLaunchPacketLatestRefusedNowByPreActivationGate
        }
        LivePackageLaunchPacketLatestResult::Completed => {
            TinyLivePackageLaunchPacketLatestVerdict::TinyLivePackageLaunchPacketLatestCompleted
        }
    }
}

fn verdict_from_failure_kind(
    kind: ResolutionFailureKind,
) -> TinyLivePackageLaunchPacketLatestVerdict {
    verdict_for_result(result_from_failure_kind(kind))
}

fn operator_next_action_summary(
    result: LivePackageLaunchPacketLatestResult,
    frozen_controller_command_summary: Option<&str>,
) -> String {
    match result {
        LivePackageLaunchPacketLatestResult::RefusedByMissingLatestChain => {
            "no immutable clerestory chain is available yet; do not manually hunt authorization or launch lineage by hand".to_string()
        }
        LivePackageLaunchPacketLatestResult::RefusedByInvalidLatestChain => {
            "the latest immutable chain is incomplete or below the accepted clerestory layer; mint a fresh coherent clerestory chain instead of bypassing the accepted authorization/launch path".to_string()
        }
        LivePackageLaunchPacketLatestResult::RefusedByDriftedLaunchContract => {
            "the latest chain no longer resolves cleanly into the accepted authorization/launch contract; repair that drift instead of bypassing the bounded handoff path".to_string()
        }
        LivePackageLaunchPacketLatestResult::RefusedNowByStage3 => format!(
            "today refused by current Stage 3 truth; do not run live cutover now. Once current Stage 3 / pre-activation truth genuinely turns green, the frozen bounded controller command remains {}",
            frozen_controller_command_summary.unwrap_or("<unavailable>")
        ),
        LivePackageLaunchPacketLatestResult::RefusedNowByPreActivationGate => format!(
            "today refused by the current pre-activation gate; do not run live cutover now. Once current Stage 3 / pre-activation truth genuinely turns green, the frozen bounded controller command remains {}",
            frozen_controller_command_summary.unwrap_or("<unavailable>")
        ),
        LivePackageLaunchPacketLatestResult::Completed => format!(
            "the latest immutable chain is now frozen into the accepted launch-packet contract. The exact bounded next step remains {} if and only if current Stage 3 / pre-activation truth is genuinely green; this wrapper still did not execute live cutover.",
            frozen_controller_command_summary.unwrap_or("<unavailable>")
        ),
    }
}

fn launch_packet_nested_authorization_command_summary(
    snapshot: &LatestLaunchPacketSnapshot,
    session_dir: &Path,
) -> String {
    format!(
        "copybot_tiny_live_activation_package_live_authorization --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --service-status-max-staleness-ms {} --session-dir {} --run-live-package-authorization",
        shell_escape_path(&snapshot.package_dir),
        shell_escape_path(&snapshot.install_root),
        shell_escape_string(&snapshot.target_service_name),
        shell_escape_string(&snapshot.backend_command),
        snapshot.wrapper_timeout_ms,
        snapshot.service_status_max_staleness_ms,
        shell_escape_path(&launch_packet_nested_authorization_session_dir(session_dir)),
    )
}

fn serialize_enum<T: Serialize>(value: &T) -> String {
    match serde_json::to_value(value) {
        Ok(Value::String(raw)) => raw,
        Ok(other) => other.to_string(),
        Err(_) => "<serialization-error>".to_string(),
    }
}

fn render_report_lines(report: &PackageLaunchPacketLatestReport) -> String {
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
    if let Some(value) = &report.historical_launch_packet_session_dir {
        lines.push(format!("historical_launch_packet_session_dir={value}"));
    }
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
    if let Some(value) = &report.session_dir {
        lines.push(format!("session_dir={value}"));
    }
    if let Some(value) = &report.nested_authorize_latest_session_dir {
        lines.push(format!("nested_authorize_latest_session_dir={value}"));
    }
    if let Some(value) = &report.nested_launch_packet_session_dir {
        lines.push(format!("nested_launch_packet_session_dir={value}"));
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
    if let Some(value) = &report.downstream_launch_packet_command_summary {
        lines.push(format!("downstream_launch_packet_command_summary={value}"));
    }
    if let Some(value) = &report.reviewed_frozen_live_cutover_controller_command_summary {
        lines.push(format!(
            "reviewed_frozen_live_cutover_controller_command_summary={value}"
        ));
    }
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
    use serde_json::json;
    use std::ffi::OsString;
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    struct FakeLaunchPacketLatestFixture {
        temp_dir: PathBuf,
        root: PathBuf,
        wrapper_session_dir: PathBuf,
        bin_dir: PathBuf,
        latest_top_session_dir: PathBuf,
        historical_launch_packet_session_dir: PathBuf,
        package_dir: PathBuf,
        install_root: PathBuf,
        backend_command: PathBuf,
        authorize_plan_json_path: PathBuf,
        authorize_run_json_path: PathBuf,
        authorize_verify_json_path: PathBuf,
        launch_plan_json_path: PathBuf,
        launch_run_json_path: PathBuf,
        launch_verify_json_path: PathBuf,
        frozen_controller_command: String,
    }

    impl FakeLaunchPacketLatestFixture {
        fn plan_config(&self) -> Config {
            Config {
                mode: Mode::PlanLatestLaunchPacket,
                root: Some(self.root.clone()),
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn render_config(&self, output_path: &Path) -> Config {
            Config {
                mode: Mode::RenderLatestLaunchPacketScript,
                root: Some(self.root.clone()),
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: Some(output_path.to_path_buf()),
                json: true,
            }
        }

        fn run_config(&self) -> Config {
            Config {
                mode: Mode::RunLatestLaunchPacket,
                root: Some(self.root.clone()),
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn verify_config(&self) -> Config {
            Config {
                mode: Mode::VerifyLatestLaunchPacket,
                root: None,
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }
    }

    #[test]
    fn plan_mode_resolves_latest_valid_clerestory_chain_to_expected_nested_latest_authorization_lineage(
    ) {
        let fixture =
            fake_launch_packet_latest_fixture("launch_packet_latest_plan_ok", "authorized_now");
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let report = plan_latest_launch_packet_report(&fixture.plan_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageLaunchPacketLatestVerdict::TinyLivePackageLaunchPacketLatestPlanReady
        );
        assert_eq!(
            report.latest_top_step_name.as_deref(),
            Some("clerestory_certificate")
        );
        assert_eq!(
            report.nested_authorize_latest_session_dir.as_deref(),
            Some(
                launch_packet_latest_paths(&fixture.wrapper_session_dir)
                    .nested_authorize_latest_session_dir
                    .display()
                    .to_string()
                    .as_str()
            )
        );
        assert_eq!(
            report.downstream_launch_packet_command_summary.as_deref(),
            Some(
                downstream_launch_packet_command_summary(
                    &LatestLaunchPacketSnapshot {
                        latest_top_step_name: "clerestory_certificate".to_string(),
                        latest_top_session_dir: fixture.latest_top_session_dir.clone(),
                        historical_launch_packet_session_dir: fixture
                            .historical_launch_packet_session_dir
                            .clone(),
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
                    },
                    &launch_packet_latest_paths(&fixture.wrapper_session_dir)
                        .nested_launch_packet_session_dir,
                )
                .as_str()
            )
        );
    }

    #[test]
    fn render_mode_emits_exact_downstream_launch_packet_contract_not_second_controller_path() {
        let fixture =
            fake_launch_packet_latest_fixture("launch_packet_latest_render_ok", "authorized_now");
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        let output_path = fixture.temp_dir.join("launch-packet-latest.sh");

        let report =
            render_latest_launch_packet_script_report(&fixture.render_config(&output_path))
                .unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageLaunchPacketLatestVerdict::TinyLivePackageLaunchPacketLatestScriptRendered
        );
        let script = fs::read_to_string(&output_path).unwrap();
        assert!(script.contains(LAUNCH_PACKET_BIN));
        assert!(script.contains("--run-live-package-launch-packet"));
        assert!(script.contains("--verify-live-package-launch-packet"));
        assert!(!script.contains("live_cutover"));
    }

    #[test]
    fn run_mode_refuses_when_latest_chain_missing_invalid_or_below_clerestory() {
        for (label, verdict, expected) in [
            (
                "launch_packet_latest_missing",
                "tiny_live_package_authorize_latest_refused_by_missing_latest_chain",
                TinyLivePackageLaunchPacketLatestVerdict::TinyLivePackageLaunchPacketLatestRefusedByMissingLatestChain,
            ),
            (
                "launch_packet_latest_invalid",
                "tiny_live_package_authorize_latest_refused_by_invalid_latest_chain",
                TinyLivePackageLaunchPacketLatestVerdict::TinyLivePackageLaunchPacketLatestRefusedByInvalidLatestChain,
            ),
            (
                "launch_packet_latest_below",
                "tiny_live_package_authorize_latest_refused_by_invalid_latest_chain",
                TinyLivePackageLaunchPacketLatestVerdict::TinyLivePackageLaunchPacketLatestRefusedByInvalidLatestChain,
            ),
        ] {
            let fixture = fake_launch_packet_latest_fixture(label, "authorized_now");
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            let mut plan: Value = load_json(&fixture.authorize_plan_json_path).unwrap();
            plan["verdict"] = json!(verdict);
            plan["reason"] = if label.ends_with("below") {
                json!("latest persisted top chain is choir_receipt, which is below clerestory_certificate")
            } else {
                json!("latest chain unavailable or invalid")
            };
            if label.ends_with("below") {
                plan["latest_top_step_name"] = json!("choir_receipt");
            }
            persist_json(&fixture.authorize_plan_json_path, &plan).unwrap();

            let report = run_latest_launch_packet_report(&fixture.run_config()).unwrap();
            assert_eq!(report.verdict, expected);
        }
    }

    #[test]
    fn run_mode_refuses_when_nested_authorization_lineage_drifts_from_latest_immutable_chain() {
        let fixture =
            fake_launch_packet_latest_fixture("launch_packet_latest_auth_drift", "authorized_now");
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let mut run: Value = load_json(&fixture.authorize_run_json_path).unwrap();
        run["package_dir"] = json!("/tampered/package");
        persist_json(&fixture.authorize_run_json_path, &run).unwrap();

        let report = run_latest_launch_packet_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageLaunchPacketLatestVerdict::TinyLivePackageLaunchPacketLatestRefusedByDriftedLaunchContract
        );
        assert!(report
            .reason
            .contains("stored latest-authorization run package_dir"));
    }

    #[test]
    fn verify_mode_fails_when_persisted_latest_launch_packet_artifacts_are_tampered_or_missing_nested_lineage_truth(
    ) {
        {
            let fixture = fake_launch_packet_latest_fixture(
                "launch_packet_latest_verify_tampered_copy",
                "authorized_now",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_latest_launch_packet_report(&fixture.run_config()).unwrap();

            let paths = launch_packet_latest_paths(&fixture.wrapper_session_dir);
            let mut report: Value = load_json(&paths.launch_packet_report_path).unwrap();
            report["reason"] = json!("tampered stored launch-packet copy reason");
            persist_json(&paths.launch_packet_report_path, &report).unwrap();

            let verify = verify_latest_launch_packet_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageLaunchPacketLatestVerdict::TinyLivePackageLaunchPacketLatestVerifyInvalid
            );
            assert!(verify
                .verification_mismatches
                .iter()
                .any(|entry| { entry.contains("stored launch-packet copy reason") }));
        }

        {
            let fixture = fake_launch_packet_latest_fixture(
                "launch_packet_latest_verify_missing_nested_truth",
                "authorized_now",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_latest_launch_packet_report(&fixture.run_config()).unwrap();

            let paths = launch_packet_latest_paths(&fixture.wrapper_session_dir);
            fs::remove_file(authorize_latest_nested_report_path(
                &paths.nested_authorize_latest_session_dir,
            ))
            .unwrap();

            let verify = verify_latest_launch_packet_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageLaunchPacketLatestVerdict::TinyLivePackageLaunchPacketLatestVerifyInvalid
            );
            assert!(verify.verification_mismatches.iter().any(|entry| {
                entry.contains("tiny_live_activation_package_authorize_latest.report.json")
            }));
        }
    }

    #[test]
    fn verify_fails_when_stored_report_verdict_drifts() {
        let fixture = fake_launch_packet_latest_fixture(
            "launch_packet_latest_report_verdict_tamper",
            "authorized_now",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_launch_packet_report(&fixture.run_config()).unwrap();

        let paths = launch_packet_latest_paths(&fixture.wrapper_session_dir);
        let mut report: PackageLaunchPacketLatestReport = load_json(&paths.report_path).unwrap();
        report.verdict =
            TinyLivePackageLaunchPacketLatestVerdict::TinyLivePackageLaunchPacketLatestVerifyOk;
        persist_json(&paths.report_path, &report).unwrap();

        let verify = verify_latest_launch_packet_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageLaunchPacketLatestVerdict::TinyLivePackageLaunchPacketLatestVerifyInvalid
        );
        assert!(verify
            .verification_mismatches
            .iter()
            .any(|entry| entry.contains("stored report verdict")));
    }

    #[test]
    fn verify_fails_when_stored_report_explicit_statement_drifts() {
        let fixture = fake_launch_packet_latest_fixture(
            "launch_packet_latest_report_explicit_statement_tamper",
            "authorized_now",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_launch_packet_report(&fixture.run_config()).unwrap();

        let paths = launch_packet_latest_paths(&fixture.wrapper_session_dir);
        let mut report: PackageLaunchPacketLatestReport = load_json(&paths.report_path).unwrap();
        report.explicit_statement = "tampered wrapper report explicit statement".to_string();
        persist_json(&paths.report_path, &report).unwrap();

        let verify = verify_latest_launch_packet_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageLaunchPacketLatestVerdict::TinyLivePackageLaunchPacketLatestVerifyInvalid
        );
        assert!(verify
            .verification_mismatches
            .iter()
            .any(|entry| entry.contains("stored report explicit_statement")));
    }

    #[test]
    fn verify_fails_when_stored_report_republished_verdict_fields_drift() {
        let fixture = fake_launch_packet_latest_fixture(
            "launch_packet_latest_report_republished_verdict_tamper",
            "authorized_now",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_launch_packet_report(&fixture.run_config()).unwrap();

        let paths = launch_packet_latest_paths(&fixture.wrapper_session_dir);
        let mut report: PackageLaunchPacketLatestReport = load_json(&paths.report_path).unwrap();
        report.latest_authorization_verdict =
            Some("tampered_latest_authorization_verdict".to_string());
        report.launch_packet_plan_verdict = Some("tampered_launch_packet_plan_verdict".to_string());
        persist_json(&paths.report_path, &report).unwrap();

        let verify = verify_latest_launch_packet_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageLaunchPacketLatestVerdict::TinyLivePackageLaunchPacketLatestVerifyInvalid
        );
        assert!(verify.verification_mismatches.iter().any(|entry| {
            entry.contains("stored report latest_authorization_verdict")
                || entry.contains("stored report launch_packet_plan_verdict")
        }));
    }

    #[test]
    fn verify_fails_when_stored_report_latest_authorization_plan_verdict_drifts() {
        let fixture = fake_launch_packet_latest_fixture(
            "launch_packet_latest_report_latest_authorization_plan_verdict_tamper",
            "authorized_now",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_launch_packet_report(&fixture.run_config()).unwrap();

        let paths = launch_packet_latest_paths(&fixture.wrapper_session_dir);
        let mut report: PackageLaunchPacketLatestReport = load_json(&paths.report_path).unwrap();
        report.latest_authorization_plan_verdict =
            Some("tampered_latest_authorization_plan_verdict".to_string());
        persist_json(&paths.report_path, &report).unwrap();

        let verify = verify_latest_launch_packet_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageLaunchPacketLatestVerdict::TinyLivePackageLaunchPacketLatestVerifyInvalid
        );
        assert!(verify
            .verification_mismatches
            .iter()
            .any(|entry| entry.contains("stored report latest_authorization_plan_verdict")));
    }

    #[test]
    fn verify_fails_when_stored_status_explicit_statement_drifts() {
        let fixture = fake_launch_packet_latest_fixture(
            "launch_packet_latest_status_explicit_statement_tamper",
            "authorized_now",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_launch_packet_report(&fixture.run_config()).unwrap();

        let paths = launch_packet_latest_paths(&fixture.wrapper_session_dir);
        let mut status: PackageLaunchPacketLatestStatus = load_json(&paths.status_path).unwrap();
        status.explicit_statement = "tampered wrapper status explicit statement".to_string();
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_latest_launch_packet_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageLaunchPacketLatestVerdict::TinyLivePackageLaunchPacketLatestVerifyInvalid
        );
        assert!(verify
            .verification_mismatches
            .iter()
            .any(|entry| entry.contains("stored status explicit_statement")));
    }

    #[test]
    fn all_modes_preserve_stage3_and_pre_activation_refusal_semantics_and_do_not_imply_activation_authorization_by_themselves(
    ) {
        {
            let fixture = fake_launch_packet_latest_fixture(
                "launch_packet_latest_stage3",
                "refused_by_stage3",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);

            let plan = plan_latest_launch_packet_report(&fixture.plan_config()).unwrap();
            assert_eq!(
                plan.verdict,
                TinyLivePackageLaunchPacketLatestVerdict::TinyLivePackageLaunchPacketLatestPlanReady
            );

            let run = run_latest_launch_packet_report(&fixture.run_config()).unwrap();
            assert_eq!(
                run.verdict,
                TinyLivePackageLaunchPacketLatestVerdict::TinyLivePackageLaunchPacketLatestRefusedNowByStage3
            );
            assert_eq!(run.result.as_deref(), Some("refused_now_by_stage3"));

            let verify = verify_latest_launch_packet_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageLaunchPacketLatestVerdict::TinyLivePackageLaunchPacketLatestVerifyOk
            );
        }

        {
            let fixture = fake_launch_packet_latest_fixture(
                "launch_packet_latest_pre_activation",
                "refused_by_pre_activation_gate",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);

            let run = run_latest_launch_packet_report(&fixture.run_config()).unwrap();
            assert_eq!(
                run.verdict,
                TinyLivePackageLaunchPacketLatestVerdict::TinyLivePackageLaunchPacketLatestRefusedNowByPreActivationGate
            );
            assert_eq!(
                run.result.as_deref(),
                Some("refused_now_by_pre_activation_gate")
            );

            let verify = verify_latest_launch_packet_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageLaunchPacketLatestVerdict::TinyLivePackageLaunchPacketLatestVerifyOk
            );
        }

        {
            let fixture =
                fake_launch_packet_latest_fixture("launch_packet_latest_green", "authorized_now");
            let _path_guard = PathGuard::install(&fixture.bin_dir);

            let run = run_latest_launch_packet_report(&fixture.run_config()).unwrap();
            assert_eq!(
                run.verdict,
                TinyLivePackageLaunchPacketLatestVerdict::TinyLivePackageLaunchPacketLatestCompleted
            );
            assert_eq!(run.result.as_deref(), Some("completed"));
            assert!(run
                .explicit_statement
                .contains("never executes live cutover"));
        }
    }

    fn fake_launch_packet_latest_fixture(
        label: &str,
        authorize_run_result: &str,
    ) -> FakeLaunchPacketLatestFixture {
        let temp_dir = temp_dir(label);
        let root = temp_dir.join("latest-root");
        let wrapper_session_dir = temp_dir.join("launch-packet-latest-session");
        let bin_dir = temp_dir.join("bin");
        let latest_top_session_dir = temp_dir.join("latest-clerestory-session");
        let historical_launch_packet_session_dir =
            temp_dir.join("historical-launch-packet-session");
        let package_dir = temp_dir.join("package");
        let install_root = temp_dir.join("install-root");
        let backend_command = temp_dir.join("backend-bin");
        let authorize_plan_json_path = temp_dir.join("authorize-plan.json");
        let authorize_run_json_path = temp_dir.join("authorize-run.json");
        let authorize_verify_json_path = temp_dir.join("authorize-verify.json");
        let launch_plan_json_path = temp_dir.join("launch-plan.json");
        let launch_run_json_path = temp_dir.join("launch-run.json");
        let launch_verify_json_path = temp_dir.join("launch-verify.json");
        let frozen_controller_command =
            "copybot_tiny_live_activation_live_cutover --run-live-cutover".to_string();

        fs::create_dir_all(&root).unwrap();
        fs::create_dir_all(&bin_dir).unwrap();
        fs::create_dir_all(&package_dir).unwrap();
        fs::create_dir_all(&install_root).unwrap();
        fs::write(&backend_command, "#!/bin/sh\nexit 0\n").unwrap();

        let fixture = FakeLaunchPacketLatestFixture {
            temp_dir,
            root,
            wrapper_session_dir,
            bin_dir,
            latest_top_session_dir,
            historical_launch_packet_session_dir,
            package_dir,
            install_root,
            backend_command,
            authorize_plan_json_path,
            authorize_run_json_path,
            authorize_verify_json_path,
            launch_plan_json_path,
            launch_run_json_path,
            launch_verify_json_path,
            frozen_controller_command,
        };

        persist_json(
            &fixture.authorize_plan_json_path,
            &default_authorize_latest_plan_report(&fixture),
        )
        .unwrap();
        persist_json(
            &fixture.authorize_run_json_path,
            &default_authorize_latest_run_report(&fixture, authorize_run_result),
        )
        .unwrap();
        persist_json(
            &fixture.authorize_verify_json_path,
            &default_authorize_latest_verify_report(&fixture, authorize_run_result),
        )
        .unwrap();
        persist_json(
            &fixture.launch_plan_json_path,
            &default_launch_packet_plan_report(&fixture),
        )
        .unwrap();
        persist_json(
            &fixture.launch_run_json_path,
            &default_launch_packet_run_report(&fixture, authorize_run_result),
        )
        .unwrap();
        persist_json(
            &fixture.launch_verify_json_path,
            &default_launch_packet_verify_report(&fixture, authorize_run_result),
        )
        .unwrap();

        write_fake_authorize_latest_command(
            &fixture.bin_dir.join(AUTHORIZE_LATEST_BIN),
            &fixture.authorize_plan_json_path,
            &fixture.authorize_run_json_path,
            &fixture.authorize_verify_json_path,
            &fixture.root,
        );
        write_fake_launch_packet_command(
            &fixture.bin_dir.join(LAUNCH_PACKET_BIN),
            &fixture.launch_plan_json_path,
            &fixture.launch_run_json_path,
            &fixture.launch_verify_json_path,
            &fixture.package_dir,
            &fixture.install_root,
            "solana-copy-bot.service",
            &fixture.backend_command,
            1200,
            4500,
        );

        fixture
    }

    fn default_authorize_latest_plan_report(fixture: &FakeLaunchPacketLatestFixture) -> Value {
        let paths = launch_packet_latest_paths(&fixture.wrapper_session_dir);
        json!({
            "generated_at": Utc::now(),
            "mode": "plan_latest_authorization",
            "verdict": AUTHORIZE_LATEST_PLAN_READY,
            "reason": "latest immutable chain resolves cleanly into accepted authorization contract",
            "latest_top_step_name": "clerestory_certificate",
            "latest_top_session_dir": fixture.latest_top_session_dir.display().to_string(),
            "launch_packet_session_dir": fixture.historical_launch_packet_session_dir.display().to_string(),
            "package_dir": fixture.package_dir.display().to_string(),
            "install_root": fixture.install_root.display().to_string(),
            "target_service_name": "solana-copy-bot.service",
            "backend_command": fixture.backend_command.display().to_string(),
            "wrapper_timeout_ms": 1200,
            "service_status_max_staleness_ms": 4500,
            "session_dir": paths.nested_authorize_latest_session_dir.display().to_string(),
            "nested_live_authorization_session_dir": paths.nested_authorize_latest_session_dir.join("tiny_live_activation_package_authorize_latest.live_authorization_session").display().to_string(),
            "reviewed_frozen_live_cutover_controller_command_summary": fixture.frozen_controller_command,
            "clerestory_certificate_summary": "latest clerestory summary",
            "clerestory_certificate_sha256": "latest-clerestory-sha",
            "clerestory_certificate_algorithm": "sha256",
            "current_pre_activation_gate_verdict": null,
            "current_pre_activation_gate_reason": null
        })
    }

    fn default_authorize_latest_run_report(
        fixture: &FakeLaunchPacketLatestFixture,
        result: &str,
    ) -> Value {
        let paths = launch_packet_latest_paths(&fixture.wrapper_session_dir);
        let (verdict, gate_reason) = match result {
            "authorized_now" => (
                "tiny_live_package_authorize_latest_authorized_now",
                "green now",
            ),
            "refused_by_stage3" => (
                "tiny_live_package_authorize_latest_refused_now_by_stage3",
                "stage3 blocked",
            ),
            "refused_by_pre_activation_gate" => (
                "tiny_live_package_authorize_latest_refused_now_by_pre_activation_gate",
                "pre gate blocked",
            ),
            _ => (
                "tiny_live_package_authorize_latest_refused_by_drifted_authorization_contract",
                "invalid",
            ),
        };
        json!({
            "generated_at": Utc::now(),
            "mode": "run_latest_authorization",
            "verdict": verdict,
            "reason": format!("authorization {result}"),
            "latest_top_step_name": "clerestory_certificate",
            "latest_top_session_dir": fixture.latest_top_session_dir.display().to_string(),
            "launch_packet_session_dir": fixture.historical_launch_packet_session_dir.display().to_string(),
            "package_dir": fixture.package_dir.display().to_string(),
            "install_root": fixture.install_root.display().to_string(),
            "target_service_name": "solana-copy-bot.service",
            "backend_command": fixture.backend_command.display().to_string(),
            "wrapper_timeout_ms": 1200,
            "service_status_max_staleness_ms": 4500,
            "session_dir": paths.nested_authorize_latest_session_dir.display().to_string(),
            "nested_live_authorization_session_dir": paths.nested_authorize_latest_session_dir.join("tiny_live_activation_package_authorize_latest.live_authorization_session").display().to_string(),
            "result": result,
            "current_pre_activation_gate_verdict": if result == "authorized_now" { json!("green") } else { json!("blocked") },
            "current_pre_activation_gate_reason": gate_reason,
            "reviewed_frozen_live_cutover_controller_command_summary": fixture.frozen_controller_command,
            "clerestory_certificate_summary": "latest clerestory summary",
            "clerestory_certificate_sha256": "latest-clerestory-sha",
            "clerestory_certificate_algorithm": "sha256"
        })
    }

    fn default_authorize_latest_verify_report(
        fixture: &FakeLaunchPacketLatestFixture,
        result: &str,
    ) -> Value {
        let mut report = default_authorize_latest_run_report(fixture, result);
        report["mode"] = json!("verify_latest_authorization");
        report["verdict"] = json!(AUTHORIZE_LATEST_VERIFY_OK);
        report["reason"] = json!("verify ok");
        report
    }

    fn default_launch_packet_plan_report(fixture: &FakeLaunchPacketLatestFixture) -> Value {
        let paths = launch_packet_latest_paths(&fixture.wrapper_session_dir);
        json!({
            "generated_at": Utc::now(),
            "mode": "plan_live_package_launch_packet",
            "verdict": LAUNCH_PACKET_PLAN_READY,
            "reason": "launch packet plan ready",
            "package_dir": fixture.package_dir.display().to_string(),
            "install_root": fixture.install_root.display().to_string(),
            "target_service_name": "solana-copy-bot.service",
            "backend_command": fixture.backend_command.display().to_string(),
            "wrapper_timeout_ms": 1200,
            "service_status_max_staleness_ms": 4500,
            "session_dir": paths.nested_launch_packet_session_dir.display().to_string(),
            "launch_packet_session_path": paths.nested_launch_packet_session_dir.join("tiny_live_activation_package_launch_packet.session.json").display().to_string(),
            "launch_packet_status_path": paths.nested_launch_packet_session_dir.join("tiny_live_activation_package_launch_packet.status.json").display().to_string(),
            "nested_authorization_session_dir": launch_packet_nested_authorization_session_dir(&paths.nested_launch_packet_session_dir).display().to_string(),
            "run_live_authorization_command_summary": launch_packet_nested_authorization_command_summary(
                &LatestLaunchPacketSnapshot {
                    latest_top_step_name: "clerestory_certificate".to_string(),
                    latest_top_session_dir: fixture.latest_top_session_dir.clone(),
                    historical_launch_packet_session_dir: fixture.historical_launch_packet_session_dir.clone(),
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
                &paths.nested_launch_packet_session_dir,
            ),
            "frozen_live_cutover_controller_command_summary": fixture.frozen_controller_command,
            "operator_next_action_summary": "inspect the frozen launch packet",
            "current_pre_activation_gate_verdict": null,
            "current_pre_activation_gate_reason": null
        })
    }

    fn default_launch_packet_run_report(
        fixture: &FakeLaunchPacketLatestFixture,
        authorize_result: &str,
    ) -> Value {
        let paths = launch_packet_latest_paths(&fixture.wrapper_session_dir);
        let (result, verdict, gate_verdict, gate_reason, operator_next_action_summary) =
            match authorize_result {
                "authorized_now" => (
                    "eligible_when_gate_turns_green",
                    "tiny_live_package_launch_packet_eligible_when_gate_turns_green",
                    "green",
                    "ok",
                    format!(
                        "current authorization truth is green; the exact bounded next step remains {}. This launch packet itself still does not execute live cutover.",
                        fixture.frozen_controller_command
                    ),
                ),
                "refused_by_stage3" => (
                    "refused_by_stage3",
                    "tiny_live_package_launch_packet_refused_by_stage3",
                    "blocked",
                    "stage3 blocked",
                    format!(
                        "today refused by current Stage 3 truth; do not run live cutover now. Once the current Stage 3 / pre-activation truth genuinely turns green, the frozen bounded controller command remains {}",
                        fixture.frozen_controller_command
                    ),
                ),
                "refused_by_pre_activation_gate" => (
                    "refused_by_pre_activation_gate",
                    "tiny_live_package_launch_packet_refused_by_pre_activation_gate",
                    "blocked",
                    "pre gate blocked",
                    format!(
                        "today refused by the current pre-activation gate; do not run live cutover now. Once the current Stage 3 / pre-activation truth genuinely turns green, the frozen bounded controller command remains {}",
                        fixture.frozen_controller_command
                    ),
                ),
                _ => (
                    "refused_by_invalid_target",
                    "tiny_live_package_launch_packet_refused_by_invalid_target",
                    "invalid",
                    "invalid",
                    "repair contract drift".to_string(),
                ),
            };
        json!({
            "generated_at": Utc::now(),
            "mode": "run_live_package_launch_packet",
            "verdict": verdict,
            "reason": format!("launch packet {result}"),
            "package_dir": fixture.package_dir.display().to_string(),
            "install_root": fixture.install_root.display().to_string(),
            "target_service_name": "solana-copy-bot.service",
            "backend_command": fixture.backend_command.display().to_string(),
            "wrapper_timeout_ms": 1200,
            "service_status_max_staleness_ms": 4500,
            "session_dir": paths.nested_launch_packet_session_dir.display().to_string(),
            "launch_packet_session_path": paths.nested_launch_packet_session_dir.join("tiny_live_activation_package_launch_packet.session.json").display().to_string(),
            "launch_packet_status_path": paths.nested_launch_packet_session_dir.join("tiny_live_activation_package_launch_packet.status.json").display().to_string(),
            "nested_authorization_session_dir": launch_packet_nested_authorization_session_dir(&paths.nested_launch_packet_session_dir).display().to_string(),
            "result": result,
            "authorization_result_now": authorize_result,
            "run_live_authorization_command_summary": launch_packet_nested_authorization_command_summary(
                &LatestLaunchPacketSnapshot {
                    latest_top_step_name: "clerestory_certificate".to_string(),
                    latest_top_session_dir: fixture.latest_top_session_dir.clone(),
                    historical_launch_packet_session_dir: fixture.historical_launch_packet_session_dir.clone(),
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
                &paths.nested_launch_packet_session_dir,
            ),
            "frozen_live_cutover_controller_command_summary": fixture.frozen_controller_command,
            "operator_next_action_summary": operator_next_action_summary,
            "current_pre_activation_gate_verdict": gate_verdict,
            "current_pre_activation_gate_reason": gate_reason
        })
    }

    fn default_launch_packet_verify_report(
        fixture: &FakeLaunchPacketLatestFixture,
        authorize_result: &str,
    ) -> Value {
        let mut report = default_launch_packet_run_report(fixture, authorize_result);
        report["mode"] = json!("verify_live_package_launch_packet");
        report["verdict"] = json!(LAUNCH_PACKET_VERIFY_OK);
        report["reason"] = json!("verify ok");
        report
    }

    fn write_fake_authorize_latest_command(
        path: &Path,
        plan_json_path: &Path,
        run_json_path: &Path,
        verify_json_path: &Path,
        root: &Path,
    ) {
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --root {} \"*) : ;;\n  *\" --verify-latest-authorization \"*) : ;;\n  *) echo 'unexpected root' >&2; exit 1;;\nesac\nsession=''\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = \"--session-dir\" ]; then\n    shift\n    session=\"$1\"\n    break\n  fi\n  shift\ndone\nmode=''\ncase \" $* \" in\n  *\" --plan-latest-authorization \"*) mode='plan';;\n  *\" --run-latest-authorization \"*) mode='run';;\n  *\" --verify-latest-authorization \"*) mode='verify';;\n  *) echo 'unexpected authorize_latest mode' >&2; exit 1;;\nesac\ncase \"$mode\" in\n  plan) cat '{}';;\n  run) mkdir -p \"$session\"; cp '{}' \"$session/tiny_live_activation_package_authorize_latest.report.json\"; cat '{}';;\n  verify) cat '{}';;\n  *) exit 1;;\nesac\n",
            root.display(),
            plan_json_path.display(),
            run_json_path.display(),
            run_json_path.display(),
            verify_json_path.display()
        );
        fs::write(path, script).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(path).unwrap().permissions();
            perms.set_mode(0o755);
            fs::set_permissions(path, perms).unwrap();
        }
    }

    fn write_fake_launch_packet_command(
        path: &Path,
        plan_json_path: &Path,
        run_json_path: &Path,
        verify_json_path: &Path,
        package_dir: &Path,
        install_root: &Path,
        target_service_name: &str,
        backend_command: &Path,
        wrapper_timeout_ms: u64,
        service_status_max_staleness_ms: u64,
    ) {
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --package-dir {} \"*) : ;;\n  *) echo 'unexpected package-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --install-root {} \"*) : ;;\n  *) echo 'unexpected install-root' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --target-service {} \"*) : ;;\n  *) echo 'unexpected target-service' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --backend-command {} \"*) : ;;\n  *) echo 'unexpected backend-command' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --wrapper-timeout-ms {} \"*) : ;;\n  *) echo 'unexpected wrapper-timeout-ms' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --service-status-max-staleness-ms {} \"*) : ;;\n  *) echo 'unexpected service-status-max-staleness-ms' >&2; exit 1;;\nesac\nsession=''\nset -- $*\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = \"--session-dir\" ]; then\n    shift\n    session=\"$1\"\n    break\n  fi\n  shift\ndone\nmode=''\ncase \" $* \" in\n  *\" --plan-live-package-launch-packet \"*) mode='plan';;\n  *\" --run-live-package-launch-packet \"*) mode='run';;\n  *\" --verify-live-package-launch-packet \"*) mode='verify';;\n  *) echo 'unexpected launch_packet mode' >&2; exit 1;;\nesac\ncase \"$mode\" in\n  plan) cat '{}';;\n  run) mkdir -p \"$session\"; cp '{}' \"$session/tiny_live_activation_package_launch_packet.report.json\"; cat '{}';;\n  verify) cat '{}';;\n  *) exit 1;;\nesac\n",
            package_dir.display(),
            install_root.display(),
            target_service_name,
            backend_command.display(),
            wrapper_timeout_ms,
            service_status_max_staleness_ms,
            plan_json_path.display(),
            run_json_path.display(),
            run_json_path.display(),
            verify_json_path.display()
        );
        fs::write(path, script).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(path).unwrap().permissions();
            perms.set_mode(0o755);
            fs::set_permissions(path, perms).unwrap();
        }
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
