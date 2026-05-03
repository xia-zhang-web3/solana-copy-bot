use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
#[cfg(test)]
use serde_json::json;
use serde_json::Value;
use std::env;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const USAGE: &str = "usage: copybot_tiny_live_activation_package_decision_packet_latest --root <path> [--session-dir <path>] [--output <path>] [--json] (--plan-latest-decision-packet | --render-latest-decision-packet-script | --run-latest-decision-packet | --verify-latest-decision-packet)";
const TOP_ACCEPTED_PACKAGE_STEP: &str = "clerestory_certificate";
const DECISION_PACKET_LATEST_SESSION_VERSION: &str = "1";
const DECISION_PACKET_LATEST_STATUS_VERSION: &str = "1";
const EXECUTE_LATEST_BIN: &str = "copybot_tiny_live_activation_package_execute_latest";
const DECISION_PACKET_BIN: &str = "copybot_tiny_live_activation_package_decision_packet";
const EXECUTE_LATEST_PLAN_READY: &str = "tiny_live_package_execute_latest_plan_ready";
const DECISION_PACKET_PLAN_READY: &str = "tiny_live_package_decision_packet_plan_ready";
const DECISION_PACKET_VERIFY_OK: &str = "tiny_live_package_decision_packet_verify_ok";
const PLANNING_SAFE_STATEMENT: &str =
    "this latest-decision-packet handoff binds the latest persisted immutable package chain to the accepted decision-packet contract, but it never overrides Stage 3 or pre-activation refusal truth and it never executes or authorizes the frozen live cutover controller by itself";

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
    PlanLatestDecisionPacket,
    RenderLatestDecisionPacketScript,
    RunLatestDecisionPacket,
    VerifyLatestDecisionPacket,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageDecisionPacketLatestVerdict {
    TinyLivePackageDecisionPacketLatestPlanReady,
    TinyLivePackageDecisionPacketLatestScriptRendered,
    TinyLivePackageDecisionPacketLatestRefusedByMissingLatestChain,
    TinyLivePackageDecisionPacketLatestRefusedByInvalidLatestChain,
    TinyLivePackageDecisionPacketLatestRefusedByDriftedDecisionPacketContract,
    TinyLivePackageDecisionPacketLatestRefusedNowByStage3,
    TinyLivePackageDecisionPacketLatestRefusedNowByPreActivationGate,
    TinyLivePackageDecisionPacketLatestRunnableWhenGateTruthTurnsGreen,
    TinyLivePackageDecisionPacketLatestVerifyOk,
    TinyLivePackageDecisionPacketLatestVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LivePackageDecisionPacketLatestResult {
    RefusedByMissingLatestChain,
    RefusedByInvalidLatestChain,
    RefusedByDriftedDecisionPacketContract,
    RefusedNowByStage3,
    RefusedNowByPreActivationGate,
    RunnableWhenGateTruthTurnsGreen,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageDecisionPacketLatestStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageDecisionPacketLatestSession {
    session_version: String,
    planned_at: DateTime<Utc>,
    root: String,
    session_dir: String,
    shadow_execute_latest_session_dir: String,
    nested_decision_packet_session_dir: String,
    latest_top_step_name: Option<String>,
    latest_top_session_dir: Option<String>,
    historical_execute_frozen_session_dir: Option<String>,
    turn_green_session_dir: Option<String>,
    launch_packet_session_dir: Option<String>,
    package_dir: Option<String>,
    install_root: Option<String>,
    target_service_name: Option<String>,
    backend_command: Option<String>,
    wrapper_timeout_ms: Option<u64>,
    service_status_max_staleness_ms: Option<u64>,
    resolve_latest_chain_command_summary: String,
    plan_decision_packet_command_summary: Option<String>,
    downstream_decision_packet_command_summary: Option<String>,
    verify_decision_packet_command_summary: Option<String>,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    clerestory_certificate_summary: Option<String>,
    clerestory_certificate_sha256: Option<String>,
    clerestory_certificate_algorithm: Option<String>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageDecisionPacketLatestStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    root: String,
    session_dir: String,
    latest_top_step_name: Option<String>,
    latest_top_session_dir: Option<String>,
    historical_execute_frozen_session_dir: Option<String>,
    turn_green_session_dir: Option<String>,
    launch_packet_session_dir: Option<String>,
    nested_decision_packet_session_dir: String,
    result: LivePackageDecisionPacketLatestResult,
    reason: String,
    latest_execute_verdict: Option<String>,
    decision_packet_plan_verdict: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    latest_execute_step: Option<PackageDecisionPacketLatestStepArtifact>,
    decision_packet_plan_step: Option<PackageDecisionPacketLatestStepArtifact>,
    decision_packet_step: Option<PackageDecisionPacketLatestStepArtifact>,
    operator_next_action_summary: String,
    activation_authorized: bool,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackageDecisionPacketLatestPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    report_path: PathBuf,
    latest_execute_report_path: PathBuf,
    decision_packet_plan_report_path: PathBuf,
    decision_packet_report_path: PathBuf,
    shadow_execute_latest_session_dir: PathBuf,
    nested_decision_packet_session_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageDecisionPacketLatestReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageDecisionPacketLatestVerdict,
    reason: String,
    root: Option<String>,
    latest_top_step_name: Option<String>,
    latest_top_session_dir: Option<String>,
    historical_execute_frozen_session_dir: Option<String>,
    turn_green_session_dir: Option<String>,
    launch_packet_session_dir: Option<String>,
    package_dir: Option<String>,
    install_root: Option<String>,
    target_service_name: Option<String>,
    backend_command: Option<String>,
    wrapper_timeout_ms: Option<u64>,
    service_status_max_staleness_ms: Option<u64>,
    session_dir: Option<String>,
    decision_packet_latest_session_path: Option<String>,
    decision_packet_latest_status_path: Option<String>,
    decision_packet_latest_report_path: Option<String>,
    latest_execute_report_path: Option<String>,
    decision_packet_plan_report_path: Option<String>,
    nested_decision_packet_report_path: Option<String>,
    shadow_execute_latest_session_dir: Option<String>,
    nested_decision_packet_session_dir: Option<String>,
    latest_execute_verdict: Option<String>,
    decision_packet_plan_verdict: Option<String>,
    result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    resolve_latest_chain_command_summary: Option<String>,
    plan_decision_packet_command_summary: Option<String>,
    downstream_decision_packet_command_summary: Option<String>,
    verify_decision_packet_command_summary: Option<String>,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    clerestory_certificate_summary: Option<String>,
    clerestory_certificate_sha256: Option<String>,
    clerestory_certificate_algorithm: Option<String>,
    verification_mismatches: Vec<String>,
    activation_authorized: bool,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct LatestDecisionPacketSnapshot {
    latest_top_step_name: String,
    latest_top_session_dir: PathBuf,
    historical_execute_frozen_session_dir: PathBuf,
    turn_green_session_dir: PathBuf,
    launch_packet_session_dir: PathBuf,
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
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
}

#[derive(Debug, Clone)]
struct ResolvedLatestDecisionPacket {
    snapshot: LatestDecisionPacketSnapshot,
    latest_execute_plan_raw: Value,
    latest_execute_plan: ExecuteLatestReportView,
    decision_packet_plan_raw: Value,
    decision_packet_plan: DecisionPacketReportView,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResolutionFailureKind {
    MissingLatestChain,
    InvalidLatestChain,
    DriftedDecisionPacketContract,
}

#[derive(Debug, Clone)]
struct LatestDecisionPacketResolutionFailure {
    kind: ResolutionFailureKind,
    reason: String,
    snapshot: Option<LatestDecisionPacketSnapshot>,
    latest_execute_plan_raw: Option<Value>,
    latest_execute_verdict: Option<String>,
    latest_execute_reason: Option<String>,
    latest_execute_generated_at: Option<DateTime<Utc>>,
    decision_packet_plan_raw: Option<Value>,
    decision_packet_plan_verdict: Option<String>,
    decision_packet_plan_reason: Option<String>,
    decision_packet_plan_generated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Deserialize)]
struct ExecuteLatestReportView {
    generated_at: DateTime<Utc>,
    verdict: String,
    reason: String,
    session_dir: Option<String>,
    latest_top_step_name: Option<String>,
    latest_top_session_dir: Option<String>,
    latest_chain_execute_frozen_session_dir: Option<String>,
    turn_green_session_dir: Option<String>,
    launch_packet_session_dir: Option<String>,
    package_dir: Option<String>,
    install_root: Option<String>,
    target_service_name: Option<String>,
    backend_command: Option<String>,
    wrapper_timeout_ms: Option<u64>,
    service_status_max_staleness_ms: Option<u64>,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    clerestory_certificate_summary: Option<String>,
    clerestory_certificate_sha256: Option<String>,
    clerestory_certificate_algorithm: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct DecisionPacketReportView {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: String,
    reason: String,
    execute_frozen_session_dir: String,
    turn_green_session_dir: Option<String>,
    launch_packet_session_dir: Option<String>,
    package_dir: Option<String>,
    install_root: Option<String>,
    target_service_name: Option<String>,
    backend_command: Option<String>,
    wrapper_timeout_ms: Option<u64>,
    service_status_max_staleness_ms: Option<u64>,
    session_dir: Option<String>,
    decision_packet_session_path: Option<String>,
    decision_packet_status_path: Option<String>,
    result: Option<String>,
    execute_frozen_result: Option<String>,
    verify_execute_frozen_command_summary: Option<String>,
    frozen_live_cutover_controller_command_summary: Option<String>,
    operator_checklist_summary: Option<String>,
    operator_runbook_summary: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    explicit_statement: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
struct DecisionPacketSessionView {
    execute_frozen_session_dir: String,
    turn_green_session_dir: String,
    launch_packet_session_dir: String,
    package_dir: String,
    install_root: String,
    target_service_name: String,
    backend_command: String,
    wrapper_timeout_ms: u64,
    service_status_max_staleness_ms: u64,
    session_dir: String,
    verify_execute_frozen_command_summary: String,
    frozen_live_cutover_controller_command_summary: String,
    operator_checklist_summary: String,
    operator_runbook_summary: String,
    explicit_statement: String,
}

#[derive(Debug, Clone, Deserialize)]
struct DecisionPacketStatusView {
    session_dir: String,
    execute_frozen_session_dir: String,
    result: String,
    reason: String,
    execute_frozen_result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    operator_checklist_summary: String,
    operator_runbook_summary: String,
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
    let mut root: Option<PathBuf> = None;
    let mut session_dir: Option<PathBuf> = None;
    let mut output_path: Option<PathBuf> = None;
    let mut json = false;
    let mut mode: Option<Mode> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--root" => root = Some(PathBuf::from(parse_string_arg("--root", args.next())?)),
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
            "--plan-latest-decision-packet" => {
                set_mode(&mut mode, Mode::PlanLatestDecisionPacket, arg.as_str())?
            }
            "--render-latest-decision-packet-script" => set_mode(
                &mut mode,
                Mode::RenderLatestDecisionPacketScript,
                arg.as_str(),
            )?,
            "--run-latest-decision-packet" => {
                set_mode(&mut mode, Mode::RunLatestDecisionPacket, arg.as_str())?
            }
            "--verify-latest-decision-packet" => {
                set_mode(&mut mode, Mode::VerifyLatestDecisionPacket, arg.as_str())?
            }
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown argument: {other}"),
        }
    }

    let Some(mode) = mode else {
        return Ok(None);
    };
    if matches!(
        mode,
        Mode::PlanLatestDecisionPacket
            | Mode::RenderLatestDecisionPacketScript
            | Mode::RunLatestDecisionPacket
    ) && root.is_none()
    {
        bail!("missing --root");
    }
    if matches!(mode, Mode::RenderLatestDecisionPacketScript) && output_path.is_none() {
        bail!("missing --output for --render-latest-decision-packet-script");
    }
    if matches!(
        mode,
        Mode::RunLatestDecisionPacket | Mode::VerifyLatestDecisionPacket
    ) && session_dir.is_none()
    {
        bail!("missing --session-dir for run/verify mode");
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
        Mode::PlanLatestDecisionPacket => plan_latest_decision_packet_report(&config)?,
        Mode::RenderLatestDecisionPacketScript => {
            render_latest_decision_packet_script_report(&config)?
        }
        Mode::RunLatestDecisionPacket => run_latest_decision_packet_report(&config)?,
        Mode::VerifyLatestDecisionPacket => verify_latest_decision_packet_report(&config)?,
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_report_lines(&report))
    }
}

fn plan_latest_decision_packet_report(
    config: &Config,
) -> Result<PackageDecisionPacketLatestReport> {
    let root = config
        .root
        .as_ref()
        .ok_or_else(|| anyhow!("missing --root"))?;
    let suggested_session_dir = config.session_dir.clone().unwrap_or_else(|| {
        root.join("tiny_live_activation_package_decision_packet_latest.session")
    });
    let paths = decision_packet_latest_paths(&suggested_session_dir);

    match resolve_latest_decision_packet(root, &paths) {
        Ok(resolved) => Ok(plan_report_from_resolution(
            "plan_latest_decision_packet",
            TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestPlanReady,
            format!(
                "latest immutable {} chain under {} resolves to exact execute_latest shadow {} and historical execute_frozen {} lineage for the accepted decision-packet contract",
                resolved.snapshot.latest_top_step_name,
                root.display(),
                paths.shadow_execute_latest_session_dir.display(),
                resolved.snapshot.historical_execute_frozen_session_dir.display()
            ),
            root,
            &suggested_session_dir,
            &paths,
            &resolved,
        )),
        Err(failure) => Ok(report_from_resolution_failure(
            "plan_latest_decision_packet",
            root,
            &suggested_session_dir,
            &paths,
            &failure,
        )),
    }
}

fn render_latest_decision_packet_script_report(
    config: &Config,
) -> Result<PackageDecisionPacketLatestReport> {
    let root = config
        .root
        .as_ref()
        .ok_or_else(|| anyhow!("missing --root"))?;
    let output_path = config
        .output_path
        .as_ref()
        .ok_or_else(|| anyhow!("missing --output"))?;
    let suggested_session_dir = config
        .session_dir
        .clone()
        .unwrap_or_else(|| output_path.with_extension("session"));
    let paths = decision_packet_latest_paths(&suggested_session_dir);

    match resolve_latest_decision_packet(root, &paths) {
        Ok(resolved) => {
            let script = format!(
                "#!/usr/bin/env bash\nset -euo pipefail\n\ncopybot_tiny_live_activation_package_decision_packet --execute-frozen-session-dir {} --session-dir {} --plan-live-package-decision-packet\ncopybot_tiny_live_activation_package_decision_packet --execute-frozen-session-dir {} --session-dir {} --run-live-package-decision-packet\ncopybot_tiny_live_activation_package_decision_packet --execute-frozen-session-dir {} --session-dir {} --verify-live-package-decision-packet\n",
                shell_escape_path(&resolved.snapshot.historical_execute_frozen_session_dir),
                shell_escape_path(&paths.nested_decision_packet_session_dir),
                shell_escape_path(&resolved.snapshot.historical_execute_frozen_session_dir),
                shell_escape_path(&paths.nested_decision_packet_session_dir),
                shell_escape_path(&resolved.snapshot.historical_execute_frozen_session_dir),
                shell_escape_path(&paths.nested_decision_packet_session_dir),
            );
            write_script(output_path, &script)?;
            Ok(plan_report_from_resolution(
                "render_latest_decision_packet_script",
                TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestScriptRendered,
                format!(
                    "rendered latest-decision-packet handoff script to {} using the exact accepted decision-packet contract",
                    output_path.display()
                ),
                root,
                &suggested_session_dir,
                &paths,
                &resolved,
            ))
        }
        Err(failure) => Ok(report_from_resolution_failure(
            "render_latest_decision_packet_script",
            root,
            &suggested_session_dir,
            &paths,
            &failure,
        )),
    }
}

fn run_latest_decision_packet_report(config: &Config) -> Result<PackageDecisionPacketLatestReport> {
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
            "refusing to overwrite existing latest-decision-packet session dir {}",
            session_dir.display()
        );
    }
    let paths = decision_packet_latest_paths(session_dir);
    create_clean_session_dir(session_dir, "latest-decision-packet")?;

    match resolve_latest_decision_packet(root, &paths) {
        Ok(resolved) => {
            let session = planned_session(root, session_dir, &paths, Some(&resolved), None);
            persist_json(&paths.session_path, &session)?;
            persist_json(
                &paths.latest_execute_report_path,
                &resolved.latest_execute_plan_raw,
            )?;
            persist_json(
                &paths.decision_packet_plan_report_path,
                &resolved.decision_packet_plan_raw,
            )?;

            let latest_execute_step = Some(step_artifact(
                &paths.latest_execute_report_path,
                "plan_latest_execute",
                &resolved.latest_execute_plan.verdict,
                &resolved.latest_execute_plan.reason,
                resolved.latest_execute_plan.generated_at,
            ));
            let decision_packet_plan_step = Some(step_artifact(
                &paths.decision_packet_plan_report_path,
                "plan_live_package_decision_packet",
                &resolved.decision_packet_plan.verdict,
                &resolved.decision_packet_plan.reason,
                resolved.decision_packet_plan.generated_at,
            ));

            let nested_run_raw = match run_json_command(
                DECISION_PACKET_BIN,
                &decision_packet_args(
                    &resolved.snapshot.historical_execute_frozen_session_dir,
                    &paths.nested_decision_packet_session_dir,
                    "--run-live-package-decision-packet",
                ),
            ) {
                Ok(value) => value,
                Err(error) => {
                    let status = refusal_status(
                        root,
                        session_dir,
                        &paths,
                        Some(&resolved),
                        LivePackageDecisionPacketLatestResult::RefusedByDriftedDecisionPacketContract,
                        format!(
                            "failed running downstream decision-packet contract for {}: {error}",
                            resolved.snapshot.historical_execute_frozen_session_dir.display()
                        ),
                        latest_execute_step,
                        decision_packet_plan_step,
                        None,
                        resolved.snapshot.current_pre_activation_gate_verdict.clone(),
                        resolved.snapshot.current_pre_activation_gate_reason.clone(),
                    );
                    persist_json(&paths.status_path, &status)?;
                    let report = report_from_status(
                        "run_latest_decision_packet",
                        verdict_for_result(status.result),
                        session_dir,
                        &paths,
                        &session,
                        &status,
                        Vec::new(),
                    );
                    persist_json(&paths.report_path, &report)?;
                    return Ok(report);
                }
            };

            persist_json(&paths.decision_packet_report_path, &nested_run_raw)?;
            let nested_view: DecisionPacketReportView = serde_json::from_value(nested_run_raw)
                .context("failed parsing downstream decision-packet run json")?;
            let nested_step = Some(step_artifact(
                &paths.decision_packet_report_path,
                "run_live_package_decision_packet",
                &nested_view.verdict,
                &nested_view.reason,
                nested_view.generated_at,
            ));

            let (result, verdict) = map_decision_packet_outcome(&nested_view);
            let status = PackageDecisionPacketLatestStatus {
                status_version: DECISION_PACKET_LATEST_STATUS_VERSION.to_string(),
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
                historical_execute_frozen_session_dir: Some(
                    resolved
                        .snapshot
                        .historical_execute_frozen_session_dir
                        .display()
                        .to_string(),
                ),
                turn_green_session_dir: Some(
                    resolved
                        .snapshot
                        .turn_green_session_dir
                        .display()
                        .to_string(),
                ),
                launch_packet_session_dir: Some(
                    resolved
                        .snapshot
                        .launch_packet_session_dir
                        .display()
                        .to_string(),
                ),
                nested_decision_packet_session_dir: paths
                    .nested_decision_packet_session_dir
                    .display()
                    .to_string(),
                result,
                reason: nested_view.reason.clone(),
                latest_execute_verdict: Some(resolved.latest_execute_plan.verdict.clone()),
                decision_packet_plan_verdict: Some(resolved.decision_packet_plan.verdict.clone()),
                current_pre_activation_gate_verdict: nested_view
                    .current_pre_activation_gate_verdict
                    .clone(),
                current_pre_activation_gate_reason: nested_view
                    .current_pre_activation_gate_reason
                    .clone(),
                latest_execute_step,
                decision_packet_plan_step,
                decision_packet_step: nested_step,
                operator_next_action_summary: operator_next_action_summary(result),
                activation_authorized: false,
                explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
            };
            persist_json(&paths.status_path, &status)?;
            let report = report_from_status(
                "run_latest_decision_packet",
                verdict,
                session_dir,
                &paths,
                &session,
                &status,
                Vec::new(),
            );
            persist_json(&paths.report_path, &report)?;
            Ok(report)
        }
        Err(failure) => {
            let session = planned_session(root, session_dir, &paths, None, Some(&failure));
            persist_json(&paths.session_path, &session)?;
            if let Some(raw) = &failure.latest_execute_plan_raw {
                persist_json(&paths.latest_execute_report_path, raw)?;
            }
            if let Some(raw) = &failure.decision_packet_plan_raw {
                persist_json(&paths.decision_packet_plan_report_path, raw)?;
            }
            let status =
                refusal_status_from_resolution_failure(root, session_dir, &paths, &failure);
            persist_json(&paths.status_path, &status)?;
            let report = report_from_status(
                "run_latest_decision_packet",
                verdict_from_failure_kind(failure.kind),
                session_dir,
                &paths,
                &session,
                &status,
                Vec::new(),
            );
            persist_json(&paths.report_path, &report)?;
            Ok(report)
        }
    }
}

fn verify_latest_decision_packet_report(
    config: &Config,
) -> Result<PackageDecisionPacketLatestReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for verify mode"))?;
    let paths = decision_packet_latest_paths(session_dir);

    let session: PackageDecisionPacketLatestSession = match load_json(&paths.session_path) {
        Ok(value) => value,
        Err(error) => {
            return Ok(verify_invalid_report_without_session(
                session_dir,
                vec![format!(
                    "failed reading latest-decision-packet session artifact {}: {error}",
                    paths.session_path.display()
                )],
            ));
        }
    };
    let status: PackageDecisionPacketLatestStatus = match load_json(&paths.status_path) {
        Ok(value) => value,
        Err(error) => {
            return Ok(verify_invalid_report_without_session(
                session_dir,
                vec![format!(
                    "failed reading latest-decision-packet status artifact {}: {error}",
                    paths.status_path.display()
                )],
            ));
        }
    };
    let stored_report: PackageDecisionPacketLatestReport = match load_json(&paths.report_path) {
        Ok(value) => value,
        Err(error) => {
            return Ok(verify_invalid_report_without_session(
                session_dir,
                vec![format!(
                    "failed reading latest-decision-packet report artifact {}: {error}",
                    paths.report_path.display()
                )],
            ));
        }
    };

    let root = PathBuf::from(&session.root);
    let mut mismatches = Vec::new();
    let expected_report_verdict = serialize_enum(&verdict_for_result(status.result));

    compare_string(
        &session.root,
        &root.display().to_string(),
        "stored session root",
        &mut mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "stored session session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.shadow_execute_latest_session_dir,
        &paths
            .shadow_execute_latest_session_dir
            .display()
            .to_string(),
        "stored session shadow_execute_latest_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.nested_decision_packet_session_dir,
        &paths
            .nested_decision_packet_session_dir
            .display()
            .to_string(),
        "stored session nested_decision_packet_session_dir",
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
        "stored status root",
        &mut mismatches,
    );
    compare_string(
        &status.session_dir,
        &session_dir.display().to_string(),
        "stored status session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.nested_decision_packet_session_dir,
        &paths
            .nested_decision_packet_session_dir
            .display()
            .to_string(),
        "stored status nested_decision_packet_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.operator_next_action_summary,
        &operator_next_action_summary(status.result),
        "stored status operator_next_action_summary",
        &mut mismatches,
    );
    compare_string(
        &status.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "stored status explicit_statement",
        &mut mismatches,
    );
    if status.activation_authorized {
        mismatches.push(
            "stored status activation_authorized must remain false for latest-decision-packet"
                .to_string(),
        );
    }
    compare_option_string(
        stored_report.root.as_deref(),
        Some(root.display().to_string().as_str()),
        "stored report root",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored report session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.shadow_execute_latest_session_dir.as_deref(),
        Some(
            paths
                .shadow_execute_latest_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report shadow_execute_latest_session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.nested_decision_packet_session_dir.as_deref(),
        Some(
            paths
                .nested_decision_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report nested_decision_packet_session_dir",
        &mut mismatches,
    );
    compare_string(
        &stored_report.mode,
        "run_latest_decision_packet",
        "stored report mode",
        &mut mismatches,
    );
    compare_string(
        &serialize_enum(&stored_report.verdict),
        &expected_report_verdict,
        "stored report verdict",
        &mut mismatches,
    );
    compare_string(
        &stored_report.reason,
        &status.reason,
        "stored report reason consistency",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.result.as_deref(),
        Some(serialize_enum(&status.result).as_str()),
        "stored report result consistency",
        &mut mismatches,
    );
    compare_string(
        &stored_report.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "stored report explicit_statement",
        &mut mismatches,
    );
    if stored_report.activation_authorized {
        mismatches.push(
            "stored report activation_authorized must remain false for latest-decision-packet"
                .to_string(),
        );
    }

    match resolve_latest_decision_packet(&root, &paths) {
        Ok(resolved) => {
            compare_session_against_snapshot(
                &session,
                &root,
                session_dir,
                &paths,
                &resolved,
                &mut mismatches,
            );
            compare_report_against_snapshot(
                &stored_report,
                &root,
                session_dir,
                &paths,
                &resolved,
                &mut mismatches,
            );
            compare_option_string(
                status.latest_execute_verdict.as_deref(),
                Some(resolved.latest_execute_plan.verdict.as_str()),
                "stored status latest_execute_verdict",
                &mut mismatches,
            );
            compare_option_string(
                status.decision_packet_plan_verdict.as_deref(),
                Some(resolved.decision_packet_plan.verdict.as_str()),
                "stored status decision_packet_plan_verdict",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.latest_execute_verdict.as_deref(),
                Some(resolved.latest_execute_plan.verdict.as_str()),
                "stored report latest_execute_verdict",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.decision_packet_plan_verdict.as_deref(),
                Some(resolved.decision_packet_plan.verdict.as_str()),
                "stored report decision_packet_plan_verdict",
                &mut mismatches,
            );

            let stored_latest_execute: ExecuteLatestReportView = load_required_step_json(
                &status.latest_execute_step,
                &paths.latest_execute_report_path,
                "latest_execute_step",
                &mut mismatches,
            )?;
            compare_step_artifact_against_report_metadata(
                status.latest_execute_step.as_ref(),
                &paths.latest_execute_report_path,
                "plan_latest_execute",
                &stored_latest_execute.verdict,
                &stored_latest_execute.reason,
                stored_latest_execute.generated_at,
                "stored latest_execute_step",
                &mut mismatches,
            );
            compare_execute_latest_copy_against_actual(
                &stored_latest_execute,
                &resolved.latest_execute_plan,
                &paths.shadow_execute_latest_session_dir,
                &mut mismatches,
            );

            let stored_decision_packet_plan: DecisionPacketReportView = load_required_step_json(
                &status.decision_packet_plan_step,
                &paths.decision_packet_plan_report_path,
                "decision_packet_plan_step",
                &mut mismatches,
            )?;
            compare_step_artifact_against_report_metadata(
                status.decision_packet_plan_step.as_ref(),
                &paths.decision_packet_plan_report_path,
                "plan_live_package_decision_packet",
                &stored_decision_packet_plan.verdict,
                &stored_decision_packet_plan.reason,
                stored_decision_packet_plan.generated_at,
                "stored decision_packet_plan_step",
                &mut mismatches,
            );
            compare_decision_packet_plan_copy_against_actual(
                &stored_decision_packet_plan,
                &resolved.decision_packet_plan,
                &paths.nested_decision_packet_session_dir,
                &mut mismatches,
            );

            if status.decision_packet_step.is_none() {
                mismatches.push(
                    "stored status decision_packet_step is missing even though latest-decision-packet resolution is currently coherent"
                        .to_string(),
                );
            } else {
                let stored_decision_packet_run: DecisionPacketReportView = load_required_step_json(
                    &status.decision_packet_step,
                    &paths.decision_packet_report_path,
                    "decision_packet_step",
                    &mut mismatches,
                )?;
                compare_step_artifact_against_report_metadata(
                    status.decision_packet_step.as_ref(),
                    &paths.decision_packet_report_path,
                    "run_live_package_decision_packet",
                    &stored_decision_packet_run.verdict,
                    &stored_decision_packet_run.reason,
                    stored_decision_packet_run.generated_at,
                    "stored decision_packet_step",
                    &mut mismatches,
                );
                compare_decision_packet_run_against_snapshot(
                    &stored_decision_packet_run,
                    &resolved.snapshot,
                    &paths.nested_decision_packet_session_dir,
                    &mut mismatches,
                );

                let nested_session: DecisionPacketSessionView = match load_json(
                    &nested_decision_packet_session_path(&paths.nested_decision_packet_session_dir),
                ) {
                    Ok(value) => value,
                    Err(error) => {
                        mismatches.push(format!(
                            "failed loading nested decision-packet session under {}: {error}",
                            paths.nested_decision_packet_session_dir.display()
                        ));
                        fallback_decision_packet_session(
                            &resolved.snapshot,
                            &paths.nested_decision_packet_session_dir,
                        )
                    }
                };
                let nested_status: DecisionPacketStatusView = match load_json(
                    &nested_decision_packet_status_path(&paths.nested_decision_packet_session_dir),
                ) {
                    Ok(value) => value,
                    Err(error) => {
                        mismatches.push(format!(
                            "failed loading nested decision-packet status under {}: {error}",
                            paths.nested_decision_packet_session_dir.display()
                        ));
                        fallback_decision_packet_status()
                    }
                };
                compare_decision_packet_copy_against_nested_truth(
                    &stored_decision_packet_run,
                    &nested_session,
                    &nested_status,
                    &mut mismatches,
                );
                compare_string(
                    &status.reason,
                    &nested_status.reason,
                    "stored wrapper status reason against nested decision-packet status",
                    &mut mismatches,
                );
                compare_string(
                    &stored_report.reason,
                    &nested_status.reason,
                    "stored wrapper report reason against nested decision-packet status",
                    &mut mismatches,
                );
                compare_option_string(
                    status.current_pre_activation_gate_verdict.as_deref(),
                    nested_status.current_pre_activation_gate_verdict.as_deref(),
                    "stored wrapper status current_pre_activation_gate_verdict against nested decision-packet status",
                    &mut mismatches,
                );
                compare_option_string(
                    status.current_pre_activation_gate_reason.as_deref(),
                    nested_status.current_pre_activation_gate_reason.as_deref(),
                    "stored wrapper status current_pre_activation_gate_reason against nested decision-packet status",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_report.current_pre_activation_gate_verdict.as_deref(),
                    nested_status.current_pre_activation_gate_verdict.as_deref(),
                    "stored wrapper report current_pre_activation_gate_verdict against nested decision-packet status",
                    &mut mismatches,
                );
                compare_option_string(
                    stored_report.current_pre_activation_gate_reason.as_deref(),
                    nested_status.current_pre_activation_gate_reason.as_deref(),
                    "stored wrapper report current_pre_activation_gate_reason against nested decision-packet status",
                    &mut mismatches,
                );

                let verified_nested_raw = match run_json_command(
                    DECISION_PACKET_BIN,
                    &decision_packet_args(
                        &resolved.snapshot.historical_execute_frozen_session_dir,
                        &paths.nested_decision_packet_session_dir,
                        "--verify-live-package-decision-packet",
                    ),
                ) {
                    Ok(value) => value,
                    Err(error) => {
                        mismatches.push(format!(
                            "failed verifying nested decision-packet session under {}: {error}",
                            paths.nested_decision_packet_session_dir.display()
                        ));
                        Value::Null
                    }
                };
                if !verified_nested_raw.is_null() {
                    let verified_nested: DecisionPacketReportView =
                        serde_json::from_value(verified_nested_raw)
                            .context("failed parsing nested decision-packet verify json")?;
                    if verified_nested.verdict != DECISION_PACKET_VERIFY_OK {
                        mismatches.push(format!(
                            "nested decision-packet verification is non-green: {}",
                            verified_nested.reason
                        ));
                    }
                    compare_option_string(
                        stored_decision_packet_run
                            .frozen_live_cutover_controller_command_summary
                            .as_deref(),
                        verified_nested
                            .frozen_live_cutover_controller_command_summary
                            .as_deref(),
                        "stored nested decision-packet frozen_live_cutover_controller_command_summary",
                        &mut mismatches,
                    );
                    compare_option_string(
                        status.current_pre_activation_gate_verdict.as_deref(),
                        verified_nested.current_pre_activation_gate_verdict.as_deref(),
                        "stored wrapper status current_pre_activation_gate_verdict against verified nested decision-packet",
                        &mut mismatches,
                    );
                    compare_option_string(
                        status.current_pre_activation_gate_reason.as_deref(),
                        verified_nested.current_pre_activation_gate_reason.as_deref(),
                        "stored wrapper status current_pre_activation_gate_reason against verified nested decision-packet",
                        &mut mismatches,
                    );
                    compare_option_string(
                        stored_report.current_pre_activation_gate_verdict.as_deref(),
                        verified_nested.current_pre_activation_gate_verdict.as_deref(),
                        "stored wrapper report current_pre_activation_gate_verdict against verified nested decision-packet",
                        &mut mismatches,
                    );
                    compare_option_string(
                        stored_report.current_pre_activation_gate_reason.as_deref(),
                        verified_nested.current_pre_activation_gate_reason.as_deref(),
                        "stored wrapper report current_pre_activation_gate_reason against verified nested decision-packet",
                        &mut mismatches,
                    );

                    let expected_result =
                        result_from_decision_packet_result(verified_nested.result.as_deref())
                            .unwrap_or(
                                LivePackageDecisionPacketLatestResult::RefusedByDriftedDecisionPacketContract,
                            );
                    if status.result != expected_result {
                        mismatches.push(format!(
                            "stored latest-decision-packet result {} does not match verified nested decision-packet result {}",
                            serialize_enum(&status.result),
                            serialize_enum(&expected_result)
                        ));
                    }
                }
            }
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
            if status.result != result_from_failure_kind(failure.kind) {
                mismatches.push(format!(
                    "stored failure result {} does not match current resolution failure {}",
                    serialize_enum(&status.result),
                    serialize_enum(&result_from_failure_kind(failure.kind))
                ));
            }
            compare_string(
                &status.reason,
                &failure.reason,
                "stored failure reason",
                &mut mismatches,
            );
            compare_string(
                &stored_report.reason,
                &failure.reason,
                "stored failure report reason",
                &mut mismatches,
            );
            compare_option_string(
                status.latest_execute_verdict.as_deref(),
                failure.latest_execute_verdict.as_deref(),
                "stored failure latest_execute_verdict",
                &mut mismatches,
            );
            compare_option_string(
                status.decision_packet_plan_verdict.as_deref(),
                failure.decision_packet_plan_verdict.as_deref(),
                "stored failure decision_packet_plan_verdict",
                &mut mismatches,
            );
            compare_option_string(
                status.current_pre_activation_gate_verdict.as_deref(),
                failure
                    .snapshot
                    .as_ref()
                    .and_then(|value| value.current_pre_activation_gate_verdict.as_deref()),
                "stored failure status current_pre_activation_gate_verdict",
                &mut mismatches,
            );
            compare_option_string(
                status.current_pre_activation_gate_reason.as_deref(),
                failure
                    .snapshot
                    .as_ref()
                    .and_then(|value| value.current_pre_activation_gate_reason.as_deref()),
                "stored failure status current_pre_activation_gate_reason",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.current_pre_activation_gate_verdict.as_deref(),
                failure
                    .snapshot
                    .as_ref()
                    .and_then(|value| value.current_pre_activation_gate_verdict.as_deref()),
                "stored failure report current_pre_activation_gate_verdict",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.current_pre_activation_gate_reason.as_deref(),
                failure
                    .snapshot
                    .as_ref()
                    .and_then(|value| value.current_pre_activation_gate_reason.as_deref()),
                "stored failure report current_pre_activation_gate_reason",
                &mut mismatches,
            );
            if status.decision_packet_step.is_some() {
                mismatches.push(
                    "stored status must not carry decision_packet_step when latest-decision-packet resolution fails before downstream decision-packet run"
                        .to_string(),
                );
            }
            if paths.decision_packet_report_path.exists() {
                mismatches.push(format!(
                    "unexpected nested decision-packet report artifact exists at {} while current latest-decision-packet resolution still fails",
                    paths.decision_packet_report_path.display()
                ));
            }
            if let Some(_) = &failure.latest_execute_plan_raw {
                let stored_latest_execute: ExecuteLatestReportView = load_required_step_json(
                    &status.latest_execute_step,
                    &paths.latest_execute_report_path,
                    "latest_execute_step",
                    &mut mismatches,
                )?;
                compare_step_artifact_against_report_metadata(
                    status.latest_execute_step.as_ref(),
                    &paths.latest_execute_report_path,
                    "plan_latest_execute",
                    &stored_latest_execute.verdict,
                    &stored_latest_execute.reason,
                    stored_latest_execute.generated_at,
                    "stored latest_execute_step on failure",
                    &mut mismatches,
                );
                compare_option_string(
                    Some(stored_latest_execute.verdict.as_str()),
                    failure.latest_execute_verdict.as_deref(),
                    "stored latest_execute verdict on failure",
                    &mut mismatches,
                );
            }
            if let Some(_) = &failure.decision_packet_plan_raw {
                let stored_plan: DecisionPacketReportView = load_required_step_json(
                    &status.decision_packet_plan_step,
                    &paths.decision_packet_plan_report_path,
                    "decision_packet_plan_step",
                    &mut mismatches,
                )?;
                compare_step_artifact_against_report_metadata(
                    status.decision_packet_plan_step.as_ref(),
                    &paths.decision_packet_plan_report_path,
                    "plan_live_package_decision_packet",
                    &stored_plan.verdict,
                    &stored_plan.reason,
                    stored_plan.generated_at,
                    "stored decision_packet_plan_step on failure",
                    &mut mismatches,
                );
                compare_option_string(
                    Some(stored_plan.verdict.as_str()),
                    failure.decision_packet_plan_verdict.as_deref(),
                    "stored decision_packet plan verdict on failure",
                    &mut mismatches,
                );
            }
        }
    }

    let verdict = if mismatches.is_empty() {
        TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestVerifyOk
    } else {
        TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestVerifyInvalid
    };
    let reason = if mismatches.is_empty() {
        format!(
            "latest-decision-packet wrapper under {} is coherent and correctly binds the latest immutable chain to the accepted decision-packet contract",
            session_dir.display()
        )
    } else {
        mismatches[0].clone()
    };
    Ok(report_from_verify(
        session_dir,
        &paths,
        &session,
        &status,
        verdict,
        reason,
        mismatches,
    ))
}

fn resolve_latest_decision_packet(
    root: &Path,
    paths: &PackageDecisionPacketLatestPaths,
) -> std::result::Result<ResolvedLatestDecisionPacket, LatestDecisionPacketResolutionFailure> {
    let latest_execute_plan_raw = match run_json_command(
        EXECUTE_LATEST_BIN,
        &execute_latest_plan_args(root, &paths.shadow_execute_latest_session_dir),
    ) {
        Ok(value) => value,
        Err(error) => {
            return Err(LatestDecisionPacketResolutionFailure {
                kind: ResolutionFailureKind::MissingLatestChain,
                reason: format!(
                    "failed resolving latest immutable chain under {} via accepted execute_latest handoff: {error}",
                    root.display()
                ),
                snapshot: None,
                latest_execute_plan_raw: None,
                latest_execute_verdict: None,
                latest_execute_reason: None,
                latest_execute_generated_at: None,
                decision_packet_plan_raw: None,
                decision_packet_plan_verdict: None,
                decision_packet_plan_reason: None,
                decision_packet_plan_generated_at: None,
            });
        }
    };
    let latest_execute_plan: ExecuteLatestReportView =
        serde_json::from_value(latest_execute_plan_raw.clone()).map_err(|error| {
            LatestDecisionPacketResolutionFailure {
                kind: ResolutionFailureKind::InvalidLatestChain,
                reason: format!(
                    "failed parsing execute_latest plan json for {}: {error}",
                    root.display()
                ),
                snapshot: None,
                latest_execute_plan_raw: Some(latest_execute_plan_raw.clone()),
                latest_execute_verdict: None,
                latest_execute_reason: None,
                latest_execute_generated_at: None,
                decision_packet_plan_raw: None,
                decision_packet_plan_verdict: None,
                decision_packet_plan_reason: None,
                decision_packet_plan_generated_at: None,
            }
        })?;
    let maybe_snapshot = maybe_snapshot_from_execute_latest(&latest_execute_plan);
    if latest_execute_plan.verdict != EXECUTE_LATEST_PLAN_READY {
        return Err(LatestDecisionPacketResolutionFailure {
            kind: map_execute_latest_failure_kind(&latest_execute_plan.verdict),
            reason: latest_execute_plan.reason.clone(),
            snapshot: maybe_snapshot,
            latest_execute_plan_raw: Some(latest_execute_plan_raw),
            latest_execute_verdict: Some(latest_execute_plan.verdict),
            latest_execute_reason: Some(latest_execute_plan.reason),
            latest_execute_generated_at: Some(latest_execute_plan.generated_at),
            decision_packet_plan_raw: None,
            decision_packet_plan_verdict: None,
            decision_packet_plan_reason: None,
            decision_packet_plan_generated_at: None,
        });
    }

    let snapshot = parse_snapshot_from_execute_latest(&latest_execute_plan).map_err(|error| {
        LatestDecisionPacketResolutionFailure {
            kind: ResolutionFailureKind::InvalidLatestChain,
            reason: error.to_string(),
            snapshot: maybe_snapshot_from_execute_latest(&latest_execute_plan),
            latest_execute_plan_raw: Some(latest_execute_plan_raw.clone()),
            latest_execute_verdict: Some(latest_execute_plan.verdict.clone()),
            latest_execute_reason: Some(latest_execute_plan.reason.clone()),
            latest_execute_generated_at: Some(latest_execute_plan.generated_at),
            decision_packet_plan_raw: None,
            decision_packet_plan_verdict: None,
            decision_packet_plan_reason: None,
            decision_packet_plan_generated_at: None,
        }
    })?;

    let mut latest_execute_mismatches = Vec::new();
    compare_execute_latest_copy_against_actual(
        &latest_execute_plan,
        &latest_execute_plan,
        &paths.shadow_execute_latest_session_dir,
        &mut latest_execute_mismatches,
    );
    if !latest_execute_mismatches.is_empty() {
        return Err(LatestDecisionPacketResolutionFailure {
            kind: ResolutionFailureKind::DriftedDecisionPacketContract,
            reason: latest_execute_mismatches[0].clone(),
            snapshot: Some(snapshot),
            latest_execute_plan_raw: Some(latest_execute_plan_raw),
            latest_execute_verdict: Some(latest_execute_plan.verdict),
            latest_execute_reason: Some(latest_execute_plan.reason),
            latest_execute_generated_at: Some(latest_execute_plan.generated_at),
            decision_packet_plan_raw: None,
            decision_packet_plan_verdict: None,
            decision_packet_plan_reason: None,
            decision_packet_plan_generated_at: None,
        });
    }

    let decision_packet_plan_raw = run_json_command(
        DECISION_PACKET_BIN,
        &decision_packet_args(
            &snapshot.historical_execute_frozen_session_dir,
            &paths.nested_decision_packet_session_dir,
            "--plan-live-package-decision-packet",
        ),
    )
    .map_err(|error| LatestDecisionPacketResolutionFailure {
        kind: ResolutionFailureKind::DriftedDecisionPacketContract,
        reason: format!(
            "failed planning downstream decision-packet contract under {}: {error}",
            paths.nested_decision_packet_session_dir.display()
        ),
        snapshot: Some(snapshot.clone()),
        latest_execute_plan_raw: Some(latest_execute_plan_raw.clone()),
        latest_execute_verdict: Some(latest_execute_plan.verdict.clone()),
        latest_execute_reason: Some(latest_execute_plan.reason.clone()),
        latest_execute_generated_at: Some(latest_execute_plan.generated_at),
        decision_packet_plan_raw: None,
        decision_packet_plan_verdict: None,
        decision_packet_plan_reason: None,
        decision_packet_plan_generated_at: None,
    })?;
    let decision_packet_plan: DecisionPacketReportView =
        serde_json::from_value(decision_packet_plan_raw.clone()).map_err(|error| {
            LatestDecisionPacketResolutionFailure {
                kind: ResolutionFailureKind::DriftedDecisionPacketContract,
                reason: format!("failed parsing decision-packet plan json: {error}"),
                snapshot: Some(snapshot.clone()),
                latest_execute_plan_raw: Some(latest_execute_plan_raw.clone()),
                latest_execute_verdict: Some(latest_execute_plan.verdict.clone()),
                latest_execute_reason: Some(latest_execute_plan.reason.clone()),
                latest_execute_generated_at: Some(latest_execute_plan.generated_at),
                decision_packet_plan_raw: Some(decision_packet_plan_raw.clone()),
                decision_packet_plan_verdict: None,
                decision_packet_plan_reason: None,
                decision_packet_plan_generated_at: None,
            }
        })?;
    if decision_packet_plan.verdict != DECISION_PACKET_PLAN_READY {
        return Err(LatestDecisionPacketResolutionFailure {
            kind: ResolutionFailureKind::DriftedDecisionPacketContract,
            reason: decision_packet_plan.reason.clone(),
            snapshot: Some(snapshot.clone()),
            latest_execute_plan_raw: Some(latest_execute_plan_raw),
            latest_execute_verdict: Some(latest_execute_plan.verdict),
            latest_execute_reason: Some(latest_execute_plan.reason),
            latest_execute_generated_at: Some(latest_execute_plan.generated_at),
            decision_packet_plan_raw: Some(decision_packet_plan_raw),
            decision_packet_plan_verdict: Some(decision_packet_plan.verdict),
            decision_packet_plan_reason: Some(decision_packet_plan.reason),
            decision_packet_plan_generated_at: Some(decision_packet_plan.generated_at),
        });
    }
    let mut decision_packet_plan_mismatches = Vec::new();
    compare_decision_packet_plan_against_snapshot(
        &decision_packet_plan,
        &snapshot,
        &paths.nested_decision_packet_session_dir,
        &mut decision_packet_plan_mismatches,
    );
    if !decision_packet_plan_mismatches.is_empty() {
        return Err(LatestDecisionPacketResolutionFailure {
            kind: ResolutionFailureKind::DriftedDecisionPacketContract,
            reason: decision_packet_plan_mismatches[0].clone(),
            snapshot: Some(snapshot),
            latest_execute_plan_raw: Some(latest_execute_plan_raw),
            latest_execute_verdict: Some(latest_execute_plan.verdict),
            latest_execute_reason: Some(latest_execute_plan.reason),
            latest_execute_generated_at: Some(latest_execute_plan.generated_at),
            decision_packet_plan_raw: Some(decision_packet_plan_raw),
            decision_packet_plan_verdict: Some(decision_packet_plan.verdict),
            decision_packet_plan_reason: Some(decision_packet_plan.reason),
            decision_packet_plan_generated_at: Some(decision_packet_plan.generated_at),
        });
    }

    Ok(ResolvedLatestDecisionPacket {
        snapshot,
        latest_execute_plan_raw,
        latest_execute_plan,
        decision_packet_plan_raw,
        decision_packet_plan,
    })
}

fn parse_snapshot_from_execute_latest(
    view: &ExecuteLatestReportView,
) -> Result<LatestDecisionPacketSnapshot> {
    let latest_top_step_name =
        required_option_string(view.latest_top_step_name.as_ref(), "latest_top_step_name")?;
    if latest_top_step_name != TOP_ACCEPTED_PACKAGE_STEP {
        bail!(
            "execute_latest plan top accepted step is {} instead of {}",
            latest_top_step_name,
            TOP_ACCEPTED_PACKAGE_STEP
        );
    }
    Ok(LatestDecisionPacketSnapshot {
        latest_top_step_name,
        latest_top_session_dir: PathBuf::from(required_option_string(
            view.latest_top_session_dir.as_ref(),
            "latest_top_session_dir",
        )?),
        historical_execute_frozen_session_dir: PathBuf::from(required_option_string(
            view.latest_chain_execute_frozen_session_dir.as_ref(),
            "latest_chain_execute_frozen_session_dir",
        )?),
        turn_green_session_dir: PathBuf::from(required_option_string(
            view.turn_green_session_dir.as_ref(),
            "turn_green_session_dir",
        )?),
        launch_packet_session_dir: PathBuf::from(required_option_string(
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
            .ok_or_else(|| anyhow!("execute_latest plan is missing wrapper_timeout_ms"))?,
        service_status_max_staleness_ms: view.service_status_max_staleness_ms.ok_or_else(|| {
            anyhow!("execute_latest plan is missing service_status_max_staleness_ms")
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
        current_pre_activation_gate_verdict: extract_non_empty_string_from_option(
            view.current_pre_activation_gate_verdict.as_deref(),
        )
        .map(ToOwned::to_owned),
        current_pre_activation_gate_reason: extract_non_empty_string_from_option(
            view.current_pre_activation_gate_reason.as_deref(),
        )
        .map(ToOwned::to_owned),
    })
}

fn maybe_snapshot_from_execute_latest(
    view: &ExecuteLatestReportView,
) -> Option<LatestDecisionPacketSnapshot> {
    parse_snapshot_from_execute_latest(view).ok()
}

fn map_execute_latest_failure_kind(verdict: &str) -> ResolutionFailureKind {
    match verdict {
        "tiny_live_package_execute_latest_refused_by_missing_latest_chain" => {
            ResolutionFailureKind::MissingLatestChain
        }
        "tiny_live_package_execute_latest_refused_by_invalid_latest_chain" => {
            ResolutionFailureKind::InvalidLatestChain
        }
        _ => ResolutionFailureKind::DriftedDecisionPacketContract,
    }
}

fn planned_session(
    root: &Path,
    session_dir: &Path,
    paths: &PackageDecisionPacketLatestPaths,
    resolved: Option<&ResolvedLatestDecisionPacket>,
    failure: Option<&LatestDecisionPacketResolutionFailure>,
) -> PackageDecisionPacketLatestSession {
    let snapshot = resolved
        .map(|value| &value.snapshot)
        .or_else(|| failure.and_then(|value| value.snapshot.as_ref()));
    PackageDecisionPacketLatestSession {
        session_version: DECISION_PACKET_LATEST_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        root: root.display().to_string(),
        session_dir: session_dir.display().to_string(),
        shadow_execute_latest_session_dir: paths
            .shadow_execute_latest_session_dir
            .display()
            .to_string(),
        nested_decision_packet_session_dir: paths
            .nested_decision_packet_session_dir
            .display()
            .to_string(),
        latest_top_step_name: snapshot.map(|value| value.latest_top_step_name.clone()),
        latest_top_session_dir: snapshot
            .map(|value| value.latest_top_session_dir.display().to_string()),
        historical_execute_frozen_session_dir: snapshot.map(|value| {
            value
                .historical_execute_frozen_session_dir
                .display()
                .to_string()
        }),
        turn_green_session_dir: snapshot
            .map(|value| value.turn_green_session_dir.display().to_string()),
        launch_packet_session_dir: snapshot
            .map(|value| value.launch_packet_session_dir.display().to_string()),
        package_dir: snapshot.map(|value| value.package_dir.display().to_string()),
        install_root: snapshot.map(|value| value.install_root.display().to_string()),
        target_service_name: snapshot.map(|value| value.target_service_name.clone()),
        backend_command: snapshot.map(|value| value.backend_command.clone()),
        wrapper_timeout_ms: snapshot.map(|value| value.wrapper_timeout_ms),
        service_status_max_staleness_ms: snapshot
            .map(|value| value.service_status_max_staleness_ms),
        resolve_latest_chain_command_summary: resolve_latest_chain_command_summary(
            root,
            &paths.shadow_execute_latest_session_dir,
        ),
        plan_decision_packet_command_summary: snapshot.map(|value| {
            plan_decision_packet_command_summary(value, &paths.nested_decision_packet_session_dir)
        }),
        downstream_decision_packet_command_summary: snapshot.map(|value| {
            downstream_decision_packet_command_summary(
                value,
                &paths.nested_decision_packet_session_dir,
            )
        }),
        verify_decision_packet_command_summary: snapshot.map(|value| {
            verify_decision_packet_command_summary(value, &paths.nested_decision_packet_session_dir)
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
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn refusal_status_from_resolution_failure(
    root: &Path,
    session_dir: &Path,
    paths: &PackageDecisionPacketLatestPaths,
    failure: &LatestDecisionPacketResolutionFailure,
) -> PackageDecisionPacketLatestStatus {
    let snapshot = failure.snapshot.as_ref();
    PackageDecisionPacketLatestStatus {
        status_version: DECISION_PACKET_LATEST_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        root: root.display().to_string(),
        session_dir: session_dir.display().to_string(),
        latest_top_step_name: snapshot.map(|value| value.latest_top_step_name.clone()),
        latest_top_session_dir: snapshot
            .map(|value| value.latest_top_session_dir.display().to_string()),
        historical_execute_frozen_session_dir: snapshot.map(|value| {
            value
                .historical_execute_frozen_session_dir
                .display()
                .to_string()
        }),
        turn_green_session_dir: snapshot
            .map(|value| value.turn_green_session_dir.display().to_string()),
        launch_packet_session_dir: snapshot
            .map(|value| value.launch_packet_session_dir.display().to_string()),
        nested_decision_packet_session_dir: paths
            .nested_decision_packet_session_dir
            .display()
            .to_string(),
        result: result_from_failure_kind(failure.kind),
        reason: failure.reason.clone(),
        latest_execute_verdict: failure.latest_execute_verdict.clone(),
        decision_packet_plan_verdict: failure.decision_packet_plan_verdict.clone(),
        current_pre_activation_gate_verdict: snapshot
            .and_then(|value| value.current_pre_activation_gate_verdict.clone()),
        current_pre_activation_gate_reason: snapshot
            .and_then(|value| value.current_pre_activation_gate_reason.clone()),
        latest_execute_step: failure.latest_execute_plan_raw.as_ref().map(|_| {
            step_artifact(
                &paths.latest_execute_report_path,
                "plan_latest_execute",
                failure
                    .latest_execute_verdict
                    .as_deref()
                    .unwrap_or("tiny_live_package_execute_latest_verify_invalid"),
                failure
                    .latest_execute_reason
                    .as_deref()
                    .unwrap_or("missing latest_execute failure reason"),
                failure.latest_execute_generated_at.unwrap_or_else(Utc::now),
            )
        }),
        decision_packet_plan_step: failure.decision_packet_plan_raw.as_ref().map(|_| {
            step_artifact(
                &paths.decision_packet_plan_report_path,
                "plan_live_package_decision_packet",
                failure
                    .decision_packet_plan_verdict
                    .as_deref()
                    .unwrap_or("tiny_live_package_decision_packet_verify_invalid"),
                failure
                    .decision_packet_plan_reason
                    .as_deref()
                    .unwrap_or("missing decision_packet plan failure reason"),
                failure
                    .decision_packet_plan_generated_at
                    .unwrap_or_else(Utc::now),
            )
        }),
        decision_packet_step: None,
        operator_next_action_summary: operator_next_action_summary(result_from_failure_kind(
            failure.kind,
        )),
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn refusal_status(
    root: &Path,
    session_dir: &Path,
    paths: &PackageDecisionPacketLatestPaths,
    resolved: Option<&ResolvedLatestDecisionPacket>,
    result: LivePackageDecisionPacketLatestResult,
    reason: String,
    latest_execute_step: Option<PackageDecisionPacketLatestStepArtifact>,
    decision_packet_plan_step: Option<PackageDecisionPacketLatestStepArtifact>,
    decision_packet_step: Option<PackageDecisionPacketLatestStepArtifact>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
) -> PackageDecisionPacketLatestStatus {
    let snapshot = resolved.map(|value| &value.snapshot);
    PackageDecisionPacketLatestStatus {
        status_version: DECISION_PACKET_LATEST_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        root: root.display().to_string(),
        session_dir: session_dir.display().to_string(),
        latest_top_step_name: snapshot.map(|value| value.latest_top_step_name.clone()),
        latest_top_session_dir: snapshot
            .map(|value| value.latest_top_session_dir.display().to_string()),
        historical_execute_frozen_session_dir: snapshot.map(|value| {
            value
                .historical_execute_frozen_session_dir
                .display()
                .to_string()
        }),
        turn_green_session_dir: snapshot
            .map(|value| value.turn_green_session_dir.display().to_string()),
        launch_packet_session_dir: snapshot
            .map(|value| value.launch_packet_session_dir.display().to_string()),
        nested_decision_packet_session_dir: paths
            .nested_decision_packet_session_dir
            .display()
            .to_string(),
        result,
        reason,
        latest_execute_verdict: resolved.map(|value| value.latest_execute_plan.verdict.clone()),
        decision_packet_plan_verdict: resolved
            .map(|value| value.decision_packet_plan.verdict.clone()),
        current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason,
        latest_execute_step,
        decision_packet_plan_step,
        decision_packet_step,
        operator_next_action_summary: operator_next_action_summary(result),
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn plan_report_from_resolution(
    mode: &str,
    verdict: TinyLivePackageDecisionPacketLatestVerdict,
    reason: String,
    root: &Path,
    session_dir: &Path,
    paths: &PackageDecisionPacketLatestPaths,
    resolved: &ResolvedLatestDecisionPacket,
) -> PackageDecisionPacketLatestReport {
    PackageDecisionPacketLatestReport {
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
        historical_execute_frozen_session_dir: Some(
            resolved
                .snapshot
                .historical_execute_frozen_session_dir
                .display()
                .to_string(),
        ),
        turn_green_session_dir: Some(
            resolved
                .snapshot
                .turn_green_session_dir
                .display()
                .to_string(),
        ),
        launch_packet_session_dir: Some(
            resolved
                .snapshot
                .launch_packet_session_dir
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
        decision_packet_latest_session_path: Some(paths.session_path.display().to_string()),
        decision_packet_latest_status_path: Some(paths.status_path.display().to_string()),
        decision_packet_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_execute_report_path: Some(paths.latest_execute_report_path.display().to_string()),
        decision_packet_plan_report_path: Some(
            paths.decision_packet_plan_report_path.display().to_string(),
        ),
        nested_decision_packet_report_path: Some(
            paths.decision_packet_report_path.display().to_string(),
        ),
        shadow_execute_latest_session_dir: Some(
            paths
                .shadow_execute_latest_session_dir
                .display()
                .to_string(),
        ),
        nested_decision_packet_session_dir: Some(
            paths
                .nested_decision_packet_session_dir
                .display()
                .to_string(),
        ),
        latest_execute_verdict: Some(resolved.latest_execute_plan.verdict.clone()),
        decision_packet_plan_verdict: Some(resolved.decision_packet_plan.verdict.clone()),
        result: None,
        current_pre_activation_gate_verdict: resolved
            .snapshot
            .current_pre_activation_gate_verdict
            .clone(),
        current_pre_activation_gate_reason: resolved
            .snapshot
            .current_pre_activation_gate_reason
            .clone(),
        resolve_latest_chain_command_summary: Some(resolve_latest_chain_command_summary(
            root,
            &paths.shadow_execute_latest_session_dir,
        )),
        plan_decision_packet_command_summary: Some(plan_decision_packet_command_summary(
            &resolved.snapshot,
            &paths.nested_decision_packet_session_dir,
        )),
        downstream_decision_packet_command_summary: Some(
            downstream_decision_packet_command_summary(
                &resolved.snapshot,
                &paths.nested_decision_packet_session_dir,
            ),
        ),
        verify_decision_packet_command_summary: Some(verify_decision_packet_command_summary(
            &resolved.snapshot,
            &paths.nested_decision_packet_session_dir,
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
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn report_from_resolution_failure(
    mode: &str,
    root: &Path,
    session_dir: &Path,
    paths: &PackageDecisionPacketLatestPaths,
    failure: &LatestDecisionPacketResolutionFailure,
) -> PackageDecisionPacketLatestReport {
    let snapshot = failure.snapshot.as_ref();
    PackageDecisionPacketLatestReport {
        generated_at: Utc::now(),
        mode: mode.to_string(),
        verdict: verdict_from_failure_kind(failure.kind),
        reason: failure.reason.clone(),
        root: Some(root.display().to_string()),
        latest_top_step_name: snapshot.map(|value| value.latest_top_step_name.clone()),
        latest_top_session_dir: snapshot
            .map(|value| value.latest_top_session_dir.display().to_string()),
        historical_execute_frozen_session_dir: snapshot.map(|value| {
            value
                .historical_execute_frozen_session_dir
                .display()
                .to_string()
        }),
        turn_green_session_dir: snapshot
            .map(|value| value.turn_green_session_dir.display().to_string()),
        launch_packet_session_dir: snapshot
            .map(|value| value.launch_packet_session_dir.display().to_string()),
        package_dir: snapshot.map(|value| value.package_dir.display().to_string()),
        install_root: snapshot.map(|value| value.install_root.display().to_string()),
        target_service_name: snapshot.map(|value| value.target_service_name.clone()),
        backend_command: snapshot.map(|value| value.backend_command.clone()),
        wrapper_timeout_ms: snapshot.map(|value| value.wrapper_timeout_ms),
        service_status_max_staleness_ms: snapshot
            .map(|value| value.service_status_max_staleness_ms),
        session_dir: Some(session_dir.display().to_string()),
        decision_packet_latest_session_path: Some(paths.session_path.display().to_string()),
        decision_packet_latest_status_path: Some(paths.status_path.display().to_string()),
        decision_packet_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_execute_report_path: Some(paths.latest_execute_report_path.display().to_string()),
        decision_packet_plan_report_path: Some(
            paths.decision_packet_plan_report_path.display().to_string(),
        ),
        nested_decision_packet_report_path: Some(
            paths.decision_packet_report_path.display().to_string(),
        ),
        shadow_execute_latest_session_dir: Some(
            paths
                .shadow_execute_latest_session_dir
                .display()
                .to_string(),
        ),
        nested_decision_packet_session_dir: Some(
            paths
                .nested_decision_packet_session_dir
                .display()
                .to_string(),
        ),
        latest_execute_verdict: failure.latest_execute_verdict.clone(),
        decision_packet_plan_verdict: failure.decision_packet_plan_verdict.clone(),
        result: Some(serialize_enum(&result_from_failure_kind(failure.kind))),
        current_pre_activation_gate_verdict: snapshot
            .and_then(|value| value.current_pre_activation_gate_verdict.clone()),
        current_pre_activation_gate_reason: snapshot
            .and_then(|value| value.current_pre_activation_gate_reason.clone()),
        resolve_latest_chain_command_summary: Some(resolve_latest_chain_command_summary(
            root,
            &paths.shadow_execute_latest_session_dir,
        )),
        plan_decision_packet_command_summary: snapshot.map(|value| {
            plan_decision_packet_command_summary(value, &paths.nested_decision_packet_session_dir)
        }),
        downstream_decision_packet_command_summary: snapshot.map(|value| {
            downstream_decision_packet_command_summary(
                value,
                &paths.nested_decision_packet_session_dir,
            )
        }),
        verify_decision_packet_command_summary: snapshot.map(|value| {
            verify_decision_packet_command_summary(value, &paths.nested_decision_packet_session_dir)
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
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn report_from_status(
    mode: &str,
    verdict: TinyLivePackageDecisionPacketLatestVerdict,
    session_dir: &Path,
    paths: &PackageDecisionPacketLatestPaths,
    session: &PackageDecisionPacketLatestSession,
    status: &PackageDecisionPacketLatestStatus,
    verification_mismatches: Vec<String>,
) -> PackageDecisionPacketLatestReport {
    PackageDecisionPacketLatestReport {
        generated_at: Utc::now(),
        mode: mode.to_string(),
        verdict,
        reason: status.reason.clone(),
        root: Some(session.root.clone()),
        latest_top_step_name: session.latest_top_step_name.clone(),
        latest_top_session_dir: session.latest_top_session_dir.clone(),
        historical_execute_frozen_session_dir: session
            .historical_execute_frozen_session_dir
            .clone(),
        turn_green_session_dir: session.turn_green_session_dir.clone(),
        launch_packet_session_dir: session.launch_packet_session_dir.clone(),
        package_dir: session.package_dir.clone(),
        install_root: session.install_root.clone(),
        target_service_name: session.target_service_name.clone(),
        backend_command: session.backend_command.clone(),
        wrapper_timeout_ms: session.wrapper_timeout_ms,
        service_status_max_staleness_ms: session.service_status_max_staleness_ms,
        session_dir: Some(session_dir.display().to_string()),
        decision_packet_latest_session_path: Some(paths.session_path.display().to_string()),
        decision_packet_latest_status_path: Some(paths.status_path.display().to_string()),
        decision_packet_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_execute_report_path: Some(paths.latest_execute_report_path.display().to_string()),
        decision_packet_plan_report_path: Some(
            paths.decision_packet_plan_report_path.display().to_string(),
        ),
        nested_decision_packet_report_path: Some(
            paths.decision_packet_report_path.display().to_string(),
        ),
        shadow_execute_latest_session_dir: Some(
            paths
                .shadow_execute_latest_session_dir
                .display()
                .to_string(),
        ),
        nested_decision_packet_session_dir: Some(
            paths
                .nested_decision_packet_session_dir
                .display()
                .to_string(),
        ),
        latest_execute_verdict: status.latest_execute_verdict.clone(),
        decision_packet_plan_verdict: status.decision_packet_plan_verdict.clone(),
        result: Some(serialize_enum(&status.result)),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        resolve_latest_chain_command_summary: Some(
            session.resolve_latest_chain_command_summary.clone(),
        ),
        plan_decision_packet_command_summary: session.plan_decision_packet_command_summary.clone(),
        downstream_decision_packet_command_summary: session
            .downstream_decision_packet_command_summary
            .clone(),
        verify_decision_packet_command_summary: session
            .verify_decision_packet_command_summary
            .clone(),
        reviewed_frozen_live_cutover_controller_command_summary: session
            .reviewed_frozen_live_cutover_controller_command_summary
            .clone(),
        clerestory_certificate_summary: session.clerestory_certificate_summary.clone(),
        clerestory_certificate_sha256: session.clerestory_certificate_sha256.clone(),
        clerestory_certificate_algorithm: session.clerestory_certificate_algorithm.clone(),
        verification_mismatches,
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn report_from_verify(
    session_dir: &Path,
    paths: &PackageDecisionPacketLatestPaths,
    session: &PackageDecisionPacketLatestSession,
    status: &PackageDecisionPacketLatestStatus,
    verdict: TinyLivePackageDecisionPacketLatestVerdict,
    reason: String,
    verification_mismatches: Vec<String>,
) -> PackageDecisionPacketLatestReport {
    let mut report = report_from_status(
        "verify_latest_decision_packet",
        verdict,
        session_dir,
        paths,
        session,
        status,
        verification_mismatches,
    );
    report.reason = reason;
    report
}

fn verify_invalid_report_without_session(
    session_dir: &Path,
    mismatches: Vec<String>,
) -> PackageDecisionPacketLatestReport {
    PackageDecisionPacketLatestReport {
        generated_at: Utc::now(),
        mode: "verify_latest_decision_packet".to_string(),
        verdict: TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestVerifyInvalid,
        reason: mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "latest-decision-packet verification failed".to_string()),
        root: None,
        latest_top_step_name: None,
        latest_top_session_dir: None,
        historical_execute_frozen_session_dir: None,
        turn_green_session_dir: None,
        launch_packet_session_dir: None,
        package_dir: None,
        install_root: None,
        target_service_name: None,
        backend_command: None,
        wrapper_timeout_ms: None,
        service_status_max_staleness_ms: None,
        session_dir: Some(session_dir.display().to_string()),
        decision_packet_latest_session_path: None,
        decision_packet_latest_status_path: None,
        decision_packet_latest_report_path: None,
        latest_execute_report_path: None,
        decision_packet_plan_report_path: None,
        nested_decision_packet_report_path: None,
        shadow_execute_latest_session_dir: None,
        nested_decision_packet_session_dir: None,
        latest_execute_verdict: None,
        decision_packet_plan_verdict: None,
        result: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        resolve_latest_chain_command_summary: None,
        plan_decision_packet_command_summary: None,
        downstream_decision_packet_command_summary: None,
        verify_decision_packet_command_summary: None,
        reviewed_frozen_live_cutover_controller_command_summary: None,
        clerestory_certificate_summary: None,
        clerestory_certificate_sha256: None,
        clerestory_certificate_algorithm: None,
        verification_mismatches: mismatches,
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn compare_session_against_snapshot(
    session: &PackageDecisionPacketLatestSession,
    root: &Path,
    session_dir: &Path,
    paths: &PackageDecisionPacketLatestPaths,
    resolved: &ResolvedLatestDecisionPacket,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &session.root,
        &root.display().to_string(),
        "stored session root against current snapshot",
        mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "stored session session_dir against current snapshot",
        mismatches,
    );
    compare_string(
        &session.shadow_execute_latest_session_dir,
        &paths
            .shadow_execute_latest_session_dir
            .display()
            .to_string(),
        "stored session shadow_execute_latest_session_dir",
        mismatches,
    );
    compare_string(
        &session.nested_decision_packet_session_dir,
        &paths
            .nested_decision_packet_session_dir
            .display()
            .to_string(),
        "stored session nested_decision_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        session.latest_top_step_name.as_deref(),
        Some(resolved.snapshot.latest_top_step_name.as_str()),
        "stored session latest_top_step_name",
        mismatches,
    );
    compare_option_string(
        session.latest_top_session_dir.as_deref(),
        Some(
            resolved
                .snapshot
                .latest_top_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored session latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        session.historical_execute_frozen_session_dir.as_deref(),
        Some(
            resolved
                .snapshot
                .historical_execute_frozen_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored session historical_execute_frozen_session_dir",
        mismatches,
    );
    compare_option_string(
        session.turn_green_session_dir.as_deref(),
        Some(
            resolved
                .snapshot
                .turn_green_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored session turn_green_session_dir",
        mismatches,
    );
    compare_option_string(
        session.launch_packet_session_dir.as_deref(),
        Some(
            resolved
                .snapshot
                .launch_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored session launch_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        session.package_dir.as_deref(),
        Some(resolved.snapshot.package_dir.display().to_string().as_str()),
        "stored session package_dir",
        mismatches,
    );
    compare_option_string(
        session.install_root.as_deref(),
        Some(
            resolved
                .snapshot
                .install_root
                .display()
                .to_string()
                .as_str(),
        ),
        "stored session install_root",
        mismatches,
    );
    compare_option_string(
        session.target_service_name.as_deref(),
        Some(resolved.snapshot.target_service_name.as_str()),
        "stored session target_service_name",
        mismatches,
    );
    compare_option_string(
        session.backend_command.as_deref(),
        Some(resolved.snapshot.backend_command.as_str()),
        "stored session backend_command",
        mismatches,
    );
    compare_option_u64(
        session.wrapper_timeout_ms,
        Some(resolved.snapshot.wrapper_timeout_ms),
        "stored session wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        session.service_status_max_staleness_ms,
        Some(resolved.snapshot.service_status_max_staleness_ms),
        "stored session service_status_max_staleness_ms",
        mismatches,
    );
    compare_string(
        &session.resolve_latest_chain_command_summary,
        &resolve_latest_chain_command_summary(root, &paths.shadow_execute_latest_session_dir),
        "stored session resolve_latest_chain_command_summary",
        mismatches,
    );
    compare_option_string(
        session.plan_decision_packet_command_summary.as_deref(),
        Some(
            plan_decision_packet_command_summary(
                &resolved.snapshot,
                &paths.nested_decision_packet_session_dir,
            )
            .as_str(),
        ),
        "stored session plan_decision_packet_command_summary",
        mismatches,
    );
    compare_option_string(
        session
            .downstream_decision_packet_command_summary
            .as_deref(),
        Some(
            downstream_decision_packet_command_summary(
                &resolved.snapshot,
                &paths.nested_decision_packet_session_dir,
            )
            .as_str(),
        ),
        "stored session downstream_decision_packet_command_summary",
        mismatches,
    );
    compare_option_string(
        session.verify_decision_packet_command_summary.as_deref(),
        Some(
            verify_decision_packet_command_summary(
                &resolved.snapshot,
                &paths.nested_decision_packet_session_dir,
            )
            .as_str(),
        ),
        "stored session verify_decision_packet_command_summary",
        mismatches,
    );
    compare_option_string(
        session
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        Some(
            resolved
                .snapshot
                .reviewed_frozen_live_cutover_controller_command_summary
                .as_str(),
        ),
        "stored session reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_option_string(
        session.clerestory_certificate_summary.as_deref(),
        Some(resolved.snapshot.clerestory_certificate_summary.as_str()),
        "stored session clerestory_certificate_summary",
        mismatches,
    );
    compare_option_string(
        session.clerestory_certificate_sha256.as_deref(),
        Some(resolved.snapshot.clerestory_certificate_sha256.as_str()),
        "stored session clerestory_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        session.clerestory_certificate_algorithm.as_deref(),
        Some(resolved.snapshot.clerestory_certificate_algorithm.as_str()),
        "stored session clerestory_certificate_algorithm",
        mismatches,
    );
}

fn compare_report_against_snapshot(
    report: &PackageDecisionPacketLatestReport,
    root: &Path,
    session_dir: &Path,
    paths: &PackageDecisionPacketLatestPaths,
    resolved: &ResolvedLatestDecisionPacket,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        report.root.as_deref(),
        Some(root.display().to_string().as_str()),
        "stored report root against current snapshot",
        mismatches,
    );
    compare_option_string(
        report.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored report session_dir",
        mismatches,
    );
    compare_option_string(
        report.decision_packet_latest_session_path.as_deref(),
        Some(paths.session_path.display().to_string().as_str()),
        "stored report session path",
        mismatches,
    );
    compare_option_string(
        report.decision_packet_latest_status_path.as_deref(),
        Some(paths.status_path.display().to_string().as_str()),
        "stored report status path",
        mismatches,
    );
    compare_option_string(
        report.decision_packet_latest_report_path.as_deref(),
        Some(paths.report_path.display().to_string().as_str()),
        "stored report report path",
        mismatches,
    );
    compare_option_string(
        report.latest_execute_report_path.as_deref(),
        Some(
            paths
                .latest_execute_report_path
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report latest_execute_report_path",
        mismatches,
    );
    compare_option_string(
        report.decision_packet_plan_report_path.as_deref(),
        Some(
            paths
                .decision_packet_plan_report_path
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report decision_packet_plan_report_path",
        mismatches,
    );
    compare_option_string(
        report.nested_decision_packet_report_path.as_deref(),
        Some(
            paths
                .decision_packet_report_path
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report nested_decision_packet_report_path",
        mismatches,
    );
    compare_option_string(
        report.shadow_execute_latest_session_dir.as_deref(),
        Some(
            paths
                .shadow_execute_latest_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report shadow_execute_latest_session_dir",
        mismatches,
    );
    compare_option_string(
        report.nested_decision_packet_session_dir.as_deref(),
        Some(
            paths
                .nested_decision_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report nested_decision_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        report.latest_top_step_name.as_deref(),
        Some(resolved.snapshot.latest_top_step_name.as_str()),
        "stored report latest_top_step_name",
        mismatches,
    );
    compare_option_string(
        report.latest_top_session_dir.as_deref(),
        Some(
            resolved
                .snapshot
                .latest_top_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        report.historical_execute_frozen_session_dir.as_deref(),
        Some(
            resolved
                .snapshot
                .historical_execute_frozen_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report historical_execute_frozen_session_dir",
        mismatches,
    );
    compare_option_string(
        report.turn_green_session_dir.as_deref(),
        Some(
            resolved
                .snapshot
                .turn_green_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report turn_green_session_dir",
        mismatches,
    );
    compare_option_string(
        report.launch_packet_session_dir.as_deref(),
        Some(
            resolved
                .snapshot
                .launch_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report launch_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        report.package_dir.as_deref(),
        Some(resolved.snapshot.package_dir.display().to_string().as_str()),
        "stored report package_dir",
        mismatches,
    );
    compare_option_string(
        report.install_root.as_deref(),
        Some(
            resolved
                .snapshot
                .install_root
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report install_root",
        mismatches,
    );
    compare_option_string(
        report.target_service_name.as_deref(),
        Some(resolved.snapshot.target_service_name.as_str()),
        "stored report target_service_name",
        mismatches,
    );
    compare_option_string(
        report.backend_command.as_deref(),
        Some(resolved.snapshot.backend_command.as_str()),
        "stored report backend_command",
        mismatches,
    );
    compare_option_u64(
        report.wrapper_timeout_ms,
        Some(resolved.snapshot.wrapper_timeout_ms),
        "stored report wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        report.service_status_max_staleness_ms,
        Some(resolved.snapshot.service_status_max_staleness_ms),
        "stored report service_status_max_staleness_ms",
        mismatches,
    );
    compare_option_string(
        report.resolve_latest_chain_command_summary.as_deref(),
        Some(
            resolve_latest_chain_command_summary(root, &paths.shadow_execute_latest_session_dir)
                .as_str(),
        ),
        "stored report resolve_latest_chain_command_summary",
        mismatches,
    );
    compare_option_string(
        report.plan_decision_packet_command_summary.as_deref(),
        Some(
            plan_decision_packet_command_summary(
                &resolved.snapshot,
                &paths.nested_decision_packet_session_dir,
            )
            .as_str(),
        ),
        "stored report plan_decision_packet_command_summary",
        mismatches,
    );
    compare_option_string(
        report.downstream_decision_packet_command_summary.as_deref(),
        Some(
            downstream_decision_packet_command_summary(
                &resolved.snapshot,
                &paths.nested_decision_packet_session_dir,
            )
            .as_str(),
        ),
        "stored report downstream_decision_packet_command_summary",
        mismatches,
    );
    compare_option_string(
        report.verify_decision_packet_command_summary.as_deref(),
        Some(
            verify_decision_packet_command_summary(
                &resolved.snapshot,
                &paths.nested_decision_packet_session_dir,
            )
            .as_str(),
        ),
        "stored report verify_decision_packet_command_summary",
        mismatches,
    );
    compare_option_string(
        report
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        Some(
            resolved
                .snapshot
                .reviewed_frozen_live_cutover_controller_command_summary
                .as_str(),
        ),
        "stored report reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_option_string(
        report.clerestory_certificate_summary.as_deref(),
        Some(resolved.snapshot.clerestory_certificate_summary.as_str()),
        "stored report clerestory_certificate_summary",
        mismatches,
    );
    compare_option_string(
        report.clerestory_certificate_sha256.as_deref(),
        Some(resolved.snapshot.clerestory_certificate_sha256.as_str()),
        "stored report clerestory_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        report.clerestory_certificate_algorithm.as_deref(),
        Some(resolved.snapshot.clerestory_certificate_algorithm.as_str()),
        "stored report clerestory_certificate_algorithm",
        mismatches,
    );
}

fn compare_session_against_failure(
    session: &PackageDecisionPacketLatestSession,
    root: &Path,
    session_dir: &Path,
    paths: &PackageDecisionPacketLatestPaths,
    failure: &LatestDecisionPacketResolutionFailure,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &session.root,
        &root.display().to_string(),
        "stored failure session root",
        mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "stored failure session session_dir",
        mismatches,
    );
    compare_string(
        &session.shadow_execute_latest_session_dir,
        &paths
            .shadow_execute_latest_session_dir
            .display()
            .to_string(),
        "stored failure session shadow_execute_latest_session_dir",
        mismatches,
    );
    compare_string(
        &session.nested_decision_packet_session_dir,
        &paths
            .nested_decision_packet_session_dir
            .display()
            .to_string(),
        "stored failure session nested_decision_packet_session_dir",
        mismatches,
    );
    if let Some(snapshot) = &failure.snapshot {
        compare_option_string(
            session.historical_execute_frozen_session_dir.as_deref(),
            Some(
                snapshot
                    .historical_execute_frozen_session_dir
                    .display()
                    .to_string()
                    .as_str(),
            ),
            "stored failure session historical_execute_frozen_session_dir",
            mismatches,
        );
    }
}

fn compare_report_against_failure(
    report: &PackageDecisionPacketLatestReport,
    root: &Path,
    session_dir: &Path,
    paths: &PackageDecisionPacketLatestPaths,
    failure: &LatestDecisionPacketResolutionFailure,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        report.root.as_deref(),
        Some(root.display().to_string().as_str()),
        "stored failure report root",
        mismatches,
    );
    compare_option_string(
        report.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "stored failure report session_dir",
        mismatches,
    );
    compare_option_string(
        report.latest_execute_verdict.as_deref(),
        failure.latest_execute_verdict.as_deref(),
        "stored failure report latest_execute_verdict",
        mismatches,
    );
    compare_option_string(
        report.decision_packet_plan_verdict.as_deref(),
        failure.decision_packet_plan_verdict.as_deref(),
        "stored failure report decision_packet_plan_verdict",
        mismatches,
    );
    compare_option_string(
        report.result.as_deref(),
        Some(serialize_enum(&result_from_failure_kind(failure.kind)).as_str()),
        "stored failure report result",
        mismatches,
    );
    compare_option_string(
        report.shadow_execute_latest_session_dir.as_deref(),
        Some(
            paths
                .shadow_execute_latest_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored failure report shadow_execute_latest_session_dir",
        mismatches,
    );
    compare_option_string(
        report.nested_decision_packet_session_dir.as_deref(),
        Some(
            paths
                .nested_decision_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored failure report nested_decision_packet_session_dir",
        mismatches,
    );
}

fn compare_execute_latest_copy_against_actual(
    stored: &ExecuteLatestReportView,
    actual: &ExecuteLatestReportView,
    shadow_execute_latest_session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        Some(stored.verdict.as_str()),
        Some(actual.verdict.as_str()),
        "stored latest_execute copy verdict",
        mismatches,
    );
    compare_option_string(
        Some(stored.reason.as_str()),
        Some(actual.reason.as_str()),
        "stored latest_execute copy reason",
        mismatches,
    );
    if stored.generated_at != actual.generated_at {
        mismatches.push(format!(
            "stored latest_execute copy generated_at {:?} does not match actual {:?}",
            stored.generated_at, actual.generated_at
        ));
    }
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(
            shadow_execute_latest_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored latest_execute copy session_dir",
        mismatches,
    );
    compare_option_string(
        stored.latest_top_step_name.as_deref(),
        actual.latest_top_step_name.as_deref(),
        "stored latest_execute copy latest_top_step_name",
        mismatches,
    );
    compare_option_string(
        stored.latest_top_session_dir.as_deref(),
        actual.latest_top_session_dir.as_deref(),
        "stored latest_execute copy latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.latest_chain_execute_frozen_session_dir.as_deref(),
        actual.latest_chain_execute_frozen_session_dir.as_deref(),
        "stored latest_execute copy latest_chain_execute_frozen_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.turn_green_session_dir.as_deref(),
        actual.turn_green_session_dir.as_deref(),
        "stored latest_execute copy turn_green_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.launch_packet_session_dir.as_deref(),
        actual.launch_packet_session_dir.as_deref(),
        "stored latest_execute copy launch_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.package_dir.as_deref(),
        actual.package_dir.as_deref(),
        "stored latest_execute copy package_dir",
        mismatches,
    );
    compare_option_string(
        stored.install_root.as_deref(),
        actual.install_root.as_deref(),
        "stored latest_execute copy install_root",
        mismatches,
    );
    compare_option_string(
        stored.target_service_name.as_deref(),
        actual.target_service_name.as_deref(),
        "stored latest_execute copy target_service_name",
        mismatches,
    );
    compare_option_string(
        stored.backend_command.as_deref(),
        actual.backend_command.as_deref(),
        "stored latest_execute copy backend_command",
        mismatches,
    );
    compare_option_u64(
        stored.wrapper_timeout_ms,
        actual.wrapper_timeout_ms,
        "stored latest_execute copy wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        stored.service_status_max_staleness_ms,
        actual.service_status_max_staleness_ms,
        "stored latest_execute copy service_status_max_staleness_ms",
        mismatches,
    );
    compare_option_string(
        stored
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        actual
            .reviewed_frozen_live_cutover_controller_command_summary
            .as_deref(),
        "stored latest_execute copy reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_summary.as_deref(),
        actual.clerestory_certificate_summary.as_deref(),
        "stored latest_execute copy clerestory_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_sha256.as_deref(),
        actual.clerestory_certificate_sha256.as_deref(),
        "stored latest_execute copy clerestory_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_algorithm.as_deref(),
        actual.clerestory_certificate_algorithm.as_deref(),
        "stored latest_execute copy clerestory_certificate_algorithm",
        mismatches,
    );
}

fn compare_decision_packet_plan_against_snapshot(
    view: &DecisionPacketReportView,
    snapshot: &LatestDecisionPacketSnapshot,
    nested_decision_packet_session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        Some(view.verdict.as_str()),
        Some(DECISION_PACKET_PLAN_READY),
        "decision-packet plan verdict",
        mismatches,
    );
    compare_string(
        &view.execute_frozen_session_dir,
        &snapshot
            .historical_execute_frozen_session_dir
            .display()
            .to_string(),
        "decision-packet plan execute_frozen_session_dir",
        mismatches,
    );
    compare_option_string(
        view.turn_green_session_dir.as_deref(),
        Some(
            snapshot
                .turn_green_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "decision-packet plan turn_green_session_dir",
        mismatches,
    );
    compare_option_string(
        view.launch_packet_session_dir.as_deref(),
        Some(
            snapshot
                .launch_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "decision-packet plan launch_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        view.package_dir.as_deref(),
        Some(snapshot.package_dir.display().to_string().as_str()),
        "decision-packet plan package_dir",
        mismatches,
    );
    compare_option_string(
        view.install_root.as_deref(),
        Some(snapshot.install_root.display().to_string().as_str()),
        "decision-packet plan install_root",
        mismatches,
    );
    compare_option_string(
        view.target_service_name.as_deref(),
        Some(snapshot.target_service_name.as_str()),
        "decision-packet plan target_service_name",
        mismatches,
    );
    compare_option_string(
        view.backend_command.as_deref(),
        Some(snapshot.backend_command.as_str()),
        "decision-packet plan backend_command",
        mismatches,
    );
    compare_option_u64(
        view.wrapper_timeout_ms,
        Some(snapshot.wrapper_timeout_ms),
        "decision-packet plan wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        view.service_status_max_staleness_ms,
        Some(snapshot.service_status_max_staleness_ms),
        "decision-packet plan service_status_max_staleness_ms",
        mismatches,
    );
    compare_option_string(
        view.session_dir.as_deref(),
        Some(
            nested_decision_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "decision-packet plan session_dir",
        mismatches,
    );
    compare_option_string(
        view.verify_execute_frozen_command_summary.as_deref(),
        Some(
            verify_execute_frozen_command_summary(&snapshot.historical_execute_frozen_session_dir)
                .as_str(),
        ),
        "decision-packet plan verify_execute_frozen_command_summary",
        mismatches,
    );
    compare_option_string(
        view.frozen_live_cutover_controller_command_summary
            .as_deref(),
        Some(
            snapshot
                .reviewed_frozen_live_cutover_controller_command_summary
                .as_str(),
        ),
        "decision-packet plan frozen_live_cutover_controller_command_summary",
        mismatches,
    );
}

fn compare_decision_packet_plan_copy_against_actual(
    stored: &DecisionPacketReportView,
    actual: &DecisionPacketReportView,
    nested_decision_packet_session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        Some(stored.verdict.as_str()),
        Some(actual.verdict.as_str()),
        "stored decision-packet plan verdict",
        mismatches,
    );
    compare_option_string(
        Some(stored.reason.as_str()),
        Some(actual.reason.as_str()),
        "stored decision-packet plan reason",
        mismatches,
    );
    if stored.generated_at != actual.generated_at {
        mismatches.push(format!(
            "stored decision-packet plan generated_at {:?} does not match actual {:?}",
            stored.generated_at, actual.generated_at
        ));
    }
    compare_string(
        &stored.execute_frozen_session_dir,
        &actual.execute_frozen_session_dir,
        "stored decision-packet plan execute_frozen_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.turn_green_session_dir.as_deref(),
        actual.turn_green_session_dir.as_deref(),
        "stored decision-packet plan turn_green_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.launch_packet_session_dir.as_deref(),
        actual.launch_packet_session_dir.as_deref(),
        "stored decision-packet plan launch_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.package_dir.as_deref(),
        actual.package_dir.as_deref(),
        "stored decision-packet plan package_dir",
        mismatches,
    );
    compare_option_string(
        stored.install_root.as_deref(),
        actual.install_root.as_deref(),
        "stored decision-packet plan install_root",
        mismatches,
    );
    compare_option_string(
        stored.target_service_name.as_deref(),
        actual.target_service_name.as_deref(),
        "stored decision-packet plan target_service_name",
        mismatches,
    );
    compare_option_string(
        stored.backend_command.as_deref(),
        actual.backend_command.as_deref(),
        "stored decision-packet plan backend_command",
        mismatches,
    );
    compare_option_u64(
        stored.wrapper_timeout_ms,
        actual.wrapper_timeout_ms,
        "stored decision-packet plan wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        stored.service_status_max_staleness_ms,
        actual.service_status_max_staleness_ms,
        "stored decision-packet plan service_status_max_staleness_ms",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(
            nested_decision_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored decision-packet plan session_dir",
        mismatches,
    );
    compare_option_string(
        stored.verify_execute_frozen_command_summary.as_deref(),
        actual.verify_execute_frozen_command_summary.as_deref(),
        "stored decision-packet plan verify_execute_frozen_command_summary",
        mismatches,
    );
    compare_option_string(
        stored
            .frozen_live_cutover_controller_command_summary
            .as_deref(),
        actual
            .frozen_live_cutover_controller_command_summary
            .as_deref(),
        "stored decision-packet plan frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_option_string(
        stored.operator_checklist_summary.as_deref(),
        actual.operator_checklist_summary.as_deref(),
        "stored decision-packet plan operator_checklist_summary",
        mismatches,
    );
    compare_option_string(
        stored.operator_runbook_summary.as_deref(),
        actual.operator_runbook_summary.as_deref(),
        "stored decision-packet plan operator_runbook_summary",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_verdict.as_deref(),
        actual.current_pre_activation_gate_verdict.as_deref(),
        "stored decision-packet plan current_pre_activation_gate_verdict",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_reason.as_deref(),
        actual.current_pre_activation_gate_reason.as_deref(),
        "stored decision-packet plan current_pre_activation_gate_reason",
        mismatches,
    );
    compare_string(
        &stored.explicit_statement,
        &actual.explicit_statement,
        "stored decision-packet plan explicit_statement",
        mismatches,
    );
}

fn compare_decision_packet_run_against_snapshot(
    stored: &DecisionPacketReportView,
    snapshot: &LatestDecisionPacketSnapshot,
    nested_decision_packet_session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &stored.mode,
        "run_live_package_decision_packet",
        "stored nested decision-packet run mode",
        mismatches,
    );
    compare_string(
        &stored.execute_frozen_session_dir,
        &snapshot
            .historical_execute_frozen_session_dir
            .display()
            .to_string(),
        "stored nested decision-packet run execute_frozen_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.turn_green_session_dir.as_deref(),
        Some(
            snapshot
                .turn_green_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored nested decision-packet run turn_green_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.launch_packet_session_dir.as_deref(),
        Some(
            snapshot
                .launch_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored nested decision-packet run launch_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.package_dir.as_deref(),
        Some(snapshot.package_dir.display().to_string().as_str()),
        "stored nested decision-packet run package_dir",
        mismatches,
    );
    compare_option_string(
        stored.install_root.as_deref(),
        Some(snapshot.install_root.display().to_string().as_str()),
        "stored nested decision-packet run install_root",
        mismatches,
    );
    compare_option_string(
        stored.target_service_name.as_deref(),
        Some(snapshot.target_service_name.as_str()),
        "stored nested decision-packet run target_service_name",
        mismatches,
    );
    compare_option_string(
        stored.backend_command.as_deref(),
        Some(snapshot.backend_command.as_str()),
        "stored nested decision-packet run backend_command",
        mismatches,
    );
    compare_option_u64(
        stored.wrapper_timeout_ms,
        Some(snapshot.wrapper_timeout_ms),
        "stored nested decision-packet run wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        stored.service_status_max_staleness_ms,
        Some(snapshot.service_status_max_staleness_ms),
        "stored nested decision-packet run service_status_max_staleness_ms",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(
            nested_decision_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored nested decision-packet run session_dir",
        mismatches,
    );
    compare_option_string(
        stored.decision_packet_session_path.as_deref(),
        Some(
            nested_decision_packet_session_path(nested_decision_packet_session_dir)
                .display()
                .to_string()
                .as_str(),
        ),
        "stored nested decision-packet run decision_packet_session_path",
        mismatches,
    );
    compare_option_string(
        stored.decision_packet_status_path.as_deref(),
        Some(
            nested_decision_packet_status_path(nested_decision_packet_session_dir)
                .display()
                .to_string()
                .as_str(),
        ),
        "stored nested decision-packet run decision_packet_status_path",
        mismatches,
    );
    compare_option_string(
        stored.verify_execute_frozen_command_summary.as_deref(),
        Some(
            verify_execute_frozen_command_summary(&snapshot.historical_execute_frozen_session_dir)
                .as_str(),
        ),
        "stored nested decision-packet run verify_execute_frozen_command_summary",
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
        "stored nested decision-packet run frozen_live_cutover_controller_command_summary",
        mismatches,
    );
}

fn compare_decision_packet_copy_against_nested_truth(
    stored: &DecisionPacketReportView,
    nested_session: &DecisionPacketSessionView,
    nested_status: &DecisionPacketStatusView,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &stored.execute_frozen_session_dir,
        &nested_status.execute_frozen_session_dir,
        "stored nested decision-packet execute_frozen_session_dir against nested status",
        mismatches,
    );
    compare_string(
        &stored.execute_frozen_session_dir,
        &nested_session.execute_frozen_session_dir,
        "stored nested decision-packet execute_frozen_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.turn_green_session_dir.as_deref(),
        Some(nested_session.turn_green_session_dir.as_str()),
        "stored nested decision-packet turn_green_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.launch_packet_session_dir.as_deref(),
        Some(nested_session.launch_packet_session_dir.as_str()),
        "stored nested decision-packet launch_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.package_dir.as_deref(),
        Some(nested_session.package_dir.as_str()),
        "stored nested decision-packet package_dir",
        mismatches,
    );
    compare_option_string(
        stored.install_root.as_deref(),
        Some(nested_session.install_root.as_str()),
        "stored nested decision-packet install_root",
        mismatches,
    );
    compare_option_string(
        stored.target_service_name.as_deref(),
        Some(nested_session.target_service_name.as_str()),
        "stored nested decision-packet target_service_name",
        mismatches,
    );
    compare_option_string(
        stored.backend_command.as_deref(),
        Some(nested_session.backend_command.as_str()),
        "stored nested decision-packet backend_command",
        mismatches,
    );
    compare_option_u64(
        stored.wrapper_timeout_ms,
        Some(nested_session.wrapper_timeout_ms),
        "stored nested decision-packet wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        stored.service_status_max_staleness_ms,
        Some(nested_session.service_status_max_staleness_ms),
        "stored nested decision-packet service_status_max_staleness_ms",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(nested_session.session_dir.as_str()),
        "stored nested decision-packet session_dir against nested session",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        Some(nested_status.session_dir.as_str()),
        "stored nested decision-packet session_dir",
        mismatches,
    );
    compare_option_string(
        stored.result.as_deref(),
        Some(nested_status.result.as_str()),
        "stored nested decision-packet result",
        mismatches,
    );
    compare_option_string(
        Some(stored.verdict.as_str()),
        decision_packet_run_verdict_for_result(&nested_status.result),
        "stored nested decision-packet verdict",
        mismatches,
    );
    compare_option_string(
        stored.execute_frozen_result.as_deref(),
        nested_status.execute_frozen_result.as_deref(),
        "stored nested decision-packet execute_frozen_result",
        mismatches,
    );
    compare_option_string(
        stored.verify_execute_frozen_command_summary.as_deref(),
        Some(
            nested_session
                .verify_execute_frozen_command_summary
                .as_str(),
        ),
        "stored nested decision-packet verify_execute_frozen_command_summary",
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
        "stored nested decision-packet frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_option_string(
        stored.operator_checklist_summary.as_deref(),
        Some(nested_session.operator_checklist_summary.as_str()),
        "stored nested decision-packet operator_checklist_summary against nested session",
        mismatches,
    );
    compare_option_string(
        stored.operator_checklist_summary.as_deref(),
        Some(nested_status.operator_checklist_summary.as_str()),
        "stored nested decision-packet operator_checklist_summary",
        mismatches,
    );
    compare_option_string(
        stored.operator_runbook_summary.as_deref(),
        Some(nested_session.operator_runbook_summary.as_str()),
        "stored nested decision-packet operator_runbook_summary against nested session",
        mismatches,
    );
    compare_option_string(
        stored.operator_runbook_summary.as_deref(),
        Some(nested_status.operator_runbook_summary.as_str()),
        "stored nested decision-packet operator_runbook_summary",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_verdict.as_deref(),
        nested_status.current_pre_activation_gate_verdict.as_deref(),
        "stored nested decision-packet current_pre_activation_gate_verdict",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_reason.as_deref(),
        nested_status.current_pre_activation_gate_reason.as_deref(),
        "stored nested decision-packet current_pre_activation_gate_reason",
        mismatches,
    );
    compare_string(
        &stored.reason,
        &nested_status.reason,
        "stored nested decision-packet reason",
        mismatches,
    );
    compare_string(
        &stored.explicit_statement,
        &nested_status.explicit_statement,
        "stored nested decision-packet explicit_statement",
        mismatches,
    );
}

fn fallback_decision_packet_session(
    snapshot: &LatestDecisionPacketSnapshot,
    session_dir: &Path,
) -> DecisionPacketSessionView {
    DecisionPacketSessionView {
        execute_frozen_session_dir: snapshot
            .historical_execute_frozen_session_dir
            .display()
            .to_string(),
        turn_green_session_dir: snapshot.turn_green_session_dir.display().to_string(),
        launch_packet_session_dir: snapshot.launch_packet_session_dir.display().to_string(),
        package_dir: snapshot.package_dir.display().to_string(),
        install_root: snapshot.install_root.display().to_string(),
        target_service_name: snapshot.target_service_name.clone(),
        backend_command: snapshot.backend_command.clone(),
        wrapper_timeout_ms: snapshot.wrapper_timeout_ms,
        service_status_max_staleness_ms: snapshot.service_status_max_staleness_ms,
        session_dir: session_dir.display().to_string(),
        verify_execute_frozen_command_summary: verify_execute_frozen_command_summary(
            &snapshot.historical_execute_frozen_session_dir,
        ),
        frozen_live_cutover_controller_command_summary: snapshot
            .reviewed_frozen_live_cutover_controller_command_summary
            .clone(),
        operator_checklist_summary: "<missing-nested-decision-packet-checklist>".to_string(),
        operator_runbook_summary: "<missing-nested-decision-packet-runbook>".to_string(),
        explicit_statement: "<missing-nested-decision-packet-session>".to_string(),
    }
}

fn fallback_decision_packet_status() -> DecisionPacketStatusView {
    DecisionPacketStatusView {
        session_dir: "<missing-nested-decision-packet-session>".to_string(),
        execute_frozen_session_dir: "<missing-nested-execute-frozen-session>".to_string(),
        result: "refused_now_by_invalid_or_drifted_contract".to_string(),
        reason: "<missing-nested-decision-packet-status>".to_string(),
        execute_frozen_result: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        operator_checklist_summary: "<missing-nested-decision-packet-checklist>".to_string(),
        operator_runbook_summary: "<missing-nested-decision-packet-runbook>".to_string(),
        explicit_statement: "<missing-nested-decision-packet-status>".to_string(),
    }
}

fn decision_packet_latest_paths(session_dir: &Path) -> PackageDecisionPacketLatestPaths {
    PackageDecisionPacketLatestPaths {
        session_path: session_dir
            .join("tiny_live_activation_package_decision_packet_latest.session.json"),
        status_path: session_dir
            .join("tiny_live_activation_package_decision_packet_latest.status.json"),
        report_path: session_dir
            .join("tiny_live_activation_package_decision_packet_latest.report.json"),
        latest_execute_report_path: session_dir
            .join("tiny_live_activation_package_decision_packet_latest.execute_latest.report.json"),
        decision_packet_plan_report_path: session_dir.join(
            "tiny_live_activation_package_decision_packet_latest.decision_packet_plan.report.json",
        ),
        decision_packet_report_path: session_dir.join(
            "tiny_live_activation_package_decision_packet_latest.decision_packet.report.json",
        ),
        shadow_execute_latest_session_dir: session_dir.join(
            "tiny_live_activation_package_decision_packet_latest.execute_latest_shadow.session",
        ),
        nested_decision_packet_session_dir: session_dir
            .join("tiny_live_activation_package_decision_packet_latest.decision_packet_session"),
    }
}

fn nested_decision_packet_session_path(session_dir: &Path) -> PathBuf {
    session_dir.join("tiny_live_activation_package_decision_packet.session.json")
}

fn nested_decision_packet_status_path(session_dir: &Path) -> PathBuf {
    session_dir.join("tiny_live_activation_package_decision_packet.status.json")
}

fn resolve_latest_chain_command_summary(root: &Path, shadow_session_dir: &Path) -> String {
    format!(
        "{EXECUTE_LATEST_BIN} --root {} --session-dir {} --plan-latest-execute",
        shell_escape_path(root),
        shell_escape_path(shadow_session_dir)
    )
}

fn plan_decision_packet_command_summary(
    snapshot: &LatestDecisionPacketSnapshot,
    session_dir: &Path,
) -> String {
    format!(
        "{DECISION_PACKET_BIN} --execute-frozen-session-dir {} --session-dir {} --plan-live-package-decision-packet",
        shell_escape_path(&snapshot.historical_execute_frozen_session_dir),
        shell_escape_path(session_dir)
    )
}

fn downstream_decision_packet_command_summary(
    snapshot: &LatestDecisionPacketSnapshot,
    session_dir: &Path,
) -> String {
    format!(
        "{DECISION_PACKET_BIN} --execute-frozen-session-dir {} --session-dir {} --run-live-package-decision-packet",
        shell_escape_path(&snapshot.historical_execute_frozen_session_dir),
        shell_escape_path(session_dir)
    )
}

fn verify_decision_packet_command_summary(
    snapshot: &LatestDecisionPacketSnapshot,
    session_dir: &Path,
) -> String {
    format!(
        "{DECISION_PACKET_BIN} --execute-frozen-session-dir {} --session-dir {} --verify-live-package-decision-packet",
        shell_escape_path(&snapshot.historical_execute_frozen_session_dir),
        shell_escape_path(session_dir)
    )
}

fn verify_execute_frozen_command_summary(execute_frozen_session_dir: &Path) -> String {
    format!(
        "verify immutable execute-frozen session under {} before freezing the final activation decision/checklist packet",
        execute_frozen_session_dir.display()
    )
}

fn execute_latest_plan_args(root: &Path, shadow_session_dir: &Path) -> Vec<String> {
    vec![
        "--root".to_string(),
        root.display().to_string(),
        "--session-dir".to_string(),
        shadow_session_dir.display().to_string(),
        "--plan-latest-execute".to_string(),
        "--json".to_string(),
    ]
}

fn decision_packet_args(
    execute_frozen_session_dir: &Path,
    session_dir: &Path,
    mode_flag: &str,
) -> Vec<String> {
    vec![
        "--execute-frozen-session-dir".to_string(),
        execute_frozen_session_dir.display().to_string(),
        "--session-dir".to_string(),
        session_dir.display().to_string(),
        mode_flag.to_string(),
        "--json".to_string(),
    ]
}

fn step_artifact(
    path: &Path,
    mode: &str,
    verdict: &str,
    reason: &str,
    generated_at: DateTime<Utc>,
) -> PackageDecisionPacketLatestStepArtifact {
    PackageDecisionPacketLatestStepArtifact {
        path: path.display().to_string(),
        mode: mode.to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        generated_at,
    }
}

fn load_required_step_json<T: DeserializeOwned>(
    step: &Option<PackageDecisionPacketLatestStepArtifact>,
    path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<T> {
    if step.is_none() {
        mismatches.push(format!(
            "{label} is missing from latest-decision-packet status"
        ));
    }
    match load_json(path) {
        Ok(value) => Ok(value),
        Err(error) => {
            mismatches.push(format!(
                "failed reading {} artifact {}: {error}",
                label,
                path.display()
            ));
            load_json(path)
        }
    }
}

fn compare_step_artifact_against_report_metadata(
    step: Option<&PackageDecisionPacketLatestStepArtifact>,
    path: &Path,
    expected_mode: &str,
    expected_verdict: &str,
    expected_reason: &str,
    expected_generated_at: DateTime<Utc>,
    label: &str,
    mismatches: &mut Vec<String>,
) {
    let Some(step) = step else {
        mismatches.push(format!(
            "{label} is missing from latest-decision-packet status"
        ));
        return;
    };
    compare_string(
        &step.path,
        &path.display().to_string(),
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

fn map_decision_packet_outcome(
    view: &DecisionPacketReportView,
) -> (
    LivePackageDecisionPacketLatestResult,
    TinyLivePackageDecisionPacketLatestVerdict,
) {
    let result = result_from_decision_packet_result(view.result.as_deref())
        .unwrap_or(LivePackageDecisionPacketLatestResult::RefusedByDriftedDecisionPacketContract);
    (result, verdict_for_result(result))
}

fn result_from_decision_packet_result(
    result: Option<&str>,
) -> Option<LivePackageDecisionPacketLatestResult> {
    match result {
        Some("refused_now_by_stage3") => {
            Some(LivePackageDecisionPacketLatestResult::RefusedNowByStage3)
        }
        Some("refused_now_by_pre_activation_gate") => {
            Some(LivePackageDecisionPacketLatestResult::RefusedNowByPreActivationGate)
        }
        Some("runnable_when_gate_truth_turns_green") => {
            Some(LivePackageDecisionPacketLatestResult::RunnableWhenGateTruthTurnsGreen)
        }
        Some("refused_now_by_invalid_or_drifted_contract") => {
            Some(LivePackageDecisionPacketLatestResult::RefusedByDriftedDecisionPacketContract)
        }
        _ => None,
    }
}

fn decision_packet_run_verdict_for_result(result: &str) -> Option<&'static str> {
    match result {
        "refused_now_by_stage3" => Some("tiny_live_package_decision_packet_refused_now_by_stage3"),
        "refused_now_by_pre_activation_gate" => {
            Some("tiny_live_package_decision_packet_refused_now_by_pre_activation_gate")
        }
        "runnable_when_gate_truth_turns_green" => {
            Some("tiny_live_package_decision_packet_runnable_when_gate_truth_turns_green")
        }
        "refused_now_by_invalid_or_drifted_contract" => {
            Some("tiny_live_package_decision_packet_refused_now_by_invalid_or_drifted_contract")
        }
        _ => None,
    }
}

fn result_from_failure_kind(kind: ResolutionFailureKind) -> LivePackageDecisionPacketLatestResult {
    match kind {
        ResolutionFailureKind::MissingLatestChain => {
            LivePackageDecisionPacketLatestResult::RefusedByMissingLatestChain
        }
        ResolutionFailureKind::InvalidLatestChain => {
            LivePackageDecisionPacketLatestResult::RefusedByInvalidLatestChain
        }
        ResolutionFailureKind::DriftedDecisionPacketContract => {
            LivePackageDecisionPacketLatestResult::RefusedByDriftedDecisionPacketContract
        }
    }
}

fn verdict_for_result(
    result: LivePackageDecisionPacketLatestResult,
) -> TinyLivePackageDecisionPacketLatestVerdict {
    match result {
        LivePackageDecisionPacketLatestResult::RefusedByMissingLatestChain => {
            TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestRefusedByMissingLatestChain
        }
        LivePackageDecisionPacketLatestResult::RefusedByInvalidLatestChain => {
            TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestRefusedByInvalidLatestChain
        }
        LivePackageDecisionPacketLatestResult::RefusedByDriftedDecisionPacketContract => {
            TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestRefusedByDriftedDecisionPacketContract
        }
        LivePackageDecisionPacketLatestResult::RefusedNowByStage3 => {
            TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestRefusedNowByStage3
        }
        LivePackageDecisionPacketLatestResult::RefusedNowByPreActivationGate => {
            TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestRefusedNowByPreActivationGate
        }
        LivePackageDecisionPacketLatestResult::RunnableWhenGateTruthTurnsGreen => {
            TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestRunnableWhenGateTruthTurnsGreen
        }
    }
}

fn verdict_from_failure_kind(
    kind: ResolutionFailureKind,
) -> TinyLivePackageDecisionPacketLatestVerdict {
    verdict_for_result(result_from_failure_kind(kind))
}

fn operator_next_action_summary(result: LivePackageDecisionPacketLatestResult) -> String {
    match result {
        LivePackageDecisionPacketLatestResult::RefusedByMissingLatestChain => {
            "no immutable clerestory chain is available yet; do not manually hunt execute/session lineage by hand".to_string()
        }
        LivePackageDecisionPacketLatestResult::RefusedByInvalidLatestChain => {
            "the latest immutable chain is incomplete or below the accepted clerestory layer; mint a fresh coherent chain instead of bypassing the execute_latest / decision-packet handoff".to_string()
        }
        LivePackageDecisionPacketLatestResult::RefusedByDriftedDecisionPacketContract => {
            "the latest immutable chain no longer resolves cleanly into the accepted decision-packet contract; repair execute-frozen or decision-packet drift instead of bypassing the bounded handoff".to_string()
        }
        LivePackageDecisionPacketLatestResult::RefusedNowByStage3 => {
            "the final decision packet remains an explicit Stage 3 refusal; rerun this latest-decision-packet handoff after current Stage 3 truth turns green".to_string()
        }
        LivePackageDecisionPacketLatestResult::RefusedNowByPreActivationGate => {
            "the final decision packet remains an explicit pre-activation refusal; rerun this latest-decision-packet handoff after the current pre-activation gate turns green".to_string()
        }
        LivePackageDecisionPacketLatestResult::RunnableWhenGateTruthTurnsGreen => {
            "the latest immutable chain now resolves into a final runnable_when_gate_truth_turns_green decision packet; any later live cutover remains an explicit operator choice under the existing frozen controller contract".to_string()
        }
    }
}

fn create_clean_session_dir(session_dir: &Path, label: &str) -> Result<()> {
    fs::create_dir_all(session_dir).with_context(|| {
        format!(
            "failed creating {} session dir {}",
            label,
            session_dir.display()
        )
    })
}

fn run_json_command(bin: &str, args: &[String]) -> Result<Value> {
    let output = Command::new(bin)
        .args(args)
        .output()
        .with_context(|| format!("failed running {bin}"))?;
    if !output.status.success() {
        bail!(
            "{} exited with status {}{}{}",
            bin,
            output.status,
            if output.stderr.is_empty() { "" } else { ": " },
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let stdout = String::from_utf8(output.stdout)
        .with_context(|| format!("{} produced non-utf8 output", bin))?;
    serde_json::from_str(stdout.trim())
        .with_context(|| format!("failed parsing json output from {}", bin))
}

fn parse_string_arg(label: &str, value: Option<String>) -> Result<String> {
    value.ok_or_else(|| anyhow!("missing value for {label}"))
}

fn set_mode(mode: &mut Option<Mode>, value: Mode, flag: &str) -> Result<()> {
    if mode.replace(value).is_some() {
        bail!("multiple mode flags provided, including {flag}");
    }
    Ok(())
}

fn required_option_string(value: Option<&String>, label: &str) -> Result<String> {
    value
        .filter(|entry| !entry.trim().is_empty())
        .cloned()
        .ok_or_else(|| anyhow!("missing {}", label))
}

fn extract_non_empty_string_from_option(value: Option<&str>) -> Option<&str> {
    value.and_then(|entry| {
        if entry.trim().is_empty() {
            None
        } else {
            Some(entry)
        }
    })
}

fn compare_string(actual: &str, expected: &str, label: &str, mismatches: &mut Vec<String>) {
    if actual != expected {
        mismatches.push(format!(
            "{} {} does not match expected {}",
            label, actual, expected
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
            "{} {:?} does not match expected {:?}",
            label, actual, expected
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
            "{} {:?} does not match expected {:?}",
            label, actual, expected
        ));
    }
}

fn persist_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed creating parent dir {}", parent.display()))?;
    }
    fs::write(path, serde_json::to_vec_pretty(value)?)
        .with_context(|| format!("failed writing {}", path.display()))
}

fn load_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let raw = fs::read(path).with_context(|| format!("failed reading {}", path.display()))?;
    serde_json::from_slice(&raw).with_context(|| format!("failed parsing {}", path.display()))
}

fn serialize_enum<T: Serialize>(value: &T) -> String {
    match serde_json::to_value(value) {
        Ok(Value::String(raw)) => raw,
        Ok(other) => other.to_string(),
        Err(_) => "<serialization-error>".to_string(),
    }
}

fn shell_escape_path(path: &Path) -> String {
    shell_escape(path.as_os_str())
}

fn shell_escape(value: &OsStr) -> String {
    let raw = value.to_string_lossy();
    if raw.is_empty() {
        "''".to_string()
    } else if raw
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '_' | '-' | '.' | ':'))
    {
        raw.to_string()
    } else {
        format!("'{}'", raw.replace('\'', "'\"'\"'"))
    }
}

fn write_script(path: &Path, script: &str) -> Result<()> {
    fs::write(path, script).with_context(|| format!("failed writing {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(path)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms)?;
    }
    Ok(())
}

fn render_report_lines(report: &PackageDecisionPacketLatestReport) -> String {
    let mut lines = Vec::new();
    lines.push(format!("verdict={}", serialize_enum(&report.verdict)));
    lines.push(format!("reason={}", report.reason));
    if let Some(value) = &report.latest_top_step_name {
        lines.push(format!("latest_top_step_name={value}"));
    }
    if let Some(value) = &report.latest_top_session_dir {
        lines.push(format!("latest_top_session_dir={value}"));
    }
    if let Some(value) = &report.historical_execute_frozen_session_dir {
        lines.push(format!("historical_execute_frozen_session_dir={value}"));
    }
    if let Some(value) = &report.shadow_execute_latest_session_dir {
        lines.push(format!("shadow_execute_latest_session_dir={value}"));
    }
    if let Some(value) = &report.nested_decision_packet_session_dir {
        lines.push(format!("nested_decision_packet_session_dir={value}"));
    }
    if let Some(value) = &report.result {
        lines.push(format!("result={value}"));
    }
    lines.push(format!(
        "activation_authorized={}",
        report.activation_authorized
    ));
    if let Some(value) = &report.downstream_decision_packet_command_summary {
        lines.push(format!(
            "downstream_decision_packet_command_summary={value}"
        ));
    }
    if !report.verification_mismatches.is_empty() {
        lines.push("verification_mismatches:".to_string());
        for entry in &report.verification_mismatches {
            lines.push(format!("- {entry}"));
        }
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{Mutex, MutexGuard, OnceLock};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    struct PathGuard {
        _lock: MutexGuard<'static, ()>,
        original_path: Option<std::ffi::OsString>,
    }

    impl PathGuard {
        fn install(bin_dir: &Path) -> Self {
            let lock = env_lock()
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let original_path = std::env::var_os("PATH");
            let mut new_path = std::ffi::OsString::from(bin_dir.as_os_str());
            if let Some(value) = &original_path {
                new_path.push(":");
                new_path.push(value);
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

    #[derive(Debug)]
    struct FakeDecisionPacketLatestFixture {
        _temp_dir: PathBuf,
        root: PathBuf,
        wrapper_session_dir: PathBuf,
        output_script_path: PathBuf,
        bin_dir: PathBuf,
        latest_top_session_dir: PathBuf,
        historical_execute_frozen_session_dir: PathBuf,
        turn_green_session_dir: PathBuf,
        launch_packet_session_dir: PathBuf,
        package_dir: PathBuf,
        install_root: PathBuf,
        backend_command: PathBuf,
        execute_latest_plan_json_path: PathBuf,
        decision_packet_plan_json_path: PathBuf,
        decision_packet_run_json_path: PathBuf,
        decision_packet_verify_json_path: PathBuf,
        decision_packet_session_json_path: PathBuf,
        decision_packet_status_json_path: PathBuf,
        frozen_controller_command: String,
    }

    impl FakeDecisionPacketLatestFixture {
        fn plan_config(&self) -> Config {
            Config {
                mode: Mode::PlanLatestDecisionPacket,
                root: Some(self.root.clone()),
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn render_config(&self) -> Config {
            Config {
                mode: Mode::RenderLatestDecisionPacketScript,
                root: Some(self.root.clone()),
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: Some(self.output_script_path.clone()),
                json: true,
            }
        }

        fn run_config(&self) -> Config {
            Config {
                mode: Mode::RunLatestDecisionPacket,
                root: Some(self.root.clone()),
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn verify_config(&self) -> Config {
            Config {
                mode: Mode::VerifyLatestDecisionPacket,
                root: None,
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }
    }

    fn temp_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "copybot-decision-packet-latest-{}-{}",
            label,
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn fake_decision_packet_latest_fixture(
        label: &str,
        decision_packet_result: &str,
    ) -> FakeDecisionPacketLatestFixture {
        let fixture_dir = temp_dir(label);
        let root = fixture_dir.join("root");
        let wrapper_session_dir = fixture_dir.join("decision-packet-latest-session");
        let output_script_path = fixture_dir.join("decision-packet-latest.sh");
        let bin_dir = fixture_dir.join("bin");
        let latest_top_session_dir = fixture_dir.join("latest-clerestory-session");
        let historical_execute_frozen_session_dir = fixture_dir.join("historical-execute-frozen");
        let turn_green_session_dir = fixture_dir.join("turn-green-session");
        let launch_packet_session_dir = fixture_dir.join("launch-packet-session");
        let package_dir = fixture_dir.join("package");
        let install_root = fixture_dir.join("install-root");
        let backend_command = fixture_dir.join("backend");
        let execute_latest_plan_json_path = fixture_dir.join("execute_latest.plan.json");
        let decision_packet_plan_json_path = fixture_dir.join("decision_packet.plan.json");
        let decision_packet_run_json_path = fixture_dir.join("decision_packet.run.json");
        let decision_packet_verify_json_path = fixture_dir.join("decision_packet.verify.json");
        let decision_packet_session_json_path = fixture_dir.join("decision_packet.session.json");
        let decision_packet_status_json_path = fixture_dir.join("decision_packet.status.json");
        let frozen_controller_command =
            "copybot_tiny_live_activation_package_live_cutover --package-dir /fake/pkg".to_string();

        for path in [
            &root,
            &bin_dir,
            &latest_top_session_dir,
            &historical_execute_frozen_session_dir,
            &turn_green_session_dir,
            &launch_packet_session_dir,
            &package_dir,
            &install_root,
        ] {
            fs::create_dir_all(path).unwrap();
        }
        fs::write(&backend_command, "#!/bin/sh\n").unwrap();

        let fixture = FakeDecisionPacketLatestFixture {
            _temp_dir: fixture_dir,
            root,
            wrapper_session_dir,
            output_script_path,
            bin_dir,
            latest_top_session_dir,
            historical_execute_frozen_session_dir,
            turn_green_session_dir,
            launch_packet_session_dir,
            package_dir,
            install_root,
            backend_command,
            execute_latest_plan_json_path,
            decision_packet_plan_json_path,
            decision_packet_run_json_path,
            decision_packet_verify_json_path,
            decision_packet_session_json_path,
            decision_packet_status_json_path,
            frozen_controller_command,
        };

        persist_json(
            &fixture.execute_latest_plan_json_path,
            &default_execute_latest_plan_report(&fixture),
        )
        .unwrap();
        persist_json(
            &fixture.decision_packet_plan_json_path,
            &default_decision_packet_plan_report(&fixture, decision_packet_result),
        )
        .unwrap();
        persist_json(
            &fixture.decision_packet_run_json_path,
            &default_decision_packet_run_report(&fixture, decision_packet_result),
        )
        .unwrap();
        persist_json(
            &fixture.decision_packet_verify_json_path,
            &default_decision_packet_verify_report(&fixture, decision_packet_result),
        )
        .unwrap();
        persist_json(
            &fixture.decision_packet_session_json_path,
            &default_decision_packet_session(&fixture, decision_packet_result),
        )
        .unwrap();
        persist_json(
            &fixture.decision_packet_status_json_path,
            &default_decision_packet_status(&fixture, decision_packet_result),
        )
        .unwrap();

        let paths = decision_packet_latest_paths(&fixture.wrapper_session_dir);
        write_fake_execute_latest_command(
            &fixture.bin_dir.join(EXECUTE_LATEST_BIN),
            &fixture.execute_latest_plan_json_path,
            &fixture.root,
            &paths.shadow_execute_latest_session_dir,
        );
        write_fake_decision_packet_command(
            &fixture.bin_dir.join(DECISION_PACKET_BIN),
            &fixture.decision_packet_plan_json_path,
            &fixture.decision_packet_run_json_path,
            &fixture.decision_packet_verify_json_path,
            &fixture.decision_packet_session_json_path,
            &fixture.decision_packet_status_json_path,
            &fixture.historical_execute_frozen_session_dir,
            &paths.nested_decision_packet_session_dir,
        );

        fixture
    }

    fn default_execute_latest_plan_report(fixture: &FakeDecisionPacketLatestFixture) -> Value {
        let paths = decision_packet_latest_paths(&fixture.wrapper_session_dir);
        let (gate_verdict, gate_reason) =
            match default_decision_packet_status(fixture, "runnable_when_gate_truth_turns_green")
                ["current_pre_activation_gate_verdict"]
                .as_str()
            {
                Some(value) => (
                    json!(value),
                    default_decision_packet_status(fixture, "runnable_when_gate_truth_turns_green")
                        ["current_pre_activation_gate_reason"]
                        .clone(),
                ),
                None => (Value::Null, Value::Null),
            };
        json!({
            "generated_at": Utc::now(),
            "verdict": EXECUTE_LATEST_PLAN_READY,
            "reason": "latest execute plan ready",
            "session_dir": paths.shadow_execute_latest_session_dir.display().to_string(),
            "latest_top_step_name": TOP_ACCEPTED_PACKAGE_STEP,
            "latest_top_session_dir": fixture.latest_top_session_dir.display().to_string(),
            "latest_chain_execute_frozen_session_dir": fixture.historical_execute_frozen_session_dir.display().to_string(),
            "turn_green_session_dir": fixture.turn_green_session_dir.display().to_string(),
            "launch_packet_session_dir": fixture.launch_packet_session_dir.display().to_string(),
            "package_dir": fixture.package_dir.display().to_string(),
            "install_root": fixture.install_root.display().to_string(),
            "target_service_name": "solana-copy-bot.service",
            "backend_command": fixture.backend_command.display().to_string(),
            "wrapper_timeout_ms": 1200,
            "service_status_max_staleness_ms": 4500,
            "reviewed_frozen_live_cutover_controller_command_summary": fixture.frozen_controller_command,
            "clerestory_certificate_summary": "latest clerestory summary",
            "clerestory_certificate_sha256": "latest-clerestory-sha",
            "clerestory_certificate_algorithm": "sha256",
            "current_pre_activation_gate_verdict": gate_verdict,
            "current_pre_activation_gate_reason": gate_reason
        })
    }

    fn default_decision_packet_session(
        fixture: &FakeDecisionPacketLatestFixture,
        result: &str,
    ) -> Value {
        let paths = decision_packet_latest_paths(&fixture.wrapper_session_dir);
        let (checklist, runbook) =
            decision_packet_checklist_and_runbook(result, &fixture.frozen_controller_command);
        json!({
            "session_version": "1",
            "planned_at": Utc::now(),
            "execute_frozen_session_dir": fixture.historical_execute_frozen_session_dir.display().to_string(),
            "turn_green_session_dir": fixture.turn_green_session_dir.display().to_string(),
            "launch_packet_session_dir": fixture.launch_packet_session_dir.display().to_string(),
            "package_dir": fixture.package_dir.display().to_string(),
            "install_root": fixture.install_root.display().to_string(),
            "target_service_name": "solana-copy-bot.service",
            "backend_command": fixture.backend_command.display().to_string(),
            "wrapper_timeout_ms": 1200,
            "service_status_max_staleness_ms": 4500,
            "session_dir": paths.nested_decision_packet_session_dir.display().to_string(),
            "verify_execute_frozen_command_summary": verify_execute_frozen_command_summary(&fixture.historical_execute_frozen_session_dir),
            "frozen_live_cutover_controller_command_summary": fixture.frozen_controller_command,
            "operator_checklist_summary": checklist,
            "operator_runbook_summary": runbook,
            "explicit_statement": "the verified execute-frozen session is the only handoff input here; this packet freezes one final go/no-go checklist over the exact frozen live cutover command"
        })
    }

    fn default_decision_packet_status(
        fixture: &FakeDecisionPacketLatestFixture,
        result: &str,
    ) -> Value {
        let paths = decision_packet_latest_paths(&fixture.wrapper_session_dir);
        let (reason, execute_frozen_result, gate_verdict, gate_reason) = match result {
            "refused_now_by_stage3" => (
                "the frozen controller remains refused now by Stage 3, so the final operator packet stays explicit refusal",
                json!("refused_now_by_stage3"),
                json!("blocked"),
                json!("stage3 blocked"),
            ),
            "refused_now_by_pre_activation_gate" => (
                "the frozen controller remains refused now by the pre-activation gate, so the final operator packet stays explicit refusal",
                json!("refused_now_by_pre_activation_gate"),
                json!("blocked"),
                json!("pre gate blocked"),
            ),
            _ => (
                "the verified frozen execution handoff is coherent and freezes the exact live cutover controller command; this packet is the final operator-facing checklist before any future manual go-live decision",
                json!("completed_keep_running"),
                json!("green"),
                json!("ok"),
            ),
        };
        let (checklist, runbook) =
            decision_packet_checklist_and_runbook(result, &fixture.frozen_controller_command);
        json!({
            "status_version": "1",
            "updated_at": Utc::now(),
            "session_dir": paths.nested_decision_packet_session_dir.display().to_string(),
            "execute_frozen_session_dir": fixture.historical_execute_frozen_session_dir.display().to_string(),
            "result": result,
            "reason": reason,
            "execute_frozen_result": execute_frozen_result,
            "current_pre_activation_gate_verdict": gate_verdict,
            "current_pre_activation_gate_reason": gate_reason,
            "operator_checklist_summary": checklist,
            "operator_runbook_summary": runbook,
            "explicit_statement": "this decision/checklist packet is read-only and archival; it never executes the frozen controller itself"
        })
    }

    fn default_decision_packet_plan_report(
        fixture: &FakeDecisionPacketLatestFixture,
        result: &str,
    ) -> Value {
        let paths = decision_packet_latest_paths(&fixture.wrapper_session_dir);
        let status = default_decision_packet_status(fixture, result);
        let session = default_decision_packet_session(fixture, result);
        json!({
            "generated_at": Utc::now(),
            "mode": "plan_live_package_decision_packet",
            "verdict": DECISION_PACKET_PLAN_READY,
            "reason": "decision packet plan ready",
            "execute_frozen_session_dir": fixture.historical_execute_frozen_session_dir.display().to_string(),
            "turn_green_session_dir": fixture.turn_green_session_dir.display().to_string(),
            "launch_packet_session_dir": fixture.launch_packet_session_dir.display().to_string(),
            "package_dir": fixture.package_dir.display().to_string(),
            "install_root": fixture.install_root.display().to_string(),
            "target_service_name": "solana-copy-bot.service",
            "backend_command": fixture.backend_command.display().to_string(),
            "wrapper_timeout_ms": 1200,
            "service_status_max_staleness_ms": 4500,
            "session_dir": paths.nested_decision_packet_session_dir.display().to_string(),
            "result": status["result"],
            "execute_frozen_result": status["execute_frozen_result"],
            "verify_execute_frozen_command_summary": session["verify_execute_frozen_command_summary"],
            "frozen_live_cutover_controller_command_summary": fixture.frozen_controller_command,
            "operator_checklist_summary": session["operator_checklist_summary"],
            "operator_runbook_summary": session["operator_runbook_summary"],
            "current_pre_activation_gate_verdict": status["current_pre_activation_gate_verdict"],
            "current_pre_activation_gate_reason": status["current_pre_activation_gate_reason"],
            "explicit_statement": "this final packet freezes verified execute-frozen truth into one operator-facing decision/checklist artifact without enabling production execution"
        })
    }

    fn default_decision_packet_run_report(
        fixture: &FakeDecisionPacketLatestFixture,
        result: &str,
    ) -> Value {
        let paths = decision_packet_latest_paths(&fixture.wrapper_session_dir);
        let status = default_decision_packet_status(fixture, result);
        json!({
            "generated_at": Utc::now(),
            "mode": "run_live_package_decision_packet",
            "verdict": decision_packet_run_verdict(result),
            "reason": status["reason"],
            "execute_frozen_session_dir": fixture.historical_execute_frozen_session_dir.display().to_string(),
            "turn_green_session_dir": fixture.turn_green_session_dir.display().to_string(),
            "launch_packet_session_dir": fixture.launch_packet_session_dir.display().to_string(),
            "package_dir": fixture.package_dir.display().to_string(),
            "install_root": fixture.install_root.display().to_string(),
            "target_service_name": "solana-copy-bot.service",
            "backend_command": fixture.backend_command.display().to_string(),
            "wrapper_timeout_ms": 1200,
            "service_status_max_staleness_ms": 4500,
            "session_dir": paths.nested_decision_packet_session_dir.display().to_string(),
            "decision_packet_session_path": nested_decision_packet_session_path(&paths.nested_decision_packet_session_dir).display().to_string(),
            "decision_packet_status_path": nested_decision_packet_status_path(&paths.nested_decision_packet_session_dir).display().to_string(),
            "result": result,
            "execute_frozen_result": status["execute_frozen_result"],
            "verify_execute_frozen_command_summary": verify_execute_frozen_command_summary(&fixture.historical_execute_frozen_session_dir),
            "frozen_live_cutover_controller_command_summary": fixture.frozen_controller_command,
            "operator_checklist_summary": status["operator_checklist_summary"],
            "operator_runbook_summary": status["operator_runbook_summary"],
            "current_pre_activation_gate_verdict": status["current_pre_activation_gate_verdict"],
            "current_pre_activation_gate_reason": status["current_pre_activation_gate_reason"],
            "explicit_statement": status["explicit_statement"]
        })
    }

    fn default_decision_packet_verify_report(
        fixture: &FakeDecisionPacketLatestFixture,
        result: &str,
    ) -> Value {
        let mut report = default_decision_packet_run_report(fixture, result);
        report["mode"] = json!("verify_live_package_decision_packet");
        report["verdict"] = json!(DECISION_PACKET_VERIFY_OK);
        report["reason"] = json!("verify ok");
        report["explicit_statement"] = json!("this verification binds one final operator-facing decision packet to verified execute-frozen truth and never substitutes a different frozen controller contract");
        report
    }

    fn decision_packet_run_verdict(result: &str) -> &'static str {
        match result {
            "refused_now_by_stage3" => "tiny_live_package_decision_packet_refused_now_by_stage3",
            "refused_now_by_pre_activation_gate" => {
                "tiny_live_package_decision_packet_refused_now_by_pre_activation_gate"
            }
            _ => "tiny_live_package_decision_packet_runnable_when_gate_truth_turns_green",
        }
    }

    fn decision_packet_checklist_and_runbook(
        result: &str,
        frozen_command: &str,
    ) -> (String, String) {
        match result {
            "refused_now_by_stage3" => (
                "Checklist: do not run the frozen controller; wait for Discovery V2/current operational truth to turn green; rerun turn_green, execute_frozen, and regenerate this final decision packet.".to_string(),
                "Runbook: remain read-only; monitor Discovery V2/current operational truth recovery; rerun the packet-native refresh surfaces only after production discovery truth turns green.".to_string(),
            ),
            "refused_now_by_pre_activation_gate" => (
                "Checklist: do not run the frozen controller; clear the current pre-activation gate blocker; rerun turn_green, execute_frozen, and regenerate this final decision packet.".to_string(),
                "Runbook: remain read-only; resolve the current pre-activation gate blocker; rerun turn_green, execute_frozen, and this final packet after the gate turns green.".to_string(),
            ),
            _ => (
                format!(
                    "Checklist: verify the intended prod host still matches the frozen contract; confirm Stage 3 and pre-activation truth are green at execution time; then use exactly this frozen controller command with no substitutions: {frozen_command}"
                ),
                format!(
                    "Runbook: 1. Reconfirm current gate truth immediately before go-live. 2. Re-open this packet and compare the frozen controller command. 3. If still accepted, execute exactly: {frozen_command}. 4. Stop immediately if any regenerated packet no longer yields runnable_when_gate_truth_turns_green."
                ),
            ),
        }
    }

    fn write_fake_execute_latest_command(
        path: &Path,
        plan_json_path: &Path,
        expected_root: &Path,
        expected_shadow_session_dir: &Path,
    ) {
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --root {} \"*) : ;;\n  *) echo 'unexpected root' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --session-dir {} \"*) : ;;\n  *) echo 'unexpected shadow execute_latest session dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --plan-latest-execute \"*) : ;;\n  *) echo 'unexpected execute_latest mode' >&2; exit 1;;\nesac\ncat '{}'\n",
            shell_escape_path(expected_root),
            shell_escape_path(expected_shadow_session_dir),
            plan_json_path.display(),
        );
        fs::write(path, script).unwrap();
        let mut perms = fs::metadata(path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).unwrap();
    }

    fn write_fake_decision_packet_command(
        path: &Path,
        plan_json_path: &Path,
        run_json_path: &Path,
        verify_json_path: &Path,
        session_json_path: &Path,
        status_json_path: &Path,
        expected_execute_frozen_session_dir: &Path,
        expected_session_dir: &Path,
    ) {
        let nested_session_path = nested_decision_packet_session_path(expected_session_dir);
        let nested_status_path = nested_decision_packet_status_path(expected_session_dir);
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --execute-frozen-session-dir {} \"*) : ;;\n  *) echo 'unexpected execute-frozen-session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --session-dir {} \"*) : ;;\n  *) echo 'unexpected decision-packet session-dir' >&2; exit 1;;\nesac\nmode=''\ncase \" $* \" in\n  *\" --plan-live-package-decision-packet \"*) mode='plan';;\n  *\" --run-live-package-decision-packet \"*) mode='run';;\n  *\" --verify-live-package-decision-packet \"*) mode='verify';;\n  *) echo 'unexpected decision-packet mode' >&2; exit 1;;\nesac\ncase \"$mode\" in\n  plan) cat '{}';;\n  run) mkdir -p '{}'; cp '{}' '{}'; cp '{}' '{}'; cat '{}';;\n  verify) cat '{}';;\n  *) exit 1;;\nesac\n",
            shell_escape_path(expected_execute_frozen_session_dir),
            shell_escape_path(expected_session_dir),
            plan_json_path.display(),
            expected_session_dir.display(),
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

    #[test]
    fn plan_mode_resolves_latest_valid_clerestory_chain_to_the_exact_expected_nested_latest_execute_and_execute_frozen_lineage(
    ) {
        let fixture = fake_decision_packet_latest_fixture(
            "decision_packet_latest_plan_valid",
            "runnable_when_gate_truth_turns_green",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let report = plan_latest_decision_packet_report(&fixture.plan_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestPlanReady
        );
        assert_eq!(
            report.latest_top_step_name.as_deref(),
            Some(TOP_ACCEPTED_PACKAGE_STEP)
        );
        assert_eq!(
            report.historical_execute_frozen_session_dir.as_deref(),
            Some(
                fixture
                    .historical_execute_frozen_session_dir
                    .display()
                    .to_string()
                    .as_str()
            )
        );
        assert_eq!(
            report.shadow_execute_latest_session_dir.as_deref(),
            Some(
                decision_packet_latest_paths(&fixture.wrapper_session_dir)
                    .shadow_execute_latest_session_dir
                    .display()
                    .to_string()
                    .as_str()
            )
        );
        assert!(report
            .downstream_decision_packet_command_summary
            .as_deref()
            .unwrap()
            .contains(
                fixture
                    .historical_execute_frozen_session_dir
                    .display()
                    .to_string()
                    .as_str()
            ));
    }

    #[test]
    fn render_mode_emits_the_exact_downstream_decision_packet_contract_not_a_second_controller_path(
    ) {
        let fixture = fake_decision_packet_latest_fixture(
            "decision_packet_latest_render",
            "runnable_when_gate_truth_turns_green",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let report = render_latest_decision_packet_script_report(&fixture.render_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestScriptRendered
        );
        let script = fs::read_to_string(&fixture.output_script_path).unwrap();
        assert!(script.contains(DECISION_PACKET_BIN));
        assert!(!script.contains(
            "copybot_tiny_live_activation_package_decision_packet_latest --run-latest-decision-packet"
        ));
    }

    #[test]
    fn run_mode_refuses_when_latest_chain_is_missing_invalid_or_below_clerestory() {
        {
            let fixture = fake_decision_packet_latest_fixture(
                "decision_packet_latest_missing",
                "runnable_when_gate_truth_turns_green",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            let mut plan: Value = load_json(&fixture.execute_latest_plan_json_path).unwrap();
            plan["verdict"] =
                json!("tiny_live_package_execute_latest_refused_by_missing_latest_chain");
            plan["reason"] = json!("missing latest chain");
            persist_json(&fixture.execute_latest_plan_json_path, &plan).unwrap();
            let report = run_latest_decision_packet_report(&fixture.run_config()).unwrap();
            assert_eq!(
                report.verdict,
                TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestRefusedByMissingLatestChain
            );
        }
        {
            let fixture = fake_decision_packet_latest_fixture(
                "decision_packet_latest_invalid",
                "runnable_when_gate_truth_turns_green",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            let mut plan: Value = load_json(&fixture.execute_latest_plan_json_path).unwrap();
            plan["verdict"] =
                json!("tiny_live_package_execute_latest_refused_by_invalid_latest_chain");
            plan["reason"] = json!("invalid latest chain");
            persist_json(&fixture.execute_latest_plan_json_path, &plan).unwrap();
            let report = run_latest_decision_packet_report(&fixture.run_config()).unwrap();
            assert_eq!(
                report.verdict,
                TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestRefusedByInvalidLatestChain
            );
        }
        {
            let fixture = fake_decision_packet_latest_fixture(
                "decision_packet_latest_below",
                "runnable_when_gate_truth_turns_green",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            let mut plan: Value = load_json(&fixture.execute_latest_plan_json_path).unwrap();
            plan["latest_top_step_name"] = json!("choir_receipt");
            persist_json(&fixture.execute_latest_plan_json_path, &plan).unwrap();
            let report = run_latest_decision_packet_report(&fixture.run_config()).unwrap();
            assert_eq!(
                report.verdict,
                TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestRefusedByInvalidLatestChain
            );
        }
    }

    #[test]
    fn run_mode_refuses_when_nested_execute_frozen_lineage_drifts_from_the_latest_immutable_chain()
    {
        let fixture = fake_decision_packet_latest_fixture(
            "decision_packet_latest_drift",
            "runnable_when_gate_truth_turns_green",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let mut plan: Value = load_json(&fixture.decision_packet_plan_json_path).unwrap();
        plan["package_dir"] = json!("/tampered/package");
        persist_json(&fixture.decision_packet_plan_json_path, &plan).unwrap();

        let report = run_latest_decision_packet_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestRefusedByDriftedDecisionPacketContract
        );
        assert!(report.reason.contains("decision-packet plan package_dir"));
    }

    #[test]
    fn verify_mode_fails_when_persisted_latest_decision_packet_artifacts_are_tampered_or_missing_nested_lineage_truth(
    ) {
        {
            let fixture = fake_decision_packet_latest_fixture(
                "decision_packet_latest_verify_tamper",
                "runnable_when_gate_truth_turns_green",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_latest_decision_packet_report(&fixture.run_config()).unwrap();

            let paths = decision_packet_latest_paths(&fixture.wrapper_session_dir);
            let mut report: Value = load_json(&paths.decision_packet_report_path).unwrap();
            report["reason"] = json!("tampered stored nested decision-packet reason");
            persist_json(&paths.decision_packet_report_path, &report).unwrap();

            let verify = verify_latest_decision_packet_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestVerifyInvalid
            );
            assert!(verify
                .verification_mismatches
                .iter()
                .any(|entry| entry.contains("stored nested decision-packet reason")));
        }

        {
            let fixture = fake_decision_packet_latest_fixture(
                "decision_packet_latest_verify_missing_nested_truth",
                "runnable_when_gate_truth_turns_green",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_latest_decision_packet_report(&fixture.run_config()).unwrap();

            let paths = decision_packet_latest_paths(&fixture.wrapper_session_dir);
            fs::remove_file(nested_decision_packet_status_path(
                &paths.nested_decision_packet_session_dir,
            ))
            .unwrap();

            let verify = verify_latest_decision_packet_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestVerifyInvalid
            );
            assert!(verify.verification_mismatches.iter().any(|entry| {
                entry.contains("failed loading nested decision-packet status")
                    || entry.contains("stored nested decision-packet")
            }));
        }
    }

    #[test]
    fn verify_mode_fails_when_wrapper_reason_or_gate_metadata_drift_from_nested_or_failure_truth() {
        {
            let fixture = fake_decision_packet_latest_fixture(
                "decision_packet_latest_wrapper_status_reason",
                "runnable_when_gate_truth_turns_green",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_latest_decision_packet_report(&fixture.run_config()).unwrap();

            let paths = decision_packet_latest_paths(&fixture.wrapper_session_dir);
            let mut status: Value = load_json(&paths.status_path).unwrap();
            status["reason"] = json!("tampered wrapper status reason");
            persist_json(&paths.status_path, &status).unwrap();

            let verify = verify_latest_decision_packet_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestVerifyInvalid
            );
            assert!(verify.verification_mismatches.iter().any(|entry| {
                entry.contains("stored wrapper status reason against nested decision-packet status")
            }));
        }

        {
            let fixture = fake_decision_packet_latest_fixture(
                "decision_packet_latest_wrapper_report_reason",
                "runnable_when_gate_truth_turns_green",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_latest_decision_packet_report(&fixture.run_config()).unwrap();

            let paths = decision_packet_latest_paths(&fixture.wrapper_session_dir);
            let mut status: Value = load_json(&paths.status_path).unwrap();
            let mut report: Value = load_json(&paths.report_path).unwrap();
            status["reason"] = json!("tampered wrapper reason");
            report["reason"] = json!("tampered wrapper reason");
            persist_json(&paths.status_path, &status).unwrap();
            persist_json(&paths.report_path, &report).unwrap();

            let verify = verify_latest_decision_packet_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestVerifyInvalid
            );
            assert!(verify.verification_mismatches.iter().any(|entry| {
                entry.contains("stored wrapper report reason against nested decision-packet status")
            }));
        }

        {
            let fixture = fake_decision_packet_latest_fixture(
                "decision_packet_latest_wrapper_status_gate_verdict",
                "runnable_when_gate_truth_turns_green",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_latest_decision_packet_report(&fixture.run_config()).unwrap();

            let paths = decision_packet_latest_paths(&fixture.wrapper_session_dir);
            let mut status: Value = load_json(&paths.status_path).unwrap();
            status["current_pre_activation_gate_verdict"] = json!("tampered");
            persist_json(&paths.status_path, &status).unwrap();

            let verify = verify_latest_decision_packet_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestVerifyInvalid
            );
            assert!(verify.verification_mismatches.iter().any(|entry| {
                entry.contains(
                    "stored wrapper status current_pre_activation_gate_verdict against nested decision-packet status",
                )
            }));
        }

        {
            let fixture = fake_decision_packet_latest_fixture(
                "decision_packet_latest_wrapper_status_gate_reason",
                "runnable_when_gate_truth_turns_green",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_latest_decision_packet_report(&fixture.run_config()).unwrap();

            let paths = decision_packet_latest_paths(&fixture.wrapper_session_dir);
            let mut status: Value = load_json(&paths.status_path).unwrap();
            status["current_pre_activation_gate_reason"] = json!("tampered");
            persist_json(&paths.status_path, &status).unwrap();

            let verify = verify_latest_decision_packet_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestVerifyInvalid
            );
            assert!(verify.verification_mismatches.iter().any(|entry| {
                entry.contains(
                    "stored wrapper status current_pre_activation_gate_reason against nested decision-packet status",
                )
            }));
        }

        {
            let fixture = fake_decision_packet_latest_fixture(
                "decision_packet_latest_wrapper_report_gate_verdict",
                "runnable_when_gate_truth_turns_green",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_latest_decision_packet_report(&fixture.run_config()).unwrap();

            let paths = decision_packet_latest_paths(&fixture.wrapper_session_dir);
            let mut report: Value = load_json(&paths.report_path).unwrap();
            report["current_pre_activation_gate_verdict"] = json!("tampered");
            persist_json(&paths.report_path, &report).unwrap();

            let verify = verify_latest_decision_packet_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestVerifyInvalid
            );
            assert!(verify.verification_mismatches.iter().any(|entry| {
                entry.contains(
                    "stored wrapper report current_pre_activation_gate_verdict against nested decision-packet status",
                )
            }));
        }

        {
            let fixture = fake_decision_packet_latest_fixture(
                "decision_packet_latest_wrapper_report_gate_reason",
                "runnable_when_gate_truth_turns_green",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);
            run_latest_decision_packet_report(&fixture.run_config()).unwrap();

            let paths = decision_packet_latest_paths(&fixture.wrapper_session_dir);
            let mut report: Value = load_json(&paths.report_path).unwrap();
            report["current_pre_activation_gate_reason"] = json!("tampered");
            persist_json(&paths.report_path, &report).unwrap();

            let verify = verify_latest_decision_packet_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestVerifyInvalid
            );
            assert!(verify.verification_mismatches.iter().any(|entry| {
                entry.contains(
                    "stored wrapper report current_pre_activation_gate_reason against nested decision-packet status",
                )
            }));
        }

        {
            let fixture = fake_decision_packet_latest_fixture(
                "decision_packet_latest_wrapper_failure_gate_verdict",
                "runnable_when_gate_truth_turns_green",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);

            let mut plan: Value = load_json(&fixture.decision_packet_plan_json_path).unwrap();
            plan["package_dir"] = json!("/tampered/package");
            persist_json(&fixture.decision_packet_plan_json_path, &plan).unwrap();

            run_latest_decision_packet_report(&fixture.run_config()).unwrap();

            let paths = decision_packet_latest_paths(&fixture.wrapper_session_dir);
            let mut report: Value = load_json(&paths.report_path).unwrap();
            report["current_pre_activation_gate_verdict"] = json!("tampered");
            persist_json(&paths.report_path, &report).unwrap();

            let verify = verify_latest_decision_packet_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestVerifyInvalid
            );
            assert!(verify.verification_mismatches.iter().any(|entry| {
                entry.contains("stored failure report current_pre_activation_gate_verdict")
            }));
        }
    }

    #[test]
    fn all_modes_preserve_the_existing_stage3_and_pre_activation_refusal_semantics_and_do_not_imply_activation_authorization(
    ) {
        {
            let fixture = fake_decision_packet_latest_fixture(
                "decision_packet_latest_stage3",
                "refused_now_by_stage3",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);

            let plan = plan_latest_decision_packet_report(&fixture.plan_config()).unwrap();
            assert_eq!(
                plan.verdict,
                TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestPlanReady
            );
            assert!(!plan.activation_authorized);

            let run = run_latest_decision_packet_report(&fixture.run_config()).unwrap();
            assert_eq!(
                run.verdict,
                TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestRefusedNowByStage3
            );
            assert_eq!(run.result.as_deref(), Some("refused_now_by_stage3"));
            assert!(!run.activation_authorized);

            let verify = verify_latest_decision_packet_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestVerifyOk,
                "{:#?}",
                verify.verification_mismatches
            );
            assert!(!verify.activation_authorized);
        }

        {
            let fixture = fake_decision_packet_latest_fixture(
                "decision_packet_latest_pre_gate",
                "refused_now_by_pre_activation_gate",
            );
            let _path_guard = PathGuard::install(&fixture.bin_dir);

            let run = run_latest_decision_packet_report(&fixture.run_config()).unwrap();
            assert_eq!(
                run.verdict,
                TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestRefusedNowByPreActivationGate
            );
            assert_eq!(
                run.result.as_deref(),
                Some("refused_now_by_pre_activation_gate")
            );
            assert!(!run.activation_authorized);

            let verify = verify_latest_decision_packet_report(&fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageDecisionPacketLatestVerdict::TinyLivePackageDecisionPacketLatestVerifyOk,
                "{:#?}",
                verify.verification_mismatches
            );
            assert!(!verify.activation_authorized);
        }
    }
}
