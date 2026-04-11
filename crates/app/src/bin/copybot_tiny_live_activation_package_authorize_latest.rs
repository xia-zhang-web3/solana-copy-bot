use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use copybot_app::tiny_live_activation::package_turn_green_execute_frozen;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const USAGE: &str = "usage: copybot_tiny_live_activation_package_authorize_latest --root <path> [--session-dir <path>] [--output <path>] [--json] (--plan-latest-authorization | --render-latest-authorization-script | --run-latest-authorization | --verify-latest-authorization)";
const AUTHORIZE_LATEST_SESSION_VERSION: &str = "1";
const AUTHORIZE_LATEST_STATUS_VERSION: &str = "1";
const EXECUTE_LATEST_BIN: &str = "copybot_tiny_live_activation_package_execute_latest";
const LIVE_AUTHORIZATION_BIN: &str = "copybot_tiny_live_activation_package_live_authorization";
const LAUNCH_PACKET_BIN: &str = "copybot_tiny_live_activation_package_launch_packet";
const LIVE_AUTHORIZATION_PLAN_READY: &str = "tiny_live_package_live_authorization_plan_ready";
const LIVE_AUTHORIZATION_VERIFY_OK: &str = "tiny_live_package_live_authorization_verify_ok";
const LAUNCH_PACKET_VERIFY_OK: &str = "tiny_live_package_launch_packet_verify_ok";
const EXECUTE_LATEST_PLAN_READY: &str = "tiny_live_package_execute_latest_plan_ready";
const PLANNING_SAFE_STATEMENT: &str =
    "this latest-authorization handoff binds the latest persisted immutable package chain to the accepted live authorization contract, but it never overrides Stage 3 or pre-activation refusal truth and plan/render do not authorize activation by themselves";

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
    PlanLatestAuthorization,
    RenderLatestAuthorizationScript,
    RunLatestAuthorization,
    VerifyLatestAuthorization,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TinyLivePackageAuthorizeLatestVerdict {
    TinyLivePackageAuthorizeLatestPlanReady,
    TinyLivePackageAuthorizeLatestScriptRendered,
    TinyLivePackageAuthorizeLatestRefusedByMissingLatestChain,
    TinyLivePackageAuthorizeLatestRefusedByInvalidLatestChain,
    TinyLivePackageAuthorizeLatestRefusedByDriftedAuthorizationContract,
    TinyLivePackageAuthorizeLatestRefusedNowByStage3,
    TinyLivePackageAuthorizeLatestRefusedNowByPreActivationGate,
    TinyLivePackageAuthorizeLatestAuthorizedNow,
    TinyLivePackageAuthorizeLatestVerifyOk,
    TinyLivePackageAuthorizeLatestVerifyInvalid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum LivePackageAuthorizeLatestResult {
    RefusedByMissingLatestChain,
    RefusedByInvalidLatestChain,
    RefusedByDriftedAuthorizationContract,
    RefusedNowByStage3,
    RefusedNowByPreActivationGate,
    AuthorizedNow,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageAuthorizeLatestStepArtifact {
    path: String,
    mode: String,
    verdict: String,
    reason: String,
    generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageAuthorizeLatestSession {
    session_version: String,
    planned_at: DateTime<Utc>,
    root: String,
    session_dir: String,
    shadow_execute_latest_session_dir: String,
    nested_live_authorization_session_dir: String,
    latest_top_step_name: Option<String>,
    latest_top_session_dir: Option<String>,
    choir_receipt_session_dir: Option<String>,
    decision_packet_session_dir: Option<String>,
    latest_chain_execute_frozen_session_dir: Option<String>,
    turn_green_session_dir: Option<String>,
    launch_packet_session_dir: Option<String>,
    historical_authorization_session_dir: Option<String>,
    package_dir: Option<String>,
    install_root: Option<String>,
    target_service_name: Option<String>,
    backend_command: Option<String>,
    wrapper_timeout_ms: Option<u64>,
    service_status_max_staleness_ms: Option<u64>,
    resolve_latest_chain_command_summary: String,
    verify_historical_launch_packet_command_summary: Option<String>,
    plan_live_authorization_command_summary: Option<String>,
    downstream_live_authorization_command_summary: Option<String>,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    clerestory_certificate_summary: Option<String>,
    clerestory_certificate_sha256: Option<String>,
    clerestory_certificate_algorithm: Option<String>,
    explicit_statement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageAuthorizeLatestStatus {
    status_version: String,
    updated_at: DateTime<Utc>,
    session_dir: String,
    root: String,
    latest_top_step_name: Option<String>,
    latest_top_session_dir: Option<String>,
    latest_chain_execute_frozen_session_dir: Option<String>,
    turn_green_session_dir: Option<String>,
    launch_packet_session_dir: Option<String>,
    historical_authorization_session_dir: Option<String>,
    nested_live_authorization_session_dir: String,
    result: LivePackageAuthorizeLatestResult,
    reason: String,
    latest_execute_verdict: Option<String>,
    historical_launch_packet_verdict: Option<String>,
    current_authorization_plan_verdict: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    latest_execute_step: Option<PackageAuthorizeLatestStepArtifact>,
    historical_launch_packet_step: Option<PackageAuthorizeLatestStepArtifact>,
    current_authorization_plan_step: Option<PackageAuthorizeLatestStepArtifact>,
    live_authorization_step: Option<PackageAuthorizeLatestStepArtifact>,
    operator_next_action_summary: String,
    activation_authorized: bool,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct PackageAuthorizeLatestPaths {
    session_path: PathBuf,
    status_path: PathBuf,
    report_path: PathBuf,
    latest_execute_report_path: PathBuf,
    historical_launch_packet_report_path: PathBuf,
    current_authorization_plan_report_path: PathBuf,
    live_authorization_report_path: PathBuf,
    shadow_execute_latest_session_dir: PathBuf,
    nested_live_authorization_session_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackageAuthorizeLatestReport {
    generated_at: DateTime<Utc>,
    mode: String,
    verdict: TinyLivePackageAuthorizeLatestVerdict,
    reason: String,
    root: Option<String>,
    latest_top_step_name: Option<String>,
    latest_top_session_dir: Option<String>,
    choir_receipt_session_dir: Option<String>,
    decision_packet_session_dir: Option<String>,
    latest_chain_execute_frozen_session_dir: Option<String>,
    turn_green_session_dir: Option<String>,
    launch_packet_session_dir: Option<String>,
    historical_authorization_session_dir: Option<String>,
    package_dir: Option<String>,
    install_root: Option<String>,
    target_service_name: Option<String>,
    backend_command: Option<String>,
    wrapper_timeout_ms: Option<u64>,
    service_status_max_staleness_ms: Option<u64>,
    session_dir: Option<String>,
    authorize_latest_session_path: Option<String>,
    authorize_latest_status_path: Option<String>,
    authorize_latest_report_path: Option<String>,
    latest_execute_report_path: Option<String>,
    historical_launch_packet_report_path: Option<String>,
    current_authorization_plan_report_path: Option<String>,
    nested_live_authorization_report_path: Option<String>,
    shadow_execute_latest_session_dir: Option<String>,
    nested_live_authorization_session_dir: Option<String>,
    latest_execute_verdict: Option<String>,
    historical_launch_packet_verdict: Option<String>,
    current_authorization_plan_verdict: Option<String>,
    result: Option<String>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    resolve_latest_chain_command_summary: Option<String>,
    verify_historical_launch_packet_command_summary: Option<String>,
    plan_live_authorization_command_summary: Option<String>,
    downstream_live_authorization_command_summary: Option<String>,
    reviewed_frozen_live_cutover_controller_command_summary: Option<String>,
    clerestory_certificate_summary: Option<String>,
    clerestory_certificate_sha256: Option<String>,
    clerestory_certificate_algorithm: Option<String>,
    verification_mismatches: Vec<String>,
    authorization_happened: bool,
    activation_authorized: bool,
    explicit_statement: String,
}

#[derive(Debug, Clone)]
struct LatestAuthorizationSnapshot {
    latest_top_step_name: String,
    latest_top_session_dir: PathBuf,
    choir_receipt_session_dir: PathBuf,
    decision_packet_session_dir: PathBuf,
    latest_chain_execute_frozen_session_dir: PathBuf,
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
struct ResolvedLatestAuthorization {
    snapshot: LatestAuthorizationSnapshot,
    latest_execute_report_json: Value,
    latest_execute_verdict: String,
    latest_execute_reason: String,
    latest_execute_generated_at: DateTime<Utc>,
    historical_launch_packet_report_json: Value,
    historical_launch_packet_verdict: String,
    historical_launch_packet_reason: String,
    historical_launch_packet_generated_at: DateTime<Utc>,
    current_authorization_plan_report_json: Value,
    current_authorization_plan_verdict: String,
    current_authorization_plan_reason: String,
    current_authorization_plan_generated_at: DateTime<Utc>,
    historical_authorization_session_dir: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResolutionFailureKind {
    MissingLatestChain,
    InvalidLatestChain,
    DriftedAuthorizationContract,
}

#[derive(Debug, Clone)]
struct LatestAuthorizationResolutionFailure {
    kind: ResolutionFailureKind,
    reason: String,
    snapshot: Option<LatestAuthorizationSnapshot>,
    latest_execute_report_json: Option<Value>,
    latest_execute_verdict: Option<String>,
    latest_execute_reason: Option<String>,
    latest_execute_generated_at: Option<DateTime<Utc>>,
    historical_launch_packet_report_json: Option<Value>,
    historical_launch_packet_verdict: Option<String>,
    historical_launch_packet_reason: Option<String>,
    historical_launch_packet_generated_at: Option<DateTime<Utc>>,
    current_authorization_plan_report_json: Option<Value>,
    current_authorization_plan_verdict: Option<String>,
    current_authorization_plan_reason: Option<String>,
    current_authorization_plan_generated_at: Option<DateTime<Utc>>,
    historical_authorization_session_dir: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
struct ExecuteLatestReportView {
    generated_at: DateTime<Utc>,
    verdict: String,
    reason: String,
    session_dir: Option<String>,
    latest_top_step_name: Option<String>,
    latest_top_session_dir: Option<String>,
    choir_receipt_session_dir: Option<String>,
    decision_packet_session_dir: Option<String>,
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

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
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
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    activation_authorized: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct LiveAuthorizationReportView {
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
    nested_preflight_session_dir: Option<String>,
    result: Option<String>,
    run_preflight_command_summary: String,
    gate_evaluation_command_summary: String,
    authorized_live_cutover_command_summary: String,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    activation_authorized: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct LiveAuthorizationStatusView {
    result: String,
    reason: String,
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
            "--plan-latest-authorization" => {
                set_mode(
                    &mut mode,
                    Mode::PlanLatestAuthorization,
                    "--plan-latest-authorization",
                )?;
            }
            "--render-latest-authorization-script" => {
                set_mode(
                    &mut mode,
                    Mode::RenderLatestAuthorizationScript,
                    "--render-latest-authorization-script",
                )?;
            }
            "--run-latest-authorization" => {
                set_mode(
                    &mut mode,
                    Mode::RunLatestAuthorization,
                    "--run-latest-authorization",
                )?;
            }
            "--verify-latest-authorization" => {
                set_mode(
                    &mut mode,
                    Mode::VerifyLatestAuthorization,
                    "--verify-latest-authorization",
                )?;
            }
            "--help" | "-h" => return Ok(None),
            other => bail!("unknown arg {other}; {USAGE}"),
        }
    }

    let Some(mode) = mode else {
        return Ok(None);
    };
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
        Mode::PlanLatestAuthorization => plan_latest_authorization_report(&config)?,
        Mode::RenderLatestAuthorizationScript => {
            render_latest_authorization_script_report(&config)?
        }
        Mode::RunLatestAuthorization => run_latest_authorization_report(&config)?,
        Mode::VerifyLatestAuthorization => verify_latest_authorization_report(&config)?,
    };
    if config.json {
        Ok(serde_json::to_string_pretty(&report)?)
    } else {
        Ok(render_report_lines(&report))
    }
}

fn plan_latest_authorization_report(config: &Config) -> Result<PackageAuthorizeLatestReport> {
    let root = config
        .root
        .as_ref()
        .ok_or_else(|| anyhow!("missing --root"))?;
    let suggested_session_dir = config
        .session_dir
        .clone()
        .unwrap_or_else(|| root.join("tiny_live_activation_package_authorize_latest.session"));
    let paths = authorize_latest_paths(&suggested_session_dir);
    match resolve_latest_authorization(root, &paths) {
        Ok(resolved) => Ok(plan_report_from_resolution(
            "plan_latest_authorization",
            TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestPlanReady,
            format!(
                "latest immutable {} chain under {} resolves to exact launch_packet {} / turn_green {} / historical execute_frozen {} lineage and the accepted live authorization contract remains current",
                resolved.snapshot.latest_top_step_name,
                root.display(),
                resolved.snapshot.launch_packet_session_dir.display(),
                resolved.snapshot.turn_green_session_dir.display(),
                resolved
                    .snapshot
                    .latest_chain_execute_frozen_session_dir
                    .display()
            ),
            root,
            &suggested_session_dir,
            &paths,
            &resolved,
            false,
        )),
        Err(failure) => Ok(report_from_resolution_failure(
            config,
            "plan_latest_authorization",
            root,
            Some(&suggested_session_dir),
            Some(&paths),
            &failure,
            false,
        )),
    }
}

fn render_latest_authorization_script_report(
    config: &Config,
) -> Result<PackageAuthorizeLatestReport> {
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
    let paths = authorize_latest_paths(&suggested_session_dir);

    match resolve_latest_authorization(root, &paths) {
        Ok(resolved) => {
            let script = format!(
                "#!/usr/bin/env bash\nset -euo pipefail\n\n{plan}\n{run}\n{verify}\n",
                plan = plan_live_authorization_command_summary(
                    &resolved.snapshot,
                    &paths.nested_live_authorization_session_dir,
                ),
                run = downstream_live_authorization_command_summary(
                    &resolved.snapshot,
                    &paths.nested_live_authorization_session_dir,
                ),
                verify = verify_live_authorization_command_summary(
                    &resolved.snapshot,
                    &paths.nested_live_authorization_session_dir,
                ),
            );
            write_script(output_path, &script)?;
            Ok(plan_report_from_resolution(
                "render_latest_authorization_script",
                TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestScriptRendered,
                format!(
                    "rendered latest-authorization handoff script to {} using the accepted live authorization contract",
                    output_path.display()
                ),
                root,
                &suggested_session_dir,
                &paths,
                &resolved,
                false,
            ))
        }
        Err(failure) => Ok(report_from_resolution_failure(
            config,
            "render_latest_authorization_script",
            root,
            Some(&suggested_session_dir),
            Some(&paths),
            &failure,
            false,
        )),
    }
}

fn run_latest_authorization_report(config: &Config) -> Result<PackageAuthorizeLatestReport> {
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
            "refusing to overwrite existing latest-authorization session dir {}",
            session_dir.display()
        );
    }
    let paths = authorize_latest_paths(session_dir);
    let resolution = resolve_latest_authorization(root, &paths);

    match resolution {
        Ok(resolved) => {
            package_turn_green_execute_frozen::validate_turn_green_session_dir(
                &resolved.snapshot.install_root,
                session_dir,
            )?;
            create_clean_session_dir(session_dir, "latest-authorization")?;
            let session = planned_session(root, session_dir, &paths, Some(&resolved), None);
            persist_json(&paths.session_path, &session)?;
            persist_json(
                &paths.latest_execute_report_path,
                &resolved.latest_execute_report_json,
            )?;
            persist_json(
                &paths.historical_launch_packet_report_path,
                &resolved.historical_launch_packet_report_json,
            )?;
            persist_json(
                &paths.current_authorization_plan_report_path,
                &resolved.current_authorization_plan_report_json,
            )?;

            let latest_execute_step = Some(step_artifact(
                &paths.latest_execute_report_path,
                "plan_latest_execute",
                &resolved.latest_execute_verdict,
                &resolved.latest_execute_reason,
                resolved.latest_execute_generated_at,
            ));
            let historical_launch_packet_step = Some(step_artifact(
                &paths.historical_launch_packet_report_path,
                "verify_live_package_launch_packet",
                &resolved.historical_launch_packet_verdict,
                &resolved.historical_launch_packet_reason,
                resolved.historical_launch_packet_generated_at,
            ));
            let current_authorization_plan_step = Some(step_artifact(
                &paths.current_authorization_plan_report_path,
                "plan_live_package_authorization",
                &resolved.current_authorization_plan_verdict,
                &resolved.current_authorization_plan_reason,
                resolved.current_authorization_plan_generated_at,
            ));

            let nested_run_raw = match run_json_command(
                LIVE_AUTHORIZATION_BIN,
                &live_authorization_args(
                    &resolved.snapshot,
                    &paths.nested_live_authorization_session_dir,
                    "--run-live-package-authorization",
                ),
            ) {
                Ok(value) => value,
                Err(error) => {
                    let status = refusal_status(
                        root,
                        session_dir,
                        &paths,
                        Some(&resolved),
                        LivePackageAuthorizeLatestResult::RefusedByDriftedAuthorizationContract,
                        format!(
                            "failed running downstream live authorization contract for {}: {error}",
                            paths.nested_live_authorization_session_dir.display()
                        ),
                        latest_execute_step,
                        historical_launch_packet_step,
                        current_authorization_plan_step,
                        None,
                        None,
                        None,
                        false,
                        operator_next_action_summary(
                            LivePackageAuthorizeLatestResult::RefusedByDriftedAuthorizationContract,
                        ),
                    );
                    persist_json(&paths.status_path, &status)?;
                    let report = report_from_status(
                        "run_latest_authorization",
                        verdict_for_result(status.result),
                        session_dir,
                        &paths,
                        &session,
                        &status,
                        Vec::new(),
                        false,
                    );
                    persist_json(&paths.report_path, &report)?;
                    return Ok(report);
                }
            };
            persist_json(&paths.live_authorization_report_path, &nested_run_raw)?;
            let nested_view: LiveAuthorizationReportView =
                serde_json::from_value(nested_run_raw.clone())
                    .context("failed parsing downstream live authorization run json")?;
            let nested_step = Some(step_artifact(
                &paths.live_authorization_report_path,
                "run_live_package_authorization",
                &nested_view.verdict,
                &nested_view.reason,
                nested_view.generated_at,
            ));
            let (result, verdict) = map_live_authorization_outcome(&nested_view);
            let activation_authorized = result == LivePackageAuthorizeLatestResult::AuthorizedNow
                && nested_view.activation_authorized;
            let status = PackageAuthorizeLatestStatus {
                status_version: AUTHORIZE_LATEST_STATUS_VERSION.to_string(),
                updated_at: Utc::now(),
                session_dir: session_dir.display().to_string(),
                root: root.display().to_string(),
                latest_top_step_name: Some(resolved.snapshot.latest_top_step_name.clone()),
                latest_top_session_dir: Some(
                    resolved
                        .snapshot
                        .latest_top_session_dir
                        .display()
                        .to_string(),
                ),
                latest_chain_execute_frozen_session_dir: Some(
                    resolved
                        .snapshot
                        .latest_chain_execute_frozen_session_dir
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
                historical_authorization_session_dir: Some(
                    resolved
                        .historical_authorization_session_dir
                        .display()
                        .to_string(),
                ),
                nested_live_authorization_session_dir: paths
                    .nested_live_authorization_session_dir
                    .display()
                    .to_string(),
                result,
                reason: nested_view.reason.clone(),
                latest_execute_verdict: Some(resolved.latest_execute_verdict.clone()),
                historical_launch_packet_verdict: Some(
                    resolved.historical_launch_packet_verdict.clone(),
                ),
                current_authorization_plan_verdict: Some(
                    resolved.current_authorization_plan_verdict.clone(),
                ),
                current_pre_activation_gate_verdict: nested_view
                    .current_pre_activation_gate_verdict
                    .clone(),
                current_pre_activation_gate_reason: nested_view
                    .current_pre_activation_gate_reason
                    .clone(),
                latest_execute_step,
                historical_launch_packet_step,
                current_authorization_plan_step,
                live_authorization_step: nested_step,
                operator_next_action_summary: operator_next_action_summary(result),
                activation_authorized,
                explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
            };
            persist_json(&paths.status_path, &status)?;
            let report = report_from_status(
                "run_latest_authorization",
                verdict,
                session_dir,
                &paths,
                &session,
                &status,
                Vec::new(),
                true,
            );
            persist_json(&paths.report_path, &report)?;
            Ok(report)
        }
        Err(failure) => {
            create_clean_session_dir(session_dir, "latest-authorization")?;
            let session = planned_session(root, session_dir, &paths, None, Some(&failure));
            persist_json(&paths.session_path, &session)?;
            if let Some(value) = &failure.latest_execute_report_json {
                persist_json(&paths.latest_execute_report_path, value)?;
            }
            if let Some(value) = &failure.historical_launch_packet_report_json {
                persist_json(&paths.historical_launch_packet_report_path, value)?;
            }
            if let Some(value) = &failure.current_authorization_plan_report_json {
                persist_json(&paths.current_authorization_plan_report_path, value)?;
            }
            let status =
                refusal_status_from_resolution_failure(root, session_dir, &paths, &failure);
            persist_json(&paths.status_path, &status)?;
            let report = report_from_status(
                "run_latest_authorization",
                verdict_from_failure_kind(failure.kind),
                session_dir,
                &paths,
                &session,
                &status,
                Vec::new(),
                false,
            );
            persist_json(&paths.report_path, &report)?;
            Ok(report)
        }
    }
}

fn verify_latest_authorization_report(config: &Config) -> Result<PackageAuthorizeLatestReport> {
    let session_dir = config
        .session_dir
        .as_ref()
        .ok_or_else(|| anyhow!("missing --session-dir for verify mode"))?;
    let paths = authorize_latest_paths(session_dir);
    let mut mismatches = Vec::new();

    let session: PackageAuthorizeLatestSession = match load_json(&paths.session_path) {
        Ok(value) => value,
        Err(error) => {
            return Ok(verify_invalid_report_without_session(
                session_dir,
                vec![format!(
                    "failed reading latest-authorization session artifact {}: {error}",
                    paths.session_path.display()
                )],
            ))
        }
    };
    let status: PackageAuthorizeLatestStatus = match load_json(&paths.status_path) {
        Ok(value) => value,
        Err(error) => {
            return Ok(verify_invalid_report_without_session(
                session_dir,
                vec![format!(
                    "failed reading latest-authorization status artifact {}: {error}",
                    paths.status_path.display()
                )],
            ))
        }
    };
    let stored_report: PackageAuthorizeLatestReport = match load_json(&paths.report_path) {
        Ok(value) => value,
        Err(error) => {
            return Ok(verify_invalid_report_without_session(
                session_dir,
                vec![format!(
                    "failed reading latest-authorization report artifact {}: {error}",
                    paths.report_path.display()
                )],
            ))
        }
    };

    compare_string(
        &session.root,
        &status.root,
        "latest-authorization root consistency",
        &mut mismatches,
    );
    compare_string(
        &session.session_dir,
        &session_dir.display().to_string(),
        "latest-authorization session session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.session_dir,
        &session_dir.display().to_string(),
        "latest-authorization status session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.session_dir.as_deref(),
        Some(session_dir.display().to_string().as_str()),
        "latest-authorization report session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.shadow_execute_latest_session_dir,
        &paths
            .shadow_execute_latest_session_dir
            .display()
            .to_string(),
        "latest-authorization shadow_execute_latest_session_dir",
        &mut mismatches,
    );
    compare_string(
        &session.nested_live_authorization_session_dir,
        &paths
            .nested_live_authorization_session_dir
            .display()
            .to_string(),
        "latest-authorization nested_live_authorization_session_dir",
        &mut mismatches,
    );
    compare_string(
        &status.nested_live_authorization_session_dir,
        &paths
            .nested_live_authorization_session_dir
            .display()
            .to_string(),
        "latest-authorization status nested_live_authorization_session_dir",
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
        "latest-authorization report shadow_execute_latest_session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report
            .nested_live_authorization_session_dir
            .as_deref(),
        Some(
            paths
                .nested_live_authorization_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "latest-authorization report nested_live_authorization_session_dir",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.authorize_latest_session_path.as_deref(),
        Some(paths.session_path.display().to_string().as_str()),
        "latest-authorization report session path",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.authorize_latest_status_path.as_deref(),
        Some(paths.status_path.display().to_string().as_str()),
        "latest-authorization report status path",
        &mut mismatches,
    );
    compare_option_string(
        stored_report.authorize_latest_report_path.as_deref(),
        Some(paths.report_path.display().to_string().as_str()),
        "latest-authorization report report path",
        &mut mismatches,
    );
    compare_string(
        &session.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "latest-authorization session explicit_statement",
        &mut mismatches,
    );
    compare_string(
        &status.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "latest-authorization status explicit_statement",
        &mut mismatches,
    );
    compare_string(
        &stored_report.explicit_statement,
        PLANNING_SAFE_STATEMENT,
        "latest-authorization report explicit_statement",
        &mut mismatches,
    );
    compare_string(
        &stored_report.mode,
        "run_latest_authorization",
        "latest-authorization stored report mode",
        &mut mismatches,
    );
    let expected_stored_report_verdict = serialize_enum(&verdict_for_result(status.result));
    compare_string(
        &serialize_enum(&stored_report.verdict),
        &expected_stored_report_verdict,
        "latest-authorization stored report verdict",
        &mut mismatches,
    );
    let expected_stored_report_result = serialize_enum(&status.result);
    compare_option_string(
        stored_report.result.as_deref(),
        Some(expected_stored_report_result.as_str()),
        "latest-authorization stored report result consistency",
        &mut mismatches,
    );
    compare_string(
        &status.operator_next_action_summary,
        &operator_next_action_summary(status.result),
        "latest-authorization stored status operator_next_action_summary baseline",
        &mut mismatches,
    );
    compare_string(
        &stored_report.reason,
        &status.reason,
        "latest-authorization stored report reason consistency",
        &mut mismatches,
    );

    let root = PathBuf::from(&session.root);
    let resolution = resolve_latest_authorization(&root, &paths);

    match resolution {
        Ok(resolved) => {
            compare_session_contract_against_resolution(
                &session,
                &root,
                &paths,
                &resolved,
                &mut mismatches,
            );
            compare_report_contract_against_resolution(
                &stored_report,
                &root,
                &paths,
                &resolved,
                &mut mismatches,
            );
            compare_option_string(
                status.latest_execute_verdict.as_deref(),
                Some(resolved.latest_execute_verdict.as_str()),
                "stored latest_execute_verdict",
                &mut mismatches,
            );
            compare_option_string(
                status.historical_launch_packet_verdict.as_deref(),
                Some(resolved.historical_launch_packet_verdict.as_str()),
                "stored historical_launch_packet_verdict",
                &mut mismatches,
            );
            compare_option_string(
                status.current_authorization_plan_verdict.as_deref(),
                Some(resolved.current_authorization_plan_verdict.as_str()),
                "stored current_authorization_plan_verdict",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.latest_execute_verdict.as_deref(),
                Some(resolved.latest_execute_verdict.as_str()),
                "stored report latest_execute_verdict",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.historical_launch_packet_verdict.as_deref(),
                Some(resolved.historical_launch_packet_verdict.as_str()),
                "stored report historical_launch_packet_verdict",
                &mut mismatches,
            );
            compare_option_string(
                stored_report.current_authorization_plan_verdict.as_deref(),
                Some(resolved.current_authorization_plan_verdict.as_str()),
                "stored report current_authorization_plan_verdict",
                &mut mismatches,
            );

            let stored_latest_execute: Option<ExecuteLatestReportView> =
                match load_required_step_json(
                    &status.latest_execute_step,
                    &paths.latest_execute_report_path,
                    "latest_execute_step",
                    &mut mismatches,
                ) {
                    Ok(value) => Some(value),
                    Err(error) => {
                        mismatches.push(error.to_string());
                        None
                    }
                };
            if let Some(stored_latest_execute) = stored_latest_execute.as_ref() {
                compare_execute_latest_copy_against_snapshot(
                    stored_latest_execute,
                    &resolved.snapshot,
                    &paths.shadow_execute_latest_session_dir,
                    &mut mismatches,
                );
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
            }

            let stored_launch_packet: Option<LaunchPacketReportView> = match load_required_step_json(
                &status.historical_launch_packet_step,
                &paths.historical_launch_packet_report_path,
                "historical_launch_packet_step",
                &mut mismatches,
            ) {
                Ok(value) => Some(value),
                Err(error) => {
                    mismatches.push(error.to_string());
                    None
                }
            };
            if let Some(stored_launch_packet) = stored_launch_packet.as_ref() {
                compare_launch_packet_copy_against_snapshot(
                    stored_launch_packet,
                    &resolved.snapshot,
                    &resolved.historical_authorization_session_dir,
                    &mut mismatches,
                );
                compare_step_artifact_against_report_metadata(
                    status.historical_launch_packet_step.as_ref(),
                    &paths.historical_launch_packet_report_path,
                    "verify_live_package_launch_packet",
                    &stored_launch_packet.verdict,
                    &stored_launch_packet.reason,
                    stored_launch_packet.generated_at,
                    "stored historical_launch_packet_step",
                    &mut mismatches,
                );
            }

            let stored_current_authorization_plan: Option<LiveAuthorizationReportView> =
                match load_required_step_json(
                    &status.current_authorization_plan_step,
                    &paths.current_authorization_plan_report_path,
                    "current_authorization_plan_step",
                    &mut mismatches,
                ) {
                    Ok(value) => Some(value),
                    Err(error) => {
                        mismatches.push(error.to_string());
                        None
                    }
                };
            if let Some(stored_current_authorization_plan) =
                stored_current_authorization_plan.as_ref()
            {
                mismatches.extend(compare_live_authorization_plan_against_snapshot(
                    stored_current_authorization_plan,
                    &resolved.snapshot,
                    &paths.nested_live_authorization_session_dir,
                ));
                compare_step_artifact_against_report_metadata(
                    status.current_authorization_plan_step.as_ref(),
                    &paths.current_authorization_plan_report_path,
                    "plan_live_package_authorization",
                    &stored_current_authorization_plan.verdict,
                    &stored_current_authorization_plan.reason,
                    stored_current_authorization_plan.generated_at,
                    "stored current_authorization_plan_step",
                    &mut mismatches,
                );
            }

            let expected_preflight_session_dir = live_authorization_preflight_session_dir(
                &paths.nested_live_authorization_session_dir,
            );

            let expects_downstream_run = matches!(
                status.result,
                LivePackageAuthorizeLatestResult::AuthorizedNow
                    | LivePackageAuthorizeLatestResult::RefusedNowByStage3
                    | LivePackageAuthorizeLatestResult::RefusedNowByPreActivationGate
            );

            if expects_downstream_run {
                let stored_live_authorization_run: Option<LiveAuthorizationReportView> =
                    match load_required_step_json(
                        &status.live_authorization_step,
                        &paths.live_authorization_report_path,
                        "live_authorization_step",
                        &mut mismatches,
                    ) {
                        Ok(value) => Some(value),
                        Err(error) => {
                            mismatches.push(error.to_string());
                            None
                        }
                    };
                let verified_raw = match run_json_command(
                    LIVE_AUTHORIZATION_BIN,
                    &live_authorization_args(
                        &resolved.snapshot,
                        &paths.nested_live_authorization_session_dir,
                        "--verify-live-package-authorization",
                    ),
                ) {
                    Ok(value) => Some(value),
                    Err(error) => {
                        mismatches.push(format!(
                            "failed verifying nested live authorization under {}: {error}",
                            paths.nested_live_authorization_session_dir.display()
                        ));
                        None
                    }
                };
                let verified_live_authorization: Option<LiveAuthorizationReportView> =
                    match verified_raw {
                        Some(raw) => match serde_json::from_value(raw) {
                            Ok(value) => Some(value),
                            Err(error) => {
                                mismatches.push(format!(
                                    "failed parsing downstream live authorization verify json: {error}"
                                ));
                                None
                            }
                        },
                        None => None,
                    };
                if let Some(verified_live_authorization) = verified_live_authorization.as_ref() {
                    if verified_live_authorization.verdict != LIVE_AUTHORIZATION_VERIFY_OK {
                        mismatches.push(format!(
                            "nested live authorization verification is non-green: {}",
                            verified_live_authorization.reason
                        ));
                    }
                    mismatches.extend(compare_live_authorization_plan_against_snapshot(
                        verified_live_authorization,
                        &resolved.snapshot,
                        &paths.nested_live_authorization_session_dir,
                    ));
                    if let Some(stored_live_authorization_run) =
                        stored_live_authorization_run.as_ref()
                    {
                        let verified_live_authorization_run = match load_json::<
                            LiveAuthorizationStatusView,
                        >(
                            &nested_live_authorization_status_path(
                                &paths.nested_live_authorization_session_dir,
                            ),
                        ) {
                            Ok(status) => Some(verified_live_authorization_run_view(
                                verified_live_authorization,
                                &status,
                            )),
                            Err(error) => {
                                mismatches.push(format!(
                                    "failed loading nested live authorization status under {}: {error}",
                                    paths.nested_live_authorization_session_dir.display()
                                ));
                                None
                            }
                        };
                        if let Some(verified_live_authorization_run) =
                            verified_live_authorization_run.as_ref()
                        {
                            compare_live_authorization_copy_against_verified(
                                stored_live_authorization_run,
                                verified_live_authorization_run,
                                &expected_preflight_session_dir,
                                &mut mismatches,
                            );
                        }
                        compare_step_artifact_against_report_metadata(
                            status.live_authorization_step.as_ref(),
                            &paths.live_authorization_report_path,
                            "run_live_package_authorization",
                            &stored_live_authorization_run.verdict,
                            &stored_live_authorization_run.reason,
                            stored_live_authorization_run.generated_at,
                            "stored live_authorization_step",
                            &mut mismatches,
                        );
                        compare_string(
                            &status.reason,
                            &stored_live_authorization_run.reason,
                            "stored status reason",
                            &mut mismatches,
                        );
                        compare_string(
                            &stored_report.reason,
                            &stored_live_authorization_run.reason,
                            "stored report reason",
                            &mut mismatches,
                        );
                    }
                    let expected_result = result_from_live_authorization_result(
                        verified_live_authorization.result.as_deref(),
                    );
                    if status.result != expected_result {
                        mismatches.push(format!(
                            "stored latest-authorization result {} does not match verified nested live authorization result {}",
                            serialize_enum(&status.result),
                            serialize_enum(&expected_result)
                        ));
                    }
                    compare_string(
                        &status.operator_next_action_summary,
                        &operator_next_action_summary(expected_result),
                        "stored status operator_next_action_summary",
                        &mut mismatches,
                    );
                    let expected_wrapper_result = serialize_enum(&expected_result);
                    compare_option_string(
                        stored_report.result.as_deref(),
                        Some(expected_wrapper_result.as_str()),
                        "stored report result",
                        &mut mismatches,
                    );
                    compare_option_string(
                        status.current_pre_activation_gate_verdict.as_deref(),
                        verified_live_authorization
                            .current_pre_activation_gate_verdict
                            .as_deref(),
                        "stored status current_pre_activation_gate_verdict",
                        &mut mismatches,
                    );
                    compare_option_string(
                        status.current_pre_activation_gate_reason.as_deref(),
                        verified_live_authorization
                            .current_pre_activation_gate_reason
                            .as_deref(),
                        "stored status current_pre_activation_gate_reason",
                        &mut mismatches,
                    );
                    compare_option_string(
                        stored_report.current_pre_activation_gate_verdict.as_deref(),
                        verified_live_authorization
                            .current_pre_activation_gate_verdict
                            .as_deref(),
                        "stored report current_pre_activation_gate_verdict",
                        &mut mismatches,
                    );
                    compare_option_string(
                        stored_report.current_pre_activation_gate_reason.as_deref(),
                        verified_live_authorization
                            .current_pre_activation_gate_reason
                            .as_deref(),
                        "stored report current_pre_activation_gate_reason",
                        &mut mismatches,
                    );
                    let expected_activation_authorized = expected_result
                        == LivePackageAuthorizeLatestResult::AuthorizedNow
                        && verified_live_authorization.activation_authorized;
                    if status.activation_authorized != expected_activation_authorized {
                        mismatches.push(format!(
                            "stored status activation_authorized {} does not match verified nested live authorization activation_authorized {}",
                            status.activation_authorized, expected_activation_authorized
                        ));
                    }
                    if stored_report.activation_authorized != expected_activation_authorized {
                        mismatches.push(format!(
                            "stored report activation_authorized {} does not match verified nested live authorization activation_authorized {}",
                            stored_report.activation_authorized, expected_activation_authorized
                        ));
                    }
                    if !stored_report.authorization_happened {
                        mismatches.push(
                            "stored report authorization_happened must be true when a nested live authorization step exists"
                                .to_string(),
                        );
                    }
                }
            } else {
                if status.live_authorization_step.is_some() {
                    mismatches.push(
                        "stored status must not carry live_authorization_step when latest-authorization refused before invoking the accepted authorization contract"
                            .to_string(),
                    );
                }
                if paths.live_authorization_report_path.exists() {
                    mismatches.push(format!(
                        "unexpected nested live authorization report artifact exists at {} for a non-run refusal result",
                        paths.live_authorization_report_path.display()
                    ));
                }
                if status.activation_authorized {
                    mismatches.push(
                        "stored status must never set activation_authorized=true when no nested live authorization run happened"
                            .to_string(),
                    );
                }
                if stored_report.activation_authorized {
                    mismatches.push(
                        "stored report must never set activation_authorized=true when no nested live authorization run happened"
                            .to_string(),
                    );
                }
                if stored_report.authorization_happened {
                    mismatches.push(
                        "stored report authorization_happened must be false when no nested live authorization run happened"
                            .to_string(),
                    );
                }
            }
        }
        Err(failure) => {
            compare_failure_session_contract(&session, &root, &paths, &failure, &mut mismatches);
            compare_failure_report_contract(
                &stored_report,
                &root,
                &paths,
                &failure,
                &mut mismatches,
            );
            if status.result != result_from_failure_kind(failure.kind) {
                mismatches.push(format!(
                    "stored latest-authorization result {} does not match current resolution failure {}",
                    serialize_enum(&status.result),
                    serialize_enum(&result_from_failure_kind(failure.kind))
                ));
            }
            compare_string(
                &status.reason,
                &failure.reason,
                "stored status reason on failure",
                &mut mismatches,
            );
            compare_string(
                &stored_report.reason,
                &failure.reason,
                "stored report reason on failure",
                &mut mismatches,
            );
            compare_string(
                &status.operator_next_action_summary,
                &operator_next_action_summary(result_from_failure_kind(failure.kind)),
                "stored status operator_next_action_summary on failure",
                &mut mismatches,
            );
            if status.live_authorization_step.is_some() {
                mismatches.push(
                    "stored status must not carry live_authorization_step when current latest-authorization resolution fails before downstream authorization run"
                        .to_string(),
                );
            }
            if paths.live_authorization_report_path.exists() {
                mismatches.push(format!(
                    "unexpected nested live authorization report artifact exists at {} while current latest-authorization resolution still fails",
                    paths.live_authorization_report_path.display()
                ));
            }
            if status.activation_authorized {
                mismatches.push(
                    "stored status must never set activation_authorized=true on a latest-authorization resolution failure"
                        .to_string(),
                );
            }
            if stored_report.activation_authorized {
                mismatches.push(
                    "stored report must never set activation_authorized=true on a latest-authorization resolution failure"
                        .to_string(),
                );
            }
            if stored_report.authorization_happened {
                mismatches.push(
                    "stored report authorization_happened must be false on a latest-authorization resolution failure"
                        .to_string(),
                );
            }
            if let Some(_) = &failure.latest_execute_report_json {
                let stored_latest_execute: Option<ExecuteLatestReportView> =
                    match load_required_step_json(
                        &status.latest_execute_step,
                        &paths.latest_execute_report_path,
                        "latest_execute_step",
                        &mut mismatches,
                    ) {
                        Ok(value) => Some(value),
                        Err(error) => {
                            mismatches.push(error.to_string());
                            None
                        }
                    };
                if let Some(stored_latest_execute) = stored_latest_execute.as_ref() {
                    let expected_shadow = &paths.shadow_execute_latest_session_dir;
                    compare_option_string(
                        Some(stored_latest_execute.verdict.as_str()),
                        failure.latest_execute_verdict.as_deref(),
                        "stored latest_execute verdict on failure",
                        &mut mismatches,
                    );
                    compare_option_string(
                        stored_latest_execute.session_dir.as_deref(),
                        Some(expected_shadow.display().to_string().as_str()),
                        "stored latest_execute shadow session_dir on failure",
                        &mut mismatches,
                    );
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
                }
            }
            if let Some(_) = &failure.historical_launch_packet_report_json {
                let stored_launch_packet: Option<LaunchPacketReportView> =
                    match load_required_step_json(
                        &status.historical_launch_packet_step,
                        &paths.historical_launch_packet_report_path,
                        "historical_launch_packet_step",
                        &mut mismatches,
                    ) {
                        Ok(value) => Some(value),
                        Err(error) => {
                            mismatches.push(error.to_string());
                            None
                        }
                    };
                if let Some(stored_launch_packet) = stored_launch_packet.as_ref() {
                    if let Some(snapshot) = &failure.snapshot {
                        if let Some(historical_authorization_session_dir) =
                            failure.historical_authorization_session_dir.as_ref()
                        {
                            compare_launch_packet_copy_against_snapshot(
                                stored_launch_packet,
                                snapshot,
                                historical_authorization_session_dir,
                                &mut mismatches,
                            );
                            compare_step_artifact_against_report_metadata(
                                status.historical_launch_packet_step.as_ref(),
                                &paths.historical_launch_packet_report_path,
                                "verify_live_package_launch_packet",
                                &stored_launch_packet.verdict,
                                &stored_launch_packet.reason,
                                stored_launch_packet.generated_at,
                                "stored historical_launch_packet_step on failure",
                                &mut mismatches,
                            );
                        }
                    }
                }
            }
            if let Some(_) = &failure.current_authorization_plan_report_json {
                let stored_current_authorization_plan: Option<LiveAuthorizationReportView> =
                    match load_required_step_json(
                        &status.current_authorization_plan_step,
                        &paths.current_authorization_plan_report_path,
                        "current_authorization_plan_step",
                        &mut mismatches,
                    ) {
                        Ok(value) => Some(value),
                        Err(error) => {
                            mismatches.push(error.to_string());
                            None
                        }
                    };
                if let Some(stored_current_authorization_plan) =
                    stored_current_authorization_plan.as_ref()
                {
                    if let Some(snapshot) = &failure.snapshot {
                        mismatches.extend(compare_live_authorization_plan_against_snapshot(
                            stored_current_authorization_plan,
                            snapshot,
                            &paths.nested_live_authorization_session_dir,
                        ));
                        compare_step_artifact_against_report_metadata(
                            status.current_authorization_plan_step.as_ref(),
                            &paths.current_authorization_plan_report_path,
                            "plan_live_package_authorization",
                            &stored_current_authorization_plan.verdict,
                            &stored_current_authorization_plan.reason,
                            stored_current_authorization_plan.generated_at,
                            "stored current_authorization_plan_step on failure",
                            &mut mismatches,
                        );
                    }
                }
            }
        }
    }

    let verdict = if mismatches.is_empty() {
        TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestVerifyOk
    } else {
        TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestVerifyInvalid
    };
    let reason = if mismatches.is_empty() {
        format!(
            "latest-authorization handoff session under {} is coherent and still bound to the current latest immutable package chain plus the accepted live authorization contract",
            session_dir.display()
        )
    } else {
        mismatches
            .first()
            .cloned()
            .unwrap_or_else(|| "latest-authorization handoff verification failed".to_string())
    };
    Ok(PackageAuthorizeLatestReport {
        generated_at: Utc::now(),
        mode: "verify_latest_authorization".to_string(),
        verdict,
        reason,
        root: Some(session.root.clone()),
        latest_top_step_name: session.latest_top_step_name.clone(),
        latest_top_session_dir: session.latest_top_session_dir.clone(),
        choir_receipt_session_dir: session.choir_receipt_session_dir.clone(),
        decision_packet_session_dir: session.decision_packet_session_dir.clone(),
        latest_chain_execute_frozen_session_dir: session.latest_chain_execute_frozen_session_dir.clone(),
        turn_green_session_dir: session.turn_green_session_dir.clone(),
        launch_packet_session_dir: session.launch_packet_session_dir.clone(),
        historical_authorization_session_dir: session.historical_authorization_session_dir.clone(),
        package_dir: session.package_dir.clone(),
        install_root: session.install_root.clone(),
        target_service_name: session.target_service_name.clone(),
        backend_command: session.backend_command.clone(),
        wrapper_timeout_ms: session.wrapper_timeout_ms,
        service_status_max_staleness_ms: session.service_status_max_staleness_ms,
        session_dir: Some(session_dir.display().to_string()),
        authorize_latest_session_path: Some(paths.session_path.display().to_string()),
        authorize_latest_status_path: Some(paths.status_path.display().to_string()),
        authorize_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_execute_report_path: Some(paths.latest_execute_report_path.display().to_string()),
        historical_launch_packet_report_path: Some(
            paths.historical_launch_packet_report_path.display().to_string(),
        ),
        current_authorization_plan_report_path: Some(
            paths.current_authorization_plan_report_path
                .display()
                .to_string(),
        ),
        nested_live_authorization_report_path: Some(
            paths.live_authorization_report_path.display().to_string(),
        ),
        shadow_execute_latest_session_dir: Some(
            paths.shadow_execute_latest_session_dir.display().to_string(),
        ),
        nested_live_authorization_session_dir: Some(
            paths.nested_live_authorization_session_dir
                .display()
                .to_string(),
        ),
        latest_execute_verdict: status.latest_execute_verdict.clone(),
        historical_launch_packet_verdict: status.historical_launch_packet_verdict.clone(),
        current_authorization_plan_verdict: status.current_authorization_plan_verdict.clone(),
        result: Some(serialize_enum(&status.result)),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        resolve_latest_chain_command_summary: Some(
            session.resolve_latest_chain_command_summary.clone(),
        ),
        verify_historical_launch_packet_command_summary: session
            .verify_historical_launch_packet_command_summary
            .clone(),
        plan_live_authorization_command_summary: session
            .plan_live_authorization_command_summary
            .clone(),
        downstream_live_authorization_command_summary: session
            .downstream_live_authorization_command_summary
            .clone(),
        reviewed_frozen_live_cutover_controller_command_summary: session
            .reviewed_frozen_live_cutover_controller_command_summary
            .clone(),
        clerestory_certificate_summary: session.clerestory_certificate_summary.clone(),
        clerestory_certificate_sha256: session.clerestory_certificate_sha256.clone(),
        clerestory_certificate_algorithm: session.clerestory_certificate_algorithm.clone(),
        verification_mismatches: mismatches,
        authorization_happened: stored_report.authorization_happened,
        activation_authorized: if verdict
            == TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestVerifyOk
        {
            status.activation_authorized
        } else {
            false
        },
        explicit_statement:
            "this latest-authorization handoff verification checks bounded immutable-package / launch-packet / authorization evidence only; it does not authorize activation by itself"
                .to_string(),
    })
}

fn resolve_latest_authorization(
    root: &Path,
    paths: &PackageAuthorizeLatestPaths,
) -> std::result::Result<ResolvedLatestAuthorization, LatestAuthorizationResolutionFailure> {
    let latest_execute_raw = match run_json_command(
        EXECUTE_LATEST_BIN,
        &[
            "--root".to_string(),
            root.display().to_string(),
            "--session-dir".to_string(),
            paths
                .shadow_execute_latest_session_dir
                .display()
                .to_string(),
            "--plan-latest-execute".to_string(),
            "--json".to_string(),
        ],
    ) {
        Ok(value) => value,
        Err(error) => {
            return Err(LatestAuthorizationResolutionFailure {
                kind: ResolutionFailureKind::InvalidLatestChain,
                reason: format!(
                    "failed resolving latest immutable chain under {} via accepted execute_latest handoff: {error}",
                    root.display()
                ),
                snapshot: None,
                latest_execute_report_json: None,
                latest_execute_verdict: None,
                latest_execute_reason: None,
                latest_execute_generated_at: None,
                historical_launch_packet_report_json: None,
                historical_launch_packet_verdict: None,
                historical_launch_packet_reason: None,
                historical_launch_packet_generated_at: None,
                current_authorization_plan_report_json: None,
                current_authorization_plan_verdict: None,
                current_authorization_plan_reason: None,
                current_authorization_plan_generated_at: None,
                historical_authorization_session_dir: None,
            });
        }
    };
    let latest_execute_view: ExecuteLatestReportView =
        match serde_json::from_value(latest_execute_raw.clone()) {
            Ok(value) => value,
            Err(error) => {
                return Err(LatestAuthorizationResolutionFailure {
                    kind: ResolutionFailureKind::InvalidLatestChain,
                    reason: format!(
                        "failed parsing execute_latest plan json for {}: {error}",
                        root.display()
                    ),
                    snapshot: None,
                    latest_execute_report_json: Some(latest_execute_raw),
                    latest_execute_verdict: None,
                    latest_execute_reason: None,
                    latest_execute_generated_at: None,
                    historical_launch_packet_report_json: None,
                    historical_launch_packet_verdict: None,
                    historical_launch_packet_reason: None,
                    historical_launch_packet_generated_at: None,
                    current_authorization_plan_report_json: None,
                    current_authorization_plan_verdict: None,
                    current_authorization_plan_reason: None,
                    current_authorization_plan_generated_at: None,
                    historical_authorization_session_dir: None,
                });
            }
        };
    let maybe_snapshot = maybe_snapshot_from_execute_latest(&latest_execute_view);
    if latest_execute_view.verdict != EXECUTE_LATEST_PLAN_READY {
        return Err(LatestAuthorizationResolutionFailure {
            kind: map_execute_latest_failure_kind(&latest_execute_view.verdict),
            reason: latest_execute_view.reason.clone(),
            snapshot: maybe_snapshot,
            latest_execute_report_json: Some(latest_execute_raw),
            latest_execute_verdict: Some(latest_execute_view.verdict),
            latest_execute_reason: Some(latest_execute_view.reason),
            latest_execute_generated_at: Some(latest_execute_view.generated_at),
            historical_launch_packet_report_json: None,
            historical_launch_packet_verdict: None,
            historical_launch_packet_reason: None,
            historical_launch_packet_generated_at: None,
            current_authorization_plan_report_json: None,
            current_authorization_plan_verdict: None,
            current_authorization_plan_reason: None,
            current_authorization_plan_generated_at: None,
            historical_authorization_session_dir: None,
        });
    }

    let snapshot = match parse_snapshot_from_execute_latest(&latest_execute_view) {
        Ok(value) => value,
        Err(error) => {
            return Err(LatestAuthorizationResolutionFailure {
                kind: ResolutionFailureKind::InvalidLatestChain,
                reason: error.to_string(),
                snapshot: maybe_snapshot_from_execute_latest(&latest_execute_view),
                latest_execute_report_json: Some(latest_execute_raw),
                latest_execute_verdict: Some(latest_execute_view.verdict),
                latest_execute_reason: Some(latest_execute_view.reason),
                latest_execute_generated_at: Some(latest_execute_view.generated_at),
                historical_launch_packet_report_json: None,
                historical_launch_packet_verdict: None,
                historical_launch_packet_reason: None,
                historical_launch_packet_generated_at: None,
                current_authorization_plan_report_json: None,
                current_authorization_plan_verdict: None,
                current_authorization_plan_reason: None,
                current_authorization_plan_generated_at: None,
                historical_authorization_session_dir: None,
            });
        }
    };

    let historical_launch_packet_raw = match run_json_command(
        LAUNCH_PACKET_BIN,
        &live_launch_packet_verify_args(&snapshot),
    ) {
        Ok(value) => value,
        Err(error) => {
            return Err(LatestAuthorizationResolutionFailure {
                kind: ResolutionFailureKind::DriftedAuthorizationContract,
                reason: format!(
                    "failed verifying historical launch_packet lineage under {}: {error}",
                    snapshot.launch_packet_session_dir.display()
                ),
                snapshot: Some(snapshot),
                latest_execute_report_json: Some(latest_execute_raw),
                latest_execute_verdict: Some(latest_execute_view.verdict),
                latest_execute_reason: Some(latest_execute_view.reason),
                latest_execute_generated_at: Some(latest_execute_view.generated_at),
                historical_launch_packet_report_json: None,
                historical_launch_packet_verdict: None,
                historical_launch_packet_reason: None,
                historical_launch_packet_generated_at: None,
                current_authorization_plan_report_json: None,
                current_authorization_plan_verdict: None,
                current_authorization_plan_reason: None,
                current_authorization_plan_generated_at: None,
                historical_authorization_session_dir: None,
            });
        }
    };
    let historical_launch_packet_view: LaunchPacketReportView =
        match serde_json::from_value(historical_launch_packet_raw.clone()) {
            Ok(value) => value,
            Err(error) => {
                return Err(LatestAuthorizationResolutionFailure {
                    kind: ResolutionFailureKind::DriftedAuthorizationContract,
                    reason: format!(
                        "failed parsing historical launch_packet verify json for {}: {error}",
                        snapshot.launch_packet_session_dir.display()
                    ),
                    snapshot: Some(snapshot),
                    latest_execute_report_json: Some(latest_execute_raw),
                    latest_execute_verdict: Some(latest_execute_view.verdict),
                    latest_execute_reason: Some(latest_execute_view.reason),
                    latest_execute_generated_at: Some(latest_execute_view.generated_at),
                    historical_launch_packet_report_json: Some(historical_launch_packet_raw),
                    historical_launch_packet_verdict: None,
                    historical_launch_packet_reason: None,
                    historical_launch_packet_generated_at: None,
                    current_authorization_plan_report_json: None,
                    current_authorization_plan_verdict: None,
                    current_authorization_plan_reason: None,
                    current_authorization_plan_generated_at: None,
                    historical_authorization_session_dir: None,
                });
            }
        };
    let historical_authorization_session_dir = match historical_launch_packet_view
        .nested_authorization_session_dir
        .as_ref()
    {
        Some(value) if !value.trim().is_empty() => PathBuf::from(value),
        _ => {
            return Err(LatestAuthorizationResolutionFailure {
                    kind: ResolutionFailureKind::DriftedAuthorizationContract,
                    reason: format!(
                        "historical launch_packet verification under {} did not return nested_authorization_session_dir",
                        snapshot.launch_packet_session_dir.display()
                    ),
                    snapshot: Some(snapshot),
                    latest_execute_report_json: Some(latest_execute_raw),
                    latest_execute_verdict: Some(latest_execute_view.verdict),
                    latest_execute_reason: Some(latest_execute_view.reason),
                    latest_execute_generated_at: Some(latest_execute_view.generated_at),
                    historical_launch_packet_report_json: Some(historical_launch_packet_raw),
                    historical_launch_packet_verdict: Some(historical_launch_packet_view.verdict),
                    historical_launch_packet_reason: Some(historical_launch_packet_view.reason),
                    historical_launch_packet_generated_at: Some(
                        historical_launch_packet_view.generated_at,
                    ),
                    current_authorization_plan_report_json: None,
                    current_authorization_plan_verdict: None,
                    current_authorization_plan_reason: None,
                    current_authorization_plan_generated_at: None,
                    historical_authorization_session_dir: None,
                });
        }
    };
    if historical_launch_packet_view.verdict != LAUNCH_PACKET_VERIFY_OK {
        return Err(LatestAuthorizationResolutionFailure {
            kind: ResolutionFailureKind::DriftedAuthorizationContract,
            reason: format!(
                "historical launch_packet lineage is non-green: {}",
                historical_launch_packet_view.reason
            ),
            snapshot: Some(snapshot.clone()),
            latest_execute_report_json: Some(latest_execute_raw),
            latest_execute_verdict: Some(latest_execute_view.verdict),
            latest_execute_reason: Some(latest_execute_view.reason),
            latest_execute_generated_at: Some(latest_execute_view.generated_at),
            historical_launch_packet_report_json: Some(historical_launch_packet_raw),
            historical_launch_packet_verdict: Some(historical_launch_packet_view.verdict),
            historical_launch_packet_reason: Some(historical_launch_packet_view.reason),
            historical_launch_packet_generated_at: Some(historical_launch_packet_view.generated_at),
            current_authorization_plan_report_json: None,
            current_authorization_plan_verdict: None,
            current_authorization_plan_reason: None,
            current_authorization_plan_generated_at: None,
            historical_authorization_session_dir: Some(historical_authorization_session_dir),
        });
    }
    let historical_launch_packet_mismatches = compare_launch_packet_against_snapshot(
        &historical_launch_packet_view,
        &snapshot,
        &historical_authorization_session_dir,
    );
    if !historical_launch_packet_mismatches.is_empty() {
        return Err(LatestAuthorizationResolutionFailure {
            kind: ResolutionFailureKind::DriftedAuthorizationContract,
            reason: historical_launch_packet_mismatches[0].clone(),
            snapshot: Some(snapshot.clone()),
            latest_execute_report_json: Some(latest_execute_raw),
            latest_execute_verdict: Some(latest_execute_view.verdict),
            latest_execute_reason: Some(latest_execute_view.reason),
            latest_execute_generated_at: Some(latest_execute_view.generated_at),
            historical_launch_packet_report_json: Some(historical_launch_packet_raw),
            historical_launch_packet_verdict: Some(historical_launch_packet_view.verdict),
            historical_launch_packet_reason: Some(historical_launch_packet_view.reason),
            historical_launch_packet_generated_at: Some(historical_launch_packet_view.generated_at),
            current_authorization_plan_report_json: None,
            current_authorization_plan_verdict: None,
            current_authorization_plan_reason: None,
            current_authorization_plan_generated_at: None,
            historical_authorization_session_dir: Some(historical_authorization_session_dir),
        });
    }

    let current_authorization_plan_raw = match run_json_command(
        LIVE_AUTHORIZATION_BIN,
        &live_authorization_args(
            &snapshot,
            &paths.nested_live_authorization_session_dir,
            "--plan-live-package-authorization",
        ),
    ) {
        Ok(value) => value,
        Err(error) => {
            return Err(LatestAuthorizationResolutionFailure {
                kind: ResolutionFailureKind::DriftedAuthorizationContract,
                reason: format!(
                    "failed planning accepted live authorization contract for latest immutable chain under {}: {error}",
                    root.display()
                ),
                snapshot: Some(snapshot),
                latest_execute_report_json: Some(latest_execute_raw),
                latest_execute_verdict: Some(latest_execute_view.verdict),
                latest_execute_reason: Some(latest_execute_view.reason),
                latest_execute_generated_at: Some(latest_execute_view.generated_at),
                historical_launch_packet_report_json: Some(historical_launch_packet_raw),
                historical_launch_packet_verdict: Some(historical_launch_packet_view.verdict),
                historical_launch_packet_reason: Some(historical_launch_packet_view.reason),
                historical_launch_packet_generated_at: Some(historical_launch_packet_view.generated_at),
                current_authorization_plan_report_json: None,
                current_authorization_plan_verdict: None,
                current_authorization_plan_reason: None,
                current_authorization_plan_generated_at: None,
                historical_authorization_session_dir: Some(historical_authorization_session_dir),
            });
        }
    };
    let current_authorization_plan_view: LiveAuthorizationReportView = match serde_json::from_value(
        current_authorization_plan_raw.clone(),
    ) {
        Ok(value) => value,
        Err(error) => {
            return Err(LatestAuthorizationResolutionFailure {
                    kind: ResolutionFailureKind::DriftedAuthorizationContract,
                    reason: format!(
                        "failed parsing accepted live authorization plan json for latest immutable chain under {}: {error}",
                        root.display()
                    ),
                    snapshot: Some(snapshot),
                    latest_execute_report_json: Some(latest_execute_raw),
                    latest_execute_verdict: Some(latest_execute_view.verdict),
                    latest_execute_reason: Some(latest_execute_view.reason),
                    latest_execute_generated_at: Some(latest_execute_view.generated_at),
                    historical_launch_packet_report_json: Some(historical_launch_packet_raw),
                    historical_launch_packet_verdict: Some(historical_launch_packet_view.verdict),
                    historical_launch_packet_reason: Some(historical_launch_packet_view.reason),
                    historical_launch_packet_generated_at: Some(historical_launch_packet_view.generated_at),
                    current_authorization_plan_report_json: Some(current_authorization_plan_raw),
                    current_authorization_plan_verdict: None,
                    current_authorization_plan_reason: None,
                    current_authorization_plan_generated_at: None,
                    historical_authorization_session_dir: Some(historical_authorization_session_dir),
                });
        }
    };
    if current_authorization_plan_view.verdict != LIVE_AUTHORIZATION_PLAN_READY {
        return Err(LatestAuthorizationResolutionFailure {
            kind: ResolutionFailureKind::DriftedAuthorizationContract,
            reason: format!(
                "accepted live authorization contract is non-green for the latest immutable chain: {}",
                current_authorization_plan_view.reason
            ),
            snapshot: Some(snapshot),
            latest_execute_report_json: Some(latest_execute_raw),
            latest_execute_verdict: Some(latest_execute_view.verdict),
            latest_execute_reason: Some(latest_execute_view.reason),
            latest_execute_generated_at: Some(latest_execute_view.generated_at),
            historical_launch_packet_report_json: Some(historical_launch_packet_raw),
            historical_launch_packet_verdict: Some(historical_launch_packet_view.verdict),
            historical_launch_packet_reason: Some(historical_launch_packet_view.reason),
            historical_launch_packet_generated_at: Some(historical_launch_packet_view.generated_at),
            current_authorization_plan_report_json: Some(current_authorization_plan_raw),
            current_authorization_plan_verdict: Some(current_authorization_plan_view.verdict),
            current_authorization_plan_reason: Some(current_authorization_plan_view.reason),
            current_authorization_plan_generated_at: Some(
                current_authorization_plan_view.generated_at,
            ),
            historical_authorization_session_dir: Some(historical_authorization_session_dir),
        });
    }
    let current_authorization_plan_mismatches = compare_live_authorization_plan_against_snapshot(
        &current_authorization_plan_view,
        &snapshot,
        &paths.nested_live_authorization_session_dir,
    );
    if !current_authorization_plan_mismatches.is_empty() {
        return Err(LatestAuthorizationResolutionFailure {
            kind: ResolutionFailureKind::DriftedAuthorizationContract,
            reason: current_authorization_plan_mismatches[0].clone(),
            snapshot: Some(snapshot),
            latest_execute_report_json: Some(latest_execute_raw),
            latest_execute_verdict: Some(latest_execute_view.verdict),
            latest_execute_reason: Some(latest_execute_view.reason),
            latest_execute_generated_at: Some(latest_execute_view.generated_at),
            historical_launch_packet_report_json: Some(historical_launch_packet_raw),
            historical_launch_packet_verdict: Some(historical_launch_packet_view.verdict),
            historical_launch_packet_reason: Some(historical_launch_packet_view.reason),
            historical_launch_packet_generated_at: Some(historical_launch_packet_view.generated_at),
            current_authorization_plan_report_json: Some(current_authorization_plan_raw),
            current_authorization_plan_verdict: Some(current_authorization_plan_view.verdict),
            current_authorization_plan_reason: Some(current_authorization_plan_view.reason),
            current_authorization_plan_generated_at: Some(
                current_authorization_plan_view.generated_at,
            ),
            historical_authorization_session_dir: Some(historical_authorization_session_dir),
        });
    }

    Ok(ResolvedLatestAuthorization {
        snapshot,
        latest_execute_report_json: latest_execute_raw,
        latest_execute_verdict: latest_execute_view.verdict,
        latest_execute_reason: latest_execute_view.reason,
        latest_execute_generated_at: latest_execute_view.generated_at,
        historical_launch_packet_report_json: historical_launch_packet_raw,
        historical_launch_packet_verdict: historical_launch_packet_view.verdict,
        historical_launch_packet_reason: historical_launch_packet_view.reason,
        historical_launch_packet_generated_at: historical_launch_packet_view.generated_at,
        current_authorization_plan_report_json: current_authorization_plan_raw,
        current_authorization_plan_verdict: current_authorization_plan_view.verdict,
        current_authorization_plan_reason: current_authorization_plan_view.reason,
        current_authorization_plan_generated_at: current_authorization_plan_view.generated_at,
        historical_authorization_session_dir,
    })
}

fn parse_snapshot_from_execute_latest(
    view: &ExecuteLatestReportView,
) -> Result<LatestAuthorizationSnapshot> {
    Ok(LatestAuthorizationSnapshot {
        latest_top_step_name: required_option_string(
            view.latest_top_step_name.as_ref(),
            "latest_top_step_name",
        )?,
        latest_top_session_dir: PathBuf::from(required_option_string(
            view.latest_top_session_dir.as_ref(),
            "latest_top_session_dir",
        )?),
        choir_receipt_session_dir: PathBuf::from(required_option_string(
            view.choir_receipt_session_dir.as_ref(),
            "choir_receipt_session_dir",
        )?),
        decision_packet_session_dir: PathBuf::from(required_option_string(
            view.decision_packet_session_dir.as_ref(),
            "decision_packet_session_dir",
        )?),
        latest_chain_execute_frozen_session_dir: PathBuf::from(required_option_string(
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
        current_pre_activation_gate_verdict: view.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: view.current_pre_activation_gate_reason.clone(),
    })
}

fn maybe_snapshot_from_execute_latest(
    view: &ExecuteLatestReportView,
) -> Option<LatestAuthorizationSnapshot> {
    parse_snapshot_from_execute_latest(view).ok()
}

fn compare_launch_packet_against_snapshot(
    view: &LaunchPacketReportView,
    snapshot: &LatestAuthorizationSnapshot,
    historical_authorization_session_dir: &Path,
) -> Vec<String> {
    let mut mismatches = Vec::new();
    compare_option_string(
        view.session_dir.as_deref(),
        Some(
            snapshot
                .launch_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "historical launch_packet verify session_dir",
        &mut mismatches,
    );
    compare_option_string(
        view.launch_packet_session_path.as_deref(),
        Some(
            snapshot
                .launch_packet_session_dir
                .join("tiny_live_activation_package_launch_packet.session.json")
                .display()
                .to_string()
                .as_str(),
        ),
        "historical launch_packet verify launch_packet_session_path",
        &mut mismatches,
    );
    compare_option_string(
        view.launch_packet_status_path.as_deref(),
        Some(
            snapshot
                .launch_packet_session_dir
                .join("tiny_live_activation_package_launch_packet.status.json")
                .display()
                .to_string()
                .as_str(),
        ),
        "historical launch_packet verify launch_packet_status_path",
        &mut mismatches,
    );
    compare_option_string(
        view.nested_authorization_session_dir.as_deref(),
        Some(
            historical_authorization_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "historical launch_packet verify nested_authorization_session_dir",
        &mut mismatches,
    );
    compare_string(
        &view.package_dir,
        &snapshot.package_dir.display().to_string(),
        "historical launch_packet verify package_dir",
        &mut mismatches,
    );
    compare_string(
        &view.install_root,
        &snapshot.install_root.display().to_string(),
        "historical launch_packet verify install_root",
        &mut mismatches,
    );
    compare_string(
        &view.target_service_name,
        &snapshot.target_service_name,
        "historical launch_packet verify target_service_name",
        &mut mismatches,
    );
    compare_string(
        &view.backend_command,
        &snapshot.backend_command,
        "historical launch_packet verify backend_command",
        &mut mismatches,
    );
    if view.wrapper_timeout_ms != snapshot.wrapper_timeout_ms {
        mismatches.push(format!(
            "historical launch_packet verify wrapper_timeout_ms {} does not match expected {}",
            view.wrapper_timeout_ms, snapshot.wrapper_timeout_ms
        ));
    }
    if view.service_status_max_staleness_ms != snapshot.service_status_max_staleness_ms {
        mismatches.push(format!(
            "historical launch_packet verify service_status_max_staleness_ms {} does not match expected {}",
            view.service_status_max_staleness_ms, snapshot.service_status_max_staleness_ms
        ));
    }
    compare_string(
        &view.run_live_authorization_command_summary,
        &historical_live_authorization_command_summary(
            snapshot,
            historical_authorization_session_dir,
        ),
        "historical launch_packet verify run_live_authorization_command_summary",
        &mut mismatches,
    );
    compare_string(
        &view.frozen_live_cutover_controller_command_summary,
        &snapshot.reviewed_frozen_live_cutover_controller_command_summary,
        "historical launch_packet verify frozen_live_cutover_controller_command_summary",
        &mut mismatches,
    );
    mismatches
}

fn compare_live_authorization_plan_against_snapshot(
    view: &LiveAuthorizationReportView,
    snapshot: &LatestAuthorizationSnapshot,
    nested_live_authorization_session_dir: &Path,
) -> Vec<String> {
    let mut mismatches = Vec::new();
    compare_string(
        &view.package_dir,
        &snapshot.package_dir.display().to_string(),
        "current live authorization package_dir",
        &mut mismatches,
    );
    compare_string(
        &view.install_root,
        &snapshot.install_root.display().to_string(),
        "current live authorization install_root",
        &mut mismatches,
    );
    compare_string(
        &view.target_service_name,
        &snapshot.target_service_name,
        "current live authorization target_service_name",
        &mut mismatches,
    );
    compare_string(
        &view.backend_command,
        &snapshot.backend_command,
        "current live authorization backend_command",
        &mut mismatches,
    );
    if view.wrapper_timeout_ms != snapshot.wrapper_timeout_ms {
        mismatches.push(format!(
            "current live authorization wrapper_timeout_ms {} does not match expected {}",
            view.wrapper_timeout_ms, snapshot.wrapper_timeout_ms
        ));
    }
    if view.service_status_max_staleness_ms != snapshot.service_status_max_staleness_ms {
        mismatches.push(format!(
            "current live authorization service_status_max_staleness_ms {} does not match expected {}",
            view.service_status_max_staleness_ms, snapshot.service_status_max_staleness_ms
        ));
    }
    compare_option_string(
        view.session_dir.as_deref(),
        Some(
            nested_live_authorization_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "current live authorization session_dir",
        &mut mismatches,
    );
    compare_option_string(
        view.nested_preflight_session_dir.as_deref(),
        Some(
            live_authorization_preflight_session_dir(nested_live_authorization_session_dir)
                .display()
                .to_string()
                .as_str(),
        ),
        "current live authorization nested_preflight_session_dir",
        &mut mismatches,
    );
    compare_string(
        &view.run_preflight_command_summary,
        &live_authorization_run_preflight_command_summary(
            snapshot,
            &live_authorization_preflight_session_dir(nested_live_authorization_session_dir),
        ),
        "current live authorization run_preflight_command_summary",
        &mut mismatches,
    );
    if view.gate_evaluation_command_summary.trim().is_empty() {
        mismatches.push(
            "current live authorization gate_evaluation_command_summary is missing".to_string(),
        );
    }
    compare_string(
        &view.authorized_live_cutover_command_summary,
        &snapshot.reviewed_frozen_live_cutover_controller_command_summary,
        "current live authorization authorized_live_cutover_command_summary",
        &mut mismatches,
    );
    mismatches
}

fn planned_session(
    root: &Path,
    session_dir: &Path,
    paths: &PackageAuthorizeLatestPaths,
    resolved: Option<&ResolvedLatestAuthorization>,
    failure: Option<&LatestAuthorizationResolutionFailure>,
) -> PackageAuthorizeLatestSession {
    let snapshot = resolved
        .map(|value| &value.snapshot)
        .or_else(|| failure.and_then(|value| value.snapshot.as_ref()));
    let latest_top_step_name = snapshot
        .map(|value| value.latest_top_step_name.clone())
        .or_else(|| {
            failure.and_then(|value| {
                value
                    .latest_execute_report_json
                    .as_ref()
                    .and_then(|raw| extract_non_empty_string(Some(raw), "latest_top_step_name"))
            })
        });
    let latest_top_session_dir = snapshot
        .map(|value| value.latest_top_session_dir.display().to_string())
        .or_else(|| {
            failure.and_then(|value| {
                value
                    .latest_execute_report_json
                    .as_ref()
                    .and_then(|raw| extract_non_empty_string(Some(raw), "latest_top_session_dir"))
            })
        });
    PackageAuthorizeLatestSession {
        session_version: AUTHORIZE_LATEST_SESSION_VERSION.to_string(),
        planned_at: Utc::now(),
        root: root.display().to_string(),
        session_dir: session_dir.display().to_string(),
        shadow_execute_latest_session_dir: paths
            .shadow_execute_latest_session_dir
            .display()
            .to_string(),
        nested_live_authorization_session_dir: paths
            .nested_live_authorization_session_dir
            .display()
            .to_string(),
        latest_top_step_name,
        latest_top_session_dir,
        choir_receipt_session_dir: snapshot
            .map(|value| value.choir_receipt_session_dir.display().to_string()),
        decision_packet_session_dir: snapshot
            .map(|value| value.decision_packet_session_dir.display().to_string()),
        latest_chain_execute_frozen_session_dir: snapshot.map(|value| {
            value
                .latest_chain_execute_frozen_session_dir
                .display()
                .to_string()
        }),
        turn_green_session_dir: snapshot
            .map(|value| value.turn_green_session_dir.display().to_string()),
        launch_packet_session_dir: snapshot
            .map(|value| value.launch_packet_session_dir.display().to_string()),
        historical_authorization_session_dir: resolved
            .map(|value| {
                value
                    .historical_authorization_session_dir
                    .display()
                    .to_string()
            })
            .or_else(|| {
                failure.and_then(|value| {
                    value
                        .historical_authorization_session_dir
                        .as_ref()
                        .map(|path| path.display().to_string())
                })
            }),
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
        verify_historical_launch_packet_command_summary: snapshot
            .map(|value| verify_historical_launch_packet_command_summary(value)),
        plan_live_authorization_command_summary: snapshot.map(|value| {
            plan_live_authorization_command_summary(
                value,
                &paths.nested_live_authorization_session_dir,
            )
        }),
        downstream_live_authorization_command_summary: snapshot.map(|value| {
            downstream_live_authorization_command_summary(
                value,
                &paths.nested_live_authorization_session_dir,
            )
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
    paths: &PackageAuthorizeLatestPaths,
    failure: &LatestAuthorizationResolutionFailure,
) -> PackageAuthorizeLatestStatus {
    let snapshot = failure.snapshot.as_ref();
    PackageAuthorizeLatestStatus {
        status_version: AUTHORIZE_LATEST_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        session_dir: session_dir.display().to_string(),
        root: root.display().to_string(),
        latest_top_step_name: snapshot
            .map(|value| value.latest_top_step_name.clone())
            .or_else(|| {
                failure
                    .latest_execute_report_json
                    .as_ref()
                    .and_then(|raw| extract_non_empty_string(Some(raw), "latest_top_step_name"))
            }),
        latest_top_session_dir: snapshot
            .map(|value| value.latest_top_session_dir.display().to_string())
            .or_else(|| {
                failure
                    .latest_execute_report_json
                    .as_ref()
                    .and_then(|raw| extract_non_empty_string(Some(raw), "latest_top_session_dir"))
            }),
        latest_chain_execute_frozen_session_dir: snapshot.map(|value| {
            value
                .latest_chain_execute_frozen_session_dir
                .display()
                .to_string()
        }),
        turn_green_session_dir: snapshot
            .map(|value| value.turn_green_session_dir.display().to_string()),
        launch_packet_session_dir: snapshot
            .map(|value| value.launch_packet_session_dir.display().to_string()),
        historical_authorization_session_dir: failure
            .historical_authorization_session_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        nested_live_authorization_session_dir: paths
            .nested_live_authorization_session_dir
            .display()
            .to_string(),
        result: result_from_failure_kind(failure.kind),
        reason: failure.reason.clone(),
        latest_execute_verdict: failure.latest_execute_verdict.clone(),
        historical_launch_packet_verdict: failure.historical_launch_packet_verdict.clone(),
        current_authorization_plan_verdict: failure.current_authorization_plan_verdict.clone(),
        current_pre_activation_gate_verdict: snapshot
            .and_then(|value| value.current_pre_activation_gate_verdict.clone()),
        current_pre_activation_gate_reason: snapshot
            .and_then(|value| value.current_pre_activation_gate_reason.clone()),
        latest_execute_step: failure.latest_execute_report_json.as_ref().map(|_| {
            step_artifact(
                &paths.latest_execute_report_path,
                "plan_latest_execute",
                failure
                    .latest_execute_verdict
                    .as_deref()
                    .unwrap_or("tiny_live_package_execute_latest_refused_by_invalid_latest_chain"),
                failure
                    .latest_execute_reason
                    .as_deref()
                    .unwrap_or(failure.reason.as_str()),
                failure.latest_execute_generated_at.unwrap_or_else(Utc::now),
            )
        }),
        historical_launch_packet_step: failure.historical_launch_packet_report_json.as_ref().map(
            |_| {
                step_artifact(
                    &paths.historical_launch_packet_report_path,
                    "verify_live_package_launch_packet",
                    failure
                        .historical_launch_packet_verdict
                        .as_deref()
                        .unwrap_or("tiny_live_package_launch_packet_verify_invalid"),
                    failure
                        .historical_launch_packet_reason
                        .as_deref()
                        .unwrap_or(failure.reason.as_str()),
                    failure
                        .historical_launch_packet_generated_at
                        .unwrap_or_else(Utc::now),
                )
            },
        ),
        current_authorization_plan_step: failure
            .current_authorization_plan_report_json
            .as_ref()
            .map(|_| {
                step_artifact(
                    &paths.current_authorization_plan_report_path,
                    "plan_live_package_authorization",
                    failure
                        .current_authorization_plan_verdict
                        .as_deref()
                        .unwrap_or("tiny_live_package_live_authorization_verify_invalid"),
                    failure
                        .current_authorization_plan_reason
                        .as_deref()
                        .unwrap_or(failure.reason.as_str()),
                    failure
                        .current_authorization_plan_generated_at
                        .unwrap_or_else(Utc::now),
                )
            }),
        live_authorization_step: None,
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
    paths: &PackageAuthorizeLatestPaths,
    resolved: Option<&ResolvedLatestAuthorization>,
    result: LivePackageAuthorizeLatestResult,
    reason: String,
    latest_execute_step: Option<PackageAuthorizeLatestStepArtifact>,
    historical_launch_packet_step: Option<PackageAuthorizeLatestStepArtifact>,
    current_authorization_plan_step: Option<PackageAuthorizeLatestStepArtifact>,
    live_authorization_step: Option<PackageAuthorizeLatestStepArtifact>,
    current_pre_activation_gate_verdict: Option<String>,
    current_pre_activation_gate_reason: Option<String>,
    activation_authorized: bool,
    operator_next_action_summary: String,
) -> PackageAuthorizeLatestStatus {
    let snapshot = resolved.map(|value| &value.snapshot);
    PackageAuthorizeLatestStatus {
        status_version: AUTHORIZE_LATEST_STATUS_VERSION.to_string(),
        updated_at: Utc::now(),
        session_dir: session_dir.display().to_string(),
        root: root.display().to_string(),
        latest_top_step_name: snapshot.map(|value| value.latest_top_step_name.clone()),
        latest_top_session_dir: snapshot
            .map(|value| value.latest_top_session_dir.display().to_string()),
        latest_chain_execute_frozen_session_dir: snapshot.map(|value| {
            value
                .latest_chain_execute_frozen_session_dir
                .display()
                .to_string()
        }),
        turn_green_session_dir: snapshot
            .map(|value| value.turn_green_session_dir.display().to_string()),
        launch_packet_session_dir: snapshot
            .map(|value| value.launch_packet_session_dir.display().to_string()),
        historical_authorization_session_dir: resolved.map(|value| {
            value
                .historical_authorization_session_dir
                .display()
                .to_string()
        }),
        nested_live_authorization_session_dir: paths
            .nested_live_authorization_session_dir
            .display()
            .to_string(),
        result,
        reason,
        latest_execute_verdict: resolved.map(|value| value.latest_execute_verdict.clone()),
        historical_launch_packet_verdict: resolved
            .map(|value| value.historical_launch_packet_verdict.clone()),
        current_authorization_plan_verdict: resolved
            .map(|value| value.current_authorization_plan_verdict.clone()),
        current_pre_activation_gate_verdict,
        current_pre_activation_gate_reason,
        latest_execute_step,
        historical_launch_packet_step,
        current_authorization_plan_step,
        live_authorization_step,
        operator_next_action_summary,
        activation_authorized,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn plan_report_from_resolution(
    mode: &str,
    verdict: TinyLivePackageAuthorizeLatestVerdict,
    reason: String,
    root: &Path,
    session_dir: &Path,
    paths: &PackageAuthorizeLatestPaths,
    resolved: &ResolvedLatestAuthorization,
    authorization_happened: bool,
) -> PackageAuthorizeLatestReport {
    PackageAuthorizeLatestReport {
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
        choir_receipt_session_dir: Some(
            resolved
                .snapshot
                .choir_receipt_session_dir
                .display()
                .to_string(),
        ),
        decision_packet_session_dir: Some(
            resolved
                .snapshot
                .decision_packet_session_dir
                .display()
                .to_string(),
        ),
        latest_chain_execute_frozen_session_dir: Some(
            resolved
                .snapshot
                .latest_chain_execute_frozen_session_dir
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
        historical_authorization_session_dir: Some(
            resolved
                .historical_authorization_session_dir
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
        authorize_latest_session_path: Some(paths.session_path.display().to_string()),
        authorize_latest_status_path: Some(paths.status_path.display().to_string()),
        authorize_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_execute_report_path: Some(paths.latest_execute_report_path.display().to_string()),
        historical_launch_packet_report_path: Some(
            paths
                .historical_launch_packet_report_path
                .display()
                .to_string(),
        ),
        current_authorization_plan_report_path: Some(
            paths
                .current_authorization_plan_report_path
                .display()
                .to_string(),
        ),
        nested_live_authorization_report_path: Some(
            paths.live_authorization_report_path.display().to_string(),
        ),
        shadow_execute_latest_session_dir: Some(
            paths
                .shadow_execute_latest_session_dir
                .display()
                .to_string(),
        ),
        nested_live_authorization_session_dir: Some(
            paths
                .nested_live_authorization_session_dir
                .display()
                .to_string(),
        ),
        latest_execute_verdict: Some(resolved.latest_execute_verdict.clone()),
        historical_launch_packet_verdict: Some(resolved.historical_launch_packet_verdict.clone()),
        current_authorization_plan_verdict: Some(
            resolved.current_authorization_plan_verdict.clone(),
        ),
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
        verify_historical_launch_packet_command_summary: Some(
            verify_historical_launch_packet_command_summary(&resolved.snapshot),
        ),
        plan_live_authorization_command_summary: Some(plan_live_authorization_command_summary(
            &resolved.snapshot,
            &paths.nested_live_authorization_session_dir,
        )),
        downstream_live_authorization_command_summary: Some(
            downstream_live_authorization_command_summary(
                &resolved.snapshot,
                &paths.nested_live_authorization_session_dir,
            ),
        ),
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
        authorization_happened,
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn report_from_resolution_failure(
    _config: &Config,
    mode: &str,
    root: &Path,
    session_dir: Option<&Path>,
    paths: Option<&PackageAuthorizeLatestPaths>,
    failure: &LatestAuthorizationResolutionFailure,
    authorization_happened: bool,
) -> PackageAuthorizeLatestReport {
    let snapshot = failure.snapshot.as_ref();
    PackageAuthorizeLatestReport {
        generated_at: Utc::now(),
        mode: mode.to_string(),
        verdict: verdict_from_failure_kind(failure.kind),
        reason: failure.reason.clone(),
        root: Some(root.display().to_string()),
        latest_top_step_name: snapshot
            .map(|value| value.latest_top_step_name.clone())
            .or_else(|| {
                failure
                    .latest_execute_report_json
                    .as_ref()
                    .and_then(|raw| extract_non_empty_string(Some(raw), "latest_top_step_name"))
            }),
        latest_top_session_dir: snapshot
            .map(|value| value.latest_top_session_dir.display().to_string())
            .or_else(|| {
                failure
                    .latest_execute_report_json
                    .as_ref()
                    .and_then(|raw| extract_non_empty_string(Some(raw), "latest_top_session_dir"))
            }),
        choir_receipt_session_dir: snapshot
            .map(|value| value.choir_receipt_session_dir.display().to_string()),
        decision_packet_session_dir: snapshot
            .map(|value| value.decision_packet_session_dir.display().to_string()),
        latest_chain_execute_frozen_session_dir: snapshot.map(|value| {
            value
                .latest_chain_execute_frozen_session_dir
                .display()
                .to_string()
        }),
        turn_green_session_dir: snapshot
            .map(|value| value.turn_green_session_dir.display().to_string()),
        launch_packet_session_dir: snapshot
            .map(|value| value.launch_packet_session_dir.display().to_string()),
        historical_authorization_session_dir: failure
            .historical_authorization_session_dir
            .as_ref()
            .map(|path| path.display().to_string()),
        package_dir: snapshot.map(|value| value.package_dir.display().to_string()),
        install_root: snapshot.map(|value| value.install_root.display().to_string()),
        target_service_name: snapshot.map(|value| value.target_service_name.clone()),
        backend_command: snapshot.map(|value| value.backend_command.clone()),
        wrapper_timeout_ms: snapshot.map(|value| value.wrapper_timeout_ms),
        service_status_max_staleness_ms: snapshot
            .map(|value| value.service_status_max_staleness_ms),
        session_dir: session_dir.map(|path| path.display().to_string()),
        authorize_latest_session_path: paths.map(|value| value.session_path.display().to_string()),
        authorize_latest_status_path: paths.map(|value| value.status_path.display().to_string()),
        authorize_latest_report_path: paths.map(|value| value.report_path.display().to_string()),
        latest_execute_report_path: paths
            .map(|value| value.latest_execute_report_path.display().to_string()),
        historical_launch_packet_report_path: paths.map(|value| {
            value
                .historical_launch_packet_report_path
                .display()
                .to_string()
        }),
        current_authorization_plan_report_path: paths.map(|value| {
            value
                .current_authorization_plan_report_path
                .display()
                .to_string()
        }),
        nested_live_authorization_report_path: paths
            .map(|value| value.live_authorization_report_path.display().to_string()),
        shadow_execute_latest_session_dir: paths.map(|value| {
            value
                .shadow_execute_latest_session_dir
                .display()
                .to_string()
        }),
        nested_live_authorization_session_dir: paths.map(|value| {
            value
                .nested_live_authorization_session_dir
                .display()
                .to_string()
        }),
        latest_execute_verdict: failure.latest_execute_verdict.clone(),
        historical_launch_packet_verdict: failure.historical_launch_packet_verdict.clone(),
        current_authorization_plan_verdict: failure.current_authorization_plan_verdict.clone(),
        result: Some(serialize_enum(&result_from_failure_kind(failure.kind))),
        current_pre_activation_gate_verdict: snapshot
            .and_then(|value| value.current_pre_activation_gate_verdict.clone()),
        current_pre_activation_gate_reason: snapshot
            .and_then(|value| value.current_pre_activation_gate_reason.clone()),
        resolve_latest_chain_command_summary: paths.map(|value| {
            resolve_latest_chain_command_summary(root, &value.shadow_execute_latest_session_dir)
        }),
        verify_historical_launch_packet_command_summary: snapshot
            .map(verify_historical_launch_packet_command_summary),
        plan_live_authorization_command_summary: snapshot.map(|value| {
            plan_live_authorization_command_summary(
                value,
                &paths
                    .map(|entry| entry.nested_live_authorization_session_dir.clone())
                    .unwrap_or_else(|| PathBuf::from("<session-dir>")),
            )
        }),
        downstream_live_authorization_command_summary: snapshot.map(|value| {
            downstream_live_authorization_command_summary(
                value,
                &paths
                    .map(|entry| entry.nested_live_authorization_session_dir.clone())
                    .unwrap_or_else(|| PathBuf::from("<session-dir>")),
            )
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
        authorization_happened,
        activation_authorized: false,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn report_from_status(
    mode: &str,
    verdict: TinyLivePackageAuthorizeLatestVerdict,
    session_dir: &Path,
    paths: &PackageAuthorizeLatestPaths,
    session: &PackageAuthorizeLatestSession,
    status: &PackageAuthorizeLatestStatus,
    verification_mismatches: Vec<String>,
    authorization_happened: bool,
) -> PackageAuthorizeLatestReport {
    PackageAuthorizeLatestReport {
        generated_at: Utc::now(),
        mode: mode.to_string(),
        verdict,
        reason: status.reason.clone(),
        root: Some(session.root.clone()),
        latest_top_step_name: session.latest_top_step_name.clone(),
        latest_top_session_dir: session.latest_top_session_dir.clone(),
        choir_receipt_session_dir: session.choir_receipt_session_dir.clone(),
        decision_packet_session_dir: session.decision_packet_session_dir.clone(),
        latest_chain_execute_frozen_session_dir: session
            .latest_chain_execute_frozen_session_dir
            .clone(),
        turn_green_session_dir: session.turn_green_session_dir.clone(),
        launch_packet_session_dir: session.launch_packet_session_dir.clone(),
        historical_authorization_session_dir: session.historical_authorization_session_dir.clone(),
        package_dir: session.package_dir.clone(),
        install_root: session.install_root.clone(),
        target_service_name: session.target_service_name.clone(),
        backend_command: session.backend_command.clone(),
        wrapper_timeout_ms: session.wrapper_timeout_ms,
        service_status_max_staleness_ms: session.service_status_max_staleness_ms,
        session_dir: Some(session_dir.display().to_string()),
        authorize_latest_session_path: Some(paths.session_path.display().to_string()),
        authorize_latest_status_path: Some(paths.status_path.display().to_string()),
        authorize_latest_report_path: Some(paths.report_path.display().to_string()),
        latest_execute_report_path: Some(paths.latest_execute_report_path.display().to_string()),
        historical_launch_packet_report_path: Some(
            paths
                .historical_launch_packet_report_path
                .display()
                .to_string(),
        ),
        current_authorization_plan_report_path: Some(
            paths
                .current_authorization_plan_report_path
                .display()
                .to_string(),
        ),
        nested_live_authorization_report_path: Some(
            paths.live_authorization_report_path.display().to_string(),
        ),
        shadow_execute_latest_session_dir: Some(
            paths
                .shadow_execute_latest_session_dir
                .display()
                .to_string(),
        ),
        nested_live_authorization_session_dir: Some(
            paths
                .nested_live_authorization_session_dir
                .display()
                .to_string(),
        ),
        latest_execute_verdict: status.latest_execute_verdict.clone(),
        historical_launch_packet_verdict: status.historical_launch_packet_verdict.clone(),
        current_authorization_plan_verdict: status.current_authorization_plan_verdict.clone(),
        result: Some(serialize_enum(&status.result)),
        current_pre_activation_gate_verdict: status.current_pre_activation_gate_verdict.clone(),
        current_pre_activation_gate_reason: status.current_pre_activation_gate_reason.clone(),
        resolve_latest_chain_command_summary: Some(
            session.resolve_latest_chain_command_summary.clone(),
        ),
        verify_historical_launch_packet_command_summary: session
            .verify_historical_launch_packet_command_summary
            .clone(),
        plan_live_authorization_command_summary: session
            .plan_live_authorization_command_summary
            .clone(),
        downstream_live_authorization_command_summary: session
            .downstream_live_authorization_command_summary
            .clone(),
        reviewed_frozen_live_cutover_controller_command_summary: session
            .reviewed_frozen_live_cutover_controller_command_summary
            .clone(),
        clerestory_certificate_summary: session.clerestory_certificate_summary.clone(),
        clerestory_certificate_sha256: session.clerestory_certificate_sha256.clone(),
        clerestory_certificate_algorithm: session.clerestory_certificate_algorithm.clone(),
        verification_mismatches,
        authorization_happened,
        activation_authorized: status.activation_authorized,
        explicit_statement: PLANNING_SAFE_STATEMENT.to_string(),
    }
}

fn verify_invalid_report_without_session(
    session_dir: &Path,
    mismatches: Vec<String>,
) -> PackageAuthorizeLatestReport {
    let reason = mismatches
        .first()
        .cloned()
        .unwrap_or_else(|| "latest-authorization verification failed".to_string());
    PackageAuthorizeLatestReport {
        generated_at: Utc::now(),
        mode: "verify_latest_authorization".to_string(),
        verdict: TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestVerifyInvalid,
        reason,
        root: None,
        latest_top_step_name: None,
        latest_top_session_dir: None,
        choir_receipt_session_dir: None,
        decision_packet_session_dir: None,
        latest_chain_execute_frozen_session_dir: None,
        turn_green_session_dir: None,
        launch_packet_session_dir: None,
        historical_authorization_session_dir: None,
        package_dir: None,
        install_root: None,
        target_service_name: None,
        backend_command: None,
        wrapper_timeout_ms: None,
        service_status_max_staleness_ms: None,
        session_dir: Some(session_dir.display().to_string()),
        authorize_latest_session_path: None,
        authorize_latest_status_path: None,
        authorize_latest_report_path: None,
        latest_execute_report_path: None,
        historical_launch_packet_report_path: None,
        current_authorization_plan_report_path: None,
        nested_live_authorization_report_path: None,
        shadow_execute_latest_session_dir: None,
        nested_live_authorization_session_dir: None,
        latest_execute_verdict: None,
        historical_launch_packet_verdict: None,
        current_authorization_plan_verdict: None,
        result: None,
        current_pre_activation_gate_verdict: None,
        current_pre_activation_gate_reason: None,
        resolve_latest_chain_command_summary: None,
        verify_historical_launch_packet_command_summary: None,
        plan_live_authorization_command_summary: None,
        downstream_live_authorization_command_summary: None,
        reviewed_frozen_live_cutover_controller_command_summary: None,
        clerestory_certificate_summary: None,
        clerestory_certificate_sha256: None,
        clerestory_certificate_algorithm: None,
        verification_mismatches: mismatches,
        authorization_happened: false,
        activation_authorized: false,
        explicit_statement:
            "this latest-authorization handoff verification checks bounded immutable-package / launch-packet / authorization evidence only; it does not authorize activation by itself"
                .to_string(),
    }
}

fn compare_session_contract_against_resolution(
    session: &PackageAuthorizeLatestSession,
    root: &Path,
    paths: &PackageAuthorizeLatestPaths,
    resolved: &ResolvedLatestAuthorization,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &session.root,
        &root.display().to_string(),
        "stored session root",
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
        session.choir_receipt_session_dir.as_deref(),
        Some(
            resolved
                .snapshot
                .choir_receipt_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored session choir_receipt_session_dir",
        mismatches,
    );
    compare_option_string(
        session.decision_packet_session_dir.as_deref(),
        Some(
            resolved
                .snapshot
                .decision_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored session decision_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        session.latest_chain_execute_frozen_session_dir.as_deref(),
        Some(
            resolved
                .snapshot
                .latest_chain_execute_frozen_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored session latest_chain_execute_frozen_session_dir",
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
        session.historical_authorization_session_dir.as_deref(),
        Some(
            resolved
                .historical_authorization_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored session historical_authorization_session_dir",
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
        session
            .verify_historical_launch_packet_command_summary
            .as_deref(),
        Some(verify_historical_launch_packet_command_summary(&resolved.snapshot).as_str()),
        "stored session verify_historical_launch_packet_command_summary",
        mismatches,
    );
    compare_option_string(
        session.plan_live_authorization_command_summary.as_deref(),
        Some(
            plan_live_authorization_command_summary(
                &resolved.snapshot,
                &paths.nested_live_authorization_session_dir,
            )
            .as_str(),
        ),
        "stored session plan_live_authorization_command_summary",
        mismatches,
    );
    compare_option_string(
        session
            .downstream_live_authorization_command_summary
            .as_deref(),
        Some(
            downstream_live_authorization_command_summary(
                &resolved.snapshot,
                &paths.nested_live_authorization_session_dir,
            )
            .as_str(),
        ),
        "stored session downstream_live_authorization_command_summary",
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

fn compare_report_contract_against_resolution(
    report: &PackageAuthorizeLatestReport,
    root: &Path,
    paths: &PackageAuthorizeLatestPaths,
    resolved: &ResolvedLatestAuthorization,
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
        report.choir_receipt_session_dir.as_deref(),
        Some(
            resolved
                .snapshot
                .choir_receipt_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report choir_receipt_session_dir",
        mismatches,
    );
    compare_option_string(
        report.decision_packet_session_dir.as_deref(),
        Some(
            resolved
                .snapshot
                .decision_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report decision_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        report.latest_chain_execute_frozen_session_dir.as_deref(),
        Some(
            resolved
                .snapshot
                .latest_chain_execute_frozen_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report latest_chain_execute_frozen_session_dir",
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
        report.historical_authorization_session_dir.as_deref(),
        Some(
            resolved
                .historical_authorization_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored report historical_authorization_session_dir",
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
        report
            .verify_historical_launch_packet_command_summary
            .as_deref(),
        Some(verify_historical_launch_packet_command_summary(&resolved.snapshot).as_str()),
        "stored report verify_historical_launch_packet_command_summary",
        mismatches,
    );
    compare_option_string(
        report.plan_live_authorization_command_summary.as_deref(),
        Some(
            plan_live_authorization_command_summary(
                &resolved.snapshot,
                &paths.nested_live_authorization_session_dir,
            )
            .as_str(),
        ),
        "stored report plan_live_authorization_command_summary",
        mismatches,
    );
    compare_option_string(
        report
            .downstream_live_authorization_command_summary
            .as_deref(),
        Some(
            downstream_live_authorization_command_summary(
                &resolved.snapshot,
                &paths.nested_live_authorization_session_dir,
            )
            .as_str(),
        ),
        "stored report downstream_live_authorization_command_summary",
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

fn compare_failure_session_contract(
    session: &PackageAuthorizeLatestSession,
    root: &Path,
    paths: &PackageAuthorizeLatestPaths,
    failure: &LatestAuthorizationResolutionFailure,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &session.root,
        &root.display().to_string(),
        "stored session root on failure",
        mismatches,
    );
    compare_string(
        &session.resolve_latest_chain_command_summary,
        &resolve_latest_chain_command_summary(root, &paths.shadow_execute_latest_session_dir),
        "stored session resolve_latest_chain_command_summary on failure",
        mismatches,
    );
    compare_string(
        &session.shadow_execute_latest_session_dir,
        &paths
            .shadow_execute_latest_session_dir
            .display()
            .to_string(),
        "stored session shadow_execute_latest_session_dir on failure",
        mismatches,
    );
    compare_string(
        &session.nested_live_authorization_session_dir,
        &paths
            .nested_live_authorization_session_dir
            .display()
            .to_string(),
        "stored session nested_live_authorization_session_dir on failure",
        mismatches,
    );
    if let Some(snapshot) = &failure.snapshot {
        compare_option_string(
            session.package_dir.as_deref(),
            Some(snapshot.package_dir.display().to_string().as_str()),
            "stored session package_dir on failure",
            mismatches,
        );
    }
}

fn compare_failure_report_contract(
    report: &PackageAuthorizeLatestReport,
    root: &Path,
    paths: &PackageAuthorizeLatestPaths,
    failure: &LatestAuthorizationResolutionFailure,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        report.root.as_deref(),
        Some(root.display().to_string().as_str()),
        "stored report root on failure",
        mismatches,
    );
    compare_option_string(
        report.resolve_latest_chain_command_summary.as_deref(),
        Some(
            resolve_latest_chain_command_summary(root, &paths.shadow_execute_latest_session_dir)
                .as_str(),
        ),
        "stored report resolve_latest_chain_command_summary on failure",
        mismatches,
    );
    compare_option_string(
        report.latest_execute_verdict.as_deref(),
        failure.latest_execute_verdict.as_deref(),
        "stored report latest_execute_verdict on failure",
        mismatches,
    );
    if let Some(snapshot) = &failure.snapshot {
        compare_option_string(
            report.package_dir.as_deref(),
            Some(snapshot.package_dir.display().to_string().as_str()),
            "stored report package_dir on failure",
            mismatches,
        );
    }
}

fn compare_execute_latest_copy_against_snapshot(
    stored: &ExecuteLatestReportView,
    snapshot: &LatestAuthorizationSnapshot,
    shadow_execute_latest_session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        Some(stored.verdict.as_str()),
        Some(EXECUTE_LATEST_PLAN_READY),
        "stored latest_execute copy verdict",
        mismatches,
    );
    compare_option_string(
        stored.latest_top_step_name.as_deref(),
        Some(snapshot.latest_top_step_name.as_str()),
        "stored latest_execute copy latest_top_step_name",
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
        "stored latest_execute copy latest_top_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.choir_receipt_session_dir.as_deref(),
        Some(
            snapshot
                .choir_receipt_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored latest_execute copy choir_receipt_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.decision_packet_session_dir.as_deref(),
        Some(
            snapshot
                .decision_packet_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored latest_execute copy decision_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.latest_chain_execute_frozen_session_dir.as_deref(),
        Some(
            snapshot
                .latest_chain_execute_frozen_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored latest_execute copy latest_chain_execute_frozen_session_dir",
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
        "stored latest_execute copy turn_green_session_dir",
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
        "stored latest_execute copy launch_packet_session_dir",
        mismatches,
    );
    compare_option_string(
        stored.package_dir.as_deref(),
        Some(snapshot.package_dir.display().to_string().as_str()),
        "stored latest_execute copy package_dir",
        mismatches,
    );
    compare_option_string(
        stored.install_root.as_deref(),
        Some(snapshot.install_root.display().to_string().as_str()),
        "stored latest_execute copy install_root",
        mismatches,
    );
    compare_option_string(
        stored.target_service_name.as_deref(),
        Some(snapshot.target_service_name.as_str()),
        "stored latest_execute copy target_service_name",
        mismatches,
    );
    compare_option_string(
        stored.backend_command.as_deref(),
        Some(snapshot.backend_command.as_str()),
        "stored latest_execute copy backend_command",
        mismatches,
    );
    compare_option_u64(
        stored.wrapper_timeout_ms,
        Some(snapshot.wrapper_timeout_ms),
        "stored latest_execute copy wrapper_timeout_ms",
        mismatches,
    );
    compare_option_u64(
        stored.service_status_max_staleness_ms,
        Some(snapshot.service_status_max_staleness_ms),
        "stored latest_execute copy service_status_max_staleness_ms",
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
        "stored latest_execute copy reviewed_frozen_live_cutover_controller_command_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_summary.as_deref(),
        Some(snapshot.clerestory_certificate_summary.as_str()),
        "stored latest_execute copy clerestory_certificate_summary",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_sha256.as_deref(),
        Some(snapshot.clerestory_certificate_sha256.as_str()),
        "stored latest_execute copy clerestory_certificate_sha256",
        mismatches,
    );
    compare_option_string(
        stored.clerestory_certificate_algorithm.as_deref(),
        Some(snapshot.clerestory_certificate_algorithm.as_str()),
        "stored latest_execute copy clerestory_certificate_algorithm",
        mismatches,
    );
    compare_option_string(
        extract_non_empty_string_from_option(stored.current_pre_activation_gate_verdict.as_deref()),
        snapshot.current_pre_activation_gate_verdict.as_deref(),
        "stored latest_execute copy current_pre_activation_gate_verdict",
        mismatches,
    );
    compare_option_string(
        extract_non_empty_string_from_option(stored.current_pre_activation_gate_reason.as_deref()),
        snapshot.current_pre_activation_gate_reason.as_deref(),
        "stored latest_execute copy current_pre_activation_gate_reason",
        mismatches,
    );
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
}

fn compare_launch_packet_copy_against_snapshot(
    stored: &LaunchPacketReportView,
    snapshot: &LatestAuthorizationSnapshot,
    historical_authorization_session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_option_string(
        Some(stored.verdict.as_str()),
        Some(LAUNCH_PACKET_VERIFY_OK),
        "stored launch_packet copy verdict",
        mismatches,
    );
    mismatches.extend(compare_launch_packet_against_snapshot(
        stored,
        snapshot,
        historical_authorization_session_dir,
    ));
}

fn compare_live_authorization_copy_against_verified(
    stored: &LiveAuthorizationReportView,
    verified: &LiveAuthorizationReportView,
    expected_preflight_session_dir: &Path,
    mismatches: &mut Vec<String>,
) {
    compare_string(
        &stored.verdict,
        &verified.verdict,
        "stored nested live authorization verdict",
        mismatches,
    );
    compare_string(
        &stored.reason,
        &verified.reason,
        "stored nested live authorization reason",
        mismatches,
    );
    compare_option_string(
        stored.session_dir.as_deref(),
        verified.session_dir.as_deref(),
        "stored nested live authorization session_dir",
        mismatches,
    );
    compare_option_string(
        stored.nested_preflight_session_dir.as_deref(),
        Some(
            expected_preflight_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "stored nested live authorization nested_preflight_session_dir",
        mismatches,
    );
    compare_option_string(
        verified.nested_preflight_session_dir.as_deref(),
        Some(
            expected_preflight_session_dir
                .display()
                .to_string()
                .as_str(),
        ),
        "verified nested live authorization nested_preflight_session_dir",
        mismatches,
    );
    compare_string(
        &stored.package_dir,
        &verified.package_dir,
        "stored nested live authorization package_dir",
        mismatches,
    );
    compare_string(
        &stored.install_root,
        &verified.install_root,
        "stored nested live authorization install_root",
        mismatches,
    );
    compare_string(
        &stored.target_service_name,
        &verified.target_service_name,
        "stored nested live authorization target_service_name",
        mismatches,
    );
    compare_string(
        &stored.backend_command,
        &verified.backend_command,
        "stored nested live authorization backend_command",
        mismatches,
    );
    if stored.wrapper_timeout_ms != verified.wrapper_timeout_ms {
        mismatches.push(format!(
            "stored nested live authorization wrapper_timeout_ms {} does not match verified {}",
            stored.wrapper_timeout_ms, verified.wrapper_timeout_ms
        ));
    }
    if stored.service_status_max_staleness_ms != verified.service_status_max_staleness_ms {
        mismatches.push(format!(
            "stored nested live authorization service_status_max_staleness_ms {} does not match verified {}",
            stored.service_status_max_staleness_ms, verified.service_status_max_staleness_ms
        ));
    }
    compare_option_string(
        stored.result.as_deref(),
        verified.result.as_deref(),
        "stored nested live authorization result",
        mismatches,
    );
    compare_string(
        &stored.run_preflight_command_summary,
        &verified.run_preflight_command_summary,
        "stored nested live authorization run_preflight_command_summary",
        mismatches,
    );
    compare_string(
        &stored.gate_evaluation_command_summary,
        &verified.gate_evaluation_command_summary,
        "stored nested live authorization gate_evaluation_command_summary",
        mismatches,
    );
    compare_string(
        &stored.authorized_live_cutover_command_summary,
        &verified.authorized_live_cutover_command_summary,
        "stored nested live authorization authorized_live_cutover_command_summary",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_verdict.as_deref(),
        verified.current_pre_activation_gate_verdict.as_deref(),
        "stored nested live authorization current_pre_activation_gate_verdict",
        mismatches,
    );
    compare_option_string(
        stored.current_pre_activation_gate_reason.as_deref(),
        verified.current_pre_activation_gate_reason.as_deref(),
        "stored nested live authorization current_pre_activation_gate_reason",
        mismatches,
    );
    if stored.activation_authorized != verified.activation_authorized {
        mismatches.push(format!(
            "stored nested live authorization activation_authorized {} does not match verified {}",
            stored.activation_authorized, verified.activation_authorized
        ));
    }
}

fn nested_live_authorization_status_path(session_dir: &Path) -> PathBuf {
    session_dir.join("tiny_live_activation_package_live_authorization.status.json")
}

fn verified_live_authorization_run_view(
    verified: &LiveAuthorizationReportView,
    status: &LiveAuthorizationStatusView,
) -> LiveAuthorizationReportView {
    let mut view = verified.clone();
    view.verdict = live_authorization_run_verdict_for_result(&status.result).to_string();
    view.reason = status.reason.clone();
    view
}

fn live_authorization_run_verdict_for_result(result: &str) -> &'static str {
    match result {
        "authorized_now" => "tiny_live_package_live_authorization_authorized_now",
        "refused_by_stage3" => "tiny_live_package_live_authorization_refused_by_stage3",
        "refused_by_pre_activation_gate" => {
            "tiny_live_package_live_authorization_refused_by_pre_activation_gate"
        }
        _ => "tiny_live_package_live_authorization_refused_by_invalid_target",
    }
}

fn map_live_authorization_outcome(
    report: &LiveAuthorizationReportView,
) -> (
    LivePackageAuthorizeLatestResult,
    TinyLivePackageAuthorizeLatestVerdict,
) {
    match report.result.as_deref() {
        Some("authorized_now") => (
            LivePackageAuthorizeLatestResult::AuthorizedNow,
            TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestAuthorizedNow,
        ),
        Some("refused_by_stage3") => (
            LivePackageAuthorizeLatestResult::RefusedNowByStage3,
            TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestRefusedNowByStage3,
        ),
        Some("refused_by_pre_activation_gate") => (
            LivePackageAuthorizeLatestResult::RefusedNowByPreActivationGate,
            TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestRefusedNowByPreActivationGate,
        ),
        _ => (
            LivePackageAuthorizeLatestResult::RefusedByDriftedAuthorizationContract,
            TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestRefusedByDriftedAuthorizationContract,
        ),
    }
}

fn result_from_live_authorization_result(result: Option<&str>) -> LivePackageAuthorizeLatestResult {
    match result {
        Some("authorized_now") => LivePackageAuthorizeLatestResult::AuthorizedNow,
        Some("refused_by_stage3") => LivePackageAuthorizeLatestResult::RefusedNowByStage3,
        Some("refused_by_pre_activation_gate") => {
            LivePackageAuthorizeLatestResult::RefusedNowByPreActivationGate
        }
        _ => LivePackageAuthorizeLatestResult::RefusedByDriftedAuthorizationContract,
    }
}

fn verdict_for_result(
    result: LivePackageAuthorizeLatestResult,
) -> TinyLivePackageAuthorizeLatestVerdict {
    match result {
        LivePackageAuthorizeLatestResult::RefusedByMissingLatestChain => {
            TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestRefusedByMissingLatestChain
        }
        LivePackageAuthorizeLatestResult::RefusedByInvalidLatestChain => {
            TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestRefusedByInvalidLatestChain
        }
        LivePackageAuthorizeLatestResult::RefusedByDriftedAuthorizationContract => {
            TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestRefusedByDriftedAuthorizationContract
        }
        LivePackageAuthorizeLatestResult::RefusedNowByStage3 => {
            TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestRefusedNowByStage3
        }
        LivePackageAuthorizeLatestResult::RefusedNowByPreActivationGate => {
            TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestRefusedNowByPreActivationGate
        }
        LivePackageAuthorizeLatestResult::AuthorizedNow => {
            TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestAuthorizedNow
        }
    }
}

fn result_from_failure_kind(kind: ResolutionFailureKind) -> LivePackageAuthorizeLatestResult {
    match kind {
        ResolutionFailureKind::MissingLatestChain => {
            LivePackageAuthorizeLatestResult::RefusedByMissingLatestChain
        }
        ResolutionFailureKind::InvalidLatestChain => {
            LivePackageAuthorizeLatestResult::RefusedByInvalidLatestChain
        }
        ResolutionFailureKind::DriftedAuthorizationContract => {
            LivePackageAuthorizeLatestResult::RefusedByDriftedAuthorizationContract
        }
    }
}

fn verdict_from_failure_kind(kind: ResolutionFailureKind) -> TinyLivePackageAuthorizeLatestVerdict {
    verdict_for_result(result_from_failure_kind(kind))
}

fn map_execute_latest_failure_kind(verdict: &str) -> ResolutionFailureKind {
    match verdict {
        "tiny_live_package_execute_latest_refused_by_missing_latest_chain" => {
            ResolutionFailureKind::MissingLatestChain
        }
        "tiny_live_package_execute_latest_refused_by_drifted_frozen_contract" => {
            ResolutionFailureKind::DriftedAuthorizationContract
        }
        _ => ResolutionFailureKind::InvalidLatestChain,
    }
}

fn operator_next_action_summary(result: LivePackageAuthorizeLatestResult) -> String {
    match result {
        LivePackageAuthorizeLatestResult::RefusedByMissingLatestChain => {
            "no immutable clerestory chain is available yet; do not manually hunt launch_packet or authorization lineage by hand".to_string()
        }
        LivePackageAuthorizeLatestResult::RefusedByInvalidLatestChain => {
            "the latest immutable chain is incomplete or below the accepted top layer; mint a fresh coherent clerestory chain instead of bypassing the authorization path".to_string()
        }
        LivePackageAuthorizeLatestResult::RefusedByDriftedAuthorizationContract => {
            "the latest immutable chain no longer proves the exact accepted authorization contract; reconcile launch/authorization/controller drift before any go/no-go decision".to_string()
        }
        LivePackageAuthorizeLatestResult::RefusedNowByStage3 => {
            "the handoff resolved cleanly, but Stage 3 truth is still non-green right now; rerun later without changing the accepted authorization contract".to_string()
        }
        LivePackageAuthorizeLatestResult::RefusedNowByPreActivationGate => {
            "the handoff resolved cleanly, but the pre-activation gate is still non-green right now; rerun later without changing the accepted authorization contract".to_string()
        }
        LivePackageAuthorizeLatestResult::AuthorizedNow => {
            "the latest immutable chain is currently authorized through the accepted live authorization contract; any later live cutover decision still remains an explicit operator choice under the existing bounded contract".to_string()
        }
    }
}

fn authorize_latest_paths(session_dir: &Path) -> PackageAuthorizeLatestPaths {
    PackageAuthorizeLatestPaths {
        session_path: session_dir
            .join("tiny_live_activation_package_authorize_latest.session.json"),
        status_path: session_dir.join("tiny_live_activation_package_authorize_latest.status.json"),
        report_path: session_dir.join("tiny_live_activation_package_authorize_latest.report.json"),
        latest_execute_report_path: session_dir
            .join("tiny_live_activation_package_authorize_latest.latest_execute.report.json"),
        historical_launch_packet_report_path: session_dir.join(
            "tiny_live_activation_package_authorize_latest.historical_launch_packet.report.json",
        ),
        current_authorization_plan_report_path: session_dir
            .join("tiny_live_activation_package_authorize_latest.authorization_plan.report.json"),
        live_authorization_report_path: session_dir
            .join("tiny_live_activation_package_authorize_latest.live_authorization.report.json"),
        shadow_execute_latest_session_dir: session_dir
            .join("tiny_live_activation_package_authorize_latest.execute_latest_shadow.session"),
        nested_live_authorization_session_dir: session_dir
            .join("tiny_live_activation_package_authorize_latest.live_authorization_session"),
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

fn resolve_latest_chain_command_summary(
    root: &Path,
    shadow_execute_latest_session_dir: &Path,
) -> String {
    format!(
        "{EXECUTE_LATEST_BIN} --root {} --session-dir {} --plan-latest-execute",
        shell_escape_path(root),
        shell_escape_path(shadow_execute_latest_session_dir),
    )
}

fn verify_historical_launch_packet_command_summary(
    snapshot: &LatestAuthorizationSnapshot,
) -> String {
    format!(
        "{LAUNCH_PACKET_BIN} --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --service-status-max-staleness-ms {} --session-dir {} --verify-live-package-launch-packet",
        shell_escape_path(&snapshot.package_dir),
        shell_escape_path(&snapshot.install_root),
        shell_escape_string(&snapshot.target_service_name),
        shell_escape_string(&snapshot.backend_command),
        snapshot.wrapper_timeout_ms,
        snapshot.service_status_max_staleness_ms,
        shell_escape_path(&snapshot.launch_packet_session_dir),
    )
}

fn historical_live_authorization_command_summary(
    snapshot: &LatestAuthorizationSnapshot,
    session_dir: &Path,
) -> String {
    format!(
        "{LIVE_AUTHORIZATION_BIN} --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --service-status-max-staleness-ms {} --session-dir {} --run-live-package-authorization",
        shell_escape_path(&snapshot.package_dir),
        shell_escape_path(&snapshot.install_root),
        shell_escape_string(&snapshot.target_service_name),
        shell_escape_string(&snapshot.backend_command),
        snapshot.wrapper_timeout_ms,
        snapshot.service_status_max_staleness_ms,
        shell_escape_path(session_dir),
    )
}

fn plan_live_authorization_command_summary(
    snapshot: &LatestAuthorizationSnapshot,
    session_dir: &Path,
) -> String {
    format!(
        "{LIVE_AUTHORIZATION_BIN} --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --service-status-max-staleness-ms {} --session-dir {} --plan-live-package-authorization",
        shell_escape_path(&snapshot.package_dir),
        shell_escape_path(&snapshot.install_root),
        shell_escape_string(&snapshot.target_service_name),
        shell_escape_string(&snapshot.backend_command),
        snapshot.wrapper_timeout_ms,
        snapshot.service_status_max_staleness_ms,
        shell_escape_path(session_dir),
    )
}

fn downstream_live_authorization_command_summary(
    snapshot: &LatestAuthorizationSnapshot,
    session_dir: &Path,
) -> String {
    historical_live_authorization_command_summary(snapshot, session_dir)
}

fn verify_live_authorization_command_summary(
    snapshot: &LatestAuthorizationSnapshot,
    session_dir: &Path,
) -> String {
    format!(
        "{LIVE_AUTHORIZATION_BIN} --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --service-status-max-staleness-ms {} --session-dir {} --verify-live-package-authorization",
        shell_escape_path(&snapshot.package_dir),
        shell_escape_path(&snapshot.install_root),
        shell_escape_string(&snapshot.target_service_name),
        shell_escape_string(&snapshot.backend_command),
        snapshot.wrapper_timeout_ms,
        snapshot.service_status_max_staleness_ms,
        shell_escape_path(session_dir),
    )
}

fn live_authorization_preflight_session_dir(session_dir: &Path) -> PathBuf {
    session_dir.join("tiny_live_activation_package_live_authorization.preflight_session")
}

fn live_authorization_run_preflight_command_summary(
    snapshot: &LatestAuthorizationSnapshot,
    preflight_session_dir: &Path,
) -> String {
    format!(
        "copybot_tiny_live_activation_package_preflight --package-dir {} --install-root {} --target-service {} --backend-command {} --wrapper-timeout-ms {} --service-status-max-staleness-ms {} --session-dir {} --run-live-package-preflight",
        shell_escape_path(&snapshot.package_dir),
        shell_escape_path(&snapshot.install_root),
        shell_escape_string(&snapshot.target_service_name),
        shell_escape_string(&snapshot.backend_command),
        snapshot.wrapper_timeout_ms,
        snapshot.service_status_max_staleness_ms,
        shell_escape_path(preflight_session_dir),
    )
}

fn live_launch_packet_verify_args(snapshot: &LatestAuthorizationSnapshot) -> Vec<String> {
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
        snapshot.launch_packet_session_dir.display().to_string(),
        "--verify-live-package-launch-packet".to_string(),
        "--json".to_string(),
    ]
}

fn live_authorization_args(
    snapshot: &LatestAuthorizationSnapshot,
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

fn parse_string_arg(flag: &str, value: Option<String>) -> Result<String> {
    value.ok_or_else(|| anyhow!("missing value for {flag}"))
}

fn set_mode(mode: &mut Option<Mode>, value: Mode, flag: &str) -> Result<()> {
    if mode.replace(value).is_some() {
        bail!("multiple modes specified; latest was {flag}");
    }
    Ok(())
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

fn write_script(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, contents).with_context(|| {
        format!(
            "failed writing latest-authorization script to {}",
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
) -> PackageAuthorizeLatestStepArtifact {
    PackageAuthorizeLatestStepArtifact {
        path: path.display().to_string(),
        mode: mode.to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        generated_at,
    }
}

fn load_required_step_json<T: for<'de> Deserialize<'de>>(
    step: &Option<PackageAuthorizeLatestStepArtifact>,
    expected_path: &Path,
    label: &str,
    mismatches: &mut Vec<String>,
) -> Result<T> {
    let step = step
        .as_ref()
        .ok_or_else(|| anyhow!("{label} is missing from latest-authorization status"))?;
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

fn compare_datetime(
    actual: DateTime<Utc>,
    expected: DateTime<Utc>,
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
    step: Option<&PackageAuthorizeLatestStepArtifact>,
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
            "{label} is missing from latest-authorization status"
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
    compare_datetime(
        step.generated_at,
        expected_generated_at,
        &format!("{label} generated_at"),
        mismatches,
    );
}

fn required_option_string(value: Option<&String>, label: &str) -> Result<String> {
    match value.map(|raw| raw.trim()).filter(|raw| !raw.is_empty()) {
        Some(raw) => Ok(raw.to_string()),
        None => bail!("execute_latest plan is missing required field {label}"),
    }
}

fn extract_non_empty_string(value: Option<&Value>, key: &str) -> Option<String> {
    let candidate = value
        .and_then(|json| json.get(key))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|raw| !raw.is_empty())?;
    Some(candidate.to_string())
}

fn extract_non_empty_string_from_option(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|raw| !raw.is_empty())
}

fn serialize_enum<T: Serialize>(value: &T) -> String {
    match serde_json::to_value(value) {
        Ok(Value::String(raw)) => raw,
        Ok(other) => other.to_string(),
        Err(_) => "<serialization-error>".to_string(),
    }
}

fn render_report_lines(report: &PackageAuthorizeLatestReport) -> String {
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
    if let Some(value) = &report.launch_packet_session_dir {
        lines.push(format!("launch_packet_session_dir={value}"));
    }
    if let Some(value) = &report.turn_green_session_dir {
        lines.push(format!("turn_green_session_dir={value}"));
    }
    if let Some(value) = &report.latest_chain_execute_frozen_session_dir {
        lines.push(format!("latest_chain_execute_frozen_session_dir={value}"));
    }
    if let Some(value) = &report.historical_authorization_session_dir {
        lines.push(format!("historical_authorization_session_dir={value}"));
    }
    if let Some(value) = &report.result {
        lines.push(format!("result={value}"));
    }
    lines.push(format!(
        "authorization_happened={}",
        report.authorization_happened
    ));
    lines.push(format!(
        "activation_authorized={}",
        report.activation_authorized
    ));
    if let Some(value) = &report.downstream_live_authorization_command_summary {
        lines.push(format!(
            "downstream_live_authorization_command_summary={value}"
        ));
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
    use serde_json::json;
    use std::ffi::OsString;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[test]
    fn plan_mode_resolves_latest_valid_clerestory_chain_to_expected_nested_authorization_lineage() {
        let fixture = fake_authorize_latest_fixture(
            "authorize_latest_plan_valid",
            "authorized_now",
            "authorized_now",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let report = plan_latest_authorization_report(&fixture.plan_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestPlanReady
        );
        assert_eq!(
            report.latest_top_session_dir.as_deref(),
            Some(
                fixture
                    .latest_top_session_dir
                    .display()
                    .to_string()
                    .as_str()
            )
        );
        assert_eq!(
            report.turn_green_session_dir.as_deref(),
            Some(
                fixture
                    .turn_green_session_dir
                    .display()
                    .to_string()
                    .as_str()
            )
        );
        assert_eq!(
            report.latest_chain_execute_frozen_session_dir.as_deref(),
            Some(
                fixture
                    .historical_execute_frozen_session_dir
                    .display()
                    .to_string()
                    .as_str()
            )
        );
        assert_eq!(
            report.launch_packet_session_dir.as_deref(),
            Some(
                fixture
                    .launch_packet_session_dir
                    .display()
                    .to_string()
                    .as_str()
            )
        );
        assert_eq!(
            report.historical_authorization_session_dir.as_deref(),
            Some(
                fixture
                    .historical_authorization_session_dir
                    .display()
                    .to_string()
                    .as_str()
            )
        );
        assert!(report
            .downstream_live_authorization_command_summary
            .as_deref()
            .unwrap()
            .contains(LIVE_AUTHORIZATION_BIN));
    }

    #[test]
    fn render_mode_emits_exact_downstream_authorization_contract_not_second_controller_path() {
        let fixture = fake_authorize_latest_fixture(
            "authorize_latest_render_contract",
            "authorized_now",
            "authorized_now",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let report = render_latest_authorization_script_report(&fixture.render_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestScriptRendered
        );
        let script = fs::read_to_string(&fixture.output_script_path).unwrap();
        assert!(script.contains(LIVE_AUTHORIZATION_BIN));
        assert!(!script.contains("copybot_tiny_live_activation_package_live_cutover"));
        assert!(!script.contains(
            "copybot_tiny_live_activation_package_authorize_latest --run-latest-authorization"
        ));
    }

    #[test]
    fn run_mode_refuses_when_latest_chain_missing_invalid_or_below_clerestory() {
        let missing_fixture = fake_authorize_latest_fixture(
            "authorize_latest_missing_latest",
            "authorized_now",
            "authorized_now",
        );
        overwrite_json(
            &missing_fixture.execute_latest_plan_json_path,
            &json!({
                "generated_at": Utc::now(),
                "verdict": "tiny_live_package_execute_latest_refused_by_missing_latest_chain",
                "reason": "no persisted immutable chain exists",
                "session_dir": authorize_latest_paths(&missing_fixture.wrapper_session_dir)
                    .shadow_execute_latest_session_dir
                    .display()
                    .to_string()
            }),
        );
        let _path_guard = PathGuard::install(&missing_fixture.bin_dir);
        let missing = run_latest_authorization_report(&missing_fixture.run_config()).unwrap();
        assert_eq!(
            missing.verdict,
            TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestRefusedByMissingLatestChain
        );

        {
            let invalid_fixture = fake_authorize_latest_fixture(
                "authorize_latest_invalid_latest",
                "authorized_now",
                "authorized_now",
            );
            overwrite_json(
                &invalid_fixture.execute_latest_plan_json_path,
                &json!({
                    "generated_at": Utc::now(),
                    "verdict": "tiny_live_package_execute_latest_refused_by_invalid_latest_chain",
                    "reason": "latest immutable chain is incomplete",
                    "session_dir": authorize_latest_paths(&invalid_fixture.wrapper_session_dir)
                        .shadow_execute_latest_session_dir
                        .display()
                        .to_string(),
                    "latest_top_step_name": "clerestory_certificate",
                    "latest_top_session_dir": invalid_fixture.latest_top_session_dir.display().to_string()
                }),
            );
            let invalid = run_latest_authorization_report(&invalid_fixture.run_config()).unwrap();
            assert_eq!(
                invalid.verdict,
                TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestRefusedByInvalidLatestChain
            );
        }

        {
            let below_fixture = fake_authorize_latest_fixture(
                "authorize_latest_below_top",
                "authorized_now",
                "authorized_now",
            );
            overwrite_json(
                &below_fixture.execute_latest_plan_json_path,
                &json!({
                    "generated_at": Utc::now(),
                    "verdict": "tiny_live_package_execute_latest_refused_by_invalid_latest_chain",
                    "reason": "latest persisted top chain is choir_receipt, which is below clerestory_certificate",
                    "session_dir": authorize_latest_paths(&below_fixture.wrapper_session_dir)
                        .shadow_execute_latest_session_dir
                        .display()
                        .to_string(),
                    "latest_top_step_name": "choir_receipt",
                    "latest_top_session_dir": below_fixture.root.join("choir-only-session").display().to_string()
                }),
            );
            let below = run_latest_authorization_report(&below_fixture.run_config()).unwrap();
            assert_eq!(
                below.verdict,
                TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestRefusedByInvalidLatestChain
            );
        }
    }

    #[test]
    fn run_mode_refuses_when_nested_authorization_lineage_drifts_from_latest_immutable_chain() {
        let fixture = fake_authorize_latest_fixture(
            "authorize_latest_drifted_launch_packet",
            "authorized_now",
            "authorized_now",
        );
        let mut launch_packet_report: Value =
            load_json(&fixture.launch_packet_verify_json_path).unwrap();
        launch_packet_report["frozen_live_cutover_controller_command_summary"] =
            json!("drifted frozen controller");
        persist_json(
            &fixture.launch_packet_verify_json_path,
            &launch_packet_report,
        )
        .unwrap();
        let _path_guard = PathGuard::install(&fixture.bin_dir);

        let report = run_latest_authorization_report(&fixture.run_config()).unwrap();
        assert_eq!(
            report.verdict,
            TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestRefusedByDriftedAuthorizationContract
        );
        assert!(!report.authorization_happened);
        assert!(!report.activation_authorized);
    }

    #[test]
    fn verify_mode_fails_when_persisted_latest_authorization_artifacts_are_tampered_or_missing_nested_lineage_truth(
    ) {
        let fixture = fake_authorize_latest_fixture(
            "authorize_latest_verify_tamper",
            "authorized_now",
            "authorized_now",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        let run = run_latest_authorization_report(&fixture.run_config()).unwrap();
        assert_eq!(
            run.verdict,
            TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestAuthorizedNow
        );

        let paths = authorize_latest_paths(&fixture.wrapper_session_dir);
        let mut session: PackageAuthorizeLatestSession = load_json(&paths.session_path).unwrap();
        session.downstream_live_authorization_command_summary =
            Some("tampered downstream authorization command".to_string());
        persist_json(&paths.session_path, &session).unwrap();
        fs::remove_file(&paths.historical_launch_packet_report_path).unwrap();

        let verify = verify_latest_authorization_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestVerifyInvalid
        );
        assert!(
            verify.verification_mismatches.iter().any(|entry| {
                entry.contains("downstream_live_authorization_command_summary")
                    || entry.contains("historical_launch_packet_step")
                    || entry.contains(
                        &paths
                            .historical_launch_packet_report_path
                            .display()
                            .to_string(),
                    )
            }),
            "{:#?}",
            verify.verification_mismatches
        );
    }

    #[test]
    fn verify_fails_when_stored_status_reason_drifts() {
        let fixture = fake_authorize_latest_fixture(
            "authorize_latest_verify_status_reason_tamper",
            "authorized_now",
            "authorized_now",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_authorization_report(&fixture.run_config()).unwrap();

        let paths = authorize_latest_paths(&fixture.wrapper_session_dir);
        let mut status: PackageAuthorizeLatestStatus = load_json(&paths.status_path).unwrap();
        status.reason = "tampered wrapper status reason".to_string();
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_latest_authorization_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestVerifyInvalid
        );
        assert!(
            verify
                .verification_mismatches
                .iter()
                .any(|entry| entry.contains("stored status reason")),
            "{:#?}",
            verify.verification_mismatches
        );
    }

    #[test]
    fn verify_fails_when_stored_status_operator_next_action_summary_drifts() {
        let fixture = fake_authorize_latest_fixture(
            "authorize_latest_verify_operator_summary_tamper",
            "refused_by_stage3",
            "refused_by_stage3",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_authorization_report(&fixture.run_config()).unwrap();

        let paths = authorize_latest_paths(&fixture.wrapper_session_dir);
        let mut status: PackageAuthorizeLatestStatus = load_json(&paths.status_path).unwrap();
        status.operator_next_action_summary = "tampered operator summary".to_string();
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_latest_authorization_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestVerifyInvalid
        );
        assert!(
            verify
                .verification_mismatches
                .iter()
                .any(|entry| entry.contains("operator_next_action_summary")),
            "{:#?}",
            verify.verification_mismatches
        );
    }

    #[test]
    fn verify_fails_when_stored_report_reason_drifts() {
        let fixture = fake_authorize_latest_fixture(
            "authorize_latest_verify_report_reason_tamper",
            "authorized_now",
            "authorized_now",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_authorization_report(&fixture.run_config()).unwrap();

        let paths = authorize_latest_paths(&fixture.wrapper_session_dir);
        let mut report: PackageAuthorizeLatestReport = load_json(&paths.report_path).unwrap();
        report.reason = "tampered wrapper report reason".to_string();
        persist_json(&paths.report_path, &report).unwrap();

        let verify = verify_latest_authorization_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestVerifyInvalid
        );
        assert!(
            verify
                .verification_mismatches
                .iter()
                .any(|entry| entry.contains("stored report reason")),
            "{:#?}",
            verify.verification_mismatches
        );
    }

    #[test]
    fn verify_fails_when_stored_step_artifact_metadata_drifts() {
        let fixture = fake_authorize_latest_fixture(
            "authorize_latest_verify_step_metadata_tamper",
            "authorized_now",
            "authorized_now",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_authorization_report(&fixture.run_config()).unwrap();

        let paths = authorize_latest_paths(&fixture.wrapper_session_dir);
        let mut status: PackageAuthorizeLatestStatus = load_json(&paths.status_path).unwrap();
        status
            .current_authorization_plan_step
            .as_mut()
            .unwrap()
            .mode = "tampered_plan_mode".to_string();
        status.live_authorization_step.as_mut().unwrap().reason =
            "tampered live authorization step reason".to_string();
        persist_json(&paths.status_path, &status).unwrap();

        let verify = verify_latest_authorization_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestVerifyInvalid
        );
        assert!(
            verify.verification_mismatches.iter().any(|entry| {
                entry.contains("current_authorization_plan_step mode")
                    || entry.contains("live_authorization_step reason")
            }),
            "{:#?}",
            verify.verification_mismatches
        );
    }

    #[test]
    fn verify_fails_when_stored_nested_live_authorization_reason_drifts() {
        let fixture = fake_authorize_latest_fixture(
            "authorize_latest_verify_nested_live_authorization_reason_tamper",
            "authorized_now",
            "authorized_now",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_authorization_report(&fixture.run_config()).unwrap();

        let paths = authorize_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.live_authorization_report_path).unwrap();
        report["reason"] = json!("tampered stored nested live authorization reason");
        persist_json(&paths.live_authorization_report_path, &report).unwrap();

        let verify = verify_latest_authorization_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestVerifyInvalid
        );
        assert!(
            verify
                .verification_mismatches
                .iter()
                .any(|entry| entry.contains("stored nested live authorization reason")),
            "{:#?}",
            verify.verification_mismatches
        );
    }

    #[test]
    fn verify_fails_when_stored_nested_live_authorization_verdict_drifts() {
        let fixture = fake_authorize_latest_fixture(
            "authorize_latest_verify_nested_live_authorization_verdict_tamper",
            "authorized_now",
            "authorized_now",
        );
        let _path_guard = PathGuard::install(&fixture.bin_dir);
        run_latest_authorization_report(&fixture.run_config()).unwrap();

        let paths = authorize_latest_paths(&fixture.wrapper_session_dir);
        let mut report: Value = load_json(&paths.live_authorization_report_path).unwrap();
        report["verdict"] = json!("tampered_nested_live_authorization_verdict");
        persist_json(&paths.live_authorization_report_path, &report).unwrap();

        let verify = verify_latest_authorization_report(&fixture.verify_config()).unwrap();
        assert_eq!(
            verify.verdict,
            TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestVerifyInvalid
        );
        assert!(
            verify
                .verification_mismatches
                .iter()
                .any(|entry| entry.contains("stored nested live authorization verdict")),
            "{:#?}",
            verify.verification_mismatches
        );
    }

    #[test]
    fn all_modes_preserve_stage3_and_pre_activation_refusal_semantics_and_do_not_imply_activation_authorization_by_themselves(
    ) {
        {
            let stage3_fixture = fake_authorize_latest_fixture(
                "authorize_latest_stage3_refusal",
                "refused_by_stage3",
                "refused_by_stage3",
            );
            let _path_guard = PathGuard::install(&stage3_fixture.bin_dir);

            let plan = plan_latest_authorization_report(&stage3_fixture.plan_config()).unwrap();
            assert!(!plan.activation_authorized);

            let run = run_latest_authorization_report(&stage3_fixture.run_config()).unwrap();
            assert_eq!(
                run.verdict,
                TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestRefusedNowByStage3
            );
            assert_eq!(run.result.as_deref(), Some("refused_now_by_stage3"));
            assert!(!run.activation_authorized);

            let verify =
                verify_latest_authorization_report(&stage3_fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestVerifyOk,
                "{:#?}",
                verify.verification_mismatches
            );
            assert!(!verify.activation_authorized);
        }

        {
            let pre_gate_fixture = fake_authorize_latest_fixture(
                "authorize_latest_pre_gate_refusal",
                "refused_by_pre_activation_gate",
                "refused_by_pre_activation_gate",
            );
            let _path_guard = PathGuard::install(&pre_gate_fixture.bin_dir);

            let run = run_latest_authorization_report(&pre_gate_fixture.run_config()).unwrap();
            assert_eq!(
                run.verdict,
                TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestRefusedNowByPreActivationGate
            );
            assert_eq!(
                run.result.as_deref(),
                Some("refused_now_by_pre_activation_gate")
            );
            assert!(!run.activation_authorized);

            let verify =
                verify_latest_authorization_report(&pre_gate_fixture.verify_config()).unwrap();
            assert_eq!(
                verify.verdict,
                TinyLivePackageAuthorizeLatestVerdict::TinyLivePackageAuthorizeLatestVerifyOk,
                "{:#?}",
                verify.verification_mismatches
            );
            assert!(!verify.activation_authorized);
        }
    }

    struct FakeAuthorizeLatestFixture {
        root: PathBuf,
        bin_dir: PathBuf,
        output_script_path: PathBuf,
        wrapper_session_dir: PathBuf,
        latest_top_session_dir: PathBuf,
        choir_receipt_session_dir: PathBuf,
        decision_packet_session_dir: PathBuf,
        historical_execute_frozen_session_dir: PathBuf,
        turn_green_session_dir: PathBuf,
        launch_packet_session_dir: PathBuf,
        historical_authorization_session_dir: PathBuf,
        package_dir: PathBuf,
        install_root: PathBuf,
        backend_command: PathBuf,
        execute_latest_plan_json_path: PathBuf,
        launch_packet_verify_json_path: PathBuf,
        authorization_plan_json_path: PathBuf,
        authorization_run_json_path: PathBuf,
        authorization_verify_json_path: PathBuf,
        frozen_controller_command: String,
    }

    impl FakeAuthorizeLatestFixture {
        fn plan_config(&self) -> Config {
            Config {
                mode: Mode::PlanLatestAuthorization,
                root: Some(self.root.clone()),
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn render_config(&self) -> Config {
            Config {
                mode: Mode::RenderLatestAuthorizationScript,
                root: Some(self.root.clone()),
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: Some(self.output_script_path.clone()),
                json: true,
            }
        }

        fn run_config(&self) -> Config {
            Config {
                mode: Mode::RunLatestAuthorization,
                root: Some(self.root.clone()),
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }

        fn verify_config(&self) -> Config {
            Config {
                mode: Mode::VerifyLatestAuthorization,
                root: None,
                session_dir: Some(self.wrapper_session_dir.clone()),
                output_path: None,
                json: true,
            }
        }
    }

    fn fake_authorize_latest_fixture(
        label: &str,
        live_authorization_run_result: &str,
        live_authorization_verify_result: &str,
    ) -> FakeAuthorizeLatestFixture {
        let fixture_dir = temp_dir(label);
        let root = fixture_dir.join("root");
        let bin_dir = fixture_dir.join("bin");
        let output_script_path = fixture_dir.join("authorize-latest.sh");
        let wrapper_session_dir = fixture_dir.join("authorize-latest-session");
        let latest_top_session_dir = root.join("latest-clerestory-session");
        let choir_receipt_session_dir = fixture_dir.join("choir-receipt-session");
        let decision_packet_session_dir = fixture_dir.join("decision-packet-session");
        let historical_execute_frozen_session_dir =
            fixture_dir.join("historical-execute-frozen-session");
        let turn_green_session_dir = fixture_dir.join("turn-green-session");
        let launch_packet_session_dir = fixture_dir.join("launch-packet-session");
        let historical_authorization_session_dir =
            fixture_dir.join("historical-live-authorization-session");
        let package_dir = fixture_dir.join("package");
        let install_root = fixture_dir.join("install-root");
        let backend_command = fixture_dir.join("backend");
        let execute_latest_plan_json_path = fixture_dir.join("execute_latest.plan.json");
        let launch_packet_verify_json_path = fixture_dir.join("launch_packet.verify.json");
        let authorization_plan_json_path = fixture_dir.join("authorization.plan.json");
        let authorization_run_json_path = fixture_dir.join("authorization.run.json");
        let authorization_verify_json_path = fixture_dir.join("authorization.verify.json");
        let frozen_controller_command =
            "copybot_tiny_live_activation_package_live_cutover --package-dir /fake/pkg".to_string();

        for path in [
            &root,
            &bin_dir,
            &latest_top_session_dir,
            &choir_receipt_session_dir,
            &decision_packet_session_dir,
            &historical_execute_frozen_session_dir,
            &turn_green_session_dir,
            &launch_packet_session_dir,
            &historical_authorization_session_dir,
            &package_dir,
            &install_root,
        ] {
            fs::create_dir_all(path).unwrap();
        }
        fs::write(&backend_command, "#!/bin/sh\n").unwrap();

        let fixture = FakeAuthorizeLatestFixture {
            root,
            bin_dir,
            output_script_path,
            wrapper_session_dir,
            latest_top_session_dir,
            choir_receipt_session_dir,
            decision_packet_session_dir,
            historical_execute_frozen_session_dir,
            turn_green_session_dir,
            launch_packet_session_dir,
            historical_authorization_session_dir,
            package_dir,
            install_root,
            backend_command,
            execute_latest_plan_json_path,
            launch_packet_verify_json_path,
            authorization_plan_json_path,
            authorization_run_json_path,
            authorization_verify_json_path,
            frozen_controller_command,
        };

        persist_json(
            &fixture.execute_latest_plan_json_path,
            &default_execute_latest_plan_report(&fixture),
        )
        .unwrap();
        persist_json(
            &fixture.launch_packet_verify_json_path,
            &default_launch_packet_verify_report(&fixture),
        )
        .unwrap();
        persist_json(
            &fixture.authorization_plan_json_path,
            &default_live_authorization_plan_report(&fixture),
        )
        .unwrap();
        persist_json(
            &fixture.authorization_run_json_path,
            &default_live_authorization_run_report(&fixture, live_authorization_run_result),
        )
        .unwrap();
        persist_json(
            &fixture.authorization_verify_json_path,
            &default_live_authorization_verify_report(&fixture, live_authorization_verify_result),
        )
        .unwrap();

        write_fake_execute_latest_command(
            &fixture.bin_dir.join(EXECUTE_LATEST_BIN),
            &fixture.execute_latest_plan_json_path,
            &fixture.root,
            &authorize_latest_paths(&fixture.wrapper_session_dir).shadow_execute_latest_session_dir,
        );
        write_fake_launch_packet_command(
            &fixture.bin_dir.join(LAUNCH_PACKET_BIN),
            &fixture.launch_packet_verify_json_path,
            &fixture.launch_packet_session_dir,
            &fixture.package_dir,
            &fixture.install_root,
            "solana-copy-bot.service",
            &fixture.backend_command,
        );
        write_fake_live_authorization_command(
            &fixture.bin_dir.join(LIVE_AUTHORIZATION_BIN),
            &fixture.authorization_plan_json_path,
            &fixture.authorization_run_json_path,
            &fixture.authorization_verify_json_path,
            &fixture.package_dir,
            &fixture.install_root,
            "solana-copy-bot.service",
            &fixture.backend_command,
            1200,
            4500,
            &authorize_latest_paths(&fixture.wrapper_session_dir)
                .nested_live_authorization_session_dir,
        );

        fixture
    }

    fn default_execute_latest_plan_report(fixture: &FakeAuthorizeLatestFixture) -> Value {
        let paths = authorize_latest_paths(&fixture.wrapper_session_dir);
        json!({
            "generated_at": Utc::now(),
            "mode": "plan_latest_execute",
            "verdict": EXECUTE_LATEST_PLAN_READY,
            "reason": "latest immutable chain resolves cleanly",
            "root": fixture.root.display().to_string(),
            "session_dir": paths.shadow_execute_latest_session_dir.display().to_string(),
            "latest_top_step_name": "clerestory_certificate",
            "latest_top_session_dir": fixture.latest_top_session_dir.display().to_string(),
            "choir_receipt_session_dir": fixture.choir_receipt_session_dir.display().to_string(),
            "decision_packet_session_dir": fixture.decision_packet_session_dir.display().to_string(),
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
            "current_pre_activation_gate_verdict": "blocked",
            "current_pre_activation_gate_reason": "stage3 blocked"
        })
    }

    fn default_launch_packet_verify_report(fixture: &FakeAuthorizeLatestFixture) -> Value {
        json!({
            "generated_at": Utc::now(),
            "mode": "verify_live_package_launch_packet",
            "verdict": LAUNCH_PACKET_VERIFY_OK,
            "reason": "launch packet lineage ok",
            "package_dir": fixture.package_dir.display().to_string(),
            "install_root": fixture.install_root.display().to_string(),
            "target_service_name": "solana-copy-bot.service",
            "backend_command": fixture.backend_command.display().to_string(),
            "wrapper_timeout_ms": 1200,
            "service_status_max_staleness_ms": 4500,
            "session_dir": fixture.launch_packet_session_dir.display().to_string(),
            "launch_packet_session_path": fixture.launch_packet_session_dir.join("tiny_live_activation_package_launch_packet.session.json").display().to_string(),
            "launch_packet_status_path": fixture.launch_packet_session_dir.join("tiny_live_activation_package_launch_packet.status.json").display().to_string(),
            "nested_authorization_session_dir": fixture.historical_authorization_session_dir.display().to_string(),
            "result": "refused_by_stage3",
            "authorization_result_now": "refused_by_stage3",
            "run_live_authorization_command_summary": historical_live_authorization_command_summary(
                &LatestAuthorizationSnapshot {
                    latest_top_step_name: "clerestory_certificate".to_string(),
                    latest_top_session_dir: fixture.latest_top_session_dir.clone(),
                    choir_receipt_session_dir: fixture.choir_receipt_session_dir.clone(),
                    decision_packet_session_dir: fixture.decision_packet_session_dir.clone(),
                    latest_chain_execute_frozen_session_dir: fixture.historical_execute_frozen_session_dir.clone(),
                    turn_green_session_dir: fixture.turn_green_session_dir.clone(),
                    launch_packet_session_dir: fixture.launch_packet_session_dir.clone(),
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
                    current_pre_activation_gate_verdict: Some("blocked".to_string()),
                    current_pre_activation_gate_reason: Some("stage3 blocked".to_string()),
                },
                &fixture.historical_authorization_session_dir,
            ),
            "frozen_live_cutover_controller_command_summary": fixture.frozen_controller_command,
            "current_pre_activation_gate_verdict": "blocked",
            "current_pre_activation_gate_reason": "stage3 blocked",
            "activation_authorized": false
        })
    }

    fn default_live_authorization_plan_report(fixture: &FakeAuthorizeLatestFixture) -> Value {
        let paths = authorize_latest_paths(&fixture.wrapper_session_dir);
        json!({
            "generated_at": Utc::now(),
            "mode": "plan_live_package_authorization",
            "verdict": LIVE_AUTHORIZATION_PLAN_READY,
            "reason": "authorization plan ready",
            "package_dir": fixture.package_dir.display().to_string(),
            "install_root": fixture.install_root.display().to_string(),
            "target_service_name": "solana-copy-bot.service",
            "backend_command": fixture.backend_command.display().to_string(),
            "wrapper_timeout_ms": 1200,
            "service_status_max_staleness_ms": 4500,
            "session_dir": paths.nested_live_authorization_session_dir.display().to_string(),
            "nested_preflight_session_dir": live_authorization_preflight_session_dir(&paths.nested_live_authorization_session_dir).display().to_string(),
            "run_preflight_command_summary": default_run_preflight_command_summary(fixture),
            "gate_evaluation_command_summary": "evaluate pre-activation gate",
            "authorized_live_cutover_command_summary": fixture.frozen_controller_command,
            "current_pre_activation_gate_verdict": null,
            "current_pre_activation_gate_reason": null,
            "activation_authorized": false
        })
    }

    fn default_live_authorization_run_report(
        fixture: &FakeAuthorizeLatestFixture,
        result: &str,
    ) -> Value {
        default_live_authorization_result_report(
            fixture,
            "run_live_package_authorization",
            live_authorization_run_verdict(result),
            result,
        )
    }

    fn default_live_authorization_verify_report(
        fixture: &FakeAuthorizeLatestFixture,
        result: &str,
    ) -> Value {
        default_live_authorization_result_report(
            fixture,
            "verify_live_package_authorization",
            LIVE_AUTHORIZATION_VERIFY_OK,
            result,
        )
    }

    fn default_live_authorization_result_report(
        fixture: &FakeAuthorizeLatestFixture,
        mode: &str,
        verdict: &str,
        result: &str,
    ) -> Value {
        let paths = authorize_latest_paths(&fixture.wrapper_session_dir);
        let (gate_verdict, gate_reason, activation_authorized) = match result {
            "authorized_now" => ("green", "ok", true),
            "refused_by_pre_activation_gate" => ("blocked", "pre gate blocked", false),
            "refused_by_stage3" => ("blocked", "stage3 blocked", false),
            _ => ("invalid", "invalid target", false),
        };
        json!({
            "generated_at": Utc::now(),
            "mode": mode,
            "verdict": verdict,
            "reason": format!("authorization {result}"),
            "package_dir": fixture.package_dir.display().to_string(),
            "install_root": fixture.install_root.display().to_string(),
            "target_service_name": "solana-copy-bot.service",
            "backend_command": fixture.backend_command.display().to_string(),
            "wrapper_timeout_ms": 1200,
            "service_status_max_staleness_ms": 4500,
            "session_dir": paths.nested_live_authorization_session_dir.display().to_string(),
            "nested_preflight_session_dir": live_authorization_preflight_session_dir(&paths.nested_live_authorization_session_dir).display().to_string(),
            "result": result,
            "run_preflight_command_summary": default_run_preflight_command_summary(fixture),
            "gate_evaluation_command_summary": "evaluate pre-activation gate",
            "authorized_live_cutover_command_summary": fixture.frozen_controller_command,
            "current_pre_activation_gate_verdict": gate_verdict,
            "current_pre_activation_gate_reason": gate_reason,
            "activation_authorized": activation_authorized
        })
    }

    fn live_authorization_run_verdict(result: &str) -> &'static str {
        match result {
            "authorized_now" => "tiny_live_package_live_authorization_authorized_now",
            "refused_by_stage3" => "tiny_live_package_live_authorization_refused_by_stage3",
            "refused_by_pre_activation_gate" => {
                "tiny_live_package_live_authorization_refused_by_pre_activation_gate"
            }
            _ => "tiny_live_package_live_authorization_refused_by_invalid_target",
        }
    }

    fn default_run_preflight_command_summary(fixture: &FakeAuthorizeLatestFixture) -> String {
        let snapshot = LatestAuthorizationSnapshot {
            latest_top_step_name: "clerestory_certificate".to_string(),
            latest_top_session_dir: fixture.latest_top_session_dir.clone(),
            choir_receipt_session_dir: fixture.choir_receipt_session_dir.clone(),
            decision_packet_session_dir: fixture.decision_packet_session_dir.clone(),
            latest_chain_execute_frozen_session_dir: fixture
                .historical_execute_frozen_session_dir
                .clone(),
            turn_green_session_dir: fixture.turn_green_session_dir.clone(),
            launch_packet_session_dir: fixture.launch_packet_session_dir.clone(),
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
            current_pre_activation_gate_verdict: Some("blocked".to_string()),
            current_pre_activation_gate_reason: Some("stage3 blocked".to_string()),
        };
        live_authorization_run_preflight_command_summary(
            &snapshot,
            &live_authorization_preflight_session_dir(
                &authorize_latest_paths(&fixture.wrapper_session_dir)
                    .nested_live_authorization_session_dir,
            ),
        )
    }

    fn overwrite_json(path: &Path, value: &Value) {
        persist_json(path, value).unwrap();
    }

    fn write_fake_execute_latest_command(
        path: &Path,
        json_path: &Path,
        root: &Path,
        shadow_execute_latest_session_dir: &Path,
    ) {
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --root {} \"*) : ;;\n  *) echo 'unexpected root' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --session-dir {} \"*) : ;;\n  *) echo 'unexpected execute_latest session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --plan-latest-execute \"*) : ;;\n  *) echo 'unexpected execute_latest mode' >&2; exit 1;;\nesac\ncat '{}'\n",
            root.display(),
            shadow_execute_latest_session_dir.display(),
            json_path.display()
        );
        fs::write(path, script).unwrap();
        let mut perms = fs::metadata(path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).unwrap();
    }

    fn write_fake_launch_packet_command(
        path: &Path,
        json_path: &Path,
        launch_packet_session_dir: &Path,
        package_dir: &Path,
        install_root: &Path,
        target_service_name: &str,
        backend_command: &Path,
    ) {
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --package-dir {} \"*) : ;;\n  *) echo 'unexpected package-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --install-root {} \"*) : ;;\n  *) echo 'unexpected install-root' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --target-service {} \"*) : ;;\n  *) echo 'unexpected target-service' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --backend-command {} \"*) : ;;\n  *) echo 'unexpected backend-command' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --session-dir {} \"*) : ;;\n  *) echo 'unexpected launch-packet session-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --verify-live-package-launch-packet \"*) : ;;\n  *) echo 'unexpected launch-packet mode' >&2; exit 1;;\nesac\ncat '{}'\n",
            package_dir.display(),
            install_root.display(),
            target_service_name,
            backend_command.display(),
            launch_packet_session_dir.display(),
            json_path.display()
        );
        fs::write(path, script).unwrap();
        let mut perms = fs::metadata(path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).unwrap();
    }

    fn write_fake_live_authorization_command(
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
        session_dir: &Path,
    ) {
        let status_path = nested_live_authorization_status_path(session_dir);
        let script = format!(
            "#!/bin/sh\nset -eu\ncase \" $* \" in\n  *\" --package-dir {} \"*) : ;;\n  *) echo 'unexpected package-dir' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --install-root {} \"*) : ;;\n  *) echo 'unexpected install-root' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --target-service {} \"*) : ;;\n  *) echo 'unexpected target-service' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --backend-command {} \"*) : ;;\n  *) echo 'unexpected backend-command' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --wrapper-timeout-ms {} \"*) : ;;\n  *) echo 'unexpected wrapper-timeout-ms' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --service-status-max-staleness-ms {} \"*) : ;;\n  *) echo 'unexpected service-status-max-staleness-ms' >&2; exit 1;;\nesac\ncase \" $* \" in\n  *\" --session-dir {} \"*) : ;;\n  *) echo 'unexpected live-authorization session-dir' >&2; exit 1;;\nesac\nmode=''\ncase \" $* \" in\n  *\" --plan-live-package-authorization \"*) mode='plan';;\n  *\" --run-live-package-authorization \"*) mode='run';;\n  *\" --verify-live-package-authorization \"*) mode='verify';;\n  *) echo 'unexpected live-authorization mode' >&2; exit 1;;\nesac\ncase \"$mode\" in\n  plan) cat '{}';;\n  run) mkdir -p '{}'; cp '{}' '{}'; cat '{}';;\n  verify) cat '{}';;\n  *) echo 'unsupported mode' >&2; exit 1;;\nesac\n",
            package_dir.display(),
            install_root.display(),
            target_service_name,
            backend_command.display(),
            wrapper_timeout_ms,
            service_status_max_staleness_ms,
            session_dir.display(),
            plan_json_path.display(),
            session_dir.display(),
            run_json_path.display(),
            status_path.display(),
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
            .unwrap_or(Duration::from_nanos(0))
            .as_nanos();
        let path = std::env::temp_dir().join(format!("{label}.{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }
}
